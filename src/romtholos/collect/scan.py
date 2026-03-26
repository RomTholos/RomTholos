"""Phase 1: Scan — walk sources, hash files, extract and hash archive contents.

After scan completes, the DB contains 5 hashes (CRC32, MD5, SHA1, SHA256,
BLAKE3) for every file across all sources — plain files and archive contents
alike. The match phase can then work as pure DB hash lookups.

Source types:
- romroot: Load hashes from RSCF sidecars (fast). Full rescan rebuilds them.
- ingest:  Hash everything. Write RSCF sidecars for faster re-scans.
- disposal: Same as ingest. Source files deleted after collection (see execute.py).
- readonly: Same scan as ingest, but never writes sidecars or modifies the source.
"""

from __future__ import annotations

import shutil
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from rscf import (
    FileEntry,
    Sidecar,
    SidecarResolver,
    StorageMode,
    hash_file,
    read_sidecar,
    write_sidecar,
)
from rscf.sidecar import RscfError

from romtholos.collect.compress import strip_archive_extension
from romtholos.collect.config import ORPHANED_DIR_NAME, SourceDir
from romtholos.collect.lock import LOCK_FILENAME
from romtholos.collect.db import CacheDB
from romtholos.collect.extract import ExtractionLimits, extract_recursive

# File extensions we recognize as ROM-related or archives
_ROM_EXTENSIONS: set[str] = {
    # Disc images
    ".iso", ".bin", ".cue", ".img", ".mdf", ".mds",
    ".chd", ".rvz", ".gcz", ".wia", ".nrg", ".cdi", ".aaru",
    # Cartridge ROMs
    ".nes", ".sfc", ".smc", ".gb", ".gbc", ".gba",
    ".n64", ".z64", ".v64", ".nds", ".3ds",
    ".md", ".smd", ".gen", ".gg", ".sms",
    ".pce", ".ngp", ".ngc", ".ws", ".wsc",
    ".a26", ".a52", ".a78", ".lnx", ".jag",
    ".col", ".sg", ".sc",
    ".vb", ".vec", ".int",
    # Floppy / tape
    ".adf", ".dsk", ".d64", ".t64", ".tap", ".tzx",
    ".ipf", ".fdi",
    # Archives (we extract these and hash contents)
    ".zip", ".7z", ".rar",
    ".gz", ".bz2", ".xz", ".zst", ".lz4",
    ".tar", ".tgz", ".tbz2", ".txz",
}

# Archive extensions — files that contain other files
_ARCHIVE_EXTENSIONS: set[str] = {
    ".zip", ".7z", ".rar",
    ".gz", ".bz2", ".xz", ".zst", ".lz4",
    ".tar", ".tgz", ".tbz2", ".txz",
    ".rvz", ".gcz", ".wia",  # dolphin disc images (contain ISO)
    ".aaru",  # aaru disc images (contain CUE/BIN or ISO)
}


# Files per transaction — limits progress loss on interrupt.
# Future: parallel scanning (--workers N) could further improve throughput on
# network mounts where CIFS can pipeline multiple requests.
_COMMIT_CHUNK = 50


def _is_scannable(path: Path) -> bool:
    """Check if a file should be scanned."""
    name = path.name.lower()
    if ".tar." in name:
        return True
    return path.suffix.lower() in _ROM_EXTENSIONS


def _is_archive(path: Path) -> bool:
    """Check if a file is an archive whose contents should be extracted."""
    name = path.name.lower()
    if ".tar." in name:
        return True
    return path.suffix.lower() in _ARCHIVE_EXTENSIONS


@dataclass
class SourceScanStats:
    """Results from scanning a single source."""

    source_type: str
    files_total: int = 0
    files_hashed: int = 0
    files_skipped: int = 0
    files_from_sidecar: int = 0
    archives_extracted: int = 0
    archive_entries_hashed: int = 0
    warnings: list[str] = field(default_factory=list)

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)
        print(f"  Warning: {msg}", file=sys.stderr)


def scan_all(
    sources: list[SourceDir],
    db: CacheDB,
    work_dir: Path,
    *,
    force_rescan: bool = False,
    path_filter: Path | None = None,
) -> dict[str, SourceScanStats]:
    """Scan all sources according to their type.

    Args:
        sources: List of SourceDir with source_type.
        db: Cache database.
        work_dir: Working directory for archive extraction (SSD).
        force_rescan: If True, rebuild RSCF sidecars for romroot.
        path_filter: If set, restrict scanning to this subfolder.
            Must be within at least one configured source.

    Returns:
        Dict of source_path (str) -> SourceScanStats.
    """
    results: dict[str, SourceScanStats] = {}

    for source in sources:
        # When path_filter is set, skip sources that don't contain the path
        if path_filter is not None:
            if not path_filter.is_relative_to(source.path):
                continue

        if not source.path.exists():
            print(f"  Skipping {source.path}: does not exist", file=sys.stderr)
            continue

        walk_root = path_filter if path_filter is not None else None
        label = f"{walk_root} (subset of {source.path})" if walk_root else str(source.path)
        print(f"Scanning: {label} ({source.source_type})", file=sys.stderr)

        if source.source_type == "romroot":
            stats = _scan_romroot(
                source.path, db, force_rescan=force_rescan, walk_root=walk_root,
            )
        else:
            stats = _scan_untrusted(
                source.path, source.source_type, db, work_dir,
                walk_root=walk_root,
            )

        results[str(source.path)] = stats
        print(
            f"  Done: {stats.files_hashed} hashed, "
            f"{stats.files_from_sidecar} from sidecar, "
            f"{stats.files_skipped} skipped, "
            f"{stats.archives_extracted} archives "
            f"({stats.archive_entries_hashed} entries)",
            file=sys.stderr,
        )
        if stats.warnings:
            print(f"  {len(stats.warnings)} warning(s)", file=sys.stderr)

    return results


def _scan_romroot(
    source: Path, db: CacheDB, *, force_rescan: bool,
    walk_root: Path | None = None,
) -> SourceScanStats:
    """Scan romroot by loading RSCF sidecars.

    Stat-cache: if a file's (path, size, mtime_ns, ctime_ns, inode) matches
    the DB, skip it entirely — no sidecar read needed.
    Cold start: read sidecar → load hashes into DB without re-hashing.
    force_rescan: re-hash every file, rewrite sidecars, delete orphans.

    Args:
        walk_root: If set, restrict the walk to this subdirectory of source.
    """
    stats = SourceScanStats(source_type="romroot")
    resolver = SidecarResolver(StorageMode.IN_TREE)
    now = datetime.now(timezone.utc).isoformat()

    # Single-pass walk: collect (path, size, mtime_ns, ctime_ns, inode) tuples
    # Exclude _orphaned/ subtree — it is scanned separately as an ingest source
    file_stats: list[tuple[Path, int, int, int, int]] = []
    sidecar_files: set[Path] = set()
    orphaned_dir = source / ORPHANED_DIR_NAME
    effective_root = walk_root if walk_root is not None else source

    for p in sorted(effective_root.glob("**/*")):
        if not p.is_file():
            continue
        if p == orphaned_dir or p.is_relative_to(orphaned_dir):
            continue
        if p.name == LOCK_FILENAME:
            continue
        if p.suffix == ".rscf":
            sidecar_files.add(p)
        else:
            st = p.stat()
            file_stats.append((p, st.st_size, st.st_mtime_ns, st.st_ctime_ns, st.st_ino))

    total = len(file_stats)
    for chunk_start in range(0, total, _COMMIT_CHUNK):
        chunk_end = min(chunk_start + _COMMIT_CHUNK, total)
        with db.batch():
            for pos in range(chunk_start, chunk_end):
                filepath, size, mtime_ns, ctime_ns, inode = file_stats[pos]
                file_num = pos + 1
                stats.files_total += 1
                path_str = str(filepath)

                # Stat-cache: skip if DB already has this file unchanged
                if not force_rescan and db.is_unchanged(path_str, size, mtime_ns, ctime_ns, inode):
                    stats.files_skipped += 1
                    continue

                sidecar_path = resolver.sidecar_path(filepath)

                if not force_rescan and sidecar_path in sidecar_files:
                    # Fast path: load from sidecar
                    try:
                        sidecar = read_sidecar(sidecar_path)

                        # Store container in scanned_files
                        db.upsert_scanned(
                            path=path_str,
                            size=size,
                            mtime_ns=mtime_ns,
                            ctime_ns=ctime_ns,
                            inode=inode,
                            source_type="romroot",
                            crc32="",
                            md5="",
                            sha1="",
                            sha256="",
                            blake3=sidecar.container_blake3,
                            is_archive=False,
                            scanned_at=now,
                        )

                        # Store file entries in archive_contents
                        # and populate romroot_files for match phase
                        rel = filepath.relative_to(source)
                        # Parent directory name — works for both flat and
                        # provider-based hierarchy (archive-mode games)
                        system = rel.parts[-2] if len(rel.parts) > 1 else ""

                        for entry in sidecar.files:
                            db.upsert_archive_content(
                                archive_path=path_str,
                                entry_name=entry.path,
                                entry_size=entry.size,
                                crc32=entry.crc32,
                                md5=entry.md5,
                                sha1=entry.sha1,
                                sha256=entry.sha256,
                                blake3=entry.blake3,
                            )

                            game_name = strip_archive_extension(filepath.name)
                            db.upsert_romroot(
                                path=path_str,
                                system=system,
                                game_name=game_name,
                                rom_name=entry.path,
                                crc32=entry.crc32,
                                md5=entry.md5,
                                sha1=entry.sha1,
                                sha256=entry.sha256,
                                blake3=entry.blake3,
                                rscf_path=str(sidecar_path),
                            )

                        stats.files_from_sidecar += 1
                        sidecar_files.discard(sidecar_path)
                        continue

                    except RscfError as e:
                        stats.warn(
                            f"corrupt sidecar {sidecar_path.name}: {e}"
                        )
                        # Fall through to hash

                # Slow path: hash the file
                print(
                    f"  [{file_num}/{total}] Hashing: {filepath.name} ({size:,} bytes)",
                    file=sys.stderr,
                )
                hashes = hash_file(filepath)

                db.upsert_scanned(
                    path=path_str,
                    size=size,
                    mtime_ns=mtime_ns,
                    ctime_ns=ctime_ns,
                    inode=inode,
                    source_type="romroot",
                    crc32=hashes.crc32,
                    md5=hashes.md5,
                    sha1=hashes.sha1,
                    sha256=hashes.sha256,
                    blake3=hashes.blake3,
                    is_archive=False,
                    scanned_at=now,
                )
                stats.files_hashed += 1

                if force_rescan:
                    # Rebuild sidecar (use already-collected stat)
                    new_sidecar = Sidecar(
                        container_blake3=hashes.blake3,
                        container_size=size,
                        container_mtime_ns=mtime_ns,
                        container_ctime_ns=ctime_ns,
                        container_inode=inode,
                        renderer="",
                        files=[
                            FileEntry.from_hashes(
                                path=filepath.name,
                                size=size,
                                hashes=hashes,
                            ),
                        ],
                    )
                    write_sidecar(new_sidecar, sidecar_path)
                    sidecar_files.discard(sidecar_path)

    # Orphaned sidecars: .rscf files with no corresponding source file
    for orphan in sidecar_files:
        expected_source = resolver.source_path(orphan)
        if not expected_source.exists():
            if force_rescan:
                orphan.unlink()
                stats.warn(f"deleted orphan sidecar: {orphan.name}")
            else:
                stats.warn(f"orphan sidecar (no source): {orphan.name}")

    return stats


def _writes_sidecars(source_type: str) -> bool:
    """Whether this source type writes RSCF sidecars alongside source files."""
    return source_type in ("ingest", "disposal")


def _write_plain_sidecar(
    filepath: Path, hashes, size: int, mtime_ns: int, ctime_ns: int, inode: int,
) -> None:
    """Write an RSCF sidecar for a plain (non-archive) source file."""
    resolver = SidecarResolver(StorageMode.IN_TREE)
    sc = Sidecar(
        container_blake3=hashes.blake3,
        container_size=size,
        container_mtime_ns=mtime_ns,
        container_ctime_ns=ctime_ns,
        container_inode=inode,
        renderer="",
        files=[
            FileEntry.from_hashes(
                path=filepath.name,
                size=size,
                hashes=hashes,
            ),
        ],
    )
    write_sidecar(sc, resolver.sidecar_path(filepath))


def _write_archive_sidecar(
    archive: Path, container_hashes, container_size: int,
    mtime_ns: int, ctime_ns: int, inode: int,
    entries: list[tuple[str, int, object]],
) -> None:
    """Write an RSCF sidecar for a source archive after extraction.

    Args:
        entries: List of (name, size, FileHashes) from extraction.
    """
    resolver = SidecarResolver(StorageMode.IN_TREE)
    sc = Sidecar(
        container_blake3=container_hashes.blake3,
        container_size=container_size,
        container_mtime_ns=mtime_ns,
        container_ctime_ns=ctime_ns,
        container_inode=inode,
        renderer="",
        files=[
            FileEntry.from_hashes(path=name, size=size, hashes=entry_hashes)
            for name, size, entry_hashes in entries
        ],
    )
    write_sidecar(sc, resolver.sidecar_path(archive))


def _load_plain_from_sidecar(
    filepath: Path,
    path_str: str,
    source_type: str,
    db: CacheDB,
    now: str,
    stats: SourceScanStats,
) -> bool:
    """Try to load a plain file's hashes from its RSCF sidecar.

    Returns True if sidecar was used, False if hashing is needed.
    """
    resolver = SidecarResolver(StorageMode.IN_TREE)
    sidecar_path = resolver.sidecar_path(filepath)
    if not sidecar_path.exists():
        return False

    try:
        sc = read_sidecar(sidecar_path)
    except RscfError:
        return False

    # Verify sidecar matches current file stat
    st = filepath.stat()
    if (sc.container_size != st.st_size
            or sc.container_mtime_ns != st.st_mtime_ns):
        return False

    db.upsert_scanned(
        path=path_str,
        size=st.st_size,
        mtime_ns=st.st_mtime_ns,
        ctime_ns=st.st_ctime_ns,
        inode=st.st_ino,
        source_type=source_type,
        crc32="",
        md5="",
        sha1="",
        sha256="",
        blake3=sc.container_blake3,
        is_archive=False,
        scanned_at=now,
    )
    stats.files_from_sidecar += 1
    return True


def _load_archive_from_sidecar(
    filepath: Path,
    path_str: str,
    source_type: str,
    db: CacheDB,
    now: str,
    stats: SourceScanStats,
) -> bool:
    """Try to load an archive's hashes from its RSCF sidecar.

    Recovers scanned_files + archive_contents from sidecar data
    without re-extracting the archive. Used when the stat-cache
    misses (e.g. after CIFS inode change) but the sidecar is intact.

    Returns True if sidecar was used, False if extraction is needed.
    """
    resolver = SidecarResolver(StorageMode.IN_TREE)
    sidecar_path = resolver.sidecar_path(filepath)
    if not sidecar_path.exists():
        return False

    try:
        sc = read_sidecar(sidecar_path)
    except RscfError:
        return False

    # Verify sidecar matches current file stat (size + mtime, not inode)
    st = filepath.stat()
    if (sc.container_size != st.st_size
            or sc.container_mtime_ns != st.st_mtime_ns):
        return False

    # Restore scanned_files entry with current inode
    db.upsert_scanned(
        path=path_str,
        size=st.st_size,
        mtime_ns=st.st_mtime_ns,
        ctime_ns=st.st_ctime_ns,
        inode=st.st_ino,
        source_type=source_type,
        crc32="",
        md5="",
        sha1="",
        sha256="",
        blake3=sc.container_blake3,
        is_archive=True,
        scanned_at=now,
    )

    # Restore archive_contents from sidecar file entries
    for entry in sc.files:
        db.upsert_archive_content(
            archive_path=path_str,
            entry_name=entry.path,
            entry_size=entry.size,
            crc32=entry.crc32,
            md5=entry.md5,
            sha1=entry.sha1,
            sha256=entry.sha256,
            blake3=entry.blake3,
        )

    stats.files_from_sidecar += 1
    stats.archive_entries_hashed += len(sc.files)
    return True


def _scan_untrusted(
    source: Path,
    source_type: str,
    db: CacheDB,
    work_dir: Path,
    *,
    walk_root: Path | None = None,
) -> SourceScanStats:
    """Scan an ingest, disposal, or readonly source — hash everything.

    Mid-download detection: stat before and after hashing. If the file changed
    during hashing, warn and skip (expected for actively downloading files).

    Archive cache: if an archive's path+size+mtime_ns+ctime_ns+inode match the
    DB and archive_contents already exist, skip re-extraction.

    Ingest/disposal sources write RSCF sidecars alongside files for faster
    re-scans.  Read-only sources never write sidecars.

    Args:
        walk_root: If set, restrict the walk to this subdirectory of source.
    """
    stats = SourceScanStats(source_type=source_type)
    limits = ExtractionLimits()
    now = datetime.now(timezone.utc).isoformat()
    writable = _writes_sidecars(source_type)

    # Single-pass walk: collect (path, size, mtime_ns, ctime_ns, inode) tuples
    file_stats: list[tuple[Path, int, int, int, int]] = []
    effective_root = walk_root if walk_root is not None else source
    for p in sorted(effective_root.glob("**/*")):
        if p.is_file() and _is_scannable(p):
            st = p.stat()
            file_stats.append((p, st.st_size, st.st_mtime_ns, st.st_ctime_ns, st.st_ino))

    total = len(file_stats)
    for chunk_start in range(0, total, _COMMIT_CHUNK):
        chunk_end = min(chunk_start + _COMMIT_CHUNK, total)
        with db.batch():
            for pos in range(chunk_start, chunk_end):
                filepath, size, mtime_ns, ctime_ns, inode = file_stats[pos]
                file_num = pos + 1
                stats.files_total += 1

                path_str = str(filepath)
                is_archive = _is_archive(filepath)

                # Archive cache check
                if is_archive:
                    if (db.is_unchanged(path_str, size, mtime_ns, ctime_ns, inode)
                            and db.has_archive_contents(path_str)):
                        stats.files_skipped += 1
                        continue

                    # Sidecar fast path for archives
                    if _load_archive_from_sidecar(filepath, path_str, source_type, db, now, stats):
                        continue
                else:
                    # Plain file cache check
                    if db.is_unchanged(path_str, size, mtime_ns, ctime_ns, inode):
                        stats.files_skipped += 1
                        continue

                    # Sidecar fast path for plain files (cold start recovery)
                    if _load_plain_from_sidecar(filepath, path_str, source_type, db, now, stats):
                        continue

                print(
                    f"  [{file_num}/{total}] Hashing: {filepath.name} ({size:,} bytes)",
                    file=sys.stderr,
                )
                hashes = hash_file(filepath)

                # Mid-download detection: stat after hashing, compare against
                # walk-collected values
                try:
                    post_st = filepath.stat()
                except OSError:
                    stats.warn(f"file vanished during scan: {filepath.name}")
                    continue

                if post_st.st_size != size or post_st.st_mtime_ns != mtime_ns:
                    stats.warn(
                        f"file changed during scan (mid-download?): "
                        f"{filepath.name}"
                    )
                    continue

                # Use post-hash stat for ctime/inode (most current)
                db.upsert_scanned(
                    path=path_str,
                    size=post_st.st_size,
                    mtime_ns=post_st.st_mtime_ns,
                    ctime_ns=post_st.st_ctime_ns,
                    inode=post_st.st_ino,
                    source_type=source_type,
                    crc32=hashes.crc32,
                    md5=hashes.md5,
                    sha1=hashes.sha1,
                    sha256=hashes.sha256,
                    blake3=hashes.blake3,
                    is_archive=is_archive,
                    scanned_at=now,
                )
                stats.files_hashed += 1

                if not is_archive:
                    # Write sidecar for plain files (ingest/disposal only)
                    if writable:
                        _write_plain_sidecar(
                            filepath, hashes,
                            post_st.st_size, post_st.st_mtime_ns,
                            post_st.st_ctime_ns, post_st.st_ino,
                        )
                    continue

                # Load archive contents: try sidecar fast path, fall back to extraction
                if _load_archive_contents_from_sidecar(filepath, path_str, db, stats):
                    pass  # loaded from sidecar
                else:
                    _extract_and_hash_archive(
                        filepath, path_str, db, work_dir, limits, stats,
                        write_sidecar_=writable,
                        container_hashes=hashes,
                        container_size=post_st.st_size,
                        container_mtime_ns=post_st.st_mtime_ns,
                        container_ctime_ns=post_st.st_ctime_ns,
                        container_inode=post_st.st_ino,
                    )

    return stats


def _load_archive_contents_from_sidecar(
    archive: Path,
    archive_path_str: str,
    db: CacheDB,
    stats: SourceScanStats,
) -> bool:
    """Try to load archive contents from an RSCF sidecar.

    If a sidecar exists alongside the archive and is valid, load the
    per-ROM hashes into archive_contents (same as _scan_romroot does).
    This avoids expensive extraction for archives in _orphaned/ or
    any ingest source where sidecars were previously written.

    Returns True if sidecar was used, False if extraction is needed.
    """
    sidecar_path = archive.parent / (archive.name + ".rscf")
    if not sidecar_path.exists():
        return False

    try:
        sc = read_sidecar(sidecar_path)
    except RscfError:
        return False

    # Clear stale entries and load from sidecar
    db.delete_archive_contents(archive_path_str)
    for entry in sc.files:
        db.upsert_archive_content(
            archive_path=archive_path_str,
            entry_name=entry.path,
            entry_size=entry.size,
            crc32=entry.crc32,
            md5=entry.md5,
            sha1=entry.sha1,
            sha256=entry.sha256,
            blake3=entry.blake3,
        )
    stats.files_from_sidecar += 1
    return True


def _extract_and_hash_archive(
    archive: Path,
    archive_path_str: str,
    db: CacheDB,
    work_dir: Path,
    limits: ExtractionLimits,
    stats: SourceScanStats,
    *,
    write_sidecar_: bool = False,
    container_hashes=None,
    container_size: int = 0,
    container_mtime_ns: int = 0,
    container_ctime_ns: int = 0,
    container_inode: int = 0,
) -> None:
    """Extract an archive to work_dir, hash all contents, store in DB.

    When write_sidecar_ is True (ingest/disposal sources), writes an RSCF
    sidecar alongside the archive containing all extracted entry hashes.
    """
    # Clear stale entries
    db.delete_archive_contents(archive_path_str)

    # Create per-archive subdirectory in work_dir
    archive_work = work_dir / f"_scan_{archive.stem}"
    archive_work.mkdir(parents=True, exist_ok=True)

    try:
        extracted = extract_recursive(archive, archive_work, limits)
        stats.archives_extracted += 1

        sidecar_entries: list[tuple[str, int, object]] = []

        for entry in extracted:
            entry_hashes = hash_file(entry.path)
            db.upsert_archive_content(
                archive_path=archive_path_str,
                entry_name=entry.original_name,
                entry_size=entry.size,
                crc32=entry_hashes.crc32,
                md5=entry_hashes.md5,
                sha1=entry_hashes.sha1,
                sha256=entry_hashes.sha256,
                blake3=entry_hashes.blake3,
            )
            stats.archive_entries_hashed += 1
            sidecar_entries.append((entry.original_name, entry.size, entry_hashes))

        if write_sidecar_ and container_hashes is not None and sidecar_entries:
            _write_archive_sidecar(
                archive, container_hashes, container_size,
                container_mtime_ns, container_ctime_ns, container_inode,
                sidecar_entries,
            )

    except Exception as e:
        stats.warn(f"extraction failed for {archive.name}: {e}")
    finally:
        if archive_work.exists():
            shutil.rmtree(archive_work, ignore_errors=True)
