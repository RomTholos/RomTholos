"""Phase 3: Execute — process match plan, write ROMs to romroot with RSCF.

Builds per-game archives incrementally. If a game has some ROMs already
collected and new ones become available, the archive is updated (append
or rebuild depending on the compression profile).

Configuration is authoritative: if the configured profile differs from
the stored sidecar.renderer, the archive is rebuilt/recompressed.

Strategies per profile type:
  append (7z, zip): add new files to existing archive
  rebuild (tar.zst, torrentzip): extract existing + add new → recompress all
  directory (none): copy individual files to game directory
  single_only (rvz): only handles single-ROM games
"""

from __future__ import annotations

import enum
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path

from rscf import (
    FileEntry,
    FileHashes,
    Sidecar,
    SidecarResolver,
    StorageMode,
    hash_file,
    read_sidecar,
    write_sidecar,
)
from rscf.sidecar import RscfError

from romtholos.collect.compress import (
    PROFILES,
    compress,
    dimg_header_matches_profile,
    dolphin_header_matches_profile,
    profile_extension,
    read_dimg_header,
    read_dolphin_header,
    strip_archive_extension,
)
from romtholos.collect.db import CacheDB
from romtholos.collect.extract import ExtractionLimits, ExtractedFile, extract_recursive
from romtholos.collect.match import GamePlan, MatchOp


class ExtractionCache:
    """Cache extracted archive contents across games within a system run.

    Avoids re-extracting the same source .7z for every partial game
    that shares tracks from it.
    """

    def __init__(self, cache_dir: Path, max_mb: int = 2048):
        self._cache_dir = cache_dir
        self._max_bytes = max_mb * 1024 * 1024
        self._entries: dict[str, list[ExtractedFile]] = {}
        self._sizes: dict[str, int] = {}
        self._current_bytes = 0

    def get_or_extract(
        self, source: Path, limits: ExtractionLimits,
    ) -> list[ExtractedFile]:
        """Return cached extraction or extract and cache.

        The quota is a soft limit — we always cache the current extraction
        (must be able to hold at least the largest single archive). Old
        entries are evicted to stay near the target.
        """
        key = str(source)
        if key in self._entries:
            return self._entries[key]

        subdir = self._cache_dir / f"_cache_{hash(key) & 0xFFFFFFFF:08x}"
        subdir.mkdir(parents=True, exist_ok=True)

        extracted = extract_recursive(source, subdir, limits)
        total_size = sum(e.size for e in extracted)

        # Evict oldest entries to make room (soft quota)
        while self._current_bytes + total_size > self._max_bytes and self._entries:
            evict_key = next(iter(self._entries))
            self._evict(evict_key)

        # Always cache — quota is a target, not a hard limit
        self._entries[key] = extracted
        self._sizes[key] = total_size
        self._current_bytes += total_size

        return extracted

    def _evict(self, key: str) -> None:
        """Remove a cached extraction."""
        size = self._sizes.pop(key, 0)
        self._current_bytes -= size
        entries = self._entries.pop(key, [])
        # Remove the cache subdir for this key
        if entries:
            parent = entries[0].path.parent
            if parent.exists() and parent.is_relative_to(self._cache_dir):
                shutil.rmtree(parent, ignore_errors=True)

    def find_file(
        self, source: str, hash_type: str, hash_value: str,
    ) -> Path | None:
        """Find a specific file from a cached extraction by hash.

        Hash is the only authoritative identifier — filenames inside
        archives can differ from DAT names.
        """
        entries = self._entries.get(source, [])
        if not entries:
            return None
        return _find_by_hash(
            [e.path for e in entries], hash_type, hash_value,
        )

    def cleanup(self) -> None:
        """Remove all cached extractions."""
        if self._cache_dir.exists():
            shutil.rmtree(self._cache_dir, ignore_errors=True)
        self._entries.clear()
        self._sizes.clear()
        self._current_bytes = 0


def _find_by_hash(
    candidates: list[Path], hash_type: str, hash_value: str,
) -> Path | None:
    """Find a file among candidates by hashing and comparing.

    Hash is the only authoritative identifier for ROM matching.
    """
    for path in candidates:
        if not path.exists():
            continue
        hashes = hash_file(path)
        actual = getattr(hashes, hash_type, "")
        if actual and actual.upper() == hash_value.upper():
            return path
    return None


def _ensure_dir(path: Path) -> Path:
    """Create directory if it doesn't exist."""
    path.mkdir(parents=True, exist_ok=True)
    return path


def _clean_dir(path: Path) -> None:
    """Remove all contents of a directory, recreate it empty."""
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _verify_on_target(target_path: Path, expected_blake3: str) -> bool:
    """Re-read a file from the target filesystem and verify BLAKE3."""
    if not target_path.exists():
        return False
    hashes = hash_file(target_path)
    return hashes.blake3.upper() == expected_blake3.upper()


def _get_rom_to_work_dir(
    op: MatchOp, work_dir: Path, limits: ExtractionLimits,
    cache: ExtractionCache | None = None,
) -> Path | None:
    """Get a ROM file into work_dir. Returns path or None on failure.

    All extraction lookups use hash matching — filenames inside archives
    can differ from DAT names and are not authoritative.

    When cache is provided, archive extractions are cached and the needed
    file is *copied* from cache to work_dir.
    """
    assert op.source_path is not None
    source = Path(op.source_path)

    if op.source_type == "plain":
        dest = work_dir / source.name
        shutil.copy2(source, dest)
        return dest

    if op.source_type == "archive":
        dest = work_dir / source.name
        shutil.copy2(source, dest)
        return dest

    if op.source_type in ("archive_content", "romroot"):
        if cache is not None:
            extracted = cache.get_or_extract(source, limits)
            # Hash-match directly against extracted files
            matched = _find_by_hash(
                [e.path for e in extracted], op.hash_type, op.hash_value,
            )
            if matched is not None and matched.exists():
                dest = work_dir / matched.name
                shutil.copy2(matched, dest)
                return dest
            return None

        extracted = extract_recursive(source, work_dir, limits)
        return _find_by_hash(
            [e.path for e in extracted], op.hash_type, op.hash_value,
        )

    return None


def _archive_strategy(profile_name: str) -> str:
    """Determine strategy for building/updating game archives.

    Returns:
        'append': can add files to existing archive (7z, zip)
        'rebuild': must extract all + new, recompress (tar.zst, torrentzip)
        'single_only': profile only supports single-file games (rvz)
        'directory': files placed individually in game dir (none)
    """
    if profile_name == "none":
        return "directory"

    profile = PROFILES[profile_name]

    if not profile["multi_file"]:
        return "single_only"

    if profile["append_function"] is not None:
        return "append"

    return "rebuild"


def _source_matches_dimg_profile(
    source_path: Path,
    compression_profile: str,
) -> bool:
    """Check if a source .aaru already matches the target dimg profile.

    Returns True if the source can be copied as-is to romroot.
    Returns False on any error (safe fallback to extract+recompress).
    """
    from romtholos.collect.compress import CompressionError

    try:
        header = read_dimg_header(source_path)
        return dimg_header_matches_profile(header, compression_profile)
    except (CompressionError, Exception):
        return False


def _copy_sbi_to_work_dir(
    game_name: str,
    work_dir: Path,
    sbi_dir: Path | None,
) -> None:
    """Copy SBI file for a PS1 game to work_dir if available.

    SBI files use Redump naming convention: exact game name + ".sbi".
    dimg-tool auto-detects SBI files next to the CUE file.
    """
    if sbi_dir is None or not sbi_dir.is_dir():
        return

    sbi_name = game_name + ".sbi"
    sbi_path = sbi_dir / sbi_name
    if sbi_path.is_file():
        dest = work_dir / sbi_name
        shutil.copy2(sbi_path, dest)


def _profile_requires_all_tracks(compression_profile: str) -> bool:
    """Check if a profile requires all tracks present (no partial games).

    Aaru profiles need complete CUE/BIN sets — can't compress partial games.
    """
    return compression_profile.startswith("aaru-")


def _detect_media_type(game: GamePlan) -> str:
    """Detect media type from a game's ROM file extensions.

    Returns:
        'gdi': GD-ROM (Dreamcast .gdi format)
        'cd': CD disc image (CUE/BIN)
        'dvd': DVD disc image (single ISO)
        'rom': Everything else (cartridge ROMs, plain files)
    """
    extensions = {Path(op.rom_name).suffix.lower() for op in game.ops}
    if ".gdi" in extensions:
        return "gdi"
    if ".cue" in extensions:
        return "cd"
    if ".iso" in extensions:
        return "dvd"
    return "rom"


def _source_matches_dolphin_profile(
    source_path: Path,
    compression_profile: str,
) -> bool:
    """Check if a source disc image already matches the target dolphin profile.

    Returns True if the source can be copied as-is to romroot.
    Returns False on any error (safe fallback to extract+recompress).
    """
    from romtholos.collect.compress import CompressionError

    try:
        header = read_dolphin_header(source_path)
        return dolphin_header_matches_profile(header, compression_profile)
    except (CompressionError, Exception):
        return False


# ---------------------------------------------------------------------------
# Archive discovery
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ExistingArchive:
    """An archive (or game directory) found on the target filesystem."""

    path: Path
    """Archive file or game directory."""

    sidecar: Sidecar
    """Parsed sidecar content."""

    rscf_path: Path
    """Sidecar file path."""

    is_directory: bool
    """True for 'none' profile game directories."""


def _find_existing_archive(
    target_dir: Path,
    game_name: str,
    resolver: SidecarResolver,
) -> ExistingArchive | None:
    """Scan target_dir for an existing archive (any extension) for game_name.

    Checks files where strip_archive_extension(name) == game_name, plus
    a game directory (none profile). Returns first with a valid sidecar.
    """
    # Check archive files
    if target_dir.is_dir():
        for f in target_dir.iterdir():
            if not f.is_file() or f.suffix == ".rscf":
                continue
            if strip_archive_extension(f.name) == game_name:
                rscf_path = resolver.sidecar_path(f)
                if rscf_path.exists():
                    try:
                        sidecar = read_sidecar(rscf_path)
                        return ExistingArchive(
                            path=f,
                            sidecar=sidecar,
                            rscf_path=rscf_path,
                            is_directory=False,
                        )
                    except RscfError:
                        pass  # corrupt sidecar, skip

    # Check game directory (none profile)
    game_dir = target_dir / game_name
    if game_dir.is_dir():
        # Collect per-file sidecars into a synthetic Sidecar
        entries: list[FileEntry] = []
        for rom_file in sorted(game_dir.iterdir()):
            if not rom_file.is_file() or rom_file.suffix == ".rscf":
                continue
            rscf_path = resolver.sidecar_path(rom_file)
            if rscf_path.exists():
                try:
                    sc = read_sidecar(rscf_path)
                    entries.extend(sc.files)
                except RscfError:
                    pass

        if entries:
            # Build synthetic sidecar representing the directory
            synthetic = Sidecar(
                container_blake3="",
                container_size=0,
                container_mtime_ns=0,
                container_ctime_ns=0,
                container_inode=0,
                renderer="none",
                files=entries,
            )
            return ExistingArchive(
                path=game_dir,
                sidecar=synthetic,
                rscf_path=game_dir,  # placeholder
                is_directory=True,
            )

    return None


# ---------------------------------------------------------------------------
# Action determination
# ---------------------------------------------------------------------------

class GameAction(enum.Enum):
    SKIP = "skip"
    CREATE = "create"
    APPEND = "append"
    REBUILD = "rebuild"
    RECOMPRESS = "recompress"
    NOTHING = "nothing"


def _determine_action(
    existing: ExistingArchive | None,
    new_ops: list[MatchOp],
    compression_profile: str,
    strategy: str,
) -> GameAction:
    """Decide what to do for a game based on existing state and new ops."""
    if existing is None:
        if new_ops:
            return GameAction.CREATE
        return GameAction.NOTHING

    # Existing archive/directory found
    profile_matches = existing.sidecar.renderer == compression_profile

    if profile_matches:
        if not new_ops:
            return GameAction.SKIP

        if strategy == "append":
            # Check if extension would change (zstd single→multi)
            old_count = len(existing.sidecar.files)
            new_count = old_count + len(new_ops)
            old_ext = profile_extension(compression_profile, old_count)
            new_ext = profile_extension(compression_profile, new_count)
            if old_ext != new_ext:
                return GameAction.REBUILD
            return GameAction.APPEND

        return GameAction.REBUILD

    # Profile mismatch — config is authoritative
    if new_ops:
        return GameAction.REBUILD
    return GameAction.RECOMPRESS


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def execute_plan(
    game_plans: list[GamePlan],
    romroot: Path,
    work_dir: Path,
    compression_profile: str,
    db: CacheDB,
    source_modes: dict[str, str] | None = None,
    romroot_overrides: dict[str, Path] | None = None,
    sbi_dir: Path | None = None,
    verify_roundtrip: bool = False,
    limit: int = 0,
    partial_fallback: str = "",
    partial_min_ratio: float = 0.0,
    extraction_cache_mb: int = 2048,
    compression_map: dict[str, str] | None = None,
) -> dict[str, int]:
    """Execute the match plan — build per-game archives in romroot.

    Processes collectable and partial games (collects available ROMs).
    Complete games with wrong profile are recompressed.
    Unavailable games are skipped.

    Args:
        game_plans: List of GamePlan from match phase.
        romroot: Default romroot path.
        work_dir: Fast local work directory (SSD).
        compression_profile: Default compression profile name.
        db: Cache database.
        source_modes: Dict of source_path -> source_type.
        romroot_overrides: Per-system romroot path overrides.
        sbi_dir: Directory containing SBI files for PS1 LibCrypt games.
        verify_roundtrip: If True, pass verify=True to dimg-tool convert.
        limit: Stop after processing this many games (0 = no limit).
        partial_fallback: Profile to use for partial games when the main
            profile requires all tracks (e.g. aaru). Empty = skip.
        partial_min_ratio: Minimum fraction of matched ROMs for a partial
            game to be worth collecting. 0.0 = collect everything.
        extraction_cache_mb: Maximum size of extraction cache in MiB.
        compression_map: Media-type overrides (e.g. {"cd": "aaru-ps2cd-zstd"}).

    Returns:
        Dict with counts: processed, skipped, failed, missing.
    """
    if source_modes is None:
        source_modes = {}
    if romroot_overrides is None:
        romroot_overrides = {}
    if compression_map is None:
        compression_map = {}

    resolver = SidecarResolver(StorageMode.IN_TREE)
    limits = ExtractionLimits()

    stats = {"processed": 0, "skipped": 0, "failed": 0, "missing": 0}

    _ensure_dir(work_dir)

    cache_dir = work_dir / "_extraction_cache"
    cache = ExtractionCache(cache_dir, max_mb=extraction_cache_mb)
    _ensure_dir(cache_dir)

    try:
        for game in sorted(game_plans, key=lambda g: (g.system, g.game_name)):
            # --limit N: stop after N games processed
            if limit and stats["processed"] >= limit:
                break

            if game.status == "unavailable":
                stats["missing"] += len(game.ops)
                continue

            # Resolve compression profile per game.
            # Media-type override (compression_map) takes priority,
            # then falls back to system-level compression_profile.
            media_type = _detect_media_type(game)
            game_profile = compression_map.get(media_type, compression_profile)
            game_strategy = _archive_strategy(game_profile)

            # Aaru profiles require all tracks — partial games use fallback.
            effective_profile = game_profile
            effective_strategy = game_strategy
            if (_profile_requires_all_tracks(game_profile)
                    and game.status == "partial"):
                if partial_fallback and partial_fallback in PROFILES:
                    # Check minimum completeness threshold
                    available = len(game.new_ops) + len(game.existing_ops)
                    total = len(game.ops)
                    ratio = available / total if total > 0 else 0.0
                    if ratio < partial_min_ratio:
                        stats["missing"] += len(game.missing_ops)
                        continue

                    effective_profile = partial_fallback
                    effective_strategy = _archive_strategy(effective_profile)
                    print(
                        f"  Partial: {game.system}/{game.game_name} "
                        f"({available}/{total} ROMs, "
                        f"using fallback {effective_profile})",
                        file=sys.stderr,
                    )
                else:
                    stats["missing"] += len(game.missing_ops)
                    continue

            target_root = romroot_overrides.get(game.system, romroot)
            target_dir = _ensure_dir(target_root / game.system)

            new_ops = game.new_ops

            # Find existing archive (any extension/profile)
            existing = _find_existing_archive(target_dir, game.game_name, resolver)

            action = _determine_action(
                existing, new_ops, effective_profile, effective_strategy,
            )

            if action == GameAction.NOTHING:
                stats["missing"] += len(game.ops)
                continue

            if action == GameAction.SKIP:
                stats["skipped"] += len(game.ops)
                continue

            # Multi-ROM game with single_only profile
            if effective_strategy == "single_only" and len(game.ops) > 1:
                print(
                    f"  Skipping: {game.game_name} "
                    f"(multi-ROM, profile {effective_profile} is single-file only)",
                    file=sys.stderr,
                )
                stats["failed"] += len(new_ops)
                continue

            if action == GameAction.RECOMPRESS:
                print(
                    f"  Recompressing: {game.system}/{game.game_name} "
                    f"({existing.sidecar.renderer} → {effective_profile})",
                    file=sys.stderr,
                )
            else:
                print(
                    f"  Collecting: {game.system}/{game.game_name} "
                    f"({len(new_ops)} new ROM(s), action={action.value})",
                    file=sys.stderr,
                )

            try:
                if effective_strategy == "directory" and action != GameAction.RECOMPRESS:
                    if existing and not existing.is_directory:
                        _handle_archive_to_directory(
                            game, new_ops, existing, target_dir, work_dir,
                            resolver, limits, db, stats,
                        )
                    else:
                        _execute_directory(
                            game, new_ops, target_dir, work_dir,
                            resolver, limits, db, stats, cache=cache,
                        )
                elif action == GameAction.RECOMPRESS and effective_strategy == "directory":
                    assert existing is not None
                    _handle_archive_to_directory(
                        game, [], existing, target_dir, work_dir,
                        resolver, limits, db, stats,
                    )
                elif existing and existing.is_directory and effective_strategy != "directory":
                    _handle_directory_to_archive(
                        game, new_ops, existing, target_dir, work_dir,
                        effective_profile, resolver, limits, db, stats,
                    )
                else:
                    _execute_archive(
                        game, new_ops, existing, target_dir, work_dir,
                        effective_profile, effective_strategy, action, resolver,
                        limits, db, stats,
                        sbi_dir=sbi_dir,
                        verify_roundtrip=verify_roundtrip,
                        cache=cache,
                    )
            except Exception as e:
                print(
                    f"    Error processing {game.game_name}: {e}",
                    file=sys.stderr,
                )
                stats["failed"] += len(new_ops) or len(game.ops)
            finally:
                # Clean per-game work subdirs, but preserve the cache
                for subdir_name in ("_roms", "_compressed", "_existing", "_extract"):
                    subdir = work_dir / subdir_name
                    if subdir.exists():
                        shutil.rmtree(subdir)
    finally:
        cache.cleanup()

    return stats


# ---------------------------------------------------------------------------
# Directory mode (none profile)
# ---------------------------------------------------------------------------

def _execute_directory(
    game: GamePlan,
    new_ops: list[MatchOp],
    target_dir: Path,
    work_dir: Path,
    resolver: SidecarResolver,
    limits: ExtractionLimits,
    db: CacheDB,
    stats: dict[str, int],
    cache: ExtractionCache | None = None,
) -> None:
    """Execute 'none' profile: copy individual files to game directory."""
    game_dir = _ensure_dir(target_dir / game.game_name)

    for op in new_ops:
        assert op.source_path is not None

        rom_in_work = _get_rom_to_work_dir(op, work_dir, limits, cache=cache)
        if rom_in_work is None or not rom_in_work.exists():
            print(f"    Failed: could not get {op.rom_name}", file=sys.stderr)
            stats["failed"] += 1
            continue

        rom_hashes = hash_file(rom_in_work)
        rom_size = rom_in_work.stat().st_size

        if op.hash_value:
            actual = getattr(rom_hashes, op.hash_type, "")
            if actual.upper() != op.hash_value.upper():
                print(
                    f"    Failed: hash mismatch for {op.rom_name}",
                    file=sys.stderr,
                )
                stats["failed"] += 1
                continue

        # Copy to game directory
        target_file = game_dir / Path(op.rom_name).name
        shutil.copy2(rom_in_work, target_file)

        # Verify
        if not _verify_on_target(target_file, rom_hashes.blake3):
            print(
                f"    Failed: verification failed for {op.rom_name}",
                file=sys.stderr,
            )
            target_file.unlink(missing_ok=True)
            stats["failed"] += 1
            continue

        # Per-file RSCF sidecar
        target_stat = target_file.stat()
        sidecar = Sidecar(
            container_blake3=rom_hashes.blake3,
            container_size=target_stat.st_size,
            container_mtime_ns=target_stat.st_mtime_ns,
            container_ctime_ns=target_stat.st_ctime_ns,
            container_inode=target_stat.st_ino,
            renderer="none",
            files=[
                FileEntry.from_hashes(
                    path=op.rom_name,
                    size=rom_size,
                    hashes=rom_hashes,
                ),
            ],
        )
        rscf_path = resolver.sidecar_path(target_file)
        write_sidecar(sidecar, rscf_path)

        db.upsert_romroot(
            path=str(target_file),
            system=game.system,
            game_name=game.game_name,
            rom_name=op.rom_name,
            crc32=rom_hashes.crc32,
            md5=rom_hashes.md5,
            sha1=rom_hashes.sha1,
            sha256=rom_hashes.sha256,
            blake3=rom_hashes.blake3,
            rscf_path=str(rscf_path),
        )
        stats["processed"] += 1


# ---------------------------------------------------------------------------
# Profile transitions
# ---------------------------------------------------------------------------

def _handle_archive_to_directory(
    game: GamePlan,
    new_ops: list[MatchOp],
    existing: ExistingArchive,
    target_dir: Path,
    work_dir: Path,
    resolver: SidecarResolver,
    limits: ExtractionLimits,
    db: CacheDB,
    stats: dict[str, int],
) -> None:
    """Transition from archive → directory (none) profile."""
    assert not existing.is_directory

    extract_dir = _ensure_dir(work_dir / "_extract")
    extracted = extract_recursive(existing.path, extract_dir, limits)

    game_dir = _ensure_dir(target_dir / game.game_name)

    # Place extracted files in game dir with per-file sidecars
    for ef in extracted:
        # Match to a sidecar entry for hashes
        entry = next(
            (e for e in existing.sidecar.files if Path(e.path).name == ef.original_name),
            None,
        )

        target_file = game_dir / ef.original_name
        shutil.copy2(ef.path, target_file)

        rom_hashes = hash_file(target_file)
        target_stat = target_file.stat()

        rom_name = entry.path if entry else ef.original_name
        sidecar = Sidecar(
            container_blake3=rom_hashes.blake3,
            container_size=target_stat.st_size,
            container_mtime_ns=target_stat.st_mtime_ns,
            container_ctime_ns=target_stat.st_ctime_ns,
            container_inode=target_stat.st_ino,
            renderer="none",
            files=[
                FileEntry.from_hashes(
                    path=rom_name,
                    size=target_stat.st_size,
                    hashes=rom_hashes,
                ),
            ],
        )
        rscf_path = resolver.sidecar_path(target_file)
        write_sidecar(sidecar, rscf_path)

        db.upsert_romroot(
            path=str(target_file),
            system=game.system,
            game_name=game.game_name,
            rom_name=rom_name,
            crc32=rom_hashes.crc32,
            md5=rom_hashes.md5,
            sha1=rom_hashes.sha1,
            sha256=rom_hashes.sha256,
            blake3=rom_hashes.blake3,
            rscf_path=str(rscf_path),
        )

    # Handle new ops too (if any)
    if new_ops:
        _execute_directory(
            game, new_ops, target_dir, work_dir,
            resolver, limits, db, stats,
        )

    # Cleanup old archive + sidecar
    _cleanup_old(existing.path, existing.rscf_path, db)

    stats["processed"] += len(extracted)


def _handle_directory_to_archive(
    game: GamePlan,
    new_ops: list[MatchOp],
    existing: ExistingArchive,
    target_dir: Path,
    work_dir: Path,
    compression_profile: str,
    resolver: SidecarResolver,
    limits: ExtractionLimits,
    db: CacheDB,
    stats: dict[str, int],
) -> None:
    """Transition from directory (none) → archive profile."""
    assert existing.is_directory

    rom_work = _ensure_dir(work_dir / "_roms")
    compress_dir = _ensure_dir(work_dir / "_compressed")

    # Copy existing files from game dir to rom_work
    existing_entries: list[FileEntry] = []
    game_dir = existing.path
    for rom_file in sorted(game_dir.iterdir()):
        if not rom_file.is_file() or rom_file.suffix == ".rscf":
            continue
        # Find matching entry for proper naming
        entry = next(
            (e for e in existing.sidecar.files if Path(e.path).name == rom_file.name),
            None,
        )
        dat_name = Path(entry.path).name if entry else rom_file.name
        dest = rom_work / dat_name
        shutil.copy2(rom_file, dest)
        if entry:
            existing_entries.append(entry)

    # Get new ROMs
    verified_roms: list[tuple[MatchOp, Path, FileHashes, int]] = []
    for op in new_ops:
        assert op.source_path is not None
        rom_in_work = _get_rom_to_work_dir(op, rom_work, limits)  # no cache for transitions
        if rom_in_work is None or not rom_in_work.exists():
            print(f"    Failed: could not get {op.rom_name}", file=sys.stderr)
            stats["failed"] += 1
            return

        rom_hashes = hash_file(rom_in_work)
        rom_size = rom_in_work.stat().st_size

        if op.hash_value:
            actual = getattr(rom_hashes, op.hash_type, "")
            if actual.upper() != op.hash_value.upper():
                print(f"    Failed: hash mismatch for {op.rom_name}", file=sys.stderr)
                stats["failed"] += 1
                return

        dat_name = Path(op.rom_name).name
        final_path = rom_work / dat_name
        if final_path != rom_in_work:
            if final_path.exists():
                final_path.unlink()
            rom_in_work.rename(final_path)

        verified_roms.append((op, final_path, rom_hashes, rom_size))

    # Compress all ROMs
    all_roms = sorted(rom_work.iterdir())
    compress_base = compress_dir / game.game_name
    result = compress(compression_profile, all_roms, compress_base)
    archive_in_work = result.output

    compressed_hashes = hash_file(archive_in_work)

    # Determine target path
    ext = profile_extension(compression_profile, len(all_roms))
    archive_name = game.game_name + ext
    target_archive = target_dir / archive_name

    shutil.copy2(archive_in_work, target_archive)

    if not _verify_on_target(target_archive, compressed_hashes.blake3):
        print(f"    Failed: target verification for {archive_name}", file=sys.stderr)
        target_archive.unlink(missing_ok=True)
        stats["failed"] += len(new_ops) + len(existing_entries)
        return

    # Build sidecar with all entries
    new_entries = [
        FileEntry.from_hashes(path=op.rom_name, size=rom_size, hashes=rom_hashes)
        for op, _, rom_hashes, rom_size in verified_roms
    ]
    new_rom_names = {Path(op.rom_name).name for op, _, _, _ in verified_roms}
    merged = [e for e in existing_entries if Path(e.path).name not in new_rom_names] + new_entries

    target_stat = target_archive.stat()
    sidecar = Sidecar(
        container_blake3=compressed_hashes.blake3,
        container_size=target_stat.st_size,
        container_mtime_ns=target_stat.st_mtime_ns,
        container_ctime_ns=target_stat.st_ctime_ns,
        container_inode=target_stat.st_ino,
        renderer=compression_profile,
        files=merged,
    )
    rscf_path = resolver.sidecar_path(target_archive)
    write_sidecar(sidecar, rscf_path)

    # Update DB
    for rom_file in game_dir.iterdir():
        if rom_file.is_file() and rom_file.suffix != ".rscf":
            db.delete_romroot_entries(str(rom_file))

    for entry in merged:
        db.upsert_romroot(
            path=str(target_archive),
            system=game.system,
            game_name=game.game_name,
            rom_name=entry.path,
            crc32=entry.crc32,
            md5=entry.md5,
            sha1=entry.sha1,
            sha256=entry.sha256,
            blake3=entry.blake3,
            rscf_path=str(rscf_path),
        )

    # Remove old game directory
    shutil.rmtree(game_dir)

    stats["processed"] += len(merged)


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def _cleanup_old(old_path: Path, old_rscf: Path, db: CacheDB) -> None:
    """Remove an old archive/file and its sidecar after replacement."""
    if old_path.is_dir():
        # Directory mode: delete per-file DB entries and rmtree
        for f in old_path.iterdir():
            if f.is_file() and f.suffix != ".rscf":
                db.delete_romroot_entries(str(f))
        shutil.rmtree(old_path)
    else:
        db.delete_romroot_entries(str(old_path))
        old_path.unlink(missing_ok=True)
        old_rscf.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Dolphin copy-as-is optimization
# ---------------------------------------------------------------------------

def _execute_dolphin_copy(
    game: GamePlan,
    op: MatchOp,
    target_dir: Path,
    compression_profile: str,
    resolver: SidecarResolver,
    db: CacheDB,
    stats: dict[str, int],
) -> bool:
    """Try to copy a source RVZ/GCZ/WIA as-is if it matches the target profile.

    Returns True if the copy succeeded, False to fall back to standard flow.
    """
    assert op.source_path is not None
    source = Path(op.source_path)

    if not _source_matches_dolphin_profile(source, compression_profile):
        return False

    # Get all 5 hashes of the inner ISO from archive_contents
    assert op.archive_entry is not None
    rows = db.find_archive_content_by_hash(op.hash_type, op.hash_value)
    if not rows:
        return False

    content_row = next(
        (r for r in rows
         if r["archive_path"] == op.source_path
         and r["entry_name"] == op.archive_entry),
        None,
    )
    if content_row is None:
        return False

    # Determine target path
    ext = profile_extension(compression_profile, 1)
    archive_name = game.game_name + ext
    target_archive = target_dir / archive_name

    # Copy source directly to target
    shutil.copy2(source, target_archive)

    # Hash the copy for container verification
    compressed_hashes = hash_file(target_archive)

    if not _verify_on_target(target_archive, compressed_hashes.blake3):
        target_archive.unlink(missing_ok=True)
        return False

    # Build sidecar with ISO hashes from archive_contents
    target_stat = target_archive.stat()
    iso_entry = FileEntry(
        path=op.rom_name,
        size=content_row["entry_size"] or 0,
        crc32=content_row["crc32"] or "",
        md5=content_row["md5"] or "",
        sha1=content_row["sha1"] or "",
        sha256=content_row["sha256"] or "",
        blake3=content_row["blake3"] or "",
    )
    sidecar = Sidecar(
        container_blake3=compressed_hashes.blake3,
        container_size=target_stat.st_size,
        container_mtime_ns=target_stat.st_mtime_ns,
        container_ctime_ns=target_stat.st_ctime_ns,
        container_inode=target_stat.st_ino,
        renderer=compression_profile,
        files=[iso_entry],
    )
    rscf_path = resolver.sidecar_path(target_archive)
    write_sidecar(sidecar, rscf_path)

    db.upsert_romroot(
        path=str(target_archive),
        system=game.system,
        game_name=game.game_name,
        rom_name=op.rom_name,
        crc32=iso_entry.crc32,
        md5=iso_entry.md5,
        sha1=iso_entry.sha1,
        sha256=iso_entry.sha256,
        blake3=iso_entry.blake3,
        rscf_path=str(rscf_path),
    )
    stats["processed"] += 1
    return True


# ---------------------------------------------------------------------------
# Aaru (.aaru) copy-as-is optimization
# ---------------------------------------------------------------------------

def _execute_dimg_copy(
    game: GamePlan,
    op: MatchOp,
    target_dir: Path,
    compression_profile: str,
    resolver: SidecarResolver,
    db: CacheDB,
    stats: dict[str, int],
) -> bool:
    """Try to copy a source .aaru as-is if it matches the target profile.

    Returns True if the copy succeeded, False to fall back to standard flow.
    """
    assert op.source_path is not None
    source = Path(op.source_path)

    if not _source_matches_dimg_profile(source, compression_profile):
        return False

    # Get all content hashes from archive_contents
    assert op.archive_entry is not None
    rows = db.find_archive_content_by_hash(op.hash_type, op.hash_value)
    if not rows:
        return False

    # Find the specific source row
    content_row = next(
        (r for r in rows
         if r["archive_path"] == op.source_path
         and r["entry_name"] == op.archive_entry),
        None,
    )
    if content_row is None:
        return False

    # Determine target path
    ext = profile_extension(compression_profile, 1)
    archive_name = game.game_name + ext
    target_archive = target_dir / archive_name

    # Copy source directly to target
    shutil.copy2(source, target_archive)

    # Hash the copy for container verification
    compressed_hashes = hash_file(target_archive)

    if not _verify_on_target(target_archive, compressed_hashes.blake3):
        target_archive.unlink(missing_ok=True)
        return False

    # Build sidecar with all track hashes from archive_contents
    # Get all entries for this source archive
    all_entries = db.get_archive_contents(op.source_path)
    file_entries = [
        FileEntry(
            path=row["entry_name"],
            size=row["entry_size"] or 0,
            crc32=row["crc32"] or "",
            md5=row["md5"] or "",
            sha1=row["sha1"] or "",
            sha256=row["sha256"] or "",
            blake3=row["blake3"] or "",
        )
        for row in all_entries
    ]

    target_stat = target_archive.stat()
    sidecar = Sidecar(
        container_blake3=compressed_hashes.blake3,
        container_size=target_stat.st_size,
        container_mtime_ns=target_stat.st_mtime_ns,
        container_ctime_ns=target_stat.st_ctime_ns,
        container_inode=target_stat.st_ino,
        renderer=compression_profile,
        files=file_entries,
    )
    rscf_path = resolver.sidecar_path(target_archive)
    write_sidecar(sidecar, rscf_path)

    for entry in file_entries:
        db.upsert_romroot(
            path=str(target_archive),
            system=game.system,
            game_name=game.game_name,
            rom_name=entry.path,
            crc32=entry.crc32,
            md5=entry.md5,
            sha1=entry.sha1,
            sha256=entry.sha256,
            blake3=entry.blake3,
            rscf_path=str(rscf_path),
        )
    stats["processed"] += 1
    return True


# ---------------------------------------------------------------------------
# Archive mode
# ---------------------------------------------------------------------------

def _execute_archive(
    game: GamePlan,
    new_ops: list[MatchOp],
    existing: ExistingArchive | None,
    target_dir: Path,
    work_dir: Path,
    compression_profile: str,
    strategy: str,
    action: GameAction,
    resolver: SidecarResolver,
    limits: ExtractionLimits,
    db: CacheDB,
    stats: dict[str, int],
    sbi_dir: Path | None = None,
    verify_roundtrip: bool = False,
    cache: ExtractionCache | None = None,
) -> None:
    """Execute archive profiles: build/update per-game archive."""
    profile = PROFILES[compression_profile]
    rom_work = _ensure_dir(work_dir / "_roms")
    compress_dir = _ensure_dir(work_dir / "_compressed")

    # Step 1: Get all ROMs into work_dir, hash and verify.
    #
    # For aaru profiles, ALL tracks must be physically present (CUE is a
    # manifest). This includes ops already "in_romroot" under a different
    # game — we need our own copy in the work directory.
    requires_all = _profile_requires_all_tracks(compression_profile)
    ops_to_fetch = list(new_ops)
    if requires_all:
        # Also fetch in_romroot ops that aren't new (shared tracks etc.)
        for op in game.existing_ops:
            if not any(n.rom_name == op.rom_name for n in new_ops):
                ops_to_fetch.append(op)

    verified_roms: list[tuple[MatchOp, Path, FileHashes, int]] = []

    for op in ops_to_fetch:
        assert op.source_path is not None

        rom_in_work = _get_rom_to_work_dir(op, rom_work, limits, cache=cache)
        if rom_in_work is None or not rom_in_work.exists():
            print(f"    Failed: could not get {op.rom_name}", file=sys.stderr)
            stats["failed"] += 1
            return  # fail entire game — archive must be consistent

        rom_hashes = hash_file(rom_in_work)
        rom_size = rom_in_work.stat().st_size

        if op.hash_value:
            actual = getattr(rom_hashes, op.hash_type, "")
            if actual.upper() != op.hash_value.upper():
                print(
                    f"    Failed: hash mismatch for {op.rom_name}",
                    file=sys.stderr,
                )
                stats["failed"] += 1
                return

        # Rename to DAT-specified name for archive entry naming
        dat_name = Path(op.rom_name).name
        final_path = rom_work / dat_name
        if final_path != rom_in_work:
            if final_path.exists():
                final_path.unlink()
            rom_in_work.rename(final_path)

        verified_roms.append((op, final_path, rom_hashes, rom_size))

    # Step 2: Collect existing FileEntry data from sidecar
    existing_entries: list[FileEntry] = []
    if existing is not None:
        existing_entries = list(existing.sidecar.files)

    # Step 3: Determine total file count and target extension
    total_count = len(existing_entries) + len(verified_roms)
    if total_count == 0:
        # Recompress with zero files shouldn't happen, but guard
        stats["skipped"] += len(game.ops)
        return

    ext = profile_extension(compression_profile, total_count)
    archive_name = game.game_name + ext
    target_archive = target_dir / archive_name

    # Step 4: Build or update the archive
    new_inputs = [path for _, path, _, _ in verified_roms]

    # Copy-as-is optimization: if source disc image already matches
    # the target profile, copy directly instead of extract+recompress.
    if (action == GameAction.CREATE
            and len(new_ops) == 1
            and new_ops[0].source_type == "archive_content"):
        # Dolphin (RVZ/GCZ/WIA)
        if strategy == "single_only" and _execute_dolphin_copy(
            game, new_ops[0], target_dir, compression_profile,
            resolver, db, stats,
        ):
            return
        # Aaru (.aaru)
        if compression_profile.startswith("aaru-") and _execute_dimg_copy(
            game, new_ops[0], target_dir, compression_profile,
            resolver, db, stats,
        ):
            return

    if action == GameAction.CREATE:
        # Copy SBI for PS1 aaru profiles before compression
        if compression_profile.startswith("aaru-ps1"):
            _copy_sbi_to_work_dir(game.game_name, rom_work, sbi_dir)

        compress_base = compress_dir / game.game_name
        # Pass verify flag for aaru profiles
        if compression_profile.startswith("aaru-") and verify_roundtrip:
            profile_kwargs = dict(profile["kwargs"])
            profile_kwargs["verify"] = True
            func = profile["function"]
            result = func(new_inputs, compress_base, **profile_kwargs)
        else:
            result = compress(compression_profile, new_inputs, compress_base)
        archive_in_work = result.output

    elif action == GameAction.APPEND:
        assert existing is not None
        archive_in_work = compress_dir / existing.path.name
        shutil.copy2(existing.path, archive_in_work)
        append_fn = profile["append_function"]
        append_kwargs = profile["append_kwargs"]
        append_fn(archive_in_work, new_inputs, **append_kwargs)

    elif action in (GameAction.REBUILD, GameAction.RECOMPRESS):
        # Extract existing ROMs if we have an existing archive
        if existing is not None and not existing.is_directory:
            existing_work = _ensure_dir(work_dir / "_existing")
            extracted = extract_recursive(existing.path, existing_work, limits)

            # Map extracted files to sidecar entry names.
            # Needed for single-file .zst where extracted name (stem)
            # differs from actual ROM name stored in sidecar.
            sidecar_names = [Path(e.path).name for e in existing.sidecar.files]

            for ef in extracted:
                # Try direct name match first
                dest_name = ef.original_name
                if dest_name not in sidecar_names and len(extracted) == len(sidecar_names):
                    # Single-file .zst: extracted "Game" → sidecar "Game.bin"
                    idx = extracted.index(ef)
                    dest_name = sidecar_names[idx]

                dest = rom_work / dest_name
                if not dest.exists():  # don't overwrite new ROMs
                    shutil.move(str(ef.path), str(dest))

        # Compress everything in rom_work
        all_roms = sorted(rom_work.iterdir())
        compress_base = compress_dir / game.game_name
        # Apply verify for aaru profiles (same as CREATE path)
        if compression_profile.startswith("aaru-") and verify_roundtrip:
            profile_kwargs = dict(profile["kwargs"])
            profile_kwargs["verify"] = True
            func = profile["function"]
            result = func(all_roms, compress_base, **profile_kwargs)
        else:
            result = compress(compression_profile, all_roms, compress_base)
        archive_in_work = result.output

    else:
        assert False, f"Unexpected action {action} in _execute_archive"

    # Step 5: Hash compressed output
    compressed_hashes = hash_file(archive_in_work)

    # Step 6: Copy to target and verify
    shutil.copy2(archive_in_work, target_archive)

    if not _verify_on_target(target_archive, compressed_hashes.blake3):
        print(
            f"    Failed: target verification for {archive_name}",
            file=sys.stderr,
        )
        target_archive.unlink(missing_ok=True)
        stats["failed"] += len(new_ops) or len(game.ops)
        return

    # Step 7: Build RSCF sidecar (existing + new entries)
    new_entries = [
        FileEntry.from_hashes(
            path=op.rom_name,
            size=rom_size,
            hashes=rom_hashes,
        )
        for op, _, rom_hashes, rom_size in verified_roms
    ]

    # Merge: keep existing entries that aren't being replaced
    new_rom_names = {Path(op.rom_name).name for op, _, _, _ in verified_roms}
    merged_entries = [
        e for e in existing_entries
        if Path(e.path).name not in new_rom_names
    ] + new_entries

    target_stat = target_archive.stat()
    sidecar = Sidecar(
        container_blake3=compressed_hashes.blake3,
        container_size=target_stat.st_size,
        container_mtime_ns=target_stat.st_mtime_ns,
        container_ctime_ns=target_stat.st_ctime_ns,
        container_inode=target_stat.st_ino,
        renderer=compression_profile,
        files=merged_entries,
    )

    rscf_path = resolver.sidecar_path(target_archive)
    write_sidecar(sidecar, rscf_path)

    # Step 8: Cleanup old archive if path changed
    if existing is not None and existing.path != target_archive:
        _cleanup_old(existing.path, existing.rscf_path, db)

    # Step 9: Update DB — replace all entries for this archive
    db.delete_romroot_entries(str(target_archive))
    for entry in merged_entries:
        db.upsert_romroot(
            path=str(target_archive),
            system=game.system,
            game_name=game.game_name,
            rom_name=entry.path,
            crc32=entry.crc32,
            md5=entry.md5,
            sha1=entry.sha1,
            sha256=entry.sha256,
            blake3=entry.blake3,
            rscf_path=str(rscf_path),
        )

    stats["processed"] += len(verified_roms) or len(existing_entries)
