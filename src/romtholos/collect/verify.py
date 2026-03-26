"""Verify romroot integrity — read-only post-mortem corruption detection.

Walks romroot, re-hashes each archive, compares BLAKE3 against the RSCF
sidecar. Reports corrupt files with per-ROM recovery status (whether a
source is known in the DB for re-collection).

Never modifies files or the database. Exit code 0 = clean, 1 = corruption.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path

from rscf import (
    FileEntry,
    SidecarResolver,
    StorageMode,
    hash_file,
    read_sidecar,
)
from rscf.sidecar import RscfError

from romtholos.collect.config import ORPHANED_DIR_NAME
from romtholos.collect.db import CacheDB, HASH_TYPES


@dataclass
class RomStatus:
    """Recovery status for a single ROM within a corrupt archive."""

    rom_name: str
    size: int
    source_available: bool = False
    source_path: str = ""


@dataclass
class CorruptFile:
    """A romroot archive that failed BLAKE3 verification."""

    path: Path
    expected_blake3: str
    actual_blake3: str
    renderer: str
    roms: list[RomStatus] = field(default_factory=list)

    @property
    def recoverable_count(self) -> int:
        return sum(1 for r in self.roms if r.source_available)

    @property
    def lost_count(self) -> int:
        return sum(1 for r in self.roms if not r.source_available)


@dataclass
class VerifyResult:
    """Summary of a verify run."""

    verified: int = 0
    corrupt: int = 0
    missing_sidecar: int = 0
    total: int = 0
    corrupt_files: list[CorruptFile] = field(default_factory=list)

    @property
    def clean(self) -> bool:
        return self.corrupt == 0 and self.missing_sidecar == 0


def _check_source_available(
    entry: FileEntry, db: CacheDB | None,
) -> tuple[bool, str]:
    """Check if a ROM's content hash exists in any non-romroot source.

    Searches scanned_files and archive_contents for a matching hash.
    Returns (available, source_path).
    """
    if db is None:
        return False, ""

    for hash_type in ("blake3", "sha1", "md5", "sha256", "crc32"):
        hash_value = getattr(entry, hash_type, "")
        if not hash_value:
            continue

        # Check scanned_files (non-romroot sources)
        rows = db.find_by_hash(hash_type, hash_value)
        for row in rows:
            if row["source_type"] != "romroot":
                return True, row["path"]

        # Check archive_contents
        cur = db._conn.execute(
            f"SELECT ac.archive_path FROM archive_contents ac "
            f"JOIN scanned_files sf ON ac.archive_path = sf.path "
            f"WHERE ac.{hash_type} = ? AND sf.source_type != 'romroot'",
            (hash_value,),
        )
        row = cur.fetchone()
        if row:
            return True, row[0]

    return False, ""


def verify_romroot(
    romroot: Path,
    db: CacheDB | None = None,
    romroot_overrides: dict[str, Path] | None = None,
) -> VerifyResult:
    """Verify integrity of all romroot archives.

    Walks romroot (and any overrides), re-hashes each archive file,
    and compares BLAKE3 against the stored sidecar value.

    Args:
        romroot: Default romroot path.
        db: Optional DB for source availability checks. If None,
            source availability is not checked.
        romroot_overrides: Per-system romroot path overrides.

    Returns:
        VerifyResult with per-file details for any corrupt archives.
    """
    result = VerifyResult()
    resolver = SidecarResolver(StorageMode.IN_TREE)

    # Collect all romroot paths to verify
    roots = [romroot]
    if romroot_overrides:
        for override_path in romroot_overrides.values():
            if override_path not in roots:
                roots.append(override_path)

    # Walk all romroot paths, collect archives with sidecars
    archives: list[tuple[Path, Path]] = []  # (archive, sidecar)
    for root in roots:
        if not root.exists():
            continue
        orphaned_dir = root / ORPHANED_DIR_NAME
        for p in sorted(root.glob("**/*")):
            if not p.is_file():
                continue
            if p == orphaned_dir or p.is_relative_to(orphaned_dir):
                continue
            if p.suffix == ".rscf":
                continue
            sidecar_path = resolver.sidecar_path(p)
            if sidecar_path.is_file():
                archives.append((p, sidecar_path))
            else:
                result.missing_sidecar += 1
                print(
                    f"  Warning: no sidecar for {p.name}",
                    file=sys.stderr,
                )

    result.total = len(archives) + result.missing_sidecar

    for i, (archive_path, sidecar_path) in enumerate(archives, 1):
        print(
            f"  [{i}/{len(archives)}] Verifying: {archive_path.name}",
            file=sys.stderr,
        )

        # Read sidecar
        try:
            sidecar = read_sidecar(sidecar_path)
        except RscfError as e:
            # Corrupt sidecar — can't verify, treat as missing
            result.missing_sidecar += 1
            print(
                f"  Warning: corrupt sidecar {sidecar_path.name}: {e}",
                file=sys.stderr,
            )
            continue

        expected_blake3 = sidecar.container_blake3.upper()
        if not expected_blake3:
            result.missing_sidecar += 1
            print(
                f"  Warning: sidecar has no container BLAKE3: {sidecar_path.name}",
                file=sys.stderr,
            )
            continue

        # Re-hash the archive
        hashes = hash_file(archive_path)
        actual_blake3 = hashes.blake3.upper()

        if actual_blake3 == expected_blake3:
            result.verified += 1
            continue

        # Corruption detected
        print(
            f"  CORRUPT: {archive_path.name}",
            file=sys.stderr,
        )
        print(
            f"    Expected: {expected_blake3}",
            file=sys.stderr,
        )
        print(
            f"    Actual:   {actual_blake3}",
            file=sys.stderr,
        )

        # Check per-ROM recovery status
        rom_statuses = []
        for entry in sidecar.files:
            available, source_path = _check_source_available(entry, db)
            rom_statuses.append(RomStatus(
                rom_name=entry.path,
                size=entry.size,
                source_available=available,
                source_path=source_path,
            ))

        corrupt_file = CorruptFile(
            path=archive_path,
            expected_blake3=expected_blake3,
            actual_blake3=actual_blake3,
            renderer=sidecar.renderer,
            roms=rom_statuses,
        )
        result.corrupt += 1
        result.corrupt_files.append(corrupt_file)

        recoverable = corrupt_file.recoverable_count
        lost = corrupt_file.lost_count
        total_roms = len(rom_statuses)
        if total_roms > 0:
            print(
                f"    ROMs: {total_roms} total, "
                f"{recoverable} recoverable, {lost} lost",
                file=sys.stderr,
            )
            for rom in rom_statuses:
                status = "recoverable" if rom.source_available else "LOST"
                source_info = f" ({rom.source_path})" if rom.source_available else ""
                print(
                    f"      [{status}] {rom.rom_name}{source_info}",
                    file=sys.stderr,
                )

    return result
