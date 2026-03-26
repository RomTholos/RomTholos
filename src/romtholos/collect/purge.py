"""User-initiated orphan purge — list and delete quarantined files.

RomTholos never auto-deletes from _orphaned/. This module provides
the API for users to review orphans and explicitly request deletion.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from rscf import SidecarResolver, StorageMode

from romtholos.collect.compress import _KNOWN_ARCHIVE_EXTENSIONS
from romtholos.collect.config import ORPHANED_DIR_NAME

# Extensions that indicate a file is an archive (not a bare ROM).
_ARCHIVE_SUFFIXES = set(_KNOWN_ARCHIVE_EXTENSIONS) | {".tar.zst"}


@dataclass
class OrphanEntry:
    """A single orphaned item (archive file or game directory)."""

    path: Path
    size: int  # total bytes (including sidecar for archives, all files for dirs)
    is_directory: bool

    @property
    def relative(self) -> Path:
        """Path relative to the _orphaned/ directory."""
        parts = self.path.parts
        for i, part in enumerate(parts):
            if part == ORPHANED_DIR_NAME:
                return Path(*parts[i + 1:])
        return Path(self.path.name)


def _is_archive_file(path: Path) -> bool:
    """Check if a file has a known archive extension."""
    name = path.name
    if name.endswith(".tar.zst"):
        return True
    return path.suffix in _ARCHIVE_SUFFIXES


def _item_size(path: Path, resolver: SidecarResolver) -> int:
    """Total size of an orphan item including sidecars."""
    if path.is_dir():
        return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())

    size = path.stat().st_size
    sidecar = resolver.sidecar_path(path)
    if sidecar.exists():
        size += sidecar.stat().st_size
    return size


def list_orphans(romroot: Path) -> list[OrphanEntry]:
    """List all orphaned items in romroot/_orphaned/.

    Returns a sorted list of OrphanEntry objects. Each entry is either:
    - An archive file (.7z, .tar.zst, .rvz, etc.) with its sidecar
    - A game directory (none-profile) containing bare ROM files

    A directory is treated as a game directory when it contains
    non-archive files (bare ROMs). Directories that only contain
    archives are path-structure directories — the archives inside
    are listed individually.
    """
    orphan_dir = romroot / ORPHANED_DIR_NAME
    if not orphan_dir.is_dir():
        return []

    resolver = SidecarResolver(StorageMode.IN_TREE)
    entries: list[OrphanEntry] = []
    game_dirs: set[Path] = set()

    # First pass: identify game directories (contain bare ROM files)
    for d in sorted(orphan_dir.rglob("*")):
        if not d.is_dir():
            continue
        has_bare_roms = any(
            f.is_file() and f.suffix != ".rscf" and not _is_archive_file(f)
            for f in d.iterdir()
        )
        if has_bare_roms:
            game_dirs.add(d)
            entries.append(OrphanEntry(
                path=d,
                size=_item_size(d, resolver),
                is_directory=True,
            ))

    # Second pass: collect archive files not inside game directories
    for f in sorted(orphan_dir.rglob("*")):
        if not f.is_file() or f.suffix == ".rscf":
            continue
        # Skip files inside game directories
        if any(f.is_relative_to(gd) for gd in game_dirs):
            continue
        entries.append(OrphanEntry(
            path=f,
            size=_item_size(f, resolver),
            is_directory=False,
        ))

    entries.sort(key=lambda e: e.path)
    return entries


def purge_orphan(entry: OrphanEntry, romroot: Path) -> int:
    """Delete a single orphan entry. Returns bytes freed.

    Removes the file (+ sidecar) or directory. Cleans up empty parent
    directories up to _orphaned/.
    """
    orphan_dir = romroot / ORPHANED_DIR_NAME
    assert entry.path.is_relative_to(orphan_dir), (
        f"Refusing to delete outside _orphaned/: {entry.path}"
    )

    freed = entry.size
    resolver = SidecarResolver(StorageMode.IN_TREE)

    if entry.is_directory:
        import shutil
        shutil.rmtree(entry.path)
    else:
        sidecar = resolver.sidecar_path(entry.path)
        entry.path.unlink()
        if sidecar.exists():
            sidecar.unlink()

    # Clean empty parents up to (but not including) _orphaned/ itself
    current = entry.path.parent
    while current != orphan_dir and current.exists():
        if any(current.iterdir()):
            break
        parent = current.parent
        current.rmdir()
        current = parent

    return freed


def purge_all(romroot: Path) -> tuple[int, int]:
    """Delete all orphans. Returns (count, bytes_freed)."""
    entries = list_orphans(romroot)
    total_freed = 0
    for entry in entries:
        total_freed += purge_orphan(entry, romroot)
    return len(entries), total_freed
