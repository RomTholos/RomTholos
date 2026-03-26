"""User-initiated orphan purge safety — FILE_SAFETY.md § Deletion by user request only.

User-initiated cleanup of _orphaned/ is the only way to delete quarantined files.
The user reviews orphaned files and explicitly requests deletion.
RomTholos never deletes from _orphaned/ on its own.

Guarantees tested:
- Listing orphans with sizes
- Purging a specific orphan by path
- Purging all orphans, returning freed space
- Empty parent directories cleaned up after purge
- Purge refuses to delete outside _orphaned/
"""

from __future__ import annotations

from pathlib import Path

import pytest

from rscf import FileEntry, Sidecar, SidecarResolver, StorageMode, hash_file, write_sidecar

from romtholos.collect.config import ORPHANED_DIR_NAME, SourceDir
from romtholos.collect.db import CacheDB
from romtholos.collect.purge import list_orphans, purge_all, purge_orphan

from .conftest import (
    make_dat,
    make_rom,
    pipeline_run_with_quarantine,
    rom_entry,
)


def _make_orphan_file(romroot: Path, rel_path: str, content: bytes) -> Path:
    """Create a file in _orphaned/ with an RSCF sidecar."""
    orphan_dir = romroot / ORPHANED_DIR_NAME
    path = orphan_dir / rel_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)

    # Write a sidecar
    resolver = SidecarResolver(StorageMode.IN_TREE)
    hashes = hash_file(path)
    st = path.stat()
    sc = Sidecar(
        container_blake3=hashes.blake3,
        container_size=st.st_size,
        container_mtime_ns=st.st_mtime_ns,
        container_ctime_ns=int(st.st_ctime * 1e9),
        container_inode=st.st_ino,
        renderer="7z-16m",
        files=[FileEntry(
            path=path.name,
            size=st.st_size,
            crc32=hashes.crc32,
            md5=hashes.md5,
            sha1=hashes.sha1,
            sha256=hashes.sha256,
            blake3=hashes.blake3,
        )],
    )
    write_sidecar(sc, resolver.sidecar_path(path))
    return path


def _make_orphan_dir(romroot: Path, rel_path: str, files: dict[str, bytes]) -> Path:
    """Create a game directory in _orphaned/ with files and sidecars."""
    orphan_dir = romroot / ORPHANED_DIR_NAME
    game_dir = orphan_dir / rel_path
    game_dir.mkdir(parents=True, exist_ok=True)

    resolver = SidecarResolver(StorageMode.IN_TREE)
    for name, content in files.items():
        fpath = game_dir / name
        fpath.write_bytes(content)
        hashes = hash_file(fpath)
        st = fpath.stat()
        sc = Sidecar(
            container_blake3=hashes.blake3,
            container_size=st.st_size,
            container_mtime_ns=st.st_mtime_ns,
            container_ctime_ns=int(st.st_ctime * 1e9),
            container_inode=st.st_ino,
            renderer="none",
            files=[FileEntry(
                path=name,
                size=st.st_size,
                crc32=hashes.crc32,
                md5=hashes.md5,
                sha1=hashes.sha1,
                sha256=hashes.sha256,
                blake3=hashes.blake3,
            )],
        )
        write_sidecar(sc, resolver.sidecar_path(fpath))

    return game_dir


class TestListOrphans:
    """list_orphans() discovers all items in _orphaned/."""

    def test_empty_when_no_orphans(self, env):
        entries = list_orphans(env.romroot)
        assert entries == []

    def test_empty_when_no_orphan_dir(self, env):
        # romroot exists but _orphaned/ does not
        assert env.romroot.is_dir()
        entries = list_orphans(env.romroot)
        assert entries == []

    def test_finds_archive_orphan(self, env):
        path = _make_orphan_file(env.romroot, "System/Game.7z", b"archive" * 100)
        entries = list_orphans(env.romroot)
        assert len(entries) == 1
        assert entries[0].path == path
        assert not entries[0].is_directory
        assert entries[0].size > 0

    def test_finds_directory_orphan(self, env):
        game_dir = _make_orphan_dir(env.romroot, "System/Game", {
            "track01.bin": b"track1" * 100,
            "track02.bin": b"track2" * 100,
        })
        entries = list_orphans(env.romroot)
        assert len(entries) == 1
        assert entries[0].path == game_dir
        assert entries[0].is_directory

    def test_size_includes_sidecar(self, env):
        path = _make_orphan_file(env.romroot, "System/Game.7z", b"data" * 500)
        resolver = SidecarResolver(StorageMode.IN_TREE)
        sidecar = resolver.sidecar_path(path)
        assert sidecar.exists()

        entries = list_orphans(env.romroot)
        expected_size = path.stat().st_size + sidecar.stat().st_size
        assert entries[0].size == expected_size

    def test_relative_path(self, env):
        _make_orphan_file(env.romroot, "Folder/Sub/Game.7z", b"x" * 100)
        entries = list_orphans(env.romroot)
        assert entries[0].relative == Path("Folder/Sub/Game.7z")


class TestPurgeSingle:
    """purge_orphan() deletes one item and cleans up."""

    def test_deletes_file_and_sidecar(self, env):
        path = _make_orphan_file(env.romroot, "Sys/Game.7z", b"data" * 100)
        resolver = SidecarResolver(StorageMode.IN_TREE)
        sidecar = resolver.sidecar_path(path)

        entries = list_orphans(env.romroot)
        assert len(entries) == 1

        freed = purge_orphan(entries[0], env.romroot)
        assert freed > 0
        assert not path.exists()
        assert not sidecar.exists()

    def test_deletes_directory(self, env):
        game_dir = _make_orphan_dir(env.romroot, "Sys/Game", {
            "a.bin": b"aaa" * 100,
            "b.bin": b"bbb" * 100,
        })

        entries = list_orphans(env.romroot)
        freed = purge_orphan(entries[0], env.romroot)
        assert freed > 0
        assert not game_dir.exists()

    def test_cleans_empty_parents(self, env):
        _make_orphan_file(env.romroot, "Deep/Nested/Path/Game.7z", b"x" * 100)
        orphan_dir = env.romroot / ORPHANED_DIR_NAME

        entries = list_orphans(env.romroot)
        purge_orphan(entries[0], env.romroot)

        # All intermediate dirs removed, but _orphaned/ itself stays
        assert orphan_dir.exists()
        assert not (orphan_dir / "Deep").exists()

    def test_preserves_sibling_orphans(self, env):
        _make_orphan_file(env.romroot, "Sys/Game1.7z", b"one" * 100)
        _make_orphan_file(env.romroot, "Sys/Game2.7z", b"two" * 100)

        entries = list_orphans(env.romroot)
        assert len(entries) == 2

        # Delete only the first
        purge_orphan(entries[0], env.romroot)

        remaining = list_orphans(env.romroot)
        assert len(remaining) == 1
        assert remaining[0].path == entries[1].path

    def test_refuses_outside_orphaned(self, env):
        """purge_orphan refuses to delete a file not inside _orphaned/."""
        from romtholos.collect.purge import OrphanEntry
        fake = OrphanEntry(
            path=env.romroot / "System" / "Game.7z",
            size=100,
            is_directory=False,
        )
        with pytest.raises(AssertionError, match="Refusing to delete"):
            purge_orphan(fake, env.romroot)


class TestPurgeAll:
    """purge_all() clears everything and reports freed space."""

    def test_returns_count_and_freed(self, env):
        _make_orphan_file(env.romroot, "A/Game1.7z", b"a" * 1000)
        _make_orphan_file(env.romroot, "B/Game2.7z", b"b" * 2000)

        count, freed = purge_all(env.romroot)
        assert count == 2
        assert freed > 3000  # files + sidecars

        remaining = list_orphans(env.romroot)
        assert remaining == []

    def test_noop_when_empty(self, env):
        count, freed = purge_all(env.romroot)
        assert count == 0
        assert freed == 0


class TestPurgeIntegration:
    """End-to-end: quarantine via pipeline, then purge."""

    def test_quarantine_then_purge(self, env):
        """Full flow: collect, remove from DAT, quarantine, purge."""
        rom_a = make_rom(env.source / "A.gba", b"ROMDATA_A" * 100)
        rom_b = make_rom(env.source / "B.gba", b"ROMDATA_B" * 100)
        entry_a = rom_entry(rom_a)
        entry_b = rom_entry(rom_b)

        # Initial DAT with both games
        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "GameA", "roms": [entry_a]},
            {"name": "GameB", "roms": [entry_b]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run_with_quarantine(
                sources, env.selection, env.romroot, env.work, db,
            )

        # Both collected
        assert len(list(env.romroot.rglob("*.7z"))) == 2

        # Remove GameB from DAT
        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "GameA", "roms": [entry_a]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run_with_quarantine(
                sources, env.selection, env.romroot, env.work, db,
            )

        # GameB quarantined
        orphans = list_orphans(env.romroot)
        assert len(orphans) == 1
        assert "GameB" in str(orphans[0].path)

        # Purge
        count, freed = purge_all(env.romroot)
        assert count == 1
        assert freed > 0
        assert list_orphans(env.romroot) == []
