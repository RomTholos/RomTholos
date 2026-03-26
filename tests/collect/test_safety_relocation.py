"""Relocation safety — FILE_SAFETY.md § Relocation: Romroot path change.

When DATs are reorganized in selection/, romroot files automatically move
to match. Relocations must be pure filesystem moves — no repacking, no
recompression. The file content must be byte-identical before and after.

Guarantees tested:
- Archive content is byte-identical after relocation (proves no recompression)
- RSCF sidecar moves alongside the archive
- Sidecar ROM hashes still valid after relocation
- Old path is gone, no duplicates exist
- Empty parent directories are cleaned up
- Directory mode (none profile) works the same way
- Simultaneous relocation + profile change preserves ROM content
- Cross-filesystem moves trigger re-verification
- Sidecar metadata (inode) updated after move
"""

from __future__ import annotations

from pathlib import Path

import pytest

from rscf import Sidecar, SidecarResolver, StorageMode, hash_file, read_sidecar, write_sidecar

from romtholos.collect.config import SourceDir
from romtholos.collect.db import CacheDB

from .conftest import make_dat, make_rom, pipeline_run, rom_entry


class TestRelocationContentPreservation:
    """Relocated files must be byte-identical — proves no recompression."""

    def test_archive_blake3_identical(self, env):
        """Archive BLAKE3 is identical before and after relocation.

        7z is not deterministic across runs — if the BLAKE3 is the same,
        the file was moved, not recompressed.
        """
        rom = make_rom(env.source / "Game.gba", b"RELOC_IDENT" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "OldFolder" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            old_archive = env.romroot / "OldFolder" / "Sys" / "Game.7z"
            assert old_archive.exists()
            original_blake3 = hash_file(old_archive).blake3
            original_size = old_archive.stat().st_size

            # Move DAT to new folder
            (env.selection / "NewFolder").mkdir(parents=True)
            (env.selection / "OldFolder" / "Sys.dat").rename(
                env.selection / "NewFolder" / "Sys.dat",
            )
            (env.selection / "OldFolder").rmdir()

            result = pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
            )

            assert result["relocated"] == 1

            new_archive = env.romroot / "NewFolder" / "Sys" / "Game.7z"
            assert new_archive.exists()
            assert not old_archive.exists()
            assert hash_file(new_archive).blake3 == original_blake3
            assert new_archive.stat().st_size == original_size

    def test_directory_mode_rom_identical(self, env):
        """Directory mode: ROM files byte-identical after relocation."""
        rom = make_rom(env.source / "Game.gba", b"DIR_RELOC" * 100)
        entry = rom_entry(rom)

        make_dat(env.selection / "A" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="none",
            )

            old_rom = env.romroot / "A" / "Sys" / "Game" / "Game.gba"
            assert old_rom.exists()
            original_blake3 = hash_file(old_rom).blake3

            # Move DAT
            (env.selection / "B").mkdir(parents=True)
            (env.selection / "A" / "Sys.dat").rename(
                env.selection / "B" / "Sys.dat",
            )
            (env.selection / "A").rmdir()

            result = pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="none",
            )

            assert result["relocated"] == 1
            new_rom = env.romroot / "B" / "Sys" / "Game" / "Game.gba"
            assert new_rom.exists()
            assert not old_rom.exists()
            assert hash_file(new_rom).blake3 == original_blake3

    def test_multi_rom_archive_identical(self, env):
        """Multi-ROM archive: all tracks preserved, archive byte-identical."""
        roms = []
        original_hashes = {}
        for i in range(3):
            r = make_rom(
                env.source / f"Track{i}.bin",
                f"TRACK_{i}".encode() * 200,
            )
            roms.append(rom_entry(r))
            original_hashes[f"Track{i}.bin"] = hash_file(r).sha1

        make_dat(env.selection / "Old" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": roms},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            old_archive = env.romroot / "Old" / "Sys" / "Game.7z"
            original_blake3 = hash_file(old_archive).blake3

            # Relocate
            (env.selection / "New").mkdir(parents=True)
            (env.selection / "Old" / "Sys.dat").rename(
                env.selection / "New" / "Sys.dat",
            )
            (env.selection / "Old").rmdir()

            result = pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
            )

            assert result["relocated"] == 1
            new_archive = env.romroot / "New" / "Sys" / "Game.7z"
            assert hash_file(new_archive).blake3 == original_blake3

            # Verify all tracks still in sidecar
            sidecar = read_sidecar(new_archive.parent / "Game.7z.rscf")
            assert len(sidecar.files) == 3
            for f in sidecar.files:
                name = Path(f.path).name
                assert name in original_hashes
                assert f.sha1.upper() == original_hashes[name].upper()


class TestSidecarRelocation:
    """Sidecar must move alongside the archive and remain valid."""

    def test_sidecar_moves_with_archive(self, env):
        """RSCF sidecar is at new path after relocation."""
        rom = make_rom(env.source / "Game.gba", b"SIDECAR_RELOC" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "A" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            old_rscf = env.romroot / "A" / "Sys" / "Game.7z.rscf"
            assert old_rscf.exists()
            original_sidecar = read_sidecar(old_rscf)

            # Relocate
            (env.selection / "B").mkdir(parents=True)
            (env.selection / "A" / "Sys.dat").rename(
                env.selection / "B" / "Sys.dat",
            )
            (env.selection / "A").rmdir()

            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            new_rscf = env.romroot / "B" / "Sys" / "Game.7z.rscf"
            assert new_rscf.exists()
            assert not old_rscf.exists()

            # Sidecar content preserved
            new_sidecar = read_sidecar(new_rscf)
            assert len(new_sidecar.files) == len(original_sidecar.files)
            assert new_sidecar.files[0].sha1 == original_sidecar.files[0].sha1
            assert new_sidecar.renderer == original_sidecar.renderer

    def test_sidecar_container_hash_still_valid(self, env):
        """After relocation, the sidecar's container_blake3 still matches the file."""
        rom = make_rom(env.source / "Game.gba", b"CONTAINER_VALID" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "A" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # Relocate
            (env.selection / "B").mkdir(parents=True)
            (env.selection / "A" / "Sys.dat").rename(
                env.selection / "B" / "Sys.dat",
            )
            (env.selection / "A").rmdir()

            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            new_archive = env.romroot / "B" / "Sys" / "Game.7z"
            sidecar = read_sidecar(new_archive.parent / "Game.7z.rscf")

            # The file on disk must match what the sidecar claims
            actual_blake3 = hash_file(new_archive).blake3
            assert actual_blake3.upper() == sidecar.container_blake3.upper()

    def test_directory_mode_sidecars_move(self, env):
        """Directory mode: per-file sidecars move with ROM files."""
        rom = make_rom(env.source / "Game.gba", b"DIR_SC_MOVE" * 100)
        entry = rom_entry(rom)

        make_dat(env.selection / "A" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="none",
            )

            old_rscf = env.romroot / "A" / "Sys" / "Game" / "Game.gba.rscf"
            assert old_rscf.exists()

            # Relocate
            (env.selection / "B").mkdir(parents=True)
            (env.selection / "A" / "Sys.dat").rename(
                env.selection / "B" / "Sys.dat",
            )
            (env.selection / "A").rmdir()

            pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="none",
            )

            new_rscf = env.romroot / "B" / "Sys" / "Game" / "Game.gba.rscf"
            assert new_rscf.exists()
            assert not old_rscf.exists()


class TestRelocationCleanup:
    """Old paths and empty directories must be cleaned up after relocation."""

    def test_old_path_gone(self, env):
        """After relocation, no files remain at the old path."""
        rom = make_rom(env.source / "Game.gba", b"OLD_PATH_GONE" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "OldFolder" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # Relocate
            (env.selection / "NewFolder").mkdir(parents=True)
            (env.selection / "OldFolder" / "Sys.dat").rename(
                env.selection / "NewFolder" / "Sys.dat",
            )
            (env.selection / "OldFolder").rmdir()

            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # Old directory should not exist (empty dirs removed)
            old_sys_dir = env.romroot / "OldFolder" / "Sys"
            assert not old_sys_dir.exists()

    def test_empty_parent_directories_removed(self, env):
        """Empty parent directories are cleaned up after last game relocates."""
        rom = make_rom(env.source / "Game.gba", b"EMPTY_PARENTS" * 200)
        entry = rom_entry(rom)

        # Deep nesting: selection/A/B/C/Sys.dat → romroot/A/B/C/Sys/
        make_dat(env.selection / "A" / "B" / "C" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            assert (env.romroot / "A" / "B" / "C" / "Sys" / "Game.7z").exists()

            # Move DAT to flat location
            make_dat(env.selection / "Flat" / "Sys.dat", "Sys", [
                {"name": "Game", "roms": [entry]},
            ])
            import shutil
            shutil.rmtree(env.selection / "A")

            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # New location exists
            assert (env.romroot / "Flat" / "Sys" / "Game.7z").exists()

            # Entire old hierarchy should be gone (all were empty)
            assert not (env.romroot / "A").exists()

    def test_no_duplicates_after_relocation(self, env):
        """After relocation, the game exists at exactly one path."""
        rom = make_rom(env.source / "Game.gba", b"NO_DUPES" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "A" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # Relocate
            (env.selection / "B").mkdir(parents=True)
            (env.selection / "A" / "Sys.dat").rename(
                env.selection / "B" / "Sys.dat",
            )
            (env.selection / "A").rmdir()

            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # Count all Game.7z files in romroot (excluding _orphaned)
            game_archives = [
                f for f in env.romroot.rglob("Game.7z")
                if "_orphaned" not in str(f)
            ]
            assert len(game_archives) == 1
            assert game_archives[0] == env.romroot / "B" / "Sys" / "Game.7z"


class TestRelocationWithProfileChange:
    """Simultaneous relocation + profile change must preserve ROM content."""

    def test_relocate_and_recompress(self, env):
        """ROM hashes are preserved when relocation and profile change happen together."""
        rom = make_rom(env.source / "Game.gba", b"RELOC_PROFILE" * 200)
        entry = rom_entry(rom)
        original_sha1 = hash_file(env.source / "Game.gba").sha1

        make_dat(env.selection / "A" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]

            # Collect with 7z
            pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="7z-16m",
            )
            old_archive = env.romroot / "A" / "Sys" / "Game.7z"
            assert old_archive.exists()

            # Move DAT + change profile
            (env.selection / "B").mkdir(parents=True)
            (env.selection / "A" / "Sys.dat").rename(
                env.selection / "B" / "Sys.dat",
            )
            (env.selection / "A").rmdir()

            pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="zstd-19",
            )

            # Old location fully gone
            assert not (env.romroot / "A").exists()

            # New location with new format
            new_archive = env.romroot / "B" / "Sys" / "Game.zst"
            assert new_archive.exists()

            # ROM content hash preserved (SHA1 from sidecar matches original)
            sidecar = read_sidecar(new_archive.parent / "Game.zst.rscf")
            assert sidecar.files[0].sha1.upper() == original_sha1.upper()

            # Sidecar renderer reflects new profile
            assert sidecar.renderer == "zstd-19"


class TestRelocationIdempotent:
    """Subsequent runs after relocation must not re-relocate or re-collect."""

    def test_second_run_skips(self, env):
        """After relocation, the next run skips the game (already in place)."""
        rom = make_rom(env.source / "Game.gba", b"IDEMPOTENT" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "A" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # Relocate
            (env.selection / "B").mkdir(parents=True)
            (env.selection / "A" / "Sys.dat").rename(
                env.selection / "B" / "Sys.dat",
            )
            (env.selection / "A").rmdir()

            r1 = pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
            )
            assert r1["relocated"] == 1
            archive_blake3 = hash_file(
                env.romroot / "B" / "Sys" / "Game.7z"
            ).blake3

            # Second run: nothing should change
            r2 = pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
            )
            assert r2["relocated"] == 0
            assert r2["processed"] == 0
            assert r2["skipped"] == 1
            assert hash_file(
                env.romroot / "B" / "Sys" / "Game.7z"
            ).blake3 == archive_blake3


class TestCrossFilesystemRelocation:
    """Cross-filesystem moves must trigger re-verification.

    FILE_SAFETY.md § Cross-filesystem moves:
    RSCF sidecars record inode numbers; an inode change after a move signals
    a cross-filesystem copy occurred, and re-verification is required before
    deleting the source.
    """

    def test_inode_change_triggers_reverification(self, env):
        """When inode changes after move, BLAKE3 re-verification runs.

        Simulates cross-filesystem move by writing a fake inode into the
        sidecar before relocation. The relocation code detects the inode
        mismatch and re-verifies BLAKE3 of the target.
        """
        rom = make_rom(env.source / "Game.gba", b"CROSS_FS" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "A" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            archive = env.romroot / "A" / "Sys" / "Game.7z"
            assert archive.exists()

            # Tamper with sidecar: set a fake inode to simulate cross-fs
            resolver = SidecarResolver(StorageMode.IN_TREE)
            rscf_path = resolver.sidecar_path(archive)
            sc = read_sidecar(rscf_path)
            tampered = Sidecar(
                container_blake3=sc.container_blake3,
                container_size=sc.container_size,
                container_mtime_ns=sc.container_mtime_ns,
                container_ctime_ns=sc.container_ctime_ns,
                container_inode=sc.container_inode + 999999,  # fake inode
                renderer=sc.renderer,
                files=sc.files,
            )
            write_sidecar(tampered, rscf_path)

            archive_blake3 = hash_file(archive).blake3

            # Relocate: A → B
            (env.selection / "B").mkdir(parents=True)
            (env.selection / "A" / "Sys.dat").rename(
                env.selection / "B" / "Sys.dat",
            )
            (env.selection / "A").rmdir()

            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # File relocated and content preserved (re-verification passed)
            new_archive = env.romroot / "B" / "Sys" / "Game.7z"
            assert new_archive.exists()
            assert hash_file(new_archive).blake3 == archive_blake3

            # Sidecar updated with correct inode
            new_sc = read_sidecar(resolver.sidecar_path(new_archive))
            assert new_sc.container_inode == new_archive.stat().st_ino

    def test_sidecar_metadata_updated_after_move(self, env):
        """After relocation, sidecar's inode/mtime/size reflect the new file."""
        rom = make_rom(env.source / "Game.gba", b"META_UPDATE" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "A" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # Relocate
            (env.selection / "B").mkdir(parents=True)
            (env.selection / "A" / "Sys.dat").rename(
                env.selection / "B" / "Sys.dat",
            )
            (env.selection / "A").rmdir()

            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            new_archive = env.romroot / "B" / "Sys" / "Game.7z"
            sidecar = read_sidecar(new_archive.parent / "Game.7z.rscf")
            actual_stat = new_archive.stat()

            # Sidecar metadata must reflect the file at its NEW location
            assert sidecar.container_inode == actual_stat.st_ino
            assert sidecar.container_size == actual_stat.st_size
