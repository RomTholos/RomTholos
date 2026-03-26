"""Post-mortem corruption safety — FILE_SAFETY.md § Corruption detection.

Post-mortem corruption is detected later — during a periodic integrity
check, rescan, or when a user tries to use a file. Recovery options:
1. Source still available → report as recoverable
2. Source unavailable, other sources have the hash → report as recoverable
3. No source available → report as lost, require manual intervention

RomTholos must never automatically delete a romroot file discovered to be
corrupt post-mortem.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from rscf import hash_file

from romtholos.collect.config import SourceDir
from romtholos.collect.db import CacheDB
from romtholos.collect.verify import verify_romroot

from .conftest import make_dat, make_rom, pipeline_run, rom_entry


class TestPostMortemCorruption:
    """Corrupt romroot files must never be silently deleted."""

    def test_corrupt_file_flagged_not_deleted(self, env):
        """A file found corrupt during integrity check is flagged, not deleted."""
        rom = make_rom(env.source / "Game.gba", b"CORRUPT_PM" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            archive = env.romroot / "F" / "Sys" / "Game.7z"
            assert archive.exists()

            # Simulate corruption: overwrite archive with garbage
            archive.write_bytes(b"CORRUPTED_DATA")

            # Verify detects corruption but does NOT delete the file
            result = verify_romroot(env.romroot, db=db)

            assert result.corrupt == 1
            assert result.verified == 0
            assert not result.clean

            # File must still exist — never auto-delete corrupt romroot files
            assert archive.exists()
            assert archive.read_bytes() == b"CORRUPTED_DATA"

    def test_source_available_re_collect(self, env):
        """Corrupt file with source available → reported as recoverable."""
        rom = make_rom(env.source / "Game.gba", b"RECOLLECT" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            archive = env.romroot / "F" / "Sys" / "Game.7z"
            assert archive.exists()

            # Corrupt the archive
            archive.write_bytes(b"CORRUPTED_DATA")

            # Source is still available in scanned_files
            result = verify_romroot(env.romroot, db=db)

            assert result.corrupt == 1
            assert len(result.corrupt_files) == 1

            corrupt = result.corrupt_files[0]
            assert corrupt.recoverable_count > 0
            assert corrupt.lost_count == 0

            # File must still exist
            assert archive.exists()

    def test_no_source_requires_manual_intervention(self, env):
        """Corrupt file with no source → flagged as lost."""
        rom = make_rom(env.source / "Game.gba", b"NOSOURCE" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            archive = env.romroot / "F" / "Sys" / "Game.7z"
            assert archive.exists()

            # Corrupt the archive
            archive.write_bytes(b"CORRUPTED_DATA")

            # Verify with no DB — simulates no source info available
            result = verify_romroot(env.romroot, db=None)

            assert result.corrupt == 1
            assert len(result.corrupt_files) == 1

            corrupt = result.corrupt_files[0]
            assert corrupt.lost_count > 0
            assert corrupt.recoverable_count == 0

            # File must still exist
            assert archive.exists()


class TestVerifyClean:
    """Verify reports clean when nothing is corrupt."""

    def test_all_clean(self, env):
        """Uncorrupted romroot passes verification."""
        rom = make_rom(env.source / "Game.gba", b"CLEAN" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            result = verify_romroot(env.romroot, db=db)

            assert result.verified == 1
            assert result.corrupt == 0
            assert result.clean

    def test_empty_romroot(self, env):
        """Empty romroot is clean."""
        result = verify_romroot(env.romroot)
        assert result.clean
        assert result.total == 0

    def test_missing_sidecar_reported(self, env):
        """Archive without sidecar is reported but not as corrupt."""
        rom = make_rom(env.source / "Game.gba", b"NOSIDECAR" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # Delete the sidecar
            archive = env.romroot / "F" / "Sys" / "Game.7z"
            sidecar = archive.with_suffix(".7z.rscf")
            assert sidecar.exists()
            sidecar.unlink()

            result = verify_romroot(env.romroot, db=db)

            assert result.missing_sidecar == 1
            assert result.corrupt == 0
            assert not result.clean  # missing sidecar is not clean


class TestVerifyMultipleFiles:
    """Verify handles mixed clean/corrupt romroots."""

    def test_one_corrupt_one_clean(self, env):
        """Two games, one corrupt — verify reports both correctly."""
        rom_a = make_rom(env.source / "GameA.gba", b"AAAAAA" * 200)
        rom_b = make_rom(env.source / "GameB.gba", b"BBBBBB" * 200)
        entry_a = rom_entry(rom_a)
        entry_b = rom_entry(rom_b)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "GameA", "roms": [entry_a]},
            {"name": "GameB", "roms": [entry_b]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # Corrupt only GameA
            archive_a = env.romroot / "F" / "Sys" / "GameA.7z"
            assert archive_a.exists()
            archive_a.write_bytes(b"CORRUPTED")

            result = verify_romroot(env.romroot, db=db)

            assert result.verified == 1
            assert result.corrupt == 1
            assert result.corrupt_files[0].path == archive_a
