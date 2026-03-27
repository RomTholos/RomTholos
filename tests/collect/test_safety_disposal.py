"""Disposal mode safety — FILE_SAFETY.md § Source Modes / Disposal.

Disposal mode extends ingest with post-collection deletion. A source
file may only be deleted when ALL conditions are met:
1. Content hashes verified in romroot copy
2. Romroot copy BLAKE3-verified after write (re-read from target)
3. RSCF sidecar written and verified in romroot
4. No other game references the same source file (by filesystem path)

This is the ONLY place in the entire system where source files may be deleted.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from rscf import hash_file

from romtholos.collect.config import SourceDir
from romtholos.collect.db import CacheDB

from .conftest import make_dat, make_rom, make_zip, pipeline_run, rom_entry


class TestDisposalConditions:
    """All 4 verification conditions must pass before source deletion."""

    def test_source_deleted_after_all_conditions_met(self, env):
        """Source file deleted only when all 4 conditions are satisfied."""
        rom = make_rom(env.source / "Game.gba", b"DISPOSE_OK" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="disposal")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # Source should be deleted after verified collection
            assert not rom.exists(), "Source not deleted after verified collection"

            # Romroot copy must exist and be correct
            archive = env.romroot / "F" / "Sys" / "Game.7z"
            assert archive.exists()

    def test_shared_source_not_deleted_until_all_games_collected(self, env):
        """Source archive serving multiple games: not deleted until all collected.

        Condition 4: no other game references the same source file.
        """
        content_a = b"ROM_A" * 200
        content_b = b"ROM_B" * 300

        # Single source archive containing both ROMs
        archive = make_zip(
            env.source / "bundle.zip",
            {"GameA.gba": content_a, "GameB.gba": content_b},
        )

        # Create temp files to get hashes
        tmp_a = env.work / "a.gba"
        tmp_a.parent.mkdir(parents=True, exist_ok=True)
        tmp_a.write_bytes(content_a)
        ha = hash_file(tmp_a)

        tmp_b = env.work / "b.gba"
        tmp_b.write_bytes(content_b)
        hb = hash_file(tmp_b)

        # DAT references both ROMs
        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "GameA", "roms": [{
                "name": "GameA.gba", "size": len(content_a), "sha1": ha.sha1,
            }]},
            {"name": "GameB", "roms": [{
                "name": "GameB.gba", "size": len(content_b), "sha1": hb.sha1,
            }]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="disposal")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # Both games collected
            assert (env.romroot / "F" / "Sys" / "GameA.7z").exists()
            assert (env.romroot / "F" / "Sys" / "GameB.7z").exists()

            # Source archive should now be deleted (both games collected)
            assert not archive.exists()

    def test_verification_failure_preserves_source(self, env):
        """If romroot verification fails, source must NOT be deleted.

        When verification fails the game is not marked as collected,
        so the disposal condition is never satisfied.
        """
        rom = make_rom(env.source / "Game.gba", b"NO_DELETE" * 200)
        entry = rom_entry(rom)
        original_hash = hash_file(rom).blake3

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        from unittest.mock import patch
        from romtholos.collect import execute as exe_mod

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="disposal")]

            with patch.object(
                exe_mod, "_verify_on_target", return_value=False,
            ):
                pipeline_run(
                    sources, env.selection, env.romroot, env.work, db,
                )

            # Source MUST survive verification failure
            assert rom.exists()
            assert hash_file(rom).blake3 == original_hash

    def test_readonly_source_never_deleted(self, env):
        """Read-only sources are never deleted regardless of collection."""
        rom = make_rom(env.source / "Game.gba", b"READONLY" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            assert rom.exists(), "Read-only source must never be deleted"

    def test_ingest_source_never_deleted(self, env):
        """Ingest sources are never deleted regardless of collection."""
        rom = make_rom(env.source / "Game.gba", b"INGEST" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="ingest")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            assert rom.exists(), "Ingest source must never be deleted"

    def test_mode_change_readonly_to_disposal_deletes_source(self, env):
        """Source collected as read-only then mode changed to disposal.

        When a source was previously scanned as read-only and ROMs are
        already in romroot, changing the mode to disposal should delete
        the source files on the next run.
        """
        rom = make_rom(env.source / "Game.gba", b"MODE_CHANGE" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            # First run: read-only — collects to romroot, source preserved
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)
            assert rom.exists(), "Read-only source must survive"
            assert (env.romroot / "F" / "Sys" / "Game.7z").exists()

            # Second run: same source as disposal — should delete
            sources = [SourceDir(path=env.source, source_type="disposal")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)
            assert not rom.exists(), (
                "Source not deleted after mode change to disposal"
            )

    def test_archive_with_untracked_entries_not_disposed(self, env):
        """Archive containing files not in any DAT must NOT be disposed.

        If collectionA.zip has rom.bin (matches DAT, collected to romroot)
        and extra.bin (matches no DAT), the archive must be preserved —
        disposing it would silently destroy extra.bin.
        """
        content_rom = b"TRACKED_ROM" * 200
        content_extra = b"UNTRACKED_EXTRA" * 300

        archive = make_zip(
            env.source / "collection.zip",
            {"Game.gba": content_rom, "extra.bin": content_extra},
        )

        # DAT only references Game.gba, not extra.bin
        tmp = env.work / "tmp_rom.gba"
        tmp.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_bytes(content_rom)
        h = hash_file(tmp)
        tmp.unlink()

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [{
                "name": "Game.gba", "size": len(content_rom), "sha1": h.sha1,
            }]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="disposal")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # Game collected to romroot
            assert (env.romroot / "F" / "Sys" / "Game.7z").exists()

            # Archive must NOT be disposed — extra.bin is untracked
            assert archive.exists(), (
                "Archive with untracked entries was disposed — data loss!"
            )

    def test_archive_fully_tracked_is_disposed(self, env):
        """Archive where ALL entries are in romroot IS disposed."""
        content_a = b"ROM_A_FULL" * 200
        content_b = b"ROM_B_FULL" * 300

        archive = make_zip(
            env.source / "complete.zip",
            {"GameA.gba": content_a, "GameB.gba": content_b},
        )

        tmp_a = env.work / "a.gba"
        tmp_a.parent.mkdir(parents=True, exist_ok=True)
        tmp_a.write_bytes(content_a)
        ha = hash_file(tmp_a)

        tmp_b = env.work / "b.gba"
        tmp_b.write_bytes(content_b)
        hb = hash_file(tmp_b)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "GameA", "roms": [{
                "name": "GameA.gba", "size": len(content_a), "sha1": ha.sha1,
            }]},
            {"name": "GameB", "roms": [{
                "name": "GameB.gba", "size": len(content_b), "sha1": hb.sha1,
            }]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="disposal")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            assert (env.romroot / "F" / "Sys" / "GameA.7z").exists()
            assert (env.romroot / "F" / "Sys" / "GameB.7z").exists()

            # All entries tracked — archive should be disposed
            assert not archive.exists()

    def test_duplicate_across_disposal_sources(self, env):
        """Same ROM in two disposal sources — both disposed in one run.

        Source A and B both contain Game.gba with identical content.
        After collection from one source, both copies are redundant and
        should be disposed without requiring a second run.
        """
        src_b = env.source.parent / "source_b"
        src_b.mkdir()

        content = b"DUPLICATE_ROM" * 200
        rom_a = make_rom(env.source / "Game.gba", content)
        rom_b = make_rom(src_b / "Game.gba", content)
        entry = rom_entry(rom_a)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [
                SourceDir(path=env.source, source_type="disposal"),
                SourceDir(path=src_b, source_type="disposal"),
            ]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # Game collected
            assert (env.romroot / "F" / "Sys" / "Game.7z").exists()

            # Both disposal copies disposed in one run
            assert not rom_a.exists(), "Source A not disposed"
            assert not rom_b.exists(), "Source B not disposed"

    def test_second_run_source_already_gone(self, env):
        """Second run with disposal source already deleted doesn't crash."""
        rom = make_rom(env.source / "Game.gba", b"DISPOSE2" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="disposal")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)
            assert not rom.exists()

            # Second run — source gone, game already in romroot
            pipeline_run(sources, env.selection, env.romroot, env.work, db)
            assert (env.romroot / "F" / "Sys" / "Game.7z").exists()
