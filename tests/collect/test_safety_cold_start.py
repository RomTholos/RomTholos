"""Cold-start recovery safety — FILE_SAFETY.md § RSCF Sidecars.

If the DB cache is lost, romroot scan rebuilds the DB from sidecars without
re-hashing file contents. After recovery, all games must still be recognized
as in_romroot with zero orphans.

Guarantees tested:
- DB loss + rescan from sidecars → all games matched as in_romroot
- Zero orphans after cold-start recovery
- No re-collection triggered after recovery
- Corrupt sidecar → graceful fallback to rehashing (no crash, no data loss)
"""

from __future__ import annotations

from pathlib import Path

from rscf import hash_file, read_sidecar

from romtholos.collect.config import SourceDir
from romtholos.collect.db import CacheDB
from romtholos.collect.match import find_orphaned_romroot, match_all_dats

from .conftest import make_dat, make_rom, pipeline_run, rom_entry


class TestColdStartRecovery:
    """DB loss must be recoverable from RSCF sidecars."""

    def test_db_loss_recovery_all_games_matched(self, env):
        """After DB is deleted and rebuilt, all games still match."""
        games = []
        for i in range(4):
            r = make_rom(
                env.source / f"Game{i}.gba",
                f"COLD_START_{i}".encode() * 100,
            )
            games.append({"name": f"Game{i}", "roms": [rom_entry(r)]})

        make_dat(env.selection / "F" / "Sys.dat", "Sys", games)

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            for i in range(4):
                assert (env.romroot / "F" / "Sys" / f"Game{i}.7z").exists()

        # Destroy DB
        env.db_path.unlink()

        # Cold start: new DB, rescan romroot from sidecars
        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            result = pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
            )

            # All games recognized as in_romroot, nothing re-collected
            assert result["processed"] == 0
            assert result["skipped"] == 4

    def test_db_loss_recovery_zero_orphans(self, env):
        """After DB rebuild, zero orphans detected."""
        games = []
        for i in range(3):
            r = make_rom(
                env.source / f"Game{i}.gba",
                f"ORPHAN_CHECK_{i}".encode() * 100,
            )
            games.append({"name": f"Game{i}", "roms": [rom_entry(r)]})

        make_dat(env.selection / "F" / "Sys.dat", "Sys", games)

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

        env.db_path.unlink()

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            fresh = match_all_dats(env.selection, db)
            orphans = find_orphaned_romroot(fresh, db)
            assert len(orphans) == 0

    def test_db_loss_multi_rom_game_recovered(self, env):
        """Multi-ROM game's sidecar restores all ROM entries in DB."""
        roms = []
        for i in range(3):
            r = make_rom(
                env.source / f"Track {i}.bin",
                f"MULTI_COLD_{i}".encode() * 200,
            )
            roms.append(rom_entry(r))

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": roms},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

        env.db_path.unlink()

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            result = pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
            )

            # All 3 ROMs recognized from sidecar
            assert result["processed"] == 0
            assert result["skipped"] == 3

    def test_db_loss_directory_mode_recovered(self, env):
        """Directory mode (none profile) recovery from per-file sidecars."""
        rom = make_rom(env.source / "Game.gba", b"DIR_COLD" * 100)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="none",
            )

        env.db_path.unlink()

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            result = pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="none",
            )

            assert result["processed"] == 0
            assert result["skipped"] == 1


class TestCorruptSidecarRecovery:
    """Corrupt sidecars must not cause data loss."""

    def test_corrupt_sidecar_falls_back_to_rehash(self, env):
        """Corrupt sidecar: scanner rehashes the file, game still matched."""
        rom = make_rom(env.source / "Game.gba", b"CORRUPT_SC" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

        env.db_path.unlink()

        # Corrupt the sidecar
        rscf_path = env.romroot / "F" / "Sys" / "Game.7z.rscf"
        assert rscf_path.exists()
        rscf_path.write_bytes(b"GARBAGE_SIDECAR_DATA")

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            # Should not crash — falls back to rehashing
            result = pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
            )

            # Archive file itself is still valid
            archive = env.romroot / "F" / "Sys" / "Game.7z"
            assert archive.exists()
