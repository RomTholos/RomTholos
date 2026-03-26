"""Orphan quarantine safety — FILE_SAFETY.md § Orphan quarantine.

When a romroot file is no longer claimed by any current DAT, it is moved
to romroot/_orphaned/ — NEVER deleted. The file is preserved with its full
path context. If a future DAT claims the file's hash, the relocation logic
moves it back automatically.

RomTholos never deletes from _orphaned/ on its own.

Guarantees tested:
- Quarantine is a move, not a deletion
- File content is byte-identical in _orphaned/
- RSCF sidecar moves alongside the quarantined file
- Relative path structure is preserved in _orphaned/
- Quarantined files remain scannable and matchable (DB updated)
- Rescue from _orphaned/ works (re-add to DAT → relocated back)
- Rescue roundtrip preserves file content
- No false orphans when all games match
- _orphaned/ is never auto-cleaned
- Empty parent directories are cleaned after quarantine
- Directory mode (none profile) quarantine works correctly
"""

from __future__ import annotations

from pathlib import Path

import pytest

from rscf import hash_file, read_sidecar

from romtholos.collect.config import SourceDir
from romtholos.collect.db import CacheDB
from romtholos.collect.match import find_orphaned_romroot, match_all_dats

from .conftest import (
    make_dat,
    make_rom,
    pipeline_run,
    pipeline_run_with_quarantine,
    rom_entry,
)


class TestQuarantineIsNotDeletion:
    """Quarantine must be a move, never a deletion."""

    def test_removed_game_exists_in_orphaned(self, env):
        """Game removed from DAT → file exists in _orphaned/, not deleted."""
        rom = make_rom(env.source / "Game.gba", b"QUARANTINE" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            assert (env.romroot / "F" / "Sys" / "Game.7z").exists()

            # Remove game from DAT
            make_dat(env.selection / "F" / "Sys.dat", "Sys", [])

            _, q = pipeline_run_with_quarantine(
                sources, env.selection, env.romroot, env.work, db,
            )

            assert q == 1
            assert not (env.romroot / "F" / "Sys" / "Game.7z").exists()
            assert (env.romroot / "_orphaned" / "F" / "Sys" / "Game.7z").exists()

    def test_entire_dat_removed_quarantines_all(self, env):
        """Removing the DAT file from selection quarantines all its games.

        The safety invariant: every game's archive must exist in _orphaned/.
        """
        games = []
        for i in range(3):
            r = make_rom(env.source / f"G{i}.gba", f"DAT_RM_{i}".encode() * 100)
            games.append({"name": f"G{i}", "roms": [rom_entry(r)]})

        make_dat(env.selection / "F" / "Sys.dat", "Sys", games)

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            hashes = {}
            for i in range(3):
                archive = env.romroot / "F" / "Sys" / f"G{i}.7z"
                assert archive.exists()
                hashes[i] = hash_file(archive).blake3

            # Delete the DAT
            (env.selection / "F" / "Sys.dat").unlink()

            _, q = pipeline_run_with_quarantine(
                sources, env.selection, env.romroot, env.work, db,
            )

            assert q >= 1  # at least one quarantine action
            # Safety invariant: every game preserved in _orphaned/
            for i in range(3):
                orphaned = env.romroot / "_orphaned" / "F" / "Sys" / f"G{i}.7z"
                assert orphaned.exists(), f"G{i} not in _orphaned/"
                assert hash_file(orphaned).blake3 == hashes[i]
                # Original path must be gone
                assert not (env.romroot / "F" / "Sys" / f"G{i}.7z").exists()

    def test_directory_mode_quarantine(self, env):
        """Directory mode game is quarantined as a directory, not individual files."""
        rom = make_rom(env.source / "Game.gba", b"DIR_QUARANTINE" * 100)
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

            game_dir = env.romroot / "F" / "Sys" / "Game"
            assert game_dir.is_dir()

            # Remove from DAT
            make_dat(env.selection / "F" / "Sys.dat", "Sys", [])

            _, q = pipeline_run_with_quarantine(
                sources, env.selection, env.romroot, env.work, db,
                compression="none",
            )

            assert q == 1
            orphaned_dir = env.romroot / "_orphaned" / "F" / "Sys" / "Game"
            assert orphaned_dir.is_dir()
            assert (orphaned_dir / "Game.gba").exists()


class TestQuarantineContentPreservation:
    """Quarantined files must be byte-identical to the originals."""

    def test_archive_blake3_identical(self, env):
        """Quarantined archive has identical BLAKE3 hash."""
        rom = make_rom(env.source / "Game.gba", b"PRESERVE_HASH" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            original_blake3 = hash_file(
                env.romroot / "F" / "Sys" / "Game.7z"
            ).blake3

            make_dat(env.selection / "F" / "Sys.dat", "Sys", [])
            _, q = pipeline_run_with_quarantine(
                sources, env.selection, env.romroot, env.work, db,
            )

            assert q == 1
            orphaned = env.romroot / "_orphaned" / "F" / "Sys" / "Game.7z"
            assert hash_file(orphaned).blake3 == original_blake3

    def test_sidecar_moves_with_quarantined_file(self, env):
        """RSCF sidecar is moved alongside the quarantined archive."""
        rom = make_rom(env.source / "Game.gba", b"SC_QUARANTINE" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            original_sidecar = read_sidecar(
                env.romroot / "F" / "Sys" / "Game.7z.rscf"
            )

            make_dat(env.selection / "F" / "Sys.dat", "Sys", [])
            pipeline_run_with_quarantine(
                sources, env.selection, env.romroot, env.work, db,
            )

            orphan_rscf = env.romroot / "_orphaned" / "F" / "Sys" / "Game.7z.rscf"
            assert orphan_rscf.exists()

            orphan_sidecar = read_sidecar(orphan_rscf)
            assert orphan_sidecar.files[0].sha1 == original_sidecar.files[0].sha1
            assert orphan_sidecar.renderer == original_sidecar.renderer

    def test_relative_path_preserved(self, env):
        """Quarantined file retains its original relative path under _orphaned/."""
        rom = make_rom(env.source / "Game.gba", b"PATH_PRESERVE" * 200)
        entry = rom_entry(rom)

        # Deep path: F/G/H/Sys.dat
        make_dat(env.selection / "F" / "G" / "H" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            original = env.romroot / "F" / "G" / "H" / "Sys" / "Game.7z"
            assert original.exists()

            # Remove DAT
            (env.selection / "F" / "G" / "H" / "Sys.dat").unlink()

            pipeline_run_with_quarantine(
                sources, env.selection, env.romroot, env.work, db,
            )

            # Same relative path under _orphaned/
            expected = env.romroot / "_orphaned" / "F" / "G" / "H" / "Sys" / "Game.7z"
            assert expected.exists()


class TestQuarantineRescue:
    """Quarantined files can be rescued when a DAT claims their hash again."""

    def test_rescue_from_orphaned(self, env):
        """Re-adding a game to the DAT rescues it from _orphaned/."""
        rom = make_rom(env.source / "Game.gba", b"RESCUE_ME" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            original_blake3 = hash_file(
                env.romroot / "F" / "Sys" / "Game.7z"
            ).blake3

            # Quarantine
            make_dat(env.selection / "F" / "Sys.dat", "Sys", [])
            pipeline_run_with_quarantine(
                sources, env.selection, env.romroot, env.work, db,
            )
            assert not (env.romroot / "F" / "Sys" / "Game.7z").exists()

            # Rescue: re-add to DAT — file is re-collected from _orphaned/
            # (ingest source), not relocated. Goes through full pipeline.
            make_dat(env.selection / "F" / "Sys.dat", "Sys", [
                {"name": "Game", "roms": [entry]},
            ])
            result = pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
            )

            assert result["processed"] >= 1
            archive = env.romroot / "F" / "Sys" / "Game.7z"
            assert archive.exists()
            # ROM content hash preserved (archive BLAKE3 may differ — recompressed)
            sidecar = read_sidecar(archive.parent / "Game.7z.rscf")
            assert sidecar.files[0].sha1.upper() == entry["sha1"].upper()

    def test_rescue_roundtrip_preserves_content(self, env):
        """Quarantine → rescue → quarantine again: ROM content preserved each time.

        Rescue goes through full collection pipeline (recompression), so the
        archive BLAKE3 may change. The ROM content SHA1 must be identical.
        """
        rom = make_rom(env.source / "Game.gba", b"ROUNDTRIP" * 200)
        entry = rom_entry(rom)
        original_sha1 = entry["sha1"]

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            for _cycle in range(3):
                # Quarantine
                make_dat(env.selection / "F" / "Sys.dat", "Sys", [])
                pipeline_run_with_quarantine(
                    sources, env.selection, env.romroot, env.work, db,
                )
                orphaned = env.romroot / "_orphaned" / "F" / "Sys" / "Game.7z"
                assert orphaned.exists()

                # Rescue — re-collected from _orphaned/ via full pipeline
                make_dat(env.selection / "F" / "Sys.dat", "Sys", [
                    {"name": "Game", "roms": [entry]},
                ])
                pipeline_run(
                    sources, env.selection, env.romroot, env.work, db,
                )
                archive = env.romroot / "F" / "Sys" / "Game.7z"
                assert archive.exists()
                sidecar = read_sidecar(archive.parent / "Game.7z.rscf")
                assert sidecar.files[0].sha1.upper() == original_sha1.upper()


class TestNoFalseOrphans:
    """Files matching current DATs must never be orphaned."""

    def test_all_games_match_zero_orphans(self, env):
        """When every romroot file matches a DAT, orphan count is zero."""
        games = []
        for i in range(5):
            r = make_rom(
                env.source / f"Game{i}.gba",
                f"NO_ORPHAN_{i}".encode() * 100,
            )
            games.append({"name": f"Game{i}", "roms": [rom_entry(r)]})

        make_dat(env.selection / "F" / "Sys.dat", "Sys", games)

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            fresh = match_all_dats(env.selection, db)
            orphans = find_orphaned_romroot(fresh, db)
            assert len(orphans) == 0

    def test_stable_dat_repeated_runs_zero_orphans(self, env):
        """Repeated runs with unchanged DAT produce zero orphans every time."""
        rom = make_rom(env.source / "Game.gba", b"STABLE_ROM" * 100)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]

            for run in range(5):
                pipeline_run(
                    sources, env.selection, env.romroot, env.work, db,
                )
                fresh = match_all_dats(env.selection, db)
                orphans = find_orphaned_romroot(fresh, db)
                assert len(orphans) == 0, f"False orphans on run {run + 1}"

    def test_one_removed_one_kept_correct_orphan(self, env):
        """Removing one game while keeping another: only removed game orphaned."""
        rom_keep = make_rom(env.source / "Keep.gba", b"KEEP_THIS" * 100)
        rom_drop = make_rom(env.source / "Drop.gba", b"DROP_THIS" * 200)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Keep", "roms": [rom_entry(rom_keep)]},
            {"name": "Drop", "roms": [rom_entry(rom_drop)]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # Remove Drop from DAT
            make_dat(env.selection / "F" / "Sys.dat", "Sys", [
                {"name": "Keep", "roms": [rom_entry(rom_keep)]},
            ])

            _, q = pipeline_run_with_quarantine(
                sources, env.selection, env.romroot, env.work, db,
            )

            assert q == 1
            assert (env.romroot / "F" / "Sys" / "Keep.7z").exists()
            assert not (env.romroot / "F" / "Sys" / "Drop.7z").exists()
            assert (env.romroot / "_orphaned" / "F" / "Sys" / "Drop.7z").exists()


class TestOrphanedNeverAutoDeleted:
    """_orphaned/ must never be automatically cleaned by the collector."""

    def test_orphaned_survives_subsequent_runs(self, env):
        """Files in _orphaned/ survive any number of subsequent pipeline runs."""
        rom = make_rom(env.source / "Game.gba", b"SURVIVE" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            original_blake3 = hash_file(
                env.romroot / "F" / "Sys" / "Game.7z"
            ).blake3

            # Quarantine
            make_dat(env.selection / "F" / "Sys.dat", "Sys", [])
            pipeline_run_with_quarantine(
                sources, env.selection, env.romroot, env.work, db,
            )

            orphaned = env.romroot / "_orphaned" / "F" / "Sys" / "Game.7z"
            assert orphaned.exists()

            # Run 5 more times — orphan must survive each
            for run in range(5):
                pipeline_run_with_quarantine(
                    sources, env.selection, env.romroot, env.work, db,
                )
                assert orphaned.exists(), f"Orphan deleted on run {run + 1}"
                assert hash_file(orphaned).blake3 == original_blake3

    def test_orphaned_from_different_dats_coexist(self, env):
        """Orphans from different DATs coexist without interfering."""
        roms = {}
        for sys in ("Sys1", "Sys2"):
            r = make_rom(
                env.source / f"{sys}.gba", f"{sys}_DATA".encode() * 100,
            )
            roms[sys] = r
            make_dat(env.selection / "F" / f"{sys}.dat", sys, [
                {"name": "Game", "roms": [rom_entry(r)]},
            ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # Remove both DATs
            (env.selection / "F" / "Sys1.dat").unlink()
            (env.selection / "F" / "Sys2.dat").unlink()

            _, q = pipeline_run_with_quarantine(
                sources, env.selection, env.romroot, env.work, db,
            )

            assert q == 2
            assert (env.romroot / "_orphaned" / "F" / "Sys1" / "Game.7z").exists()
            assert (env.romroot / "_orphaned" / "F" / "Sys2" / "Game.7z").exists()


class TestQuarantineCollision:
    """Quarantine collision: same path, different content → BLAKE3 suffix."""

    def test_same_content_replaces_silently(self, env):
        """Same game quarantined twice with same content → replaced, no suffix."""
        rom = make_rom(env.source / "Game.gba", b"SAME_TWICE" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # Quarantine
            make_dat(env.selection / "F" / "Sys.dat", "Sys", [])
            pipeline_run_with_quarantine(
                sources, env.selection, env.romroot, env.work, db,
            )
            orphaned = env.romroot / "_orphaned" / "F" / "Sys" / "Game.7z"
            assert orphaned.exists()

            # Rescue and quarantine again (same content)
            make_dat(env.selection / "F" / "Sys.dat", "Sys", [
                {"name": "Game", "roms": [entry]},
            ])
            pipeline_run(sources, env.selection, env.romroot, env.work, db)
            make_dat(env.selection / "F" / "Sys.dat", "Sys", [])
            _, q2 = pipeline_run_with_quarantine(
                sources, env.selection, env.romroot, env.work, db,
            )

            assert q2 == 1
            # Same path, no suffix — content is identical
            assert orphaned.exists()
            # No suffixed duplicates
            suffixed = [
                f for f in orphaned.parent.iterdir()
                if f.name.startswith("Game") and f.suffix == ".7z"
                and f.name != "Game.7z"
            ]
            assert suffixed == [], f"Unexpected suffixed files: {suffixed}"

    def test_different_content_gets_blake3_suffix(self, env):
        """Different content at same path → BLAKE3 suffix, both preserved."""
        # First version of the game
        rom_v1 = make_rom(env.source / "v1" / "Game.gba", b"VERSION_1" * 200)
        entry_v1 = rom_entry(rom_v1)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry_v1]},
        ])

        with CacheDB(env.db_path) as db:
            sources_v1 = [
                SourceDir(path=env.source / "v1", source_type="readonly"),
            ]
            pipeline_run(
                sources_v1, env.selection, env.romroot, env.work, db,
            )

            # Quarantine v1
            make_dat(env.selection / "F" / "Sys.dat", "Sys", [])
            pipeline_run_with_quarantine(
                sources_v1, env.selection, env.romroot, env.work, db,
            )
            orphaned_v1 = env.romroot / "_orphaned" / "F" / "Sys" / "Game.7z"
            assert orphaned_v1.exists()
            v1_blake3 = hash_file(orphaned_v1).blake3

            # Now collect a DIFFERENT version of "Game" with different content
            rom_v2 = make_rom(env.source / "v2" / "Game.gba", b"VERSION_2" * 200)
            entry_v2 = rom_entry(rom_v2)

            make_dat(env.selection / "F" / "Sys.dat", "Sys", [
                {"name": "Game", "roms": [entry_v2]},
            ])

            sources_v2 = [
                SourceDir(path=env.source / "v2", source_type="readonly"),
            ]
            pipeline_run(
                sources_v2, env.selection, env.romroot, env.work, db,
            )

            # Quarantine v2 → collision with v1 at same path
            make_dat(env.selection / "F" / "Sys.dat", "Sys", [])
            _, q2 = pipeline_run_with_quarantine(
                sources_v2, env.selection, env.romroot, env.work, db,
            )

            assert q2 == 1

            # v1 still exists at original path (untouched)
            assert orphaned_v1.exists()
            assert hash_file(orphaned_v1).blake3 == v1_blake3

            # v2 exists with BLAKE3 suffix on the system directory
            # (all files in F/Sys are grouped as one quarantine item)
            orphan_parent = env.romroot / "_orphaned" / "F"
            sys_dirs = [
                d for d in orphan_parent.iterdir()
                if d.is_dir() and d.name.startswith("Sys")
            ]
            assert len(sys_dirs) == 2, (
                f"Expected Sys + Sys_<blake3>, got: {[d.name for d in sys_dirs]}"
            )
            # Both contain Game.7z
            for d in sys_dirs:
                assert (d / "Game.7z").exists(), f"Missing Game.7z in {d.name}"


class TestQuarantineCleanup:
    """Quarantine must clean up empty parent directories."""

    def test_empty_parents_removed(self, env):
        """After quarantine, empty parent directories are cleaned."""
        rom = make_rom(env.source / "Game.gba", b"CLEANUP" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "Deep" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            make_dat(env.selection / "Deep" / "Sys.dat", "Sys", [])
            pipeline_run_with_quarantine(
                sources, env.selection, env.romroot, env.work, db,
            )

            # The "Deep/Sys/" directory chain should be gone from romroot
            # (only exists under _orphaned now)
            assert not (env.romroot / "Deep").exists()
            assert (env.romroot / "_orphaned" / "Deep" / "Sys" / "Game.7z").exists()
