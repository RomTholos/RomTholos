"""Shared hash safety — FILE_SAFETY.md (cross-cutting concern).

Multiple games in a DAT can reference the same ROM hash (e.g. a shared
audio track across disc games). Each game must get its own archive with
its own copy of the shared ROM. Shared hashes must never cause:
- False orphans (one game's archive incorrectly orphaned)
- Cross-matching (game A's archive satisfying game B's match)
- Data loss during relocation, profile change, or quarantine

Guarantees tested:
- Multiple games sharing a ROM hash: all collected, all in romroot
- Shared-hash games: zero orphans on stable DAT
- Shared-hash games: survive relocation without cross-matching
- Shared-hash games: survive profile change
- Removing one shared-hash game: only that game orphaned
- Shared hash across different DATs: independent handling
"""

from __future__ import annotations

from pathlib import Path

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


def _make_shared_hash_dat(selection, games_with_shared, shared_entry):
    """Create a DAT where multiple games share one ROM (by hash).

    Each game has one unique track and the shared track.
    Returns dict of game_name → unique_entry for later verification.
    """
    unique_entries = {}
    dat_games = []
    for game_name, unique_entry in games_with_shared:
        unique_entries[game_name] = unique_entry
        dat_games.append({
            "name": game_name,
            "roms": [shared_entry, unique_entry],
        })
    make_dat(selection, "Sys", dat_games)
    return unique_entries


class TestSharedHashCollection:
    """All games sharing a hash must be independently collected."""

    def test_three_games_shared_track_all_collected(self, env):
        """Three games sharing one ROM: all three archives created."""
        shared = make_rom(env.source / "shared.bin", b"SHARED_TRACK" * 100)
        shared_e = rom_entry(shared)

        games = []
        for i in range(3):
            unique = make_rom(
                env.source / f"unique_{i}.bin",
                f"UNIQUE_{i}_DATA".encode() * 200,
            )
            games.append((f"Game{i}", rom_entry(unique)))

        _make_shared_hash_dat(
            env.selection / "F" / "Sys.dat", games, shared_e,
        )

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            result = pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
            )

            # All ROMs processed (3 games × 2 ROMs each = 6)
            assert result["processed"] == 6

            # All three archives exist
            for i in range(3):
                archive = env.romroot / "F" / "Sys" / f"Game{i}.7z"
                assert archive.exists(), f"Game{i} archive missing"

                sidecar = read_sidecar(archive.parent / f"Game{i}.7z.rscf")
                assert len(sidecar.files) == 2

    def test_shared_hash_each_archive_has_own_copy(self, env):
        """Each game's archive contains its own copy of the shared ROM."""
        shared = make_rom(env.source / "shared.bin", b"OWN_COPY" * 100)
        shared_e = rom_entry(shared)
        shared_sha1 = shared_e["sha1"]

        games = []
        for i in range(2):
            unique = make_rom(
                env.source / f"u{i}.bin", f"UNIQUE_{i}".encode() * 200,
            )
            games.append((f"Game{i}", rom_entry(unique)))

        _make_shared_hash_dat(
            env.selection / "F" / "Sys.dat", games, shared_e,
        )

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # Both archives contain the shared track
            for i in range(2):
                sidecar = read_sidecar(
                    env.romroot / "F" / "Sys" / f"Game{i}.7z.rscf",
                )
                shared_entries = [
                    f for f in sidecar.files
                    if f.sha1.upper() == shared_sha1.upper()
                ]
                assert len(shared_entries) == 1, (
                    f"Game{i} missing shared track in sidecar"
                )


class TestSharedHashNoFalseOrphans:
    """Shared hashes must never produce false orphans."""

    def test_zero_orphans_on_stable_dat(self, env):
        """Stable DAT with shared hashes: zero orphans across runs."""
        shared = make_rom(env.source / "shared.bin", b"STABLE_SHARED" * 100)
        shared_e = rom_entry(shared)

        games = []
        for i in range(3):
            unique = make_rom(
                env.source / f"u{i}.bin", f"STABLE_{i}".encode() * 200,
            )
            games.append((f"Game{i}", rom_entry(unique)))

        _make_shared_hash_dat(
            env.selection / "F" / "Sys.dat", games, shared_e,
        )

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]

            # Initial collection
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # Three re-runs: zero orphans each time
            for run in range(3):
                r = pipeline_run(
                    sources, env.selection, env.romroot, env.work, db,
                )
                assert r["processed"] == 0
                assert r["skipped"] == 6  # 3 games × 2 ROMs

                fresh = match_all_dats(env.selection, db)
                orphans = find_orphaned_romroot(fresh, db)
                assert len(orphans) == 0, (
                    f"False orphans on run {run + 1}: {orphans}"
                )

    def test_remove_one_shared_game_others_survive(self, env):
        """Removing one game that shares a hash: only that game orphaned."""
        shared = make_rom(env.source / "shared.bin", b"REMOVE_ONE" * 100)
        shared_e = rom_entry(shared)

        unique_entries = {}
        games = []
        for i in range(3):
            unique = make_rom(
                env.source / f"u{i}.bin", f"REMOVE_{i}".encode() * 200,
            )
            e = rom_entry(unique)
            unique_entries[f"Game{i}"] = e
            games.append((f"Game{i}", e))

        _make_shared_hash_dat(
            env.selection / "F" / "Sys.dat", games, shared_e,
        )

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # Remove Game1 from DAT (keep Game0 and Game2)
            remaining = [g for g in games if g[0] != "Game1"]
            _make_shared_hash_dat(
                env.selection / "F" / "Sys.dat", remaining, shared_e,
            )

            _, q = pipeline_run_with_quarantine(
                sources, env.selection, env.romroot, env.work, db,
            )

            assert q == 1
            assert not (env.romroot / "F" / "Sys" / "Game1.7z").exists()
            assert (env.romroot / "_orphaned" / "F" / "Sys" / "Game1.7z").exists()

            # Other games still in romroot
            assert (env.romroot / "F" / "Sys" / "Game0.7z").exists()
            assert (env.romroot / "F" / "Sys" / "Game2.7z").exists()


class TestSharedHashRelocation:
    """Shared-hash games must survive relocation without cross-matching."""

    def test_relocate_all_shared_games(self, env):
        """Moving the DAT: all shared-hash games relocate correctly."""
        shared = make_rom(env.source / "shared.bin", b"RELOC_SHARED" * 100)
        shared_e = rom_entry(shared)

        games = []
        original_blake3s = {}
        for i in range(2):
            unique = make_rom(
                env.source / f"u{i}.bin", f"RELOC_{i}".encode() * 200,
            )
            games.append((f"Game{i}", rom_entry(unique)))

        _make_shared_hash_dat(
            env.selection / "FolderA" / "Sys.dat", games, shared_e,
        )

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            for i in range(2):
                original_blake3s[i] = hash_file(
                    env.romroot / "FolderA" / "Sys" / f"Game{i}.7z"
                ).blake3

            # Move DAT
            (env.selection / "FolderB").mkdir(parents=True)
            (env.selection / "FolderA" / "Sys.dat").rename(
                env.selection / "FolderB" / "Sys.dat",
            )
            (env.selection / "FolderA").rmdir()

            result = pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
            )

            assert result["relocated"] == 2

            # Both at new location, byte-identical
            for i in range(2):
                new_archive = env.romroot / "FolderB" / "Sys" / f"Game{i}.7z"
                assert new_archive.exists()
                assert hash_file(new_archive).blake3 == original_blake3s[i]

            # Zero orphans
            fresh = match_all_dats(env.selection, db)
            orphans = find_orphaned_romroot(fresh, db)
            assert len(orphans) == 0


class TestSharedHashProfileChange:
    """Profile changes must work correctly with shared hashes."""

    def test_profile_change_preserves_all_shared_games(self, env):
        """Profile change: all shared-hash games recompressed, content intact."""
        shared = make_rom(env.source / "shared.bin", b"PROF_SHARED" * 100)
        shared_e = rom_entry(shared)
        shared_sha1 = shared_e["sha1"]

        unique_sha1s = {}
        games = []
        for i in range(2):
            unique = make_rom(
                env.source / f"u{i}.bin", f"PROF_{i}".encode() * 200,
            )
            e = rom_entry(unique)
            unique_sha1s[f"Game{i}"] = e["sha1"]
            games.append((f"Game{i}", e))

        _make_shared_hash_dat(
            env.selection / "F" / "Sys.dat", games, shared_e,
        )

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]

            pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="7z-16m",
            )
            pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="zstd-19",
            )

            for i in range(2):
                archive = env.romroot / "F" / "Sys" / f"Game{i}.tar.zst"
                assert archive.exists(), f"Game{i} missing after profile change"

                sidecar = read_sidecar(
                    archive.parent / f"Game{i}.tar.zst.rscf",
                )
                assert sidecar.renderer == "zstd-19"
                assert len(sidecar.files) == 2

                hashes = {f.sha1.upper() for f in sidecar.files}
                assert shared_sha1.upper() in hashes
                assert unique_sha1s[f"Game{i}"].upper() in hashes


class TestSharedHashAcrossDats:
    """Same hash appearing in different DATs: independent handling."""

    def test_same_hash_different_dats(self, env):
        """Same ROM hash in two different DATs: both collected independently."""
        shared_content = b"CROSS_DAT_ROM" * 100
        rom = make_rom(env.source / "shared.bin", shared_content)
        shared_e = rom_entry(rom)

        # DAT 1: shared + unique1
        unique1 = make_rom(env.source / "u1.bin", b"UNIQUE_DAT1" * 200)
        make_dat(env.selection / "F" / "Sys1.dat", "Sys1", [
            {"name": "Game1", "roms": [shared_e, rom_entry(unique1)]},
        ])

        # DAT 2: shared + unique2
        unique2 = make_rom(env.source / "u2.bin", b"UNIQUE_DAT2" * 200)
        make_dat(env.selection / "F" / "Sys2.dat", "Sys2", [
            {"name": "Game2", "roms": [shared_e, rom_entry(unique2)]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            result = pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
            )

            # Both games collected
            assert (env.romroot / "F" / "Sys1" / "Game1.7z").exists()
            assert (env.romroot / "F" / "Sys2" / "Game2.7z").exists()

            # Zero orphans
            fresh = match_all_dats(env.selection, db)
            orphans = find_orphaned_romroot(fresh, db)
            assert len(orphans) == 0

    def test_remove_one_dat_other_survives(self, env):
        """Removing one DAT: games from other DAT with same hash survive."""
        shared = make_rom(env.source / "shared.bin", b"CROSS_SURVIVE" * 100)
        shared_e = rom_entry(shared)

        unique1 = make_rom(env.source / "u1.bin", b"XDAT_U1" * 200)
        make_dat(env.selection / "F" / "Sys1.dat", "Sys1", [
            {"name": "Game1", "roms": [shared_e, rom_entry(unique1)]},
        ])

        unique2 = make_rom(env.source / "u2.bin", b"XDAT_U2" * 200)
        make_dat(env.selection / "F" / "Sys2.dat", "Sys2", [
            {"name": "Game2", "roms": [shared_e, rom_entry(unique2)]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # Remove Sys1
            (env.selection / "F" / "Sys1.dat").unlink()

            _, q = pipeline_run_with_quarantine(
                sources, env.selection, env.romroot, env.work, db,
            )

            assert q == 1
            assert (env.romroot / "_orphaned" / "F" / "Sys1" / "Game1.7z").exists()
            assert (env.romroot / "F" / "Sys2" / "Game2.7z").exists()
