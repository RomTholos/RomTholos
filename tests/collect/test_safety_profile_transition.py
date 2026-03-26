"""Profile transition safety — FILE_SAFETY.md § Profile transition.

When the configured compression profile changes for a system, the old archive
is extracted, recompressed with the new profile, verified, and only then is
the old archive deleted.

The old archive is deleted ONLY AFTER the new archive is verified on the
target filesystem. If any step fails, the old archive remains untouched.

Guarantees tested:
- ROM content hashes preserved across profile change
- Old archive gone only after new is verified
- New sidecar has correct renderer field
- Extension changes correctly (7z→zst, zst→7z, etc.)
- Old sidecar is replaced by new sidecar
- Verification failure leaves old archive untouched
- Multiple profile changes preserve content through all transitions
- Zstd single↔multi transition (extension change without profile change)
- Directory mode ↔ archive mode transitions
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from rscf import hash_file, read_sidecar

from romtholos.collect.config import SourceDir
from romtholos.collect.db import CacheDB

from .conftest import make_dat, make_rom, pipeline_run, rom_entry


class TestProfileChangePreservesContent:
    """ROM content must be preserved across any profile change."""

    def test_7z_to_zstd(self, env):
        """7z → zstd-19: ROM SHA1 identical, extension changes."""
        rom = make_rom(env.source / "Game.gba", b"7Z_TO_ZSTD" * 200)
        entry = rom_entry(rom)
        original_sha1 = entry["sha1"]

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]

            pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="7z-16m",
            )
            old = env.romroot / "F" / "Sys" / "Game.7z"
            assert old.exists()

            pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="zstd-19",
            )
            new = env.romroot / "F" / "Sys" / "Game.zst"
            assert new.exists()
            assert not old.exists()

            sidecar = read_sidecar(new.parent / "Game.zst.rscf")
            assert sidecar.files[0].sha1.upper() == original_sha1.upper()
            assert sidecar.renderer == "zstd-19"

    def test_zstd_to_7z(self, env):
        """zstd-19 → 7z-16m: ROM SHA1 identical, extension changes."""
        rom = make_rom(env.source / "Game.gba", b"ZSTD_TO_7Z" * 200)
        entry = rom_entry(rom)
        original_sha1 = entry["sha1"]

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]

            pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="zstd-19",
            )
            assert (env.romroot / "F" / "Sys" / "Game.zst").exists()

            pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="7z-16m",
            )
            new = env.romroot / "F" / "Sys" / "Game.7z"
            assert new.exists()
            assert not (env.romroot / "F" / "Sys" / "Game.zst").exists()

            sidecar = read_sidecar(new.parent / "Game.7z.rscf")
            assert sidecar.files[0].sha1.upper() == original_sha1.upper()
            assert sidecar.renderer == "7z-16m"

    def test_7z_to_none(self, env):
        """7z → none (directory): ROM SHA1 identical, archive becomes dir."""
        rom = make_rom(env.source / "Game.gba", b"7Z_TO_NONE" * 200)
        entry = rom_entry(rom)
        original_sha1 = entry["sha1"]

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]

            pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="7z-16m",
            )
            assert (env.romroot / "F" / "Sys" / "Game.7z").exists()

            pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="none",
            )
            game_dir = env.romroot / "F" / "Sys" / "Game"
            assert game_dir.is_dir()
            assert not (env.romroot / "F" / "Sys" / "Game.7z").exists()

            rom_file = game_dir / "Game.gba"
            assert rom_file.exists()
            assert hash_file(rom_file).sha1.upper() == original_sha1.upper()

    def test_none_to_7z(self, env):
        """none (directory) → 7z: ROM SHA1 identical, dir becomes archive."""
        rom = make_rom(env.source / "Game.gba", b"NONE_TO_7Z" * 200)
        entry = rom_entry(rom)
        original_sha1 = entry["sha1"]

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]

            pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="none",
            )
            assert (env.romroot / "F" / "Sys" / "Game" / "Game.gba").exists()

            pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="7z-16m",
            )
            archive = env.romroot / "F" / "Sys" / "Game.7z"
            assert archive.exists()
            assert not (env.romroot / "F" / "Sys" / "Game").exists()

            sidecar = read_sidecar(archive.parent / "Game.7z.rscf")
            assert sidecar.files[0].sha1.upper() == original_sha1.upper()

    def test_multi_rom_all_tracks_preserved(self, env):
        """Profile change on multi-ROM game: all tracks preserved."""
        original_sha1s = {}
        rom_entries = []
        for i in range(3):
            r = make_rom(
                env.source / f"Track {i}.bin",
                f"MULTI_TRACK_{i}".encode() * 200,
            )
            e = rom_entry(r)
            original_sha1s[f"Track {i}.bin"] = e["sha1"]
            rom_entries.append(e)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": rom_entries},
        ])

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

            archive = env.romroot / "F" / "Sys" / "Game.tar.zst"
            assert archive.exists()

            sidecar = read_sidecar(archive.parent / "Game.tar.zst.rscf")
            assert len(sidecar.files) == 3

            for f in sidecar.files:
                name = Path(f.path).name
                assert name in original_sha1s, f"Unexpected ROM: {name}"
                assert f.sha1.upper() == original_sha1s[name].upper()


class TestOldArchiveDeletedOnlyAfterVerification:
    """Old archive must survive until the new one is verified."""

    def test_verification_failure_keeps_old_archive(self, env):
        """If new archive verification fails, old archive stays untouched."""
        rom = make_rom(env.source / "Game.gba", b"FAIL_VERIFY" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]

            # Initial collection with 7z
            pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="7z-16m",
            )
            old_archive = env.romroot / "F" / "Sys" / "Game.7z"
            assert old_archive.exists()
            old_blake3 = hash_file(old_archive).blake3

            # Attempt profile change with verification forced to fail
            from romtholos.collect import execute as exe_mod
            original_verify = exe_mod._verify_on_target

            def fail_new_only(target_path, expected_blake3):
                # Fail verification for zst files (the new format)
                if str(target_path).endswith(".zst"):
                    return False
                return original_verify(target_path, expected_blake3)

            with patch.object(
                exe_mod, "_verify_on_target", side_effect=fail_new_only,
            ):
                result = pipeline_run(
                    sources, env.selection, env.romroot, env.work, db,
                    compression="zstd-19",
                )

            assert result["failed"] >= 1

            # Old archive MUST still exist and be intact
            assert old_archive.exists(), "Old archive deleted despite verification failure"
            assert hash_file(old_archive).blake3 == old_blake3

            # New archive must NOT exist
            assert not (env.romroot / "F" / "Sys" / "Game.zst").exists()

    def test_no_orphan_state_between_old_delete_and_new_create(self, env):
        """At no point should both old and new archives exist simultaneously
        (except transiently on disk). After a successful transition, only the
        new archive exists.
        """
        rom = make_rom(env.source / "Game.gba", b"NO_OVERLAP" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

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

            # Only the new format should exist
            game_dir = env.romroot / "F" / "Sys"
            game_files = [
                f for f in game_dir.iterdir()
                if f.is_file() and f.suffix != ".rscf"
            ]
            assert len(game_files) == 1
            assert game_files[0].name == "Game.zst"


class TestSidecarUpdatedOnProfileChange:
    """Sidecar must reflect new profile after transition."""

    def test_old_sidecar_replaced(self, env):
        """Old .rscf is replaced by new one with correct renderer."""
        rom = make_rom(env.source / "Game.gba", b"SIDECAR_REPLACE" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]

            pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="7z-16m",
            )
            old_rscf = env.romroot / "F" / "Sys" / "Game.7z.rscf"
            assert old_rscf.exists()

            pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="zstd-19",
            )

            # Old sidecar gone
            assert not old_rscf.exists()

            # New sidecar present with correct renderer
            new_rscf = env.romroot / "F" / "Sys" / "Game.zst.rscf"
            assert new_rscf.exists()
            sidecar = read_sidecar(new_rscf)
            assert sidecar.renderer == "zstd-19"

    def test_new_sidecar_container_hash_valid(self, env):
        """New sidecar's container_blake3 matches the new archive on disk."""
        rom = make_rom(env.source / "Game.gba", b"CONTAINER_CHECK" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

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

            archive = env.romroot / "F" / "Sys" / "Game.zst"
            sidecar = read_sidecar(archive.parent / "Game.zst.rscf")
            actual = hash_file(archive).blake3
            assert actual.upper() == sidecar.container_blake3.upper()


class TestMultipleProfileTransitions:
    """Content preserved through a chain of profile changes."""

    def test_three_transitions(self, env):
        """7z → zstd → none → 7z: ROM SHA1 identical through all transitions."""
        rom = make_rom(env.source / "Game.gba", b"CHAIN_TRANSITION" * 200)
        entry = rom_entry(rom)
        original_sha1 = entry["sha1"]

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        profiles = ["7z-16m", "zstd-19", "none", "7z-16m"]

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]

            for profile in profiles:
                pipeline_run(
                    sources, env.selection, env.romroot, env.work, db,
                    compression=profile,
                )

                # Find the ROM's SHA1 in whatever format it is now
                rscf_files = list(env.romroot.rglob("*.rscf"))
                # Filter out _orphaned
                rscf_files = [
                    f for f in rscf_files
                    if "_orphaned" not in str(f)
                ]
                assert len(rscf_files) >= 1, f"No sidecar after {profile}"
                sidecar = read_sidecar(rscf_files[0])
                assert sidecar.files[0].sha1.upper() == original_sha1.upper(), (
                    f"SHA1 mismatch after transition to {profile}"
                )
                assert sidecar.renderer == profile


class TestZstdExtensionTransition:
    """Zstd single↔multi transition triggers rebuild even without profile change."""

    def test_single_to_multi_extension_changes(self, env):
        """1 ROM (.zst) → 2 ROMs (.tar.zst): extension changes, content preserved."""
        rom1 = make_rom(env.source / "Track 1.bin", b"SINGLE_ROM" * 200)
        e1 = rom_entry(rom1)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [e1]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]

            pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="zstd-19",
            )
            single = env.romroot / "F" / "Sys" / "Game.zst"
            assert single.exists()

            # Add second ROM
            rom2 = make_rom(env.source / "Track 2.bin", b"SECOND_ROM" * 300)
            e2 = rom_entry(rom2)

            make_dat(env.selection / "F" / "Sys.dat", "Sys", [
                {"name": "Game", "roms": [e1, e2]},
            ])

            pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="zstd-19",
            )

            # .zst gone, .tar.zst created
            assert not single.exists()
            multi = env.romroot / "F" / "Sys" / "Game.tar.zst"
            assert multi.exists()

            sidecar = read_sidecar(multi.parent / "Game.tar.zst.rscf")
            assert len(sidecar.files) == 2
            assert sidecar.renderer == "zstd-19"
            hashes = {Path(f.path).name: f.sha1.upper() for f in sidecar.files}
            assert hashes["Track 1.bin"] == e1["sha1"].upper()
            assert hashes["Track 2.bin"] == e2["sha1"].upper()


class TestIdempotentAfterTransition:
    """After a profile change, subsequent runs with same profile skip."""

    def test_second_run_skips(self, env):
        """After 7z→zstd, another run with zstd skips the game."""
        rom = make_rom(env.source / "Game.gba", b"IDEMPOTENT_PROF" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

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

            archive = env.romroot / "F" / "Sys" / "Game.zst"
            archive_blake3 = hash_file(archive).blake3

            # Third run: same profile, should skip
            result = pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="zstd-19",
            )
            assert result["skipped"] == 1
            assert result["processed"] == 0
            assert hash_file(archive).blake3 == archive_blake3
