"""Collection verification safety — FILE_SAFETY.md § Collection + In-flight corruption.

The collection pipeline hashes compressed output before copying to romroot,
then re-reads and re-verifies from the target filesystem after copy. This
catches CIFS/network corruption.

Guarantees tested:
- Collected files have correct BLAKE3 recorded in RSCF sidecar
- RSCF sidecar content hashes match the original ROM data
- Verification failure deletes the corrupt target (in-flight only)
- Source data is safe throughout the pipeline (work dir has copies)
- Work dir is cleaned after each game
- Roundtrip verify (extract after compress) works for all archive types
"""

from __future__ import annotations

import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

from rscf import hash_file, read_sidecar

from romtholos.collect.compress import profile_extension
from romtholos.collect.config import SourceDir
from romtholos.collect.db import CacheDB

from .conftest import make_dat, make_rom, pipeline_run, rom_entry

# External tool availability
_has_dimg_tool = shutil.which("dimg-tool") is not None
_has_dolphin_tool = shutil.which("dolphin-tool") is not None
_has_trrntzip = shutil.which("trrntzip") is not None

_FIXTURES = Path(__file__).parent.parent / "fixtures"


class TestRoundtripVerification:
    """Collected files must pass BLAKE3 roundtrip verification."""

    def test_archive_blake3_matches_sidecar(self, env):
        """Archive on disk matches BLAKE3 recorded in its RSCF sidecar."""
        rom = make_rom(env.source / "Game.gba", b"VERIFY_BLAKE3" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            archive = env.romroot / "F" / "Sys" / "Game.7z"
            assert archive.exists()

            sidecar = read_sidecar(archive.parent / "Game.7z.rscf")
            actual_blake3 = hash_file(archive).blake3
            assert actual_blake3.upper() == sidecar.container_blake3.upper()

    def test_sidecar_rom_hashes_match_original(self, env):
        """RSCF sidecar records the correct ROM content hashes."""
        content = b"ORIGINAL_CONTENT" * 200
        rom = make_rom(env.source / "Game.gba", content)
        original_hashes = hash_file(rom)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            sidecar = read_sidecar(env.romroot / "F" / "Sys" / "Game.7z.rscf")
            assert len(sidecar.files) == 1
            f = sidecar.files[0]
            assert f.sha1.upper() == original_hashes.sha1.upper()
            assert f.md5.upper() == original_hashes.md5.upper()
            assert f.crc32.upper() == original_hashes.crc32.upper()

    def test_directory_mode_blake3_matches_sidecar(self, env):
        """Directory mode (none profile) also verifies and writes correct sidecar."""
        rom = make_rom(env.source / "Game.gba", b"DIR_MODE_VERIFY" * 200)
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

            rom_file = env.romroot / "F" / "Sys" / "Game" / "Game.gba"
            assert rom_file.exists()

            sidecar = read_sidecar(rom_file.parent / "Game.gba.rscf")
            actual_blake3 = hash_file(rom_file).blake3
            assert actual_blake3.upper() == sidecar.container_blake3.upper()

    def test_multi_rom_game_all_hashes_correct(self, env):
        """Multi-ROM game: every ROM's hash is recorded correctly in the sidecar."""
        roms = {}
        rom_entries = []
        for i in range(3):
            name = f"Track {i}.bin"
            content = f"TRACK_{i}_DATA".encode() * 200
            r = make_rom(env.source / name, content)
            roms[name] = hash_file(r)
            rom_entries.append(rom_entry(r))

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": rom_entries},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            sidecar = read_sidecar(env.romroot / "F" / "Sys" / "Game.7z.rscf")
            assert len(sidecar.files) == 3

            sidecar_by_name = {
                Path(f.path).name: f for f in sidecar.files
            }
            for name, original_h in roms.items():
                assert name in sidecar_by_name, f"ROM {name} missing from sidecar"
                assert sidecar_by_name[name].sha1.upper() == original_h.sha1.upper()


class TestRoundtripVerifyFlag:
    """--verify-roundtrip extracts compressed archive and checks ROM BLAKE3."""

    # All cartridge profiles — torrentzip requires trrntzip binary
    _CARTRIDGE_PROFILES = [
        "7z-16m", "7z-96m", "zstd-3", "zstd-12", "zstd-19", "zip",
    ] + (["torrentzip"] if _has_trrntzip else [])

    @pytest.mark.parametrize("profile", _CARTRIDGE_PROFILES)
    def test_roundtrip_verify_single_rom(self, env, profile):
        """Roundtrip verify succeeds for single-ROM game across all profiles."""
        rom = make_rom(env.source / "Game.gba", b"RT_SINGLE" * 200)
        entry = rom_entry(rom)
        ext = profile_extension(profile, 1)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            result = pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression=profile,
                verify_roundtrip=True,
            )

            assert result["processed"] == 1
            assert result["failed"] == 0
            assert (env.romroot / "F" / "Sys" / f"Game{ext}").exists()

    @pytest.mark.parametrize("profile", _CARTRIDGE_PROFILES)
    def test_roundtrip_verify_multi_rom(self, env, profile):
        """Roundtrip verify checks all ROMs in a multi-ROM archive."""
        rom_entries = []
        for i in range(3):
            rom = make_rom(
                env.source / f"Track {i}.bin",
                f"TRACK_{i}".encode() * 200,
            )
            rom_entries.append(rom_entry(rom))

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": rom_entries},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            result = pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression=profile,
                verify_roundtrip=True,
            )

            assert result["processed"] == 3
            assert result["failed"] == 0

    @pytest.mark.parametrize("profile", _CARTRIDGE_PROFILES)
    def test_roundtrip_verify_detects_corruption(self, env, profile):
        """Roundtrip verify catches real corrupt content for each profile.

        Injects wrong data into the ROM file after hashing but before
        compression — the compressed archive genuinely contains wrong
        content, so the roundtrip extract+hash detects the mismatch.
        """
        rom = make_rom(env.source / "Game.gba", b"CORRUPT_TEST" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        from romtholos.collect import execute as exe_mod
        from romtholos.collect.compress import compress as real_compress

        def corrupt_compress(prof, inputs, base, **kwargs):
            inputs[0].write_bytes(b"WRONG_DATA" * 200)
            return real_compress(prof, inputs, base, **kwargs)

        ext = profile_extension(profile, 1)

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]

            with patch.object(
                exe_mod, "compress", side_effect=corrupt_compress,
            ):
                result = pipeline_run(
                    sources, env.selection, env.romroot, env.work, db,
                    compression=profile,
                    verify_roundtrip=True,
                )

            assert result["failed"] >= 1
            assert not (env.romroot / "F" / "Sys" / f"Game{ext}").exists()

    # --- Disc image profiles (require external tools) ---

    @pytest.mark.skipif(not _has_dolphin_tool, reason="dolphin-tool not available")
    def test_roundtrip_verify_rvz(self, env):
        """Roundtrip verify for RVZ (GameCube) using real fixture."""
        iso = _FIXTURES / "test_gc.iso"
        assert iso.exists(), "Missing test fixture test_gc.iso"

        iso_hashes = hash_file(iso)
        # Copy ISO to source as a plain file (will be compressed to RVZ)
        src = env.source / "Test Game (USA).iso"
        shutil.copy2(iso, src)

        entry = {
            "name": "Test Game (USA).iso",
            "size": iso.stat().st_size,
            "sha1": iso_hashes.sha1,
            "crc32": iso_hashes.crc32,
            "md5": iso_hashes.md5,
        }

        make_dat(env.selection / "F" / "Nintendo - GameCube.dat",
                 "Nintendo - GameCube", [
            {"name": "Test Game (USA)", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            result = pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="rvz-zstd-5",
                verify_roundtrip=True,
            )

            assert result["failed"] == 0
            assert result["processed"] == 1

    @pytest.mark.skipif(not _has_dimg_tool, reason="dimg-tool not available")
    def test_roundtrip_verify_aaru_ps2dvd(self, env):
        """Roundtrip verify for aaru (PS2 DVD) using real fixture."""
        iso = _FIXTURES / "test_ps2dvd.iso"
        assert iso.exists(), "Missing test fixture test_ps2dvd.iso"

        iso_hashes = hash_file(iso)
        src = env.source / "Test Game (USA).iso"
        shutil.copy2(iso, src)

        entry = {
            "name": "Test Game (USA).iso",
            "size": iso.stat().st_size,
            "sha1": iso_hashes.sha1,
            "crc32": iso_hashes.crc32,
            "md5": iso_hashes.md5,
        }

        make_dat(env.selection / "F" / "Sony - PlayStation 2.dat",
                 "Sony - PlayStation 2", [
            {"name": "Test Game (USA)", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            result = pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="aaru-ps2dvd-zstd",
                verify_roundtrip=True,
            )

            assert result["failed"] == 0
            assert result["processed"] == 1

    @pytest.mark.skipif(not _has_dimg_tool, reason="dimg-tool not available")
    def test_roundtrip_verify_aaru_ps1_multitrack(self, env):
        """Roundtrip verify for aaru PS1 with multi-track fixture (data+audio).

        Uses the multi-track BIN fixture but writes a CUE with DAT-matching
        filenames, just like the real pipeline does (rewrite_cue_for_directory).
        """
        bin_file = _FIXTURES / "test_ps1_multitrack.bin"
        assert bin_file.exists(), "Missing PS1 multi-track BIN fixture"

        # Copy BIN with DAT name
        shutil.copy2(bin_file, env.source / "Test Game (USA).bin")

        # Write CUE referencing the DAT-named BIN (as Redump DATs specify)
        cue_path = env.source / "Test Game (USA).cue"
        cue_path.write_text(
            'FILE "Test Game (USA).bin" BINARY\n'
            '  TRACK 01 MODE2/2352\n'
            '    INDEX 01 00:00:00\n'
            '  TRACK 02 AUDIO\n'
            '    INDEX 01 00:02:50\n'
        )

        cue_hashes = hash_file(cue_path)
        bin_hashes = hash_file(env.source / "Test Game (USA).bin")

        cue_entry = {
            "name": "Test Game (USA).cue",
            "size": cue_path.stat().st_size,
            "sha1": cue_hashes.sha1,
            "crc32": cue_hashes.crc32,
            "md5": cue_hashes.md5,
        }
        bin_entry = {
            "name": "Test Game (USA).bin",
            "size": bin_file.stat().st_size,
            "sha1": bin_hashes.sha1,
            "crc32": bin_hashes.crc32,
            "md5": bin_hashes.md5,
        }

        make_dat(env.selection / "F" / "Sony - PlayStation.dat",
                 "Sony - PlayStation", [
            {"name": "Test Game (USA)", "roms": [cue_entry, bin_entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            result = pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="aaru-ps1-zstd",
                verify_roundtrip=True,
            )

            assert result["failed"] == 0
            assert result["processed"] >= 1

    # --- General safety ---

    def test_roundtrip_verify_source_preserved_on_failure(self, env):
        """Source ROM is preserved when roundtrip verify fails."""
        rom = make_rom(env.source / "Game.gba", b"SRC_SAFE" * 200)
        original_blake3 = hash_file(rom).blake3
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        from romtholos.collect import execute as exe_mod

        def always_fail(archive_path, expected, limits):
            return False, [e[0] for e in expected]

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]

            with patch.object(
                exe_mod, "_verify_roundtrip", side_effect=always_fail,
            ):
                pipeline_run(
                    sources, env.selection, env.romroot, env.work, db,
                    verify_roundtrip=True,
                )

            # Source must be untouched
            assert rom.exists()
            assert hash_file(rom).blake3 == original_blake3


class TestInFlightCorruption:
    """In-flight corruption: corrupt target detected and deleted safely."""

    def test_corrupt_target_deleted_source_safe(self, env):
        """If target verification fails, corrupt file is removed and source is safe."""
        rom = make_rom(env.source / "Game.gba", b"CORRUPT_TEST" * 200)
        original_hash = hash_file(rom).blake3
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        # Patch _verify_on_target to simulate corruption on the first call
        from romtholos.collect import execute as exe_mod
        original_verify = exe_mod._verify_on_target
        call_count = 0

        def fake_verify(target_path, expected_blake3):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return False  # simulate corruption
            return original_verify(target_path, expected_blake3)

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]

            with patch.object(exe_mod, "_verify_on_target", side_effect=fake_verify):
                result = pipeline_run(
                    sources, env.selection, env.romroot, env.work, db,
                )

            # The game should have failed
            assert result["failed"] >= 1

            # Source file is untouched
            assert rom.exists()
            assert hash_file(rom).blake3 == original_hash

    def test_failed_verification_leaves_no_corrupt_file(self, env):
        """After verification failure, romroot has no file for that game."""
        rom = make_rom(env.source / "Game.gba", b"NO_CORRUPT" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        from romtholos.collect import execute as exe_mod

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]

            with patch.object(
                exe_mod, "_verify_on_target", return_value=False,
            ):
                pipeline_run(
                    sources, env.selection, env.romroot, env.work, db,
                )

            # No archive or directory for this game in romroot
            game_dir = env.romroot / "F" / "Sys"
            if game_dir.exists():
                game_files = [
                    f for f in game_dir.rglob("*")
                    if f.is_file() and f.suffix != ".rscf"
                ]
                assert game_files == [], (
                    f"Corrupt files left in romroot: {game_files}"
                )

    def test_one_game_fails_others_succeed(self, env):
        """Verification failure for one game does not affect other games."""
        rom_good = make_rom(env.source / "Good.gba", b"GOOD_ROM" * 200)
        rom_bad = make_rom(env.source / "Bad.gba", b"BAD_ROM" * 200)
        entry_good = rom_entry(rom_good)
        entry_bad = rom_entry(rom_bad)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Good", "roms": [entry_good]},
            {"name": "Bad", "roms": [entry_bad]},
        ])

        from romtholos.collect import execute as exe_mod
        original_verify = exe_mod._verify_on_target

        def selective_fail(target_path, expected_blake3):
            if "Bad" in str(target_path):
                return False
            return original_verify(target_path, expected_blake3)

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]

            with patch.object(
                exe_mod, "_verify_on_target", side_effect=selective_fail,
            ):
                result = pipeline_run(
                    sources, env.selection, env.romroot, env.work, db,
                )

            assert result["failed"] >= 1
            assert result["processed"] >= 1

            # Good game collected successfully
            good_archive = env.romroot / "F" / "Sys" / "Good.7z"
            assert good_archive.exists()

            # Bad game has no file
            bad_archive = env.romroot / "F" / "Sys" / "Bad.7z"
            assert not bad_archive.exists()


class TestSidecarCompleteness:
    """Every collected file must have a valid RSCF sidecar."""

    def test_every_romroot_file_has_sidecar(self, env):
        """All files in romroot have a corresponding .rscf sidecar."""
        for i in range(4):
            rom = make_rom(
                env.source / f"Game{i}.gba",
                f"ROM_{i}_CONTENT".encode() * 100,
            )
            entry = rom_entry(rom)
            make_dat(env.selection / "F" / "Sys.dat", "Sys", [
                {"name": f"Game{i}", "roms": [entry]},
            ]) if i == 0 else None

        # Create all games in one DAT
        games = []
        for i in range(4):
            rom = env.source / f"Game{i}.gba"
            games.append({"name": f"Game{i}", "roms": [rom_entry(rom)]})

        make_dat(env.selection / "F" / "Sys.dat", "Sys", games)

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # Find all non-sidecar files in romroot
            romroot_files = [
                f for f in env.romroot.rglob("*")
                if f.is_file() and f.suffix != ".rscf"
            ]
            assert len(romroot_files) == 4

            for f in romroot_files:
                sidecar_path = f.parent / (f.name + ".rscf")
                assert sidecar_path.exists(), (
                    f"Missing sidecar for {f.relative_to(env.romroot)}"
                )

    def test_sidecar_renderer_matches_profile(self, env):
        """RSCF sidecar records the compression profile used."""
        rom = make_rom(env.source / "Game.gba", b"RENDERER_CHECK" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        for profile in ("7z-16m", "zstd-19", "none"):
            with CacheDB(env.db_path) as db:
                sources = [SourceDir(path=env.source, source_type="readonly")]
                pipeline_run(
                    sources, env.selection, env.romroot, env.work, db,
                    compression=profile,
                )

                # Find the sidecar
                rscf_files = list(env.romroot.rglob("*.rscf"))
                assert len(rscf_files) >= 1
                sidecar = read_sidecar(rscf_files[0])
                assert sidecar.renderer == profile

            # Clean romroot and DB for next profile
            import shutil
            if env.romroot.exists():
                shutil.rmtree(env.romroot)
                env.romroot.mkdir()
            env.db_path.unlink(missing_ok=True)


class TestWorkDirCleanup:
    """Work directory must be cleaned after each game — no leaked files."""

    def test_work_dir_clean_after_successful_run(self, env):
        """Work directory has no ROM files after pipeline completes."""
        games = []
        for i in range(3):
            rom = make_rom(
                env.source / f"Game{i}.gba",
                f"WORKDIR_{i}".encode() * 100,
            )
            games.append({"name": f"Game{i}", "roms": [rom_entry(rom)]})

        make_dat(env.selection / "F" / "Sys.dat", "Sys", games)

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            if env.work.exists():
                leftover = [
                    f for f in env.work.rglob("*")
                    if f.is_file()
                ]
                assert leftover == [], f"Work dir has leftover files: {leftover}"

    def test_work_dir_clean_after_failed_verification(self, env):
        """Work directory is cleaned even when verification fails."""
        rom = make_rom(env.source / "Game.gba", b"WORKDIR_FAIL" * 200)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        from romtholos.collect import execute as exe_mod

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]

            with patch.object(
                exe_mod, "_verify_on_target", return_value=False,
            ):
                pipeline_run(
                    sources, env.selection, env.romroot, env.work, db,
                )

            if env.work.exists():
                leftover = [f for f in env.work.rglob("*") if f.is_file()]
                assert leftover == [], (
                    f"Work dir has leftover files after failure: {leftover}"
                )
