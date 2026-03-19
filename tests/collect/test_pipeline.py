"""Integration test for the full collector pipeline: scan → match → execute."""

from __future__ import annotations

import os
import subprocess
import zipfile
from pathlib import Path

import pytest

from rscf import hash_file, read_sidecar

from romtholos.collect.config import SourceDir
from romtholos.collect.db import CacheDB
from romtholos.collect.execute import execute_plan
from romtholos.collect.match import GamePlan, group_by_game, match_dat
from romtholos.collect.scan import scan_all


def _make_rom(path: Path, content: bytes = b"\x00" * 1024) -> Path:
    """Create a fake ROM file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def _make_dat(path: Path, system: str, games: list[dict]) -> Path:
    """Create a minimal DAT with pre-computed hashes."""
    path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        '<?xml version="1.0"?>',
        '<datafile>',
        '\t<header>',
        f'\t\t<name>{system}</name>',
        '\t</header>',
    ]

    for game in games:
        lines.append(f'\t<game name="{game["name"]}">')
        lines.append('\t\t<category>Games</category>')
        for rom in game["roms"]:
            attrs = f'name="{rom["name"]}" size="{rom["size"]}"'
            if rom.get("crc32"):
                attrs += f' crc="{rom["crc32"].lower()}"'
            if rom.get("md5"):
                attrs += f' md5="{rom["md5"].lower()}"'
            if rom.get("sha1"):
                attrs += f' sha1="{rom["sha1"].lower()}"'
            lines.append(f'\t\t<rom {attrs}/>')
        lines.append('\t</game>')

    lines.append('</datafile>')
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def _full_pipeline_run(
    sources: list[SourceDir],
    dat_path: Path,
    romroot: Path,
    work: Path,
    db: CacheDB,
    compression: str = "7z-16m",
) -> dict[str, int]:
    """Run scan → match → execute, mirroring real `collector run`.

    Scans romroot first (implicit source), then explicit sources.
    """
    all_sources = [SourceDir(path=romroot, source_type="romroot")] + sources
    scan_all(all_sources, db, work)
    ops = match_dat(dat_path, db)
    game_plans = group_by_game(ops)
    return execute_plan(game_plans, romroot, work, compression, db)


class TestFullPipeline:
    def test_scan_match_execute_single_rom(self, tmp_path: Path):
        """End-to-end: single-ROM game with none profile."""
        ingest = tmp_path / "ingest"
        selection = tmp_path / "selection" / "Test System"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom_content = b"FAKE_ROM_CONTENT_12345" * 100
        rom = _make_rom(ingest / "Cool Game (USA).gba", rom_content)
        hashes = hash_file(rom)

        _make_dat(
            selection / "Test System (1) (retooling).dat",
            "Test System",
            [{
                "name": "Cool Game (USA)",
                "roms": [{
                    "name": "Cool Game (USA).gba",
                    "size": rom.stat().st_size,
                    "crc32": hashes.crc32,
                    "md5": hashes.md5,
                    "sha1": hashes.sha1,
                }],
            }],
        )

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]
            dat = selection / "Test System (1) (retooling).dat"
            result = _full_pipeline_run(sources, dat, romroot, work, db, "none")

            assert result["processed"] == 1
            assert result["failed"] == 0

            # Verify romroot — "none" profile uses game directory
            game_dir = romroot / "Test System" / "Cool Game (USA)"
            rom_files = list(game_dir.rglob("*.gba"))
            assert len(rom_files) == 1

            rscf_files = list(game_dir.rglob("*.rscf"))
            assert len(rscf_files) == 1

            sidecar = read_sidecar(rscf_files[0])
            assert len(sidecar.files) == 1
            assert sidecar.files[0].sha1 == hashes.sha1

            assert db.stats()["romroot_files"] == 1

    def test_zip_source_fully_hashed(self, tmp_path: Path):
        """ROM inside a zip archive — contents fully hashed during scan."""
        ingest = tmp_path / "ingest"
        selection = tmp_path / "selection" / "Test System"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom_content = b"ZIP_ROM_CONTENT" * 200
        ingest.mkdir(parents=True)
        rom_path = ingest / "temp_rom.gba"
        rom_path.write_bytes(rom_content)
        rom_hashes = hash_file(rom_path)
        rom_size = rom_path.stat().st_size

        archive = ingest / "game.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.write(rom_path, "Cool Game (USA).gba")
        rom_path.unlink()

        _make_dat(
            selection / "Test System (1) (retooling).dat",
            "Test System",
            [{
                "name": "Cool Game (USA)",
                "roms": [{
                    "name": "Cool Game (USA).gba",
                    "size": rom_size,
                    "crc32": rom_hashes.crc32,
                    "sha1": rom_hashes.sha1,
                }],
            }],
        )

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]
            results = scan_all(sources, db, work)
            assert results[str(ingest)].archives_extracted == 1

            dat = selection / "Test System (1) (retooling).dat"
            ops = match_dat(dat, db)
            assert len(ops) == 1
            assert ops[0].status == "matched"
            assert ops[0].source_type == "archive_content"

    def test_already_in_romroot(self, tmp_path: Path):
        """Skip ROMs already in romroot."""
        ingest = tmp_path / "ingest"
        selection = tmp_path / "selection" / "Test System"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom_content = b"EXISTING_ROM" * 100
        rom = _make_rom(ingest / "Game.gba", rom_content)
        hashes = hash_file(rom)

        _make_dat(
            selection / "Test System (1) (retooling).dat",
            "Test System",
            [{
                "name": "Game",
                "roms": [{
                    "name": "Game.gba",
                    "size": rom.stat().st_size,
                    "crc32": hashes.crc32,
                    "sha1": hashes.sha1,
                }],
            }],
        )

        dat = selection / "Test System (1) (retooling).dat"
        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]

            # First run — collects
            r1 = _full_pipeline_run(sources, dat, romroot, work, db, "none")
            assert r1["processed"] == 1
            assert db.stats()["romroot_files"] == 1

            # Second run — should skip
            r2 = _full_pipeline_run(sources, dat, romroot, work, db, "none")
            assert r2["skipped"] == 1
            assert r2["processed"] == 0

    def test_multi_rom_7z(self, tmp_path: Path):
        """Multi-ROM game (bin+cue) collected into one 7z archive."""
        ingest = tmp_path / "ingest"
        selection = tmp_path / "selection" / "Test System"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        bin_content = b"DISC_DATA" * 500
        cue_content = b'FILE "track01.bin" BINARY\n  TRACK 01 MODE2/2352\n'

        bin_rom = _make_rom(ingest / "track01.bin", bin_content)
        cue_rom = _make_rom(ingest / "game.cue", cue_content)

        bin_hashes = hash_file(bin_rom)
        cue_hashes = hash_file(cue_rom)

        _make_dat(
            selection / "Test System (1).dat",
            "Test System",
            [{
                "name": "Cool Game (USA)",
                "roms": [
                    {
                        "name": "Cool Game (USA).bin",
                        "size": bin_rom.stat().st_size,
                        "sha1": bin_hashes.sha1,
                    },
                    {
                        "name": "Cool Game (USA).cue",
                        "size": cue_rom.stat().st_size,
                        "sha1": cue_hashes.sha1,
                    },
                ],
            }],
        )

        dat = selection / "Test System (1).dat"
        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]
            result = _full_pipeline_run(sources, dat, romroot, work, db)

            assert result["processed"] == 2

            # Verify: one .7z archive with both ROMs
            archives = list((romroot / "Test System").glob("*.7z"))
            assert len(archives) == 1
            assert archives[0].name == "Cool Game (USA).7z"

            # Verify RSCF sidecar lists both ROMs
            rscf_path = archives[0].parent / (archives[0].name + ".rscf")
            assert rscf_path.exists()
            sidecar = read_sidecar(rscf_path)
            assert len(sidecar.files) == 2

            rom_names = {f.path for f in sidecar.files}
            assert "Cool Game (USA).bin" in rom_names
            assert "Cool Game (USA).cue" in rom_names

            assert db.stats()["romroot_files"] == 2


class TestIncrementalAppend:
    """Incremental archive building via append (7z, zip)."""

    def _make_multi_rom_dat(self, dat_path, system, game_name, rom_specs):
        """Helper: create DAT with multiple ROMs per game.

        rom_specs: list of (name, content_bytes) — hashes computed automatically.
        Returns dict of name -> FileHashes for verification.
        """
        import tempfile
        hashes_map = {}
        roms = []
        for name, content in rom_specs:
            with tempfile.NamedTemporaryFile(delete=False) as f:
                f.write(content)
                tmp = Path(f.name)
            h = hash_file(tmp)
            hashes_map[name] = h
            roms.append({
                "name": name,
                "size": len(content),
                "sha1": h.sha1,
            })
            tmp.unlink()

        _make_dat(dat_path, system, [{"name": game_name, "roms": roms}])
        return hashes_map

    def test_7z_append_cue_then_bin(self, tmp_path: Path):
        """7z append: collect cue first, then bin in second run."""
        ingest = tmp_path / "ingest"
        dat_path = tmp_path / "sel" / "Sys" / "Sys.dat"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        bin_content = b"BIN_TRACK_DATA" * 500
        cue_content = b'FILE "game.bin" BINARY\n  TRACK 01 MODE2/2352\n'

        hashes = self._make_multi_rom_dat(
            dat_path, "Sys", "Game (USA)",
            [("Game (USA).bin", bin_content), ("Game (USA).cue", cue_content)],
        )

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]

            # Run 1: only cue available
            _make_rom(ingest / "some_cue.cue", cue_content)
            r1 = _full_pipeline_run(sources, dat_path, romroot, work, db)
            assert r1["processed"] == 1

            # Partial archive with 1 ROM
            archive = romroot / "Sys" / "Game (USA).7z"
            assert archive.exists()
            s1 = read_sidecar(archive.parent / (archive.name + ".rscf"))
            assert len(s1.files) == 1
            assert s1.files[0].path == "Game (USA).cue"

            # Run 2: bin arrives
            _make_rom(ingest / "track_data.bin", bin_content)
            r2 = _full_pipeline_run(sources, dat_path, romroot, work, db)
            assert r2["processed"] == 1  # only the new ROM
            assert r2["skipped"] == 0

            # Archive now has both ROMs
            s2 = read_sidecar(archive.parent / (archive.name + ".rscf"))
            assert len(s2.files) == 2
            names = {f.path for f in s2.files}
            assert names == {"Game (USA).bin", "Game (USA).cue"}

            # DB has 2 entries for same archive
            rows = db.get_romroot_game("Sys", "Game (USA)")
            assert len(rows) == 2

            # Run 3: nothing new — should be complete
            r3 = _full_pipeline_run(sources, dat_path, romroot, work, db)
            assert r3["skipped"] == 2
            assert r3["processed"] == 0

    def test_zip_append(self, tmp_path: Path):
        """zip append: incremental collection works with zip profile."""
        ingest = tmp_path / "ingest"
        dat_path = tmp_path / "sel" / "Sys" / "Sys.dat"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom_a = b"ROM_A_DATA" * 100
        rom_b = b"ROM_B_DATA" * 200

        self._make_multi_rom_dat(
            dat_path, "Sys", "Game",
            [("Game.bin", rom_a), ("Game.cue", rom_b)],
        )

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]

            # Run 1: first ROM
            _make_rom(ingest / "a.bin", rom_a)
            r1 = _full_pipeline_run(sources, dat_path, romroot, work, db, "zip")
            assert r1["processed"] == 1

            archive = romroot / "Sys" / "Game.zip"
            assert archive.exists()

            # Run 2: second ROM
            _make_rom(ingest / "b.cue", rom_b)
            r2 = _full_pipeline_run(sources, dat_path, romroot, work, db, "zip")
            assert r2["processed"] == 1

            # Verify zip contains both
            with zipfile.ZipFile(archive, "r") as zf:
                assert len(zf.namelist()) == 2

            s = read_sidecar(archive.parent / (archive.name + ".rscf"))
            assert len(s.files) == 2

    def test_three_step_incremental(self, tmp_path: Path):
        """Three ROMs added one at a time across three runs."""
        ingest = tmp_path / "ingest"
        dat_path = tmp_path / "sel" / "Sys" / "Sys.dat"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        contents = {
            "Game.track1.bin": b"TRACK1" * 300,
            "Game.track2.bin": b"TRACK2" * 400,
            "Game.cue": b"CUE_FILE_DATA" * 10,
        }

        self._make_multi_rom_dat(
            dat_path, "Sys", "Game",
            [(name, data) for name, data in contents.items()],
        )

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]

            # Run 1: track1 only
            _make_rom(ingest / "t1.bin", contents["Game.track1.bin"])
            r1 = _full_pipeline_run(sources, dat_path, romroot, work, db)
            assert r1["processed"] == 1

            s1 = read_sidecar(
                romroot / "Sys" / "Game.7z.rscf"
            )
            assert len(s1.files) == 1

            # Run 2: cue added
            _make_rom(ingest / "c.cue", contents["Game.cue"])
            r2 = _full_pipeline_run(sources, dat_path, romroot, work, db)
            assert r2["processed"] == 1

            s2 = read_sidecar(romroot / "Sys" / "Game.7z.rscf")
            assert len(s2.files) == 2

            # Run 3: track2 completes the game
            _make_rom(ingest / "t2.bin", contents["Game.track2.bin"])
            r3 = _full_pipeline_run(sources, dat_path, romroot, work, db)
            assert r3["processed"] == 1

            s3 = read_sidecar(romroot / "Sys" / "Game.7z.rscf")
            assert len(s3.files) == 3

            # Run 4: everything complete — skip
            r4 = _full_pipeline_run(sources, dat_path, romroot, work, db)
            assert r4["skipped"] == 3
            assert r4["processed"] == 0


class TestIncrementalRebuild:
    """Incremental archive building via rebuild (zstd tar.zst)."""

    def test_zstd_rebuild_partial_then_complete(self, tmp_path: Path):
        """zstd: single ROM as .zst, then rebuild to .tar.zst when 2nd arrives."""
        ingest = tmp_path / "ingest"
        dat_path = tmp_path / "sel" / "Sys" / "Sys.dat"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom_a = b"ROM_A_FOR_ZSTD" * 200
        rom_b = b"ROM_B_FOR_ZSTD" * 300

        # Pre-compute hashes
        _make_rom(tmp_path / "_ha", rom_a)
        _make_rom(tmp_path / "_hb", rom_b)
        ha = hash_file(tmp_path / "_ha")
        hb = hash_file(tmp_path / "_hb")

        _make_dat(
            dat_path, "Sys", [{
                "name": "Game",
                "roms": [
                    {"name": "Game.bin", "size": len(rom_a), "sha1": ha.sha1},
                    {"name": "Game.cue", "size": len(rom_b), "sha1": hb.sha1},
                ],
            }],
        )

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]

            # Run 1: only rom_a → single file = .zst
            _make_rom(ingest / "a.bin", rom_a)
            r1 = _full_pipeline_run(
                sources, dat_path, romroot, work, db, "zstd-19",
            )
            assert r1["processed"] == 1

            # Single file: should be .zst (not .tar.zst)
            zst_files = list((romroot / "Sys").glob("*.zst"))
            # Filter out .tar.zst
            zst_only = [f for f in zst_files if not f.name.endswith(".tar.zst")]
            assert len(zst_only) == 1
            assert zst_only[0].name == "Game.zst"
            s1 = read_sidecar(zst_only[0].parent / (zst_only[0].name + ".rscf"))
            assert len(s1.files) == 1

            # Run 2: rom_b arrives → rebuild as .tar.zst, old .zst removed
            _make_rom(ingest / "b.cue", rom_b)
            r2 = _full_pipeline_run(
                sources, dat_path, romroot, work, db, "zstd-19",
            )
            assert r2["processed"] == 1

            # Old .zst should be gone
            assert not zst_only[0].exists()

            # New .tar.zst should exist
            tar_zst = list((romroot / "Sys").glob("*.tar.zst"))
            assert len(tar_zst) == 1
            assert tar_zst[0].name == "Game.tar.zst"

            s2 = read_sidecar(tar_zst[0].parent / (tar_zst[0].name + ".rscf"))
            assert len(s2.files) == 2
            names = {f.path for f in s2.files}
            assert names == {"Game.bin", "Game.cue"}

            # Verify tar.zst actually contains both files
            result = subprocess.run(
                ["tar", "--zstd", "-tf", str(tar_zst[0])],
                capture_output=True, text=True,
            )
            tar_contents = set(result.stdout.strip().splitlines())
            assert "Game.bin" in tar_contents
            assert "Game.cue" in tar_contents

            # Run 3: complete → skip
            r3 = _full_pipeline_run(
                sources, dat_path, romroot, work, db, "zstd-19",
            )
            assert r3["skipped"] == 2
            assert r3["processed"] == 0


class TestIncrementalDirectory:
    """Incremental collection with "none" profile (directory mode)."""

    def test_none_profile_multi_rom_incremental(self, tmp_path: Path):
        """None profile: files added individually to game directory."""
        ingest = tmp_path / "ingest"
        dat_path = tmp_path / "sel" / "Sys" / "Sys.dat"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom_a = b"FILE_A_CONTENT" * 100
        rom_b = b"FILE_B_CONTENT" * 200

        _make_rom(tmp_path / "_ha", rom_a)
        _make_rom(tmp_path / "_hb", rom_b)
        ha = hash_file(tmp_path / "_ha")
        hb = hash_file(tmp_path / "_hb")

        _make_dat(
            dat_path, "Sys", [{
                "name": "Game",
                "roms": [
                    {"name": "Game.bin", "size": len(rom_a), "sha1": ha.sha1},
                    {"name": "Game.cue", "size": len(rom_b), "sha1": hb.sha1},
                ],
            }],
        )

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]

            # Run 1: only bin
            _make_rom(ingest / "a.bin", rom_a)
            r1 = _full_pipeline_run(
                sources, dat_path, romroot, work, db, "none",
            )
            assert r1["processed"] == 1

            game_dir = romroot / "Sys" / "Game"
            assert game_dir.is_dir()
            assert (game_dir / "Game.bin").exists()
            assert not (game_dir / "Game.cue").exists()

            # Per-file sidecar
            assert (game_dir / "Game.bin.rscf").exists()

            # Run 2: cue arrives
            _make_rom(ingest / "b.cue", rom_b)
            r2 = _full_pipeline_run(
                sources, dat_path, romroot, work, db, "none",
            )
            assert r2["processed"] == 1

            assert (game_dir / "Game.cue").exists()
            assert (game_dir / "Game.cue.rscf").exists()

            # Both files now in romroot
            assert db.stats()["romroot_files"] == 2

            # Run 3: complete — skip
            r3 = _full_pipeline_run(
                sources, dat_path, romroot, work, db, "none",
            )
            assert r3["skipped"] == 2
            assert r3["processed"] == 0


class TestGamePlanClassification:
    """Test GamePlan status classification logic."""

    def test_complete(self, tmp_path: Path):
        """All ROMs in romroot → complete."""
        ingest = tmp_path / "ingest"
        dat_path = tmp_path / "sel" / "Sys" / "Sys.dat"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom = _make_rom(ingest / "game.gba", b"ROM" * 100)
        h = hash_file(rom)

        _make_dat(dat_path, "Sys", [{
            "name": "Game",
            "roms": [{"name": "Game.gba", "size": len(b"ROM" * 100), "sha1": h.sha1}],
        }])

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]
            _full_pipeline_run(sources, dat_path, romroot, work, db, "none")

            # Re-match — should be complete
            ops = match_dat(dat_path, db)
            plans = group_by_game(ops)
            assert len(plans) == 1
            assert plans[0].status == "complete"

    def test_collectable(self, tmp_path: Path):
        """All ROMs matched but not yet in romroot → collectable."""
        ingest = tmp_path / "ingest"
        dat_path = tmp_path / "sel" / "Sys" / "Sys.dat"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom_a = _make_rom(ingest / "a.bin", b"AA" * 100)
        rom_b = _make_rom(ingest / "b.cue", b"BB" * 50)

        _make_dat(dat_path, "Sys", [{
            "name": "Game",
            "roms": [
                {"name": "Game.bin", "size": rom_a.stat().st_size,
                 "sha1": hash_file(rom_a).sha1},
                {"name": "Game.cue", "size": rom_b.stat().st_size,
                 "sha1": hash_file(rom_b).sha1},
            ],
        }])

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]
            all_sources = [SourceDir(path=romroot, source_type="romroot")] + sources
            scan_all(all_sources, db, work)

            ops = match_dat(dat_path, db)
            plans = group_by_game(ops)
            assert len(plans) == 1
            assert plans[0].status == "collectable"
            assert len(plans[0].new_ops) == 2

    def test_partial(self, tmp_path: Path):
        """Some ROMs matched, some missing → partial."""
        ingest = tmp_path / "ingest"
        dat_path = tmp_path / "sel" / "Sys" / "Sys.dat"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom_a = _make_rom(ingest / "a.bin", b"FOUND" * 100)

        _make_rom(tmp_path / "_hb", b"NOTFOUND" * 100)
        hb = hash_file(tmp_path / "_hb")

        _make_dat(dat_path, "Sys", [{
            "name": "Game",
            "roms": [
                {"name": "Game.bin", "size": rom_a.stat().st_size,
                 "sha1": hash_file(rom_a).sha1},
                {"name": "Game.cue", "size": len(b"NOTFOUND" * 100),
                 "sha1": hb.sha1},
            ],
        }])

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]
            all_sources = [SourceDir(path=romroot, source_type="romroot")] + sources
            scan_all(all_sources, db, work)

            ops = match_dat(dat_path, db)
            plans = group_by_game(ops)
            assert len(plans) == 1
            assert plans[0].status == "partial"
            assert len(plans[0].new_ops) == 1
            assert len(plans[0].missing_ops) == 1

    def test_unavailable(self, tmp_path: Path):
        """No ROMs found → unavailable."""
        dat_path = tmp_path / "sel" / "Sys" / "Sys.dat"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        _make_rom(tmp_path / "_h", b"GHOST" * 100)
        h = hash_file(tmp_path / "_h")

        _make_dat(dat_path, "Sys", [{
            "name": "Game",
            "roms": [{"name": "Game.bin", "size": len(b"GHOST" * 100),
                       "sha1": h.sha1}],
        }])

        with CacheDB(db_path) as db:
            # No sources at all — nothing to find
            all_sources = [SourceDir(path=romroot, source_type="romroot")]
            scan_all(all_sources, db, work)

            ops = match_dat(dat_path, db)
            plans = group_by_game(ops)
            assert len(plans) == 1
            assert plans[0].status == "unavailable"

    def test_mixed_games(self, tmp_path: Path):
        """Multiple games with different statuses in same DAT."""
        ingest = tmp_path / "ingest"
        dat_path = tmp_path / "sel" / "Sys" / "Sys.dat"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        # Game A: available (single ROM)
        rom_a = _make_rom(ingest / "a.gba", b"GAME_A" * 100)
        ha = hash_file(rom_a)

        # Game B: completely missing
        _make_rom(tmp_path / "_hb", b"GAME_B" * 100)
        hb = hash_file(tmp_path / "_hb")

        _make_dat(dat_path, "Sys", [
            {
                "name": "Game A",
                "roms": [{"name": "A.gba", "size": rom_a.stat().st_size,
                           "sha1": ha.sha1}],
            },
            {
                "name": "Game B",
                "roms": [{"name": "B.gba", "size": len(b"GAME_B" * 100),
                           "sha1": hb.sha1}],
            },
        ])

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]
            all_sources = [SourceDir(path=romroot, source_type="romroot")] + sources
            scan_all(all_sources, db, work)

            ops = match_dat(dat_path, db)
            plans = group_by_game(ops)
            statuses = {g.game_name: g.status for g in plans}

            assert statuses["Game A"] == "collectable"
            assert statuses["Game B"] == "unavailable"


class TestProfileChanges:
    """Profile change detection and recompression."""

    def _setup_single_rom_game(self, tmp_path, profile):
        """Helper: create a single-ROM game with given profile, return paths."""
        ingest = tmp_path / "ingest"
        dat_path = tmp_path / "sel" / "Sys" / "Sys.dat"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom_content = b"PROFILE_CHANGE_ROM" * 200
        rom = _make_rom(ingest / "game.gba", rom_content)
        h = hash_file(rom)

        _make_dat(dat_path, "Sys", [{
            "name": "Game",
            "roms": [{
                "name": "Game.gba",
                "size": len(rom_content),
                "sha1": h.sha1,
            }],
        }])

        return ingest, dat_path, romroot, work, db_path

    def test_profile_change_7z_to_zstd(self, tmp_path: Path):
        """Game with .7z, config changes to zstd → .zst created, .7z removed."""
        ingest, dat_path, romroot, work, db_path = self._setup_single_rom_game(
            tmp_path, "7z-16m",
        )

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]

            # Run 1: collect with 7z
            r1 = _full_pipeline_run(sources, dat_path, romroot, work, db, "7z-16m")
            assert r1["processed"] == 1
            archives_7z = list((romroot / "Sys").glob("*.7z"))
            assert len(archives_7z) == 1

            # Run 2: switch to zstd → recompresses
            r2 = _full_pipeline_run(sources, dat_path, romroot, work, db, "zstd-19")
            assert r2["processed"] == 1

            # Old .7z should be gone
            assert not archives_7z[0].exists()
            assert not (archives_7z[0].parent / (archives_7z[0].name + ".rscf")).exists()

            # New .zst should exist
            zst_files = list((romroot / "Sys").glob("*.zst"))
            assert len(zst_files) == 1

            sidecar = read_sidecar(zst_files[0].parent / (zst_files[0].name + ".rscf"))
            assert sidecar.renderer == "zstd-19"

            # Run 3: complete with correct profile → skip
            r3 = _full_pipeline_run(sources, dat_path, romroot, work, db, "zstd-19")
            assert r3["skipped"] == 1
            assert r3["processed"] == 0

    def test_profile_change_zstd_to_7z(self, tmp_path: Path):
        """Game with .zst, config changes to 7z → .7z created, .zst removed."""
        ingest, dat_path, romroot, work, db_path = self._setup_single_rom_game(
            tmp_path, "zstd-19",
        )

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]

            # Run 1: collect with zstd
            r1 = _full_pipeline_run(sources, dat_path, romroot, work, db, "zstd-19")
            assert r1["processed"] == 1
            zst_files = list((romroot / "Sys").glob("*.zst"))
            assert len(zst_files) == 1

            # Run 2: switch to 7z → recompresses
            r2 = _full_pipeline_run(sources, dat_path, romroot, work, db, "7z-16m")
            assert r2["processed"] == 1

            # Old .zst should be gone
            assert not zst_files[0].exists()

            # New .7z should exist
            archives_7z = list((romroot / "Sys").glob("*.7z"))
            assert len(archives_7z) == 1

            sidecar = read_sidecar(archives_7z[0].parent / (archives_7z[0].name + ".rscf"))
            assert sidecar.renderer == "7z-16m"

    def test_complete_game_recompressed_on_profile_change(self, tmp_path: Path):
        """All ROMs present but wrong profile → recompressed, no new ROMs."""
        ingest = tmp_path / "ingest"
        dat_path = tmp_path / "sel" / "Sys" / "Sys.dat"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom_a = b"RECOMPRESS_A" * 200
        rom_b = b"RECOMPRESS_B" * 300

        _make_rom(tmp_path / "_ha", rom_a)
        _make_rom(tmp_path / "_hb", rom_b)
        ha = hash_file(tmp_path / "_ha")
        hb = hash_file(tmp_path / "_hb")

        _make_dat(dat_path, "Sys", [{
            "name": "Game",
            "roms": [
                {"name": "Game.bin", "size": len(rom_a), "sha1": ha.sha1},
                {"name": "Game.cue", "size": len(rom_b), "sha1": hb.sha1},
            ],
        }])

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]

            # Run 1: collect both ROMs with 7z
            _make_rom(ingest / "a.bin", rom_a)
            _make_rom(ingest / "b.cue", rom_b)
            r1 = _full_pipeline_run(sources, dat_path, romroot, work, db, "7z-16m")
            assert r1["processed"] == 2

            archives_7z = list((romroot / "Sys").glob("*.7z"))
            assert len(archives_7z) == 1

            # Run 2: same profile → skip (complete)
            r2 = _full_pipeline_run(sources, dat_path, romroot, work, db, "7z-16m")
            assert r2["skipped"] == 2
            assert r2["processed"] == 0

            # Run 3: change to zip → recompress (all ROMs, no new ops)
            r3 = _full_pipeline_run(sources, dat_path, romroot, work, db, "zip")
            assert r3["processed"] == 2

            # Old .7z should be gone
            assert not archives_7z[0].exists()

            # New .zip should exist
            archives_zip = list((romroot / "Sys").glob("*.zip"))
            assert len(archives_zip) == 1

            sidecar = read_sidecar(archives_zip[0].parent / (archives_zip[0].name + ".rscf"))
            assert sidecar.renderer == "zip"
            assert len(sidecar.files) == 2

            # Run 4: complete with correct profile → skip
            r4 = _full_pipeline_run(sources, dat_path, romroot, work, db, "zip")
            assert r4["skipped"] == 2

    def test_complete_correct_profile_skipped(self, tmp_path: Path):
        """All ROMs present, correct profile → skipped."""
        ingest, dat_path, romroot, work, db_path = self._setup_single_rom_game(
            tmp_path, "7z-16m",
        )

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]

            # Collect
            r1 = _full_pipeline_run(sources, dat_path, romroot, work, db, "7z-16m")
            assert r1["processed"] == 1

            # Second run — same profile → skip
            r2 = _full_pipeline_run(sources, dat_path, romroot, work, db, "7z-16m")
            assert r2["skipped"] == 1
            assert r2["processed"] == 0

    def test_none_to_archive(self, tmp_path: Path):
        """Game dir → compressed archive, dir removed."""
        ingest = tmp_path / "ingest"
        dat_path = tmp_path / "sel" / "Sys" / "Sys.dat"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom_content = b"NONE_TO_ARCHIVE" * 200
        rom = _make_rom(ingest / "game.gba", rom_content)
        h = hash_file(rom)

        _make_dat(dat_path, "Sys", [{
            "name": "Game",
            "roms": [{
                "name": "Game.gba",
                "size": len(rom_content),
                "sha1": h.sha1,
            }],
        }])

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]

            # Run 1: collect with none (directory mode)
            r1 = _full_pipeline_run(sources, dat_path, romroot, work, db, "none")
            assert r1["processed"] == 1
            game_dir = romroot / "Sys" / "Game"
            assert game_dir.is_dir()

            # Run 2: switch to 7z → archive created, directory removed
            r2 = _full_pipeline_run(sources, dat_path, romroot, work, db, "7z-16m")
            assert r2["processed"] == 1

            assert not game_dir.exists()

            archives = list((romroot / "Sys").glob("*.7z"))
            assert len(archives) == 1

            sidecar = read_sidecar(archives[0].parent / (archives[0].name + ".rscf"))
            assert sidecar.renderer == "7z-16m"
            assert len(sidecar.files) == 1

    def test_archive_to_none(self, tmp_path: Path):
        """Archive → game dir, archive removed."""
        ingest = tmp_path / "ingest"
        dat_path = tmp_path / "sel" / "Sys" / "Sys.dat"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom_content = b"ARCHIVE_TO_NONE" * 200
        rom = _make_rom(ingest / "game.gba", rom_content)
        h = hash_file(rom)

        _make_dat(dat_path, "Sys", [{
            "name": "Game",
            "roms": [{
                "name": "Game.gba",
                "size": len(rom_content),
                "sha1": h.sha1,
            }],
        }])

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]

            # Run 1: collect with 7z
            r1 = _full_pipeline_run(sources, dat_path, romroot, work, db, "7z-16m")
            assert r1["processed"] == 1
            archives = list((romroot / "Sys").glob("*.7z"))
            assert len(archives) == 1

            # Run 2: switch to none → directory created, archive removed
            r2 = _full_pipeline_run(sources, dat_path, romroot, work, db, "none")
            assert r2["processed"] == 1

            assert not archives[0].exists()

            game_dir = romroot / "Sys" / "Game"
            assert game_dir.is_dir()
            rom_files = list(game_dir.glob("*.gba"))
            assert len(rom_files) == 1

            rscf_files = list(game_dir.glob("*.rscf"))
            assert len(rscf_files) == 1

            sidecar = read_sidecar(rscf_files[0])
            assert sidecar.renderer == "none"


class TestCompressHelpers:
    """Unit tests for compress.py extension helpers."""

    def test_profile_extension_zstd(self):
        from romtholos.collect.compress import profile_extension
        assert profile_extension("zstd-19", 1) == ".zst"
        assert profile_extension("zstd-19", 2) == ".tar.zst"
        assert profile_extension("zstd-3", 1) == ".zst"
        assert profile_extension("zstd-3", 5) == ".tar.zst"

    def test_profile_extension_7z(self):
        from romtholos.collect.compress import profile_extension
        assert profile_extension("7z-96m", 1) == ".7z"
        assert profile_extension("7z-96m", 3) == ".7z"
        assert profile_extension("7z-16m", 1) == ".7z"

    def test_profile_extension_zip(self):
        from romtholos.collect.compress import profile_extension
        assert profile_extension("zip", 1) == ".zip"
        assert profile_extension("torrentzip", 1) == ".zip"

    def test_profile_extension_none(self):
        from romtholos.collect.compress import profile_extension
        assert profile_extension("none", 1) == ""
        assert profile_extension("none", 5) == ""

    def test_profile_extension_rvz(self):
        from romtholos.collect.compress import profile_extension
        assert profile_extension("rvz-zstd-19", 1) == ".rvz"
        assert profile_extension("rvz-to-iso", 1) == ".iso"

    def test_strip_archive_extension(self):
        from romtholos.collect.compress import strip_archive_extension
        assert strip_archive_extension("Game.7z") == "Game"
        assert strip_archive_extension("Game.zip") == "Game"
        assert strip_archive_extension("Game.zst") == "Game"
        assert strip_archive_extension("Game.tar.zst") == "Game"
        assert strip_archive_extension("Game.rvz") == "Game"
        assert strip_archive_extension("Game.iso") == "Game"
        assert strip_archive_extension("Game.gcz") == "Game"
        assert strip_archive_extension("Game.wia") == "Game"
        assert strip_archive_extension("Game") == "Game"
        assert strip_archive_extension("Game (USA).7z") == "Game (USA)"
        assert strip_archive_extension("Game.bin") == "Game.bin"

    def test_strip_archive_extension_tar_zst_priority(self):
        """Double extension .tar.zst is stripped before single .zst."""
        from romtholos.collect.compress import strip_archive_extension
        assert strip_archive_extension("Multi.tar.zst") == "Multi"

    def test_dolphin_header_matches_profile(self):
        from romtholos.collect.compress import dolphin_header_matches_profile
        header = {
            "block_size": 131072,
            "compression_level": 19,
            "compression_method": "Zstandard",
        }
        assert dolphin_header_matches_profile(header, "rvz-zstd-19") is True
        assert dolphin_header_matches_profile(header, "rvz-zstd-5") is False

        header_5 = {**header, "compression_level": 5}
        assert dolphin_header_matches_profile(header_5, "rvz-zstd-5") is True
        assert dolphin_header_matches_profile(header_5, "rvz-zstd-19") is False

        # Non-dolphin profiles always return False
        assert dolphin_header_matches_profile(header, "7z-16m") is False
        assert dolphin_header_matches_profile(header, "none") is False

    def test_dolphin_header_block_size_mismatch(self):
        from romtholos.collect.compress import dolphin_header_matches_profile
        header = {
            "block_size": 65536,
            "compression_level": 19,
            "compression_method": "Zstandard",
        }
        assert dolphin_header_matches_profile(header, "rvz-zstd-19") is False


# Real RVZ file for integration tests (requires dolphin-tool)
# Set ROMTHOLOS_TEST_RVZ env var to point to a real RVZ file
_TEST_RVZ = Path(
    os.environ.get(
        "ROMTHOLOS_TEST_RVZ",
        "/tmp/test-fixtures/Xeno Crisis.rvz",
    )
)

# ISO hashes from the Redump DAT
_TEST_ISO_SHA1 = "46acc5a470af00a3adf916ba879e382d1540b1c5"
_TEST_ISO_CRC32 = "0c6d4075"
_TEST_ISO_SIZE = 115898368


def _dolphin_available() -> bool:
    import shutil
    if shutil.which("dolphin-tool"):
        return True
    if shutil.which("flatpak"):
        result = subprocess.run(
            ["flatpak", "info", "org.DolphinEmu.dolphin-emu"],
            capture_output=True, timeout=10,
        )
        return result.returncode == 0
    return False


_skip_no_dolphin = pytest.mark.skipif(
    not _dolphin_available(),
    reason="dolphin-tool not available",
)

_skip_no_rvz = pytest.mark.skipif(
    not _TEST_RVZ.exists(),
    reason=f"Test RVZ not found: {_TEST_RVZ}",
)


class TestDolphinHeader:
    """Tests for dolphin-tool header reading (requires dolphin-tool + test RVZ)."""

    @_skip_no_dolphin
    @_skip_no_rvz
    def test_read_dolphin_header(self):
        from romtholos.collect.compress import read_dolphin_header
        header = read_dolphin_header(_TEST_RVZ)
        assert header["compression_method"] == "Zstandard"
        assert header["compression_level"] == 19
        assert header["block_size"] == 131072

    @_skip_no_dolphin
    @_skip_no_rvz
    def test_header_matches_rvz_zstd_19(self):
        from romtholos.collect.compress import (
            dolphin_header_matches_profile,
            read_dolphin_header,
        )
        header = read_dolphin_header(_TEST_RVZ)
        assert dolphin_header_matches_profile(header, "rvz-zstd-19") is True
        assert dolphin_header_matches_profile(header, "rvz-zstd-5") is False


class TestDolphinExtract:
    """Tests for RVZ extraction during scan (requires dolphin-tool + test RVZ)."""

    @_skip_no_dolphin
    @_skip_no_rvz
    def test_rvz_extraction_produces_iso(self, tmp_path: Path):
        """Extract RVZ → ISO via dolphin-tool in extract module."""
        import shutil as _shutil
        from romtholos.collect.extract import ExtractionLimits, extract_recursive

        # Copy to local fs — flatpak can't access CIFS symlinks
        local_rvz = tmp_path / "input" / _TEST_RVZ.name
        local_rvz.parent.mkdir()
        _shutil.copy2(_TEST_RVZ, local_rvz)

        out_dir = tmp_path / "output"
        out_dir.mkdir()
        extracted = extract_recursive(local_rvz, out_dir, ExtractionLimits())
        assert len(extracted) == 1
        assert extracted[0].original_name.endswith(".iso")
        assert extracted[0].size == _TEST_ISO_SIZE

    @_skip_no_dolphin
    @_skip_no_rvz
    def test_rvz_scan_stores_iso_hash(self, tmp_path: Path):
        """Scan an RVZ ingest source → archive_contents has ISO hash."""
        ingest = tmp_path / "ingest"
        ingest.mkdir()
        import shutil
        shutil.copy2(_TEST_RVZ, ingest / _TEST_RVZ.name)

        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="readonly")]
            scan_all(sources, db, work)

            # Should have extracted and hashed the ISO
            stats = db.stats()
            assert stats["archive_contents"] == 1

            # The ISO hash should match the Redump DAT
            rows = db.find_archive_content_by_hash("sha1", _TEST_ISO_SHA1)
            assert len(rows) >= 1
            assert rows[0]["entry_name"].endswith(".iso")

    @_skip_no_dolphin
    @_skip_no_rvz
    def test_rvz_matches_iso_dat(self, tmp_path: Path):
        """Full scan+match: RVZ source matches Redump ISO DAT."""
        ingest = tmp_path / "ingest"
        ingest.mkdir()
        (ingest / _TEST_RVZ.name).symlink_to(_TEST_RVZ)

        selection = tmp_path / "selection" / "Nintendo - GameCube"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        # Create a minimal ISO DAT for this one game
        _make_dat(
            selection / "Nintendo - GameCube.dat",
            "Nintendo - GameCube",
            [{
                "name": "Xeno Crisis (Japan) (En,Ja,Fr,De,Es,It,Nl,Pt) (Unl)",
                "roms": [{
                    "name": "Xeno Crisis (Japan) (En,Ja,Fr,De,Es,It,Nl,Pt) (Unl).iso",
                    "size": _TEST_ISO_SIZE,
                    "crc32": _TEST_ISO_CRC32,
                    "sha1": _TEST_ISO_SHA1,
                }],
            }],
        )

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="readonly")]
            all_sources = [SourceDir(path=romroot, source_type="romroot")] + sources
            scan_all(all_sources, db, work)

            dat = selection / "Nintendo - GameCube.dat"
            ops = match_dat(dat, db)
            assert len(ops) == 1
            assert ops[0].status == "matched"
            assert ops[0].source_type == "archive_content"

    @_skip_no_dolphin
    @_skip_no_rvz
    def test_rvz_copy_when_profile_matches(self, tmp_path: Path):
        """Full pipeline: RVZ with matching profile → copied as-is."""
        ingest = tmp_path / "ingest"
        ingest.mkdir()
        (ingest / _TEST_RVZ.name).symlink_to(_TEST_RVZ)

        selection = tmp_path / "selection" / "Nintendo - GameCube"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        game_name = "Xeno Crisis (Japan) (En,Ja,Fr,De,Es,It,Nl,Pt) (Unl)"
        _make_dat(
            selection / "GC.dat",
            "Nintendo - GameCube",
            [{
                "name": game_name,
                "roms": [{
                    "name": game_name + ".iso",
                    "size": _TEST_ISO_SIZE,
                    "crc32": _TEST_ISO_CRC32,
                    "sha1": _TEST_ISO_SHA1,
                }],
            }],
        )

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="readonly")]
            result = _full_pipeline_run(
                sources,
                selection / "GC.dat",
                romroot, work, db, "rvz-zstd-19",
            )
            assert result["processed"] == 1
            assert result["failed"] == 0

            # Should have copied the RVZ as-is (same size as source)
            rvz_out = romroot / "Nintendo - GameCube" / (game_name + ".rvz")
            assert rvz_out.exists()
            assert rvz_out.stat().st_size == _TEST_RVZ.stat().st_size

            # Sidecar should have ISO hashes
            sidecar = read_sidecar(rvz_out.parent / (rvz_out.name + ".rscf"))
            assert sidecar.renderer == "rvz-zstd-19"
            assert len(sidecar.files) == 1
            assert sidecar.files[0].sha1.upper() == _TEST_ISO_SHA1.upper()

            # Second run: should skip
            result2 = _full_pipeline_run(
                sources,
                selection / "GC.dat",
                romroot, work, db, "rvz-zstd-19",
            )
            assert result2["skipped"] == 1
            assert result2["processed"] == 0
