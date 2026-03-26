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
from romtholos.collect.execute import execute_plan, quarantine_orphans
from romtholos.collect.match import (
    GamePlan, find_orphaned_romroot, group_by_game, match_all_dats, match_dat,
)
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
    # Tests use flat structure: romroot/<system>/ — mirror what CLI now does
    system = game_plans[0].system if game_plans else ""
    target = romroot / system if system else romroot
    return execute_plan(game_plans, target, work, compression, db)


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


def _pipeline_with_dat_folder(
    sources: list[SourceDir],
    selection_dir: Path,
    romroot: Path,
    work: Path,
    db: CacheDB,
    compression: str = "7z-16m",
) -> dict[str, int]:
    """Run scan → match_all_dats → execute with dat_folder-aware romroot paths.

    Mirrors the real CLI flow: romroot / dat_folder / system.
    """
    all_sources = [SourceDir(path=romroot, source_type="romroot")] + sources
    orphaned = romroot / "_orphaned"
    if orphaned.is_dir():
        all_sources.append(SourceDir(path=orphaned, source_type="ingest"))
    scan_all(all_sources, db, work)
    match_results = match_all_dats(selection_dir, db)

    total_stats: dict[str, int] = {
        "processed": 0, "skipped": 0, "failed": 0,
        "missing": 0, "relocated": 0,
    }

    for system, dat_folder, ops in sorted(match_results):
        game_plans = group_by_game(ops)
        target_root = romroot / dat_folder / system
        result = execute_plan(
            game_plans, target_root, work, compression, db,
        )
        for k in total_stats:
            total_stats[k] += result.get(k, 0)

    return total_stats


class TestRelocation:
    """Automatic relocation when DAT layout changes."""

    def test_relocate_archive(self, tmp_path: Path):
        """Game at old path, DAT moved to new folder — archive relocated."""
        ingest = tmp_path / "ingest"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom_content = b"RELOCATE_TEST_ROM" * 100
        rom = _make_rom(ingest / "Game (USA).gba", rom_content)
        hashes = hash_file(rom)

        # Initial DAT under "OldFolder"
        _make_dat(
            selection / "OldFolder" / "System.dat",
            "System",
            [{"name": "Game (USA)", "roms": [{
                "name": "Game (USA).gba",
                "size": rom.stat().st_size,
                "sha1": hashes.sha1,
            }]}],
        )

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]

            # Run 1: collect at OldFolder/System/
            r1 = _pipeline_with_dat_folder(
                sources, selection, romroot, work, db, "none",
            )
            assert r1["processed"] == 1

            old_game_dir = romroot / "OldFolder" / "System" / "Game (USA)"
            assert old_game_dir.exists()

            # Move DAT to "NewFolder"
            new_folder = selection / "NewFolder"
            new_folder.mkdir(parents=True, exist_ok=True)
            (selection / "OldFolder" / "System.dat").rename(
                new_folder / "System.dat",
            )
            # Clean up old selection folder
            (selection / "OldFolder").rmdir()

            # Run 2: should relocate, not repack
            r2 = _pipeline_with_dat_folder(
                sources, selection, romroot, work, db, "none",
            )
            assert r2["relocated"] == 1
            assert r2["processed"] == 0

            # Old path gone, new path exists
            assert not old_game_dir.exists()
            assert not (romroot / "OldFolder").exists()

            new_game_dir = romroot / "NewFolder" / "System" / "Game (USA)"
            assert new_game_dir.exists()

            # ROM + sidecar moved correctly
            rom_files = list(new_game_dir.glob("*.gba"))
            assert len(rom_files) == 1
            rscf_files = list(new_game_dir.glob("*.rscf"))
            assert len(rscf_files) == 1

            # DB updated
            assert db.stats()["romroot_files"] == 1

            # Run 3: nothing to do — skip
            r3 = _pipeline_with_dat_folder(
                sources, selection, romroot, work, db, "none",
            )
            assert r3["skipped"] == 1
            assert r3["relocated"] == 0

    def test_relocate_7z_archive(self, tmp_path: Path):
        """7z archive relocated when DAT moves — no repacking."""
        ingest = tmp_path / "ingest"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom_content = b"RELOCATE_7Z_ROM" * 200
        rom = _make_rom(ingest / "Game.gba", rom_content)
        hashes = hash_file(rom)

        _make_dat(
            selection / "FolderA" / "Sys.dat",
            "Sys",
            [{"name": "Game", "roms": [{
                "name": "Game.gba",
                "size": rom.stat().st_size,
                "sha1": hashes.sha1,
            }]}],
        )

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]

            # Run 1: collect
            r1 = _pipeline_with_dat_folder(
                sources, selection, romroot, work, db,
            )
            assert r1["processed"] == 1

            old_archive = romroot / "FolderA" / "Sys" / "Game.7z"
            assert old_archive.exists()
            old_size = old_archive.stat().st_size

            # Move DAT
            (selection / "FolderB").mkdir(parents=True)
            (selection / "FolderA" / "Sys.dat").rename(
                selection / "FolderB" / "Sys.dat",
            )
            (selection / "FolderA").rmdir()

            # Run 2: relocate
            r2 = _pipeline_with_dat_folder(
                sources, selection, romroot, work, db,
            )
            assert r2["relocated"] == 1
            assert r2["processed"] == 0

            new_archive = romroot / "FolderB" / "Sys" / "Game.7z"
            assert new_archive.exists()
            assert not old_archive.exists()

            # Same size — not repacked
            assert new_archive.stat().st_size == old_size

            # Sidecar moved too
            assert (new_archive.parent / "Game.7z.rscf").exists()

    def test_relocate_plus_new_roms(self, tmp_path: Path):
        """Game at old path + new ROM available — relocate then append."""
        ingest = tmp_path / "ingest"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom_a = b"ROM_A_RELOCATE" * 200
        rom_b = b"ROM_B_RELOCATE" * 300
        rom_a_file = _make_rom(ingest / "a.gba", rom_a)
        hashes_a = hash_file(rom_a_file)

        # DAT with two ROMs, but only rom_a available initially
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(rom_b)
            tmp_b = Path(f.name)
        hashes_b = hash_file(tmp_b)
        tmp_b.unlink()

        _make_dat(
            selection / "FolderA" / "Sys.dat",
            "Sys",
            [{"name": "Game", "roms": [
                {"name": "Game.bin", "size": len(rom_a), "sha1": hashes_a.sha1},
                {"name": "Game.cue", "size": len(rom_b), "sha1": hashes_b.sha1},
            ]}],
        )

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]

            # Run 1: collect partial (only rom_a)
            r1 = _pipeline_with_dat_folder(
                sources, selection, romroot, work, db,
            )
            assert r1["processed"] == 1

            old_archive = romroot / "FolderA" / "Sys" / "Game.7z"
            assert old_archive.exists()

            # Move DAT + add rom_b
            (selection / "FolderB").mkdir(parents=True)
            (selection / "FolderA" / "Sys.dat").rename(
                selection / "FolderB" / "Sys.dat",
            )
            (selection / "FolderA").rmdir()
            _make_rom(ingest / "b.cue", rom_b)

            # Run 2: should relocate then append
            r2 = _pipeline_with_dat_folder(
                sources, selection, romroot, work, db,
            )
            assert r2["relocated"] == 1
            assert r2["processed"] == 1  # new ROM appended

            new_archive = romroot / "FolderB" / "Sys" / "Game.7z"
            assert new_archive.exists()
            assert not old_archive.exists()

            sidecar = read_sidecar(new_archive.parent / "Game.7z.rscf")
            assert len(sidecar.files) == 2

    def test_relocate_cleans_empty_parents(self, tmp_path: Path):
        """After relocation, empty parent directories are removed."""
        ingest = tmp_path / "ingest"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom_content = b"CLEANUP_TEST" * 100
        rom = _make_rom(ingest / "Game.gba", rom_content)
        hashes = hash_file(rom)

        # Deep nesting: A/B/C/
        _make_dat(
            selection / "A" / "B" / "C" / "Sys.dat",
            "Sys",
            [{"name": "Game", "roms": [{
                "name": "Game.gba",
                "size": rom.stat().st_size,
                "sha1": hashes.sha1,
            }]}],
        )

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]

            # Collect at deep path
            r1 = _pipeline_with_dat_folder(
                sources, selection, romroot, work, db, "none",
            )
            assert r1["processed"] == 1
            assert (romroot / "A" / "B" / "C" / "Sys" / "Game").exists()

            # Move DAT to flat location
            (selection / "A" / "B" / "C" / "Sys.dat").rename(
                selection / "Sys.dat",
            )
            # Clean up selection dirs
            (selection / "A" / "B" / "C").rmdir()
            (selection / "A" / "B").rmdir()
            (selection / "A").rmdir()

            # Run: relocate
            r2 = _pipeline_with_dat_folder(
                sources, selection, romroot, work, db, "none",
            )
            assert r2["relocated"] == 1

            # New location (dat_folder = "." for root-level DAT)
            assert (romroot / "." / "Sys" / "Game").exists() or \
                   (romroot / "Sys" / "Game").exists()

            # Old deep directory tree cleaned up
            assert not (romroot / "A").exists()


class TestOrphanQuarantine:
    """Quarantine romroot files not claimed by any current DAT."""

    def test_orphan_archive_quarantined(self, tmp_path: Path):
        """Game removed from DAT → quarantined to _orphaned/."""
        ingest = tmp_path / "ingest"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom_a = b"ORPHAN_ROM_A" * 100
        rom_b = b"ORPHAN_ROM_B" * 200
        file_a = _make_rom(ingest / "a.gba", rom_a)
        file_b = _make_rom(ingest / "b.gba", rom_b)
        hashes_a = hash_file(file_a)
        hashes_b = hash_file(file_b)

        # DAT with two games
        _make_dat(
            selection / "Folder" / "Sys.dat",
            "Sys",
            [
                {"name": "GameA", "roms": [{
                    "name": "GameA.gba",
                    "size": file_a.stat().st_size,
                    "sha1": hashes_a.sha1,
                }]},
                {"name": "GameB", "roms": [{
                    "name": "GameB.gba",
                    "size": file_b.stat().st_size,
                    "sha1": hashes_b.sha1,
                }]},
            ],
        )

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]

            # Run 1: collect both games
            r1 = _pipeline_with_dat_folder(
                sources, selection, romroot, work, db,
            )
            assert r1["processed"] == 2
            assert (romroot / "Folder" / "Sys" / "GameA.7z").exists()
            assert (romroot / "Folder" / "Sys" / "GameB.7z").exists()

            # Update DAT: remove GameB
            _make_dat(
                selection / "Folder" / "Sys.dat",
                "Sys",
                [{"name": "GameA", "roms": [{
                    "name": "GameA.gba",
                    "size": file_a.stat().st_size,
                    "sha1": hashes_a.sha1,
                }]}],
            )

            # Run 2: scan + match + execute + quarantine
            all_sources = [SourceDir(path=romroot, source_type="romroot")] + sources
            scan_all(all_sources, db, work)
            match_results = match_all_dats(selection, db)

            # Detect orphans
            orphans = find_orphaned_romroot(match_results, db)
            assert len(orphans) == 1
            assert any("GameB" in p for p in orphans)

            # Quarantine
            n = quarantine_orphans(orphans, romroot, db)
            assert n == 1

            # GameB moved to _orphaned/, GameA stays
            assert (romroot / "Folder" / "Sys" / "GameA.7z").exists()
            assert not (romroot / "Folder" / "Sys" / "GameB.7z").exists()
            assert (romroot / "_orphaned" / "Folder" / "Sys" / "GameB.7z").exists()
            assert (romroot / "_orphaned" / "Folder" / "Sys" / "GameB.7z.rscf").exists()

            # DB updated — GameB removed from romroot_files (now in _orphaned/)
            assert db.stats()["romroot_files"] == 1

    def test_freshly_collected_not_quarantined(self, tmp_path: Path):
        """Games collected in the same run must not be quarantined.

        Orphan detection must use post-execute state, not pre-execute
        match results which don't know about newly written files.
        """
        ingest = tmp_path / "ingest"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom_content = b"FRESH_COLLECT_ROM" * 200
        rom = _make_rom(ingest / "Game.gba", rom_content)
        hashes = hash_file(rom)

        _make_dat(
            selection / "Folder" / "Sys.dat",
            "Sys",
            [{"name": "Game", "roms": [{
                "name": "Game.gba",
                "size": rom.stat().st_size,
                "sha1": hashes.sha1,
            }]}],
        )

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]

            # Collect the game
            r = _pipeline_with_dat_folder(
                sources, selection, romroot, work, db,
            )
            assert r["processed"] == 1

            archive = romroot / "Folder" / "Sys" / "Game.7z"
            assert archive.exists()

            # Re-match AFTER execute (simulating the cli.py fix)
            fresh_results = match_all_dats(selection, db)
            orphans = find_orphaned_romroot(fresh_results, db)

            # Freshly collected game must NOT be orphaned
            assert len(orphans) == 0, (
                f"Freshly collected game falsely orphaned: {orphans}"
            )

    def test_orphan_directory_quarantined(self, tmp_path: Path):
        """Directory-mode game removed from DAT → quarantined."""
        ingest = tmp_path / "ingest"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom_a = b"DIR_ORPHAN_A" * 100
        rom_b = b"DIR_ORPHAN_B" * 200
        file_a = _make_rom(ingest / "a.gba", rom_a)
        file_b = _make_rom(ingest / "b.gba", rom_b)
        hashes_a = hash_file(file_a)
        hashes_b = hash_file(file_b)

        _make_dat(
            selection / "Folder" / "Sys.dat",
            "Sys",
            [
                {"name": "GameA", "roms": [{
                    "name": "GameA.gba",
                    "size": file_a.stat().st_size,
                    "sha1": hashes_a.sha1,
                }]},
                {"name": "GameB", "roms": [{
                    "name": "GameB.gba",
                    "size": file_b.stat().st_size,
                    "sha1": hashes_b.sha1,
                }]},
            ],
        )

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]

            # Run 1: collect both with none profile
            r1 = _pipeline_with_dat_folder(
                sources, selection, romroot, work, db, "none",
            )
            assert r1["processed"] == 2

            # Update DAT: remove GameB
            _make_dat(
                selection / "Folder" / "Sys.dat",
                "Sys",
                [{"name": "GameA", "roms": [{
                    "name": "GameA.gba",
                    "size": file_a.stat().st_size,
                    "sha1": hashes_a.sha1,
                }]}],
            )

            # Scan + match + detect orphans
            all_sources = [SourceDir(path=romroot, source_type="romroot")] + sources
            scan_all(all_sources, db, work)
            match_results = match_all_dats(selection, db)

            orphans = find_orphaned_romroot(match_results, db)
            assert len(orphans) == 1

            n = quarantine_orphans(orphans, romroot, db)
            assert n == 1

            # GameB directory moved to _orphaned/
            assert (romroot / "Folder" / "Sys" / "GameA").exists()
            assert not (romroot / "Folder" / "Sys" / "GameB").exists()
            assert (romroot / "_orphaned" / "Folder" / "Sys" / "GameB").is_dir()

    def test_orphan_rescued_by_new_dat(self, tmp_path: Path):
        """Orphaned game is rescued when a new DAT claims it."""
        ingest = tmp_path / "ingest"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom_content = b"RESCUE_ROM" * 200
        rom = _make_rom(ingest / "Game.gba", rom_content)
        hashes = hash_file(rom)

        # DAT with one game
        _make_dat(
            selection / "Folder" / "Sys.dat",
            "Sys",
            [{"name": "Game", "roms": [{
                "name": "Game.gba",
                "size": rom.stat().st_size,
                "sha1": hashes.sha1,
            }]}],
        )

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]

            # Run 1: collect
            r1 = _pipeline_with_dat_folder(
                sources, selection, romroot, work, db,
            )
            assert r1["processed"] == 1

            # Remove DAT entirely → orphan
            (selection / "Folder" / "Sys.dat").unlink()
            (selection / "Folder").rmdir()

            all_sources = [SourceDir(path=romroot, source_type="romroot")] + sources
            scan_all(all_sources, db, work)
            match_results = match_all_dats(selection, db)

            orphans = find_orphaned_romroot(match_results, db)
            assert len(orphans) == 1
            quarantine_orphans(orphans, romroot, db)
            assert (romroot / "_orphaned" / "Folder" / "Sys" / "Game.7z").exists()

            # Re-add DAT (possibly at different location)
            _make_dat(
                selection / "NewFolder" / "Sys.dat",
                "Sys",
                [{"name": "Game", "roms": [{
                    "name": "Game.gba",
                    "size": rom.stat().st_size,
                    "sha1": hashes.sha1,
                }]}],
            )

            # Run 2: scan finds orphan in _orphaned/ (ingest), match claims it,
            # execute re-collects via full pipeline
            r2 = _pipeline_with_dat_folder(
                sources, selection, romroot, work, db,
            )
            assert r2["processed"] >= 1  # re-collected from _orphaned

            # Game rescued to correct location
            assert (romroot / "NewFolder" / "Sys" / "Game.7z").exists()


class TestSharedHashMatch:
    """Games sharing a ROM hash must each claim their own romroot archive."""

    def test_shared_hash_claims_own_archive(self, tmp_path: Path):
        """Two games with identical Track 1 — each must match its own archive.

        CD games commonly share silent pregap tracks with identical hashes.
        find_in_romroot must not cross-match game A's archive for game B.
        """
        ingest = tmp_path / "ingest"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        # Shared track — identical content in both games (e.g. silence)
        shared_track = b"SILENCE_PREGAP" * 100

        # Game-specific tracks — unique per game
        game_a_track2 = b"GAME_A_AUDIO_TRACK" * 200
        game_b_track2 = b"GAME_B_AUDIO_TRACK" * 300

        # Create source files
        shared_file = _make_rom(ingest / "shared.bin", shared_track)
        a_track2_file = _make_rom(ingest / "a_track2.bin", game_a_track2)
        b_track2_file = _make_rom(ingest / "b_track2.bin", game_b_track2)

        shared_h = hash_file(shared_file)
        a_h = hash_file(a_track2_file)
        b_h = hash_file(b_track2_file)

        # DAT with two games, both sharing Track 1 hash
        _make_dat(
            selection / "Folder" / "Sys.dat",
            "Sys",
            [
                {"name": "Game A", "roms": [
                    {"name": "Game A (Track 1).bin",
                     "size": len(shared_track), "sha1": shared_h.sha1},
                    {"name": "Game A (Track 2).bin",
                     "size": len(game_a_track2), "sha1": a_h.sha1},
                ]},
                {"name": "Game B", "roms": [
                    {"name": "Game B (Track 1).bin",
                     "size": len(shared_track), "sha1": shared_h.sha1},
                    {"name": "Game B (Track 2).bin",
                     "size": len(game_b_track2), "sha1": b_h.sha1},
                ]},
            ],
        )

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]

            # Run 1: collect both games
            r1 = _pipeline_with_dat_folder(
                sources, selection, romroot, work, db,
            )
            assert r1["processed"] == 4  # 2 ROMs per game

            game_a_archive = romroot / "Folder" / "Sys" / "Game A.7z"
            game_b_archive = romroot / "Folder" / "Sys" / "Game B.7z"
            assert game_a_archive.exists()
            assert game_b_archive.exists()

            # Run 2: both should be skipped (complete), not cross-matched
            r2 = _pipeline_with_dat_folder(
                sources, selection, romroot, work, db,
            )
            assert r2["skipped"] == 4  # all ROMs skipped
            assert r2["processed"] == 0

            # Critical: each game's in_romroot ops must point to its OWN archive
            all_sources = [SourceDir(path=romroot, source_type="romroot")] + sources
            scan_all(all_sources, db, work)
            match_results = match_all_dats(selection, db)

            for system, dat_folder, ops in match_results:
                games = group_by_game(ops)
                for game in games:
                    for op in game.existing_ops:
                        assert game.game_name in op.source_path, (
                            f"Game '{game.game_name}' matched to wrong archive: "
                            f"{op.source_path}"
                        )

            # No false orphans
            orphans = find_orphaned_romroot(match_results, db)
            assert len(orphans) == 0, (
                f"False orphans detected: {orphans}"
            )


class TestScanPathFilter:
    """Tests for ``scan_all(path_filter=...)`` subfolder restriction."""

    def test_path_filter_restricts_scan(self, tmp_path: Path):
        """Only files under path_filter are scanned."""
        source = tmp_path / "source"
        sub_a = source / "a"
        sub_b = source / "b"

        rom_a = _make_rom(sub_a / "game_a.gba", b"ROM_A" * 100)
        rom_b = _make_rom(sub_b / "game_b.gba", b"ROM_B" * 100)

        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=source, source_type="ingest")]

            # Scan only subfolder "a"
            results = scan_all(sources, db, work, path_filter=sub_a)

            # Only rom_a should be scanned
            assert results[str(source)].files_hashed == 1

            # DB should contain rom_a but not rom_b
            row_a = db.find_by_hash("sha1", hash_file(rom_a).sha1)
            assert len(row_a) == 1
            row_b = db.find_by_hash("sha1", hash_file(rom_b).sha1)
            assert len(row_b) == 0

    def test_path_filter_skips_unrelated_sources(self, tmp_path: Path):
        """Sources that don't contain path_filter are skipped entirely."""
        source_a = tmp_path / "source_a"
        source_b = tmp_path / "source_b"
        sub = source_a / "sub"

        _make_rom(sub / "game.gba", b"ROM" * 100)
        _make_rom(source_b / "other.gba", b"OTHER" * 100)

        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        with CacheDB(db_path) as db:
            sources = [
                SourceDir(path=source_a, source_type="ingest"),
                SourceDir(path=source_b, source_type="ingest"),
            ]

            results = scan_all(sources, db, work, path_filter=sub)

            # Only source_a should appear in results
            assert str(source_a) in results
            assert str(source_b) not in results

    def test_path_filter_equal_to_source(self, tmp_path: Path):
        """path_filter equal to a source root scans that source normally."""
        source = tmp_path / "source"
        _make_rom(source / "game.gba", b"ROM" * 100)

        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=source, source_type="ingest")]

            results = scan_all(sources, db, work, path_filter=source)
            assert results[str(source)].files_hashed == 1

    def test_path_filter_romroot(self, tmp_path: Path):
        """path_filter works for romroot sources too."""
        from rscf import Sidecar, SidecarResolver, StorageMode, write_sidecar

        romroot = tmp_path / "romroot"
        system_a = romroot / "System A"
        system_b = romroot / "System B"

        rom_a = _make_rom(system_a / "Game A" / "game.gba", b"A" * 100)
        rom_b = _make_rom(system_b / "Game B" / "game.gba", b"B" * 100)

        # Write sidecars so romroot scan can load them
        resolver = SidecarResolver(StorageMode.IN_TREE)
        for rom in (rom_a, rom_b):
            hashes = hash_file(rom)
            from rscf import FileEntry
            sc = Sidecar(
                renderer="none",
                files=[FileEntry(
                    path=rom.name,
                    size=rom.stat().st_size,
                    crc32=hashes.crc32,
                    md5=hashes.md5,
                    sha1=hashes.sha1,
                    sha256=hashes.sha256,
                    blake3=hashes.blake3,
                )],
            )
            write_sidecar(sc, resolver.sidecar_path(rom))

        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=romroot, source_type="romroot")]

            # Scan only System A
            results = scan_all(sources, db, work, path_filter=system_a)

            assert results[str(romroot)].files_from_sidecar == 1

            # Only Game A should be in romroot_files
            assert db.stats()["romroot_files"] == 1


class TestStandaloneExecute:
    """Tests for decoupled scan → execute workflow."""

    def test_execute_with_preexisting_scan(self, tmp_path: Path):
        """Execute works using scan data from a prior scan_all call."""
        ingest = tmp_path / "ingest"
        selection = tmp_path / "selection" / "Test"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom_content = b"STANDALONE_EXEC_TEST" * 100
        rom = _make_rom(ingest / "Game (USA).gba", rom_content)
        hashes = hash_file(rom)

        _make_dat(
            selection / "Test System.dat",
            "Test System",
            [{
                "name": "Game (USA)",
                "roms": [{
                    "name": "Game (USA).gba",
                    "size": rom.stat().st_size,
                    "crc32": hashes.crc32,
                    "md5": hashes.md5,
                    "sha1": hashes.sha1,
                }],
            }],
        )

        with CacheDB(db_path) as db:
            # Phase 1: Scan (separate step)
            sources = [
                SourceDir(path=romroot, source_type="romroot"),
                SourceDir(path=ingest, source_type="ingest"),
            ]
            scan_all(sources, db, work)

            assert db.stats()["scanned_files"] > 0

            # Phase 2+3: Match + Execute (separate step, no re-scan)
            match_results = match_all_dats(selection, db)
            assert len(match_results) > 0

            for system, dat_folder, ops in sorted(match_results):
                game_plans = group_by_game(ops)
                target_root = romroot / dat_folder / system
                result = execute_plan(
                    game_plans, target_root, work, "none", db,
                )

                assert result["processed"] == 1
                assert result["failed"] == 0

            # Verify the ROM was collected
            assert db.stats()["romroot_files"] == 1

    def test_execute_system_filter(self, tmp_path: Path):
        """System filter in match results restricts execution."""
        ingest = tmp_path / "ingest"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        # Create ROMs for two systems
        rom_a = _make_rom(ingest / "game_a.gba", b"SYS_A_ROM" * 100)
        rom_b = _make_rom(ingest / "game_b.nes", b"SYS_B_ROM" * 100)
        hashes_a = hash_file(rom_a)
        hashes_b = hash_file(rom_b)

        sel_a = selection / "GBA"
        sel_b = selection / "NES"

        _make_dat(
            sel_a / "GBA System.dat", "GBA System",
            [{"name": "Game A", "roms": [{
                "name": "game_a.gba",
                "size": rom_a.stat().st_size,
                "crc32": hashes_a.crc32, "md5": hashes_a.md5,
                "sha1": hashes_a.sha1,
            }]}],
        )
        _make_dat(
            sel_b / "NES System.dat", "NES System",
            [{"name": "Game B", "roms": [{
                "name": "game_b.nes",
                "size": rom_b.stat().st_size,
                "crc32": hashes_b.crc32, "md5": hashes_b.md5,
                "sha1": hashes_b.sha1,
            }]}],
        )

        with CacheDB(db_path) as db:
            sources = [
                SourceDir(path=romroot, source_type="romroot"),
                SourceDir(path=ingest, source_type="ingest"),
            ]
            scan_all(sources, db, work)

            # Match all, then filter to GBA only
            match_results = match_all_dats(selection, db)
            assert len(match_results) == 2

            filtered = [
                (s, d, ops) for s, d, ops in match_results
                if s == "GBA System"
            ]
            assert len(filtered) == 1

            # Execute only GBA
            for system, dat_folder, ops in filtered:
                game_plans = group_by_game(ops)
                target_root = romroot / dat_folder / system
                result = execute_plan(
                    game_plans, target_root, work, "none", db,
                )
                assert result["processed"] == 1

            # Only GBA game should be in romroot
            assert db.stats()["romroot_files"] == 1

    def test_execute_empty_db_has_no_matches(self, tmp_path: Path):
        """Execute on empty DB produces no match results."""
        selection = tmp_path / "selection" / "Test"
        db_path = tmp_path / "test.db"

        _make_dat(
            selection / "Test System.dat",
            "Test System",
            [{"name": "Game", "roms": [{
                "name": "game.gba", "size": 100,
                "crc32": "deadbeef", "sha1": "da39a3ee5e6b4b0d3255bfef95601890afd80709",
            }]}],
        )

        with CacheDB(db_path) as db:
            assert db.stats()["scanned_files"] == 0

            # Match should return results but all missing
            match_results = match_all_dats(selection, db)
            for _, _, ops in match_results:
                for op in ops:
                    assert op.status == "missing"

    def test_scan_then_execute_incrementally(self, tmp_path: Path):
        """Scan subfolder, execute, scan another subfolder, execute again."""
        ingest = tmp_path / "ingest"
        sub_a = ingest / "batch_a"
        sub_b = ingest / "batch_b"
        selection = tmp_path / "selection" / "Test"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom_a = _make_rom(sub_a / "game_a.gba", b"BATCH_A" * 100)
        rom_b = _make_rom(sub_b / "game_b.gba", b"BATCH_B" * 100)
        hashes_a = hash_file(rom_a)
        hashes_b = hash_file(rom_b)

        _make_dat(
            selection / "Test System.dat",
            "Test System",
            [
                {"name": "Game A", "roms": [{
                    "name": "game_a.gba",
                    "size": rom_a.stat().st_size,
                    "crc32": hashes_a.crc32, "md5": hashes_a.md5,
                    "sha1": hashes_a.sha1,
                }]},
                {"name": "Game B", "roms": [{
                    "name": "game_b.gba",
                    "size": rom_b.stat().st_size,
                    "crc32": hashes_b.crc32, "md5": hashes_b.md5,
                    "sha1": hashes_b.sha1,
                }]},
            ],
        )

        with CacheDB(db_path) as db:
            sources = [
                SourceDir(path=romroot, source_type="romroot"),
                SourceDir(path=ingest, source_type="ingest"),
            ]

            # Round 1: scan only batch_a, then execute
            scan_all(sources, db, work, path_filter=sub_a)
            match_results = match_all_dats(selection, db)

            for system, dat_folder, ops in sorted(match_results):
                game_plans = group_by_game(ops)
                target_root = romroot / dat_folder / system
                result = execute_plan(
                    game_plans, target_root, work, "none", db,
                )
                assert result["processed"] == 1  # Game A
                assert result["missing"] == 1     # Game B

            assert db.stats()["romroot_files"] == 1

            # Round 2: scan batch_b, then execute
            # Re-scan romroot too so it knows about Game A
            scan_all(sources, db, work, path_filter=sub_b)
            # Romroot needs rescanning to see Game A
            scan_all(
                [SourceDir(path=romroot, source_type="romroot")],
                db, work,
            )
            match_results = match_all_dats(selection, db)

            for system, dat_folder, ops in sorted(match_results):
                game_plans = group_by_game(ops)
                target_root = romroot / dat_folder / system
                result = execute_plan(
                    game_plans, target_root, work, "none", db,
                )
                assert result["processed"] == 1  # Game B
                assert result["skipped"] == 1     # Game A (in_romroot)

            assert db.stats()["romroot_files"] == 2


class TestSourceConsolidation:
    """Tests for source consolidation — minimize archives per game."""

    def test_cross_game_romroot_consolidated_to_source(self, tmp_path: Path):
        """Shared track matched as in_romroot from another game is
        re-assigned to the game's own source archive when it contains
        the same track."""
        ingest = tmp_path / "ingest"
        selection = tmp_path / "selection" / "Sys"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        # Shared silence track — same content, same hash
        shared_content = b"SILENCE_TRACK" * 500

        # Game A: unique track + shared track (collected first)
        unique_a = _make_rom(ingest / "unique_a.bin", b"GAME_A_DATA" * 500)
        shared_a = _make_rom(ingest / "shared_a.bin", shared_content)

        # Game B: unique track + shared track (same hash as Game A's)
        # Put both in a zip so they're archive_content matches
        unique_b_content = b"GAME_B_DATA" * 500
        unique_b_tmp = _make_rom(ingest / "unique_b_tmp.bin", unique_b_content)
        shared_b_tmp = _make_rom(ingest / "shared_b_tmp.bin", shared_content)

        hashes_unique_a = hash_file(unique_a)
        hashes_shared = hash_file(shared_a)
        hashes_unique_b = hash_file(unique_b_tmp)

        # Put Game B's files in a zip archive
        game_b_zip = ingest / "game_b.zip"
        with zipfile.ZipFile(game_b_zip, "w") as zf:
            zf.write(unique_b_tmp, "Game B (Track 02).bin")
            zf.write(shared_b_tmp, "Game B (Track 01).bin")
        unique_b_tmp.unlink()
        shared_b_tmp.unlink()

        def _rom_entry(name, size, h):
            return {
                "name": name, "size": size,
                "crc32": h.crc32, "md5": h.md5, "sha1": h.sha1,
            }

        _make_dat(
            selection / "Sys.dat", "Sys",
            [
                {
                    "name": "Game A",
                    "roms": [
                        _rom_entry("unique_a.bin", unique_a.stat().st_size,
                                   hashes_unique_a),
                        _rom_entry("shared.bin", shared_a.stat().st_size,
                                   hashes_shared),
                    ],
                },
                {
                    "name": "Game B",
                    "roms": [
                        _rom_entry("Game B (Track 02).bin",
                                   len(unique_b_content),
                                   hashes_unique_b),
                        _rom_entry("Game B (Track 01).bin",
                                   shared_a.stat().st_size,
                                   hashes_shared),
                    ],
                },
            ],
        )

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]

            # Run 1: collect Game A (from loose files) + Game B (from zip)
            result = _full_pipeline_run(
                sources, selection / "Sys.dat", romroot, work, db, "none",
            )
            assert result["processed"] == 4  # 2 games × 2 ROMs
            assert result["failed"] == 0

            # Both games collected
            assert (romroot / "Sys" / "Game A").exists()
            assert (romroot / "Sys" / "Game B").exists()

            # Verify Game B has its own copy of the shared track
            game_b_dir = romroot / "Sys" / "Game B"
            track_01 = game_b_dir / "Game B (Track 01).bin"
            assert track_01.exists()
            assert hash_file(track_01).sha1 == hashes_shared.sha1

    def test_consolidation_preserves_same_game_romroot(self, tmp_path: Path):
        """Same-game in_romroot ops are never re-assigned."""
        ingest = tmp_path / "ingest"
        selection = tmp_path / "selection" / "Sys"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom_a = _make_rom(ingest / "track_a.bin", b"TRACK_A" * 500)
        rom_b = _make_rom(ingest / "track_b.bin", b"TRACK_B" * 500)
        hashes_a = hash_file(rom_a)
        hashes_b = hash_file(rom_b)

        _make_dat(
            selection / "Sys.dat", "Sys",
            [{
                "name": "Game",
                "roms": [
                    {
                        "name": "track_a.bin",
                        "size": rom_a.stat().st_size,
                        "crc32": hashes_a.crc32, "md5": hashes_a.md5,
                        "sha1": hashes_a.sha1,
                    },
                    {
                        "name": "track_b.bin",
                        "size": rom_b.stat().st_size,
                        "crc32": hashes_b.crc32, "md5": hashes_b.md5,
                        "sha1": hashes_b.sha1,
                    },
                ],
            }],
        )

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]

            # Run 1: collect
            r1 = _full_pipeline_run(
                sources, selection / "Sys.dat", romroot, work, db, "none",
            )
            assert r1["processed"] == 2

            # Run 2: game is complete, should skip
            r2 = _full_pipeline_run(
                sources, selection / "Sys.dat", romroot, work, db, "none",
            )
            assert r2["processed"] == 0
            assert r2["skipped"] == 2

    def test_multi_source_game_all_tracks_collected(self, tmp_path: Path):
        """Game with tracks split across two archives — both extracted."""
        ingest = tmp_path / "ingest"
        selection = tmp_path / "selection" / "Sys"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        # Track 1 only in archive A
        track1_content = b"TRACK1_ONLY_A" * 500
        # Track 2 only in archive B
        track2_content = b"TRACK2_ONLY_B" * 500

        track1_tmp = _make_rom(ingest / "t1.bin", track1_content)
        track2_tmp = _make_rom(ingest / "t2.bin", track2_content)
        hashes_1 = hash_file(track1_tmp)
        hashes_2 = hash_file(track2_tmp)

        # Create two separate archives
        arc_a = ingest / "archive_a.zip"
        with zipfile.ZipFile(arc_a, "w") as zf:
            zf.write(track1_tmp, "track1.bin")
        arc_b = ingest / "archive_b.zip"
        with zipfile.ZipFile(arc_b, "w") as zf:
            zf.write(track2_tmp, "track2.bin")
        track1_tmp.unlink()
        track2_tmp.unlink()

        _make_dat(
            selection / "Sys.dat", "Sys",
            [{
                "name": "Split Game",
                "roms": [
                    {
                        "name": "track1.bin",
                        "size": len(track1_content),
                        "crc32": hashes_1.crc32, "md5": hashes_1.md5,
                        "sha1": hashes_1.sha1,
                    },
                    {
                        "name": "track2.bin",
                        "size": len(track2_content),
                        "crc32": hashes_2.crc32, "md5": hashes_2.md5,
                        "sha1": hashes_2.sha1,
                    },
                ],
            }],
        )

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]
            result = _full_pipeline_run(
                sources, selection / "Sys.dat", romroot, work, db, "none",
            )

            assert result["processed"] == 2
            assert result["failed"] == 0

            game_dir = romroot / "Sys" / "Split Game"
            assert (game_dir / "track1.bin").exists()
            assert (game_dir / "track2.bin").exists()


class TestSourceAffinityRanking:
    """Tests for source affinity ranking — prefer game's own archive."""

    def test_affinity_game_name_aaru(self):
        """Exact game-name match + .aaru scores highest."""
        from romtholos.collect.match import _source_affinity

        assert _source_affinity("/path/Game X (USA).aaru", "Game X (USA)") == (1, 1)

    def test_affinity_game_name_7z(self):
        """Exact game-name match + .7z: name match but not aaru."""
        from romtholos.collect.match import _source_affinity

        assert _source_affinity("/path/Game X (USA).7z", "Game X (USA)") == (1, 0)

    def test_affinity_different_name_aaru(self):
        """Different game name + .aaru: aaru bonus but no name match."""
        from romtholos.collect.match import _source_affinity

        assert _source_affinity("/path/Other Game.aaru", "Game X (USA)") == (0, 1)

    def test_affinity_different_name_7z(self):
        """Different game name + .7z: no bonus at all."""
        from romtholos.collect.match import _source_affinity

        assert _source_affinity("/path/Other Game.7z", "Game X (USA)") == (0, 0)

    def test_affinity_empty_game_name(self):
        """Empty game name never matches, but aaru bonus still applies."""
        from romtholos.collect.match import _source_affinity

        assert _source_affinity("/path/Game.aaru", "") == (0, 1)

    def test_affinity_aaruf_extension(self):
        """Aaru format variant .aaruf recognized as aaru."""
        from romtholos.collect.match import _source_affinity

        assert _source_affinity("/path/Game.aaruf", "whatever")[1] == 1

    def test_affinity_dicf_extension(self):
        """Aaru format variant .dicf recognized as aaru."""
        from romtholos.collect.match import _source_affinity

        assert _source_affinity("/path/Game.dicf", "whatever")[1] == 1

    def test_affinity_ranking_order(self):
        """Verify ranking produces correct sort order."""
        from romtholos.collect.match import _source_affinity

        game = "Game X (USA)"
        candidates = [
            "/path/Other.7z",
            "/path/Other.aaru",
            "/path/Game X (USA).7z",
            "/path/Game X (USA).aaru",
        ]
        ranked = sorted(candidates, key=lambda p: _source_affinity(p, game))
        assert ranked[-1] == "/path/Game X (USA).aaru"  # best
        assert ranked[-2] == "/path/Game X (USA).7z"  # second
        assert ranked[0] == "/path/Other.7z"  # worst

    def test_match_prefers_game_name_archive(self, tmp_path: Path):
        """When two archives contain the same hash, match prefers the
        one named after the game."""
        ingest = tmp_path / "ingest"
        ingest.mkdir()
        selection = tmp_path / "selection" / "Sys"
        romroot = tmp_path / "romroot"
        romroot.mkdir()
        work = tmp_path / "work"
        work.mkdir()
        db_path = tmp_path / "test.db"

        # Shared content — same hash in both archives
        shared_content = b"SHARED_TRACK_DATA" * 500
        unique_a = b"UNIQUE_GAME_A" * 500
        unique_b = b"UNIQUE_GAME_B" * 500

        # Create temp files for hashing
        shared_tmp = _make_rom(tmp_path / "shared.bin", shared_content)
        unique_a_tmp = _make_rom(tmp_path / "ua.bin", unique_a)
        unique_b_tmp = _make_rom(tmp_path / "ub.bin", unique_b)

        h_shared = hash_file(shared_tmp)
        h_unique_a = hash_file(unique_a_tmp)
        h_unique_b = hash_file(unique_b_tmp)

        # Game A archive: unique_a + shared
        arc_a = ingest / "Game A.zip"
        with zipfile.ZipFile(arc_a, "w") as zf:
            zf.write(unique_a_tmp, "Game A (Track 1).bin")
            zf.write(shared_tmp, "Game A (Track 2).bin")

        # Game B archive: unique_b + shared (same hash)
        arc_b = ingest / "Game B.zip"
        with zipfile.ZipFile(arc_b, "w") as zf:
            zf.write(unique_b_tmp, "Game B (Track 1).bin")
            zf.write(shared_tmp, "Game B (Track 2).bin")

        for f in (shared_tmp, unique_a_tmp, unique_b_tmp):
            f.unlink()

        def _rom(name, size, h):
            return {
                "name": name, "size": size,
                "crc32": h.crc32, "md5": h.md5, "sha1": h.sha1,
            }

        _make_dat(selection / "Sys.dat", "Sys", [
            {
                "name": "Game A",
                "roms": [
                    _rom("Game A (Track 1).bin", len(unique_a), h_unique_a),
                    _rom("Game A (Track 2).bin", len(shared_content), h_shared),
                ],
            },
            {
                "name": "Game B",
                "roms": [
                    _rom("Game B (Track 1).bin", len(unique_b), h_unique_b),
                    _rom("Game B (Track 2).bin", len(shared_content), h_shared),
                ],
            },
        ])

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]
            result = _full_pipeline_run(
                sources, selection / "Sys.dat", romroot, work, db, "none",
            )

            # Both games collected successfully
            assert result["failed"] == 0
            assert result["processed"] == 4

            # Verify both games have their shared track
            game_a_dir = romroot / "Sys" / "Game A"
            game_b_dir = romroot / "Sys" / "Game B"
            assert (game_a_dir / "Game A (Track 2).bin").exists()
            assert (game_b_dir / "Game B (Track 2).bin").exists()

            # Both shared tracks have the correct hash
            assert hash_file(game_a_dir / "Game A (Track 2).bin").sha1 == h_shared.sha1
            assert hash_file(game_b_dir / "Game B (Track 2).bin").sha1 == h_shared.sha1

    def test_consolidation_prefers_game_named_archive(self, tmp_path: Path):
        """Consolidation picks the game's own archive as primary even when
        another archive has an equal track count."""
        ingest = tmp_path / "ingest"
        ingest.mkdir()
        selection = tmp_path / "selection" / "Sys"
        romroot = tmp_path / "romroot"
        romroot.mkdir()
        work = tmp_path / "work"
        work.mkdir()
        db_path = tmp_path / "test.db"

        track1 = b"TRACK_ONE_DATA" * 500
        track2 = b"TRACK_TWO_DATA" * 500

        t1 = _make_rom(tmp_path / "t1.bin", track1)
        t2 = _make_rom(tmp_path / "t2.bin", track2)
        h1 = hash_file(t1)
        h2 = hash_file(t2)

        # Game's own archive — named after the game
        own = ingest / "My Game.zip"
        with zipfile.ZipFile(own, "w") as zf:
            zf.write(t1, "track1.bin")
            zf.write(t2, "track2.bin")

        # Compilation archive — contains same tracks but different name
        comp = ingest / "compilation.zip"
        with zipfile.ZipFile(comp, "w") as zf:
            zf.write(t1, "track1.bin")
            zf.write(t2, "track2.bin")

        t1.unlink()
        t2.unlink()

        _make_dat(selection / "Sys.dat", "Sys", [{
            "name": "My Game",
            "roms": [
                {
                    "name": "track1.bin", "size": len(track1),
                    "crc32": h1.crc32, "md5": h1.md5, "sha1": h1.sha1,
                },
                {
                    "name": "track2.bin", "size": len(track2),
                    "crc32": h2.crc32, "md5": h2.md5, "sha1": h2.sha1,
                },
            ],
        }])

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]

            # Scan + match (don't execute — just inspect match results)
            all_sources = [SourceDir(path=romroot, source_type="romroot")] + sources
            scan_all(all_sources, db, work)
            ops = match_dat(selection / "Sys.dat", db)

            # After match + consolidation, both tracks should come from
            # "My Game.zip" (the game-name-matching archive)
            matched_ops = [op for op in ops if op.status == "matched"]
            source_paths = {op.source_path for op in matched_ops}

            # All matched ops should be from the game's own archive
            assert len(source_paths) == 1
            assert str(own) in source_paths.pop()


class TestRebuildCorruptExisting:
    """Rebuild/recompress must verify existing ROMs against DAT hashes."""

    def test_corrupt_rom_in_existing_archive_rejected(self, tmp_path: Path):
        """A corrupt ROM in an existing archive is skipped during rebuild.

        Simulates: previous run created an archive with a truncated Track 02.
        On rebuild (profile change), the corrupt file must not be reused.
        If the correct file is available from another source, it should be
        used instead.
        """
        ingest = tmp_path / "ingest"
        ingest.mkdir()
        selection = tmp_path / "selection" / "Sys"
        romroot = tmp_path / "romroot"
        romroot.mkdir()
        work = tmp_path / "work"
        work.mkdir()
        db_path = tmp_path / "test.db"

        # Two ROMs: track1 (correct) and track2 (will be corrupt in archive)
        track1_content = b"TRACK_ONE_CORRECT" * 500
        track2_content = b"TRACK_TWO_CORRECT" * 500
        track2_corrupt = b"TRUNCATED"  # wrong data

        t1 = _make_rom(tmp_path / "t1.bin", track1_content)
        t2 = _make_rom(tmp_path / "t2.bin", track2_content)
        h1 = hash_file(t1)
        h2 = hash_file(t2)

        _make_dat(selection / "Sys.dat", "Sys", [{
            "name": "Game",
            "roms": [
                {
                    "name": "track1.bin", "size": len(track1_content),
                    "crc32": h1.crc32, "md5": h1.md5, "sha1": h1.sha1,
                },
                {
                    "name": "track2.bin", "size": len(track2_content),
                    "crc32": h2.crc32, "md5": h2.md5, "sha1": h2.sha1,
                },
            ],
        }])

        # Put correct files in ingest (available for collection)
        arc = ingest / "Game.zip"
        with zipfile.ZipFile(arc, "w") as zf:
            zf.write(t1, "track1.bin")
            zf.write(t2, "track2.bin")
        t1.unlink()
        t2.unlink()

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=ingest, source_type="ingest")]

            # Run 1: collect with 7z profile — creates correct .7z
            r1 = _full_pipeline_run(
                sources, selection / "Sys.dat", romroot, work, db, "7z-16m",
            )
            assert r1["processed"] == 2
            assert r1["failed"] == 0

            archives = list((romroot / "Sys").glob("*.7z"))
            assert len(archives) == 1

            # Now CORRUPT the existing archive: replace track2 with truncated data
            from romtholos.collect.compress import compress_7z
            corrupt_work = tmp_path / "corrupt_work"
            corrupt_work.mkdir()
            t1_copy = corrupt_work / "track1.bin"
            t2_corrupt_file = corrupt_work / "track2.bin"
            t1_copy.write_bytes(track1_content)
            t2_corrupt_file.write_bytes(track2_corrupt)

            # Overwrite the .7z with corrupt content
            corrupt_archive = archives[0]
            corrupt_archive.unlink()
            compress_7z([t1_copy, t2_corrupt_file], corrupt_archive.with_suffix(""))

            # Also corrupt the sidecar so it matches the corrupt archive
            from rscf import FileEntry, Sidecar, write_sidecar, SidecarResolver, StorageMode
            h1_good = hash_file(t1_copy)
            h2_bad = hash_file(t2_corrupt_file)
            corrupt_stat = corrupt_archive.stat()
            corrupt_container = hash_file(corrupt_archive)
            sidecar = Sidecar(
                container_blake3=corrupt_container.blake3,
                container_size=corrupt_stat.st_size,
                container_mtime_ns=corrupt_stat.st_mtime_ns,
                container_ctime_ns=corrupt_stat.st_ctime_ns,
                container_inode=corrupt_stat.st_ino,
                renderer="7z-16m",
                files=[
                    FileEntry.from_hashes(
                        path="track1.bin", size=len(track1_content), hashes=h1_good,
                    ),
                    FileEntry.from_hashes(
                        path="track2.bin", size=len(track2_corrupt), hashes=h2_bad,
                    ),
                ],
            )
            resolver = SidecarResolver(StorageMode.IN_TREE)
            write_sidecar(sidecar, resolver.sidecar_path(corrupt_archive))

            # Clear stat-cache so rescan picks up the corrupt archive.
            # Don't use force_rescan — that rebuilds sidecars from container
            # hashes. We want to load our corrupt sidecar's per-ROM entries.
            db._conn.execute("DELETE FROM scanned_files")
            db._conn.execute("DELETE FROM romroot_files")
            db._conn.commit()

            # Run 2: switch profile → triggers REBUILD.
            # Match phase: track1 in_romroot (hash matches), track2 matched
            # from ingest (romroot has corrupt hash, doesn't match DAT).
            # Rebuild extracts existing .7z, our fix rejects the corrupt
            # track2, and the correct version from ingest takes its place.
            r2 = _full_pipeline_run(
                sources, selection / "Sys.dat", romroot, work, db, "zstd-19",
            )
            assert r2["failed"] == 0

            # Verify the new archive has the CORRECT track2
            new_archives = list((romroot / "Sys").glob("*.zst"))
            assert len(new_archives) == 1

            # Extract and verify track2 hash matches DAT (not the corrupt version)
            from romtholos.collect.extract import extract_recursive
            verify_dir = tmp_path / "verify"
            verify_dir.mkdir()
            extracted = extract_recursive(new_archives[0], verify_dir)
            track2_extracted = [e for e in extracted if "track2" in e.original_name]
            assert len(track2_extracted) == 1
            assert hash_file(track2_extracted[0].path).sha1 == h2.sha1
