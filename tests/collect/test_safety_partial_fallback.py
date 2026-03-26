"""Partial fallback safety — FILE_SAFETY.md § Partial game fallback.

When the configured profile requires all tracks (aaru) but not all tracks
are available, the game falls back to partial_fallback profile. Games below
partial_min_ratio are skipped entirely.

partial_min_ratio only applies to profiles that require all tracks (aaru/rvz).
For standard archive profiles (7z, zstd, zip), partial collection is normal:
available ROMs are collected, missing ones are tracked.

Safety concerns:
- Standard profiles: partial games collected normally, sidecar correct
- All-tracks profiles: below ratio → cleanly skipped, no corrupt archive
- The fallback archive must be fully verified with correct RSCF sidecar
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from rscf import hash_file, read_sidecar

from romtholos.collect.config import SourceDir
from romtholos.collect.db import CacheDB
from romtholos.collect.execute import execute_plan
from romtholos.collect.match import group_by_game, match_all_dats
from romtholos.collect.scan import scan_all

from .conftest import make_dat, make_rom, pipeline_run, rom_entry

FIXTURES = Path(__file__).parent.parent / "fixtures"


def _inner_hashes_from_scan(source_path: str, db: CacheDB) -> list[dict]:
    """Get the extracted inner-content hashes for a disc image from the DB."""
    rows = db.get_archive_contents(source_path)
    return [
        {
            "name": row["entry_name"],
            "size": row["entry_size"],
            "sha1": row["sha1"],
            "crc32": row["crc32"],
            "md5": row["md5"],
        }
        for row in rows
    ]


class TestStandardProfilePartialCollection:
    """Standard profiles (7z, zstd): partial games collected normally."""

    def test_partial_game_collected_with_available_roms(self, env):
        """With standard profile, available ROMs are collected even if some missing."""
        t1 = make_rom(env.source / "Track 1.bin", b"PARTIAL_STD" * 200)
        e1 = rom_entry(t1)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [
                e1,
                {"name": "Track 2.bin", "size": 1000, "sha1": "A" * 40},
                {"name": "Track 3.bin", "size": 1000, "sha1": "B" * 40},
            ]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # Archive created with available ROM
            archive = env.romroot / "F" / "Sys" / "Game.7z"
            assert archive.exists()
            sidecar = read_sidecar(archive.parent / "Game.7z.rscf")
            assert len(sidecar.files) == 1
            assert sidecar.files[0].sha1.upper() == e1["sha1"].upper()

    def test_partial_game_sidecar_verified(self, env):
        """Partial game's archive is still BLAKE3-verified and sidecar valid."""
        t1 = make_rom(env.source / "Track 1.bin", b"VERIFY_PARTIAL" * 200)
        t2 = make_rom(env.source / "Track 2.bin", b"VERIFY_AVAIL" * 300)
        e1 = rom_entry(t1)
        e2 = rom_entry(t2)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [
                e1, e2,
                {"name": "Track 3.bin", "size": 1000, "sha1": "C" * 40},
            ]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            archive = env.romroot / "F" / "Sys" / "Game.7z"
            sidecar = read_sidecar(archive.parent / "Game.7z.rscf")

            # Container hash matches file on disk
            actual = hash_file(archive).blake3
            assert actual.upper() == sidecar.container_blake3.upper()

            # ROM hashes correct
            assert len(sidecar.files) == 2
            hashes = {Path(f.path).name: f.sha1.upper() for f in sidecar.files}
            assert hashes["Track 1.bin"] == e1["sha1"].upper()
            assert hashes["Track 2.bin"] == e2["sha1"].upper()


class TestAllTracksProfileRatioSkip:
    """Profiles requiring all tracks (aaru): partial_min_ratio enforced."""

    def test_below_ratio_skipped_for_aaru(self, env):
        """Aaru profile with ratio below threshold: game skipped entirely.

        Game has 3 ROMs in DAT, only 2 available from aaru fixture (ratio 0.67).
        With partial_min_ratio=0.8, the game should be skipped.
        """
        src_aaru = env.source / "test_ps1.aaru"
        shutil.copy2(FIXTURES / "test_ps1_zstd.aaru", src_aaru)

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            scan_all(sources, db, env.work)

            entries = _inner_hashes_from_scan(str(src_aaru), db)
            assert len(entries) == 2

            # DAT with 2 real entries + 1 fake missing entry
            dat_roms = entries + [
                {"name": "Track 99.bin", "size": 9999, "sha1": "F" * 40},
            ]
            make_dat(env.selection / "F" / "Sys.dat", "Sys", [
                {"name": "TestPS1", "roms": dat_roms},
            ])

            # ratio = 2/3 = 0.67, threshold = 0.8 → skip
            result = pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="aaru-ps1-zstd",
                partial_fallback="7z-16m",
                partial_min_ratio=0.8,
            )

            # Game skipped — no archive created
            aaru_files = list(env.romroot.rglob("*.aaru"))
            archive_files = list(env.romroot.rglob("*.7z"))
            assert aaru_files == [], "Game should be skipped, no aaru file"
            assert archive_files == [], "Game should be skipped, no fallback file"

    def test_above_ratio_uses_fallback_profile(self, env):
        """Aaru profile with ratio above threshold: fallback profile used.

        Game has 3 ROMs in DAT, 2 available (ratio 0.67).
        With partial_min_ratio=0.5, the game should be collected
        using the fallback profile (7z) instead of aaru.
        """
        src_aaru = env.source / "test_ps1.aaru"
        shutil.copy2(FIXTURES / "test_ps1_zstd.aaru", src_aaru)

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            scan_all(sources, db, env.work)

            entries = _inner_hashes_from_scan(str(src_aaru), db)
            assert len(entries) == 2

            dat_roms = entries + [
                {"name": "Track 99.bin", "size": 9999, "sha1": "F" * 40},
            ]
            make_dat(env.selection / "F" / "Sys.dat", "Sys", [
                {"name": "TestPS1", "roms": dat_roms},
            ])

            # ratio = 2/3 = 0.67, threshold = 0.5 → collect with fallback
            result = pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="aaru-ps1-zstd",
                partial_fallback="7z-16m",
                partial_min_ratio=0.5,
            )
            assert result["processed"] >= 1

            # Collected as 7z (fallback), NOT aaru
            aaru_files = list(env.romroot.rglob("*.aaru"))
            assert aaru_files == [], "Should use fallback, not aaru"

            archive = env.romroot / "F" / "Sys" / "TestPS1.7z"
            assert archive.exists(), "Fallback 7z archive should exist"

            # Sidecar records fallback profile and available tracks
            sidecar = read_sidecar(archive.parent / "TestPS1.7z.rscf")
            assert sidecar.renderer == "7z-16m"
            assert len(sidecar.files) == 2

            # Container hash valid
            assert hash_file(archive).blake3.upper() == sidecar.container_blake3.upper()
