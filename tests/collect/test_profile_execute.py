"""Profiling tests for execute_plan performance.

Creates a synthetic library at scale (3000+ games) to identify
bottlenecks when processing fully-collected systems. Each ROM is
a unique small file (≤4K compressed, block-size optimal).

Run with:
    uv run pytest tests/collect/test_profile_execute.py -v -s
"""

from __future__ import annotations

import struct
import time
from pathlib import Path

import pytest

from rscf import FileEntry, Sidecar, SidecarResolver, StorageMode
from rscf import hash_file, write_sidecar

from romtholos.collect.compress import compress
from romtholos.collect.config import SourceDir
from romtholos.collect.db import CacheDB
from romtholos.collect.execute import execute_plan
from romtholos.collect.match import group_by_game, match_all_dats
from romtholos.collect.scan import scan_all

from .conftest import make_dat


# ---------------------------------------------------------------------------
# Synthetic library generator
# ---------------------------------------------------------------------------

GAME_COUNT = 3000
SYSTEM_NAME = "Synthetic - Cartridge"


def _unique_rom_content(index: int) -> bytes:
    """Generate unique ROM content for a given index.

    Each ROM is 256 bytes: 8-byte index prefix + 248 bytes derived pattern.
    Unique content ensures unique hashes across all games.
    """
    prefix = struct.pack(">Q", index)
    pattern = bytes((index * 7 + i) & 0xFF for i in range(248))
    return prefix + pattern


def build_synthetic_library(base: Path, game_count: int = GAME_COUNT) -> dict:
    """Build a fully-collected synthetic library.

    Creates:
    - selection/ with a DAT covering all games
    - romroot/ with compressed archives + RSCF sidecars
    - source/ as an empty disposal source
    - work/ directory
    - Populates DB cache

    Returns dict with paths and timing info.
    """
    selection = base / "selection"
    romroot = base / "romroot"
    source = base / "source"
    work = base / "work"
    db_path = base / "profile.db"

    for d in (selection, romroot, source, work):
        d.mkdir(parents=True, exist_ok=True)

    target_dir = romroot / "F" / SYSTEM_NAME
    target_dir.mkdir(parents=True, exist_ok=True)

    rom_staging = work / "_rom_staging"
    rom_staging.mkdir(parents=True, exist_ok=True)

    resolver = SidecarResolver(StorageMode.IN_TREE)

    # --- Generate ROMs, compress, write sidecars ---
    t0 = time.monotonic()

    dat_games = []
    for i in range(game_count):
        game_name = f"Game {i:04d}"
        rom_name = f"{game_name}.bin"
        content = _unique_rom_content(i)

        # Write ROM to staging
        rom_path = rom_staging / rom_name
        rom_path.write_bytes(content)
        hashes = hash_file(rom_path)

        # Compress to romroot
        archive_base = target_dir / game_name
        result = compress("zstd-19", [rom_path], archive_base)
        archive_path = result.output

        # Write RSCF sidecar
        archive_hashes = hash_file(archive_path)
        sidecar = Sidecar(
            container_blake3=archive_hashes.blake3,
            renderer="zstd-19",
            files=[
                FileEntry(
                    path=rom_name,
                    size=len(content),
                    crc32=hashes.crc32,
                    md5=hashes.md5,
                    sha1=hashes.sha1,
                    sha256=hashes.sha256,
                    blake3=hashes.blake3,
                ),
            ],
        )
        write_sidecar(sidecar, resolver.sidecar_path(archive_path))

        # Track for DAT
        dat_games.append({
            "name": game_name,
            "roms": [{
                "name": rom_name,
                "size": len(content),
                "sha1": hashes.sha1,
                "crc32": hashes.crc32,
                "md5": hashes.md5,
            }],
        })

        rom_path.unlink()

    t_gen = time.monotonic() - t0

    # --- Write DAT ---
    make_dat(selection / "F" / f"{SYSTEM_NAME}.dat", SYSTEM_NAME, dat_games)

    # --- Populate DB via scan ---
    t0 = time.monotonic()
    with CacheDB(db_path) as db:
        sources = [
            SourceDir(path=romroot, source_type="romroot"),
            SourceDir(path=source, source_type="disposal"),
        ]
        scan_all(sources, db, work)
    t_scan = time.monotonic() - t0

    return {
        "selection": selection,
        "romroot": romroot,
        "source": source,
        "work": work,
        "db_path": db_path,
        "target_dir": target_dir,
        "game_count": game_count,
        "t_generate": t_gen,
        "t_scan": t_scan,
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def library(tmp_path_factory):
    """Module-scoped synthetic library (built once, reused across tests)."""
    base = tmp_path_factory.mktemp("profile")
    lib = build_synthetic_library(base, game_count=GAME_COUNT)
    print(
        f"\n  Fixture: {lib['game_count']} games, "
        f"generate={lib['t_generate']:.1f}s, scan={lib['t_scan']:.1f}s",
    )
    return lib


# ---------------------------------------------------------------------------
# Profiling tests
# ---------------------------------------------------------------------------


class TestExecuteFullyCollected:
    """Profile execute_plan when all games are already in romroot (SKIP)."""

    def test_execute_all_skip(self, library):
        """Measure time for execute_plan when every game is SKIP.

        This is the scenario for GBA after full migration: 3000+ games,
        all complete, disposal source empty. Should be fast.
        """
        lib = library

        with CacheDB(lib["db_path"]) as db:
            # Match
            t0 = time.monotonic()
            match_results = match_all_dats(lib["selection"], db)
            t_match = time.monotonic() - t0

            assert len(match_results) == 1
            system, dat_folder, ops = match_results[0]
            game_plans = group_by_game(ops)
            assert len(game_plans) == lib["game_count"]

            # Execute
            source_modes = {
                str(lib["romroot"]): "romroot",
                str(lib["source"]): "disposal",
            }
            target_root = lib["romroot"] / dat_folder / system

            t0 = time.monotonic()
            result = execute_plan(
                game_plans=game_plans,
                romroot=target_root,
                work_dir=lib["work"],
                compression_profile="zstd-19",
                db=db,
                source_modes=source_modes,
            )
            t_execute = time.monotonic() - t0

            print(
                f"\n  Results: {result['processed']} processed, "
                f"{result['skipped']} skipped, "
                f"{result['failed']} failed, {result['missing']} missing",
            )
            print(
                f"  Timing: match={t_match:.3f}s, execute={t_execute:.3f}s",
            )

            assert result["skipped"] == lib["game_count"]
            assert result["processed"] == 0
            assert result["failed"] == 0

    def test_execute_with_disposal_source_populated(self, library):
        """Profile when disposal source has files (already-collected ROMs).

        Simulates a system where disposal source still has files that
        match romroot content — the disposal map building does DB lookups
        for every existing_op.
        """
        lib = library

        # Create some files in the disposal source (already collected in romroot)
        source = lib["source"]
        for i in range(100):
            content = _unique_rom_content(i)
            (source / f"Game {i:04d}.bin").write_bytes(content)

        with CacheDB(lib["db_path"]) as db:
            # Re-scan to pick up disposal source files
            sources = [
                SourceDir(path=lib["romroot"], source_type="romroot"),
                SourceDir(path=source, source_type="disposal"),
            ]
            scan_all(sources, db, lib["work"])

            match_results = match_all_dats(lib["selection"], db)
            system, dat_folder, ops = match_results[0]
            game_plans = group_by_game(ops)

            source_modes = {
                str(lib["romroot"]): "romroot",
                str(source): "disposal",
            }
            target_root = lib["romroot"] / dat_folder / system

            t0 = time.monotonic()
            result = execute_plan(
                game_plans=game_plans,
                romroot=target_root,
                work_dir=lib["work"],
                compression_profile="zstd-19",
                db=db,
                source_modes=source_modes,
            )
            t_execute = time.monotonic() - t0

            print(
                f"\n  With disposal source: execute={t_execute:.3f}s, "
                f"disposed={result.get('disposed', 0)}",
            )

            # Disposal source files should have been disposed
            assert result.get("disposed", 0) == 100

        # Cleanup disposal source for other tests
        for f in source.iterdir():
            f.unlink()
