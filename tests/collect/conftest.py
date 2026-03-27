"""Shared fixtures and helpers for collector safety tests.

These helpers support specification-driven tests derived from FILE_SAFETY.md.
They are intentionally decoupled from implementation details — tests should
validate the *documented behavior*, not the current code paths.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from rscf import hash_file

from romtholos.collect.config import SourceDir
from romtholos.collect.db import CacheDB
from romtholos.collect.execute import execute_plan, quarantine_orphans
from romtholos.collect.match import (
    find_orphaned_romroot,
    group_by_game,
    match_all_dats,
)
from romtholos.collect.scan import scan_all


# ---------------------------------------------------------------------------
# ROM / DAT creation helpers
# ---------------------------------------------------------------------------


def make_rom(path: Path, content: bytes = b"\x00" * 1024) -> Path:
    """Create a fake ROM file with known content."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def make_zip(archive_path: Path, files: dict[str, bytes]) -> Path:
    """Create a zip archive containing the given {name: content} entries."""
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive_path, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return archive_path


def make_dat(path: Path, system: str, games: list[dict]) -> Path:
    """Create a minimal DAT XML with pre-computed hashes.

    Each game dict has:
        name: str
        roms: list[dict] with keys: name, size, and hash fields (sha1, crc32, md5)
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        '<?xml version="1.0"?>',
        "<datafile>",
        "\t<header>",
        f"\t\t<name>{system}</name>",
        "\t</header>",
    ]

    for game in games:
        lines.append(f'\t<game name="{game["name"]}">')
        for rom in game["roms"]:
            attrs = f'name="{rom["name"]}" size="{rom["size"]}"'
            if rom.get("sha1"):
                attrs += f' sha1="{rom["sha1"]}"'
            if rom.get("crc32"):
                attrs += f' crc="{rom["crc32"]}"'
            if rom.get("md5"):
                attrs += f' md5="{rom["md5"]}"'
            lines.append(f"\t\t<rom {attrs}/>")
        lines.append("\t</game>")

    lines.append("</datafile>")
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def rom_entry(path: Path) -> dict:
    """Hash a ROM file and return a DAT-ready rom dict."""
    h = hash_file(path)
    return {
        "name": path.name,
        "size": path.stat().st_size,
        "sha1": h.sha1,
        "crc32": h.crc32,
        "md5": h.md5,
    }


# ---------------------------------------------------------------------------
# Pipeline runners
# ---------------------------------------------------------------------------


def pipeline_run(
    sources: list[SourceDir],
    selection_dir: Path,
    romroot: Path,
    work: Path,
    db: CacheDB,
    compression: str = "7z-16m",
    **execute_kwargs,
) -> dict[str, int]:
    """Full pipeline: scan → match_all_dats → execute.

    Always includes romroot as a source (for relocation/rebuild detection).
    Includes romroot/_orphaned/ as ingest source for rescue by hash.
    """
    all_sources = [SourceDir(path=romroot, source_type="romroot")] + sources
    orphaned = romroot / "_orphaned"
    if orphaned.is_dir():
        all_sources.append(SourceDir(path=orphaned, source_type="ingest"))
    scan_all(all_sources, db, work)
    match_results = match_all_dats(selection_dir, db)

    # Build source_modes map if not already provided
    if "source_modes" not in execute_kwargs:
        execute_kwargs["source_modes"] = {
            str(s.path): s.source_type for s in all_sources
        }

    total: dict[str, int] = {
        "processed": 0,
        "skipped": 0,
        "failed": 0,
        "missing": 0,
        "relocated": 0,
        "disposed": 0,
    }

    for system, dat_folder, ops in sorted(match_results):
        game_plans = group_by_game(ops)
        target_root = romroot / dat_folder / system
        result = execute_plan(
            game_plans, target_root, work, compression, db,
            **execute_kwargs,
        )
        for k in total:
            total[k] += result.get(k, 0)

    return total


def pipeline_run_with_quarantine(
    sources: list[SourceDir],
    selection_dir: Path,
    romroot: Path,
    work: Path,
    db: CacheDB,
    compression: str = "7z-16m",
    **execute_kwargs,
) -> tuple[dict[str, int], int]:
    """Full pipeline including orphan quarantine.

    Returns (stats, quarantine_count).
    """
    result = pipeline_run(
        sources, selection_dir, romroot, work, db, compression,
        **execute_kwargs,
    )

    # Fresh match for accurate orphan detection (post-execute state)
    fresh_results = match_all_dats(selection_dir, db)
    orphans = find_orphaned_romroot(fresh_results, db)
    quarantined = quarantine_orphans(orphans, romroot, db)

    return result, quarantined


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def env(tmp_path: Path):
    """Standard test environment with all directories pre-created.

    Returns a namespace with: source, selection, romroot, work, db_path.
    Usage:
        def test_something(env):
            rom = make_rom(env.source / "Game.gba", b"content")
            ...
    """

    class Env:
        source = tmp_path / "source"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

    Env.source.mkdir()
    Env.selection.mkdir()
    Env.romroot.mkdir()
    Env.work.mkdir()
    return Env
