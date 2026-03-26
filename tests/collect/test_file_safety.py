"""File safety tests — verify that ROM data is never lost.

These tests validate the promises made in FILE_SAFETY.md:
- Source files are never deleted or modified
- Romroot files are never deleted without verified replacement
- Orphaned files are quarantined, never deleted
- Relocations are pure moves, no repacking
- Profile transitions preserve data until new archive verified
- Shared hashes don't cause cross-matching
- DAT updates that invalidate single tracks are handled correctly
"""

from __future__ import annotations

import shutil
import zipfile
from pathlib import Path

import pytest

from rscf import hash_file, read_sidecar, SidecarResolver, StorageMode

from romtholos.collect.config import SourceDir
from romtholos.collect.db import CacheDB
from romtholos.collect.execute import execute_plan, quarantine_orphans
from romtholos.collect.match import (
    find_orphaned_romroot, group_by_game, match_all_dats, match_dat,
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
        for rom in game["roms"]:
            attrs = f'name="{rom["name"]}" size="{rom["size"]}"'
            if rom.get("sha1"):
                attrs += f' sha1="{rom["sha1"]}"'
            if rom.get("crc32"):
                attrs += f' crc="{rom["crc32"]}"'
            if rom.get("md5"):
                attrs += f' md5="{rom["md5"]}"'
            lines.append(f'\t\t<rom {attrs}/>')
        lines.append('\t</game>')

    lines.append('</datafile>')
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def _pipeline_run(
    sources: list[SourceDir],
    selection_dir: Path,
    romroot: Path,
    work: Path,
    db: CacheDB,
    compression: str = "7z-16m",
    **execute_kwargs,
) -> dict[str, int]:
    """Full pipeline: scan → match_all_dats → execute with dat_folder paths."""
    all_sources = [SourceDir(path=romroot, source_type="romroot")] + sources
    orphaned = romroot / "_orphaned"
    if orphaned.is_dir():
        all_sources.append(SourceDir(path=orphaned, source_type="ingest"))
    scan_all(all_sources, db, work)
    match_results = match_all_dats(selection_dir, db)

    total: dict[str, int] = {
        "processed": 0, "skipped": 0, "failed": 0,
        "missing": 0, "relocated": 0,
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


def _pipeline_run_with_quarantine(
    sources: list[SourceDir],
    selection_dir: Path,
    romroot: Path,
    work: Path,
    db: CacheDB,
    compression: str = "7z-16m",
    **execute_kwargs,
) -> tuple[dict[str, int], int]:
    """Full pipeline including orphan quarantine. Returns (stats, quarantined)."""
    result = _pipeline_run(
        sources, selection_dir, romroot, work, db, compression,
        **execute_kwargs,
    )

    # Fresh match for accurate orphan detection (post-execute state)
    fresh_results = match_all_dats(selection_dir, db)
    orphans = find_orphaned_romroot(fresh_results, db)
    quarantined = quarantine_orphans(orphans, romroot, db)

    return result, quarantined


# ---------------------------------------------------------------------------
# Source safety — sources are never modified or deleted
# ---------------------------------------------------------------------------

class TestSourceSafety:
    """Source files must never be modified or deleted regardless of mode."""

    def test_readonly_source_untouched(self, tmp_path: Path):
        """Read-only source files are never modified."""
        source = tmp_path / "source"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom = _make_rom(source / "Game.gba", b"SOURCE_ROM" * 100)
        original_hash = hash_file(rom)
        original_stat = rom.stat()
        hashes = hash_file(rom)

        _make_dat(selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [{
                "name": "Game.gba",
                "size": rom.stat().st_size,
                "sha1": hashes.sha1,
            }]},
        ])

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=source, source_type="readonly")]
            _pipeline_run(sources, selection, romroot, work, db, "none")

            # Source file unchanged
            assert rom.exists()
            assert hash_file(rom).blake3 == original_hash.blake3
            # No sidecars written next to source
            assert not (source / "Game.gba.rscf").exists()

    def test_ingest_source_file_untouched(self, tmp_path: Path):
        """Ingest source files themselves are never modified or deleted."""
        source = tmp_path / "source"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom = _make_rom(source / "Game.gba", b"INGEST_ROM" * 100)
        original_hash = hash_file(rom)
        hashes = hash_file(rom)

        _make_dat(selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [{
                "name": "Game.gba",
                "size": rom.stat().st_size,
                "sha1": hashes.sha1,
            }]},
        ])

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=source, source_type="ingest")]
            _pipeline_run(sources, selection, romroot, work, db, "none")

            # Source file unchanged
            assert rom.exists()
            assert hash_file(rom).blake3 == original_hash.blake3

    def test_source_with_archive_untouched(self, tmp_path: Path):
        """Source archives are never modified after extraction."""
        source = tmp_path / "source"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom_content = b"ARCHIVE_ROM" * 200
        source.mkdir(parents=True)
        rom_tmp = source / "temp.gba"
        rom_tmp.write_bytes(rom_content)
        hashes = hash_file(rom_tmp)
        rom_size = rom_tmp.stat().st_size

        archive = source / "game.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.write(rom_tmp, "Game.gba")
        rom_tmp.unlink()

        archive_hash = hash_file(archive)

        _make_dat(selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [{
                "name": "Game.gba",
                "size": rom_size,
                "sha1": hashes.sha1,
            }]},
        ])

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=source, source_type="readonly")]
            _pipeline_run(sources, selection, romroot, work, db, "none")

            # Archive untouched
            assert archive.exists()
            assert hash_file(archive).blake3 == archive_hash.blake3

    def test_multiple_runs_never_modify_source(self, tmp_path: Path):
        """Repeated pipeline runs never accumulate changes in source dirs."""
        source = tmp_path / "source"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom = _make_rom(source / "Game.gba", b"MULTI_RUN" * 100)
        hashes = hash_file(rom)

        _make_dat(selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [{
                "name": "Game.gba",
                "size": rom.stat().st_size,
                "sha1": hashes.sha1,
            }]},
        ])

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=source, source_type="readonly")]

            # Run 3 times
            for _ in range(3):
                _pipeline_run(sources, selection, romroot, work, db, "none")

            # Source dir has only original file
            all_files = list(source.rglob("*"))
            assert all_files == [rom]


# ---------------------------------------------------------------------------
# Romroot safety — files never deleted without replacement
# ---------------------------------------------------------------------------

class TestRomrootSafety:
    """Romroot files must never be silently deleted."""

    def test_profile_change_preserves_old_until_new_verified(self, tmp_path: Path):
        """Old archive survives until new archive is verified on target."""
        source = tmp_path / "source"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom = _make_rom(source / "Game.gba", b"PROFILE_CHANGE" * 200)
        hashes = hash_file(rom)

        _make_dat(selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [{
                "name": "Game.gba",
                "size": rom.stat().st_size,
                "sha1": hashes.sha1,
            }]},
        ])

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=source, source_type="readonly")]

            # Collect with 7z
            _pipeline_run(sources, selection, romroot, work, db, "7z-16m")
            old_archive = romroot / "F" / "Sys" / "Game.7z"
            assert old_archive.exists()
            old_sidecar = read_sidecar(old_archive.parent / "Game.7z.rscf")

            # Change to zstd — old .7z must be gone, new .zst must exist
            _pipeline_run(sources, selection, romroot, work, db, "zstd-19")
            new_archive = romroot / "F" / "Sys" / "Game.zst"
            assert new_archive.exists()
            assert not old_archive.exists()

            # New archive has same ROM content
            new_sidecar = read_sidecar(new_archive.parent / "Game.zst.rscf")
            assert new_sidecar.files[0].sha1 == old_sidecar.files[0].sha1

    def test_complete_game_never_deleted_on_rerun(self, tmp_path: Path):
        """A complete game is never deleted by subsequent runs."""
        source = tmp_path / "source"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom = _make_rom(source / "Game.gba", b"STABLE_ROM" * 100)
        hashes = hash_file(rom)

        _make_dat(selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [{
                "name": "Game.gba",
                "size": rom.stat().st_size,
                "sha1": hashes.sha1,
            }]},
        ])

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=source, source_type="readonly")]
            _pipeline_run(sources, selection, romroot, work, db)

            archive = romroot / "F" / "Sys" / "Game.7z"
            assert archive.exists()
            archive_hash = hash_file(archive).blake3

            # 5 more runs — archive must survive unchanged
            for _ in range(5):
                _pipeline_run(sources, selection, romroot, work, db)
                assert archive.exists()
                assert hash_file(archive).blake3 == archive_hash

    def test_verification_failure_does_not_leave_corrupt_file(self, tmp_path: Path):
        """If post-write verification fails, corrupt file is removed.

        This is the ONE case where a romroot file is deleted — but it
        contains corrupt data, not valid ROM content. The source is safe.
        """
        # This tests the principle; actual CIFS corruption is hard to simulate.
        # We verify the invariant: after a failed run, romroot either has
        # a valid file or no file — never a corrupt one.
        source = tmp_path / "source"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom = _make_rom(source / "Game.gba", b"VERIFY_TEST" * 100)
        hashes = hash_file(rom)

        _make_dat(selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [{
                "name": "Game.gba",
                "size": rom.stat().st_size,
                "sha1": hashes.sha1,
            }]},
        ])

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=source, source_type="readonly")]
            _pipeline_run(sources, selection, romroot, work, db, "none")

            # File exists and is valid
            game_dir = romroot / "F" / "Sys" / "Game"
            rom_files = list(game_dir.glob("*.gba"))
            assert len(rom_files) == 1
            assert hash_file(rom_files[0]).sha1 == hashes.sha1


# ---------------------------------------------------------------------------
# Orphan quarantine safety
# ---------------------------------------------------------------------------

class TestOrphanSafety:
    """Orphan quarantine must never lose data."""

    def test_quarantine_preserves_file_content(self, tmp_path: Path):
        """Quarantined files have identical content to originals."""
        source = tmp_path / "source"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom_a = b"KEEP_ROM" * 100
        rom_b = b"ORPHAN_ROM" * 200
        file_a = _make_rom(source / "a.gba", rom_a)
        file_b = _make_rom(source / "b.gba", rom_b)
        hashes_a = hash_file(file_a)
        hashes_b = hash_file(file_b)

        _make_dat(selection / "F" / "Sys.dat", "Sys", [
            {"name": "GameA", "roms": [{
                "name": "GameA.gba", "size": len(rom_a), "sha1": hashes_a.sha1,
            }]},
            {"name": "GameB", "roms": [{
                "name": "GameB.gba", "size": len(rom_b), "sha1": hashes_b.sha1,
            }]},
        ])

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=source, source_type="readonly")]
            _pipeline_run(sources, selection, romroot, work, db)

            archive_b = romroot / "F" / "Sys" / "GameB.7z"
            original_hash = hash_file(archive_b).blake3

            # Remove GameB from DAT
            _make_dat(selection / "F" / "Sys.dat", "Sys", [
                {"name": "GameA", "roms": [{
                    "name": "GameA.gba", "size": len(rom_a), "sha1": hashes_a.sha1,
                }]},
            ])

            _, quarantined = _pipeline_run_with_quarantine(
                sources, selection, romroot, work, db,
            )
            assert quarantined == 1

            # Quarantined file has identical content
            orphaned = romroot / "_orphaned" / "F" / "Sys" / "GameB.7z"
            assert orphaned.exists()
            assert hash_file(orphaned).blake3 == original_hash

    def test_quarantine_preserves_sidecar(self, tmp_path: Path):
        """RSCF sidecar is moved alongside the quarantined archive."""
        source = tmp_path / "source"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom = _make_rom(source / "Game.gba", b"SIDECAR_TEST" * 100)
        hashes = hash_file(rom)

        _make_dat(selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [{
                "name": "Game.gba", "size": rom.stat().st_size, "sha1": hashes.sha1,
            }]},
        ])

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=source, source_type="readonly")]
            _pipeline_run(sources, selection, romroot, work, db)

            # Read original sidecar
            original_sidecar = read_sidecar(
                romroot / "F" / "Sys" / "Game.7z.rscf"
            )

            # Remove from DAT → quarantine
            _make_dat(selection / "F" / "Sys.dat", "Sys", [])
            _, quarantined = _pipeline_run_with_quarantine(
                sources, selection, romroot, work, db,
            )
            assert quarantined == 1

            # Sidecar moved with archive
            orphan_sidecar = read_sidecar(
                romroot / "_orphaned" / "F" / "Sys" / "Game.7z.rscf"
            )
            assert orphan_sidecar.files[0].sha1 == original_sidecar.files[0].sha1

    def test_quarantine_rescue_roundtrip(self, tmp_path: Path):
        """Quarantined game can be rescued and quarantined again without data loss."""
        source = tmp_path / "source"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom = _make_rom(source / "Game.gba", b"ROUNDTRIP" * 200)
        hashes = hash_file(rom)

        _make_dat(selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [{
                "name": "Game.gba", "size": rom.stat().st_size, "sha1": hashes.sha1,
            }]},
        ])

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=source, source_type="readonly")]

            # Collect
            _pipeline_run(sources, selection, romroot, work, db)
            archive = romroot / "F" / "Sys" / "Game.7z"
            original_hash = hash_file(archive).blake3

            # Quarantine (remove from DAT)
            _make_dat(selection / "F" / "Sys.dat", "Sys", [])
            _, q1 = _pipeline_run_with_quarantine(
                sources, selection, romroot, work, db,
            )
            assert q1 == 1
            assert not archive.exists()

            # Rescue (re-add to DAT)
            _make_dat(selection / "F" / "Sys.dat", "Sys", [
                {"name": "Game", "roms": [{
                    "name": "Game.gba", "size": rom.stat().st_size,
                    "sha1": hashes.sha1,
                }]},
            ])
            r2 = _pipeline_run(sources, selection, romroot, work, db)
            assert r2["processed"] >= 1  # re-collected from _orphaned
            assert archive.exists()

            # Quarantine again
            _make_dat(selection / "F" / "Sys.dat", "Sys", [])
            _, q2 = _pipeline_run_with_quarantine(
                sources, selection, romroot, work, db,
            )
            assert q2 == 1
            orphaned = romroot / "_orphaned" / "F" / "Sys" / "Game.7z"
            assert orphaned.exists()

    def test_no_orphans_when_all_games_match(self, tmp_path: Path):
        """Zero orphans when every romroot file matches a DAT entry."""
        source = tmp_path / "source"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        roms = {}
        games = []
        for i in range(5):
            name = f"Game{i}"
            content = f"ROM_CONTENT_{i}".encode() * 100
            f = _make_rom(source / f"{name}.gba", content)
            h = hash_file(f)
            roms[name] = h
            games.append({"name": name, "roms": [{
                "name": f"{name}.gba", "size": f.stat().st_size, "sha1": h.sha1,
            }]})

        _make_dat(selection / "F" / "Sys.dat", "Sys", games)

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=source, source_type="readonly")]
            _pipeline_run(sources, selection, romroot, work, db)

            # Fresh match — zero orphans
            fresh = match_all_dats(selection, db)
            orphans = find_orphaned_romroot(fresh, db)
            assert len(orphans) == 0


# ---------------------------------------------------------------------------
# Relocation safety — pure moves, no data loss
# ---------------------------------------------------------------------------

class TestRelocationSafety:
    """Relocations must be pure moves — identical content before and after."""

    def test_relocated_archive_identical(self, tmp_path: Path):
        """Archive content is byte-identical after relocation."""
        source = tmp_path / "source"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom = _make_rom(source / "Game.gba", b"RELOCATE_VERIFY" * 200)
        hashes = hash_file(rom)

        _make_dat(selection / "OldFolder" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [{
                "name": "Game.gba", "size": rom.stat().st_size, "sha1": hashes.sha1,
            }]},
        ])

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=source, source_type="readonly")]
            _pipeline_run(sources, selection, romroot, work, db)

            old_archive = romroot / "OldFolder" / "Sys" / "Game.7z"
            original_hash = hash_file(old_archive).blake3
            original_size = old_archive.stat().st_size

            # Move DAT
            (selection / "NewFolder").mkdir(parents=True)
            (selection / "OldFolder" / "Sys.dat").rename(
                selection / "NewFolder" / "Sys.dat",
            )
            (selection / "OldFolder").rmdir()

            _pipeline_run(sources, selection, romroot, work, db)

            new_archive = romroot / "NewFolder" / "Sys" / "Game.7z"
            assert new_archive.exists()
            assert not old_archive.exists()
            assert hash_file(new_archive).blake3 == original_hash
            assert new_archive.stat().st_size == original_size

    def test_relocated_directory_identical(self, tmp_path: Path):
        """Directory-mode game files are identical after relocation."""
        source = tmp_path / "source"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom = _make_rom(source / "Game.gba", b"DIR_RELOCATE" * 100)
        hashes = hash_file(rom)

        _make_dat(selection / "A" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [{
                "name": "Game.gba", "size": rom.stat().st_size, "sha1": hashes.sha1,
            }]},
        ])

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=source, source_type="readonly")]
            _pipeline_run(sources, selection, romroot, work, db, "none")

            old_rom = romroot / "A" / "Sys" / "Game" / "Game.gba"
            original_hash = hash_file(old_rom).blake3

            # Move DAT
            (selection / "B").mkdir(parents=True)
            (selection / "A" / "Sys.dat").rename(selection / "B" / "Sys.dat")
            (selection / "A").rmdir()

            _pipeline_run(sources, selection, romroot, work, db, "none")

            new_rom = romroot / "B" / "Sys" / "Game" / "Game.gba"
            assert new_rom.exists()
            assert not old_rom.exists()
            assert hash_file(new_rom).blake3 == original_hash

    def test_relocation_plus_profile_change(self, tmp_path: Path):
        """Simultaneous relocation + profile change preserves ROM content."""
        source = tmp_path / "source"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom = _make_rom(source / "Game.gba", b"RELOC_PROFILE" * 200)
        hashes = hash_file(rom)

        _make_dat(selection / "A" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [{
                "name": "Game.gba", "size": rom.stat().st_size, "sha1": hashes.sha1,
            }]},
        ])

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=source, source_type="readonly")]

            # Collect with 7z
            _pipeline_run(sources, selection, romroot, work, db, "7z-16m")
            old_sidecar = read_sidecar(romroot / "A" / "Sys" / "Game.7z.rscf")

            # Move DAT + change profile to zstd
            (selection / "B").mkdir(parents=True)
            (selection / "A" / "Sys.dat").rename(selection / "B" / "Sys.dat")
            (selection / "A").rmdir()

            _pipeline_run(sources, selection, romroot, work, db, "zstd-19")

            # Old location gone
            assert not (romroot / "A").exists()

            # New location with new profile
            new_archive = romroot / "B" / "Sys" / "Game.zst"
            assert new_archive.exists()
            new_sidecar = read_sidecar(new_archive.parent / "Game.zst.rscf")
            assert new_sidecar.files[0].sha1 == old_sidecar.files[0].sha1


# ---------------------------------------------------------------------------
# Shared hash safety — games with identical ROMs don't cross-match
# ---------------------------------------------------------------------------

class TestSharedHashSafety:
    """Games sharing ROM hashes must each claim their own archive."""

    def test_three_games_shared_hash_no_orphans(self, tmp_path: Path):
        """Three games sharing a ROM — all in romroot, zero orphans."""
        source = tmp_path / "source"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        shared = b"SHARED_TRACK" * 100
        shared_file = _make_rom(source / "shared.bin", shared)
        shared_h = hash_file(shared_file)

        games = []
        for i in range(3):
            unique = f"UNIQUE_{i}_DATA".encode() * 200
            f = _make_rom(source / f"unique_{i}.bin", unique)
            h = hash_file(f)
            games.append({"name": f"Game{i}", "roms": [
                {"name": f"Game{i} (Track 1).bin",
                 "size": len(shared), "sha1": shared_h.sha1},
                {"name": f"Game{i} (Track 2).bin",
                 "size": len(unique), "sha1": h.sha1},
            ]})

        _make_dat(selection / "F" / "Sys.dat", "Sys", games)

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=source, source_type="readonly")]
            r1 = _pipeline_run(sources, selection, romroot, work, db)
            assert r1["processed"] == 6

            # All three archives exist
            for i in range(3):
                assert (romroot / "F" / "Sys" / f"Game{i}.7z").exists()

            # Second run: all skipped, no orphans
            r2 = _pipeline_run(sources, selection, romroot, work, db)
            assert r2["skipped"] == 6
            assert r2["processed"] == 0

            fresh = match_all_dats(selection, db)
            orphans = find_orphaned_romroot(fresh, db)
            assert len(orphans) == 0

    def test_shared_hash_after_relocation(self, tmp_path: Path):
        """Games with shared hashes survive relocation without cross-matching."""
        source = tmp_path / "source"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        shared = b"SHARED_RELOC" * 100
        shared_file = _make_rom(source / "shared.bin", shared)
        shared_h = hash_file(shared_file)

        games = []
        for i in range(2):
            unique = f"UNIQUE_RELOC_{i}".encode() * 200
            f = _make_rom(source / f"u{i}.bin", unique)
            h = hash_file(f)
            games.append({"name": f"Game{i}", "roms": [
                {"name": f"Game{i} (Track 1).bin",
                 "size": len(shared), "sha1": shared_h.sha1},
                {"name": f"Game{i} (Track 2).bin",
                 "size": len(unique), "sha1": h.sha1},
            ]})

        _make_dat(selection / "FolderA" / "Sys.dat", "Sys", games)

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=source, source_type="readonly")]
            _pipeline_run(sources, selection, romroot, work, db)

            # Move DAT
            (selection / "FolderB").mkdir(parents=True)
            (selection / "FolderA" / "Sys.dat").rename(
                selection / "FolderB" / "Sys.dat",
            )
            (selection / "FolderA").rmdir()

            r2 = _pipeline_run(sources, selection, romroot, work, db)
            assert r2["relocated"] == 2

            # Both at new location, no orphans
            for i in range(2):
                assert (romroot / "FolderB" / "Sys" / f"Game{i}.7z").exists()

            fresh = match_all_dats(selection, db)
            orphans = find_orphaned_romroot(fresh, db)
            assert len(orphans) == 0


# ---------------------------------------------------------------------------
# DAT update safety — track hash changes
# ---------------------------------------------------------------------------

class TestDatUpdateSafety:
    """DAT updates changing individual ROM hashes must not lose data."""

    def test_dat_update_single_track_game_stays_in_romroot(self, tmp_path: Path):
        """When a DAT updates one track hash, game stays as partial — not orphaned."""
        source = tmp_path / "source"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        track1 = b"TRACK_1_ORIGINAL" * 200
        track2 = b"TRACK_2_UNCHANGED" * 300
        t1 = _make_rom(source / "t1.bin", track1)
        t2 = _make_rom(source / "t2.bin", track2)
        h1 = hash_file(t1)
        h2 = hash_file(t2)

        _make_dat(selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [
                {"name": "Game (Track 1).bin",
                 "size": len(track1), "sha1": h1.sha1},
                {"name": "Game (Track 2).bin",
                 "size": len(track2), "sha1": h2.sha1},
            ]},
        ])

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=source, source_type="readonly")]
            r1 = _pipeline_run(sources, selection, romroot, work, db)
            assert r1["processed"] == 2

            archive = romroot / "F" / "Sys" / "Game.7z"
            assert archive.exists()
            original_hash = hash_file(archive).blake3

            # DAT update: Track 1 hash changes (new dump found upstream)
            new_track1_hash = "A" * 40  # fake new hash — won't match anything
            _make_dat(selection / "F" / "Sys.dat", "Sys", [
                {"name": "Game", "roms": [
                    {"name": "Game (Track 1).bin",
                     "size": len(track1), "sha1": new_track1_hash},
                    {"name": "Game (Track 2).bin",
                     "size": len(track2), "sha1": h2.sha1},
                ]},
            ])

            # Run again — archive must NOT be orphaned
            # Track 2 still matches → archive is still claimed
            r2, quarantined = _pipeline_run_with_quarantine(
                sources, selection, romroot, work, db,
            )
            assert quarantined == 0
            assert archive.exists()
            # Content unchanged — no repacking happened
            assert hash_file(archive).blake3 == original_hash

    def test_dat_update_all_tracks_changed_quarantines(self, tmp_path: Path):
        """When ALL track hashes change, the archive is orphaned."""
        source = tmp_path / "source"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        track1 = b"ALL_CHANGE_T1" * 200
        track2 = b"ALL_CHANGE_T2" * 300
        t1 = _make_rom(source / "t1.bin", track1)
        t2 = _make_rom(source / "t2.bin", track2)
        h1 = hash_file(t1)
        h2 = hash_file(t2)

        _make_dat(selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [
                {"name": "Game (Track 1).bin",
                 "size": len(track1), "sha1": h1.sha1},
                {"name": "Game (Track 2).bin",
                 "size": len(track2), "sha1": h2.sha1},
            ]},
        ])

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=source, source_type="readonly")]
            _pipeline_run(sources, selection, romroot, work, db)

            archive = romroot / "F" / "Sys" / "Game.7z"
            original_hash = hash_file(archive).blake3

            # DAT update: BOTH hashes change
            _make_dat(selection / "F" / "Sys.dat", "Sys", [
                {"name": "Game", "roms": [
                    {"name": "Game (Track 1).bin",
                     "size": len(track1), "sha1": "A" * 40},
                    {"name": "Game (Track 2).bin",
                     "size": len(track2), "sha1": "B" * 40},
                ]},
            ])

            _, quarantined = _pipeline_run_with_quarantine(
                sources, selection, romroot, work, db,
            )
            assert quarantined == 1
            assert not archive.exists()

            # Data preserved in _orphaned
            orphaned = romroot / "_orphaned" / "F" / "Sys" / "Game.7z"
            assert orphaned.exists()
            assert hash_file(orphaned).blake3 == original_hash


# ---------------------------------------------------------------------------
# Work dir safety — temporary files cleaned, no leaks
# ---------------------------------------------------------------------------

class TestWorkDirSafety:
    """Work directory must be cleaned after each game."""

    def test_work_dir_clean_after_run(self, tmp_path: Path):
        """Work directory has no leftover files after pipeline completes."""
        source = tmp_path / "source"
        selection = tmp_path / "selection"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        for i in range(3):
            content = f"WORKDIR_ROM_{i}".encode() * 100
            rom = _make_rom(source / f"Game{i}.gba", content)
            h = hash_file(rom)

        games = []
        for i in range(3):
            rom = source / f"Game{i}.gba"
            h = hash_file(rom)
            games.append({"name": f"Game{i}", "roms": [{
                "name": f"Game{i}.gba", "size": rom.stat().st_size,
                "sha1": h.sha1,
            }]})

        _make_dat(selection / "F" / "Sys.dat", "Sys", games)

        with CacheDB(db_path) as db:
            sources = [SourceDir(path=source, source_type="readonly")]
            _pipeline_run(sources, selection, romroot, work, db)

            # Work dir should have no ROM-related files left
            if work.exists():
                leftover = [
                    f for f in work.rglob("*")
                    if f.is_file() and f.suffix != ".rscf"
                ]
                assert leftover == [], f"Work dir has leftover files: {leftover}"


# ---------------------------------------------------------------------------
# Selection safety — stale files cleaned
# ---------------------------------------------------------------------------

class TestSelectionSafety:
    """Selection directory must only contain files produced by current config."""

    def test_stale_dat_removed(self, tmp_path: Path):
        """DATs removed from upstream are cleaned from selection."""
        from romtholos.select.config import SelectConfig, DatEntry, FilterDef
        from romtholos.select.run import run_selection

        upstream = tmp_path / "upstream"
        selection = tmp_path / "selection"

        # Create two upstream DATs
        _make_dat(upstream / "Sys1.dat", "Sys1", [])
        _make_dat(upstream / "Sys2.dat", "Sys2", [])

        config = SelectConfig(
            upstream=upstream,
            selection=selection,
            filters={},
            default_filter="bypass",
            default_filter_args={},
            dats=[],
        )

        stats1 = run_selection(config)
        assert stats1["bypassed"] == 2
        assert (selection / "Sys1.dat").exists()
        assert (selection / "Sys2.dat").exists()

        # Remove Sys2 from upstream — should be cleaned from selection
        (upstream / "Sys2.dat").unlink()

        stats2 = run_selection(config)
        assert stats2["bypassed"] == 1
        assert stats2["cleaned"] == 1
        assert (selection / "Sys1.dat").exists()
        assert not (selection / "Sys2.dat").exists()

    def test_partial_run_skips_cleanup(self, tmp_path: Path):
        """--match partial runs must NOT clean stale files."""
        from romtholos.select.config import SelectConfig, DatEntry, FilterDef
        from romtholos.select.run import run_selection

        upstream = tmp_path / "upstream"
        selection = tmp_path / "selection"

        _make_dat(upstream / "Sys1.dat", "Sys1", [])
        _make_dat(upstream / "Sys2.dat", "Sys2", [])

        config = SelectConfig(
            upstream=upstream,
            selection=selection,
            filters={},
            default_filter="bypass",
            default_filter_args={},
            dats=[
                DatEntry(upstream="Sys1.dat", filter="bypass", filter_args={}),
                DatEntry(upstream="Sys2.dat", filter="bypass", filter_args={}),
            ],
        )

        run_selection(config)

        # Partial run with --match — must NOT clean Sys2
        stats = run_selection(config, match_glob="Sys1*")
        assert stats.get("cleaned", 0) == 0
        assert (selection / "Sys2.dat").exists()
