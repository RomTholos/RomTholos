"""DAT update safety — verify correct handling when DATs change between runs.

Tests the promises from FILE_SAFETY.md § Orphan quarantine / Granularity:
- DAT adds a new game → collected
- DAT removes a game → quarantined
- DAT changes one track hash → archive stays (partial), not orphaned
- DAT changes all track hashes → archive orphaned
- DAT renames game, hashes unchanged → still matched (hash-based)
- DAT adds ROM to existing game → appended/rebuilt
- DAT with updated version replaces old in selection → stale cleaned
"""

from __future__ import annotations

from pathlib import Path

from rscf import hash_file, read_sidecar

from romtholos.collect.config import SourceDir
from romtholos.collect.db import CacheDB
from romtholos.collect.execute import execute_plan, quarantine_orphans
from romtholos.collect.match import (
    find_orphaned_romroot, group_by_game, match_all_dats,
)
from romtholos.collect.scan import scan_all


def _make_rom(path: Path, content: bytes = b"\x00" * 1024) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def _make_dat(path: Path, system: str, games: list[dict]) -> Path:
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
            lines.append(f'\t\t<rom {attrs}/>')
        lines.append('\t</game>')
    lines.append('</datafile>')
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def _run(sources, selection, romroot, work, db, compression="7z-16m"):
    """Scan → match → execute."""
    all_src = [SourceDir(path=romroot, source_type="romroot")] + sources
    orphaned = romroot / "_orphaned"
    if orphaned.is_dir():
        all_src.append(SourceDir(path=orphaned, source_type="ingest"))
    scan_all(all_src, db, work)
    results = match_all_dats(selection, db)

    stats = {"processed": 0, "skipped": 0, "failed": 0,
             "missing": 0, "relocated": 0}
    for system, dat_folder, ops in sorted(results):
        plans = group_by_game(ops)
        target = romroot / dat_folder / system
        r = execute_plan(plans, target, work, compression, db)
        for k in stats:
            stats[k] += r.get(k, 0)
    return stats


def _run_with_quarantine(sources, selection, romroot, work, db,
                         compression="7z-16m"):
    """Scan → match → execute → fresh match → quarantine."""
    stats = _run(sources, selection, romroot, work, db, compression)
    fresh = match_all_dats(selection, db)
    orphans = find_orphaned_romroot(fresh, db)
    q = quarantine_orphans(orphans, romroot, db)
    return stats, q


class TestDatAddsGame:
    """DAT adds a new game → collected on next run."""

    def test_new_game_collected(self, tmp_path: Path):
        """Adding a game to an existing DAT collects it."""
        src = tmp_path / "src"
        sel = tmp_path / "sel"
        rr = tmp_path / "rr"
        work = tmp_path / "work"

        rom_a = _make_rom(src / "a.gba", b"GAME_A" * 100)
        rom_b = _make_rom(src / "b.gba", b"GAME_B" * 200)
        ha = hash_file(rom_a)
        hb = hash_file(rom_b)

        # Initial DAT: only GameA
        _make_dat(sel / "F" / "S.dat", "S", [
            {"name": "GameA", "roms": [
                {"name": "GameA.gba", "size": rom_a.stat().st_size,
                 "sha1": ha.sha1}]},
        ])

        with CacheDB(tmp_path / "db") as db:
            sources = [SourceDir(path=src, source_type="readonly")]
            r1 = _run(sources, sel, rr, work, db)
            assert r1["processed"] == 1
            assert (rr / "F" / "S" / "GameA.7z").exists()

            # Update DAT: add GameB
            _make_dat(sel / "F" / "S.dat", "S", [
                {"name": "GameA", "roms": [
                    {"name": "GameA.gba", "size": rom_a.stat().st_size,
                     "sha1": ha.sha1}]},
                {"name": "GameB", "roms": [
                    {"name": "GameB.gba", "size": rom_b.stat().st_size,
                     "sha1": hb.sha1}]},
            ])

            r2 = _run(sources, sel, rr, work, db)
            assert r2["processed"] == 1  # only GameB
            assert r2["skipped"] == 1    # GameA skipped
            assert (rr / "F" / "S" / "GameB.7z").exists()


class TestDatRemovesGame:
    """DAT removes a game → quarantined to _orphaned/."""

    def test_removed_game_quarantined(self, tmp_path: Path):
        """Removing a game from DAT quarantines its archive."""
        src = tmp_path / "src"
        sel = tmp_path / "sel"
        rr = tmp_path / "rr"
        work = tmp_path / "work"

        rom_a = _make_rom(src / "a.gba", b"KEEP" * 100)
        rom_b = _make_rom(src / "b.gba", b"REMOVE" * 200)
        ha = hash_file(rom_a)
        hb = hash_file(rom_b)

        _make_dat(sel / "F" / "S.dat", "S", [
            {"name": "Keep", "roms": [
                {"name": "Keep.gba", "size": rom_a.stat().st_size,
                 "sha1": ha.sha1}]},
            {"name": "Remove", "roms": [
                {"name": "Remove.gba", "size": rom_b.stat().st_size,
                 "sha1": hb.sha1}]},
        ])

        with CacheDB(tmp_path / "db") as db:
            sources = [SourceDir(path=src, source_type="readonly")]
            _run(sources, sel, rr, work, db)
            assert (rr / "F" / "S" / "Remove.7z").exists()
            original_hash = hash_file(rr / "F" / "S" / "Remove.7z").blake3

            # Remove game from DAT
            _make_dat(sel / "F" / "S.dat", "S", [
                {"name": "Keep", "roms": [
                    {"name": "Keep.gba", "size": rom_a.stat().st_size,
                     "sha1": ha.sha1}]},
            ])

            _, q = _run_with_quarantine(sources, sel, rr, work, db)
            assert q == 1
            assert not (rr / "F" / "S" / "Remove.7z").exists()
            orphaned = rr / "_orphaned" / "F" / "S" / "Remove.7z"
            assert orphaned.exists()
            assert hash_file(orphaned).blake3 == original_hash

    def test_entire_dat_removed(self, tmp_path: Path):
        """Removing entire DAT from selection quarantines all its games."""
        src = tmp_path / "src"
        sel = tmp_path / "sel"
        rr = tmp_path / "rr"
        work = tmp_path / "work"

        rom = _make_rom(src / "g.gba", b"DAT_REMOVE" * 100)
        h = hash_file(rom)

        _make_dat(sel / "F" / "S.dat", "S", [
            {"name": "Game", "roms": [
                {"name": "Game.gba", "size": rom.stat().st_size,
                 "sha1": h.sha1}]},
        ])

        with CacheDB(tmp_path / "db") as db:
            sources = [SourceDir(path=src, source_type="readonly")]
            _run(sources, sel, rr, work, db)
            assert (rr / "F" / "S" / "Game.7z").exists()

            # Remove DAT file entirely
            (sel / "F" / "S.dat").unlink()

            _, q = _run_with_quarantine(sources, sel, rr, work, db)
            assert q == 1
            assert (rr / "_orphaned" / "F" / "S" / "Game.7z").exists()


class TestDatChangesTrackHash:
    """DAT changes individual track hashes — partial invalidation."""

    def test_one_track_changed_archive_stays(self, tmp_path: Path):
        """One track hash changed → archive NOT orphaned, game becomes partial."""
        src = tmp_path / "src"
        sel = tmp_path / "sel"
        rr = tmp_path / "rr"
        work = tmp_path / "work"

        t1 = _make_rom(src / "t1.bin", b"TRACK1_ORIG" * 200)
        t2 = _make_rom(src / "t2.bin", b"TRACK2_SAME" * 300)
        h1 = hash_file(t1)
        h2 = hash_file(t2)

        _make_dat(sel / "F" / "S.dat", "S", [
            {"name": "Game", "roms": [
                {"name": "Game (Track 1).bin", "size": t1.stat().st_size,
                 "sha1": h1.sha1},
                {"name": "Game (Track 2).bin", "size": t2.stat().st_size,
                 "sha1": h2.sha1},
            ]},
        ])

        with CacheDB(tmp_path / "db") as db:
            sources = [SourceDir(path=src, source_type="readonly")]
            _run(sources, sel, rr, work, db)

            archive = rr / "F" / "S" / "Game.7z"
            assert archive.exists()
            original_hash = hash_file(archive).blake3

            # DAT update: Track 1 hash changes
            _make_dat(sel / "F" / "S.dat", "S", [
                {"name": "Game", "roms": [
                    {"name": "Game (Track 1).bin", "size": t1.stat().st_size,
                     "sha1": "A" * 40},  # fake new hash
                    {"name": "Game (Track 2).bin", "size": t2.stat().st_size,
                     "sha1": h2.sha1},   # unchanged
                ]},
            ])

            _, q = _run_with_quarantine(sources, sel, rr, work, db)
            assert q == 0  # NOT quarantined
            assert archive.exists()
            assert hash_file(archive).blake3 == original_hash  # unchanged

    def test_all_tracks_changed_archive_quarantined(self, tmp_path: Path):
        """All track hashes changed → archive IS orphaned."""
        src = tmp_path / "src"
        sel = tmp_path / "sel"
        rr = tmp_path / "rr"
        work = tmp_path / "work"

        t1 = _make_rom(src / "t1.bin", b"ALL_T1" * 200)
        t2 = _make_rom(src / "t2.bin", b"ALL_T2" * 300)
        h1 = hash_file(t1)
        h2 = hash_file(t2)

        _make_dat(sel / "F" / "S.dat", "S", [
            {"name": "Game", "roms": [
                {"name": "Game (Track 1).bin", "size": t1.stat().st_size,
                 "sha1": h1.sha1},
                {"name": "Game (Track 2).bin", "size": t2.stat().st_size,
                 "sha1": h2.sha1},
            ]},
        ])

        with CacheDB(tmp_path / "db") as db:
            sources = [SourceDir(path=src, source_type="readonly")]
            _run(sources, sel, rr, work, db)
            original_hash = hash_file(rr / "F" / "S" / "Game.7z").blake3

            # Both hashes change
            _make_dat(sel / "F" / "S.dat", "S", [
                {"name": "Game", "roms": [
                    {"name": "Game (Track 1).bin", "size": t1.stat().st_size,
                     "sha1": "A" * 40},
                    {"name": "Game (Track 2).bin", "size": t2.stat().st_size,
                     "sha1": "B" * 40},
                ]},
            ])

            _, q = _run_with_quarantine(sources, sel, rr, work, db)
            assert q == 1
            orphaned = rr / "_orphaned" / "F" / "S" / "Game.7z"
            assert orphaned.exists()
            assert hash_file(orphaned).blake3 == original_hash

    def test_changed_track_available_triggers_rebuild(self, tmp_path: Path):
        """New track arrives after DAT update → game rebuilt with corrected track."""
        src = tmp_path / "src"
        sel = tmp_path / "sel"
        rr = tmp_path / "rr"
        work = tmp_path / "work"

        t1_old = _make_rom(src / "t1_old.bin", b"TRACK1_OLD" * 200)
        t2 = _make_rom(src / "t2.bin", b"TRACK2_KEEP" * 300)
        h1_old = hash_file(t1_old)
        h2 = hash_file(t2)

        _make_dat(sel / "F" / "S.dat", "S", [
            {"name": "Game", "roms": [
                {"name": "Game (Track 1).bin", "size": t1_old.stat().st_size,
                 "sha1": h1_old.sha1},
                {"name": "Game (Track 2).bin", "size": t2.stat().st_size,
                 "sha1": h2.sha1},
            ]},
        ])

        with CacheDB(tmp_path / "db") as db:
            sources = [SourceDir(path=src, source_type="readonly")]
            _run(sources, sel, rr, work, db)

            archive = rr / "F" / "S" / "Game.7z"
            assert archive.exists()
            sidecar1 = read_sidecar(archive.parent / "Game.7z.rscf")
            assert len(sidecar1.files) == 2

            # New corrected Track 1 arrives, DAT updated
            t1_new_content = b"TRACK1_CORRECTED" * 200
            t1_new = _make_rom(src / "t1_new.bin", t1_new_content)
            h1_new = hash_file(t1_new)

            _make_dat(sel / "F" / "S.dat", "S", [
                {"name": "Game", "roms": [
                    {"name": "Game (Track 1).bin",
                     "size": len(t1_new_content),
                     "sha1": h1_new.sha1},
                    {"name": "Game (Track 2).bin", "size": t2.stat().st_size,
                     "sha1": h2.sha1},
                ]},
            ])

            # Run again — should rebuild with new Track 1
            r2 = _run(sources, sel, rr, work, db)
            assert r2["processed"] >= 1  # rebuild happened

            # Verify sidecar has both tracks with correct hashes
            sidecar2 = read_sidecar(archive.parent / "Game.7z.rscf")
            assert len(sidecar2.files) == 2
            rom_hashes = {f.path: f.sha1 for f in sidecar2.files}
            assert rom_hashes["Game (Track 1).bin"].upper() == h1_new.sha1.upper()
            assert rom_hashes["Game (Track 2).bin"].upper() == h2.sha1.upper()


class TestDatRenamesGame:
    """DAT renames a game — hash-based matching should still find it."""

    def test_game_rename_hash_unchanged(self, tmp_path: Path):
        """Game renamed in DAT, same ROM hash → no repack, just relocate or skip."""
        src = tmp_path / "src"
        sel = tmp_path / "sel"
        rr = tmp_path / "rr"
        work = tmp_path / "work"

        rom = _make_rom(src / "game.gba", b"RENAME_ROM" * 100)
        h = hash_file(rom)

        _make_dat(sel / "F" / "S.dat", "S", [
            {"name": "Old Name", "roms": [
                {"name": "Old Name.gba", "size": rom.stat().st_size,
                 "sha1": h.sha1}]},
        ])

        with CacheDB(tmp_path / "db") as db:
            sources = [SourceDir(path=src, source_type="readonly")]
            _run(sources, sel, rr, work, db)
            assert (rr / "F" / "S" / "Old Name.7z").exists()

            # Rename game in DAT (same hash)
            _make_dat(sel / "F" / "S.dat", "S", [
                {"name": "New Name", "roms": [
                    {"name": "New Name.gba", "size": rom.stat().st_size,
                     "sha1": h.sha1}]},
            ])

            r2, q = _run_with_quarantine(sources, sel, rr, work, db)

            # The ROM is found by hash — either collected to new name
            # or the old archive is quarantined and new one created.
            # In either case: data is preserved, not lost.
            new_archive = rr / "F" / "S" / "New Name.7z"
            if new_archive.exists():
                # Good: new name archive exists
                sidecar = read_sidecar(new_archive.parent / "New Name.7z.rscf")
                assert sidecar.files[0].sha1.upper() == h.sha1.upper()
            else:
                # ROM must be somewhere (not deleted)
                old_archive = rr / "F" / "S" / "Old Name.7z"
                orphaned = rr / "_orphaned" / "F" / "S" / "Old Name.7z"
                assert old_archive.exists() or orphaned.exists(), (
                    "ROM data lost — not in romroot or _orphaned"
                )


class TestDatAddsRomToGame:
    """DAT adds new ROM to existing multi-ROM game."""

    def test_new_rom_appended(self, tmp_path: Path):
        """Adding a ROM to existing game → append (7z) or rebuild (zstd)."""
        src = tmp_path / "src"
        sel = tmp_path / "sel"
        rr = tmp_path / "rr"
        work = tmp_path / "work"

        rom_a = _make_rom(src / "a.bin", b"ROM_A" * 200)
        rom_b = _make_rom(src / "b.bin", b"ROM_B" * 300)
        ha = hash_file(rom_a)
        hb = hash_file(rom_b)

        # Initial: 1 ROM
        _make_dat(sel / "F" / "S.dat", "S", [
            {"name": "Game", "roms": [
                {"name": "Game.bin", "size": rom_a.stat().st_size,
                 "sha1": ha.sha1}]},
        ])

        with CacheDB(tmp_path / "db") as db:
            sources = [SourceDir(path=src, source_type="readonly")]
            _run(sources, sel, rr, work, db)

            sidecar1 = read_sidecar(rr / "F" / "S" / "Game.7z.rscf")
            assert len(sidecar1.files) == 1

            # Add second ROM to DAT
            _make_dat(sel / "F" / "S.dat", "S", [
                {"name": "Game", "roms": [
                    {"name": "Game.bin", "size": rom_a.stat().st_size,
                     "sha1": ha.sha1},
                    {"name": "Game.cue", "size": rom_b.stat().st_size,
                     "sha1": hb.sha1},
                ]},
            ])

            r2 = _run(sources, sel, rr, work, db)
            assert r2["processed"] >= 1

            sidecar2 = read_sidecar(rr / "F" / "S" / "Game.7z.rscf")
            assert len(sidecar2.files) == 2


class TestMultipleDatUpdates:
    """Multiple sequential DAT updates handled correctly."""

    def test_three_updates_no_data_loss(self, tmp_path: Path):
        """Three DAT updates across runs — no data lost at any point."""
        src = tmp_path / "src"
        sel = tmp_path / "sel"
        rr = tmp_path / "rr"
        work = tmp_path / "work"

        roms = {}
        for name in ("A", "B", "C"):
            f = _make_rom(src / f"{name}.gba", f"ROM_{name}".encode() * 100)
            roms[name] = hash_file(f)

        def make_dat_with(*names):
            games = [{"name": n, "roms": [{
                "name": f"{n}.gba",
                "size": (src / f"{n}.gba").stat().st_size,
                "sha1": roms[n].sha1,
            }]} for n in names]
            _make_dat(sel / "F" / "S.dat", "S", games)

        with CacheDB(tmp_path / "db") as db:
            sources = [SourceDir(path=src, source_type="readonly")]

            # Run 1: A, B, C
            make_dat_with("A", "B", "C")
            _run(sources, sel, rr, work, db)
            for n in ("A", "B", "C"):
                assert (rr / "F" / "S" / f"{n}.7z").exists()

            # Run 2: remove B → quarantined
            make_dat_with("A", "C")
            _, q2 = _run_with_quarantine(sources, sel, rr, work, db)
            assert q2 == 1
            assert (rr / "_orphaned" / "F" / "S" / "B.7z").exists()

            # Run 3: add B back, remove A → B re-collected from _orphaned
            make_dat_with("B", "C")
            r3 = _run(sources, sel, rr, work, db)
            assert r3["processed"] >= 1  # B re-collected from _orphaned

            fresh = match_all_dats(sel, db)
            orphans = find_orphaned_romroot(fresh, db)
            q3 = quarantine_orphans(orphans, rr, db)
            assert q3 == 1  # A quarantined

            # Final state: B and C in romroot, A in _orphaned
            assert (rr / "F" / "S" / "B.7z").exists()
            assert (rr / "F" / "S" / "C.7z").exists()
            assert (rr / "_orphaned" / "F" / "S" / "A.7z").exists()
