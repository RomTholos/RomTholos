"""Orphan granularity safety — FILE_SAFETY.md § Orphan quarantine / Granularity.

Orphan detection works at the ROM level, not the game level. A DAT update
may invalidate individual tracks in any multi-ROM game. The key distinction:
a file is orphaned when NONE of its content hashes match any current DAT entry.
If even one ROM inside it is still claimed, the file stays in romroot.

This is a general problem that applies to all container types, not just
disc images.

Guarantees tested:
- One track hash changed → archive NOT orphaned (partial game)
- All track hashes changed → archive IS orphaned
- Corrected track arrives → game rebuilt with new + existing tracks
- Rebuild preserves existing valid tracks
- Single-file game: hash change → orphaned
- Single-file game: hash unchanged → not orphaned
"""

from __future__ import annotations

from pathlib import Path

import pytest

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


class TestSingleTrackInvalidated:
    """One track hash changes in a multi-ROM game → archive stays (partial)."""

    def test_two_track_game_one_changed_not_orphaned(self, env):
        """Archive with 2 tracks: one hash changes → NOT orphaned."""
        t1 = make_rom(env.source / "Track 1.bin", b"TRACK1_ORIG" * 200)
        t2 = make_rom(env.source / "Track 2.bin", b"TRACK2_SAME" * 300)
        e1 = rom_entry(t1)
        e2 = rom_entry(t2)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [e1, e2]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            archive = env.romroot / "F" / "Sys" / "Game.7z"
            assert archive.exists()
            original_blake3 = hash_file(archive).blake3

            # DAT update: Track 1 hash changes (upstream found better dump)
            make_dat(env.selection / "F" / "Sys.dat", "Sys", [
                {"name": "Game", "roms": [
                    {"name": "Track 1.bin", "size": e1["size"],
                     "sha1": "A" * 40},  # new hash — not available
                    e2,  # unchanged
                ]},
            ])

            _, q = pipeline_run_with_quarantine(
                sources, env.selection, env.romroot, env.work, db,
            )

            assert q == 0, "Archive should NOT be orphaned — Track 2 still matches"
            assert archive.exists()
            assert hash_file(archive).blake3 == original_blake3

    def test_three_track_game_two_changed_not_orphaned(self, env):
        """Archive with 3 tracks: two hashes change → still NOT orphaned.

        As long as at least one ROM inside is claimed, the file stays.
        """
        tracks = []
        entries = []
        for i in range(3):
            t = make_rom(env.source / f"Track {i}.bin", f"TRACK_{i}".encode() * 200)
            tracks.append(t)
            entries.append(rom_entry(t))

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": entries},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            archive = env.romroot / "F" / "Sys" / "Game.7z"
            assert archive.exists()

            # DAT update: Track 0 and Track 1 change, Track 2 stays
            make_dat(env.selection / "F" / "Sys.dat", "Sys", [
                {"name": "Game", "roms": [
                    {"name": "Track 0.bin", "size": entries[0]["size"],
                     "sha1": "A" * 40},
                    {"name": "Track 1.bin", "size": entries[1]["size"],
                     "sha1": "B" * 40},
                    entries[2],  # unchanged
                ]},
            ])

            _, q = pipeline_run_with_quarantine(
                sources, env.selection, env.romroot, env.work, db,
            )

            assert q == 0
            assert archive.exists()


class TestAllTracksInvalidated:
    """All track hashes change in a multi-ROM game → archive IS orphaned."""

    def test_two_tracks_both_changed_orphaned(self, env):
        """Both tracks in a 2-ROM game changed → archive orphaned."""
        t1 = make_rom(env.source / "Track 1.bin", b"ALL_T1" * 200)
        t2 = make_rom(env.source / "Track 2.bin", b"ALL_T2" * 300)
        e1 = rom_entry(t1)
        e2 = rom_entry(t2)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [e1, e2]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            archive = env.romroot / "F" / "Sys" / "Game.7z"
            original_blake3 = hash_file(archive).blake3

            # Both hashes change
            make_dat(env.selection / "F" / "Sys.dat", "Sys", [
                {"name": "Game", "roms": [
                    {"name": "Track 1.bin", "size": e1["size"],
                     "sha1": "A" * 40},
                    {"name": "Track 2.bin", "size": e2["size"],
                     "sha1": "B" * 40},
                ]},
            ])

            _, q = pipeline_run_with_quarantine(
                sources, env.selection, env.romroot, env.work, db,
            )

            assert q == 1
            assert not archive.exists()
            orphaned = env.romroot / "_orphaned" / "F" / "Sys" / "Game.7z"
            assert orphaned.exists()
            assert hash_file(orphaned).blake3 == original_blake3


class TestRebuildWithCorrectedTrack:
    """Corrected track arrives after DAT update → game rebuilt correctly."""

    def test_new_track_replaces_old_in_rebuild(self, env):
        """After DAT changes Track 1 hash and new track is available → rebuild.

        The rebuilt archive must contain the corrected Track 1 and the
        original Track 2. The old Track 1 must not remain in the archive.
        """
        # Use a subdirectory for the old source to avoid filename collisions
        old_src = env.source / "old"
        t1_old = make_rom(old_src / "Track 1.bin", b"TRACK1_OLD" * 200)
        t2 = make_rom(env.source / "Track 2.bin", b"TRACK2_KEEP" * 300)
        e1_old = rom_entry(t1_old)
        e2 = rom_entry(t2)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [e1_old, e2]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [
                SourceDir(path=old_src, source_type="readonly"),
                SourceDir(path=env.source, source_type="readonly"),
            ]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            archive = env.romroot / "F" / "Sys" / "Game.7z"
            assert archive.exists()
            sidecar1 = read_sidecar(archive.parent / "Game.7z.rscf")
            assert len(sidecar1.files) == 2

            # New corrected Track 1 arrives in a new source dir
            new_src = env.source / "new"
            t1_new = make_rom(new_src / "Track 1.bin", b"TRACK1_CORRECTED" * 200)
            e1_new = rom_entry(t1_new)

            # DAT updated with new Track 1 hash
            make_dat(env.selection / "F" / "Sys.dat", "Sys", [
                {"name": "Game", "roms": [e1_new, e2]},
            ])

            sources_with_new = [
                SourceDir(path=old_src, source_type="readonly"),
                SourceDir(path=new_src, source_type="readonly"),
                SourceDir(path=env.source, source_type="readonly"),
            ]
            result = pipeline_run(
                sources_with_new, env.selection, env.romroot, env.work, db,
            )

            assert result["processed"] >= 1  # rebuild happened

            # Spec: rebuilt archive has exactly 2 entries with correct hashes
            sidecar2 = read_sidecar(archive.parent / "Game.7z.rscf")
            assert len(sidecar2.files) == 2, (
                f"Expected 2 entries, got {len(sidecar2.files)}: "
                f"{[f.path for f in sidecar2.files]}"
            )
            rom_hashes = {
                Path(f.path).name: f.sha1.upper() for f in sidecar2.files
            }
            assert rom_hashes["Track 1.bin"] == e1_new["sha1"].upper()
            assert rom_hashes["Track 2.bin"] == e2["sha1"].upper()

    def test_rebuild_preserves_existing_valid_track(self, env):
        """During rebuild, existing valid tracks are extracted from the
        old archive — not re-fetched from source.

        We verify by removing the source for Track 2 before rebuild.
        The rebuild must still succeed using Track 2 from the existing archive.
        """
        t1_old = make_rom(env.source / "Track 1.bin", b"OLD_TRACK1" * 200)
        t2 = make_rom(env.source / "Track 2.bin", b"KEEP_TRACK2" * 300)
        e1_old = rom_entry(t1_old)
        e2 = rom_entry(t2)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [e1_old, e2]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # Remove Track 2 from source — it only exists in romroot now
            t2.unlink()

            # New Track 1 arrives in a separate dir
            new_src = env.source / "new"
            t1_new = make_rom(new_src / "Track 1.bin", b"NEW_TRACK1" * 200)
            e1_new = rom_entry(t1_new)

            make_dat(env.selection / "F" / "Sys.dat", "Sys", [
                {"name": "Game", "roms": [e1_new, e2]},
            ])

            sources_with_new = [
                SourceDir(path=env.source, source_type="readonly"),
                SourceDir(path=new_src, source_type="readonly"),
            ]
            result = pipeline_run(
                sources_with_new, env.selection, env.romroot, env.work, db,
            )

            assert result["processed"] >= 1

            # Both tracks correct in sidecar
            sidecar = read_sidecar(
                env.romroot / "F" / "Sys" / "Game.7z.rscf",
            )
            assert len(sidecar.files) == 2, (
                f"Expected 2 entries, got {len(sidecar.files)}: "
                f"{[f.path for f in sidecar.files]}"
            )
            rom_hashes = {
                Path(f.path).name: f.sha1.upper() for f in sidecar.files
            }
            assert rom_hashes["Track 1.bin"] == e1_new["sha1"].upper()
            assert rom_hashes["Track 2.bin"] == e2["sha1"].upper()


class TestSingleFileGame:
    """Single-file games: matched or orphaned, no partial state."""

    def test_hash_unchanged_not_orphaned(self, env):
        """Single-file game with unchanged hash → not orphaned."""
        rom = make_rom(env.source / "Game.gba", b"SINGLE_KEEP" * 100)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [
                {"name": "Game.gba", **entry},
            ]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # Re-run: still matched
            _, q = pipeline_run_with_quarantine(
                sources, env.selection, env.romroot, env.work, db,
            )
            assert q == 0
            assert (env.romroot / "F" / "Sys" / "Game.7z").exists()

    def test_hash_changed_orphaned(self, env):
        """Single-file game with changed hash → orphaned."""
        rom = make_rom(env.source / "Game.gba", b"SINGLE_ORPHAN" * 100)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [
                {"name": "Game.gba", **entry},
            ]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            original_blake3 = hash_file(
                env.romroot / "F" / "Sys" / "Game.7z"
            ).blake3

            # DAT update: hash changes
            make_dat(env.selection / "F" / "Sys.dat", "Sys", [
                {"name": "Game", "roms": [
                    {"name": "Game.gba", "size": entry["size"],
                     "sha1": "C" * 40},
                ]},
            ])

            _, q = pipeline_run_with_quarantine(
                sources, env.selection, env.romroot, env.work, db,
            )

            assert q == 1
            assert not (env.romroot / "F" / "Sys" / "Game.7z").exists()
            orphaned = env.romroot / "_orphaned" / "F" / "Sys" / "Game.7z"
            assert orphaned.exists()
            assert hash_file(orphaned).blake3 == original_blake3

    def test_corrected_single_file_replaces(self, env):
        """Single-file game: corrected ROM replaces old via REBUILD.

        The old ROM is discarded as part of the rebuild — no orphaning
        needed because the archive is replaced in-place. The spec says:
        "The old track is discarded only as part of the rebuild (the new
        archive is verified before the old one is deleted)."
        """
        old_src = env.source / "old"
        rom_old = make_rom(old_src / "Game.gba", b"OLD_SINGLE" * 100)
        e_old = rom_entry(rom_old)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [e_old]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=old_src, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # New corrected ROM arrives in separate dir, DAT updated
            new_src = env.source / "new"
            rom_new = make_rom(new_src / "Game.gba", b"NEW_SINGLE" * 100)
            e_new = rom_entry(rom_new)

            make_dat(env.selection / "F" / "Sys.dat", "Sys", [
                {"name": "Game", "roms": [e_new]},
            ])

            sources_both = [
                SourceDir(path=old_src, source_type="readonly"),
                SourceDir(path=new_src, source_type="readonly"),
            ]
            result, q = pipeline_run_with_quarantine(
                sources_both, env.selection, env.romroot, env.work, db,
            )

            # Rebuilt in place — old content replaced, no orphaning
            assert q == 0
            assert result["processed"] >= 1

            # Archive has exactly 1 entry with the corrected ROM hash
            archive = env.romroot / "F" / "Sys" / "Game.7z"
            assert archive.exists()
            sidecar = read_sidecar(archive.parent / "Game.7z.rscf")
            assert len(sidecar.files) == 1, (
                f"Expected 1 entry, got {len(sidecar.files)}: "
                f"{[f.path for f in sidecar.files]}"
            )
            assert sidecar.files[0].sha1.upper() == e_new["sha1"].upper()


class TestPartialGameStatusTransitions:
    """Track the game's status across DAT updates."""

    def test_complete_to_partial_to_complete(self, env):
        """Complete game → partial (track invalidated) → complete (corrected track).

        End-to-end lifecycle: no data lost at any point.
        """
        t1 = make_rom(env.source / "Track 1.bin", b"LIFECYCLE_T1" * 200)
        t2 = make_rom(env.source / "Track 2.bin", b"LIFECYCLE_T2" * 300)
        e1 = rom_entry(t1)
        e2 = rom_entry(t2)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [e1, e2]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]

            # Phase 1: Complete collection
            pipeline_run(sources, env.selection, env.romroot, env.work, db)
            archive = env.romroot / "F" / "Sys" / "Game.7z"
            assert archive.exists()

            # Phase 2: Track 1 invalidated — game becomes partial
            make_dat(env.selection / "F" / "Sys.dat", "Sys", [
                {"name": "Game", "roms": [
                    {"name": "Track 1.bin", "size": e1["size"],
                     "sha1": "D" * 40},
                    e2,
                ]},
            ])

            _, q = pipeline_run_with_quarantine(
                sources, env.selection, env.romroot, env.work, db,
            )
            assert q == 0, "Archive must NOT be orphaned — Track 2 still valid"
            assert archive.exists()

            # Phase 3: Corrected Track 1 arrives in separate dir
            fix_src = env.source / "fixed"
            t1_fixed = make_rom(
                fix_src / "Track 1.bin", b"LIFECYCLE_T1_FIXED" * 200,
            )
            e1_fixed = rom_entry(t1_fixed)

            make_dat(env.selection / "F" / "Sys.dat", "Sys", [
                {"name": "Game", "roms": [e1_fixed, e2]},
            ])

            sources_with_fix = [
                SourceDir(path=env.source, source_type="readonly"),
                SourceDir(path=fix_src, source_type="readonly"),
            ]
            result = pipeline_run(
                sources_with_fix, env.selection, env.romroot, env.work, db,
            )
            assert result["processed"] >= 1  # rebuild

            # Both tracks correct, exactly 2 entries
            sidecar = read_sidecar(archive.parent / "Game.7z.rscf")
            assert len(sidecar.files) == 2, (
                f"Expected 2 entries, got {len(sidecar.files)}: "
                f"{[f.path for f in sidecar.files]}"
            )
            hashes = {Path(f.path).name: f.sha1.upper() for f in sidecar.files}
            assert hashes["Track 1.bin"] == e1_fixed["sha1"].upper()
            assert hashes["Track 2.bin"] == e2["sha1"].upper()

    def test_partial_to_orphaned(self, env):
        """Partial game (one track matched) → all tracks change → orphaned.

        Two-step invalidation: first one track, then the remaining one.
        """
        t1 = make_rom(env.source / "Track 1.bin", b"STEP_T1" * 200)
        t2 = make_rom(env.source / "Track 2.bin", b"STEP_T2" * 300)
        e1 = rom_entry(t1)
        e2 = rom_entry(t2)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [e1, e2]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            archive = env.romroot / "F" / "Sys" / "Game.7z"
            original_blake3 = hash_file(archive).blake3

            # Step 1: Track 1 invalidated
            make_dat(env.selection / "F" / "Sys.dat", "Sys", [
                {"name": "Game", "roms": [
                    {"name": "Track 1.bin", "size": e1["size"],
                     "sha1": "E" * 40},
                    e2,
                ]},
            ])

            _, q1 = pipeline_run_with_quarantine(
                sources, env.selection, env.romroot, env.work, db,
            )
            assert q1 == 0  # not orphaned yet

            # Step 2: Track 2 also invalidated — now all hashes changed
            make_dat(env.selection / "F" / "Sys.dat", "Sys", [
                {"name": "Game", "roms": [
                    {"name": "Track 1.bin", "size": e1["size"],
                     "sha1": "E" * 40},
                    {"name": "Track 2.bin", "size": e2["size"],
                     "sha1": "F" * 40},
                ]},
            ])

            _, q2 = pipeline_run_with_quarantine(
                sources, env.selection, env.romroot, env.work, db,
            )
            assert q2 == 1
            orphaned = env.romroot / "_orphaned" / "F" / "Sys" / "Game.7z"
            assert orphaned.exists()
            assert hash_file(orphaned).blake3 == original_blake3


class TestMonolithicContainerPartialInvalidation:
    """Monolithic containers (aaru, rvz, tar.zst) on partial track invalidation.

    FILE_SAFETY.md spec: the container must be fully extracted, the invalidated
    track(s) orphaned as individual files, and the remaining valid tracks
    rebuilt into a new container (using the fallback profile if the primary
    profile requires all tracks). The old container is deleted only after the
    new one is verified on target.
    """

    def test_tar_zst_one_track_invalidated_rebuild(self, env):
        """tar.zst container: one track invalidated → extract all, rebuild.

        tar.zst is monolithic — cannot append. When one track hash changes,
        the container must be rebuilt with the valid tracks + new track.
        """
        t1 = make_rom(env.source / "Track 1.bin", b"TAR_T1_ORIG" * 200)
        t2 = make_rom(env.source / "Track 2.bin", b"TAR_T2_SAME" * 300)
        e1 = rom_entry(t1)
        e2 = rom_entry(t2)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [e1, e2]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="zstd-12",
            )

            archive = env.romroot / "F" / "Sys" / "Game.tar.zst"
            assert archive.exists()

            # Track 1 hash changes in DAT
            new_src = env.source / "new"
            t1_new = make_rom(new_src / "Track 1.bin", b"TAR_T1_NEW!" * 200)
            e1_new = rom_entry(t1_new)

            make_dat(env.selection / "F" / "Sys.dat", "Sys", [
                {"name": "Game", "roms": [e1_new, e2]},
            ])

            sources_with_new = [
                SourceDir(path=env.source, source_type="readonly"),
                SourceDir(path=new_src, source_type="readonly"),
            ]
            result = pipeline_run(
                sources_with_new, env.selection, env.romroot, env.work, db,
                compression="zstd-12",
            )
            assert result["processed"] >= 1

            # Rebuilt archive with both tracks
            rebuilt = env.romroot / "F" / "Sys" / "Game.tar.zst"
            assert rebuilt.exists()

            sidecar = read_sidecar(rebuilt.parent / "Game.tar.zst.rscf")
            assert len(sidecar.files) == 2
            rom_hashes = {
                Path(f.path).name: f.sha1.upper() for f in sidecar.files
            }
            assert rom_hashes["Track 1.bin"] == e1_new["sha1"].upper()
            assert rom_hashes["Track 2.bin"] == e2["sha1"].upper()

    def test_aaru_partial_game_uses_fallback(self, env):
        """Aaru profile: partial game (not all tracks available) → fallback.

        When the source aaru has 2 tracks but the DAT asks for those 2 plus
        a third that doesn't exist, the game is partial. Since aaru requires
        all tracks, the fallback profile is used to collect the available ones.
        """
        import shutil

        fixtures = Path(__file__).parent.parent / "fixtures"
        src_aaru = env.source / "test_ps1.aaru"
        shutil.copy2(fixtures / "test_ps1_zstd.aaru", src_aaru)

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]

            from romtholos.collect.scan import scan_all
            scan_all(sources, db, env.work)

            # Get inner content hashes
            rows = db.get_archive_contents(str(src_aaru))
            entries = [
                {
                    "name": row["entry_name"],
                    "size": row["entry_size"],
                    "sha1": row["sha1"],
                    "crc32": row["crc32"],
                    "md5": row["md5"],
                }
                for row in rows
            ]
            assert len(entries) == 2

            # DAT with 2 real entries + 1 unavailable
            dat_roms = entries + [
                {"name": "Track 99.bin", "size": 9999, "sha1": "D" * 40},
            ]
            make_dat(env.selection / "F" / "Sys.dat", "Sys", [
                {"name": "TestPS1", "roms": dat_roms},
            ])

            # Ratio = 2/3 = 0.67, threshold = 0.3 → fallback
            result = pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="aaru-ps1-zstd",
                partial_fallback="7z-16m",
                partial_min_ratio=0.3,
            )
            assert result["processed"] >= 1

            # Collected with fallback, NOT aaru
            aaru_files = list(env.romroot.rglob("*.aaru"))
            assert aaru_files == [], "Should use fallback, not aaru"

            fallback = env.romroot / "F" / "Sys" / "TestPS1.7z"
            assert fallback.exists(), "Fallback 7z should exist for partial game"

            sidecar = read_sidecar(fallback.parent / "TestPS1.7z.rscf")
            assert sidecar.renderer == "7z-16m"
            assert len(sidecar.files) == 2  # both available tracks collected
