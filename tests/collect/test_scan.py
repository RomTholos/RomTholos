"""Tests for the scan phase — source type handling, RSCF loading, archive hashing."""

from __future__ import annotations

import os
import zipfile
from pathlib import Path

import pytest

from rscf import FileEntry, Sidecar, hash_file, read_sidecar, write_sidecar

from romtholos.collect.config import SourceDir
from romtholos.collect.db import CacheDB
from romtholos.collect.scan import SourceScanStats, scan_all


def _make_rom(path: Path, content: bytes = b"\x00" * 1024) -> Path:
    """Create a fake ROM file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def _make_sidecar(rom_path: Path) -> Path:
    """Hash a ROM and write an RSCF sidecar next to it."""
    hashes = hash_file(rom_path)
    stat = rom_path.stat()
    sidecar = Sidecar(
        container_blake3=hashes.blake3,
        container_size=stat.st_size,
        container_mtime_ns=stat.st_mtime_ns,
        container_ctime_ns=stat.st_ctime_ns,
        container_inode=stat.st_ino,
        renderer="",
        files=[
            FileEntry.from_hashes(
                path=rom_path.name,
                size=stat.st_size,
                hashes=hashes,
            ),
        ],
    )
    sidecar_path = rom_path.parent / (rom_path.name + ".rscf")
    write_sidecar(sidecar, sidecar_path)
    return sidecar_path


class TestRomrootScan:
    def test_loads_from_rscf_sidecar(self, tmp_path: Path):
        """Romroot scan loads hashes from sidecar without re-hashing."""
        romroot = tmp_path / "romroot" / "Test System"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom = _make_rom(romroot / "game.gba", b"ROMDATA" * 100)
        hashes = hash_file(rom)
        _make_sidecar(rom)

        sources = [SourceDir(path=tmp_path / "romroot", source_type="romroot")]
        with CacheDB(db_path) as db:
            results = scan_all(sources, db, work)
            stats = results[str(tmp_path / "romroot")]

            assert stats.files_from_sidecar == 1
            assert stats.files_hashed == 0
            assert stats.files_total == 1

            # Check DB has the file entry hashes from sidecar
            db_stats = db.stats()
            assert db_stats["scanned_files"] == 1
            assert db_stats["archive_contents"] == 1

            # Verify hashes were loaded correctly
            rows = db.find_archive_content_by_hash("sha1", hashes.sha1)
            assert len(rows) == 1
            assert rows[0]["entry_name"] == "game.gba"

    def test_corrupt_sidecar_falls_back_to_hash(self, tmp_path: Path):
        """Corrupt sidecar triggers warning and falls back to hashing."""
        romroot = tmp_path / "romroot" / "Test System"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom = _make_rom(romroot / "game.gba", b"ROMDATA" * 100)
        sidecar_path = romroot / "game.gba.rscf"
        sidecar_path.write_bytes(b"NOT_A_VALID_RSCF_FILE")

        sources = [SourceDir(path=tmp_path / "romroot", source_type="romroot")]
        with CacheDB(db_path) as db:
            results = scan_all(sources, db, work)
            stats = results[str(tmp_path / "romroot")]

            assert stats.files_hashed == 1
            assert stats.files_from_sidecar == 0
            assert len(stats.warnings) == 1
            assert "corrupt" in stats.warnings[0].lower()

    def test_orphan_sidecar_warned(self, tmp_path: Path):
        """Sidecar without source file produces a warning."""
        romroot = tmp_path / "romroot" / "Test System"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        # Create a real sidecar but no source file
        rom = _make_rom(romroot / "deleted_game.gba", b"DELETED" * 100)
        _make_sidecar(rom)
        rom.unlink()  # Delete the source

        sources = [SourceDir(path=tmp_path / "romroot", source_type="romroot")]
        with CacheDB(db_path) as db:
            results = scan_all(sources, db, work)
            stats = results[str(tmp_path / "romroot")]

            assert len(stats.warnings) == 1
            assert "orphan" in stats.warnings[0].lower()

    def test_force_rescan_rehashes(self, tmp_path: Path):
        """force_rescan=True re-hashes even when sidecar exists."""
        romroot = tmp_path / "romroot" / "Test System"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom = _make_rom(romroot / "game.gba", b"ROMDATA" * 100)
        _make_sidecar(rom)

        sources = [SourceDir(path=tmp_path / "romroot", source_type="romroot")]
        with CacheDB(db_path) as db:
            results = scan_all(sources, db, work, force_rescan=True)
            stats = results[str(tmp_path / "romroot")]

            assert stats.files_hashed == 1
            assert stats.files_from_sidecar == 0

    def test_romroot_cache_skip(self, tmp_path: Path):
        """Unchanged romroot files skip sidecar read on second scan."""
        romroot = tmp_path / "romroot" / "Test System"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom = _make_rom(romroot / "game.gba", b"ROMDATA" * 100)
        _make_sidecar(rom)

        sources = [SourceDir(path=tmp_path / "romroot", source_type="romroot")]
        with CacheDB(db_path) as db:
            # First scan — loads from sidecar
            r1 = scan_all(sources, db, work)
            assert r1[str(tmp_path / "romroot")].files_from_sidecar == 1
            assert r1[str(tmp_path / "romroot")].files_skipped == 0

            # Second scan — stat matches DB, skip entirely
            r2 = scan_all(sources, db, work)
            assert r2[str(tmp_path / "romroot")].files_skipped == 1
            assert r2[str(tmp_path / "romroot")].files_from_sidecar == 0
            assert r2[str(tmp_path / "romroot")].files_hashed == 0

    def test_no_sidecar_hashes_file(self, tmp_path: Path):
        """Romroot file without sidecar gets hashed."""
        romroot = tmp_path / "romroot" / "Test System"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom = _make_rom(romroot / "game.gba", b"ROMDATA" * 100)
        # No sidecar created

        sources = [SourceDir(path=tmp_path / "romroot", source_type="romroot")]
        with CacheDB(db_path) as db:
            results = scan_all(sources, db, work)
            stats = results[str(tmp_path / "romroot")]

            assert stats.files_hashed == 1
            assert stats.files_from_sidecar == 0


class TestIngestScan:
    def test_hashes_plain_file(self, tmp_path: Path):
        """Plain file in ingest source gets all 5 hashes."""
        ingest = tmp_path / "ingest"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom = _make_rom(ingest / "game.gba", b"TESTROM" * 100)
        expected = hash_file(rom)

        sources = [SourceDir(path=ingest, source_type="ingest")]
        with CacheDB(db_path) as db:
            results = scan_all(sources, db, work)
            stats = results[str(ingest)]

            assert stats.files_hashed == 1
            assert stats.files_total == 1

            # Verify all 5 hashes in DB
            row = db.get_scanned(str(rom))
            assert row is not None
            assert row["sha1"] == expected.sha1
            assert row["md5"] == expected.md5
            assert row["crc32"] == expected.crc32
            assert row["sha256"] == expected.sha256
            assert row["blake3"] == expected.blake3
            assert row["source_type"] == "ingest"

    def test_hashes_archive_contents(self, tmp_path: Path):
        """Archive contents are extracted, hashed with 5 types, stored in DB."""
        ingest = tmp_path / "ingest"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        # Create ROM and zip it
        rom_content = b"ARCHIVE_ROM_DATA" * 200
        ingest.mkdir(parents=True)
        raw = ingest / "temp.gba"
        raw.write_bytes(rom_content)
        rom_hashes = hash_file(raw)

        archive = ingest / "game.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.write(raw, "Cool Game (USA).gba")
        raw.unlink()

        sources = [SourceDir(path=ingest, source_type="ingest")]
        with CacheDB(db_path) as db:
            results = scan_all(sources, db, work)
            stats = results[str(ingest)]

            assert stats.archives_extracted == 1
            assert stats.archive_entries_hashed >= 1

            # Archive contents should have all 5 hashes
            rows = db.find_archive_content_by_hash("sha1", rom_hashes.sha1)
            assert len(rows) == 1
            row = rows[0]
            assert row["md5"] == rom_hashes.md5
            assert row["crc32"] == rom_hashes.crc32
            assert row["sha256"] == rom_hashes.sha256
            assert row["blake3"] == rom_hashes.blake3

    def test_nested_archive(self, tmp_path: Path):
        """Nested archive contents are recursively extracted and hashed."""
        ingest = tmp_path / "ingest"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        # Create ROM → inner zip → outer zip
        ingest.mkdir(parents=True)
        raw = ingest / "game.gba"
        raw.write_bytes(b"NESTED_ROM" * 100)
        rom_hashes = hash_file(raw)

        inner = tmp_path / "inner.zip"
        with zipfile.ZipFile(inner, "w") as zf:
            zf.write(raw, "game.gba")

        outer = ingest / "bundle.zip"
        with zipfile.ZipFile(outer, "w") as zf:
            zf.write(inner, "inner.zip")
        raw.unlink()
        inner.unlink()

        sources = [SourceDir(path=ingest, source_type="ingest")]
        with CacheDB(db_path) as db:
            results = scan_all(sources, db, work)

            # The ROM inside the nested zip should be hashed
            rows = db.find_archive_content_by_hash("sha1", rom_hashes.sha1)
            assert len(rows) == 1

    def test_archive_cache_skip(self, tmp_path: Path):
        """Unchanged archive skips re-extraction on second scan."""
        ingest = tmp_path / "ingest"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        ingest.mkdir(parents=True)
        raw = ingest / "temp.gba"
        raw.write_bytes(b"CACHED_ROM" * 100)

        archive = ingest / "game.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.write(raw, "game.gba")
        raw.unlink()

        sources = [SourceDir(path=ingest, source_type="ingest")]
        with CacheDB(db_path) as db:
            # First scan — extracts and hashes
            r1 = scan_all(sources, db, work)
            assert r1[str(ingest)].archives_extracted == 1

            # Second scan — should skip (unchanged archive)
            r2 = scan_all(sources, db, work)
            assert r2[str(ingest)].files_skipped == 1
            assert r2[str(ingest)].archives_extracted == 0

    def test_plain_file_cache_skip(self, tmp_path: Path):
        """Unchanged plain file skips re-hashing on second scan."""
        ingest = tmp_path / "ingest"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        _make_rom(ingest / "game.gba", b"STABLE_ROM" * 100)

        sources = [SourceDir(path=ingest, source_type="ingest")]
        with CacheDB(db_path) as db:
            r1 = scan_all(sources, db, work)
            assert r1[str(ingest)].files_hashed == 1

            r2 = scan_all(sources, db, work)
            assert r2[str(ingest)].files_skipped == 1
            assert r2[str(ingest)].files_hashed == 0


class TestReadonlyScan:
    def test_same_behavior_as_ingest(self, tmp_path: Path):
        """Readonly source hashes files the same as ingest."""
        source = tmp_path / "roms"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        rom = _make_rom(source / "game.gba", b"READONLY_ROM" * 100)

        sources = [SourceDir(path=source, source_type="readonly")]
        with CacheDB(db_path) as db:
            results = scan_all(sources, db, work)
            stats = results[str(source)]

            assert stats.files_hashed == 1
            row = db.get_scanned(str(rom))
            assert row is not None
            assert row["source_type"] == "readonly"


class TestScanAll:
    def test_all_source_types(self, tmp_path: Path):
        """Scan with all three source types in one call."""
        romroot = tmp_path / "romroot"
        ingest = tmp_path / "ingest"
        readonly = tmp_path / "readonly"
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        # Romroot with sidecar
        rom1 = _make_rom(romroot / "Test" / "game1.gba", b"ROM1" * 100)
        _make_sidecar(rom1)

        # Ingest
        _make_rom(ingest / "game2.gba", b"ROM2" * 100)

        # Readonly
        _make_rom(readonly / "game3.gba", b"ROM3" * 100)

        sources = [
            SourceDir(path=romroot, source_type="romroot"),
            SourceDir(path=ingest, source_type="ingest"),
            SourceDir(path=readonly, source_type="readonly"),
        ]

        with CacheDB(db_path) as db:
            results = scan_all(sources, db, work)

            assert results[str(romroot)].files_from_sidecar == 1
            assert results[str(ingest)].files_hashed == 1
            assert results[str(readonly)].files_hashed == 1

            assert db.stats()["scanned_files"] == 3

    def test_nonexistent_source_skipped(self, tmp_path: Path):
        """Non-existent source is skipped without error."""
        work = tmp_path / "work"
        db_path = tmp_path / "test.db"

        sources = [
            SourceDir(path=tmp_path / "does_not_exist", source_type="readonly"),
        ]

        with CacheDB(db_path) as db:
            results = scan_all(sources, db, work)
            assert len(results) == 0


class TestDBBatch:
    def test_is_unchanged_checks_all_fields(self, tmp_path: Path):
        """is_unchanged verifies path, size, mtime_ns, ctime_ns, inode."""
        db_path = tmp_path / "test.db"

        with CacheDB(db_path) as db:
            db.upsert_scanned(
                path="/test/file.gba",
                size=1024,
                mtime_ns=100,
                ctime_ns=200,
                inode=999,
                scanned_at="2024-01-01",
            )

            # All match
            assert db.is_unchanged("/test/file.gba", 1024, 100, 200, 999)

            # Any mismatch → changed
            assert not db.is_unchanged("/test/file.gba", 2048, 100, 200, 999)
            assert not db.is_unchanged("/test/file.gba", 1024, 101, 200, 999)
            assert not db.is_unchanged("/test/file.gba", 1024, 100, 201, 999)
            assert not db.is_unchanged("/test/file.gba", 1024, 100, 200, 998)

    def test_batch_context_manager(self, tmp_path: Path):
        """Batch context manager commits once, not per row."""
        db_path = tmp_path / "test.db"

        with CacheDB(db_path) as db:
            with db.batch():
                for i in range(100):
                    db.upsert_scanned(
                        path=f"/test/file{i}.gba",
                        size=1024,
                        mtime_ns=100,
                        ctime_ns=200,
                        inode=i,
                        scanned_at="2024-01-01",
                    )

            assert db.stats()["scanned_files"] == 100

    def test_schema_migration(self, tmp_path: Path):
        """Opening DB with stale schema recreates tables."""
        db_path = tmp_path / "test.db"

        # Create DB with old schema (fake version)
        import sqlite3
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA user_version = 1")
        conn.execute("CREATE TABLE old_table (x TEXT)")
        conn.commit()
        conn.close()

        # Opening should migrate
        with CacheDB(db_path) as db:
            # Old table should be gone, new schema in place
            stats = db.stats()
            assert stats["scanned_files"] == 0
