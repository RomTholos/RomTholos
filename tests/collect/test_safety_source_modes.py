"""Source mode safety — FILE_SAFETY.md § Source Modes.

Sources are external directories that RomTholos scans for ROM files.
Regardless of mode, source files themselves are never modified or deleted
(disposal mode is the sole exception, tested separately).

Guarantees tested:
- read-only: source files untouched, no sidecars written
- ingest: source files untouched, RSCF sidecars written alongside
- disposal: same sidecar behavior as ingest (deletion tested separately)
- archives in sources: never modified after extraction
- multiple pipeline runs never accumulate changes in source dirs
- sidecar fast path: ingest/disposal second scan loads from sidecar
"""

from __future__ import annotations

from pathlib import Path

from rscf import SidecarResolver, StorageMode, hash_file, read_sidecar

from romtholos.collect.config import SourceDir
from romtholos.collect.db import CacheDB
from romtholos.collect.scan import scan_all

from .conftest import make_dat, make_rom, make_zip, pipeline_run, rom_entry


class TestReadOnlySource:
    """Read-only sources must never be touched in any way."""

    def test_source_file_unchanged(self, env):
        """ROM file content and metadata are identical after collection."""
        rom = make_rom(env.source / "Game.gba", b"SOURCE_ROM" * 100)
        original_hash = hash_file(rom).blake3
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            assert rom.exists()
            assert hash_file(rom).blake3 == original_hash

    def test_no_sidecars_written(self, env):
        """Read-only mode must not write RSCF sidecars next to source files."""
        rom = make_rom(env.source / "Game.gba", b"NO_SIDECAR" * 100)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            rscf_files = list(env.source.rglob("*.rscf"))
            assert rscf_files == [], f"Sidecars written in read-only source: {rscf_files}"

    def test_source_directory_no_new_files(self, env):
        """No files are created in a read-only source directory."""
        rom = make_rom(env.source / "Game.gba", b"DIR_CHECK" * 100)
        entry = rom_entry(rom)
        files_before = set(env.source.rglob("*"))

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            files_after = set(env.source.rglob("*"))
            new_files = files_after - files_before
            assert new_files == set(), f"New files in read-only source: {new_files}"


class TestIngestSource:
    """Ingest sources: files untouched, sidecars allowed alongside."""

    def test_source_file_unchanged(self, env):
        """ROM file content is identical after collection in ingest mode."""
        rom = make_rom(env.source / "Game.gba", b"INGEST_ROM" * 100)
        original_hash = hash_file(rom).blake3
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="ingest")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            assert rom.exists()
            assert hash_file(rom).blake3 == original_hash

    def test_source_file_never_deleted(self, env):
        """Ingest mode never deletes source files, even after collection."""
        rom = make_rom(env.source / "Game.gba", b"INGEST_KEEP" * 100)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="ingest")]
            # Run twice — source must survive both
            pipeline_run(sources, env.selection, env.romroot, env.work, db)
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            assert rom.exists()

    def test_only_rscf_sidecars_added(self, env):
        """Ingest mode may only add .rscf files, nothing else."""
        rom = make_rom(env.source / "Game.gba", b"INGEST_SIDECAR" * 100)
        entry = rom_entry(rom)
        original_files = {f.name for f in env.source.rglob("*") if f.is_file()}

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="ingest")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            new_files = {
                f.name for f in env.source.rglob("*") if f.is_file()
            } - original_files
            non_rscf = {f for f in new_files if not f.endswith(".rscf")}
            assert non_rscf == set(), f"Non-RSCF files added to ingest source: {non_rscf}"


class TestSourceArchives:
    """Source archives must never be modified after extraction."""

    def test_zip_archive_unchanged(self, env):
        """A zip archive in the source is byte-identical after collection."""
        rom_content = b"ARCHIVE_ROM" * 200
        # Create a temp file to hash, then remove it
        tmp_rom = env.source / "_tmp.gba"
        tmp_rom.write_bytes(rom_content)
        h = hash_file(tmp_rom)
        rom_size = tmp_rom.stat().st_size
        tmp_rom.unlink()

        archive = make_zip(env.source / "game.zip", {"Game.gba": rom_content})
        archive_hash = hash_file(archive).blake3

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [{
                "name": "Game.gba", "size": rom_size, "sha1": h.sha1,
            }]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            assert archive.exists()
            assert hash_file(archive).blake3 == archive_hash


class TestMultipleRuns:
    """Repeated pipeline runs must never accumulate changes in sources."""

    def test_five_runs_source_identical(self, env):
        """Five consecutive runs leave source directory unchanged."""
        rom = make_rom(env.source / "Game.gba", b"MULTI_RUN" * 100)
        entry = rom_entry(rom)
        original_hash = hash_file(rom).blake3

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]

            for i in range(5):
                pipeline_run(sources, env.selection, env.romroot, env.work, db)

                # Source unchanged after every run
                assert rom.exists(), f"Source deleted on run {i + 1}"
                assert hash_file(rom).blake3 == original_hash, (
                    f"Source modified on run {i + 1}"
                )

            # No extra files appeared
            all_files = [f for f in env.source.rglob("*") if f.is_file()]
            assert all_files == [rom]

    def test_multiple_sources_independent(self, env):
        """Two source dirs with overlapping ROMs — neither modified."""
        src2 = env.source.parent / "source2"
        src2.mkdir()

        content = b"SHARED_ROM" * 100
        rom1 = make_rom(env.source / "Game.gba", content)
        rom2 = make_rom(src2 / "Game.gba", content)
        entry = rom_entry(rom1)

        hash1 = hash_file(rom1).blake3
        hash2 = hash_file(rom2).blake3

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        with CacheDB(env.db_path) as db:
            sources = [
                SourceDir(path=env.source, source_type="readonly"),
                SourceDir(path=src2, source_type="readonly"),
            ]
            pipeline_run(sources, env.selection, env.romroot, env.work, db)

            assert hash_file(rom1).blake3 == hash1
            assert hash_file(rom2).blake3 == hash2


class TestIngestSidecars:
    """Ingest sources write RSCF sidecars alongside files."""

    def test_plain_file_sidecar_written(self, env):
        """Scanning an ingest source writes a sidecar for each plain file."""
        rom = make_rom(env.source / "Game.gba", b"SIDECAR_PLAIN" * 100)
        resolver = SidecarResolver(StorageMode.IN_TREE)
        sidecar_path = resolver.sidecar_path(rom)

        assert not sidecar_path.exists()

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="ingest")]
            scan_all(sources, db, env.work)

        assert sidecar_path.exists()
        sc = read_sidecar(sidecar_path)
        assert sc.container_blake3 == hash_file(rom).blake3

    def test_archive_sidecar_written(self, env):
        """Scanning an ingest source writes a sidecar for each archive."""
        rom_content = b"SIDECAR_ARCHIVE" * 200
        archive = make_zip(env.source / "game.zip", {"Game.gba": rom_content})
        resolver = SidecarResolver(StorageMode.IN_TREE)
        sidecar_path = resolver.sidecar_path(archive)

        assert not sidecar_path.exists()

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="ingest")]
            scan_all(sources, db, env.work)

        assert sidecar_path.exists()
        sc = read_sidecar(sidecar_path)
        assert len(sc.files) == 1
        assert sc.files[0].path == "Game.gba"

    def test_sidecar_fast_path_on_cold_start(self, env):
        """Cold start (fresh DB) loads from sidecar instead of rehashing."""
        rom = make_rom(env.source / "Game.gba", b"FAST_PATH" * 100)

        # First scan: hashes the file, writes sidecar
        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="ingest")]
            result1 = scan_all(sources, db, env.work)
            stats1 = result1[str(env.source)]
            assert stats1.files_hashed == 1
            assert stats1.files_from_sidecar == 0

        # Simulate cold start: fresh DB
        cold_db_path = env.db_path.parent / "cold.db"
        with CacheDB(cold_db_path) as db:
            sources = [SourceDir(path=env.source, source_type="ingest")]
            result2 = scan_all(sources, db, env.work)
            stats2 = result2[str(env.source)]
            assert stats2.files_hashed == 0
            assert stats2.files_from_sidecar == 1

    def test_disposal_writes_sidecars_like_ingest(self, env):
        """Disposal sources write sidecars the same as ingest."""
        rom = make_rom(env.source / "Game.gba", b"DISPOSAL_SC" * 100)
        resolver = SidecarResolver(StorageMode.IN_TREE)
        sidecar_path = resolver.sidecar_path(rom)

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="disposal")]
            scan_all(sources, db, env.work)

        assert sidecar_path.exists()

    def test_readonly_no_sidecars(self, env):
        """Read-only sources never write sidecars (same as existing test)."""
        rom = make_rom(env.source / "Game.gba", b"RO_NO_SC" * 100)

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            scan_all(sources, db, env.work)

        rscf_files = list(env.source.rglob("*.rscf"))
        assert rscf_files == []
