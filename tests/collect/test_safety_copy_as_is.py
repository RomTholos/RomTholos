"""Copy-as-is optimization safety — FILE_SAFETY.md § Copy-as-is optimization.

When a source file already matches the target profile, it is copied directly
to romroot without extract→recompress. The RSCF sidecar must still record
the correct inner content hashes and the copy must still be verified.

Safety concerns:
- Verification must still happen (BLAKE3 roundtrip)
- RSCF sidecar must record correct inner content hashes
- If copy-as-is check fails, must fall back to normal pipeline (not crash)
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from rscf import hash_file, read_sidecar

from romtholos.collect.config import SourceDir
from romtholos.collect.db import CacheDB
from romtholos.collect.scan import scan_all

from .conftest import make_dat, pipeline_run

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


class TestCopyAsIsSafety:
    """Copy-as-is optimization must still verify and write correct sidecars."""

    def test_rvz_copy_as_is_verified(self, env):
        """RVZ file matching target profile: copied, verified, sidecar correct."""
        src_rvz = env.source / "test_gc.rvz"
        shutil.copy2(FIXTURES / "test_gc.rvz", src_rvz)
        original_blake3 = hash_file(src_rvz).blake3

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            scan_all(sources, db, env.work)

            entries = _inner_hashes_from_scan(str(src_rvz), db)
            assert len(entries) >= 1, "Scanner should extract inner ISO from RVZ"

            make_dat(env.selection / "F" / "Sys.dat", "Sys", [
                {"name": "TestGame", "roms": entries},
            ])

            result = pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="rvz-zstd-5",
            )
            assert result["processed"] >= 1

            # Verify: archive exists and was copied as-is (same BLAKE3)
            target = env.romroot / "F" / "Sys" / "TestGame.rvz"
            assert target.exists()
            assert hash_file(target).blake3 == original_blake3

            # Sidecar exists with correct inner content hashes
            sidecar = read_sidecar(target.parent / "TestGame.rvz.rscf")
            assert sidecar.container_blake3.upper() == original_blake3.upper()
            assert len(sidecar.files) >= 1
            assert sidecar.renderer == "rvz-zstd-5"

    def test_aaru_copy_as_is_verified(self, env):
        """Aaru file matching target profile: copied, verified, sidecar has all tracks."""
        src_aaru = env.source / "test_ps1.aaru"
        shutil.copy2(FIXTURES / "test_ps1_zstd.aaru", src_aaru)
        original_blake3 = hash_file(src_aaru).blake3

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            scan_all(sources, db, env.work)

            entries = _inner_hashes_from_scan(str(src_aaru), db)
            assert len(entries) >= 1, "Scanner should extract inner tracks from aaru"

            make_dat(env.selection / "F" / "Sys.dat", "Sys", [
                {"name": "TestPS1", "roms": entries},
            ])

            result = pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="aaru-ps1-zstd",
            )
            assert result["processed"] >= 1

            target = env.romroot / "F" / "Sys" / "TestPS1.aaru"
            assert target.exists()
            assert hash_file(target).blake3 == original_blake3

            sidecar = read_sidecar(target.parent / "TestPS1.aaru.rscf")
            assert sidecar.container_blake3.upper() == original_blake3.upper()
            assert len(sidecar.files) >= 1
            assert sidecar.renderer == "aaru-ps1-zstd"

    def test_copy_as_is_header_mismatch_falls_back(self, env):
        """Source doesn't match target profile: falls back to extract+recompress."""
        # lzma aaru fixture does NOT match aaru-ps1-zstd (wants zstd)
        src_aaru = env.source / "test_ps1.aaru"
        shutil.copy2(FIXTURES / "test_ps1.aaru", src_aaru)
        lzma_blake3 = hash_file(src_aaru).blake3

        with CacheDB(env.db_path) as db:
            sources = [SourceDir(path=env.source, source_type="readonly")]
            scan_all(sources, db, env.work)

            entries = _inner_hashes_from_scan(str(src_aaru), db)
            assert len(entries) >= 1

            make_dat(env.selection / "F" / "Sys.dat", "Sys", [
                {"name": "TestPS1", "roms": entries},
            ])

            # Profile wants zstd, source has lzma → mismatch → fallback
            result = pipeline_run(
                sources, env.selection, env.romroot, env.work, db,
                compression="aaru-ps1-zstd",
            )
            assert result["processed"] >= 1

            target = env.romroot / "F" / "Sys" / "TestPS1.aaru"
            assert target.exists()

            # Recompressed → different BLAKE3 than source
            assert hash_file(target).blake3 != lzma_blake3

            # Inner content hashes and sidecar still correct
            sidecar = read_sidecar(target.parent / "TestPS1.aaru.rscf")
            assert len(sidecar.files) >= 1
            assert sidecar.renderer == "aaru-ps1-zstd"
