"""Tests for safe recursive archive extraction."""

from __future__ import annotations

import subprocess
import zipfile
from pathlib import Path

import pytest

from romtholos.collect.extract import (
    ExtractionLimits,
    ExtractedFile,
    NestingDepthError,
    PathTraversalError,
    ZipBombError,
    extract_recursive,
)


def _make_rom(path: Path, size: int = 1024) -> Path:
    """Create a fake ROM file."""
    path.write_bytes(b"\x00" * size)
    return path


class TestDirectFiles:
    def test_single_file(self, tmp_path: Path):
        rom = _make_rom(tmp_path / "game.gba")
        results = extract_recursive(rom, tmp_path / "work")
        assert len(results) == 1
        assert results[0].original_name == "game.gba"
        assert results[0].size == 1024

    def test_directory(self, tmp_path: Path):
        src = tmp_path / "input"
        src.mkdir()
        _make_rom(src / "game1.gba")
        _make_rom(src / "game2.sfc")

        results = extract_recursive(src, tmp_path / "work")
        assert len(results) == 2
        names = {r.original_name for r in results}
        assert names == {"game1.gba", "game2.sfc"}


class TestZipExtraction:
    def test_simple_zip(self, tmp_path: Path):
        src = tmp_path / "input"
        src.mkdir()
        rom = _make_rom(src / "game.gba")

        archive = tmp_path / "game.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.write(rom, "game.gba")

        results = extract_recursive(archive, tmp_path / "work")
        assert len(results) == 1
        assert results[0].original_name == "game.gba"

    def test_multi_file_zip(self, tmp_path: Path):
        src = tmp_path / "input"
        src.mkdir()
        _make_rom(src / "track01.bin", 2048)
        _make_rom(src / "game.cue", 128)

        archive = tmp_path / "game.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.write(src / "track01.bin", "track01.bin")
            zf.write(src / "game.cue", "game.cue")

        results = extract_recursive(archive, tmp_path / "work")
        assert len(results) == 2
        names = {r.original_name for r in results}
        assert names == {"track01.bin", "game.cue"}

    def test_path_traversal_rejected(self, tmp_path: Path):
        archive = tmp_path / "evil.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("../../etc/passwd", "root:x:0:0")

        with pytest.raises(PathTraversalError):
            extract_recursive(archive, tmp_path / "work")


class TestNestedArchives:
    def test_zip_in_zip(self, tmp_path: Path):
        src = tmp_path / "input"
        src.mkdir()
        _make_rom(src / "game.gba")

        inner = tmp_path / "inner.zip"
        with zipfile.ZipFile(inner, "w") as zf:
            zf.write(src / "game.gba", "game.gba")

        outer = tmp_path / "outer.zip"
        with zipfile.ZipFile(outer, "w") as zf:
            zf.write(inner, "inner.zip")

        results = extract_recursive(outer, tmp_path / "work")
        assert len(results) == 1
        assert results[0].original_name == "game.gba"

    def test_nesting_depth_limit(self, tmp_path: Path):
        src = tmp_path / "input"
        src.mkdir()
        _make_rom(src / "game.gba")

        # Create 4 levels of nesting
        current = tmp_path / "level0.zip"
        with zipfile.ZipFile(current, "w") as zf:
            zf.write(src / "game.gba", "game.gba")

        for i in range(1, 5):
            next_archive = tmp_path / f"level{i}.zip"
            with zipfile.ZipFile(next_archive, "w") as zf:
                zf.write(current, current.name)
            current = next_archive

        limits = ExtractionLimits(max_depth=2)
        with pytest.raises(NestingDepthError):
            extract_recursive(current, tmp_path / "work", limits=limits)


class TestZipBombProtection:
    def test_high_ratio_rejected(self, tmp_path: Path):
        # Create a zip with extreme compression ratio
        # 1 MiB of zeros compresses to almost nothing
        archive = tmp_path / "bomb.zip"
        with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("huge.bin", b"\x00" * (10 * 1024 * 1024))  # 10 MiB of zeros

        # Set a very low ratio limit
        limits = ExtractionLimits(max_ratio=5.0)
        with pytest.raises(ZipBombError):
            extract_recursive(archive, tmp_path / "work", limits=limits)

    def test_normal_ratio_accepted(self, tmp_path: Path):
        src = tmp_path / "input"
        src.mkdir()
        # Random-ish data that doesn't compress well
        import os
        rom = src / "game.gba"
        rom.write_bytes(os.urandom(4096))

        archive = tmp_path / "game.zip"
        with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(rom, "game.gba")

        results = extract_recursive(archive, tmp_path / "work")
        assert len(results) == 1


class Test7zExtraction:
    def test_simple_7z(self, tmp_path: Path):
        src = tmp_path / "input"
        src.mkdir()
        rom = _make_rom(src / "game.iso", 4096)

        archive = tmp_path / "game.7z"
        subprocess.run(
            ["7z", "a", str(archive), str(rom)],
            capture_output=True, check=True,
        )

        results = extract_recursive(archive, tmp_path / "work")
        assert len(results) == 1
        assert results[0].size == 4096


class TestCompressedSingleFile:
    def test_gzip(self, tmp_path: Path):
        src = tmp_path / "game.bin"
        src.write_bytes(b"\x42" * 2048)

        gz = tmp_path / "game.bin.gz"
        with open(gz, "wb") as f:
            subprocess.run(
                ["gzip", "-c", str(src)],
                stdout=f, check=True,
            )

        results = extract_recursive(gz, tmp_path / "work")
        assert len(results) == 1
        assert results[0].original_name == "game.bin"

    def test_zstd(self, tmp_path: Path):
        src = tmp_path / "game.bin"
        src.write_bytes(b"\x42" * 2048)

        zst = tmp_path / "game.bin.zst"
        subprocess.run(
            ["zstd", "-o", str(zst), str(src)],
            capture_output=True, check=True,
        )

        results = extract_recursive(zst, tmp_path / "work")
        assert len(results) == 1
        assert results[0].original_name == "game.bin"
