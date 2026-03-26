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
    SizeLimitError,
    extract_recursive,
)


def _make_rom(path: Path, size: int = 1024) -> Path:
    """Create a fake ROM file."""
    path.parent.mkdir(parents=True, exist_ok=True)
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
    """Zip extraction via 7z backend."""

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

    def test_path_traversal_contained(self, tmp_path: Path):
        """7z sanitizes path traversal entries — files stay within target."""
        archive = tmp_path / "evil.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("../../etc/passwd", "root:x:0:0")

        work = tmp_path / "work"
        results = extract_recursive(archive, work)

        # 7z strips the ../ prefix and extracts safely within target
        assert len(results) == 1
        assert results[0].path.resolve().is_relative_to(work.resolve())


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


class TestSizeLimitProtection:
    def test_size_limit_enforced(self, tmp_path: Path):
        """Archives exceeding max_total_bytes are rejected."""
        archive = tmp_path / "big.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("big.bin", b"\x42" * (2 * 1024 * 1024))  # 2 MiB

        limits = ExtractionLimits(max_total_bytes=1 * 1024 * 1024)  # 1 MiB
        with pytest.raises(SizeLimitError):
            extract_recursive(archive, tmp_path / "work", limits=limits)

    def test_within_size_limit(self, tmp_path: Path):
        """Archives within max_total_bytes succeed."""
        archive = tmp_path / "ok.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("small.bin", b"\x42" * 1024)

        limits = ExtractionLimits(max_total_bytes=1 * 1024 * 1024)
        results = extract_recursive(archive, tmp_path / "work", limits=limits)
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


class TestTarExtraction:
    def test_tar_gz(self, tmp_path: Path):
        src = tmp_path / "input"
        src.mkdir()
        _make_rom(src / "game.bin", 2048)

        archive = tmp_path / "game.tar.gz"
        subprocess.run(
            ["tar", "czf", str(archive), "-C", str(src), "game.bin"],
            check=True,
        )

        results = extract_recursive(archive, tmp_path / "work")
        assert len(results) == 1
        assert results[0].original_name == "game.bin"
        assert results[0].size == 2048

    def test_tar_zst(self, tmp_path: Path):
        src = tmp_path / "input"
        src.mkdir()
        _make_rom(src / "game.bin", 2048)

        archive = tmp_path / "game.tar.zst"
        subprocess.run(
            ["tar", "--zstd", "-cf", str(archive), "-C", str(src), "game.bin"],
            check=True,
        )

        results = extract_recursive(archive, tmp_path / "work")
        assert len(results) == 1
        assert results[0].original_name == "game.bin"

    def test_plain_tar(self, tmp_path: Path):
        src = tmp_path / "input"
        src.mkdir()
        _make_rom(src / "a.bin", 512)
        _make_rom(src / "b.bin", 512)

        archive = tmp_path / "games.tar"
        subprocess.run(
            ["tar", "cf", str(archive), "-C", str(src), "a.bin", "b.bin"],
            check=True,
        )

        results = extract_recursive(archive, tmp_path / "work")
        assert len(results) == 2
        names = {r.original_name for r in results}
        assert names == {"a.bin", "b.bin"}


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


class TestPostExtractionValidation:
    def test_size_limit_on_single_compressed(self, tmp_path: Path):
        """Single-file decompression enforces size limit mid-stream."""
        src = tmp_path / "big.bin"
        src.write_bytes(b"\x00" * (2 * 1024 * 1024))

        zst = tmp_path / "big.bin.zst"
        subprocess.run(
            ["zstd", "-o", str(zst), str(src)],
            capture_output=True, check=True,
        )

        limits = ExtractionLimits(max_total_bytes=1 * 1024 * 1024)
        with pytest.raises(SizeLimitError):
            extract_recursive(zst, tmp_path / "work", limits=limits)
