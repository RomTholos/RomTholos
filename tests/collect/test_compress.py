"""Tests for compression profiles."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from romtholos.collect.compress import (
    compress,
    compress_7z,
    compress_zstd,
    compress_zip,
    copy_uncompressed,
)


def _make_file(path: Path, size: int = 4096) -> Path:
    """Create a test file with pseudo-random content."""
    path.write_bytes(os.urandom(size))
    return path


class TestZstd:
    def test_single_file(self, tmp_path: Path):
        src = _make_file(tmp_path / "game.iso")
        result = compress_zstd([src], tmp_path / "game")
        assert result.output.exists()
        assert result.output.suffix == ".zst"
        assert result.original_size == 4096
        assert result.compressed_size > 0

    def test_input_preserved(self, tmp_path: Path):
        """Source file is NOT deleted after compression (no --rm)."""
        src = _make_file(tmp_path / "game.iso")
        compress_zstd([src], tmp_path / "game")
        assert src.exists()

    def test_level_affects_size(self, tmp_path: Path):
        # Compressible data
        src = tmp_path / "zeros.bin"
        src.write_bytes(b"\x00" * 100_000)

        r1 = compress_zstd([src], tmp_path / "fast", level=1)
        r19 = compress_zstd([src], tmp_path / "best", level=19)

        assert r19.compressed_size <= r1.compressed_size


class Test7z:
    def test_single_file(self, tmp_path: Path):
        src = _make_file(tmp_path / "game.iso")
        result = compress_7z([src], tmp_path / "game")
        assert result.output.exists()
        assert result.output.suffix == ".7z"
        assert result.original_size == 4096


class TestZip:
    def test_single_file(self, tmp_path: Path):
        src = _make_file(tmp_path / "game.gba")
        result = compress_zip([src], tmp_path / "game")
        assert result.output.exists()
        assert result.output.suffix == ".zip"

    def test_multi_file(self, tmp_path: Path):
        f1 = _make_file(tmp_path / "track01.bin", 2048)
        f2 = _make_file(tmp_path / "game.cue", 128)
        result = compress_zip([f1, f2], tmp_path / "game")
        assert result.output.exists()
        assert result.original_size == 2048 + 128


class TestUncompressed:
    def test_single_file(self, tmp_path: Path):
        src = _make_file(tmp_path / "game.gba")
        result = copy_uncompressed([src], tmp_path / "output" / "game")
        assert result.output.is_file()
        assert result.output.name == "game.gba"
        assert result.ratio == 1.0

    def test_preserves_extension(self, tmp_path: Path):
        src = _make_file(tmp_path / "rom.lyx")
        result = copy_uncompressed([src], tmp_path / "out" / "Cool Game")
        assert result.output.suffix == ".lyx"
        assert result.output.name == "Cool Game.lyx"

    def test_multi_file_asserts(self, tmp_path: Path):
        f1 = _make_file(tmp_path / "track.bin")
        f2 = _make_file(tmp_path / "game.cue")
        with pytest.raises(AssertionError, match="single file"):
            copy_uncompressed([f1, f2], tmp_path / "output")


class TestProfileRegistry:
    def test_zstd_19(self, tmp_path: Path):
        src = _make_file(tmp_path / "game.iso")
        result = compress("zstd-19", [src], tmp_path / "game")
        assert result.output.exists()
        assert result.output.is_file()

    def test_none(self, tmp_path: Path):
        src = _make_file(tmp_path / "game.gba")
        result = compress("none", [src], tmp_path / "output")
        assert result.output.is_file()
        assert result.output.suffix == ".gba"

    def test_unknown_profile_raises(self, tmp_path: Path):
        src = _make_file(tmp_path / "game.gba")
        with pytest.raises(Exception, match="Unknown profile"):
            compress("nonexistent", [src], tmp_path / "game")
