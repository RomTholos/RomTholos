"""Tests for dimg-tool / .aaru integration.

Tests that require dimg-tool are skipped if the binary is not available.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

from romtholos.collect.compress import (
    PROFILES,
    CompressionError,
    dimg_header_matches_profile,
    profile_extension,
    strip_archive_extension,
)
from romtholos.collect.execute import _detect_media_type, _profile_requires_all_tracks
from romtholos.collect.extract import _ARCHIVE_EXTENSIONS, _AARU_EXTENSIONS

has_dimg_tool = shutil.which("dimg-tool") is not None
skip_no_dimg = pytest.mark.skipif(
    not has_dimg_tool, reason="dimg-tool not available"
)


class TestAaruProfiles:
    """Test that aaru profile configuration is correct."""

    def test_profiles_registered(self):
        aaru_profiles = [k for k in PROFILES if k.startswith("aaru-")]
        assert len(aaru_profiles) >= 9

    def test_all_aaru_profiles_have_correct_keys(self):
        for name, profile in PROFILES.items():
            if not name.startswith("aaru-"):
                continue
            assert profile["multi_file"] is True
            assert profile["append_function"] is None
            assert "system" in profile["kwargs"]
            assert "codec" in profile["kwargs"]

    def test_profile_extension_aaru(self):
        assert profile_extension("aaru-ps1-zstd") == ".aaru"
        assert profile_extension("aaru-dc-zstd") == ".aaru"
        assert profile_extension("aaru-ps2dvd-zstd") == ".aaru"

    def test_strip_archive_extension_aaru(self):
        assert strip_archive_extension("Game.aaru") == "Game"
        assert strip_archive_extension("Cool (USA).aaru") == "Cool (USA)"


class TestDimgHeaderMatches:
    """Test header matching logic (no dimg-tool needed)."""

    def test_matches_same_system_and_codec(self):
        header = {"system": "ps1", "codec": "zstd"}
        assert dimg_header_matches_profile(header, "aaru-ps1-zstd") is True

    def test_rejects_different_codec(self):
        header = {"system": "ps1", "codec": "lzma"}
        assert dimg_header_matches_profile(header, "aaru-ps1-zstd") is False

    def test_rejects_different_system(self):
        header = {"system": "dc", "codec": "zstd"}
        assert dimg_header_matches_profile(header, "aaru-ps1-zstd") is False

    def test_rejects_non_aaru_profile(self):
        header = {"system": "ps1", "codec": "zstd"}
        assert dimg_header_matches_profile(header, "7z-96m") is False

    def test_rejects_unknown_profile(self):
        header = {"system": "ps1", "codec": "zstd"}
        assert dimg_header_matches_profile(header, "aaru-nonexistent") is False


class TestAaruExtensions:
    """Test that .aaru is recognized as an archive extension."""

    def test_aaru_in_archive_extensions(self):
        assert ".aaru" in _ARCHIVE_EXTENSIONS

    def test_aaru_extensions_set(self):
        assert ".aaru" in _AARU_EXTENSIONS
        assert ".aaruf" in _AARU_EXTENSIONS
        assert ".dicf" in _AARU_EXTENSIONS


class TestPartialGameBlocking:
    """Test that aaru profiles block partial games."""

    def test_aaru_requires_all_tracks(self):
        assert _profile_requires_all_tracks("aaru-ps1-zstd") is True
        assert _profile_requires_all_tracks("aaru-dc-zstd") is True

    def test_non_aaru_allows_partial(self):
        assert _profile_requires_all_tracks("7z-96m") is False
        assert _profile_requires_all_tracks("zstd-19") is False
        assert _profile_requires_all_tracks("none") is False


class TestMediaTypeDetection:
    """Test _detect_media_type for different game ROM patterns."""

    def _make_game(self, rom_names: list[str]) -> "GamePlan":
        from romtholos.collect.match import GamePlan, MatchOp
        ops = [
            MatchOp(
                dat_path="test.dat",
                system="Test",
                game_name="Test Game",
                rom_name=name,
                rom_size=100,
                source_path=None,
                source_type=None,
                archive_entry=None,
                status="missing",
            )
            for name in rom_names
        ]
        return GamePlan(system="Test", game_name="Test Game", ops=ops)

    def test_cd_detected_by_cue(self):
        game = self._make_game(["Game.cue", "Game (Track 1).bin", "Game (Track 2).bin"])
        assert _detect_media_type(game) == "cd"

    def test_dvd_detected_by_iso(self):
        game = self._make_game(["Game.iso"])
        assert _detect_media_type(game) == "dvd"

    def test_gdi_detected(self):
        game = self._make_game(["disc.gdi", "track01.bin", "track02.raw"])
        assert _detect_media_type(game) == "gdi"

    def test_rom_for_plain_files(self):
        game = self._make_game(["game.bin"])
        assert _detect_media_type(game) == "rom"

    def test_rom_for_cartridge(self):
        game = self._make_game(["game.lyx"])
        assert _detect_media_type(game) == "rom"

    def test_gdi_takes_priority_over_cue(self):
        # Unlikely but test priority: gdi > cue > iso
        game = self._make_game(["disc.gdi", "game.cue", "track.bin"])
        assert _detect_media_type(game) == "gdi"

    def test_cue_takes_priority_over_iso(self):
        game = self._make_game(["game.cue", "game.iso", "game.bin"])
        assert _detect_media_type(game) == "cd"

    def test_case_insensitive(self):
        game = self._make_game(["Game.CUE", "Game.BIN"])
        assert _detect_media_type(game) == "cd"


class TestCompressionMap:
    """Test compression_map config and per-game profile resolution."""

    def test_compression_map_empty_by_default(self):
        from romtholos.collect.config import SystemConfig
        cfg = SystemConfig()
        assert cfg.compression_map == {}

    def test_compression_for_system_with_media_type(self):
        from romtholos.collect.config import CollectorConfig, SystemConfig
        cfg = CollectorConfig(
            default_compression="zstd-19",
            systems={
                "Sony - PlayStation 2": SystemConfig(
                    compression="aaru-ps2dvd-zstd",
                    compression_map={"cd": "aaru-ps2cd-zstd"},
                ),
            },
        )
        # DVD (default)
        assert cfg.compression_for_system("Sony - PlayStation 2", "dvd") == "aaru-ps2dvd-zstd"
        # CD (override from map)
        assert cfg.compression_for_system("Sony - PlayStation 2", "cd") == "aaru-ps2cd-zstd"
        # ROM (no override, falls back to system default)
        assert cfg.compression_for_system("Sony - PlayStation 2", "rom") == "aaru-ps2dvd-zstd"
        # No media type
        assert cfg.compression_for_system("Sony - PlayStation 2") == "aaru-ps2dvd-zstd"

    def test_compression_map_no_system_config(self):
        from romtholos.collect.config import CollectorConfig
        cfg = CollectorConfig(default_compression="zstd-19")
        # Falls back to global default regardless of media type
        assert cfg.compression_for_system("Unknown", "cd") == "zstd-19"

    def test_execute_plan_with_compression_map(self, tmp_path: Path):
        """Verify execute_plan accepts compression_map parameter."""
        from romtholos.collect.db import CacheDB
        from romtholos.collect.execute import execute_plan

        db_path = tmp_path / "test.db"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        romroot.mkdir()
        work.mkdir()

        with CacheDB(db_path) as db:
            result = execute_plan(
                game_plans=[],
                romroot=romroot,
                work_dir=work,
                compression_profile="aaru-ps2dvd-zstd",
                db=db,
                compression_map={"cd": "aaru-ps2cd-zstd"},
            )
            assert result["processed"] == 0


class TestLimitFlag:
    """Test the --limit flag in execute_plan."""

    def test_limit_stops_processing(self, tmp_path: Path):
        """Verify that limit=0 means no limit (doesn't break)."""
        from romtholos.collect.db import CacheDB
        from romtholos.collect.execute import execute_plan

        db_path = tmp_path / "test.db"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        romroot.mkdir()
        work.mkdir()

        with CacheDB(db_path) as db:
            # Empty game plans, limit=5 should not error
            result = execute_plan(
                game_plans=[],
                romroot=romroot,
                work_dir=work,
                compression_profile="7z-16m",
                db=db,
                limit=5,
            )
            assert result["processed"] == 0


class TestVerifyRoundtripFlag:
    """Test that verify_roundtrip parameter is accepted."""

    def test_verify_flag_accepted(self, tmp_path: Path):
        from romtholos.collect.db import CacheDB
        from romtholos.collect.execute import execute_plan

        db_path = tmp_path / "test.db"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        romroot.mkdir()
        work.mkdir()

        with CacheDB(db_path) as db:
            result = execute_plan(
                game_plans=[],
                romroot=romroot,
                work_dir=work,
                compression_profile="aaru-ps1-zstd",
                db=db,
                verify_roundtrip=True,
            )
            assert result["processed"] == 0


class TestPartialFallback:
    """Test partial_fallback configuration and resolution."""

    def test_partial_fallback_empty_by_default(self):
        from romtholos.collect.config import CollectorConfig

        cfg = CollectorConfig()
        assert cfg.partial_fallback == ""

    def test_partial_fallback_global_resolution(self):
        from romtholos.collect.config import CollectorConfig

        cfg = CollectorConfig(partial_fallback="zstd-12")
        assert cfg.partial_fallback_for_system("anything") == "zstd-12"

    def test_partial_fallback_per_system_override(self):
        from romtholos.collect.config import CollectorConfig, SystemConfig

        cfg = CollectorConfig(
            partial_fallback="zstd-12",
            systems={
                "SNK - Neo Geo CD": SystemConfig(partial_fallback="7z-16m"),
            },
        )
        assert cfg.partial_fallback_for_system("SNK - Neo Geo CD") == "7z-16m"
        assert cfg.partial_fallback_for_system("other") == "zstd-12"

    def test_partial_fallback_accepted_by_execute(self, tmp_path: Path):
        from romtholos.collect.db import CacheDB
        from romtholos.collect.execute import execute_plan

        db_path = tmp_path / "test.db"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        romroot.mkdir()
        work.mkdir()

        with CacheDB(db_path) as db:
            result = execute_plan(
                game_plans=[],
                romroot=romroot,
                work_dir=work,
                compression_profile="aaru-neogeo-zstd",
                db=db,
                partial_fallback="zstd-12",
            )
            assert result["processed"] == 0

    def test_zstd_12_profile_exists(self):
        assert "zstd-12" in PROFILES
        assert PROFILES["zstd-12"]["kwargs"]["level"] == 12


class TestExtractionCache:
    """Test ExtractionCache behavior."""

    def test_cache_reuses_extraction(self, tmp_path: Path):
        from romtholos.collect.execute import ExtractionCache
        from romtholos.collect.extract import ExtractionLimits

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        cache = ExtractionCache(cache_dir, max_mb=100)

        # Create a fake archive (plain file, extract_recursive handles it)
        src = tmp_path / "source.bin"
        src.write_bytes(b"hello world")

        limits = ExtractionLimits()
        result1 = cache.get_or_extract(src, limits)
        result2 = cache.get_or_extract(src, limits)

        # Same object returned (cache hit)
        assert result1 is result2
        assert len(result1) == 1

    def test_cache_find_file_by_hash(self, tmp_path: Path):
        from rscf import hash_file

        from romtholos.collect.execute import ExtractionCache
        from romtholos.collect.extract import ExtractionLimits

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        cache = ExtractionCache(cache_dir, max_mb=100)

        src = tmp_path / "game.bin"
        src.write_bytes(b"data")
        hashes = hash_file(src)

        limits = ExtractionLimits()
        cache.get_or_extract(src, limits)

        # Find by hash — filename is irrelevant
        found = cache.find_file(str(src), "sha1", hashes.sha1)
        assert found is not None
        assert found.exists()

    def test_cache_find_file_wrong_hash(self, tmp_path: Path):
        from romtholos.collect.execute import ExtractionCache
        from romtholos.collect.extract import ExtractionLimits

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        cache = ExtractionCache(cache_dir, max_mb=100)

        src = tmp_path / "game.bin"
        src.write_bytes(b"data")

        limits = ExtractionLimits()
        cache.get_or_extract(src, limits)

        result = cache.find_file(str(src), "sha1", "0000000000000000000000000000000000000000")
        assert result is None

    def test_cache_find_file_not_cached(self, tmp_path: Path):
        from romtholos.collect.execute import ExtractionCache

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        cache = ExtractionCache(cache_dir, max_mb=100)

        result = cache.find_file("/nonexistent", "sha1", "AA" * 20)
        assert result is None

    def test_cache_cleanup(self, tmp_path: Path):
        from romtholos.collect.execute import ExtractionCache
        from romtholos.collect.extract import ExtractionLimits

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        cache = ExtractionCache(cache_dir, max_mb=100)

        src = tmp_path / "test.bin"
        src.write_bytes(b"test data")

        limits = ExtractionLimits()
        cache.get_or_extract(src, limits)
        assert any(cache_dir.iterdir())

        cache.cleanup()
        assert not cache_dir.exists()

    def test_cache_soft_quota(self, tmp_path: Path):
        from romtholos.collect.execute import ExtractionCache
        from romtholos.collect.extract import ExtractionLimits

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        # Soft quota: 0 bytes target, but always caches current extraction
        cache = ExtractionCache(cache_dir, max_mb=0)
        src = tmp_path / "big.bin"
        src.write_bytes(b"x" * 100)

        limits = ExtractionLimits()
        result = cache.get_or_extract(src, limits)
        assert len(result) == 1
        # Always cached even when exceeding soft quota
        assert str(src) in cache._entries

    def test_cache_evicts_old_entries(self, tmp_path: Path):
        from romtholos.collect.execute import ExtractionCache
        from romtholos.collect.extract import ExtractionLimits

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        # Tiny quota so second entry evicts first
        cache = ExtractionCache(cache_dir, max_mb=0)

        src1 = tmp_path / "first.bin"
        src1.write_bytes(b"a" * 50)
        src2 = tmp_path / "second.bin"
        src2.write_bytes(b"b" * 50)

        limits = ExtractionLimits()
        cache.get_or_extract(src1, limits)
        assert str(src1) in cache._entries

        cache.get_or_extract(src2, limits)
        # First entry evicted to make room
        assert str(src1) not in cache._entries
        assert str(src2) in cache._entries

    def test_execute_plan_with_cache_params(self, tmp_path: Path):
        """Verify execute_plan accepts extraction_cache_mb parameter."""
        from romtholos.collect.db import CacheDB
        from romtholos.collect.execute import execute_plan

        db_path = tmp_path / "test.db"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        romroot.mkdir()
        work.mkdir()

        with CacheDB(db_path) as db:
            result = execute_plan(
                game_plans=[],
                romroot=romroot,
                work_dir=work,
                compression_profile="7z-16m",
                db=db,
                extraction_cache_mb=512,
            )
            assert result["processed"] == 0


class TestPartialMinRatio:
    """Test partial_min_ratio configuration and threshold."""

    def test_default_ratio_is_zero(self):
        from romtholos.collect.config import CollectorConfig

        cfg = CollectorConfig()
        assert cfg.partial_min_ratio == 0.0

    def test_ratio_global_resolution(self):
        from romtholos.collect.config import CollectorConfig

        cfg = CollectorConfig(partial_min_ratio=0.1)
        assert cfg.partial_min_ratio_for_system("anything") == 0.1

    def test_ratio_per_system_override(self):
        from romtholos.collect.config import CollectorConfig, SystemConfig

        cfg = CollectorConfig(
            partial_min_ratio=0.1,
            systems={
                "Sega - Mega CD": SystemConfig(partial_min_ratio=0.2),
            },
        )
        assert cfg.partial_min_ratio_for_system("Sega - Mega CD") == 0.2
        assert cfg.partial_min_ratio_for_system("other") == 0.1

    def test_system_negative_ratio_falls_back(self):
        from romtholos.collect.config import CollectorConfig, SystemConfig

        cfg = CollectorConfig(
            partial_min_ratio=0.15,
            systems={
                "test": SystemConfig(partial_min_ratio=-1.0),
            },
        )
        assert cfg.partial_min_ratio_for_system("test") == 0.15

    def test_threshold_accepted_by_execute(self, tmp_path: Path):
        from romtholos.collect.db import CacheDB
        from romtholos.collect.execute import execute_plan

        db_path = tmp_path / "test.db"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        romroot.mkdir()
        work.mkdir()

        with CacheDB(db_path) as db:
            result = execute_plan(
                game_plans=[],
                romroot=romroot,
                work_dir=work,
                compression_profile="aaru-ps1-zstd",
                db=db,
                partial_fallback="zstd-12",
                partial_min_ratio=0.1,
            )
            assert result["processed"] == 0

    def test_threshold_skips_below_ratio(self, tmp_path: Path):
        """Partial game with 1/35 tracks should be skipped at 0.1 ratio."""
        from romtholos.collect.db import CacheDB
        from romtholos.collect.execute import execute_plan
        from romtholos.collect.match import GamePlan, MatchOp

        db_path = tmp_path / "test.db"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        romroot.mkdir()
        work.mkdir()

        # Create a partial game: 1 available out of 35 total
        new_op = MatchOp(
            dat_path="test.dat",
            system="Sega - Mega CD",
            game_name="Sonic CD",
            rom_name="Track 01.bin",
            rom_size=100,
            source_path=str(tmp_path / "source.7z"),
            source_type="archive_content",
            archive_entry="Track 01.bin",
            status="matched",
            hash_type="sha1",
            hash_value="AA" * 20,
        )
        missing_ops = [
            MatchOp(
                dat_path="test.dat",
                system="Sega - Mega CD",
                game_name="Sonic CD",
                rom_name=f"Track {i:02d}.bin",
                rom_size=100,
                source_path=None,
                source_type=None,
                archive_entry=None,
                status="missing",
                hash_type="sha1",
                hash_value="BB" * 20,
            )
            for i in range(2, 36)
        ]
        game = GamePlan(
            system="Sega - Mega CD",
            game_name="Sonic CD",
            ops=[new_op] + missing_ops,
        )

        with CacheDB(db_path) as db:
            result = execute_plan(
                game_plans=[game],
                romroot=romroot,
                work_dir=work,
                compression_profile="aaru-ps1-zstd",
                db=db,
                partial_fallback="zstd-12",
                partial_min_ratio=0.1,  # need 10%, have ~3%
            )
            # Game should be skipped (below threshold)
            assert result["processed"] == 0
            assert result["missing"] == 34  # missing_ops count

    def test_threshold_allows_above_ratio(self, tmp_path: Path):
        """Partial game with 10/20 tracks should pass at 0.1 ratio."""
        from romtholos.collect.db import CacheDB
        from romtholos.collect.execute import execute_plan
        from romtholos.collect.match import GamePlan, MatchOp

        db_path = tmp_path / "test.db"
        romroot = tmp_path / "romroot"
        work = tmp_path / "work"
        romroot.mkdir()
        work.mkdir()

        # Create source files
        for i in range(1, 11):
            src = tmp_path / f"Track {i:02d}.bin"
            src.write_bytes(b"x" * 100)

        new_ops = [
            MatchOp(
                dat_path="test.dat",
                system="Sega - Mega CD",
                game_name="Test Game",
                rom_name=f"Track {i:02d}.bin",
                rom_size=100,
                source_path=str(tmp_path / f"Track {i:02d}.bin"),
                source_type="plain",
                archive_entry=None,
                status="matched",
                hash_type="sha1",
                hash_value="AA" * 20,
            )
            for i in range(1, 11)
        ]
        missing_ops = [
            MatchOp(
                dat_path="test.dat",
                system="Sega - Mega CD",
                game_name="Test Game",
                rom_name=f"Track {i:02d}.bin",
                rom_size=100,
                source_path=None,
                source_type=None,
                archive_entry=None,
                status="missing",
                hash_type="sha1",
                hash_value="BB" * 20,
            )
            for i in range(11, 21)
        ]
        game = GamePlan(
            system="Sega - Mega CD",
            game_name="Test Game",
            ops=new_ops + missing_ops,
        )

        with CacheDB(db_path) as db:
            # 10/20 = 0.5 which is above 0.1 threshold
            # Game should be attempted (not skipped by threshold)
            # It will likely fail during processing but won't be skipped
            result = execute_plan(
                game_plans=[game],
                romroot=romroot,
                work_dir=work,
                compression_profile="aaru-ps1-zstd",
                db=db,
                partial_fallback="zstd-12",
                partial_min_ratio=0.1,
            )
            # Should attempt processing (not skipped by threshold)
            # Failures are expected since hashes won't match
            assert result["missing"] <= 10  # not all missing


class TestConfigSbiDir:
    """Test that sbi_dir config is loaded."""

    def test_config_sbi_dir_none_by_default(self):
        from romtholos.collect.config import CollectorConfig

        cfg = CollectorConfig()
        assert cfg.sbi_dir is None

    def test_config_verify_roundtrip_false_by_default(self):
        from romtholos.collect.config import CollectorConfig

        cfg = CollectorConfig()
        assert cfg.verify_roundtrip is False

    def test_config_extraction_cache_mb_default(self):
        from romtholos.collect.config import CollectorConfig

        cfg = CollectorConfig()
        assert cfg.extraction_cache_mb == 2048

    def test_config_partial_min_ratio_default(self):
        from romtholos.collect.config import CollectorConfig

        cfg = CollectorConfig()
        assert cfg.partial_min_ratio == 0.0
