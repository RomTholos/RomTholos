"""Tests for collector DB backup and tiered rotation."""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from romtholos.collect.backup import (
    _COOLDOWN_SECS,
    _list_backups,
    _rotate_backups,
    _tier_for_age,
    backup_db,
)


def _make_db(path: Path) -> Path:
    """Create a fake collector.db."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\x00" * 128)
    return path


def _make_backup(backup_dir: Path, name: str, age_secs: float) -> Path:
    """Create a backup file with a specific mtime in the past."""
    p = backup_dir / name
    p.write_bytes(b"\x00" * 64)
    mtime = time.time() - age_secs
    os.utime(p, (mtime, mtime))
    return p


class TestBackupDb:
    def test_creates_backup(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path / "collector.db")
        backup_dir = tmp_path / "backup"

        result = backup_db(db, backup_dir)

        assert result is not None
        assert result.exists()
        assert result.parent == backup_dir
        assert result.name.startswith("collector-")
        assert result.name.endswith(".db")
        assert result.stat().st_size == db.stat().st_size

    def test_skips_missing_db(self, tmp_path: Path) -> None:
        result = backup_db(tmp_path / "nonexistent.db", tmp_path / "backup")
        assert result is None

    def test_skips_empty_db(self, tmp_path: Path) -> None:
        db = tmp_path / "collector.db"
        db.write_bytes(b"")

        result = backup_db(db, tmp_path / "backup")
        assert result is None

    def test_cooldown_skips_recent(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path / "collector.db")
        backup_dir = tmp_path / "backup"

        # First backup succeeds
        result1 = backup_db(db, backup_dir)
        assert result1 is not None

        # Second backup within cooldown is skipped
        result2 = backup_db(db, backup_dir)
        assert result2 is None

    def test_cooldown_allows_after_expiry(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path / "collector.db")
        backup_dir = tmp_path / "backup"
        backup_dir.mkdir()

        # Simulate an old backup beyond cooldown
        _make_backup(backup_dir, "collector-20260101-000000.db", _COOLDOWN_SECS + 60)

        result = backup_db(db, backup_dir)
        assert result is not None

    def test_creates_backup_dir(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path / "collector.db")
        backup_dir = tmp_path / "deep" / "nested" / "backup"

        result = backup_db(db, backup_dir)

        assert result is not None
        assert backup_dir.is_dir()


class TestTierForAge:
    def test_fresh(self) -> None:
        assert _tier_for_age(timedelta(minutes=5)) == 0

    def test_one_hour_boundary(self) -> None:
        assert _tier_for_age(timedelta(minutes=59)) == 0
        assert _tier_for_age(timedelta(hours=1)) == 1

    def test_one_day_boundary(self) -> None:
        assert _tier_for_age(timedelta(hours=23)) == 1
        assert _tier_for_age(timedelta(hours=24)) == 2

    def test_one_week_boundary(self) -> None:
        assert _tier_for_age(timedelta(days=6)) == 2
        assert _tier_for_age(timedelta(days=7)) == 3

    def test_six_month_boundary(self) -> None:
        assert _tier_for_age(timedelta(days=179)) == 3
        assert _tier_for_age(timedelta(days=180)) is None

    def test_very_old(self) -> None:
        assert _tier_for_age(timedelta(days=365)) is None


class TestRotateBackups:
    def test_deletes_old_files(self, tmp_path: Path) -> None:
        """Files older than 6 months are deleted."""
        old = _make_backup(tmp_path, "collector-old.db", 200 * 86400)

        _rotate_backups([old])

        assert not old.exists()

    def test_keeps_single_recent(self, tmp_path: Path) -> None:
        """A single recent backup is kept."""
        recent = _make_backup(tmp_path, "collector-recent.db", 60)

        _rotate_backups([recent])

        assert recent.exists()

    def test_thins_hourly_tier(self, tmp_path: Path) -> None:
        """Within 1-24h tier, only one per hour bucket is kept."""
        # Two backups 10 minutes apart, both in the 2h-ago hour
        b1 = _make_backup(tmp_path, "collector-a.db", 2 * 3600)
        b2 = _make_backup(tmp_path, "collector-b.db", 2 * 3600 + 600)

        # Sorted newest first
        backups = sorted([b1, b2], key=lambda p: p.stat().st_mtime, reverse=True)
        _rotate_backups(backups)

        # Only one should survive
        survivors = [b for b in [b1, b2] if b.exists()]
        assert len(survivors) == 1

    def test_keeps_across_tiers(self, tmp_path: Path) -> None:
        """Files in different tiers are independently retained."""
        recent = _make_backup(tmp_path, "collector-recent.db", 30 * 60)  # tier 0
        hourly = _make_backup(tmp_path, "collector-hourly.db", 5 * 3600)  # tier 1
        daily = _make_backup(tmp_path, "collector-daily.db", 3 * 86400)  # tier 2

        backups = sorted(
            [recent, hourly, daily],
            key=lambda p: p.stat().st_mtime, reverse=True,
        )
        _rotate_backups(backups)

        assert recent.exists()
        assert hourly.exists()
        assert daily.exists()


class TestListBackups:
    def test_sorted_newest_first(self, tmp_path: Path) -> None:
        old = _make_backup(tmp_path, "collector-20260101-000000.db", 3600)
        new = _make_backup(tmp_path, "collector-20260320-120000.db", 60)

        result = _list_backups(tmp_path)

        assert result[0] == new
        assert result[1] == old

    def test_ignores_non_matching(self, tmp_path: Path) -> None:
        _make_backup(tmp_path, "collector-20260101-000000.db", 60)
        (tmp_path / "other.txt").write_text("ignored")

        result = _list_backups(tmp_path)
        assert len(result) == 1
