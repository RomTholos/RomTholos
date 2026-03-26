"""Collector DB backup with tiered rotation."""

from __future__ import annotations

import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Minimum interval between backups (seconds)
_COOLDOWN_SECS = 15 * 60  # 15 minutes

# Tier boundaries and bucket sizes (seconds)
# (max_age, bucket_size)
_TIERS: list[tuple[timedelta, timedelta]] = [
    (timedelta(hours=1), timedelta(minutes=15)),    # 0–1h: keep every 15 min
    (timedelta(hours=24), timedelta(hours=1)),       # 1–24h: keep 1 per hour
    (timedelta(days=7), timedelta(days=1)),          # 1–7d: keep 1 per day
    (timedelta(days=180), timedelta(weeks=1)),       # 1w–6mo: keep 1 per week
]


def backup_db(db_path: Path, backup_dir: Path) -> Path | None:
    """Back up the collector DB with tiered rotation.

    Returns the path to the new backup, or None if skipped.
    """
    if not db_path.exists() or db_path.stat().st_size == 0:
        return None

    backup_dir.mkdir(parents=True, exist_ok=True)

    existing = _list_backups(backup_dir)

    # Cooldown: skip if newest backup is recent enough
    if existing:
        newest_age = datetime.now() - _mtime(existing[0])
        if newest_age.total_seconds() < _COOLDOWN_SECS:
            return None

    # Copy
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    dest = backup_dir / f"collector-{timestamp}.db"
    shutil.copy2(db_path, dest)
    print(f"Backup: {dest}", file=sys.stderr)

    # Rotate (include new file)
    all_backups = [dest] + existing
    _rotate_backups(all_backups)

    return dest


def _list_backups(backup_dir: Path) -> list[Path]:
    """Return collector-*.db files sorted newest first by mtime."""
    files = list(backup_dir.glob("collector-*.db"))
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files


def _mtime(path: Path) -> datetime:
    return datetime.fromtimestamp(path.stat().st_mtime)


def _rotate_backups(backups: list[Path]) -> None:
    """Apply tiered rotation, deleting files that don't fit any bucket.

    backups must be sorted newest first.
    """
    now = datetime.now()
    keep: set[Path] = set()

    # Track last-kept timestamp per tier to enforce bucket spacing
    tier_last_kept: list[datetime | None] = [None] * len(_TIERS)

    for backup in backups:
        age = now - _mtime(backup)

        # Find which tier this file belongs to
        tier_idx = _tier_for_age(age)
        if tier_idx is None:
            # Older than all tiers — delete
            backup.unlink()
            continue

        _, bucket_size = _TIERS[tier_idx]
        last = tier_last_kept[tier_idx]

        if last is None:
            # First file in this tier — always keep
            keep.add(backup)
            tier_last_kept[tier_idx] = _mtime(backup)
        else:
            # Keep if it's in a different bucket than the last kept file
            if abs((last - _mtime(backup)).total_seconds()) >= bucket_size.total_seconds():
                keep.add(backup)
                tier_last_kept[tier_idx] = _mtime(backup)

    # Delete files not kept
    for backup in backups:
        if backup not in keep and backup.exists():
            backup.unlink()


def _tier_for_age(age: timedelta) -> int | None:
    """Return tier index for a given age, or None if beyond all tiers."""
    prev_max = timedelta(0)
    for i, (max_age, _) in enumerate(_TIERS):
        if prev_max <= age < max_age:
            return i
        prev_max = max_age
    return None
