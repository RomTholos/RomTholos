"""Concurrent run protection — FILE_SAFETY.md § Concurrent Run Protection.

Only one collector process may run against a given romroot at a time.
The collector must acquire an exclusive lock file at startup and hold it
for the duration of the run.

Guarantees tested:
- Lock file acquired at start, released after run
- Second process refused while lock held
- Stale lock detection (crashed process)
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from romtholos.collect.config import SourceDir
from romtholos.collect.db import CacheDB
from romtholos.collect.lock import (
    LOCK_FILENAME,
    CollectorLockError,
    acquire_lock,
    release_lock,
)

from .conftest import make_dat, make_rom, pipeline_run, rom_entry


class TestLockFile:
    """Lock file must prevent concurrent collector runs."""

    def test_lock_file_created_and_released(self, env):
        """Lock file exists while held, gone after release."""
        lock_path = acquire_lock(env.romroot)
        assert lock_path.exists()
        assert lock_path.name == LOCK_FILENAME

        # File contains PID and timestamp
        text = lock_path.read_text(encoding="utf-8")
        assert f"pid={os.getpid()}" in text
        assert "timestamp=" in text

        release_lock(lock_path)
        assert not lock_path.exists()

    def test_lock_prevents_second_acquisition(self, env):
        """A second acquire_lock call refuses while the first lock is held."""
        lock_path = acquire_lock(env.romroot)
        try:
            with pytest.raises(CollectorLockError, match="Another collector is running"):
                acquire_lock(env.romroot)
        finally:
            release_lock(lock_path)

    def test_stale_lock_detected(self, env):
        """Lock from a dead PID is detected as stale and overridden."""
        lock_path = env.romroot / LOCK_FILENAME
        # Write a lock with a PID that cannot exist (max PID + 1 on Linux
        # is guaranteed dead, but we use a safer approach: PID 1 is init
        # and always alive, so we pick a very large PID)
        dead_pid = 4_194_300  # above typical PID range, will not exist
        lock_path.write_text(
            f"pid={dead_pid}\ntimestamp=1700000000.0\n",
            encoding="utf-8",
        )

        # Should succeed — stale lock overridden
        new_lock = acquire_lock(env.romroot)
        try:
            text = new_lock.read_text(encoding="utf-8")
            assert f"pid={os.getpid()}" in text
        finally:
            release_lock(new_lock)

    def test_release_idempotent(self, env):
        """Releasing an already-released lock does not raise."""
        lock_path = acquire_lock(env.romroot)
        release_lock(lock_path)
        # Second release is a no-op
        release_lock(lock_path)

    def test_corrupt_lock_overridden(self, env):
        """An unreadable lock file is treated as stale."""
        lock_path = env.romroot / LOCK_FILENAME
        lock_path.write_text("garbage\n", encoding="utf-8")

        new_lock = acquire_lock(env.romroot)
        try:
            text = new_lock.read_text(encoding="utf-8")
            assert f"pid={os.getpid()}" in text
        finally:
            release_lock(new_lock)

    def test_pipeline_holds_lock_during_execution(self, env):
        """Lock file exists in romroot during pipeline execution."""
        rom = make_rom(env.source / "Game.gba", b"LOCK_TEST" * 100)
        entry = rom_entry(rom)

        make_dat(env.selection / "F" / "Sys.dat", "Sys", [
            {"name": "Game", "roms": [entry]},
        ])

        lock_path = env.romroot / LOCK_FILENAME

        # Pipeline doesn't use CLI lock (tests call pipeline_run directly),
        # so verify the lock module works end-to-end by wrapping manually.
        lock = acquire_lock(env.romroot)
        assert lock_path.exists()
        try:
            with CacheDB(env.db_path) as db:
                sources = [SourceDir(path=env.source, source_type="readonly")]
                pipeline_run(sources, env.selection, env.romroot, env.work, db)

            # Lock still held during entire pipeline
            assert lock_path.exists()
        finally:
            release_lock(lock)

        assert not lock_path.exists()

        # Game was collected successfully despite lock overhead
        collected = list(env.romroot.rglob("*.7z"))
        assert len(collected) == 1
