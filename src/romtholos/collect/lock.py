"""Concurrent run protection — exclusive lock file for romroot.

Only one collector process may run against a given romroot at a time.
The lock file records PID and start timestamp so stale locks from
crashed processes can be detected and overridden.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

LOCK_FILENAME = ".collector.lock"


class CollectorLockError(RuntimeError):
    """Raised when the lock cannot be acquired because another process holds it."""


def _pid_alive(pid: int) -> bool:
    """Check whether a process with the given PID is still running."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Process exists but we can't signal it — still alive.
        return True
    return True


def _read_lock(lock_path: Path) -> tuple[int, float] | None:
    """Parse a lock file.  Returns (pid, timestamp) or None if unreadable."""
    try:
        text = lock_path.read_text(encoding="utf-8").strip()
        lines = text.splitlines()
        pid = int(lines[0].split("=", 1)[1])
        ts = float(lines[1].split("=", 1)[1])
        return pid, ts
    except (OSError, IndexError, ValueError):
        return None


def acquire_lock(romroot: Path) -> Path:
    """Acquire an exclusive collector lock for *romroot*.

    Creates ``romroot/.collector.lock`` containing the current PID and
    an ISO-precision start timestamp.

    If a lock already exists:
    - **Live PID** → raise :class:`CollectorLockError`.
    - **Dead PID (stale)** → override the lock (the previous process crashed).
    - **Unreadable lock file** → override (treat as corrupt/stale).

    Returns the lock file path (caller must pass it to :func:`release_lock`).
    """
    romroot.mkdir(parents=True, exist_ok=True)
    lock_path = romroot / LOCK_FILENAME

    if lock_path.exists():
        info = _read_lock(lock_path)
        if info is not None:
            pid, ts = info
            if _pid_alive(pid):
                raise CollectorLockError(
                    f"Another collector is running (PID {pid}, "
                    f"started {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))}). "
                    f"Lock file: {lock_path}"
                )
        # Stale or unreadable — override below.

    lock_path.write_text(
        f"pid={os.getpid()}\ntimestamp={time.time()}\n",
        encoding="utf-8",
    )
    return lock_path


def release_lock(lock_path: Path) -> None:
    """Release a previously acquired lock.

    Safe to call even if the lock file was already removed (e.g. by
    another process overriding a stale lock).
    """
    try:
        lock_path.unlink()
    except FileNotFoundError:
        pass
