"""SQLite DB cache — disposable index for fast hash lookups.

Not the source of truth. Rebuilt from RSCF files + DATs if lost.
"""

from __future__ import annotations

import contextlib
import sqlite3
from pathlib import Path

_SCHEMA_VERSION = 4

# Canonical hash types — used for assertions and iteration across all stages.
HASH_TYPES: tuple[str, ...] = ("crc32", "md5", "sha1", "sha256", "blake3")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS scanned_files (
    path        TEXT PRIMARY KEY,
    size        INTEGER NOT NULL,
    mtime_ns    INTEGER NOT NULL,
    ctime_ns    INTEGER NOT NULL DEFAULT 0,
    inode       INTEGER NOT NULL DEFAULT 0,
    source_type TEXT NOT NULL DEFAULT 'readonly',
    crc32       TEXT,
    md5         TEXT,
    sha1        TEXT,
    sha256      TEXT,
    blake3      TEXT,
    is_archive  INTEGER DEFAULT 0,
    scanned_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS archive_contents (
    archive_path    TEXT NOT NULL,
    entry_name      TEXT NOT NULL,
    entry_size      INTEGER,
    crc32           TEXT,
    md5             TEXT,
    sha1            TEXT,
    sha256          TEXT,
    blake3          TEXT,
    PRIMARY KEY (archive_path, entry_name)
);

CREATE TABLE IF NOT EXISTS dat_entries (
    dat_path    TEXT NOT NULL,
    system      TEXT NOT NULL,
    game_name   TEXT NOT NULL,
    rom_name    TEXT NOT NULL,
    rom_size    INTEGER,
    crc32       TEXT,
    md5         TEXT,
    sha1        TEXT,
    sha256      TEXT,
    blake3      TEXT,
    PRIMARY KEY (dat_path, game_name, rom_name)
);

CREATE TABLE IF NOT EXISTS matches (
    dat_path        TEXT NOT NULL,
    game_name       TEXT NOT NULL,
    rom_name        TEXT NOT NULL,
    source_path     TEXT,
    source_type     TEXT,
    archive_entry   TEXT,
    status          TEXT NOT NULL,
    PRIMARY KEY (dat_path, game_name, rom_name)
);

CREATE TABLE IF NOT EXISTS romroot_files (
    path        TEXT NOT NULL,
    system      TEXT NOT NULL,
    game_name   TEXT NOT NULL,
    rom_name    TEXT NOT NULL,
    crc32       TEXT,
    md5         TEXT,
    sha1        TEXT,
    sha256      TEXT,
    blake3      TEXT,
    rscf_path   TEXT,
    PRIMARY KEY (path, rom_name)
);

CREATE INDEX IF NOT EXISTS idx_scanned_crc32 ON scanned_files(crc32);
CREATE INDEX IF NOT EXISTS idx_scanned_md5 ON scanned_files(md5);
CREATE INDEX IF NOT EXISTS idx_scanned_sha1 ON scanned_files(sha1);
CREATE INDEX IF NOT EXISTS idx_scanned_sha256 ON scanned_files(sha256);
CREATE INDEX IF NOT EXISTS idx_scanned_blake3 ON scanned_files(blake3);
CREATE INDEX IF NOT EXISTS idx_archive_crc32 ON archive_contents(crc32);
CREATE INDEX IF NOT EXISTS idx_archive_md5 ON archive_contents(md5);
CREATE INDEX IF NOT EXISTS idx_archive_sha1 ON archive_contents(sha1);
CREATE INDEX IF NOT EXISTS idx_archive_sha256 ON archive_contents(sha256);
CREATE INDEX IF NOT EXISTS idx_archive_blake3 ON archive_contents(blake3);
CREATE INDEX IF NOT EXISTS idx_dat_crc32 ON dat_entries(crc32);
CREATE INDEX IF NOT EXISTS idx_dat_md5 ON dat_entries(md5);
CREATE INDEX IF NOT EXISTS idx_dat_sha1 ON dat_entries(sha1);
CREATE INDEX IF NOT EXISTS idx_dat_sha256 ON dat_entries(sha256);
CREATE INDEX IF NOT EXISTS idx_dat_blake3 ON dat_entries(blake3);
CREATE INDEX IF NOT EXISTS idx_romroot_crc32 ON romroot_files(crc32);
CREATE INDEX IF NOT EXISTS idx_romroot_md5 ON romroot_files(md5);
CREATE INDEX IF NOT EXISTS idx_romroot_sha1 ON romroot_files(sha1);
CREATE INDEX IF NOT EXISTS idx_romroot_sha256 ON romroot_files(sha256);
CREATE INDEX IF NOT EXISTS idx_romroot_blake3 ON romroot_files(blake3);
CREATE INDEX IF NOT EXISTS idx_romroot_game ON romroot_files(system, game_name);
"""


class CacheDB:
    """SQLite cache database for the collector.

    Disposable — rebuild from RSCF + DATs if lost.
    """

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.row_factory = sqlite3.Row
        self._in_batch = False
        self._migrate_if_needed()

    def _migrate_if_needed(self) -> None:
        """Check schema version and recreate if stale."""
        ver = self._conn.execute("PRAGMA user_version").fetchone()[0]
        if ver == _SCHEMA_VERSION:
            return

        # Drop all tables and recreate — DB is disposable
        tables = [
            r[0]
            for r in self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        ]
        for table in tables:
            self._conn.execute(f"DROP TABLE IF EXISTS {table}")

        self._conn.executescript(_SCHEMA)
        self._conn.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> CacheDB:
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def _auto_commit(self) -> None:
        """Commit if not inside a batch() context."""
        if not self._in_batch:
            self._conn.commit()

    @contextlib.contextmanager
    def batch(self):
        """Context manager for batch operations — commits once on exit.

        Wraps all writes in a single transaction for performance.
        Nested batch() calls are allowed (only outermost commits).
        """
        if self._in_batch:
            yield
            return

        self._in_batch = True
        try:
            yield
            self._conn.commit()
        except BaseException:
            self._conn.rollback()
            raise
        finally:
            self._in_batch = False

    # --- Scanned files ---

    def get_scanned(self, path: str) -> sqlite3.Row | None:
        """Look up a previously scanned file by path."""
        cur = self._conn.execute(
            "SELECT * FROM scanned_files WHERE path = ?", (path,)
        )
        return cur.fetchone()

    def is_unchanged(
        self, path: str, size: int, mtime_ns: int, ctime_ns: int, inode: int
    ) -> bool:
        """Check if a file hasn't changed since last scan.

        Checks path + size + mtime_ns + ctime_ns + inode.
        """
        row = self.get_scanned(path)
        if row is None:
            return False
        return (
            row["size"] == size
            and row["mtime_ns"] == mtime_ns
            and row["ctime_ns"] == ctime_ns
            and row["inode"] == inode
        )

    def upsert_scanned(
        self,
        path: str,
        size: int,
        mtime_ns: int,
        ctime_ns: int = 0,
        inode: int = 0,
        source_type: str = "readonly",
        crc32: str = "",
        md5: str = "",
        sha1: str = "",
        sha256: str = "",
        blake3: str = "",
        is_archive: bool = False,
        scanned_at: str = "",
    ) -> None:
        """Insert or update a scanned file record."""
        self._conn.execute(
            """INSERT OR REPLACE INTO scanned_files
               (path, size, mtime_ns, ctime_ns, inode, source_type,
                crc32, md5, sha1, sha256, blake3, is_archive, scanned_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (path, size, mtime_ns, ctime_ns, inode, source_type,
             crc32, md5, sha1, sha256, blake3,
             1 if is_archive else 0, scanned_at),
        )
        self._auto_commit()

    # --- Archive contents ---

    def upsert_archive_content(
        self,
        archive_path: str,
        entry_name: str,
        entry_size: int | None = None,
        crc32: str = "",
        md5: str = "",
        sha1: str = "",
        sha256: str = "",
        blake3: str = "",
    ) -> None:
        """Record an archive content entry with all hashes."""
        self._conn.execute(
            """INSERT OR REPLACE INTO archive_contents
               (archive_path, entry_name, entry_size, crc32, md5, sha1, sha256, blake3)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (archive_path, entry_name, entry_size, crc32, md5, sha1, sha256, blake3),
        )
        self._auto_commit()

    def has_archive_contents(self, archive_path: str) -> bool:
        """Check if we have any hashed archive content entries for this archive."""
        cur = self._conn.execute(
            "SELECT 1 FROM archive_contents WHERE archive_path = ? LIMIT 1",
            (archive_path,),
        )
        return cur.fetchone() is not None

    def delete_archive_contents(self, archive_path: str) -> None:
        """Delete all archive content entries for an archive (before re-extraction)."""
        self._conn.execute(
            "DELETE FROM archive_contents WHERE archive_path = ?",
            (archive_path,),
        )
        self._auto_commit()

    def get_archive_contents(self, archive_path: str) -> list[sqlite3.Row]:
        """Get all content entries for an archive."""
        cur = self._conn.execute(
            "SELECT * FROM archive_contents WHERE archive_path = ?",
            (archive_path,),
        )
        return cur.fetchall()

    def find_archive_content_by_hash(
        self, hash_type: str, hash_value: str
    ) -> list[sqlite3.Row]:
        """Find archive content entries matching a hash value."""
        assert hash_type in HASH_TYPES
        cur = self._conn.execute(
            f"SELECT * FROM archive_contents WHERE {hash_type} = ?",
            (hash_value,),
        )
        return cur.fetchall()

    def find_archive_content_by_name(
        self, entry_name: str
    ) -> list[sqlite3.Row]:
        """Find archive content entries by filename."""
        cur = self._conn.execute(
            "SELECT * FROM archive_contents WHERE entry_name = ?",
            (entry_name,),
        )
        return cur.fetchall()

    # --- DAT entries ---

    def load_dat(
        self,
        dat_path: str,
        system: str,
        entries: list[dict],
    ) -> None:
        """Load DAT entries into the cache.

        entries: list of dicts with keys: game_name, rom_name, rom_size,
        crc32, md5, sha1, sha256, blake3
        """
        # Clear existing entries for this DAT
        self._conn.execute(
            "DELETE FROM dat_entries WHERE dat_path = ?", (dat_path,)
        )

        for entry in entries:
            self._conn.execute(
                """INSERT INTO dat_entries
                   (dat_path, system, game_name, rom_name, rom_size,
                    crc32, md5, sha1, sha256, blake3)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (dat_path, system, entry["game_name"], entry["rom_name"],
                 entry.get("rom_size"), entry.get("crc32", ""),
                 entry.get("md5", ""), entry.get("sha1", ""),
                 entry.get("sha256", ""), entry.get("blake3", "")),
            )

        self._conn.commit()

    def get_dat_entries(self, dat_path: str) -> list[sqlite3.Row]:
        """Get all DAT entries for a given DAT path."""
        cur = self._conn.execute(
            "SELECT * FROM dat_entries WHERE dat_path = ?", (dat_path,)
        )
        return cur.fetchall()

    # --- Hash lookups ---

    def find_by_hash(self, hash_type: str, hash_value: str) -> list[sqlite3.Row]:
        """Find scanned files matching a hash value."""
        assert hash_type in HASH_TYPES
        cur = self._conn.execute(
            f"SELECT * FROM scanned_files WHERE {hash_type} = ?",
            (hash_value,),
        )
        return cur.fetchall()

    # --- Match recording ---

    def record_match(
        self,
        dat_path: str,
        game_name: str,
        rom_name: str,
        source_path: str | None,
        source_type: str | None,
        archive_entry: str | None,
        status: str,
    ) -> None:
        """Record a match result."""
        self._conn.execute(
            """INSERT OR REPLACE INTO matches
               (dat_path, game_name, rom_name, source_path, source_type,
                archive_entry, status)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (dat_path, game_name, rom_name, source_path, source_type,
             archive_entry, status),
        )
        self._auto_commit()

    def get_matches(self, dat_path: str | None = None) -> list[sqlite3.Row]:
        """Get all matches, optionally filtered by DAT."""
        if dat_path:
            cur = self._conn.execute(
                "SELECT * FROM matches WHERE dat_path = ?", (dat_path,)
            )
        else:
            cur = self._conn.execute("SELECT * FROM matches")
        return cur.fetchall()

    def get_unmatched_dat_entries(
        self, dat_path: str | None = None
    ) -> list[sqlite3.Row]:
        """Get DAT entries with no match."""
        if dat_path:
            cur = self._conn.execute(
                """SELECT d.* FROM dat_entries d
                   LEFT JOIN matches m ON d.dat_path = m.dat_path
                     AND d.game_name = m.game_name AND d.rom_name = m.rom_name
                   WHERE m.status IS NULL AND d.dat_path = ?""",
                (dat_path,),
            )
        else:
            cur = self._conn.execute(
                """SELECT d.* FROM dat_entries d
                   LEFT JOIN matches m ON d.dat_path = m.dat_path
                     AND d.game_name = m.game_name AND d.rom_name = m.rom_name
                   WHERE m.status IS NULL""",
            )
        return cur.fetchall()

    # --- Romroot inventory ---

    def upsert_romroot(
        self,
        path: str,
        system: str,
        game_name: str,
        rom_name: str,
        crc32: str = "",
        md5: str = "",
        sha1: str = "",
        sha256: str = "",
        blake3: str = "",
        rscf_path: str = "",
    ) -> None:
        """Record a file in the romroot inventory."""
        self._conn.execute(
            """INSERT OR REPLACE INTO romroot_files
               (path, system, game_name, rom_name, crc32, md5, sha1,
                sha256, blake3, rscf_path)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (path, system, game_name, rom_name, crc32, md5, sha1,
             sha256, blake3, rscf_path),
        )
        self._auto_commit()

    def find_in_romroot(
        self, hash_type: str, hash_value: str
    ) -> sqlite3.Row | None:
        """Check if a hash already exists in romroot."""
        assert hash_type in HASH_TYPES
        cur = self._conn.execute(
            f"SELECT * FROM romroot_files WHERE {hash_type} = ?",
            (hash_value,),
        )
        return cur.fetchone()

    def get_romroot_game(
        self, system: str, game_name: str
    ) -> list[sqlite3.Row]:
        """Get all romroot entries for a game (all ROMs in the archive)."""
        cur = self._conn.execute(
            "SELECT * FROM romroot_files WHERE system = ? AND game_name = ?",
            (system, game_name),
        )
        return cur.fetchall()

    def delete_romroot_entries(self, path: str) -> None:
        """Delete all romroot entries for a container path.

        Used when rebuilding an archive (replace all entries).
        """
        self._conn.execute(
            "DELETE FROM romroot_files WHERE path = ?", (path,)
        )
        self._auto_commit()

    # --- Stats ---

    def stats(self) -> dict:
        """Return summary statistics."""
        def count(table: str) -> int:
            return self._conn.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]

        matched = self._conn.execute(
            "SELECT COUNT(*) FROM matches WHERE status = 'matched'"
        ).fetchone()[0]
        missing = self._conn.execute(
            "SELECT COUNT(*) FROM matches WHERE status = 'missing'"
        ).fetchone()[0]

        return {
            "scanned_files": count("scanned_files"),
            "archive_contents": count("archive_contents"),
            "dat_entries": count("dat_entries"),
            "matched": matched,
            "missing": missing,
            "romroot_files": count("romroot_files"),
        }
