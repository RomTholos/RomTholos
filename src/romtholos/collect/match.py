"""Phase 2: Match — compare scanned files against selection DATs.

Read-only (except DB cache writes). Produces a match plan.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

from defusedxml.ElementTree import parse as safe_parse

from romtholos.collect.db import CacheDB


@dataclass
class MatchOp:
    """A single operation in the match plan."""

    dat_path: str
    system: str
    game_name: str
    rom_name: str
    rom_size: int | None
    source_path: str | None
    source_type: str | None  # 'plain', 'archive', 'archive_content'
    archive_entry: str | None
    status: str  # 'matched', 'missing', 'in_romroot'
    hash_type: str = ""  # which hash matched
    hash_value: str = ""


@dataclass
class GamePlan:
    """Game-level view of match operations.

    Groups all ROM match ops for one game and classifies overall status.
    """

    system: str
    game_name: str
    ops: list[MatchOp]

    @property
    def status(self) -> str:
        """Classify the game's overall collection status.

        Returns:
            complete: all ROMs in romroot
            collectable: all ROMs either matched or in romroot (can finish)
            partial: some ROMs matched/in_romroot, some missing
            unavailable: no ROMs matched (all missing)
        """
        statuses = {op.status for op in self.ops}
        if statuses == {"in_romroot"}:
            return "complete"
        if "matched" in statuses or "in_romroot" in statuses:
            return "partial" if "missing" in statuses else "collectable"
        return "unavailable"

    @property
    def new_ops(self) -> list[MatchOp]:
        """ROMs that need collecting (matched but not yet in romroot)."""
        return [op for op in self.ops if op.status == "matched"]

    @property
    def existing_ops(self) -> list[MatchOp]:
        """ROMs already in romroot."""
        return [op for op in self.ops if op.status == "in_romroot"]

    @property
    def missing_ops(self) -> list[MatchOp]:
        """ROMs not found in any source."""
        return [op for op in self.ops if op.status == "missing"]


def group_by_game(ops: list[MatchOp]) -> list[GamePlan]:
    """Group match operations by game, producing game-level plans."""
    games: dict[str, list[MatchOp]] = {}
    for op in ops:
        key = f"{op.system}\x00{op.game_name}"
        games.setdefault(key, []).append(op)

    return [
        GamePlan(
            system=game_ops[0].system,
            game_name=game_ops[0].game_name,
            ops=game_ops,
        )
        for game_ops in games.values()
    ]


def load_dat_to_db(dat_path: Path, db: CacheDB) -> str:
    """Parse a selection DAT and load entries into the DB cache.

    Returns the system name from the DAT header.
    """
    tree = safe_parse(str(dat_path))
    root = tree.getroot()

    header = root.find("header")
    system = ""
    if header is not None:
        name_el = header.find("name")
        if name_el is not None and name_el.text:
            system = name_el.text

    entries = []
    for game in root.findall("game"):
        game_name = game.get("name", "")
        if not game_name:
            continue

        for rom in game.findall("rom"):
            rom_name = rom.get("name", "")
            entries.append({
                "game_name": game_name,
                "rom_name": rom_name,
                "rom_size": int(rom.get("size") or "0"),
                "crc32": (rom.get("crc", "") or "").upper(),
                "md5": (rom.get("md5", "") or "").upper(),
                "sha1": (rom.get("sha1", "") or "").upper(),
                "sha256": (rom.get("sha256", "") or "").upper(),
            })

    db.load_dat(str(dat_path), system, entries)
    return system


def _try_match_hash(
    db: CacheDB, hash_type: str, hash_value: str
) -> tuple[str | None, str | None, str | None]:
    """Try to find a match by hash in scanned files and extracted hashes.

    Returns (source_path, source_type, archive_entry) or (None, None, None).
    """
    if not hash_value:
        return None, None, None

    # Try plain scanned files first
    rows = db.find_by_hash(hash_type, hash_value)
    if rows:
        row = rows[0]
        if row["is_archive"]:
            return str(row["path"]), "archive", None
        else:
            return str(row["path"]), "plain", None

    # Try archive contents (fully hashed during scan)
    rows = db.find_archive_content_by_hash(hash_type, hash_value)
    if rows:
        row = rows[0]
        return str(row["archive_path"]), "archive_content", str(row["entry_name"])

    return None, None, None


def match_dat(dat_path: Path, db: CacheDB) -> list[MatchOp]:
    """Match a selection DAT against the scanned file cache.

    Returns list of MatchOp describing what to do for each ROM.
    """
    system = load_dat_to_db(dat_path, db)
    ops: list[MatchOp] = []

    dat_entries = db.get_dat_entries(str(dat_path))

    for entry in dat_entries:
        game_name = entry["game_name"]
        rom_name = entry["rom_name"]
        rom_size = entry["rom_size"]

        # Check if already in romroot
        for ht in ("sha1", "md5", "crc32"):
            hv = entry[ht]
            if hv:
                existing = db.find_in_romroot(ht, hv)
                if existing:
                    ops.append(MatchOp(
                        dat_path=str(dat_path),
                        system=system,
                        game_name=game_name,
                        rom_name=rom_name,
                        rom_size=rom_size,
                        source_path=str(existing["path"]),
                        source_type="romroot",
                        archive_entry=None,
                        status="in_romroot",
                        hash_type=ht,
                        hash_value=hv,
                    ))
                    break
        else:
            # Try matching by hash — prefer sha1, then md5, then crc32
            matched = False
            for ht in ("sha1", "md5", "crc32", "sha256"):
                hv = entry[ht]
                if not hv:
                    continue

                source_path, source_type, archive_entry = _try_match_hash(db, ht, hv)
                if source_path:
                    ops.append(MatchOp(
                        dat_path=str(dat_path),
                        system=system,
                        game_name=game_name,
                        rom_name=rom_name,
                        rom_size=rom_size,
                        source_path=source_path,
                        source_type=source_type,
                        archive_entry=archive_entry,
                        status="matched",
                        hash_type=ht,
                        hash_value=hv,
                    ))
                    matched = True
                    break

            if not matched:
                ops.append(MatchOp(
                    dat_path=str(dat_path),
                    system=system,
                    game_name=game_name,
                    rom_name=rom_name,
                    rom_size=rom_size,
                    source_path=None,
                    source_type=None,
                    archive_entry=None,
                    status="missing",
                ))

        # Record in DB
        op = ops[-1]
        db.record_match(
            dat_path=str(dat_path),
            game_name=game_name,
            rom_name=rom_name,
            source_path=op.source_path,
            source_type=op.source_type,
            archive_entry=op.archive_entry,
            status=op.status,
        )

    return ops


def match_all_dats(selection_dir: Path, db: CacheDB) -> dict[str, list[MatchOp]]:
    """Match all selection DATs against scanned files.

    Returns dict of system_name -> list of MatchOps.
    """
    results: dict[str, list[MatchOp]] = {}

    if not selection_dir.is_dir():
        return results

    for system_dir in sorted(selection_dir.iterdir()):
        if not system_dir.is_dir():
            continue

        for dat_file in sorted(system_dir.glob("*.dat")):
            print(f"Matching: {dat_file.name}", file=sys.stderr)
            ops = match_dat(dat_file, db)

            matched = sum(1 for o in ops if o.status == "matched")
            missing = sum(1 for o in ops if o.status == "missing")
            in_romroot = sum(1 for o in ops if o.status == "in_romroot")

            system = ops[0].system if ops else system_dir.name
            print(
                f"  {system}: {matched} matched, {in_romroot} in romroot, "
                f"{missing} missing",
                file=sys.stderr,
            )

            results[system] = ops

    return results


def print_plan(ops_by_system: dict[str, list[MatchOp]]) -> None:
    """Print the match plan summary to stderr."""
    total_games = {"complete": 0, "collectable": 0, "partial": 0, "unavailable": 0}

    for system, ops in sorted(ops_by_system.items()):
        games = group_by_game(ops)

        counts: dict[str, int] = {}
        for g in games:
            counts[g.status] = counts.get(g.status, 0) + 1
            total_games[g.status] = total_games.get(g.status, 0) + 1

        matched = sum(1 for o in ops if o.status == "matched")
        missing = sum(1 for o in ops if o.status == "missing")
        in_romroot = sum(1 for o in ops if o.status == "in_romroot")

        print(f"\n  {system} ({len(games)} games):", file=sys.stderr)
        print(f"    Complete:     {counts.get('complete', 0)} games", file=sys.stderr)
        print(f"    Collectable:  {counts.get('collectable', 0)} games", file=sys.stderr)
        print(f"    Partial:      {counts.get('partial', 0)} games", file=sys.stderr)
        print(f"    Unavailable:  {counts.get('unavailable', 0)} games", file=sys.stderr)
        print(
            f"    ROMs: {in_romroot} in romroot, {matched} matched, "
            f"{missing} missing",
            file=sys.stderr,
        )

        missing_games = [g for g in games if g.status in ("partial", "unavailable")]
        for g in missing_games[:5]:
            n_have = len(g.existing_ops) + len(g.new_ops)
            print(
                f"      ? {g.game_name} ({n_have}/{len(g.ops)} ROMs)",
                file=sys.stderr,
            )
        remaining = len(missing_games) - 5
        if remaining > 0:
            print(f"      ... and {remaining} more", file=sys.stderr)

    t = total_games
    print(
        f"\n  Total: {t.get('complete', 0)} complete, "
        f"{t.get('collectable', 0)} collectable, "
        f"{t.get('partial', 0)} partial, "
        f"{t.get('unavailable', 0)} unavailable",
        file=sys.stderr,
    )
