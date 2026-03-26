"""Phase 2: Match — compare scanned files against selection DATs.

Read-only (except DB cache writes). Produces a match plan.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

from defusedxml.ElementTree import parse as safe_parse

from romtholos.collect.compress import strip_archive_extension
from romtholos.collect.config import ORPHANED_DIR_NAME
from romtholos.collect.db import CacheDB


_AARU_EXTENSIONS: frozenset[str] = frozenset({".aaru", ".aaruf", ".dicf"})


def _source_affinity(path: str, game_name: str) -> tuple[int, int]:
    """Score a source path for game-name affinity (higher is better).

    Returns (name_match, is_aaru) where:
        name_match: 1 if archive stem matches game_name, else 0
        is_aaru: 1 if path has an aaru extension, else 0

    Used as a sort key — max() picks the best candidate among
    hash-verified matches. Hash is always the authority; this only
    breaks ties when multiple archives contain the same content.
    """
    p = Path(path)
    stem = strip_archive_extension(p.name)
    name_match = 1 if (game_name and stem == game_name) else 0
    is_aaru = 1 if p.suffix.lower() in _AARU_EXTENSIONS else 0
    return (name_match, is_aaru)


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
    dat_folder: str = ""  # relative path from selection root to DAT's parent
    hash_type: str = ""  # which hash matched
    hash_value: str = ""


@dataclass
class GamePlan:
    """Game-level view of match operations.

    Groups all ROM match ops for one game and classifies overall status.
    """

    system: str
    game_name: str
    dat_folder: str
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
            dat_folder=game_ops[0].dat_folder,
            ops=game_ops,
        )
        for game_ops in games.values()
    ]


def _consolidate_sources(
    game_name: str, system: str, ops: list[MatchOp], db: CacheDB,
) -> list[MatchOp]:
    """Re-assign ops to prefer the best source archive for the game.

    Multi-track disc games can have tracks scattered across many sources
    (e.g. a shared silence track matched to another game's romroot archive
    while all other tracks come from the game's own source .7z).  This pass
    identifies the primary source and re-assigns non-primary ops to it when
    the primary archive also contains those tracks.

    Primary selection ranks candidates by:
      1. Game-name match (archive stem == game_name)
      2. .aaru format (likely copy-as-is candidate)
      3. Track count (original behavior, tiebreaker)

    Same-game romroot ops (tracks already collected for THIS game) are never
    re-assigned.  Cross-game romroot ops and matched ops from secondary
    archives are re-assigned when the primary has the track.

    Ops that cannot be re-assigned (track only in one source) stay as-is —
    the execute phase handles multiple source archives.
    """
    from collections import Counter

    matched = [op for op in ops if op.status == "matched"]
    if not matched:
        return ops  # game complete or unavailable, nothing to consolidate

    # Find primary source among archive_content matched ops
    archive_ops = [
        op for op in matched
        if op.source_type == "archive_content"
    ]
    if not archive_ops:
        return ops  # all plain/archive matches, no grouping possible

    source_counts = Counter(op.source_path for op in archive_ops)

    # Rank candidates: game-name match > .aaru > track count
    primary = max(source_counts, key=lambda sp: (
        _source_affinity(sp, game_name),
        source_counts[sp],
    ))

    # Same-game romroot paths — sacred, never re-assign
    own_romroot_paths = {
        row["path"] for row in db.get_romroot_game(system, game_name)
    }

    # Build hash index for primary source's archive contents
    primary_hashes: dict[tuple[str, str], str] = {}
    for entry in db.get_archive_contents(primary):
        for ht in ("crc32", "md5", "sha1", "sha256", "blake3"):
            v = entry[ht]
            if v:
                primary_hashes[(ht, v.upper())] = entry["entry_name"]

    result: list[MatchOp] = []
    reassigned = 0

    for op in ops:
        # Already on primary — keep
        if op.source_path == primary:
            result.append(op)
            continue

        # Same-game romroot — sacred, keep
        if op.status == "in_romroot" and op.source_path in own_romroot_paths:
            result.append(op)
            continue

        # Missing — keep
        if op.status == "missing":
            result.append(op)
            continue

        # Check if primary source also has this track
        key = (op.hash_type, op.hash_value.upper()) if op.hash_value else None
        if key and key in primary_hashes:
            result.append(MatchOp(
                dat_path=op.dat_path,
                system=op.system,
                game_name=op.game_name,
                rom_name=op.rom_name,
                rom_size=op.rom_size,
                source_path=primary,
                source_type="archive_content",
                archive_entry=primary_hashes[key],
                status="matched",
                dat_folder=op.dat_folder,
                hash_type=op.hash_type,
                hash_value=op.hash_value,
            ))
            reassigned += 1
        else:
            result.append(op)

    return result


def _consolidate_game_sources(ops: list[MatchOp], db: CacheDB) -> list[MatchOp]:
    """Apply source consolidation per game across all ops in a DAT."""
    # Group by game
    games: dict[tuple[str, str], list[MatchOp]] = {}
    for op in ops:
        key = (op.system, op.game_name)
        games.setdefault(key, []).append(op)

    result: list[MatchOp] = []
    for (system, game_name), game_ops in games.items():
        result.extend(_consolidate_sources(game_name, system, game_ops, db))

    return result


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
                "blake3": (rom.get("blake3", "") or "").upper(),
            })

    db.load_dat(str(dat_path), system, entries)
    return system


def _try_match_hash(
    db: CacheDB, hash_type: str, hash_value: str,
    game_name: str = "",
) -> tuple[str | None, str | None, str | None]:
    """Try to find a match by hash in scanned files and extracted hashes.

    When multiple sources contain the same hash (common for shared silence
    or pregap tracks), prefers the source whose filename matches game_name,
    then .aaru sources. Hash is always the authority — affinity only breaks
    ties among hash-verified candidates.

    Returns (source_path, source_type, archive_entry) or (None, None, None).
    """
    if not hash_value:
        return None, None, None

    # Try plain scanned files first
    rows = db.find_by_hash(hash_type, hash_value)
    if rows:
        if len(rows) > 1 and game_name:
            row = max(rows, key=lambda r: _source_affinity(r["path"], game_name))
        else:
            row = rows[0]
        if row["is_archive"]:
            return str(row["path"]), "archive", None
        else:
            return str(row["path"]), "plain", None

    # Try archive contents (fully hashed during scan)
    rows = db.find_archive_content_by_hash(hash_type, hash_value)
    if rows:
        if len(rows) > 1 and game_name:
            row = max(
                rows,
                key=lambda r: _source_affinity(r["archive_path"], game_name),
            )
        else:
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

        # Check if already in romroot.
        # CRC32 is excluded — it's a 32-bit checksum, not a cryptographic
        # hash. Collisions are expected (confirmed: truncated files can
        # share CRC32 with the correct full-size file).
        for ht in ("sha1", "md5", "sha256", "blake3"):
            hv = entry[ht]
            if hv:
                existing = db.find_in_romroot(ht, hv, game_name=game_name)
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
            # Try matching by hash — cryptographic hashes only
            matched = False
            for ht in ("sha1", "md5", "sha256", "blake3"):
                hv = entry[ht]
                if not hv:
                    continue

                source_path, source_type, archive_entry = _try_match_hash(
                    db, ht, hv, game_name=game_name,
                )
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

    # Source consolidation: prefer fewest source archives per game
    ops = _consolidate_game_sources(ops, db)

    return ops


def match_all_dats(
    selection_dir: Path, db: CacheDB,
) -> list[tuple[str, str, list[MatchOp]]]:
    """Match all selection DATs against scanned files.

    Recursively discovers DATs in selection_dir. Each DAT's folder path
    relative to selection_dir becomes the dat_folder for romroot placement.

    Returns list of (system, dat_folder, ops) tuples.
    """
    results: list[tuple[str, str, list[MatchOp]]] = []

    if not selection_dir.is_dir():
        return results

    # Validate: no duplicate <name> within the same dat_folder
    seen: dict[tuple[str, str], Path] = {}  # (dat_folder, system) -> dat_path

    for dat_file in sorted(selection_dir.glob("**/*.dat")):
        if not dat_file.is_file():
            continue

        dat_folder = str(dat_file.parent.relative_to(selection_dir))

        rel = dat_file.relative_to(selection_dir)
        print(f"Matching: {rel}", file=sys.stderr)

        try:
            ops = match_dat(dat_file, db)
        except Exception as e:
            print(f"  Skipping (parse error): {e}", file=sys.stderr)
            continue

        if not ops:
            continue

        system = ops[0].system

        # Check for duplicate <name> in same folder
        key = (dat_folder, system)
        if key in seen:
            print(
                f"  ERROR: duplicate <name> '{system}' in folder '{dat_folder}' "
                f"(also in {seen[key].name})",
                file=sys.stderr,
            )
            continue
        seen[key] = dat_file

        # Set dat_folder on all ops
        for op in ops:
            op.dat_folder = dat_folder

        matched = sum(1 for o in ops if o.status == "matched")
        missing = sum(1 for o in ops if o.status == "missing")
        in_romroot = sum(1 for o in ops if o.status == "in_romroot")
        print(
            f"  {system}: {matched} matched, {in_romroot} in romroot, "
            f"{missing} missing",
            file=sys.stderr,
        )

        results.append((system, dat_folder, ops))

    return results


def print_plan(match_results: list[tuple[str, str, list[MatchOp]]]) -> None:
    """Print the match plan summary to stderr."""
    total_games = {"complete": 0, "collectable": 0, "partial": 0, "unavailable": 0}

    for system, dat_folder, ops in sorted(match_results):
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


def count_relocations(
    match_results: list[tuple[str, str, list[MatchOp]]],
    romroot_for_system: callable,
) -> dict[str, int]:
    """Count games that need relocation per system.

    A game needs relocation when it has in_romroot ROMs whose source_path
    is not under the expected target directory.
    """
    counts: dict[str, int] = {}

    for system, dat_folder, ops in match_results:
        games = group_by_game(ops)
        target_dir = romroot_for_system(system) / dat_folder / system

        relocations = 0
        for game in games:
            if not game.existing_ops:
                continue
            source_paths = {
                op.source_path for op in game.existing_ops if op.source_path
            }
            if not source_paths:
                continue

            # Check if any source is outside the expected target_dir
            for sp in source_paths:
                parent = Path(sp).parent
                # Archive mode: parent should be target_dir
                # Directory mode: parent.parent should be target_dir
                if parent != target_dir and parent.parent != target_dir:
                    relocations += 1
                    break

        if relocations:
            counts[system] = relocations

    return counts


def find_orphaned_romroot(
    match_results: list[tuple[str, str, list[MatchOp]]],
    db: CacheDB,
) -> set[str]:
    """Find romroot paths not claimed by any current DAT.

    Compares all paths in romroot_files against the set of paths
    referenced as in_romroot by match results. Paths not claimed
    by any DAT are orphaned.

    Paths already in _orphaned/ are excluded — they are already
    quarantined and scanned separately as an ingest source.
    """
    claimed: set[str] = set()
    for _system, _dat_folder, ops in match_results:
        for op in ops:
            if op.status == "in_romroot" and op.source_path:
                claimed.add(op.source_path)

    all_paths = db.all_romroot_paths()

    # Exclude paths already in _orphaned/ — they are already quarantined
    orphaned_marker = f"/{ORPHANED_DIR_NAME}/"
    active_paths = {p for p in all_paths if orphaned_marker not in p}

    return active_paths - claimed
