"""CLI subcommands for the collect module."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Annotated

import typer

app = typer.Typer(no_args_is_help=True)


def _load_config(config_path: Path):
    from romtholos.collect.config import load_config
    return load_config(config_path)


@app.command()
def scan(
    config: Annotated[Path, typer.Argument(help="Path to config YAML")],
    force_rescan: Annotated[
        bool, typer.Option("--force-rescan", help="Rebuild romroot RSCF sidecars")
    ] = False,
    scan_path: Annotated[
        Path | None, typer.Option(
            "--path",
            help="Restrict scanning to this subfolder of a configured source",
        )
    ] = None,
) -> None:
    """Phase 1: Scan all sources, populate DB cache."""
    cfg = _load_config(config)

    from romtholos.collect.backup import backup_db
    from romtholos.collect.db import CacheDB
    from romtholos.collect.scan import scan_all

    # Validate --path: must be within a configured source
    path_filter: Path | None = None
    if scan_path is not None:
        path_filter = scan_path.resolve()
        if not any(path_filter.is_relative_to(s.path) for s in cfg.sources):
            print(
                f"Error: --path {path_filter} is not within any configured source",
                file=sys.stderr,
            )
            raise typer.Exit(code=1)

    backup_db(cfg.db_cache, cfg.db_backup_dir)

    print("=== Scan Phase ===", file=sys.stderr)

    with CacheDB(cfg.db_cache) as db:
        results = scan_all(
            cfg.sources, db, cfg.work_dir,
            force_rescan=force_rescan, path_filter=path_filter,
        )

        total_hashed = sum(s.files_hashed for s in results.values())
        total_sidecar = sum(s.files_from_sidecar for s in results.values())
        total_skipped = sum(s.files_skipped for s in results.values())
        print(
            f"\nScan complete: {total_hashed} hashed, "
            f"{total_sidecar} from sidecar, {total_skipped} skipped",
            file=sys.stderr,
        )

        stats = db.stats()
        print(
            f"  DB: {stats['scanned_files']} files, "
            f"{stats['archive_contents']} archive entries",
            file=sys.stderr,
        )


@app.command()
def plan(
    config: Annotated[Path, typer.Argument(help="Path to config YAML")],
) -> None:
    """Phase 2: Match scanned files against DATs, show plan."""
    cfg = _load_config(config)

    from romtholos.collect.db import CacheDB
    from romtholos.collect.match import (
        count_relocations, find_orphaned_romroot, match_all_dats, print_plan,
    )

    print("=== Match Phase ===", file=sys.stderr)

    with CacheDB(cfg.db_cache) as db:
        match_results = match_all_dats(cfg.selection, db)
        print_plan(match_results)

        relocations = count_relocations(match_results, cfg.romroot_for_system)
        if relocations:
            total = sum(relocations.values())
            print(f"\n  Pending relocations: {total} game(s)", file=sys.stderr)
            for system, count in sorted(relocations.items()):
                print(f"    {system}: {count}", file=sys.stderr)

        orphans = find_orphaned_romroot(match_results, db)
        if orphans:
            print(
                f"\n  Orphaned romroot files: {len(orphans)} "
                f"(will be quarantined on next run)",
                file=sys.stderr,
            )


@app.command()
def execute(
    config: Annotated[Path, typer.Argument(help="Path to config YAML")],
    verify_roundtrip: Annotated[
        bool, typer.Option("--verify-roundtrip",
                           help="Roundtrip verify aaru disc image conversions")
    ] = False,
    limit: Annotated[
        int, typer.Option("--limit",
                          help="Stop after processing N games (0 = no limit)")
    ] = 0,
    system: Annotated[
        str | None, typer.Option("--system",
                                 help="Process only this system (DAT <name>)")
    ] = None,
) -> None:
    """Execute collection using pre-existing scan data.

    Requires a prior 'collect scan'. Re-runs match (sub-second) to ensure
    consistency, then executes collection and quarantines orphans.

    Use --system to restrict to a single system (quarantine is skipped
    when filtering by system since it requires full DAT coverage).
    """
    cfg = _load_config(config)

    from romtholos.collect.backup import backup_db
    from romtholos.collect.db import CacheDB
    from romtholos.collect.lock import CollectorLockError, acquire_lock, release_lock
    from romtholos.collect.match import match_all_dats, print_plan

    try:
        lock_path = acquire_lock(cfg.romroot)
    except CollectorLockError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise typer.Exit(code=1) from None

    try:
        backup_db(cfg.db_cache, cfg.db_backup_dir)

        with CacheDB(cfg.db_cache) as db:
            # Precondition: DB must have scan data
            stats = db.stats()
            if stats['scanned_files'] == 0:
                print(
                    "Error: no scanned files in DB. "
                    "Run 'collect scan' first.",
                    file=sys.stderr,
                )
                raise typer.Exit(code=1)

            # Match (cheap, always fresh from current DB state)
            print("=== Match ===", file=sys.stderr)
            match_results = match_all_dats(cfg.selection, db)

            # Filter by system if requested
            if system is not None:
                match_results = [
                    (s, d, ops) for s, d, ops in match_results if s == system
                ]
                if not match_results:
                    print(
                        f"Error: no DAT found for system '{system}'",
                        file=sys.stderr,
                    )
                    raise typer.Exit(code=1)

            print_plan(match_results)

            skip_quarantine = system is not None
            if skip_quarantine:
                print(
                    "  (quarantine skipped — requires full DAT coverage)",
                    file=sys.stderr,
                )

            _execute_and_quarantine(
                cfg, db, match_results, verify_roundtrip, limit,
                skip_quarantine=skip_quarantine,
            )
    finally:
        release_lock(lock_path)


@app.command()
def run(
    config: Annotated[Path, typer.Argument(help="Path to config YAML")],
    force_rescan: Annotated[
        bool, typer.Option("--force-rescan", help="Rebuild romroot RSCF sidecars")
    ] = False,
    verify_roundtrip: Annotated[
        bool, typer.Option("--verify-roundtrip",
                           help="Roundtrip verify aaru disc image conversions")
    ] = False,
    limit: Annotated[
        int, typer.Option("--limit",
                          help="Stop after processing N games (0 = no limit)")
    ] = 0,
) -> None:
    """Full pipeline: scan, match, execute."""
    cfg = _load_config(config)

    from romtholos.collect.lock import CollectorLockError, acquire_lock, release_lock

    try:
        lock_path = acquire_lock(cfg.romroot)
    except CollectorLockError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise typer.Exit(code=1) from None

    try:
        _run_pipeline(cfg, force_rescan, verify_roundtrip, limit)
    finally:
        release_lock(lock_path)


def _execute_and_quarantine(
    cfg, db, match_results, verify_roundtrip: bool, limit: int,
    *, skip_quarantine: bool = False,
) -> None:
    """Execute match results and optionally quarantine orphans.

    Shared by ``run`` (full pipeline) and ``execute`` (standalone).
    """
    from romtholos.collect.execute import execute_plan, quarantine_orphans
    from romtholos.collect.match import (
        find_orphaned_romroot, group_by_game, match_all_dats,
    )

    print("\n=== Execute ===", file=sys.stderr)

    source_modes = {str(s.path): s.source_type for s in cfg.sources}

    for system, dat_folder, ops in sorted(match_results):
        game_plans = group_by_game(ops)

        compression = cfg.compression_for_system(system)
        target_root = cfg.romroot_for_system(system) / dat_folder / system
        new_roms = sum(len(g.new_ops) for g in game_plans)

        # Get media-type compression overrides for this system
        sys_config = cfg.systems.get(system)
        comp_map = sys_config.compression_map if sys_config else {}

        print(
            f"\nProcessing: {system} "
            f"({len(game_plans)} games, {new_roms} new ROMs, {compression})",
            file=sys.stderr,
        )

        effective_verify = verify_roundtrip or cfg.verify_roundtrip
        fallback = cfg.partial_fallback_for_system(system)
        min_ratio = cfg.partial_min_ratio_for_system(system)

        result = execute_plan(
            game_plans=game_plans,
            romroot=target_root,
            work_dir=cfg.work_dir,
            compression_profile=compression,
            db=db,
            source_modes=source_modes,
            romroot_overrides={},
            sbi_dir=cfg.sbi_dir,
            verify_roundtrip=effective_verify,
            limit=limit,
            partial_fallback=fallback,
            partial_min_ratio=min_ratio,
            extraction_cache_mb=cfg.extraction_cache_mb,
            compression_map=comp_map,
            global_fallback=cfg.default_compression,
        )

        relocated = result.get('relocated', 0)
        reloc_str = f", {relocated} relocated" if relocated else ""
        print(
            f"  Done: {result['processed']} processed, "
            f"{result['skipped']} skipped{reloc_str}, "
            f"{result['failed']} failed, {result['missing']} missing",
            file=sys.stderr,
        )

    # Quarantine orphans — re-match to capture newly collected files
    if not skip_quarantine:
        fresh_results = match_all_dats(cfg.selection, db)
        orphans = find_orphaned_romroot(fresh_results, db)
        if orphans:
            print("\n=== Quarantine ===", file=sys.stderr)
            n = quarantine_orphans(orphans, cfg.romroot, db)
            if n:
                print(f"  {n} game(s) quarantined to _orphaned/", file=sys.stderr)

    print("\n=== Complete ===", file=sys.stderr)
    final = db.stats()
    print(f"  Romroot: {final['romroot_files']} files", file=sys.stderr)


def _run_pipeline(cfg, force_rescan: bool, verify_roundtrip: bool, limit: int) -> None:
    """Execute the full pipeline (called under lock)."""
    from romtholos.collect.backup import backup_db
    from romtholos.collect.db import CacheDB
    from romtholos.collect.match import match_all_dats, print_plan
    from romtholos.collect.scan import scan_all

    backup_db(cfg.db_cache, cfg.db_backup_dir)

    with CacheDB(cfg.db_cache) as db:
        # Phase 1: Scan
        print("=== Phase 1: Scan ===", file=sys.stderr)
        scan_all(cfg.sources, db, cfg.work_dir, force_rescan=force_rescan)

        # Phase 2: Match
        print("\n=== Phase 2: Match ===", file=sys.stderr)
        match_results = match_all_dats(cfg.selection, db)
        print_plan(match_results)

        # Phase 3+4: Execute + Quarantine
        _execute_and_quarantine(cfg, db, match_results, verify_roundtrip, limit)


@app.command()
def verify(
    config: Annotated[Path, typer.Argument(help="Path to config YAML")],
) -> None:
    """Verify romroot integrity — detect post-mortem corruption.

    Re-reads every romroot archive and compares BLAKE3 against the stored
    RSCF sidecar. Reports corrupt files with per-ROM recovery status.

    Read-only: never modifies files or the database.
    Exit code 0 = all clean, 1 = corruption found.
    """
    cfg = _load_config(config)

    from romtholos.collect.db import CacheDB
    from romtholos.collect.verify import verify_romroot

    print("=== Verify Romroot Integrity ===", file=sys.stderr)

    with CacheDB(cfg.db_cache) as db:
        result = verify_romroot(
            romroot=cfg.romroot,
            db=db,
            romroot_overrides=cfg.romroot_overrides,
        )

    print(file=sys.stderr)
    print(
        f"Verified: {result.verified}, "
        f"Corrupt: {result.corrupt}, "
        f"Missing sidecar: {result.missing_sidecar}",
        file=sys.stderr,
    )

    if result.corrupt_files:
        total_roms = sum(len(cf.roms) for cf in result.corrupt_files)
        recoverable = sum(cf.recoverable_count for cf in result.corrupt_files)
        lost = sum(cf.lost_count for cf in result.corrupt_files)
        print(
            f"ROMs affected: {total_roms} total, "
            f"{recoverable} recoverable, {lost} require manual intervention",
            file=sys.stderr,
        )

    if not result.clean:
        raise typer.Exit(code=1)


@app.command()
def status(
    config: Annotated[Path, typer.Argument(help="Path to config YAML")],
) -> None:
    """Show current status of DB cache."""
    cfg = _load_config(config)

    from romtholos.collect.db import CacheDB

    with CacheDB(cfg.db_cache) as db:
        stats = db.stats()
        print(f"Scanned files:    {stats['scanned_files']}")
        print(f"Archive entries:  {stats['archive_contents']}")
        print(f"DAT entries:      {stats['dat_entries']}")
        print(f"Matched:          {stats['matched']}")
        print(f"Missing:          {stats['missing']}")
        print(f"Romroot files:    {stats['romroot_files']}")


def _format_size(size_bytes: int) -> str:
    """Human-readable file size."""
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.1f} {unit}" if unit != "B" else f"{size_bytes} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PiB"


@app.command()
def purge(
    config: Annotated[Path, typer.Argument(help="Path to config YAML")],
    yes: Annotated[
        bool, typer.Option("--yes", "-y", help="Skip confirmation prompt")
    ] = False,
    path_filter: Annotated[
        str | None, typer.Option("--path", help="Purge only orphan matching this path (relative to _orphaned/)")
    ] = None,
) -> None:
    """Delete orphaned files from _orphaned/ after user review."""
    cfg = _load_config(config)

    from romtholos.collect.purge import list_orphans, purge_orphan

    entries = list_orphans(cfg.romroot)
    if not entries:
        print("No orphaned files found.", file=sys.stderr)
        return

    # Filter if --path given
    if path_filter is not None:
        filter_path = Path(path_filter)
        entries = [e for e in entries if e.relative == filter_path]
        if not entries:
            print(f"No orphan matching path: {path_filter}", file=sys.stderr)
            raise typer.Exit(code=1)

    total_size = sum(e.size for e in entries)
    print(f"Orphaned files ({len(entries)}, {_format_size(total_size)}):", file=sys.stderr)
    for entry in entries:
        kind = "dir " if entry.is_directory else "file"
        print(f"  [{kind}] {entry.relative}  ({_format_size(entry.size)})", file=sys.stderr)

    if not yes:
        confirm = input(f"\nDelete {len(entries)} orphan(s)? [y/N] ")
        if confirm.lower() not in ("y", "yes"):
            print("Aborted.", file=sys.stderr)
            return

    freed = 0
    for entry in entries:
        freed += purge_orphan(entry, cfg.romroot)
        print(f"  Deleted: {entry.relative}", file=sys.stderr)

    print(f"\nFreed {_format_size(freed)} from {len(entries)} orphan(s).", file=sys.stderr)


