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
) -> None:
    """Phase 1: Scan all sources, populate DB cache."""
    cfg = _load_config(config)

    from romtholos.collect.db import CacheDB
    from romtholos.collect.scan import scan_all

    print("=== Scan Phase ===", file=sys.stderr)

    with CacheDB(cfg.db_cache) as db:
        results = scan_all(
            cfg.sources, db, cfg.work_dir, force_rescan=force_rescan,
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
    from romtholos.collect.match import match_all_dats, print_plan

    print("=== Match Phase ===", file=sys.stderr)

    with CacheDB(cfg.db_cache) as db:
        ops_by_system = match_all_dats(cfg.selection, db)
        print_plan(ops_by_system)


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

    from romtholos.collect.db import CacheDB
    from romtholos.collect.execute import execute_plan
    from romtholos.collect.match import group_by_game, match_all_dats, print_plan
    from romtholos.collect.scan import scan_all

    # Phase 1: Scan
    print("=== Phase 1: Scan ===", file=sys.stderr)
    with CacheDB(cfg.db_cache) as db:
        scan_all(cfg.sources, db, cfg.work_dir, force_rescan=force_rescan)

        # Phase 2: Match
        print("\n=== Phase 2: Match ===", file=sys.stderr)
        ops_by_system = match_all_dats(cfg.selection, db)
        print_plan(ops_by_system)

        # Phase 3: Execute
        print("\n=== Phase 3: Execute ===", file=sys.stderr)

        source_modes = {str(s.path): s.source_type for s in cfg.sources}

        for system, ops in sorted(ops_by_system.items()):
            game_plans = group_by_game(ops)

            compression = cfg.compression_for_system(system)
            target_root = cfg.romroot_for_system(system)
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
            )

            print(
                f"  Done: {result['processed']} processed, "
                f"{result['skipped']} skipped, "
                f"{result['failed']} failed, {result['missing']} missing",
                file=sys.stderr,
            )

        print("\n=== Pipeline Complete ===", file=sys.stderr)
        final = db.stats()
        print(f"  Romroot: {final['romroot_files']} files", file=sys.stderr)


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


