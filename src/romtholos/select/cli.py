"""CLI subcommands for the select module."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Annotated

import typer

app = typer.Typer(no_args_is_help=True)


def _load_config(config_path: Path):
    from romtholos.select.config import load_select_config
    return load_select_config(config_path)


@app.command()
def run(
    config: Annotated[Path, typer.Argument(help="Path to config YAML")],
    match: Annotated[
        str | None,
        typer.Option("--match", help="Only process DATs matching this glob"),
    ] = None,
) -> None:
    """Run configured filters on upstream DATs."""
    cfg = _load_config(config)

    from romtholos.select.run import run_selection

    print("=== Selection ===", file=sys.stderr)
    stats = run_selection(cfg, match_glob=match)

    cleaned = stats.get("cleaned", 0)
    clean_str = f", {cleaned} cleaned" if cleaned else ""
    print(
        f"\nDone: {stats['bypassed']} bypassed, {stats['filtered']} filtered, "
        f"{stats['skipped']} skipped, {stats['failed']} failed{clean_str}",
        file=sys.stderr,
    )

    if stats["failed"]:
        raise typer.Exit(code=1)


@app.command()
def status(
    config: Annotated[Path, typer.Argument(help="Path to config YAML")],
) -> None:
    """Show upstream vs selection status."""
    cfg = _load_config(config)

    from romtholos.select.run import selection_status

    results = selection_status(cfg)

    if not results:
        print("No DATs configured.", file=sys.stderr)
        return

    for r in results:
        marker = {
            "current": " ",
            "outdated": "*",
            "missing": "!",
        }.get(r["status"], "?")

        print(f"  [{marker}] {r['upstream']}  ({r['filter']}, {r['status']})")

    current = sum(1 for r in results if r["status"] == "current")
    outdated = sum(1 for r in results if r["status"] == "outdated")
    missing = sum(1 for r in results if r["status"] == "missing")
    print(
        f"\n{current} current, {outdated} outdated, {missing} missing",
        file=sys.stderr,
    )
