"""Unified CLI entry point for RomTholos."""

from __future__ import annotations

import typer

from romtholos import __version__
from romtholos.collect.cli import app as collect_app

app = typer.Typer(
    name="romtholos",
    help="ROM collection manager — collect, render, browse.",
    no_args_is_help=True,
)

app.add_typer(collect_app, name="collect", help="Scan, match, and collect ROMs.")


@app.command()
def version() -> None:
    """Show RomTholos version."""
    print(f"romtholos {__version__}")


if __name__ == "__main__":
    app()
