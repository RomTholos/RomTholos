"""Selection runner — execute filters on upstream DATs."""

from __future__ import annotations

import shlex
import shutil
import subprocess
import sys
from pathlib import Path

from romtholos.select.config import DatEntry, SelectConfig


def resolve_dats(config: SelectConfig) -> list[tuple[DatEntry, list[Path]]]:
    """Discover all upstream DATs and resolve filter assignments.

    Auto-discovers all *.dat files in the upstream tree. Each file is
    matched against configured dats entries (by glob). Unconfigured
    DATs use the default filter (bypass).

    Returns list of (DatEntry, [matched_paths]) tuples. Configured
    entries that match no files emit a warning via their original glob.
    """
    from fnmatch import fnmatch

    # Discover all DATs in upstream
    all_dats = sorted(
        p for p in config.upstream.rglob("*.dat")
        if p.is_file() and p.suffix == ".dat"
    )

    # Build lookup: which configured entry claims each DAT?
    # A DAT is claimed by the first matching config entry.
    claimed: dict[Path, DatEntry] = {}
    config_matched: dict[int, list[Path]] = {i: [] for i in range(len(config.dats))}

    for dat_path in all_dats:
        rel = str(dat_path.relative_to(config.upstream))
        for i, entry in enumerate(config.dats):
            if fnmatch(rel, entry.upstream):
                claimed[dat_path] = entry
                config_matched[i].append(dat_path)
                break

    # Warn for configured globs that matched nothing
    for i, entry in enumerate(config.dats):
        if not config_matched[i]:
            import sys
            print(
                f"  Warning: no DAT files match '{entry.upstream}'",
                file=sys.stderr,
            )

    # Build result: configured entries first (in config order),
    # then unconfigured DATs with default filter.
    results: list[tuple[DatEntry, list[Path]]] = []
    seen: set[Path] = set()

    for i, entry in enumerate(config.dats):
        if config_matched[i]:
            results.append((entry, config_matched[i]))
            seen.update(config_matched[i])

    # Unconfigured DATs get default filter
    unconfigured = [p for p in all_dats if p not in seen]
    if unconfigured:
        default_entry = DatEntry(
            upstream="**/*.dat",
            filter=config.default_filter,
            filter_args=dict(config.default_filter_args),
        )
        results.append((default_entry, unconfigured))

    return results


def _output_path(config: SelectConfig, upstream_path: Path) -> Path:
    """Compute selection output path mirroring the upstream relative path."""
    rel = upstream_path.relative_to(config.upstream)
    return config.selection / rel


def _run_bypass(upstream_path: Path, output_path: Path) -> None:
    """Copy a DAT file as-is."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(upstream_path, output_path)


def _run_filter(
    command_template: str,
    upstream_path: Path,
    output_path: Path,
    filter_args: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    """Run an external filter command."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Substitute placeholders, then split into argv
    cmd = command_template.format(
        input=shlex.quote(str(upstream_path)),
        output=shlex.quote(str(output_path)),
        **filter_args,
    )
    argv = shlex.split(cmd)

    return subprocess.run(
        argv,
        check=True,
        capture_output=True,
        text=True,
    )


def run_selection(
    config: SelectConfig,
    *,
    match_glob: str | None = None,
) -> dict[str, int]:
    """Run all configured selection filters.

    After processing, removes stale files from selection/ that were not
    produced in this run. The selection directory is owned by RomTholos —
    only upstream and config are manually curated inputs.

    Args:
        config: Selection configuration.
        match_glob: If set, only process DATs whose upstream path matches.
            Cleanup is skipped when match_glob is set (partial run).

    Returns:
        Summary counts: bypassed, filtered, skipped, failed, cleaned.
    """
    stats = {"bypassed": 0, "filtered": 0, "skipped": 0, "failed": 0,
             "cleaned": 0}

    resolved = resolve_dats(config)
    produced: set[Path] = set()

    for entry, dat_files in resolved:
        if not dat_files:
            print(
                f"  Warning: no DAT files match '{entry.upstream}'",
                file=sys.stderr,
            )
            stats["skipped"] += 1
            continue

        for dat_path in dat_files:
            # Apply --match filter if specified
            rel_path = dat_path.relative_to(config.upstream)
            if match_glob is not None:
                from fnmatch import fnmatch
                if not fnmatch(str(rel_path), match_glob):
                    continue

            output_path = _output_path(config, dat_path)

            if entry.filter == "bypass":
                print(f"  Bypass: {rel_path}", file=sys.stderr)
                _run_bypass(dat_path, output_path)
                stats["bypassed"] += 1

            else:
                filter_def = config.filters.get(entry.filter)
                assert filter_def is not None, (
                    f"Unknown filter '{entry.filter}'. "
                    f"Defined filters: {', '.join(sorted(config.filters)) or '(none)'}"
                )

                print(
                    f"  Filter ({entry.filter}): {rel_path}",
                    file=sys.stderr,
                )
                try:
                    result = _run_filter(
                        filter_def.command,
                        dat_path,
                        output_path,
                        entry.filter_args,
                    )
                    if result.stderr:
                        print(result.stderr, file=sys.stderr, end="")
                    stats["filtered"] += 1
                except subprocess.CalledProcessError as e:
                    print(
                        f"  FAILED: {rel_path}: {e}",
                        file=sys.stderr,
                    )
                    if e.stderr:
                        print(e.stderr, file=sys.stderr, end="")
                    stats["failed"] += 1

            produced.add(output_path)

    # Clean stale files — skip when running partial (--match)
    if match_glob is None and config.selection.is_dir():
        stats["cleaned"] = _cleanup_stale(config.selection, produced)

    return stats


def _cleanup_stale(selection_dir: Path, produced: set[Path]) -> int:
    """Remove files in selection/ that were not produced in this run.

    Filters may create companion files (e.g. .report.json) alongside
    DATs. A file is kept if its corresponding .dat was produced.
    Empty directories are removed after cleanup.

    Returns count of removed files.
    """
    removed = 0

    for path in sorted(selection_dir.rglob("*")):
        if not path.is_file():
            continue
        if path in produced:
            continue

        # Keep companion files if their .dat was produced
        # e.g. keep "Game.report.json" if "Game.dat" was produced
        stem = path.name
        dat_companion = None
        for suffix in (".report.json",):
            if stem.endswith(suffix):
                dat_name = stem[: -len(suffix)] + ".dat"
                dat_companion = path.parent / dat_name
                break

        if dat_companion is not None and dat_companion in produced:
            continue

        print(f"  Cleanup: {path.relative_to(selection_dir)}", file=sys.stderr)
        path.unlink()
        removed += 1

    # Remove empty directories
    for path in sorted(selection_dir.rglob("*"), reverse=True):
        if path.is_dir() and not any(path.iterdir()):
            path.rmdir()

    return removed


def selection_status(config: SelectConfig) -> list[dict[str, str]]:
    """Compare upstream DATs against selection, return status per DAT.

    Returns list of dicts with keys: upstream, selection, status.
    Status is one of: current, outdated, missing.
    """
    results: list[dict[str, str]] = []

    resolved = resolve_dats(config)

    for entry, dat_files in resolved:
        for dat_path in dat_files:
            rel_path = dat_path.relative_to(config.upstream)
            output_path = _output_path(config, dat_path)

            if not output_path.exists():
                status = "missing"
            elif output_path.stat().st_mtime < dat_path.stat().st_mtime:
                status = "outdated"
            else:
                status = "current"

            results.append({
                "upstream": str(rel_path),
                "selection": str(output_path),
                "filter": entry.filter,
                "status": status,
            })

    return results
