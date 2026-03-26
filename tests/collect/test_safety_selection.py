"""Selection safety — FILE_SAFETY.md § Directory Roles / selection.

The selection directory contains filtered DATs produced by `select run`.
Stale files (DATs no longer produced by current config) are cleaned on
full runs. Partial runs (--match) must NOT clean stale files — they only
process a subset and cannot know what's still valid.

Guarantees tested:
- Stale DATs removed on full run
- Partial runs skip cleanup
"""

from __future__ import annotations

from pathlib import Path

from romtholos.select.config import SelectConfig, DatEntry
from romtholos.select.run import run_selection

from .conftest import make_dat


class TestStaleSelectionCleanup:
    """Full select runs must clean DATs no longer in config."""

    def test_stale_dat_removed(self, env):
        """DAT removed from upstream is deleted from selection on full run."""
        upstream = env.source  # reuse as upstream dir

        make_dat(upstream / "Sys1.dat", "Sys1", [])
        make_dat(upstream / "Sys2.dat", "Sys2", [])

        config = SelectConfig(
            upstream=upstream,
            selection=env.selection,
            filters={},
            default_filter="bypass",
            default_filter_args={},
            dats=[],
        )

        stats1 = run_selection(config)
        assert stats1["bypassed"] == 2
        assert (env.selection / "Sys1.dat").exists()
        assert (env.selection / "Sys2.dat").exists()

        # Remove Sys2 from upstream — should be cleaned from selection
        (upstream / "Sys2.dat").unlink()

        stats2 = run_selection(config)
        assert stats2["bypassed"] == 1
        assert stats2["cleaned"] == 1
        assert (env.selection / "Sys1.dat").exists()
        assert not (env.selection / "Sys2.dat").exists()

    def test_stale_dat_in_subdirectory(self, env):
        """Stale DATs in subdirectories are also cleaned."""
        upstream = env.source

        (upstream / "sub").mkdir(parents=True)
        make_dat(upstream / "Sys1.dat", "Sys1", [])
        make_dat(upstream / "sub" / "Sys2.dat", "Sys2", [])

        config = SelectConfig(
            upstream=upstream,
            selection=env.selection,
            filters={},
            default_filter="bypass",
            default_filter_args={},
            dats=[],
        )

        run_selection(config)
        assert (env.selection / "sub" / "Sys2.dat").exists()

        # Remove sub/Sys2 from upstream
        (upstream / "sub" / "Sys2.dat").unlink()

        stats = run_selection(config)
        assert stats["cleaned"] == 1
        assert not (env.selection / "sub" / "Sys2.dat").exists()


class TestPartialRunSafety:
    """Partial runs (--match) must NOT clean stale files."""

    def test_partial_run_skips_cleanup(self, env):
        """--match partial run must not delete files outside the match glob."""
        upstream = env.source

        make_dat(upstream / "Sys1.dat", "Sys1", [])
        make_dat(upstream / "Sys2.dat", "Sys2", [])

        config = SelectConfig(
            upstream=upstream,
            selection=env.selection,
            filters={},
            default_filter="bypass",
            default_filter_args={},
            dats=[
                DatEntry(upstream="Sys1.dat", filter="bypass", filter_args={}),
                DatEntry(upstream="Sys2.dat", filter="bypass", filter_args={}),
            ],
        )

        run_selection(config)

        # Partial run with --match — must NOT clean Sys2
        stats = run_selection(config, match_glob="Sys1*")
        assert stats.get("cleaned", 0) == 0
        assert (env.selection / "Sys2.dat").exists()
