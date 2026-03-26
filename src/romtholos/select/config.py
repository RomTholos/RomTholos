"""Selection configuration — loaded from YAML."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import strictyaml as sy


@dataclass
class FilterDef:
    """A named filter with a command template."""

    name: str
    command: str  # e.g. "retooling process {input} -o {output} -l {languages}"


@dataclass
class DatEntry:
    """A configured DAT mapping: upstream glob → filter."""

    upstream: str  # glob relative to paths.upstream
    filter: str  # filter name or "bypass"
    filter_args: dict[str, str] = field(default_factory=dict)


@dataclass
class SelectConfig:
    """Full selection configuration."""

    upstream: Path
    selection: Path
    filters: dict[str, FilterDef] = field(default_factory=dict)
    default_filter: str = "bypass"
    default_filter_args: dict[str, str] = field(default_factory=dict)
    dats: list[DatEntry] = field(default_factory=list)


_CONFIG_SCHEMA = sy.Map({
    "paths": sy.Map({
        "upstream": sy.Str(),
        "selection": sy.Str(),
        sy.Optional("metadata"): sy.Str(),
        # Allow other path keys used by collector
        sy.Optional("romroot"): sy.Str(),
        sy.Optional("work_dir"): sy.Str(),
        sy.Optional("db_cache"): sy.Str(),
        sy.Optional("sbi_dir"): sy.Str(),
    }),
    sy.Optional("filters"): sy.MapPattern(
        sy.Str(),
        sy.Map({
            "command": sy.Str(),
        }),
    ),
    sy.Optional("selection"): sy.Map({
        sy.Optional("defaults"): sy.Map({
            sy.Optional("filter"): sy.Str(),
            sy.Optional("filter_args"): sy.MapPattern(sy.Str(), sy.Str()),
        }),
        sy.Optional("dats"): sy.Seq(sy.Map({
            "upstream": sy.Str(),
            sy.Optional("filter"): sy.Str(),
            sy.Optional("filter_args"): sy.MapPattern(sy.Str(), sy.Str()),
        })),
    }),
    # Allow collector sections to coexist
    sy.Optional("romroot_overrides"): sy.Any(),
    sy.Optional("sources"): sy.Any(),
    sy.Optional("defaults"): sy.Any(),
    sy.Optional("systems"): sy.Any(),
})


def load_select_config(config_path: Path) -> SelectConfig:
    """Load selection configuration from YAML."""
    text = config_path.read_text(encoding="utf-8")
    data = sy.load(text, _CONFIG_SCHEMA).data

    paths = data.get("paths", {})
    upstream = Path(paths["upstream"])
    selection = Path(paths["selection"])

    # Parse filter definitions
    filters: dict[str, FilterDef] = {}
    for name, fdata in data.get("filters", {}).items():
        filters[name] = FilterDef(name=name, command=fdata["command"])

    # Parse selection config
    sel = data.get("selection", {})
    sel_defaults = sel.get("defaults", {})
    default_filter = sel_defaults.get("filter", "bypass")
    default_filter_args = dict(sel_defaults.get("filter_args", {}))

    dats: list[DatEntry] = []
    for entry in sel.get("dats", []):
        entry_args = dict(entry.get("filter_args", {}))
        dats.append(DatEntry(
            upstream=entry["upstream"],
            filter=entry.get("filter", default_filter),
            filter_args={**default_filter_args, **entry_args},
        ))

    return SelectConfig(
        upstream=upstream,
        selection=selection,
        filters=filters,
        default_filter=default_filter,
        default_filter_args=default_filter_args,
        dats=dats,
    )
