"""Collector configuration — loaded from YAML."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import strictyaml as sy

# Map YAML mode values to internal source types
_MODE_TO_SOURCE_TYPE = {
    "read-only": "readonly",
    "read-write": "ingest",
    "romroot": "romroot",
}


@dataclass
class SourceDir:
    """A directory to scan for ROMs."""

    path: Path
    source_type: str = "readonly"  # "romroot" | "ingest" | "readonly"


@dataclass
class SystemConfig:
    """Per-system configuration."""

    compression: str = ""  # empty = use default
    compression_map: dict[str, str] = field(default_factory=dict)  # media_type → profile
    partial_fallback: str = ""  # empty = use default
    partial_min_ratio: float = -1.0  # -1 = use default
    romroot_override: Path | None = None


@dataclass
class CollectorConfig:
    """Full collector configuration."""

    # Paths
    selection: Path = Path("selection")
    romroot: Path = Path("romroot")
    work_dir: Path = Path("/tmp/romtholos-work")
    db_cache: Path = Path("collector.db")
    sbi_dir: Path | None = None

    # Sources (includes implicit romroot sources)
    sources: list[SourceDir] = field(default_factory=list)

    # Defaults
    default_compression: str = "zstd-19"
    partial_fallback: str = ""  # empty = skip partial games
    partial_min_ratio: float = 0.0  # minimum fraction of ROMs for partial games
    extraction_cache_mb: int = 2048  # extraction cache size in MiB
    verify_roundtrip: bool = False

    # Per-system
    systems: dict[str, SystemConfig] = field(default_factory=dict)

    # Per-system romroot overrides
    romroot_overrides: dict[str, Path] = field(default_factory=dict)

    def romroot_for_system(self, system: str) -> Path:
        """Resolve the romroot path for a system, considering overrides."""
        if system in self.romroot_overrides:
            return self.romroot_overrides[system]
        sys_config = self.systems.get(system)
        if sys_config and sys_config.romroot_override:
            return sys_config.romroot_override
        return self.romroot

    def compression_for_system(
        self, system: str, media_type: str = "",
    ) -> str:
        """Resolve compression profile for a system and media type.

        Checks media-type-specific override first (compression_map),
        then falls back to the system's default compression, then global.
        """
        sys_config = self.systems.get(system)
        if sys_config:
            if media_type and media_type in sys_config.compression_map:
                return sys_config.compression_map[media_type]
            if sys_config.compression:
                return sys_config.compression
        return self.default_compression

    def partial_fallback_for_system(self, system: str) -> str:
        """Resolve partial fallback profile for a system.

        Returns empty string if no fallback configured (skip partial games).
        """
        sys_config = self.systems.get(system)
        if sys_config and sys_config.partial_fallback:
            return sys_config.partial_fallback
        return self.partial_fallback

    def partial_min_ratio_for_system(self, system: str) -> float:
        """Resolve minimum partial ratio for a system."""
        sys_config = self.systems.get(system)
        if sys_config and sys_config.partial_min_ratio >= 0:
            return sys_config.partial_min_ratio
        return self.partial_min_ratio


_CONFIG_SCHEMA = sy.Map({
    "paths": sy.Map({
        sy.Optional("selection"): sy.Str(),
        sy.Optional("romroot"): sy.Str(),
        sy.Optional("work_dir"): sy.Str(),
        sy.Optional("db_cache"): sy.Str(),
        sy.Optional("metadata"): sy.Str(),
        sy.Optional("upstream"): sy.Str(),
        sy.Optional("sbi_dir"): sy.Str(),
    }),
    sy.Optional("romroot_overrides"): sy.MapPattern(sy.Str(), sy.Str()),
    sy.Optional("sources"): sy.Seq(sy.Map({
        "path": sy.Str(),
        sy.Optional("mode"): sy.Str(),
    })),
    sy.Optional("defaults"): sy.Map({
        sy.Optional("compression"): sy.Str(),
        sy.Optional("partial_fallback"): sy.Str(),
        sy.Optional("partial_min_ratio"): sy.Float(),
        sy.Optional("extraction_cache_mb"): sy.Int(),
        sy.Optional("action"): sy.Str(),
        sy.Optional("languages"): sy.Seq(sy.Str()),
        sy.Optional("include_fallback"): sy.Bool(),
        sy.Optional("verify_roundtrip"): sy.Bool(),
    }),
    sy.Optional("systems"): sy.MapPattern(
        sy.Str(),
        sy.Map({
            sy.Optional("action"): sy.Str(),
            sy.Optional("languages"): sy.Seq(sy.Str()),
            sy.Optional("include_fallback"): sy.Bool(),
            sy.Optional("compression"): sy.Str(),
            sy.Optional("compression_map"): sy.MapPattern(sy.Str(), sy.Str()),
            sy.Optional("partial_fallback"): sy.Str(),
            sy.Optional("partial_min_ratio"): sy.Float(),
            sy.Optional("romroot"): sy.Str(),
        }),
    ),
})


def load_config(config_path: Path) -> CollectorConfig:
    """Load collector configuration from YAML."""
    text = config_path.read_text(encoding="utf-8")
    data = sy.load(text, _CONFIG_SCHEMA).data

    paths = data.get("paths", {})
    defaults = data.get("defaults", {})
    systems_data = data.get("systems", {})
    sources_data = data.get("sources", [])
    overrides_data = data.get("romroot_overrides", {})

    romroot_path = Path(paths.get("romroot", "romroot"))
    romroot_overrides = {k: Path(v) for k, v in overrides_data.items()}

    # Build implicit romroot sources (main + overrides)
    romroot_paths_seen: set[Path] = set()
    implicit_sources: list[SourceDir] = []

    implicit_sources.append(SourceDir(path=romroot_path, source_type="romroot"))
    romroot_paths_seen.add(romroot_path)

    for override_path in romroot_overrides.values():
        if override_path not in romroot_paths_seen:
            implicit_sources.append(
                SourceDir(path=override_path, source_type="romroot")
            )
            romroot_paths_seen.add(override_path)

    # Build explicit sources from config
    explicit_sources: list[SourceDir] = []
    for s in sources_data:
        mode = s.get("mode", "read-only")
        source_type = _MODE_TO_SOURCE_TYPE.get(mode)
        assert source_type is not None, (
            f"Unknown source mode {mode!r}. "
            f"Valid: {', '.join(sorted(_MODE_TO_SOURCE_TYPE))}"
        )
        source_path = Path(s["path"])
        # Skip if already covered by implicit romroot
        if source_path in romroot_paths_seen:
            continue
        explicit_sources.append(
            SourceDir(path=source_path, source_type=source_type)
        )

    systems = {}
    for name, sys_data in systems_data.items():
        compression_map_raw = sys_data.get("compression_map", {})
        systems[name] = SystemConfig(
            compression=sys_data.get("compression", ""),
            compression_map=dict(compression_map_raw),
            partial_fallback=sys_data.get("partial_fallback", ""),
            partial_min_ratio=float(sys_data.get("partial_min_ratio", -1.0)),
            romroot_override=(
                Path(sys_data["romroot"]) if sys_data.get("romroot") else None
            ),
        )

    sbi_dir_str = paths.get("sbi_dir")
    sbi_dir = Path(sbi_dir_str) if sbi_dir_str else None

    return CollectorConfig(
        selection=Path(paths.get("selection", "selection")),
        romroot=romroot_path,
        work_dir=Path(paths.get("work_dir", "/tmp/romtholos-work")),
        db_cache=Path(paths.get("db_cache", "collector.db")),
        sbi_dir=sbi_dir,
        sources=implicit_sources + explicit_sources,
        default_compression=defaults.get("compression", "zstd-19"),
        partial_fallback=defaults.get("partial_fallback", ""),
        partial_min_ratio=float(defaults.get("partial_min_ratio", 0.0)),
        extraction_cache_mb=int(defaults.get("extraction_cache_mb", 2048)),
        verify_roundtrip=defaults.get("verify_roundtrip", False),
        systems=systems,
        romroot_overrides=romroot_overrides,
    )
