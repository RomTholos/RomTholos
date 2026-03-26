# RomTholos Architecture

Status: **Draft** — selection component implemented, collector functional (scan/match/execute/safety/verify), renderer planned.

## Overview

RomTholos is a ROM management system with three independent components, each owning its domain. All three expose a read API for state inspection and a trigger API for actions. The web UI is the primary interface; CLI is the power-user alternative. Both use the same API.

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│  Selection   │      │  Collector   │      │   Output    │
│              │      │              │      │  (Renderer) │
│ upstream DAT │─────▶│ selection +  │─────▶│ romroot ──▶ │
│ ──▶ filter   │      │ sources ──▶  │      │ output dirs │
│ ──▶ sel. DAT │      │ romroot      │      │             │
└──────┬───────┘      └──────┬───────┘      └──────┬──────┘
       │ read API            │ read API            │ read API
       │ trigger API         │ trigger API         │ trigger API
       └─────────┬───────────┴─────────┬───────────┘
                 │    Web UI / CLI     │
                 │  (same API client)  │
                 └─────────────────────┘
```

### Design Principles

- **Three components, not a pipeline engine.** Each component has its own domain, data model, and execution logic. No shared pipeline abstraction.
- **Read API for information, trigger API for actions.** Components never write into each other's domain. A renderer that needs to know what games exist queries the collector's read API.
- **One process, three modules.** Single Starlette app with route namespaces (`/api/selection/...`, `/api/collector/...`, `/api/output/...`). No microservices.
- **Web UI is primary.** Most users configure everything through the web UI. CLI does the same things via the same API.
- **Plugins are API clients.** No plugin SDK. External tools call the HTTP API like any other client. Parameter schemas declared in a manifest for UI discoverability.

## Component 1: Selection

Transforms upstream DATs into filtered selection DATs. Filters are external CLI tools — RomTholos orchestrates them but owns no filtering logic.

### Directory structure

The user organizes `upstream/` however they want. That folder hierarchy flows through to `selection/` and `romroot/` unchanged. RomTholos does not impose or derive structure.

Within each folder, the user can place one or more DAT files. Each DAT occupies its own subfolder in `romroot/`, named after the DAT's `<name>` field from the XML header. This guarantees no two DATs write to the same romroot location while allowing the user full freedom in upstream organization.

```
upstream/                                       selection/                                      romroot/
  No-Intro/                                       No-Intro/                                      No-Intro/
    Atari - Atari Lynx/                             Atari - Atari Lynx/                            Atari - Atari Lynx/
      ...(BLL).dat       ──bypass──▶                  ...(BLL).dat                                   Atari - Atari Lynx (BLL)/
      ...(LNX).dat       ──bypass──▶                  ...(LNX).dat                                     game1.zip
      ...(LYX).dat       ──bypass──▶                  ...(LYX).dat                                   Atari - Atari Lynx (LNX)/
    GBA.dat              ──bypass──▶                GBA.dat                                            game2.zip
  Redump/                                         Redump/                                          Atari - Atari Lynx (LYX)/
    Sony - PlayStation/                             Sony - PlayStation/                               game3.zip
      ...Datfile.dat     ──filter──▶                  ...Datfile.dat                             Redump/
      ...BIOS.dat        ──bypass──▶                  ...BIOS.dat                                  Sony - PlayStation/
                                                                                                     Sony - PlayStation/
                                                                                                       game.7z
                                                                                                     Sony - PlayStation - BIOS Images/
                                                                                                       bios.bin
```

**Why `<name>` for romroot subfolders:**
- Stable across DAT version updates (filenames include dates and entry counts that change every release)
- Semantically correct — the identity chosen by the DAT provider
- Available in all DAT formats (Logiqx XML, No-Intro XSD, CLRMamePro)
- Already parsed by the collect module

**Validation:** Two DATs with the same `<name>` in the same upstream folder is an error (duplicate/conflicting DATs).

### Flow

1. User places upstream DATs in any folder hierarchy under `upstream/`
2. Config maps upstream DATs (by glob) to a filter command
3. RomTholos runs the filter per DAT file, substituting `{input}` and `{output}`
4. Output lands in `selection/` mirroring the upstream relative path
5. `bypass` copies the DAT as-is (no subprocess)

### Filters

A filter is any CLI tool that reads a DAT file and writes a DAT file. RomTholos doesn't interpret filter arguments — it substitutes `{input}`, `{output}`, and any `{key}` placeholders from `filter_args`, then runs the command.

Named filters are defined in config with a command template:

```yaml
filters:
  retooling:
    command: /path/to/retooling process {input} -o {output} -l {languages} -m {metadata} --fallback
  my-custom-filter:
    command: /usr/local/bin/my-filter --in {input} --out {output}
```

`{input}` and `{output}` are automatically shell-quoted. Other placeholders are substituted from `filter_args` as-is.

`bypass` is built-in (copy, no command template needed).

### Configuration

```yaml
selection:
  defaults:
    filter: retooling
    filter_args:
      languages: En,De
      metadata: /path/to/retool-clonelists-metadata

  dats:
    # glob — matches all .dat files under this upstream subfolder
    - upstream: "Sony - PlayStation 2/*.dat"
      filter: retooling

    # explicit file, different filter
    - upstream: "Nintendo - SNES/No-Intro.dat"
      filter: my-custom-filter

    # bypass — copy as-is, no filtering
    - upstream: "Microsoft - Xbox 360/*.dat"
      filter: bypass

    # per-DAT filter_args override
    - upstream: "Sega - Mega Drive/*.dat"
      filter: retooling
      filter_args:
        languages: En,De,Ja
```

Upstream paths are relative to `paths.upstream`, globs are resolved at run time. Output path is `paths.selection / <relative path from upstream>`.

Unmatched upstream DATs (not covered by any glob) are ignored — only explicitly configured DATs are processed.

### Collect module integration

The collect module discovers selection DATs by recursively walking `selection/` for `**/*.dat`. It does not need to know about the upstream/filter config — it just reads whatever DATs are in `selection/`.

Each DAT's `<name>` field determines the romroot subfolder within the DAT's parent directory path. The romroot path for a game is: `romroot / <upstream relative folder> / <dat name> / <game>`.

This means: multiple DATs per folder work (each gets its own romroot subfolder), deep hierarchies work, and the selection step is fully decoupled from collect.

Implemented: recursive `**/*.dat` discovery, `dat_folder` field on match ops, romroot path = `romroot/<dat_folder>/<dat_name>/`.

### CLI

```bash
# Run all configured filters
romtholos select run config.yaml

# Run only DATs matching a glob
romtholos select run config.yaml --match "Sony*"

# Show upstream vs selection status
romtholos select status config.yaml
```

### API (read + trigger)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/selection/dats` | GET | List configured DAT entries with upstream/selection status |
| `/api/selection/dats/{id}/games` | GET | List games in a selection DAT |
| `/api/selection/run` | POST | Trigger all configured filters |
| `/api/selection/run/{id}` | POST | Trigger filter for a specific DAT entry |

## Component 2: Collector

Ingests ROM files from sources into romroot. Fixed three-phase pipeline (scan → match → execute). Already implemented — see SPEC.md.

### API (read + trigger)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/collector/status` | GET | DB cache statistics, scan/match/run state |
| `/api/collector/systems` | GET | List systems with match counts (matched/missing/in_romroot) |
| `/api/collector/systems/{system}/games` | GET | Games for a system with status |
| `/api/collector/systems/{system}/games/{game}` | GET | Game detail (ROMs, sources, match status) |
| `/api/collector/romroot/{system}` | GET | Inventory of romroot files for a system |
| `/api/collector/scan` | POST | Trigger scan phase |
| `/api/collector/plan` | POST | Trigger match/plan phase |
| `/api/collector/run` | POST | Trigger full pipeline (scan → match → execute) |

## Component 3: Output (Renderer)

Takes romroot content and produces output folders. Not yet implemented — design TBD.

Key difference from selection: output stages need rich context (game metadata, system info, cover art) and must declare parameter schemas so the web UI can render configuration forms dynamically.

### Open Design Questions

- Stage parameter schema format (for UI form generation)
- Game selection mechanism (explicit list, import, filter, remaining)
- Info layers (covers, descriptions) — how stages declare and consume them
- Storage strategy (symlinks vs copies vs hardlinks)
- Incremental updates vs full rebuild

## Cross-Cutting: Job Tracking

All three components run potentially long-running operations. The web UI needs to show progress and history regardless of when the user connects.

### Event Protocol

```
job_started(component, action, metadata)
job_progress(component, action, current, total, detail)
job_completed(component, action, summary)
job_failed(component, action, error)
```

Delivered to the web UI via SSE (Server-Sent Events). Persisted in the database so the UI can show history and reconnect to in-progress jobs.

### Job State

| Field | Description |
|-------|-------------|
| `id` | Unique job identifier |
| `component` | selection / collector / output |
| `action` | scan / plan / run / filter / render |
| `status` | queued / running / completed / failed |
| `started_at` | Timestamp |
| `completed_at` | Timestamp (null if running) |
| `progress` | Current / total items |
| `detail` | Current item description |
| `summary` | Result summary (on completion) |
| `error` | Error message (on failure) |

## Technology

- **Python** (uv, Starlette, Jinja2)
- **SQLite** (WAL mode, local filesystem only)
- **Docker** deployment for novice users
- **openmun-auth** for authentication (if needed)
- CLI via **typer**, sharing the same underlying library code
