# RomTholos

ROM collection manager. CLI-first pipeline for scanning, matching, and collecting ROMs into a canonical romroot with RSCF sidecar verification.

## Pipeline

```
upstream → [curate] → selection → [collect] → romroot → [render] → output
```

- **Curate**: Filter DATs by language/region ([retooling](https://github.com/RomTholos/retooling))
- **Collect**: Scan sources, match against DATs, compress to disc image formats
- **Render**: Format conversion for emulators (planned)

## Installation

```bash
pip install romtholos
```

Requires [dimg-tool](https://github.com/RomTholos/dimg-tool) on PATH for disc image conversion, and [dolphin-tool](https://dolphin-emu.org/) for GameCube/Wii RVZ.

## Usage

```bash
# Full pipeline: scan, match, collect
romtholos collect run config.yaml

# With roundtrip verification (extract after compress, verify ROM BLAKE3)
romtholos collect run config.yaml --verify-roundtrip

# Individual phases
romtholos collect scan config.yaml
romtholos collect plan config.yaml
romtholos collect status config.yaml

# Verify romroot integrity (container BLAKE3 vs sidecar)
romtholos collect verify config.yaml
```

## Configuration

```yaml
paths:
  selection: /path/to/selection
  romroot: /path/to/romroot
  work_dir: /tmp/romtholos-work
  db_cache: romtholos.db
  db_backup_dir: /path/to/backup  # optional, default: next to db_cache

sources:
  - path: /path/to/roms
    mode: read-only

defaults:
  compression: zstd-19
  partial_fallback: zstd-12
  partial_min_ratio: 0.1
  extraction_cache_mb: 2048

systems:
  "Sony - PlayStation":
    compression: aaru-ps1-zstd
  "Sony - PlayStation 2":
    compression: aaru-ps2dvd-zstd
    compression_map:
      cd: aaru-ps2cd-zstd
```

## Compression profiles

| Profile | Format | Systems |
|---------|--------|---------|
| `zstd-19` | tar.zst | Cartridge ROMs |
| `7z-96m` | 7z LZMA2 | General purpose |
| `rvz-zstd-19` | RVZ | GameCube, Wii |
| `aaru-*-zstd` | .aaru | PS1, PS2, Dreamcast, Saturn, Mega CD, PCE CD, Neo Geo CD |
| `none` | Directory | Uncompressed |

## Workflow: Ingesting a new system

Step-by-step for adding a new system to RomTholos.

### 1. Copy the DAT to upstream

Place the latest `.dat` into the upstream tree, mirroring the DAT provider structure:

```bash
mkdir -p "upstream/<Provider>/<System Name>"
cp "<System Name> (<date>).dat" "upstream/<Provider>/<System Name>/"
```

### 2. Add DAT to selection config

Add an entry under `selection.dats`:

```yaml
- upstream: "<Provider>/<System Name>/*.dat"
  filter: bypass        # cartridge systems — full set, no filtering
```

Use `filter: retooling` for disc-based systems where size makes full collection impractical.

### 3. Run selection

```bash
romtholos select run config.yaml
```

Verify the selection DAT appears under the selection directory.

### 4. Add source to collector config

Add the ROM source directory to `collector.yaml`:

```yaml
sources:
  - path: /path/to/source/roms
    mode: disposal    # auto-delete after verified collection
```

Source modes: `disposal` (delete after collect), `read-write` (keep, write sidecars), `read-only` (keep, no writes).

For cartridge systems, the default `zstd-19` compression applies. For disc systems, add a `systems:` entry with the appropriate `aaru-*` or `rvz-*` profile.

### 5. Run collect

```bash
romtholos collect run collector.yaml
```

Scans the source, extracts archives, matches ROMs by hash against the DAT, compresses to the target profile, writes to romroot with RSCF sidecars, and (for disposal mode) deletes source files after verified write.

### 6. Verify

- Romroot has `N` compressed files + `N` `.rscf` sidecars
- Disposal source directory is empty (if applicable)
- Spot-check: decompress a ROM and compare its hash against the DAT entry

## Dependencies

- [rscf](https://github.com/RomTholos/rscf) — sidecar file library
- [dimg-tool](https://github.com/RomTholos/dimg-tool) — disc image conversion (external binary)
- [retooling](https://github.com/RomTholos/retooling) — DAT filter (separate tool)

## License

MIT
