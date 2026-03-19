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

# With roundtrip verification for disc images
romtholos collect run config.yaml --verify-roundtrip

# Individual phases
romtholos collect scan config.yaml
romtholos collect plan config.yaml
romtholos collect status config.yaml
```

## Configuration

```yaml
paths:
  selection: /path/to/selection
  romroot: /path/to/romroot
  work_dir: /tmp/romtholos-work
  db_cache: romtholos.db

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

## Dependencies

- [rscf](https://github.com/RomTholos/rscf) — sidecar file library
- [dimg-tool](https://github.com/RomTholos/dimg-tool) — disc image conversion (external binary)
- [retooling](https://github.com/RomTholos/retooling) — DAT filter (separate tool)

## License

MIT
