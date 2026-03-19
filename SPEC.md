# Collector Specification

## Overview

The collector ingests ROM files from various sources into romroot, matching them against selection DATs, creating RSCF sidecars, and applying the configured compression profile. It operates in three phases: scan, match, execute.

## Directory Roles

```
Source types:                     Local SSD:                    NAS/target:
  romroot (implicit source)        work_dir (extraction)        romroot (canonical storage)
  ingest (read-write)              db cache (SQLite)
  readonly sources
```

### Source Types

Three source types with different scan behaviors:

| Source type | Scan behavior | Modify |
|-------------|---------------|--------|
| `romroot` | Load hashes from RSCF sidecars (fast path). Populates `romroot_files` table. | Only with `--force-rescan` |
| `ingest` | Hash everything. Mid-download detection (stat before+after). | Never during scan |
| `readonly` | Hash everything. Mid-download detection. | Never |

Romroot is automatically added as a source from the config `paths.romroot` and any `romroot_overrides`. Explicit sources use `mode: read-only` (→ readonly) or `mode: read-write` (→ ingest).

### Work directory

- Should be on **fast local storage** (SSD) for extraction and hashing speed
- Extraction, hashing, and compression happen here
- Compressed output is then copied to target romroot
- Only holds one game's worth of data at a time
- Cleaned after each game is processed

### Romroot (target filesystem)

- Can be a single path or per-system overrides (different mounts for large systems)
- Compressed output is written here after verification in work_dir
- After writing, file is re-read and BLAKE3-verified (catches CIFS/network corruption)
- RSCF sidecar written alongside the verified file

### DB cache

- SQLite on local filesystem (not CIFS — WAL mode requires proper locking)
- Schema versioned via `PRAGMA user_version` — auto-migrates (drop+recreate) on version mismatch
- Rebuilt from RSCF files + DATs if lost — **never the source of truth**
- Change detection: `path + size + mtime_ns + ctime_ns + inode` (5-field check)

### Execute flow per ROM

```
Source (readonly)     Work dir (SSD)                  Target romroot (NAS)
     │                      │                              │
     └──→ [extract] ──→─────┘                              │
                            │                              │
                      [hash 5 types]                       │
                      [verify vs DAT]                      │
                            │                              │
                      [compress] ──→ compressed file        │
                      [hash compressed]                    │
                            │                              │
                      [copy to target] ──→─────────────────┘
                                                           │
                                               [verify: re-read + BLAKE3]
                                                           │
                                               [write RSCF sidecar]
                            │
                      [clean work dir]
```

## Three-Phase Pipeline

### Phase 1: Scan

Walks all configured sources. Different behavior per source type.

**Romroot scan:**
```
for each file in romroot (excluding .rscf files):
    resolve sidecar path (file.rscf)

    if sidecar exists and not force_rescan:
        read_sidecar() → load container + file entry hashes into DB
        populate romroot_files table (enables match to find "in_romroot")
        on RscfError: warn, fall through to hash

    if no sidecar or force_rescan or corrupt:
        hash the file → record in scanned_files
        if force_rescan: rewrite sidecar, delete orphans

    detect orphaned sidecars (.rscf with no source file) → warn
```

**Untrusted source scan (ingest + readonly):**
```
for each scannable file:
    stat → (size, mtime_ns, ctime_ns, inode)

    if archive and unchanged in DB and archive_contents exist:
        skip (archive cache hit)
    elif plain file and unchanged in DB:
        skip

    mid-download detection:
        stat before hash, hash, stat after hash
        if size or mtime changed → warn + skip

    hash_file() → record in scanned_files

    if archive:
        delete stale archive_contents
        extract_recursive() → work_dir
        hash each extracted file → record in archive_contents (all 5 hashes)
        clean extraction subdirectory
```

**Dolphin disc images (RVZ, GCZ, WIA):**

Treated as archive formats during scan. `dolphin-tool convert -f iso` extracts the raw ISO, which is then hashed and stored in `archive_contents`. This allows matching against Redump ISO-based DATs using a single canonical DAT per system — no separate NKit RVZ DATs needed.

Plain `.iso` files are NOT treated as archives — they match DATs directly as plain files.

**Key properties:**
- After scan, DB contains 5 hashes for every file across all sources
- Archive contents are fully hashed (extracted and hashed, not just peeked)
- Dolphin disc images extracted via `dolphin-tool` to reveal inner ISO hashes
- Archive cache: unchanged archives skip re-extraction on subsequent scans
- All DB writes wrapped in batch transactions for performance

**Output:** DB cache populated with:
- `scanned_files`: path, size, mtime_ns, ctime_ns, inode, source_type, 5 hashes
- `archive_contents`: archive_path, entry_name, entry_size, 5 hashes
- `romroot_files`: path, system, game_name, rom_name, 5 hashes, rscf_path

### Phase 2: Match (pure DB lookups)

No file I/O except reading DAT XML files. All hash comparisons use the DB cache.

```
for each selection DAT:
    parse DAT XML → load entries into dat_entries table

    for each ROM entry:
        check romroot_files by hash (sha1 → md5 → crc32) → in_romroot

        if not in romroot:
            search scanned_files by hash → matched (plain or archive)
            search archive_contents by hash → matched (archive_content)

        if no match:
            record as missing
```

**Output:** Match plan with operations:
- `in_romroot`: already collected, skip
- `matched` (plain): source file matches, ready to collect
- `matched` (archive_content): file inside archive matches, extract during execute
- `missing`: no source found

### Phase 3: Execute (writes to romroot)

Process the match plan. **Configuration is authoritative** — if the configured compression profile differs from the stored `sidecar.renderer`, the archive is rebuilt or recompressed.

**Action determination per game:**

```
existing = find existing archive/directory in romroot (any extension)

if no existing:
    if new ROMs available: CREATE
    else: NOTHING (unavailable)

elif existing.sidecar.renderer == configured_profile:
    if no new ROMs: SKIP
    elif profile supports append AND extension won't change: APPEND
    else: REBUILD

else:  # profile mismatch — config wins
    if new ROMs: REBUILD
    else: RECOMPRESS
```

Archive discovery scans for any file where `strip_archive_extension(name) == game_name`, regardless of extension. This enables transparent profile transitions (e.g., `.zst` → `.7z`).

**Execute flow per game:**

```
extraction cache created (soft quota, survives between games)

for each game (sorted by system, then name):
    detect media type (cd/dvd/gdi/rom) from ROM extensions
    resolve compression profile (compression_map override → system default)
    if aaru profile + partial game: apply threshold + fallback
    find existing archive (any extension) via sidecar scan
    determine action (SKIP / CREATE / APPEND / REBUILD / RECOMPRESS)

    for action in (CREATE, APPEND, REBUILD, RECOMPRESS):
        1. get ROMs into work_dir (extract via cache, match by hash)
        2. hash in work_dir, verify against DAT entry hash
        3. compress to work_dir/_compressed/ using effective profile
        4. hash compressed output → expected_blake3
        5. copy compressed file to target romroot
        6. re-read from target, verify BLAKE3 matches expected
        7. write RSCF sidecar with container + ROM hashes
        8. cleanup old archive if extension changed
        9. update romroot_files in DB

    clean per-game work subdirs (cache preserved)

cleanup extraction cache
```

**Extraction cache:** Archives are extracted once and reused across games within a system run. Avoids re-extracting the same source `.7z` for every partial game that shares tracks. Soft quota — always caches the current extraction, evicts oldest entries to stay near the configured size target.

**ROM lookup by hash:** Extracted files are matched to DAT entries by content hash, not filename. Archive entry names can differ from DAT ROM names (e.g. `Game (USA).iso` vs `Game (USA, Canada).iso`) — the hash is the only authoritative identifier.

**Profile transitions handled:**
- Archive format change (e.g., `zstd-19` → `7z-16m`): REBUILD/RECOMPRESS, old file+sidecar deleted
- Extension change (e.g., zstd single `.zst` → multi `.tar.zst`): REBUILD, old file deleted
- `none` ↔ archive: directory extracted/compressed, old form removed

**Key properties:**
- Compress to work_dir first, then copy to target — enables meaningful verification
- Post-write verification compares against hash computed before the copy
- Corrupt files are deleted from target, not left as partial state
- RSCF sidecar tracks both container (compressed) and content (original ROM) hashes
- Old archives cleaned up after new archive is verified on target
- Work dir cleaned after each game

## DB Cache Schema

```sql
CREATE TABLE scanned_files (
    path        TEXT PRIMARY KEY,
    size        INTEGER NOT NULL,
    mtime_ns    INTEGER NOT NULL,
    ctime_ns    INTEGER NOT NULL DEFAULT 0,
    inode       INTEGER NOT NULL DEFAULT 0,
    source_type TEXT NOT NULL DEFAULT 'readonly',
    crc32       TEXT,
    md5         TEXT,
    sha1        TEXT,
    sha256      TEXT,
    blake3      TEXT,
    is_archive  INTEGER DEFAULT 0,
    scanned_at  TEXT NOT NULL
);

CREATE TABLE archive_contents (
    archive_path    TEXT NOT NULL,
    entry_name      TEXT NOT NULL,
    entry_size      INTEGER,
    crc32           TEXT,
    md5             TEXT,
    sha1            TEXT,
    sha256          TEXT,
    blake3          TEXT,
    PRIMARY KEY (archive_path, entry_name)
);

CREATE TABLE dat_entries (
    dat_path    TEXT NOT NULL,
    system      TEXT NOT NULL,
    game_name   TEXT NOT NULL,
    rom_name    TEXT NOT NULL,
    rom_size    INTEGER,
    crc32       TEXT,
    md5         TEXT,
    sha1        TEXT,
    sha256      TEXT,
    PRIMARY KEY (dat_path, game_name, rom_name)
);

CREATE TABLE matches (
    dat_path        TEXT NOT NULL,
    game_name       TEXT NOT NULL,
    rom_name        TEXT NOT NULL,
    source_path     TEXT,
    source_type     TEXT,
    archive_entry   TEXT,
    status          TEXT NOT NULL,
    PRIMARY KEY (dat_path, game_name, rom_name)
);

CREATE TABLE romroot_files (
    path        TEXT PRIMARY KEY,
    system      TEXT NOT NULL,
    game_name   TEXT NOT NULL,
    rom_name    TEXT NOT NULL,
    crc32       TEXT,
    md5         TEXT,
    sha1        TEXT,
    sha256      TEXT,
    blake3      TEXT,
    rscf_path   TEXT
);

-- Hash lookup indexes
CREATE INDEX idx_scanned_sha1 ON scanned_files(sha1);
CREATE INDEX idx_scanned_md5 ON scanned_files(md5);
CREATE INDEX idx_scanned_crc32 ON scanned_files(crc32);
CREATE INDEX idx_archive_sha1 ON archive_contents(sha1);
CREATE INDEX idx_archive_md5 ON archive_contents(md5);
CREATE INDEX idx_archive_crc32 ON archive_contents(crc32);
CREATE INDEX idx_dat_sha1 ON dat_entries(sha1);
CREATE INDEX idx_dat_md5 ON dat_entries(md5);
CREATE INDEX idx_dat_crc32 ON dat_entries(crc32);
CREATE INDEX idx_romroot_sha1 ON romroot_files(sha1);
```

## Configuration

```yaml
paths:
  selection: /data/romtholos/selection
  romroot: /data/romtholos/romroot             # implicit romroot source
  work_dir: /tmp/romtholos-work
  db_cache: romtholos.db

# Per-system romroot overrides (also become implicit romroot sources)
romroot_overrides:
  "Sony - PlayStation 3": /mnt/ps3-drive/romroot

# Scan sources (romroot is always added implicitly)
sources:
  - path: /data/romtholos/ingest
    mode: read-write      # → source_type "ingest"
  - path: /data/roms
    mode: read-only        # → source_type "readonly"

defaults:
  compression: zstd-19
  partial_fallback: zstd-12         # profile for partial games when main requires all tracks
  partial_min_ratio: 0.1            # skip partials with < 10% of tracks available
  extraction_cache_mb: 2048         # soft quota for extraction cache (MiB)

systems:
  "Sony - PlayStation":
    compression: 7z-96m
  "Nintendo - GameCube":
    compression: rvz-zstd-19
  "Sony - PlayStation 2":
    compression: aaru-ps2dvd-zstd   # default (most games are DVD)
    compression_map:                # media-type overrides
      cd: aaru-ps2cd-zstd           # CUE/BIN games use CD profile
```

### Media type detection

Games within a single DAT can have different disc formats (e.g. PS2 has both CD and DVD games). The `compression_map` field maps detected media types to profiles, overriding the system default. Detection uses ROM file extensions:

| Media type | Detection | Example |
|-----------|-----------|---------|
| `cd` | `.cue` file present | PS1, PS2 CD, Saturn, Mega CD |
| `dvd` | `.iso` file present | PS2 DVD |
| `gdi` | `.gdi` file present | Dreamcast GD-ROM |
| `rom` | None of the above | Cartridge systems |

Priority: gdi > cd > dvd > rom.

## Compression Profiles

| Profile | Tool | Multi-file | Append | Description |
|---------|------|------------|--------|-------------|
| `zstd-19` | zstd | yes (tar.zst) | no (rebuild) | Level 19, excellent ratio. Single file = `.zst`, multi = `.tar.zst` |
| `zstd-3` | zstd | yes (tar.zst) | no (rebuild) | Level 3, fast compression |
| `7z-96m` | 7z | yes | yes | LZMA2, 96 MiB dictionary, best ratio for large files |
| `7z-16m` | 7z | yes | yes | LZMA2, 16 MiB dictionary, balanced |
| `zip` | zip | yes | yes | Standard deflate, maximum compatibility |
| `torrentzip` | trrntzip | yes | no (rebuild) | Deterministic zip for hash-verified sharing |
| `rvz-zstd-19` | dolphin-tool | no (single disc) | no | GameCube/Wii optimal format |
| `rvz-zstd-5` | dolphin-tool | no (single disc) | no | Faster RVZ variant |
| `rvz-to-iso` | dolphin-tool | no (single disc) | no | Convert to raw ISO (decompression) |
| `none` | cp | yes (directory) | n/a | Passthrough, per-file copies + per-file sidecars |
| `aaru-*-zstd` | dimg-tool | yes (disc image) | no | Aaru disc container with zstd. Systems: ps1, ps2cd, ps2dvd, dc, saturn, megacd, neogeo, pce |

**Dolphin profile optimization:** When the source is an RVZ/GCZ/WIA that already matches the target profile parameters (compression method, level, block size — read via `dolphin-tool header --json`), the file is copied as-is instead of extracting to ISO and recompressing. The RSCF sidecar records the inner ISO hashes from the scan phase.

**Aaru profile optimization:** Same copy-as-is logic applies — when the source `.aaru` already matches the target profile system and codec, it is copied directly.

**Aaru partial game handling:** Aaru profiles require complete disc images (all tracks). Partial games use the `partial_fallback` profile (e.g. `zstd-12`) instead. Games below `partial_min_ratio` (fraction of available ROMs) are skipped entirely.

## Safety

- Sources are never modified during scan (only romroot with `--force-rescan`)
- Per-game work subdirs cleaned after each game; extraction cache cleaned after all games
- Compressed output verified after copy to target (BLAKE3 comparison)
- Corrupt target files deleted, not left as partial state
- Archive extraction has zip bomb protection (ratio, depth, size limits)
- Single-file decompression streams to disk (no OOM on large files)
- Mid-download detection prevents recording unstable files
- DB cache is disposable — rebuild from `romroot/*.rscf` + selection DATs
- Schema auto-migrates on version mismatch (drop+recreate)

## CLI

```bash
# Full pipeline: scan, match, execute
collector run config.yaml

# Scan only (populate DB cache)
collector scan config.yaml
collector scan config.yaml --force-rescan   # rebuild romroot sidecars

# Match only (show plan without executing)
collector plan config.yaml

# Show DB cache status
collector status config.yaml
```

## Known Limitations

- DAT parsing only handles `<game>` elements, not `<machine>` (MAME-style DATs).
- No `rebuild-cache` or `execute` standalone commands yet.
- Dolphin disc image extraction requires `dolphin-tool` (native binary or Dolphin Emulator flatpak).
