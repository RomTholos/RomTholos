# Collect Module Specification

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

| Source type | YAML mode | RSCF sidecars | Scan behavior | Writes | Orphan cleanup |
|-------------|-----------|---------------|---------------|--------|----------------|
| `romroot` | (implicit) | Yes (written by execute) | Stat-cache → sidecar → hash fallback | Execute writes files + sidecars | `--force-rescan` deletes orphan `.rscf` |
| `ingest` | `read-write` | Yes (written by scan) | Stat-cache → sidecar → hash fallback | Scan writes sidecars | Scan deletes orphan `.rscf` |
| `readonly` | `read-only` | No (never writes) | Stat-cache → sidecar read → hash | Never | N/A |
| `disposal` | `disposal` | Yes (same as ingest) | Same as ingest | Deletes source after verified collection | Same as ingest |

**Romroot** is automatically added as a source from `paths.romroot` and any `romroot_overrides`. Explicit sources use `mode: read-only`, `mode: read-write`, or `mode: disposal`.

**Stat-cache** (all types): if a file's `(path, size, mtime_ns, ctime_ns, inode)` matches the DB from a previous scan, it is skipped entirely — no sidecar read, no hashing. This makes warm-DB scans near-instant regardless of source type.

**RSCF sidecars** (romroot + ingest + disposal): sidecars store container metadata and all 5 content hashes. On cold DB (first run or after DB loss), sidecars provide instant hash recovery without re-reading file contents. For romroot, sidecars are written by the execute phase. For ingest/disposal, sidecars are written by the scan phase after hashing. Read-only sources can read existing sidecars (e.g. from a previous ingest run) but never write them.

**Orphan sidecar cleanup**: For any source type that writes sidecars, orphaned `.rscf` files (sidecar with no corresponding source file) must be detected and cleaned. Romroot cleans orphans only during `--force-rescan`. Ingest should clean orphans during normal scan (since it owns the directory).

**Disposal**: extends ingest with post-collection deletion. A source file may only be deleted when ALL conditions are met: (1) content hashes verified in romroot copy, (2) romroot copy BLAKE3-verified after write, (3) RSCF sidecar written in romroot, (4) no other game references the same source file. Shared source archives (containing ROMs for multiple games) are not deleted until all referencing games are collected.

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

**Walk phase** (all source types): single-pass `glob("**/*")` collecting `(path, size, mtime_ns, ctime_ns, inode)` tuples. One stat per file. Sidecars (`.rscf`) collected separately for romroot/ingest.

**Romroot scan:**
```
for each file (chunked, 50 per transaction):
    if stat matches DB (is_unchanged): skip           ← stat-cache, zero I/O

    if sidecar exists and not force_rescan:
        read_sidecar() → load container + file entry hashes into DB
        populate romroot_files table (enables match to find "in_romroot")
        on RscfError: warn, fall through to hash

    if no sidecar or force_rescan or corrupt:
        hash the file → record in scanned_files
        if force_rescan: rewrite sidecar

    detect orphaned sidecars (.rscf with no source file) → warn
    force_rescan: delete orphan sidecars
```

**Untrusted source scan (ingest + readonly):**
```
for each scannable file (chunked, 50 per transaction):
    if archive and stat matches DB and archive_contents exist: skip
    elif plain file and stat matches DB: skip

    [N/total] hash_file()

    mid-download detection:
        compare post-hash stat against walk-collected stat
        if size or mtime changed → warn + skip

    record in scanned_files

    if archive:
        delete stale archive_contents
        extract_recursive() → work_dir
        hash each extracted file → record in archive_contents (all 5 hashes)
        clean extraction subdirectory
```

**Chunked commits**: transactions commit every 50 files instead of all-or-nothing per source. Limits progress loss on interrupt to at most one chunk.

**Dolphin disc images (RVZ, GCZ, WIA):**

Treated as archive formats during scan. `dolphin-tool convert -f iso` extracts the raw ISO, which is then hashed and stored in `archive_contents`. This allows matching against Redump ISO-based DATs using a single canonical DAT per system — no separate NKit RVZ DATs needed.

Plain `.iso` files are NOT treated as archives — they match DATs directly as plain files.

**Key properties:**
- After scan, DB contains 5 hashes for every file across all sources
- Archive contents are fully hashed (extracted and hashed, not just peeked)
- Dolphin disc images extracted via `dolphin-tool` to reveal inner ISO hashes
- Archive cache: unchanged archives skip re-extraction on subsequent scans
- DB writes chunked (50 files per transaction) — limits progress loss on interrupt
- Disc image extractors (dolphin, dimg) skip compression ratio checks — output
  size is medium-determined, not content-determined. Absolute size limit (50 GiB) applies.

**Output:** DB cache populated with:
- `scanned_files`: path, size, mtime_ns, ctime_ns, inode, source_type, 5 hashes
- `archive_contents`: archive_path, entry_name, entry_size, 5 hashes
- `romroot_files`: path, system, game_name, rom_name, 5 hashes, rscf_path

### DAT discovery and romroot path

The match phase discovers selection DATs by recursively walking `selection/` for `**/*.dat`. Each DAT's romroot target path is derived from two things:

1. **Folder path**: the DAT file's parent directory relative to `selection/`
2. **DAT `<name>`**: the `<name>` field from the DAT XML header

Romroot path for a game: `romroot / <relative folder> / <dat name> / <game_name>`.

This allows multiple DATs in the same selection folder (e.g. Redump + BIOS for PS2) — each gets its own romroot subfolder. Two DATs with the same `<name>` in the same folder is a validation error.

Implemented: recursive `**/*.dat` discovery, `dat_folder` threaded through match → execute.

### Phase 2: Match (pure DB lookups)

No file I/O except reading DAT XML files. All hash comparisons use the DB cache.

```
for each selection DAT:
    parse DAT XML → load entries into dat_entries table

    for each ROM entry:
        check romroot_files by hash (sha1 → md5 → sha256 → blake3 → crc32) → in_romroot

        if not in romroot:
            search scanned_files by hash (same order) → matched (plain or archive)
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

if no existing AND game has in_romroot ops:
    detect relocation (game exists at old path in romroot)
    if relocation detected:
        if no new ROMs AND profile matches: RELOCATE (move only, done)
        else: relocate first, then re-discover and fall through

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

**Automatic relocation:** When DATs are reorganized in `selection/` (moved to different folders), the romroot target path changes. The execute phase detects games that exist in romroot at a different path than expected and relocates them — pure filesystem move, no repacking. Works for both archive-mode games (single archive file + sidecar) and directory-mode games (game directory with per-file sidecars). Empty parent directories are cleaned up after the move. The `plan` command shows pending relocations. Relocation can combine with other actions: relocate + append (new ROMs available), relocate + recompress (profile changed).

**Execute flow per game:**

```
extraction cache created (soft quota, survives between games)

for each game (sorted by system, then name):
    detect media type (cd/dvd/gdi/rom) from ROM extensions
    resolve compression profile (compression_map override → system default)
    if aaru profile + partial game: apply threshold + fallback
    find existing archive (any extension) via sidecar scan
    if not found: detect relocation (game at wrong path in romroot)
    determine action (SKIP / CREATE / APPEND / REBUILD / RECOMPRESS)

    for RELOCATE (pure move):
        1. move archive/directory + sidecar(s) to target romroot
        2. update romroot_files in DB (delete old, insert new paths)
        3. clean up empty parent directories

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

**Orphan quarantine:** After execute completes, romroot files not claimed by any current DAT are quarantined to `romroot/_orphaned/`, preserving their relative path structure. DB entries are updated with the new path. Orphaned files remain scannable — if a future DAT claims their hashes, the relocation logic moves them back to the correct romroot location automatically.

**Key properties:**
- Compress to work_dir first, then copy to target — enables meaningful verification
- Post-write verification compares against hash computed before the copy
- Corrupt files are deleted from target, not left as partial state
- RSCF sidecar tracks both container (compressed) and content (original ROM) hashes
- Old archives cleaned up after new archive is verified on target
- Work dir cleaned after each game
- Files removed from DATs are quarantined, never deleted

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
    blake3      TEXT,
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

-- Hash lookup indexes (all 5 types on all tables for flexible matching)
CREATE INDEX idx_scanned_{crc32,md5,sha1,sha256,blake3} ON scanned_files(...);
CREATE INDEX idx_archive_{crc32,md5,sha1,sha256,blake3} ON archive_contents(...);
CREATE INDEX idx_dat_{crc32,md5,sha1,sha256,blake3} ON dat_entries(...);
CREATE INDEX idx_romroot_{crc32,md5,sha1,sha256,blake3} ON romroot_files(...);
CREATE INDEX idx_romroot_game ON romroot_files(system, game_name);
```

## Configuration

```yaml
paths:
  selection: /data/romtholos/selection
  romroot: /data/romtholos/romroot             # implicit romroot source
  work_dir: /tmp/romtholos-work
  db_cache: romtholos.db
  db_backup_dir: /path/to/db-backup  # optional, default: <db_cache parent>/backup/

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

**Profile compatibility:** Each profile declares which media types it can handle (`compatible_media`). Generic profiles (zstd, 7z, zip, torrentzip, none) accept any media type. Disc-specific profiles are restricted:

| Profile | Compatible media |
|---------|-----------------|
| `rvz-*` | dvd |
| `aaru-ps1-*` | cd |
| `aaru-ps2cd-*` | cd |
| `aaru-ps2dvd-*` | dvd |
| `aaru-dc-*` | cd, gdi |
| `aaru-saturn-*` | cd |
| `aaru-megacd-*` | cd |
| `aaru-pce-*` | cd |
| `aaru-neogeo-*` | cd |

When a profile is incompatible with the detected media type, the resolve cascade falls through:

1. `compression_map[media_type]` → check compatible → if not, fall to 2
2. System default profile → check compatible → if not, fall to 3
3. Global `defaults.compression` → always a generic profile

This prevents crashes from disc-specific tools (dolphin-tool, dimg-tool) receiving incompatible input (e.g. a BIOS `.bin` file in a GameCube DAT with `rvz-zstd-19` default).

## DB Backup

The collector DB is backed up automatically before every `scan` and `run` invocation. The backup is unconditional — it always runs when the DB exists and is non-empty.

**Cooldown:** If the newest backup is less than 15 minutes old, the backup is skipped. This prevents redundant copies when running `scan` followed by `run`.

**Tiered rotation:** Backups are retained with decreasing granularity:

| Age | Retention | Max files |
|-----|-----------|-----------|
| 0–1 hour | every 15 min | ~4 |
| 1–24 hours | 1 per hour | ~23 |
| 1–7 days | 1 per day | ~6 |
| 1 week – 6 months | 1 per week | ~25 |
| > 6 months | deleted | — |

**Configuration:** `paths.db_backup_dir` sets the backup directory. Default: `<db_cache parent>/backup/`. Point it to a CIFS mount for snapshot coverage.

## Safety

- Sources are never modified during scan (only romroot with `--force-rescan`)
- Per-game work subdirs cleaned after each game; extraction cache cleaned after all games
- Compressed output verified after copy to target (BLAKE3 comparison)
- Corrupt target files deleted, not left as partial state
- Archive extraction via external tools (7z for zip/7z/rar, GNU tar for tar variants, dolphin-tool/dimg-tool for disc images). Post-extraction validation: absolute size limit (50 GiB), path containment check, nesting depth limit (3)
- Single-file decompression streams to disk (no OOM on large files)
- Mid-download detection prevents recording unstable files
- DB cache is disposable — rebuild from `romroot/*.rscf` + selection DATs
- DB automatically backed up before scan/run with tiered rotation
- Schema auto-migrates on version mismatch (drop+recreate)
- Post-mortem corruption detection via `collect verify`: re-hashes every romroot
  archive against its RSCF sidecar. Reports per-ROM recovery status (source
  available vs lost). Read-only — never modifies files or the database.
  Exit code 1 on corruption for scripting/monitoring

## CLI

```bash
# Full pipeline: scan, match, execute
romtholos collect run config.yaml

# Scan only (populate DB cache)
romtholos collect scan config.yaml
romtholos collect scan config.yaml --force-rescan   # rebuild romroot sidecars
romtholos collect scan config.yaml --path /path/to/source/ps3  # scan subfolder only

# Match only (show plan without executing)
romtholos collect plan config.yaml

# Execute only (requires prior scan, re-matches internally)
romtholos collect execute config.yaml
romtholos collect execute config.yaml --system "Sony - PlayStation 3"  # single system

# Verify romroot integrity (read-only, exit 1 on corruption)
romtholos collect verify config.yaml

# Show DB cache status
romtholos collect status config.yaml
```

## Known Limitations

- DAT parsing only handles `<game>` elements, not `<machine>` (MAME-style DATs).
- No `rebuild-cache` standalone command yet.
- Dolphin disc image extraction requires `dolphin-tool` (native binary or Dolphin Emulator flatpak).
