# File Safety & Data Flow

ROM files can be irreplaceable. This document defines how RomTholos handles every file it touches, where files can and cannot be deleted, and how data flows through the system.

## Core Principle

**RomTholos never deletes ROM data unless the user explicitly requests it.** Files are moved, compressed, verified, and relocated — but never silently removed. This aligns with how established ROM managers (RomVault, CLRMamePro) handle file safety: unneeded files go to a holding area, not to the trash.

## Directory Roles

### User-owned (manual curation)

| Directory | Config key | Written by | Purpose |
|-----------|-----------|------------|---------|
| `upstream/` | `paths.upstream` | User | Raw DAT files from providers (Redump, No-Intro). Never modified by RomTholos. |
| Source dirs | `sources[].path` | External | ROM files from any origin. Mode determines what RomTholos may do. |

### RomTholos-owned (managed automatically)

| Directory | Config key | Written by | Purpose |
|-----------|-----------|------------|---------|
| `selection/` | `paths.selection` | Select module | Filtered DATs. Mirrors upstream structure. Stale files cleaned on `select run`. |
| `romroot/` | `paths.romroot` | Collect module | Canonical ROM storage. Compressed archives + RSCF sidecars. Structure: `romroot/<dat_folder>/<dat_name>/<game>.ext` |
| `romroot/_orphaned/` | (convention) | Collect module | Quarantine for romroot files no longer claimed by any DAT. Preserves relative path. User reviews before deletion. |
| `work_dir/` | `paths.work_dir` | Collect module | Temporary extraction/compression workspace. Should be on fast local storage (SSD). Cleaned after each game. |
| `db_cache` | `paths.db_cache` | Collect module | SQLite database. Local filesystem only (WAL mode). Disposable — rebuilt from RSCF sidecars + DATs. |
| `db_backup_dir` | `paths.db_backup_dir` | Collect module | Tiered backup rotation for the DB cache. |
| `romroot_overrides` | `romroot_overrides` | Collect module | Per-system alternate romroot mount points. Same rules as romroot. |

### Source Modes

Sources are external directories that RomTholos scans for ROM files. The mode determines what the collector may do:

| Mode | Config | Reads | Writes sidecars | Deletes files | Use case |
|------|--------|-------|----------------|---------------|----------|
| `read-only` | `mode: read-only` | Yes | No | No | RomVault exports, shared network mounts, torrent directories |
| `read-write` (ingest) | `mode: read-write` | Yes | Yes | No | Personal download/staging area |
| `disposal` | `mode: disposal` | Yes | Yes | Yes (after verified collection) | Temporary staging where source is expendable |

**Read-only** is the default and safest mode. Source files are never touched.
<!-- tested-by: test_safety_source_modes::TestReadOnlySource -->

**Ingest** allows RomTholos to write RSCF sidecars alongside source files for faster re-scans. Source files themselves are never modified or deleted.
<!-- tested-by: test_safety_source_modes::TestIngestSource -->

**Disposal** extends ingest with post-collection deletion. A source file may only be deleted when ALL of these conditions are met:
1. The file's content hashes have been verified in the romroot copy
2. The romroot copy has been BLAKE3-verified after write (re-read from target)
3. The RSCF sidecar is written and verified in romroot
4. No other game references the same source file (by filesystem path — if a source archive contains ROMs used by multiple games, it cannot be deleted until all are collected)
5. For archives: every entry inside the archive is accounted for in romroot (prevents silent destruction of files not tracked by any DAT in mixed archives)
<!-- tested-by: test_safety_disposal::TestDisposalConditions -->

This is the **only** place in the entire system where source files may be deleted.

## Compression Profiles

### Standard archives

| Profile | Tool | Container | Single-file ext | Multi-file ext | Append | Rebuild |
|---------|------|-----------|----------------|---------------|--------|---------|
| `zstd-19` | zstd | tar.zst or zst | `.zst` | `.tar.zst` | No | Yes |
| `zstd-12` | zstd | tar.zst or zst | `.zst` | `.tar.zst` | No | Yes |
| `zstd-3` | zstd | tar.zst or zst | `.zst` | `.tar.zst` | No | Yes |
| `7z-96m` | 7z | 7z | `.7z` | `.7z` | Yes | Yes |
| `7z-16m` | 7z | 7z | `.7z` | `.7z` | Yes | Yes |
| `zip` | zip | zip | `.zip` | `.zip` | Yes | Yes |
| `torrentzip` | trrntzip | zip | `.zip` | `.zip` | No | Yes |
| `none` | cp | directory | (per-file) | (directory) | N/A | N/A |

**Zstd single↔multi transition**: When a zstd game goes from 1 ROM to 2+ ROMs (or vice versa), the container format changes (`.zst` ↔ `.tar.zst`). This triggers a REBUILD even though the profile hasn't changed — the extension changes.
<!-- tested-by: test_safety_profile_transition::TestZstdExtensionTransition -->

**Append vs Rebuild**: Appendable formats (7z, zip) can add new ROMs to an existing archive without extracting/recompressing existing content. Non-appendable formats (zstd, torrentzip) must extract all existing ROMs + add new ones → recompress everything.
<!-- tested-by: test_safety_dat_updates::TestDatAddsRomToGame -->

### Disc image formats

| Profile | Tool | Container | Extension | All tracks required | Use case |
|---------|------|-----------|-----------|-------------------|----------|
| `rvz-zstd-19` | dolphin-tool | RVZ | `.rvz` | Single disc only | GameCube/Wii |
| `rvz-zstd-5` | dolphin-tool | RVZ | `.rvz` | Single disc only | GameCube/Wii (fast) |
| `rvz-to-iso` | dolphin-tool | Raw ISO | `.iso` | Single disc only | Decompression |
| `aaru-*-zstd` | dimg-tool | Aaru | `.aaru` | Yes (all tracks) | CD/DVD systems |

**Aaru profiles** are system-specific (ps1, ps2cd, ps2dvd, dc, saturn, megacd, neogeo, pce) because the disc format parameters differ per system. They require ALL tracks present — partial games cannot use aaru profiles.

**Partial game fallback**: When the configured profile requires all tracks (aaru) but not all tracks are available, the game falls back to `partial_fallback` profile (e.g. `zstd-12`). Games below `partial_min_ratio` (fraction of tracks available) are skipped entirely.
<!-- tested-by: test_safety_partial_fallback::TestStandardProfilePartialCollection -->
<!-- tested-by: test_safety_partial_fallback::TestAllTracksProfileRatioSkip -->

### Copy-as-is optimization

When a source file already matches the target profile (same compression method, level, block size), it is copied directly to romroot without extract→recompress. Applies to both RVZ (checked via `dolphin-tool header --json`) and Aaru (checked via `dimg-tool` header). The RSCF sidecar records the inner content hashes from the scan phase.
<!-- tested-by: test_safety_copy_as_is::TestCopyAsIsSafety -->

## Data Flow

### Collection: Source → Romroot

```
Source file ──[READ]──→ Work dir (SSD)
                           │
                     [extract if archive]
                     [hash 5 types: CRC32, MD5, SHA1, SHA256, BLAKE3]
                     [verify hash against DAT entry]
                           │
                     [compress with configured profile]
                     [hash compressed output]
                           │
                     [roundtrip verify (--verify-roundtrip)]
                     [extract compressed output, verify per-ROM BLAKE3]
                           │
                     [copy to romroot target] ──→ Romroot (NAS/local)
                                                      │
                                                [re-read from target]
                                                [verify BLAKE3 matches pre-copy hash]
                                                      │
                                              [write RSCF sidecar]
                                              [update DB]
                           │
                     [clean work dir]
```
<!-- tested-by: test_safety_collection_verify::TestRoundtripVerification -->
<!-- tested-by: test_safety_collection_verify::TestRoundtripVerifyFlag -->
<!-- tested-by: test_safety_collection_verify::TestSidecarCompleteness -->
<!-- tested-by: test_safety_collection_verify::TestWorkDirCleanup -->

**Verification is mandatory**: The compressed output is hashed before copy, then re-read and re-verified from the target filesystem after copy. This catches CIFS/network corruption. If verification fails, the corrupt target file is deleted (it contains no useful data — the original is still in the source or work dir).
<!-- tested-by: test_safety_collection_verify::TestInFlightCorruption -->

**Roundtrip verification** (`--verify-roundtrip`): After compression, the archive is extracted back and every ROM's BLAKE3 is verified against the input hashes. This catches compression bugs and data corruption during the compress→extract cycle. Works for all archive types (7z, zstd, zip, rvz, aaru). Bidirectional: every expected ROM must appear in extracted output, every extracted file must match an expected hash. For disc image profiles (aaru), this verifies that the CUE/BIN→aaru→CUE/BIN roundtrip preserves all data tracks bit-perfectly.
<!-- tested-by: test_safety_collection_verify::TestRoundtripVerifyFlag -->

### Corruption detection: in-flight vs post-mortem

**In-flight corruption** is detected during the collection pipeline (the roundtrip verification above). The source data is still available in the work dir, so deleting the corrupt target and recompressing is safe — no data is lost.
<!-- tested-by: test_safety_collection_verify::TestInFlightCorruption -->

**Post-mortem corruption** is detected later — during a periodic integrity check, rescan, or when a user tries to use a file. This is a fundamentally different problem because the source may no longer be available (removable media ejected, source directory cleaned up, disposal mode already deleted the original). Recovery options:

1. **Source still available**: Re-collect from source automatically. Same pipeline as initial collection.
2. **Source unavailable, other sources have the hash**: Re-collect from an alternative source.
3. **No source available**: The corrupt file cannot be silently deleted — it may be the only copy. Flag the file as corrupt in the DB, log the corruption, and require manual user intervention. The user decides whether to attempt repair, find the source elsewhere, or accept the loss.

RomTholos must never automatically delete a romroot file discovered to be corrupt post-mortem. In-flight corruption (where the original is confirmed safe) is the only case where automatic deletion is permitted.
<!-- tested-by: test_safety_corruption_postmortem::TestPostMortemCorruption -->
<!-- tested-by: test_safety_corruption_postmortem::TestVerifyClean -->
<!-- tested-by: test_safety_corruption_postmortem::TestVerifyMultipleFiles -->

### Relocation: Romroot path change

When DATs are reorganized in `selection/`, romroot files automatically move to match.

```
romroot/old-path/Game.7z  ──[move]──→  romroot/new-path/Game.7z
romroot/old-path/Game.7z.rscf  ──[move]──→  romroot/new-path/Game.7z.rscf
[update DB: delete old path entries, insert new path entries]
[remove empty parent directories]
```
<!-- tested-by: test_safety_relocation::TestRelocationContentPreservation -->
<!-- tested-by: test_safety_relocation::TestSidecarRelocation -->
<!-- tested-by: test_safety_relocation::TestRelocationCleanup -->

Pure filesystem move — no repacking, no recompression. Works for both archive files and game directories (none profile).
<!-- tested-by: test_safety_relocation::TestRelocationContentPreservation::test_archive_blake3_identical -->
<!-- tested-by: test_safety_relocation::TestRelocationContentPreservation::test_directory_mode_rom_identical -->

**Cross-filesystem moves**: When source and target are on the same filesystem, `rename()` is atomic — no data is copied, no corruption possible. However, romroot may span multiple mount points (e.g. `romroot_overrides` on a different drive, or the user mounting additional storage under romroot). In these cases the move becomes copy+delete, which can introduce corruption. RSCF sidecars record inode numbers; an inode change after a move signals a cross-filesystem copy occurred, and re-verification (BLAKE3 of the target file against the sidecar's container hash) is required before deleting the source.
<!-- tested-by: test_safety_relocation::TestCrossFilesystemRelocation -->

### Orphan quarantine

When a romroot file is no longer claimed by any current DAT (game removed from DAT, DAT removed from selection):

```
romroot/path/Game.7z  ──[move]──→  romroot/_orphaned/path/Game.7z
[delete romroot_files DB entries — file exits romroot domain]
[sidecar moves alongside archive]
```
<!-- tested-by: test_safety_orphan_quarantine::TestQuarantineIsNotDeletion -->
<!-- tested-by: test_safety_orphan_quarantine::TestQuarantineContentPreservation -->

**NOT a deletion.** The file is preserved with its full path context. If a future DAT claims the file's hash, the file is re-collected from `_orphaned/` via the full collection pipeline (extract → compress → verify). This handles profile changes that occurred while the file was orphaned.
<!-- tested-by: test_safety_orphan_quarantine::TestQuarantineRescue -->

**Scan isolation**: `_orphaned/` is excluded from the romroot scan. Instead, it is scanned as an ingest source — files are discoverable by hash but not subject to orphan detection. RSCF sidecars in `_orphaned/` are reused via a sidecar fast path (no re-extraction of large archives). This prevents recursive quarantine (`_orphaned/_orphaned/...`) while keeping orphaned files matchable for rescue.

**Collision handling**: If a file is quarantined to a path that already exists in `_orphaned/`, the BLAKE3 hashes are compared. Same content → replace silently. Different content → add BLAKE3 suffix to preserve both versions (e.g., `Game_a3f7c1d2.7z`). No data is ever silently overwritten with different content.
<!-- tested-by: test_safety_orphan_quarantine::TestQuarantineCollision -->

**Granularity**: Orphan detection works at the ROM level, not the game level. A DAT update may invalidate individual tracks in any multi-ROM game — this is a general problem that applies to all container types in romroot, not just disc images. For example, upstream may find a better dump of one audio track and update the DAT with the corrected hash.
<!-- tested-by: test_safety_orphan_granularity::TestSingleTrackInvalidated -->

When a DAT update invalidates one or more (but not all) tracks in a previously complete game:

- **Appendable archives (7z, zip)**: The archive still contains the old (now-invalidated) track. The game becomes partial — the changed track is `missing`, the rest are `in_romroot`. When the corrected track becomes available, the archive is REBUILT: all ROMs extracted, the invalidated track replaced, and recompressed. The old track is discarded only as part of the rebuild (the new archive is verified before the old one is deleted).
<!-- tested-by: test_safety_orphan_granularity::TestRebuildWithCorrectedTrack -->
<!-- tested-by: test_safety_orphan_granularity::TestPartialGameStatusTransitions::test_complete_to_partial_to_complete -->
- **Monolithic containers (aaru, rvz, tar.zst)**: All tracks are baked into a single container. The container must be fully extracted, the invalidated track(s) orphaned as individual files, and the remaining valid tracks rebuilt into a new container (using the fallback profile if the primary profile requires all tracks). The old container is deleted only after the new one is verified on target.
<!-- tested-by: test_safety_orphan_granularity::TestMonolithicContainerPartialInvalidation -->
- **Single-file games**: The file itself is either matched or orphaned — no partial state possible.
<!-- tested-by: test_safety_orphan_granularity::TestSingleFileGame -->

The key distinction: a file is orphaned when **none** of its content hashes match any current DAT entry. If even one ROM inside it is still claimed, the file stays in romroot (as a partial game needing rebuild). This applies equally to files already in romroot and to newly collected files.
<!-- tested-by: test_safety_orphan_granularity::TestAllTracksInvalidated -->
<!-- tested-by: test_safety_orphan_granularity::TestPartialGameStatusTransitions::test_partial_to_orphaned -->

**User-initiated cleanup** of `_orphaned/` uses `collect purge`. The user reviews orphaned files and explicitly confirms deletion. RomTholos never deletes from `_orphaned/` on its own.
<!-- tested-by: test_safety_orphan_quarantine::TestOrphanedNeverAutoDeleted -->

### Profile transition

When the configured compression profile changes for a system:

```
romroot/Game.7z (old profile)
    ↓
[extract all ROMs to work dir]
[recompress with new profile]
[verify compressed output]
[copy to romroot as Game.zst (or whatever new extension)]
[verify on target]
[write new RSCF sidecar]
[delete old archive + sidecar]  ← ONLY after new archive verified
```
<!-- tested-by: test_safety_profile_transition::TestProfileChangePreservesContent -->
<!-- tested-by: test_safety_profile_transition::TestSidecarUpdatedOnProfileChange -->

The old archive is deleted only AFTER the new archive is verified on the target filesystem. If any step fails, the old archive remains untouched.
<!-- tested-by: test_safety_profile_transition::TestOldArchiveDeletedOnlyAfterVerification -->

## Deletion Rules

### Where files ARE deleted

| What | When | Condition |
|------|------|-----------|
| Work dir temporary files | After each game | Always — these are copies, originals safe |
| Old archive after profile transition | After new archive verified | New archive BLAKE3-verified on target |
| Corrupt target file | Failed verification | File contains garbage — no data loss |
| Old archive after extension change | After new archive verified | Same as profile transition |
| Stale selection DATs | On `select run` | DAT no longer in upstream (auto-discovery) or removed from config |
| Disposal source files | After verified collection | All 5 verification conditions met |

<!-- tested-by: test_safety_collection_verify::TestWorkDirCleanup (work dir) -->
<!-- tested-by: test_safety_profile_transition::TestOldArchiveDeletedOnlyAfterVerification (profile transition) -->
<!-- tested-by: test_safety_collection_verify::TestInFlightCorruption (corrupt target) -->
<!-- tested-by: test_safety_profile_transition::TestZstdExtensionTransition (extension change) -->
<!-- tested-by: test_safety_selection::TestStaleSelectionCleanup (stale DATs) -->
<!-- tested-by: test_safety_disposal::TestDisposalConditions -->

### Where files are NEVER deleted

| What | Instead |
|------|---------|
| Source files (read-only mode) | Never touched |
| Source files (ingest mode) | Never touched (sidecars may be added alongside) |
| Romroot files not in any DAT | Moved to `_orphaned/` — user reviews |
| Romroot files at wrong path | Relocated to correct path |
| RSCF sidecars | Moved with their source file, or deleted with old archive after replacement |

<!-- tested-by: test_safety_source_modes::TestReadOnlySource (read-only) -->
<!-- tested-by: test_safety_source_modes::TestIngestSource (ingest) -->
<!-- tested-by: test_safety_orphan_quarantine::TestQuarantineIsNotDeletion (orphan → _orphaned) -->
<!-- tested-by: test_safety_relocation::TestRelocationContentPreservation (wrong path → relocate) -->
<!-- tested-by: test_safety_relocation::TestSidecarRelocation (sidecars move with file) -->
<!-- tested-by: test_safety_orphan_quarantine::TestQuarantineContentPreservation::test_sidecar_moves_with_quarantined_file -->

### Deletion by user request only

| Operation | Trigger | Safety |
|-----------|---------|--------|
| Purge `_orphaned/` | `collect purge` CLI command | User reviews list, confirms deletion (`--yes` to skip) |
| Delete specific orphan | `collect purge --path <rel>` | Per-file confirmation |
| Disposal source cleanup | Automatic after verified collection | All 4 verification conditions met |

<!-- tested-by: test_safety_orphan_purge::TestListOrphans -->
<!-- tested-by: test_safety_orphan_purge::TestPurgeSingle -->
<!-- tested-by: test_safety_orphan_purge::TestPurgeAll -->
<!-- tested-by: test_safety_orphan_purge::TestPurgeIntegration -->

## RSCF Sidecars

Every file in romroot has an RSCF sidecar (`.rscf`) containing:
- **Container hash**: BLAKE3 of the compressed file (for integrity checking)
- **Container metadata**: size, mtime, ctime, inode (for stat-cache)
- **Renderer**: compression profile name (for profile change detection)
- **File entries**: per-ROM hashes (all 5 types) and original sizes

<!-- tested-by: test_safety_collection_verify::TestRoundtripVerification (container hash, ROM hashes) -->
<!-- tested-by: test_safety_collection_verify::TestSidecarCompleteness::test_sidecar_renderer_matches_profile (renderer) -->
<!-- tested-by: test_safety_relocation::TestCrossFilesystemRelocation::test_sidecar_metadata_updated_after_move (metadata) -->

Sidecars serve three purposes:
1. **Cold-start recovery**: If the DB cache is lost, romroot scan rebuilds the DB from sidecars without re-hashing file contents.
2. **Stat-cache acceleration**: Warm-DB scans compare file stats against the DB. If unchanged, no I/O needed — not even a sidecar read.
3. **Profile detection**: The `renderer` field tells the collector whether a file needs recompression when the configured profile changes.

<!-- tested-by: test_safety_cold_start::TestColdStartRecovery -->
<!-- tested-by: test_safety_cold_start::TestCorruptSidecarRecovery -->

## Concurrent Run Protection

Only one collector process may run against a given romroot at a time. Concurrent runs risk:
- Two processes moving/rebuilding the same archive simultaneously
- Race conditions between orphan detection and collection (one process quarantines a file another is about to relocate)
- DB corruption despite SQLite WAL mode (concurrent schema operations, interleaved transaction logic)

The collector must acquire an exclusive lock file (`romroot/.collector.lock`) at startup and hold it for the duration of the run. If the lock is already held, the process must refuse to start with a clear error message. The lock file should record the PID and start timestamp so stale locks (from crashed processes) can be identified.
<!-- tested-by: test_safety_concurrent::TestLockFile -->

## Comparison with Other ROM Managers

| Feature | RomVault | CLRMamePro | RomTholos |
|---------|----------|------------|-----------|
| Unneeded files | Move to ToSort | Move to Backup (or delete!) | Move to `_orphaned/` |
| Compression | Limited (zip, 7z) | None (works with raw) | Full pipeline (multiple profiles) |
| Source safety | ToSort can be read-only | Backup can be disabled (dangerous) | Sources never modified by default |
| DAT handling | Direct use | Direct use | Pipeline: upstream → filter → selection |
| Disc images | No conversion | No conversion | Extract + convert (RVZ, Aaru) |
| Hash-based matching | Yes | Yes | Yes (5 hash types, game-aware) |
| Profile transitions | N/A | N/A | Automatic recompress, old file safe until new verified |
| DB recovery | Rescan required | Rescan required | Instant from RSCF sidecars |
