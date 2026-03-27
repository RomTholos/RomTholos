"""Phase 3: Execute — process match plan, write ROMs to romroot with RSCF.

Builds per-game archives incrementally. If a game has some ROMs already
collected and new ones become available, the archive is updated (append
or rebuild depending on the compression profile).

Configuration is authoritative: if the configured profile differs from
the stored sidecar.renderer, the archive is rebuilt/recompressed.

Strategies per profile type:
  append (7z, zip): add new files to existing archive
  rebuild (tar.zst, torrentzip): extract existing + add new → recompress all
  directory (none): copy individual files to game directory
  single_only (rvz): only handles single-ROM games
"""

from __future__ import annotations

import enum
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path

from rscf import (
    FileEntry,
    FileHashes,
    Sidecar,
    SidecarResolver,
    StorageMode,
    hash_file,
    read_sidecar,
    write_sidecar,
)
from rscf.sidecar import RscfError

from romtholos.collect.compress import (
    PROFILES,
    compress,
    dimg_header_matches_profile,
    dolphin_header_matches_profile,
    profile_compatible,
    profile_extension,
    read_dimg_header,
    read_dolphin_header,
    strip_archive_extension,
)
from romtholos.collect.config import ORPHANED_DIR_NAME
from romtholos.collect.db import CacheDB
from romtholos.collect.extract import ExtractionLimits, ExtractedFile, extract_recursive
from romtholos.collect.match import GamePlan, MatchOp


class ExtractionCache:
    """Cache extracted archive contents across games within a system run.

    Avoids re-extracting the same source .7z for every partial game
    that shares tracks from it.  Hash indexes are built lazily on first
    lookup and cached alongside the extraction — each file is hashed
    exactly once regardless of how many tracks reference it.
    """

    def __init__(self, cache_dir: Path, max_mb: int = 2048):
        self._cache_dir = cache_dir
        self._max_bytes = max_mb * 1024 * 1024
        self._entries: dict[str, list[ExtractedFile]] = {}
        self._sizes: dict[str, int] = {}
        self._hash_indexes: dict[str, dict[tuple[str, str], Path]] = {}
        self._current_bytes = 0

    def get_or_extract(
        self, source: Path, limits: ExtractionLimits,
    ) -> list[ExtractedFile]:
        """Return cached extraction or extract and cache.

        The quota is a soft limit — we always cache the current extraction
        (must be able to hold at least the largest single archive). Old
        entries are evicted to stay near the target.
        """
        key = str(source)
        if key in self._entries:
            return self._entries[key]

        subdir = self._cache_dir / f"_cache_{hash(key) & 0xFFFFFFFF:08x}"
        subdir.mkdir(parents=True, exist_ok=True)

        extracted = extract_recursive(source, subdir, limits)
        total_size = sum(e.size for e in extracted)

        # Evict oldest entries to make room (soft quota)
        while self._current_bytes + total_size > self._max_bytes and self._entries:
            evict_key = next(iter(self._entries))
            self._evict(evict_key)

        # Always cache — quota is a target, not a hard limit
        self._entries[key] = extracted
        self._sizes[key] = total_size
        self._current_bytes += total_size

        return extracted

    def _evict(self, key: str) -> None:
        """Remove a cached extraction."""
        size = self._sizes.pop(key, 0)
        self._current_bytes -= size
        self._hash_indexes.pop(key, None)
        entries = self._entries.pop(key, [])
        # Remove the cache subdir for this key
        if entries:
            parent = entries[0].path.parent
            if parent.exists() and parent.is_relative_to(self._cache_dir):
                shutil.rmtree(parent, ignore_errors=True)

    def _ensure_index(self, key: str) -> dict[tuple[str, str], Path]:
        """Build hash index for a cached extraction (lazy, once per archive)."""
        if key in self._hash_indexes:
            return self._hash_indexes[key]

        index: dict[tuple[str, str], Path] = {}
        for ef in self._entries.get(key, []):
            if not ef.path.exists():
                continue
            hashes = hash_file(ef.path)
            for ht in ("crc32", "md5", "sha1", "sha256", "blake3"):
                v = getattr(hashes, ht, "")
                if v:
                    index[(ht, v.upper())] = ef.path

        self._hash_indexes[key] = index
        return index

    def find_indexed(
        self, source: Path, hash_type: str, hash_value: str,
        limits: ExtractionLimits,
    ) -> Path | None:
        """Find a file by hash with O(1) lookup.

        Extracts the archive if not cached, builds the hash index if not
        yet indexed.  Each file is hashed exactly once.
        """
        self.get_or_extract(source, limits)
        key = str(source)
        index = self._ensure_index(key)
        return index.get((hash_type, hash_value.upper()))

    def find_file(
        self, source: str, hash_type: str, hash_value: str,
    ) -> Path | None:
        """Find a specific file from a cached extraction by hash.

        Uses the indexed lookup — each file hashed once, O(1) per call.
        Requires the source to have been extracted via get_or_extract first.
        """
        index = self._ensure_index(source)
        return index.get((hash_type, hash_value.upper()))

    def cleanup(self) -> None:
        """Remove all cached extractions."""
        if self._cache_dir.exists():
            shutil.rmtree(self._cache_dir, ignore_errors=True)
        self._entries.clear()
        self._sizes.clear()
        self._current_bytes = 0


def _find_by_hash(
    candidates: list[Path], hash_type: str, hash_value: str,
) -> Path | None:
    """Find a file among candidates by hashing and comparing.

    Hash is the only authoritative identifier for ROM matching.
    """
    for path in candidates:
        if not path.exists():
            continue
        hashes = hash_file(path)
        actual = getattr(hashes, hash_type, "")
        if actual and actual.upper() == hash_value.upper():
            return path
    return None


def _ensure_dir(path: Path) -> Path:
    """Create directory if it doesn't exist."""
    path.mkdir(parents=True, exist_ok=True)
    return path


def _clean_dir(path: Path) -> None:
    """Remove all contents of a directory, recreate it empty."""
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _verify_on_target(target_path: Path, expected_blake3: str) -> bool:
    """Re-read a file from the target filesystem and verify BLAKE3."""
    if not target_path.exists():
        return False
    hashes = hash_file(target_path)
    return hashes.blake3.upper() == expected_blake3.upper()


def _verify_roundtrip(
    archive_path: Path,
    expected_entries: list[tuple[str, str]],
    limits: ExtractionLimits,
) -> tuple[bool, list[str]]:
    """Extract a compressed archive and verify per-ROM BLAKE3 hashes.

    Performs the same extract cycle that a consumer would, then verifies
    bidirectionally: every expected ROM must appear in the extracted
    output, and every extracted file must match an expected BLAKE3.

    Matching is by BLAKE3 only — filenames are never trusted.

    Args:
        archive_path: Compressed archive in work_dir.
        expected_entries: List of (rom_name, blake3) for every ROM
            that should be inside the archive.
        limits: Extraction safety limits.

    Returns:
        (ok, errors): ok is True when all checks pass.
    """
    import tempfile

    errors: list[str] = []

    with tempfile.TemporaryDirectory(prefix="rt_verify_") as tmp:
        try:
            extracted = extract_recursive(archive_path, Path(tmp), limits)
        except Exception as e:
            return False, [f"extraction failed: {e}"]

        if not extracted:
            return False, ["extraction produced no files"]

        expected_blake3 = {b.upper() for _, b in expected_entries}

        # Hash all extracted files
        extracted_blake3: set[str] = set()
        for ef in extracted:
            h = hash_file(ef.path)
            extracted_blake3.add(h.blake3.upper())

        # Every expected ROM must be present in extracted output
        for rom_name, blake3 in expected_entries:
            if blake3.upper() not in extracted_blake3:
                errors.append(rom_name)

        # Every extracted file must match a known-good BLAKE3
        for ef in extracted:
            h = hash_file(ef.path)
            if h.blake3.upper() not in expected_blake3:
                errors.append(ef.path.name)

    return len(errors) == 0, errors


def _get_rom_to_work_dir(
    op: MatchOp, work_dir: Path, limits: ExtractionLimits,
    cache: ExtractionCache | None = None,
) -> Path | None:
    """Get a ROM file into work_dir. Returns path or None on failure.

    All extraction lookups use hash matching — filenames inside archives
    can differ from DAT names and are not authoritative.

    When cache is provided, archive extractions are cached and the needed
    file is *copied* from cache to work_dir.
    """
    assert op.source_path is not None
    source = Path(op.source_path)

    if op.source_type == "plain":
        dest = work_dir / source.name
        shutil.copy2(source, dest)
        return dest

    if op.source_type == "archive":
        dest = work_dir / source.name
        shutil.copy2(source, dest)
        return dest

    if op.source_type in ("archive_content", "romroot"):
        if cache is not None:
            matched = cache.find_indexed(
                source, op.hash_type, op.hash_value, limits,
            )
            if matched is not None and matched.exists():
                # Use the DAT rom_name as destination to avoid collisions:
                # an extracted filename could match another track's DAT name,
                # overwriting an already-placed file.
                dat_name = Path(op.rom_name).name
                dest = work_dir / dat_name
                shutil.copy2(matched, dest)
                return dest
            return None

        extracted = extract_recursive(source, work_dir, limits)
        return _find_by_hash(
            [e.path for e in extracted], op.hash_type, op.hash_value,
        )

    return None


def _archive_strategy(profile_name: str) -> str:
    """Determine strategy for building/updating game archives.

    Returns:
        'append': can add files to existing archive (7z, zip)
        'rebuild': must extract all + new, recompress (tar.zst, torrentzip)
        'single_only': profile only supports single-file games (rvz)
        'directory': files placed individually in game dir (none)
    """
    if profile_name == "none":
        return "directory"

    profile = PROFILES[profile_name]

    if not profile["multi_file"]:
        return "single_only"

    if profile["append_function"] is not None:
        return "append"

    return "rebuild"


def _source_matches_dimg_profile(
    source_path: Path,
    compression_profile: str,
) -> bool:
    """Check if a source .aaru already matches the target dimg profile.

    Returns True if the source can be copied as-is to romroot.
    Returns False on any error (safe fallback to extract+recompress).
    """
    from romtholos.collect.compress import CompressionError

    try:
        header = read_dimg_header(source_path)
        return dimg_header_matches_profile(header, compression_profile)
    except (CompressionError, Exception):
        return False


def _copy_sbi_to_work_dir(
    game_name: str,
    work_dir: Path,
    sbi_dir: Path | None,
) -> None:
    """Copy SBI file for a PS1 game to work_dir if available.

    SBI files use Redump naming convention: exact game name + ".sbi".
    dimg-tool auto-detects SBI files next to the CUE file.
    """
    if sbi_dir is None or not sbi_dir.is_dir():
        return

    sbi_name = game_name + ".sbi"
    sbi_path = sbi_dir / sbi_name
    if sbi_path.is_file():
        dest = work_dir / sbi_name
        shutil.copy2(sbi_path, dest)


def _profile_requires_all_tracks(compression_profile: str) -> bool:
    """Check if a profile requires all tracks present (no partial games).

    Aaru profiles need complete CUE/BIN sets — can't compress partial games.
    """
    return compression_profile.startswith("aaru-")


def game_is_multi_bin(game: GamePlan) -> bool:
    """Check if a game's DAT entries use multi-BIN format (per-track BIN files).

    Redump DATs list one .bin per track for multi-track discs (e.g. PS1, PCE CD,
    Saturn with audio tracks). Single-track discs get one .bin entry.

    This is the authoritative way to determine whether dimg-tool --multi-bin
    output will match the DAT hashes: the DAT defines the expected file layout.
    """
    bin_count = sum(
        1 for op in game.ops if Path(op.rom_name).suffix.lower() == ".bin"
    )
    return bin_count > 1


def _detect_media_type(game: GamePlan) -> str:
    """Detect media type from a game's ROM file extensions.

    Returns:
        'gdi': GD-ROM (Dreamcast .gdi format)
        'cd': CD disc image (CUE/BIN)
        'dvd': DVD disc image (single ISO)
        'rom': Everything else (cartridge ROMs, plain files)
    """
    extensions = {Path(op.rom_name).suffix.lower() for op in game.ops}
    if ".gdi" in extensions:
        return "gdi"
    if ".cue" in extensions:
        return "cd"
    if ".iso" in extensions:
        return "dvd"
    return "rom"


def _source_matches_dolphin_profile(
    source_path: Path,
    compression_profile: str,
) -> bool:
    """Check if a source disc image already matches the target dolphin profile.

    Returns True if the source can be copied as-is to romroot.
    Returns False on any error (safe fallback to extract+recompress).
    """
    from romtholos.collect.compress import CompressionError

    try:
        header = read_dolphin_header(source_path)
        return dolphin_header_matches_profile(header, compression_profile)
    except (CompressionError, Exception):
        return False


# ---------------------------------------------------------------------------
# Archive discovery
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ExistingArchive:
    """An archive (or game directory) found on the target filesystem."""

    path: Path
    """Archive file or game directory."""

    sidecar: Sidecar
    """Parsed sidecar content."""

    rscf_path: Path
    """Sidecar file path."""

    is_directory: bool
    """True for 'none' profile game directories."""


def _build_existing_cache(
    target_dir: Path,
    resolver: SidecarResolver,
) -> dict[str, ExistingArchive]:
    """Pre-scan target directory to build game_name → ExistingArchive cache.

    Called once per system before the game loop. Replaces per-game
    _find_existing_archive calls which caused O(n²) directory scans:
    each of N games listed the same N-file directory.
    """
    cache: dict[str, ExistingArchive] = {}
    if not target_dir.is_dir():
        return cache

    for entry in target_dir.iterdir():
        if entry.is_dir():
            # Game directory (none profile)
            game_name = entry.name
            if game_name in cache:
                continue
            entries: list[FileEntry] = []
            for rom_file in sorted(entry.iterdir()):
                if not rom_file.is_file() or rom_file.suffix == ".rscf":
                    continue
                rscf_path = resolver.sidecar_path(rom_file)
                if rscf_path.exists():
                    try:
                        sc = read_sidecar(rscf_path)
                        entries.extend(sc.files)
                    except RscfError:
                        pass
            if entries:
                synthetic = Sidecar(
                    container_blake3="",
                    container_size=0,
                    container_mtime_ns=0,
                    container_ctime_ns=0,
                    container_inode=0,
                    renderer="none",
                    files=entries,
                )
                cache[game_name] = ExistingArchive(
                    path=entry,
                    sidecar=synthetic,
                    rscf_path=entry,  # placeholder
                    is_directory=True,
                )

        elif entry.is_file() and entry.suffix != ".rscf":
            game_name = strip_archive_extension(entry.name)
            if game_name in cache:
                continue
            rscf_path = resolver.sidecar_path(entry)
            if rscf_path.exists():
                try:
                    sidecar = read_sidecar(rscf_path)
                    cache[game_name] = ExistingArchive(
                        path=entry,
                        sidecar=sidecar,
                        rscf_path=rscf_path,
                        is_directory=False,
                    )
                except RscfError:
                    pass  # corrupt sidecar, skip

    return cache


def _find_existing_archive(
    target_dir: Path,
    game_name: str,
    resolver: SidecarResolver,
) -> ExistingArchive | None:
    """Scan target_dir for an existing archive (any extension) for game_name.

    Checks files where strip_archive_extension(name) == game_name, plus
    a game directory (none profile). Returns first with a valid sidecar.

    This is the single-game fallback for cases where the pre-built cache
    is stale (e.g. after relocation). For bulk resolution, use
    _build_existing_cache instead.
    """
    # Check archive files
    if target_dir.is_dir():
        for f in target_dir.iterdir():
            if not f.is_file() or f.suffix == ".rscf":
                continue
            if strip_archive_extension(f.name) == game_name:
                rscf_path = resolver.sidecar_path(f)
                if rscf_path.exists():
                    try:
                        sidecar = read_sidecar(rscf_path)
                        return ExistingArchive(
                            path=f,
                            sidecar=sidecar,
                            rscf_path=rscf_path,
                            is_directory=False,
                        )
                    except RscfError:
                        pass  # corrupt sidecar, skip

    # Check game directory (none profile)
    game_dir = target_dir / game_name
    if game_dir.is_dir():
        # Collect per-file sidecars into a synthetic Sidecar
        entries: list[FileEntry] = []
        for rom_file in sorted(game_dir.iterdir()):
            if not rom_file.is_file() or rom_file.suffix == ".rscf":
                continue
            rscf_path = resolver.sidecar_path(rom_file)
            if rscf_path.exists():
                try:
                    sc = read_sidecar(rscf_path)
                    entries.extend(sc.files)
                except RscfError:
                    pass

        if entries:
            # Build synthetic sidecar representing the directory
            synthetic = Sidecar(
                container_blake3="",
                container_size=0,
                container_mtime_ns=0,
                container_ctime_ns=0,
                container_inode=0,
                renderer="none",
                files=entries,
            )
            return ExistingArchive(
                path=game_dir,
                sidecar=synthetic,
                rscf_path=game_dir,  # placeholder
                is_directory=True,
            )

    return None


# ---------------------------------------------------------------------------
# Action determination
# ---------------------------------------------------------------------------

class GameAction(enum.Enum):
    SKIP = "skip"
    CREATE = "create"
    APPEND = "append"
    REBUILD = "rebuild"
    RECOMPRESS = "recompress"
    NOTHING = "nothing"


def _determine_action(
    existing: ExistingArchive | None,
    new_ops: list[MatchOp],
    compression_profile: str,
    strategy: str,
) -> GameAction:
    """Decide what to do for a game based on existing state and new ops."""
    if existing is None:
        if new_ops:
            return GameAction.CREATE
        return GameAction.NOTHING

    # Existing archive/directory found
    profile_matches = existing.sidecar.renderer == compression_profile

    if profile_matches:
        if not new_ops:
            return GameAction.SKIP

        if strategy == "append":
            # Same-name replacement requires rebuild (extract + replace + recompress)
            existing_names = {Path(e.path).name for e in existing.sidecar.files}
            new_names = {Path(op.rom_name).name for op in new_ops}
            if existing_names & new_names:
                return GameAction.REBUILD

            # Check if extension would change (zstd single→multi)
            old_count = len(existing.sidecar.files)
            new_count = old_count + len(new_ops)
            old_ext = profile_extension(compression_profile, old_count)
            new_ext = profile_extension(compression_profile, new_count)
            if old_ext != new_ext:
                return GameAction.REBUILD
            return GameAction.APPEND

        return GameAction.REBUILD

    # Profile mismatch — config is authoritative
    if new_ops:
        return GameAction.REBUILD
    return GameAction.RECOMPRESS


# ---------------------------------------------------------------------------
# Per-game resolution — resolve all state before dispatch
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ResolvedGame:
    """Fully resolved state for a single game, ready for dispatch.

    All decision-making happens in resolve_game(). The execute loop
    only needs to handle relocation and dispatch.
    """

    game: GamePlan
    effective_profile: str
    effective_strategy: str
    action: GameAction
    existing: ExistingArchive | None
    new_ops: list[MatchOp]
    romroot_status: str  # "absent" | "at_target" | "elsewhere"
    old_location: ExistingArchive | None  # set when romroot_status == "elsewhere"
    skip_reason: str  # "missing" | "below_ratio" | "" (no skip)


def _resolve_profile(
    media_type: str,
    compression_map: dict[str, str],
    system_profile: str,
    global_fallback: str,
) -> str:
    """Resolve compression profile with compatibility cascade.

    Tries each candidate in order, returning the first that is compatible
    with the detected media type:
      1. compression_map[media_type] (user's media-type override)
      2. system_profile (system or global default from config)
      3. global_fallback (global defaults.compression)

    If none are compatible (config error), returns the global fallback
    anyway — better to attempt a generic profile than crash.
    """
    # 1. Media-type override from compression_map
    if media_type in compression_map:
        candidate = compression_map[media_type]
        if candidate in PROFILES and profile_compatible(candidate, media_type):
            return candidate
        print(
            f"  Warning: compression_map[{media_type!r}] = {candidate!r} "
            f"is not compatible with media type {media_type!r}, "
            f"falling back",
            file=sys.stderr,
        )

    # 2. System default (or global default if no system config)
    if profile_compatible(system_profile, media_type):
        return system_profile

    # 3. Global fallback
    if global_fallback != system_profile and profile_compatible(global_fallback, media_type):
        print(
            f"  Warning: system profile {system_profile!r} is not compatible "
            f"with media type {media_type!r}, using global fallback "
            f"{global_fallback!r}",
            file=sys.stderr,
        )
        return global_fallback

    # Config error: nothing compatible. Use global fallback anyway —
    # generic profiles (zstd, 7z, zip) accept any media type, so this
    # should only happen with a misconfigured global default.
    print(
        f"  Warning: no compatible profile found for media type {media_type!r} "
        f"(tried map, {system_profile!r}, {global_fallback!r}), "
        f"using {global_fallback!r} anyway",
        file=sys.stderr,
    )
    return global_fallback


def resolve_game(
    game: GamePlan,
    target_dir: Path,
    compression_profile: str,
    compression_map: dict[str, str],
    partial_fallback: str,
    partial_min_ratio: float,
    resolver: SidecarResolver,
    global_fallback: str = "",
    existing_cache: dict[str, ExistingArchive] | None = None,
) -> ResolvedGame | None:
    """Resolve all per-game state into a deterministic result.

    Returns None for unavailable games. Returns a ResolvedGame with
    skip_reason set for games that should be skipped (below ratio, etc.).
    All filesystem reads happen here; the execute loop only mutates.
    """
    if game.status == "unavailable":
        return None

    if not global_fallback:
        global_fallback = compression_profile

    # Profile resolution: media-type override → system default → global fallback
    # with compatibility checking at each step
    media_type = _detect_media_type(game)
    game_profile = _resolve_profile(
        media_type, compression_map, compression_profile, global_fallback,
    )
    game_strategy = _archive_strategy(game_profile)

    effective_profile = game_profile
    effective_strategy = game_strategy

    # Partial fallback for all-tracks profiles (aaru)
    if (_profile_requires_all_tracks(game_profile)
            and game.status == "partial"):
        if partial_fallback and partial_fallback in PROFILES:
            available = len(game.new_ops) + len(game.existing_ops)
            total = len(game.ops)
            ratio = available / total if total > 0 else 0.0
            if ratio < partial_min_ratio:
                return ResolvedGame(
                    game=game,
                    effective_profile=game_profile,
                    effective_strategy=game_strategy,
                    action=GameAction.NOTHING,
                    existing=None,
                    new_ops=[],
                    romroot_status="absent",
                    old_location=None,
                    skip_reason="below_ratio",
                )
            effective_profile = partial_fallback
            effective_strategy = _archive_strategy(effective_profile)
            print(
                f"  Partial: {game.system}/{game.game_name} "
                f"({available}/{total} ROMs, "
                f"using fallback {effective_profile})",
                file=sys.stderr,
            )
        else:
            return ResolvedGame(
                game=game,
                effective_profile=game_profile,
                effective_strategy=game_strategy,
                action=GameAction.NOTHING,
                existing=None,
                new_ops=[],
                romroot_status="absent",
                old_location=None,
                skip_reason="missing",
            )

    # Find existing archive at target location (O(1) from pre-built cache)
    if existing_cache is not None:
        existing = existing_cache.get(game.game_name)
    else:
        existing = _find_existing_archive(target_dir, game.game_name, resolver)

    # Detect relocation: game exists elsewhere in romroot
    old_location = None
    if existing is not None:
        romroot_status = "at_target"
    elif game.existing_ops:
        old_location = _detect_relocation(
            game, target_dir, game.game_name, resolver,
        )
        romroot_status = "elsewhere" if old_location else "absent"
    else:
        romroot_status = "absent"

    new_ops = game.new_ops

    # Determine action based on what's at the target (not old location)
    action = _determine_action(
        existing, new_ops, effective_profile, effective_strategy,
    )

    return ResolvedGame(
        game=game,
        effective_profile=effective_profile,
        effective_strategy=effective_strategy,
        action=action,
        existing=existing,
        new_ops=new_ops,
        romroot_status=romroot_status,
        old_location=old_location,
        skip_reason="",
    )


# ---------------------------------------------------------------------------
# Relocation — move romroot files when DAT layout changes
# ---------------------------------------------------------------------------

def _detect_relocation(
    game: GamePlan,
    target_dir: Path,
    game_name: str,
    resolver: SidecarResolver,
) -> ExistingArchive | None:
    """Check if a game exists elsewhere in romroot (needs relocation).

    Called when _find_existing_archive returned None (game not at target)
    and the game has in_romroot ops (exists somewhere).

    Returns ExistingArchive at the OLD location, or None if not found.
    """
    existing_ops = game.existing_ops
    if not existing_ops:
        return None

    source_paths = {op.source_path for op in existing_ops if op.source_path}
    if not source_paths:
        return None

    # Determine mode: if all source files live in a game-named directory,
    # it's directory mode (none profile). Otherwise archive mode.
    parent_dirs = {Path(sp).parent for sp in source_paths}

    # Directory mode: all files under one game-named directory
    if len(parent_dirs) == 1:
        old_game_dir = next(iter(parent_dirs))
        if (old_game_dir.is_dir()
                and old_game_dir.name == game_name
                and old_game_dir.parent != target_dir):
            # Build synthetic sidecar from per-file sidecars
            entries: list[FileEntry] = []
            for rom_file in sorted(old_game_dir.iterdir()):
                if not rom_file.is_file() or rom_file.suffix == ".rscf":
                    continue
                rscf_path = resolver.sidecar_path(rom_file)
                if rscf_path.exists():
                    try:
                        sc = read_sidecar(rscf_path)
                        entries.extend(sc.files)
                    except RscfError:
                        pass

            if entries:
                synthetic = Sidecar(
                    container_blake3="",
                    container_size=0,
                    container_mtime_ns=0,
                    container_ctime_ns=0,
                    container_inode=0,
                    renderer="none",
                    files=entries,
                )
                return ExistingArchive(
                    path=old_game_dir,
                    sidecar=synthetic,
                    rscf_path=old_game_dir,
                    is_directory=True,
                )

    # Archive mode: all ops share one source_path (the archive file)
    if len(source_paths) == 1:
        old_path = Path(next(iter(source_paths)))
        if old_path.is_file() and old_path.parent != target_dir:
            rscf_path = resolver.sidecar_path(old_path)
            if rscf_path.exists():
                try:
                    sidecar = read_sidecar(rscf_path)
                    return ExistingArchive(
                        path=old_path,
                        sidecar=sidecar,
                        rscf_path=rscf_path,
                        is_directory=False,
                    )
                except RscfError:
                    pass

    return None


def _execute_relocate(
    game: GamePlan,
    old: ExistingArchive,
    target_dir: Path,
    resolver: SidecarResolver,
    db: CacheDB,
    stats: dict[str, int],
) -> None:
    """Relocate a game's archive/directory from old path to target_dir.

    Pure filesystem move — no repacking, no recompression.
    Updates DB entries to reflect the new paths.
    Cleans up empty parent directories left behind.
    """
    _ensure_dir(target_dir)

    if old.is_directory:
        _relocate_directory(game, old, target_dir, resolver, db)
    else:
        _relocate_archive(game, old, target_dir, resolver, db)

    stats["relocated"] += 1
    print(
        f"  Relocated: {game.system}/{game.game_name}",
        file=sys.stderr,
    )


def _relocate_archive(
    game: GamePlan,
    old: ExistingArchive,
    target_dir: Path,
    resolver: SidecarResolver,
    db: CacheDB,
) -> None:
    """Relocate an archive file + sidecar to target_dir.

    Cross-filesystem detection: if the inode changes after the move,
    the file was copied (not atomically renamed). In that case, re-verify
    BLAKE3 of the target before proceeding. Updates the sidecar metadata
    to reflect the file at its new location.
    """
    new_archive = target_dir / old.path.name
    new_rscf = resolver.sidecar_path(new_archive)

    assert not new_archive.exists(), f"Target already exists: {new_archive}"

    old_path_str = str(old.path)
    old_parent = old.path.parent
    old_inode = old.sidecar.container_inode

    shutil.move(str(old.path), str(new_archive))
    if old.rscf_path.exists():
        shutil.move(str(old.rscf_path), str(new_rscf))

    # Cross-filesystem detection: inode change means copy occurred
    new_stat = new_archive.stat()
    if old_inode and new_stat.st_ino != old_inode:
        # Re-verify BLAKE3 — the copy may have corrupted the file
        if not _verify_on_target(new_archive, old.sidecar.container_blake3):
            # Verification failed — cannot trust the target
            raise RuntimeError(
                f"Cross-filesystem re-verification failed for "
                f"{game.game_name}: BLAKE3 mismatch after move"
            )

    # Update sidecar metadata to reflect the file at its new location
    updated_sidecar = Sidecar(
        container_blake3=old.sidecar.container_blake3,
        container_size=new_stat.st_size,
        container_mtime_ns=new_stat.st_mtime_ns,
        container_ctime_ns=new_stat.st_ctime_ns,
        container_inode=new_stat.st_ino,
        renderer=old.sidecar.renderer,
        files=old.sidecar.files,
    )
    write_sidecar(updated_sidecar, new_rscf)

    # Update DB: delete old entries, insert new
    db.delete_romroot_entries(old_path_str)
    for entry in old.sidecar.files:
        db.upsert_romroot(
            path=str(new_archive),
            system=game.system,
            game_name=game.game_name,
            rom_name=entry.path,
            crc32=entry.crc32,
            md5=entry.md5,
            sha1=entry.sha1,
            sha256=entry.sha256,
            blake3=entry.blake3,
            rscf_path=str(new_rscf),
        )

    _cleanup_empty_parents(old_parent)


def _relocate_directory(
    game: GamePlan,
    old: ExistingArchive,
    target_dir: Path,
    resolver: SidecarResolver,
    db: CacheDB,
) -> None:
    """Relocate a game directory (none profile) to target_dir.

    Cross-filesystem detection: after the move, each file's inode is
    compared against its sidecar. If any inode changed, re-verify BLAKE3.
    Updates all sidecars with new file metadata.
    """
    new_game_dir = target_dir / old.path.name

    assert not new_game_dir.exists(), f"Target already exists: {new_game_dir}"

    old_parent = old.path.parent

    # Collect old inodes from sidecars before the move
    old_inodes: dict[str, int] = {}
    for rom_file in old.path.iterdir():
        if rom_file.is_file() and rom_file.suffix != ".rscf":
            rscf_path = resolver.sidecar_path(rom_file)
            if rscf_path.exists():
                try:
                    sc = read_sidecar(rscf_path)
                    old_inodes[rom_file.name] = sc.container_inode
                except RscfError:
                    pass

    # Delete old DB entries (keyed by individual file paths)
    for rom_file in old.path.iterdir():
        if rom_file.is_file() and rom_file.suffix != ".rscf":
            db.delete_romroot_entries(str(rom_file))

    shutil.move(str(old.path), str(new_game_dir))

    # Re-register with new paths, verify on inode change, update sidecars
    for rom_file in sorted(new_game_dir.iterdir()):
        if not rom_file.is_file() or rom_file.suffix == ".rscf":
            continue
        rscf_path = resolver.sidecar_path(rom_file)
        if rscf_path.exists():
            try:
                sc = read_sidecar(rscf_path)

                # Cross-filesystem detection
                new_stat = rom_file.stat()
                old_inode = old_inodes.get(rom_file.name, 0)
                if old_inode and new_stat.st_ino != old_inode:
                    if not _verify_on_target(rom_file, sc.container_blake3):
                        raise RuntimeError(
                            f"Cross-filesystem re-verification failed for "
                            f"{rom_file.name} in {game.game_name}"
                        )

                # Update sidecar metadata
                updated = Sidecar(
                    container_blake3=sc.container_blake3,
                    container_size=new_stat.st_size,
                    container_mtime_ns=new_stat.st_mtime_ns,
                    container_ctime_ns=new_stat.st_ctime_ns,
                    container_inode=new_stat.st_ino,
                    renderer=sc.renderer,
                    files=sc.files,
                )
                write_sidecar(updated, rscf_path)

                for entry in sc.files:
                    db.upsert_romroot(
                        path=str(rom_file),
                        system=game.system,
                        game_name=game.game_name,
                        rom_name=entry.path,
                        crc32=entry.crc32,
                        md5=entry.md5,
                        sha1=entry.sha1,
                        sha256=entry.sha256,
                        blake3=entry.blake3,
                        rscf_path=str(rscf_path),
                    )
            except RscfError:
                pass

    _cleanup_empty_parents(old_parent)


def _cleanup_empty_parents(start_dir: Path) -> None:
    """Remove empty parent directories walking upward.

    Stops at the first non-empty directory.
    """
    current = start_dir
    while current.exists() and not any(current.iterdir()):
        parent = current.parent
        current.rmdir()
        current = parent


def _get_container_blake3(path: Path, resolver: SidecarResolver) -> str:
    """Get BLAKE3 of a file from its sidecar, or by hashing directly."""
    rscf_path = resolver.sidecar_path(path)
    if rscf_path.exists():
        try:
            sc = read_sidecar(rscf_path)
            if sc.container_blake3:
                return sc.container_blake3.upper()
        except RscfError:
            pass
    return hash_file(path).blake3.upper()


def _get_directory_blake3(path: Path, resolver: SidecarResolver) -> str:
    """Get a combined BLAKE3 fingerprint for a game directory.

    Concatenates sorted per-file BLAKE3 hashes. Two directories with
    identical ROM content produce the same fingerprint.
    """
    hashes = []
    for f in sorted(path.iterdir()):
        if f.is_file() and f.suffix != ".rscf":
            hashes.append(_get_container_blake3(f, resolver))
    return "".join(hashes)


def _resolve_orphan_collision(
    source: Path,
    target: Path,
    is_directory: bool,
    resolver: SidecarResolver,
) -> Path:
    """Handle collision when quarantine target already exists.

    If same BLAKE3 content: replace the existing target (same data).
    If different content: create a new target with BLAKE3 suffix to
    preserve both versions. E.g., Game_a3f7c1d2.7z.

    Returns the (possibly modified) target path.
    """
    if is_directory:
        source_hash = _get_directory_blake3(source, resolver)
        target_hash = _get_directory_blake3(target, resolver)
    else:
        source_hash = _get_container_blake3(source, resolver)
        target_hash = _get_container_blake3(target, resolver)

    if source_hash == target_hash:
        # Same content — replace silently
        if target.is_dir():
            shutil.rmtree(target)
        else:
            target.unlink()
            resolver.sidecar_path(target).unlink(missing_ok=True)
        return target

    # Different content — add BLAKE3 prefix to stem for disambiguation
    blake3_suffix = source_hash[:8].lower()
    if is_directory:
        new_target = target.parent / f"{target.name}_{blake3_suffix}"
    else:
        # Game.7z → Game_a3f7c1d2.7z, Game.tar.zst → Game_a3f7c1d2.tar.zst
        base = strip_archive_extension(target.name)
        ext = target.name[len(base):]
        new_target = target.parent / f"{base}_{blake3_suffix}{ext}"

    print(
        f"    Collision in _orphaned/: {target.name} exists with different "
        f"content, saving as {new_target.name}",
        file=sys.stderr,
    )
    return new_target


# ---------------------------------------------------------------------------
# Orphan quarantine — romroot files not claimed by any DAT
# ---------------------------------------------------------------------------

def quarantine_orphans(
    orphaned_paths: set[str],
    romroot: Path,
    db: CacheDB,
) -> int:
    """Move orphaned romroot files to romroot/_orphaned/.

    Preserves relative path structure. Updates DB entries with new paths.
    Cleans up empty parent directories after each move.

    Returns count of quarantined games (archives or directories).
    """
    if not orphaned_paths:
        return 0

    orphan_dir = romroot / ORPHANED_DIR_NAME
    resolver = SidecarResolver(StorageMode.IN_TREE)
    quarantined = 0

    # Group by container path (archive files share the same path for all
    # ROM entries; directory-mode files have individual paths)
    # Deduplicate to game level: derive game directory or archive file
    items_to_move: dict[Path, bool] = {}  # path -> is_directory

    for path_str in sorted(orphaned_paths):
        path = Path(path_str)
        if not path.exists():
            # Already gone (moved by another orphan in same game dir)
            continue

        # Check if this is a file inside a game directory (none profile)
        # by looking for sibling .rscf files
        if path.is_file():
            parent = path.parent
            # If the parent dir contains only ROM files + sidecars and
            # the parent is not the romroot itself, treat as game directory
            if (parent != romroot
                    and parent.name != ORPHANED_DIR_NAME
                    and not parent.is_relative_to(orphan_dir)):
                # Check if ALL files in this directory are orphaned
                all_orphaned = True
                for sibling in parent.iterdir():
                    if sibling.suffix == ".rscf" or not sibling.is_file():
                        continue
                    if str(sibling) not in orphaned_paths:
                        all_orphaned = False
                        break

                if all_orphaned:
                    if parent not in items_to_move:
                        items_to_move[parent] = True  # directory mode
                    continue  # skip individual file — parent handles it

            # Archive mode: single file
            if path not in items_to_move:
                items_to_move[path] = False

    for item_path, is_directory in items_to_move.items():
        try:
            rel = item_path.relative_to(romroot)
        except ValueError:
            continue

        target = orphan_dir / rel
        target.parent.mkdir(parents=True, exist_ok=True)

        # Handle collision: if target already exists in _orphaned/,
        # check if it's the same content (same BLAKE3). If same, replace
        # silently. If different, add BLAKE3 suffix to avoid data loss.
        if target.exists():
            target = _resolve_orphan_collision(
                item_path, target, is_directory, resolver,
            )
        old_parent = item_path.parent

        if is_directory:
            # Directory mode: delete DB entries, move game dir.
            # File exits romroot cleanly — ingest scan will discover it
            # in _orphaned/ via sidecar fast path on next run.
            for rom_file in item_path.iterdir():
                if rom_file.is_file() and rom_file.suffix != ".rscf":
                    db.delete_romroot_entries(str(rom_file))

            shutil.move(str(item_path), str(target))
        else:
            # Archive mode: delete DB entries, move file + sidecar.
            old_rscf = resolver.sidecar_path(item_path)
            db.delete_romroot_entries(str(item_path))

            shutil.move(str(item_path), str(target))
            new_rscf = resolver.sidecar_path(target)
            if old_rscf.exists():
                shutil.move(str(old_rscf), str(new_rscf))

        quarantined += 1
        print(
            f"  Quarantined: {rel}",
            file=sys.stderr,
        )
        _cleanup_empty_parents(old_parent)

    return quarantined


# ---------------------------------------------------------------------------
# Disposal mode — source file deletion after verified collection
# ---------------------------------------------------------------------------


def _source_dir_for_path(
    file_path: str, source_modes: dict[str, str],
) -> str | None:
    """Find the source directory that contains a given file path.

    Returns the source directory string, or None if not found.
    """
    fp = Path(file_path)
    for source_dir_str in source_modes:
        source_dir = Path(source_dir_str)
        try:
            fp.relative_to(source_dir)
            return source_dir_str
        except ValueError:
            continue
    return None


def _archive_fully_accounted(source_path: str, db: CacheDB) -> bool:
    """Check that every entry in an archive is accounted for in romroot.

    An archive is safe to dispose only when ALL files inside it have a
    matching hash in romroot_files. Files not tracked by any DAT would
    be silently destroyed otherwise.

    Plain files (non-archives) are always considered fully accounted —
    they contain exactly one ROM whose collection status is already
    verified by the game_keys check.
    """
    contents = db.get_archive_contents(source_path)
    if not contents:
        return True  # plain file or archive with no extracted entries

    for entry in contents:
        # Check each entry's hash exists in romroot
        found = False
        for ht in ("blake3", "sha1", "md5", "sha256", "crc32"):
            hv = entry[ht]
            if not hv:
                continue
            if db.find_in_romroot(ht, hv):
                found = True
                break
        if not found:
            return False

    return True


def _try_eager_disposal(
    game_key: str,
    game_to_sources: dict[str, set[str]],
    source_to_games: dict[str, set[str]],
    collected_games: set[str],
    db: CacheDB,
    stats: dict[str, int],
) -> None:
    """Dispose sources that became eligible after a game was collected.

    Called after each game is added to collected_games. Uses the reverse
    index to check only sources relevant to this game, avoiding O(all)
    scans.  Disk space is reclaimed as early as possible — critical for
    large disc systems where source + romroot cannot coexist.

    Only deletes when ALL of these conditions are met:
    1. All games that reference this source file are collected
    2. For archives: every entry inside is accounted for in romroot
       (prevents silent destruction of untracked files in mixed archives)
    """
    candidate_sources = game_to_sources.get(game_key)
    if not candidate_sources:
        return

    for source_path in sorted(candidate_sources):
        if source_path not in source_to_games:
            continue  # already disposed by an earlier game

        game_keys = source_to_games[source_path]

        # Condition 1: all games referencing this source must be collected
        if not game_keys.issubset(collected_games):
            continue

        # Condition 2: all archive entries must be in romroot
        if not _archive_fully_accounted(source_path, db):
            print(
                f"  Disposal deferred: {Path(source_path).name} "
                f"(archive has untracked entries)",
                file=sys.stderr,
            )
            continue

        path = Path(source_path)
        if not path.exists():
            del source_to_games[source_path]
            continue

        path.unlink()
        stats["disposed"] += 1
        print(
            f"  Disposed: {path.name}",
            file=sys.stderr,
        )

        # Clean up sidecar if present
        try:
            resolver = SidecarResolver(StorageMode.IN_TREE)
            sidecar = resolver.sidecar_path(path)
            if sidecar.exists():
                sidecar.unlink()
        except Exception:
            pass  # sidecar cleanup is best-effort

        del source_to_games[source_path]


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def execute_plan(
    game_plans: list[GamePlan],
    romroot: Path,
    work_dir: Path,
    compression_profile: str,
    db: CacheDB,
    source_modes: dict[str, str] | None = None,
    romroot_overrides: dict[str, Path] | None = None,
    sbi_dir: Path | None = None,
    verify_roundtrip: bool = False,
    limit: int = 0,
    partial_fallback: str = "",
    partial_min_ratio: float = 0.0,
    extraction_cache_mb: int = 2048,
    compression_map: dict[str, str] | None = None,
    global_fallback: str = "",
) -> dict[str, int]:
    """Execute the match plan — build per-game archives in romroot.

    Processes collectable and partial games (collects available ROMs).
    Complete games with wrong profile are recompressed.
    Unavailable games are skipped.

    Args:
        game_plans: List of GamePlan from match phase.
        romroot: Default romroot path.
        work_dir: Fast local work directory (SSD).
        compression_profile: Default compression profile name.
        db: Cache database.
        source_modes: Dict of source_path -> source_type.
        romroot_overrides: Per-system romroot path overrides.
        sbi_dir: Directory containing SBI files for PS1 LibCrypt games.
        verify_roundtrip: If True, extract each compressed archive after
            building and verify per-ROM BLAKE3 against the input hashes.
        limit: Stop after processing this many games (0 = no limit).
        partial_fallback: Profile to use for partial games when the main
            profile requires all tracks (e.g. aaru). Empty = skip.
        partial_min_ratio: Minimum fraction of matched ROMs for a partial
            game to be worth collecting. 0.0 = collect everything.
        extraction_cache_mb: Maximum size of extraction cache in MiB.
        compression_map: Media-type overrides (e.g. {"cd": "aaru-ps2cd-zstd"}).
        global_fallback: Global default compression profile for compatibility
            fallback. When a profile is incompatible with the detected media
            type, the cascade falls through to this. Defaults to
            compression_profile if empty.

    Returns:
        Dict with counts: processed, skipped, failed, missing.
    """
    if source_modes is None:
        source_modes = {}
    if romroot_overrides is None:
        romroot_overrides = {}
    if compression_map is None:
        compression_map = {}

    resolver = SidecarResolver(StorageMode.IN_TREE)
    limits = ExtractionLimits()

    stats = {"processed": 0, "skipped": 0, "failed": 0, "missing": 0,
             "relocated": 0, "disposed": 0}

    _ensure_dir(work_dir)

    cache_dir = work_dir / "_extraction_cache"
    cache = ExtractionCache(cache_dir, max_mb=extraction_cache_mb)
    _ensure_dir(cache_dir)

    # Disposal tracking: which source files feed which games, and which
    # games have been successfully collected this run.
    disposal_source_to_games: dict[str, set[str]] = {}
    collected_games: set[str] = set()

    # Build source→games map for disposal sources.
    # For new_ops the source_path already points to the disposal source.
    # For existing_ops (in_romroot) the source_path points to romroot,
    # so we look up the hash in scanned_files/archive_contents to find
    # disposal source copies that should be cleaned up.
    for game in game_plans:
        game_key = f"{game.system}/{game.game_name}"
        for op in game.new_ops:
            if op.source_path is None:
                continue
            source_dir = _source_dir_for_path(op.source_path, source_modes)
            if source_dir is not None and source_modes.get(str(source_dir)) == "disposal":
                disposal_source_to_games.setdefault(op.source_path, set()).add(game_key)
        for op in game.existing_ops:
            if not op.hash_value:
                continue
            # Search scanned files for disposal copies of this ROM
            for row in db.find_by_hash(op.hash_type, op.hash_value):
                src_dir = _source_dir_for_path(str(row["path"]), source_modes)
                if src_dir is not None and source_modes.get(str(src_dir)) == "disposal":
                    disposal_source_to_games.setdefault(str(row["path"]), set()).add(game_key)
            for row in db.find_archive_content_by_hash(op.hash_type, op.hash_value):
                src_dir = _source_dir_for_path(str(row["archive_path"]), source_modes)
                if src_dir is not None and source_modes.get(str(src_dir)) == "disposal":
                    disposal_source_to_games.setdefault(str(row["archive_path"]), set()).add(game_key)

    # Reverse index: game_key → source paths to check after that game is collected
    game_to_disposal_sources: dict[str, set[str]] = {}
    for src, games in disposal_source_to_games.items():
        for gk in games:
            game_to_disposal_sources.setdefault(gk, set()).add(src)

    # Pre-scan romroot directory once for O(1) per-game lookups.
    # Replaces per-game _find_existing_archive which caused O(n²) I/O.
    target_dir = _ensure_dir(romroot)
    existing_cache = _build_existing_cache(target_dir, resolver)

    try:
        for game in sorted(game_plans, key=lambda g: (g.system, g.game_name)):
            # --limit N: stop after N games processed
            if limit and stats["processed"] >= limit:
                break

            processed_before = stats["processed"]

            # --- Phase A: Resolve all state ---
            resolved = resolve_game(
                game, target_dir, compression_profile, compression_map,
                partial_fallback, partial_min_ratio, resolver,
                global_fallback=global_fallback,
                existing_cache=existing_cache,
            )

            if resolved is None:
                # Unavailable game
                stats["missing"] += len(game.ops)
                continue

            if resolved.skip_reason == "below_ratio":
                stats["missing"] += len(game.missing_ops)
                continue
            if resolved.skip_reason == "missing":
                stats["missing"] += len(game.missing_ops)
                continue

            # --- Phase B: Handle relocation if needed ---
            existing = resolved.existing
            new_ops = resolved.new_ops
            effective_profile = resolved.effective_profile
            effective_strategy = resolved.effective_strategy
            action = resolved.action

            if resolved.romroot_status == "elsewhere":
                assert resolved.old_location is not None
                is_pure_relocation = (
                    not new_ops
                    and resolved.old_location.sidecar.renderer == effective_profile
                )
                try:
                    _execute_relocate(
                        game, resolved.old_location, target_dir,
                        resolver, db, stats,
                    )
                except Exception as e:
                    print(
                        f"    Error relocating {game.game_name}: {e}",
                        file=sys.stderr,
                    )
                    stats["failed"] += len(game.ops)
                    continue

                if is_pure_relocation:
                    continue

                # Re-resolve after relocation for append/recompress
                existing = _find_existing_archive(
                    target_dir, game.game_name, resolver,
                )
                action = _determine_action(
                    existing, new_ops, effective_profile, effective_strategy,
                )

            # --- Phase C: Dispatch action ---
            if action == GameAction.NOTHING:
                stats["missing"] += len(game.ops)
                continue

            if action == GameAction.SKIP:
                stats["skipped"] += len(game.ops)
                # Already in romroot → safe for disposal
                game_key = f"{game.system}/{game.game_name}"
                collected_games.add(game_key)
                _try_eager_disposal(
                    game_key, game_to_disposal_sources,
                    disposal_source_to_games, collected_games, db, stats,
                )
                continue

            # Multi-ROM game with single_only profile
            if effective_strategy == "single_only" and len(game.ops) > 1:
                print(
                    f"  Skipping: {game.game_name} "
                    f"(multi-ROM, profile {effective_profile} is single-file only)",
                    file=sys.stderr,
                )
                stats["failed"] += len(new_ops)
                continue

            if action == GameAction.RECOMPRESS:
                print(
                    f"  Recompressing: {game.system}/{game.game_name} "
                    f"({existing.sidecar.renderer} → {effective_profile})",
                    file=sys.stderr,
                )
            else:
                print(
                    f"  Collecting: {game.system}/{game.game_name} "
                    f"({len(new_ops)} new ROM(s), action={action.value})",
                    file=sys.stderr,
                )

            try:
                if effective_strategy == "directory" and action != GameAction.RECOMPRESS:
                    if existing and not existing.is_directory:
                        _handle_archive_to_directory(
                            game, new_ops, existing, target_dir, work_dir,
                            resolver, limits, db, stats,
                        )
                    else:
                        _execute_directory(
                            game, new_ops, target_dir, work_dir,
                            resolver, limits, db, stats, cache=cache,
                        )
                elif action == GameAction.RECOMPRESS and effective_strategy == "directory":
                    assert existing is not None
                    _handle_archive_to_directory(
                        game, [], existing, target_dir, work_dir,
                        resolver, limits, db, stats,
                    )
                elif existing and existing.is_directory and effective_strategy != "directory":
                    _handle_directory_to_archive(
                        game, new_ops, existing, target_dir, work_dir,
                        effective_profile, resolver, limits, db, stats,
                    )
                else:
                    _execute_archive(
                        game, new_ops, existing, target_dir, work_dir,
                        effective_profile, effective_strategy, action, resolver,
                        limits, db, stats,
                        sbi_dir=sbi_dir,
                        verify_roundtrip=verify_roundtrip,
                        cache=cache,
                    )
            except Exception as e:
                print(
                    f"    Error processing {game.game_name}: {e}",
                    file=sys.stderr,
                )
                stats["failed"] += len(new_ops) or len(game.ops)
            finally:
                # Clean per-game work subdirs, but preserve the cache
                for subdir_name in ("_roms", "_compressed", "_existing", "_extract"):
                    subdir = work_dir / subdir_name
                    if subdir.exists():
                        shutil.rmtree(subdir)

            # Track successful collections and try eager disposal
            if stats["processed"] > processed_before:
                game_key = f"{game.system}/{game.game_name}"
                collected_games.add(game_key)
                _try_eager_disposal(
                    game_key, game_to_disposal_sources,
                    disposal_source_to_games, collected_games, db, stats,
                )

    finally:
        cache.cleanup()

    return stats


# ---------------------------------------------------------------------------
# Directory mode (none profile)
# ---------------------------------------------------------------------------

def _execute_directory(
    game: GamePlan,
    new_ops: list[MatchOp],
    target_dir: Path,
    work_dir: Path,
    resolver: SidecarResolver,
    limits: ExtractionLimits,
    db: CacheDB,
    stats: dict[str, int],
    cache: ExtractionCache | None = None,
) -> None:
    """Execute 'none' profile: copy individual files to game directory."""
    game_dir = _ensure_dir(target_dir / game.game_name)

    for op in new_ops:
        assert op.source_path is not None

        rom_in_work = _get_rom_to_work_dir(op, work_dir, limits, cache=cache)
        if rom_in_work is None or not rom_in_work.exists():
            print(f"    Failed: could not get {op.rom_name}", file=sys.stderr)
            stats["failed"] += 1
            continue

        rom_hashes = hash_file(rom_in_work)
        rom_size = rom_in_work.stat().st_size

        if op.hash_value:
            actual = getattr(rom_hashes, op.hash_type, "")
            if actual.upper() != op.hash_value.upper():
                print(
                    f"    Failed: hash mismatch for {op.rom_name}",
                    file=sys.stderr,
                )
                stats["failed"] += 1
                continue

        # Copy to game directory
        target_file = game_dir / Path(op.rom_name).name
        shutil.copy2(rom_in_work, target_file)

        # Verify
        if not _verify_on_target(target_file, rom_hashes.blake3):
            print(
                f"    Failed: verification failed for {op.rom_name}",
                file=sys.stderr,
            )
            target_file.unlink(missing_ok=True)
            stats["failed"] += 1
            continue

        # Per-file RSCF sidecar
        target_stat = target_file.stat()
        sidecar = Sidecar(
            container_blake3=rom_hashes.blake3,
            container_size=target_stat.st_size,
            container_mtime_ns=target_stat.st_mtime_ns,
            container_ctime_ns=target_stat.st_ctime_ns,
            container_inode=target_stat.st_ino,
            renderer="none",
            files=[
                FileEntry.from_hashes(
                    path=op.rom_name,
                    size=rom_size,
                    hashes=rom_hashes,
                ),
            ],
        )
        rscf_path = resolver.sidecar_path(target_file)
        write_sidecar(sidecar, rscf_path)

        db.upsert_romroot(
            path=str(target_file),
            system=game.system,
            game_name=game.game_name,
            rom_name=op.rom_name,
            crc32=rom_hashes.crc32,
            md5=rom_hashes.md5,
            sha1=rom_hashes.sha1,
            sha256=rom_hashes.sha256,
            blake3=rom_hashes.blake3,
            rscf_path=str(rscf_path),
        )
        stats["processed"] += 1


# ---------------------------------------------------------------------------
# Profile transitions
# ---------------------------------------------------------------------------

def _handle_archive_to_directory(
    game: GamePlan,
    new_ops: list[MatchOp],
    existing: ExistingArchive,
    target_dir: Path,
    work_dir: Path,
    resolver: SidecarResolver,
    limits: ExtractionLimits,
    db: CacheDB,
    stats: dict[str, int],
) -> None:
    """Transition from archive → directory (none) profile."""
    assert not existing.is_directory

    extract_dir = _ensure_dir(work_dir / "_extract")
    extracted = extract_recursive(existing.path, extract_dir, limits)

    game_dir = _ensure_dir(target_dir / game.game_name)

    # Place extracted files in game dir with per-file sidecars
    for ef in extracted:
        # Match to a sidecar entry for hashes
        entry = next(
            (e for e in existing.sidecar.files if Path(e.path).name == ef.original_name),
            None,
        )

        target_file = game_dir / ef.original_name
        shutil.copy2(ef.path, target_file)

        rom_hashes = hash_file(target_file)
        target_stat = target_file.stat()

        rom_name = entry.path if entry else ef.original_name
        sidecar = Sidecar(
            container_blake3=rom_hashes.blake3,
            container_size=target_stat.st_size,
            container_mtime_ns=target_stat.st_mtime_ns,
            container_ctime_ns=target_stat.st_ctime_ns,
            container_inode=target_stat.st_ino,
            renderer="none",
            files=[
                FileEntry.from_hashes(
                    path=rom_name,
                    size=target_stat.st_size,
                    hashes=rom_hashes,
                ),
            ],
        )
        rscf_path = resolver.sidecar_path(target_file)
        write_sidecar(sidecar, rscf_path)

        db.upsert_romroot(
            path=str(target_file),
            system=game.system,
            game_name=game.game_name,
            rom_name=rom_name,
            crc32=rom_hashes.crc32,
            md5=rom_hashes.md5,
            sha1=rom_hashes.sha1,
            sha256=rom_hashes.sha256,
            blake3=rom_hashes.blake3,
            rscf_path=str(rscf_path),
        )

    # Handle new ops too (if any)
    if new_ops:
        _execute_directory(
            game, new_ops, target_dir, work_dir,
            resolver, limits, db, stats,
        )

    # Cleanup old archive + sidecar
    _cleanup_old(existing.path, existing.rscf_path, db)

    stats["processed"] += len(extracted)


def _handle_directory_to_archive(
    game: GamePlan,
    new_ops: list[MatchOp],
    existing: ExistingArchive,
    target_dir: Path,
    work_dir: Path,
    compression_profile: str,
    resolver: SidecarResolver,
    limits: ExtractionLimits,
    db: CacheDB,
    stats: dict[str, int],
) -> None:
    """Transition from directory (none) → archive profile."""
    assert existing.is_directory

    rom_work = _ensure_dir(work_dir / "_roms")
    compress_dir = _ensure_dir(work_dir / "_compressed")

    # Copy existing files from game dir to rom_work
    existing_entries: list[FileEntry] = []
    game_dir = existing.path
    for rom_file in sorted(game_dir.iterdir()):
        if not rom_file.is_file() or rom_file.suffix == ".rscf":
            continue
        # Find matching entry for proper naming
        entry = next(
            (e for e in existing.sidecar.files if Path(e.path).name == rom_file.name),
            None,
        )
        dat_name = Path(entry.path).name if entry else rom_file.name
        dest = rom_work / dat_name
        shutil.copy2(rom_file, dest)
        if entry:
            existing_entries.append(entry)

    # Get new ROMs
    verified_roms: list[tuple[MatchOp, Path, FileHashes, int]] = []
    for op in new_ops:
        assert op.source_path is not None
        rom_in_work = _get_rom_to_work_dir(op, rom_work, limits)  # no cache for transitions
        if rom_in_work is None or not rom_in_work.exists():
            print(f"    Failed: could not get {op.rom_name}", file=sys.stderr)
            stats["failed"] += 1
            return

        rom_hashes = hash_file(rom_in_work)
        rom_size = rom_in_work.stat().st_size

        if op.hash_value:
            actual = getattr(rom_hashes, op.hash_type, "")
            if actual.upper() != op.hash_value.upper():
                print(f"    Failed: hash mismatch for {op.rom_name}", file=sys.stderr)
                stats["failed"] += 1
                return

        dat_name = Path(op.rom_name).name
        final_path = rom_work / dat_name
        if final_path != rom_in_work:
            if final_path.exists():
                final_path.unlink()
            rom_in_work.rename(final_path)

        verified_roms.append((op, final_path, rom_hashes, rom_size))

    # Compress all ROMs
    all_roms = sorted(rom_work.iterdir())
    compress_base = compress_dir / game.game_name
    result = compress(compression_profile, all_roms, compress_base)
    archive_in_work = result.output

    compressed_hashes = hash_file(archive_in_work)

    # Determine target path
    ext = profile_extension(compression_profile, len(all_roms))
    archive_name = game.game_name + ext
    target_archive = target_dir / archive_name

    shutil.copy2(archive_in_work, target_archive)

    if not _verify_on_target(target_archive, compressed_hashes.blake3):
        print(f"    Failed: target verification for {archive_name}", file=sys.stderr)
        target_archive.unlink(missing_ok=True)
        stats["failed"] += len(new_ops) + len(existing_entries)
        return

    # Build sidecar with all entries
    new_entries = [
        FileEntry.from_hashes(path=op.rom_name, size=rom_size, hashes=rom_hashes)
        for op, _, rom_hashes, rom_size in verified_roms
    ]
    new_rom_names = {Path(op.rom_name).name for op, _, _, _ in verified_roms}
    merged = [e for e in existing_entries if Path(e.path).name not in new_rom_names] + new_entries

    target_stat = target_archive.stat()
    sidecar = Sidecar(
        container_blake3=compressed_hashes.blake3,
        container_size=target_stat.st_size,
        container_mtime_ns=target_stat.st_mtime_ns,
        container_ctime_ns=target_stat.st_ctime_ns,
        container_inode=target_stat.st_ino,
        renderer=compression_profile,
        files=merged,
    )
    rscf_path = resolver.sidecar_path(target_archive)
    write_sidecar(sidecar, rscf_path)

    # Update DB
    for rom_file in game_dir.iterdir():
        if rom_file.is_file() and rom_file.suffix != ".rscf":
            db.delete_romroot_entries(str(rom_file))

    for entry in merged:
        db.upsert_romroot(
            path=str(target_archive),
            system=game.system,
            game_name=game.game_name,
            rom_name=entry.path,
            crc32=entry.crc32,
            md5=entry.md5,
            sha1=entry.sha1,
            sha256=entry.sha256,
            blake3=entry.blake3,
            rscf_path=str(rscf_path),
        )

    # Remove old game directory
    shutil.rmtree(game_dir)

    stats["processed"] += len(merged)


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def _cleanup_old(old_path: Path, old_rscf: Path, db: CacheDB) -> None:
    """Remove an old archive/file and its sidecar after replacement."""
    if old_path.is_dir():
        # Directory mode: delete per-file DB entries and rmtree
        for f in old_path.iterdir():
            if f.is_file() and f.suffix != ".rscf":
                db.delete_romroot_entries(str(f))
        shutil.rmtree(old_path)
    else:
        db.delete_romroot_entries(str(old_path))
        old_path.unlink(missing_ok=True)
        old_rscf.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Dolphin copy-as-is optimization
# ---------------------------------------------------------------------------

def _execute_dolphin_copy(
    game: GamePlan,
    op: MatchOp,
    target_dir: Path,
    compression_profile: str,
    resolver: SidecarResolver,
    db: CacheDB,
    stats: dict[str, int],
) -> bool:
    """Try to copy a source RVZ/GCZ/WIA as-is if it matches the target profile.

    Returns True if the copy succeeded, False to fall back to standard flow.
    """
    assert op.source_path is not None
    source = Path(op.source_path)

    if not _source_matches_dolphin_profile(source, compression_profile):
        return False

    # Get all 5 hashes of the inner ISO from archive_contents
    assert op.archive_entry is not None
    rows = db.find_archive_content_by_hash(op.hash_type, op.hash_value)
    if not rows:
        return False

    content_row = next(
        (r for r in rows
         if r["archive_path"] == op.source_path
         and r["entry_name"] == op.archive_entry),
        None,
    )
    if content_row is None:
        return False

    # Determine target path
    ext = profile_extension(compression_profile, 1)
    archive_name = game.game_name + ext
    target_archive = target_dir / archive_name

    # Copy source directly to target
    shutil.copy2(source, target_archive)

    # Hash the copy for container verification
    compressed_hashes = hash_file(target_archive)

    if not _verify_on_target(target_archive, compressed_hashes.blake3):
        target_archive.unlink(missing_ok=True)
        return False

    # Build sidecar with ISO hashes from archive_contents
    target_stat = target_archive.stat()
    iso_entry = FileEntry(
        path=op.rom_name,
        size=content_row["entry_size"] or 0,
        crc32=content_row["crc32"] or "",
        md5=content_row["md5"] or "",
        sha1=content_row["sha1"] or "",
        sha256=content_row["sha256"] or "",
        blake3=content_row["blake3"] or "",
    )
    sidecar = Sidecar(
        container_blake3=compressed_hashes.blake3,
        container_size=target_stat.st_size,
        container_mtime_ns=target_stat.st_mtime_ns,
        container_ctime_ns=target_stat.st_ctime_ns,
        container_inode=target_stat.st_ino,
        renderer=compression_profile,
        files=[iso_entry],
    )
    rscf_path = resolver.sidecar_path(target_archive)
    write_sidecar(sidecar, rscf_path)

    db.upsert_romroot(
        path=str(target_archive),
        system=game.system,
        game_name=game.game_name,
        rom_name=op.rom_name,
        crc32=iso_entry.crc32,
        md5=iso_entry.md5,
        sha1=iso_entry.sha1,
        sha256=iso_entry.sha256,
        blake3=iso_entry.blake3,
        rscf_path=str(rscf_path),
    )
    stats["processed"] += 1
    return True


# ---------------------------------------------------------------------------
# Aaru (.aaru) copy-as-is optimization
# ---------------------------------------------------------------------------

def _execute_dimg_copy(
    game: GamePlan,
    op: MatchOp,
    target_dir: Path,
    compression_profile: str,
    resolver: SidecarResolver,
    db: CacheDB,
    stats: dict[str, int],
) -> bool:
    """Try to copy a source .aaru as-is if it matches the target profile.

    Returns True if the copy succeeded, False to fall back to standard flow.
    """
    assert op.source_path is not None
    source = Path(op.source_path)

    if not _source_matches_dimg_profile(source, compression_profile):
        return False

    # Get all content hashes from archive_contents
    assert op.archive_entry is not None
    rows = db.find_archive_content_by_hash(op.hash_type, op.hash_value)
    if not rows:
        return False

    # Find the specific source row
    content_row = next(
        (r for r in rows
         if r["archive_path"] == op.source_path
         and r["entry_name"] == op.archive_entry),
        None,
    )
    if content_row is None:
        return False

    # Determine target path
    ext = profile_extension(compression_profile, 1)
    archive_name = game.game_name + ext
    target_archive = target_dir / archive_name

    # Copy source directly to target
    shutil.copy2(source, target_archive)

    # Hash the copy for container verification
    compressed_hashes = hash_file(target_archive)

    if not _verify_on_target(target_archive, compressed_hashes.blake3):
        target_archive.unlink(missing_ok=True)
        return False

    # Build sidecar with all track hashes from archive_contents
    # Get all entries for this source archive
    all_entries = db.get_archive_contents(op.source_path)
    file_entries = [
        FileEntry(
            path=row["entry_name"],
            size=row["entry_size"] or 0,
            crc32=row["crc32"] or "",
            md5=row["md5"] or "",
            sha1=row["sha1"] or "",
            sha256=row["sha256"] or "",
            blake3=row["blake3"] or "",
        )
        for row in all_entries
    ]

    target_stat = target_archive.stat()
    sidecar = Sidecar(
        container_blake3=compressed_hashes.blake3,
        container_size=target_stat.st_size,
        container_mtime_ns=target_stat.st_mtime_ns,
        container_ctime_ns=target_stat.st_ctime_ns,
        container_inode=target_stat.st_ino,
        renderer=compression_profile,
        files=file_entries,
    )
    rscf_path = resolver.sidecar_path(target_archive)
    write_sidecar(sidecar, rscf_path)

    for entry in file_entries:
        db.upsert_romroot(
            path=str(target_archive),
            system=game.system,
            game_name=game.game_name,
            rom_name=entry.path,
            crc32=entry.crc32,
            md5=entry.md5,
            sha1=entry.sha1,
            sha256=entry.sha256,
            blake3=entry.blake3,
            rscf_path=str(rscf_path),
        )
    stats["processed"] += 1
    return True


# ---------------------------------------------------------------------------
# Archive mode
# ---------------------------------------------------------------------------

def _execute_archive(
    game: GamePlan,
    new_ops: list[MatchOp],
    existing: ExistingArchive | None,
    target_dir: Path,
    work_dir: Path,
    compression_profile: str,
    strategy: str,
    action: GameAction,
    resolver: SidecarResolver,
    limits: ExtractionLimits,
    db: CacheDB,
    stats: dict[str, int],
    sbi_dir: Path | None = None,
    verify_roundtrip: bool = False,
    cache: ExtractionCache | None = None,
) -> None:
    """Execute archive profiles: build/update per-game archive."""
    profile = PROFILES[compression_profile]
    rom_work = _ensure_dir(work_dir / "_roms")
    compress_dir = _ensure_dir(work_dir / "_compressed")

    # Step 1: Get all ROMs into work_dir, hash and verify.
    #
    # For aaru profiles, ALL tracks must be physically present (CUE is a
    # manifest). This includes ops already "in_romroot" under a different
    # game — we need our own copy in the work directory.
    requires_all = _profile_requires_all_tracks(compression_profile)
    ops_to_fetch = list(new_ops)
    if requires_all:
        # Also fetch in_romroot ops that aren't new (shared tracks etc.)
        for op in game.existing_ops:
            if not any(n.rom_name == op.rom_name for n in new_ops):
                ops_to_fetch.append(op)

    verified_roms: list[tuple[MatchOp, Path, FileHashes, int]] = []

    for op in ops_to_fetch:
        assert op.source_path is not None

        rom_in_work = _get_rom_to_work_dir(op, rom_work, limits, cache=cache)
        if rom_in_work is None or not rom_in_work.exists():
            print(f"    Failed: could not get {op.rom_name}", file=sys.stderr)
            stats["failed"] += 1
            return  # fail entire game — archive must be consistent

        rom_hashes = hash_file(rom_in_work)
        rom_size = rom_in_work.stat().st_size

        if op.hash_value:
            actual = getattr(rom_hashes, op.hash_type, "")
            if actual.upper() != op.hash_value.upper():
                print(
                    f"    Failed: hash mismatch for {op.rom_name}",
                    file=sys.stderr,
                )
                stats["failed"] += 1
                return

        # Rename to DAT-specified name for archive entry naming
        dat_name = Path(op.rom_name).name
        final_path = rom_work / dat_name
        if final_path != rom_in_work:
            if final_path.exists():
                final_path.unlink()
            rom_in_work.rename(final_path)

        verified_roms.append((op, final_path, rom_hashes, rom_size))

    # Step 2: Collect existing FileEntry data from sidecar
    existing_entries: list[FileEntry] = []
    if existing is not None:
        existing_entries = list(existing.sidecar.files)

    # Step 3: Determine total file count and target extension
    total_count = len(existing_entries) + len(verified_roms)
    if total_count == 0:
        # Recompress with zero files shouldn't happen, but guard
        stats["skipped"] += len(game.ops)
        return

    ext = profile_extension(compression_profile, total_count)
    archive_name = game.game_name + ext
    target_archive = target_dir / archive_name

    # Step 4: Build or update the archive
    new_inputs = [path for _, path, _, _ in verified_roms]

    # Copy-as-is optimization: if source disc image already matches
    # the target profile, copy directly instead of extract+recompress.
    if action == GameAction.CREATE and new_ops:
        first_op = new_ops[0]
        all_same_source = (
            first_op.source_type == "archive_content"
            and all(op.source_path == first_op.source_path for op in new_ops)
        )
        if all_same_source:
            # Dolphin (RVZ/GCZ/WIA) — single-ROM games only
            if (strategy == "single_only"
                    and len(new_ops) == 1
                    and _execute_dolphin_copy(
                        game, first_op, target_dir, compression_profile,
                        resolver, db, stats,
                    )):
                return
            # Aaru (.aaru) — multi-track games are normal
            if (compression_profile.startswith("aaru-")
                    and _execute_dimg_copy(
                        game, first_op, target_dir, compression_profile,
                        resolver, db, stats,
                    )):
                return

    if action == GameAction.CREATE:
        # Copy SBI for PS1 aaru profiles before compression
        if compression_profile.startswith("aaru-ps1"):
            _copy_sbi_to_work_dir(game.game_name, rom_work, sbi_dir)

        compress_base = compress_dir / game.game_name
        result = compress(
            compression_profile, new_inputs, compress_base,
            verify=verify_roundtrip,
        )
        archive_in_work = result.output

    elif action == GameAction.APPEND:
        assert existing is not None
        archive_in_work = compress_dir / existing.path.name
        shutil.copy2(existing.path, archive_in_work)
        append_fn = profile["append_function"]
        append_kwargs = profile["append_kwargs"]
        append_fn(archive_in_work, new_inputs, **append_kwargs)

    elif action in (GameAction.REBUILD, GameAction.RECOMPRESS):
        # Extract existing ROMs if we have an existing archive
        if existing is not None and not existing.is_directory:
            existing_work = _ensure_dir(work_dir / "_existing")
            extracted = extract_recursive(existing.path, existing_work, limits)

            # Map extracted files to sidecar entry names.
            # Needed for single-file .zst where extracted name (stem)
            # differs from actual ROM name stored in sidecar.
            sidecar_names = [Path(e.path).name for e in existing.sidecar.files]

            # Build DAT hash lookup for verification. The DAT is the
            # source of truth — existing archives can be corrupt from a
            # bad previous run (truncated files, wrong pregap layout,
            # incomplete collection). Sidecar hashes are written at the
            # same time as the archive, so they match the corrupt data.
            dat_hashes: dict[str, tuple[str, str]] = {}
            for op in game.ops:
                if op.hash_value:
                    rom_key = Path(op.rom_name).name
                    dat_hashes[rom_key] = (op.hash_type, op.hash_value.upper())

            for ef in extracted:
                # Try direct name match first
                dest_name = ef.original_name
                if dest_name not in sidecar_names and len(extracted) == len(sidecar_names):
                    # Single-file .zst: extracted "Game" → sidecar "Game.bin"
                    idx = extracted.index(ef)
                    dest_name = sidecar_names[idx]

                # Verify against DAT hash before reuse
                expected = dat_hashes.get(dest_name)
                if expected:
                    ht, hv = expected
                    actual = getattr(hash_file(ef.path), ht, "").upper()
                    if actual != hv:
                        print(
                            f"    Warning: corrupt ROM in existing archive: "
                            f"{dest_name} (hash mismatch, skipping)",
                            file=sys.stderr,
                        )
                        continue

                dest = rom_work / dest_name
                if not dest.exists():  # don't overwrite new ROMs
                    shutil.move(str(ef.path), str(dest))

        # Compress everything in rom_work
        all_roms = sorted(rom_work.iterdir())
        compress_base = compress_dir / game.game_name
        result = compress(
            compression_profile, all_roms, compress_base,
            verify=verify_roundtrip,
        )
        archive_in_work = result.output

    else:
        assert False, f"Unexpected action {action} in _execute_archive"

    # Step 5: Hash compressed output
    compressed_hashes = hash_file(archive_in_work)

    # Step 5b: Roundtrip verify — extract the archive and verify per-ROM BLAKE3
    # Aaru profiles use dimg-tool's built-in --verify (passed via compress()),
    # so skip the external extraction+hash roundtrip for them.
    if verify_roundtrip and not _profile_requires_all_tracks(compression_profile):
        expected = [
            (op.rom_name, rom_hashes.blake3)
            for op, _, rom_hashes, _ in verified_roms
        ] + [
            (entry.path, entry.blake3)
            for entry in existing_entries
            if entry.blake3  # existing entries from sidecar
        ]
        ok, rt_errors = _verify_roundtrip(archive_in_work, expected, limits)
        if not ok:
            for rom_name in rt_errors:
                print(
                    f"    Failed: roundtrip verify for {rom_name}",
                    file=sys.stderr,
                )
            archive_in_work.unlink(missing_ok=True)
            stats["failed"] += len(new_ops) or len(game.ops)
            return

    # Step 6: Copy to target and verify
    shutil.copy2(archive_in_work, target_archive)

    if not _verify_on_target(target_archive, compressed_hashes.blake3):
        print(
            f"    Failed: target verification for {archive_name}",
            file=sys.stderr,
        )
        target_archive.unlink(missing_ok=True)
        stats["failed"] += len(new_ops) or len(game.ops)
        return

    # Step 7: Build RSCF sidecar (existing + new entries)
    new_entries = [
        FileEntry.from_hashes(
            path=op.rom_name,
            size=rom_size,
            hashes=rom_hashes,
        )
        for op, _, rom_hashes, rom_size in verified_roms
    ]

    # Merge: keep existing entries that aren't being replaced
    new_rom_names = {Path(op.rom_name).name for op, _, _, _ in verified_roms}
    merged_entries = [
        e for e in existing_entries
        if Path(e.path).name not in new_rom_names
    ] + new_entries

    target_stat = target_archive.stat()
    sidecar = Sidecar(
        container_blake3=compressed_hashes.blake3,
        container_size=target_stat.st_size,
        container_mtime_ns=target_stat.st_mtime_ns,
        container_ctime_ns=target_stat.st_ctime_ns,
        container_inode=target_stat.st_ino,
        renderer=compression_profile,
        files=merged_entries,
    )

    rscf_path = resolver.sidecar_path(target_archive)
    write_sidecar(sidecar, rscf_path)

    # Step 8: Cleanup old archive if path changed
    if existing is not None and existing.path != target_archive:
        _cleanup_old(existing.path, existing.rscf_path, db)

    # Step 9: Update DB — replace all entries for this archive
    db.delete_romroot_entries(str(target_archive))
    for entry in merged_entries:
        db.upsert_romroot(
            path=str(target_archive),
            system=game.system,
            game_name=game.game_name,
            rom_name=entry.path,
            crc32=entry.crc32,
            md5=entry.md5,
            sha1=entry.sha1,
            sha256=entry.sha256,
            blake3=entry.blake3,
            rscf_path=str(rscf_path),
        )

    stats["processed"] += len(verified_roms) or len(existing_entries)
