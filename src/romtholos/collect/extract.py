"""Safe recursive archive extraction.

Supports: zip, 7z, rar (via 7z), gz, bz2, xz, zstd, lz4,
          tar (and tar.* combos, via GNU tar),
          dolphin disc images (rvz, gcz, wia → iso via dolphin-tool),
          aaru disc images (.aaru → CUE/BIN or ISO via dimg-tool).
Nested archives are extracted recursively up to a configurable depth.

Safety:
- Maximum total extracted size limit (post-extraction check)
- Maximum nesting depth
- Path traversal prevention (post-extraction containment check)
- Temp directory isolation
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path


class ExtractionError(Exception):
    """Base exception for extraction failures."""


class NestingDepthError(ExtractionError):
    """Archive nesting exceeds maximum depth."""


class SizeLimitError(ExtractionError):
    """Total extracted size exceeds limit."""


class PathTraversalError(ExtractionError):
    """Extracted file escapes the target directory."""


@dataclass
class ExtractionLimits:
    """Safety limits for archive extraction."""

    max_total_bytes: int = 50 * 1024 * 1024 * 1024  # 50 GiB
    """Maximum total extracted size across all nesting levels."""

    max_depth: int = 3
    """Maximum archive nesting depth (archive inside archive inside ...)."""


@dataclass
class ExtractedFile:
    """A file extracted from an archive (or found as-is)."""

    path: Path
    """Absolute path to the extracted file on disk."""

    original_name: str
    """Original filename from the archive (or source filename)."""

    size: int
    """File size in bytes."""


# Dolphin disc image formats (extracted via dolphin-tool → ISO)
_DOLPHIN_EXTENSIONS: set[str] = {".rvz", ".gcz", ".wia"}

# Aaru disc image formats (extracted via dimg-tool → CUE/BIN or ISO)
_AARU_EXTENSIONS: set[str] = {".aaru", ".aaruf", ".dicf"}

# Known archive extensions
_ARCHIVE_EXTENSIONS: set[str] = {
    ".zip", ".7z", ".rar",
    ".gz", ".bz2", ".xz", ".zst", ".lz4",
    ".tar", ".tgz", ".tbz2", ".txz",
} | _DOLPHIN_EXTENSIONS | _AARU_EXTENSIONS


def _is_archive(path: Path) -> bool:
    """Check if a file looks like an archive by extension."""
    name = path.name.lower()

    # Handle double extensions like .tar.gz, .tar.zst
    if ".tar." in name:
        return True

    return path.suffix.lower() in _ARCHIVE_EXTENSIONS


def _validate_extraction(target: Path, limits: ExtractionLimits) -> None:
    """Validate extracted contents: size limit + path containment.

    Called after every extraction to verify safety invariants.
    On failure, cleans up extracted files and raises.
    """
    resolved_target = target.resolve()
    total_size = 0

    for dirpath, _dirnames, filenames in os.walk(target):
        for fname in filenames:
            fpath = Path(dirpath) / fname

            # Path containment: every file must be within the target dir
            if not fpath.resolve().is_relative_to(resolved_target):
                shutil.rmtree(target, ignore_errors=True)
                raise PathTraversalError(
                    f"Extracted file escapes target directory: {fpath}"
                )

            total_size += fpath.stat().st_size

    if total_size > limits.max_total_bytes:
        shutil.rmtree(target, ignore_errors=True)
        raise SizeLimitError(
            f"Extracted size {total_size} exceeds limit {limits.max_total_bytes}"
        )


def _extract_7z(archive: Path, target: Path) -> None:
    """Extract zip/7z/rar archives via 7z command."""
    result = subprocess.run(
        ["7z", "x", f"-o{target}", "-y", str(archive)],
        capture_output=True, text=True, timeout=3600,
    )
    if result.returncode != 0:
        raise ExtractionError(f"7z extract failed for {archive}: {result.stderr}")


def _extract_tar(archive: Path, target: Path) -> None:
    """Extract tar archives (including compressed variants) via GNU tar.

    GNU tar auto-detects compression (gz, bz2, xz, zst, lz4) when the
    corresponding decompressor is in PATH.
    """
    result = subprocess.run(
        [
            "tar", "xf", str(archive),
            "-C", str(target),
            "--no-same-owner", "--no-same-permissions",
        ],
        capture_output=True, text=True, timeout=3600,
    )
    if result.returncode != 0:
        raise ExtractionError(f"tar extract failed for {archive}: {result.stderr}")


def _extract_single_compressed(archive: Path, target: Path, limits: ExtractionLimits) -> None:
    """Extract single-file compressed formats (gz, bz2, xz, zst, lz4).

    These contain a single file — the output name is the archive name
    without the compression extension.  Streams to disk in chunks to
    avoid loading multi-GiB files into memory.
    """
    suffix = archive.suffix.lower()
    stem = archive.stem

    decompress_cmds: dict[str, list[str]] = {
        ".gz": ["gzip", "-d", "-k", "-c"],
        ".bz2": ["bzip2", "-d", "-k", "-c"],
        ".xz": ["xz", "-d", "-k", "-c"],
        ".zst": ["zstd", "-d", "-c"],
        ".lz4": ["lz4", "-d", "-c"],
    }

    cmd_base = decompress_cmds.get(suffix)
    if cmd_base is None:
        raise ExtractionError(f"Unsupported single-file compression: {suffix}")

    cmd = cmd_base + [str(archive)]
    output_path = target / stem
    bytes_written = 0

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        assert proc.stdout is not None
        with output_path.open("wb") as out_f:
            while True:
                chunk = proc.stdout.read(256 * 1024)
                if not chunk:
                    break
                bytes_written += len(chunk)

                if bytes_written > limits.max_total_bytes:
                    proc.kill()
                    raise SizeLimitError(
                        f"Decompressed size {bytes_written} exceeds "
                        f"limit {limits.max_total_bytes}"
                    )

                out_f.write(chunk)

        proc.wait(timeout=3600)
        if proc.returncode != 0:
            assert proc.stderr is not None
            stderr = proc.stderr.read().decode(errors="replace")
            raise ExtractionError(f"Decompression failed for {archive}: {stderr}")
    except BaseException:
        proc.kill()
        proc.wait()
        if output_path.exists():
            output_path.unlink()
        raise
    finally:
        if proc.stdout:
            proc.stdout.close()
        if proc.stderr:
            proc.stderr.close()


def _extract_dolphin(archive: Path, target: Path) -> None:
    """Extract ISO from a dolphin disc image (RVZ/GCZ/WIA).

    Uses dolphin-tool convert to produce a raw ISO.
    Output name: archive stem + ".iso" (e.g., "Game.rvz" → "Game.iso").
    """
    from romtholos.collect.compress import CompressionError, dolphin_tool_cmd

    out_path = target / (archive.stem + ".iso")

    try:
        cmd = dolphin_tool_cmd()
    except CompressionError as e:
        raise ExtractionError(str(e)) from e

    cmd.extend([
        "convert",
        f"--input={archive}",
        f"--output={out_path}",
        "-f", "iso",
    ])

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
    if result.returncode != 0:
        raise ExtractionError(
            f"dolphin-tool extract failed for {archive}: {result.stderr}"
        )

    if not out_path.exists():
        raise ExtractionError(f"dolphin-tool produced no output for {archive}")


def _extract_dimg(archive: Path, target: Path) -> None:
    """Extract CUE/BIN or ISO from an .aaru disc image.

    Uses dimg-tool convert to render the original format.
    System is auto-detected from .aaru metadata (no -s needed for render).

    CD systems always use --multi-bin (per-track BIN files). This is required
    for multi-track discs (Redump DATs list individual track hashes) and is a
    no-op for single-track discs (produces the same single BIN either way).
    """
    from romtholos.collect.compress import CompressionError, dimg_tool_cmd

    try:
        cmd_base = dimg_tool_cmd()
    except CompressionError as e:
        raise ExtractionError(str(e)) from e

    # Use dimg-tool info --json to determine if it's CD (→ CUE) or DVD (→ ISO)
    info_cmd = cmd_base + ["info", "--json", str(archive)]
    info_result = subprocess.run(
        info_cmd, capture_output=True, text=True, timeout=30,
    )
    if info_result.returncode != 0:
        raise ExtractionError(
            f"dimg-tool info failed for {archive}: {info_result.stderr}"
        )

    import json
    info = json.loads(info_result.stdout)
    system = info.get("system", "")

    # DVD systems render to ISO, CD systems render to CUE/BIN
    dvd_systems = {"dvd", "ps2dvd", "psp"}
    is_cd = system not in dvd_systems

    out_ext = ".cue" if is_cd else ".iso"
    out_path = target / (archive.stem + out_ext)

    cmd = cmd_base + [
        "convert",
        "-i", str(archive),
        "-o", str(out_path),
    ]

    # Multi-bin: produce per-track BIN files for CD systems.
    # Matches Redump multi-BIN format where each track has its own hash.
    if is_cd:
        cmd.append("--multi-bin")

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
    if result.returncode != 0:
        raise ExtractionError(
            f"dimg-tool extract failed for {archive}: {result.stderr}"
        )

    if not out_path.exists():
        raise ExtractionError(f"dimg-tool produced no output for {archive}")


def _extract_archive(archive: Path, target: Path, limits: ExtractionLimits) -> None:
    """Extract an archive using the appropriate method."""
    suffix = archive.suffix.lower()
    name_lower = archive.name.lower()

    # Tar variants (must check before single-file compressed)
    if name_lower.endswith((".tar.gz", ".tgz", ".tar.bz2", ".tbz2",
                            ".tar.xz", ".txz", ".tar.zst", ".tar.lz4", ".tar")):
        _extract_tar(archive, target)
    elif suffix in (".zip", ".7z", ".rar"):
        _extract_7z(archive, target)
    elif suffix in _DOLPHIN_EXTENSIONS:
        _extract_dolphin(archive, target)
    elif suffix in _AARU_EXTENSIONS:
        _extract_dimg(archive, target)
    elif suffix in (".gz", ".bz2", ".xz", ".zst", ".lz4"):
        _extract_single_compressed(archive, target, limits)
    else:
        raise ExtractionError(f"Unsupported archive format: {archive.name}")

    _validate_extraction(target, limits)


def extract_recursive(
    source: Path,
    work_dir: Path | None = None,
    limits: ExtractionLimits | None = None,
    _depth: int = 0,
) -> list[ExtractedFile]:
    """Recursively extract archives until only non-archive files remain.

    Args:
        source: Path to a file or directory to process.
        work_dir: Working directory for extraction. If None, a temp dir is created.
        limits: Safety limits for extraction.
        _depth: Current nesting depth (internal, do not set).

    Returns:
        List of ExtractedFile objects for all non-archive files found.

    Raises:
        NestingDepthError: If nesting exceeds max_depth.
        SizeLimitError: If total extracted size exceeds limit.
        PathTraversalError: If extracted file escapes target directory.
        ExtractionError: For other extraction failures.
    """
    if limits is None:
        limits = ExtractionLimits()

    if _depth > limits.max_depth:
        raise NestingDepthError(
            f"Archive nesting depth {_depth} exceeds limit {limits.max_depth}"
        )

    own_work_dir = work_dir is None
    if work_dir is None:
        work_dir = Path(tempfile.mkdtemp(prefix="collector_"))

    results: list[ExtractedFile] = []

    try:
        if source.is_dir():
            # Process all files in directory
            for child in sorted(source.iterdir()):
                if child.is_file():
                    results.extend(
                        extract_recursive(child, work_dir, limits, _depth)
                    )
                elif child.is_dir():
                    results.extend(
                        extract_recursive(child, work_dir, limits, _depth)
                    )
        elif source.is_file():
            if _is_archive(source):
                # Extract to a subdirectory named after the archive
                extract_target = work_dir / f"_level{_depth}_{source.stem}"
                extract_target.mkdir(parents=True, exist_ok=True)

                _extract_archive(source, extract_target, limits)

                # Recurse into extracted contents
                results.extend(
                    extract_recursive(extract_target, work_dir, limits, _depth + 1)
                )
            else:
                # Non-archive file — this is a leaf (potential ROM)
                results.append(ExtractedFile(
                    path=source,
                    original_name=source.name,
                    size=source.stat().st_size,
                ))
    except Exception:
        if own_work_dir:
            shutil.rmtree(work_dir, ignore_errors=True)
        raise

    return results
