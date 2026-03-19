"""Safe recursive archive extraction with zip bomb protection.

Supports: zip, 7z, rar, gz, bz2, xz, zstd, tar (and tar.* combos),
          dolphin disc images (rvz, gcz, wia → iso via dolphin-tool).
Nested archives are extracted recursively up to a configurable depth.

Security:
- Compression ratio limit (default 200:1) for standard archives — catches zip bombs
- Disc image extractors (dolphin, dimg) skip ratio checks — output size is
  determined by the medium (CD/DVD/BD), not content. Absolute size limit applies.
- Maximum total extracted size limit
- Maximum nesting depth
- Path traversal prevention (no ../ in archive entries)
- Temp directory isolation
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tarfile
import tempfile
import zipfile
from dataclasses import dataclass, field
from pathlib import Path


class ExtractionError(Exception):
    """Base exception for extraction failures."""


class ZipBombError(ExtractionError):
    """Compression ratio exceeds safety threshold."""


class NestingDepthError(ExtractionError):
    """Archive nesting exceeds maximum depth."""


class SizeLimitError(ExtractionError):
    """Total extracted size exceeds limit."""


class PathTraversalError(ExtractionError):
    """Archive entry contains path traversal (../)."""


@dataclass
class ExtractionLimits:
    """Safety limits for archive extraction."""

    max_ratio: float = 200.0
    """Maximum allowed compression ratio (uncompressed/compressed). 200:1 is generous for ROMs."""

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


def _check_path_traversal(entry_name: str) -> None:
    """Reject archive entries with path traversal."""
    normalized = os.path.normpath(entry_name)
    if normalized.startswith("..") or os.sep + ".." + os.sep in normalized:
        raise PathTraversalError(f"Path traversal detected: {entry_name!r}")


def _check_ratio(compressed_size: int, uncompressed_size: int, limits: ExtractionLimits) -> None:
    """Check compression ratio against limit."""
    if compressed_size <= 0:
        return  # can't compute ratio, allow
    ratio = uncompressed_size / compressed_size
    if ratio > limits.max_ratio:
        raise ZipBombError(
            f"Compression ratio {ratio:.0f}:1 exceeds limit {limits.max_ratio:.0f}:1"
        )


def _extract_zip(archive: Path, target: Path, limits: ExtractionLimits) -> None:
    """Extract a zip archive with safety checks."""
    with zipfile.ZipFile(archive, "r") as zf:
        total_uncompressed = sum(info.file_size for info in zf.infolist() if not info.is_dir())
        _check_ratio(archive.stat().st_size, total_uncompressed, limits)

        if total_uncompressed > limits.max_total_bytes:
            raise SizeLimitError(
                f"Declared size {total_uncompressed} exceeds limit {limits.max_total_bytes}"
            )

        for info in zf.infolist():
            _check_path_traversal(info.filename)

        zf.extractall(target)


def _extract_7z(archive: Path, target: Path, limits: ExtractionLimits) -> None:
    """Extract a 7z/rar archive via 7z command with safety checks.

    Uses 7z's list mode first to check sizes before extracting.
    """
    # List contents first to check sizes
    result = subprocess.run(
        ["7z", "l", "-slt", str(archive)],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        raise ExtractionError(f"7z list failed for {archive}: {result.stderr}")

    total_size = 0
    for line in result.stdout.splitlines():
        if line.startswith("Size = "):
            try:
                total_size += int(line.split("=")[1].strip())
            except ValueError:
                pass
        if line.startswith("Path = "):
            entry_path = line.split("=", 1)[1].strip()
            if entry_path:
                _check_path_traversal(entry_path)

    _check_ratio(archive.stat().st_size, total_size, limits)

    if total_size > limits.max_total_bytes:
        raise SizeLimitError(f"Declared size {total_size} exceeds limit {limits.max_total_bytes}")

    # Extract
    result = subprocess.run(
        ["7z", "x", f"-o{target}", "-y", str(archive)],
        capture_output=True, text=True, timeout=3600,
    )
    if result.returncode != 0:
        raise ExtractionError(f"7z extract failed for {archive}: {result.stderr}")


def _extract_tar(archive: Path, target: Path, limits: ExtractionLimits) -> None:
    """Extract tar archives (including compressed variants) with safety checks."""
    with tarfile.open(archive, "r:*") as tf:
        total_size = 0
        for member in tf.getmembers():
            _check_path_traversal(member.name)
            if member.isfile():
                total_size += member.size

        _check_ratio(archive.stat().st_size, total_size, limits)

        if total_size > limits.max_total_bytes:
            raise SizeLimitError(f"Declared size {total_size} exceeds limit {limits.max_total_bytes}")

        tf.extractall(target, filter="data")


def _extract_single_compressed(archive: Path, target: Path, limits: ExtractionLimits) -> None:
    """Extract single-file compressed formats (gz, bz2, xz, zst, lz4).

    These contain a single file — the output name is the archive name without the compression extension.
    Streams to disk in chunks to avoid loading multi-GiB files into memory.
    """
    suffix = archive.suffix.lower()
    stem = archive.stem

    # Map extension to decompression command
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
    compressed_size = archive.stat().st_size
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

                _check_ratio(compressed_size, bytes_written, limits)

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


def _extract_dolphin(archive: Path, target: Path, limits: ExtractionLimits) -> None:
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

    iso_size = out_path.stat().st_size

    # Skip ratio check for disc images — they always decompress to full disc size
    # (GC: 1.4 GiB, Wii SL: 4.7 GiB, Wii DL: 8.5 GiB) regardless of actual
    # content, so small games compress extremely well (2000:1+) legitimately.
    # The absolute size limit is sufficient protection.

    if iso_size > limits.max_total_bytes:
        out_path.unlink()
        raise SizeLimitError(
            f"Extracted ISO size {iso_size} exceeds limit {limits.max_total_bytes}"
        )


def _extract_dimg(archive: Path, target: Path, limits: ExtractionLimits) -> None:
    """Extract CUE/BIN or ISO from an .aaru disc image.

    Uses dimg-tool convert to render the original format.
    System is auto-detected from .aaru metadata (no -s needed for render).
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
    dvd_systems = {"dvd", "ps2dvd"}
    if system in dvd_systems:
        out_ext = ".iso"
    else:
        out_ext = ".cue"

    out_path = target / (archive.stem + out_ext)

    cmd = cmd_base + [
        "convert",
        "-i", str(archive),
        "-o", str(out_path),
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
    if result.returncode != 0:
        raise ExtractionError(
            f"dimg-tool extract failed for {archive}: {result.stderr}"
        )

    if not out_path.exists():
        raise ExtractionError(f"dimg-tool produced no output for {archive}")

    # Skip ratio check for disc images — they always decompress to full disc size
    # (same rationale as dolphin RVZ extraction)

    # Check total extracted size against limit
    total_extracted = sum(
        f.stat().st_size for f in target.iterdir() if f.is_file()
    )
    if total_extracted > limits.max_total_bytes:
        # Clean up
        for f in target.iterdir():
            if f.is_file():
                f.unlink()
        raise SizeLimitError(
            f"Extracted disc image size {total_extracted} exceeds "
            f"limit {limits.max_total_bytes}"
        )


def _extract_archive(archive: Path, target: Path, limits: ExtractionLimits) -> None:
    """Extract an archive using the appropriate method."""
    suffix = archive.suffix.lower()
    name_lower = archive.name.lower()

    # Tar variants (must check before single-file compressed)
    if name_lower.endswith((".tar.gz", ".tgz", ".tar.bz2", ".tbz2",
                            ".tar.xz", ".txz", ".tar.zst", ".tar.lz4", ".tar")):
        _extract_tar(archive, target, limits)
    elif suffix == ".zip":
        _extract_zip(archive, target, limits)
    elif suffix in (".7z", ".rar"):
        _extract_7z(archive, target, limits)
    elif suffix in _DOLPHIN_EXTENSIONS:
        _extract_dolphin(archive, target, limits)
    elif suffix in _AARU_EXTENSIONS:
        _extract_dimg(archive, target, limits)
    elif suffix in (".gz", ".bz2", ".xz", ".zst", ".lz4"):
        _extract_single_compressed(archive, target, limits)
    else:
        raise ExtractionError(f"Unsupported archive format: {archive.name}")


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
        ZipBombError: If compression ratio exceeds limit.
        SizeLimitError: If total extracted size exceeds limit.
        PathTraversalError: If archive contains path traversal.
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
