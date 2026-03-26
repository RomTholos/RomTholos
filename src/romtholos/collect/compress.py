"""Compression profiles for ROM storage.

Each profile defines how to pack ROM files into an archive format.
Profiles are used both for romroot storage and for rendering collections.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path


class CompressionError(Exception):
    """Compression operation failed."""


@dataclass(frozen=True)
class CompressResult:
    """Result of a compression operation."""

    output: Path
    """Path to the compressed output file."""

    original_size: int
    """Total size of input files in bytes."""

    compressed_size: int
    """Size of compressed output in bytes."""

    @property
    def ratio(self) -> float:
        if self.compressed_size == 0:
            return 0.0
        return self.original_size / self.compressed_size


def _total_size(paths: list[Path]) -> int:
    """Sum of file sizes."""
    return sum(p.stat().st_size for p in paths)


def _run(cmd: list[str], timeout: int = 3600) -> None:
    """Run a subprocess, raise on failure."""
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise CompressionError(f"Command failed: {' '.join(cmd)}\n{result.stderr}")


def compress_zstd(
    inputs: list[Path],
    output: Path,
    level: int = 19,
) -> CompressResult:
    """Compress file(s) with zstd.

    For single files: produces output.zst
    For multiple files: creates a tar.zst archive.
    """
    orig_size = _total_size(inputs)

    if len(inputs) == 1:
        out_path = output.with_suffix(output.suffix + ".zst")
        _run(["zstd", f"-{level}", "-o", str(out_path), str(inputs[0])])
    else:
        # tar + zstd for multiple files
        out_path = output.with_suffix(".tar.zst")
        input_names = [p.name for p in inputs]
        input_dir = inputs[0].parent
        _run(
            ["tar", f"--use-compress-program=zstd -{level}",
             "-cf", str(out_path), "-C", str(input_dir)] + input_names
        )

    return CompressResult(
        output=out_path,
        original_size=orig_size,
        compressed_size=out_path.stat().st_size,
    )


def compress_7z(
    inputs: list[Path],
    output: Path,
    dict_size: str = "96m",
    solid: bool = True,
) -> CompressResult:
    """Compress file(s) with 7z LZMA2.

    Args:
        inputs: Files to compress.
        output: Output path (without .7z extension).
        dict_size: Dictionary size (e.g. '96m' for 96 MiB — good for large ISOs).
        solid: Use solid archive mode.
    """
    orig_size = _total_size(inputs)
    out_path = output.with_suffix(".7z")

    cmd = [
        "7z", "a",
        f"-md={dict_size}",
        "-mx=9",
        "-m0=lzma2",
    ]
    if solid:
        cmd.append("-ms=on")

    cmd.append(str(out_path))
    cmd.extend(str(p) for p in inputs)

    _run(cmd)

    return CompressResult(
        output=out_path,
        original_size=orig_size,
        compressed_size=out_path.stat().st_size,
    )


def compress_zip(
    inputs: list[Path],
    output: Path,
) -> CompressResult:
    """Compress file(s) with standard zip (deflate)."""
    orig_size = _total_size(inputs)
    out_path = output.with_suffix(".zip")

    cmd = ["zip", "-j", str(out_path)]
    cmd.extend(str(p) for p in inputs)

    _run(cmd)

    return CompressResult(
        output=out_path,
        original_size=orig_size,
        compressed_size=out_path.stat().st_size,
    )


def compress_torrentzip(
    inputs: list[Path],
    output: Path,
) -> CompressResult:
    """Compress file(s) with torrentzip (deterministic zip for hash-verified sharing).

    Requires `trrntzip` binary.
    """
    # First create a normal zip, then convert to torrentzip
    result = compress_zip(inputs, output)

    _run(["trrntzip", str(result.output)])

    return CompressResult(
        output=result.output,
        original_size=result.original_size,
        compressed_size=result.output.stat().st_size,
    )


def rewrite_cue_for_directory(cue_path: Path) -> None:
    """Rewrite a CUE file's FILE directives to match actual files in its directory.

    After renaming BIN files to DAT names, the CUE's FILE directives may
    reference old names. This scans the directory for .bin files and rewrites
    the CUE to reference them by matching track numbers or order.
    """
    text = cue_path.read_text(encoding="utf-8", errors="replace")
    directory = cue_path.parent

    # Collect actual .bin files in the directory
    actual_bins = sorted(
        f.name for f in directory.iterdir()
        if f.suffix.lower() == ".bin" and f.is_file()
    )

    # Parse FILE directives and check if they need updating
    import re
    file_pattern = re.compile(r'^FILE\s+"([^"]+)"\s+BINARY\s*$', re.MULTILINE)
    cue_files = file_pattern.findall(text)

    if not cue_files:
        return  # No FILE directives found

    # Check if all referenced files already exist
    all_exist = all((directory / name).is_file() for name in cue_files)
    if all_exist:
        return  # CUE is already correct

    # Build mapping: match by track number suffix or by position
    new_text = text
    for i, old_name in enumerate(cue_files):
        if (directory / old_name).is_file():
            continue  # This reference is fine

        # Try to find matching actual file by track number
        # Extract track number pattern like "(Track 01)" or "(Track 1)"
        track_match = re.search(r'\(Track\s*(\d+)\)', old_name)
        replacement = None

        if track_match:
            track_num = track_match.group(1)
            for actual in actual_bins:
                if f"(Track {track_num})" in actual or f"(Track {track_num.lstrip('0')})" in actual:
                    replacement = actual
                    break

        # Fallback: match by position
        if replacement is None and i < len(actual_bins):
            replacement = actual_bins[i]

        if replacement and replacement != old_name:
            new_text = new_text.replace(f'"{old_name}"', f'"{replacement}"')

    if new_text != text:
        cue_path.write_text(new_text, encoding="utf-8")


def dimg_tool_cmd() -> list[str]:
    """Detect dimg-tool binary.

    Returns the base command list to invoke dimg-tool.
    Raises CompressionError if not available.
    """
    if shutil.which("dimg-tool"):
        return ["dimg-tool"]

    raise CompressionError(
        "dimg-tool not found. Build and install from romtholos/dimg-tool."
    )


def read_dimg_header(path: Path) -> dict:
    """Read .aaru image metadata via dimg-tool.

    Returns dict with keys: format, system, media_type, tracks, sectors,
    sector_size, media_size, application, application_version, codec.
    """
    cmd = dimg_tool_cmd() + ["info", "--json", str(path)]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise CompressionError(
            f"dimg-tool info failed for {path}: {result.stderr}"
        )
    return json.loads(result.stdout)


def dimg_header_matches_profile(header: dict, profile_name: str) -> bool:
    """Check if an .aaru image's header matches a compression profile.

    Used to detect whether an existing .aaru can be copied as-is
    rather than re-extracted and recompressed.
    """
    if profile_name not in PROFILES:
        return False
    profile = PROFILES[profile_name]
    if profile["function"] is not convert_dimg:
        return False

    kwargs = profile["kwargs"]

    # Check system matches
    if header.get("system") != kwargs.get("system"):
        return False

    # Check codec matches
    if header.get("codec") != kwargs.get("codec"):
        return False

    return True


def convert_dimg(
    inputs: list[Path],
    output: Path,
    system: str = "ps1",
    codec: str = "zstd",
    verify: bool = False,
) -> CompressResult:
    """Convert CUE/BIN or ISO to .aaru using dimg-tool.

    Unlike other compress functions, dimg-tool takes a CUE file as input
    and reads referenced BIN files from the same directory. For ISO input,
    it takes the ISO directly.

    Args:
        inputs: ROM files — must include one .cue or .iso file.
        output: Output path (without .aaru extension — added automatically).
        system: Disc system (ps1, dc, saturn, etc.).
        codec: Compression codec (zstd, lzma, none).
        verify: If True, run roundtrip verification after conversion.
    """
    # Find the manifest file (.cue or .iso)
    cue_files = [p for p in inputs if p.suffix.lower() == ".cue"]
    iso_files = [p for p in inputs if p.suffix.lower() == ".iso"]

    if cue_files:
        main_input = cue_files[0]
        # Ensure CUE FILE directives match actual files on disk
        # (files may have been renamed to DAT names)
        rewrite_cue_for_directory(main_input)
    elif iso_files:
        main_input = iso_files[0]
    else:
        raise CompressionError(
            f"No .cue or .iso file found in inputs: {[p.name for p in inputs]}"
        )

    # Calculate original size (all input files)
    orig_size = _total_size(inputs)

    out_path = output.with_suffix(".aaru")
    cmd = dimg_tool_cmd()
    cmd.extend([
        "convert",
        "-i", str(main_input),
        "-o", str(out_path),
        "-s", system,
        "-c", codec,
    ])

    if verify:
        cmd.append("--verify")

    cmd.append("--json")

    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=7200,
    )
    if result.returncode != 0:
        raise CompressionError(
            f"dimg-tool convert failed: {result.stderr}"
        )

    return CompressResult(
        output=out_path,
        original_size=orig_size,
        compressed_size=out_path.stat().st_size,
    )


def dolphin_tool_cmd() -> list[str]:
    """Detect dolphin-tool — native binary or flatpak.

    Returns the base command list to invoke dolphin-tool.
    Raises CompressionError if not available.
    """
    # Try native binary first
    if shutil.which("dolphin-tool"):
        return ["dolphin-tool"]

    # Try flatpak
    if shutil.which("flatpak"):
        result = subprocess.run(
            ["flatpak", "info", "org.DolphinEmu.dolphin-emu"],
            capture_output=True, timeout=10,
        )
        if result.returncode == 0:
            return ["flatpak", "run", "--command=dolphin-tool",
                    "--filesystem=host", "org.DolphinEmu.dolphin-emu"]

    raise CompressionError(
        "dolphin-tool not found. Install Dolphin Emulator (native or flatpak)."
    )


def read_dolphin_header(path: Path) -> dict:
    """Read disc image header via dolphin-tool.

    Returns dict with keys: block_size, compression_level,
    compression_method, country, game_id, internal_name, region, revision.
    """
    cmd = dolphin_tool_cmd() + ["header", f"--input={path}", "--json"]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise CompressionError(
            f"dolphin-tool header failed for {path}: {result.stderr}"
        )
    return json.loads(result.stdout)


# Map dolphin-tool header compression names to our profile compression names
_DOLPHIN_COMPRESSION_MAP = {
    "Zstandard": "zstd",
    "bzip2": "bzip2",
    "LZMA": "lzma",
    "LZMA2": "lzma2",
    "None": "none",
}


def dolphin_header_matches_profile(header: dict, profile_name: str) -> bool:
    """Check if a disc image's header matches a compression profile.

    Used to detect whether an existing RVZ can be copied as-is
    rather than re-extracted and recompressed.
    """
    if profile_name not in PROFILES:
        return False
    profile = PROFILES[profile_name]
    if profile["function"] is not convert_dolphin:
        return False

    kwargs = profile["kwargs"]
    target_format = kwargs.get("target_format", "")

    # Check compression method and level for RVZ/WIA
    if target_format in ("rvz", "wia"):
        actual_compression = _DOLPHIN_COMPRESSION_MAP.get(
            header.get("compression_method", ""), ""
        )
        expected_compression = kwargs.get("compression", "zstd")
        if actual_compression != expected_compression:
            return False

        expected_level = kwargs.get("compression_level", 19)
        if header.get("compression_level") != expected_level:
            return False

        expected_block = kwargs.get("block_size", 131072)
        if header.get("block_size") != expected_block:
            return False

    return True


def convert_dolphin(
    inputs: list[Path],
    output: Path,
    target_format: str = "rvz",
    compression: str = "zstd",
    compression_level: int = 19,
    block_size: int = 131072,
    scrub: bool = False,
) -> CompressResult:
    """Convert disc images using dolphin-tool.

    Supports ISO/GCZ/WIA/RVZ in any direction.

    Args:
        inputs: Single disc image file.
        output: Output path (without extension — added based on format).
        target_format: Target format: iso, gcz, wia, rvz.
        compression: Compression method: none, zstd, bzip2, lzma, lzma2.
        compression_level: Compression level (19 recommended for zstd).
        block_size: Block size for GCZ/WIA/RVZ (131072 = 128 KiB recommended).
        scrub: Scrub junk data during conversion.
    """
    assert len(inputs) == 1, "dolphin-tool converts one disc image at a time"
    source = inputs[0]
    orig_size = source.stat().st_size

    ext_map = {"iso": ".iso", "gcz": ".gcz", "wia": ".wia", "rvz": ".rvz"}
    ext = ext_map.get(target_format)
    if ext is None:
        raise CompressionError(f"Unsupported dolphin format: {target_format}")

    out_path = output.with_suffix(ext)
    cmd = dolphin_tool_cmd()

    cmd.extend([
        "convert",
        "-i", str(source),
        "-o", str(out_path),
        "-f", target_format,
    ])

    if target_format in ("rvz", "wia"):
        cmd.extend(["-b", str(block_size), "-c", compression, "-l", str(compression_level)])

    if scrub:
        cmd.append("-s")

    _run(cmd, timeout=7200)  # Large images may take a while

    return CompressResult(
        output=out_path,
        original_size=orig_size,
        compressed_size=out_path.stat().st_size,
    )


def append_7z(
    archive: Path,
    inputs: list[Path],
    dict_size: str = "96m",
) -> CompressResult:
    """Add files to an existing 7z archive.

    Args:
        archive: Path to existing .7z archive.
        inputs: New files to add.
        dict_size: Dictionary size for compression.
    """
    orig_size = _total_size(inputs)

    cmd = [
        "7z", "u",
        f"-md={dict_size}",
        "-mx=9",
        "-m0=lzma2",
        str(archive),
    ]
    cmd.extend(str(p) for p in inputs)

    _run(cmd)

    return CompressResult(
        output=archive,
        original_size=orig_size,
        compressed_size=archive.stat().st_size,
    )


def append_zip(
    archive: Path,
    inputs: list[Path],
) -> CompressResult:
    """Add files to an existing zip archive.

    Args:
        archive: Path to existing .zip archive.
        inputs: New files to add.
    """
    orig_size = _total_size(inputs)

    cmd = ["zip", "-g", "-j", str(archive)]
    cmd.extend(str(p) for p in inputs)

    _run(cmd)

    return CompressResult(
        output=archive,
        original_size=orig_size,
        compressed_size=archive.stat().st_size,
    )


def copy_uncompressed(
    inputs: list[Path],
    output: Path,
) -> CompressResult:
    """Copy file without compression (passthrough).

    Output path is output_base + original file extension.
    E.g. output="game" + input="rom.gba" → "game.gba"
    """
    assert len(inputs) == 1, (
        f"Passthrough (none) requires single file, got {len(inputs)}. "
        f"Use an archive profile for multi-ROM games."
    )
    inp = inputs[0]
    orig_size = inp.stat().st_size
    out_path = output.with_suffix(inp.suffix)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    shutil.copy2(inp, out_path)

    return CompressResult(
        output=out_path,
        original_size=orig_size,
        compressed_size=out_path.stat().st_size,
    )


# Profile registry
#
# Each profile has:
#   function: compression function (inputs, output, **kwargs) -> CompressResult
#   kwargs: extra arguments for the function
#   multi_file: True if the profile can handle multiple input files
#   append_function: function to add files to existing archive (or None → rebuild)
#   append_kwargs: extra arguments for the append function
#   compatible_media: set of media types the profile can handle, or None for any.
#       Media types: "cd" (CUE/BIN), "dvd" (ISO), "gdi" (GD-ROM), "rom" (everything else).
#       Used as a safety net — the compression_map declares user intent, this declares
#       profile capability. The cascade falls back when intent doesn't match capability.
PROFILES: dict[str, dict] = {
    "zstd-19": {
        "function": compress_zstd,
        "kwargs": {"level": 19},
        "multi_file": True,
        "append_function": None,  # tar.zst must rebuild
        "append_kwargs": {},
        "compatible_media": None,
    },
    "zstd-12": {
        "function": compress_zstd,
        "kwargs": {"level": 12},
        "multi_file": True,
        "append_function": None,
        "append_kwargs": {},
        "compatible_media": None,
    },
    "zstd-3": {
        "function": compress_zstd,
        "kwargs": {"level": 3},
        "multi_file": True,
        "append_function": None,
        "append_kwargs": {},
        "compatible_media": None,
    },
    "7z-96m": {
        "function": compress_7z,
        "kwargs": {"dict_size": "96m"},
        "multi_file": True,
        "append_function": append_7z,
        "append_kwargs": {"dict_size": "96m"},
        "compatible_media": None,
    },
    "7z-16m": {
        "function": compress_7z,
        "kwargs": {"dict_size": "16m"},
        "multi_file": True,
        "append_function": append_7z,
        "append_kwargs": {"dict_size": "16m"},
        "compatible_media": None,
    },
    "zip": {
        "function": compress_zip,
        "kwargs": {},
        "multi_file": True,
        "append_function": append_zip,
        "append_kwargs": {},
        "compatible_media": None,
    },
    "torrentzip": {
        "function": compress_torrentzip,
        "kwargs": {},
        "multi_file": True,
        "append_function": None,  # deterministic, must rebuild
        "append_kwargs": {},
        "compatible_media": None,
    },
    "rvz-zstd-19": {
        "function": convert_dolphin,
        "kwargs": {"target_format": "rvz", "compression": "zstd", "compression_level": 19},
        "multi_file": False,
        "append_function": None,
        "append_kwargs": {},
        "compatible_media": {"dvd"},
    },
    "rvz-zstd-5": {
        "function": convert_dolphin,
        "kwargs": {"target_format": "rvz", "compression": "zstd", "compression_level": 5},
        "multi_file": False,
        "append_function": None,
        "append_kwargs": {},
        "compatible_media": {"dvd"},
    },
    "rvz-to-iso": {
        "function": convert_dolphin,
        "kwargs": {"target_format": "iso"},
        "multi_file": False,
        "append_function": None,
        "append_kwargs": {},
        "compatible_media": {"dvd"},
    },
    "aaru-ps1-zstd": {
        "function": convert_dimg,
        "kwargs": {"system": "ps1", "codec": "zstd"},
        "multi_file": True,
        "append_function": None,
        "append_kwargs": {},
        "compatible_media": {"cd"},
    },
    "aaru-ps1-lzma": {
        "function": convert_dimg,
        "kwargs": {"system": "ps1", "codec": "lzma"},
        "multi_file": True,
        "append_function": None,
        "append_kwargs": {},
        "compatible_media": {"cd"},
    },
    "aaru-ps2cd-zstd": {
        "function": convert_dimg,
        "kwargs": {"system": "ps2cd", "codec": "zstd"},
        "multi_file": True,
        "append_function": None,
        "append_kwargs": {},
        "compatible_media": {"cd"},
    },
    "aaru-ps2dvd-zstd": {
        "function": convert_dimg,
        "kwargs": {"system": "ps2dvd", "codec": "zstd"},
        "multi_file": True,
        "append_function": None,
        "append_kwargs": {},
        "compatible_media": {"dvd"},
    },
    "aaru-psp-zstd": {
        "function": convert_dimg,
        "kwargs": {"system": "psp", "codec": "zstd"},
        "multi_file": True,
        "append_function": None,
        "append_kwargs": {},
        "compatible_media": {"umd"},
    },
    "aaru-dc-zstd": {
        "function": convert_dimg,
        "kwargs": {"system": "dc", "codec": "zstd"},
        "multi_file": True,
        "append_function": None,
        "append_kwargs": {},
        "compatible_media": {"cd", "gdi"},
    },
    "aaru-saturn-zstd": {
        "function": convert_dimg,
        "kwargs": {"system": "saturn", "codec": "zstd"},
        "multi_file": True,
        "append_function": None,
        "append_kwargs": {},
        "compatible_media": {"cd"},
    },
    "aaru-megacd-zstd": {
        "function": convert_dimg,
        "kwargs": {"system": "megacd", "codec": "zstd"},
        "multi_file": True,
        "append_function": None,
        "append_kwargs": {},
        "compatible_media": {"cd"},
    },
    "aaru-pce-zstd": {
        "function": convert_dimg,
        "kwargs": {"system": "pce", "codec": "zstd"},
        "multi_file": True,
        "append_function": None,
        "append_kwargs": {},
        "compatible_media": {"cd"},
    },
    "aaru-neogeo-zstd": {
        "function": convert_dimg,
        "kwargs": {"system": "neogeo", "codec": "zstd"},
        "multi_file": True,
        "append_function": None,
        "append_kwargs": {},
        "compatible_media": {"cd"},
    },
    "none": {
        "function": copy_uncompressed,
        "kwargs": {},
        "multi_file": True,
        "append_function": None,  # directory mode, handled by execute
        "append_kwargs": {},
        "compatible_media": None,
    },
}


def profile_compatible(profile_name: str, media_type: str) -> bool:
    """Check if a compression profile can handle the given media type.

    Returns True if the profile accepts any media type (compatible_media is None)
    or if the media type is in the profile's compatible_media set.

    Raises KeyError if the profile name is unknown.
    """
    compat = PROFILES[profile_name]["compatible_media"]
    return compat is None or media_type in compat


_KNOWN_ARCHIVE_EXTENSIONS = (".7z", ".zip", ".zst", ".rvz", ".iso", ".gcz", ".wia", ".aaru")


def profile_extension(profile_name: str, file_count: int = 1) -> str:
    """Return the file extension a profile produces for a given file count.

    Pure function, no I/O. Returns extension including the leading dot,
    or empty string for "none" profile (directory mode).
    """
    if profile_name not in PROFILES:
        available = ", ".join(sorted(PROFILES.keys()))
        raise CompressionError(f"Unknown profile {profile_name!r}. Available: {available}")

    if profile_name == "none":
        return ""

    if profile_name.startswith("zstd-"):
        return ".zst" if file_count == 1 else ".tar.zst"

    if profile_name.startswith("7z-"):
        return ".7z"

    if profile_name in ("zip", "torrentzip"):
        return ".zip"

    if profile_name.startswith("rvz-"):
        # Check the kwargs for target_format
        fmt = PROFILES[profile_name]["kwargs"].get("target_format", "rvz")
        ext_map = {"rvz": ".rvz", "iso": ".iso", "gcz": ".gcz", "wia": ".wia"}
        return ext_map.get(fmt, ".rvz")

    if profile_name.startswith("aaru-"):
        return ".aaru"

    # Fallback: shouldn't reach here for known profiles
    raise CompressionError(f"Cannot determine extension for profile {profile_name!r}")


def strip_archive_extension(filename: str) -> str:
    """Strip known archive extensions from a filename, returning the base game name.

    Handles double extensions (.tar.zst) before single extensions.
    """
    # Double extension first
    if filename.endswith(".tar.zst"):
        return filename[: -len(".tar.zst")]

    # Single extensions
    for ext in _KNOWN_ARCHIVE_EXTENSIONS:
        if filename.endswith(ext):
            return filename[: -len(ext)]

    return filename


def compress(
    profile_name: str,
    inputs: list[Path],
    output: Path,
    *,
    verify: bool = False,
) -> CompressResult:
    """Compress files using a named profile.

    Args:
        profile_name: Name from PROFILES registry.
        inputs: Files to compress.
        output: Output path (extension added by profile).
        verify: If True and the profile supports it (aaru/dimg-tool),
            run the tool's built-in roundtrip verification.

    Returns:
        CompressResult with output path and size info.
    """
    if profile_name not in PROFILES:
        available = ", ".join(sorted(PROFILES.keys()))
        raise CompressionError(f"Unknown profile {profile_name!r}. Available: {available}")

    profile = PROFILES[profile_name]
    func = profile["function"]
    kwargs = dict(profile["kwargs"])

    if verify and func is convert_dimg:
        kwargs["verify"] = True

    return func(inputs, output, **kwargs)
