"""Output path policy for hostless relative layout."""

from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import Iterable
from urllib.parse import unquote, urlparse


def _sanitize_part(part: str) -> str:
    """Sanitize one path segment while keeping it readable."""
    if part in {"", "."}:
        return ""
    if part == "..":
        return "_up_"
    return part.replace("\\", "_").replace("/", "_")


def normalize_relative_path(path: str, directory: bool = False) -> str:
    """Normalize a user-provided relative path into a safe posix path."""
    raw_parts = [segment.strip() for segment in path.replace("\\", "/").split("/")]
    safe_parts = [_sanitize_part(part) for part in raw_parts]
    parts = [part for part in safe_parts if part]
    normalized = str(PurePosixPath(*parts)) if parts else ""
    if directory and normalized and not normalized.endswith("/"):
        return f"{normalized}/"
    return normalized


def relative_path_from_url(url: str, keep_filename: bool) -> str:
    """Build hostless relative path from URL path only."""
    parsed = urlparse(url)
    parts = [part for part in unquote(parsed.path).split("/") if part]
    if not keep_filename and parts:
        parts = parts[:-1]
    return normalize_relative_path("/".join(parts), directory=False)


def filename_from_url(url: str) -> str:
    """Best-effort file name extraction from URL path."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    if "/" not in path:
        return unquote(path)
    return unquote(path.split("/")[-1])


def get_target_dir(output_root: str, relative_file_path: str) -> Path:
    """Get local target directory for a relative file path."""
    parent = str(PurePosixPath(relative_file_path).parent)
    if parent == ".":
        return Path(output_root)
    return Path(output_root) / parent


def dedupe_preserve_order(values: Iterable[str]) -> list[str]:
    """Dedupe while preserving insertion order."""
    seen = set()
    output = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output
