"""Utilities for run-scoped log file management.

This module handles:
1) per-run timestamped log file naming,
2) compression of prior plain-text logs,
3) retention cleanup of compressed archives.
"""

from __future__ import annotations

import gzip
import shutil
from datetime import datetime
from pathlib import Path


def build_run_log_file(base_log_file: Path, run_started_at: datetime) -> Path:
    """Create a timestamped log filename for the current run."""
    suffix = base_log_file.suffix or ".log"
    timestamp = run_started_at.strftime("%Y%m%d_%H%M%S_%f")
    return base_log_file.with_name(f"{base_log_file.stem}_{timestamp}{suffix}")


def gzip_file(path: Path) -> bool:
    """Compress a file to gzip and remove the original if successful."""
    gz_path = path.with_suffix(path.suffix + ".gz")
    if gz_path.exists():
        return False

    with open(path, "rb") as src, gzip.open(gz_path, "wb") as dst:
        shutil.copyfileobj(src, dst)
    path.unlink()
    return True


def cleanup_log_archives(
    log_dir: Path,
    log_stem: str,
    log_suffix: str,
    max_archives: int,
    max_total_bytes: int,
) -> int:
    """Delete oldest compressed log archives when retention limits are exceeded."""
    archives = sorted(
        log_dir.glob(f"{log_stem}_*{log_suffix}.gz"),
        key=lambda item: item.stat().st_mtime,
    )
    deleted = 0
    total_size = sum(item.stat().st_size for item in archives)

    while len(archives) > max_archives or total_size > max_total_bytes:
        oldest = archives.pop(0)
        size = oldest.stat().st_size
        oldest.unlink()
        total_size -= size
        deleted += 1

    return deleted


def prepare_log_files(
    base_log_file: Path,
    run_started_at: datetime,
    max_archives: int,
    max_total_mb: int,
) -> tuple[Path, int, int, list[str]]:
    """Prepare per-run logs by compressing previous logs and enforcing retention."""
    log_dir = base_log_file.parent
    log_dir.mkdir(parents=True, exist_ok=True)

    run_log_file = build_run_log_file(base_log_file, run_started_at)
    suffix = base_log_file.suffix or ".log"

    compressed = 0
    failures: list[str] = []
    for plain_log in sorted(log_dir.glob(f"{base_log_file.stem}_*{suffix}")):
        if plain_log == run_log_file or plain_log.suffix.endswith(".gz"):
            continue
        try:
            if gzip_file(plain_log):
                compressed += 1
        except OSError as err:
            failures.append(f"{plain_log}: {err}")

    # Also migrate the old single-file log style into gzip archives.
    if base_log_file.exists() and base_log_file != run_log_file:
        try:
            if gzip_file(base_log_file):
                compressed += 1
        except OSError as err:
            failures.append(f"{base_log_file}: {err}")

    deleted = 0
    max_total_bytes = max_total_mb * 1024 * 1024
    try:
        deleted = cleanup_log_archives(
            log_dir=log_dir,
            log_stem=base_log_file.stem,
            log_suffix=suffix,
            max_archives=max_archives,
            max_total_bytes=max_total_bytes,
        )
    except OSError as err:
        failures.append(f"archive cleanup failed: {err}")

    return run_log_file, compressed, deleted, failures