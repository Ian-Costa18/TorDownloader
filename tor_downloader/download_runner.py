"""Execute planned jobs with mirror fallback and relative-path identity."""

from __future__ import annotations

import logging
import threading
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path
from urllib.parse import urljoin

from stemquests import TorConnectionError, TorInstance
from tqdm import tqdm

from .file_downloader import FileDownloader
from .link_discovery import list_directory_entries
from .mirror_planner import DownloadJob
from .output_layout import (
    dedupe_preserve_order,
    filename_from_url,
    get_target_dir,
    normalize_relative_path,
)

logger = logging.getLogger(__name__)
_THREAD_LOCAL = threading.local()


def _get_thread_requests_session(
    tor_instance: TorInstance,
    tor_port: int,
):
    """Reuse one Tor-backed requests session per worker thread."""
    session = getattr(_THREAD_LOCAL, "requests_session", None)
    if session is not None:
        return session

    downloader = FileDownloader(
        tor_instance=tor_instance,
        tor_port=tor_port,
    )
    _THREAD_LOCAL.requests_session = downloader.requests_session
    return _THREAD_LOCAL.requests_session


def _with_trailing_slash(url: str) -> str:
    return url if url.endswith("/") else f"{url}/"


def _dedupe_jobs(jobs: list[DownloadJob]) -> list[DownloadJob]:
    """Merge jobs sharing the same relative target to avoid overwrite races."""
    merged: dict[str, DownloadJob] = {}
    for job in jobs:
        key = (job.relative_key, job.is_directory)
        dict_key = f"{int(job.is_directory)}::{key[0]}"
        if dict_key not in merged:
            merged[dict_key] = DownloadJob(
                relative_key=job.relative_key,
                candidate_urls=list(job.candidate_urls),
                is_directory=job.is_directory,
                source_entry=job.source_entry,
                bases=list(job.bases),
            )
            continue
        existing = merged[dict_key]
        existing.candidate_urls = dedupe_preserve_order(
            [*existing.candidate_urls, *job.candidate_urls]
        )
        existing.bases = dedupe_preserve_order([*existing.bases, *job.bases])
    return list(merged.values())


def _expand_relative_path(parent_relative: str, name: str, is_directory: bool) -> str:
    """Build child relative key from parent directory and child name."""
    rel_parts = [p for p in [parent_relative.rstrip("/"), name] if p]
    return normalize_relative_path("/".join(rel_parts), directory=is_directory)


def _build_child_candidates(
    child_relative: str,
    parent_job: DownloadJob,
    discovered_url: str,
    is_directory: bool,
) -> list[str]:
    """Build candidate URLs for discovered child entries."""
    if parent_job.bases:
        suffix = f"{child_relative}/" if is_directory else child_relative
        return [
            urljoin(_with_trailing_slash(base), suffix) for base in parent_job.bases
        ]
    if is_directory:
        return [_with_trailing_slash(discovered_url)]
    return [discovered_url]


def _enumerate_directory_once(
    job: DownloadJob,
    tor_instance: TorInstance,
    tor_port: int,
    request_timeout: tuple[int, int],
    probe_retries: int,
) -> tuple[list[DownloadJob], str]:
    """Enumerate one directory task and return immediate child tasks."""
    child_jobs: list[DownloadJob] = []
    parent_relative = job.relative_key

    for directory_url in job.candidate_urls:
        session = _get_thread_requests_session(tor_instance, tor_port)
        downloader = FileDownloader(
            tor_instance=tor_instance,
            tor_port=tor_port,
            requests_session=session,
            request_timeout=request_timeout,
        )

        try:
            file_urls, subdir_urls = list_directory_entries(
                directory_url,
                downloader.requests_session,
                request_timeout=request_timeout,
                probe_retries=probe_retries,
            )
        except Exception as err:  # pylint: disable=broad-exception-caught
            logger.warning(
                "Directory enumeration failed for '%s': %s. Trying next candidate.",
                directory_url,
                err,
            )
            continue

        for subdir_url in subdir_urls:
            subdir_name = filename_from_url(subdir_url)
            if subdir_name == "":
                continue
            subdir_relative = _expand_relative_path(parent_relative, subdir_name, True)
            child_jobs.append(
                DownloadJob(
                    relative_key=subdir_relative,
                    candidate_urls=_build_child_candidates(
                        subdir_relative, job, subdir_url, True
                    ),
                    is_directory=True,
                    source_entry=job.source_entry,
                    bases=list(job.bases),
                )
            )

        for file_url in file_urls:
            filename = filename_from_url(file_url)
            if filename == "":
                continue
            file_relative = _expand_relative_path(parent_relative, filename, False)
            child_jobs.append(
                DownloadJob(
                    relative_key=file_relative,
                    candidate_urls=_build_child_candidates(
                        file_relative, job, file_url, False
                    ),
                    is_directory=False,
                    source_entry=job.source_entry,
                    bases=list(job.bases),
                )
            )

        if child_jobs:
            return _dedupe_jobs(child_jobs), f"enumerated:{job.relative_key}"

        logger.warning(
            "No files discovered for directory candidate '%s', trying next candidate.",
            directory_url,
        )

    return [], f"failed:{job.relative_key}"


def _download_file_job(
    job: DownloadJob,
    output_dir: str,
    tor_instance: TorInstance,
    tor_port: int,
    request_timeout: tuple[int, int],
) -> str:
    """Attempt one logical file using ordered candidate URLs."""
    target_dir = get_target_dir(output_dir, job.relative_key)
    relative_name = Path(job.relative_key).name
    destination = Path(target_dir) / relative_name if relative_name else None

    # First-success-wins collision policy for mirrored targets.
    if destination is not None and destination.exists() and destination.is_file():
        logger.info(
            "Skipping existing file for '%s' at '%s'.", job.relative_key, destination
        )
        return str(destination)

    for candidate in job.candidate_urls:
        try:
            session = _get_thread_requests_session(tor_instance, tor_port)
            downloader = FileDownloader(
                tor_instance=tor_instance,
                tor_port=tor_port,
                requests_session=session,
                request_timeout=request_timeout,
            )
            result = downloader.download_file(candidate, target_dir=str(target_dir))
            if result and Path(result).exists():
                return result
            logger.warning(
                "Candidate '%s' did not complete for '%s'; trying next candidate.",
                candidate,
                job.relative_key,
            )
        except TorConnectionError as err:
            logger.warning(
                "Tor connection failed for '%s' on candidate '%s': %s",
                job.relative_key,
                candidate,
                err,
            )
        except Exception as err:  # pylint: disable=broad-exception-caught
            logger.warning(
                "Candidate '%s' failed for '%s': %s",
                candidate,
                job.relative_key,
                err,
            )

    return f"failed:{job.relative_key}"


def run_download_jobs(
    jobs: list[DownloadJob],
    output_dir: str,
    tor_instance: TorInstance,
    tor_port: int,
    max_downloads: int,
    request_timeout: tuple[int, int],
    probe_retries: int,
    enum_workers: int | None = None,
    download_workers: int | None = None,
) -> dict[str, str]:
    """Run mixed directory/file jobs concurrently and return results by relative key."""
    worker_default = max(1, int(max_downloads))
    enum_limit = (
        max(1, int(enum_workers)) if enum_workers is not None else worker_default
    )
    download_limit = (
        max(1, int(download_workers))
        if download_workers is not None
        else worker_default
    )

    results: dict[str, str] = {}
    pending_dir_jobs: deque[DownloadJob] = deque()
    pending_file_jobs: deque[DownloadJob] = deque()
    queued_dir_keys: set[str] = set()
    queued_file_keys: set[str] = set()
    active_dir_keys: set[str] = set()
    active_file_keys: set[str] = set()
    completed_directories: set[str] = set()

    enum_pbar = tqdm(total=0, desc="Enumerating", unit="dir", dynamic_ncols=True)
    download_pbar = tqdm(
        total=0,
        desc="Downloading",
        unit="file",
        dynamic_ncols=True,
    )

    def _enqueue(new_job: DownloadJob) -> None:
        key = f"{int(new_job.is_directory)}::{new_job.relative_key}"

        if new_job.is_directory:
            if key in completed_directories:
                return
            if key in queued_dir_keys or key in active_dir_keys:
                return
            pending_dir_jobs.append(new_job)
            queued_dir_keys.add(key)
            enum_pbar.total += 1
            enum_pbar.refresh()
            return

        if key in queued_file_keys or key in active_file_keys:
            return
        if new_job.relative_key in results:
            return
        pending_file_jobs.append(new_job)
        queued_file_keys.add(key)
        download_pbar.total += 1
        download_pbar.refresh()

    for job in _dedupe_jobs(jobs):
        _enqueue(job)

    try:
        with (
            ThreadPoolExecutor(max_workers=enum_limit) as enum_executor,
            ThreadPoolExecutor(max_workers=download_limit) as download_executor,
        ):
            enum_inflight: dict = {}
            download_inflight: dict = {}

            def _submit_enum(job: DownloadJob):
                key = f"{int(job.is_directory)}::{job.relative_key}"
                active_dir_keys.add(key)
                return enum_executor.submit(
                    _enumerate_directory_once,
                    job,
                    tor_instance,
                    tor_port,
                    request_timeout,
                    probe_retries,
                )

            def _submit_file(job: DownloadJob):
                key = f"{int(job.is_directory)}::{job.relative_key}"
                active_file_keys.add(key)
                return download_executor.submit(
                    _download_file_job,
                    job,
                    output_dir,
                    tor_instance,
                    tor_port,
                    request_timeout,
                )

            while (
                pending_dir_jobs
                or pending_file_jobs
                or enum_inflight
                or download_inflight
            ):
                while pending_dir_jobs and len(enum_inflight) < enum_limit:
                    job = pending_dir_jobs.popleft()
                    key = f"{int(job.is_directory)}::{job.relative_key}"
                    queued_dir_keys.discard(key)
                    if key in completed_directories:
                        continue
                    future = _submit_enum(job)
                    enum_inflight[future] = job

                while pending_file_jobs and len(download_inflight) < download_limit:
                    job = pending_file_jobs.popleft()
                    key = f"{int(job.is_directory)}::{job.relative_key}"
                    queued_file_keys.discard(key)
                    if job.relative_key in results:
                        continue
                    future = _submit_file(job)
                    download_inflight[future] = job

                all_inflight = set(enum_inflight.keys()) | set(download_inflight.keys())
                if not all_inflight:
                    continue

                done, _ = wait(all_inflight, return_when=FIRST_COMPLETED)
                for future in done:
                    if future in enum_inflight:
                        job = enum_inflight.pop(future)
                        key = f"{int(job.is_directory)}::{job.relative_key}"
                        active_dir_keys.discard(key)
                        completed_directories.add(key)
                        enum_pbar.update(1)
                        try:
                            child_jobs, status = future.result()
                            results[job.relative_key or "/"] = status
                            for child_job in child_jobs:
                                _enqueue(child_job)
                        except Exception as err:  # pylint: disable=broad-exception-caught
                            results[job.relative_key or "/"] = (
                                f"failed:{job.relative_key}:{err}"
                            )
                        continue

                    job = download_inflight.pop(future)
                    key = f"{int(job.is_directory)}::{job.relative_key}"
                    active_file_keys.discard(key)
                    download_pbar.update(1)
                    try:
                        results[job.relative_key] = future.result()
                    except Exception as err:  # pylint: disable=broad-exception-caught
                        results[job.relative_key] = f"failed:{job.relative_key}:{err}"
    finally:
        enum_pbar.close()
        download_pbar.close()

    return results
