from __future__ import annotations

import time
from pathlib import Path, PurePosixPath

import requests

from tor_downloader.download_runner import run_download_jobs
from tor_downloader.file_downloader import FileDownloader
from tor_downloader.link_discovery import list_directory_entries, stream_directory_files
from tor_downloader.mirror_planner import DownloadJob


def test_list_directory_entries_finds_files_and_subdirs(fake_web_server) -> None:
    session = requests.Session()
    file_urls, subdir_urls = list_directory_entries(
        fake_web_server.root_url,
        session,
        request_timeout=(5, 5),
        probe_retries=1,
    )

    assert len(file_urls) > 0
    assert len(subdir_urls) > 0
    assert all(url.startswith(fake_web_server.base_url) for url in file_urls)
    assert all(url.startswith(fake_web_server.base_url) for url in subdir_urls)


def test_stream_directory_files_walks_nested_tree(fake_web_server) -> None:
    session = requests.Session()
    discovered = list(
        stream_directory_files(
            fake_web_server.root_url,
            session,
            request_timeout=(5, 5),
            probe_retries=1,
        )
    )

    expected_count = len(fake_web_server.files)
    assert len(discovered) == expected_count


def test_file_downloader_downloads_multichunk_file(fake_web_server, tmp_path) -> None:
    largest_path = max(
        fake_web_server.files, key=lambda p: len(fake_web_server.files[p])
    )
    url = fake_web_server.file_urls[largest_path]

    downloader = FileDownloader(
        use_tor=False,
        requests_session=requests.Session(),
        request_timeout=(5, 5),
    )
    target_dir = tmp_path / "downloads"
    target_dir.mkdir(parents=True, exist_ok=True)

    saved_path = downloader.download_file(
        url, target_dir=str(target_dir), chunk_size=256
    )
    saved_bytes = Path(saved_path).read_bytes()
    assert saved_bytes == fake_web_server.files[largest_path]


def test_file_downloader_handles_completed_file_with_416(
    fake_web_server, tmp_path
) -> None:
    any_path = next(iter(fake_web_server.files.keys()))
    url = fake_web_server.file_urls[any_path]

    downloader = FileDownloader(
        use_tor=False,
        requests_session=requests.Session(),
        request_timeout=(5, 5),
    )
    target_dir = tmp_path / "downloads"
    target_dir.mkdir(parents=True, exist_ok=True)

    first = downloader.download_file(url, target_dir=str(target_dir), chunk_size=256)
    second = downloader.download_file(url, target_dir=str(target_dir), chunk_size=256)

    assert first == second
    assert Path(second).read_bytes() == fake_web_server.files[any_path]


def test_run_download_jobs_end_to_end_with_local_server(
    fake_web_server,
    dummy_tor_instance,
    tmp_path,
) -> None:
    output_dir = tmp_path / "out"
    progress_file = tmp_path / "download_progress.sqlite3"
    jobs = [
        DownloadJob(
            relative_key="",
            candidate_urls=[fake_web_server.root_url],
            is_directory=True,
            source_entry=fake_web_server.root_url,
            bases=[],
        )
    ]

    failures = run_download_jobs(
        jobs=jobs,
        output_dir=str(output_dir),
        tor_instance=dummy_tor_instance,
        tor_port=9051,
        max_downloads=4,
        request_timeout=(5, 5),
        probe_retries=1,
        enum_workers=2,
        download_workers=2,
        progress_file=str(progress_file),
    )

    assert failures == {}

    for web_path, expected_bytes in fake_web_server.files.items():
        relative = PurePosixPath(web_path.lstrip("/"))
        local_path = output_dir / relative
        assert local_path.exists(), f"missing downloaded file: {relative}"
        assert local_path.read_bytes() == expected_bytes


def test_file_downloader_handles_slow_response_under_30_seconds(
    onion_like_server,
    tmp_path,
) -> None:
    downloader = FileDownloader(
        use_tor=False,
        requests_session=requests.Session(),
        request_timeout=(5, 10),
    )
    target_dir = tmp_path / "slow"
    target_dir.mkdir(parents=True, exist_ok=True)

    start = time.monotonic()
    saved_path = downloader.download_file(
        onion_like_server.slow_url,
        target_dir=str(target_dir),
        chunk_size=128,
    )
    elapsed = time.monotonic() - start

    assert elapsed >= 1.8
    assert elapsed < 30
    assert Path(saved_path).read_bytes() == onion_like_server.files["/slow/slow.bin"]


def test_run_download_jobs_falls_back_after_timeout(
    onion_like_server,
    dummy_tor_instance,
    tmp_path,
) -> None:
    output_dir = tmp_path / "out"
    progress_file = tmp_path / "download_progress.sqlite3"
    jobs = [
        DownloadJob(
            relative_key="mirror/stable.bin",
            candidate_urls=[onion_like_server.slow_url, onion_like_server.stable_url],
            is_directory=False,
            source_entry="mirror/stable.bin",
            bases=[],
        )
    ]

    failures = run_download_jobs(
        jobs=jobs,
        output_dir=str(output_dir),
        tor_instance=dummy_tor_instance,
        tor_port=9051,
        max_downloads=2,
        request_timeout=(1, 1),
        probe_retries=2,
        enum_workers=1,
        download_workers=1,
        progress_file=str(progress_file),
    )

    assert failures == {}
    saved = output_dir / "mirror" / "stable.bin"
    assert saved.exists()
    assert saved.read_bytes() == onion_like_server.files["/stable.bin"]


def test_run_download_jobs_falls_back_after_truncated_transfer(
    onion_like_server,
    dummy_tor_instance,
    tmp_path,
) -> None:
    output_dir = tmp_path / "out"
    progress_file = tmp_path / "download_progress.sqlite3"
    jobs = [
        DownloadJob(
            relative_key="mirror/stable.bin",
            candidate_urls=[
                onion_like_server.truncated_url,
                onion_like_server.stable_url,
            ],
            is_directory=False,
            source_entry="mirror/stable.bin",
            bases=[],
        )
    ]

    failures = run_download_jobs(
        jobs=jobs,
        output_dir=str(output_dir),
        tor_instance=dummy_tor_instance,
        tor_port=9051,
        max_downloads=2,
        request_timeout=(5, 10),
        probe_retries=2,
        enum_workers=1,
        download_workers=1,
        progress_file=str(progress_file),
    )

    assert failures == {}
    saved = output_dir / "mirror" / "stable.bin"
    assert saved.exists()
    assert saved.read_bytes() == onion_like_server.files["/stable.bin"]
