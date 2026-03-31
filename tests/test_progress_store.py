from __future__ import annotations

from tor_downloader.mirror_planner import DownloadJob
from tor_downloader.progress_store import SQLiteProgressStore


def test_progress_store_job_lifecycle(tmp_path) -> None:
    db_path = tmp_path / "progress.sqlite3"
    store = SQLiteProgressStore(db_path)

    file_job = DownloadJob(
        relative_key="folder/file.bin",
        candidate_urls=["http://x.onion/folder/file.bin"],
        is_directory=False,
        source_entry="folder/file.bin",
        bases=["http://x.onion"],
    )
    dir_job = DownloadJob(
        relative_key="folder/sub/",
        candidate_urls=["http://x.onion/folder/sub/"],
        is_directory=True,
        source_entry="folder/sub/",
        bases=["http://x.onion"],
    )

    store.enqueue_job(file_job)
    store.enqueue_job(dir_job)
    loaded = store.load_pending_jobs()
    assert len(loaded) == 2

    store.mark_job_active(file_job)
    store.reset_active_jobs()
    loaded_again = store.load_pending_jobs()
    assert any(job.relative_key == "folder/file.bin" for job in loaded_again)

    store.mark_job_done(file_job)
    remaining = store.load_pending_jobs()
    assert all(job.relative_key != "folder/file.bin" for job in remaining)

    store.mark_directory_completed("folder/sub/")
    completed = store.load_completed_directories()
    assert "folder/sub/" in completed

    store.close()


def test_progress_store_results_and_failed_filter(tmp_path) -> None:
    db_path = tmp_path / "progress.sqlite3"
    store = SQLiteProgressStore(db_path)

    store.upsert_result("a/file1.bin", "/tmp/a/file1.bin")
    store.upsert_result("a/file2.bin", "failed:a/file2.bin:timeout")

    assert store.get_result("a/file1.bin") == "/tmp/a/file1.bin"
    failed = store.fetch_failed_results()
    assert failed == {"a/file2.bin": "failed:a/file2.bin:timeout"}

    store.close()
