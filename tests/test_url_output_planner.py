from __future__ import annotations

from tor_downloader.link_specs import LinksSpec
from tor_downloader.mirror_planner import plan_download_jobs
from tor_downloader.output_layout import (
    dedupe_preserve_order,
    filename_from_url,
    get_target_dir,
    normalize_relative_path,
    relative_path_from_url,
)
from tor_downloader.url_utils import ensure_trailing_slash, normalize_url_for_request


def test_normalize_url_for_request_encodes_unsafe_chars() -> None:
    url = "http://example.onion/path with spaces/and#hash/file.txt?q=hello world&x=1#ignored"
    normalized = normalize_url_for_request(url)
    assert "path%20with%20spaces" in normalized
    assert "and%23hash" in normalized
    assert "q=hello%20world" in normalized


def test_ensure_trailing_slash() -> None:
    assert ensure_trailing_slash("") == ""
    assert ensure_trailing_slash("http://a.onion") == "http://a.onion/"
    assert ensure_trailing_slash("http://a.onion/") == "http://a.onion/"


def test_output_layout_helpers() -> None:
    assert normalize_relative_path("../a//b\\c", directory=False) == "_up_/a/b/c"
    assert normalize_relative_path("folder/sub", directory=True) == "folder/sub/"
    assert relative_path_from_url("http://host/base/one/two.txt", keep_filename=True) == "base/one/two.txt"
    assert relative_path_from_url("http://host/base/one/two.txt", keep_filename=False) == "base/one"
    assert filename_from_url("http://host/base/one/two.txt") == "two.txt"
    assert str(get_target_dir("/tmp/output", "a/b/file.bin")).endswith("a/b")


def test_dedupe_preserve_order() -> None:
    assert dedupe_preserve_order(["a", "b", "a", "c", "b"]) == ["a", "b", "c"]


def test_plan_download_jobs_mirror_mode() -> None:
    spec = LinksSpec(
        mode="mirror",
        bases=["http://m1.onion", "http://m2.onion/"],
        files=["folder/file1.bin", "folder/dir2/", "http://single.onion/path/file2.dat"],
    )

    jobs = plan_download_jobs(spec)

    file_job = next(job for job in jobs if job.relative_key == "folder/file1.bin")
    assert file_job.is_directory is False
    assert file_job.candidate_urls == [
        "http://m1.onion/folder/file1.bin",
        "http://m2.onion/folder/file1.bin",
    ]

    dir_job = next(job for job in jobs if job.relative_key == "folder/dir2/")
    assert dir_job.is_directory is True
    assert dir_job.candidate_urls == [
        "http://m1.onion/folder/dir2/",
        "http://m2.onion/folder/dir2/",
    ]

    absolute_job = next(job for job in jobs if job.source_entry.startswith("http://single.onion"))
    assert absolute_job.candidate_urls == ["http://single.onion/path/file2.dat"]
