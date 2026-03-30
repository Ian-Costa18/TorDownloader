"""Build logical download jobs from parsed link specs."""

from __future__ import annotations

from dataclasses import dataclass, field
from urllib.parse import urljoin, urlparse

from .link_specs import LinksSpec
from .output_layout import normalize_relative_path, relative_path_from_url


@dataclass
class DownloadJob:
    """A logical file or directory download target."""

    relative_key: str
    candidate_urls: list[str]
    is_directory: bool = False
    source_entry: str = ""
    bases: list[str] = field(default_factory=list)


def _is_absolute_url(value: str) -> bool:
    parsed = urlparse(value)
    return bool(parsed.scheme and parsed.netloc)


def _with_trailing_slash(url: str) -> str:
    return url if url.endswith("/") else f"{url}/"


def plan_download_jobs(spec: LinksSpec) -> list[DownloadJob]:
    """Plan logical jobs from links spec."""
    jobs: list[DownloadJob] = []
    sequence = 0

    if spec.mode == "list":
        for link in spec.links:
            sequence += 1
            is_directory = link.rstrip().endswith("/")
            relative = relative_path_from_url(link, keep_filename=is_directory)
            relative = normalize_relative_path(relative, directory=is_directory)
            if not is_directory and relative == "":
                relative = f"unnamed_file_{sequence}"
            jobs.append(
                DownloadJob(
                    relative_key=relative,
                    candidate_urls=[link],
                    is_directory=is_directory,
                    source_entry=link,
                )
            )
        return jobs

    for file_entry in spec.files:
        sequence += 1
        is_directory = file_entry.rstrip().endswith("/")

        if _is_absolute_url(file_entry):
            relative = relative_path_from_url(file_entry, keep_filename=is_directory)
            relative = normalize_relative_path(relative, directory=is_directory)
            if not is_directory and relative == "":
                relative = f"unnamed_file_{sequence}"
            jobs.append(
                DownloadJob(
                    relative_key=relative,
                    candidate_urls=[file_entry],
                    is_directory=is_directory,
                    source_entry=file_entry,
                )
            )
            continue

        relative = normalize_relative_path(file_entry, directory=is_directory)
        if not is_directory and relative == "":
            relative = f"unnamed_file_{sequence}"
        candidates = [
            urljoin(_with_trailing_slash(base), relative) for base in spec.bases
        ]
        jobs.append(
            DownloadJob(
                relative_key=relative,
                candidate_urls=candidates,
                is_directory=is_directory,
                source_entry=file_entry,
                bases=spec.bases,
            )
        )

    return jobs
