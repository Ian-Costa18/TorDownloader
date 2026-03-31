"""Project-specific link probing and directory enumeration logic."""

from __future__ import annotations

import logging
import re
from collections import deque
from time import sleep
from typing import Iterator, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import requests
from tqdm import tqdm

from .utils.config_utils import min_int
from .utils.url_utils import normalize_url_for_request

logger = logging.getLogger(__name__)


_PLACEHOLDER_TOKEN_RE = re.compile(r"^\s*\$\{[^}]+}\s*$")


def _is_ignored_link_target(link: str) -> bool:
    """Ignore templated or non-download href values.

    Some directory pages emit placeholders like ${href}; these should never
    be treated as concrete files.
    """
    candidate = link.strip()
    lower_candidate = candidate.lower()
    return (
        candidate == ""
        or _PLACEHOLDER_TOKEN_RE.match(candidate) is not None
        or (candidate.startswith("{{") and candidate.endswith("}}"))
        or lower_candidate.startswith("javascript:")
        or lower_candidate.startswith("mailto:")
        or candidate.startswith("#")
    )


def detect_content_type(
    session: requests.Session,
    url: str,
    request_timeout: Tuple[int, int] = (30, 120),
    probe_retries: int = 3,
) -> str:
    """Best-effort content type probe for slow onion services."""
    retries = min_int(probe_retries)
    for attempt in range(1, retries + 1):
        try:
            head_resp = session.head(
                url,
                allow_redirects=True,
                verify=False,
                timeout=request_timeout,
            )
            return head_resp.headers.get("Content-Type", "")
        except Exception as head_err:  # pylint: disable=broad-exception-caught
            logger.warning(
                "HEAD probe failed (attempt %d/%d) for %s: %s",
                attempt,
                retries,
                url,
                head_err,
            )

        try:
            with session.get(
                url,
                stream=True,
                verify=False,
                timeout=request_timeout,
            ) as get_resp:
                return get_resp.headers.get("Content-Type", "")
        except Exception as get_err:  # pylint: disable=broad-exception-caught
            logger.warning(
                "GET probe failed (attempt %d/%d) for %s: %s",
                attempt,
                retries,
                url,
                get_err,
            )

        if attempt < retries:
            sleep(2)

    return ""


def get_url_text_with_retries(
    session: requests.Session,
    url: str,
    request_timeout: Tuple[int, int] = (30, 120),
    probe_retries: int = 3,
) -> str:
    """Fetch page text with Tor-friendly retry behavior."""
    retries = min_int(probe_retries)
    for attempt in range(1, retries + 1):
        try:
            with session.get(url, verify=False, timeout=request_timeout) as resp:
                return resp.text
        except Exception as err:  # pylint: disable=broad-exception-caught
            logger.warning(
                "GET text failed (attempt %d/%d) for %s: %s",
                attempt,
                retries,
                url,
                err,
            )
            if attempt < retries:
                sleep(2)
    return ""


def get_download_links_web(
    url: str,
    regex: str,
    session: requests.Session,
    request_timeout: Tuple[int, int] = (30, 120),
    probe_retries: int = 3,
) -> list[str]:
    """Get download links from web page."""
    site = get_url_text_with_retries(
        session,
        url,
        request_timeout=request_timeout,
        probe_retries=probe_retries,
    )
    if site == "":
        logger.warning("Unable to fetch directory listing text from '%s'.", url)
        return []
    links = [
        link for link in re.findall(regex, site) if not _is_ignored_link_target(link)
    ]
    logger.info("Found %d links through url '%s'.", len(links), url)
    logger.debug("Link list: %s", ", ".join(links))
    return links


def _normalize_dir_url(url: str) -> str:
    """Normalize directory URLs so visited checks are consistent."""
    return url if url.endswith("/") else f"{url}/"


def _is_same_host_url(base_url: str, candidate_url: str) -> bool:
    """Only follow links that stay on the same host as the current directory."""
    base_host = urlparse(base_url).netloc.lower()
    candidate_host = urlparse(candidate_url).netloc.lower()
    return candidate_host == "" or candidate_host == base_host


def _relative_dir_from_root(start_url: str, current_dir_url: str) -> str:
    """Get current directory path relative to the provided root directory URL."""
    root_parsed = urlparse(_normalize_dir_url(start_url))
    current_parsed = urlparse(_normalize_dir_url(current_dir_url))

    root_path = root_parsed.path
    current_path = current_parsed.path
    if current_parsed.netloc == root_parsed.netloc and current_path.startswith(
        root_path
    ):
        return current_path[len(root_path) :].strip("/")
    return current_path.strip("/")


def stream_directory_files(
    start_url: str,
    session: requests.Session,
    disallowed_exts=None,
    visited: Optional[Set[str]] = None,
    request_timeout: Tuple[int, int] = (30, 120),
    probe_retries: int = 3,
) -> Iterator[tuple[str, str]]:
    """Breadth-first walk that yields files as soon as they are discovered."""
    if disallowed_exts is None:
        disallowed_exts = (
            ".html",
            ".htm",
            ".css",
            ".js",
            ".mjs",
            ".ico",
            ".svg",
        )
    visited_set: Set[str] = set() if visited is None else visited
    start_dir_url = _normalize_dir_url(start_url)
    queue = deque([start_dir_url])
    total_dirs = 1
    pbar = tqdm(
        total=total_dirs, desc="Enumerating directories", unit="dir", dynamic_ncols=True
    )
    file_link_regex = r'href=["\']([^"\']+)["\']'

    try:
        while queue:
            dir_url = queue.popleft()
            if dir_url in visited_set:
                continue
            visited_set.add(dir_url)

            links = get_download_links_web(
                dir_url,
                file_link_regex,
                session,
                request_timeout=request_timeout,
                probe_retries=probe_retries,
            )
            relative_dir = _relative_dir_from_root(start_dir_url, dir_url)

            for link in links:
                if link == "../":
                    continue
                if link.startswith("?"):
                    continue

                abs_url = urljoin(dir_url, link)
                if not _is_same_host_url(dir_url, abs_url):
                    continue

                abs_url = normalize_url_for_request(abs_url)

                if link.endswith("/"):
                    subdir_url = _normalize_dir_url(abs_url)
                    if subdir_url not in visited_set:
                        queue.append(subdir_url)
                        total_dirs += 1
                        pbar.total = total_dirs
                        pbar.refresh()
                    continue

                if not abs_url.lower().endswith(disallowed_exts):
                    yield abs_url, relative_dir

            pbar.update(1)
    finally:
        pbar.close()


def list_directory_entries(
    directory_url: str,
    session: requests.Session,
    request_timeout: Tuple[int, int] = (30, 120),
    probe_retries: int = 3,
    disallowed_exts=None,
) -> tuple[list[str], list[str]]:
    """List files and immediate subdirectories for a single directory URL."""
    if disallowed_exts is None:
        disallowed_exts = (
            ".html",
            ".htm",
            ".css",
            ".js",
            ".mjs",
            ".ico",
            ".svg",
        )

    current_dir = _normalize_dir_url(directory_url)
    file_link_regex = r'href=["\']([^"\']+)["\']'
    links = get_download_links_web(
        current_dir,
        file_link_regex,
        session,
        request_timeout=request_timeout,
        probe_retries=probe_retries,
    )

    file_urls: list[str] = []
    subdir_urls: list[str] = []
    for link in links:
        if link == "../":
            continue
        if link.startswith("?"):
            continue

        abs_url = urljoin(current_dir, link)
        if not _is_same_host_url(current_dir, abs_url):
            continue

        abs_url = normalize_url_for_request(abs_url)
        if link.endswith("/"):
            subdir_urls.append(_normalize_dir_url(abs_url))
            continue
        if not abs_url.lower().endswith(disallowed_exts):
            file_urls.append(abs_url)

    return file_urls, subdir_urls
