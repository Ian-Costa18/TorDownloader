"""Functions to grab download links from web pages and JSON files."""

import json
import logging
import re
from collections import deque
from time import sleep
from typing import Iterator, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import requests
from tqdm import tqdm

logger = logging.getLogger(__name__)


def detect_content_type(
    session: requests.Session,
    url: str,
    request_timeout: Tuple[int, int] = (30, 120),
    probe_retries: int = 3,
) -> str:
    """Best-effort content type probe for slow onion services.

    Args:
        session (requests.Session): Requests session to use.
        url (str): URL to probe.
        request_timeout (Tuple[int, int], optional): (connect_timeout, read_timeout).
        probe_retries (int, optional): Number of probe attempts.

    Returns:
        str: Content-Type header value if available, otherwise empty string.
    """
    retries = max(1, int(probe_retries))
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
    retries = max(1, int(probe_retries))
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


def get_download_links_json(json_path: str) -> List[str]:
    """Get download links from JSON file.

    Args:
        json_path (str): Path to JSON file.

    Returns:
        List[str]: List of download links.
    """
    with open(json_path, "r", encoding="utf-8") as file:
        links = json.load(file)
        if len(links) == 0:
            logger.error("JSON file '%s' is empty.", json_path)
            return []
        logger.info("Found %d link(s) in file '%s'", len(links), json_path)
        logger.debug("Link list: %s", ", ".join(links))
        return links


def get_download_links_web(
    url: str,
    regex: str,
    session: requests.Session,
    request_timeout: Tuple[int, int] = (30, 120),
    probe_retries: int = 3,
) -> List[str]:
    """Get download links from web page.

    Args:
        url (str): URL of web page.
        regex (str): Regex to extract download links.
        session (requests.Session): Requests session to use.

    Returns:
        List[str]: List of download links.
    """
    site = get_url_text_with_retries(
        session,
        url,
        request_timeout=request_timeout,
        probe_retries=probe_retries,
    )
    if site == "":
        logger.warning("Unable to fetch directory listing text from '%s'.", url)
        return []
    links = re.findall(regex, site)
    logger.info("Found %d links through url '%s'.", len(links), url)
    logger.debug("Link list: %s", ", ".join(links))
    return links


def enumerate_all_files(
    start_url: str,
    session: requests.Session,
    allowed_exts=None,
    visited: Optional[Set[str]] = None,
    max_workers=8,
    request_timeout: Tuple[int, int] = (30, 120),
    probe_retries: int = 3,
) -> list:
    """Recursively enumerate all downloadable file links from a directory URL and its subdirectories."""
    return [
        file_url
        for file_url, _relative_dir in stream_directory_files(
            start_url,
            session,
            allowed_exts=allowed_exts,
            visited=visited,
            request_timeout=request_timeout,
            probe_retries=probe_retries,
        )
    ]


def _normalize_dir_url(url: str) -> str:
    """Normalize directory URLs so visited checks are consistent."""
    return url if url.endswith("/") else f"{url}/"


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
    allowed_exts=None,
    visited: Optional[Set[str]] = None,
    request_timeout: Tuple[int, int] = (30, 120),
    probe_retries: int = 3,
) -> Iterator[Tuple[str, str]]:
    """Breadth-first walk that yields files as soon as they are discovered.

    Yields:
        Iterator[Tuple[str, str]]: (file_url, relative_dir_from_start_url)
    """
    if allowed_exts is None:
        allowed_exts = (
            ".zip",
            ".7z",
            ".rar",
            ".tar",
            ".gz",
            ".xz",
            ".bz2",
            ".txt",
            ".csv",
            ".json",
            ".pdf",
            ".doc",
            ".docx",
            ".xls",
            ".xlsx",
            ".jpg",
            ".jpeg",
            ".png",
            ".gif",
            ".mp4",
            ".mp3",
            ".bin",
            ".exe",
            ".iso",
        )
    visited_set: Set[str] = set() if visited is None else visited
    start_dir_url = _normalize_dir_url(start_url)
    queue = deque([start_dir_url])
    total_dirs = 1  # Will be updated as we discover more
    pbar = tqdm(
        total=total_dirs, desc="Enumerating directories", unit="dir", dynamic_ncols=True
    )
    file_link_regex = r'href=["\"]([^"\"]+)["\"]'

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
                abs_url = urljoin(dir_url, link)
                if link == "../":
                    continue

                if link.endswith("/"):
                    subdir_url = _normalize_dir_url(abs_url)
                    if subdir_url not in visited_set:
                        queue.append(subdir_url)
                        total_dirs += 1
                        pbar.total = total_dirs
                        pbar.refresh()
                    continue

                if abs_url.lower().endswith(allowed_exts):
                    yield abs_url, relative_dir

            pbar.update(1)
    finally:
        pbar.close()
