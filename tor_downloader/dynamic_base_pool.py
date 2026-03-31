"""Dynamic base-pool management for redirect-driven mirror roots.

This module is intentionally standalone so it can be used without changing the
existing generic download pipeline.
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import unquote, urljoin, urlparse, urlunparse

import requests
from stemquests import TorInstance

from .utils.config_utils import min_int
from .utils.url_utils import ensure_trailing_slash, normalize_url_for_request

logger = logging.getLogger(__name__)


_PLACEHOLDER_TOKEN_RE = re.compile(r"^\s*\$\{[^}]+}\s*$")


class BaseResolutionError(RuntimeError):
    """Raised when a dynamic mirror base cannot be discovered."""


def _is_placeholder_hint(candidate: str) -> bool:
    value = candidate.strip()
    return _PLACEHOLDER_TOKEN_RE.match(value) is not None or (
        value.startswith("{{") and value.endswith("}}")
    )


def _normalize_folder_name(folder: str) -> str:
    return unquote(folder.strip().strip("/")).strip()


def extract_dynamic_base(url: str, top_level_folder: str) -> str:
    """Extract mirror base from a final dataset URL.

    Example:
        final URL: http://host.onion/uuid/FOLDER/path/file.txt
        result:    http://host.onion/uuid/
    """
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        raise BaseResolutionError(f"Invalid URL for base extraction: {url}")

    folder_token = _normalize_folder_name(top_level_folder)
    if folder_token == "":
        raise BaseResolutionError("top_level_folder cannot be empty")

    raw_segments = [seg for seg in parsed.path.split("/") if seg]
    decoded_segments = [unquote(seg) for seg in raw_segments]

    try:
        folder_idx = decoded_segments.index(folder_token)
    except ValueError:
        # Some redirectors resolve to the dynamic base directly, e.g. /<uuid>/,
        # and the top-level folder is appended later by the downloader.
        if len(raw_segments) == 0:
            raise BaseResolutionError(
                f"Could not find top-level folder '{folder_token}' in URL: {url}"
            )
        fallback_base = ensure_trailing_slash(
            urlunparse(
                (
                    parsed.scheme,
                    parsed.netloc,
                    f"/{'/'.join(raw_segments)}/",
                    "",
                    "",
                    "",
                )
            )
        )
        logger.debug(
            "Extracted dynamic base (folder omitted in redirect): %s", fallback_base
        )
        return fallback_base

    if folder_idx == 0:
        raise BaseResolutionError(
            f"Expected unique path segment before folder '{folder_token}' in URL: {url}"
        )

    base_path = "/" + "/".join(raw_segments[:folder_idx]) + "/"
    extracted_base = ensure_trailing_slash(
        urlunparse((parsed.scheme, parsed.netloc, base_path, "", "", ""))
    )
    logger.debug("Extracted dynamic base from redirect path: %s", extracted_base)
    return extracted_base


class DynamicBasePool:
    """Maintain a thread-safe pool of dynamic mirror bases.

    The pool can be shared by all worker threads. When a base fails, call
    `report_base_failure(base)` to evict it and trigger replenishment.
    """

    _REDIRECT_HINT_PATTERNS = (
        re.compile(
            r"window\.location(?:\.href)?\s*=\s*[\"']([^\"']+)[\"']",
            re.IGNORECASE,
        ),
        re.compile(
            r"location\.replace\(\s*[\"']([^\"']+)[\"']\s*\)",
            re.IGNORECASE,
        ),
        re.compile(
            r"<meta[^>]+http-equiv=[\"']refresh[\"'][^>]+content=[\"'][^\"']*url=([^\"']+)[\"']",
            re.IGNORECASE,
        ),
        re.compile(r"href=[\"']([^\"']+)[\"']", re.IGNORECASE),
    )

    def __init__(
        self,
        bootstrap_urls: list[str],
        top_level_folder: str,
        min_bases: int = 5,
        max_bases: Optional[int] = None,
        request_timeout: tuple[int, int] = (30, 120),
        tor_instance: Optional[TorInstance] = None,
        tor_port: int = 9051,
        requests_session: Optional[requests.Session] = None,
        initial_bases: Optional[Iterable[str]] = None,
        refresh_cooldown_sec: float = 10.0,
        discovery_workers: Optional[int] = None,
        session_init_retries: int = 3,
        bootstrap_retries: int = 2,
        retry_backoff_sec: float = 1.0,
    ) -> None:
        if not bootstrap_urls:
            raise ValueError("bootstrap_urls must contain at least one URL")
        if min_bases < 1:
            raise ValueError("min_bases must be >= 1")
        if max_bases is not None and max_bases < min_bases:
            raise ValueError("max_bases must be >= min_bases")

        self.bootstrap_urls = [u.strip() for u in bootstrap_urls if u.strip()]
        self.top_level_folder = top_level_folder
        self.min_bases = min_bases
        self.max_bases = max_bases
        self.request_timeout = request_timeout
        self.refresh_cooldown_sec = max(0.0, float(refresh_cooldown_sec))
        self.discovery_workers = (
            min_int(discovery_workers)
            if discovery_workers is not None
            else min_int(min_bases)
        )
        self.session_init_retries = min_int(session_init_retries)
        self.bootstrap_retries = min_int(bootstrap_retries)
        self.retry_backoff_sec = max(0.0, float(retry_backoff_sec))

        self._tor_instance = tor_instance or TorInstance(socks_port=tor_port)
        self._session_is_external = requests_session is not None
        self._session = requests_session
        self._lock = threading.RLock()
        self._last_refresh_ts = 0.0

        self._bases: list[str] = []
        self._known_bases: set[str] = set()

        logger.info(
            "DynamicBasePool initialized: bootstrap_urls=%d min_bases=%d discovery_workers=%d",
            len(self.bootstrap_urls),
            self.min_bases,
            self.discovery_workers,
        )

        if initial_bases:
            for base in initial_bases:
                self._add_base(base)

    def _add_base(self, base: str) -> bool:
        normalized = ensure_trailing_slash(base.strip())
        if normalized in self._known_bases:
            logger.debug("Skipping duplicate mirror base: %s", normalized)
            return False

        self._bases.append(normalized)
        self._known_bases.add(normalized)

        if self.max_bases is not None and len(self._bases) > self.max_bases:
            removed = self._bases.pop(0)
            self._known_bases.discard(removed)

        logger.info("Added mirror base: %s", normalized)
        return True

    def _invalidate_session(self) -> None:
        """Drop cached internal Tor session so a fresh one is created on retry."""
        if self._session_is_external:
            return
        with self._lock:
            self._session = None

    def _get_or_create_session(self) -> requests.Session:
        """Get a Tor-backed requests session with retries for transient failures."""
        with self._lock:
            if self._session is not None:
                return self._session

        last_error: Exception | None = None
        for attempt in range(1, self.session_init_retries + 1):
            try:
                session, session_num = self._tor_instance.get_session_with_number()
                with self._lock:
                    self._session = session
                logger.info(
                    "Acquired Tor session for dynamic discovery (session=%s attempt=%d/%d)",
                    session_num,
                    attempt,
                    self.session_init_retries,
                )
                return session
            except Exception as err:  # pylint: disable=broad-exception-caught
                last_error = err
                logger.warning(
                    "Failed to acquire Tor session for dynamic discovery (attempt %d/%d): %s",
                    attempt,
                    self.session_init_retries,
                    err,
                )
                if attempt < self.session_init_retries and self.retry_backoff_sec > 0:
                    time.sleep(self.retry_backoff_sec * attempt)

        raise BaseResolutionError(
            "Unable to initialize Tor session for dynamic discovery "
            f"after {self.session_init_retries} attempt(s): {last_error}"
        )

    def _extract_redirect_hint(self, response: requests.Response) -> Optional[str]:
        location = response.headers.get("Location")
        if location and not _is_placeholder_hint(location):
            return location

        refresh = response.headers.get("Refresh") or response.headers.get("refresh")
        if refresh and "url=" in refresh.lower():
            _, _, tail = refresh.partition("=")
            if tail.strip() and not _is_placeholder_hint(tail):
                return tail.strip()

        body = response.text or ""
        for pattern in self._REDIRECT_HINT_PATTERNS:
            match = pattern.search(body)
            if not match:
                continue
            candidate = match.group(1).strip()
            if candidate == "":
                continue
            if _is_placeholder_hint(candidate):
                continue
            return candidate

        return None

    def _resolve_final_url(self, bootstrap_url: str) -> str:
        logger.info("Resolving dynamic base from bootstrap URL: %s", bootstrap_url)
        last_error: Exception | None = None
        for attempt in range(1, self.bootstrap_retries + 1):
            try:
                session = self._get_or_create_session()

                # Do not follow redirect chains first; extract redirect target from
                # bootstrap response so we do not eagerly request base root.
                response = session.get(
                    bootstrap_url,
                    allow_redirects=False,
                    timeout=self.request_timeout,
                    verify=False,
                )
                response.raise_for_status()

                hint = self._extract_redirect_hint(response)
                if hint:
                    hinted_url = normalize_url_for_request(urljoin(bootstrap_url, hint))
                    logger.debug("Resolved redirect hint URL: %s", hinted_url)
                    return hinted_url

                # Fallback for redirect flows where no parseable hint was exposed.
                follow_response = session.get(
                    bootstrap_url,
                    allow_redirects=True,
                    timeout=self.request_timeout,
                    verify=False,
                )
                follow_response.raise_for_status()
                if follow_response.url and follow_response.url != bootstrap_url:
                    logger.info(
                        "Bootstrap URL redirected to final URL (fallback follow): %s",
                        follow_response.url,
                    )
                    return follow_response.url

                logger.info(
                    "Bootstrap URL resolved without explicit redirect: %s",
                    follow_response.url,
                )
                return follow_response.url or bootstrap_url
            except requests.exceptions.RequestException as err:
                last_error = err
                logger.warning(
                    "Bootstrap request failed (attempt %d/%d) for '%s': %s",
                    attempt,
                    self.bootstrap_retries,
                    bootstrap_url,
                    err,
                )
                self._invalidate_session()
                if attempt < self.bootstrap_retries and self.retry_backoff_sec > 0:
                    time.sleep(self.retry_backoff_sec * attempt)

        raise BaseResolutionError(
            "Bootstrap URL resolution failed "
            f"after {self.bootstrap_retries} attempt(s): {last_error}"
        )

    def discover_base(self) -> str:
        """Discover one new base URL from any configured bootstrap URL."""
        errors: list[str] = []
        for bootstrap_url in self.bootstrap_urls:
            try:
                final_url = self._resolve_final_url(bootstrap_url)
                discovered_base = extract_dynamic_base(final_url, self.top_level_folder)
                logger.debug(
                    "Discovered mirror base from bootstrap '%s': %s",
                    bootstrap_url,
                    discovered_base,
                )
                return discovered_base
            except Exception as err:  # pylint: disable=broad-exception-caught
                logger.warning(
                    "Dynamic base discovery attempt failed for '%s': %s",
                    bootstrap_url,
                    err,
                )
                errors.append(f"{bootstrap_url}: {err}")

        raise BaseResolutionError(
            "All bootstrap URLs failed while discovering a new base. "
            f"Details: {' | '.join(errors)}"
        )

    def ensure_minimum_bases(self, force: bool = False) -> list[str]:
        """Ensure the base pool has at least min_bases entries."""
        with self._lock:
            now = time.monotonic()
            if (
                not force
                and self.refresh_cooldown_sec > 0
                and (now - self._last_refresh_ts) < self.refresh_cooldown_sec
            ):
                logger.info(
                    "Skipping base refresh due to cooldown; current pool size=%d",
                    len(self._bases),
                )
                return list(self._bases)

            missing = max(0, self.min_bases - len(self._bases))
            self._last_refresh_ts = now

        logger.info(
            "Ensuring minimum bases: current=%d target=%d missing=%d force=%s",
            self.min_bases - missing,
            self.min_bases,
            missing,
            force,
        )

        if missing == 0:
            logger.info(
                "Base pool already satisfies minimum size (%d).", self.min_bases
            )
            return self.get_bases()

        max_attempts = max(8, missing * 8)
        attempts_remaining = max_attempts
        workers = min_int(self.discovery_workers)

        while attempts_remaining > 0:
            with self._lock:
                still_missing = max(0, self.min_bases - len(self._bases))
            if still_missing == 0:
                break

            batch_size = min(workers, attempts_remaining)
            attempts_remaining -= batch_size
            logger.info(
                "Starting discovery batch: threads=%d attempts_remaining=%d still_missing=%d",
                batch_size,
                attempts_remaining,
                still_missing,
            )

            with ThreadPoolExecutor(max_workers=batch_size) as executor:
                futures = [
                    executor.submit(self.discover_base) for _ in range(batch_size)
                ]

                for future in as_completed(futures):
                    try:
                        new_base = future.result()
                    except BaseResolutionError as err:
                        logger.warning("Could not discover new base: %s", err)
                        continue

                    with self._lock:
                        self._add_base(new_base)

            with self._lock:
                logger.debug(
                    "Discovery batch complete: pool_size=%d target=%d",
                    len(self._bases),
                    self.min_bases,
                )
                if len(self._bases) >= self.min_bases:
                    break

        logger.info("Final base pool size after refresh: %d", len(self.get_bases()))
        return self.get_bases()

    def get_bases(self) -> list[str]:
        """Return a snapshot of currently active bases."""
        with self._lock:
            return list(self._bases)

    def report_base_failure(self, failed_base: str) -> list[str]:
        """Evict a failed base, then refill the pool back to min_bases."""
        normalized = ensure_trailing_slash(failed_base.strip())
        with self._lock:
            if normalized in self._known_bases:
                self._known_bases.discard(normalized)
                self._bases = [b for b in self._bases if b != normalized]
                logger.warning("Removed failed base: %s", normalized)
                logger.info(
                    "Base pool size after removal: %d (target=%d)",
                    len(self._bases),
                    self.min_bases,
                )

        return self.ensure_minimum_bases(force=True)

    def build_candidate_urls(
        self, relative_path: str, directory: bool = False
    ) -> list[str]:
        """Build candidate URLs from active bases for a relative target."""
        suffix = relative_path.strip().lstrip("/")
        if directory and suffix and not suffix.endswith("/"):
            suffix = f"{suffix}/"

        return [
            urljoin(ensure_trailing_slash(base), suffix) for base in self.get_bases()
        ]

    def write_links_schema(self, files: list[str], output_path: str) -> Path:
        """Write a mirror-mode links JSON file from current base pool state."""
        payload = {
            "bases": self.get_bases(),
            "files": files,
        }
        destination = Path(output_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.write("\n")
        return destination
