"""Shared URL normalization helpers for HTTP requests."""

from __future__ import annotations

from urllib.parse import quote, unquote, urlsplit, urlunsplit


def ensure_trailing_slash(url: str) -> str:
    """Return URL with one trailing slash when non-empty."""
    if url == "":
        return ""
    return url if url.endswith("/") else f"{url}/"


def normalize_url_for_request(url: str) -> str:
    """Encode unsafe URL characters while preserving request semantics.

    Uses ``allow_fragments=False`` so raw ``#`` characters found in directory
    listings are treated as path/query data and encoded to ``%23`` instead of
    truncating the request URL as a fragment delimiter.
    """
    parsed = urlsplit(url, allow_fragments=False)
    encoded_path = quote(unquote(parsed.path), safe="/%:@")
    encoded_query = quote(unquote(parsed.query), safe="=&;%:+,/?%")
    return urlunsplit((parsed.scheme, parsed.netloc, encoded_path, encoded_query, ""))
