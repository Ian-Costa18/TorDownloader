"""Parse and validate supported links.json input schemas."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import List, Literal

logger = logging.getLogger(__name__)


@dataclass
class LinksSpec:
    """Normalized links input specification."""

    mode: Literal["list", "mirror"]
    links: List[str] = field(default_factory=list)
    bases: List[str] = field(default_factory=list)
    files: List[str] = field(default_factory=list)


def _clean_string_list(values, field_name: str) -> List[str]:
    """Validate that a JSON value is a non-empty string list."""
    if not isinstance(values, list):
        raise ValueError(f"'{field_name}' must be a list of strings")

    cleaned: List[str] = []
    for idx, value in enumerate(values):
        if not isinstance(value, str):
            raise ValueError(f"'{field_name}[{idx}]' must be a string")
        stripped = value.strip()
        if stripped == "":
            raise ValueError(f"'{field_name}[{idx}]' cannot be empty")
        cleaned.append(stripped)
    return cleaned


def load_links_spec(json_path: str) -> LinksSpec:
    """Load links.json in either list mode or mirror mode.

    Supported formats:
        ["https://...", "https://..."]
        {"bases": ["http://a.onion"], "files": ["dir/file.txt", "dir/"]}
    """
    with open(json_path, "r", encoding="utf-8") as file:
        payload = json.load(file)

    if isinstance(payload, list):
        links = _clean_string_list(payload, "links")
        logger.info("Found %d link(s) in file '%s'", len(links), json_path)
        return LinksSpec(mode="list", links=links)

    if isinstance(payload, dict):
        if "bases" not in payload or "files" not in payload:
            raise ValueError(
                "Dictionary links schema must contain 'bases' and 'files' keys"
            )

        bases = _clean_string_list(payload.get("bases"), "bases")
        files = _clean_string_list(payload.get("files"), "files")
        logger.info(
            "Found mirror schema in '%s' with %d base(s) and %d file entry(ies)",
            json_path,
            len(bases),
            len(files),
        )
        return LinksSpec(mode="mirror", bases=bases, files=files)

    raise ValueError("Unsupported links.json schema. Expected list or dict")
