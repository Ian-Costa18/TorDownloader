"""Parse and validate supported links.json input schemas."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import List, Literal

from .utils.config_utils import parse_int_field

logger = logging.getLogger(__name__)


@dataclass
class LinksSpec:
    """Normalized links input specification."""

    mode: Literal["list", "mirror"]
    links: List[str] = field(default_factory=list)
    bases: List[str] = field(default_factory=list)
    files: List[str] = field(default_factory=list)
    dynamic_base: str | None = None
    dynamic_min_bases: int = 5


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
        dynamic_base = payload.get("dynamic_base")
        if dynamic_base is not None:
            if not isinstance(dynamic_base, str):
                raise ValueError("'dynamic_base' must be a string when provided")
            dynamic_base = dynamic_base.strip()
            if dynamic_base == "":
                raise ValueError("'dynamic_base' cannot be empty")

        if "files" not in payload:
            raise ValueError("Dictionary links schema must contain 'files' key")
        files = _clean_string_list(payload.get("files"), "files")

        if "bases" in payload:
            bases = _clean_string_list(payload.get("bases"), "bases")
        elif dynamic_base is not None:
            bases = []
        else:
            raise ValueError(
                "Dictionary links schema must contain 'bases' key unless 'dynamic_base' is provided"
            )

        dynamic_min_bases_raw = payload.get("dynamic_min_bases", 5)
        dynamic_min_bases = parse_int_field(
            dynamic_min_bases_raw,
            "dynamic_min_bases",
        )
        if dynamic_min_bases < 1:
            raise ValueError("'dynamic_min_bases' must be >= 1")

        logger.info(
            "Found mirror schema in '%s' with %d base(s) and %d file entry(ies)",
            json_path,
            len(bases),
            len(files),
        )
        if dynamic_base is not None:
            logger.info(
                "Dynamic base mode enabled with bootstrap URL '%s' (min bases: %d)",
                dynamic_base,
                dynamic_min_bases,
            )
        return LinksSpec(
            mode="mirror",
            bases=bases,
            files=files,
            dynamic_base=dynamic_base,
            dynamic_min_bases=dynamic_min_bases,
        )

    raise ValueError("Unsupported links.json schema. Expected list or dict")
