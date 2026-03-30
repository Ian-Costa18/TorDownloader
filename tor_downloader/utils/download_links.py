"""Backward-compatible shim for legacy imports.

Project-specific link logic now lives under the top-level tor_downloader package.
"""

from __future__ import annotations

import logging

from ..link_discovery import *  # noqa: F401,F403
from ..link_specs import load_links_spec

logger = logging.getLogger(__name__)


def get_download_links_json(json_path: str) -> list[str]:
    """Legacy list-mode loader kept for compatibility.

    If the new mirror schema is used, this returns a flattened candidate list.
    New code should use link_specs.load_links_spec + mirror_planner.plan_download_jobs.
    """
    spec = load_links_spec(json_path)
    if spec.mode == "list":
        return spec.links

    flattened: list[str] = []
    for file_entry in spec.files:
        if "://" in file_entry:
            flattened.append(file_entry)
            continue
        for base in spec.bases:
            base_url = base if base.endswith("/") else f"{base}/"
            flattened.append(f"{base_url}{file_entry.lstrip('/')}")

    logger.warning(
        "get_download_links_json() is in compatibility mode for mirror schema. "
        "Use link_specs.load_links_spec + mirror_planner.plan_download_jobs instead."
    )
    return flattened
