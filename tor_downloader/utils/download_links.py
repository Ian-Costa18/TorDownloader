"""Functions to grab download links from web pages and JSON files."""

import json
import logging
import re
from typing import List

import requests

logger = logging.getLogger(__name__)


def get_download_links_web(url: str, regex: str, session: requests.Session) -> List[str]:
    """Get download links from web page.

    Args:
        url (str): URL of web page.
        regex (str): Regex to extract download links.
        session (requests.Session): Requests session to use.

    Returns:
        List[str]: List of download links.
    """
    site = session.get(url, verify=False).text
    links = re.findall(regex, site)
    logger.info("Found %d links through url '%s'.", len(links), url)
    logger.debug("Link list: %s", ", ".join(links))
    return links

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
            return None
        logger.info("Found %d link(s) in file '%s'", len(links), json_path)
        logger.debug("Link list: %s", ", ".join(links))
        return links
