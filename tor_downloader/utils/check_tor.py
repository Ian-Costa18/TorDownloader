import logging

import requests

logger = logging.getLogger(__name__)


def check_tor(session: requests.Session) -> bool:
    """Check if Tor is working.

    Args:
        session (requests.Session): Requests session to check.

    Returns:
        bool: True if Tor is working, False otherwise.
    """
    with session.get("https://check.torproject.org") as tor_check:
        return "Congratulations. This browser is configured to use Tor." in tor_check.text
