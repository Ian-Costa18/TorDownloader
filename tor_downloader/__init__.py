"""Download files from Tor websites. Used to download data from ransomware leak sites.

Uses streamed file downloads and restarts if a file is not fully downloaded.
Gets the list of URLs to download either from a JSON file or from another URL (work in progress).
"""

import logging

import requests

logger = logging.getLogger(__name__)

# Disable warnings for requests
requests.packages.urllib3.disable_warnings() # pylint: disable=no-member
