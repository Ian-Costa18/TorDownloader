"""The FileDownloader class is used to download files from a URL, using Tor through a TorInstance.

File Downloader for the TorDownloader project.
Streams file downloads and continues file downloads if the file is partially downloaded.

LinkError is raised if the given URL is not valid for some reason.
"""

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple

import requests
import validators
from stemquests import TorInstance
from tqdm import tqdm

from . import logger


class LinkError(Exception):
    """Exception raised for errors in the download links."""


# From: https://betterprogramming.pub/python-progress-bars-with-tqdm-by-example-ce98dbbc9697
class FileDownloader(object):
    """
    Downloads a file from a URL.
    Meant to be used for 1 file for each object.
    Made using https://betterprogramming.pub/python-progress-bars-with-tqdm-by-example-ce98dbbc9697

    Args:
        tor_instance (TorInstance, optional): TorInstance to use for getting new sessions.
                                              Defaults to creating a new TorInstance with the given port if use_tor=True.
        tor_port (int, optional): Port to create new TorInstance on. Ignored if tor_instance is set. Defaults to 9051.
        use_tor (bool, optional): Whether to use Tor. Created a new TorInstance and requests.Session if they're not provided. Defaults to True.
        requests_session (requests.Session, optional): Requests session to use for the requests. Requires session_num to be set.
                                                       Defaults to creating a new session using self.tor_instance.
        session_num (int, optional): Session number of given session. Ignored unless requests_session is set. Defaults to None.
    Raises:
        ValueError: If the target_dir is not a valid directory for the download_file function.
        LinkError: If URL is not valid.
        tor_instance.TorConnectionError: If there is an error connecting to Tor.
    """

    def __init__(self, tor_instance: TorInstance=None, tor_port: int=9051,
                 use_tor: bool=True, requests_session: requests.Session=None, max_retries: int=5) -> None:
        if not use_tor and tor_instance is not None:
            raise ValueError("use_tor cannot be false if tor_instance is provided.")

        if use_tor:
            self.tor_instance = tor_instance or TorInstance(socks_port=tor_port)
            if requests_session is None:
                self.requests_session, self.session_num = self.tor_instance.get_session_with_number()
            else:
                self.requests_session = requests_session
                self.session_num = -1
        else:
            self.tor_instance = None
            self.requests_session = requests_session or requests.Session()

        self.start_time = datetime.now()

        self.max_retries = max_retries
        self.num_retries = 0

    def _get_url_filename(self, url: str, session: requests.Session) -> str:
        """
        Discover file name from HTTP URL, If none is discovered derive name from http redirect HTTP content header Location

        Args:
            url (str): Url link for the file to download.
            session (requests.Session): Requests session to use for the request.

        Returns:
            str: Base filename

        Raises:
            LinkError: If URL is not valid.
            requests.exceptions.HTTPError: If there is an HTTP error.
            requests.exceptions.ConnectionError: If there is a connection error.
            requests.exceptions.Timeout: If the request times out.
            requests.exceptions.RequestException: If there is another error with the request.
        """
        try:
            if not validators.url(url):
                logger.error("Invalid URL: %s", url)
                raise LinkError('Invalid url')
            filename = os.path.basename(url)
            _, ext = os.path.splitext(filename)
            if ext:
                return filename
            header = session.head(url, allow_redirects=False).headers
            return os.path.basename(header.get('Location')) if 'Location' in header else filename
        except requests.exceptions.HTTPError as errh:
            logger.error("Http Error: %s | URL: %s", errh, url)
            raise errh
        except requests.exceptions.ConnectionError as errc:
            logger.error("Error Connecting:%s | URL: %s", errc, url)
            raise errc
        except requests.exceptions.Timeout as errt:
            logger.error("Timeout Error:%s | URL: %s", errt, url)
            raise errt
        except requests.exceptions.RequestException as err:
            logger.error("Oops, Something Else: %s | URL: %s", err, url)
            raise err

    def _check_local_file(self, filename: str, chunk_size: int, full_path: str, header: Dict=None) -> Tuple[Dict, int]:
        """Check if filename already exists, get the file size if it does and create a header to resume download.

        Args:
            filename (str): File name to check.
            chunk_size (int): Chunk size to use for the download.
            full_path (str): Full path to the file.
            header (Dict, optional): Original header to modify. Defaults to creating a new header.

        Returns:
            Tuple[Dict, int]: Header to use for the download and the number of chunks already downloaded.
        """
        original_file_size = 0
        header = {} if header is None else header
        if os.path.isfile(full_path):
            original_file_size = Path(full_path).stat().st_size
            header['Range'] = f'bytes= {original_file_size}-'
            original_file_chunks = original_file_size // chunk_size
            logger.info("Found file '%s' in output directory, resuming download after %d chunks", filename, original_file_chunks)
            return header, original_file_chunks
        return header, 0

    def download_file(self, url: str, target_dir: str=None, filename: str=None, chunk_size: int=1024) -> str:
        """Stream downloads files via HTTP

        Args:
            url (str): Url for the file to download.
            target_dir (str, optional): Target destination directory to download file to. Defaults to the current directory.
            filename (str, optional): Name of the file. Defaults to filename defined in URL parameter.
            chunk_size (int, optional): Size of the chunk to download in bytes. Defaults to 1024 (1KB).

        Raises:
            ValueError: If the target_dir is not a valid directory.

        Returns:
            str: Absolute path to target destination where file has been downloaded to
        """
        # Check if number of retries is less than max retries
        if self.num_retries >= self.max_retries:
            logger.error("Max retries (#%d) exceeded for URL: %s", self.num_retries, url)
            return None
        self.num_retries += 1

        logger.debug("Starting download from URL: %s", url)

        # Check if the target_dir is a valid directory
        if target_dir and not os.path.isdir(target_dir):
            if os.path.isfile(target_dir):
                raise ValueError(f'Invalid target_dir={target_dir} specified, target_dir is a file.')
            try:
                # Create the target_dir if it doesn't exist
                logger.warning("Directory '%s' does not exist, attempting to create it.", target_dir)
                os.mkdirs(target_dir)
                logger.info("Directory '%s' successfully created.", target_dir)
            except OSError as err:
                logger.error("Error creating target_dir: %s", err)
                raise ValueError(f'Invalid target_dir={target_dir} specified') from err

        # Get filename from URL if not given
        filename = self._get_url_filename(url, self.requests_session) if filename is None else filename
        # Get the target_dir and base_path if target_dir is not given
        full_path, base_path = os.path.join(target_dir, filename), os.path.abspath(os.path.dirname(__file__))
        # Get the path to the file to download
        target_dest_dir = os.path.join(target_dir, filename) if target_dir else os.path.join(base_path, filename)

        # Check if file already exists, get the file size if it does and resume the download at the end of the file
        resume_header, original_file_chunks = self._check_local_file(filename, chunk_size, full_path)
        try:
            req = self.requests_session.get(url, headers=resume_header, stream=True, verify=False)
            # Does the entire file need to be restarted? What if the local file's size is slightly higher than the recieved file size?
            if req.status_code == 404:
                logger.error("Recieved 404, recheck download links. 404 link: %s", url)
                raise LinkError(f"404 for URL: {url}")
            if req.status_code == 416:
                logger.info("Received 416 response, assuming download is done for file: %s", filename)
                return target_dest_dir
            if (file_size := req.headers.get('Content-Length')) is None:
                raise LinkError(f"Content-Length for URL is none. URL: {url} | Request: {req}")
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError,
                requests.exceptions.Timeout, requests.exceptions.RequestException) as err:
            logger.warning("Request error, trying again. URL: %s | Error: %s", url, err)
            return self.download_file(url, target_dir=target_dir, filename=filename, chunk_size=chunk_size)
        num_bars = int(file_size) // chunk_size

        logger.debug("Got request: %s | Number of chunks: %d", req, num_bars)
        # Set up TQDM options we will use for every progress bar
        tqdm_options = {
            "unit": 'KB',
            "desc": filename,
            "leave": True,
            "file": sys.stdout,
            "dynamic_ncols": True
        }
        # TODO: BUG: The progress bar does not show the first time a file is downloaded.
        # If the file size is less then a chunk, download it and append to file
        if int(file_size) < chunk_size:
            with open(target_dest_dir, 'ab') as output_file:
                output_file.write(req.content)
            logger.info("Last bytes have been downloaded (%s bytes)!  URL: %s | Filepath: %s", file_size, url, target_dest_dir)
            return target_dest_dir
        # If the file size is 0, there is nothing to download
        if file_size == 0:
            logger.info("No more bytes left to download!  URL: %s | Filepath: %s", url, target_dest_dir)
            return target_dest_dir
        # If the file is not in the output directory, create it and start a stream download
        if not resume_header:
            logger.info("File not found in output directory, creating new file: %s", target_dest_dir)
            with open(target_dest_dir, 'wb') as output_file:
                for chunk in tqdm(req.iter_content(chunk_size=chunk_size), total=num_bars, **tqdm_options):
                    output_file.write(chunk)
        # If the file is in the output directory, start a stream download and append to file
        else:
            logger.info("File found in output directory, resuming download of file: %s", target_dest_dir)
            with open(target_dest_dir, 'ab') as output_file:
                for chunk in tqdm(req.iter_content(chunk_size=chunk_size), total=num_bars+original_file_chunks,
                                  initial=original_file_chunks, **tqdm_options):
                    output_file.write(chunk)
        # Check if the file size is the same as the expected file size.
        # TODO: Check this, expected file size may not be the same as the full file size since we can start halfway through
        target_file_size = Path(target_dest_dir).stat().st_size
        logger.debug("Finalizing file: URL: %s | File size: %s | Expected file size: %s | Difference: %d",
                     url, target_file_size, file_size, target_file_size - int(file_size))
        if target_file_size != int(file_size):
            logger.warning("Target file size (%d) does not match expected file size (%d), restarting...", target_file_size, int(file_size))
            return self.download_file(url, target_dir=target_dir, filename=filename, chunk_size=chunk_size)
        logger.info("File downloaded! Elapsed Time: %s | URL: %s | Filepath: %s", str(self.start_time - datetime.now()), url, target_dest_dir)
        return target_dest_dir
