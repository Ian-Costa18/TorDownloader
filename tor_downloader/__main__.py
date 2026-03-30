"""Download files from Tor websites. Used to download data from ransomware leak sites.

Uses streamed file downloads and restarts if a file is not fully downloaded.
Gets the list of URLs to download either from a JSON file or from another URL (work in progress).

Configuration options can be given in either a JSON file or as command line arguments.
The config file must be a JSON file with a single dictionary.
The command line arguments must be formatted like so:
    ```CONFIG=SETTING```

Configuration options:
    socks_port: Port of Tor Socks5 proxy.
    max_downloads: Maximum number of downloads to run at once.
    request_connect_timeout: Per-request connect timeout in seconds. Default is 60.
    request_read_timeout: Per-request read timeout in seconds. Default is 300.
    max_tor_checks: Number of times the Tor proxy will be checked to ensure Tor is working before crashing. Default is 5.
    tor_path: Path to the Tor executable (tor.exe). Often found in Tor Browser if installed (Tor Browser\\Browser\\TorBrowser\\Tor\\tor.exe).
    links_file: Path to the file containing the list of URLs to download. Must be a .json file with a single list of URLs.
    log_file: Path to the log file. Log file will be created if it does not exist.
    output_dir: Path to the directory to download the files to.
    config: Path to the configuration file. Only usable through the command line arguments.

Example command:
    ```python main.py max_downloads=7 tor_path=Tor Browser\\Browser\\TorBrowser\\Tor\\tor.exe links_file=links.json output_directory=output```
"""

# sourcery skip: assign-if-exp
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict
from urllib.parse import unquote, urlparse

from stemquests import TorConnectionError, TorInstance

from .file_downloader import FileDownloader
from .utils import (
    TDFormatter,
    TqdmLoggingHandler,
    detect_content_type,
    get_download_links_json,
    stream_directory_files,
)

CURRENT_PATH = Path(__file__).parent
DEFAULT_CONFIG = {
    "socks_port": 9051,
    # "max_downloads": 7,
    "max_downloads": 1,  # Set to 1 for testing
    "request_connect_timeout": 60,
    "request_read_timeout": 300,
    "probe_retries": 3,
    "links_file": CURRENT_PATH / "data/input/links.json",
    "log_file": CURRENT_PATH / "log/TorDownloader.log",
    "output_dir": CURRENT_PATH / "data/output",
}


def get_config_file(config_file: str) -> Dict:
    """Get configurations from a JSON file.

    The file must be a JSON file with a single dictionary.

    Args:
        config_file (str): Path to the JSON file.

    Returns:
        Dict: Dictionary of configuration options.
    """
    # Check if config.json exists, return empty dict if not
    if not Path(config_file).exists():
        logging.warning("Config file '%s' does not exist.", config_file)
        return {}

    # Load config.json into a dictionary
    clean_config = {}
    with open(config_file, "r", encoding="utf-8") as file:
        for key, value in json.load(file).items():
            # Do not load empty strings or None values into the config
            if value == "" or value is None:
                continue
            # Convert the value to the correct type
            match key:
                case (
                    "socks_port"
                    | "max_downloads"
                    | "max_tor_checks"
                    | "request_connect_timeout"
                    | "request_read_timeout"
                    | "probe_retries"
                ):
                    clean_config[key] = int(value)
                case _:
                    clean_config[key] = (
                        value.lower() if isinstance(value, str) else value
                    )
    return clean_config


def get_config_args() -> Dict:
    """Get configurations from command line arguments.

    Returns:
        Dict: Dictionary of configuration options.
    """
    if len(sys.argv) == 1:
        return {}

    # Get config from command line arguments
    arg_dict = {}
    for arg in sys.argv[1:]:
        arg_split = arg.split("=")
        if len(arg_split) != 2:
            raise ValueError(
                f"Invalid command line argument: '{arg}'. Arguments must be formatted like so: 'CONFIG=SETTING'."
            )
        arg_key, arg_val = arg_split
        if arg_val.isnumeric():
            arg_val = int(arg_val)
        elif arg_val.lower() == "true":
            arg_val = True
        elif arg_val.lower() == "false":
            arg_val = False
        arg_dict[arg_key] = arg_val

    return arg_dict


def main():
    """Main function for TorDownloader. Runs the program using system arguments or a config file."""
    # Check if user passed the "path" argument
    if len(sys.argv) >= 2:
        if sys.argv[1] == "path":
            print(CURRENT_PATH)
            sys.exit(0)
    # Get the configuration options from the arguments and config file
    arg_config = get_config_args()
    # Overwrite the default config with the config options from the file
    file_config = (
        {**DEFAULT_CONFIG, **get_config_file(arg_config["config"])}
        if arg_config.get("config") is not None
        else DEFAULT_CONFIG
    )

    # Merge the config dictionaries, with the cmd arguments taking precedence
    CONFIG = {**file_config, **arg_config}

    # Add a new line to the end of the log file before starting
    Path(CONFIG["log_file"]).parents[0].mkdir(
        parents=True, exist_ok=True
    )  # Make the parent directories first
    with open(CONFIG["log_file"], "a", encoding="utf-8") as log_file:
        log_file.write("\n")
    # Setup logging
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    # Add a rotating handler with a max of 100 MB, keeping 5 backup files
    handler = RotatingFileHandler(
        CONFIG["log_file"], maxBytes=1024 * 1024 * 100, backupCount=5
    )
    handler.setFormatter(
        logging.Formatter(fmt="%(asctime)s:%(levelname)s:%(name)s:%(message)s")
    )
    logger.addHandler(handler)

    # Set up Tor Downloader logger
    tqdm_handler = TqdmLoggingHandler(logging.INFO)
    tqdm_handler.setFormatter(TDFormatter())
    logger.addHandler(tqdm_handler)

    logger.info("Starting TorDownloader on %s", datetime.now().isoformat())
    logger.debug("Using config options: %s", str(CONFIG))

    links_file = str(CONFIG.get("links_file", DEFAULT_CONFIG["links_file"]))
    download_links = get_download_links_json(links_file)
    if len(download_links) == 0:
        logger.error("Links file is empty.")
        return

    # Create a Tor instance for the downloader
    socks_port = int(CONFIG.get("socks_port", DEFAULT_CONFIG["socks_port"]))
    tor_path = CONFIG.get("tor_path")
    tor_instance = (
        TorInstance(socks_port, str(tor_path))
        if tor_path is not None
        else TorInstance(socks_port)
    )
    files = {}
    request_timeout = (
        int(CONFIG.get("request_connect_timeout", 60)),
        int(CONFIG.get("request_read_timeout", 300)),
    )
    probe_retries = max(1, int(CONFIG.get("probe_retries", 3)))

    def _safe_url_segments(url: str, keep_filename: bool) -> Path:
        """Create local path segments from URL netloc/path."""
        parsed = urlparse(url)
        host = parsed.netloc.replace(":", "_") or "unknown_host"
        parts = [part for part in unquote(parsed.path).split("/") if part]
        if not keep_filename and parts:
            parts = parts[:-1]
        return Path(host, *parts)

    with ThreadPoolExecutor(max_workers=CONFIG["max_downloads"]) as executor:
        try:
            logger.info("Submitting %d job(s) to the executor.", len(download_links))
            futures = {}
            for download_link in download_links:
                downloader = FileDownloader(
                    tor_instance=tor_instance,
                    tor_port=CONFIG["socks_port"],
                    request_timeout=request_timeout,
                )
                # Detect if the link is a directory (HTML) or file.
                content_type = detect_content_type(
                    downloader.requests_session,
                    download_link,
                    request_timeout=request_timeout,
                    probe_retries=probe_retries,
                )
                if content_type == "":
                    logger.warning(
                        "Could not probe content type for '%s', submitting as direct file URL.",
                        download_link,
                    )
                # If it's HTML, treat as directory and extract links
                if "text/html" in content_type:
                    logger.info(
                        f"Detected directory (HTML) at {download_link}, recursively extracting all file links."
                    )

                    directory_root = Path(CONFIG["output_dir"]) / _safe_url_segments(
                        download_link, keep_filename=True
                    )
                    submitted_count = 0
                    for file_url, relative_dir in stream_directory_files(
                        download_link,
                        downloader.requests_session,
                        request_timeout=request_timeout,
                        probe_retries=probe_retries,
                    ):
                        target_dir = (
                            directory_root / relative_dir
                            if relative_dir
                            else directory_root
                        )
                        future = executor.submit(
                            downloader.download_file,
                            file_url,
                            target_dir=str(target_dir),
                        )
                        futures[future] = (file_url, str(target_dir))
                        submitted_count += 1
                        logger.info(
                            "Submitted job for file URL '%s' from directory '%s' to '%s' using session #%d.",
                            file_url,
                            download_link,
                            target_dir,
                            downloader.session_num,
                        )
                    logger.info(
                        "Finished streaming enumeration for directory '%s'. Submitted %d file jobs.",
                        download_link,
                        submitted_count,
                    )
                else:
                    # Treat as file
                    target_dir = Path(CONFIG["output_dir"]) / _safe_url_segments(
                        download_link, keep_filename=False
                    )
                    future = executor.submit(
                        downloader.download_file,
                        download_link,
                        target_dir=str(target_dir),
                    )
                    futures[future] = (download_link, str(target_dir))
                    logger.info(
                        "Submitted job for URL '%s' to '%s' using session #%d.",
                        download_link,
                        target_dir,
                        downloader.session_num,
                    )
            logger.info("Submitted %d jobs to the executor.", len(futures))
            while futures:
                for future in as_completed(list(futures.keys())):
                    url, target_dir = futures.pop(future)
                    future_exception = future.exception()
                    if isinstance(future_exception, TorConnectionError):
                        logger.error(
                            "Could not connect to Tor for URL '%s', readding the URL to the queue.",
                            url,
                        )
                        downloader = FileDownloader(
                            tor_instance=tor_instance, tor_port=CONFIG["socks_port"]
                        )
                        retry_url_future = executor.submit(
                            downloader.download_file, url, target_dir=target_dir
                        )
                        futures[retry_url_future] = (url, target_dir)
                        continue
                    if future_exception is not None:
                        files[url] = future.exception()
                        logger.error(
                            "Error downloading %s: %s", url, future.exception()
                        )
                        continue
                    if str(CONFIG["output_dir"]) in (result := future.result()):
                        logger.info(
                            "Download finished! Filepath: %s | URL: %s", result, url
                        )
                    else:
                        logger.error(
                            "Download failed! Reason: %s | URL: %s", result, url
                        )
                    files[url] = result
                    logger.info("%d files finished so far.", len(files))
        except ConnectionError:
            logger.error("Connection error, restarting script...")
            return main()
        except KeyboardInterrupt:
            logger.info("Keyboard Interrupt, stopping program...")
            sys.exit(1)
        except Exception as err:
            logger.error("Fatal Error, restarting script... Error: %s", err)
            return main()

    logger.info("-" * 25)
    logger.info("All Downloads Finished:")
    for url, result in files.items():
        logger.info("\t- %s: %s", url, result)


if __name__ == "__main__":
    main()
