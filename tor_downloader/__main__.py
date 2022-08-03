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
    tor_path: Path to the Tor executable (tor.exe). Often found in Tor Browser if installed (Tor Browser\\Browser\\TorBrowser\\Tor\\tor.exe).
    links_file: Path to the file containing the list of URLs to download. Must be a .json file with a single list of URLs.
    log_file: Path to the log file. Log file will be created if it does not exist.
    output_dir: Path to the directory to download the files to.
    config_file: Path to the configuration file. Only usable through the command line arguments.

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

from stemquests import TorConnectionError, TorInstance

from tor_downloader.file_downloader import FileDownloader
from tor_downloader.utils import (TDFormatter, TqdmLoggingHandler,
                                  get_download_links_json)

CURRENT_PATH = Path(__file__).parent
DEFAULT_CONFIG = {
    "socks_port": 9051,
    "max_downloads": 7,
    "links_file": CURRENT_PATH / "data\\input\\links.json",
    "log_file": CURRENT_PATH / "log\\TorDownloader.log",
    "output_dir": CURRENT_PATH / "data\\output"
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
                case "socks_port" | "max_downloads":
                    clean_config[key] = int(value)
                case _:
                    clean_config[key] = value
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
            raise ValueError(f"Invalid command line argument: '{arg}'. Arguments must be formatted like so: 'CONFIG=SETTING'.")
        arg_key, arg_val = arg_split
        if arg_val.isnumeric():
            arg_val = int(arg_val)
        if arg_val.lower() == "true":
            arg_val = True
        if arg_val.lower() == "false":
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
    file_config = get_config_file(arg_config["config_file"]) if arg_config.get("config_file") is not None else DEFAULT_CONFIG

    # Merge the config dictionaries, with the cmd arguments taking precedence
    CONFIG = {**file_config, **arg_config}

    # Add a new line to the end of the log file before starting
    with open(CONFIG["log_file"], "a", encoding="utf-8") as log_file:
        log_file.write("\n")
    # Setup logging
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    # Add a rotating handler with a max of 100 MB, keeping 5 backup files
    handler = RotatingFileHandler(CONFIG["log_file"], maxBytes=1024 * 1024 * 100, backupCount=5)
    handler.setFormatter(logging.Formatter(fmt="%(asctime)s:%(levelname)s:%(name)s:%(message)s"))
    logger.addHandler(handler)

    # Set up Tor Downloader logger
    td_logger = logging.getLogger("tor_downloader")
    tqdm_handler = TqdmLoggingHandler(logging.INFO)
    tqdm_handler.setFormatter(TDFormatter())
    td_logger.addHandler(tqdm_handler)

    logger.info("Starting TorDownloader on %s", datetime.now().isoformat())

    download_links = get_download_links_json(CONFIG.get("links_file"))
    if download_links is None:
        logger.error("Links file is empty.")
        return

    # Create a Tor instance for the downloader
    tor_instance = TorInstance(CONFIG["socks_port"], CONFIG.get("tor_path"))
    files = {}
    with ThreadPoolExecutor(max_workers=CONFIG["max_downloads"]) as executor:
        try: # Catch keyboard interrupts and other exceptions
            # Submit the jobs to the executor.
            futures = {}
            for download_link in download_links:
                downloader = FileDownloader(tor_instance=tor_instance, tor_port=CONFIG["socks_port"])
                future = executor.submit(downloader.download_file, download_link, target_dir=CONFIG["output_dir"])
                futures[future] = download_link
            # Get the results for the downloads
            for future in as_completed(futures):
                url = futures[future]
                # TODO: Handle more exceptions by restarting the download
                future_exception = future.exception()
                if isinstance(future_exception, TorConnectionError):
                    logger.error("Could not connect to Tor for URL '%s', readding the URL to the queue.", url)
                    downloader = FileDownloader(tor_instance=tor_instance, tor_port=CONFIG["socks_port"])
                    retry_url_future = executor.submit(downloader.download_file, url, target_dir=CONFIG["output_dir"])
                    futures[retry_url_future] = url
                    continue
                if future_exception is not None:
                    files[url] = future.exception()
                    td_logger.error("Error downloading %s: %s", url, future.exception())
                    continue
                if CONFIG["output_dir"] in (result := future.result()):
                    td_logger.info("Download finished! Filepath: %s | URL: %s", result, url)
                else:
                    td_logger.error("Download failed! Reason: %s | URL: %s", result, url)
                files[url] = result
        except KeyboardInterrupt:
            # TODO: Fix this, it doesn't work. For now manually kill the program.
            print('Keyboard Interrupt')
            logger.info("Keyboard Interrupt, stopping program...")
            sys.exit(1)
        except Exception as err:
            logger.error("Fatal Error (time = %s): %s", datetime.now().isoformat(), err)
            raise err

    print("-"*25)
    print("All Downloads Finished:")
    for url, result in files.items():
        print(f"\t- {url}: {result}")

if __name__ == '__main__':
    main()
