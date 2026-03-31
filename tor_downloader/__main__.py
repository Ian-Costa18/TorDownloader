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
    enum_workers: Number of concurrent directory enumeration workers. Defaults to max_downloads.
    download_workers: Number of concurrent file download workers. Defaults to max_downloads.
    request_connect_timeout: Per-request connect timeout in seconds. Default is 60.
    request_read_timeout: Per-request read timeout in seconds. Default is 300.
    max_tor_checks: Number of times the Tor proxy will be checked to ensure Tor is working before crashing. Default is 5.
    tor_path: Path to the Tor executable (tor.exe). Often found in Tor Browser if installed (Tor Browser\\Browser\\TorBrowser\\Tor\\tor.exe).
    links_file: Path to links.json containing either a list of URLs or a mirror schema with {bases, files}.
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
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict
from urllib.parse import unquote

from stemquests import TorInstance

from .config_utils import clamp_min_int, coerce_cli_value, coerce_config_file_value, min_int
from .download_runner import run_download_jobs
from .dynamic_base_pool import BaseResolutionError, DynamicBasePool
from .link_specs import load_links_spec
from .mirror_planner import plan_download_jobs
from .utils import TDFormatter, TqdmLoggingHandler

CURRENT_PATH = Path(__file__).parent
DEFAULT_CONFIG = {
    "socks_port": 9051,
    "max_downloads": 10,
    "enum_workers": 12,
    "download_workers": 4,
    # "max_downloads": 1,  # Set to 1 for testing
    "request_connect_timeout": 60,
    "request_read_timeout": 300,
    "probe_retries": 1,
    "links_file": CURRENT_PATH / "data/input/links.json",
    "log_file": CURRENT_PATH / "log/TorDownloader.log",
    "output_dir": CURRENT_PATH / "data/output",
}

INT_CONFIG_KEYS = {
    "socks_port",
    "max_downloads",
    "enum_workers",
    "download_workers",
    "max_tor_checks",
    "request_connect_timeout",
    "request_read_timeout",
    "probe_retries",
}


def _derive_top_level_folder(file_entries: list[str]) -> str | None:
    """Infer the top-level folder from relative mirror file entries."""
    for entry in file_entries:
        if "://" in entry:
            continue
        normalized = entry.strip().lstrip("/")
        if normalized == "":
            continue
        top = normalized.split("/", 1)[0].strip()
        if top:
            return unquote(top)
    return None


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
            clean_config[key] = coerce_config_file_value(
                key,
                value,
                int_keys=INT_CONFIG_KEYS,
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
        arg_dict[arg_key] = coerce_cli_value(arg_val)

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
    progress_file = str(Path(links_file).parent / "download_progress.sqlite3")
    logger.info("Progress store file: %s", progress_file)
    legacy_progress_file = Path(links_file).parent / "download_progress.json"
    if legacy_progress_file.exists():
        logger.info(
            "Legacy JSON progress file detected at '%s'. It is no longer used.",
            legacy_progress_file,
        )
    try:
        links_spec = load_links_spec(links_file)
    except (ValueError, OSError, json.JSONDecodeError) as err:
        logger.error("Could not load links file '%s': %s", links_file, err)
        return

    # Create a Tor instance for the downloader
    socks_port = int(CONFIG.get("socks_port", DEFAULT_CONFIG["socks_port"]))
    tor_path = CONFIG.get("tor_path")
    tor_instance = (
        TorInstance(socks_port, str(tor_path))
        if tor_path is not None
        else TorInstance(socks_port)
    )
    request_timeout = (
        int(CONFIG.get("request_connect_timeout", 60)),
        int(CONFIG.get("request_read_timeout", 300)),
    )

    shared_dynamic_base_pool: DynamicBasePool | None = None

    if links_spec.mode == "mirror" and links_spec.dynamic_base is not None:
        top_level_folder = _derive_top_level_folder(links_spec.files)
        if top_level_folder is None:
            logger.warning(
                "dynamic_base is set, but top-level folder could not be inferred from files. "
                "Skipping dynamic base refresh."
            )
        else:
            min_bases = max(5, int(links_spec.dynamic_min_bases))
            try:
                base_pool = DynamicBasePool(
                    bootstrap_urls=[links_spec.dynamic_base],
                    top_level_folder=top_level_folder,
                    min_bases=min_bases,
                    request_timeout=request_timeout,
                    tor_instance=tor_instance,
                    initial_bases=links_spec.bases,
                )
                refreshed_bases = base_pool.ensure_minimum_bases(force=True)
                if len(refreshed_bases) > 0:
                    links_spec.bases = refreshed_bases
                    shared_dynamic_base_pool = base_pool
                    logger.info(
                        "Dynamic base refresh loaded %d base(s).",
                        len(refreshed_bases),
                    )
                    logger.info(
                        "Dynamic base pool contents: %s",
                        ", ".join(refreshed_bases),
                    )
                else:
                    logger.warning(
                        "Dynamic base refresh returned no bases. Using static bases from links file."
                    )
            except (BaseResolutionError, ValueError, OSError) as err:
                logger.warning(
                    "Dynamic base refresh failed (%s). Falling back to static bases.",
                    err,
                )

    probe_retries = min_int(CONFIG.get("probe_retries", 3))

    max_downloads, max_downloads_clamped = clamp_min_int(
        CONFIG.get("max_downloads", 4)
    )
    if max_downloads_clamped:
        logger.warning("max_downloads must be >= 1. Using %d.", max_downloads)

    enum_workers_cfg = CONFIG.get("enum_workers")
    if enum_workers_cfg is None:
        enum_workers = max_downloads
        enum_workers_clamped = False
    else:
        enum_workers, enum_workers_clamped = clamp_min_int(enum_workers_cfg)
    if enum_workers_clamped:
        logger.warning("enum_workers must be >= 1. Using %d.", enum_workers)

    download_workers_cfg = CONFIG.get("download_workers")
    if download_workers_cfg is None:
        download_workers = max_downloads
        download_workers_clamped = False
    else:
        download_workers, download_workers_clamped = clamp_min_int(
            download_workers_cfg
        )
    if download_workers_clamped:
        logger.warning("download_workers must be >= 1. Using %d.", download_workers)

    try:
        jobs = plan_download_jobs(links_spec)
        if len(jobs) == 0:
            logger.error("Links file is empty.")
            return

        logger.info("Planned %d logical job(s).", len(jobs))
        files = run_download_jobs(
            jobs=jobs,
            output_dir=str(CONFIG["output_dir"]),
            tor_instance=tor_instance,
            tor_port=CONFIG["socks_port"],
            max_downloads=max_downloads,
            request_timeout=request_timeout,
            probe_retries=probe_retries,
            enum_workers=enum_workers,
            download_workers=download_workers,
            base_pool=shared_dynamic_base_pool,
            progress_file=progress_file,
        )
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
    logger.info("All Downloads Finished.")
    if len(files) == 0:
        logger.info("No failed downloads recorded.")
    else:
        logger.info("Failed downloads (%d):", len(files))
        for relative_target, result in files.items():
            logger.info("\t- %s: %s", relative_target, result)


if __name__ == "__main__":
    main()
