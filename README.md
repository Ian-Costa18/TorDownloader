# TorDownloader

Download files from Tor websites. Used to download data from ransomware leak sites.

Uses streamed file downloads and restarts if a file is not fully downloaded.
Gets the list of URLs to download either from a JSON file or from another URL (work in progress).

## Installing TorDownloader

### Using Pip

TorDownloader can be easily installed through pip:
- First, create a virtual environment: `python -m venv .venv`
- Then activate it: `.venv\Scripts\activate` on Windows or `.venv/activate` on Linux
- Finally install the tor_downloader package: `python -m pip install tor-downloader`

If installing TorDownloader through pip, it will be installed in your Python path. This is important as the path is used for default options such as the output directory, input directory, and log directory.
Because of this, you can give TorDownloader the argument `path` to find the folder TorDownloader was installed into.
Example of this command:
```python -m tor_downloader path```

## Running TorDownloader

Links must be given to TorDownloader in a JSON file with a single list of links. By default, TorDownloader looks for a `links.json` file in the `tor_downloader/data/input` directory.

Configuration options can be given in either a JSON file or as command line arguments.
The config file must be a JSON file with a single dictionary.
The command line arguments must be formatted like so:
    ```CONFIG=SETTING```

Configuration options:
<<<<<<< HEAD
    socks_port: Port of Tor Socks5 proxy.
    max_downloads: Maximum number of downloads to run at once.
    max_tor_checks: Number of times the Tor proxy will be checked to ensure Tor is working before crashing. Default is 5.
    tor_path: Path to the Tor executable (tor.exe). Often found in Tor Browser if installed (Tor Browser\\Browser\\TorBrowser\\Tor\\tor.exe).
    links_file: Path to the file containing the list of URLs to download. Must be a .json file with a single list of URLs.
    log_file: Path to the log file. Log file will be created if it does not exist.
    output_directory: Path to the directory to download the files to.
    config: Path to the configuration file. Only usable through the command line arguments.
=======
- socks_port: Port of Tor Socks5 proxy.
- max_downloads: Maximum number of downloads to run at once.
- tor_path: Path to the Tor executable (tor.exe). Often found in Tor Browser if installed (Tor Browser\\Browser\\TorBrowser\\Tor\\tor.exe).
- links_file: Path to the file containing the list of URLs to download. Must be a .json file with a single list of URLs.
- log_file: Path to the log file. Log file will be created if it does not exist.
- output_directory: Path to the directory to download the files to.
- config_file: Path to the configuration file. Only usable through the command line arguments.
>>>>>>> 4c9dfb4cd9ff34a2a23cb15d3212c4030e23871a

Example command:
    ```python -m tor_downloader max_downloads=7 tor_path="Tor Browser\\Browser\\TorBrowser\\Tor\\tor.exe" links_file=links.json output_directory=output```

## Getting Results

All files will be installed into the output directory specified in the config file or command line argument. By default, the output directory is `tor_downloader/data/input`.
