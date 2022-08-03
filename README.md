# TorDownloader

Download files from Tor websites. Used to download data from ransomware leak sites.

Uses streamed file downloads and restarts if a file is not fully downloaded.
Gets the list of URLs to download either from a JSON file or from another URL (work in progress).

If installing TorDownloader through pip, it will be installed in your Python path. This is important as the path is used for default options such as the output directory, input directory, and log directory.
Because of this, you can give TorDownloader the argument `path` to find the folder TorDownloader was installed into.
Example of this command:
```python -m tor_downloader path```

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
    output_directory: Path to the directory to download the files to.
    config_file: Path to the configuration file. Only usable through the command line arguments.

Example command:
    ```python main.py max_downloads=7 tor_path=Tor Browser\\Browser\\TorBrowser\\Tor\\tor.exe links_file=links.json output_directory=output```