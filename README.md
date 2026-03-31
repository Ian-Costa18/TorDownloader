# TorDownloader

Download files from Tor websites. Used to download data from ransomware leak sites.

Uses streamed file downloads and restarts if a file is not fully downloaded.
Gets the list of URLs to download either from a JSON file or from another URL (work in progress).

## Installing TorDownloader

### Using Pip

TorDownloader can be easily installed through pip:

* First, create a virtual environment: `python -m venv .venv`
* Then activate it: `.venv\Scripts\activate` on Windows or `.venv/activate` on Linux
* Finally install the tor_downloader package: `python -m pip install tor-downloader`

If installing TorDownloader through pip, it will be installed in your Python path. This is important as the path is used for default options such as the output directory, input directory, and log directory.
Because of this, you can give TorDownloader the argument `path` to find the folder TorDownloader was installed into.
Example of this command:
```python -m tor_downloader path```

## Running TorDownloader

Links are given to TorDownloader in `links.json` using either schema below. By default, TorDownloader looks for this file in `tor_downloader/data/input`.

Schema 1 (legacy list mode):

```json
[
    "http://mirror1.onion/path/to/file.txt",
    "http://mirror1.onion/path/to/directory/"
]
```

Schema 2 (mirror mode):

```json
{
    "bases": [
        "http://mirror1.onion",
        "http://mirror2.onion"
    ],
    "files": [
        "path/to/file.txt",
        "path/to/directory/"
    ]
}
```

In mirror mode, each file is treated as a relative target and candidate bases can be mixed across retries. This means a partial file can continue from another base if one mirror goes down.

Configuration options can be given in either a JSON file or as command line arguments.
The config file must be a JSON file with a single dictionary.
The command line arguments must be formatted like so:
    ```CONFIG=SETTING```

Configuration options:

* socks_port: Port of Tor Socks5 proxy.
* enum_workers: Number of concurrent directory enumeration workers.
* download_workers: Number of concurrent file download workers.
* request_connect_timeout: Per-request connect timeout in seconds. Default is 60.
* request_read_timeout: Per-request read timeout in seconds. Default is 300.
* max_tor_checks: Number of times the Tor proxy will be checked to ensure Tor is working before crashing. Default is 5.
* tor_path: Path to the Tor executable (tor.exe). Often found in Tor Browser if installed (Tor Browser\\Browser\\TorBrowser\\Tor\\tor.exe).
* links_file: Path to the JSON file containing either a list of URLs or a `{bases, files}` mirror schema.
* log_file: Base path for logs. Each run writes to a timestamped log file in the same directory.
* log_max_archives: Maximum number of compressed historical logs to keep. Default is 30.
* log_max_total_mb: Maximum total size (MB) for compressed historical logs. Default is 500.
* output_dir: Path to the directory to download the files to.
* config: Path to the configuration file. Only usable through the command line arguments.

Example command:
    ```python -m tor_downloader enum_workers=12 download_workers=4 tor_path="Tor Browser\\Browser\\TorBrowser\\Tor\\tor.exe" links_file=links.json output_directory=output```

## Getting Results

All files are installed into the output directory specified in config/arguments. Paths are hostless and relative to the requested file structure (no base host folder at the top level).

During execution, TorDownloader shows two progress bars:

* Enumerating: directory tasks completed versus discovered.
* Downloading: file tasks completed versus discovered.

Runtime progress is stored in a SQLite database named `download_progress.sqlite3` in the same folder as `links.json`. This allows interrupted runs to resume without rewriting large JSON snapshots.

## TODO

* Change all file handling from os to pathlib.
* Fix ctrl-c for quitting the script while it is downloading.
* Fix progress bar bug in file_downloader.py.
