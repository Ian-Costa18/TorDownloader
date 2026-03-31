from __future__ import annotations

import random
import re
import socket
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import PurePosixPath
from typing import Dict
from urllib.parse import unquote, urlparse

import pytest
import requests


def _rand_name(rng: random.Random, prefix: str) -> str:
    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
    return prefix + "_" + "".join(rng.choice(alphabet) for _ in range(8))


@dataclass
class FakeSiteData:
    directories: Dict[str, dict[str, list[str]]]
    files: Dict[str, bytes]
    base_url: str

    @property
    def root_url(self) -> str:
        return f"{self.base_url}/"

    @property
    def file_urls(self) -> dict[str, str]:
        return {path: f"{self.base_url}{path}" for path in self.files}

    def directory_listing_html(self, directory_path: str) -> str:
        entry = self.directories[directory_path]
        lines = ["<html><body>"]
        if directory_path != "/":
            lines.append('<a href="../">../</a>')
        lines.append('<a href="${href}">ignored-template</a>')
        lines.append('<a href="javascript:void(0)">ignored-js</a>')
        lines.append('<a href="#anchor">ignored-anchor</a>')
        for child_dir in entry["dirs"]:
            name = PurePosixPath(child_dir.rstrip("/")).name + "/"
            lines.append(f'<a href="{name}">{name}</a>')
        for child_file in entry["files"]:
            name = PurePosixPath(child_file).name
            lines.append(f'<a href="{name}">{name}</a>')
        lines.append("</body></html>")
        return "\n".join(lines)


@dataclass
class OnionLikeServerData:
    base_url: str
    files: Dict[str, bytes]

    @property
    def root_url(self) -> str:
        return f"{self.base_url}/"

    @property
    def stable_url(self) -> str:
        return f"{self.base_url}/stable.bin"

    @property
    def slow_url(self) -> str:
        return f"{self.base_url}/slow/slow.bin"

    @property
    def flaky_url(self) -> str:
        return f"{self.base_url}/flaky/flaky.bin"

    @property
    def outage_url(self) -> str:
        return f"{self.base_url}/outage/outage.bin"

    @property
    def truncated_url(self) -> str:
        return f"{self.base_url}/broken/truncated.bin"


def _build_onion_like_payloads() -> Dict[str, bytes]:
    rng = random.Random(1337)

    def _payload(size: int) -> bytes:
        return bytes(rng.randrange(0, 256) for _ in range(size))

    return {
        "/stable.bin": _payload(4096),
        "/slow/slow.bin": _payload(3072),
        "/flaky/flaky.bin": _payload(2048),
        "/broken/truncated.bin": _payload(3584),
    }


def _create_random_site_data() -> tuple[
    dict[str, dict[str, list[str]]], dict[str, bytes]
]:
    rng = random.Random()
    directories: dict[str, dict[str, list[str]]] = {"/": {"dirs": [], "files": []}}
    files: dict[str, bytes] = {}
    file_exts = [".bin", ".txt", ".dat"]

    top_level_dirs: list[str] = []
    for _ in range(3):
        dirname = _rand_name(rng, "dir")
        path = f"/{dirname}/"
        directories[path] = {"dirs": [], "files": []}
        directories["/"]["dirs"].append(path)
        top_level_dirs.append(path)

    for parent in top_level_dirs:
        for _ in range(2):
            subdir = _rand_name(rng, "sub")
            sub_path = f"{parent}{subdir}/"
            directories[sub_path] = {"dirs": [], "files": []}
            directories[parent]["dirs"].append(sub_path)

    all_dirs = list(directories.keys())
    sizes = [128, 700, 2048, 4096, 8192]
    for directory in all_dirs:
        for _ in range(3):
            fname = _rand_name(rng, "file") + rng.choice(file_exts)
            fpath = f"{directory}{fname}" if directory != "/" else f"/{fname}"
            directories[directory]["files"].append(fpath)
            payload_size = rng.choice(sizes)
            files[fpath] = bytes(rng.randrange(0, 256) for _ in range(payload_size))

    return directories, files


def _make_handler(site_data: FakeSiteData):
    class FakeHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format, *args):  # noqa: A003
            return

        def do_HEAD(self):
            self._serve(send_body=False)

        def do_GET(self):
            self._serve(send_body=True)

        def _serve(self, send_body: bool) -> None:
            parsed = urlparse(self.path)
            path = unquote(parsed.path)

            if path in site_data.directories and not path.endswith("/"):
                location = path + "/"
                self.send_response(301)
                self.send_header("Location", location)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return

            if path in site_data.directories:
                html = site_data.directory_listing_html(path).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(html)))
                self.end_headers()
                if send_body:
                    self.wfile.write(html)
                return

            if path not in site_data.files:
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return

            body = site_data.files[path]
            status = 200
            headers = {}
            range_header = self.headers.get("Range")
            if range_header:
                match = re.match(r"bytes=\s*(\d+)-", range_header)
                if match:
                    start = int(match.group(1))
                    if start >= len(body):
                        self.send_response(416)
                        self.send_header("Content-Range", f"bytes */{len(body)}")
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                        return
                    status = 206
                    body = body[start:]
                    headers["Content-Range"] = (
                        f"bytes {start}-{start + len(body) - 1}/{len(site_data.files[path])}"
                    )

            self.send_response(status)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(len(body)))
            for key, value in headers.items():
                self.send_header(key, value)
            self.end_headers()

            if not send_body:
                return

            cursor = 0
            while cursor < len(body):
                chunk_len = min(64 + (cursor % 257), len(body) - cursor)
                self.wfile.write(body[cursor : cursor + chunk_len])
                self.wfile.flush()
                cursor += chunk_len

    return FakeHandler


def _make_onion_like_handler(payloads: Dict[str, bytes]):
    flaky_counters: dict[str, int] = {}
    counter_lock = threading.Lock()

    class OnionLikeHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format, *args):  # noqa: A003
            return

        def do_HEAD(self):
            self._route(send_body=False)

        def do_GET(self):
            self._route(send_body=True)

        def _route(self, send_body: bool) -> None:
            path = unquote(urlparse(self.path).path)

            if path == "/":
                html = (
                    "<html><body>"
                    '<a href="stable.bin">stable.bin</a>'
                    '<a href="slow/">slow/</a>'
                    '<a href="flaky/">flaky/</a>'
                    '<a href="outage/">outage/</a>'
                    '<a href="broken/">broken/</a>'
                    '<a href="${href}">ignored</a>'
                    "</body></html>"
                ).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(html)))
                self.end_headers()
                if send_body:
                    self.wfile.write(html)
                return

            if path in {"/slow/", "/flaky/", "/outage/", "/broken/"}:
                html = (
                    "<html><body>"
                    '<a href="../">../</a>'
                    f'<a href="{path.rsplit("/", 2)[-2]}.bin">file</a>'
                    "</body></html>"
                ).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(html)))
                self.end_headers()
                if send_body:
                    self.wfile.write(html)
                return

            file_path = path
            if path == "/slow/slow.bin":
                time.sleep(2)
            if path == "/flaky/flaky.bin":
                with counter_lock:
                    attempt = flaky_counters.get(path, 0) + 1
                    flaky_counters[path] = attempt
                if attempt <= 2:
                    self.send_response(503)
                    self.send_header("Retry-After", "1")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
            if path == "/outage/outage.bin":
                self.send_response(503)
                self.send_header("Retry-After", "1")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return

            if file_path not in payloads:
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return

            body = payloads[file_path]
            full_len = len(body)
            status = 200
            headers: dict[str, str] = {}

            range_header = self.headers.get("Range")
            if range_header:
                match = re.match(r"bytes=\s*(\d+)-", range_header)
                if match:
                    start = int(match.group(1))
                    if start >= full_len:
                        self.send_response(416)
                        self.send_header("Content-Range", f"bytes */{full_len}")
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                        return
                    status = 206
                    body = body[start:]
                    headers["Content-Range"] = (
                        f"bytes {start}-{full_len - 1}/{full_len}"
                    )

            self.send_response(status)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(len(body)))
            for key, value in headers.items():
                self.send_header(key, value)
            self.end_headers()

            if not send_body:
                return

            if file_path == "/broken/truncated.bin":
                cutoff = max(1, len(body) // 2)
                self.wfile.write(body[:cutoff])
                self.wfile.flush()
                try:
                    self.connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                self.connection.close()
                return

            cursor = 0
            while cursor < len(body):
                chunk_len = min(80 + (cursor % 97), len(body) - cursor)
                self.wfile.write(body[cursor : cursor + chunk_len])
                self.wfile.flush()
                cursor += chunk_len

    return OnionLikeHandler


@pytest.fixture()
def fake_web_server() -> FakeSiteData:
    directories, files = _create_random_site_data()

    server: ThreadingHTTPServer | None = None
    thread: threading.Thread | None = None

    try:
        placeholder = FakeSiteData(directories=directories, files=files, base_url="")
        server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(placeholder))
        host, port = server.server_address
        final_site = FakeSiteData(
            directories=directories,
            files=files,
            base_url=f"http://{host}:{port}",
        )

        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        yield final_site
    finally:
        if server is not None:
            server.shutdown()
            server.server_close()
        if thread is not None:
            thread.join(timeout=2)


class DummyTorInstance:
    """Minimal TorInstance-like test double returning plain requests sessions."""

    def __init__(self):
        self._counter = 0

    def get_session_with_number(self):
        self._counter += 1
        return requests.Session(), self._counter


@pytest.fixture()
def dummy_tor_instance() -> DummyTorInstance:
    return DummyTorInstance()


@pytest.fixture()
def onion_like_server() -> OnionLikeServerData:
    payloads = _build_onion_like_payloads()

    server: ThreadingHTTPServer | None = None
    thread: threading.Thread | None = None

    try:
        server = ThreadingHTTPServer(
            ("127.0.0.1", 0), _make_onion_like_handler(payloads)
        )
        host, port = server.server_address
        data = OnionLikeServerData(
            base_url=f"http://{host}:{port}",
            files=payloads,
        )

        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        yield data
    finally:
        if server is not None:
            server.shutdown()
            server.server_close()
        if thread is not None:
            thread.join(timeout=2)
