from __future__ import annotations

import json
import sys

import pytest

from tor_downloader.__main__ import _derive_top_level_folder, get_config_args, get_config_file
from tor_downloader.link_specs import load_links_spec


def test_load_links_spec_list_mode(tmp_path) -> None:
    links_file = tmp_path / "links.json"
    links_file.write_text(json.dumps(["http://a.onion/x", "http://b.onion/y/"]), encoding="utf-8")

    spec = load_links_spec(str(links_file))
    assert spec.mode == "list"
    assert spec.links == ["http://a.onion/x", "http://b.onion/y/"]


def test_load_links_spec_mirror_mode_with_dynamic_base(tmp_path) -> None:
    links_file = tmp_path / "links.json"
    payload = {
        "dynamic_base": "http://bootstrap.onion",
        "dynamic_min_bases": 3,
        "files": ["dataset/a.bin", "dataset/sub/"],
    }
    links_file.write_text(json.dumps(payload), encoding="utf-8")

    spec = load_links_spec(str(links_file))
    assert spec.mode == "mirror"
    assert spec.dynamic_base == "http://bootstrap.onion"
    assert spec.dynamic_min_bases == 3
    assert spec.bases == []


def test_load_links_spec_invalid_schema_raises(tmp_path) -> None:
    links_file = tmp_path / "links.json"
    links_file.write_text(json.dumps({"bases": ["http://a.onion"]}), encoding="utf-8")

    with pytest.raises(ValueError):
        load_links_spec(str(links_file))


def test_get_config_file_converts_values(tmp_path) -> None:
    config_file = tmp_path / "config.json"
    payload = {
        "socks_port": "9055",
        "max_downloads": "7",
        "request_connect_timeout": "20",
        "tor_path": "TOR/BIN",
        "empty_val": "",
        "none_val": None,
    }
    config_file.write_text(json.dumps(payload), encoding="utf-8")

    config = get_config_file(str(config_file))
    assert config["socks_port"] == 9055
    assert config["max_downloads"] == 7
    assert config["request_connect_timeout"] == 20
    assert config["tor_path"] == "tor/bin"
    assert "empty_val" not in config
    assert "none_val" not in config


def test_get_config_args_parses_cli(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "tor_downloader",
            "max_downloads=10",
            "probe_retries=2",
            "debug=true",
            "dry_run=false",
            "config=cfg.json",
        ],
    )
    args = get_config_args()

    assert args["max_downloads"] == 10
    assert args["probe_retries"] == 2
    assert args["debug"] is True
    assert args["dry_run"] is False
    assert args["config"] == "cfg.json"


def test_derive_top_level_folder() -> None:
    top = _derive_top_level_folder(
        [
            "http://host.onion/absolute/file.txt",
            "DATASET_A/dir/file.txt",
            "DATASET_B/another.txt",
        ]
    )
    assert top == "DATASET_A"
