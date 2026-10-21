"""File-backed gateway settings; knowledge-base/knowledge/features/local_voice_gateway.md §4.4."""

from __future__ import annotations

import base64
import binascii
import ipaddress
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

import yaml

from voice_gateway.engines import build_engines
from voice_gateway.errors import ConfigError

__all__ = ["Config", "ConfigError", "Line", "load_config"]


@dataclass(frozen=True)
class Line:
    name: str
    connector_id: str
    webhook_key: bytes = field(repr=False)


@dataclass(frozen=True)
class Config:
    host: str
    port: int
    api_keys: tuple[bytes, ...] = field(repr=False)
    orchestrator_url: str
    lines: dict[str, Line]
    profiles: dict[str, dict]
    test_control: bool = False
    webhook_verify_ssl: bool = True
    # Where calls come from: {"kind": "scripted"} or a source's own settings
    # (sources.py); the source module validates the rest.
    source: dict = field(default_factory=lambda: {"kind": "scripted"})
    # Relative secret files in ``source`` resolve against the config file.
    base_dir: Path = Path(".")


def _text(value, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{label} must be a nonempty string")
    return value.strip()


def _secret(base: Path, value, label: str) -> str:
    path = base / _text(value, label)
    try:
        secret = path.read_text().strip()
    except (OSError, UnicodeError, ValueError):
        raise ConfigError(f"cannot read {label}") from None
    if not secret:
        raise ConfigError(f"{label} is empty")
    return secret


def _boolean(raw: dict, name: str, default: bool) -> bool:
    value = raw.get(name, default)
    if not isinstance(value, bool):
        raise ConfigError(f"{name} must be true or false")
    return value


def load_config(path: str | Path) -> Config:
    """Load YAML, resolving secret file paths relative to the config file."""
    path = Path(path)
    try:
        raw = yaml.safe_load(path.read_text())
    except (OSError, UnicodeError, yaml.YAMLError):
        raise ConfigError("cannot read config as YAML") from None
    if not isinstance(raw, dict):
        raise ConfigError("config must be a mapping")
    allowed = {
        "listen",
        "api_key_files",
        "orchestrator_url",
        "lines",
        "profiles",
        "test_control",
        "allow_remote_test_control",
        "webhook_verify_ssl",
        "source",
    }
    if raw.keys() - allowed:
        raise ConfigError("config contains unknown fields")
    listen = _text(raw.get("listen", "127.0.0.1:18440"), "listen")
    try:
        parsed = urlsplit("http://" + listen)
        host, port = parsed.hostname, parsed.port
        if (
            not host
            or not port
            or parsed.path
            or parsed.query
            or parsed.fragment
            or parsed.username
        ):
            raise ValueError
        loopback = host == "localhost" or ipaddress.ip_address(host).is_loopback
    except ValueError:
        raise ConfigError(
            "listen must be an IP address (or localhost) and port 1..65535"
        ) from None
    test_control = _boolean(raw, "test_control", False)
    allow_remote = _boolean(raw, "allow_remote_test_control", False)
    if test_control and not loopback and not allow_remote:
        raise ConfigError(
            "test_control requires loopback listen or allow_remote_test_control: true"
        )
    files = raw.get("api_key_files")
    if not isinstance(files, list) or not files:
        raise ConfigError("api_key_files must be a nonempty list")
    keys = tuple(_secret(path.parent, f, "API key file").encode() for f in files)
    if any(any(c <= 32 or c >= 127 for c in key) for key in keys):
        raise ConfigError(
            "API key files must contain printable ASCII without whitespace"
        )
    orchestrator = _text(raw.get("orchestrator_url"), "orchestrator_url").rstrip("/")
    try:
        url = urlsplit(orchestrator)
        if (
            url.scheme not in {"http", "https"}
            or not url.hostname
            or url.username
            or url.query
            or url.fragment
        ):
            raise ValueError
        _ = url.port
    except ValueError:
        raise ConfigError(
            "orchestrator_url must be an http(s) base URL without credentials, query or fragment"
        ) from None
    lines = raw.get("lines")
    if not isinstance(lines, list) or not lines:
        raise ConfigError("lines must be a nonempty list")
    configured = {}
    connectors = set()
    for item in lines:
        if not isinstance(item, dict) or item.keys() - {
            "name",
            "connector_id",
            "webhook_secret_file",
        }:
            raise ConfigError(
                "each line needs name, connector_id and webhook_secret_file"
            )
        name = _text(item.get("name"), "line name")
        connector = _text(item.get("connector_id"), "connector_id")
        if name in configured or connector in connectors:
            raise ConfigError("line names and connector IDs must be unique")
        secret = _secret(
            path.parent, item.get("webhook_secret_file"), "webhook secret file"
        )
        try:
            if not secret.startswith("whsec_"):
                raise ValueError
            key = base64.b64decode(secret[6:], validate=True)
            if not key:
                raise ValueError
        except (ValueError, binascii.Error):
            raise ConfigError(
                "webhook secret file must contain whsec_ followed by nonempty base64"
            ) from None
        configured[name] = Line(name, connector, key)
        connectors.add(connector)
    profiles = raw.get("profiles")
    if not isinstance(profiles, dict) or not profiles:
        raise ConfigError("profiles must be a nonempty mapping")
    for name in profiles:
        _text(name, "profile name")
    build_engines(profiles, path.parent)
    source = raw.get("source", {"kind": "scripted"})
    if not isinstance(source, dict) or not isinstance(source.get("kind"), str):
        raise ConfigError("source must be a mapping with a kind")
    # Imported here: a source imports the session, which imports this module.
    from voice_gateway.sources import build_source

    build_source(source, path.parent)
    return Config(
        host,
        port,
        keys,
        orchestrator,
        configured,
        profiles,
        test_control,
        _boolean(raw, "webhook_verify_ssl", True),
        source,
        path.parent.absolute(),
    )
