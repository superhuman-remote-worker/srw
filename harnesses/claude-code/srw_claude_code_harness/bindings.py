"""Parsing of the workspace descriptor in ``/run/srw/bindings.json``. Standard library only."""

from __future__ import annotations

from .config import ConfigError, _is_int, _object
from .workspace import WorkspaceAccess

__all__ = ["WorkspaceAccess", "parse_bindings"]


def _text(workspace: dict, key: str) -> str:
    value = workspace.get(key)
    if not isinstance(value, str) or not value:
        raise ConfigError(f"workspace.{key} must be a non-empty string")
    return value


def parse_bindings(data: object) -> WorkspaceAccess:
    obj = _object(data, "bindings")
    if "workspace" not in obj or obj["workspace"] is None:
        raise ConfigError("bindings have no workspace")
    workspace = _object(obj["workspace"], "workspace")
    if workspace.get("transport") != "ssh":
        raise ConfigError("workspace transport must be ssh")
    port = workspace.get("port")
    if not _is_int(port) or not 1 <= port <= 65535:
        raise ConfigError("workspace.port must be a valid port number")
    return WorkspaceAccess(
        host=_text(workspace, "host"),
        port=port,
        user=_text(workspace, "user"),
        root=_text(workspace, "root"),
        private_key_file=_text(workspace, "privateKeyFile"),
        known_hosts_file=_text(workspace, "knownHostsFile"),
    )
