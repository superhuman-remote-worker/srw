"""Connectors whose SSH key is held by an ``ssh-agent`` in the workspace (C1).

Two connector kinds hand a private key to the workspace: an SSH-key
``repository`` and an ``ssh_key``. Their key never lands on disk there; it is
loaded into a dedicated ``ssh-agent`` and reached through an opaque
``srw-repo-<32hex>`` alias (``shared.runtime.core.workspace_ssh_identity``).

This module is the control-plane half and is deliberately self-contained so
the ``ssh_key`` and ``repository`` drivers (connector drivers, D1a) can take it
over unchanged:

* :func:`validate_workspace_ssh_connector` runs on create and update. A shared
  connector writes into other users' ``~/.ssh/config``, so host, user, port and
  pinned host keys are held to a strict grammar, and a passphrase-protected key
  is refused because a workspace can never unlock it.

Errors are :class:`WorkspaceSshConnectorError` (a ``ValueError``) whose message
is the API's 400 detail; it never echoes key material.
"""

from __future__ import annotations

from typing import Any, Mapping

from shared.runtime.core.workspace_ssh_identity import (
    SshEndpointError,
    normalize_ssh_host,
    normalize_ssh_port,
    normalize_ssh_user,
    parse_known_hosts,
    parse_ssh_repository_url,
)
from shared.runtime.utils.ssh_key import InvalidSSHKeyError, ssh_public_identity

#: Optional non-secret fields an ``ssh_key`` connector carries in ``config``.
SSH_KEY_CONFIG_FIELDS = frozenset({"host", "user", "port", "known_hosts"})


class WorkspaceSshConnectorError(ValueError):
    """A connector's SSH settings are unsafe or unusable (HTTP 400 detail)."""


def repository_uses_ssh_key(credentials: Mapping[str, Any] | None) -> bool:
    """Whether a repository connector authenticates with an SSH key.

    Mirrors the clone path: an explicit ``auth_method`` wins, otherwise a
    stored ``ssh_key`` means SSH.
    """

    creds = credentials if isinstance(credentials, Mapping) else {}
    auth_method = str(creds.get("auth_method") or "").strip().lower()
    if auth_method:
        return auth_method == "ssh"
    return bool(creds.get("ssh_key"))


def ssh_key_connector_private_key(credentials: Mapping[str, Any] | None) -> str:
    """The private key of an ``ssh_key`` connector (``files[0].contents``)."""

    creds = credentials if isinstance(credentials, Mapping) else {}
    files = creds.get("files")
    if not isinstance(files, list) or not files or not isinstance(files[0], Mapping):
        return ""
    contents = files[0].get("contents")
    return contents if isinstance(contents, str) else ""


def _check_private_key(private_key: str) -> None:
    try:
        ssh_public_identity(private_key)
    except InvalidSSHKeyError as exc:
        raise WorkspaceSshConnectorError(f"Invalid SSH key: {exc}") from exc


def _known_hosts_config(value: Any, *, host: str, port: int) -> str | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if not isinstance(value, str):
        raise WorkspaceSshConnectorError("known_hosts must be text")
    try:
        entries = parse_known_hosts(value, host=host, port=port, require_match=True)
    except SshEndpointError as exc:
        raise WorkspaceSshConnectorError(str(exc)) from exc
    return "\n".join(entries) or None


def _validate_ssh_key_config(config: Mapping[str, Any] | None) -> dict[str, Any]:
    raw = dict(config or {})
    unknown = sorted(set(raw) - SSH_KEY_CONFIG_FIELDS)
    if unknown:
        raise WorkspaceSshConnectorError(
            f"Unknown SSH key connector config field(s): {', '.join(unknown)}"
        )
    host_value = raw.get("host")
    if host_value is None or (isinstance(host_value, str) and not host_value.strip()):
        if any(raw.get(field) not in (None, "") for field in ("user", "port")) or (
            str(raw.get("known_hosts") or "").strip()
        ):
            raise WorkspaceSshConnectorError(
                "Set an SSH host before setting its user, port or known_hosts"
            )
        return {}
    try:
        host = normalize_ssh_host(host_value)
        normalized: dict[str, Any] = {"host": host}
        if raw.get("user") not in (None, ""):
            normalized["user"] = normalize_ssh_user(raw["user"])
        port = 22
        if raw.get("port") not in (None, ""):
            port = normalize_ssh_port(raw["port"])
            normalized["port"] = port
    except SshEndpointError as exc:
        raise WorkspaceSshConnectorError(str(exc)) from exc
    known_hosts = _known_hosts_config(raw.get("known_hosts"), host=host, port=port)
    if known_hosts:
        normalized["known_hosts"] = known_hosts
    return normalized


def _validate_repository_ssh_config(
    connection_url: str | None, config: Mapping[str, Any] | None
) -> dict[str, Any]:
    out = dict(config or {})
    try:
        target = parse_ssh_repository_url(connection_url)
    except SshEndpointError as exc:
        raise WorkspaceSshConnectorError(str(exc)) from exc
    known_hosts = _known_hosts_config(
        out.pop("known_hosts", None), host=target.host, port=target.port
    )
    if known_hosts:
        out["known_hosts"] = known_hosts
    return out


def validate_workspace_ssh_connector(
    ds_type: str,
    *,
    connection_url: str | None,
    config: Mapping[str, Any] | None,
    credentials: Mapping[str, Any] | None,
    check_key: bool = True,
) -> dict[str, Any]:
    """Validate an ``ssh_key`` or SSH-key ``repository`` connector.

    Returns the normalized ``config`` to persist. ``credentials`` are the
    effective credentials; ``check_key=False`` skips the private-key check when
    an update preserves the stored key. A token repository only has to keep
    ``known_hosts`` out of its config, since nothing would ever read it.
    """

    if ds_type == "ssh_key":
        normalized = _validate_ssh_key_config(config)
        if check_key:
            _check_private_key(ssh_key_connector_private_key(credentials))
        return normalized
    if ds_type != "repository":
        return dict(config or {})
    if not repository_uses_ssh_key(credentials):
        out = dict(config or {})
        if str(out.pop("known_hosts", None) or "").strip():
            raise WorkspaceSshConnectorError(
                "known_hosts applies only to SSH-key repository connectors"
            )
        return out
    normalized = _validate_repository_ssh_config(connection_url, config)
    if check_key:
        _check_private_key(str((credentials or {}).get("ssh_key") or ""))
    return normalized


__all__ = [
    "SSH_KEY_CONFIG_FIELDS",
    "WorkspaceSshConnectorError",
    "repository_uses_ssh_key",
    "ssh_key_connector_private_key",
    "validate_workspace_ssh_connector",
]
