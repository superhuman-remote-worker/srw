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
* :func:`build_workspace_ssh_identities` turns resolved rows into the hidden
  ``workspace_ssh_identities`` delivery field, carried and stripped wherever
  ``managed_repository_credentials`` is; :func:`workspace_ssh_descriptor` is
  the non-secret part that rides the connector's ``datasources`` entry in place
  of its key. Both re-validate the stored row, so a row that predates the
  validation degrades to an unavailable connector instead of reaching a
  workspace's SSH config.
* :func:`probe_workspace_ssh_connector` is Test connection: it reaches the
  connector's host and reports the host key, which the connector form offers
  to pin.

Errors are :class:`WorkspaceSshConnectorError` (a ``ValueError``) whose message
is the API's 400 detail; it never echoes key material.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Iterable, Mapping
from uuid import UUID, uuid5

from shared.runtime.core.workspace_ssh_identity import (
    WORKSPACE_SSH_IDENTITY_VERSION,
    SshEndpointError,
    SshRepositoryTarget,
    normalize_ssh_host,
    normalize_ssh_port,
    normalize_ssh_user,
    parse_known_hosts,
    parse_ssh_repository_url,
    workspace_ssh_identity_alias,
)
from shared.runtime.utils.ssh_key import InvalidSSHKeyError, ssh_public_identity

logger = logging.getLogger(__name__)

#: Deployment default pins (``orchestrator.workspaceSshKnownHosts``), in
#: ``known_hosts`` format. Only lines naming a connector's host apply to it.
WORKSPACE_SSH_KNOWN_HOSTS_ENV = "WORKSPACE_SSH_KNOWN_HOSTS"
# uuid5 namespace for connector identity authorities. One resident per
# connector per workspace, shared by a root job and its worktree children.
_IDENTITY_NAMESPACE = UUID("8a3c4f0e-6d1b-5c2a-9e7f-5b0d2c1a4e93")

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


# ---------------------------------------------------------------------------
# Delivery
# ---------------------------------------------------------------------------


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return {}
    return dict(value) if isinstance(value, Mapping) else {}


def is_workspace_ssh_connector(ds: Mapping[str, Any]) -> bool:
    """An ``ssh_key`` connector, or a repository that authenticates by key."""

    ds_type = ds.get("type")
    if ds_type == "ssh_key":
        return True
    return ds_type == "repository" and repository_uses_ssh_key(
        _json_object(ds.get("credentials"))
    )


def workspace_ssh_authority_id(datasource_id: Any) -> str:
    """The identity authority of one connector (stable across dispatches)."""

    return str(uuid5(_IDENTITY_NAMESPACE, f"datasource:{UUID(str(datasource_id))}"))


def default_workspace_ssh_known_hosts() -> str:
    """The deployment's default pins, read per call like the KB default."""

    return os.getenv(WORKSPACE_SSH_KNOWN_HOSTS_ENV, "")


@dataclass(frozen=True, repr=False)
class WorkspaceSshIdentity:
    """One connector's resolved identity. Never log or persist it."""

    authority_id: str
    kind: str
    alias: str
    host: str | None
    port: int | None
    user: str | None
    repository: SshRepositoryTarget | None
    known_hosts: tuple[str, ...]
    private_key: str
    public_key: str
    fingerprint: str

    @property
    def clone_url(self) -> str | None:
        if self.repository is None:
            return None
        return self.repository.clone_url(self.alias)

    def to_payload(self) -> dict[str, Any]:
        """Internal ``workspace_ssh_identities`` item, the only one with a key."""

        return {
            "version": WORKSPACE_SSH_IDENTITY_VERSION,
            "authority_id": self.authority_id,
            # No credential revision exists; the workspace replaces a resident
            # whose key no longer matches the fingerprint (self-heal).
            "generation": 1,
            "kind": self.kind,
            "alias": self.alias,
            "ssh_host": self.host,
            "ssh_port": self.port,
            "ssh_user": self.user,
            "extra_hosts": [self.host] if self.kind == "ssh_key" and self.host else [],
            "known_hosts": list(self.known_hosts),
            "strict_host_key_checking": bool(self.known_hosts),
            "private_key": self.private_key,
            "public_key_fingerprint": self.fingerprint,
        }

    def descriptor(self) -> dict[str, Any]:
        """The non-secret ``ssh_identity`` of a ``datasources`` entry."""

        descriptor: dict[str, Any] = {
            "alias": self.alias,
            "authority_id": self.authority_id,
            "fingerprint": self.fingerprint,
            "host_key_pinned": bool(self.known_hosts),
        }
        if self.clone_url is not None:
            descriptor["clone_url"] = self.clone_url
        if self.host is not None:
            descriptor["host"] = self.host
        if self.port is not None:
            descriptor["port"] = self.port
        if self.user is not None:
            descriptor["user"] = self.user
        if self.kind == "ssh_key":
            descriptor["public_key"] = self.public_key
        return descriptor


def _pins(
    configured: Any, *, host: str, port: int, default_known_hosts: str
) -> tuple[str, ...]:
    if str(configured or "").strip():
        try:
            return tuple(
                parse_known_hosts(configured, host=host, port=port, require_match=True)
            )
        except SshEndpointError as exc:
            raise WorkspaceSshConnectorError(str(exc)) from exc
    try:
        return tuple(
            parse_known_hosts(
                default_known_hosts, host=host, port=port, require_match=False
            )
        )
    except SshEndpointError as exc:
        raise WorkspaceSshConnectorError(
            f"{WORKSPACE_SSH_KNOWN_HOSTS_ENV} is invalid: {exc}"
        ) from exc


def workspace_ssh_identity(
    ds: Mapping[str, Any], *, default_known_hosts: str | None = None
) -> WorkspaceSshIdentity:
    """Resolve one stored row into its identity, or say why it is unusable."""

    if default_known_hosts is None:
        default_known_hosts = default_workspace_ssh_known_hosts()
    try:
        authority_id = workspace_ssh_authority_id(ds.get("id"))
    except (TypeError, ValueError) as exc:
        raise WorkspaceSshConnectorError("connector has no stable identity") from exc
    credentials = _json_object(ds.get("credentials"))
    config = _json_object(ds.get("config"))
    if ds.get("type") == "ssh_key":
        settings = _validate_ssh_key_config(config)
        private_key = ssh_key_connector_private_key(credentials)
        host = settings.get("host")
        port = settings.get("port", 22 if host else None)
        user = settings.get("user")
        repository = None
        pins = (
            _pins(
                settings.get("known_hosts"),
                host=host,
                port=port,
                default_known_hosts=default_known_hosts,
            )
            if host
            else ()
        )
    elif ds.get("type") == "repository":
        try:
            target = parse_ssh_repository_url(ds.get("connection_url"))
        except SshEndpointError as exc:
            raise WorkspaceSshConnectorError(str(exc)) from exc
        private_key = str(credentials.get("ssh_key") or "")
        host, port, user, repository = target.host, target.port, target.user, target
        pins = _pins(
            config.get("known_hosts"),
            host=host,
            port=port,
            default_known_hosts=default_known_hosts,
        )
    else:
        raise WorkspaceSshConnectorError("connector does not hold an SSH key")
    try:
        public_key, fingerprint = ssh_public_identity(private_key)
    except InvalidSSHKeyError as exc:
        raise WorkspaceSshConnectorError(f"Invalid SSH key: {exc}") from exc
    return WorkspaceSshIdentity(
        authority_id=authority_id,
        kind=str(ds["type"]),
        alias=workspace_ssh_identity_alias(authority_id),
        host=host,
        port=port,
        user=user,
        repository=repository,
        known_hosts=pins,
        private_key=private_key,
        public_key=public_key,
        fingerprint=fingerprint,
    )


def workspace_ssh_descriptor(
    ds: Mapping[str, Any], *, default_known_hosts: str | None = None
) -> dict[str, Any] | None:
    """The ``ssh_identity`` an agent payload entry carries instead of a key.

    ``unavailable`` names why a stored row cannot be delivered (a pre-C1 row
    with an encrypted key or an unsafe host, say); the agent then skips that
    connector rather than writing anything from it.
    """

    if not is_workspace_ssh_connector(ds):
        return None
    try:
        return workspace_ssh_identity(
            ds, default_known_hosts=default_known_hosts
        ).descriptor()
    except WorkspaceSshConnectorError as exc:
        try:
            authority_id = workspace_ssh_authority_id(ds.get("id"))
        except (TypeError, ValueError):
            return {"unavailable": str(exc)}
        return {
            "alias": workspace_ssh_identity_alias(authority_id),
            "authority_id": authority_id,
            "unavailable": str(exc),
        }


def build_workspace_ssh_identities(
    resolved_ds: Iterable[Mapping[str, Any]] | None,
    *,
    default_known_hosts: str | None = None,
) -> list[dict[str, Any]] | None:
    """The hidden ``workspace_ssh_identities`` field for one delivery.

    Built only from an already-authorized, exactly-resolved connector set,
    like ``build_datasources_payload``. A row that cannot be delivered is
    logged by name and left out; it never fails the delivery.
    """

    if default_known_hosts is None:
        default_known_hosts = default_workspace_ssh_known_hosts()
    identities: list[dict[str, Any]] = []
    for ds in resolved_ds or []:
        if not is_workspace_ssh_connector(ds):
            continue
        try:
            identity = workspace_ssh_identity(
                ds, default_known_hosts=default_known_hosts
            )
        except WorkspaceSshConnectorError as exc:
            logger.warning(
                "SSH connector %r cannot be delivered to a workspace: %s",
                ds.get("name"),
                exc,
            )
            continue
        identities.append(identity.to_payload())
    return identities or None


# ---------------------------------------------------------------------------
# Test connection
# ---------------------------------------------------------------------------

_HOST_KEY_PROBE_TIMEOUT_S = 10


async def fetch_ssh_host_key(host: str, port: int) -> str:
    """Return the ``"<type> <base64>"`` host key ``host:port`` presents.

    Only the key exchange runs; nothing authenticates and no connector key
    is offered. The answer is what the connector form offers to pin.
    """

    import asyncio

    import asyncssh

    async with asyncio.timeout(_HOST_KEY_PROBE_TIMEOUT_S):
        key = await asyncssh.get_server_host_key(host, port)
    if key is None:
        raise ValueError("the server presented no host key")
    exported = key.export_public_key("openssh").decode("ascii").split()
    (entry,) = parse_known_hosts(" ".join(exported[:2]))
    return entry


async def probe_workspace_ssh_connector(ds: Mapping[str, Any]) -> dict[str, Any] | None:
    """Test an SSH connector's endpoint: reach it and report its host key.

    ``None`` means there is no endpoint to test (an ``ssh_key`` without a
    host). The reported ``details.host_key`` is the bare ``<type> <base64>``
    pair the connector's ``known_hosts`` field accepts. A pinned connector
    whose host now presents another key fails, as its workspace clone would.
    """

    import base64
    import hashlib

    if (
        ds.get("type") == "ssh_key"
        and not str(_json_object(ds.get("config")).get("host") or "").strip()
    ):
        return None
    try:
        identity = workspace_ssh_identity(ds)
    except WorkspaceSshConnectorError as exc:
        return {"status": "error", "message": str(exc)}
    if identity.host is None or identity.port is None:
        return None
    try:
        host_key = await fetch_ssh_host_key(identity.host, identity.port)
    except Exception:
        logger.warning(
            "SSH host key probe failed for connector %r", ds.get("name"), exc_info=True
        )
        return {
            "status": "error",
            "message": f"Could not reach SSH host {identity.host}:{identity.port}",
        }
    digest = hashlib.sha256(base64.b64decode(host_key.split()[1])).digest()
    fingerprint = "SHA256:" + base64.b64encode(digest).decode("ascii").rstrip("=")
    pinned = bool(identity.known_hosts)
    matches = host_key in identity.known_hosts
    details = {
        "host": identity.host,
        "port": identity.port,
        "host_key": host_key,
        "host_key_fingerprint": fingerprint,
        "host_key_pinned": pinned,
        "host_key_matches_pin": matches if pinned else None,
    }
    endpoint = f"{identity.host}:{identity.port}"
    if pinned and not matches:
        return {
            "status": "error",
            "message": (
                f"{endpoint} presented host key {fingerprint}, which is not the "
                "pinned key; workspaces will refuse to connect"
            ),
            "details": details,
        }
    if pinned:
        message = f"Reached {endpoint}; host key {fingerprint} matches the pin"
    else:
        message = (
            f"Reached {endpoint}; host key {fingerprint} is not pinned "
            "(workspaces trust it on first use). Pin it to refuse a changed key"
        )
    return {"status": "ok", "message": message, "details": details}


__all__ = [
    "SSH_KEY_CONFIG_FIELDS",
    "WORKSPACE_SSH_KNOWN_HOSTS_ENV",
    "WorkspaceSshConnectorError",
    "WorkspaceSshIdentity",
    "build_workspace_ssh_identities",
    "default_workspace_ssh_known_hosts",
    "fetch_ssh_host_key",
    "is_workspace_ssh_connector",
    "repository_uses_ssh_key",
    "ssh_key_connector_private_key",
    "probe_workspace_ssh_connector",
    "validate_workspace_ssh_connector",
    "workspace_ssh_authority_id",
    "workspace_ssh_descriptor",
    "workspace_ssh_identity",
]
