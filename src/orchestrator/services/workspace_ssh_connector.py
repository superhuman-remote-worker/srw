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
* :func:`workspace_ssh_identity` resolves a stored row into the item of the
  hidden ``workspace_ssh_identities`` delivery field (built through the
  connector drivers by ``agent_datasource_payload.build_workspace_ssh_identities``
  and carried and stripped wherever ``managed_repository_credentials`` is);
  :func:`workspace_ssh_descriptor` is the non-secret part that rides the
  connector's ``datasources`` entry in place of its key. Both re-validate the
  stored row, so a row that predates the validation degrades to an
  unavailable connector instead of reaching a workspace's SSH config.
* :func:`probe_workspace_ssh_connector` is Test connection: it reaches the
  connector's host and reports the host key, which the connector form offers
  to pin.

Errors are :class:`WorkspaceSshConnectorError` (a ``ValueError``) whose message
is the API's 400 detail; it never echoes key material.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Mapping
from uuid import UUID, uuid5

from orchestrator.services.datasource_config import stored_json_object
from shared.runtime.core.workspace_ssh_identity import (
    WORKSPACE_SSH_IDENTITY_VERSION,
    SshEndpointError,
    SshRepositoryTarget,
    normalize_ssh_host,
    normalize_ssh_port,
    normalize_ssh_user,
    parse_known_hosts,
    parse_ssh_repository_url,
    select_known_hosts,
    workspace_ssh_identity_alias,
)
from shared.runtime.utils.ssh_key import (
    InvalidSSHKeyError,
    private_key_is_encrypted,
    ssh_public_identity,
)

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
    """A connector's SSH settings are unsafe or unusable (HTTP 400 detail).

    ``code`` is a fixed, credential-free reason: the only thing that leaves
    the API path. Descriptors, the workspace README and logs carry the code,
    never the message, which may quote what the connector's author typed.
    """

    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


#: The reason codes an ``unavailable`` descriptor can carry.
UNAVAILABLE_REASONS = frozenset(
    {
        "ssh_endpoint_invalid",
        "ssh_key_invalid",
        "ssh_key_passphrase",
        "known_hosts_invalid",
        "default_known_hosts_invalid",
        "ssh_identity_unresolvable",
    }
)


def known_hosts_host_field(host: str, port: int) -> str:
    """How ``known_hosts`` names ``host`` on ``port``."""

    return host if port == 22 else f"[{host}]:{port}"


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


def _check_private_key(private_key: str) -> tuple[str, str]:
    """``(public key, fingerprint)``; refusals never quote the key text."""

    try:
        return ssh_public_identity(private_key)
    except InvalidSSHKeyError as exc:
        # validate_private_key quotes the first or last key line in some of
        # its messages; none of that may reach a log, a README or a 400.
        if private_key_is_encrypted(private_key):
            raise WorkspaceSshConnectorError(
                "Invalid SSH key: it is passphrase-protected; remove the "
                "passphrase (ssh-keygen -p) or generate a dedicated key without one",
                code="ssh_key_passphrase",
            ) from exc
        raise WorkspaceSshConnectorError(
            "Invalid SSH key: expected an unencrypted OpenSSH or PEM private key",
            code="ssh_key_invalid",
        ) from exc


def _known_hosts_config(value: Any, *, host: str, port: int) -> str | None:
    """Pinned host keys as ``known_hosts`` lines that name ``host``/``port``.

    Stored host-qualified (``host type key`` or ``[host]:port type key``) so
    that a later edit of the host or port no longer matches its old pin and
    is refused instead of silently trusting the old host's key for the new.
    A bare ``type key`` pair (what Test reports) is qualified here.
    """

    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if not isinstance(value, str):
        raise WorkspaceSshConnectorError(
            "known_hosts must be text", code="known_hosts_invalid"
        )
    try:
        entries = parse_known_hosts(value, host=host, port=port)
    except SshEndpointError as exc:
        raise WorkspaceSshConnectorError(
            f"{exc}; Test the connector again and pin the key it reports",
            code="known_hosts_invalid",
        ) from exc
    field = known_hosts_host_field(host, port)
    return "\n".join(f"{field} {entry}" for entry in entries) or None


def _validate_ssh_key_config(config: Mapping[str, Any] | None) -> dict[str, Any]:
    raw = dict(config or {})
    unknown = sorted(set(raw) - SSH_KEY_CONFIG_FIELDS)
    if unknown:
        raise WorkspaceSshConnectorError(
            f"Unknown SSH key connector config field(s): {', '.join(unknown)}",
            code="ssh_endpoint_invalid",
        )
    host_value = raw.get("host")
    if host_value is None or (isinstance(host_value, str) and not host_value.strip()):
        if any(raw.get(field) not in (None, "") for field in ("user", "port")) or (
            str(raw.get("known_hosts") or "").strip()
        ):
            raise WorkspaceSshConnectorError(
                "Set an SSH host before setting its user, port or known_hosts",
                code="ssh_endpoint_invalid",
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
        raise WorkspaceSshConnectorError(str(exc), code="ssh_endpoint_invalid") from exc
    known_hosts = _known_hosts_config(raw.get("known_hosts"), host=host, port=port)
    if known_hosts:
        normalized["known_hosts"] = known_hosts
    return normalized


def _repository_target(connection_url: str | None) -> SshRepositoryTarget:
    try:
        return parse_ssh_repository_url(connection_url)
    except SshEndpointError as exc:
        raise WorkspaceSshConnectorError(str(exc), code="ssh_endpoint_invalid") from exc


def _validate_repository_ssh_config(
    connection_url: str | None, config: Mapping[str, Any] | None
) -> dict[str, Any]:
    out = dict(config or {})
    target = _repository_target(connection_url)
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
                "known_hosts applies only to SSH-key repository connectors",
                code="known_hosts_invalid",
            )
        return out
    normalized = _validate_repository_ssh_config(connection_url, config)
    if check_key:
        _check_private_key(str((credentials or {}).get("ssh_key") or ""))
    return normalized


# ---------------------------------------------------------------------------
# Delivery
# ---------------------------------------------------------------------------


def is_workspace_ssh_connector(ds: Mapping[str, Any]) -> bool:
    """An ``ssh_key`` connector, or a repository that authenticates by key."""

    ds_type = ds.get("type")
    if ds_type == "ssh_key":
        return True
    return ds_type == "repository" and repository_uses_ssh_key(
        stored_json_object(ds.get("credentials"))
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
            return tuple(parse_known_hosts(configured, host=host, port=port))
        except SshEndpointError as exc:
            raise WorkspaceSshConnectorError(
                str(exc), code="known_hosts_invalid"
            ) from exc
    try:
        entries, ignored = select_known_hosts(default_known_hosts, host=host, port=port)
    except SshEndpointError as exc:
        raise WorkspaceSshConnectorError(
            f"{WORKSPACE_SSH_KNOWN_HOSTS_ENV} is invalid",
            code="default_known_hosts_invalid",
        ) from exc
    if ignored:
        # One bad line in a deployment-wide list must not disable every SSH
        # connector; it disables only itself.
        logger.warning(
            "%s: ignored %d unusable line(s) for %s",
            WORKSPACE_SSH_KNOWN_HOSTS_ENV,
            ignored,
            known_hosts_host_field(host, port),
        )
    return tuple(entries)


def workspace_ssh_identity(
    ds: Mapping[str, Any], *, default_known_hosts: str | None = None
) -> WorkspaceSshIdentity:
    """Resolve one stored row into its identity, or say why it is unusable."""

    if default_known_hosts is None:
        default_known_hosts = default_workspace_ssh_known_hosts()
    try:
        authority_id = workspace_ssh_authority_id(ds.get("id"))
    except (TypeError, ValueError) as exc:
        raise WorkspaceSshConnectorError(
            "connector has no stable identity", code="ssh_identity_unresolvable"
        ) from exc
    credentials = stored_json_object(ds.get("credentials"))
    config = stored_json_object(ds.get("config"))
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
        target = _repository_target(ds.get("connection_url"))
        private_key = str(credentials.get("ssh_key") or "")
        host, port, user, repository = target.host, target.port, target.user, target
        pins = _pins(
            config.get("known_hosts"),
            host=host,
            port=port,
            default_known_hosts=default_known_hosts,
        )
    else:
        raise WorkspaceSshConnectorError(
            "connector does not hold an SSH key", code="ssh_identity_unresolvable"
        )
    public_key, fingerprint = _check_private_key(private_key)
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

    ``unavailable`` is a fixed reason code (:data:`UNAVAILABLE_REASONS`) for a
    stored row that cannot be delivered (a pre-C1 row with an encrypted key or
    an unsafe host, say); the agent then skips that connector rather than
    writing anything from it. Never the error text, which can quote the key or
    whatever the connector's author typed into another user's README.
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
            return {"unavailable": "ssh_identity_unresolvable"}
        return {
            "alias": workspace_ssh_identity_alias(authority_id),
            "authority_id": authority_id,
            "unavailable": exc.code,
        }


# ---------------------------------------------------------------------------
# Test connection
# ---------------------------------------------------------------------------

_HOST_KEY_PROBE_TIMEOUT_S = 10


async def fetch_ssh_host_key(host: str, port: int) -> str:
    """Return the ``"<type> <base64>"`` host key ``host:port`` presents.

    Only the key exchange runs; nothing authenticates and no connector key
    is offered.
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


def apply_ssh_test_overrides(
    ds: Mapping[str, Any], overrides: Mapping[str, Any] | None
) -> dict[str, Any]:
    """The row Test probes: an SSH connector's endpoint as the form edits it.

    The connector form tests before it saves, so Test must reach the host
    and port being typed, not the stored ones. Only the non-secret endpoint
    fields of an SSH connector are taken (``connection_url`` and ``config``);
    they are validated exactly as an update would validate them. Other
    connector types ignore overrides.
    """

    row = dict(ds)
    if not overrides or not is_workspace_ssh_connector(row):
        return row
    if "connection_url" in overrides and row.get("type") == "repository":
        row["connection_url"] = overrides.get("connection_url")
    if "config" in overrides:
        row["config"] = overrides.get("config") or {}
    row["config"] = validate_workspace_ssh_connector(
        str(row.get("type")),
        connection_url=row.get("connection_url"),
        config=stored_json_object(row.get("config")),
        credentials=stored_json_object(row.get("credentials")),
        check_key=False,
    )
    return row


async def probe_workspace_ssh_connector(ds: Mapping[str, Any]) -> dict[str, Any] | None:
    """Test an SSH connector's endpoint: reach it and report its host key.

    ``None`` means there is no endpoint to test (an ``ssh_key`` without a
    host). ``details.host_key`` is a host-qualified ``known_hosts`` line
    (``host type key`` or ``[host]:port type key``), the form the connector's
    pin is stored in, so a pin can never silently follow a host change. A
    pinned connector whose host now presents another key fails, as its
    workspace clone would.
    """

    import base64
    import hashlib

    if (
        ds.get("type") == "ssh_key"
        and not str(stored_json_object(ds.get("config")).get("host") or "").strip()
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
            "SSH host key probe failed for connector %s", ds.get("id"), exc_info=True
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
        "host_key": f"{known_hosts_host_field(identity.host, identity.port)} {host_key}",
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
    "UNAVAILABLE_REASONS",
    "WORKSPACE_SSH_KNOWN_HOSTS_ENV",
    "WorkspaceSshConnectorError",
    "WorkspaceSshIdentity",
    "apply_ssh_test_overrides",
    "default_workspace_ssh_known_hosts",
    "fetch_ssh_host_key",
    "is_workspace_ssh_connector",
    "known_hosts_host_field",
    "probe_workspace_ssh_connector",
    "repository_uses_ssh_key",
    "ssh_key_connector_private_key",
    "validate_workspace_ssh_connector",
    "workspace_ssh_authority_id",
    "workspace_ssh_descriptor",
    "workspace_ssh_identity",
]
