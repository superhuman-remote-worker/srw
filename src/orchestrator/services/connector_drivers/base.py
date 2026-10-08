"""The control-plane half of a connector driver.

The orchestrator asks a driver of a stored connector type four things:

* ``validate`` — normalize a create or update request, raising the API's own
  400/403 ``HTTPException`` (the detail strings are the contract);
* ``check`` — Test connection: a ``{"status", "message", ...}`` report that
  never raises for a target failure;
* ``effective_access`` — the access level a resolved row binds at;
* ``bind`` — the payload entry an agent receives for one resolved row, or
  ``None`` to send nothing.  Built-in drivers return today's wire entry,
  byte for byte; the agent builds binding descriptors from it.

``revoke`` retires what a binding delivered; nothing a built-in driver
delivers outlives the execution, so it is a no-op for all of them.
``resource_driver`` and ``credential_config`` describe a stored row as its
manifest Connector resource (``orchestrator.services.manifest_connectors``):
the driver name it stores and which credential fields are not secret.  Optional
capabilities are protocols checked with ``isinstance``, as in
``services/cloud/base.py``.

The generic-hosting drivers (``srw.env/v1``, ``srw.files/v1``) have no stored
row and bind an inline manifest connector instead; they are
:class:`ManifestDeliveryDriver`.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable
from uuid import uuid4

from fastapi import HTTPException

from orchestrator.services import datasource_config
from orchestrator.services.connector_drivers import knowledge_note
from shared.connectors.binding import BindingDescriptor
from shared.connectors.contract import DriverSpec
from shared.connectors.envelope import unsupported_check

logger = logging.getLogger(__name__)

#: The one config refusal every driver without a config shares.
NO_CONFIG_DETAIL = (
    "Connector config is only supported for OKF Knowledge Bases and email connectors"
)
#: A credential file's non-secret fields; ``contents`` is the secret.
_FILE_TARGET_KEYS = ("name", "target_path", "mode", "env_var")


@dataclass(frozen=True)
class DeploymentGates:
    """The installation switches a driver's ``deployment_gate`` names.

    Callables, read per use, so a test or the application that steers the
    gate on its own module is observed here too.
    """

    mcp_datasources_enabled: Callable[[], bool]
    mcp_stdio_enabled: Callable[[], bool]

    def enabled(self, gate: str) -> bool:
        if gate == "mcp_datasources":
            return bool(self.mcp_datasources_enabled())
        raise ValueError(f"Unknown deployment gate {gate!r}")


@dataclass(frozen=True)
class DriverEnvironment:
    """What connector writes and Test consult: the gates and the validators
    the application injects (tests replace the MCP validator)."""

    gates: DeploymentGates
    validate_mcp_datasource: Callable[[str | None, dict[str, Any]], None]


@dataclass(frozen=True)
class ConnectorDraft:
    """A create or update request, free of the HTTP schema.

    ``None`` means the field was not supplied; ``supplied`` names the fields
    the caller sent, so an explicit ``null`` can be told from an omission.
    """

    name: str | None
    connection_url: str | None
    credentials: dict[str, Any] | None
    config: dict[str, Any] | None
    read_only: bool | None
    is_global: bool | None
    default_branch: str | None
    supplied: frozenset[str] = frozenset()

    @classmethod
    def from_body(cls, body: Any) -> ConnectorDraft:
        return cls(
            name=body.name,
            connection_url=body.connection_url,
            credentials=body.credentials,
            config=body.config,
            read_only=body.read_only,
            is_global=body.is_global,
            default_branch=body.default_branch,
            supplied=frozenset(body.model_fields_set),
        )


@dataclass(frozen=True)
class ValidationContext:
    environment: DriverEnvironment
    #: The caller's ``email_autonomous_send`` grant, resolved only when asked.
    can_autonomous_send: Callable[[], Awaitable[bool]]


@dataclass(frozen=True)
class NormalizedConnector:
    """What a create or update stores.  ``None`` leaves a stored value alone.

    ``connection_url_set`` clears a stored URL to ``None`` (an MCP stdio
    server has none); ``reindex_required`` asks the KB driver's write effect
    for a full rebuild.
    """

    connection_url: str | None
    config: dict[str, Any] | None
    credentials: dict[str, Any] | None
    connection_url_set: bool = False
    reindex_required: bool = False


@dataclass(frozen=True)
class CheckContext:
    environment: DriverEnvironment


@dataclass(frozen=True)
class BindContext:
    gates: DeploymentGates
    logger: logging.Logger
    #: The deployment's default SSH host-key pins (known_hosts text).
    default_known_hosts: str


def auth_method_config(credentials: Mapping[str, Any]) -> dict[str, Any]:
    """A repository's stored ``auth_method``, the one non-secret field of its
    credentials (repository and knowledge-base drivers)."""
    method = credentials.get("auth_method")
    return {"auth_method": method} if isinstance(method, str) and method else {}


def probe_failure(message: str, ds_type: str) -> dict[str, Any]:
    """Report a failed connectivity probe without disclosing the exception.

    A driver's exception text routinely carries the connection URL (with its
    password), internal hostnames and ports, so it is logged rather than
    returned. ``error_ref`` — the same 12-hex-char shape the request-id
    middleware uses — is what an operator quotes to find that log line.
    """
    error_ref = uuid4().hex[:12]
    logger.exception(
        "Connectivity probe failed for a %s connector (error_ref=%s)",
        ds_type,
        error_ref,
    )
    return {"status": "error", "message": message, "error_ref": error_ref}


_FROM_ROW: Any = object()


def payload_entry(
    row: Mapping[str, Any],
    *,
    credentials: Any,
    read_only: Any,
    connection_url: Any = _FROM_ROW,
    fields: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Today's agent payload entry for one resolved row.

    Key order is part of the wire format: the common keys, then the driver's
    own ``fields``, then ``cli_hint`` and ``default_branch`` when set.
    ``connection_url`` defaults to the row's.
    """
    entry: dict[str, Any] = {
        "type": row["type"],
        "name": row["name"],
        "description": row.get("description"),
        "connection_url": (
            row.get("connection_url") if connection_url is _FROM_ROW else connection_url
        ),
        "credentials": credentials,
        "project_read_only": read_only,
    }
    entry.update(fields or {})
    if row.get("cli_hint"):
        entry["cli_hint"] = row["cli_hint"]
    if row.get("default_branch"):
        entry["default_branch"] = row["default_branch"]
    return entry


class DatasourceDriver:
    """A driver for one stored ``datasources.type``.

    The defaults describe a connector with no config, no Test, and a payload
    entry that forwards the row as stored.
    """

    spec: DriverSpec

    def __init__(self, spec: DriverSpec) -> None:
        if not spec.legacy_type:
            raise ValueError(f"{spec.name} serves no datasource type")
        self.spec = spec

    @property
    def type_id(self) -> str:
        return self.spec.legacy_type or ""

    def disabled_detail(self) -> str:
        return f"{self.spec.title} connectors are disabled on this deployment"

    def require_enabled(self, gates: DeploymentGates) -> None:
        """403 when the driver's deployment gate is off."""
        gate = self.spec.deployment_gate
        if gate and not gates.enabled(gate):
            raise HTTPException(status_code=403, detail=self.disabled_detail())

    async def prevalidate(
        self, draft: ConnectorDraft, environment: DriverEnvironment
    ) -> None:
        """Checks a create runs before the caller is authenticated."""

    async def validate(
        self,
        draft: ConnectorDraft,
        *,
        existing: Mapping[str, Any] | None,
        ctx: ValidationContext,
    ) -> NormalizedConnector:
        """Normalize a create (``existing is None``) or an update.

        A create refuses a config before it reads the credentials; an update
        reads the credentials first.  Drivers keep that order so a request
        with two faults reports the same one as before the drivers existed.
        """
        if existing is None:
            config = self.no_config(draft, existing)
            credentials = self.stored_credentials(draft, existing)
        else:
            credentials = self.stored_credentials(draft, existing)
            config = self.no_config(draft, existing)
        return NormalizedConnector(
            connection_url=draft.connection_url,
            config=config,
            credentials=credentials,
        )

    async def check(
        self, row: Mapping[str, Any], credentials: dict[str, Any], *, ctx: CheckContext
    ) -> dict[str, Any]:
        return unsupported_check(
            f"{self.spec.title} connectors have no connection test"
        )

    def effective_access(self, row: Mapping[str, Any]) -> str | None:
        """``ReadOnly`` on a read-only project link, else ``ReadWrite``."""
        return "ReadOnly" if row.get("project_read_only") else "ReadWrite"

    def runtime_allowed(self, row: Mapping[str, Any], gates: DeploymentGates) -> bool:
        """Whether this installation may hand the row to an agent at all."""
        return True

    def bind(
        self, row: Mapping[str, Any], credentials: Any, *, ctx: BindContext
    ) -> dict[str, Any] | None:
        return payload_entry(
            row,
            credentials=credentials,
            read_only=row.get("project_read_only", False),
        )

    async def revoke(self, binding: Mapping[str, Any], *, ctx: BindContext) -> None:
        """Nothing a built-in driver delivers outlives its execution."""

    def knowledge_note(self, row: Mapping[str, Any]) -> str:
        """The project knowledge note that describes a stored connector.

        Never a credential value. The default names the connector only.
        """
        return knowledge_note.bare_note(row)

    def retrieval_messages(self, row: Mapping[str, Any]) -> list[str]:
        """The phrases the connector's knowledge note is retrieved by."""
        return knowledge_note.connection_phrases(row)

    # -- the Connector resource of a stored row (slice D3a) ----------------

    def resource_driver(self, credentials: Mapping[str, Any]) -> str:
        """The ``driver`` the row's Connector resource names."""
        return self.spec.name

    def credential_config(self, credentials: Mapping[str, Any]) -> dict[str, Any]:
        """The non-secret parts of stored credentials, as Connector config.

        Whatever this leaves out counts as secret: it stays on the row, and
        slice D3b moves it into the Connector's resource secret.  The default
        keeps nothing.
        """
        return {}

    # -- helpers shared by the built-in drivers -----------------------------

    @staticmethod
    def credential_file_targets(credentials: Mapping[str, Any]) -> dict[str, Any]:
        """Where each stored credential file lands, without its contents."""
        files = credentials.get("files")
        if not isinstance(files, list):
            return {}
        return {
            "files": [
                {
                    key: entry[key]
                    for key in _FILE_TARGET_KEYS
                    if isinstance(entry.get(key), str)
                }
                for entry in files
                if isinstance(entry, Mapping)
            ]
        }

    @staticmethod
    def no_config(
        draft: ConnectorDraft, existing: Mapping[str, Any] | None
    ) -> dict[str, Any] | None:
        """Refuse a config; a create stores ``{}``, an update leaves it alone."""
        if draft.config:
            raise HTTPException(status_code=400, detail=NO_CONFIG_DETAIL)
        return dict(draft.config or {}) if existing is None else draft.config

    @staticmethod
    def stored_credentials(
        draft: ConnectorDraft, existing: Mapping[str, Any] | None
    ) -> dict[str, Any] | None:
        """The credentials to store; on an update a blank value keeps them.

        The cockpit's edit form sends an empty dict when the user did not
        re-enter a secret; storing it would clobber the secret.
        """
        credentials = datasource_config.normalize_datasource_credentials(
            draft.credentials
        )
        if existing is None:
            return credentials
        return credentials if credentials else None


class ManifestDeliveryDriver:
    """A generic-hosting driver: binds an inline manifest connector."""

    spec: DriverSpec

    def __init__(self, spec: DriverSpec) -> None:
        self.spec = spec

    def validate(self, connector: Mapping[str, Any]) -> None:
        """Refuse a connector this driver cannot deliver (``HTTPException`` 422)."""
        raise NotImplementedError

    async def bind(
        self,
        connector: Mapping[str, Any],
        *,
        alias: str,
        resolve_credential: Callable[[Mapping[str, Any]], Awaitable[str]],
    ) -> BindingDescriptor:
        """The connector's entries; ``resolve_credential`` reads a SecretRef."""
        raise NotImplementedError

    def effective_access(self, connector: Mapping[str, Any]) -> str | None:
        """Env and file delivery cannot enforce an access level."""
        return None

    async def check(self, connector: Mapping[str, Any]) -> dict[str, Any]:
        return unsupported_check(
            f"{self.spec.title} connectors have no connection test"
        )

    async def revoke(self, binding: BindingDescriptor) -> None:
        """The pod's Secret goes with the pod."""


# =============================================================================
# Optional capabilities
# =============================================================================


@runtime_checkable
class SupportsWriteEffects(Protocol):
    """Work that follows a stored create or update (the KB's reindex)."""

    async def after_write(
        self,
        datasource_id: str,
        normalized: NormalizedConnector,
        *,
        created: bool,
        knowledge_index: Any,
    ) -> None: ...


@runtime_checkable
class SupportsTestOverrides(Protocol):
    """Test connection of an endpoint a connector form is still editing."""

    def apply_test_overrides(
        self, row: Mapping[str, Any], overrides: Mapping[str, Any]
    ) -> dict[str, Any]:
        """The row Test probes; ``HTTPException(400)`` for an invalid edit."""
        ...


@runtime_checkable
class SupportsWorkspaceSshIdentity(Protocol):
    """A connector whose key is loaded into the workspace's ssh-agent."""

    def workspace_ssh_identity(
        self, row: Mapping[str, Any], *, default_known_hosts: str
    ) -> Any:
        """The identity for ``workspace_ssh_identities``, or ``None``."""
        ...


@runtime_checkable
class SupportsCredentialLease(Protocol):
    """A driver whose upstream credential SRW hands out through a lease.

    The lease exchange (slice C2) calls this after it has authorized a
    driver identity and a live lease of the connector; the agent never sees
    what it returns.
    """

    def lease_upstream(self, row: Mapping[str, Any]) -> dict[str, Any]:
        """``{"credential": str, "allowed_upstream": [str, ...]}`` for a
        decrypted connector row; ``ValueError`` when it holds none."""
        ...


@runtime_checkable
class SupportsIndexOperations(Protocol):
    """A connector SRW indexes: delete through the index fence, status, reindex."""

    async def delete_with_index(
        self,
        datasource_id: str,
        *,
        authority_project_scope_id: str | None,
        deleted_by: str,
        knowledge_index: Any,
    ) -> bool: ...

    async def index_status(
        self, row: Mapping[str, Any], datasource_id: str, *, vector_db: Any
    ) -> dict[str, Any]: ...

    async def reindex(
        self, row: Mapping[str, Any], *, full: bool, knowledge_index: Any
    ) -> dict[str, Any]: ...
