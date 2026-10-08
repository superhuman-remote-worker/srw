"""Datasource payload construction for an agent bundle (worker and session).

Extracted verbatim from ``orchestrator.main`` (R1.B05 lane J, census group
``J_assignment``). These four helpers are the *only* place a resolved connector
row is turned into something an agent receives, and both preparation paths use
them: the worker bundle (``_build_job_start_request``) and the session attach
payload (``_assemble_session_attach_payload`` / ``_resolve_thread_datasources``,
root-owned). That shared use is why they live in one module rather than being
duplicated per lane — the two boundaries previously disagreed about read-write
managed connectors, and a second copy is how that recurs.

What one row becomes is its connector driver's ``bind``
(``orchestrator.services.connector_drivers``); this module walks the rows,
applies the drivers' deployment gates and per-execution limits, and keeps
the wire format.

What is deliberately NOT here: connector authorization. ``datasource_policy``
remains the authority for whether a selection may be attached at all, and the
job-side reauthorization lives in ``job_datasource_selection``. This module
assumes it was handed an already-authorized, exactly-resolved set and only
decides what of it crosses the wire.

Deployment gates (``MCP_DATASOURCES_ENABLED`` / ``MCP_STDIO_ENABLED``) arrive as
injected callables rather than being read here, so a test that steers the gate
on the application module still steers this module.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Callable

from orchestrator.services.connector_credential_leases import lease_spec
from orchestrator.services.connector_drivers import ConnectorDriverRegistry
from orchestrator.services.connector_drivers.base import (
    BindContext,
    DeploymentGates,
    SupportsWorkspaceSshIdentity,
    payload_entry,
)
from orchestrator.services.connector_drivers.workspace_ssh import (
    WorkspaceSshConnectorError,
)
from shared.datasource_policy import datasource_tool_categories


@dataclass(frozen=True)
class DatasourcePayloadDependencies:
    """Per-invocation collaborators for the agent datasource payload.

    ``mcp_datasources_enabled`` / ``mcp_stdio_enabled`` are callables, not
    booleans: the deployment gates they read are evaluated per call by the
    application, so a monkeypatched gate is observed here too.
    """

    logger: logging.Logger
    mcp_datasources_enabled: Callable[[], bool]
    mcp_stdio_enabled: Callable[[], bool]
    #: The application's installed connector drivers.
    connector_drivers: ConnectorDriverRegistry
    #: The deployment's default SSH host-key pins, as known_hosts
    #: text, for SSH connectors without a pin of their own. Both the
    #: ``ssh_identity`` descriptor and the delivered identity read it
    #: here, so they always agree.
    workspace_ssh_known_hosts: Callable[[], str]

    def deployment_gates(self) -> DeploymentGates:
        return DeploymentGates(
            mcp_datasources_enabled=self.mcp_datasources_enabled,
            mcp_stdio_enabled=self.mcp_stdio_enabled,
        )


def forwarded_datasources(
    datasources: list[dict[str, Any]] | None,
    *,
    dependencies: DatasourcePayloadDependencies,
    log_skipped: bool = False,
) -> list[dict[str, Any]]:
    """The resolved rows an agent receives, in order.

    A row whose driver's deployment gate is off is left out, and a driver
    that allows only so many connectors per execution (email: one, because
    the agent keys connections by type) keeps the first ones. Both the
    payload and the tool categories are built from this, so a tool tier is
    only granted for a connector that is delivered. A stored type no driver
    serves is forwarded as stored.
    """
    gates = dependencies.deployment_gates()
    forwarded: list[dict[str, Any]] = []
    bound_per_driver: dict[str, int] = {}
    for ds in datasources or []:
        driver = dependencies.connector_drivers.for_type(ds.get("type"))
        if driver is not None:
            if not driver.runtime_allowed(ds, gates):
                continue
            limit = driver.spec.max_per_execution
            if limit is not None:
                bound = bound_per_driver.get(driver.spec.name, 0)
                if bound >= limit:
                    if log_skipped:
                        ds_type = ds["type"]
                        dependencies.logger.warning(
                            "Skipping additional %s datasource %r: only %s %s "
                            "datasource per job/session is supported",
                            ds_type,
                            ds.get("name"),
                            "one" if limit == 1 else limit,
                            ds_type,
                        )
                    continue
                bound_per_driver[driver.spec.name] = bound + 1
        forwarded.append(ds)
    return forwarded


def build_datasource_tool_override(
    datasources: list[dict[str, Any]],
    config_override: dict[str, Any] | None,
    *,
    dependencies: DatasourcePayloadDependencies,
) -> dict[str, Any]:
    """Inject/strip database tool categories based on attached datasources.

    For each known datasource type, if a datasource is attached, the corresponding
    tool category is injected. If not attached, the category is set to an empty list.
    This ensures the agent only has database tools for databases that are actually
    connected.

    Args:
        datasources: List of resolved datasource dicts (from resolve_datasources_for_job)
        config_override: Existing config override dict (may be None)

    Returns:
        Updated config override dict with tool categories adjusted
    """
    override = dict(config_override or {})
    tools_override = dict(override.get("tools", {}))
    # Shared single source of truth with the agent's session attach path
    # (they previously disagreed on read-write managed connectors). Email is
    # tier-keyed inside the shared map (EMAIL_TIER_TOOLS keyed by
    # config.access, clamped by project_read_only). The categories come from
    # the rows the agent actually receives, so a second mailbox the payload
    # leaves out cannot raise the email tier of the one it forwards.
    forwarded = forwarded_datasources(datasources, dependencies=dependencies)
    tools_override.update(datasource_tool_categories(forwarded))
    override["tools"] = tools_override
    return override


def apply_cloud_storage_override(
    resolved_ds: list[dict[str, Any]], job_context: dict[str, Any]
) -> None:
    """Apply a job's cloud_storage_read_only to its WebDAV datasources.

    The key is the job creator's, unchecked at admission, so it can only
    tighten: a WebDAV connector is read-only when its project link is or the
    job asks for it. A job cannot lift a read-only link the project owner set.
    Mutates resolved_ds in place.
    """
    override = job_context.get("cloud_storage_read_only")
    if override is None:
        return
    for ds in resolved_ds:
        if ds["type"] == "webdav":
            ds["project_read_only"] = bool(ds.get("project_read_only")) or bool(
                override
            )


def build_datasources_payload(
    resolved_ds: list[dict[str, Any]],
    *,
    dependencies: DatasourcePayloadDependencies,
) -> list[dict[str, Any]] | None:
    """Build the datasources payload for sending to the agent.

    Each forwarded row's driver (:func:`forwarded_datasources`) binds it to
    the entry the agent receives (or to nothing); internal fields such as the
    row's id, job and timestamps stay behind.  Rows over a driver's
    per-execution limit are skipped with a warning.

    Args:
        resolved_ds: List of resolved datasource dicts from the database

    Returns:
        List of datasource dicts for the agent, or None if empty
    """
    if not resolved_ds:
        return None

    gates = dependencies.deployment_gates()
    ctx = BindContext(
        gates=gates,
        logger=dependencies.logger,
        default_known_hosts=dependencies.workspace_ssh_known_hosts(),
    )
    payload = []
    for ds in forwarded_datasources(
        resolved_ds, dependencies=dependencies, log_skipped=True
    ):
        creds = ds.get("credentials") or {}
        if isinstance(creds, str):
            try:
                creds = json.loads(creds)
            except (json.JSONDecodeError, ValueError):
                creds = {}

        driver = dependencies.connector_drivers.for_type(ds["type"])
        if driver is None:
            # A stored type no driver serves is forwarded as stored, except
            # the upstream secret of a lease driver (connector drivers C2),
            # which never reaches an agent, installed or not.
            if lease_spec(ds) is not None:
                creds = {}
            payload.append(
                payload_entry(
                    ds,
                    credentials=creds,
                    read_only=ds.get("project_read_only", False),
                )
            )
            continue
        entry = driver.bind(ds, creds, ctx=ctx)
        if entry is not None:
            if lease_spec(entry) is not None:
                # A lease driver's entry never carries its upstream secret,
                # whatever its bind returned; the lease step fills it.
                entry["credentials"] = {}
            payload.append(entry)

    return payload or None


def build_workspace_ssh_identities(
    resolved_ds: list[dict[str, Any]] | None,
    *,
    dependencies: DatasourcePayloadDependencies,
) -> list[dict[str, Any]] | None:
    """The hidden ``workspace_ssh_identities`` field for one delivery.

    Built only from an already-authorized, exactly-resolved connector set,
    like :func:`build_datasources_payload`: each row's driver says whether it
    holds a key for the workspace's ssh-agent. A row that cannot be delivered
    is logged by id and fixed reason code and left out; it never fails the
    delivery. ``None`` when nothing is delivered, so the field is absent from
    the wire.
    """
    default_known_hosts = dependencies.workspace_ssh_known_hosts()
    identities: list[dict[str, Any]] = []
    for ds in resolved_ds or []:
        driver = dependencies.connector_drivers.for_type(ds.get("type"))
        if not isinstance(driver, SupportsWorkspaceSshIdentity):
            continue
        try:
            identity = driver.workspace_ssh_identity(
                ds, default_known_hosts=default_known_hosts
            )
        except WorkspaceSshConnectorError as exc:
            dependencies.logger.warning(
                "SSH connector %s cannot be delivered to a workspace (%s)",
                ds.get("id"),
                exc.code,
            )
            continue
        if identity is not None:
            identities.append(identity.to_payload())
    return identities or None
