"""Datasource payload construction for an agent bundle (worker and session).

Extracted verbatim from ``orchestrator.main`` (R1.B05 lane J, census group
``J_assignment``). These four helpers are the *only* place a resolved connector
row is turned into something an agent receives, and both preparation paths use
them: the worker bundle (``_build_job_start_request``) and the session attach
payload (``_assemble_session_attach_payload`` / ``_resolve_thread_datasources``,
root-owned). That shared use is why they live in one module rather than being
duplicated per lane — the two boundaries previously disagreed about read-write
managed connectors, and a second copy is how that recurs.

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

from orchestrator.services.datasource_config import (
    normalize_kb_config as _normalize_kb_config,
)
from orchestrator.services.email_datasource import email_dispatch_config
from orchestrator.services.workspace_ssh_connector import workspace_ssh_descriptor
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


def mcp_datasource_runtime_allowed(
    datasource: dict[str, Any],
    *,
    dependencies: DatasourcePayloadDependencies,
) -> bool:
    """Apply deployment gates to a resolved datasource without exposing secrets."""
    if datasource.get("type") != "mcp":
        return True
    if not dependencies.mcp_datasources_enabled():
        return False
    credentials = datasource.get("credentials") or {}
    if isinstance(credentials, str):
        try:
            credentials = json.loads(credentials)
        except (json.JSONDecodeError, ValueError):
            credentials = {}
    transport = (
        credentials.get("transport", "http")
        if isinstance(credentials, dict)
        else "http"
    )
    return str(transport).lower() != "stdio" or dependencies.mcp_stdio_enabled()


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
    # config.access, clamped by project_read_only) — no extra handling here.
    enabled_datasources = [
        datasource
        for datasource in datasources
        if mcp_datasource_runtime_allowed(datasource, dependencies=dependencies)
    ]
    tools_override.update(datasource_tool_categories(enabled_datasources))
    override["tools"] = tools_override
    return override


def apply_cloud_storage_override(
    resolved_ds: list[dict[str, Any]], job_context: dict[str, Any]
) -> None:
    """Apply job-level cloud_storage_read_only override to WebDAV datasources.

    If the job's context contains cloud_storage_read_only, it overrides the
    project-level read_only setting on any webdav datasource in the resolved list.
    Mutates resolved_ds in place.
    """
    override = job_context.get("cloud_storage_read_only")
    if override is None:
        return
    for ds in resolved_ds:
        if ds["type"] == "webdav":
            ds["project_read_only"] = bool(override)


def build_datasources_payload(
    resolved_ds: list[dict[str, Any]],
    *,
    dependencies: DatasourcePayloadDependencies,
) -> list[dict[str, Any]] | None:
    """Build the datasources payload for sending to the agent.

    Strips internal fields (id, job_id, created_at, updated_at) that the
    agent doesn't need. For read-only managed connectors, credentials are
    withheld (tools hold them internally).

    Args:
        resolved_ds: List of resolved datasource dicts from the database

    Returns:
        List of datasource dicts for the agent, or None if empty
    """
    if not resolved_ds:
        return None

    managed_types = {"postgresql", "neo4j", "mongodb", "webdav", "email"}
    payload = []
    email_forwarded = False
    for ds in resolved_ds:
        creds = ds.get("credentials") or {}
        if isinstance(creds, str):
            import json as json_module

            try:
                creds = json_module.loads(creds)
            except (json.JSONDecodeError, ValueError):
                creds = {}

        is_read_only = ds.get("project_read_only", False)
        ds_type = ds["type"]
        if not mcp_datasource_runtime_allowed(ds, dependencies=dependencies):
            continue

        # Read-only managed connectors: withhold credentials (tools hold them).
        # Email is exempt — its tools need a live IMAP login at every tier;
        # read-only is expressed as the access floor on entry['config'] instead.
        if ds_type in managed_types and is_read_only and ds_type != "email":
            creds = {}

        # External OKF KBs are centrally indexed and read-only in Slice 4 v1.
        # The agent needs the stable index id + display metadata, never the
        # remote URL or repository credentials.
        if ds_type == "kb":
            creds = {}
            is_read_only = True

        # SSH keys never ride ``datasources``: that list becomes job metadata
        # and graph state. The key travels once, in the hidden
        # ``workspace_ssh_identities`` field, into a workspace ssh-agent; the
        # entry keeps only the non-secret alias it is reached through.
        ssh_identity = workspace_ssh_descriptor(ds)
        if ds_type == "repository" and ssh_identity is not None:
            creds = {key: value for key, value in creds.items() if key != "ssh_key"}
        if ds_type == "ssh_key":
            creds = {
                **creds,
                "files": [
                    {key: value for key, value in item.items() if key != "contents"}
                    for item in creds.get("files") or []
                    if isinstance(item, dict)
                ],
            }

        entry = {
            "type": ds_type,
            "name": ds["name"],
            "description": ds.get("description"),
            "connection_url": None if ds_type == "kb" else ds.get("connection_url"),
            "credentials": creds,
            "project_read_only": is_read_only,
        }
        if ds_type == "kb":
            entry["datasource_id"] = str(ds["id"])
            # stored=True keeps the native-project marker in the payload: the
            # agent's binding builder needs it to collapse a project's own KB
            # row into the writable native binding instead of adding a second,
            # read-only binding for the same notes.
            entry["config"] = _normalize_kb_config(ds.get("config"), stored=True)
        if ds_type == "repository":
            # Repository identity is server-owned runtime authority.  Keep the
            # raw database ``id`` out of the payload, but carry its exact value
            # under the dedicated internal key consumed by the clone/tool
            # binding.  ``resolved_ds`` comes from the authorization query;
            # callers and models never select this field.
            datasource_id = ds.get("id")
            if datasource_id is not None:
                entry["datasource_id"] = str(datasource_id)
            # The clone reads config["forge"] to resolve the forge API base;
            # without it every repository records forge="" and repo_open_pr
            # can never be used. _datasource_row_to_dict already parsed the
            # JSONB, so this is a real dict. No secrets live in config —
            # credentials travel in `creds`.
            entry["config"] = ds.get("config") or {}
        if ds_type == "email":
            # v1: one mailbox per job/session — the agent keys connections by
            # type, so a second email datasource would silently shadow the
            # first (knowledge-base/knowledge/features/email_datasource.md, open questions).
            if email_forwarded:
                dependencies.logger.warning(
                    "Skipping additional email datasource %r: only one email "
                    "datasource per job/session is supported",
                    ds.get("name"),
                )
                continue
            email_forwarded = True
            entry["config"] = email_dispatch_config(
                ds.get("config"),
                project_read_only=bool(is_read_only),
                owner_can_autonomous_send=bool(
                    ds.get("_owner_can_autonomous_send", False)
                ),
            )
        if ds.get("cli_hint"):
            entry["cli_hint"] = ds["cli_hint"]
        if ds.get("default_branch"):
            entry["default_branch"] = ds["default_branch"]
        if ds_type == "repository" and ds.get("require_default_branch") is True:
            entry["require_default_branch"] = True
        if ssh_identity is not None:
            entry["ssh_identity"] = ssh_identity

        payload.append(entry)

    return payload or None
