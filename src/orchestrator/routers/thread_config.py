"""Live session configuration and workspace-tier upgrade routes.

Extracted from ``orchestrator.main`` (R1.B06 lane B). Five route declarations —
four internal agent-facing (``X-Internal-Key``; ingress strips those paths) and
one owner-facing — moved with their handler names, paths, methods, parameter
order and docstrings intact; a sixth internal one, ``upgrade-availability``,
was added later (2026-10-07). The docstring is the published OpenAPI
description, so it is part of the route's identity.

None of the five declarations carried ``tags``, ``response_model``,
``status_code`` or a ``dependencies`` list, and none acquires one here. Each
guard (``require_internal`` / ``require_thread_owner``) still runs inside the
operation, in its original position, so the refusal order is unchanged.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

from orchestrator.schemas.thread_config import (
    AgentThreadConfigUpdateRequest,
    ThreadConfigPatchRequest,
    ThreadWorkspaceUpgradeRequest,
)
from orchestrator.services import thread_config_update

# No `tags=` and no prefix: the declarations this replaces carried neither.
router = APIRouter()


def get_thread_config_dependencies(
    request: Request,
) -> thread_config_update.ThreadConfigUpdateDependencies:
    """Resolve collaborators only from the application handling this request."""
    return request.app.state.thread_config_dependencies_factory()


@router.patch("/api/agents/threads/{thread_id}/config")
async def agent_update_thread_config(
    request: Request,
    thread_id: str,
    body: AgentThreadConfigUpdateRequest,
) -> dict[str, Any]:
    """Persist runtime config changes for a thread. **Internal** (P4b) —
    requires ``X-Internal-Key``. Ingress strips this path.

    Deep-merges the provided config_override into the existing
    ``threads.metadata.config_override``. Ordered permission/narration
    scalars are rejected here and must use the durable control inbox.

    Returns the enriched ``config_override`` so the agent can rebuild its
    LLM with the resolved ``base_url``/``api_key`` instead of sending the
    next request to api.openai.com with ``not-needed``.
    """
    # The gate runs here, in the declaration's own body:
    # `scripts/check_endpoint_auth.py` reads the audited gate from the route
    # it is declared on and does not follow a call into a service module, so a
    # handler that only delegates is reported `unscoped` and rewrites
    # `policy/endpoint_inventory.txt`.
    await get_thread_config_dependencies(request).require_internal(request)
    return await thread_config_update.agent_update_thread_config(
        request,
        thread_id,
        body,
        dependencies=get_thread_config_dependencies(request),
    )


@router.post("/api/agents/threads/{thread_id}/upgrade-to-vm")
async def agent_upgrade_thread_to_vm(
    request: Request, thread_id: str
) -> dict[str, Any]:
    """Request VM provisioning for a persistent thread. **Internal** (P4b) —
    requires ``X-Internal-Key``. Ingress strips this path.

    Called by the persistent agent when a sudo command is detected and the
    user approves a VM upgrade via WebSocket.
    """
    # The gate runs here, in the declaration's own body:
    # `scripts/check_endpoint_auth.py` reads the audited gate from the route
    # it is declared on and does not follow a call into a service module, so a
    # handler that only delegates is reported `unscoped` and rewrites
    # `policy/endpoint_inventory.txt`.
    await get_thread_config_dependencies(request).require_internal(request)
    return await thread_config_update.agent_upgrade_thread_to_vm(
        request,
        thread_id,
        dependencies=get_thread_config_dependencies(request),
    )


@router.post("/api/agents/threads/{thread_id}/abort-vm-upgrade")
async def agent_abort_thread_vm_upgrade(
    request: Request, thread_id: str
) -> dict[str, Any]:
    """Tear down a thread's VM after a failed/timed-out live upgrade.
    **Internal** (P4b) — requires ``X-Internal-Key``. Ingress strips this path.

    Called by the persistent agent when ``_poll_vm_ready`` gives up: a cold CDI
    registry import can outrun the poll budget, leaving a half-provisioned VM +
    DataVolume + importer pod with nobody attached. This deletes the VM and
    marks ``metadata.vm.status='aborted'`` so the provisioning-in-progress guard
    (``status in provisioning/created/ready``) doesn't wedge a later retry
    (workspace_tier_upgrade.md Q7). Idempotent — safe to call when no VM exists.
    """
    # The gate runs here, in the declaration's own body:
    # `scripts/check_endpoint_auth.py` reads the audited gate from the route
    # it is declared on and does not follow a call into a service module, so a
    # handler that only delegates is reported `unscoped` and rewrites
    # `policy/endpoint_inventory.txt`.
    await get_thread_config_dependencies(request).require_internal(request)
    return await thread_config_update.agent_abort_thread_vm_upgrade(
        request,
        thread_id,
        dependencies=get_thread_config_dependencies(request),
    )


@router.post("/api/agents/threads/{thread_id}/upgrade-to-workspace")
async def agent_upgrade_thread_to_workspace(
    request: Request,
    thread_id: str,
    body: ThreadWorkspaceUpgradeRequest | None = None,
) -> dict[str, Any]:
    """Provision a real workspace container for a lite (``virtual``/``none``)
    thread, upgrading it to the ``sandbox`` tier. **Internal** (P4b) — requires
    ``X-Internal-Key``. Ingress strips this path.

    The session-side counterpart to the live ``swap_backend()`` hot-swap
    (workspace_tier_upgrade.md §4.2 S2): the agent calls this when a ``virtual``
    session needs a real environment (the user starts coding / the agent
    requests an upgrade), then polls ``/workspace`` for readiness via
    ``_poll_workspace_ready`` and swaps its backend in place — the conversation
    never drops. Idempotent: a second call while a container is already
    provisioning/ready is a no-op.

    ``vm`` targets (workspace_tier_upgrade.md Phase 2) are delegated to the
    operator-gated VM path (``/upgrade-to-vm``): same grant gate, but provisions
    a KubeVirt VM and records ``metadata.vm``. The agent polls vm readiness and
    hot-swaps in place just like the container tier.
    """
    # The gate runs here, in the declaration's own body:
    # `scripts/check_endpoint_auth.py` reads the audited gate from the route
    # it is declared on and does not follow a call into a service module, so a
    # handler that only delegates is reported `unscoped` and rewrites
    # `policy/endpoint_inventory.txt`.
    await get_thread_config_dependencies(request).require_internal(request)
    return await thread_config_update.agent_upgrade_thread_to_workspace(
        request,
        thread_id,
        body,
        dependencies=get_thread_config_dependencies(request),
    )


@router.get("/api/agents/threads/{thread_id}/upgrade-availability")
async def agent_thread_upgrade_availability(
    request: Request, thread_id: str
) -> dict[str, Any]:
    """Whether a workspace upgrade would be accepted now. **Internal** (P4b) —
    requires ``X-Internal-Key``. Ingress strips this path.

    Read-only: runs the VM upgrade's pre-effect refusals (lane, protected
    Session, grants and the VM kill switch, the VM provisioner, an upgrade
    already in progress, the tier rule) and provisions nothing. The pinned
    agent asks before it offers an upgrade.
    """
    # The gate runs here, in the declaration's own body:
    # `scripts/check_endpoint_auth.py` reads the audited gate from the route
    # it is declared on and does not follow a call into a service module, so a
    # handler that only delegates is reported `unscoped` and rewrites
    # `policy/endpoint_inventory.txt`.
    await get_thread_config_dependencies(request).require_internal(request)
    return await thread_config_update.agent_thread_upgrade_availability(
        request,
        thread_id,
        dependencies=get_thread_config_dependencies(request),
    )


@router.patch("/api/persistent/threads/{thread_id}/config")
async def update_thread_config(
    thread_id: str, body: ThreadConfigPatchRequest, request: Request
) -> dict[str, Any]:
    """Edit a DISCONNECTED session's config (auth: owner only).

    Slice C of live_session_settings.md. Runs the exact validate → datasource-
    authorize → grant-check → merge core the live (internal) PATCH uses —
    authorization stays keyed to the THREAD OWNER, so an API caller can't
    exceed what the live settings pane allows. Changes take effect at the next
    attach: every attach path re-resolves ``metadata.config_override`` +
    ``datasource_ids`` and injects credentials in-flight, so no enrichment
    round-trip to an agent is needed.

    Refuses PINNED threads currently bound to an agent (409) — there is no
    orchestrator→agent config-push channel, so an edit here would silently go
    stale on the running session until its next attach; connected pinned
    sessions edit through the settings pane's ``config.update`` frame instead.
    Ended threads are editable (they resume via POST .../resume → fresh attach).

    STATELESS threads bind no agent, so this endpoint IS their live path: the
    Cockpit routes the pane's ``config.update`` here whenever ``/connection``
    declares ``controls["config.update"] == "rest"``. The change is persisted
    at admission and picked up by the next claim, whose attach fingerprint
    changes with the config (turn_executor.attach_fingerprint) — the same
    turn-boundary semantics the pane's transcript stamp already promises. A
    turn already in flight keeps the config it started with. ``effective``
    on the response says which: ``next_turn`` (stateless) or ``next_attach``.

    The response ``config_override`` is the REDACTED accepted fragment — the
    internal endpoint intentionally returns plaintext transport secrets to the
    agent; that shape must never reach a browser-facing route.
    """
    # The gate runs here, in the declaration's own body:
    # `scripts/check_endpoint_auth.py` reads the audited gate from the route
    # it is declared on and does not follow a call into a service module, so a
    # handler that only delegates is reported `unscoped` and rewrites
    # `policy/endpoint_inventory.txt`.
    dependencies = get_thread_config_dependencies(request)
    owner = await dependencies.require_thread_owner(
        request, dependencies.store, thread_id
    )
    return await thread_config_update.update_thread_config(
        thread_id,
        body,
        request,
        dependencies=dependencies,
        owner=owner,
    )


__all__ = [
    "agent_abort_thread_vm_upgrade",
    "agent_update_thread_config",
    "agent_upgrade_thread_to_vm",
    "agent_upgrade_thread_to_workspace",
    "get_thread_config_dependencies",
    "router",
    "update_thread_config",
]
