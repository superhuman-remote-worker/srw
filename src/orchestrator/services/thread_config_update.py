"""Live session configuration edits and live workspace-tier upgrades.

Extracted verbatim from ``orchestrator.main`` (R1.B06 lane B, census group
``S_CONFIG``). Five route operations share one commit core and one strict
protected-cloud marker; this module owns both so the internal agent PATCH and
the owner-facing PATCH cannot drift apart.

The properties that make these a credential and provisioning boundary move
unchanged:

* **The strict marker never coerces.** ``protected_cloud_mutation_marker``
  deliberately does *not* use ``thread_metadata_object``'s legacy best-effort
  parse: corrupt JSON or list metadata would otherwise become an ordinary row
  and bypass the protected fixed-runtime contract. Malformed is
  ``409 protected_cloud_malformed``, and ``require_unprotected_workspace_upgrade``
  turns a live marker into ``409 protected_cloud_workspace_fixed`` *before* any
  provisioning effect.
* **The commit is serialized and generation-fenced.**
  :func:`apply_thread_config_update` takes
  ``thread_configuration_transaction`` and re-reads the row ``FOR UPDATE``, so a
  mixed before/after selection can never be rendered; a managed runtime must
  additionally present the exact snapshot generation or get
  ``409``. Ordered permission/narration scalars are refused here and must use
  the durable control inbox.
* **Every upgrade authorizes before it provisions.** Both upgrade routes run
  ``enforce_workspace_upgrade_grants`` (the ``vm_workspaces`` kill switch,
  per-user ``can_use_vm`` and the ``vm_workspace`` PDP grant) ahead of any
  effect, and the VM path re-reads under the advisory lock so a concurrent End
  wins cleanly.
* **An unproven VM teardown is a 503, not an "aborted" stamp.** An
  accepted/absent control-plane response is not process-zero for a partitioned
  guest; marking it aborted would hide a credential-capable VM from the
  lifecycle owner and from migration 0189.

``apply_thread_config_update_locked`` stays in the application module: the
manifest lane owns it, so it reaches this module as an injected callable rather
than a re-implementation (port contract §P2/§P5). The late imports inside the
function bodies are kept where they were written, because tests steer those
paths by patching the attribute on the owning module.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Literal, Protocol
from uuid import UUID

from fastapi import HTTPException, Request

from orchestrator.database.postgres import (
    DatasourceMaterializationAuthorizationError,
)
from orchestrator.schemas.thread_config import (
    AgentThreadConfigUpdateRequest,
    ThreadConfigPatchRequest,
    ThreadWorkspaceUpgradeRequest,
)
from orchestrator.security.access import redact_config_override
from orchestrator.services.manifest_runtime_ownership import require_srw_runtime
from orchestrator.services.vm_workspace_config import vm_provisioning_options
from orchestrator.services.session_runtime_admission import (
    protected_cloud_marker_state,
    thread_runtime_authority,
)
from orchestrator.services.vm_workspace_recovery_store import (
    acquire_vm_cleanup_permit,
    completed_cleanup_outcome,
    complete_vm_cleanup_permit,
)
from shared.run_queue import LANE_PINNED
from shared.runtime.core.loader import canonical_config_name

logger = logging.getLogger(__name__)


class ThreadConfigStore(Protocol):
    async def get_thread(self, thread_id: str) -> dict[str, Any] | None: ...

    def thread_configuration_transaction(self, thread_id: str) -> Any: ...

    def thread_advisory_lock(self, thread_id: str) -> Any: ...

    async def refresh_session_execution(
        self, thread_id: str, *, conn: Any, config_override: dict[str, Any]
    ) -> dict[str, Any]: ...

    async def merge_thread_vm_context(
        self, thread_id: str, patch: dict[str, Any]
    ) -> Any: ...


@dataclass(frozen=True)
class ThreadConfigUpdateDependencies:
    """Per-invocation collaborators for session config and upgrade routes.

    ``store`` is main's ``postgres_db``; ``vm_provisioner`` and
    ``container_provisioner`` are its provisioner singletons. The four
    callables are owned elsewhere and reach this module only through here:

    * ``apply_thread_config_update_locked`` — the manifest lane's commit core,
      called under the configuration transaction this module opens;
    * ``enforce_workspace_upgrade_grants`` — B05 grant enforcement, bound to
      main's dependency object;
    * ``require_internal`` / ``require_thread_owner`` — the two auth guards, so
      a test can steer them without reaching into the security package.
    """

    store: ThreadConfigStore
    vm_provisioner: Any
    container_provisioner: Any
    recovery_store: Any
    apply_thread_config_update_locked: Callable[..., Awaitable[Any]]
    enforce_workspace_upgrade_grants: Callable[..., Awaitable[Any]]
    require_internal: Callable[[Request], Awaitable[Any]]
    require_thread_owner: Callable[..., Awaitable[Any]]


def config_change_summary(
    config_override: dict[str, Any], datasource_ids: list[str] | None
) -> str:
    """One-line audit summary of a config change: dotted KEY paths only.

    Values are deliberately omitted — the fragment can carry transport
    secrets after enrichment, and the security_events table must never
    hold credential material.
    """
    keys: list[str] = []
    for k, v in sorted(config_override.items()):
        if isinstance(v, dict) and v:
            keys.extend(f"{k}.{sub}" for sub in sorted(v))
        else:
            keys.append(k)
    parts = []
    if keys:
        parts.append("keys=" + ",".join(keys))
    if datasource_ids is not None:
        parts.append(f"datasource_ids={len(datasource_ids)}")
    return " ".join(parts) or "empty"


async def apply_thread_config_update(
    thread_id: str,
    thread_row: dict[str, Any] | None,
    config_override: dict[str, Any],
    datasource_ids: list[str] | None,
    *,
    request: Request,
    actor: dict[str, Any] | None,
    managed_runtime: bool = False,
    snapshot_patch_protocol: int | None = None,
    snapshot_generation: int | None = None,
    dependencies: ThreadConfigUpdateDependencies,
) -> tuple[dict[str, Any], list[str] | None]:
    """Commit accepted settings, connector selection and one spec generation."""
    if thread_row is not None:
        require_srw_runtime(thread_row)
    from shared.runtime.core.session_config_patch import validate_session_settings_patch

    try:
        validate_session_settings_patch(config_override)
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc
    interactive = config_override.get("interactive")
    if isinstance(interactive, dict) and {
        "permission_mode",
        "narration_mode",
    }.intersection(interactive):
        raise HTTPException(
            409,
            "permission_mode and narration_mode must use the ordered session control endpoint",
        )
    async with dependencies.store.thread_configuration_transaction(thread_id) as conn:
        # Serialize config updates; never render a mixed before/after selection.
        current = await conn.fetchrow(
            "SELECT * FROM threads WHERE id=$1 FOR UPDATE", UUID(str(thread_id))
        )
        if current is None:
            raise HTTPException(404, "Thread not found")
        if managed_runtime:
            from orchestrator.services.manifest_execution_snapshot import read_execution

            execution = await read_execution(conn, "Session", thread_id)
            if execution is not None and (
                snapshot_patch_protocol != 1
                or snapshot_generation != execution["generation"]
            ):
                raise HTTPException(
                    409,
                    "Session configuration changed; reattach to load its current generation before editing settings.",
                )
        result = await dependencies.apply_thread_config_update_locked(
            thread_id,
            dict(current),
            config_override,
            datasource_ids,
            request=request,
            actor=actor,
        )
        try:
            saved = await dependencies.store.refresh_session_execution(
                thread_id, conn=conn, config_override=result[0]
            )
        except DatasourceMaterializationAuthorizationError as exc:
            raise HTTPException(403, str(exc)) from exc
        return saved["delivery_override"], result[1]


def protected_cloud_mutation_marker(
    thread: dict[str, Any] | None,
) -> Literal["off", "on"]:
    """Strict protected marker for live runtime mutations.

    Runtime upgrade/config endpoints are credential and provisioning
    boundaries.  They must not use ``thread_metadata_object``'s legacy
    best-effort coercion: corrupt JSON/list metadata could otherwise become an
    ordinary row and bypass the protected fixed-runtime contract.
    """

    if thread is None:
        return "off"
    raw_metadata = thread.get("metadata")
    if raw_metadata is None:
        metadata: Any = {}
    elif isinstance(raw_metadata, str):
        try:
            metadata = json.loads(raw_metadata)
        except (json.JSONDecodeError, TypeError) as exc:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "protected_cloud_malformed",
                    "message": "Protected cloud session state is invalid.",
                },
            ) from exc
    else:
        metadata = raw_metadata
    marker = protected_cloud_marker_state(metadata)
    if marker == "malformed":
        raise HTTPException(
            status_code=409,
            detail={
                "code": "protected_cloud_malformed",
                "message": "Protected cloud session state is invalid.",
            },
        )
    return marker


def require_unprotected_workspace_upgrade(
    thread: dict[str, Any],
) -> dict[str, Any]:
    """Refuse every protected/malformed live workspace upgrade pre-effect."""

    marker = protected_cloud_mutation_marker(thread)
    if marker == "on":
        raise HTTPException(
            status_code=409,
            detail={
                "code": "protected_cloud_workspace_fixed",
                "message": (
                    "Protected cloud sessions cannot upgrade or replace their "
                    "Container workspace."
                ),
            },
        )
    raw_metadata = thread.get("metadata")
    if raw_metadata is None:
        return {}
    if isinstance(raw_metadata, str):
        parsed = json.loads(raw_metadata)
        # The strict marker helper above has already proved this shape.
        assert isinstance(parsed, dict)
        return parsed
    assert isinstance(raw_metadata, dict)
    return raw_metadata


async def agent_update_thread_config(
    request: Request,
    thread_id: str,
    body: AgentThreadConfigUpdateRequest,
    *,
    dependencies: ThreadConfigUpdateDependencies,
) -> dict[str, Any]:
    """Body of ``PATCH /api/agents/threads/{thread_id}/config``."""
    await dependencies.require_internal(request)
    try:
        thread_row = await dependencies.store.get_thread(thread_id)
        config_override, selected_ds_ids = await apply_thread_config_update(
            thread_id,
            thread_row,
            dict(body.config_override or {}),
            body.datasource_ids,
            request=request,
            actor=None,
            managed_runtime=True,
            snapshot_patch_protocol=body.snapshot_patch_protocol,
            snapshot_generation=body.snapshot_generation,
            dependencies=dependencies,
        )
        return {
            "status": "updated",
            "config_override": config_override,
            "datasource_ids": selected_ds_ids,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def agent_upgrade_thread_to_vm(
    request: Request,
    thread_id: str,
    *,
    dependencies: ThreadConfigUpdateDependencies,
) -> dict[str, Any]:
    """Body of ``POST /api/agents/threads/{thread_id}/upgrade-to-vm``."""
    await dependencies.require_internal(request)
    vm_provisioner = dependencies.vm_provisioner
    thread = await dependencies.store.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")

    if thread.get("execution_lane") != LANE_PINNED:
        raise HTTPException(
            status_code=409,
            detail="Workspace upgrades are not yet supported on the stateless lane",
        )

    metadata = require_unprotected_workspace_upgrade(thread)

    # Sec-1 — authorize BEFORE provisioning (fail-closed). This endpoint is the
    # target of both the sandbox→VM sudo path and the lite→vm delegation from
    # /upgrade-to-workspace; it previously ran ungated. The shared gate enforces
    # the global vm_workspaces kill-switch + per-user can_use_vm + the
    # vm_workspace PDP grant (workspace_tier_upgrade.md §4.4 Sec-1 / Phase 2).
    await dependencies.enforce_workspace_upgrade_grants(thread, target_tier="vm")

    if not vm_provisioner.is_available:
        raise HTTPException(
            status_code=503,
            detail="VM provisioning not available (no NATS or K8s)",
        )

    # The capability read above is advisory.  Serialize with End/protected
    # lifecycle work, then re-read and install the provision generation under
    # the exact current T/G/actor tuple before dispatch.  If End won after the
    # route read, the DB transition returns False and no VM request is sent.
    async with dependencies.store.thread_advisory_lock(thread_id):
        thread = await dependencies.store.get_thread(thread_id)
        if not thread:
            raise HTTPException(status_code=404, detail="Thread not found")
        metadata = require_unprotected_workspace_upgrade(thread)
        runtime_authority = thread_runtime_authority(thread)
        if runtime_authority is None:
            raise HTTPException(
                status_code=409,
                detail={"code": "pinned_runtime_identity_mismatch"},
            )
        raw_vm_ctx = metadata.get("vm")
        if raw_vm_ctx is not None and not isinstance(raw_vm_ctx, Mapping):
            raise HTTPException(status_code=409, detail="VM authority is malformed")
        vm_ctx = dict(raw_vm_ctx) if raw_vm_ctx is not None else None
        if (vm_ctx or {}).get("status") in (
            "provisioning",
            "created",
            "starting",
            "ssh_pending",
            "ready",
            "waiting_golden",
            "waiting_capacity",
            "waiting_headscale",
            "waiting_preparation",
        ):
            return {
                "status": vm_ctx["status"],
                "thread_id": thread_id,
                "message": "VM already provisioned or in progress",
            }

        options = await vm_provisioning_options(
            dependencies.store,
            "Session",
            thread,
            fallback=metadata.get("config_override"),
        )
        ok = await vm_provisioner.create_thread_vm(
            thread_id=thread_id,
            **options,
            agent_config=canonical_config_name(
                thread.get("config_name", "session_base")
            ),
            expected_runtime_generation=runtime_authority.generation,
            expected_agent_id=(
                str(thread["agent_id"]) if thread.get("agent_id") is not None else None
            ),
            expected_attach_token=(
                str(thread["runtime_attach_token"])
                if thread.get("runtime_attach_token") is not None
                else None
            ),
            expected_vm_context=vm_ctx,
        )
    if not ok:
        raise HTTPException(
            status_code=409,
            detail={"code": "vm_provision_authority_changed"},
        )

    return {
        "status": "provisioning",
        "thread_id": thread_id,
        "vm_provisioner_mode": vm_provisioner.mode,
    }


async def agent_abort_thread_vm_upgrade(
    request: Request,
    thread_id: str,
    *,
    dependencies: ThreadConfigUpdateDependencies,
) -> dict[str, Any]:
    """Body of ``POST /api/agents/threads/{thread_id}/abort-vm-upgrade``."""
    await dependencies.require_internal(request)
    vm_provisioner = dependencies.vm_provisioner
    thread = await dependencies.store.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")

    deleted = False
    if vm_provisioner.lifecycle_available:
        try:
            identity = await vm_provisioner.capture_vm_teardown_identity(
                thread_id, entity_type="thread"
            )
            cleanup = await acquire_vm_cleanup_permit(
                dependencies.recovery_store,
                owner_kind="thread",
                owner_id=thread_id,
                identity=identity,
                source="abort_thread_vm_upgrade",
                purge_disk=True,
            )
            if not cleanup.allowed:
                raise RuntimeError("VM cleanup held for workspace recovery")
            disposition = completed_cleanup_outcome(cleanup)
            if disposition is None:
                outcome = await vm_provisioner.release_vm_captured(
                    thread_id,
                    identity,
                    entity_type="thread",
                    purge_disk=True,
                    capture_snapshot=False,
                )
                disposition = outcome.disposition
                if disposition in {"completed", "identity_superseded"}:
                    await complete_vm_cleanup_permit(
                        dependencies.recovery_store,
                        cleanup,
                        outcome=disposition,
                    )
            deleted = disposition == "completed"
        except Exception as e:
            logger.warning(
                "abort-vm-upgrade: delete_thread_vm failed for %s: %s", thread_id, e
            )
    if not deleted:
        # An accepted/absent control-plane response is not process-zero for a
        # partitioned guest. Preserve the exact generation and retry handle;
        # marking it aborted would hide a potentially credential-capable VM
        # from both the lifecycle owner and migration 0189.
        raise HTTPException(
            status_code=503,
            detail={
                "code": "vm_process_zero_unproven",
                "retryable": True,
            },
        )
    await dependencies.store.merge_thread_vm_context(thread_id, {"status": "aborted"})
    return {"status": "aborted", "thread_id": thread_id, "vm_deleted": deleted}


async def agent_upgrade_thread_to_workspace(
    request: Request,
    thread_id: str,
    body: ThreadWorkspaceUpgradeRequest | None = None,
    *,
    dependencies: ThreadConfigUpdateDependencies,
) -> dict[str, Any]:
    """Body of ``POST /api/agents/threads/{thread_id}/upgrade-to-workspace``."""
    await dependencies.require_internal(request)
    container_provisioner = dependencies.container_provisioner
    target_tier = (body.target_tier if body else "sandbox") or "sandbox"
    if target_tier not in ("sandbox", "vm"):
        raise HTTPException(
            status_code=400,
            detail=(
                f"upgrade-to-workspace supports target_tier 'sandbox' or 'vm'; "
                f"got {target_tier!r}"
            ),
        )

    # vm targets reuse the operator-gated VM provisioning path: it runs the same
    # enforce_workspace_upgrade_grants gate, provisions the VM, and records
    # metadata.vm. The agent then polls vm readiness and hot-swaps in place
    # exactly like the sandbox path — the swap handler (_handle_workspace_upgrade)
    # is tier-agnostic and sets sudo_action="allow" for a vm backend
    # (workspace_tier_upgrade.md Phase 2). Keeping a single client method +
    # endpoint means the agent stays uniform across tiers.
    if target_tier == "vm":
        return await agent_upgrade_thread_to_vm(
            request, thread_id, dependencies=dependencies
        )

    thread = await dependencies.store.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")

    if thread.get("execution_lane") != LANE_PINNED:
        raise HTTPException(
            status_code=409,
            detail="Workspace upgrades are not yet supported on the stateless lane",
        )

    metadata = require_unprotected_workspace_upgrade(thread)

    # Sec-1 — authorize the upgrade against the owner's capability grants BEFORE
    # provisioning (fail-closed). sandbox passes by default; a shell-restricted
    # owner (or a vm target without vm_workspace) is refused with 403.
    await dependencies.enforce_workspace_upgrade_grants(thread, target_tier=target_tier)

    if not (container_provisioner.is_available and container_provisioner.in_cluster):
        raise HTTPException(
            status_code=503,
            detail="Workspace container provisioning not available (no in-cluster K8s)",
        )

    # Idempotency: short-circuit if a container is already in flight or ready.
    wc = metadata.get("workspace_container") or {}
    if wc.get("status") in ("pending", "creating", "created", "ready"):
        return {
            "status": wc["status"],
            "thread_id": thread_id,
            "target_tier": "sandbox",
            "message": "Workspace container already provisioned or in progress",
        }

    # The background owner installs the exact T/G/actor provision intent under
    # the lifecycle lock before its first Kubernetes effect.  Do not publish a
    # generic pending marker here: a stale route read must lose cleanly to End.
    asyncio.create_task(container_provisioner.create_pinned_thread_workspace(thread_id))

    return {
        "status": "provisioning",
        "thread_id": thread_id,
        "target_tier": "sandbox",
    }


async def update_thread_config(
    thread_id: str,
    body: ThreadConfigPatchRequest,
    request: Request,
    *,
    dependencies: ThreadConfigUpdateDependencies,
    owner: tuple[Any, Any] | None = None,
) -> dict[str, Any]:
    """Body of ``PATCH /api/persistent/threads/{thread_id}/config``.

    ``owner`` is the ``(user, thread)`` pair the route already proved. The route
    has to run the gate in its own body — ``scripts/check_endpoint_auth.py``
    reads the audited gate from the declaration and does not follow a call into
    a service — so accepting the result keeps this a single ownership read.
    """
    if owner is None:
        owner = await dependencies.require_thread_owner(
            request, dependencies.store, thread_id
        )
    user, thread = owner
    # Connected = an agent is bound AND the thread is in a live state. A
    # suspended/ended thread can carry a stale agent_id from a crash path
    # (drain-suspend clears it, a hard pod kill may not) — no live agent
    # serves those states, so they stay editable.
    if thread.get("agent_id") and thread.get("status") not in ("suspended", "ended"):
        raise HTTPException(
            status_code=409,
            detail=(
                "Session is connected to an agent — change settings from the "
                "session's settings pane; a server-side edit would not reach "
                "the running session until its next attach."
            ),
        )
    if not body.config_override and body.datasource_ids is None:
        raise HTTPException(status_code=400, detail="No changes provided")
    config_override, selected_ds_ids = await apply_thread_config_update(
        thread_id,
        thread,
        dict(body.config_override or {}),
        body.datasource_ids,
        request=request,
        actor=user,
        dependencies=dependencies,
    )
    return {
        "status": "updated",
        "config_override": redact_config_override(config_override),
        "datasource_ids": selected_ds_ids,
        "effective": (
            "next_turn"
            if thread.get("execution_lane") == "stateless"
            else "next_attach"
        ),
    }


__all__ = [
    "ThreadConfigStore",
    "ThreadConfigUpdateDependencies",
    "agent_abort_thread_vm_upgrade",
    "agent_update_thread_config",
    "agent_upgrade_thread_to_vm",
    "agent_upgrade_thread_to_workspace",
    "apply_thread_config_update",
    "config_change_summary",
    "protected_cloud_mutation_marker",
    "require_unprotected_workspace_upgrade",
    "update_thread_config",
]
