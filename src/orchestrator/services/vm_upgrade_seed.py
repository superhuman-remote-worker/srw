"""The durable seed marker of a stateless Session's VM upgrade (C2, D4 (a)).

``agent_upgrade_thread_to_vm`` records ``metadata.vm.upgrade_seed`` in the
pre-CAS merge that already records the upgrade's template:
``{control_request_id, state: "pending", source}`` when the call names the
lane switch's control request, ``None`` for every other call, so a later
WebSocket retry overwrites a stale marker. The pinned agent marks it
``seeded`` after a verified copy and before the swap; a new agent that
attaches the VM while it is still ``pending`` copies the old prefix first
(the internal routes below).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from uuid import UUID

from fastapi import HTTPException

from orchestrator.services import stateless_workspace_upgrade
from orchestrator.services.workspace_tier_policy import (
    LiteWorkspaceConfigError,
    thread_workspace_backend,
)
from shared.workspace_upgrade import (
    VM_UPGRADE_SEED_PENDING,
    VM_UPGRADE_SEED_SEEDED,
    WORKSPACE_UPGRADE_SEED_VERSION,
    WORKSPACE_UPGRADE_VERB,
    metadata_object,
    vm_provision_generation,
    vm_upgrade_seed,
)

REQUEST_UNKNOWN = {"code": "workspace_upgrade_request_unknown"}
SOURCE_UNSUPPORTED = {"code": "workspace_upgrade_source_unsupported"}
PINNED_IDENTITY_MISMATCH = {"code": "pinned_runtime_identity_mismatch"}
NO_PENDING_UPGRADE_SEED = {"code": "no_pending_upgrade_seed"}
UPGRADE_SEED_SOURCE_CHANGED = {"code": "upgrade_seed_source_changed"}
UPGRADE_SEED_FENCE_LOST = {"code": "upgrade_seed_fence_lost"}

_IDENTITY_HEADERS = (
    ("X-Agent-ID", "agent_id"),
    ("X-Session-Runtime-Generation", "runtime_generation"),
    ("X-Session-Runtime-Attach-Token", "runtime_attach_token"),
)

_REQUEST_SQL = """
SELECT verb, outcome FROM thread_control_requests
WHERE id = $1::uuid AND thread_id = $2::uuid
"""

# Same fence as merge_thread_vm_context_if_provision_generation (provision
# generation, no unresolved recovery) plus the marker's request id and the
# bound agent. ``seeded`` → ``seeded`` is an idempotent retry.
_MARK_SEEDED_SQL = """
UPDATE threads
   SET metadata = jsonb_set(metadata, '{vm,upgrade_seed,state}', '"seeded"'::jsonb, false),
       last_activity = now()
 WHERE id = $1::uuid
   AND execution_lane = 'pinned'
   AND runtime_retirement_token IS NULL
   AND agent_id = $4::uuid
   AND metadata->'vm'->>'provision_generation' = $2
   AND metadata->'vm'->'upgrade_seed'->>'control_request_id' = $3
   AND metadata->'vm'->'upgrade_seed'->>'state' IN ('pending', 'seeded')
   AND NOT EXISTS (
       SELECT 1 FROM vm_workspace_recoveries recovery
        WHERE recovery.owner_kind = 'thread'
          AND recovery.owner_id = threads.id
          AND recovery.resolved_at IS NULL)
RETURNING 1
"""


async def vm_upgrade_seed_marker(
    store: Any,
    thread: Mapping[str, Any],
    metadata: Mapping[str, Any],
    *,
    control_request_id: str | None,
) -> dict[str, Any] | None:
    """The marker this VM request records; None when no C2 request asked.

    The request must be a pending ``upgrade-to-workspace`` of this Session:
    the agent asks before it journals the request's receipt.
    """
    if control_request_id is None:
        return None
    try:
        request_id = str(UUID(str(control_request_id)))
        thread_id = str(UUID(str(thread["id"])))
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(status_code=409, detail=REQUEST_UNKNOWN) from exc
    row = await store.fetchrow(_REQUEST_SQL, request_id, thread_id)
    if (
        row is None
        or row["verb"] != WORKSPACE_UPGRADE_VERB
        or row["outcome"] is not None
    ):
        raise HTTPException(status_code=409, detail=REQUEST_UNKNOWN)
    backend = thread_workspace_backend(dict(thread)) or "virtual"
    if backend == "none":
        source = None
    elif backend == "virtual":
        binding = metadata.get("_workspace_binding")
        source = {
            "backend": "virtual",
            "prefix": f"threads/{thread_id}/",
            "binding": (
                dict(binding) if isinstance(binding, Mapping) and binding else None
            ),
        }
    else:
        raise HTTPException(status_code=409, detail=SOURCE_UNSUPPORTED)
    return {
        "control_request_id": request_id,
        "state": VM_UPGRADE_SEED_PENDING,
        "source": source,
    }


def require_pinned_runtime_identity(
    thread: Mapping[str, Any], headers: Mapping[str, str]
) -> str:
    """The presenting agent's id; 409 unless it is the exact bound runtime.

    ``seed-source`` hands out object-store credentials, so it binds delivery
    to the reciprocal identity the way the ``/workspace`` poll does. Values
    compare as canonical UUIDs; an absent or malformed one never matches.
    """
    try:
        presented = tuple(
            str(UUID(str(headers.get(header) or ""))) for header, _ in _IDENTITY_HEADERS
        )
        current = tuple(
            str(UUID(str(thread.get(column) or ""))) for _, column in _IDENTITY_HEADERS
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=PINNED_IDENTITY_MISMATCH) from exc
    if (
        thread.get("execution_lane") != "pinned"
        or thread.get("runtime_retirement_token") is not None
        or presented != current
    ):
        raise HTTPException(status_code=409, detail=PINNED_IDENTITY_MISMATCH)
    return presented[0]


async def mark_vm_upgrade_seeded(
    store: Any,
    thread_id: str,
    *,
    agent_id: str,
    provision_generation: str,
    control_request_id: str,
) -> bool:
    """CAS the marker to ``seeded`` for the exact provision; False if fenced out."""
    updated = await store.fetchval(
        _MARK_SEEDED_SQL,
        str(UUID(str(thread_id))),
        str(UUID(str(provision_generation))),
        str(UUID(str(control_request_id))),
        str(UUID(str(agent_id))),
    )
    return updated is not None


async def agent_vm_upgrade_seed_source(
    request: Any, thread_id: str, *, dependencies: Any
) -> dict[str, Any]:
    """Body of ``GET /api/agents/threads/{thread_id}/workspace-upgrade/seed-source``.

    Answers only while the marker is ``pending`` and names an
    ``upgrade-to-workspace`` request of this Session. The mount spec is C1's
    (the builder lite attaches use), so exposure doesn't grow; C1's builder
    also refuses a source bound to another object store, or none configured.
    """
    await dependencies.require_internal(request)
    thread = await dependencies.store.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    require_pinned_runtime_identity(thread, getattr(request, "headers", {}) or {})
    metadata = metadata_object(thread.get("metadata"))
    marker = vm_upgrade_seed(metadata)
    provision_generation = vm_provision_generation(metadata)
    if (
        marker is None
        or marker["state"] != VM_UPGRADE_SEED_PENDING
        or provision_generation is None
    ):
        raise HTTPException(status_code=409, detail=NO_PENDING_UPGRADE_SEED)
    row = await dependencies.store.fetchrow(
        _REQUEST_SQL, marker["control_request_id"], str(thread["id"])
    )
    if row is None or row["verb"] != WORKSPACE_UPGRADE_VERB:
        raise HTTPException(status_code=409, detail=NO_PENDING_UPGRADE_SEED)
    source = marker["source"]
    if source is None:
        spec: dict[str, Any] = {
            "version": WORKSPACE_UPGRADE_SEED_VERSION,
            "seed_from": None,
            "workspace": {"backend": "none"},
        }
    else:
        try:
            spec = stateless_workspace_upgrade.workspace_upgrade_seed_spec(
                str(thread["id"]), source
            )
        except (LiteWorkspaceConfigError, ValueError) as exc:
            raise HTTPException(
                status_code=409, detail=UPGRADE_SEED_SOURCE_CHANGED
            ) from exc
    return {
        **spec,
        "control_request_id": marker["control_request_id"],
        "provision_generation": provision_generation,
    }


async def agent_vm_upgrade_seeded(
    request: Any, thread_id: str, body: Any, *, dependencies: Any
) -> dict[str, str]:
    """Body of ``POST /api/agents/threads/{thread_id}/workspace-upgrade/seeded``."""
    await dependencies.require_internal(request)
    thread = await dependencies.store.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    agent_id = require_pinned_runtime_identity(
        thread, getattr(request, "headers", {}) or {}
    )
    if not await mark_vm_upgrade_seeded(
        dependencies.store,
        str(thread["id"]),
        agent_id=agent_id,
        provision_generation=str(body.provision_generation),
        control_request_id=str(body.control_request_id),
    ):
        raise HTTPException(status_code=409, detail=UPGRADE_SEED_FENCE_LOST)
    return {"state": VM_UPGRADE_SEED_SEEDED}


__all__ = [
    "NO_PENDING_UPGRADE_SEED",
    "PINNED_IDENTITY_MISMATCH",
    "REQUEST_UNKNOWN",
    "SOURCE_UNSUPPORTED",
    "UPGRADE_SEED_FENCE_LOST",
    "UPGRADE_SEED_SOURCE_CHANGED",
    "agent_vm_upgrade_seed_source",
    "agent_vm_upgrade_seeded",
    "mark_vm_upgrade_seeded",
    "require_pinned_runtime_identity",
    "vm_upgrade_seed_marker",
]
