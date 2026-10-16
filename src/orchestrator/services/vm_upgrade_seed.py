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

from orchestrator.services.workspace_tier_policy import thread_workspace_backend
from shared.workspace_upgrade import VM_UPGRADE_SEED_PENDING, WORKSPACE_UPGRADE_VERB

REQUEST_UNKNOWN = {"code": "workspace_upgrade_request_unknown"}
SOURCE_UNSUPPORTED = {"code": "workspace_upgrade_source_unsupported"}

_REQUEST_SQL = """
SELECT verb, outcome FROM thread_control_requests
WHERE id = $1::uuid AND thread_id = $2::uuid
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


__all__ = ["REQUEST_UNKNOWN", "SOURCE_UNSUPPORTED", "vm_upgrade_seed_marker"]
