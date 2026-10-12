"""Exact pinned-runtime forwarding for owner session input (R1.B10).

A pinned session's input and legacy interrupt reach one exact agent Pod. The
recipient is never a recyclable coordinate: every field of the target comes
from one reciprocal ``PinnedSessionBinding`` snapshot, and that binding is
re-read and compared before transport setup and again after client entry, so
no stale target receives an effect because it was authoritative earlier.

Stateless callers branch before this module; :func:`load_thread_for_owner`
applies the same owner gate without the pinned resolution side effects.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import httpx
from fastapi import HTTPException

from orchestrator.services.session_runtime_admission import (
    ThreadRuntimeAuthority,
    pinned_binding_invalid_detail,
    protected_cloud_marker_state,
    thread_runtime_authority,
    thread_runtime_refusal_detail,
)
from orchestrator.services.session_runtime_identity import thread_accepts_runtime
from orchestrator.services.stateless_workspace_gate import thread_metadata_object
from shared.pinned_session_identity import PinnedSessionBinding

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PinnedForwardingDependencies:
    """Application-owned collaborators for exact pinned forwarding."""

    store: Any
    workspace_suspension: Any
    protected_cloud_delivery_state: Callable[
        [dict[str, Any], dict[str, Any]], Awaitable[tuple[str, str | None]]
    ]


async def load_thread_for_owner(thread_id: str, user: dict, *, store: Any) -> dict:
    """Load a thread under the same owner gate ``resolve_thread_for_forwarding``
    applies (404 unknown; fail-closed 403 for orphans and non-owners; admin
    bypass) — WITHOUT its agent-resolution / workspace-restore side effects.

    Used by the stateless-lane branches: queue-lane threads have no bound
    agent, so the forwarding resolver's 503 would mask the lane entirely.
    """
    thread = await store.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    if not user.get("is_admin") and str(thread.get("user_id") or "") != str(user["id"]):
        raise HTTPException(status_code=403, detail="Not your thread")
    return thread


async def resolve_thread_for_forwarding(
    thread_id: str,
    user: dict,
    *,
    dependencies: PinnedForwardingDependencies,
) -> tuple[dict, PinnedSessionBinding]:
    """Resolve one owner-visible thread and its exact pinned runtime binding.

    Stateless callers branch before this helper.  All agent/endpoint fields in
    the result come from one reciprocal DB snapshot rather than independent
    thread and agent reads.  A suspended pinned workspace is restored before
    that final snapshot.
    """
    store = dependencies.store
    thread = await store.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    # Fail-closed for orphans (user_id IS NULL); admins bypass.
    if not user.get("is_admin") and str(thread.get("user_id") or "") != str(user["id"]):
        raise HTTPException(status_code=403, detail="Not your thread")
    if not thread_accepts_runtime(thread):
        raise HTTPException(
            status_code=409, detail=thread_runtime_refusal_detail(thread)
        )
    if thread.get("execution_lane") != "pinned":
        raise HTTPException(
            status_code=409,
            detail="Thread execution lane does not support direct forwarding",
        )

    async def _refresh_runtime_authority() -> dict[str, Any]:
        current = await store.get_thread(thread_id)
        if not thread_accepts_runtime(current):
            raise HTTPException(
                status_code=409, detail=thread_runtime_refusal_detail(current)
            )
        if not user.get("is_admin") and str(current.get("user_id") or "") != str(
            user["id"]
        ):
            raise HTTPException(status_code=403, detail="Not your thread")
        if current.get("execution_lane") != "pinned":
            raise HTTPException(
                status_code=409,
                detail="Thread execution lane does not support direct forwarding",
            )
        marker = protected_cloud_marker_state(thread_metadata_object(current))
        if marker == "malformed":
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "protected_cloud_malformed",
                    "message": "Protected cloud session state is invalid.",
                },
            )
        if marker == "on":
            state, code = await dependencies.protected_cloud_delivery_state(
                current, thread_metadata_object(current)
            )
            if state != "ready":
                raise HTTPException(
                    status_code=425,
                    detail={
                        "code": "protected_cloud_not_ready",
                        "state": state,
                        "reason": code,
                    },
                )
        return current

    thread = await _refresh_runtime_authority()

    # Restore suspended workspace before forwarding (mirrors persistent_ws_proxy)
    metadata = thread.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (json.JSONDecodeError, TypeError):
            metadata = {}
    ws_ctx = metadata.get("workspace_container") or {}
    suspension = dependencies.workspace_suspension
    if ws_ctx.get("status") == "suspended" and suspension.is_enabled:
        logger.info("Restoring suspended workspace for thread %s", thread_id)
        ok = await suspension.restore_thread_workspace(thread_id)
        if not ok:
            raise HTTPException(
                status_code=503,
                detail="Failed to restore suspended workspace",
            )
        thread = await _refresh_runtime_authority()

    runtime_authority = thread_runtime_authority(thread)
    if runtime_authority is None:  # _refresh_runtime_authority proves this
        raise HTTPException(
            status_code=409, detail=thread_runtime_refusal_detail(thread)
        )
    binding = await store.get_pinned_session_binding(
        thread_id,
        expected_runtime_generation=runtime_authority.generation,
    )
    if binding is None:
        raise HTTPException(
            status_code=409,
            detail=pinned_binding_invalid_detail(runtime_authority),
        )
    require_forwardable_pinned_binding(binding)
    return thread, binding


def require_forwardable_pinned_binding(binding: PinnedSessionBinding) -> None:
    """Require a currently live agent status without freezing status equality."""

    if binding.agent_status not in {"ready", "working", "session"}:
        raise HTTPException(status_code=425, detail="session not ready")


def binding_runtime_authority(
    binding: PinnedSessionBinding,
) -> ThreadRuntimeAuthority:
    return ThreadRuntimeAuthority(
        thread_id=binding.thread_id,
        generation=binding.runtime_generation,
    )


async def revalidate_pinned_forwarding_binding(
    binding: PinnedSessionBinding, *, store: Any
) -> PinnedSessionBinding:
    """Re-read and compare every immutable DB/routing coordinate."""

    current = await store.get_pinned_session_binding(
        binding.thread_id,
        expected_runtime_generation=binding.runtime_generation,
    )
    if current is None or current.target_key != binding.target_key:
        raise HTTPException(
            status_code=409,
            detail=pinned_binding_invalid_detail(binding_runtime_authority(binding)),
        )
    require_forwardable_pinned_binding(current)
    return current


async def forward_to_agent(
    binding: PinnedSessionBinding,
    path: str,
    payload: dict,
    timeout: float = 30.0,
    *,
    store: Any,
) -> dict[str, Any]:
    """POST to one exact pinned Pod after a client-boundary DB reread."""

    identity_fingerprint = binding.session_identity_fingerprint
    forwarded_payload = dict(payload)
    supplied_fingerprint = forwarded_payload.get("session_identity_fingerprint")
    if supplied_fingerprint not in (None, identity_fingerprint):
        raise ValueError("forwarded session identity does not match its binding")
    forwarded_payload["session_identity_fingerprint"] = identity_fingerprint
    agent_url = f"http://{binding.pod_ip}:{binding.pod_port}{path}"
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            # Client/pool entry may await.  Re-read after it so no stale target
            # receives an effect merely because it was authoritative before
            # transport setup.  The endpoint validates the fingerprint again
            # across the final network race.
            await revalidate_pinned_forwarding_binding(binding, store=store)
            response = await client.post(agent_url, json=forwarded_payload)
    except HTTPException:
        raise
    except Exception as e:
        logger.warning(
            "Agent forward failed: %s %s -> %s",
            path,
            binding.agent_id,
            e,
        )
        raise HTTPException(status_code=503, detail=f"Agent unreachable: {e}") from e
    try:
        response_body = response.json()
    except Exception:
        response_body = None
    if (
        response.status_code == 409
        and isinstance(response_body, dict)
        and response_body.get("error") == "session_identity_mismatch"
    ):
        raise HTTPException(
            status_code=409,
            detail=pinned_binding_invalid_detail(binding_runtime_authority(binding)),
        )
    if response.status_code == 503:
        if (
            isinstance(response_body, dict)
            and response_body.get("error") == "runtime_terminating"
        ):
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "runtime_terminating",
                    "retryable": True,
                    "message": "The runtime is terminating; retry on its replacement.",
                },
                headers={"Retry-After": response.headers.get("Retry-After", "5")},
            )
    if response.status_code >= 500:
        raise HTTPException(
            status_code=502,
            detail=f"Agent error: {response.status_code} {response.text[:200]}",
        )
    if response.status_code >= 400:
        raise HTTPException(
            status_code=response.status_code,
            detail=response.text[:200],
        )
    return (
        response_body
        if isinstance(response_body, dict)
        else {"raw": response.text[:500]}
    )


__all__ = [
    "PinnedForwardingDependencies",
    "binding_runtime_authority",
    "forward_to_agent",
    "load_thread_for_owner",
    "require_forwardable_pinned_binding",
    "resolve_thread_for_forwarding",
    "revalidate_pinned_forwarding_binding",
]
