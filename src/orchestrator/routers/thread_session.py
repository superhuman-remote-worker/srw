"""Owner-facing session surface: detail, state, controls and tool groups (R1.B10).

These HTTP adapters read their collaborators from the requesting application's
``thread_session_dependencies_factory``. Authorization is always the first
statement of each route body and stays a visible gate call, so the endpoint
inventory classifies it. Route paths, names, docstrings and status codes are
the public contract and are kept byte-identical to their former declarations.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from orchestrator.schemas.thread_admission import ThreadUpdateRequest
from orchestrator.schemas.thread_session import (
    ThreadControlRequest,
    ToolGroupPreviewRequest,
)
from orchestrator.security.access import log_security_event
from orchestrator.services import session_tool_view
from orchestrator.services.deployment_gates import require_pinned_status_identity
from orchestrator.services.grant_enforcement import GrantDenied
from orchestrator.services.session_class_policy import require_stateless_workspace
from orchestrator.services.session_runtime_admission import (
    protected_cloud_marker_state,
)
from orchestrator.services.session_state_snapshot import build_session_state_snapshot
from orchestrator.services.stateless_workspace_gate import thread_metadata_object
from orchestrator.services.thread_control_inbox import (
    ControlAdmissionError,
    ControlAdmissionNotReady,
    admit_thread_control,
    find_existing_thread_control,
)
from orchestrator.services.thread_projection import redact_thread_metadata

logger = logging.getLogger(__name__)

router = APIRouter()


@dataclass(frozen=True, slots=True)
class ThreadSessionDependencies:
    """Application-owned collaborators for one session-surface request."""

    store: Any
    require_thread_owner: Callable[
        [Request, Any, str], Awaitable[tuple[dict[str, Any], dict[str, Any]]]
    ]
    require_approved_user: Callable[[Request, Any], Awaitable[dict[str, Any]]]
    resolve_cloud_session_url: Callable[..., str | None]
    resolve_session_config: Callable[..., Awaitable[dict[str, Any] | None]]
    enforce_session_create_grants: Callable[..., Awaitable[Any]]
    tool_view: session_tool_view.SessionToolViewDependencies


def get_thread_session_dependencies(request: Request) -> ThreadSessionDependencies:
    return request.app.state.thread_session_dependencies_factory()


@router.get("/api/persistent/threads/{thread_id}")
async def get_thread(
    thread_id: str,
    request: Request,
    *,
    dependencies: ThreadSessionDependencies = Depends(get_thread_session_dependencies),
) -> dict[str, Any]:
    """Get thread status and metadata (auth: owner only).

    Phase 1 of cloud_collaboration_model.md §9 surfaces the thread's
    attached mounts here so the Cockpit "Project files" panel can render
    them without a second round-trip. ``project_ids`` is the derived
    list-of-strings view kept stable for callers that only need scoping.

    Threads created before migration 0202 have ``ssh_handle IS NULL``; mint
    one lazily here on first view rather than showing an empty SSH panel
    forever. Deliberately not done on the list endpoint — minting up to 50
    handles as a side effect of rendering a list is unwanted write
    amplification.

    The mint is guarded (M-1): it's a write on an otherwise read-only view,
    so a write failure here (a read-only replica, a full disk — this
    deployment has actually had one) must not turn the whole thread view
    into a 500 for the sake of one SSH-panel field. Caught broadly since
    ``ensure_thread_ssh_handle`` can raise asyncpg errors or its own
    exhausted-retries ``RuntimeError``; either way the response degrades to
    a null handle (the panel already renders "unavailable" for that).
    """
    store = dependencies.store
    user, thread = await dependencies.require_thread_owner(request, store, thread_id)
    result = redact_thread_metadata(dict(thread))
    if not result.get("ssh_handle"):
        try:
            result["ssh_handle"] = await store.ensure_thread_ssh_handle(thread_id)
        except Exception:
            logger.warning(
                "ensure_thread_ssh_handle failed for thread %s (non-fatal)",
                str(thread_id)[:8],
                exc_info=True,
            )
    mounts = await store.list_thread_mounts(thread_id)
    result["cloud_session_url"] = dependencies.resolve_cloud_session_url(thread, mounts)
    result["mounts"] = [
        {
            "id": str(m["id"]),
            "mount_kind": m["mount_kind"],
            "target_path": m["target_path"],
            "source_kind": m["source_kind"],
            "source_ref": str(m["source_ref"]) if m.get("source_ref") else None,
            "backend_id": m.get("backend_id"),
        }
        for m in mounts
    ]
    result["project_ids"] = [
        str(m["source_ref"])
        for m in mounts
        if m.get("mount_kind") == "project" and m.get("source_ref")
    ]
    return result


@router.get("/api/persistent/threads/{thread_id}/state")
async def get_thread_session_state(
    thread_id: str,
    request: Request,
    response: Response,
    *,
    dependencies: ThreadSessionDependencies = Depends(get_thread_session_dependencies),
) -> dict[str, Any]:
    """Lane-agnostic, owner-gated current state for a session Cockpit.

    This is the REST twin of the agent's direct ``session.state`` welcome
    frame.  It intentionally reads durable state for *both* execution lanes;
    no lane or pod identity crosses the wire.  Journal-derived fields are
    point-in-time values at ``event_cursor``.  A client must apply the snapshot
    before replaying the journal from ``replay_cursor`` so the latest logical
    turn is rebuilt before any not-yet-flushed agent edge advances it.
    """

    started = time.perf_counter()
    _user, _thread = await dependencies.require_thread_owner(
        request, dependencies.store, thread_id
    )
    auth_done = time.perf_counter()

    # Model/temperature/narration are not all first-class thread columns yet.
    # Resolve from the exact thread row captured inside the snapshot's
    # repeatable-read transaction. A later config write then lands above the
    # returned event cursor and SSE replays it, instead of the cursor hiding a
    # scalar resolved from a different metadata revision.
    config_seconds = 0.0

    async def _resolve_snapshot_config(
        snapshot_thread: dict[str, Any], snapshot_metadata: dict[str, Any]
    ) -> dict[str, Any] | None:
        nonlocal config_seconds
        config_started = time.perf_counter()
        try:
            return await dependencies.resolve_session_config(
                snapshot_thread, snapshot_metadata
            )
        except GrantDenied:
            logger.warning(
                "Session-state config resolve denied for thread %s; using stored "
                "display fields",
                thread_id,
            )
            return None
        finally:
            config_seconds += time.perf_counter() - config_started

    snapshot = await build_session_state_snapshot(
        dependencies.store,
        thread_id,
        config_resolver=_resolve_snapshot_config,
    )
    if snapshot is None:
        raise HTTPException(status_code=404, detail="Thread not found")
    # Pending permissions include tool arguments. Never let a browser or an
    # intermediary retain one user's current control state for another read.
    response.headers["Cache-Control"] = "private, no-store"
    finished = time.perf_counter()
    logger.info(
        "session-state timing: thread=%s auth=%.3fs config=%.3fs "
        "snapshot=%.3fs total=%.3fs",
        thread_id,
        auth_done - started,
        config_seconds,
        max(0.0, finished - auth_done - config_seconds),
        finished - started,
    )
    return snapshot


@router.post(
    "/api/persistent/threads/{thread_id}/controls",
    status_code=202,
)
async def submit_thread_control(
    thread_id: str,
    body: ThreadControlRequest,
    request: Request,
    *,
    dependencies: ThreadSessionDependencies = Depends(get_thread_session_dependencies),
) -> dict[str, Any]:
    """Admit an owner-authorized control for the exact serving owner.

    This endpoint serves both execution lanes and deliberately exposes neither
    one. It persists a commit-ordered request, but neither the desired scalar
    nor a journal frame: the current lease owner (or exact reciprocal pinned
    binding) applies the request and journals the result with its own allocator.
    """
    from shared.run_queue import LANE_STATELESS

    started = time.perf_counter()
    store = dependencies.store
    user, thread = await dependencies.require_thread_owner(request, store, thread_id)
    thread_owner_id = thread.get("user_id")
    policy_user_id = str(thread_owner_id or user["id"])
    control_payload = body.control_payload()
    control_metadata = thread_metadata_object(thread)
    require_control_generation = bool(
        thread.get("execution_lane") == "pinned"
        and (
            protected_cloud_marker_state(control_metadata) != "off"
            or require_pinned_status_identity()
        )
    )

    try:
        existing = await find_existing_thread_control(
            store,
            thread_id=thread_id,
            owner_user_id=thread_owner_id,
            client_request_id=body.client_request_id,
            verb=body.method,
            payload=control_payload,
        )
    except ControlAdmissionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    # A new stateless control can create a control-only queue claim just like
    # human input. Refuse unsupported workspace bindings before that durable
    # admission can wake an executor. Exact idempotent retries remain observable
    # even if the thread's lane/tier changed after their commit.
    if existing is None and thread.get("execution_lane") == LANE_STATELESS:
        require_stateless_workspace(thread)

    if body.method == "mode.set" and existing is None:
        # Same PDP as create/attach/config.update. A stale or direct client
        # cannot persist a permission mode above the owner's current ceiling.
        # A retry of an already committed UUID bypasses mutable policy: a lost
        # 202 must stay observable even if grants changed afterward.
        try:
            await dependencies.enforce_session_create_grants(
                {"interactive": {"permission_mode": body.mode}},
                user_id=policy_user_id,
                project_ids=(
                    [str(thread["project_id"])] if thread.get("project_id") else []
                ),
            )
        except HTTPException:
            # Close the concurrent masked-commit race between the preflight
            # and PDP without weakening authorization for a genuinely new id.
            try:
                existing = await find_existing_thread_control(
                    store,
                    thread_id=thread_id,
                    owner_user_id=thread_owner_id,
                    client_request_id=body.client_request_id,
                    verb=body.method,
                    payload=control_payload,
                )
            except ControlAdmissionError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            if existing is None:
                raise

    actor_id = str(user.get("id") or user.get("sub") or "rest_client")
    try:
        admitted = await admit_thread_control(
            store,
            thread_id=thread_id,
            owner_user_id=thread_owner_id,
            client_request_id=body.client_request_id,
            verb=body.method,
            payload=control_payload,
            requested_by=actor_id,
            expected_runtime_generation=body.session_runtime_generation,
            require_pinned_runtime_generation=require_control_generation,
        )
    except ControlAdmissionNotReady as exc:
        # Registration intentionally keeps the exact pinned-owner capability
        # closed until its writer and first inbox drain are ready.  A control
        # clicked during that window is not a semantic conflict: 425 tells the
        # lane-free client to retry the same UUID after its bounded backoff.
        raise HTTPException(status_code=425, detail=str(exc)) from exc
    except ControlAdmissionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    await log_security_event(
        store,
        resource_type="thread",
        event_type="session_control_requested",
        user=user,
        resource_id=thread_id,
        detail=f"verb={body.method} request_seq={admitted.request_seq}",
        request=request,
    )
    logger.info(
        "session-control admission: thread=%s verb=%s seq=%d duplicate=%s total=%.3fs",
        thread_id,
        body.method,
        admitted.request_seq,
        admitted.duplicate,
        time.perf_counter() - started,
    )
    return {
        "accepted": True,
        "request_id": str(admitted.id),
        "client_request_id": str(admitted.client_request_id),
        "request_seq": admitted.request_seq,
        "method": admitted.verb,
        "state": admitted.state,
        "duplicate": admitted.duplicate,
        "session_runtime_generation": (
            str(admitted.runtime_generation)
            if admitted.runtime_generation is not None
            else None
        ),
    }


@router.get("/api/persistent/threads/{thread_id}/tool-groups")
async def get_thread_tool_groups(
    thread_id: str,
    request: Request,
    *,
    dependencies: ThreadSessionDependencies = Depends(get_thread_session_dependencies),
) -> dict[str, Any]:
    """What toolset does this session's agent actually have? (auth: owner)

    D6: **the answer comes from the agent.** The orchestrator asks the bound
    pod what it bound and serves that; it does not recompute it. Only the agent
    sees the runtime injection layer (``persistent_session._load_tools_for_backend``
    appends the session-task trio, the product guide, the fleet/catalog/workflow
    lists, ``srw_cloud_status``, the officer pair, the datasource categories),
    ``filter_tools_by_backend``, and ``load_tools``'s per-tool fallback. A
    config-only view over-reports by dozens of names, and the divergence
    between two implementations of one fact is the original bug here.

    ``origin`` is the field that matters, and callers MUST branch on it:

    - ``agent`` — **measured, in full**. A running pod enumerated its bound
      tools and returned the structured report. ``observed_at`` and ``backend``
      are set.
    - ``agent_partial`` — **measured, names only**. The pod answered but its
      image predates ``GET /session/toolset``, so the bound names come from
      ``/status`` with no timestamp, no workspace capabilities and no
      agent-side categorisation. ``degraded_reason`` says so. The names are as
      trustworthy as ``agent``; do NOT render a workspace-tier explanation from
      this answer, and do NOT infer measured-ness from ``observed_at``, which
      is legitimately null here.
    - ``prediction`` — **forecast** from the merged config, because there is no
      agent to ask (a new session, a suspended one, an unreachable pod).
      ``prediction_reason`` says which. Structurally weaker, not merely older:
      it cannot see the three layers listed above. Rendering it as fact is D1
      violated at a new seam.

    ``categories`` answers for ALL of them (25, ``mcp`` included), each with
    ``state`` (``on``/``off``/``unavailable``), ``reason`` when not settable,
    ``settable``, ``decided_by`` (the layer that produced the answer) and
    ``tools``. Measured entries also carry ``configured``, so a caller can see
    the merge and the measurement disagree instead of having to trust one.

    ``off`` is a promise that ticking the box would work, and it is only made
    when it can be kept: on a measurement, a category whose merged config
    grants tools while the agent bound none is ``unavailable``. See
    ``compose_tool_view``.

    ``source`` is unchanged and still describes the PREDICTION's model —
    ``resolved`` / ``legacy`` / ``error``. It says nothing about ``origin``:
    a measured answer is a measured answer whichever path the config took.

    ``tool_groups`` (the closed groups, booleans) is retained for the
    cockpit and is now DERIVED from ``categories`` rather than computed beside
    it, so the endpoint cannot disagree with itself.

    ``enumerate_only`` answers the *write* half of the same question: which
    categories refuse ``tools.<c>: true`` at the write boundary, and the
    registry-derived enumeration a caller must send instead
    (``{"shell": ["cancel_command", ...]}``). Without it the only way for the
    New Session form to offer "shell on" would be a hand-maintained tool-name
    list in the cockpit — a fifth parallel list, in the change that deletes
    four. See :func:`src.core.tool_policy.enumerate_only_members`.

    Deliberately NOT a field on ``GET /api/persistent/threads/{id}``: that
    endpoint is hot and this answer costs a config resolve plus a pod probe.
    """
    user, thread = await dependencies.require_thread_owner(
        request, dependencies.store, thread_id
    )
    return await session_tool_view.thread_tool_groups(
        thread_id, thread, dependencies=dependencies.tool_view
    )


@router.post("/api/persistent/tool-groups/preview")
async def preview_tool_groups(
    body: ToolGroupPreviewRequest,
    request: Request,
    *,
    dependencies: ThreadSessionDependencies = Depends(get_thread_session_dependencies),
) -> dict[str, Any]:
    """The New Session form's read. **Always a prediction, by construction.**

    There is no agent yet, so this endpoint can never return ``origin:
    "agent"`` — and that is the point of it being a separate route rather than
    a mode of the thread endpoint. D6's consequence is that the creation form
    forecasts while the live pane measures; making the difference structural
    (two routes, one of which cannot ever say "measured") is cheaper to keep
    honest than a flag someone forgets to read.

    ``source`` models the same three agent paths as the thread endpoint and is
    NOT hardcoded: with the experts feature or the per-user kill switch off, a
    created session takes the legacy path, where the compatibility groups are
    APPENDED unless explicitly disabled — the opposite of the resolved path for
    an unset group. Predicting "off" and labelling it ``resolved`` on such a
    deployment would be this series' own defect, rebuilt in the form that
    predicts it.

    Same ``categories`` shape as the thread endpoint, so one renderer serves
    both surfaces.
    """
    user = await dependencies.require_approved_user(request, dependencies.store)
    return await session_tool_view.preview_tool_groups(
        body, user, request=request, dependencies=dependencies.tool_view
    )


@router.patch("/api/persistent/threads/{thread_id}")
async def update_thread(
    thread_id: str,
    body: ThreadUpdateRequest,
    request: Request,
    *,
    dependencies: ThreadSessionDependencies = Depends(get_thread_session_dependencies),
) -> dict[str, str]:
    """Rename a persistent thread (auth: owner only).

    The title was previously settable only at creation and auto-generated
    once by the LLM after the first turn; this lets the user rename a session
    inline from the Cockpit. A user-chosen title naturally blocks the
    auto-titler, which only overwrites empty / "Untitled Session" / "Local
    Session" titles (src/api/persistent_app.py).
    """
    user, thread = await dependencies.require_thread_owner(
        request, dependencies.store, thread_id
    )
    title = (body.title or "").strip()
    if not title:
        raise HTTPException(status_code=400, detail="Title cannot be empty")
    if len(title) > 200:
        raise HTTPException(status_code=400, detail="Title too long (max 200)")
    await dependencies.store.update_thread_title(thread_id, title)
    return {"status": "updated", "title": title}


__all__ = [
    "ThreadSessionDependencies",
    "get_thread_session_dependencies",
    "router",
]
