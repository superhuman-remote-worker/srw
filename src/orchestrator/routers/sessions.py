"""``/api/sessions`` — prepare endpoint (Task 6) + connection endpoint (Task 7).

These two endpoints replace the WS handshake's pre-flight work that used to
live inline in ``src/orchestrator/main.py``'s ``persistent_ws_proxy``.

  - ``POST /api/sessions/{thread_id}/prepare`` — pinned-lane slow path. Auth,
    ownership, provisioning, readiness. Returns 202 immediately; progress goes
    via the existing SSE notification feed on event type ``session.lifecycle``.
    Idempotent: a concurrent retry blocks on a Postgres advisory lock keyed by
    thread_id and returns the in-flight call's result. Non-pinned lanes are
    refused rather than provisioned.
  - ``GET /api/sessions/{thread_id}/connection`` — transport discovery. A
    pinned session returns its WebSocket coordinates; a stateless session is
    immediately admission-ready and explicitly reports that it has no control
    socket. Stateless control transport is not implemented in this slice.

Spec: knowledge-base/knowledge/features/direct_session_websockets.md §Component details.

R1.B06 closed this router's late ``from orchestrator.main import ...``.
Collaborators arrive on :class:`SessionsDependencies`, resolved from the
application handling the request and threaded explicitly into the background
tasks the two endpoints schedule — a task outlives its request, so it has to
carry the ports rather than look them up later.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from typing import Annotated, Any, Literal

from dataclasses import dataclass
from typing import Awaitable, Callable

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, Field

from orchestrator.security.auth import require_approved_user
from orchestrator.services.agent_pod_entrypoint import (
    InvalidConfigNameError,
    validate_config_name,
)
from orchestrator.services.session_lifecycle import emit as lifecycle_emit
from orchestrator.services.session_lifecycle import (
    probe_ready,
    wait_for_binding,
    wait_for_ready,
)
from orchestrator.services.session_provisioning_state import (
    agent_pod_provisioning_in_progress,
)
from orchestrator.services.session_router import SessionRouteAuthorityError
from orchestrator.services.session_urls import session_websocket_origin
from orchestrator.services.deployment_gates import (
    stateless_idle_conversation_rewind_enabled,
)

# R1.B05 moved these three out of `main`; they are pure, so this router
# reaches their owners directly instead of going back through the
# application module.
from orchestrator.services.grant_enforcement import grant_violations_detail
from orchestrator.services.session_config_resolution import (
    endpoint_violations_detail,
)
from orchestrator.services import session_tool_policy
from orchestrator.services import session_workspace_policy
from orchestrator.services import workspace_tier_policy
from orchestrator.services.session_runtime_admission import (
    ThreadRuntimeAuthority,
    pinned_binding_invalid_detail,
    protected_cloud_marker_state,
    same_thread_runtime_authority,
    thread_runtime_authority,
    thread_requests_protected_cloud,
    thread_runtime_is_preparable,
    thread_runtime_refusal_detail,
)
from shared.pinned_session_identity import (
    PinnedSessionBinding,
)
from shared.run_queue import LANE_PINNED, LANE_STATELESS

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/sessions", tags=["Sessions"])


async def _read_queue_block(db: Any, thread: dict[str, Any]) -> dict[str, Any] | None:
    """The connection's ``queue`` block, or ``None`` if it cannot be read.

    Diagnostics must never make a ready session unreachable, so a database
    fault here is logged and the block is omitted; the cockpit treats a
    missing block as "unknown", not as "idle".
    """
    from orchestrator.services.stateless_queue_state import queue_block_for_thread

    acquire = getattr(db, "acquire", None)
    if acquire is None:
        return None
    try:
        async with acquire() as conn:
            return await queue_block_for_thread(conn, thread)
    except Exception as exc:
        logger.warning(
            "connection queue block unavailable for thread %s: %s",
            thread.get("id"),
            exc,
        )
        return None


@dataclass(frozen=True, slots=True)
class SessionsDependencies:
    """Everything the two session endpoints need, already constructed.

    ``store`` and the four provisioner/router singletons are the composition
    bindings of the application answering the request. The callables are the
    owning services' operations with their own dependency objects applied.
    """

    store: Any
    agent_provisioner: Any
    container_provisioner: Any
    workspace_suspension_service: Any
    session_router: Any
    session_tokens: Any
    ensure_session_workspace: Callable[..., Awaitable[Any]]
    await_late_cloud_setup: Callable[[str], Awaitable[None]]
    await_protected_cloud_runtime_ready: Callable[..., Awaitable[bool]]
    session_grant_violations: Callable[..., Awaitable[list[str]]]
    session_endpoint_violations: Callable[..., Awaitable[list[str]]]
    find_idle_persistent_agent: Callable[[], Awaitable[dict[str, Any] | None]]
    send_session_attach: Callable[..., Awaitable[bool]]


def get_sessions_dependencies(request: Request) -> SessionsDependencies:
    """Resolve collaborators only from the application handling this request."""
    return request.app.state.sessions_dependencies_factory()


def _is_expert_uuid(value: str | None) -> bool:
    """True if *value* is a UUID — i.e. an expert id that leaked into
    ``config_name`` via the cockpit's picker. ``config_name`` is always a
    bundled-config slug, never a UUID, so a UUID here means the expert was sent
    in the wrong field. The thread row already carries the materialized expert
    in ``config_override``, so the boot config must fall back to the base —
    never ``--config <uuid>`` (which has no on-disk YAML and crashes startup)."""
    if not value:
        return False
    try:
        uuid.UUID(str(value))
        return True
    except (ValueError, TypeError, AttributeError):
        return False


# --------------------------------------------------------------------------- #
# Models
# --------------------------------------------------------------------------- #


class PrepareRequest(BaseModel):
    config_name: str | None = Field(None, max_length=120)
    config_override: dict[str, Any] | None = None


class PrepareResponse(BaseModel):
    state: str = Field(..., examples=["provisioning"])


_PINNED_PROVISIONING_ONLY_DETAIL = (
    "Session execution lane does not use pinned provisioning"
)


def _require_preparable_thread(thread: dict[str, Any] | None) -> dict[str, Any]:
    """Public/runtime-boundary lifecycle gate, separate from lane selection."""

    if not thread_runtime_is_preparable(thread):
        raise HTTPException(
            status_code=409,
            detail=thread_runtime_refusal_detail(thread),
        )
    return thread


def _require_supported_protected_prepare_override(
    thread: dict[str, Any], override: dict[str, Any] | None
) -> None:
    """Reject protected topology changes before a detached task can exist.

    The body override reaches provisioning as a replacement configuration, so
    validating only the persisted thread leaves an API caller able to smuggle
    a VM/lite workspace or Officer class into the background task.  This is a
    synchronous admission check: every refusal happens before task scheduling,
    lifecycle emission, reservation, or infrastructure work.
    """

    metadata = thread.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            import json

            metadata = json.loads(metadata)
        except (TypeError, ValueError):
            metadata = None
    marker = protected_cloud_marker_state(metadata)
    if marker == "off":
        return
    if marker != "on":
        raise HTTPException(
            status_code=409,
            detail={
                "code": "protected_cloud_malformed",
                "message": "Protected cloud session state is invalid.",
            },
        )

    def _check_fragment(fragment: Any) -> None:
        if fragment is None:
            return
        if not isinstance(fragment, dict):
            raise HTTPException(
                status_code=422,
                detail={"code": "protected_cloud_config_malformed"},
            )
        workspace = fragment.get("workspace")
        if workspace is not None:
            if not isinstance(workspace, dict):
                raise HTTPException(
                    status_code=422,
                    detail={"code": "protected_cloud_config_malformed"},
                )
            for key in ("backend", "tier"):
                if key not in workspace:
                    continue
                value = workspace.get(key)
                if not isinstance(value, str) or value.strip().lower() not in {
                    "sandbox",
                    "container",
                }:
                    raise HTTPException(
                        status_code=422,
                        detail={
                            "code": "protected_cloud_unsupported_workspace",
                            "message": (
                                "Protected cloud sessions require the Container "
                                "workspace tier."
                            ),
                        },
                    )
        officer = fragment.get("officer")
        if officer is not None:
            if not isinstance(officer, dict) or (
                "enabled" in officer and type(officer.get("enabled")) is not bool
            ):
                raise HTTPException(
                    status_code=422,
                    detail={"code": "protected_cloud_config_malformed"},
                )
            if officer.get("enabled") is True:
                raise HTTPException(
                    status_code=422,
                    detail={
                        "code": "protected_cloud_unsupported_session_class",
                        "message": (
                            "Protected cloud sessions are not supported for the "
                            "background Officer runtime."
                        ),
                    },
                )

    persisted_override = (
        metadata.get("config_override") if isinstance(metadata, dict) else None
    )
    _check_fragment(persisted_override)
    _check_fragment(override)
    if isinstance(override, dict):
        _check_fragment(override.get("agent"))


def _schedule_prepare_task(coro: Any) -> asyncio.Task[Any]:
    return asyncio.create_task(coro)


# --------------------------------------------------------------------------- #
# POST /api/sessions/{thread_id}/prepare
# --------------------------------------------------------------------------- #


@router.post(
    "/{thread_id}/prepare",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PrepareResponse,
)
async def prepare_session(
    request: Request,
    thread_id: str,
    body: PrepareRequest,
):
    """Kick off (or rejoin) provisioning for the given thread.

    Returns 202 immediately. The caller subscribes to the SSE notification
    feed and waits for ``session.lifecycle`` events with state=ready, then
    calls GET /api/sessions/{tid}/connection for the token.
    """
    dependencies = get_sessions_dependencies(request)
    db = dependencies.store
    user = await require_approved_user(request, db)

    thread = await db.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="thread not found")
    if str(thread.get("user_id") or "") != str(user["id"]):
        raise HTTPException(status_code=403, detail="thread access denied")
    _require_preparable_thread(thread)
    runtime_authority = thread_runtime_authority(thread)
    if runtime_authority is None:
        raise HTTPException(status_code=409, detail="Session runtime is unavailable")
    if thread.get("execution_lane") != LANE_PINNED:
        # Fail closed: only the explicitly pinned lane may enter the pod
        # provisioner.  A stateless thread is served by run_queue claims, and
        # an unknown future lane must not silently inherit pinned semantics.
        raise HTTPException(
            status_code=409,
            detail=_PINNED_PROVISIONING_ONLY_DETAIL,
        )

    _require_supported_protected_prepare_override(thread, body.config_override)

    # The body's own config_name is a caller boundary, so it gets the pod
    # entrypoint's allow-list here: a hostile value is one 422 rather than a
    # lifecycle failure the caller has to subscribe to. The PERSISTED value is
    # deliberately NOT checked — a row poisoned before that column was validated
    # on write must still be preparable-to-a-clear-failure, which _do_prepare's
    # own handler records.
    if body.config_name is not None:
        try:
            validate_config_name(body.config_name)
        except InvalidConfigNameError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    # The agent boots `--config <config_name>` and must load a real base YAML.
    # The cockpit's expert picker sends the expert UUID in config_name (here and
    # at create_thread); the bound expert is already materialized into the
    # thread's config_override and applied at attach, so a UUID here must fall
    # back to the persisted base instead of crashing startup on a missing file.
    boot_config_name = body.config_name or thread.get("config_name")
    if _is_expert_uuid(boot_config_name):
        persisted = thread.get("config_name")
        boot_config_name = (
            persisted
            if persisted and not _is_expert_uuid(persisted)
            else "session_base"
        )

    # This body's config_override is a write boundary, not a hint: it flows to
    # _resolve_session_config (main.py), where a non-None value REPLACES the
    # thread's persisted override outright. Unvalidated, that is the smuggle
    # from the job surface reappearing here — `tools.canvas: ["run_command"]`
    # binds a shell tool, because the loader resolves a name against the global
    # registry rather than the key it arrived under. Same one validator as
    # every other boundary. The cockpit posts `{}`, so nothing it sends is
    # affected; this closes the API-direct path.
    validated_override = session_tool_policy.with_validated_tool_overrides(
        body.config_override
    )

    # Fire-and-forget the actual work in a background task. Progress reaches
    # the cockpit via SSE. Idempotency is enforced by the advisory lock
    # inside _do_prepare.
    _schedule_prepare_task(
        _do_prepare(
            thread_id=thread_id,
            user_id=str(user["id"]),
            config_name=boot_config_name,
            config_override=validated_override,
            runtime_authority=runtime_authority,
            dependencies=dependencies,
        )
    )

    return PrepareResponse(state="provisioning")


async def _do_prepare(
    thread_id: str,
    user_id: str,
    config_name: str | None,
    config_override: dict[str, Any] | None,
    runtime_authority: ThreadRuntimeAuthority,
    *,
    dependencies: SessionsDependencies,
) -> None:
    """Run the actual provisioning + readiness work asynchronously.

    Serializes concurrent prepares on the same thread via advisory lock.
    Broadcasts ``session.lifecycle`` SSE events at each phase change.

    Locking strategy: the advisory lock is held ONLY across the
    decide-who-provisions critical section. ``wait_for_binding`` and
    ``wait_for_ready`` run AFTER the lock is released — the fresh-pod
    path's agent registers via ``POST /api/agents/register``, which
    acquires the SAME advisory lock for its duplicate-rejection check,
    so holding it across the wait deadlocks both for the asyncpg query
    timeout (~60s).
    """
    db = dependencies.store

    def _emit(state: str, **extra: Any) -> None:
        lifecycle_emit(
            user_id,
            thread_id,
            state,
            session_runtime_generation=runtime_authority.generation,
            **extra,
        )

    # The public handler can race End before this detached task starts.  Never
    # emit a fresh provisioning phase for a terminal generation.
    thread = await db.get_thread(thread_id)
    if not same_thread_runtime_authority(thread, runtime_authority):
        return

    # Emit "provisioning" up-front so the cockpit's progress card surfaces
    # the phase even when a sibling path (POST /resume) wins the race and
    # has the agent_id set by the time we acquire the lock. The state names
    # describe phases of the request, not of the underlying work — if the
    # agent is already bound, we still went through a "preparing" phase
    # before we got there.
    _emit("provisioning")

    # Second attach path (the first is POST /resume's _reprovision): let any
    # in-flight cloud session-folder provisioning land before an agent is
    # bound, since the agent reads its cloud config within ~150ms of attach
    # and never re-reads. Emitted "provisioning" already, so the cockpit's
    # progress card covers the wait. Deliberately OUTSIDE the advisory lock —
    # this can take seconds and the fresh pod's /register needs the same lock.
    # knowledge-history/done/session_resume_cloud_sync_race_late_provision.md
    await dependencies.await_late_cloud_setup(thread_id)
    thread = await db.get_thread(thread_id)
    if not same_thread_runtime_authority(thread, runtime_authority):
        return

    # A protected thread may not reserve or provision an agent until reader
    # engagement has produced the active mount payload.  The helper is a no-op
    # for ordinary sessions and rechecks terminal lifecycle while waiting.
    if not await dependencies.await_protected_cloud_runtime_ready(thread_id):
        current = await db.get_thread(thread_id)
        if same_thread_runtime_authority(current, runtime_authority):
            _emit(
                "failed",
                reason="Protected cloud reader or staging overlay is unavailable",
            )
        return

    needs_binding_wait = False
    try:
        async with db.thread_advisory_lock(thread_id):
            thread = await db.get_thread(thread_id)
            if not thread:
                _emit("failed", reason="thread vanished")
                return
            if not same_thread_runtime_authority(thread, runtime_authority):
                return
            if thread.get("execution_lane") != LANE_PINNED:
                # Defense in depth for direct/internal callers and for a lane
                # change between the public handler and this background task.
                # Do not provision, attach, reconcile a workspace, or wait for
                # a binding on behalf of a queue-served thread.
                logger.warning(
                    "Thread %s: refusing pinned prepare for execution lane %r",
                    thread_id,
                    thread.get("execution_lane"),
                )
                _emit("failed", reason=_PINNED_PROVISIONING_ONLY_DETAIL)
                return

            # Provisioning (if needed). Only kick off the bind here; the
            # wait happens after the lock is released so the new pod's
            # /register can acquire it.
            if not thread.get("agent_id"):
                # Pre-flight the capability grants BEFORE any provisioning work:
                # a never-startable config (e.g. permission_mode above the user's
                # ceiling) would otherwise reconcile a workspace and boot a pod
                # that 403s at the workspace endpoint and exits, leaving the
                # cockpit to poll /connection until its ~5m40s ready timeout. Fail
                # fast with the real reason instead.
                # knowledge-base/knowledge/issues/session_permission_mode_grant_denied_ready_timeout.md
                _violations = await dependencies.session_grant_violations(thread)
                thread = await db.get_thread(thread_id)
                if not same_thread_runtime_authority(thread, runtime_authority):
                    return
                if _violations:
                    logger.warning(
                        "Thread %s: prepare denied by capability grants: %s",
                        thread_id,
                        "; ".join(_violations),
                    )
                    _emit("failed", reason=grant_violations_detail(_violations))
                    return

                # Same fail-fast for unusable model-role transports (e.g. the
                # memory reranker with no reachable embedding endpoint) — reject
                # before reconciling a workspace + booting a doomed pod.
                # knowledge-base/knowledge/issues/openrouter_auxiliary_crashes_session_via_memory_reranker.md
                _ep_violations = await dependencies.session_endpoint_violations(thread)
                thread = await db.get_thread(thread_id)
                if not same_thread_runtime_authority(thread, runtime_authority):
                    return
                if _ep_violations:
                    logger.warning(
                        "Thread %s: prepare denied by unusable transport: %s",
                        thread_id,
                        "; ".join(_ep_violations),
                    )
                    _emit(
                        "failed",
                        reason=endpoint_violations_detail(_ep_violations),
                    )
                    return

                # Re-provisioning the agent (cold start / reopen): also reconcile
                # the session workspace. ensure_workspace's drift probe recreates
                # a 'ready'-but-dead pod (e.g. one destroyed by a cluster
                # restart), restores a 'suspended' one, and creates a
                # missing/failed one — so the fresh agent binds to a live
                # workspace instead of SSH-looping a dead address. Fire-and-forget
                # (mirrors the resume path in main.py); the agent tolerates a
                # not-yet-ready workspace.
                asyncio.create_task(
                    dependencies.ensure_session_workspace(
                        thread_id,
                        db=dependencies.store,
                        provisioner=dependencies.container_provisioner,
                        suspension=dependencies.workspace_suspension_service,
                        expected_runtime_generation=runtime_authority.generation,
                    )
                )

                if agent_pod_provisioning_in_progress(thread):
                    logger.info(
                        "Thread %s: agent pod already provisioning — "
                        "waiting for binding.",
                        thread_id,
                    )
                else:
                    await _provision_agent_for_thread(
                        thread_id=thread_id,
                        config_name=config_name or "session_base",
                        config_override=config_override,
                        dependencies=dependencies,
                    )
                    thread = await db.get_thread(thread_id)
                    if not same_thread_runtime_authority(thread, runtime_authority):
                        return
                needs_binding_wait = True

        # Lock released. For fresh-pod paths, wait for the agent's
        # /register to write threads.agent_id (which needs the lock we
        # just dropped). For idle-pool paths, send_session_attach has
        # already set agent_id via the orchestrator's own DB connection,
        # so wait_for_binding returns immediately.
        if needs_binding_wait:
            bind_timeout_s = int(os.environ.get("AGENT_BIND_TIMEOUT_S", "300"))
            if not await wait_for_binding(thread_id, bind_timeout_s, store=db):
                current = await db.get_thread(thread_id)
                if same_thread_runtime_authority(current, runtime_authority):
                    _emit("failed", reason="agent failed to register")
                return

        thread = await db.get_thread(thread_id)
        if not same_thread_runtime_authority(thread, runtime_authority):
            return

        # Readiness probe. A VM-backed thread pays a cold KubeVirt boot far
        # beyond the sandbox default; size the budget from the thread's stored
        # backend and tag the lifecycle event so the cockpit shows the VM copy.
        # (knowledge-base/knowledge/features/session_create_on_vm.md)

        thread = await db.get_thread(thread_id)
        if not same_thread_runtime_authority(thread, runtime_authority):
            return
        _backend = workspace_tier_policy.thread_workspace_backend(thread)
        _vm_tag = {"backend": "vm"} if _backend == "vm" else {}
        _emit("booting", **_vm_tag)
        binding: PinnedSessionBinding | None = await db.get_pinned_session_binding(
            thread_id,
            expected_runtime_generation=runtime_authority.generation,
        )
        startup_statuses = {"booting", "ready", "working", "session"}
        if binding is None or binding.agent_status not in startup_statuses:
            current = await db.get_thread(thread_id)
            if same_thread_runtime_authority(current, runtime_authority):
                _emit("failed", reason="session binding is not authoritative")
            return

        from orchestrator.services.stateless_workspace_gate import (
            thread_metadata_object,
        )

        ready_timeout_s = session_workspace_policy.session_ready_timeout_s(
            _backend,
            preparation=bool(
                session_workspace_policy.preparation_wait_budget(
                    thread_metadata_object(thread).get("config_override")
                )
            ),
        )
        if not await wait_for_ready(
            pod_ip=binding.pod_ip,
            pod_port=binding.pod_port,
            timeout_s=ready_timeout_s,
            require_protected_cloud=thread_requests_protected_cloud(thread),
            expected_session_identity_fingerprint=(
                binding.session_identity_fingerprint
            ),
        ):
            current = await db.get_thread(thread_id)
            if same_thread_runtime_authority(current, runtime_authority):
                _emit("failed", reason="agent /ready timeout")
            return

        current_binding: (
            PinnedSessionBinding | None
        ) = await db.get_pinned_session_binding(
            thread_id,
            expected_runtime_generation=runtime_authority.generation,
        )
        if (
            current_binding is None
            or current_binding.target_key != binding.target_key
            or current_binding.agent_status not in startup_statuses
        ):
            return

        # Create the route resource.
        route_published = False
        try:
            await dependencies.session_router.ensure_route(
                thread_id=thread_id,
                pod_name=binding.agent_hostname,
                pod_uid=binding.pod_uid,
                runtime_generation=runtime_authority.generation,
            )

            current_binding = await db.get_pinned_session_binding(
                thread_id,
                expected_runtime_generation=runtime_authority.generation,
            )
            if (
                current_binding is None
                or current_binding.target_key != binding.target_key
                or current_binding.agent_status not in startup_statuses
            ):
                return

            _emit("ready")
            route_published = True
        finally:
            # ``ensure_route`` may create the deterministic Service before an
            # Ingress failure. Any path that does not publish ready therefore
            # owes exact G/Pod cleanup, including exceptions from the final DB
            # reread. A false result means cleanup authority was not proven;
            # surface that failure instead of silently wedging the successor.
            if not route_published:
                route_removed = await dependencies.session_router.teardown_route(
                    thread_id,
                    expected_namespace=binding.pod_namespace,
                    expected_runtime_generation=runtime_authority.generation,
                    expected_owner_uid=binding.pod_uid,
                )
                if not route_removed:
                    raise RuntimeError("incomplete session route could not be removed")
    except Exception as e:
        logger.exception("prepare failed for thread %s: %s", thread_id, e)
        current = await db.get_thread(thread_id)
        if same_thread_runtime_authority(current, runtime_authority):
            _emit("failed", reason=str(e))


async def _provision_agent_for_thread(
    thread_id: str,
    config_name: str,
    config_override: dict[str, Any] | None,
    *,
    dependencies: SessionsDependencies,
) -> None:
    """Trigger pool-first then create-pod provisioning.

    Migrated from main.py:_ws_provision (the inline helper that used to live
    inside persistent_ws_proxy at main.py:13851-13884).
    """
    # This helper is also called directly in tests and is a tempting future
    # reuse point.  Re-read the authoritative row and whitelist the one lane
    # that is allowed to bind a registered agent; a blacklist of today's
    # stateless name would make the next lane unsafe by default.
    db = dependencies.store
    thread = await db.get_thread(thread_id)
    runtime_authority = thread_runtime_authority(thread)
    if runtime_authority is None or thread.get("execution_lane") != LANE_PINNED:
        raise RuntimeError(_PINNED_PROVISIONING_ONLY_DETAIL)

    if not await dependencies.await_protected_cloud_runtime_ready(thread_id):
        raise RuntimeError("Protected cloud engagement is not ready")
    if not same_thread_runtime_authority(
        await db.get_thread(thread_id), runtime_authority
    ):
        raise RuntimeError("Session lifecycle changed during preparation")

    idle_agent = await dependencies.find_idle_persistent_agent()
    thread = await db.get_thread(thread_id)
    if not same_thread_runtime_authority(thread, runtime_authority):
        raise RuntimeError("Session lifecycle changed during preparation")
    if idle_agent:
        ok = await dependencies.send_session_attach(
            idle_agent, thread_id, config_override or {}, [], datasources=None
        )
        if ok:
            return

        # Reservation refusal can race a lane transition or a sibling bind.
        # Re-read before the fresh-pod fallback; the entry snapshot is no
        # longer authority after an awaited HTTP/DB path.
        current = await db.get_thread(thread_id)
        if (
            not same_thread_runtime_authority(current, runtime_authority)
            or current.get("execution_lane") != LANE_PINNED
        ):
            raise RuntimeError(_PINNED_PROVISIONING_ONLY_DETAIL)
        if current.get("agent_id") or agent_pod_provisioning_in_progress(current):
            return

    current = await db.get_thread(thread_id)
    if not same_thread_runtime_authority(current, runtime_authority):
        raise RuntimeError("Session lifecycle changed during preparation")
    await dependencies.agent_provisioner.provision_agent(
        purpose="session", thread_id=thread_id, config_name=config_name
    )
    if not same_thread_runtime_authority(
        await db.get_thread(thread_id), runtime_authority
    ):
        raise RuntimeError("Session lifecycle changed during agent provisioning")


# --------------------------------------------------------------------------- #
# GET /api/sessions/{thread_id}/connection
# --------------------------------------------------------------------------- #


#: Transport a control verb travels over for THIS session. The Cockpit
#: dispatches by this declaration and never by inferring a lane from
#: ``control_socket`` — a verb absent from ``controls`` is unavailable and is
#: rendered disabled rather than queued for a socket that will never open
#: (MCP's lifecycle rule: only use capabilities that were negotiated). See
#: knowledge-base/knowledge/issues/live_settings_silently_dropped_on_stateless_sessions.md.
ControlTransport = Literal["websocket", "rest"]

#: Pinned sessions: the direct per-session socket carries every session-scoped
#: verb; the two ordered scalars ride the durable control inbox on both lanes.
PINNED_CONTROLS: dict[str, ControlTransport] = {
    "config.update": "websocket",
    "compact": "websocket",
    "archive": "websocket",
    "rewind": "websocket",
    "undo": "websocket",
    "upgrade-to-workspace": "websocket",
    "mode.set": "rest",
    "narration.set": "rest",
}

#: Queue-served sessions bind no agent, so nothing rides a socket. Config
#: edits go through the owner PATCH (persisted at admission, applied by the
#: next claim's attach — the same turn-boundary semantics the pane already
#: documents); the scalar verbs and workspace undo ride the control inbox.
#: ``compact`` / ``archive`` / ``rewind`` / ``upgrade-to-workspace`` have no
#: stateless transport yet (stateless_agents.md §"Still open after S1/S2")
#: and are deliberately ABSENT rather than mapped to something that drops them.
STATELESS_CONTROLS: dict[str, ControlTransport] = {
    "config.update": "rest",
    "workspace.undo": "rest",
    "mode.set": "rest",
    "narration.set": "rest",
}


class PinnedConnectionResponse(BaseModel):
    """Connection coordinates when a per-session control socket exists."""

    state: Literal["ready"]
    control_socket: Literal["websocket"]
    ws_url: str
    token: str
    expires_at: int
    pinned_runtime_generation_contract: Literal[1] = 1
    session_runtime_generation: str
    controls: dict[str, ControlTransport] = Field(
        default_factory=lambda: dict(PINNED_CONTROLS)
    )
    control_options: dict[str, Any] = Field(default_factory=dict)
    conversation_revision: int = 0
    # Queue lifecycle block (stateless_turn_resilience.md step 2). Always
    # None on the pinned lane: a pinned session has no run_queue unit.
    queue: dict[str, Any] | None = None


class StatelessConnectionResponse(BaseModel):
    """Admission readiness when no per-session control socket exists."""

    state: Literal["ready"]
    control_socket: Literal["none"]
    ws_url: None
    token: None
    expires_at: None
    pinned_runtime_generation_contract: Literal[1] = 1
    session_runtime_generation: str
    controls: dict[str, ControlTransport] = Field(
        default_factory=lambda: dict(STATELESS_CONTROLS)
    )
    control_options: dict[str, Any] = Field(default_factory=dict)
    conversation_revision: int = 0
    # Queue lifecycle block (stateless_turn_resilience.md step 2): the same
    # shape POST …/input and GET …/queue return, so a reload can re-derive a
    # queued or parked turn instead of erasing it. None only when the read
    # itself failed — never a reason to refuse the connection.
    queue: dict[str, Any] | None = None


ConnectionResponse = Annotated[
    PinnedConnectionResponse | StatelessConnectionResponse,
    Field(discriminator="control_socket"),
]


@router.get(
    "/{thread_id}/connection",
    response_model=ConnectionResponse,
)
async def get_connection(
    request: Request,
    thread_id: str,
):
    """Return the lane-free control transport available for this session.

    Pinned cold-start and warm reconnect share one WebSocket token-mint path.
    Queue-served sessions bind no agent and can accept turns immediately, so
    they return ``control_socket='none'`` and null socket fields. Execution
    topology remains an internal server concern. This does not claim a
    replacement control transport exists. Unknown lanes fail closed.
    """
    dependencies = get_sessions_dependencies(request)
    db = dependencies.store
    user = await require_approved_user(request, db)

    thread = await db.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="thread not found")
    if str(thread.get("user_id") or "") != str(user["id"]):
        raise HTTPException(status_code=403, detail="thread access denied")
    _require_preparable_thread(thread)
    runtime_authority = thread_runtime_authority(thread)
    if runtime_authority is None:
        raise HTTPException(status_code=409, detail="Session runtime is unavailable")

    if not await dependencies.await_protected_cloud_runtime_ready(
        thread_id, timeout_s=0
    ):
        _require_preparable_thread(await db.get_thread(thread_id))
        raise HTTPException(
            status_code=425,
            detail={"code": "protected_cloud_not_ready"},
        )

    # The readiness join above crosses database/cloud awaits.  Its entry row
    # is no longer authoritative for either lifecycle or lane: End or a
    # detached lane transition may have committed while the reader probe was
    # running.  Re-read before the stateless fast return (and before using any
    # pinned binding) so a stale snapshot can never mint a ready connection.
    thread = await db.get_thread(thread_id)
    if not same_thread_runtime_authority(thread, runtime_authority):
        raise HTTPException(status_code=409, detail="Session runtime changed")

    execution_lane = thread.get("execution_lane")
    if execution_lane == LANE_STATELESS:
        if thread.get("agent_id"):
            # Lane flips are permitted only while detached.  Reporting this
            # row as healthy would conceal the exact double-executor state the
            # provisioning gate exists to prevent.
            raise HTTPException(
                status_code=409,
                detail="Stateless session has an incompatible agent binding",
            )
        controls = dict(STATELESS_CONTROLS)
        control_options: dict[str, Any] = {}
        officer = False
        if stateless_idle_conversation_rewind_enabled() and (
            str(thread.get("kind") or "") == "session"
            and thread.get("parent_job_id") is None
            and thread.get("parent_thread_id") is None
        ):
            metadata = thread.get("metadata")
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except (TypeError, ValueError):
                    metadata = None
            officer = not isinstance(metadata, dict) or bool(
                isinstance(metadata.get("config_override"), dict)
                and isinstance(metadata["config_override"].get("officer"), dict)
                and metadata["config_override"]["officer"].get("enabled") is True
            )
            async with db.acquire() as conn:
                officer = officer or bool(
                    await conn.fetchval(
                        "SELECT EXISTS (SELECT 1 FROM project_officers "
                        "WHERE thread_id=$1)",
                        thread_id,
                    )
                )
            if not officer:
                controls["rewind"] = "rest"
                control_options["rewind"] = {
                    "version": 1,
                    "modes": ["conversation"],
                    "requires_idle": True,
                }
        return StatelessConnectionResponse(
            state="ready",
            control_socket="none",
            ws_url=None,
            token=None,
            expires_at=None,
            session_runtime_generation=runtime_authority.generation,
            controls=controls,
            control_options=control_options,
            conversation_revision=int(thread.get("conversation_revision") or 0),
            queue=await _read_queue_block(db, thread),
        )
    if execution_lane != LANE_PINNED:
        raise HTTPException(
            status_code=409,
            detail="Unsupported session execution lane",
        )

    if not thread.get("agent_id"):
        # Not bound yet — caller should POST /prepare.
        raise HTTPException(status_code=425, detail="session not ready")

    async def _raise_exact_binding_refusal() -> None:
        current = await db.get_thread(runtime_authority.thread_id)
        if not same_thread_runtime_authority(current, runtime_authority):
            _require_preparable_thread(current)
            raise HTTPException(status_code=425, detail="session not ready")
        raise HTTPException(
            status_code=409,
            detail=pinned_binding_invalid_detail(runtime_authority),
        )

    async def _read_binding() -> PinnedSessionBinding | None:
        return await db.get_pinned_session_binding(
            runtime_authority.thread_id,
            expected_runtime_generation=runtime_authority.generation,
        )

    def _require_live_agent(binding: PinnedSessionBinding) -> None:
        # Offline is a liveness hint with a durable stale-agent detector as
        # recovery owner. Other non-live states are bound cold-start states;
        # the cockpit's generic 409 readiness poll remains their owner.
        if binding.agent_status == "offline":
            raise HTTPException(status_code=425, detail="session not ready")
        if binding.agent_status not in ("ready", "working", "session"):
            raise HTTPException(status_code=409, detail="agent not ready")

    binding = await _read_binding()
    if binding is None:
        await _raise_exact_binding_refusal()
        raise AssertionError("unreachable")
    _require_live_agent(binding)
    captured_target = binding.target_key
    identity_fingerprint = binding.session_identity_fingerprint

    # Verify the agent is actually session-ready before minting a token.
    # ``agent.status`` is set by the heartbeat (~60s lag), and an idle
    # pool agent that just received SESSION_ATTACH still reads "ready"
    # for that window even though its ``_attach_session`` hasn't
    # finished. For fresh-pod path, Uvicorn doesn't even start serving
    # until ``_attach_session`` completes — so the WS would 503 at
    # Traefik until K8s sees the pod's /ready flip true and updates
    # endpoints. ``probe_ready`` is the truthful signal here: 425 makes
    # the cockpit's ``_pollConnectionUntilReady`` wait (180s window)
    # instead of opening WS into the void and burning through its
    # 8-attempt reconnect budget before K8s converges.
    if not await probe_ready(
        binding.pod_ip,
        binding.pod_port,
        require_protected_cloud=thread_requests_protected_cloud(thread),
        expected_session_identity_fingerprint=identity_fingerprint,
    ):
        raise HTTPException(status_code=425, detail="session not ready")

    current_binding = await _read_binding()
    if current_binding is None or current_binding.target_key != captured_target:
        await _raise_exact_binding_refusal()
        raise AssertionError("unreachable")
    _require_live_agent(current_binding)

    # Make /connection self-healing: any code path that binds an agent to a
    # thread (POST /prepare, the legacy resume in main.py, orchestrator restart
    # re-binding from DB) must end up routable. ensure_route is idempotent and
    # tolerates concurrent-create races, so calling it here guarantees the
    # Service + Ingress exist by the time the cockpit opens the WS — no matter
    # which path bound the agent.
    route_committed = False
    try:
        await dependencies.session_router.ensure_route(
            thread_id=runtime_authority.thread_id,
            pod_name=binding.agent_hostname,
            pod_uid=binding.pod_uid,
            runtime_generation=runtime_authority.generation,
        )

        # This is the final await before token mint.  The joined DB predicate is
        # the linearization boundary for the route/token response; a stale route
        # is removed with the exact generation + Pod UID and can never target a
        # successor.
        current_binding = await _read_binding()
        if current_binding is None or current_binding.target_key != captured_target:
            await _raise_exact_binding_refusal()
            raise AssertionError("unreachable")
        _require_live_agent(current_binding)

        token, expires_at = dependencies.session_tokens.mint(
            user_id=str(user["id"]),
            thread_id=runtime_authority.thread_id,
            session_identity_fingerprint=identity_fingerprint,
        )

        socket_origin = session_websocket_origin(
            os.environ.get("SESSION_PUBLIC_ORIGIN", ""),
            os.environ.get("SESSION_INGRESS_HOST", "api.example.com"),
        )
        ws_url = f"{socket_origin}/p/{runtime_authority.thread_id}/ws?t={token}"
        response = PinnedConnectionResponse(
            state="ready",
            control_socket="websocket",
            ws_url=ws_url,
            token=token,
            expires_at=expires_at,
            session_runtime_generation=runtime_authority.generation,
            conversation_revision=int(thread.get("conversation_revision") or 0),
        )
        route_committed = True
        return response
    except SessionRouteAuthorityError as exc:
        raise HTTPException(status_code=425, detail="session not ready") from exc
    finally:
        # Exact cleanup covers partial Service/Ingress creation, a failed final
        # DB read, a status/identity race, and token construction failure.  It
        # is deliberately armed only when route mutation begins.
        if not route_committed:
            route_removed = await dependencies.session_router.teardown_route(
                runtime_authority.thread_id,
                expected_namespace=binding.pod_namespace,
                expected_runtime_generation=runtime_authority.generation,
                expected_owner_uid=binding.pod_uid,
            )
            if not route_removed:
                raise RuntimeError("incomplete session route could not be removed")
