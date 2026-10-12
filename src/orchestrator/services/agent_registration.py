"""Agent registration, heartbeat and the fleet admin reads.

Extracted verbatim from ``orchestrator.main`` (R1.B06, lane C, census group
``S_REG``). This is where an agent process acquires — and keeps proving — its
identity, so the port contract's identity rules bite hardest here.

Five properties are load-bearing and moved unchanged:

* **Identity is compared, never re-derived** (§P6). Every authority decision in
  :func:`register_agent` reads the thread row and compares it against the
  registration body: ``same_thread_runtime_authority`` against the authority
  captured *before* the upsert, the planned provision intent's ``pod_name``
  against the presented hostname, and the presented
  ``session_runtime_generation`` against the stored generation. Nothing is
  reconstructed from parts. The thread is deliberately re-read after every
  await that can cross a boot window — five times in one handler — because a
  dedicated pod can start while the row is pinned and register only after an
  operator moved it, and an entry-time check cannot close that window.
* **The bind-boundary fence precedes the hostname upsert.** The lane check runs
  *before* ``register_agent`` writes anything, because its result id may name a
  pre-existing legitimate row: "upsert then delete on refusal" would delete
  another binding through the FK cascade.
* **The exact thread<->agent pair is published in one statement** (§P8).
  ``register_agent`` inserts unbound (``thread_id=None``) unless it is targeting
  an exact same-host restart; the reciprocal pair is written by
  ``bind_registered_persistent_agent``. Publishing ``agents.thread_id`` first
  would create an inverse-only process authority that neither DELETE nor
  retirement can fence.
* **Every fence fails closed with its original shape** (§P7). The 409 codes
  (``pinned_runtime_generation_mismatch``,
  ``pinned_runtime_generation_required``, ``protected_cloud_malformed``,
  ``protected_cloud_not_ready``, ``agent_pod_provision_intent_mismatch``,
  ``pinned_runtime_identity_mismatch``), the 403 runtime-actor bootstrap
  refusals with their ``runtime_actor_denied`` security events, and the 503
  repository-authority refusal all keep their exact status, body and audit
  record. Duplicate persistent registration keeps its outcome: a same-hostname
  restart targets the exact authorized row, any other live owner is a 409.
* **A heartbeat is best-effort about everything except authority.** The grant
  slide, the workspace-activity merge and the job-status backstop each swallow
  their own failures; the ``authority_refused`` result does not.

Collaborators arrive through :class:`AgentRegistrationDependencies`, rebuilt per
invocation by the application. ``bind_registered_persistent_agent`` belongs to
lane A's attach surface and is injected rather than imported (§P2/§P10).

**The access gate belongs to the caller, not to these functions.** Each of them
was the body of a route whose first statement was ``require_internal`` /
``_require_admin``; that statement now lives in
``orchestrator.routers.agent_registration`` (and in the application's
compatibility wrapper), which is where the endpoint-inventory gate scanner reads
it and where the audited policy identity is declared. Every function below
assumes it has already run. Do not call one from an ungated path.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

import httpx
from fastapi import HTTPException, Request

from orchestrator.schemas.agent_registration import PodRuntimeActorRequest
from orchestrator.schemas.agent_runtime import (
    AgentHeartbeat,
    AgentRegistration,
    AgentRegistrationResponse,
)
from orchestrator.services.managed_repository_authority import (
    ManagedRepositoryAuthorityError,
    prepare_thread_repository_authority,
)
from orchestrator.services.runtime_actor import (
    RuntimeActorCredentialError,
    exchange_runtime_actor_pod_bootstrap,
    mint_thread_runtime_actor,
    request_bootstrap_token,
    validate_thread_runtime_actor_bootstrap,
)
from orchestrator.services.session_runtime_admission import (
    protected_cloud_marker_state,
    same_thread_runtime_authority,
    thread_runtime_authority,
    thread_runtime_refusal_detail,
)
from orchestrator.services.stateless_workspace_gate import thread_metadata_object
from shared.run_queue import LANE_PINNED
from shared.session_subagent_batch import (
    SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT,
    SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT_KEY,
    SESSION_SUBAGENT_FANOUT_KEY,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AgentRegistrationDependencies:
    """Collaborators for one registration/heartbeat/admin-read, per invocation.

    Every field is resolved by a main factory at call time rather than captured
    at import. ``store`` and ``gitea_client`` are rebound during ``lifespan``
    and by tests; the guards and the audit sink are names tests patch on the
    application module; and the two import-time flags are callables rather than
    values so a rebound flag still steers the route (port contract §P1).
    """

    store: Any
    gitea_client: Any
    logger: logging.Logger

    # Guards and audit (orchestrator.security.access, bound by main).
    require_internal: Callable[[Request], Awaitable[None]]
    require_admin: Callable[[Request], Awaitable[dict[str, Any]]]
    is_internal_call: Callable[[Request], bool]
    log_security_event: Callable[..., Awaitable[Any]]

    # Import-time flags as callables (§P1).
    completion_commands_enabled: Callable[[], bool]
    require_pinned_status_identity: Callable[[], bool]

    # Runtime admission predicates — main stays their single reader (§P6).
    thread_uses_pinned_execution: Callable[[Any], bool]
    thread_accepts_runtime: Callable[[Any], bool]

    # B04 protected-cloud delivery state.
    protected_cloud_delivery_state: Callable[
        [dict[str, Any], dict[str, Any]], Awaitable[tuple[str, str | None]]
    ]

    # Lane A attach surface — injected, never re-implemented (§P2).
    bind_registered_persistent_agent: Callable[
        [str, str, str | None, str], Awaitable[str | None]
    ]

    # Runtime actor grant liveness (orchestrator.services.runtime_actor).
    slide_thread_grant_on_liveness: Callable[..., Awaitable[Any]]

    # B11 scheduler.
    trigger_dispatch: Callable[[], None]

    # The operator's session fan-out switch, ``(lane) -> bool``
    # (``DeploymentSettings.session_subagent_fanout``), read at every
    # heartbeat of a pinned session (parallel_subagents.md §14.2 P5). Off
    # unless the application binds it.
    session_subagent_fanout: Callable[[str], bool] = lambda _lane: False


def pinned_session_subagent_advertisement(
    result: dict[str, Any],
    *,
    dependencies: AgentRegistrationDependencies,
) -> dict[str, Any]:
    """The fan-out advertisement a heartbeat carries to a pinned session.

    A pinned runtime claims its inputs from Postgres, so no orchestrator
    response accompanies a turn; before this it learned the operator's switch
    only at attach, and turning the lane off never reached a running session
    (parallel_subagents.md §14.1, §14.2 P5). The heartbeat already resolves
    the bound thread, so it carries the same two keys as the pushed pinned
    attach body and the ready workspace payload, evaluated now with the same
    per-lane setting. The agent holds them and applies them at its next turn
    start. Empty for an agent with no bound thread or a non-pinned one; an
    agent that predates the keys ignores them.
    """

    if not result.get("thread_id"):
        return {}
    if not dependencies.thread_uses_pinned_execution(
        {"execution_lane": result.get("execution_lane")}
    ):
        return {}
    return {
        SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT_KEY: (
            SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT
        ),
        SESSION_SUBAGENT_FANOUT_KEY: bool(
            dependencies.session_subagent_fanout(LANE_PINNED)
        ),
    }


async def register_agent(
    request: Request,
    registration: AgentRegistration,
    *,
    dependencies: AgentRegistrationDependencies,
) -> AgentRegistrationResponse:
    """Register a new agent or update existing one. **Internal** (P4b) —
    requires ``X-Internal-Key``. Public ingress also strips this path.

    When an agent starts up, it calls this endpoint to register itself.
    If an agent with the same hostname exists, its pod_ip is updated.

    Returns:
        AgentRegistrationResponse with agent_id and heartbeat_interval_seconds
    """
    postgres_db = dependencies.store
    logger = dependencies.logger
    gitea_client = dependencies.gitea_client

    try:
        runtime_actor_payload: dict[str, Any] | None = None
        registered_runtime_generation: str | None = None
        registered_runtime_attach_token: str | None = None
        dedicated_bootstrap: str | None = None
        register_kwargs = dict(
            config_name=registration.config_name,
            pod_ip=registration.pod_ip,
            hostname=registration.hostname,
            pod_port=registration.pod_port,
            pid=registration.pid,
            agent_mode=registration.agent_mode,
            thread_id=registration.thread_id,
            build_sha=registration.build_sha,
            product_provenance=registration.product_provenance.model_dump(mode="json"),
            pod_uid=registration.pod_uid,
            completion_commands_enabled=dependencies.completion_commands_enabled(),
        )
        if registration.agent_mode == "persistent" and registration.thread_id:
            # The lane check must precede the hostname upsert.  Its result ID
            # may name a pre-existing legitimate row, so "upsert then delete on
            # refusal" can delete another binding through the FK cascade.
            async with postgres_db.thread_advisory_lock(registration.thread_id):
                thread = await postgres_db.get_thread(registration.thread_id)
                registration_authority = thread_runtime_authority(thread)
                if (
                    not dependencies.thread_uses_pinned_execution(thread)
                    or registration_authority is None
                ):
                    # Authoritative bind-boundary fence. A dedicated pod can
                    # start while the row is pinned but register only after an
                    # operator has moved the detached thread to the queue lane;
                    # entry-time checks cannot close that boot window.
                    logger.warning(
                        "register_agent: refusing persistent bind for thread %s "
                        "on execution lane %r before agent upsert",
                        registration.thread_id,
                        thread.get("execution_lane") if thread else None,
                    )
                    raise HTTPException(
                        status_code=409,
                        detail="thread execution lane does not accept persistent agents",
                    )

                try:
                    await prepare_thread_repository_authority(
                        postgres_db, gitea_client, thread
                    )
                except ManagedRepositoryAuthorityError as exc:
                    logger.warning(
                        "register_agent: repository authority unavailable for "
                        "thread %s (%s)",
                        registration.thread_id,
                        exc.code,
                    )
                    raise HTTPException(
                        status_code=503,
                        detail="Workspace repository authority is unavailable",
                    ) from exc

                # Repository authority preparation crosses DB/Gitea awaits.
                # End may land while the dedicated pod is booting; re-read
                # before the hostname upsert can create or rebind an agent row.
                thread = await postgres_db.get_thread(registration.thread_id)
                if not dependencies.thread_uses_pinned_execution(
                    thread
                ) or not same_thread_runtime_authority(thread, registration_authority):
                    raise HTTPException(
                        status_code=409,
                        detail=thread_runtime_refusal_detail(thread),
                    )
                marker = protected_cloud_marker_state(thread_metadata_object(thread))
                if marker == "malformed":
                    raise HTTPException(
                        status_code=409,
                        detail={"code": "protected_cloud_malformed"},
                    )
                if (
                    registration.session_runtime_generation is not None
                    and str(registration.session_runtime_generation)
                    != registration_authority.generation
                ):
                    raise HTTPException(
                        status_code=409,
                        detail={"code": "pinned_runtime_generation_mismatch"},
                    )
                if (
                    marker == "off"
                    and dependencies.require_pinned_status_identity()
                    and registration.session_runtime_generation is None
                ):
                    raise HTTPException(
                        status_code=409,
                        detail={"code": "pinned_runtime_generation_required"},
                    )
                if marker == "on":
                    if (
                        registration.session_runtime_generation is None
                        or str(registration.session_runtime_generation)
                        != registration_authority.generation
                    ):
                        raise HTTPException(
                            status_code=409,
                            detail={
                                "code": "pinned_runtime_generation_required",
                                "message": (
                                    "Protected runtime registration requires its "
                                    "exact session generation."
                                ),
                            },
                        )
                    (
                        protected_state,
                        protected_code,
                    ) = await dependencies.protected_cloud_delivery_state(
                        thread, thread_metadata_object(thread)
                    )
                    if protected_state != "ready":
                        raise HTTPException(
                            status_code=409,
                            detail={
                                "code": "protected_cloud_not_ready",
                                "state": protected_state,
                                "reason": protected_code,
                            },
                        )

                # Defense-in-depth against the double-provisioning race
                # (knowledge-base/knowledge/issues/persistent_thread_double_provisioning_race.md):
                # refuse a different live owner before the hostname upsert can
                # pause its jobs, change its binding, or delete it through an
                # attempted loser rollback. A same-host restart targets that
                # exact authorized row; every genuinely new binding inserts a
                # fresh row because hostname is not unique or an ownership
                # credential.
                existing_id = thread.get("agent_id") if thread else None
                expected_upsert_id: str | None = None
                if existing_id:
                    existing = await postgres_db.get_agent(str(existing_id))
                    thread = await postgres_db.get_thread(registration.thread_id)
                    if not same_thread_runtime_authority(
                        thread, registration_authority
                    ):
                        raise HTTPException(
                            status_code=409,
                            detail=thread_runtime_refusal_detail(thread),
                        )
                    existing_status = (existing or {}).get("status")
                    if not existing:
                        logger.warning(
                            "register_agent: thread %s references missing agent %s; "
                            "refusing before agent upsert.",
                            registration.thread_id,
                            existing_id,
                        )
                        raise HTTPException(
                            status_code=409,
                            detail="thread agent ownership is inconsistent",
                        )
                    if (
                        registration.hostname
                        and existing.get("hostname") == registration.hostname
                    ):
                        expected_upsert_id = str(existing_id)
                    elif existing_status not in ("offline", "failed"):
                        logger.warning(
                            "register_agent: duplicate persistent registration "
                            "for thread %s; winner=%s hostname=%r — refusing "
                            "before agent upsert.",
                            registration.thread_id,
                            existing_id,
                            registration.hostname,
                        )
                        raise HTTPException(
                            status_code=409,
                            detail="thread already bound to another live agent",
                        )

                # A thread-bound pod receives actor identity only after proving
                # the unique bootstrap injected into that pod at provision
                # time. The shared internal key is deliberately insufficient.
                try:
                    bootstrap = request_bootstrap_token(request)
                except RuntimeActorCredentialError as exc:
                    await dependencies.log_security_event(
                        postgres_db,
                        request=request,
                        event_type="runtime_actor_denied",
                        resource_type="runtime_actor_bootstrap",
                        resource_id=registration.thread_id,
                        detail=exc.code,
                    )
                    raise HTTPException(
                        status_code=403,
                        detail="Runtime actor bootstrap is malformed or duplicated.",
                    ) from exc
                if bootstrap is not None:
                    try:
                        await validate_thread_runtime_actor_bootstrap(
                            postgres_db,
                            thread_id=registration.thread_id,
                            bootstrap_token=bootstrap,
                        )
                    except RuntimeActorCredentialError as exc:
                        await dependencies.log_security_event(
                            postgres_db,
                            request=request,
                            event_type="runtime_actor_denied",
                            resource_type="runtime_actor_bootstrap",
                            resource_id=registration.thread_id,
                            detail=exc.code,
                        )
                        raise HTTPException(
                            status_code=403,
                            detail="Runtime actor bootstrap is invalid or expired.",
                        ) from exc
                    dedicated_bootstrap = bootstrap

                thread = await postgres_db.get_thread(registration.thread_id)
                if not dependencies.thread_uses_pinned_execution(
                    thread
                ) or not same_thread_runtime_authority(thread, registration_authority):
                    raise HTTPException(
                        status_code=409,
                        detail=thread_runtime_refusal_detail(thread),
                    )

                planned_provision = await postgres_db.fetchrow(
                    "SELECT attempt_id,pod_name,namespace FROM "
                    "thread_agent_pod_provision_intents "
                    "WHERE thread_id=$1::uuid AND runtime_generation=$2::uuid "
                    "AND status='planned'",
                    registration.thread_id,
                    registration_authority.generation,
                )
                if planned_provision is not None:
                    if (
                        not registration.hostname
                        or not registration.pod_uid
                        or str(planned_provision["pod_name"])
                        != str(registration.hostname)
                        or not await postgres_db.publish_pinned_agent_pod_provision_intent(
                            registration.thread_id,
                            expected_runtime_generation=(
                                registration_authority.generation
                            ),
                            attempt_id=str(planned_provision["attempt_id"]),
                            pod_name=str(registration.hostname),
                            pod_uid=str(registration.pod_uid),
                            namespace=str(planned_provision["namespace"] or ""),
                        )
                    ):
                        raise HTTPException(
                            status_code=409,
                            detail={"code": "agent_pod_provision_intent_mismatch"},
                        )

                # The exact thread<->agent pair is published atomically below.
                # Publishing ``agents.thread_id`` first creates an inverse-only
                # process authority that direct DELETE/retirement cannot fence.
                register_kwargs["thread_id"] = (
                    registration.thread_id if expected_upsert_id is not None else None
                )
                result = await postgres_db.register_agent(
                    **register_kwargs,
                    expected_agent_id=expected_upsert_id,
                    insert_only=expected_upsert_id is None,
                )
                new_id = str(result["agent_id"])
                if expected_upsert_id is not None and new_id != expected_upsert_id:
                    logger.warning(
                        "register_agent: exact persistent restart target changed "
                        "for thread %s; expected=%s got=%s",
                        registration.thread_id,
                        expected_upsert_id,
                        new_id,
                    )
                    raise HTTPException(
                        status_code=409,
                        detail="persistent agent identity changed during registration",
                    )
                registered_runtime_attach_token = (
                    await dependencies.bind_registered_persistent_agent(
                        registration.thread_id,
                        new_id,
                        str(existing_id) if existing_id else None,
                        registration_authority.generation,
                    )
                )
                if registered_runtime_attach_token is None:
                    logger.warning(
                        "register_agent: final pinned-lane bind lost for "
                        "thread %s (agent=%s)",
                        registration.thread_id,
                        new_id,
                    )
                    current = await postgres_db.get_thread(registration.thread_id)
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            thread_runtime_refusal_detail(current)
                            if not dependencies.thread_accepts_runtime(current)
                            else "thread execution lane changed before agent binding"
                        ),
                    )
                if dedicated_bootstrap is not None:
                    try:
                        runtime_actor = await mint_thread_runtime_actor(
                            postgres_db,
                            thread_id=registration.thread_id,
                            agent_id=new_id,
                        )
                    except RuntimeActorCredentialError as exc:
                        await dependencies.log_security_event(
                            postgres_db,
                            request=request,
                            event_type="runtime_actor_denied",
                            resource_type="runtime_actor_binding",
                            resource_id=registration.thread_id,
                            detail=exc.code,
                        )
                        raise HTTPException(
                            status_code=403,
                            detail="Runtime actor binding is no longer current.",
                        ) from exc
                    runtime_actor_payload = runtime_actor.to_payload()
                    current = await postgres_db.get_thread(registration.thread_id)
                    if not same_thread_runtime_authority(
                        current, registration_authority
                    ):
                        raise HTTPException(
                            status_code=409,
                            detail=thread_runtime_refusal_detail(current),
                        )
                final_registered_thread = await postgres_db.get_thread(
                    registration.thread_id
                )
                if (
                    not same_thread_runtime_authority(
                        final_registered_thread, registration_authority
                    )
                    or str(final_registered_thread.get("agent_id") or "") != new_id
                    or str(final_registered_thread.get("runtime_attach_token") or "")
                    != registered_runtime_attach_token
                ):
                    raise HTTPException(
                        status_code=409,
                        detail="Pinned runtime changed before registration completed",
                    )
                registered_runtime_generation = registration_authority.generation
        else:
            result = await postgres_db.register_agent(**register_kwargs)
        return AgentRegistrationResponse(
            **result,
            runtime_actor=runtime_actor_payload,
            session_runtime_generation=registered_runtime_generation,
            session_runtime_attach_token=registered_runtime_attach_token,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def agent_pod_runtime_actor(
    request: Request,
    agent_id: str,
    body: PodRuntimeActorRequest,
    *,
    dependencies: AgentRegistrationDependencies,
) -> dict[str, Any]:
    """Bind a warm pool pod's thread-less bootstrap to the session it just got.
    **Internal** (P4b) — requires ``X-Internal-Key`` *and* the pod bootstrap
    header. Ingress strips this path.

    A dedicated session pod does this inside ``/api/agents/register``, where
    the thread is known at provision time. A pool pod cannot: it registers
    thread-less and is handed a session later over ``/session/attach``, and K8s
    env is not patchable on a running pod. Without this route the pod runs the
    session with no actor identity at all and every sensitive knowledge write
    fails ``missing_credential`` — the failure BP-05's live gate hit.

    The shared internal key is deliberately insufficient here, exactly as it is
    at registration: the caller must also present the unique bootstrap injected
    into its own pod, and the thread binding is read from the ``agents`` row
    rather than believed from the body.
    """
    postgres_db = dependencies.store

    try:
        bootstrap = request_bootstrap_token(request)
    except RuntimeActorCredentialError as exc:
        await dependencies.log_security_event(
            postgres_db,
            request=request,
            event_type="runtime_actor_denied",
            resource_type="runtime_actor_pod_bootstrap",
            resource_id=body.thread_id,
            detail=exc.code,
        )
        raise HTTPException(
            status_code=403,
            detail="Runtime actor bootstrap is malformed or duplicated.",
        ) from exc
    if bootstrap is None:
        raise HTTPException(
            status_code=403,
            detail="Runtime actor pod bootstrap is required.",
        )
    try:
        runtime_actor = await exchange_runtime_actor_pod_bootstrap(
            postgres_db,
            agent_id=agent_id,
            thread_id=body.thread_id,
            bootstrap_token=bootstrap,
        )
    except RuntimeActorCredentialError as exc:
        await dependencies.log_security_event(
            postgres_db,
            request=request,
            event_type="runtime_actor_denied",
            resource_type="runtime_actor_pod_bootstrap",
            resource_id=body.thread_id,
            detail=exc.code,
        )
        raise HTTPException(
            status_code=403,
            detail="Runtime actor pod bootstrap is invalid or not bound.",
        ) from exc
    return {"runtime_actor": runtime_actor.to_payload()}


async def agent_heartbeat(
    request: Request,
    agent_id: str,
    heartbeat: AgentHeartbeat,
    *,
    dependencies: AgentRegistrationDependencies,
) -> dict[str, Any]:
    """Update agent heartbeat and status. **Internal** (P4b) — requires
    ``X-Internal-Key``. Ingress strips this path.

    Agents call this every 60 seconds to report their status.
    The orchestrator uses this to track agent health and current job state.
    """
    postgres_db = dependencies.store
    logger = dependencies.logger

    try:
        metrics = dict(heartbeat.metrics or {})
        if heartbeat.graph_progress is not None:
            metrics["graph_progress"] = heartbeat.graph_progress

        # Surface auxiliary-model health (aux Phase 2): the agent folds a
        # compact AuxHealth summary into metrics.aux; persist its degraded flag
        # on the agent row so the admin view can badge it. Absent on older
        # agent builds / before the aux LLM is wired → None, which leaves the
        # persisted flag untouched.
        aux = metrics.get("aux")
        aux_degraded = bool(aux.get("degraded")) if isinstance(aux, dict) else None
        result = await postgres_db.heartbeat(
            agent_id=agent_id,
            status=heartbeat.status,
            current_job_id=heartbeat.current_job_id,
            metrics=metrics if metrics else None,
            aux_degraded=aux_degraded,
            session_runtime_generation=heartbeat.session_runtime_generation,
            session_runtime_attach_token=heartbeat.session_runtime_attach_token,
            require_pinned_identity=dependencies.require_pinned_status_identity(),
        )
        if result is None:
            raise HTTPException(status_code=404, detail=f"Agent '{agent_id}' not found")
        if result.get("authority_refused"):
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "pinned_runtime_identity_mismatch",
                    "message": (
                        "The heartbeat does not own the current pinned session runtime."
                    ),
                },
            )

        # If agent transitioned to ready, trigger the dispatcher
        # (will be wired up in the dispatcher task). Use effective_status —
        # the orchestrator may preserve 'draining' against an agent-reported
        # 'ready', and we must not dispatch in that case.
        prev_status = result.get("previous_status")
        effective_status = result.get("effective_status", heartbeat.status)
        if (
            prev_status
            and prev_status != effective_status
            and effective_status == "ready"
        ):
            logger.info(f"Agent {agent_id} transitioned {prev_status} → ready")
            dependencies.trigger_dispatch()

        # A heartbeat from a thread-bound runtime IS the liveness signal that
        # licenses extending its actor grant. The grant's lifetime is an IDLE
        # timeout, but it only ever slid inside a refresh — and the runtime
        # only refreshes to make a PRIVILEGED call, so a quiet-but-alive
        # officer (6ce5bc4c: 24h of wake/read-SITREP/sleep, no privileged call)
        # hit the wall while running. Best-effort by construction: never fail
        # or meaningfully delay a heartbeat over it. The service throttles the
        # write to the grant's second half, so this is a no-write read on the
        # overwhelming majority of beats.
        # knowledge/issues/officer_runtime_grant_expires_after_24h_and_dies_silently.md
        _hb_thread_id = result.get("thread_id")
        if _hb_thread_id:
            try:
                await dependencies.slide_thread_grant_on_liveness(
                    postgres_db,
                    str(_hb_thread_id),
                    agent_id=agent_id,
                    session_runtime_generation=result.get("session_runtime_generation"),
                    session_runtime_attach_token=result.get(
                        "session_runtime_attach_token"
                    ),
                )
            except Exception as exc:
                logger.warning(
                    f"Runtime actor liveness slide failed for thread "
                    f"{_hb_thread_id}: {exc}"
                )

        # Track workspace container activity for idle suspension
        if heartbeat.current_job_id and heartbeat.status == "working":
            try:
                await postgres_db.merge_workspace_container_context(
                    heartbeat.current_job_id,
                    {"last_activity": datetime.now(timezone.utc).isoformat()},
                    existing_only=True,
                )
            except Exception:
                pass  # Non-critical — don't fail heartbeat

        # Report the CURRENT status of the job the agent thinks it is running.
        # The heartbeat is the only channel that already runs on the right
        # cadence, and it was one-directional: the agent asserted liveness and
        # learned nothing back. So when a job was terminated out-of-band, the
        # agent kept executing — 21 minutes and 45 LLM calls in the observed
        # case — and only found out when its VM was collected underneath it.
        #
        # A push stop signal already exists and stays the fast path; this is the
        # BACKSTOP that catches the 13+ call sites which can write a terminal
        # status without sending one.
        # knowledge-base/knowledge/issues/transient_db_error_hard_fails_job_and_destroys_vm.md (Defect 3)
        job_status: str | None = None
        pending_guidance: list[dict[str, Any]] | None = None
        queued_replies: list[dict[str, Any]] | None = None
        if heartbeat.current_job_id:
            try:
                _hb_job = await postgres_db.get_job(heartbeat.current_job_id)
                if _hb_job:
                    job_status = _hb_job.get("status")
                    # Supervisor guidance (P1-A) rides the same row read at
                    # zero marginal DB cost. Contract: a LIST whenever the
                    # row was read — an empty list is the prune signal for
                    # the agent's inbox; None (lookup failed / older
                    # orchestrator) means "no information, keep your inbox".
                    _hb_ctx = _hb_job.get("context") or {}
                    if isinstance(_hb_ctx, str):
                        try:
                            _hb_ctx = json.loads(_hb_ctx)
                        except json.JSONDecodeError:
                            _hb_ctx = {}
                    _hb_pg = _hb_ctx.get("pending_guidance")
                    pending_guidance = _hb_pg if isinstance(_hb_pg, list) else []
                    # Queued (non-urgent) replies ride along on the same read,
                    # same contract. The worker used to learn about these only
                    # at a tactical->strategic boundary, which stops being a
                    # usable cadence as tactical phases grow — at three phases
                    # a reply sent during review would never be delivered at
                    # all. The agent now drains them at its own natural breaks
                    # (a completed todo), so it needs them locally.
                    _hb_qr = _hb_ctx.get("queued_replies")
                    queued_replies = _hb_qr if isinstance(_hb_qr, list) else []
            except Exception:
                # Never fail a heartbeat over this — a missing job_status just
                # degrades to the old push-only behaviour.
                job_status = None
                pending_guidance = None
                queued_replies = None

        # A pinned session's fan-out switch (P5). Best-effort like the rest:
        # an absent pair means "no change" to the agent, never "off".
        try:
            advertisement = pinned_session_subagent_advertisement(
                result, dependencies=dependencies
            )
        except Exception as exc:
            advertisement = {}
            logger.warning(
                f"Session fan-out advertisement skipped for agent {agent_id}: {exc}"
            )

        # Surface orchestrator-set intents (drain, version-upgrade hints)
        # so the agent can react on the next heartbeat tick. Keeping the
        # legacy {"status": "ok"} key for back-compat with older agent
        # builds that don't read intents.
        return {
            "status": "ok",
            "intents": result.get("intents") or {},
            "job_status": job_status,
            "pending_guidance": pending_guidance,
            "queued_replies": queued_replies,
            **advertisement,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def list_agents(
    request: Request,
    status: str | None = None,
    limit: int = 100,
    *,
    dependencies: AgentRegistrationDependencies,
) -> list[dict[str, Any]]:
    """List all registered agents. **Admin only** (G4) — exposes pod IPs,
    hostnames, and full fleet metadata. Non-admins must use
    `/api/me/active-jobs` for a stripped, per-user projection of their
    in-flight work.

    Args:
        status: Optional status filter (booting, ready, working, completed, failed, offline)
        limit: Maximum agents to return
    """
    postgres_db = dependencies.store

    try:
        return await postgres_db.list_agents(status=status, limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def get_agent(
    request: Request,
    agent_id: str,
    *,
    dependencies: AgentRegistrationDependencies,
) -> dict[str, Any]:
    """Get agent details by ID. **Admin only** (G4)."""
    postgres_db = dependencies.store

    try:
        agent = await postgres_db.get_agent(agent_id)
        if not agent:
            raise HTTPException(status_code=404, detail=f"Agent '{agent_id}' not found")
        return agent
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def get_agent_system_info(
    request: Request,
    agent_id: str,
    *,
    dependencies: AgentRegistrationDependencies,
) -> dict[str, Any]:
    """Proxy system info request to an agent's /system/info endpoint.
    **Admin only** (G4) — proxies host-level CPU/memory/process/port
    inventory from the agent container.

    Returns CPU, memory, disk, processes, listening ports, and network
    connections from the agent's container.
    """
    postgres_db = dependencies.store

    try:
        agent = await postgres_db.get_agent(agent_id)
        if not agent:
            raise HTTPException(status_code=404, detail=f"Agent '{agent_id}' not found")

        if agent["status"] == "offline":
            raise HTTPException(status_code=400, detail="Agent is offline")

        pod_ip = agent.get("pod_ip")
        if not pod_ip:
            raise HTTPException(
                status_code=400, detail="Agent has no pod IP configured"
            )

        pod_port = agent.get("pod_port", 8001)
        agent_url = f"http://{pod_ip}:{pod_port}/system/info"

        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(agent_url)

        if response.status_code != 200:
            raise HTTPException(
                status_code=502,
                detail=f"Agent returned {response.status_code}: {response.text}",
            )

        return response.json()

    except HTTPException:
        raise
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to connect to agent: {str(e)}",
        ) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def delete_agent(
    request: Request,
    agent_id: str,
    *,
    dependencies: AgentRegistrationDependencies,
) -> dict[str, str]:
    """Deregister an agent. **Admin or internal-key** (G4).

    Used by the cockpit's agent-list admin tool, and by agents
    deregistering on graceful shutdown via X-Internal-Key so clean exits
    stop aging into missed-heartbeat corpses (Track B will move them to a
    bearer-credentialled path). The heartbeat timeout (3min) remains the
    backstop for crashes.
    """
    postgres_db = dependencies.store

    try:
        success = await postgres_db.delete_agent(agent_id)
        if not success:
            raise HTTPException(status_code=404, detail=f"Agent '{agent_id}' not found")
        return {"status": "deleted"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


__all__ = [
    "AgentRegistrationDependencies",
    "agent_heartbeat",
    "agent_pod_runtime_actor",
    "delete_agent",
    "get_agent",
    "get_agent_system_info",
    "list_agents",
    "register_agent",
]
