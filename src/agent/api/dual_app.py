"""Dual-mode FastAPI application for Universal Agent.

Merges worker (job dispatch) and persistent (interactive session) routes
into a single application with a state machine:

Pod-per-task mode (default, for K8s):
    IDLE  ──/job/start──>  WORKING  ──job done──>  EXIT
    IDLE  ──/session/attach──>  SESSION  ──detach──>  EXIT

Loop mode (AGENT_LOOP=1, enable with --loop):
    IDLE  ──/job/start──>  WORKING  ──job done──>  IDLE
    IDLE  ──/session/attach──>  SESSION  ──detach──>  IDLE

Start with: python agent.py --mode dual --port 8001
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException, Request, WebSocket
from fastapi.responses import JSONResponse

from agent.api.models import (
    ErrorResponse,
    HealthResponse,
    HealthStatus,
    JobCancelByOrchestratorRequest,
    JobResumeRequest,
    JobStartRequest,
    JobStartResponse,
    PinnedJobRecipient,
    PinnedSessionRecipient,
    ReadyResponse,
    pinned_job_recipient_matches,
    pinned_session_recipient_matches,
)
from agent.api._session_auth import validate_session_token as _validate_session_token
from agent.api.job_stream import close_job_stream as _close_stream
from agent.api.orchestrator_client import (
    OrchestratorClient,
    create_orchestrator_client_from_env,
)
from agent.agent import UniversalAgent
from shared.runtime.core.loader import resolve_config_path
from agent.core.phase import push_evidence_snapshot
from shared.runtime.core.workspace_backend import completion_error_payload
from shared.workspace_contract import (
    WorkspaceContractError,
    validate_worker_workspace_projection,
)
from shared.subagent_lifecycle import (
    SubagentLifecycleError,
)
from shared.pinned_session_identity import (
    PINNED_SESSION_READY_IDENTITY_CONTRACT,
    pinned_session_ready_identity_fingerprint,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pod state machine
# ---------------------------------------------------------------------------


class PodState(str, Enum):
    IDLE = "idle"
    WORKING = "working"
    SESSION = "session"


_pod_state: PodState = PodState.IDLE
_state_lock = asyncio.Lock()

# Shared singleton layer
_agent: Optional[UniversalAgent] = None
_config_path: Optional[str] = None
_orchestrator_client: Optional[OrchestratorClient] = None
_heartbeat_task: Optional[asyncio.Task] = None
_session_attach_task: Optional[asyncio.Task] = None
# One-way proof boundary for a delivered warm attach.  Before ``setup_started``
# flips, no PersistentSession, backend, workspace request, writer, or side task
# has been constructed.  Once it flips it is never reset for this exact
# G/attach claim: every failure must produce real process-zero proof instead.
_session_attach_claim: Optional[dict[str, Any]] = None
_shutdown_requested = False
_started_at: Optional[datetime] = None
_pending_exit_task: Optional[asyncio.Task] = None

# Worker-mode state (imported from app.py at runtime to avoid circular deps)
_current_job_id: Optional[str] = None
_current_job_task: Optional[asyncio.Task] = None
_stop_requested: asyncio.Event = asyncio.Event()
_stop_reason: Optional[str] = None
_stop_completed: asyncio.Event = asyncio.Event()

_PINNED_RECIPIENT_MISMATCH = {"code": "pinned_recipient_mismatch"}
_JOB_STREAM_CLOSE_TIMEOUT_SECONDS = 60.0


async def _close_job_stream(streaming_gen: Any, *, job_id: str) -> None:
    """Finalize through one close owner with this app's timeout and logger."""
    await _close_stream(
        streaming_gen,
        job_id=job_id,
        timeout_seconds=_JOB_STREAM_CLOSE_TIMEOUT_SECONDS,
        logger=logger,
    )


def _require_pinned_job_recipient(
    recipient: Optional[PinnedJobRecipient],
    *,
    job_id: Optional[str],
) -> None:
    """Fail closed unless this exact registered process owns the mutation."""

    client = _orchestrator_client
    if not pinned_job_recipient_matches(
        recipient,
        agent_id=getattr(client, "agent_id", None),
        pod_uid=os.environ.get("POD_UID"),
        process_generation=getattr(client, "dispatch_process_generation", None),
        job_id=job_id,
    ):
        raise HTTPException(status_code=409, detail=_PINNED_RECIPIENT_MISMATCH)


def _pinned_session_recipient_capable() -> bool:
    return bool(
        getattr(_orchestrator_client, "agent_id", None)
        and str(
            getattr(_orchestrator_client, "dispatch_process_generation", None) or ""
        ).strip()
    )


def _pinned_session_recipient_refusal(
    request: Any,
    *,
    expected_thread_id: str,
) -> JSONResponse | None:
    recipient = request.get("_recipient") if isinstance(request, dict) else None
    actual_pod_uid = str(os.environ.get("POD_UID") or "").strip() or None
    exact_contract = bool(
        isinstance(request, dict)
        and type(request.get("pinned_runtime_generation_contract")) is int
        and request["pinned_runtime_generation_contract"] == 1
    )
    if recipient is None and not (
        actual_pod_uid or (exact_contract and _pinned_session_recipient_capable())
    ):
        return None
    try:
        parsed = PinnedSessionRecipient.model_validate(recipient)
    except (TypeError, ValueError):
        parsed = None
    if not pinned_session_recipient_matches(
        parsed,
        thread_id=expected_thread_id,
        agent_id=getattr(_orchestrator_client, "agent_id", None),
        pod_uid=actual_pod_uid,
        process_generation=getattr(
            _orchestrator_client, "dispatch_process_generation", None
        ),
    ):
        return JSONResponse(
            {"error": "recipient_authority_mismatch", "retryable": True},
            status_code=503,
            headers={"Retry-After": "5"},
        )
    return None


def _request_stop(reason: str) -> None:
    global _stop_reason
    if _stop_reason == "cancel" and reason == "pause":
        return
    _stop_reason = reason
    _stop_completed.clear()
    _stop_requested.set()


def _clear_stop() -> None:
    global _stop_reason
    _stop_reason = None
    _stop_requested.clear()
    _stop_completed.clear()


# ---------------------------------------------------------------------------
# Metrics helpers (shared by both modes)
# ---------------------------------------------------------------------------


def _get_agent_metrics() -> Optional[Dict[str, Any]]:
    metrics: Dict[str, Any] = {}
    try:
        graph_progress = (
            _agent._tool_context.get_graph_progress()
            if _agent is not None and _agent._tool_context is not None
            else None
        )
        if graph_progress is not None:
            metrics["graph_progress"] = graph_progress
    except Exception:
        pass

    try:
        import psutil

        proc = psutil.Process()
        metrics.update(
            {
                "memory_mb": round(proc.memory_info().rss / 1_048_576, 1),
                "cpu_percent": proc.cpu_percent(interval=0),
            }
        )
    except Exception:
        pass

    # Contained memory-store failures (deadlock containment). Best-effort;
    # never fail heartbeat.
    memory = _memory_health_for_heartbeat()
    if memory is not None:
        metrics["memory"] = memory

    return metrics or None


def _canonical_optional_runtime_uuid(value: Any) -> str | None:
    if value is None or value == "":
        return None
    try:
        return str(UUID(str(value)))
    except (TypeError, ValueError):
        raise ValueError("malformed runtime identity") from None


def _same_session_attach_claim(
    claim: dict[str, Any],
    *,
    require_setup_started: bool | None = None,
) -> bool:
    """Match the exact in-process attach claim, including its one-way latch."""

    if _session_attach_claim is not claim:
        return False
    if require_setup_started is not None and (
        claim.get("setup_started") is not require_setup_started
    ):
        return False
    return True


async def _release_unstarted_session_attach(claim: dict[str, Any]) -> bool:
    """Retry one pre-setup attach abort until its exact G rotation is proven."""

    global _pod_state, _session_attach_claim, _session_attach_task

    import agent.api.persistent_app as pa

    if not _same_session_attach_claim(claim, require_setup_started=False):
        return False
    receipt = {
        "thread_id": claim["thread_id"],
        "session_runtime_generation": claim["session_runtime_generation"],
        "session_runtime_attach_token": claim["session_runtime_attach_token"],
        "agent_pod_uid": claim["agent_pod_uid"],
        "local_runtime_quiesced": True,
        "local_quiescence_protocol": "agent_attach_not_started_v1",
        "workspace_generation": claim.get("workspace_generation"),
        "workspace_runtime_incarnation": claim.get("workspace_runtime_incarnation"),
    }
    pa._orchestrator_client = _orchestrator_client
    if not pa._retain_failed_attach_release_receipt(receipt):
        logger.error(
            "Pre-setup attach abort for thread %s conflicts with a retained proof",
            claim["thread_id"],
        )
        return False
    confirmed = await pa._release_failed_attach_receipt_until_confirmed(
        claim["thread_id"],
        runtime_generation=claim["session_runtime_generation"],
        runtime_attach_token=claim["session_runtime_attach_token"],
    )
    if not confirmed:
        return False

    cleared = False
    async with _state_lock:
        if _pod_state is PodState.SESSION and _same_session_attach_claim(
            claim, require_setup_started=False
        ):
            if _orchestrator_client is not None:
                _orchestrator_client.clear_runtime_actor()
                _orchestrator_client.clear_session_runtime_identity(
                    expected_generation=claim["session_runtime_generation"],
                    expected_attach_token=claim["session_runtime_attach_token"],
                )
            _pod_state = PodState.IDLE
            _session_attach_claim = None
            cleared = True
    if not cleared:
        # The DB release was exact, but a successor already owns the local
        # latch. Never clear its client identity or advertise this pod idle.
        return True
    if _session_attach_task is asyncio.current_task():
        _session_attach_task = None
    return True


def _memory_health_for_heartbeat() -> Optional[Dict[str, Any]]:
    """Contained memory-store failure counters for the heartbeat.

    Returns None while every counter is zero so the healthy common case adds
    no payload (and the orchestrator persists nothing new).
    """
    try:
        from shared.runtime.services.recall_store import memory_health

        return memory_health.snapshot()
    except Exception:
        return None


def _get_heartbeat_status() -> str:
    if _shutdown_requested:
        return "draining"
    if _agent is None:
        return "booting"
    if _pod_state == PodState.WORKING:
        return "working"
    if _pod_state == PodState.SESSION:
        return "session"
    status = _agent.get_status()
    if not status.get("initialized"):
        return "booting"
    return "ready"


def _get_current_job_id() -> Optional[str]:
    return _current_job_id


# Drain intent flag — set the first time the orchestrator's heartbeat
# response carries ``intents.should_drain=true``. Idle workers exit
# immediately; busy workers expose the flag to the graph so it can
# freeze with ``freeze_data.type="version_upgrade"`` at the next phase
# boundary (Continue-as-New); attached sessions defer until the loop
# parks, then drain-suspend (delegated to persistent_app).
_drain_intent_received: bool = False
_drain_intent_handled: bool = False
_drain_deferred_logged: bool = False


def is_drain_requested() -> bool:
    """Public helper for the worker graph to check drain intent.

    The graph uses this at phase boundaries to decide whether to freeze
    with ``version_upgrade`` instead of continuing into the next phase.
    """
    return _drain_intent_received


# Statuses that mean "this job is no longer ours to run". Deliberately a
# DENY-list: an unrecognised or new status leaves the run alone (fail-open),
# because wrongly stopping a healthy run is worse than a late stop, and the
# push signal remains the fast path either way. Faithful port of the app.py
# backstop — same statuses, same stop mechanism.
_PREEMPTED_JOB_STATUSES = {
    "failed": "cancel",
    "cancelled": "cancel",
    "paused": "pause",
}


def _check_job_preempted(response: Dict[str, Any]) -> None:
    """Stop the run if the orchestrator says our job was taken away.

    ``job_status`` in the heartbeat response is the DB status of the job
    THIS pod reported as current (the orchestrator looks it up from
    ``heartbeat.current_job_id``). When an out-of-band steer / cockpit
    pause / cancel / manual flip moves the row to a denied status, no push
    stop signal reaches the pod — without this backstop it runs to natural
    END as an orphan, double-writing the shared checkpoint thread
    (``thread_id = job_id``) against its replacement and pinning
    single-slot pools at 'working'.

    Reuses the exact machinery /job/cancel and /job/pause drive: the
    streaming loop breaks after the current node and ``_complete_stop()``
    resets the pod to IDLE. Idempotent — once ``_stop_requested`` is set,
    further heartbeats no-op. The agent's own orderly completion can't
    trip this: both completion paths null ``_current_job_id`` before
    ``report_completion``, so by the time its own report flips the row the
    current-job guard already blocks.
    knowledge-base/knowledge/issues/officer_blind_reads_and_worker_bureaucracy.md (P0-D)
    """
    job_id = _current_job_id
    if _pod_state != PodState.WORKING or not job_id or _stop_requested.is_set():
        return
    if not isinstance(response, dict):
        return
    job_status = response.get("job_status")
    reason = _PREEMPTED_JOB_STATUSES.get(job_status)
    if reason is None:
        # Includes job_status=None (older orchestrator, or the lookup failed) —
        # degrade to the previous push-only behaviour rather than guessing.
        return
    logger.warning(
        "Job %s is '%s' on the orchestrator but this agent is still running it "
        "— stopping (reason=%s). The job was terminated out-of-band; work since "
        "then was not attributable to it.",
        job_id,
        job_status,
        reason,
    )
    _request_stop(reason)


# Supervisor-guidance inbox (P1-A of
# knowledge-base/knowledge/issues/officer_blind_reads_and_worker_bureaucracy.md): the
# non-destructive steer lane. The heartbeat response carries the job's
# ``context.pending_guidance`` (entries ``{id, text, source, created_at}``);
# the graph renders the inbox as a transient [SUPERVISOR GUIDANCE] block each
# LLM turn (src/graph.py) and acks rendered entries so the orchestrator moves
# them to ``context.consumed_replies``. Once the orchestrator stops sending an
# id the inbox prunes it. Worst-case delivery latency = one heartbeat interval
# (set by the orchestrator at registration, currently 60s) + the time to the
# worker's next LLM turn. Entries survive pod death before the ack — still
# pending in job context, so the successor pod gets them redelivered.
_guidance_inbox: Dict[str, List[Dict[str, Any]]] = {}

# Queued-reply inbox — the OTHER steering lane, carried on the same heartbeat
# under the same contract. These are non-urgent messages ("see this when you
# surface"), as opposed to guidance ("act on this now"). They used to be read
# straight from the DB and delivered only at a tactical->strategic boundary,
# which stops being a usable cadence once tactical phases grow: at three phases
# per job there is exactly one such boundary, and a reply sent during the
# review phase would never be delivered at all. The graph now drains this inbox
# at source-aware cadence: completed child events on every tool-node pass;
# human/operator mail at the agent's own natural breaks — a completed todo —
# with a wall-clock floor so a stuck agent still sees it.
_reply_inbox: Dict[str, List[Dict[str, Any]]] = {}


def get_pending_guidance(job_id: str) -> List[Dict[str, Any]]:
    """Public helper for the worker graph: pending guidance for ``job_id``."""
    return list(_guidance_inbox.get(job_id, []))


def get_queued_replies(job_id: str) -> List[Dict[str, Any]]:
    """Public helper for the worker graph: queued replies for ``job_id``."""
    return list(_reply_inbox.get(job_id, []))


def ack_guidance(
    job_id: str,
    guidance_ids: Optional[List[str]] = None,
    reply_threads: Optional[List[str]] = None,
    reply_keys: Optional[List[str]] = None,
) -> None:
    """Fire-and-forget ack: mark guidance/queued replies consumed on the orchestrator.

    Best-effort by design — on failure the entries stay in
    ``context.pending_guidance``/``context.queued_replies`` and are simply
    redelivered (at-least-once). Never blocks the graph.
    """
    client = _orchestrator_client
    if client is None or not (guidance_ids or reply_threads or reply_keys):
        return

    async def _send() -> None:
        try:
            kwargs: Dict[str, List[str]] = {}
            if guidance_ids is not None:
                kwargs["guidance_ids"] = list(guidance_ids)
            if reply_threads is not None:
                kwargs["reply_threads"] = list(reply_threads)
            if reply_keys is not None:
                kwargs["reply_keys"] = list(reply_keys)
            await client.ack_job_guidance(job_id, **kwargs)
        except Exception as e:
            logger.debug(f"Guidance ack for job {job_id} failed (will redeliver): {e}")

    try:
        asyncio.get_running_loop().create_task(_send())
    except RuntimeError:
        # No running loop (sync/test context) — skip; redelivery covers it.
        pass


def _replace_inbox(
    inbox: Dict[str, List[Dict[str, Any]]],
    job_id: str,
    entries: Any,
    label: str,
) -> None:
    """Replace one inbox for ``job_id`` from a heartbeat-response field.

    The orchestrator is the source of truth: a present list overwrites the
    inbox wholesale (so consumed entries prune themselves once the ack has
    landed), an empty list clears it, and a missing/None field (older
    orchestrator, or the job lookup failed) leaves the inbox untouched.
    """
    if not isinstance(entries, list):
        return
    valid = [e for e in entries if isinstance(e, dict)]
    if valid:
        if inbox.get(job_id) != valid:
            logger.info(
                f"{label} pending for job {job_id}: {len(valid)} entr"
                f"{'y' if len(valid) == 1 else 'ies'}"
            )
        inbox[job_id] = valid
    else:
        inbox.pop(job_id, None)


def _update_guidance_inbox(response: Dict[str, Any]) -> None:
    """Refresh both steering inboxes for the current job from the heartbeat.

    Both lanes share the transport and prune contract. Guidance renders every
    provider turn. In Lane B, child completion events render on every tool pass
    while human/operator replies wait for the next natural break or expiry.
    """
    job_id = _current_job_id
    if _pod_state != PodState.WORKING or not job_id:
        return
    if not isinstance(response, dict):
        return
    _replace_inbox(
        _guidance_inbox,
        job_id,
        response.get("pending_guidance"),
        "Supervisor guidance",
    )
    _replace_inbox(
        _reply_inbox,
        job_id,
        response.get("queued_replies"),
        "Queued replies",
    )


async def _handle_heartbeat_intents(response: Dict[str, Any]) -> None:
    """Heartbeat-response callback: react to orchestrator-set intents.

    Runs the job-status preemption backstop first, then the supervisor-
    guidance inbox update, then drain intents. Idle workers (``ready``
    status, no job) exit immediately to free the slot for a fresh-version
    pod. Busy workers just record the intent — the graph reacts at its
    next safe boundary. Attached sessions get persistent_app's semantics
    (defer while a turn is in flight, clean drain-suspend once parked) —
    a session never reaches a phase boundary, so before that branch the
    intent dead-lettered and stale adopted-session pods survived every
    deploy (knowledge-base/knowledge/issues/dual_app_persistent_app_redundancy.md).
    """
    global _drain_intent_received, _drain_intent_handled, _drain_deferred_logged
    _check_job_preempted(response)
    _update_guidance_inbox(response)
    intents = response.get("intents") or {}
    if not isinstance(intents, dict) or not intents.get("should_drain"):
        return
    _drain_intent_received = True
    if _drain_intent_handled:
        return
    if _current_job_id is None and _pod_state == PodState.IDLE:
        _drain_intent_handled = True
        logger.info(
            "Drain intent received (reason=%s) — idle worker exiting",
            intents.get("drain_reason", "unspecified"),
        )
        # Best-effort drain heartbeat so the orchestrator sees us go.
        try:
            await _orchestrator_client.heartbeat(
                status="draining",
                job_id=None,
                metrics=_get_agent_metrics(),
            )
        except Exception:
            pass
        await _deregister_before_exit()
        os._exit(0)
    elif _pod_state == PodState.SESSION:
        # Dual pods host adopted sessions on persistent_app's module state
        # (/session/attach seeds pa.* and calls pa._attach_session), so the
        # drain semantics are delegated, not re-implemented.
        import agent.api.persistent_app as pa

        if pa._session is None or not pa._session_parked():
            # No live session object yet (/session/attach flips the pod
            # state before the backgrounded setup creates pa._session) or a
            # turn is in flight — never tear down out-of-band. Left
            # unhandled so every heartbeat re-checks until the loop parks;
            # if the session instead detaches, the IDLE branch exits the
            # stale pod rather than returning it to the pool.
            if not _drain_deferred_logged:
                logger.info(
                    "Drain intent received (reason=%s) — session attached, "
                    "deferring until the loop parks",
                    intents.get("drain_reason", "unspecified"),
                )
                _drain_deferred_logged = True
            return
        _drain_intent_handled = True
        logger.info(
            "Drain intent received (reason=%s) — suspending session and exiting",
            intents.get("drain_reason", "unspecified"),
        )
        await pa._drain_suspend_session()
    elif not _drain_intent_handled:
        # Busy — log once. The graph picks this up via is_drain_requested().
        _drain_intent_handled = True
        logger.info(
            "Drain intent received (reason=%s) — will freeze at next phase boundary",
            intents.get("drain_reason", "unspecified"),
        )


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    global \
        _agent, \
        _shutdown_requested, \
        _orchestrator_client, \
        _heartbeat_task, \
        _session_attach_task, \
        _started_at

    _started_at = datetime.now()
    logger.info("Starting dual-mode agent application...")

    config_path = _config_path or os.getenv("AGENT_CONFIG", "worker_base")
    resolved_path, deployment_dir = resolve_config_path(config_path)
    logger.info(f"Loading agent configuration from: {resolved_path}")

    _agent = UniversalAgent.from_config(config_path)
    await _agent.initialize()

    # Register with orchestrator
    _orchestrator_client = create_orchestrator_client_from_env(_agent.config.agent_id)
    logger.info("Registering with orchestrator...")
    await _orchestrator_client.connect()

    if await _orchestrator_client.register(agent_mode="dual"):
        logger.info("Registered with orchestrator (dual mode)")
    else:
        logger.warning("Initial registration failed — will keep retrying in background")

    _heartbeat_task = asyncio.create_task(
        _orchestrator_client.run_heartbeat_loop(
            get_status=_get_heartbeat_status,
            get_job_id=_get_current_job_id,
            get_metrics=_get_agent_metrics,
            on_response=_handle_heartbeat_intents,
        )
    )

    yield

    # --- Shutdown ---
    logger.info("Shutting down dual-mode agent...")
    _shutdown_requested = True

    # The session attach endpoint returns before heavy setup.  Cancel and await
    # that owned task before deregistering its orchestrator client so its
    # exact binding-release cleanup cannot race a closed transport.
    if _session_attach_task is not None and not _session_attach_task.done():
        _session_attach_task.cancel()
        try:
            await _session_attach_task
        except asyncio.CancelledError:
            pass

    # Drain: send immediate draining heartbeat
    if _orchestrator_client and _orchestrator_client.agent_id:
        try:
            await _orchestrator_client.heartbeat(
                status="draining",
                job_id=_current_job_id,
                metrics=_get_agent_metrics(),
            )
        except Exception:
            pass

    # If a job is running, cooperative stop
    releasing_job_id = None
    if _current_job_task and not _current_job_task.done():
        releasing_job_id = _current_job_id
        logger.info("Requesting cooperative stop for graceful shutdown...")
        _request_stop("pause")
        try:
            await asyncio.wait_for(_stop_completed.wait(), timeout=120.0)
            logger.info("Job stopped cooperatively during shutdown")
        except asyncio.TimeoutError:
            logger.warning("Cooperative stop timed out — hard cancelling")
            _current_job_task.cancel()
            try:
                await _current_job_task
            except asyncio.CancelledError:
                pass

    if releasing_job_id and _orchestrator_client:
        try:
            await _orchestrator_client.report_pause(releasing_job_id)
        except Exception:
            logger.warning(
                f"Could not notify orchestrator to pause job {releasing_job_id}"
            )

    # Detach any active session
    if _pod_state == PodState.SESSION:
        try:
            from agent.api.persistent_app import _detach_session

            await _detach_session()
        except Exception as e:
            logger.warning(f"Session detach during shutdown failed: {e}")

    # Stop heartbeat and deregister
    if _orchestrator_client:
        _orchestrator_client.stop_heartbeat()
        if _heartbeat_task:
            _heartbeat_task.cancel()
            try:
                await _heartbeat_task
            except asyncio.CancelledError:
                pass
        await _orchestrator_client.deregister()
        await _orchestrator_client.close()

    if _agent:
        await _agent.shutdown()

    logger.info("Dual-mode agent shutdown complete")


# ---------------------------------------------------------------------------
# Pod exit helper
# ---------------------------------------------------------------------------


_DEREGISTER_ON_EXIT_TIMEOUT_S = 5.0


async def _deregister_before_exit() -> None:
    """Best-effort deregistration ahead of os._exit.

    os._exit bypasses the lifespan shutdown that normally deregisters, so
    without this every clean exit leaves an agents row that the
    orchestrator's 3-minute heartbeat sweep flips to offline and reports
    as a fleet:agents_offline corpse. Bounded and non-raising — a slow or
    failed deregister must never hold up or abort the exit (the stale-agent
    sweep stays the backstop, exactly as for crashes).
    """
    client = _orchestrator_client
    if client is None:
        return
    client.stop_heartbeat()
    hb = _heartbeat_task
    if hb is not None and not hb.done() and hb is not asyncio.current_task():
        # A heartbeat landing mid-deregister would 404 and re-register,
        # resurrecting the row this call is about to delete. Never
        # self-cancel — the idle-drain exit runs inside the heartbeat task.
        hb.cancel()
    if not client.agent_id:
        return
    try:
        await asyncio.wait_for(
            client.deregister(), timeout=_DEREGISTER_ON_EXIT_TIMEOUT_S
        )
    except Exception as e:
        logger.warning(f"Best-effort deregister before exit failed: {e}")


def _schedule_exit(delay: float = 1.0) -> None:
    """Schedule process exit after a short delay (allows response to be sent)."""
    global _pending_exit_task

    # Cancel any previously scheduled exit
    if _pending_exit_task and not _pending_exit_task.done():
        _pending_exit_task.cancel()

    async def _exit():
        await asyncio.sleep(delay)
        await _deregister_before_exit()
        logger.info("Pod task complete — exiting process")
        # Use os._exit to bypass uvicorn's signal handling
        os._exit(0)

    _pending_exit_task = asyncio.create_task(_exit())


async def _fail_closed_subagent_lifecycle(job_id: str) -> None:
    """Drain and retire a dual-mode owner whose children did not settle."""

    global _shutdown_requested
    _shutdown_requested = True
    # Fatal even in loop mode. Arm retirement before cleanup/heartbeat awaits
    # so a wedged runtime cannot keep this stale ToolContext dispatchable.
    _schedule_exit(delay=1.0)
    logger.critical(
        "Pinned job child lifecycle could not settle; keeping pod non-idle and "
        "retiring without completion report: job=%s",
        job_id,
    )
    agent = _agent
    abandon = getattr(agent, "abandon_worker_subagents", None)
    if callable(abandon):
        try:
            await abandon("pinned parent lifecycle failed")
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Pinned child runtime abandon retry failed")
    client = _orchestrator_client
    if client is not None and client.agent_id:
        try:
            await client.heartbeat(
                status="draining",
                job_id=job_id,
                metrics=_get_agent_metrics(),
            )
        except Exception:
            logger.exception("Could not publish pinned lifecycle drain heartbeat")


def _should_loop() -> bool:
    """Check if agent should loop back to IDLE instead of exiting."""
    return os.environ.get("AGENT_LOOP", "").strip() == "1"


def _final_idle_status() -> str:
    """Status asserted by post-task heartbeats: 'ready' iff we actually stay.

    One-shot workers exit ~2s after completion; a final 'ready' heartbeat
    leaves a dispatchable-looking row for up to the 3-min offline threshold
    after the process is gone — the dispatcher claiming a job for that dead
    pod is Finding 5 of
    knowledge-history/done/stale_agent_detector_sql_crash_disables_recovery_sweeps.md.
    'draining' is agent-assertable (same vocabulary as the drain-intent
    path), excluded from get_available_agents, and the row falls to
    'offline' via the normal heartbeat timeout after the exit.
    """
    return "ready" if _should_loop() else "draining"


async def _reset_to_idle(
    source: str,
    *,
    skip_session_cleanup: bool = False,
    final_status: str = "ready",
) -> None:
    """Clean up current task state and return to IDLE for next task.

    The state flip to IDLE and the ready-heartbeat are in a try/finally so
    they run even if the inline cleanup (session detach, file-handler tear-
    down) raises. Without that guarantee a partial failure leaves the agent
    reporting 'working'/'session' forever — exactly the zombie pattern we
    saw in dev (PR 1's orchestrator-side sweep is the safety net for this).

    The ready-heartbeat retries 3x with backoff. If all attempts fail the
    orchestrator's stuck-working/session sweep catches it within 60s.
    """
    global _pod_state, _current_job_id, _current_job_task
    global _session_attach_claim

    logger.info(f"Resetting to IDLE after: {source}")

    try:
        _current_job_id = None
        _current_job_task = None
        _clear_stop()
        # Guidance is job-scoped; anything unrendered is still pending in job
        # context on the orchestrator and will be redelivered on re-dispatch.
        _guidance_inbox.clear()

        if _pod_state == PodState.SESSION and not skip_session_cleanup:
            try:
                from agent.api.persistent_app import _detach_session

                await _detach_session()
            except Exception as e:
                logger.warning(f"Session cleanup during reset failed: {e}")

        # Remove per-job file handlers from root logger
        try:
            root_logger = logging.getLogger()
            for handler in root_logger.handlers[:]:
                if isinstance(handler, logging.FileHandler) and "job_" in getattr(
                    handler, "baseFilename", ""
                ):
                    handler.close()
                    root_logger.removeHandler(handler)
        except Exception as e:
            logger.warning(f"File handler cleanup during reset failed: {e}")
    finally:
        async with _state_lock:
            # Scrub thread/project authority before publishing IDLE. Otherwise
            # a successor can claim the pod between the state flip and these
            # synchronous clears, then lose its freshly adopted identity.
            if _orchestrator_client is not None:
                _orchestrator_client.clear_runtime_actor()
                _orchestrator_client.clear_session_runtime_identity()
            _session_attach_claim = None
            _pod_state = PodState.IDLE

        if _orchestrator_client and _orchestrator_client.agent_id:
            last_err: Optional[Exception] = None
            for attempt in range(3):
                try:
                    await _orchestrator_client.heartbeat(
                        status=final_status,
                        job_id=None,
                        metrics=_get_agent_metrics(),
                    )
                    last_err = None
                    break
                except Exception as e:
                    last_err = e
                    if attempt < 2:
                        await asyncio.sleep(0.5 * (attempt + 1))
            if last_err is not None:
                logger.warning(
                    f"Failed to send {final_status} heartbeat "
                    f"after 3 attempts: {last_err}"
                )

    logger.info(f"Agent returned to IDLE (reported '{final_status}')")


# Ceiling for the pre-teardown evidence push. Must stay under the 120s
# cooperative-stop window (/job/cancel, /job/pause, lifespan drain), or a
# wedged push would degrade every cooperative stop into a hard kill. The
# per-command git timeouts (GitManager, 60s/120s) bound the healthy path;
# this bounds a wedged SSH backend.
_EVIDENCE_PUSH_TIMEOUT_SECONDS = 60.0


async def _push_evidence_snapshot(job_id: str, reason: str) -> None:
    """Best-effort commit+push of the workspace before cooperative-stop teardown.

    A cancel used to destroy everything committed or written since the last
    phase-boundary push — per-todo commits are local-only, and workspace
    reaping then erases them permanently. The supervisor's kill switch must
    never destroy the evidence he kills for (P1-D of
    knowledge-base/knowledge/issues/officer_blind_reads_and_worker_bureaucracy.md).

    Runs the sync git calls (possibly over SSH) in a thread with a hard
    timeout. A git failure must never block or fail the teardown itself.
    """
    workspace = getattr(_agent, "_workspace_manager", None)
    if workspace is None:
        return
    try:
        await asyncio.wait_for(
            asyncio.to_thread(push_evidence_snapshot, workspace, reason, job_id),
            timeout=_EVIDENCE_PUSH_TIMEOUT_SECONDS,
        )
    except Exception as e:
        logger.warning(
            f"[{job_id}] Evidence push before {reason} teardown failed (non-fatal): {e}"
        )


async def _complete_stop(reset_source: str) -> None:
    """Finish a cooperative stop (cancel/pause): push an evidence snapshot,
    reset to IDLE, then signal the waiting cancel/pause handler.

    The evidence push comes first: at this point the streaming loop has
    broken out, so the graph is suspended (no concurrent workspace writes)
    and the workspace handle is still live — the last moment the job's
    unpushed work can reach its Gitea branch before teardown/reaping.

    Ordering is load-bearing. ``_reset_to_idle()`` calls ``_clear_stop()``,
    which clears ``_stop_completed``; the cancel/pause handler is blocked on
    that event, so it must be ``set()`` *after* the reset or the handler hangs
    to its 120s timeout. Without the reset, ``_pod_state`` stays ``WORKING``
    with no job — the zombie pattern (see
    knowledge-history/done/worker_pod_state_zombie_on_cancel.md).
    """
    await _push_evidence_snapshot(_current_job_id or "unknown", _stop_reason or "stop")
    await _reset_to_idle(reset_source)
    _stop_completed.set()


# ---------------------------------------------------------------------------
# Job processing (adapted from app.py)
# ---------------------------------------------------------------------------


async def _process_orchestrator_job(
    job_id: str,
    description: str,
    upload_id: Optional[str] = None,
    config_upload_id: Optional[str] = None,
    instructions_upload_id: Optional[str] = None,
    document_path: Optional[str] = None,
    document_dir: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
    instructions: Optional[str] = None,
    config_name: Optional[str] = None,
    config_override: Optional[Dict[str, Any]] = None,
    resolved_config: Optional[Dict[str, Any]] = None,
    git_remote_url: Optional[str] = None,
    datasources: Optional[list] = None,
    repositories: Optional[list] = None,
    managed_repository_credentials: Optional[list] = None,
    branch_name: Optional[str] = None,
    project_id: Optional[str] = None,
    runtime_actor: Optional[Dict[str, Any]] = None,
) -> None:
    """Process a job assigned by the orchestrator, then exit."""
    global _current_job_id, _pod_state

    if _agent is None:
        logger.error("Cannot process job — agent not initialized")
        return

    from agent.core.logging_config import (
        bind_log_context,
        build_formatter,
        reset_log_context,
    )

    # Tag every log line for this job (stdout + file) with job_id (correlation).
    _log_token = bind_log_context(job_id=job_id)
    try:
        from agent.core.workspace import get_logs_path

        # Set up per-job file logging — same formatter as stdout (JSON in-cluster).
        logs_dir = get_logs_path()
        log_file = logs_dir / f"job_{job_id}.log"
        level_name = os.getenv("LOG_LEVEL", "INFO").upper()
        level = getattr(logging, level_name, logging.INFO)

        file_handler = logging.FileHandler(log_file, mode="a")
        file_handler.setLevel(level)
        file_handler.setFormatter(build_formatter(component="agent"))
        logging.getLogger().addHandler(file_handler)

        logger.info(f"Starting orchestrator job {job_id}")

        # Build metadata
        metadata: Dict[str, Any] = {"description": description}
        if upload_id:
            metadata["upload_id"] = upload_id
        if config_upload_id:
            metadata["config_upload_id"] = config_upload_id
        if instructions_upload_id:
            metadata["instructions_upload_id"] = instructions_upload_id
        if document_path:
            metadata["document_path"] = document_path
        if document_dir:
            metadata["document_dir"] = document_dir
        if context:
            metadata.update(context)
        if instructions:
            metadata["instructions"] = instructions
        if config_name and config_name != "worker_base":
            metadata["config_name"] = config_name
        if config_override:
            metadata["config_override"] = config_override
        if resolved_config:
            metadata["resolved_config"] = resolved_config
        if git_remote_url:
            metadata["git_remote_url"] = git_remote_url
        if datasources:
            metadata["datasources"] = datasources
        if repositories:
            metadata["repositories"] = repositories
        if managed_repository_credentials:
            metadata["managed_repository_credentials"] = managed_repository_credentials
        if branch_name:
            metadata["branch_name"] = branch_name
        if project_id:
            metadata["project_id"] = project_id
        if runtime_actor:
            metadata["runtime_actor"] = runtime_actor

        _clear_stop()

        # Inject orchestrator client so delegation tools can reach the API
        _agent._orchestrator_client = _orchestrator_client

        # Process with streaming
        final_state = None
        last_iteration = "?"
        streaming_gen = await _agent.process_job(job_id, metadata, stream=True)
        try:
            async for state in streaming_gen:
                final_state = state
                if isinstance(state, dict):
                    iteration = state.get("iteration")
                    if iteration is not None:
                        last_iteration = iteration
                    logger.info(f"[Iteration {last_iteration}] job={job_id}")

                if _stop_requested.is_set():
                    logger.info(f"Stop requested ({_stop_reason}) for job {job_id}")
                    break
        finally:
            await _close_job_stream(streaming_gen, job_id=job_id)

        # Handle cooperative stop vs normal completion
        if _stop_requested.is_set():
            reason = _stop_reason
            logger.info(f"Job {job_id} stopped gracefully (reason={reason})")
            # Reset to IDLE so the pod doesn't strand _pod_state=WORKING with no
            # job (zombie). Don't exit — cancel/pause means the orchestrator may
            # reassign/resume. See worker_pod_state_zombie_on_cancel.md.
            await _complete_stop(f"job {reason}")
            return

        result = final_state or {}
        logger.info(f"Job {job_id} completed: {result.get('should_stop')}")

        _current_job_id = None

        # Report completion before advertising an idle slot. A ready/draining
        # heartbeat while the job is still processing lets the orphan sweep
        # pause and redispatch finished work before /complete reaches its
        # guard. The ordinary app follows the same ordering.
        if _orchestrator_client:
            try:
                await _orchestrator_client.report_completion(
                    job_id,
                    result,
                    agent_id=_orchestrator_client.agent_id,
                )
                if _orchestrator_client.agent_id:
                    await _orchestrator_client.heartbeat(
                        status=_final_idle_status(),
                        job_id=None,
                        metrics=_get_agent_metrics(),
                    )
            except Exception as e:
                logger.error(f"Failed to report completion for job {job_id}: {e}")

        # Always reset state — _reset_to_idle pushes a final heartbeat so the
        # DB matches reality even in non-loop mode where _schedule_exit would
        # otherwise os._exit(0) before lifespan cleanup runs. Non-loop asserts
        # 'draining', not 'ready' — see _final_idle_status.
        await _reset_to_idle("job completion", final_status=_final_idle_status())
        if not _should_loop():
            _schedule_exit(delay=2.0)

    except asyncio.CancelledError:
        logger.info(f"Job {job_id} was cancelled")
        raise
    except SubagentLifecycleError:
        await _fail_closed_subagent_lifecycle(job_id)
    except Exception as e:
        logger.error(f"Job {job_id} failed: {e}", exc_info=True)
        if _orchestrator_client:
            try:
                await _orchestrator_client.report_completion(
                    job_id,
                    completion_error_payload(e),
                    agent_id=_orchestrator_client.agent_id,
                )
            except Exception:
                logger.error(f"Failed to report error for job {job_id}")
        await _reset_to_idle("job error", final_status=_final_idle_status())
        if not _should_loop():
            _schedule_exit(delay=2.0)
    finally:
        if _current_job_id == job_id:
            _current_job_id = None
        reset_log_context(_log_token)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def create_dual_app(config_path: Optional[str] = None) -> FastAPI:
    """Create the dual-mode FastAPI application.

    Merges worker and persistent routes with state-machine guards.
    """
    global _config_path
    if config_path:
        _config_path = config_path

    app = FastAPI(
        title="Universal Agent API (Dual Mode)",
        description="Dual-mode agent: accepts jobs or interactive sessions",
        version="2.0.0",
        lifespan=lifespan,
    )

    # ===================================================================
    # Health endpoints
    # ===================================================================

    @app.get("/health", response_model=HealthResponse, tags=["Health"])
    async def health_check() -> HealthResponse:
        if _agent is None:
            return HealthResponse(
                status=HealthStatus.UNHEALTHY,
                agent_id="unknown",
                agent_name="Unknown",
                uptime_seconds=0,
                checks={"initialized": False},
            )
        status = _agent.get_status()
        health = HealthStatus.HEALTHY
        if not status["initialized"]:
            health = HealthStatus.UNHEALTHY
        elif not status["connections"]["postgres"]:
            health = HealthStatus.DEGRADED
        return HealthResponse(
            status=health,
            agent_id=status["agent_id"],
            agent_name=status["display_name"],
            uptime_seconds=status["uptime_seconds"],
            checks={
                "initialized": status["initialized"],
                "postgres": status["connections"].get("postgres", False),
            },
        )

    @app.get("/ready", response_model=ReadyResponse, tags=["Health"])
    async def readiness_check() -> ReadyResponse:
        if _shutdown_requested:
            return ReadyResponse(
                ready=False,
                message="Agent is draining (shutting down)",
                connections={},
            )
        if _agent is None:
            return ReadyResponse(
                ready=False, message="Agent not initialized", connections={}
            )
        status = _agent.get_status()
        base_ready = status["initialized"] and status["connections"]["postgres"]

        # When in SESSION state, the readiness gate is the three-way check
        # in persistent_app._session_ready() — shared with /session/status
        # and handle_persistent_websocket so probe and WS gate stay aligned.
        if _pod_state == PodState.SESSION:
            import agent.api.persistent_app as pa

            session_ready = pa._session_ready()
            protected_required = bool(
                pa._session is not None
                and getattr(pa._session, "protected_cloud_required", False) is True
            )
            protected_ready = bool(
                protected_required
                and session_ready is True
                and pa._session is not None
                and pa._session.protected_cloud_ready() is True
            )
            session_identity_fingerprint = pinned_session_ready_identity_fingerprint(
                thread_id=pa._thread_id,
                runtime_generation=pa._session_runtime_generation,
                agent_id=pa._registered_pinned_agent_id(),
                runtime_attach_token=pa._session_runtime_attach_token,
                pod_uid=os.environ.get("POD_UID"),
            )
            capabilities: dict[str, bool | int] = {
                "durable_input_delivery": True,
                "pinned_session_identity_contract": (
                    PINNED_SESSION_READY_IDENTITY_CONTRACT
                ),
                # Exact integer protocol version: the orchestrator rejects
                # bool/float/string lookalikes from mixed-version agents.
                "protected_cloud_contract": 1,
                "protected_cloud_ready": protected_ready,
            }
            if _pinned_session_recipient_capable():
                capabilities["pinned_session_recipient_binding"] = True
            return ReadyResponse(
                ready=session_ready,
                message="Session ready" if session_ready else "Session initializing",
                connections=status["connections"],
                session_identity_fingerprint=session_identity_fingerprint,
                capabilities=capabilities,
            )

        capabilities = {
            "resolved_config_resume": True,
            "pinned_recipient_binding": True,
        }
        if _pinned_session_recipient_capable():
            capabilities["pinned_session_recipient_binding"] = True
        return ReadyResponse(
            ready=base_ready,
            message="Ready to accept work" if base_ready else "Not ready",
            connections=status["connections"],
            capabilities=capabilities,
        )

    @app.get("/status", tags=["Health"])
    async def agent_status() -> Dict[str, Any]:
        runtime_status = _agent.get_status() if _agent is not None else {}
        return {
            "mode": "dual",
            "pod_state": _pod_state.value,
            "loop_mode": _should_loop(),
            "current_job_id": _current_job_id,
            "config": _config_path,
            "uptime_seconds": (datetime.now() - _started_at).total_seconds()
            if _started_at
            else 0,
            "research_providers": runtime_status.get("research_providers"),
        }

    # ===================================================================
    # System monitoring (from app.py)
    # ===================================================================

    @app.get("/system/info", tags=["Monitoring"])
    async def system_info() -> Dict[str, Any]:
        try:
            from agent.api.app import _collect_system_info

            return _collect_system_info()
        except ImportError:
            raise HTTPException(501, "psutil not installed")

    @app.post("/system/shell-state", tags=["Monitoring"])
    async def shell_state(recipient: PinnedJobRecipient) -> Dict[str, Any]:
        _require_pinned_job_recipient(recipient, job_id=_current_job_id)

        if _agent is None:
            return {"tabs": [], "message": "Agent not initialized"}
        shell_manager = getattr(_agent, "_shell_manager", None)
        if shell_manager is None:
            return {"tabs": [], "message": "No active shell sessions"}
        try:
            tab_list = shell_manager.list_tabs()
            tabs = []
            for tab_meta in tab_list:
                name = tab_meta.get("name", "unknown")
                try:
                    read_result = shell_manager.read_with_offset(name, lines=30)
                    recent_output = (
                        read_result.get("output", "")
                        if isinstance(read_result, dict)
                        else str(read_result)
                    )
                    total_lines = (
                        read_result.get("total_lines", 0)
                        if isinstance(read_result, dict)
                        else 0
                    )
                except Exception:
                    recent_output = ""
                    total_lines = 0
                tabs.append(
                    {
                        "name": name,
                        "type": tab_meta.get("type", "unknown"),
                        "created_at": tab_meta.get("created_at", ""),
                        "total_lines": total_lines,
                        "recent_output": recent_output,
                    }
                )
            return {"tabs": tabs}
        except Exception:
            error_ref = uuid4().hex[:12]
            logger.exception("Failed to get shell state (error_ref=%s)", error_ref)
            return {
                "tabs": [],
                "message": "Error: shell state unavailable",
                "error_ref": error_ref,
            }

    # ===================================================================
    # Worker routes (job dispatch)
    # ===================================================================

    @app.post(
        "/job/start",
        response_model=JobStartResponse,
        status_code=202,
        tags=["Worker"],
        responses={
            409: {"model": ErrorResponse, "description": "Pod is busy"},
            503: {"model": ErrorResponse, "description": "Agent not initialized"},
        },
    )
    async def start_job(request: JobStartRequest) -> JobStartResponse:
        global _pod_state, _current_job_id, _current_job_task

        _require_pinned_job_recipient(request.recipient, job_id=request.job_id)

        if _agent is None:
            raise HTTPException(503, "Agent not initialized")
        if _shutdown_requested:
            raise HTTPException(503, "Agent is shutting down")

        from agent.api.pinned_delivery import accepted_pinned_job_delivery

        if _pod_state == PodState.WORKING and _current_job_id == request.job_id:
            acknowledgement = accepted_pinned_job_delivery(
                request, _orchestrator_client, retry=True,
            )
            return JobStartResponse(
                job_id=request.job_id,
                status="accepted",
                message="Job was already accepted by this runtime",
                **acknowledgement,
            )

        try:
            validate_worker_workspace_projection(
                config_override=request.config_override,
                resolved_config=request.resolved_config,
                workspace_runtime=request.workspace_runtime,
            )
        except WorkspaceContractError as exc:
            raise HTTPException(409, {"code": exc.code}) from exc

        async with _state_lock:
            if _pod_state != PodState.IDLE:
                raise HTTPException(
                    409,
                    f"Pod is in {_pod_state.value} state, cannot accept job",
                )
            # Keep the delivery binding and local state change indivisible.
            acknowledgement = accepted_pinned_job_delivery(
                request, _orchestrator_client, retry=False,
            )
            _pod_state = PodState.WORKING
            _current_job_id = request.job_id

        _clear_stop()

        start_context = dict(request.context or {})
        start_context["workspace_runtime"] = request.workspace_runtime
        for field in (
            "workspace_provisioner",
            "workspace_generation",
            "workspace_runtime_incarnation",
            "workspace_ssh_host_key_fingerprint",
            "workspace_owner_kind",
            "workspace_owner_id",
        ):
            value = getattr(request, field)
            if value:
                start_context[field] = value
        _current_job_task = asyncio.create_task(
            _process_orchestrator_job(
                job_id=request.job_id,
                description=request.description,
                upload_id=request.upload_id,
                config_upload_id=request.config_upload_id,
                instructions_upload_id=request.instructions_upload_id,
                document_path=request.document_path,
                document_dir=request.document_dir,
                context=start_context,
                instructions=request.instructions,
                config_name=request.config_name,
                config_override=request.config_override,
                resolved_config=request.resolved_config,
                git_remote_url=request.git_remote_url,
                datasources=request.datasources,
                repositories=request.repositories,
                managed_repository_credentials=(request.managed_repository_credentials),
                branch_name=request.branch_name,
                project_id=request.project_id,
                runtime_actor=request.runtime_actor,
            )
        )

        logger.info(f"Accepted job {request.job_id} (dual mode)")
        return JobStartResponse(
            job_id=request.job_id,
            status="accepted",
            message="Job processing started",
            **acknowledgement,
        )

    @app.post("/job/cancel", tags=["Worker"])
    async def cancel_job(
        request: Optional[JobCancelByOrchestratorRequest] = None,
    ) -> Dict[str, Any]:
        global _current_job_id, _current_job_task

        if _pod_state != PodState.WORKING or _current_job_id is None:
            raise HTTPException(404, "No job currently running")

        _require_pinned_job_recipient(
            request.recipient if request is not None else None,
            job_id=_current_job_id,
        )

        job_id = _current_job_id
        _request_stop("cancel")

        logger.info(f"Cancel requested for job {job_id}")
        try:
            await asyncio.wait_for(_stop_completed.wait(), timeout=120.0)
            return {
                "job_id": job_id,
                "status": "cancelled",
                "reason": (request.reason if request is not None else None)
                or "Cancelled by orchestrator",
                "graceful": True,
            }
        except asyncio.TimeoutError:
            if _current_job_task and not _current_job_task.done():
                _current_job_task.cancel()
                try:
                    await _current_job_task
                except asyncio.CancelledError:
                    pass
            # The hard-killed task re-raises CancelledError without resetting
            # _pod_state, so reset here or the pod strands at WORKING (zombie).
            # _reset_to_idle also nulls _current_job_id/_task and clears stop
            # state. See worker_pod_state_zombie_on_cancel.md.
            await _reset_to_idle("cancel hard-kill")
            return {
                "job_id": job_id,
                "status": "cancelled",
                "reason": (
                    f"{(request.reason if request is not None else None) or 'Cancelled'} "
                    "(hard-killed)"
                ),
                "graceful": False,
            }

    @app.post("/job/pause", tags=["Worker"])
    async def pause_job(
        request: Optional[JobCancelByOrchestratorRequest] = None,
    ) -> Dict[str, Any]:
        if _pod_state != PodState.WORKING or _current_job_id is None:
            raise HTTPException(404, "No job currently running")

        _require_pinned_job_recipient(
            request.recipient if request is not None else None,
            job_id=_current_job_id,
        )

        job_id = _current_job_id
        _request_stop("pause")

        logger.info(f"Pause requested for job {job_id}")
        try:
            await asyncio.wait_for(_stop_completed.wait(), timeout=120.0)
        except asyncio.TimeoutError:
            raise HTTPException(
                408,
                f"Pause timed out after 120s. Job {job_id} will pause after current node.",
            )
        return {"job_id": job_id, "status": "paused"}

    @app.post(
        "/job/resume",
        response_model=JobStartResponse,
        status_code=202,
        tags=["Worker"],
    )
    async def resume_job(request: JobResumeRequest) -> JobStartResponse:
        global _pod_state, _current_job_id, _current_job_task

        _require_pinned_job_recipient(request.recipient, job_id=request.job_id)

        if _agent is None:
            raise HTTPException(503, "Agent not initialized")
        if _shutdown_requested:
            raise HTTPException(503, "Agent is shutting down")

        if _pod_state == PodState.WORKING and _current_job_id == request.job_id:
            return JobStartResponse(
                job_id=request.job_id,
                status="accepted",
                message="Job resume was already accepted by this runtime",
            )

        try:
            validate_worker_workspace_projection(
                config_override=request.config_override,
                resolved_config=request.resolved_config,
                workspace_runtime=request.workspace_runtime,
            )
        except WorkspaceContractError as exc:
            raise HTTPException(409, {"code": exc.code}) from exc

        async with _state_lock:
            if _pod_state != PodState.IDLE:
                raise HTTPException(
                    409,
                    f"Pod is in {_pod_state.value} state, cannot accept resume",
                )
            _pod_state = PodState.WORKING
            _current_job_id = request.job_id

        _clear_stop()

        async def _do_resume():
            global _current_job_id, _pod_state

            try:
                from agent.core.workspace import get_logs_path

                logs_dir = get_logs_path()
                log_file = logs_dir / f"job_{request.job_id}.log"
                file_handler = logging.FileHandler(log_file, mode="a")
                file_handler.setLevel(logging.INFO)
                file_handler.setFormatter(
                    logging.Formatter(
                        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S",
                    )
                )
                logging.getLogger().addHandler(file_handler)

                resume_metadata = {}
                if request.config_upload_id:
                    resume_metadata["config_upload_id"] = request.config_upload_id
                if request.config_override:
                    resume_metadata["config_override"] = request.config_override
                if request.resolved_config:
                    resume_metadata["resolved_config"] = request.resolved_config
                if request.datasources:
                    resume_metadata["datasources"] = request.datasources
                if request.repositories:
                    resume_metadata["repositories"] = request.repositories
                if request.managed_repository_credentials:
                    resume_metadata["managed_repository_credentials"] = (
                        request.managed_repository_credentials
                    )
                if request.project_id:
                    resume_metadata["project_id"] = request.project_id
                if request.runtime_actor:
                    resume_metadata["runtime_actor"] = request.runtime_actor
                resume_metadata["workspace_runtime"] = request.workspace_runtime
                for field in (
                    "workspace_provisioner",
                    "workspace_generation",
                    "workspace_runtime_incarnation",
                    "workspace_ssh_host_key_fingerprint",
                    "workspace_owner_kind",
                    "workspace_owner_id",
                ):
                    value = getattr(request, field)
                    if value:
                        resume_metadata[field] = value
                if request.git_remote_url:
                    # Feeds the pod-handoff clone fallback in
                    # _setup_job_workspace (resume_fresh_workspace_no_clone_
                    # fallback.md) — without it a fresh workspace with no
                    # snapshot starts blank.
                    resume_metadata["git_remote_url"] = request.git_remote_url

                _agent._orchestrator_client = _orchestrator_client
                final_state = None
                streaming_gen = await _agent.process_job(
                    request.job_id,
                    metadata=resume_metadata if resume_metadata else None,
                    resume=True,
                    feedback=request.feedback,
                    feedback_reason=request.feedback_reason,
                    original_config_name=request.config_name,
                    previous_status=request.previous_status,
                    stream=True,
                )
                try:
                    async for state in streaming_gen:
                        final_state = state
                        if _stop_requested.is_set():
                            break
                finally:
                    await _close_job_stream(streaming_gen, job_id=request.job_id)

                if _stop_requested.is_set():
                    reason = _stop_reason
                    logger.info(
                        f"Resume of job {request.job_id} stopped gracefully "
                        f"(reason={reason})"
                    )
                    await _complete_stop(f"job resume {reason}")
                    return

                result = final_state or {}
                _current_job_id = None

                if _orchestrator_client:
                    try:
                        await _orchestrator_client.report_completion(
                            request.job_id,
                            result,
                            agent_id=_orchestrator_client.agent_id,
                        )
                        if _orchestrator_client.agent_id:
                            await _orchestrator_client.heartbeat(
                                status=_final_idle_status(),
                                job_id=None,
                                metrics=_get_agent_metrics(),
                            )
                    except Exception as e:
                        logger.error(f"Failed to report completion: {e}")

                await _reset_to_idle(
                    "job resume completion", final_status=_final_idle_status()
                )
                if not _should_loop():
                    _schedule_exit(delay=2.0)

            except asyncio.CancelledError:
                raise
            except SubagentLifecycleError:
                await _fail_closed_subagent_lifecycle(request.job_id)
            except Exception as e:
                logger.error(f"Resume job {request.job_id} failed: {e}", exc_info=True)
                if _orchestrator_client:
                    try:
                        await _orchestrator_client.report_completion(
                            request.job_id,
                            completion_error_payload(e),
                            agent_id=_orchestrator_client.agent_id,
                        )
                    except Exception:
                        pass
                await _reset_to_idle(
                    "job resume error", final_status=_final_idle_status()
                )
                if not _should_loop():
                    _schedule_exit(delay=2.0)
            finally:
                if _current_job_id == request.job_id:
                    _current_job_id = None

        _current_job_task = asyncio.create_task(_do_resume())
        return JobStartResponse(
            job_id=request.job_id,
            status="accepted",
            message="Job resume started",
        )

    @app.get("/job/current", tags=["Worker"])
    async def get_current_job() -> Dict[str, Any]:
        return {
            "job_id": _current_job_id,
            "is_busy": _pod_state == PodState.WORKING,
        }

    # ===================================================================
    # Session routes (persistent/interactive)
    # ===================================================================

    @app.post("/session/attach", tags=["Session"])
    async def session_attach(request: dict = {}) -> JSONResponse:
        global _pod_state, _session_attach_task, _session_attach_claim

        thread_id = request.get("thread_id")
        if not thread_id:
            return JSONResponse({"error": "thread_id is required"}, status_code=400)
        recipient_refusal = _pinned_session_recipient_refusal(
            request,
            expected_thread_id=str(thread_id),
        )
        if recipient_refusal is not None:
            return recipient_refusal
        runtime_contract = bool(
            type(request.get("pinned_runtime_generation_contract")) is int
            and request["pinned_runtime_generation_contract"] == 1
        )
        try:
            runtime_generation = (
                str(UUID(str(request.get("session_runtime_generation"))))
                if request.get("session_runtime_generation") is not None
                else None
            )
            runtime_attach_token = (
                str(UUID(str(request.get("session_runtime_attach_token"))))
                if request.get("session_runtime_attach_token") is not None
                else None
            )
            workspace_generation = _canonical_optional_runtime_uuid(
                request.get("workspace_generation")
            )
            workspace_runtime_incarnation = _canonical_optional_runtime_uuid(
                request.get("workspace_runtime_incarnation")
            )
        except (TypeError, ValueError):
            return JSONResponse(
                {"error": "exact session runtime identity is required"},
                status_code=409,
            )
        if runtime_contract and (
            runtime_generation is None or runtime_attach_token is None
        ):
            return JSONResponse(
                {"error": "exact session runtime identity is required"},
                status_code=409,
            )
        if bool(workspace_generation) != bool(workspace_runtime_incarnation):
            return JSONResponse(
                {"error": "exact workspace runtime identity is required"},
                status_code=409,
            )

        pod_uid = str(os.environ.get("POD_UID") or "").strip()
        if runtime_contract and not pod_uid:
            return JSONResponse(
                {"error": "exact agent pod identity is required"},
                status_code=409,
            )
        attach_claim = {
            "thread_id": thread_id,
            "session_runtime_generation": runtime_generation,
            "session_runtime_attach_token": runtime_attach_token,
            "agent_pod_uid": pod_uid,
            "workspace_generation": workspace_generation,
            "workspace_runtime_incarnation": workspace_runtime_incarnation,
            "setup_started": False,
        }

        async with _state_lock:
            if _pod_state != PodState.IDLE:
                return JSONResponse(
                    {"error": f"Pod is in {_pod_state.value} state"},
                    status_code=409,
                )
            _pod_state = PodState.SESSION
            _session_attach_claim = attach_claim

        if (
            _orchestrator_client is not None
            and runtime_generation is not None
            and not _orchestrator_client.adopt_session_runtime_identity(
                runtime_generation,
                runtime_attach_token,
                contract_advertised=runtime_contract,
            )
        ):
            async with _state_lock:
                if _same_session_attach_claim(
                    attach_claim, require_setup_started=False
                ):
                    _pod_state = PodState.IDLE
                    _session_attach_claim = None
            return JSONResponse(
                {"error": "exact session runtime identity is required"},
                status_code=409,
            )

        # Actor identity BEFORE any session setup, and synchronously, because
        # the answer decides whether this pod may serve the session at all.
        # A dedicated pod got its actor at registration; a pool pod has to bind
        # its thread-less bootstrap now that it knows the thread. Refusing here
        # is the whole point: provision_or_assign falls through to a dedicated
        # pod, which is slower but correct. Continuing without identity gives a
        # session that boots clean and then denies every machine-tag write —
        # the silent failure that blocked the BP-05 live gate.
        if _orchestrator_client is not None:
            bound = await _orchestrator_client.bind_pod_runtime_actor(thread_id)
            if not bound:
                logger.error(
                    "Refusing session %s: no runtime actor identity for this pod",
                    thread_id,
                )
                if (
                    runtime_generation is not None
                    and runtime_attach_token is not None
                    and _same_session_attach_claim(
                        attach_claim, require_setup_started=False
                    )
                ):
                    # This is the sole weak-proof boundary: no session/backend/
                    # workspace setup task exists yet, and the one-way claim
                    # latch still says setup never began. Retain a tracked
                    # retry owner until the exact G rotation is confirmed.
                    _session_attach_task = asyncio.create_task(
                        _release_unstarted_session_attach(attach_claim),
                        name=f"dual-attach-pre-setup-release:{thread_id}",
                    )
                return JSONResponse(
                    {"error": "pod cannot obtain runtime actor identity"},
                    status_code=409,
                )

        # Respond immediately — heavy setup (workspace polling, session init)
        # runs in the background. The /ready endpoint will report readiness
        # once the session is fully set up.
        async def _setup_session():
            # Without this, `_pod_state = PodState.IDLE` below creates a
            # closure-local variable instead of resetting the module global,
            # leaving the agent permanently stuck reporting `session` status.
            global _pod_state, _session_attach_task, _session_attach_claim
            try:
                import agent.api.persistent_app as pa

                pa._agent = _agent
                pa._orchestrator_client = _orchestrator_client
                pa._config_path = _config_path
                pa._started_at = _started_at

                await pa._attach_session(
                    thread_id=thread_id,
                    config_override=request.get("config_override"),
                    resolved_config=request.get("resolved_config"),
                    project_ids=request.get("project_ids"),
                    datasources=request.get("datasources"),
                    # Thread's config beats this pod's boot config — dual
                    # pool pods boot as workers ('defaults'); see
                    # knowledge-base/knowledge/issues/session_config_name_plumbing.md (hole B).
                    config_name=request.get("config_name"),
                    runtime_actor=request.get("runtime_actor"),
                    pinned_status_identity_contract=request.get(
                        "pinned_status_identity_contract"
                    ),
                    pinned_runtime_generation_contract=request.get(
                        "pinned_runtime_generation_contract"
                    ),
                    session_runtime_generation=runtime_generation,
                    session_runtime_attach_token=runtime_attach_token,
                    workspace_generation=workspace_generation,
                    workspace_runtime_incarnation=workspace_runtime_incarnation,
                )
                logger.info(f"Session setup complete for thread {thread_id}")
            except BaseException as attach_error:
                logger.exception(f"Session setup failed for thread {thread_id}")
                import agent.api.persistent_app as pa

                # _attach_session sets pa._thread_id before it can fail;
                # _detach_session short-circuits when _session is None and
                # never clears _thread_id, so we have to do it explicitly.
                pa._thread_id = None
                # Release the exact reciprocal DB reservation before this pod
                # advertises itself idle.  The endpoint treats an already-
                # replaced binding as a successful/benign no-op, so ``True``
                # means either our pair was cleared or successor authority
                # already won.  A transport failure leaves this pod in
                # SESSION/non-ready rather than creating an idle double-owner.
                release_confirmed = False
                if _orchestrator_client:
                    try:
                        release_confirmed = (
                            await pa._release_failed_attach_receipt_until_confirmed(
                                thread_id,
                                runtime_generation=runtime_generation,
                                runtime_attach_token=runtime_attach_token,
                            )
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to release thread→agent binding for {thread_id}: {e}"
                        )
                if release_confirmed:
                    async with _state_lock:
                        if _same_session_attach_claim(
                            attach_claim, require_setup_started=True
                        ):
                            if _orchestrator_client is not None:
                                _orchestrator_client.clear_runtime_actor()
                                _orchestrator_client.clear_session_runtime_identity(
                                    expected_generation=runtime_generation,
                                    expected_attach_token=runtime_attach_token,
                                )
                            _pod_state = PodState.IDLE
                            _session_attach_claim = None
                # The normal heartbeat loop observes IDLE after the exact
                # claim clear. Do not fire a detached ``ready`` heartbeat here:
                # a successor can claim the pod between this task and the POST,
                # which would overwrite its reserved SESSION state.
                if isinstance(attach_error, asyncio.CancelledError):
                    raise
            finally:
                if _session_attach_task is asyncio.current_task() and (
                    _pod_state is PodState.IDLE or pa._session is not None
                ):
                    _session_attach_task = None

        setup_coro = _setup_session()
        try:
            setup_task = asyncio.create_task(setup_coro)
        except BaseException as task_error:
            setup_coro.close()
            # Task creation itself did not cross the setup boundary. Retire the
            # exact delivered claim with the same pre-setup proof; if delivery
            # is ambiguous this request remains blocked/non-ready until the
            # exact replay is confirmed.
            await _release_unstarted_session_attach(attach_claim)
            logger.error(
                "Failed to schedule session setup for thread %s (%s)",
                thread_id,
                type(task_error).__name__,
            )
            return JSONResponse(
                {"error": "session setup could not be scheduled"},
                status_code=500,
            )
        # ``asyncio.create_task`` never runs the coroutine inline. There is no
        # await between task creation and this one-way flip, so the task cannot
        # enter PersistentSession/backend setup while the weak proof remains
        # available.
        attach_claim["setup_started"] = True
        _session_attach_task = setup_task

        return JSONResponse(
            {
                "status": "attaching",
                "thread_id": thread_id,
            }
        )

    @app.get("/session/status", tags=["Session"])
    async def session_status() -> JSONResponse:
        """Check if session is fully set up and ready for WebSocket.

        Delegates to ``persistent_app._session_ready()`` so /ready (SESSION
        branch), this endpoint, and ``handle_persistent_websocket`` share one
        definition of "ready for a WS" and can't drift.
        """
        import agent.api.persistent_app as pa

        if _pod_state != PodState.SESSION:
            return JSONResponse({"ready": False, "state": _pod_state.value})

        return JSONResponse(
            {
                "ready": pa._session_ready(),
                "thread_id": pa._thread_id,
                "state": "session",
                "turn_in_flight": pa._turn_in_flight(),
            }
        )

    @app.post("/session/status", tags=["Session"])
    async def recipient_bound_session_status(request: dict = {}) -> JSONResponse:
        """Return mutation-authorizing state only to the exact bound life."""

        import agent.api.persistent_app as pa

        expected = pa._canonical_pinned_session_identity_fingerprint(
            request.get("session_identity_fingerprint")
            if isinstance(request, dict)
            else None
        )
        if (
            expected is None
            or pa._current_pinned_session_identity_fingerprint() != expected
        ):
            return JSONResponse(
                {"error": "session_identity_mismatch", "retryable": True},
                status_code=409,
            )
        if _pod_state != PodState.SESSION:
            return JSONResponse(
                {"error": "session_recipient_unavailable", "retryable": True},
                status_code=503,
            )
        return JSONResponse(
            {
                "ready": pa._session_ready(),
                "thread_id": pa._thread_id,
                "state": "session",
                "turn_in_flight": pa._turn_in_flight(),
                "recipient_verified": True,
                "session_identity_fingerprint": expected,
            }
        )

    @app.get("/session/toolset", tags=["Session"])
    async def session_toolset() -> JSONResponse:
        """Same measured toolset read as the dedicated-session app.

        Registered on BOTH apps deliberately. ``/session/status`` exists only
        here, so ``_thread_turn_in_flight`` 404s against every dedicated
        session pod (they run ``--mode persistent``) — a live bug this route
        must not reproduce, since a pool-attached session that answered 404
        would silently downgrade the orchestrator to a prediction.
        """
        import agent.api.persistent_app as pa

        return JSONResponse(pa._session_toolset_report())

    @app.post("/session/detach", tags=["Session"])
    async def session_detach(request: dict = {}) -> JSONResponse:
        global _pod_state

        import agent.api.persistent_app as pa

        expected = pa._canonical_pinned_session_identity_fingerprint(
            request.get("session_identity_fingerprint")
            if isinstance(request, dict)
            else None
        )
        if (
            expected is None
            or pa._current_pinned_session_identity_fingerprint() != expected
        ):
            return JSONResponse(
                {"error": "session_identity_mismatch", "retryable": True},
                status_code=409,
            )

        if _pod_state != PodState.SESSION:
            return JSONResponse({"status": "not_in_session"}, status_code=404)

        try:
            thread_id = pa._thread_id
            # Same reason string as persistent_app's /session/detach so the
            # documented "Terminate(rest_detach)" signal greps identically
            # on dual pool pods (was the "legacy" back-compat shim).
            await pa._terminate_session("rest_detach")

            if _should_loop():
                await _reset_to_idle("session detach", skip_session_cleanup=True)
            else:
                _schedule_exit(delay=2.0)

            return JSONResponse(
                {
                    "status": "detached",
                    "thread_id": thread_id,
                }
            )
        except Exception:
            error_ref = uuid4().hex[:12]
            logger.exception("Failed to detach session (error_ref=%s)", error_ref)
            return JSONResponse(
                {"error": "session detach failed", "error_ref": error_ref},
                status_code=500,
            )

    # --- Persistent-session REST endpoints (orchestrator-driven turns) ---
    #
    # Mirror of /api/{input,interrupt,approve} in persistent_app.py. The
    # orchestrator forwards from POST /api/persistent/threads/{id}/{input,
    # interrupt,approve} to whichever agent is attached to the thread, which
    # in cluster usage is a dual-mode pod. Without these routes the agent
    # returns 404 and the cockpit's turn never lands. See task #136 /
    # knowledge-base/knowledge/issues/persistent_session_dual_mode_phase1_gap.md for the parallel
    # WS-handler gap this duplication caused.

    @app.post("/api/input", tags=["Session"])
    async def api_input(request: Request):
        if _pod_state != PodState.SESSION:
            return JSONResponse(
                {"error": "Pod is not in session mode"}, status_code=404
            )
        import agent.api.persistent_app as pa

        return await pa.handle_api_input(request)

    @app.post("/api/interrupt", tags=["Session"])
    async def api_interrupt(request: Request):
        if _pod_state != PodState.SESSION:
            return JSONResponse(
                {"error": "Pod is not in session mode"}, status_code=404
            )
        import agent.api.persistent_app as pa

        return await pa.handle_api_interrupt(request)

    @app.post("/api/approve", tags=["Session"])
    async def api_approve(request: Request):
        if _pod_state != PodState.SESSION:
            return JSONResponse(
                {"error": "Pod is not in session mode"}, status_code=404
            )
        import agent.api.persistent_app as pa

        return await pa.handle_api_approve(request)

    async def _do_ws_chat(ws: WebSocket) -> None:
        """Shared WS handler for both /ws/chat and /p/{thread_id}/ws.

        The two routes exist so the agent answers both the legacy direct path
        (/ws/chat, used by local dev with websocat and any cluster-internal
        callers) and the external path that the per-session Ingress forwards
        through Traefik (/p/<thread_id>/ws — cockpit's new direct-WS path).
        """
        global _pending_exit_task

        # Validate the session JWT carried as ?t={token}.
        if not await _validate_session_token(ws):
            return

        # Cancel any pending exit from a previous WS disconnect (e.g. page
        # refresh). Vestigial under the Phase-1 keystone — the WS-disconnect
        # path no longer schedules exit — but kept as a no-op safety net for
        # other paths (drain intent, shutdown) that still arm _pending_exit_task.
        if _pending_exit_task and not _pending_exit_task.done():
            _pending_exit_task.cancel()
            _pending_exit_task = None
            logger.info("Cancelled pending exit — new WebSocket connecting")

        if _pod_state != PodState.SESSION:
            await ws.accept()
            await ws.send_json(
                {
                    "method": "error",
                    "params": {"message": "Pod is not in session mode"},
                }
            )
            await ws.close(code=4403, reason="Not in session mode")
            return

        import agent.api.persistent_app as pa

        if not pa._session or not pa._session.llm_with_tools:
            await ws.accept()
            await ws.send_json(
                {
                    "method": "error",
                    "params": {"message": "Session not ready"},
                }
            )
            await ws.close(code=4503, reason="Session not ready")
            return

        # Delegate to the shared module-level handler. Same implementation
        # the pure-persistent route uses — Phase-1 keystone, subscriber
        # model, Phase-5 status flips all included. See
        # knowledge-base/knowledge/issues/persistent_session_dual_mode_phase1_gap.md for why
        # this delegation matters.
        await pa.handle_persistent_websocket(ws)

    @app.websocket("/ws/chat")
    async def ws_chat(ws: WebSocket):
        await _do_ws_chat(ws)

    @app.websocket("/p/{thread_id}/ws")
    async def ws_session(ws: WebSocket, thread_id: str):
        # thread_id is enforced by _validate_session_token against
        # SESSION_BOUND_THREAD_ID + the JWT's tid claim — the path param is
        # only here so the Ingress's /p/<tid>/ws path matches a FastAPI route.
        await _do_ws_chat(ws)

    return app
