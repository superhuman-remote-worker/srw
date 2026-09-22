"""FastAPI application for Persistent Agent mode.

Provides WebSocket endpoint for interactive sessions. Completely separate
from app.py (worker mode) — own globals, own lifespan, no shared state.

Start with: python agent.py --mode persistent --thread-id <uuid> --port 8001
Connect with: websocat ws://localhost:8001/ws/chat
"""

import asyncio
import hashlib
import hmac
import inspect
import json
import logging
import contextlib
import os
import socket
import re
import time
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    NoReturn,
    Optional,
    Set,
    Tuple,
)
from uuid import UUID, uuid4

import httpx
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from agent.api import session_transport as _session_transport
from agent.api._session_auth import validate_session_token as _validate_session_token
from agent.api.models import PinnedSessionRecipient, pinned_session_recipient_matches
from agent.api.orchestrator_client import (
    DuplicateThreadBinding,
    OrchestratorClient,
    SessionEnded,
    SessionGrantDenied,
    ThreadConfigUpdateDenied,
    create_orchestrator_client_from_env,
)
from agent.api.persistent_session import (
    CloudOverlayUnavailable,
    MemoryUnavailableError,
    PersistentSession,
    resolve_memory_extraction_prompt,
)
from agent.tools.registry import TOOL_REGISTRY
from agent.core.archiver import inflight_tool_call
from agent.core.context import extract_summary_text, repair_tool_pairing
from shared.runtime.core.message_markers import is_compaction_view, turn_membership
from shared.runtime.core.skill_resolution import (
    APP_GUIDE_LOADER_TOOL,
    app_guide_health_snapshot,
)
from shared.runtime.core.loader import normalize_delegation_block, normalize_llm_tiers
from shared.runtime.core.tool_policy import (
    normalize_tool_policy,
    validate_tool_override_fragment,
)
from shared.runtime.core.workspace_backend import WorkspaceUnavailableError
from agent.services.workspace_undo import (
    WorkspaceUndoRetryable,
    WorkspaceUndoUnavailable,
)
from agent.api.lease_context import LeaseLostError
from agent.api.lease_context import current_lease as _current_lease_var
from shared import event_journal as _event_journal
from shared.pinned_session_identity import (
    PINNED_SESSION_READY_IDENTITY_CONTRACT,
    pinned_session_ready_identity_fingerprint,
)
from shared.thread_presence import (
    expire_permission_if_untethered,
    mark_stateless_natural_pause,
)
from shared.session_retirement import update_stateless_claim_status
from shared.runtime_actor import RuntimeActorContext
from agent.agent import UniversalAgent

# Re-exported: tests/test_persistent_app.py and tests/test_session_wake_event_role.py
# import the thread_messages serialiser from here (lifted to src/core in U3 WP1
# so the subagent runtime shares it).
from agent.core.thread_messages import (  # noqa: F401
    _ROLE_MAP,
    _extract_thinking,
    _persist_one_message,
    _serialize_message_row,
)
from agent.persistent_graph import (
    INTERRUPT_SENTINEL,
    IdleTimeoutError,
    PermissionOutcome,
    PersistentLoopCallbacks,
    run_persistent_loop,
)

logger = logging.getLogger(__name__)

# --- Module globals ---
# Singleton layer (lives for the entire process lifetime):
_agent: Optional[UniversalAgent] = None
_config_path: Optional[str] = None
_orchestrator_client: Optional[OrchestratorClient] = None
_heartbeat_task: Optional[asyncio.Task] = None
_started_at: Optional[datetime] = None

# Session layer (created/destroyed per thread assignment):
_session: Optional[PersistentSession] = None
_thread_id: Optional[str] = None
_pinned_status_identity_enabled = False
_pinned_runtime_generation_enabled = False
_session_runtime_generation: Optional[str] = None
_session_runtime_attach_token: Optional[str] = None
# Process-local mirror of the orchestrator's exact ``ending`` authority. It is
# scoped to the complete attached runtime identity, not the process: pool/dual
# agents may safely serve a successor after exact cleanup clears this tuple.
_retirement_admission_identity: Optional[tuple[str, Optional[str], Optional[str]]] = (
    None
)
_retirement_admission_disposition: Optional[str] = None
_retirement_admission_token: Optional[str] = None
_retirement_admission_permanent: Optional[bool] = None
# An exact drain-suspend can outlive local teardown when the settlement
# response is lost. Keep that process nonclaimable and retry the same immutable
# identity from a tracked task independent of normal heartbeat authority;
# never downgrade it to a broad End.
_pending_drain_suspend: Optional[dict[str, Any]] = None
_pending_drain_suspend_retry_task: Optional[asyncio.Task[None]] = None

# Pool-mode attach admission is deliberately split from the heavy attach
# transaction.  The orchestrator holds its datasource-selection advisory lock
# while POSTing /session/attach.  A synchronous attach polls the workspace
# endpoint, which takes that same lock, so the two processes deadlock.  Claim
# the empty process synchronously, return ``attaching``, then let the fully
# exception-safe attach transaction run after the orchestrator has released
# its lock.  The claim is process-local and set before the 200 response: a
# second POST can never slip into the setup window.
_pool_attach_lock = asyncio.Lock()
_pool_attach_claim: Optional[str] = None
_pool_attach_runtime_generation: Optional[str] = None
_pool_attach_token: Optional[str] = None
_pool_attach_task: Optional[asyncio.Task[None]] = None
# Exact proof retained between exception-safe attach rollback and the
# orchestrator's generation-rotating release CAS. Never infer it from a
# swallowed cleanup error or from an absent session after globals were reset.
_failed_attach_release_receipt: Optional[dict[str, Any]] = None
# Mutable only while one exact delivered attach is constructing. It records
# the one-way setup boundary plus the attested workspace coordinates needed to
# prove a partial sandbox runtime writer-free even when PersistentSession was
# not constructed yet. Never log this mapping: it contains transport paths.
_failed_attach_workspace_cleanup_context: Optional[dict[str, Any]] = None


def _retain_failed_attach_release_receipt(receipt: dict[str, Any]) -> bool:
    """Install one immutable attach-abort proof without replacing a claimant.

    Dual mode can prove that actor binding failed before its one-way setup latch
    flipped; the normal attach rollback installs a stronger process-zero proof.
    In both cases a delayed failure from G1 must never replace G2's retained
    proof, so equality is benign/idempotent and every other incumbent wins.
    """

    global _failed_attach_release_receipt

    if not isinstance(receipt, dict):
        return False
    incumbent = _failed_attach_release_receipt
    if incumbent is not None:
        return incumbent == receipt
    _failed_attach_release_receipt = dict(receipt)
    return True


def _pool_heartbeat_status() -> str:
    """Advertise a synchronous pool claim before ``_session`` exists."""

    return (
        "ready"
        if _session is None
        and _pool_attach_claim is None
        and _pending_drain_suspend is None
        and _failed_attach_release_receipt is None
        else "session"
    )


def _pinned_status_identity_advertised(payload: Any) -> bool:
    """Accept only the exact numeric v1 additive lifecycle capability."""

    return bool(
        isinstance(payload, dict)
        and type(payload.get("pinned_status_identity_contract")) is int
        and payload["pinned_status_identity_contract"] == 1
    )


def _pinned_runtime_generation_advertised(payload: Any) -> bool:
    """Accept only the exact numeric v1 runtime-generation capability."""

    return bool(
        isinstance(payload, dict)
        and type(payload.get("pinned_runtime_generation_contract")) is int
        and payload["pinned_runtime_generation_contract"] == 1
    )


def _canonical_runtime_generation(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return str(UUID(value.strip()))
    except (TypeError, ValueError):
        return None


_ATTACH_WORKSPACE_IDENTITY_UNSET = object()


def _canonical_attach_workspace_identity(
    workspace_generation: Any,
    workspace_runtime_incarnation: Any,
) -> Optional[Tuple[Optional[str], Optional[str]]]:
    """Canonicalize an explicitly delivered workspace-authority pair.

    ``None`` as the return value means the caller omitted the contract (the
    dedicated startup path). ``(None, None)`` means the claim bundle captured
    no physical identity yet; shared attach may still poll a workspace whose
    session-runtime generation is authoritative. Keeping those states distinct
    prevents a compatibility default from weakening a delivered exact pair.
    """

    generation_omitted = workspace_generation is _ATTACH_WORKSPACE_IDENTITY_UNSET
    incarnation_omitted = (
        workspace_runtime_incarnation is _ATTACH_WORKSPACE_IDENTITY_UNSET
    )
    if generation_omitted or incarnation_omitted:
        if generation_omitted and incarnation_omitted:
            return None
        raise WorkspaceNotReady(
            "Attach workspace identity must be delivered as an exact pair"
        )
    if workspace_generation is None and workspace_runtime_incarnation is None:
        return (None, None)

    canonical_generation = _canonical_runtime_generation(workspace_generation)
    canonical_incarnation = _canonical_runtime_generation(workspace_runtime_incarnation)
    if canonical_generation is None or canonical_incarnation is None:
        raise WorkspaceNotReady("Attach workspace identity is malformed or incomplete")
    return canonical_generation, canonical_incarnation


def _assert_attach_workspace_tier(
    expected: Optional[Tuple[Optional[str], Optional[str]]],
    *,
    is_lite_session: bool,
) -> None:
    """Reject an exact physical claim for a resolved no-workspace tier."""

    if expected is None or expected == (None, None):
        return
    if is_lite_session:
        raise WorkspaceNotReady(
            "Attach workspace identity does not match the resolved workspace tier"
        )


def _assert_attach_workspace_payload(
    expected: Optional[Tuple[Optional[str], Optional[str]]],
    payload: Any,
) -> None:
    """Fence an observed workspace response to the delivered claim identity."""

    if expected is None or expected == (None, None):
        return
    raw_generation = (
        payload.get("workspace_generation") if isinstance(payload, dict) else None
    )
    raw_incarnation = (
        payload.get("workspace_runtime_incarnation")
        if isinstance(payload, dict)
        else None
    )
    if raw_generation is None and raw_incarnation is None:
        observed: Tuple[Optional[str], Optional[str]] = (None, None)
    else:
        canonical_generation = _canonical_runtime_generation(raw_generation)
        canonical_incarnation = _canonical_runtime_generation(raw_incarnation)
        if canonical_generation is None or canonical_incarnation is None:
            raise WorkspaceNotReady(
                "Observed workspace identity is malformed or incomplete"
            )
        observed = canonical_generation, canonical_incarnation
    if observed != expected:
        raise WorkspaceNotReady("Workspace identity changed during attach")


def _adopt_attached_runtime_identity(
    generation: Any,
    attach_token: Any = None,
    *,
    contract_advertised: bool,
) -> None:
    """Install one exact runtime identity before any attach-side await."""

    global _session_runtime_generation, _session_runtime_attach_token
    global _pinned_runtime_generation_enabled
    global _retirement_admission_identity, _retirement_admission_disposition
    global _retirement_admission_token
    global _retirement_admission_permanent

    canonical_generation = _canonical_runtime_generation(generation)
    canonical_token = (
        _canonical_runtime_generation(attach_token)
        if attach_token is not None
        else None
    )
    if contract_advertised and (
        canonical_generation is None
        or (not _stateless_mode() and canonical_token is None)
    ):
        raise WorkspaceNotReady(
            "Pinned runtime generation contract omitted its exact generation "
            "or attach token"
        )
    if generation is not None and canonical_generation is None:
        raise WorkspaceNotReady("Pinned runtime generation is malformed")
    if attach_token is not None and canonical_token is None:
        raise WorkspaceNotReady("Pinned runtime attach token is malformed")
    _session_runtime_generation = canonical_generation
    _session_runtime_attach_token = canonical_token
    _pinned_runtime_generation_enabled = contract_advertised
    _retirement_admission_identity = None
    _retirement_admission_disposition = None
    _retirement_admission_token = None
    _retirement_admission_permanent = None
    client = _orchestrator_client
    if client is not None and canonical_generation is not None:
        adopt = getattr(client, "adopt_session_runtime_identity", None)
        if not callable(adopt) or not adopt(
            canonical_generation,
            canonical_token,
            contract_advertised=contract_advertised,
        ):
            raise WorkspaceNotReady("Pinned runtime identity could not be adopted")


def _clear_attached_runtime_identity(
    *,
    expected_generation: str | None = None,
    expected_attach_token: str | None = None,
) -> bool:
    """Clear only a captured generation, never a successor's authority."""

    global _session_runtime_generation, _session_runtime_attach_token
    global _pinned_runtime_generation_enabled
    global _retirement_admission_identity, _retirement_admission_disposition
    global _retirement_admission_token
    global _retirement_admission_permanent

    if (
        expected_generation is not None
        and _session_runtime_generation != expected_generation
    ):
        return False
    if (
        expected_attach_token is not None
        and _session_runtime_attach_token != expected_attach_token
    ):
        return False
    client = _orchestrator_client
    if client is not None:
        clear = getattr(client, "clear_session_runtime_identity", None)
        if callable(clear) and not inspect.iscoroutinefunction(clear):
            clear(
                expected_generation=expected_generation,
                expected_attach_token=expected_attach_token,
            )
    _session_runtime_generation = None
    _session_runtime_attach_token = None
    _pinned_runtime_generation_enabled = False
    _retirement_admission_identity = None
    _retirement_admission_disposition = None
    _retirement_admission_token = None
    _retirement_admission_permanent = None
    return True


def _bind_attached_runtime_payload(
    payload: Any,
    *,
    protected_required: bool,
) -> str | None:
    """Fence a workspace response to this attach's exact runtime life."""

    if not isinstance(payload, dict):
        raise WorkspaceNotReady("Workspace runtime identity is unavailable")
    advertised = _pinned_runtime_generation_advertised(payload)
    raw_generation = payload.get("session_runtime_generation")
    generation = _canonical_runtime_generation(raw_generation)
    if raw_generation is not None and generation is None:
        raise WorkspaceNotReady("Workspace runtime generation is malformed")
    if advertised and generation is None:
        raise WorkspaceNotReady(
            "Workspace runtime generation contract omitted its generation"
        )
    if _pinned_runtime_generation_enabled and not advertised:
        raise WorkspaceNotReady("Workspace runtime generation contract disappeared")
    if (
        _session_runtime_generation is not None
        and generation != _session_runtime_generation
    ):
        raise WorkspaceNotReady("Workspace runtime generation changed during attach")
    if _session_runtime_generation is None and generation is not None:
        _adopt_attached_runtime_identity(
            generation,
            _session_runtime_attach_token,
            contract_advertised=advertised,
        )
    if protected_required and (
        not advertised or generation is None or _session_runtime_generation is None
    ):
        raise ProtectedCloudUnavailable(
            "Protected workspace has no exact runtime generation"
        )
    return generation


# Pool mode: agent can be reused across sessions (Docker Compose mode)
_sessions_served: int = 0
_max_sessions_per_process: int = int(
    os.environ.get("MAX_SESSIONS_PER_PROCESS", "0")
)  # 0 = unlimited

# Pod exit scheduling
_pending_exit_task: Optional[asyncio.Task] = None

# Drain intent — set the first time the orchestrator's heartbeat response
# carries ``intents.should_drain=true`` AND the session is in a drainable
# state. Drives a one-shot suspend/detach + exit so the agent doesn't keep
# reacting on every subsequent heartbeat. While a turn is in flight the
# intent is deferred (flag stays False) and re-checked on each 5s tick.
_drain_intent_handled: bool = False
_drain_deferred_logged: bool = False

# Kubernetes termination-admission fence for dedicated persistent pods.
#
# The pod's preStop hook creates this sentinel before it makes an HTTP request
# back into the event loop.  Reading the file at every admission boundary means
# a busy loop does not have to process the callback before new provider work is
# refused.  The boolean is the fast in-process half, set by the loopback route.
# Neither is durable authority: Kubernetes owns pod termination; Post/thread
# state and the Officer recycler own durable replacement.
_TERMINATION_SENTINEL_PATH = Path("/tmp/srw-persistent-terminating")
_termination_admission_fenced: bool = False
_termination_fence_reason: Optional[str] = None
_TERMINATION_QUEUE_SENTINEL = INTERRUPT_SENTINEL

# Process-local authorization latch for auxiliary provider work. The primary
# loop must remain able to reach its pre-turn maintenance callback after a
# failure, so this is deliberately separate from the termination/provider
# fence used by ``run_persistent_loop``. Commissioned Officers start closed,
# open only after successful server maintenance, and close again immediately
# on any failed maintenance result. AuxiliaryLLM re-checks the latch at the
# actual provider boundary, including work queued before the failure.
_runtime_authorization_admission_open: bool = False

# One process incarnation inside one pod. Kubernetes may restart a container
# without changing the pod UID; this value lets the successor reclaim work
# that existed only in the predecessor's RAM queue. The separate session
# generation fences the durable thread binding. Reset on every attach.
_input_runtime_generation: Optional[str] = None
_queued_input_claims: set[tuple[str, int]] = set()
# Serializes durable reclaim with local queue/priority publication. In
# particular, a concurrent human B may not steal a just-deferred wake A between
# A's generation CAS and its priority reclaim.
_input_delivery_reclaim_lock = asyncio.Lock()
_INPUT_CANCELLATION_ENABLED_ENV = "PERSISTENT_INPUT_CANCELLATION_ENABLED"


def _persistent_input_cancellation_enabled() -> bool:
    """Writer rollout gate; readers must deploy fleet-wide before enabling."""

    return os.environ.get(_INPUT_CANCELLATION_ENABLED_ENV, "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


# True exactly while the persistent loop is parked in _loop_get_user_input's
# queue wait — the only state where an out-of-band teardown (drain-suspend)
# cannot kill user-visible work mid-turn.
_awaiting_input: bool = False

# Self-cleanup watchdogs (PR 2 — protect against the abandoned-pod failure modes
# that the orchestrator reconciler can only catch with a 60s+ delay):
#   _ws_connected_event  → set when /ws/chat first accepts a connection.
#   _watchdog_tasks      → background tasks cancelled on detach/shutdown.
_ws_connected_event: Optional[asyncio.Event] = None
_watchdog_tasks: list[asyncio.Task] = []

# Awaitable single-flight for _terminate_session. Out-of-band teardown (drain,
# watchdog, REST detach) cancels the loop task and awaits it — but
# run_persistent_loop swallows CancelledError during the input wait and
# returns CLEANLY, so the loop's completion-handler wrapper observes a
# normal exit and re-enters _terminate_session("loop_complete") while the
# outer teardown is still in progress. Historically that just duplicated
# work (double sync/cleanup, duplicate 'ended' writes); under drain-suspend
# the inner call's 'ended' write would defeat the orchestrator's
# 'suspended' transition. The loop task itself remains a non-awaiting
# re-entrant caller (otherwise outer teardown and cancelled loop deadlock),
# while every independent caller awaits the same authoritative cleanup result.
_terminating: bool = False
_termination_task: Optional[asyncio.Task[None]] = None

# Reference to the currently running persistent-loop task. Set by ws_chat when
# it spawns the loop, cleared when _terminate_session runs. _terminate_session()
# cancels and awaits it before nulling _session, so out-of-band callers
# (heartbeat intents, thread-status watchdog, drain) can't race the in-flight
# turn into a NoneType.permission_mode crash. See
# knowledge-base/knowledge/issues/persistent_session_permission_check_race.md.
#
# Headless sessions (chunk 1): the loop now outlives any single WebSocket. It is
# only cancelled by _terminate_session, never by WS close.
_loop_task: Optional[asyncio.Task] = None
_session_boot_ws_timeout_s: int = int(
    os.environ.get("SESSION_BOOT_WS_TIMEOUT_S", "600")
)
_thread_status_poll_s: int = int(os.environ.get("THREAD_STATUS_POLL_S", "60"))

# Resume backstop: a hard ceiling on how many messages the restore read loads,
# applied as `seq DESC LIMIT N` (newest N) so one pathological tail — thousands
# of messages with no usable summary/boundary — can't OOM the agent on resume
# (the exit-137 wedge). This is a floor, NOT the mechanism: boundary_seq (Path A)
# normally bounds the tail to ~keep_recent, well under this. Generous so it never
# trims a healthy session; logged when it does. Tunable via env.
_resume_message_limit: int = int(os.environ.get("RESUME_MESSAGE_LIMIT", "1000"))

# How long the live VM-upgrade handler waits for a KubeVirt VM to become ready
# before giving up and tearing it down. The default sandbox-container poll is
# ~300s, but a *cold* VM pays a fresh ~2.8GB CDI registry import per DataVolume
# (helm vm-controller template `source.registry`), which routinely exceeds 5min.
# Tunable so warm clusters (or a future golden-image `sourceRef` clone — see
# workspace_tier_upgrade.md Q7) can dial it back down. (workspace_tier_upgrade.md
# Phase 2 / Q7.)
_vm_upgrade_poll_timeout: int = int(os.environ.get("VM_UPGRADE_POLL_TIMEOUT", "900"))

# Subscriber registry for headless persistent sessions.
#
# Loop-driven output (token chunks, tool events, turn lifecycle, etc.) used to
# be sent directly to a single WebSocket scoped to ws_chat. Under headless
# semantics the loop must outlive any single WS attach, so the loop instead
# broadcasts via _broadcast() and each WebSocket connection registers its own
# queue via _subscribe(). A _run_subscriber_pump task drains each queue into
# its WS. Closing a WS just calls _unsubscribe() — the loop keeps running.
#
# Keyed by client_id (generated server-side per WS connection). Bounded queue
# protects the loop from a slow consumer: on overflow the oldest frame is
# dropped (token-stream pacing semantics).
_SUBSCRIBER_QUEUE_MAXSIZE: int = 1000
_subscribers: Dict[str, asyncio.Queue] = {}

_CANVAS_AWARENESS_TTL_S: float = max(
    15.0, min(60.0, float(os.environ.get("CANVAS_AWARENESS_TTL_S", "15")))
)
_CANVAS_CONTROL_VALIDATION_MIN_INTERVAL_S = 0.5
_CANVAS_AWARENESS_RENEW_MIN_INTERVAL_S = 1.0


@dataclass(frozen=True)
class _CanvasAwarenessLease:
    task: asyncio.Task
    params: Dict[str, Any]
    renewed_at: float
    validated_at: float


_canvas_awareness: Dict[str, _CanvasAwarenessLease] = {}
_canvas_control_validation_at: Dict[tuple[str, str], float] = {}
_canvas_source_updates: Dict[str, tuple[str, int, str]] = {}
_canvas_presentation_updates: Dict[str, int] = {}

# Idle keepalive on the control WS (see _run_subscriber_pump). Must be
# shorter than the cockpit's CONTROL_WS_WATCHDOG_TIMEOUT_MS and any
# edge/tunnel idle timeout on the WS path.
_WS_PING_INTERVAL_S: float = 20.0

# Loop-facing input primitives. Used to be closure-scoped inside ws_chat;
# hoisted to module level so they survive WS reconnect. All three are reset
# on session attach / cleared on _terminate_session.
_loop_user_queue: Optional[asyncio.Queue] = None
# Tri-state interrupt flag (phase 2): None = no interrupt pending,
# "graceful" = stop after current tool call completes, "hard" = cancel the
# in-flight LLM stream immediately and drop the partial AIMessage. Set by
# the agent's POST /api/interrupt handler based on current _tool_inflight
# state. Consumed by persistent_graph's check_interrupt callback at three
# sites (pre-LLM, mid-astream, between tool calls). Legacy WS interrupt
# path uses the same flag — sets "hard" when no tool is inflight.
_loop_interrupt_flag: Optional[str] = None
# Exact transcript turn the pending interrupt belongs to. A mode without a
# matching target is invalid and is cleared rather than allowed to strike a
# successor turn.
_loop_interrupt_target_turn_id: Optional[int] = None
# Hard-interrupt signal (phase 3). Set alongside _loop_interrupt_flag="hard"
# so the loop can tear down a blocked LLM / auxiliary await (e.g. a hung
# summarization read) immediately, instead of waiting for the cooperative
# check_interrupt poll — which can't fire while the turn is parked in a
# network read. Created in _attach_session, set in the interrupt handlers
# when no tool is in flight, cleared whenever the flag is consumed.
_hard_interrupt_event: Optional[asyncio.Event] = None
_loop_last_user_content: List[str] = [""]

# Serializes rewinds: two concurrent rewind frames on one session would race
# the sweep/truncate pair. Second caller gets an error, not a queue.
_rewind_lock: asyncio.Lock = asyncio.Lock()

# The LLM-free draft title written at submit time (_early_title_from_prompt), so
# the authoritative after-turn pass knows it may overwrite *this* value while
# still leaving a manual rename untouched. None once no draft is outstanding
# (never written, or already replaced by the real title). Process memory only:
# if the pod restarts between the two passes the draft simply sticks, which is a
# fine title — not worth a DB column.
_draft_title_value: Optional[str] = None
# Session-local fire-and-forget work must not survive pool reuse.  Every task
# captures an immutable attach generation and is terminally joined during
# teardown before the event writer or claimant lease is released.
_session_generation: int = 0
_session_side_tasks: set[asyncio.Task[Any]] = set()
_protected_input_reclaim_task: Optional[asyncio.Task[Any]] = None


def _track_session_side_task(task: asyncio.Task[Any]) -> asyncio.Task[Any]:
    _session_side_tasks.add(task)
    task.add_done_callback(_session_side_tasks.discard)
    return task


def _session_identity_matches(
    session: Any,
    thread_id: str,
    generation: int,
) -> bool:
    return bool(
        _session is session
        and _thread_id == thread_id
        and _session_generation == generation
    )


async def _quiesce_session_side_tasks() -> None:
    """Cancel and join title/stage tasks before process-global identity reuse."""

    pending = {
        task
        for task in _session_side_tasks
        if task is not asyncio.current_task() and not task.done()
    }
    if not pending:
        return
    for task in pending:
        task.cancel()
    done, pending = await asyncio.wait(
        pending,
        timeout=float(os.environ.get("SESSION_SIDE_TASK_CLOSE_TIMEOUT_S", "5")),
    )
    for task in done:
        try:
            task.result()
        except (asyncio.CancelledError, Exception):
            pass
    if pending:
        raise RuntimeError(f"{len(pending)} session side task(s) ignored cancellation")


# True while a tool call is mid-`ainvoke`. Read by POST /api/interrupt to
# pick hard vs graceful mode. Latched only after the final execution-admission
# callback succeeds, immediately before `tool.ainvoke`; cleared in
# _loop_on_tool_result.
_tool_inflight: bool = False

# Exact stateless claim/turn identity after a bound, approved tool reaches the
# pre-ainvoke external-effect boundary. Unlike _tool_inflight this survives the
# post-tool/pre-next-provider and turn-settled/pre-complete gaps. It is
# monotonic until the executor durably completes/parks/releases that exact
# queue claim (or a new turn/attach/termination proves the old runtime gone),
# so shutdown cannot replay a possibly side-effecting tool merely because the
# interrupt watcher has already closed.
_turn_tool_execution_identity: Optional[Tuple[str, int, int]] = None


def _record_turn_tool_execution_identity() -> Optional[Tuple[str, int, int]]:
    """Latch the exact stateless claim/turn crossing the tool-effect seam."""

    global _turn_tool_execution_identity
    handle = _current_lease_var.get()
    if (
        handle is None
        or handle.unit_id is None
        or int(handle.lease_token) <= 0
        or _thread_id is None
        or str(_thread_id) != str(handle.unit_id)
        or _session is None
    ):
        # Pinned sessions have no queue claim and need no shutdown replay
        # classification. A malformed stateless runtime fails conservative:
        # it never invents an identity from process-global watcher fields.
        return None
    turn_id = getattr(_session, "turn_count", None)
    if isinstance(turn_id, bool) or not isinstance(turn_id, int) or turn_id <= 0:
        return None
    identity = (str(handle.unit_id), int(handle.lease_token), int(turn_id))
    _turn_tool_execution_identity = identity
    return identity


def _turn_tool_execution_started_for_claim(
    *,
    thread_id: str,
    lease_token: int,
    turn_id: Optional[int] = None,
) -> bool:
    """Whether the exact claim (and optionally turn) crossed the tool seam."""

    identity = _turn_tool_execution_identity
    if identity is None:
        return False
    expected = (str(thread_id), int(lease_token))
    if identity[:2] != expected:
        return False
    return turn_id is None or identity[2] == int(turn_id)


def _turn_tool_execution_identity_for_claim(
    *,
    thread_id: str,
    lease_token: int,
) -> Optional[Tuple[str, int, int]]:
    """Return the exact matching identity, never a truthy approximation."""

    identity = _turn_tool_execution_identity
    if identity is None or identity[:2] != (str(thread_id), int(lease_token)):
        return None
    return identity


def _clear_turn_tool_execution_identity(
    *,
    thread_id: Optional[str] = None,
    lease_token: Optional[int] = None,
    turn_id: Optional[int] = None,
) -> bool:
    """Clear all state, or only the named exact claim/turn identity."""

    global _turn_tool_execution_identity
    identity = _turn_tool_execution_identity
    if identity is None:
        return False
    if thread_id is not None and identity[0] != str(thread_id):
        return False
    if lease_token is not None and identity[1] != int(lease_token):
        return False
    if turn_id is not None and identity[2] != int(turn_id):
        return False
    _turn_tool_execution_identity = None
    return True


# True from the UI turn.started edge until its terminal turn.completed/error
# edge. Deliberately narrower than _turn_in_flight(): that safety helper stays
# true through turn-end persistence/git callbacks, after the transcript turn is
# already closed. Cockpit reattach needs the transcript lifecycle, or it can
# incorrectly reopen a completed turn during that post-turn window.
_turn_event_open: bool = False

# Cloud sync failed to start at attach (no sync target resolved, or the initial
# pull raised) and is worth retrying at a turn boundary. Without this the
# session keeps ``workspace_sync = None`` for its WHOLE life — every use site is
# guarded by ``if _session.workspace_sync:`` and nothing ever rebuilds it — so a
# few-seconds-late sync target (or a transient WebDAV blip) cost the user every
# push and pull of the session.
# knowledge-history/done/session_resume_cloud_sync_race_late_provision.md
_cloud_sync_retry_pending: bool = False

# The turn-end cloud push runs as a background task so the loop can park —
# and the next queued input can start its turn — without waiting on WebDAV
# round-trips (a fresh pod's first push took minutes and made queued sends
# look swallowed). Awaited by the next turn's start hook before its pull
# (strict push(N)→pull(N+1) ordering per mount, never a concurrent walk of
# the same dedup state) and by every teardown path before the final
# push_all/aclose. At most one task is pending at a time: the only spawner
# runs after the previous turn's task was awaited.
# knowledge-base/knowledge/issues/session_turn_end_cloud_push_blocks_queued_input.md
_pending_cloud_push_task: Optional[asyncio.Task] = None

# Commit-then-effects (stateless_turn_resilience.md step 4a). The stateless
# turn-end push is STAGED (workspace read into temp files) while the unit is
# still leased — `_pending_cloud_push_staged` is set at that point — and then
# handed its own fence by the executor before complete_unit: the task moves
# from `_pending_cloud_push_task` into `_background_cloud_pushes` (keyed by
# thread), keeps the coordinator it transmits through, and the session may be
# detached. No journal frames are written after that hand-off; push state is
# read from thread_cloud_sync_generations by /connection instead.
_pending_cloud_push_staged: Optional[asyncio.Event] = None
_pending_cloud_push_claim: Optional["_CloudGenerationClaim"] = None
_pending_cloud_push_sync: Any = None
_background_cloud_pushes: Dict[str, "_BackgroundCloudPush"] = {}

# M3 full-turn-settlement seam for the stateless executor (turn_executor.py): a
# synchronous callable invoked by ``_loop_on_turn_settled`` only after the
# transcript reconcile, turn-owned memory work, and Git commit/push/ledger
# mapping. The executor installs a closure that sets an asyncio.Event per claim;
# the pinned lane leaves this None. Publishing this at transcript completion is
# too early: the executor can detach/cancel the loop and strand an unmapped Git
# commit, breaking cross-pod undo.
_turn_complete_external_hook: Optional[Callable[[int], None]] = None

# S2 turn-start seam for the stateless executor. Unlike the completion seam,
# this hook is awaited: the exact lease/turn interrupt admission must be open
# and its consumer armed before ``turn.started`` makes the turn interruptible
# to clients. Pinned sessions leave it unset.
_turn_start_external_hook: Optional[Callable[[int], Awaitable[None]]] = None

# Exact tool-effect seam for the stateless executor. The process-local
# identity below is cleared by physical session detach before the executor's
# queue completion CAS. A stateless claimant therefore installs this
# synchronous hook and keeps its own exact identity until complete/park/release
# reaches Postgres. Pinned sessions leave it unset.
_turn_tool_execution_external_hook: Optional[Callable[[Tuple[str, int, int]], None]] = (
    None
)

# Phase 2 event-log cursor. Allocated synchronously by _broadcast, then queued
# through one ordered writer so a later sequence can never become visible in
# Postgres before an earlier queued sequence. Each DB-backed runtime attach
# resolves (epoch, seq seed) via _resolve_event_journal_epoch — REUSING the
# thread's current epoch on clean reattaches and bumping only when the prior
# session life is terminal (doc §5.3.2); teardown clears the process-local
# cursor.
_events_epoch: int = 0
_next_seq: int = 0
_event_writer: Optional["_OrderedPersistentEventWriter"] = None

# Durable control-inbox consumer. Pinned agents own this for their whole
# attach; stateless agents own it only while turn_executor holds a live lease.
# The task is deliberately separate from the persistent loop so a control can
# change the next permission gate while an LLM turn is already running.
_control_watcher_task: Optional[asyncio.Task] = None
_control_watcher_stop: Optional[asyncio.Event] = None
_control_owner_lease_token: Optional[int] = None
_control_owner_agent_id: Optional[str] = None
_control_drain_lock: asyncio.Lock = asyncio.Lock()

_CONTROL_NOTIFY_CHANNEL = "thread_control_requests"
_CONTROL_POLL_SECONDS = 5.0
_CONTROL_STOP_GRACE_SECONDS = 1.0

# A stateless interrupt belongs to one exact (lease token, turn id) pair. Its
# consumer is deliberately independent of the scalar-control watcher: an
# interrupt has a synchronous RAM side effect and its admission window closes
# before queue completion, not at attach/detach.
_interrupt_watcher_task: Optional[asyncio.Task] = None
_interrupt_watcher_stop: Optional[asyncio.Event] = None
_interrupt_owner_lease_token: Optional[int] = None
_interrupt_owner_turn_id: Optional[int] = None
_interrupt_drain_lock: asyncio.Lock = asyncio.Lock()
_interrupt_watcher_lifecycle_lock: asyncio.Lock = asyncio.Lock()

_INTERRUPT_NOTIFY_CHANNEL = "thread_interrupt_requests"
_INTERRUPT_POLL_SECONDS = 1.0

_STALE_PERMISSION_EXISTS_SQL = """
SELECT EXISTS (
    SELECT 1
    FROM thread_permission_requests
    WHERE thread_id = $1::uuid
      AND status = 'pending'
      AND (accepted_lease_token IS NULL
           OR (accepted_lease_token > 0
               AND accepted_lease_token < $2::bigint))
)
"""

_EVENT_WRITER_QUEUE_MAXSIZE: int = int(
    os.environ.get("THREAD_EVENT_WRITER_QUEUE_MAXSIZE", "10000")
)
_EVENT_WRITER_BATCH_SIZE: int = int(
    os.environ.get("THREAD_EVENT_WRITER_BATCH_SIZE", "100")
)
_EVENT_WRITER_STATE_MAX_ATTEMPTS: int = int(
    os.environ.get("THREAD_EVENT_WRITER_STATE_MAX_ATTEMPTS", "3")
)
_EVENT_WRITER_RETRY_BASE_S: float = float(
    os.environ.get("THREAD_EVENT_WRITER_RETRY_BASE_S", "0.1")
)
_EVENT_WRITER_CLOSE_TIMEOUT_S: float = float(
    os.environ.get("THREAD_EVENT_WRITER_CLOSE_TIMEOUT_S", "10")
)

# ---------------------------------------------------------------------------
# NATS notification publishing (Direct Session WebSockets, Task 9)
# ---------------------------------------------------------------------------
# The agent pod mirrors notification-worthy _broadcast events onto NATS subject
# ``session.events.{tid}`` so the orchestrator's nats_bridge (Task 5) can
# re-broadcast them onto the SSE notification feed. Replaces the orchestrator's
# old per-WS-frame inspection. Non-fatal: if NATS is unconfigured or down,
# WS subscribers still get the event — only the SSE notification mirror is lost.
import json as _json  # noqa: E402
import os as _os  # noqa: E402

_nats_client = None  # Lazily initialized.

# Methods that the orchestrator's nats_bridge subscribes to and forwards
# to the SSE notification feed. Keep in sync with the event_type_map in
# orchestrator/services/nats_bridge.py:_on_session_event.
_NOTIFICATION_METHODS = frozenset(
    {
        "permission.request",
        "vm_upgrade.needed",
        "workspace_upgrade.needed",
        "approve",
        "deny",
        "ready",
    }
)

# Roles POST /api/input may request. 'human' is the normal path; 'event' is a
# system-injected notice (currently: a worker job this session created reached a
# terminal state — knowledge-base/knowledge/features/session_wake_on_job_completion.md).
#
# An allow-list rather than a passthrough because ordinary /api/input is an
# internal orchestrator effect endpoint rather than a browser-token endpoint.
# Durable events must name the exact runtime fingerprint and also require the
# existing internal transport key below. Human requests that carry the new
# fingerprint are fenced identically; the missing-field compatibility branch
# remains only until the orchestrator forwarding cutover is atomic. An
# arbitrary role would let anything that can reach the pod forge 'ai' or
# 'system' rows.
_ACCEPTED_INPUT_ROLES = frozenset({"human", "event"})


def _stateless_mode() -> bool:
    """True when this process runs as the M3 stateless turn executor.

    Set via ``STATELESS_EXECUTOR=1`` (agent.py ``--mode stateless`` exports
    it). In this mode the pod claims ``session_turn`` units from the shared
    ``run_queue`` (``src/agent/api/turn_executor.py``) instead of being registered,
    heartbeated, watched and driven over WS/REST: registration, the
    orchestrator heartbeat loop, the boot-WS/status watchdogs, and the
    direct input/attach surface are all disabled. Read per call (not cached
    at import) so tests can flip it with monkeypatch.setenv.
    """
    return os.environ.get("STATELESS_EXECUTOR", "").strip() == "1"


def _current_stateless_lease_token() -> Optional[int]:
    """Return the exact live claim token for this attached stateless thread."""

    if not _stateless_mode() or _thread_id is None:
        return None
    handle = _current_lease_var.get()
    if (
        handle is None
        or not handle.active
        or handle.lost.is_set()
        or str(handle.unit_id) != str(_thread_id)
    ):
        return None
    return int(handle.lease_token)


async def _safe_mark_stateless_natural_pause(*, require_untethered: bool) -> bool:
    """Best-effort durable presence oracle for stateless natural pauses."""

    if _session is None or _session.postgres_conn is None or _thread_id is None:
        return False
    lease_token = _current_stateless_lease_token()
    if lease_token is None:
        logger.warning(
            "Skipped stateless natural pause without exact lease (thread=%s)",
            _thread_id,
        )
        return False
    try:
        return await mark_stateless_natural_pause(
            _session.postgres_conn,
            thread_id=_thread_id,
            lease_token=lease_token,
            require_untethered=require_untethered,
        )
    except Exception as exc:
        # Lifecycle state is converged by the orchestrator's expired-presence
        # sweep after run_queue reaches done; never fail a completed turn for
        # this advisory status write.
        logger.warning(
            "Failed stateless natural-pause presence check (thread=%s): %s",
            _thread_id,
            exc,
        )
        return False


def _stateless_reject() -> JSONResponse:
    """409 for direct-session verbs on a stateless executor pod."""
    return JSONResponse(
        {
            "error": (
                "stateless executor: this pod serves queued turns from the "
                "run_queue (threads.execution_lane='stateless'); direct "
                "session attach/input is not accepted here"
            )
        },
        status_code=409,
    )


class WorkspaceNotReady(RuntimeError):
    """The session's workspace container never became ready in time.

    Subclasses RuntimeError so existing ``except RuntimeError`` handlers (e.g.
    the pool-mode /session/attach path) keep catching it, while the lifespan
    startup can catch it specifically to exit cleanly instead of crash-looping.
    """


class ProtectedCloudUnavailable(WorkspaceNotReady):
    """A protected attach cannot prove its lower+overlay delivery contract.

    This is terminal for the current attach.  Retrying a partially described
    protected payload as an ordinary workspace would expose credentials and
    tools before the review boundary exists, so callers must fail closed.
    """


_PROTECTED_CLOUD_SAFE_ERROR_CODES = frozenset(
    {
        "feature_disabled",
        "malformed_protected_marker",
        "unsupported_workspace_tier",
        "no_protected_mount",
        "engage_refused",
        "engage_failed",
    }
)


def _protected_workspace_marker(payload: Dict[str, Any]) -> str:
    """Classify the additive workspace marker without truthiness coercion."""

    if "protected_cloud" not in payload or payload.get("protected_cloud") is False:
        return "off"
    if payload.get("protected_cloud") is True:
        return "on"
    raise ProtectedCloudUnavailable("malformed protected-cloud workspace marker")


def _validate_protected_cloud_mount(payload: Any) -> Dict[str, Any]:
    """Return an exact protected lower+overlay payload or fail closed."""

    if not isinstance(payload, dict):
        raise ProtectedCloudUnavailable("protected-cloud mount payload is missing")
    overlay = payload.get("overlay")
    mounts = payload.get("mounts")
    if (
        type(payload.get("version")) is not int
        or payload.get("version") != 1
        or payload.get("driver") != "rclone"
        or payload.get("protected") is not True
        or payload.get("skip_workspace_links") is not True
        or payload.get("fallback") is not False
        or not isinstance(overlay, dict)
        or overlay.get("lower") != "/cloud/lower"
        or overlay.get("merged") != "/cloud/merged"
        or overlay.get("upper") != "/home/agent-host/.overlay/upper"
        or overlay.get("work") != "/home/agent-host/.overlay/work"
        or not isinstance(overlay.get("quota_bytes"), int)
        or isinstance(overlay.get("quota_bytes"), bool)
        or overlay.get("quota_bytes") <= 0
        or not isinstance(mounts, list)
        or len(mounts) != 1
    ):
        raise ProtectedCloudUnavailable("protected-cloud mount payload is malformed")

    lowers = [
        mount
        for mount in mounts
        if isinstance(mount, dict) and mount.get("mount_kind") == "protected_lower"
    ]
    if len(lowers) != 1:
        raise ProtectedCloudUnavailable(
            "protected-cloud payload must contain exactly one protected lower"
        )
    lower = lowers[0]
    source = lower.get("source")
    source_config = source.get("config") if isinstance(source, dict) else None
    auth = lower.get("auth")
    if (
        not isinstance(lower.get("mount_id"), str)
        or not lower.get("mount_id")
        or lower.get("backend") != "nextcloud"
        or lower.get("target_path") != "/cloud/lower"
        or lower.get("workspace_name") != "lower"
        or lower.get("access") != "read_only"
        or not isinstance(source, dict)
        or source.get("type") != "webdav"
        or not isinstance(source_config, dict)
        or source_config.get("vendor") != "nextcloud"
        or not isinstance(source_config.get("url"), str)
        or not source_config.get("url")
        or not isinstance(source_config.get("user"), str)
        or not source_config.get("user")
        or not isinstance(auth, dict)
        or auth.get("type") != "basic"
        or not isinstance(auth.get("password"), str)
        or not auth.get("password")
    ):
        raise ProtectedCloudUnavailable("protected-cloud lower mount is malformed")
    return payload


def _protected_workspace_delivery(payload: Dict[str, Any]) -> str:
    """Return ``off``, ``engaging`` or ``ready`` for a workspace response."""

    if _protected_workspace_marker(payload) == "off":
        mount = payload.get("cloud_mount")
        protected_mount_shape = False
        if isinstance(mount, dict):
            mounts = mount.get("mounts")
            protected_mount_shape = (
                ("protected" in mount and mount.get("protected") is not False)
                or "overlay" in mount
                or (
                    isinstance(mounts, list)
                    and any(
                        isinstance(candidate, dict)
                        and candidate.get("mount_kind") == "protected_lower"
                        for candidate in mounts
                    )
                )
            )
        if (
            payload.get("protected_cloud_state") is not None
            or payload.get("protected_cloud_error_code") is not None
            or protected_mount_shape
        ):
            raise ProtectedCloudUnavailable(
                "protected-cloud payload has no authoritative marker"
            )
        return "off"
    state = payload.get("protected_cloud_state")
    status = payload.get("status")
    if state in {"engaging", "failed"}:
        # Pending/refused responses are a deliberately tiny, coordinate-free
        # projection.  Use an allowlist rather than chasing aliases: attach
        # consumes ``remote.host`` directly, and one forgotten transport key
        # would otherwise bypass a blacklist without ever being inspected.
        allowed_non_ready = {
            "status",
            "protected_cloud",
            "protected_cloud_state",
            "protected_cloud_error_code",
            "pod_ip",
            "pod_name",
            "pod_port",
            "namespace",
            "vm_status",
            "vm_ssh_host",
            "vm_ssh_port",
            "vm_name",
            "ssh_key_path",
            "workspace_generation",
            "workspace_runtime_incarnation",
            "workspace_ssh_host_key_fingerprint",
            "git_remote_url",
            "managed_repository_credentials",
            "repositories",
            "resolved_config",
            "config_override",
            "project_ids",
            "datasources",
            "nc_session_folder",
            "cloud_sync",
            "cloud_mount",
            "cloud_sync_degraded",
            "canvas_presentation_available",
            "canvas_live_apps_available",
            "canvas_shared_browser_available",
        }
        neutral_none = allowed_non_ready - {
            "status",
            "protected_cloud",
            "protected_cloud_state",
            "protected_cloud_error_code",
            "project_ids",
            "cloud_sync_degraded",
            "canvas_presentation_available",
            "canvas_live_apps_available",
            "canvas_shared_browser_available",
        }
        if (
            any(key not in allowed_non_ready for key in payload)
            or any(payload.get(key) is not None for key in neutral_none)
            or ("project_ids" in payload and payload.get("project_ids") != [])
            or any(
                payload.get(key) is not False
                for key in (
                    "cloud_sync_degraded",
                    "canvas_presentation_available",
                    "canvas_live_apps_available",
                    "canvas_shared_browser_available",
                )
                if key in payload
            )
        ):
            raise ProtectedCloudUnavailable(
                "protected-cloud non-ready payload exposed runtime coordinates"
            )
    if state == "engaging" and status == "creating":
        return "engaging"
    if state == "failed" and status == "failed":
        code = payload.get("protected_cloud_error_code")
        safe_code = (
            code
            if isinstance(code, str) and code in _PROTECTED_CLOUD_SAFE_ERROR_CODES
            else "engage_failed"
        )
        raise ProtectedCloudUnavailable(
            f"protected-cloud engage was refused ({safe_code})"
        )
    if state != "ready" or status != "ready":
        raise ProtectedCloudUnavailable(
            "protected-cloud workspace has no authoritative ready state"
        )
    declared_backends: list[str] = []
    if "backend" in payload:
        direct_backend = payload.get("backend")
        if not isinstance(direct_backend, str):
            raise ProtectedCloudUnavailable(
                "protected cloud workspace backend declaration is malformed"
            )
        declared_backends.append(direct_backend)
    override = payload.get("config_override")
    if override is not None:
        if not isinstance(override, dict):
            raise ProtectedCloudUnavailable(
                "protected cloud workspace override is malformed"
            )
        workspace = override.get("workspace")
        if workspace is not None:
            if not isinstance(workspace, dict) or not isinstance(
                workspace.get("backend"), str
            ):
                raise ProtectedCloudUnavailable(
                    "protected cloud workspace override is malformed"
                )
            declared_backends.append(workspace["backend"])
    resolved = payload.get("resolved_config")
    if resolved is not None:
        if not isinstance(resolved, dict):
            raise ProtectedCloudUnavailable(
                "protected cloud resolved config is malformed"
            )
        agent = resolved.get("agent")
        if agent is not None and not isinstance(agent, dict):
            raise ProtectedCloudUnavailable(
                "protected cloud resolved agent config is malformed"
            )
        workspace = agent.get("workspace") if isinstance(agent, dict) else None
        if workspace is not None:
            if not isinstance(workspace, dict) or not isinstance(
                workspace.get("backend"), str
            ):
                raise ProtectedCloudUnavailable(
                    "protected cloud resolved workspace config is malformed"
                )
            declared_backends.append(workspace["backend"])
    if (
        not declared_backends
        or any(backend != "sandbox" for backend in declared_backends)
        or not isinstance(payload.get("pod_ip"), str)
        or not payload.get("pod_ip")
        or (
            payload.get("pod_port") is not None
            and (
                type(payload.get("pod_port")) is not int
                or not 1 <= payload.get("pod_port") <= 65535
            )
        )
        or payload.get("vm_status") not in (None, "none")
        or payload.get("vm_ssh_host") is not None
        or payload.get("vm_ssh_port") is not None
        or payload.get("vm_name") is not None
        or _canonical_runtime_generation(payload.get("workspace_generation")) is None
        or _canonical_runtime_generation(payload.get("workspace_runtime_incarnation"))
        is None
        or not isinstance(payload.get("workspace_ssh_host_key_fingerprint"), str)
        or not payload.get("workspace_ssh_host_key_fingerprint")
        or not _pinned_runtime_generation_advertised(payload)
        or _canonical_runtime_generation(payload.get("session_runtime_generation"))
        is None
    ):
        raise ProtectedCloudUnavailable(
            "protected cloud requires an exact sandbox workspace"
        )
    remote = payload.get("remote")
    if remote is not None:
        expected_port = payload.get("pod_port") or 30022
        expected_key = payload.get("ssh_key_path") or "/run/secrets/vm-ssh-key"
        if (
            not isinstance(remote, dict)
            or set(remote)
            - {
                "host",
                "port",
                "username",
                "key_path",
                "workspace_path",
            }
            or remote.get("host") != payload.get("pod_ip")
            or remote.get("port") != expected_port
            or remote.get("username") != "agent-host"
            or remote.get("key_path") != expected_key
            or remote.get("workspace_path") != "/home/agent-host/workspace"
        ):
            raise ProtectedCloudUnavailable(
                "protected cloud remote endpoint does not match its attestation"
            )
    if (
        payload.get("cloud_sync") is not None
        or payload.get("nc_session_folder") is not None
    ):
        raise ProtectedCloudUnavailable(
            "protected-cloud payload exposed a legacy live-write surface"
        )
    _validate_protected_cloud_mount(payload.get("cloud_mount"))
    return "ready"


@dataclass(frozen=True, slots=True)
class _ProtectedWorkspaceIdentity:
    """Exact protected workspace + mount bytes authorized for one attach."""

    pod_ip: str
    pod_port: int
    workspace_generation: str
    runtime_incarnation: str
    host_fingerprint: str
    session_runtime_generation: str
    cloud_mount_json: str


def _protected_workspace_identity(
    payload: Dict[str, Any],
) -> _ProtectedWorkspaceIdentity:
    if _protected_workspace_delivery(payload) != "ready":
        raise ProtectedCloudUnavailable("protected workspace is not ready")
    return _ProtectedWorkspaceIdentity(
        pod_ip=payload["pod_ip"],
        pod_port=payload.get("pod_port") or 30022,
        workspace_generation=str(UUID(payload["workspace_generation"])),
        runtime_incarnation=str(UUID(payload["workspace_runtime_incarnation"])),
        host_fingerprint=payload["workspace_ssh_host_key_fingerprint"],
        session_runtime_generation=str(UUID(payload["session_runtime_generation"])),
        cloud_mount_json=json.dumps(
            payload["cloud_mount"],
            sort_keys=True,
            separators=(",", ":"),
        ),
    )


class EventJournalUnavailable(RuntimeError):
    """The persistent event generation could not be resolved authoritatively."""


class ControlInboxBlocked(EventJournalUnavailable):
    """The oldest control cannot be safely consumed by this runtime owner."""


class InterruptInboxBlocked(EventJournalUnavailable):
    """An exact-turn interrupt cannot be safely consumed by this owner."""


async def _ensure_nats_client():
    """Lazy NATS connection. Returns None if NATS is unconfigured."""
    global _nats_client
    if _nats_client is not None:
        return _nats_client
    url = _os.environ.get("NATS_URL")
    if not url:
        return None
    try:
        import nats

        _nats_client = await nats.connect(url)
        return _nats_client
    except Exception as e:
        logger.warning("agent pod: NATS connect failed: %s", e)
        return None


def _officer_cfg():
    """Return this session's OfficerConfig when officer.enabled, else None.

    Centurion sessions (knowledge-base/knowledge/features/centurion.md): the flag decides the
    officer branches in the input wait, the natural-pause flips, the boot
    self-wake, and the ready-mirror suppression.
    """
    if _session is None:
        return None
    # Double getattr: attach-path callers (boot-WS watchdog, boot self-wake)
    # run against test FakeSessions that may not carry .config at all.
    cfg = getattr(getattr(_session, "config", None), "officer", None)
    # Strict `is True`: tests wire sessions with MagicMock configs, and a
    # truthy Mock attribute must not flip a normal session into officer mode.
    return cfg if getattr(cfg, "enabled", False) is True else None


def _terminal_retirement_disposition() -> str:
    """Choose the immutable outcome before beginning pinned retirement."""

    identity = _attached_retirement_identity()
    if (
        identity is not None
        and _retirement_admission_identity == identity
        and _retirement_admission_disposition in {"ended", "suspended"}
    ):
        return _retirement_admission_disposition
    return "suspended" if _officer_cfg() is not None else "ended"


async def emit_session_event(method: str, params: dict) -> None:
    """Publish a notification event to ``session.events.{oid}.{tid}`` on NATS.

    Mirrors what _broadcast does to WS subscribers but for the
    orchestrator's bridge. Methods not in _NOTIFICATION_METHODS are
    skipped. Failures are non-fatal: if NATS is down, the WS subscribers
    still get the event. If NATS is configured but ORCHESTRATOR_ID is
    unset the publish is refused (the bridge subscribes to scoped subjects
    and won't see flat ones — better to log a warning than silently no-op).
    """
    if method not in _NOTIFICATION_METHODS:
        return
    if method == "ready" and _officer_cfg() is not None:
        # An officer parks after every wake (~48×/day on timer alone).
        # Mirroring each park into the SSE notification feed as
        # session.waiting is pure spam (centurion.md §4); WS subscribers
        # watching the log still receive the plain "ready" broadcast.
        return
    tid = _os.environ.get("SESSION_BOUND_THREAD_ID", "")
    if not tid:
        return
    nc = await _ensure_nats_client()
    if not nc:
        return
    oid = (_os.environ.get("ORCHESTRATOR_ID") or "").strip()
    if not oid:
        logger.warning(
            "agent pod: ORCHESTRATOR_ID unset — refusing session.events publish "
            "(would publish to a subject the orchestrator does not subscribe to)"
        )
        return
    payload = {
        "thread_id": tid,
        "method": method,
        "params": params,
        "orchestrator_id": oid,
    }
    try:
        await nc.publish(f"session.events.{oid}.{tid}", _json.dumps(payload).encode())
    except Exception as e:
        logger.warning("agent pod: NATS publish failed: %s", e)


def _turn_in_flight() -> bool:
    """True while the loop is executing a turn (not parked waiting for input).

    Exposed via the agent's ``/status`` and ``/session/status`` so the
    orchestrator's ``end_thread`` can refuse to tear down a session that is
    mid-turn without an explicit ``force``
    (knowledge-base/knowledge/issues/session_silent_failure_audit.md #11).
    """
    return _loop_task is not None and not _loop_task.done() and not _awaiting_input


def _session_toolset_report() -> dict:
    """The toolset this session ACTUALLY bound, for the orchestrator to serve.

    D6 (``knowledge-base/knowledge/issues/tool_configuration_defects_and_fix_roadmap.md``): the
    resolved answer comes from the agent, not from an orchestrator-side
    re-implementation.  Everything that decides the final set happens HERE and
    only here — the runtime injection layer (``_load_tools_for_backend``
    appends the session-task trio, the product guide, the fleet/catalog/
    workflow lists, ``srw_cloud_status``, the officer pair, …),
    ``filter_tools_by_backend``, and ``load_tools``'s per-tool instantiation
    fallback.  A view rebuilt from YAML over-reports by dozens of names.

    Read off the live tool objects rather than any name list, because
    ``apply_description_overrides`` / ``apply_instruction_enforcement`` rebind
    ``self.tools`` and the never-bind-zero floor can replace it outright.  What
    the model is offered is the only thing worth reporting.

    ``attached`` is false when no session is bound: an honest "nothing to
    measure" beats a confident empty toolset.
    """
    from shared.runtime.core.tool_report import build_agent_toolset_report

    session = _session
    if session is None:
        return {
            "attached": False,
            "thread_id": _thread_id,
            "report": None,
        }
    names = [
        tool.name
        for tool in (session.tools or [])
        if isinstance(getattr(tool, "name", None), str)
    ]
    backend = getattr(getattr(session, "workspace_manager", None), "backend", None)
    report = build_agent_toolset_report(
        thread_id=getattr(session, "thread_id", None) or _thread_id,
        tool_names=names,
        backend=backend,
    )
    return {"attached": True, "thread_id": report["thread_id"], "report": report}


def _app_guide_health() -> dict[str, str]:
    """Return bounded product-guide delivery health for agent diagnostics."""

    metadata = TOOL_REGISTRY.get(APP_GUIDE_LOADER_TOOL)
    reader_available = (
        isinstance(metadata, dict) and metadata.get("category") == "product_help"
    )
    return app_guide_health_snapshot(reader_available=reader_available)


def _termination_admission_closed() -> bool:
    """Return the earliest process-visible Kubernetes termination signal.

    ``deletionTimestamp`` itself lives outside the container, so this function
    deliberately does not claim to observe the API-server mutation atomically.
    The preStop shell creates the sentinel before Python/HTTP work; the route
    then latches the in-process boolean.  Either one closes admission.
    """

    if _termination_admission_fenced:
        return True
    try:
        return _TERMINATION_SENTINEL_PATH.exists()
    except OSError:
        # A broken pod-local fence path is not a reason to spend through a
        # termination signal.  The normal /tmp path is always stat-able.
        return True


def _attached_retirement_identity() -> tuple[str, Optional[str], Optional[str]] | None:
    if _thread_id is None:
        return None
    return (
        str(_thread_id),
        _session_runtime_generation,
        _session_runtime_attach_token,
    )


def _retirement_admission_closed() -> bool:
    """True only for the exact attached life whose End has begun."""

    identity = _attached_retirement_identity()
    return identity is not None and _retirement_admission_identity == identity


def _runtime_admission_closed() -> bool:
    """Combined local fence for user/control/provider work.

    Kubernetes termination is process-scoped. An owner End is session-scoped
    so a pool agent can safely accept a later exact attach after cleanup.
    """

    return _termination_admission_closed() or _retirement_admission_closed()


def activate_termination_admission_fence(source: str) -> bool:
    """Latch the no-new-turn fence and wake an idle queue waiter.

    Returns True only for the first process-local transition.  Repeated preStop
    callbacks/signals are idempotent and never consume a queued user/event row.
    """

    global _termination_admission_fenced, _termination_fence_reason
    first = not _termination_admission_fenced
    _termination_admission_fenced = True
    if _termination_fence_reason is None:
        _termination_fence_reason = str(source or "termination")[:80]
    if first:
        logger.warning(
            "Persistent runtime admission fenced for termination (source=%s, "
            "turn_open=%s, tool_inflight=%s)",
            _termination_fence_reason,
            _turn_event_open,
            _tool_inflight,
        )
    # queue.get() otherwise has no reason to wake and notice the file/flag.
    # The sentinel is filtered by the loop and is never persisted.
    if _awaiting_input and _loop_user_queue is not None:
        try:
            _loop_user_queue.put_nowait(_TERMINATION_QUEUE_SENTINEL)
        except asyncio.QueueFull:  # pragma: no cover - production queue unbounded
            pass
    return first


def _termination_quiescent() -> bool:
    """True after the current turn's complete settlement boundary."""

    if _tool_inflight or _turn_event_open:
        return False
    session = _session
    if session is not None:
        auxiliary = getattr(session, "auxiliary_llm", None)
        if int(getattr(auxiliary, "provider_calls_inflight", 0) or 0) > 0:
            return False
        memory = getattr(session, "memory_service", None)
        if int(getattr(memory, "background_tasks_inflight", 0) or 0) > 0:
            return False
    task = _loop_task
    return task is None or task.done() or _awaiting_input


async def _wait_for_termination_quiescence(timeout_seconds: float) -> bool:
    """Wait within preStop's grace budget; never cancel an active tool."""

    deadline = asyncio.get_running_loop().time() + max(0.0, timeout_seconds)
    while not _termination_quiescent():
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            return False
        await asyncio.sleep(min(0.05, remaining))
    return True


def _termination_rejection() -> JSONResponse:
    """Stable, base64/secret-free retry contract for direct injectors."""

    return JSONResponse(
        {
            "error": "runtime_terminating",
            "retryable": True,
            "message": "Persistent runtime is terminating; retry on its replacement.",
        },
        status_code=503,
        headers={"Retry-After": "5"},
    )


def _protected_cloud_runtime_ready() -> bool:
    """Fail-closed runtime check shared by input, provider and tool gates."""

    session = _session
    if session is None:
        return False
    required = getattr(session, "protected_cloud_required", False)
    if required is False:
        return True
    if required is not True:
        return False
    try:
        return session.protected_cloud_ready() is True
    except Exception:
        return False


def _runtime_input_admission_open() -> bool:
    return not _runtime_admission_closed() and _protected_cloud_runtime_ready()


def _protected_cloud_unavailable_rejection() -> JSONResponse:
    return JSONResponse(
        {
            "error": "protected_cloud_unavailable",
            "retryable": True,
            "message": "Protected cloud is temporarily unavailable; retry when it recovers.",
        },
        status_code=503,
        headers={"Retry-After": "5"},
    )


def _current_pinned_session_identity_fingerprint() -> str | None:
    """Return the exact local identity used by readiness and effect gates."""

    return pinned_session_ready_identity_fingerprint(
        thread_id=_thread_id,
        runtime_generation=_session_runtime_generation,
        agent_id=_registered_pinned_agent_id(),
        runtime_attach_token=_session_runtime_attach_token,
        pod_uid=os.environ.get("POD_UID"),
    )


def _canonical_pinned_session_identity_fingerprint(value: Any) -> str | None:
    """Return one exact v1 fingerprint, never a normalised lookalike."""

    if not isinstance(value, str) or not (
        value.startswith("sha256:")
        and len(value) == 71
        and all(char in "0123456789abcdef" for char in value[7:])
    ):
        return None
    return value


def _session_ready() -> bool:
    """True when the persistent session is fully attached and the loop
    primitives are ready to accept a WS subscriber.

    Three-way check: ``_session.llm_with_tools`` is set near the end of
    ``PersistentSession.setup()``, but ``_loop_user_queue`` is initialized
    later in ``_attach_session`` (after repo clone, cloud sync pull, message
    restore, and the ``thread_status='active'`` DB update). Anything that
    gates session readiness — the readiness probes (``/ready``,
    ``/session/status``) and ``handle_persistent_websocket`` — must call
    this so the WS isn't accepted in the mid-attach window where the loop's
    get-user-input callback would crash on a ``None`` queue.
    """
    return (
        not _runtime_admission_closed()
        and _session is not None
        and _session.llm_with_tools is not None
        and _loop_user_queue is not None
        and _protected_cloud_runtime_ready()
    )


def _ensure_persistent_loop_started(
    source: str,
    client_id: str | None = None,
) -> bool:
    """Start the persistent turn loop once the session is fully attached.

    Headless/SSE clients drive turns through REST and may never attach the
    legacy control WebSocket. The loop therefore cannot be WebSocket-owned.
    """
    global _loop_task

    if not _session_ready():
        return False

    if _loop_task is None or _loop_task.done():
        # Compaction progress frames (started/progress/failed) ride the
        # broadcast/SSE channel; idempotent, also set by _handle_compact for
        # manual compaction before the first loop start. getattr: test
        # doubles stub context_manager with plain objects.
        _cb_setter = getattr(_session.context_manager, "set_progress_callback", None)
        if callable(_cb_setter):
            _cb_setter(_loop_compaction_progress)
        callbacks = PersistentLoopCallbacks(
            get_user_input=_loop_get_user_input,
            on_token=_loop_on_token,
            on_thinking=_loop_on_thinking,
            on_tool_start=_loop_on_tool_start,
            on_tool_execution_start=_loop_on_tool_execution_start,
            on_tool_result=_loop_on_tool_result,
            permission_check=_loop_permission_check,
            announce_permission_batch=_loop_announce_permission_batch,
            on_turn_start=_loop_on_turn_start,
            on_turn_complete=_loop_on_turn_complete,
            on_error=_loop_on_error,
            check_interrupt=_loop_check_interrupt,
            on_workspace_upgrade_needed=_loop_on_workspace_upgrade_needed,
            on_workspace_commit=_loop_on_workspace_commit,
            on_context_compacted=_loop_on_context_compacted,
            persist_message=_loop_persist_message,
            require_delegation_persistence=True,
            on_turn_settled=_loop_on_turn_settled,
            archive_llm_call=_loop_archive_llm_call,
            on_usage=_loop_on_usage,
            hard_interrupt_event=_hard_interrupt_event,
            on_thinking_reset=_loop_on_thinking_reset,
            before_turn_authorization=_loop_before_turn_authorization,
            before_provider_admission=_loop_provider_admission_open,
            before_provider_execution=_loop_runtime_effect_authority_current,
            admit_input_delivery=_loop_admit_input_delivery,
            defer_input_delivery=_loop_defer_input_delivery,
            cancel_input_delivery=_loop_cancel_input_delivery,
            defer_and_requeue_input_delivery=(_loop_defer_and_requeue_input_delivery),
            settle_input_delivery=_loop_settle_input_delivery,
        )
        # Tag the loop task — and the turn/aux tasks it spawns, which copy this
        # context at creation — with thread_id for log correlation.
        from agent.core.logging_config import bind_log_context, reset_log_context

        _ctx_token = bind_log_context(thread_id=_thread_id)
        _loop_task = asyncio.create_task(
            run_persistent_loop(
                llm_with_tools=_session.llm_with_tools,
                tools=_session.tools,
                context_manager=_session.context_manager,
                config=_session.config,
                system_prompt=_session.system_prompt,
                callbacks=callbacks,
                messages=_session.messages,
                auxiliary_llm=_session.auxiliary_llm,
                recall_store=_session.recall_store,
                knowledge_store=_session.knowledge_store,
                project_id=_session.project_id,
                project_ids=_session.project_ids,
                tool_context=_session.tool_context,
                initial_turn_count=_session.turn_count,
                get_current_tools=lambda: (
                    _session.llm_with_tools,
                    _session.tools,
                ),
                get_current_context=lambda: (
                    _session.context_manager,
                    _session.config,
                    _session.auxiliary_llm,
                ),
                get_current_system_prompt=lambda: _session.system_prompt,
                memory_extraction_prompt=_session.memory_extraction_prompt,
                memory_service=_session.memory_service,
                claim_memory_extraction_interval=(
                    (
                        lambda turn_count,
                        interval: _session.postgres_conn.claim_memory_extraction_interval(
                            _session.thread_id,
                            turn_count=turn_count,
                            interval=interval,
                        )
                    )
                    if _session.postgres_conn is not None
                    else None
                ),
                defer_memory_extraction_to_outbox=_stateless_mode(),
                memory_thread_id=_session.thread_id,
            ),
            name="persistent-loop",
        )
        asyncio.create_task(
            _loop_completion_handler(_loop_task),
            name="persistent-loop-completion",
        )
        logger.info(
            "Persistent loop started: thread=%s source=%s",
            _thread_id,
            source,
        )
        reset_log_context(_ctx_token)
        return True

    if client_id:
        logger.info(
            "Persistent loop already running, attached as subscriber client=%s",
            client_id[:8],
        )
    else:
        logger.info(
            "Persistent loop already running: thread=%s source=%s",
            _thread_id,
            source,
        )
    return True


async def _handle_heartbeat_intents(response: dict[str, Any]) -> None:
    """Heartbeat-response callback: react to orchestrator-set intents.

    Currently only ``should_drain`` triggers anything. What it does depends
    on session state:

    - No session attached → exit the pod (idle pool agent, nothing to save).
    - Session attached, loop parked between turns → clean drain-suspend:
      flush + teardown, then ask the orchestrator to snapshot the workspace
      and mark the thread ``suspended`` so the next user input walks the
      proven suspended-resume path on a fresh (new-build) agent. Falls back
      to the legacy ``ended`` detach if the orchestrator can't suspend.
    - Session attached, turn in flight → defer; re-checked on every 5s
      heartbeat until the loop parks. A drain never kills a running turn.

    Idempotent: fires once per process; later heartbeats observing the same
    intent are no-ops. See
    knowledge-base/knowledge/issues/session_agent_drift_drain_kills_idle_sessions.md.
    """
    global _drain_intent_handled, _drain_deferred_logged
    if _drain_intent_handled:
        return
    intents = response.get("intents") or {}
    if not isinstance(intents, dict):
        return
    if not intents.get("should_drain"):
        return
    reason = intents.get("drain_reason", "unspecified")

    pending = _pending_drain_suspend
    if pending is not None and pending.get("locally_quiesced") is True:
        # Begin makes ordinary heartbeats authority-refused, so they cannot be
        # the retry clock. Rejoin/start the one tracked exact settlement task;
        # it uses capped backoff and keeps this runtime non-ready throughout.
        _drain_intent_handled = True
        await asyncio.shield(_start_pending_exact_drain_suspend_retry())
        return

    if _session is not None and not _session_parked():
        if not _drain_deferred_logged:
            logger.info(
                "Drain intent received from orchestrator (reason=%s) but a "
                "turn is in flight — deferring until the loop parks",
                reason,
            )
            _drain_deferred_logged = True
        return

    _drain_intent_handled = True
    if _session is None:
        logger.info(
            "Drain intent received from orchestrator (reason=%s) — no session "
            "attached, exiting",
            reason,
        )
        _schedule_exit(delay=1.0)
        return

    logger.info(
        "Drain intent received from orchestrator (reason=%s) — suspending "
        "session and exiting",
        reason,
    )
    await _drain_suspend_session()


def _session_parked() -> bool:
    """True when the persistent loop is parked waiting for user input.

    Parked = blocked in _loop_get_user_input's queue wait with nothing
    queued and no tool call in flight. Anything else counts as an active
    turn and must not be torn down out-of-band.
    """
    if not _awaiting_input or _tool_inflight:
        return False
    if _runtime_admission_closed():
        # Queue contents remain durable/deferred for the replacement.  They do
        # not make the predecessor active once termination admission is closed.
        return True
    queue = _loop_user_queue
    return queue is None or queue.empty()


async def _drain_suspend_session() -> None:
    """Drain an attached idle session via clean suspend instead of kill.

    Converges on the attention-sleep terminal state — thread ``suspended``,
    workspace snapshotted to S3, both pods gone — so the next user input
    resumes through the existing suspended-restore path instead of racing a
    half-deleted workspace pod (the 409→503 "session ended" failure this
    replaces).
    """
    global _drain_intent_handled, _pending_drain_suspend

    thread_id = _thread_id
    runtime_agent_id = _registered_pinned_agent_id()
    runtime_generation = _session_runtime_generation
    runtime_attach_token = _session_runtime_attach_token
    runtime_session = _session

    # Suspend is a terminal retirement disposition too. Close/drain durable
    # controls and install the exact retirement token before touching the
    # loop, workspace, mounts, or transport. A failed begin leaves the current
    # session intact so a later heartbeat can retry safely.
    if not await _begin_exact_session_retirement(
        pinned_agent_id=runtime_agent_id,
        retirement_disposition="suspended",
    ):
        _drain_intent_handled = False
        logger.warning(
            "Drain-suspend could not begin exact retirement; local teardown "
            "suppressed (thread=%s)",
            thread_id,
        )
        return

    if runtime_generation is not None and runtime_attach_token is not None:
        _pending_drain_suspend = {
            "thread_id": thread_id,
            "agent_id": runtime_agent_id,
            "session_runtime_generation": runtime_generation,
            "session_runtime_attach_token": runtime_attach_token,
            "session_runtime_retirement_token": _retirement_admission_token,
            "workspace_generation": str(
                getattr(runtime_session, "workspace_generation", "") or ""
            ),
            "workspace_runtime_incarnation": str(
                getattr(runtime_session, "workspace_runtime_incarnation", "") or ""
            ),
            "locally_quiesced": False,
        }

    # Flush + teardown WITHOUT marking the thread ended — the orchestrator
    # owns the 'suspended' transition and its durable lifecycle frame below.
    # Publishing that outcome from the agent before the settlement CAS would
    # lie on a failed snapshot/cleanup. Clearing _session here also
    # makes the SIGTERM shutdown handler a no-op when the orchestrator
    # deletes this pod as part of the suspend.
    try:
        await _terminate_session(
            "drain",
            mark_thread=False,
            preserve_shell=False,
        )
    except Exception as e:
        logger.warning(f"Session teardown during drain-suspend failed: {e}")
        # The exact ``ending`` token remains the durable authority fence. Do
        # not ask the orchestrator to snapshot/delete/settle while local
        # producers, the ordered writer, or mount managers may still be live.
        # Leaving this unhandled lets the same runtime retry quiescence; the
        # retirement reconciler is the cross-process backstop.
        _drain_intent_handled = False
        return

    if _pending_drain_suspend is not None:
        _pending_drain_suspend["locally_quiesced"] = True
        _pending_drain_suspend["local_quiescence_protocol"] = str(
            getattr(runtime_session, "local_quiescence_protocol", "") or ""
        )
        await asyncio.shield(_start_pending_exact_drain_suspend_retry())
        return

    suspended = False
    if _orchestrator_client and thread_id:
        try:
            suspended = await _orchestrator_client.suspend_thread(
                thread_id,
                pinned_agent_id=runtime_agent_id,
                session_runtime_generation=runtime_generation,
                session_runtime_attach_token=runtime_attach_token,
            )
        except Exception as e:
            logger.warning(f"Drain-suspend request failed: {e}")
    if (
        not suspended
        and thread_id
        and (runtime_generation is None or runtime_attach_token is None)
    ):
        # Legacy fallback: mark ended (recoverable — the orchestrator's
        # 'ended' handler snapshots best-effort via _suspend_thread_resources
        # and refuses to clobber an already-'suspended' thread, so a lost
        # suspend response can't end a suspended session). Uses the captured
        # thread_id — _update_thread_status reads module globals that
        # _terminate_session already cleared.
        logger.warning(
            "Drain-suspend unavailable for thread %s — falling back to "
            "legacy ended detach",
            thread_id,
        )
        if _orchestrator_client:
            try:
                await _orchestrator_client.update_thread_status(
                    thread_id,
                    "ended",
                    pinned_agent_id=runtime_agent_id,
                    session_runtime_generation=runtime_generation,
                    session_runtime_attach_token=runtime_attach_token,
                )
            except Exception as e:
                logger.warning(f"Fallback ended write failed: {e}")
    _schedule_exit(delay=1.0)


def _start_pending_exact_drain_suspend_retry() -> asyncio.Task[None]:
    """Own one exact post-quiescence suspend retry loop."""

    global _pending_drain_suspend_retry_task

    task = _pending_drain_suspend_retry_task
    if task is not None and not task.done():
        return task
    task = asyncio.create_task(
        _retry_pending_exact_drain_suspend(),
        name="exact-drain-suspend-settlement",
    )
    _pending_drain_suspend_retry_task = task
    return task


async def _retry_pending_exact_drain_suspend() -> None:
    """Retry/reconcile exact suspend without heartbeat or cleanup replay."""

    global _drain_intent_handled, _pending_drain_suspend
    global _pending_drain_suspend_retry_task

    pending = _pending_drain_suspend
    if pending is None or pending.get("locally_quiesced") is not True:
        _drain_intent_handled = False
        return
    attempt = 0
    try:
        while _pending_drain_suspend is pending:
            client = _orchestrator_client
            suspended = False
            if client is not None:
                try:
                    suspended = await client.suspend_thread(
                        pending["thread_id"],
                        pinned_agent_id=pending.get("agent_id"),
                        session_runtime_generation=pending[
                            "session_runtime_generation"
                        ],
                        session_runtime_attach_token=pending[
                            "session_runtime_attach_token"
                        ],
                        session_runtime_retirement_token=pending.get(
                            "session_runtime_retirement_token"
                        ),
                        local_runtime_quiesced=True,
                        local_quiescence_protocol=pending.get(
                            "local_quiescence_protocol"
                        ),
                        workspace_generation=pending.get("workspace_generation")
                        or None,
                        workspace_runtime_incarnation=pending.get(
                            "workspace_runtime_incarnation"
                        )
                        or None,
                    )
                except Exception as exc:
                    logger.warning(
                        "Exact drain-suspend retry failed: %s",
                        type(exc).__name__,
                    )
            if not suspended and client is not None:
                outcome_reader = getattr(client, "get_thread_retirement_outcome", None)
                if callable(outcome_reader):
                    try:
                        outcome = await outcome_reader(
                            pending["thread_id"],
                            pinned_agent_id=pending["agent_id"],
                            session_runtime_generation=pending[
                                "session_runtime_generation"
                            ],
                            session_runtime_attach_token=pending[
                                "session_runtime_attach_token"
                            ],
                            session_runtime_retirement_token=pending[
                                "session_runtime_retirement_token"
                            ],
                            retirement_disposition="suspended",
                            retirement_permanent=False,
                        )
                    except Exception:
                        outcome = None
                    suspended = bool(
                        isinstance(outcome, dict)
                        and outcome.get("status") == "settled_or_superseded"
                        and outcome.get("outcome") in {"settled", "deleted"}
                        and outcome.get("retirement_disposition") == "suspended"
                        and outcome.get("retirement_permanent") is False
                    )
            if suspended:
                _pending_drain_suspend = None
                _drain_intent_handled = True
                _schedule_exit(delay=1.0)
                return
            _drain_intent_handled = True
            delay = _EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS[
                min(attempt + 1, len(_EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS) - 1)
            ]
            logger.warning(
                "Exact drain-suspend remains pending for thread %s; process "
                "kept nonclaimable for retry",
                pending["thread_id"],
            )
            attempt += 1
            await asyncio.sleep(delay)
    finally:
        if _pending_drain_suspend_retry_task is asyncio.current_task():
            _pending_drain_suspend_retry_task = None


_DEREGISTER_ON_EXIT_TIMEOUT_S = 5.0


async def _deregister_before_exit() -> None:
    """Best-effort deregistration ahead of os._exit.

    os._exit bypasses the lifespan shutdown that normally deregisters
    (the startup-failure ``_exit_*`` helpers already deregister inline),
    so without this every clean exit leaves an agents row that the
    orchestrator's 3-minute heartbeat sweep flips to offline and reports
    as a fleet:agents_offline corpse. Bounded and non-raising — a slow or
    failed deregister must never hold up or abort the exit (the
    stale-agent sweep stays the backstop, exactly as for crashes).
    """
    client = _orchestrator_client
    if client is None:
        return
    client.stop_heartbeat()
    hb = _heartbeat_task
    if hb is not None and not hb.done() and hb is not asyncio.current_task():
        # A heartbeat landing mid-deregister would 404 and re-register,
        # resurrecting the row this call is about to delete. Never
        # self-cancel — drain intents arrive inside the heartbeat task.
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
    """Schedule process exit after a short delay (allows final I/O to flush)."""
    global _pending_exit_task

    if _pending_exit_task and not _pending_exit_task.done():
        _pending_exit_task.cancel()

    async def _exit():
        await asyncio.sleep(delay)
        await _deregister_before_exit()
        logger.info("Session complete — exiting process")
        os._exit(0)

    _pending_exit_task = asyncio.create_task(_exit())


# ---------------------------------------------------------------------------
# Self-cleanup watchdogs (PR 2)
# ---------------------------------------------------------------------------


async def _boot_ws_watchdog(timeout_s: int) -> None:
    """Exit if no /ws/chat connection arrives within ``timeout_s`` of attach.

    A persistent agent that boots, attaches to a thread, then never receives
    a WebSocket has no other way to know it's been abandoned (e.g. user
    navigated away during creation). Without this watchdog the pod sits
    forever heartbeating and holding a slot. The orchestrator reconciler
    catches this too, but only after a 60s+ delay; this watchdog kills
    locally on the configured cadence.

    Officer sessions are exempt: they are headless BY DESIGN — no browser
    ever attaches, so "no WS yet" is their normal steady state, not
    abandonment (found by the S3 k3d smoke: every officer died exactly
    600s after boot and had to be respawned by the orchestrator watchdog).
    The officer watchdog owns their lifecycle end to end.
    """
    if _officer_cfg() is not None:
        return
    if _ws_connected_event is None:
        return
    try:
        await asyncio.wait_for(_ws_connected_event.wait(), timeout=timeout_s)
        return  # WS arrived — normal lifecycle takes over
    except asyncio.TimeoutError:
        logger.warning(
            "No WebSocket connection within %ds for thread %s — "
            "exiting (likely abandoned during creation).",
            timeout_s,
            _thread_id,
        )
    try:
        await _terminate_session("boot_ws_timeout")
    except Exception as e:
        logger.warning(f"Detach during boot-WS timeout failed: {e}")
        # Exact pinned teardown may still have a writer, workspace process, or
        # unacknowledged retirement receipt.  Keep the runtime fenced and let
        # its tracked retry/reconciler converge; exiting would discard the
        # only truthful local-quiescence owner.
        return
    _schedule_exit(delay=1.0)


async def _thread_status_watchdog(poll_s: int) -> None:
    """Exit if the bound thread transitions to a terminal state out-of-band.

    The orchestrator's stale_agent_detector can flip a thread to 'ended'
    via ``mark_orphaned_threads_ended`` or release the binding via
    ``mark_stuck_session_agents_ready`` (PR 1). When that happens this pod
    is orphaned — no work to do, holding a slot.

    'awaiting_user' is the eager-mode transient idle state set by this same
    agent's loop on natural pause with no subscribers (Phase 5,
    ``_loop_get_user_input``). It is NOT a terminal state — the orchestrator's
    attention-sleep watchdog owns the eventual ``awaiting_user → suspended``
    transition and we mustn't pre-empt it from here, or we kill the very
    untethered-survival behaviour Phase 1 + Phase 5 were built to enable.

    'suspended' means the orchestrator has already snapshotted + deleted the
    workspace pod — at that point we're a stranded agent with no workspace,
    so we exit.
    """
    global _retirement_admission_identity, _retirement_admission_disposition
    global _retirement_admission_token
    global _retirement_admission_permanent

    bound_thread_id = _thread_id
    bound_runtime_generation = _session_runtime_generation
    bound_runtime_attach_token = _session_runtime_attach_token
    runtime_generation_required = _pinned_runtime_generation_enabled
    while True:
        try:
            await asyncio.sleep(poll_s)
        except asyncio.CancelledError:
            raise
        if not _orchestrator_client or not bound_thread_id:
            continue
        if (
            _thread_id != bound_thread_id
            or _session_runtime_generation != bound_runtime_generation
            or _session_runtime_attach_token != bound_runtime_attach_token
        ):
            return
        try:
            lifecycle = await _orchestrator_client.get_thread_lifecycle(bound_thread_id)
        except Exception as e:
            logger.debug(f"Thread lifecycle poll failed (non-fatal): {e}")
            continue
        if not lifecycle:
            continue
        if (
            _thread_id != bound_thread_id
            or _session_runtime_generation != bound_runtime_generation
            or _session_runtime_attach_token != bound_runtime_attach_token
        ):
            return
        status = lifecycle.get("status")
        observed_generation = _canonical_runtime_generation(
            lifecycle.get("session_runtime_generation")
        )
        generation_moved = bool(
            bound_runtime_generation is not None
            and observed_generation != bound_runtime_generation
        )
        generation_missing = bool(
            runtime_generation_required and observed_generation is None
        )
        observed_attach_token = _canonical_runtime_generation(
            lifecycle.get("session_runtime_attach_token")
        )
        attach_token_moved = observed_attach_token != bound_runtime_attach_token
        retirement_preflight = lifecycle.get("runtime_retirement_preflight") is True
        retirement_authorized = lifecycle.get("runtime_retirement_authorized") is True
        if retirement_preflight and not retirement_authorized:
            # Owner End is still checking turn/control preconditions. No
            # authority has been minted and it may be aborted; keep the exact
            # runtime fully alive and never infer retirement from "pending".
            continue
        if (
            status == "ending"
            and retirement_authorized
            and not generation_moved
            and not generation_missing
            and not attach_token_moved
        ):
            disposition = lifecycle.get("retirement_disposition")
            permanent = lifecycle.get("retirement_permanent")
            retirement_token = _canonical_runtime_generation(
                lifecycle.get("session_runtime_retirement_token")
            )
            if disposition not in {"ended", "suspended"}:
                logger.warning(
                    "Authorized retirement omitted its immutable disposition "
                    "(thread=%s)",
                    bound_thread_id,
                )
                continue
            if type(permanent) is not bool:
                logger.warning(
                    "Authorized retirement omitted immutable permanent intent "
                    "(thread=%s)",
                    bound_thread_id,
                )
                continue
            identity = _attached_retirement_identity()
            if identity is None:
                return
            _retirement_admission_identity = identity
            _retirement_admission_disposition = disposition
            # A malformed/missing token is not locally accepted. The common
            # Begin call below will idempotently recover the exact token from
            # the server before any teardown effect.
            _retirement_admission_token = retirement_token
            _retirement_admission_permanent = permanent
            try:
                await _terminate_session("thread_retirement_authorized")
            except Exception as exc:
                logger.warning(
                    "Authorized retirement cleanup failed: %s", type(exc).__name__
                )
                return
            _schedule_exit(delay=1.0)
            return
        if status == "ending":
            # Exact-contract retirement is actionable only with the explicit
            # authorization bit and immutable disposition/token handshake.
            # A malformed or mixed-version shape must not trick the runtime
            # into tearing down while owner preflight may still abort.
            logger.warning(
                "Ignoring unauthorised/malformed ending lifecycle response "
                "for thread %s",
                bound_thread_id,
            )
            continue
        if (
            status not in ("created", "active", "awaiting_user")
            or generation_moved
            or generation_missing
            or attach_token_moved
        ):
            logger.info(
                "Thread %s lifecycle no longer belongs to this runtime "
                "(status=%r generation_match=%s attach_match=%s) — exiting.",
                bound_thread_id,
                status,
                not (generation_moved or generation_missing),
                not attach_token_moved,
            )
            try:
                await _terminate_session("thread_ended_oob")
            except Exception as e:
                logger.warning(f"Detach during status-watchdog exit failed: {e}")
                # Local writer/process/mount quiescence or exact receipt is
                # unproven.  Exiting here would abandon live workspace writers
                # and the only local retry owner. Stay fenced/nonclaimable;
                # the exact retirement task or durable reconciler converges it.
                return
            _schedule_exit(delay=1.0)
            return


def _start_watchdogs() -> None:
    """Start watchdog tasks for the active session. Safe to call repeatedly."""
    global _ws_connected_event, _watchdog_tasks

    # Stateless executor (M3): no boot-WS ever arrives (input rides the run
    # queue) and thread status is orchestrator-owned — both watchdogs would
    # tear down healthy cached sessions. The run_queue lease/reaper plays
    # their abandoned-pod role in this mode.
    if _stateless_mode():
        logger.debug("Stateless executor mode: session watchdogs disabled")
        return

    # Stop any prior watchdogs (defensive — should already be cleared).
    for task in _watchdog_tasks:
        if not task.done():
            task.cancel()
    _watchdog_tasks = []

    _ws_connected_event = asyncio.Event()
    _watchdog_tasks = [
        asyncio.create_task(
            _boot_ws_watchdog(_session_boot_ws_timeout_s),
            name="boot-ws-watchdog",
        ),
        asyncio.create_task(
            _thread_status_watchdog(_thread_status_poll_s),
            name="thread-status-watchdog",
        ),
    ]


def _stop_watchdogs() -> None:
    """Cancel all active watchdogs. Skips the current task to avoid self-cancel."""
    global _watchdog_tasks
    current = asyncio.current_task()
    for task in _watchdog_tasks:
        if task is current or task.done():
            continue
        task.cancel()
    _watchdog_tasks = []


async def _stop_and_join_watchdogs() -> None:
    """Cancel watchdogs and prove every independent task has quiesced."""

    global _watchdog_tasks
    current = asyncio.current_task()
    owned = [
        task for task in _watchdog_tasks if task is not current and not task.done()
    ]
    _stop_watchdogs()
    if not owned:
        return
    done, pending = await asyncio.wait(
        owned,
        timeout=float(os.environ.get("SESSION_WATCHDOG_CLOSE_TIMEOUT_S", "5")),
    )
    if pending:
        # Retain exact task ownership so the same retirement can retry joining
        # it. Remote stage/delete/settlement must not race an ignored cancel.
        _watchdog_tasks = list(pending)
        raise EventJournalUnavailable(
            f"{len(pending)} session watchdog(s) ignored cancellation"
        )
    for task in done:
        if not task.cancelled() and task.exception() is not None:
            logger.warning(
                "Session watchdog failed while joining terminal teardown: %s",
                type(task.exception()).__name__,
            )


def _signal_ws_connected() -> None:
    """Signal that a WebSocket has connected. Cancels the boot-WS watchdog."""
    if _ws_connected_event is not None:
        _ws_connected_event.set()


def _get_agent_metrics() -> Optional[Dict[str, Any]]:
    """Collect metrics for heartbeat."""
    metrics: Dict[str, Any] = {}
    try:
        tool_context = _session.tool_context if _session is not None else None
        if tool_context is not None:
            graph_progress = tool_context.get_graph_progress()
            metrics["graph_progress"] = graph_progress
    except BaseException:
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

    # Auxiliary-task health → orchestrator persists the degraded flag and
    # surfaces an admin badge (aux Phase 2). Best-effort; never fail heartbeat.
    aux = _aux_health_for_heartbeat()
    if aux is not None:
        metrics["aux"] = aux

    # Contained memory-store failures (deadlock containment). Best-effort;
    # never fail heartbeat.
    memory = _memory_health_for_heartbeat()
    if memory is not None:
        metrics["memory"] = memory

    return metrics or None


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


def _aux_health_for_heartbeat() -> Optional[Dict[str, Any]]:
    """Compact auxiliary-model health for the heartbeat (aux Phase 2).

    Reads the shared session auxiliary LLM. Returns None before a session is
    bound so the orchestrator leaves any previously-persisted ``aux_degraded``
    untouched rather than clearing it on incomplete information.
    """
    aux_llm = getattr(_session, "auxiliary_llm", None) if _session is not None else None
    if aux_llm is None:
        return None
    try:
        return aux_llm.health.heartbeat_summary()
    except Exception:
        return None


async def _release_delivered_attach_before_dedicated_exit(thread_id: str) -> None:
    """Rotate a failed dedicated attach only from its exact zero proof."""

    receipt = _failed_attach_release_receipt
    if receipt is None:
        return
    if not isinstance(receipt, dict) or receipt.get("thread_id") != thread_id:
        raise EventJournalUnavailable(
            "failed attach release receipt belongs to a different runtime"
        )
    confirmed = await _release_failed_attach_receipt_until_confirmed(
        thread_id,
        runtime_generation=receipt.get("session_runtime_generation"),
        runtime_attach_token=receipt.get("session_runtime_attach_token"),
    )
    if not confirmed:
        raise EventJournalUnavailable(
            "failed attach release remains unconfirmed; exit suppressed"
        )


async def _exit_workspace_not_ready(thread_id: str, exc: Exception) -> NoReturn:
    """Handle an unrecoverable workspace error during lifespan startup
    (WorkspaceNotReady — never provisioned/wedged; or WorkspaceUnavailableError
    — pod dead/unreachable): best-effort deregister then exit.

    Exits the process with status 0 (pod Completed, not Failed) so Kubernetes
    does not restart-loop the pod.  The orchestrator's session reconciler will
    recover the workspace and bind a fresh agent on the next interaction.
    """
    logger.info(
        "Workspace not ready for thread %s (%s) — exiting cleanly so the "
        "orchestrator can rebind once the workspace recovers (not a crash).",
        thread_id,
        exc,
    )
    await _release_delivered_attach_before_dedicated_exit(thread_id)
    if _orchestrator_client:
        try:
            _orchestrator_client.stop_heartbeat()
            if _heartbeat_task:
                _heartbeat_task.cancel()
            await _orchestrator_client.deregister()
            await _orchestrator_client.close()
        except Exception as de:
            logger.warning(
                "Best-effort deregister on workspace-not-ready failed: %s", de
            )
    os._exit(0)


async def _exit_grant_denied(thread_id: str, exc: Exception) -> NoReturn:
    """Handle a capability-grant denial at session attach (the workspace endpoint
    returned 403): log the REAL reason and exit cleanly (status 0, pod Completed
    — no K8s restart-loop). Unlike :func:`_exit_workspace_not_ready` this is NOT
    a transient workspace problem — a rebind hits the identical denial — so we do
    NOT claim the orchestrator will recover it. The cockpit re-surfaces the
    reason on its next create/prepare via the grant pre-flight (Layers 1/2).
    See knowledge-base/knowledge/issues/session_permission_mode_grant_denied_ready_timeout.md.
    """
    logger.error(
        "Session attach denied for thread %s by capability grants (%s) — exiting "
        "cleanly; NOT retrying (a rebind hits the same denial). The cockpit "
        "surfaces this on its next create/prepare grant pre-flight.",
        thread_id,
        exc,
    )
    await _release_delivered_attach_before_dedicated_exit(thread_id)
    if _orchestrator_client:
        try:
            _orchestrator_client.stop_heartbeat()
            if _heartbeat_task:
                _heartbeat_task.cancel()
            await _orchestrator_client.deregister()
            await _orchestrator_client.close()
        except Exception as de:
            logger.warning("Best-effort deregister on grant-denied exit failed: %s", de)
    os._exit(0)


async def _exit_memory_unavailable(thread_id: str, exc: Exception) -> NoReturn:
    """Handle a required-memory setup failure at session attach: a configured
    memory component (embedding-backed store or a plugin whose transport won't
    resolve — e.g. the reranker endpoint) could not be set up.

    Like :func:`_exit_grant_denied` this is a deterministic config failure, NOT
    a transient workspace problem — a rebind hits the identical failure — so we
    exit cleanly (status 0, pod Completed, no K8s restart-loop) rather than
    crash-looping. The cockpit re-surfaces the reason on its next create/prepare
    via the orchestrator's endpoint pre-flight (which validates the same roles
    before spawning a pod). See
    knowledge-base/knowledge/issues/openrouter_auxiliary_crashes_session_via_memory_reranker.md.
    """
    logger.error(
        "Session attach failed for thread %s — required memory unavailable (%s) "
        "— exiting cleanly; NOT retrying (a rebind hits the same failure). The "
        "cockpit surfaces this on its next create/prepare endpoint pre-flight.",
        thread_id,
        exc,
    )
    await _release_delivered_attach_before_dedicated_exit(thread_id)
    if _orchestrator_client:
        try:
            _orchestrator_client.stop_heartbeat()
            if _heartbeat_task:
                _heartbeat_task.cancel()
            await _orchestrator_client.deregister()
            await _orchestrator_client.close()
        except Exception as de:
            logger.warning(
                "Best-effort deregister on memory-unavailable exit failed: %s", de
            )
    os._exit(0)


async def _exit_duplicate_provision(thread_id: str) -> NoReturn:
    """Handle a lost provisioning race (409) during lifespan startup.

    Another live agent already owns this thread, so this pod must not serve it.
    We exit with status 0 (pod Completed under restartPolicy: Never, no restart
    loop) so the pod drops out of the per-session Service's endpoints instead of
    lingering as an orphan that black-holes ~half the cockpit's connection
    attempts (the Service uses publishNotReadyAddresses, so a not-ready orphan
    stays a live target). Only this pod's own agent record is cleaned up — never
    any thread-scoped resource, which belongs to the winning agent.
    """
    logger.warning(
        "Lost the provisioning race for thread %s — another live agent already "
        "owns it; exiting cleanly so this orphan pod leaves the session Service "
        "endpoints (not a crash).",
        thread_id,
    )
    if _orchestrator_client:
        try:
            _orchestrator_client.stop_heartbeat()
            if _heartbeat_task:
                _heartbeat_task.cancel()
            await _orchestrator_client.deregister()
            await _orchestrator_client.close()
        except Exception as de:
            logger.warning(
                "Best-effort deregister on duplicate-provision exit failed: %s", de
            )
    os._exit(0)


async def _exit_session_ended(thread_id: str) -> NoReturn:
    """Exit a dedicated runtime refused by the ended-session fence."""

    logger.info(
        "Thread %s ended before runtime attach — exiting without retrying or "
        "serving credentials.",
        thread_id,
    )
    if _orchestrator_client:
        try:
            _orchestrator_client.stop_heartbeat()
            if _heartbeat_task:
                _heartbeat_task.cancel()
            await _orchestrator_client.deregister()
            await _orchestrator_client.close()
        except Exception as de:
            logger.warning("Best-effort ended-session deregister failed: %s", de)
    os._exit(0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize persistent agent, register with orchestrator, start heartbeat."""
    global \
        _agent, \
        _session, \
        _orchestrator_client, \
        _heartbeat_task, \
        _started_at, \
        _thread_id

    _started_at = datetime.now()
    stateless = _stateless_mode()
    pool_mode = _thread_id is None
    if stateless:
        logger.info(
            f"Starting stateless turn executor agent: config={_config_path} "
            "(no registration, no heartbeat, no watchdogs — work arrives via "
            "run_queue claims)"
        )
    else:
        logger.info(
            f"Starting persistent agent: config={_config_path}, "
            f"thread={_thread_id or '(pool mode — waiting for assignment)'}"
        )

    # 1. Create and initialize UniversalAgent (singleton layer)
    _agent = UniversalAgent.from_config(_config_path)
    await _agent.initialize()

    # 2. Connect to orchestrator
    orchestrator_url = os.getenv("ORCHESTRATOR_URL", "http://localhost:8085")
    dedicated_register_ok = True
    if stateless:
        # M3 stateless executor: the orchestrator client exists ONLY for the
        # claim-bundle fetch and the attach-path reads (get_thread_workspace,
        # status writes). No registration, no heartbeat loop — liveness is the
        # run_queue lease, and the reaper replaces the watchdog/sweep roles.
        if orchestrator_url:
            try:
                _orchestrator_client = create_orchestrator_client_from_env(
                    _agent.config.agent_id
                )
                await _orchestrator_client.connect()
            except Exception as e:
                logger.warning(
                    f"Orchestrator client init failed (stateless mode, "
                    f"non-fatal — claims will release until it recovers): {e}"
                )
                _orchestrator_client = None
        from agent.api.turn_executor import start_stateless_executor

        await start_stateless_executor()
    elif orchestrator_url:
        try:
            _orchestrator_client = create_orchestrator_client_from_env(
                _agent.config.agent_id
            )
            await _orchestrator_client.connect()

            if pool_mode:
                # Pool mode: register as available, no thread yet.
                # The orchestrator will assign threads via POST /session/attach.
                await _orchestrator_client.register(
                    agent_mode="persistent",
                    thread_id=None,
                )
            else:
                # Dedicated mode: auto-create thread if needed (backwards compatible)
                if _thread_id is None:
                    created_id = await _orchestrator_client.create_thread(
                        config_name=_config_path or "session_base",
                        permission_mode=_agent.config.interactive.permission_mode,
                        title=f"Local Session ({_config_path or 'session_base'})",
                    )
                    if created_id:
                        _thread_id = created_id
                        logger.info(f"Auto-created thread: {_thread_id}")
                    else:
                        logger.warning(
                            "Failed to create thread — generating local UUID"
                        )

                if _thread_id is None:
                    import uuid

                    _thread_id = str(uuid.uuid4())

                # A thread-bound registration that loses the provisioning race
                # raises DuplicateThreadBinding (orchestrator 409); the except
                # clause below exits this pod cleanly so it leaves the
                # per-session Service endpoints instead of black-holing
                # connections. See
                # knowledge-history/done/persistent_thread_double_provisioning_race.md.
                # A False return here is a *different*, transient failure
                # (network / 5xx): keep the pod up but session-less.
                dedicated_register_ok = await _orchestrator_client.register(
                    agent_mode="persistent",
                    thread_id=_thread_id,
                )
                if not dedicated_register_ok:
                    logger.error(
                        "Persistent registration for thread %s failed "
                        "(transient / non-409) — pod will stay up but will NOT "
                        "attach a session.",
                        _thread_id,
                    )

            # Start heartbeat
            def _heartbeat_status():
                # A synchronously admitted pool attach owns a DB reservation
                # before the heavy background setup constructs ``_session``.
                # Advertising ready in that window would overwrite the
                # reservation's agents.status='session', make exact failure
                # cleanup impossible, and let dispatch double-claim the pod.
                return _pool_heartbeat_status()

            _heartbeat_task = asyncio.create_task(
                _orchestrator_client.run_heartbeat_loop(
                    get_status=_heartbeat_status,
                    get_job_id=lambda: None,
                    get_metrics=_get_agent_metrics,
                    on_response=_handle_heartbeat_intents,
                )
            )
            logger.info("Registered with orchestrator as persistent agent")
        except DuplicateThreadBinding:
            # Lost the provisioning race for this thread (orchestrator 409).
            # Exit cleanly so this orphan pod leaves the per-session Service
            # endpoints; the winning agent keeps serving and the orchestrator
            # does not rebind (the binding already exists). Does not return.
            await _exit_duplicate_provision(_thread_id)
        except SessionEnded:
            await _exit_session_ended(_thread_id)
        except Exception as e:
            logger.warning(f"Failed to register with orchestrator (non-fatal): {e}")
            _orchestrator_client = None
    else:
        logger.info("No ORCHESTRATOR_URL — running standalone")

    # If we have a thread_id (dedicated mode) and registration succeeded, set
    # up the session immediately. If register was refused (409), skip the
    # attach — the legitimate owner already holds this thread.
    if _thread_id and dedicated_register_ok:
        # Fallback: generate UUID if still None (standalone mode)
        if _thread_id is None:
            import uuid

            _thread_id = str(uuid.uuid4())

        try:
            await _attach_session(_thread_id)
        except SessionEnded:
            await _exit_session_ended(_thread_id)
        except SessionGrantDenied as e:
            # The session's resolved config exceeds the owner's capability grants
            # (workspace endpoint returned 403) — e.g. a grant revoked between the
            # orchestrator's create/provision pre-flight (Layers 1/2) and this
            # attach. Permanent: exit with the REAL reason instead of the
            # misleading 'workspace not provisioned' rebind path; the cockpit
            # re-surfaces it on its next create/prepare grant pre-flight.
            await _exit_grant_denied(_thread_id, e)
        except MemoryUnavailableError as e:
            # A configured/required memory component couldn't be set up (store
            # init or a plugin transport that won't resolve — e.g. the reranker
            # endpoint). Deterministic config failure: exit cleanly with the REAL
            # reason instead of crashing (which triggered a workspace-release +
            # crash-loop retry). The cockpit re-surfaces it via the orchestrator
            # endpoint pre-flight.
            await _exit_memory_unavailable(_thread_id, e)
        except (WorkspaceNotReady, WorkspaceUnavailableError) as e:
            # Workspace raced us / is wedged (WorkspaceNotReady) or its pod is
            # dead/unreachable (WorkspaceUnavailableError — SSH connect exhausted
            # against a destroyed workspace): exit cleanly (status 0) instead of
            # crashing, so K8s doesn't restart-loop. The orchestrator's session
            # reconcile (ensure_workspace drift probe) recreates the pod and
            # rebinds a fresh agent. See _exit_workspace_not_ready.
            await _exit_workspace_not_ready(_thread_id, e)
    elif _thread_id and not dedicated_register_ok:
        logger.info(
            "Skipping session attach for thread %s — orchestrator refused "
            "registration.",
            _thread_id,
        )
    elif stateless:
        logger.info(
            "Stateless executor: claim loop running — sessions attach only "
            "under run_queue leases"
        )
    else:
        logger.info(
            "Pool mode: waiting for session assignment via POST /session/attach"
        )

    yield

    # --- Shutdown ---
    logger.info("Shutting down persistent agent")

    # A pool attach is admitted synchronously but finishes in the background.
    # Own that task through shutdown so workspace/setup code cannot continue
    # after the client is deregistered and lose its exact release fence.
    attach_task = _pool_attach_task
    if attach_task is not None and not attach_task.done():
        attach_task.cancel()
        try:
            await attach_task
        except asyncio.CancelledError:
            pass

    # Exact drain settlement retries are independent of ordinary heartbeats
    # (Begin makes those authority-refused). Own their lifetime explicitly so
    # no HTTP/outcome task survives client close during process shutdown.
    drain_retry_task = _pending_drain_suspend_retry_task
    if drain_retry_task is not None and not drain_retry_task.done():
        drain_retry_task.cancel()
        try:
            await drain_retry_task
        except asyncio.CancelledError:
            pass

    if stateless:
        # SIGTERM/preStop contract (M3): stop claiming; a mid-flight turn
        # finishes under its lease (bounded) and completes/releases before
        # the session teardown below. Exit stays 0.
        from agent.api.turn_executor import stop_stateless_executor

        await stop_stateless_executor()

    # Detach any active session. Stateless lane: never mark the thread ended —
    # thread lifecycle belongs to the orchestrator, and the next claim (on any
    # pod) picks the thread back up from thread_messages.
    if _session:
        await _terminate_session("shutdown", mark_thread=not stateless)

    if _orchestrator_client:
        try:
            if not stateless:
                _orchestrator_client.stop_heartbeat()
                if _heartbeat_task:
                    _heartbeat_task.cancel()
                    try:
                        await _heartbeat_task
                    except asyncio.CancelledError:
                        pass
                await _orchestrator_client.deregister()
            await _orchestrator_client.close()
        except Exception as e:
            logger.warning(f"Orchestrator cleanup error: {e}")

    if _agent:
        await _agent.shutdown()

    logger.info("Persistent agent shutdown complete")


def _legacy_nc_cloud_cfg(nc_folder: str) -> Dict[str, Any]:
    """Translate legacy ``nc_session_folder`` + env vars into the cloud_sync schema.

    Used when the orchestrator is on a version that still returns the
    pre-refactor flat field. Drops away once the orchestrator rolls.
    """
    nc_url = os.getenv("NEXTCLOUD_URL", "http://localhost:8800")
    nc_user = os.getenv("NEXTCLOUD_AGENT_USER", "agent-service")
    nc_pass = os.getenv("NEXTCLOUD_AGENT_PASSWORD", "agent-service-dev")
    return {
        "backend": "nextcloud",
        "webdav_url": f"{nc_url.rstrip('/')}/remote.php/dav/files/{nc_user}/{nc_folder}/",
        "auth": {"type": "basic", "username": nc_user, "password": nc_pass},
    }


def _build_sync_coordinator(
    *,
    workspace_path,
    workspace_backend,
    cloud_cfg: Optional[Dict[str, Any]],
    thread_id: str = "",
    workspace_generation: str = "",
):
    """Construct a ``WorkspaceSyncCoordinator`` from the orchestrator's payload.

    Handles both v1 (flat ``{backend, webdav_url, auth}``) and v2
    (``{version: 2, session_folder, mounts: [...]}``). Phase 1 of
    ``knowledge-base/knowledge/features/cloud_collaboration_model.md`` §9.

    The legacy session folder (v2 ``session_folder``, or the whole v1
    payload) is mounted at the workspace root. Project mounts (and, in
    Phase 3+, user-home and repo mounts) attach under their
    ``target_path`` via the base class's ``mount_subdir``.

    Returns ``None`` when nothing resolvable was provided.
    """
    if not cloud_cfg:
        return None

    from agent.services.cloud_sync import (
        MountSync,
        WorkspaceSyncCoordinator,
        build_workspace_sync,
    )

    coordinator = WorkspaceSyncCoordinator(
        thread_id=thread_id,
        workspace_generation=workspace_generation,
    )

    def _identity(
        cfg: Dict[str, Any], *, logical_key: str, target_path: str
    ) -> tuple[str, str]:
        """Stable row key + exact non-secret source/destination digest.

        ``thread_mounts.id`` is replace-on-edit and therefore cannot name a
        durable generation. The logical key excludes workspace incarnation;
        the scope digest includes it so a clean row can be rebound while a
        pending row for an old incarnation fails closed.
        """

        destination = {
            "backend": str(cfg.get("backend") or ""),
            "mount_kind": str(cfg.get("mount_kind") or ""),
            "target_path": str(target_path or "").strip("/"),
            "webdav_url": str(cfg.get("webdav_url") or "").rstrip("/"),
        }
        encoded_destination = json.dumps(
            destination, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        destination_sha = hashlib.sha256(encoded_destination).hexdigest()
        mount_key = (
            "legacy-session"
            if logical_key == "legacy-session"
            else f"mount:{destination_sha}"
        )
        if not thread_id or not workspace_generation:
            return mount_key, ""
        scope = {
            "thread_id": str(thread_id),
            "workspace_generation": str(workspace_generation),
            "mount_key": mount_key,
            "destination_sha256": destination_sha,
        }
        scope_sha = hashlib.sha256(
            json.dumps(scope, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return mount_key, scope_sha

    def _attach(cfg: Dict[str, Any], *, mount_id: str, target_path: str) -> None:
        sync = build_workspace_sync(
            workspace_path=workspace_path,
            cloud_cfg=cfg,
            workspace_backend=workspace_backend,
            mount_subdir=target_path,
        )
        if sync is not None:
            mount_key, scope_sha = _identity(
                cfg, logical_key=mount_id, target_path=target_path
            )
            coordinator.add(
                MountSync(
                    mount_id=mount_id,
                    target_path=target_path,
                    sync=sync,
                    sync_scope_sha256=scope_sha,
                    generation_key=mount_key,
                )
            )

    if cloud_cfg.get("version") == 2:
        session_folder = cloud_cfg.get("session_folder")
        if session_folder:
            _attach(session_folder, mount_id="legacy-session", target_path="")
        for m in cloud_cfg.get("mounts") or []:
            mid = str(m.get("mount_id") or "")
            tp = m.get("target_path") or ""
            _attach(m, mount_id=mid, target_path=tp)
    else:
        # v1 flat shape — single session-folder mount at workspace root.
        _attach(cloud_cfg, mount_id="legacy-session", target_path="")

    return coordinator if len(coordinator) > 0 else None


def _load_expert_config(config_name: str):
    """Resolve a named config exactly like the worker job path does.

    Mirrors src/agent/agent.py's ``metadata["config_name"]`` reload (expert YAML
    via $extends + settings-matrix application), so a pool pod that booted
    as a worker ('defaults') can serve a session under the thread's own
    config — post-cutover the memory pipeline (and the whole session
    profile) is a per-mode YAML choice
    (knowledge-base/knowledge/issues/session_config_name_plumbing.md, hole B).

    Raises on unknown names: the attach endpoint turns that into a 500 and
    the orchestrator falls back to provisioning a dedicated pod with the
    right config baked in — failing loud beats silently running a session
    on the worker YAML.
    """
    from shared.runtime.core.loader import (
        _apply_settings_matrix,
        authored_llm_keys,
        load_agent_config_from_dict,
        load_and_merge_config,
        resolve_bundled_config_path,
    )

    config_path, deployment_dir = resolve_bundled_config_path(config_name)
    merged_config_data = load_and_merge_config(config_path)
    # The leaf's own llm keys are explicit for the matrix (for a role root
    # such as `session_base`, the overlay + expert_base pair).
    raw_llm_keys: set = authored_llm_keys(config_path)
    _apply_settings_matrix(merged_config_data, raw_llm_keys, deployment_dir)
    # Materialise the subagent roster like load_agent_config() does (disk
    # refs only; an unresolvable ref drops with a warning, never a 500).
    if merged_config_data.get("subagents") is not None:
        from shared.runtime.core.subagent_roster import resolve_subagent_roster

        merged_config_data = resolve_subagent_roster(
            merged_config_data,
            db_refs={},
            deployment_dir=deployment_dir,
            on_missing="drop",
        )
    return load_agent_config_from_dict(
        merged_config_data, deployment_dir=deployment_dir
    )


def _session_backend_is_lite(config: Optional[Dict[str, Any]]) -> bool:
    """True if a resolved session config / override selects a lite tier
    (``virtual``/``none``).

    Lite tiers run with no workspace pod, so the session attach must skip the
    workspace-readiness poll (which would otherwise raise ``WorkspaceNotReady``
    for a pod that never exists) and let ``PersistentSession._setup_workspace``
    build the object-store backend from the injected mounts (the lite tiers
    have no SSH workspace pod — no_workspace_agent_mode.md §4).
    """
    if not isinstance(config, dict):
        return False
    from agent.core.backends.factory import LITE_BACKENDS

    # config_override is flat ({workspace: ...}); a resolved_config blob nests
    # the agent config under "agent".
    ws = config.get("workspace") or (config.get("agent") or {}).get("workspace") or {}
    return ws.get("backend") in LITE_BACKENDS


def _session_backend_is_vm(config: Optional[Dict[str, Any]]) -> bool:
    """True if a resolved session config / override selects the VM tier
    (``vm``, or its legacy ``remote`` alias).

    A vm-tier session's workspace is a KubeVirt VM, and its readiness poll must
    accept ONLY that VM: a sandbox container is ready in seconds while a cold VM
    boot takes minutes, so a container that exists for any reason would always
    win the race and silently attach the session to the wrong tier
    (knowledge-base/knowledge/issues/session_vm_backend_never_attaches.md Defect 2).

    Same dual-shape contract as :func:`_session_backend_is_lite`.
    """
    if not isinstance(config, dict):
        return False
    from agent.core.backends.factory import VM_BACKENDS

    ws = config.get("workspace") or (config.get("agent") or {}).get("workspace") or {}
    return ws.get("backend") in VM_BACKENDS


_FLEET_MANAGEMENT_DISABLED_KEY = "_fleet_management_disabled"
_JOB_CONTROL_DISABLED_KEY = "_job_control_disabled"
_JOB_INSPECTION_DISABLED_KEY = "_job_inspection_disabled"
_AGENT_CATALOG_DISABLED_KEY = "_agent_catalog_disabled"
_WORKFLOWS_DISABLED_KEY = "_workflows_disabled"
_CANVAS_DISABLED_KEY = "_canvas_disabled"


def _apply_session_tool_group_markers(
    merged_config: Dict[str, Any],
    config_override: Optional[Dict[str, Any]],
) -> None:
    """Preserve explicit session tool group off/on across dataclass re-parsing."""
    tools = (config_override or {}).get("tools")
    if not isinstance(tools, dict):
        return
    group_markers = {
        "orchestrator": _FLEET_MANAGEMENT_DISABLED_KEY,
        "job_control": _JOB_CONTROL_DISABLED_KEY,
        "job_inspection": _JOB_INSPECTION_DISABLED_KEY,
        "agent_catalog": _AGENT_CATALOG_DISABLED_KEY,
        "workflows": _WORKFLOWS_DISABLED_KEY,
        "canvas": _CANVAS_DISABLED_KEY,
    }
    for group, marker in group_markers.items():
        if group not in tools:
            continue
        if tools.get(group) == []:
            merged_config[marker] = True
        else:
            merged_config.pop(marker, None)


def _apply_datasource_enrichment_to_resolved(
    resolved_config: Optional[Dict[str, Any]],
    ds_tool_categories: Dict[str, List[str]],
    cli_ds_types: List[str],
) -> None:
    """Fold datasource-derived config into an orchestrator-resolved blob.

    Hydration (``load_config_from_resolved``) deliberately skips the
    config_override merge, so the datasource tool categories and
    ``_cli_datasources`` applied to config_override during attach never reach
    a hydrated session. Mutate the blob's ``agent`` dict in place instead:
    tool categories merge into ``agent["tools"]``; ``_cli_datasources`` goes
    at the TOP level, because ``serialize_resolved_config`` flattens
    ``extra`` keys there and ``load_agent_config_from_dict`` folds unknown
    top-level keys back into ``config.extra``.

    No-op when ``resolved_config`` is absent or malformed.
    """
    if not resolved_config:
        return
    agent_dict = resolved_config.get("agent")
    if not isinstance(agent_dict, dict):
        return
    if ds_tool_categories:
        agent_tools = agent_dict.get("tools")
        agent_tools = dict(agent_tools) if isinstance(agent_tools, dict) else {}
        agent_tools.update(ds_tool_categories)
        agent_dict["tools"] = agent_tools
    if cli_ds_types:
        agent_dict["_cli_datasources"] = cli_ds_types


def _sanitize_live_session_config_override(
    config_override: Any,
) -> Dict[str, Any]:
    """Fence the live config surface before it reaches generic config loading.

    Every ``tools.<category>`` is checked against the registry — the category
    must exist and every name in it must belong to that category, because the
    loader resolves a name globally rather than by the key it arrived under.
    Anything that fails raises ``ToolPolicyError`` (a ``ValueError``, which is
    what this boundary has always signalled with); nothing is silently
    dropped.  This mirrors the orchestrator's PATCH boundary rather than
    narrowing it, so the two cannot disagree about what a live update means.
    Returning a copy keeps the caller-owned WebSocket payload immutable.
    """

    if not isinstance(config_override, dict):
        raise ValueError("Session config override must be an object")
    # Loader-owned provenance keys are never a live-update surface (see
    # strip_loader_owned_keys) — dropped, not honoured.
    from shared.runtime.core.loader import strip_loader_owned_keys
    from shared.runtime.core.session_config_patch import validate_session_settings_patch

    sanitized = strip_loader_owned_keys(dict(config_override))
    validate_session_settings_patch(sanitized)
    interactive = sanitized.get("interactive")
    if isinstance(interactive, dict) and {
        "permission_mode",
        "narration_mode",
    }.intersection(interactive):
        raise ValueError(
            "permission_mode and narration_mode use the session control endpoint"
        )
    if "tools" in sanitized:
        accepted_tools = validate_tool_override_fragment(sanitized)
        if accepted_tools:
            sanitized["tools"] = accepted_tools
        else:
            sanitized.pop("tools", None)
    return sanitized


# Thread statuses that mark the previous session life as over. Only 'ended'
# is terminal in the vocabulary enforced by valid_thread_status (created /
# active / idle / awaiting_user / suspended / ended): 'suspended' threads are
# live-resumable (drain-suspend, attention-sleep) and their reattach is
# exactly the clean-handoff case that must REUSE the epoch.
_TERMINAL_THREAD_STATUSES: frozenset = frozenset({"ended"})

# Journal kinds that render a session life terminal client-side. Keep in
# lockstep with the cockpit's terminal-lifecycle handlers and its
# _isSupersededLifecycleFrame guard (persistent-chat.service.ts), which
# swallows exactly these kinds at epoch <= resumedFromEpoch: a resume that
# REUSED the epoch would have its genuine future terminal frames swallowed
# forever, so an epoch that already carries one of these must bump on the
# next attach. 'session.suspended' is deliberately absent — the cockpit
# treats it as live-resumable, not terminal.
_TERMINAL_LIFECYCLE_EVENT_KINDS: tuple = ("session.ended", "session.idle_timeout")


async def _resolve_event_journal_epoch(
    postgres_conn: Any, thread_id: str
) -> Tuple[int, int]:
    """Resolve ``(events_epoch, seq_seed)`` for this runtime attach.

    REUSE by default, bump only when the previous session life is provably
    over (doc §5.3.2). The old contract here allocated a new epoch on every
    attach; that made every reattach fire the client cascade — ~2s of
    dead-epoch polling, then ``gone_beyond_horizon`` → IndexedDB thread-cache
    wipe → full transcript refetch → SSE reopen — and is the #1 blocker for
    per-turn (stateless) attaches, where it would fire every turn. With seq
    seeded monotonic (below), a clean reattach on the same epoch is invisible
    to clients: their cached cursors stay valid and replay continues.

    BUMP iff any of:
      - thread status is terminal (``_TERMINAL_THREAD_STATUSES``);
      - the current epoch already carries a terminal lifecycle frame
        (``_TERMINAL_LIFECYCLE_EVENT_KINDS``): the cockpit's
        ``resumedFromEpoch`` guard swallows terminal frames at
        ``epoch <= resumedFromEpoch``, so a resumed life must move to a
        higher epoch for its own eventual terminal frames to render;
      - the epoch is non-virgin yet has no surviving rows (retention pruned
        it, or the 0116 backfill found it already pruned): every cursor a
        client could hold predates retention, so a bump costs nothing, while
        reuse could seed below a cached cursor (dead poll) and re-trips the
        resume-guard hazard above;
      - the seed probe itself fails (safety fallback: never reuse an epoch
        we could not read).

    On REUSE the seed is ``GREATEST(events_seq_hwm, MAX(seq))``:
    ``events_seq_hwm`` (0116) survives retention pruning of the rows
    themselves, so the seed stays above every seq ever served even when
    ``MAX(seq)`` shrank; MAX is belt-and-braces for rows written before the
    hwm existed (the UNIQUE (thread_id, epoch, seq) index backs it). The
    caller sets ``_next_seq = seq_seed`` — ``_broadcast`` pre-increments, so
    the first frame lands at seed + 1.
    """

    try:
        async with postgres_conn.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT events_epoch, events_seq_hwm, status "
                "FROM threads WHERE id = $1",
                thread_id,
            )
            if row is None:
                raise EventJournalUnavailable(
                    "Persistent event journal thread does not exist"
                )
            try:
                epoch = int(row["events_epoch"])
                hwm = int(row["events_seq_hwm"] or 0)
            except (KeyError, TypeError, ValueError) as exc:
                raise EventJournalUnavailable(
                    "Persistent event journal returned an invalid generation"
                ) from exc
            status = row["status"]

            bump_reason: Optional[str] = None
            max_seq = 0
            if status in _TERMINAL_THREAD_STATUSES:
                bump_reason = f"terminal_status:{status}"
            else:
                try:
                    has_terminal_frame = await conn.fetchval(
                        "SELECT EXISTS ("
                        "  SELECT 1 FROM thread_events "
                        "  WHERE thread_id = $1 AND epoch = $2 "
                        "    AND kind = ANY($3::text[])"
                        ")",
                        thread_id,
                        epoch,
                        list(_TERMINAL_LIFECYCLE_EVENT_KINDS),
                    )
                    if has_terminal_frame:
                        bump_reason = "terminal_lifecycle_frame"
                    else:
                        max_seq_raw = await conn.fetchval(
                            "SELECT MAX(seq) FROM thread_events "
                            "WHERE thread_id = $1 AND epoch = $2",
                            thread_id,
                            epoch,
                        )
                        if max_seq_raw is None and (hwm > 0 or epoch > 0):
                            # Non-virgin epoch with zero surviving rows: its
                            # entire history is beyond retention (hwm > 0), or
                            # it predates 0116 and was backfilled to 0 after
                            # the prune already ran (epoch > 0). Either way no
                            # client cursor for it can still be served.
                            bump_reason = "epoch_beyond_retention"
                        else:
                            max_seq = int(max_seq_raw or 0)
                except Exception as probe_exc:
                    bump_reason = f"seed_probe_failed:{type(probe_exc).__name__}"
                    logger.warning(
                        "Event journal seed probe failed for thread %s — "
                        "falling back to an epoch bump: %s",
                        thread_id,
                        probe_exc,
                    )

            if bump_reason is None:
                seq_seed = max(hwm, max_seq)
                if seq_seed > hwm:
                    # Persist the correction so the mark is authoritative for
                    # the fenced flush and any system-frame allocation.
                    await conn.execute(
                        "UPDATE threads SET events_seq_hwm = $2 "
                        "WHERE id = $1 AND events_seq_hwm < $2",
                        thread_id,
                        seq_seed,
                    )
                logger.info(
                    "Reusing events_epoch %d for thread %s "
                    "(seq_seed=%d hwm=%d max_seq=%d status=%s)",
                    epoch,
                    thread_id,
                    seq_seed,
                    hwm,
                    max_seq,
                    status,
                )
                return epoch, seq_seed

            new_epoch = await _event_journal.bump_epoch(conn, thread_id=thread_id)
            logger.info(
                "Bumped events_epoch %d -> %d for thread %s (reason=%s)",
                epoch,
                new_epoch,
                thread_id,
                bump_reason,
            )
            return new_epoch, 0
    except EventJournalUnavailable:
        raise
    except LookupError as exc:
        # bump_epoch: the thread row vanished between statements.
        raise EventJournalUnavailable(
            "Persistent event journal thread does not exist"
        ) from exc
    except Exception as exc:
        raise EventJournalUnavailable(
            "Persistent event journal initialization failed"
        ) from exc


async def _bump_event_journal_epoch(postgres_conn: Any, thread_id: str) -> int:
    """Force a new event generation: epoch + 1, seq high-water mark to 0.

    The deliberate-bump half of the epoch contract — rewind (its caller here)
    and the reaper's steal (M4, importing ``shared.event_journal``
    directly) are the only legitimate bumpers; attach resolution reuses live
    epochs (``_resolve_event_journal_epoch``). Wraps the shared
    single-statement implementation with this app's pool acquire and failure
    taxonomy.
    """

    try:
        async with postgres_conn.acquire() as conn:
            new_epoch = await _event_journal.bump_epoch(conn, thread_id=thread_id)
    except LookupError as exc:
        raise EventJournalUnavailable(
            "Persistent event journal thread does not exist"
        ) from exc
    except Exception as exc:
        raise EventJournalUnavailable(
            "Persistent event journal epoch bump failed"
        ) from exc
    logger.info(
        "Bumped events_epoch to %d for thread %s (deliberate bump)",
        new_epoch,
        thread_id,
    )
    return new_epoch


async def _strict_cleanup_partial_attach_local_resources(
    context: dict[str, Any],
) -> None:
    """Close every datasource/MCP owner created before session construction."""

    resources: list[Any] = []
    seen: set[int] = set()
    for registry_name in ("datasources", "datasource_clients"):
        registry = context.get(registry_name)
        if not isinstance(registry, dict):
            continue
        for resource in registry.values():
            if resource is None or id(resource) in seen:
                continue
            seen.add(id(resource))
            resources.append(resource)
    for resource in resources:
        try:
            aclose = getattr(resource, "aclose", None)
            if callable(aclose):
                result = aclose()
                if inspect.isawaitable(result):
                    await result
                continue
            close = getattr(resource, "close", None)
            if callable(close):
                result = close()
                if inspect.isawaitable(result):
                    await result
        except Exception as exc:
            raise EventJournalUnavailable(
                "partial attach local datasource owner did not quiesce"
            ) from exc
    context["datasources"] = {}
    context["datasource_clients"] = {}


async def _strict_cleanup_partial_sandbox_workspace(
    context: dict[str, Any],
) -> str:
    """Prove all writers zero on an attested sandbox before G rotation."""

    remote = context.get("remote")
    generation = _canonical_runtime_generation(context.get("workspace_generation"))
    incarnation = _canonical_runtime_generation(
        context.get("workspace_runtime_incarnation")
    )
    fingerprint = context.get("workspace_ssh_host_key_fingerprint")
    thread_id = context.get("thread_id")
    if not (
        isinstance(remote, dict)
        and isinstance(remote.get("host"), str)
        and remote["host"].strip()
        and type(remote.get("port", 22)) is int
        and 1 <= remote.get("port", 22) <= 65535
        and isinstance(thread_id, str)
        and thread_id
        and generation is not None
        and incarnation is not None
        and isinstance(fingerprint, str)
        and fingerprint.strip()
    ):
        raise EventJournalUnavailable(
            "partial sandbox attach lacks exact workspace cleanup authority"
        )

    from shared.runtime.core.backends.remote import RemoteBackend

    try:
        backend = RemoteBackend(
            host=remote["host"],
            port=remote.get("port", 22),
            username=remote.get("username", "agent-host"),
            key_path=remote.get("key_path", "/run/secrets/vm-ssh-key"),
            workspace_path=remote.get("workspace_path", "/home/agent-host/workspace"),
            job_id=thread_id,
            connect_timeout=remote.get("connect_timeout", 30),
            max_retries=remote.get("max_retries", 5),
            retry_timeouts_as_booting=remote.get("retry_timeouts_as_booting", False),
            sudo_action="freeze",
            workspace_generation=generation,
            runtime_incarnation=incarnation,
            expected_host_key_fingerprint=fingerprint,
            workspace_tier="sandbox",
        )
    except Exception as exc:
        raise EventJournalUnavailable(
            "partial sandbox cleanup authority is malformed"
        ) from exc

    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, backend.connect)
        protocol = await loop.run_in_executor(
            None, backend.protected_workspace_zero_cleanup_strict
        )
        if protocol != "workspace_process_zero_v1":
            raise EventJournalUnavailable(
                "partial sandbox workspace process-zero proof is unavailable"
            )
        return protocol
    except EventJournalUnavailable:
        raise
    except Exception as exc:
        raise EventJournalUnavailable(
            "partial sandbox workspace did not quiesce"
        ) from exc
    finally:
        try:
            await loop.run_in_executor(None, backend.disconnect)
        except Exception:
            logger.warning(
                "Partial sandbox cleanup transport did not disconnect",
                exc_info=True,
            )


async def _cleanup_failed_event_journal_attach(
    thread_id: str, *, restore_thread_id: str | None = None
) -> dict[str, Any] | None:
    """Quiesce a partial attach and return its exact release proof.

    A delivered pinned attach owns a real G/attach reservation. Rotating that
    generation is safe only after every local/remote writer is proven zero;
    preserving the shell or swallowing writer/cleanup uncertainty would let
    old work cross into the replacement. Legacy/stateless cleanup retains its
    historical handoff behavior and returns no release receipt.
    """

    global _session, _thread_id, _event_writer
    global _events_epoch, _next_seq, _tool_inflight, _turn_event_open
    global _turn_tool_execution_identity, _turn_tool_execution_external_hook
    global _loop_user_queue, _loop_interrupt_flag, _loop_interrupt_target_turn_id
    global _hard_interrupt_event
    global _loop_last_user_content
    global _input_runtime_generation
    global _runtime_authorization_admission_open
    global _pinned_status_identity_enabled
    global _pinned_runtime_generation_enabled
    global _failed_attach_workspace_cleanup_context
    global _failed_attach_release_receipt

    exact_generation = _session_runtime_generation
    exact_attach_token = _session_runtime_attach_token
    exact_pinned_attach = bool(
        _pinned_runtime_generation_enabled
        and exact_generation is not None
        and exact_attach_token is not None
    )
    release_receipt: dict[str, Any] | None = None
    cleanup_context = _failed_attach_workspace_cleanup_context

    await _stop_thread_interrupt_watcher()
    await _stop_thread_control_watcher()
    if exact_pinned_attach:
        await _stop_and_join_watchdogs()
        await _quiesce_session_side_tasks()

    writer = _event_writer
    if writer is not None:
        try:
            await writer.close()
        except Exception as exc:
            if exact_pinned_attach:
                raise EventJournalUnavailable(
                    "partial attach event writer did not quiesce"
                ) from exc
            logger.warning(
                "Failed to close event writer after attach failure (thread=%s): %s",
                thread_id,
                exc,
            )
        else:
            _event_writer = None

    failed_session = _session
    if failed_session is not None:
        tool_context = getattr(failed_session, "tool_context", None)
        if tool_context is not None:
            tool_context.citation_verdict_callback = None
            tool_context.canvas_event_callback = None
        try:
            # An exact delivered attach is about to rotate its G and therefore
            # must destructively retire/prove its shell and workspace writers.
            # Legacy/stateless handoff keeps the historical preserve behavior.
            await failed_session.cleanup(
                preserve_shell=not exact_pinned_attach,
                preserve_workspace_daemons=(
                    not exact_pinned_attach
                    and getattr(failed_session, "shell_owner_token", None) is not None
                    and getattr(
                        failed_session,
                        "stateless_warm_reuse_safe",
                        True,
                    )
                    is False
                ),
            )
        except Exception as exc:
            if exact_pinned_attach:
                raise EventJournalUnavailable(
                    "partial attach runtime did not quiesce"
                ) from exc
            logger.warning(
                "Failed to clean partial session after event-journal error "
                "(thread=%s): %s",
                thread_id,
                exc,
            )
    elif exact_pinned_attach:
        if not (
            isinstance(cleanup_context, dict)
            and cleanup_context.get("thread_id") == thread_id
        ):
            raise EventJournalUnavailable(
                "partial attach lacks an exact local cleanup boundary"
            )
        if cleanup_context.get("setup_started") is True:
            await _strict_cleanup_partial_attach_local_resources(cleanup_context)
            tier = cleanup_context.get("workspace_tier")
            if tier == "sandbox":
                protocol = await _strict_cleanup_partial_sandbox_workspace(
                    cleanup_context
                )
                cleanup_context["local_quiescence_protocol"] = protocol
            elif tier in {"virtual", "none"}:
                cleanup_context["local_quiescence_protocol"] = "agent_runtime_zero_v1"
            else:
                # VM/remote requires the orchestrator's exact actuator proof.
                # An agent process can close only its local producers and must
                # retain the delivered attach fence until that authority acts.
                raise EventJournalUnavailable(
                    "partial physical attach requires actuator quiescence"
                )
        else:
            # This monotonic branch is possible only before datasource,
            # session, backend or workspace setup begins. The server repeats
            # the zero-input/control and exact workspace-tuple predicates
            # before rotating G.
            cleanup_context["local_quiescence_protocol"] = "agent_attach_not_started_v1"

    if exact_pinned_attach:
        protocol = str(
            (
                getattr(failed_session, "local_quiescence_protocol", "")
                if failed_session is not None
                else (cleanup_context or {}).get("local_quiescence_protocol")
            )
            or ""
        )
        workspace_generation = (
            str(getattr(failed_session, "workspace_generation", "") or "")
            if failed_session is not None
            else str((cleanup_context or {}).get("workspace_generation") or "")
        )
        workspace_runtime_incarnation = (
            str(getattr(failed_session, "workspace_runtime_incarnation", "") or "")
            if failed_session is not None
            else str((cleanup_context or {}).get("workspace_runtime_incarnation") or "")
        )
        pod_uid = str(os.environ.get("POD_UID") or "").strip()
        if (
            not pod_uid
            or protocol
            not in {
                "workspace_process_zero_v1",
                "agent_runtime_zero_v1",
                "agent_attach_not_started_v1",
            }
            or bool(workspace_generation) != bool(workspace_runtime_incarnation)
            or (protocol == "workspace_process_zero_v1" and not workspace_generation)
            or (protocol == "agent_runtime_zero_v1" and workspace_generation)
        ):
            raise EventJournalUnavailable(
                "partial attach produced no trusted local quiescence receipt"
            )
        release_receipt = {
            "thread_id": thread_id,
            "session_runtime_generation": exact_generation,
            "session_runtime_attach_token": exact_attach_token,
            "agent_pod_uid": pod_uid,
            "local_runtime_quiesced": True,
            "local_quiescence_protocol": protocol,
            "workspace_generation": workspace_generation or None,
            "workspace_runtime_incarnation": (workspace_runtime_incarnation or None),
        }

    _session = None
    _thread_id = restore_thread_id
    _failed_attach_workspace_cleanup_context = None
    _events_epoch = 0
    _next_seq = 0
    _tool_inflight = False
    _turn_tool_execution_identity = None
    _turn_tool_execution_external_hook = None
    _turn_event_open = False
    _loop_user_queue = None
    _loop_interrupt_flag = None
    _loop_interrupt_target_turn_id = None
    _hard_interrupt_event = None
    _loop_last_user_content = [""]
    _input_runtime_generation = None
    _runtime_authorization_admission_open = False
    _pinned_status_identity_enabled = False
    _clear_attached_runtime_identity()
    _clear_attached_runtime_actor()
    _queued_input_claims.clear()
    _clear_all_canvas_awareness()
    _subscribers.clear()
    from agent.tools.registry import register_mcp_tools

    register_mcp_tools(None)
    _apply_session_embedding_env(None)
    if release_receipt is not None and not _retain_failed_attach_release_receipt(
        release_receipt
    ):
        raise EventJournalUnavailable(
            "partial attach release proof conflicts with another runtime"
        )
    return release_receipt


async def _cleanup_failed_attach_until_proven(
    thread_id: str,
    *,
    restore_thread_id: str | None = None,
) -> dict[str, Any] | None:
    """Keep one delivered exact attach nonclaimable until cleanup is proven."""

    expected_session = _session
    expected_identity = (
        _session_runtime_generation,
        _session_runtime_attach_token,
    )
    exact = bool(_pinned_runtime_generation_enabled and all(expected_identity))
    attempt = 0
    while True:
        try:
            return await _cleanup_failed_event_journal_attach(
                thread_id,
                restore_thread_id=restore_thread_id,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            still_exact_owner = bool(
                exact
                and _session is expected_session
                and _session_runtime_generation == expected_identity[0]
                and _session_runtime_attach_token == expected_identity[1]
            )
            if not still_exact_owner:
                raise
            delay = _EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS[
                min(
                    attempt + 1,
                    len(_EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS) - 1,
                )
            ]
            attempt += 1
            logger.warning(
                "Exact failed-attach cleanup remains unproven; retaining "
                "the nonclaimable owner and retrying (thread=%s type=%s)",
                thread_id,
                type(exc).__name__,
            )
            if delay:
                await asyncio.sleep(delay)


async def _release_failed_attach_receipt_until_confirmed(
    thread_id: str,
    *,
    runtime_generation: str | None,
    runtime_attach_token: str | None,
) -> bool:
    """Retry one proven delivered-attach abort without weakening its fence."""

    global _failed_attach_release_receipt

    receipt = _failed_attach_release_receipt
    if not (
        isinstance(receipt, dict)
        and receipt.get("thread_id") == thread_id
        and receipt.get("session_runtime_generation") == runtime_generation
        and receipt.get("session_runtime_attach_token") == runtime_attach_token
    ):
        return False
    client = _orchestrator_client
    if client is None:
        return False
    attempt = 0
    while _failed_attach_release_receipt is receipt:
        try:
            confirmed = await client.release_thread_agent(
                thread_id,
                session_runtime_generation=runtime_generation,
                session_runtime_attach_token=runtime_attach_token,
                agent_pod_uid=receipt["agent_pod_uid"],
                local_runtime_quiesced=True,
                local_quiescence_protocol=receipt["local_quiescence_protocol"],
                workspace_generation=receipt.get("workspace_generation"),
                workspace_runtime_incarnation=receipt.get(
                    "workspace_runtime_incarnation"
                ),
            )
        except Exception as exc:
            logger.warning(
                "Exact failed-attach release attempt failed (thread=%s type=%s)",
                thread_id,
                type(exc).__name__,
            )
            confirmed = False
        if confirmed:
            if _failed_attach_release_receipt is receipt:
                _failed_attach_release_receipt = None
            return True
        delay = _EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS[
            min(attempt + 1, len(_EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS) - 1)
        ]
        attempt += 1
        await asyncio.sleep(delay)
    return False


# Memory-path embedding routing keys. EmbeddingService is a process-wide
# singleton built from these EMBEDDING_* env vars at first call. The memory
# reranker's RERANK_* transport (the `rerank` catalog slot) rides along under
# the same scrub-on-claim contract: the scorer reads them at bind time, so a
# following tenant must never inherit the previous tenant's rerank host/key.
MEMORY_EMBEDDING_ENV_KEYS = (
    "EMBEDDING_PROVIDER",
    "EMBEDDING_MODEL",
    "EMBEDDING_BASE_URL",
    "EMBEDDING_API_KEY",
    "RERANK_MODEL",
    "RERANK_BASE_URL",
    "RERANK_API_KEY",
)


def _apply_session_embedding_env(env_keys: Optional[Dict[str, Any]]) -> None:
    """Replace the process embedding profile with this attach's snapshot.

    Scrub-on-claim (stateless_agents.md §5.6 — M3 deliverable D, and a live
    pinned-lane pod-reuse leak): the KB path (``apply_kb_embedding_env``) was
    deliberately hardened pop-first for pod reuse; the memory path was not —
    it pushed ``EMBEDDING_API_KEY`` into process-global ``os.environ`` and
    never popped it, so a following tenant whose config omitted ``env_keys``
    skipped the block and inherited the prior tenant's key + un-reset
    singleton. Symmetric now: at EVERY attach, unconditionally pop all
    memory-embedding keys and null the memory-embedding singleton BEFORE
    applying the new ``env_keys`` (which then re-set them only if provided).

    Acceptance (tests/test_turn_executor.py scrub matrix): after an attach
    with tenant-A env_keys followed by an attach with tenant-B env_keys
    absent, ``os.environ`` carries no A values and the singleton is None.
    """
    for k in MEMORY_EMBEDDING_ENV_KEYS:
        os.environ.pop(k, None)
    from shared.runtime.services import embedding_service as _embedding_module

    _embedding_module._embedding_service = None
    # KB path: already a complete pop-first attach-time snapshot.
    _embedding_module.apply_kb_embedding_env(env_keys)
    if env_keys:
        for k in MEMORY_EMBEDDING_ENV_KEYS:
            value = env_keys.get(k)
            if value is not None:
                os.environ[k] = str(value)
        if any(
            k in env_keys
            for k in MEMORY_EMBEDDING_ENV_KEYS + _embedding_module.KB_EMBEDDING_ENV_KEYS
        ):
            logger.info(
                "Embedding overrides applied: memory_model=%s, kb_model=%s",
                os.environ.get("EMBEDDING_MODEL"),
                os.environ.get("KB_EMBEDDING_MODEL"),
            )


def _llm_config_with_cache_key(llm_cfg: Any) -> Any:
    """Copy ``llm_cfg`` with the per-thread OpenAI cache-routing key.

    The key is stable for the thread's life, so the provider-side prefix
    cache survives pod rotation on the stateless lane (stateless_agents.md
    OQ5 — the ``cache_read_input_tokens > 0`` turn-2 acceptance). The loader
    transmits it to first-party OpenAI only; every other provider/endpoint
    ignores the field, so setting it unconditionally here is safe.
    """
    if not _thread_id:
        return llm_cfg
    import dataclasses

    return dataclasses.replace(llm_cfg, prompt_cache_key=f"srw-thread-{_thread_id}")


async def _attach_session_inner(
    thread_id: str,
    config_override: Optional[Dict[str, Any]] = None,
    resolved_config: Optional[Dict[str, Any]] = None,
    project_ids: Optional[List[str]] = None,
    datasources: Optional[List[Dict[str, Any]]] = None,
    config_name: Optional[str] = None,
    runtime_actor: Optional[Dict[str, Any]] = None,
    pinned_status_identity_contract: Any = None,
    pinned_runtime_generation_contract: Any = None,
    session_runtime_generation: Any = None,
    session_runtime_attach_token: Any = None,
    conversation_revision: Any = None,
    events_epoch: Any = None,
    workspace_generation: Any = _ATTACH_WORKSPACE_IDENTITY_UNSET,
    workspace_runtime_incarnation: Any = _ATTACH_WORKSPACE_IDENTITY_UNSET,
) -> None:
    """Create and attach a PersistentSession for the given thread.

    This is the core session setup logic, extracted from the lifespan so it
    can be reused by both dedicated mode (lifespan startup) and pool mode
    (POST /session/attach).

    ``config_name`` (pool mode): the thread's config, used as the session
    base instead of the pod's boot config when provided.
    """
    global _session, _thread_id, _events_epoch, _next_seq, _tool_inflight
    global _turn_tool_execution_identity, _turn_tool_execution_external_hook
    global _turn_event_open, _session_generation, _draft_title_value
    global _event_writer, _cloud_sync_retry_pending
    global _runtime_authorization_admission_open
    global _pinned_status_identity_enabled
    global _pinned_runtime_generation_enabled
    global _failed_attach_workspace_cleanup_context

    expected_workspace_identity = _canonical_attach_workspace_identity(
        workspace_generation,
        workspace_runtime_incarnation,
    )
    prior_thread_id = _thread_id

    _cloud_sync_retry_pending = False
    # A pooled process must never carry a prior Officer's successful
    # maintenance result into the next attachment. Ordinary sessions bypass
    # this latch through ``_officer_cfg() is None`` below.
    _runtime_authorization_admission_open = False
    _pinned_status_identity_enabled = bool(
        type(pinned_status_identity_contract) is int
        and pinned_status_identity_contract == 1
    )
    runtime_contract_advertised = bool(
        type(pinned_runtime_generation_contract) is int
        and pinned_runtime_generation_contract == 1
    )
    client_generation = (
        getattr(_orchestrator_client, "session_runtime_generation", None)
        if _orchestrator_client is not None
        else None
    )
    if not isinstance(client_generation, str):
        client_generation = None
    client_attach_token = (
        getattr(_orchestrator_client, "session_runtime_attach_token", None)
        if _orchestrator_client is not None
        else None
    )
    if not isinstance(client_attach_token, str):
        client_attach_token = None
    _adopt_attached_runtime_identity(
        session_runtime_generation
        if session_runtime_generation is not None
        else client_generation,
        session_runtime_attach_token
        if session_runtime_attach_token is not None
        else client_attach_token,
        contract_advertised=(
            runtime_contract_advertised
            or (
                getattr(
                    _orchestrator_client,
                    "pinned_runtime_generation_contract",
                    False,
                )
                is True
            )
        ),
    )
    _clear_all_canvas_awareness()

    if _session is not None:
        raise RuntimeError(
            f"Cannot attach thread {thread_id}: already attached to {_thread_id}"
        )
    if _failed_attach_workspace_cleanup_context is not None:
        raise RuntimeError(
            "Cannot attach while a prior runtime cleanup proof remains pending"
        )

    stale_side_tasks = {task for task in _session_side_tasks if not task.done()}
    if stale_side_tasks:
        raise RuntimeError(
            "Cannot attach a new thread while prior session tasks remain active"
        )
    _session_generation += 1
    _draft_title_value = None

    # A normal detach always closes and clears the prior writer. Recover from a
    # stale writer defensively before a pool-mode reattach so no event can land
    # under the previous thread/pool identity.
    if _event_writer is not None:
        logger.warning(
            "Closing stale thread_events writer before attaching thread %s",
            thread_id,
        )
        await _event_writer.close()
        _event_writer = None

    _thread_id = thread_id
    if _pinned_runtime_generation_enabled and not _stateless_mode():
        _failed_attach_workspace_cleanup_context = {
            "thread_id": thread_id,
            "setup_started": False,
            "workspace_tier": None,
            "workspace_generation": None,
            "workspace_runtime_incarnation": None,
            "workspace_ssh_host_key_fingerprint": None,
            "remote": None,
            "datasources": {},
            "datasource_clients": {},
        }

    runtime_actor_context = _runtime_actor_context_for_attach(runtime_actor)

    # Determine the backend before polling: a lite (virtual/none) session has
    # NO workspace pod, so polling for one would always fail (WorkspaceNotReady).
    # The pool path passes config_override; a dedicated agent fetches it here.
    # The orchestrator attaches the lite object-store mounts to this response
    # for lite threads, so the session can build its backend without a pod.
    _rc, _co = resolved_config, config_override
    attached_workspace_generation = ""
    if _rc is None and _co is None and _orchestrator_client and _thread_id:
        try:
            _peek = await _orchestrator_client.get_thread_workspace(_thread_id)
            if isinstance(_peek, dict):
                peek_delivery = _protected_workspace_delivery(_peek)
                # A valid engaging response is intentionally coordinate- and
                # credential-free, including the runtime generation.  It is a
                # poll instruction, not an attach payload: validating ready
                # identity here would make the dedicated path fail before
                # `_poll_workspace_ready` can observe engage -> ready.
                if peek_delivery != "engaging":
                    _assert_attach_workspace_payload(
                        expected_workspace_identity,
                        _peek,
                    )
                    _bind_attached_runtime_payload(
                        _peek,
                        protected_required=(_protected_workspace_marker(_peek) == "on"),
                    )
                    _pinned_status_identity_enabled = (
                        _pinned_status_identity_advertised(_peek)
                    )
                    attached_workspace_generation = str(
                        _peek.get("workspace_generation") or ""
                    )
                _rc = _peek.get("resolved_config")
                _co = _peek.get("config_override")
        except (ProtectedCloudUnavailable, SessionEnded, WorkspaceNotReady):
            raise
        except Exception:
            pass
    # Check BOTH blobs: the resolved config is the agent's preferred hydration
    # source, the override is the authoritative tier — either may carry it.
    is_lite_session = _session_backend_is_lite(_rc) or _session_backend_is_lite(_co)
    # Same dual-blob read for the VM tier: a vm-tier session must attach to its
    # VM and never to a container that happens to be ready (Defect 2).
    is_vm_session = _session_backend_is_vm(_rc) or _session_backend_is_vm(_co)
    _assert_attach_workspace_tier(
        expected_workspace_identity,
        is_lite_session=is_lite_session,
    )
    if _failed_attach_workspace_cleanup_context is not None:
        _failed_attach_workspace_cleanup_context["workspace_tier"] = (
            "vm" if is_vm_session else "virtual" if is_lite_session else "sandbox"
        )

    # Wait for workspace container (if orchestrator is provisioning one).
    # Skipped for lite tiers, which run with no pod — the session builds its
    # object-store backend from the injected mounts (persistent_session.py).
    workspace_override = None
    if not is_lite_session and _orchestrator_client and _thread_id:
        workspace_override = await _poll_workspace_ready(
            _orchestrator_client,
            _thread_id,
            timeout=120,
            raise_on_denied=True,
            require_vm=is_vm_session,
        )
        if workspace_override:
            _assert_attach_workspace_payload(
                expected_workspace_identity,
                workspace_override,
            )
            _bind_attached_runtime_payload(
                workspace_override,
                protected_required=(
                    _protected_workspace_marker(workspace_override) == "on"
                ),
            )
            _pinned_status_identity_enabled = _pinned_status_identity_advertised(
                workspace_override
            )
            logger.info(
                f"Workspace ready ({workspace_override.get('backend')}): "
                f"{workspace_override['remote']['host']}"
            )
            if _failed_attach_workspace_cleanup_context is not None:
                remote = workspace_override.get("remote")
                _failed_attach_workspace_cleanup_context.update(
                    {
                        "workspace_tier": workspace_override.get("backend"),
                        "workspace_generation": workspace_override.get(
                            "workspace_generation"
                        ),
                        "workspace_runtime_incarnation": workspace_override.get(
                            "workspace_runtime_incarnation"
                        ),
                        "workspace_ssh_host_key_fingerprint": workspace_override.get(
                            "workspace_ssh_host_key_fingerprint"
                        ),
                        "remote": dict(remote) if isinstance(remote, dict) else None,
                    }
                )
        elif is_vm_session:
            # Never silently downgrade a vm-tier session to a container. Say what
            # actually failed so the pod log names the real cause instead of
            # blaming a container this session was never supposed to have.
            raise WorkspaceNotReady(
                "VM workspace never became ready for this vm-tier session "
                "(metadata.vm did not reach status='ready' with an ssh_host "
                "within the VM budget). Not falling back to a sandbox container."
            )
        else:
            raise WorkspaceNotReady(
                "No workspace container provisioned for thread. "
                "Cannot attach session without an isolated workspace."
            )
    elif is_lite_session:
        logger.info(
            "Lite (no-pod) session for thread %s — skipping workspace poll",
            _thread_id,
        )

    attached_workspace_generation = str(
        (workspace_override or {}).get("workspace_generation")
        or attached_workspace_generation
        or ""
    )

    # Apply config overrides, project_ids, and datasources from thread metadata
    if not config_override:
        config_override = (workspace_override or {}).get("config_override")
    if resolved_config is None:
        resolved_config = (workspace_override or {}).get("resolved_config")
    if not project_ids:
        project_ids = (workspace_override or {}).get("project_ids") or []
    cloud_mount_cfg = (
        workspace_override.get("cloud_mount") if workspace_override else None
    )
    # Protected state is an exact three-way contract.  Always perform a fresh
    # fetch before constructing PersistentSession: an engage can be revoked or
    # fail after the readiness poll, and stale credentials must not win merely
    # because the first response already populated every optional field.
    initial_delivery = _protected_workspace_delivery(workspace_override or {})
    protected_cloud = initial_delivery == "ready"
    protected_workspace_identity = (
        _protected_workspace_identity(workspace_override)
        if protected_cloud and workspace_override is not None
        else None
    )
    if protected_cloud:
        _bind_attached_runtime_payload(
            workspace_override,
            protected_required=True,
        )
    if _orchestrator_client and _thread_id:
        try:
            ws_info = await _orchestrator_client.get_thread_workspace(_thread_id)
            if ws_info:
                _assert_attach_workspace_payload(
                    expected_workspace_identity,
                    ws_info,
                )
                _bind_attached_runtime_payload(
                    ws_info,
                    protected_required=(
                        protected_cloud or _protected_workspace_marker(ws_info) == "on"
                    ),
                )
                _pinned_status_identity_enabled = _pinned_status_identity_advertised(
                    ws_info
                )
                fresh_delivery = _protected_workspace_delivery(ws_info)
                if fresh_delivery == "engaging":
                    raise ProtectedCloudUnavailable(
                        "protected-cloud engage changed while attaching"
                    )
                if protected_cloud and fresh_delivery != "ready":
                    raise ProtectedCloudUnavailable(
                        "protected-cloud authority disappeared while attaching"
                    )
                protected_cloud = fresh_delivery == "ready"
                attached_workspace_generation = attached_workspace_generation or str(
                    ws_info.get("workspace_generation") or ""
                )
                if not config_override:
                    config_override = ws_info.get("config_override")
                if resolved_config is None:
                    resolved_config = ws_info.get("resolved_config")
                if not project_ids:
                    project_ids = ws_info.get("project_ids") or []
                if not datasources:
                    datasources = ws_info.get("datasources")
                if protected_cloud:
                    # Latest authoritative bytes replace, rather than fill, an
                    # earlier mount so a revoked/rotated reader cannot be used.
                    cloud_mount_cfg = ws_info.get("cloud_mount")
                    _validate_protected_cloud_mount(cloud_mount_cfg)
                    fresh_identity = _protected_workspace_identity(ws_info)
                    if fresh_identity != protected_workspace_identity:
                        raise ProtectedCloudUnavailable(
                            "protected workspace identity changed before setup"
                        )
                elif not cloud_mount_cfg:
                    cloud_mount_cfg = ws_info.get("cloud_mount")
            elif protected_cloud:
                raise ProtectedCloudUnavailable(
                    "protected-cloud workspace authority is unavailable"
                )
        except (ProtectedCloudUnavailable, SessionEnded, WorkspaceNotReady):
            raise
        except Exception:
            if protected_cloud:
                raise ProtectedCloudUnavailable(
                    "protected-cloud workspace revalidation failed"
                )

    # config_override is final here (request > workspace_override > ws_info)
    # and caller-authored on every one of those routes. Strip loader-owned
    # keys ONCE, before the runtime decorates it (``extra._cli_datasources``
    # below) and before the deep-merge onto config.extra further down: a
    # thread override carrying ``_db_prompt_keys: []`` must not unfence the
    # expert's DB prompts (security audit 2026-08-27, finding #2).
    if isinstance(config_override, dict):
        from shared.runtime.core.loader import strip_loader_owned_keys

        config_override = strip_loader_owned_keys(config_override)

    # Crossing this one-way boundary means config/datasource/session setup may
    # have created local or remote actors. Any later delivered-attach abort
    # must prove actual agent/workspace process zero; it can never downgrade
    # to `agent_attach_not_started_v1` even if construction fails before
    # PersistentSession is assigned.
    if _failed_attach_workspace_cleanup_context is not None:
        _failed_attach_workspace_cleanup_context["setup_started"] = True

    # Process datasources: create connections, inject env vars, apply tool overrides
    # Note: repository cloning is deferred until AFTER the workspace is
    # initialized, then runs on the workspace backend (repos/<name> on the
    # workspace container) — never on the agent pod.
    datasources_dict: Dict[str, Any] = {}
    datasource_clients: Dict[str, Any] = {}
    repo_datasources: List[Dict[str, Any]] = []
    kb_datasources: List[Dict[str, Any]] = []
    mcp_manager = None
    if datasources:
        from agent.core.datasource_setup import (
            datasource_tool_categories,
            process_datasources,
        )

        # Separate repos (cloned later) from other datasources
        repo_datasources = [ds for ds in datasources if ds.get("type") == "repository"]
        kb_datasources = [ds for ds in datasources if ds.get("type") == "kb"]
        non_repo_datasources = [
            ds for ds in datasources if ds.get("type") not in ("repository", "kb")
        ]
        datasources_dict, datasource_clients, cli_ds_types = process_datasources(
            non_repo_datasources
        )
        if _failed_attach_workspace_cleanup_context is not None:
            _failed_attach_workspace_cleanup_context["datasources"] = datasources_dict
            _failed_attach_workspace_cleanup_context["datasource_clients"] = (
                datasource_clients
            )
        mcp_manager = datasources_dict.get("mcp")
        if mcp_manager is not None:
            _t_step = time.perf_counter()
            try:
                await mcp_manager.connect_all()
            except Exception as e:
                logger.warning(
                    "Unexpected session MCP discovery failure (%s); continuing",
                    type(e).__name__,
                )
            mcp_manager.annotate_configs()
            logger.info(
                "attach step: mcp connect_all %.2fs", time.perf_counter() - _t_step
            )

        # Inject datasource tool categories so the correct tools are loaded
        # when config is resolved below. Shared map with the orchestrator's
        # _build_datasource_tool_override — the two previously disagreed on
        # read-write managed connectors (write-tools vs CLI-only).
        ds_tool_categories = datasource_tool_categories(datasources)
        config_override = dict(config_override or {})
        tools_override = dict(config_override.get("tools", {}))
        tools_override.update(ds_tool_categories)
        if tools_override:
            config_override["tools"] = tools_override

        if cli_ds_types:
            config_override.setdefault("extra", {})["_cli_datasources"] = cli_ds_types

        # Hydrated attaches load the orchestrator-resolved blob below and
        # never touch config_override — fold the same enrichment into the
        # blob's agent dict, or a hydrated attach silently drops read-only
        # connector tools and the CLI prompt block. The warm-pool path
        # compensated orchestrator-side; the dedicated-pod path did not
        # (live_session_settings.md P0.2).
        _apply_datasource_enrichment_to_resolved(
            resolved_config, ds_tool_categories, cli_ds_types
        )

        logger.info(
            "Processed %d datasource(s) for session: %d connections, %d CLI",
            len(datasources),
            len(datasources_dict),
            len(cli_ds_types),
        )

    # Pool-mode agents serve sequential sessions. Replace (or clear) the
    # process-global dynamic entries before config hydration/tool loading.
    from agent.tools.registry import register_mcp_tools

    register_mcp_tools(mcp_manager)

    effective_config = _agent.config
    _hydrated = False
    if resolved_config:
        # Orchestrator-resolved config: the blob is the full, frozen,
        # credential-injected session config (base + expert + overrides). Hydrate
        # it directly — no config_name load, no config_override flat-merge (which
        # would degrade the resolved layers). This is the warm-pool / cold-attach
        # expert delivery channel — the fix for the 3-minute stall.
        from shared.runtime.core.loader import create_llm, load_config_from_resolved

        effective_config = load_config_from_resolved(resolved_config)
        _hydrated = True
        logger.info(
            "Attach: hydrated orchestrator-resolved config for thread %s "
            "(model=%s, persona_source=%s)",
            thread_id,
            effective_config.llm.model,
            effective_config.extra.get("_persona_source"),
        )
    elif config_name:
        # The thread's config beats the pod's boot config — idle-pool pods
        # boot as workers, and a session served from the worker YAML loses
        # its persistent memory pipeline (no teardown_extractor) among the
        # rest of the session profile. Fail-loud on unknown names.
        effective_config = _load_expert_config(config_name)
        logger.info(
            "Attach: session base config '%s' (overrides pod boot config)",
            config_name,
        )

    llm = _agent._llm
    if _hydrated:
        # The resolved llm carries the final model + injected transport.
        llm = create_llm(
            _llm_config_with_cache_key(effective_config.llm),
            effective_config.limits,
        )
        logger.info(
            "Attach: built session LLM from resolved config: model=%s",
            effective_config.llm.model,
        )
    elif config_override:
        import dataclasses

        from shared.runtime.core.loader import (
            _apply_settings_matrix,
            create_llm,
            deep_merge,
            load_agent_config_from_dict,
        )

        # The legacy (experts-off) attach path reads the RAW request override
        # rather than the orchestrator's merged fragment, so it needs its own
        # normalisation — otherwise `canvas: false` never becomes the `[]` that
        # _apply_session_tool_group_markers matches on, and the group stays on.
        # Same seam for a legacy llm.strategic/tactical/subagent block.
        config_override = normalize_delegation_block(
            normalize_llm_tiers(
                normalize_tool_policy(config_override, source="thread-override"),
                source="thread-override",
            ),
            source="thread-override",
        )
        base_dict = dataclasses.asdict(effective_config)
        merged = deep_merge(base_dict, config_override)
        _apply_session_tool_group_markers(merged, config_override)

        # If the override changes the model, re-apply settings_matrix for the
        # new model family so temperature/top_p/limits get correct defaults.
        # Override LLM keys are treated as "explicitly set" so the matrix
        # won't overwrite them.
        if config_override.get("llm"):
            override_llm_keys = set(config_override["llm"].keys())
            _apply_settings_matrix(
                merged, override_llm_keys, effective_config._deployment_dir
            )

        effective_config = load_agent_config_from_dict(
            merged, deployment_dir=effective_config._deployment_dir
        )
        if config_override.get("llm"):
            llm = create_llm(
                _llm_config_with_cache_key(effective_config.llm),
                effective_config.limits,
            )
            logger.info(
                f"Config override applied: model={effective_config.llm.model}, "
                f"temperature={effective_config.llm.temperature}"
            )

    # Task 15: thread protected_cloud into config.extra via the same channel
    # _cli_datasources uses (loader.py reads config.extra["_protected_cloud"]
    # at render time — loader.py:3913-3915), so the interactive prompt's
    # honesty block renders for this session. Applied once, after
    # effective_config is fully resolved (hydrated / config_override-merged /
    # config_name-loaded / plain boot config) rather than folded into the
    # config_override merge above — pushing it through config_override would
    # make an otherwise-empty override truthy and force every protected
    # thread through the `elif config_override:` deep-merge/rebuild branch
    # even when no other override exists.
    #
    # NEVER mutate effective_config in place here: on the plain-boot path
    # (no resolved_config / config_name / config_override) effective_config
    # IS the module-singleton _agent.config, which pool-mode pods reuse
    # across sequential session attaches — an in-place write would leak
    # _protected_cloud into every later NON-protected session on the pod
    # (whose live cloud files really are saved, making the honesty block a
    # lie). Clone via dataclasses.replace with a copied extra dict instead;
    # the new object is assigned back to the local, so all downstream use
    # in this function picks it up. The guards skip test stubs that aren't
    # real AgentConfig dataclasses.
    import dataclasses

    if (
        protected_cloud
        and hasattr(effective_config, "extra")
        and dataclasses.is_dataclass(effective_config)
    ):
        effective_config = dataclasses.replace(
            effective_config,
            extra={**effective_config.extra, "_protected_cloud": True},
        )

    # Auxiliary LLM rebuild. The boot-time _agent._auxiliary_llm is built from
    # config.auxiliary.model in the YAML default — for persistent sessions
    # without an override that's RedHatAI/... with no transport, which routes
    # title-generation/memory-extraction calls to api.openai.com with
    # not-needed and 401s. When the orchestrator's create_thread injection
    # (or a runtime config.update) supplies an auxiliary section, build a
    # session-scoped AuxiliaryLLM and pass it in instead of the singleton.
    auxiliary_llm = _agent._auxiliary_llm
    if (config_override and config_override.get("auxiliary", {}).get("model")) or (
        _hydrated and effective_config.auxiliary and effective_config.auxiliary.model
    ):
        from shared.runtime.core.loader import (
            LLMConfig,
            create_llm,
            resolve_model_settings,
        )
        from shared.runtime.services.auxiliary import AuxiliaryLLM

        aux_cfg = effective_config.auxiliary
        model_settings = resolve_model_settings(
            aux_cfg.model, effective_config._deployment_dir
        )
        aux_llm_config = LLMConfig(
            model=aux_cfg.model,
            base_url=aux_cfg.base_url,
            api_key=aux_cfg.api_key,
            provider=aux_cfg.provider,
            extra_headers=aux_cfg.extra_headers,
            temperature=aux_cfg.temperature,
            top_p=model_settings.get("top_p"),
            top_k=model_settings.get("top_k"),
            model_max_context_tokens=model_settings.get("model_max_context_tokens"),
            extra_body=model_settings.get("extra_body"),
            max_retries=1,
        )
        aux_structured_output_method = model_settings.get(
            "structured_output_method", "json_schema"
        )
        fallback_model = effective_config.llm.model
        fallback_settings = resolve_model_settings(
            fallback_model, effective_config._deployment_dir
        )
        aux_inner = create_llm(aux_llm_config, effective_config.limits)
        auxiliary_llm = AuxiliaryLLM(
            llm=aux_inner,
            max_iterations=aux_cfg.max_iterations,
            timeout=aux_cfg.timeout,
            max_context_tokens=model_settings.get("model_max_context_tokens"),
            structured_output_method=aux_structured_output_method,
            # Drop-in fallback to the main session model when the dedicated aux
            # model is unreachable — keeps compaction/memory/titles alive instead
            # of crashing the session. See
            # knowledge-base/knowledge/issues/openrouter_auxiliary_misrouted_to_openai.md.
            fallback_llm=llm,
            fallback_structured_output_method=fallback_settings.get(
                "structured_output_method", "json_schema"
            ),
        )
        logger.info(
            "Auxiliary override applied: model=%s, base_url=%s",
            aux_cfg.model,
            aux_cfg.base_url or "default",
        )

    # Embedding override + scrub-on-claim (§5.6): replace the process-wide
    # embedding profile (memory + KB) with this attach's snapshot — pop-first
    # on BOTH paths, singleton nulled unconditionally. Extracted to a helper
    # so the tenant-A→tenant-B residue acceptance is unit-testable.
    _env_keys_src = (
        (effective_config.extra or {}).get("env_keys")
        if _hydrated
        else (config_override.get("env_keys") if config_override else None)
    )
    _apply_session_embedding_env(_env_keys_src)

    from agent.services.knowledge.bindings import build_knowledge_bindings

    knowledge_bindings = build_knowledge_bindings(
        project_ids=project_ids or [],
        datasources=kb_datasources,
        runtime_actor=runtime_actor_context,
    )

    # Create PersistentSession
    live_lease = _current_lease_var.get()
    shell_owner_token = None
    if live_lease is not None and live_lease.active:
        if str(live_lease.unit_id) != str(_thread_id):
            raise RuntimeError(
                "Stateless lease identity does not match the session being attached"
            )
        shell_owner_token = live_lease.lease_token

    _session = PersistentSession(
        thread_id=_thread_id,
        config=effective_config,
        shell_owner_token=shell_owner_token,
        protected_cloud_required=protected_cloud,
        pinned_runtime_identity_required=bool(
            _pinned_runtime_generation_enabled and shell_owner_token is None
        ),
        orchestrator_client=_orchestrator_client,
        session_parent_authority_provider=_session_subagent_parent_authority,
        subagent_provider_admission=_loop_provider_admission_open,
        subagent_effect_authority=_loop_runtime_effect_authority_current,
        subagent_settlement_authority=(_loop_runtime_settlement_authority_current),
        subagent_event_callback=_session_subagent_event_available,
        project_ids=project_ids or [],
        datasources=datasources_dict,
        knowledge_bindings=knowledge_bindings,
        runtime_actor=runtime_actor_context,
        _datasource_clients=datasource_clients,
        # Raw payload kept as the live-change diff baseline (Slice B).
        datasource_configs=list(datasources or []),
    )
    _session.execution_snapshot = (resolved_config or {}).get("execution_snapshot")
    # PersistentSession now owns every local/remote cleanup handle. The
    # construction-only context must not survive into pool reuse.
    _failed_attach_workspace_cleanup_context = None
    if protected_workspace_identity is not None:
        _session.protected_workspace_generation = (
            protected_workspace_identity.workspace_generation
        )
        _session.protected_workspace_runtime_incarnation = (
            protected_workspace_identity.runtime_incarnation
        )
    if project_ids:
        logger.info(f"Session scoped to {len(project_ids)} project(s): {project_ids}")
    git_remote_url = (
        workspace_override.get("git_remote_url") if workspace_override else None
    )
    _t_step = time.perf_counter()
    try:
        await _session.setup(
            llm=llm,
            auxiliary_llm=auxiliary_llm,
            postgres_conn=_agent.postgres_conn,
            vector_conn=getattr(_agent, "vector_conn", None),
            workspace_override=workspace_override,
            git_remote_url=git_remote_url,
            cloud_mount_cfg=cloud_mount_cfg,
        )
        if protected_cloud:
            if _orchestrator_client is None or _thread_id is None:
                raise ProtectedCloudUnavailable(
                    "protected-cloud workspace cannot be revalidated"
                )
            final_workspace = await _orchestrator_client.get_thread_workspace(
                _thread_id
            )
            if not isinstance(final_workspace, dict):
                raise ProtectedCloudUnavailable(
                    "protected-cloud workspace authority is unavailable"
                )
            _assert_attach_workspace_payload(
                expected_workspace_identity,
                final_workspace,
            )
            _bind_attached_runtime_payload(
                final_workspace,
                protected_required=True,
            )
            if _protected_workspace_delivery(final_workspace) != "ready":
                raise ProtectedCloudUnavailable(
                    "protected-cloud workspace is no longer ready"
                )
            final_mount = final_workspace.get("cloud_mount")
            _validate_protected_cloud_mount(final_mount)
            final_identity = _protected_workspace_identity(final_workspace)
            if (
                final_identity != protected_workspace_identity
                or final_mount != cloud_mount_cfg
                or not _session.protected_cloud_ready()
            ):
                raise ProtectedCloudUnavailable(
                    "protected-cloud mount authority changed during setup"
                )
    except BaseException:
        await _cleanup_failed_event_journal_attach(
            thread_id, restore_thread_id=prior_thread_id
        )
        raise
    logger.info("attach step: session.setup %.2fs", time.perf_counter() - _t_step)
    # Install the lifecycle provider fence before restore/attach can invoke
    # compaction or any other auxiliary model. Turn-complete and hot-swap paths
    # call the same idempotent wiring helper again for rebuilt instances.
    _wire_session_aux_archiver()

    # Live citation-verdict push: let the engine's background verifier broadcast
    # pending→verified/failed so the cockpit citations panel updates in place
    # rather than only at the next per-turn refresh. Set before the first turn
    # (so it's wired before the lazily-built CitationEngine is first used).
    if _session is not None and _session.tool_context is not None:
        _session.tool_context.citation_verdict_callback = _emit_citation_verdict
        _session.tool_context.canvas_event_callback = _emit_canvas_event

    # Resolve the authoritative (generation, seq seed) before the first
    # broadcast. Clean reattaches REUSE the thread's current epoch with the
    # seq counter seeded above every previously served frame, so cached client
    # cursors stay valid and no cache-wipe cascade fires; the epoch bumps only
    # when the previous session life is terminal (see
    # _resolve_event_journal_epoch). A provisioning SSE opened against a
    # pre-bump generation uses the existing mid-stream epoch-change
    # reconciliation path.
    _tool_inflight = False
    _turn_tool_execution_identity = None
    _turn_tool_execution_external_hook = None
    _turn_event_open = False
    _events_epoch = 0
    _next_seq = 0
    if _session is not None and _session.postgres_conn is not None:
        try:
            _events_epoch, _next_seq = await _resolve_event_journal_epoch(
                _session.postgres_conn, _thread_id
            )
            live_lease = _current_lease_var.get()
            pinned_agent_id = (
                _registered_pinned_agent_id() if live_lease is None else None
            )
            writer = _OrderedPersistentEventWriter(
                postgres_conn=_session.postgres_conn,
                thread_id=_thread_id,
                epoch=_events_epoch,
                on_terminal_failure=_event_persistence_failed,
                # Stateless executor attach: fence every flush on the live
                # claim (the executor set the LeaseHandle before attaching).
                # Pinned lane: ContextVar default None → today's statement.
                lease=live_lease,
                pinned_agent_id=pinned_agent_id,
                pinned_runtime_generation=(
                    _session_runtime_generation if live_lease is None else None
                ),
                pinned_runtime_attach_token=(
                    _session_runtime_attach_token if live_lease is None else None
                ),
            )
            writer.start()
            _event_writer = writer
            if live_lease is None and pinned_agent_id is not None:
                await _start_thread_control_watcher(agent_id=pinned_agent_id)
        except Exception as exc:
            logger.error(
                "Event journal initialization failed; aborting session attach "
                "(thread=%s): %s",
                _thread_id,
                exc,
                exc_info=True,
            )
            await _cleanup_failed_event_journal_attach(thread_id)
            if isinstance(exc, EventJournalUnavailable):
                raise
            raise EventJournalUnavailable(
                "Persistent event journal initialization failed"
            ) from exc

    cloud_mount_active = bool(
        _session.cloud_mount_manager and _session.cloud_mount_manager.active
    )

    from agent.core.datasource_setup import install_workspace_credentials

    await asyncio.to_thread(
        install_workspace_credentials, datasources or [], _session.workspace_manager
    )

    # Clone repository datasources into the workspace (deferred from above).
    # All clone/auth operations run on the workspace backend — there is no
    # agent-local clone path (knowledge-base/knowledge/features/no_workspace_agent_mode.md §9.4).
    if repo_datasources and _session.workspace_manager:
        from agent.core.datasource_setup import clone_repository_datasources

        clone_repository_datasources(repo_datasources, _session.workspace_manager)

    # README.md workspace-facts block (connectors, materials, layout) — after
    # the workspace is initialized and repositories are cloned. Written even
    # without connectors so the file states the explicit "none" case.
    if _session.workspace_manager:
        from agent.core.datasource_setup import inject_workspace_facts

        try:
            inject_workspace_facts(
                datasources or [],
                _session.workspace_manager,
                expert=getattr(_session.config, "display_name", None),
            )
        except Exception as e:
            logger.warning(f"Failed to write workspace facts: {e}")

    # Initialize cloud workspace sync if the orchestrator gave us a config.
    # F-C1: a protected thread NEVER adopts cloud_sync or nc_session_folder
    # from either fetch site — protected mode's only sanctioned live-write
    # surface is the capture overlay (already reflected in
    # cloud_mount_active above); letting either field through here would
    # rebuild a live agent-service WebDAV sync in every degraded-protected
    # scenario (refused engage, flag off, VM tier, overlay-failure teardown).
    suppress_disposable_cloud = bool(
        _stateless_mode()
        and getattr(getattr(effective_config, "workspace", None), "backend", None)
        == "none"
    )
    cloud_cfg = (
        None
        if cloud_mount_active or protected_cloud or suppress_disposable_cloud
        else workspace_override.get("cloud_sync")
        if workspace_override
        else None
    )
    nc_folder = (
        None
        if protected_cloud or suppress_disposable_cloud
        else workspace_override.get("nc_session_folder")
        if workspace_override
        else None
    )
    cloud_degraded_hint = False
    if not suppress_disposable_cloud and (
        not cloud_mount_active
        and (not cloud_cfg or not nc_folder)
        and _orchestrator_client
        and _thread_id
    ):
        try:
            ws_info = await _orchestrator_client.get_thread_workspace(_thread_id)
            if ws_info:
                _assert_attach_workspace_payload(
                    expected_workspace_identity,
                    ws_info,
                )
                _bind_attached_runtime_payload(
                    ws_info,
                    protected_required=(
                        protected_cloud or _protected_workspace_marker(ws_info) == "on"
                    ),
                )
                fresh_delivery = _protected_workspace_delivery(ws_info)
                if protected_cloud:
                    if fresh_delivery != "ready":
                        raise ProtectedCloudUnavailable(
                            "protected-cloud authority changed during attach"
                        )
                    fresh_mount = ws_info.get("cloud_mount")
                    _validate_protected_cloud_mount(fresh_mount)
                    if (
                        fresh_mount != cloud_mount_cfg
                        or _protected_workspace_identity(ws_info)
                        != protected_workspace_identity
                    ):
                        raise ProtectedCloudUnavailable(
                            "protected-cloud mount authority changed during attach"
                        )
                elif fresh_delivery == "ready":
                    raise ProtectedCloudUnavailable(
                        "protected-cloud mode was enabled during attach"
                    )
                attached_workspace_generation = attached_workspace_generation or str(
                    ws_info.get("workspace_generation") or ""
                )
                if not protected_cloud:
                    cloud_cfg = cloud_cfg or ws_info.get("cloud_sync")
                    nc_folder = nc_folder or ws_info.get("nc_session_folder")
                cloud_degraded_hint = bool(ws_info.get("cloud_sync_degraded"))
        except (ProtectedCloudUnavailable, SessionEnded, WorkspaceNotReady):
            raise
        except Exception:
            # A stateless turn cannot distinguish "no cloud configured" from
            # "the credential/config boundary was unreachable" and must not
            # execute unsynced on that ambiguity. Pinned keeps the historical
            # degraded behavior and retries on its next boundary.
            if _stateless_mode():
                _cloud_sync_retry_pending = True
            if protected_cloud:
                raise ProtectedCloudUnavailable(
                    "protected-cloud workspace revalidation failed"
                )
    if suppress_disposable_cloud:
        # backend=none is an intentionally disposable ScratchBackend with no
        # user file tools. The orchestrator may still provision a generic
        # session cloud folder; mirroring internal scratch scaffolding into it
        # would both violate the stateless tier contract and lack a durable
        # workspace generation. Suppress both structured and legacy sync paths
        # only for stateless claims; pinned keeps its historical behavior.
        cloud_cfg = None
        nc_folder = None
        _cloud_sync_retry_pending = False
    # The late credential/config fetch above is often the first place a lite
    # attach receives its binding generation. Retain the final value even when
    # no coordinator is built, so an omitted/degraded payload cannot hide a
    # pending generation row from the turn-start fail-closed check.
    _session.cloud_sync_workspace_generation = attached_workspace_generation
    # Back-compat: translate a bare nc_session_folder into the new schema.
    # F-C1: gated on `not protected_cloud` too (defense-in-depth — nc_folder
    # is already forced None above for a protected thread, but this keeps
    # the invariant explicit at the point the shim actually fires).
    if not cloud_mount_active and not protected_cloud and not cloud_cfg and nc_folder:
        cloud_cfg = _legacy_nc_cloud_cfg(nc_folder)
    if cloud_cfg:
        try:
            _session.workspace_sync = _build_sync_coordinator(
                workspace_path=_session.workspace_manager.path,
                workspace_backend=_session.workspace_manager.backend,
                cloud_cfg=cloud_cfg,
                thread_id=str(_thread_id or ""),
                workspace_generation=attached_workspace_generation,
            )
            if _session.workspace_sync is None:
                raise RuntimeError("cloud sync payload resolved no usable mounts")
            if _session.workspace_sync:
                # Phase 1 of cloud_collaboration_model.md: turn-boundary sync,
                # not background polling. Do one blocking initial pull to
                # seed the workspace with current cloud-side contents before
                # the agent starts its first turn — and raise immediately if
                # any mount is broken, so the operator sees it before any
                # actual work is committed.
                #
                # Stateless executor: SKIP this pull. Every claimed turn runs
                # the same full pull at turn start (_run_persistent_turn's
                # turn-boundary sync) seconds after this attach, so the
                # attach-time pull is a duplicate full-mount walk on the
                # claim's critical path (measured 41s of the 49s attach,
                # 2026-08-08 baseline). Broken-mount surfacing moves to the
                # turn's _resilient_cloud_sync path, which broadcasts
                # workspace_sync.error and flags degradation — same operator
                # visibility, one walk instead of two.
                if _stateless_mode():
                    logger.info(
                        "attach step: initial cloud pull_all skipped "
                        "(stateless — turn-start pull covers seeding)"
                    )
                else:
                    _t_step = time.perf_counter()
                    await _session.workspace_sync.pull_all()
                    logger.info(
                        "attach step: initial cloud pull_all %.2fs",
                        time.perf_counter() - _t_step,
                    )
                logger.info(
                    "Cloud workspace sync coordinator started (%d mount(s))",
                    len(_session.workspace_sync),
                )
        except Exception as e:
            # The coordinator build or initial pull failed. Historically this
            # was swallowed to a warning and the session then ran unsynced for
            # its entire life with no signal — the exact mechanism behind the
            # prod-private "files didn't clone, but I saw no error" incident
            # (knowledge-base/knowledge/issues/main_cloud.md Issue 13). Surface it to the cockpit
            # over the same workspace_sync.error channel the turn-loop uses
            # (_resilient_cloud_sync), so the operator sees a degraded-sync
            # state instead of silence.
            logger.warning(f"Failed to start cloud workspace sync: {e}")
            _broadcast(
                "workspace_sync.error",
                {
                    "op": "initial_pull",
                    "turn_id": 0,
                    "message": str(e),
                    "degraded": True,
                },
            )
            _session.workspace_sync = None
            _cloud_sync_retry_pending = True
    elif cloud_degraded_hint:
        # Cloud is up but the orchestrator resolved no sync target for this
        # thread (session-folder provisioning failed upstream, so nc_session_folder
        # and the project mounts are all empty). Surface the same degraded-sync
        # state the failed-initial-pull path uses, instead of running silently
        # unsynced for the session's whole life (knowledge-base/knowledge/issues/main_cloud.md Issue 13).
        logger.warning(
            "Thread %s: main cloud is up but no sync target resolved — "
            "session will run unsynced.",
            _thread_id,
        )
        _broadcast(
            "workspace_sync.error",
            {
                "op": "provision",
                "turn_id": 0,
                "message": "Cloud sync could not be set up for this session "
                "(no sync target was provisioned).",
                "degraded": True,
            },
        )
        _cloud_sync_retry_pending = True

    # Restore message history from DB (for session resume)
    _t_step = time.perf_counter()
    await _restore_session_messages()
    logger.info("attach step: message restore %.2fs", time.perf_counter() - _t_step)

    # Mark thread as active. Stateless attach is an authorization boundary:
    # End may have fenced the queue after claim-bundle returned, so a failed
    # exact-lease CAS must abort before loop/tool admission.
    if not await _update_thread_status("active"):
        raise LeaseLostError("stateless attach lost lifecycle authority")

    # Initialize headless loop primitives. These survive WS reconnect so that
    # the loop can keep reading input / responding to interrupts across
    # transport churn. Cleared in _terminate_session.
    global _loop_user_queue, _loop_interrupt_flag, _loop_interrupt_target_turn_id
    global _loop_last_user_content
    global _hard_interrupt_event, _input_runtime_generation
    global _input_delivery_reclaim_lock
    # Keep readiness closed until durable child recovery has completely
    # converged.  Publishing the queue earlier lets a concurrent status/input
    # request start the provider between two orphan reconciliations.
    _loop_user_queue = None
    _loop_interrupt_flag = None
    _loop_interrupt_target_turn_id = None
    _hard_interrupt_event = asyncio.Event()
    _loop_last_user_content = [""]
    _input_runtime_generation = str(uuid4())
    _input_delivery_reclaim_lock = asyncio.Lock()
    _queued_input_claims.clear()

    # Child generations survive their parent process. Reconcile predecessors
    # under this exact authority before any provider can become ready;
    # recovered background evidence joins the durable-input reclaim below.
    await _session.recover_subagents()
    _loop_user_queue = asyncio.Queue()

    # Publish mount state only after the authoritative active CAS and queue
    # barrier.  An End racing message/repository restore must not observe a
    # misleading ready event from a runtime that is about to roll back.
    if cloud_mount_active:
        _broadcast(
            "cloud_mount.ready",
            {
                "mounts": [
                    {
                        "mount_id": m.mount_id,
                        "mount_kind": m.mount_kind,
                        "target_path": m.target_path,
                        "workspace_name": m.workspace_name,
                    }
                    for m in _session.cloud_mount_manager.mounts
                ]
            },
        )
    elif _session.cloud_mount_error:
        _broadcast(
            "cloud_mount.error",
            {"message": _session.cloud_mount_error, "degraded": True},
        )

    # Restore deliberately excludes persisted-but-unadmitted delivery rows:
    # they are executable inbox work, not passive conversation context. Claim
    # and queue them after the exact reciprocal binding is active.
    # Pinned input deliveries are owned by the reciprocal thread/agent/pod
    # binding.  A stateless turn is instead owned by its run_queue lease and
    # deliberately has no registered agent row; trying to enter the pinned
    # reclaimer here makes every pooled attach fail after all of its durable
    # setup has already completed.  The turn executor reads the stateless
    # inbox through input_seq/consumed_seq after this attach returns.
    if not _stateless_mode():
        await _reclaim_pending_pinned_inputs()

    # Start self-cleanup watchdogs (PR 2): exit on boot-WS timeout or
    # out-of-band thread.status='ended'. Cancelled by _terminate_session.
    _start_watchdogs()

    # Officer boot self-wake (centurion.md §4): the loop starts LAZILY on
    # first input / WS attach, so a freshly booted or respawned officer would
    # otherwise park forever with restored history and no running loop. This
    # wake IS the bootstrap, and it makes any durable notices restored above
    # readable in the very first turn. Gated on the loop not already running:
    # a re-attach (e.g. a retried /session/attach POST) must not inject a
    # second boot wake — the k3d smoke produced exactly that duplicate.
    # Stateless executor pods never self-wake: turns run only under a
    # run_queue lease (officer threads stay on the pinned lane in S1).
    if (
        _officer_cfg() is not None
        and not _stateless_mode()
        and (_loop_task is None or _loop_task.done())
    ):
        _ensure_persistent_loop_started("officer_boot")
        await _accept_user_input(
            "[wake: session started/restarted] You are the project officer "
            "coming back online after a start or restart. Reorient from your "
            "charter and knowledge base; recent orchestrator notices (if any) "
            "are in your history above. A fresh sitrep arrives with the next "
            "orchestrator wake. If nothing needs you now, file a sleep.",
            role="event",
        )

    logger.info(f"Session attached: thread={_thread_id} events_epoch={_events_epoch}")


async def _attach_session(
    thread_id: str,
    config_override: Optional[Dict[str, Any]] = None,
    resolved_config: Optional[Dict[str, Any]] = None,
    project_ids: Optional[List[str]] = None,
    datasources: Optional[List[Dict[str, Any]]] = None,
    config_name: Optional[str] = None,
    runtime_actor: Optional[Dict[str, Any]] = None,
    pinned_status_identity_contract: Any = None,
    pinned_runtime_generation_contract: Any = None,
    session_runtime_generation: Any = None,
    session_runtime_attach_token: Any = None,
    conversation_revision: Any = None,
    events_epoch: Any = None,
    workspace_generation: Any = _ATTACH_WORKSPACE_IDENTITY_UNSET,
    workspace_runtime_incarnation: Any = _ATTACH_WORKSPACE_IDENTITY_UNSET,
) -> None:
    """Exception-safe attach transaction around the full construction tail."""

    previous_thread_id = _thread_id
    try:
        await _attach_session_inner(
            thread_id=thread_id,
            config_override=config_override,
            resolved_config=resolved_config,
            project_ids=project_ids,
            datasources=datasources,
            config_name=config_name,
            runtime_actor=runtime_actor,
            pinned_status_identity_contract=pinned_status_identity_contract,
            pinned_runtime_generation_contract=pinned_runtime_generation_contract,
            session_runtime_generation=session_runtime_generation,
            session_runtime_attach_token=session_runtime_attach_token,
            conversation_revision=conversation_revision,
            events_epoch=events_epoch,
            workspace_generation=workspace_generation,
            workspace_runtime_incarnation=workspace_runtime_incarnation,
        )
    except BaseException:
        # Covers every post-construction await, including event-journal setup,
        # repository/message restore, lifecycle CAS, and input reclamation.
        # The helper is idempotent when the inner setup guard already ran.
        exact_identity = (
            _session_runtime_generation,
            _session_runtime_attach_token,
        )
        retained_receipt = _failed_attach_release_receipt
        exact_receipt_exists = bool(
            isinstance(retained_receipt, dict)
            and retained_receipt.get("thread_id") == thread_id
            and retained_receipt.get("session_runtime_generation") == exact_identity[0]
            and retained_receipt.get("session_runtime_attach_token")
            == exact_identity[1]
        )
        if (
            _session is not None
            or _thread_id != previous_thread_id
            or (
                _pinned_runtime_generation_enabled
                and all(exact_identity)
                and not exact_receipt_exists
            )
        ):
            await _cleanup_failed_attach_until_proven(
                thread_id, restore_thread_id=previous_thread_id
            )
        raise


async def _terminate_session(
    reason: str,
    *,
    mark_thread: bool = True,
    preserve_shell: Optional[bool] = None,
    preserve_workspace_daemons: bool = False,
) -> None:
    """Tear down the current session and return to idle.

    Called by:
      - WS-handler finally block? NO — under headless semantics WS close only
        unsubscribes; the loop survives. WS close never calls this.
      - Out-of-band lifecycle: drain intent, boot-WS timeout, thread-status
        watchdog, REST /session/detach, process shutdown, MAX_SESSIONS sweep.
      - The persistent loop's own completion handler (idle timeout, crash,
        clean /done exit) routes here via _loop_completion_handler.

    Re-entrancy: cancelling the loop task makes run_persistent_loop return
    CLEANLY (it swallows CancelledError in the input wait), so the loop's
    completion handler re-enters this function with reason="loop_complete"
    while the out-of-band teardown is still running. The _terminating guard
    makes that inner call a no-op — load-bearing for drain-suspend, where
    the inner call's 'ended' write would defeat the orchestrator's
    'suspended' transition.

    Steps:
      1. Cancel in-flight persistent-loop task (prevents permission_check race
         that the commit 3a1d265 race-fix protects against).
      2. Mark thread as ended (still resumable — `ended` is the only inactive
         state). Skipped when ``mark_thread=False`` — the drain-suspend path
         uses that to keep status authority with the orchestrator, which
         flips the thread to 'suspended' instead.
      3. Git commit + push.
      4. Clean up session resources. ``preserve_shell`` is an independent
         ownership disposition: true for a claim/pod handoff, false for a
         genuine thread end. When omitted it follows ``not mark_thread`` for
         back-compat, but losing an exact pinned binding always forces preserve.
         ``preserve_workspace_daemons`` is narrower still: only the stateless
         physical-claim handoff leaves workspace-side rclone/overlay processes
         resident while retiring their agent-local controllers.
      5. Clear session globals AND headless input primitives + subscribers.
      6. Increment session counter, exit if max reached.

    `reason` is logged and stored for observability — e.g. "drain",
    "idle_timeout", "loop_crash", "loop_complete", "shutdown", "rest_detach",
    "thread_ended_oob", "boot_ws_timeout", "legacy".
    """
    global _terminating, _termination_task
    active = _termination_task
    if active is not None and not active.done():
        if asyncio.current_task() is _loop_task:
            # The active owner cancels and awaits this loop task. Awaiting the
            # owner here would form a cycle; this is the one safe no-op
            # re-entry. Every independent release/complete caller waits below.
            logger.debug("Terminate(%s) re-entered from the loop being joined", reason)
            return
        await asyncio.shield(active)
        return
    if not _session:
        return
    termination_session = _session
    termination_identity = _attached_retirement_identity()

    async def _run() -> None:
        global _terminating, _termination_task
        _terminating = True
        try:
            retry_attempt = 0
            while True:
                try:
                    await _terminate_session_inner(
                        reason,
                        mark_thread=mark_thread,
                        preserve_shell=preserve_shell,
                        preserve_workspace_daemons=preserve_workspace_daemons,
                    )
                    if reason in {
                        "boot_ws_timeout",
                        "thread_ended_oob",
                        "thread_retirement_authorized",
                    }:
                        # These callers are watchdog tasks. The common teardown
                        # cancels/joins them while this child remains shielded,
                        # so only the surviving exact owner can schedule exit.
                        _schedule_exit(delay=1.0)
                    return
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    retry_identity = _retirement_admission_identity
                    retryable_exact_retirement = bool(
                        _pinned_runtime_generation_enabled
                        and termination_identity is not None
                        and _session is termination_session
                        and _attached_retirement_identity() == termination_identity
                        and (
                            retry_identity == termination_identity
                            or (mark_thread and retry_identity is None)
                        )
                    )
                    if not retryable_exact_retirement:
                        raise
                    delay = _EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS[
                        min(
                            retry_attempt + 1,
                            len(_EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS) - 1,
                        )
                    ]
                    retry_attempt += 1
                    logger.warning(
                        "Exact local retirement quiescence failed; retaining "
                        "the nonclaimable owner and retrying (thread=%s type=%s)",
                        termination_identity[0],
                        type(exc).__name__,
                    )
                    if delay:
                        await asyncio.sleep(delay)
        finally:
            _terminating = False
            if _termination_task is asyncio.current_task():
                _termination_task = None

    task = asyncio.create_task(
        _run(),
        name=f"session-terminate-{str(_thread_id or 'detached')[:12]}",
    )
    _termination_task = task
    try:
        await asyncio.shield(task)
    except asyncio.CancelledError:
        # Teardown continues as the single owner. Propagate caller
        # cancellation without publishing a false completion signal.
        raise


_EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS = (0.0, 0.25, 1.0, 3.0)


async def _settle_exact_retirement_after_quiescence(
    *,
    pinned_agent_id: str | None,
    retirement_disposition: str,
    retirement_permanent: bool,
    expected_identity: tuple[str, str | None, str | None] | None,
) -> bool:
    """Retry/reconcile only the immutable final ACK, never local cleanup.

    The orchestrator may durably settle and then lose the HTTP 200. Replaying
    the same G/attach/T/disposition/permanent/proof tuple is idempotent and a
    settled-or-superseded 200 is authoritative.  The tracked common
    termination task remains the retry owner with a capped backoff after the
    short fast-retry window.  It never re-enters shell/mount/session cleanup.
    A read of the append-only exact outcome ledger closes the masked-response
    case without inferring success from a generic 409 or a successor life.
    """

    exact_generation = expected_identity[1] if expected_identity else None
    exact_attach_token = expected_identity[2] if expected_identity else None
    exact_retirement_token = _retirement_admission_token
    exact_contract = bool(
        _pinned_runtime_generation_enabled
        and pinned_agent_id
        and exact_generation
        and exact_attach_token
        and exact_retirement_token
    )
    attempt = 0
    while True:
        delay = _EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS[
            min(attempt, len(_EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS) - 1)
        ]
        if delay:
            await asyncio.sleep(delay)
        if _attached_retirement_identity() != expected_identity:
            return False
        try:
            settled = await _update_thread_status(
                "ended",
                pinned_agent_id=pinned_agent_id,
                retirement_disposition=retirement_disposition,
                retirement_permanent=retirement_permanent,
            )
        except Exception as exc:
            logger.warning(
                "Exact retirement settlement attempt failed (thread=%s type=%s)",
                expected_identity[0] if expected_identity else _thread_id,
                type(exc).__name__,
            )
            settled = False
        if settled:
            return True
        if not exact_contract:
            return False
        outcome_reader = getattr(
            _orchestrator_client, "get_thread_retirement_outcome", None
        )
        if callable(outcome_reader):
            try:
                outcome = await outcome_reader(
                    expected_identity[0],
                    pinned_agent_id=pinned_agent_id,
                    session_runtime_generation=exact_generation,
                    session_runtime_attach_token=exact_attach_token,
                    session_runtime_retirement_token=exact_retirement_token,
                    retirement_disposition=retirement_disposition,
                    retirement_permanent=retirement_permanent,
                )
            except Exception as exc:
                logger.warning(
                    "Exact retirement outcome reconciliation failed "
                    "(thread=%s type=%s)",
                    expected_identity[0],
                    type(exc).__name__,
                )
                outcome = None
            if (
                isinstance(outcome, dict)
                and outcome.get("status") == "settled_or_superseded"
                and outcome.get("outcome") in {"settled", "deleted"}
                and outcome.get("retirement_disposition") == retirement_disposition
                and outcome.get("retirement_permanent") is retirement_permanent
            ):
                return True
        attempt += 1


async def _terminate_session_inner(
    reason: str,
    *,
    mark_thread: bool = True,
    preserve_shell: Optional[bool] = None,
    preserve_workspace_daemons: bool = False,
) -> None:
    """Body of _terminate_session — only reached holding the _terminating guard."""
    global _session, _thread_id, _sessions_served, _loop_task
    global _loop_user_queue, _loop_interrupt_flag, _loop_interrupt_target_turn_id
    global _loop_last_user_content
    global _hard_interrupt_event
    global _events_epoch, _next_seq, _tool_inflight, _turn_event_open
    global _turn_tool_execution_identity, _turn_tool_execution_external_hook
    global _event_writer, _cloud_sync_retry_pending, _draft_title_value
    global _active_permission_request_id
    global _input_runtime_generation
    global _runtime_authorization_admission_open
    global _pinned_status_identity_enabled

    if not _session:
        return

    thread_id = _thread_id
    runtime_generation = _session_runtime_generation
    runtime_attach_token = _session_runtime_attach_token
    retirement_disposition = _terminal_retirement_disposition()
    retirement_permanent = (
        _retirement_admission_permanent
        if _retirement_admission_identity == _attached_retirement_identity()
        and _retirement_admission_permanent is not None
        else False
    )
    preserve_remote_shell = (
        not mark_thread if preserve_shell is None else preserve_shell
    )
    logger.info(f"Terminating session: thread={thread_id} reason={reason}")

    pinned_control_owner = (
        None
        if _stateless_mode()
        else (_control_owner_agent_id or _registered_pinned_agent_id())
    )
    if mark_thread and not _stateless_mode():
        # Linearize retirement before *any* local teardown effect. Besides
        # explicit/idle archive, this common path owns loop crash/complete,
        # shutdown, watchdog and REST detach. Cancelling the loop, stopping
        # transports or cleaning mounts before the durable ``ending`` fence
        # would leave the row preparable while teardown was already in flight.
        # Repeating the exact generation/attach-token transition is
        # intentionally idempotent and reuses the orchestrator's pending
        # retirement authority.
        if not await _begin_exact_session_retirement(
            pinned_agent_id=pinned_control_owner,
            retirement_disposition=retirement_disposition,
            retirement_permanent=retirement_permanent,
            # Common termination owns a dedicated retry task and may run after
            # the turn loop has already completed/crashed. Never reopen input
            # or controls into a consumerless runtime between Begin retries.
            reopen_controls_if_uncommitted=False,
        ):
            raise EventJournalUnavailable(
                f"cannot begin exact thread retirement before teardown: {thread_id}"
            )

    # Cancel in-flight loop_task FIRST. Out-of-band callers (heartbeat-intent
    # drain, thread-status watchdog) reach this without going through the
    # loop's normal exit path, so without this the loop's next
    # _session.permission_mode access AttributeErrors when we null _session
    # below. Skipped when invoked from inside the loop itself (e.g. via
    # _loop_completion_handler's cleanup, which would deadlock awaiting self).
    loop_task = _loop_task
    if loop_task is not None and loop_task is not asyncio.current_task():
        if not loop_task.done():
            loop_task.cancel()
            try:
                await loop_task
            except (asyncio.CancelledError, Exception):
                pass
    _loop_task = None

    # Cancel and join self-cleanup watchdogs first — merely dropping their
    # task references would let a delayed cancellation/finally block overlap
    # the remote stage/delete/settlement actuator below.
    await _stop_and_join_watchdogs()

    # Close public admission before stopping a pinned owner. The dedicated
    # gate is needed for drain-suspend: the general agent status route forbids
    # writing ``suspended``, and marking ``ended`` would prevent the snapshot
    # transition that follows teardown. The thread-row update serializes with
    # admission; one last exact-owner drain then consumes every request that
    # committed before closure. A lost binding means a successor owns that
    # work, so this runtime must not adopt it.
    admission_closed = True
    if pinned_control_owner is not None and not _retirement_admission_closed():
        admission_closed = await _close_pinned_control_inbox(
            agent_id=pinned_control_owner
        )
        if not admission_closed:
            # The reciprocal binding is the pinned owner's resource fence. A
            # stale pod that lost it may close only its own transports; the
            # successor can already be using the deterministic remote tmux.
            preserve_remote_shell = True
            logger.info(
                "Pinned control admission close skipped: exact binding moved "
                "(thread=%s agent=%s)",
                thread_id,
                pinned_control_owner,
            )

    # Retire this thread's announced permission rows, then drop the ledger.
    # The turn-end sweep in _loop_on_turn_complete is the usual owner, but the
    # cancel above skips it. The helper holds the exact queue lease or pinned
    # reciprocal binding through each irreversible UPDATE, so a binding move
    # after admission closure cannot let this stale runtime touch successor
    # rows.
    _gates_in_flight.clear()
    _active_permission_request_id = None
    await _retire_announced_permission_rows(f"session terminated ({reason})")
    _announced_permission_rows.clear()

    # A pinned consumer owns the attach lifetime; a stateless consumer owns
    # the active lease. In both cases it must be fully stopped before the
    # journal writer drains or ownership is released.
    await _stop_thread_interrupt_watcher()
    await _stop_thread_control_watcher()

    # Join process-global side tasks while the captured session/thread identity
    # and event writer are still authoritative.  A delayed title or protected
    # cloud ping must never observe the next pool attachment.
    await _quiesce_session_side_tasks()

    # B11: final memory capture for ALL pinned terminate reasons — the ✕-button
    # detach (and drain, watchdog, shutdown, …) historically skipped
    # extraction entirely. Stateless turns instead mint one durable per-turn
    # obligation and must never run this full-history writer as a duplicate.
    # Manager-mode only; the flag-off pinned path keeps today's (skipping)
    # behaviour. The guard flag stops a re-extraction when
    # _handle_archive/_handle_idle_archive already captured. Must run before
    # _session.cleanup() tears down the stores; contained like the sibling
    # teardown steps — a memory failure must never skip cleanup.
    if (
        not _stateless_mode()
        and _session.memory_service is not None
        and not _session.final_memory_extracted
        and _session.messages
        and not (_session.shell_owner_token is not None and not mark_thread)
        and not _termination_admission_closed()
    ):
        try:
            from agent.services.memory import CaptureEvent

            await _session.memory_service.capture(
                CaptureEvent(kind="session_end", messages=_session.messages)
            )
            _session.final_memory_extracted = True
            logger.info("Terminate(%s): final memory capture complete", reason)
        except Exception as e:
            logger.warning(f"Terminate memory capture failed (non-fatal): {e}")

    # capture_nowait(pre_compaction) and asynchronous citation verification
    # both carry session-scoped write/callback authority. Disarm and join them
    # before the journal closes and before a queue claimant can be released.
    try:
        quiesce_result = _session.quiesce_background_tasks()
        if inspect.isawaitable(quiesce_result):
            await quiesce_result
        elif isinstance(_session, PersistentSession):
            raise RuntimeError("PersistentSession RAM quiescence is not awaitable")
    except Exception as exc:
        if not _stateless_mode():
            raise EventJournalUnavailable(
                "pinned session background work did not quiesce"
            ) from exc
        if _session.shell_owner_token is not None:
            raise
        logger.warning(
            "Pinned session background-task quiescence failed (contained)",
            exc_info=True,
        )

    # Final cloud sync + drop secrets. No more background polling to stop:
    # Phase 1 moved sync to turn boundaries via the coordinator. The last
    # turn's background push must land first — never two concurrent walks of
    # one mount, and never an aclose under an in-flight push.
    if _session.workspace_sync:
        try:
            await _await_pending_cloud_push()
            # Stateless bytes are committed only by the armed generation task
            # above. A second raw push here would have no durable requirement
            # or acknowledgement and, on lease-loss teardown, could overlap a
            # successor's pull. Pinned teardown keeps its existing final
            # push+pull byte-for-byte.
            if not _stateless_mode():
                await _session.workspace_sync.push_all()
                await _session.workspace_sync.pull_all()
        except Exception as e:
            logger.warning(f"Final cloud sync failed (non-fatal): {e}")
        if _background_push_owns(_session.workspace_sync):
            # Step 4a: a handed-off push still transmits through this
            # coordinator; its done-callback closes it. Closing here would
            # yank the WebDAV client from under the off-slot transmit.
            logger.info(
                "cloud sync coordinator left open for the handed-off push (thread %s)",
                thread_id,
            )
        else:
            try:
                await _session.workspace_sync.aclose()
            except Exception as e:
                logger.debug(f"Cloud sync aclose failed (non-fatal): {e}")

    # Final git commit + push
    if _session.workspace_manager:
        git_mgr = getattr(_session.workspace_manager, "git_manager", None)
        if git_mgr and git_mgr.is_active:
            try:
                if git_mgr.has_uncommitted_changes():
                    git_mgr.commit(f"Session detach: thread {thread_id}")
                git_mgr.push()
            except Exception as e:
                logger.warning(f"Final git push failed (non-fatal): {e}")

    # The journal owns a captured pool + thread identity. Drain it while both
    # the session and live subscribers still exist: terminal Canvas failures
    # can then emit their direct reconciliation control before teardown clears
    # either registry, and a pool-mode reattach cannot inherit queued events.
    event_writer = _event_writer
    if event_writer is not None:
        try:
            await event_writer.close()
        except Exception as e:
            if not _stateless_mode():
                # A pinned End is not allowed to stage/delete/settle while an
                # ordinary G-scoped journal batch may still be in flight.
                # Preserve the writer and exact local retirement fence so the
                # same authority can retry close; do not publish a false
                # terminal lifecycle edge.
                raise EventJournalUnavailable(
                    "pinned thread event writer did not quiesce"
                ) from e
            logger.warning(
                "thread_events writer close failed (thread=%s): %s",
                thread_id,
                e,
            )
        else:
            _event_writer = None

    # Shell ownership is deliberately separate from thread-status authority.
    # Claim switches preserve by explicit/default disposition; a stale pinned
    # owner that lost its reciprocal binding is forced to preserve above.
    # This is deliberately after final Git: GitManager itself delegates through
    # the remote shell. From here onward cleanup may mutate mount transports but
    # no new tool/shell command is admitted.
    _session.retire_shell_owner()
    await _session.cleanup(
        preserve_shell=preserve_remote_shell,
        preserve_workspace_daemons=preserve_workspace_daemons,
    )

    if mark_thread and not await _settle_exact_retirement_after_quiescence(
        pinned_agent_id=pinned_control_owner,
        retirement_disposition=retirement_disposition,
        retirement_permanent=retirement_permanent,
        expected_identity=(
            str(thread_id),
            runtime_generation,
            runtime_attach_token,
        ),
    ):
        # Keep the exact local retirement mirror + captured session identity
        # intact. The durable retirement reconciler may settle the same proof;
        # no caller re-enters local cleanup, no false terminal frame is emitted,
        # and no broad DB fallback may reopen Resume while unresolved.
        raise EventJournalUnavailable(
            f"cannot durably settle thread lifecycle after local teardown: {thread_id}"
        )

    # Clear session state
    _session = None
    _thread_id = None
    _clear_attached_runtime_actor()

    # Clear headless input primitives + subscriber registry. The pump tasks
    # owned by each subscriber are cancelled by their ws_chat finally blocks
    # when those handlers notice the WS close; dropping the registry here
    # ensures stale entries don't accumulate across sessions.
    _loop_user_queue = None
    _loop_interrupt_flag = None
    _loop_interrupt_target_turn_id = None
    _hard_interrupt_event = None
    _loop_last_user_content = [""]
    _input_runtime_generation = None
    _runtime_authorization_admission_open = False
    _pinned_status_identity_enabled = False
    _clear_attached_runtime_identity(
        expected_generation=runtime_generation,
        expected_attach_token=runtime_attach_token,
    )
    _queued_input_claims.clear()
    _draft_title_value = None
    _clear_all_canvas_awareness()
    _subscribers.clear()

    # Phase 2 event-log cursor reset. The next session attach reads the
    # epoch fresh from the threads table. The ordered writer was already
    # drained and cleared above, before either captured identity disappeared.
    _events_epoch = 0
    _next_seq = 0
    _tool_inflight = False
    _turn_tool_execution_identity = None
    _turn_tool_execution_external_hook = None
    _turn_event_open = False
    # Pool agents serve many threads; a pending retry must not leak into the
    # next session, whose attach resolves its own cloud state.
    _cloud_sync_retry_pending = False

    # Safety valve: restart after N sessions to guard against state leakage
    _sessions_served += 1
    if _max_sessions_per_process > 0 and _sessions_served >= _max_sessions_per_process:
        logger.info(
            f"Max sessions per process reached ({_sessions_served}/{_max_sessions_per_process}). "
            "Exiting — Docker will restart the container."
        )
        import sys

        sys.exit(0)

    logger.info(
        f"Session terminated: thread={thread_id} "
        f"reason={reason} (sessions served: {_sessions_served})"
    )


async def _detach_session() -> None:
    """Back-compat shim. Prefer _terminate_session(reason) at new call sites.

    Kept so existing tests patching `_detach_session` continue to work and so
    code paths not yet updated don't break. Logs at DEBUG so each invocation
    is traceable.
    """
    logger.debug("_detach_session() called via back-compat shim")
    await _terminate_session("legacy")


def _runtime_actor_context_for_attach(
    payload: Optional[Dict[str, Any]],
) -> RuntimeActorContext | None:
    """Resolve one actor object shared by maintenance and every session tool."""

    actor = RuntimeActorContext.from_payload(payload)
    if payload is not None and actor is None:
        raise RuntimeError("Malformed server-derived runtime actor context")
    client = _orchestrator_client
    if actor is None and client is not None:
        # Dedicated runtime clients receive the actor during registration.
        actor = getattr(client, "runtime_actor", None)
    elif actor is not None and client is not None:
        # Pool/stateless attach receives its actor in the server payload. The
        # heartbeat maintenance channel and the session/tool bindings must
        # share this exact mutable object so a rotation cannot leave tools on
        # the predecessor bearer.
        adopt = getattr(client, "adopt_runtime_actor", None)
        if callable(adopt):
            adopt(actor)
        else:  # deliberately tiny dry-run/test adapters
            client.runtime_actor = actor
    return actor


def _clear_attached_runtime_actor() -> None:
    """Drop project authority at the common session teardown boundary."""

    client = _orchestrator_client
    clear = getattr(client, "clear_runtime_actor", None) if client else None
    if callable(clear):
        clear()


async def _run_pool_attach_transaction(
    thread_id: str,
    attach: Dict[str, Any],
    runtime_generation: str | None,
    attach_token: str | None,
) -> None:
    """Finish one synchronously claimed pool attach in the background.

    ``_attach_session`` owns rollback of every process-global/session resource.
    This wrapper owns only the admission claim and the exact orchestrator
    thread↔agent reservation.  A failed attach releases that reservation once;
    a successful attach leaves it in place for the live session.
    """

    global _pool_attach_claim, _pool_attach_task
    global _pool_attach_runtime_generation, _pool_attach_token

    succeeded = False
    release_confirmed = False
    try:
        await _attach_session(thread_id=thread_id, **attach)
        succeeded = True
        logger.info("Pool session setup complete for thread %s", thread_id)
    except asyncio.CancelledError:
        logger.info("Pool session setup cancelled for thread %s", thread_id)
        raise
    except BaseException as exc:
        # Do not echo an arbitrary workspace/config exception: internal
        # payloads may carry credential material.  The attach transaction logs
        # its own bounded diagnostics at the failing boundary.
        logger.error(
            "Pool session setup failed for thread %s (%s)",
            thread_id,
            type(exc).__name__,
        )
    finally:
        if not succeeded and _orchestrator_client is not None:
            try:
                # The server rotates G only after this exact delivered attach
                # proves every local/workspace writer zero.  Missing or stale
                # receipts deliberately retain the process-local claim.
                release_confirmed = (
                    await _release_failed_attach_receipt_until_confirmed(
                        thread_id,
                        runtime_generation=runtime_generation,
                        runtime_attach_token=attach_token,
                    )
                )
            except BaseException as exc:
                logger.warning(
                    "Failed to release exact pool binding for thread %s (%s)",
                    thread_id,
                    type(exc).__name__,
                )
        async with _pool_attach_lock:
            if succeeded or release_confirmed:
                if (
                    _pool_attach_claim == thread_id
                    and _pool_attach_runtime_generation == runtime_generation
                    and _pool_attach_token == attach_token
                ):
                    _pool_attach_claim = None
                    _pool_attach_runtime_generation = None
                    _pool_attach_token = None
                if _pool_attach_task is asyncio.current_task():
                    _pool_attach_task = None
            elif (
                _pool_attach_claim == thread_id
                and _pool_attach_runtime_generation == runtime_generation
                and _pool_attach_token == attach_token
            ):
                # Deliberately retain the claim.  The exact reservation could
                # not be proven released, so this process must remain
                # non-ready/nonclaimable until lifecycle reconciliation or
                # shutdown removes it.
                logger.error(
                    "Pool attach failure for thread %s retained its local "
                    "ownership fence after unconfirmed DB release",
                    thread_id,
                )


async def _admit_pool_session_attach(request: Dict[str, Any]) -> JSONResponse:
    """Claim an idle persistent process and schedule its heavy attach.

    The claim and task are installed before returning 200.  This is the
    persistent-pool equivalent of dual mode's ``PodState.SESSION`` latch and
    is the narrow callback-deadlock break for protected workspace polling.
    """

    global _pool_attach_claim, _pool_attach_task
    global _pool_attach_runtime_generation, _pool_attach_token

    thread_id = request.get("thread_id")
    if not isinstance(thread_id, str) or not thread_id:
        return JSONResponse({"error": "thread_id is required"}, status_code=400)
    recipient_refusal = _pinned_session_recipient_refusal(
        request,
        expected_thread_id=thread_id,
    )
    if recipient_refusal is not None:
        return recipient_refusal
    runtime_contract = _pinned_runtime_generation_advertised(request)
    generation_raw = request.get("session_runtime_generation")
    attach_token_raw = request.get("session_runtime_attach_token")
    runtime_generation = _canonical_runtime_generation(generation_raw)
    attach_token = _canonical_runtime_generation(attach_token_raw)
    if (
        (generation_raw is not None and runtime_generation is None)
        or (attach_token_raw is not None and attach_token is None)
        or (runtime_contract and (runtime_generation is None or attach_token is None))
    ):
        return JSONResponse(
            {"error": "exact session runtime identity is required"},
            status_code=409,
        )

    async with _pool_attach_lock:
        if (
            _session is not None
            or _pool_attach_claim is not None
            or _pending_drain_suspend is not None
        ):
            owner = (
                _thread_id
                or _pool_attach_claim
                or _pending_drain_suspend.get("thread_id")
            )
            return JSONResponse(
                {
                    "error": f"Already attached to thread {owner}",
                    "current_thread_id": owner,
                },
                status_code=409,
            )

        _pool_attach_claim = thread_id
        _pool_attach_runtime_generation = runtime_generation
        _pool_attach_token = attach_token
        attach = {
            "config_override": request.get("config_override"),
            "resolved_config": request.get("resolved_config"),
            "project_ids": request.get("project_ids"),
            "datasources": request.get("datasources"),
            "config_name": request.get("config_name"),
            "runtime_actor": request.get("runtime_actor"),
            "pinned_status_identity_contract": request.get(
                "pinned_status_identity_contract"
            ),
            "pinned_runtime_generation_contract": request.get(
                "pinned_runtime_generation_contract"
            ),
            "session_runtime_generation": runtime_generation,
            "session_runtime_attach_token": attach_token,
        }
        try:
            _adopt_attached_runtime_identity(
                runtime_generation,
                attach_token,
                contract_advertised=runtime_contract,
            )
            _pool_attach_task = asyncio.create_task(
                _run_pool_attach_transaction(
                    thread_id,
                    attach,
                    runtime_generation,
                    attach_token,
                ),
                name=f"pool-session-attach:{thread_id}",
            )
        except BaseException:
            _pool_attach_claim = None
            _pool_attach_runtime_generation = None
            _pool_attach_token = None
            _pool_attach_task = None
            _clear_attached_runtime_identity(
                expected_generation=runtime_generation,
                expected_attach_token=attach_token,
            )
            raise

    return JSONResponse({"status": "attaching", "thread_id": thread_id})


def create_persistent_app(config_path: str, thread_id: Optional[str] = None) -> FastAPI:
    """Create the persistent-mode FastAPI application.

    Args:
        config_path: Agent config name or path
        thread_id: Session thread UUID

    Returns:
        FastAPI app with WebSocket and health endpoints
    """
    global _config_path, _thread_id
    _config_path = config_path
    _thread_id = thread_id

    app = FastAPI(
        title="Persistent Agent API",
        description="Interactive persistent agent with WebSocket transport",
        version="1.0.0",
        lifespan=lifespan,
    )

    # --- Health endpoints (same pattern as worker) ---

    @app.get("/health")
    async def health():
        app_guide = _app_guide_health()
        return JSONResponse(
            {
                "status": (
                    "healthy" if app_guide.get("state") == "ready" else "degraded"
                ),
                "mode": "stateless" if _stateless_mode() else "persistent",
                "thread_id": _thread_id,
                "uptime_seconds": (datetime.now() - _started_at).total_seconds()
                if _started_at
                else 0,
                "app_guide": app_guide,
            }
        )

    @app.post("/api/lifecycle/termination-fence")
    async def termination_fence(request: Request):
        """Close admission and hold preStop at the exact parked boundary.

        This is transport-authenticated by loopback placement, not by a bearer:
        the preStop helper runs in the same container and no credential is
        available or necessary.  A caller-provided identity is never accepted.
        """

        host = request.client.host if request.client is not None else ""
        if host not in {"127.0.0.1", "::1", "localhost"}:
            return JSONResponse({"error": "loopback only"}, status_code=403)
        activate_termination_admission_fence("kubernetes_prestop")
        timeout_seconds = max(
            0.0,
            float(os.environ.get("PERSISTENT_TERMINATION_DRAIN_SECONDS", "165")),
        )
        parked = await _wait_for_termination_quiescence(timeout_seconds)
        if not parked:
            logger.error(
                "Persistent termination grace expired before the current turn "
                "settled; Kubernetes may force-stop it and LF-5 restore repair "
                "will own recovery"
            )
        return JSONResponse(
            {
                "fenced": True,
                "parked": parked,
                "retryable_deferred_input": True,
            }
        )

    @app.get("/ready")
    async def ready():
        if _stateless_mode():
            # A stateless executor is "ready" when its claim loop is alive —
            # there is no session/WS surface to gate on (k8s probes, M5).
            from agent.api.turn_executor import executor_running

            is_ready = executor_running()
            return JSONResponse(
                {
                    "ready": is_ready,
                    "mode": "stateless",
                    "thread_id": _thread_id,
                    "capabilities": {"durable_input_delivery": False},
                },
                status_code=200 if is_ready else 503,
            )
        is_ready = _session_ready()
        protected_ready = bool(
            is_ready
            and _session is not None
            and _session.protected_cloud_required
            and _session.protected_cloud_ready()
        )
        session_identity_fingerprint = _current_pinned_session_identity_fingerprint()
        capabilities: dict[str, Any] = {
            "durable_input_delivery": True,
            "pinned_session_identity_contract": (
                PINNED_SESSION_READY_IDENTITY_CONTRACT
            ),
            "protected_cloud_contract": 1
            if _session is not None and _session.protected_cloud_required
            else None,
            "protected_cloud_ready": protected_ready,
        }
        if _pinned_session_recipient_capable():
            capabilities["pinned_session_recipient_binding"] = True
        return JSONResponse(
            {
                "ready": is_ready,
                "mode": "persistent",
                "thread_id": _thread_id,
                "session_identity_fingerprint": session_identity_fingerprint,
                "capabilities": capabilities,
            },
            status_code=200 if is_ready else 503,
        )

    @app.get("/status")
    async def status():
        # Embedding-path health (B4): degraded == dimension mismatch latched.
        from shared.runtime.services.embedding_service import peek_embedding_service

        emb_service = peek_embedding_service()
        runtime_status = _agent.get_status() if _agent is not None else {}
        return JSONResponse(
            {
                "mode": "persistent",
                "thread_id": _thread_id,
                "turn_in_flight": _turn_in_flight(),
                "config": _config_path,
                "permission_mode": _session.permission_mode if _session else None,
                "turn_count": _session.turn_count if _session else 0,
                "message_count": len(_session.messages) if _session else 0,
                "research_providers": runtime_status.get("research_providers"),
                "tools": [t.name for t in _session.tools]
                if _session and _session.tools
                else [],
                "embedding": emb_service.health_snapshot()
                if emb_service is not None
                else None,
                "app_guide": _app_guide_health(),
            }
        )

    @app.post("/session/status")
    async def recipient_bound_session_status(request: dict = {}):
        """Report turn state only for this exact pinned runtime identity."""

        expected = _canonical_pinned_session_identity_fingerprint(
            request.get("session_identity_fingerprint")
            if isinstance(request, dict)
            else None
        )
        if (
            expected is None
            or _current_pinned_session_identity_fingerprint() != expected
        ):
            return JSONResponse(
                {"error": "session_identity_mismatch", "retryable": True},
                status_code=409,
            )
        return JSONResponse(
            {
                "ready": _session_ready(),
                "thread_id": _thread_id,
                "state": "session" if _session is not None else "idle",
                "turn_in_flight": _turn_in_flight(),
                "recipient_verified": True,
                "session_identity_fingerprint": expected,
            }
        )

    @app.get("/session/toolset")
    async def session_toolset():
        """The bound toolset, measured. Backs the orchestrator's tool-groups read.

        In-cluster only and unauthenticated, matching every other control route
        on this app — the orchestrator reaches it by pod IP and the ingress does
        not expose it. It leaks no config values, only tool names.
        """
        return JSONResponse(_session_toolset_report())

    # --- Session attach/detach (pool mode) ---

    @app.post("/session/attach")
    async def session_attach(request: dict = {}):
        """Attach this agent to a thread (Docker Compose pool mode).

        Called by the orchestrator when a user creates a persistent thread
        and this agent is available.  Creates a new PersistentSession.

        Body:
            thread_id (str): Thread UUID to attach to
            config_override (dict, optional): Config overrides from thread metadata
            project_ids (list[str], optional): Project IDs for scoping
            config_name (str, optional): Thread's config — used as the session
                base instead of this pod's boot config (pool pods boot as
                workers; see knowledge-base/knowledge/issues/session_config_name_plumbing.md)
        """
        if _stateless_mode():
            # The executor owns attach/detach on this pod — an out-of-band
            # attach would corrupt its session cache and run outside a lease.
            return _stateless_reject()
        return await _admit_pool_session_attach(request)

    @app.post("/session/detach")
    async def session_detach(request: dict = {}):
        """Detach from the current thread and return to idle pool.

        Called by the orchestrator when a thread ends, or by the agent
        itself on idle timeout.  Tears down the PersistentSession.
        """
        if _stateless_mode():
            # A detach mid-lease would kill a claimed turn out-of-band; the
            # executor detaches its own cached session between claims.
            return _stateless_reject()
        expected = _canonical_pinned_session_identity_fingerprint(
            request.get("session_identity_fingerprint")
            if isinstance(request, dict)
            else None
        )
        if (
            expected is None
            or _current_pinned_session_identity_fingerprint() != expected
        ):
            return JSONResponse(
                {"error": "session_identity_mismatch", "retryable": True},
                status_code=409,
            )
        if _session is None:
            return JSONResponse({"status": "already_idle", "thread_id": None})

        thread_id = _thread_id
        try:
            await _terminate_session("rest_detach")
            return JSONResponse(
                {
                    "status": "detached",
                    "thread_id": thread_id,
                    "sessions_served": _sessions_served,
                }
            )
        except Exception:
            error_ref = uuid4().hex[:12]
            logger.exception(
                "Failed to detach session for thread %s (error_ref=%s)",
                thread_id,
                error_ref,
            )
            return JSONResponse(
                {"error": "session detach failed", "error_ref": error_ref},
                status_code=500,
            )

    @app.post("/cloud-overlay/reset")
    async def cloud_overlay_reset(request: Request):
        """Discard the staged upperdir and remount fresh after the user
        Applies or Rejects a staged cloud diff (Task 10's orchestrator apply
        flow calls this).

        Reset is destructive and thread names/agent pod addresses are reused.
        Bind the request to the exact attached runtime and physical workspace
        that produced the reviewed overlay before touching upperdir. A delayed
        G1 Apply/Reject can therefore never reset a same-thread G2 overlay.
        """
        overlay = getattr(_session, "overlay_mount_manager", None)
        if _session is None or overlay is None:
            return JSONResponse({"error": "no cloud overlay"}, status_code=404)
        try:
            body = await request.json()
        except Exception:
            body = None
        expected_agent_id = _registered_pinned_agent_id()
        expected_generation = _session_runtime_generation
        expected_attach_token = _session_runtime_attach_token
        expected_workspace_generation = getattr(
            _session, "protected_workspace_generation", ""
        )
        expected_runtime_incarnation = getattr(
            _session, "protected_workspace_runtime_incarnation", ""
        )
        if (
            getattr(_session, "protected_cloud_required", None) is not True
            or not isinstance(body, dict)
            or not expected_agent_id
            or not expected_generation
            or not expected_attach_token
            or not expected_workspace_generation
            or not expected_runtime_incarnation
            or request.headers.get("x-agent-id") != expected_agent_id
            or request.headers.get("x-session-runtime-generation")
            != expected_generation
            or request.headers.get("x-session-runtime-attach-token")
            != expected_attach_token
            or body.get("thread_id") != _thread_id
            or body.get("workspace_generation") != expected_workspace_generation
            or body.get("workspace_runtime_incarnation") != expected_runtime_incarnation
        ):
            return JSONResponse(
                {"error": "stale cloud overlay reset authority"},
                status_code=409,
            )
        try:
            await asyncio.to_thread(_session.reset_cloud_overlay)
            return JSONResponse({"ok": True})
        except CloudOverlayUnavailable:
            # Precondition only (overlay exists but isn't active — mount
            # failed, or already torn down): 404 = give up, don't retry.
            error_ref = uuid4().hex[:12]
            logger.exception(
                "Cloud overlay unavailable for thread %s (error_ref=%s)",
                _thread_id,
                error_ref,
            )
            return JSONResponse(
                {"error": "cloud overlay unavailable", "error_ref": error_ref},
                status_code=404,
            )
        except Exception:
            # EVERYTHING else is a real failure and must surface as 500
            # (retry/alert). This includes OverlayMountError (remount/wipe
            # script) and RcloneMountError (vfs/refresh) — both subclass
            # RuntimeError, so no RuntimeError-shaped clause may sit above
            # this one or real failures get misreported as 404 give-up.
            error_ref = uuid4().hex[:12]
            logger.exception(
                "Failed to reset cloud overlay for thread %s (error_ref=%s)",
                _thread_id,
                error_ref,
            )
            return JSONResponse(
                {"error": "cloud overlay reset failed", "error_ref": error_ref},
                status_code=500,
            )

    # --- Headless REST input endpoints (phase 2) ---
    #
    # Counterparts to the WS-receive-loop methods, exposed so the orchestrator's
    # SSE-based clients (cockpit chunk 3, MCP, curl) can drive the session
    # without a WebSocket. The orchestrator forwards from
    # POST /api/threads/{id}/{input,interrupt,approve/{approval_id}}.

    @app.post("/api/input")
    async def api_input(request: Request):
        return await handle_api_input(request)

    @app.post("/api/interrupt")
    async def api_interrupt(request: Request):
        return await handle_api_interrupt(request)

    @app.post("/api/approve")
    async def api_approve(request: Request):
        return await handle_api_approve(request)

    # --- WebSocket endpoint ---

    @app.websocket("/ws/chat")
    async def ws_chat(ws: WebSocket):
        # Validate the session JWT carried as ?t={token}.
        if not await _validate_session_token(ws):
            return
        await handle_persistent_websocket(ws)

    # External path the per-session Ingress routes to (the cockpit dials
    # wss://api.<domain>/p/<thread_id>/ws?t=<jwt>). The {thread_id} path param
    # is unused — the bound thread is enforced by _validate_session_token
    # against SESSION_BOUND_THREAD_ID + the JWT's tid claim.
    @app.websocket("/p/{thread_id}/ws")
    async def ws_session(ws: WebSocket, thread_id: str):
        if not await _validate_session_token(ws):
            return
        await handle_persistent_websocket(ws)

    return app


# --- REST handlers (module-level so dual_app can call them too) ---
#
# Reached from both:
#   - persistent_app.create_persistent_app()'s /api/{input,interrupt,approve}
#     routes (pure persistent mode, agent.py --mode persistent).
#   - dual_app.create_dual_app() routes (dual mode — adds pod-state pre-check
#     then delegates here).
#
# Mirror of the /ws/chat consolidation; same rationale, see
# knowledge-base/knowledge/issues/persistent_session_dual_mode_phase1_gap.md.


class TerminationAdmissionClosed(RuntimeError):
    """Input reached the runtime after its termination fence closed."""


class DurableInputUnavailable(RuntimeError):
    """A retry-stable event could not establish its durable inbox row."""


class SessionIdentityMismatch(RuntimeError):
    """Input was addressed to a different pinned runtime incarnation."""


@dataclass(frozen=True, slots=True)
class AcceptedInput:
    message_id: str
    delivery_id: str
    delivery_state: str
    claim_generation: int
    enqueued: bool
    duplicate: bool = False
    deferred: bool = False


def _accepted_input_payload(admission: AcceptedInput) -> dict[str, Any]:
    """Serialize one durable input acknowledgement across REST and WS.

    Once the transcript+delivery transaction commits, the input belongs to
    the durable inbox.  In particular, ``deferred`` means the successor will
    reclaim it; telling an uncorrelated WebSocket client to retry would mint a
    second delivery identity and could buy a second turn.
    """

    return {
        "accepted": True,
        "message_id": admission.message_id,
        "duplicate": admission.duplicate,
        "deferred": admission.deferred,
        "retryable": False,
        "delivery_id": admission.delivery_id,
        "delivery_state": admission.delivery_state,
    }


def _pinned_input_runtime_identity() -> tuple[str, str, str, str]:
    agent_id = _registered_pinned_agent_id()
    pod_uid = str(os.environ.get("POD_UID") or "").strip()
    generation = str(_input_runtime_generation or "").strip()
    attach_token = str(_session_runtime_attach_token or "").strip()
    if agent_id is None or not pod_uid or not generation or not attach_token:
        raise DurableInputUnavailable
    return agent_id, pod_uid, generation, attach_token


def _session_subagent_parent_authority():
    """Snapshot the exact current pinned life or stateless turn lease."""

    from shared.session_subagent_authority import (
        SessionParentAuthority,
        SessionParentAuthorityRefused,
    )

    thread_id = str(_thread_id or "").strip()
    if not thread_id:
        raise SessionParentAuthorityRefused("parent_missing")
    lease = _current_lease_var.get()
    if lease is not None:
        if (
            not lease.active
            or lease.lost.is_set()
            or str(lease.unit_id or "") != thread_id
            or type(lease.lease_token) is not int
            or lease.lease_token <= 0
            or not isinstance(lease.executor_id, str)
            or not lease.executor_id
            or not isinstance(lease.pod_uid, str)
            or not lease.pod_uid
        ):
            raise SessionParentAuthorityRefused("stateless_parent_not_current")
        return SessionParentAuthority(
            execution_lane="stateless",
            parent_thread_id=thread_id,
            lease_token=lease.lease_token,
            executor_id=lease.executor_id,
            executor_pod_uid=lease.pod_uid,
        )

    agent_id = _registered_pinned_agent_id()
    pod_uid = str(os.environ.get("POD_UID") or "").strip()
    generation = str(_session_runtime_generation or "").strip()
    attach_token = str(_session_runtime_attach_token or "").strip()
    if not agent_id or not pod_uid or not generation or not attach_token:
        raise SessionParentAuthorityRefused("pinned_parent_not_current")
    return SessionParentAuthority(
        execution_lane="pinned",
        parent_thread_id=thread_id,
        agent_id=agent_id,
        pod_uid=pod_uid,
        session_runtime_generation=generation,
        runtime_attach_token=attach_token,
    )


async def _session_subagent_event_available(_message: str) -> None:
    """Low-latency wake after the terminal transaction queued role=event."""

    if _stateless_mode() or _session is None or _loop_user_queue is None:
        return
    await _reclaim_pending_pinned_inputs()


async def _loop_runtime_authority_current(
    *, allow_retirement_settlement: bool = False
) -> bool:
    """Fail closed unless this exact session life may start a new effect.

    A process-local retirement latch is fast but owner Force-End can authorize
    a durable retirement token between 60-second lifecycle watchdog polls.
    This exact DB proof is therefore awaited immediately before every provider
    invocation and real tool ``ainvoke``. Stateless turns need the same awaited
    proof: a reaper may rotate their queue lease before the mutable local lease
    handle observes its lost event.
    """

    def local_authority_open() -> bool:
        return bool(
            not _termination_admission_closed()
            and (allow_retirement_settlement or not _retirement_admission_closed())
        )

    if not local_authority_open() or not _protected_cloud_runtime_ready():
        return False
    if _stateless_mode():
        session = _session
        if session is None or session.postgres_conn is None:
            return False
        authority = None
        try:
            authority = _session_subagent_parent_authority()
            if authority.execution_lane != "stateless":
                return False
            current = await session.postgres_conn.session_parent_authority_current(
                authority
            )
        except Exception as exc:
            logger.warning(
                "Stateless runtime effect-authority proof failed (%s)",
                type(exc).__name__,
            )
            current = False
        try:
            authority_after = _session_subagent_parent_authority()
        except Exception:
            authority_after = None
        same_local_life = bool(
            session is _session
            and authority is not None
            and authority_after == authority
            and str(authority.parent_thread_id) == str(_thread_id or "")
        )
        if current is not True and same_local_life:
            # Wake the executor immediately when the DB rejects a lease whose
            # local identity has not already rotated. Never mark a freshly
            # mutated successor handle lost on the TOCTOU branch.
            handle = _current_lease_var.get()
            if handle is not None:
                handle.mark_lost()
        return bool(
            current is True
            and same_local_life
            and local_authority_open()
            and _protected_cloud_runtime_ready()
        )
    if not _pinned_runtime_generation_enabled:
        # Compatibility phase for old orchestrators. Strict mode/new attaches
        # advertise the additive contract and always take the exact DB fence.
        return True
    session = _session
    thread_id = _thread_id
    if session is None or session.postgres_conn is None or thread_id is None:
        return False
    try:
        agent_id, pod_uid, _process_generation, attach_token = (
            _pinned_input_runtime_identity()
        )
        session_generation = str(_session_runtime_generation or "").strip()
        if not session_generation:
            return False
        current = await session.postgres_conn.verify_pinned_runtime_effect_authority(
            thread_id=str(thread_id),
            agent_id=agent_id,
            pod_uid=pod_uid,
            session_runtime_generation=session_generation,
            runtime_attach_token=attach_token,
        )
    except Exception as exc:
        logger.warning(
            "Pinned runtime effect-authority proof failed (%s)",
            type(exc).__name__,
        )
        return False
    return bool(
        current is True
        and session is _session
        and str(thread_id) == str(_thread_id)
        and _process_generation == _input_runtime_generation
        and attach_token == _session_runtime_attach_token
        and local_authority_open()
        and _protected_cloud_runtime_ready()
    )


async def _loop_runtime_effect_authority_current() -> bool:
    """Exact authority for new provider/tool effects."""

    return await _loop_runtime_authority_current()


async def _loop_runtime_settlement_authority_current() -> bool:
    """Exact authority for work admitted before retirement preflight."""

    return await _loop_runtime_authority_current(allow_retirement_settlement=True)


async def _transition_claimed_input(
    delivery_id: str,
    claim_generation: int,
    transition: str,
    *,
    turn_number: int | None = None,
    reason: str | None = None,
) -> bool:
    if _session is None or _session.postgres_conn is None or _thread_id is None:
        return False
    try:
        live_lease = _current_lease_var.get()
        if live_lease is not None and live_lease.active:
            executor_id = str(live_lease.executor_id or "").strip()
            pod_uid = str(live_lease.pod_uid or "").strip()
            if (
                str(live_lease.unit_id or "") != str(_thread_id)
                or not executor_id
                or not pod_uid
            ):
                return False
            return await _session.postgres_conn.transition_stateless_input_delivery(
                thread_id=_thread_id,
                delivery_id=delivery_id,
                lease_token=int(live_lease.lease_token),
                executor_id=executor_id,
                pod_uid=pod_uid,
                claim_generation=claim_generation,
                transition=transition,
                turn_number=turn_number,
                reason=reason,
            )
        agent_id, pod_uid, runtime_generation, runtime_attach_token = (
            _pinned_input_runtime_identity()
        )
        return await _session.postgres_conn.transition_pinned_input_delivery(
            thread_id=_thread_id,
            delivery_id=delivery_id,
            agent_id=agent_id,
            pod_uid=pod_uid,
            runtime_generation=runtime_generation,
            session_runtime_generation=str(_session_runtime_generation or ""),
            runtime_attach_token=runtime_attach_token,
            claim_generation=claim_generation,
            transition=transition,
            turn_number=turn_number,
            reason=reason,
        )
    except Exception as exc:
        logger.warning(
            "Durable input %s transition failed (%s)",
            transition,
            type(exc).__name__,
        )
        return False


async def _queue_claimed_input(row: dict[str, Any]) -> bool:
    """Queue one exact durable claim once in this process generation."""

    if _session is None or _session.postgres_conn is None or _thread_id is None:
        return False
    queue = _loop_user_queue
    if queue is None:
        return False
    delivery_id = str(row["delivery_id"])
    claim_generation = int(row["claim_generation"])
    key = (delivery_id, claim_generation)
    if key in _queued_input_claims:
        return False
    if not _protected_cloud_runtime_ready():
        await _transition_claimed_input(
            delivery_id,
            claim_generation,
            "deferred",
            reason="protected_cloud_unavailable_before_queue",
        )
        _schedule_protected_input_reclaim()
        return False
    agent_id, pod_uid, runtime_generation, runtime_attach_token = (
        _pinned_input_runtime_identity()
    )
    queued = await _session.postgres_conn.mark_pinned_input_delivery_queued(
        thread_id=_thread_id,
        delivery_id=delivery_id,
        agent_id=agent_id,
        pod_uid=pod_uid,
        runtime_generation=runtime_generation,
        session_runtime_generation=str(_session_runtime_generation or ""),
        runtime_attach_token=runtime_attach_token,
        claim_generation=claim_generation,
    )
    if not queued:
        return False
    # Another same-process request may have completed the identical DB CAS
    # while this coroutine awaited it. Re-check at the no-await publication
    # boundary so concurrent HTTP retries still produce one queue item.
    if key in _queued_input_claims:
        return False
    if _runtime_admission_closed() or not _protected_cloud_runtime_ready():
        await _transition_claimed_input(
            delivery_id,
            claim_generation,
            "deferred",
            reason=(
                "runtime_terminating_before_queue"
                if _runtime_admission_closed()
                else "protected_cloud_unavailable_before_queue_publish"
            ),
        )
        if not _runtime_admission_closed():
            _schedule_protected_input_reclaim()
        return False

    # No await between local dedup publication and the unbounded put. A retry
    # in this process observes the set; a process death loses the set and its
    # new runtime generation reclaims the durable queued row.
    _queued_input_claims.add(key)
    queue_item = {
        "content": str(row["content"]),
        "id": str(row["message_id"]),
        "role": str(row["role"]),
        "source": str(row["source"]),
        "delivery_id": delivery_id,
        "claim_generation": claim_generation,
    }
    if row.get("supersedes_input_seq") is not None:
        queue_item["supersedes_input_seq"] = int(row["supersedes_input_seq"])
    queue.put_nowait(queue_item)
    return True


async def _reclaim_pending_pinned_inputs_locked() -> set[tuple[str, int]]:
    """Replay pending input while holding ``_input_delivery_reclaim_lock``."""

    if _session is None or _session.postgres_conn is None or _thread_id is None:
        return set()
    agent_id, pod_uid, runtime_generation, runtime_attach_token = (
        _pinned_input_runtime_identity()
    )
    rows = await _session.postgres_conn.claim_pending_pinned_input_deliveries(
        thread_id=_thread_id,
        agent_id=agent_id,
        pod_uid=pod_uid,
        runtime_generation=runtime_generation,
        session_runtime_generation=str(_session_runtime_generation or ""),
        runtime_attach_token=runtime_attach_token,
    )
    queued: set[tuple[str, int]] = set()
    for row in rows:
        if await _queue_claimed_input(row):
            queued.add((str(row["delivery_id"]), int(row["claim_generation"])))
    if queued:
        logger.info(
            "Reclaimed %d durable persistent input(s) for thread %s",
            len(queued),
            _thread_id,
        )
    return queued


async def _reclaim_pending_pinned_inputs() -> set[tuple[str, int]]:
    """Attach-time or accept-time replay for persisted unadmitted input."""

    async with _input_delivery_reclaim_lock:
        return await _reclaim_pending_pinned_inputs_locked()


def _schedule_protected_input_reclaim() -> None:
    """Reclaim deferred input once this exact protected runtime heals."""

    global _protected_input_reclaim_task
    if _runtime_admission_closed() or _session is None or _thread_id is None:
        return
    current = _protected_input_reclaim_task
    if current is not None and not current.done():
        return
    session = _session
    thread_id = str(_thread_id)
    generation = _session_generation

    async def _wait_and_reclaim() -> None:
        global _protected_input_reclaim_task
        try:
            while _session_identity_matches(session, thread_id, generation):
                if _runtime_admission_closed():
                    return
                if _protected_cloud_runtime_ready():
                    await _reclaim_pending_pinned_inputs()
                    return
                await asyncio.sleep(1.0)
        finally:
            if _protected_input_reclaim_task is asyncio.current_task():
                _protected_input_reclaim_task = None

    _protected_input_reclaim_task = _track_session_side_task(
        asyncio.create_task(
            _wait_and_reclaim(),
            name=f"protected-input-reclaim-{thread_id[:12]}",
        )
    )


async def _accept_user_input(
    content: str,
    *,
    role: str = "human",
    delivery_id: str | None = None,
    expected_session_identity_fingerprint: str | None = None,
) -> AcceptedInput:
    """Persist an accepted user message, then enqueue it for the loop.

    Returns the durable/local admission outcome. Persisting BEFORE the 200 closes the
    swallowed-input gap (session_silent_failure_audit.md #1): the queue is
    process memory, so without the row a mid-turn input vanished from the UI
    on reload and died with the pod. The loop reuses the id when it consumes
    the item, so its own persist is an upsert onto this row (final
    turn_number), never a duplicate.

    ``role`` controls only how the row is PERSISTED; the in-memory message stays
    a ``HumanMessage`` regardless. That split is deliberate, and both halves are
    load-bearing:

    * ``role='event'`` keeps a system-injected notice (a worker job the session
      created has finished) out of the human-bubble family, so the transcript
      does not claim the user said it. It joins the shipped non-conversational
      roles ``summary`` and ``error``, which the cockpit already branches on.
    * Keeping the carrier a ``HumanMessage`` is what keeps ``_save_turn_ai_messages``
      correct — it reconciles a turn by walking backwards until it hits one — and
      avoids introducing a novel LangChain type into the graph. A synthetic
      AIMessage+ToolMessage pair (the *transient* injection family) would be the
      wrong shape: this is a one-time fact that must survive compaction.
    """
    if _runtime_admission_closed():
        raise TerminationAdmissionClosed
    if not _protected_cloud_runtime_ready():
        raise ProtectedCloudUnavailable
    if (
        expected_session_identity_fingerprint is not None
        and _current_pinned_session_identity_fingerprint()
        != expected_session_identity_fingerprint
    ):
        raise SessionIdentityMismatch

    parsed_delivery_id = UUID(str(delivery_id)) if delivery_id else uuid4()
    injected = role != "human"
    if not injected:
        _loop_last_user_content[0] = content

    if _session is None or _session.postgres_conn is None or _thread_id is None:
        raise DurableInputUnavailable
    try:
        agent_id, pod_uid, runtime_generation, runtime_attach_token = (
            _pinned_input_runtime_identity()
        )
        row = await asyncio.wait_for(
            _session.postgres_conn.persist_pinned_input_delivery(
                thread_id=_thread_id,
                delivery_id=str(parsed_delivery_id),
                role=role,
                content=content,
                source="officer_wake" if injected else "direct_human",
                turn_number=_session.turn_count + 1,
                agent_id=agent_id,
                pod_uid=pod_uid,
                runtime_generation=runtime_generation,
                session_runtime_generation=str(_session_runtime_generation or ""),
                runtime_attach_token=runtime_attach_token,
            ),
            timeout=5.0,
        )
    except Exception as exc:
        logger.warning("Durable input persist/claim failed (%s)", type(exc).__name__)
        raise DurableInputUnavailable from exc

    state = str(row["state"])
    claim_generation = int(row["claim_generation"])
    duplicate = not bool(row.get("transcript_inserted"))
    if state in {"admitted", "settled", "cancelled"}:
        return AcceptedInput(
            message_id=str(row["message_id"]),
            delivery_id=str(parsed_delivery_id),
            delivery_state=state,
            claim_generation=claim_generation,
            enqueued=False,
            duplicate=True,
        )

    if not _protected_cloud_runtime_ready():
        deferred = await _transition_claimed_input(
            str(parsed_delivery_id),
            claim_generation,
            "deferred",
            reason="protected_cloud_unavailable_after_persist",
        )
        if not deferred:
            raise DurableInputUnavailable
        _schedule_protected_input_reclaim()
        return AcceptedInput(
            message_id=str(row["message_id"]),
            delivery_id=str(parsed_delivery_id),
            delivery_state="deferred",
            claim_generation=claim_generation,
            enqueued=False,
            duplicate=duplicate,
            deferred=True,
        )

    if _runtime_admission_closed():
        await _transition_claimed_input(
            str(parsed_delivery_id),
            claim_generation,
            "deferred",
            reason="runtime_terminating_after_persist",
        )
        return AcceptedInput(
            message_id=str(row["message_id"]),
            delivery_id=str(parsed_delivery_id),
            delivery_state="deferred",
            claim_generation=claim_generation,
            enqueued=False,
            duplicate=duplicate,
            deferred=True,
        )

    key = (str(parsed_delivery_id), claim_generation)
    already_queued = key in _queued_input_claims
    # Claim the whole durable inbox in transcript order, including this row.
    # That both preserves ordering and gives a same-process runtime a bounded
    # way to recover inputs deferred by a transient authorization failure. A
    # retry of this row observes the local claim set and cannot publish twice.
    newly_queued = await _reclaim_pending_pinned_inputs()
    queued_here = key in _queued_input_claims
    enqueued = key in newly_queued and not already_queued
    deferred = not queued_here and _runtime_admission_closed()
    if injected and enqueued:
        # Make the injection visible in a live cockpit. Nothing else would:
        # /api/input broadcasts nothing and no frame carries user-message
        # content (the cockpit builds a user turn from its own optimistic
        # dispatch on send, or from a history reload). Without this the user
        # watches a turn start and stream a reply with no visible prompt — the
        # agent apparently talking to itself. Rides the normal _broadcast path,
        # so it reaches WS subscribers and the thread_events log (hence SSE)
        # alike.
        _broadcast(
            "session.event",
            {
                "content": str(row["content"]),
                "id": str(row["message_id"]),
                "role": role,
            },
        )
    # Title the thread from the opening prompt(s) so the cockpit header fills in
    # on submit rather than only after the (possibly long) first turn ends.
    # Fire-and-forget — must not block input acceptance. _early_title_from_prompt
    # self-guards on a placeholder title and a low-signal prompt; the after-turn
    # pass in _loop_on_turn_complete remains the fallback.
    #
    # Injected input is excluded: a wake landing in a young session would
    # retitle the whole thread after the job-completion text.
    if not injected and _session is not None and _session.turn_count <= 2:
        title_session = _session
        title_thread_id = str(_thread_id or "")
        title_generation = _session_generation
        _track_session_side_task(
            asyncio.create_task(
                _early_title_from_prompt(
                    content,
                    expected_session=title_session,
                    expected_thread_id=title_thread_id,
                    expected_generation=title_generation,
                ),
                name=f"early-title-{title_thread_id[:12]}",
            )
        )
    return AcceptedInput(
        message_id=str(row["message_id"]),
        delivery_id=str(parsed_delivery_id),
        delivery_state="deferred" if deferred else "queued" if queued_here else state,
        claim_generation=claim_generation,
        enqueued=enqueued,
        duplicate=duplicate,
        deferred=deferred,
    )


async def handle_api_input(request: Request) -> JSONResponse:
    """Push user input onto the loop's queue. Body: {content, role?, turn_id?}.

    ``role`` defaults to 'human'. The orchestrator sends ``role='event'`` when
    injecting a system notice (a worker job the session created finished) so the
    persisted row does not render as a user bubble.

    Before persistence, a 503 tells either caller to retry. After the durable
    transaction commits, both human and event inputs receive an accepted 202
    with their exact delivery state. The orchestrator interprets that as
    persisted-not-executed and retains its stable-identity outbox claim; an
    uncorrelated human client must not submit a second input.
    """
    if _stateless_mode():
        return _stateless_reject()
    if _runtime_admission_closed():
        return _termination_rejection()
    if _session is not None and not _protected_cloud_runtime_ready():
        return _protected_cloud_unavailable_rejection()
    if _session is None or _loop_user_queue is None:
        return JSONResponse({"error": "Session not active"}, status_code=503)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid JSON"}, status_code=400)
    content = body.get("content", "")
    if not isinstance(content, str) or not content:
        return JSONResponse(
            {"error": "content must be a non-empty string"},
            status_code=400,
        )
    role = body.get("role") or "human"
    if role not in _ACCEPTED_INPUT_ROLES:
        return JSONResponse(
            {"error": f"role must be one of {sorted(_ACCEPTED_INPUT_ROLES)}"},
            status_code=400,
        )
    delivery_id = body.get("delivery_id")
    expected_session_identity_fingerprint = body.get("session_identity_fingerprint")
    if role == "event":
        import uuid as _uuid

        if delivery_id is None:
            return JSONResponse(
                {"error": "durable event input requires a delivery_id"},
                status_code=400,
            )
        expected_key = os.environ.get("MCP_INTERNAL_KEY", "")
        presented_key = request.headers.get("X-Internal-Key", "")
        if not expected_key or not hmac.compare_digest(expected_key, presented_key):
            # The identity links an outbox row to its one paid turn. It is
            # server-owned authority, not an unauthenticated dedup hint.
            return JSONResponse(
                {"error": "durable event delivery requires internal authority"},
                status_code=403,
            )
        try:
            delivery_id = str(_uuid.UUID(str(delivery_id)))
        except (ValueError, TypeError, AttributeError):
            return JSONResponse(
                {"error": "delivery_id must be a UUID"}, status_code=400
            )
    elif delivery_id is not None:
        return JSONResponse(
            {"error": "delivery_id is reserved for durable event input"},
            status_code=400,
        )
    expected_session_identity_fingerprint = (
        _canonical_pinned_session_identity_fingerprint(
            expected_session_identity_fingerprint
        )
    )
    if (
        expected_session_identity_fingerprint is None
        or _current_pinned_session_identity_fingerprint()
        != expected_session_identity_fingerprint
    ):
        return JSONResponse(
            {"error": "session_identity_mismatch", "retryable": True},
            status_code=409,
        )
    if not _ensure_persistent_loop_started("rest_input"):
        if _runtime_admission_closed():
            return _termination_rejection()
        return JSONResponse({"error": "Session not ready"}, status_code=503)
    try:
        admission = await _accept_user_input(
            content,
            role=role,
            delivery_id=delivery_id,
            expected_session_identity_fingerprint=expected_session_identity_fingerprint,
        )
    except TerminationAdmissionClosed:
        return _termination_rejection()
    except ProtectedCloudUnavailable:
        return _protected_cloud_unavailable_rejection()
    except DurableInputUnavailable:
        return JSONResponse(
            {
                "error": "durable_input_unavailable",
                "retryable": True,
                "message": "Durable input admission is temporarily unavailable.",
            },
            status_code=503,
            headers={"Retry-After": "5"},
        )
    except SessionIdentityMismatch:
        return JSONResponse(
            {"error": "session_identity_mismatch", "retryable": True},
            status_code=409,
        )
    return JSONResponse(
        {
            **_accepted_input_payload(admission),
            "turn_id": _session.turn_count,
            "queue_depth": _loop_user_queue.qsize(),
        },
        status_code=(
            200
            if admission.delivery_state in {"admitted", "settled", "cancelled"}
            else 202
        ),
    )


def _clear_loop_interrupt(*, target_turn_id: int | None = None) -> bool:
    """Clear one pending interrupt without crossing a turn boundary.

    When ``target_turn_id`` is supplied, a newer turn's pending interrupt is
    left untouched. Mode, target and the hard-event signal are one logical
    value and are always cleared together.
    """

    global _loop_interrupt_flag, _loop_interrupt_target_turn_id
    if target_turn_id is not None and _loop_interrupt_target_turn_id != int(
        target_turn_id
    ):
        return False
    had_interrupt = (
        _loop_interrupt_flag is not None
        or _loop_interrupt_target_turn_id is not None
        or bool(_hard_interrupt_event and _hard_interrupt_event.is_set())
    )
    _loop_interrupt_flag = None
    _loop_interrupt_target_turn_id = None
    if _hard_interrupt_event is not None:
        _hard_interrupt_event.clear()
    return had_interrupt


def _signal_interrupt_for_turn(
    target_turn_id: int,
    *,
    force_graceful: bool = False,
) -> Optional[str]:
    """Synchronously signal RAM iff ``target_turn_id`` is still active.

    The check and mutation have no await between them. That is the local half
    of the exact-target fence: the database protects the lease generation,
    while this function prevents a late request for turn N from interrupting
    turn N+1 after an in-process transition.
    """

    global _loop_interrupt_flag, _loop_interrupt_target_turn_id
    if (
        _session is None
        or not _turn_event_open
        or int(_session.turn_count) != int(target_turn_id)
    ):
        return None
    mode = "graceful" if (_tool_inflight or force_graceful) else "hard"
    _loop_interrupt_flag = mode
    _loop_interrupt_target_turn_id = int(target_turn_id)
    # Hard interrupt with no tool in flight ⇒ the loop is parked in an LLM /
    # auxiliary await; signal it to cancel that await immediately rather than
    # waiting for the next cooperative check_interrupt poll.
    if mode == "hard" and _hard_interrupt_event is not None:
        _hard_interrupt_event.set()
    return mode


async def handle_api_interrupt(request: Optional[Request] = None) -> JSONResponse:
    """Signal the pinned loop, optionally fenced to a correlated turn.

    Every HTTP call must carry the exact pinned-session fingerprint.  After
    removing that identity field, an otherwise-empty body retains the legacy
    active-turn behavior. New orchestrator forwarding may additionally supply
    ``client_request_id`` and ``target_turn_id``; those calls are rejected
    before any RAM mutation unless the exact transcript turn is active.
    Stateless pods consume the durable inbox instead of this direct route.
    """

    if _stateless_mode():
        return _stateless_reject()
    if _session is None:
        return JSONResponse({"error": "Session not active"}, status_code=503)

    parsed_body: Dict[str, Any] = {}
    if request is not None:
        raw = await request.body()
        if raw:
            try:
                parsed = json.loads(raw)
            except (TypeError, ValueError):
                return JSONResponse(
                    {"error": "invalid JSON", "error_code": "invalid_request"},
                    status_code=400,
                )
            if not isinstance(parsed, dict):
                return JSONResponse(
                    {
                        "error": "body must be a JSON object",
                        "error_code": "invalid_request",
                    },
                    status_code=400,
                )
            parsed_body = parsed

    expected_session_identity_fingerprint = (
        _canonical_pinned_session_identity_fingerprint(
            parsed_body.get("session_identity_fingerprint")
        )
    )
    if (
        expected_session_identity_fingerprint is None
        or _current_pinned_session_identity_fingerprint()
        != expected_session_identity_fingerprint
    ):
        return JSONResponse(
            {"error": "session_identity_mismatch", "retryable": True},
            status_code=409,
        )

    body = dict(parsed_body)
    body.pop("session_identity_fingerprint", None)
    if not body:
        # Legacy clients omitted a turn identity. Scope the exact-runtime
        # request to the concrete active turn observed now; an idle request
        # must never arm the next input.
        target_turn_id = int(_session.turn_count)
        mode = _signal_interrupt_for_turn(target_turn_id)
        if mode is None:
            return JSONResponse(
                {
                    "ack": False,
                    "applied": False,
                    "target_turn_id": target_turn_id,
                    "error": "target turn is no longer active",
                    "error_code": "target_turn_not_active",
                },
                status_code=409,
            )
        logger.info(
            "Interrupt received via legacy REST "
            "(target_turn=%d mode=%s tool_inflight=%s)",
            target_turn_id,
            mode,
            _tool_inflight,
        )
        return JSONResponse(
            {
                "ack": True,
                "applied": True,
                "target_turn_id": target_turn_id,
                "mode": mode,
            }
        )

    client_request_id = body.get("client_request_id")
    target_turn_id = body.get("target_turn_id")
    if not isinstance(client_request_id, str) or not client_request_id:
        return JSONResponse(
            {
                "error": "client_request_id must be a non-empty string",
                "error_code": "invalid_request",
            },
            status_code=400,
        )
    if (
        isinstance(target_turn_id, bool)
        or not isinstance(target_turn_id, int)
        or target_turn_id < 1
    ):
        return JSONResponse(
            {
                "error": "target_turn_id must be a positive integer",
                "error_code": "invalid_request",
            },
            status_code=400,
        )

    response: Dict[str, Any] = {
        "client_request_id": client_request_id,
        "target_turn_id": target_turn_id,
    }
    request_id = body.get("request_id")
    if request_id is not None:
        if not isinstance(request_id, str) or not request_id:
            return JSONResponse(
                {
                    "error": "request_id must be a non-empty string",
                    "error_code": "invalid_request",
                },
                status_code=400,
            )
        response["request_id"] = request_id

    mode = _signal_interrupt_for_turn(target_turn_id)
    if mode is None:
        return JSONResponse(
            {
                **response,
                "applied": False,
                "error": "target turn is no longer active",
                "error_code": "target_turn_not_active",
            },
            status_code=409,
        )
    logger.info(
        "Correlated interrupt received via REST "
        "(target_turn=%d mode=%s tool_inflight=%s)",
        target_turn_id,
        mode,
        _tool_inflight,
    )
    return JSONResponse({**response, "ack": True, "applied": True, "mode": mode})


async def handle_api_approve(request: Request) -> JSONResponse:
    """Resolve a pending permission gate by UPDATEing the
    thread_permission_requests row. Body: {decision: approve|deny,
    approval_id?}. If approval_id is omitted, the most-recent-pending
    row for this thread is resolved (legacy single-pending-at-a-time
    contract). The DB trigger emits NOTIFY → agent's permission_check
    wakes up."""
    if _stateless_mode():
        return _stateless_reject()
    if _session is None:
        return JSONResponse({"error": "Session not active"}, status_code=503)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid JSON"}, status_code=400)
    decision_raw = body.get("decision")
    if decision_raw == "approve":
        decision = "approved"
    elif decision_raw == "deny":
        decision = "denied"
    else:
        return JSONResponse(
            {"error": "decision must be 'approve' or 'deny'"},
            status_code=400,
        )
    approval_id = body.get("approval_id")
    resolved = await _resolve_pending_permission(
        decision,
        approval_id=approval_id,
        decided_by="rest_client",
    )
    if resolved is None:
        return JSONResponse(
            {
                "error": "No matching pending request",
                "approval_id": approval_id,
            },
            status_code=404,
        )
    return JSONResponse(
        {
            "accepted": True,
            "decision": decision_raw,
            "approval_id": str(resolved["id"]),
            "tool_call_id": resolved["tool_call_id"],
        }
    )


# --- WebSocket handler (module-level so dual_app can call it too) ---


async def handle_persistent_websocket(ws: WebSocket) -> None:
    """WebSocket consumer for an already-running persistent session.

    Headless lifecycle (chunk 1):
      - First WS attach spawns the persistent loop with module-level
        callbacks. Subsequent attaches just register a subscriber and
        tap into the existing loop's broadcast stream.
      - WS close calls _unsubscribe() and cancels this connection's pump
        task. The loop keeps running. It only stops via
        _loop_completion_handler (idle timeout, /done, crash) or via
        out-of-band _terminate_session (drain, watchdog, REST detach).
      - The pod no longer exits when the WS closes — that was the
        WS-bound era. _schedule_exit is now driven only by drain intent
        and shutdown paths.

    Reached from both:
      - persistent_app.create_persistent_app()'s /ws/chat route (pure
        persistent mode, agent.py --mode persistent).
      - dual_app.ws_chat (dual mode — adds pod-state pre-checks then
        delegates here). Sharing this body is what closes the Phase-1
        gap described in
        knowledge-base/knowledge/issues/persistent_session_dual_mode_phase1_gap.md.
    """
    import uuid

    validated_identity = getattr(ws.state, "session_identity_fingerprint", None)
    validated_session = _session
    await ws.accept()
    if (
        not isinstance(validated_identity, str)
        or validated_session is None
        or _session is not validated_session
        or _current_pinned_session_identity_fingerprint() != validated_identity
    ):
        await ws.close(code=4403, reason="session identity changed")
        return

    # Stateless executor (M3): sessions on this pod are driven exclusively by
    # run_queue claims — there is no live WS surface. Mirror the REST 409.
    if _stateless_mode():
        try:
            await ws.send_json(
                {
                    "method": "error",
                    "params": {
                        "message": (
                            "stateless executor: this pod serves queued turns; "
                            "no direct session WebSocket is available"
                        )
                    },
                }
            )
        except Exception:
            pass
        await ws.close(code=4409, reason="stateless executor")
        return

    if _runtime_admission_closed():
        try:
            await ws.send_json(
                {
                    "method": "input.rejected",
                    "params": {
                        "error": "runtime_terminating",
                        "retryable": True,
                        "message": "Retry input on the replacement runtime.",
                    },
                }
            )
        finally:
            await ws.close(code=4512, reason="runtime terminating")
        return

    # Signal the boot-WS watchdog that a connection arrived. Done before
    # the readiness check so even a failed-to-be-ready connection counts:
    # the user clearly came back, and a different error path applies.
    _signal_ws_connected()

    # Readiness gates on the loop primitives, not just the session — see
    # _session_ready() for the why. Single source of truth shared with
    # /ready and /session/status so the probe and the WS gate can't drift.
    if not _session_ready():
        await _ws_send(ws, "error", {"message": "Agent not ready"})
        await ws.close(code=4503, reason="Agent not ready")
        return

    # Register this WS as a subscriber on the broadcast hub.
    client_id = uuid.uuid4().hex
    queue = _subscribe(client_id)
    pump_task = asyncio.create_task(
        _run_subscriber_pump(ws, client_id, queue),
        name=f"subscriber-pump-{client_id[:8]}",
    )

    logger.info(f"WebSocket connected: thread={_thread_id} client={client_id[:8]}")

    def _identity_current() -> bool:
        return bool(
            _session is validated_session
            and _current_pinned_session_identity_fingerprint() == validated_identity
        )

    async def _reject_preloop_identity_change() -> None:
        _unsubscribe(client_id)
        _clear_canvas_awareness(client_id)
        if not pump_task.done():
            pump_task.cancel()
            try:
                await pump_task
            except asyncio.CancelledError:
                pass
        await ws.close(code=4403, reason="session identity changed")

    def _spawn_ws_effect(coro: Any, *, name: str) -> asyncio.Task[Any]:
        return _track_session_side_task(asyncio.create_task(coro, name=name))

    # Send current session state so this client can sync. Direct send —
    # this is the welcome frame, only the connecting client cares.
    #
    # running_tool: if the loop is blocked in a tool call right now, tell this
    # (re)attaching client which command is running so it can render a "running
    # command" card instead of a blank "Connecting…". Incremental history may
    # already carry the AIMessage + tool_call, but it cannot say that the call
    # is still running — this welcome frame is the authoritative snapshot.
    #
    # pending_permissions: same idea for supervised gates that are still
    # waiting on an answer. The durable row survives, but REST history does
    # not carry it, so without this a reload (or a dropped live stream) leaves
    # the approval card unrenderable and the gate unanswerable — the failure
    # in knowledge-history/done/supervised_parallel_gates_timeout_fabricates_denial.md.
    running_tool = (
        inflight_tool_call(validated_session.messages) if _tool_inflight else None
    )
    if running_tool is not None:
        running_tool = {
            "id": running_tool["id"],
            "tool": running_tool["tool"],
            "args": _safe_serialize(running_tool["args"]),
        }
    (
        durable_permission_mode,
        durable_narration_mode,
    ) = await _durable_session_control_modes()
    if not _identity_current():
        await _reject_preloop_identity_change()
        return
    pending_permissions = await _pending_permission_requests()
    if not _identity_current():
        await _reject_preloop_identity_change()
        return
    task_manager = validated_session.session_task_manager
    session_tasks = task_manager.to_dict_list() if task_manager is not None else []
    await _ws_send(
        ws,
        "session.state",
        {
            "thread_id": _thread_id,
            "permission_mode": durable_permission_mode,
            "narration_mode": durable_narration_mode,
            "turn_count": validated_session.turn_count,
            # Authoritative join signal for a cold Cockpit reattach. REST can
            # already contain an incrementally persisted prefix of this turn;
            # the client uses (turn_in_flight, turn_count) to keep that prefix
            # and cursor-replayed suffix in one streaming bubble.
            "turn_in_flight": _turn_event_open,
            "message_count": len(validated_session.messages),
            "model": validated_session.config.llm.model,
            "temperature": validated_session.config.llm.temperature,
            "running_tool": running_tool,
            "pending_permissions": pending_permissions,
            "tasks": session_tasks,
        },
    )
    if not _identity_current():
        await _reject_preloop_identity_change()
        return

    # Spawn the persistent loop if it isn't already running. Reconnecting
    # to a session whose loop is mid-turn just joins the broadcast — no
    # restart, no replay (replay arrives in chunk 2 via the event log).
    _ensure_persistent_loop_started("websocket", client_id=client_id)

    # --- WebSocket receive loop ---
    try:
        # The exact current queue lock is also the admission serialization
        # point. Any old-token admission that started before this claim must
        # commit before this snapshot; one that starts after it observes the
        # new token and cannot create old-generation work. Fetch the complete
        # (unbounded) generation once so one steal produces at most one epoch
        # rotation and one terminal boundary per abandoned target.
        while True:
            raw = await ws.receive_text()
            if not _identity_current():
                await ws.close(code=4403, reason="session identity changed")
                break
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                # Plain text → treat as message
                data = {"method": "message", "content": raw}

            method = data.get("method", "message")
            if _retirement_admission_closed():
                # This socket may predate the exact ``ending`` transition.
                # Retire every moment-scoped control surface together: a late
                # approval/config/upgrade must not leak through simply because
                # it is not a user-message verb. The SSE/journal plane lives in
                # Cockpit and is intentionally independent of this socket.
                await _ws_send(
                    ws,
                    "input.rejected",
                    {
                        "error": "runtime_terminating",
                        "retryable": True,
                        "message": "Retry input on the replacement runtime.",
                    },
                )
                await ws.close(code=4512, reason="runtime terminating")
                break

            if method == "message":
                content = data.get("content", "")
                if content and _loop_user_queue is not None:
                    try:
                        admission = await _accept_user_input(
                            content,
                            expected_session_identity_fingerprint=validated_identity,
                        )
                        rejection_error = "runtime_terminating"
                        rejection_message = "Retry input on the replacement runtime."
                    except TerminationAdmissionClosed:
                        admission = None
                        rejection_error = "runtime_terminating"
                        rejection_message = "Retry input on the replacement runtime."
                    except ProtectedCloudUnavailable:
                        admission = None
                        rejection_error = "protected_cloud_unavailable"
                        rejection_message = (
                            "Retry input when the protected cloud mount recovers."
                        )
                    except DurableInputUnavailable:
                        admission = None
                        rejection_error = "durable_input_unavailable"
                        rejection_message = "Retry input when durable storage recovers."
                    except SessionIdentityMismatch:
                        await ws.close(code=4403, reason="session identity changed")
                        break
                    if admission is None:
                        await _ws_send(
                            ws,
                            "input.rejected",
                            {
                                "error": rejection_error,
                                "retryable": True,
                                "message": rejection_message,
                            },
                        )
                    elif admission.deferred:
                        await _ws_send(
                            ws,
                            "input.accepted",
                            _accepted_input_payload(admission),
                        )

            elif method in {
                "canvas.presentation_updated",
                "canvas.source_updated",
                "canvas.user_editing",
                "canvas.user_idle",
            }:
                await _handle_canvas_control(
                    ws,
                    data,
                    client_id,
                    expected_session_identity_fingerprint=validated_identity,
                )

            elif method == "approve":
                # Phase 3: resolve the most-recent-pending permission
                # request in the DB. Cockpit can pass an explicit
                # approval_id to disambiguate when multiple are
                # pending (rare — agent's loop serializes most flows).
                approval_id = data.get("approval_id")
                _spawn_ws_effect(
                    _resolve_pending_permission(
                        "approved",
                        approval_id=approval_id,
                        decided_by="ws_client",
                    ),
                    name="resolve-approve",
                )

            elif method == "deny":
                approval_id = data.get("approval_id")
                _spawn_ws_effect(
                    _resolve_pending_permission(
                        "denied",
                        approval_id=approval_id,
                        decided_by="ws_client",
                    ),
                    name="resolve-deny",
                )

            elif method == "interrupt":
                # Legacy WebSocket clients carry no target. Bind the request to
                # the concrete active turn observed now; never leave a bare flag
                # that can interrupt a later input.
                target_turn_id = int(_session.turn_count) if _session else 0
                mode = (
                    _signal_interrupt_for_turn(target_turn_id)
                    if target_turn_id > 0
                    else None
                )
                if mode is None:
                    await _ws_send(
                        ws,
                        "interrupt.ack",
                        {
                            "applied": False,
                            "target_turn_id": target_turn_id or None,
                            "error_code": "target_turn_not_active",
                        },
                    )
                    logger.info("Idle legacy WebSocket interrupt rejected")
                else:
                    await _ws_send(
                        ws,
                        "interrupt.ack",
                        {
                            "applied": True,
                            "target_turn_id": target_turn_id,
                            "mode": mode,
                        },
                    )
                    logger.info(
                        "Interrupt acknowledged (target_turn=%d mode=%s)",
                        target_turn_id,
                        mode,
                    )

            elif method in {"mode.set", "narration.set"}:
                # These verbs are lane-agnostic orchestrator REST controls.
                # Keeping a second live-only mutation path here would let an
                # old client change RAM without the desired scalar, inbox
                # order, durable result receipt, or owner fence.
                await _ws_send(
                    ws,
                    "error",
                    {
                        "code": "control_transport_retired",
                        "message": "Use the session control REST endpoint",
                    },
                )

            elif method == "config.update":
                config_override = data.get("config", {})
                # Slice B: a datasource change rides the same frame as a
                # sibling key — the full desired selection (None = unchanged).
                datasource_ids = data.get("datasource_ids")
                if config_override or datasource_ids is not None:
                    _spawn_ws_effect(
                        _handle_config_update(
                            ws,
                            config_override,
                            datasource_ids=datasource_ids,
                            request_id=data.get("request_id"),
                        ),
                        name="handle-config-update",
                    )

            elif method == "compact":
                # Manual compaction (/compact command, or the rewind action
                # sheet's "Summarize up to here" with boundary_message_id).
                focus = data.get("focus", "")
                _spawn_ws_effect(
                    _handle_compact(
                        ws, focus, boundary_message_id=data.get("boundary_message_id")
                    ),
                    name="handle-compact",
                )

            elif method == "archive":
                # End session (/done command)
                _spawn_ws_effect(_handle_archive(ws), name="handle-archive")

            elif method == "upgrade-to-vm":
                # Upgrade workspace from container to VM
                _spawn_ws_effect(_handle_vm_upgrade(ws), name="handle-vm-upgrade")

            elif method == "upgrade-to-workspace":
                # Upgrade a lite (virtual) session to a real sandbox container
                target_tier = data.get("target_tier", "sandbox")
                _spawn_ws_effect(
                    _handle_workspace_upgrade(ws, target_tier),
                    name="handle-workspace-upgrade",
                )

            elif method == "undo":
                if _session is None:
                    await _ws_send(
                        ws,
                        "error",
                        {"message": "Session no longer active"},
                    )
                    continue
                if _session.shell_owner_token is not None:
                    await _ws_send(
                        ws,
                        "error",
                        {
                            "code": "control_transport_required",
                            "message": "Use the session control REST endpoint",
                        },
                    )
                    continue
                turn_id = data.get("turn_id")
                try:
                    restored = await _session.undo_turn(turn_id)
                except WorkspaceUndoUnavailable as exc:
                    await _ws_send(
                        ws,
                        "error",
                        {
                            "code": exc.code,
                            "message": str(exc),
                        },
                    )
                except WorkspaceUndoRetryable as exc:
                    await _ws_send(
                        ws,
                        "error",
                        {
                            "code": "workspace_undo_retryable",
                            "message": str(exc),
                        },
                    )
                else:
                    await _ws_send(
                        ws,
                        "files.restored",
                        {**restored, "turn_id": turn_id},
                    )

            elif method == "rewind":
                if _session is None:
                    await _ws_send(
                        ws,
                        "error",
                        {
                            "message": "Session no longer active",
                            "request_id": data.get("request_id"),
                        },
                    )
                    continue
                _spawn_ws_effect(
                    _handle_rewind(ws, data),
                    name="handle-rewind",
                )

            else:
                await _ws_send(ws, "error", {"message": f"Unknown method: {method}"})

    except WebSocketDisconnect:
        logger.info(
            f"WebSocket disconnected: thread={_thread_id} "
            f"client={client_id[:8]} (loop continues)"
        )
    except Exception as e:
        logger.exception(f"WebSocket error: {e}")
    finally:
        # Headless keystone: WS close only unsubscribes. The loop keeps
        # running until _loop_completion_handler routes its natural exit,
        # or out-of-band _terminate_session intervenes. We do NOT cancel
        # _loop_task here, and we do NOT schedule pod exit.
        _unsubscribe(client_id)
        _clear_canvas_awareness(client_id)
        if not pump_task.done():
            pump_task.cancel()
            try:
                await pump_task
            except asyncio.CancelledError:
                pass
        logger.info(
            f"WebSocket pump released: thread={_thread_id} client={client_id[:8]}"
        )


# --- Helpers ---


async def _ws_send(ws: WebSocket, method: str, params: Dict[str, Any]) -> None:
    """Send a direct response without adding it to the event journal."""
    await _session_transport.send_message(ws, method, params)


def _subscribe(client_id: str) -> asyncio.Queue:
    """Register a new subscriber and return its outbound queue.

    Each WebSocket connection (and later, each SSE consumer) gets its own
    bounded queue. _broadcast() enqueues onto every registered queue;
    _run_subscriber_pump drains one queue into one WS.

    Phase 5: if this is the first subscriber after an untethered pause,
    schedule a status revert to 'active' so the attention-sleep watchdog
    disarms. Fire-and-forget — a failed status write doesn't block the
    attach.
    """
    was_empty = not _subscribers
    queue = _session_transport.subscribe(
        _subscribers, client_id, maxsize=_SUBSCRIBER_QUEUE_MAXSIZE
    )
    if was_empty and _orchestrator_client is not None and _thread_id is not None:
        _track_session_side_task(
            asyncio.create_task(
                _safe_set_thread_status("active"), name="phase5-revert-active"
            )
        )
    return queue


def _unsubscribe(client_id: str) -> None:
    """Remove a subscriber. Cheap — does not touch the loop or session state.

    This is what WS close calls. The loop keeps running with one fewer audience.
    """
    _session_transport.unsubscribe(_subscribers, client_id)


async def _file_officer_wake(minutes: int, reason: str) -> None:
    """File the officer's durable timer wake with the orchestrator.

    Fire-and-forget from the park path. Failure is deliberately non-fatal:
    when no ``timer`` row exists the officer watchdog files ``sleep_max`` on
    the officer's behalf, and the local backstop covers the rest
    (centurion.md §4).
    """
    try:
        if _orchestrator_client is None or _thread_id is None:
            return
        ok = await _orchestrator_client.file_officer_wake(_thread_id, minutes, reason)
        if not ok:
            logger.warning(
                "Officer wake filing rejected for thread %s "
                "(watchdog will file sleep_max)",
                _thread_id,
            )
    except Exception as e:
        logger.warning(
            "Officer wake filing failed (non-fatal — watchdog files sleep_max): %s",
            e,
        )


async def _safe_set_thread_status(status: str) -> None:
    """Best-effort wrapper around _update_thread_status for fire-and-forget
    Phase 5 transitions (awaiting_user / active revert). A transient failure
    here is acceptable — the next natural-pause or subscriber attach will
    retry.
    """
    try:
        await _update_thread_status(status)
    except Exception as e:
        logger.warning("Failed to set thread status to %s: %s", status, e)


@dataclass(frozen=True)
class _QueuedPersistentEvent:
    """One immutable event-log write owned by the ordered writer."""

    epoch: int
    seq: int
    kind: str
    payload: Any
    control_request_id: Optional[str] = None
    control_lease_token: Optional[int] = None
    control_agent_id: Optional[str] = None
    interrupt_request_id: Optional[str] = None
    interrupt_lease_token: Optional[int] = None
    interrupt_accepted_lease_token: Optional[int] = None
    interrupt_stale_recovery: bool = False
    receipt: Optional[asyncio.Future] = None


def _requires_bounded_event_retry(kind: str) -> bool:
    """Return whether losing ``kind`` requires explicit client reconciliation.

    Canvas events are state invalidations rather than replaceable token pacing.
    Keep this predicate at the transport boundary so future Canvas adapters do
    not need their own event sequencer or persistence path.
    """

    return kind.startswith("canvas.") and kind != "canvas.reconcile_required"


class _OrderedPersistentEventWriter:
    """Persist one runtime's thread events in FIFO, atomic batches.

    ``_broadcast`` remains synchronous/non-blocking for the agent loop. It puts
    immutable events onto this bounded queue; one task is the sole database
    writer for the attached runtime. A single INSERT persists each batch in
    queue order, eliminating the old one-task-per-frame visibility race.

    The writer OWNS the epoch it attached under (constructor argument, not
    inferred from events) and every flush is fenced on it: rows land only
    while ``threads.events_epoch`` still equals that epoch. A fenced-out
    flush (another runtime, a rewind, or the reaper moved the generation) is
    terminal — the writer stops rather than keep inserting into a dead epoch
    forever, which is exactly what the unfenced INSERT used to do.

    Ordinary streaming frames get one best-effort write. State invalidations
    receive bounded retries. Queue overflow and terminal write failure are
    reported through ``on_terminal_failure`` so callers can force an
    authoritative-state reconciliation without recursively journaling it.
    """

    # One guarded round-trip (autocommit-safe): the INSERT is gated on the
    # threads row still being on this writer's epoch ($3), each unnested row
    # is belt-checked against the same epoch, and the thread's seq high-water
    # mark advances to the batch tail in the same statement (guarded on rows
    # actually inserted). The final SELECT returns the inserted count — 0 on
    # a non-empty batch means the epoch (or, stateless lane, the run_queue
    # lease) moved underneath us.
    #
    # M3 (stateless lane): when the writer is constructed with a lease, the
    # {lease_fence} slot carries a second EXISTS against run_queue —
    # token-equality only, deliberately WITHOUT ``state='leased'``: trailing
    # flushes between complete_unit (which does not bump the token) and the
    # next claim must still land, while any later claim/steal bumps the token
    # and fences stragglers out (§5.2 — "stragglers are invalidated by the
    # NEXT claim"). FOR SHARE holds the queue row against a concurrent
    # claim/steal for the duration of this single statement, so a flush can
    # never interleave with the token bump that should have fenced it.
    _INSERT_BATCH_SQL_TEMPLATE = """
        WITH queued_rows AS (
            SELECT
                (queued.value->>'epoch')::integer AS epoch,
                (queued.value->>'seq')::bigint    AS seq,
                queued.value->>'kind'             AS kind,
                queued.value->'payload'           AS payload,
                NULLIF(queued.value->>'control_request_id', '')::uuid
                                                    AS control_request_id,
                NULLIF(queued.value->>'interrupt_request_id', '')::uuid
                                                    AS interrupt_request_id,
                queued.ordinal                    AS ordinal
            FROM jsonb_array_elements($2::jsonb) WITH ORDINALITY
                AS queued(value, ordinal)
        ),
        inserted AS (
            INSERT INTO thread_events (
                thread_id, epoch, seq, kind, payload, control_request_id,
                interrupt_request_id
            )
            SELECT $1, queued_rows.epoch, queued_rows.seq,
                   queued_rows.kind, queued_rows.payload,
                   queued_rows.control_request_id,
                   queued_rows.interrupt_request_id
            FROM queued_rows
            WHERE EXISTS (
                    SELECT 1 FROM threads
                    WHERE threads.id = $1
                      AND threads.events_epoch = $3
                  )
              AND queued_rows.epoch = $3{lease_fence}
            ORDER BY queued_rows.ordinal
            RETURNING seq
        ),
        hwm_update AS (
            UPDATE threads
            SET events_seq_hwm = GREATEST(
                    events_seq_hwm,
                    (SELECT MAX(seq) FROM inserted)
                )
            WHERE id = $1
              AND EXISTS (SELECT 1 FROM inserted)
            RETURNING 1
        )
        SELECT COUNT(*)::bigint FROM inserted
    """
    _LEASE_FENCE_SQL = """
              AND EXISTS (
                    SELECT 1 FROM run_queue
                    WHERE run_queue.unit_id = $4::uuid
                      AND run_queue.lease_token = $5::bigint
                    FOR SHARE
                  )"""
    # Ordinary stateless frames retain the relaxed trailing-flush token rule,
    # but must establish the same threads -> run_queue lock order as public
    # End before their INSERT takes an implicit thread FK lock and updates the
    # event HWM.  Acquire that mutation's FOR NO KEY UPDATE strength up front:
    # it conflicts with public End's FOR UPDATE and serializes concurrent HWM
    # writers.  A weaker prelock lets two writers both reach the guarded HWM
    # UPDATE and deadlock while converting their parent-row locks.
    _LOCK_STATELESS_EVENT_THREAD_SQL = """
        SELECT 1 FROM threads
        WHERE id = $1::uuid AND events_epoch = $2::integer
        FOR NO KEY UPDATE
    """
    _LOCK_STATELESS_EVENT_QUEUE_SQL = """
        SELECT 1 FROM run_queue
        WHERE unit_id = $1::uuid AND lease_token = $2::bigint
        FOR SHARE
    """
    _LOCK_PINNED_EVENT_THREAD_SQL = """
        SELECT 1 FROM threads
        WHERE id = $1::uuid
          AND execution_lane = 'pinned'
          AND agent_id = $2::uuid
          AND runtime_generation = $3::uuid
          AND runtime_attach_token = $4::uuid
          AND runtime_retirement_token IS NULL
          AND status IN ('created', 'active', 'awaiting_user', 'suspended')
          AND events_epoch = $5::integer
        FOR NO KEY UPDATE
    """
    # Durable control batches use an explicit transaction and acquire these
    # locks in the same threads -> queue/agent -> request order as admission.
    # The ordinary stateless writer intentionally keeps its relaxed trailing
    # flush fence; only control results require a live leased state.
    _LOCK_STATELESS_CONTROL_THREAD_SQL = """
        SELECT 1 FROM threads
        WHERE id = $1::uuid
          AND execution_lane = 'stateless'
          AND agent_id IS NULL
        FOR NO KEY UPDATE
    """
    _LOCK_PINNED_CONTROL_THREAD_SQL = """
        SELECT 1 FROM threads
        WHERE id = $1::uuid
          AND execution_lane = 'pinned'
          AND agent_id = $2::uuid
          AND runtime_generation = $3::uuid
          AND runtime_attach_token IS NOT DISTINCT FROM $4::uuid
          AND runtime_retirement_token IS NULL
        FOR NO KEY UPDATE
    """
    _LOCK_STATELESS_CONTROL_QUEUE_SQL = """
        SELECT 1 FROM run_queue
        WHERE unit_id = $1::uuid
          AND unit_kind = 'session_turn'
          AND state = 'leased'
          AND lease_token = $2::bigint
        FOR SHARE
    """
    _LOCK_PINNED_CONTROL_AGENT_SQL = """
        SELECT 1 FROM agents
        WHERE id = $1::uuid AND thread_id = $2::uuid
        FOR SHARE
    """
    _LOCK_CONTROL_REQUESTS_SQL = """
        SELECT COUNT(*)::bigint
        FROM (
            SELECT request.id
            FROM thread_control_requests request
            WHERE request.id = ANY($1::uuid[])
              AND request.thread_id = $2::uuid
              AND request.accepted_agent_id IS NOT DISTINCT FROM $3::uuid
              AND ($3::uuid IS NULL OR request.runtime_generation = $4::uuid)
              AND request.outcome IS NULL
            FOR SHARE
        ) locked_requests
    """
    _LOCK_INTERRUPT_REQUESTS_SQL = """
        SELECT COUNT(*)::bigint
        FROM (
            SELECT request.id
            FROM thread_interrupt_requests request
            WHERE request.id = ANY($1::uuid[])
              AND request.thread_id = $2::uuid
              AND request.accepted_lease_token = $3::bigint
              AND request.outcome IS NULL
            FOR SHARE
        ) locked_requests
    """
    # Pinned lane keeps the exact pre-M3 statement (and its 4-arg call shape).
    _INSERT_BATCH_SQL = _INSERT_BATCH_SQL_TEMPLATE.format(lease_fence="")

    def __init__(
        self,
        *,
        postgres_conn: Any,
        thread_id: str,
        epoch: int,
        on_terminal_failure: Callable[[list[_QueuedPersistentEvent], str], None],
        queue_maxsize: int = _EVENT_WRITER_QUEUE_MAXSIZE,
        batch_size: int = _EVENT_WRITER_BATCH_SIZE,
        state_max_attempts: int = _EVENT_WRITER_STATE_MAX_ATTEMPTS,
        retry_base_s: float = _EVENT_WRITER_RETRY_BASE_S,
        lease: Any = None,
        pinned_agent_id: Optional[str] = None,
        pinned_runtime_generation: Optional[str] = None,
        pinned_runtime_attach_token: Optional[str] = None,
    ) -> None:
        if queue_maxsize < 1:
            raise ValueError("event writer queue_maxsize must be positive")
        if batch_size < 1:
            raise ValueError("event writer batch_size must be positive")
        if state_max_attempts < 1:
            raise ValueError("event writer state_max_attempts must be positive")
        if int(epoch) < 0:
            raise ValueError("event writer epoch must be non-negative")

        self.postgres_conn = postgres_conn
        self.thread_id = thread_id
        self.epoch = int(epoch)
        # Stateless lane: a live LeaseHandle (src/api/lease_context.py). The
        # flush reads unit_id/lease_token AT FLUSH TIME, so an affinity
        # re-claim of the same thread (new token, same writer) keeps flushing
        # under the current claim without a writer swap. None = pinned lane =
        # the exact pre-M3 statement and call shape.
        self._lease = lease
        self._pinned_agent_id = pinned_agent_id
        self._pinned_runtime_generation = pinned_runtime_generation
        self._pinned_runtime_attach_token = pinned_runtime_attach_token
        self._insert_sql = (
            self._INSERT_BATCH_SQL
            if lease is None
            else self._INSERT_BATCH_SQL_TEMPLATE.format(
                lease_fence=self._LEASE_FENCE_SQL
            )
        )
        self._on_terminal_failure = on_terminal_failure
        self._queue: asyncio.Queue[_QueuedPersistentEvent] = asyncio.Queue(
            maxsize=queue_maxsize
        )
        self._batch_size = batch_size
        self._state_max_attempts = state_max_attempts
        self._retry_base_s = max(0.0, retry_base_s)
        self._task: Optional[asyncio.Task] = None
        self._closing = False
        self._last_enqueued_cursor: Optional[tuple[int, int]] = None
        self._active_batch: tuple[_QueuedPersistentEvent, ...] = ()
        self._deferred_event: Optional[_QueuedPersistentEvent] = None

    def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("event writer already started")
        self._task = asyncio.create_task(
            self._run(),
            name=f"thread-event-writer-{self.thread_id}",
        )

    def enqueue(self, event: _QueuedPersistentEvent) -> bool:
        """Queue an event without blocking the agent's output loop."""

        if self._task is None or self._closing or self._task.done():
            self._notify_terminal_failure([event], "writer_unavailable")
            self._fail_receipt(event, "writer_unavailable")
            return False

        cursor = (event.epoch, event.seq)
        if (
            self._last_enqueued_cursor is not None
            and cursor <= self._last_enqueued_cursor
        ):
            logger.error(
                "thread_events rejected non-monotonic cursor "
                "(thread=%s previous=%s next=%s kind=%s)",
                self.thread_id,
                self._last_enqueued_cursor,
                cursor,
                event.kind,
            )
            self._notify_terminal_failure([event], "non_monotonic_cursor")
            self._fail_receipt(event, "non_monotonic_cursor")
            return False
        self._last_enqueued_cursor = cursor

        try:
            self._queue.put_nowait(event)
            return True
        except asyncio.QueueFull:
            # The live WS frame was already delivered. Journal overflow is
            # therefore a replay/reconciliation failure, not a reason to block
            # the model loop. Canvas invalidations are never lost silently:
            # their callback emits a direct, non-journaled reconcile control.
            logger.warning(
                "thread_events queue full — dropping journal frame "
                "(thread=%s epoch=%d seq=%d kind=%s)",
                self.thread_id,
                event.epoch,
                event.seq,
                event.kind,
            )
            self._notify_terminal_failure([event], "queue_overflow")
            self._fail_receipt(event, "queue_overflow")
            return False

    async def close(self, timeout_s: float = _EVENT_WRITER_CLOSE_TIMEOUT_S) -> None:
        """Stop accepting events, drain queued writes, and stop the worker."""

        task = self._task
        if task is None:
            self._closing = True
            return

        self._closing = True
        timed_out = False
        try:
            await asyncio.wait_for(self._queue.join(), timeout=max(0.0, timeout_s))
        except asyncio.TimeoutError:
            timed_out = True

        failed_on_shutdown: list[_QueuedPersistentEvent] = []
        if timed_out:
            # Snapshot the in-flight batch before cancellation clears it, then
            # drain the still-queued tail. The callback is intentionally live
            # only; attempting another journal write here could hang teardown.
            failed_on_shutdown.extend(self._active_batch)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            if self._deferred_event is not None:
                failed_on_shutdown.append(self._deferred_event)
                self._deferred_event = None
                self._queue.task_done()
            while True:
                try:
                    failed_on_shutdown.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
                else:
                    self._queue.task_done()
            if failed_on_shutdown:
                logger.warning(
                    "thread_events writer drain timed out (thread=%s pending=%d)",
                    self.thread_id,
                    len(failed_on_shutdown),
                )
                self._notify_terminal_failure(
                    failed_on_shutdown, "writer_shutdown_timeout"
                )
                for event in failed_on_shutdown:
                    self._fail_receipt(event, "writer_shutdown_timeout")
        else:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self._task = None

    async def _run(self) -> None:
        while True:
            if self._deferred_event is not None:
                first = self._deferred_event
                self._deferred_event = None
            else:
                first = await self._queue.get()
            batch = [first]
            # A strict owner-fenced control result must never share a database
            # batch with ordinary trailing journal frames. If its live lease
            # fence fails, only that acknowledgement may fail; ordinary frames
            # retain the established epoch/token trailing-flush contract.
            if first.control_request_id is None and first.interrupt_request_id is None:
                while len(batch) < self._batch_size:
                    try:
                        candidate = self._queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    if (
                        candidate.control_request_id is not None
                        or candidate.interrupt_request_id is not None
                    ):
                        self._deferred_event = candidate
                        break
                    batch.append(candidate)

            self._active_batch = tuple(batch)
            try:
                failure = await self._write_with_retry(batch)
                for event in batch:
                    if failure is None:
                        self._resolve_receipt(event)
                    else:
                        self._fail_receipt(event, failure)
            finally:
                self._active_batch = ()
                for _event in batch:
                    self._queue.task_done()

    async def _write_with_retry(
        self, batch: list[_QueuedPersistentEvent]
    ) -> Optional[str]:
        # The writer owns one epoch; a frame stamped with any other epoch in
        # its stream is a sequencing bug (the rewind path swaps writers around
        # its bump precisely so this cannot happen legitimately). Fail loudly
        # and stop — a writer that cannot trust its stream must not keep
        # writing.
        if any(event.epoch != self.epoch for event in batch):
            self._closing = True
            logger.error(
                "thread_events batch spans epochs — writer owns %d, saw %s; "
                "stopping writer (thread=%s first_seq=%d last_seq=%d)",
                self.epoch,
                sorted({event.epoch for event in batch}),
                self.thread_id,
                batch[0].seq,
                batch[-1].seq,
            )
            self._notify_terminal_failure(batch, "epoch_mismatch")
            return "epoch_mismatch"

        attempts = (
            self._state_max_attempts
            if any(_requires_bounded_event_retry(event.kind) for event in batch)
            else 1
        )
        for attempt in range(1, attempts + 1):
            try:
                inserted = await self._write_batch(batch)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if attempt < attempts:
                    logger.warning(
                        "thread_events batch write failed; retrying "
                        "(thread=%s first_seq=%d last_seq=%d attempt=%d/%d): %s",
                        self.thread_id,
                        batch[0].seq,
                        batch[-1].seq,
                        attempt,
                        attempts,
                        exc,
                    )
                    if self._retry_base_s:
                        await asyncio.sleep(self._retry_base_s * (2 ** (attempt - 1)))
                    continue

                logger.warning(
                    "thread_events batch write failed terminally "
                    "(thread=%s first_seq=%d last_seq=%d attempts=%d): %s",
                    self.thread_id,
                    batch[0].seq,
                    batch[-1].seq,
                    attempts,
                    exc,
                )
                self._notify_terminal_failure(batch, "write_failed")
                return "write_failed"

            if inserted < len(batch):
                # The fence rejected the batch: threads.events_epoch moved
                # past this writer (rewind, reaper steal, or a competing
                # runtime) — or, stateless lane, the run_queue lease token
                # moved (a successor claimed the unit). Deterministic, not
                # retryable — stop the writer so a superseded runtime can
                # never keep appending into a dead (or worse, the
                # successor's) generation.
                self._closing = True
                logger.error(
                    "thread_events writer fenced out — fence rejected the "
                    "flush (events_epoch or run_queue lease moved past this "
                    "writer); stopping "
                    "(thread=%s writer_epoch=%d inserted=%d batch=%d "
                    "first_seq=%d last_seq=%d lease=%s)",
                    self.thread_id,
                    self.epoch,
                    inserted,
                    len(batch),
                    batch[0].seq,
                    batch[-1].seq,
                    (
                        f"{self._lease.unit_id}:{self._lease.lease_token}"
                        if self._lease is not None
                        else "none"
                    ),
                )
                self._notify_terminal_failure(batch, "epoch_fenced")
                return "epoch_fenced"
            return None

    async def _write_batch(self, batch: list[_QueuedPersistentEvent]) -> int:
        """Flush one batch through the fenced statement; return inserted count."""

        rows = [
            {
                "epoch": event.epoch,
                "seq": event.seq,
                "kind": event.kind,
                "payload": event.payload,
                "control_request_id": event.control_request_id,
                "interrupt_request_id": event.interrupt_request_id,
            }
            for event in batch
        ]
        args: list[Any] = [self.thread_id, json.dumps(rows), self.epoch]
        control_events = [
            event for event in batch if event.control_request_id is not None
        ]
        interrupt_events = [
            event for event in batch if event.interrupt_request_id is not None
        ]
        if control_events and interrupt_events:
            return 0
        if any(
            event.control_request_id is not None
            and event.interrupt_request_id is not None
            for event in batch
        ):
            return 0
        control_agent_id: Optional[str] = None
        control_lease_token: Optional[int] = None
        if control_events and self._lease is not None:
            control_tokens = {event.control_lease_token for event in control_events}
            if (
                None in control_tokens
                or len(control_tokens) != 1
                or any(event.control_agent_id is not None for event in control_events)
            ):
                return 0
            # A control acknowledgement is fenced on the immutable token held
            # when the owner consumed the request, never the mutable warm
            # LeaseHandle a successor may already have refreshed by flush time.
            control_lease_token = control_tokens.pop()
        elif control_events:
            control_agents = {event.control_agent_id for event in control_events}
            if (
                None in control_agents
                or len(control_agents) != 1
                or self._pinned_agent_id not in control_agents
                or any(
                    event.control_lease_token is not None for event in control_events
                )
            ):
                return 0
            control_agent_id = control_agents.pop()

        interrupt_lease_token: Optional[int] = None
        interrupt_accepted_lease_token: Optional[int] = None
        if interrupt_events:
            # The inbox is stateless-only. ``interrupt_lease_token`` is the
            # CURRENT writer authority. Usually it is also the immutable
            # admission token. Explicit stale recovery instead locks an older
            # accepted request while still fencing the INSERT on the current
            # live queue token; a warm LeaseHandle is never authority here.
            if self._lease is None:
                return 0
            interrupt_tokens = {
                event.interrupt_lease_token for event in interrupt_events
            }
            if None in interrupt_tokens or len(interrupt_tokens) != 1:
                return 0
            interrupt_lease_token = interrupt_tokens.pop()
            accepted_tokens = {
                (
                    event.interrupt_accepted_lease_token
                    if event.interrupt_accepted_lease_token is not None
                    else event.interrupt_lease_token
                )
                for event in interrupt_events
            }
            stale_modes = {
                bool(event.interrupt_stale_recovery) for event in interrupt_events
            }
            if None in accepted_tokens or len(accepted_tokens) != 1:
                return 0
            if len(stale_modes) != 1:
                return 0
            interrupt_accepted_lease_token = accepted_tokens.pop()
            stale_recovery = stale_modes.pop()
            if stale_recovery:
                if interrupt_accepted_lease_token >= interrupt_lease_token:
                    return 0
            elif interrupt_accepted_lease_token != interrupt_lease_token:
                return 0

        ordinary_lease_unit_id: Optional[str] = None
        ordinary_lease_token: Optional[int] = None
        if not control_events and not interrupt_events and self._lease is not None:
            # Snapshot the warm handle before the first await.  A later claim
            # may repoint the shared handle, but one flush must authorize one
            # coherent (unit_id, token) pair, and a handle for another thread
            # is never authority to write this writer's journal.
            ordinary_lease_unit_id = self._lease.unit_id
            ordinary_lease_token = int(self._lease.lease_token)
            if ordinary_lease_unit_id is None or str(ordinary_lease_unit_id) != str(
                self.thread_id
            ):
                return 0

        async with self.postgres_conn.acquire() as conn:
            if control_events:
                request_ids = [
                    str(event.control_request_id) for event in control_events
                ]
                if len(set(request_ids)) != len(request_ids):
                    return 0
                async with conn.transaction():
                    if control_lease_token is not None:
                        thread_fenced = await conn.fetchval(
                            self._LOCK_STATELESS_CONTROL_THREAD_SQL,
                            self.thread_id,
                        )
                        if thread_fenced is None:
                            return 0
                        queue_fenced = await conn.fetchval(
                            self._LOCK_STATELESS_CONTROL_QUEUE_SQL,
                            self.thread_id,
                            control_lease_token,
                        )
                        if queue_fenced is None:
                            return 0
                        accepted_agent_id = None
                    else:
                        if (
                            control_agent_id is None
                            or self._pinned_runtime_generation is None
                        ):
                            return 0
                        thread_fenced = await conn.fetchval(
                            self._LOCK_PINNED_CONTROL_THREAD_SQL,
                            self.thread_id,
                            control_agent_id,
                            self._pinned_runtime_generation,
                            self._pinned_runtime_attach_token,
                        )
                        if thread_fenced is None:
                            return 0
                        agent_fenced = await conn.fetchval(
                            self._LOCK_PINNED_CONTROL_AGENT_SQL,
                            control_agent_id,
                            self.thread_id,
                        )
                        if agent_fenced is None:
                            return 0
                        accepted_agent_id = control_agent_id
                    locked_requests = int(
                        await conn.fetchval(
                            self._LOCK_CONTROL_REQUESTS_SQL,
                            request_ids,
                            self.thread_id,
                            accepted_agent_id,
                            (
                                self._pinned_runtime_generation
                                if accepted_agent_id is not None
                                else None
                            ),
                        )
                        or 0
                    )
                    if locked_requests != len(request_ids):
                        return 0
                    inserted = await conn.fetchval(self._INSERT_BATCH_SQL, *args)
            elif interrupt_events:
                request_ids = [
                    str(event.interrupt_request_id) for event in interrupt_events
                ]
                if len(set(request_ids)) != len(request_ids):
                    return 0
                if interrupt_lease_token is None:
                    return 0
                async with conn.transaction():
                    thread_fenced = await conn.fetchval(
                        self._LOCK_STATELESS_CONTROL_THREAD_SQL,
                        self.thread_id,
                    )
                    if thread_fenced is None:
                        return 0
                    queue_fenced = await conn.fetchval(
                        self._LOCK_STATELESS_CONTROL_QUEUE_SQL,
                        self.thread_id,
                        interrupt_lease_token,
                    )
                    if queue_fenced is None:
                        return 0
                    locked_requests = int(
                        await conn.fetchval(
                            self._LOCK_INTERRUPT_REQUESTS_SQL,
                            request_ids,
                            self.thread_id,
                            interrupt_accepted_lease_token,
                        )
                        or 0
                    )
                    if locked_requests != len(request_ids):
                        return 0
                    inserted = await conn.fetchval(self._INSERT_BATCH_SQL, *args)
            else:
                insert_sql = self._insert_sql
                ordinary_args = list(args)
                if self._lease is None:
                    if self._pinned_runtime_generation is None:
                        # Deliberate rolling-deploy compatibility for an old
                        # orchestrator that did not advertise runtime identity.
                        # Protected sessions reject that attach earlier; once
                        # the contract is present every pinned event below is
                        # exact-owner fenced.
                        inserted = await conn.fetchval(insert_sql, *ordinary_args)
                    else:
                        if (
                            self._pinned_agent_id is None
                            or self._pinned_runtime_attach_token is None
                        ):
                            return 0
                        # Lock order matches retirement/admission: thread,
                        # reciprocal agent, then INSERT/HWM. A G1 batch waiting
                        # here cannot commit after End/Resume G2; settlement
                        # either waits for this transaction or installs the
                        # retirement token first and makes this return zero.
                        async with conn.transaction():
                            thread_fenced = await conn.fetchval(
                                self._LOCK_PINNED_EVENT_THREAD_SQL,
                                self.thread_id,
                                self._pinned_agent_id,
                                self._pinned_runtime_generation,
                                self._pinned_runtime_attach_token,
                                self.epoch,
                            )
                            if thread_fenced is None:
                                return 0
                            agent_fenced = await conn.fetchval(
                                self._LOCK_PINNED_CONTROL_AGENT_SQL,
                                self._pinned_agent_id,
                                self.thread_id,
                            )
                            if agent_fenced is None:
                                return 0
                            inserted = await conn.fetchval(
                                insert_sql,
                                *ordinary_args,
                            )
                else:
                    # Ordinary trailing journal frames retain the M3 rule:
                    # token equality is sufficient after completion, but the
                    # exact unit remains bound to this writer.  Lock/validate
                    # thread epoch first, then queue identity, then perform the
                    # existing guarded INSERT/HWM update in one transaction.
                    if ordinary_lease_unit_id is None or ordinary_lease_token is None:
                        return 0
                    ordinary_args.extend([ordinary_lease_unit_id, ordinary_lease_token])
                    async with conn.transaction():
                        thread_fenced = await conn.fetchval(
                            self._LOCK_STATELESS_EVENT_THREAD_SQL,
                            self.thread_id,
                            self.epoch,
                        )
                        if thread_fenced is None:
                            return 0
                        queue_fenced = await conn.fetchval(
                            self._LOCK_STATELESS_EVENT_QUEUE_SQL,
                            ordinary_lease_unit_id,
                            ordinary_lease_token,
                        )
                        if queue_fenced is None:
                            return 0
                        inserted = await conn.fetchval(insert_sql, *ordinary_args)
        return int(inserted or 0)

    @staticmethod
    def _resolve_receipt(event: _QueuedPersistentEvent) -> None:
        receipt = event.receipt
        if receipt is not None and not receipt.done():
            receipt.set_result((event.epoch, event.seq))

    @staticmethod
    def _fail_receipt(event: _QueuedPersistentEvent, reason: str) -> None:
        receipt = event.receipt
        if receipt is not None and not receipt.done():
            receipt.set_exception(
                EventJournalUnavailable(
                    f"durable owner result journal persistence failed: {reason}"
                )
            )

    def _notify_terminal_failure(
        self, events: list[_QueuedPersistentEvent], reason: str
    ) -> None:
        try:
            self._on_terminal_failure(events, reason)
        except Exception as exc:
            logger.warning(
                "thread_events terminal-failure callback failed "
                "(thread=%s reason=%s): %s",
                self.thread_id,
                reason,
                exc,
            )


def _fan_out_live_frame(frame: Dict[str, Any]) -> None:
    """Deliver an already-built frame through the current subscriber registry."""
    _session_transport.fan_out_frame(_subscribers, frame, logger=logger)


def _event_persistence_failed(
    events: list[_QueuedPersistentEvent], reason: str
) -> None:
    """Force live Canvas clients back to authoritative REST state.

    This control frame deliberately has no ``_seq`` and never re-enters the
    journal. Ordinary event failures remain best-effort and are covered by the
    existing history/reconnect behavior; Canvas state invalidations require an
    explicit reconciliation signal when a live control subscriber exists.
    """

    canvas_events = [
        event for event in events if _requires_bounded_event_retry(event.kind)
    ]
    if not canvas_events:
        return

    canvas_id = "main"
    payload = canvas_events[-1].payload
    if isinstance(payload, dict) and isinstance(payload.get("canvas_id"), str):
        canvas_id = payload["canvas_id"]
    _fan_out_live_frame(
        {
            "method": "canvas.reconcile_required",
            "params": {
                "canvas_id": canvas_id,
                "reason": reason,
            },
        }
    )


def _broadcast_frame(
    method: str,
    params: Dict[str, Any],
    *,
    control_request_id: Optional[str] = None,
    control_lease_token: Optional[int] = None,
    control_agent_id: Optional[str] = None,
    interrupt_request_id: Optional[str] = None,
    interrupt_lease_token: Optional[int] = None,
    interrupt_accepted_lease_token: Optional[int] = None,
    interrupt_stale_recovery: bool = False,
    durable_receipt: bool = False,
) -> Optional[asyncio.Future]:
    """Fan out one frame and enqueue its ordered journal write.

    The ordinary broadcast path stays non-blocking and best-effort. A control
    result asks for a receipt future and is not acknowledged in its inbox
    until the ordered writer resolves that future after the INSERT commits.
    Both paths allocate through this one function, so an inbox result can
    never race a second sequence allocator.

    Phase 2 (event log): allocates the next seq synchronously and stamps
    `(_events_epoch, seq)` into the frame's params under `_seq`. One
    per-runtime writer persists FIFO batches, so the SSE poller cannot advance
    past an earlier event whose independent write is still in flight.
    """
    global _next_seq
    receipt: Optional[asyncio.Future] = None
    if durable_receipt:
        receipt = asyncio.get_running_loop().create_future()
    _next_seq += 1
    seq = _next_seq
    epoch = _events_epoch
    # Stamp the cursor onto the frame so existing WS subscribers see the
    # same (epoch, seq) the event log records — keeps WS and SSE paths
    # consistent under reconnect.
    params_with_cursor = {**params, "_seq": [epoch, seq]}
    frame = {"method": method, "params": params_with_cursor}

    def _publish_live() -> None:
        _fan_out_live_frame(frame)
        # Mirror notification-worthy events to NATS so the orchestrator's
        # bridge can fan them out to the SSE notification feed. Non-blocking,
        # non-fatal on failure.
        if method in _NOTIFICATION_METHODS:
            asyncio.create_task(emit_session_event(method, params))

    if receipt is None:
        _publish_live()
    else:
        # A durable control acknowledgement is authoritative only after its
        # journal INSERT commits. Do not let a live subscriber observe a frame
        # that a failed writer would be unable to replay after reload.
        def _publish_committed(done: asyncio.Future) -> None:
            if done.cancelled():
                return
            try:
                done.result()
            except Exception:
                return
            _publish_live()

        receipt.add_done_callback(_publish_committed)

    # Queue one immutable journal snapshot. The loop stays non-blocking, while
    # the sole writer serializes database visibility for every sequence.
    if _session is not None and _session.postgres_conn is not None:
        event = _QueuedPersistentEvent(
            epoch=epoch,
            seq=seq,
            kind=method,
            payload=json.loads(json.dumps(_safe_serialize(params))),
            control_request_id=control_request_id,
            control_lease_token=control_lease_token,
            control_agent_id=control_agent_id,
            interrupt_request_id=interrupt_request_id,
            interrupt_lease_token=interrupt_lease_token,
            interrupt_accepted_lease_token=interrupt_accepted_lease_token,
            interrupt_stale_recovery=interrupt_stale_recovery,
            receipt=receipt,
        )
        writer = _event_writer
        if writer is None or writer.thread_id != _thread_id:
            logger.error(
                "thread_events writer unavailable (thread=%s epoch=%d seq=%d kind=%s)",
                _thread_id,
                epoch,
                seq,
                method,
            )
            _event_persistence_failed([event], "writer_unavailable")
            _OrderedPersistentEventWriter._fail_receipt(event, "writer_unavailable")
        else:
            writer.enqueue(event)
    elif receipt is not None:
        receipt.set_exception(
            EventJournalUnavailable(
                "durable owner result journal persistence failed: session_unavailable"
            )
        )
    return receipt


def _broadcast(method: str, params: Dict[str, Any]) -> None:
    """Non-blocking live fanout plus best-effort ordered journal persistence."""

    _broadcast_frame(method, params)


async def _broadcast_durable(
    method: str,
    params: Dict[str, Any],
    *,
    control_request_id: str,
    lease_token: Optional[int],
    agent_id: Optional[str],
) -> tuple[int, int]:
    """Broadcast and wait until the owner-fenced journal INSERT commits."""

    if (lease_token is None) == (agent_id is None):
        raise ValueError("exactly one durable control owner is required")

    receipt = _broadcast_frame(
        method,
        params,
        control_request_id=control_request_id,
        control_lease_token=lease_token,
        control_agent_id=agent_id,
        durable_receipt=True,
    )
    if receipt is None:  # pragma: no cover - defensive; durable always creates one
        raise EventJournalUnavailable("control result receipt was not created")
    return await receipt


async def _broadcast_interrupt_durable(
    method: str,
    params: Dict[str, Any],
    *,
    interrupt_request_id: str,
    lease_token: int,
    accepted_lease_token: Optional[int] = None,
    stale_recovery: bool = False,
) -> tuple[int, int]:
    """Wait for one immutable-lease interrupt receipt to commit.

    ``_broadcast_frame`` allocates and enqueues synchronously before this
    coroutine reaches its first suspension. The RAM interrupt signal can
    therefore be followed immediately by this call without allowing the graph
    to consume the flag and finish ahead of receipt allocation.
    """

    receipt = _broadcast_frame(
        method,
        params,
        interrupt_request_id=interrupt_request_id,
        interrupt_lease_token=int(lease_token),
        interrupt_accepted_lease_token=(
            int(accepted_lease_token) if accepted_lease_token is not None else None
        ),
        interrupt_stale_recovery=bool(stale_recovery),
        durable_receipt=True,
    )
    if receipt is None:  # pragma: no cover - defensive; durable always creates one
        raise EventJournalUnavailable("interrupt result receipt was not created")
    return await receipt


async def _broadcast_event_durable(
    method: str,
    params: Dict[str, Any],
) -> tuple[int, int]:
    """Wait for an ordinary current-owner journal frame to commit."""

    receipt = _broadcast_frame(method, params, durable_receipt=True)
    if receipt is None:  # pragma: no cover - defensive; durable always creates one
        raise EventJournalUnavailable("durable event receipt was not created")
    return await receipt


async def _durable_session_control_modes() -> tuple[str, str]:
    """Read journal-authoritative welcome scalars for a pinned WS client.

    A second tab can connect while a control result is between journal commit
    and request finalization. Start with the first-class columns and overlay
    only fully matching pending receipts, exactly like the orchestrator REST
    snapshot. Falling back to RAM is safe because RAM changes only after
    durable finalization.
    """

    if _session is None:
        return "supervised", "auto"
    fallback = (_session.permission_mode, _session.narration_mode)
    if _session.postgres_conn is None or _thread_id is None:
        return fallback

    from shared.thread_controls import applied_control_scalar

    try:
        async with _session.postgres_conn.acquire() as conn:
            async with conn.transaction(isolation="repeatable_read", readonly=True):
                row = await conn.fetchrow(
                    "SELECT permission_mode, narration_mode FROM threads "
                    "WHERE id = $1::uuid",
                    _thread_id,
                )
                if row is None:
                    return fallback
                permission_mode = str(row["permission_mode"] or "supervised")
                narration_mode = (
                    str(row["narration_mode"])
                    if row["narration_mode"] is not None
                    else fallback[1]
                )
                receipts = await conn.fetch(
                    "SELECT request.id AS request_id, request.client_request_id, "
                    "request.request_seq, "
                    "request.verb, request.payload AS request_payload, "
                    "event.kind AS event_kind, event.payload AS event_payload "
                    "FROM thread_control_requests request "
                    "JOIN thread_events event "
                    "ON event.thread_id = request.thread_id "
                    "AND event.control_request_id = request.id "
                    "WHERE request.thread_id = $1::uuid "
                    "AND request.outcome IS NULL "
                    "ORDER BY request.request_seq",
                    _thread_id,
                )
        for receipt in receipts:
            scalar = applied_control_scalar(
                request_id=receipt["request_id"],
                client_request_id=receipt["client_request_id"],
                request_seq=int(receipt["request_seq"]),
                verb=str(receipt["verb"]),
                request_payload=receipt["request_payload"],
                event_kind=str(receipt["event_kind"]),
                event_payload=receipt["event_payload"],
            )
            if scalar is None:
                continue
            column, value = scalar
            if column == "permission_mode":
                permission_mode = value
            elif column == "narration_mode":
                narration_mode = value
        return permission_mode, narration_mode
    except Exception:
        logger.warning(
            "durable control scalar read failed for WS welcome; using "
            "post-finalization RAM",
            exc_info=True,
        )
        return fallback


def _registered_pinned_agent_id() -> Optional[str]:
    """Return the orchestrator-issued DB identity, never a hostname/config id."""

    if _orchestrator_client is None or _orchestrator_client.agent_id is None:
        return None
    try:
        from uuid import UUID

        return str(UUID(str(_orchestrator_client.agent_id)))
    except (TypeError, ValueError, AttributeError):
        return None


_PINNED_SESSION_RECIPIENT_KEY = "_recipient"


def _pinned_session_recipient_capable() -> bool:
    """Return whether this process can validate an orchestrator envelope."""

    return bool(
        _registered_pinned_agent_id()
        and str(
            getattr(_orchestrator_client, "dispatch_process_generation", None) or ""
        ).strip()
    )


def _pinned_session_recipient_refusal(
    body: Any,
    *,
    expected_thread_id: str | None,
) -> JSONResponse | None:
    """Fail closed before a session mutation reaches process-local state."""

    recipient = (
        body.get(_PINNED_SESSION_RECIPIENT_KEY) if isinstance(body, dict) else None
    )
    actual_pod_uid = str(os.environ.get("POD_UID") or "").strip() or None
    exact_contract = bool(
        isinstance(body, dict)
        and type(body.get("pinned_runtime_generation_contract")) is int
        and body["pinned_runtime_generation_contract"] == 1
    )
    if recipient is None and not (
        actual_pod_uid or (exact_contract and _pinned_session_recipient_capable())
    ):
        # Explicit local/Docker compatibility lane. It cannot advertise the
        # recipient capability unless registration supplied a process epoch.
        return None
    if not isinstance(recipient, dict):
        return JSONResponse(
            {"error": "recipient_authority_mismatch", "retryable": True},
            status_code=503,
            headers={"Retry-After": "5"},
        )

    try:
        parsed_recipient = PinnedSessionRecipient.model_validate(recipient)
    except (TypeError, ValueError):
        return JSONResponse(
            {"error": "recipient_authority_mismatch", "retryable": True},
            status_code=503,
            headers={"Retry-After": "5"},
        )

    actual_agent_id = _registered_pinned_agent_id()
    actual_generation = str(
        getattr(_orchestrator_client, "dispatch_process_generation", None) or ""
    ).strip()
    if not pinned_session_recipient_matches(
        parsed_recipient,
        thread_id=expected_thread_id,
        agent_id=actual_agent_id,
        pod_uid=actual_pod_uid,
        process_generation=actual_generation,
    ):
        return JSONResponse(
            {"error": "recipient_authority_mismatch", "retryable": True},
            status_code=503,
            headers={"Retry-After": "5"},
        )
    return None


def _control_receipt_matches(request: Any, receipt: Any) -> bool:
    """Reject a malformed/cross-linked receipt instead of phantom-applying it."""

    from shared.thread_controls import control_receipt_result

    return (
        control_receipt_result(
            request_id=request.id,
            client_request_id=request.client_request_id,
            request_seq=request.request_seq,
            verb=request.verb,
            request_payload=request.payload,
            event_kind=receipt.kind,
            event_payload=receipt.payload,
        )
        is not None
    )


def _describe_control_request(request: Any) -> tuple[str, str, Optional[str]]:
    """Validate one request without mutating RAM or any durable state."""

    mode = str(request.payload.get("mode") or "")
    if request.verb == "mode.set" and mode in {
        "supervised",
        "auto_accept",
        "autonomous",
    }:
        return "mode.changed", "applied", None
    if request.verb == "narration.set" and mode in {"silent", "verbose", "auto"}:
        return "narration.changed", "applied", None
    if request.verb == "workspace.undo" and request.payload == {}:
        return "files.restored", "applied", None
    return "control.rejected", "rejected", "unsupported_control"


async def _finalize_durable_control(
    request: Any,
    *,
    lease_token: Optional[int],
    agent_id: Optional[str],
    outcome: str,
    error_code: Optional[str] = None,
) -> str:
    """Atomically terminalize a receipt and its stateless consumed watermark."""

    from shared.thread_controls import finalize_control_request

    if _session is None or _session.postgres_conn is None:
        raise EventJournalUnavailable("control finalization lost its session")
    async with _session.postgres_conn.acquire() as conn:
        async with conn.transaction():
            return await finalize_control_request(
                conn,
                request_id=request.id,
                lease_token=lease_token,
                agent_id=agent_id,
                runtime_generation=(
                    _session_runtime_generation if agent_id is not None else None
                ),
                runtime_attach_token=(
                    _session_runtime_attach_token if agent_id is not None else None
                ),
                outcome=outcome,
                error_code=error_code,
            )


async def _reconcile_durable_control_scalars(
    *,
    lease_token: Optional[int],
    agent_id: Optional[str],
) -> None:
    """Converge RAM from first-class scalars under the current owner fence.

    This closes the ambiguous-finalizer window: PostgreSQL may commit the
    scalar/request/watermark transaction just as the awaiting task is
    cancelled. A warm affinity reuse must not keep the old in-process mode
    merely because the now-terminal request no longer appears in the inbox.
    """

    from shared.thread_controls import owner_fence_current

    if (lease_token is None) == (agent_id is None):
        raise ValueError("exactly one control owner credential is required")
    if _session is None or _session.postgres_conn is None or _thread_id is None:
        raise EventJournalUnavailable("control scalar reconciliation lost session")

    async with _session.postgres_conn.acquire() as conn:
        async with conn.transaction():
            fenced = await owner_fence_current(
                conn,
                thread_id=_thread_id,
                lease_token=lease_token,
                agent_id=agent_id,
                runtime_generation=(
                    _session_runtime_generation if agent_id is not None else None
                ),
                runtime_attach_token=(
                    _session_runtime_attach_token if agent_id is not None else None
                ),
            )
            if not fenced:
                raise ControlInboxBlocked(
                    "control scalar reconciliation lost current owner"
                )
            row = await conn.fetchrow(
                "SELECT permission_mode, narration_mode FROM threads "
                "WHERE id = $1::uuid",
                _thread_id,
            )
    if row is None or _session is None:
        raise ControlInboxBlocked("control scalar reconciliation lost thread")
    _session.permission_mode = str(row["permission_mode"] or "supervised")
    # NULL is the migration sentinel for a legacy inherited narration value;
    # retain the mode already loaded from resolved config until a control (or
    # creation-time materialization) writes a first-class value.
    if row["narration_mode"] is not None:
        _session.narration_mode = str(row["narration_mode"])


async def _set_pinned_control_admission(
    *, agent_id: str, open_for_admission: bool
) -> bool:
    """Set the durable admission credential under the reciprocal binding.

    Teardown closes this before its last drain, serializing with orchestrator
    admission on the threads row. A new pinned owner reopens it only after its
    writer/session are attached. ``False`` means this runtime no longer owns
    the binding; database failures raise and must not be treated as closure.
    """

    if _session is None or _session.postgres_conn is None or _thread_id is None:
        raise EventJournalUnavailable("control admission gate lost session")
    runtime_generation = _session_runtime_generation
    runtime_attach_token = _session_runtime_attach_token
    if runtime_generation is None:
        return False
    async with _session.postgres_conn.acquire() as conn:
        async with conn.transaction():
            thread = await conn.fetchrow(
                "SELECT agent_id, execution_lane, status, runtime_generation, "
                "runtime_attach_token, runtime_retirement_token FROM threads "
                "WHERE id = $1::uuid FOR UPDATE",
                _thread_id,
            )
            if (
                thread is None
                or str(thread["execution_lane"] or "") != "pinned"
                or str(thread["agent_id"] or "") != str(agent_id)
                or str(thread["runtime_generation"] or "") != runtime_generation
                or str(thread["runtime_attach_token"] or "")
                != str(runtime_attach_token or "")
                or thread["runtime_retirement_token"] is not None
                or (
                    open_for_admission
                    and str(thread["status"] or "") in {"ending", "ended", "suspended"}
                )
            ):
                return False
            reciprocal = await conn.fetchval(
                "SELECT 1 FROM agents WHERE id = $1::uuid "
                "AND thread_id = $2::uuid FOR SHARE",
                agent_id,
                _thread_id,
            )
            if reciprocal is None:
                return False
            updated = await conn.fetchval(
                "UPDATE threads SET control_admission_agent_id = "
                "CASE WHEN $3::boolean THEN $2::uuid ELSE NULL END "
                "WHERE id = $1::uuid AND agent_id = $2::uuid "
                "AND runtime_generation = $4::uuid "
                "AND runtime_attach_token IS NOT DISTINCT FROM $5::uuid "
                "AND runtime_retirement_token IS NULL RETURNING id",
                _thread_id,
                agent_id,
                bool(open_for_admission),
                runtime_generation,
                runtime_attach_token,
            )
            return updated is not None


async def _close_pinned_control_inbox(*, agent_id: str) -> bool:
    """Close admission and drain controls without trapping a stale owner.

    The binding can move after the first close transaction commits but before
    the final drain locks its first request.  In that case the successor owns
    the request and this process must continue local cleanup without attempting
    the lifecycle CAS.  A drain failure while the binding is still ours stays
    fatal: otherwise a real journal/finalization failure could strand work.
    """

    if not await _set_pinned_control_admission(
        agent_id=agent_id,
        open_for_admission=False,
    ):
        return False
    try:
        await _drain_thread_controls(agent_id=agent_id)
    except ControlInboxBlocked:
        if await _set_pinned_control_admission(
            agent_id=agent_id,
            open_for_admission=False,
        ):
            raise
        logger.info(
            "Pinned control owner moved during final drain (thread=%s agent=%s)",
            _thread_id,
            agent_id,
        )
        return False
    return True


def _apply_control_request(request: Any) -> tuple[str, str, Optional[str]]:
    """Converge RAM after durable owner-fenced terminalization.

    Validation is intentionally side-effect free and happens before the
    journal write. This function is called only after finalization proves the
    immutable journal owner and publishes the first-class DB scalar.
    """

    if _session is None:
        raise EventJournalUnavailable("control apply lost its session")

    event_kind, outcome, error_code = _describe_control_request(request)
    if outcome != "applied":
        return event_kind, outcome, error_code

    mode = str(request.payload.get("mode") or "")
    if request.verb == "mode.set":
        _session.permission_mode = mode
        # Permission-card retirement stays owned by the existing gate/turn
        # cleanup path. It is not part of this control acknowledgement:
        # rows have no lease identity yet, so sweeping them here cannot be
        # made crash-recoverable without risking a successor's active gate.
    elif request.verb == "narration.set":
        _session.narration_mode = mode
    return event_kind, outcome, error_code


async def _drain_thread_controls(
    *,
    lease_token: Optional[int] = None,
    agent_id: Optional[str] = None,
) -> int:
    """Drain the exact owner's pending inbox in request_seq order.

    There is no durable ``claimed`` state. Idempotent scalar assignments may
    safely repeat after a crash before the result write. If the result write
    committed but finalization did not, the unique journal receipt is found
    first. The current owner validates and finalizes it, then idempotently
    re-converges RAM without allocating another event sequence.
    """

    from shared.thread_controls import (
        adopt_next_pinned_control_request,
        control_receipt_result,
        fetch_control_receipt,
        fetch_next_control_request,
        owner_fence_current,
    )

    if (lease_token is None) == (agent_id is None):
        raise ValueError("exactly one control owner credential is required")

    runtime_generation = _session_runtime_generation if agent_id is not None else None
    runtime_attach_token = (
        _session_runtime_attach_token if agent_id is not None else None
    )
    if agent_id is not None and runtime_generation is None:
        raise ControlInboxBlocked("pinned control owner lacks runtime generation")

    applied = 0
    async with _control_drain_lock:
        while True:
            if _session is None or _session.postgres_conn is None or _thread_id is None:
                return applied
            if lease_token is not None:
                handle = _current_lease_var.get()
                if (
                    handle is None
                    or handle.unit_id != str(_thread_id)
                    or handle.lease_token != int(lease_token)
                    or handle.lost.is_set()
                ):
                    if handle is not None:
                        handle.lost.set()
                    raise ControlInboxBlocked(f"control owner lost lease {lease_token}")

            started = time.perf_counter()
            async with _session.postgres_conn.acquire() as conn:
                async with conn.transaction():
                    owns_thread = await owner_fence_current(
                        conn,
                        thread_id=_thread_id,
                        lease_token=lease_token,
                        agent_id=agent_id,
                        runtime_generation=runtime_generation,
                        runtime_attach_token=runtime_attach_token,
                    )
                    if not owns_thread:
                        if lease_token is not None:
                            handle = _current_lease_var.get()
                            if handle is not None:
                                handle.lost.set()
                        raise ControlInboxBlocked(
                            "control owner fence is no longer current"
                        )
                    if agent_id is not None:
                        await adopt_next_pinned_control_request(
                            conn,
                            thread_id=_thread_id,
                            agent_id=agent_id,
                            runtime_generation=runtime_generation,
                            runtime_attach_token=runtime_attach_token,
                        )
                    request = await fetch_next_control_request(
                        conn,
                        thread_id=_thread_id,
                        lease_token=lease_token,
                        agent_id=agent_id,
                        runtime_generation=runtime_generation,
                        runtime_attach_token=runtime_attach_token,
                    )
                    if request is None:
                        return applied
                    if (
                        agent_id is not None
                        and str(request.runtime_generation) != runtime_generation
                    ):
                        raise ControlInboxBlocked(
                            "pinned control request belongs to another runtime generation"
                        )
                    receipt = await fetch_control_receipt(
                        conn,
                        thread_id=_thread_id,
                        request_id=request.id,
                    )

            if receipt is not None:
                receipt_result = control_receipt_result(
                    request_id=request.id,
                    client_request_id=request.client_request_id,
                    request_seq=request.request_seq,
                    verb=request.verb,
                    request_payload=request.payload,
                    event_kind=receipt.kind,
                    event_payload=receipt.payload,
                )
                if receipt_result is None:
                    raise ControlInboxBlocked(
                        "control receipt does not match its durable request: "
                        f"{request.id}"
                    )
                outcome, error_code, _scalar = receipt_result
                result = await _finalize_durable_control(
                    request,
                    lease_token=lease_token,
                    agent_id=agent_id,
                    outcome=outcome,
                    error_code=error_code,
                )
                if result not in {outcome, "already_terminal"}:
                    if result == "lost_owner" and lease_token is not None:
                        handle = _current_lease_var.get()
                        if handle is not None:
                            handle.lost.set()
                    raise ControlInboxBlocked(
                        "control receipt recovery failed: "
                        f"request={request.id} result={result}"
                    )
                if result == outcome:
                    _apply_control_request(request)
                else:
                    await _reconcile_durable_control_scalars(
                        lease_token=lease_token,
                        agent_id=agent_id,
                    )
                applied += 1
                logger.info(
                    "session-control timing: thread=%s verb=%s seq=%d "
                    "recovered_receipt=true total=%.3fs",
                    _thread_id,
                    request.verb,
                    request.request_seq,
                    time.perf_counter() - started,
                )
                continue

            event_kind, outcome, error_code = _describe_control_request(request)
            effect_params: Dict[str, Any] = {}
            if outcome == "applied" and request.verb == "workspace.undo":
                if _session is None:
                    raise ControlInboxBlocked(
                        f"workspace undo lost its session: {request.id}"
                    )
                try:
                    effect_params = await _session.undo_turn(
                        control_request_id=str(request.id)
                    )
                except WorkspaceUndoUnavailable as exc:
                    # Tier/history refusal is known before any mutation and can
                    # therefore receive a durable rejected acknowledgement.
                    event_kind = "control.rejected"
                    outcome = "rejected"
                    error_code = exc.code
                except WorkspaceUndoRetryable as exc:
                    # Git may already contain the effect marker. Leave the
                    # request pending so this or a successor claimant recovers
                    # it; never journal a rejection after possible mutation.
                    raise ControlInboxBlocked(
                        f"workspace undo remains retryable: {request.id}"
                    ) from exc
                except Exception as exc:
                    raise ControlInboxBlocked(
                        f"workspace undo failed before acknowledgement: {request.id}"
                    ) from exc
            params: Dict[str, Any] = {
                "request_id": str(request.id),
                "client_request_id": str(request.client_request_id),
                "request_seq": request.request_seq,
                "method": request.verb,
            }
            if outcome == "applied" and request.verb in {
                "mode.set",
                "narration.set",
            }:
                params["mode"] = str(request.payload.get("mode") or "")
            elif outcome == "applied" and request.verb == "workspace.undo":
                params.update(effect_params)
            else:
                params["error_code"] = error_code

            journal_started = time.perf_counter()
            try:
                epoch, seq = await _broadcast_durable(
                    event_kind,
                    params,
                    control_request_id=str(request.id),
                    lease_token=lease_token,
                    agent_id=agent_id,
                )
            except EventJournalUnavailable as exc:
                raise ControlInboxBlocked(
                    f"control journal acknowledgement failed: {request.id}"
                ) from exc
            journal_seconds = time.perf_counter() - journal_started
            result = await _finalize_durable_control(
                request,
                lease_token=lease_token,
                agent_id=agent_id,
                outcome=outcome,
                error_code=error_code,
            )
            if result not in {outcome, "already_terminal"}:
                if result == "lost_owner" and lease_token is not None:
                    handle = _current_lease_var.get()
                    if handle is not None:
                        handle.lost.set()
                raise ControlInboxBlocked(
                    "control finalization failed: "
                    f"request={request.id} journal={epoch}/{seq} result={result}"
                )
            if result == outcome:
                _apply_control_request(request)
            else:
                await _reconcile_durable_control_scalars(
                    lease_token=lease_token,
                    agent_id=agent_id,
                )
            applied += 1
            logger.info(
                "session-control timing: thread=%s verb=%s seq=%d "
                "recovered_receipt=false journal=%.3fs total=%.3fs",
                _thread_id,
                request.verb,
                request.request_seq,
                journal_seconds,
                time.perf_counter() - started,
            )


async def _drain_current_thread_controls() -> int:
    """Authoritative pre-gate drain for whichever owner currently serves."""

    if _control_owner_lease_token is not None:
        await _reconcile_durable_control_scalars(
            lease_token=_control_owner_lease_token,
            agent_id=None,
        )
        return await _drain_thread_controls(
            lease_token=_control_owner_lease_token,
        )
    if _control_owner_agent_id is not None:
        await _reconcile_durable_control_scalars(
            lease_token=None,
            agent_id=_control_owner_agent_id,
        )
        return await _drain_thread_controls(agent_id=_control_owner_agent_id)
    return 0


async def _control_watcher_loop(
    *,
    postgres_conn: Any,
    thread_id: str,
    stop: asyncio.Event,
    lease_token: Optional[int],
    agent_id: Optional[str],
) -> None:
    """LISTEN for latency, with a bounded poll so delivery is never correctness."""

    wake = asyncio.Event()

    def _on_notify(_conn: Any, _pid: int, _channel: str, payload: str) -> None:
        try:
            notice = json.loads(payload)
        except Exception:
            return
        if str(notice.get("thread_id") or "") == thread_id:
            wake.set()

    try:
        async with postgres_conn.acquire() as listener:
            await listener.add_listener(_CONTROL_NOTIFY_CHANNEL, _on_notify)
            try:
                # Register first, then drain: a request committed in the
                # start-up window is either visible to this read or wakes the
                # following one.
                while not stop.is_set():
                    try:
                        await _drain_thread_controls(
                            lease_token=lease_token,
                            agent_id=agent_id,
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        logger.warning(
                            "session-control drain failed; request remains pending "
                            "for retry (thread=%s)",
                            thread_id,
                            exc_info=True,
                        )
                    wake.clear()
                    if stop.is_set():
                        break
                    # Stop is a first-class waiter, not just a flag checked
                    # after the five-second poll.  Stateless control-only
                    # claims start and stop this watcher back-to-back; direct
                    # task cancellation while asyncpg is entering LISTEN can
                    # leave its connection-lost future unobserved.  A normal
                    # stop now lets the listener unregister and return its
                    # connection cooperatively.
                    wake_waiter = asyncio.create_task(wake.wait())
                    stop_waiter = asyncio.create_task(stop.wait())
                    waiters = {wake_waiter, stop_waiter}
                    try:
                        await asyncio.wait(
                            waiters,
                            timeout=_CONTROL_POLL_SECONDS,
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                    finally:
                        for waiter in waiters:
                            if not waiter.done():
                                waiter.cancel()
                        await asyncio.gather(*waiters, return_exceptions=True)
            finally:
                with suppress(Exception):
                    await listener.remove_listener(_CONTROL_NOTIFY_CHANNEL, _on_notify)
    except asyncio.CancelledError:
        raise
    except Exception:
        # A listener connection is only a latency optimization. Fall back to
        # bounded polling on fresh pool connections until the owner stops.
        logger.warning(
            "session-control LISTEN unavailable; polling (thread=%s)",
            thread_id,
            exc_info=True,
        )
        while not stop.is_set():
            try:
                await _drain_thread_controls(
                    lease_token=lease_token,
                    agent_id=agent_id,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    "session-control poll failed (thread=%s)",
                    thread_id,
                    exc_info=True,
                )
            try:
                await asyncio.wait_for(stop.wait(), timeout=_CONTROL_POLL_SECONDS)
            except asyncio.TimeoutError:
                pass


async def _start_thread_control_watcher(
    *,
    lease_token: Optional[int] = None,
    agent_id: Optional[str] = None,
) -> int:
    """Take ownership, synchronously drain, then watch for later admissions."""

    global _control_watcher_task, _control_watcher_stop
    global _control_owner_lease_token, _control_owner_agent_id

    if (lease_token is None) == (agent_id is None):
        raise ValueError("exactly one control owner credential is required")
    # A restart/re-attach may enter with the same exact binding while the old
    # watcher still advertised capability. Close first, before cancelling that
    # watcher, so a failed initial drain cannot leave public admission open
    # with no consumer. A different successor credential is unaffected.
    if agent_id is not None:
        await _set_pinned_control_admission(
            agent_id=str(agent_id),
            open_for_admission=False,
        )
    await _stop_thread_control_watcher()
    if _session is None or _session.postgres_conn is None or _thread_id is None:
        raise EventJournalUnavailable("cannot start control owner without a session")

    _control_owner_lease_token = int(lease_token) if lease_token is not None else None
    _control_owner_agent_id = str(agent_id) if agent_id is not None else None
    try:
        if _control_owner_agent_id is not None:
            # Existing work can be consumed while the public gate is closed.
            # Only an inbox-capable owner that completed this first drain
            # advertises readiness to new REST admissions. Drain again after
            # opening to cover the commit window before LISTEN registration.
            drained = await _drain_current_thread_controls()
            if not await _set_pinned_control_admission(
                agent_id=_control_owner_agent_id,
                open_for_admission=True,
            ):
                raise ControlInboxBlocked(
                    "cannot reopen control admission without the exact pinned owner"
                )
            drained += await _drain_current_thread_controls()
        else:
            drained = await _drain_current_thread_controls()
    except BaseException:
        if _control_owner_agent_id is not None:
            with suppress(BaseException):
                await _set_pinned_control_admission(
                    agent_id=_control_owner_agent_id,
                    open_for_admission=False,
                )
        _control_owner_lease_token = None
        _control_owner_agent_id = None
        raise

    stop = asyncio.Event()
    _control_watcher_stop = stop
    _control_watcher_task = asyncio.create_task(
        _control_watcher_loop(
            postgres_conn=_session.postgres_conn,
            thread_id=str(_thread_id),
            stop=stop,
            lease_token=_control_owner_lease_token,
            agent_id=_control_owner_agent_id,
        ),
        name=f"thread-control-watcher-{str(_thread_id)[:8]}",
    )
    return drained


async def _stop_thread_control_watcher() -> None:
    """Stop and await the owner consumer before lease completion/release."""

    global _control_watcher_task, _control_watcher_stop
    global _control_owner_lease_token, _control_owner_agent_id

    task = _control_watcher_task
    stop = _control_watcher_stop
    # This helper is also used by partial-attach cleanup. Never cancel the
    # only pinned consumer while its public capability is still advertised;
    # teardown's earlier close+final-drain makes this an idempotent second
    # close on the normal path.
    if _control_owner_agent_id is not None:
        await _set_pinned_control_admission(
            agent_id=_control_owner_agent_id,
            open_for_admission=False,
        )
    if stop is not None:
        stop.set()
    if task is not None:
        try:
            await asyncio.wait_for(
                asyncio.shield(task), timeout=_CONTROL_STOP_GRACE_SECONDS
            )
        except asyncio.CancelledError:
            if not task.cancelled():
                raise
        except asyncio.TimeoutError:
            # A wedged DB/listener must not pin lease completion forever.
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
    _control_watcher_task = None
    _control_watcher_stop = None
    _control_owner_lease_token = None
    _control_owner_agent_id = None


async def _finalize_durable_interrupt(
    request: Any,
    *,
    lease_token: int,
    outcome: str,
    mode: Optional[str],
    error_code: Optional[str],
    accepted_lease_token: Optional[int] = None,
    stale_recovery: bool = False,
) -> str:
    """Terminalize one interrupt from its committed journal receipt."""

    from shared.thread_interrupts import finalize_interrupt_request

    if _session is None or _session.postgres_conn is None or _thread_id is None:
        return "lost_owner"
    async with _session.postgres_conn.acquire() as conn:
        async with conn.transaction():
            return await finalize_interrupt_request(
                conn,
                request_id=request.id,
                thread_id=_thread_id,
                lease_token=int(lease_token),
                target_turn_id=int(request.target_turn_id),
                outcome=outcome,
                mode=mode,
                error_code=error_code,
                accepted_lease_token=accepted_lease_token,
                stale_recovery=stale_recovery,
            )


async def _drain_thread_interrupts(*, lease_token: int, target_turn_id: int) -> int:
    """Consume pending interrupts for one exact live lease/turn pair.

    The RAM signal happens synchronously before allocating the durable receipt.
    Only a committed receipt may terminalize the request. Receipt recovery
    deliberately skips the RAM mutation: repeating a historical hard signal
    after its target await already unwound could hit unrelated work inside the
    same turn.
    """

    from shared.thread_interrupts import (
        fetch_interrupt_receipt,
        fetch_next_interrupt_request,
        interrupt_receipt_result,
        owner_fence_current,
    )

    applied_count = 0
    async with _interrupt_drain_lock:
        while True:
            if _session is None or _session.postgres_conn is None or _thread_id is None:
                return applied_count
            handle = _current_lease_var.get()
            if (
                handle is None
                or handle.unit_id != str(_thread_id)
                or handle.lease_token != int(lease_token)
                or handle.lost.is_set()
            ):
                if handle is not None:
                    handle.lost.set()
                raise InterruptInboxBlocked(f"interrupt owner lost lease {lease_token}")

            started = time.perf_counter()
            async with _session.postgres_conn.acquire() as conn:
                async with conn.transaction():
                    owns_thread = await owner_fence_current(
                        conn,
                        thread_id=_thread_id,
                        lease_token=int(lease_token),
                    )
                    if not owns_thread:
                        handle.lost.set()
                        raise InterruptInboxBlocked(
                            "interrupt owner fence is no longer current"
                        )
                    request = await fetch_next_interrupt_request(
                        conn,
                        thread_id=_thread_id,
                        lease_token=int(lease_token),
                        target_turn_id=int(target_turn_id),
                    )
                    if request is None:
                        return applied_count
                    receipt = await fetch_interrupt_receipt(
                        conn,
                        thread_id=_thread_id,
                        request_id=request.id,
                    )

            if receipt is not None:
                receipt_result = interrupt_receipt_result(
                    request_id=request.id,
                    client_request_id=request.client_request_id,
                    target_turn_id=request.target_turn_id,
                    event_kind=receipt.kind,
                    event_payload=receipt.payload,
                )
                if receipt_result is None:
                    raise InterruptInboxBlocked(
                        "interrupt receipt does not match its durable request: "
                        f"{request.id}"
                    )
                outcome, mode, error_code = receipt_result
                result = await _finalize_durable_interrupt(
                    request,
                    lease_token=int(lease_token),
                    outcome=outcome,
                    mode=mode,
                    error_code=error_code,
                )
                if result not in {outcome, "already_terminal"}:
                    if result == "lost_owner":
                        handle.lost.set()
                    raise InterruptInboxBlocked(
                        "interrupt receipt recovery failed: "
                        f"request={request.id} result={result}"
                    )
                applied_count += 1
                logger.info(
                    "session-interrupt timing: thread=%s turn=%d "
                    "recovered_receipt=true total=%.3fs",
                    _thread_id,
                    request.target_turn_id,
                    time.perf_counter() - started,
                )
                continue

            mode = _signal_interrupt_for_turn(request.target_turn_id)
            outcome = "applied" if mode is not None else "rejected"
            error_code = None if mode is not None else "target_turn_not_active"
            params: Dict[str, Any] = {
                "request_id": str(request.id),
                "client_request_id": str(request.client_request_id),
                "target_turn_id": request.target_turn_id,
                "applied": outcome == "applied",
            }
            if mode is not None:
                params["mode"] = mode
            else:
                params["error_code"] = error_code

            journal_started = time.perf_counter()
            try:
                epoch, seq = await _broadcast_interrupt_durable(
                    "interrupt.ack",
                    params,
                    interrupt_request_id=str(request.id),
                    lease_token=int(lease_token),
                )
            except EventJournalUnavailable as exc:
                raise InterruptInboxBlocked(
                    f"interrupt journal acknowledgement failed: {request.id}"
                ) from exc
            journal_seconds = time.perf_counter() - journal_started
            result = await _finalize_durable_interrupt(
                request,
                lease_token=int(lease_token),
                outcome=outcome,
                mode=mode,
                error_code=error_code,
            )
            if result not in {outcome, "already_terminal"}:
                if result == "lost_owner":
                    handle.lost.set()
                raise InterruptInboxBlocked(
                    "interrupt finalization failed: "
                    f"request={request.id} journal={epoch}/{seq} result={result}"
                )
            applied_count += 1
            logger.info(
                "session-interrupt timing: thread=%s turn=%d applied=%s "
                "recovered_receipt=false journal=%.3fs total=%.3fs",
                _thread_id,
                request.target_turn_id,
                outcome == "applied",
                journal_seconds,
                time.perf_counter() - started,
            )


async def _rotate_thread_interrupt_recovery_epoch(*, lease_token: int) -> int:
    """Fence an abandoned turn before its successor emits any turn output.

    A claim can beat the reaper's post-steal journal transaction. The attach
    then initially seeds an ordered writer in the abandoned epoch. Close that
    writer first, lock the stateless thread then the exact CURRENT queue token,
    bump the epoch, and publish a fresh writer at seq 0.
    Any old process or partial-delta flush is now rejected by the epoch/token
    fence before this owner journals recovery receipts or starts a new turn.
    """

    global _event_writer, _events_epoch, _next_seq

    if _session is None or _session.postgres_conn is None or _thread_id is None:
        raise EventJournalUnavailable("interrupt epoch recovery lost its session")
    handle = _current_lease_var.get()
    if (
        handle is None
        or handle.unit_id != str(_thread_id)
        or handle.lease_token != int(lease_token)
        or handle.lost.is_set()
    ):
        raise InterruptInboxBlocked("interrupt epoch recovery lost local lease")

    old_writer = _event_writer
    if old_writer is None or old_writer.thread_id != str(_thread_id):
        raise EventJournalUnavailable("interrupt epoch recovery has no writer")
    _event_writer = None
    await old_writer.close()

    old_epoch = _events_epoch
    try:
        async with _session.postgres_conn.acquire() as conn:
            async with conn.transaction():
                # Two statements make the cross-writer lock order explicit;
                # a joined FOR clause does not guarantee acquisition order.
                thread = await conn.fetchrow(
                    "SELECT 1 FROM threads WHERE id = $1::uuid "
                    "AND execution_lane = 'stateless' AND agent_id IS NULL "
                    "FOR UPDATE",
                    _thread_id,
                )
                if thread is None:
                    handle.lost.set()
                    raise InterruptInboxBlocked(
                        "interrupt epoch recovery thread is no longer stateless"
                    )
                queue = await conn.fetchrow(
                    "SELECT 1 FROM run_queue "
                    "WHERE unit_id = $1::uuid AND unit_kind = 'session_turn' "
                    "AND state = 'leased' AND lease_token = $2::bigint "
                    "FOR UPDATE",
                    _thread_id,
                    int(lease_token),
                )
                if queue is None:
                    handle.lost.set()
                    raise InterruptInboxBlocked(
                        "interrupt epoch recovery queue token is no longer current"
                    )
                from shared.session_permission_retirement import (
                    retire_stale_stateless_permissions,
                )

                permission_retirement = await retire_stale_stateless_permissions(
                    conn,
                    thread_id=str(_thread_id),
                    retired_lease_token=int(lease_token) - 1,
                    successor_lease_token=int(lease_token),
                    reason="lease_expired",
                    epoch_already_bumped=False,
                )
                if permission_retirement.epoch_bumped:
                    new_epoch = permission_retirement.receipts[0].epoch
                    if any(
                        receipt.epoch != int(new_epoch)
                        for receipt in permission_retirement.receipts
                    ):
                        raise RuntimeError(
                            "permission recovery receipts span journal epochs"
                        )
                    recovered_hwm = permission_retirement.receipts[-1].seq
                else:
                    new_epoch = await _event_journal.bump_epoch(
                        conn,
                        thread_id=str(_thread_id),
                    )
                    recovered_hwm = 0
    except BaseException:
        # A failed transaction exit can be commit-ambiguous. Never recreate a
        # writer in the old epoch by guessing that the bump rolled back; leave
        # this token to expire and let the next exact claimant resolve the DB.
        handle.lost.set()
        raise

    _events_epoch = int(new_epoch)
    _next_seq = int(recovered_hwm)
    try:
        new_writer = _OrderedPersistentEventWriter(
            postgres_conn=_session.postgres_conn,
            thread_id=str(_thread_id),
            epoch=_events_epoch,
            on_terminal_failure=_event_persistence_failed,
            lease=handle,
        )
        new_writer.start()
    except BaseException:
        handle.lost.set()
        raise
    _event_writer = new_writer
    logger.info(
        "session-owner recovery epoch: thread=%s token=%d old=%d new=%d permissions=%d",
        _thread_id,
        lease_token,
        old_epoch,
        _events_epoch,
        permission_retirement.count,
    )
    return _events_epoch


async def _reconcile_stale_thread_interrupts(
    *, lease_token: int
) -> tuple[int, Optional[int]]:
    """Settle pending requests from superseded leases without signalling RAM.

    The current exact owner supplies journal authority and holds the queue row
    against a concurrent reaper/claim. The request retains its immutable OLD
    accepted token; ``applied_lease_token`` stores that token solely because
    the frozen schema requires equality. It does not attribute a
    successor/system-writer owner-loss receipt to the expired process. The
    successor never signals its own RAM from the stale request.
    """

    from shared.thread_interrupts import (
        fetch_interrupt_receipt,
        consume_applied_interrupt_input_live,
        fetch_stale_interrupt_requests,
        interrupt_receipt_result,
        owner_fence_current_for_update,
    )

    reconciled = 0
    async with _interrupt_drain_lock:
        if _session is None or _session.postgres_conn is None or _thread_id is None:
            return reconciled, None
        while True:
            handle = _current_lease_var.get()
            if (
                handle is None
                or handle.unit_id != str(_thread_id)
                or handle.lease_token != int(lease_token)
                or handle.lost.is_set()
            ):
                if handle is not None:
                    handle.lost.set()
                raise InterruptInboxBlocked(
                    f"stale-interrupt reconciler lost lease {lease_token}"
                )

            async with _session.postgres_conn.acquire() as conn:
                async with conn.transaction():
                    owns_thread = await owner_fence_current_for_update(
                        conn,
                        thread_id=_thread_id,
                        lease_token=int(lease_token),
                    )
                    if not owns_thread:
                        handle.lost.set()
                        raise InterruptInboxBlocked(
                            "stale-interrupt owner fence is no longer current"
                        )
                    requests = await fetch_stale_interrupt_requests(
                        conn,
                        thread_id=_thread_id,
                        current_lease_token=int(lease_token),
                    )
                    stale_permissions = bool(
                        await conn.fetchval(
                            _STALE_PERMISSION_EXISTS_SQL,
                            _thread_id,
                            int(lease_token),
                        )
                    )
                    no_interrupts = not requests
                    if no_interrupts:
                        queue = await conn.fetchrow(
                            "SELECT consumed_seq FROM run_queue "
                            "WHERE unit_id = $1::uuid AND state = 'leased' "
                            "AND lease_token = $2::bigint",
                            _thread_id,
                            int(lease_token),
                        )
                        result = (
                            reconciled,
                            (
                                int(queue["consumed_seq"])
                                if queue is not None
                                and queue["consumed_seq"] is not None
                                else None
                            ),
                        )
                    else:
                        receipts = {
                            request.id: await fetch_interrupt_receipt(
                                conn,
                                thread_id=_thread_id,
                                request_id=request.id,
                            )
                            for request in requests
                        }

            if no_interrupts:
                if stale_permissions:
                    # The discovery transaction has released its thread/queue
                    # locks before rotation takes a fresh connection. Holding
                    # them while awaiting the second connection self-deadlocks.
                    await _rotate_thread_interrupt_recovery_epoch(
                        lease_token=int(lease_token)
                    )
                return result

            # A stale row proves the claim beat the reaper's post-steal
            # boundary. Rotate before any recovery frame or successor output.
            await _rotate_thread_interrupt_recovery_epoch(lease_token=int(lease_token))

            results: list[tuple[Any, str, Optional[str], Optional[str]]] = []
            for request in requests:
                receipt = receipts[request.id]
                if receipt is not None:
                    receipt_result = interrupt_receipt_result(
                        request_id=request.id,
                        client_request_id=request.client_request_id,
                        target_turn_id=request.target_turn_id,
                        event_kind=receipt.kind,
                        event_payload=receipt.payload,
                    )
                    if receipt_result is None:
                        raise InterruptInboxBlocked(
                            "stale interrupt receipt does not match its request: "
                            f"{request.id}"
                        )
                    outcome, mode, error_code = receipt_result
                elif request.outcome is not None:
                    raise InterruptInboxBlocked(
                        "terminal stale interrupt is missing its durable receipt: "
                        f"{request.id}"
                    )
                else:
                    # Exact durable admission is itself stop intent. The old
                    # owner may have signalled RAM and died before allocating
                    # its receipt; rejecting here would re-run a turn the user
                    # already stopped. The successor never signals its own
                    # RAM, but durably settles the old target as a hard stop.
                    outcome = "applied"
                    mode = "hard"
                    error_code = None
                    params: Dict[str, Any] = {
                        "request_id": str(request.id),
                        "client_request_id": str(request.client_request_id),
                        "target_turn_id": request.target_turn_id,
                        "applied": True,
                        "mode": mode,
                        "reason": "owner_lost",
                        "owner_loss_reason": "lease_expired",
                    }
                    try:
                        await _broadcast_interrupt_durable(
                            "interrupt.ack",
                            params,
                            interrupt_request_id=str(request.id),
                            lease_token=int(lease_token),
                            accepted_lease_token=int(request.accepted_lease_token),
                            stale_recovery=True,
                        )
                    except EventJournalUnavailable as exc:
                        raise InterruptInboxBlocked(
                            f"stale interrupt journal settlement failed: {request.id}"
                        ) from exc
                results.append((request, outcome, mode, error_code))

            # Match the reaper's generation shape: all correlated acks become
            # durable first, then one terminal boundary closes the abandoned
            # partial turn before any input watermark moves.
            requests_by_target: Dict[int, list[Any]] = {}
            for request in requests:
                requests_by_target.setdefault(int(request.target_turn_id), []).append(
                    request
                )
            for target_turn_id, target_requests in requests_by_target.items():
                await _broadcast_event_durable(
                    "turn.interrupted",
                    {
                        "reason": "lease_expired",
                        "recovered_by_lease_token": int(lease_token),
                        "target_turn_id": target_turn_id,
                        "interrupt_request_ids": [
                            str(request.id) for request in target_requests
                        ],
                        "accepted_lease_tokens": sorted(
                            {
                                int(request.accepted_lease_token)
                                for request in target_requests
                            }
                        ),
                    },
                )

            for request, outcome, mode, error_code in results:
                if request.outcome == "applied":
                    # Receipt-before-finalizer recovery may also encounter an
                    # already-terminal applied row written by older code. The
                    # same group helper stamps/reuses the exactly-once marker.
                    async with _session.postgres_conn.acquire() as conn:
                        async with conn.transaction():
                            if not await owner_fence_current_for_update(
                                conn,
                                thread_id=_thread_id,
                                lease_token=int(lease_token),
                            ):
                                handle.lost.set()
                                raise InterruptInboxBlocked(
                                    "stale applied interrupt lost current owner"
                                )
                            consumed = await consume_applied_interrupt_input_live(
                                conn,
                                thread_id=_thread_id,
                                current_lease_token=int(lease_token),
                                accepted_lease_token=int(request.accepted_lease_token),
                                target_turn_id=int(request.target_turn_id),
                                request_id=request.id,
                            )
                            if consumed is None:
                                raise InterruptInboxBlocked(
                                    "stale applied interrupt could not settle input"
                                )
                    result = "already_terminal"
                elif request.outcome is not None:
                    # Only unmarked applied terminal rows are selected.
                    raise InterruptInboxBlocked(
                        "unexpected terminal stale interrupt candidate: "
                        f"{request.id}/{request.outcome}"
                    )
                else:
                    result = await _finalize_durable_interrupt(
                        request,
                        lease_token=int(lease_token),
                        outcome=outcome,
                        mode=mode,
                        error_code=error_code,
                        accepted_lease_token=int(request.accepted_lease_token),
                        stale_recovery=True,
                    )
                if result not in {outcome, "already_terminal"}:
                    if result == "lost_owner":
                        handle.lost.set()
                    raise InterruptInboxBlocked(
                        "stale interrupt finalization failed: "
                        f"request={request.id} result={result}"
                    )
                reconciled += 1
                logger.info(
                    "session-interrupt stale recovery: thread=%s request=%s "
                    "accepted_token=%d current_token=%d receipt=%s",
                    _thread_id,
                    request.id,
                    request.accepted_lease_token,
                    lease_token,
                    receipts[request.id] is not None,
                )

            async with _session.postgres_conn.acquire() as conn:
                async with conn.transaction():
                    if not await owner_fence_current_for_update(
                        conn,
                        thread_id=_thread_id,
                        lease_token=int(lease_token),
                    ):
                        handle.lost.set()
                        raise InterruptInboxBlocked(
                            "stale-interrupt owner lost before watermark refresh"
                        )
                    queue = await conn.fetchrow(
                        "SELECT consumed_seq FROM run_queue "
                        "WHERE unit_id = $1::uuid AND state = 'leased' "
                        "AND lease_token = $2::bigint",
                        _thread_id,
                        int(lease_token),
                    )
                    return (
                        reconciled,
                        (
                            int(queue["consumed_seq"])
                            if queue is not None and queue["consumed_seq"] is not None
                            else None
                        ),
                    )


async def _interrupt_watcher_loop(
    *,
    postgres_conn: Any,
    thread_id: str,
    stop: asyncio.Event,
    lease_token: int,
    target_turn_id: int,
) -> None:
    """LISTEN for interrupt latency and poll for correctness."""

    wake = asyncio.Event()

    def _on_notify(_conn: Any, _pid: int, _channel: str, payload: str) -> None:
        try:
            notice = json.loads(payload)
        except Exception:
            return
        if str(notice.get("thread_id") or "") == thread_id:
            wake.set()

    async def _drain_once() -> None:
        try:
            await _drain_thread_interrupts(
                lease_token=lease_token,
                target_turn_id=target_turn_id,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning(
                "session-interrupt drain failed; request remains pending "
                "(thread=%s turn=%d)",
                thread_id,
                target_turn_id,
                exc_info=True,
            )

    try:
        async with postgres_conn.acquire() as listener:
            await listener.add_listener(_INTERRUPT_NOTIFY_CHANNEL, _on_notify)
            try:
                while not stop.is_set():
                    await _drain_once()
                    wake.clear()
                    if stop.is_set():
                        break
                    wake_waiter = asyncio.create_task(wake.wait())
                    stop_waiter = asyncio.create_task(stop.wait())
                    waiters = {wake_waiter, stop_waiter}
                    try:
                        await asyncio.wait(
                            waiters,
                            timeout=_INTERRUPT_POLL_SECONDS,
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                    finally:
                        for waiter in waiters:
                            if not waiter.done():
                                waiter.cancel()
                        await asyncio.gather(*waiters, return_exceptions=True)
            finally:
                with suppress(Exception):
                    await listener.remove_listener(
                        _INTERRUPT_NOTIFY_CHANNEL,
                        _on_notify,
                    )
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.warning(
            "session-interrupt LISTEN unavailable; polling (thread=%s turn=%d)",
            thread_id,
            target_turn_id,
            exc_info=True,
        )
        while not stop.is_set():
            await _drain_once()
            try:
                await asyncio.wait_for(
                    stop.wait(),
                    timeout=_INTERRUPT_POLL_SECONDS,
                )
            except asyncio.TimeoutError:
                pass


async def _start_thread_interrupt_watcher(
    *, lease_token: int, target_turn_id: int
) -> int:
    """Synchronously drain then watch one exact stateless turn."""

    async with _interrupt_watcher_lifecycle_lock:
        return await _start_thread_interrupt_watcher_locked(
            lease_token=lease_token,
            target_turn_id=target_turn_id,
        )


async def _start_thread_interrupt_watcher_locked(
    *, lease_token: int, target_turn_id: int
) -> int:
    """Start while holding ``_interrupt_watcher_lifecycle_lock``."""

    global _interrupt_watcher_task, _interrupt_watcher_stop
    global _interrupt_owner_lease_token, _interrupt_owner_turn_id

    await _stop_thread_interrupt_watcher_locked()
    if _session is None or _session.postgres_conn is None or _thread_id is None:
        raise EventJournalUnavailable("cannot start interrupt owner without a session")
    _interrupt_owner_lease_token = int(lease_token)
    _interrupt_owner_turn_id = int(target_turn_id)
    try:
        drained = await _drain_thread_interrupts(
            lease_token=_interrupt_owner_lease_token,
            target_turn_id=_interrupt_owner_turn_id,
        )
    except BaseException:
        _interrupt_owner_lease_token = None
        _interrupt_owner_turn_id = None
        raise

    stop = asyncio.Event()
    _interrupt_watcher_stop = stop
    _interrupt_watcher_task = asyncio.create_task(
        _interrupt_watcher_loop(
            postgres_conn=_session.postgres_conn,
            thread_id=str(_thread_id),
            stop=stop,
            lease_token=_interrupt_owner_lease_token,
            target_turn_id=_interrupt_owner_turn_id,
        ),
        name=f"thread-interrupt-watcher-{str(_thread_id)[:8]}",
    )
    return drained


async def _stop_thread_interrupt_watcher() -> None:
    """Stop and terminally join the consumer before any final drain.

    This join is intentionally unbounded. Cancelling a drain that is awaiting
    its ordered-writer receipt is ambiguous: the INSERT may still commit after
    cancellation, and a replacement drain could then re-signal RAM and try to
    write a duplicate receipt. Queue completion must wait until the original
    consumer has reached a durable terminal outcome instead.
    """

    async with _interrupt_watcher_lifecycle_lock:
        await _stop_thread_interrupt_watcher_locked()


async def _stop_thread_interrupt_watcher_locked() -> None:
    """Terminally join while holding the watcher lifecycle lock."""

    global _interrupt_watcher_task, _interrupt_watcher_stop
    global _interrupt_owner_lease_token, _interrupt_owner_turn_id

    task = _interrupt_watcher_task
    stop = _interrupt_watcher_stop
    if stop is not None:
        stop.set()
    terminal = task is None
    try:
        if task is not None:
            await asyncio.shield(task)
            terminal = True
    finally:
        # If our caller is cancelled, shield leaves the consumer running and
        # these owner globals intact. The queue transition is abandoned; a
        # later cleanup can join the same unambiguous task. A task that itself
        # failed/cancelled is terminal and may be cleared, while its exception
        # still propagates so the executor marks the lease lost.
        if task is not None and task.done():
            terminal = True
        if terminal:
            _interrupt_watcher_task = None
            _interrupt_watcher_stop = None
            _interrupt_owner_lease_token = None
            _interrupt_owner_turn_id = None


def _emit_citation_verdict(citation_id: int, verification_status: str) -> None:
    """Broadcast a citation verification verdict to the live session.

    Wired onto the session's ``ToolContext`` (``citation_verdict_callback``) so
    the CitationEngine's background verifier can push a ``pending`` →
    ``verified``/``failed`` transition the moment it lands, instead of the
    cockpit waiting for the next per-turn ``/citations`` refresh. The cockpit
    patches the one citation in place (see ``citation.verdict`` in
    persistent-chat.service). Worker jobs never set this, so the engine no-ops.

    Goes through ``_broadcast`` (WS subscribers + ``thread_events`` SSE replay).
    Not in ``_NOTIFICATION_METHODS`` — it's an in-session UI update, not a
    cross-session notification. Fire-and-forget; never raises into the verifier.
    """
    try:
        _broadcast(
            "citation.verdict",
            {
                "citation_id": int(citation_id),
                "verification_status": str(verification_status),
            },
        )
    except Exception as e:
        logger.debug("citation.verdict broadcast failed (non-fatal): %s", e)


def _emit_canvas_event(method: str, params: Dict[str, Any]) -> None:
    """Route a committed tool-originated Canvas invalidation through `_broadcast`."""
    if method not in {"canvas.updated", "canvas.cleared"}:
        logger.warning("Ignored unsupported Canvas callback method: %s", method)
        return
    try:
        _broadcast(method, params)
    except Exception as exc:
        # The orchestrator row is already committed. The Canvas tool treats this
        # callback as best-effort and the Cockpit reconciles from REST.
        logger.warning("Canvas invalidation broadcast failed: %s", exc)


async def _current_canvas_for_control() -> dict[str, Any] | None:
    """Load authoritative Canvas state with the attached delegated owner."""

    if _session is None or _thread_id is None or _session.tool_context is None:
        return None
    context = _session.tool_context
    user_id = str(context.user_id or "").strip()
    if not user_id:
        return None
    config_name = str(context.config.get("agent_id") or _config_path or "persistent")
    client = create_orchestrator_client_from_env(config_name, user_id=user_id)
    try:
        return await client.get_thread_canvas(_thread_id)
    finally:
        await client.close()


def _validated_canvas_control_state(
    data: Dict[str, Any], state: dict[str, Any] | None
) -> dict[str, Any] | None:
    """Match an untrusted control frame to exact authoritative Canvas state."""

    if state is None or data.get("canvas_id") != "main":
        return None
    revision = data.get("presentation_revision")
    if (
        isinstance(revision, bool)
        or not isinstance(revision, int)
        or revision != state.get("presentation_revision")
    ):
        return None
    if data.get("method") == "canvas.presentation_updated":
        return state

    source = state.get("source")
    if (
        not isinstance(source, dict)
        or source.get("type") != "workspace_file"
        or not isinstance(data.get("path"), str)
        or data["path"] != source.get("path")
        or not isinstance(data.get("source_version"), str)
        or data["source_version"] != state.get("source_version")
    ):
        return None
    return state


def _cancel_canvas_awareness(client_id: str) -> _CanvasAwarenessLease | None:
    lease = _canvas_awareness.pop(client_id, None)
    if lease is not None and lease.task is not asyncio.current_task():
        lease.task.cancel()
    return lease


def _fan_out_canvas_idle(client_id: str, params: Dict[str, Any]) -> None:
    _fan_out_live_frame(
        {
            "method": "canvas.user_idle",
            "params": {**params, "sender_id": client_id},
        }
    )


async def _expire_canvas_awareness(
    client_id: str, editing_session_id: str, params: Dict[str, Any]
) -> None:
    try:
        await asyncio.sleep(_CANVAS_AWARENESS_TTL_S)
    except asyncio.CancelledError:
        return
    lease = _canvas_awareness.get(client_id)
    if (
        lease is None
        or lease.task is not asyncio.current_task()
        or lease.params.get("editing_session_id") != editing_session_id
    ):
        return
    _canvas_awareness.pop(client_id, None)
    _fan_out_canvas_idle(client_id, params)


def _clear_canvas_awareness(client_id: str) -> None:
    """Expire every courtesy lease owned by one disconnected connection."""

    lease = _cancel_canvas_awareness(client_id)
    if lease is not None:
        _fan_out_canvas_idle(client_id, lease.params)
    for key in [key for key in _canvas_control_validation_at if key[0] == client_id]:
        _canvas_control_validation_at.pop(key, None)
    _canvas_source_updates.pop(client_id, None)
    _canvas_presentation_updates.pop(client_id, None)


def _clear_all_canvas_awareness() -> None:
    """Cancel leases without emitting across a detach/reattach boundary."""

    leases = list(_canvas_awareness.values())
    _canvas_awareness.clear()
    _canvas_control_validation_at.clear()
    _canvas_source_updates.clear()
    _canvas_presentation_updates.clear()
    for lease in leases:
        lease.task.cancel()


def _start_canvas_awareness(
    client_id: str,
    params: Dict[str, Any],
    *,
    renewed_at: float,
    validated_at: float,
) -> None:
    editing_session_id = str(params["editing_session_id"])
    task = asyncio.create_task(
        _expire_canvas_awareness(client_id, editing_session_id, params),
        name=f"canvas-awareness-{client_id[:8]}",
    )
    _canvas_awareness[client_id] = _CanvasAwarenessLease(
        task=task,
        params=params,
        renewed_at=renewed_at,
        validated_at=validated_at,
    )
    _fan_out_live_frame(
        {
            "method": "canvas.user_editing",
            "params": {
                **params,
                "sender_id": client_id,
                "ttl_ms": int(_CANVAS_AWARENESS_TTL_S * 1000),
            },
        }
    )


async def _handle_canvas_control(
    ws: WebSocket,
    data: Dict[str, Any],
    client_id: str,
    *,
    expected_session_identity_fingerprint: str | None = None,
) -> bool:
    """Handle validated edit invalidation and live-only awareness frames."""

    method = data.get("method")
    allowed = {
        "canvas.presentation_updated",
        "canvas.source_updated",
        "canvas.user_editing",
        "canvas.user_idle",
    }
    if method not in allowed:
        return False
    if method == "canvas.presentation_updated":
        expected_fields = {"method", "canvas_id", "presentation_revision"}
    else:
        expected_fields = {
            "method",
            "canvas_id",
            "path",
            "presentation_revision",
            "source_version",
        }
        if method in {"canvas.user_editing", "canvas.user_idle"}:
            expected_fields.add("editing_session_id")
    if set(data) != expected_fields:
        await _ws_send(
            ws,
            "error",
            {
                "code": "invalid_canvas_control",
                "message": "Canvas control message is invalid",
            },
        )
        return True
    editing_session_id = data.get("editing_session_id")
    if method in {"canvas.user_editing", "canvas.user_idle"} and (
        not isinstance(editing_session_id, str)
        or not re.fullmatch(r"[A-Za-z0-9_-]{8,128}", editing_session_id)
    ):
        await _ws_send(
            ws,
            "error",
            {
                "code": "invalid_canvas_control",
                "message": "Canvas editing session is invalid",
            },
        )
        return True
    path = data.get("path")
    revision = data.get("presentation_revision")
    source_version = data.get("source_version")
    invalid_identity = (
        data.get("canvas_id") != "main"
        or isinstance(revision, bool)
        or not isinstance(revision, int)
        or revision < 1
    )
    invalid_file_identity = method != "canvas.presentation_updated" and (
        not isinstance(path, str)
        or not 0 < len(path) <= 4096
        or not isinstance(source_version, str)
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", source_version)
    )
    if invalid_identity or invalid_file_identity:
        await _ws_send(
            ws,
            "error",
            {
                "code": "invalid_canvas_control",
                "message": "Canvas control message is invalid",
            },
        )
        return True

    now = asyncio.get_running_loop().time()
    if method in {"canvas.user_editing", "canvas.user_idle"}:
        lease = _canvas_awareness.get(client_id)
        if lease is not None and all(
            data.get(field) == lease.params.get(field)
            for field in (
                "canvas_id",
                "path",
                "presentation_revision",
                "source_version",
                "editing_session_id",
            )
        ):
            if method == "canvas.user_idle":
                _cancel_canvas_awareness(client_id)
                _fan_out_canvas_idle(client_id, lease.params)
                return True
            if now - lease.validated_at < _CANVAS_AWARENESS_TTL_S:
                if now - lease.renewed_at < _CANVAS_AWARENESS_RENEW_MIN_INTERVAL_S:
                    # Exact duplicate/over-eager renewal: the current server TTL
                    # is still live, so avoid task churn and redundant fan-out.
                    return True
                _cancel_canvas_awareness(client_id)
                _start_canvas_awareness(
                    client_id,
                    lease.params,
                    renewed_at=now,
                    validated_at=lease.validated_at,
                )
                return True

    source_identity: tuple[str, int, str] | None = None
    if method == "canvas.source_updated":
        assert isinstance(path, str) and isinstance(revision, int)
        assert isinstance(source_version, str)
        source_identity = (path, revision, source_version)
        if _canvas_source_updates.get(client_id) == source_identity:
            # The successful save response may be retried. A real subsequent
            # save advances the revision, so only the exact last accepted
            # identity is safe to deduplicate locally.
            return True

        # Do not drop a distinct committed revision. Pace authoritative checks
        # instead, bounding invalid/mismatched spam without losing a real save.
        validation_key = (client_id, "source")
        last_validation = _canvas_control_validation_at.get(validation_key)
        if last_validation is not None:
            remaining = _CANVAS_CONTROL_VALIDATION_MIN_INTERVAL_S - (
                now - last_validation
            )
            if remaining > 0:
                await asyncio.sleep(remaining)
                now = asyncio.get_running_loop().time()
        _canvas_control_validation_at[validation_key] = now
    elif method == "canvas.presentation_updated":
        assert isinstance(revision, int)
        if _canvas_presentation_updates.get(client_id) == revision:
            return True
        validation_key = (client_id, "presentation")
        last_validation = _canvas_control_validation_at.get(validation_key)
        if last_validation is not None:
            remaining = _CANVAS_CONTROL_VALIDATION_MIN_INTERVAL_S - (
                now - last_validation
            )
            if remaining > 0:
                await asyncio.sleep(remaining)
                now = asyncio.get_running_loop().time()
        _canvas_control_validation_at[validation_key] = now
    else:
        validation_key = (client_id, "awareness")
        last_validation = _canvas_control_validation_at.get(validation_key)
        if (
            last_validation is not None
            and now - last_validation < _CANVAS_CONTROL_VALIDATION_MIN_INTERVAL_S
        ):
            await _ws_send(
                ws,
                "error",
                {
                    "code": "canvas_control_rate_limited",
                    "message": "Canvas control messages are arriving too quickly",
                },
            )
            return True
        _canvas_control_validation_at[validation_key] = now

    try:
        state = await _current_canvas_for_control()
    except Exception as exc:
        logger.warning("Canvas control state validation failed: %s", exc)
        await _ws_send(
            ws,
            "error",
            {
                "code": "canvas_control_unavailable",
                "message": "Canvas state could not be validated",
            },
        )
        return True
    if (
        expected_session_identity_fingerprint is not None
        and _current_pinned_session_identity_fingerprint()
        != expected_session_identity_fingerprint
    ):
        return True
    state = _validated_canvas_control_state(data, state)
    if state is None:
        await _ws_send(
            ws,
            "error",
            {
                "code": "canvas_control_stale",
                "message": "Canvas state changed; reload before continuing",
            },
        )
        return True

    if method == "canvas.presentation_updated":
        source = state.get("source")
        source_type = source.get("type") if isinstance(source, dict) else None
        _broadcast(
            "canvas.updated",
            {
                "canvas_id": "main",
                "presentation_revision": state["presentation_revision"],
                "source_type": source_type,
                "updated_at": state.get("updated_at"),
            },
        )
        assert isinstance(revision, int)
        _canvas_presentation_updates[client_id] = revision
        return True

    if method == "canvas.source_updated":
        if _session is not None and _session.tool_context is not None:
            _session.tool_context.invalidate_recent_read(path)
        _broadcast(
            "canvas.source_updated",
            {
                "canvas_id": "main",
                "presentation_revision": state["presentation_revision"],
                "source_type": "workspace_file",
                "updated_at": state.get("updated_at"),
            },
        )
        assert source_identity is not None
        _canvas_source_updates[client_id] = source_identity
        return True

    awareness_params = {
        "canvas_id": "main",
        "path": path,
        "presentation_revision": state["presentation_revision"],
        "source_version": state["source_version"],
        "editing_session_id": editing_session_id,
    }
    assert isinstance(editing_session_id, str)
    previous = _cancel_canvas_awareness(client_id)
    if previous is not None:
        _fan_out_canvas_idle(client_id, previous.params)
    if method == "canvas.user_idle":
        _fan_out_canvas_idle(client_id, awareness_params)
        return True

    _start_canvas_awareness(
        client_id,
        awareness_params,
        renewed_at=now,
        validated_at=now,
    )
    return True


async def _run_subscriber_pump(
    ws: WebSocket, client_id: str, queue: asyncio.Queue
) -> None:
    """Drain one socket's queue; the handler owns cancellation and unsubscribe."""
    await _session_transport.run_subscriber_pump(
        ws, queue, ping_interval=_WS_PING_INTERVAL_S
    )


# ---------------------------------------------------------------------------
# Persistent-loop callbacks (module-level under headless semantics).
#
# These used to be closures inside ws_chat. They've been hoisted so the loop
# can outlive any single WebSocket connection: callbacks reference module
# globals (_session, _loop_user_queue, _loop_interrupt_flag,
# _loop_last_user_content, _orchestrator_client, _thread_id) and emit via
# _broadcast() rather than writing to one ws.
# ---------------------------------------------------------------------------


async def _loop_before_turn_authorization() -> tuple[bool, str]:
    """Maintain a commissioned Officer before any provider spend."""

    global _runtime_authorization_admission_open

    if _officer_cfg() is None:
        _runtime_authorization_admission_open = True
        return True, "not an Officer session"
    client = _orchestrator_client
    if client is None:
        _runtime_authorization_admission_open = False
        return False, "orchestrator maintenance channel is unavailable"
    try:
        maintained, reason = await client.maintain_runtime_actor(force=True)
        _runtime_authorization_admission_open = bool(maintained)
        return maintained, reason
    except Exception as exc:
        _runtime_authorization_admission_open = False
        # The durable server watchdog owns incident creation independently.
        # This local fence owns only the immediate no-spend guarantee and may
        # never expose a credential or response body in its error.
        logger.error(
            "Officer runtime authorization gate failed (%s)", type(exc).__name__
        )
        return False, "authorization maintenance failed before the turn"


def _loop_provider_admission_open() -> bool:
    """Synchronous fence read immediately before every provider invocation."""

    return _runtime_input_admission_open()


def _loop_auxiliary_provider_admission_open() -> bool:
    """Fence auxiliary spend on termination and Officer authorization.

    This must not replace ``_loop_provider_admission_open`` on the primary
    loop: a failed Officer still needs to consume a later durable input far
    enough to retry server maintenance, without reaching retrieval or a model.
    """

    if not _runtime_input_admission_open():
        return False
    return _officer_cfg() is None or _runtime_authorization_admission_open


async def _loop_admit_input_delivery(
    delivery_id: str, claim_generation: int, turn_number: int
) -> bool | None:
    """Cross the durable execution boundary immediately before model spend."""

    if not _runtime_input_admission_open():
        if not _runtime_admission_closed():
            _schedule_protected_input_reclaim()
        return False
    admitted = await _transition_claimed_input(
        delivery_id,
        claim_generation,
        "admitted",
        turn_number=turn_number,
    )
    # Close the in-process race as tightly as possible. If the sentinel became
    # visible while the CAS awaited Postgres, roll the not-yet-used admission
    # back to retryable before returning to the loop. No provider call exists
    # between these two statements.
    if admitted and not _runtime_input_admission_open():
        deferred = await _transition_claimed_input(
            delivery_id,
            claim_generation,
            "unadmit",
            reason=(
                "runtime_terminating_before_provider"
                if _runtime_admission_closed()
                else "protected_cloud_unavailable_before_provider"
            ),
        )
        if deferred:
            _queued_input_claims.discard((delivery_id, claim_generation))
            if not _runtime_admission_closed():
                _schedule_protected_input_reclaim()
            return None
        return False
    return admitted


async def _loop_defer_input_delivery(
    delivery_id: str, claim_generation: int, reason: str
) -> bool:
    deferred = await _transition_claimed_input(
        delivery_id,
        claim_generation,
        "deferred",
        reason=reason,
    )
    if deferred:
        _queued_input_claims.discard((delivery_id, claim_generation))
        if not _runtime_admission_closed() and not _protected_cloud_runtime_ready():
            _schedule_protected_input_reclaim()
    return deferred


async def _loop_cancel_input_delivery(
    delivery_id: str,
    claim_generation: int,
    turn_number: int,
    reason: str,
) -> bool:
    if not _persistent_input_cancellation_enabled():
        logger.warning(
            "Pinned input cancellation writer is disabled; halting before provider"
        )
        return False
    cancelled = await _transition_claimed_input(
        delivery_id,
        claim_generation,
        "cancelled",
        turn_number=turn_number,
        reason=reason,
    )
    if cancelled:
        _queued_input_claims.discard((delivery_id, claim_generation))
    return cancelled


async def _loop_defer_and_requeue_input_delivery(
    delivery_id: str,
    claim_generation: int,
    content: str,
    role: str,
    source: str,
    reason: str,
) -> dict[str, Any] | None:
    """Atomically defer and priority-reclaim one stopped server wake.

    The shared lock prevents concurrent input acceptance from reclaiming the
    deferred row into the ordinary FIFO in the gap between its generation CAS
    and priority publication. Claiming the exact stable identity mints a fresh
    generation; the returned item stays in the graph's one-item priority slot.
    """

    if source != "officer_wake" or role == "human":
        return None
    if (
        _runtime_admission_closed()
        or _session is None
        or _session.postgres_conn is None
        or _thread_id is None
    ):
        return None
    async with _input_delivery_reclaim_lock:
        try:
            deferred = await _transition_claimed_input(
                delivery_id,
                claim_generation,
                "deferred",
                reason=reason,
            )
            if not deferred:
                return None
            _queued_input_claims.discard((delivery_id, claim_generation))
            agent_id, pod_uid, runtime_generation, runtime_attach_token = (
                _pinned_input_runtime_identity()
            )
            row = await asyncio.wait_for(
                _session.postgres_conn.persist_pinned_input_delivery(
                    thread_id=_thread_id,
                    delivery_id=delivery_id,
                    role=role,
                    content=content,
                    source=source,
                    turn_number=_session.turn_count + 1,
                    agent_id=agent_id,
                    pod_uid=pod_uid,
                    runtime_generation=runtime_generation,
                    session_runtime_generation=str(_session_runtime_generation or ""),
                    runtime_attach_token=runtime_attach_token,
                ),
                timeout=5.0,
            )
            next_generation = int(row["claim_generation"])
            if str(row["state"]) not in {"owned", "queued"}:
                return None
            queued = await _session.postgres_conn.mark_pinned_input_delivery_queued(
                thread_id=_thread_id,
                delivery_id=delivery_id,
                agent_id=agent_id,
                pod_uid=pod_uid,
                runtime_generation=runtime_generation,
                session_runtime_generation=str(_session_runtime_generation or ""),
                runtime_attach_token=runtime_attach_token,
                claim_generation=next_generation,
            )
            if not queued:
                return None
            if _runtime_admission_closed():
                await _transition_claimed_input(
                    delivery_id,
                    next_generation,
                    "deferred",
                    reason="runtime_terminating_before_priority_requeue",
                )
                return None
        except Exception as exc:
            logger.warning(
                "Deferred wake priority reclaim failed (%s)", type(exc).__name__
            )
            return None

        key = (delivery_id, next_generation)
        if key in _queued_input_claims:
            return None
        _queued_input_claims.add(key)
        return {
            "content": str(row["content"]),
            "id": str(row["message_id"]),
            "role": str(row["role"]),
            "source": source,
            "delivery_id": delivery_id,
            "claim_generation": next_generation,
        }


async def _loop_settle_input_delivery(delivery_id: str, claim_generation: int) -> bool:
    settled = await _transition_claimed_input(
        delivery_id,
        claim_generation,
        "settled",
    )
    if settled:
        _queued_input_claims.discard((delivery_id, claim_generation))
    return settled


_PINNED_INPUT_POLL_SECONDS = 1.0


async def _wait_for_persistent_input(
    queue: asyncio.Queue,
    *,
    timeout: float | None = None,
) -> Any:
    """Wait while polling the durable pinned inbox for correctness.

    Orchestrator-side input no longer needs a Pod-IP POST: it commits a stable
    delivery and this exact reciprocal runtime claims it. LISTEN would only be
    a latency optimization; the bounded poll means a lost notification, a pod
    replacement, or an IP-reused foreign process cannot lose or consume work.
    """

    if _stateless_mode():
        if timeout is None:
            return await queue.get()
        return await asyncio.wait_for(queue.get(), timeout=timeout)

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout if timeout is not None else None
    while True:
        if _runtime_admission_closed():
            # Leave every claimed/queued row for the successor. Normal
            # teardown cancels this wait after preStop observes the park.
            await asyncio.Future()
        try:
            await _reclaim_pending_pinned_inputs()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning(
                "Durable pinned input poll failed for thread %s; retrying",
                _thread_id,
                exc_info=True,
            )
        if not queue.empty():
            return await queue.get()

        remaining = None if deadline is None else deadline - loop.time()
        if remaining is not None and remaining <= 0:
            raise asyncio.TimeoutError
        wait_for = _PINNED_INPUT_POLL_SECONDS
        if remaining is not None:
            wait_for = min(wait_for, remaining)
        try:
            return await asyncio.wait_for(queue.get(), timeout=wait_for)
        except asyncio.TimeoutError:
            if deadline is not None and loop.time() >= deadline:
                raise


async def _loop_get_user_input() -> str:
    """Wait for the next user input. Honors session idle timeout.

    On idle timeout, raises IdleTimeoutError. The loop unwinds and
    ``_handle_idle_archive`` first authorizes exact retirement before it emits
    the generation-tagged diagnostic; a failed Begin must leave the live UI and
    runtime usable.
    """
    global _awaiting_input

    queue = _loop_user_queue
    if queue is None:
        # _attach_session always initializes this. If we hit None here the
        # session is being torn down — fail loudly so the loop unwinds.
        raise RuntimeError("_loop_user_queue not initialized — session torn down?")

    if _runtime_admission_closed():
        # Do not consume already-durable queued work. The exact successor
        # reclaims it from thread_input_deliveries; transcript restore excludes
        # unadmitted rows because conversation context is not an inbox. This
        # wait is cancelled by normal process shutdown after preStop observes
        # the exact parked boundary.
        _awaiting_input = True
        try:
            await asyncio.Future()
        finally:
            _awaiting_input = False

    _broadcast("ready", {})

    # Phase 5/6: natural-pause transition to 'awaiting_user'. Eager mode
    # (default) only flips when untethered — the agent is presumed to be
    # working in the background and we only need to flag-and-notify when
    # the user has nobody watching. Polite mode flips at every turn boundary
    # regardless of subscribers — the user has explicitly opted in to a
    # review-heavy "see every step" workflow and wants notification + an
    # explicit reply gate after each completed turn. Idempotent on the
    # orchestrator side: repeated writes preserve awaiting_user_since.
    officer_cfg = _officer_cfg()

    headless_mode = "eager"
    if _session is not None:
        headless_cfg = getattr(_session.config, "headless", None)
        if headless_cfg is not None:
            headless_mode = getattr(headless_cfg, "mode", "eager") or "eager"
    # Officer sessions never flip to awaiting_user — sleeping is not "awaiting
    # a user", and the flip would arm the attention-sleep sweeper + orphan
    # reclassification against a thread the officer watchdog owns
    # (centurion.md §4). Officer also overrides polite mode: an explicit
    # reply gate after each turn contradicts autonomous cycling.
    should_consider_flip = (
        officer_cfg is None
        and _session is not None
        and _session.turn_count > 0
        and _orchestrator_client is not None
        and _thread_id is not None
    )
    if should_consider_flip:
        if _stateless_mode():
            # SSE presence is durable and replica-independent. Polite mode
            # still pauses with a viewer; eager mode only pauses untethered.
            await _safe_mark_stateless_natural_pause(
                require_untethered=headless_mode != "polite"
            )
        elif headless_mode == "polite" or not _subscribers:
            # Pinned behavior remains the exact process-local subscriber path.
            asyncio.create_task(
                _safe_set_thread_status("awaiting_user"),
                name="phase5-flip-awaiting-user",
            )

    # Parked window for the drain-suspend gate (_session_parked): exactly the
    # span where this coroutine is blocked on the queue. The finally also
    # covers loop-task cancellation and the idle-timeout raise.
    _awaiting_input = True
    try:
        if _session is None:
            return await _wait_for_persistent_input(queue)

        if officer_cfg is not None:
            # Officer park (centurion.md §4). The primary wake is the
            # orchestrator's Postgres-durable timer: consume the sleep tool's
            # parked request and FILE the wake-up call; when no request was
            # made (turn ended in plain text) file nothing — the officer
            # watchdog files sleep_max on our behalf. The local wait is only
            # a long, labeled backstop for the one failure external timers
            # can't cover: timer path down while the API is up. Never raises
            # IdleTimeoutError — an officer session never idle-archives.
            sleep_req = None
            if _session.tool_context is not None:
                sleep_req = _session.tool_context.consume_officer_sleep()
            if sleep_req is not None:
                minutes = max(
                    officer_cfg.sleep_min_minutes,
                    min(
                        int(sleep_req.get("minutes") or 0),
                        officer_cfg.sleep_max_minutes,
                    ),
                )
                asyncio.create_task(
                    _file_officer_wake(minutes, sleep_req.get("reason") or ""),
                    name="officer-file-wake",
                )
            try:
                return await _wait_for_persistent_input(
                    queue,
                    timeout=officer_cfg.backstop_seconds,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "Officer backstop wake fired for thread %s — the "
                    "orchestrator's durable timer never delivered",
                    _thread_id,
                )
                return {
                    "content": (
                        "[backstop wake] The orchestrator's durable timer did "
                        "not fire in time — its wake path may be degraded. "
                        "Check the situation via your tools; if a conference "
                        "hold is standing and no brief has arrived yet, go "
                        "back to sleep."
                    ),
                    "role": "event",
                }

        idle_timeout_minutes = _session.config.interactive.idle_timeout_minutes
        if idle_timeout_minutes and idle_timeout_minutes > 0:
            idle_timeout_seconds = idle_timeout_minutes * 60
            try:
                return await _wait_for_persistent_input(
                    queue,
                    timeout=idle_timeout_seconds,
                )
            except asyncio.TimeoutError:
                logger.info(
                    "Idle timeout (%dmin) for thread %s",
                    idle_timeout_minutes,
                    _thread_id,
                )
                raise IdleTimeoutError(f"Idle timeout after {idle_timeout_seconds}s")
        return await _wait_for_persistent_input(queue)
    finally:
        _awaiting_input = False


def _loop_check_interrupt() -> Optional[str]:
    """One-shot read of the interrupt flag. Returns the mode or None.

    Returns:
        None when no interrupt is pending.
        "hard" to cancel the in-flight LLM stream immediately and drop the
            partial AIMessage (set when interrupt fires with no tool active).
        "graceful" to stop after the current tool call completes (set when
            interrupt fires with a tool mid-`ainvoke`).

    Consumed by persistent_graph at three checkpoints. A `bool(result)`
    check preserves the legacy "any interrupt → stop" semantics for sites
    that don't yet branch on the mode.
    """
    mode = _loop_interrupt_flag
    target_turn_id = _loop_interrupt_target_turn_id
    if mode is None:
        if target_turn_id is not None:
            _clear_loop_interrupt()
        return None
    current_turn_id = int(_session.turn_count) if _session is not None else None
    if target_turn_id is None or target_turn_id != current_turn_id:
        logger.warning(
            "Discarding unscoped/stale interrupt "
            "(target_turn=%s current_turn=%s mode=%s)",
            target_turn_id,
            current_turn_id,
            mode,
        )
        _clear_loop_interrupt()
        return None
    _clear_loop_interrupt(target_turn_id=target_turn_id)
    return mode


async def _loop_on_token(token: str) -> None:
    _broadcast("token", {"content": token})


async def _loop_on_thinking(content: str, message_id: Optional[str] = None) -> None:
    payload: Dict[str, Any] = {"content": content}
    if message_id:
        # The agent mints `msg_…` ids but the thread_messages PK is a derived
        # uuid5 (``_coerce_row_id``), and history serves that UUID. Coerce the
        # same way here so the live frame's id matches the row's, letting the
        # client dedupe a reasoning frame replayed after history painted the
        # bubble (the gemma "reasoning duplicates on replay" bug).
        from agent.database.postgres_db import _coerce_row_id

        payload["message_id"] = _coerce_row_id(message_id)
    _broadcast("thinking", payload)


async def _loop_on_thinking_reset(message_id: Optional[str] = None) -> None:
    # Drop the in-progress reasoning bubble for this message id on the client.
    # The empty-response retry emits this right before re-streaming the retry's
    # reasoning so the dead-end reasoning is REPLACED, not appended-under. The
    # _coerce_row_id coercion is load-bearing: it must match the id
    # _loop_on_thinking stamped on the original reasoning frames, or the client
    # won't find the bubble to clear.
    payload: Dict[str, Any] = {}
    if message_id:
        from agent.database.postgres_db import _coerce_row_id

        payload["message_id"] = _coerce_row_id(message_id)
    _broadcast("thinking.reset", payload)


async def _loop_on_tool_start(
    tool_name: str, tool_args: Dict[str, Any], tool_call_id: str
) -> None:
    meta = TOOL_REGISTRY.get(tool_name, {})
    _broadcast(
        "tool.started",
        {
            "tool": tool_name,
            "args": _safe_serialize(tool_args),
            "id": tool_call_id,
            "category": meta.get("category", ""),
        },
    )


async def _loop_on_tool_execution_start(tool_name: str, tool_call_id: str) -> None:
    """Latch the exact claim/turn crossing a real tool's effect boundary."""

    global _tool_inflight
    del tool_name, tool_call_id
    if not await _loop_runtime_effect_authority_current():
        if not _protected_cloud_runtime_ready():
            _schedule_protected_input_reclaim()
            raise WorkspaceUnavailableError(
                "protected cloud became unavailable before tool execution"
            )
        raise TerminationAdmissionClosed(
            "pinned runtime authority closed before tool execution"
        )
    identity = _record_turn_tool_execution_identity()
    lease = _current_lease_var.get()
    if lease is not None:
        hook = _turn_tool_execution_external_hook
        if identity is None or hook is None:
            # A stateless tool must never run unless shutdown/reaper handling can
            # name its exact claimant. Fence all later writes/completion and let
            # the callback failure abort before ``tool.ainvoke``.
            lease.mark_lost()
            raise RuntimeError(
                "stateless tool execution lacks an exact claimant identity"
            )
        try:
            hook(identity)
        except BaseException:
            lease.mark_lost()
            raise
    # This callback returns directly into `tool.ainvoke`. No earlier UI or
    # permission callback may set the latch: if this final health/identity
    # gate raises, no result callback runs to clear it.
    _tool_inflight = True


async def _loop_on_tool_result(
    tool_name: str,
    result: str,
    tool_call_id: str,
    is_error: bool = False,
) -> None:
    global _tool_inflight
    _tool_inflight = False
    # Truncate large results for transport (full result is in message history)
    display_result = result[:2000] + "..." if len(result) > 2000 else result
    _broadcast(
        "tool.completed",
        {
            "tool": tool_name,
            "result": display_result,
            "id": tool_call_id,
            "is_error": is_error,
        },
    )

    # Notify frontend of file checkpoint availability after writes
    if tool_name in ("write_file", "edit_file") and _session is not None:
        _broadcast(
            "file.checkpoint",
            {"turn_id": _session.turn_count},
        )

    # Broadcast task state after task tool calls
    if (
        tool_name in ("task_add", "task_complete", "task_list")
        and _session is not None
        and _session.session_task_manager
    ):
        _broadcast(
            "tasks.updated",
            {"tasks": _session.session_task_manager.to_dict_list()},
        )


# ---------------------------------------------------------------------------
# Phase 3: DB-backed permission gates (thread_permission_requests)
# ---------------------------------------------------------------------------
#
# The agent INSERTs a pending row when permission_check fires, then checks that
# row until an UPDATE resolves it. Approval can arrive from any path
# (WS-attached cockpit, REST POST from MCP/cockpit, future email magic-link) —
# all converge on the same UPDATE statement. The agent never blocks on an
# in-memory queue anymore; the queue path is still in place for non-permission
# user input.
#
# Poll on short-lived pool acquisitions rather than pinning a LISTEN connection
# for the whole human wait. A stateless turn already owns long-lived control and
# interrupt listeners; with the supported three-connection agent pool, a third
# listener would starve the independent lease heartbeat and let a live turn be
# stolen. Polling is the correctness path for the other inboxes too; one second
# keeps approval latency bounded while always returning the connection between
# checks.

_PERMISSION_TIMEOUT_S: float = 300.0
_PERMISSION_POLL_SECONDS: float = 1.0


async def _insert_permission_request(
    tool_call_id: str, tool_name: str, tool_args: Dict[str, Any]
) -> Optional[str]:
    """INSERT a pending row and return its UUID. None on failure."""
    if _session is None or _session.postgres_conn is None or _thread_id is None:
        return None
    try:
        async with _session.postgres_conn.acquire() as conn:
            if _stateless_mode():
                # Permission admission is part of the exact serving claim.  A
                # plain INSERT can wait behind public End's thread lock and
                # then create a fresh pending approval after End has expired
                # the old cards.  Serialize in the global threads -> queue
                # order and hold both locks through the INSERT so End observes
                # either the card (and expires it) or the completed fence.
                handle = _current_lease_var.get()
                if (
                    handle is None
                    or not handle.active
                    or handle.lost.is_set()
                    or str(handle.unit_id) != str(_thread_id)
                    or int(handle.lease_token) <= 0
                ):
                    if handle is not None:
                        handle.mark_lost()
                    return None
                async with conn.transaction():
                    thread_live = await conn.fetchval(
                        "SELECT id FROM threads WHERE id = $1::uuid "
                        "AND execution_lane = 'stateless' "
                        "AND status IN ('created', 'active', 'awaiting_user', "
                        "               'suspended') "
                        "AND NOT (COALESCE(metadata, '{}'::jsonb) "
                        "         ? '_stateless_workspace_retirement_pending') "
                        "FOR UPDATE",
                        _thread_id,
                    )
                    queue_live = None
                    if thread_live is not None:
                        queue_live = await conn.fetchval(
                            "SELECT unit_id FROM run_queue "
                            "WHERE unit_id = $1::uuid "
                            "AND unit_kind = 'session_turn' "
                            "AND state = 'leased' "
                            "AND lease_token = $2::bigint "
                            "FOR SHARE",
                            _thread_id,
                            int(handle.lease_token),
                        )
                    if queue_live is None:
                        handle.mark_lost()
                        return None
                    row_id = await conn.fetchval(
                        "INSERT INTO thread_permission_requests "
                        "(thread_id, tool_call_id, tool_name, tool_args, "
                        " accepted_lease_token) "
                        "VALUES ($1, $2, $3, $4::jsonb, $5::bigint) "
                        "RETURNING id",
                        _thread_id,
                        tool_call_id,
                        tool_name,
                        json.dumps(_safe_serialize(tool_args)),
                        int(handle.lease_token),
                    )
                return str(row_id) if row_id is not None else None

            # Pinned permission admission retains its existing statement and
            # autocommit shape.
            row_id = await conn.fetchval(
                "INSERT INTO thread_permission_requests "
                "(thread_id, tool_call_id, tool_name, tool_args) "
                "VALUES ($1, $2, $3, $4::jsonb) "
                "RETURNING id",
                _thread_id,
                tool_call_id,
                tool_name,
                json.dumps(_safe_serialize(tool_args)),
            )
        return str(row_id) if row_id is not None else None
    except Exception as e:
        logger.warning(
            "thread_permission_requests INSERT failed (tool=%s): %s",
            tool_name,
            e,
        )
        return None


_SHELL_TOOLS = {"run_command", "shell_execute", "shell_read"}


def _gate_needed(mode: str, tool_name: str) -> bool:
    """Whether this call would actually hit a permission gate.

    Mirrors the early-returns in ``_loop_permission_check`` so the announce
    never creates a row for a call that auto-approves.
    """
    if mode == "autonomous":
        return False
    if mode == "auto_accept":
        return tool_name in _SHELL_TOOLS
    return True


async def _has_terminal_permission_decision(tool_call_id: str) -> bool:
    """True if this tool_call_id already has an approved/denied row for the
    current thread.

    Covers the Phase 5 wake replay: a gate resolved out-of-band (e.g. a
    magic-link click) while the agent was suspended leaves a terminal row
    behind, then LangGraph restores the SAME tool_call_id on wake. Without
    this check the announce step would INSERT a fresh 'pending' row for it;
    _loop_permission_check's terminal-row short-circuit returns immediately
    without ever claiming, waiting on, or expiring that new row, and nothing
    else reaps it (there is no expires_at sweeper — only an active waiter
    CAS-expires on timeout). The orphan re-renders as a live approval card
    on every reattach, forever.

    Soft-fails to False (assume no terminal row) so a DB hiccup degrades to
    the pre-batch behavior — the per-call gate path still inserts and gates
    — rather than blocking the announce.
    """
    if _session is None or _session.postgres_conn is None or _thread_id is None:
        return False
    try:
        async with _session.postgres_conn.acquire() as conn:
            found = await conn.fetchval(
                "SELECT 1 FROM thread_permission_requests "
                "WHERE thread_id = $1 AND tool_call_id = $2 "
                "  AND status IN ('approved', 'denied') "
                "LIMIT 1",
                _thread_id,
                tool_call_id,
            )
        return found is not None
    except Exception as e:
        logger.warning(
            "Terminal-decision check for tool_call %s failed (%s); "
            "assuming none exists",
            tool_call_id,
            e,
        )
        return False


# tool_call_id -> (permission-row id, tool name, thread id) for the batch
# announced by the turn currently running. Mutated in place (never rebound) so
# tests and concurrent readers see one dict.
#
# The thread id is carried per entry, not read from the ambient `_thread_id`,
# because this module is process-global and a pool agent serves many threads in
# sequence: an entry that outlived its session (see _terminate_session_inner)
# must never be swept — and broadcast as resolved — by the NEXT thread, whose
# clients have never seen those tool_call_ids.
_announced_permission_rows: Dict[str, Tuple[str, str, str]] = {}

# The row _loop_permission_check is blocked on right now, if any. A sweep must
# never expire it out from under its own waiter: the waiter would read
# 'expired', report NO_ANSWER and park a turn the user had just unblocked.
_active_permission_request_id: Optional[str] = None

# tool_call_ids whose gate is currently *resolving* — from just before the
# claim SELECT until the gate returns. `_active_permission_request_id` cannot
# cover that whole span: the row id is not known until after the awaited
# SELECT, and `async with pool.acquire()` exits through a yield point. A
# fire-and-forget `mode.set` sweep landing in that window would CAS-expire the
# very row the gate is about to wait on, and the waiter's re-SELECT would read
# 'expired' -> NO_ANSWER — parking the turn the user had just unblocked. The
# reservation is keyed on tool_call_id, which IS known up front.
_gates_in_flight: Set[str] = set()


def _permission_retirement_authority() -> Optional[Tuple[str, int | str]]:
    """Capture the exact credential allowed to retire permission rows."""

    if _stateless_mode():
        lease_token = _current_stateless_lease_token()
        return ("stateless", lease_token) if lease_token is not None else None
    agent_id = _control_owner_agent_id or _registered_pinned_agent_id()
    return ("pinned", agent_id) if agent_id is not None else None


_RETIRE_STATELESS_PERMISSION_SQL = """
    WITH owner AS MATERIALIZED (
        SELECT queue.unit_id
        FROM run_queue AS queue
        WHERE queue.unit_id = $2::uuid
          AND queue.unit_kind = 'session_turn'
          AND queue.state = 'leased'
          AND queue.lease_token = $3::bigint
        FOR SHARE OF queue
    ), expired AS (
        UPDATE thread_permission_requests AS request
        SET status = 'expired', decided_at = clock_timestamp(),
            decided_by = 'system'
        WHERE request.id = $1::uuid
          AND request.thread_id = $2::uuid
          AND request.status = 'pending'
          AND EXISTS (SELECT 1 FROM owner)
        RETURNING request.id
    )
    SELECT id FROM expired
"""


_RETIRE_PINNED_PERMISSION_SQL = """
    WITH thread_owner AS MATERIALIZED (
        SELECT thread.id
        FROM threads AS thread
        WHERE thread.id = $2::uuid
          AND thread.execution_lane = 'pinned'
          AND thread.agent_id = $3::uuid
        FOR NO KEY UPDATE OF thread
    ), agent_owner AS MATERIALIZED (
        SELECT agent.id
        FROM agents AS agent
        WHERE agent.id = $3::uuid
          AND agent.thread_id = $2::uuid
          AND EXISTS (SELECT 1 FROM thread_owner)
        FOR SHARE OF agent
    ), expired AS (
        UPDATE thread_permission_requests AS request
        SET status = 'expired', decided_at = clock_timestamp(),
            decided_by = 'system'
        WHERE request.id = $1::uuid
          AND request.thread_id = $2::uuid
          AND request.status = 'pending'
          AND EXISTS (SELECT 1 FROM thread_owner)
          AND EXISTS (SELECT 1 FROM agent_owner)
        RETURNING request.id
    )
    SELECT id FROM expired
"""


async def _retire_announced_permission_rows(
    reason: str, mode: Optional[str] = None
) -> None:
    """CAS-expire announced permission rows that nothing will ever claim.

    An announced row is only retired by its OWN gate in
    ``_loop_permission_check``, and that gate runs at most once per call. Any
    turn exit before call *i* — parked on NO_ANSWER, interrupted, errored, or
    a mid-batch mode downgrade — strands rows *i..N* as 'pending' forever:
    there is no ``expires_at`` sweeper anywhere, only an active waiter
    CAS-expires a row.

    A stranded row is not inert. ``_pending_permission_requests`` rides the
    ``session.state`` welcome frame, so it re-renders on every reattach as a
    live approval card; "Approve all" flips it to 'approved', the NOTIFY
    reaches NO waiter, and **nothing executes** while the user believes they
    approved those tools. That silent divergence between what the UI claims
    and what runs is why this cleanup exists.

    ``mode``: retire only the rows that permission mode would no longer gate
    (a mid-batch downgrade). Omitted ⇒ retire the whole batch, which is what
    the end of a turn wants. A row whose gate is in flight is never touched:
    ``_gates_in_flight`` covers the whole resolve span and
    ``_active_permission_request_id`` the wait itself (belt and braces).
    Neither is another thread's row — entries are thread-scoped, so a sweep can
    only ever expire rows announced by the session it is running in.

    CAS (``WHERE id = $1 AND status = 'pending'``) so a genuine decision that
    landed a microsecond earlier still wins. The same statement holds either
    the exact stateless run-queue lease or the reciprocal pinned binding while
    updating; a stale runtime therefore cannot expire a successor's gate.
    Only rows this sweep really expired are broadcast, so an attached client
    drops exactly those cards.
    """
    if not _announced_permission_rows:
        return
    # Checked BEFORE taking ownership below: with no pool nothing can be
    # expired, and popping the entries here would delete rows from memory that
    # were never retired in the DB. Nothing else reaps them (there is no
    # expires_at sweeper), so they would re-render as phantom approval cards
    # on every reattach with no way left to resolve them.
    if _session is None or _session.postgres_conn is None:
        return
    authority = _permission_retirement_authority()
    if authority is None or _thread_id is None:
        logger.info(
            "Skipped permission-row retirement without exact owner (%s)", reason
        )
        return
    authority_kind, authority_credential = authority
    # Take ownership up front so a second sweep can't double-work the same
    # rows; anything we fail to reach goes back on the ledger below. No await
    # between this and the pop, so ownership is atomic.
    doomed: Dict[str, Tuple[str, str, str]] = {
        tool_call_id: entry
        for tool_call_id, entry in _announced_permission_rows.items()
        if (mode is None or not _gate_needed(mode, entry[1]))
        and entry[2] == _thread_id
        and tool_call_id not in _gates_in_flight
        and entry[0] != _active_permission_request_id
    }
    if not doomed:
        return
    for tool_call_id in doomed:
        _announced_permission_rows.pop(tool_call_id, None)

    expired: List[Tuple[str, str]] = []
    try:
        async with _session.postgres_conn.acquire() as conn:
            for tool_call_id, (request_id, _tool, _tid) in list(doomed.items()):
                row_id = await conn.fetchval(
                    _RETIRE_STATELESS_PERMISSION_SQL
                    if authority_kind == "stateless"
                    else _RETIRE_PINNED_PERMISSION_SQL,
                    request_id,
                    _thread_id,
                    authority_credential,
                )
                doomed.pop(tool_call_id, None)
                if row_id is not None:
                    expired.append((tool_call_id, request_id))
    except Exception as e:
        logger.warning("Retiring announced permission rows failed (%s): %s", reason, e)
        # Put back whatever we never reached so the next sweep retries it —
        # setdefault so a fresh announce for the same call still wins.
        for tool_call_id, entry in doomed.items():
            _announced_permission_rows.setdefault(tool_call_id, entry)

    for tool_call_id, request_id in expired:
        # Journal it like any other outcome so an attached cockpit drops the
        # card immediately instead of showing a gate nobody is waiting on.
        _broadcast(
            "permission.resolved",
            {
                "id": tool_call_id,
                "approval_id": request_id,
                "decision": "expired",
            },
        )
    if expired:
        logger.info(
            "Retired %d unclaimed announced permission row(s) (%s)",
            len(expired),
            reason,
        )


async def _loop_announce_permission_batch(tool_calls: List[Dict[str, Any]]) -> None:
    """Insert a pending row for every gate-needing call in one batch, then
    emit a single ``permission.request_batch`` frame.

    Lets the cockpit show every pending call at once instead of one card per
    finished tool. The per-call gate path then *claims* these rows rather
    than inserting its own — see ``_loop_permission_check``.
    """
    if _session is None or _thread_id is None:
        return
    # A mode control can commit while the LLM is producing its tool batch.
    # Drain under the exact current owner immediately before deciding which
    # rows to announce; LISTEN is latency-only and may have missed a notice.
    await _drain_current_thread_controls()
    if _session is None:
        return
    # Belt-and-braces against a turn that ended without its cleanup hook
    # (e.g. the loop task cancelled outright): never let a previous batch's
    # rows sit pending behind a fresh one.
    await _retire_announced_permission_rows("superseded by a new batch")
    mode = _session.permission_mode
    gated = [tc for tc in tool_calls if _gate_needed(mode, tc.get("name", ""))]
    if not gated:
        return

    requests: List[Dict[str, Any]] = []
    for tc in gated:
        tool_call_id = tc.get("id") or ""
        tool_name = tc.get("name", "")
        tool_args = tc.get("args", {}) or {}
        if not tool_call_id:
            continue
        if await _has_terminal_permission_decision(tool_call_id):
            # Already resolved out-of-band before this wake — see
            # _has_terminal_permission_decision. _loop_permission_check
            # will short-circuit on that terminal row on its own; a fresh
            # pending row here would just be orphaned.
            continue
        request_id = await _insert_permission_request(
            tool_call_id, tool_name, tool_args
        )
        if request_id is None:
            # DB refused this row — leave it to the per-call gate path.
            continue
        # Own the row until its gate claims it or the turn ends. Without this
        # ledger nothing can tell a row that was answered from one the turn
        # walked away from. Stamped with the announcing thread so a later
        # session in this same process can never sweep it.
        _announced_permission_rows[tool_call_id] = (request_id, tool_name, _thread_id)
        requests.append(
            {
                "id": tool_call_id,
                "approval_id": request_id,
                "tool": tool_name,
                "args": _safe_serialize(tool_args),
            }
        )

    if requests:
        _broadcast("permission.request_batch", {"requests": requests})


async def _pending_permission_requests() -> List[Dict[str, Any]]:
    """Every still-pending gate for this thread, shaped like the
    ``permission.request`` broadcast payload.

    Rides the ``session.state`` welcome frame so a (re)attaching client can
    re-render an approval card it never received — or received and then lost
    when the live stream dropped. REST history does not carry pending gates,
    so without this a reload leaves the gate stranded and the user has no way
    to answer it (knowledge-history/done/supervised_parallel_gates_timeout_fabricates_denial.md).

    Soft-fails to ``[]``: a welcome frame must still go out if this lookup
    breaks.
    """
    if _session is None or _session.postgres_conn is None or _thread_id is None:
        return []
    try:
        async with _session.postgres_conn.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, tool_call_id, tool_name, tool_args "
                "FROM thread_permission_requests "
                "WHERE thread_id = $1 AND status = 'pending' "
                "ORDER BY requested_at ASC",
                _thread_id,
            )
    except Exception as e:
        logger.warning("Pending permission lookup failed: %s", e)
        return []

    pending: List[Dict[str, Any]] = []
    for row in rows:
        # asyncpg hands JSONB back as a raw string on this pool — parse it so
        # the client gets an object, not a quoted blob (see the JSONB
        # guard-without-parse family of bugs).
        raw_args = row["tool_args"]
        if isinstance(raw_args, str):
            try:
                args = json.loads(raw_args)
            except Exception:
                args = {}
        else:
            args = raw_args or {}
        pending.append(
            {
                "id": row["tool_call_id"],
                "approval_id": str(row["id"]),
                "tool": row["tool_name"],
                "args": args,
            }
        )
    return pending


async def _wait_for_permission_resolution(
    request_id: str, timeout: float = _PERMISSION_TIMEOUT_S
) -> str:
    """Block until the row's status flips from pending. Returns the final
    status string ('approved'/'denied'/'expired'/'interrupted'/'unavailable').

    Only 'denied' means the user said no. Everything else is a *non-decision*
    the caller maps to PermissionOutcome.NO_ANSWER: an unreachable DB or an
    unexpected error must not be reported as a refusal the user never made.

    Re-SELECTs on short-lived pool acquisitions and sleeps without owning a
    connection between checks. This is deliberately polling-only: a stateless
    turn's control and interrupt watchers already pin two LISTEN connections,
    so holding a third throughout a human wait would starve the exact-lease
    heartbeat on the supported three-connection agent pool.

    ``timeout`` is a *polling slice*, not a deadline, while a client is
    attached: a user who is simply slow to click must not have the gate
    expired under them (their later click would 404 and the tool would never
    run). Only when untethered — nobody is there to answer — does the slice
    CAS-expire the row so the loop can't hang on a client that isn't coming
    back. A hard interrupt (Stop) always breaks the wait promptly.
    See knowledge-history/done/supervised_parallel_gates_timeout_fabricates_denial.md.
    """
    if _session is None or _session.postgres_conn is None:
        # The session died mid-wait. Nothing can answer the question any more,
        # but nobody refused it either — say so rather than inventing a click.
        return "unavailable"

    postgres_conn = _session.postgres_conn
    base_timeout = max(0.0, float(timeout))
    wait_timeout = base_timeout
    terminal_statuses = ("approved", "denied", "expired")

    async def _read_status() -> Any:
        async with postgres_conn.acquire() as conn:
            return await conn.fetchval(
                "SELECT status FROM thread_permission_requests WHERE id = $1",
                request_id,
            )

    async def _read_status_interruptibly() -> Any:
        """Race only the read-only pool operation against a hard interrupt."""

        read_task = asyncio.create_task(_read_status())
        interrupt_task = (
            asyncio.create_task(_hard_interrupt_event.wait())
            if _hard_interrupt_event is not None
            else None
        )
        if interrupt_task is None:
            return await read_task
        try:
            done, _pending = await asyncio.wait(
                {read_task, interrupt_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if read_task in done:
                return read_task.result()
            raise InterruptedError
        finally:
            for task in (read_task, interrupt_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(read_task, interrupt_task, return_exceptions=True)

    try:
        while True:
            # Race-safe initial/current read. The connection is returned before
            # any human-scale wait so lease renewal always has a pool slot.
            current = await _read_status_interruptibly()
            if current in terminal_statuses:
                return str(current)

            if _hard_interrupt_event is not None and _hard_interrupt_event.is_set():
                # Stop pressed. Leave the row pending — the user made no
                # decision, so nothing may be recorded as one.
                logger.info(
                    "Permission wait interrupted (req=%s) — leaving pending",
                    request_id,
                )
                return "interrupted"

            # Preserve `timeout` as the presence/expiry slice, not a total
            # deadline. Status polls within the slice only reduce answer
            # latency; they do not move the expiry boundary.
            deadline = asyncio.get_running_loop().time() + wait_timeout
            while True:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    break
                poll_seconds = min(_PERMISSION_POLL_SECONDS, remaining)
                if _hard_interrupt_event is None:
                    await asyncio.sleep(poll_seconds)
                else:
                    try:
                        await asyncio.wait_for(
                            _hard_interrupt_event.wait(),
                            timeout=poll_seconds,
                        )
                    except asyncio.TimeoutError:
                        pass
                    if _hard_interrupt_event.is_set():
                        logger.info(
                            "Permission wait interrupted (req=%s) — leaving pending",
                            request_id,
                        )
                        return "interrupted"

                # Reaching the slice boundary must run the same durable
                # presence/expiry decision as before. Do not insert one final
                # status poll ahead of it; the CAS/CTE below already resolves
                # decision races at that boundary.
                if asyncio.get_running_loop().time() >= deadline:
                    break
                current = await _read_status_interruptibly()
                if current in terminal_statuses:
                    return str(current)

            if _stateless_mode():
                lease_token = _current_stateless_lease_token()
                if lease_token is None:
                    # Lease loss is not a user decision. Leave the row pending
                    # for the successor/retirement path.
                    logger.info(
                        "Permission wait lost stateless owner "
                        "(req=%s) — leaving pending",
                        request_id,
                    )
                    return "interrupted"

                if _hard_interrupt_event is not None and _hard_interrupt_event.is_set():
                    logger.info(
                        "Permission wait interrupted (req=%s) — leaving pending",
                        request_id,
                    )
                    return "interrupted"

                try:
                    async with postgres_conn.acquire() as conn:
                        expiry = await expire_permission_if_untethered(
                            conn,
                            thread_id=str(_thread_id),
                            request_id=request_id,
                            lease_token=lease_token,
                        )
                        if (
                            expiry.status not in terminal_statuses
                            and expiry.owner_live
                            and expiry.live_for_seconds is None
                        ):
                            # A concurrent resolver may have won after the
                            # CTE's statement snapshot. Re-read before looping.
                            status_now = await conn.fetchval(
                                "SELECT status FROM thread_permission_requests "
                                "WHERE id = $1",
                                request_id,
                            )
                        else:
                            status_now = None
                except Exception as exc:
                    # Unknown presence must retain the card. A broken
                    # connection may recover; retry on a short bounded slice
                    # rather than fabricating a denial/expiry.
                    logger.warning(
                        "Permission presence check failed (req=%s): %s",
                        request_id,
                        exc,
                    )
                    wait_timeout = min(base_timeout, 5.0)
                    continue

                if expiry.status in terminal_statuses:
                    return str(expiry.status)
                if not expiry.owner_live:
                    logger.info(
                        "Permission owner fence rejected (req=%s) — leaving pending",
                        request_id,
                    )
                    return "interrupted"
                if expiry.live_for_seconds is not None:
                    # A tab closed just before this timeout should be
                    # reconsidered at its short presence deadline, not after
                    # another full five-minute permission slice.
                    wait_timeout = min(
                        base_timeout,
                        max(0.1, expiry.live_for_seconds + 0.05),
                    )
                    continue

                if status_now in terminal_statuses:
                    return str(status_now)
                wait_timeout = base_timeout
                continue

            if not _subscribers:
                # Untethered: nobody can answer. CAS-style expire — only if
                # nobody beat us to it. This acquisition is released as soon
                # as the boundary update and read complete.

                if _hard_interrupt_event is not None and _hard_interrupt_event.is_set():
                    logger.info(
                        "Permission wait interrupted (req=%s) — leaving pending",
                        request_id,
                    )
                    return "interrupted"
                async with postgres_conn.acquire() as conn:
                    await conn.execute(
                        "UPDATE thread_permission_requests "
                        "SET status = 'expired', decided_at = now(), "
                        "    decided_by = 'system' "
                        "WHERE id = $1 AND status = 'pending'",
                        request_id,
                    )
                    final = await conn.fetchval(
                        "SELECT status FROM thread_permission_requests WHERE id = $1",
                        request_id,
                    )
                # No row means it was retired out from under the CAS. The
                # question is gone unanswered, which is an expiry — reading it
                # as a denial would put a refusal in the transcript that no
                # user ever made.
                return str(final) if final is not None else "expired"

            # Tethered: a client is watching, so keep the question open.
            # Re-read at the slice boundary in case the decision committed
            # after the last within-slice poll.
            status_now = await _read_status_interruptibly()
            if status_now in terminal_statuses:
                return str(status_now)
            wait_timeout = base_timeout
    except InterruptedError:
        logger.info(
            "Permission wait interrupted (req=%s) — leaving pending",
            request_id,
        )
        return "interrupted"
    except Exception as e:
        # An infrastructure failure is not a user decision. Reporting 'denied'
        # here told the model the user refused a call they were never asked
        # about — the same fabricated denial the TTL used to produce, and just
        # as invisible: the row stays pending, so nothing in the DB records the
        # refusal the transcript claims. Leave the row pending and report a
        # non-decision; the caller parks the turn (NO_ANSWER) and the model
        # re-decides on the next one.
        logger.warning("Permission resolution wait failed (id=%s): %s", request_id, e)
        return "unavailable"


async def _resolve_pending_permission(
    decision: str,
    approval_id: Optional[str] = None,
    decided_by: str = "ws_client",
) -> Optional[Dict[str, Any]]:
    """UPDATE a pending permission row by id, or the most-recent-pending if
    no id given. Returns the resolved row dict or None if not found / no
    pending request matched."""
    if _session is None or _session.postgres_conn is None or _thread_id is None:
        return None
    if decision not in ("approved", "denied"):
        return None
    try:
        async with _session.postgres_conn.acquire() as conn:
            if approval_id is not None:
                row = await conn.fetchrow(
                    "UPDATE thread_permission_requests "
                    "SET status = $2, decided_at = now(), decided_by = $3 "
                    "WHERE id = $1 AND status = 'pending' "
                    "RETURNING id, status, tool_call_id, thread_id",
                    approval_id,
                    decision,
                    decided_by,
                )
            else:
                row = await conn.fetchrow(
                    "UPDATE thread_permission_requests "
                    "SET status = $2, decided_at = now(), decided_by = $3 "
                    "WHERE id = ("
                    "  SELECT id FROM thread_permission_requests "
                    "  WHERE thread_id = $1 AND status = 'pending' "
                    "  ORDER BY requested_at DESC LIMIT 1"
                    ") "
                    "RETURNING id, status, tool_call_id, thread_id",
                    _thread_id,
                    decision,
                    decided_by,
                )
        return dict(row) if row else None
    except Exception as e:
        logger.warning("Resolve pending permission failed: %s", e)
        return None


async def _loop_permission_check(
    tool_name: str, tool_args: Dict[str, Any], tool_call_id: str
) -> PermissionOutcome:
    """Resolve a supervised gate. INSERTs a pending row and waits for the DB
    status to flip via short-acquisition polling.

    Returns a THREE-state outcome, because a gate is a question to the user:

      APPROVED  — the user said yes; run the tool.
      DECLINED  — the user said no (or no answer is possible: the session is
                  gone / the DB can't hold a gate), so the call really is off.
      NO_ANSWER — the gate was never answered. NOT a refusal: the loop parks
                  the turn instead of telling the model the user denied it
                  (knowledge-history/done/supervised_parallel_gates_timeout_fabricates_denial.md).

    The race-fix from commit 3a1d265: if _terminate_session nulled _session
    while permission_check was being scheduled, this DECLINEs — the session is
    gone, the tool result has nowhere to land, and no later answer can arrive.
    """
    if _session is None:
        logger.warning(
            "permission_check fired with _session=None for tool %s — declining",
            tool_name,
        )
        return PermissionOutcome.DECLINED

    # Same authoritative edge as batch announcement, repeated for each gate:
    # a committed mid-turn mode change must affect the very next tool call,
    # even if DB NOTIFY delivery was delayed or lost.
    await _drain_current_thread_controls()
    if _session is None:
        return PermissionOutcome.DECLINED

    mode = _session.permission_mode

    # A mid-batch mode downgrade takes these early exits *before* the claim
    # block below, so this call's announced row — and every later one the new
    # mode auto-approves — would never be retired by anyone. Sweep them here,
    # scoped by the new mode so a shell call still gating under auto_accept
    # keeps the row its own gate is about to claim.
    if mode == "autonomous":
        await _retire_announced_permission_rows("mode downgraded to autonomous", mode)
        return PermissionOutcome.APPROVED

    if mode == "auto_accept":
        # Auto-accept reads and writes; still ask for shell commands. Reads
        # the shared _SHELL_TOOLS constant (not a local copy) so this stays
        # in lockstep with _gate_needed — two independently-maintained sets
        # could silently drift and leave a gated call with no announced row.
        if tool_name not in _SHELL_TOOLS:
            await _retire_announced_permission_rows(
                "mode downgraded to auto_accept", mode
            )
            return PermissionOutcome.APPROVED

    # Reserve this gate BEFORE the claim SELECT. `async with pool.acquire()`
    # exits through a yield point and the row id is not known until after the
    # awaited SELECT, so `_active_permission_request_id` cannot cover the gap:
    # a fire-and-forget mode.set sweep landing there would CAS-expire the very
    # row this gate is about to wait on, and the waiter's re-SELECT would read
    # 'expired' -> NO_ANSWER, parking the turn the user had just unblocked. The
    # tool_call_id IS known up front. Nothing may await between the add and the
    # try, and the finally must cover EVERY exit below — including the two
    # early returns — or the reservation strands a row no sweep will touch.
    _gates_in_flight.add(tool_call_id)
    try:
        # Phase 5 wake path: if this tool_call_id was already resolved (typical
        # case: user clicked the magic-link approve/deny while the agent was
        # suspended; on wake LangGraph restores the same tool_call_id from
        # checkpoint), reuse that decision instead of inserting a fresh
        # request. We only honor terminal 'approved'/'denied' here — 'expired'
        # means the prior request timed out without a user response, so the
        # new attempt deserves a fresh prompt. A 'pending' row means the batch
        # announce (_loop_announce_permission_batch) already inserted it —
        # claim that row instead of inserting a second one for the same
        # tool_call_id (there is no unique constraint to stop a duplicate).
        claimed_request_id: Optional[str] = None
        if _session.postgres_conn is not None and _thread_id is not None:
            try:
                async with _session.postgres_conn.acquire() as conn:
                    existing = await conn.fetchrow(
                        "SELECT id, status FROM thread_permission_requests "
                        "WHERE thread_id = $1 AND tool_call_id = $2 "
                        "  AND status IN ('approved', 'denied', 'pending') "
                        "ORDER BY decided_at DESC NULLS LAST, requested_at DESC "
                        "LIMIT 1",
                        _thread_id,
                        tool_call_id,
                    )
                if existing is not None and existing["status"] != "pending":
                    decision = existing["status"]
                    _session.tool_decisions[tool_call_id] = decision
                    logger.info(
                        "Phase 5 wake: reusing prior %s decision for tool_call %s "
                        "(tool=%s)",
                        decision,
                        tool_call_id,
                        tool_name,
                    )
                    return (
                        PermissionOutcome.APPROVED
                        if decision == "approved"
                        else PermissionOutcome.DECLINED
                    )
                if existing is not None:
                    # A batch announce already inserted this row. Claim it —
                    # inserting again would orphan a card nobody waits on.
                    claimed_request_id = str(existing["id"])
            except Exception as e:
                # Soft-fail: fall through to the regular INSERT-and-wait path…
                logger.warning(
                    "Wake-path SELECT for tool_call %s failed (%s); falling back",
                    tool_call_id,
                    e,
                )
                # …unless the announce step still remembers the row it created
                # for this exact tool_call_id. Inserting a SECOND row would hand
                # the waiter a NEW approval_id while the card keeps showing the
                # announced one; the user's decision then resolves a row nobody
                # is listening on and the turn blocks forever with the card gone.
                remembered = _announced_permission_rows.get(tool_call_id)
                if remembered is not None:
                    claimed_request_id = remembered[0]

        # Supervised mode (or shell under auto_accept): ask user via the
        # durable permission table, then wait via short-acquisition polling.
        if claimed_request_id is not None:
            request_id = claimed_request_id
        else:
            request_id = await _insert_permission_request(
                tool_call_id, tool_name, tool_args
            )
            if request_id is None:
                # DB unavailable — conservative deny rather than risk silent
                # auto-approval. Logged at WARNING by the insert helper. This is a
                # real DECLINE, not a park: with no durable row there is nothing a
                # later approval could resolve.
                if _session is not None:
                    _session.tool_decisions[tool_call_id] = "denied"
                return PermissionOutcome.DECLINED

            # Broadcast carries both ids so clients can refer back via either.
            # Skipped when the row was claimed: the batch frame already
            # announced it, and a second frame would duplicate the card.
            _broadcast(
                "permission.request",
                {
                    "id": tool_call_id,
                    "approval_id": request_id,
                    "tool": tool_name,
                    "args": _safe_serialize(tool_args),
                },
            )

        # Phase 5: sudo gate hit untethered is the second natural-pause site.
        # Flip the thread so the attention-sleep watchdog can fire after the
        # configured TTL. Idempotent against the _loop_get_user_input write.
        # Officer sessions never flip (centurion.md §4) — their pending gate
        # surfaces via the sitrep instead.
        if (
            _officer_cfg() is None
            and _orchestrator_client is not None
            and _thread_id is not None
        ):
            if _stateless_mode():
                await _safe_mark_stateless_natural_pause(require_untethered=True)
            elif not _subscribers:
                asyncio.create_task(
                    _safe_set_thread_status("awaiting_user"),
                    name="phase5-flip-awaiting-user-sudo",
                )

        # Publish the row we are blocked on so a concurrent sweep (mode.set from
        # the WS task) can't expire it under us and turn a live question into a
        # parked turn.
        global _active_permission_request_id
        _active_permission_request_id = request_id
        try:
            final_status = await _wait_for_permission_resolution(request_id)
        finally:
            _active_permission_request_id = None
        # Three-state, deliberately NOT collapsed to a bool: 'expired' means the
        # question was never answered, which is not the user refusing.
        if final_status == "approved":
            outcome = PermissionOutcome.APPROVED
        elif final_status == "denied":
            outcome = PermissionOutcome.DECLINED
        else:
            # 'expired' / 'interrupted' / 'unavailable' — and any status a
            # later change adds. Unanswered, so park. Only an explicit
            # 'denied' may ever be reported to the model as a refusal.
            outcome = PermissionOutcome.NO_ANSWER
        if _session is not None:
            # Record the raw status so audit can tell a timeout from a refusal.
            _session.tool_decisions[tool_call_id] = final_status
        # Journal the outcome too: SSE replay-from-cursor re-delivers the
        # permission.request frame, and without a matching resolution event a
        # reloading client resurrects an already-decided approval card — whose
        # re-click then 409s (session_silent_failure_audit.md #10).
        _broadcast(
            "permission.resolved",
            {
                "id": tool_call_id,
                "approval_id": request_id,
                "decision": final_status,
            },
        )
        return outcome
    finally:
        _gates_in_flight.discard(tool_call_id)


async def _resilient_cloud_sync(
    op: str,
    runner,
    turn_id: int,
    *,
    broadcast_errors: Optional[Callable[[], bool]] = None,
    retry_allowed: Optional[Callable[[], bool]] = None,
) -> bool:
    """Run a cloud_sync op with retry+backoff; surface failure without crashing.

    Retries the bound coroutine factory ``runner`` up to three times with
    exponential backoff against transient ``CloudSyncError`` (e.g. Cloudflare
    502 between agent and OpenCloud edge). On final failure broadcasts
    ``workspace_sync.error`` so the cockpit can surface a retry affordance,
    logs a warning, and returns False — the persistent loop keeps running
    instead of terminating the entire session over a transient infra hiccup.
    """
    from agent.services.cloud_sync import CloudSyncError

    attempts = 3
    delay = 1.0
    last_error: Optional[BaseException] = None
    for attempt in range(1, attempts + 1):
        try:
            await runner()
            return True
        except CloudSyncError as e:
            last_error = e
            if retry_allowed is not None and not retry_allowed():
                break
            if attempt < attempts:
                logger.warning(
                    "workspace_sync %s attempt %d/%d failed; retrying in %.0fs: %s",
                    op,
                    attempt,
                    attempts,
                    delay,
                    e,
                )
                await asyncio.sleep(delay)
                delay *= 2
    logger.warning(
        "workspace_sync %s failed after %d attempts (turn %s): %s",
        op,
        attempt,
        turn_id,
        last_error,
    )
    if broadcast_errors is None or broadcast_errors():
        _broadcast(
            "workspace_sync.error",
            {
                "op": op,
                "turn_id": turn_id,
                "message": str(last_error) if last_error else "unknown error",
            },
        )
    return False


@dataclass
class _CloudWriterFence:
    """Which fence guards this writer's remote writes right now.

    ``lease`` while the run_queue unit is leased; ``push`` once the executor
    handed the push its own ``push_owner_token`` (step 4a). Mutable on
    purpose: the push task captured its claim before the hand-off and must
    see the flip on its next ``before_write``.
    """

    kind: str = "lease"
    push_owner_token: int = 0


@dataclass(frozen=True)
class _CloudGenerationClaim:
    thread_id: str
    lease_token: int
    workspace_generation: str
    postgres: Any
    lease_handle: Any
    fence: _CloudWriterFence = field(default_factory=_CloudWriterFence)


@dataclass
class _BackgroundCloudPush:
    thread_id: str
    task: asyncio.Task
    claim: _CloudGenerationClaim
    sync: Any
    heartbeat_task: Optional[asyncio.Task] = None
    started_at: float = field(default_factory=time.monotonic)


def _pod_identity() -> tuple[str, str]:
    return (
        str(os.getenv("POD_NAME") or os.getenv("HOSTNAME") or socket.gethostname()),
        str(os.getenv("POD_UID") or ""),
    )


def _capture_cloud_generation_claim(sync: Any) -> _CloudGenerationClaim:
    """Freeze the current claim identity for callbacks/background work."""

    handle = _current_lease_var.get()
    if (
        handle is None
        or not handle.active
        or not _thread_id
        or str(handle.unit_id) != str(_thread_id)
        or _session is None
        or _session.postgres_conn is None
        or not str(getattr(sync, "workspace_generation", ""))
    ):
        raise LeaseLostError(
            "stateless cloud sync lacks an exact queue/workspace generation"
        )
    return _CloudGenerationClaim(
        thread_id=str(_thread_id),
        lease_token=int(handle.lease_token),
        workspace_generation=str(sync.workspace_generation),
        postgres=_session.postgres_conn,
        lease_handle=handle,
    )


async def _assert_cloud_generation_owner(claim: _CloudGenerationClaim) -> None:
    """Recheck the writer's fence + workspace incarnation before a remote write.

    Under the lease: queue token + workspace generation (S2). After the
    hand-off: the row's exact push_owner_token (step 4a) — a successor's
    adoption bumps it and this writer stops at its next write.
    """

    from shared.cloud_sync_generations import (
        cloud_sync_lease_is_current,
        cloud_sync_writer_is_current,
    )

    fence = claim.fence
    if fence.kind == "push":
        current = await cloud_sync_writer_is_current(
            claim.postgres,
            kind="push",
            thread_id=claim.thread_id,
            workspace_generation=claim.workspace_generation,
            push_owner_token=fence.push_owner_token,
        )
        if current:
            return
        raise LeaseLostError("cloud push write rejected by push-owner fence")
    # The lease kind keeps its S2 entry point (the alias) so existing seams
    # that patch ``cloud_sync_lease_is_current`` still fence this path.
    current = await cloud_sync_lease_is_current(
        claim.postgres,
        thread_id=claim.thread_id,
        lease_token=claim.lease_token,
        workspace_generation=claim.workspace_generation,
    )
    if current:
        return
    claim.lease_handle.mark_lost()
    raise LeaseLostError(
        "cloud sync write rejected by queue/workspace generation fence"
    )


async def _ack_cloud_generation(
    claim: _CloudGenerationClaim, mount_id: str, requirement: Any
) -> None:
    from shared.cloud_sync_generations import acknowledge_cloud_sync_generation

    fence = claim.fence
    acknowledged = await acknowledge_cloud_sync_generation(
        claim.postgres,
        thread_id=claim.thread_id,
        lease_token=claim.lease_token,
        mount_id=mount_id,
        generation=requirement.required_generation,
        workspace_generation=requirement.workspace_generation,
        sync_scope_sha256=requirement.sync_scope_sha256,
        baseline_sha256=requirement.baseline_sha256,
        push_owner_token=(fence.push_owner_token if fence.kind == "push" else None),
    )
    if acknowledged:
        return
    if fence.kind != "push":
        claim.lease_handle.mark_lost()
    raise LeaseLostError(
        f"cloud generation acknowledgement fenced for mount {mount_id}"
    )


async def _prepare_stateless_cloud_sync(sync: Any, turn_id: int) -> None:
    """Recover predecessor, pull, then arm this claim before any tool work."""

    from agent.services.cloud_sync.coordinator import CloudSyncGenerationError
    from shared.cloud_sync_generations import (
        arm_cloud_sync_generations,
        load_cloud_sync_requirements,
    )

    claim = _capture_cloud_generation_claim(sync)
    await _assert_cloud_generation_owner(claim)
    requirements = await load_cloud_sync_requirements(
        claim.postgres,
        thread_id=claim.thread_id,
        lease_token=claim.lease_token,
        workspace_generation=claim.workspace_generation,
    )
    # LOAD deliberately returns no rows when its owner CTE is fenced. Distinguish
    # that from a genuine first claim before treating an empty set as safe.
    await _assert_cloud_generation_owner(claim)

    async def acknowledge(mount_id: str, requirement: Any) -> None:
        await _ack_cloud_generation(claim, mount_id, requirement)

    async def before_write() -> None:
        await _assert_cloud_generation_owner(claim)

    async def adopt() -> Dict[str, Any]:
        # Step 4a: this claim is the thread's authority now. Bump the push
        # owner token so a predecessor still transmitting off-slot (any pod,
        # including this one) stops at its next write, and take its durable
        # progress so the replay below resumes instead of re-uploading.
        from shared.cloud_sync_generations import adopt_push_ownership

        pod_name, pod_uid = _pod_identity()
        adopted = await adopt_push_ownership(
            claim.postgres,
            thread_id=claim.thread_id,
            lease_token=claim.lease_token,
            workspace_generation=claim.workspace_generation,
            pod_name=pod_name,
            pod_uid=pod_uid,
        )
        if adopted:
            logger.info(
                "cloud push adopted: thread=%s mounts=%d resumed_files=%d",
                claim.thread_id,
                len(adopted),
                sum(len(p.get("files") or {}) for p in adopted.values()),
            )
        return adopted

    _broadcast("workspace_sync.reconciling", {"turn_id": turn_id})
    recovered = await _resilient_cloud_sync(
        "generation_recovery",
        lambda: sync.reconcile_before_pull(
            requirements,
            before_write=before_write,
            acknowledge=acknowledge,
            adopt=adopt,
        ),
        turn_id,
    )
    if not recovered:
        raise CloudSyncGenerationError(
            "predecessor cloud generation recovery failed; pull refused"
        )

    _broadcast("workspace_sync.pulling", {"turn_id": turn_id})
    if not await _resilient_cloud_sync(
        "pull",
        lambda: sync.pull_all(before_write=before_write, force_unknown=True),
        turn_id,
    ):
        raise CloudSyncGenerationError("stateless turn-start cloud pull failed")
    _broadcast("workspace_sync.pulled", {"turn_id": turn_id})

    scopes = await sync.capture_generation_scopes()
    armed = await arm_cloud_sync_generations(
        claim.postgres,
        thread_id=claim.thread_id,
        lease_token=claim.lease_token,
        scopes=scopes,
    )
    expected = {scope.mount_id for scope in scopes}
    if set(armed) != expected:
        raise CloudSyncGenerationError(
            "cloud generation arm was fenced or left a partial mount set"
        )
    sync.validate_requirements(armed)
    if _session is None:
        raise LeaseLostError("session detached while arming cloud generation")
    _session.cloud_sync_requirements = dict(armed)
    logger.info(
        "cloud generation armed: thread=%s lease=%d mounts=%d",
        claim.thread_id,
        claim.lease_token,
        len(armed),
    )


async def _assert_no_pending_stateless_cloud_generation() -> None:
    """Do not let an omitted cloud payload hide predecessor work."""

    if _session is None or not _session.cloud_sync_workspace_generation:
        return
    from agent.services.cloud_sync.coordinator import CloudSyncGenerationError
    from shared.cloud_sync_generations import load_cloud_sync_requirements

    handle = _current_lease_var.get()
    if (
        handle is None
        or not handle.active
        or not _thread_id
        or str(handle.unit_id) != str(_thread_id)
        or _session.postgres_conn is None
    ):
        raise LeaseLostError("stateless no-cloud check lacks an exact lease")
    claim = _CloudGenerationClaim(
        thread_id=str(_thread_id),
        lease_token=int(handle.lease_token),
        workspace_generation=_session.cloud_sync_workspace_generation,
        postgres=_session.postgres_conn,
        lease_handle=handle,
    )
    await _assert_cloud_generation_owner(claim)
    requirements = await load_cloud_sync_requirements(
        claim.postgres,
        thread_id=claim.thread_id,
        lease_token=claim.lease_token,
        workspace_generation=claim.workspace_generation,
    )
    await _assert_cloud_generation_owner(claim)
    pending = sorted(
        mount_id
        for mount_id, requirement in requirements.items()
        if requirement.acknowledged_generation < requirement.required_generation
    )
    if pending:
        raise CloudSyncGenerationError(
            "pending cloud generation has no configured recovery target: "
            + ", ".join(pending)
        )


class _PushProgressRecorder:
    """Batch landed-file progress into thread_cloud_sync_generations.

    Writes happen only under the push fence (after the hand-off); before it
    the entries accumulate and land on the first flush after the flip, so a
    successor never sees progress a lease-fenced writer could still lose.
    Every 8 files or 5 s, whichever first; the final flush is unconditional.
    """

    def __init__(self, claim: _CloudGenerationClaim) -> None:
        self._claim = claim
        self._pending: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._planned: Dict[str, int] = {}
        self._planned_written: set[str] = set()
        self._count = 0
        self._last_flush = time.monotonic()

    async def planned(self, mount_id: str, count: int) -> None:
        self._planned[mount_id] = int(count)
        await self._maybe_flush()

    async def record(self, mount_id: str, path: str, entry: Dict[str, Any]) -> None:
        self._pending.setdefault(mount_id, {})[path] = dict(entry)
        self._count += 1
        await self._maybe_flush()

    async def _maybe_flush(self) -> None:
        if self._count >= 8 or time.monotonic() - self._last_flush >= 5.0:
            await self.flush()

    async def flush(self) -> None:
        fence = self._claim.fence
        if fence.kind != "push" or fence.push_owner_token <= 0:
            return
        from shared.cloud_sync_generations import record_push_progress

        mounts = set(self._pending) | (set(self._planned) - self._planned_written)
        for mount_id in sorted(mounts):
            files = self._pending.pop(mount_id, {})
            planned = None
            if mount_id in self._planned and mount_id not in self._planned_written:
                planned = self._planned[mount_id]
            try:
                owned = await record_push_progress(
                    self._claim.postgres,
                    thread_id=self._claim.thread_id,
                    mount_id=mount_id,
                    push_owner_token=fence.push_owner_token,
                    files=files,
                    planned=planned,
                )
            except Exception:
                logger.warning(
                    "cloud push progress write failed (non-fatal): thread=%s mount=%s",
                    self._claim.thread_id,
                    mount_id,
                    exc_info=True,
                )
                self._pending.setdefault(mount_id, {}).update(files)
                continue
            if planned is not None and owned:
                self._planned_written.add(mount_id)
            if not owned:
                logger.info(
                    "cloud push progress refused: token no longer owns thread=%s mount=%s",
                    self._claim.thread_id,
                    mount_id,
                )
        self._count = 0
        self._last_flush = time.monotonic()


async def _run_turn_end_cloud_push(
    sync: Any,
    turn_id: int,
    *,
    requirements: Optional[Dict[str, Any]] = None,
    claim: Optional[_CloudGenerationClaim] = None,
    staged_event: Optional[asyncio.Event] = None,
) -> None:
    """Body of the background turn-end push task.

    Pinned lane: unchanged (push_all, same retry/backoff and broadcasts).
    Stateless lane (step 4a): stage every mount while the unit is leased,
    signal ``staged_event`` so the executor may hand off and complete, then
    transmit — recording durable progress and heartbeating once the fence has
    flipped to the push token. Broadcasts only while still under the lease:
    nothing is journaled after the hand-off.
    """
    if claim is None or claim.fence.kind == "lease":
        _broadcast("workspace_sync.pushing", {"turn_id": turn_id})
    runner = sync.push_all
    op = "push"
    recorder: Optional[_PushProgressRecorder] = None
    generation_staged = False
    if requirements is not None:
        if claim is None:
            raise LeaseLostError("generation push lacks a captured claim")
        recorder = _PushProgressRecorder(claim)

        async def before_write() -> None:
            await _assert_cloud_generation_owner(claim)

        async def acknowledge(mount_id: str, requirement: Any) -> None:
            await _ack_cloud_generation(claim, mount_id, requirement)

        async def generation_runner() -> Any:
            nonlocal generation_staged
            staged = await sync.stage_generation(requirements)
            generation_staged = True
            if staged_event is not None:
                staged_event.set()
            try:
                return await sync.transmit_generation(
                    staged,
                    before_write=before_write,
                    acknowledge=acknowledge,
                    progress=recorder.record,
                    planned=recorder.planned,
                )
            finally:
                await recorder.flush()

        runner = generation_runner
        op = "generation_push"
    try:
        ok = await _resilient_cloud_sync(
            op,
            runner,
            turn_id,
            broadcast_errors=lambda: claim is None or claim.fence.kind == "lease",
            # Once staging succeeds the executor can detach at any await.
            # A failed transmit has durable progress; recovery must rebuild
            # under a new owner instead of re-reading this old workspace.
            retry_allowed=lambda: not generation_staged,
        )
    finally:
        if staged_event is not None:
            staged_event.set()
    if ok:
        if claim is None or claim.fence.kind == "lease":
            _broadcast("workspace_sync.pushed", {"turn_id": turn_id})
        return
    if claim is not None and claim.fence.kind == "push":
        from shared.cloud_sync_generations import record_push_failure

        try:
            await record_push_failure(
                claim.postgres,
                thread_id=claim.thread_id,
                push_owner_token=claim.fence.push_owner_token,
                error=f"{op} failed after retries (turn {turn_id})",
            )
        except Exception:
            logger.debug("cloud push failure record skipped", exc_info=True)


async def _hand_off_cloud_push(
    db: Any,
    *,
    thread_id: str,
    lease_token: int,
    pod_name: str,
    pod_uid: str,
) -> bool:
    """Executor seam (step 4a): give the pending push its own fence.

    Runs under the still-live lease, before ``complete_unit``. One UPDATE
    stamps every pending generation row with a fresh ``push_owner_token``;
    the captured claim's fence flips to that token, the task moves into the
    background registry (so no teardown awaits it and no detach closes its
    coordinator), and a heartbeat starts. Returns False when nothing is
    pending — the push already acknowledged every mount — or when the
    pending task is not this claim's; the executor then waits it out as before.
    """

    global _pending_cloud_push_task, _pending_cloud_push_claim
    global _pending_cloud_push_sync, _pending_cloud_push_staged
    task = _pending_cloud_push_task
    claim = _pending_cloud_push_claim
    sync = _pending_cloud_push_sync
    if task is None:
        return False
    if task.done():
        _pending_cloud_push_task = None
        _pending_cloud_push_claim = None
        _pending_cloud_push_sync = None
        _pending_cloud_push_staged = None
        return False
    if (
        claim is None
        or str(claim.thread_id) != str(thread_id)
        or int(claim.lease_token) != int(lease_token)
    ):
        return False
    from shared.cloud_sync_generations import hand_off_push_ownership

    token = await hand_off_push_ownership(
        db,
        thread_id=thread_id,
        lease_token=lease_token,
        workspace_generation=claim.workspace_generation,
        pod_name=pod_name,
        pod_uid=pod_uid,
    )
    if token is None:
        # Nothing pending in the database: the transmit already acknowledged
        # everything (or had nothing to write). The task is finishing; let the
        # executor's ordinary wait collect it.
        return False
    claim.fence.push_owner_token = int(token)
    claim.fence.kind = "push"
    entry = _BackgroundCloudPush(
        thread_id=str(thread_id), task=task, claim=claim, sync=sync
    )
    entry.heartbeat_task = asyncio.create_task(
        _push_owner_heartbeat_loop(claim),
        name=f"cloud-push-heartbeat-{str(thread_id)[:8]}",
    )
    _background_cloud_pushes[str(thread_id)] = entry
    task.add_done_callback(lambda _t, e=entry: _on_background_push_done(e))
    _pending_cloud_push_task = None
    _pending_cloud_push_claim = None
    _pending_cloud_push_sync = None
    _pending_cloud_push_staged = None
    logger.info(
        "cloud push handed off: thread=%s lease=%d push_owner_token=%d pod=%s",
        thread_id,
        lease_token,
        int(token),
        pod_name,
    )
    return True


async def _push_owner_heartbeat_loop(claim: _CloudGenerationClaim) -> None:
    from shared.cloud_sync_generations import heartbeat_push_owner

    try:
        while True:
            await asyncio.sleep(20.0)
            if claim.fence.kind != "push":
                return
            try:
                alive = await heartbeat_push_owner(
                    claim.postgres,
                    thread_id=claim.thread_id,
                    push_owner_token=claim.fence.push_owner_token,
                )
            except Exception:
                logger.debug("cloud push heartbeat failed", exc_info=True)
                continue
            if not alive:
                return
    except asyncio.CancelledError:
        raise


def _background_push_owns(sync: Any) -> bool:
    """True when a handed-off push still transmits through ``sync``."""

    if sync is None:
        return False
    return any(
        entry.sync is sync and not entry.task.done()
        for entry in _background_cloud_pushes.values()
    )


def _on_background_push_done(entry: _BackgroundCloudPush) -> None:
    """Registry cleanup when a handed-off push ends (any outcome)."""

    if entry.heartbeat_task is not None and not entry.heartbeat_task.done():
        entry.heartbeat_task.cancel()
    if _background_cloud_pushes.get(entry.thread_id) is entry:
        _background_cloud_pushes.pop(entry.thread_id, None)
    task = entry.task
    outcome = (
        "cancelled"
        if task.cancelled()
        else ("failed" if task.exception() is not None else "done")
    )
    logger.info(
        "cloud push background task %s: thread=%s push_owner_token=%d after %.1fs",
        outcome,
        entry.thread_id,
        entry.claim.fence.push_owner_token,
        time.monotonic() - entry.started_at,
    )
    # The coordinator belongs to the push once handed off; close it unless a
    # live session still uses the very same object (warm reuse / adoption on
    # this pod), in which case the session's teardown owns it.
    session = _session
    if session is not None and getattr(session, "workspace_sync", None) is entry.sync:
        return
    close = getattr(entry.sync, "aclose", None)
    if close is not None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(_aclose_quietly(entry.sync))


async def _aclose_quietly(sync: Any) -> None:
    try:
        await sync.aclose()
    except Exception:
        logger.debug("cloud sync aclose after background push failed", exc_info=True)


async def _cancel_background_cloud_push(thread_id: str) -> None:
    """A new claim of this thread on this pod supersedes its own old push.

    Adoption bumps the DB token anyway; cancelling first avoids a needless
    fenced write and lets the recovery resume from the durable progress.
    """

    entry = _background_cloud_pushes.get(str(thread_id))
    if entry is None or entry.task.done():
        return
    entry.task.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await asyncio.wait_for(asyncio.shield(entry.task), timeout=15.0)


async def _await_background_cloud_pushes(timeout: float) -> None:
    """Executor shutdown seam: wait (bounded) for handed-off pushes."""

    tasks = [
        entry.task
        for entry in list(_background_cloud_pushes.values())
        if not entry.task.done()
    ]
    if not tasks:
        return
    logger.info(
        "waiting up to %.0fs for %d handed-off cloud push(es) before exit",
        timeout,
        len(tasks),
    )
    done, pending = await asyncio.wait(tasks, timeout=max(0.0, timeout))
    if pending:
        logger.warning(
            "%d handed-off cloud push(es) still running at exit; progress is "
            "durable and the thread's next claim adopts the rest",
            len(pending),
        )


async def _await_pending_cloud_push() -> None:
    """Wait out the background turn-end push, if one is in flight.

    Failure handling lives inside the task (``_resilient_cloud_sync`` never
    raises), so this only guards against the task machinery itself; a broken
    push must delay the next pull, not kill the turn or the teardown that
    called us.
    """
    global _pending_cloud_push_task, _pending_cloud_push_claim
    global _pending_cloud_push_sync, _pending_cloud_push_staged
    task = _pending_cloud_push_task
    _pending_cloud_push_task = None
    _pending_cloud_push_claim = None
    _pending_cloud_push_sync = None
    _pending_cloud_push_staged = None
    if task is None:
        return
    try:
        await task
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.warning("Turn-end cloud push task failed", exc_info=True)


async def _retry_cloud_sync_start(turn_id: int) -> None:
    """Re-attempt a cloud sync that failed to start at attach.

    The attach-time resolution is a single snapshot read: an agent that lost
    the race against session-folder provisioning (or hit a transient WebDAV
    failure) kept ``workspace_sync = None`` for the session's WHOLE life,
    because every use site is guarded and nothing rebuilt it. One retry per
    turn boundary turns that permanent loss into a late start.

    Silent by design after the first failure — the degraded toast was already
    raised at attach, and re-broadcasting it every turn would be noise. Only
    the transition back to working is announced.
    knowledge-history/done/session_resume_cloud_sync_race_late_provision.md
    """
    global _cloud_sync_retry_pending

    if _session is None or not _orchestrator_client or not _thread_id:
        return
    if (
        _session.workspace_manager is None
        or not _session.workspace_manager.backend.supports_file_tools
    ):
        _cloud_sync_retry_pending = False
        return
    try:
        ws_info = await _orchestrator_client.get_thread_workspace(_thread_id)
    except Exception:
        return
    if not ws_info:
        return

    # F-C1 fail-closed: a protected thread's only sanctioned live-write surface
    # is the capture overlay. Never let a retry hand it a live WebDAV sync —
    # and stop retrying, since that verdict cannot change mid-session.
    if ws_info.get("protected_cloud"):
        _cloud_sync_retry_pending = False
        return

    cloud_cfg = ws_info.get("cloud_sync")
    if not cloud_cfg and ws_info.get("nc_session_folder"):
        cloud_cfg = _legacy_nc_cloud_cfg(ws_info["nc_session_folder"])
    if not cloud_cfg:
        if _stateless_mode() and not ws_info.get("cloud_sync_degraded"):
            # A successful authoritative response says there is deliberately
            # no mirror. This is different from the transport ambiguity that
            # set the retry flag and is safe to clear.
            _cloud_sync_retry_pending = False
        return  # still nothing to sync to — or explicit no-cloud configuration

    try:
        coordinator = _build_sync_coordinator(
            workspace_path=_session.workspace_manager.path,
            workspace_backend=_session.workspace_manager.backend,
            cloud_cfg=cloud_cfg,
            thread_id=str(_thread_id),
            workspace_generation=str(ws_info.get("workspace_generation") or ""),
        )
        if not coordinator:
            return
        # A stateless successor must reconcile the prior required generation
        # before its first pull. Construct-only here; the single fenced
        # turn-start path below performs recovery -> pull -> arm in order.
        if not _stateless_mode():
            await coordinator.pull_all()
    except Exception as e:
        # Keep the flag set: the target exists but isn't usable yet. Don't
        # leave a half-built coordinator behind for the push at turn end.
        logger.warning("Cloud sync retry failed on turn %s: %s", turn_id, e)
        return

    _session.workspace_sync = coordinator
    _cloud_sync_retry_pending = False
    logger.info(
        "Cloud workspace sync recovered on turn %s (%d mount(s))",
        turn_id,
        len(coordinator),
    )
    _broadcast("workspace_sync.recovered", {"turn_id": turn_id})


async def _loop_on_turn_start(turn_id: int) -> None:
    global _turn_event_open, _turn_tool_execution_identity
    if _session is None:
        _turn_event_open = False
        _turn_tool_execution_identity = None
        return
    _session.turn_count = turn_id
    _turn_tool_execution_identity = None
    _turn_event_open = True
    hook = _turn_start_external_hook
    if hook is not None:
        try:
            await hook(turn_id)
        except BaseException:
            # Do not publish a turn as interruptible when its exact admission
            # and consumer could not be armed. The loop will run its normal
            # terminal callback for this failed turn.
            _turn_event_open = False
            _clear_loop_interrupt(target_turn_id=turn_id)
            raise
    _broadcast("turn.started", {"turn_id": turn_id})

    # Cloud sync never started for this session (lost the race against
    # session-folder provisioning, or the initial pull failed). Retry before
    # the pull below so a recovered session syncs from this turn on.
    if _cloud_sync_retry_pending and _session.workspace_sync is None:
        await _retry_cloud_sync_start(turn_id)
        if (
            _stateless_mode()
            and _cloud_sync_retry_pending
            and _session.workspace_sync is None
        ):
            from agent.services.cloud_sync.coordinator import CloudSyncGenerationError

            raise CloudSyncGenerationError(
                "stateless cloud sync remains degraded; tool work refused"
            )
    if _stateless_mode() and _session.workspace_sync is None:
        await _assert_no_pending_stateless_cloud_generation()
    # The previous turn's push may still be flushing in the background —
    # wait it out before pulling so each mount keeps strict push→pull
    # ordering (and the pull's remote listing reflects the last turn's
    # writes). This is where a too-fast reply pays the push cost: inside a
    # started turn, visibly, instead of in an invisible pre-turn queue.
    if _stateless_mode() and _thread_id:
        # Step 4a: a push this pod handed off for THIS thread is superseded by
        # the new claim — cancel it and let generation recovery adopt+resume.
        await _cancel_background_cloud_push(str(_thread_id))
    await _await_pending_cloud_push()

    # Phase 1 of cloud_collaboration_model.md §9: pull cloud-side edits
    # before the turn runs so the agent sees the latest user-side state.
    # On transient failure (Cloudflare/edge hiccup) we retry+surface via
    # workspace_sync.error rather than letting the exception kill the loop.
    if _session.workspace_sync:
        if _stateless_mode():
            await _prepare_stateless_cloud_sync(_session.workspace_sync, turn_id)
        else:
            _broadcast("workspace_sync.pulling", {"turn_id": turn_id})
            if await _resilient_cloud_sync(
                "pull", _session.workspace_sync.pull_all, turn_id
            ):
                _broadcast("workspace_sync.pulled", {"turn_id": turn_id})

    # User-message persistence moved to accept time (_accept_user_input) plus
    # the loop's per-append upsert (persist_message). The content-based save
    # that lived here read the *most recent* content global for every queued
    # turn, so multiple queued inputs all persisted the last message's text
    # (session_silent_failure_audit.md #1).


async def _loop_on_usage(payload: Dict[str, Any]) -> None:
    """Per-LLM-call token telemetry → usage.updated frame (cockpit panel)."""
    _broadcast(
        "usage.updated",
        {
            "turn": (_session.turn_count + 1) if _session else None,
            **payload,
        },
    )


def _wire_session_aux_archiver() -> None:
    """Point the session's AuxiliaryLLM at the default archiver + thread context.

    Worker jobs wire this in ``UniversalAgent.process_job``; persistent sessions
    never did, so auxiliary failures/calls (memory extraction, title generation)
    produced no ``llm_requests`` row — invisible in the debug view. The session
    aux LLM is a shared instance (``_session.memory_service.runtime.auxiliary_llm``
    is the same object), so wiring it once keeps every aux path archived.

    Idempotent and cheap (just assigns three fields); fire-and-forget. Uses
    ``job_id=_thread_id`` + ``agent_type="persistent"`` to match the session
    main-call archiving in ``_loop_archive_llm_call``. See
    knowledge-base/knowledge/issues/surface_silent_aux_failures.md (Phase 1.6).
    """
    if _session is None or getattr(_session, "auxiliary_llm", None) is None:
        return
    gate_setter = getattr(_session.auxiliary_llm, "set_provider_admission_gate", None)
    if callable(gate_setter):
        # This belongs beside archiver wiring because every aux rebuild already
        # converges through this function. A hot swap must not detach either
        # observability or the termination no-spend fence.
        gate_setter(_loop_auxiliary_provider_admission_open)
    if not _thread_id:
        return
    try:
        from agent.core.archiver import get_archiver

        archiver = get_archiver()
        if archiver is not None:
            _session.auxiliary_llm.set_job_context(
                archiver=archiver, job_id=_thread_id, agent_type="persistent"
            )
    except Exception as e:
        logger.debug(f"Could not wire session aux archiver (non-fatal): {e}")


def _should_notify_cloud_stage() -> bool:
    """True when the session's protected-cloud capture overlay is mounted and
    active — gates the turn-end staging ping (Slice C, design §5). Split out
    from ``_loop_on_turn_complete`` for unit-testability; overlay absence or
    inactivity (unprotected session, or a protected one whose overlay failed
    to mount — see Slice B's fail-safe in persistent_session.py) is the
    common case and must be a silent no-op, never an error."""
    overlay = getattr(_session, "overlay_mount_manager", None)
    return overlay is not None and overlay.active


async def _notify_cloud_stage(
    thread_id: str | None = None,
    *,
    agent_id: str | None = None,
    session_runtime_generation: str | None = None,
    session_runtime_attach_token: str | None = None,
) -> None:
    """Fire-and-forget turn-end staging ping (protected cloud, Slice C).

    Never raises — staging failure must not touch the turn. Mirrors the
    internal-call header pattern in
    ``src/agent/tools/communication/messaging.py:197-226``.
    """
    orchestrator_url = os.getenv("ORCHESTRATOR_URL", "http://localhost:8085")
    headers: dict[str, str] = {}
    internal_key = os.getenv("MCP_INTERNAL_KEY", "")
    if internal_key:
        headers["X-Internal-Key"] = internal_key
    target_thread_id = str(thread_id or _thread_id or "")
    target_agent_id = str(
        agent_id or getattr(_orchestrator_client, "agent_id", None) or ""
    )
    target_generation = _canonical_runtime_generation(
        session_runtime_generation or _session_runtime_generation
    )
    target_attach_token = _canonical_runtime_generation(
        session_runtime_attach_token or _session_runtime_attach_token
    )
    if not target_thread_id or not target_agent_id or target_generation is None:
        return
    headers["X-Agent-ID"] = target_agent_id
    headers["X-Session-Runtime-Generation"] = target_generation
    if target_attach_token is not None:
        headers["X-Session-Runtime-Attach-Token"] = target_attach_token
    try:
        async with httpx.AsyncClient(timeout=10.0, headers=headers) as client:
            await client.post(
                f"{orchestrator_url}/api/agents/threads/{target_thread_id}/cloud-stage"
            )
    except Exception as e:
        logger.debug(f"cloud-stage ping failed (non-fatal): {e}")


async def _loop_on_workspace_commit(sha: str) -> None:
    """Record a workspace commit against the current transcript position.

    Best-effort: a miss only degrades rewind code-restore granularity for
    this turn (the resolver falls back to the previous mapped commit).
    """
    if _session is None or _session.postgres_conn is None or _thread_id is None:
        return
    try:
        await _session.postgres_conn.record_turn_commit(_thread_id, sha)
    except Exception:
        logger.warning("record_turn_commit failed (non-fatal)", exc_info=True)


async def _loop_on_turn_complete(
    turn_id: int,
    metrics: Optional[dict] = None,
    turn_input_message_id: Optional[str] = None,
    memory_scope_kind: Optional[str] = None,
    memory_scope_id: Optional[str] = None,
    *,
    skip_message_reconcile: bool = False,
) -> None:
    global _turn_event_open
    # This is the transcript terminal edge. Clear before any awaited cleanup so
    # a reattach during turn-end persistence never reopens an already-completed
    # assistant bubble merely because the broader loop is not parked yet.
    _turn_event_open = False
    _clear_loop_interrupt(target_turn_id=turn_id)
    await _loop_on_turn_complete_body(
        turn_id,
        metrics,
        turn_input_message_id=turn_input_message_id,
        memory_scope_kind=memory_scope_kind,
        memory_scope_id=memory_scope_id,
        skip_message_reconcile=skip_message_reconcile,
    )


async def _loop_on_turn_settled(turn_id: int) -> None:
    """Publish the physical-executor edge after all turn-owned work settles.

    ``_loop_on_turn_complete`` persists the transcript before the Git mapping,
    so it cannot be the detach authorization for a stateless claimant.  The
    persistent loop invokes this callback in a ``finally`` after memory,
    commit, push, and turn-ledger mapping; only then may the executor cancel or
    reuse the loop on another pod/claim.
    """

    hook = _turn_complete_external_hook
    if hook is not None:
        try:
            hook(turn_id)
        except Exception:
            logger.warning(
                "External turn-settled hook failed (non-fatal)",
                exc_info=True,
            )
    # Deliberately do NOT clear the external-effect identity here. The hook is
    # only a process-local settlement notification; the executor still has to
    # close interrupt admission, detach any physical owner, and commit the
    # run_queue disposition. Shutdown in that gap must retain the post-effect
    # classification. The executor clears the exact identity after its durable
    # complete/park/release CAS.


# Turn-complete reconcile: bounded attempts + per-attempt timeout. The
# reconcile is an idempotent upsert transaction (ON CONFLICT (id); the memory
# effect is minted-or-reused), so a retry after a timeout or a dropped
# connection is safe — and on the stateless lane a settlement that gives up
# parks the whole thread, so one transient DB hiccup must not do that.
_TURN_RECONCILE_ATTEMPTS = 3
_TURN_RECONCILE_TIMEOUT_S = 5.0
_TURN_RECONCILE_RETRY_DELAY_S = 0.5


def _is_transient_db_error(exc: BaseException) -> bool:
    """Connection-level / timeout failures a settlement retry can recover from.

    Contract failures (``ValueError``/``RuntimeError`` from the reconcile
    itself, a lost lease) are deterministic and never retried.
    """
    if isinstance(exc, LeaseLostError):
        return False
    if isinstance(exc, (asyncio.TimeoutError, ConnectionError)):
        return True
    try:
        import asyncpg
    except ImportError:  # pragma: no cover
        return False
    transient = (
        asyncpg.PostgresConnectionError,
        asyncpg.InterfaceError,
        asyncpg.exceptions.DeadlockDetectedError,
        asyncpg.exceptions.SerializationError,
    )
    return isinstance(exc, transient)


async def _reconcile_turn_with_retry(
    turn_id: int,
    *,
    metrics: Optional[dict],
    authoritative_turn_boundary: bool,
    turn_input_message_id: Optional[str],
    memory_scope_kind: Optional[str],
    memory_scope_id: Optional[str],
) -> None:
    """Run ``_save_turn_ai_messages`` with bounded retries on transient errors.

    Pinned keeps its historical best-effort shape (a timeout is logged and
    the turn proceeds). Stateless: the final failure propagates so the
    caller refuses settlement — the queue generation stays retryable — but
    only after every attempt is spent.
    """
    if _session is None:
        return
    for attempt in range(1, _TURN_RECONCILE_ATTEMPTS + 1):
        try:
            await asyncio.wait_for(
                _save_turn_ai_messages(
                    _session.postgres_conn,
                    _thread_id,
                    _session.messages,
                    turn_id,
                    metrics=metrics,
                    tool_decisions=dict(_session.tool_decisions),
                    authoritative_turn_boundary=authoritative_turn_boundary,
                    turn_input_message_id=turn_input_message_id,
                    memory_scope_kind=memory_scope_kind,
                    memory_scope_id=memory_scope_id,
                ),
                timeout=_TURN_RECONCILE_TIMEOUT_S,
            )
            return
        except Exception as exc:
            if not _is_transient_db_error(exc):
                raise
            if not authoritative_turn_boundary:
                logger.warning(
                    "AI message save failed transiently (%s) — proceeding",
                    exc if not isinstance(exc, asyncio.TimeoutError) else "timeout",
                )
                return
            if attempt >= _TURN_RECONCILE_ATTEMPTS:
                logger.error(
                    "Authoritative stateless turn persist failed after %d "
                    "attempts (%s); refusing turn settlement",
                    attempt,
                    exc if not isinstance(exc, asyncio.TimeoutError) else "timeout",
                )
                raise
            logger.warning(
                "Authoritative stateless turn persist attempt %d/%d failed "
                "transiently (%s); retrying",
                attempt,
                _TURN_RECONCILE_ATTEMPTS,
                exc if not isinstance(exc, asyncio.TimeoutError) else "timeout",
            )
            await asyncio.sleep(_TURN_RECONCILE_RETRY_DELAY_S * attempt)


async def _loop_on_turn_complete_body(
    turn_id: int,
    metrics: Optional[dict] = None,
    *,
    turn_input_message_id: Optional[str] = None,
    memory_scope_kind: Optional[str] = None,
    memory_scope_id: Optional[str] = None,
    skip_message_reconcile: bool = False,
) -> None:
    # Runs on EVERY turn exit — completed, parked on an unanswered gate,
    # interrupted, or errored (run_persistent_loop catches and still calls
    # this). Announced rows no gate ever claimed must die with the turn, or
    # they resurface on reattach as an approval card that runs nothing.
    await _retire_announced_permission_rows(f"turn {turn_id} ended")
    if _session is None:
        return
    authoritative_turn_boundary = _stateless_mode()
    if authoritative_turn_boundary and skip_message_reconcile:
        raise RuntimeError(
            "stateless turn cannot skip its authoritative transcript boundary"
        )
    if not authoritative_turn_boundary:
        # Preserve the pinned lane's historical UI ordering. Stateless moves
        # this terminal edge below its authoritative transaction: publishing a
        # completed frame before a fence loss would make a retry look complete.
        _broadcast("turn.completed", {"turn_id": turn_id, "metrics": metrics or {}})
    if skip_message_reconcile:
        # A cancelled/deferred/unresolved input did no admitted work. Close the
        # live UI turn but run no transcript walk, title model, cloud push, or
        # protected-stage side effect against a previous turn's state.
        _session.tool_decisions.clear()
        return
    # Ensure the (shared) session aux LLM logs to llm_requests — covers the
    # title call below plus the observer/extraction paths that reuse it.
    _wire_session_aux_archiver()
    # Save AI messages from this turn straight to the DB (bounded await). Direct
    # write via the agent's own pool — the orchestrator REST hop is bypassed.
    # On the stateless lane this exact transaction also mints the durable memory
    # effect, so failure must abort settlement and leave the queue generation
    # retryable. Pinned sessions retain the historical best-effort behavior.
    if _session.postgres_conn:
        await _reconcile_turn_with_retry(
            turn_id,
            metrics=metrics,
            authoritative_turn_boundary=authoritative_turn_boundary,
            turn_input_message_id=turn_input_message_id,
            memory_scope_kind=memory_scope_kind,
            memory_scope_id=memory_scope_id,
        )
    elif authoritative_turn_boundary:
        raise RuntimeError(
            "authoritative stateless turn persist requires a Postgres connection"
        )
    if authoritative_turn_boundary:
        _broadcast("turn.completed", {"turn_id": turn_id, "metrics": metrics or {}})
    _session.tool_decisions.clear()

    # Auto-generate title after first few turns. Awaited (not fire-and-forget)
    # so the title.updated broadcast is enqueued onto every subscriber's WS
    # queue before this callback returns — otherwise a crash in the next
    # turn-start hook (e.g. workspace_sync.error) can tear down the WS before
    # the title frame is flushed, leaving the cockpit header stuck on
    # "Untitled Session" until a manual refetch.
    if turn_id <= 3 and _session.postgres_conn and not _termination_admission_closed():
        await _auto_title_after_first_turn()

    # Phase 1 of cloud_collaboration_model.md §9: push the agent's edits to
    # every mounted cloud surface. Runs as a BACKGROUND task — the old inline
    # await here held the loop for the whole push (minutes on a fresh pod),
    # after turn.completed had already gone out, so queued input sat in an
    # invisible pre-turn limbo. The next turn's start hook awaits the task
    # before its pull, preserving push→pull ordering per mount; teardown
    # awaits it before the final sync. Retry + workspace_sync.* broadcasts
    # are unchanged inside _run_turn_end_cloud_push.
    # knowledge-base/knowledge/issues/session_turn_end_cloud_push_blocks_queued_input.md
    if _session.workspace_sync:
        global _pending_cloud_push_task
        # Only reachable with no task pending (turn start awaited it), but a
        # future caller must never let two pushes walk one mount concurrently.
        await _await_pending_cloud_push()
        if _stateless_mode():
            requirements = dict(_session.cloud_sync_requirements)
            if not requirements:
                raise LeaseLostError(
                    "stateless turn completed without an armed cloud generation"
                )
            claim = _capture_cloud_generation_claim(_session.workspace_sync)
            global _pending_cloud_push_claim, _pending_cloud_push_sync
            global _pending_cloud_push_staged
            staged_event = asyncio.Event()
            _pending_cloud_push_claim = claim
            _pending_cloud_push_sync = _session.workspace_sync
            _pending_cloud_push_staged = staged_event
            _pending_cloud_push_task = asyncio.create_task(
                _run_turn_end_cloud_push(
                    _session.workspace_sync,
                    turn_id,
                    requirements=requirements,
                    claim=claim,
                    staged_event=staged_event,
                ),
                name=f"cloud-generation-push-turn-{turn_id}",
            )
        else:
            _pending_cloud_push_task = asyncio.create_task(
                _run_turn_end_cloud_push(_session.workspace_sync, turn_id),
                name=f"cloud-push-turn-{turn_id}",
            )

    # Slice C (design §5): ping the orchestrator to stage the protected
    # session's upperdir diff to S3. Fire-and-forget — never blocks the next
    # turn on a slow SSH+tar+upload round-trip.
    if _should_notify_cloud_stage():
        if _stateless_mode():
            # Protected-cloud stages are deliberately pinned for S2. A legacy
            # malformed stateless row must fail closed rather than leave an
            # unfenced SSH/tar task running after claimant handoff.
            raise LeaseLostError("protected-cloud staging requires pinned execution")
        stage_thread_id = str(_thread_id or "")
        stage_agent_id = str(getattr(_orchestrator_client, "agent_id", None) or "")
        stage_runtime_generation = _session_runtime_generation
        stage_attach_token = _session_runtime_attach_token
        _track_session_side_task(
            asyncio.create_task(
                _notify_cloud_stage(
                    stage_thread_id,
                    agent_id=stage_agent_id,
                    session_runtime_generation=stage_runtime_generation,
                    session_runtime_attach_token=stage_attach_token,
                ),
                name=f"cloud-stage-{stage_thread_id[:12]}",
            )
        )


def _loop_archive_llm_call(prepared: Any, response: Any, metrics: dict) -> None:
    """Audit one main-LLM call to the llm_requests trail, in the background.

    Sessions previously wrote no llm_requests at all — job agents were
    auditable, session hangs were not (session_silent_failure_audit.md #14).
    The Mongo insert is synchronous, so it runs in a thread; failures are
    non-fatal by audit-trail contract.
    """
    if _session is None or _thread_id is None:
        return
    thread_id = _thread_id
    turn = _session.turn_count
    model = metrics.get("model") or getattr(
        getattr(_session.config, "llm", None), "model", "unknown"
    )

    def _do() -> None:
        try:
            from agent.core.archiver import archive_llm_request

            archive_llm_request(
                job_id=thread_id,
                agent_type="persistent",
                messages=prepared,
                response=response,
                model=model,
                latency_ms=metrics.get("latency_ms"),
                iteration=turn,
                call_type="main",
                metadata={
                    "thread_id": thread_id,
                    "turn": turn,
                    "input_tokens": metrics.get("input_tokens"),
                    "output_tokens": metrics.get("output_tokens"),
                    "cached_tokens": metrics.get("cached_tokens"),
                },
            )
        except Exception as e:
            logger.debug(f"llm_requests archive failed (non-fatal): {e}")

    asyncio.create_task(asyncio.to_thread(_do), name="archive-llm-call")


async def _loop_persist_message(msg: Any) -> bool:
    """Persist a single message the instant the loop produces it (incremental
    durability — closes Symptom 1).

    Bounded + non-fatal: a slow or failed write must never stall or crash the
    turn, so it's wrapped in the same ``wait_for(timeout=5)`` + swallow pattern
    as the turn-boundary saves. The turn-complete reconciliation
    (``_save_turn_ai_messages``) re-saves the same rows idempotently, so a write
    dropped here is recovered there for any turn that finishes; only a hard
    mid-turn crash relies on what landed incrementally. Uses
    ``_session.turn_count`` for the turn number — the loop callback carries no
    turn id (same convention as ``_record_compaction``).
    """
    if _session is None or _session.postgres_conn is None or _thread_id is None:
        return False
    try:
        await asyncio.wait_for(
            _persist_one_message(
                _session.postgres_conn,
                _thread_id,
                msg,
                _session.turn_count,
                tool_decisions=dict(_session.tool_decisions),
            ),
            timeout=5.0,
        )
        return True
    except asyncio.TimeoutError:
        logger.warning("Incremental message save timed out (5s) — proceeding")
    except Exception as e:
        logger.warning(f"Incremental message save failed (non-fatal): {e}")
    return False


async def _loop_on_error(message: str, turn_id: Optional[int] = None) -> None:
    """Surface a turn-killing error: broadcast live AND persist it.

    Previously this was a single transient ``error`` frame — the cockpit
    flashed a banner that the immediately-following ``ready`` frame state
    obscured, the still-open turn kept spinning, and a reload showed nothing
    (session_silent_failure_audit.md #2). Now:
      - ``error`` frame: legacy transient banner (kept for old clients)
      - ``turn.error`` frame: cockpit closes the open turn + renders a
        durable error bubble
      - ``role='error'`` row: the bubble survives reload. Excluded from the
        agent's own history restore (postgres_db.get_thread_messages) so it
        never enters the LLM context.
    """
    global _turn_event_open
    _turn_event_open = False
    terminal_turn_id = (
        int(turn_id)
        if turn_id is not None
        else (int(_session.turn_count) if _session is not None else None)
    )
    if terminal_turn_id is None:
        _clear_loop_interrupt()
    else:
        _clear_loop_interrupt(target_turn_id=terminal_turn_id)
    payload: Dict[str, Any] = {"message": message}
    if turn_id is not None:
        payload["turn_id"] = turn_id
    _broadcast("error", payload)
    _broadcast("turn.error", payload)
    if (
        _session is not None
        and _session.postgres_conn is not None
        and _thread_id is not None
    ):
        try:
            await asyncio.wait_for(
                _session.postgres_conn.save_thread_message(
                    thread_id=_thread_id,
                    role="error",
                    content=message,
                    turn_number=(
                        turn_id if turn_id is not None else _session.turn_count
                    ),
                ),
                timeout=5.0,
            )
        except Exception as e:
            logger.warning(f"Turn-error persist failed (non-fatal): {e}")


async def _loop_on_workspace_upgrade_needed(freeze_data: Dict[str, Any]) -> None:
    """Notify subscribers that a workspace upgrade is available.

    Fires for two distinct freeze types and emits the matching per-tier offer
    event so the existing cockpit handlers + nats_bridge map keep working
    unchanged (workspace_tier_upgrade.md §4.2 S5):

    - ``workspace_upgrade_required`` — a lite agent called
      ``request_workspace_upgrade`` → emit ``workspace_upgrade.needed`` (the
      sandbox offer; accept sends ``upgrade-to-workspace``).
    - ``vm_upgrade_required`` — a sandbox sudo intercept → emit
      ``vm_upgrade.needed`` (the existing VM offer; accept sends
      ``upgrade-to-vm``). Unchanged behavior.
    """
    if freeze_data.get("freeze_type") == "workspace_upgrade_required":
        _broadcast(
            "workspace_upgrade.needed",
            {
                "target_tier": freeze_data.get("target_tier", "sandbox"),
                "reason": freeze_data.get("reason", "A real workspace is needed"),
            },
        )
        return
    _broadcast(
        "vm_upgrade.needed",
        {
            "reason": freeze_data.get("reason", "sudo detected"),
            "command": freeze_data.get("command"),
        },
    )


async def _record_compaction(
    summary_text: Optional[str],
    before: int,
    after: int,
    trigger: str,
    ws: Optional[WebSocket] = None,
) -> None:
    """Notify the client of a context compaction and persist a restorable
    checkpoint row.

    The ``role='summary'`` row drives two things: (1) the "Context summarized"
    banner survives reload, and (2) it's a restore checkpoint — resume reads
    ``metrics.boundary_turn`` and loads ``[summary] + history(since_turn=B)``
    instead of the full pre-compaction history (see
    ``_restore_session_messages``). The agent's history query excludes
    ``role='summary'`` (``src/agent/database/postgres_db.py``) so the row never
    re-enters the LLM context.

    Boundary semantics: ``boundary_turn = turn_count - 1`` is the last
    *fully-saved* turn. At auto-compaction (mid-turn) the current turn's user
    message is saved but its AI/tool messages save only at turn-complete; on
    resume reloading ``turn_number > boundary_turn`` recaptures the whole
    current turn once it's persisted — lossless, with at most minor in-turn
    overlap that ``_repair_tool_pairing`` and re-bounding clean. The same rule
    is safe for manual ``/compact`` and resume-time compaction (where all turns
    are already fully saved).

    A non-None ``ws`` sends the live event over the control socket (manual
    ``/compact``); the auto and resume paths pass ``ws=None`` to use the
    broadcast/SSE channel.
    """
    turn = _session.turn_count if _session else None
    # Type-guard: defensive against an unexpected turn_count type so an
    # arithmetic glitch never kills event emission. In production turn_count
    # is always int (initialized to 0 in PersistentSession); the guard mainly
    # protects test mocks where a bare MagicMock attribute slips through.
    turn_int = turn if isinstance(turn, int) else 0
    boundary_turn = max(turn_int - 1, 0)
    params = {
        "before": before,
        "after": after,
        "trigger": trigger,
        "summary": summary_text,
        "turn": turn,
    }
    # Completion stats from the summarization engine (n_passes, duration_ms,
    # before/after tokens) — extends context.compacted per
    # knowledge-base/knowledge/features/context_summarization_rework.md.
    ctx_mgr_stats = getattr(
        getattr(_session, "context_manager", None),
        "_last_summarization_stats",
        None,
    )
    if isinstance(ctx_mgr_stats, dict):
        params.update(ctx_mgr_stats)
    try:
        if ws is not None:
            await _ws_send(ws, "context.compacted", params)
        else:
            _broadcast("context.compacted", params)
    except Exception as e:
        logger.debug(f"Failed to emit context.compacted (non-fatal): {e}")

    if summary_text and _session and _session.postgres_conn and _thread_id:
        # Resolve the message-granular boundary the summarizer just set (the id
        # of the last message its summary covers) into a seq, so resume loads
        # `summary + (seq > boundary_seq)` — the exact live tail — instead of the
        # whole post-boundary turn(s). None ⇒ resume falls back to boundary_turn
        # (e.g. resume-time compaction, whose restored messages carry fresh ids
        # that don't resolve to a persisted row).
        boundary_seq = None
        ctx_mgr = getattr(_session, "context_manager", None)
        boundary_id = getattr(ctx_mgr, "_last_compaction_boundary_id", None)
        if boundary_id:
            try:
                boundary_seq = await _session.postgres_conn.get_seq_for_message_id(
                    _thread_id, boundary_id
                )
            except Exception as e:
                logger.debug(f"boundary_seq lookup failed (non-fatal): {e}")
        try:
            await _session.postgres_conn.save_thread_message(
                thread_id=_thread_id,
                role="summary",
                content=summary_text,
                turn_number=turn,
                metrics={
                    "before": before,
                    "after": after,
                    "trigger": trigger,
                    "boundary_turn": boundary_turn,
                    "boundary_seq": boundary_seq,
                },
            )
        except Exception as e:
            logger.warning(f"Failed to persist compaction marker (non-fatal): {e}")


async def _loop_on_context_compacted(
    summary_text: str, before: int, after: int
) -> None:
    """Auto-summarization fired inside the loop — persist + broadcast a banner."""
    await _record_compaction(summary_text, before, after, trigger="auto", ws=None)


async def _loop_compaction_progress(event: str, params: Dict[str, Any]) -> None:
    """ContextManager compaction progress → journaled broadcast frames.

    Carries ``compaction.started`` / ``compaction.progress`` /
    ``compaction.failed`` (the completion event stays ``context.compacted``,
    emitted by ``_record_compaction``). ``_broadcast`` stamps ``(epoch, seq)``
    and journals into ``thread_events``, so a reload mid-compaction
    reconstructs the progress UI from SSE replay.
    See knowledge-base/knowledge/features/context_summarization_rework.md (S3).
    """
    _broadcast(event, params)


async def _loop_completion_handler(loop_task: asyncio.Task) -> None:
    """Wait for the persistent loop to finish, then run reason-appropriate cleanup.

    Under headless semantics the WS handler no longer cleans up after the loop
    in its finally block — the loop outlives the WS. So we attach this
    completion handler when the loop is spawned, and it routes the exit path:

    - IdleTimeoutError → archive + terminate as "idle_timeout"
    - Other exceptions → terminate as "loop_crash"
    - Clean exit → terminate as "loop_complete"
    - CancelledError → already inside _terminate_session, do nothing
    """
    try:
        await loop_task
    except IdleTimeoutError:
        logger.info("Persistent loop exited via idle timeout")
        if _stateless_mode():
            # Stateless lane: the pod-side idle timer must never end the
            # THREAD — thread lifecycle is orchestrator-owned, and an
            # 'ended' status (or the session.ended frame the archive
            # broadcasts) would force an epoch bump on the next claim's
            # attach (client cache-wipe cascade). Just drop the cached
            # session; the next claim rebuilds from thread_messages.
            await _terminate_session("idle_timeout", mark_thread=False)
            return
        try:
            await _handle_idle_archive()
        except Exception as e:
            logger.warning(f"Idle archive failed: {e}")
        await _terminate_session("idle_timeout")
    except asyncio.CancelledError:
        # Cancellation came from _terminate_session itself — don't re-enter.
        # Re-raise so the wrapper task surfaces as cancelled.
        raise
    except Exception as e:
        logger.warning(f"Persistent loop crashed: {e}", exc_info=True)
        # Every opened turn gets a terminal edge, on this path too: without a
        # turn.error the journal keeps turn.started open, every attached
        # client spins on a turn that ended, and every reload that replays
        # the journal reopens it. Persisted as a role='error' row so the
        # line survives reload. Not on a lost lease — that turn belongs to
        # a successor claim now, which closes it itself.
        if not isinstance(e, LeaseLostError):
            try:
                await _loop_on_error(
                    "The session loop stopped before this turn could be "
                    f"settled: {e}. The transcript is preserved — send a "
                    "message to continue."
                )
            except Exception:
                logger.debug(
                    "turn.error on loop crash failed (non-fatal)", exc_info=True
                )
        await _terminate_session("loop_crash", mark_thread=not _stateless_mode())
    else:
        logger.info("Persistent loop completed cleanly")
        await _terminate_session("loop_complete", mark_thread=not _stateless_mode())


def _safe_serialize(obj: Any) -> Any:
    """Make an object JSON-serializable (best effort)."""
    try:
        json.dumps(obj)
        return obj
    except (TypeError, ValueError):
        return str(obj)


# Bidirectional tool-call pairing repair now lives in core.context so the live
# persistent turn loop (persistent_graph) and this resume path share one
# implementation. Alias preserves the private name used by the call sites below
# and by tests/test_persistent_app.py.
_repair_tool_pairing = repair_tool_pairing


def _sanitize_restored_history(restored: list) -> list:
    """Slice D restore rung: normalize a restored history for the bound model.

    The durable config's model may have changed while the session was
    detached (the owner-facing offline PATCH), so rows persisted under one
    provider can be replayed under another. Reasoning shapes are already
    flattened at persist time (``_serialize_message_row``); what survives
    restore verbatim is tool-call ids — remap any that don't conform to the
    bound model's accepted format. No-op (beyond the pairing re-check) when
    the history is native to the bound model.
    """
    from agent.core.context import sanitize_history_for_provider_boundary

    model = ""
    if _session is not None:
        model = getattr(getattr(_session, "config", None), "llm", None)
        model = getattr(model, "model", "") or ""
    return sanitize_history_for_provider_boundary(restored, model)


def _db_rows_to_lc_messages(db_messages: list) -> list:
    """Convert ``thread_messages`` rows to LangChain messages with stable ids.

    Shared by the restore paths (Path A checkpoint+tail, Path B full load).
    Falls back to positional pairing of tool results for legacy rows whose
    ``tool_call_id`` column is NULL (predates the column); current rows carry
    it explicitly. Skips system rows — the loop adds a fresh system from the
    current config. ``role='summary'`` rows are already excluded by the DB
    query.
    """
    import uuid as _uuid
    from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

    restored: list = []
    pending_tool_call_ids: list[str] = []

    for db_msg in db_messages:
        role = db_msg["role"]
        content = db_msg["content"] or ""
        tool_calls = db_msg.get("tool_calls")

        # Preserve the durable row id when available. It still gives
        # RemoveMessage a real state key, and lets a reclaimed input append the
        # exact row once rather than duplicating a restored conversation item.
        # Legacy/unit rows without an id retain the fresh-id fallback.
        msg_id = str(db_msg.get("id") or _uuid.uuid4())

        if role in ("human", "user"):
            restored.append(HumanMessage(content=content, id=msg_id))

        elif role == "event":
            # System-injected notice (a worker job this session created reached
            # a terminal state). Restored as a HumanMessage because that is what
            # it was in memory when the model first saw it — the 'event' role is
            # a TRANSCRIPT distinction, not a model-context one.
            #
            # This branch is not optional. The if/elif chain has no else, so an
            # unhandled role is dropped SILENTLY: the notice would keep existing
            # in the DB and in the UI while vanishing from the model's context
            # on the next pod recycle, and the session would answer a question
            # it can no longer see. (postgres_db's history query excludes only
            # 'summary' and 'error', so 'event' rows do reach here.)
            restored.append(HumanMessage(content=content, id=msg_id))

        elif role in ("ai", "assistant"):
            lc_tool_calls = []
            if tool_calls:
                lc_tool_calls = [
                    {
                        "id": tc.get("id", ""),
                        "name": tc.get("name", ""),
                        "args": tc.get("args", {}),
                    }
                    for tc in tool_calls
                ]
            pending_tool_call_ids = [tc["id"] for tc in lc_tool_calls]
            restored.append(
                AIMessage(content=content, tool_calls=lc_tool_calls, id=msg_id)
            )

        elif role == "tool":
            # Prefer the persisted tool_call_id; fall back to positional
            # pairing for legacy rows that predate the column. Pop either
            # way so the fallback queue stays aligned.
            fallback_id = pending_tool_call_ids.pop(0) if pending_tool_call_ids else ""
            tool_call_id = db_msg.get("tool_call_id") or fallback_id
            restored.append(
                ToolMessage(
                    content=content,
                    tool_call_id=tool_call_id,
                    id=msg_id,
                )
            )

        # Skip system rows — the loop adds a fresh one from current config

    return restored


async def _restore_session_messages() -> None:
    """Restore LangChain message history from DB into session.messages.

    Called during lifespan startup. On a fresh session this is a no-op.
    On pod restart or session resume, this restores the LLM's conversation
    context so it doesn't start with amnesia.

    Two paths:

    * **Path A — checkpoint resume:** if a ``role='summary'`` row exists with
      a ``metrics.boundary_turn`` set, restore
      ``[SystemMessage(summary)] + history(since_turn=boundary_turn)``. The
      summary covers turns ≤ boundary_turn; the tail covers everything after.
      This avoids re-loading the full pre-checkpoint history and
      re-summarizing on every resume — the fix for the OOM observed on a
      793-message / 395k-token thread. If the tail itself has outgrown the
      budget, the re-bound summarizes again and the merged result is persisted
      as a fresh checkpoint (counter-gated, same as Path B) so the cost is
      paid once, not on every subsequent resume.

    * **Path B — full load (back-compat):** no checkpoint, or
      ``boundary_turn`` missing (rows that predate this feature). Load the
      full log and let ``ensure_within_limits`` bound it the same way a live
      turn would. If that re-summarization actually compacts, persist a
      fresh checkpoint via ``_record_compaction(trigger="resume")`` so
      subsequent resumes hit Path A and the "Context summarized" banner
      appears.

    The raw conversation always stays in ``thread_messages`` for the user to
    view (cockpit reads it via a separate orchestrator-side query); only the
    in-memory LLM context is bounded.
    """
    if not _session or not _agent or not _agent.postgres_conn or not _thread_id:
        return

    try:
        import uuid as _uuid
        from langchain_core.messages import SystemMessage

        from agent.llm.response_guards import strip_removal_markers

        ctx_mgr = getattr(_session, "context_manager", None)
        aux = getattr(_session, "auxiliary_llm", None)
        max_summary_length = getattr(
            _session.config.context_management, "max_summary_length", 10000
        )

        ckpt = await _agent.postgres_conn.get_latest_compaction_checkpoint(_thread_id)
        boundary_turn = ckpt.get("boundary_turn") if ckpt else None
        boundary_seq = ckpt.get("boundary_seq") if ckpt else None

        # ============================================================
        # Path A — checkpoint resume
        # ============================================================
        if ckpt is not None and (boundary_seq is not None or boundary_turn is not None):
            if boundary_seq is not None:
                # Message-granular cursor: load exactly the tail the summary does
                # not cover (seq > boundary_seq), independent of turn size — the
                # fix for the 793-message-turn OOM. Capped by the resume floor.
                db_messages = await _agent.postgres_conn.get_thread_messages_history(
                    thread_id=_thread_id,
                    limit=_resume_message_limit,
                    seq_gt=boundary_seq,
                    newest_first=True,
                )
            else:
                # Back-compat: summary rows written before boundary_seq shipped
                # carry only boundary_turn — fall back to the turn cursor.
                db_messages = await _agent.postgres_conn.get_thread_messages_history(
                    thread_id=_thread_id,
                    limit=_resume_message_limit,
                    since_turn=boundary_turn,
                    newest_first=True,
                )
            if len(db_messages) >= _resume_message_limit:
                logger.warning(
                    f"Resume floor hit on thread {_thread_id}: post-boundary tail "
                    f"trimmed to newest {_resume_message_limit} messages (stale "
                    f"boundary or a runaway tail) — bounded, but old context may drop"
                )

            summary_msg = SystemMessage(
                content=f"[Summary of prior work]\n{ckpt['summary']}",
                id=str(_uuid.uuid4()),
            )
            restored: list = [summary_msg, *_db_rows_to_lc_messages(db_messages)]

            # Defense-in-depth: drop any tool call/result orphaned by an
            # interrupted persist (e.g. the prior pod died mid-turn before
            # the tool result was saved). find_safe_slice_start already
            # guaranteed the live cut never orphaned a pair.
            restored = _repair_tool_pairing(restored)
            restored = _sanitize_restored_history(restored)

            pre_compact_len = len(restored)
            runs_before = getattr(ctx_mgr, "compaction_runs", 0)
            if ctx_mgr and aux and restored:
                try:
                    bounded = await ctx_mgr.ensure_within_limits(
                        restored,
                        aux,
                        max_summary_length=max_summary_length,
                        trigger="resume",
                    )
                    restored = strip_removal_markers(bounded)
                except Exception as e:
                    logger.warning(
                        "Re-bound during checkpoint restore failed "
                        f"(non-fatal, keeping uncompacted): {e}"
                    )
            runs_after = getattr(ctx_mgr, "compaction_runs", 0)
            compacted_on_restore = (
                isinstance(runs_before, int)
                and isinstance(runs_after, int)
                and runs_after > runs_before
            )

            if restored:
                _session.messages.extend(restored)
                _bturn = boundary_turn or 0
                tail_turn_max = max(
                    (m.get("turn_number") or 0 for m in db_messages),
                    default=_bturn,
                )
                _session.turn_count = max(tail_turn_max, _bturn)
                cursor = (
                    f"seq>{boundary_seq}"
                    if boundary_seq is not None
                    else f"turn>{boundary_turn}"
                )
                logger.info(
                    f"Restored from checkpoint ({cursor}) for "
                    f"thread {_thread_id} ({len(restored)} msgs in context; "
                    f"tail of {len(db_messages)} raw rows; "
                    f"turn_count={_session.turn_count})"
                )

                # If the post-checkpoint tail outgrew the budget, the re-bound
                # above ran a REAL summarization — persist it so the checkpoint
                # advances and the next resume loads the merged summary plus a
                # short tail. Without this, every subsequent resume re-runs the
                # same blocking aux-LLM summarization and discards the result
                # (per-claim cost on the stateless lane, where every turn is a
                # resume). Counter-gated exactly like Path B below: when nothing
                # compacted, the existing row stands and no duplicate banner row
                # is written.
                if compacted_on_restore:
                    summary_text = extract_summary_text(restored)
                    if summary_text:
                        try:
                            await _record_compaction(
                                summary_text,
                                pre_compact_len,
                                len(restored),
                                trigger="resume",
                                ws=None,
                            )
                        except Exception as e:
                            logger.debug(
                                f"Path-A resume checkpoint persist failed "
                                f"(non-fatal): {e}"
                            )
            return

        # ============================================================
        # Path B — full load (back-compat)
        # ============================================================
        # No checkpoint to bound the load, so the resume floor is the only guard:
        # take the newest N messages instead of the entire append-only log (the
        # exit-137 OOM). ensure_within_limits then summarizes them and Path B
        # writes a checkpoint so the next resume hits Path A.
        db_messages = await _agent.postgres_conn.get_thread_messages_history(
            thread_id=_thread_id,
            limit=_resume_message_limit,
            newest_first=True,
        )
        if len(db_messages) >= _resume_message_limit:
            logger.warning(
                f"Resume floor hit on thread {_thread_id} (no checkpoint): loaded "
                f"newest {_resume_message_limit} of a larger log — bounded to avoid OOM"
            )

        if not db_messages:
            return

        restored = _db_rows_to_lc_messages(db_messages)

        # Defense-in-depth: drop any call/result orphaned by an interrupted
        # persist (full load already eliminates truncation orphans).
        restored = _repair_tool_pairing(restored)
        restored = _sanitize_restored_history(restored)

        # Bound the working context the way a live turn does. Summarizes to
        # (summary + recent) when over the token budget, else passes through.
        # ensure_within_limits returns a LangGraph reducer delta; this loop
        # has no reducer, so strip the RemoveMessage markers to materialize it.
        pre_compact_len = len(restored)
        runs_before = getattr(ctx_mgr, "compaction_runs", 0)
        if ctx_mgr and aux and restored:
            try:
                bounded = await ctx_mgr.ensure_within_limits(
                    restored,
                    aux,
                    max_summary_length=max_summary_length,
                    trigger="resume",
                )
                restored = strip_removal_markers(bounded)
            except Exception as e:
                logger.warning(
                    "Compaction during restore failed "
                    f"(non-fatal, keeping full history): {e}"
                )
        runs_after = getattr(ctx_mgr, "compaction_runs", 0)
        compacted_on_resume = (
            isinstance(runs_before, int)
            and isinstance(runs_after, int)
            and runs_after > runs_before
        )

        if restored:
            _session.messages.extend(restored)
            # Set turn_count from the last stored message's turn_number
            last_turn = max((m.get("turn_number") or 0 for m in db_messages), default=0)
            _session.turn_count = last_turn
            logger.info(
                f"Restored {len(restored)} messages for thread {_thread_id} "
                f"(from {len(db_messages)} stored; last turn: {last_turn})"
            )

            # If a real compaction happened during resume, persist a
            # checkpoint (writes a role='summary' row with boundary_turn)
            # so subsequent resumes hit Path A and the banner appears.
            # Path A applies the same counter-gated persist when ITS re-bound
            # actually compacts (an outgrown tail); when nothing compacted,
            # neither path writes, so the existing row keeps driving the
            # banner without a duplicate. Gated on the manager's run counter,
            # not a length delta (the heuristic false-fires on stray
            # RemoveMessage markers).
            if compacted_on_resume:
                summary_text = extract_summary_text(restored)
                if summary_text:
                    try:
                        await _record_compaction(
                            summary_text,
                            pre_compact_len,
                            len(restored),
                            trigger="resume",
                            ws=None,
                        )
                    except Exception as e:
                        logger.debug(
                            f"Resume compaction checkpoint failed (non-fatal): {e}"
                        )

    except Exception as e:
        logger.warning(f"Failed to restore session messages (non-fatal): {e}")


async def _save_message(
    client: Any,
    thread_id: str,
    role: str,
    content: Optional[str],
    tool_calls: Optional[Any],
    turn_number: int,
    tool_call_id: Optional[str] = None,
    thinking: Optional[str] = None,
    id: Optional[str] = None,
) -> None:
    """Fire-and-forget: save a single message by direct DB write.

    ``client`` is the agent's ``PostgresDB`` (``_session.postgres_conn``); the
    write no longer hops through the orchestrator REST endpoint. ``id`` is the
    message's stable id when one exists (so a re-save upserts onto the same row);
    ``None`` lets the DB layer mint a fresh UUID for single-shot rows like the
    user message.
    """
    try:
        await client.save_thread_message(
            thread_id=thread_id,
            role=role,
            content=content,
            tool_calls=tool_calls,
            turn_number=turn_number,
            tool_call_id=tool_call_id,
            thinking=thinking,
            id=id,
        )
    except Exception as e:
        logger.warning(f"Failed to save message (non-fatal): {e}")


def _select_turn_messages(
    messages: List[Any],
    turn_number: int,
    *,
    authoritative_turn_boundary: bool,
    turn_input_message_id: Optional[str],
) -> List[Any]:
    """The messages the turn-complete reconcile re-upserts for ``turn_number``.

    Membership, not position: the loop stamps every message it appends
    during a turn (``message_markers.stamp_turn_membership``), so the turn's
    rows are selected by stamp regardless of what a mid-turn compaction did
    to the working set. The transcript in ``thread_messages`` is the source
    of truth for the turn boundary — ``save_thread_messages`` looks the
    accepted input row up there — so the exact input message need not be
    resident in RAM, and it is never rewritten here (persisted at accept
    time). See
    knowledge-base/knowledge/issues/stateless_turn_settlement_crashes_after_midturn_compaction.md.

    Unstamped history keeps the historical walk from the tail: back to the
    exact input id (stateless) or the latest HumanMessage (pinned). A
    stateless walk that finds no anchor reconciles zero rows — the
    incremental writes already hold the turn's content and the DB still
    mints the boundary — rather than failing the settlement.
    """
    input_id = str(turn_input_message_id) if turn_input_message_id else None
    if authoritative_turn_boundary and any(
        turn_membership(msg) is not None for msg in messages
    ):
        wanted = int(turn_number)
        return [
            msg
            for msg in messages
            if turn_membership(msg) == wanted
            and str(getattr(msg, "id", "")) != input_id
        ]
    to_save: List[Any] = []
    boundary_found = False
    for msg in reversed(messages):
        if authoritative_turn_boundary:
            if str(getattr(msg, "id", "")) == input_id:
                boundary_found = True
                break
        elif hasattr(msg, "type") and msg.type in (
            "human",
            "HumanMessageChunk",
        ):
            boundary_found = True
            break
        to_save.append(msg)
    to_save.reverse()
    if authoritative_turn_boundary and not boundary_found:
        logger.warning(
            "turn %s reconcile: input message %s is not resident and the "
            "history carries no membership stamps; reconciling zero rows "
            "(incremental rows stand, the DB mints the boundary)",
            turn_number,
            input_id,
        )
        return []
    return to_save


async def _save_turn_ai_messages(
    client: Any,
    thread_id: str,
    messages: List[Any],
    turn_number: int,
    metrics: dict | None = None,
    tool_decisions: Optional[Dict[str, str]] = None,
    *,
    authoritative_turn_boundary: bool = False,
    turn_input_message_id: Optional[str] = None,
    memory_scope_kind: Optional[str] = None,
    memory_scope_id: Optional[str] = None,
) -> None:
    """Reconcile the most recent turn by direct DB write.

    ``client`` is the agent's ``PostgresDB`` (``_session.postgres_conn``) — the
    write goes straight to the pool, not through the orchestrator REST endpoint.
    Each message carries a stable ``id`` (minted at creation), passed through so
    this save upserts onto the same row a future incremental/reconciliation pass
    would touch (``ON CONFLICT (id)``) rather than duplicating.

    ``tool_decisions`` carries the per-call supervised approval outcome
    (``tool_call_id -> 'approved' | 'denied'``) so the decision survives
    history reload as a field on the persisted tool_calls.

    Stateless callers set ``authoritative_turn_boundary``. The exact input
    message id and turn number then ride the same fenced transaction as the
    final rows so the DB can mint the per-turn outbox identity. Even a turn
    with zero output must call the DB. Any missing boundary or write failure is
    fatal; pinned callers keep the historical empty/no-error behavior.
    """
    try:
        if authoritative_turn_boundary and not turn_input_message_id:
            raise ValueError(
                "authoritative stateless turn lacks an exact input message id"
            )
        to_save = _select_turn_messages(
            messages,
            turn_number,
            authoritative_turn_boundary=authoritative_turn_boundary,
            turn_input_message_id=turn_input_message_id,
        )
        if authoritative_turn_boundary and (
            memory_scope_kind not in {"thread", "project"} or not memory_scope_id
        ):
            raise ValueError(
                "authoritative stateless turn lacks an immutable memory destination"
            )
        if not to_save and not authoritative_turn_boundary:
            return

        # Reconcile the whole turn in ONE batched upsert (was a serial
        # save_thread_message per message — ~2 round-trips each). Re-upserts
        # ALL turn rows via the shared serializer, now with turn-level metrics +
        # approval decisions: incremental writes already landed the content
        # mid-turn, this updates the same rows (stable id, ON CONFLICT). Saving
        # every row (not just AI) keeps the reconcile the durability backstop for
        # any incremental write that was dropped mid-turn (_loop_persist_message
        # is best-effort). A compaction view (a capped/elided/shed copy of a
        # row) is written insert-if-absent: it backstops a lost write — a tool
        # call must never be left without its result — but never overwrites
        # the durable full row with the lossy copy.
        rows = []
        for msg in to_save:
            row = _serialize_message_row(
                msg, turn_number, metrics=metrics, tool_decisions=tool_decisions
            )
            if is_compaction_view(msg):
                row["insert_if_absent"] = True
            rows.append(row)
        if authoritative_turn_boundary:
            producer_id = await client.save_thread_messages(
                thread_id,
                rows,
                turn_input_message_id=turn_input_message_id,
                turn_number=turn_number,
                memory_scope_kind=memory_scope_kind,
                memory_scope_id=memory_scope_id,
            )
            if not producer_id:
                raise RuntimeError(
                    "authoritative stateless turn persist minted no memory effect"
                )
        else:
            await client.save_thread_messages(thread_id, rows)
    except Exception as e:
        if authoritative_turn_boundary:
            raise
        logger.warning(f"Failed to save turn messages (non-fatal): {e}")


async def _handle_rewind(ws: WebSocket, data: Dict[str, Any]) -> None:
    """Rewind the session to just before an earlier user message.

    knowledge-base/knowledge/features/session_rewind.md §Flow — attached. Order is load-bearing:
    resolve+validate target (a pure validation error must not disturb an
    in-flight turn or queued input) → interrupt+wait → drain queue → git
    forward-restore (fallible, gates everything) → DB sweep+ledger →
    in-memory truncate/rehydrate → narrow resweep (mops up any straggler
    written during the interrupt wait) → events-epoch bump → acks. Bash side
    effects and non-git sessions degrade exactly like Claude Code:
    conversation-only. From the sweep onward a broad exception handler
    guarantees the initiator always gets a terminal frame — the DB write may
    already be committed by the time anything fails.
    """
    global _events_epoch, _next_seq, _event_writer

    request_id = data.get("request_id")

    async def _err(message: str) -> None:
        await _ws_send(ws, "error", {"message": message, "request_id": request_id})

    if _session is None or _session.postgres_conn is None or _thread_id is None:
        await _err("Session no longer active")
        return
    mode = data.get("mode", "conversation")
    if mode not in ("both", "conversation", "code"):
        await _err(f"Invalid rewind mode: {mode}")
        return
    message_id = data.get("message_id")
    if not message_id:
        await _err("rewind requires message_id")
        return

    if _rewind_lock.locked():
        await _err("A rewind is already in progress")
        return
    async with _rewind_lock:
        conn = _session.postgres_conn

        # 1. Resolve + validate the target FIRST. A pure validation error must
        #    not kill the in-flight turn or discard queued inputs — cheap to do
        #    ahead of the interrupt since the rewind lock excludes the only
        #    writer that could tombstone this row concurrently.
        row = await conn.get_live_message(_thread_id, message_id)
        if row is None:
            await _err("Message not found (it may already be rewound)")
            return
        if row["role"] != "human":
            await _err("Rewind targets must be user messages")
            return
        from_seq = row["seq"]
        prompt = row["content"] or ""

        # 2. Interrupt any in-flight turn (same policy as the interrupt verb:
        #    graceful while a tool is mid-invoke, hard otherwise) and wait for
        #    the loop to park. _turn_event_open is the turn-in-flight signal.
        if _turn_event_open:
            active_turn_id = int(_session.turn_count)
            if _signal_interrupt_for_turn(active_turn_id) is None:
                await _err("Could not interrupt the running turn — try again")
                return
            deadline = asyncio.get_event_loop().time() + 60.0
            while _turn_event_open:
                if asyncio.get_event_loop().time() > deadline:
                    await _err("Could not interrupt the running turn — try again")
                    return
                await asyncio.sleep(0.1)

        # The wait above can run for up to 60s — long enough for an
        # out-of-band teardown (drain, watchdog, REST detach) to tear the
        # session down underneath us. Re-validate before touching anything
        # that assumes it's still alive.
        if _session is None or _session.postgres_conn is None or _thread_id is None:
            await _err("Session no longer active")
            return

        # 3. Drain queued inputs: their rows sit past the sweep boundary and
        #    are about to be tombstoned; processing them post-rewind would
        #    resurrect the abandoned timeline.
        if _loop_user_queue is not None:
            while True:
                try:
                    _loop_user_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

        # 4. Workspace forward-restore — fallible, so it gates the sweep.
        abandoned_sha = None
        restored_to_sha = None
        restore_commit_sha = None
        if mode in ("both", "code"):
            ws_mgr = _session.workspace_manager
            git_mgr = getattr(ws_mgr, "git_manager", None) if ws_mgr else None
            if not (git_mgr and git_mgr.is_active):
                await _err(
                    "This session has no version history — file restore is "
                    "unavailable (conversation-only rewind still works)"
                )
                return
            restored_to_sha = await conn.resolve_restore_commit(_thread_id, from_seq)
            if not restored_to_sha:
                await _err(
                    "No workspace checkpoint exists before this message — "
                    "file restore is unavailable for this target"
                )
                return
            # Snapshot the abandoned state first: nothing is ever lost in git.
            if not git_mgr.commit("Rewind: pre-rewind snapshot"):
                await _err("Could not snapshot the current workspace state")
                return
            abandoned_sha = git_mgr.get_current_commit()
            if not git_mgr.restore_tree(restored_to_sha):
                await _err(
                    "Workspace restore failed — files are unchanged (a "
                    "snapshot commit was kept); conversation was not rewound"
                )
                return
            if not git_mgr.commit(
                f"Rewind: restore workspace to {restored_to_sha[:12]}"
            ):
                await _err("Workspace restore could not be committed")
                return
            restore_commit_sha = git_mgr.get_current_commit()
            if restore_commit_sha:
                # Best-effort mapping write (same non-fatal shape as
                # _loop_on_workspace_commit). record_turn_commit's INSERT ...
                # ON CONFLICT (thread_id, seq) DO UPDATE keys on the
                # *current* (pre-sweep) MAX(seq) — i.e. the abandoned tip's
                # own row — so this upsert overwrites that newest stale
                # mapping in place. Without it, a second rewind (or any
                # resolve_restore_commit lookup) landing between the
                # abandoned tip's seq and the next post-rewind turn-commit
                # would resolve to the abandoned tree instead of the one we
                # just restored to.
                try:
                    await conn.record_turn_commit(_thread_id, restore_commit_sha)
                except Exception:
                    logger.warning(
                        "record_turn_commit failed after rewind restore (non-fatal)",
                        exc_info=True,
                    )

        # From here on the DB sweep is one await away from being committed and
        # durable — an unexpected exception (e.g. the session detaching mid-op)
        # must not leave the initiator hanging with no terminal frame at all.
        try:
            # 5. Sweep + ledger (one transaction). mode='code' ledgers only.
            result = await conn.apply_rewind(
                _thread_id,
                from_seq=from_seq,
                mode=mode,
                actor="ws_client",
                abandoned_sha=abandoned_sha,
                restored_to_sha=restored_to_sha,
                restore_commit_sha=restore_commit_sha,
            )

            # 6. Fix in-memory state (transcript-changing modes only).
            # _coerce_row_id maps in-memory `msg_…` ids to the row UUIDs the
            # frontend sends; restored-prefix messages carry no id (the HF-7
            # resume diet drops the column) and correctly fall through to the
            # deep-rewind path.
            rehydrate_failed = False
            if mode in ("both", "conversation"):
                from agent.database.postgres_db import _coerce_row_id

                target_uuid = str(_coerce_row_id(message_id))
                cut_index = None
                for i, m in enumerate(_session.messages):
                    mid = getattr(m, "id", None)
                    if mid and str(_coerce_row_id(mid)) == target_uuid:
                        cut_index = i
                        break
                if cut_index is not None:
                    # Shallow rewind: fidelity-preserving in-place truncate.
                    del _session.messages[cut_index:]
                else:
                    # Deep rewind (target predates the live compaction
                    # boundary, or the prefix was restored without ids):
                    # rebuild from the now-filtered transcript. This read can
                    # silently come back short (pod-restart id loss, a
                    # transient DB blip _restore_session_messages swallows) —
                    # an empty result is only legitimate when nothing
                    # survives the rewind, so retry once and otherwise treat
                    # it as a failure rather than falsely acking amnesia.
                    _session.messages.clear()
                    await _restore_session_messages()
                    if not _session.messages and result["surviving_turn"] > 0:
                        await _restore_session_messages()
                        rehydrate_failed = (
                            not _session.messages and result["surviving_turn"] > 0
                        )
                _session.turn_count = result["surviving_turn"]
                _loop_last_user_content[0] = ""

                # 6b. Narrow resweep: step 2's interrupt-wait can run for up
                #     to 60s, long enough for the very turn being interrupted
                #     to still land a completion INSERT after step 5's sweep
                #     already ran. Idempotent (rewound_at IS NULL guard) — a
                #     normal run with no stragglers sweeps 0 rows. Not a
                #     second apply_rewind: that would append a duplicate
                #     thread_rewinds ledger row for the same rewind.
                stray_count = await conn.resweep_rewind(_thread_id, from_seq)
                if stray_count > 0:
                    logger.warning(
                        "Rewind resweep caught %d stray row(s) written during "
                        "the interrupt wait (thread=%s from_seq=%s)",
                        stray_count,
                        _thread_id,
                        from_seq,
                    )

                # 7. New event generation → every SSE viewer takes the
                #    existing gone_beyond_horizon repaint against the
                #    filtered history. Rewind is one of the two legitimate
                #    deliberate bumpers (reaper steal is the other — doc
                #    §5.3.2), so this must ALWAYS bump, never reuse-resolve.
                #    Order matters: drain+close the old writer first so any
                #    straggler frames flush under the epoch they are stamped
                #    with (the fenced flush would reject them post-bump and
                #    stop the writer); then bump (epoch+1, hwm=0); then start
                #    a fresh writer owning the new epoch. The subsequent
                #    rewind.done broadcast lands at (new_epoch, 1) and the
                #    fenced flush pushes events_seq_hwm to 1 — which IS the
                #    doc §5.3.2 item 5 allocator init above the rewind.done
                #    row: the next attach seeds from GREATEST(hwm, MAX(seq))
                #    and can never collide with it.
                try:
                    old_writer = _event_writer
                    if old_writer is not None:
                        _event_writer = None
                        await old_writer.close()
                    _events_epoch = await _bump_event_journal_epoch(conn, _thread_id)
                    _next_seq = 0
                    new_writer = _OrderedPersistentEventWriter(
                        postgres_conn=conn,
                        thread_id=_thread_id,
                        epoch=_events_epoch,
                        on_terminal_failure=_event_persistence_failed,
                        pinned_agent_id=_registered_pinned_agent_id(),
                        pinned_runtime_generation=_session_runtime_generation,
                        pinned_runtime_attach_token=_session_runtime_attach_token,
                    )
                    new_writer.start()
                    _event_writer = new_writer
                except Exception:
                    logger.warning(
                        "Rewind epoch bump failed — viewers repaint on next attach",
                        exc_info=True,
                    )
                    if _event_writer is None and _thread_id is not None:
                        # Keep journaling alive under the still-current epoch
                        # rather than leaving the rest of the session
                        # unjournaled because the bump failed.
                        try:
                            recovery_writer = _OrderedPersistentEventWriter(
                                postgres_conn=conn,
                                thread_id=_thread_id,
                                epoch=_events_epoch,
                                on_terminal_failure=_event_persistence_failed,
                                pinned_agent_id=_registered_pinned_agent_id(),
                                pinned_runtime_generation=(_session_runtime_generation),
                                pinned_runtime_attach_token=(
                                    _session_runtime_attach_token
                                ),
                            )
                            recovery_writer.start()
                            _event_writer = recovery_writer
                        except Exception:
                            logger.warning(
                                "Rewind writer recovery failed — journal "
                                "frames will be dropped until reattach",
                                exc_info=True,
                            )

                if rehydrate_failed:
                    # The DB sweep is already committed and durable — other
                    # viewers still need to repaint to the filtered
                    # transcript — but the initiator's own context came back
                    # empty air, so they get an error instead of a
                    # false-success ack.
                    _broadcast("rewind.done", {"message_id": message_id, "mode": mode})
                    logger.warning(
                        "Rewind rehydrate came back empty after sweep "
                        "(thread=%s from_seq=%s surviving_turn=%s)",
                        _thread_id,
                        from_seq,
                        result["surviving_turn"],
                    )
                    await _err(
                        "Rewind applied, but reloading the live context "
                        "failed — close and re-open the session to pick it up"
                    )
                    return

            # 8. Acks: direct to the initiator (no _seq), then the journaled
            #    all-viewer signal in the NEW epoch.
            await _ws_send(
                ws,
                "rewind.ack",
                {
                    "request_id": request_id,
                    "message_id": message_id,
                    "mode": mode,
                    "prompt": prompt,
                    "swept": result["swept"],
                    "restored_to_sha": restored_to_sha,
                },
            )
            if mode in ("both", "conversation"):
                _broadcast("rewind.done", {"message_id": message_id, "mode": mode})
            else:
                _broadcast(
                    "rewind.files_restored", {"restored_to_sha": restored_to_sha}
                )
        except Exception as e:
            logger.exception(
                "Rewind failed after the sweep gate: thread=%s from_seq=%s",
                _thread_id,
                from_seq,
            )
            await _err(f"Rewind failed: {e}")
            return
        logger.info(
            "Rewind applied: thread=%s mode=%s from_seq=%s swept=%s",
            _thread_id,
            mode,
            from_seq,
            result["swept"],
        )


async def _handle_compact(
    ws: WebSocket, focus: str = "", boundary_message_id: Optional[str] = None
) -> None:
    """Handle /compact command — trigger manual context compaction.

    Serialized against `rewind` via the shared `_rewind_lock`: compaction
    rewrites `_session.messages` in place and persists a boundary-keyed
    summary row, so a rewind sweeping the transcript underneath a
    still-running manual compaction (or the reverse) would race the same
    in-memory list and the same seq-keyed rows. Applies equally to a plain
    `/compact` (`boundary_message_id=None`) — any manual compaction racing a
    rewind hits the same hazard, not just the rewind sheet's "Summarize up
    to here".
    """
    if _runtime_admission_closed():
        await _ws_send(
            ws,
            "error",
            {"message": "Persistent runtime is terminating; retry on its replacement"},
        )
        return
    if _rewind_lock.locked():
        await _ws_send(
            ws,
            "error",
            {"message": "A rewind is in progress — try again when it finishes"},
        )
        return
    async with _rewind_lock:
        try:
            if not _session or not _session.context_manager:
                await _ws_send(ws, "error", {"message": "Session not ready"})
                return

            from agent.llm.response_guards import strip_removal_markers

            ctx_mgr = _session.context_manager

            # Manual compaction can run before the first loop start — make sure
            # progress frames flow either way (idempotent setter; getattr for
            # test doubles that stub the context manager).
            _cb_setter = getattr(ctx_mgr, "set_progress_callback", None)
            if callable(_cb_setter):
                _cb_setter(_loop_compaction_progress)

            # "Summarize up to here" (session rewind's sibling action): map the
            # chosen message to keep_recent_override = the number of messages from
            # it (inclusive) to the end, counted on the same basis
            # summarize_and_compact uses (workspace injections excluded — they are
            # filtered before keep_recent applies).
            keep_recent_override = None
            if boundary_message_id:
                from shared.runtime.core.workspace_injection import (
                    is_workspace_injection_message,
                )
                from agent.database.postgres_db import _coerce_row_id

                target_uuid = str(_coerce_row_id(boundary_message_id))
                cut_index = None
                for i, m in enumerate(_session.messages):
                    mid = getattr(m, "id", None)
                    if mid and str(_coerce_row_id(mid)) == target_uuid:
                        cut_index = i
                        break
                if cut_index is None:
                    await _ws_send(
                        ws,
                        "error",
                        {
                            "message": "That message is no longer in working "
                            "context — it may already be summarized"
                        },
                    )
                    return
                keep_recent_override = sum(
                    1
                    for m in _session.messages[cut_index:]
                    if not is_workspace_injection_message(m)
                )

            before_count = len(_session.messages)
            runs_before = getattr(ctx_mgr, "compaction_runs", 0)
            if _runtime_admission_closed():
                await _ws_send(
                    ws,
                    "error",
                    {
                        "message": "Persistent runtime is terminating; retry on "
                        "its replacement"
                    },
                )
                return
            result = await ctx_mgr.summarize_and_compact(
                messages=_session.messages,
                auxiliary=_session.auxiliary_llm,
                max_summary_length=getattr(
                    _session.config.context_management, "max_summary_length", 10000
                ),
                keep_recent_override=keep_recent_override,
                trigger="manual",
                focus=focus or None,
            )
            # summarize_and_compact returns a LangGraph reducer delta; this
            # transport has no reducer, so strip the RemoveMessage markers before
            # adopting. Leaking them into _session.messages made every later LLM
            # call false-detect a compaction and re-persist the same summary row
            # (the duplicate-banner bug, 2026-06-12).
            _session.messages[:] = strip_removal_markers(result)
            after_count = len(_session.messages)

            runs_after = getattr(ctx_mgr, "compaction_runs", 0)
            compacted_now = (
                isinstance(runs_before, int)
                and isinstance(runs_after, int)
                and runs_after > runs_before
            )
            if compacted_now:
                # A summary was actually produced: journal the completion
                # (broadcast, not ws-only — SSE replay must be able to clear the
                # progress UI after a reload) and persist the role='summary'
                # checkpoint row.
                summary_text = extract_summary_text(_session.messages)
                await _record_compaction(
                    summary_text, before_count, after_count, trigger="manual", ws=None
                )
            else:
                # No-op (below thresholds / nothing to fold): transient notice to
                # the requesting client only — no banner row, no journal entry.
                # summary=None tells the cockpit to render a system line instead
                # of a banner. Extracting + re-persisting the *previous* summary
                # here was another duplicate-banner source.
                turn = _session.turn_count if _session else None
                await _ws_send(
                    ws,
                    "context.compacted",
                    {
                        "before": before_count,
                        "after": after_count,
                        "trigger": "manual",
                        "summary": None,
                        "turn": turn if isinstance(turn, int) else None,
                    },
                )
            logger.info(
                f"Manual compaction: {before_count} → {after_count} messages "
                f"(summarized={compacted_now})"
            )

            # Commit + push workspace to Gitea on compaction (natural checkpoint boundary)
            if _session.workspace_manager:
                git_mgr = getattr(_session.workspace_manager, "git_manager", None)
                if git_mgr and git_mgr.is_active:
                    try:
                        if git_mgr.has_uncommitted_changes():
                            if git_mgr.commit(
                                f"Compaction checkpoint ({before_count} → {after_count} msgs)"
                            ):
                                sha = git_mgr.get_current_commit()
                                if sha:
                                    await _loop_on_workspace_commit(sha)
                        git_mgr.push()
                    except Exception as e:
                        logger.debug(f"Git push on compaction failed (non-fatal): {e}")
        except Exception as e:
            logger.warning(f"Compaction failed: {e}")
            await _ws_send(ws, "error", {"message": f"Compaction failed: {e}"})


def _scrub_secret_values(fragment: Any) -> Any:
    """Recursively drop secret-named keys from a config fragment.

    The ``config.changed`` ack echoes the applied fragment to every
    subscriber and into the persistent event journal — an api-key-bearing
    key (``llm.api_key``, ``env_keys.EMBEDDING_API_KEY``) must never ride
    along, mirroring the orchestrator's redact-at-rest rule.
    """
    if isinstance(fragment, dict):
        return {
            k: _scrub_secret_values(v)
            for k, v in fragment.items()
            if "api_key" not in k.lower()
        }
    if isinstance(fragment, list):
        return [_scrub_secret_values(v) for v in fragment]
    return fragment


async def _close_datasources_after_turn(
    connections: Dict[str, Any], clients: Dict[str, Any]
) -> None:
    """Close replaced datasource connections once no turn is executing.

    Live datasource removal defers the close because a call may already be
    using an old connection. Security-sensitive tools reject a newly invoked
    stale binding, while deferred close lets an operation already unwinding
    release its resource cleanly. Polls the turn flag and closes as soon as
    the loop parks (or the session ends).
    """
    from agent.core.datasource_setup import close_datasource_connections

    try:
        while _turn_in_flight():
            await asyncio.sleep(0.5)
        close_datasource_connections(connections, clients)
        logger.info(
            "Closed %d replaced datasource connection(s) after turn end",
            len(connections),
        )
    except asyncio.CancelledError:
        # Shutdown teardown — close immediately; nothing left in flight.
        close_datasource_connections(connections, clients)
        raise
    except Exception as e:
        logger.warning("Deferred datasource close failed: %s", e)


async def _model_swap_fit_ladder(new_config: Any) -> Optional[str]:
    """Fit-check ladder for a model hot-swap (live_session_settings.md Slice D).

    Pre-checks the history against the *candidate* model's context budget →
    if over, compacts now (with the old model still bound — a rejection
    leaves the session working untouched) targeting the new budget → if
    still over the candidate's hard window, rejects the swap. Returns None
    when the history fits (possibly after compacting), or a user-facing
    rejection detail. Moves the overflow discovery from the user's next
    message (where a failed compaction dead-ends the turn) to the swap
    itself.

    ``refresh_context_limits`` still handles the threshold rebind after the
    swap is applied; this ladder is the proactive check/compact/reject in
    front of it.
    """
    from agent.core.context import get_token_counter
    from agent.llm.response_guards import strip_removal_markers

    session = _session
    msgs = session.messages
    if not msgs:
        return None

    new_ctx_cfg = session._build_context_config(new_config)
    new_model = new_config.llm.model or "gpt-4"
    counter = get_token_counter(new_model, getattr(new_ctx_cfg, "image_tokens", None))

    live_cm = getattr(session, "context_manager", None)
    # The provider anchor (old model's real input_tokens) is biased-high — it
    # carries the system-prompt/tool-schema overhead the bare list lacks.
    # Same max(local, anchor) philosophy as the per-turn trigger. The anchor
    # minus the bare count also estimates that FIXED overhead — compaction
    # can't shrink it, so the post-compaction verdict must add it back or a
    # window smaller than the prompt+schemas alone slips through the ladder
    # and 413s on the next turn (live-observed: 2.2k bare history passed a
    # 3k window while the real request measured 17.6k).
    anchor = 0
    if live_cm is not None:
        anchor = getattr(live_cm.state, "last_provider_input_tokens", None) or 0
    bare_count = counter(msgs)
    fixed_overhead = max(0, anchor - bare_count)
    projected = max(bare_count, anchor)
    if projected <= new_ctx_cfg.compaction_threshold_tokens:
        return None

    hard_cap = new_ctx_cfg.model_max_context_tokens
    if _turn_in_flight():
        # Compacting the durable list concurrently with a running turn can
        # drop messages the turn appends mid-await — refuse instead.
        return (
            "The conversation is too large for the new model and a response "
            "is still in progress — retry the switch once the current turn "
            "finishes."
        )

    aux = getattr(session, "auxiliary_llm", None)
    if live_cm is None or aux is None:
        # No compaction machinery — allow only when the history at least fits
        # the hard window (per-turn elision can still trim the rest).
        if projected <= hard_cap:
            return None
        return (
            f"Conversation (~{projected:,} tokens) exceeds the context window "
            f"of {new_model} ({hard_cap:,} tokens) and no summarizer is "
            "available to compact it — the model switch was not applied."
        )

    # Compact with the old model still bound, targeting the NEW budget: roll
    # the live manager's limits forward for the compaction call (the
    # _record_compaction plumbing — progress frames, boundary id, stats —
    # reads the live manager, so a scratch manager would lose them) and roll
    # back on failure or rejection.
    old_ctx_cfg = session._build_context_config()
    old_model = session.config.llm.model or "gpt-4"
    before_count = len(msgs)
    runs_before = getattr(live_cm, "compaction_runs", 0)
    # Manual-compact parity: make sure progress frames flow even before the
    # first loop start (idempotent setter; getattr for test doubles).
    _cb_setter = getattr(live_cm, "set_progress_callback", None)
    if callable(_cb_setter):
        _cb_setter(_loop_compaction_progress)
    live_cm.update_limits(new_ctx_cfg, new_model)
    try:
        result = await live_cm.ensure_within_limits(
            msgs,
            aux,
            max_summary_length=getattr(
                session.config.context_management, "max_summary_length", 10000
            ),
            force=True,
            trigger="model_swap",
        )
    except Exception as e:
        live_cm.update_limits(old_ctx_cfg, old_model)
        logger.warning(f"Pre-switch compaction failed: {e}")
        return (
            "The conversation is too large for the new model and compacting "
            f"it failed ({e}) — the model switch was not applied."
        )

    # Adopt durably (the compaction is model-agnostic — it only shrinks — so
    # it stays valid for the old model if the swap is rejected below).
    msgs[:] = strip_removal_markers(result)
    runs_after = getattr(live_cm, "compaction_runs", 0)
    if (
        isinstance(runs_before, int)
        and isinstance(runs_after, int)
        and runs_after > runs_before
    ):
        await _record_compaction(
            extract_summary_text(msgs),
            before_count,
            len(msgs),
            trigger="model_swap",
            ws=None,
        )

    # Recount with the anchor gone (compaction invalidated it): the local
    # count of the compacted history plus the fixed request overhead the
    # anchor measured — the system prompt and tool schemas ride every request
    # and no amount of history compaction shrinks them.
    remaining = counter(msgs) + fixed_overhead
    if remaining > hard_cap:
        live_cm.update_limits(old_ctx_cfg, old_model)
        return (
            f"Conversation still measures ~{remaining:,} tokens after "
            "compaction (including the system prompt and tool schemas) — "
            f"more than the context window of {new_model} ({hard_cap:,} "
            "tokens). The model switch was not applied; start a new session "
            "to use this model."
        )
    return None


async def _handle_config_update(
    ws: WebSocket,
    config_override: Dict[str, Any],
    datasource_ids: Optional[List[str]] = None,
    request_id: Optional[str] = None,
) -> None:
    """Apply runtime config changes (model, temperature, permission mode).

    Deep-merges *config_override* into the session config, rebuilds the
    LLM if the ``llm`` key changed, and persists the update to the
    orchestrator DB so it survives session resume.

    The cockpit only sends the model ID — never the matching ``base_url``
    or ``api_key``. We must let the orchestrator resolve credentials
    BEFORE rebuilding the LLM, otherwise endpoint-backed models silently
    route to api.openai.com with ``not-needed``.

    ``datasource_ids`` (Slice B) is the desired FULL datasource selection
    (``None`` = no change, ``[]`` = detach all). It forwards on the internal
    PATCH — where the orchestrator authorizes it, grant-checks the derived
    tool flip, and persists ``metadata.datasource_ids`` — then the enriched
    datasource payloads are re-fetched via the workspace endpoint (creds
    never ride config_override) and applied through
    ``session.resetup_datasources``; replaced connections close only after
    any in-flight turn ends.

    ``request_id`` (optional, client-chosen) is echoed on the success ack
    and on every error frame this handler emits, so a client with several
    in-flight updates can correlate outcomes (live_session_settings.md P0.3).
    """
    global _session, _orchestrator_client, _thread_id

    saved_snapshot = None

    async def _send_error(message: str, detail: Optional[str] = None) -> None:
        if saved_snapshot is not None:
            detail = (detail + " " if detail else "") + (
                "The settings were saved; reattach the session to activate the saved configuration."
            )
        payload: Dict[str, Any] = {"message": message}
        if detail:
            payload["detail"] = detail
        if request_id:
            payload["request_id"] = request_id
        await _ws_send(ws, "error", payload)

    if not _session:
        await _send_error("No active session")
        return

    security_runtime_update = bool(
        {"workspace", "officer"}.intersection(config_override)
    )
    if security_runtime_update and (
        getattr(_session, "protected_cloud_required", None) is not False
    ):
        # Protected cloud is supported only by the Container/non-Officer
        # runtime established at attach.  Refuse before sanitizing, loading a
        # config, contacting the orchestrator, or mutating any in-memory
        # manager/tool state.  ``is not False`` deliberately fails closed for
        # a malformed/mixed-version session object.
        await _send_error(
            "Session config update rejected",
            detail=(
                "Protected cloud sessions cannot change workspace tier or Officer mode."
            ),
        )
        return

    try:
        config_override = _sanitize_live_session_config_override(config_override)
        if not config_override and datasource_ids is None:
            await _send_error("No supported session config fields were provided")
            return

        import dataclasses

        from shared.runtime.core.loader import (
            _apply_settings_matrix,
            create_llm,
            deep_merge,
            load_agent_config_from_dict,
            strip_loader_owned_keys,
        )

        # Resolve credentials with the orchestrator first when any
        # credential-bearing slot is changing (chat model, auxiliary model,
        # or embedding env keys). The PATCH endpoint enriches the override
        # with the right base_url + api_key (custom/system endpoint or
        # built-in provider key) and returns the merged dict. Every managed
        # change must commit its generation before any local mutation.
        embedding_env_keys = (
            "EMBEDDING_PROVIDER",
            "EMBEDDING_MODEL",
            "EMBEDDING_BASE_URL",
            "EMBEDDING_API_KEY",
            # The rerank slot is credential-bearing too; a change must round-
            # trip through the orchestrator for base_url/api_key enrichment.
            # The bound scorer keeps its attach-time transport until re-attach.
            "RERANK_MODEL",
            "RERANK_BASE_URL",
            "RERANK_API_KEY",
        )
        ds_update = datasource_ids is not None
        tools_update = bool(config_override.get("tools"))
        authorization_update = tools_update or ds_update or security_runtime_update
        effective_override = config_override
        if _orchestrator_client and _thread_id:
            try:
                execution_snapshot = getattr(_session, "execution_snapshot", None)
                enriched = await _orchestrator_client.update_thread_config(
                    _thread_id,
                    config_override,
                    datasource_ids=datasource_ids,
                    snapshot_generation=(
                        execution_snapshot.get("generation")
                        if isinstance(execution_snapshot, dict)
                        else None
                    ),
                )
                if enriched is not None:
                    effective_override = enriched
                else:
                    await _send_error(
                        "Session connector update was rejected"
                        if ds_update
                        else "Session config update was rejected"
                    )
                    return
            except ThreadConfigUpdateDenied as e:
                # A deliberate 4xx (grant denial, invalid override) — surface
                # the orchestrator's detail and never apply locally. This also
                # closes the old fallback hole where a grant-denied model swap
                # fell through to "apply raw override anyway".
                await _send_error("Session config update rejected", detail=e.detail)
                return
            except Exception:
                await _send_error(
                    "Session connector update could not be authorized"
                    if ds_update
                    else "Session config update could not be saved"
                )
                return
        elif authorization_update:
            # A tool/datasource update without the authoritative orchestrator
            # is unsafe: local loading cannot evaluate owner capability grants,
            # and datasource credentials only exist orchestrator-side.
            await _send_error(
                "Session connector update could not be authorized"
                if ds_update
                else "Session config update could not be authorized"
            )
            return

        from shared.runtime.core.session_config_patch import RESOLVED_PATCH_MARKER

        effective_override = dict(effective_override)
        saved_snapshot = effective_override.pop(RESOLVED_PATCH_MARKER, None)

        # Slice B: fetch the enriched datasource payloads BEFORE any local
        # mutation, so a fetch failure leaves the runtime consistent (the
        # durable selection is already updated; the user retries or the next
        # attach converges). Credentials never ride config_override — this
        # internal endpoint re-injects them per fetch.
        new_ds_payload: Optional[List[Dict[str, Any]]] = None
        if ds_update:
            ws_info = await _orchestrator_client.get_thread_workspace(_thread_id)
            if not isinstance(ws_info, dict):
                await _send_error(
                    "Session connector update could not be applied",
                    detail=(
                        "The change was saved but the refreshed connector "
                        "payload could not be fetched; retry, or resume the "
                        "session to converge."
                    ),
                )
                return
            new_ds_payload = ws_info.get("datasources") or []

        # Live `config.update` is the same raw-override shape as the legacy
        # attach path above; normalise before both the merge and the markers.
        # The orchestrator-enriched copy is re-stripped too: whichever side
        # produced it, loader-owned keys never ride an override onto extra.
        effective_override = strip_loader_owned_keys(
            normalize_delegation_block(
                normalize_llm_tiers(
                    normalize_tool_policy(effective_override, source="thread-override"),
                    source="thread-override",
                ),
                source="thread-override",
            )
        )
        base_dict = dataclasses.asdict(_session.config)
        merged = deep_merge(base_dict, effective_override)
        _apply_session_tool_group_markers(merged, effective_override)

        # Re-apply settings_matrix when LLM config changes so model-family
        # defaults (temperature, top_p, limits) are resolved correctly.
        if effective_override.get("llm") and saved_snapshot is None:
            override_llm_keys = set(effective_override["llm"].keys())
            _apply_settings_matrix(
                merged, override_llm_keys, _session.config._deployment_dir
            )

        new_config = load_agent_config_from_dict(
            merged, deployment_dir=_session.config._deployment_dir
        )

        llm_changed = bool(effective_override.get("llm"))
        tools_changed = bool(effective_override.get("tools"))

        # Rebuild chat LLM if llm settings changed
        if llm_changed:
            new_llm = create_llm(
                _llm_config_with_cache_key(new_config.llm), new_config.limits
            )

            # Slice D — model hot-swap hardening. Only when the model itself
            # changes (temperature-only llm fragments skip both rungs):
            # 1. fit ladder: history must fit the candidate's window
            #    (compacting first if needed) or the swap is refused;
            # 2. provider-boundary sanitizer when the swap crosses a
            #    family/provider line: foreign reasoning + tool-call id
            #    formats are the #1 cross-provider session killer.
            old_llm_cfg = _session.config.llm
            model_swapped = bool(
                (effective_override.get("llm") or {}).get("model")
            ) and (new_config.llm.model != old_llm_cfg.model)
            if model_swapped:
                rejection = await _model_swap_fit_ladder(new_config)
                if rejection is not None:
                    await _send_error("Model switch rejected", detail=rejection)
                    return
                from agent.core.context import sanitize_history_for_provider_boundary
                from shared.runtime.core.model_registry import family_of

                crossed_boundary = family_of(new_config.llm.model or "") != family_of(
                    old_llm_cfg.model or ""
                ) or (new_config.llm.provider or "openai") != (
                    old_llm_cfg.provider or "openai"
                )
                if crossed_boundary and _session.messages:
                    # In-memory working set only — persisted thread_messages
                    # rows stay provider-native. Pure-sync, so safe even with
                    # a turn in flight (appends land in the same list object).
                    _session.messages[:] = sanitize_history_for_provider_boundary(
                        _session.messages, new_config.llm.model or ""
                    )

            _session._llm = new_llm
            _session.config = new_config
            logger.info(
                "LLM hot-swapped: model=%s, temperature=%s, base_url=%s",
                new_config.llm.model,
                new_config.llm.temperature,
                new_config.llm.base_url or "default",
            )
        else:
            _session.config = new_config

        ds_summary: Optional[Dict[str, Any]] = None
        if ds_update:
            # resetup_datasources ends in resetup_tools_for_backend(), which
            # also covers a tools fragment riding the same frame. Replaced
            # connections stay open until the in-flight turn (if any) ends —
            # bound tools captured them in closures at load time.
            ds_summary = await _session.resetup_datasources(new_ds_payload or [])
            stale_conns = ds_summary.pop("stale_connections", {})
            stale_clients = ds_summary.pop("stale_clients", {})
            if stale_conns or stale_clients:
                asyncio.create_task(
                    _close_datasources_after_turn(stale_conns, stale_clients)
                )
        elif tools_changed:
            _session.resetup_tools_for_backend()
        elif llm_changed:
            _session._bind_tools()

        # Re-derive the compaction thresholds from the NEW config, in place.
        # Without this the ContextManager keeps the session-start model's
        # window after a model switch — a downswitch (e.g. gpt-5.5 → codex
        # spark) then never compacts and every turn dead-ends in "empty
        # response" once the history exceeds the new model's window. See
        # knowledge-history/done/session_model_switch_stale_context_manager_empty_response.md.
        _session.refresh_context_limits()

        # Rebuild auxiliary LLM if auxiliary settings changed. Symmetric to
        # the chat-side rebuild — the boot-time singleton on _agent doesn't
        # carry the new credentials, so we replace _session.auxiliary_llm
        # with a session-scoped instance.
        if effective_override.get("auxiliary"):
            from shared.runtime.core.loader import LLMConfig, resolve_model_settings
            from shared.runtime.services.auxiliary import AuxiliaryLLM

            aux_cfg = new_config.auxiliary
            model_settings = resolve_model_settings(
                aux_cfg.model, new_config._deployment_dir
            )
            aux_llm_config = LLMConfig(
                model=aux_cfg.model,
                base_url=aux_cfg.base_url,
                api_key=aux_cfg.api_key,
                provider=aux_cfg.provider,
                extra_headers=aux_cfg.extra_headers,
                temperature=aux_cfg.temperature,
                top_p=model_settings.get("top_p"),
                top_k=model_settings.get("top_k"),
                model_max_context_tokens=model_settings.get("model_max_context_tokens"),
                extra_body=model_settings.get("extra_body"),
                max_retries=1,
            )
            aux_structured_output_method = model_settings.get(
                "structured_output_method", "json_schema"
            )
            fallback_model = new_config.llm.model
            fallback_settings = resolve_model_settings(
                fallback_model, new_config._deployment_dir
            )
            aux_inner = create_llm(aux_llm_config, new_config.limits)
            _session.auxiliary_llm = AuxiliaryLLM(
                llm=aux_inner,
                max_iterations=aux_cfg.max_iterations,
                timeout=aux_cfg.timeout,
                max_context_tokens=model_settings.get("model_max_context_tokens"),
                structured_output_method=aux_structured_output_method,
                # Fall back to the (possibly just-rebuilt) main session model when
                # the dedicated aux model is unreachable.
                fallback_llm=_session._llm,
                fallback_structured_output_method=fallback_settings.get(
                    "structured_output_method", "json_schema"
                ),
            )
            logger.info(
                "Auxiliary hot-swapped: model=%s, base_url=%s",
                aux_cfg.model,
                aux_cfg.base_url or "default",
            )

        # Re-resolve the memory-extraction prompt: its prompt-matrix family
        # follows the auxiliary/summarization/main model that may have just
        # changed.
        _session.memory_extraction_prompt = resolve_memory_extraction_prompt(new_config)

        # Keep the MemoryManager runtime in lockstep: its writers read the
        # auxiliary LLM and extraction prompt at event time, so mutating the
        # runtime preserves the B1 hot-swap. memory_config is deliberately
        # NOT re-pointed — the legacy loop freezes its extraction interval
        # at loop start, and the writers must match that.
        if _session.memory_service is not None:
            _session.memory_service.runtime.auxiliary_llm = _session.auxiliary_llm
            _session.memory_service.runtime.extraction_prompt = (
                _session.memory_extraction_prompt
            )

        # The swap replaced the aux LLM with a fresh (unwired) instance — re-point
        # it at the archiver so its failures keep landing in llm_requests.
        _wire_session_aux_archiver()

        # Reset embedding singleton if embedding env keys changed.
        new_env_block = effective_override.get("env_keys") or {}
        if any(k in new_env_block for k in embedding_env_keys):
            for k in embedding_env_keys:
                if k in new_env_block and new_env_block[k] is not None:
                    os.environ[k] = str(new_env_block[k])
            from shared.runtime.services import embedding_service as _embedding_module

            _embedding_module._embedding_service = None
            logger.info(
                "Embedding hot-swapped: provider=%s, model=%s, base_url=%s",
                new_env_block.get(
                    "EMBEDDING_PROVIDER", os.environ.get("EMBEDDING_PROVIDER")
                ),
                new_env_block.get("EMBEDDING_MODEL", os.environ.get("EMBEDDING_MODEL")),
                new_env_block.get(
                    "EMBEDDING_BASE_URL",
                    os.environ.get("EMBEDDING_BASE_URL", "default"),
                ),
            )

        # Update permission mode if included.
        # _session may have been detached concurrently — bail out cleanly
        # instead of AttributeError'ing on assignment.
        if _session is None:
            await _send_error("Session no longer active")
            return
        pm = (config_override.get("interactive") or {}).get("permission_mode")
        if pm and pm in ("supervised", "auto_accept", "autonomous"):
            _session.permission_mode = pm
        nm = (config_override.get("interactive") or {}).get("narration_mode")
        if nm and nm in ("silent", "verbose", "auto"):
            _session.narration_mode = nm
        if saved_snapshot is not None:
            _session.execution_snapshot = saved_snapshot

        # Acknowledge with resolved values — broadcast to every subscriber
        # (all viewers should converge on the new config, and the frame lands
        # in the event journal as the durable transcript record), echoing the
        # applied fragment (secret-scrubbed) + request_id for correlation.
        ack: Dict[str, Any] = {
            "model": new_config.llm.model,
            "temperature": new_config.llm.temperature,
            "permission_mode": _session.permission_mode,
            "applied": _scrub_secret_values(config_override),
        }
        if ds_summary is not None:
            # Names only (no ids/credentials) — feeds the transcript stamp.
            ack["datasources"] = {
                "added": ds_summary.get("added", []),
                "removed": ds_summary.get("removed", []),
            }
            if ds_summary.get("kb_deferred"):
                ack["datasources"]["kb_deferred"] = True
        if request_id:
            ack["request_id"] = request_id
        _broadcast("config.changed", ack)

    except Exception as e:
        logger.exception("Config update failed: %s", e)
        await _send_error(f"Config update failed: {e}")


async def _handle_archive(ws: WebSocket) -> None:
    """Handle /done command — end the session with memory extraction and title."""
    try:
        if not _session:
            await _ws_send(ws, "error", {"message": "Session not ready"})
            return

        archived_thread_id = _thread_id
        archived_runtime_generation = _session_runtime_generation
        archived_disposition = _terminal_retirement_disposition()
        # Close durable input/control/Resume admission before the first
        # teardown-side await. Memory, title, cloud sync and Git finalization
        # can all be slow; leaving the runtime live through those operations
        # would admit work that this archive path is already committed to
        # discarding. The orchestrator's exact generation/token transition is
        # the cross-process authority fence.
        if not await _begin_exact_session_retirement(
            retirement_disposition=archived_disposition,
        ):
            raise EventJournalUnavailable(
                "session retirement admission could not be closed"
            )

        # 0. Final cloud sync. No background poll to stop after Phase 1.
        # Await the last turn's background push first (same contract as
        # _terminate_session): no concurrent walk, no aclose under a push.
        if _session.workspace_sync:
            try:
                await _await_pending_cloud_push()
                await _session.workspace_sync.push_all()
                await _session.workspace_sync.pull_all()
            except Exception as e:
                logger.warning(f"Final cloud sync failed (non-fatal): {e}")
            try:
                await _session.workspace_sync.aclose()
            except Exception as e:
                logger.debug(f"Cloud sync aclose failed (non-fatal): {e}")

        # 1. Extract final memories on the pinned lane. Stateless turns already
        # own durable per-turn obligations; a full-history teardown pass would
        # duplicate writes and refresh memory TTLs a second time.
        # Manager path (memory overhaul Phase 1): the teardown_extractor
        # writer reproduces the gates, the call, and the log line below;
        # the guard flag stops _terminate_session from re-extracting (B11).
        recall_store = (
            getattr(_session.tool_context, "recall_store", None)
            if _session.tool_context
            else None
        )
        if (
            not _stateless_mode()
            and _session.memory_service is not None
            and not _termination_admission_closed()
        ):
            from agent.services.memory import CaptureEvent

            await _session.memory_service.capture(
                CaptureEvent(kind="session_end", messages=_session.messages)
            )
            _session.final_memory_extracted = True
        elif (
            not _stateless_mode()
            and recall_store
            and _session.auxiliary_llm
            and _session.messages
            and not _termination_admission_closed()
        ):
            try:
                from shared.runtime.services.auxiliary import extract_and_store_memories

                await extract_and_store_memories(
                    auxiliary_llm=_session.auxiliary_llm,
                    recall_store=recall_store,
                    messages=_session.messages,
                    memory_extraction_prompt=_session.memory_extraction_prompt,
                )
                logger.info("Final memory extraction complete")
            except Exception as e:
                logger.warning(f"Final memory extraction failed (non-fatal): {e}")

        # 2. Generate title if untitled
        if _session.postgres_conn:
            try:
                thread = await _session.postgres_conn.get_thread(_thread_id)
                current = thread.get("title", "") if thread else ""
                if (
                    not current
                    or current.startswith("Local Session")
                    or current == "Untitled Session"
                ):
                    title = await _generate_title(
                        _session.messages, _session.auxiliary_llm
                    )
                    if title:
                        async with _session.postgres_conn.acquire() as conn:
                            await conn.execute(
                                "UPDATE threads SET title = $2 WHERE id = $1",
                                _thread_id,
                                title,
                            )
            except Exception as e:
                logger.warning(f"Title generation failed (non-fatal): {e}")

        # Common teardown performs the exact pinned-owner settlement. Do not
        # announce a terminal lifecycle edge before that transaction commits:
        # a failed settlement must leave Cockpit in the retryable ``ending``
        # state, not a falsely ended one. The orchestrator journals the
        # authoritative terminal edge; this direct WS acknowledgement is only
        # a low-latency echo for the command issuer after settlement succeeds.
        await _terminate_session("archive")
        terminal_params = {
            "thread_id": archived_thread_id,
            "session_runtime_generation": archived_runtime_generation,
        }
        if archived_disposition == "suspended":
            terminal_params["message"] = (
                "Session suspended. Send a message to resume where you left off."
            )
        await _ws_send(
            ws,
            "session.suspended"
            if archived_disposition == "suspended"
            else "session.ended",
            terminal_params,
        )
        logger.info(f"Session archived: thread={archived_thread_id}")
    except Exception as e:
        logger.warning(f"Archive failed: {e}")
        await _ws_send(ws, "error", {"message": f"Archive failed: {e}"})


async def _update_thread_status(
    status: str,
    *,
    pinned_agent_id: Optional[str] = None,
    retirement_disposition: Optional[str] = None,
    retirement_permanent: Optional[bool] = None,
) -> bool:
    """Durably update status via REST, falling back when REST says ``False``."""
    runtime_generation = _session_runtime_generation
    runtime_attach_token = _session_runtime_attach_token
    runtime_retirement_token = _retirement_admission_token
    if not _stateless_mode() and _pinned_runtime_generation_enabled:
        if runtime_generation is None:
            logger.warning(
                "Pinned runtime generation was advertised but no exact "
                "generation is attached (thread=%s status=%s)",
                _thread_id,
                status,
            )
            return False
    if not _stateless_mode() and (
        _pinned_status_identity_enabled or _pinned_runtime_generation_enabled
    ):
        exact_agent_id = pinned_agent_id or _registered_pinned_agent_id()
        if exact_agent_id is None:
            logger.warning(
                "Pinned status identity was advertised but no registered "
                "agent id is available (thread=%s status=%s)",
                _thread_id,
                status,
            )
            return False
        pinned_agent_id = exact_agent_id
    if _stateless_mode():
        if (
            pinned_agent_id is not None
            or status not in {"active", "awaiting_user"}
            or _session is None
            or _session.postgres_conn is None
            or _thread_id is None
        ):
            return False
        lease_token = _current_stateless_lease_token()
        if lease_token is None:
            return False
        try:
            return await update_stateless_claim_status(
                _session.postgres_conn,
                thread_id=_thread_id,
                lease_token=lease_token,
                status=status,
            )
        except Exception as exc:
            logger.warning(
                "Exact-lease stateless status update failed "
                "(thread=%s status=%s token=%s): %s",
                _thread_id,
                status,
                lease_token,
                exc,
            )
            return False
    if _orchestrator_client and _thread_id:
        try:
            if pinned_agent_id is None:
                if retirement_disposition is not None:
                    return False
                updated = await _orchestrator_client.update_thread_status(
                    _thread_id,
                    status,
                )
            else:
                status_kwargs: dict[str, Any] = {
                    "pinned_agent_id": pinned_agent_id,
                }
                if runtime_generation is not None:
                    status_kwargs["session_runtime_generation"] = runtime_generation
                if runtime_attach_token is not None:
                    status_kwargs["session_runtime_attach_token"] = runtime_attach_token
                if retirement_disposition is not None:
                    status_kwargs["retirement_disposition"] = retirement_disposition
                if retirement_permanent is not None:
                    if status not in {"ending", "ended"}:
                        return False
                    status_kwargs["retirement_permanent"] = retirement_permanent
                if status == "ended" and _pinned_runtime_generation_enabled:
                    identity = _attached_retirement_identity()
                    if (
                        identity is None
                        or _retirement_admission_identity != identity
                        or _retirement_admission_disposition != retirement_disposition
                        or _retirement_admission_permanent is not retirement_permanent
                        or runtime_retirement_token is None
                        or _session is None
                    ):
                        return False
                    protocol = str(
                        getattr(_session, "local_quiescence_protocol", "") or ""
                    )
                    if protocol not in {
                        "workspace_process_zero_v1",
                        "agent_runtime_zero_v1",
                    }:
                        return False
                    status_kwargs["session_runtime_retirement_token"] = (
                        runtime_retirement_token
                    )
                    status_kwargs["local_runtime_quiesced"] = True
                    status_kwargs["local_quiescence_protocol"] = protocol
                    workspace_generation = str(
                        getattr(_session, "workspace_generation", "") or ""
                    )
                    workspace_runtime_incarnation = str(
                        getattr(_session, "workspace_runtime_incarnation", "") or ""
                    )
                    if bool(workspace_generation) != bool(
                        workspace_runtime_incarnation
                    ):
                        return False
                    if (
                        protocol == "workspace_process_zero_v1"
                        and not workspace_generation
                    ):
                        return False
                    if protocol == "agent_runtime_zero_v1" and workspace_generation:
                        return False
                    if workspace_generation:
                        status_kwargs["workspace_generation"] = workspace_generation
                        status_kwargs["workspace_runtime_incarnation"] = (
                            workspace_runtime_incarnation
                        )
                updated = await _orchestrator_client.update_thread_status(
                    _thread_id,
                    status,
                    **status_kwargs,
                )
            if updated:
                return True
        except Exception:
            pass
    # End/ending is a multi-resource retirement transaction owned by the
    # orchestrator. Never reopen Resume with a broad local row write while
    # routes/pods/mounts are still being retired, even during the optional
    # identity rollout phase when `pinned_agent_id` was not supplied.
    if not _stateless_mode() and status in {"ending", "ended"}:
        return False

    # Fallback to direct DB
    if _session and _session.postgres_conn and _thread_id:
        try:
            if pinned_agent_id is not None:
                async with _session.postgres_conn.acquire() as conn:
                    async with conn.transaction():
                        thread = await conn.fetchrow(
                            "SELECT agent_id, execution_lane, runtime_generation, "
                            "runtime_attach_token "
                            "FROM threads "
                            "WHERE id = $1::uuid FOR UPDATE",
                            _thread_id,
                        )
                        if (
                            thread is None
                            or str(thread["execution_lane"] or "") != "pinned"
                            or str(thread["agent_id"] or "") != str(pinned_agent_id)
                            or (
                                runtime_generation is not None
                                and str(thread["runtime_generation"] or "")
                                != runtime_generation
                            )
                            or (
                                runtime_generation is not None
                                and str(thread["runtime_attach_token"] or "")
                                != str(runtime_attach_token or "")
                            )
                        ):
                            return False
                        reciprocal = await conn.fetchval(
                            "SELECT 1 FROM agents WHERE id = $1::uuid "
                            "AND thread_id = $2::uuid FOR SHARE",
                            pinned_agent_id,
                            _thread_id,
                        )
                        if reciprocal is None:
                            return False
                        if status == "ended":
                            # Pinned End is a multi-resource retirement
                            # transaction owned by the orchestrator. A local
                            # fallback that flips the row first would reopen
                            # Resume while old routes/pods/mounts are still
                            # being retired. Fail closed and let the exact
                            # REST request/reconciler retry.
                            return False
                        elif status == "awaiting_user":
                            sql = (
                                "UPDATE threads "
                                "SET status = 'awaiting_user', "
                                "    awaiting_user_since = CASE "
                                "      WHEN status = 'awaiting_user' "
                                "      THEN awaiting_user_since ELSE now() END, "
                                "    extend_count = CASE "
                                "      WHEN status = 'awaiting_user' "
                                "      THEN extend_count ELSE 0 END, "
                                "    last_activity = CURRENT_TIMESTAMP "
                                "WHERE id = $1::uuid AND agent_id = $2::uuid "
                                + (
                                    "AND runtime_generation = $3::uuid "
                                    if runtime_generation is not None
                                    else ""
                                )
                                + (
                                    "AND runtime_attach_token IS NOT DISTINCT "
                                    "FROM $4::uuid "
                                    if runtime_generation is not None
                                    else ""
                                )
                                + "AND status <> 'ended' RETURNING id"
                            )
                            params = [_thread_id, pinned_agent_id]
                            if runtime_generation is not None:
                                params.append(runtime_generation)
                                params.append(runtime_attach_token)
                            updated = await conn.fetchval(sql, *params)
                        elif status == "active":
                            sql = (
                                "UPDATE threads "
                                "SET status = 'active', "
                                "    awaiting_user_since = NULL, "
                                "    extend_count = 0, "
                                "    last_activity = CURRENT_TIMESTAMP "
                                "WHERE id = $1::uuid AND agent_id = $2::uuid "
                                + (
                                    "AND runtime_generation = $3::uuid "
                                    if runtime_generation is not None
                                    else ""
                                )
                                + (
                                    "AND runtime_attach_token IS NOT DISTINCT "
                                    "FROM $4::uuid "
                                    if runtime_generation is not None
                                    else ""
                                )
                                + "AND status <> 'ended' RETURNING id"
                            )
                            params = [_thread_id, pinned_agent_id]
                            if runtime_generation is not None:
                                params.append(runtime_generation)
                                params.append(runtime_attach_token)
                            updated = await conn.fetchval(sql, *params)
                        else:
                            return False
                        return updated is not None
            if status == "ended":
                await _session.postgres_conn.end_thread(_thread_id)
                return True
            else:
                return bool(
                    await _session.postgres_conn.update_thread_status(
                        _thread_id, status
                    )
                )
        except Exception as e:
            logger.warning(f"Failed to update thread status to {status}: {e}")
    return False


async def _reconcile_retirement_begin_or_reopen_controls(
    *,
    identity: tuple[str, Optional[str], Optional[str]],
    exact_agent_id: str,
    retirement_disposition: str,
    retirement_permanent: bool,
    begin_was_sent: bool,
    reopen_if_uncommitted: bool,
) -> bool:
    """Resolve an ambiguous Begin before reopening durable controls.

    The control inbox is closed before the HTTP Begin. A dropped response may
    mean either no retirement exists (the same runtime must reopen controls) or
    an exact T was authorized (the runtime must adopt it and quiesce). Only the
    exact lifecycle projection and token-null reopen CAS may distinguish those
    cases; a generic 409, timeout, or moved successor never authorizes reopen.
    """

    global _retirement_admission_identity, _retirement_admission_disposition
    global _retirement_admission_token, _retirement_admission_permanent

    if _attached_retirement_identity() != identity:
        return False

    async def reopen_exact_runtime() -> bool | None:
        """Reopen durable controls and the already-settled child runtime.

        The DB CAS proves this exact life is still token-null.  Child resume
        then performs its own awaited effect-authority proof.  If that second
        proof or the settled-state check fails, close controls again and latch
        local admission: an otherwise-live session must not resume only half
        of its execution surfaces.
        """

        global _retirement_admission_identity, _retirement_admission_disposition
        global _retirement_admission_token, _retirement_admission_permanent

        if _attached_retirement_identity() != identity:
            return None
        if (
            _retirement_admission_identity == identity
            and _retirement_admission_token is not None
        ):
            return None
        if not await _set_pinned_control_admission(
            agent_id=exact_agent_id,
            open_for_admission=True,
        ):
            return None
        if _retirement_admission_identity == identity:
            # A prior local resume failure may have latched a token-less
            # admission fence. The exact token-null CAS above proves that
            # fence is now safe to clear before SessionHost re-proves effect
            # authority. An authorized token is never reopenable here.
            _retirement_admission_identity = None
            _retirement_admission_disposition = None
            _retirement_admission_token = None
            _retirement_admission_permanent = None
        session = _session
        resume = getattr(session, "resume_subagents", None)
        try:
            if session is None or not callable(resume):
                raise RuntimeError("session has no child-runtime resume boundary")
            await resume()
            if _session is not session or _attached_retirement_identity() != identity:
                raise RuntimeError("session identity moved during child-runtime resume")
            return True
        except Exception:
            logger.warning(
                "Child runtime could not resume after retirement abort "
                "(thread=%s agent=%s)",
                identity[0],
                exact_agent_id,
                exc_info=True,
            )
            try:
                if _attached_retirement_identity() == identity:
                    await _set_pinned_control_admission(
                        agent_id=exact_agent_id,
                        open_for_admission=False,
                    )
            except Exception:
                logger.warning(
                    "Control admission re-close failed after child-runtime "
                    "resume refusal (thread=%s agent=%s)",
                    identity[0],
                    exact_agent_id,
                    exc_info=True,
                )
            if _attached_retirement_identity() == identity:
                _retirement_admission_identity = identity
                _retirement_admission_disposition = retirement_disposition
                _retirement_admission_token = None
                _retirement_admission_permanent = retirement_permanent
            return False

    if not begin_was_sent:
        # No retirement request crossed the process boundary. Reopening still
        # uses the exact token-null DB CAS: a concurrent owner Begin or a moved
        # successor wins and leaves this process closed.
        if reopen_if_uncommitted:
            try:
                await reopen_exact_runtime()
            except Exception:
                logger.warning(
                    "Exact control admission reopen failed after local retirement "
                    "preflight (thread=%s agent=%s)",
                    identity[0],
                    exact_agent_id,
                    exc_info=True,
                )
        return False

    client = _orchestrator_client
    lifecycle_reader = getattr(client, "get_thread_lifecycle", None)
    if not callable(lifecycle_reader):
        # Begin may have committed. A missing outcome reader cannot prove that
        # controls are safe to reopen, even if the transport reported failure.
        return False

    attempt = 0
    while _attached_retirement_identity() == identity:
        try:
            lifecycle = await lifecycle_reader(identity[0])
        except Exception as exc:
            logger.warning(
                "Exact retirement Begin reconciliation failed (thread=%s type=%s)",
                identity[0],
                type(exc).__name__,
            )
            return False
        if not isinstance(lifecycle, dict):
            return False
        observed_generation = _canonical_runtime_generation(
            lifecycle.get("session_runtime_generation")
        )
        observed_attach = _canonical_runtime_generation(
            lifecycle.get("session_runtime_attach_token")
        )
        exact_life = bool(
            observed_generation == identity[1] and observed_attach == identity[2]
        )
        if lifecycle.get("authority_refused") is True or not exact_life:
            # A successor/moved owner must never be reopened by this actor.
            return False
        pending = lifecycle.get("runtime_retirement_pending") is True
        preflight = lifecycle.get("runtime_retirement_preflight") is True
        authorized = lifecycle.get("runtime_retirement_authorized") is True
        if pending and authorized and lifecycle.get("status") == "ending":
            token = _canonical_runtime_generation(
                lifecycle.get("session_runtime_retirement_token")
            )
            if (
                token is not None
                and lifecycle.get("retirement_disposition") == retirement_disposition
                and lifecycle.get("retirement_permanent") is retirement_permanent
            ):
                _retirement_admission_identity = identity
                _retirement_admission_disposition = retirement_disposition
                _retirement_admission_token = token
                _retirement_admission_permanent = retirement_permanent
                return True
            # An authorized but malformed/conflicting immutable authority is
            # not recoverable by this actor and must remain fail-closed.
            return False
        if (
            not pending
            and not preflight
            and not authorized
            and lifecycle.get("status")
            in {
                "created",
                "active",
                "awaiting_user",
            }
        ):
            # This exact read proves no T at its snapshot. The reopen CAS
            # repeats the same token-null predicate, so a Begin landing in
            # between wins and forces another reconciliation iteration.
            if not reopen_if_uncommitted:
                return False
            try:
                reopened = await reopen_exact_runtime()
                if reopened is not None:
                    return False
            except Exception:
                logger.warning(
                    "Exact control admission reopen raced retirement "
                    "reconciliation (thread=%s agent=%s)",
                    identity[0],
                    exact_agent_id,
                    exc_info=True,
                )
                return False
        elif not (pending and preflight and not authorized):
            # Only the server's hidden preflight is expected to remain
            # unresolved until its TTL either authorizes or aborts it.
            return False
        delay = _EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS[
            min(attempt + 1, len(_EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS) - 1)
        ]
        attempt += 1
        if delay:
            await asyncio.sleep(delay)
    return False


async def _begin_exact_session_retirement(
    *,
    pinned_agent_id: Optional[str] = None,
    retirement_disposition: str = "ended",
    retirement_permanent: bool = False,
    reopen_controls_if_uncommitted: bool = True,
) -> bool:
    """Close admission for this exact pinned life and mirror it locally."""

    global _retirement_admission_identity, _retirement_admission_disposition
    global _retirement_admission_token
    global _retirement_admission_permanent

    if _stateless_mode() or _session is None:
        return False
    if retirement_disposition not in {"ended", "suspended"}:
        return False
    if type(retirement_permanent) is not bool:
        return False
    identity = _attached_retirement_identity()
    if identity is None:
        return False
    # Close the whole parent admission surface before child quiescence. A
    # timeout or ambiguous terminal-delivery write leaves the child runtime
    # non-accepting; keeping parent providers/inputs open in that state would
    # create a half-live session. The common termination owner retries this
    # exact tokenless identity until child settlement and Begin converge.
    if _retirement_admission_identity != identity:
        _retirement_admission_identity = identity
        _retirement_admission_disposition = retirement_disposition
        _retirement_admission_token = None
        _retirement_admission_permanent = retirement_permanent
    # Child terminal/transcript writes require the still-current parent
    # authority. Close child admission and settle every generation before the
    # server installs the retirement token that revokes it.
    try:
        await _session.quiesce_subagents(
            f"parent session retiring as {retirement_disposition}"
        )
    except Exception:
        logger.warning(
            "Session child runtime did not quiesce before retirement "
            "(thread=%s disposition=%s)",
            _thread_id,
            retirement_disposition,
            exc_info=True,
        )
        return False
    # From this point onward every child is settled. Exact no-retirement
    # reconciliation clears the tokenless latch and resumes both surfaces
    # together; every other failure stays safely fail-closed.
    if _retirement_admission_identity == identity:
        if _retirement_admission_disposition != retirement_disposition:
            return False
        if _retirement_admission_permanent is not retirement_permanent:
            return False
        if _retirement_admission_token is not None:
            return True
        # The watchdog may have observed an authorised shape whose token was
        # lost/malformed. Fall through to the idempotent exact Begin call and
        # recover T before any local teardown effect.
    exact_agent_id = pinned_agent_id or _registered_pinned_agent_id()
    if exact_agent_id is not None:
        # This is the sole pre-retirement operation. It serializes with public
        # durable-control admission while the runtime token is still open and
        # drains everything admitted before closure. Once `ending` installs a
        # retirement token, the control owner fence intentionally refuses all
        # further consumption; never move this drain after the status call.
        try:
            if not await _close_pinned_control_inbox(agent_id=exact_agent_id):
                if _pinned_runtime_generation_enabled:
                    return await _reconcile_retirement_begin_or_reopen_controls(
                        identity=identity,
                        exact_agent_id=exact_agent_id,
                        retirement_disposition=retirement_disposition,
                        retirement_permanent=retirement_permanent,
                        begin_was_sent=False,
                        reopen_if_uncommitted=reopen_controls_if_uncommitted,
                    )
                return False
        except Exception:
            logger.warning(
                "Exact control preflight failed before retirement (thread=%s agent=%s)",
                _thread_id,
                exact_agent_id,
                exc_info=True,
            )
            if _pinned_runtime_generation_enabled:
                return await _reconcile_retirement_begin_or_reopen_controls(
                    identity=identity,
                    exact_agent_id=exact_agent_id,
                    retirement_disposition=retirement_disposition,
                    retirement_permanent=retirement_permanent,
                    begin_was_sent=False,
                    reopen_if_uncommitted=reopen_controls_if_uncommitted,
                )
            return False
    retirement_token: str | None = None
    if _pinned_runtime_generation_enabled:
        if (
            exact_agent_id is None
            or _session_runtime_generation is None
            or _session_runtime_attach_token is None
            or _orchestrator_client is None
        ):
            if exact_agent_id is not None:
                await _reconcile_retirement_begin_or_reopen_controls(
                    identity=identity,
                    exact_agent_id=exact_agent_id,
                    retirement_disposition=retirement_disposition,
                    retirement_permanent=retirement_permanent,
                    begin_was_sent=False,
                    reopen_if_uncommitted=reopen_controls_if_uncommitted,
                )
            return False
        begin = getattr(_orchestrator_client, "begin_thread_retirement", None)
        if not callable(begin):
            await _reconcile_retirement_begin_or_reopen_controls(
                identity=identity,
                exact_agent_id=exact_agent_id,
                retirement_disposition=retirement_disposition,
                retirement_permanent=retirement_permanent,
                begin_was_sent=False,
                reopen_if_uncommitted=reopen_controls_if_uncommitted,
            )
            return False
        try:
            response = await begin(
                str(_thread_id),
                pinned_agent_id=exact_agent_id,
                session_runtime_generation=_session_runtime_generation,
                session_runtime_attach_token=_session_runtime_attach_token,
                retirement_disposition=retirement_disposition,
                retirement_permanent=retirement_permanent,
            )
        except Exception:
            logger.warning(
                "Exact retirement Begin transport failed (thread=%s agent=%s)",
                identity[0],
                exact_agent_id,
                exc_info=True,
            )
            return await _reconcile_retirement_begin_or_reopen_controls(
                identity=identity,
                exact_agent_id=exact_agent_id,
                retirement_disposition=retirement_disposition,
                retirement_permanent=retirement_permanent,
                begin_was_sent=True,
                reopen_if_uncommitted=reopen_controls_if_uncommitted,
            )
        if not isinstance(response, dict):
            return await _reconcile_retirement_begin_or_reopen_controls(
                identity=identity,
                exact_agent_id=exact_agent_id,
                retirement_disposition=retirement_disposition,
                retirement_permanent=retirement_permanent,
                begin_was_sent=True,
                reopen_if_uncommitted=reopen_controls_if_uncommitted,
            )
        if (
            response.get("status") != "ending"
            or response.get("retirement_disposition") != retirement_disposition
            or response.get("retirement_permanent") is not retirement_permanent
        ):
            return await _reconcile_retirement_begin_or_reopen_controls(
                identity=identity,
                exact_agent_id=exact_agent_id,
                retirement_disposition=retirement_disposition,
                retirement_permanent=retirement_permanent,
                begin_was_sent=True,
                reopen_if_uncommitted=reopen_controls_if_uncommitted,
            )
        retirement_token = _canonical_runtime_generation(
            response.get("session_runtime_retirement_token")
        )
        if retirement_token is None:
            return await _reconcile_retirement_begin_or_reopen_controls(
                identity=identity,
                exact_agent_id=exact_agent_id,
                retirement_disposition=retirement_disposition,
                retirement_permanent=retirement_permanent,
                begin_was_sent=True,
                reopen_if_uncommitted=reopen_controls_if_uncommitted,
            )
    else:
        if not await _update_thread_status(
            "ending",
            pinned_agent_id=exact_agent_id,
            retirement_disposition=retirement_disposition,
            retirement_permanent=retirement_permanent,
        ):
            if exact_agent_id is not None:
                await _reconcile_retirement_begin_or_reopen_controls(
                    identity=identity,
                    exact_agent_id=exact_agent_id,
                    retirement_disposition=retirement_disposition,
                    retirement_permanent=retirement_permanent,
                    begin_was_sent=False,
                    reopen_if_uncommitted=reopen_controls_if_uncommitted,
                )
            return False
    # The server fenced the captured G/token. Never mirror that fence onto a
    # successor attached while the request was in flight.
    if _session is None or _attached_retirement_identity() != identity:
        return False
    _retirement_admission_identity = identity
    _retirement_admission_disposition = retirement_disposition
    _retirement_admission_token = retirement_token
    _retirement_admission_permanent = retirement_permanent
    return True


async def _handle_idle_archive(ws: Optional[WebSocket] = None) -> None:
    """Handle idle timeout — archive session state, set thread to ended.

    `ws` is retained for compatibility with callers that still hold one. The
    orchestrator settlement transaction is the sole terminal-frame publisher.
    """
    try:
        if not _session:
            return

        idle_thread_id = _thread_id
        # Idle exit is just as terminal as an explicit /done. Close exact
        # runtime admission before memory/title/Git awaits so a late control
        # or user input cannot enter behind the decision to retire.
        if not await _begin_exact_session_retirement(
            retirement_disposition=_terminal_retirement_disposition(),
            # IdleTimeoutError means the loop has already returned. Keep the
            # gate closed and let common termination's exact retry owner retry
            # Begin; reopening here would admit work with no loop consumer.
            reopen_controls_if_uncommitted=False,
        ):
            logger.warning(
                "Idle archive could not begin exact retirement; terminal "
                "settlement suppressed (thread=%s)",
                idle_thread_id,
            )
            return

        # 0. Extract memories on the pinned lane. Stateless extraction belongs
        # solely to the per-turn outbox, including its final turn.
        # Manager path (memory overhaul Phase 1): the teardown_extractor
        # writer reproduces the gates, the call, and the log line below.
        recall_store = (
            getattr(_session.tool_context, "recall_store", None)
            if _session.tool_context
            else None
        )
        if (
            not _stateless_mode()
            and _session.memory_service is not None
            and not _termination_admission_closed()
        ):
            from agent.services.memory import CaptureEvent

            await _session.memory_service.capture(
                CaptureEvent(kind="idle_archive", messages=_session.messages)
            )
            _session.final_memory_extracted = True
        elif (
            not _stateless_mode()
            and recall_store
            and _session.auxiliary_llm
            and _session.messages
            and not _termination_admission_closed()
        ):
            try:
                from shared.runtime.services.auxiliary import extract_and_store_memories

                await extract_and_store_memories(
                    auxiliary_llm=_session.auxiliary_llm,
                    recall_store=recall_store,
                    messages=_session.messages,
                    memory_extraction_prompt=_session.memory_extraction_prompt,
                )
                logger.info("Idle archive: memory extraction complete")
            except Exception as e:
                logger.warning(f"Idle archive memory extraction failed: {e}")

        # 1. Generate title if untitled
        if _session.postgres_conn:
            try:
                thread = await _session.postgres_conn.get_thread(_thread_id)
                current = thread.get("title", "") if thread else ""
                if (
                    not current
                    or current.startswith("Local Session")
                    or current == "Untitled Session"
                ):
                    title = await _generate_title(
                        _session.messages, _session.auxiliary_llm
                    )
                    if title:
                        async with _session.postgres_conn.acquire() as conn:
                            await conn.execute(
                                "UPDATE threads SET title = $2 WHERE id = $1",
                                _thread_id,
                                title,
                            )
            except Exception as e:
                logger.warning(f"Idle title generation failed: {e}")

        # 2. Git commit + push. The caller immediately enters common teardown,
        # which alone closes admission, drains controls, and performs the exact
        # pinned-owner lifecycle CAS.
        if _session.workspace_manager:
            git_mgr = getattr(_session.workspace_manager, "git_manager", None)
            if git_mgr and git_mgr.is_active:
                try:
                    if git_mgr.has_uncommitted_changes():
                        git_mgr.commit(f"Idle timeout: thread {_thread_id}")
                    git_mgr.push()
                except Exception as e:
                    logger.warning(f"Idle git push failed: {e}")

        # 3. Close durable admission/Resume, then settle the exact retirement
        # before reporting completion. The orchestrator's settlement owns the
        # durable ``session.ended`` journal edge. Publishing an agent-local
        # terminal frame earlier would make a failed settlement look ended and
        # could let Cockpit reopen Resume while cleanup is still in flight.
        await _terminate_session("idle_timeout")

        logger.info(f"Idle archive complete: thread={idle_thread_id}")
    except Exception as e:
        logger.warning(f"Idle archive failed: {e}")


async def _poll_workspace_ready(
    client: Any,
    thread_id: str,
    timeout: int = 120,
    poll_interval: float = 2.0,
    *,
    raise_on_denied: bool = False,
    vm_timeout: int = _vm_upgrade_poll_timeout,
    require_vm: bool = False,
) -> Optional[Dict[str, Any]]:
    """Poll orchestrator for workspace container readiness.

    ``vm_timeout`` is the extended budget applied automatically once the poll
    observes a VM-backed thread in flight (``vm_status`` provisioning/created):
    a cold KubeVirt boot (CDI import + guest boot) routinely runs minutes past
    the sandbox-container ``timeout`` default, so the deadline self-extends
    rather than declaring a still-booting VM "not ready"
    (knowledge-base/knowledge/features/session_create_on_vm.md).

    ``require_vm`` makes the VM the ONLY acceptable answer: a ready sandbox
    container is refused (and logged as a provisioning leak) instead of being
    returned. Checking ``vm_status`` first is not sufficient on its own — within
    a single iteration a not-yet-ready VM falls through to the container branch,
    and since a container is ready in ~8 s against a multi-minute VM boot it wins
    that race every time. Callers pass this when the thread's resolved tier is
    ``vm``; the sandbox-upgrade caller deliberately does not
    (knowledge-base/knowledge/issues/session_vm_backend_never_attaches.md Defect 2).

    Returns:
        Workspace config dict {"backend": "remote", "remote": {host, port, ...}}
        or None if timeout, unavailable, or no workspace provisioned.
    """
    import time

    start = time.monotonic()
    deadline = start + timeout
    _vm_budget_applied = False

    while time.monotonic() < deadline:
        ws = await client.get_thread_workspace(
            thread_id, raise_on_denied=raise_on_denied
        )
        if not ws:
            # The client collapses every non-200 to None. For a vm-tier
            # session that includes a transient 5xx from the orchestrator
            # (a restart, or a repository authority that is briefly
            # unavailable) and bailing here mis-reports a booting VM as
            # "never became ready" while releasing the pinned agent — the
            # VM budget bounds the retry instead.
            if require_vm:
                logger.warning(
                    "Thread %s: workspace status unavailable — retrying within "
                    "the VM budget.",
                    thread_id,
                )
                await asyncio.sleep(poll_interval)
                continue
            return None

        protected_delivery = _protected_workspace_delivery(ws)
        if protected_delivery == "engaging":
            await asyncio.sleep(poll_interval)
            continue

        # SSH key: orchestrator sends the path it resolved (dev compose
        # key or K8s secret mount); fall back to the K8s default.
        ssh_key = ws.get("ssh_key_path") or "/run/secrets/vm-ssh-key"

        # Check VM workspace first (takes precedence over container)
        vm_status = ws.get("vm_status")

        # A VM-backed thread pays a cold KubeVirt boot far beyond the
        # sandbox-container default. Extend the poll deadline ONCE the moment we
        # observe the VM is in flight so a legitimate cold boot isn't declared
        # "not ready" — self-adjusting, no caller signal needed.
        if not _vm_budget_applied and vm_status in ("provisioning", "created"):
            deadline = start + max(timeout, vm_timeout)
            _vm_budget_applied = True
            logger.info(
                "Thread %s: VM workspace provisioning detected — extending "
                "workspace readiness budget to %ss.",
                thread_id,
                max(timeout, vm_timeout),
            )
        if vm_status == "ready" and ws.get("vm_ssh_host"):
            return {
                "backend": "vm",
                # Server-derived provisioner authority must survive this
                # normalization boundary. PersistentSession deliberately does
                # not trust the provisioner from agent config.
                "workspace_provisioner": ws.get("workspace_provisioner"),
                "workspace_generation": ws.get("workspace_generation"),
                "workspace_runtime_incarnation": ws.get(
                    "workspace_runtime_incarnation"
                ),
                # VM endpoints retain the historical AutoAddPolicy path; Slice
                # 2 admits no stateless VM claimant with a pinned pod identity.
                "workspace_ssh_host_key_fingerprint": None,
                # Slice 1 has no trusted VM host-identity adapter.
                "canvas_presentation_available": False,
                "canvas_live_apps_available": False,
                "canvas_shared_browser_available": False,
                "remote": {
                    "host": ws["vm_ssh_host"],
                    "port": ws.get("vm_ssh_port", 22),
                    "username": "agent-host",
                    "key_path": ssh_key,
                    "workspace_path": "/home/agent-host/workspace",
                },
                "git_remote_url": ws.get("git_remote_url"),
                "managed_repository_credentials": ws.get(
                    "managed_repository_credentials"
                ),
                "repositories": ws.get("repositories"),
                "config_override": ws.get("config_override"),
                "project_ids": ws.get("project_ids") or [],
                "datasources": ws.get("datasources"),
                "nc_session_folder": ws.get("nc_session_folder"),
                "cloud_sync": ws.get("cloud_sync"),
                "cloud_mount": ws.get("cloud_mount"),
                "cloud_sync_degraded": ws.get("cloud_sync_degraded"),
                # F-C1: carried through so _attach_session can fail-close the
                # legacy nc_session_folder sync shim for protected threads.
                "protected_cloud": ws.get("protected_cloud"),
                "protected_cloud_state": ws.get("protected_cloud_state"),
                "protected_cloud_error_code": ws.get("protected_cloud_error_code"),
            }

        # A vm-tier thread accepts no substitute. Bail on a terminal VM instead
        # of burning the full VM budget — the pre-existing 'failed' bail below
        # also requires the CONTAINER to have failed, which never happens on a
        # thread that (correctly) has no container.
        if require_vm:
            if not vm_status:
                # No VM context at all on a vm-tier thread — provisioning was
                # never requested (create_thread sets vm.status='provisioning'
                # synchronously before the agent can poll, and it persists across
                # resume). Terminal, so fail fast rather than sitting out the
                # budget; mirrors the container branch's status=='none' bail.
                logger.warning(
                    "Thread %s: vm-tier session has no VM context — no VM was "
                    "ever provisioned for it.",
                    thread_id,
                )
                return None
            if vm_status == "failed":
                logger.warning(
                    "Thread %s: VM provisioning failed — not falling back to a "
                    "container (vm-tier session).",
                    thread_id,
                )
                return None
            if ws.get("status") == "ready" and ws.get("pod_ip"):
                # A container exists for a vm-tier thread: a provisioning leak
                # (see Defect 1). Refuse it — attaching here is precisely the
                # silent wrong-tier downgrade this guard exists to prevent.
                logger.warning(
                    "Thread %s: ignoring a ready workspace container on a vm-tier "
                    "session (pod %s) — this container should not exist; waiting "
                    "for the VM instead.",
                    thread_id,
                    ws.get("pod_ip"),
                )
            await asyncio.sleep(poll_interval)
            continue

        # Check container workspace
        status = ws.get("status", "none")

        if status == "ready" and ws.get("pod_ip"):
            workspace_generation = ws.get("workspace_generation")
            workspace_runtime_incarnation = ws.get("workspace_runtime_incarnation")
            workspace_ssh_host_key_fingerprint = ws.get(
                "workspace_ssh_host_key_fingerprint"
            )
            if not workspace_generation or not workspace_runtime_incarnation:
                # Never let a detached fingerprint look like independently
                # usable authority. Stateless setup consumes one triplet.
                workspace_ssh_host_key_fingerprint = None
            return {
                "backend": "sandbox",
                # This is orchestrator authority, not an inference from the
                # normalized backend label. Dropping it makes every sandbox
                # attach fail closed before its first model call.
                "workspace_provisioner": ws.get("workspace_provisioner"),
                # Preserve the authoritative protected-ready tuple through
                # normalization.  `_attach_session_inner` revalidates the
                # normalized response immediately before constructing
                # PersistentSession; dropping status/pod coordinates here
                # would turn a valid protected answer into an ambiguous one.
                "status": "ready",
                "pod_ip": ws["pod_ip"],
                "pod_port": ws.get("pod_port") or 30022,
                "ssh_key_path": ssh_key,
                "pinned_status_identity_contract": ws.get(
                    "pinned_status_identity_contract"
                ),
                "pinned_runtime_generation_contract": ws.get(
                    "pinned_runtime_generation_contract"
                ),
                "session_runtime_generation": ws.get("session_runtime_generation"),
                "workspace_generation": workspace_generation,
                "workspace_runtime_incarnation": workspace_runtime_incarnation,
                "workspace_ssh_host_key_fingerprint": (
                    workspace_ssh_host_key_fingerprint
                ),
                # This is an orchestrator-attested capability, not a property
                # inferred from the backend label or endpoint reachability.
                "canvas_presentation_available": (
                    ws.get("canvas_presentation_available") is True
                ),
                "canvas_live_apps_available": (
                    ws.get("canvas_live_apps_available") is True
                ),
                "canvas_shared_browser_available": (
                    ws.get("canvas_shared_browser_available") is True
                ),
                "remote": {
                    "host": ws["pod_ip"],
                    "port": ws.get("pod_port") or 30022,
                    "username": "agent-host",
                    "key_path": ssh_key,
                    "workspace_path": "/home/agent-host/workspace",
                },
                "git_remote_url": ws.get("git_remote_url"),
                "managed_repository_credentials": ws.get(
                    "managed_repository_credentials"
                ),
                "repositories": ws.get("repositories"),
                "config_override": ws.get("config_override"),
                "project_ids": ws.get("project_ids") or [],
                "datasources": ws.get("datasources"),
                "nc_session_folder": ws.get("nc_session_folder"),
                "cloud_sync": ws.get("cloud_sync"),
                "cloud_mount": ws.get("cloud_mount"),
                "cloud_sync_degraded": ws.get("cloud_sync_degraded"),
                # F-C1: see comment above (vm branch).
                "protected_cloud": ws.get("protected_cloud"),
                "protected_cloud_state": ws.get("protected_cloud_state"),
                "protected_cloud_error_code": ws.get("protected_cloud_error_code"),
            }
        if status == "failed" and (not vm_status or vm_status == "failed"):
            # The internal readiness response can carry the one-shot managed
            # repository authority bundle once a runtime is ready.  Never log
            # the response object on a terminal/error branch: a mixed-version
            # or racing response could otherwise put encrypted-handoff
            # plaintext in pod logs.  Status fields are sufficient to diagnose
            # the provisioning failure.
            logger.warning(
                "Workspace provisioning failed for thread %s "
                "(container_status=%s, vm_status=%s)",
                thread_id,
                status,
                vm_status or "none",
            )
            return None
        if status == "none" and not vm_status:
            # No workspace provisioned for this thread (no K8s)
            return None

        # Still creating — wait and poll again
        await asyncio.sleep(poll_interval)

    logger.warning(f"Workspace polling timed out after {timeout}s")
    return None


def _upgrade_already_satisfied(src_backend: Any, target_tier: str) -> bool:
    """True when the live backend already provides ``target_tier`` — the
    workspace upgrade is then a no-op.

    Both ``sandbox`` and ``vm`` are ``RemoteBackend`` (``supports_shell`` is True
    for each), so a plain shell check can't tell them apart. ``vm`` is the only
    tier built with ``sudo_action="allow"`` (its guest owns the sudo gate); a
    ``sandbox`` keeps ``"freeze"`` so its sudo→VM escalation still fires. Hence a
    ``vm`` target is satisfied only by an already-``vm`` backend, while a
    ``sandbox`` target is satisfied by any shell-capable backend (sandbox OR vm).

    This is what lets a sandbox→vm upgrade PROCEED (seed + swap + sudo-reopen)
    instead of short-circuiting on "already supports a shell" — the Q8 bug where
    the sandbox sudo-escalation accept silently no-op'd
    (workspace_tier_upgrade.md Q8).
    """
    if not getattr(src_backend, "supports_shell", False):
        return False
    if target_tier == "vm":
        return getattr(src_backend, "sudo_action", None) == "allow"
    return True


async def _handle_vm_upgrade(ws: WebSocket) -> None:
    """Back-compat alias for the sandbox→vm sudo-escalation accept
    (the ``upgrade-to-vm`` control message).

    Delegates to the unified ``_handle_workspace_upgrade`` vm path so a single
    implementation seeds the new VM from the live workspace, re-opens the sudo
    gate, and persists the tier. The old standalone handler did none of those —
    it swapped to an empty VM, losing the sandbox's working files, and never
    recorded ``backend=vm`` (so suspend/resume re-provisioned a sandbox). The
    cockpit's ``vm_upgrade.needed`` accept now sends ``/upgrade-workspace vm``
    directly; this remains only for older clients still emitting
    ``upgrade-to-vm`` (workspace_tier_upgrade.md Q8).
    """
    await _handle_workspace_upgrade(ws, target_tier="vm")


async def _handle_workspace_upgrade(
    ws: WebSocket, target_tier: str = "sandbox"
) -> None:
    """Handle a lite (``virtual``) → ``sandbox`` workspace upgrade from cockpit.

    The live, in-process counterpart to the worker freeze→re-dispatch path
    (``workspace_tier_upgrade.md`` §4.2 S3): provision a real workspace
    container for a lite session, seed it from the live object-store prefix, and
    hot-swap the backend in place — the conversation never drops (session state
    is in Postgres, one process holds both backends). Flow: request provisioning
    (S2) → poll ready → build ``RemoteBackend`` → **seed** while both backends
    are live (S3a) → ``swap_backend`` → ``resetup_tools_for_backend`` (S1) →
    persist the new tier (S3b).

    Phase 2: the same handler serves ``virtual → vm``. The orchestrator delegates
    vm provisioning (operator-gated); this handler polls vm readiness via
    ``_poll_vm_ready``, builds the backend with ``sudo_action="allow"``, seeds +
    swaps as usual, and re-opens the shell-layer sudo gate after the swap. (The
    pre-existing ``_handle_vm_upgrade`` stays the sandbox→vm sudo-escalation path.)
    """
    if not _session or not _orchestrator_client or not _thread_id:
        await _ws_send(ws, "workspace_upgrade.failed", {"reason": "Session not ready"})
        return

    if getattr(_session, "protected_cloud_required", None) is not False:
        # The protected mount contract is pinned to the Container workspace
        # created at attach.  A hot swap would expose tools to an unsupported
        # backend and detach the joined lower+overlay readiness authority.
        # Refuse before even announcing/provisioning an upgrade.
        await _ws_send(
            ws,
            "workspace_upgrade.failed",
            {
                "reason": (
                    "Protected cloud sessions cannot upgrade or replace their "
                    "Container workspace"
                )
            },
        )
        return

    target_tier = target_tier or "sandbox"
    if target_tier == "sandbox":
        # The old in-process swap has no durable receipt spanning Pod
        # attestation, repository-key delivery, and seed writes. Fail before
        # provisioning or allowing any workspace bytes to leave this process.
        await _ws_send(
            ws,
            "workspace_upgrade.failed",
            {
                "reason": (
                    "Kubernetes hot workspace upgrades require exact runtime "
                    "authority; use the normal pause/redispatch path"
                )
            },
        )
        return
    await _ws_send(
        ws,
        "workspace_upgrade.started",
        {"thread_id": _thread_id, "target_tier": target_tier},
    )

    try:
        src_backend = (
            _session.workspace_manager.backend if _session.workspace_manager else None
        )

        # Already serves the requested tier — nothing to upgrade (idempotent).
        # Tier-aware: a sandbox source does NOT satisfy a vm target (sandbox is
        # shell-capable but unprivileged), so sandbox→vm falls through to provision
        # below instead of short-circuiting here (workspace_tier_upgrade.md Q8).
        if _upgrade_already_satisfied(src_backend, target_tier):
            await _ws_send(
                ws,
                "workspace_upgrade.complete",
                {
                    "thread_id": _thread_id,
                    "target_tier": target_tier,
                    "message": f"Workspace already provides the {target_tier} tier",
                },
            )
            return

        # 1. Request provisioning via orchestrator (S2).
        ok = await _orchestrator_client.request_thread_workspace_upgrade(
            _thread_id, target_tier=target_tier
        )
        if not ok:
            await _ws_send(
                ws,
                "workspace_upgrade.failed",
                {"reason": "Orchestrator rejected workspace upgrade request"},
            )
            return

        # 2. Poll for readiness, then normalize to a {"backend", "remote"} block.
        #    A vm provisions through metadata.vm (vm_status), which _poll_vm_ready
        #    tolerates through the async provisioning window and bails promptly on
        #    'failed'; _poll_workspace_ready would mis-bail on the still-empty
        #    container status. Sandbox keeps the container poller (returns the
        #    block directly).
        if target_tier == "vm":

            async def _emit_vm_progress(elapsed_s: int) -> None:
                # Heartbeat the cockpit so a multi-minute cold VM import isn't a
                # silent black box (workspace_tier_upgrade.md Q7).
                await _ws_send(
                    ws,
                    "workspace_upgrade.progress",
                    {
                        "thread_id": _thread_id,
                        "target_tier": "vm",
                        "elapsed_s": elapsed_s,
                        "timeout_s": _vm_upgrade_poll_timeout,
                    },
                )

            vm_cfg = await _poll_vm_ready(
                _orchestrator_client,
                _thread_id,
                timeout=_vm_upgrade_poll_timeout,
                progress_cb=_emit_vm_progress,
            )
            ssh_key = os.environ.get("SSH_KEY_PATH", "/run/secrets/vm-ssh-key")
            ws_config = (
                {
                    "backend": "vm",
                    "remote": {
                        "host": vm_cfg["ssh_host"],
                        "port": vm_cfg.get("ssh_port", 22),
                        "username": "agent-host",
                        "key_path": ssh_key,
                        "workspace_path": "/home/agent-host/workspace",
                    },
                }
                if vm_cfg
                else None
            )
        else:
            ws_config = await _poll_workspace_ready(
                _orchestrator_client, _thread_id, timeout=300
            )
        if not ws_config or not ws_config.get("remote"):
            # A vm that never came ready (usually the cold ~2.8GB CDI import
            # outrunning the poll budget) leaves a half-provisioned VM +
            # DataVolume + importer pod running with nobody attached. Tear it down
            # so it doesn't leak — the orphan that previously needed a manual
            # `kubectl delete` (workspace_tier_upgrade.md Q7). Best-effort.
            if target_tier == "vm":
                try:
                    await _orchestrator_client.abort_thread_vm_upgrade(_thread_id)
                except Exception as e:
                    logger.warning(
                        f"VM abort/teardown after failed upgrade ({_thread_id}): {e}"
                    )
            await _ws_send(
                ws,
                "workspace_upgrade.failed",
                {"reason": f"{target_tier} workspace did not become ready in time"},
            )
            return

        # 3. Build a RemoteBackend from the ready connection block. The sandbox
        #    keeps sudo_action="freeze" — preserving the existing sandbox→VM
        #    sudo escalation; only a vm target allows sudo through.
        from shared.runtime.core.backends.remote import RemoteBackend

        backend_tier = ws_config.get("backend", "sandbox")
        remote = ws_config["remote"]
        shell_config = _session.config.extra.get("shell", {})
        sudo_action = "allow" if backend_tier == "vm" else "freeze"
        shell_owner_token = _session.shell_owner_token
        workspace_generation = ws_config.get("workspace_generation")
        workspace_runtime_incarnation = ws_config.get("workspace_runtime_incarnation")
        workspace_ssh_host_key_fingerprint = ws_config.get(
            "workspace_ssh_host_key_fingerprint"
        )
        if shell_owner_token is not None and (
            not workspace_generation
            or not workspace_runtime_incarnation
            or not workspace_ssh_host_key_fingerprint
        ):
            raise WorkspaceUnavailableError(
                "A stateless physical workspace upgrade requires an "
                "orchestrator-attested backing, runtime incarnation, and SSH "
                "host identity"
            )
        new_backend = RemoteBackend(
            host=remote["host"],
            port=remote.get("port", 30022),
            username=remote.get("username", "agent-host"),
            key_path=remote.get("key_path"),
            workspace_path=remote.get("workspace_path", "/home/agent-host/workspace"),
            job_id=_thread_id,
            default_timeout=shell_config.get("default_timeout", 120),
            max_tabs=shell_config.get("max_tabs", 15),
            connect_timeout=remote.get("connect_timeout", 30),
            max_retries=remote.get("max_retries", 5),
            retry_timeouts_as_booting=remote.get("retry_timeouts_as_booting", False),
            sudo_action=sudo_action,
            workspace_generation=(
                workspace_generation
                if workspace_generation and workspace_runtime_incarnation
                else None
            ),
            runtime_incarnation=(
                workspace_runtime_incarnation
                if workspace_generation and workspace_runtime_incarnation
                else None
            ),
            expected_host_key_fingerprint=(
                workspace_ssh_host_key_fingerprint
                if shell_owner_token is not None
                else None
            ),
        )
        # Capability is attested by the orchestrator from a paired generation
        # and pinned workspace identity. Never infer it from "sandbox": a
        # usable Docker workspace may intentionally lack Canvas attestation.
        new_backend.supports_canvas_presentation = (
            ws_config.get("canvas_presentation_available") is True
        )
        new_backend.supports_canvas_live_apps = (
            ws_config.get("canvas_live_apps_available") is True
        )
        new_backend.supports_canvas_shared_browser = (
            ws_config.get("canvas_shared_browser_available") is True
        )

        # 4. Connect the new backend now so the SEED copy (next) runs while BOTH
        #    backends are live — swap_backend would otherwise disconnect the old
        #    one. swap_backend then sees it connected and skips re-connecting.
        if shell_owner_token is not None:
            new_backend.set_shell_owner_token(shell_owner_token)
        await asyncio.to_thread(new_backend.connect)
        if shell_owner_token is not None:
            # Promote before the swap exposes this backend to tools. The same
            # backing+runtime fence used on a cold attach protects hot upgrades.
            await asyncio.to_thread(new_backend.claim_shell_owner)

        # 5. Seed the new workspace from the live virtual prefix (S3a). Pure
        #    in-process copy (the agent holds the object-store creds). Run off
        #    the event loop — SFTP writes are blocking.
        seeded = 0
        if src_backend is not None:
            from agent.core.backends.seed import seed_workspace

            seeded = await asyncio.to_thread(seed_workspace, src_backend, new_backend)
            logger.info(
                f"Seeded {seeded} file(s) into upgraded workspace for {_thread_id}"
            )

        # 6. Hot-swap + re-derive the toolset (S1) so shell/git/file tools
        #    appear on the next turn (get_current_tools re-reads per turn).
        _session.swap_backend(new_backend)

        # 6a. Re-establish the OpenCloud cloud mount on the NEW backend. The
        #     mount is a per-host rclone process, so it does NOT follow the
        #     backend swap — without this the agent loses the cloud data it
        #     upgraded to keep working on. Re-fetch the freshly-built payload
        #     (the orchestrator now targets the public WebDAV URL + read-only
        #     for a vm runtime) and remount BEFORE retooling, since
        #     srw_cloud_status exposure is gated on an active mount. Best-effort:
        #     a remount failure must not abort the otherwise-successful upgrade.
        #     See knowledge-base/knowledge/issues/workspace_upgrade_drops_cloud_mount.md.
        try:
            _ws_info = await _orchestrator_client.get_thread_workspace(_thread_id)
            _fresh_cloud_mount = _ws_info.get("cloud_mount") if _ws_info else None
            if _fresh_cloud_mount:
                if _session.cloud_mount_manager is not None:
                    await _session.cloud_mount_manager.aclose()
                    _session.cloud_mount_manager = None
                _session.cloud_mount_error = None
                await _session._setup_cloud_mount(_fresh_cloud_mount)
                _mgr = _session.cloud_mount_manager
                if _mgr is None or not getattr(_mgr, "active", False):
                    await _ws_send(
                        ws,
                        "workspace_upgrade.cloud_mount_degraded",
                        {
                            "thread_id": _thread_id,
                            "reason": _session.cloud_mount_error or "mount inactive",
                        },
                    )
        except Exception as e:
            logger.warning(f"Cloud remount after upgrade failed ({_thread_id}): {e}")

        _session.resetup_tools_for_backend()

        # A vm keeps its own in-guest sudo gate — re-open the shell-layer sudo
        # intercept. swap_backend rebuilds ShellManager with the config-default
        # ("freeze"), and ShellManager._check_blocked gates sudo BEFORE the
        # backend, so a vm-upgraded session would otherwise freeze on sudo. The
        # sandbox tier deliberately keeps "freeze" so its sudo→VM escalation
        # still fires. Mirrors _handle_vm_upgrade.
        if (
            backend_tier == "vm"
            and _session.shell_manager
            and hasattr(_session.shell_manager, "sudo_action")
        ):
            _session.shell_manager.sudo_action = "allow"

        # 7. Persist the new tier (S3b) so the suspend/resume/reconcile
        #    lifecycle engages and a resumed session re-provisions a sandbox,
        #    not a virtual. Deep-merged into metadata.config_override; non-fatal.
        try:
            await _orchestrator_client.update_thread_config(
                _thread_id, {"workspace": {"backend": backend_tier}}
            )
        except Exception as e:
            logger.warning(f"Persisting upgraded tier failed (non-fatal): {e}")

        await _ws_send(
            ws,
            "workspace_upgrade.complete",
            {
                "thread_id": _thread_id,
                "target_tier": backend_tier,
                "seeded_files": seeded,
            },
        )
        logger.info(
            f"Workspace upgrade ({backend_tier}) complete for thread {_thread_id}"
        )

    except Exception as e:
        logger.exception(f"Workspace upgrade failed for thread {_thread_id}")
        # A failure AFTER the vm was provisioned (e.g. the seed or swap step,
        # once vm_status was already ready) would otherwise leak the running VM —
        # only the poll-timeout path tore it down before. Tear it down here too
        # (idempotent; a no-op if no vm was created) so no failure path leaks a
        # ready VM (workspace_tier_upgrade.md Q7).
        if target_tier == "vm" and _orchestrator_client is not None:
            try:
                await _orchestrator_client.abort_thread_vm_upgrade(_thread_id)
            except Exception as ee:
                logger.warning(
                    f"VM teardown after upgrade failure ({_thread_id}): {ee}"
                )
        await _ws_send(ws, "workspace_upgrade.failed", {"reason": str(e)})


async def _poll_vm_ready(
    client: Any,
    thread_id: str,
    timeout: Optional[int] = None,
    poll_interval: float = 3.0,
    progress_cb: Optional[Callable[[int], Awaitable[None]]] = None,
) -> Optional[Dict[str, Any]]:
    """Poll orchestrator for VM readiness.

    Args:
        timeout: seconds to wait before giving up; defaults to
            ``_vm_upgrade_poll_timeout`` (env ``VM_UPGRADE_POLL_TIMEOUT``) so the
            cold-import budget is tunable per cluster (Q7).
        progress_cb: optional async heartbeat invoked ~every 60s with elapsed
            seconds, so the cockpit can show a live "still provisioning" notice
            instead of a multi-minute black box (kept sparse to avoid spamming
            the transcript, since each fires a system message).

    Returns:
        VM config dict {"ssh_host": ..., "ssh_port": ...} or None on timeout/failure.
    """
    import time

    adaptive_preparation_timeout = timeout is None
    if timeout is None:
        timeout = _vm_upgrade_poll_timeout
    start = time.monotonic()
    deadline = start + timeout
    last_progress = start

    while time.monotonic() < deadline:
        ws = await client.get_thread_workspace(thread_id)
        if ws:
            preparation_budget = ws.get("vm_preparation_timeout_s", 0)
            if (
                adaptive_preparation_timeout
                and type(preparation_budget) is int
                and 0 < preparation_budget <= 3 * 86400 + 3900
            ):
                # Server-owned preparation gets one bounded extension. Repeated
                # progress responses never move the absolute deadline forward.
                deadline = max(deadline, start + timeout + preparation_budget)
            vm_status = ws.get("vm_status")
            if vm_status == "ready" and ws.get("vm_ssh_host"):
                return {
                    "ssh_host": ws["vm_ssh_host"],
                    "ssh_port": ws.get("vm_ssh_port", 22),
                }
            if vm_status == "failed":
                logger.warning(f"VM provisioning failed for thread {thread_id}")
                return None

        now = time.monotonic()
        if progress_cb is not None and (now - last_progress) >= 60.0:
            last_progress = now
            try:
                await progress_cb(int(now - start))
            except Exception:
                pass

        await asyncio.sleep(poll_interval)

    logger.warning(f"VM polling timed out after {timeout}s for thread {thread_id}")
    return None


def _excerpt_for_title(text: str, cap: int = 240) -> str:
    """Trim a message to a short excerpt for titling.

    Cuts on a word boundary (never mid-word) and appends an ellipsis when
    trimmed, so the title model reads it as an excerpt rather than a message
    that "got cut off". The old hard 200-char chop landed mid-word (e.g.
    ``the dog has a "Que``), which made the aux chat-model reply "It sounds
    like your message got cut off" — and that reply became the thread title.
    """
    text = text.strip()
    if len(text) <= cap:
        return text
    head = text[:cap].rsplit(None, 1)[0]  # drop the partial trailing word
    return f"{head} …"


# Deflection phrases an aux chat-model emits when it "answers" the sample
# instead of titling it — a truncated opener reads as cut-off, or the text
# references an image/table the title call never received (list-of-blocks image
# parts are dropped below). Matched as lowercased substrings; a hit means the
# model replied instead of titling, so the "title" is rejected and the
# after-turn pass (which has the assistant's real reply for context) retries.
_CONVERSATIONAL_TITLE_MARKERS = (
    "i don't see",
    "i do not see",
    "i can't see",
    "i cannot see",
    "don't see any",
    "no attached",
    "wasn't attached",
    "isn't attached",
    "didn't come through",
    "did not come through",
    "message got cut off",
    "got cut off",
    "it sounds like",
    "your message",
    "the file may not",
    "appears to be empty",
    "i'm sorry",
    "i am sorry",
    "sorry, ",
    "as an ai",
    "i'm not able",
    "i am not able",
)


def _title_looks_conversational(title: str) -> bool:
    """True when a generated "title" is really the aux model replying to the
    sample (deflecting about a truncated message or a missing image/table)
    rather than naming the topic. Such a reply must never become the title.
    """
    text = (title or "").strip().lower()
    if not text:
        return False
    # A real title is a short fragment; a conversational reply runs long even
    # after the 100-char clamp. Both observed regressions were 13+ words.
    if len(text.split()) > 12:
        return True
    return any(marker in text for marker in _CONVERSATIONAL_TITLE_MARKERS)


async def _generate_title(messages: List[Any], auxiliary_llm: Any) -> Optional[str]:
    """Generate a short title from conversation using AuxiliaryLLM."""
    if _termination_admission_closed():
        logger.debug("Title generation skipped: termination admission is closed")
        return None
    if not auxiliary_llm or not messages:
        logger.debug(
            "Title generation skipped: auxiliary_llm=%s, messages=%d",
            bool(auxiliary_llm),
            len(messages) if messages else 0,
        )
        return None
    try:
        # Grab first few exchanges for title generation
        sample = []
        for m in messages[:10]:
            content = getattr(m, "content", None)
            if isinstance(content, str) and content:
                sample.append(_excerpt_for_title(content))
            elif isinstance(content, list):
                # Handle list-of-blocks content (e.g. responses API). Image
                # parts have no "text" and are dropped — hence the prompt below
                # warns the model the excerpt may reference images it can't see.
                text_parts = [
                    b.get("text", "") if isinstance(b, dict) else str(b)
                    for b in content
                ]
                joined = " ".join(t for t in text_parts if t)
                if joined:
                    sample.append(_excerpt_for_title(joined))
        if not sample:
            logger.debug(
                "Title generation skipped: no text content in %d messages",
                len(messages),
            )
            return None

        # Structured output (single-field ConversationTitle) is the primary
        # guard against the model *answering* the sample instead of *labelling*
        # it: a schema slot has no room for "I don't see your image". Routed
        # through AuxiliaryLLM.chain — the same structured path (with per-family
        # method + recovery) as every other aux task, so it also inherits the
        # main-model fallback rather than a silent "Untitled Session".
        from shared.runtime.services.auxiliary import GenerateTitleTask

        if _termination_admission_closed():
            logger.debug("Title generation skipped: termination admission closed")
            return None
        result = await auxiliary_llm.chain(GenerateTitleTask("\n".join(sample)))
        auxiliary_llm.health.record_success("title_generation")
        raw_title = (getattr(result, "title", None) or "").strip()
        title = raw_title[:100] if raw_title else None
        if not title:
            logger.debug("Title generation returned empty response")
            return None
        # Backstop: even under a schema a model can occasionally stuff a
        # deflection into the title field. Reject it and leave the current title
        # (placeholder or draft) so nothing conversational is ever persisted.
        if _title_looks_conversational(title):
            logger.info("Title generation rejected conversational reply: %r", title)
            return None
        return title
    except Exception as e:
        if auxiliary_llm is not None:
            auxiliary_llm.health.record_failure("title_generation", e)
        logger.warning(f"Title generation error: {e}")
        return None


def _title_is_placeholder(current: Optional[str]) -> bool:
    """True while the thread still carries a default placeholder title.

    Shared guard for both titling passes (early-from-prompt and after-turn) so
    each is a no-op once the other has written a real title — and so a manual
    rename is never clobbered.
    """
    return (
        not current
        or current.startswith("Local Session")
        or current == "Untitled Session"
    )


# Opening lines too terse/generic to mint a useful title from. Matched after
# lowercasing and stripping trailing punctuation; anything <10 chars is also
# treated as low-signal. Such prompts fall through to _auto_title_after_first_turn.
_LOW_SIGNAL_PROMPTS = frozenset(
    {
        "hi",
        "hey",
        "hello",
        "yo",
        "sup",
        "ok",
        "okay",
        "thanks",
        "thank you",
        "help",
        "please help",
        "can you help",
        "can you help me",
        "go on",
        "go ahead",
        "keep going",
        "carry on",
        "continue",
        "resume",
        "are you there",
        "whats up",
        "what's up",
        "test",
        "testing",
    }
)


def _is_low_signal_prompt(content: str) -> bool:
    """A prompt too terse/generic to title from — greeting, ack, or 'continue'.

    These are left to the after-turn fallback, which titles from the agent's
    response once there's real content, rather than minting a useless
    "Hello" / "Continue" title from the opening line.
    """
    text = (content or "").strip().lower()
    if len(text) < 10:
        return True
    return text.rstrip("?!. ") in _LOW_SIGNAL_PROMPTS


async def _write_title_if_placeholder(
    title: str,
    *,
    origin: str,
    allow_draft_overwrite: bool = False,
    expected_session: Any | None = None,
    expected_thread_id: str | None = None,
    expected_generation: int | None = None,
) -> bool:
    """Write ``title`` while the thread is still untitled — or, when
    ``allow_draft_overwrite``, while it still shows the LLM-free draft — then
    broadcast.

    The re-check under this shared guard makes the draft and after-turn passes
    idempotent against each other and against a manual rename: a title that is
    neither a placeholder nor the outstanding draft (i.e. a user rename) is left
    untouched. Returns True iff the title was written.
    """
    session = expected_session if expected_session is not None else _session
    thread_id = expected_thread_id if expected_thread_id is not None else _thread_id
    generation = (
        expected_generation if expected_generation is not None else _session_generation
    )
    if not title or not session or not session.postgres_conn or not thread_id:
        return False
    if not _session_identity_matches(session, str(thread_id), int(generation)):
        return False
    thread = await session.postgres_conn.get_thread(thread_id)
    if not _session_identity_matches(session, str(thread_id), int(generation)):
        return False
    current = thread.get("title", "") if thread else ""
    writable = _title_is_placeholder(current) or (
        allow_draft_overwrite
        and _draft_title_value is not None
        and current == _draft_title_value
    )
    if not writable:
        return False
    async with session.postgres_conn.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET title = $2 WHERE id = $1",
            thread_id,
            title,
        )
    if not _session_identity_matches(session, str(thread_id), int(generation)):
        # The write was bound to the captured thread, so it cannot corrupt the
        # successor; suppress only the process-global broadcast/marker update.
        return False
    _broadcast("title.updated", {"title": title})
    logger.info("%s-titled thread %s: %s", origin, thread_id, title)
    return True


def _draft_title_from_prompt(
    content: str, *, max_words: int = 8, max_chars: int = 60
) -> Optional[str]:
    """A cheap, LLM-free draft title lifted straight from the opening prompt.

    Deliberately no aux call: a lone, possibly-truncated or image-referencing
    prompt is exactly the input that baits a chat-model into replying ("I don't
    see your image") instead of titling, and that reply used to land as the
    title. A string slice can't deflect. Takes the first few words, bounded on a
    word boundary. Returns None when the prompt has no usable words.
    """
    text = " ".join((content or "").split())  # collapse whitespace/newlines
    if not text:
        return None
    draft = " ".join(text.split(" ")[:max_words])
    if len(draft) > max_chars:
        draft = draft[:max_chars].rsplit(" ", 1)[0]
    draft = draft.strip(" -–—:;,\"'")
    return draft or None


async def _early_title_from_prompt(
    content: str,
    *,
    expected_session: Any | None = None,
    expected_thread_id: str | None = None,
    expected_generation: int | None = None,
) -> None:
    """Fill the cockpit header the instant the user submits, with an LLM-free
    draft taken from the opening prompt — so it fills on submit instead of only
    after the (possibly long) first turn lands.

    Primary *placeholder* path. Fire-and-forget from _accept_user_input; must
    never block input acceptance. The authoritative, grounded, schema-bound LLM
    title is minted later by _auto_title_after_first_turn, which replaces this
    draft. Low-signal prompts get no draft and are left to that pass.
    """
    global _draft_title_value
    try:
        session = expected_session if expected_session is not None else _session
        thread_id = expected_thread_id if expected_thread_id is not None else _thread_id
        generation = (
            expected_generation
            if expected_generation is not None
            else _session_generation
        )
        if not session or not session.postgres_conn or not thread_id:
            return
        if not _session_identity_matches(session, str(thread_id), int(generation)):
            return
        if _is_low_signal_prompt(content):
            return
        # Cheap early-out on an already-titled thread (e.g. a resumed session).
        thread = await session.postgres_conn.get_thread(thread_id)
        if not _session_identity_matches(session, str(thread_id), int(generation)):
            return
        if not _title_is_placeholder(thread.get("title", "") if thread else ""):
            return
        draft = _draft_title_from_prompt(content)
        if draft and await _write_title_if_placeholder(
            draft,
            origin="Draft",
            expected_session=session,
            expected_thread_id=str(thread_id),
            expected_generation=int(generation),
        ):
            _draft_title_value = draft
    except Exception as e:
        logger.warning(f"Early draft title failed (non-fatal): {e}")


async def _auto_title_after_first_turn() -> None:
    """Authoritative title pass, fired from _loop_on_turn_complete on the first
    few turns. Mints the real title from the grounded sample
    (``_session.messages`` — user message *plus* the assistant's reply, a
    completed exchange) via structured output, and replaces the LLM-free draft
    written at submit time.

    Titling from a completed exchange is the input shape that never baited the
    model into replying, which is why the authoritative pass lives here and the
    submit-time pass only drafts. A no-op once a real, non-draft title exists
    (e.g. a manual rename) — see _write_title_if_placeholder's shared guard.
    """
    global _draft_title_value
    try:
        if not _session or not _session.postgres_conn or not _thread_id:
            return
        thread = await _session.postgres_conn.get_thread(_thread_id)
        current = thread.get("title", "") if thread else ""
        # Overwrite a placeholder or our own draft — never a manual rename.
        if not _title_is_placeholder(current) and current != _draft_title_value:
            return
        title = await _generate_title(_session.messages, _session.auxiliary_llm)
        if title and await _write_title_if_placeholder(
            title, origin="Auto", allow_draft_overwrite=True
        ):
            _draft_title_value = None
    except Exception as e:
        logger.warning(f"Auto-title generation failed (non-fatal): {e}")
