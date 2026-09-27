"""Runtime owner of persistent-session input admission, delivery and interrupts.

One :class:`SessionInputRuntime` per runtime holds the state the loop and the
transports share for input: the loop's input queue, the claims this process
generation has published into it, the reclaim lock and its protected-cloud
reclaim task, the interrupt mode/target/hard-cancel event, and the window in
which the loop is parked waiting for input.

Everything else arrives through :class:`SessionInputPorts`, every one of them
a call-time provider. Runtime identity in particular is read through
``ports.identity()`` at each operation boundary — never at construction and
never cached across an await the operation re-reads over — so a renewed or
replaced lease, a rotated attach token or a successor session is observed
exactly where the runtime always observed it. The identity values themselves
stay with their owners; this module only defines what it reads.

This module does not import the runtime that composes it, an application
factory, the loop or the worker graph (import contract and boundary guard).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional
from uuid import UUID, uuid4

from agent.api.lease_context import LeaseHandle
from agent.api.session_contract import (
    AcceptedInput,
    DurableInputUnavailable,
    ProtectedCloudUnavailable,
    SessionIdentityMismatch,
    TerminationAdmissionClosed,
)

_logger = logging.getLogger(__name__)

# Bounded durable-inbox poll while the pinned loop waits for input.
PINNED_INPUT_POLL_SECONDS = 1.0


@dataclass(frozen=True, slots=True)
class SessionRuntimeIdentity:
    """One synchronous read of the attached runtime's identity.

    ``process_generation`` is this process incarnation's input generation
    (minted per attach); ``session_generation`` is the durable session runtime
    generation; ``attach_generation`` is the local attach counter that scopes
    session side tasks. ``lease`` is the *current* stateless lease handle, or
    ``None`` on the pinned lane.
    """

    thread_id: Optional[str]
    process_generation: Optional[str]
    session_generation: Optional[str]
    attach_token: Optional[str]
    agent_id: Optional[str]
    pod_uid: Optional[str]
    lease: Optional[LeaseHandle]
    attach_generation: int


@dataclass(frozen=True, slots=True)
class InputWaitPlan:
    """How long the loop may wait for input, and what a timeout yields.

    ``on_timeout`` returns the item to deliver (an Officer backstop wake) or
    raises (idle timeout). With no timeout the wait is unbounded.
    """

    timeout_seconds: Optional[float] = None
    on_timeout: Optional[Callable[[], Any]] = None


@dataclass(frozen=True, slots=True)
class SessionInputPorts:
    """Call-time dependencies of the input owner.

    ``begin_input_wait`` runs the loop's turn-boundary effects (the ``ready``
    frame, the awaiting-user flip, Officer sleep filing) and returns the wait
    plan; the owner sets the parked window only after it returns.
    ``human_input_accepted`` receives the content of every accepted human
    input (the early-title hook).
    """

    session: Callable[[], Any]
    identity: Callable[[], SessionRuntimeIdentity]
    stateless_mode: Callable[[], bool]
    runtime_admission_closed: Callable[[], bool]
    protected_cloud_ready: Callable[[], bool]
    identity_fingerprint: Callable[[], Optional[str]]
    cancellation_enabled: Callable[[], bool]
    turn_open: Callable[[], bool]
    tool_inflight: Callable[[], bool]
    broadcast: Callable[[str, dict[str, Any]], Any]
    track_side_task: Callable[[asyncio.Task[Any]], asyncio.Task[Any]]
    human_input_accepted: Callable[[str], None]
    begin_input_wait: Callable[[], Awaitable[InputWaitPlan]]


class SessionInputRuntime:
    """Input queue, claimed deliveries, reclaim and interrupt state of one runtime."""

    def __init__(
        self,
        ports: SessionInputPorts,
        *,
        poll_seconds: float = PINNED_INPUT_POLL_SECONDS,
        logger: logging.Logger | None = None,
    ) -> None:
        self._ports = ports
        self._poll_seconds = poll_seconds
        self._logger = logger or _logger
        # Loop-facing input queue. Published after attach recovery, cleared at
        # teardown; survives socket reconnects.
        self._queue: Optional[asyncio.Queue] = None
        # (delivery_id, claim_generation) published into this queue by this
        # process generation. A retry observes it; a process death loses it
        # and the successor generation reclaims the durable row.
        self._queued_claims: set[tuple[str, int]] = set()
        # Serializes durable reclaim with local queue/priority publication. In
        # particular, a concurrent human B may not steal a just-deferred wake
        # A between A's generation CAS and its priority reclaim.
        self._reclaim_lock = asyncio.Lock()
        self._protected_reclaim_task: Optional[asyncio.Task[Any]] = None
        # Tri-state interrupt: None, "graceful" (stop after the current tool
        # call) or "hard" (cancel the in-flight stream). The target is the
        # exact transcript turn it belongs to; a mode without a matching
        # target is invalid and is cleared rather than allowed to strike a
        # successor turn.
        self._interrupt_mode: Optional[str] = None
        self._interrupt_target_turn_id: Optional[int] = None
        # Set with a hard interrupt so the loop can tear down a blocked LLM /
        # auxiliary await immediately. Created per attach, cleared whenever
        # the interrupt is consumed or cleared.
        self._hard_interrupt_event: Optional[asyncio.Event] = None
        # True exactly while the loop is parked in the input wait — the only
        # state where an out-of-band teardown cannot kill work mid-turn.
        self._awaiting_input = False

    # --- State views -------------------------------------------------------

    @property
    def queue(self) -> Optional[asyncio.Queue]:
        return self._queue

    @property
    def awaiting_input(self) -> bool:
        return self._awaiting_input

    @property
    def hard_interrupt_event(self) -> Optional[asyncio.Event]:
        return self._hard_interrupt_event

    @property
    def interrupt_mode(self) -> Optional[str]:
        return self._interrupt_mode

    @property
    def interrupt_target_turn_id(self) -> Optional[int]:
        return self._interrupt_target_turn_id

    @property
    def queued_claims(self) -> frozenset[tuple[str, int]]:
        return frozenset(self._queued_claims)

    @property
    def reclaim_lock(self) -> asyncio.Lock:
        return self._reclaim_lock

    @property
    def protected_reclaim_task(self) -> Optional[asyncio.Task[Any]]:
        return self._protected_reclaim_task

    # --- Lifecycle -----------------------------------------------------------

    def begin_attach(self) -> None:
        """Reset for a new attach; readiness stays closed until ``open_queue``."""

        self._queue = None
        self._interrupt_mode = None
        self._interrupt_target_turn_id = None
        self._hard_interrupt_event = asyncio.Event()
        self._reclaim_lock = asyncio.Lock()
        self._queued_claims.clear()

    def open_queue(self) -> asyncio.Queue:
        """Publish the loop's queue once durable child recovery converged."""

        self._queue = asyncio.Queue()
        return self._queue

    def teardown(self) -> None:
        """Drop the queue, claims and interrupt state of the ending session."""

        self._queue = None
        self._interrupt_mode = None
        self._interrupt_target_turn_id = None
        self._hard_interrupt_event = None
        self._queued_claims.clear()

    def wake_parked_wait(self, item: Any) -> None:
        """Wake a parked input wait so it notices a closed admission fence."""

        if self._awaiting_input and self._queue is not None:
            try:
                self._queue.put_nowait(item)
            except asyncio.QueueFull:  # pragma: no cover - production queue unbounded
                pass

    def drain_queue(self) -> int:
        """Discard every locally queued item; durable rows are not touched."""

        drained = 0
        if self._queue is not None:
            while True:
                try:
                    self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                drained += 1
        return drained

    async def publish_executor_input(self, item: dict[str, Any]) -> None:
        """Hand one stateless-executor turn input to the loop."""

        await self._queue.put(item)

    # --- Identity ----------------------------------------------------------

    def pinned_identity(self) -> tuple[str, str, str, str]:
        """Exact pinned (agent, Pod UID, process generation, attach token)."""

        return self._pinned_fields(self._ports.identity())

    @staticmethod
    def _pinned_fields(identity: SessionRuntimeIdentity) -> tuple[str, str, str, str]:
        agent_id = identity.agent_id
        pod_uid = str(identity.pod_uid or "").strip()
        generation = str(identity.process_generation or "").strip()
        attach_token = str(identity.attach_token or "").strip()
        if agent_id is None or not pod_uid or not generation or not attach_token:
            raise DurableInputUnavailable
        return agent_id, pod_uid, generation, attach_token

    def _session_life_matches(
        self, session: Any, thread_id: str, attach_generation: int
    ) -> bool:
        identity = self._ports.identity()
        return bool(
            self._ports.session() is session
            and identity.thread_id == thread_id
            and identity.attach_generation == attach_generation
        )

    def _input_admission_open(self) -> bool:
        return (
            not self._ports.runtime_admission_closed()
            and self._ports.protected_cloud_ready()
        )

    # --- Durable admission and delivery ----------------------------------------

    async def transition_claimed(
        self,
        delivery_id: str,
        claim_generation: int,
        transition: str,
        *,
        turn_number: int | None = None,
        reason: str | None = None,
    ) -> bool:
        session = self._ports.session()
        identity = self._ports.identity()
        thread_id = identity.thread_id
        if session is None or session.postgres_conn is None or thread_id is None:
            return False
        try:
            live_lease = identity.lease
            if live_lease is not None and live_lease.active:
                executor_id = str(live_lease.executor_id or "").strip()
                pod_uid = str(live_lease.pod_uid or "").strip()
                if (
                    str(live_lease.unit_id or "") != str(thread_id)
                    or not executor_id
                    or not pod_uid
                ):
                    return False
                return await session.postgres_conn.transition_stateless_input_delivery(
                    thread_id=thread_id,
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
                self._pinned_fields(identity)
            )
            return await session.postgres_conn.transition_pinned_input_delivery(
                thread_id=thread_id,
                delivery_id=delivery_id,
                agent_id=agent_id,
                pod_uid=pod_uid,
                runtime_generation=runtime_generation,
                session_runtime_generation=str(identity.session_generation or ""),
                runtime_attach_token=runtime_attach_token,
                claim_generation=claim_generation,
                transition=transition,
                turn_number=turn_number,
                reason=reason,
            )
        except Exception as exc:
            self._logger.warning(
                "Durable input %s transition failed (%s)",
                transition,
                type(exc).__name__,
            )
            return False

    async def queue_claimed(self, row: dict[str, Any]) -> bool:
        """Queue one exact durable claim once in this process generation."""

        session = self._ports.session()
        if (
            session is None
            or session.postgres_conn is None
            or self._ports.identity().thread_id is None
        ):
            return False
        queue = self._queue
        if queue is None:
            return False
        delivery_id = str(row["delivery_id"])
        claim_generation = int(row["claim_generation"])
        key = (delivery_id, claim_generation)
        if key in self._queued_claims:
            return False
        if not self._ports.protected_cloud_ready():
            await self.transition_claimed(
                delivery_id,
                claim_generation,
                "deferred",
                reason="protected_cloud_unavailable_before_queue",
            )
            self.schedule_protected_reclaim()
            return False
        session = self._ports.session()
        identity = self._ports.identity()
        agent_id, pod_uid, runtime_generation, runtime_attach_token = (
            self._pinned_fields(identity)
        )
        queued = await session.postgres_conn.mark_pinned_input_delivery_queued(
            thread_id=identity.thread_id,
            delivery_id=delivery_id,
            agent_id=agent_id,
            pod_uid=pod_uid,
            runtime_generation=runtime_generation,
            session_runtime_generation=str(identity.session_generation or ""),
            runtime_attach_token=runtime_attach_token,
            claim_generation=claim_generation,
        )
        if not queued:
            return False
        # Another same-process request may have completed the identical DB CAS
        # while this coroutine awaited it. Re-check at the no-await publication
        # boundary so concurrent HTTP retries still produce one queue item.
        if key in self._queued_claims:
            return False
        if (
            self._ports.runtime_admission_closed()
            or not self._ports.protected_cloud_ready()
        ):
            await self.transition_claimed(
                delivery_id,
                claim_generation,
                "deferred",
                reason=(
                    "runtime_terminating_before_queue"
                    if self._ports.runtime_admission_closed()
                    else "protected_cloud_unavailable_before_queue_publish"
                ),
            )
            if not self._ports.runtime_admission_closed():
                self.schedule_protected_reclaim()
            return False

        # No await between local dedup publication and the unbounded put. A
        # retry in this process observes the set; a process death loses the
        # set and its new runtime generation reclaims the durable queued row.
        self._queued_claims.add(key)
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

    async def _reclaim_pending_locked(self) -> set[tuple[str, int]]:
        """Replay pending input while holding the reclaim lock."""

        session = self._ports.session()
        if (
            session is None
            or session.postgres_conn is None
            or self._ports.identity().thread_id is None
        ):
            return set()
        identity = self._ports.identity()
        agent_id, pod_uid, runtime_generation, runtime_attach_token = (
            self._pinned_fields(identity)
        )
        rows = await session.postgres_conn.claim_pending_pinned_input_deliveries(
            thread_id=identity.thread_id,
            agent_id=agent_id,
            pod_uid=pod_uid,
            runtime_generation=runtime_generation,
            session_runtime_generation=str(identity.session_generation or ""),
            runtime_attach_token=runtime_attach_token,
        )
        queued: set[tuple[str, int]] = set()
        for row in rows:
            if await self.queue_claimed(row):
                queued.add((str(row["delivery_id"]), int(row["claim_generation"])))
        if queued:
            self._logger.info(
                "Reclaimed %d durable persistent input(s) for thread %s",
                len(queued),
                self._ports.identity().thread_id,
            )
        return queued

    async def reclaim_pending(self) -> set[tuple[str, int]]:
        """Attach-time or accept-time replay for persisted unadmitted input."""

        async with self._reclaim_lock:
            return await self._reclaim_pending_locked()

    def schedule_protected_reclaim(self) -> None:
        """Reclaim deferred input once this exact protected runtime heals."""

        session = self._ports.session()
        identity = self._ports.identity()
        if (
            self._ports.runtime_admission_closed()
            or session is None
            or identity.thread_id is None
        ):
            return
        current = self._protected_reclaim_task
        if current is not None and not current.done():
            return
        thread_id = str(identity.thread_id)
        attach_generation = identity.attach_generation

        async def _wait_and_reclaim() -> None:
            try:
                while self._session_life_matches(session, thread_id, attach_generation):
                    if self._ports.runtime_admission_closed():
                        return
                    if self._ports.protected_cloud_ready():
                        await self.reclaim_pending()
                        return
                    await asyncio.sleep(1.0)
            finally:
                if self._protected_reclaim_task is asyncio.current_task():
                    self._protected_reclaim_task = None

        self._protected_reclaim_task = self._ports.track_side_task(
            asyncio.create_task(
                _wait_and_reclaim(),
                name=f"protected-input-reclaim-{thread_id[:12]}",
            )
        )

    async def accept(
        self,
        content: str,
        *,
        role: str = "human",
        delivery_id: str | None = None,
        expected_session_identity_fingerprint: str | None = None,
    ) -> AcceptedInput:
        """Persist an accepted user message, then enqueue it for the loop.

        Returns the durable/local admission outcome. Persisting BEFORE the
        acknowledgement closes the swallowed-input gap
        (session_silent_failure_audit.md #1): the queue is process memory, so
        without the row a mid-turn input vanished from the UI on reload and
        died with the pod. The loop reuses the id when it consumes the item, so
        its own persist is an upsert onto this row (final turn_number), never a
        duplicate.

        ``role`` controls only how the row is PERSISTED; the in-memory message
        stays a ``HumanMessage`` regardless. That split is deliberate, and both
        halves are load-bearing:

        * ``role='event'`` keeps a system-injected notice (a worker job the
          session created has finished) out of the human-bubble family, so the
          transcript does not claim the user said it. It joins the shipped
          non-conversational roles ``summary`` and ``error``, which the cockpit
          already branches on.
        * Keeping the carrier a ``HumanMessage`` is what keeps
          ``_save_turn_ai_messages`` correct — it reconciles a turn by walking
          backwards until it hits one — and avoids introducing a novel
          LangChain type into the graph. A synthetic AIMessage+ToolMessage pair
          (the *transient* injection family) would be the wrong shape: this is
          a one-time fact that must survive compaction.
        """
        if self._ports.runtime_admission_closed():
            raise TerminationAdmissionClosed
        if not self._ports.protected_cloud_ready():
            raise ProtectedCloudUnavailable
        if (
            expected_session_identity_fingerprint is not None
            and self._ports.identity_fingerprint()
            != expected_session_identity_fingerprint
        ):
            raise SessionIdentityMismatch

        parsed_delivery_id = UUID(str(delivery_id)) if delivery_id else uuid4()
        injected = role != "human"

        session = self._ports.session()
        identity = self._ports.identity()
        if (
            session is None
            or session.postgres_conn is None
            or identity.thread_id is None
        ):
            raise DurableInputUnavailable
        try:
            agent_id, pod_uid, runtime_generation, runtime_attach_token = (
                self._pinned_fields(identity)
            )
            row = await asyncio.wait_for(
                session.postgres_conn.persist_pinned_input_delivery(
                    thread_id=identity.thread_id,
                    delivery_id=str(parsed_delivery_id),
                    role=role,
                    content=content,
                    source="officer_wake" if injected else "direct_human",
                    # Numbers only a row this call creates; a retry of an
                    # admitted/settled identity resolves to its receipt.
                    turn_number_hint=session.turn_count + 1,
                    agent_id=agent_id,
                    pod_uid=pod_uid,
                    runtime_generation=runtime_generation,
                    session_runtime_generation=str(identity.session_generation or ""),
                    runtime_attach_token=runtime_attach_token,
                ),
                timeout=5.0,
            )
        except Exception as exc:
            self._logger.warning(
                "Durable input persist/claim failed (%s)", type(exc).__name__
            )
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

        if not self._ports.protected_cloud_ready():
            deferred = await self.transition_claimed(
                str(parsed_delivery_id),
                claim_generation,
                "deferred",
                reason="protected_cloud_unavailable_after_persist",
            )
            if not deferred:
                raise DurableInputUnavailable
            self.schedule_protected_reclaim()
            return AcceptedInput(
                message_id=str(row["message_id"]),
                delivery_id=str(parsed_delivery_id),
                delivery_state="deferred",
                claim_generation=claim_generation,
                enqueued=False,
                duplicate=duplicate,
                deferred=True,
            )

        if self._ports.runtime_admission_closed():
            await self.transition_claimed(
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
        already_queued = key in self._queued_claims
        # Claim the whole durable inbox in transcript order, including this
        # row. That both preserves ordering and gives a same-process runtime a
        # bounded way to recover inputs deferred by a transient authorization
        # failure. A retry of this row observes the local claim set and cannot
        # publish twice.
        newly_queued = await self.reclaim_pending()
        queued_here = key in self._queued_claims
        enqueued = key in newly_queued and not already_queued
        deferred = not queued_here and self._ports.runtime_admission_closed()
        if injected and enqueued:
            # Make the injection visible in a live cockpit. Nothing else would:
            # /api/input broadcasts nothing and no frame carries user-message
            # content (the cockpit builds a user turn from its own optimistic
            # dispatch on send, or from a history reload). Without this the
            # user watches a turn start and stream a reply with no visible
            # prompt — the agent apparently talking to itself. Rides the normal
            # journal broadcast, so it reaches socket subscribers and the
            # thread_events log (hence SSE) alike.
            self._ports.broadcast(
                "session.event",
                {
                    "content": str(row["content"]),
                    "id": str(row["message_id"]),
                    "role": role,
                },
            )
        # Injected input never titles the thread: a wake landing in a young
        # session would retitle the whole thread after the job-completion text.
        if not injected:
            self._ports.human_input_accepted(content)
        return AcceptedInput(
            message_id=str(row["message_id"]),
            delivery_id=str(parsed_delivery_id),
            delivery_state="deferred"
            if deferred
            else "queued"
            if queued_here
            else state,
            claim_generation=claim_generation,
            enqueued=enqueued,
            duplicate=duplicate,
            deferred=deferred,
        )

    # --- Loop delivery callbacks ---------------------------------------------

    async def admit_delivery(
        self, delivery_id: str, claim_generation: int, turn_number: int
    ) -> bool | None:
        """Cross the durable execution boundary immediately before model spend."""

        if not self._input_admission_open():
            if not self._ports.runtime_admission_closed():
                self.schedule_protected_reclaim()
            return False
        admitted = await self.transition_claimed(
            delivery_id,
            claim_generation,
            "admitted",
            turn_number=turn_number,
        )
        # Close the in-process race as tightly as possible. If the sentinel
        # became visible while the CAS awaited Postgres, roll the not-yet-used
        # admission back to retryable before returning to the loop. No provider
        # call exists between these two statements.
        if admitted and not self._input_admission_open():
            deferred = await self.transition_claimed(
                delivery_id,
                claim_generation,
                "unadmit",
                reason=(
                    "runtime_terminating_before_provider"
                    if self._ports.runtime_admission_closed()
                    else "protected_cloud_unavailable_before_provider"
                ),
            )
            if deferred:
                self._queued_claims.discard((delivery_id, claim_generation))
                if not self._ports.runtime_admission_closed():
                    self.schedule_protected_reclaim()
                return None
            return False
        return admitted

    async def defer_delivery(
        self, delivery_id: str, claim_generation: int, reason: str
    ) -> bool:
        deferred = await self.transition_claimed(
            delivery_id,
            claim_generation,
            "deferred",
            reason=reason,
        )
        if deferred:
            self._queued_claims.discard((delivery_id, claim_generation))
            if (
                not self._ports.runtime_admission_closed()
                and not self._ports.protected_cloud_ready()
            ):
                self.schedule_protected_reclaim()
        return deferred

    async def cancel_delivery(
        self,
        delivery_id: str,
        claim_generation: int,
        turn_number: int,
        reason: str,
    ) -> bool:
        if not self._ports.cancellation_enabled():
            self._logger.warning(
                "Pinned input cancellation writer is disabled; halting before provider"
            )
            return False
        cancelled = await self.transition_claimed(
            delivery_id,
            claim_generation,
            "cancelled",
            turn_number=turn_number,
            reason=reason,
        )
        if cancelled:
            self._queued_claims.discard((delivery_id, claim_generation))
        return cancelled

    async def defer_and_requeue_delivery(
        self,
        delivery_id: str,
        claim_generation: int,
        content: str,
        role: str,
        source: str,
        reason: str,
    ) -> dict[str, Any] | None:
        """Atomically defer and priority-reclaim one stopped server wake.

        The shared lock prevents concurrent input acceptance from reclaiming
        the deferred row into the ordinary FIFO in the gap between its
        generation CAS and priority publication. Claiming the exact stable
        identity mints a fresh generation; the returned item stays in the
        graph's one-item priority slot.
        """

        if source != "officer_wake" or role == "human":
            return None
        session = self._ports.session()
        if (
            self._ports.runtime_admission_closed()
            or session is None
            or session.postgres_conn is None
            or self._ports.identity().thread_id is None
        ):
            return None
        async with self._reclaim_lock:
            try:
                deferred = await self.transition_claimed(
                    delivery_id,
                    claim_generation,
                    "deferred",
                    reason=reason,
                )
                if not deferred:
                    return None
                self._queued_claims.discard((delivery_id, claim_generation))
                session = self._ports.session()
                identity = self._ports.identity()
                agent_id, pod_uid, runtime_generation, runtime_attach_token = (
                    self._pinned_fields(identity)
                )
                row = await asyncio.wait_for(
                    session.postgres_conn.persist_pinned_input_delivery(
                        thread_id=identity.thread_id,
                        delivery_id=delivery_id,
                        role=role,
                        content=content,
                        source=source,
                        turn_number_hint=session.turn_count + 1,
                        agent_id=agent_id,
                        pod_uid=pod_uid,
                        runtime_generation=runtime_generation,
                        session_runtime_generation=str(
                            identity.session_generation or ""
                        ),
                        runtime_attach_token=runtime_attach_token,
                    ),
                    timeout=5.0,
                )
                next_generation = int(row["claim_generation"])
                if str(row["state"]) not in {"owned", "queued"}:
                    return None
                session = self._ports.session()
                identity = self._ports.identity()
                queued = await session.postgres_conn.mark_pinned_input_delivery_queued(
                    thread_id=identity.thread_id,
                    delivery_id=delivery_id,
                    agent_id=agent_id,
                    pod_uid=pod_uid,
                    runtime_generation=runtime_generation,
                    session_runtime_generation=str(identity.session_generation or ""),
                    runtime_attach_token=runtime_attach_token,
                    claim_generation=next_generation,
                )
                if not queued:
                    return None
                if self._ports.runtime_admission_closed():
                    await self.transition_claimed(
                        delivery_id,
                        next_generation,
                        "deferred",
                        reason="runtime_terminating_before_priority_requeue",
                    )
                    return None
            except Exception as exc:
                self._logger.warning(
                    "Deferred wake priority reclaim failed (%s)", type(exc).__name__
                )
                return None

            key = (delivery_id, next_generation)
            if key in self._queued_claims:
                return None
            self._queued_claims.add(key)
            return {
                "content": str(row["content"]),
                "id": str(row["message_id"]),
                "role": str(row["role"]),
                "source": source,
                "delivery_id": delivery_id,
                "claim_generation": next_generation,
            }

    async def settle_delivery(self, delivery_id: str, claim_generation: int) -> bool:
        settled = await self.transition_claimed(
            delivery_id,
            claim_generation,
            "settled",
        )
        if settled:
            self._queued_claims.discard((delivery_id, claim_generation))
        return settled

    async def wait_for_input(
        self,
        queue: asyncio.Queue,
        *,
        timeout: float | None = None,
    ) -> Any:
        """Wait while polling the durable pinned inbox for correctness.

        Orchestrator-side input no longer needs a Pod-IP POST: it commits a
        stable delivery and this exact reciprocal runtime claims it. LISTEN
        would only be a latency optimization; the bounded poll means a lost
        notification, a pod replacement, or an IP-reused foreign process cannot
        lose or consume work.
        """

        if self._ports.stateless_mode():
            if timeout is None:
                return await queue.get()
            return await asyncio.wait_for(queue.get(), timeout=timeout)

        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout if timeout is not None else None
        while True:
            if self._ports.runtime_admission_closed():
                # Leave every claimed/queued row for the successor. Normal
                # teardown cancels this wait after preStop observes the park.
                await asyncio.Future()
            try:
                await self.reclaim_pending()
            except asyncio.CancelledError:
                raise
            except Exception:
                self._logger.warning(
                    "Durable pinned input poll failed for thread %s; retrying",
                    self._ports.identity().thread_id,
                    exc_info=True,
                )
            if not queue.empty():
                return await queue.get()

            remaining = None if deadline is None else deadline - loop.time()
            if remaining is not None and remaining <= 0:
                raise asyncio.TimeoutError
            wait_for = self._poll_seconds
            if remaining is not None:
                wait_for = min(wait_for, remaining)
            try:
                return await asyncio.wait_for(queue.get(), timeout=wait_for)
            except asyncio.TimeoutError:
                if deadline is not None and loop.time() >= deadline:
                    raise

    async def get_user_input(self) -> Any:
        """The loop's input callback: wait for the next input.

        Admission closed parks without consuming durable work (the exact
        successor reclaims it). Otherwise the loop's turn-boundary step runs,
        then the parked window covers exactly the wait, including a timeout
        that the plan maps to a backstop wake or an idle-timeout error.
        """

        queue = self._queue
        if queue is None:
            # Attach always publishes the queue before the loop can start. None
            # here means the session is being torn down: unwind loudly.
            raise RuntimeError("input queue not initialized — session torn down?")

        if self._ports.runtime_admission_closed():
            # Do not consume already-durable queued work. The exact successor
            # reclaims it from thread_input_deliveries; transcript restore
            # excludes unadmitted rows because conversation context is not an
            # inbox. This wait is cancelled by normal process shutdown after
            # preStop observes the exact parked boundary.
            self._awaiting_input = True
            try:
                await asyncio.Future()
            finally:
                self._awaiting_input = False

        plan = await self._ports.begin_input_wait()

        # Parked window for the drain-suspend gate: exactly the span where
        # this coroutine is blocked on the queue. The finally also covers
        # loop-task cancellation and a raising timeout action.
        self._awaiting_input = True
        try:
            if plan.timeout_seconds is None:
                return await self.wait_for_input(queue)
            try:
                return await self.wait_for_input(queue, timeout=plan.timeout_seconds)
            except asyncio.TimeoutError:
                if plan.on_timeout is None:
                    raise
                return plan.on_timeout()
        finally:
            self._awaiting_input = False

    # --- Interrupts ------------------------------------------------------------

    def clear_interrupt(self, *, target_turn_id: int | None = None) -> bool:
        """Clear one pending interrupt without crossing a turn boundary.

        When ``target_turn_id`` is supplied, a newer turn's pending interrupt
        is left untouched. Mode, target and the hard-event signal are one
        logical value and are always cleared together.
        """

        if target_turn_id is not None and self._interrupt_target_turn_id != int(
            target_turn_id
        ):
            return False
        had_interrupt = (
            self._interrupt_mode is not None
            or self._interrupt_target_turn_id is not None
            or bool(self._hard_interrupt_event and self._hard_interrupt_event.is_set())
        )
        self._interrupt_mode = None
        self._interrupt_target_turn_id = None
        if self._hard_interrupt_event is not None:
            self._hard_interrupt_event.clear()
        return had_interrupt

    def signal_interrupt_for_turn(
        self,
        target_turn_id: int,
        *,
        force_graceful: bool = False,
    ) -> Optional[str]:
        """Synchronously signal RAM iff ``target_turn_id`` is still active.

        The check and mutation have no await between them. That is the local
        half of the exact-target fence: the database protects the lease
        generation, while this method prevents a late request for turn N from
        interrupting turn N+1 after an in-process transition.
        """

        session = self._ports.session()
        if (
            session is None
            or not self._ports.turn_open()
            or int(session.turn_count) != int(target_turn_id)
        ):
            return None
        mode = "graceful" if (self._ports.tool_inflight() or force_graceful) else "hard"
        self._interrupt_mode = mode
        self._interrupt_target_turn_id = int(target_turn_id)
        # Hard interrupt with no tool in flight ⇒ the loop is parked in an LLM /
        # auxiliary await; signal it to cancel that await immediately rather
        # than waiting for the next cooperative check_interrupt poll.
        if mode == "hard" and self._hard_interrupt_event is not None:
            self._hard_interrupt_event.set()
        return mode

    def check_interrupt(self) -> Optional[str]:
        """One-shot read of the interrupt flag. Returns the mode or None.

        Returns:
            None when no interrupt is pending.
            "hard" to cancel the in-flight LLM stream immediately and drop the
                partial AIMessage (set when interrupt fires with no tool active).
            "graceful" to stop after the current tool call completes (set when
                interrupt fires with a tool mid-`ainvoke`).

        Consumed by the persistent loop at three checkpoints. A `bool(result)`
        check preserves the legacy "any interrupt → stop" semantics for sites
        that don't yet branch on the mode.
        """
        mode = self._interrupt_mode
        target_turn_id = self._interrupt_target_turn_id
        if mode is None:
            if target_turn_id is not None:
                self.clear_interrupt()
            return None
        session = self._ports.session()
        current_turn_id = int(session.turn_count) if session is not None else None
        if target_turn_id is None or target_turn_id != current_turn_id:
            self._logger.warning(
                "Discarding unscoped/stale interrupt "
                "(target_turn=%s current_turn=%s mode=%s)",
                target_turn_id,
                current_turn_id,
                mode,
            )
            self.clear_interrupt()
            return None
        self.clear_interrupt(target_turn_id=target_turn_id)
        return mode
