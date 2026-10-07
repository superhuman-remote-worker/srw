"""Exact termination, retirement admission and runtime quiescence ownership."""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, NoReturn, Optional

from agent.api.lease_context import LeaseLostError
from agent.api.orchestrator_client import SessionEnded, SessionGrantDenied
from agent.api.session_contract import EventJournalUnavailable, WorkspaceNotReady
from agent.api.session_identity import canonical_runtime_generation

_DEREGISTER_ON_EXIT_TIMEOUT_S = 5.0

_EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS = (0.0, 0.25, 1.0, 3.0)

_DEDICATED_SELF_END_REASONS = frozenset(
    {"archive", "idle_timeout", "loop_complete", "loop_crash"}
)


@dataclass(frozen=True, slots=True)
class SessionTerminationPorts:
    """Named call-time collaborators; no application or universal context."""

    announced_permissions: Callable[..., Any]
    attach: Callable[..., Any]
    await_pending_cloud_push: Callable[..., Any]
    background_push_owns: Callable[..., Any]
    canvas_control: Callable[..., Any]
    close_pinned_control_inbox: Callable[..., Any]
    control_owner_agent_id: Callable[..., Any]
    #: True only while the open turn waits on nothing but a delegation batch
    #: handed to the successor (parallel_subagents.md §14.2 P4).
    delegation_batch_left_for_successor: Callable[..., Any]
    event_writer: Callable[..., Any]
    handle_idle_archive: Callable[..., Any]
    heartbeat_task: Callable[..., Any]
    identity: Callable[..., Any]
    idle_timeout_error: type[Exception]
    memory_unavailable_error: type[Exception]
    workspace_unavailable_error: type[Exception]
    input_runtime: Callable[..., Any]
    #: Hands a pinned session's running delegation batch to its successor at
    #: the termination fence's first activation, a platform shutdown (P4).
    leave_delegation_batch_for_successor: Callable[..., Any]
    loop_on_error: Callable[..., Any]
    loop_task: Callable[..., Any]
    officer_config: Callable[..., Any]
    orchestrator_client: Callable[..., Any]
    permission_gates: Callable[..., Any]
    publish_active_permission: Callable[..., Any]
    publish_draft_title: Callable[..., Any]
    publish_event_writer: Callable[..., Any]
    publish_loop_task: Callable[..., Any]
    publish_runtime_authorization: Callable[..., Any]
    publish_session: Callable[..., Any]
    registered_pinned_agent_id: Callable[..., Any]
    reset_journal_cursor: Callable[..., Any]
    reset_turn_state: Callable[..., Any]
    retire_announced_permissions: Callable[..., Any]
    session: Callable[..., Any]
    session_type: type
    set_cloud_sync_retry_pending: Callable[..., Any]
    set_pinned_control_admission: Callable[..., Any]
    stateless_mode: Callable[..., Any]
    stop_control_watcher: Callable[..., Any]
    stop_interrupt_watcher: Callable[..., Any]
    subscribers: Callable[..., Any]
    tool_inflight: Callable[..., Any]
    turn_event_open: Callable[..., Any]
    update_thread_status: Callable[..., Any]


class SessionTerminationCoordinator:
    """One captured life, one shielded cleanup task and its tracked work."""

    def __init__(
        self,
        ports: SessionTerminationPorts,
        *,
        logger: logging.Logger,
        termination_queue_sentinel: Any,
    ) -> None:
        self._ports = ports
        self._logger = logger
        self.retirement_admission_identity = None
        self.retirement_admission_disposition = None
        self.retirement_admission_token = None
        self.retirement_admission_permanent = None
        self.pending_drain_suspend = None
        self.pending_drain_suspend_retry_task = None
        self.pending_exit_task = None
        self.drain_intent_handled = False
        self.drain_deferred_logged = False
        self.termination_admission_fenced = False
        self.termination_fence_reason = None
        self.ws_connected_event = None
        self.native_first_use_identity = None
        self.boot_first_use_closed_identity = None
        self.watchdog_tasks = []
        self.terminating = False
        self.termination_task = None
        self.sessions_served = 0
        self.max_sessions_per_process = int(
            os.environ.get("MAX_SESSIONS_PER_PROCESS", "0")
        )
        self.session_side_tasks = set()
        self.loop_completion_tasks: set[asyncio.Task[Any]] = set()
        self.termination_sentinel_path = Path("/tmp/srw-persistent-terminating")
        self.termination_queue_sentinel = termination_queue_sentinel
        self.session_boot_ws_timeout_s = int(
            os.environ.get("SESSION_BOOT_WS_TIMEOUT_S", "600")
        )
        self.thread_status_poll_s = int(os.environ.get("THREAD_STATUS_POLL_S", "60"))

    def reset_retirement_admission_mirror(self) -> None:
        """Drop the local ``ending`` mirror when the attached identity changes."""

        self.retirement_admission_identity = None
        self.retirement_admission_disposition = None
        self.retirement_admission_token = None
        self.retirement_admission_permanent = None
        self.native_first_use_identity = None
        self.boot_first_use_closed_identity = None

    def native_life_identity(self) -> tuple[str, ...] | None:
        """Read the complete attached life, including registered process epoch."""

        identity = self._ports.identity().snapshot()
        client = self._ports.orchestrator_client()
        parts = (
            identity.thread_id,
            identity.session_generation,
            identity.agent_id,
            identity.attach_token,
            identity.pod_uid,
            getattr(client, "dispatch_process_generation", None),
        )
        if not all(isinstance(part, str) and part for part in parts):
            return None
        return parts

    def note_native_first_use(self, identity: tuple[str, ...]) -> str | None:
        """Latch first use synchronously after the caller holds runtime authority.

        There must be no await between the caller's final local check and this
        method: the boot timeout and all local retirement fences run on this
        same event loop.
        """

        if (
            identity != self.native_life_identity()
            or self._ports.session() is None
            or self._ports.officer_config() is not None
            or self._ports.stateless_mode()
            or self.ws_connected_event is None
            or self.runtime_admission_closed()
            or self.terminating
            or self.boot_first_use_closed_identity == identity
        ):
            return None
        if self.native_first_use_identity == identity:
            return "already_observed"
        self.native_first_use_identity = identity
        self.ws_connected_event.set()
        return "accepted"

    def track_session_side_task(self, task: asyncio.Task[Any]) -> asyncio.Task[Any]:
        self.session_side_tasks.add(task)
        task.add_done_callback(self.session_side_tasks.discard)
        return task

    async def quiesce_session_side_tasks(self) -> None:
        """Cancel and join title/stage tasks before process-global identity reuse."""

        pending = {
            task
            for task in self.session_side_tasks
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
            raise RuntimeError(
                f"{len(pending)} session side task(s) ignored cancellation"
            )

    def terminal_retirement_disposition(self) -> str:
        """Choose the immutable outcome before beginning pinned retirement."""

        identity = self._ports.identity().retirement_identity()
        if (
            identity is not None
            and self.retirement_admission_identity == identity
            and self.retirement_admission_disposition in {"ended", "suspended"}
        ):
            return self.retirement_admission_disposition
        return "suspended" if self._ports.officer_config() is not None else "ended"

    def termination_admission_closed(self) -> bool:
        """Return the earliest process-visible Kubernetes termination signal.

        ``deletionTimestamp`` itself lives outside the container, so this function
        deliberately does not claim to observe the API-server mutation atomically.
        The preStop shell creates the sentinel before Python/HTTP work; the route
        then latches the in-process boolean.  Either one closes admission.
        """

        if self.termination_admission_fenced:
            return True
        try:
            return self.termination_sentinel_path.exists()
        except OSError:
            # A broken pod-local fence path is not a reason to spend through a
            # termination signal.  The normal /tmp path is always stat-able.
            return True

    def retirement_admission_closed(self) -> bool:
        """True only for the exact attached life whose End has begun."""

        identity = self._ports.identity().retirement_identity()
        return identity is not None and self.retirement_admission_identity == identity

    def runtime_admission_closed(self) -> bool:
        """Combined local fence for user/control/provider work.

        Kubernetes termination is process-scoped. An owner End is session-scoped
        so a pool agent can safely accept a later exact attach after cleanup.
        """

        return self.termination_admission_closed() or self.retirement_admission_closed()

    def activate_termination_admission_fence(self, source: str) -> bool:
        """Latch the no-new-turn fence and wake an idle queue waiter.

        Returns True only for the first process-local transition.  Repeated preStop
        callbacks/signals are idempotent and never consume a queued user/event row.
        """

        first = not self.termination_admission_fenced
        self.termination_admission_fenced = True
        if self.termination_fence_reason is None:
            self.termination_fence_reason = str(source or "termination")[:80]
        if first:
            self._logger.warning(
                "Persistent runtime admission fenced for termination (source=%s, "
                "turn_open=%s, tool_inflight=%s)",
                self.termination_fence_reason,
                self._ports.turn_event_open(),
                self._ports.tool_inflight(),
            )
            # Only the platform sets this fence (a person's End goes through
            # retirement admission), so a running delegation batch is handed
            # to the successor rather than answered with interrupted results.
            # No await since the latch: no child saw the fence without it.
            try:
                self._ports.leave_delegation_batch_for_successor()
            except Exception:
                self._logger.warning(
                    "Delegation batch could not be handed to the successor",
                    exc_info=True,
                )
        # queue.get() otherwise has no reason to wake and notice the file/flag.
        # The sentinel is filtered by the loop and is never persisted.
        self._ports.input_runtime().wake_parked_wait(self.termination_queue_sentinel)
        return first

    def termination_quiescent(self) -> bool:
        """True after the current turn's complete settlement boundary.

        A turn whose delegation batch was handed to the successor at the fence
        is such a boundary although it stays open: every call of it is held,
        no child runs, and the termination's cancellation ends it writing
        nothing.
        """

        handed_over = (
            self.termination_admission_closed()
            and self._ports.delegation_batch_left_for_successor() is True
        )
        if not handed_over and (
            self._ports.tool_inflight() or self._ports.turn_event_open()
        ):
            return False
        session = self._ports.session()
        if session is not None:
            auxiliary = getattr(session, "auxiliary_llm", None)
            if int(getattr(auxiliary, "provider_calls_inflight", 0) or 0) > 0:
                return False
            memory = getattr(session, "memory_service", None)
            if int(getattr(memory, "background_tasks_inflight", 0) or 0) > 0:
                return False
        if handed_over:
            return True
        task = self._ports.loop_task()
        return task is None or task.done() or self._ports.input_runtime().awaiting_input

    async def wait_for_termination_quiescence(self, timeout_seconds: float) -> bool:
        """Wait within preStop's grace budget; never cancel an active tool."""

        deadline = asyncio.get_running_loop().time() + max(0.0, timeout_seconds)
        while not self.termination_quiescent():
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False
            await asyncio.sleep(min(0.05, remaining))
        return True

    async def handle_heartbeat_intents(self, response: dict[str, Any]) -> None:
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
        if self.drain_intent_handled:
            return
        intents = response.get("intents") or {}
        if not isinstance(intents, dict):
            return
        if not intents.get("should_drain"):
            return
        reason = intents.get("drain_reason", "unspecified")

        pending = self.pending_drain_suspend
        if pending is not None and pending.get("locally_quiesced") is True:
            # Begin makes ordinary heartbeats authority-refused, so they cannot be
            # the retry clock. Rejoin/start the one tracked exact settlement task;
            # it uses capped backoff and keeps this runtime non-ready throughout.
            self.drain_intent_handled = True
            await asyncio.shield(self.start_pending_exact_drain_suspend_retry())
            return

        if self._ports.session() is not None and not self.session_parked():
            if not self.drain_deferred_logged:
                self._logger.info(
                    "Drain intent received from orchestrator (reason=%s) but a "
                    "turn is in flight — deferring until the loop parks",
                    reason,
                )
                self.drain_deferred_logged = True
            return

        self.drain_intent_handled = True
        if self._ports.session() is None:
            self._logger.info(
                "Drain intent received from orchestrator (reason=%s) — no session "
                "attached, exiting",
                reason,
            )
            self.schedule_exit(delay=1.0)
            return

        self._logger.info(
            "Drain intent received from orchestrator (reason=%s) — suspending "
            "session and exiting",
            reason,
        )
        await self.drain_suspend_session()

    def session_parked(self) -> bool:
        """True when the persistent loop is parked waiting for user input.

        Parked = blocked in the input owner's queue wait with nothing
        queued and no tool call in flight. Anything else counts as an active
        turn and must not be torn down out-of-band.
        """
        if (
            not self._ports.input_runtime().awaiting_input
            or self._ports.tool_inflight()
        ):
            return False
        if self.runtime_admission_closed():
            # Queue contents remain durable/deferred for the replacement.  They do
            # not make the predecessor active once termination admission is closed.
            return True
        queue = self._ports.input_runtime().queue
        return queue is None or queue.empty()

    async def drain_suspend_session(self) -> None:
        """Drain an attached idle session via clean suspend instead of kill.

        Converges on the attention-sleep terminal state — thread ``suspended``,
        workspace snapshotted to S3, both pods gone — so the next user input
        resumes through the existing suspended-restore path instead of racing a
        half-deleted workspace pod (the 409→503 "session ended" failure this
        replaces).
        """

        thread_id = self._ports.identity().thread_id
        runtime_agent_id = self._ports.registered_pinned_agent_id()
        runtime_generation = self._ports.identity().session_generation
        runtime_attach_token = self._ports.identity().attach_token
        runtime_session = self._ports.session()

        # Suspend is a terminal retirement disposition too. Close/drain durable
        # controls and install the exact retirement token before touching the
        # loop, workspace, mounts, or transport. A failed begin leaves the current
        # session intact so a later heartbeat can retry safely.
        if not await self.begin_retirement(
            pinned_agent_id=runtime_agent_id,
            retirement_disposition="suspended",
        ):
            self.drain_intent_handled = False
            self._logger.warning(
                "Drain-suspend could not begin exact retirement; local teardown "
                "suppressed (thread=%s)",
                thread_id,
            )
            return

        if runtime_generation is not None and runtime_attach_token is not None:
            self.pending_drain_suspend = {
                "thread_id": thread_id,
                "agent_id": runtime_agent_id,
                "session_runtime_generation": runtime_generation,
                "session_runtime_attach_token": runtime_attach_token,
                "session_runtime_retirement_token": self.retirement_admission_token,
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
            await self.terminate(
                "drain",
                mark_thread=False,
                preserve_shell=False,
            )
        except Exception as e:
            self._logger.warning(f"Session teardown during drain-suspend failed: {e}")
            # The exact ``ending`` token remains the durable authority fence. Do
            # not ask the orchestrator to snapshot/delete/settle while local
            # producers, the ordered writer, or mount managers may still be live.
            # Leaving this unhandled lets the same runtime retry quiescence; the
            # retirement reconciler is the cross-process backstop.
            self.drain_intent_handled = False
            return

        if self.pending_drain_suspend is not None:
            self.pending_drain_suspend["locally_quiesced"] = True
            self.pending_drain_suspend["local_quiescence_protocol"] = str(
                getattr(runtime_session, "local_quiescence_protocol", "") or ""
            )
            await asyncio.shield(self.start_pending_exact_drain_suspend_retry())
            return

        suspended = False
        if self._ports.orchestrator_client() and thread_id:
            try:
                suspended = await self._ports.orchestrator_client().suspend_thread(
                    thread_id,
                    pinned_agent_id=runtime_agent_id,
                    session_runtime_generation=runtime_generation,
                    session_runtime_attach_token=runtime_attach_token,
                )
            except Exception as e:
                self._logger.warning(f"Drain-suspend request failed: {e}")
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
            self._logger.warning(
                "Drain-suspend unavailable for thread %s — falling back to "
                "legacy ended detach",
                thread_id,
            )
            if self._ports.orchestrator_client():
                try:
                    await self._ports.orchestrator_client().update_thread_status(
                        thread_id,
                        "ended",
                        pinned_agent_id=runtime_agent_id,
                        session_runtime_generation=runtime_generation,
                        session_runtime_attach_token=runtime_attach_token,
                    )
                except Exception as e:
                    self._logger.warning(f"Fallback ended write failed: {e}")
        self.schedule_exit(delay=1.0)

    def start_pending_exact_drain_suspend_retry(self) -> asyncio.Task[None]:
        """Own one exact post-quiescence suspend retry loop."""

        task = self.pending_drain_suspend_retry_task
        if task is not None and not task.done():
            return task
        task = asyncio.create_task(
            self.retry_pending_exact_drain_suspend(),
            name="exact-drain-suspend-settlement",
        )
        self.pending_drain_suspend_retry_task = task
        return task

    async def retry_pending_exact_drain_suspend(self) -> None:
        """Retry/reconcile exact suspend without heartbeat or cleanup replay."""

        pending = self.pending_drain_suspend
        if pending is None or pending.get("locally_quiesced") is not True:
            self.drain_intent_handled = False
            return
        attempt = 0
        try:
            while self.pending_drain_suspend is pending:
                client = self._ports.orchestrator_client()
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
                        self._logger.warning(
                            "Exact drain-suspend retry failed: %s",
                            type(exc).__name__,
                        )
                if not suspended and client is not None:
                    outcome_reader = getattr(
                        client, "get_thread_retirement_outcome", None
                    )
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
                    self.pending_drain_suspend = None
                    self.drain_intent_handled = True
                    self.schedule_exit(delay=1.0)
                    return
                self.drain_intent_handled = True
                delay = _EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS[
                    min(attempt + 1, len(_EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS) - 1)
                ]
                self._logger.warning(
                    "Exact drain-suspend remains pending for thread %s; process "
                    "kept nonclaimable for retry",
                    pending["thread_id"],
                )
                attempt += 1
                await asyncio.sleep(delay)
        finally:
            if self.pending_drain_suspend_retry_task is asyncio.current_task():
                self.pending_drain_suspend_retry_task = None

    async def deregister_before_exit(self) -> None:
        """Best-effort deregistration ahead of os._exit.

        os._exit bypasses the lifespan shutdown that normally deregisters
        (the startup-failure ``_exit_*`` helpers already deregister inline),
        so without this every clean exit leaves an agents row that the
        orchestrator's 3-minute heartbeat sweep flips to offline and reports
        as a fleet:agents_offline corpse. Bounded and non-raising — a slow or
        failed deregister must never hold up or abort the exit (the
        stale-agent sweep stays the backstop, exactly as for crashes).
        """
        client = self._ports.orchestrator_client()
        if client is None:
            return
        client.stop_heartbeat()
        hb = self._ports.heartbeat_task()
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
            self._logger.warning(f"Best-effort deregister before exit failed: {e}")

    def schedule_exit(self, delay: float = 1.0) -> None:
        """Schedule process exit after a short delay (allows final I/O to flush)."""

        if self.pending_exit_task and not self.pending_exit_task.done():
            self.pending_exit_task.cancel()

        async def _exit():
            await asyncio.sleep(delay)
            await self.deregister_before_exit()
            self._logger.info("Session complete — exiting process")
            os._exit(0)

        self.pending_exit_task = asyncio.create_task(_exit())

    async def boot_ws_watchdog(self, timeout_s: int) -> None:
        """Exit if no client connects or queues human input within ``timeout_s``.

        A persistent agent that boots, attaches to a thread, then never receives
        a WebSocket or accepted human input has no other way to know it's been
        abandoned (e.g. user navigated away during creation). Without this watchdog
        the pod sits
        forever heartbeating and holding a slot. The orchestrator reconciler
        catches this too, but only after a 60s+ delay; this watchdog kills
        locally on the configured cadence.

        Officer sessions are exempt: they are headless BY DESIGN — no browser
        ever attaches, so "no WS yet" is their normal steady state, not
        abandonment (found by the S3 k3d smoke: every officer died exactly
        600s after boot and had to be respawned by the orchestrator watchdog).
        The officer watchdog owns their lifecycle end to end.
        """
        if self._ports.officer_config() is not None:
            return
        if self.ws_connected_event is None:
            return
        try:
            await asyncio.wait_for(self.ws_connected_event.wait(), timeout=timeout_s)
            return  # A client used this runtime — normal lifecycle takes over.
        except asyncio.TimeoutError:
            identity = self.native_life_identity()
            if identity is not None and self.native_first_use_identity == identity:
                return
            # Close native admission before the first await in termination.
            # A later notice cannot resurrect a life after timeout won.
            self.boot_first_use_closed_identity = identity
            self._logger.warning(
                "No WebSocket connection or queued human input within %ds for thread %s — "
                "exiting (likely abandoned during creation).",
                timeout_s,
                self._ports.identity().thread_id,
            )
        try:
            await self.terminate("boot_ws_timeout")
        except Exception as e:
            self._logger.warning(f"Detach during boot-WS timeout failed: {e}")
            # Exact pinned teardown may still have a writer, workspace process, or
            # unacknowledged retirement receipt.  Keep the runtime fenced and let
            # its tracked retry/reconciler converge; exiting would discard the
            # only truthful local-quiescence owner.
            return
        self.schedule_exit(delay=1.0)

    async def thread_status_watchdog(self, poll_s: int) -> None:
        """Exit if the bound thread transitions to a terminal state out-of-band.

        The orchestrator's stale_agent_detector can flip a thread to 'ended'
        via ``mark_orphaned_threads_ended`` or release the binding via
        ``mark_stuck_session_agents_ready`` (PR 1). When that happens this pod
        is orphaned — no work to do, holding a slot.

        'awaiting_user' is the eager-mode transient idle state set by this same
        agent's loop on natural pause with no subscribers (Phase 5,
        ``_begin_loop_input_wait``). It is NOT a terminal state — the orchestrator's
        attention-sleep watchdog owns the eventual ``awaiting_user → suspended``
        transition and we mustn't pre-empt it from here, or we kill the very
        untethered-survival behaviour Phase 1 + Phase 5 were built to enable.

        'suspended' means the orchestrator has already snapshotted + deleted the
        workspace pod — at that point we're a stranded agent with no workspace,
        so we exit.
        """

        bound_thread_id = self._ports.identity().thread_id
        bound_runtime_generation = self._ports.identity().session_generation
        bound_runtime_attach_token = self._ports.identity().attach_token
        runtime_generation_required = self._ports.identity().runtime_contract
        while True:
            try:
                await asyncio.sleep(poll_s)
            except asyncio.CancelledError:
                raise
            if not self._ports.orchestrator_client() or not bound_thread_id:
                continue
            if (
                self._ports.identity().thread_id != bound_thread_id
                or self._ports.identity().session_generation != bound_runtime_generation
                or self._ports.identity().attach_token != bound_runtime_attach_token
            ):
                return
            try:
                lifecycle = (
                    await self._ports.orchestrator_client().get_thread_lifecycle(
                        bound_thread_id
                    )
                )
            except Exception as e:
                self._logger.debug(f"Thread lifecycle poll failed (non-fatal): {e}")
                continue
            if not lifecycle:
                continue
            if (
                self._ports.identity().thread_id != bound_thread_id
                or self._ports.identity().session_generation != bound_runtime_generation
                or self._ports.identity().attach_token != bound_runtime_attach_token
            ):
                return
            status = lifecycle.get("status")
            observed_generation = canonical_runtime_generation(
                lifecycle.get("session_runtime_generation")
            )
            generation_moved = bool(
                bound_runtime_generation is not None
                and observed_generation != bound_runtime_generation
            )
            generation_missing = bool(
                runtime_generation_required and observed_generation is None
            )
            observed_attach_token = canonical_runtime_generation(
                lifecycle.get("session_runtime_attach_token")
            )
            attach_token_moved = observed_attach_token != bound_runtime_attach_token
            retirement_preflight = lifecycle.get("runtime_retirement_preflight") is True
            retirement_authorized = (
                lifecycle.get("runtime_retirement_authorized") is True
            )
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
                retirement_token = canonical_runtime_generation(
                    lifecycle.get("session_runtime_retirement_token")
                )
                if disposition not in {"ended", "suspended"}:
                    self._logger.warning(
                        "Authorized retirement omitted its immutable disposition "
                        "(thread=%s)",
                        bound_thread_id,
                    )
                    continue
                if type(permanent) is not bool:
                    self._logger.warning(
                        "Authorized retirement omitted immutable permanent intent "
                        "(thread=%s)",
                        bound_thread_id,
                    )
                    continue
                identity = self._ports.identity().retirement_identity()
                if identity is None:
                    return
                self.retirement_admission_identity = identity
                self.retirement_admission_disposition = disposition
                # A malformed/missing token is not locally accepted. The common
                # Begin call below will idempotently recover the exact token from
                # the server before any teardown effect.
                self.retirement_admission_token = retirement_token
                self.retirement_admission_permanent = permanent
                try:
                    await self.terminate("thread_retirement_authorized")
                except Exception as exc:
                    self._logger.warning(
                        "Authorized retirement cleanup failed: %s", type(exc).__name__
                    )
                    return
                self.schedule_exit(delay=1.0)
                return
            if status == "ending":
                # Exact-contract retirement is actionable only with the explicit
                # authorization bit and immutable disposition/token handshake.
                # A malformed or mixed-version shape must not trick the runtime
                # into tearing down while owner preflight may still abort.
                self._logger.warning(
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
                self._logger.info(
                    "Thread %s lifecycle no longer belongs to this runtime "
                    "(status=%r generation_match=%s attach_match=%s) — exiting.",
                    bound_thread_id,
                    status,
                    not (generation_moved or generation_missing),
                    not attach_token_moved,
                )
                try:
                    await self.terminate("thread_ended_oob")
                except Exception as e:
                    self._logger.warning(
                        f"Detach during status-watchdog exit failed: {e}"
                    )
                    # Local writer/process/mount quiescence or exact receipt is
                    # unproven.  Exiting here would abandon live workspace writers
                    # and the only local retry owner. Stay fenced/nonclaimable;
                    # the exact retirement task or durable reconciler converges it.
                    return
                self.schedule_exit(delay=1.0)
                return

    def start_watchdogs(self) -> None:
        """Start watchdog tasks for the active session. Safe to call repeatedly."""

        # Stateless executor (M3): no boot-WS ever arrives (input rides the run
        # queue) and thread status is orchestrator-owned — both watchdogs would
        # tear down healthy cached sessions. The run_queue lease/reaper plays
        # their abandoned-pod role in this mode.
        if self._ports.stateless_mode():
            self._logger.debug("Stateless executor mode: session watchdogs disabled")
            return

        # Stop any prior watchdogs (defensive — should already be cleared).
        for task in self.watchdog_tasks:
            if not task.done():
                task.cancel()
        self.watchdog_tasks = []

        identity = self.native_life_identity()
        if self.native_first_use_identity != identity:
            self.native_first_use_identity = None
        if self.boot_first_use_closed_identity != identity:
            self.boot_first_use_closed_identity = None
        self.ws_connected_event = asyncio.Event()
        if identity is not None and self.native_first_use_identity == identity:
            self.ws_connected_event.set()
        self.watchdog_tasks = [
            asyncio.create_task(
                self.boot_ws_watchdog(self.session_boot_ws_timeout_s),
                name="boot-ws-watchdog",
            ),
            asyncio.create_task(
                self.thread_status_watchdog(self.thread_status_poll_s),
                name="thread-status-watchdog",
            ),
        ]

    def stop_watchdogs(self) -> None:
        """Cancel all active watchdogs. Skips the current task to avoid self-cancel."""
        current = asyncio.current_task()
        for task in self.watchdog_tasks:
            if task is current or task.done():
                continue
            task.cancel()
        self.watchdog_tasks = []

    async def stop_and_join_watchdogs(self) -> None:
        """Cancel watchdogs and prove every independent task has quiesced."""

        current = asyncio.current_task()
        owned = [
            task
            for task in self.watchdog_tasks
            if task is not current and not task.done()
        ]
        self.stop_watchdogs()
        if not owned:
            return
        done, pending = await asyncio.wait(
            owned,
            timeout=float(os.environ.get("SESSION_WATCHDOG_CLOSE_TIMEOUT_S", "5")),
        )
        if pending:
            # Retain exact task ownership so the same retirement can retry joining
            # it. Remote stage/delete/settlement must not race an ignored cancel.
            self.watchdog_tasks = list(pending)
            raise EventJournalUnavailable(
                f"{len(pending)} session watchdog(s) ignored cancellation"
            )
        for task in done:
            if not task.cancelled() and task.exception() is not None:
                self._logger.warning(
                    "Session watchdog failed while joining terminal teardown: %s",
                    type(task.exception()).__name__,
                )

    def signal_ws_connected(self) -> None:
        """Signal a client connection or accepted input, ending the boot watchdog."""
        if self.ws_connected_event is not None:
            self.ws_connected_event.set()

    async def handle_attach_failure(self, thread_id: str, exc: Exception) -> None:
        """Preserve dedicated attach's exact failure exit after async startup."""
        if isinstance(exc, SessionEnded):
            await self.exit_session_ended(thread_id)
        elif isinstance(exc, SessionGrantDenied):
            await self.exit_grant_denied(thread_id, exc)
        elif isinstance(exc, self._ports.memory_unavailable_error):
            await self.exit_memory_unavailable(thread_id, exc)
        elif isinstance(
            exc, (WorkspaceNotReady, self._ports.workspace_unavailable_error)
        ):
            await self.exit_workspace_not_ready(thread_id, exc)
        else:
            self._logger.error(
                "Dedicated session attach failed for thread %s (%s)",
                thread_id,
                type(exc).__name__,
            )
            os._exit(1)

    async def exit_workspace_not_ready(
        self, thread_id: str, exc: Exception
    ) -> NoReturn:
        """Handle an unrecoverable workspace error during lifespan startup
        (WorkspaceNotReady — never provisioned/wedged; or WorkspaceUnavailableError
        — pod dead/unreachable): best-effort deregister then exit.

        Exits the process with status 0 (pod Completed, not Failed) so Kubernetes
        does not restart-loop the pod.  The orchestrator's session reconciler will
        recover the workspace and bind a fresh agent on the next interaction.
        """
        self._logger.info(
            "Workspace not ready for thread %s (%s) — exiting cleanly so the "
            "orchestrator can rebind once the workspace recovers (not a crash).",
            thread_id,
            exc,
        )
        await self._ports.attach().release_before_dedicated_exit(thread_id)
        if self._ports.orchestrator_client():
            try:
                self._ports.orchestrator_client().stop_heartbeat()
                if self._ports.heartbeat_task():
                    self._ports.heartbeat_task().cancel()
                await self._ports.orchestrator_client().deregister()
                await self._ports.orchestrator_client().close()
            except Exception as de:
                self._logger.warning(
                    "Best-effort deregister on workspace-not-ready failed: %s", de
                )
        os._exit(0)

    async def exit_grant_denied(self, thread_id: str, exc: Exception) -> NoReturn:
        """Handle a capability-grant denial at session attach (the workspace endpoint
        returned 403): log the REAL reason and exit cleanly (status 0, pod Completed
        — no K8s restart-loop). Unlike :func:`_exit_workspace_not_ready` this is NOT
        a transient workspace problem — a rebind hits the identical denial — so we do
        NOT claim the orchestrator will recover it. The cockpit re-surfaces the
        reason on its next create/prepare via the grant pre-flight (Layers 1/2).
        See knowledge-base/knowledge/issues/session_permission_mode_grant_denied_ready_timeout.md.
        """
        self._logger.error(
            "Session attach denied for thread %s by capability grants (%s) — exiting "
            "cleanly; NOT retrying (a rebind hits the same denial). The cockpit "
            "surfaces this on its next create/prepare grant pre-flight.",
            thread_id,
            exc,
        )
        await self._ports.attach().release_before_dedicated_exit(thread_id)
        if self._ports.orchestrator_client():
            try:
                self._ports.orchestrator_client().stop_heartbeat()
                if self._ports.heartbeat_task():
                    self._ports.heartbeat_task().cancel()
                await self._ports.orchestrator_client().deregister()
                await self._ports.orchestrator_client().close()
            except Exception as de:
                self._logger.warning(
                    "Best-effort deregister on grant-denied exit failed: %s", de
                )
        os._exit(0)

    async def exit_memory_unavailable(self, thread_id: str, exc: Exception) -> NoReturn:
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
        self._logger.error(
            "Session attach failed for thread %s — required memory unavailable (%s) "
            "— exiting cleanly; NOT retrying (a rebind hits the same failure). The "
            "cockpit surfaces this on its next create/prepare endpoint pre-flight.",
            thread_id,
            exc,
        )
        await self._ports.attach().release_before_dedicated_exit(thread_id)
        if self._ports.orchestrator_client():
            try:
                self._ports.orchestrator_client().stop_heartbeat()
                if self._ports.heartbeat_task():
                    self._ports.heartbeat_task().cancel()
                await self._ports.orchestrator_client().deregister()
                await self._ports.orchestrator_client().close()
            except Exception as de:
                self._logger.warning(
                    "Best-effort deregister on memory-unavailable exit failed: %s", de
                )
        os._exit(0)

    async def exit_duplicate_provision(self, thread_id: str) -> NoReturn:
        """Handle a lost provisioning race (409) during lifespan startup.

        Another live agent already owns this thread, so this pod must not serve it.
        We exit with status 0 (pod Completed under restartPolicy: Never, no restart
        loop) so the pod drops out of the per-session Service's endpoints instead of
        lingering as an orphan that black-holes ~half the cockpit's connection
        attempts (the Service uses publishNotReadyAddresses, so a not-ready orphan
        stays a live target). Only this pod's own agent record is cleaned up — never
        any thread-scoped resource, which belongs to the winning agent.
        """
        self._logger.warning(
            "Lost the provisioning race for thread %s — another live agent already "
            "owns it; exiting cleanly so this orphan pod leaves the session Service "
            "endpoints (not a crash).",
            thread_id,
        )
        if self._ports.orchestrator_client():
            try:
                self._ports.orchestrator_client().stop_heartbeat()
                if self._ports.heartbeat_task():
                    self._ports.heartbeat_task().cancel()
                await self._ports.orchestrator_client().deregister()
                await self._ports.orchestrator_client().close()
            except Exception as de:
                self._logger.warning(
                    "Best-effort deregister on duplicate-provision exit failed: %s", de
                )
        os._exit(0)

    async def exit_session_ended(self, thread_id: str) -> NoReturn:
        """Exit a dedicated runtime refused by the ended-session fence."""

        self._logger.info(
            "Thread %s ended before runtime attach — exiting without retrying or "
            "serving credentials.",
            thread_id,
        )
        if self._ports.orchestrator_client():
            try:
                # One exact pre-setup proof nominates normal retirement cleanup.
                # Retain identity through process exit; an accepted nomination
                # alone is deliberately not a confirmed release.
                receipt = self._ports.attach().release_receipt
                if isinstance(receipt, dict) and receipt.get("thread_id") == thread_id:
                    await self._ports.orchestrator_client().release_thread_agent(
                        thread_id,
                        **{
                            key: value
                            for key, value in receipt.items()
                            if key != "thread_id"
                        },
                    )
                self._ports.orchestrator_client().stop_heartbeat()
                if self._ports.heartbeat_task():
                    self._ports.heartbeat_task().cancel()
                await self._ports.orchestrator_client().deregister()
                await self._ports.orchestrator_client().close()
            except Exception as de:
                self._logger.warning(
                    "Best-effort ended-session deregister failed: %s", de
                )
        os._exit(0)

    async def terminate(
        self,
        reason: str,
        *,
        mark_thread: bool = True,
        preserve_shell: Optional[bool] = None,
        preserve_workspace_daemons: bool = False,
    ) -> str | None:
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
        active = self.termination_task
        if active is not None and not active.done():
            if asyncio.current_task() is self._ports.loop_task():
                # The active owner cancels and awaits this loop task. Awaiting the
                # owner here would form a cycle; this is the one safe no-op
                # re-entry. Every independent release/complete caller waits below.
                self._logger.debug(
                    "Terminate(%s) re-entered from the loop being joined", reason
                )
                return
            return await asyncio.shield(active)
        if not self._ports.session():
            return
        termination_session = self._ports.session()
        termination_identity = self._ports.identity().retirement_identity()
        termination_thread_id = self._ports.identity().thread_id

        async def _run() -> str | None:
            pass
            self.terminating = True
            try:
                retry_attempt = 0
                while True:
                    try:
                        result = await self._terminate_inner(
                            reason,
                            mark_thread=mark_thread,
                            preserve_shell=preserve_shell,
                            preserve_workspace_daemons=preserve_workspace_daemons,
                        )
                        if result != "actuator_requested" and reason in {
                            "boot_ws_timeout",
                            "thread_ended_oob",
                            "thread_retirement_authorized",
                        }:
                            # These callers are watchdog tasks. The common teardown
                            # cancels/joins them while this child remains shielded,
                            # so only the surviving exact owner can schedule exit.
                            self.schedule_exit(delay=1.0)
                        elif (
                            result != "actuator_requested"
                            and self.dedicated_pod_owes_exit(
                                reason, termination_thread_id, mark_thread=mark_thread
                            )
                        ):
                            # A dedicated Pod exits once it settled its own End.
                            # An End handed to the VM retirement actuator is still
                            # pending, so in either branch the Pod stays for it.
                            self.schedule_exit(delay=1.0)
                        return result
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        retry_identity = self.retirement_admission_identity
                        retryable_exact_retirement = bool(
                            self._ports.identity().runtime_contract
                            and termination_identity is not None
                            and self._ports.session() is termination_session
                            and self._ports.identity().retirement_identity()
                            == termination_identity
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
                        self._logger.warning(
                            "Exact local retirement quiescence failed; retaining "
                            "the nonclaimable owner and retrying (thread=%s type=%s)",
                            termination_identity[0],
                            type(exc).__name__,
                        )
                        if delay:
                            await asyncio.sleep(delay)
            finally:
                self.terminating = False
                if self.termination_task is asyncio.current_task():
                    self.termination_task = None

        task = asyncio.create_task(
            _run(),
            name=f"session-terminate-{str(self._ports.identity().thread_id or 'detached')[:12]}",
        )
        self.termination_task = task
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            # Teardown continues as the single owner. Propagate caller
            # cancellation without publishing a false completion signal.
            raise

    def dedicated_pod_owes_exit(
        self, reason: str, thread_id: Optional[str], *, mark_thread: bool
    ) -> bool:
        """Whether a settled self-End leaves this dedicated Pod without a purpose."""

        return bool(
            mark_thread
            and reason in _DEDICATED_SELF_END_REASONS
            and not self._ports.stateless_mode()
            and thread_id
            and os.environ.get("SESSION_BOUND_THREAD_ID", "") == str(thread_id)
        )

    async def request_vm_retirement_actuator(
        self,
        *,
        pinned_agent_id: str,
        retirement_permanent: bool,
    ) -> str:
        """Retry only the frozen drain handoff; acceptance leaves End pending."""
        session = self._ports.session()
        identity = self._ports.identity().retirement_identity()
        if (
            not isinstance(session, self._ports.session_type)
            or not session.terminal_vm_drain_complete
            or identity is None
            or self.retirement_admission_identity != identity
            or self.retirement_admission_disposition != "ended"
            or self.retirement_admission_permanent is not retirement_permanent
            or not self.retirement_admission_token
            or session.local_quiescence_protocol
        ):
            raise EventJournalUnavailable("VM actuator handoff lacks exact local drain")
        if session.terminal_actuator_request_accepted:
            return "actuator_requested"
        if session.terminal_actuator_request is None:
            session.terminal_actuator_request = {
                "pinned_agent_id": pinned_agent_id,
                "pod_uid": str(os.environ.get("POD_UID") or ""),
                "process_generation": str(
                    getattr(
                        self._ports.orchestrator_client(),
                        "dispatch_process_generation",
                        "",
                    )
                    or ""
                ),
                "session_runtime_generation": identity[1],
                "session_runtime_attach_token": identity[2],
                "session_runtime_retirement_token": self.retirement_admission_token,
                "retirement_disposition": "ended",
                "retirement_permanent": retirement_permanent,
                "workspace_generation": session.workspace_generation,
                "workspace_runtime_incarnation": session.workspace_runtime_incarnation,
            }
        attempt = 0
        while (
            self._ports.session() is session
            and self._ports.identity().retirement_identity() == identity
        ):
            try:
                response = await self._ports.orchestrator_client().request_thread_retirement_actuator(
                    identity[0],
                    **session.terminal_actuator_request,
                )
                if isinstance(response, dict) and response.get("status") in {
                    "actuator_requested",
                    "settled_or_superseded",
                }:
                    session.terminal_actuator_request_accepted = True
                    return "actuator_requested"
            except Exception as exc:
                self._logger.warning(
                    "VM actuator handoff response unavailable (thread=%s type=%s)",
                    identity[0],
                    type(exc).__name__,
                )
            delay = _EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS[
                min(attempt, len(_EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS) - 1)
            ]
            attempt += 1
            await asyncio.sleep(delay)
        raise EventJournalUnavailable("VM actuator handoff identity changed")

    async def settle_exact_retirement_after_quiescence(
        self,
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
        exact_retirement_token = self.retirement_admission_token
        exact_contract = bool(
            self._ports.identity().runtime_contract
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
            if self._ports.identity().retirement_identity() != expected_identity:
                return False
            try:
                settled = await self._ports.update_thread_status(
                    "ended",
                    pinned_agent_id=pinned_agent_id,
                    retirement_disposition=retirement_disposition,
                    retirement_permanent=retirement_permanent,
                )
            except Exception as exc:
                self._logger.warning(
                    "Exact retirement settlement attempt failed (thread=%s type=%s)",
                    expected_identity[0]
                    if expected_identity
                    else self._ports.identity().thread_id,
                    type(exc).__name__,
                )
                settled = False
            if settled:
                return True
            if not exact_contract:
                return False
            outcome_reader = getattr(
                self._ports.orchestrator_client(), "get_thread_retirement_outcome", None
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
                    self._logger.warning(
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

    async def _terminate_inner(
        self,
        reason: str,
        *,
        mark_thread: bool = True,
        preserve_shell: Optional[bool] = None,
        preserve_workspace_daemons: bool = False,
    ) -> str | None:
        """Body of _terminate_session — only reached holding the _terminating guard."""

        if not self._ports.session():
            return

        thread_id = self._ports.identity().thread_id
        runtime_generation = self._ports.identity().session_generation
        runtime_attach_token = self._ports.identity().attach_token
        retirement_disposition = self.terminal_retirement_disposition()
        retirement_permanent = (
            self.retirement_admission_permanent
            if self.retirement_admission_identity
            == self._ports.identity().retirement_identity()
            and self.retirement_admission_permanent is not None
            else False
        )
        preserve_remote_shell = (
            not mark_thread if preserve_shell is None else preserve_shell
        )
        self._logger.info(f"Terminating session: thread={thread_id} reason={reason}")

        pinned_control_owner = (
            None
            if self._ports.stateless_mode()
            else (
                self._ports.control_owner_agent_id()
                or self._ports.registered_pinned_agent_id()
            )
        )
        if mark_thread and not self._ports.stateless_mode():
            # Linearize retirement before *any* local teardown effect. Besides
            # explicit/idle archive, this common path owns loop crash/complete,
            # shutdown, watchdog and REST detach. Cancelling the loop, stopping
            # transports or cleaning mounts before the durable ``ending`` fence
            # would leave the row preparable while teardown was already in flight.
            # Repeating the exact generation/attach-token transition is
            # intentionally idempotent and reuses the orchestrator's pending
            # retirement authority.
            if not await self.begin_retirement(
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

        vm_actuator_handoff = bool(
            mark_thread
            and not preserve_remote_shell
            and not preserve_workspace_daemons
            and self._ports.identity().runtime_contract
            and pinned_control_owner
            and retirement_disposition == "ended"
            and self.retirement_admission_identity
            == self._ports.identity().retirement_identity()
            and self.retirement_admission_token
            and self._ports.session().workspace_backend_tier in {"vm", "remote"}
        )
        if (
            vm_actuator_handoff
            and self._ports.session().terminal_vm_drain_complete is True
        ):
            return await self.request_vm_retirement_actuator(
                pinned_agent_id=pinned_control_owner,
                retirement_permanent=retirement_permanent,
            )

        # Cancel in-flight loop_task FIRST. Out-of-band callers (heartbeat-intent
        # drain, thread-status watchdog) reach this without going through the
        # loop's normal exit path, so without this the loop's next
        # _session.permission_mode access AttributeErrors when we null _session
        # below. Skipped when invoked from inside the loop itself (e.g. via
        # _loop_completion_handler's cleanup, which would deadlock awaiting self).
        loop_task = self._ports.loop_task()
        if loop_task is not None and loop_task is not asyncio.current_task():
            if not loop_task.done():
                loop_task.cancel()
                try:
                    await loop_task
                except (asyncio.CancelledError, Exception):
                    pass
        self._ports.publish_loop_task(None)

        # Cancel and join self-cleanup watchdogs first — merely dropping their
        # task references would let a delayed cancellation/finally block overlap
        # the remote stage/delete/settlement actuator below.
        await self.stop_and_join_watchdogs()

        # Close public admission before stopping a pinned owner. The dedicated
        # gate is needed for drain-suspend: the general agent status route forbids
        # writing ``suspended``, and marking ``ended`` would prevent the snapshot
        # transition that follows teardown. The thread-row update serializes with
        # admission; one last exact-owner drain then consumes every request that
        # committed before closure. A lost binding means a successor owns that
        # work, so this runtime must not adopt it.
        admission_closed = True
        if pinned_control_owner is not None and not self.retirement_admission_closed():
            admission_closed = await self._ports.close_pinned_control_inbox(
                agent_id=pinned_control_owner
            )
            if not admission_closed:
                # The reciprocal binding is the pinned owner's resource fence. A
                # stale pod that lost it may close only its own transports; the
                # successor can already be using the deterministic remote tmux.
                preserve_remote_shell = True
                self._logger.info(
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
        self._ports.permission_gates().clear()
        self._ports.publish_active_permission(None)
        await self._ports.retire_announced_permissions(f"session terminated ({reason})")
        self._ports.announced_permissions().clear()

        # A pinned consumer owns the attach lifetime; a stateless consumer owns
        # the active lease. In both cases it must be fully stopped before the
        # journal writer drains or ownership is released.
        await self._ports.stop_interrupt_watcher()
        await self._ports.stop_control_watcher()

        # Join process-global side tasks while the captured session/thread identity
        # and event writer are still authoritative.  A delayed title or protected
        # cloud ping must never observe the next pool attachment.
        await self.quiesce_session_side_tasks()

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
            getattr(self._ports.session(), "terminal_memory_capture_attempted", False)
            is not True
            and not self._ports.stateless_mode()
            and self._ports.session().memory_service is not None
            and not self._ports.session().final_memory_extracted
            and self._ports.session().messages
            and not (
                self._ports.session().shell_owner_token is not None and not mark_thread
            )
            and not self.termination_admission_closed()
        ):
            self._ports.session().terminal_memory_capture_attempted = True
            try:
                from agent.services.memory import CaptureEvent

                await self._ports.session().memory_service.capture(
                    CaptureEvent(
                        kind="session_end", messages=self._ports.session().messages
                    )
                )
                self._ports.session().final_memory_extracted = True
                self._logger.info(
                    "Terminate(%s): final memory capture complete", reason
                )
            except Exception as e:
                self._logger.warning(
                    f"Terminate memory capture failed (non-fatal): {e}"
                )

        # capture_nowait(pre_compaction) and asynchronous citation verification
        # both carry session-scoped write/callback authority. Disarm and join them
        # before the journal closes and before a queue claimant can be released.
        try:
            quiesce_result = self._ports.session().quiesce_background_tasks()
            if inspect.isawaitable(quiesce_result):
                await quiesce_result
            elif isinstance(self._ports.session(), self._ports.session_type):
                raise RuntimeError("PersistentSession RAM quiescence is not awaitable")
        except Exception as exc:
            if not self._ports.stateless_mode():
                raise EventJournalUnavailable(
                    "pinned session background work did not quiesce"
                ) from exc
            if self._ports.session().shell_owner_token is not None:
                raise
            self._logger.warning(
                "Pinned session background-task quiescence failed (contained)",
                exc_info=True,
            )

        if (
            getattr(self._ports.session(), "terminal_finalization_attempted", False)
            is not True
        ):
            self._ports.session().terminal_finalization_attempted = True
            # Final cloud sync + drop secrets. No more background polling to stop:
            # Phase 1 moved sync to turn boundaries via the coordinator. The last
            # turn's background push must land first — never two concurrent walks of
            # one mount, and never an aclose under an in-flight push.
            if self._ports.session().workspace_sync:
                try:
                    await self._ports.await_pending_cloud_push()
                    # Stateless bytes are committed only by the armed generation task
                    # above. A second raw push here would have no durable requirement
                    # or acknowledgement and, on lease-loss teardown, could overlap a
                    # successor's pull. Pinned teardown keeps its existing final
                    # push+pull byte-for-byte.
                    if not self._ports.stateless_mode():
                        await self._ports.session().workspace_sync.push_all()
                        await self._ports.session().workspace_sync.pull_all()
                except Exception as e:
                    self._logger.warning(f"Final cloud sync failed (non-fatal): {e}")
                if self._ports.background_push_owns(
                    self._ports.session().workspace_sync
                ):
                    # Step 4a: a handed-off push still transmits through this
                    # coordinator; its done-callback closes it. Closing here would
                    # yank the WebDAV client from under the off-slot transmit.
                    self._logger.info(
                        "cloud sync coordinator left open for the handed-off push (thread %s)",
                        thread_id,
                    )
                else:
                    try:
                        await self._ports.session().workspace_sync.aclose()
                    except Exception as e:
                        self._logger.debug(f"Cloud sync aclose failed (non-fatal): {e}")

            # Final git commit + push
            if self._ports.session().workspace_manager:
                git_mgr = getattr(
                    self._ports.session().workspace_manager, "git_manager", None
                )
                if git_mgr and git_mgr.is_active:
                    try:
                        if git_mgr.has_uncommitted_changes():
                            git_mgr.commit(f"Session detach: thread {thread_id}")
                        if git_mgr.push() is False:
                            self._logger.warning(
                                "Final git push was unsuccessful (non-fatal): %s",
                                getattr(git_mgr, "last_push_error", None),
                            )
                    except Exception as e:
                        self._logger.warning(f"Final git push failed (non-fatal): {e}")

        # The journal owns a captured pool + thread identity. Drain it while both
        # the session and live subscribers still exist: terminal Canvas failures
        # can then emit their direct reconciliation control before teardown clears
        # either registry, and a pool-mode reattach cannot inherit queued events.
        event_writer = self._ports.event_writer()
        if event_writer is not None:
            try:
                await event_writer.close()
            except Exception as e:
                if not self._ports.stateless_mode():
                    # A pinned End is not allowed to stage/delete/settle while an
                    # ordinary G-scoped journal batch may still be in flight.
                    # Preserve the writer and exact local retirement fence so the
                    # same authority can retry close; do not publish a false
                    # terminal lifecycle edge.
                    raise EventJournalUnavailable(
                        "pinned thread event writer did not quiesce"
                    ) from e
                self._logger.warning(
                    "thread_events writer close failed (thread=%s): %s",
                    thread_id,
                    e,
                )
            else:
                self._ports.publish_event_writer(None)

        # Shell ownership is deliberately separate from thread-status authority.
        # Claim switches preserve by explicit/default disposition; a stale pinned
        # owner that lost its reciprocal binding is forced to preserve above.
        # This is deliberately after final Git: GitManager itself delegates through
        # the remote shell. From here onward cleanup may mutate mount transports but
        # no new tool/shell command is admitted.
        self._ports.session().retire_shell_owner()
        cleanup_kwargs = {
            "preserve_shell": preserve_remote_shell,
            "preserve_workspace_daemons": preserve_workspace_daemons,
        }
        if vm_actuator_handoff:
            cleanup_kwargs["allow_vm_actuator_handoff"] = True
        cleanup_result = await self._ports.session().cleanup(**cleanup_kwargs)
        if cleanup_result == "actuator_required":
            return await self.request_vm_retirement_actuator(
                pinned_agent_id=pinned_control_owner,
                retirement_permanent=retirement_permanent,
            )

        if mark_thread and not await self.settle_exact_retirement_after_quiescence(
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
        self._ports.publish_session(None)
        self._ports.identity().release_thread()
        self._ports.attach().clear_runtime_actor()

        # Clear headless input state + subscriber registry. The pump tasks owned by
        # each subscriber are cancelled by their socket handlers' finally blocks
        # when those handlers notice the WS close; dropping the registry here
        # ensures stale entries don't accumulate across sessions.
        self._ports.input_runtime().teardown()
        self._ports.identity().clear_process_generation()
        self._ports.publish_runtime_authorization(False)
        self._ports.identity().set_status_contract(False)
        self._ports.identity().clear(
            expected_generation=runtime_generation,
            expected_attach_token=runtime_attach_token,
        )
        self._ports.publish_draft_title(None)
        self._ports.canvas_control().clear_all()
        self._ports.subscribers().clear()

        # Phase 2 event-log cursor reset. The next session attach reads the
        # epoch fresh from the threads table. The ordered writer was already
        # drained and cleared above, before either captured identity disappeared.
        self._ports.reset_journal_cursor()
        self._ports.reset_turn_state()
        # Pool agents serve many threads; a pending retry must not leak into the
        # next session, whose attach resolves its own cloud state.
        self._ports.set_cloud_sync_retry_pending(False)

        # Safety valve: restart after N sessions to guard against state leakage
        self.sessions_served += 1
        if (
            self.max_sessions_per_process > 0
            and self.sessions_served >= self.max_sessions_per_process
        ):
            self._logger.info(
                f"Max sessions per process reached ({self.sessions_served}/{self.max_sessions_per_process}). "
                "Exiting — Docker will restart the container."
            )
            import sys

            sys.exit(0)

        self._logger.info(
            f"Session terminated: thread={thread_id} "
            f"reason={reason} (sessions served: {self.sessions_served})"
        )

    async def detach_session(self) -> None:
        """Back-compat shim. Prefer _terminate_session(reason) at new call sites.

        Kept so existing tests patching `_detach_session` continue to work and so
        code paths not yet updated don't break. Logs at DEBUG so each invocation
        is traceable.
        """
        self._logger.debug("_detach_session() called via back-compat shim")
        await self.terminate("legacy")

    def start_loop_completion_handler(self, loop_task: asyncio.Task) -> asyncio.Task:
        """Track completion separately: it may shield/join the termination owner."""
        task = asyncio.create_task(
            self.loop_completion_handler(loop_task), name="persistent-loop-completion"
        )
        self.loop_completion_tasks.add(task)
        task.add_done_callback(self.loop_completion_tasks.discard)
        return task

    async def drain_loop_completion_tasks(self) -> None:
        """Shutdown joins these after termination, never from its cleanup loop."""
        pending = {
            task
            for task in self.loop_completion_tasks
            if task is not asyncio.current_task() and not task.done()
        }
        if pending:
            await asyncio.gather(*(asyncio.shield(task) for task in pending))

    async def loop_completion_handler(self, loop_task: asyncio.Task) -> None:
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
        except self._ports.idle_timeout_error:
            self._logger.info("Persistent loop exited via idle timeout")
            if self._ports.stateless_mode():
                # Stateless lane: the pod-side idle timer must never end the
                # THREAD — thread lifecycle is orchestrator-owned, and an
                # 'ended' status (or the session.ended frame the archive
                # broadcasts) would force an epoch bump on the next claim's
                # attach (client cache-wipe cascade). Just drop the cached
                # session; the next claim rebuilds from thread_messages.
                await self.terminate("idle_timeout", mark_thread=False)
                return
            try:
                await self._ports.handle_idle_archive()
            except Exception as e:
                self._logger.warning(f"Idle archive failed: {e}")
            await self.terminate("idle_timeout")
        except asyncio.CancelledError:
            # Cancellation came from _terminate_session itself — don't re-enter.
            # Re-raise so the wrapper task surfaces as cancelled.
            raise
        except Exception as e:
            self._logger.warning(f"Persistent loop crashed: {e}", exc_info=True)
            # Every opened turn gets a terminal edge, on this path too: without a
            # turn.error the journal keeps turn.started open, every attached
            # client spins on a turn that ended, and every reload that replays
            # the journal reopens it. Persisted as a role='error' row so the
            # line survives reload. Not on a lost lease — that turn belongs to
            # a successor claim now, which closes it itself.
            if not isinstance(e, LeaseLostError):
                try:
                    await self._ports.loop_on_error(
                        "The session loop stopped before this turn could be "
                        f"settled: {e}. The transcript is preserved — send a "
                        "message to continue."
                    )
                except Exception:
                    self._logger.debug(
                        "turn.error on loop crash failed (non-fatal)", exc_info=True
                    )
            await self.terminate(
                "loop_crash", mark_thread=not self._ports.stateless_mode()
            )
        else:
            self._logger.info("Persistent loop completed cleanly")
            await self.terminate(
                "loop_complete", mark_thread=not self._ports.stateless_mode()
            )

    async def reconcile_retirement_begin_or_reopen_controls(
        self,
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

        if self._ports.identity().retirement_identity() != identity:
            return False

        async def reopen_exact_runtime() -> bool | None:
            """Reopen durable controls and the already-settled child runtime.

            The DB CAS proves this exact life is still token-null.  Child resume
            then performs its own awaited effect-authority proof.  If that second
            proof or the settled-state check fails, close controls again and latch
            local admission: an otherwise-live session must not resume only half
            of its execution surfaces.
            """

            pass
            pass

            if self._ports.identity().retirement_identity() != identity:
                return None
            if (
                self.retirement_admission_identity == identity
                and self.retirement_admission_token is not None
            ):
                return None
            if not await self._ports.set_pinned_control_admission(
                agent_id=exact_agent_id,
                open_for_admission=True,
            ):
                return None
            if self.retirement_admission_identity == identity:
                # A prior local resume failure may have latched a token-less
                # admission fence. The exact token-null CAS above proves that
                # fence is now safe to clear before SessionHost re-proves effect
                # authority. An authorized token is never reopenable here.
                self.retirement_admission_identity = None
                self.retirement_admission_disposition = None
                self.retirement_admission_token = None
                self.retirement_admission_permanent = None
            session = self._ports.session()
            resume = getattr(session, "resume_subagents", None)
            try:
                if session is None or not callable(resume):
                    raise RuntimeError("session has no child-runtime resume boundary")
                await resume()
                if (
                    self._ports.session() is not session
                    or self._ports.identity().retirement_identity() != identity
                ):
                    raise RuntimeError(
                        "session identity moved during child-runtime resume"
                    )
                return True
            except Exception:
                self._logger.warning(
                    "Child runtime could not resume after retirement abort "
                    "(thread=%s agent=%s)",
                    identity[0],
                    exact_agent_id,
                    exc_info=True,
                )
                try:
                    if self._ports.identity().retirement_identity() == identity:
                        await self._ports.set_pinned_control_admission(
                            agent_id=exact_agent_id,
                            open_for_admission=False,
                        )
                except Exception:
                    self._logger.warning(
                        "Control admission re-close failed after child-runtime "
                        "resume refusal (thread=%s agent=%s)",
                        identity[0],
                        exact_agent_id,
                        exc_info=True,
                    )
                if self._ports.identity().retirement_identity() == identity:
                    self.retirement_admission_identity = identity
                    self.retirement_admission_disposition = retirement_disposition
                    self.retirement_admission_token = None
                    self.retirement_admission_permanent = retirement_permanent
                return False

        if not begin_was_sent:
            # No retirement request crossed the process boundary. Reopening still
            # uses the exact token-null DB CAS: a concurrent owner Begin or a moved
            # successor wins and leaves this process closed.
            if reopen_if_uncommitted:
                try:
                    await reopen_exact_runtime()
                except Exception:
                    self._logger.warning(
                        "Exact control admission reopen failed after local retirement "
                        "preflight (thread=%s agent=%s)",
                        identity[0],
                        exact_agent_id,
                        exc_info=True,
                    )
            return False

        client = self._ports.orchestrator_client()
        lifecycle_reader = getattr(client, "get_thread_lifecycle", None)
        if not callable(lifecycle_reader):
            # Begin may have committed. A missing outcome reader cannot prove that
            # controls are safe to reopen, even if the transport reported failure.
            return False

        attempt = 0
        while self._ports.identity().retirement_identity() == identity:
            try:
                lifecycle = await lifecycle_reader(identity[0])
            except Exception as exc:
                self._logger.warning(
                    "Exact retirement Begin reconciliation failed (thread=%s type=%s)",
                    identity[0],
                    type(exc).__name__,
                )
                return False
            if not isinstance(lifecycle, dict):
                return False
            observed_generation = canonical_runtime_generation(
                lifecycle.get("session_runtime_generation")
            )
            observed_attach = canonical_runtime_generation(
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
                token = canonical_runtime_generation(
                    lifecycle.get("session_runtime_retirement_token")
                )
                if (
                    token is not None
                    and lifecycle.get("retirement_disposition")
                    == retirement_disposition
                    and lifecycle.get("retirement_permanent") is retirement_permanent
                ):
                    self.retirement_admission_identity = identity
                    self.retirement_admission_disposition = retirement_disposition
                    self.retirement_admission_token = token
                    self.retirement_admission_permanent = retirement_permanent
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
                    self._logger.warning(
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

    async def begin_retirement(
        self,
        *,
        pinned_agent_id: Optional[str] = None,
        retirement_disposition: str = "ended",
        retirement_permanent: bool = False,
        reopen_controls_if_uncommitted: bool = True,
    ) -> bool:
        """Close admission for this exact pinned life and mirror it locally."""

        if self._ports.stateless_mode() or self._ports.session() is None:
            return False
        if retirement_disposition not in {"ended", "suspended"}:
            return False
        if type(retirement_permanent) is not bool:
            return False
        identity = self._ports.identity().retirement_identity()
        if identity is None:
            return False
        # Close the whole parent admission surface before child quiescence. A
        # timeout or ambiguous terminal-delivery write leaves the child runtime
        # non-accepting; keeping parent providers/inputs open in that state would
        # create a half-live session. The common termination owner retries this
        # exact tokenless identity until child settlement and Begin converge.
        if self.retirement_admission_identity != identity:
            self.retirement_admission_identity = identity
            self.retirement_admission_disposition = retirement_disposition
            self.retirement_admission_token = None
            self.retirement_admission_permanent = retirement_permanent
        # Child terminal/transcript writes require the still-current parent
        # authority. Close child admission and settle every generation before the
        # server installs the retirement token that revokes it.
        quiesce_reason = f"parent session retiring as {retirement_disposition}"
        try:
            await self._ports.session().quiesce_subagents(quiesce_reason)
        except Exception as exc:
            if self.retirement_admission_token is None:
                self._logger.warning(
                    "Session child runtime did not quiesce before retirement "
                    "(thread=%s disposition=%s)",
                    self._ports.identity().thread_id,
                    retirement_disposition,
                    exc_info=True,
                )
                return False
            # A person's End: the server installed and authorized this exact
            # life's token first (the watchdog mirrored it), so the authority
            # children settle with is gone for good and a live child can never
            # settle. Leave the children to the retirement, which ends their
            # running rows ``cancelled:parent_retired`` (D4).
            self._logger.info(
                "Session child runtime cannot settle under the authorized "
                "retirement (%s); leaving its children to it (thread=%s)",
                exc,
                self._ports.identity().thread_id,
            )
            try:
                await self._ports.session().leave_subagents_to_retirement(
                    quiesce_reason
                )
            except Exception:
                self._logger.warning(
                    "Session child runtime could not be left to the authorized "
                    "retirement (thread=%s disposition=%s)",
                    self._ports.identity().thread_id,
                    retirement_disposition,
                    exc_info=True,
                )
                return False
        # From this point onward every child is settled. Exact no-retirement
        # reconciliation clears the tokenless latch and resumes both surfaces
        # together; every other failure stays safely fail-closed.
        if self.retirement_admission_identity == identity:
            if self.retirement_admission_disposition != retirement_disposition:
                return False
            if self.retirement_admission_permanent is not retirement_permanent:
                return False
            if self.retirement_admission_token is not None:
                return True
            # The watchdog may have observed an authorised shape whose token was
            # lost/malformed. Fall through to the idempotent exact Begin call and
            # recover T before any local teardown effect.
        exact_agent_id = pinned_agent_id or self._ports.registered_pinned_agent_id()
        if exact_agent_id is not None:
            # This is the sole pre-retirement operation. It serializes with public
            # durable-control admission while the runtime token is still open and
            # drains everything admitted before closure. Once `ending` installs a
            # retirement token, the control owner fence intentionally refuses all
            # further consumption; never move this drain after the status call.
            try:
                if not await self._ports.close_pinned_control_inbox(
                    agent_id=exact_agent_id
                ):
                    if self._ports.identity().runtime_contract:
                        return await self.reconcile_retirement_begin_or_reopen_controls(
                            identity=identity,
                            exact_agent_id=exact_agent_id,
                            retirement_disposition=retirement_disposition,
                            retirement_permanent=retirement_permanent,
                            begin_was_sent=False,
                            reopen_if_uncommitted=reopen_controls_if_uncommitted,
                        )
                    return False
            except Exception:
                self._logger.warning(
                    "Exact control preflight failed before retirement (thread=%s agent=%s)",
                    self._ports.identity().thread_id,
                    exact_agent_id,
                    exc_info=True,
                )
                if self._ports.identity().runtime_contract:
                    return await self.reconcile_retirement_begin_or_reopen_controls(
                        identity=identity,
                        exact_agent_id=exact_agent_id,
                        retirement_disposition=retirement_disposition,
                        retirement_permanent=retirement_permanent,
                        begin_was_sent=False,
                        reopen_if_uncommitted=reopen_controls_if_uncommitted,
                    )
                return False
        retirement_token: str | None = None
        if self._ports.identity().runtime_contract:
            if (
                exact_agent_id is None
                or self._ports.identity().session_generation is None
                or self._ports.identity().attach_token is None
                or self._ports.orchestrator_client() is None
            ):
                if exact_agent_id is not None:
                    await self.reconcile_retirement_begin_or_reopen_controls(
                        identity=identity,
                        exact_agent_id=exact_agent_id,
                        retirement_disposition=retirement_disposition,
                        retirement_permanent=retirement_permanent,
                        begin_was_sent=False,
                        reopen_if_uncommitted=reopen_controls_if_uncommitted,
                    )
                return False
            begin = getattr(
                self._ports.orchestrator_client(), "begin_thread_retirement", None
            )
            if not callable(begin):
                await self.reconcile_retirement_begin_or_reopen_controls(
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
                    str(self._ports.identity().thread_id),
                    pinned_agent_id=exact_agent_id,
                    session_runtime_generation=self._ports.identity().session_generation,
                    session_runtime_attach_token=self._ports.identity().attach_token,
                    retirement_disposition=retirement_disposition,
                    retirement_permanent=retirement_permanent,
                )
            except Exception:
                self._logger.warning(
                    "Exact retirement Begin transport failed (thread=%s agent=%s)",
                    identity[0],
                    exact_agent_id,
                    exc_info=True,
                )
                return await self.reconcile_retirement_begin_or_reopen_controls(
                    identity=identity,
                    exact_agent_id=exact_agent_id,
                    retirement_disposition=retirement_disposition,
                    retirement_permanent=retirement_permanent,
                    begin_was_sent=True,
                    reopen_if_uncommitted=reopen_controls_if_uncommitted,
                )
            if not isinstance(response, dict):
                return await self.reconcile_retirement_begin_or_reopen_controls(
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
                return await self.reconcile_retirement_begin_or_reopen_controls(
                    identity=identity,
                    exact_agent_id=exact_agent_id,
                    retirement_disposition=retirement_disposition,
                    retirement_permanent=retirement_permanent,
                    begin_was_sent=True,
                    reopen_if_uncommitted=reopen_controls_if_uncommitted,
                )
            retirement_token = canonical_runtime_generation(
                response.get("session_runtime_retirement_token")
            )
            if retirement_token is None:
                return await self.reconcile_retirement_begin_or_reopen_controls(
                    identity=identity,
                    exact_agent_id=exact_agent_id,
                    retirement_disposition=retirement_disposition,
                    retirement_permanent=retirement_permanent,
                    begin_was_sent=True,
                    reopen_if_uncommitted=reopen_controls_if_uncommitted,
                )
        else:
            if not await self._ports.update_thread_status(
                "ending",
                pinned_agent_id=exact_agent_id,
                retirement_disposition=retirement_disposition,
                retirement_permanent=retirement_permanent,
            ):
                if exact_agent_id is not None:
                    await self.reconcile_retirement_begin_or_reopen_controls(
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
        if (
            self._ports.session() is None
            or self._ports.identity().retirement_identity() != identity
        ):
            return False
        self.retirement_admission_identity = identity
        self.retirement_admission_disposition = retirement_disposition
        self.retirement_admission_token = retirement_token
        self.retirement_admission_permanent = retirement_permanent
        return True
