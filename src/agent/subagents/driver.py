"""``SubagentDriver`` — a headless child session on ``run_persistent_loop`` (U3 B.3).

The driver implements ``PersistentLoopCallbacks`` for one child: an inbox
queue drained by ``get_user_input`` (STOP → ``CancelledError`` = the loop's
clean exit), accumulation of tokens/thinking, tool audit under the PARENT job,
an auto-approving permission gate, the interrupt flag, the end-of-turn edge
(``on_turn_settled``), LLM metering as ``call_type="subagent"`` and the child
budgets (turns, tokens, staleness) enforced from outside the loop.

``run(brief)`` enqueues one brief and waits for the turn to settle, then
classifies the outcome from the durable message list (spike §4):

- ``completed``          last message is a tool-free assistant answer;
- ``parked``             the turn ended on an unanswered tool call (a
                         NO_ANSWER gate or a ``[tool-call interruption]``);
- ``interrupted:<why>``  the driver ended it (``stale``, ``drain``, ``stopped``);
- ``capped:<turns|tokens>`` a budget hit and the forced synthesis turn answered;
- ``error``              ``on_error`` fired, the loop died, the turn ended on
                         a ToolMessage, or the answer is a ``⚠`` placeholder
                         (empty response / refusal / output cap).

Forced synthesis: a cap arms a ``graceful`` interrupt (the loop ends the turn
at its next LLM/tool checkpoint, recording the interruption event if a batch
was pending) → ``on_turn_settled`` sees ``_synth_pending`` → ``current_tools``
flips to a tool-less binding → one ``role=event`` synthesis turn (the light
runner's prompt) → classify. Tool output is never promoted to a result.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)

from shared.runtime.core.context_entries import is_context_entry
from shared.runtime.core.message_markers import PERSIST_ROLE_EVENT, PERSIST_ROLE_KEY
from agent.persistent_graph import (
    PermissionOutcome,
    PersistentLoopCallbacks,
    run_persistent_loop,
)

from agent.subagents.budgets import ChildBudgets, StalenessWatcher
from agent.subagents.child import ChildBuild
from agent.subagents.host import ParentHost
from agent.subagents.ledger import NullLedger, SubagentLedger

logger = logging.getLogger(__name__)


class _Stop:
    """Inbox sentinel: ``get_user_input`` raises ``CancelledError`` on it."""

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "<STOP>"


STOP = _Stop()


class SubagentAuthorityLost(RuntimeError):
    """The parent lost exact authority before an external child effect."""


#: The forced-synthesis prompt (inherited from the light runner, deleted in
#: U3 WP4).
SYNTH_PROMPT = (
    "You have reached your {reason}. Do NOT call any more tools. Based only "
    "on what you have gathered so far, write your final answer now."
)
_CAP_REASONS = {"turns": "turn budget", "tokens": "token budget"}
_STALE_REASON = "activity limit"

#: The loop's placeholder texts (persistent_graph.py) — never a result.
PLACEHOLDER_PREFIX = "⚠ "
TRUNCATION_MARKER = "⚠ Output truncated at the model's limit"
_UNSET = object()
_SEED_PERSIST_TIMEOUT_S = 5.0


def message_text(content: Any) -> str:
    """Plain text of a message's content (string or provider blocks)."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: List[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and block.get("type") in (None, "text"):
                text = block.get("text")
                if isinstance(text, str):
                    parts.append(text)
        return "".join(parts)
    return str(content) if content else ""


@dataclass
class SubagentResult:
    """What ``run(brief)`` returns; ``envelope.build_envelope`` renders it."""

    status: str
    text: str
    turns: int
    tokens: int
    duration: float
    handle: str
    subagent_type: str
    subagent_id: str
    parked_call: Optional[Dict[str, Any]] = None
    sudo_requested: bool = False
    error: Optional[str] = None
    partial: bool = False
    tool_calls: int = 0
    loop_turns: int = 0
    streamed_text: str = ""
    thinking: str = ""

    @property
    def kind(self) -> str:
        """``completed`` / ``parked`` / ``interrupted`` / ``capped`` / ``error``."""
        return self.status.split(":", 1)[0]

    @property
    def ok(self) -> bool:
        return self.status == "completed"


class SubagentDriver:
    """One child: the callbacks, the loop task, the budgets, the outcome."""

    def __init__(
        self,
        build: ChildBuild,
        *,
        host: ParentHost,
        parent_context: Any,
        subagent_id: Optional[str] = None,
        budgets: Optional[ChildBudgets] = None,
        ledger: Optional[SubagentLedger] = None,
        clock: Callable[[], float] = time.monotonic,
        parent_tool_call_id: Optional[str] = None,
        archiver: Any = _UNSET,
        archive_fn: Optional[Callable[..., Any]] = None,
        watcher_poll_interval: float = 1.0,
        messages: Optional[List[BaseMessage]] = None,
        messages_already_persisted: bool = False,
        initial_turn_count: int = 0,
    ) -> None:
        self.build = build
        self.host = host
        self.parent_context = parent_context
        self.subagent_id = subagent_id or str(uuid.uuid4())
        self.handle = build.handle
        self.subagent_type = build.subagent_type
        self.budgets = budgets or ChildBudgets.defaults_for(None)
        self.ledger: SubagentLedger = ledger if ledger is not None else NullLedger()
        self.clock = clock
        self.parent_tool_call_id = parent_tool_call_id
        self._archiver = archiver
        self._archive_fn = archive_fn
        self._watcher_poll_interval = watcher_poll_interval

        #: The child's durable history. A fork seed stays byte-for-byte free of
        #: the child's transient system prompt; ``_loop_messages`` adds that
        #: prompt only to the provider working view.
        self.messages: List[BaseMessage] = messages if messages is not None else []
        self._history_supplied = messages is not None
        self._seed_required = self._history_supplied and not messages_already_persisted
        self._seed_persist_attempted = False
        self._seed_persisted = not self._seed_required
        self._seed_persist_error: Optional[BaseException] = None
        self._loop_messages: Optional[List[BaseMessage]] = None
        self._transient_prompt: Optional[SystemMessage] = None
        self._transient_prompt_id = f"subagent_prompt_{uuid.uuid4().hex}"
        self.inbox: asyncio.Queue = asyncio.Queue()
        self.turn_done = asyncio.Event()
        self.hard_interrupt_event = asyncio.Event()

        # Activity stamps read by the staleness watcher (all on ``clock``).
        self.running = False
        self.last_activity = self.clock()
        self.in_tool_since: Optional[float] = None
        self.stale_armed_at: Optional[float] = None

        # Per-brief counters and flags.
        self.turn_number = max(0, int(initial_turn_count))
        self._initial_turn_count = self.turn_number
        self.loop_turns = 0
        self.provider_calls = 0
        self.tokens_in = 0
        self.tokens_out = 0
        self.tool_calls = 0
        self.errors: List[str] = []
        self.streamed: List[str] = []
        self.thinking: List[str] = []
        self.sudo_requested = False
        self._brief_start = 0
        self._brief_started_at = self.clock()
        self._interrupt: Optional[str] = None
        self._cap_reason: Optional[str] = None
        self._stale_soft: Optional[str] = None
        self._stale_hard = False
        self._stopped = False
        self._authority_lost = False
        self._synth_pending = False
        self._synth_reason = ""
        self._synth_turn = False
        self._steer_pending = 0
        self._finish_length = False
        self._loop_exception: Optional[BaseException] = None
        self._loop_cancelled = False
        self._audits: Dict[str, tuple[Optional[str], float]] = {}
        self._bg_tasks: set = set()
        self._loop_task: Optional[asyncio.Task] = None
        self._watcher_task: Optional[asyncio.Task] = None
        self._watcher = StalenessWatcher(
            self, self.budgets, poll_interval=watcher_poll_interval
        )
        self._log_token = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @property
    def tokens(self) -> int:
        return self.tokens_in + self.tokens_out

    @property
    def started(self) -> bool:
        return self._loop_task is not None

    @property
    def alive(self) -> bool:
        return self._loop_task is not None and not self._loop_task.done()

    @property
    def authority_lost(self) -> bool:
        """Whether an awaited provider/effect fence refused this driver."""
        return self._authority_lost

    def start(self) -> None:
        """Create the loop task (one task per child: task-local contextvars
        keep the reasoning sink per child) and the staleness watcher."""
        if self._loop_task is not None:
            return
        if self._seed_required and not self._seed_persisted:
            raise RuntimeError(
                f"subagent {self.handle}: fork seed must be durably persisted "
                "before the provider loop starts"
            )
        from agent.core.logging_config import bind_log_context, reset_log_context

        token = bind_log_context(
            thread_id=self.subagent_id,
            subagent_id=self.subagent_id,
            subagent_handle=self.handle,
        )
        try:
            self._loop_task = asyncio.create_task(
                self._run_loop(), name=f"subagent-loop-{self.handle}"
            )
            self._watcher_task = asyncio.create_task(
                self._watcher.run(), name=f"subagent-watch-{self.handle}"
            )
        finally:
            reset_log_context(token)
        self._loop_task.add_done_callback(self._on_loop_done)

    async def _run_loop(self) -> None:
        build = self.build
        if self._history_supplied:
            self._transient_prompt = SystemMessage(
                content=build.system_prompt,
                id=self._transient_prompt_id,
            )
            # These are the exact seed objects accepted by ``persist_seed``.
            # Only the list container and leading system prompt are transient.
            self._loop_messages = [self._transient_prompt, *self.messages]
        else:
            # Preserve U3's lightweight non-fork behavior: the persistent loop
            # installs its prompt in the otherwise empty in-memory history.
            self._loop_messages = self.messages
        try:
            await run_persistent_loop(
                llm_with_tools=build.llm_with_tools,
                tools=build.tools,
                context_manager=build.context_manager,
                config=build.config,
                system_prompt=build.system_prompt,
                callbacks=self.callbacks(),
                messages=self._loop_messages,
                auxiliary_llm=getattr(self.host, "auxiliary_llm", None),
                recall_store=None,
                knowledge_store=None,
                project_id=None,
                project_ids=None,
                tool_context=build.tool_context,
                initial_turn_count=self._initial_turn_count,
                get_current_tools=self.current_tools,
                get_current_context=None,
                get_current_system_prompt=lambda: build.system_prompt,
                memory_extraction_prompt="",
                memory_service=None,
                claim_memory_extraction_interval=None,
                defer_memory_extraction_to_outbox=False,
                memory_thread_id=self.subagent_id,
            )
        finally:
            self._sync_durable_messages_from_loop()

    def _is_transient_prompt(self, message: Any) -> bool:
        return getattr(message, "id", None) == self._transient_prompt_id

    def _ensure_transient_prompt(self) -> None:
        """Keep the child prompt ahead of a compacted durable summary."""
        if not self._history_supplied or self._loop_messages is None:
            return
        prompt = next(
            (msg for msg in self._loop_messages if self._is_transient_prompt(msg)),
            None,
        )
        if prompt is None:
            prompt = self._transient_prompt or SystemMessage(
                content=self.build.system_prompt,
                id=self._transient_prompt_id,
            )
            self._transient_prompt = prompt
        self._loop_messages[:] = [
            prompt,
            *(msg for msg in self._loop_messages if not self._is_transient_prompt(msg)),
        ]

    def _sync_durable_messages_from_loop(self) -> None:
        """Mirror the provider loop without its transient child prompt."""
        if not self._history_supplied or self._loop_messages is None:
            return
        self._ensure_transient_prompt()
        self.messages[:] = [
            msg for msg in self._loop_messages if not self._is_transient_prompt(msg)
        ]

    def _on_loop_done(self, task: asyncio.Task) -> None:
        self._sync_durable_messages_from_loop()
        if task.cancelled():
            self._loop_cancelled = True
        else:
            exc = task.exception()
            if exc is not None:
                self._loop_exception = exc
                logger.warning(
                    "subagent %s: loop task died: %s: %s",
                    self.handle,
                    type(exc).__name__,
                    exc,
                )
        self.running = False
        self.turn_done.set()

    def current_tools(self) -> tuple:
        """Re-read by the loop at every turn start: the tool-less binding
        during the forced synthesis turn, the real one otherwise."""
        if self._synth_turn:
            return self.build.llm, []
        return self.build.llm_with_tools, self.build.tools

    def _reset_brief(self) -> None:
        self.provider_calls = 0
        self.tokens_in = 0
        self.tokens_out = 0
        self.tool_calls = 0
        self.errors = []
        self.streamed = []
        self.thinking = []
        self.sudo_requested = False
        self._interrupt = None
        self._cap_reason = None
        self._stale_soft = None
        self.stale_armed_at = None
        self._synth_pending = False
        self._synth_reason = ""
        self._synth_turn = False
        self._steer_pending = 0
        self._finish_length = False
        self.in_tool_since = None
        self._brief_start = len(self.messages)
        self._brief_started_at = self.clock()
        self.last_activity = self._brief_started_at

    def _mint_id(self) -> str:
        return f"msg_{uuid.uuid4().hex[:24]}"

    def _item(self, content: str, role: str) -> Dict[str, Any]:
        # Only content/id/role — never delivery_id/claim_generation (U0 #9).
        return {"content": content, "id": self._mint_id(), "role": role}

    async def run(self, brief: str, *, role: str = "human") -> SubagentResult:
        """Deliver one brief, wait for the turn to settle, classify."""
        if self._stopped:
            self._reset_brief()
            return self.classify()
        await self._persist_seed_before_start()
        # ``graceful_stop`` can win while a fork seed is being persisted.  Do
        # not create the provider loop after that stop has already settled.
        if self._stopped:
            self._reset_brief()
            return self.classify()
        self.start()
        if not self.alive:
            raise RuntimeError(f"subagent {self.handle}: loop is not running")
        self._reset_brief()
        self.turn_done.clear()
        self.running = True
        self.inbox.put_nowait(self._item(brief, role))
        await self.turn_done.wait()
        self.running = False
        return self.classify()

    async def _persist_seed_before_start(self) -> None:
        """Persist a fork seed exactly once before any provider task exists."""
        if not self._seed_required or self._seed_persisted:
            return
        if self._seed_persist_attempted:
            error = self._seed_persist_error or RuntimeError(
                "the prior fork-seed persistence attempt did not succeed"
            )
            raise RuntimeError(
                f"subagent {self.handle}: fork seed is not durable"
            ) from error

        self._seed_persist_attempted = True
        try:
            persisted = await asyncio.wait_for(
                self.ledger.persist_seed(self.subagent_id, self.messages),
                timeout=_SEED_PERSIST_TIMEOUT_S,
            )
        except asyncio.CancelledError:
            self._seed_persist_error = RuntimeError(
                "fork-seed persistence was cancelled"
            )
            raise
        except Exception as exc:
            self._seed_persist_error = exc
            raise RuntimeError(
                f"subagent {self.handle}: fork seed persistence failed before "
                "provider start"
            ) from exc
        if persisted is not True:
            error = RuntimeError("fork-seed persistence returned no success receipt")
            self._seed_persist_error = error
            raise RuntimeError(
                f"subagent {self.handle}: fork seed persistence was refused before "
                "provider start"
            ) from error
        self._seed_persisted = True

    def steer(self, text: str) -> None:
        """Queue a ``role=event`` follow-up; mid-turn it ends the current turn
        at the next checkpoint and the steered turn continues the brief."""
        self.inbox.put_nowait(self._item(text, PERSIST_ROLE_EVENT))
        if self.running:
            self._steer_pending += 1
            self._interrupt = "graceful"

    async def stop(self, *, timeout: float = 10.0) -> None:
        """End the loop: hard-interrupt a running turn, then the STOP sentinel."""
        task = self._loop_task
        if task is None or task.done():
            await self._cancel_watcher()
            return
        if self.running:
            self._stopped = True
            self._interrupt = "hard"
            self.hard_interrupt_event.set()
        self.inbox.put_nowait(STOP)
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(
                "subagent %s: loop did not stop in %ss — cancelling",
                self.handle,
                timeout,
            )
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        except asyncio.CancelledError:
            raise
        except Exception:  # the loop's own exception is recorded by _on_loop_done
            pass
        await self._cancel_watcher()

    async def graceful_stop(
        self, reason: str = "parent requested stop", *, timeout: float = 10.0
    ) -> SubagentResult:
        """Ask a live brief for one tool-less partial, then hard-stop it.

        The synthesis uses the driver's existing cap/staleness path: the
        current turn is interrupted at a safe checkpoint, tools are removed,
        and one evidence-only answer is attempted.  The grace window is
        bounded; a stuck provider/tool is then stopped by :meth:`stop`.
        """
        self._stopped = True
        self._discard_pending_inputs()
        self._steer_pending = 0
        if self.running and self.alive:
            self._arm_synthesis(str(reason or "parent requested stop"))
            try:
                await asyncio.wait_for(self.turn_done.wait(), timeout=max(0.0, timeout))
            except asyncio.TimeoutError:
                pass
        # Once the graceful window expires, the hard interrupt should settle
        # promptly; do not accidentally wait a second full grace window.
        await self.stop(timeout=min(1.0, max(0.1, timeout)))
        return self.classify()

    def _discard_pending_inputs(self) -> None:
        """Drop queued steers before ordering the one stop synthesis turn."""
        saw_stop = False
        while True:
            try:
                item = self.inbox.get_nowait()
            except asyncio.QueueEmpty:
                break
            if item is STOP:
                saw_stop = True
        if saw_stop:
            self.inbox.put_nowait(STOP)

    async def close(self) -> None:
        """``stop()`` and release the child's environment."""
        await self.stop()
        await self.build.release()

    async def _cancel_watcher(self) -> None:
        task = self._watcher_task
        self._watcher_task = None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    # ------------------------------------------------------------------
    # Budgets / staleness (called by on_usage, archive_llm_call, the watcher)
    # ------------------------------------------------------------------

    def _arm_synthesis(self, reason: str) -> None:
        self._interrupt = "graceful"
        self._synth_pending = True
        self._synth_reason = reason

    def _arm_cap(self, which: str) -> None:
        if self._cap_reason or self._stale_soft or self._synth_turn:
            return
        self._cap_reason = which
        logger.info(
            "subagent %s: %s cap reached (%d calls, %d tokens) — forcing synthesis",
            self.handle,
            which,
            self.provider_calls,
            self.tokens,
        )
        self._arm_synthesis(_CAP_REASONS[which])

    def arm_stale(self, kind: str) -> None:
        """Soft stage: graceful stop + one synthesis turn (B.4)."""
        if self.stale_armed_at is not None:
            return
        self.stale_armed_at = self.clock()
        self._stale_soft = kind
        logger.warning("subagent %s: stale (%s) — forcing synthesis", self.handle, kind)
        if not self._synth_turn:
            self._arm_synthesis(_STALE_REASON)
        else:
            self._interrupt = "graceful"

    def escalate_stale(self, kind: str) -> None:
        """Hard stage: the child still has not returned — cancel it."""
        if self._stale_hard:
            return
        self._stale_hard = True
        logger.warning(
            "subagent %s: still stale (%s) after the grace period — hard interrupt",
            self.handle,
            kind,
        )
        self._interrupt = "hard"
        self.hard_interrupt_event.set()
        task = self._loop_task
        if task is not None and not task.done():
            task.cancel()

    def _activity(self) -> None:
        self.last_activity = self.clock()

    # ------------------------------------------------------------------
    # PersistentLoopCallbacks
    # ------------------------------------------------------------------

    async def get_user_input(self, *args: Any, **kwargs: Any) -> Any:
        item = await self.inbox.get()
        if item is STOP:
            raise asyncio.CancelledError
        self._activity()
        return item

    async def on_token(self, token: str, *args: Any, **kwargs: Any) -> None:
        self.streamed.append(token)
        self._activity()

    async def on_thinking(self, text: str, *args: Any, **kwargs: Any) -> None:
        self.thinking.append(text)
        self._activity()

    def _get_archiver(self) -> Any:
        if self._archiver is _UNSET:
            try:
                from agent.core.archiver import get_archiver

                self._archiver = get_archiver()
            except Exception:  # pragma: no cover - audit must never break a child
                self._archiver = None
        return self._archiver

    def _audit_metadata(self) -> Dict[str, Any]:
        parent_meta = getattr(self.host, "audit_metadata", None)
        meta = dict(parent_meta) if isinstance(parent_meta, dict) else {}
        meta.update(
            {
                "subagent_id": self.subagent_id,
                "subagent_handle": self.handle,
                "subagent_type": self.subagent_type,
            }
        )
        return meta

    def _parent_correlation_id(self) -> str:
        """UUID-shaped audit identity for either a job or a session thread."""
        value = getattr(self.host, "correlation_id", None)
        if value is None:
            value = getattr(self.host, "job_id", "")
        return str(value or "")

    def _requires_strict_persistence(self) -> bool:
        ref = getattr(self.host, "parent_ref", None)
        return getattr(ref, "kind", None) == "thread"

    async def on_tool_start(
        self, name: str, args: Dict[str, Any], call_id: str, *rest: Any, **kwargs: Any
    ) -> None:
        self.tool_calls += 1
        now = self.clock()
        self.last_activity = now
        self.in_tool_since = now
        audit_id: Optional[str] = None
        archiver = self._get_archiver()
        if archiver is not None:
            try:
                audit_id = archiver.audit_tool_call(
                    job_id=self._parent_correlation_id(),
                    agent_type=self.host.agent_type,
                    iteration=self.turn_number,
                    tool_name=name,
                    call_id=call_id,
                    arguments=dict(args or {}),
                    metadata=self._audit_metadata(),
                    phase=getattr(self.parent_context, "_current_phase", None),
                    phase_number=getattr(
                        self.parent_context, "_current_phase_number", None
                    ),
                )
            except Exception as e:  # never let audit break the child
                logger.debug("subagent %s: tool audit failed: %s", self.handle, e)
        self._audits[call_id] = (audit_id, now)

    async def on_tool_execution_start(self, *args: Any, **kwargs: Any) -> None:
        if not await self._effect_authority_open():
            raise SubagentAuthorityLost(
                f"subagent {self.handle}: parent authority was lost before tool effect"
            )
        self._activity()

    async def on_tool_result(
        self,
        name: str,
        result: str,
        call_id: str,
        is_error: bool = False,
        *rest: Any,
        **kwargs: Any,
    ) -> None:
        now = self.clock()
        self.last_activity = now
        self.in_tool_since = None
        audit_id, started = self._audits.pop(call_id, (None, now))
        archiver = self._get_archiver()
        if archiver is not None and audit_id:
            try:
                archiver.update_tool_result(
                    audit_id,
                    result or "",
                    success=not is_error,
                    latency_ms=int(max(0.0, now - started) * 1000),
                    error=(result or "")[:500] if is_error else None,
                )
            except Exception as e:  # never let audit break the child
                logger.debug(
                    "subagent %s: tool audit update failed: %s", self.handle, e
                )

    async def permission_check(
        self, name: str, args: Dict[str, Any], call_id: str, *rest: Any, **kwargs: Any
    ) -> PermissionOutcome:
        # The allowlist is the policy (B.9): every bound tool is approved and
        # no human is ever asked. NO_ANSWER can never originate here.
        return PermissionOutcome.APPROVED

    async def announce_permission_batch(self, *args: Any, **kwargs: Any) -> None:
        return None

    async def on_turn_start(self, turn_id: int, *args: Any, **kwargs: Any) -> None:
        self.turn_number = int(turn_id)
        self.loop_turns += 1
        self._activity()

    async def on_turn_complete(
        self, turn_id: int, metrics: Any = None, *args: Any, **kwargs: Any
    ) -> None:
        self._activity()

    async def on_context_compacted(self, *args: Any, **kwargs: Any) -> None:
        self._ensure_transient_prompt()
        self._sync_durable_messages_from_loop()
        self._activity()

    async def on_turn_settled(self, turn_id: int, *args: Any, **kwargs: Any) -> None:
        """The authoritative end-of-turn edge (after memory/commit work)."""
        self._sync_durable_messages_from_loop()
        self._activity()
        self.in_tool_since = None
        if self._synth_turn:
            # The forced synthesis turn just ended: the brief is over.
            self._synth_turn = False
            self._synth_pending = False
            self._finish_brief()
            return
        if self._synth_pending:
            self._synth_pending = False
            tail = self._tail_kind()
            if tail == "ai" and not self._stale_hard and not self._stopped:
                # The cap landed exactly on a final answer — nothing to
                # synthesise: the answer stands as completed. Disarm the
                # interrupt the loop never consumed.
                self._interrupt = None
                self._cap_reason = None
                self._stale_soft = None
                self._finish_brief()
                return
            self._synth_turn = True
            self._interrupt = None
            self.inbox.put_nowait(
                self._item(
                    SYNTH_PROMPT.format(reason=self._synth_reason), PERSIST_ROLE_EVENT
                )
            )
            return
        if self._steer_pending:
            # Each queued steer owns one continuation turn.  A counter (not a
            # bool) keeps multiple accepted events inside this tracked brief.
            self._steer_pending -= 1
            return
        self._finish_brief()

    def _finish_brief(self) -> None:
        self.running = False
        self.turn_done.set()

    async def on_error(self, message: str, *args: Any, **kwargs: Any) -> None:
        self.errors.append(str(message))
        self._activity()

    def check_interrupt(self, *args: Any, **kwargs: Any) -> Optional[str]:
        mode, self._interrupt = self._interrupt, None
        return mode

    async def persist_message(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        self._sync_durable_messages_from_loop()
        try:
            await asyncio.wait_for(
                self.ledger.persist_message(self.subagent_id, msg, self.turn_number),
                timeout=5.0,
            )
        except asyncio.TimeoutError:
            if self._requires_strict_persistence():
                raise
            logger.warning("subagent %s: transcript save timed out (5s)", self.handle)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if self._requires_strict_persistence():
                raise
            logger.warning(
                "subagent %s: transcript save failed (non-fatal): %s", self.handle, e
            )

    def archive_llm_call(
        self, prepared: Any, response: Any, metrics: Any, *args: Any, **kwargs: Any
    ) -> None:
        """Sync (the loop's contract): count the call, note truncation, and
        write the ``llm_requests`` row in a thread under the PARENT job."""
        self.provider_calls += 1
        self._activity()
        try:
            from shared.runtime.core.loader import _is_output_truncated

            finish = (getattr(response, "response_metadata", None) or {}).get(
                "finish_reason"
            )
            if _is_output_truncated(finish):
                self._finish_length = True
        except Exception:  # pragma: no cover - defensive
            pass
        metrics = dict(metrics or {})
        turn = self.turn_number
        call_index = self.provider_calls
        model = metrics.get("model") or getattr(
            getattr(self.build.config, "llm", None), "model", "unknown"
        )
        archive = self._archive_fn

        def _do() -> None:
            try:
                fn = archive
                if fn is None:
                    from agent.core.archiver import archive_llm_request as fn  # type: ignore[no-redef]
                fn(
                    job_id=self._parent_correlation_id(),
                    agent_type=self.host.agent_type,
                    messages=prepared,
                    response=response,
                    model=model,
                    latency_ms=metrics.get("latency_ms"),
                    iteration=turn,
                    call_type="subagent",
                    auxiliary_metadata={
                        "subagent_id": self.subagent_id,
                        "subagent_handle": self.handle,
                        "subagent_type": self.subagent_type,
                        "turn": turn,
                        "provider_call": call_index,
                    },
                    metadata={
                        "subagent_id": self.subagent_id,
                        "subagent_handle": self.handle,
                        "turn": turn,
                        "input_tokens": metrics.get("input_tokens"),
                        "output_tokens": metrics.get("output_tokens"),
                        "cached_tokens": metrics.get("cached_tokens"),
                    },
                )
            except Exception as e:
                logger.debug(
                    "subagent %s: llm_requests archive failed: %s", self.handle, e
                )

        try:
            task = asyncio.create_task(
                asyncio.to_thread(_do), name=f"subagent-archive-{self.handle}"
            )
            self._bg_tasks.add(task)
            task.add_done_callback(self._bg_tasks.discard)
        except RuntimeError:  # no running loop (sync test harness)
            _do()
        if self.provider_calls >= self.budgets.max_turns:
            self._arm_cap("turns")

    async def on_usage(self, usage: Dict[str, Any], *args: Any, **kwargs: Any) -> None:
        self.tokens_in += int(usage.get("input_tokens") or 0)
        self.tokens_out += int(usage.get("output_tokens") or 0)
        self._activity()
        if self.tokens >= self.budgets.max_tokens:
            self._arm_cap("tokens")

    async def on_workspace_upgrade_needed(
        self, freeze_data: Dict[str, Any], *args: Any, **kwargs: Any
    ) -> None:
        """A child sudo/upgrade request is the PARENT's freeze, tagged (B.9)."""
        self.sudo_requested = True
        request = getattr(self.parent_context, "request_freeze", None)
        if callable(request):
            request(
                {
                    **dict(freeze_data or {}),
                    "subagent_handle": self.handle,
                    "subagent_id": self.subagent_id,
                }
            )

    def before_provider_admission(self, *args: Any, **kwargs: Any) -> bool:
        return self._admission_open()

    async def before_provider_execution(self, *args: Any, **kwargs: Any) -> bool:
        """Exact awaited fence immediately before every provider attempt."""
        return await self._effect_authority_open()

    async def _effect_authority_open(self) -> bool:
        if not self._admission_open():
            return False
        probe = getattr(self.host, "effect_authority", None)
        if not callable(probe):
            self._authority_lost = True
            return False
        try:
            admitted = bool(await probe())
        except Exception:
            admitted = False
        if not admitted:
            self._authority_lost = True
            self._interrupt = "hard"
            self.hard_interrupt_event.set()
        return admitted and self._admission_open()

    def _admission_open(self) -> bool:
        try:
            return bool(self.host.provider_admission())
        except Exception:
            return False

    def callbacks(self) -> PersistentLoopCallbacks:
        return PersistentLoopCallbacks(
            get_user_input=self.get_user_input,
            on_token=self.on_token,
            on_thinking=self.on_thinking,
            on_tool_start=self.on_tool_start,
            on_tool_execution_start=self.on_tool_execution_start,
            on_tool_result=self.on_tool_result,
            permission_check=self.permission_check,
            announce_permission_batch=self.announce_permission_batch,
            on_turn_start=self.on_turn_start,
            on_turn_complete=self.on_turn_complete,
            on_turn_settled=self.on_turn_settled,
            on_error=self.on_error,
            check_interrupt=self.check_interrupt,
            on_workspace_upgrade_needed=self.on_workspace_upgrade_needed,
            on_context_compacted=self.on_context_compacted,
            persist_message=self.persist_message,
            archive_llm_call=self.archive_llm_call,
            on_usage=self.on_usage,
            hard_interrupt_event=self.hard_interrupt_event,
            before_provider_admission=self.before_provider_admission,
            before_provider_execution=self.before_provider_execution,
        )

    # ------------------------------------------------------------------
    # Classification
    # ------------------------------------------------------------------

    def _brief_messages(self) -> List[BaseMessage]:
        return self.messages[self._brief_start :]

    def _tail_kind(self) -> str:
        # A context entry rides the message before it; it is never the tail.
        msgs = [m for m in self._brief_messages() if not is_context_entry(m)]
        if not msgs:
            return "none"
        last = msgs[-1]
        if isinstance(last, ToolMessage):
            return "tool"
        if isinstance(last, AIMessage):
            return "ai_open" if getattr(last, "tool_calls", None) else "ai"
        if isinstance(last, HumanMessage):
            role = (getattr(last, "additional_kwargs", None) or {}).get(
                PERSIST_ROLE_KEY
            )
            return "event" if role == PERSIST_ROLE_EVENT else "human"
        return type(last).__name__

    def _inspect(self) -> tuple[Optional[str], Optional[Dict[str, Any]]]:
        """``(last assistant text, first unanswered tool call)`` of the brief."""
        msgs = self._brief_messages()
        answered = {
            getattr(m, "tool_call_id", None) for m in msgs if isinstance(m, ToolMessage)
        }
        open_call: Optional[Dict[str, Any]] = None
        text: Optional[str] = None
        seen_last_ai = False
        for message in reversed(msgs):
            if not isinstance(message, AIMessage):
                continue
            calls = list(getattr(message, "tool_calls", None) or [])
            unanswered = [tc for tc in calls if tc.get("id") not in answered]
            if not seen_last_ai:
                seen_last_ai = True
                if unanswered:
                    open_call = dict(unanswered[0])
                    continue  # its text is not an answer; look for the last one
            if calls:
                continue
            text = message_text(message.content)
            break
        return text, open_call

    @staticmethod
    def _split_placeholder(text: str) -> tuple[Optional[str], Optional[str]]:
        """``(kept text, error)`` for the loop's ⚠ placeholders; ``(text, None)``
        when the text is a real answer."""
        stripped = text.strip()
        if stripped.startswith(PLACEHOLDER_PREFIX):
            return None, stripped.splitlines()[0][:300]
        marker = text.find(TRUNCATION_MARKER)
        if marker >= 0:
            return text[:marker].rstrip(), "output truncated at the model's limit"
        return text, None

    def classify(self) -> SubagentResult:
        """Classify the brief's outcome from the durable history (spike §4)."""
        text, open_call = self._inspect()
        tail = self._tail_kind()
        error: Optional[str] = self.errors[-1] if self.errors else None
        if self._loop_exception is not None and error is None:
            exc = self._loop_exception
            error = f"{type(exc).__name__}: {exc}"
        partial = False
        if text is not None:
            kept, placeholder = self._split_placeholder(text)
            if placeholder is not None:
                error = error or placeholder
                text = kept
                partial = bool(kept)
                placeholder_hit = True
            else:
                placeholder_hit = False
        else:
            placeholder_hit = False
        has_text = bool(text)

        if getattr(self, "_authority_lost", False):
            status = "interrupted:authority"
            partial = has_text
            error = error or "the parent execution authority is no longer current"
        elif self._stopped:
            status = "interrupted:stopped"
            partial = has_text
        elif self._stale_hard:
            status = "interrupted:stale"
            partial = has_text
        elif not self._admission_open() and (error or open_call or not has_text):
            status = "interrupted:drain"
            partial = has_text
        elif error and not placeholder_hit:
            status = "error"
            partial = has_text
        elif tail == "tool":
            status = "error"
            error = (
                error or "the child's turn ended on a tool result (no assistant answer)"
            )
            partial = has_text
        elif open_call is not None:
            status = "parked"
            partial = has_text
        elif not has_text and not placeholder_hit:
            status = "error"
            error = error or "no assistant output"
        elif placeholder_hit:
            status = "error"
        elif self._finish_length:
            status = "error"
            error = error or "output truncated at the model's limit"
            partial = True
        elif self._stale_soft is not None:
            status = "interrupted:stale"
        elif self._cap_reason is not None:
            status = f"capped:{self._cap_reason}"
        else:
            status = "completed"

        return SubagentResult(
            status=status,
            text=text or "",
            turns=self.provider_calls,
            tokens=self.tokens,
            duration=max(0.0, self.clock() - self._brief_started_at),
            handle=self.handle,
            subagent_type=self.subagent_type,
            subagent_id=self.subagent_id,
            parked_call=open_call,
            sudo_requested=self.sudo_requested,
            error=error,
            partial=partial,
            tool_calls=self.tool_calls,
            loop_turns=self.loop_turns,
            streamed_text="".join(self.streamed),
            thinking="".join(self.thinking),
        )


__all__ = [
    "PLACEHOLDER_PREFIX",
    "STOP",
    "SYNTH_PROMPT",
    "TRUNCATION_MARKER",
    "SubagentDriver",
    "SubagentAuthorityLost",
    "SubagentResult",
    "message_text",
]
