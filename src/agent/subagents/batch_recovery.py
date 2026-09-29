"""Recover a session parent's abandoned delegation turn as one batch (WP2).

When the executor running a session dies while children it delegated to are
running or finished, its successor recovers them at attach, before any input
reaches the loop. The orchestrator lists the candidates together with one plan
per parent turn (``recovery_turns``): every ``delegate_agent`` call of the
turn, in provider order, with its class. For a turn whose plan has more than
one call, :class:`SessionTurnRecovery` makes ONE settle call. The orchestrator
then writes, in one transaction, a tool result for every call that has none
and one continuation that supersedes the abandoned input.

This module builds what the agent supplies: the members it must name
(``needs_entry``) and the text of the members that need it
(``needs_message``). An ended child gets its replayed report: the spill file,
or, when the spill is missing or unreadable (a ``none``-tier scratch
workspace dies with its process), the child's last assistant message from its
stored transcript. A child that was still live gets the interrupted envelope,
built from its last assistant text only. Every text is redacted before it is
cut, and its control markers are neutralised; the return budget is split by
the number of members that carry text (§5.7), passed explicitly: the
runtime's own batch size is process-local and 1 after a restart (F7). A
settle is first-write-wins, so what is sent here is what the parent keeps.

The plans themselves prove that the orchestrator can settle a batch: they and
the settle shipped together. An orchestrator that lists none (or lists them
malformed) gets per-child recovery for every candidate. Per-child recovery
also stays for a turn with exactly one call, for a plan the orchestrator could
not compute (``error``), and for candidates no plan covers. It never runs for
a turn whose plan has more than one call (§7), with one exception: when a
continuation for the turn already exists (``idempotent``) while candidates of
the turn are still listed, an older agent recovered part of the turn one child
at a time. The batch settle can write nothing more for that turn, so the
remaining children are recovered the way that agent would have finished it.

Recovery never constructs a provider (invariant 7).

Design: knowledge-base/knowledge/features/parallel_subagents.md §5.3–§5.7,
§6.2 and §7.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Sequence, Tuple
from uuid import UUID

from agent.core.tool_output_redaction import redact_tool_result
from agent.subagents.budgets import ChildBudgets
from agent.subagents.envelope import (
    REPORT_FROM_SPILL,
    REPORT_FROM_TRANSCRIPT,
    _cut_at_line,
    count_tokens,
    neutralise_control_markers,
    read_spilled_report,
    render_replay_envelope,
    report_path,
    return_budget,
    wrap_report,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from agent.subagents.runtime import SubagentRuntime

logger = logging.getLogger(__name__)

#: First line of the result of a call whose child was still live at the
#: restart (§5.5). The ``delegate_agent`` description names it.
INTERRUPTED_HEADER = "[delegate_agent: INTERRUPTED - no final report]"
#: The share of a member's return budget its interrupted tail may use. A tail
#: is partial and unverified; a report is the child's answer (§5.7).
INTERRUPTED_TAIL_SHARE = 0.25
PARENT_RESTART_STATUS = "interrupted"
PARENT_RESTART_OUTCOME = "interrupted:parent_restart"
PARENT_RESTART_ERROR = "the parent runtime restarted"
#: One transcript read or listing, as the single-child path bounds it.
LOAD_TIMEOUT_S = 5.0
#: One settle: a bounded transaction (at most 64 members, 1M characters).
SETTLE_TIMEOUT_S = 30.0

_CALL_ENDED = "ended"
_CALL_CLASSES = frozenset(
    {"delivered", "ended", "live", "not_started", "declined", "retired"}
)
_CALL_LIVE = "live"
_TEXT_BLOCK_TYPES = frozenset({"text", "output_text"})


class BatchRecoveryError(RuntimeError):
    """The turn could not be settled; recovery stays incomplete and re-runs."""


# ---------------------------------------------------------------------------
# Member text
# ---------------------------------------------------------------------------


def _text_of(content: Any) -> str:
    """The assistant text of a message's content, without tool or reasoning parts."""

    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif (
                isinstance(block, Mapping)
                and block.get("type") in _TEXT_BLOCK_TYPES
                and isinstance(block.get("text"), str)
            ):
                parts.append(block["text"])
        return "\n".join(parts)
    return ""


def last_assistant_text(messages: Sequence[Any]) -> str:
    """The most recent assistant text of a child transcript.

    Tool output is never a child's result (``envelope``), so a tool message
    is skipped, and so is an assistant message that carries only tool calls.
    """

    for message in reversed(list(messages or [])):
        if str(getattr(message, "type", "")) != "ai":
            continue
        text = _text_of(getattr(message, "content", "")).strip()
        if text:
            return text
    return ""


def tail_within(
    text: str, budget_tokens: int, *, model: Optional[str] = None
) -> Tuple[str, bool]:
    """The end of ``text`` within ``budget_tokens``; ``True`` when cut."""

    total = count_tokens(text, model)
    budget = max(1, int(budget_tokens))
    if total <= budget:
        return text, False
    chars = int(budget * len(text) / max(1, total))
    return _cut_at_line(text, chars, from_end=True).lstrip("\n"), True


def interrupted_envelope(
    *,
    handle: str,
    subagent_type: str,
    turns: int,
    tokens: int,
    tail: str,
    tail_cut: bool,
    report_file: Optional[str] = None,
) -> str:
    """The result of a call whose child was still live at the restart (§5.5).

    Worded per call: a batch puts several of them into one tool round, next
    to the reports of the children that finished. ``report_file`` is the
    child's spilled report when it exists although the child never recorded
    its end (it died between the spill and the terminal write).
    """

    lines = [
        INTERRUPTED_HEADER,
        f"handle: {handle}   type: {subagent_type}",
    ]
    if report_file:
        lines.append(
            "This subagent was still working when the process running this "
            "conversation was replaced. Its end was never recorded, but its "
            f"report file exists: {report_file}. It may have finished just "
            "before the interruption; read that file before you decide what is "
            "still missing. This was an infrastructure interruption, not a "
            "failure of the task, and nobody cancelled it."
        )
    else:
        lines.append(
            "This subagent was still working when the process running this "
            "conversation was replaced. It did not finish and produced no final "
            "report. This was an infrastructure interruption, not a failure of "
            "the task, and nobody cancelled it."
        )
    lines.append(
        f"Progress before the interruption: {int(turns)} turns, {int(tokens):,} tokens."
    )
    if tail.strip():
        body = f"[… earlier text omitted …]\n{tail}" if tail_cut else tail
        lines.append("Last message from the subagent (partial and unverified):")
        lines.append(wrap_report(handle, body))
    else:
        lines.append("It left no message before the interruption.")
    lines.append(
        "Anything it changed in the workspace is still there and may be incomplete."
    )
    lines.append(
        "If this work is still needed, first check the workspace for what it "
        "already produced. Then either finish the remainder yourself or call "
        "delegate_agent again with a task limited to what is still missing."
    )
    return "\n".join(lines)


def _timestamp(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return value


def replay_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    """A listed candidate (the roster payload) in the ledger-row shape the
    replay envelope reads."""

    from agent.subagents.runtime import _orphan_row_counter

    return {
        "subagent_handle": row.get("handle") or row.get("subagent_handle"),
        "subagent_type": row.get("subagent_type"),
        # A roster row names the child's own status ``status``; a ledger row
        # names it ``subagent_status`` and uses ``status`` for the thread.
        "subagent_status": row.get("subagent_status") or row.get("status"),
        "subagent_outcome": row.get("subagent_outcome") or row.get("outcome"),
        "subagent_error": row.get("subagent_error") or row.get("error"),
        "total_turns": _orphan_row_counter(row, "turns"),
        "total_tokens": _orphan_row_counter(row, "tokens"),
        "report_path": row.get("report_path"),
        "created_at": _timestamp(row.get("started_at") or row.get("created_at")),
        "ended_at": _timestamp(row.get("ended_at")),
    }


# ---------------------------------------------------------------------------
# Plans
# ---------------------------------------------------------------------------


TurnKey = Tuple[str, int]


def turn_key(value: Mapping[str, Any]) -> Optional[TurnKey]:
    """``(parent_input_message_id, parent_iteration)`` of a row or a plan."""

    iteration = value.get("parent_iteration")
    if isinstance(iteration, bool) or not isinstance(iteration, int) or iteration <= 0:
        return None
    try:
        return str(UUID(str(value.get("parent_input_message_id")))), iteration
    except (TypeError, ValueError, AttributeError):
        return None


def _uuid_text(value: Any) -> Optional[str]:
    try:
        return str(UUID(str(value)))
    except (TypeError, ValueError, AttributeError):
        return None


def _call_malformed(call: Any) -> Optional[str]:
    if not isinstance(call, Mapping):
        return "a call is not an object"
    if call.get("class") not in _CALL_CLASSES:
        return f"a call has an unknown class {call.get('class')!r}"
    if not isinstance(call.get("needs_entry"), bool) or not isinstance(
        call.get("needs_message"), bool
    ):
        return "a call does not say which members it needs"
    if call.get("needs_message") and not call.get("needs_entry"):
        return "a call needs text but names no member"
    if call.get("needs_entry") and (
        _uuid_text(call.get("thread_id")) is None
        or _uuid_text(call.get("runtime_generation")) is None
    ):
        return "a member has no exact child generation"
    if not isinstance(call.get("tool_call_id"), str):
        return "a call has no id"
    return None


def plans_by_turn(plans: Sequence[Any]) -> Dict[TurnKey, Mapping[str, Any]]:
    """The plans keyed by turn; :class:`BatchRecoveryError` when malformed."""

    keyed: Dict[TurnKey, Mapping[str, Any]] = {}
    for plan in plans:
        if not isinstance(plan, Mapping):
            raise BatchRecoveryError("a session recovery plan is not an object")
        key = turn_key(plan)
        if key is None:
            raise BatchRecoveryError("a session recovery plan names no exact turn")
        if plan.get("error") is None:
            calls = plan.get("calls")
            if not isinstance(calls, list):
                raise BatchRecoveryError("a session recovery plan has no calls")
            for call in calls:
                problem = _call_malformed(call)
                if problem is not None:
                    raise BatchRecoveryError(f"a session recovery plan: {problem}")
        keyed[key] = plan
    return keyed


def batchable(plan: Optional[Mapping[str, Any]]) -> bool:
    """Whether the turn is settled as one batch: more than one delegation call.

    That includes a turn with a single child after a partial approval: the
    per-child path must never run for a turn whose manifest has more than one
    call (§7).
    """

    if plan is None or plan.get("error") is not None:
        return False
    calls = plan.get("calls")
    return isinstance(calls, list) and len(calls) > 1


# ---------------------------------------------------------------------------
# The settle of one turn
# ---------------------------------------------------------------------------


def _row_thread_id(row: Mapping[str, Any]) -> str:
    return _uuid_text(row.get("thread_id") or row.get("id")) or ""


class SessionTurnRecovery:
    """The batch settles of one recovery pass, turn by turn.

    ``recover(row)`` answers ``(entries, handled)``. The first row of a
    batchable turn settles the whole turn and brings its entries; a row the
    settle named is then handled, and a row that is not the settle's member is
    left to the single-child path (``handled`` false), with a warning.
    """

    def __init__(
        self,
        runtime: "SubagentRuntime",
        rows: Sequence[Mapping[str, Any]],
        plans: Dict[TurnKey, Mapping[str, Any]],
    ) -> None:
        self.runtime = runtime
        self._rows: Dict[str, Mapping[str, Any]] = {}
        self._remember(rows)
        # The children the orchestrator lists now; a stale settle refreshes it.
        self._listed = set(self._rows)
        self._plans = dict(plans)
        # turn -> "settled" (the batch settle closed it) or "per_child" (an
        # existing continuation left its listed children to the old path).
        self._done: Dict[TurnKey, str] = {}
        # turn -> the children its settle named.
        self._named: Dict[TurnKey, set[str]] = {}

    @classmethod
    def for_runtime(
        cls,
        runtime: "SubagentRuntime",
        rows: Sequence[Mapping[str, Any]],
        plans: Optional[Sequence[Any]],
    ) -> Optional["SessionTurnRecovery"]:
        """Batch recovery for this pass, or ``None`` when it does not apply.

        It applies when the orchestrator listed plans, which it does only
        when it can settle a batch, and the ledger can settle. Malformed plans
        are ignored with a warning: every candidate then recovers on its own.
        The attach capability is not consulted: it gates creating wider
        batches, and a pod that attached without it may still inherit one.
        """

        if plans is None:
            return None
        if not callable(getattr(runtime.ledger, "settle_batch", None)):
            return None
        try:
            keyed = plans_by_turn(plans)
        except BatchRecoveryError as exc:
            logger.warning(
                "session recovery plans are malformed (%s); every child is "
                "recovered on its own",
                exc,
            )
            return None
        return cls(runtime, rows, keyed)

    def _remember(self, rows: Sequence[Mapping[str, Any]]) -> None:
        for row in rows:
            thread_id = _row_thread_id(row)
            if thread_id:
                self._rows[thread_id] = row

    async def recover(
        self, row: Mapping[str, Any]
    ) -> Tuple[List[Dict[str, Any]], bool]:
        key = turn_key(row)
        if key is None:
            return [], False
        entries: List[Dict[str, Any]] = []
        if key not in self._done:
            plan = self._plans.get(key)
            if plan is None or not batchable(plan):
                return [], False
            entries = await self._settle(key, plan)
        if self._done[key] == "per_child":
            return entries, False
        thread_id = _row_thread_id(row)
        if thread_id in self._named.get(key, set()):
            return entries, True
        if thread_id not in self._listed:
            # A stale settle's fresh listing no longer shows this child: it
            # was closed in the meantime and is owed nothing.
            logger.info(
                "session child %s of turn %s/%s is no longer listed; skipped",
                thread_id,
                key[0],
                key[1],
            )
            return entries, True
        logger.warning(
            "session child %s is listed under turn %s/%s but is not a member of "
            "its settled plan; recovering it on its own",
            thread_id,
            key[0],
            key[1],
        )
        return entries, False

    async def _settle(
        self, key: TurnKey, plan: Mapping[str, Any]
    ) -> List[Dict[str, Any]]:
        result = await self._send(plan)
        if result.get("result") == "stale":
            # The server's view differs from the listing (a child moved in
            # between). Nothing was written; list again and retry once.
            logger.warning(
                "session batch settle for input %s turn %s was stale (%s); "
                "listing again",
                key[0],
                key[1],
                result.get("reason"),
            )
            plan = await self._relist(key)
            if plan is None:
                # The turn is no longer owed anything.
                self._done[key] = "settled"
                self._named[key] = set()
                return []
            if not batchable(plan):
                raise BatchRecoveryError(
                    "a stale delegation turn changed its shape; recovery re-runs"
                )
            result = await self._send(plan)
            if result.get("result") == "stale":
                raise BatchRecoveryError(
                    "session batch settle stayed stale; recovery re-runs"
                )
        verdict = str(result.get("result") or "")
        members = [call for call in plan["calls"] if call.get("needs_entry")]
        if verdict == "idempotent" and members:
            # A continuation for this input already exists, but children of
            # the turn are still owed: an older agent recovered part of the
            # turn one child at a time. The settle cannot write more for this
            # input, so the remaining children finish on that same path.
            logger.warning(
                "session delegation turn %s/%s already has continuation %s; "
                "recovering its %d remaining child(ren) one at a time",
                key[0],
                key[1],
                result.get("delivery_id"),
                len(members),
            )
            self._done[key] = "per_child"
            return []
        self._done[key] = "settled"
        self._named[key] = {_uuid_text(call.get("thread_id")) or "" for call in members}
        delivery_id = result.get("delivery_id") if verdict == "applied" else None
        logger.info(
            "session delegation turn %s/%s settled as one batch: %s "
            "(%d call(s), %d member(s), continuation=%s)",
            key[0],
            key[1],
            verdict,
            len(plan["calls"]),
            len(members),
            delivery_id,
        )
        return [
            {
                "handle": call.get("handle"),
                "thread_id": str(call.get("thread_id")),
                "status": (
                    str(call.get("subagent_status") or "")
                    if call.get("class") == _CALL_ENDED
                    else PARENT_RESTART_STATUS
                ),
                "run_in_background": False,
                "delivery_id": delivery_id,
                "supersedes_input_seq": result.get("supersedes_input_seq"),
            }
            for call in members
        ]

    async def _relist(self, key: TurnKey) -> Optional[Mapping[str, Any]]:
        lister = getattr(self.runtime.ledger, "list_live_recovery", None)
        if not callable(lister):
            raise BatchRecoveryError("session recovery cannot list its plans again")
        listed = await asyncio.wait_for(
            lister(self.runtime._parent_ref().id), timeout=LOAD_TIMEOUT_S
        )
        plans = listed.get("recovery_turns") if isinstance(listed, Mapping) else None
        if plans is None:
            raise BatchRecoveryError("session recovery lost its plans")
        rows = listed.get("subagents") or []
        self._remember(rows)
        self._listed = {_row_thread_id(row) for row in rows}
        fresh = plans_by_turn(plans)
        # Only this turn is decided from the new listing; another turn keeps
        # the plan it was listed with, and its own settle checks it.
        self._plans.pop(key, None)
        if key in fresh:
            self._plans[key] = fresh[key]
        return fresh.get(key)

    async def _send(self, plan: Mapping[str, Any]) -> Mapping[str, Any]:
        members = await self._members(plan)
        key = turn_key(plan)
        if key is None:
            raise BatchRecoveryError("a session recovery plan names no exact turn")
        result = await asyncio.wait_for(
            self.runtime.ledger.settle_batch(
                parent_input_message_id=key[0],
                parent_iteration=key[1],
                members=members,
            ),
            timeout=SETTLE_TIMEOUT_S,
        )
        if not isinstance(result, Mapping) or str(result.get("result") or "") not in {
            "applied",
            "idempotent",
            "already_delivered",
            "nothing_to_recover",
            "stale",
        }:
            raise BatchRecoveryError("session batch settle returned no verdict")
        return result

    async def _members(self, plan: Mapping[str, Any]) -> List[Dict[str, Any]]:
        """One entry per member the plan asks for, in provider order."""

        from agent.subagents.runtime import _orphan_row_counter

        calls = plan["calls"]
        texts = sum(1 for call in calls if call.get("needs_message"))
        probe = self.runtime.host.context_probe()
        model = self.runtime._parent_model()
        members: List[Dict[str, Any]] = []
        for call in calls:
            if not call.get("needs_entry"):
                continue
            try:
                thread_id = str(UUID(str(call.get("thread_id"))))
                generation = str(UUID(str(call.get("runtime_generation"))))
            except (TypeError, ValueError, AttributeError) as exc:
                raise BatchRecoveryError(
                    "a batch member has no exact child generation"
                ) from exc
            row = self._rows.get(thread_id)
            if row is None:
                raise BatchRecoveryError("a batch member is missing from the live list")
            handle = str(call.get("handle") or row.get("handle") or "").strip()
            subagent_type = str(
                call.get("subagent_type") or row.get("subagent_type") or "unknown"
            )
            member: Dict[str, Any] = {
                "thread_id": thread_id,
                "runtime_generation": generation,
            }
            report_file = None
            if call.get("class") == _CALL_ENDED:
                # The terminal facts are stored; state only the status.
                member["subagent_status"] = str(call.get("subagent_status") or "")
                member["outcome"] = call.get("outcome")
            else:
                if self.runtime._report_exists(handle):
                    report_file = report_path(handle)
                member.update(
                    subagent_status=PARENT_RESTART_STATUS,
                    outcome=PARENT_RESTART_OUTCOME,
                    turns=_orphan_row_counter(row, "turns"),
                    tokens=_orphan_row_counter(row, "tokens"),
                    report_path=report_file or row.get("report_path") or None,
                    error=PARENT_RESTART_ERROR,
                )
            if call.get("needs_message"):
                member["message"] = await self._text(
                    call,
                    row,
                    handle=handle,
                    subagent_type=subagent_type,
                    thread_id=thread_id,
                    report_file=report_file,
                    n_in_batch=texts,
                    probe=probe,
                    model=model,
                )
            members.append(member)
        return members

    async def _transcript_text(self, thread_id: str) -> str:
        """The child's last assistant text from its stored transcript."""

        loader = getattr(self.runtime.ledger, "load_messages", None)
        if not callable(loader):
            raise BatchRecoveryError("session batch recovery cannot read a transcript")
        transcript = await asyncio.wait_for(loader(thread_id), timeout=LOAD_TIMEOUT_S)
        return last_assistant_text(getattr(transcript, "messages", None) or [])

    async def _text(
        self,
        call: Mapping[str, Any],
        row: Mapping[str, Any],
        *,
        handle: str,
        subagent_type: str,
        thread_id: str,
        report_file: Optional[str],
        n_in_batch: int,
        probe: Any,
        model: Optional[str],
    ) -> str:
        from agent.subagents.runtime import _orphan_row_counter

        context = self.runtime.parent_context
        workspace = getattr(context, "workspace_manager", None)
        entry = self.runtime.roster.get(subagent_type) or {}
        entry_budget = ChildBudgets.from_entry(
            entry, subagent_type
        ).return_budget_tokens
        if call.get("class") == _CALL_ENDED:
            # The spill first; a successor may not have it (a ``none``-tier
            # scratch workspace dies with its process) or may fail to read
            # it. The settle is first-write-wins, so fall back to the report
            # in the child's stored transcript rather than store "report
            # unavailable" for good and make the parent pay for it again.
            replayed = replay_row(row)
            report = read_spilled_report(replayed, workspace)
            source = REPORT_FROM_SPILL
            if report is None:
                report = await self._transcript_text(thread_id)
                source = REPORT_FROM_TRANSCRIPT
                logger.warning(
                    "subagent %s: spilled report unreadable after the restart; %s",
                    handle,
                    "replaying its transcript" if report else "nothing to replay",
                )
            return redact_tool_result(
                render_replay_envelope(
                    replayed,
                    tool_call_id=str(call.get("tool_call_id") or ""),
                    # Redacted before the envelope cuts it (as the tail below).
                    text=redact_tool_result(report, context) if report else None,
                    source=source,
                    entry_budget=entry_budget,
                    probe=probe,
                    n_in_batch=n_in_batch,
                    model=model,
                ),
                context,
            )
        if call.get("class") != _CALL_LIVE:
            raise BatchRecoveryError(
                f"a {call.get('class')!r} call cannot carry a recovered message"
            )
        # Redact before cutting, so a cut can never split a credential out of
        # the pattern that recognizes it; then quote harness markers.
        partial = neutralise_control_markers(
            redact_tool_result(await self._transcript_text(thread_id), context)
        )
        budget = return_budget(entry_budget, probe, n_in_batch)
        tail, cut = tail_within(
            partial, max(1, int(budget * INTERRUPTED_TAIL_SHARE)), model=model
        )
        text = interrupted_envelope(
            handle=handle,
            subagent_type=subagent_type,
            turns=_orphan_row_counter(row, "turns"),
            tokens=_orphan_row_counter(row, "tokens"),
            tail=tail,
            tail_cut=cut,
            report_file=report_file,
        )
        return redact_tool_result(text, context)


__all__ = [
    "BatchRecoveryError",
    "INTERRUPTED_HEADER",
    "INTERRUPTED_TAIL_SHARE",
    "SessionTurnRecovery",
    "batchable",
    "interrupted_envelope",
    "last_assistant_text",
    "plans_by_turn",
    "replay_row",
    "tail_within",
    "turn_key",
]
