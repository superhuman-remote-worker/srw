"""A stateless turn that compacts mid-turn settles against durable truth.

knowledge-base/knowledge/issues/stateless_turn_settlement_crashes_after_midturn_compaction.md
§Fix item 6: the unit tests pin each piece (identity-preserving compaction,
membership stamps, membership selection) against doubles. This drives the
composed path — the real persistent loop, the real identity-preserving
``ContextManager`` summarising in the middle of a tool-heavy turn, the
incremental per-message persist, then the authoritative turn-end reconcile
(``_save_turn_ai_messages``) — against an in-memory ``thread_messages`` that
models the batch upsert's ``ON CONFLICT (id)`` semantics.

What the settlement owes the durable transcript:
- it does not raise (the original crash: the summary evicted the input the
  reconcile walked back to);
- every reconciled row lands on a row the incremental writer already made
  (a fresh id is a duplicate transcript row);
- no reconciled row rewrites a durable row's content with a compaction view
  (the working set is lossy; the transcript is the source of truth);
- a row whose incremental write was lost is still filled in — a tool call
  left without its result wedges the session;
- the memory effect's range ends at the turn's final answer.
"""

import asyncio
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import AIMessage, SystemMessage, ToolMessage

from agent.api.persistent_app import _save_turn_ai_messages
from agent.core.context import ContextConfig, ContextManager, ConversationSummary
from agent.core.thread_messages import _serialize_message_row
from agent.persistent_graph import PersistentLoopCallbacks, run_persistent_loop
from shared.row_identity import _coerce_row_id

THREAD_ID = "11111111-2222-3333-4444-555555555555"
INPUT_ID = "66666666-7777-8888-9999-000000000000"
# The message-count trigger (below) fires exactly once, before the model call
# that follows the fourth tool result: [system, input, 4 x (call, result)] is
# 10 messages > 8. The summary covers the input and the first two pairs; the
# kept window (4 messages) holds calls tc2/tc3 and their results, both above
# the keep-window cap, so both results are capped. The next call answers.
TOOL_CALLS = 4
CAPPED_CALLS = ("tc2", "tc3")
TOOL_RESULT = "row-data " * 80
FINAL_ANSWER = "Final answer: all rows read."


@pytest.fixture(autouse=True)
def _fast_summarizer_backoff(monkeypatch):
    monkeypatch.setattr("agent.core.summarizer.BACKOFF_SECONDS", (0.0, 0.0))


class _Transcript:
    """In-memory ``thread_messages``: rows keyed by coerced id, in seq order."""

    def __init__(self, lose_writes_for: tuple = ()) -> None:
        self.rows: Dict[str, Dict[str, Any]] = {}
        self.reconciled: List[Dict[str, Any]] = []
        self.boundary: Dict[str, Any] = {}
        self._lose_writes_for = set(lose_writes_for)

    def _write(self, row: Dict[str, Any], *, insert_if_absent: bool = False) -> str:
        row_id = _coerce_row_id(row.get("id"))
        existing = self.rows.get(row_id)
        if existing is not None and insert_if_absent:
            return row_id  # ON CONFLICT (id) DO NOTHING
        seq = existing["seq"] if existing else len(self.rows) + 1
        self.rows[row_id] = {**row, "id": row_id, "seq": seq}
        return row_id

    async def save_thread_message(self, *, thread_id: str, **row: Any) -> None:
        assert thread_id == THREAD_ID
        if row.get("tool_call_id") in self._lose_writes_for:
            raise ConnectionError("incremental write lost")
        self._write(row)

    async def save_thread_messages(
        self,
        thread_id: str,
        rows: List[Dict[str, Any]],
        *,
        turn_input_message_id: str,
        turn_number: int,
        memory_scope_kind: str,
        memory_scope_id: str,
    ) -> str:
        assert thread_id == THREAD_ID
        input_row_id = _coerce_row_id(turn_input_message_id)
        # The DB-side fail-closed check: the accepted input row must exist.
        if input_row_id not in self.rows:
            raise ValueError("exact live turn-boundary message was not found")
        before = {key: dict(value) for key, value in self.rows.items()}
        reconciled_ids = [input_row_id]
        for row in rows:
            row_id = self._write(
                row, insert_if_absent=row.get("insert_if_absent") is True
            )
            self.reconciled.append(
                {"before": before.get(row_id), "after": dict(self.rows[row_id])}
            )
            reconciled_ids.append(row_id)
        self.boundary = {
            "turn_number": turn_number,
            "end_seq": max(self.rows[i]["seq"] for i in reconciled_ids),
        }
        return "effect-producer"

    def unpaired_tool_calls(self) -> set:
        calls = {
            call["id"]
            for row in self.rows.values()
            if row["role"] == "ai"
            for call in (row.get("tool_calls") or [])
        }
        results = {
            row["tool_call_id"] for row in self.rows.values() if row["role"] == "tool"
        }
        return calls ^ results


def _aux() -> Any:
    from shared.runtime.services.auxiliary import AuxiliaryLLM

    structured = AsyncMock()
    structured.ainvoke = AsyncMock(
        return_value={
            "raw": AIMessage(content="structured output"),
            "parsed": ConversationSummary(
                summary="Read the rows one by one.",
                tasks_completed="- rows read",
                key_decisions="",
                current_state="answering",
                blockers="",
            ),
            "parsing_error": None,
        }
    )
    llm = MagicMock()
    llm.with_structured_output = MagicMock(return_value=structured)
    return AuxiliaryLLM(llm=llm, max_context_tokens=50_000)


def _llm() -> MagicMock:
    replies = [
        AIMessage(
            content="",
            tool_calls=[{"name": "read_rows", "args": {"page": i}, "id": f"tc{i}"}],
        )
        for i in range(TOOL_CALLS)
    ]
    replies.append(AIMessage(content=FINAL_ANSWER))
    queue = iter(replies)

    async def _astream(_messages, **_kwargs):
        yield next(queue)

    llm = MagicMock(reasoning=None)
    llm.astream = _astream
    return llm


def _tool() -> MagicMock:
    tool = MagicMock()
    tool.name = "read_rows"
    tool.ainvoke = AsyncMock(return_value=TOOL_RESULT)
    return tool


def _config() -> MagicMock:
    config = MagicMock()
    config.llm.timeout = 30
    config.memory.enabled = False
    config.memory.observer_interval = 5
    config.context_management.max_summary_length = 10_000
    config.officer.enabled = False
    return config


def _context_manager() -> ContextManager:
    # The persistent session's shape (persistent_session._setup_context_manager):
    # identity-preserving, since the loop adopts a compaction wholesale. The
    # token gate is out of reach; the message-count gate is the trigger.
    return ContextManager(
        config=ContextConfig(
            compaction_threshold_tokens=1_000_000,
            summarization_threshold_tokens=1_000_000,
            message_count_threshold=8,
            message_count_min_tokens=0,
            keep_recent_messages=4,
            keep_recent_tool_results=2,
            keep_window_max_tool_result_chars=200,
            model_max_context_tokens=200_000,
        ),
        model="gpt-4",
        preserve_message_identity=True,
    )


async def _run_one_compacting_turn(
    lose_writes_for: tuple = (),
) -> tuple[_Transcript, list, ContextManager]:
    transcript = _Transcript(lose_writes_for)
    manager = _context_manager()
    messages: list = []
    settled: list = []
    queue = iter([("Read every row, then answer.", INPUT_ID)])

    async def _input():
        try:
            text, input_id = next(queue)
        except StopIteration:
            raise asyncio.CancelledError from None
        return {"content": text, "id": input_id}

    async def _persist(msg: Any) -> bool:
        # The incremental writer (persistent_app._loop_persist_message):
        # best-effort, a failed write is logged and the turn goes on.
        try:
            await transcript.save_thread_message(
                thread_id=THREAD_ID, **_serialize_message_row(msg, 1)
            )
        except ConnectionError:
            return False
        return True

    async def _on_turn_complete(
        turn_id, metrics, input_id, scope_kind, scope_id, **_kwargs
    ) -> None:
        # The stateless settlement (persistent_app._reconcile_turn_with_retry).
        await _save_turn_ai_messages(
            transcript,
            THREAD_ID,
            messages,
            turn_id,
            metrics=metrics,
            authoritative_turn_boundary=True,
            turn_input_message_id=input_id,
            memory_scope_kind=scope_kind,
            memory_scope_id=scope_id,
        )
        settled.append(turn_id)

    callbacks = PersistentLoopCallbacks(
        get_user_input=_input,
        on_token=AsyncMock(),
        on_thinking=AsyncMock(),
        on_tool_start=AsyncMock(),
        on_tool_result=AsyncMock(),
        permission_check=AsyncMock(return_value=True),
        on_turn_start=AsyncMock(),
        on_turn_complete=_on_turn_complete,
        on_error=AsyncMock(),
        check_interrupt=MagicMock(return_value=False),
        persist_message=_persist,
    )
    await run_persistent_loop(
        llm_with_tools=_llm(),
        tools=[_tool()],
        context_manager=manager,
        config=_config(),
        system_prompt="system",
        callbacks=callbacks,
        messages=messages,
        auxiliary_llm=_aux(),
        memory_thread_id=THREAD_ID,
    )
    assert settled == [1], "the compacting turn must settle"
    callbacks.on_error.assert_not_awaited()
    assert manager.compaction_runs == 1, "the scenario compacts exactly once"
    capped = sorted(
        m.tool_call_id
        for m in messages
        if isinstance(m, ToolMessage) and m.content != TOOL_RESULT
    )
    assert capped == list(CAPPED_CALLS), "the kept window's results were capped"
    return transcript, messages, manager


def _tool_row(transcript: _Transcript, call_id: str) -> Optional[Dict[str, Any]]:
    rows = [
        row
        for row in transcript.rows.values()
        if row["role"] == "tool" and row["tool_call_id"] == call_id
    ]
    assert len(rows) <= 1, f"duplicate result rows for {call_id}"
    return rows[0] if rows else None


class TestMidTurnCompactionSettlement:
    @pytest.mark.asyncio
    async def test_turn_settles_after_a_summary_covered_its_input(self):
        transcript, messages, _manager = await _run_one_compacting_turn()

        summary_at = next(
            i
            for i, m in enumerate(messages)
            if isinstance(m, SystemMessage) and "[Summary of prior work]" in m.content
        )
        # The input sat in the summarised region; its pin re-seated it
        # verbatim right after the recap, with its accepted id.
        assert messages[summary_at + 1].id == INPUT_ID
        assert transcript.reconciled, "the turn's rows are reconciled"
        final = next(
            row
            for row in transcript.rows.values()
            if row["role"] == "ai" and row["content"] == FINAL_ANSWER
        )
        assert transcript.boundary == {"turn_number": 1, "end_seq": final["seq"]}

    @pytest.mark.asyncio
    async def test_reconcile_lands_only_on_incrementally_written_rows(self):
        transcript, _messages, _manager = await _run_one_compacting_turn()

        fresh = [
            entry["after"] for entry in transcript.reconciled if not entry["before"]
        ]
        assert fresh == [], "a reconciled row with a new id duplicates the transcript"
        roles = [row["role"] for row in transcript.rows.values()]
        assert roles.count("tool") == TOOL_CALLS
        assert roles.count("human") == 1
        assert transcript.unpaired_tool_calls() == set()

    @pytest.mark.asyncio
    async def test_reconcile_never_writes_a_compaction_view_over_a_durable_row(
        self,
    ):
        transcript, _messages, _manager = await _run_one_compacting_turn()

        rewritten = [
            entry["after"]["content"]
            for entry in transcript.reconciled
            if entry["before"] is not None
            and entry["after"]["content"] != entry["before"]["content"]
        ]
        assert rewritten == []
        tool_rows = [r for r in transcript.rows.values() if r["role"] == "tool"]
        assert all(row["content"] == TOOL_RESULT for row in tool_rows)

    @pytest.mark.asyncio
    async def test_reconcile_fills_a_capped_row_whose_incremental_write_was_lost(
        self,
    ):
        lost, kept = CAPPED_CALLS[1], CAPPED_CALLS[0]
        transcript, _messages, _manager = await _run_one_compacting_turn(
            lose_writes_for=(lost,)
        )

        # The reconcile still owed the lost row: it exists (the capped view
        # is all that is left of it) and no tool call is left without its
        # result.
        filled = _tool_row(transcript, lost)
        assert filled is not None, "the lost tool result row was never written"
        assert filled["content"].startswith(TOOL_RESULT[:200])
        assert "[tool result truncated by compaction" in filled["content"]
        assert transcript.unpaired_tool_calls() == set()
        # ...and the capped view still never rewrites a row that did land.
        assert _tool_row(transcript, kept)["content"] == TOOL_RESULT
        rewritten = [
            entry["after"]["content"]
            for entry in transcript.reconciled
            if entry["before"] is not None
            and entry["after"]["content"] != entry["before"]["content"]
        ]
        assert rewritten == []
