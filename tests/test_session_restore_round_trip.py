"""Typed context entries survive a session persist/restore byte for byte.

Design: knowledge-base/knowledge/features/append_only_context_injection.md
(D3, D27, D28, B1, B2); spec:
knowledge-base/knowledge/plans/append_only_context_injection_wp2_spec.md §F,
§D rows 15 and 21, sub-step 2.2. Nothing produces entries yet, so these
histories build them with ``make_context_entry``.

- The serializer writes a ``role='context'`` row with exactly the entry's
  ``srw_injection`` schema in ``additional_kwargs``; every other row is
  unchanged (no ``additional_kwargs`` key).
- Rows -> JSON -> the history reader -> ``_db_rows_to_lc_messages`` ->
  ``fold_context_entries`` -> the OpenAI chat request is byte-identical to
  the live request built from the original messages (prompt cache across a
  stateless pod hop, D28: stored text, never re-rendered).
- ``strip_restored_pending_humans`` takes a stripped input's entries along.
- A child transcript restores its context rows the same way.
- End to end (WP2.5): two live append_only turns of the real session loop
  write their rows through the real serializer, the turn-end reconcile and
  the resume reader; a fresh process restores them and runs turn three. Its
  first request is byte-identical to the one the live process sends for
  turn three, and starts with the live turn-two request.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)

from agent.api.persistent_app import _db_rows_to_lc_messages
from agent.api.turn_executor import strip_restored_pending_humans
from agent.core.thread_messages import _persist_one_message, _serialize_message_row
from agent.database.postgres_db import PostgresDB
from agent.subagents.persistence import restore_subagent_messages
from shared.row_identity import _coerce_row_id
from shared.runtime.core.context_entries import (
    SRW_INJECTION_KEY,
    entry_kind,
    entry_meta,
    fold_context_entries,
    is_context_entry,
    make_context_entry,
)
from shared.runtime.core.message_markers import (
    PERSIST_ROLE_EVENT,
    PERSIST_ROLE_KEY,
    stamp_turn_membership,
)
from shared.runtime.llm.reasoning_chat import ReasoningChatOpenAI
from tests import _prompt_cache_prefix_harness as harness

SYSTEM = "You are a careful assistant."


def _id() -> str:
    return str(uuid4())


def _entry(kind: str, body: str, *, turn: int, section: str | None = None):
    items = (
        [{"key": f"{kind}-1", "hash": "a" * 16, "handle": "m:3f9a2c"}]
        if kind in ("memory", "knowledge", "guidance")
        else []
    )
    entry = make_context_entry(
        kind, body, section=section or kind, items=items, turn=turn
    )
    entry.id = _id()
    return stamp_turn_membership(entry, turn)


def _call(*ids: str) -> AIMessage:
    return AIMessage(
        content="",
        tool_calls=[
            {"name": "read_file", "args": {"path": f"{i}.md"}, "id": i} for i in ids
        ],
        id=_id(),
    )


def _session_history() -> List[tuple]:
    """``(message, turn)`` pairs of a two-turn session as the loop holds it.

    Turn 1: the input carries charter, memory and the turn boundary; a
    parallel batch has a knowledge entry stored between its two results
    (it folds into the last one). Turn 2: an entry after a tool result is
    the request tail.
    """
    turn1 = [
        HumanMessage(content="Summarize the brief.", id=_id()),
        _entry("charter", "Project charter: summaries for the board.", turn=1),
        _entry("memory", "[m:3f9a2c] The brief lives in docs/brief.md.", turn=1),
        _entry(
            "turn_boundary", "App Guide: turn 1.", turn=1, section="turn_boundary:1"
        ),
        _call("c1", "c2"),
        ToolMessage(content="brief text", tool_call_id="c1", id=_id()),
        _entry("knowledge", "Note: board summaries are 300 words.", turn=1),
        ToolMessage(content="style guide", tool_call_id="c2", id=_id()),
        AIMessage(content="Here is the summary.", id=_id()),
    ]
    turn2 = [
        HumanMessage(content="Shorter, please.", id=_id()),
        _entry(
            "turn_boundary", "App Guide: turn 2.", turn=2, section="turn_boundary:2"
        ),
        _call("c3"),
        ToolMessage(content="summary.md", tool_call_id="c3", id=_id()),
        _entry("memory", "[m:77aa01] The owner prefers bullet points.", turn=2),
        _entry("citation", "Citation [2] failed verification.", turn=2),
    ]
    return [(m, 1) for m in turn1] + [(m, 2) for m in turn2]


def _db_projection(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """What the history query's projection returns for the stored rows:
    jsonb as text, ``additional_kwargs`` NULL unless the row is context."""
    projected = []
    for row in rows:
        stored = json.loads(json.dumps(row))  # the jsonb / text round trip
        projected.append(
            {
                "id": stored["id"],
                "role": stored["role"],
                "content": stored["content"],
                "tool_calls": json.dumps(stored["tool_calls"])
                if stored["tool_calls"] is not None
                else None,
                "tool_call_id": stored["tool_call_id"],
                "turn_number": stored["turn_number"],
                "admitted_turn_number": None,
                "additional_kwargs": json.dumps(stored["additional_kwargs"])
                if stored["role"] == "context"
                else None,
            }
        )
    return projected


async def _restore(rows: List[Dict[str, Any]]) -> List[BaseMessage]:
    db = PostgresDB.__new__(PostgresDB)
    db.fetch = AsyncMock(return_value=_db_projection(rows))
    history = await db.get_thread_messages_history("thread", order_by_seq=True)
    return _db_rows_to_lc_messages(history)


def _llm(*, anthropic_breakpoints: bool) -> ReasoningChatOpenAI:
    return ReasoningChatOpenAI(
        model="gpt-4o" if not anthropic_breakpoints else "claude-sonnet-4-5",
        api_key="sk-test",
        base_url="http://provider.invalid/v1",
        anthropic_cache_breakpoints=anthropic_breakpoints,
    )


def _request_bytes(llm: ReasoningChatOpenAI, history: List[BaseMessage]) -> str:
    request = [SystemMessage(content=SYSTEM), *fold_context_entries(history)]
    payload = llm._get_request_payload(request)
    return json.dumps(payload["messages"], sort_keys=True, ensure_ascii=False)


# ---------------------------------------------------------------------------
# The serializer
# ---------------------------------------------------------------------------


class TestSerializer:
    def test_a_context_row_carries_exactly_its_schema(self):
        entry = _entry("memory", "fact", turn=3)
        row = _serialize_message_row(entry, 3)

        assert row["role"] == "context"
        assert row["content"] == entry.content
        assert row["additional_kwargs"] == {
            SRW_INJECTION_KEY: entry.additional_kwargs[SRW_INJECTION_KEY]
        }
        # The turn stamp and the persist role stay in memory.
        assert set(row["additional_kwargs"]) == {SRW_INJECTION_KEY}
        assert row["metrics"] is None and row["thinking"] is None

    @pytest.mark.parametrize(
        "message",
        [
            HumanMessage(content="hi", id="h"),
            HumanMessage(
                content="[job done]",
                id="ev",
                additional_kwargs={PERSIST_ROLE_KEY: PERSIST_ROLE_EVENT},
            ),
            AIMessage(content="answer", id="a"),
            AIMessage(
                content="", tool_calls=[{"name": "t", "args": {}, "id": "c"}], id="b"
            ),
            ToolMessage(content="result", tool_call_id="c", id="t"),
        ],
        ids=["human", "event", "ai", "ai-call", "tool"],
    )
    def test_every_other_row_is_unchanged(self, message):
        stamp_turn_membership(message, 4)
        row = _serialize_message_row(message, 4, metrics={"tokens": 1})

        assert "additional_kwargs" not in row
        assert set(row) == {
            "id",
            "role",
            "content",
            "tool_calls",
            "turn_number",
            "metrics",
            "tool_call_id",
            "thinking",
        }

    def test_a_context_role_without_a_schema_gets_no_kwargs(self):
        bare = HumanMessage(
            content="x", id="x", additional_kwargs={PERSIST_ROLE_KEY: "context"}
        )
        row = _serialize_message_row(bare, 1)
        assert row["role"] == "context"
        assert "additional_kwargs" not in row


# ---------------------------------------------------------------------------
# Persist -> restore -> fold -> request: byte-identical
# ---------------------------------------------------------------------------


class TestRestoreRoundTrip:
    @pytest.mark.asyncio
    async def test_entries_come_back_as_entries_in_conversation_order(self):
        pairs = _session_history()
        rows = [_serialize_message_row(m, turn) for m, turn in pairs]

        restored = await _restore(rows)

        original = [m for m, _ in pairs]
        # The batch's knowledge entry moves behind the batch's last result
        # (where the fold puts it anyway); everything else keeps its place.
        knowledge = original[6]
        expected = original[:6] + [original[7], knowledge] + original[8:]
        assert [m.id for m in restored] == [m.id for m in expected]
        for got, want in zip(restored, expected):
            assert type(got) is type(want)
            assert got.content == want.content
            assert is_context_entry(got) == is_context_entry(want)
            if is_context_entry(want):
                assert entry_meta(got) == entry_meta(want)
                assert got.additional_kwargs[PERSIST_ROLE_KEY] == "context"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "anthropic_breakpoints", [False, True], ids=["openai", "claude-proxy"]
    )
    async def test_the_restored_request_is_byte_identical(self, anthropic_breakpoints):
        pairs = _session_history()
        live = [m for m, _ in pairs]
        rows = [_serialize_message_row(m, turn) for m, turn in pairs]

        restored = await _restore(rows)
        llm = _llm(anthropic_breakpoints=anthropic_breakpoints)

        live_bytes = _request_bytes(llm, live)
        assert _request_bytes(llm, restored) == live_bytes
        # Non-vacuous: the entries are on the wire, inside their carriers.
        sent = json.loads(live_bytes)
        assert [m["role"] for m in sent] == [
            "system",
            "user",
            "assistant",
            "tool",
            "tool",
            "assistant",
            "user",
            "assistant",
            "tool",
        ]
        assert '<srw_context kind="charter">' in str(sent[1]["content"])
        assert '<srw_context kind="knowledge">' in str(sent[4]["content"])
        assert str(sent[-1]["content"]).count("<srw_context") == 2

    @pytest.mark.asyncio
    async def test_an_unreadable_context_row_is_dropped(self, caplog):
        good = _entry("memory", "fact", turn=1)
        rows = [
            _serialize_message_row(HumanMessage(content="go", id=_id()), 1),
            _serialize_message_row(good, 1),
            {
                **_serialize_message_row(_entry("memory", "lost", turn=1), 1),
                "additional_kwargs": {"not": "a schema"},
            },
        ]

        with caplog.at_level("DEBUG", logger="agent.api.persistent_app"):
            restored = await _restore(rows)

        assert [m.content for m in restored] == ["go", good.content]
        assert "unreadable context row" in caplog.text


# ---------------------------------------------------------------------------
# strip_restored_pending_humans takes a stripped input's entries along
# ---------------------------------------------------------------------------


class TestStripTakesTheEntriesAlong:
    def _history(self):
        answered = HumanMessage(content="q1", id="h1")
        answered_entry = _entry("turn_boundary", "turn 1", turn=1)
        answer = AIMessage(content="a1", id="a1")
        pending = HumanMessage(content="q2", id="row-2")
        pending_entries = [
            _entry("memory", "fact", turn=2),
            _entry("turn_boundary", "turn 2", turn=2),
        ]
        return answered, answered_entry, answer, pending, pending_entries

    def test_by_id_the_entries_after_the_input_go_too(self):
        answered, answered_entry, answer, pending, pending_entries = self._history()
        later = HumanMessage(content="[notice]", id="event-row")
        messages = [answered, answered_entry, answer, pending, *pending_entries, later]

        removed = strip_restored_pending_humans(
            messages, [{"id": "row-2", "seq": 9, "content": "q2"}]
        )

        assert removed == 3
        assert messages == [answered, answered_entry, answer, later]

    def test_by_content_the_trailing_entries_go_too(self):
        answered, answered_entry, answer, pending, pending_entries = self._history()
        pending.id = "fresh-uuid"  # restored without its row id
        messages = [answered, answered_entry, answer, pending, *pending_entries]

        removed = strip_restored_pending_humans(
            messages, [{"id": "row-2", "seq": 9, "content": "q2"}]
        )

        assert removed == 3
        assert messages == [answered, answered_entry, answer]

    def test_entries_after_an_answer_stop_the_tail_matcher(self):
        answered, answered_entry, answer, _pending, _entries = self._history()
        trailing = _entry("memory", "fact", turn=1)
        messages = [answered, answered_entry, answer, trailing]

        removed = strip_restored_pending_humans(
            messages, [{"id": "row-9", "seq": 9, "content": "q1"}]
        )

        assert removed == 0
        assert messages == [answered, answered_entry, answer, trailing]

    def test_an_entry_never_matches_a_pending_row_by_id(self):
        entry = _entry("memory", "fact", turn=2)
        messages = [HumanMessage(content="q1", id="h1"), entry]

        removed = strip_restored_pending_humans(
            messages, [{"id": entry.id, "seq": 9, "content": entry.content}]
        )

        # A context row is never pending input; the content match stops at
        # the input before it, which is not pending either.
        assert removed == 0
        assert messages[-1] is entry


# ---------------------------------------------------------------------------
# Child transcripts (subagents run the same loop and the same serializer)
# ---------------------------------------------------------------------------


class TestChildTranscript:
    def test_a_context_row_restores_as_an_entry(self):
        entry = _entry("memory", "fact", turn=1)
        rows = _db_projection(
            [
                _serialize_message_row(HumanMessage(content="brief", id=_id()), 1),
                _serialize_message_row(entry, 1),
                _serialize_message_row(AIMessage(content="done", id=_id()), 1),
            ]
        )
        for row in rows:
            if row["additional_kwargs"] is not None:
                row["additional_kwargs"] = json.loads(row["additional_kwargs"])
            row.pop("admitted_turn_number")

        restored = restore_subagent_messages(rows)

        assert [type(m) for m in restored] == [HumanMessage, HumanMessage, AIMessage]
        assert is_context_entry(restored[1])
        assert restored[1].content == entry.content
        assert entry_meta(restored[1]) == entry_meta(entry)

    def test_an_unreadable_context_row_is_dropped_not_fatal(self):
        rows = [
            {"id": _id(), "role": "human", "content": "brief"},
            {"id": _id(), "role": "context", "content": "x", "additional_kwargs": None},
        ]
        restored = restore_subagent_messages(rows)
        assert [m.content for m in restored] == ["brief"]


# ---------------------------------------------------------------------------
# End to end: two live append_only turns, a fresh-process restore, turn three
# ---------------------------------------------------------------------------

THREAD = "5b9c4f0e-2d1a-4c7e-9f3b-8a6d2e1c0b74"
ROUND_TRIP_INPUTS = (
    "Turn one: what is in notes.md?",
    "Turn two: and what does the product guide say?",
    "Turn three: anything else still open?",
)
ROUND_TRIP_SCRIPT = (
    harness.Step(
        calls=(("read_file", {"path": "notes.md", "why": "STEP00"}),),
        reasoning="Reasoning STEP00: open notes.md.",
    ),
    harness.Step(
        text="STEP01 notes.md lists three open items.",
        reasoning="Reasoning STEP01: answer turn one.",
    ),
    harness.Step(
        calls=(("read_product_guide", {"topic": "index", "why": "STEP02"}),),
        reasoning="Reasoning STEP02: check the guide.",
    ),
    harness.Step(
        text="STEP03 The guide covers sessions and jobs.",
        reasoning="Reasoning STEP03: answer turn two.",
    ),
    harness.Step(
        text="STEP04 Nothing else is open.",
        reasoning="Reasoning STEP04: answer turn three.",
    ),
)


class _ThreadRows:
    """``thread_messages`` of one thread, as the upsert and the resume read it.

    Writes arrive through the real serializer (the loop's incremental persist
    and the turn-end reconcile). The upsert mirrors postgres_db: ``seq`` is
    assigned on first insert, a re-save never changes the role, and it keeps
    the stored ``additional_kwargs`` when it brings none (COALESCE).
    ``fetch`` answers the resume history query with its projection.
    """

    def __init__(self) -> None:
        self.rows: Dict[str, Dict[str, Any]] = {}

    def _upsert(self, row: Dict[str, Any]) -> None:
        row = dict(row)
        row_id = str(_coerce_row_id(row.get("id")))
        stored = self.rows.get(row_id)
        if stored is None:
            row.pop("insert_if_absent", None)
            row.update(id=row_id, seq=len(self.rows) + 1)
            self.rows[row_id] = row
            return
        if row.pop("insert_if_absent", False):
            return
        kwargs = row.pop("additional_kwargs", None)
        for key in ("id", "role", "seq"):
            row.pop(key, None)
        stored.update(row)
        if kwargs is not None:
            stored["additional_kwargs"] = kwargs

    async def save_thread_message(self, thread_id: str, **row: Any) -> None:
        assert thread_id == THREAD
        self._upsert(row)

    async def save_thread_messages(
        self, thread_id: str, rows: List[Dict[str, Any]], **boundary: Any
    ) -> Optional[str]:
        assert thread_id == THREAD
        for row in rows:
            self._upsert(row)
        return "producer-1" if boundary else None

    def ordered(self) -> List[Dict[str, Any]]:
        return sorted(self.rows.values(), key=lambda row: row["seq"])

    async def fetch(self, query: str, *params: Any) -> List[Dict[str, Any]]:
        projected = _db_projection(self.ordered())
        if "ORDER BY seq DESC" in query:
            projected.reverse()
        return projected


async def _restore_rows(rows: _ThreadRows, config: Any):
    """Restore the thread in a fresh process: ``_restore_session_messages``
    over the real history reader, with no compaction checkpoint (Path B)."""
    import agent.api.persistent_app as pa

    db = PostgresDB.__new__(PostgresDB)
    db.fetch = rows.fetch
    db.get_latest_compaction_checkpoint = AsyncMock(return_value=None)
    session = SimpleNamespace(
        messages=[],
        context_manager=None,
        auxiliary_llm=None,
        config=config,
        turn_count=0,
    )
    with (
        patch.object(pa, "_session", session),
        patch.object(pa, "_agent", SimpleNamespace(postgres_conn=db)),
        patch.object(pa._session_identity, "_thread_id", THREAD),
    ):
        await pa._restore_session_messages()
    return session.messages, session.turn_count


def _round_trip_config(family: Any) -> Any:
    return harness._session_config(
        injections=True, model=family.model, injection_mode="append_only"
    )


async def _run_session(
    family: Any,
    monkeypatch: Any,
    *,
    rows: _ThreadRows,
    inputs: tuple,
    messages: List[BaseMessage],
    lane: str,
    initial_turn_count: int = 0,
) -> List[Any]:
    """Run ``inputs`` through the real session loop in append_only mode.

    Rows are written the way the transport writes them: every message the
    loop persists incrementally, then the turn-end reconcile (the pinned
    walk, or the stateless lane's authoritative selection by stamp).
    """
    from agent.api.persistent_app import _save_turn_ai_messages
    from agent.persistent_graph import PersistentLoopCallbacks, run_persistent_loop
    from shared.runtime.core.loader import create_llm

    provider = harness.FakeProvider(ROUND_TRIP_SCRIPT, family)
    provider.install(monkeypatch)
    llm_config = family.llm_config()
    llm = harness.bind_like_srw(
        create_llm(llm_config), llm_config, harness.SESSION_TOOLS
    )
    context_manager = AsyncMock()
    context_manager.should_summarize = MagicMock(return_value=False)
    context_manager.ensure_within_limits = AsyncMock(
        side_effect=lambda history, *_a, **_k: history
    )
    context_manager.config.keep_recent_messages = 10
    context_manager.record_provider_usage = MagicMock()
    turn = {"id": initial_turn_count}
    errors: List[Any] = []
    stateless = lane == "stateless"

    async def on_turn_start(turn_id: int) -> None:
        turn["id"] = turn_id

    async def persist(msg: Any) -> bool:
        await _persist_one_message(rows, THREAD, msg, turn["id"])
        return True

    async def on_turn_complete(turn_id, metrics, input_id, scope_kind, scope_id, **_):
        await _save_turn_ai_messages(
            rows,
            THREAD,
            messages,
            turn_id,
            metrics,
            authoritative_turn_boundary=stateless,
            turn_input_message_id=input_id if stateless else None,
            memory_scope_kind=scope_kind if stateless else None,
            memory_scope_id=scope_id if stateless else None,
        )

    async def on_error(*args: Any, **kwargs: Any) -> None:
        errors.append((args, kwargs))

    callbacks = PersistentLoopCallbacks(
        get_user_input=AsyncMock(side_effect=[*inputs, asyncio.CancelledError()]),
        on_token=AsyncMock(),
        on_thinking=AsyncMock(),
        on_tool_start=AsyncMock(),
        on_tool_result=AsyncMock(),
        permission_check=AsyncMock(return_value=True),
        on_turn_start=on_turn_start,
        on_turn_complete=on_turn_complete,
        on_error=on_error,
        check_interrupt=MagicMock(return_value=None),
        persist_message=persist,
    )
    knowledge_store = MagicMock()
    knowledge_store.get_charter_note = AsyncMock(return_value=harness.CHARTER)
    await run_persistent_loop(
        llm_with_tools=llm,
        tools=[
            harness._session_tool(t["function"]["name"]) for t in harness.SESSION_TOOLS
        ],
        context_manager=context_manager,
        config=_round_trip_config(family),
        system_prompt="You are the SRW session assistant under test.",
        callbacks=callbacks,
        messages=messages,
        knowledge_store=knowledge_store,
        project_ids=[harness.PROJECT_ID],
        tool_context=SimpleNamespace(
            knowledge_bindings=[],
            citation_engine=None,
            subagent_runtime=SimpleNamespace(
                active_subagents_block=lambda: harness.ACTIVE_SUBAGENTS
            ),
        ),
        memory_service=harness.RecordingMemoryManager(),
        initial_turn_count=initial_turn_count,
        defer_memory_extraction_to_outbox=stateless,
        memory_thread_id=THREAD,
    )
    assert errors == []
    return provider.requests


# langchain-openai warns about SRW's `reasoning_effort` in model_kwargs on every
# Chat Completions client the harness builds; it is not under test here.
@pytest.mark.filterwarnings(
    "ignore:Parameters .*reasoning_effort.* should be specified explicitly:UserWarning"
)
class TestTurnThreeAfterRestore:
    """D28 end to end: a restore replays the stored entries, never re-renders.

    One live process runs three turns. A second one runs the first two and
    stops (a stateless pod hop, or a pinned pod restart); a fresh process
    restores its rows and runs turn three. That request must equal the live
    process's turn-three request byte for byte, so the provider cache that
    turn two wrote is read by turn three on either path.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize("lane", ["pinned", "stateless"])
    @pytest.mark.parametrize(
        "family_id", ["openai-chat", "openai-chat-claude-proxy", "glm-5.2"]
    )
    async def test_restored_turn_three_matches_the_live_request(
        self, family_id, lane, monkeypatch
    ):
        family = harness.FAMILIES[family_id]
        live_rows = _ThreadRows()

        warm = await _run_session(
            family,
            monkeypatch,
            rows=_ThreadRows(),
            inputs=ROUND_TRIP_INPUTS,
            messages=[],
            lane=lane,
        )
        live_messages: List[BaseMessage] = []
        live = await _run_session(
            family,
            monkeypatch,
            rows=live_rows,
            inputs=ROUND_TRIP_INPUTS[:2],
            messages=live_messages,
            lane=lane,
        )
        assert len(warm) == 5 and len(live) == 4
        for warm_request, live_request in zip(warm, live):
            assert harness.canonical(warm_request.body) == harness.canonical(
                live_request.body
            )

        # Every entry the live process appended is a context row with its
        # exact text and schema, in history order (written first, then
        # re-saved by the turn-end reconcile without losing the schema).
        live_entries = [m for m in live_messages if is_context_entry(m)]
        assert [entry_kind(m) for m in live_entries] == [
            "charter",
            "memory",
            "knowledge",
            "subagents",
            "turn_boundary",
            "turn_boundary",
        ]
        context_rows = [r for r in live_rows.ordered() if r["role"] == "context"]
        assert [r["content"] for r in context_rows] == [m.content for m in live_entries]
        assert [r["additional_kwargs"] for r in context_rows] == [
            {SRW_INJECTION_KEY: entry_meta(m)} for m in live_entries
        ]

        restored, turn_count = await _restore_rows(
            live_rows, _round_trip_config(family)
        )
        assert turn_count == 2
        assert [m.content for m in restored if is_context_entry(m)] == [
            m.content for m in live_entries
        ]

        # Nothing is rendered again from the restored history: every kind is
        # present, so turn three appends only its own boundary. A renderer
        # that ran for a present kind would fail the turn.
        from shared.runtime.services.knowledge_store import KnowledgeStore
        from shared.runtime.services.recall_store import RecallStore

        def _no_render(*_a: Any, **_k: Any) -> str:
            raise AssertionError("a present entry was rendered again")

        monkeypatch.setattr(RecallStore, "render_memory_entry", _no_render)
        monkeypatch.setattr(KnowledgeStore, "assemble_knowledge_block", _no_render)
        (resumed,) = await _run_session(
            family,
            monkeypatch,
            rows=live_rows,
            inputs=ROUND_TRIP_INPUTS[2:],
            messages=restored,
            lane=lane,
            initial_turn_count=turn_count,
        )

        assert harness.api_prefix_violation(live[-1], resumed) is None
        assert harness.canonical(resumed.body) == harness.canonical(warm[-1].body)
        body = resumed.body
        assert harness.text_occurrences(body, '<srw_context kind="turn_boundary">') == 3
        for kind in ("charter", "memory", "knowledge", "subagents"):
            assert harness.text_occurrences(body, f'<srw_context kind="{kind}">') == 1
