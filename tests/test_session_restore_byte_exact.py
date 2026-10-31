"""A restored session sends the bytes the live loop sends (F7).

Design: knowledge-base/knowledge/features/append_only_context_injection.md
§3.6 ("F7, new: restore drift at session turn boundaries"). A stateless turn
or a pinned pod restart rebuilds the history from ``thread_messages`` rows.
Its next request must start with the bytes the live loop would have sent, or
the provider cache misses from the first differing message on.

The causes, each pinned below:

- A tool's image row is list content: the text "Image content from tool
  call X:" and the image. The serializer stored the text only, so a restore
  sent a string without the image, and a context entry folded into it joined
  with ``ENTRY_SEPARATOR`` where the live fold appends a text part. The audit
  (``llm_requests``) shows that as ``": <srw_context"`` against
  ``":\\n\\n<srw_context"``: its archiver joins text parts with a space and
  drops images. The 46 or 47 characters the audited message lost are the
  live carrier's ``_srw_persist_role`` and ``_srw_turn_id`` kwargs (49 or 50)
  less the 3 the escaped separator adds; neither kwarg reaches the wire.
- The image row with a trailing space and an empty AI turn stored as " "
  came from the flattening that joined every part, text or not.
- Missing from the audit because ``llm_requests`` is jsonb as well:
  ``tool_calls`` is a jsonb column, which hands object keys back shortest
  first, and the clients serialise a call's ``args`` in their own order. A
  restored call whose keys came back reordered sent other ``arguments``.
"""

from __future__ import annotations

import asyncio
import itertools
import json
from types import SimpleNamespace
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest
from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)

from agent.api.persistent_app import _db_rows_to_lc_messages
from agent.core.archiver import _message_to_dict
from agent.core.thread_messages import (
    ANTHROPIC_CONTENT_KEY,
    ARGUMENTS_KEY,
    NATIVE_CONTENT_KEY,
    _serialize_message_row,
    restored_tool_call_args,
)
from agent.database.postgres_db import PostgresDB
from agent.services.image_content import ExtractedImage, make_multimodal_user_message
from agent.subagents.persistence import restore_subagent_messages
from shared.runtime.core.context_entries import (
    fold_context_entries,
    is_context_entry,
    make_context_entry,
)
from shared.runtime.core.message_markers import (
    PERSIST_ROLE_EVENT,
    PERSIST_ROLE_KEY,
    TURN_MEMBERSHIP_KEY,
    stamp_turn_membership,
)
from tests import _prompt_cache_prefix_harness as harness
from tests.test_session_restore_round_trip import (
    SYSTEM,
    THREAD,
    _db_projection,
    _llm,
    _restore_rows,
    _ThreadRows,
)

# A 2x2 PNG, so the downscaler leaves it untouched.
PNG = (
    "iVBORw0KGgoAAAANSUhEUgAAAAIAAAACCAIAAAD91JpzAAAAFklEQVR4nGP8z8DAwMDAxMDA"
    "wMDAAAANHQEDasKb6QAAAABJRU5ErkJggg=="
)
MARKER = "Image content from tool call call_01a157c0fb1071a9a7d4dad67c2f4037:"


def _jsonb(value: Any) -> Any:
    """``value`` as a jsonb column returns it: object keys shortest first,
    keys of one length in byte order (checked against Postgres in
    test_session_restore_byte_exact_pg.py)."""
    if isinstance(value, dict):
        keys = sorted(value, key=lambda k: (len(k.encode()), k.encode()))
        return {key: _jsonb(value[key]) for key in keys}
    if isinstance(value, list):
        return [_jsonb(item) for item in value]
    return value


def _stored(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """The rows with their jsonb columns as Postgres hands them back."""
    return [
        {
            **row,
            **{
                column: _jsonb(row[column])
                for column in ("tool_calls", "provider_raw", "additional_kwargs")
                if row.get(column) is not None
            },
        }
        for row in rows
    ]


def _image_row(turn: int = 3) -> HumanMessage:
    message = make_multimodal_user_message(
        MARKER, [ExtractedImage(base64_data=PNG, mime_type="image/png")]
    )
    message.id = str(uuid4())
    return stamp_turn_membership(message, turn)


def _memory_entry(turn: int = 3) -> HumanMessage:
    entry = make_context_entry(
        "memory",
        "[m:3f9a2c] The brief lives in docs/brief.md.",
        section="memory",
        items=[{"key": "memory-1", "hash": "a" * 16, "handle": "m:3f9a2c"}],
        turn=turn,
    )
    entry.id = str(uuid4())
    return stamp_turn_membership(entry, turn)


def _call(args: Dict[str, Any], turn: int = 3) -> AIMessage:
    message = AIMessage(
        content="",
        tool_calls=[{"name": "edit_file", "args": args, "id": "call_1"}],
        id=str(uuid4()),
    )
    return stamp_turn_membership(message, turn)


def _result(content: str, turn: int = 3) -> ToolMessage:
    message = ToolMessage(content=content, tool_call_id="call_1", id=str(uuid4()))
    return stamp_turn_membership(message, turn)


def _wire(history: List[BaseMessage]) -> str:
    """The Chat Completions messages of a request, key order included."""
    request = [SystemMessage(content=SYSTEM), *fold_context_entries(history)]
    payload = _llm(anthropic_breakpoints=False)._get_request_payload(request)
    return json.dumps(payload["messages"], ensure_ascii=False)


async def _read_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Store rows (jsonb) and read them back as the restores read."""
    db = PostgresDB.__new__(PostgresDB)
    db.fetch = AsyncMock(return_value=_db_projection(_stored(rows)))
    return await db.get_thread_messages_history(
        THREAD, limit=None, order_by_seq=True, include_provider_raw=True
    )


async def _read_back(history: List[BaseMessage]) -> List[Dict[str, Any]]:
    """Serialize, store (jsonb) and read back as the restores read."""
    return await _read_rows([_serialize_message_row(m, 3) for m in history])


async def _round_trip(history: List[BaseMessage]) -> List[BaseMessage]:
    return _db_rows_to_lc_messages(await _read_back(history))


# ---------------------------------------------------------------------------
# A tool's image row
# ---------------------------------------------------------------------------


class TestImageRow:
    def test_the_row_keeps_the_text_for_the_transcript_and_the_parts(self):
        message = _image_row()
        row = _serialize_message_row(message, 3)

        assert row["role"] == PERSIST_ROLE_EVENT
        # What the cockpit shows (and hides by this exact text), without the
        # trailing space the old flattening left.
        assert row["content"] == MARKER
        assert json.loads(row["provider_raw"][NATIVE_CONTENT_KEY]) == message.content

    @pytest.mark.asyncio
    async def test_it_comes_back_with_its_image_and_its_role(self):
        message = _image_row()

        (restored,) = await _round_trip([message])

        assert restored.content == message.content
        assert restored.additional_kwargs == {PERSIST_ROLE_KEY: PERSIST_ROLE_EVENT}
        assert _serialize_message_row(restored, 3)["role"] == PERSIST_ROLE_EVENT

    @pytest.mark.asyncio
    @pytest.mark.parametrize("multimodal", [True, False])
    async def test_a_model_without_vision_gets_the_row_as_text(self, multimodal):
        """The model may have changed while the session was detached. A
        text-only one gets the row's text, as every restore did before."""
        import agent.api.persistent_app as pa

        message = _image_row()
        restored = await _round_trip([message])
        llm = SimpleNamespace(model="gpt-4.1", multimodal=multimodal)

        with patch.object(
            pa, "_session", SimpleNamespace(config=SimpleNamespace(llm=llm))
        ):
            (sanitized,) = pa._sanitize_restored_history(restored)

        assert sanitized.content == (message.content if multimodal else MARKER)
        assert sanitized.id == message.id

    @pytest.mark.asyncio
    async def test_a_folded_entry_sends_the_live_bytes(self):
        history = [
            HumanMessage(content="Look at the screenshot.", id=str(uuid4())),
            _call({"path": "shot.png"}),
            _result("ok: read_file\n[image attached: image/png]"),
            _image_row(),
            _memory_entry(),
        ]

        restored = await _round_trip(history)

        sent = json.loads(_wire(history))
        assert _wire(restored) == _wire(history)
        # Non-vacuous: the entry rides the image row as its own text part.
        assert [part["type"] for part in sent[-1]["content"]] == [
            "text",
            "image_url",
            "text",
        ]
        assert sent[-1]["content"][2]["text"].startswith('<srw_context kind="memory">')

    @pytest.mark.asyncio
    async def test_the_audit_view_differs_only_in_the_turn_stamp(self):
        """``llm_requests`` keeps the in-memory kwargs. The live carrier has
        its persist role and turn stamp; the restored one gets the persist
        role back, and only the stamp of the turn that wrote it is missing."""
        history = [_image_row(), _memory_entry()]

        restored = await _round_trip(history)

        (live,) = fold_context_entries(history)
        (again,) = fold_context_entries(restored)
        live_view, restored_view = _message_to_dict(live), _message_to_dict(again)
        assert live_view["content"] == restored_view["content"]
        live_kwargs = dict(live_view["additional_kwargs"])
        assert live_kwargs.pop(TURN_MEMBERSHIP_KEY) == 3
        assert live_kwargs == restored_view["additional_kwargs"]


# ---------------------------------------------------------------------------
# Flattened text: no whitespace the live message did not have
# ---------------------------------------------------------------------------


class TestFlattenedText:
    @pytest.mark.asyncio
    async def test_an_empty_ai_turn_stays_empty(self):
        """A Responses turn of reasoning and a function call has no text. The
        old join stored it as " ", which came back as a one-space message. It
        comes back as its items; a row without them is empty, not " "."""
        turn = AIMessage(
            content=[
                {"type": "reasoning", "id": "rs_1", "summary": []},
                {
                    "type": "function_call",
                    "call_id": "call_1",
                    "name": "read_file",
                    "arguments": "{}",
                },
            ],
            tool_calls=[{"name": "read_file", "args": {}, "id": "call_1"}],
            id=str(uuid4()),
        )

        row = _serialize_message_row(turn, 1)
        assert row["content"] == ""
        (restored,) = await _round_trip([turn])
        assert restored.content == turn.content

        del row["provider_raw"]
        (flat,) = _db_rows_to_lc_messages(await _read_rows([row]))
        assert flat.content == ""


# ---------------------------------------------------------------------------
# Tool-call arguments through a jsonb column
# ---------------------------------------------------------------------------


class TestToolCallArguments:
    ARGS = {"path": "app.py", "old_string": "x = 1", "new_string": "x = 2"}

    def test_arguments_jsonb_would_reorder_are_kept_as_text(self):
        (entry,) = _serialize_message_row(_call(self.ARGS), 3)["tool_calls"]

        assert entry["args"] == self.ARGS
        assert entry[ARGUMENTS_KEY] == json.dumps(self.ARGS)
        assert list(_jsonb(entry)["args"]) != list(self.ARGS)

    @pytest.mark.parametrize(
        "args",
        [{}, {"path": "a.md"}, {"path": "a.md", "content": "b"}],
        ids=["empty", "one-key", "already-in-jsonb-order"],
    )
    def test_arguments_jsonb_returns_in_order_add_nothing(self, args):
        (entry,) = _serialize_message_row(_call(args), 3)["tool_calls"]

        assert ARGUMENTS_KEY not in entry

    @pytest.mark.asyncio
    async def test_the_restored_call_sends_the_live_arguments(self):
        args = {**self.ARGS, "options": {"strict": True, "all": False}}
        history = [
            HumanMessage(content="Bump x.", id=str(uuid4())),
            _call(args),
            _result("ok"),
        ]

        restored = await _round_trip(history)

        assert list(restored[1].tool_calls[0]["args"]) == list(args)
        assert list(restored[1].tool_calls[0]["args"]["options"]) == ["strict", "all"]
        assert _wire(restored) == _wire(history)

    def test_unreadable_arguments_fall_back_to_args(self):
        call = {"id": "c", "name": "t", "args": {"a": 1}, ARGUMENTS_KEY: "{not json"}

        assert restored_tool_call_args(call) == {"a": 1}


# ---------------------------------------------------------------------------
# An assistant turn's native blocks through a jsonb column
# ---------------------------------------------------------------------------


class TestNativeBlocks:
    """``provider_raw`` is jsonb as well, and a client sends a restored turn's
    blocks as they come back (an Anthropic thinking block, a Responses item).
    They are kept the way the arguments are: as JSON text where jsonb would
    reorder a key, as the list where it would not."""

    BLOCKS = [
        {"type": "thinking", "thinking": "Open app.py first.", "signature": "sig-1"},
        {"type": "text", "text": "Let me look."},
        {
            "type": "tool_use",
            "id": "call_1",
            "name": "edit_file",
            "input": {"path": "app.py", "why": "look"},
        },
    ]

    @staticmethod
    def _turn(content: List[Any]) -> AIMessage:
        return AIMessage(
            content=content,
            tool_calls=[
                {
                    "name": "edit_file",
                    "args": {"path": "app.py", "why": "look"},
                    "id": "call_1",
                }
            ],
            response_metadata={
                "model_name": "claude-opus-5-5",
                "model_provider": "anthropic",
            },
            id=str(uuid4()),
        )

    def test_blocks_jsonb_would_reorder_are_kept_as_text(self):
        raw = _serialize_message_row(self._turn(self.BLOCKS), 3)["provider_raw"]

        assert raw[NATIVE_CONTENT_KEY] == json.dumps(self.BLOCKS)
        assert [list(b) for b in _jsonb(self.BLOCKS)] != [list(b) for b in self.BLOCKS]

    def test_blocks_jsonb_returns_in_order_stay_a_list(self):
        # A thinking block's keys are in jsonb's order already.
        blocks = self.BLOCKS[:1]

        raw = _serialize_message_row(self._turn(blocks), 3)["provider_raw"]

        assert raw[NATIVE_CONTENT_KEY] == blocks

    @pytest.mark.asyncio
    async def test_the_restored_turn_has_the_live_key_order(self):
        (restored,) = await _round_trip([self._turn(self.BLOCKS)])

        assert json.dumps(restored.content) == json.dumps(self.BLOCKS)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "envelope",
        [
            {NATIVE_CONTENT_KEY: BLOCKS},
            {ANTHROPIC_CONTENT_KEY: BLOCKS},
        ],
        ids=["list", "anthropic-key"],
    )
    async def test_a_turn_stored_as_a_list_still_restores(self, envelope):
        """Rows from before the text: the blocks come back in jsonb's order."""
        row = _serialize_message_row(self._turn(self.BLOCKS), 3)
        row["provider_raw"] = {**envelope, "model": "claude-opus-5-5"}

        (restored,) = _db_rows_to_lc_messages(await _read_rows([row]))

        assert restored.content == self.BLOCKS
        assert restored.response_metadata == {"model_name": "claude-opus-5-5"}

    @pytest.mark.asyncio
    async def test_unreadable_text_falls_back_to_the_row_text(self):
        row = _serialize_message_row(self._turn(self.BLOCKS), 3)
        row["provider_raw"][NATIVE_CONTENT_KEY] = "[not json"

        (restored,) = _db_rows_to_lc_messages(await _read_rows([row]))

        assert restored.content == "Let me look."


# ---------------------------------------------------------------------------
# A child transcript restores the same way
# ---------------------------------------------------------------------------


class TestChildTranscript:
    @pytest.mark.asyncio
    async def test_a_revived_child_gets_its_image_and_argument_order(self):
        args = {"path": "shot.png", "why": "look"}
        history = [
            HumanMessage(content="Describe shot.png.", id=str(uuid4())),
            _call(args),
            _result("ok\n[image attached: image/png]"),
            _image_row(),
        ]

        restored = restore_subagent_messages(await _read_back(history))

        assert list(restored[1].tool_calls[0]["args"]) == list(args)
        assert restored[3].content == history[3].content


# ---------------------------------------------------------------------------
# End to end: an image turn, a restore, the next turn
# ---------------------------------------------------------------------------

INPUTS = (
    "Turn one: what is on shot.png?",
    "Turn two: is it still red?",
    "Turn three: anything else?",
)
SCRIPT = (
    harness.Step(
        calls=(("read_file", {"path": "shot.png", "why": "STEP00"}),),
        reasoning="Reasoning STEP00: open the screenshot.",
    ),
    harness.Step(
        text="STEP01 A red square.",
        reasoning="Reasoning STEP01: describe it.",
    ),
    harness.Step(
        text="STEP02 Still red.",
        reasoning="Reasoning STEP02: answer turn two.",
    ),
    harness.Step(
        text="STEP03 Nothing else.",
        reasoning="Reasoning STEP03: answer turn three.",
    ),
)


class _DripMemory(harness.RecordingMemoryManager):
    """Seven memories against a cap of five: the first request appends five,
    the second the other two, right after the tool's image row (its carrier).
    The same rows on every retrieval, so a fresh process finds them present."""

    def __init__(self) -> None:
        super().__init__(churn=True)

    def _payload(self, call: int) -> Any:
        return super()._payload(0)


class _JsonbRows(_ThreadRows):
    """``_ThreadRows`` whose jsonb columns come back as Postgres returns them."""

    async def fetch(self, query: str, *params: Any) -> List[Dict[str, Any]]:
        rows = await super().fetch(query, *params)
        for row in rows:
            for column in ("tool_calls", "provider_raw", "additional_kwargs"):
                if isinstance(row.get(column), str):
                    row[column] = json.dumps(_jsonb(json.loads(row[column])))
        return rows


def _image_tool(name: str) -> MagicMock:
    async def run(args: Any) -> str:
        text = f"ok: {name} {json.dumps(args, sort_keys=True)}"
        if name == "read_file":
            text += f'\n<image_data mime_type="image/png">\n{PNG}\n</image_data>'
        return text

    tool = MagicMock()
    tool.name = name
    tool.args_schema = None
    tool.ainvoke = AsyncMock(side_effect=run)
    return tool


def _config(family: Any) -> Any:
    config = harness._session_config(injections=True, model=family.model)
    config.limits.image_tokens = {}
    config.image_quality = "standard"
    return config


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
    """``inputs`` through the real session loop; rows written as the
    transport writes them (incremental persist, then the turn-end reconcile)."""
    from agent.api.persistent_app import _save_turn_ai_messages
    from agent.core.thread_messages import _persist_one_message
    from agent.persistent_graph import PersistentLoopCallbacks, run_persistent_loop
    from shared.runtime.core.loader import create_llm

    provider = harness.FakeProvider(SCRIPT, family)
    provider.install(monkeypatch)
    # Gemini answers without call ids and langchain-google-genai mints a uuid4
    # for each, which the image row quotes. Minted from 0 in every run, the
    # warm run and the live one (whose rows are restored) mint the same ids.
    from langchain_google_genai import chat_models as genai_chat_models

    minted = itertools.count()
    monkeypatch.setattr(
        genai_chat_models,
        "uuid",
        SimpleNamespace(uuid4=lambda: UUID(int=next(minted))),
    )
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
        tools=[_image_tool(t["function"]["name"]) for t in harness.SESSION_TOOLS],
        context_manager=context_manager,
        config=_config(family),
        system_prompt="You are the SRW session assistant under test.",
        callbacks=callbacks,
        messages=messages,
        knowledge_store=knowledge_store,
        project_ids=[harness.PROJECT_ID],
        tool_context=SimpleNamespace(knowledge_bindings=[], citation_engine=None),
        memory_service=_DripMemory(),
        initial_turn_count=initial_turn_count,
        defer_memory_extraction_to_outbox=stateless,
        memory_thread_id=THREAD,
    )
    assert errors == []
    return provider.requests


def _exact(captured: Any) -> str:
    """A request body as sent, key order included (``harness.canonical``
    sorts keys, which hides a reordered tool-call input)."""
    return json.dumps(captured.body, ensure_ascii=False)


@pytest.mark.filterwarnings(
    "ignore:Parameters .*reasoning_effort.* should be specified explicitly:UserWarning"
)
class TestTurnThreeAfterRestore:
    """One process runs three turns; another runs two, a fresh one restores
    its rows and runs turn three. Turn one reads an image whose row carries a
    memory entry, through a call whose arguments jsonb reorders. Turn three's
    request must equal the live one byte for byte.

    The Responses and Gemini routes also replay each assistant turn's native
    blocks (reasoning items and item ids, thought parts and signatures),
    which come back from provider_raw in the live key order.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize("lane", ["pinned", "stateless"])
    @pytest.mark.parametrize(
        "family_id",
        [
            "openai-chat",
            "openai-chat-claude-proxy",
            "anthropic-claude-proxy",
            "anthropic",
            "glm-5.2",
            "responses-openai",
            "responses-codex",
            "gemini",
        ],
    )
    async def test_restored_turn_three_matches_the_live_request(
        self, family_id, lane, monkeypatch
    ):
        family = harness.FAMILIES[family_id]
        warm = await _run_session(
            family,
            monkeypatch,
            rows=_JsonbRows(),
            inputs=INPUTS,
            messages=[],
            lane=lane,
        )
        live_rows = _JsonbRows()
        live_messages: List[BaseMessage] = []
        live = await _run_session(
            family,
            monkeypatch,
            rows=live_rows,
            inputs=INPUTS[:2],
            messages=live_messages,
            lane=lane,
        )
        assert len(warm) == 4 and len(live) == 3

        # Non-vacuous: the image row carries an entry, and the stored call
        # needed its argument text.
        image_index = next(
            i
            for i, m in enumerate(live_messages)
            if isinstance(m, HumanMessage) and isinstance(m.content, list)
        )
        assert is_context_entry(live_messages[image_index + 1])
        stored_calls = [
            call for row in live_rows.ordered() for call in row["tool_calls"] or []
        ]
        assert [ARGUMENTS_KEY in call for call in stored_calls] == [True]

        restored, turn_count = await _restore_rows(live_rows, _config(family))
        (resumed,) = await _run_session(
            family,
            monkeypatch,
            rows=live_rows,
            inputs=INPUTS[2:],
            messages=restored,
            lane=lane,
            initial_turn_count=turn_count,
        )

        assert harness.api_prefix_violation(live[-1], resumed) is None
        assert _exact(resumed) == _exact(warm[-1])
        assert PNG in _exact(resumed)
