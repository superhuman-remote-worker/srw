"""Typed context entries, the one injection predicate and the carrier fold.

Spec: knowledge-base/knowledge/plans/append_only_context_injection_wp2_spec.md
§A (schema v1), §B (the fold) and §D (the predicate); design D3, D27, D28 in
knowledge-base/knowledge/features/append_only_context_injection.md.
"""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import random
from types import SimpleNamespace

import pytest
from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)

from agent.core.citation_feedback_injection import (
    create_citation_feedback_injection_messages,
)
from agent.core.guidance_injection import (
    GUIDANCE_TOOL_CALL_ID_PREFIX,
    create_guidance_injection_messages,
)
from agent.core.knowledge_injection import (
    create_charter_injection_messages,
    create_knowledge_injection_messages,
)
from agent.core.memory_injection import create_memory_injection_messages
from agent.managers.todo import TODO_LIST_RESTATEMENT_LEAD
from agent.subagents.runtime import SubagentRuntime
from shared.runtime.core import injection_markers
from shared.runtime.core.context_entries import (
    ENTRY_SEPARATOR,
    FOLDED_KEY,
    INJECTION_KINDS,
    SRW_INJECTION_KEY,
    context_entry_from_row,
    digest,
    entry_body,
    entry_kind,
    entry_meta,
    fold_context_entries,
    has_folded_carrier,
    is_context_entry,
    is_context_injection,
    is_legacy_injection,
    last_user_text,
    make_context_entry,
    memory_handle,
    wrap,
)
from shared.runtime.core.message_markers import (
    PERSIST_ROLE_CONTEXT,
    PERSIST_ROLE_EVENT,
    PERSIST_ROLE_KEY,
)
from shared.runtime.core.skill_resolution import (
    APP_GUIDE_LOADER_TOOL,
    APP_GUIDE_SKILL,
    managed_product_guide_turn_boundary,
)
from shared.runtime.core.workspace_injection import (
    create_instruction_tool_messages,
    create_phase_instruction_message,
    create_todos_human_message,
    is_workspace_injection_message,
)


def _memory(body: str = "[m:3f9a2c] The deploy uses Fleet.") -> HumanMessage:
    return make_context_entry(
        "memory",
        body,
        section="memory",
        items=[{"key": "42", "hash": digest(body), "handle": "m:3f9a2c"}],
    )


def _entry(kind: str, body: str) -> HumanMessage:
    return make_context_entry(kind, body, section=kind)


def _ai_calls(*ids: str) -> AIMessage:
    return AIMessage(
        content="",
        tool_calls=[{"id": i, "name": "read_file", "args": {"path": i}} for i in ids],
    )


# =============================================================================
# §A schema v1
# =============================================================================


class TestSchema:
    def test_entry_is_a_human_message_with_the_v1_metadata(self):
        body = "[m:3f9a2c] The deploy uses Fleet."
        entry = make_context_entry(
            "memory",
            body,
            section="memory",
            items=[{"key": 42, "hash": digest(body), "handle": "m:3f9a2c"}],
            turn=7,
        )

        assert isinstance(entry, HumanMessage)
        assert entry.content == wrap("memory", body)
        assert entry.additional_kwargs[PERSIST_ROLE_KEY] == PERSIST_ROLE_CONTEXT
        assert entry.additional_kwargs[SRW_INJECTION_KEY] == {
            "v": 1,
            "kind": "memory",
            "section": "memory",
            "items": [{"key": "42", "hash": digest(body), "handle": "m:3f9a2c"}],
            "hash": digest(body),
            "turn": 7,
            "visible": False,
        }
        assert is_context_entry(entry)
        assert entry_meta(entry) is entry.additional_kwargs[SRW_INJECTION_KEY]
        assert entry_kind(entry) == "memory"

    def test_state_kinds_carry_the_state_hash_and_no_items(self):
        entry = make_context_entry(
            "citation", "2 failed citations", section="citation", state_hash="ab" * 8
        )
        meta = entry_meta(entry)
        assert meta["hash"] == "ab" * 8
        assert meta["items"] == []
        assert meta["turn"] is None

    def test_turn_boundary_section_is_scoped_to_the_turn(self):
        entry = make_context_entry(
            "turn_boundary", "Return to the request.", section="turn_boundary:7", turn=7
        )
        assert entry_meta(entry)["section"] == "turn_boundary:7"

    def test_unknown_kind_is_rejected(self):
        with pytest.raises(ValueError, match="todos"):
            make_context_entry("todos", "x", section="todos")

    def test_kinds_are_the_append_order(self):
        assert INJECTION_KINDS == (
            "charter",
            "memory",
            "knowledge",
            "citation",
            "guidance",
            "subagents",
            "turn_boundary",
        )

    def test_wrap_strips_the_body(self):
        assert wrap("memory", "\n  fact one\n\n") == (
            '<srw_context kind="memory">\nfact one\n</srw_context>'
        )

    def test_digest_is_the_sha256_prefix(self):
        assert digest("abc") == hashlib.sha256(b"abc").hexdigest()[:16]
        assert len(digest("")) == 16

    def test_memory_handle_is_short_and_stable(self):
        handle = memory_handle(1234)
        assert handle == "m:" + hashlib.sha256(b"1234").hexdigest()[:6]
        assert handle == memory_handle("1234")
        assert memory_handle(1235) != handle

    def test_entry_body_unwraps(self):
        assert entry_body(_entry("guidance", "  Use the staging DB. ")) == (
            "Use the staging DB."
        )
        assert entry_body(HumanMessage(content="plain")) == "plain"

    def test_row_round_trip_replays_the_stored_text(self):
        entry = _memory()
        row = {
            "content": entry.content,
            "additional_kwargs": json.loads(
                json.dumps({SRW_INJECTION_KEY: entry_meta(entry)})
            ),
        }

        restored = context_entry_from_row(
            row["content"], row["additional_kwargs"], id="msg_1"
        )

        assert restored is not None
        assert restored.id == "msg_1"
        assert restored.content == entry.content
        assert entry_meta(restored) == entry_meta(entry)
        assert restored.additional_kwargs[PERSIST_ROLE_KEY] == PERSIST_ROLE_CONTEXT

    def test_row_kwargs_may_arrive_as_json_text(self):
        entry = _memory()
        restored = context_entry_from_row(
            entry.content,
            json.dumps({SRW_INJECTION_KEY: entry_meta(entry)}),
            id=None,
        )
        assert restored is not None and entry_meta(restored) == entry_meta(entry)

    @pytest.mark.parametrize(
        "content, kwargs",
        [
            ("text", None),
            ("text", {}),
            ("text", "{not json"),
            ("text", {SRW_INJECTION_KEY: {"v": 1}}),
            ("", {SRW_INJECTION_KEY: {"v": 1, "kind": "memory"}}),
            (None, {SRW_INJECTION_KEY: {"v": 1, "kind": "memory"}}),
        ],
    )
    def test_unreadable_rows_are_dropped(self, content, kwargs):
        assert context_entry_from_row(content, kwargs, id="x") is None

    def test_an_ai_message_is_never_an_entry(self):
        meta = entry_meta(_memory())
        ai = AIMessage(content="x", additional_kwargs={SRW_INJECTION_KEY: meta})
        tool = ToolMessage(
            content="x", tool_call_id="c1", additional_kwargs={SRW_INJECTION_KEY: meta}
        )
        assert not is_context_entry(ai)
        assert not is_context_entry(tool)


# =============================================================================
# §D the predicate
# =============================================================================


def _active_subagents_text() -> str:
    run = SimpleNamespace(handle="sa-1", subagent_type="explorer", status="running")
    fake_runtime = SimpleNamespace(_background={"sa-1": run}, max_concurrent=4)
    return SubagentRuntime.active_subagents_block(fake_runtime)


def _turn_boundary_text(digest_hex: str = "a" * 64) -> str:
    catalog = {
        "menu": [
            {
                "name": APP_GUIDE_SKILL,
                "system_managed": True,
                "loader_tool": APP_GUIDE_LOADER_TOOL,
                "bundle_digest": digest_hex,
            }
        ]
    }
    return managed_product_guide_turn_boundary(catalog, [APP_GUIDE_LOADER_TOOL])


def _legacy_shapes() -> list[tuple[str, BaseMessage]]:
    shapes: list[tuple[str, BaseMessage]] = []
    for name, (ai, tool) in (
        ("instruction", create_instruction_tool_messages("a.md", "x")),
        ("memory", create_memory_injection_messages("--- Memories ---")),
        ("knowledge", create_knowledge_injection_messages("--- Knowledge ---")),
        ("charter", create_charter_injection_messages("[CHARTER]")),
        ("citation", create_citation_feedback_injection_messages("2 failed")),
        ("guidance", create_guidance_injection_messages("[SUPERVISOR GUIDANCE]")),
    ):
        shapes.append((f"{name}-ai", ai))
        shapes.append((f"{name}-tool", tool))
    shapes.append(("todos", create_todos_human_message("- [ ] one")))
    shapes.append(
        (
            "subagents",
            HumanMessage(
                content=_active_subagents_text(),
                additional_kwargs={PERSIST_ROLE_KEY: PERSIST_ROLE_EVENT},
            ),
        )
    )
    shapes.append(("turn_boundary", HumanMessage(content=_turn_boundary_text())))
    shapes.append(
        ("turn_boundary_no_digest", HumanMessage(content=_turn_boundary_text("")))
    )
    return shapes


def _negatives() -> list[tuple[str, BaseMessage]]:
    from agent.graph import _format_subagent_replies

    return [
        ("user", HumanMessage(content="Please fix the build.")),
        ("answer", AIMessage(content="Done.")),
        ("tool_call", _ai_calls("call_1")),
        ("tool_result", ToolMessage(content="ok", tool_call_id="call_1")),
        ("system", SystemMessage(content="You are an agent.")),
        (
            "phase_block",
            create_phase_instruction_message("plan.md", "Plan.", "strategic", "1:s"),
        ),
        (
            "todo_restatement",
            HumanMessage(content=f"{TODO_LIST_RESTATEMENT_LEAD}\n\n- [ ] one"),
        ),
        (
            "subagent_evidence",
            HumanMessage(
                content=_format_subagent_replies(
                    [{"handle": "sa-1", "thread_id": "t", "message": "found it"}]
                ),
                additional_kwargs={PERSIST_ROLE_KEY: PERSIST_ROLE_EVENT},
            ),
        ),
        (
            "image",
            HumanMessage(
                content=[
                    {"type": "text", "text": "Image content from tool call call_1:"},
                    {"type": "image_url", "image_url": {"url": "data:,"}},
                ]
            ),
        ),
        (
            "event",
            HumanMessage(
                content="[JOB_FINISHED] worker job completed",
                additional_kwargs={PERSIST_ROLE_KEY: PERSIST_ROLE_EVENT},
            ),
        ),
        ("mentions_tag", HumanMessage(content="what is <active_subagents>?")),
        (
            "folded_carrier",
            HumanMessage(content="hi", additional_kwargs={FOLDED_KEY: ["memory"]}),
        ),
        ("tool_without_id", ToolMessage(content="x", tool_call_id="")),
    ]


class TestPredicate:
    @pytest.mark.parametrize(
        "message",
        [m for _, m in _legacy_shapes()],
        ids=[n for n, _ in _legacy_shapes()],
    )
    def test_every_legacy_tail_shape_is_an_injection(self, message):
        assert is_legacy_injection(message)
        assert is_context_injection(message)
        assert not is_context_entry(message)
        assert is_workspace_injection_message(message)  # the old name, aliased

    def test_the_legacy_matrix_covers_the_nine_shapes(self):
        kinds = {name.rsplit("-", 1)[0] for name, _ in _legacy_shapes()}
        assert {
            "instruction",
            "memory",
            "knowledge",
            "charter",
            "citation",
            "guidance",
            "todos",
            "subagents",
            "turn_boundary",
        } <= kinds

    @pytest.mark.parametrize("kind", INJECTION_KINDS)
    def test_typed_entries_are_injections_but_not_legacy(self, kind):
        entry = _entry(kind, f"{kind} body")
        assert is_context_entry(entry)
        assert is_context_injection(entry)
        assert not is_legacy_injection(entry)

    @pytest.mark.parametrize(
        "message", [m for _, m in _negatives()], ids=[n for n, _ in _negatives()]
    )
    def test_history_is_not_an_injection(self, message):
        assert not is_context_injection(message)
        assert not is_legacy_injection(message)

    def test_the_alias_is_the_predicate(self):
        assert is_workspace_injection_message is is_context_injection

    def test_guidance_prefix_has_one_home(self):
        assert GUIDANCE_TOOL_CALL_ID_PREFIX is (
            injection_markers.GUIDANCE_TOOL_CALL_ID_PREFIX
        )

    def test_producers_emit_the_marker_prefixes(self):
        assert _active_subagents_text().startswith(
            injection_markers.ACTIVE_SUBAGENTS_CONTENT_PREFIX
        )
        assert _turn_boundary_text().startswith(
            injection_markers.PRODUCT_GUIDE_TURN_BOUNDARY_CONTENT_PREFIX
        )


class TestLastUserText:
    def test_skips_injections_and_returns_the_user_message(self):
        messages = [
            HumanMessage(content="first"),
            AIMessage(content="ok"),
            HumanMessage(content="what did we decide?"),
            _memory(),
            HumanMessage(content=_turn_boundary_text()),
        ]
        assert last_user_text(messages) == "what did we decide?"

    def test_list_content_is_string_coerced(self):
        content = [{"type": "text", "text": "look"}]
        assert last_user_text([HumanMessage(content=content)]) == str(content)

    def test_empty_without_a_user_message(self):
        assert last_user_text([AIMessage(content="x"), _memory()]) == ""


# =============================================================================
# §B the carrier fold
# =============================================================================


class TestFold:
    def test_without_entries_it_returns_the_same_objects(self):
        messages = [
            SystemMessage(content="sys"),
            HumanMessage(content="hi"),
            *create_memory_injection_messages("--- Memories ---"),
        ]
        folded = fold_context_entries(messages)
        assert folded is not messages
        assert len(folded) == len(messages)
        assert all(a is b for a, b in zip(folded, messages))

    def test_string_carrier(self):
        user = HumanMessage(content="hi", id="u1")
        entry = _memory()

        folded = fold_context_entries([user, entry])

        assert len(folded) == 1
        assert folded[0].content == "hi" + ENTRY_SEPARATOR + entry.content
        assert folded[0].id == "u1"
        assert folded[0].additional_kwargs == {FOLDED_KEY: ["memory"]}
        assert user.content == "hi" and user.additional_kwargs == {}

    def test_empty_carrier_gets_no_leading_separator(self):
        entry = _memory()
        folded = fold_context_entries([HumanMessage(content=""), entry])
        assert folded[0].content == entry.content

    def test_list_carrier_gets_one_text_part_per_entry(self):
        parts = [
            {"type": "text", "text": "see the screenshot"},
            {"type": "image_url", "image_url": {"url": "data:,"}},
        ]
        user = HumanMessage(content=parts)
        memory, knowledge = _memory(), _entry("knowledge", "note")

        folded = fold_context_entries([user, memory, knowledge])

        assert folded[0].content == parts + [
            {"type": "text", "text": memory.content},
            {"type": "text", "text": knowledge.content},
        ]
        assert user.content == parts and len(user.content) == 2

    def test_human_carrier_keeps_its_own_kwargs(self):
        event = HumanMessage(
            content="[JOB_FINISHED] done",
            additional_kwargs={PERSIST_ROLE_KEY: PERSIST_ROLE_EVENT, "_srw_turn_id": 3},
        )
        folded = fold_context_entries([event, _memory()])
        assert folded[0].additional_kwargs == {
            PERSIST_ROLE_KEY: PERSIST_ROLE_EVENT,
            "_srw_turn_id": 3,
            FOLDED_KEY: ["memory"],
        }
        assert FOLDED_KEY not in event.additional_kwargs

    def test_several_entries_fold_in_history_order(self):
        charter = _entry("charter", "standing orders")
        memory = _memory()
        guidance = _entry("guidance", "use staging")
        user = HumanMessage(content="go")

        folded = fold_context_entries([user, charter, memory, guidance])

        assert folded[0].content == ENTRY_SEPARATOR.join(
            ["go", charter.content, memory.content, guidance.content]
        )
        assert folded[0].additional_kwargs[FOLDED_KEY] == [
            "charter",
            "memory",
            "guidance",
        ]

    def test_tool_result_carrier(self):
        ai = _ai_calls("c1")
        result = ToolMessage(content="file", tool_call_id="c1", name="read_file")
        memory = _memory()

        folded = fold_context_entries([ai, result, memory])

        assert folded[0] is ai
        assert isinstance(folded[1], ToolMessage)
        assert folded[1].tool_call_id == "c1" and folded[1].name == "read_file"
        assert folded[1].content == "file" + ENTRY_SEPARATOR + memory.content

    def test_entry_between_batch_results_goes_to_the_last_result(self):
        ai = _ai_calls("a", "b", "c")
        ra = ToolMessage(content="A", tool_call_id="a")
        rb = ToolMessage(content="B", tool_call_id="b")
        rc = ToolMessage(content="C", tool_call_id="c")
        early, late = _memory(), _entry("knowledge", "note")

        folded = fold_context_entries([ai, ra, early, rb, rc, late])

        assert folded[:3] == [ai, ra, rb]
        assert folded[1] is ra and folded[2] is rb
        assert folded[3].content == ENTRY_SEPARATOR.join(
            ["C", early.content, late.content]
        )
        assert folded[3].additional_kwargs[FOLDED_KEY] == ["memory", "knowledge"]

    def test_batch_guard_stops_at_the_next_assistant_message(self):
        first = [_ai_calls("a"), ToolMessage(content="A", tool_call_id="a")]
        entry = _memory()
        second = [_ai_calls("b"), ToolMessage(content="B", tool_call_id="b")]

        folded = fold_context_entries(first + [entry] + second)

        assert folded[1].content == "A" + ENTRY_SEPARATOR + entry.content
        assert folded[3] is second[1]

    def test_after_a_text_answer_the_entry_stands_alone(self):
        answer = AIMessage(content="Here you go.")
        entry = _memory()

        folded = fold_context_entries([HumanMessage(content="q"), answer, entry])

        assert folded[1] is answer
        assert isinstance(folded[2], HumanMessage)
        assert folded[2].content == entry.content
        assert folded[2].additional_kwargs == {FOLDED_KEY: ["memory"]}
        assert not is_context_entry(folded[2])

    @pytest.mark.parametrize(
        "prefix", [[], [SystemMessage(content="sys")]], ids=["no-carrier", "system"]
    )
    def test_without_a_carrier_the_entry_stands_alone(self, prefix):
        entry = _memory()
        folded = fold_context_entries(prefix + [entry])
        assert folded[-1].content == entry.content
        assert folded[-1].additional_kwargs == {FOLDED_KEY: ["memory"]}

    def test_after_a_tool_call_the_entry_is_dropped(self, caplog):
        ai = _ai_calls("a")
        result = ToolMessage(content="A", tool_call_id="a")

        with caplog.at_level(logging.WARNING):
            folded = fold_context_entries([ai, _memory(), result])

        assert folded == [ai, result]
        assert folded[1] is result
        assert "memory context entry" in caplog.text

    def test_folded_view_has_no_entries_and_says_so(self):
        folded = fold_context_entries([HumanMessage(content="q"), _memory()])
        assert not any(is_context_entry(m) for m in folded)
        assert has_folded_carrier(folded)
        assert not has_folded_carrier([HumanMessage(content="q")])

    def test_idempotent(self):
        history = [
            HumanMessage(content="q"),
            _memory(),
            _ai_calls("a"),
            ToolMessage(content="A", tool_call_id="a"),
            _entry("guidance", "g"),
        ]
        once = fold_context_entries(history)
        assert fold_context_entries(once) == once

    def test_never_mutates_its_input(self):
        history = [
            HumanMessage(content=[{"type": "text", "text": "q"}]),
            _memory(),
            _ai_calls("a", "b"),
            ToolMessage(content="A", tool_call_id="a"),
            _entry("citation", "c"),
            ToolMessage(content="B", tool_call_id="b"),
            AIMessage(content="answer"),
            _entry("subagents", "s"),
        ]
        before = copy.deepcopy(history)
        fold_context_entries(history)
        assert history == before


# =============================================================================
# §B guarantees over random histories
# =============================================================================


def _random_history(rng: random.Random) -> tuple[list[BaseMessage], list[int]]:
    """A request-shaped history and the indices where a unit starts.

    Units: a user message (string or list content), a text answer, or a
    tool-call batch with all its results. Entries follow any unit, may sit
    between the results of one batch, and occasionally directly after a call
    (the drop case). A cut at a unit start never splits a batch, which is the
    only place entries are appended in practice (at request build, after the
    batch's results are all present).
    """
    history: list[BaseMessage] = []
    starts: list[int] = []
    if rng.random() < 0.7:
        history.append(SystemMessage(content="system prompt"))

    def entries() -> list[BaseMessage]:
        out = []
        for _ in range(rng.choice((0, 0, 1, 1, 2, 3))):
            kind = rng.choice(INJECTION_KINDS)
            out.append(_entry(kind, f"{kind} {rng.randrange(1000)}"))
        return out

    for unit_no in range(rng.randrange(1, 9)):
        starts.append(len(history))
        unit = rng.choice(("user", "user", "answer", "batch", "batch", "batch"))
        if unit == "user":
            if rng.random() < 0.25:
                content = [{"type": "text", "text": f"look {unit_no}"}]
                history.append(HumanMessage(content=content))
            else:
                history.append(HumanMessage(content=f"ask {unit_no}"))
        elif unit == "answer":
            history.append(AIMessage(content=f"answer {unit_no}"))
        else:
            ids = [f"c{unit_no}_{i}" for i in range(rng.randrange(1, 4))]
            history.append(_ai_calls(*ids))
            if rng.random() < 0.1:
                history.extend(entries())  # dropped: between call and results
            for tool_call_id in ids:
                history.append(
                    ToolMessage(content=f"r {tool_call_id}", tool_call_id=tool_call_id)
                )
                if rng.random() < 0.3:
                    history.extend(entries())
        history.extend(entries())
    starts.append(len(history))
    return history, starts


def _is_standalone(message: BaseMessage) -> bool:
    return (
        isinstance(message, HumanMessage)
        and set(message.additional_kwargs) == {FOLDED_KEY}
        and isinstance(message.content, str)
        and message.content.startswith("<srw_context ")
    )


def _unfold(message: BaseMessage) -> BaseMessage:
    """The original carrier of a folded copy (entry texts removed)."""
    kinds = message.additional_kwargs.get(FOLDED_KEY)
    if not kinds:
        return message
    kwargs = {k: v for k, v in message.additional_kwargs.items() if k != FOLDED_KEY}
    if isinstance(message.content, list):
        content = message.content[: -len(kinds)]
    else:
        content = ENTRY_SEPARATOR.join(
            message.content.split(ENTRY_SEPARATOR)[: -len(kinds)]
        )
    return message.model_copy(update={"content": content, "additional_kwargs": kwargs})


def _expected_drops(history: list[BaseMessage]) -> int:
    drops, carrier = 0, None
    for message in history:
        if not is_context_entry(message):
            carrier = message
        elif isinstance(carrier, AIMessage) and carrier.tool_calls:
            drops += 1
    return drops


@pytest.mark.parametrize("seed", range(200))
def test_fold_guarantees_over_random_histories(seed):
    rng = random.Random(seed)
    history, starts = _random_history(rng)
    before = copy.deepcopy(history)

    folded = fold_context_entries(history)

    assert history == before  # never mutates
    assert fold_context_entries(history) == folded  # deterministic
    assert fold_context_entries(folded) == folded  # idempotent
    assert not any(is_context_entry(m) for m in folded)

    # Every non-entry message survives in order; carriers only gain text.
    assert [_unfold(m) for m in folded if not _is_standalone(m)] == [
        m for m in history if not is_context_entry(m)
    ]
    # Every entry is delivered exactly once, unless it follows a tool call.
    entries = [m for m in history if is_context_entry(m)]
    delivered = sum(len(m.additional_kwargs.get(FOLDED_KEY, [])) for m in folded)
    assert delivered == len(entries) - _expected_drops(history)

    # Prefix stability: fold(H) is a prefix of fold(H + X) when X starts with
    # a non-entry message that does not continue an open tool batch.
    for cut in starts:
        prefix = fold_context_entries(history[:cut])
        assert folded[: len(prefix)] == prefix, f"seed={seed} cut={cut}"
