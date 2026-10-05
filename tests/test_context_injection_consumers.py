"""Consumers of the one injection predicate (WP2 spec §D, sub-step 2.1).

Every reader that must tell injected context from conversation asks
``is_context_injection``: the chat_history archiver, the summarizer and
extraction formatters, the retrieval query builders, fork seeding, the
subagent driver's tail check and the MCP chat formatter. Three of them carry
the B4 fixes of the WP0 code survey: the archiver anchored its delta on the
supervisor-guidance pair's synthetic AIMessage, archived the charter as
``other`` and the App Guide boundary and active-subagent status as human
input.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

from agent.core.archiver import LLMArchiver
from agent.core.context import ContextConfig, ContextManager
from agent.core.guidance_injection import create_guidance_injection_messages
from agent.core.knowledge_injection import create_charter_injection_messages
from agent.core.memory_injection import create_memory_injection_messages
from agent.services.memory.extraction_engine import MemoryExtractionEngine
from agent.services.memory.plugins.legacy import build_persistent_query_text
from agent.services.memory.query import build_digest_query_text
from agent.subagents.driver import SubagentDriver
from agent.subagents.fork import FORK_NOTICE, seed_fork_history
from shared.orch_surface.formatters import _context_label, _format_chat_entry
from shared.runtime.core.context_entries import (
    digest,
    is_context_entry,
    make_context_entry,
)
from shared.runtime.core.message_markers import PERSIST_ROLE_EVENT, PERSIST_ROLE_KEY

BOUNDARY = (
    '<managed_product_guide_turn_boundary current_bundle_sha256="'
    + "a" * 64
    + "\">\nReturn to the current user's request above.\n"
    "</managed_product_guide_turn_boundary>"
)
SUBAGENTS = (
    "<active_subagents>\n- sa-1 (explorer): running\n"
    "Reports push automatically as evidence; do not poll.\n</active_subagents>"
)


def _entry(kind: str, body: str):
    return make_context_entry(kind, body, section=kind)


def _memory_entry(body: str = "[m:3f9a2c] Deploys go through Fleet."):
    return make_context_entry(
        "memory",
        body,
        section="memory",
        items=[{"key": "7", "hash": digest(body), "handle": "m:3f9a2c"}],
    )


def _calls(*ids: str) -> AIMessage:
    return AIMessage(
        content="",
        tool_calls=[{"id": i, "name": "read_file", "args": {"path": i}} for i in ids],
    )


# =============================================================================
# Archiver: delta anchor and context kinds (B4)
# =============================================================================


def _archive(messages, *, job_id="job-1", archiver=None):
    archiver = archiver or LLMArchiver(writer=MagicMock())
    archiver._archive_chat_entry(
        job_id=job_id,
        agent_type="worker",
        messages=messages,
        response=AIMessage(content="next step"),
        model="m",
        latency_ms=1,
        iteration=1,
        request_id="r1",
        phase="tactical",
        phase_number=2,
    )
    row = archiver._writer.insert_chat_entry.call_args[0][0]
    humans = [i for i in row["inputs"] if i["type"] in ("human", "tool")]
    context = [i for i in row["inputs"] if i["type"] == "context"]
    return humans, context, archiver


class TestArchiver:
    def test_guidance_pair_no_longer_hides_the_tool_results(self):
        """Legacy B4: the guidance pair's AIMessage used to be the delta anchor."""
        guid_ai, guid_tool = create_guidance_injection_messages(
            "[SUPERVISOR GUIDANCE] use staging"
        )
        mem_ai, mem_tool = create_memory_injection_messages("--- Memories ---")
        messages = [
            SystemMessage(content="sys"),
            HumanMessage(content="task"),
            _calls("c1", "c2"),
            ToolMessage(content="result one", tool_call_id="c1"),
            ToolMessage(content="result two", tool_call_id="c2"),
            mem_ai,
            mem_tool,
            guid_ai,
            guid_tool,
        ]

        humans, context, _ = _archive(messages)

        assert [(i["type"], i["content"]) for i in humans] == [
            ("tool", "result one"),
            ("tool", "result two"),
        ]
        assert [c["kind"] for c in context] == ["memory", "guidance"]

    def test_session_tail_is_archived_as_context(self):
        """Charter -> charter (was "other"); subagents and the App Guide
        boundary are context, not human input (B4)."""
        ch_ai, ch_tool = create_charter_injection_messages("[CHARTER] orders")
        messages = [
            SystemMessage(content="sys"),
            HumanMessage(content="hello"),
            AIMessage(content="hi"),
            HumanMessage(content="what next?"),
            ch_ai,
            ch_tool,
            HumanMessage(
                content=SUBAGENTS,
                additional_kwargs={PERSIST_ROLE_KEY: PERSIST_ROLE_EVENT},
            ),
            HumanMessage(content=BOUNDARY),
        ]

        humans, context, _ = _archive(messages)

        assert [i["content"] for i in humans] == ["what next?"]
        assert [c["kind"] for c in context] == ["charter", "subagents", "turn_boundary"]

    def test_typed_entries_in_the_delta_use_their_metadata_kind(self):
        messages = [
            SystemMessage(content="sys"),
            HumanMessage(content="task"),
            _calls("c1"),
            ToolMessage(content="result", tool_call_id="c1"),
            _memory_entry(),
            _entry("citation", "2 failed citations"),
            _entry("guidance", "use staging"),
        ]

        humans, context, _ = _archive(messages)

        assert [i["content"] for i in humans] == ["result"]
        assert [c["kind"] for c in context] == [
            "memory",
            "citation_feedback",
            "guidance",
        ]
        assert all("content" in c for c in context)  # first sighting: full text

    def test_entries_before_the_last_answer_are_not_re_archived(self):
        old = _memory_entry("[m:111111] an older fact")
        messages = [
            SystemMessage(content="sys"),
            HumanMessage(content="task"),
            old,
            _calls("c1"),
            ToolMessage(content="result", tool_call_id="c1"),
        ]

        humans, context, _ = _archive(messages)

        assert [i["content"] for i in humans] == ["result"]
        assert context == []

    def test_unchanged_context_is_stored_without_content(self):
        mem_ai, mem_tool = create_memory_injection_messages("--- Memories ---")
        request = [HumanMessage(content="task"), mem_ai, mem_tool]
        _, first, archiver = _archive(request)
        archiver._writer.insert_chat_entry.reset_mock()

        _, second, _ = _archive(request, archiver=archiver)

        assert "content" in first[0]
        assert "content" not in second[0]


# =============================================================================
# Summarizer and extraction formatters: no entries (guidance kept, O1)
# =============================================================================


@pytest.fixture
def context_manager():
    return ContextManager(
        config=ContextConfig(
            compaction_threshold_tokens=1000,
            summarization_threshold_tokens=1000,
            keep_recent_messages=3,
            model_max_context_tokens=2000,
        ),
        model="gpt-4",
    )


class TestSummaryFormatter:
    def test_entries_are_left_out_but_guidance_is_kept(self, context_manager):
        mem_ai, mem_tool = create_memory_injection_messages("LEGACY_MEMORY")
        messages = [
            HumanMessage(content="Fix the deploy."),
            _memory_entry("[m:3f9a2c] ENTRY_MEMORY"),
            _calls("c1"),
            ToolMessage(content="deploy log", tool_call_id="c1", name="read_file"),
            _entry("knowledge", "ENTRY_KNOWLEDGE"),
            _entry("guidance", "Use the staging cluster first."),
            HumanMessage(content=BOUNDARY),
            mem_ai,
            mem_tool,
        ]

        parts = context_manager._format_messages_for_summary(messages)
        text = "\n".join(parts)

        assert "[User]: Fix the deploy." in parts
        assert "[Supervisor guidance]: Use the staging cluster first." in parts
        for leaked in (
            "ENTRY_MEMORY",
            "ENTRY_KNOWLEDGE",
            "LEGACY_MEMORY",
            "srw_context",
        ):
            assert leaked not in text
        assert "managed_product_guide_turn_boundary" not in text
        assert "[Tool result: read_file]: deploy log" in text


class TestExtractionFormatter:
    def test_engine_format_skips_entries_and_legacy_pieces(self):
        engine = MemoryExtractionEngine(
            SimpleNamespace(max_context_tokens=8000),
            None,
            extraction_prompt="",
            token_counter=lambda text: len(text) // 4,
            output_reserve=200,
        )
        guid_ai, guid_tool = create_guidance_injection_messages("LEGACY_GUIDANCE")
        messages = [
            HumanMessage(content="We chose Postgres."),
            _memory_entry("[m:3f9a2c] ENTRY_MEMORY"),
            _entry("guidance", "ENTRY_GUIDANCE"),
            AIMessage(content="Noted."),
            guid_ai,
            guid_tool,
        ]

        parts = engine._format(messages)

        assert parts == ["[User] We chose Postgres.", "[Agent] Noted."]


# =============================================================================
# Retrieval query builders never query with injected text
# =============================================================================


class TestQueryBuilders:
    def test_digest_window_counts_only_conversation(self):
        messages = [
            HumanMessage(content="deploy the app"),
            AIMessage(content="which cluster?"),
            HumanMessage(content="staging"),
            _memory_entry("[m:3f9a2c] ENTRY_MEMORY"),
            _entry("guidance", "ENTRY_GUIDANCE"),
            HumanMessage(content=BOUNDARY),
        ]

        query = build_digest_query_text(messages, None, window=3)

        assert query == "deploy the app\nwhich cluster?\nstaging"

    def test_persistent_query_is_the_last_user_message(self):
        messages = [
            HumanMessage(content="what did we decide about X?"),
            _memory_entry("[m:3f9a2c] ENTRY_MEMORY"),
            HumanMessage(content=BOUNDARY),
        ]
        assert build_persistent_query_text(messages) == "what did we decide about X?"

    def test_persistent_query_without_entries_is_unchanged(self):
        content = [{"type": "text", "text": "look"}]
        assert build_persistent_query_text([HumanMessage(content=content)]) == str(
            content
        )
        assert build_persistent_query_text([AIMessage(content="x")]) == ""


# =============================================================================
# Subagents: fork seeding and the driver's tail check
# =============================================================================


class TestSubagents:
    def test_fork_seed_skips_injected_context(self):
        mem_ai, mem_tool = create_memory_injection_messages("LEGACY")
        parent = [
            SystemMessage(content="[Summary of prior work] s"),
            HumanMessage(content="task"),
            _memory_entry(),
            _calls("c1"),
            ToolMessage(content="r", tool_call_id="c1"),
            _entry("guidance", "g"),
            AIMessage(content="done"),
            mem_ai,
            mem_tool,
        ]

        seed = seed_fork_history(parent)

        assert not any(is_context_entry(m) for m in seed)
        assert [type(m).__name__ for m in seed] == [
            "SystemMessage",
            "HumanMessage",
            "AIMessage",
            "ToolMessage",
            "AIMessage",
            "HumanMessage",
        ]
        assert seed[-1].content == FORK_NOTICE

    @pytest.mark.parametrize(
        "brief, expected",
        [
            ([HumanMessage(content="go")], "human"),
            ([HumanMessage(content="go"), _memory_entry()], "human"),
            ([_calls("c1"), ToolMessage(content="r", tool_call_id="c1")], "tool"),
            (
                [
                    _calls("c1"),
                    ToolMessage(content="r", tool_call_id="c1"),
                    _entry("subagents", "s"),
                ],
                "tool",
            ),
            ([AIMessage(content="answer"), _entry("guidance", "g")], "ai"),
            ([_memory_entry()], "none"),
        ],
    )
    def test_tail_kind_ignores_entries(self, brief, expected):
        driver = SimpleNamespace(_brief_messages=lambda: brief)
        assert SubagentDriver._tail_kind(driver) == expected


# =============================================================================
# MCP chat formatter
# =============================================================================


class TestChatFormatter:
    @pytest.mark.parametrize(
        "element, kind",
        [
            ({"type": "tool", "tool_call_id": "charter_inject_ab12cd34"}, "charter"),
            ({"type": "tool", "tool_call_id": "guidance_inject_ab12cd34"}, "guidance"),
            ({"type": "tool", "tool_call_id": "memory_inject_ab12cd34"}, "memory"),
            ({"type": "human", "content_preview": "<active_tasks>\n- x"}, "todos"),
            ({"type": "human", "content_preview": SUBAGENTS}, "subagents"),
            ({"type": "human", "content_preview": BOUNDARY}, "turn_boundary"),
            (
                {"type": "human", "content_preview": "[phase: tactical] x"},
                "phase_instruction",
            ),
            ({"type": "context", "kind": "memory"}, "memory"),
        ],
    )
    def test_injected_inputs_get_a_kind(self, element, kind):
        assert _context_label(element) == kind

    @pytest.mark.parametrize(
        "element",
        [
            {"type": "human", "content_preview": "please check <active_subagents>"},
            {"type": "tool", "tool_call_id": "call_abc"},
        ],
    )
    def test_conversation_inputs_get_none(self, element):
        assert _context_label(element) is None

    def test_context_line_says_injected_context(self):
        entry = {
            "turn_number": 3,
            "inputs": [
                {"type": "human", "content_preview": "go"},
                {"type": "tool", "tool_call_id": "guidance_inject_1", "content": "g"},
                {"type": "human", "content_preview": BOUNDARY},
            ],
            "response": {"content": "ok"},
        }
        lines = _format_chat_entry(entry, 3)
        assert "[human]: go" in lines
        assert "[context]: guidance, turn_boundary (injected context)" in lines
        assert not any("re-injected each turn" in line for line in lines)
