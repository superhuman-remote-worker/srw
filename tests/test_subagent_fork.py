"""``fork=true`` seeding (U3 WP1, plan B.7): a pure function over messages."""

from __future__ import annotations

import re
from unittest.mock import patch

from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    RemoveMessage,
    SystemMessage,
    ToolMessage,
)

from shared.runtime.core.message_markers import PERSIST_ROLE_KEY
from shared.runtime.core.workspace_injection import create_phase_instruction_message
from agent.subagents import seed_fork_history
from agent.subagents.fork import FORK_NOTICE, _conforming_tool_call_id


def _parent_history():
    return [
        SystemMessage(content="[Summary of prior work] parent state", id="parent-sys"),
        HumanMessage(content="turn 1", id="parent-human-1"),
        create_phase_instruction_message(
            "skills/x/SKILL.md", "phase body", "tactical", "2:tactical"
        ),
        AIMessage(
            content="",
            id="parent-ai-call",
            tool_calls=[
                {"id": "p1", "name": "list_files", "args": {}, "type": "tool_call"}
            ],
        ),
        ToolMessage(content="a.md", tool_call_id="p1", id="parent-tool"),
        AIMessage(content="one file", id="parent-ai-answer"),
        RemoveMessage(id="msg_old"),
        ToolMessage(
            content="orphan result", tool_call_id="never_called", id="parent-orphan"
        ),
        HumanMessage(content="turn 2", id="parent-human-2"),
        AIMessage(
            content="",
            id="parent-open",
            tool_calls=[
                {
                    "id": "p2_open",
                    "name": "read_file",
                    "args": {"path": "a"},
                    "type": "tool_call",
                }
            ],
        ),
    ]


class TestSeedForkHistory:
    def test_preserves_summary_and_drops_protected_orphans_and_open_calls(self):
        seed = seed_fork_history(_parent_history())
        assert isinstance(seed[0], SystemMessage)
        assert seed[0].content == "[Summary of prior work] parent state"
        assert not any("phase body" in str(m.content) for m in seed)
        assert not any(type(m).__name__ == "RemoveMessage" for m in seed)
        assert not any(
            isinstance(m, ToolMessage) and m.tool_call_id == "never_called"
            for m in seed
        )
        assert not any(
            isinstance(m, AIMessage)
            and m.tool_calls
            and m.tool_calls[0]["id"] == "p2_open"
            for m in seed
        )
        kinds = [type(m).__name__ for m in seed]
        assert kinds == [
            "SystemMessage",
            "HumanMessage",
            "AIMessage",
            "ToolMessage",
            "AIMessage",
            "HumanMessage",
            "HumanMessage",
        ]
        notice = seed[-1]
        assert notice.content == FORK_NOTICE
        assert notice.additional_kwargs[PERSIST_ROLE_KEY] == "event"
        assert "brief" in FORK_NOTICE

    def test_legacy_summaries_survive_wherever_they_sit(self):
        history = [
            SystemMessage(content="[Summary of prior work] leading"),
            HumanMessage(content="hi"),
            SystemMessage(content="[Summary of prior work] mid-history"),
            AIMessage(content="ok"),
        ]
        seed = seed_fork_history(history)
        summaries = [m.content for m in seed if isinstance(m, SystemMessage)]
        assert summaries == [
            "[Summary of prior work] leading",
            "[Summary of prior work] mid-history",
        ]

    def test_drops_the_parents_prompt_and_nudges(self):
        """A session's durable list starts with its own prompt and a worker's
        state holds nudges; neither is history for the child, which runs under
        its own prompt (a second system message breaks one-system templates)."""
        history = [
            SystemMessage(content="Session prompt."),
            HumanMessage(content="hi"),
            AIMessage(
                content="",
                tool_calls=[
                    {"id": "p1", "name": "list_files", "args": {}, "type": "tool_call"}
                ],
            ),
            ToolMessage(content="a.md", tool_call_id="p1"),
            SystemMessage(content="OBSERVATION: 5 tool calls since the last write."),
            AIMessage(content="ok"),
        ]
        seed = seed_fork_history(history)
        assert not any(isinstance(m, SystemMessage) for m in seed)
        assert [type(m).__name__ for m in seed] == [
            "HumanMessage",
            "AIMessage",
            "ToolMessage",
            "AIMessage",
            "HumanMessage",
        ]

    def test_never_mutates_or_shares_nested_state_with_parent_messages(self):
        history = _parent_history()
        before = [m.model_dump() for m in history]
        seed = seed_fork_history(history)
        assert [m.model_dump() for m in history] == before
        assert all(s is not p for s in seed for p in history)
        fork_call = next(m for m in seed if isinstance(m, AIMessage) and m.tool_calls)
        fork_call.tool_calls[0]["args"]["new"] = "child-only"
        parent_call = next(
            m for m in history if isinstance(m, AIMessage) and m.tool_calls
        )
        assert "new" not in parent_call.tool_calls[0]["args"]

    def test_every_message_and_tool_pair_gets_fresh_child_owned_ids(self):
        history = _parent_history()
        parent_message_ids = {m.id for m in history if getattr(m, "id", None)}
        seed = seed_fork_history(history, child_model="gpt-5.5")
        child_message_ids = [m.id for m in seed]
        assert all(child_message_ids)
        assert len(child_message_ids) == len(set(child_message_ids))
        assert not parent_message_ids.intersection(child_message_ids)
        ai = next(m for m in seed if isinstance(m, AIMessage) and m.tool_calls)
        tool = next(m for m in seed if isinstance(m, ToolMessage))
        fresh_call_id = ai.tool_calls[0]["id"]
        assert fresh_call_id != "p1"
        assert re.fullmatch(r"call_[a-f0-9]{24}", fresh_call_id)
        assert tool.tool_call_id == fresh_call_id

    def test_remints_raw_chat_anthropic_and_responses_call_representations(self):
        parent_ai = AIMessage(
            id="parent-ai",
            content=[
                {
                    "type": "tool_use",
                    "id": "parent-call",
                    "name": "read_file",
                    "input": {"path": "a.md"},
                },
                {
                    "type": "function_call",
                    "id": "provider-item-parent",
                    "call_id": "parent-call",
                    "name": "read_file",
                    "arguments": '{"path":"a.md"}',
                },
                {
                    "type": "tool_call",
                    "id": "parent-call",
                    "name": "read_file",
                    "args": {"path": "a.md"},
                    "extras": {"item_id": "provider-item-parent-v1"},
                },
            ],
            tool_calls=[
                {
                    "id": "parent-call",
                    "name": "read_file",
                    "args": {"path": "a.md"},
                    "type": "tool_call",
                }
            ],
            additional_kwargs={
                "tool_calls": [
                    {
                        "id": "parent-call",
                        "type": "function",
                        "function": {
                            "name": "read_file",
                            "arguments": '{"path":"a.md"}',
                        },
                    }
                ]
            },
            response_metadata={
                "provider_raw": {
                    "output": [
                        {
                            "type": "function_call",
                            "id": "provider-item-parent-2",
                            "call_id": "parent-call",
                            "name": "read_file",
                            "arguments": '{"path":"a.md"}',
                        }
                    ]
                }
            },
        )
        parent_tool = ToolMessage(
            content=[
                {
                    "type": "tool_result",
                    "tool_use_id": "parent-call",
                    "content": "contents",
                }
            ],
            tool_call_id="parent-call",
            id="parent-tool",
            additional_kwargs={"call_id": "parent-call"},
            response_metadata={"tool_call_id": "parent-call"},
        )
        before = [parent_ai.model_dump(), parent_tool.model_dump()]

        seed = seed_fork_history(
            [parent_ai, parent_tool], child_model="gpt-5.5", parent_model="gpt-5.5"
        )
        ai = next(m for m in seed if isinstance(m, AIMessage))
        tool = next(m for m in seed if isinstance(m, ToolMessage))
        fresh = ai.tool_calls[0]["id"]
        assert ai.additional_kwargs["tool_calls"][0]["id"] == fresh
        assert ai.content[0]["id"] == fresh
        assert ai.content[1]["call_id"] == fresh
        # Responses item ids are server-owned anchors, not pairing ids. A
        # self-contained child fork drops them and remints only ``call_id``.
        assert "id" not in ai.content[1]
        assert ai.content[2]["id"] == fresh
        assert "item_id" not in ai.content[2]["extras"]
        raw = ai.response_metadata["provider_raw"]["output"][0]
        assert raw["call_id"] == fresh
        assert "id" not in raw
        assert tool.tool_call_id == fresh
        assert tool.content[0]["tool_use_id"] == fresh
        assert tool.additional_kwargs["call_id"] == fresh
        assert tool.response_metadata["tool_call_id"] == fresh
        assert [parent_ai.model_dump(), parent_tool.model_dump()] == before

    def test_mistral_fork_ids_are_fresh_nine_alphanumeric_values(self):
        with patch(
            "agent.subagents.fork._conforming_tool_call_id",
            wraps=_conforming_tool_call_id,
        ) as conforming:
            seed = seed_fork_history(
                [
                    AIMessage(
                        content="",
                        tool_calls=[{"id": "aB3dE6gH9", "name": "t", "args": {}}],
                    ),
                    ToolMessage(content="r", tool_call_id="aB3dE6gH9"),
                ],
                child_model="mistral-large-latest",
                parent_model="mistral-large-latest",
            )
        ai = next(m for m in seed if isinstance(m, AIMessage))
        tool = next(m for m in seed if isinstance(m, ToolMessage))
        assert ai.tool_calls[0]["id"] != "aB3dE6gH9"
        assert re.fullmatch(r"[A-Za-z0-9]{9}", ai.tool_calls[0]["id"])
        assert tool.tool_call_id == ai.tool_calls[0]["id"]
        assert conforming.call_count == 1
        assert conforming.call_args.args[1] == "mistral"

    def test_empty_history_is_just_the_notice(self):
        seed = seed_fork_history([])
        assert len(seed) == 1 and seed[0].content == FORK_NOTICE

    def test_sanitizes_across_model_families(self):
        history = [HumanMessage(content="hi"), AIMessage(content="ok")]
        with patch(
            "agent.subagents.fork.sanitize_history_for_provider_boundary",
            wraps=lambda msgs, model: list(msgs),
        ) as spy:
            seed_fork_history(
                history, child_model="claude-sonnet-4-5", parent_model="gpt-4o"
            )
            assert spy.call_count == 1
            assert spy.call_args.args[1] == "claude-sonnet-4-5"
            spy.reset_mock()
            seed_fork_history(history, child_model="gpt-4o-mini", parent_model="gpt-4o")
            assert spy.call_count == 0
            seed_fork_history(history, child_model=None, parent_model="gpt-4o")
            assert spy.call_count == 0

    def test_cross_family_sanitization_happens_before_fresh_id_remint(self):
        history = [
            AIMessage(
                content="",
                tool_calls=[{"id": "parent-call", "name": "t", "args": {}}],
            ),
            ToolMessage(content="r", tool_call_id="parent-call"),
        ]
        seen = []

        def _sanitize(messages, model):
            seen.append(messages[0].tool_calls[0]["id"])
            return list(messages)

        with patch(
            "agent.subagents.fork.sanitize_history_for_provider_boundary",
            side_effect=_sanitize,
        ):
            seed = seed_fork_history(
                history, child_model="claude-sonnet-4-5", parent_model="gpt-4o"
            )
        assert seen == ["parent-call"]
        ai = next(m for m in seed if isinstance(m, AIMessage))
        assert ai.tool_calls[0]["id"] != "parent-call"
