"""Image post-processing in src/persistent_graph.py tool-execution loop.

Verifies that when a tool returns a string containing an `<image_data>`
or `<page_image>` tag, the persistent loop:

1. Constructs the ToolMessage with the cleaned content (no base64).
2. Appends a synthesized HumanMessage carrying the image as a real
   provider content block.
3. Passes the cleaned content (not the raw base64 wall) to the
   ``on_tool_result`` callback so the cockpit displays a small payload.
4. Leaves non-image tool results untouched (fast path).
"""

from __future__ import annotations

import asyncio
import base64
from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    ToolMessage,
)

from agent.persistent_graph import (
    PersistentLoopCallbacks,
    run_persistent_loop,
)
from agent.services.image_content import IMAGE_DATA_TAG_TEMPLATE
from shared.orch_surface.jobs import JobToolResult, ToolImageAttachment
from agent.tools.orchestrator.jobs import _agent_tool_result


# =============================================================================
# Test fixtures and helpers
# =============================================================================


def _b64(text: str) -> str:
    return base64.b64encode(text.encode()).decode()


def _make_callbacks(**overrides) -> PersistentLoopCallbacks:
    defaults = dict(
        get_user_input=AsyncMock(return_value="hello"),
        on_token=AsyncMock(),
        on_thinking=AsyncMock(),
        on_tool_start=AsyncMock(),
        on_tool_result=AsyncMock(),
        permission_check=AsyncMock(return_value=True),
        on_turn_start=AsyncMock(),
        on_turn_complete=AsyncMock(),
        on_error=AsyncMock(),
        check_interrupt=MagicMock(return_value=False),
        on_vm_upgrade_needed=None,
    )
    defaults.update(overrides)
    return PersistentLoopCallbacks(**defaults)


def _make_config(**overrides):
    cfg = MagicMock()
    cfg.llm.timeout = overrides.get("llm_timeout", 600)
    cfg.memory.enabled = overrides.get("memory_enabled", False)
    cfg.memory.observer_interval = overrides.get("observer_interval", 5)
    cfg.context_management.max_summary_length = 10000
    return cfg


def _make_tool(name: str, result: str):
    """Build a mock tool whose ainvoke returns ``result``."""
    tool = MagicMock()
    tool.name = name
    tool.ainvoke = AsyncMock(return_value=result)
    return tool


async def _run_one_turn_with_tool(
    *,
    tool_result: str,
    messages: list[BaseMessage],
    on_tool_result: AsyncMock,
):
    """Drive run_persistent_loop through exactly one tool-using turn.

    LLM behavior:
      - Call 1: emit AIMessage with one tool_call to ``read_file``.
      - Call 2: emit a final AIMessage with no tool_calls (turn ends).
    Then ``get_user_input`` raises CancelledError so the outer loop exits.
    """
    response_with_tool = AIMessage(
        content="",
        tool_calls=[
            {
                "name": "read_file",
                "args": {"file_path": "uploads/x.jpg"},
                "id": "tc_img",
            }
        ],
    )
    final_response = AIMessage(content="Looks like a photo.")

    call_count = 0

    async def _astream(_messages, **kw):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            yield response_with_tool
        else:
            yield final_response

    llm = AsyncMock()
    llm.reasoning = None
    llm.astream = _astream

    tool = _make_tool("read_file", tool_result)

    turns = 0

    async def _input():
        nonlocal turns
        turns += 1
        if turns == 1:
            return "what is in this image?"
        raise asyncio.CancelledError

    callbacks = _make_callbacks(
        get_user_input=_input,
        on_tool_result=on_tool_result,
    )

    await run_persistent_loop(
        llm_with_tools=llm,
        tools=[tool],
        context_manager=AsyncMock(
            ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
        ),
        config=_make_config(),
        system_prompt="sys",
        callbacks=callbacks,
        messages=messages,
    )


# =============================================================================
# Tests
# =============================================================================


class TestPersistentImagePostProcessor:
    @pytest.mark.asyncio
    async def test_officer_evidence_bridge_reaches_provider_as_image_block(self):
        b64 = _b64("verified-officer-screenshot")
        bridged = _agent_tool_result(
            JobToolResult(
                text="Evidence ev-shot for job j1",
                image=ToolImageAttachment(
                    base64_data=b64,
                    media_type="image/png",
                ),
            )
        )
        on_tool_result = AsyncMock()
        messages: list[BaseMessage] = []
        await _run_one_turn_with_tool(
            tool_result=bridged,
            messages=messages,
            on_tool_result=on_tool_result,
        )
        tool_message = next(m for m in messages if isinstance(m, ToolMessage))
        image_message = next(
            m
            for m in messages
            if isinstance(m, HumanMessage) and isinstance(m.content, list)
        )
        assert b64 not in str(tool_message.content)
        assert b64 not in on_tool_result.await_args.args[1]
        assert image_message.content[1]["image_url"]["url"].endswith(b64)

    @pytest.mark.asyncio
    async def test_image_data_tag_stripped_and_humanmessage_appended(self):
        b64 = _b64("fake-jpeg-bytes")
        raw_result = (
            "[IMAGE: x.jpg]\nType: image/jpeg\nSize: 16 bytes\n\n"
            + IMAGE_DATA_TAG_TEMPLATE.format(mime="image/jpeg", b64=b64)
        )
        on_tool_result = AsyncMock()
        messages: list[BaseMessage] = []

        await _run_one_turn_with_tool(
            tool_result=raw_result,
            messages=messages,
            on_tool_result=on_tool_result,
        )

        tool_msgs = [m for m in messages if isinstance(m, ToolMessage)]
        human_msgs = [
            m
            for m in messages
            if isinstance(m, HumanMessage) and isinstance(m.content, list)
        ]

        # Cleaned ToolMessage: marker present, no base64
        assert len(tool_msgs) == 1
        assert b64 not in tool_msgs[0].content
        assert "image attached: image/jpeg" in tool_msgs[0].content

        # Synthesized multimodal HumanMessage with the image as a content block
        assert len(human_msgs) == 1
        content = human_msgs[0].content
        assert content[0]["type"] == "text"
        assert "tc_img" in content[0]["text"]
        assert content[1] == {
            "type": "image_url",
            "image_url": {"url": f"data:image/jpeg;base64,{b64}"},
        }

        # Order: ToolMessage comes before the synthesized HumanMessage
        tool_idx = next(i for i, m in enumerate(messages) if isinstance(m, ToolMessage))
        human_idx = next(
            i
            for i, m in enumerate(messages)
            if isinstance(m, HumanMessage) and isinstance(m.content, list)
        )
        assert tool_idx < human_idx

        # on_tool_result callback received the CLEANED content (small)
        on_tool_result.assert_called()
        first_call_args = on_tool_result.call_args_list[0]
        # signature: on_tool_result(tool_name, content, tool_call_id, is_error=...)
        passed_content = first_call_args.args[1]
        assert b64 not in passed_content
        assert "image attached: image/jpeg" in passed_content

    @pytest.mark.asyncio
    async def test_no_image_tag_no_extra_humanmessage_and_unchanged_callback(self):
        plain_result = "Just a normal text result with no image tags."
        on_tool_result = AsyncMock()
        messages: list[BaseMessage] = []

        await _run_one_turn_with_tool(
            tool_result=plain_result,
            messages=messages,
            on_tool_result=on_tool_result,
        )

        # Exactly one ToolMessage, content unchanged
        tool_msgs = [m for m in messages if isinstance(m, ToolMessage)]
        assert len(tool_msgs) == 1
        assert tool_msgs[0].content == plain_result

        # No synthesized multimodal HumanMessage was appended.
        # (The user's input HumanMessage is a string, not a list — filter on list-content.)
        human_list_msgs = [
            m
            for m in messages
            if isinstance(m, HumanMessage) and isinstance(m.content, list)
        ]
        assert human_list_msgs == []

        # Callback got the original content (== plain_result; nothing to clean)
        on_tool_result.assert_called()
        passed_content = on_tool_result.call_args_list[0].args[1]
        assert passed_content == plain_result


class TestImagesWaitForTheWholeBatch:
    """A batch's image follow-ups come after its last tool result.

    Session 2c6d3e06 (2026-10-11) died on Anthropic's 400 "tool_use ids
    were found without tool_result blocks immediately after" because the
    image of the first ``read_file`` landed between that call's result and
    the next one's.
    """

    @pytest.mark.asyncio
    async def test_every_result_precedes_the_batch_images(self):
        b64_a, b64_b = _b64("first-png"), _b64("second-png")

        def _image(name: str, b64: str) -> str:
            return f"[IMAGE: {name}]\n" + IMAGE_DATA_TAG_TEMPLATE.format(
                mime="image/png", b64=b64
            )

        batch = AIMessage(
            content="",
            tool_calls=[
                {"name": "read_file", "args": {"path": "a.png"}, "id": "tc_a"},
                {"name": "view_image", "args": {"path": "b.png"}, "id": "tc_b"},
                {"name": "get_canvas", "args": {}, "id": "tc_c"},
            ],
        )
        provider_inputs: list[list[BaseMessage]] = []

        async def _astream(provider_messages, **kw):
            provider_inputs.append(list(provider_messages))
            if len(provider_inputs) == 1:
                yield batch
            else:
                yield AIMessage(content="Both screenshots look fine.")

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        turns = 0

        async def _input():
            nonlocal turns
            turns += 1
            if turns == 1:
                return "look at both screenshots"
            raise asyncio.CancelledError

        messages: list[BaseMessage] = []
        await run_persistent_loop(
            llm_with_tools=llm,
            tools=[
                _make_tool("read_file", _image("a.png", b64_a)),
                _make_tool("view_image", _image("b.png", b64_b)),
                _make_tool("get_canvas", ""),
            ],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=_make_config(),
            system_prompt="sys",
            callbacks=_make_callbacks(get_user_input=_input),
            messages=messages,
        )

        assert len(provider_inputs) == 2
        second = provider_inputs[1]
        call_index = next(
            i for i, m in enumerate(second) if isinstance(m, AIMessage) and m.tool_calls
        )
        after_call = second[call_index + 1 :]
        assert [m.tool_call_id for m in after_call[:3]] == ["tc_a", "tc_b", "tc_c"]
        images = [
            m
            for m in after_call[3:]
            if isinstance(m, HumanMessage) and isinstance(m.content, list)
        ]
        assert len(images) == 2
        assert images[0].content[1]["image_url"]["url"].endswith(b64_a)
        assert images[1].content[1]["image_url"]["url"].endswith(b64_b)
        # The durable transcript keeps the same order, so a restore needs no repair.
        durable_call = next(
            i
            for i, m in enumerate(messages)
            if isinstance(m, AIMessage) and m.tool_calls
        )
        assert [
            type(m).__name__ for m in messages[durable_call + 1 : durable_call + 6]
        ] == ["ToolMessage"] * 3 + ["HumanMessage"] * 2
