"""Tests for AuxiliaryLLM — unified support task system.

Validates:
- Chain mode: structured output via with_structured_output()
- Agent mode: tool loop + final structured output call
- Task base classes: ExtractMemoriesTask, CurateKnowledgeTask
- Config parsing: AuxiliaryConfig from YAML
- Message formatting helpers
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from shared.runtime.services.auxiliary import (
    AuxiliaryLLM,
    CurateKnowledgeTask,
    ExtractedMemories,
    ExtractedMemory,
    ExtractMemoriesTask,
    CurationResult,
    KnowledgeVerdict,
    KnowledgeVerdictTask,
    _format_messages_for_extraction,
    _get_message_role,
)
from shared.runtime.core.loader import (
    _parse_auxiliary_config,
)


# =============================================================================
# Helpers
# =============================================================================


def _make_structured_llm(return_value):
    """Create a mock LLM that supports with_structured_output(include_raw=True).

    The mock LLM's with_structured_output() returns a new mock
    whose ainvoke() returns the include_raw dict format:
    {"raw": AIMessage, "parsed": return_value, "parsing_error": None}
    """
    mock_llm = MagicMock()
    raw_response = AIMessage(content="structured output")
    structured_mock = AsyncMock()
    structured_mock.ainvoke = AsyncMock(
        return_value={
            "raw": raw_response,
            "parsed": return_value,
            "parsing_error": None,
        }
    )
    mock_llm.with_structured_output = MagicMock(return_value=structured_mock)
    return mock_llm


def _sample_extracted_memories():
    """Return a sample ExtractedMemories result."""
    return ExtractedMemories(
        memories=[
            ExtractedMemory(
                content="The users table uses soft deletes via deleted_at.",
                summary="Users table has soft deletes",
                keywords=["users", "soft_delete", "deleted_at"],
                importance=0.8,
                type="factual",
            ),
            ExtractedMemory(
                content="API rate limiting is set to 100 req/min per user.",
                summary="Rate limit: 100 req/min",
                keywords=["api", "rate_limit"],
                importance=0.6,
                type="factual",
            ),
        ]
    )


def _sample_curation_result():
    """Return a sample CurationResult."""
    return CurationResult(
        notes_created=3,
        notes_updated=1,
        summary="Created 3 knowledge notes and updated 1 existing note.",
    )


# =============================================================================
# Config parsing
# =============================================================================


class TestAuxiliaryConfigParsing:
    """Test _parse_auxiliary_config from loader.py."""

    def test_default_config(self):
        config = _parse_auxiliary_config({})
        assert config.enabled is True
        assert config.model is None
        assert config.base_url is None
        assert config.temperature == 0.0
        assert config.max_iterations == 15
        assert config.timeout == 120.0
        assert "extract_memories" in config.tasks
        assert "curate_knowledge" in config.tasks
        assert config.tasks["extract_memories"].enabled is True

    def test_custom_model(self):
        config = _parse_auxiliary_config(
            {
                "model": "gpt-oss-120b",
                "base_url": "http://uni-server:8080/v1",
                "temperature": 0.1,
            }
        )
        assert config.model == "gpt-oss-120b"
        assert config.base_url == "http://uni-server:8080/v1"
        assert config.temperature == 0.1

    def test_disabled_task(self):
        config = _parse_auxiliary_config(
            {
                "tasks": {
                    "extract_memories": {"enabled": False},
                    "curate_knowledge": {"enabled": True},
                }
            }
        )
        assert config.tasks["extract_memories"].enabled is False
        assert config.tasks["curate_knowledge"].enabled is True

    def test_custom_iterations_and_timeout(self):
        config = _parse_auxiliary_config(
            {
                "max_iterations": 10,
                "timeout": 60.0,
            }
        )
        assert config.max_iterations == 10
        assert config.timeout == 60.0

    def test_full_config_from_yaml(self):
        """Test that worker_base.yaml's auxiliary section parses correctly."""
        from shared.runtime.core.loader import load_agent_config

        config = load_agent_config("config/worker_base.yaml")
        assert config.auxiliary.enabled is True
        assert config.auxiliary.model == "RedHatAI/gemma-4-31B-it-FP8-Dynamic"
        assert config.auxiliary.tasks["extract_memories"].enabled is True
        assert config.auxiliary.tasks["curate_knowledge"].enabled is True

    def test_curator_verdict_defaults_off(self):
        # The KB ingestion verdict gate is a measured opt-in (mirrors
        # memory.ingestion.enabled) — off by default with the memory knob values.
        config = _parse_auxiliary_config({})
        ck = config.tasks["curate_knowledge"]
        assert ck.verdict is False
        assert ck.verdict_top_k == 5
        assert ck.review_floor == 0.6

    def test_curator_verdict_knobs_parse(self):
        config = _parse_auxiliary_config(
            {
                "tasks": {
                    "curate_knowledge": {
                        "enabled": True,
                        "verdict": True,
                        "verdict_top_k": 8,
                        "review_floor": 0.72,
                    }
                }
            }
        )
        ck = config.tasks["curate_knowledge"]
        assert ck.enabled is True
        assert ck.verdict is True
        assert ck.verdict_top_k == 8
        assert ck.review_floor == 0.72


# =============================================================================
# ExtractMemoriesTask
# =============================================================================


_TEST_MEMORY_PROMPT = "You are a memory extraction system. Extract noteworthy info."


class TestExtractMemoriesTask:
    """Test the ExtractMemoriesTask chain-mode task."""

    def test_system_prompt_uses_provided_prompt(self):
        task = ExtractMemoriesTask(messages=[], prompt=_TEST_MEMORY_PROMPT, phase=0)
        assert task.system_prompt == _TEST_MEMORY_PROMPT

    def test_output_schema(self):
        task = ExtractMemoriesTask(messages=[], prompt=_TEST_MEMORY_PROMPT, phase=0)
        assert task.output_schema is ExtractedMemories

    def test_build_context_formats_messages(self):
        messages = [
            HumanMessage(content="What's the database schema?"),
            AIMessage(content="The users table has columns: id, name, email."),
        ]
        task = ExtractMemoriesTask(
            messages=messages, prompt=_TEST_MEMORY_PROMPT, phase=1
        )
        context = task.build_context()
        assert "database schema" in context
        assert "users table" in context

    def test_build_context_empty_messages(self):
        task = ExtractMemoriesTask(messages=[], prompt=_TEST_MEMORY_PROMPT, phase=0)
        context = task.build_context()
        assert context == ""

    @patch(
        "shared.runtime.core.context_entries.is_context_injection",
        return_value=True,
    )
    def test_build_context_skips_injections(self, mock_is_injection):
        messages = [
            HumanMessage(content="This is an injection"),
            HumanMessage(content="This is real"),
        ]
        task = ExtractMemoriesTask(
            messages=messages, prompt=_TEST_MEMORY_PROMPT, phase=0
        )
        # All messages will be skipped because mock returns True for all
        context = task.build_context()
        assert context == ""


# =============================================================================
# CurateKnowledgeTask
# =============================================================================


_TEST_CURATION_PROMPT = "You are the knowledge curator for a project."


class TestCurateKnowledgeTask:
    """Test the CurateKnowledgeTask agent-mode task."""

    def test_system_prompt_uses_provided_prompt(self):
        task = CurateKnowledgeTask(
            phase_data="Phase 1 done.",
            workspace_md="# Workspace",
            plan_md="# Plan",
            existing_notes=[],
            kb_tools=[],
            prompt=_TEST_CURATION_PROMPT,
        )
        assert task.system_prompt == _TEST_CURATION_PROMPT

    def test_output_schema(self):
        task = CurateKnowledgeTask(
            phase_data="",
            workspace_md="",
            plan_md="",
            existing_notes=[],
            kb_tools=[],
            prompt=_TEST_CURATION_PROMPT,
        )
        assert task.output_schema is CurationResult

    def test_build_context_includes_artifacts(self):
        task = CurateKnowledgeTask(
            phase_data="Completed API design phase.",
            workspace_md="# Workspace\nDecided on REST.",
            plan_md="# Plan\nPhase 1: API design",
            existing_notes=["- note-1: JWT Auth (decision)"],
            kb_tools=[],
            prompt=_TEST_CURATION_PROMPT,
        )
        context = task.build_context()
        assert "API design" in context
        assert "Decided on REST" in context
        assert "JWT Auth" in context
        assert "Existing Knowledge" in context

    def test_build_context_no_existing_notes(self):
        task = CurateKnowledgeTask(
            phase_data="Done.",
            workspace_md="# WS",
            plan_md="# Plan",
            existing_notes=[],
            kb_tools=[],
            prompt=_TEST_CURATION_PROMPT,
        )
        context = task.build_context()
        assert "Existing Knowledge" not in context

    def test_get_tools_returns_provided_tools(self):
        mock_tools = [MagicMock(), MagicMock()]
        task = CurateKnowledgeTask(
            phase_data="",
            workspace_md="",
            plan_md="",
            existing_notes=[],
            kb_tools=mock_tools,
            prompt=_TEST_CURATION_PROMPT,
        )
        assert task.get_tools() is mock_tools


# =============================================================================
# KnowledgeVerdict + KnowledgeVerdictTask (OKF KB slice 2 PR2)
# =============================================================================


_TEST_VERDICT_PROMPT = (
    "You adjudicate knowledge notes. Return ADD/UPDATE/SUPERSEDE/DISCARD."
)


class TestKnowledgeVerdict:
    """The verdict schema — the KB analog of IngestionVerdict."""

    def test_add_needs_no_targets(self):
        v = KnowledgeVerdict(action="ADD", reason="new")
        assert v.action == "ADD"
        assert v.target_indices == []

    def test_supersede_carries_targets(self):
        v = KnowledgeVerdict(action="SUPERSEDE", target_indices=[1, 2], reason="stale")
        assert v.action == "SUPERSEDE"
        assert v.target_indices == [1, 2]


class TestKnowledgeVerdictTask:
    """Chain-mode task that formats the candidate + neighbours for the adjudicator."""

    def test_system_prompt_is_event_time_prompt(self):
        task = KnowledgeVerdictTask(
            candidate_content="c", neighbours=[], prompt=_TEST_VERDICT_PROMPT
        )
        assert task.system_prompt == _TEST_VERDICT_PROMPT

    def test_output_schema(self):
        task = KnowledgeVerdictTask(
            candidate_content="c", neighbours=[], prompt=_TEST_VERDICT_PROMPT
        )
        assert task.output_schema is KnowledgeVerdict

    def test_build_context_numbers_neighbours_with_similarity_and_title(self):
        task = KnowledgeVerdictTask(
            candidate_content="We now use RS256, not HS256.",
            neighbours=[
                {"content": "Auth uses HS256.", "similarity": 0.88, "title": "Auth"},
            ],
            prompt=_TEST_VERDICT_PROMPT,
        )
        ctx = task.build_context()
        assert "We now use RS256" in ctx
        assert "Auth uses HS256." in ctx
        assert "similarity 0.88" in ctx
        assert "Auth" in ctx
        assert "[1]" in ctx
        # instruction lists the four KB verbs
        assert "ADD" in ctx and "SUPERSEDE" in ctx and "DISCARD" in ctx

    def test_build_context_omits_similarity_when_absent(self):
        task = KnowledgeVerdictTask(
            candidate_content="c",
            neighbours=[{"content": "n1"}],
            prompt=_TEST_VERDICT_PROMPT,
        )
        ctx = task.build_context()
        assert "similarity" not in ctx
        assert "[1]" in ctx and "n1" in ctx


# =============================================================================
# AuxiliaryLLM.chain()
# =============================================================================


class TestAuxiliaryLLMChain:
    """Test AuxiliaryLLM chain mode execution."""

    @pytest.mark.asyncio
    async def test_chain_returns_structured_output(self):
        expected = _sample_extracted_memories()
        mock_llm = _make_structured_llm(expected)

        aux = AuxiliaryLLM(llm=mock_llm, max_iterations=15, timeout=30.0)
        task = ExtractMemoriesTask(
            messages=[HumanMessage(content="test")],
            prompt=_TEST_MEMORY_PROMPT,
            phase=0,
        )

        result = await aux.chain(task)

        assert result is expected
        assert len(result.memories) == 2
        assert (
            result.memories[0].content
            == "The users table uses soft deletes via deleted_at."
        )
        mock_llm.with_structured_output.assert_called_once_with(
            ExtractedMemories,
            method="json_schema",
            include_raw=True,
        )

    @pytest.mark.asyncio
    async def test_chain_passes_correct_messages(self):
        expected = ExtractedMemories(memories=[])
        mock_llm = _make_structured_llm(expected)

        aux = AuxiliaryLLM(llm=mock_llm, timeout=30.0)
        task = ExtractMemoriesTask(
            messages=[HumanMessage(content="Hello world")],
            prompt=_TEST_MEMORY_PROMPT,
            phase=0,
        )

        await aux.chain(task)

        # Check the messages passed to ainvoke
        structured_mock = mock_llm.with_structured_output.return_value
        call_args = structured_mock.ainvoke.call_args[0][0]
        assert len(call_args) == 2
        assert call_args[0].content == task.system_prompt  # SystemMessage
        assert "Hello world" in call_args[1].content  # HumanMessage with context

    @pytest.mark.asyncio
    async def test_chain_timeout(self):
        mock_llm = MagicMock()
        structured_mock = AsyncMock()

        async def slow_invoke(*args, **kwargs):
            await asyncio.sleep(10)

        structured_mock.ainvoke = slow_invoke
        mock_llm.with_structured_output = MagicMock(return_value=structured_mock)

        aux = AuxiliaryLLM(llm=mock_llm, timeout=0.1)
        task = ExtractMemoriesTask(messages=[], prompt=_TEST_MEMORY_PROMPT, phase=0)

        with pytest.raises(asyncio.TimeoutError):
            await aux.chain(task)


# =============================================================================
# AuxiliaryLLM.agent()
# =============================================================================


class TestAuxiliaryLLMAgent:
    """Test AuxiliaryLLM agent mode execution."""

    @pytest.mark.asyncio
    async def test_agent_no_tool_calls(self):
        """When the LLM makes no tool calls, agent goes straight to final output."""
        expected = _sample_curation_result()
        mock_llm = MagicMock()

        # Tool loop LLM returns a response with no tool calls
        no_tools_response = MagicMock()
        no_tools_response.tool_calls = []
        no_tools_response.content = "Done."

        tool_loop_mock = AsyncMock()
        tool_loop_mock.ainvoke = AsyncMock(return_value=no_tools_response)
        mock_llm.bind_tools = MagicMock(return_value=tool_loop_mock)

        # Final structured output call (include_raw=True format)
        raw_response = AIMessage(content="structured output")
        structured_mock = AsyncMock()
        structured_mock.ainvoke = AsyncMock(
            return_value={
                "raw": raw_response,
                "parsed": expected,
                "parsing_error": None,
            }
        )
        mock_llm.with_structured_output = MagicMock(return_value=structured_mock)

        aux = AuxiliaryLLM(llm=mock_llm, max_iterations=5, timeout=30.0)
        task = CurateKnowledgeTask(
            phase_data="Phase 1 done.",
            workspace_md="# WS",
            plan_md="# Plan",
            existing_notes=[],
            kb_tools=[],
            prompt=_TEST_CURATION_PROMPT,
        )

        result = await aux.agent(task)

        assert result is expected
        assert result.notes_created == 3

    @pytest.mark.asyncio
    async def test_agent_with_tool_calls(self):
        """Agent should execute tool calls then produce structured output."""
        expected = _sample_curation_result()
        mock_llm = MagicMock()

        # First response: one tool call
        tool_call_response = MagicMock()
        tool_call_response.tool_calls = [
            {"name": "kb_search", "args": {"query": "API design"}, "id": "call-1"}
        ]
        tool_call_response.content = ""

        # Second response: no more tool calls
        done_response = MagicMock()
        done_response.tool_calls = []
        done_response.content = "Done curating."

        tool_loop_mock = AsyncMock()
        tool_loop_mock.ainvoke = AsyncMock(
            side_effect=[tool_call_response, done_response]
        )
        mock_llm.bind_tools = MagicMock(return_value=tool_loop_mock)

        # Final structured output (include_raw=True format)
        raw_response = AIMessage(content="structured output")
        structured_mock = AsyncMock()
        structured_mock.ainvoke = AsyncMock(
            return_value={
                "raw": raw_response,
                "parsed": expected,
                "parsing_error": None,
            }
        )
        mock_llm.with_structured_output = MagicMock(return_value=structured_mock)

        # Create a mock tool
        mock_kb_search = AsyncMock()
        mock_kb_search.name = "kb_search"
        mock_kb_search.ainvoke = AsyncMock(
            return_value="Found 2 notes about API design."
        )

        task = CurateKnowledgeTask(
            phase_data="Phase 1 done.",
            workspace_md="# WS",
            plan_md="# Plan",
            existing_notes=[],
            kb_tools=[mock_kb_search],
            prompt=_TEST_CURATION_PROMPT,
        )

        aux = AuxiliaryLLM(llm=mock_llm, max_iterations=5, timeout=30.0)
        result = await aux.agent(task)

        assert result is expected
        mock_kb_search.ainvoke.assert_called_once_with({"query": "API design"})

    @pytest.mark.asyncio
    async def test_agent_respects_max_iterations(self):
        """Agent should stop after max_iterations even if LLM keeps calling tools."""
        expected = _sample_curation_result()
        mock_llm = MagicMock()

        # Always returns tool calls (infinite loop scenario)
        infinite_response = MagicMock()
        infinite_response.tool_calls = [
            {"name": "kb_search", "args": {"query": "test"}, "id": "call-n"}
        ]
        infinite_response.content = ""

        tool_loop_mock = AsyncMock()
        tool_loop_mock.ainvoke = AsyncMock(return_value=infinite_response)
        mock_llm.bind_tools = MagicMock(return_value=tool_loop_mock)

        # Final structured output (include_raw=True format)
        raw_response = AIMessage(content="structured output")
        structured_mock = AsyncMock()
        structured_mock.ainvoke = AsyncMock(
            return_value={
                "raw": raw_response,
                "parsed": expected,
                "parsing_error": None,
            }
        )
        mock_llm.with_structured_output = MagicMock(return_value=structured_mock)

        mock_tool = AsyncMock()
        mock_tool.name = "kb_search"
        mock_tool.ainvoke = AsyncMock(return_value="result")

        task = CurateKnowledgeTask(
            phase_data="",
            workspace_md="",
            plan_md="",
            existing_notes=[],
            kb_tools=[mock_tool],
            prompt=_TEST_CURATION_PROMPT,
        )

        aux = AuxiliaryLLM(llm=mock_llm, max_iterations=3, timeout=30.0)
        result = await aux.agent(task)

        # Should have stopped after 3 iterations
        assert tool_loop_mock.ainvoke.call_count == 3
        assert result is expected

    @pytest.mark.asyncio
    async def test_agent_handles_tool_errors(self):
        """Agent should handle tool execution errors gracefully."""
        expected = _sample_curation_result()
        mock_llm = MagicMock()

        # First response: tool call that will fail
        tool_call_response = MagicMock()
        tool_call_response.tool_calls = [
            {"name": "kb_write", "args": {"title": "test"}, "id": "call-1"}
        ]
        tool_call_response.content = ""

        # Second response: done
        done_response = MagicMock()
        done_response.tool_calls = []
        done_response.content = "Done."

        tool_loop_mock = AsyncMock()
        tool_loop_mock.ainvoke = AsyncMock(
            side_effect=[tool_call_response, done_response]
        )
        mock_llm.bind_tools = MagicMock(return_value=tool_loop_mock)

        # Final structured output (include_raw=True format)
        raw_response = AIMessage(content="structured output")
        structured_mock = AsyncMock()
        structured_mock.ainvoke = AsyncMock(
            return_value={
                "raw": raw_response,
                "parsed": expected,
                "parsing_error": None,
            }
        )
        mock_llm.with_structured_output = MagicMock(return_value=structured_mock)

        # Tool that raises an error
        mock_tool = AsyncMock()
        mock_tool.name = "kb_write"
        mock_tool.ainvoke = AsyncMock(
            side_effect=RuntimeError("Neo4j connection failed")
        )

        task = CurateKnowledgeTask(
            phase_data="",
            workspace_md="",
            plan_md="",
            existing_notes=[],
            kb_tools=[mock_tool],
            prompt=_TEST_CURATION_PROMPT,
        )

        aux = AuxiliaryLLM(llm=mock_llm, max_iterations=5, timeout=30.0)
        result = await aux.agent(task)

        # Should complete despite tool error
        assert result is expected


# =============================================================================
# Message formatting helpers
# =============================================================================


class TestMessageFormatting:
    """Test _format_messages_for_extraction and _get_message_role."""

    def test_get_message_role_human(self):
        assert _get_message_role(HumanMessage(content="hi")) == "User"

    def test_get_message_role_ai(self):
        assert _get_message_role(AIMessage(content="hello")) == "Agent"

    def test_get_message_role_ai_with_tools(self):
        msg = AIMessage(
            content="", tool_calls=[{"name": "read_file", "args": {}, "id": "1"}]
        )
        role = _get_message_role(msg)
        assert "Agent" in role
        assert "read_file" in role

    def test_get_message_role_tool(self):
        msg = ToolMessage(content="result", tool_call_id="1")
        assert _get_message_role(msg) == "Tool Result"

    def test_format_basic_conversation(self):
        messages = [
            HumanMessage(content="What tables exist?"),
            AIMessage(
                content="There are 5 tables: users, posts, comments, tags, settings."
            ),
        ]
        result = _format_messages_for_extraction(messages)
        assert "[User]" in result
        assert "[Agent]" in result
        assert "5 tables" in result

    def test_format_truncates_long_tool_results(self):
        messages = [
            ToolMessage(content="x" * 2000, tool_call_id="1"),
        ]
        result = _format_messages_for_extraction(messages)
        assert "... [truncated]" in result
        assert len(result) < 1500

    def test_format_skips_empty_messages(self):
        messages = [
            HumanMessage(content=""),
            AIMessage(content="   "),
            HumanMessage(content="Real content"),
        ]
        result = _format_messages_for_extraction(messages)
        assert "Real content" in result
        # Only one message should be in the output
        assert result.count("[") == 1

    def test_format_skips_typed_context_entries(self):
        from shared.runtime.core.context_entries import make_context_entry

        messages = [
            HumanMessage(content="We chose Postgres."),
            make_context_entry("memory", "ENTRY_MEMORY", section="memory"),
            make_context_entry("guidance", "ENTRY_GUIDANCE", section="guidance"),
        ]
        result = _format_messages_for_extraction(messages)
        assert result == "[User] We chose Postgres."


class TestObservationWindowFiltersBeforeCap:
    """Injected context is filtered BEFORE the 40-message window cap.

    Capping first let injections use up the window: 5 real messages followed
    by 40 injections left the extractor nothing to read (WP0 survey B4).
    """

    @staticmethod
    def _history():
        from agent.core.memory_injection import create_memory_injection_messages
        from shared.runtime.core.context_entries import make_context_entry

        real = [
            HumanMessage(content=f"real {i}")
            if i % 2 == 0
            else AIMessage(content=f"real {i}")
            for i in range(5)
        ]
        injected = []
        for i in range(10):
            injected.extend(create_memory_injection_messages(f"--- legacy {i} ---"))
            injected.append(
                make_context_entry("memory", f"entry {i}", section="memory")
            )
            injected.append(
                make_context_entry("knowledge", f"note {i}", section="knowledge")
            )
        assert len(injected) == 40
        return real, real + injected

    @pytest.mark.asyncio
    async def test_extraction_window_keeps_the_real_messages(self):
        from shared.runtime.services.auxiliary import extract_and_store_memories

        real, history = self._history()
        aux = MagicMock()
        aux.chain = AsyncMock(return_value=ExtractedMemories(memories=[]))

        await extract_and_store_memories(aux, MagicMock(), history, "prompt")

        task = aux.chain.call_args[0][0]
        assert task.messages == real

    @pytest.mark.asyncio
    async def test_assembler_window_keeps_the_real_messages(self):
        from shared.runtime.services.auxiliary import assemble_memories

        real, history = self._history()
        aux = MagicMock()
        aux.agent = AsyncMock(
            return_value=MagicMock(actions_taken=[], gaps_identified=[], summary="")
        )

        with patch(
            "shared.runtime.services.assembler_tools.create_assembler_tools",
            return_value=[],
        ):
            await assemble_memories(aux, MagicMock(), history, "", "prompt")

        task = aux.agent.call_args[0][0]
        assert task.recent_context == _format_messages_for_extraction(real)
        assert "real 0" in task.recent_context

    @pytest.mark.asyncio
    async def test_all_injections_means_no_extraction(self):
        from shared.runtime.services.auxiliary import extract_and_store_memories

        _, history = self._history()
        aux = MagicMock()
        aux.chain = AsyncMock()

        stored = await extract_and_store_memories(
            aux, MagicMock(), history[5:], "prompt"
        )

        assert stored == 0
        aux.chain.assert_not_called()


# =============================================================================
# Output schema validation
# =============================================================================


class TestOutputSchemas:
    """Test Pydantic output schema validation."""

    def test_extracted_memory_valid(self):
        mem = ExtractedMemory(
            content="Test content",
            summary="Test summary",
            keywords=["test"],
            importance=0.5,
            type="factual",
        )
        assert mem.content == "Test content"

    def test_extracted_memory_clamps_importance(self):
        with pytest.raises(Exception):
            ExtractedMemory(
                content="test",
                summary="test",
                keywords=[],
                importance=1.5,  # Over max
                type="factual",
            )

    def test_extracted_memories_empty(self):
        result = ExtractedMemories(memories=[])
        assert len(result.memories) == 0

    def test_curation_result(self):
        result = CurationResult(
            notes_created=3,
            notes_updated=1,
            summary="Created 3 notes.",
        )
        assert result.notes_created == 3


# =============================================================================
# Failed-aux archiving — surface silent aux failures
# (knowledge-base/knowledge/issues/surface_silent_aux_failures.md)
# =============================================================================


class TestAuxErrorArchiving:
    @pytest.mark.asyncio
    async def test_chain_failure_archives_error_and_reraises(self):
        mock_llm = MagicMock()
        structured_mock = AsyncMock()
        structured_mock.ainvoke = AsyncMock(
            side_effect=ConnectionError("503 backend down")
        )
        mock_llm.with_structured_output = MagicMock(return_value=structured_mock)

        archiver = MagicMock()
        aux = AuxiliaryLLM(llm=mock_llm, timeout=5.0)
        aux.set_job_context(archiver=archiver, job_id="job-123", agent_type="scholar")
        task = ExtractMemoriesTask(messages=[], prompt=_TEST_MEMORY_PROMPT, phase=0)

        with pytest.raises(ConnectionError):
            await aux.chain(task)

        archiver.archive_error.assert_called_once()
        kwargs = archiver.archive_error.call_args.kwargs
        assert kwargs["job_id"] == "job-123"
        assert kwargs["call_type"] == "memory_extraction"
        assert kwargs["error_type"] == "ConnectionError"
        assert "503" in kwargs["error"]
        # A failed call must not also archive a success row.
        archiver.archive.assert_not_called()

    @pytest.mark.asyncio
    async def test_archive_error_noop_without_job_context(self):
        # Persistent-session path: no archiver wired -> must not crash, still raises.
        mock_llm = MagicMock()
        structured_mock = AsyncMock()
        structured_mock.ainvoke = AsyncMock(side_effect=RuntimeError("boom"))
        mock_llm.with_structured_output = MagicMock(return_value=structured_mock)

        aux = AuxiliaryLLM(llm=mock_llm, timeout=5.0)  # no set_job_context
        task = ExtractMemoriesTask(messages=[], prompt=_TEST_MEMORY_PROMPT, phase=0)

        with pytest.raises(RuntimeError):
            await aux.chain(task)

    @pytest.mark.asyncio
    async def test_successful_chain_archives_success_not_error(self):
        mock_llm = _make_structured_llm(ExtractedMemories(memories=[]))
        archiver = MagicMock()
        aux = AuxiliaryLLM(llm=mock_llm)
        aux.set_job_context(archiver=archiver, job_id="job-1", agent_type="scholar")
        task = ExtractMemoriesTask(
            messages=[HumanMessage(content="hi")],
            prompt=_TEST_MEMORY_PROMPT,
            phase=0,
        )

        await aux.chain(task)

        archiver.archive.assert_called_once()
        archiver.archive_error.assert_not_called()


class TestArchiveError:
    class _FakeWriter:
        """Captures the row the Postgres archiver builds (mirrors test_archiver_pg)."""

        def __init__(self, ready=True, request_id="deadbeef"):
            self.ready = ready
            self.request_id = request_id
            self.rows = []

        def ensure_ready(self):
            return self.ready

        def insert_llm_request(self, row):
            self.rows.append(row)
            return self.request_id

    def _archiver(self, connected=True, inserted_id="deadbeef"):
        from agent.core.archiver import LLMArchiver

        return LLMArchiver(
            writer=self._FakeWriter(ready=connected, request_id=inserted_id)
        )

    def test_writes_error_document(self):
        archiver = self._archiver()
        request_id = archiver.archive_error(
            job_id="job-1",
            agent_type="scholar",
            messages=[HumanMessage(content="hello")],
            model="gemma-4-moe",
            error="503 All backends unavailable",
            error_type="ConnectionError",
            latency_ms=12,
            call_type="memory_extraction",
            auxiliary_metadata={"task_class": "ExtractMemoriesTask"},
        )
        assert request_id == "deadbeef"
        row = archiver._writer.rows[0]
        # The Postgres schema folds the error into metadata and stores an empty
        # response (response is NOT NULL) so a failed call stays queryable.
        assert row["metadata"]["status"] == "error"
        assert row["call_type"] == "memory_extraction"
        assert row["metadata"]["error"]["type"] == "ConnectionError"
        assert "503" in row["metadata"]["error"]["message"]
        assert row["response"] == {}
        assert row["model"] == "gemma-4-moe"
        assert row["request"]["message_count"] == 1

    def test_noop_when_disconnected(self):
        archiver = self._archiver(connected=False)
        out = archiver.archive_error(
            job_id="j",
            agent_type="a",
            messages=[],
            model="m",
            error="e",
            error_type="E",
        )
        assert out is None
        assert archiver._writer.rows == []
