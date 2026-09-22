"""Unit tests for stuck agent detection and recovery.

Tests the refactored create_audited_tool_node function in src/graph.py,
covering: fingerprint-based loop warnings (soft, never blocking),
progress tracking, category failure logging, stuck detection with
reflection/freeze, hard caps, and tool-not-found enrichment.

See knowledge-base/knowledge/features/stuck_agent_recovery.md for the full design.
"""

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Optional
from unittest.mock import MagicMock, AsyncMock, call, patch

import pytest
from langchain_core.messages import AIMessage, ToolMessage, SystemMessage

from agent.graph import create_audited_tool_node  # noqa: E402
from shared.runtime.core.loader import InstructionFileEntry, LimitsConfig  # noqa: E402
from shared.runtime.core.workspace_backend import WorkspaceUnavailableError  # noqa: E402
from agent.tools.context import ToolContext  # noqa: E402


# =============================================================================
# Test fixtures and helpers
# =============================================================================


@dataclass
class FakeToolsConfig:
    workspace: list = field(default_factory=list)
    core: list = field(default_factory=list)
    document: list = field(default_factory=list)
    research: list = field(default_factory=list)
    citation: list = field(default_factory=list)
    graph: list = field(default_factory=list)
    sql: list = field(default_factory=list)
    mongodb: list = field(default_factory=list)
    git: list = field(default_factory=list)
    shell: list = field(default_factory=list)
    evaluation: list = field(default_factory=list)
    knowledge: list = field(default_factory=list)
    cloud: list = field(default_factory=list)
    communication: list = field(default_factory=list)


@dataclass
class FakeLLMConfig:
    """Minimal LLM-config slice for guardrails resolution."""

    model: Optional[str] = None


@dataclass
class FakeConfig:
    """Minimal config matching what create_audited_tool_node reads."""

    agent_id: str = "test_agent"
    limits: LimitsConfig = field(default_factory=LimitsConfig)
    tools: FakeToolsConfig = field(default_factory=FakeToolsConfig)
    llm: FakeLLMConfig = field(default_factory=FakeLLMConfig)


def make_tool_call(name: str, args: dict = None, call_id: str = None):
    """Create a tool call dict matching LangChain format."""
    return {
        "name": name,
        "id": call_id or f"call_{name}",
        "args": args or {},
    }


def make_state(
    tool_calls: list,
    phase_number: int = 1,
    is_strategic: bool = False,
    job_id: str = "test-job",
):
    """Create a minimal state dict with an AIMessage containing tool_calls."""
    ai_msg = AIMessage(content="", tool_calls=tool_calls)
    return {
        "messages": [ai_msg],
        "job_id": job_id,
        "iteration": 1,
        "is_strategic_phase": is_strategic,
        "phase_number": phase_number,
        "metadata": {},
    }


def make_tool_result(name: str, content: str, call_id: str = None):
    """Create a ToolMessage result."""
    return ToolMessage(
        content=content,
        tool_call_id=call_id or f"call_{name}",
        name=name,
    )


@pytest.fixture
def config():
    """Default test config."""
    return FakeConfig()


@pytest.fixture
def low_threshold_config():
    """Config with low thresholds for easier testing."""
    return FakeConfig(
        limits=LimitsConfig(
            progress_stall_threshold=3,
            max_tool_calls_per_phase=10,
        )
    )


@pytest.fixture
def mock_tool_node():
    """Create a mock ToolNode that returns configurable results."""
    mock = AsyncMock()
    return mock


@pytest.mark.asyncio
async def test_stateless_tool_batch_checkpoints_instruction_read_receipt(config):
    entry = InstructionFileEntry(
        trigger="before_tool:todo_complete",
        skill="verify-before-done",
        phases=["tactical"],
        read_scope="phase",
        max_read_age_turns=20,
    )
    context = ToolContext()
    context._stateless_worker = True
    context._instruction_files = [entry]
    context.set_current_phase("tactical", phase_number=1, turn_count=4)
    context.record_file_read(entry.path, "checkpoint this exact guide")
    fake_read = MagicMock()
    fake_read.name = "read_file"

    with patch("agent.graph.ToolNode") as mock_tool_node_class:
        tool_node = AsyncMock()
        tool_node.ainvoke.return_value = {
            "messages": [make_tool_result("read_file", "guide", "call-read")]
        }
        mock_tool_node_class.return_value = tool_node
        audited = create_audited_tool_node(
            [fake_read],
            config,
            tool_context=context,
        )
        result = await audited(
            make_state(
                [make_tool_call("read_file", {"path": entry.path}, "call-read")],
                phase_number=1,
                is_strategic=False,
            )
        )

    assert result["instruction_read_receipts"] == (
        context.export_instruction_read_receipts()
    )


@pytest.mark.asyncio
async def test_pending_tools_resume_restores_phase_before_instruction_gate(config):
    entry = InstructionFileEntry(
        trigger="before_tool:todo_complete",
        skill="verify-before-done",
        phases=["tactical"],
        read_scope="phase",
        max_read_age_turns=20,
    )
    context = ToolContext()
    context._instruction_files = [entry]
    # A fresh successor context has no phase, so phase-scoped instruction
    # bindings are not evaluated until the resumed graph node supplies it.
    assert context.check_tool_enforcement("todo_complete") is None
    fake_read = MagicMock()
    fake_read.name = "read_file"

    with patch("agent.graph.ToolNode") as mock_tool_node_class:
        tool_node = AsyncMock()
        tool_node.ainvoke.return_value = {
            "messages": [make_tool_result("read_file", "ok", "call-read")]
        }
        mock_tool_node_class.return_value = tool_node
        audited = create_audited_tool_node(
            [fake_read],
            config,
            tool_context=context,
        )
        state = make_state(
            [make_tool_call("read_file", {"path": "notes.md"}, "call-read")],
            phase_number=3,
            is_strategic=False,
        )
        state["turn_count"] = 12
        await audited(state)

    assert context._current_phase == "tactical"
    assert context._current_phase_number == 3
    assert context._current_turn_count == 12
    assert context.check_tool_enforcement("todo_complete") is not None


@pytest.mark.asyncio
async def test_audited_node_receipt_survives_reconstruction_and_invalidates(
    config, tmp_path
):
    """A valid skill-read receipt survives worker reconstruction via the real
    audited tool node and real persisted-checkpoint wiring — and nothing more.

    Claim 1 executes real reads through a real saver-backed audited-tool
    subgraph (no ToolNode mock, no provider call) under one thread ID. Claim 2
    reconstructs a fresh worker (new workspace manager over the same bytes,
    new ToolContext, new compiled graph on the same saver) and restores from
    the values retrieved through the successor graph's ``aget_state`` — the
    same production path as ``snapshot.values``: the gated action then
    succeeds with no further read. Changed content, a new concrete phase, and
    an expired turn window each legitimately re-arm the gate until one fresh
    read. An ordinary file read through the same node grants local
    read-before-write authority yet never enters the saved receipts and never
    crosses the lease as write authority — in either direction.
    """
    from typing import Annotated

    from langchain_core.tools import tool as langchain_tool
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.graph import START, END, StateGraph
    from langgraph.graph.message import add_messages
    from typing_extensions import TypedDict

    from agent.agent import UniversalAgent  # noqa: E402
    from agent.core.workspace import WorkspaceManager  # noqa: E402
    from agent.tools.registry import apply_instruction_enforcement  # noqa: E402
    from agent.tools.workspace.files import create_file_tools  # noqa: E402
    from tests._fs_backend import FilesystemTestBackend  # noqa: E402

    SKILL_PATH = "skills/verify-before-done/SKILL.md"
    BODY_V1 = "---\nname: verify-before-done\n---\nNODE-RESUME-BODY-v1\n"
    BODY_V2 = "---\nname: verify-before-done\n---\nNODE-RESUME-BODY-v2\n"

    class _NodeState(TypedDict, total=False):
        messages: Annotated[list, add_messages]
        job_id: str
        iteration: int
        is_strategic_phase: bool
        phase_number: int
        turn_count: int
        metadata: dict
        instruction_read_receipts: dict

    def _binding():
        return InstructionFileEntry(
            trigger="before_tool:verify_probe",
            skill="verify-before-done",
            enforce=True,
            phases=["tactical"],
            read_scope="phase",
            max_read_age_turns=20,
        )

    def _workspace():
        return WorkspaceManager(
            job_id="t", base_path=tmp_path, backend=FilesystemTestBackend(tmp_path)
        )

    def _context_and_tools(workspace):
        ctx = ToolContext(workspace_manager=workspace)
        ctx._stateless_worker = True
        ctx._instruction_files = [_binding()]
        ctx._llm_config = None
        file_tools = {t.name: t for t in create_file_tools(ctx)}

        @langchain_tool
        def verify_probe() -> str:
            """Gated probe with no phase metadata."""
            return "GATED-OK"

        apply_instruction_enforcement([verify_probe], ctx)
        return ctx, file_tools["read_file"], verify_probe

    def _compile(audited, checkpointer=None):
        workflow = StateGraph(_NodeState)
        workflow.add_node("audited_tools", audited)
        workflow.add_edge(START, "audited_tools")
        workflow.add_edge("audited_tools", END)
        return workflow.compile(checkpointer=checkpointer)

    def _state(tool_calls, phase_number, turn_count):
        return {
            "messages": [AIMessage(content="", tool_calls=tool_calls)],
            "job_id": "test-job",
            "iteration": 1,
            "is_strategic_phase": False,
            "phase_number": phase_number,
            "turn_count": turn_count,
            "metadata": {},
        }

    def _content(result, call_id):
        messages = [
            m
            for m in result.get("messages", [])
            if getattr(m, "tool_call_id", None) == call_id
        ]
        assert len(messages) == 1
        return messages[0].content or ""

    def _restore(context, values):
        agent = UniversalAgent.__new__(UniversalAgent)
        agent._tool_context = context
        return agent._restore_worker_instruction_reads(values)

    # ---- claim 1: gate blocks, then real reads persist receipts ----
    saver = InMemorySaver()
    thread_config = {"configurable": {"thread_id": "node-resume-test"}}
    ws1 = _workspace()
    ws1.backend.mkdir("skills/verify-before-done")
    ws1.backend.mkdir("notes")
    ws1.write_file(SKILL_PATH, BODY_V1)
    ws1.write_file("notes/ordinary.md", "ordinary bytes")
    ctx1, read_tool_1, probe_1 = _context_and_tools(ws1)
    app1 = _compile(
        create_audited_tool_node([read_tool_1, probe_1], config, tool_context=ctx1),
        checkpointer=saver,
    )

    blocked = await app1.ainvoke(
        _state([make_tool_call("verify_probe", {}, "c-probe-0")], 2, 5),
        config=thread_config,
    )
    assert "must read" in _content(blocked, "c-probe-0").lower()

    first = await app1.ainvoke(
        _state([make_tool_call("read_file", {"path": SKILL_PATH}, "c-read-1")], 2, 5),
        config=thread_config,
    )
    assert "NODE-RESUME-BODY-v1" in _content(first, "c-read-1")
    assert SKILL_PATH in (first.get("instruction_read_receipts") or {})

    # The ordinary file goes through the same real node before the successor
    # ever reads the checkpoint: locally recorded, granting local
    # read-before-write authority — yet never an instruction receipt.
    ordinary = await app1.ainvoke(
        _state(
            [make_tool_call("read_file", {"path": "notes/ordinary.md"}, "c-read-ord")],
            2,
            5,
        ),
        config=thread_config,
    )
    assert "ordinary bytes" in _content(ordinary, "c-read-ord")
    assert ctx1.recent_read_matches("notes/ordinary.md", "ordinary bytes")
    assert "notes/ordinary.md" not in ctx1.export_instruction_read_receipts()

    # ---- claim 2: persisted checkpoint restores enforcement, not authority ----
    ctx2, read_tool_2, probe_2 = _context_and_tools(_workspace())
    app2 = _compile(
        create_audited_tool_node([read_tool_2, probe_2], config, tool_context=ctx2),
        checkpointer=saver,
    )
    snapshot = await app2.aget_state(thread_config)
    saved_values = snapshot.values
    assert saved_values, "expected a persisted checkpoint for the thread"
    assert SKILL_PATH in (saved_values.get("instruction_read_receipts") or {})
    assert "notes/ordinary.md" not in (
        saved_values.get("instruction_read_receipts") or {}
    )
    assert _restore(ctx2, saved_values) == 1
    second = await app2.ainvoke(
        _state([make_tool_call("verify_probe", {}, "c-probe-2")], 2, 6),
        config=thread_config,
    )
    assert "GATED-OK" in _content(second, "c-probe-2")
    # Neither the ordinary read nor the restored skill receipt grants write
    # authority on the new lease.
    assert not ctx2.recent_read_matches("notes/ordinary.md", "ordinary bytes")
    assert not ctx2.recent_read_matches(SKILL_PATH, BODY_V1)

    # ---- changed content fails closed until one fresh read ----
    _workspace().write_file(SKILL_PATH, BODY_V2)
    ctx3, read_tool_3, probe_3 = _context_and_tools(_workspace())
    assert _restore(ctx3, first) == 0
    app3 = _compile(
        create_audited_tool_node([read_tool_3, probe_3], config, tool_context=ctx3)
    )
    stale = await app3.ainvoke(
        _state([make_tool_call("verify_probe", {}, "c-probe-3")], 2, 6)
    )
    assert "must read" in _content(stale, "c-probe-3").lower()
    reread = await app3.ainvoke(
        _state([make_tool_call("read_file", {"path": SKILL_PATH}, "c-read-3")], 2, 6)
    )
    assert "NODE-RESUME-BODY-v2" in _content(reread, "c-read-3")
    fixed = await app3.ainvoke(
        _state([make_tool_call("verify_probe", {}, "c-probe-3b")], 2, 7)
    )
    assert "GATED-OK" in _content(fixed, "c-probe-3b")

    # ---- a new concrete phase re-arms the gate until one fresh read ----
    _workspace().write_file(SKILL_PATH, BODY_V1)
    ctx4, read_tool_4, probe_4 = _context_and_tools(_workspace())
    assert _restore(ctx4, first) == 1
    app4 = _compile(
        create_audited_tool_node([read_tool_4, probe_4], config, tool_context=ctx4)
    )
    moved = await app4.ainvoke(
        _state([make_tool_call("verify_probe", {}, "c-probe-4")], 4, 9)
    )
    assert "must read" in _content(moved, "c-probe-4").lower()
    reread4 = await app4.ainvoke(
        _state([make_tool_call("read_file", {"path": SKILL_PATH}, "c-read-4")], 4, 9)
    )
    assert "NODE-RESUME-BODY-v1" in _content(reread4, "c-read-4")
    fixed4 = await app4.ainvoke(
        _state([make_tool_call("verify_probe", {}, "c-probe-4b")], 4, 10)
    )
    assert "GATED-OK" in _content(fixed4, "c-probe-4b")

    # ---- an expired turn window re-arms the gate until one fresh read ----
    ctx5, read_tool_5, probe_5 = _context_and_tools(_workspace())
    assert _restore(ctx5, first) == 1
    app5 = _compile(
        create_audited_tool_node([read_tool_5, probe_5], config, tool_context=ctx5)
    )
    expired = await app5.ainvoke(
        _state([make_tool_call("verify_probe", {}, "c-probe-5")], 2, 30)
    )
    assert "must read" in _content(expired, "c-probe-5").lower()
    reread5 = await app5.ainvoke(
        _state([make_tool_call("read_file", {"path": SKILL_PATH}, "c-read-5")], 2, 30)
    )
    assert "NODE-RESUME-BODY-v1" in _content(reread5, "c-read-5")
    fixed5 = await app5.ainvoke(
        _state([make_tool_call("verify_probe", {}, "c-probe-5b")], 2, 31)
    )
    assert "GATED-OK" in _content(fixed5, "c-probe-5b")


# =============================================================================
# Test: Fingerprint-based loop detection -> soft warnings (never blocks)
# =============================================================================


class TestFingerPrintWarning:
    """Tests for fingerprint-based loop detection with soft warnings."""

    @pytest.mark.asyncio
    async def test_identical_calls_get_warning(self, config):
        """After threshold identical calls, tool results get a LOOP WARNING."""
        fake_tool = MagicMock()
        fake_tool.name = "some_tool"

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node([fake_tool], config)

            # Call up to just under threshold (default 10 in last 30).
            # All calls should execute — loop detection is advisory.
            for i in range(9):
                call_id = f"call_{i}"
                mock_tn.ainvoke = AsyncMock(
                    return_value={
                        "messages": [make_tool_result("some_tool", "ok", call_id)]
                    }
                )
                state = make_state([make_tool_call("some_tool", {}, call_id)])
                result = await audited(state)
                msgs = result.get("messages", [])
                tool_msgs = [m for m in msgs if isinstance(m, ToolMessage)]
                assert len(tool_msgs) >= 1
                # No "restricted" — tools are never blocked
                assert not any("restricted" in (m.content or "") for m in tool_msgs)
                assert not any("LOOP WARNING" in (m.content or "") for m in tool_msgs)

            # Call 10: hits threshold (>= 10), should have LOOP WARNING appended
            mock_tn.ainvoke = AsyncMock(
                return_value={
                    "messages": [make_tool_result("some_tool", "ok", "call_9")]
                }
            )
            state = make_state([make_tool_call("some_tool", {}, "call_9")])
            result = await audited(state)
            msgs = result.get("messages", [])
            tool_msgs = [m for m in msgs if isinstance(m, ToolMessage)]
            assert len(tool_msgs) >= 1
            # Tool still executed (result contains "ok") plus warning
            assert any("LOOP WARNING" in (m.content or "") for m in tool_msgs)
            assert any("ok" in (m.content or "") for m in tool_msgs)

    @pytest.mark.asyncio
    async def test_different_args_no_warning(self, config):
        """Different arguments should NOT trigger warnings."""
        fake_tool = MagicMock()
        fake_tool.name = "search"

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            mock_tn.ainvoke = AsyncMock(
                return_value={"messages": [make_tool_result("search", "results")]}
            )
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node([fake_tool], config)

            # 15 calls with different args: no warnings
            for i in range(15):
                state = make_state(
                    [make_tool_call("search", {"query": f"query_{i}"}, f"call_{i}")]
                )
                result = await audited(state)
                msgs = [
                    m
                    for m in result.get("messages", [])
                    if isinstance(m, ToolMessage)
                    and "LOOP WARNING" in (m.content or "")
                ]
                assert len(msgs) == 0

    @pytest.mark.asyncio
    async def test_warning_resets_on_phase_change(self, config):
        """Loop warning state should be cleared when phase changes."""
        fake_tool = MagicMock()
        fake_tool.name = "some_tool"

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            mock_tn.ainvoke = AsyncMock(
                return_value={"messages": [make_tool_result("some_tool", "ok")]}
            )
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node([fake_tool], config)

            # Phase 1: build up call history (9 identical calls, just under threshold)
            for i in range(9):
                state = make_state(
                    [make_tool_call("some_tool", {}, f"call_{i}")],
                    phase_number=1,
                )
                await audited(state)

            # Phase 2: state resets, so 1 more call should NOT trigger warning
            state = make_state(
                [make_tool_call("some_tool", {}, "call_new")],
                phase_number=2,
            )
            result = await audited(state)
            msgs = result.get("messages", [])
            tool_msgs = [m for m in msgs if isinstance(m, ToolMessage)]
            assert not any("LOOP WARNING" in (m.content or "") for m in tool_msgs)

    @pytest.mark.asyncio
    async def test_tools_always_execute(self, config):
        """Tools should ALWAYS execute, even after warning threshold."""
        fake_tool = MagicMock()
        fake_tool.name = "stuck_tool"

        with patch("agent.graph.ToolNode") as MockToolNode:
            call_count = [0]

            async def counting_ainvoke(state):
                call_count[0] += 1
                return {"messages": [make_tool_result("stuck_tool", "ok")]}

            mock_tn = AsyncMock()
            mock_tn.ainvoke = counting_ainvoke
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node([fake_tool], config)

            # 15 identical calls — all should execute
            for i in range(15):
                state = make_state([make_tool_call("stuck_tool", {}, f"call_{i}")])
                await audited(state)

            assert call_count[0] == 15  # All 15 executed


# =============================================================================
# Test: Progress tracking
# =============================================================================


class TestProgressTracking:
    """Tests for progress-based stuck detection."""

    @pytest.mark.asyncio
    async def test_progress_tool_resets_counter(self, low_threshold_config):
        """Calling a progress tool should reset the stall counter."""
        fake_read = MagicMock()
        fake_read.name = "read_file"
        fake_complete = MagicMock()
        fake_complete.name = "todo_complete"

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node(
                [fake_read, fake_complete], low_threshold_config
            )

            # 2 non-progress calls (different args to avoid fingerprint warnings)
            for i in range(2):
                mock_tn.ainvoke = AsyncMock(
                    return_value={
                        "messages": [
                            make_tool_result("read_file", f"content_{i}", f"call_{i}")
                        ]
                    }
                )
                state = make_state(
                    [make_tool_call("read_file", {"path": f"f{i}"}, f"call_{i}")]
                )
                await audited(state)

            # 1 progress call (todo_complete) — resets counter.
            mock_tn.ainvoke = AsyncMock(
                return_value={
                    "messages": [make_tool_result("todo_complete", "Done", "call_p")]
                }
            )
            state = make_state(
                [make_tool_call("todo_complete", {"notes": "done"}, "call_p")]
            )
            result = await audited(state)
            # No stuck detection should have fired
            sys_msgs = [
                m for m in result.get("messages", []) if isinstance(m, SystemMessage)
            ]
            assert len(sys_msgs) == 0

            # 2 more non-progress calls — still under threshold (3)
            for i in range(2):
                mock_tn.ainvoke = AsyncMock(
                    return_value={
                        "messages": [
                            make_tool_result("read_file", f"more_{i}", f"call_g{i}")
                        ]
                    }
                )
                state = make_state(
                    [make_tool_call("read_file", {"path": f"g{i}"}, f"call_g{i}")]
                )
                result = await audited(state)
                sys_msgs = [
                    m
                    for m in result.get("messages", [])
                    if isinstance(m, SystemMessage)
                ]
                assert len(sys_msgs) == 0  # Not stuck yet

    @pytest.mark.asyncio
    async def test_failed_progress_tool_does_not_reset(self, low_threshold_config):
        """A progress tool that errors should NOT reset the counter."""
        fake_complete = MagicMock()
        fake_complete.name = "todo_complete"

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node([fake_complete], low_threshold_config)

            # 3 calls to todo_complete that all error (threshold = 3).
            # Use different args each time to avoid fingerprint warnings.
            for i in range(3):
                mock_tn.ainvoke = AsyncMock(
                    return_value={
                        "messages": [
                            make_tool_result(
                                "todo_complete",
                                f"Error: todo_{i} not found",
                                f"call_tc{i}",
                            )
                        ]
                    }
                )
                state = make_state(
                    [
                        make_tool_call(
                            "todo_complete", {"notes": f"attempt_{i}"}, f"call_tc{i}"
                        )
                    ]
                )
                result = await audited(state)

            # Should have triggered progress nudge (errors don't count as progress)
            all_msgs = result.get("messages", [])
            sys_msgs = [m for m in all_msgs if isinstance(m, SystemMessage)]
            assert len(sys_msgs) == 1
            assert "OBSERVATION:" in sys_msgs[0].content


# =============================================================================
# Test: Stuck detection -> reflection -> freeze
# =============================================================================


class TestStuckDetectionEscalation:
    """Tests for the stuck detection escalation chain."""

    @pytest.mark.asyncio
    async def test_nudge_injected_at_threshold(self, low_threshold_config):
        """Progress nudge should inject a SystemMessage at threshold."""
        fake_tool = MagicMock()
        fake_tool.name = "read_file"

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node(
                [fake_tool],
                low_threshold_config,  # threshold=3
            )

            # 3 non-progress calls (different args to avoid fingerprint warnings)
            for i in range(3):
                mock_tn.ainvoke = AsyncMock(
                    return_value={
                        "messages": [
                            make_tool_result("read_file", f"content_{i}", f"call_{i}")
                        ]
                    }
                )
                state = make_state(
                    [
                        make_tool_call(
                            "read_file", {"path": f"file_{i}.txt"}, f"call_{i}"
                        )
                    ]
                )
                result = await audited(state)

            # 3rd call should trigger progress nudge
            all_msgs = result.get("messages", [])
            sys_msgs = [m for m in all_msgs if isinstance(m, SystemMessage)]
            assert len(sys_msgs) == 1
            assert "OBSERVATION:" in sys_msgs[0].content
            assert "write it to a file" in sys_msgs[0].content
            assert not result.get("should_stop", False)

    @pytest.mark.asyncio
    async def test_no_freeze_after_nudge(self, low_threshold_config):
        """Agent should never be frozen by progress nudge — only nudged repeatedly."""
        fake_tool = MagicMock()
        fake_tool.name = "read_file"

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node(
                [fake_tool],
                low_threshold_config,  # threshold=3
            )

            # 6 non-progress calls (2x threshold): should get nudges but never freeze
            for i in range(6):
                mock_tn.ainvoke = AsyncMock(
                    return_value={
                        "messages": [
                            make_tool_result("read_file", f"c_{i}", f"call_{i}")
                        ]
                    }
                )
                state = make_state(
                    [make_tool_call("read_file", {"path": f"f_{i}"}, f"call_{i}")]
                )
                result = await audited(state)

            assert not result.get("should_stop", False)
            assert result.get("freeze_data") is None

    @pytest.mark.asyncio
    async def test_progress_after_nudge_resets(self, low_threshold_config):
        """Progress after nudge should reset the counter."""
        fake_read = MagicMock()
        fake_read.name = "read_file"
        fake_write = MagicMock()
        fake_write.name = "write_file"

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node(
                [fake_read, fake_write], low_threshold_config
            )

            # 3 non-progress calls: triggers nudge
            for i in range(3):
                mock_tn.ainvoke = AsyncMock(
                    return_value={
                        "messages": [
                            make_tool_result("read_file", f"c_{i}", f"call_{i}")
                        ]
                    }
                )
                state = make_state(
                    [make_tool_call("read_file", {"path": f"f_{i}"}, f"call_{i}")]
                )
                await audited(state)

            # Now make progress with write_file
            mock_tn.ainvoke = AsyncMock(
                return_value={
                    "messages": [make_tool_result("write_file", "Written", "call_w")]
                }
            )
            state = make_state(
                [make_tool_call("write_file", {"path": "out.md"}, "call_w")]
            )
            result = await audited(state)
            assert not result.get("should_stop", False)

            # 3 more non-progress calls: should trigger another nudge
            for i in range(3):
                mock_tn.ainvoke = AsyncMock(
                    return_value={
                        "messages": [
                            make_tool_result("read_file", f"d_{i}", f"call_d{i}")
                        ]
                    }
                )
                state = make_state(
                    [make_tool_call("read_file", {"path": f"g_{i}"}, f"call_d{i}")]
                )
                result = await audited(state)

            # Should get another nudge (not a freeze)
            sys_msgs = [
                m for m in result.get("messages", []) if isinstance(m, SystemMessage)
            ]
            assert len(sys_msgs) == 1
            assert "OBSERVATION:" in sys_msgs[0].content


class TestProgressHeartbeatMarker:
    """Tests for heartbeat-facing graph-progress signaling."""

    @pytest.mark.asyncio
    async def test_graph_progress_increments_on_each_batch(self, config):
        """Each executed tool batch increments graph-progress by one."""
        fake_tool = MagicMock()
        fake_tool.name = "read_file"
        tool_context = ToolContext()

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            mock_tn.ainvoke = AsyncMock(
                return_value={"messages": [make_tool_result("read_file", "ok")]}
            )
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node(
                [fake_tool], config, tool_context=tool_context
            )

            assert tool_context.get_graph_progress() == 0

            state = make_state(
                [make_tool_call("read_file", {"path": "a.txt"}, "call_a")]
            )
            await audited(state)
            assert tool_context.get_graph_progress() == 1

            state = make_state(
                [make_tool_call("read_file", {"path": "b.txt"}, "call_b")]
            )
            await audited(state)
            assert tool_context.get_graph_progress() == 2


# =============================================================================
# Test: Hard cap
# =============================================================================


class TestBudgetCaps:
    """Tests for the tool-call budgets.

    The per-phase cap is now OFF by default and the JOB cap is the real
    backstop — bounding a phase stopped bounding a job once phases got large
    (it reset at every boundary). Exceeding either freezes; the old tactical
    branch instead called ``archive_with_failure_note``, which wrote every todo
    — completed ones included — into a failure archive and emptied the list.
    """

    @pytest.mark.asyncio
    async def test_job_cap_freezes_without_destroying_todos(self):
        cfg = FakeConfig(limits=LimitsConfig(max_tool_calls_per_job=5))
        fake_tool = MagicMock()
        fake_tool.name = "read_file"

        mock_todo = MagicMock()
        mock_todo.archive_with_failure_note = MagicMock(return_value="Archived")
        mock_todo.has_staged_todos = MagicMock(return_value=False)
        mock_ctx = MagicMock()
        mock_ctx.workspace_manager = MagicMock()
        mock_ctx.todo_manager = mock_todo
        mock_ctx.consume_freeze_request.return_value = None
        mock_ctx.drain_pending_memories.return_value = []

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            mock_tn.ainvoke = AsyncMock(
                return_value={"messages": [make_tool_result("read_file", "ok")]}
            )
            MockToolNode.return_value = mock_tn
            audited = create_audited_tool_node([fake_tool], cfg, tool_context=mock_ctx)

            for i in range(5):
                state = make_state(
                    [make_tool_call("read_file", {"path": f"f_{i}"}, f"call_{i}")]
                )
                assert not (await audited(state)).get("should_stop", False)

            state = make_state([make_tool_call("read_file", {"path": "f_6"}, "call_6")])
            result = await audited(state)

        assert result.get("should_stop") is True
        assert result["freeze_data"]["freeze_type"] == "budget_exceeded"
        assert result["freeze_data"]["budget_scope"] == "job"
        # The regression this replaced: todos must survive.
        mock_todo.archive_with_failure_note.assert_not_called()

    @pytest.mark.asyncio
    async def test_job_cap_does_not_reset_on_phase_change(self):
        """The whole point — a per-phase counter never bounded the job."""
        cfg = FakeConfig(limits=LimitsConfig(max_tool_calls_per_job=6))
        fake_tool = MagicMock()
        fake_tool.name = "read_file"

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            mock_tn.ainvoke = AsyncMock(
                return_value={"messages": [make_tool_result("read_file", "ok")]}
            )
            MockToolNode.return_value = mock_tn
            audited = create_audited_tool_node([fake_tool], cfg)

            for i in range(4):
                await audited(
                    make_state(
                        [make_tool_call("read_file", {"path": f"f_{i}"}, f"c{i}")],
                        phase_number=1,
                    )
                )
            # New phase: the phase counter resets, the job counter must not.
            results = []
            for i in range(3):
                results.append(
                    await audited(
                        make_state(
                            [make_tool_call("read_file", {"path": f"g_{i}"}, f"g{i}")],
                            phase_number=2,
                        )
                    )
                )

        assert results[-1].get("should_stop") is True
        assert results[-1]["freeze_data"]["budget_scope"] == "job"

    @pytest.mark.asyncio
    async def test_job_cap_zero_disables_the_stop(self):
        cfg = FakeConfig(limits=LimitsConfig(max_tool_calls_per_job=0))
        fake_tool = MagicMock()
        fake_tool.name = "read_file"

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            mock_tn.ainvoke = AsyncMock(
                return_value={"messages": [make_tool_result("read_file", "ok")]}
            )
            MockToolNode.return_value = mock_tn
            audited = create_audited_tool_node([fake_tool], cfg)

            for i in range(40):
                result = await audited(
                    make_state(
                        [make_tool_call("read_file", {"path": f"f_{i}"}, f"c{i}")]
                    )
                )
                assert not result.get("should_stop", False)

    @pytest.mark.asyncio
    async def test_phase_cap_still_honoured_when_set(self):
        """Back-compat: an operator who deliberately sets one still gets it."""
        cfg = FakeConfig(
            limits=LimitsConfig(max_tool_calls_per_phase=5, max_tool_calls_per_job=0)
        )
        fake_tool = MagicMock()
        fake_tool.name = "read_file"

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            mock_tn.ainvoke = AsyncMock(
                return_value={"messages": [make_tool_result("read_file", "ok")]}
            )
            MockToolNode.return_value = mock_tn
            audited = create_audited_tool_node([fake_tool], cfg)

            for i in range(5):
                await audited(
                    make_state(
                        [make_tool_call("read_file", {"path": f"f_{i}"}, f"c{i}")]
                    )
                )
            result = await audited(
                make_state([make_tool_call("read_file", {"path": "f6"}, "c6")])
            )

        assert result.get("should_stop") is True
        assert result["freeze_data"]["budget_scope"] == "phase"

    @pytest.mark.asyncio
    async def test_phase_cap_off_by_default(self):
        """The default config must not stop a long single phase."""
        cfg = FakeConfig(limits=LimitsConfig())
        assert cfg.limits.max_tool_calls_per_phase == 0
        assert cfg.limits.max_tool_calls_per_job == 5000


# =============================================================================
# Test: Tool-not-found enrichment
# =============================================================================


class TestToolNotFoundEnrichment:
    """Tests for enriching tool-not-found errors with guidance."""

    @pytest.mark.asyncio
    async def test_unknown_tool_gets_guidance(self, config):
        """Tool-not-found errors should get actionable guidance appended."""
        fake_tool = MagicMock()
        fake_tool.name = "read_file"

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            # Simulate ToolNode's response for unknown tool
            mock_tn.ainvoke = AsyncMock(
                return_value={
                    "messages": [
                        ToolMessage(
                            content="Error: kb_list is not a valid tool, try one of [read_file].",
                            tool_call_id="call_kb",
                            name="kb_list",
                            status="error",
                        )
                    ]
                }
            )
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node([fake_tool], config)

            state = make_state([make_tool_call("kb_list", {}, "call_kb")])
            result = await audited(state)

            msgs = result.get("messages", [])
            tool_msgs = [m for m in msgs if isinstance(m, ToolMessage)]
            assert len(tool_msgs) >= 1
            assert "todo_complete" in tool_msgs[0].content
            assert "blocked" in tool_msgs[0].content

    @pytest.mark.asyncio
    async def test_normal_error_not_enriched(self, config):
        """Normal tool errors should NOT get the not-found guidance."""
        fake_tool = MagicMock()
        fake_tool.name = "read_file"

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            mock_tn.ainvoke = AsyncMock(
                return_value={
                    "messages": [
                        make_tool_result(
                            "read_file", "Error: file not found: missing.txt"
                        )
                    ]
                }
            )
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node([fake_tool], config)

            state = make_state(
                [make_tool_call("read_file", {"path": "missing.txt"}, "call_rf")]
            )
            result = await audited(state)

            msgs = result.get("messages", [])
            tool_msgs = [m for m in msgs if isinstance(m, ToolMessage)]
            assert len(tool_msgs) >= 1
            # Should NOT have the "blocked" guidance
            assert "blocked" not in tool_msgs[0].content


# =============================================================================
# Test: Category failure tracking (logging only, no masking)
# =============================================================================


class TestCategoryFailureTracking:
    """Tests for category-wide failure logging (no masking)."""

    @pytest.mark.asyncio
    async def test_category_failures_logged_not_masked(self, config):
        """3 distinct tools in same category failing should log, NOT mask."""
        fake_tools = []
        for name in ["kb_read", "kb_write", "kb_search", "kb_list", "read_file"]:
            t = MagicMock()
            t.name = name
            fake_tools.append(t)

        # Mock TOOL_REGISTRY to return "knowledge" category for kb_* tools
        mock_registry = {
            "kb_read": {"category": "knowledge"},
            "kb_write": {"category": "knowledge"},
            "kb_search": {"category": "knowledge"},
            "kb_list": {"category": "knowledge"},
            "read_file": {"category": "workspace"},
        }

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node(fake_tools, config)

            # Patch the registry lookups inside the closure helpers
            with patch("agent.tools.registry.TOOL_REGISTRY", mock_registry):
                # 3 different kb tools all fail
                for i, name in enumerate(["kb_read", "kb_write", "kb_search"]):
                    mock_tn.ainvoke = AsyncMock(
                        return_value={
                            "messages": [
                                make_tool_result(
                                    name,
                                    "Error: knowledge store not configured",
                                    f"call_{name}",
                                )
                            ]
                        }
                    )
                    state = make_state(
                        [make_tool_call(name, {"id": f"note_{i}"}, f"call_{name}")]
                    )
                    await audited(state)

                # Now kb_list (same category) should still execute (no masking)
                mock_tn.ainvoke = AsyncMock(
                    return_value={
                        "messages": [
                            make_tool_result(
                                "kb_list", "Error: not configured", "call_kl"
                            )
                        ]
                    }
                )
                state = make_state([make_tool_call("kb_list", {}, "call_kl")])
                result = await audited(state)
                msgs = result.get("messages", [])
                tool_msgs = [m for m in msgs if isinstance(m, ToolMessage)]
                # Tool should have executed (no "restricted" message)
                assert not any("restricted" in (m.content or "") for m in tool_msgs)
                # But the error from the tool itself should be there
                assert any("not configured" in (m.content or "") for m in tool_msgs)


class TestToolNodeTimeoutEscalation:
    """Tests for audited tool-node timeout behavior and reconnect/failure handoff."""

    @pytest.mark.asyncio
    async def test_tool_node_timeout_reconnects_and_returns_error_once(self):
        """A first timeout disconnects and reconnects, then returns timeout errors."""

        fake_tool = MagicMock()
        fake_tool.name = "search_files"

        cfg = FakeConfig(
            limits=LimitsConfig(
                tool_category_timeouts={"default": 1},
            )
        )

        async def slow_ainvoke(_):
            await asyncio.sleep(1.25)
            return {"messages": [make_tool_result("search_files", "ok", "call_t")]}

        backend = MagicMock()
        ctx = MagicMock()
        ctx.workspace_manager = MagicMock(backend=backend)
        ctx.consume_freeze_request.return_value = None
        ctx.drain_pending_memories.return_value = []

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            mock_tn.ainvoke = slow_ainvoke
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node([fake_tool], cfg, tool_context=ctx)
            start = time.perf_counter()
            result = await audited(
                make_state(
                    [make_tool_call("search_files", {"query": "needle"}, "call_t")]
                )
            )
            elapsed = time.perf_counter() - start

        msgs = [m for m in result.get("messages", []) if isinstance(m, ToolMessage)]
        assert len(msgs) == 1
        assert "timed out" in (msgs[0].content or "").lower()
        assert elapsed >= 1.0
        assert elapsed < 2.0
        assert backend.method_calls == [
            call.shell_reset_after_timeout(),
            call.disconnect(),
            call.connect(),
        ]

    @pytest.mark.asyncio
    async def test_tool_node_timeout_repeated_raises_workspace_unavailable(self):
        """A second timeout without recovery raises WorkspaceUnavailableError."""

        fake_tool = MagicMock()
        fake_tool.name = "search_files"

        cfg = FakeConfig(
            limits=LimitsConfig(
                tool_category_timeouts={"default": 1},
            )
        )

        async def slow_ainvoke(_):
            await asyncio.sleep(1.25)
            return {"messages": [make_tool_result("search_files", "ok", "call_t")]}

        backend = MagicMock()
        ctx = MagicMock()
        ctx.workspace_manager = MagicMock(backend=backend)
        ctx.consume_freeze_request.return_value = None
        ctx.drain_pending_memories.return_value = []

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            mock_tn.ainvoke = slow_ainvoke
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node([fake_tool], cfg, tool_context=ctx)
            await audited(
                make_state(
                    [make_tool_call("search_files", {"query": "needle"}, "call_t")]
                )
            )

            with pytest.raises(WorkspaceUnavailableError):
                await audited(
                    make_state(
                        [make_tool_call("search_files", {"query": "needle"}, "call_t2")]
                    )
                )

        assert backend.shell_reset_after_timeout.call_count == 1
        assert backend.disconnect.call_count == 1
        assert backend.connect.call_count == 1

    @pytest.mark.asyncio
    async def test_tool_node_timeout_reconnect_failure_raises_workspace_unavailable(
        self,
    ):
        """If reconnect fails, timeout escalates immediately to WorkspaceUnavailableError."""

        fake_tool = MagicMock()
        fake_tool.name = "search_files"

        cfg = FakeConfig(
            limits=LimitsConfig(
                tool_category_timeouts={"default": 1},
            )
        )

        async def slow_ainvoke(_):
            await asyncio.sleep(1.25)
            return {"messages": [make_tool_result("search_files", "ok", "call_t")]}

        backend = MagicMock()
        backend.connect.side_effect = RuntimeError("can't connect")
        ctx = MagicMock()
        ctx.workspace_manager = MagicMock(backend=backend)
        ctx.consume_freeze_request.return_value = None
        ctx.drain_pending_memories.return_value = []

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            mock_tn.ainvoke = slow_ainvoke
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node([fake_tool], cfg, tool_context=ctx)
            with pytest.raises(WorkspaceUnavailableError):
                await audited(
                    make_state(
                        [make_tool_call("search_files", {"query": "needle"}, "call_t")]
                    )
                )

        assert backend.method_calls == [
            call.shell_reset_after_timeout(),
            call.disconnect(),
            call.connect(),
        ]

    @pytest.mark.asyncio
    async def test_tool_node_timeout_reset_failure_never_reconnects(self):
        """An unproven shell reset must stop recovery before transport reuse."""

        fake_tool = MagicMock()
        fake_tool.name = "run_command"
        cfg = FakeConfig(
            limits=LimitsConfig(
                tool_category_timeouts={"default": 1},
            )
        )

        async def slow_ainvoke(_):
            await asyncio.sleep(1.25)
            return {"messages": [make_tool_result("run_command", "ok", "call_t")]}

        backend = MagicMock()
        backend.shell_reset_after_timeout.side_effect = RuntimeError("fence moved")
        ctx = MagicMock()
        ctx.workspace_manager = MagicMock(backend=backend)
        ctx.consume_freeze_request.return_value = None
        ctx.drain_pending_memories.return_value = []

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            mock_tn.ainvoke = slow_ainvoke
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node([fake_tool], cfg, tool_context=ctx)
            with pytest.raises(WorkspaceUnavailableError):
                await audited(
                    make_state(
                        [
                            make_tool_call(
                                "run_command",
                                {"command": "sleep 5"},
                                "call_t",
                            )
                        ]
                    )
                )

        backend.shell_reset_after_timeout.assert_called_once_with()
        backend.disconnect.assert_not_called()
        backend.connect.assert_not_called()


# =============================================================================
# Test: Structured freeze payload
# =============================================================================


class TestFreezePayload:
    """Tests for the structured freeze payload content."""

    @pytest.mark.asyncio
    async def test_progress_nudge_repeats_periodically(self):
        """Progress nudge should fire every threshold interval, never freeze."""
        cfg = FakeConfig(limits=LimitsConfig(progress_stall_threshold=2))
        fake_tool = MagicMock()
        fake_tool.name = "read_file"

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node([fake_tool], cfg)

            nudge_counts = 0
            # 6 calls = 3x threshold of 2
            for i in range(6):
                mock_tn.ainvoke = AsyncMock(
                    return_value={
                        "messages": [
                            make_tool_result("read_file", f"c_{i}", f"call_{i}")
                        ]
                    }
                )
                state = make_state(
                    [make_tool_call("read_file", {"path": f"f_{i}"}, f"call_{i}")]
                )
                result = await audited(state)
                sys_msgs = [
                    m
                    for m in result.get("messages", [])
                    if isinstance(m, SystemMessage) and "OBSERVATION:" in m.content
                ]
                nudge_counts += len(sys_msgs)

            # Should have gotten nudges at calls 2, 4, 6
            assert nudge_counts == 3
            # Never frozen
            assert not result.get("should_stop", False)

    @pytest.mark.asyncio
    async def test_hard_cap_strategic_freeze_has_required_fields(self):
        """Hard cap freeze in strategic phase should contain budget information."""
        cfg = FakeConfig(limits=LimitsConfig(max_tool_calls_per_phase=2))
        fake_tool = MagicMock()
        fake_tool.name = "read_file"

        mock_ws = MagicMock()
        mock_ws.git_manager = MagicMock()
        mock_ws.git_manager.is_active = False
        mock_ctx = MagicMock()
        mock_ctx.workspace_manager = mock_ws
        mock_ctx.consume_freeze_request.return_value = None
        mock_ctx.drain_pending_memories.return_value = []

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            mock_tn.ainvoke = AsyncMock(
                return_value={"messages": [make_tool_result("read_file", "ok")]}
            )
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node([fake_tool], cfg, tool_context=mock_ctx)

            # 3 calls in strategic phase: exceeds cap of 2
            for i in range(3):
                state = make_state(
                    [make_tool_call("read_file", {"path": f"f_{i}"}, f"call_{i}")],
                    is_strategic=True,
                )
                result = await audited(state)

            assert result.get("should_stop") is True
            mock_ws.write_file.assert_called_once()
            freeze_json = mock_ws.write_file.call_args[0][1]
            payload = json.loads(freeze_json)

            assert payload["freeze_type"] == "budget_exceeded"
            assert "tool_calls_this_phase" in payload
            assert (
                payload["tool_calls_this_phase"] > cfg.limits.max_tool_calls_per_phase
            )


# =============================================================================
# Test: request_replan resets loop detection
# =============================================================================


class TestRequestReplanReset:
    """Tests for request_replan resetting loop detection state."""

    @pytest.mark.asyncio
    async def test_rewind_clears_loop_state(self, config):
        """A successful request_replan should reset loop detection state."""
        fake_tool = MagicMock()
        fake_tool.name = "some_tool"
        fake_rewind = MagicMock()
        fake_rewind.name = "request_replan"

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node([fake_tool, fake_rewind], config)

            # Build up 9 identical calls (just under threshold of 10)
            mock_tn.ainvoke = AsyncMock(
                return_value={"messages": [make_tool_result("some_tool", "ok")]}
            )
            for i in range(9):
                state = make_state([make_tool_call("some_tool", {}, f"call_{i}")])
                await audited(state)

            # Rewind — should reset loop detection
            mock_tn.ainvoke = AsyncMock(
                return_value={
                    "messages": [
                        make_tool_result(
                            "request_replan",
                            "Replan requested. Progress kept...",
                            "call_rw",
                        )
                    ]
                }
            )
            state = make_state(
                [
                    make_tool_call(
                        "request_replan", {"reason": "approach broken"}, "call_rw"
                    )
                ]
            )
            await audited(state)

            # Now 9 more identical calls should NOT trigger warning
            # (because rewind cleared the history)
            mock_tn.ainvoke = AsyncMock(
                return_value={"messages": [make_tool_result("some_tool", "ok again")]}
            )
            for i in range(9):
                state = make_state([make_tool_call("some_tool", {}, f"call_post_{i}")])
                result = await audited(state)
                msgs = result.get("messages", [])
                tool_msgs = [m for m in msgs if isinstance(m, ToolMessage)]
                assert not any("LOOP WARNING" in (m.content or "") for m in tool_msgs)

    @pytest.mark.asyncio
    async def test_failed_rewind_does_not_reset(self, config):
        """A failed request_replan should NOT reset loop detection state."""
        fake_tool = MagicMock()
        fake_tool.name = "some_tool"
        fake_rewind = MagicMock()
        fake_rewind.name = "request_replan"

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node([fake_tool, fake_rewind], config)

            # Build up 9 identical calls
            mock_tn.ainvoke = AsyncMock(
                return_value={"messages": [make_tool_result("some_tool", "ok")]}
            )
            for i in range(9):
                state = make_state([make_tool_call("some_tool", {}, f"call_{i}")])
                await audited(state)

            # Failed rewind — should NOT reset
            mock_tn.ainvoke = AsyncMock(
                return_value={
                    "messages": [
                        make_tool_result(
                            "request_replan",
                            "Error: You must provide an 'issue' describing why",
                            "call_rw",
                        )
                    ]
                }
            )
            state = make_state(
                [make_tool_call("request_replan", {"reason": ""}, "call_rw")]
            )
            await audited(state)

            # 1 more identical call should now trigger warning (9+1 = 10 = threshold)
            mock_tn.ainvoke = AsyncMock(
                return_value={
                    "messages": [
                        make_tool_result("some_tool", "ok still", "call_extra")
                    ]
                }
            )
            state = make_state([make_tool_call("some_tool", {}, "call_extra")])
            result = await audited(state)
            msgs = result.get("messages", [])
            tool_msgs = [m for m in msgs if isinstance(m, ToolMessage)]
            assert any("LOOP WARNING" in (m.content or "") for m in tool_msgs)


# =============================================================================
# Test: Phase gate defense-in-depth (rejects wrong-phase tool calls)
# =============================================================================


class TestPhaseGate:
    """The runtime phase gate in audited_tools.

    With one tool binding for every phase, the gate IS the enforcement and
    decides per call: the batch's phase-legal calls execute, each illegal one
    gets an error ToolMessage naming its phase. The full per-call contract
    (audit, budget, progress, order, timeout) is in tests/test_phase_gate.py.
    """

    @pytest.mark.asyncio
    async def test_registered_tool_rejected_in_wrong_phase(self, config):
        """A tool registered as strategic-only is rejected in tactical phase."""
        fake_tool = MagicMock()
        fake_tool.name = "job_complete"  # strategic-only in registry

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node([fake_tool], config)

            # Call in tactical phase — should be rejected by phase gate
            state = make_state(
                [
                    make_tool_call(
                        "job_complete",
                        {"summary": "done", "deliverables": []},
                        "call_jc",
                    )
                ],
                is_strategic=False,
            )
            result = await audited(state)
            msgs = result.get("messages", [])
            tool_msgs = [m for m in msgs if isinstance(m, ToolMessage)]

            assert len(tool_msgs) == 1
            assert (
                "'job_complete' is a strategic-phase tool; you are in the "
                "tactical phase (phase 1)" in tool_msgs[0].content
            )
            assert tool_msgs[0].tool_call_id == "call_jc"

            # ToolNode should NOT have been called
            mock_tn.ainvoke.assert_not_called()

    @pytest.mark.asyncio
    async def test_registered_tool_allowed_in_correct_phase(self, config):
        """A tool registered as strategic-only passes in strategic phase."""
        fake_tool = MagicMock()
        fake_tool.name = "job_complete"

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            mock_tn.ainvoke = AsyncMock(
                return_value={
                    "messages": [make_tool_result("job_complete", "ok", "call_jc")]
                }
            )
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node([fake_tool], config)

            # Call in strategic phase — should pass through to ToolNode
            state = make_state(
                [make_tool_call("job_complete", {}, "call_jc")],
                is_strategic=True,
            )
            result = await audited(state)
            msgs = result.get("messages", [])
            tool_msgs = [m for m in msgs if isinstance(m, ToolMessage)]

            assert len(tool_msgs) == 1
            assert tool_msgs[0].content == "ok"
            mock_tn.ainvoke.assert_called_once()

    @pytest.mark.asyncio
    async def test_tactical_only_tool_rejected_in_strategic(self, config):
        """request_replan (tactical-only) is rejected in strategic phase."""
        fake_tool = MagicMock()
        fake_tool.name = "request_replan"

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node([fake_tool], config)

            state = make_state(
                [make_tool_call("request_replan", {"reason": "stuck"}, "call_rw")],
                is_strategic=True,
            )
            result = await audited(state)
            msgs = result.get("messages", [])
            tool_msgs = [m for m in msgs if isinstance(m, ToolMessage)]

            assert len(tool_msgs) == 1
            assert (
                "'request_replan' is a tactical-phase tool; you are in the "
                "strategic phase (phase 1)" in tool_msgs[0].content
            )
            assert "next_phase_todos" in tool_msgs[0].content
            mock_tn.ainvoke.assert_not_called()

    @pytest.mark.asyncio
    async def test_unregistered_tool_passes_through(self, config):
        """Tools not in TOOL_REGISTRY are not phase-gated (test/dynamic tools)."""
        fake_tool = MagicMock()
        fake_tool.name = "custom_dynamic_tool"  # Not in TOOL_REGISTRY

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            mock_tn.ainvoke = AsyncMock(
                return_value={
                    "messages": [
                        make_tool_result("custom_dynamic_tool", "executed", "call_cd")
                    ]
                }
            )
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node([fake_tool], config)

            # Should pass through regardless of phase
            state = make_state(
                [make_tool_call("custom_dynamic_tool", {}, "call_cd")],
                is_strategic=False,
            )
            result = await audited(state)
            msgs = result.get("messages", [])
            tool_msgs = [m for m in msgs if isinstance(m, ToolMessage)]

            assert len(tool_msgs) == 1
            assert tool_msgs[0].content == "executed"
            mock_tn.ainvoke.assert_called_once()

    @pytest.mark.asyncio
    async def test_batch_executes_legal_calls_and_rejects_illegal_ones_per_call(
        self, config
    ):
        """A mixed batch is decided per call (U2): the phase-legal call runs
        through the ToolNode, the illegal one gets its error in place.

        Only the violating call is told it is phase-illegal — telling a legal
        tool it is unavailable taught models their tool surface was
        unreliable (job edd06963 "stale palette" belief spiral).
        """
        fake_read = MagicMock()
        fake_read.name = "read_file"  # both phases
        fake_jc = MagicMock()
        fake_jc.name = "job_complete"  # strategic only

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            mock_tn.ainvoke = AsyncMock(
                return_value={
                    "messages": [make_tool_result("read_file", "x-content", "call_rf")]
                }
            )
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node([fake_read, fake_jc], config)

            # Tactical phase: read_file is fine, job_complete is not
            state = make_state(
                [
                    make_tool_call("read_file", {"path": "x"}, "call_rf"),
                    make_tool_call("job_complete", {}, "call_jc"),
                ],
                is_strategic=False,
            )
            result = await audited(state)
            msgs = result.get("messages", [])
            tool_msgs = [m for m in msgs if isinstance(m, ToolMessage)]

            # One answer per call, original order kept
            assert [m.tool_call_id for m in tool_msgs] == ["call_rf", "call_jc"]
            assert tool_msgs[0].content == "x-content"
            assert (
                "'job_complete' is a strategic-phase tool; you are in the "
                "tactical phase (phase 1)" in tool_msgs[1].content
            )
            assert "Other calls in this batch were executed normally." in (
                tool_msgs[1].content
            )
            assert "read_file" not in tool_msgs[1].content

            # The ToolNode ran once, on the legal call only
            mock_tn.ainvoke.assert_awaited_once()
            seen = mock_tn.ainvoke.await_args.args[0]["messages"][-1]
            assert [c["name"] for c in seen.tool_calls] == ["read_file"]

    @pytest.mark.asyncio
    async def test_both_phase_tool_passes_in_either(self, config):
        """Tools declared for both phases work in both."""
        fake_tool = MagicMock()
        fake_tool.name = "read_file"  # both phases in registry

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            MockToolNode.return_value = mock_tn

            audited = create_audited_tool_node([fake_tool], config)

            for is_strategic in [True, False]:
                mock_tn.ainvoke = AsyncMock(
                    return_value={
                        "messages": [
                            make_tool_result("read_file", "content", "call_rf")
                        ]
                    }
                )
                state = make_state(
                    [make_tool_call("read_file", {}, "call_rf")],
                    is_strategic=is_strategic,
                )
                result = await audited(state)
                tool_msgs = [
                    m for m in result.get("messages", []) if isinstance(m, ToolMessage)
                ]
                assert tool_msgs[0].content == "content"


# =============================================================================
# Test: Act-ratio tripwire (process-artifact-only streaks)
# =============================================================================


class TestActRatioTripwire:
    """N consecutive process-artifact-only tool actions inject a one-line
    "stop planning" nudge; any concrete action resets the counter."""

    def _make_audited(self, threshold=3):
        cfg = FakeConfig(limits=LimitsConfig(act_ratio_nudge_threshold=threshold))
        fake_tools = []
        for name in ["read_file", "edit_file", "write_file", "todo_list", "web_search"]:
            t = MagicMock()
            t.name = name
            fake_tools.append(t)
        patcher = patch("agent.graph.ToolNode")
        MockToolNode = patcher.start()
        mock_tn = AsyncMock()
        MockToolNode.return_value = mock_tn
        audited = create_audited_tool_node(fake_tools, cfg)
        return audited, mock_tn, patcher

    @staticmethod
    def _act_nudges(result):
        return [
            m
            for m in result.get("messages", [])
            if isinstance(m, SystemMessage) and "Stop planning" in m.content
        ]

    async def _run_call(self, audited, mock_tn, name, args, call_id):
        mock_tn.ainvoke = AsyncMock(
            return_value={"messages": [make_tool_result(name, "ok", call_id)]}
        )
        state = make_state([make_tool_call(name, args, call_id)])
        return await audited(state)

    @pytest.mark.asyncio
    async def test_nudge_fires_after_n_process_only_actions(self):
        audited, mock_tn, patcher = self._make_audited(threshold=3)
        try:
            process_calls = [
                ("read_file", {"path": "todos.yaml"}),
                ("edit_file", {"path": "plan.md", "old_string": "a"}),
                ("write_file", {"path": "archive/phase_2_retrospective.md"}),
            ]
            for i, (name, args) in enumerate(process_calls):
                result = await self._run_call(audited, mock_tn, name, args, f"c_{i}")
                if i < 2:
                    assert self._act_nudges(result) == []
            nudges = self._act_nudges(result)
            assert len(nudges) == 1
            assert "3 steps" in nudges[0].content
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    async def test_concrete_action_resets_counter(self):
        audited, mock_tn, patcher = self._make_audited(threshold=3)
        try:
            await self._run_call(
                audited, mock_tn, "read_file", {"path": "plan.md"}, "c0"
            )
            await self._run_call(
                audited, mock_tn, "edit_file", {"path": "todos.yaml"}, "c1"
            )
            # Concrete target -> reset
            result = await self._run_call(
                audited, mock_tn, "write_file", {"path": "output/report.md"}, "c2"
            )
            assert self._act_nudges(result) == []
            # Two more process actions: streak is 2, still under threshold
            await self._run_call(
                audited, mock_tn, "read_file", {"path": "plan.md"}, "c3"
            )
            result = await self._run_call(
                audited,
                mock_tn,
                "edit_file",
                {"path": "plan.md", "old_string": "x"},
                "c4",
            )
            assert self._act_nudges(result) == []
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    async def test_no_target_tool_resets_counter(self):
        """Tools without file targets (e.g. web_search) count as concrete."""
        audited, mock_tn, patcher = self._make_audited(threshold=3)
        try:
            await self._run_call(
                audited, mock_tn, "read_file", {"path": "plan.md"}, "c0"
            )
            await self._run_call(
                audited,
                mock_tn,
                "edit_file",
                {"path": "plan.md", "old_string": "x"},
                "c1",
            )
            result = await self._run_call(
                audited, mock_tn, "web_search", {"query": "topic"}, "c2"
            )
            assert self._act_nudges(result) == []
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    async def test_todo_state_tools_count_as_process(self):
        audited, mock_tn, patcher = self._make_audited(threshold=3)
        try:
            for i in range(2):
                result = await self._run_call(
                    audited, mock_tn, "todo_list", {}, f"t{i}"
                )
                assert self._act_nudges(result) == []
            result = await self._run_call(
                audited, mock_tn, "read_file", {"path": "todos.yaml"}, "t2"
            )
            assert len(self._act_nudges(result)) == 1
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    async def test_rearms_after_firing(self):
        audited, mock_tn, patcher = self._make_audited(threshold=2)
        try:
            await self._run_call(audited, mock_tn, "todo_list", {}, "a0")
            result = await self._run_call(audited, mock_tn, "todo_list", {}, "a1")
            assert len(self._act_nudges(result)) == 1
            # Counter re-armed: two more process actions fire again
            await self._run_call(audited, mock_tn, "todo_list", {}, "a2")
            result = await self._run_call(audited, mock_tn, "todo_list", {}, "a3")
            assert len(self._act_nudges(result)) == 1
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    async def test_zero_threshold_disables(self):
        audited, mock_tn, patcher = self._make_audited(threshold=0)
        try:
            result = None
            for i in range(8):
                result = await self._run_call(
                    audited, mock_tn, "read_file", {"path": "todos.yaml"}, f"z{i}"
                )
            assert self._act_nudges(result) == []
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    async def test_counter_resets_on_phase_change(self):
        audited, mock_tn, patcher = self._make_audited(threshold=3)
        try:
            for i in range(2):
                mock_tn.ainvoke = AsyncMock(
                    return_value={
                        "messages": [make_tool_result("todo_list", "ok", f"p{i}")]
                    }
                )
                state = make_state(
                    [make_tool_call("todo_list", {}, f"p{i}")], phase_number=1
                )
                await audited(state)
            # Phase flips: streak resets, so one more process call stays quiet
            mock_tn.ainvoke = AsyncMock(
                return_value={"messages": [make_tool_result("todo_list", "ok", "p9")]}
            )
            state = make_state([make_tool_call("todo_list", {}, "p9")], phase_number=2)
            result = await audited(state)
            assert self._act_nudges(result) == []
        finally:
            patcher.stop()
