"""The per-call phase gate in ``create_audited_tool_node`` (U2 WP3).

With one tool binding for every phase the LLM schema no longer filters tools
by phase, so the runtime
gate is the enforcement — and it decides per call: the batch's phase-legal
calls execute through the ToolNode exactly as before (same batch timeout and
watchdog), every phase-illegal call gets an error ToolMessage in its original
position, both sets are audited (the rejection shows in the audit trail), and
rejected calls count in the tool-call budget but never as progress.

Design: knowledge-base/knowledge/features/universal_experts_and_subagents.md
§1.2 — acceptance (c): no phase-illegal tool executes, the gate returns an
error ToolMessage, the audit shows the rejection.
"""

import asyncio
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from langchain_core.messages import AIMessage, SystemMessage, ToolMessage

from shared.runtime.core.loader import LimitsConfig
from agent.graph import create_audited_tool_node
from agent.tools.registry import TOOL_REGISTRY

# The exact texts (config/guardrails/default.yaml, default family).
STRATEGIC_IN_TACTICAL = (
    "Error: 'job_complete' is a strategic-phase tool; you are in the tactical "
    "phase (phase 4). Finish or replan the current todos — it becomes available "
    "at the next strategic phase."
)
TACTICAL_IN_STRATEGIC = (
    "Error: 'request_replan' is a tactical-phase tool; you are in the strategic "
    "phase (phase 3). Stage that work as a todo with `next_phase_todos` — it "
    "becomes available in the next tactical phase."
)
BATCH_NOTE = "Other calls in this batch were executed normally."


@dataclass
class FakeConfig:
    """What ``create_audited_tool_node`` reads."""

    agent_id: str = "gate-agent"
    limits: LimitsConfig = field(default_factory=LimitsConfig)
    llm: Any = field(default_factory=lambda: SimpleNamespace(model=None))


def tool(name: str) -> MagicMock:
    fake = MagicMock()
    fake.name = name
    return fake


def tc(name: str, call_id: str, args: Optional[dict] = None) -> dict:
    return {"name": name, "id": call_id, "args": args or {}}


def state(calls, *, is_strategic: bool, phase_number: int, job_id: str = "job-gate"):
    return {
        "messages": [AIMessage(content="", tool_calls=calls)],
        "job_id": job_id,
        "iteration": 1,
        "is_strategic_phase": is_strategic,
        "phase_number": phase_number,
        "metadata": {},
    }


def result_for(name: str, call_id: str, content: str = "ok") -> ToolMessage:
    return ToolMessage(content=content, tool_call_id=call_id, name=name)


def tool_messages(result) -> list:
    return [m for m in result.get("messages", []) if isinstance(m, ToolMessage)]


def observations(result) -> list:
    return [
        m
        for m in result.get("messages", [])
        if isinstance(m, SystemMessage) and "OBSERVATION" in m.content
    ]


def tool_context() -> MagicMock:
    ctx = MagicMock()
    ctx.consume_freeze_request.return_value = None
    ctx.drain_pending_memories.return_value = []
    ctx._stateless_worker = False
    return ctx


@pytest.fixture
def tool_node():
    """The patched LangGraph ToolNode (tests set ``ainvoke``); the patch is
    live while the test creates its audited node."""
    with patch("agent.graph.ToolNode") as MockToolNode:
        mock_tn = AsyncMock()
        MockToolNode.return_value = mock_tn
        yield mock_tn


# ---------------------------------------------------------------------------
# Split → execute → synthesise → merge
# ---------------------------------------------------------------------------


class TestPerCallGate:
    @pytest.mark.asyncio
    async def test_mixed_batch_executes_legal_calls_and_rejects_illegal_ones_in_place(
        self, tool_node
    ):
        tool_node.ainvoke = AsyncMock(
            return_value={
                "messages": [
                    result_for("read_file", "c1", "file body"),
                    result_for("write_file", "c3", "written"),
                ]
            }
        )
        audited = create_audited_tool_node(
            [tool("read_file"), tool("job_complete"), tool("write_file")],
            FakeConfig(),
        )
        st = state(
            [
                tc("read_file", "c1", {"path": "a"}),
                tc("job_complete", "c2", {"summary": "s"}),
                tc("write_file", "c3", {"path": "b", "content": "x"}),
            ],
            is_strategic=False,
            phase_number=4,
        )
        result = await audited(st)

        # The ToolNode saw the legal calls only — ids intact, order kept ...
        tool_node.ainvoke.assert_awaited_once()
        seen = tool_node.ainvoke.await_args.args[0]
        assert [c["id"] for c in seen["messages"][-1].tool_calls] == ["c1", "c3"]
        # ... on a copy: the state's own AIMessage was not rewritten
        assert [c["id"] for c in st["messages"][-1].tool_calls] == ["c1", "c2", "c3"]

        msgs = tool_messages(result)
        assert [(m.tool_call_id, m.name) for m in msgs] == [
            ("c1", "read_file"),
            ("c2", "job_complete"),
            ("c3", "write_file"),
        ]
        assert msgs[0].content == "file body"
        assert msgs[2].content == "written"
        assert msgs[1].content == f"{STRATEGIC_IN_TACTICAL} {BATCH_NOTE}"

    @pytest.mark.asyncio
    async def test_strategic_tool_alone_in_a_tactical_phase(self, tool_node):
        ctx = tool_context()
        audited = create_audited_tool_node(
            [tool("job_complete")], FakeConfig(), tool_context=ctx
        )
        result = await audited(
            state([tc("job_complete", "c1")], is_strategic=False, phase_number=4)
        )
        (msg,) = tool_messages(result)
        assert msg.content == STRATEGIC_IN_TACTICAL  # no batch note: nothing else ran
        assert (msg.tool_call_id, msg.name) == ("c1", "job_complete")
        tool_node.ainvoke.assert_not_called()
        # Nothing executed → no heartbeat progress marker, no freeze
        ctx.next_graph_progress.assert_not_called()
        assert "should_stop" not in result

    @pytest.mark.asyncio
    async def test_tactical_tool_in_a_strategic_phase_gets_the_mirror(self, tool_node):
        audited = create_audited_tool_node([tool("request_replan")], FakeConfig())
        result = await audited(
            state(
                [tc("request_replan", "c1", {"reason": "r"})],
                is_strategic=True,
                phase_number=3,
            )
        )
        (msg,) = tool_messages(result)
        assert msg.content == TACTICAL_IN_STRATEGIC
        tool_node.ainvoke.assert_not_called()

    @pytest.mark.asyncio
    async def test_every_call_of_an_all_rejected_batch_is_answered(self, tool_node):
        audited = create_audited_tool_node(
            [tool("job_complete"), tool("next_phase_todos")], FakeConfig()
        )
        result = await audited(
            state(
                [tc("job_complete", "c1"), tc("next_phase_todos", "c2")],
                is_strategic=False,
                phase_number=2,
            )
        )
        msgs = tool_messages(result)
        assert [m.tool_call_id for m in msgs] == ["c1", "c2"]
        assert all(BATCH_NOTE not in m.content for m in msgs)
        assert "'next_phase_todos' is a strategic-phase tool" in msgs[1].content
        tool_node.ainvoke.assert_not_called()

    @pytest.mark.asyncio
    async def test_unregistered_tools_are_not_gated(self, tool_node):
        tool_node.ainvoke = AsyncMock(
            return_value={"messages": [result_for("custom_dynamic_tool", "c1", "ran")]}
        )
        audited = create_audited_tool_node([tool("custom_dynamic_tool")], FakeConfig())
        for is_strategic in (True, False):
            result = await audited(
                state(
                    [tc("custom_dynamic_tool", "c1")],
                    is_strategic=is_strategic,
                    phase_number=1,
                )
            )
            assert tool_messages(result)[0].content == "ran"
        assert tool_node.ainvoke.await_count == 2

    def test_the_error_text_never_enumerates_the_tool_surface(self):
        """Models read a tool list as exhaustive (job 1cab4b88, edd06963): the
        rejection names the tool's phase and what advances the job, nothing
        else that exists."""
        for text, own in (
            (STRATEGIC_IN_TACTICAL, "job_complete"),
            (TACTICAL_IN_STRATEGIC, "request_replan"),
        ):
            assert "Tools available" not in text
            named = {name for name in TOOL_REGISTRY if name in text}
            # The rejected tool itself and, in the strategic mirror only, the
            # one tool that stages the work for the next tactical phase.
            assert named <= {own, "next_phase_todos"}, named
        assert "next_phase_todos" not in STRATEGIC_IN_TACTICAL


# ---------------------------------------------------------------------------
# Audit, budget, progress
# ---------------------------------------------------------------------------


class TestAuditAndAccounting:
    @pytest.mark.asyncio
    async def test_both_sets_are_audited_and_the_rejection_is_recorded(self, tool_node):
        tool_node.ainvoke = AsyncMock(
            return_value={"messages": [result_for("read_file", "c1", "body")]}
        )
        auditor = MagicMock()
        auditor.audit_tool_call.side_effect = lambda **kw: f"doc-{kw['call_id']}"
        audited = create_audited_tool_node(
            [tool("read_file"), tool("job_complete")], FakeConfig()
        )
        with patch("agent.graph.get_archiver", return_value=auditor):
            await audited(
                state(
                    [tc("read_file", "c1", {"path": "a"}), tc("job_complete", "c2")],
                    is_strategic=False,
                    phase_number=4,
                )
            )

        audited_calls = [
            (
                c.kwargs["tool_name"],
                c.kwargs["call_id"],
                c.kwargs["phase"],
                c.kwargs["phase_number"],
            )
            for c in auditor.audit_tool_call.call_args_list
        ]
        assert audited_calls == [
            ("read_file", "c1", "tactical", 4),
            ("job_complete", "c2", "tactical", 4),
        ]

        updates = {
            c.kwargs["audit_doc_id"]: c.kwargs
            for c in auditor.update_tool_result.call_args_list
        }
        assert set(updates) == {"doc-c1", "doc-c2"}
        assert updates["doc-c1"]["success"] is True
        assert updates["doc-c1"]["error"] is None
        rejected = updates["doc-c2"]
        assert rejected["success"] is False
        assert rejected["result"] == f"{STRATEGIC_IN_TACTICAL} {BATCH_NOTE}"
        assert rejected["error"] == rejected["result"][:500]

    @pytest.mark.asyncio
    async def test_rejected_calls_count_toward_the_job_budget(self, tool_node):
        tool_node.ainvoke = AsyncMock(
            return_value={"messages": [result_for("read_file", "c1")]}
        )
        cfg = FakeConfig(
            limits=LimitsConfig(max_tool_calls_per_job=2, max_tool_calls_per_phase=0)
        )
        audited = create_audited_tool_node(
            [tool("read_file"), tool("job_complete")], cfg
        )
        first = await audited(
            state(
                [tc("read_file", "c1"), tc("job_complete", "c2")],
                is_strategic=False,
                phase_number=4,
            )
        )
        assert "freeze_data" not in first  # 2 of 2 used: one executed, one rejected

        tool_node.ainvoke = AsyncMock(
            return_value={"messages": [result_for("read_file", "c3")]}
        )
        second = await audited(
            state([tc("read_file", "c3")], is_strategic=False, phase_number=4)
        )
        assert second["should_stop"] is True
        assert second["freeze_data"]["freeze_type"] == "budget_exceeded"
        assert second["freeze_data"]["tool_calls_this_job"] == 3

    @pytest.mark.asyncio
    async def test_rejected_calls_are_calls_without_progress(self, tool_node):
        cfg = FakeConfig(limits=LimitsConfig(progress_stall_threshold=2))
        audited = create_audited_tool_node(
            [tool("job_complete"), tool("write_file")], cfg
        )
        first = await audited(
            state([tc("job_complete", "c1")], is_strategic=False, phase_number=4)
        )
        assert not observations(first)
        second = await audited(
            state([tc("job_complete", "c2")], is_strategic=False, phase_number=4)
        )
        # two rejected calls = two calls without progress → the stall nudge
        assert observations(second)
        tool_node.ainvoke.assert_not_called()

    @pytest.mark.asyncio
    async def test_only_the_executed_progress_call_resets_the_stall_counter(
        self, tool_node
    ):
        cfg = FakeConfig(limits=LimitsConfig(progress_stall_threshold=2))
        audited = create_audited_tool_node(
            [tool("job_complete"), tool("write_file")], cfg
        )
        tool_node.ainvoke = AsyncMock(
            return_value={"messages": [result_for("write_file", "c1", "written")]}
        )
        mixed = await audited(
            state(
                [
                    tc("write_file", "c1", {"path": "p", "content": "c"}),
                    tc("job_complete", "c2"),
                ],
                is_strategic=False,
                phase_number=4,
            )
        )
        assert not observations(mixed)
        third = await audited(
            state([tc("job_complete", "c3")], is_strategic=False, phase_number=4)
        )
        assert not observations(third)  # one call since the write: below 2
        fourth = await audited(
            state([tc("job_complete", "c4")], is_strategic=False, phase_number=4)
        )
        assert observations(fourth)  # the rejected calls did not count as progress


# ---------------------------------------------------------------------------
# Same batch semantics as before for the legal subset
# ---------------------------------------------------------------------------


class TestBatchSemantics:
    @pytest.mark.asyncio
    async def test_timeout_on_the_legal_subset_keeps_the_rejection_and_the_watchdog(
        self, tool_node
    ):
        cfg = FakeConfig(limits=LimitsConfig(tool_category_timeouts={"default": 1}))

        async def slow(_):
            await asyncio.sleep(1.25)
            return {"messages": [result_for("read_file", "c1")]}

        tool_node.ainvoke = slow
        backend = MagicMock()
        ctx = tool_context()
        ctx.workspace_manager = MagicMock(backend=backend)
        audited = create_audited_tool_node(
            [tool("read_file"), tool("job_complete")], cfg, tool_context=ctx
        )
        result = await audited(
            state(
                [tc("read_file", "c1", {"path": "a"}), tc("job_complete", "c2")],
                is_strategic=False,
                phase_number=4,
            )
        )
        msgs = tool_messages(result)
        assert [m.tool_call_id for m in msgs] == ["c1", "c2"]
        assert "timed out" in msgs[0].content.lower()
        # Nothing "executed normally" this time — the note stays off
        assert msgs[1].content == STRATEGIC_IN_TACTICAL
        assert backend.method_calls == [
            call.shell_reset_after_timeout(),
            call.disconnect(),
            call.connect(),
        ]

    @pytest.mark.asyncio
    async def test_a_single_call_batch_takes_the_same_path(self, tool_node):
        """parallel_tool_calls=false families issue one call per batch — the
        split degenerates to that one call, nothing else changes."""
        tool_node.ainvoke = AsyncMock(
            return_value={"messages": [result_for("read_file", "c1", "body")]}
        )
        audited = create_audited_tool_node(
            [tool("read_file"), tool("job_complete")], FakeConfig()
        )
        ok = await audited(
            state(
                [tc("read_file", "c1", {"path": "a"})],
                is_strategic=False,
                phase_number=4,
            )
        )
        assert tool_messages(ok)[0].content == "body"
        assert tool_node.ainvoke.await_args.args[0] is not None
        rejected = await audited(
            state([tc("job_complete", "c2")], is_strategic=False, phase_number=4)
        )
        assert tool_messages(rejected)[0].content == STRATEGIC_IN_TACTICAL
        assert tool_node.ainvoke.await_count == 1


# ---------------------------------------------------------------------------
# Delegation batches (U3 B.6): delegation-only, no watchdog, per-call rejection
# ---------------------------------------------------------------------------

DELEGATION_COBATCH = (
    "Not executed: `read_file` was batched with delegate_agent; re-issue it in "
    "the next turn — delegation runs in a turn of its own"
)


class TestDelegationBatch:
    @pytest.mark.asyncio
    async def test_a_slow_child_outlives_the_old_delegation_timeout(self, tool_node):
        """The old 600 s ``tool_category_timeouts.delegation`` case is gone: a
        delegation batch runs without ``wait_for`` — the child's own budgets
        bound it — and never trips the SSH-wedge reconnect."""
        cfg = FakeConfig(
            limits=LimitsConfig(tool_category_timeouts={"default": 1, "delegation": 1})
        )

        async def slow(_):
            await asyncio.sleep(1.3)
            return {"messages": [result_for("delegate_agent", "c1", "child report")]}

        tool_node.ainvoke = slow
        backend = MagicMock()
        ctx = tool_context()
        ctx.workspace_manager = MagicMock(backend=backend)
        audited = create_audited_tool_node(
            [tool("delegate_agent"), tool("read_file")], cfg, tool_context=ctx
        )
        result = await audited(
            state(
                [tc("delegate_agent", "c1", {"prompt": "x", "subagent_type": "e"})],
                is_strategic=False,
                phase_number=4,
            )
        )
        (msg,) = tool_messages(result)
        assert msg.content == "child report"
        assert backend.method_calls == []

    @pytest.mark.asyncio
    async def test_a_mixed_batch_rejects_the_co_batched_call_and_runs_the_child(
        self, tool_node
    ):
        tool_node.ainvoke = AsyncMock(
            return_value={
                "messages": [result_for("delegate_agent", "c1", "child report")]
            }
        )
        ctx = tool_context()
        audited = create_audited_tool_node(
            [tool("delegate_agent"), tool("read_file")], FakeConfig(), tool_context=ctx
        )
        result = await audited(
            state(
                [
                    tc("delegate_agent", "c1", {"prompt": "x", "subagent_type": "e"}),
                    tc("read_file", "c2", {"path": "a"}),
                ],
                is_strategic=False,
                phase_number=4,
            )
        )
        msgs = tool_messages(result)
        assert [m.tool_call_id for m in msgs] == ["c1", "c2"]
        assert msgs[0].content == "child report"
        assert msgs[1].content == DELEGATION_COBATCH
        # The ToolNode saw only the delegation call.
        executed = tool_node.ainvoke.await_args.args[0]["messages"][-1]
        assert [c["id"] for c in executed.tool_calls] == ["c1"]

    @pytest.mark.asyncio
    async def test_a_shell_only_batch_still_times_out_and_reconnects(self, tool_node):
        cfg = FakeConfig(limits=LimitsConfig(tool_category_timeouts={"default": 1}))

        async def slow(_):
            await asyncio.sleep(1.25)
            return {"messages": [result_for("run_command", "c1")]}

        tool_node.ainvoke = slow
        backend = MagicMock()
        ctx = tool_context()
        ctx.workspace_manager = MagicMock(backend=backend)
        audited = create_audited_tool_node(
            [tool("run_command"), tool("delegate_agent")], cfg, tool_context=ctx
        )
        result = await audited(
            state(
                [tc("run_command", "c1", {"command": "sleep 5"})],
                is_strategic=False,
                phase_number=4,
            )
        )
        (msg,) = tool_messages(result)
        assert "timed out" in msg.content.lower()
        assert backend.method_calls == [
            call.shell_reset_after_timeout(),
            call.disconnect(),
            call.connect(),
        ]

    @pytest.mark.asyncio
    async def test_a_phase_illegal_call_keeps_the_phase_text(self, tool_node):
        """Phase gate first, delegation rule second: the strategic tool in a
        tactical delegation batch gets the phase rejection, not the co-batch
        text, and the child still runs."""
        tool_node.ainvoke = AsyncMock(
            return_value={
                "messages": [result_for("delegate_agent", "c1", "child report")]
            }
        )
        audited = create_audited_tool_node(
            [tool("delegate_agent"), tool("job_complete"), tool("read_file")],
            FakeConfig(),
            tool_context=tool_context(),
        )
        result = await audited(
            state(
                [
                    tc("delegate_agent", "c1", {"prompt": "x", "subagent_type": "e"}),
                    tc("job_complete", "c2"),
                    tc("read_file", "c3", {"path": "a"}),
                ],
                is_strategic=False,
                phase_number=4,
            )
        )
        by_id = {m.tool_call_id: m.content for m in tool_messages(result)}
        assert by_id["c1"] == "child report"
        assert by_id["c2"].startswith(STRATEGIC_IN_TACTICAL)
        assert BATCH_NOTE in by_id["c2"]
        assert by_id["c3"] == DELEGATION_COBATCH
        executed = tool_node.ainvoke.await_args.args[0]["messages"][-1]
        assert [c["id"] for c in executed.tool_calls] == ["c1"]

    @pytest.mark.asyncio
    async def test_the_node_stamps_fork_source_metadata_and_batch_size(self, tool_node):
        tool_node.ainvoke = AsyncMock(
            return_value={
                "messages": [
                    result_for("delegate_agent", "c1", "one"),
                    result_for("delegate_agent", "c2", "two"),
                ]
            }
        )
        ctx = tool_context()
        audited = create_audited_tool_node(
            [tool("delegate_agent")], FakeConfig(), tool_context=ctx
        )
        st = state(
            [
                tc("delegate_agent", "c1", {"prompt": "x", "subagent_type": "e"}),
                tc("delegate_agent", "c2", {"prompt": "y", "subagent_type": "e"}),
            ],
            is_strategic=True,
            phase_number=3,
        )
        st["metadata"] = {"job_id": "job-gate", "config_name": "developer"}
        result = await audited(st)
        assert [m.content for m in tool_messages(result)] == ["one", "two"]
        assert ctx._fork_source is st["messages"]
        assert ctx._parent_audit_metadata == {
            "job_id": "job-gate",
            "config_name": "developer",
        }
        ctx.subagent_runtime.begin_batch.assert_called_once_with(2)

    @pytest.mark.asyncio
    async def test_a_batch_without_delegation_is_not_stamped(self, tool_node):
        tool_node.ainvoke = AsyncMock(
            return_value={"messages": [result_for("read_file", "c1", "body")]}
        )
        ctx = tool_context()
        audited = create_audited_tool_node(
            [tool("read_file"), tool("delegate_agent")], FakeConfig(), tool_context=ctx
        )
        result = await audited(
            state(
                [tc("read_file", "c1", {"path": "a"})],
                is_strategic=False,
                phase_number=4,
            )
        )
        assert tool_messages(result)[0].content == "body"
        ctx.subagent_runtime.begin_batch.assert_not_called()

    @pytest.mark.asyncio
    async def test_the_rejection_names_the_delegation_tool_in_the_batch(
        self, tool_node, monkeypatch
    ):
        """The rule keys on the registry CATEGORY, and the text names the tool
        that was in the batch — not the literal ``delegate_agent``. Pinned
        with a stand-in registry entry (delegate_agent is the only delegation
        tool since U3 WP4)."""
        from agent.tools.registry import TOOL_REGISTRY

        monkeypatch.setitem(
            TOOL_REGISTRY,
            "delegate_probe",
            {"category": "delegation", "phases": ["strategic", "tactical"]},
        )
        tool_node.ainvoke = AsyncMock(
            return_value={"messages": [result_for("delegate_probe", "c2", "reader")]}
        )
        audited = create_audited_tool_node(
            [tool("delegate_probe"), tool("read_file")],
            FakeConfig(),
            tool_context=tool_context(),
        )
        result = await audited(
            state(
                [
                    tc("read_file", "c1", {"path": "a"}),
                    tc("delegate_probe", "c2", {"prompt": "x"}),
                ],
                is_strategic=False,
                phase_number=4,
            )
        )
        by_id = {m.tool_call_id: m.content for m in tool_messages(result)}
        assert by_id["c2"] == "reader"
        assert by_id["c1"] == (
            "Not executed: `read_file` was batched with delegate_probe; re-issue "
            "it in the next turn — delegation runs in a turn of its own"
        )


class TestStrategicTools:
    """``phase_settings.strategic_tools`` opens named tactical tools in the
    strategic phase for one expert (the phone desk answers a caller from its
    datasources before it ever plans)."""

    @pytest.mark.asyncio
    async def test_a_listed_tactical_tool_runs_in_the_strategic_phase(self, tool_node):
        tool_node.ainvoke = AsyncMock(
            return_value={"messages": [result_for("request_replan", "c1")]}
        )
        config = FakeConfig()
        config.phase_settings = SimpleNamespace(strategic_tools=["request_replan"])
        audited = create_audited_tool_node([tool("request_replan")], config)
        result = await audited(
            state([tc("request_replan", "c1")], is_strategic=True, phase_number=3)
        )
        assert [m.content for m in tool_messages(result)] == ["ok"]

    @pytest.mark.asyncio
    async def test_unlisted_tools_keep_their_phase(self, tool_node):
        tool_node.ainvoke = AsyncMock(return_value={"messages": []})
        config = FakeConfig()
        config.phase_settings = SimpleNamespace(strategic_tools=["sql_query"])
        audited = create_audited_tool_node([tool("request_replan")], config)
        result = await audited(
            state([tc("request_replan", "c1")], is_strategic=True, phase_number=3)
        )
        assert [m.content for m in tool_messages(result)] == [TACTICAL_IN_STRATEGIC]

    def test_a_listed_tool_carries_no_phase_prefix(self):
        from agent.tools.description_manager import apply_phase_description_prefixes

        replan = MagicMock()
        replan.name = "request_replan"
        replan.description = "Ask for a replan."
        plain = apply_phase_description_prefixes(
            [replan], both_phases=["request_replan"]
        )
        assert plain[0].description == "Ask for a replan."
        prefixed = apply_phase_description_prefixes([replan])
        assert prefixed[0].description.startswith("[tactical-phase tool] ")
