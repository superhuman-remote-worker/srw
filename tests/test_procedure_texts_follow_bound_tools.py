"""The worker's procedure texts follow the job's loaded tools, nothing else.

Texts about to-dos depend on whether ``todo_complete`` is bound; texts about
phases and plans on whether ``next_phase_todos`` is bound. A job that binds
both (every worker-mode job) gets exactly today's bytes; the expected texts
below are spelled out literally for that reason. A job without them gets the
variant that names only ``job_complete``.

Covered here: the reminder after a reply without a tool call, the kickoff
sentence, the to-do list header and footer, the feedback message, the
deliverable contract block and the phase instruction header. The two tool-node
suffixes are pinned in ``tests/test_stuck_detection.py``; ``job_complete``'s
description and results in ``tests/test_job_complete_without_todo_tool.py``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import HumanMessage

from agent.core.bound_tools import binds, binds_phase_tool, binds_todo_tool
from agent.core.deliverables import format_deliverable_contract_block
from agent.core.workspace import WorkspaceManager
from agent.graph import (
    _todo_reminder_text,
    create_init_strategic_todos_node,
    create_restore_from_feedback_node,
)
from agent.managers.todo import TodoManager
from shared.runtime.core.workspace_injection import create_phase_instruction_message
from shared.runtime.services.guardrails import format_nudge
from tests._fs_backend import FilesystemTestBackend

WORKER_TOOLS = ["read_file", "todo_complete", "next_phase_todos", "job_complete"]
AGENT_TOOLS = ["read_file", "job_complete"]
BRIEF = "# Task Brief\n\n## Description\n\nWrite hello.txt."
ONE_TODO_YAML = "todos:\n  - id: 1\n    content: Do the task in the brief.\n"

TODAYS_KICKOFF = (
    "You are starting in strategic mode. Work through the predefined todos "
    "to understand the task, create a plan, and prepare todos for execution.\n\n"
    "Your task brief is saved to `task_brief.md` in your workspace for reference."
)
TODAYS_FEEDBACK_STEP = (
    "Process the feedback using the strategic todos below, then create "
    "corrective tactical todos to address each feedback item."
)


@pytest.fixture
def workspace(tmp_path) -> WorkspaceManager:
    ws = WorkspaceManager(
        job_id="texts-job", base_path=tmp_path, backend=FilesystemTestBackend(tmp_path)
    )
    ws.initialize()
    ws.write_file("task_brief.md", BRIEF)
    return ws


@pytest.fixture
def config():
    config = MagicMock()
    config.agent_id = "test-agent"
    config.llm.model = "gpt-4o"
    config.extra = {
        "_resolved_instructions": {
            "strategic_todos_initial": ONE_TODO_YAML,
            "strategic_todos_resume": ONE_TODO_YAML,
        }
    }
    config._deployment_dir = None
    return config


class TestBindsHelper:
    @pytest.mark.parametrize("names", [None, [], (), "todo_complete", MagicMock()])
    def test_an_unknown_list_counts_as_bound(self, names):
        assert binds(names, "todo_complete") is True

    def test_a_known_list_decides(self):
        assert binds_todo_tool(WORKER_TOOLS) and binds_phase_tool(WORKER_TOOLS)
        assert not binds_todo_tool(AGENT_TOOLS)
        assert not binds_phase_tool(AGENT_TOOLS)


class TestReminder:
    def _pending(self, *contents):
        mgr = TodoManager(MagicMock())
        for content in contents:
            mgr.add(content)
        return mgr.list_pending()

    @pytest.mark.parametrize("model", ["gpt-4o", "google/gemma-4-31b"])
    def test_unchanged_with_todo_complete(self, model):
        remaining = self._pending("Write the report", "x" * 100)
        todo_lines = "\n".join(
            f"  - {t.id}: {t.content[:80]}{'...' if len(t.content) > 80 else ''}"
            for t in remaining
        )
        expected = (
            format_nudge("todo_action", model=model, todo_id="todo_1") + "\n\n"
            "If you already finished the work for this todo, that's perfectly fine — "
            "invoke `todo_complete` now to record it. You don't need to redo anything.\n\n"
            f"Pending todos ({len(remaining)}):\n"
            f"{todo_lines}"
        )

        assert _todo_reminder_text(remaining, model, True) == expected

    def test_without_todo_complete_points_at_job_complete(self):
        remaining = self._pending("Do the task in the brief.")

        text = _todo_reminder_text(remaining, "gpt-4o", False)

        assert text == (
            format_nudge("todo_action_job_complete", model="gpt-4o")
            + "\n\nPending todos (1):\n  - todo_1: Do the task in the brief."
        )
        assert "todo_complete" not in text


class TestKickoff:
    def _first_message(self, workspace, config, tool_names):
        todo = TodoManager(workspace)
        todo.todo_tool_bound = binds_todo_tool(tool_names)
        node = create_init_strategic_todos_node(
            workspace, todo, config, tool_names=tool_names
        )
        update = node({"job_id": "texts-job"})
        return update["messages"][0].content, todo

    @pytest.mark.parametrize("tool_names", [None, WORKER_TOOLS])
    def test_unchanged_with_next_phase_todos(self, workspace, config, tool_names):
        content, todo = self._first_message(workspace, config, tool_names)

        assert content == "\n\n".join(
            [BRIEF, TODAYS_KICKOFF, todo.format_for_injection()]
        )
        assert "Current Tasks — Phase 0 (Strategic)" in content

    def test_without_next_phase_todos(self, workspace, config):
        content, todo = self._first_message(workspace, config, AGENT_TOOLS)

        assert content == "\n\n".join(
            [
                BRIEF,
                "Do the task in the brief now. When it is done, call `job_complete`.\n\n"
                "Your task brief is saved to `task_brief.md` in your workspace for "
                "reference.",
                todo.format_for_injection(),
            ]
        )
        for word in ("strategic mode", "todo_complete", "next_phase_todos"):
            assert word not in content


class TestTodoListRendering:
    def _manager(self, bound: bool) -> TodoManager:
        mgr = TodoManager(MagicMock(), model_name="gpt-4o")
        mgr.is_strategic_phase = True
        mgr.phase_number = 0
        mgr.todo_tool_bound = bound
        mgr.add("Do the task in the brief.")
        return mgr

    def test_unchanged_with_todo_complete(self):
        assert self._manager(True).format_for_injection() == (
            "Current Tasks — Phase 0 (Strategic)\n\n"
            "Pending:\n"
            "  - [ ] todo_1: Do the task in the brief.\n\n"
            + format_nudge("todo_list_footer", model="gpt-4o")
        )

    def test_the_default_is_bound(self):
        assert TodoManager(MagicMock()).todo_tool_bound is True

    def test_without_todo_complete(self):
        mgr = self._manager(False)

        rendering = mgr.format_for_injection()

        assert rendering == (
            "Current task\n\n"
            "Pending:\n"
            "  - [ ] todo_1: Do the task in the brief.\n\n"
            + format_nudge("todo_list_footer_job_complete", model="gpt-4o")
        )
        assert "todo_complete" not in rendering
        assert "Strategic" not in rendering
        # The restatement after a compaction carries the same rendering.
        assert rendering in mgr.list_restatement([HumanMessage(content="other")])


class TestFeedbackMessage:
    async def _banner(self, workspace, config, tool_names):
        context_mgr = MagicMock()
        context_mgr.ensure_within_limits = AsyncMock(
            side_effect=lambda msgs, *a, **k: msgs
        )
        node = create_restore_from_feedback_node(
            workspace,
            TodoManager(workspace),
            config,
            context_mgr,
            auxiliary_llm=None,
            summarization_prompt="",
            tool_names=tool_names,
        )
        state = {
            "job_id": "texts-job",
            "resume_feedback": "Add a second line.",
            "resume_reason": "Returned by the reviewer.",
            "messages": [HumanMessage(content="old context")],
            "phase_number": 0,
            "iteration": 7,
        }
        with patch("agent.graph.get_resume_strategic_todos", return_value=[]):
            update = await node(state)
        return update, update["messages"][-1].content

    @pytest.mark.asyncio
    @pytest.mark.parametrize("tool_names", [None, WORKER_TOOLS])
    async def test_unchanged_with_next_phase_todos(self, workspace, config, tool_names):
        _, banner = await self._banner(workspace, config, tool_names)

        assert banner.startswith(
            "[FEEDBACK_RESUME] Returned by the reviewer.\n\n"
            "## Feedback\n\nAdd a second line.\n\n"
            "The feedback has been saved to feedback.md for reference. "
            + TODAYS_FEEDBACK_STEP
        )

    @pytest.mark.asyncio
    async def test_without_next_phase_todos(self, workspace, config):
        update, banner = await self._banner(workspace, config, AGENT_TOOLS)

        assert banner.startswith(
            "[FEEDBACK_RESUME] Returned by the reviewer.\n\n"
            "## Feedback\n\nAdd a second line.\n\n"
            "The feedback has been saved to feedback.md for reference. "
            "Take the message above into account, then finish the task and call "
            "`job_complete`."
        )
        assert "tactical" not in banner

    @pytest.mark.asyncio
    async def test_a_resume_starts_a_fresh_budget_and_streak(self, workspace, config):
        update, _ = await self._banner(workspace, config, WORKER_TOOLS)

        assert update["llm_call_baseline"] == 7
        assert update["plain_reply_streak"] == 0


class TestContractBlock:
    SCAFFOLD = "- Scaffold each deliverable file EARLY (phase 1), even as an outline."
    PLAN = '- Keep plan.md\'s "## Deliverables" section mapped to these paths.'

    def test_unchanged_by_default(self):
        block = format_deliverable_contract_block(["output/report.md"])

        assert block == "\n".join(
            [
                "\n\n## Required Deliverables (Contract)",
                "",
                "This job carries a deliverable contract. The seal (`job_complete`) is",
                "validated against the EXISTENCE of these artifacts — a completion that",
                "claims success while any of them is missing will be bounced back to",
                "you with the missing paths listed. A PR publication promise also",
                "requires proof when you report that delivery was not possible; it",
                "ends blocked/undelivered rather than completed if retries are exhausted.",
                "",
                "- `output/report.md`",
                "",
                "Rules:",
                self.SCAFFOLD,
                "- Keep each deliverable current as work progresses; Git-backed workspaces",
                "  commit and push progress automatically.",
                "- Do not create a separate manifest or status file for this contract; the",
                "  platform validates the listed artifacts directly.",
                self.PLAN,
                "- Paths are workspace-relative; `repo/` prefix is accepted either way.",
                "- A `pr:owner/repository` item is satisfied only by a server-recorded,",
                "  forge-verifiable pull request for that exact attached repository.",
            ]
        )
        assert block == format_deliverable_contract_block(
            ["output/report.md"], phased=True
        )

    def test_without_phases_the_two_phase_rules_drop_out(self):
        phased = format_deliverable_contract_block(["output/report.md"])

        block = format_deliverable_contract_block(["output/report.md"], phased=False)

        assert block == phased.replace(self.SCAFFOLD + "\n", "").replace(
            self.PLAN + "\n", ""
        )
        assert "plan.md" not in block
        assert "phase 1" not in block


class TestPhaseInstructionHeader:
    def test_unchanged_by_default(self):
        message = create_phase_instruction_message(
            "skills/strategic-phase/SKILL.md", "BODY", "strategic", "0:strategic"
        )

        assert message.content == (
            "[phase: strategic] Phase instructions "
            "(from skills/strategic-phase/SKILL.md). They apply for the whole "
            "phase.\n\nBODY"
        )

    def test_whole_job(self):
        message = create_phase_instruction_message(
            "skills/agent-mode/SKILL.md",
            "BODY",
            "strategic",
            "0:strategic",
            whole_job=True,
        )

        assert message.content == (
            "Instructions (from skills/agent-mode/SKILL.md). They apply for the "
            "whole job.\n\nBODY"
        )
        # Still the protected phase block, re-seated after a summary.
        default = create_phase_instruction_message(
            "skills/agent-mode/SKILL.md", "BODY", "strategic", "0:strategic"
        )
        assert message.additional_kwargs == default.additional_kwargs
