"""Unit tests for inline knowledge curation via AuxiliaryLLM.

Tests the curate_and_store_knowledge helper, curation config,
inline curation wiring in archive_phase, and CurateKnowledgeTask.
"""

import asyncio
import pytest
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from shared.runtime.core.loader import load_agent_config, resolve_config_path  # noqa: E402
from agent.services.knowledge_auxiliary import curate_and_store_knowledge  # noqa: E402
from shared.runtime.services.auxiliary import (  # noqa: E402
    AuxiliaryLLM,
    CurateKnowledgeTask,
    CurationResult,
)
from agent.tools.context import ToolContext  # noqa: E402
from tests._kb_context import attached_project_bindings

#: The job's project; its knowledge-base connector is attached where a test
#: gives the context bindings.
PROJECT_ID = "1a387b4d-0000-4000-8000-000000000123"


def _load_config(name: str):
    """Helper to load a config by name."""
    path, deploy_dir = resolve_config_path(name)
    return load_agent_config(path, deployment_dir=deploy_dir)


# =========================================================================
# curate_and_store_knowledge helper
# =========================================================================


class TestCurateAndStoreKnowledge:
    """Tests for the curate_and_store_knowledge helper function."""

    @pytest.mark.asyncio
    async def test_returns_none_when_no_knowledge_graph(self):
        """Should return None if tool_context has no knowledge_graph."""
        ctx = ToolContext(workspace_manager=None, config={})
        mock_llm = MagicMock()
        aux = AuxiliaryLLM(llm=mock_llm, timeout=30.0)

        result = await curate_and_store_knowledge(
            auxiliary_llm=aux,
            tool_context=ctx,
            phase_data="Phase 1 archived.",
            workspace_md="# Workspace",
            plan_md="# Plan",
            curation_prompt="You are a curator.",
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_no_project_id(self):
        """Should return None if tool_context has no project_id."""
        ctx = ToolContext(workspace_manager=None, config={})
        ctx.knowledge_graph = MagicMock()
        ctx.knowledge_store = MagicMock()
        # No project_id set

        mock_llm = MagicMock()
        aux = AuxiliaryLLM(llm=mock_llm, timeout=30.0)

        result = await curate_and_store_knowledge(
            auxiliary_llm=aux,
            tool_context=ctx,
            phase_data="Phase 1 archived.",
            workspace_md="# Workspace",
            plan_md="# Plan",
            curation_prompt="You are a curator.",
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_happy_path_calls_agent(self):
        """Should call auxiliary_llm.agent() with CurateKnowledgeTask."""
        ctx = ToolContext(workspace_manager=None, config={})
        mock_kg = MagicMock()
        mock_kg.list_notes.return_value = [
            {"id": "note-1", "title": "Existing note", "type": "decision"}
        ]
        ctx.knowledge_graph = mock_kg
        ctx.knowledge_store = MagicMock()
        ctx.project_id = PROJECT_ID
        ctx.knowledge_bindings = attached_project_bindings(PROJECT_ID, None)

        curation_result = CurationResult(
            notes_created=2, notes_updated=1, summary="Curated 3 notes"
        )

        mock_llm = MagicMock()
        aux = AuxiliaryLLM(llm=mock_llm, timeout=30.0)

        with patch.object(
            aux, "agent", new_callable=AsyncMock, return_value=curation_result
        ):
            with patch(
                "agent.tools.knowledge.knowledge_tools.create_kb_tools",
                return_value=[MagicMock()],
            ):
                result = await curate_and_store_knowledge(
                    auxiliary_llm=aux,
                    tool_context=ctx,
                    phase_data="Phase 1 (tactical) archived.\n- Task: done",
                    workspace_md="# Workspace",
                    plan_md="# Plan",
                    curation_prompt="You are a curator.",
                )

        assert result is not None
        assert result.notes_created == 2
        assert result.notes_updated == 1

    @pytest.mark.asyncio
    async def test_llm_failure_returns_none(self):
        """LLM failure should be non-fatal and return None."""
        ctx = ToolContext(workspace_manager=None, config={})
        ctx.knowledge_graph = MagicMock()
        ctx.knowledge_graph.list_notes.return_value = []
        ctx.knowledge_store = MagicMock()
        ctx.project_id = PROJECT_ID
        ctx.knowledge_bindings = attached_project_bindings(PROJECT_ID, None)

        mock_llm = MagicMock()
        aux = AuxiliaryLLM(llm=mock_llm, timeout=30.0)

        with patch.object(
            aux, "agent", new_callable=AsyncMock, side_effect=RuntimeError("LLM down")
        ):
            with patch(
                "agent.tools.knowledge.knowledge_tools.create_kb_tools",
                return_value=[MagicMock()],
            ):
                result = await curate_and_store_knowledge(
                    auxiliary_llm=aux,
                    tool_context=ctx,
                    phase_data="Phase 1 archived.",
                    workspace_md="",
                    plan_md="",
                    curation_prompt="You are a curator.",
                )

        assert result is None


# =========================================================================
# CurateKnowledgeTask
# =========================================================================


class TestCurateKnowledgeTask:
    """Tests for the CurateKnowledgeTask class."""

    def test_system_prompt_uses_provided_prompt(self):
        task = CurateKnowledgeTask(
            phase_data="Phase 1 archived.",
            workspace_md="# WS",
            plan_md="# Plan",
            existing_notes=[],
            kb_tools=[],
            prompt="You are the knowledge curator for a project.",
        )
        assert task.system_prompt == "You are the knowledge curator for a project."

    def test_build_context_includes_phase_data(self):
        task = CurateKnowledgeTask(
            phase_data="Phase 1 archived. Built auth module.",
            workspace_md="# Workspace",
            plan_md="# Plan",
            existing_notes=["- note-1: Decision (decision)"],
            kb_tools=[],
            prompt="You are a curator.",
        )
        ctx = task.build_context()
        assert "Phase 1 archived" in ctx
        assert "Workspace" in ctx
        assert "Plan" in ctx
        assert "note-1" in ctx

    def test_output_schema_is_curation_result(self):
        task = CurateKnowledgeTask(
            phase_data="",
            workspace_md="",
            plan_md="",
            existing_notes=[],
            kb_tools=[],
            prompt="You are a curator.",
        )
        assert task.output_schema is CurationResult

    def test_get_tools_returns_provided_tools(self):
        mock_tools = [MagicMock(), MagicMock()]
        task = CurateKnowledgeTask(
            phase_data="",
            workspace_md="",
            plan_md="",
            existing_notes=[],
            kb_tools=mock_tools,
            prompt="You are a curator.",
        )
        assert task.get_tools() == mock_tools


# =========================================================================
# Curation config
# =========================================================================


class TestCurationConfig:
    """Tests for curation config in worker_base.yaml and curator expert config."""

    def test_curation_config_defaults(self):
        """Verify worker_base.yaml has the curator section."""
        config = _load_config("defaults")
        assert hasattr(config, "extra")
        curator = config.extra.get("curator", {})
        assert curator.get("enabled") is False

    def test_curator_config_loads(self):
        """Verify the curator expert config loads without errors."""
        config = _load_config("curator")
        assert config.agent_id == "curator"
        assert config.display_name == "Curator"


# =========================================================================
# Inline curation wiring in archive_phase
# =========================================================================


class TestInlineCurationWiring:
    """Tests that inline curation is wired into archive_phase."""

    @pytest.fixture
    def temp_workspace(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def workspace_manager(self, temp_workspace):
        from agent.core.workspace import WorkspaceManager
        from tests._fs_backend import FilesystemTestBackend

        ws = WorkspaceManager(
            job_id="test-job-123",
            base_path=temp_workspace,
            backend=FilesystemTestBackend(temp_workspace),
        )
        ws.initialize()
        return ws

    @pytest.mark.asyncio
    async def test_no_curation_when_no_kb(self, workspace_manager):
        """Verify no curation when tool_context has no knowledge base."""
        from agent.graph import create_archive_phase_node
        from agent.managers import TodoManager, PlanManager
        from agent.core.context import ContextManager

        config = _load_config("defaults")
        config.context_management.compact_on_archive = False
        # Enable curator in config
        config.extra["curator"] = {"enabled": True}

        todo_manager = TodoManager(workspace_manager)
        plan_manager = PlanManager(workspace_manager)
        plan_manager.write("# Plan\n## Phase 1\nTest.\n")

        context_mgr = ContextManager(config)
        mock_llm = MagicMock()

        # ToolContext with NO knowledge base
        ctx = ToolContext(workspace_manager=workspace_manager, config={})

        archive_fn = create_archive_phase_node(
            todo_manager=todo_manager,
            plan_manager=plan_manager,
            config=config,
            context_mgr=context_mgr,
            auxiliary_llm=mock_llm,
            summarization_prompt="Summarize.",
            tool_context=ctx,
            workspace_manager=workspace_manager,
            curation_prompt="You are a curator.",
        )

        state = {
            "job_id": "test-job-123",
            "iteration": 5,
            "phase_number": 1,
            "is_strategic_phase": False,
            "messages": [],
        }

        # Should not raise even with no KB
        result = await archive_fn(state)
        assert "messages" in result

    @pytest.mark.asyncio
    async def test_no_curation_when_disabled(self, workspace_manager):
        """Verify no curation when curator.enabled is False."""
        from agent.graph import create_archive_phase_node
        from agent.managers import TodoManager, PlanManager
        from agent.core.context import ContextManager

        config = _load_config("defaults")
        config.context_management.compact_on_archive = False
        # Curator disabled (default)

        todo_manager = TodoManager(workspace_manager)
        plan_manager = PlanManager(workspace_manager)
        plan_manager.write("# Plan\n## Phase 1\nTest.\n")

        context_mgr = ContextManager(config)
        mock_llm = MagicMock()

        # ToolContext WITH knowledge base but curator disabled
        ctx = ToolContext(workspace_manager=workspace_manager, config={})
        ctx.knowledge_graph = MagicMock()
        ctx.knowledge_store = MagicMock()

        archive_fn = create_archive_phase_node(
            todo_manager=todo_manager,
            plan_manager=plan_manager,
            config=config,
            context_mgr=context_mgr,
            auxiliary_llm=mock_llm,
            summarization_prompt="Summarize.",
            tool_context=ctx,
            workspace_manager=workspace_manager,
            curation_prompt="You are a curator.",
        )

        state = {
            "job_id": "test-job-123",
            "iteration": 5,
            "phase_number": 1,
            "is_strategic_phase": False,
            "messages": [],
        }

        result = await archive_fn(state)
        assert "messages" in result

    @pytest.mark.asyncio
    async def test_curation_triggered_when_enabled(self, workspace_manager):
        """Verify inline curation fires when KB available and curator enabled."""
        from agent.graph import create_archive_phase_node
        from agent.managers import TodoManager, PlanManager
        from agent.core.context import ContextManager

        config = _load_config("defaults")
        config.context_management.compact_on_archive = False
        config.extra["curator"] = {"enabled": True}

        todo_manager = TodoManager(workspace_manager)
        plan_manager = PlanManager(workspace_manager)
        plan_manager.write("# Plan\n## Phase 1\nTactical work.\n")

        # Write workspace.md so read_file doesn't fail
        workspace_manager.write_file("workspace.md", "# Workspace\nTest workspace.")

        # Create and complete a todo
        todo_manager.add("Build feature")
        for t in todo_manager.list_all():
            todo_manager.complete(t.id, notes=["Feature built"])

        context_mgr = ContextManager(config)
        mock_llm = MagicMock()

        # ToolContext WITH knowledge base
        ctx = ToolContext(workspace_manager=workspace_manager, config={})
        ctx.knowledge_graph = MagicMock()
        ctx.knowledge_store = MagicMock()
        ctx.project_id = PROJECT_ID
        ctx.knowledge_bindings = attached_project_bindings(PROJECT_ID, None)

        archive_fn = create_archive_phase_node(
            todo_manager=todo_manager,
            plan_manager=plan_manager,
            config=config,
            context_mgr=context_mgr,
            auxiliary_llm=mock_llm,
            summarization_prompt="Summarize.",
            tool_context=ctx,
            workspace_manager=workspace_manager,
            curation_prompt="You are a curator.",
        )

        state = {
            "job_id": "test-job-123",
            "iteration": 5,
            "phase_number": 1,
            "is_strategic_phase": False,
            "messages": [],
        }

        with patch(
            "agent.services.knowledge_auxiliary.curate_and_store_knowledge",
            new_callable=AsyncMock,
        ) as mock_curate:
            await archive_fn(state)
            # Allow async task to run
            await asyncio.sleep(0.1)

            mock_curate.assert_called_once()
            call_kwargs = mock_curate.call_args
            assert call_kwargs.kwargs["auxiliary_llm"] is mock_llm
            assert call_kwargs.kwargs["tool_context"] is ctx
            assert "Phase 1 (tactical) archived" in call_kwargs.kwargs["phase_data"]
            assert "Build feature" in call_kwargs.kwargs["phase_data"]


# =========================================================================
# Agent no longer has curation_callback
# =========================================================================


class TestAgentNoCurationCallback:
    """Tests that the agent no longer has the curation_callback attribute."""

    def test_agent_has_no_curation_callback(self):
        """Verify curation_callback was removed from UniversalAgent."""
        from agent.agent import UniversalAgent

        config = _load_config("defaults")
        agent = UniversalAgent(config)
        assert not hasattr(agent, "curation_callback")

    def test_agent_has_knowledge_graph_attr(self):
        """Verify UniversalAgent tracks knowledge_graph for cleanup."""
        from agent.agent import UniversalAgent

        config = _load_config("defaults")
        agent = UniversalAgent(config)
        assert hasattr(agent, "_knowledge_graph")
        assert agent._knowledge_graph is None
