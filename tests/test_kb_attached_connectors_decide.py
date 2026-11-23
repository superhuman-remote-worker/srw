"""The attached connectors decide which knowledge bases a runtime has.

A project's own knowledge base has a connector row like any other knowledge
base. A job or session that names its connectors and leaves that row out gets
no knowledge base from its project: nothing bound, no store, no injected
notes, no knowledge tools, no curation, no charter. Before, the agent bound
the project's knowledge base from the project id alone, so unticking the
connector changed nothing
(knowledge-base/knowledge/issues/project_knowledge_base_is_bound_without_its_connector.md).
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from agent.persistent_graph import PersistentLoopCallbacks, _execute_turn
from agent.services.knowledge.bindings import (
    KnowledgeBinding,
    build_knowledge_bindings,
)
from agent.services.knowledge_auxiliary import (
    assemble_and_converge_knowledge,
    curate_and_store_knowledge,
)
from agent.tools.context import ToolContext
from agent.tools.knowledge.knowledge_tools import create_kb_tools
from shared.runtime.core.context_entries import entry_kind, is_context_entry
from shared.runtime.core.loader import OfficerConfig
from shared.runtime.services.auxiliary import AuxiliaryLLM
from tests._kb_context import project_kb_connector

PROJECT = "1a387b4d-0000-4000-8000-00000000000a"
OTHER_PROJECT = "1a387b4d-0000-4000-8000-00000000000b"
EXTERNAL_KB = "1a387b4d-0000-4000-8000-0000000000e1"


def _external_kb(**overrides) -> dict:
    row = {
        "type": "kb",
        "datasource_id": EXTERNAL_KB,
        "name": "Handbook",
        "config": {"root_path": "docs"},
    }
    row.update(overrides)
    return row


def _database() -> dict:
    return {
        "type": "postgresql",
        "datasource_id": str(uuid.uuid4()),
        "name": "Experiment database",
        "config": {},
    }


class TestBindingsFollowTheAttachedConnectors:
    def test_a_project_alone_binds_nothing(self):
        assert build_knowledge_bindings(project_ids=[PROJECT]) == []

    def test_an_explicit_empty_list_binds_nothing(self):
        assert build_knowledge_bindings(project_ids=[PROJECT], datasources=[]) == []

    def test_other_connectors_do_not_bring_the_knowledge_base_along(self):
        """The thesis job shape: one database, the project's folder, nothing
        else. The project's knowledge base is not among them."""
        bindings = build_knowledge_bindings(
            project_ids=[PROJECT], datasources=[_database()]
        )

        assert bindings == []

    def test_the_projects_own_connector_binds_it_writable(self):
        bindings = build_knowledge_bindings(
            project_ids=[PROJECT], datasources=[project_kb_connector(PROJECT)]
        )

        assert len(bindings) == 1
        (binding,) = bindings
        assert binding.kb_id == uuid.UUID(PROJECT)
        assert binding.kind == "native"
        assert binding.alias == "project"
        assert binding.writable is True
        assert binding.root_path == "knowledge"

    def test_an_external_knowledge_base_without_the_projects_own(self):
        """Only what is attached: the external one, read-only, and nothing
        to write to."""
        bindings = build_knowledge_bindings(
            project_ids=[PROJECT], datasources=[_external_kb()]
        )

        assert [str(b.kb_id) for b in bindings] == [EXTERNAL_KB]
        assert bindings[0].kind == "datasource"
        assert not any(b.writable for b in bindings)

    def test_only_the_second_projects_knowledge_base_attached(self):
        """A two-project session with the primary project's knowledge base
        unticked: the second one is read, and nothing is writable, because
        only the primary project is ever the write target."""
        bindings = build_knowledge_bindings(
            project_ids=[PROJECT, OTHER_PROJECT],
            datasources=[project_kb_connector(OTHER_PROJECT)],
        )

        assert [str(b.kb_id) for b in bindings] == [OTHER_PROJECT]
        assert bindings[0].kind == "native"
        assert bindings[0].alias == f"project-{uuid.UUID(OTHER_PROJECT).hex[:8]}"
        assert bindings[0].writable is False

    def test_another_projects_knowledge_base_binds_read_only(self):
        """Attached to work of a different project it is a plain read-only
        knowledge base, keyed by the project its notes are indexed under."""
        bindings = build_knowledge_bindings(
            project_ids=[PROJECT], datasources=[project_kb_connector(OTHER_PROJECT)]
        )

        assert [str(b.kb_id) for b in bindings] == [OTHER_PROJECT]
        assert bindings[0].kind == "datasource"
        assert bindings[0].writable is False

    def test_the_same_row_twice_binds_once(self):
        row = project_kb_connector(PROJECT)

        bindings = build_knowledge_bindings(
            project_ids=[PROJECT], datasources=[row, dict(row)]
        )

        assert len(bindings) == 1
        assert bindings[0].writable is True

    def test_a_malformed_native_marker_binds_nothing_for_the_project(self):
        row = project_kb_connector("not-a-uuid")

        bindings = build_knowledge_bindings(project_ids=[PROJECT], datasources=[row])

        assert all(str(b.kb_id) != PROJECT for b in bindings)
        assert not any(b.writable for b in bindings)


class TestToolContextScope:
    def test_a_project_id_is_not_a_knowledge_base(self):
        ctx = ToolContext(workspace_manager=None, config={})
        ctx.project_ids = [PROJECT]

        assert ctx.kb_ids == []
        assert ctx.writable_kb_id is None

    def test_scope_is_the_bound_knowledge_bases(self):
        ctx = ToolContext(workspace_manager=None, config={})
        ctx.project_ids = [PROJECT]
        ctx.knowledge_bindings = build_knowledge_bindings(
            project_ids=[PROJECT],
            datasources=[project_kb_connector(PROJECT), _external_kb()],
        )

        assert ctx.kb_ids == [PROJECT, EXTERNAL_KB]
        assert ctx.writable_kb_id == PROJECT


def _tools(context):
    with patch(
        "agent.tools.knowledge.knowledge_tools.asyncio.get_running_loop",
        side_effect=RuntimeError,
    ):
        return {tool.name: tool for tool in create_kb_tools(context)}


class TestKnowledgeToolsWithoutAnAttachedKnowledgeBase:
    """Belt and braces: a runtime without bindings gets no store, so these
    tools are not even loaded. Should one be built anyway, it reaches
    nothing."""

    def _context(self):
        context = MagicMock()
        context.knowledge_bindings = []
        context.project_id = PROJECT
        context.project_ids = [PROJECT]
        context.job_id = str(uuid.uuid4())
        context.config = {}
        context.knowledge_graph = MagicMock()
        context.knowledge_store = AsyncMock()
        context.has_git.return_value = False
        context.has_workspace.return_value = False
        return context

    @pytest.mark.parametrize(
        "tool,args",
        [
            ("kb_search", {"query": "anything"}),
            ("kb_list", {}),
            ("kb_read", {"note": "some-note"}),
        ],
    )
    def test_reads_are_refused_and_touch_nothing(self, tool, args):
        context = self._context()

        result = _tools(context)[tool].invoke(args)

        assert result.startswith("Error")
        assert context.knowledge_store.method_calls == []
        assert context.knowledge_graph.method_calls == []

    def test_a_write_is_refused_and_posts_nothing(self):
        context = self._context()

        with patch("agent.tools.knowledge.knowledge_tools._post_vault_file") as post:
            result = _tools(context)["kb_write"].invoke(
                {"title": "X", "content": "body", "type": "learning"}
            )

        assert result.startswith("Error")
        post.assert_not_called()
        assert context.knowledge_graph.method_calls == []


class TestCurationNeedsTheWritableKnowledgeBase:
    def _context(self, bindings):
        ctx = ToolContext(workspace_manager=None, config={})
        ctx.project_ids = [PROJECT]
        ctx.knowledge_graph = MagicMock()
        ctx.knowledge_store = MagicMock()
        ctx.knowledge_bindings = bindings
        return ctx

    def _external_only(self):
        return build_knowledge_bindings(
            project_ids=[PROJECT], datasources=[_external_kb()]
        )

    @pytest.mark.asyncio
    async def test_curation_writes_nothing_into_an_unattached_knowledge_base(self):
        """A job with only an external knowledge base has a store, and its
        project has a knowledge base, but that one is not attached."""
        ctx = self._context(self._external_only())
        llm = MagicMock()

        result = await curate_and_store_knowledge(
            auxiliary_llm=AuxiliaryLLM(llm=llm, timeout=30.0),
            tool_context=ctx,
            phase_data="Phase 1 archived.",
            workspace_md="# Workspace",
            plan_md="# Plan",
            curation_prompt="You are a curator.",
        )

        assert result is None
        assert llm.mock_calls == []
        assert ctx.knowledge_store.method_calls == []
        assert ctx.knowledge_graph.method_calls == []

    @pytest.mark.asyncio
    async def test_convergence_leaves_an_unattached_knowledge_base_alone(self):
        ctx = self._context(self._external_only())
        llm = MagicMock()

        result = await assemble_and_converge_knowledge(
            auxiliary_llm=AuxiliaryLLM(llm=llm, timeout=30.0),
            tool_context=ctx,
            knowledge_assembler_prompt="You are an assembler.",
        )

        assert result is None
        assert llm.mock_calls == []
        assert ctx.knowledge_store.method_calls == []


class TestTheCharterNeedsTheProjectsKnowledgeBase:
    async def _officer_wake(self, bindings):
        config = MagicMock()
        config.extra = {}
        config.llm.model = None
        config.memory.query = None
        config.context_management.max_summary_length = 10_000
        config.officer = OfficerConfig(enabled=True)

        ks = MagicMock()
        ks.embedding_service.model = "test-embed"
        ks.embedding_service.expected_dimensions = 16
        ks.get_charter_note = AsyncMock(
            return_value={"title": "Standing orders", "content": "orders"}
        )
        ks.search_chunks = AsyncMock(return_value=[])
        ks.hybrid_search = AsyncMock(return_value=[])
        ks.get_watermark = AsyncMock(return_value=None)

        tool_context = MagicMock()
        tool_context.knowledge_bindings = bindings
        tool_context.citation_engine = None
        tool_context.subagent_runtime = None
        tool_context.consume_freeze_request.return_value = None

        context_manager = AsyncMock()
        context_manager.should_summarize = MagicMock(return_value=False)
        context_manager.record_main_request = MagicMock()
        context_manager.ensure_within_limits = AsyncMock(
            side_effect=lambda messages, *_args, **_kwargs: messages
        )

        async def _astream(_provider_input, **_kwargs):
            yield AIMessage(content="sleeping")

        llm = MagicMock()
        llm.reasoning = None
        llm.astream = _astream
        callbacks = PersistentLoopCallbacks(
            get_user_input=AsyncMock(),
            on_token=AsyncMock(),
            on_thinking=AsyncMock(),
            on_tool_start=AsyncMock(),
            on_tool_result=AsyncMock(),
            permission_check=AsyncMock(return_value=True),
            on_turn_start=AsyncMock(),
            on_turn_complete=AsyncMock(),
            on_error=AsyncMock(),
            check_interrupt=MagicMock(return_value=False),
            persist_message=AsyncMock(return_value=True),
        )
        messages = [SystemMessage(content="sys"), HumanMessage(content="wake")]
        result = await _execute_turn(
            llm_with_tools=llm,
            tool_map={},
            context_manager=context_manager,
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=config,
            knowledge_store=ks,
            project_id=PROJECT,
            tool_context=tool_context,
        )
        assert result.error is None
        return ks, [entry_kind(m) for m in messages if is_context_entry(m)]

    @pytest.mark.asyncio
    async def test_attached_the_charter_is_read(self):
        bindings = build_knowledge_bindings(
            project_ids=[PROJECT], datasources=[project_kb_connector(PROJECT)]
        )

        ks, kinds = await self._officer_wake(bindings)

        ks.get_charter_note.assert_awaited_once_with(uuid.UUID(PROJECT))
        assert kinds == ["charter"]

    @pytest.mark.asyncio
    async def test_with_only_an_external_knowledge_base_it_is_not_read(self):
        """The session has a store (for the external knowledge base), but
        the charter is a note of the project's own, which is not attached."""
        bindings = build_knowledge_bindings(
            project_ids=[PROJECT], datasources=[_external_kb()]
        )
        assert [b.kind for b in bindings] == ["datasource"]

        ks, kinds = await self._officer_wake(bindings)

        ks.get_charter_note.assert_not_called()
        ks.hybrid_search.assert_not_called()
        assert "charter" not in kinds

    def test_bindings_are_knowledge_bindings(self):
        """The charter gate reads ``is_native`` off real bindings."""
        (binding,) = build_knowledge_bindings(
            project_ids=[PROJECT], datasources=[project_kb_connector(PROJECT)]
        )
        assert isinstance(binding, KnowledgeBinding) and binding.is_native


class TestTheScopeOfReadsByProjectId:
    """The memory manager's note retriever and the charter read a project's
    notes by project id. Their scope is the attached native knowledge bases."""

    def test_only_projects_whose_own_knowledge_base_is_attached(self):
        from agent.core.knowledge_injection import attached_project_knowledge_ids

        ctx = ToolContext(workspace_manager=None, config={})
        ctx.project_ids = [PROJECT, OTHER_PROJECT]
        ctx.knowledge_bindings = build_knowledge_bindings(
            project_ids=[PROJECT, OTHER_PROJECT],
            datasources=[project_kb_connector(OTHER_PROJECT), _external_kb()],
        )

        assert attached_project_knowledge_ids(ctx) == [OTHER_PROJECT]

    def test_none_without_a_binding(self):
        from agent.core.knowledge_injection import attached_project_knowledge_ids

        ctx = ToolContext(workspace_manager=None, config={})
        ctx.project_ids = [PROJECT]

        assert attached_project_knowledge_ids(ctx) == []
        assert attached_project_knowledge_ids(None) == []

    def test_the_note_retriever_has_no_scope_without_an_attached_knowledge_base(self):
        """What both graphs hand the memory manager: with nothing attached,
        ``kb_notes`` has no project to search, even if it were given a store."""
        from agent.core.knowledge_injection import attached_project_knowledge_ids
        from agent.services.memory.plugins.legacy import KbNotesRetriever

        ctx = ToolContext(workspace_manager=None, config={})
        ctx.project_ids = [PROJECT]
        scope = attached_project_knowledge_ids(ctx)
        store = MagicMock()
        store.hybrid_search = AsyncMock(return_value=["a note"])
        retriever = KbNotesRetriever(
            store, project_id=next(iter(scope), None), project_ids=scope
        )

        import asyncio

        found = asyncio.run(retriever.retrieve(MagicMock(query_text="anything")))

        assert found == []
        store.hybrid_search.assert_not_called()
