"""Tests for KB convergence — TTL re-verification.

Design: knowledge-base/knowledge/features/kb_convergence_ttl_reverification.md (loop_review F13).

Covers:
  - KnowledgeStore TTL: _ttl_for_note_type, upsert TTL assignment (INSERT only),
    decrement_ttl, get_stale_notes, refresh_ttl
  - AssembleKnowledgeTask (the converge aux task) + KnowledgeAssemblyResult schema
  - assemble_and_converge_knowledge runner: stale-queue gate, refresh bookkeeping,
    KB-unavailable guard
  - create_loop_job turns curation ON for loop jobs (competing hooks stay off)
  - create_loop_job splits the prompt: concise description (title) + full loop
    protocol via context["kickoff_message"] (the "Opening Message" channel)
"""

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from agent.services.knowledge_auxiliary import assemble_and_converge_knowledge  # noqa: E402
from shared.runtime.services.auxiliary import (
    AssembleKnowledgeTask,
    KnowledgeAssemblyResult,
)
from shared.runtime.services.knowledge_store import KnowledgeRecord, KnowledgeStore


def _make_store():
    """KnowledgeStore with mocked async db + embedding service."""
    mock_db = AsyncMock()
    mock_embed = AsyncMock()
    mock_embed.embed = AsyncMock(return_value=[0.1, 0.2, 0.3])
    return KnowledgeStore(db=mock_db, embedding_service=mock_embed), mock_db, mock_embed


# =============================================================================
# TTL by note_type
# =============================================================================


class TestTtlForNoteType:
    def test_moving_target_types_have_short_ttl(self):
        assert KnowledgeStore._ttl_for_note_type("state") == 2
        assert KnowledgeStore._ttl_for_note_type("goal") == 3
        assert KnowledgeStore._ttl_for_note_type("plan") == 3
        assert KnowledgeStore._ttl_for_note_type("question") == 3

    def test_durable_types_have_no_ttl(self):
        for t in ("decision", "learning", "code", "retrospective", "source"):
            assert KnowledgeStore._ttl_for_note_type(t) is None

    def test_unknown_type_is_conservative_none(self):
        assert KnowledgeStore._ttl_for_note_type("weird") is None
        assert KnowledgeStore._ttl_for_note_type("") is None

    def test_case_and_whitespace_insensitive(self):
        assert KnowledgeStore._ttl_for_note_type("STATE") == 2
        assert KnowledgeStore._ttl_for_note_type("  Goal ") == 3


# =============================================================================
# upsert_note sets remaining_cycles on INSERT (not on the ON CONFLICT branch)
# =============================================================================


class TestUpsertSetsTtl:
    @pytest.mark.asyncio
    async def test_insert_sets_moving_target_ttl(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchval.side_effect = [None, uuid.uuid4()]  # no existing -> INSERT
        await store.upsert_note(
            note_id="n1",
            project_id=uuid.uuid4(),
            title="T",
            note_type="state",
            content="body",
        )
        insert_call = mock_db.fetchval.call_args_list[1]
        assert "remaining_cycles" in insert_call[0][0]
        # ttl_value is $18, so index 18 (args[0] is the query). Pinned
        # absolutely: this assertion has now been chased twice by a new
        # trailing parameter — priority ($19), then ready ($20) — and a
        # relative pin quietly starts asserting about the newcomer instead.
        assert insert_call[0][18] == 2

    @pytest.mark.asyncio
    async def test_insert_sets_null_ttl_for_durable(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchval.side_effect = [None, uuid.uuid4()]
        await store.upsert_note(
            note_id="n1",
            project_id=uuid.uuid4(),
            title="T",
            note_type="decision",
            content="body",
        )
        assert mock_db.fetchval.call_args_list[1][0][18] is None  # $18 = ttl_value

    @pytest.mark.asyncio
    async def test_on_conflict_branch_preserves_ttl(self):
        # The ON CONFLICT DO UPDATE must NOT touch remaining_cycles, so an
        # existing note's countdown survives a content edit.
        store, mock_db, _ = _make_store()
        mock_db.fetchval.side_effect = [None, uuid.uuid4()]
        await store.upsert_note(
            note_id="n1",
            project_id=uuid.uuid4(),
            title="T",
            note_type="state",
            content="body",
        )
        query = mock_db.fetchval.call_args_list[1][0][0]
        conflict_clause = query.split("ON CONFLICT")[1]
        assert "remaining_cycles" not in conflict_clause


# =============================================================================
# upsert_note persists priority on both branches (project-backlog-pipeline
# task 2, fix round 1 findings 1 + 2) — mutation-tested: deleting the bound
# arg or the SET/VALUES clause on either branch fails these.
# =============================================================================


class TestUpsertNotePriorityBinding:
    @pytest.mark.asyncio
    async def test_insert_branch_binds_priority_at_its_own_slot(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchval.side_effect = [None, uuid.uuid4()]  # no existing -> INSERT
        await store.upsert_note(
            note_id="n1",
            project_id=uuid.uuid4(),
            title="T",
            note_type="decision",
            content="body",
            priority=0,
        )
        insert_call = mock_db.fetchval.call_args_list[1]
        assert "priority" in insert_call[0][0]
        assert insert_call[0][19] == 0  # $19 = priority; $20 = ready (B2)

    @pytest.mark.asyncio
    async def test_metadata_only_branch_binds_priority(self):
        # Finding 1 (fix round 1): a status/metadata-only kb_update (content
        # hash unchanged, so this branch — not the INSERT branch — runs) must
        # not silently discard a priority change.
        store, mock_db, _ = _make_store()
        existing_hash = KnowledgeStore._content_hash("body")
        mock_db.fetchval.side_effect = [existing_hash, uuid.uuid4()]
        await store.upsert_note(
            note_id="n1",
            project_id=uuid.uuid4(),
            title="T",
            note_type="decision",
            content="body",
            priority=2,
        )
        update_call = mock_db.fetchval.call_args_list[1]
        assert "priority" in update_call[0][0]
        assert update_call[0][13] == 2  # $13 = priority; $14 = ready (B2)


# =============================================================================
# decrement_ttl / get_stale_notes / refresh_ttl
# =============================================================================


class TestDecrementTtl:
    @pytest.mark.asyncio
    async def test_decrements_active_ttl_bearing_notes(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = [{"id": uuid.uuid4()}, {"id": uuid.uuid4()}]
        n = await store.decrement_ttl(uuid.uuid4())
        assert n == 2
        query = mock_db.fetch.call_args[0][0]
        assert "remaining_cycles = remaining_cycles - 1" in query
        assert "remaining_cycles IS NOT NULL" in query
        assert "status = 'active'" in query

    @pytest.mark.asyncio
    async def test_returns_zero_when_nothing_decremented(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []
        assert await store.decrement_ttl(uuid.uuid4()) == 0


class TestGetStaleNotes:
    @pytest.mark.asyncio
    async def test_queries_expired_active_notes(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = [
            {
                "note_id": "n1",
                "note_type": "state",
                "title": "S",
                "content": "c",
                "status": "active",
            }
        ]
        notes = await store.get_stale_notes(uuid.uuid4())
        query = mock_db.fetch.call_args[0][0]
        assert "remaining_cycles <= 0" in query
        assert "status = 'active'" in query
        assert len(notes) == 1
        assert isinstance(notes[0], KnowledgeRecord)
        assert notes[0].note_id == "n1"

    @pytest.mark.asyncio
    async def test_empty_when_nothing_stale(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []
        assert await store.get_stale_notes(uuid.uuid4()) == []


class TestRefreshTtl:
    @pytest.mark.asyncio
    async def test_resets_moving_target_to_type_ttl(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = [{"id": uuid.uuid4()}]
        notes = [KnowledgeRecord(note_id="n1", note_type="state")]
        n = await store.refresh_ttl(uuid.uuid4(), notes, current_cycle=5)
        assert n == 1
        args = mock_db.fetch.call_args[0]
        assert "remaining_cycles = $1" in args[0]
        assert args[1] == 2  # state -> 2
        assert args[2] == 5  # current_cycle stamped

    @pytest.mark.asyncio
    async def test_skips_durable_notes(self):
        store, mock_db, _ = _make_store()
        notes = [KnowledgeRecord(note_id="d1", note_type="decision")]
        n = await store.refresh_ttl(uuid.uuid4(), notes)
        assert n == 0
        mock_db.fetch.assert_not_called()  # durable -> no UPDATE issued

    @pytest.mark.asyncio
    async def test_update_filters_on_active_status(self):
        # Survivors are only refreshed if still active — superseded/archived
        # notes are skipped by the WHERE clause.
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []  # row not active -> no RETURNING row
        notes = [KnowledgeRecord(note_id="n1", note_type="goal")]
        n = await store.refresh_ttl(uuid.uuid4(), notes)
        assert n == 0
        assert "status = 'active'" in mock_db.fetch.call_args[0][0]


# =============================================================================
# AssembleKnowledgeTask
# =============================================================================


class TestAssembleKnowledgeTask:
    def test_build_context_includes_stale_and_related(self):
        task = AssembleKnowledgeTask(
            stale_notes=["- n1 [state] Title: body"],
            related_notes=["- n2: Other (decision)"],
            kb_tools=[],
            prompt="P",
        )
        ctx = task.build_context()
        assert "Stale notes to re-verify" in ctx
        assert "n1 [state]" in ctx
        assert "n2: Other" in ctx

    def test_empty_stale_renders_none(self):
        task = AssembleKnowledgeTask(
            stale_notes=[], related_notes=[], kb_tools=[], prompt="P"
        )
        assert "(none)" in task.build_context()

    def test_output_schema_is_assembly_result(self):
        task = AssembleKnowledgeTask(
            stale_notes=[], related_notes=[], kb_tools=[], prompt="P"
        )
        assert task.output_schema is KnowledgeAssemblyResult

    def test_system_prompt_passthrough(self):
        task = AssembleKnowledgeTask(
            stale_notes=[], related_notes=[], kb_tools=[], prompt="SYS"
        )
        assert task.system_prompt == "SYS"

    def test_get_tools_passthrough(self):
        sentinel = [object()]
        task = AssembleKnowledgeTask(
            stale_notes=[], related_notes=[], kb_tools=sentinel, prompt="P"
        )
        assert task.get_tools() is sentinel


# =============================================================================
# assemble_and_converge_knowledge runner
# =============================================================================


def _lock(claimed: bool):
    """A ``try_converge_lock``-shaped async context manager."""
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _cm(_kb_id):
        yield claimed

    return _cm


def _make_tool_context(stale_notes, *, active_total=1000, lock_claimed=True):
    tc = MagicMock()
    tc.project_id = str(uuid.uuid4())
    ks = AsyncMock()
    ks.get_stale_notes = AsyncMock(return_value=stale_notes)
    ks.refresh_ttl = AsyncMock(return_value=len(stale_notes))
    ks.get_summary = AsyncMock(return_value={"active": active_total})
    ks.list_notes = AsyncMock(return_value=[])
    ks.try_converge_lock = _lock(lock_claimed)
    tc.knowledge_store = ks
    kg = MagicMock()
    kg.list_notes = MagicMock(return_value=[])
    tc.knowledge_graph = kg
    return tc, ks, kg


def _make_aux_llm():
    aux = MagicMock()
    aux.agent = AsyncMock(
        return_value=KnowledgeAssemblyResult(
            notes_refreshed=1,
            notes_superseded=0,
            notes_merged=0,
            notes_archived=0,
            summary="ok",
        )
    )
    aux.health = MagicMock()
    return aux


class TestAssembleAndConvergeRunner:
    @pytest.mark.asyncio
    async def test_empty_queue_skips_llm(self, monkeypatch):
        monkeypatch.setattr(
            "agent.tools.knowledge.knowledge_tools.create_kb_tools",
            lambda tc: [],
        )
        tc, ks, kg = _make_tool_context([])
        aux = _make_aux_llm()

        result = await assemble_and_converge_knowledge(aux, tc, "prompt")

        assert result is None
        aux.agent.assert_not_called()  # no aux-LLM call when nothing stale
        ks.refresh_ttl.assert_not_called()

    @pytest.mark.asyncio
    async def test_nonempty_queue_runs_and_refreshes(self, monkeypatch):
        monkeypatch.setattr(
            "agent.tools.knowledge.knowledge_tools.create_kb_tools",
            lambda tc: [],
        )
        stale = [
            KnowledgeRecord(note_id="n1", note_type="state", title="S", content="c")
        ]
        tc, ks, kg = _make_tool_context(stale)
        aux = _make_aux_llm()

        result = await assemble_and_converge_knowledge(
            aux, tc, "prompt", current_cycle=3
        )

        assert result is not None
        aux.agent.assert_awaited_once()
        ks.refresh_ttl.assert_awaited_once()
        # survivors refreshed with the stale list + cycle stamp
        args, kwargs = ks.refresh_ttl.call_args
        assert kwargs.get("current_cycle") == 3
        assert stale in args

    @pytest.mark.asyncio
    async def test_returns_none_when_kb_unavailable(self):
        tc = MagicMock()
        tc.knowledge_store = None
        tc.knowledge_graph = None
        tc.project_id = None
        aux = _make_aux_llm()

        result = await assemble_and_converge_knowledge(aux, tc, "prompt")

        assert result is None
        aux.agent.assert_not_called()

    @pytest.mark.asyncio
    async def test_runs_without_neo4j(self, monkeypatch):
        """kb_gardening G9: the graph is optional; before this the pass
        silently no-op'd on every graph-less pod."""
        monkeypatch.setattr(
            "agent.tools.knowledge.knowledge_tools.create_kb_tools",
            lambda tc: [],
        )
        stale = [KnowledgeRecord(note_id="n1", note_type="state", title="S")]
        tc, ks, _kg = _make_tool_context(stale)
        tc.knowledge_graph = None
        ks.list_notes = AsyncMock(
            return_value=[{"id": "other", "title": "Other", "type": "decision"}]
        )
        aux = _make_aux_llm()

        result = await assemble_and_converge_knowledge(aux, tc, "prompt")

        assert result is not None
        aux.agent.assert_awaited_once()
        task = aux.agent.await_args.args[0]
        assert any("other" in line for line in task.related_notes)
        ks.list_notes.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_skips_when_another_pass_holds_the_lock(self, monkeypatch):
        """kb_gardening G4: one consolidator per KB. Fan-out members and
        replicas that lose the claim skip; the queue waits for the holder."""
        monkeypatch.setattr(
            "agent.tools.knowledge.knowledge_tools.create_kb_tools",
            lambda tc: [],
        )
        stale = [KnowledgeRecord(note_id="n1", note_type="state", title="S")]
        tc, ks, _kg = _make_tool_context(stale, lock_claimed=False)
        aux = _make_aux_llm()

        result = await assemble_and_converge_knowledge(aux, tc, "prompt")

        assert result is None
        aux.agent.assert_not_called()
        ks.get_stale_notes.assert_not_called()
        ks.refresh_ttl.assert_not_called()
        aux.health.record_failure.assert_not_called()

    @pytest.mark.asyncio
    async def test_queue_is_bounded_per_run(self, monkeypatch):
        monkeypatch.setenv("KB_CONVERGE_MAX_NOTES", "7")
        monkeypatch.setattr(
            "agent.tools.knowledge.knowledge_tools.create_kb_tools",
            lambda tc: [],
        )
        stale = [KnowledgeRecord(note_id=f"n{i}", note_type="state") for i in range(3)]
        tc, ks, _kg = _make_tool_context(stale)
        aux = _make_aux_llm()

        await assemble_and_converge_knowledge(aux, tc, "prompt")

        assert ks.get_stale_notes.await_args.kwargs["limit"] == 7

    @pytest.mark.asyncio
    async def test_blast_radius_cap_against_the_active_corpus(self, monkeypatch):
        """Never more than 25 % of the KB's active notes in one run, however
        deep the stale queue is; the rest waits. Survivors of the truncated
        list are the only ones refreshed."""
        monkeypatch.setattr(
            "agent.tools.knowledge.knowledge_tools.create_kb_tools",
            lambda tc: [],
        )
        stale = [KnowledgeRecord(note_id=f"n{i}", note_type="state") for i in range(10)]
        tc, ks, _kg = _make_tool_context(stale, active_total=8)  # cap = 2
        aux = _make_aux_llm()

        await assemble_and_converge_knowledge(aux, tc, "prompt")

        task = aux.agent.await_args.args[0]
        assert len(task.stale_notes) == 2
        refreshed_list = ks.refresh_ttl.await_args.args[1]
        assert [n.note_id for n in refreshed_list] == ["n0", "n1"]

    @pytest.mark.asyncio
    async def test_unknown_corpus_size_does_not_cap(self, monkeypatch):
        monkeypatch.setattr(
            "agent.tools.knowledge.knowledge_tools.create_kb_tools",
            lambda tc: [],
        )
        stale = [KnowledgeRecord(note_id=f"n{i}", note_type="state") for i in range(5)]
        tc, ks, _kg = _make_tool_context(stale)
        ks.get_summary = AsyncMock(side_effect=RuntimeError("no summary"))
        aux = _make_aux_llm()

        await assemble_and_converge_knowledge(aux, tc, "prompt")

        assert len(aux.agent.await_args.args[0].stale_notes) == 5

    @pytest.mark.asyncio
    async def test_agent_failure_is_non_fatal(self, monkeypatch):
        monkeypatch.setattr(
            "agent.tools.knowledge.knowledge_tools.create_kb_tools",
            lambda tc: [],
        )
        stale = [KnowledgeRecord(note_id="n1", note_type="state")]
        tc, ks, kg = _make_tool_context(stale)
        aux = _make_aux_llm()
        aux.agent = AsyncMock(side_effect=RuntimeError("boom"))

        result = await assemble_and_converge_knowledge(aux, tc, "prompt")

        assert result is None  # swallowed, non-fatal
        aux.health.record_failure.assert_called_once()


# =============================================================================
# Loop enables curation
# =============================================================================


def _configure_no_datasource_defaults(db: AsyncMock) -> None:
    """Give loop-job tests an approved owner with no automatic connectors."""
    db.get_user = AsyncMock(return_value={"id": str(uuid.uuid4()), "is_approved": True})
    db.user_is_member_of_projects = AsyncMock(return_value=True)
    db.list_default_datasource_candidates = AsyncMock(return_value=[])
    # No project_workspace_defaults row: the installation decides the tier.
    db.fetchrow = AsyncMock(return_value=None)


class TestLoopCurationEnabled:
    @pytest.mark.asyncio
    async def test_create_loop_job_turns_curation_on(self):
        from orchestrator.services.project_loops import create_loop_job

        db = AsyncMock()
        db.create_job = AsyncMock(return_value={"id": uuid.uuid4()})
        db.list_project_datasources = AsyncMock(return_value=[])
        _configure_no_datasource_defaults(db)
        loop = {
            "id": uuid.uuid4(),
            "project_id": uuid.uuid4(),
            "owner_id": uuid.uuid4(),
        }

        await create_loop_job(db, loop, role="scholar", iteration=1)

        override = db.create_job.call_args.kwargs["config_override"]
        assert override["curator"]["enabled"] is True
        # The competing per-job hooks must stay OFF (they fight the rotation).
        assert override["verification"]["enabled"] is False
        assert override["scholar"]["enabled"] is False


# =============================================================================
# Loop splits the prompt: concise description (title) + kickoff message
# =============================================================================


class TestLoopKickoffSplit:
    @pytest.mark.asyncio
    async def test_create_loop_job_splits_description_and_kickoff(self):
        from orchestrator.services.project_loops import create_loop_job

        db = AsyncMock()
        db.create_job = AsyncMock(return_value={"id": uuid.uuid4()})
        db.list_project_datasources = AsyncMock(return_value=[])
        _configure_no_datasource_defaults(db)
        loop = {
            "id": uuid.uuid4(),
            "project_id": uuid.uuid4(),
            "owner_id": uuid.uuid4(),
            "goal": "Build a Salesforce-like CRM",
        }

        await create_loop_job(db, loop, role="scholar", iteration=3)

        kwargs = db.create_job.call_args.kwargs
        description = kwargs["description"]
        context = kwargs["context"]

        # The description is the concise title — role + iteration + goal — NOT the
        # full multi-paragraph preamble that used to be the cockpit row title.
        assert description.startswith("Loop iter 3 · SCHOLAR")
        assert "Build a Salesforce-like CRM" in description
        assert "You are ONE step" not in description
        assert len(description) < 200

        # The full loop protocol rides the "Opening Message" channel so it reaches
        # the agent's task_brief without polluting the job title.
        kickoff = context["kickoff_message"]
        assert "You are ONE step in a CONTINUOUS" in kickoff
        assert "PROJECT GOAL:" in kickoff
        # Coordination keys stay alongside it.
        assert context["loop_role"] == "scholar"
        assert context["loop_iteration"] == 3


# =============================================================================
# Loop resolves a custom expert NAME in the rotation → expert_id
# =============================================================================


class TestLoopExpertResolution:
    """create_loop_job resolves a role NAME to a DB expert_id so a custom expert
    in role_sequence pulls its own overlay (model/prompts/tools), mirroring the
    automations name-resolution path. Gated on EXPERTS_DB_ENABLED; falls back to
    the bundled config_name when the flag is off or nothing matches."""

    def _db(self):
        db = AsyncMock()
        db.create_job = AsyncMock(return_value={"id": uuid.uuid4()})
        db.list_project_datasources = AsyncMock(return_value=[])
        _configure_no_datasource_defaults(db)
        return db

    @pytest.mark.asyncio
    async def test_resolves_db_expert_name_to_expert_id(self, monkeypatch):
        monkeypatch.setenv("EXPERTS_DB_ENABLED", "true")
        from orchestrator.services.project_loops import create_loop_job

        owner = uuid.uuid4()
        expert_id = uuid.uuid4()
        db = self._db()
        # Owner-owned visible expert named "scholar-fast" → pick_expert_by_name
        # selects it (tier 3) → its UUID is threaded as expert_id.
        db.list_experts_visible = AsyncMock(
            return_value=[
                {
                    "id": expert_id,
                    "name": "scholar-fast",
                    "owner_id": owner,
                    "is_global": False,
                    "project_ids": [],
                    "created_at": "2026-01-01T00:00:00",
                }
            ]
        )
        loop = {"id": uuid.uuid4(), "project_id": uuid.uuid4(), "owner_id": owner}

        await create_loop_job(db, loop, role="scholar-fast", iteration=1)

        kwargs = db.create_job.call_args.kwargs
        assert kwargs["expert_id"] == str(expert_id)
        # A DB expert resolves directly on the worker base; combining it with a
        # bundled role slug would merge two experts.
        assert kwargs["config_name"] == "worker_base"

    @pytest.mark.asyncio
    async def test_unknown_role_passes_no_expert_id(self, monkeypatch):
        monkeypatch.setenv("EXPERTS_DB_ENABLED", "true")
        from orchestrator.services.project_loops import create_loop_job

        db = self._db()
        db.list_experts_visible = AsyncMock(return_value=[])  # no DB match
        loop = {
            "id": uuid.uuid4(),
            "project_id": uuid.uuid4(),
            "owner_id": uuid.uuid4(),
        }

        await create_loop_job(db, loop, role="developer", iteration=1)

        kwargs = db.create_job.call_args.kwargs
        assert kwargs["expert_id"] is None
        assert kwargs["config_name"] == "developer"

    @pytest.mark.asyncio
    async def test_flag_off_skips_resolution(self, monkeypatch):
        monkeypatch.setenv("EXPERTS_DB_ENABLED", "false")
        from orchestrator.services.project_loops import create_loop_job

        db = self._db()
        db.list_experts_visible = AsyncMock(return_value=[])
        loop = {
            "id": uuid.uuid4(),
            "project_id": uuid.uuid4(),
            "owner_id": uuid.uuid4(),
        }

        await create_loop_job(db, loop, role="scholar", iteration=1)

        assert db.create_job.call_args.kwargs["expert_id"] is None
        db.list_experts_visible.assert_not_called()
