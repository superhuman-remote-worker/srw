"""S7 — charter + officer KB note types (knowledge-base/knowledge/features/centurion.md §5).

Covers:
  1. Note-type vocabulary lockstep: NOTE_TYPES / reindexer / vector migration
     0015 stay in agreement (the reindexer silently rewrites unknown types to
     'learning' — drift here is silent data corruption).
  2. Charter write authority: workers can never create or edit 'charter'
     notes (trust boundary — recon workers ingest untrusted content);
     one ACTIVE charter per project.
  3. Sole-store honesty: on lite sessions (no Neo4j, no git) a failed
     pgvector write fails the tool instead of claiming "Created".
  4. Charter fetch (KnowledgeStore.get_charter_note) — project_id-keyed,
     path-agnostic, active-only.
  5. Charter injection: dedicated pair, first in the injection zone,
     excluded from summarization; gate strict on officer.enabled/conference.
  6. Config plumbing: officer.conference parses in the loader and is admitted
     by the session override sanitizer.
"""

import re
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from agent.core.knowledge_injection import (
    CHARTER_TOOL_CALL_ID_PREFIX,
    create_charter_injection_messages,
)
from shared.runtime.core.loader import OfficerConfig, _parse_officer_config
from shared.runtime.core.workspace_injection import is_workspace_injection_message
from agent.persistent_graph import (
    _charter_injection_enabled,
    _inject_context_pairs,
)
from shared.runtime.services.knowledge_graph import NOTE_TYPES
from agent.services.knowledge.bindings import KnowledgeBinding
from shared.runtime.services.knowledge_store import KB_TTL_BY_NOTE_TYPE, KnowledgeStore
from shared.runtime_actor import (
    SENSITIVE_KNOWLEDGE_HUMAN_ROLE_POLICY,
    RuntimeActorContext,
    RuntimeAuthorizationResult,
)
from agent.tools.knowledge.knowledge_tools import create_kb_tools
from orchestrator.services import session_create_overrides

_MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "migrations"
    / "vector"
    / "0015_kb_officer_note_types.sql"
)


# =============================================================================
# 1. Vocabulary lockstep
# =============================================================================


class TestNoteTypeLockstep:
    def test_officer_types_in_agent_vocabulary(self):
        assert "charter" in NOTE_TYPES
        assert "report" in NOTE_TYPES

    def test_reindexer_vocabulary_matches_agent(self):
        from orchestrator.services.kb_reindex import VALID_NOTE_TYPES

        assert VALID_NOTE_TYPES == set(NOTE_TYPES)

    def test_migration_covers_full_vocabulary(self):
        sql = _MIGRATION.read_text()
        quoted = set(re.findall(r"'([a-z_]+)'", sql))
        for note_type in NOTE_TYPES:
            assert note_type in quoted, f"migration 0015 missing '{note_type}'"

    def test_officer_types_are_durable(self):
        # Explicit None (no TTL), not merely absent — the charter is infinite
        # by definition; reports are retired by gardening, not a clock.
        assert KB_TTL_BY_NOTE_TYPE["charter"] is None
        assert KB_TTL_BY_NOTE_TYPE["report"] is None


# =============================================================================
# kb-tool harness (kg-less = the session shape; real asyncio so AsyncMock
# returns flow through _run_async's asyncio.run fallback)
# =============================================================================


def _session_context(
    thread_id="11111111-2222-3333-4444-555555555555",
    *,
    caller_kind="human",
    project_role="owner",
):
    ctx = MagicMock()
    ctx.project_id = str(uuid.uuid4())
    ctx.project_ids = [ctx.project_id]
    ctx.job_id = ctx.project_id
    ctx.config = {"current_phase": None}
    ctx.knowledge_graph = None
    ctx.knowledge_store = AsyncMock()
    actor = RuntimeActorContext(
        caller_kind=caller_kind,
        project_id=ctx.project_id,
        project_role=project_role,
        thread_id=thread_id,
        officer_incarnation=0 if caller_kind == "officer" else None,
        user_id=str(uuid.uuid4()),
    )
    ctx.runtime_actor = actor
    ctx.knowledge_bindings = [
        KnowledgeBinding(
            kb_id=uuid.UUID(ctx.project_id),
            alias="project",
            name="Project Knowledge",
            kind="native",
            writable=True,
            runtime_actor=actor,
        )
    ]
    ctx._thread_id = thread_id
    ctx.has_git = MagicMock(return_value=False)
    return ctx


def _worker_context():
    ctx = _session_context(thread_id=None, caller_kind="worker", project_role=None)
    ctx.job_id = str(uuid.uuid4())
    return ctx


def _authorize_from_test_actor(ctx, project_id, action):
    actor = ctx.runtime_actor
    allowed = bool(
        actor.project_id == project_id
        and (
            actor.caller_kind == "officer"
            or (
                actor.caller_kind in {"human", "conference"}
                and SENSITIVE_KNOWLEDGE_HUMAN_ROLE_POLICY.get(
                    actor.project_role or "", False
                )
            )
        )
    )
    return RuntimeAuthorizationResult(
        authorized=allowed,
        code="authorized" if allowed else "project_role_denied",
        action=action,
        actor=actor.audit_payload(),
        message="allowed by test PEP" if allowed else "role is denied by policy",
    )


@pytest.fixture(autouse=True)
def _runtime_actor_pep():
    """Stub the PEP and the server-side commit; yields the commit mock."""
    with (
        patch(
            "agent.tools.knowledge.knowledge_tools._request_runtime_actor_authorization",
            side_effect=_authorize_from_test_actor,
        ),
        patch(
            "agent.tools.knowledge.knowledge_tools._post_vault_file",
            return_value={
                "status": "committed",
                "canonical_state": "canonical",
                "projection_state": "pending",
                "retry_state": "none",
                "indexed": True,
                "index_reason": None,
            },
        ) as vault_commit,
    ):
        yield vault_commit


@pytest.fixture
def canonical_write(_runtime_actor_pep):
    """The single server-side write kb_write makes, as an assertable mock.

    kb_write no longer writes the ``knowledge_index`` row itself (Slice A —
    the orchestrator owns it), so ``upsert_note`` is no longer evidence that
    a note was written, or refused. This is.
    """
    return _runtime_actor_pep


def _tool(ctx, name):
    for t in create_kb_tools(ctx):
        if t.name == name:
            return t
    raise KeyError(name)


# =============================================================================
# 2. Charter write authority
# =============================================================================


class TestCharterWriteAuthority:
    def test_worker_cannot_create_charter(self, canonical_write):
        ctx = _worker_context()
        result = _tool(ctx, "kb_write").func(
            title="Charter", type="charter", content="orders"
        )
        assert "Authorization denied" in result
        assert "No changes were made" in result
        canonical_write.assert_not_called()

    def test_worker_can_file_reports(self, canonical_write):
        ctx = _worker_context()
        ctx.knowledge_store.get_note_by_slug.return_value = None
        result = _tool(ctx, "kb_write").func(
            title="Recon findings", type="report", content="findings"
        )
        assert "Error" not in result
        canonical_write.assert_called_once()
        # _post_vault_file(project_id, slug, content, job_id) — the note type
        # is frontmatter in the committed markdown, not a column the agent sets.
        _, slug, content, _ = canonical_write.call_args.args
        assert slug == "recon-findings"
        assert "type: report" in content
        # The seam every other index-reporting test patches out. The stub here
        # is at `_post_vault_file` — the HTTP boundary — so the REAL
        # `_materialize_note` runs, and this proves the endpoint's `indexed`
        # field survives that passthrough into the string the model reads.
        # Patch `_materialize_note` instead and the whole hop goes untested.
        assert "indexed=yes" in result

    def test_session_creates_charter_when_none_exists(self, canonical_write):
        ctx = _session_context()
        ctx.knowledge_store.get_charter_note.return_value = None
        ctx.knowledge_store.get_note_by_slug.return_value = None
        result = _tool(ctx, "kb_write").func(
            title="Century charter", type="charter", content="orders"
        )
        assert "Error" not in result
        canonical_write.assert_called_once()

    def test_second_active_charter_refused(self, canonical_write):
        ctx = _session_context()
        ctx.knowledge_store.get_charter_note.return_value = {
            "id": "century-charter",
            "type": "charter",
            "status": "active",
        }
        result = _tool(ctx, "kb_write").func(
            title="Another charter", type="charter", content="orders"
        )
        assert "already has an active charter" in result
        assert "century-charter" in result
        canonical_write.assert_not_called()

    def test_charter_lookup_failure_refuses_write(self, canonical_write):
        # Fail-closed: if we cannot verify uniqueness we do not write.
        ctx = _session_context()
        ctx.knowledge_store.get_charter_note.side_effect = RuntimeError("db down")
        result = _tool(ctx, "kb_write").func(
            title="Charter", type="charter", content="orders"
        )
        assert "could not verify" in result
        canonical_write.assert_not_called()

    def test_worker_cannot_update_charter_kgless(self, canonical_write):
        ctx = _worker_context()
        ctx.knowledge_store.get_note_by_slug.return_value = {
            "id": "century-charter",
            "type": "charter",
            "status": "active",
            "content": "orders",
            "tags": [],
        }
        result = _tool(ctx, "kb_update").func(
            note="century-charter", content="my orders now"
        )
        assert "Authorization denied" in result
        assert "No changes were made" in result
        canonical_write.assert_not_called()

    def test_session_can_update_charter_kgless(self, canonical_write):
        ctx = _session_context()
        ctx.knowledge_store.get_note_by_slug.return_value = {
            "id": "century-charter",
            "type": "charter",
            "status": "active",
            "content": "orders",
            "tags": [],
            "keywords": [],
            "priority": 1,
        }
        result = _tool(ctx, "kb_update").func(
            note="century-charter", content="posture: demo Friday"
        )
        assert "cannot be written" not in result
        canonical_write.assert_called_once()

    def test_worker_cannot_update_charter_graph_path(self):
        ctx = _worker_context()
        ctx.knowledge_graph = MagicMock()
        ctx.knowledge_graph.read_note.return_value = {
            "id": "century-charter",
            "type": "charter",
        }
        result = _tool(ctx, "kb_update").func(
            note="century-charter", content="my orders now"
        )
        assert "Authorization denied" in result
        assert "No changes were made" in result
        ctx.knowledge_graph.update_note.assert_not_called()

    def test_graph_type_read_failure_refuses_before_any_update(self):
        ctx = _worker_context()
        ctx.knowledge_graph = MagicMock()
        ctx.knowledge_graph.read_note.side_effect = RuntimeError("neo4j read down")
        result = _tool(ctx, "kb_update").func(
            note="century-charter", content="my orders now"
        )
        assert "could not verify the note type" in result
        assert "No changes were made" in result
        ctx.knowledge_graph.update_note.assert_not_called()

    def test_worker_updates_ordinary_notes_freely(self, canonical_write):
        ctx = _worker_context()
        ctx.knowledge_store.get_note_by_slug.return_value = {
            "id": "some-learning",
            "type": "learning",
            "status": "active",
            "content": "old",
            "tags": [],
            "keywords": [],
            "priority": 1,
        }
        result = _tool(ctx, "kb_update").func(note="some-learning", content="new")
        assert "cannot be written" not in result
        canonical_write.assert_called_once()

    @pytest.mark.parametrize(
        ("caller_kind", "project_role", "allowed"),
        [
            ("worker", None, False),
            ("human", "viewer", False),
            ("human", "editor", False),
            ("human", "owner", True),
            ("human", "admin", True),
            ("officer", "owner", True),
            ("conference", "viewer", False),
            ("conference", "editor", False),
            ("conference", "owner", True),
            ("conference", "admin", True),
        ],
    )
    def test_charter_human_role_matrix(
        self, caller_kind, project_role, allowed, canonical_write
    ):
        ctx = _session_context(
            thread_id=None if caller_kind == "worker" else "thread-1",
            caller_kind=caller_kind,
            project_role=project_role,
        )
        ctx.knowledge_store.get_charter_note.return_value = None
        ctx.knowledge_store.get_note_by_slug.return_value = None
        result = _tool(ctx, "kb_write").func(
            title="Century charter", type="charter", content="orders"
        )
        if allowed:
            canonical_write.assert_called_once()
        else:
            canonical_write.assert_not_called()
            assert "No changes were made" in result

    def test_denied_charter_update_has_zero_side_effects(self):
        ctx = _session_context(caller_kind="human", project_role="editor")
        existing = {
            "id": "century-charter",
            "type": "charter",
            "status": "active",
            "content": "orders",
            "tags": [],
        }
        ctx.knowledge_graph = MagicMock()
        ctx.knowledge_graph.read_note.return_value = existing
        with patch(
            "agent.tools.knowledge.knowledge_tools._post_vault_file"
        ) as canonical_write:
            result = _tool(ctx, "kb_update").func(
                note="century-charter", content="editor's new orders"
            )
        assert "No changes were made" in result
        ctx.knowledge_graph.update_note.assert_not_called()
        canonical_write.assert_not_called()


# =============================================================================
# 3. Sole-store honesty (risk 11)
# =============================================================================


class TestSoleStoreWriteHonesty:
    """A session with no Neo4j and no git has exactly one place its notes can
    land, so a write that did not land must not be reported as one. Since
    Slice A that place is the canonical commit (the orchestrator writes the
    searchable row from it), and this is the failure the tool must not hide.
    """

    def test_canonical_failure_fails_tool_on_lite_session(self, canonical_write):
        ctx = _session_context()  # kg None + has_git False = sole store
        ctx.knowledge_store.get_note_by_slug.return_value = None
        canonical_write.return_value = {
            "status": "failed",
            "reason": "commit-refused",
            "retry_state": "pending",
        }
        result = _tool(ctx, "kb_write").func(
            title="State note", type="state", content="the century stands"
        )
        assert result.startswith("Error: canonical knowledge write")
        assert "commit-refused" in result

    def test_canonical_failure_fails_closed_even_with_git(self, canonical_write):
        ctx = _session_context()
        ctx.has_git = MagicMock(return_value=True)
        # Dual-write path needs a workspace write to succeed.
        ctx.workspace_manager.write_file = MagicMock()
        ctx.knowledge_store.get_note_by_slug.return_value = None
        canonical_write.return_value = {
            "status": "failed",
            "reason": "commit-refused",
            "retry_state": "pending",
        }
        result = _tool(ctx, "kb_write").func(
            title="State note", type="state", content="the century stands"
        )
        assert result.startswith("Error: canonical knowledge write")


# =============================================================================
# 4. get_charter_note
# =============================================================================


class TestGetCharterNote:
    @pytest.mark.asyncio
    async def test_query_shape_and_row_mapping(self):
        db = MagicMock()
        captured: dict = {}

        async def fetchrow(sql, *params):
            captured["sql"] = sql
            captured["params"] = params
            return {
                "note_id": "century-charter",
                "title": "Century charter",
                "note_type": "charter",
                "status": "active",
                "content": "orders",
                "job_id": None,
                "created_at": None,
                "modified_at": None,
            }

        db.fetchrow = fetchrow
        store = KnowledgeStore(db, embedding_service=MagicMock())
        pid = uuid.uuid4()
        note = await store.get_charter_note(pid)

        assert note["id"] == "century-charter"
        assert note["type"] == "charter"
        sql = captured["sql"]
        assert "note_type = 'charter'" in sql
        assert "status = 'active'" in sql
        assert "project_id" in sql
        # Lite sessions write pathless rows — the charter fetch must not
        # filter on path like get_note_by_slug does.
        assert "path" not in sql
        assert captured["params"] == (pid,)

    @pytest.mark.asyncio
    async def test_no_charter_returns_none(self):
        db = MagicMock()

        async def fetchrow(sql, *params):
            return None

        db.fetchrow = fetchrow
        store = KnowledgeStore(db, embedding_service=MagicMock())
        assert await store.get_charter_note(uuid.uuid4()) is None


# =============================================================================
# 5. Charter injection
# =============================================================================


class TestCharterInjection:
    def test_pair_shape_and_summarization_exclusion(self):
        ai, tool = create_charter_injection_messages("standing orders")
        assert isinstance(ai, AIMessage) and isinstance(tool, ToolMessage)
        assert ai.tool_calls[0]["id"].startswith(CHARTER_TOOL_CALL_ID_PREFIX)
        assert tool.tool_call_id == ai.tool_calls[0]["id"]
        assert tool.content == "standing orders"
        # Excluded from summarization — re-injected fresh each turn, so
        # compaction can never strip the standing orders.
        assert is_workspace_injection_message(ai)
        assert is_workspace_injection_message(tool)

    def test_charter_lands_first_in_injection_zone(self):
        prepared = [HumanMessage(content="wake")]
        n = _inject_context_pairs(
            prepared,
            manager_injection=[],
            memory_block="memories",
            knowledge_block="notes",
            citation_feedback_block="",
            charter_block="orders",
        )
        assert n == 6
        # Zone starts after the trailing human message.
        first_ai = prepared[1]
        assert isinstance(first_ai, AIMessage)
        assert first_ai.tool_calls[0]["id"].startswith(CHARTER_TOOL_CALL_ID_PREFIX)
        assert prepared[2].content == "orders"

    def test_no_charter_block_no_pair(self):
        prepared = [HumanMessage(content="wake")]
        n = _inject_context_pairs(
            prepared,
            manager_injection=[],
            memory_block="",
            knowledge_block="",
            citation_feedback_block="",
        )
        assert n == 0

    def test_gate_officer_enabled(self):
        cfg = MagicMock()
        cfg.officer = OfficerConfig(enabled=True)
        assert _charter_injection_enabled(cfg) is True

    def test_gate_conference(self):
        cfg = MagicMock()
        cfg.officer = OfficerConfig(enabled=False, conference=True)
        assert _charter_injection_enabled(cfg) is True

    def test_gate_off_by_default(self):
        cfg = MagicMock()
        cfg.officer = OfficerConfig()
        assert _charter_injection_enabled(cfg) is False

    def test_gate_strict_against_mock_configs(self):
        # MagicMock attrs are truthy; the gate demands `is True` (S1 lesson).
        cfg = MagicMock()
        assert _charter_injection_enabled(cfg) is False

    def test_gate_no_officer(self):
        cfg = MagicMock()
        cfg.officer = None
        assert _charter_injection_enabled(cfg) is False


# =============================================================================
# 6. Config plumbing
# =============================================================================


class TestConferenceConfig:
    def test_loader_parses_conference(self):
        cfg = _parse_officer_config({"officer": {"conference": True}})
        assert cfg.conference is True
        assert cfg.enabled is False

    def test_loader_default_off(self):
        cfg = _parse_officer_config({"officer": {"enabled": True}})
        assert cfg.conference is False

    def test_sanitizer_admits_conference(self):
        cleaned = session_create_overrides.validated_session_officer_override(
            {"officer": {"conference": True}}
        )
        assert cleaned == {"conference": True}

    @pytest.mark.parametrize(
        "field,value",
        [("auto_pull", False), ("worker_spend_ceiling_daily", 12.5)],
    )
    def test_generic_sanitizer_rejects_post_owned_authority(self, field, value):
        from fastapi import HTTPException

        with pytest.raises(HTTPException, match="Unknown officer override"):
            session_create_overrides.validated_session_officer_override(
                {"officer": {field: value}}
            )

    def test_generic_sanitizer_rejects_slot_spend_authority(self):
        from fastapi import HTTPException

        with pytest.raises(HTTPException, match="owned by the Officer Post"):
            session_create_overrides.validated_session_officer_override(
                {
                    "officer": {
                        "slots": {"line": {"count": 1, "spend_ceiling_daily": 4.5}}
                    }
                }
            )

    def test_private_post_snapshot_materializes_safe_exact_authority(self):
        import orchestrator.main

        cleaned = session_create_overrides.validated_post_owned_officer_create_fragment(
            {
                "officer": {
                    "auto_pull": True,
                    "worker_spend_ceiling_daily": 12.5,
                    "slots": {"line": {"count": 1, "spend_ceiling_daily": 4.5}},
                }
            },
            validated_officer_post_patch=orchestrator.main._validated_officer_post_patch,
        )
        assert cleaned == {
            "auto_pull": True,
            "worker_spend_ceiling_daily": 12.5,
            "slots": {"line": {"count": 1, "spend_ceiling_daily": 4.5}},
        }

    @pytest.mark.parametrize("value", [1, "true", None])
    def test_sanitizer_rejects_non_boolean_auto_pull(self, value):
        from fastapi import HTTPException

        with pytest.raises(HTTPException):
            session_create_overrides.validated_session_officer_override(
                {"officer": {"auto_pull": value}}
            )

    @pytest.mark.parametrize("value", [0, -1, True, "nan", "inf"])
    def test_sanitizer_rejects_invalid_century_ceiling(self, value):
        from fastapi import HTTPException

        with pytest.raises(HTTPException):
            session_create_overrides.validated_session_officer_override(
                {"officer": {"worker_spend_ceiling_daily": value}}
            )

    def test_sanitizer_still_rejects_unknown_keys(self):
        from fastapi import HTTPException

        with pytest.raises(HTTPException):
            session_create_overrides.validated_session_officer_override(
                {"officer": {"conferance": True}}
            )
