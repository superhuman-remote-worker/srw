"""Project backlog / idea pipeline — spec:
knowledge-base/knowledge/superpowers/specs/2026-07-26-project-backlog-pipeline-design.md
"""

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.routers.project_loops import ProjectLoopsDependencies
from orchestrator.services.project_loop_spawn import (
    ProjectLoopDependencies,
    spawn_loop_job,
)


# =============================================================================
# Helpers (Task 3: priority on the agent tool surface)
# =============================================================================


def _kb_context(job_id=None):
    """A ToolContext with Neo4j disabled (kg=None) and a mocked pgvector store.

    Mirrors the proven kgless fixture in
    tests/test_knowledge_tools.py::_make_context_no_kg. The task-3 brief's own
    ``_kb_context()`` called ``create_knowledge_tools`` imported from
    ``knowledge_tools.py`` — that name only lives in
    ``src/tools/knowledge/__init__.py`` (a thin wrapper over
    ``create_kb_tools``), so the brief's import raises ImportError. No
    existing test constructs these tools that way; ``create_kb_tools`` (used
    below) is the proven path, already exercised with a bare ``MagicMock`` in
    tests/test_tool_vocabularies.py::test_kb_tool_params_are_constrained.
    """
    ctx = MagicMock()
    ctx.project_id = str(uuid.uuid4())
    ctx.project_ids = [ctx.project_id]
    ctx.job_id = job_id or "aaaaaaaa-0000-0000-0000-000000000001"
    ctx.config = {"current_phase": 2}
    ctx.knowledge_graph = None
    ctx.knowledge_store = AsyncMock()
    return ctx


def _kb_context_with_kg(job_id=None):
    """A ToolContext with Neo4j present (kg is a MagicMock), store present."""
    ctx = _kb_context(job_id=job_id)
    ctx.knowledge_graph = MagicMock()
    return ctx


def _make_tools(ctx):
    """Create kb tools with a mocked context, by name.

    Patches ``asyncio`` only during construction (forcing the
    ``asyncio.run()`` fallback at invocation time) — same pattern as
    tests/test_knowledge_tools.py::_make_tools.
    """
    from agent.tools.knowledge.knowledge_tools import create_kb_tools

    with patch("agent.tools.knowledge.knowledge_tools.asyncio") as mock_asyncio:
        mock_asyncio.get_running_loop.side_effect = RuntimeError("no loop")
        tools = create_kb_tools(ctx)
    return {t.name: t for t in tools}


def _capture_materialize():
    """Patch the server-side materialisation seam and record the rendered notes.

    The OKF file is a Gitea commit the orchestrator makes now, not a workspace
    write (knowledge-base/knowledge/features/knowledge_base_repo_separation.md §7 step 4), so the
    frontmatter these tests assert on is in the POST body.
    Returns ``(patcher, rendered)`` with ``rendered`` keyed by note slug.
    """
    rendered: dict = {}

    def _fake(
        project_id,
        slug,
        content,
        job_id,
        retrieval_messages=None,
        expected_blob_sha=None,
        mode=None,
    ):
        rendered[slug] = content
        return {"status": "committed", "path": f"knowledge/{slug}.md"}

    patcher = patch(
        "agent.tools.knowledge.knowledge_tools._post_vault_file", side_effect=_fake
    )
    return patcher, rendered


@pytest.fixture(autouse=True)
def _no_materialization_http():
    """Keep these tests off the network — kb_write/kb_update POST now."""
    with patch(
        "agent.tools.knowledge.knowledge_tools._post_vault_file",
        return_value={"status": "committed", "path": "knowledge/test.md"},
    ):
        yield


class TestPriorityRoundTrip:
    def test_render_emits_priority_word(self):
        from agent.tools.knowledge.knowledge_tools import _render_note_md

        md = _render_note_md(
            {"id": "feature-x", "type": "feature", "content": "body", "priority": 0}
        )
        assert "priority: high" in md

    def test_render_omits_priority_when_absent(self):
        """Existing notes must not gain noise in their frontmatter."""
        from agent.tools.knowledge.knowledge_tools import _render_note_md

        md = _render_note_md({"id": "d-1", "type": "decision", "content": "body"})
        assert "priority:" not in md

    def test_reindex_parses_priority_word_to_rank(self):
        # note_fields(path, fm, body) is the real signature (kb_reindex.py:157)
        # — it takes already-parsed frontmatter as a dict, not raw markdown.
        from orchestrator.services.kb_reindex import note_fields

        parsed = note_fields(
            "feature-x.md",
            {"id": "feature-x", "type": "feature", "priority": "high"},
            "# T\nbody",
        )
        assert parsed["priority"] == 0

    def test_reindex_defaults_unknown_priority_to_normal(self):
        """Frontmatter is human-editable; a typo must not fail the row."""
        from orchestrator.services.kb_reindex import note_fields

        parsed = note_fields(
            "feature-x.md",
            {"id": "feature-x", "type": "feature", "priority": "URGENT!!"},
            "# T\nb",
        )
        assert parsed["priority"] == 1


# =============================================================================
# Task 3: priority on kb_write / kb_update / kb_list
# =============================================================================


class TestKbToolPrioritySchema:
    """Step 1: agents must be able to see and set priority on both tools.

    Schema-based (``args_schema.model_fields``) rather than closure-based
    (``inspect.signature(tool.func)``): the task instructions call this out
    as the more robust option, since it doesn't depend on ``.func`` being
    reachable. ``.func`` *is* reachable in this LangChain version (see
    tests/test_knowledge_tools.py::_invoke_unvalidated), so either would have
    worked; the schema form was chosen for consistency with
    test_tool_vocabularies.py, which already asserts against ``args_schema``
    for every other closed-vocabulary kb_* parameter.
    """

    def test_kb_write_accepts_priority(self):
        """Agents must be able to file a ticket at a priority in one call."""
        tools = _make_tools(_kb_context())
        assert "priority" in tools["kb_write"].args_schema.model_fields

    def test_kb_update_accepts_priority(self):
        tools = _make_tools(_kb_context())
        assert "priority" in tools["kb_update"].args_schema.model_fields

    def test_kb_write_priority_defaults_to_normal(self):
        tools = _make_tools(_kb_context())
        field = tools["kb_write"].args_schema.model_fields["priority"]
        assert field.default == "normal"

    def test_kb_update_priority_defaults_to_none(self):
        """None must mean "leave unchanged" — kb_update cannot default to a
        priority word, or every status-only edit would reset the rank."""
        tools = _make_tools(_kb_context())
        field = tools["kb_update"].args_schema.model_fields["priority"]
        assert field.default is None


class TestKbWritePriorityPersistence:
    """Proves priority reaches the persisted note, not just the tool schema
    (task-3 test-quality bar: "a signature/schema assertion alone does not
    prove the value reaches storage").

    Since Slice A the agent makes exactly one write — the canonical OKF note —
    and the orchestrator projects ``knowledge_index`` from it. The rank still
    runs through ``PRIORITY_RANKS`` into ``new_note["priority"]``; what changed
    is that the only place it is observable is the frontmatter word
    ``_render_note_md`` maps it back to.
    """

    def test_high_priority_reaches_the_note(self):
        ctx = _kb_context()
        ctx.knowledge_store.get_note_by_slug = AsyncMock(return_value=None)
        tools = _make_tools(ctx)
        patcher, rendered = _capture_materialize()
        with patcher:
            tools["kb_write"].invoke(
                {
                    "title": "Add dark mode",
                    "type": "feature",
                    "content": "body",
                    "priority": "high",
                }
            )
        assert "priority: high" in rendered["add-dark-mode"]

    def test_low_priority_reaches_the_note(self):
        ctx = _kb_context()
        ctx.knowledge_store.get_note_by_slug = AsyncMock(return_value=None)
        tools = _make_tools(ctx)
        patcher, rendered = _capture_materialize()
        with patcher:
            tools["kb_write"].invoke(
                {
                    "title": "Someday idea",
                    "type": "idea",
                    "content": "body",
                    "priority": "low",
                }
            )
        assert "priority: low" in rendered["someday-idea"]

    def test_omitted_priority_defaults_to_normal(self):
        ctx = _kb_context()
        ctx.knowledge_store.get_note_by_slug = AsyncMock(return_value=None)
        tools = _make_tools(ctx)
        patcher, rendered = _capture_materialize()
        with patcher:
            tools["kb_write"].invoke(
                {"title": "Some issue", "type": "issue", "content": "body"}
            )
        assert "priority: normal" in rendered["some-issue"]


class TestKbWritePriorityNonTicketUnaffected:
    """Global constraint: an existing decision/learning note keeps identical
    frontmatter — priority is scoped to feature/issue/idea tickets."""

    def test_decision_note_gets_no_priority_frontmatter_line(self):
        ctx = _kb_context()
        ctx.knowledge_store.get_note_by_slug = AsyncMock(return_value=None)
        tools = _make_tools(ctx)
        patcher, rendered = _capture_materialize()
        with patcher:
            tools["kb_write"].invoke(
                {"title": "Chose JWT", "type": "decision", "content": "body"}
            )
        assert "priority:" not in rendered["chose-jwt"]

    def test_feature_note_gets_priority_frontmatter_line(self):
        ctx = _kb_context()
        ctx.knowledge_store.get_note_by_slug = AsyncMock(return_value=None)
        tools = _make_tools(ctx)
        patcher, rendered = _capture_materialize()
        with patcher:
            tools["kb_write"].invoke(
                {
                    "title": "Add dark mode",
                    "type": "feature",
                    "content": "body",
                    "priority": "high",
                }
            )
        assert "priority: high" in rendered["add-dark-mode"]


def _existing_ticket(**over):
    """A get_note_by_slug-shaped dict for a feature/issue/idea ticket."""
    base = {
        "id": "n1",
        "title": "T",
        "type": "feature",
        "status": "active",
        "content": "body",
        "confidence": None,
        "tags": [],
        "keywords": [],
        "job_id": None,
        "phase": None,
        "created": None,
        "modified": None,
        "priority": 0,  # existing: high
    }
    base.update(over)
    return base


class TestKbUpdatePriorityPreservation:
    """kb_update(priority=None) must never silently reset an existing
    ticket's priority to normal (Global constraint). This was a dormant hole:
    fe8dd707 (task 2) fixed the storage layer's binding but nothing called it
    with a real value yet — this wires the caller up.
    """

    def test_omitted_priority_preserves_existing_high(self):
        ctx = _kb_context()
        ctx.knowledge_store.get_note_by_slug = AsyncMock(
            return_value=_existing_ticket(priority=0)
        )
        tools = _make_tools(ctx)
        patcher, rendered = _capture_materialize()
        with patcher:
            result = tools["kb_update"].invoke({"note": "n1", "status": "resolved"})
        assert "priority: high" in rendered["n1"]
        # The status change itself still goes through — this isn't a no-op.
        assert "status → resolved" in result

    def test_explicit_priority_overrides_existing(self):
        ctx = _kb_context()
        ctx.knowledge_store.get_note_by_slug = AsyncMock(
            return_value=_existing_ticket(priority=0)
        )
        tools = _make_tools(ctx)
        patcher, rendered = _capture_materialize()
        with patcher:
            tools["kb_update"].invoke({"note": "n1", "priority": "low"})
        assert "priority: low" in rendered["n1"]


class TestKbUpdatePriorityPreservationWithNeo4j:
    """The kg-enabled path must also preserve priority on omission. Neo4j
    itself has no priority property (Tasks 1-2 only added it to the pgvector
    row), so pgvector via ks.get_note_by_slug is the only place the prior
    value can come from — without this lookback, any kb_update that omits
    priority on a Neo4j-backed ticket would reset it to normal.
    """

    def test_omitted_priority_preserves_existing_high_via_pgvector_lookback(self):
        ctx = _kb_context_with_kg()
        ctx.knowledge_graph.update_note.return_value = True
        ctx.knowledge_graph.read_note.return_value = {
            "type": "feature",
            "title": "Add dark mode",
            "content": "body",
            "status": "active",
        }
        ctx.knowledge_store.get_note_by_slug = AsyncMock(
            return_value={"priority": 0}  # pgvector's last-known value: high
        )
        tools = _make_tools(ctx)
        patcher, rendered = _capture_materialize()
        with patcher:
            tools["kb_update"].invoke({"note": "add-dark-mode", "status": "resolved"})
        assert "priority: high" in rendered["add-dark-mode"]

    def test_lookback_failure_rejects_before_canonical_mutation(self):
        """Fix round 1, Finding 1 repro: kg-enabled, get_note_by_slug raises,
        existing ticket is priority=high, kb_update omits priority.

        Before the fix this seeded DEFAULT_PRIORITY_RANK (normal) ahead of
        the lookback and the except swallowed the raise, so ks.upsert_note
        was called with priority=1 and the rewritten OKF file claimed
        "priority: normal" -- silently clobbering "high". A failed lookback
        must instead leave priority as None (the "unknown, don't touch"
        sentinel COALESCE'd against the real row in knowledge_store.py), so
        neither the DB row nor the frontmatter ever assert a guessed value.
        """
        ctx = _kb_context_with_kg()
        ctx.knowledge_graph.update_note.return_value = True
        ctx.knowledge_graph.read_note.return_value = {
            "type": "feature",
            "title": "Add dark mode",
            "content": "body",
            "status": "active",
        }
        ctx.knowledge_store.get_note_by_slug = AsyncMock(
            side_effect=Exception("pgvector hiccup")
        )
        tools = _make_tools(ctx)
        patcher, rendered = _capture_materialize()
        with patcher:
            result = tools["kb_update"].invoke(
                {"note": "add-dark-mode", "status": "resolved"}
            )

        assert result.startswith("Error: could not read")
        assert rendered == {}
        ctx.knowledge_graph.update_note.assert_not_called()

    def test_lookback_failure_is_returned_not_swallowed(self):
        ctx = _kb_context_with_kg()
        ctx.knowledge_graph.update_note.return_value = True
        ctx.knowledge_graph.read_note.return_value = {
            "type": "feature",
            "title": "Add dark mode",
            "content": "body",
            "status": "active",
        }
        ctx.knowledge_store.get_note_by_slug = AsyncMock(
            side_effect=Exception("pgvector hiccup")
        )
        tools = _make_tools(ctx)
        result = tools["kb_update"].invoke(
            {"note": "add-dark-mode", "status": "resolved"}
        )
        assert "READY/priority state" in result


class TestKbListPriorityDisplay:
    """Step 5 (corrected): priority is visible for backlog tickets, and the
    line is byte-identical to the pre-task-3 format for every other type."""

    def test_ticket_row_shows_priority_word(self):
        ctx = _kb_context()
        ctx.knowledge_store.list_notes = AsyncMock(
            return_value=[
                {
                    "id": "add-dark-mode",
                    "title": "Add dark mode",
                    "type": "feature",
                    "status": "active",
                    "confidence": None,
                    "priority": 0,
                }
            ]
        )
        tools = _make_tools(ctx)
        result = tools["kb_list"].invoke({})
        lines = [line for line in result.splitlines() if "add-dark-mode" in line]
        assert len(lines) == 1
        assert lines[0] == (
            "● **add-dark-mode** — Add dark mode (feature [priority: high])"
        )

    def test_non_ticket_row_is_byte_identical_to_pre_task3_format(self):
        ctx = _kb_context()
        ctx.knowledge_store.list_notes = AsyncMock(
            return_value=[
                {
                    "id": "chose-jwt",
                    "title": "Chose JWT",
                    "type": "decision",
                    "status": "active",
                    "confidence": "high",
                }
            ]
        )
        tools = _make_tools(ctx)
        result = tools["kb_list"].invoke({})
        lines = [line for line in result.splitlines() if "chose-jwt" in line]
        assert len(lines) == 1
        assert lines[0] == "● **chose-jwt** — Chose JWT (decision [high])"
        assert "priority" not in lines[0]


# =============================================================================
# Task 4: the pool query and its renderer
# =============================================================================


def _row(note_id, note_type="feature", title="T", priority=1):
    return {
        "note_id": note_id,
        "note_type": note_type,
        "title": title,
        "priority": priority,
    }


class TestRenderBacklogBlock:
    def test_counts_line_comes_first_and_breaks_down_by_priority(self):
        """The cap hides the tail; the counts line is what tells the reader a
        tail exists at all. It must lead the block."""
        from orchestrator.services.project_backlog import render_backlog_block

        rows = [_row("a", priority=0), _row("b", priority=1)]
        counts = {0: 12, 1: 15, 2: 7}
        block = render_backlog_block(rows, counts, limit=20)

        first = block.splitlines()[0]
        assert first == (
            "PROJECT BACKLOG — 34 open: 12 high, 15 normal, 7 low (showing top 20)"
        )

    def test_in_progress_line_precedes_the_list(self):
        from orchestrator.services.project_backlog import render_backlog_block

        block = render_backlog_block(
            [_row("a")],
            {1: 1},
            in_progress={
                "note_id": "issue-deploy",
                "title": "Deploy docs",
                "priority": 0,
                "note_type": "issue",
            },
        )
        lines = block.splitlines()
        assert lines[1].startswith("IN PROGRESS: [high] issue-deploy — Deploy docs")
        assert lines[2].lstrip().startswith("[normal]")

    def test_no_in_progress_renders_none(self):
        from orchestrator.services.project_backlog import render_backlog_block

        block = render_backlog_block([_row("a")], {1: 1})
        assert "IN PROGRESS: (none)" in block

    def test_claimed_tickets_are_marked_with_their_job(self):
        """B2: a pool read by agents that can't see claim state gets re-picked.
        Claim state stays derived — the caller resolves it, this only renders."""
        from orchestrator.services.project_backlog import render_backlog_block

        block = render_backlog_block(
            [_row("issue-login"), _row("feature-dark")],
            {1: 2},
            claims={"issue-login": "4f2a91c8-1111-2222-3333-444444444444"},
        )
        lines = block.splitlines()
        assert "issue-login — T · claimed by job 4f2a91c8" in lines[2]
        assert "claimed by" not in lines[3]

    def test_unclaimed_rendering_is_byte_identical_without_claims(self):
        # Passing no claims must not perturb the block every existing pin reads.
        from orchestrator.services.project_backlog import render_backlog_block

        rows, counts = [_row("a"), _row("b", priority=0)], {0: 1, 1: 1}
        assert render_backlog_block(rows, counts) == render_backlog_block(
            rows, counts, claims={}
        )

    def test_in_progress_without_priority_omits_the_tag(self):
        """Fix round 1, Finding 2: the caller (main.py's _spawn_loop_job)
        cannot honestly assert a priority for the campaign's in-progress
        initiative — it never read one. Asserting a guessed value (the old
        hardcoded `priority: 1`) would make a genuinely-high ticket render
        `[normal]`. The honest renderer omits the `[...]` tag entirely when
        the caller didn't supply a priority, rather than defaulting to one."""
        from orchestrator.services.project_backlog import render_backlog_block

        block = render_backlog_block(
            [_row("a")],
            {1: 1},
            in_progress={"note_id": "issue-underway", "title": "Underway thing"},
        )
        lines = block.splitlines()
        assert lines[1] == "IN PROGRESS: issue-underway — Underway thing"
        assert "[" not in lines[1]

    def test_in_progress_with_explicit_priority_still_shows_the_tag(self):
        """Non-regression companion to the omission test above: a caller that
        DOES have a real priority (rank 0 / high — the falsy-int footgun)
        must still see it rendered, not silently omitted."""
        from orchestrator.services.project_backlog import render_backlog_block

        block = render_backlog_block(
            [_row("a")],
            {1: 1},
            in_progress={
                "note_id": "issue-underway",
                "title": "Underway thing",
                "priority": 0,
            },
        )
        lines = block.splitlines()
        assert lines[1] == "IN PROGRESS: [high] issue-underway — Underway thing"

    def test_remainder_is_reported_when_capped(self):
        from orchestrator.services.project_backlog import render_backlog_block

        rows = [_row(f"n{i}") for i in range(3)]
        block = render_backlog_block(rows, {1: 10}, limit=3)
        assert "… 7 more" in block

    def test_empty_pool_tells_the_agent_to_file_tickets(self):
        """An empty pool must not render an empty void — the loop's own agents
        are the ones expected to fill it."""
        from orchestrator.services.project_backlog import render_backlog_block

        block = render_backlog_block([], {})
        assert "0 open" in block
        assert "kb_write" in block


class _FakeAcquire:
    """Async-context-manager pool.acquire() double.

    Mirrors the idiom in tests/test_loop_campaign_scheduling.py::_FakeAcquire
    and tests/test_delegation_timeout_outage.py::_FakeAcquire rather than
    inventing a bespoke one here.
    """

    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *a):
        return False


def _fake_vector_db(conn):
    """A pool double whose ``.acquire()`` yields ``conn`` via _FakeAcquire."""
    vector_db = MagicMock()
    vector_db.acquire = MagicMock(return_value=_FakeAcquire(conn))
    return vector_db


class TestFetchBacklog:
    @pytest.mark.asyncio
    async def test_ready_depth_candidate_query_batches_category_overlap(self):
        from orchestrator.services.project_backlog import (
            fetch_ready_backlog_candidates,
        )

        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[])

        rows = await fetch_ready_backlog_candidates(
            _fake_vector_db(conn),
            "p-1",
            ["tester", "researcher", "researcher"],
            limit=50001,
        )

        assert rows == []
        assert conn.fetch.await_count == 1
        call = conn.fetch.await_args
        sql = " ".join(call.args[0].split())
        assert "tags @> $2::text[]" in sql
        assert "tags && $3::text[]" in sql
        assert "LIMIT $4" in sql
        assert call.args[2] == ["ready"]
        assert call.args[3] == ["category:researcher", "category:tester"]
        assert call.args[4] == 50001

    @pytest.mark.asyncio
    async def test_keyset_cursor_binds_the_complete_stable_order(self):
        from datetime import datetime, timezone

        from orchestrator.services.project_backlog import BacklogCursor, fetch_backlog

        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[])
        cursor_time = datetime(2026, 8, 16, tzinfo=timezone.utc)

        await fetch_backlog(
            _fake_vector_db(conn),
            "p-1",
            after=BacklogCursor(1, cursor_time, "ticket-099"),
            include_counts=False,
        )

        call = conn.fetch.await_args_list[0]
        sql = " ".join(call.args[0].split())
        assert "priority > $6::integer" in sql
        assert "created_at IS NOT DISTINCT FROM $7::timestamptz" in sql
        assert "note_id > $8::text" in sql
        assert call.args[5:] == (True, 1, cursor_time, "ticket-099")
        assert conn.fetch.await_count == 1

    @pytest.mark.asyncio
    async def test_scan_page_can_skip_the_unrelated_counts_query(self):
        from orchestrator.services.project_backlog import fetch_backlog

        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[])

        rows, counts = await fetch_backlog(
            _fake_vector_db(conn), "p-1", include_counts=False
        )

        assert rows == [] and counts == {}
        assert conn.fetch.await_count == 1

    @pytest.mark.asyncio
    async def test_excludes_the_in_progress_initiative(self):
        """The overseer must never be offered a ticket already underway."""
        from orchestrator.services.project_backlog import fetch_backlog

        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[])
        vector_db = _fake_vector_db(conn)

        await fetch_backlog(vector_db, "p-1", exclude_note_id="issue-x")

        first_sql = conn.fetch.await_args_list[0].args[0]
        assert "note_id <> $2" in first_sql
        assert conn.fetch.await_args_list[0].args[2] == "issue-x"

    @pytest.mark.asyncio
    async def test_counts_query_also_excludes_the_in_progress_initiative(self):
        """The pool listing and its counts line must agree on what "open"
        means -- a mismatch here would make the header's total lie about
        what the list below it actually shows."""
        from orchestrator.services.project_backlog import fetch_backlog

        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[])
        vector_db = _fake_vector_db(conn)

        await fetch_backlog(vector_db, "p-1", exclude_note_id="issue-x")

        second_sql = conn.fetch.await_args_list[1].args[0]
        assert "note_id <> $2" in second_sql
        assert conn.fetch.await_args_list[1].args[2] == "issue-x"

    @pytest.mark.asyncio
    async def test_no_exclusion_binds_none(self):
        """The default (no in-progress campaign) must bind SQL NULL, not the
        string 'None' -- the WHERE clause's `$2::text IS NULL` branch depends
        on this. (Position $2, not $3: fix round 1, Finding 1 inlines the
        note-type filter as a literal, so it no longer occupies a $-slot.)"""
        from orchestrator.services.project_backlog import fetch_backlog

        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[])
        vector_db = _fake_vector_db(conn)

        await fetch_backlog(vector_db, "p-1")

        assert conn.fetch.await_args_list[0].args[2] is None

    @pytest.mark.asyncio
    async def test_orders_by_priority_then_age_ascending(self):
        """Pin the actual ORDER BY clause: a rewrite to DESC (or to created_at
        first) would still exercise the same mocks and pass a looser test --
        this one fails if the ordering regresses."""
        from orchestrator.services.project_backlog import fetch_backlog

        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[])
        vector_db = _fake_vector_db(conn)

        await fetch_backlog(vector_db, "p-1")

        first_sql = " ".join(conn.fetch.await_args_list[0].args[0].split())
        assert "ORDER BY priority ASC, created_at ASC" in first_sql

    @pytest.mark.asyncio
    async def test_note_types_are_inlined_as_a_sql_literal_not_a_bound_param(self):
        """Fix round 1, Finding 1: measured on pgvector/pgvector:pg15 (303k
        rows / 300 projects), a bound `note_type = ANY($n::text[])` cannot be
        proven under `force_generic_plan` to imply idx_knowledge_backlog's
        literal partial predicate -- the planner fell back to a full
        project scan + Filter + Sort for both queries, and the index went
        unused. The fix inlines the note-type filter as a SQL literal,
        derived from BACKLOG_NOTE_TYPES once so the tuple and the SQL can
        never drift apart. A future "tidy-up" back to `= ANY($n)` must fail
        this test."""
        from orchestrator.services.project_backlog import (
            BACKLOG_NOTE_TYPES,
            fetch_backlog,
        )

        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[])
        vector_db = _fake_vector_db(conn)

        await fetch_backlog(vector_db, "p-1")

        expected_literal = (
            "note_type IN (" + ", ".join(f"'{t}'" for t in BACKLOG_NOTE_TYPES) + ")"
        )
        for call in conn.fetch.await_args_list:
            sql = " ".join(call.args[0].split())
            assert "note_type IN (" in sql
            assert expected_literal in sql
            assert "= ANY(" not in sql

    @pytest.mark.asyncio
    async def test_limit_is_bound_as_the_third_row_query_parameter(self):
        """Position $3, not $4: fix round 1, Finding 1 inlines the note-type
        filter as a literal, so it no longer occupies a $-slot."""
        from orchestrator.services.project_backlog import fetch_backlog

        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[])
        vector_db = _fake_vector_db(conn)

        await fetch_backlog(vector_db, "p-1", limit=5)

        assert conn.fetch.await_args_list[0].args[3] == 5

    @pytest.mark.asyncio
    async def test_high_priority_rank_zero_survives_the_round_trip(self):
        """Rank 0 (high) is falsy in Python. A `row["priority"] or None` or
        `{k: v for k, v in ... if k}` style bug would silently drop it --
        prove a rank-0 row and its count both come back intact."""
        from orchestrator.services.project_backlog import fetch_backlog

        conn = MagicMock()
        conn.fetch = AsyncMock(
            side_effect=[
                [
                    {
                        "note_id": "issue-x",
                        "note_type": "issue",
                        "title": "T",
                        "priority": 0,
                    }
                ],
                [{"priority": 0, "n": 3}],
            ]
        )
        vector_db = _fake_vector_db(conn)

        rows, counts = await fetch_backlog(vector_db, "p-1")

        assert rows == [
            {
                "note_id": "issue-x",
                "note_type": "issue",
                "title": "T",
                "priority": 0,
            }
        ]
        assert 0 in counts
        assert counts[0] == 3

    @pytest.mark.asyncio
    async def test_order_by_is_total_priority_created_then_note_id(self):
        """B3: `created_at ASC` alone leaves ties (same priority, same or
        NULL created_at) broken by physical row order -- which a non-HOT
        UPDATE (every reindex re-upsert is one) can silently reshuffle,
        changing which 20 tickets the critic is shown between one kickoff
        and the next with no ticket actually added or removed. `note_id`
        as a final tiebreaker makes the order total and reproducible."""
        from orchestrator.services.project_backlog import fetch_backlog

        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[])
        vector_db = _fake_vector_db(conn)

        await fetch_backlog(vector_db, "p-1")

        first_sql = " ".join(conn.fetch.await_args_list[0].args[0].split())
        assert (
            "ORDER BY priority ASC, created_at ASC NULLS LAST, note_id ASC" in first_sql
        )


class TestFetchBacklogTagFilter:
    """B2: the auto-pull tick asks "what may this pool take next", which is a
    different question from "what is open"."""

    @pytest.mark.asyncio
    async def test_no_filter_binds_an_empty_array_and_matches_everything(self):
        from orchestrator.services.project_backlog import fetch_backlog

        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[])
        await fetch_backlog(_fake_vector_db(conn), "p-1")

        rows_call, counts_call = conn.fetch.await_args_list
        assert rows_call.args[4] == []
        assert counts_call.args[3] == []

    @pytest.mark.asyncio
    async def test_filter_binds_as_the_trailing_parameter_on_both_queries(self):
        # Trailing on purpose: the positional slots above it are pinned by the
        # tests around this one and by the plan-shape reasoning in fetch_backlog's
        # docstring, so a filter inserted mid-list would renumber them.
        from orchestrator.services.project_backlog import fetch_backlog

        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[])
        await fetch_backlog(
            _fake_vector_db(conn),
            "p-1",
            require_tags=["ready", "category:researcher"],
        )

        rows_call, counts_call = conn.fetch.await_args_list
        assert rows_call.args[4] == ["ready", "category:researcher"]
        assert counts_call.args[3] == ["ready", "category:researcher"]

    @pytest.mark.asyncio
    async def test_uses_containment_not_any_so_the_gin_index_is_reachable(self):
        # `tags @> ARRAY[...]` is indexable by idx_knowledge_tags; `= ANY` is
        # not. Same class of mistake as the note-type literal above it.
        from orchestrator.services.project_backlog import fetch_backlog

        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[])
        await fetch_backlog(_fake_vector_db(conn), "p-1", require_tags=["ready"])

        for call in conn.fetch.await_args_list:
            sql = " ".join(call.args[0].split())
            assert "tags @>" in sql
            assert "= ANY" not in sql

    @pytest.mark.asyncio
    async def test_rows_carry_tags_and_ready_at(self):
        # The tick needs both: tags to classify, ready_at to decide whether the
        # authorization is newer than the newest claim.
        from orchestrator.services.project_backlog import fetch_backlog

        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[])
        await fetch_backlog(_fake_vector_db(conn), "p-1")

        rows_sql = conn.fetch.await_args_list[0].args[0]
        assert "tags" in rows_sql.split("FROM")[0]
        assert "ready_at" in rows_sql.split("FROM")[0]


class TestBacklogIndexPredicateMatchesNoteTypes:
    """M2: the partial index in migration 0014 hardcodes its own
    ``note_type IN (...)`` predicate as a SQL literal, independent of
    ``BACKLOG_NOTE_TYPES``. Measured: adding a 4th ticket type to the
    constant alone (without a matching migration) makes
    ``idx_knowledge_backlog`` silently unusable for the new type's rows --
    ``fetch_backlog``'s WHERE clause (inlined from the SAME constant) then
    asks for a note_type set the index's predicate doesn't cover, so
    Postgres can't prove the predicate implies the query and falls back to
    a full scan -- a ~44x cost regression, silent, under both plan modes.
    """

    def test_index_predicate_matches_backlog_note_types(self):
        import re
        from pathlib import Path

        from orchestrator.services.project_backlog import BACKLOG_NOTE_TYPES

        sql = Path(
            "src/orchestrator/database/migrations/vector/0014_kb_backlog_index.notx.sql"
        ).read_text()
        m = re.search(r"note_type IN \(([^)]+)\)", sql)
        assert m, "note_type IN (...) predicate not found in migration 0014"
        types_in_index = {t.strip().strip("'") for t in m.group(1).split(",")}
        assert types_in_index == set(BACKLOG_NOTE_TYPES)


# =============================================================================
# Task 5: inject the backlog into every kickoff
# =============================================================================


class TestKickoffInjection:
    def _loop(self):
        return {
            "id": "105a6f98-134c-4077-b7e1-6d08916650d7",
            "status": "running",
            "scheduling": "standard",
            "role_sequence": ["scholar", "critic", "developer"],
            "seq_index": 0,
            "goal": "Build a thing",
            "max_iterations": 30,
            "remaining_iterations": 20,
            "campaign": None,
            "campaign_history": [],
        }

    def test_block_is_injected_verbatim(self):
        from orchestrator.services.project_loops import build_loop_kickoff

        kickoff = build_loop_kickoff(
            self._loop(),
            role="critic",
            iteration=3,
            backlog_block="PROJECT BACKLOG — 2 open: 2 high\nIN PROGRESS: (none)",
        )
        assert "PROJECT BACKLOG — 2 open: 2 high" in kickoff

    def test_the_fictional_backlog_instruction_is_gone(self):
        """The old kickoff told the agent to go find a backlog that did not
        exist. Injection replaces the search; the instruction must not survive
        or agents will keep similarity-hunting for it."""
        from orchestrator.services.project_loops import build_loop_kickoff

        kickoff = build_loop_kickoff(
            self._loop(), role="critic", iteration=3, backlog_block="X"
        )
        assert "the current open backlog" not in kickoff

    def test_no_block_still_builds_a_kickoff(self):
        from orchestrator.services.project_loops import build_loop_kickoff

        kickoff = build_loop_kickoff(self._loop(), role="scholar", iteration=1)
        assert "PROJECT GOAL" in kickoff

    def test_no_block_kickoff_tells_the_truth_about_unavailability(self):
        """Fix round 1, Finding 1: the realistic route to backlog_block=None
        in production is the fetch-failure path in _spawn_loop_job (project_id
        is NOT NULL on project_loops; vector_db is assigned unconditionally at
        import) -- i.e. a KB outage, not "there is nothing to show". The old
        unconditional "The open backlog is listed above ... do not go
        searching for it" is worse than the pre-Task-5 fiction in exactly this
        case: it asserts a block exists (false) AND forbids the agent from
        doing anything about the gap. When no block was supplied, the kickoff
        must say so plainly instead of pointing at nothing."""
        from orchestrator.services.project_loops import build_loop_kickoff

        kickoff = build_loop_kickoff(self._loop(), role="critic", iteration=3)
        assert "listed above" not in kickoff
        assert "could not be read this turn" in kickoff


def _loops_deps(**over) -> ProjectLoopsDependencies:
    """The project-loops router's dependency object.

    In the app this comes from
    ``request.app.state.project_loops_dependencies_factory``; these tests call
    the declaration directly, so they hand it in explicitly (the parameter's
    ``Depends(...)`` default is not resolved off-app).
    """
    fields = dict(
        store=AsyncMock(),
        vector_store=MagicMock(),
        spawn_loop_stage=AsyncMock(),
        writeback_loop_stage=AsyncMock(),
        resume_project_loop=AsyncMock(),
        check_vm_permission=AsyncMock(),
    )
    fields.update(over)
    return ProjectLoopsDependencies(**fields)


def _loop_deps(**over) -> ProjectLoopDependencies:
    """The loop engine's one dependency object, built from mocks.

    ``main._project_loop_dependencies()`` binds these fields to the live
    application globals; the suite binds them to mocks and a test overrides
    only the field it actually steers.
    """
    fields = dict(
        store=AsyncMock(),
        vector_store=MagicMock(),
        notifier=AsyncMock(),
        gitea_client=MagicMock(),
        main_cloud_router=MagicMock(),
        trigger_dispatch=MagicMock(),
        kick_officer_event_drain=MagicMock(),
        enforce_dispatch_grants=AsyncMock(),
        reindex_project_kb=AsyncMock(),
        completion_commands_enabled=lambda: False,
        completion_sweep_router=MagicMock(),
    )
    fields.update(over)
    return ProjectLoopDependencies(**fields)


class _SpawnLoopJobHarness:
    """Shared plumbing for exercising ``spawn_loop_job`` end to end — every
    loop spawn funnels through it, so this is where the backlog fetch must be
    proven wired, not just ``render_backlog_block`` in isolation.

    Mirrors the patch set in
    tests/test_loop_unified_advance.py::test_spawn_loop_job_forwards_park_to_create
    (the engine's dependency object / create_loop_job / provision_job_repo),
    plus a live ``vector_store`` and ``services.project_backlog.fetch_backlog``
    for the backlog fetch.
    """

    def _loop(self, **over):
        base = {
            "id": "105a6f98-134c-4077-b7e1-6d08916650d7",
            "project_id": "22222222-2222-2222-2222-222222222222",
            "campaign": None,
        }
        base.update(over)
        return base

    async def _spawn(self, loop, *, fetch_backlog_double):
        from contextlib import ExitStack

        create = AsyncMock(return_value={"id": "job-1"})
        with ExitStack() as stack:
            stack.enter_context(
                patch("orchestrator.services.project_loops.create_loop_job", create)
            )
            stack.enter_context(
                patch(
                    "orchestrator.services.job_provisioning.provision_job_repo",
                    AsyncMock(),
                )
            )
            stack.enter_context(
                patch(
                    "orchestrator.services.project_backlog.fetch_backlog",
                    fetch_backlog_double,
                )
            )
            job = await spawn_loop_job(
                loop, role="critic", iteration=2, dependencies=_loop_deps()
            )
        return job, create


class TestSpawnLoopJobBacklogWiring(_SpawnLoopJobHarness):
    """Test-quality bar: a pure ``render_backlog_block`` test doesn't prove
    the orchestrator actually threads the pool through the one funnel every
    loop job spawns from. These do."""

    @pytest.mark.asyncio
    async def test_rendered_block_reaches_create_loop_job(self):
        fetch = AsyncMock(
            return_value=(
                [
                    {
                        "note_id": "issue-x",
                        "note_type": "issue",
                        "title": "Fix the thing",
                        "priority": 0,
                    }
                ],
                {0: 1},
            )
        )
        _job, create = await self._spawn(self._loop(), fetch_backlog_double=fetch)

        fetch.assert_awaited_once()
        block = create.await_args.kwargs["backlog_block"]
        assert block is not None
        assert "PROJECT BACKLOG — 1 open: 1 high" in block
        assert "issue-x" in block

    @pytest.mark.asyncio
    async def test_in_progress_campaign_initiative_is_excluded_and_surfaced(self):
        """A ticket the loop is already working (campaign.initiative_note_id)
        keeps status='active' in the KB, so the pool query must exclude it —
        it is surfaced on the IN PROGRESS line instead, never offered twice.

        Fix round 1, Finding 2: this line used to assert the hardcoded
        `[normal]` tag main.py synthesized without ever reading a real
        priority (a genuinely-high in-progress ticket would have rendered
        `[normal]`, undermining the one block agents are meant to trust).
        _spawn_loop_job no longer asserts a priority it didn't read, and the
        renderer omits the tag entirely rather than default one in — see
        TestRenderBacklogBlock.test_in_progress_without_priority_omits_the_tag
        for the renderer-level proof."""
        fetch = AsyncMock(return_value=([], {}))
        loop = self._loop(
            campaign={"initiative_note_id": "issue-underway", "title": "Underway thing"}
        )
        _job, create = await self._spawn(loop, fetch_backlog_double=fetch)

        assert fetch.await_args.kwargs["exclude_note_id"] == "issue-underway"
        block = create.await_args.kwargs["backlog_block"]
        assert "IN PROGRESS: issue-underway — Underway thing" in block
        assert "[normal] issue-underway" not in block

    @pytest.mark.asyncio
    async def test_fetch_failure_still_spawns_with_no_block(self):
        """Non-fatal: a KB/pgvector outage must cost the block, never the
        job — every loop spawn funnels through here."""
        fetch = AsyncMock(side_effect=RuntimeError("pgvector outage"))
        job, create = await self._spawn(self._loop(), fetch_backlog_double=fetch)

        assert job == {"id": "job-1"}
        assert create.await_args.kwargs["backlog_block"] is None

    @pytest.mark.asyncio
    async def test_no_project_id_skips_fetch_entirely(self):
        """Start-up / non-project loops must not even attempt the fetch."""
        fetch = AsyncMock(return_value=([], {}))
        loop = self._loop(project_id=None)
        _job, create = await self._spawn(loop, fetch_backlog_double=fetch)

        fetch.assert_not_awaited()
        assert create.await_args.kwargs["backlog_block"] is None


# =============================================================================
# Task 6: close the ticket when the campaign is disposed
# =============================================================================


def _repo_db(**roles):
    """A postgres double for close_backlog_ticket's KB-repo resolution.

    Defaults to the shape every project on the fleet has today — one jobs
    repo, no knowledge repo — so the close tests below keep targeting exactly
    the repo they always did. Pass ``knowledge=[...]`` / ``jobs=[]`` to model
    a project that has been separated, or one with no repo at all.
    """
    table = {"jobs": [{"name": "project-68137e29-jobs", "branch": "main"}]}
    table.update(roles)

    async def _by_role(project_id, role=None):
        return list(table.get(role) or [])

    db = AsyncMock()
    db.get_project_repositories.side_effect = _by_role
    intent_id = uuid.uuid4()

    async def _begin(**kwargs):
        return {
            "id": intent_id,
            "project_id": kwargs["project_id"],
            "note_id": kwargs["note_id"],
            "canonical_state": "pending_sync",
            "projection_state": "pending",
            "retry_state": "retryable",
            "attempt_claimed": True,
            "attempt_token": uuid.uuid4(),
        }

    async def _finish(_intent_id, *, canonical, permanent, **kwargs):
        return {
            "id": intent_id,
            "canonical_state": "canonical" if canonical else "failed",
            "projection_state": "pending",
            "retry_state": "none"
            if canonical
            else ("permanent" if permanent else "retryable"),
        }

    db.begin_knowledge_materialization.side_effect = _begin
    db.finish_knowledge_materialization.side_effect = _finish
    db.finish_knowledge_projection.return_value = {"id": intent_id}
    return db


class TestCloseBacklogTicket:
    @pytest.mark.asyncio
    async def test_already_canonical_without_snapshot_fails_before_projection(self):
        """A retry may only mirror exact canonical metadata, never request defaults."""
        from unittest.mock import AsyncMock, MagicMock, patch

        from orchestrator.services.project_backlog import close_backlog_ticket

        vector_db = MagicMock()
        materialization = {
            "status": "skipped",
            "reason": "already-canonical",
            "intent_id": str(uuid.uuid4()),
            "canonical_state": "canonical",
            "projection_state": "pending",
            "retry_state": "none",
        }
        with patch(
            "orchestrator.services.kb_materialize.materialize_knowledge_metadata_update",
            AsyncMock(return_value=materialization),
        ):
            ok = await close_backlog_ticket(
                vector_db,
                MagicMock(),
                "68137e29-6b1f-4f1b-a0c1-4e6dc2be3f9a",
                "feature-x",
                "resolved",
                postgres_db=_repo_db(),
            )

        assert ok is False
        vector_db.acquire.assert_not_called()

    @pytest.mark.asyncio
    async def test_handoff_lease_loss_after_file_mirror_skips_vector_follow_on(self):
        """A stale handoff owner may finish its in-flight Gitea write, but it
        must not start the independent vector consequence afterwards.
        """
        from unittest.mock import AsyncMock, MagicMock

        from orchestrator.services.project_backlog import close_backlog_ticket
        from orchestrator.services.project_loop_atomic import (
            ProjectLoopHandoffAuthorityLost,
        )

        vector_db = MagicMock()
        gitea = MagicMock()
        gitea.get_file_content = AsyncMock(
            return_value="---\nid: feature-x\ntype: feature\nstatus: active\n---\n# T\n"
        )
        gitea.create_or_update_file = AsyncMock(return_value=True)
        gitea.change_files = AsyncMock(return_value=True)
        permits = 0

        async def authority_check():
            nonlocal permits
            permits += 1
            # before canonical mutation, after canonical mutation
            if permits == 2:
                raise ProjectLoopHandoffAuthorityLost("test lease loss")

        with pytest.raises(ProjectLoopHandoffAuthorityLost, match="test lease loss"):
            await close_backlog_ticket(
                vector_db,
                gitea,
                "68137e29-6b1f-4f1b-a0c1-4e6dc2be3f9a",
                "feature-x",
                "resolved",
                postgres_db=_repo_db(),
                authority_check=authority_check,
            )

        gitea.change_files.assert_awaited_once()
        vector_db.acquire.assert_not_called()

    @pytest.mark.asyncio
    async def test_writes_file_and_index(self):
        """Index-only would be reverted by the next kb_reindex pass, which
        re-ingests status from the markdown frontmatter."""
        from unittest.mock import AsyncMock, MagicMock

        from orchestrator.services.project_backlog import close_backlog_ticket

        conn = MagicMock()
        conn.execute = AsyncMock(return_value="UPDATE 1")

        class _CM:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *a):
                return False

        vector_db = MagicMock()
        vector_db.acquire = MagicMock(return_value=_CM())

        gitea = MagicMock()
        gitea.get_file_content = AsyncMock(
            return_value="---\nid: feature-x\ntype: feature\nstatus: active\n---\n# T\n"
        )
        gitea.create_or_update_file = AsyncMock(return_value=True)
        gitea.change_files = AsyncMock(return_value=True)

        ok = await close_backlog_ticket(
            vector_db,
            gitea,
            "68137e29-6b1f-4f1b-a0c1-4e6dc2be3f9a",
            "feature-x",
            "resolved",
            postgres_db=_repo_db(),
        )

        assert ok is True
        written = gitea.change_files.await_args.args[2][0]["content_b64"]
        import base64

        written = base64.b64decode(written).decode()
        assert "status: resolved" in written
        assert "status: active" not in written
        conn.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_gitea_failure_is_not_fatal(self):
        """Repository failure is a recoverable false disposition, not a raise."""
        from unittest.mock import AsyncMock, MagicMock

        from orchestrator.services.project_backlog import close_backlog_ticket

        conn = MagicMock()
        conn.execute = AsyncMock(return_value="UPDATE 1")

        class _CM:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *a):
                return False

        vector_db = MagicMock()
        vector_db.acquire = MagicMock(return_value=_CM())

        gitea = MagicMock()
        gitea.get_file_content = AsyncMock(side_effect=RuntimeError("gitea down"))

        ok = await close_backlog_ticket(
            vector_db,
            gitea,
            "68137e29-6b1f-4f1b-a0c1-4e6dc2be3f9a",
            "feature-x",
            "resolved",
            postgres_db=_repo_db(),
        )
        assert ok is False

    @pytest.mark.asyncio
    async def test_gitea_failure_does_not_update_the_projection(self):
        """A failed canonical close must leave the backlog projection open."""
        from unittest.mock import AsyncMock, MagicMock

        from orchestrator.services.project_backlog import close_backlog_ticket

        conn = MagicMock()
        conn.execute = AsyncMock(return_value="UPDATE 1")

        class _CM:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *a):
                return False

        vector_db = MagicMock()
        vector_db.acquire = MagicMock(return_value=_CM())

        gitea = MagicMock()
        gitea.get_file_content = AsyncMock(side_effect=RuntimeError("gitea down"))

        ok = await close_backlog_ticket(
            vector_db,
            gitea,
            "68137e29-6b1f-4f1b-a0c1-4e6dc2be3f9a",
            "feature-x",
            "resolved",
            postgres_db=_repo_db(),
        )

        assert ok is False  # the durable (file) write did not happen
        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_note_file_fails_closed_before_index_mutation(self, caplog):
        """get_file_content returning None (a 404, not an exception) is the
        "note file doesn't exist" case called out explicitly in the task
        constraints. Neither canonical state nor its projection may claim a
        close, and the refusal must be logged rather than silent."""
        import logging
        from unittest.mock import AsyncMock, MagicMock

        from orchestrator.services.project_backlog import close_backlog_ticket

        conn = MagicMock()
        conn.execute = AsyncMock(return_value="UPDATE 1")

        class _CM:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *a):
                return False

        vector_db = MagicMock()
        vector_db.acquire = MagicMock(return_value=_CM())

        gitea = MagicMock()
        gitea.get_file_content = AsyncMock(return_value=None)
        gitea.create_or_update_file = AsyncMock(return_value=True)
        gitea.change_files = AsyncMock(return_value=True)

        with caplog.at_level(
            logging.INFO, logger="orchestrator.services.project_backlog"
        ):
            ok = await close_backlog_ticket(
                vector_db,
                gitea,
                "68137e29-6b1f-4f1b-a0c1-4e6dc2be3f9a",
                "feature-x",
                "resolved",
                postgres_db=_repo_db(),
            )

        assert ok is False
        gitea.create_or_update_file.assert_not_awaited()
        conn.execute.assert_not_awaited()
        assert any("canonical close refused" in r.message for r in caplog.records)

    def test_no_status_line_gains_one(self):
        """A note that predates the status field (or never had one) must gain
        exactly one status line, not be left untouched and not gain a
        duplicate on a second close."""
        from orchestrator.services.project_backlog import (
            _REWRITTEN,
            _rewrite_status,
        )

        original = "---\nid: feature-x\ntype: feature\n---\n# T\nbody\n"
        updated, outcome = _rewrite_status(original, "resolved")

        assert outcome == _REWRITTEN  # inserting a line is a real write
        assert updated.count("status:") == 1
        assert "status: resolved" in updated
        assert "id: feature-x" in updated
        assert "type: feature" in updated
        assert updated.endswith("# T\nbody\n")

    def test_surrounding_document_is_byte_identical_except_status_line(self):
        """_rewrite_status is deliberately a line rewrite, not a YAML
        round-trip (its own docstring) -- reserializing would reformat every
        note a human ever wrote. Prove every other line, including odd
        spacing, blank lines, and the body, survives untouched."""
        from orchestrator.services.project_backlog import (
            _REWRITTEN,
            _rewrite_status,
        )

        original = (
            "---\n"
            "id: feature-x\n"
            "type: feature\n"
            "status:   active   \n"  # deliberately odd spacing
            "tags: [a, b]\n"
            "\n"
            "priority: high\n"
            "---\n"
            "# A Title\n"
            "\n"
            "Some body text with  double  spaces and a trailing note.\n"
            "  - indented bullet\n"
        )
        updated, outcome = _rewrite_status(original, "resolved")

        assert outcome == _REWRITTEN
        orig_lines = original.splitlines()
        new_lines = updated.splitlines()
        assert len(orig_lines) == len(new_lines)  # no lines added or removed

        diffs = [
            (i, o, n) for i, (o, n) in enumerate(zip(orig_lines, new_lines)) if o != n
        ]
        assert len(diffs) == 1
        _, old_line, new_line = diffs[0]
        assert old_line.startswith("status:")
        assert new_line == "status: resolved"

    @pytest.mark.asyncio
    async def test_index_update_matching_zero_rows_is_logged_not_silent(self, caplog):
        """B1 finding 3 repro: filing a `decision` note (or any non-ticket
        type) as a campaign's initiative makes the index UPDATE's
        `note_type = ANY(...)` filter match zero rows -- UPDATE 0, vs
        UPDATE 1 for a real ticket -- and the pre-fix code never inspected
        the command-status tag, so a close that closed nothing was
        completely silent."""
        import logging
        from unittest.mock import AsyncMock, MagicMock

        from orchestrator.services.project_backlog import close_backlog_ticket

        conn = MagicMock()
        conn.execute = AsyncMock(return_value="UPDATE 0")
        conn.fetchval = AsyncMock(return_value="decision")

        class _CM:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *a):
                return False

        vector_db = MagicMock()
        vector_db.acquire = MagicMock(return_value=_CM())

        gitea = MagicMock()
        gitea.get_file_content = AsyncMock(
            return_value="---\nid: decision-verdict-1\ntype: decision\nstatus: active\n---\n# T\n"
        )
        gitea.change_files = AsyncMock(return_value=True)

        with caplog.at_level(
            logging.WARNING, logger="orchestrator.services.project_backlog"
        ):
            await close_backlog_ticket(
                vector_db,
                gitea,
                "68137e29-6b1f-4f1b-a0c1-4e6dc2be3f9a",
                "decision-verdict-1",
                "resolved",
                postgres_db=_repo_db(),
            )

        matches = [r for r in caplog.records if "0 rows" in r.message]
        assert matches, "expected a warning naming the zero-row index close"
        assert "decision-verdict-1" in matches[0].message
        assert "decision" in matches[0].message  # names the mismatched type

    @pytest.mark.asyncio
    async def test_index_update_matching_one_row_does_not_warn(self, caplog):
        """Non-regression: the ordinary, successful close must stay quiet
        and must not pay for the extra note_type lookup."""
        import logging
        from unittest.mock import AsyncMock, MagicMock

        from orchestrator.services.project_backlog import close_backlog_ticket

        conn = MagicMock()
        conn.execute = AsyncMock(return_value="UPDATE 1")

        class _CM:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *a):
                return False

        vector_db = MagicMock()
        vector_db.acquire = MagicMock(return_value=_CM())

        gitea = MagicMock()
        gitea.get_file_content = AsyncMock(
            return_value="---\nid: issue-real-ticket\ntype: issue\nstatus: active\n---\n# T\n"
        )
        gitea.change_files = AsyncMock(return_value=True)

        with caplog.at_level(
            logging.WARNING, logger="orchestrator.services.project_backlog"
        ):
            await close_backlog_ticket(
                vector_db,
                gitea,
                "68137e29-6b1f-4f1b-a0c1-4e6dc2be3f9a",
                "issue-real-ticket",
                "resolved",
                postgres_db=_repo_db(),
            )

        assert not any("0 rows" in r.message for r in caplog.records)
        conn.fetchval.assert_not_called()

    @pytest.mark.asyncio
    async def test_frontmatter_less_file_reports_failure_not_success(self):
        """M5: a byte-identical no-op write (no frontmatter block to carry a
        status line at all) must not report success -- the pre-fix code
        called create_or_update_file regardless of whether anything in the
        file actually changed and trusted its truthy return."""
        from unittest.mock import AsyncMock, MagicMock

        from orchestrator.services.project_backlog import close_backlog_ticket

        conn = MagicMock()
        conn.execute = AsyncMock(return_value="UPDATE 1")

        class _CM:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *a):
                return False

        vector_db = MagicMock()
        vector_db.acquire = MagicMock(return_value=_CM())

        gitea = MagicMock()
        # No frontmatter block -- _rewrite_status is a byte-identical passthrough.
        gitea.get_file_content = AsyncMock(return_value="# Just a heading\n\nbody\n")
        gitea.create_or_update_file = AsyncMock(return_value=True)
        gitea.change_files = AsyncMock(return_value=True)

        ok = await close_backlog_ticket(
            vector_db,
            gitea,
            "68137e29-6b1f-4f1b-a0c1-4e6dc2be3f9a",
            "feature-x",
            "resolved",
            postgres_db=_repo_db(),
        )

        assert ok is False
        gitea.create_or_update_file.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_frontmatter_less_no_op_is_logged(self, caplog):
        """The skip in the test above must be visible, not silent either."""
        import logging
        from unittest.mock import AsyncMock, MagicMock

        from orchestrator.services.project_backlog import close_backlog_ticket

        conn = MagicMock()
        conn.execute = AsyncMock(return_value="UPDATE 1")

        class _CM:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *a):
                return False

        vector_db = MagicMock()
        vector_db.acquire = MagicMock(return_value=_CM())

        gitea = MagicMock()
        gitea.get_file_content = AsyncMock(return_value="# Just a heading\n\nbody\n")
        gitea.create_or_update_file = AsyncMock(return_value=True)
        gitea.change_files = AsyncMock(return_value=True)

        with caplog.at_level(
            logging.WARNING, logger="orchestrator.services.project_backlog"
        ):
            await close_backlog_ticket(
                vector_db,
                gitea,
                "68137e29-6b1f-4f1b-a0c1-4e6dc2be3f9a",
                "feature-x",
                "resolved",
                postgres_db=_repo_db(),
            )
        assert any("malformed-frontmatter" in r.getMessage() for r in caplog.records)

    @pytest.mark.asyncio
    async def test_unterminated_frontmatter_reports_failure_and_warns(self, caplog):
        """The second of _rewrite_status's three byte-identical returns: a
        `---` opener with no closing `---`. It has a status line, and that
        line is NOT at the target -- but there is no parseable frontmatter
        block to rewrite it inside, so this stays a genuine no-op: warn, no
        write, False. Only the missing-frontmatter case was covered before,
        which is how these three came to be conflated
        (knowledge-history/done/backlog_close_mislabels_idempotent_reclose.md)."""
        import logging
        from unittest.mock import AsyncMock, MagicMock

        from orchestrator.services.project_backlog import close_backlog_ticket

        conn = MagicMock()
        conn.execute = AsyncMock(return_value="UPDATE 1")

        class _CM:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *a):
                return False

        vector_db = MagicMock()
        vector_db.acquire = MagicMock(return_value=_CM())

        gitea = MagicMock()
        gitea.get_file_content = AsyncMock(
            return_value="---\nid: feature-x\ntype: feature\nstatus: active\n# T\n"
        )
        gitea.create_or_update_file = AsyncMock(return_value=True)
        gitea.change_files = AsyncMock(return_value=True)

        with caplog.at_level(
            logging.WARNING, logger="orchestrator.services.project_backlog"
        ):
            ok = await close_backlog_ticket(
                vector_db,
                gitea,
                "68137e29-6b1f-4f1b-a0c1-4e6dc2be3f9a",
                "feature-x",
                "resolved",
                postgres_db=_repo_db(),
            )

        assert ok is False
        gitea.create_or_update_file.assert_not_awaited()
        assert any("malformed-frontmatter" in r.getMessage() for r in caplog.records)

    @pytest.mark.asyncio
    async def test_idempotent_reclose_reports_success_without_writing(self):
        """The third byte-identical return, and the only one that is a
        SUCCESS: the note's frontmatter already says `status: resolved`, so
        the rewrite is a no-op because the durable mirror is already exactly
        right. Reachable via the torn-advance heal window (the close runs
        before campaign=None is persisted, so a re-driven advance calls it
        twice on one ticket) and via a human setting the status by hand
        before the campaign disposes. Returning False here made main.py log
        `the durable mirror did not land` about a mirror that had."""
        from unittest.mock import AsyncMock, MagicMock

        from orchestrator.services.project_backlog import close_backlog_ticket

        conn = MagicMock()
        conn.execute = AsyncMock(return_value="UPDATE 1")

        class _CM:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *a):
                return False

        vector_db = MagicMock()
        vector_db.acquire = MagicMock(return_value=_CM())

        gitea = MagicMock()
        gitea.get_file_content = AsyncMock(
            return_value="---\nid: feature-x\ntype: feature\nstatus: resolved\n---\n# T\n"
        )
        gitea.create_or_update_file = AsyncMock(return_value=True)
        gitea.change_files = AsyncMock(return_value=True)

        ok = await close_backlog_ticket(
            vector_db,
            gitea,
            "68137e29-6b1f-4f1b-a0c1-4e6dc2be3f9a",
            "feature-x",
            "resolved",
            postgres_db=_repo_db(),
        )

        assert ok is True
        # Nothing to write: a commit here would be an empty churn commit in
        # the note repo for every re-close.
        gitea.create_or_update_file.assert_not_awaited()
        # The two writes stay independent -- the index close still runs.
        conn.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_idempotent_reclose_is_not_warned_as_malformed(self, caplog):
        """The wrong-cause half of the same bug: the frontmatter is present
        and perfectly well-formed, so the malformed-frontmatter warning must
        not fire. The re-close is still not silent -- it says what it saw, at
        debug, on a path whose whole purpose is auditable closure."""
        import logging
        from unittest.mock import AsyncMock, MagicMock

        from orchestrator.services.project_backlog import close_backlog_ticket

        conn = MagicMock()
        conn.execute = AsyncMock(return_value="UPDATE 1")

        class _CM:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *a):
                return False

        vector_db = MagicMock()
        vector_db.acquire = MagicMock(return_value=_CM())

        gitea = MagicMock()
        gitea.get_file_content = AsyncMock(
            return_value="---\nid: feature-x\ntype: feature\nstatus: resolved\n---\n# T\n"
        )
        gitea.create_or_update_file = AsyncMock(return_value=True)
        gitea.change_files = AsyncMock(return_value=True)
        # The metadata write finds the note's file in the tree first.
        gitea.list_tree = AsyncMock(
            return_value=[
                {"path": "knowledge/feature-x.md", "type": "blob", "sha": "abc"}
            ]
        )

        with caplog.at_level(
            logging.DEBUG, logger="orchestrator.services.project_backlog"
        ):
            await close_backlog_ticket(
                vector_db,
                gitea,
                "68137e29-6b1f-4f1b-a0c1-4e6dc2be3f9a",
                "feature-x",
                "resolved",
                postgres_db=_repo_db(),
            )

        assert not any("no rewritable" in r.message for r in caplog.records)
        assert not any(r.levelno >= logging.WARNING for r in caplog.records)
        assert gitea.change_files.await_count == 0


# =============================================================================
# KB repo separation step 2: the mirror resolves its repo, it doesn't guess
# (knowledge-base/knowledge/features/knowledge_base_repo_separation.md §5, §5a)
# =============================================================================


class TestCloseBacklogTicketRepoResolution:
    """close_backlog_ticket used to build ``project-<id8>-jobs`` from the
    project id. That is a second copy of the resolution rule, and once a
    project has its own knowledge repo the mirror would rewrite a note in the
    jobs repo while the reindexer reads the knowledge repo — a divergence
    with no error anywhere (§10). It now goes through
    ``kb_reindex.resolve_kb_repo`` like every other consumer.
    """

    PROJECT_ID = "68137e29-6b1f-4f1b-a0c1-4e6dc2be3f9a"

    def _deps(self):
        conn = MagicMock()
        conn.execute = AsyncMock(return_value="UPDATE 1")

        class _CM:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *a):
                return False

        vector_db = MagicMock()
        vector_db.acquire = MagicMock(return_value=_CM())

        gitea = MagicMock()
        gitea.get_file_content = AsyncMock(
            return_value="---\nid: feature-x\ntype: feature\nstatus: active\n---\n# T\n"
        )
        gitea.create_or_update_file = AsyncMock(return_value=True)
        gitea.change_files = AsyncMock(return_value=True)
        return vector_db, gitea, conn

    async def _close(self, postgres_db):
        from orchestrator.services.project_backlog import close_backlog_ticket

        vector_db, gitea, conn = self._deps()
        ok = await close_backlog_ticket(
            vector_db,
            gitea,
            self.PROJECT_ID,
            "feature-x",
            "resolved",
            postgres_db=postgres_db,
        )
        return ok, gitea, conn

    @pytest.mark.asyncio
    async def test_jobs_repo_only_is_todays_behaviour(self):
        """No project has a knowledge repo yet, so this is the whole fleet:
        the mirror must land in the jobs repo, exactly where the hardcoded
        name used to put it."""
        ok, gitea, _conn = await self._close(_repo_db())

        assert ok is True
        assert gitea.get_file_content.await_args.args == (
            "project-68137e29-jobs",
            "knowledge/feature-x.md",
        )
        assert gitea.change_files.await_args.args[0] == "project-68137e29-jobs"

    @pytest.mark.asyncio
    async def test_knowledge_repo_wins_when_the_project_has_one(self):
        ok, gitea, _conn = await self._close(
            _repo_db(knowledge=[{"name": "project-68137e29-knowledge"}])
        )

        assert ok is True
        assert gitea.get_file_content.await_args.args[0] == (
            "project-68137e29-knowledge"
        )
        assert gitea.change_files.await_args.args[0] == "project-68137e29-knowledge"

    @pytest.mark.asyncio
    async def test_github_repo_reads_snapshot_and_updates_with_blob_sha(self):
        import contextlib

        from orchestrator.services.kb_reindex import resolve_kb_repo

        datasource_id = uuid.uuid4()
        db = _repo_db(
            knowledge=[
                {
                    "name": "Design Vault",
                    "repo_url": "https://github.com/acme/design-vault.git",
                    "branch": "vault/main",
                }
            ]
        )
        db.get_native_project_kb_datasource_ref = AsyncMock(
            return_value={"id": datasource_id, "config": {"root_path": "knowledge"}}
        )
        github = MagicMock()
        github.list_tree = AsyncMock(
            return_value=[
                {
                    "path": "knowledge/feature-x.md",
                    "type": "blob",
                    "sha": "existing-blob",
                }
            ]
        )
        github.change_files = AsyncMock(return_value=True)

        class Source:
            async def get_head(self):
                return "head-sha"

            @contextlib.asynccontextmanager
            async def snapshot(self, ref):
                assert ref == "head-sha"
                snapshot = MagicMock()
                snapshot.get_file = AsyncMock(
                    return_value=(
                        "---\nid: feature-x\ntype: feature\nstatus: active\n---\n# T\n"
                    )
                )
                yield snapshot

        with (
            patch(
                "orchestrator.services.kb_materialize.kb_client_for_repo",
                AsyncMock(return_value=github),
            ) as select,
            patch(
                "orchestrator.services.project_backlog.GiteaKnowledgeGitSource",
                return_value=Source(),
            ),
        ):
            ok, gitea, _conn = await self._close(db)

        assert ok is True
        ref = await resolve_kb_repo(db, self.PROJECT_ID)
        assert ref is not None
        select.assert_awaited_once()
        selected_db, selected_gitea, selected_ref = select.await_args.args
        assert selected_db is db
        assert selected_gitea is gitea
        assert (selected_ref.forge, selected_ref.repo, selected_ref.branch) == (
            ref.forge,
            ref.repo,
            ref.branch,
        )
        gitea.get_file_content.assert_not_awaited()
        gitea.create_or_update_file.assert_not_awaited()
        github.change_files.assert_awaited_once()
        repo, branch, files = github.change_files.await_args.args[:3]
        assert (repo, branch) == ("design-vault", "vault/main")
        assert files[0]["path"] == "knowledge/feature-x.md"
        assert files[0]["operation"] == "update"
        assert files[0]["sha"] == "existing-blob"

    @pytest.mark.asyncio
    async def test_repo_less_project_fails_closed_before_index_mutation(self, caplog):
        """Without a canonical repository, closure stays unresolved.

        The old code invented a repository name and could report an index-only
        close. The authoritative-file contract now keeps the projection and
        executor disposition untouched and records a durable retry outcome.
        """
        import logging

        with caplog.at_level(
            logging.INFO, logger="orchestrator.services.project_backlog"
        ):
            ok, gitea, conn = await self._close(_repo_db(jobs=[]))

        assert ok is False
        gitea.get_file_content.assert_not_awaited()
        gitea.create_or_update_file.assert_not_awaited()
        conn.execute.assert_not_awaited()
        assert any("canonical close refused" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_resolution_matches_the_reindexer_for_the_same_project(self):
        """The property that matters, stated directly: whatever repo the KB
        reindexer would read for a project is the repo the mirror writes."""
        from orchestrator.services.kb_reindex import resolve_kb_repo

        db = _repo_db(knowledge=[{"name": "project-68137e29-knowledge"}])
        _ok, gitea, _conn = await self._close(db)

        resolved = await resolve_kb_repo(db, self.PROJECT_ID)
        assert resolved is not None
        assert gitea.change_files.await_args.args[0] == resolved.repo


# =============================================================================
# Task 7: GET /api/projects/{id}/backlog -- the cockpit-facing wire format
# =============================================================================


class TestBacklogEndpoint:
    """The wire format must not leak the 0/1/2 rank encoding -- priority goes
    out as a word (in both ``items`` and ``counts``), and the endpoint must
    never filter or gate on it (Global constraint: priority is a
    non-binding label). A ticket the active loop is already working
    (``campaign.initiative_note_id``) is excluded from the pool and
    surfaced separately as ``in_progress`` instead, so it's never listed
    twice; "no active loop" and "loop with no campaign" must both degrade
    to an empty-ish payload, never a raise.

    Patch targets mirror the established idiom for this router (see
    ``test_loop_campaign_scheduling.py::test_start_rejects_planner_with_invalid_template``):
    auth helpers patched where the router module bound them at import time,
    and the two stores handed in on ``ProjectLoopsDependencies`` -- the
    endpoint no longer reaches into ``main`` for them.
    """

    PROJECT_ID = "68137e29-6b1f-4f1b-a0c1-4e6dc2be3f9a"

    def _loop(self, **over):
        base: dict = {
            "id": "105a6f98-134c-4077-b7e1-6d08916650d7",
            "status": "running",
            "campaign": None,
        }
        base.update(over)
        return base

    def _row(self, note_id, note_type="feature", title="T", priority=1):
        return {
            "note_id": note_id,
            "note_type": note_type,
            "title": title,
            "priority": priority,
        }

    async def _call(self, *, loop, fetch_return, member_side_effect=None):
        """Invoke the real endpoint with auth + DB reach mocked out.

        Returns ``(response, fetch_backlog_mock)`` so callers can assert on
        both the wire payload and how ``fetch_backlog`` was actually
        invoked (e.g. its ``exclude_note_id`` kwarg).
        """
        from contextlib import ExitStack

        from orchestrator.routers.project_loops import get_project_backlog

        db = AsyncMock()
        db.get_active_project_loop = AsyncMock(return_value=loop)
        fetch = AsyncMock(return_value=fetch_return)

        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    "orchestrator.routers.project_loops.require_approved_user",
                    AsyncMock(return_value={"id": str(uuid.uuid4())}),
                )
            )
            stack.enter_context(
                patch(
                    "orchestrator.routers.project_loops.require_project_member",
                    AsyncMock(side_effect=member_side_effect),
                )
            )
            stack.enter_context(
                patch("orchestrator.routers.project_loops.fetch_backlog", fetch)
            )

            out = await get_project_backlog(
                MagicMock(),
                self.PROJECT_ID,
                dependencies=_loops_deps(store=db, vector_store=MagicMock()),
            )
        return out, fetch

    @pytest.mark.asyncio
    async def test_ranks_are_translated_to_words_in_items_and_counts(self):
        """The 0/1/2 encoding must not survive to the wire in either shape
        the client reads it from."""
        rows = [self._row("f-1", priority=0), self._row("f-2", priority=2)]
        counts = {0: 1, 1: 5, 2: 1}
        out, _fetch = await self._call(loop=self._loop(), fetch_return=(rows, counts))

        assert out["items"][0]["priority"] == "high"
        assert out["items"][1]["priority"] == "low"
        assert out["counts"] == {"high": 1, "normal": 5, "low": 1}
        assert all(isinstance(k, str) for k in out["counts"])

    @pytest.mark.asyncio
    async def test_high_priority_rank_zero_is_not_dropped_or_mislabelled(self):
        """Rank 0 (high) is falsy in Python -- a `row["priority"] or None`
        or `counts.get(rank) or 0`-style bug would silently drop or
        mislabel it. Prove a lone rank-0 ticket survives intact."""
        rows = [self._row("only-ticket", priority=0)]
        counts = {0: 1}
        out, _fetch = await self._call(loop=self._loop(), fetch_return=(rows, counts))

        assert out["total"] == 1
        assert len(out["items"]) == 1
        assert out["items"][0]["priority"] == "high"
        assert out["counts"]["high"] == 1

    @pytest.mark.asyncio
    async def test_exclude_note_id_passed_through_from_campaign(self):
        """The in-progress ticket keeps status='active' in the KB -- the
        pool query must genuinely receive its note_id as the exclusion, not
        just happen to omit it from a canned fixture."""
        loop = self._loop(
            campaign={"initiative_note_id": "issue-underway", "title": "Underway thing"}
        )
        out, fetch = await self._call(loop=loop, fetch_return=([], {}))

        assert fetch.await_args.kwargs["exclude_note_id"] == "issue-underway"
        assert out["in_progress"] == {
            "note_id": "issue-underway",
            "title": "Underway thing",
        }

    @pytest.mark.asyncio
    async def test_no_campaign_excludes_nothing(self):
        """An active loop that hasn't filed a campaign yet must not raise
        and must not fabricate an exclusion."""
        out, fetch = await self._call(
            loop=self._loop(campaign=None), fetch_return=([], {})
        )

        assert fetch.await_args.kwargs["exclude_note_id"] is None
        assert out["in_progress"] is None

    @pytest.mark.asyncio
    async def test_no_active_loop_returns_well_formed_empty_payload(self):
        """No loop at all (project never started one, or its only loop is
        terminal) must degrade to an empty-ish payload, not a raise or a
        404 -- the pool itself is still a real, answerable question."""
        out, fetch = await self._call(loop=None, fetch_return=([], {}))

        assert fetch.await_args.kwargs["exclude_note_id"] is None
        assert out == {
            "total": 0,
            "counts": {"high": 0, "normal": 0, "low": 0},
            "in_progress": None,
            "items": [],
        }

    @pytest.mark.asyncio
    async def test_non_member_is_rejected(self):
        """Auth is a real gate here, not decoration: a caller
        ``require_project_member`` rejects must not reach the DB/backlog
        fetch at all."""
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc:
            await self._call(
                loop=self._loop(),
                fetch_return=([], {}),
                member_side_effect=HTTPException(
                    status_code=403, detail="not a project member"
                ),
            )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_priority_does_not_filter_or_reorder_items(self):
        """Global constraint: priority is a non-binding label. The endpoint
        must hand back exactly what fetch_backlog returned, in the same
        order, regardless of rank -- it has no business re-sorting or
        dropping rows itself."""
        rows = [
            self._row("a", priority=2),
            self._row("b", priority=0),
            self._row("c", priority=1),
        ]
        out, _fetch = await self._call(loop=self._loop(), fetch_return=(rows, {}))

        assert [i["note_id"] for i in out["items"]] == ["a", "b", "c"]
