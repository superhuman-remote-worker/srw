"""Tests for src/tools/knowledge/knowledge_tools.py.

Covers section 13 of persistent_agent_tests.md:
  13.1  KNOWLEDGE_TOOLS_METADATA registry
  13.2  create_kb_tools()
  13.3  _get_project_id() / _get_project_ids()
  13.4  kb_write
  13.5  kb_update
  13.6  kb_read
  13.7  kb_list
  13.8  kb_search
  13.9  kb_related
  13.10 kb_contradictions
  13.11 kb_provenance
  13.12 kb_unanswered
  13.13 kb_export
"""

import os
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from agent.tools.knowledge.knowledge_tools import (
    KNOWLEDGE_TOOLS_METADATA,
    _post_vault_file,
    create_kb_tools,
)


# =============================================================================
# Helpers
# =============================================================================


def _make_context(project_id=None, project_ids=None, job_id=None):
    """Create a mock ToolContext."""
    ctx = MagicMock()
    ctx.project_id = project_id or str(uuid.uuid4())
    ctx.project_ids = project_ids or [ctx.project_id]
    ctx.job_id = job_id or str(uuid.uuid4())
    ctx.config = {"current_phase": 2}
    ctx.knowledge_graph = MagicMock()
    ctx.knowledge_store = AsyncMock()
    return ctx


def _make_tools(context=None):
    """Create kb tools with mocked context, patching asyncio loop."""
    ctx = context or _make_context()
    with patch("agent.tools.knowledge.knowledge_tools.asyncio") as mock_asyncio:
        mock_asyncio.get_running_loop.side_effect = RuntimeError("no loop")
        tools = create_kb_tools(ctx)
    return tools, ctx


def _get_tool(tools, name):
    """Find a tool by name."""
    for t in tools:
        if t.name == name:
            return t
    raise KeyError(f"Tool {name} not found")


def _invoke(tool_func, args):
    """Invoke a langchain tool with args dict."""
    return tool_func.invoke(args)


def _invoke_unvalidated(tool_func, **kwargs):
    """Call the tool body directly, bypassing args_schema validation.

    Closed-vocabulary params (``type``, ``status``, ``confidence``) are now
    ``Literal``-typed, so ``invoke()`` rejects an off-vocabulary value at the
    pydantic boundary and the body never runs. That is the point — but the
    body's own checks must still hold for callers that don't go through the
    schema, and this reaches them.
    """
    return tool_func.func(**kwargs)


def _capture_workspace():
    """A mock workspace_manager that records write_file(path, content) calls."""
    ws = MagicMock()
    writes: dict = {}
    ws.write_file.side_effect = lambda rel, content: writes.__setitem__(rel, content)
    return ws, writes


@pytest.fixture(autouse=True)
def _no_materialization_http():
    """Keep every test in this module off the network.

    Materialising a note is a real HTTP POST now, so any test that writes a
    note would otherwise try to reach an orchestrator. Tests that assert on
    the call patch over this with ``_capture_materialize``; ``TestPostVaultFile``
    holds its own module-level reference and is unaffected.
    """
    with patch(
        "agent.tools.knowledge.knowledge_tools._post_vault_file",
        return_value={"status": "committed", "path": "knowledge/test.md"},
    ):
        yield


def _capture_materialize(result=None):
    """Patch the server-side materialisation seam and record what it was sent.

    Returns ``(patcher, calls)``; use ``with patcher:`` around the invocation.
    ``calls`` collects one ``{project_id, slug, content, job_id,
    retrieval_messages}`` dict per POST, which is the whole payload the
    orchestrator endpoint receives. Patching here rather than at ``httpx``
    keeps every tool test off the network while still asserting the exact
    request.
    """
    calls: list = []

    def _fake(
        project_id,
        slug,
        content,
        job_id,
        retrieval_messages=None,
        expected_blob_sha=None,
    ):
        calls.append(
            {
                "project_id": project_id,
                "slug": slug,
                "content": content,
                "job_id": job_id,
                "retrieval_messages": retrieval_messages,
                "expected_blob_sha": expected_blob_sha,
            }
        )
        return dict(result or {"status": "committed", "path": f"knowledge/{slug}.md"})

    patcher = patch(
        "agent.tools.knowledge.knowledge_tools._post_vault_file", side_effect=_fake
    )
    return patcher, calls


def _fake_http(status_code=200, body=None, raises=None):
    """A patcher for the module's ``httpx.Client`` plus the recording client."""
    response = MagicMock(status_code=status_code)
    if isinstance(body, Exception):
        response.json.side_effect = body
    else:
        response.json.return_value = body
    client = MagicMock()
    if raises is not None:
        client.post.side_effect = raises
    else:
        client.post.return_value = response
    ctor = MagicMock()
    ctor.return_value.__enter__.return_value = client
    return (
        patch("agent.tools.knowledge.knowledge_tools.httpx.Client", ctor),
        ctor,
        client,
    )


def _store_row(note_id, note_type="learning", content="", **extra):
    """One ``knowledge_index`` row in the shape ``list_notes_full`` returns.

    Defaults to a materialised note (``path`` set) — the normal state; pass
    ``path=None`` for a row no file backs yet.
    """
    row = {
        "id": note_id,
        "path": f"knowledge/{note_id}.md",
        "title": note_id,
        "type": note_type,
        "status": "active",
        "content": content,
        "confidence": None,
        "priority": 1,
        "tags": [],
        "keywords": [],
        "job_id": None,
        "phase": None,
        "superseded_by": None,
        "created": None,
        "modified": None,
    }
    row.update(extra)
    return row


def _fake_kb_store(ctx, rows):
    """Back the context's knowledge_store with a fixed vault read (gardener path)."""
    ctx.knowledge_store.list_notes_full = AsyncMock(return_value=list(rows))
    return ctx.knowledge_store


def _fake_kb_workspace(files: dict):
    """Workspace mock backed by an in-memory {rel_path: text} map.

    list_files returns root-relative *.md paths (matching the real backends),
    read_file/exists read the map, write_file records + updates it.
    """
    ws = MagicMock()
    writes: dict = {}

    def _list(root, pattern="*"):
        prefix = root.rstrip("/") + "/"
        return sorted(p for p in files if p.startswith(prefix) and p.endswith(".md"))

    def _write(rel, content):
        writes[rel] = content
        files[rel] = content

    ws.list_files.side_effect = _list
    ws.read_file.side_effect = lambda p: files[p]
    ws.exists.side_effect = lambda p: p in files
    ws.write_file.side_effect = _write
    return ws, writes


# =============================================================================
# 13.1: KNOWLEDGE_TOOLS_METADATA registry
# =============================================================================


class TestMetadataRegistry:
    """Tests for KNOWLEDGE_TOOLS_METADATA."""

    def test_contains_exactly_14_tools(self):
        assert len(KNOWLEDGE_TOOLS_METADATA) == 14

    def test_expected_tool_names(self):
        expected = {
            "kb_write",
            "kb_update",
            "kb_delete",
            "kb_read",
            "kb_list",
            "kb_search",
            "kb_grep",
            "kb_related",
            "kb_contradictions",
            "kb_provenance",
            "kb_unanswered",
            "kb_export",
            "kb_lint",
            "kb_index",
        }
        assert set(KNOWLEDGE_TOOLS_METADATA.keys()) == expected

    def test_all_have_knowledge_category(self):
        for name, meta in KNOWLEDGE_TOOLS_METADATA.items():
            assert meta["category"] == "knowledge", f"{name} category mismatch"

    def test_all_have_both_phases(self):
        for name, meta in KNOWLEDGE_TOOLS_METADATA.items():
            assert meta["phases"] == ["strategic", "tactical"], (
                f"{name} phases mismatch"
            )


# =============================================================================
# 13.2: create_kb_tools()
# =============================================================================


class TestCreateKbTools:
    """Tests for create_kb_tools()."""

    def test_tolerates_knowledge_graph_none(self):
        # slice-3 PR4c: Neo4j is optional. Without a graph the tools still build
        # (the pgvector index is canonical for retrieval; files for content).
        ctx = _make_context()
        ctx.knowledge_graph = None
        with patch("agent.tools.knowledge.knowledge_tools.asyncio") as ma:
            ma.get_running_loop.side_effect = RuntimeError
            tools = create_kb_tools(ctx)
        assert len(tools) == 14

    def test_raises_when_knowledge_store_is_none(self):
        ctx = _make_context()
        ctx.knowledge_store = None
        with pytest.raises(ValueError, match="knowledge_store"):
            with patch("agent.tools.knowledge.knowledge_tools.asyncio") as ma:
                ma.get_running_loop.side_effect = RuntimeError
                create_kb_tools(ctx)

    def test_returns_list_of_14_tools(self):
        tools, _ = _make_tools()
        assert len(tools) == 14


# =============================================================================
# 13.4: kb_write
# =============================================================================


class TestKbWrite:
    """Tests for kb_write tool."""

    def test_error_when_no_project_id(self):
        ctx = _make_context()
        ctx.project_id = None
        tools, _ = _make_tools(ctx)
        result = _invoke(
            _get_tool(tools, "kb_write"),
            {
                "title": "Test",
                "type": "decision",
                "content": "body",
            },
        )
        assert "Error" in result

    def test_calls_kg_create_note(self):
        tools, ctx = _make_tools()
        kg = ctx.knowledge_graph
        kg.create_note.return_value = "test-slug"
        ctx.knowledge_store.upsert_note = AsyncMock(return_value=uuid.uuid4())

        with patch("agent.tools.knowledge.knowledge_tools.asyncio"):
            _invoke(
                _get_tool(tools, "kb_write"),
                {
                    "title": "Test",
                    "type": "decision",
                    "content": "body",
                    "tags": ["auth"],
                },
            )

        kg.create_note.assert_called_once()
        call_kwargs = kg.create_note.call_args[1]
        assert call_kwargs["title"] == "Test"
        assert call_kwargs["note_type"] == "decision"
        assert call_kwargs["job_id"] == ctx.job_id

    def test_returns_success_with_slug(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.create_note.return_value = "my-note"
        ctx.knowledge_store.upsert_note = AsyncMock(return_value=uuid.uuid4())

        with patch("agent.tools.knowledge.knowledge_tools.asyncio"):
            result = _invoke(
                _get_tool(tools, "kb_write"),
                {
                    "title": "My Note",
                    "type": "learning",
                    "content": "body",
                },
            )
        assert "my-note" in result
        assert "learning" in result

    # The "canonical but its searchable projection is pending sync" path is
    # gone with the agent-side row write (Slice A): kb_write no longer touches
    # pgvector, so there is no second write left to fail on its own. Its two
    # successors are in TestKbWriteMaterialization (the canonical write itself
    # failing, which still fails the tool closed) and in
    # TestKbWriteDoesNotWriteTheRow (a committed-but-not-yet-indexed note,
    # which is reported rather than hidden).

    def test_returns_error_on_value_error(self):
        """A ValueError out of the graph layer surfaces as an error string.

        Uses a *valid* type: the point here is the graph raising, not the
        vocabulary. An invalid type no longer reaches the body at all — the
        Literal rejects it at the schema boundary (see
        TestKbWriteWithoutNeo4j::test_no_kg_invalid_type_errors).
        """
        tools, ctx = _make_tools()
        ctx.knowledge_graph.create_note.side_effect = ValueError("Invalid note_type")

        result = _invoke(
            _get_tool(tools, "kb_write"),
            {
                "title": "T",
                "type": "decision",
                "content": "x",
            },
        )
        assert "Error" in result
        assert "optional graph projection failed" in result

    def test_returns_error_on_generic_exception(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.create_note.side_effect = RuntimeError("boom")

        result = _invoke(
            _get_tool(tools, "kb_write"),
            {
                "title": "T",
                "type": "decision",
                "content": "x",
            },
        )
        assert "Error" in result


# =============================================================================
# 13.5: kb_update
# =============================================================================


class TestKbUpdate:
    """Tests for kb_update tool."""

    def test_error_when_no_project_id(self):
        ctx = _make_context()
        ctx.project_id = None
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_update"), {"note": "n1"})
        assert "Error" in result

    def test_calls_kg_update_note(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.update_note.return_value = True
        ctx.knowledge_graph.read_note.return_value = {"title": "T", "content": "x"}

        with patch("agent.tools.knowledge.knowledge_tools.asyncio"):
            _invoke(
                _get_tool(tools, "kb_update"),
                {
                    "note": "n1",
                    "content": "new body",
                },
            )
        ctx.knowledge_graph.update_note.assert_called_once()

    def test_error_when_note_not_found(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.update_note.return_value = False

        result = _invoke(_get_tool(tools, "kb_update"), {"note": "missing"})
        assert "not found" in result

    def test_returns_summary_of_changes(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.update_note.return_value = True
        ctx.knowledge_graph.read_note.return_value = {"title": "T", "content": "x"}

        with patch("agent.tools.knowledge.knowledge_tools.asyncio"):
            result = _invoke(
                _get_tool(tools, "kb_update"),
                {
                    "note": "n1",
                    "content": "new",
                    "status": "resolved",
                    "add_tags": ["a", "b"],
                },
            )
        assert "content replaced" in result
        assert "status → resolved" in result
        # The resulting list, not a count: with remove_tags/set_tags in play a
        # bare "+2 tag(s)" no longer describes what the note now carries.
        assert "tags → [a, b]" in result

    # `test_pgvector_writethrough_failure_nonfatal` lived here. It drove a
    # second `kg.read_note` into raising so kb_update's own pgvector
    # write-through would fail, and asserted the update still reported
    # "Updated". Slice A deleted that write — the orchestrator indexes the
    # commit — so the failure mode has no trigger left. The surviving cases
    # are TestKbUpdateMaterialization::test_materialization_failure_fails_closed
    # (the canonical write itself not completing, which is now fatal) and
    # TestKbUpdateDoesNotWriteTheRow (an index that defers, reported not
    # hidden).

    def test_error_on_value_error(self):
        """A ValueError out of the graph layer surfaces as an error string.

        Uses a *valid* status — see the sibling note on kb_write.
        """
        tools, ctx = _make_tools()
        ctx.knowledge_graph.update_note.side_effect = ValueError("Invalid status")

        result = _invoke(
            _get_tool(tools, "kb_update"),
            {
                "note": "n1",
                "status": "resolved",
            },
        )
        assert "Error" in result


# =============================================================================
# 13.6: kb_read
# =============================================================================


class TestKbRead:
    """Tests for kb_read tool."""

    def test_error_when_no_project_ids(self):
        ctx = _make_context()
        ctx.project_ids = []
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_read"), {"note": "n1"})
        assert "Error" in result

    def test_searches_across_project_ids(self):
        pid1, pid2 = str(uuid.uuid4()), str(uuid.uuid4())
        ctx = _make_context(project_ids=[pid1, pid2])
        tools, _ = _make_tools(ctx)
        kg = ctx.knowledge_graph
        kg.read_note.side_effect = [
            None,
            {"id": "n1", "title": "Found", "content": "x"},
        ]
        # The first binding's graph miss must not be masked by the index
        # fallback's default (truthy) AsyncMock return — force a real miss
        # there so the second binding's graph hit is what's exercised.
        ctx.knowledge_store.get_note_by_slug.return_value = None

        result = _invoke(_get_tool(tools, "kb_read"), {"note": "n1"})
        assert "Found" in result
        assert kg.read_note.call_count == 2

    def test_returns_formatted_markdown(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.read_note.return_value = {
            "id": "n1",
            "title": "My Note",
            "type": "decision",
            "status": "active",
            "confidence": "high",
            "tags": ["auth"],
            "keywords": ["jwt"],
            "content": "Full body text",
            "relationships": [
                {"type": "SUPPORTS", "target": "n2", "target_title": "N2"}
            ],
            "incoming_relationships": [
                {"type": "DERIVED_FROM", "source": "n0", "source_title": "N0"}
            ],
        }

        result = _invoke(_get_tool(tools, "kb_read"), {"note": "n1"})
        assert "# My Note" in result
        assert "decision" in result
        assert "[[n2]]" in result
        assert "[[n0]]" in result
        assert "→ this" in result

    def test_not_found(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.read_note.return_value = None
        # The graph miss must be a genuine miss, not an accidental hit off
        # AsyncMock's default (truthy) auto-speccing on the index fallback.
        ctx.knowledge_store.get_note_by_slug.return_value = None

        result = _invoke(_get_tool(tools, "kb_read"), {"note": "missing"})
        assert "not found" in result

    def test_not_found_surfaces_indexing_status(self):
        # A not-yet-indexed KB must disclose it is still indexing rather than
        # look like a genuine miss (agent could otherwise conclude "KB empty").
        tools, ctx = _make_tools()
        ctx.knowledge_graph.read_note.return_value = None
        ctx.knowledge_store.get_note_by_slug.return_value = None
        ctx.knowledge_store.get_watermark.return_value = MagicMock(
            status="pending", indexed_commit=None, source_head=None
        )
        result = _invoke(_get_tool(tools, "kb_read"), {"note": "missing"})
        assert "not found" in result
        assert "Still indexing" in result
        assert "pending" in result

    def test_read_falls_back_to_store_when_graph_has_no_node(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.read_note.return_value = None
        ctx.knowledge_store.get_note_by_slug.return_value = {
            "id": "vault-note",
            "title": "From the vault",
            "type": "learning",
            "status": "active",
            "content": "imported by the sweep",
        }
        result = _invoke(_get_tool(tools, "kb_read"), {"note": "vault-note"})
        assert "From the vault" in result and "not found" not in result
        ctx.knowledge_store.get_note_by_slug.assert_called_once()

    def test_read_does_not_touch_store_when_graph_has_the_note(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.read_note.return_value = {
            "id": "g",
            "title": "Graph",
            "type": "learning",
            "status": "active",
            "content": "c",
            "relationships": [],
            "incoming_relationships": [],
        }
        _invoke(_get_tool(tools, "kb_read"), {"note": "g"})
        ctx.knowledge_store.get_note_by_slug.assert_not_called()


# =============================================================================
# 13.6b: _index_readiness_notice — wedged vs rebuilding (WP4, decision H4)
# =============================================================================


class TestReadinessNotice:
    def _read_missing(self, wm):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.read_note.return_value = None
        ctx.knowledge_store.get_note_by_slug.return_value = None
        ctx.knowledge_store.get_watermark.return_value = wm
        return _invoke(_get_tool(tools, "kb_read"), {"note": "missing"})

    def test_rebuilding_keeps_todays_wording(self):
        wm = MagicMock(
            status="partial",
            indexed_commit="a" * 40,
            source_head="b" * 40,
            error_streak=1,
            wedged_since=None,
            last_error="3 note operation(s) failed",
        )
        out = self._read_missing(wm)
        assert "Still indexing — results may be incomplete" in out

    def test_wedged_says_failed_not_incomplete(self):
        from datetime import datetime, timedelta, timezone

        wm = MagicMock(
            status="partial",
            indexed_commit="a" * 40,
            source_head="b" * 40,
            error_streak=9,
            wedged_since=datetime.now(timezone.utc) - timedelta(hours=26),
            last_error="1 note operation(s) failed; retry scheduled",
        )
        out = self._read_missing(wm)
        assert "1 note(s) have failed to index for 26 h" in out
        assert "rest of this knowledge base is current" in out
        assert "may be incomplete" not in out

    def test_old_watermark_row_without_new_fields_still_renders(self):
        wm = MagicMock(
            spec=["status", "indexed_commit", "source_head"],
            status="partial",
            indexed_commit="a" * 40,
            source_head="b" * 40,
        )
        assert "Still indexing" in self._read_missing(wm)

    def test_wedged_since_recent_clamps_to_one_hour(self):
        from datetime import datetime, timedelta, timezone

        wm = MagicMock(
            status="partial",
            indexed_commit="a" * 40,
            source_head="b" * 40,
            error_streak=4,
            wedged_since=datetime.now(timezone.utc) - timedelta(minutes=5),
            last_error="1 note operation(s) failed",
        )
        out = self._read_missing(wm)
        assert "for 1 h" in out

    def test_wedged_last_error_none_falls_back_to_some(self):
        from datetime import datetime, timedelta, timezone

        wm = MagicMock(
            status="partial",
            indexed_commit="a" * 40,
            source_head="b" * 40,
            error_streak=4,
            wedged_since=datetime.now(timezone.utc) - timedelta(hours=3),
            last_error=None,
        )
        out = self._read_missing(wm)
        assert "some note(s) have failed to index" in out

    def test_mixed_wedged_and_rebuilding_join_with_newline(self):
        # Two native bindings: the first's watermark is wedged, the second's
        # is a plain rebuilding partial — the spec requires both notices,
        # wedged first, joined with "\n".
        from datetime import datetime, timedelta, timezone

        id_a = str(uuid.uuid4())
        id_b = str(uuid.uuid4())
        ctx = _make_context(project_ids=[id_a, id_b])
        tools, _ = _make_tools(ctx)
        ctx.knowledge_graph.read_note.return_value = None
        ctx.knowledge_store.get_note_by_slug.return_value = None

        wedged_wm = MagicMock(
            status="partial",
            indexed_commit="a" * 40,
            source_head="b" * 40,
            error_streak=9,
            wedged_since=datetime.now(timezone.utc) - timedelta(hours=5),
            last_error="2 note operation(s) failed",
        )
        rebuilding_wm = MagicMock(
            status="partial",
            indexed_commit="c" * 40,
            source_head="d" * 40,
            error_streak=1,
            wedged_since=None,
            last_error="1 note operation(s) failed",
        )

        def _watermark_for(kb_id):
            return wedged_wm if str(kb_id) == id_a else rebuilding_wm

        ctx.knowledge_store.get_watermark.side_effect = _watermark_for

        result = _invoke(_get_tool(tools, "kb_read"), {"note": "missing"})

        second_alias = f"project-{uuid.UUID(id_b).hex[:8]}"
        assert "[project] 2 note(s) have failed to index for 5 h" in result
        assert f"Still indexing — results may be incomplete: [{second_alias}]" in result
        # Wedged notice comes first, and the two notices are newline-joined.
        assert "current.\n⚠️ Still indexing" in result
        assert result.index("note(s) have failed") < result.index("Still indexing")

    # -- advisory (final review, Important 2a) --------------------------------
    # `advisory` had no reader anywhere: the reindexer wrote "1 note skipped
    # (duplicate id)" and nothing ever showed it. A skipped duplicate is
    # precisely the explanation a zero-result branch owes the reader, and it
    # applies to a *ready* knowledge base — the index is clean and still
    # incomplete.

    def test_ready_watermark_still_surfaces_its_advisory(self):
        wm = MagicMock(
            spec=["status", "advisory"],
            status="ready",
            advisory="1 note skipped (duplicate id `foo`)",
        )
        out = self._read_missing(wm)
        assert "ℹ️ [project] 1 note skipped (duplicate id `foo`)" in out
        assert "Still indexing" not in out

    def test_partial_watermark_renders_both_rebuilding_and_advisory(self):
        wm = MagicMock(
            status="partial",
            indexed_commit="a" * 40,
            source_head="b" * 40,
            error_streak=1,
            wedged_since=None,
            last_error="3 note operation(s) failed",
            advisory="2 notes skipped (duplicate ids)",
        )
        out = self._read_missing(wm)
        assert "Still indexing — results may be incomplete" in out
        assert "ℹ️ [project] 2 notes skipped (duplicate ids)" in out
        # Advisory lines come last, after the rebuilding notice.
        assert out.index("Still indexing") < out.index("ℹ️")

    def test_blank_or_non_string_advisory_emits_nothing(self):
        # A MagicMock-shaped watermark (every other test in this class) hands
        # back a Mock for `.advisory`; a real row can hold "" or NULL. Neither
        # may become an ℹ️ line.
        for value in ("", "   ", None, MagicMock()):
            wm = MagicMock(
                status="ready",
                advisory=value,
            )
            assert "ℹ️" not in self._read_missing(wm)


# =============================================================================
# 13.7: kb_list
# =============================================================================


class TestKbList:
    """Tests for kb_list tool."""

    def test_error_when_no_project_ids(self):
        ctx = _make_context()
        ctx.project_ids = []
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_list"), {})
        assert "Error" in result

    def test_aggregates_across_projects(self):
        pid1, pid2 = str(uuid.uuid4()), str(uuid.uuid4())
        ctx = _make_context(project_ids=[pid1, pid2])
        tools, _ = _make_tools(ctx)
        ctx.knowledge_graph.list_notes.side_effect = [
            [{"id": "n1", "title": "A", "type": "decision", "status": "active"}],
            [{"id": "n2", "title": "B", "type": "learning", "status": "active"}],
        ]

        result = _invoke(_get_tool(tools, "kb_list"), {})
        assert "2 results" in result

    def test_empty_with_filter_description(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.list_notes.return_value = []
        # An empty graph must fall through to a genuinely empty index, not
        # AsyncMock's default (non-iterable, truthy) auto-return.
        ctx.knowledge_store.list_notes.return_value = []

        result = _invoke(
            _get_tool(tools, "kb_list"),
            {
                "type": "decision",
                "tag": "auth",
            },
        )
        assert "No knowledge notes found" in result
        assert "type=decision" in result
        assert "tag=auth" in result

    def test_empty_surfaces_indexing_status(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.list_notes.return_value = []
        ctx.knowledge_store.list_notes.return_value = []
        ctx.knowledge_store.get_watermark.return_value = MagicMock(
            status="partial", indexed_commit="a" * 40, source_head="b" * 40
        )
        result = _invoke(_get_tool(tools, "kb_list"), {})
        assert "No knowledge notes found" in result
        assert "Still indexing" in result
        assert "partial" in result

    def test_list_falls_back_to_store_when_graph_is_empty(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.list_notes.return_value = []
        ctx.knowledge_store.list_notes.return_value = [
            {
                "id": "n1",
                "title": "T",
                "type": "decision",
                "status": "active",
                "confidence": None,
            },
        ]
        # For T6 (tag_vocabulary): harmless here, keeps this test valid once
        # kb_list starts consulting tag vocabulary.
        ctx.knowledge_store.tag_vocabulary = AsyncMock(return_value=[])
        result = _invoke(_get_tool(tools, "kb_list"), {})
        assert "n1" in result
        ctx.knowledge_store.list_notes.assert_called_once()

    def test_formats_with_status_icon(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.list_notes.return_value = [
            {
                "id": "n1",
                "title": "Active",
                "type": "decision",
                "status": "active",
                "confidence": "high",
            },
            {"id": "n2", "title": "Archived", "type": "learning", "status": "archived"},
        ]

        result = _invoke(_get_tool(tools, "kb_list"), {})
        assert "●" in result  # active
        assert "○" in result  # non-active


# =============================================================================
# 13.8: kb_search
# =============================================================================


class TestKbSearch:
    """Tests for kb_search tool."""

    def test_error_when_no_project_ids(self):
        ctx = _make_context()
        ctx.project_ids = []
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_search"), {"query": "test"})
        assert "Error" in result

    def test_formats_result_record(self):
        ctx = _make_context()
        rec = _srec("n1")
        rec.confidence = "high"
        ctx.knowledge_store.search_chunks.return_value = [rec]
        ctx.knowledge_store.get_watermark.return_value = None
        ctx.knowledge_store.embedding_service.model = "qwen3-embedding-8b"
        ctx.knowledge_store.embedding_service.expected_dimensions = 4096
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_search"), {"query": "auth"})
        assert "n1" in result

    def test_empty_results(self):
        ctx = _make_context()
        ctx.knowledge_store.search_chunks.return_value = []
        ctx.knowledge_store.embedding_service.model = "qwen3-embedding-8b"
        ctx.knowledge_store.embedding_service.expected_dimensions = 4096
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_search"), {"query": "nothing"})
        assert "No knowledge notes match" in result

    def test_empty_results_surface_indexing_status(self):
        ctx = _make_context()
        ctx.knowledge_store.search_chunks.return_value = []
        ctx.knowledge_store.embedding_service.model = "qwen3-embedding-8b"
        ctx.knowledge_store.embedding_service.expected_dimensions = 4096
        ctx.knowledge_store.get_watermark.return_value = MagicMock(
            status="indexing", indexed_commit=None, source_head="b" * 40
        )
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_search"), {"query": "nothing"})
        assert "No knowledge notes match" in result
        assert "Still indexing" in result
        assert "indexing" in result

    def test_empty_results_ready_kb_has_no_indexing_notice(self):
        # Ready KBs must NOT raise a false "still indexing" alarm on a real miss.
        ctx = _make_context()
        ctx.knowledge_store.search_chunks.return_value = []
        ctx.knowledge_store.embedding_service.model = "qwen3-embedding-8b"
        ctx.knowledge_store.embedding_service.expected_dimensions = 4096
        ctx.knowledge_store.get_watermark.return_value = MagicMock(
            status="ready", indexed_commit="a" * 40, source_head="a" * 40
        )
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_search"), {"query": "nothing"})
        assert "No knowledge notes match" in result
        assert "Still indexing" not in result

    def test_truncates_content_preview(self):
        ctx = _make_context()
        ctx.knowledge_store.search_chunks.return_value = [
            _srec("n1", content="x" * 300)
        ]
        ctx.knowledge_store.get_watermark.return_value = None
        ctx.knowledge_store.embedding_service.model = "qwen3-embedding-8b"
        ctx.knowledge_store.embedding_service.expected_dimensions = 4096
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_search"), {"query": "q"})
        assert "..." in result


# =============================================================================
# 13.8c: kb_grep (spec WP8, D9)
# =============================================================================


class TestKbGrep:
    def _ctx(self, matches, total):
        from shared.runtime.services.knowledge_store import GrepMatch

        ctx = _make_context()
        ctx.knowledge_store.grep_notes = AsyncMock(return_value=(matches, total))
        ctx.knowledge_store.get_watermark.return_value = None
        return ctx, GrepMatch

    def test_renders_lines_with_context_and_truncation_tail(self):
        ctx, GrepMatch = self._ctx([], 0)
        kb = uuid.UUID(ctx.project_id)
        m = GrepMatch(
            kb_id=kb,
            note_id="n1",
            title="Note One",
            line_no=7,
            line="see sales_page_2026_09",
            before=["ctx before"],
            after=["ctx after"],
        )
        ctx.knowledge_store.grep_notes.return_value = ([m], 3)
        tools, _ = _make_tools(ctx)
        out = _invoke(
            _get_tool(tools, "kb_grep"), {"pattern": "sales_page", "max_matches": 1}
        )
        assert "**n1** — Note One" in out
        assert "L7: see sales_page_2026_09" in out
        assert "ctx before" in out and "ctx after" in out
        assert "2 more matching note(s)" in out

    def test_zero_matches_says_so(self):
        ctx, _ = self._ctx([], 0)
        tools, _ = _make_tools(ctx)
        out = _invoke(_get_tool(tools, "kb_grep"), {"pattern": "nope"})
        assert "No lines match 'nope'" in out

    def test_store_value_error_becomes_usage_error(self):
        ctx, _ = self._ctx([], 0)
        ctx.knowledge_store.grep_notes.side_effect = ValueError(
            "pattern must not be empty"
        )
        tools, _ = _make_tools(ctx)
        out = _invoke(_get_tool(tools, "kb_grep"), {"pattern": " "})
        assert out.startswith("Error:") and "empty" in out

    def test_registered_in_metadata(self):
        from agent.tools.knowledge.knowledge_tools import KNOWLEDGE_TOOLS_METADATA

        assert KNOWLEDGE_TOOLS_METADATA["kb_grep"]["category"] == "knowledge"

    def test_grep_is_a_body_tool_and_never_asks_for_title_candidates(self):
        """Final review, Important 1. ``grep_notes``' title branch makes a
        title-only hit a candidate — it counts toward ``total`` and burns a
        LIMIT slot — but line extraction reads ``content`` only, so it renders
        nothing. Left on, kb_grep answered "No lines match" for content that IS
        present and offered "raise max_matches" for notes that can never render
        a line. Titles belong to kb_search(exact=)."""
        ctx, _ = self._ctx([], 0)
        tools, _ = _make_tools(ctx)
        _invoke(_get_tool(tools, "kb_grep"), {"pattern": "sales_page"})
        assert (
            ctx.knowledge_store.grep_notes.await_args.kwargs["include_titles"] is False
        )

    def test_docstring_points_titles_at_kb_search_exact(self):
        tools, _ = _make_tools()
        doc = _get_tool(tools, "kb_grep").description or ""
        assert "matches note bodies" in doc.lower()
        assert "kb_search(exact=)" in doc


class TestKbListVocabulary:
    def test_unfiltered_list_prefixes_tag_vocabulary(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.list_notes.return_value = [
            {"id": "n1", "title": "T", "type": "decision", "status": "active"}
        ]
        ctx.knowledge_store.tag_vocabulary = AsyncMock(
            return_value=[("web", 12), ("sales", 7)]
        )
        out = _invoke(_get_tool(tools, "kb_list"), {})
        assert "**Tags:** web (12), sales (7)" in out

    def test_filtered_list_has_no_vocabulary(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.list_notes.return_value = []
        ctx.knowledge_store.list_notes.return_value = []
        ctx.knowledge_store.tag_vocabulary = AsyncMock(return_value=[("web", 1)])
        out = _invoke(_get_tool(tools, "kb_list"), {"tag": "web"})
        assert "**Tags:**" not in out


# =============================================================================
# 13.8b: kb_search chunk-retrieval cutover (slice-3 PR4)
# =============================================================================


def _srec(note_id, content="body text"):
    """A note-level search result record (what search_chunks returns)."""
    rec = MagicMock()
    rec.note_id = note_id
    rec.title = "Test"
    rec.note_type = "decision"
    rec.confidence = None
    rec.content = content
    return rec


class TestKbSearchChunkCutover:
    """kb_search retrieves over the chunk index (``search_chunks``), not the
    note-level ``hybrid_search`` — the note row's embedding is NULL after the
    reindexer — and surfaces the index watermark commit.

    These deliberately do NOT patch ``asyncio.run`` so the AsyncMock store
    methods actually record their calls (the tool falls back to ``asyncio.run``
    because ``_make_tools`` leaves the creator loop unset).
    """

    def _ctx_with_store(self, records, watermark=None, project_ids=None):
        ctx = _make_context(project_ids=project_ids)
        ctx.knowledge_store.search_chunks.return_value = records
        ctx.knowledge_store.get_watermark.return_value = watermark
        ctx.knowledge_store.embedding_service.model = "qwen3-embedding-8b"
        ctx.knowledge_store.embedding_service.expected_dimensions = 4096
        return ctx

    def test_calls_search_chunks_not_hybrid_search(self):
        ctx = self._ctx_with_store([_srec("n1")])
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_search"), {"query": "auth"})
        ctx.knowledge_store.search_chunks.assert_called_once()
        ctx.knowledge_store.hybrid_search.assert_not_called()
        assert "n1" in result

    def test_passes_kb_ids_and_current_embedding_version(self):
        pid = str(uuid.uuid4())
        ctx = self._ctx_with_store([], project_ids=[pid])
        ctx.knowledge_store.embedding_service.profile_fingerprint = (
            "pf-effective-profile"
        )
        tools, _ = _make_tools(ctx)
        _invoke(_get_tool(tools, "kb_search"), {"query": "q"})
        kwargs = ctx.knowledge_store.search_chunks.call_args.kwargs
        assert kwargs["kb_ids"] == [uuid.UUID(pid)]
        # Version string must match what the reindexer stamped so the filter
        # doesn't silently zero out the live index.
        assert kwargs["embedding_version"] == (
            "qwen3-embedding-8b:4096:c1:pf-effective-profile"
        )

    def test_surfaces_indexed_commit_for_single_kb(self):
        wm = MagicMock()
        wm.indexed_commit = "379e91846da3ffffffffffffffffffffffffffff"
        ctx = self._ctx_with_store([_srec("n1")], watermark=wm)
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_search"), {"query": "q"})
        assert "379e9184" in result  # short sha in the header

    def test_no_watermark_still_returns_results(self):
        ctx = self._ctx_with_store([_srec("n1")], watermark=None)
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_search"), {"query": "q"})
        assert "n1" in result

    def test_plain_query_passes_no_new_arms(self):
        ctx = self._ctx_with_store([_srec("n1")])
        tools, _ = _make_tools(ctx)
        _invoke(_get_tool(tools, "kb_search"), {"query": "auth"})
        kwargs = ctx.knowledge_store.search_chunks.call_args.kwargs
        assert kwargs.get("exact") in (None, []) and kwargs.get("tags") in (None, [])

    def test_plain_query_preserves_embedding_input_including_whitespace(self):
        ctx = self._ctx_with_store([])
        tools, _ = _make_tools(ctx)
        _invoke(_get_tool(tools, "kb_search"), {"query": " auth "})
        assert ctx.knowledge_store.search_chunks.call_args.kwargs["query"] == " auth "

    def test_exact_and_tags_are_normalised_to_lists_and_attributed(self):
        rec = _srec("n1")
        rec.matched_arms = ["exact", "tag"]
        ctx = self._ctx_with_store([rec])
        tools, _ = _make_tools(ctx)
        out = _invoke(
            _get_tool(tools, "kb_search"), {"exact": "sales_page", "tags": ["sales"]}
        )
        kwargs = ctx.knowledge_store.search_chunks.call_args.kwargs
        assert kwargs["exact"] == ["sales_page"] and kwargs["tags"] == ["sales"]
        assert kwargs["query"] == ""
        assert "⟨exact+tag⟩" in out
        assert "exact 'sales_page'" in out  # per-arm coverage line

    def test_requires_at_least_one_angle(self):
        ctx = self._ctx_with_store([])
        tools, _ = _make_tools(ctx)
        out = _invoke(_get_tool(tools, "kb_search"), {})
        assert out.startswith("Error:") and "query" in out and "exact" in out

    def test_coverage_line_reports_per_arm_hit_counts(self):
        # Per exact TERM, not per arm: "sales_page" and "billing_id" hit
        # different, differently-sized subsets of the results, so the two
        # coverage lines must carry their own counts rather than one
        # aggregate "matched the exact arm at all" number.
        rec1 = _srec("n1", content="ships the sales_page redesign")
        rec2 = _srec("n2", content="also touches sales_page copy")
        rec3 = _srec("n3", content="unrelated billing_id migration")
        ctx = self._ctx_with_store([rec1, rec2, rec3])
        tools, _ = _make_tools(ctx)
        out = _invoke(
            _get_tool(tools, "kb_search"),
            {"query": "auth", "exact": ["sales_page", "billing_id"]},
        )
        assert "exact 'sales_page': 2 shown" in out
        assert "exact 'billing_id': 1 shown" in out

    def test_tags_are_case_folded_to_match_storage(self):
        ctx = self._ctx_with_store([])
        tools, _ = _make_tools(ctx)
        _invoke(_get_tool(tools, "kb_search"), {"query": "q", "tags": ["Sales"]})
        kwargs = ctx.knowledge_store.search_chunks.call_args.kwargs
        assert kwargs["tags"] == ["sales"]

    def test_tags_accepts_a_bare_string(self):
        ctx = self._ctx_with_store([])
        tools, _ = _make_tools(ctx)
        _invoke(_get_tool(tools, "kb_search"), {"query": "q", "tags": "sales"})
        kwargs = ctx.knowledge_store.search_chunks.call_args.kwargs
        assert kwargs["tags"] == ["sales"]

    @pytest.mark.parametrize("has_hits", [False, True])
    def test_lexical_fallback_notice_is_visible_with_or_without_hits(self, has_hits):
        from shared.runtime.services.knowledge_store import KnowledgeSearchResults

        rec = _srec("n1")
        rec.matched_arms = ["sparse", "exact"]
        results = KnowledgeSearchResults(
            [rec] if has_hits else [], lexical_fallback=True
        )
        ctx = self._ctx_with_store(results)
        tools, _ = _make_tools(ctx)

        out = _invoke(_get_tool(tools, "kb_search"), {"query": "auth"})

        assert "Embeddings unavailable" in out and "lexical fallback" in out
        assert "kb_grep(pattern=...)" in out
        if has_hits:
            assert "⟨sparse+exact⟩" in out
            assert "dense" not in out
        else:
            assert "No knowledge notes match" in out

    def test_fallback_no_hits_preserves_partial_index_advisory(self):
        from shared.runtime.services.knowledge_store import (
            KbWatermark,
            KnowledgeSearchResults,
        )

        ctx = self._ctx_with_store(
            KnowledgeSearchResults(lexical_fallback=True),
            watermark=KbWatermark(status="partial"),
        )
        tools, _ = _make_tools(ctx)
        out = _invoke(_get_tool(tools, "kb_search"), {"query": "auth"})
        assert "lexical fallback" in out
        assert "Still indexing" in out

    def test_unrecoverable_search_error_signposts_lexical_tools(self):
        ctx = self._ctx_with_store([])
        ctx.knowledge_store.search_chunks.side_effect = RuntimeError(
            "search unavailable"
        )
        tools, _ = _make_tools(ctx)
        out = _invoke(_get_tool(tools, "kb_search"), {"query": "auth"})
        assert "Error searching knowledge base: search unavailable" in out
        assert "kb_grep(pattern=...)" in out and "kb_search(exact=...)" in out

    def test_whitespace_query_is_rejected_before_search(self):
        ctx = self._ctx_with_store([])
        tools, _ = _make_tools(ctx)
        out = _invoke(_get_tool(tools, "kb_search"), {"query": "  "})
        assert out.startswith("Error:")
        ctx.knowledge_store.search_chunks.assert_not_awaited()


# =============================================================================
# 13.9: kb_related
# =============================================================================


class TestKbRelated:
    """Tests for kb_related tool."""

    def test_error_when_no_project_ids(self):
        ctx = _make_context()
        ctx.project_ids = []
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_related"), {"note": "n1"})
        assert "Error" in result

    def test_passes_max_hops(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.get_related.return_value = []

        _invoke(_get_tool(tools, "kb_related"), {"note": "n1", "max_hops": 3})
        ctx.knowledge_graph.get_related.assert_called_once()
        assert ctx.knowledge_graph.get_related.call_args[1]["max_hops"] == 3

    def test_formats_hop_singular(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.get_related.return_value = [
            {
                "id": "n2",
                "title": "T",
                "type": "decision",
                "status": "active",
                "distance": 1,
                "rel_types": ["SUPPORTS"],
            },
        ]

        result = _invoke(_get_tool(tools, "kb_related"), {"note": "n1"})
        assert "1 hop)" in result  # singular

    def test_formats_hops_plural(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.get_related.return_value = [
            {
                "id": "n2",
                "title": "T",
                "type": "decision",
                "status": "active",
                "distance": 2,
                "rel_types": ["SUPPORTS", "REFERENCES"],
            },
        ]

        result = _invoke(_get_tool(tools, "kb_related"), {"note": "n1"})
        assert "2 hops)" in result  # plural

    def test_shows_relationship_chain(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.get_related.return_value = [
            {
                "id": "n2",
                "title": "T",
                "type": "learning",
                "status": "active",
                "distance": 2,
                "rel_types": ["SUPPORTS", "DERIVED_FROM"],
            },
        ]

        result = _invoke(_get_tool(tools, "kb_related"), {"note": "n1"})
        assert "SUPPORTS → DERIVED_FROM" in result

    def test_shows_non_active_status(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.get_related.return_value = [
            {
                "id": "n2",
                "title": "T",
                "type": "decision",
                "status": "archived",
                "distance": 1,
                "rel_types": ["REFERENCES"],
            },
        ]

        result = _invoke(_get_tool(tools, "kb_related"), {"note": "n1"})
        assert "[archived]" in result


# =============================================================================
# 13.10: kb_contradictions
# =============================================================================


class TestKbContradictions:
    """Tests for kb_contradictions tool."""

    def test_empty_result(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.get_contradictions.return_value = []

        result = _invoke(_get_tool(tools, "kb_contradictions"), {})
        assert "No active contradictions" in result

    def test_formats_pairs(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.get_contradictions.return_value = [
            {"note_a": "n1", "title_a": "A", "note_b": "n2", "title_b": "B"},
        ]

        result = _invoke(_get_tool(tools, "kb_contradictions"), {})
        assert "n1" in result
        assert "n2" in result
        assert "⟷ CONTRADICTS ⟷" in result


# =============================================================================
# 13.11: kb_provenance
# =============================================================================


class TestKbProvenance:
    """Tests for kb_provenance tool."""

    def test_empty_result(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.get_provenance.return_value = []

        result = _invoke(_get_tool(tools, "kb_provenance"), {"note": "n1"})
        assert "No provenance chain" in result

    def test_indents_by_depth(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.get_provenance.return_value = [
            {"id": "n2", "title": "Parent", "type": "source", "depth": 1},
            {"id": "n3", "title": "Grandparent", "type": "source", "depth": 2},
        ]

        result = _invoke(_get_tool(tools, "kb_provenance"), {"note": "n1"})
        lines = result.split("\n")
        # depth 1: no indent, depth 2: 2 spaces indent
        depth1_line = [x for x in lines if "Parent" in x][0]
        depth2_line = [x for x in lines if "Grandparent" in x][0]
        assert "  ↑" in depth1_line  # base indent
        assert "    ↑" in depth2_line  # extra indent for depth 2

    def test_uses_arrow_prefix(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.get_provenance.return_value = [
            {"id": "n2", "title": "Source", "type": "source", "depth": 1},
        ]

        result = _invoke(_get_tool(tools, "kb_provenance"), {"note": "n1"})
        assert "↑" in result


# =============================================================================
# 13.12: kb_unanswered
# =============================================================================


class TestKbUnanswered:
    """Tests for kb_unanswered tool."""

    def test_empty_result(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.get_unanswered.return_value = []

        result = _invoke(_get_tool(tools, "kb_unanswered"), {})
        assert "No unanswered questions" in result

    def test_returns_questions(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.get_unanswered.return_value = [
            {"id": "q1", "title": "Why JWT?", "content": "details"},
        ]

        result = _invoke(_get_tool(tools, "kb_unanswered"), {})
        assert "q1" in result
        assert "Why JWT?" in result

    def test_truncates_long_content(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.get_unanswered.return_value = [
            {"id": "q1", "title": "Long Q", "content": "x" * 200},
        ]

        _invoke(_get_tool(tools, "kb_unanswered"), {})
        # Content is used as fallback for title display only when title missing
        # The preview truncation happens internally


# =============================================================================
# 13.13: kb_export
# =============================================================================


class TestKbExport:
    """Tests for kb_export tool."""

    def test_error_when_no_project_ids(self):
        ctx = _make_context()
        ctx.project_ids = []
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_export"), {"path": "/tmp/export"})
        assert "Error" in result

    def test_empty_knowledge_base(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.get_all_notes_for_export.return_value = []

        result = _invoke(_get_tool(tools, "kb_export"), {"path": "/tmp/export"})
        assert "empty" in result

    def test_error_when_no_workspace(self):
        # The local-Path fallback (agent host, invisible to the pod) is gone:
        # kb_export now requires a workspace backend.
        tools, ctx = _make_tools()
        ctx.has_workspace.return_value = False
        ctx.knowledge_graph.get_all_notes_for_export.return_value = [
            {"id": "n1", "type": "decision", "status": "active", "content": "x"},
        ]
        result = _invoke(_get_tool(tools, "kb_export"), {"path": "exports/kb"})
        assert "Error" in result
        assert "workspace" in result

    def test_creates_directory_and_writes_files(self):
        tools, ctx = _make_tools()
        ctx.has_workspace.return_value = True
        ws, writes = _capture_workspace()
        ctx.workspace_manager = ws
        ctx.knowledge_graph.get_all_notes_for_export.return_value = [
            {
                "id": "chose-jwt",
                "title": "Chose JWT",
                "type": "decision",
                "status": "active",
                "confidence": "high",
                "tags": ["auth"],
                "keywords": ["jwt"],
                "content": "We chose JWT because...",
                "relationships": [{"type": "SUPPORTS", "target": "auth-design"}],
            },
        ]

        result = _invoke(_get_tool(tools, "kb_export"), {"path": "exports/kb"})
        assert "1 note" in result

        content = writes["exports/kb/chose-jwt.md"]
        assert "---" in content  # frontmatter
        assert "id: chose-jwt" in content
        assert "type: decision" in content
        assert "tags:" in content
        assert "# Chose JWT" in content
        assert "[auth-design](auth-design.md)" in content  # markdown link
        assert "[[auth-design]]" not in content  # not a wikilink

    def test_groups_relationships_by_type(self):
        tools, ctx = _make_tools()
        ctx.has_workspace.return_value = True
        ws, writes = _capture_workspace()
        ctx.workspace_manager = ws
        ctx.knowledge_graph.get_all_notes_for_export.return_value = [
            {
                "id": "n1",
                "title": "N1",
                "type": "decision",
                "status": "active",
                "content": "body",
                "relationships": [
                    {"type": "SUPPORTS", "target": "a"},
                    {"type": "SUPPORTS", "target": "b"},
                    {"type": "REFERENCES", "target": "c"},
                ],
            },
        ]

        _invoke(_get_tool(tools, "kb_export"), {"path": "exports/kb2"})
        content = writes["exports/kb2/n1.md"]
        assert "## Relationships" in content
        assert "**SUPPORTS:** [a](a.md), [b](b.md)" in content
        assert "**REFERENCES:** [c](c.md)" in content


# =============================================================================
# kb_export destination guard — the writer behind `.md`-shaped directories
# =============================================================================


class TestKbExportDestinationGuard:
    """`path` is a DIRECTORY the tool fills with one `<note_id>.md` per note.

    Two destinations silently corrupt a vault, both observed live on project
    68137e29: a note filename (creates a directory whose name ends in `.md`,
    which git then cannot also hold a blob at) and anywhere under
    `knowledge/` (the reindexer globs `knowledge/**/*.md`, so every note
    gains a second file with the same OKF id and collides forever on
    uq_knowledge_project_note).
    """

    def test_rejects_note_filename_as_destination(self):
        tools, ctx = _make_tools()
        ctx.has_workspace.return_value = True
        ws, writes = _capture_workspace()
        ctx.workspace_manager = ws
        ctx.knowledge_graph.get_all_notes_for_export.return_value = [
            {"id": "n1", "type": "decision", "status": "active", "content": "x"},
        ]

        result = _invoke(
            _get_tool(tools, "kb_export"),
            {"path": "archive/kb_index_regenerated_2026-07-06.md"},
        )

        assert "Error" in result
        assert "DIRECTORY" in result
        # Nothing written and no directory created — the whole point is that
        # the damage is a thousand files that must be removed by hand.
        assert writes == {}
        ws.create_directory.assert_not_called()

    def test_rejects_export_into_the_vault(self):
        tools, ctx = _make_tools()
        ctx.has_workspace.return_value = True
        ws, writes = _capture_workspace()
        ctx.workspace_manager = ws
        ctx.knowledge_graph.get_all_notes_for_export.return_value = [
            {"id": "n1", "type": "decision", "status": "active", "content": "x"},
        ]

        result = _invoke(
            _get_tool(tools, "kb_export"),
            {"path": "knowledge/iter-33-developer-plan-v1-adapt.md"},
        )

        assert "Error" in result
        assert "knowledge/" in result
        assert writes == {}
        ws.create_directory.assert_not_called()

    @pytest.mark.parametrize(
        "path",
        [
            "knowledge",
            "knowledge/",
            "./knowledge/exports",
            "knowledge/sub/dir",
            "knowledge\\windows-sep",
        ],
    )
    def test_rejects_every_spelling_of_the_vault_root(self, path):
        # Normalization matters: the guard must not be defeated by a trailing
        # slash, a `./` prefix, or a backslash separator.
        tools, ctx = _make_tools()
        ctx.has_workspace.return_value = True
        ws, writes = _capture_workspace()
        ctx.workspace_manager = ws
        ctx.knowledge_graph.get_all_notes_for_export.return_value = [
            {"id": "n1", "type": "decision", "status": "active", "content": "x"},
        ]

        result = _invoke(_get_tool(tools, "kb_export"), {"path": path})

        assert "Error" in result
        assert writes == {}

    def test_rejects_empty_destination(self):
        tools, ctx = _make_tools()
        ctx.has_workspace.return_value = True
        ws, writes = _capture_workspace()
        ctx.workspace_manager = ws
        result = _invoke(_get_tool(tools, "kb_export"), {"path": "   "})
        assert "Error" in result
        assert writes == {}

    def test_still_allows_an_ordinary_directory(self):
        # The guard must not break the legitimate use — a regression here
        # would be worse than the bug, since export is the migration hatch.
        tools, ctx = _make_tools()
        ctx.has_workspace.return_value = True
        ws, writes = _capture_workspace()
        ctx.workspace_manager = ws
        ctx.knowledge_graph.get_all_notes_for_export.return_value = [
            {
                "id": "n1",
                "title": "N1",
                "type": "decision",
                "status": "active",
                "content": "body",
            },
        ]

        result = _invoke(_get_tool(tools, "kb_export"), {"path": "exports/kb-dump"})

        assert "1 note" in result
        assert "exports/kb-dump/n1.md" in writes


# =============================================================================
# Slice 2 PR1: kb_lint / kb_index gardener tools
# =============================================================================


class TestKbLint:
    """kb_lint reads the knowledge index, not the workspace vault.

    knowledge_base_repo_separation §5a: once the vault moves into its own
    server-side repo a workspace glob returns nothing, and a healthy KB would
    lint as "no markdown notes found" — the silent-empty mode this surface
    exists to prevent.
    """

    def test_lints_notes_from_the_store_without_a_workspace(self):
        ctx = _make_context()
        ctx.has_workspace.return_value = False
        ctx.workspace_manager = None
        _fake_kb_store(
            ctx,
            [
                _store_row("n1", "decision", "# N1\n\nSee [ghost](ghost.md).\n"),
            ],
        )
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_lint"), {})
        assert "dead-link" in result
        assert "knowledge/n1.md" in result

    def test_no_notes_found_reports_the_index_honestly(self):
        ctx = _make_context()
        ctx.has_workspace.return_value = False
        _fake_kb_store(ctx, [])
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_lint"), {})
        assert "No knowledge notes found" in result
        assert "knowledge index" in result

    def test_reports_dead_link_finding(self):
        ctx = _make_context()
        _fake_kb_store(
            ctx, [_store_row("n1", "decision", "# N1\n\nSee [ghost](ghost.md).\n")]
        )
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_lint"), {})
        assert "dead-link" in result

    def test_vault_path_arg_never_globs_the_workspace(self):
        # `path="knowledge"` was the old default and models still pass it. It
        # must mean "the project KB", never "glob a workspace directory that no
        # longer holds the vault".
        ctx = _make_context()
        ctx.has_workspace.return_value = True
        ws, _ = _fake_kb_workspace({})
        ctx.workspace_manager = ws
        _fake_kb_store(ctx, [_store_row("n1", "decision", "# N1\n\n[n2](n2.md)\n")])
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_lint"), {"path": "knowledge"})
        ws.list_files.assert_not_called()
        assert "dead-link" in result

    def test_flags_notes_no_file_backs(self):
        # The state the read path cannot see: a row with path IS NULL is
        # invisible to kb_read/kb_search, so lint must say so out loud.
        ctx = _make_context()
        _fake_kb_store(
            ctx,
            [
                _store_row("n1", "decision", "# N1\n\n[n2](n2.md)\n", path=None),
                _store_row("n2", "decision", "# N2\n\n[n1](n1.md)\n"),
            ],
        )
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_lint"), {})
        assert "unmaterialised-note" in result
        assert "1 of 2" in result
        assert "n1" in result

    def test_no_unmaterialised_finding_when_all_notes_have_files(self):
        ctx = _make_context()
        _fake_kb_store(ctx, [_store_row("n1", "decision", "# N1\n\n[n2](n2.md)\n")])
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_lint"), {})
        assert "unmaterialised-note" not in result

    def test_index_read_error_is_reported(self):
        ctx = _make_context()
        ctx.knowledge_store.list_notes_full = AsyncMock(
            side_effect=RuntimeError("pgvector down")
        )
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_lint"), {})
        assert "Error reading the knowledge index" in result

    def test_error_when_no_knowledge_base_in_scope(self):
        ctx = _make_context()
        ctx.project_ids = []
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_lint"), {})
        assert "Error" in result
        assert "no project knowledge base" in result

    def test_respects_path_arg(self):
        # The one thing the index cannot answer: an arbitrary markdown vault
        # in the workspace (a design vault, a repository datasource checkout).
        ctx = _make_context()
        ctx.has_workspace.return_value = True
        ws, _ = _fake_kb_workspace(
            {"docs/a.md": '---\nid: a\ntype: note\ndescription: "d"\n---\n\n# A\n'}
        )
        ctx.workspace_manager = ws
        tools, _ = _make_tools(ctx)
        _invoke(_get_tool(tools, "kb_lint"), {"path": "docs"})
        ws.list_files.assert_called_with("docs", "*.md")

    def test_explicit_path_without_workspace_is_actionable(self):
        ctx = _make_context()
        ctx.has_workspace.return_value = False
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_lint"), {"path": "docs"})
        assert "Error" in result
        assert "docs" in result
        assert "Omit `path`" in result

    def test_path_inside_the_vault_is_refused_not_globbed(self):
        ctx = _make_context()
        ctx.has_workspace.return_value = True
        ws, _ = _fake_kb_workspace({})
        ctx.workspace_manager = ws
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_lint"), {"path": "knowledge/sub"})
        ws.list_files.assert_not_called()
        assert "Error" in result
        assert "Omit `path`" in result


class TestKbIndex:
    """The vault is not a workspace directory any more, so `knowledge/index.md`
    is never written from here — the last vault write in the module went with
    the note dual-write (knowledge_base_repo_separation §7 step 4). The
    explicit-`path` mode still indexes an ordinary workspace directory."""

    def test_vault_mode_writes_nothing_to_the_workspace(self):
        # Even with a git-backed workspace right there. Writing it would put
        # OKF navigation in the jobs repo while its notes live in the
        # knowledge repo — the split this design removes.
        ctx = _make_context()
        ctx.has_workspace.return_value = True
        ctx.has_git.return_value = True
        ws, writes = _fake_kb_workspace({})
        ctx.workspace_manager = ws
        _fake_kb_store(
            ctx,
            [
                _store_row(
                    "chose-jwt",
                    "decision",
                    "# Chose JWT\n\nWe chose JWT.\n",
                    title="Chose JWT",
                )
            ],
        )
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_index"), {})
        assert writes == {}
        ws.write_file.assert_not_called()
        ws.list_files.assert_not_called()
        assert "1 note" in result
        assert "NOT rewritten" in result
        assert "knowledge/index.md" in result
        assert "kb_list" in result

    def test_skips_reserved_ids(self):
        ctx = _make_context()
        ctx.has_workspace.return_value = True
        ctx.has_git.return_value = True
        ws, _writes = _fake_kb_workspace({})
        ctx.workspace_manager = ws
        _fake_kb_store(
            ctx,
            [
                _store_row("good", "learning", "# Good\n", title="Good"),
                _store_row("index", "learning", "# Index\n"),
                _store_row("log", "learning", "# Log\n"),
            ],
        )
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_index"), {})
        assert "1 note" in result  # index/log are generated artefacts, not notes

    def test_counts_notes_without_a_workspace_at_all(self):
        # Lite tiers / persistent sessions: the notes are readable from the
        # index, and the tool no longer needs a workspace to say so.
        ctx = _make_context()
        ctx.has_workspace.return_value = False
        ctx.has_git.return_value = False
        ctx.workspace_manager = None
        _fake_kb_store(ctx, [_store_row("good", "learning", "# Good\n")])
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_index"), {})
        assert "1 note" in result
        assert "NOT rewritten" in result
        assert "knowledge/index.md" in result

    def test_no_indexable_notes_reports_the_index(self):
        ctx = _make_context()
        _fake_kb_store(ctx, [])
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_index"), {})
        assert "No indexable notes found" in result
        assert "knowledge index" in result

    def test_respects_path_arg_for_other_vaults(self):
        ctx = _make_context()
        ctx.has_workspace.return_value = True
        ws, writes = _fake_kb_workspace(
            {"docs/a.md": '---\nid: a\ntype: note\ndescription: "d"\n---\n\n# A\n'}
        )
        ctx.workspace_manager = ws
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_index"), {"path": "docs"})
        ws.list_files.assert_called_with("docs", "*.md")
        assert "docs/index.md" in writes
        assert "1 note" in result

    def test_returns_summary_with_count(self, tmp_path):
        tools, ctx = _make_tools()
        export_dir = str(tmp_path / "kb_export3")
        ctx.knowledge_graph.get_all_notes_for_export.return_value = [
            {"id": "n1", "type": "decision", "status": "active", "content": "x"},
            {"id": "n2", "type": "learning", "status": "active", "content": "y"},
        ]

        result = _invoke(_get_tool(tools, "kb_export"), {"path": export_dir})
        assert "2 note(s)" in result
        assert export_dir in result


# =============================================================================
# Slice 1: _render_note_md (pure OKF serializer)
# =============================================================================


class TestRenderNoteMd:
    """Tests for the pure OKF markdown serializer (_render_note_md)."""

    def test_emits_frontmatter_fences_and_required_keys(self):
        from agent.tools.knowledge.knowledge_tools import _render_note_md

        md = _render_note_md({"id": "chose-jwt", "type": "decision", "content": "body"})
        assert md.startswith("---\n")
        assert "\n---\n" in md  # closing fence
        assert "id: chose-jwt" in md
        assert "type: decision" in md
        assert "status: active" in md  # defaults to active

    def test_title_and_body(self):
        from agent.tools.knowledge.knowledge_tools import _render_note_md

        md = _render_note_md(
            {
                "id": "n1",
                "type": "learning",
                "title": "My Note",
                "content": "The full body.",
            }
        )
        assert "# My Note" in md
        assert "The full body." in md

    def test_title_falls_back_to_id(self):
        from agent.tools.knowledge.knowledge_tools import _render_note_md

        md = _render_note_md({"id": "abc-slug", "type": "learning", "content": "x"})
        assert "# abc-slug" in md

    def test_no_double_h1_when_content_starts_with_same_h1(self):
        # Run-8 nit (docs §11.1): the serializer prepended `# {title}` even when
        # the body already opened with the same H1 → every note's title twice.
        from agent.tools.knowledge.knowledge_tools import _render_note_md

        md = _render_note_md(
            {
                "id": "n1",
                "type": "learning",
                "title": "My Note",
                "content": "# My Note\n\nThe body.",
            }
        )
        lines = md.splitlines()
        assert lines.count("# My Note") == 1  # heading emitted exactly once
        assert "The body." in md

    def test_content_own_h1_suppresses_prepended_title(self):
        from agent.tools.knowledge.knowledge_tools import _render_note_md

        md = _render_note_md(
            {
                "id": "n1",
                "type": "learning",
                "title": "Slug Title",
                "content": "# Different Heading\n\nBody.",
            }
        )
        lines = md.splitlines()
        assert "# Slug Title" not in lines  # not prepended
        assert "# Different Heading" in lines  # body's own H1 stands

    def test_h2_leading_content_still_gets_title(self):
        # An H2 opener is not a title — the H1 title should still be prepended.
        from agent.tools.knowledge.knowledge_tools import _render_note_md

        md = _render_note_md(
            {
                "id": "n1",
                "type": "learning",
                "title": "Real Title",
                "content": "## A subsection\n\nBody.",
            }
        )
        lines = md.splitlines()
        assert "# Real Title" in lines

    def test_derives_description_from_content_first_sentence(self):
        from agent.tools.knowledge.knowledge_tools import _render_note_md

        md = _render_note_md(
            {
                "id": "n1",
                "type": "decision",
                "content": "We chose JWT because it is stateless. More detail here.",
            }
        )
        assert 'description: "We chose JWT because it is stateless."' in md

    def test_explicit_description_wins_and_is_quoted(self):
        from agent.tools.knowledge.knowledge_tools import _render_note_md

        md = _render_note_md(
            {
                "id": "n1",
                "type": "decision",
                "description": "A one: liner",
                "content": "Body sentence.",
            }
        )
        assert 'description: "A one: liner"' in md

    def test_description_escapes_double_quotes(self):
        from agent.tools.knowledge.knowledge_tools import _render_note_md

        md = _render_note_md(
            {
                "id": "n1",
                "type": "decision",
                "description": 'has "quotes"',
                "content": "b",
            }
        )
        assert 'description: "has \\"quotes\\""' in md

    def test_emits_markdown_links_not_wikilinks(self):
        from agent.tools.knowledge.knowledge_tools import _render_note_md

        md = _render_note_md(
            {
                "id": "n1",
                "type": "decision",
                "content": "b",
                "relationships": [
                    {"type": "SUPPORTS", "target": "auth-design"},
                    {"type": "SUPPORTS", "target": "token-plan"},
                    {"type": "REFERENCES", "target": "rfc-7519"},
                ],
            }
        )
        assert "[[auth-design]]" not in md  # no wikilinks
        assert (
            "**SUPPORTS:** [auth-design](auth-design.md), [token-plan](token-plan.md)"
            in md
        )
        assert "**REFERENCES:** [rfc-7519](rfc-7519.md)" in md

    def test_emits_provenance_when_present(self):
        from agent.tools.knowledge.knowledge_tools import _render_note_md

        md = _render_note_md(
            {
                "id": "n1",
                "type": "decision",
                "content": "b",
                "author": "developer",
                "job": "job-123",
                "branch": "job/abc",
            }
        )
        assert "author: developer" in md
        assert "job: job-123" in md
        assert "branch: job/abc" in md

    def test_omits_optional_fields_when_absent(self):
        from agent.tools.knowledge.knowledge_tools import _render_note_md

        md = _render_note_md({"id": "n1", "type": "decision", "content": "b"})
        assert "confidence:" not in md
        assert "author:" not in md
        assert "branch:" not in md
        assert "superseded_by:" not in md

    def test_emits_tags_keywords_confidence_and_superseded_by(self):
        from agent.tools.knowledge.knowledge_tools import _render_note_md

        md = _render_note_md(
            {
                "id": "n1",
                "type": "decision",
                "content": "b",
                "tags": ["auth", "security"],
                "keywords": ["jwt"],
                "confidence": "high",
                "status": "superseded",
                "superseded_by": "new-note",
            }
        )
        # C-1: flow-sequence elements are quoted so arbitrary agent strings
        # (commas, colons, @-scalars) stay valid YAML.
        assert 'tags: ["auth", "security"]' in md
        assert 'keywords: ["jwt"]' in md
        assert "confidence: high" in md
        assert "status: superseded" in md
        assert "superseded_by: new-note" in md


# =============================================================================
# Server-side materialisation of knowledge/<slug>.md
# (knowledge-base/knowledge/features/knowledge_base_repo_separation.md §7 step 4)
#
# The note file used to be a second write into the agent's own workspace
# checkout, guarded on has_git(). It is a POST to the orchestrator now, which
# owns the commit — so it needs neither git nor a workspace, and no vault write
# touches the agent's filesystem any more.
# =============================================================================


def _make_git_context(**kwargs):
    """Context whose has_git() is True and whose workspace records writes."""
    ctx = _make_context(**kwargs)
    ctx.has_git.return_value = True
    ctx._job_metadata = {"config_name": "developer"}
    ctx.workspace_manager = MagicMock()
    ctx.workspace_manager.git_manager.current_branch.return_value = "job/abc"
    return ctx


def _make_gitless_context(**kwargs):
    """A runtime with NO workspace and NO git — persistent session / lite tier.

    Exactly the shape whose notes used to stay pathless (and therefore
    invisible to kb_read/kb_search) because the dual-write skipped it.
    """
    ctx = _make_context(**kwargs)
    ctx.has_git.return_value = False
    ctx.has_workspace.return_value = False
    ctx.workspace_manager = None
    ctx._job_metadata = {"config_name": "interactive"}
    return ctx


class TestPostVaultFile:
    """The HTTP seam itself — URL, auth, payload, and its never-raise contract.

    Everything above it is allowed to log-and-continue only because this
    function absorbs every transport outcome into a status dict.
    """

    def _post(self, **env):
        base = {"ORCHESTRATOR_URL": "http://orch:8085", "MCP_INTERNAL_KEY": "sekret"}
        base.update(env)
        with patch.dict(os.environ, base, clear=False):
            return _post_vault_file("proj-1", "chose-jwt", "# body\n", "job-9")

    def test_posts_to_the_projects_materialize_endpoint(self):
        patcher, ctor, client = _fake_http(body={"status": "committed"})
        with patcher:
            result = self._post()
        url = client.post.call_args[0][0]
        assert url == "http://orch:8085/api/projects/proj-1/knowledge/materialize"
        assert result == {"status": "committed"}

    def test_sends_slug_content_and_job_id(self):
        patcher, ctor, client = _fake_http(body={"status": "committed"})
        with patcher:
            self._post()
        payload = client.post.call_args.kwargs["json"]
        assert payload == {
            "slug": "chose-jwt",
            "content": "# body\n",
            "job_id": "job-9",
        }

    def test_authenticates_with_the_internal_key(self):
        patcher, ctor, _ = _fake_http(body={"status": "committed"})
        with patcher:
            self._post()
        assert ctor.call_args.kwargs["headers"] == {"X-Internal-Key": "sekret"}

    def test_omits_job_id_when_there_is_none(self):
        # Persistent sessions have no job; the endpoint's job_id is optional.
        patcher, ctor, client = _fake_http(body={"status": "committed"})
        with patcher, patch.dict(os.environ, {}, clear=False):
            _post_vault_file("proj-1", "n1", "x", None)
        assert "job_id" not in client.post.call_args.kwargs["json"]

    def test_sends_retrieval_messages_when_the_caller_has_some(self):
        # OKF frontmatter carries no retrieval field, so the POST body is the
        # only way a caller's synthetic queries can reach knowledge_index.
        patcher, ctor, client = _fake_http(body={"status": "committed"})
        with patcher, patch.dict(os.environ, {}, clear=False):
            _post_vault_file(
                "proj-1", "n1", "x", None, ["when does the sweep run?", "why defer?"]
            )
        payload = client.post.call_args.kwargs["json"]
        assert payload["retrieval_messages"] == [
            "when does the sweep run?",
            "why defer?",
        ]

    @pytest.mark.parametrize("messages", [None, []])
    def test_omits_retrieval_messages_entirely_when_there_are_none(self, messages):
        # Absent must stay absent, not arrive as []. The endpoint reads a
        # missing key as "leave whatever is stored alone" (upsert_kb_note's
        # COALESCE sentinel); sending [] would blank a note's messages on
        # every ordinary rewrite.
        patcher, ctor, client = _fake_http(body={"status": "committed"})
        with patcher, patch.dict(os.environ, {}, clear=False):
            _post_vault_file("proj-1", "n1", "x", None, messages)
        assert "retrieval_messages" not in client.post.call_args.kwargs["json"]

    def test_transport_failure_returns_failed_instead_of_raising(self):
        import httpx as _httpx

        patcher, _, _ = _fake_http(raises=_httpx.ConnectError("boom"))
        with patcher:
            result = self._post()
        assert result["status"] == "failed"
        assert "unreachable" in result["reason"]

    def test_non_200_returns_failed(self):
        # The endpoint answers 200 for every KB-level outcome, so a non-200 is
        # transport/auth — 401 from a missing internal key, most likely.
        patcher, _, _ = _fake_http(status_code=401, body={"detail": "nope"})
        with patcher:
            result = self._post()
        assert result == {"status": "failed", "reason": "http-401"}

    def test_malformed_body_returns_failed(self):
        patcher, _, _ = _fake_http(body=ValueError("not json"))
        with patcher:
            result = self._post()
        assert result == {"status": "failed", "reason": "malformed-response"}

    def test_statusless_body_returns_failed(self):
        patcher, _, _ = _fake_http(body={"repo": "x"})
        with patcher:
            result = self._post()
        assert result == {"status": "failed", "reason": "malformed-response"}


class TestKbWriteMaterialization:
    """kb_write hands the rendered note to the orchestrator, never the workspace."""

    def test_materializes_the_rendered_note_through_the_endpoint(self):
        ctx = _make_git_context()
        ctx.knowledge_graph.create_note.return_value = "chose-jwt"
        ctx.knowledge_store.upsert_note = AsyncMock(return_value=uuid.uuid4())
        tools, _ = _make_tools(ctx)

        patcher, calls = _capture_materialize()
        with patcher, patch("agent.tools.knowledge.knowledge_tools.asyncio"):
            _invoke(
                _get_tool(tools, "kb_write"),
                {"title": "Chose JWT", "type": "decision", "content": "We chose JWT."},
            )

        assert len(calls) == 1
        call = calls[0]
        assert call["project_id"] == ctx.project_id
        assert call["slug"] == "chose-jwt"
        assert call["job_id"] == ctx.job_id
        assert "id: chose-jwt" in call["content"]
        assert "author: developer" in call["content"]
        assert "branch: job/abc" in call["content"]

    def test_never_writes_the_note_to_the_workspace(self):
        # Acceptance criterion 11: no vault write touches the agent filesystem.
        ctx = _make_git_context()
        ctx.knowledge_graph.create_note.return_value = "n1"
        ctx.knowledge_store.upsert_note = AsyncMock(return_value=uuid.uuid4())
        tools, _ = _make_tools(ctx)

        patcher, _ = _capture_materialize()
        with patcher, patch("agent.tools.knowledge.knowledge_tools.asyncio"):
            _invoke(
                _get_tool(tools, "kb_write"),
                {"title": "T", "type": "decision", "content": "x"},
            )
        ctx.workspace_manager.write_file.assert_not_called()

    def test_materializes_without_a_workspace_or_git(self):
        # The new capability (criterion 9): persistent sessions and lite tiers
        # used to skip the write entirely and leave the note pathless, which
        # made it invisible to kb_read/kb_search.
        ctx = _make_gitless_context()
        ctx.knowledge_graph.create_note.return_value = "from-a-session"
        ctx.knowledge_store.upsert_note = AsyncMock(return_value=uuid.uuid4())
        tools, _ = _make_tools(ctx)

        patcher, calls = _capture_materialize()
        with patcher, patch("agent.tools.knowledge.knowledge_tools.asyncio"):
            result = _invoke(
                _get_tool(tools, "kb_write"),
                {"title": "From A Session", "type": "learning", "content": "x"},
            )

        assert [c["slug"] for c in calls] == ["from-a-session"]
        assert "author: interactive" in calls[0]["content"]
        assert "branch:" not in calls[0]["content"]  # no git, no branch
        assert "from-a-session" in result

    def test_materialization_failure_fails_closed(self):
        ctx = _make_git_context()
        ctx.knowledge_graph.create_note.return_value = "n1"
        ctx.knowledge_store.upsert_note = AsyncMock(return_value=uuid.uuid4())
        tools, _ = _make_tools(ctx)

        patcher, calls = _capture_materialize(
            {"status": "failed", "reason": "commit-refused"}
        )
        with patcher, patch("agent.tools.knowledge.knowledge_tools.asyncio"):
            result = _invoke(
                _get_tool(tools, "kb_write"),
                {"title": "T", "type": "decision", "content": "x"},
            )
        assert len(calls) == 1
        assert result.startswith("Error: canonical knowledge write")
        ctx.knowledge_graph.create_note.assert_not_called()
        ctx.knowledge_store.upsert_note.assert_not_awaited()

    def test_failed_materialization_is_logged_as_an_error(self):
        # Sec.10: after this change a failed materialisation means the note is
        # in Postgres and invisible to every reader. It gets an alertable
        # ERROR under the same `kb-materialize:` prefix the orchestrator uses,
        # not an unread warning.
        ctx = _make_git_context()
        ctx.knowledge_graph.create_note.return_value = "n1"
        ctx.knowledge_store.upsert_note = AsyncMock(return_value=uuid.uuid4())
        tools, _ = _make_tools(ctx)

        patcher, _ = _capture_materialize(
            {"status": "failed", "reason": "resolve-error"}
        )
        with patcher, patch("agent.tools.knowledge.knowledge_tools.asyncio"):
            with patch("agent.tools.knowledge.knowledge_tools.logger") as log:
                _invoke(
                    _get_tool(tools, "kb_write"),
                    {"title": "T", "type": "decision", "content": "x"},
                )
        assert log.error.called
        rendered = log.error.call_args[0][0] % log.error.call_args[0][1:]
        assert rendered.startswith("kb-materialize:")
        assert "resolve-error" in rendered

    def test_passes_description_arg_into_frontmatter(self):
        ctx = _make_git_context()
        ctx.knowledge_graph.create_note.return_value = "n1"
        ctx.knowledge_store.upsert_note = AsyncMock(return_value=uuid.uuid4())
        tools, _ = _make_tools(ctx)

        patcher, calls = _capture_materialize()
        with patcher, patch("agent.tools.knowledge.knowledge_tools.asyncio"):
            _invoke(
                _get_tool(tools, "kb_write"),
                {
                    "title": "T",
                    "type": "decision",
                    "content": "x",
                    "description": "A crisp summary.",
                },
            )
        assert 'description: "A crisp summary."' in calls[0]["content"]

    def test_forwards_retrieval_messages_to_the_endpoint(self):
        # The last thing kb_write's deleted row write still owned. OKF
        # frontmatter has no retrieval field, so if the tool does not hand
        # these to the endpoint they reach nothing that a reader queries.
        ctx = _make_git_context()
        ctx.knowledge_graph.create_note.return_value = "n1"
        tools, _ = _make_tools(ctx)

        patcher, calls = _capture_materialize()
        with patcher, patch("agent.tools.knowledge.knowledge_tools.asyncio"):
            _invoke(
                _get_tool(tools, "kb_write"),
                {
                    "title": "T",
                    "type": "decision",
                    "content": "x",
                    "retrieval_messages": ["why JWT?", "which auth did we pick?"],
                },
            )
        assert calls[0]["retrieval_messages"] == [
            "why JWT?",
            "which auth did we pick?",
        ]

    def test_a_note_without_retrieval_messages_forwards_none(self):
        # None, not [] — the better-typed way to say "no opinion", the
        # endpoint reading it as "leave the stored value alone". It is NOT
        # what stops a rewrite blanking an earlier note's messages: [] would
        # not blank them either, because `_post_vault_file` drops an empty
        # list from the payload exactly as it drops None. That guard is the
        # protection, and test_omits_retrieval_messages_entirely_... is what
        # holds it.
        ctx = _make_git_context()
        ctx.knowledge_graph.create_note.return_value = "n1"
        tools, _ = _make_tools(ctx)

        patcher, calls = _capture_materialize()
        with patcher, patch("agent.tools.knowledge.knowledge_tools.asyncio"):
            _invoke(
                _get_tool(tools, "kb_write"),
                {"title": "T", "type": "decision", "content": "x"},
            )
        assert calls[0]["retrieval_messages"] is None

    def test_stamps_created_and_modified_into_the_note(self):
        # knowledge_index.created_at has no column DEFAULT, and the only
        # ingest path that can set it is a frontmatter `created:` line. Before
        # Slice A the deleted row write supplied it; the file must now carry
        # it, or every agent-written note stores NULL forever.
        ctx = _make_git_context()
        ctx.knowledge_graph.create_note.return_value = "n1"
        tools, _ = _make_tools(ctx)

        patcher, calls = _capture_materialize()
        with patcher, patch("agent.tools.knowledge.knowledge_tools.asyncio"):
            _invoke(
                _get_tool(tools, "kb_write"),
                {"title": "T", "type": "decision", "content": "x"},
            )
        content = calls[0]["content"]
        assert "\ncreated: " in content
        assert "\nmodified: " in content

    def test_the_stamped_timestamps_survive_the_reindexers_parse(self):
        # The round trip that matters, both halves with real code: kb_write
        # renders the note, and the reindexer's own parser reads it back. A
        # format mismatch here is silent -- the note indexes fine and simply
        # stores NULL -- so assert on the parsed values, not on the text.
        # `note_fields` is imported by path rather than at module scope: this
        # is the one agent-side test that reaches across the seam, and the
        # repo has been bitten by loading orchestrator modules under two names.
        from orchestrator.services.kb_reindex import note_fields

        from shared.runtime.knowledge.gardener import parse_note_md

        ctx = _make_git_context()
        ctx.knowledge_graph.create_note.return_value = "n1"
        tools, _ = _make_tools(ctx)

        patcher, calls = _capture_materialize()
        with patcher, patch("agent.tools.knowledge.knowledge_tools.asyncio"):
            _invoke(
                _get_tool(tools, "kb_write"),
                {"title": "Round Trip", "type": "decision", "content": "x"},
            )
        fm, body = parse_note_md(calls[0]["content"])
        fields = note_fields("knowledge/round-trip.md", fm, body)
        assert fields["created_at"] is not None
        assert fields["modified_at"] is not None
        # Both stamps come from the one timestamp the write already computes.
        assert fields["created_at"] == fields["modified_at"]
        assert fields["created_at"].tzinfo is not None


# =============================================================================
# Slice A: the orchestrator owns the knowledge_index row
# =============================================================================


@pytest.fixture
def ks():
    """The context's knowledge store, as a mock these tests can interrogate."""
    store = AsyncMock()
    store.upsert_note = AsyncMock(return_value=uuid.uuid4())
    return store


@pytest.fixture
def materializer():
    """Patch the canonical write seam — committed AND indexed by default.

    ``_materialize_note`` is the whole server-side round trip now: the
    orchestrator commits the note and indexes it inline before answering, so
    its result dict is where ``indexed`` / ``index_reason`` come from.
    """
    with patch(
        "agent.tools.knowledge.knowledge_tools._materialize_note",
        return_value={
            "status": "committed",
            "canonical_state": "canonical",
            "indexed": True,
            "index_reason": None,
        },
    ) as materialize:
        yield materialize


@pytest.fixture
def kb_tools(ks, materializer):
    """kb tools by name, each callable with the tool's keyword arguments."""
    ctx = _make_context()
    ctx.knowledge_store = ks
    ctx.knowledge_graph.read_note.return_value = None  # no slug collision
    tools, _ = _make_tools(ctx)
    return {tool.name: (lambda _tool=tool, **kw: _invoke(_tool, kw)) for tool in tools}


class TestKbWriteDoesNotWriteTheRow:
    """After Slice A the orchestrator owns row, chunks and links. A second
    agent-side write would clobber the centroid the inline index just wrote
    and pay for an embedding nobody reads."""

    def test_kb_write_never_calls_upsert_note(self, kb_tools, ks):
        result = kb_tools["kb_write"](
            type="learning", title="A finding", content="the body"
        )
        assert result.startswith("Created knowledge note:")
        ks.upsert_note.assert_not_called()

    def test_success_reports_the_note_is_searchable(self, kb_tools):
        result = kb_tools["kb_write"](
            type="learning", title="A finding", content="the body"
        )
        # The success prefix is load-bearing, not decoration: the graph-failure
        # return carries the same `indexed=` suffix, so without this the test
        # would stay green if kb_write started answering with an error.
        assert result.startswith("Created knowledge note:")
        assert "indexed=yes" in result

    def test_a_deferred_index_is_reported_not_hidden(self, kb_tools, materializer):
        materializer.return_value = {
            "status": "committed",
            "canonical_state": "canonical",
            "indexed": False,
            "index_reason": "reindex-running",
        }
        result = kb_tools["kb_write"](
            type="learning", title="A finding", content="the body"
        )
        assert result.startswith("Created knowledge note:")
        assert "indexed=deferred:reindex-running" in result


_PRIOR_CREATED = "2026-01-02T03:04:05+00:00"


def _graph_existing(**over):
    """A ``kg.read_note``-shaped note that already exists in the KB."""
    base = {
        "id": "an-existing-note",
        "title": "T",
        "type": "decision",
        "content": "old body",
        "status": "active",
        "tags": [],
        "keywords": [],
        "created": _PRIOR_CREATED,
        "modified": _PRIOR_CREATED,
        "retrieval_messages": ["why did we pick JWT?"],
        "relationships": [],
    }
    base.update(over)
    return base


@pytest.fixture
def kb_update_tools(ks, materializer):
    """kb tools over a graph-backed KB that already holds ``an-existing-note``."""
    ctx = _make_context()
    ctx.knowledge_store = ks
    ctx.knowledge_graph.read_note.return_value = _graph_existing()
    ctx.knowledge_graph.update_note.return_value = True
    tools, _ = _make_tools(ctx)
    return {tool.name: (lambda _tool=tool, **kw: _invoke(_tool, kw)) for tool in tools}


@pytest.fixture
def kb_update_tools_kgless(ks, materializer):
    """The same, on the store-only tier (``_update_existing_kgless``)."""
    ctx = _make_context()
    ctx.knowledge_graph = None
    ctx.knowledge_store = ks
    ks.get_note_by_slug = AsyncMock(
        return_value=_existing_note(
            note_id="an-existing-note", created=_PRIOR_CREATED, modified=_PRIOR_CREATED
        )
    )
    tools, _ = _make_tools(ctx)
    return {tool.name: (lambda _tool=tool, **kw: _invoke(_tool, kw)) for tool in tools}


class TestKbRedactionMarkerGuard:
    """A note read back through a tool result is credential-redacted; writing
    it back from that view would commit `[REDACTED]` over the real value
    (agent.core.redaction_write_guard). The KB write tools refuse, compared
    against the note's current content."""

    def test_kb_write_refuses_to_commit_the_marker(self, kb_tools, materializer):
        result = kb_tools["kb_write"](
            type="learning",
            title="Remote",
            content="clone from https://[REDACTED]@gitea.local/o/r.git",
        )
        assert result.startswith("Error: kb_write refused")
        materializer.assert_not_called()

    @pytest.mark.parametrize("tier", ["kb_update_tools", "kb_update_tools_kgless"])
    @pytest.mark.parametrize(
        "change",
        [
            {"content": "rewritten: https://[REDACTED]@gitea.local/o/r.git"},
            {"append": "also https://[REDACTED]@mirror/x.git"},
        ],
        ids=["content", "append"],
    )
    def test_kb_update_refuses_to_commit_the_marker(
        self, request, tier, change, materializer
    ):
        tools = request.getfixturevalue(tier)
        result = tools["kb_update"](note="an-existing-note", **change)
        assert result.startswith("Error: kb_update refused")
        materializer.assert_not_called()

    def test_a_note_that_already_quotes_the_marker_stays_editable(
        self, ks, materializer
    ):
        ctx = _make_context()
        ctx.knowledge_store = ks
        ctx.knowledge_graph.read_note.return_value = _graph_existing(
            content='The redactor prints "[REDACTED]".'
        )
        ctx.knowledge_graph.update_note.return_value = True
        tools, _ = _make_tools(ctx)
        kb_update = _get_tool(tools, "kb_update")
        result = _invoke(
            kb_update,
            {
                "note": "an-existing-note",
                "content": 'The redactor prints "[REDACTED]" — see OC-05.',
            },
        )
        assert result.startswith("Updated **")


class TestKbUpdateDoesNotWriteTheRow:
    """Slice A leaves exactly one writer of ``knowledge_index``: the
    materialisation endpoint. kb_update's own ``upsert_note`` would clobber
    the chunks/links/stamp the inline index just wrote, and would report a
    projection state the orchestrator — not the agent — now owns."""

    def test_kb_update_never_calls_upsert_note(self, kb_update_tools, ks):
        result = kb_update_tools["kb_update"](
            note="an-existing-note", append="more text"
        )
        assert result.startswith("Updated **")
        ks.upsert_note.assert_not_called()

    def test_kgless_kb_update_never_calls_upsert_note(self, kb_update_tools_kgless, ks):
        result = kb_update_tools_kgless["kb_update"](
            note="an-existing-note", append="more text"
        )
        assert result.startswith("Updated **")
        ks.upsert_note.assert_not_called()

    def test_kb_update_reports_index_state(self, kb_update_tools):
        result = kb_update_tools["kb_update"](
            note="an-existing-note", append="more text"
        )
        assert "indexed=yes" in result
        assert "projection=synced" not in result

    def test_kgless_kb_update_reports_index_state(self, kb_update_tools_kgless):
        result = kb_update_tools_kgless["kb_update"](
            note="an-existing-note", append="more text"
        )
        assert "indexed=yes" in result
        assert "projection=synced" not in result

    def test_a_deferred_index_is_reported_not_hidden(
        self, kb_update_tools, materializer
    ):
        materializer.return_value = {
            "status": "committed",
            "canonical_state": "canonical",
            "indexed": False,
            "index_reason": "oversized",
        }
        result = kb_update_tools["kb_update"](
            note="an-existing-note", append="more text"
        )
        assert result.startswith("Updated **")
        assert "indexed=deferred:oversized" in result

    def test_kgless_deferred_index_is_reported_not_hidden(
        self, kb_update_tools_kgless, materializer
    ):
        materializer.return_value = {
            "status": "committed",
            "canonical_state": "canonical",
            "indexed": False,
            "index_reason": "reindex-running",
        }
        result = kb_update_tools_kgless["kb_update"](
            note="an-existing-note", append="more text"
        )
        assert result.startswith("Updated **")
        assert "indexed=deferred:reindex-running" in result


class TestKbUpdateForwardsRetrievalMessages:
    """The markdown has nowhere to put retrieval messages, so they ride the
    POST. kb_update rewrites the whole note; the row write that used to carry
    them is gone, so without forwarding them here the endpoint sees nothing.

    Asserted at the ``_post_vault_file`` seam — the actual request body —
    rather than at ``_materialize_note``'s arguments."""

    def _update(self, existing):
        ctx = _make_git_context()
        ctx.knowledge_graph.read_note.return_value = existing
        ctx.knowledge_graph.update_note.return_value = True
        tools, _ = _make_tools(ctx)
        patcher, calls = _capture_materialize()
        with patcher, patch("agent.tools.knowledge.knowledge_tools.asyncio"):
            _invoke(
                _get_tool(tools, "kb_update"),
                {"note": "an-existing-note", "append": "more text"},
            )
        return calls

    def test_the_existing_notes_messages_reach_the_endpoint(self):
        calls = self._update(_graph_existing())
        assert calls[0]["retrieval_messages"] == ["why did we pick JWT?"]

    def test_a_note_without_messages_forwards_the_leave_alone_sentinel(self):
        # None, not [] — the better-typed way to say "no opinion", the
        # endpoint reading it as "leave the stored value alone". Note what
        # this does NOT prove: `existing.get(...) or []` would still not blank
        # an earlier write's messages, because `_post_vault_file` drops an
        # empty list from the payload exactly as it drops None. That guard is
        # where the protection lives — see
        # test_omits_retrieval_messages_entirely_when_there_are_none.
        calls = self._update(_graph_existing(retrieval_messages=None))
        assert calls[0]["retrieval_messages"] is None

    def test_storeonly_path_forwards_the_leave_alone_sentinel(self):
        # get_note_by_slug does not read the column, so this tier has no
        # messages of its own to forward. None is what preserves them.
        ctx = _make_gitless_context()
        ctx.knowledge_graph = None
        ctx.knowledge_store.get_note_by_slug = AsyncMock(
            return_value=_existing_note(note_id="an-existing-note")
        )
        ctx.knowledge_store.upsert_note = AsyncMock(return_value=uuid.uuid4())
        tools, _ = _make_tools(ctx)
        patcher, calls = _capture_materialize()
        with patcher:
            _invoke(
                _get_tool(tools, "kb_update"),
                {"note": "an-existing-note", "append": "x"},
            )
        assert calls[0]["retrieval_messages"] is None


class TestRetireDenied:
    """kb_gardening G5 — the retirement guard as a pure function."""

    @staticmethod
    def _row(**over):
        from datetime import datetime, timedelta, timezone

        base = {
            "id": "old-state",
            "type": "state",
            "status": "active",
            "tags": [],
            "ready_at": None,
            "created": datetime.now(timezone.utc) - timedelta(days=3),
        }
        base.update(over)
        return base

    def test_plain_old_nursery_note_may_be_retired(self):
        from agent.tools.knowledge.knowledge_tools import _retire_denied

        assert _retire_denied(self._row(), []) is None

    def test_charter_is_never_retired(self):
        from agent.tools.knowledge.knowledge_tools import _retire_denied

        assert "charter" in _retire_denied(self._row(type="charter"), [])

    @pytest.mark.parametrize("ticket", ["feature", "issue", "idea"])
    def test_tickets_are_closed_not_retired(self, ticket):
        from agent.tools.knowledge.knowledge_tools import _retire_denied

        assert "ticket" in _retire_denied(self._row(type=ticket), [])

    @pytest.mark.parametrize("tag", ["pinned", "ready", "parallel-safe", "Pinned"])
    def test_protected_tags(self, tag):
        from agent.tools.knowledge.knowledge_tools import _retire_denied

        assert "tagged" in _retire_denied(self._row(tags=[tag]), [])

    def test_dispatch_authorised_note(self):
        from agent.tools.knowledge.knowledge_tools import _retire_denied

        assert "dispatch" in _retire_denied(
            self._row(ready_at="2026-09-01T00:00:00Z"), []
        )

    def test_evidence_of_an_active_durable_note(self):
        from agent.tools.knowledge.knowledge_tools import _retire_denied

        denied = _retire_denied(
            self._row(),
            [{"id": "chose-jwt", "type": "decision", "status": "active"}],
        )
        assert "chose-jwt (decision)" in denied

    def test_fresh_note_is_protected_from_a_racing_curator(self):
        from datetime import datetime, timezone

        from agent.tools.knowledge.knowledge_tools import _retire_denied

        assert "24 hours" in _retire_denied(
            self._row(created=datetime.now(timezone.utc)), []
        )

    def test_naive_created_timestamp_is_treated_as_utc(self):
        from datetime import datetime

        from agent.tools.knowledge.knowledge_tools import _retire_denied

        assert "24 hours" in _retire_denied(self._row(created=datetime.utcnow()), [])


class TestKbDelete:
    """kb_delete = tombstone (kb_gardening G1): status archived + reason in the
    note, through the same materialise path as kb_update; refusals name
    their rule; already-archived is a no-op."""

    def _tools(self, existing, inbound=None):
        ctx = _make_gitless_context()
        ctx.knowledge_graph = None
        ctx.knowledge_store.get_note_by_slug = AsyncMock(return_value=existing)
        ctx.knowledge_store.get_inbound_links = AsyncMock(return_value=inbound or [])
        ctx.knowledge_store.upsert_note = AsyncMock(return_value=uuid.uuid4())
        tools, _ = _make_tools(ctx)
        return ctx, tools

    @staticmethod
    def _old(**over):
        from datetime import datetime, timedelta, timezone

        fields = {
            "type": "state",
            "created": datetime.now(timezone.utc) - timedelta(days=5),
            "blob_sha": "c" * 40,
        }
        fields.update(over)
        return _existing_note(
            note_id="old-state", content="Iteration 12 state.", **fields
        )

    def test_is_registered(self):
        ctx, tools = self._tools(self._old())
        assert _get_tool(tools, "kb_delete") is not None

    def test_retires_with_a_reason_and_forwards_the_cas_token(self):
        ctx, tools = self._tools(self._old())
        patcher, calls = _capture_materialize()
        with patcher:
            result = _invoke(
                _get_tool(tools, "kb_delete"),
                {"note": "old-state", "reason": "superseded by iter-13 state note"},
            )
        assert result.startswith("Retired **old-state**")
        assert "kb_update" in result and 'status="active"' in result
        assert len(calls) == 1
        committed = calls[0]["content"]
        assert "status: archived" in committed
        assert "**Retired**" in committed
        assert "superseded by iter-13 state note" in committed
        assert calls[0]["expected_blob_sha"] == "c" * 40
        ctx.knowledge_store.get_inbound_links.assert_awaited_once()
        kwargs = ctx.knowledge_store.get_inbound_links.await_args.kwargs
        assert kwargs["active_only"] is True
        assert "decision" in kwargs["note_types"]

    def test_requires_a_reason(self):
        ctx, tools = self._tools(self._old())
        patcher, calls = _capture_materialize()
        with patcher:
            result = _invoke(
                _get_tool(tools, "kb_delete"), {"note": "old-state", "reason": "x"}
            )
        assert result.startswith("Error")
        assert calls == []

    def test_refuses_when_an_active_decision_links_to_it(self):
        ctx, tools = self._tools(
            self._old(),
            inbound=[{"id": "chose-jwt", "type": "decision", "status": "active"}],
        )
        patcher, calls = _capture_materialize()
        with patcher:
            result = _invoke(
                _get_tool(tools, "kb_delete"),
                {"note": "old-state", "reason": "looks stale to me"},
            )
        assert result.startswith("Refused")
        assert "chose-jwt (decision)" in result
        assert calls == []

    def test_refuses_the_charter(self):
        ctx, tools = self._tools(self._old(type="charter"))
        patcher, calls = _capture_materialize()
        with patcher:
            result = _invoke(
                _get_tool(tools, "kb_delete"),
                {"note": "old-state", "reason": "charter is long and boring"},
            )
        assert result.startswith("Refused")
        assert calls == []

    def test_already_archived_is_a_noop(self):
        ctx, tools = self._tools(self._old(status="archived"))
        patcher, calls = _capture_materialize()
        with patcher:
            result = _invoke(
                _get_tool(tools, "kb_delete"),
                {"note": "old-state", "reason": "duplicate of something"},
            )
        assert "already archived" in result
        assert calls == []

    def test_unknown_note(self):
        ctx, tools = self._tools(None)
        result = _invoke(
            _get_tool(tools, "kb_delete"),
            {"note": "nope", "reason": "duplicate of something"},
        )
        assert result.startswith("Error")

    def test_guard_lookup_failure_fails_closed(self):
        ctx, tools = self._tools(self._old())
        ctx.knowledge_store.get_inbound_links = AsyncMock(
            side_effect=RuntimeError("db")
        )
        patcher, calls = _capture_materialize()
        with patcher:
            result = _invoke(
                _get_tool(tools, "kb_delete"),
                {"note": "old-state", "reason": "duplicate of something"},
            )
        assert result.startswith("Error")
        assert "refusing to retire blind" in result
        assert calls == []

    def test_precondition_failure_surfaces_as_a_re_read_error(self):
        ctx, tools = self._tools(self._old())
        patcher, _calls = _capture_materialize(
            {
                "status": "failed",
                "reason": "precondition-failed",
                "retry_state": "permanent",
            }
        )
        with patcher:
            result = _invoke(
                _get_tool(tools, "kb_delete"),
                {"note": "old-state", "reason": "duplicate of something"},
            )
        assert result.startswith("Error")
        assert "since you read it" in result


class TestKbUpdateForwardsCompareAndSwapToken:
    """kb_gardening G3: every rewrite carries the blob SHA the row was indexed
    from, so the endpoint can refuse a write that would overwrite a
    concurrent edit — or re-create a note that was removed."""

    def test_storeonly_path_sends_the_rows_blob_sha(self):
        ctx = _make_gitless_context()
        ctx.knowledge_graph = None
        ctx.knowledge_store.get_note_by_slug = AsyncMock(
            return_value=_existing_note(note_id="an-existing-note", blob_sha="a" * 40)
        )
        ctx.knowledge_store.upsert_note = AsyncMock(return_value=uuid.uuid4())
        tools, _ = _make_tools(ctx)
        patcher, calls = _capture_materialize()
        with patcher:
            _invoke(
                _get_tool(tools, "kb_update"),
                {"note": "an-existing-note", "append": "x"},
            )
        assert calls[0]["expected_blob_sha"] == "a" * 40

    def test_storeonly_path_writes_unconditionally_while_the_index_is_deferred(self):
        # blob_sha is NULL until the inline index (or the sweep) stamps it;
        # a rewrite then has no token to send and stays last-writer-wins.
        ctx = _make_gitless_context()
        ctx.knowledge_graph = None
        ctx.knowledge_store.get_note_by_slug = AsyncMock(
            return_value=_existing_note(note_id="an-existing-note", blob_sha=None)
        )
        ctx.knowledge_store.upsert_note = AsyncMock(return_value=uuid.uuid4())
        tools, _ = _make_tools(ctx)
        patcher, calls = _capture_materialize()
        with patcher:
            _invoke(
                _get_tool(tools, "kb_update"),
                {"note": "an-existing-note", "append": "x"},
            )
        assert calls[0]["expected_blob_sha"] is None

    def test_graph_path_reads_the_token_from_the_row(self):
        ctx = _make_git_context()
        ctx.knowledge_graph.read_note.return_value = _graph_existing()
        ctx.knowledge_graph.update_note.return_value = True
        ctx.knowledge_store.get_note_by_slug = AsyncMock(
            return_value=_existing_note(note_id="an-existing-note", blob_sha="b" * 40)
        )
        tools, _ = _make_tools(ctx)
        patcher, calls = _capture_materialize()
        with patcher:
            _invoke(
                _get_tool(tools, "kb_update"),
                {"note": "an-existing-note", "append": "more text"},
            )
        assert calls[0]["expected_blob_sha"] == "b" * 40

    def test_precondition_failure_tells_the_agent_to_re_read(self):
        ctx = _make_gitless_context()
        ctx.knowledge_graph = None
        ctx.knowledge_store.get_note_by_slug = AsyncMock(
            return_value=_existing_note(note_id="an-existing-note", blob_sha="a" * 40)
        )
        tools, _ = _make_tools(ctx)
        patcher, _calls = _capture_materialize(
            {
                "status": "failed",
                "reason": "precondition-failed",
                "canonical_state": "failed",
                "retry_state": "permanent",
            }
        )
        with patcher:
            result = _invoke(
                _get_tool(tools, "kb_update"),
                {"note": "an-existing-note", "append": "x"},
            )
        assert result.startswith("Error:")
        assert "changed (or was removed) since you read it" in result
        assert "kb_read" in result


class TestKbUpdateKeepsTheNotesTimestamps:
    """Every kb_update rewrites the whole file. ``created:`` is the only
    carrier of a note's birth date — ``created_at`` is absent from
    ``upsert_kb_note``'s ON CONFLICT list, so a row that ever lands NULL can
    never be repaired — and stripping the line here would take it off before
    any sweep read it."""

    def _graph_ctx(self, **over):
        ctx = _make_git_context()
        ctx.knowledge_graph.read_note.return_value = _graph_existing(**over)
        ctx.knowledge_graph.update_note.return_value = True
        return ctx

    def test_graph_path_carries_created_and_stamps_modified(self):
        ctx = self._graph_ctx()
        tools, _ = _make_tools(ctx)
        patcher, calls = _capture_materialize()
        with patcher, patch("agent.tools.knowledge.knowledge_tools.asyncio"):
            _invoke(
                _get_tool(tools, "kb_update"),
                {"note": "an-existing-note", "append": "more"},
            )
        content = calls[0]["content"]
        assert f"created: {_PRIOR_CREATED}" in content
        assert "\nmodified: " in content
        assert f"modified: {_PRIOR_CREATED}" not in content

    def test_storeonly_path_carries_created_and_stamps_modified(self):
        ctx = _make_gitless_context()
        ctx.knowledge_graph = None
        ctx.knowledge_store.get_note_by_slug = AsyncMock(
            return_value=_existing_note(
                note_id="an-existing-note",
                created=_PRIOR_CREATED,
                modified=_PRIOR_CREATED,
            )
        )
        ctx.knowledge_store.upsert_note = AsyncMock(return_value=uuid.uuid4())
        tools, _ = _make_tools(ctx)
        patcher, calls = _capture_materialize()
        with patcher:
            _invoke(
                _get_tool(tools, "kb_update"),
                {"note": "an-existing-note", "append": "more"},
            )
        content = calls[0]["content"]
        assert f"created: {_PRIOR_CREATED}" in content
        assert "\nmodified: " in content
        assert f"modified: {_PRIOR_CREATED}" not in content

    def test_a_datetime_created_is_serialized_not_repr_dumped(self):
        # get_note_by_slug hands back the tz-aware datetime straight off the
        # row, and Neo4j hands back its own DateTime — both must land as a
        # timestamp the reindexer's YAML parse can read.
        from datetime import datetime as _dt, timezone as _tz

        ctx = self._graph_ctx(created=_dt(2025, 12, 31, 9, 0, tzinfo=_tz.utc))
        tools, _ = _make_tools(ctx)
        patcher, calls = _capture_materialize()
        with patcher, patch("agent.tools.knowledge.knowledge_tools.asyncio"):
            _invoke(
                _get_tool(tools, "kb_update"),
                {"note": "an-existing-note", "append": "more"},
            )
        assert "created: 2025-12-31T09:00:00+00:00" in calls[0]["content"]

    def test_a_non_stdlib_timestamp_is_serialized_through_isoformat(self):
        # `neo4j.time.DateTime` is NOT a `datetime` subclass, so an
        # `isinstance` check would str() it. It does have `.isoformat()`, and
        # this stands in for it — the graph path is the one that reads it.
        class _GraphDateTime:
            def isoformat(self):
                return "2025-06-01T12:00:00.123456789+00:00"

            def __str__(self):  # what a naive implementation would emit
                return "<GraphDateTime object>"

        ctx = self._graph_ctx(created=_GraphDateTime())
        tools, _ = _make_tools(ctx)
        patcher, calls = _capture_materialize()
        with patcher, patch("agent.tools.knowledge.knowledge_tools.asyncio"):
            _invoke(
                _get_tool(tools, "kb_update"),
                {"note": "an-existing-note", "append": "more"},
            )
        assert "created: 2025-06-01T12:00:00.123456789+00:00" in calls[0]["content"]
        assert "GraphDateTime object" not in calls[0]["content"]

        # ...and the reindexer resolves it (YAML truncates to microseconds).
        from orchestrator.services.kb_reindex import note_fields, parse_note_md

        fm, body = parse_note_md(calls[0]["content"])
        created_at = note_fields("knowledge/n.md", fm, body)["created_at"]
        assert created_at is not None
        assert created_at.isoformat() == "2025-06-01T12:00:00.123456+00:00"

    def test_a_note_with_no_creation_time_gains_no_created_line(self):
        ctx = self._graph_ctx(created=None)
        tools, _ = _make_tools(ctx)
        patcher, calls = _capture_materialize()
        with patcher, patch("agent.tools.knowledge.knowledge_tools.asyncio"):
            _invoke(
                _get_tool(tools, "kb_update"),
                {"note": "an-existing-note", "append": "more"},
            )
        assert "\ncreated:" not in calls[0]["content"]
        assert "\nmodified: " in calls[0]["content"]

    def test_the_timestamps_survive_the_reindexers_parse(self):
        # The whole point: the row is rebuilt from these bytes. A format the
        # reindexer cannot read indexes fine and silently stores NULL.
        from orchestrator.services.kb_reindex import note_fields, parse_note_md

        ctx = self._graph_ctx()
        tools, _ = _make_tools(ctx)
        patcher, calls = _capture_materialize()
        with patcher, patch("agent.tools.knowledge.knowledge_tools.asyncio"):
            _invoke(
                _get_tool(tools, "kb_update"),
                {"note": "an-existing-note", "append": "more"},
            )
        fm, body = parse_note_md(calls[0]["content"])
        fields = note_fields("knowledge/an-existing-note.md", fm, body)
        assert fields["created_at"] is not None
        assert fields["created_at"].isoformat() == _PRIOR_CREATED
        assert fields["modified_at"] is not None
        assert fields["modified_at"] > fields["created_at"]


class TestKbUpdateErrorPathsDoNotOverclaim:
    """The optional-graph-projection failure used to open "'<slug>' is
    canonical and searchable". With an index that can defer, that is a claim
    the tool cannot make — it has to report the state the endpoint returned."""

    def _graph_failure(self, materialization):
        ctx = _make_context()
        ctx.knowledge_graph.read_note.return_value = _graph_existing()
        ctx.knowledge_graph.update_note.side_effect = RuntimeError("neo4j down")
        tools, _ = _make_tools(ctx)
        with patch(
            "agent.tools.knowledge.knowledge_tools._materialize_note",
            return_value=materialization,
        ):
            return _invoke(
                _get_tool(tools, "kb_update"),
                {"note": "an-existing-note", "append": "x"},
            )

    def test_graph_failure_reports_the_real_index_state(self):
        result = self._graph_failure(
            {
                "status": "committed",
                "canonical_state": "canonical",
                "indexed": False,
                "index_reason": "index-error",
            }
        )
        assert result.startswith("Error:")
        assert "graph projection failed" in result
        assert "searchable" not in result
        assert "indexed=deferred:index-error" in result

    def test_an_indexed_note_still_says_so_on_an_error_path(self):
        # The negative control for the reword: dropping the claim without
        # reporting the state would pass the test above and fail this one.
        result = self._graph_failure(
            {
                "status": "committed",
                "canonical_state": "canonical",
                "indexed": True,
                "index_reason": None,
            }
        )
        assert result.startswith("Error:")
        assert "searchable" not in result
        assert "indexed=yes" in result


class TestKbUpdateMaterialization:
    """kb_update re-materializes the note server-side, on both update paths."""

    def _kg_context(self):
        ctx = _make_git_context()
        ctx.knowledge_graph.update_note.return_value = True
        ctx.knowledge_graph.read_note.return_value = {
            "id": "n1",
            "title": "T",
            "type": "decision",
            "content": "updated body",
            "status": "active",
            "relationships": [{"type": "SUPPORTS", "target": "a"}],
        }
        return ctx

    def test_graph_path_materializes_the_updated_note(self):
        ctx = self._kg_context()
        tools, _ = _make_tools(ctx)

        patcher, calls = _capture_materialize()
        with patcher, patch("agent.tools.knowledge.knowledge_tools.asyncio"):
            _invoke(
                _get_tool(tools, "kb_update"),
                {"note": "n1", "content": "updated body"},
            )

        assert len(calls) == 1
        assert calls[0]["slug"] == "n1"
        assert calls[0]["project_id"] == ctx.project_id
        assert "updated body" in calls[0]["content"]
        assert "[a](a.md)" in calls[0]["content"]
        ctx.workspace_manager.write_file.assert_not_called()

    def test_graph_path_materializes_without_a_workspace_or_git(self):
        ctx = _make_gitless_context()
        ctx.knowledge_graph.update_note.return_value = True
        ctx.knowledge_graph.read_note.return_value = {
            "id": "n1",
            "title": "T",
            "content": "x",
        }
        tools, _ = _make_tools(ctx)

        patcher, calls = _capture_materialize()
        with patcher, patch("agent.tools.knowledge.knowledge_tools.asyncio"):
            result = _invoke(
                _get_tool(tools, "kb_update"),
                {"note": "n1", "content": "x"},
            )
        assert [c["slug"] for c in calls] == ["n1"]
        assert "Updated" in result

    def test_storeonly_path_materializes_the_updated_note(self):
        # The kg-less update path (_update_existing_kgless) — the third call
        # site, and the one a lite tier actually takes.
        ctx = _make_gitless_context()
        ctx.knowledge_graph = None
        ctx.knowledge_store.get_note_by_slug = AsyncMock(
            return_value={
                "id": "n1",
                "title": "T",
                "type": "decision",
                "content": "old",
                "status": "active",
                "tags": [],
                "keywords": [],
                "priority": 1,
            }
        )
        ctx.knowledge_store.upsert_note = AsyncMock(return_value=uuid.uuid4())
        tools, _ = _make_tools(ctx)

        patcher, calls = _capture_materialize()
        with patcher:
            result = _invoke(
                _get_tool(tools, "kb_update"),
                {"note": "n1", "content": "brand new body"},
            )
        assert [c["slug"] for c in calls] == ["n1"]
        assert "brand new body" in calls[0]["content"]
        assert "Updated" in result

    def test_materialization_failure_fails_closed(self):
        ctx = self._kg_context()
        tools, _ = _make_tools(ctx)

        patcher, calls = _capture_materialize(
            {"status": "failed", "reason": "commit-error"}
        )
        with patcher, patch("agent.tools.knowledge.knowledge_tools.asyncio"):
            result = _invoke(
                _get_tool(tools, "kb_update"),
                {"note": "n1", "content": "x"},
            )
        assert len(calls) == 1
        assert result.startswith("Error: canonical knowledge write")
        ctx.knowledge_graph.update_note.assert_not_called()


# =============================================================================
# kb_write ingestion verdict gate (slice 2 PR2)
# =============================================================================

import hashlib as _hashlib  # noqa: E402
from types import SimpleNamespace  # noqa: E402

from shared.runtime.services.auxiliary import KnowledgeVerdict  # noqa: E402
from agent.services.knowledge.ingestion import KnowledgeVerdictService  # noqa: E402


class _GateAux:
    """Aux stub whose chain() returns a fixed KnowledgeVerdict."""

    def __init__(self, verdict):
        self._verdict = verdict

    async def chain(self, task, timeout=None):
        return self._verdict


class _GateStore:
    """KnowledgeStore stub for the gate: async embed + find_similar_many + upsert."""

    def __init__(self, neighbours):
        self._neighbours = neighbours
        self.embedding_service = SimpleNamespace(embed=self._embed)
        self.upsert_note = AsyncMock(return_value=uuid.uuid4())

    async def _embed(self, text):
        return [0.1, 0.2, 0.3]

    async def find_similar_many(self, project_id, embedding, k=5, min_similarity=0.6):
        return self._neighbours


def _kb_neighbour(note_id, content, title="N"):
    return SimpleNamespace(
        note_id=note_id,
        content=content,
        title=title,
        similarity=0.85,
        created_at=None,
        content_hash=_hashlib.sha256(content.encode()).hexdigest(),
    )


def _gated_tools(kg, ks, verdict, prompt="ADJUDICATE"):
    """Build kb tools with a real verdict service (asyncio NOT mocked)."""
    ctx = MagicMock()
    ctx.project_id = str(uuid.uuid4())
    ctx.project_ids = [ctx.project_id]
    ctx.job_id = str(uuid.uuid4())
    ctx.config = {"current_phase": 1}
    ctx.knowledge_graph = kg
    ctx.knowledge_store = ks
    ctx.has_git.return_value = False  # skip dual-write for isolation
    service = KnowledgeVerdictService(
        _GateAux(verdict), SimpleNamespace(verdict_top_k=5, review_floor=0.6)
    )
    tools = create_kb_tools(ctx, verdict_service=service, verdict_prompt=prompt)
    return tools, ctx


class TestKbWriteVerdictGate:
    def _write(self, tools, **kw):
        args = {"title": "New Note", "type": "decision", "content": "some content"}
        args.update(kw)
        return _invoke(_get_tool(tools, "kb_write"), args)

    def test_add_when_no_neighbours(self):
        kg = MagicMock()
        kg.create_note.return_value = "new-slug"
        ks = _GateStore(neighbours=[])
        tools, _ = _gated_tools(kg, ks, KnowledgeVerdict(action="ADD", reason="new"))
        result = self._write(tools)
        assert "Created" in result
        kg.create_note.assert_called_once()

    def test_discard_exact_duplicate_skips_write(self):
        kg = MagicMock()
        # neighbour content hashes to the candidate's content → content-hash prefilter
        ks = _GateStore(neighbours=[_kb_neighbour("dup-note", "some content")])
        tools, _ = _gated_tools(kg, ks, KnowledgeVerdict(action="ADD", reason="unused"))
        result = self._write(tools, content="some content")
        assert "DISCARD" in result
        assert "dup-note" in result
        kg.create_note.assert_not_called()

    def test_update_redirects_edit_onto_target(self):
        kg = MagicMock()
        kg.update_note.return_value = True
        # Faithful mock: the candidate slug ("new-note") does NOT pre-exist, so
        # the exact-dup pre-check is skipped and the gate runs; the UPDATE
        # target ("old-note") re-reads the dict inside _update_existing.
        kg.read_note.side_effect = lambda pid, nid: (
            {
                "type": "decision",
                "title": "Old",
                "content": "new content",
                "status": "active",
            }
            if nid == "old-note"
            else None
        )
        ks = _GateStore(neighbours=[_kb_neighbour("old-note", "different old text")])
        tools, _ = _gated_tools(
            kg, ks, KnowledgeVerdict(action="UPDATE", target_indices=[1], reason="fix")
        )
        result = self._write(tools, content="new content")
        assert "Updated" in result and "old-note" in result
        kg.create_note.assert_not_called()
        kg.update_note.assert_called_once()

    def test_supersede_creates_then_retires_target(self):
        kg = MagicMock()
        kg.create_note.return_value = "new-slug"
        kg.update_note.return_value = True
        kg.read_note.return_value = {
            "type": "decision",
            "title": "Old",
            "content": "old",
            "status": "superseded",
        }
        ks = _GateStore(neighbours=[_kb_neighbour("stale-note", "different old text")])
        tools, _ = _gated_tools(
            kg,
            ks,
            KnowledgeVerdict(action="SUPERSEDE", target_indices=[1], reason="replaced"),
        )
        result = self._write(tools, content="fresh content")
        assert "Created" in result and "superseded" in result
        kg.create_note.assert_called_once()
        # retire call: status=superseded on the stale note
        retire_kwargs = kg.update_note.call_args.kwargs
        assert retire_kwargs.get("status") == "superseded"
        assert retire_kwargs.get("note_id") == "stale-note"

    def test_no_service_writes_ungated(self):
        # Sanity: the default (no verdict service) path is unchanged.
        kg = MagicMock()
        kg.create_note.return_value = "slug"
        ks = _GateStore(neighbours=[_kb_neighbour("x", "some content")])
        ctx = MagicMock()
        ctx.project_id = str(uuid.uuid4())
        ctx.job_id = str(uuid.uuid4())
        ctx.config = {"current_phase": 1}
        ctx.knowledge_graph = kg
        ctx.knowledge_store = ks
        ctx.has_git.return_value = False
        tools = create_kb_tools(ctx)  # no verdict service
        result = _invoke(
            _get_tool(tools, "kb_write"),
            {"title": "N", "type": "decision", "content": "some content"},
        )
        assert "Created" in result
        kg.create_note.assert_called_once()


class TestKbWriteErrorPathsDoNotOverclaim:
    """An error return may not assert searchability the index state denies.

    Both of these used to open with "'<slug>' is canonical and searchable" —
    the same promise the docstring made before Slice A, made in the same
    place and wrong for the same reason: whether the note is searchable is
    the orchestrator's answer to give, and it is sometimes "not yet".
    """

    def test_graph_failure_reports_the_real_index_state(self):
        tools, ctx = _make_tools()
        ctx.knowledge_graph.read_note.return_value = None
        ctx.knowledge_graph.create_note.side_effect = RuntimeError("neo4j down")
        with patch(
            "agent.tools.knowledge.knowledge_tools._materialize_note",
            return_value={
                "status": "committed",
                "canonical_state": "canonical",
                "indexed": False,
                "index_reason": "oversized",
            },
        ):
            result = _invoke(
                _get_tool(tools, "kb_write"),
                {"title": "T", "type": "decision", "content": "x"},
            )
        assert result.startswith("Error:")
        assert "optional graph projection failed" in result
        assert "searchable" not in result
        assert "indexed=deferred:oversized" in result

    def test_supersede_failure_reports_the_real_index_state(self):
        kg = MagicMock()
        kg.create_note.return_value = "new-slug"
        kg.update_note.side_effect = RuntimeError("retire failed")
        kg.read_note.return_value = {
            "type": "decision",
            "title": "Old",
            "content": "old",
            "status": "active",
        }
        ks = _GateStore(neighbours=[_kb_neighbour("stale-note", "different old text")])
        tools, _ = _gated_tools(
            kg,
            ks,
            KnowledgeVerdict(action="SUPERSEDE", target_indices=[1], reason="replaced"),
        )
        with patch(
            "agent.tools.knowledge.knowledge_tools._materialize_note",
            return_value={
                "status": "committed",
                "canonical_state": "canonical",
                "indexed": False,
                "index_reason": "reindex-running",
            },
        ):
            result = _invoke(
                _get_tool(tools, "kb_write"),
                {"title": "New Note", "type": "decision", "content": "fresh content"},
            )
        assert result.startswith("Error:")
        assert "SUPERSEDE disposition did not converge" in result
        # Only kb_write's own clause is in scope. The retire failures quoted
        # after the colon are kb_update's message, and kb_update no longer
        # writes its own row either — it routes through the same canonical
        # write, so its wording is that path's to state, not this test's.
        own_clause, _, quoted_failures = result.partition("did not converge:")
        assert "searchable" not in own_clause
        assert quoted_failures  # the failures really were quoted, not empty
        assert "indexed=deferred:reindex-running" in result

    def test_an_indexed_note_still_says_so_on_an_error_path(self):
        # The reword must not flip the other way: when the orchestrator did
        # index the note, an unrelated failure should still say so.
        tools, ctx = _make_tools()
        ctx.knowledge_graph.read_note.return_value = None
        ctx.knowledge_graph.create_note.side_effect = RuntimeError("neo4j down")
        with patch(
            "agent.tools.knowledge.knowledge_tools._materialize_note",
            return_value={
                "status": "committed",
                "canonical_state": "canonical",
                "indexed": True,
                "index_reason": None,
            },
        ):
            result = _invoke(
                _get_tool(tools, "kb_write"),
                {"title": "T", "type": "decision", "content": "x"},
            )
        assert "indexed=yes" in result


class TestKbWriteSlugDedup:
    """Exact-content slug-collision no-op (Step 1 hardening, docs §11.1).

    A same-title write whose body is byte-identical to the existing note is a
    pure no-op for every writer (loop agents included) — it must skip the gate
    and the create entirely, killing the run-8 twin-file duplication at source.
    """

    def test_exact_duplicate_content_is_noop(self):
        tools, ctx = _make_tools()
        kg = ctx.knowledge_graph
        kg.read_note.return_value = {
            "content": "body",
            "type": "decision",
            "title": "Test",
        }
        with patch("agent.tools.knowledge.knowledge_tools.asyncio"):
            result = _invoke(
                _get_tool(tools, "kb_write"),
                {"title": "Test", "type": "decision", "content": "body"},
            )
        kg.create_note.assert_not_called()
        assert "test" in result.lower()  # references the existing slug
        assert (
            "exist" in result.lower()
            or "no-op" in result.lower()
            or "no change" in result.lower()
        )

    def test_different_content_same_title_still_creates(self):
        tools, ctx = _make_tools()
        kg = ctx.knowledge_graph
        kg.read_note.return_value = {
            "content": "OLD body",
            "type": "decision",
            "title": "Test",
        }
        kg.create_note.return_value = "test-abc123"
        ctx.knowledge_store.upsert_note = AsyncMock(return_value=uuid.uuid4())
        with patch("agent.tools.knowledge.knowledge_tools.asyncio"):
            _invoke(
                _get_tool(tools, "kb_write"),
                {"title": "Test", "type": "decision", "content": "NEW body"},
            )
        kg.create_note.assert_called_once()

    def test_no_collision_creates_normally(self):
        tools, ctx = _make_tools()
        kg = ctx.knowledge_graph
        kg.read_note.return_value = None
        kg.create_note.return_value = "fresh-slug"
        ctx.knowledge_store.upsert_note = AsyncMock(return_value=uuid.uuid4())
        with patch("agent.tools.knowledge.knowledge_tools.asyncio"):
            result = _invoke(
                _get_tool(tools, "kb_write"),
                {"title": "Fresh", "type": "learning", "content": "x"},
            )
        kg.create_note.assert_called_once()
        assert "fresh" in result
        assert ctx.knowledge_graph.create_note.call_args.kwargs["note_id"] == "fresh"


# =============================================================================
# kb_lint — embedding-backed near-duplicate pass (slice-2 owed rule)
# =============================================================================


_KB_LINT_TWIN_ROWS = [
    _store_row("n1", "learning", "# N1\n\n[n2](n2.md)\n"),
    _store_row("n2", "learning", "# N2\n\n[n1](n1.md)\n"),
]


class TestKbLintNearDuplicates:
    def _ctx(self, rows):
        ctx = _make_context()
        ctx.has_workspace.return_value = False
        _fake_kb_store(ctx, rows)
        return ctx

    def test_reports_near_duplicate_pairs_from_index(self):
        ctx = self._ctx(_KB_LINT_TWIN_ROWS)
        ctx.knowledge_store.find_near_duplicate_pairs = AsyncMock(
            return_value=[("n1", "n2", 0.95)]
        )
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_lint"), {})
        assert "near-duplicate" in result
        assert "95" in result

    def test_index_pairs_outside_vault_ignored(self):
        ctx = self._ctx(_KB_LINT_TWIN_ROWS)
        ctx.knowledge_store.find_near_duplicate_pairs = AsyncMock(
            return_value=[("ghost-a", "ghost-b", 0.99)]
        )
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_lint"), {})
        assert "near-duplicate" not in result

    def test_store_error_is_non_fatal(self):
        # The deterministic report must stand alone when pgvector is down.
        ctx = self._ctx(_KB_LINT_TWIN_ROWS)
        ctx.knowledge_store.find_near_duplicate_pairs = AsyncMock(
            side_effect=RuntimeError("pgvector down")
        )
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_lint"), {})
        assert "kb_lint:" in result
        assert "near-duplicate" not in result

    def test_applies_0_97_near_duplicate_floor(self):
        # D-1: the 07-05 lint-policy decision (raise 0.9→0.97) lives at this
        # call site — the store default (0.9) is unusable lint noise (307 pairs
        # vs 7 at 0.97 on the live KB). The lint policy owns its floor.
        ctx = self._ctx(_KB_LINT_TWIN_ROWS)
        ctx.knowledge_store.find_near_duplicate_pairs = AsyncMock(return_value=[])
        tools, _ = _make_tools(ctx)
        _invoke(_get_tool(tools, "kb_lint"), {})
        _, kwargs = ctx.knowledge_store.find_near_duplicate_pairs.call_args
        assert kwargs.get("min_similarity") == 0.97


# =============================================================================
# kb_lint — opt-in dead-external-URL sweep
# =============================================================================


_KB_LINT_URL_ROWS = [
    _store_row("n1", "source", "# N1\n\n[docs](https://gone.example/x) [n2](n2.md)\n"),
    _store_row("n2", "source", "# N2\n\n[n1](n1.md)\n"),
]


class TestKbLintUrlSweep:
    def _ctx(self, rows):
        ctx = _make_context()
        ctx.has_workspace.return_value = False
        _fake_kb_store(ctx, rows)
        ctx.knowledge_store.find_near_duplicate_pairs = AsyncMock(return_value=[])
        return ctx

    def test_off_by_default_no_network(self):
        ctx = self._ctx(_KB_LINT_URL_ROWS)
        tools, _ = _make_tools(ctx)
        with patch(
            "agent.tools.knowledge.knowledge_tools._check_external_url"
        ) as checker:
            result = _invoke(_get_tool(tools, "kb_lint"), {})
        checker.assert_not_called()
        assert "dead-external-url" not in result

    def test_flags_dead_url_when_enabled(self):
        ctx = self._ctx(_KB_LINT_URL_ROWS)
        tools, _ = _make_tools(ctx)
        with patch(
            "agent.tools.knowledge.knowledge_tools._check_external_url",
            return_value="HTTP 404",
        ):
            result = _invoke(_get_tool(tools, "kb_lint"), {"check_urls": True})
        assert "dead-external-url" in result
        assert "gone.example" in result

    def test_alive_url_not_flagged(self):
        ctx = self._ctx(_KB_LINT_URL_ROWS)
        tools, _ = _make_tools(ctx)
        with patch(
            "agent.tools.knowledge.knowledge_tools._check_external_url",
            return_value=None,
        ):
            result = _invoke(_get_tool(tools, "kb_lint"), {"check_urls": True})
        assert "dead-external-url" not in result

    def test_cap_is_loud(self):
        rows = [
            _store_row(
                f"n{i}",
                "source",
                f"# N{i}\n\n[u](https://example.com/{i}) [n0](n0.md)\n",
            )
            for i in range(30)
        ]
        ctx = self._ctx(rows)
        tools, _ = _make_tools(ctx)
        with patch(
            "agent.tools.knowledge.knowledge_tools._check_external_url",
            return_value=None,
        ) as checker:
            result = _invoke(_get_tool(tools, "kb_lint"), {"check_urls": True})
        assert checker.call_count == 25
        assert "url-sweep-truncated" in result


# =============================================================================
# Graph tools without Neo4j — the Full-tier 1-hop degrade (slice-3 PR4c)
# =============================================================================


def _make_context_no_kg():
    """A ToolContext with no knowledge_graph (Neo4j disabled), store present."""
    ctx = _make_context()
    ctx.knowledge_graph = None
    return ctx


class TestKbToolsWithoutNeo4j:
    """When Neo4j is absent, kb_related degrades to the knowledge_links 1-hop
    query; the genuinely graph-shaped tools (contradictions / provenance /
    unanswered / export) degrade honestly to a Graph-tier message rather than
    fabricating results from the generic link table."""

    def test_kb_related_uses_link_table_when_no_kg(self):
        ctx = _make_context_no_kg()
        ctx.knowledge_store.get_related_notes.return_value = [
            {
                "id": "note-b",
                "title": "B",
                "type": "decision",
                "status": "active",
                "distance": 1,
                "rel_types": ["references"],
            }
        ]
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_related"), {"note": "note-a"})
        ctx.knowledge_store.get_related_notes.assert_called_once()
        assert "note-b" in result

    def test_kb_related_no_neighbours_message(self):
        ctx = _make_context_no_kg()
        ctx.knowledge_store.get_related_notes.return_value = []
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_related"), {"note": "note-a"})
        assert "No related notes" in result

    def test_kb_contradictions_degrades_to_graph_tier_message(self):
        ctx = _make_context_no_kg()
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_contradictions"), {})
        assert "Graph tier" in result

    def test_kb_provenance_degrades_to_graph_tier_message(self):
        ctx = _make_context_no_kg()
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_provenance"), {"note": "note-a"})
        assert "Graph tier" in result

    def test_kb_unanswered_degrades_to_graph_tier_message(self):
        ctx = _make_context_no_kg()
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_unanswered"), {})
        assert "Graph tier" in result

    def test_kb_export_degrades_to_graph_tier_message(self):
        ctx = _make_context_no_kg()
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_export"), {"path": "export"})
        assert "Graph tier" in result

    def test_kb_read_uses_store_when_no_kg(self):
        ctx = _make_context_no_kg()
        ctx.knowledge_store.get_note_by_slug.return_value = {
            "id": "n1",
            "title": "T",
            "type": "decision",
            "status": "active",
            "content": "the body",
            "confidence": None,
            "tags": [],
            "keywords": [],
            "job_id": None,
            "phase": None,
            "created": None,
            "modified": None,
        }
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_read"), {"note": "n1"})
        ctx.knowledge_store.get_note_by_slug.assert_called_once()
        assert "the body" in result

    def test_kb_read_not_found_when_no_kg(self):
        ctx = _make_context_no_kg()
        ctx.knowledge_store.get_note_by_slug.return_value = None
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_read"), {"note": "nope"})
        assert "not found" in result

    def test_kb_list_uses_store_when_no_kg(self):
        ctx = _make_context_no_kg()
        ctx.knowledge_store.list_notes.return_value = [
            {
                "id": "n1",
                "title": "T",
                "type": "decision",
                "status": "active",
                "confidence": None,
            }
        ]
        tools, _ = _make_tools(ctx)
        result = _invoke(_get_tool(tools, "kb_list"), {})
        ctx.knowledge_store.list_notes.assert_called_once()
        assert "n1" in result


def _existing_note(note_id="n1", content="old body", **over):
    """A get_note_by_slug-shaped dict for the kg-less write-path tests."""
    base = {
        "id": note_id,
        "title": "T",
        "type": "decision",
        "status": "active",
        "content": content,
        "confidence": None,
        "tags": [],
        "keywords": [],
        "job_id": None,
        "phase": None,
        "created": None,
        "modified": None,
    }
    base.update(over)
    return base


class TestKbWriteWithoutNeo4j:
    """kg-less kb_write (PR4c-3 write half): the canonical OKF file is the
    write target and the orchestrator projects it into pgvector; Neo4j is
    never touched. Since Slice A the agent makes exactly one write, so
    ``_capture_materialize`` sees every note the tool writes — and every note
    it refuses to."""

    def test_no_kg_writes_the_note_through_the_canonical_seam(self):
        ctx = _make_context_no_kg()
        ctx.knowledge_store.get_note_by_slug.return_value = None  # no collision
        tools, _ = _make_tools(ctx)
        patcher, calls = _capture_materialize()
        with patcher:
            result = _invoke(
                _get_tool(tools, "kb_write"),
                {"title": "New Title", "type": "decision", "content": "body"},
            )
        assert [c["slug"] for c in calls] == ["new-title"]
        assert "body" in calls[0]["content"]
        assert "new-title" in result

    def test_no_kg_never_calls_neo4j(self):
        # kg is None — there is nothing to call; the guard must not dereference it.
        ctx = _make_context_no_kg()
        ctx.knowledge_store.get_note_by_slug.return_value = None
        tools, _ = _make_tools(ctx)
        result = _invoke(
            _get_tool(tools, "kb_write"),
            {"title": "Some Note", "type": "learning", "content": "x"},
        )
        assert "Error" not in result

    def test_no_kg_exact_duplicate_short_circuits(self):
        ctx = _make_context_no_kg()
        ctx.knowledge_store.get_note_by_slug.return_value = _existing_note(
            note_id="new-title", content="identical"
        )
        # Explicit: the short-circuit is only correct when the note is already
        # searchable. The unindexed case is a repair, covered below.
        ctx.knowledge_store.note_is_indexed.return_value = True
        tools, _ = _make_tools(ctx)
        patcher, calls = _capture_materialize()
        with patcher:
            result = _invoke(
                _get_tool(tools, "kb_write"),
                {"title": "New Title", "type": "decision", "content": "identical"},
            )
        assert "identical content" in result
        assert calls == []

    def test_no_kg_exact_duplicate_reports_index_state(self):
        """The no-op still has to answer "is it searchable?".

        Slice A's contract is that a write result always states the index
        state; this path used to return a bare sentence with no suffix, so an
        author could not tell a healthy note from a deferred one.
        """
        ctx = _make_context_no_kg()
        ctx.knowledge_store.get_note_by_slug.return_value = _existing_note(
            note_id="new-title", content="identical"
        )
        ctx.knowledge_store.note_is_indexed.return_value = True
        tools, _ = _make_tools(ctx)
        patcher, _calls = _capture_materialize()
        with patcher:
            result = _invoke(
                _get_tool(tools, "kb_write"),
                {"title": "New Title", "type": "decision", "content": "identical"},
            )
        assert "indexed=yes" in result

    def test_no_kg_exact_duplicate_repairs_a_deferred_index(self):
        """Re-writing identical content is the documented repair for a note
        whose first write deferred its index — so it must not be swallowed.

        ``kb_materialize._is_canonical`` admits ``skipped/unchanged`` purely so
        this retry re-indexes. Returning early here made that unreachable from
        ``kb_write``: the note stayed unsearchable until the next sweep and the
        author got no signal at all.
        """
        ctx = _make_context_no_kg()
        ctx.knowledge_store.get_note_by_slug.return_value = _existing_note(
            note_id="new-title", content="identical"
        )
        ctx.knowledge_store.note_is_indexed.return_value = False
        tools, _ = _make_tools(ctx)
        patcher, calls = _capture_materialize()
        with patcher:
            _invoke(
                _get_tool(tools, "kb_write"),
                {"title": "New Title", "type": "decision", "content": "identical"},
            )
        assert calls, "unindexed duplicate must re-materialise, not no-op"
        assert calls[0]["slug"] == "new-title"

    def test_no_kg_exact_duplicate_assumes_indexed_when_probe_fails(self):
        """A failing probe must not invent a write. Degrade to the old no-op."""
        ctx = _make_context_no_kg()
        ctx.knowledge_store.get_note_by_slug.return_value = _existing_note(
            note_id="new-title", content="identical"
        )
        ctx.knowledge_store.note_is_indexed.side_effect = RuntimeError("index down")
        tools, _ = _make_tools(ctx)
        patcher, calls = _capture_materialize()
        with patcher:
            result = _invoke(
                _get_tool(tools, "kb_write"),
                {"title": "New Title", "type": "decision", "content": "identical"},
            )
        assert "identical content" in result
        assert calls == []

    def test_no_kg_collision_appends_content_hash(self):
        import hashlib

        ctx = _make_context_no_kg()
        # Base slug taken by DIFFERENT content -> deterministic hash-suffixed fork.
        ctx.knowledge_store.get_note_by_slug.return_value = _existing_note(
            note_id="new-title", content="other body"
        )
        tools, _ = _make_tools(ctx)
        patcher, calls = _capture_materialize()
        with patcher:
            _invoke(
                _get_tool(tools, "kb_write"),
                {"title": "New Title", "type": "decision", "content": "fresh body"},
            )
        digest = hashlib.sha256(b"fresh body").hexdigest()[:6]
        assert [c["slug"] for c in calls] == [f"new-title-{digest}"]

    def test_no_kg_invalid_type_errors(self):
        # Parity with kg.create_note: an invalid note_type is a clean up-front
        # error, not a misleading "Created" after a silent DB CHECK failure.
        # `type` is now Literal-typed, so the schema rejects it before the body
        # runs — a stronger guarantee than the error string this used to get,
        # and the model can no longer emit the bad value at all.
        ctx = _make_context_no_kg()
        ctx.knowledge_store.get_note_by_slug.return_value = None
        tools, _ = _make_tools(ctx)
        patcher, calls = _capture_materialize()
        with patcher:
            with pytest.raises(ValidationError):
                _invoke(
                    _get_tool(tools, "kb_write"),
                    {"title": "New Title", "type": "bogus", "content": "body"},
                )
            assert calls == []

            # ...and the body still refuses it for callers that skip the schema.
            result = _invoke_unvalidated(
                _get_tool(tools, "kb_write"),
                title="New Title",
                type="bogus",
                content="body",
            )
            assert "Error" in result
            assert calls == []

    def test_no_kg_invalid_confidence_errors(self):
        ctx = _make_context_no_kg()
        ctx.knowledge_store.get_note_by_slug.return_value = None
        tools, _ = _make_tools(ctx)
        patcher, calls = _capture_materialize()
        with patcher:
            with pytest.raises(ValidationError):
                _invoke(
                    _get_tool(tools, "kb_write"),
                    {
                        "title": "New Title",
                        "type": "decision",
                        "content": "body",
                        "confidence": "bogus",
                    },
                )
            assert calls == []

            result = _invoke_unvalidated(
                _get_tool(tools, "kb_write"),
                title="New Title",
                type="decision",
                content="body",
                confidence="bogus",
            )
            assert "Error" in result
            assert calls == []

    def test_no_kg_materializes_the_okf_note(self):
        ws, writes = _capture_workspace()
        ctx = _make_context_no_kg()
        ctx.workspace_manager = ws
        ctx.has_git.return_value = True
        ctx.knowledge_store.get_note_by_slug.return_value = None
        tools, _ = _make_tools(ctx)
        patcher, calls = _capture_materialize()
        with patcher:
            _invoke(
                _get_tool(tools, "kb_write"),
                {"title": "New Title", "type": "decision", "content": "body"},
            )
        assert [c["slug"] for c in calls] == ["new-title"]
        assert writes == {}  # the file is a server-side commit, not a workspace write

    def test_no_kg_empty_slug_falls_back_deterministically(self):
        ctx = _make_context_no_kg()
        ctx.knowledge_store.get_note_by_slug.return_value = None
        tools, _ = _make_tools(ctx)
        patcher, calls = _capture_materialize()
        with patcher:
            _invoke(
                _get_tool(tools, "kb_write"),
                {"title": "!!!", "type": "learning", "content": "body"},
            )
        assert calls[0]["slug"].startswith("note-")


def _canonical_note_fields(calls):
    """The last note written, parsed back by the reindexer's own mapper.

    kb_update's only write is the canonical file now (Slice A — the
    materialisation endpoint owns the row it indexes from that commit), so
    the file is where a mutation has to be observed. Round-tripping it
    through the code that actually turns those bytes into the row is a
    stronger check than the ``upsert_note`` kwargs this replaced.
    """
    from orchestrator.services.kb_reindex import note_fields, parse_note_md

    entry = calls[-1]
    fm, body = parse_note_md(entry["content"])
    return note_fields(f"knowledge/{entry['slug']}.md", fm, body)


class TestKbUpdateWithoutNeo4j:
    """kg-less _update_existing (via kb_update): read the current row from the
    store, apply the mutation in Python, write it back as the canonical OKF
    file — which the orchestrator commits and indexes."""

    def _update(self, existing, args):
        ctx = _make_context_no_kg()
        ctx.knowledge_store.get_note_by_slug.return_value = existing
        tools, _ = _make_tools(ctx)
        patcher, calls = _capture_materialize()
        with patcher:
            result = _invoke(_get_tool(tools, "kb_update"), {"note": "n1", **args})
        return result, calls

    def test_no_kg_replaces_content(self):
        result, calls = self._update(
            _existing_note(content="old body"), {"content": "new body"}
        )
        assert "new body" in calls[0]["content"]
        assert "old body" not in calls[0]["content"]
        assert "content replaced" in result

    def test_no_kg_not_found(self):
        result, calls = self._update(None, {"content": "x"})
        assert "not found" in result
        assert calls == []

    def test_no_kg_appends_content(self):
        _, calls = self._update(_existing_note(content="old body"), {"append": "more"})
        assert "old body" in calls[0]["content"]
        assert "more" in calls[0]["content"]

    def test_no_kg_changes_status(self):
        result, calls = self._update(_existing_note(), {"status": "superseded"})
        assert _canonical_note_fields(calls)["status"] == "superseded"
        assert "status → superseded" in result

    def test_no_kg_invalid_status_errors(self):
        ctx = _make_context_no_kg()
        ctx.knowledge_store.get_note_by_slug.return_value = _existing_note()
        tools, _ = _make_tools(ctx)
        patcher, calls = _capture_materialize()
        with patcher:
            with pytest.raises(ValidationError):
                _invoke(
                    _get_tool(tools, "kb_update"), {"note": "n1", "status": "bogus"}
                )
            assert calls == []

            # ...and the body still refuses it for callers that skip the schema.
            result = _invoke_unvalidated(
                _get_tool(tools, "kb_update"), note="n1", status="bogus"
            )
            assert "Error" in result
            assert calls == []

    def test_no_kg_merges_tags_lowercased(self):
        _, calls = self._update(
            _existing_note(tags=["security"]), {"add_tags": ["Auth"]}
        )
        assert _canonical_note_fields(calls)["tags"] == ["security", "auth"]

    def test_no_kg_preserves_type_from_existing(self):
        _, calls = self._update(_existing_note(type="retrospective"), {"content": "x"})
        assert _canonical_note_fields(calls)["note_type"] == "retrospective"
