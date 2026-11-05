"""KB writes find a note wherever it lives in the vault (G14–G16).

Every write used to target ``knowledge/<id>.md``. In a vault that keeps notes
in subfolders (86 % of the SRW design vault) that meant:

* ``kb_update``/``kb_delete`` on a nested note failed forever with
  ``precondition-failed``, or — without a compare-and-swap token — committed a
  second file with the same id next to it, which the index then refused;
* on the Neo4j tier every hand-written note was "not found", and ``kb_write``
  of a hand-written flat note's title overwrote it (the graph never holds it);
* the cockpit/MCP PATCH answered ``canonical-file-missing`` and the DELETE
  dropped the row, left the file, and reported success until the next sweep
  resurrected the note.

These tests drive the real agent tools and the real orchestrator materializer
against a forge and an index that keep both uniqueness rules
(``(kb_id, path)`` and ``(project_id, note_id)``).
Design: knowledge-base/knowledge/features/knowledge_base_mcp_for_chat_uis.md
§4.2, §5.7.1.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import uuid
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.services import kb_materialize as km
from orchestrator.services.kb_reindex import KbRepoRef

PROJECT = "1a387b4d-0000-0000-0000-000000000000"
REPO = "vault"
NESTED = "knowledge/features/foo.md"
FLAT = "knowledge/foo.md"
NESTED_TEXT = (
    "---\n"
    "id: foo\n"
    "type: feature\n"
    "status: active\n"
    "tags:\n  - feature\n  - proposal\n"
    "aliases:\n  - foo feature\n"
    'related:\n  - "[[bar]]"\n'
    "---\n\n# Foo\n\nBody that links [[bar]] and [[baz|Baz]].\n"
)


def blob_sha(text: str) -> str:
    data = text.encode()
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()  # noqa: S324


NESTED_SHA = blob_sha(NESTED_TEXT)


class FakeForge:
    """Gitea-shaped forge holding a real path -> text map."""

    def __init__(self, files):
        self.files = dict(files)
        self.commits = []

    async def list_tree(self, repo, ref):
        return [
            {"path": p, "type": "blob", "sha": blob_sha(t)}
            for p, t in self.files.items()
        ]

    async def change_files(self, repo, branch, files, message):
        for f in files:
            path, op = f["path"], f["operation"]
            if op == "create" and path in self.files:
                return False
            if op == "update" and path not in self.files:
                return False
            if (
                f.get("sha")
                and path in self.files
                and f["sha"] != blob_sha(self.files[path])
            ):
                return False
            self.files[path] = base64.b64decode(f["content_b64"]).decode()
            self.commits.append((op, path, message))
        return True

    async def get_file_content(self, repo, path):
        return self.files.get(path)

    async def delete_path(self, repo, branch, path, message, expected_sha=None):
        if path not in self.files:
            return "absent"
        if expected_sha and expected_sha != blob_sha(self.files[path]):
            return "conflict"
        del self.files[path]
        self.commits.append(("delete", path, message))
        return "deleted"


class UniqueViolation(Exception):
    constraint_name = "uq_knowledge_project_note"


class FakeIndex:
    """knowledge_index with both unique keys: (kb_id, path) and (project_id, note_id)."""

    def __init__(self, rows):
        self.rows = {r["path"]: dict(r) for r in rows}  # path -> row
        self.pathless_deletes = []

    @asynccontextmanager
    async def try_reindex_lock(self, kb_id):
        yield True

    async def adopt_legacy_row(self, kb_id, note_id, path, movable_paths=None):
        return None

    async def upsert_kb_note(self, *, kb_id, note_id, path, **kw):
        for p, r in self.rows.items():
            if r["note_id"] == note_id and p != path:
                raise UniqueViolation(f"duplicate key {note_id}")
        self.rows[path] = {
            "note_id": note_id,
            "path": path,
            "content": kw.get("content"),
        }
        return {"id": uuid.uuid4()}

    async def replace_note_chunks(self, **kw):
        return None

    async def replace_note_links(self, **kw):
        return None

    async def stamp_note_indexed(self, *a, **kw):
        return None

    async def delete_kb_note(self, kb_id, path):
        return self.rows.pop(path, None) is not None

    async def delete_pathless_note(self, project_id, note_id):
        self.pathless_deletes.append(note_id)
        return False


def ledger_db():
    db = AsyncMock()

    async def _begin(**kw):
        return {
            "id": uuid.uuid4(),
            "canonical_state": "pending_sync",
            "attempt_claimed": True,
            "attempt_token": uuid.uuid4(),
        }

    async def _finish(_id, *, canonical, permanent, **kw):
        return {
            "canonical_state": "canonical"
            if canonical
            else ("failed" if permanent else "pending_sync"),
            "projection_state": "pending",
            "retry_state": "none"
            if canonical
            else ("permanent" if permanent else "retryable"),
        }

    db.begin_knowledge_materialization.side_effect = _begin
    db.finish_knowledge_materialization.side_effect = _finish
    return db


REF = KbRepoRef(forge="gitea", repo_url="", owner="", repo=REPO, branch="main")


async def _fake_embed(body, **kw):
    return [], None


@pytest.fixture
def orchestrator_seams():
    patches = [
        patch.object(km, "resolve_kb_repo", AsyncMock(return_value=REF)),
        patch.object(
            km, "kb_client_for_repo", AsyncMock(side_effect=lambda db, g, r: g)
        ),
        patch.object(km, "embedding_version_for_service", lambda svc: "stamp"),
        patch("orchestrator.services.kb_reindex.embed_note_chunks", _fake_embed),
        patch("orchestrator.services.kb_reindex.note_centroid", lambda v: None),
    ]
    for p in patches:
        p.start()
    yield
    for p in reversed(patches):
        p.stop()


def _row(blob, **over):
    """get_note_by_slug's shape for the nested note."""
    row = {
        "id": "foo",
        "title": "Foo",
        "type": "feature",
        "status": "active",
        "content": "# Foo\n\nBody that links [[bar]] and [[baz|Baz]].\n",
        "confidence": None,
        "tags": ["feature", "proposal"],
        "keywords": [],
        "created": None,
        "modified": None,
        "blob_sha": blob,
        "priority": 1,
        "ready_at": None,
        "description": None,
    }
    row.update(over)
    return row


def _run_tool(tool_name, args, *, files, index_rows, row=None, kg_note=None, kg=False):
    from agent.tools.knowledge import knowledge_tools as kt

    forge = FakeForge(files)
    index = FakeIndex(index_rows)
    db = ledger_db()

    def _post(
        project_id,
        slug,
        content,
        job_id,
        retrieval_messages=None,
        expected_blob_sha=None,
        mode=None,
    ):
        async def go():
            return await km.materialize_knowledge_note(
                postgres_db=db,
                gitea_client=forge,
                project_id=project_id,
                slug=slug,
                content=content,
                job_id=job_id,
                store=index,
                embedding_service=object(),
                expected_blob_sha=expected_blob_sha,
                mode=mode,
            )

        return asyncio.run(go())

    ctx = MagicMock()
    ctx.project_id = PROJECT
    ctx.project_ids = [PROJECT]
    ctx.job_id = None
    ctx.config = {}
    ctx.knowledge_bindings = None
    ctx.workspace_manager = None
    ctx._job_metadata = {"config_name": "developer"}
    ctx.knowledge_store = AsyncMock()
    ctx.knowledge_store.get_note_by_slug = AsyncMock(return_value=row)
    ctx.knowledge_store.get_inbound_links = AsyncMock(return_value=[])
    if kg:
        ctx.knowledge_graph = MagicMock()
        # The reindexer never writes Neo4j: hand-written notes are not there.
        ctx.knowledge_graph.read_note.return_value = kg_note
    else:
        ctx.knowledge_graph = None

    with patch.object(kt, "_post_vault_file", side_effect=_post):
        with patch("agent.tools.knowledge.knowledge_tools.asyncio") as aio:
            aio.get_running_loop.side_effect = RuntimeError("no loop")
            aio.run.side_effect = asyncio.run
            tools = kt.create_kb_tools(ctx)
        tool = next(t for t in tools if t.name == tool_name)
        out = tool.invoke(args)
    return out, forge, index


NESTED_INDEX = [{"note_id": "foo", "path": NESTED}]
AGE_GUARD = "agent.tools.knowledge.knowledge_tools._retire_denied"


# =============================================================================
# Agent tools on a nested note
# =============================================================================


@pytest.mark.usefixtures("orchestrator_seams")
class TestAgentUpdatesANestedNote:
    @pytest.mark.parametrize("blob", [NESTED_SHA, None])
    @pytest.mark.parametrize("kg", [False, True])
    def test_update_lands_in_the_notes_own_file(self, blob, kg):
        out, forge, index = _run_tool(
            "kb_update",
            {"note": "foo", "append": "More."},
            files={NESTED: NESTED_TEXT},
            index_rows=NESTED_INDEX,
            row=_row(blob),
            kg=kg,
        )

        assert out.startswith("Updated **"), out
        assert sorted(forge.files) == [NESTED]  # no twin at the flat path
        assert "More." in forge.files[NESTED]
        assert [c[:2] for c in forge.commits] == [("update", NESTED)]
        assert list(index.rows) == [NESTED]  # inline index kept the same row

    def test_retire_lands_in_the_notes_own_file(self):
        with patch(AGE_GUARD, return_value=None):
            out, forge, _ = _run_tool(
                "kb_delete",
                {"note": "foo", "reason": "duplicate of the newer note"},
                files={NESTED: NESTED_TEXT},
                index_rows=NESTED_INDEX,
                row=_row(NESTED_SHA),
            )

        assert sorted(forge.files) == [NESTED], out
        assert "status: archived" in forge.files[NESTED]

    def test_a_changed_note_is_still_refused(self):
        out, forge, _ = _run_tool(
            "kb_update",
            {"note": "foo", "append": "More."},
            files={NESTED: NESTED_TEXT + "edited by a human\n"},
            index_rows=NESTED_INDEX,
            row=_row(NESTED_SHA),  # the agent read the older blob
        )

        assert "changed (or was removed) since you read it" in out
        assert forge.commits == []

    def test_note_whose_id_is_not_its_filename_is_found_by_its_blob(self):
        path = "knowledge/features/README.md"
        text = NESTED_TEXT.replace("id: foo", "id: features-readme")
        sha = blob_sha(text)
        out, forge, _ = _run_tool(
            "kb_update",
            {"note": "features-readme", "append": "More."},
            files={path: text},
            index_rows=[{"note_id": "features-readme", "path": path}],
            row=_row(sha, id="features-readme"),
        )

        assert out.startswith("Updated **"), out
        assert sorted(forge.files) == [path]
        assert "More." in forge.files[path]


@pytest.mark.usefixtures("orchestrator_seams")
class TestGraphTierCreateNeverOverwrites:
    def test_kb_write_of_a_hand_written_title_does_not_clobber_it(self):
        """The graph misses the note; the index has it -> fork, not overwrite."""
        out, forge, _ = _run_tool(
            "kb_write",
            {"title": "Foo", "type": "learning", "content": "Brand new text."},
            files={FLAT: NESTED_TEXT},
            index_rows=[{"note_id": "foo", "path": FLAT}],
            row=_row(NESTED_SHA),
            kg=True,
        )

        assert forge.files[FLAT] == NESTED_TEXT, out  # the human's note is intact
        assert [c[0] for c in forge.commits] == ["create"]
        assert forge.commits[0][1].startswith("knowledge/foo-")

    def test_create_is_refused_when_the_index_has_not_seen_the_note(self):
        """Backstop: graph and index both miss (fresh push), the endpoint refuses."""
        out, forge, _ = _run_tool(
            "kb_write",
            {"title": "Foo", "type": "learning", "content": "Brand new text."},
            files={NESTED: NESTED_TEXT},
            index_rows=[],
            row=None,
            kg=True,
        )

        assert "already exists" in out
        assert forge.files == {NESTED: NESTED_TEXT}
        assert forge.commits == []


# =============================================================================
# The materializer itself
# =============================================================================


def _materialize(forge, *, slug="foo", content=None, **kw):
    content = content or NESTED_TEXT.replace("status: active", "status: resolved")
    return asyncio.run(
        km.materialize_knowledge_note(
            postgres_db=ledger_db(),
            gitea_client=forge,
            project_id=PROJECT,
            slug=slug,
            content=content,
            **kw,
        )
    )


@pytest.mark.usefixtures("orchestrator_seams")
class TestMaterializeResolvesThePath:
    def test_ambiguous_name_is_refused_not_guessed(self):
        forge = FakeForge(
            {"knowledge/a/foo.md": NESTED_TEXT, "knowledge/b/foo.md": NESTED_TEXT + "x"}
        )

        result = _materialize(forge)

        assert result["reason"] == "ambiguous-note-path"
        assert result["retry_state"] == "permanent"
        assert forge.commits == []

    def test_the_blob_disambiguates_two_files_with_the_same_name(self):
        other = NESTED_TEXT + "other\n"
        forge = FakeForge(
            {"knowledge/a/foo.md": NESTED_TEXT, "knowledge/b/foo.md": other}
        )

        result = _materialize(forge, expected_blob_sha=blob_sha(other))

        assert result["path"] == "knowledge/b/foo.md"
        assert forge.files["knowledge/a/foo.md"] == NESTED_TEXT

    def test_create_mode_refuses_an_existing_nested_note(self):
        forge = FakeForge({NESTED: NESTED_TEXT})

        result = _materialize(forge, mode="create")

        assert result["reason"] == "already-exists"
        assert result["retry_state"] == "permanent"
        assert forge.commits == []

    def test_create_mode_writes_a_new_note_flat(self):
        forge = FakeForge({})

        result = _materialize(forge, mode="create")

        assert result["status"] == "committed"
        assert result["path"] == FLAT

    def test_update_mode_does_not_recreate_a_removed_note(self):
        forge = FakeForge({})

        result = _materialize(forge, mode="update")

        assert result["reason"] == "precondition-failed"
        assert forge.files == {}

    def test_legacy_upsert_writes_the_nested_file(self):
        forge = FakeForge({NESTED: NESTED_TEXT})

        result = _materialize(forge)

        assert result["status"] == "committed"
        assert result["path"] == NESTED
        assert sorted(forge.files) == [NESTED]

    def test_inline_index_uses_the_committed_path(self):
        forge = FakeForge({NESTED: NESTED_TEXT})
        index = FakeIndex(NESTED_INDEX)

        result = _materialize(
            forge, store=index, embedding_service=object(), expected_blob_sha=NESTED_SHA
        )

        assert result["indexed"] is True
        assert list(index.rows) == [NESTED]


# =============================================================================
# Cockpit / MCP metadata PATCH and DELETE
# =============================================================================


@pytest.mark.usefixtures("orchestrator_seams")
class TestRestMutationsOnANestedNote:
    def test_metadata_update_changes_the_nested_file(self):
        forge = FakeForge({NESTED: NESTED_TEXT})

        result = asyncio.run(
            km.materialize_knowledge_metadata_update(
                postgres_db=ledger_db(),
                gitea_client=forge,
                project_id=PROJECT,
                slug="foo",
                status="resolved",
            )
        )

        assert result["canonical_state"] == "canonical", result
        assert sorted(forge.files) == [NESTED]
        assert "status: resolved" in forge.files[NESTED]
        # Untouched frontmatter survives the status rewrite.
        assert "aliases:" in forge.files[NESTED]

    def test_metadata_update_uses_the_indexed_path_for_a_renamed_note(self):
        path = "knowledge/features/README.md"
        text = NESTED_TEXT.replace("id: foo", "id: features-readme")
        forge = FakeForge({path: text})

        result = asyncio.run(
            km.materialize_knowledge_metadata_update(
                postgres_db=ledger_db(),
                gitea_client=forge,
                project_id=PROJECT,
                slug="features-readme",
                status="resolved",
                path_hint=path,
            )
        )

        assert result["canonical_state"] == "canonical", result
        assert sorted(forge.files) == [path]
        assert "status: resolved" in forge.files[path]

    def test_hard_delete_removes_the_nested_file_and_its_row(self):
        forge = FakeForge({NESTED: NESTED_TEXT})
        index = FakeIndex(NESTED_INDEX)

        result = asyncio.run(
            km.materialize_knowledge_note_delete(
                postgres_db=ledger_db(),
                gitea_client=forge,
                project_id=PROJECT,
                slug="foo",
                reason="deleted from the cockpit",
                store=index,
            )
        )

        assert result["status"] == "committed", result
        assert result["path"] == NESTED
        assert forge.files == {}
        assert index.rows == {}
        assert result["row_deleted"] is True

    def test_delete_with_no_file_leaves_path_backed_rows_alone(self):
        """The legacy cleanup only drops born-pathless rows now."""
        forge = FakeForge({})
        index = FakeIndex([{"note_id": "foo", "path": "knowledge/elsewhere/other.md"}])

        result = asyncio.run(
            km.materialize_knowledge_note_delete(
                postgres_db=ledger_db(),
                gitea_client=forge,
                project_id=PROJECT,
                slug="foo",
                reason="cleanup",
                store=index,
            )
        )

        assert result["reason"] == "absent"
        assert index.pathless_deletes == ["foo"]
        assert "knowledge/elsewhere/other.md" in index.rows
