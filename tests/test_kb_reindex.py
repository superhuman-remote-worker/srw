"""Tests for OKF KB slice 3 PR3 — tree-diff reindexer.

Design: knowledge-base/knowledge/features/okf_knowledge_base.md §5 / §5.1 / §11 slice-3 PR3.

The reindexer is the composition PR: watermark → git tree diff → parse changed
notes (gardener parse_note_md) → chunk+embed (PR2) → persist (PR1 store surface)
→ advance watermark. Pure helpers first (blob-map filter, diff plan, frontmatter
field mapping), then the orchestration with AsyncMock deps.
"""

import asyncio
import logging
import re
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from shared.runtime.services.knowledge_store import KbWatermark
from shared.runtime.knowledge.chunker import CHUNKER_VERSION, note_centroid
from shared.runtime.knowledge.gardener import parse_note_md

from orchestrator.services.kb_task_registry import KbDatasourceTaskRegistry
from orchestrator.services.knowledge_index import KnowledgeIndexDependencies
from orchestrator.services.kb_reindex import (
    FIRST_SWEEP_DELAY_SECONDS,
    SWEEP_TICK_SECONDS,
    KbRepoRef,
    kb_reindex_sweeper_loop,
    kb_sweep_tick,
    knowledge_blob_map,
    note_fields,
    plan_reindex,
    reindex_pipeline_version,
    reindex_kb,
    resolve_kb_repo,
)

EMBEDDING_VERSION = f"qwen3-embedding-8b:4096:{CHUNKER_VERSION}"
CURRENT_VERSION = reindex_pipeline_version(EMBEDDING_VERSION, "knowledge")


def _note_md(slug: str, body: str = "the body", note_type: str = "learning") -> str:
    return (
        f"---\nid: {slug}\ntype: {note_type}\nstatus: active\n---\n# {slug}\n\n{body}\n"
    )


def _dup_exc(note_id):
    return RuntimeError(
        f'duplicate key value violates unique constraint "uq_knowledge_project_note" '
        f"DETAIL: Key (project_id, note_id)=(x, {note_id}) already exists."
    )


def _make_deps(
    *,
    head="headsha",
    watermark=None,
    tree=None,
    indexed=None,
    contents=None,
):
    """AsyncMock gitea + store + embedding service for reindex_kb."""
    gitea = AsyncMock()
    gitea.get_branch_head_sha.return_value = head
    gitea.list_tree.return_value = tree if tree is not None else []
    contents = contents or {}
    gitea.get_file_content.side_effect = lambda repo, path, ref=None: contents.get(path)

    store = AsyncMock()
    store.get_watermark.return_value = watermark
    # One shared dict so clear_note_stamps below is observable by the later
    # get_indexed_blob_shas call, the way the real table is.
    indexed_map = dict(indexed or {})
    store.get_indexed_blob_shas.return_value = indexed_map

    async def _clear_note_stamps(_kb_id, *, embedding_version=None, batch_size=200):
        # Mirror the UPDATE: the stamp goes NULL, the *row* stays. Dropping the
        # keys instead would quietly empty plan_reindex's delete set, so a test
        # store that "cleared" harder than Postgres would hide a real bug.
        # embedding_version narrows it to rows from a *different* version; this
        # fake store stamps everything with the current one, so a targeted call
        # clears nothing — which is the production case worth modelling.
        stamped = [
            path
            for path, sha in indexed_map.items()
            if sha is not None and embedding_version is None
        ]
        for path in stamped:
            indexed_map[path] = None
        return len(stamped)

    store.clear_note_stamps = AsyncMock(side_effect=_clear_note_stamps)
    store.adopt_legacy_row.return_value = None
    store.upsert_kb_note.return_value = uuid.uuid4()
    store.replace_note_chunks.return_value = 1
    store.delete_kb_note.return_value = True

    svc = MagicMock()
    svc.model = "qwen3-embedding-8b"
    svc.expected_dimensions = 4096

    async def _batch(texts):
        return [[0.1] for _ in texts]

    svc.embed_batch = AsyncMock(side_effect=_batch)
    return gitea, store, svc


def _watermark_commits(store):
    """Every ``indexed_commit`` the run wrote, in order.

    A rebuild writes the watermark twice: once up front to record the pipeline
    version (``indexed_commit`` deliberately left where it was, which is what
    makes an interrupted run resumable) and once at the end to advance it. So
    "a failed run must not advance the index" is a claim about the as-of commit,
    not about how many times the row was touched — assert on this rather than on
    the call count, or the assertion silently starts checking the wrong thing.
    """
    return [
        c.kwargs.get("indexed_commit") for c in store.upsert_watermark.await_args_list
    ]


# =============================================================================
# knowledge_blob_map — filter a gitea list_tree to indexable knowledge notes
# =============================================================================


class TestKnowledgeBlobMap:
    def test_filters_to_knowledge_md_blobs(self):
        tree = [
            {"path": "knowledge/chose-jwt.md", "type": "blob", "sha": "a1"},
            {"path": "knowledge/deep/nested.md", "type": "blob", "sha": "a2"},
            {"path": "knowledge", "type": "tree", "sha": "t1"},
            {"path": "projects/x/report.md", "type": "blob", "sha": "a3"},
            {"path": "README.md", "type": "blob", "sha": "a4"},
            {"path": "knowledge/diagram.png", "type": "blob", "sha": "a5"},
        ]
        assert knowledge_blob_map(tree) == {
            "knowledge/chose-jwt.md": "a1",
            "knowledge/deep/nested.md": "a2",
        }

    def test_skips_reserved_index_and_log(self):
        tree = [
            {"path": "knowledge/index.md", "type": "blob", "sha": "a1"},
            {"path": "knowledge/log.md", "type": "blob", "sha": "a2"},
            {"path": "knowledge/real-note.md", "type": "blob", "sha": "a3"},
        ]
        assert knowledge_blob_map(tree) == {"knowledge/real-note.md": "a3"}

    def test_empty_tree(self):
        assert knowledge_blob_map([]) == {}

    def test_external_root_can_be_nested_or_repository_root(self):
        tree = [
            {"path": "vault/a.md", "type": "blob", "sha": "a"},
            {"path": "vault/deep/b.md", "type": "blob", "sha": "b"},
            {"path": "elsewhere/c.md", "type": "blob", "sha": "c"},
        ]
        assert knowledge_blob_map(tree, "vault") == {
            "vault/a.md": "a",
            "vault/deep/b.md": "b",
        }
        assert knowledge_blob_map(tree, "") == {
            "vault/a.md": "a",
            "vault/deep/b.md": "b",
            "elsewhere/c.md": "c",
        }

    def test_root_path_changes_watermark_pipeline_stamp(self):
        assert reindex_pipeline_version(EMBEDDING_VERSION, "vault") != (
            reindex_pipeline_version(EMBEDDING_VERSION, "docs")
        )


# =============================================================================
# plan_reindex — set-diff with per-row blob_sha self-heal
# =============================================================================


class TestPlanReindex:
    def test_changed_added_deleted(self):
        indexed = {
            "knowledge/a.md": "sha1",
            "knowledge/b.md": "sha2",
            "knowledge/c.md": "sha3",
        }
        current = {
            "knowledge/a.md": "sha1",
            "knowledge/b.md": "CHANGED",
            "knowledge/d.md": "NEW",
        }
        upserts, deletes = plan_reindex(indexed, current)
        assert upserts == ["knowledge/b.md", "knowledge/d.md"]  # sorted
        assert deletes == ["knowledge/c.md"]

    def test_unchanged_sha_is_skipped_self_heal(self):
        # An interrupted reindex left this row current — the re-run skips it.
        indexed = {"knowledge/a.md": "same"}
        current = {"knowledge/a.md": "same"}
        upserts, deletes = plan_reindex(indexed, current)
        assert upserts == []
        assert deletes == []

    def test_full_rebuild_upserts_everything(self):
        indexed = {"knowledge/a.md": "same", "knowledge/gone.md": "x"}
        current = {"knowledge/a.md": "same", "knowledge/b.md": "n"}
        upserts, deletes = plan_reindex(indexed, current, full=True)
        assert upserts == ["knowledge/a.md", "knowledge/b.md"]
        assert deletes == ["knowledge/gone.md"]

    def test_empty_current_deletes_all(self):
        upserts, deletes = plan_reindex({"knowledge/a.md": "x"}, {})
        assert upserts == []
        assert deletes == ["knowledge/a.md"]


# =============================================================================
# note_fields — invert _render_note_md's frontmatter into upsert_kb_note args
# =============================================================================


class TestNoteFields:
    def test_maps_full_frontmatter(self):
        fm = {
            "id": "chose-jwt",
            "type": "decision",
            "description": "Why JWT",
            "tags": ["auth", "security"],
            "keywords": ["JWT"],
            "confidence": "high",
            "status": "superseded",
            "superseded_by": "chose-paseto",
        }
        body = "# Chose JWT over OAuth\n\nBecause stateless."
        f = note_fields("knowledge/chose-jwt.md", fm, body)
        assert f["note_id"] == "chose-jwt"
        assert f["title"] == "Chose JWT over OAuth"
        assert f["note_type"] == "decision"
        assert f["status"] == "superseded"
        assert f["tags"] == ["auth", "security"]
        assert f["keywords"] == ["JWT"]
        assert f["confidence"] == "high"
        assert f["superseded_by"] == "chose-paseto"

    def test_no_frontmatter_derives_from_path_and_body(self):
        # Human-authored note with no frontmatter: id from the filename stem,
        # title from the first H1, safe defaults everywhere else.
        f = note_fields("knowledge/my-note.md", None, "# My Note\n\nbody")
        assert f["note_id"] == "my-note"
        assert f["title"] == "My Note"
        assert f["note_type"] == "learning"  # CHECK-constraint-safe default
        assert f["status"] == "active"
        assert f["tags"] == []
        assert f["superseded_by"] is None

    def test_invalid_type_and_status_fall_back_safely(self):
        # valid_note_type / valid_note_status CHECK constraints would reject
        # arbitrary frontmatter values — map unknowns to safe defaults.
        fm = {"id": "n", "type": "musing", "status": "kinda-done"}
        f = note_fields("knowledge/n.md", fm, "body")
        assert f["note_type"] == "learning"
        assert f["status"] == "active"

    def test_title_falls_back_to_note_id_without_h1(self):
        f = note_fields("knowledge/n.md", {"id": "n", "type": "code"}, "no heading")
        assert f["title"] == "n"

    def test_note_id_truncated_to_column_limit(self):
        long_id = "x" * 150
        f = note_fields("knowledge/n.md", {"id": long_id, "type": "code"}, "b")
        assert len(f["note_id"]) == 100  # VARCHAR(100)

    def test_scalar_tags_normalized_to_list(self):
        fm = {"id": "n", "type": "code", "tags": "single-tag"}
        f = note_fields("knowledge/n.md", fm, "b")
        assert f["tags"] == ["single-tag"]

    def test_absent_priority_maps_to_none(self):
        # Fix round 2 (Finding 3): unlike type/status, an absent priority
        # is NOT "unknown, use the default" -- it means "this file has no
        # opinion", so upsert_kb_note can tell "leave it alone" apart from
        # "set it to normal". Every pre-existing note lacks this key.
        fm = {"id": "n", "type": "feature", "status": "active"}
        f = note_fields("knowledge/n.md", fm, "b")
        assert f["priority"] is None

    def test_no_frontmatter_at_all_maps_priority_to_none(self):
        f = note_fields("knowledge/n.md", None, "# N\n\nbody")
        assert f["priority"] is None

    def test_invalid_priority_falls_back_to_normal(self):
        # Present but unparseable (a human typo) -- still a safe default,
        # unlike the absent case above.
        fm = {"id": "n", "type": "feature", "priority": "URGENT!!"}
        f = note_fields("knowledge/n.md", fm, "b")
        assert f["priority"] == 1

    def test_valid_priority_word_maps_to_its_rank(self):
        fm = {"id": "n", "type": "feature", "priority": "low"}
        f = note_fields("knowledge/n.md", fm, "b")
        assert f["priority"] == 2

    def test_numeric_priority_rank_round_trips(self):
        # M8 repro: a user who reads knowledge_index.priority (0=high)
        # elsewhere and writes it straight back into frontmatter as
        # `priority: 0` (an unquoted int, not the word "high") must get
        # rank 0 back -- the pre-fix code did
        # str(0).strip().lower() == "0", which is not a key in
        # PRIORITY_RANKS (word-keyed), and silently fell back to "normal".
        for rank in (0, 1, 2):
            fm = {"id": "n", "type": "feature", "priority": rank}
            f = note_fields("knowledge/n.md", fm, "b")
            assert f["priority"] == rank, f"rank {rank} did not round-trip"

    def test_numeric_priority_as_quoted_string_also_round_trips(self):
        fm = {"id": "n", "type": "feature", "priority": "0"}
        f = note_fields("knowledge/n.md", fm, "b")
        assert f["priority"] == 0

    def test_out_of_range_numeric_priority_falls_back_to_normal(self):
        fm = {"id": "n", "type": "feature", "priority": 7}
        f = note_fields("knowledge/n.md", fm, "b")
        assert f["priority"] == 1

    def test_boolean_priority_is_not_mistaken_for_a_rank(self):
        # bool is an int subclass in Python -- int(True) == 1 must not
        # silently alias a stray `priority: true` to rank 1.
        fm = {"id": "n", "type": "feature", "priority": True}
        f = note_fields("knowledge/n.md", fm, "b")
        assert f["priority"] == 1  # falls through to the default, not a fluke


# index_single_note — the shared per-note unit behind the sweep and the
# materialisation endpoint. The ordering (embed -> adopt -> upsert UNSTAMPED ->
# chunks -> links -> stamp) is the durability contract, so it is asserted here
# rather than only through reindex_kb.

from orchestrator.services.kb_reindex import (  # noqa: E402
    NoteIndexError,
    index_single_note,
)


def _index_store() -> AsyncMock:
    store = AsyncMock()
    store.upsert_kb_note.return_value = uuid.uuid4()
    store.adopt_legacy_row.return_value = None
    return store


def _embedder(dims: int = 3) -> AsyncMock:
    svc = AsyncMock()
    svc.embed_batch.side_effect = lambda texts: [[0.1] * dims for _ in texts]
    return svc


class TestIndexSingleNote:
    def test_indexes_a_note_and_stamps_it_last(self):
        store = _index_store()
        kb_id = uuid.uuid4()
        calls = []
        for name in (
            "adopt_legacy_row",
            "upsert_kb_note",
            "replace_note_chunks",
            "replace_note_links",
            "stamp_note_indexed",
        ):
            getattr(store, name).side_effect = lambda *a, _n=name, **k: calls.append(
                _n
            ) or (uuid.uuid4() if _n == "upsert_kb_note" else None)

        outcome = asyncio.run(
            index_single_note(
                store=store,
                embedding_service=_embedder(),
                kb_id=kb_id,
                path="knowledge/chose-jwt.md",
                text=_note_md("chose-jwt"),
                blob_sha="deadbeef",
                embedding_stamp=EMBEDDING_VERSION,
            )
        )

        assert outcome.status == "indexed"
        assert outcome.note_id == "chose-jwt"
        assert outcome.chunks == 1
        assert calls == [
            "adopt_legacy_row",
            "upsert_kb_note",
            "replace_note_chunks",
            "replace_note_links",
            "stamp_note_indexed",
        ]

    def test_upserts_unstamped_then_stamps_the_blob_sha(self):
        store = _index_store()
        asyncio.run(
            index_single_note(
                store=store,
                embedding_service=_embedder(),
                kb_id=uuid.uuid4(),
                path="knowledge/n.md",
                text=_note_md("n"),
                blob_sha="cafe1234",
                embedding_stamp=EMBEDDING_VERSION,
            )
        )
        upsert_kwargs = store.upsert_kb_note.await_args.kwargs
        assert upsert_kwargs["blob_sha"] is None
        assert upsert_kwargs["embedding_version"] is None
        stamp_args = store.stamp_note_indexed.await_args.args
        assert stamp_args[1] == "cafe1234"
        assert stamp_args[2] == EMBEDDING_VERSION

    def test_malformed_frontmatter_returns_malformed_and_writes_nothing(self):
        store = _index_store()
        outcome = asyncio.run(
            index_single_note(
                store=store,
                embedding_service=_embedder(),
                kb_id=uuid.uuid4(),
                path="knowledge/bad.md",
                text="---\nid: [unclosed\n---\n# Bad\n",
                blob_sha="sha",
                embedding_stamp=EMBEDDING_VERSION,
            )
        )
        assert outcome.status == "malformed"
        assert outcome.detail
        store.upsert_kb_note.assert_not_awaited()
        store.stamp_note_indexed.assert_not_awaited()

    def test_a_store_failure_raises_note_index_error_carrying_the_okf_id(self):
        store = _index_store()
        boom = RuntimeError("value too long for type character varying(100)")
        store.replace_note_links.side_effect = boom

        with pytest.raises(NoteIndexError) as excinfo:
            asyncio.run(
                index_single_note(
                    store=store,
                    embedding_service=_embedder(),
                    kb_id=uuid.uuid4(),
                    path="knowledge/wedged.md",
                    text=_note_md("wedged"),
                    blob_sha="sha",
                    embedding_stamp=EMBEDDING_VERSION,
                )
            )
        assert excinfo.value.note_id == "wedged"
        assert excinfo.value.cause is boom
        store.stamp_note_indexed.assert_not_awaited()

    def test_passes_movable_paths_to_adoption(self):
        store = _index_store()
        asyncio.run(
            index_single_note(
                store=store,
                embedding_service=_embedder(),
                kb_id=uuid.uuid4(),
                path="knowledge/n.md",
                text=_note_md("n"),
                blob_sha="sha",
                embedding_stamp=EMBEDDING_VERSION,
                movable_paths=["knowledge/old.md"],
            )
        )
        assert store.adopt_legacy_row.await_args.kwargs["movable_paths"] == [
            "knowledge/old.md"
        ]

    def test_over_the_chunk_cap_defers_without_embedding(self):
        store = _index_store()
        embedder = _embedder()
        # Six ~600-token sections chunk to six chunks, over a cap of 2.
        body = "\n\n".join(f"## Section {i}\n\n" + ("word " * 600) for i in range(6))
        outcome = asyncio.run(
            index_single_note(
                store=store,
                embedding_service=embedder,
                kb_id=uuid.uuid4(),
                path="knowledge/dump.md",
                text=f"---\nid: dump\ntype: learning\n---\n# Dump\n\n{body}\n",
                blob_sha="sha",
                embedding_stamp=EMBEDDING_VERSION,
                max_chunks=2,
            )
        )
        assert outcome.status == "oversized"
        assert outcome.note_id == "dump"
        assert outcome.chunks > 2
        embedder.embed_batch.assert_not_awaited()
        store.upsert_kb_note.assert_not_awaited()

    def test_under_the_cap_indexes_normally(self):
        store = _index_store()
        outcome = asyncio.run(
            index_single_note(
                store=store,
                embedding_service=_embedder(),
                kb_id=uuid.uuid4(),
                path="knowledge/small.md",
                text=_note_md("small"),
                blob_sha="sha",
                embedding_stamp=EMBEDDING_VERSION,
                max_chunks=8,
            )
        )
        assert outcome.status == "indexed"
        store.stamp_note_indexed.assert_awaited_once()

    def test_no_cap_indexes_a_large_note(self):
        store = _index_store()
        body = "\n\n".join(f"## Section {i}\n\n" + ("word " * 600) for i in range(6))
        outcome = asyncio.run(
            index_single_note(
                store=store,
                embedding_service=_embedder(),
                kb_id=uuid.uuid4(),
                path="knowledge/dump.md",
                text=f"---\nid: dump\ntype: learning\n---\n# Dump\n\n{body}\n",
                blob_sha="sha",
                embedding_stamp=EMBEDDING_VERSION,
            )
        )
        assert outcome.status == "indexed"
        assert outcome.chunks > 2

    def test_forwards_retrieval_messages_to_the_row(self):
        store = _index_store()
        asyncio.run(
            index_single_note(
                store=store,
                embedding_service=_embedder(),
                kb_id=uuid.uuid4(),
                path="knowledge/n.md",
                text=_note_md("n"),
                blob_sha="sha",
                embedding_stamp=EMBEDDING_VERSION,
                retrieval_messages=["when does the sweep run?"],
            )
        )
        kwargs = store.upsert_kb_note.await_args.kwargs
        assert kwargs["retrieval_messages"] == ["when does the sweep run?"]

    def test_omitted_retrieval_messages_leave_the_row_alone(self):
        store = _index_store()
        asyncio.run(
            index_single_note(
                store=store,
                embedding_service=_embedder(),
                kb_id=uuid.uuid4(),
                path="knowledge/n.md",
                text=_note_md("n"),
                blob_sha="sha",
                embedding_stamp=EMBEDDING_VERSION,
            )
        )
        assert store.upsert_kb_note.await_args.kwargs["retrieval_messages"] is None

    def test_forwards_the_writing_job_to_the_row(self):
        # knowledge_index.job_id is the provenance behind kb_list(job_id=...).
        # The note file cannot carry it -- OKF frontmatter has no job field --
        # so the inline caller's argument is its only route onto the row.
        store = _index_store()
        writing_job = uuid.uuid4()
        asyncio.run(
            index_single_note(
                store=store,
                embedding_service=_embedder(),
                kb_id=uuid.uuid4(),
                path="knowledge/n.md",
                text=_note_md("n"),
                blob_sha="sha",
                embedding_stamp=EMBEDDING_VERSION,
                job_id=writing_job,
            )
        )
        assert store.upsert_kb_note.await_args.kwargs["job_id"] == writing_job

    def test_an_omitted_job_id_leaves_the_row_alone(self):
        # The sweep's every call: it re-indexes notes it did not write and has
        # no job to name, so it must not overwrite the writer's provenance.
        store = _index_store()
        asyncio.run(
            index_single_note(
                store=store,
                embedding_service=_embedder(),
                kb_id=uuid.uuid4(),
                path="knowledge/n.md",
                text=_note_md("n"),
                blob_sha="sha",
                embedding_stamp=EMBEDDING_VERSION,
            )
        )
        assert store.upsert_kb_note.await_args.kwargs["job_id"] is None

    def test_forwards_both_timestamps_from_the_file(self):
        # created_at was already forwarded; modified_at was not passed at all,
        # so the recency arm saw NULL for every note the file path indexed.
        store = _index_store()
        asyncio.run(
            index_single_note(
                store=store,
                embedding_service=_embedder(),
                kb_id=uuid.uuid4(),
                path="knowledge/n.md",
                text=(
                    "---\nid: n\ntype: learning\n"
                    "created: 2026-01-15T10:30:00+00:00\n"
                    "modified: 2026-02-20T09:00:00+00:00\n"
                    "---\n# N\n\nthe body\n"
                ),
                blob_sha="sha",
                embedding_stamp=EMBEDDING_VERSION,
            )
        )
        kwargs = store.upsert_kb_note.await_args.kwargs
        assert kwargs["created_at"].isoformat() == "2026-01-15T10:30:00+00:00"
        assert kwargs["modified_at"].isoformat() == "2026-02-20T09:00:00+00:00"

    def test_a_file_with_no_timestamps_forwards_none(self):
        store = _index_store()
        asyncio.run(
            index_single_note(
                store=store,
                embedding_service=_embedder(),
                kb_id=uuid.uuid4(),
                path="knowledge/n.md",
                text=_note_md("n"),
                blob_sha="sha",
                embedding_stamp=EMBEDDING_VERSION,
            )
        )
        kwargs = store.upsert_kb_note.await_args.kwargs
        assert kwargs["created_at"] is None
        assert kwargs["modified_at"] is None


# =============================================================================
# note_fields — created (frontmatter) -> created_at (project-backlog-pipeline
# fix wave, finding B3)
# =============================================================================


class TestNoteFieldsCreatedAt:
    def test_frontmatter_created_maps_to_created_at(self):
        """The realistic path: _render_note_md writes `created:` unquoted, so
        parse_note_md's YAML load already turns it into a native datetime
        before note_fields ever sees it."""
        from datetime import datetime, timezone

        fm, body = parse_note_md(
            "---\nid: feature-x\ntype: feature\n"
            "created: 2026-01-15T10:30:00+00:00\n---\n# T\nbody\n"
        )
        f = note_fields("knowledge/feature-x.md", fm, body)
        assert f["created_at"] == datetime(2026, 1, 15, 10, 30, tzinfo=timezone.utc)

    def test_quoted_string_created_is_still_parsed(self):
        from datetime import datetime, timezone

        fm, body = parse_note_md(
            "---\nid: feature-x\ntype: feature\n"
            'created: "2026-01-15T10:30:00+00:00"\n---\n# T\nbody\n'
        )
        f = note_fields("knowledge/feature-x.md", fm, body)
        assert f["created_at"] == datetime(2026, 1, 15, 10, 30, tzinfo=timezone.utc)

    def test_absent_created_maps_to_none(self):
        """Every note without a `created:` line (the common case today, per
        the finding: the cockpit panel is read-only so a hand-authored file
        is the only user path to create a ticket, and a user is unlikely to
        know to add this line) must not crash -- and must not fabricate a
        timestamp; NULLS LAST in the pool query is what covers this."""
        fm = {"id": "n", "type": "feature"}
        f = note_fields("knowledge/n.md", fm, "b")
        assert f["created_at"] is None

    def test_unparseable_created_degrades_to_none_not_a_crash(self):
        fm = {"id": "n", "type": "feature", "created": "not-a-date-at-all"}
        f = note_fields("knowledge/n.md", fm, "b")
        assert f["created_at"] is None

    def test_frontmatter_modified_maps_to_modified_at(self):
        """`modified_at` is the ordering key of the search function's recency
        arm — a bare `ORDER BY ki.modified_at DESC`, with no NULLS LAST —
        which feeds the RRF score. A NULL there does not fail anything, and
        it does not rank a note last either: Postgres sorts NULLs FIRST under
        DESC, so every unstamped note quietly occupied the TOP of that arm's
        window instead."""
        from datetime import datetime, timezone

        fm, body = parse_note_md(
            "---\nid: feature-x\ntype: feature\n"
            "modified: 2026-01-15T10:30:00+00:00\n---\n# T\nbody\n"
        )
        f = note_fields("knowledge/feature-x.md", fm, body)
        assert f["modified_at"] == datetime(2026, 1, 15, 10, 30, tzinfo=timezone.utc)

    def test_absent_modified_maps_to_none(self):
        # Same sentinel as created/priority/ready_at: a file with no opinion
        # must not fabricate a timestamp.
        fm = {"id": "n", "type": "feature"}
        f = note_fields("knowledge/n.md", fm, "b")
        assert f["modified_at"] is None


# =============================================================================
# reindex_kb — the orchestration
# =============================================================================


class TestReindexKbShortCircuits:
    @pytest.mark.asyncio
    async def test_no_head_returns_without_work(self):
        gitea, store, svc = _make_deps(head=None)
        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=uuid.uuid4(),
            repo_name="r",
        )
        assert result["status"] == "no-head"
        gitea.list_tree.assert_not_awaited()
        store.upsert_watermark.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_up_to_date_skips_tree_fetch(self):
        kb = uuid.uuid4()
        wm = KbWatermark(
            kb_id=kb, indexed_commit="headsha", pipeline_version=CURRENT_VERSION
        )
        gitea, store, svc = _make_deps(head="headsha", watermark=wm)
        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )
        assert result["status"] == "up-to-date"
        gitea.list_tree.assert_not_awaited()
        store.upsert_watermark.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_force_full_bypasses_up_to_date(self):
        kb = uuid.uuid4()
        wm = KbWatermark(
            kb_id=kb, indexed_commit="headsha", pipeline_version=CURRENT_VERSION
        )
        gitea, store, svc = _make_deps(head="headsha", watermark=wm, tree=[])
        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
            force_full=True,
        )
        assert result["status"] == "completed"
        gitea.list_tree.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_tree_fetch_failure_does_not_advance_watermark(self):
        kb = uuid.uuid4()
        wm = KbWatermark(
            kb_id=kb, indexed_commit="last-good", pipeline_version=CURRENT_VERSION
        )
        gitea, store, svc = _make_deps(head="headsha", watermark=wm)
        gitea.list_tree.return_value = None
        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )
        assert result["status"] == "tree-fetch-failed"
        assert result["indexed_commit"] == "last-good"
        assert result["errors"] == 1
        store.upsert_watermark.assert_not_awaited()


class TestReindexKbIncremental:
    def _tree(self):
        return [
            {"path": "knowledge/changed.md", "type": "blob", "sha": "NEW"},
            {"path": "knowledge/same.md", "type": "blob", "sha": "same1"},
        ]

    @pytest.mark.asyncio
    async def test_changed_note_flows_through_pipeline(self):
        kb = uuid.uuid4()
        endpoint_stamp = f"{EMBEDDING_VERSION}:pf-effective-profile"
        endpoint_pipeline = reindex_pipeline_version(endpoint_stamp, "knowledge")
        wm = KbWatermark(
            kb_id=kb, indexed_commit="old", pipeline_version=endpoint_pipeline
        )
        gitea, store, svc = _make_deps(
            head="headsha",
            watermark=wm,
            tree=self._tree(),
            indexed={"knowledge/changed.md": "OLD", "knowledge/same.md": "same1"},
            contents={"knowledge/changed.md": _note_md("changed", "fresh insight")},
        )
        svc.profile_fingerprint = "pf-effective-profile"
        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )
        assert result["status"] == "completed"
        assert result["upserted"] == 1
        # unchanged sha skipped — only the changed note was fetched
        gitea.get_file_content.assert_awaited_once_with(
            "r", "knowledge/changed.md", ref="headsha"
        )
        # adopt-then-upsert ordering for the legacy-collision guard
        store.adopt_legacy_row.assert_awaited_once_with(
            kb, "changed", "knowledge/changed.md", movable_paths=[]
        )
        up_kwargs = store.upsert_kb_note.await_args[1]
        assert up_kwargs["kb_id"] == kb
        assert up_kwargs["path"] == "knowledge/changed.md"
        # the note lands UNSTAMPED — blob_sha/embedding_version mean "chunks
        # durable" and are set by stamp_note_indexed only after the chunk write
        assert up_kwargs["blob_sha"] is None
        assert up_kwargs["embedding_version"] is None
        # chunks persisted with the note row id
        rc_kwargs = store.replace_note_chunks.await_args[1]
        assert rc_kwargs["kb_id"] == kb
        assert rc_kwargs["embedding_version"] == endpoint_stamp
        assert len(rc_kwargs["chunks"]) >= 1
        # ... then the stamp, carrying the git blob + pipeline version + the
        # whole-note centroid of the chunk embeddings (PR4d).
        expected_centroid = note_centroid([c["embedding"] for c in rc_kwargs["chunks"]])
        store.stamp_note_indexed.assert_awaited_once_with(
            store.upsert_kb_note.return_value,
            "NEW",
            endpoint_stamp,
            centroid=expected_centroid,
        )
        # watermark advanced to head with the current pipeline version
        wm_kwargs = store.upsert_watermark.await_args[1]
        assert wm_kwargs["indexed_commit"] == "headsha"
        assert wm_kwargs["pipeline_version"] == endpoint_pipeline

    @pytest.mark.asyncio
    async def test_github_tree_and_tarball_populate_nested_path_and_links(
        self, monkeypatch
    ):
        """The external-forge source reaches the unchanged indexing pipeline."""
        import io
        import tarfile

        import httpx

        from shared.runtime.services.forge import ForgeRepo, GitHubClient

        note = (
            "---\nid: note\ntype: learning\nstatus: active\n---\n"
            "# Nested Note\n\nExternal vault insight; see [[related-note]].\n"
        ).encode()
        archive_buffer = io.BytesIO()
        with tarfile.open(fileobj=archive_buffer, mode="w:gz") as archive:
            member = tarfile.TarInfo("acme-vault-head/knowledge/nested/note.md")
            member.size = len(note)
            archive.addfile(member, io.BytesIO(note))
        archive_bytes = archive_buffer.getvalue()

        def handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if path.endswith("/branches/main"):
                return httpx.Response(200, json={"commit": {"sha": "head-sha"}})
            if path.endswith("/git/trees/head-sha"):
                assert request.url.params.get("recursive") == "1"
                return httpx.Response(
                    200,
                    json={
                        "tree": [
                            {
                                "path": "knowledge/nested/note.md",
                                "type": "blob",
                                "sha": "blob-sha",
                            }
                        ],
                        "truncated": False,
                    },
                )
            if path.endswith("/tarball/head-sha"):
                return httpx.Response(200, content=archive_bytes)
            raise AssertionError(f"unexpected GitHub request: {request.method} {path}")

        monkeypatch.setattr(
            "shared.runtime.services.forge._transport",
            httpx.MockTransport(handler),
            raising=False,
        )
        client = GitHubClient(
            ForgeRepo(
                "github",
                "https://api.github.com",
                "acme",
                "vault",
                "test-pat",
            )
        )
        _unused, store, svc = _make_deps(indexed={})
        kb_id = uuid.uuid4()

        result = await reindex_kb(
            gitea_client=client,
            store=store,
            embedding_service=svc,
            kb_id=kb_id,
            repo_name="vault",
            branch="main",
        )

        assert result["status"] == "completed"
        assert result["upserted"] == 1
        note_kwargs = store.upsert_kb_note.await_args.kwargs
        assert note_kwargs["kb_id"] == kb_id
        assert note_kwargs["path"] == "knowledge/nested/note.md"
        assert note_kwargs["note_id"] == "note"  # basename, never nested path
        assert "External vault insight" in note_kwargs["content"]
        assert store.replace_note_links.await_args.kwargs["targets"] == ["related-note"]
        # KnowledgeStore.upsert_kb_note derives note-level search_doc from this
        # content in SQL; the chunk write likewise derives its sparse document.
        assert store.replace_note_chunks.await_count == 1
        wm_kwargs = store.upsert_watermark.await_args[1]
        assert wm_kwargs["indexed_commit"] == "head-sha"
        assert wm_kwargs["pipeline_version"] == CURRENT_VERSION

    @pytest.mark.asyncio
    async def test_forwards_parsed_priority_to_upsert_kb_note(self):
        # Mutation-tested (project-backlog-pipeline task 2, fix round 1
        # finding 2): note_fields correctly parsing frontmatter priority was
        # never enough on its own — deleting the forwarding kwarg at the
        # reindexer's store.upsert_kb_note(...) call site left every existing
        # test green. Assert the forwarded value against a live note_fields
        # call on the same parsed frontmatter, so this fails if the two ever
        # drift apart, not just against a hardcoded literal.
        kb = uuid.uuid4()
        endpoint_stamp = f"{EMBEDDING_VERSION}:pf-effective-profile"
        endpoint_pipeline = reindex_pipeline_version(endpoint_stamp, "knowledge")
        wm = KbWatermark(
            kb_id=kb, indexed_commit="old", pipeline_version=endpoint_pipeline
        )
        note_text = _note_md("changed", "fresh insight", note_type="feature").replace(
            "status: active\n", "status: active\npriority: high\n"
        )
        gitea, store, svc = _make_deps(
            head="headsha",
            watermark=wm,
            tree=self._tree(),
            indexed={"knowledge/changed.md": "OLD", "knowledge/same.md": "same1"},
            contents={"knowledge/changed.md": note_text},
        )
        svc.profile_fingerprint = "pf-effective-profile"
        await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )
        fm, body = parse_note_md(note_text)
        expected = note_fields("knowledge/changed.md", fm, body)
        assert expected["priority"] == 0  # sanity: the fixture says "high"
        up_kwargs = store.upsert_kb_note.await_args[1]
        assert up_kwargs["priority"] == expected["priority"]

    @pytest.mark.asyncio
    async def test_absent_priority_forwards_none_not_a_default(self):
        # Fix round 2, Finding 3 repro: a file with NO priority: line (every
        # pre-existing note, and any edit that doesn't touch that line) must
        # forward None to upsert_kb_note, not _DEFAULT_PRIORITY_RANK -- this
        # runs on every merge and via the sweeper, so silently defaulting
        # here would stamp "normal" over a real stored priority on the very
        # next reindex. See TestUpsertKbNotePriorityCoalesceSentinel in
        # test_knowledge_store.py for proof the SQL side honors None as
        # "leave unchanged" rather than nulling the column.
        kb = uuid.uuid4()
        endpoint_stamp = f"{EMBEDDING_VERSION}:pf-effective-profile"
        endpoint_pipeline = reindex_pipeline_version(endpoint_stamp, "knowledge")
        wm = KbWatermark(
            kb_id=kb, indexed_commit="old", pipeline_version=endpoint_pipeline
        )
        # _note_md carries no priority: line at all -- the common case for
        # every note that predates this feature or was hand-edited.
        note_text = _note_md("changed", "fresh insight", note_type="feature")
        gitea, store, svc = _make_deps(
            head="headsha",
            watermark=wm,
            tree=self._tree(),
            indexed={"knowledge/changed.md": "OLD", "knowledge/same.md": "same1"},
            contents={"knowledge/changed.md": note_text},
        )
        svc.profile_fingerprint = "pf-effective-profile"
        await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )
        fm, body = parse_note_md(note_text)
        expected = note_fields("knowledge/changed.md", fm, body)
        assert expected["priority"] is None  # sanity: no priority: line
        up_kwargs = store.upsert_kb_note.await_args[1]
        assert up_kwargs["priority"] is None

    @pytest.mark.asyncio
    async def test_forwards_parsed_created_at_to_upsert_kb_note(self):
        # B3 repro: note_fields correctly parsing frontmatter `created` into
        # `created_at` was never enough on its own -- deleting the
        # forwarding kwarg at the reindexer's store.upsert_kb_note(...) call
        # site left every existing test green (created_at simply wasn't
        # asserted anywhere), and every INSERTed row kept created_at NULL.
        from datetime import datetime, timezone

        kb = uuid.uuid4()
        endpoint_stamp = f"{EMBEDDING_VERSION}:pf-effective-profile"
        endpoint_pipeline = reindex_pipeline_version(endpoint_stamp, "knowledge")
        wm = KbWatermark(
            kb_id=kb, indexed_commit="old", pipeline_version=endpoint_pipeline
        )
        note_text = _note_md("changed", "fresh insight", note_type="feature").replace(
            "status: active\n", "status: active\ncreated: 2026-01-15T10:30:00+00:00\n"
        )
        gitea, store, svc = _make_deps(
            head="headsha",
            watermark=wm,
            tree=self._tree(),
            indexed={"knowledge/changed.md": "OLD", "knowledge/same.md": "same1"},
            contents={"knowledge/changed.md": note_text},
        )
        svc.profile_fingerprint = "pf-effective-profile"
        await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )
        up_kwargs = store.upsert_kb_note.await_args[1]
        assert up_kwargs["created_at"] == datetime(
            2026, 1, 15, 10, 30, tzinfo=timezone.utc
        )

    @pytest.mark.asyncio
    async def test_absent_created_forwards_none_to_upsert_kb_note(self):
        # The common case (every pre-existing note) must forward None, not
        # crash and not fabricate a timestamp.
        kb = uuid.uuid4()
        endpoint_stamp = f"{EMBEDDING_VERSION}:pf-effective-profile"
        endpoint_pipeline = reindex_pipeline_version(endpoint_stamp, "knowledge")
        wm = KbWatermark(
            kb_id=kb, indexed_commit="old", pipeline_version=endpoint_pipeline
        )
        note_text = _note_md("changed", "fresh insight", note_type="feature")
        gitea, store, svc = _make_deps(
            head="headsha",
            watermark=wm,
            tree=self._tree(),
            indexed={"knowledge/changed.md": "OLD", "knowledge/same.md": "same1"},
            contents={"knowledge/changed.md": note_text},
        )
        svc.profile_fingerprint = "pf-effective-profile"
        await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )
        up_kwargs = store.upsert_kb_note.await_args[1]
        assert up_kwargs["created_at"] is None

    @pytest.mark.asyncio
    async def test_progress_counters_reset_and_finalize(self):
        kb = uuid.uuid4()
        endpoint_stamp = f"{EMBEDDING_VERSION}:pf-effective-profile"
        endpoint_pipeline = reindex_pipeline_version(endpoint_stamp, "knowledge")
        wm = KbWatermark(
            kb_id=kb, indexed_commit="old", pipeline_version=endpoint_pipeline
        )
        gitea, store, svc = _make_deps(
            head="headsha",
            watermark=wm,
            tree=self._tree(),
            indexed={"knowledge/changed.md": "OLD", "knowledge/same.md": "same1"},
            contents={"knowledge/changed.md": _note_md("changed", "fresh insight")},
        )
        svc.profile_fingerprint = "pf-effective-profile"

        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )

        assert result["status"] == "completed"
        # One changed note this run: notes_total set at reset, done at finalize.
        calls = [c.args for c in store.update_index_progress.await_args_list]
        assert calls[0] == (kb, 0, 1)  # reset — notes_total = changed-set size
        assert calls[-1] == (kb, 1, 1)  # finalize — 1 of 1 durably stamped

    @pytest.mark.asyncio
    async def test_stamp_centroid_spans_multiple_chunks(self):
        # A genuinely multi-chunk note: the stamped centroid must be the
        # per-dimension mean of ALL the note's chunk vectors (not just the first),
        # so find_near_duplicate_pairs compares a faithful whole-note vector.
        kb = uuid.uuid4()
        wm = KbWatermark(
            kb_id=kb, indexed_commit="old", pipeline_version=CURRENT_VERSION
        )
        body = "alpha " * 2000  # far over target_tokens -> splits into pieces
        gitea, store, svc = _make_deps(
            head="headsha",
            watermark=wm,
            tree=[{"path": "knowledge/big.md", "type": "blob", "sha": "NEW"}],
            indexed={"knowledge/big.md": "OLD"},
            contents={"knowledge/big.md": _note_md("big", body)},
        )

        async def _batch(texts):
            # A distinct vector per chunk so the mean is non-trivial.
            return [[float(i), float(i) * 2] for i in range(len(texts))]

        svc.embed_batch = AsyncMock(side_effect=_batch)

        await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )
        chunks = store.replace_note_chunks.await_args[1]["chunks"]
        assert len(chunks) >= 2  # guard: the note really did split
        centroid = store.stamp_note_indexed.await_args.kwargs["centroid"]
        assert centroid == note_centroid([c["embedding"] for c in chunks])

    @pytest.mark.asyncio
    async def test_populates_links_from_body_markdown(self):
        # The reindexer extracts a note's outbound body markdown links and writes
        # them to the link table (the kg-less kb_related backend). Link targets
        # are the basenames of `[...](slug.md)` links; the stamp lands only after
        # links are written (same "durable-then-stamp" invariant as chunks).
        kb = uuid.uuid4()
        wm = KbWatermark(
            kb_id=kb, indexed_commit="old", pipeline_version=CURRENT_VERSION
        )
        body = "See [Other](other-note.md) and [Third](third.md)."
        gitea, store, svc = _make_deps(
            head="headsha",
            watermark=wm,
            tree=[{"path": "knowledge/changed.md", "type": "blob", "sha": "NEW"}],
            indexed={"knowledge/changed.md": "OLD"},
            contents={"knowledge/changed.md": _note_md("changed", body)},
        )
        order = []
        store.replace_note_links.side_effect = lambda *a, **k: order.append("links")
        store.stamp_note_indexed.side_effect = lambda *a, **k: order.append("stamp")

        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )
        assert result["status"] == "completed"
        link_kwargs = store.replace_note_links.await_args[1]
        assert link_kwargs["kb_id"] == kb
        assert link_kwargs["source_id"] == "changed"
        assert link_kwargs["source_note_row"] == store.upsert_kb_note.return_value
        assert link_kwargs["targets"] == ["other-note", "third"]
        # links written before the stamp
        assert order == ["links", "stamp"]

    @pytest.mark.asyncio
    async def test_link_targets_include_wikilinks_and_related_frontmatter(self):
        # This vault is an Obsidian vault: most edges are [[wikilinks]] in the
        # body and `related:` frontmatter, not [text](x.md). Both must reach the
        # link table or a seeded vault indexes with no graph at all.
        kb = uuid.uuid4()
        wm = KbWatermark(
            kb_id=kb, indexed_commit="old", pipeline_version=CURRENT_VERSION
        )
        text = (
            "---\nid: changed\ntype: learning\nstatus: active\n"
            'related:\n  - "[[from_frontmatter]]"\n---\n'
            "# changed\n\nSee [Md](md_link.md) and [[body_wikilink]].\n"
        )
        gitea, store, svc = _make_deps(
            head="headsha",
            watermark=wm,
            tree=[{"path": "knowledge/changed.md", "type": "blob", "sha": "NEW"}],
            indexed={"knowledge/changed.md": "OLD"},
            contents={"knowledge/changed.md": text},
        )

        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )
        assert result["status"] == "completed"
        targets = store.replace_note_links.await_args[1]["targets"]
        assert sorted(targets) == [
            "body_wikilink",
            "from_frontmatter",
            "md_link",
        ]

    @pytest.mark.asyncio
    async def test_deleted_note_removed_from_index(self):
        kb = uuid.uuid4()
        wm = KbWatermark(
            kb_id=kb, indexed_commit="old", pipeline_version=CURRENT_VERSION
        )
        gitea, store, svc = _make_deps(
            head="headsha",
            watermark=wm,
            tree=[{"path": "knowledge/same.md", "type": "blob", "sha": "same1"}],
            indexed={"knowledge/same.md": "same1", "knowledge/gone.md": "x"},
        )
        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )
        assert result["status"] == "completed"
        assert result["deleted"] == 1
        store.delete_kb_note.assert_awaited_once_with(kb, "knowledge/gone.md")

    @pytest.mark.asyncio
    async def test_same_id_git_rename_moves_before_path_upsert(self):
        kb = uuid.uuid4()
        wm = KbWatermark(
            kb_id=kb, indexed_commit="old", pipeline_version=CURRENT_VERSION
        )
        new_path = "knowledge/new-name.md"
        old_path = "knowledge/old-name.md"
        gitea, store, svc = _make_deps(
            head="headsha",
            watermark=wm,
            tree=[{"path": new_path, "type": "blob", "sha": "same-blob"}],
            indexed={old_path: "same-blob"},
            contents={new_path: _note_md("stable-id")},
        )
        order = []
        store.adopt_legacy_row.side_effect = lambda *a, **k: order.append("move")
        store.upsert_kb_note.side_effect = lambda **k: (
            order.append("upsert") or uuid.uuid4()
        )
        # In production the move means the old path no longer matches a row.
        store.delete_kb_note.return_value = False

        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )

        assert result["status"] == "completed"
        assert result["upserted"] == 1
        assert order[:2] == ["move", "upsert"]
        store.adopt_legacy_row.assert_awaited_once_with(
            kb, "stable-id", new_path, movable_paths=[old_path]
        )
        store.delete_kb_note.assert_awaited_once_with(kb, old_path)

    @pytest.mark.asyncio
    async def test_duplicate_id_does_not_steal_still_canonical_note(self):
        kb = uuid.uuid4()
        wm = KbWatermark(
            kb_id=kb, indexed_commit="old", pipeline_version=CURRENT_VERSION
        )
        canonical = "knowledge/canonical.md"
        duplicate = "knowledge/duplicate.md"
        gitea, store, svc = _make_deps(
            head="headsha",
            watermark=wm,
            tree=[
                {"path": canonical, "type": "blob", "sha": "same"},
                {"path": duplicate, "type": "blob", "sha": "new"},
            ],
            indexed={canonical: "same"},
            contents={duplicate: _note_md("stable-id")},
        )
        # The real unique (project_id, note_id) constraint rejects the second
        # identity. Crucially, adoption was not authorized to move the existing
        # row because its canonical path remains in the current tree.
        # This message does NOT name uq_knowledge_project_note, so it stays on
        # the generic-error path (not the skip-with-advisory path below) —
        # errors == 1 / status == "partial" is correct here, do not "fix" it.
        store.upsert_kb_note.side_effect = RuntimeError("duplicate note id")

        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )

        assert result["status"] == "partial"
        assert result["errors"] == 1
        assert result["indexed_commit"] == "old"
        store.adopt_legacy_row.assert_awaited_once_with(
            kb, "stable-id", duplicate, movable_paths=[]
        )
        store.upsert_watermark.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_duplicate_note_id_is_skipped_with_advisory_and_kb_reaches_ready(
        self,
    ):
        kb = uuid.uuid4()
        wm = KbWatermark(
            kb_id=kb, indexed_commit="old", pipeline_version=CURRENT_VERSION
        )
        holder = "knowledge/research/single_cluster_vm/README.md"
        loser = "knowledge/research/kb_gardening/README.md"
        gitea, store, svc = _make_deps(
            head="headsha",
            watermark=wm,
            tree=[
                {"path": holder, "type": "blob", "sha": "same"},
                {"path": loser, "type": "blob", "sha": "new"},
            ],
            indexed={holder: "same"},
            contents={loser: _note_md("README")},
        )
        store.upsert_kb_note.side_effect = _dup_exc("README")
        store.find_note_id_owner.return_value = {"path": holder, "id": "README"}

        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )

        assert result["status"] == "completed"
        assert result["errors"] == 0
        assert result["skipped_duplicates"] == 1
        assert result["indexed_commit"] == "headsha"
        kwargs = store.upsert_watermark.await_args_list[-1].kwargs
        assert kwargs["status"] == "ready"
        assert (
            "README" in kwargs["advisory"]
            and loser in kwargs["advisory"]
            and holder in kwargs["advisory"]
        )

    @pytest.mark.asyncio
    async def test_clean_run_clears_advisory(self):
        kb = uuid.uuid4()
        wm = KbWatermark(
            kb_id=kb, indexed_commit="old", pipeline_version=CURRENT_VERSION
        )
        gitea, store, svc = _make_deps(
            head="h2",
            watermark=wm,
            tree=[{"path": "knowledge/a.md", "type": "blob", "sha": "s"}],
            contents={"knowledge/a.md": _note_md("a")},
        )
        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )
        assert result["skipped_duplicates"] == 0
        assert store.upsert_watermark.await_args_list[-1].kwargs["advisory"] is None

    @pytest.mark.asyncio
    async def test_history_rewrite_reconciles_directly_from_current_tree(self):
        """A force-push needs no ancestry walk: current path/blob truth wins."""
        kb = uuid.uuid4()
        wm = KbWatermark(
            kb_id=kb,
            indexed_commit="commit-from-rewritten-history",
            pipeline_version=CURRENT_VERSION,
        )
        gitea, store, svc = _make_deps(
            head="new-unrelated-head",
            watermark=wm,
            tree=[
                {"path": "knowledge/kept.md", "type": "blob", "sha": "same"},
                {"path": "knowledge/new.md", "type": "blob", "sha": "new"},
            ],
            indexed={
                "knowledge/kept.md": "same",
                "knowledge/removed.md": "gone",
            },
            contents={"knowledge/new.md": _note_md("new")},
        )

        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )

        assert result["status"] == "completed"
        assert result["full"] is False
        assert result["indexed_commit"] == "new-unrelated-head"
        store.upsert_kb_note.assert_awaited_once()
        store.delete_kb_note.assert_awaited_once_with(kb, "knowledge/removed.md")

    @pytest.mark.asyncio
    async def test_pipeline_version_change_forces_full_rebuild(self):
        kb = uuid.uuid4()
        wm = KbWatermark(
            kb_id=kb, indexed_commit="old", pipeline_version="old-model:1024:c0"
        )
        gitea, store, svc = _make_deps(
            head="headsha",
            watermark=wm,
            tree=self._tree(),
            indexed={"knowledge/changed.md": "NEW", "knowledge/same.md": "same1"},
            contents={
                "knowledge/changed.md": _note_md("changed"),
                "knowledge/same.md": _note_md("same"),
            },
        )
        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )
        # matching blob_shas notwithstanding, EVERY note re-embeds
        assert result["upserted"] == 2
        assert result["full"] is True

    @pytest.mark.asyncio
    async def test_no_watermark_means_full_rebuild(self):
        gitea, store, svc = _make_deps(
            head="headsha",
            watermark=None,
            tree=[{"path": "knowledge/a.md", "type": "blob", "sha": "s"}],
            contents={"knowledge/a.md": _note_md("a")},
        )
        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=uuid.uuid4(),
            repo_name="r",
        )
        assert result["full"] is True
        assert result["upserted"] == 1


class TestRebuildIsResumable:
    """A rebuild killed mid-run must not restart from note zero.

    Indexing is an in-process task, so an orchestrator rollout kills it. The
    fact that drove ``full`` (pipeline_version) used to be recorded on success
    only, so the next run re-derived it and re-embedded everything the dead run
    had finished — on dev a 2635-note vault went back to 0 on each of three
    consecutive deploys and never converged.
    """

    def _tree(self):
        return [
            {"path": "knowledge/a.md", "type": "blob", "sha": "sa"},
            {"path": "knowledge/b.md", "type": "blob", "sha": "sb"},
        ]

    def _contents(self):
        return {
            "knowledge/a.md": _note_md("a"),
            "knowledge/b.md": _note_md("b"),
        }

    @pytest.mark.asyncio
    async def test_full_rebuild_clears_stamps_and_records_pipeline_up_front(self):
        kb = uuid.uuid4()
        wm = KbWatermark(kb_id=kb, indexed_commit="old", pipeline_version="stale:1")
        gitea, store, svc = _make_deps(
            head="headsha",
            watermark=wm,
            tree=self._tree(),
            indexed={"knowledge/a.md": "sa", "knowledge/b.md": "sb"},
            contents=self._contents(),
        )

        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )

        assert result["full"] is True
        # Matching blob_shas notwithstanding, every note still re-embeds — the
        # invalidation, not a forced work list, is what selects them.
        assert result["upserted"] == 2
        # embedding_version=None is the wholesale sweep: the pipeline really did
        # change, so every stamped row is stale.
        store.clear_note_stamps.assert_awaited_once_with(kb, embedding_version=None)
        # The pipeline version is durable BEFORE the first embed, so a crash
        # from here on resumes instead of re-deriving full=True.
        early = store.upsert_watermark.await_args_list[0].kwargs
        assert early["pipeline_version"].startswith("qwen3-embedding-8b")
        assert early["status"] == "indexing"
        assert early["indexed_commit"] == "old"  # not advanced yet

    @pytest.mark.asyncio
    async def test_interrupted_rebuild_resumes_and_skips_finished_notes(self):
        """The whole point: run 2 embeds only what run 1 didn't finish."""
        kb = uuid.uuid4()
        wm = KbWatermark(kb_id=kb, indexed_commit=None, pipeline_version="stale:1")
        gitea, store, svc = _make_deps(
            head="headsha",
            watermark=wm,
            tree=self._tree(),
            indexed={},
            contents=self._contents(),
        )
        # Run 1 dies after 'a' is stamped and before 'b' is.
        store.stamp_note_indexed.side_effect = [None, RuntimeError("pod terminated")]
        first = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )
        assert first["upserted"] == 1
        assert first["errors"] == 1

        # Run 2 sees the watermark run 1 wrote up front: same pipeline version,
        # so no forced rebuild, and 'a' carries a matching stamp.
        resumed_wm = KbWatermark(
            kb_id=kb,
            indexed_commit=None,
            pipeline_version=store.upsert_watermark.await_args_list[0].kwargs[
                "pipeline_version"
            ],
        )
        gitea2, store2, svc2 = _make_deps(
            head="headsha",
            watermark=resumed_wm,
            tree=self._tree(),
            indexed={"knowledge/a.md": "sa"},
            contents=self._contents(),
        )

        second = await reindex_kb(
            gitea_client=gitea2,
            store=store2,
            embedding_service=svc2,
            kb_id=kb,
            repo_name="r",
        )

        assert second["full"] is False
        assert second["upserted"] == 1  # 'b' only — 'a' was NOT re-embedded
        store2.clear_note_stamps.assert_not_awaited()
        embedded = [c.args[0] for c in svc2.embed_batch.await_args_list]
        assert not any("# a" in "".join(texts) for texts in embedded)

    @pytest.mark.asyncio
    async def test_never_completed_index_keeps_its_existing_stamps(self):
        """The live dev case: a first index killed by a rollout.

        ``pipeline_version`` is NULL only because no run ever *finished*, so the
        notes already stamped are current. Wiping them would re-embed exactly
        the work this change exists to keep — and on a 2635-note vault that
        wipe is also what blew the 60s statement timeout.
        """
        kb = uuid.uuid4()
        wm = KbWatermark(kb_id=kb, indexed_commit=None, pipeline_version=None)
        gitea, store, svc = _make_deps(
            head="headsha",
            watermark=wm,
            tree=self._tree(),
            indexed={"knowledge/a.md": "sa"},  # 'a' finished before the kill
            contents=self._contents(),
        )

        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )

        # Targeted sweep (rows from another embedding version), not wholesale.
        kwargs = store.clear_note_stamps.await_args.kwargs
        assert kwargs["embedding_version"] is not None
        # ...so 'a' keeps its stamp and only 'b' is embedded.
        assert result["upserted"] == 1
        embedded = [c.args[0] for c in svc.embed_batch.await_args_list]
        assert not any("# a" in "".join(texts) for texts in embedded)

    @pytest.mark.asyncio
    async def test_cleared_stamps_still_delete_removed_paths(self):
        """Invalidation must not swallow the deletion set.

        ``clear_note_stamps`` NULLs the stamp but keeps the row, so a path that
        left the tree is still visible to ``plan_reindex``'s delete arm.
        """
        kb = uuid.uuid4()
        wm = KbWatermark(kb_id=kb, indexed_commit="old", pipeline_version="stale:1")
        gitea, store, svc = _make_deps(
            head="headsha",
            watermark=wm,
            tree=[{"path": "knowledge/a.md", "type": "blob", "sha": "sa"}],
            indexed={"knowledge/a.md": "sa", "knowledge/gone.md": "sg"},
            contents={"knowledge/a.md": _note_md("a")},
        )

        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )

        assert result["deleted"] == 1
        store.delete_kb_note.assert_awaited_once_with(kb, "knowledge/gone.md")

    @pytest.mark.asyncio
    async def test_failed_invalidation_falls_back_to_forced_full_pass(self):
        """Resumability is an optimization; a correct work list is not.

        If the stamps cannot be cleared, the run must still re-embed every note
        rather than trusting a diff that would skip the stale-but-stamped ones.
        """
        kb = uuid.uuid4()
        wm = KbWatermark(kb_id=kb, indexed_commit="old", pipeline_version="stale:1")
        gitea, store, svc = _make_deps(
            head="headsha",
            watermark=wm,
            tree=self._tree(),
            indexed={"knowledge/a.md": "sa", "knowledge/b.md": "sb"},
            contents=self._contents(),
        )
        store.clear_note_stamps.side_effect = RuntimeError("read-only transaction")

        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )

        assert result["full"] is True
        assert result["upserted"] == 2  # forced, despite matching blob_shas


class TestReindexKbFailureHonesty:
    @pytest.mark.asyncio
    async def test_fetch_failure_counts_error_and_blocks_watermark(self):
        gitea, store, svc = _make_deps(
            head="headsha",
            tree=[{"path": "knowledge/a.md", "type": "blob", "sha": "s"}],
            contents={},  # get_file_content returns None
        )
        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=uuid.uuid4(),
            repo_name="r",
        )
        assert result["status"] == "partial"
        assert result["errors"] == 1
        assert "headsha" not in _watermark_commits(store)

    @pytest.mark.asyncio
    async def test_unparseable_note_is_skipped_not_fatal(self):
        gitea, store, svc = _make_deps(
            head="headsha",
            tree=[
                {"path": "knowledge/bad.md", "type": "blob", "sha": "s1"},
                {"path": "knowledge/good.md", "type": "blob", "sha": "s2"},
            ],
            contents={
                "knowledge/bad.md": "---\n: bad: [yaml\n---\nbody",
                "knowledge/good.md": _note_md("good"),
            },
        )
        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=uuid.uuid4(),
            repo_name="r",
        )
        # the malformed note is lint's problem, not the reindexer's — the good
        # note lands and the watermark advances
        assert result["status"] == "completed"
        assert result["skipped"] == 1
        assert result["upserted"] == 1
        assert _watermark_commits(store)[-1] == "headsha"

    @pytest.mark.asyncio
    async def test_unparseable_changed_note_drops_stale_indexed_version(self):
        kb = uuid.uuid4()
        gitea, store, svc = _make_deps(
            head="headsha",
            tree=[{"path": "knowledge/bad.md", "type": "blob", "sha": "new"}],
            indexed={"knowledge/bad.md": "old"},
            contents={"knowledge/bad.md": "---\n: bad: [yaml\n---\nbody"},
        )

        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )

        assert result["status"] == "completed"
        assert result["skipped"] == 1
        store.delete_kb_note.assert_awaited_once_with(kb, "knowledge/bad.md")

    @pytest.mark.asyncio
    async def test_embed_failure_counts_error_and_blocks_watermark(self):
        gitea, store, svc = _make_deps(
            head="headsha",
            tree=[{"path": "knowledge/a.md", "type": "blob", "sha": "s"}],
            contents={"knowledge/a.md": _note_md("a")},
        )
        svc.embed_batch = AsyncMock(side_effect=RuntimeError("provider down"))
        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=uuid.uuid4(),
            repo_name="r",
        )
        assert result["status"] == "partial"
        assert result["errors"] == 1
        store.upsert_kb_note.assert_not_awaited()  # embed-first ordering
        assert "headsha" not in _watermark_commits(store)

    @pytest.mark.asyncio
    async def test_chunk_write_failure_leaves_note_unstamped(self):
        """Live gap 2026-07-05: chunk INSERTs failed (missing pgvector codec)
        AFTER the note upsert had stamped blob_sha — the diff then saw the note
        as up-to-date with zero chunks. The stamp must not survive a chunk-write
        failure."""
        gitea, store, svc = _make_deps(
            head="headsha",
            tree=[{"path": "knowledge/a.md", "type": "blob", "sha": "s"}],
            contents={"knowledge/a.md": _note_md("a")},
        )
        store.replace_note_chunks.side_effect = RuntimeError(
            "invalid input for query argument $6"
        )
        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=uuid.uuid4(),
            repo_name="r",
        )
        assert result["status"] == "partial"
        assert result["errors"] == 1
        store.stamp_note_indexed.assert_not_awaited()
        assert "headsha" not in _watermark_commits(store)

    @pytest.mark.asyncio
    async def test_stamp_failure_counts_error_and_blocks_watermark(self):
        gitea, store, svc = _make_deps(
            head="headsha",
            tree=[{"path": "knowledge/a.md", "type": "blob", "sha": "s"}],
            contents={"knowledge/a.md": _note_md("a")},
        )
        store.stamp_note_indexed.side_effect = RuntimeError("db blip")
        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=uuid.uuid4(),
            repo_name="r",
        )
        assert result["status"] == "partial"
        assert result["errors"] == 1
        assert "headsha" not in _watermark_commits(store)


class TestReindexKbWedgeWarning:
    @pytest.mark.asyncio
    async def test_same_failure_four_sweeps_warns_exactly_once(self, caplog):
        kb = uuid.uuid4()
        wm = KbWatermark(
            kb_id=kb, indexed_commit="old", pipeline_version=CURRENT_VERSION
        )
        gitea, store, svc = _make_deps(
            head="h",
            watermark=wm,
            tree=[{"path": "knowledge/bad.md", "type": "blob", "sha": "s"}],
            contents={"knowledge/bad.md": _note_md("bad")},
        )
        store.upsert_kb_note.side_effect = RuntimeError("boom")  # generic, persistent

        warned = 0
        for streak in (1, 2, 3, 4, 5):
            # S0 computes the streak in SQL; the mock store reports it back.
            store.get_watermark.return_value = KbWatermark(
                kb_id=kb,
                indexed_commit="old",
                pipeline_version=CURRENT_VERSION,
                status="partial",
                error_streak=streak,
            )
            with caplog.at_level("WARNING", logger="orchestrator.services.kb_reindex"):
                caplog.clear()
                await reindex_kb(
                    gitea_client=gitea,
                    store=store,
                    embedding_service=svc,
                    kb_id=kb,
                    repo_name="r",
                )
            warned += sum("WEDGED" in r.message for r in caplog.records)
            fp = store.set_watermark_status.await_args.kwargs["error_fingerprint"]
            assert fp and len(fp) == 16
        assert warned == 1

    @pytest.mark.asyncio
    async def test_watermark_readback_failure_does_not_raise(self):
        """_warn_if_wedged's own get_watermark read-back is advisory-only —
        a DB blip there must not turn an otherwise-normal partial run into an
        unhandled exception. The run's *first* get_watermark call (line ~996,
        pre-existing and unrelated to the wedge check) still has to succeed
        or the run never reaches the partial branch at all, so only the
        second call — the wedge check's own — is made to fail."""
        kb = uuid.uuid4()
        wm = KbWatermark(
            kb_id=kb, indexed_commit="old", pipeline_version=CURRENT_VERSION
        )
        gitea, store, svc = _make_deps(
            head="h",
            watermark=wm,
            tree=[{"path": "knowledge/bad.md", "type": "blob", "sha": "s"}],
            contents={},  # fetch failure -> partial, errors=1
        )
        store.get_watermark.side_effect = [wm, RuntimeError("db down")]

        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )

        assert result["status"] == "partial"
        assert result["errors"] == 1

    @pytest.mark.asyncio
    async def test_delete_only_failure_still_fingerprints(self):
        """A run whose only failure is in the delete loop must still feed
        the fingerprint — otherwise a delete-only wedge would never
        accumulate a streak worth warning about."""
        kb = uuid.uuid4()
        wm = KbWatermark(
            kb_id=kb, indexed_commit="old", pipeline_version=CURRENT_VERSION
        )
        gitea, store, svc = _make_deps(
            head="h",
            watermark=wm,
            tree=[],  # nothing in the tree now — knowledge/old.md was deleted
            indexed={"knowledge/old.md": "sha-1"},
            contents={},
        )
        store.delete_kb_note.side_effect = RuntimeError("locked")

        result = await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )

        assert result["status"] == "partial"
        fp = store.set_watermark_status.await_args.kwargs["error_fingerprint"]
        assert fp is not None and len(fp) == 16


# =============================================================================
# reindex_kb — per-KB serialization (PR3.1)
# =============================================================================


class TestReindexKbSerialization:
    """Two post-merge triggers ~30s apart ran concurrent full rebuilds against
    the same KB on dev (interleaved delete+insert chunk batches). reindex_kb
    holds a per-KB asyncio lock; distinct KBs still run concurrently."""

    def _slow_deps(self):
        gitea, store, svc = _make_deps(
            head="headsha",
            tree=[{"path": "knowledge/a.md", "type": "blob", "sha": "s"}],
            contents={"knowledge/a.md": _note_md("a")},
        )
        state = {"active": 0, "max_active": 0}

        async def slow_tree(repo, ref):
            state["active"] += 1
            state["max_active"] = max(state["max_active"], state["active"])
            await asyncio.sleep(0.05)
            state["active"] -= 1
            return [{"path": "knowledge/a.md", "type": "blob", "sha": "s"}]

        gitea.list_tree = AsyncMock(side_effect=slow_tree)
        return gitea, store, svc, state

    @pytest.mark.asyncio
    async def test_same_kb_serializes(self):
        gitea, store, svc, state = self._slow_deps()
        kb = uuid.uuid4()
        await asyncio.gather(
            reindex_kb(
                gitea_client=gitea,
                store=store,
                embedding_service=svc,
                kb_id=kb,
                repo_name="r",
            ),
            reindex_kb(
                gitea_client=gitea,
                store=store,
                embedding_service=svc,
                kb_id=kb,
                repo_name="r",
            ),
        )
        assert state["max_active"] == 1, "same-KB reindex runs overlapped"

    @pytest.mark.asyncio
    async def test_distinct_kbs_run_concurrently(self):
        gitea, store, svc, state = self._slow_deps()
        await asyncio.gather(
            reindex_kb(
                gitea_client=gitea,
                store=store,
                embedding_service=svc,
                kb_id=uuid.uuid4(),
                repo_name="r",
            ),
            reindex_kb(
                gitea_client=gitea,
                store=store,
                embedding_service=svc,
                kb_id=uuid.uuid4(),
                repo_name="r",
            ),
        )
        assert state["max_active"] == 2, "distinct KBs were serialized"


# =============================================================================
# resolve_kb_repo — project → (repo_name, branch) for the KB vault
# =============================================================================


def _repo_db(repos_by_project=None, **roles):
    """A postgres double whose ``get_project_repositories`` answers per role.

    ``_repo_db(jobs=[...], knowledge=[...])`` for one project;
    ``_repo_db({project_id: {"jobs": [...]}})`` when the sweep needs several.
    An unlisted role (or project) answers ``[]`` — the real query's shape, and
    the reason a single ``return_value`` would let a role-blind resolver pass.
    """
    db = AsyncMock()
    table = (
        {str(pid): roles_ for pid, roles_ in repos_by_project.items()}
        if repos_by_project is not None
        else None
    )

    async def _by_role(project_id, role=None):
        entry = roles if table is None else table.get(str(project_id), {})
        return list(entry.get(role) or [])

    db.get_project_repositories.side_effect = _by_role
    return db


class TestResolveKbRepo:
    @pytest.mark.asyncio
    async def test_github_knowledge_repo_resolves_to_secret_free_descriptor(self):
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
            return_value={
                "id": datasource_id,
                "config": {"root_path": "knowledge", "native_project_id": "p"},
            }
        )

        resolved = await resolve_kb_repo(db, "p")

        assert resolved == KbRepoRef(
            forge="github",
            repo_url="https://github.com/acme/design-vault.git",
            owner="acme",
            repo="design-vault",
            branch="vault/main",
            credential_ref=str(datasource_id),
        )
        assert "credential_ref" in repr(resolved)
        assert "token" not in repr(resolved)

    @pytest.mark.asyncio
    async def test_native_datasource_forge_override_supports_github_enterprise(self):
        datasource_id = uuid.uuid4()
        db = _repo_db(
            knowledge=[
                {
                    "name": "Design Vault",
                    "repo_url": "https://github.corp.example/acme/design-vault",
                    "branch": "main",
                }
            ]
        )
        db.get_native_project_kb_datasource_ref = AsyncMock(
            return_value={
                "id": datasource_id,
                "config": {
                    "root_path": "knowledge",
                    "native_project_id": "p",
                    "forge": "github",
                },
            }
        )

        resolved = await resolve_kb_repo(db, "p")

        assert resolved is not None
        assert resolved.forge == "github"
        assert resolved.owner == "acme"
        assert resolved.repo == "design-vault"
        assert resolved.credential_ref == str(datasource_id)

    @pytest.mark.asyncio
    async def test_first_jobs_role_repo_wins(self):
        """No project has a knowledge repo yet, so this is today's behaviour
        and it must stay bit-for-bit intact: the oldest jobs repo."""
        db = _repo_db(
            jobs=[
                {"name": "project-abc-jobs", "branch": "main"},
                {"name": "project-abc-jobs-2", "branch": "dev"},
            ]
        )
        result = await resolve_kb_repo(db, "proj-id")
        assert result == KbRepoRef(
            forge="gitea",
            repo_url="",
            owner="",
            repo="project-abc-jobs",
            branch="main",
        )
        assert db.get_project_repositories.await_args_list[-1].kwargs == {
            "role": "jobs"
        }

    @pytest.mark.asyncio
    async def test_knowledge_repo_wins_over_jobs_repo(self):
        """A project that has both resolves to the knowledge repo — and the
        jobs repo is never even consulted, so precedence can't be an accident
        of ordering."""
        db = _repo_db(
            knowledge=[{"name": "project-abc-knowledge", "branch": "main"}],
            jobs=[{"name": "project-abc-jobs", "branch": "main"}],
        )
        result = await resolve_kb_repo(db, "p")
        assert result is not None
        assert (result.forge, result.repo, result.branch) == (
            "gitea",
            "project-abc-knowledge",
            "main",
        )
        roles = [
            c.kwargs.get("role") for c in db.get_project_repositories.await_args_list
        ]
        assert roles == ["knowledge"]

    @pytest.mark.asyncio
    async def test_knowledge_repo_branch_is_honoured(self):
        db = _repo_db(knowledge=[{"name": "kb-repo", "branch": "vault"}])
        result = await resolve_kb_repo(db, "p")
        assert result is not None
        assert (result.repo, result.branch) == ("kb-repo", "vault")

    @pytest.mark.asyncio
    async def test_missing_branch_defaults_to_main(self):
        db = _repo_db(jobs=[{"name": "project-abc-jobs", "branch": None}])
        result = await resolve_kb_repo(db, "p")
        assert result is not None
        assert (result.repo, result.branch) == ("project-abc-jobs", "main")

    @pytest.mark.asyncio
    async def test_no_repos_returns_none(self):
        db = _repo_db()
        assert await resolve_kb_repo(db, "p") is None
        roles = [
            c.kwargs.get("role") for c in db.get_project_repositories.await_args_list
        ]
        assert roles == ["knowledge", "jobs"], "both roles must be tried"


# =============================================================================
# kb_sweep_tick — one sweep over every project KB (leader-gated caller)
# =============================================================================


class TestKbSweepTick:
    def _db(self, roles_by_project=None):
        """Two projects on the work list, each with only a jobs repo (today's
        fleet). ``fetch`` enumerates project ids only — the repo per project
        comes from the resolver."""
        self.p1, self.p2 = uuid.uuid4(), uuid.uuid4()
        table = roles_by_project or {
            self.p1: {"jobs": [{"name": "project-1-jobs", "branch": "main"}]},
            self.p2: {"jobs": [{"name": "project-2-jobs", "branch": "main"}]},
        }
        db = _repo_db(table)
        db.fetch.return_value = [{"project_id": pid} for pid in table]
        return db

    @pytest.mark.asyncio
    async def test_reindexes_every_project_kb(self):
        postgres_db = self._db()
        reindex_fn = AsyncMock(return_value={"status": "completed", "upserted": 1})
        n = await kb_sweep_tick(
            postgres_db=postgres_db,
            store=MagicMock(),
            gitea_client=MagicMock(),
            embedding_service=MagicMock(),
            reindex_fn=reindex_fn,
        )
        assert n == 2
        assert reindex_fn.await_count == 2
        kwargs = reindex_fn.await_args_list[0][1]
        assert kwargs["kb_id"] == self.p1
        assert kwargs["repo_name"] == "project-1-jobs"
        assert kwargs["branch"] == "main"
        # the work list enumerates KB-capable projects; it must not re-apply
        # the precedence rule (that lives in resolve_kb_repo alone)
        query = postgres_db.fetch.call_args[0][0]
        assert "project_repositories" in query
        assert "jobs" in str(postgres_db.fetch.call_args[0])
        assert "knowledge" in str(postgres_db.fetch.call_args[0])

    @pytest.mark.asyncio
    async def test_materialization_retry_precedes_reindex_and_projection_settlement(
        self,
    ):
        postgres_db = self._db(
            {
                (project_id := uuid.uuid4()): {
                    "jobs": [{"name": "project-retry-jobs", "branch": "main"}]
                }
            }
        )
        intent = {
            "id": uuid.uuid4(),
            "project_id": project_id,
            "note_id": "bp08-retry",
            "content": _note_md("bp08-retry"),
            "attempt_token": uuid.uuid4(),
        }
        postgres_db.claim_due_knowledge_materializations.return_value = [intent]
        order: list[str] = []

        async def retry(**kwargs):
            order.append("canonical-retry")
            return {"canonical_state": "canonical"}

        async def reindex(**kwargs):
            order.append("reindex")
            return {"status": "completed"}

        async def settle(project):
            assert str(project) == str(project_id)
            order.append("projection-settled")
            return 1

        postgres_db.mark_knowledge_projections_synced.side_effect = settle
        with patch(
            "orchestrator.services.kb_materialize.retry_knowledge_materialization_intent",
            side_effect=retry,
        ):
            await kb_sweep_tick(
                postgres_db=postgres_db,
                store=MagicMock(),
                gitea_client=MagicMock(),
                embedding_service=MagicMock(),
                reindex_fn=reindex,
            )

        assert order == ["canonical-retry", "reindex", "projection-settled"]

    @pytest.mark.asyncio
    async def test_sweep_only_enumerates_active_projects(self):
        postgres_db = self._db()

        await kb_sweep_tick(
            postgres_db=postgres_db,
            store=MagicMock(),
            gitea_client=MagicMock(),
            embedding_service=MagicMock(),
            reindex_fn=AsyncMock(return_value={"status": "up-to-date"}),
        )

        query = " ".join(postgres_db.fetch.call_args.args[0].lower().split())
        assert "from project_repositories as pr" in query
        assert "join projects as p on p.id = pr.project_id" in query
        assert "p.status = 'active'" in query

    @pytest.mark.asyncio
    async def test_sweep_accepts_asyncpg_record_like_project_rows(self):
        class RecordLike:
            def __init__(self, project_id):
                self.project_id = project_id

            def __getitem__(self, key):
                if key != "project_id":
                    raise KeyError(key)
                return self.project_id

        postgres_db = self._db()
        postgres_db.fetch.return_value = [RecordLike(self.p1)]
        reindex_fn = AsyncMock(return_value={"status": "up-to-date"})

        await kb_sweep_tick(
            postgres_db=postgres_db,
            store=MagicMock(),
            gitea_client=MagicMock(),
            embedding_service=MagicMock(),
            reindex_fn=reindex_fn,
        )

        reindex_fn.assert_awaited_once()
        assert reindex_fn.await_args.kwargs["kb_id"] == self.p1

    @pytest.mark.asyncio
    async def test_blocked_native_does_not_delay_external_first_attempt(self):
        postgres_db = self._db()
        datasource_id = uuid.uuid4()
        postgres_db.list_datasources.return_value = [
            {
                "id": datasource_id,
                "type": "kb",
                "connection_url": "https://example.test/team-docs.git",
                "credentials": {},
                "config": {},
            }
        ]
        native_started = asyncio.Event()
        release_native = asyncio.Event()
        external_started = asyncio.Event()

        async def blocked_native(**_kwargs):
            native_started.set()
            await release_native.wait()
            return {"status": "up-to-date"}

        async def external_indexer(*_args, **_kwargs):
            external_started.set()
            return {"status": "up-to-date"}

        with patch(
            "orchestrator.services.kb_datasources.reindex_kb_datasource",
            side_effect=external_indexer,
        ):
            sweep = asyncio.create_task(
                kb_sweep_tick(
                    postgres_db=postgres_db,
                    store=MagicMock(),
                    gitea_client=MagicMock(),
                    embedding_service=MagicMock(),
                    reindex_fn=blocked_native,
                )
            )
            reached_external = False
            try:
                await native_started.wait()
                try:
                    await asyncio.wait_for(external_started.wait(), timeout=0.05)
                    reached_external = True
                except TimeoutError:
                    pass
            finally:
                release_native.set()
                await sweep

        assert reached_external, "native project work starved the external phase"

    @pytest.mark.asyncio
    async def test_native_enumeration_failure_does_not_abort_external_phase(self):
        datasource_id = uuid.uuid4()
        postgres_db = AsyncMock()
        postgres_db.fetch.side_effect = RuntimeError("projects unavailable")
        postgres_db.list_datasources.return_value = [
            {
                "id": datasource_id,
                "type": "kb",
                "connection_url": "https://example.test/team-docs.git",
                "credentials": {},
                "config": {},
            }
        ]
        external_indexer = AsyncMock(return_value={"status": "completed"})

        with patch(
            "orchestrator.services.kb_datasources.reindex_kb_datasource",
            external_indexer,
        ):
            worked = await kb_sweep_tick(
                postgres_db=postgres_db,
                store=MagicMock(),
                gitea_client=MagicMock(),
                embedding_service=MagicMock(),
                reindex_fn=AsyncMock(),
            )

        assert worked == 1
        external_indexer.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_external_enumeration_failure_does_not_abort_native_phase(self):
        postgres_db = self._db()
        postgres_db.list_datasources.side_effect = RuntimeError(
            "datasources unavailable"
        )
        native_indexer = AsyncMock(return_value={"status": "completed"})

        worked = await kb_sweep_tick(
            postgres_db=postgres_db,
            store=MagicMock(),
            gitea_client=MagicMock(),
            embedding_service=MagicMock(),
            reindex_fn=native_indexer,
        )

        assert worked == 2
        assert native_indexer.await_count == 2

    @pytest.mark.asyncio
    async def test_github_project_uses_selected_client_for_reindex(self):
        project_id = uuid.uuid4()
        datasource_id = uuid.uuid4()
        postgres_db = self._db(
            {
                project_id: {
                    "knowledge": [
                        {
                            "name": "Design Vault",
                            "repo_url": "https://github.com/acme/design-vault.git",
                            "branch": "main",
                        }
                    ]
                }
            }
        )
        postgres_db.get_native_project_kb_datasource_ref = AsyncMock(
            return_value={"id": datasource_id, "config": {"root_path": "knowledge"}}
        )
        gitea = MagicMock()
        github = MagicMock()
        reindex_fn = AsyncMock(return_value={"status": "completed"})

        with patch(
            "orchestrator.services.kb_reindex.kb_client_for_repo",
            AsyncMock(return_value=github),
        ) as select:
            await kb_sweep_tick(
                postgres_db=postgres_db,
                store=MagicMock(),
                gitea_client=gitea,
                embedding_service=MagicMock(),
                reindex_fn=reindex_fn,
            )

        ref = await resolve_kb_repo(postgres_db, str(project_id))
        assert ref is not None
        select.assert_awaited_once_with(postgres_db, gitea, ref)
        assert reindex_fn.await_args.kwargs["gitea_client"] is github
        assert reindex_fn.await_args.kwargs["repo_name"] == "design-vault"

    @pytest.mark.asyncio
    async def test_sweep_picks_the_repo_the_resolver_picks(self):
        """The §10 hazard: if the sweep resolved a project's repo differently
        from every other consumer, the KB would break silently. Project 1 has
        both repos and project 2 only a jobs repo — the sweep must reindex
        exactly what resolve_kb_repo hands back for each."""
        p1, p2 = uuid.uuid4(), uuid.uuid4()
        postgres_db = self._db(
            {
                p1: {
                    "knowledge": [{"name": "project-1-knowledge", "branch": "main"}],
                    "jobs": [{"name": "project-1-jobs", "branch": "main"}],
                },
                p2: {"jobs": [{"name": "project-2-jobs", "branch": "dev"}]},
            }
        )
        reindex_fn = AsyncMock(return_value={"status": "completed"})
        await kb_sweep_tick(
            postgres_db=postgres_db,
            store=MagicMock(),
            gitea_client=MagicMock(),
            embedding_service=MagicMock(),
            reindex_fn=reindex_fn,
        )
        swept = {
            c.kwargs["kb_id"]: (c.kwargs["repo_name"], c.kwargs["branch"])
            for c in reindex_fn.await_args_list
        }
        resolved = {}
        for project_id in (p1, p2):
            ref = await resolve_kb_repo(postgres_db, str(project_id))
            assert ref is not None
            resolved[project_id] = (ref.repo, ref.branch)
        assert swept == {
            p1: resolved[p1],
            p2: resolved[p2],
        }
        assert swept[p1] == ("project-1-knowledge", "main")
        assert swept[p2] == ("project-2-jobs", "dev")

    @pytest.mark.asyncio
    async def test_project_whose_repo_vanished_is_skipped_not_reindexed(self):
        """Enumeration and resolution are two queries, so a repo can be
        detached between them. Skip that project rather than reindex a KB
        with no repo behind it."""
        gone = uuid.uuid4()
        postgres_db = self._db({gone: {}})
        reindex_fn = AsyncMock(return_value={"status": "completed"})
        n = await kb_sweep_tick(
            postgres_db=postgres_db,
            store=MagicMock(),
            gitea_client=MagicMock(),
            embedding_service=MagicMock(),
            reindex_fn=reindex_fn,
        )
        assert n == 0
        reindex_fn.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_up_to_date_not_counted_as_work(self):
        postgres_db = self._db()
        reindex_fn = AsyncMock(return_value={"status": "up-to-date"})
        n = await kb_sweep_tick(
            postgres_db=postgres_db,
            store=MagicMock(),
            gitea_client=MagicMock(),
            embedding_service=MagicMock(),
            reindex_fn=reindex_fn,
        )
        assert n == 0
        assert reindex_fn.await_count == 2  # still checked both

    @pytest.mark.asyncio
    async def test_one_kb_failure_does_not_kill_the_tick(self):
        postgres_db = self._db()
        reindex_fn = AsyncMock(
            side_effect=[RuntimeError("boom"), {"status": "completed"}]
        )
        n = await kb_sweep_tick(
            postgres_db=postgres_db,
            store=MagicMock(),
            gitea_client=MagicMock(),
            embedding_service=MagicMock(),
            reindex_fn=reindex_fn,
        )
        assert n == 1  # the second KB still reindexed
        assert reindex_fn.await_count == 2

    @pytest.mark.asyncio
    async def test_one_unresolvable_project_does_not_starve_the_rest(self):
        """A resolution failure is inside the same per-project guard as a
        reindex failure — one broken project must not cost the sweep."""
        postgres_db = self._db()
        broken, healthy = self.p1, self.p2

        async def _by_role(project_id, role=None):
            if str(project_id) == str(broken):
                raise RuntimeError("pg hiccup")
            return (
                [{"name": "project-2-jobs", "branch": "main"}] if role == "jobs" else []
            )

        postgres_db.get_project_repositories.side_effect = _by_role
        reindex_fn = AsyncMock(return_value={"status": "completed"})
        n = await kb_sweep_tick(
            postgres_db=postgres_db,
            store=MagicMock(),
            gitea_client=MagicMock(),
            embedding_service=MagicMock(),
            reindex_fn=reindex_fn,
        )
        assert n == 1
        assert reindex_fn.await_args.kwargs["kb_id"] == healthy

    @pytest.mark.asyncio
    async def test_external_datasource_is_swept_once_under_datasource_id(
        self, monkeypatch
    ):
        monkeypatch.setenv("KB_GIT_ALLOWED_HOSTS", "example.test")
        datasource_id = uuid.uuid4()
        postgres_db = AsyncMock()
        postgres_db.fetch.return_value = []
        postgres_db.list_datasources.return_value = [
            {
                "id": datasource_id,
                "type": "kb",
                "name": "Team Docs",
                "connection_url": "https://example.test/team-docs.git",
                "credentials": {"token": "orchestrator-only"},
                "default_branch": "main",
                "config": {"root_path": "vault"},
            }
        ]
        reindex_fn = AsyncMock(return_value={"status": "up-to-date"})

        worked = await kb_sweep_tick(
            postgres_db=postgres_db,
            store=MagicMock(),
            gitea_client=MagicMock(),
            embedding_service=MagicMock(),
            reindex_fn=reindex_fn,
        )

        assert worked == 0
        kwargs = reindex_fn.await_args.kwargs
        assert kwargs["kb_id"] == datasource_id
        assert kwargs["root_path"] == "vault"
        assert kwargs["source_label"] == f"datasource:{datasource_id}"
        assert kwargs["source"].label == f"datasource:{datasource_id}"

    @pytest.mark.asyncio
    async def test_external_sweep_shares_the_global_reindex_limit(self, monkeypatch):
        import orchestrator.services.kb_datasources as datasource_service

        monkeypatch.setenv("KB_GIT_ALLOWED_HOSTS", "example.test")
        datasource_id = uuid.uuid4()
        postgres_db = AsyncMock()
        postgres_db.fetch.return_value = []
        postgres_db.list_datasources.return_value = [
            {
                "id": datasource_id,
                "type": "kb",
                "name": "Team Docs",
                "connection_url": "https://example.test/team-docs.git",
                "credentials": {},
                "default_branch": "main",
                "config": {"root_path": "vault"},
            }
        ]
        reindex_started = asyncio.Event()

        async def fake_reindex(**_kwargs):
            reindex_started.set()
            return {"status": "up-to-date"}

        limiter = asyncio.Semaphore(1)
        await limiter.acquire()  # represent an in-flight create/manual build
        monkeypatch.setattr(datasource_service, "_external_reindex_semaphore", limiter)

        sweep = asyncio.create_task(
            kb_sweep_tick(
                postgres_db=postgres_db,
                store=MagicMock(),
                gitea_client=MagicMock(),
                embedding_service=MagicMock(),
                reindex_fn=fake_reindex,
            )
        )
        await asyncio.sleep(0)
        assert not reindex_started.is_set()

        limiter.release()
        assert await sweep == 0
        assert reindex_started.is_set()


# =============================================================================
# reindex_kb — R-1 ghost reconciliation pass
# =============================================================================


class TestReindexKbReconciliation:
    def _tree(self):
        return [{"path": "knowledge/keep.md", "type": "blob", "sha": "s1"}]

    async def _run(self, kb, store, gitea, svc):
        return await reindex_kb(
            gitea_client=gitea,
            store=store,
            embedding_service=svc,
            kb_id=kb,
            repo_name="r",
        )

    @pytest.mark.asyncio
    async def test_reconciles_orphans_and_reports_count(self):
        # No upsert/delete work (indexed == tree), so the reconciliation pass is
        # isolated: it must still fire, keyed on the KB id (== project_id) with
        # the tree's slug set, and surface its count in the summary.
        kb = uuid.uuid4()
        wm = KbWatermark(
            kb_id=kb, indexed_commit="old", pipeline_version=CURRENT_VERSION
        )
        gitea, store, svc = _make_deps(
            head="headsha",
            watermark=wm,
            tree=self._tree(),
            indexed={"knowledge/keep.md": "s1"},
        )
        store.reconcile_orphans.return_value = 3
        result = await self._run(kb, store, gitea, svc)
        store.reconcile_orphans.assert_awaited_once()
        kwargs = store.reconcile_orphans.await_args.kwargs
        assert kwargs["project_id"] == kb
        assert set(kwargs["tree_slugs"]) == {"keep"}  # basename minus .md
        assert result["reconciled"] == 3

    @pytest.mark.asyncio
    async def test_no_reconcile_when_tree_fetch_fails(self):
        gitea, store, svc = _make_deps(head="headsha")
        gitea.list_tree.return_value = None
        result = await self._run(uuid.uuid4(), store, gitea, svc)
        store.reconcile_orphans.assert_not_awaited()
        assert result["status"] == "tree-fetch-failed"

    @pytest.mark.asyncio
    async def test_reconcile_failure_is_non_fatal(self):
        # A hygiene pass must never wedge the watermark or count as an error.
        kb = uuid.uuid4()
        wm = KbWatermark(
            kb_id=kb, indexed_commit="old", pipeline_version=CURRENT_VERSION
        )
        gitea, store, svc = _make_deps(
            head="headsha",
            watermark=wm,
            tree=self._tree(),
            indexed={"knowledge/keep.md": "s1"},
        )
        store.reconcile_orphans.side_effect = RuntimeError("db down")
        result = await self._run(kb, store, gitea, svc)
        assert result["status"] == "completed"  # watermark still advanced
        assert result["errors"] == 0
        assert result["reconciled"] == 0


def _function_body(src: str, signature: str) -> str:
    """Everything from ``signature`` to the next top-level def.

    Previously a flat 3000-character window, which silently stopped covering
    the KB trigger as soon as anything was inserted above it — the assertion
    then failed for a function that was still perfectly correct. Slicing to
    the real end of the function checks the whole body instead of a prefix,
    so this guard cannot be defeated by pushing code past a byte count.
    """
    import re

    body = src.split(signature, 1)[1]
    end = re.search(r"\n(?=(?:async )?def )", body)
    return body[: end.start()] if end else body


class TestPostJobReindexTriggerResolvesItsOwnRepo:
    """Guard for knowledge-base/knowledge/features/knowledge_base_repo_separation.md §10a.

    ``_reindex_project_kb`` resolves the vault repo (knowledge-role first,
    jobs as fallback) **only when it is not handed a ``repo_name``**. The
    post-job KB-freshness trigger used to pass ``job["repo_name"]``, pinning
    the reindex to an execution repo rather than the project's knowledge repo.

    That failure is silent and destructive rather than loud: ``plan_reindex``
    treats every indexed path absent from the tree as a delete, so reindexing
    against a repo with no ``knowledge/`` drops the project's entire chunk
    index, and the leader-gated sweep rebuilds it minutes later. The visible
    symptom is a search index that flaps empty on every loop job, reported by
    nothing louder than a non-fatal warning.

    There is no unit seam on the closure (it is nested in the completion
    handler and fired via ``asyncio.create_task``), so this asserts on the
    source. Coarse, but it fails the moment someone re-pins the repo.
    """

    def _src(self, *parts: str) -> str:
        import pathlib

        return (
            pathlib.Path(__file__).resolve().parents[1].joinpath("src", *parts)
        ).read_text(encoding="utf-8")

    def _composition_src(self, module: str | None = None) -> str:
        """The application composition that ``main.py`` held before R1.B12.

        One module's source, or the whole ``orchestrator.application``
        package concatenated when ``module`` is omitted.
        """
        import pathlib

        package = (
            pathlib.Path(__file__)
            .resolve()
            .parents[1]
            .joinpath("src", "orchestrator", "application")
        )
        if module is not None:
            return package.joinpath(module).read_text(encoding="utf-8")
        return "\n".join(
            path.read_text(encoding="utf-8") for path in sorted(package.glob("*.py"))
        )

    def test_trigger_does_not_pin_repo_name(self):
        # R1.B03 moved the callee into orchestrator.services.knowledge_index;
        # R1.B07 then moved this caller out of `main` into the loop engine and
        # turned the call into a port. The guard follows the caller, as its own
        # docstring instructs — and now checks BOTH halves, because the project
        # id and the absent repo_name are decided in two places.
        hook = self._src("orchestrator", "services", "project_loop_spawn.py")
        assert "async def record_loop_job_outcome" in hook, (
            "post-job outcome hook not found — if it was renamed, move this "
            "guard with it rather than deleting it (see §10a)."
        )
        body = _function_body(hook, "async def record_loop_job_outcome")
        # Compare whitespace-insensitively — the call is wrapped across lines,
        # and formatting is not the thing under test.
        compact = re.sub(r"\s+", "", body)
        assert "dependencies.reindex_project_kb(pid)" in compact, (
            "The post-job KB trigger must call reindex_project_kb with the "
            "project id and no repo_name so it resolves the vault repo itself. "
            "Passing the job's repo_name pins it to an execution repo and wipes "
            "the chunk index for any project with a knowledge repo. See §10a."
        )
        assert "repo_name=" not in body, (
            "repo_name= reappeared in the post-job KB trigger — this is the "
            "exact §10a regression: silent chunk-index wipe."
        )
        # The other half: the application binds that port, and a repo_name
        # smuggled into the binding would be just as silent.
        binding = _function_body(
            self._composition_src("workflows.py"), "def project_loop_dependencies"
        )
        compact_binding = re.sub(r"\s+", "", binding)
        assert "reindex_project_kb=(lambdaproject_id:" in compact_binding, (
            "The loop engine's KB port must be bound to a one-argument "
            "project-id call. See §10a."
        )
        assert "repo_name" not in binding, (
            "repo_name reappeared in the loop engine's KB port binding — the "
            "§10a regression, moved one hop out. "
        )

    def test_no_caller_pins_repo_name_to_the_jobs_repo(self):
        # The other half of the trap: repo_name is a legitimate parameter, but
        # feeding it the *job's* repo is never right for a project-scoped KB.
        src = self._composition_src()
        assert "def project_loop_dependencies" in src
        assert 'repo_name=job.get("repo_name")' not in src, (
            "A caller is passing the job's execution repo into a KB reindex. "
            "Project KB resolution must win. See §10a."
        )


def _index_deps(db) -> KnowledgeIndexDependencies:
    """What main's ``_knowledge_index_dependencies()`` factory hands the op.

    ``store`` is the app pool the projection ledger is settled through
    (formerly ``orchestrator.main.postgres_db``) and ``vector_db`` the pgvector
    pool the KnowledgeStore is built on (formerly
    ``orchestrator.main.vector_db``); both used to arrive as patched module
    globals.
    """
    return KnowledgeIndexDependencies(
        store=db,
        vector_db=MagicMock(),
        gitea_client=MagicMock(),
        logger=logging.getLogger("tests.kb_reindex"),
        tasks=KbDatasourceTaskRegistry(),
        inject_system_kb_embedding_profile=AsyncMock(return_value=None),
    )


class TestManualReindexProjectionSettlement:
    """A successful direct reindex closes the same ledger as the sweep."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["completed", "up-to-date"])
    async def test_success_settles_latest_canonical_intents(self, status):
        from orchestrator.services.knowledge_index import reindex_project_kb

        project_id = str(uuid.uuid4())
        db = AsyncMock()
        db.mark_knowledge_projections_synced.return_value = 2
        reindex = AsyncMock(return_value={"status": status, "upserted": 1})
        with (
            patch(
                "orchestrator.services.knowledge_index.build_kb_embedding_service",
                AsyncMock(return_value=MagicMock()),
            ),
            patch("orchestrator.services.kb_reindex.reindex_kb", reindex),
        ):
            result = await reindex_project_kb(
                project_id,
                repo_name="project-knowledge",
                branch="main",
                dependencies=_index_deps(db),
            )

        assert result["status"] == status
        assert result["projection_intents_synced"] == 2
        db.mark_knowledge_projections_synced.assert_awaited_once_with(project_id)

    @pytest.mark.asyncio
    async def test_partial_reindex_does_not_claim_projection_convergence(self):
        from orchestrator.services.knowledge_index import reindex_project_kb

        project_id = str(uuid.uuid4())
        db = AsyncMock()
        with (
            patch(
                "orchestrator.services.knowledge_index.build_kb_embedding_service",
                AsyncMock(return_value=MagicMock()),
            ),
            patch(
                "orchestrator.services.kb_reindex.reindex_kb",
                AsyncMock(return_value={"status": "partial", "errors": 1}),
            ),
        ):
            result = await reindex_project_kb(
                project_id,
                repo_name="project-knowledge",
                branch="main",
                dependencies=_index_deps(db),
            )

        assert result == {"status": "partial", "errors": 1}
        db.mark_knowledge_projections_synced.assert_not_awaited()


# =============================================================================
# kb_reindex_sweeper_loop — the sweep is also the crash-recovery path
# =============================================================================


class TestSweeperFirstPassIsPrompt:
    """Indexing is an in-process task, so a rollout kills it and this loop is
    the only thing that brings it back. The loop used to wait a full tick before
    its first pass, so a KB interrupted by a deploy stayed dead for 15 minutes.
    """

    def test_first_delay_is_much_shorter_than_the_steady_tick(self):
        assert FIRST_SWEEP_DELAY_SECONDS < SWEEP_TICK_SECONDS
        assert FIRST_SWEEP_DELAY_SECONDS <= 60

    @pytest.mark.asyncio
    async def test_first_sweep_waits_the_short_delay_then_the_full_tick(self):
        waits: list[float] = []
        shutdown = asyncio.Event()

        async def fake_wait_for(_awaitable, timeout):
            # Close the coroutine we're not awaiting so the loop doesn't warn.
            _awaitable.close()
            waits.append(timeout)
            if len(waits) >= 3:
                shutdown.set()
                return True
            raise asyncio.TimeoutError

        sweeps = 0

        async def fake_tick(**_kwargs):
            nonlocal sweeps
            sweeps += 1
            return 0

        with (
            patch("orchestrator.services.kb_reindex.asyncio.wait_for", fake_wait_for),
            patch("orchestrator.services.kb_reindex.kb_sweep_tick", fake_tick),
        ):
            await kb_reindex_sweeper_loop(
                postgres_db=AsyncMock(),
                store=AsyncMock(),
                gitea_client=AsyncMock(),
                shutdown_event=shutdown,
                embedding_service_factory=AsyncMock(return_value=MagicMock()),
            )

        # The first pass comes quickly; every later one is on the coarse cadence.
        assert waits[0] == FIRST_SWEEP_DELAY_SECONDS
        assert waits[1:] == [SWEEP_TICK_SECONDS, SWEEP_TICK_SECONDS]
        assert sweeps == 2
