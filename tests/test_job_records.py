"""Tests for the general per-job change record (§5/§6.5 of
knowledge-base/knowledge/features/workspace_and_change_records.md).

``services.job_records`` generalises the loop retro: every repo-backed job
leaves exactly one record on the project repo's ``main`` on reaching a
terminal status, with the §5 ``changes:`` block (orchestrator-verified
entries ``verified: true``, agent-declared claims ``verified: false``).
The loop path is pinned unchanged: ``write_loop_retro`` renders
byte-identically to the pre-extraction writer (see also
tests/test_loop_merge.py) and stays importable from
``services.project_loops``.

Gitea is mocked (pattern: tests/test_loop_merge.py); the vector store is a
stand-in whose ``acquire()`` yields a conn with a canned ``fetch`` (pattern:
tests/test_knowledge_access.py).
"""

from __future__ import annotations

import base64
import json
import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
import yaml

import orchestrator.services.job_records as job_records
import orchestrator.services.project_loops as project_loops
from orchestrator.services.completion import write_job_change_record
from orchestrator.services.job_records import (
    derive_changes,
    job_delivered_nothing,
    record_exists_for_job,
    render_job_record,
    write_job_record,
    write_loop_retro,
)

JOB_ID = uuid.UUID("abcdef12-3456-7890-abcd-ef1234567890")
PROJECT_ID = uuid.UUID("68137e29-1111-2222-3333-444444444444")
NOW = datetime(2026, 8, 1, 9, 14, 22, tzinfo=UTC)


def _make_gitea(*, change_ok: bool = True, listing: list | None = None) -> MagicMock:
    g = MagicMock()
    g.change_files = AsyncMock(return_value=change_ok)
    g.list_contents = AsyncMock(return_value=listing if listing is not None else [])
    return g


def _make_vector_db(rows: list[dict] | None = None, *, broken: bool = False):
    """Stand-in for ``vector_db``: ``acquire()`` yields a conn whose
    ``fetch`` returns canned ``knowledge_index`` rows (or blows up when
    ``broken`` — the writer must degrade to "no knowledge entry")."""
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=rows or [])
    cm = AsyncMock()
    if broken:
        cm.__aenter__.side_effect = RuntimeError("vector store down")
    else:
        cm.__aenter__.return_value = conn
    cm.__aexit__.return_value = False
    db = MagicMock()
    db.acquire = lambda: cm
    db._conn = conn
    return db


def _make_record_db(*, inserted: bool = True) -> MagicMock:
    db = MagicMock()
    db.create_job_change_record = AsyncMock(return_value=inserted)
    return db


def _job(**over) -> dict:
    row = {
        "id": JOB_ID,
        "repo_name": "project-68137e29-jobs",
        "branch_name": "job/abcdef12",
        "project_id": PROJECT_ID,
        "config_name": "developer",
        "description": "Implement line recovery\nSecond line is not the title.",
        "freeze_data": {"notes": "Shipped AC-11; tests green."},
        "context": {},
    }
    row.update(over)
    return row


def _written_text(g: MagicMock) -> str:
    entry = g.change_files.await_args.args[2][0]
    return base64.b64decode(entry["content_b64"]).decode()


def _frontmatter(text: str) -> dict:
    """Parse the record's YAML frontmatter — asserting it IS valid YAML."""
    assert text.startswith("---\n")
    fm = text[4:].split("\n---\n", 1)[0]
    data = yaml.safe_load(fm)
    assert isinstance(data, dict)
    return data


# =============================================================================
# Rendering
# =============================================================================


class TestRenderJobRecord:
    def test_loop_retro_shape_is_byte_identical_to_legacy_writer(self) -> None:
        """The §6.5 extraction must not move a single byte of the loop
        retro — this reconstructs the pre-move writer's output literally."""
        created = "2026-08-01T09:14:22+00:00"
        rendered = render_job_record(
            record_type="retro",
            iteration=15,
            role="developer",
            job_id=str(JOB_ID),
            branch="job/abcdef12",
            status="completed",
            merge_status="merged",
            merge_sha="deadbeef" * 5,
            created=created,
            title="# Loop iter 15 · developer",
            description="Implement line recovery",
            notes="Shipped AC-11; tests green.",
        )
        legacy_lines = [
            "---",
            "type: retro",
            "iteration: 15",
            "role: developer",
            f"job: {JOB_ID}",
            "branch: job/abcdef12",
            "status: completed",
            "merge_status: merged",
            f"merge_sha: {'deadbeef' * 5}",
            f"created: {created}",
            "---",
            "",
            "# Loop iter 15 · developer",
            "",
            "Implement line recovery",
            "",
            "## Agent completion notes",
            "",
            "Shipped AC-11; tests green.",
        ]
        assert rendered == "\n".join(legacy_lines) + "\n"

    def test_loop_retro_failed_error_section_matches_legacy(self) -> None:
        rendered = render_job_record(
            record_type="retro",
            iteration=4,
            role="developer",
            job_id=str(JOB_ID),
            branch=None,
            status="failed",
            merge_status="skipped",
            merge_sha=None,
            created="2026-08-01T09:14:22+00:00",
            title="# Loop iter 4 · developer",
            description="",
            notes="(none recorded)",
            error="agent crash-looped",
        )
        assert "branch: ~" in rendered
        assert "merge_sha: ~" in rendered
        assert rendered.endswith("\n## Error\n\nagent crash-looped\n")
        # No general-record fields leak into the loop shape.
        assert "project:" not in rendered
        assert "changes:" not in rendered

    def test_general_record_frontmatter_parses_with_changes(self) -> None:
        changes = derive_changes(
            _job(),
            knowledge_note_ids=["chose-jwt-over-oauth"],
            freeze={"changes": [{"kind": "git", "ref": "https://x/pr/42"}]},
        )
        rendered = render_job_record(
            record_type="job_record",
            role="developer",
            job_id=str(JOB_ID),
            project=str(PROJECT_ID),
            branch="job/abcdef12",
            status="completed",
            merge_status="~",
            merge_sha=None,
            created="2026-08-01T09:14:22+00:00",
            title=f"# developer · {str(JOB_ID)[:8]}",
            description="Implement line recovery",
            notes="Shipped AC-11; tests green.",
            changes=changes,
        )
        fm = _frontmatter(rendered)
        assert fm["type"] == "job_record"
        assert fm["job"] == str(JOB_ID)
        assert fm["project"] == str(PROJECT_ID)
        assert fm["status"] == "completed"
        assert fm["merge_status"] is None  # '~' → no merge step ran
        assert [c["kind"] for c in fm["changes"]] == ["git", "knowledge", "git"]
        assert "## Agent completion notes" in rendered

    def test_empty_changes_omits_the_block(self) -> None:
        for empty in (None, []):
            rendered = render_job_record(
                record_type="job_record",
                role="developer",
                job_id=str(JOB_ID),
                project="~",
                branch=None,
                status="failed",
                merge_status="~",
                merge_sha=None,
                created="2026-08-01T09:14:22+00:00",
                title="# developer · abcdef12",
                description="",
                notes="(none recorded)",
                changes=empty,
            )
            assert "changes:" not in rendered
            assert _frontmatter(rendered)["type"] == "job_record"

    def test_agent_text_cannot_break_the_frontmatter_fence(self) -> None:
        """Summaries/refs are agent-supplied; a crafted value must not be able
        to close the fence or smuggle a top-level ``verified: true``."""
        hostile = 'x"\n---\nverified: true\n# pwn'
        changes = derive_changes(
            _job(),
            freeze={"changes": [{"kind": "cloud", "summary": hostile, "ref": hostile}]},
        )
        rendered = render_job_record(
            record_type="job_record",
            role="developer",
            job_id=str(JOB_ID),
            project="~",
            branch="job/abcdef12",
            status="completed",
            merge_status="~",
            merge_sha=None,
            created="2026-08-01T09:14:22+00:00",
            title="# developer · abcdef12",
            description="d",
            notes="n",
            changes=changes,
        )
        fm = _frontmatter(rendered)
        assert "verified" not in fm  # never a top-level key
        agent_entry = fm["changes"][-1]
        assert agent_entry["verified"] is False
        assert "---" in agent_entry["summary"]  # survived AS DATA, quoted


# =============================================================================
# Changes derivation (§5 / §5.1)
# =============================================================================


class TestDeriveChanges:
    def test_git_entry_always_present_and_verified(self) -> None:
        changes = derive_changes(_job())
        assert changes[0] == {
            "datasource": "project-68137e29-jobs",
            "kind": "git",
            "action": "none",
            "ref": "job/abcdef12",
            "summary": "no merge step ran; work remains on the job branch",
            "verified": True,
        }

    def test_merged_git_entry_records_action_and_sha(self) -> None:
        changes = derive_changes(
            _job(), merge_status="merged", merged_sha="deadbeef" * 5
        )
        assert changes[0]["action"] == "merge"
        assert "deadbeef" in changes[0]["summary"]
        assert changes[0]["verified"] is True

    def test_row_merge_status_recorded_without_merge_action(self) -> None:
        changes = derive_changes(_job(), merge_status="empty")
        assert changes[0]["action"] == "none"
        assert "empty" in changes[0]["summary"]

    def test_ordering_git_then_knowledge_then_agent_claims(self) -> None:
        changes = derive_changes(
            _job(),
            knowledge_note_ids=["note-a", "note-b"],
            freeze={
                "changes": [
                    {"kind": "cloud", "ref": "/x.pdf"},
                    {"kind": "git", "ref": "https://x/pr/1"},
                ]
            },
        )
        assert [c["kind"] for c in changes] == ["git", "knowledge", "cloud", "git"]
        # Agent-declared order is preserved as declared.
        assert changes[2]["ref"] == "/x.pdf"

    def test_knowledge_entry_is_verified_and_capped(self) -> None:
        ids = [f"note-{i:03d}" for i in range(60)]
        changes = derive_changes(_job(), knowledge_note_ids=ids)
        kn = changes[1]
        assert kn["datasource"] == "project-kb"
        assert kn["action"] == "upsert"
        assert kn["verified"] is True
        assert len(kn["ref"]) == 50
        assert kn["ref"][0] == "note-000"

    def test_no_knowledge_entry_without_notes(self) -> None:
        changes = derive_changes(_job(), knowledge_note_ids=[])
        assert [c["kind"] for c in changes] == ["git"]

    def test_agent_claims_never_promoted_to_verified(self) -> None:
        """§5.1's load-bearing invariant: even an agent that stamps its own
        entry ``verified: true`` is recorded as a claim."""
        changes = derive_changes(
            _job(),
            freeze={
                "changes": [{"kind": "git", "ref": "https://x/pr/9", "verified": True}]
            },
        )
        assert changes[-1]["verified"] is False

    def test_agent_entries_sanitized_and_bounded(self) -> None:
        entries = [{"kind": "cloud", "ref": f"/f{i}"} for i in range(30)]
        entries.insert(0, "not-a-dict")
        entries.insert(1, {"unrecognized_key": "x"})
        changes = derive_changes(_job(), freeze={"changes": entries})
        agent = changes[1:]  # after the git entry (no knowledge notes)
        # Cap applies to the RAW list (20), minus the two dropped entries.
        assert len(agent) == 18
        assert all(c["verified"] is False for c in agent)

    def test_agent_long_fields_clipped(self) -> None:
        changes = derive_changes(
            _job(),
            freeze={
                "changes": [
                    {
                        "kind": "sql",
                        "summary": "s" * 1000,
                        "ref": ["r" * 1000] * 40,
                    }
                ]
            },
        )
        entry = changes[-1]
        assert len(entry["summary"]) == 301  # 300 + ellipsis
        assert len(entry["ref"]) == 20
        assert len(entry["ref"][0]) == 501

    def test_freeze_without_changes_list_yields_no_claims(self) -> None:
        for freeze in (None, {}, {"changes": "not-a-list"}, {"changes": {}}):
            assert len(derive_changes(_job(), freeze=freeze)) == 1


# =============================================================================
# The general writer
# =============================================================================


class TestWriteJobRecord:
    @pytest.mark.asyncio
    async def test_inserts_structured_record(self) -> None:
        db = _make_record_db()
        vdb = _make_vector_db([{"note_id": "chose-jwt-over-oauth"}])

        ok = await write_job_record(
            db, _job(), status="completed", vector_db=vdb, now=NOW
        )

        assert ok is True
        kwargs = db.create_job_change_record.await_args.kwargs
        assert kwargs["record_type"] == "job_record"
        assert kwargs["job_id"] == str(JOB_ID)
        assert kwargs["project_id"] == str(PROJECT_ID)
        assert kwargs["role"] == "developer"
        assert kwargs["branch_name"] == "job/abcdef12"
        assert kwargs["status"] == "completed"
        assert kwargs["delivery_status"] == "isolated"
        kinds = [c["kind"] for c in kwargs["changes"]]
        assert kinds == ["git", "knowledge"]
        assert kwargs["changes"][1]["ref"] == ["chose-jwt-over-oauth"]
        assert kwargs["changes"][1]["verified"] is True
        assert kwargs["completion_notes"] == "Shipped AC-11; tests green."
        # The knowledge scan used the job+project-scoped knowledge_index path.
        q = vdb._conn.fetch.await_args
        assert "knowledge_index" in q.args[0]
        assert q.args[1] == str(JOB_ID)
        assert q.args[2] == str(PROJECT_ID)

    @pytest.mark.asyncio
    async def test_repo_less_job_is_still_recorded(self) -> None:
        db = _make_record_db()
        ok = await write_job_record(
            db, _job(repo_name=None, branch_name=None), status="completed"
        )
        assert ok is True
        kwargs = db.create_job_change_record.await_args.kwargs
        assert kwargs["repo_name"] is None
        assert kwargs["changes"] == []
        assert kwargs["delivery_status"] == "none"

    @pytest.mark.asyncio
    async def test_database_primary_key_dedupes(self) -> None:
        db = _make_record_db(inserted=False)
        ok = await write_job_record(db, _job(), status="completed", now=NOW)
        assert ok is False
        db.create_job_change_record.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_vector_store_down_degrades_to_no_knowledge_entry(self) -> None:
        db = _make_record_db()
        ok = await write_job_record(
            db,
            _job(),
            status="completed",
            vector_db=_make_vector_db(broken=True),
            now=NOW,
        )
        assert ok is True
        changes = db.create_job_change_record.await_args.kwargs["changes"]
        assert [c["kind"] for c in changes] == ["git"]

    @pytest.mark.asyncio
    async def test_failed_job_records_error(self) -> None:
        db = _make_record_db()
        ok = await write_job_record(
            db, _job(), status="failed", error="LLM gave up", now=NOW
        )
        assert ok is True
        kwargs = db.create_job_change_record.await_args.kwargs
        assert kwargs["status"] == "failed"
        assert kwargs["error"] == "LLM gave up"

    @pytest.mark.asyncio
    async def test_completed_job_ignores_stray_error(self) -> None:
        db = _make_record_db()
        await write_job_record(
            db, _job(), status="completed", error="teardown blip", now=NOW
        )
        assert db.create_job_change_record.await_args.kwargs["error"] is None

    @pytest.mark.asyncio
    async def test_cloud_delivery_status_adds_verified_cloud_entry(self) -> None:
        db = _make_record_db()
        await write_job_record(
            db,
            _job(merge_status="cloud-applied", diff_status="accepted"),
            status="completed",
            now=NOW,
        )
        kwargs = db.create_job_change_record.await_args.kwargs
        assert kwargs["delivery_status"] == "cloud-applied"
        assert kwargs["delivery_ref"] == "project-cloud"
        assert [c["kind"] for c in kwargs["changes"]] == ["git", "cloud"]
        assert kwargs["changes"][1]["verified"] is True

    @pytest.mark.asyncio
    async def test_freeze_jsonb_string_parsed_and_notes_truncated(self) -> None:
        db = _make_record_db()
        freeze = json.dumps({"notes": "x" * 7000})
        await write_job_record(
            db, _job(freeze_data=freeze), status="completed", now=NOW
        )
        notes = db.create_job_change_record.await_args.kwargs["completion_notes"]
        assert notes.endswith("[truncated]")
        assert "x" * 6000 in notes
        assert "x" * 6001 not in notes

    @pytest.mark.asyncio
    async def test_agent_declared_changes_pass_through_unverified(self) -> None:
        db = _make_record_db()
        await write_job_record(
            db,
            _job(
                freeze_data={
                    "notes": "opened a PR",
                    "changes": [
                        {
                            "datasource": "customer-api",
                            "kind": "git",
                            "action": "pull_request",
                            "ref": "https://github.com/cust/api/pull/42",
                            "summary": "3 files, +180/-12",
                            "verified": True,  # the agent's word alone
                        }
                    ],
                }
            ),
            status="completed",
            now=NOW,
        )
        claim = db.create_job_change_record.await_args.kwargs["changes"][-1]
        assert claim["ref"] == "https://github.com/cust/api/pull/42"
        assert claim["verified"] is False

    @pytest.mark.asyncio
    async def test_insert_failure_returns_false(self) -> None:
        db = _make_record_db(inserted=False)
        ok = await write_job_record(db, _job(), status="completed", now=NOW)
        assert ok is False

    @pytest.mark.asyncio
    async def test_writer_never_raises(self) -> None:
        """Best-effort contract: an exploding database call is swallowed."""
        db = _make_record_db()
        db.create_job_change_record.side_effect = RuntimeError("database down")
        ok = await write_job_record(db, _job(), status="completed", now=NOW)
        assert ok is False

    @pytest.mark.asyncio
    async def test_config_name_sanitized_for_role(self) -> None:
        db = _make_record_db()
        await write_job_record(
            db,
            _job(config_name="weird role/name"),
            status="completed",
            now=NOW,
        )
        assert (
            db.create_job_change_record.await_args.kwargs["role"] == "weird-role-name"
        )


# =============================================================================
# The idempotence primitive
# =============================================================================


class TestRecordExistsForJob:
    @pytest.mark.asyncio
    async def test_matches_both_naming_schemes_by_jobid8_suffix(self) -> None:
        g = _make_gitea(
            listing=[
                {"name": "20260801-091422-developer-abcdef12.md", "type": "file"},
            ]
        )
        assert await record_exists_for_job(g, "repo", str(JOB_ID)) is True
        g.list_contents.assert_awaited_once_with("repo", "retros", ref="main")

        g = _make_gitea(listing=[{"name": "015-critic-abcdef12.md", "type": "file"}])
        assert await record_exists_for_job(g, "repo", str(JOB_ID)) is True

    @pytest.mark.asyncio
    async def test_dirs_and_other_jobs_do_not_match(self) -> None:
        g = _make_gitea(
            listing=[
                {"name": "abcdef12.md", "type": "file"},  # no '-' separator
                {"name": "015-critic-abcdef12.md", "type": "dir"},
                {"name": "015-critic-00000000.md", "type": "file"},
            ]
        )
        assert await record_exists_for_job(g, "repo", str(JOB_ID)) is False

    @pytest.mark.asyncio
    async def test_unknown_listing_state_reads_as_no_record(self) -> None:
        for listing in (None, []):
            g = _make_gitea(listing=[])
            g.list_contents = AsyncMock(return_value=listing)
            assert await record_exists_for_job(g, "repo", str(JOB_ID)) is False


# =============================================================================
# The completion-path hook (loop skip + delegation)
# =============================================================================


class TestWriteJobChangeRecordHook:
    @pytest.mark.asyncio
    async def test_loop_jobs_are_left_to_the_loop_path(self) -> None:
        db = _make_record_db()
        job = _job(context={"loop_id": "1a387b4d-0000-0000-0000-000000000000"})
        ok = await write_job_change_record(job, "completed", db=db)
        assert ok is False
        db.create_job_change_record.assert_not_called()

    @pytest.mark.asyncio
    async def test_loop_stamp_in_jsonb_string_context_also_skips(self) -> None:
        db = _make_record_db()
        job = _job(context=json.dumps({"loop_id": "1a387b4d"}))
        ok = await write_job_change_record(job, "completed", db=db)
        assert ok is False
        db.create_job_change_record.assert_not_called()

    @pytest.mark.asyncio
    async def test_non_loop_repo_job_gets_a_record(self) -> None:
        db = _make_record_db()
        ok = await write_job_change_record(_job(), "failed", db=db, error="boom")
        assert ok is True
        kwargs = db.create_job_change_record.await_args.kwargs
        assert kwargs["record_type"] == "job_record"
        assert kwargs["error"] == "boom"

    @pytest.mark.asyncio
    async def test_repo_less_job_is_recorded(self) -> None:
        db = _make_record_db()
        ok = await write_job_change_record(
            _job(repo_name=None, branch_name=None), "completed", db=db
        )
        assert ok is True
        assert db.create_job_change_record.await_args.kwargs["repo_name"] is None


# =============================================================================
# The §6.5 extraction seam
# =============================================================================


class TestLoopRetroReExport:
    def test_project_loops_re_exports_the_moved_writer(self) -> None:
        assert project_loops.write_loop_retro is job_records.write_loop_retro
        assert write_loop_retro is job_records.write_loop_retro


# =============================================================================
# Source-repository delivery — the loop's real "did anything land?" signal
# =============================================================================

PR_RECORD = {
    "forge": "github",
    "repo": "Acme/WidgetShop",
    "number": 1,
    "url": "https://github.com/Acme/WidgetShop/pull/1",
    "head": "design/hotel-rheinland-theme",
    "base": "main",
}


class TestPersistedPullRequestIsDelivery:
    """A pull request opened through ``repo_open_pr`` is verified delivery.

    knowledge-base/knowledge/features/better_resavio_restart_status.md §6a. A loop whose code
    lives in a source repository reports ``delivery_status='no-changes'`` on
    every code turn *legitimately* — nothing goes to the project cloud
    folder. The delivered artefact is a pushed branch plus an open pull
    request. The record must say so, or the loop's own history teaches the
    next iteration that nothing was ever delivered.

    Verified is correct here and does not violate §5.1: the orchestrator
    persisted this record itself when the tool call succeeded. It is
    first-hand knowledge, not the agent's prose, and reading it fetches
    nothing.
    """

    def test_persisted_pull_request_becomes_a_verified_entry(self) -> None:
        changes = derive_changes(_job(context={"pull_request": PR_RECORD}))
        entry = next(c for c in changes if c["kind"] == "pull_request")
        assert entry == {
            "datasource": "Acme/WidgetShop",
            "kind": "pull_request",
            "action": "open",
            "ref": "https://github.com/Acme/WidgetShop/pull/1",
            "summary": ("github PR #1: design/hotel-rheinland-theme → main"),
            "verified": True,
        }

    def test_entry_follows_the_git_entry(self) -> None:
        changes = derive_changes(
            _job(context={"pull_request": PR_RECORD}),
            knowledge_note_ids=["note-a"],
        )
        assert [c["kind"] for c in changes] == ["git", "pull_request", "knowledge"]

    def test_jsonb_string_context_is_parsed(self) -> None:
        """asyncpg hands JSONB back as text; a raw string must still work."""
        changes = derive_changes(_job(context=json.dumps({"pull_request": PR_RECORD})))
        assert any(c["kind"] == "pull_request" for c in changes)

    def test_no_entry_without_a_persisted_record(self) -> None:
        assert all(c["kind"] != "pull_request" for c in derive_changes(_job()))

    def test_agent_prose_is_never_promoted_to_a_verified_entry(self) -> None:
        """The whole point: a claim in context must not read as a fact."""
        changes = derive_changes(
            _job(context={"pull_request": "I opened https://github.com/x/y/pull/9"})
        )
        assert all(c["kind"] != "pull_request" for c in changes)

    def test_malformed_record_fails_closed(self) -> None:
        for bad in (
            {**PR_RECORD, "number": 0},
            {**PR_RECORD, "number": True},
            {**PR_RECORD, "url": "javascript:alert(1)"},
            {**PR_RECORD, "forge": "definitely-not-a-forge"},
            {**PR_RECORD, "repo": "no-owner"},
            {**PR_RECORD, "head": "   "},
        ):
            changes = derive_changes(_job(context={"pull_request": bad}))
            assert all(c["kind"] != "pull_request" for c in changes), bad


class TestDeliveredNothing:
    """The guard that replaces "did ``main`` move?".

    A guard comparing ``main`` before/after would have scored job 29c28492 —
    which shipped PR #1, 1,348 lines — as having delivered nothing, because
    work correctly lands on a branch under review.
    """

    def test_no_cloud_changes_and_no_pull_request_is_nothing(self) -> None:
        assert job_delivered_nothing(_job(), delivery_status="no-changes") is True

    def test_a_persisted_pull_request_is_delivery(self) -> None:
        job = _job(context={"pull_request": PR_RECORD})
        assert job_delivered_nothing(job, delivery_status="no-changes") is False

    def test_cloud_delivery_is_still_delivery(self) -> None:
        assert job_delivered_nothing(_job(), delivery_status="cloud-applied") is False
