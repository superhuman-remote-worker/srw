"""Server-side knowledge-note materialisation (step 3 of §7,
knowledge-base/knowledge/features/knowledge_base_repo_separation.md).

``materialize_knowledge_note`` replaces the agent's workspace write of
``knowledge/<slug>.md`` with a single Gitea commit into whichever repo
``resolve_kb_repo`` picks for the project. The service reports expected
failures as structured outcomes; mutation callers fail closed unless the
result proves that the canonical boundary was crossed, while the durable
intent remains available for retry.

Gitea is mocked (pattern: tests/test_loop_merge.py). ``resolve_kb_repo`` is
patched at this module's import site — it is the reindexer's function, and
these tests are about what we do with its answer, not how it finds one.
"""

from __future__ import annotations

import base64
import hashlib
import logging
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

import orchestrator.security.access as access_module
from orchestrator.routers.knowledge import KnowledgeDependencies
from orchestrator.security.access import require_project_member
from orchestrator.services.kb_task_registry import KbDatasourceTaskRegistry
from orchestrator.services.knowledge_index import KnowledgeIndexDependencies
from orchestrator.services.knowledge_operations import KnowledgeOperationDependencies
from orchestrator.services.kb_materialize import (
    materialize_knowledge_metadata_update,
    materialize_knowledge_note,
    materialize_knowledge_note_delete,
    note_repo_path,
    slug_error,
)
from orchestrator.services.kb_reindex import KbRepoRef

PROJECT = "1a387b4d-0000-0000-0000-000000000000"
JOB = "abcdef12-3456-7890-abcd-ef1234567890"
REPO = "project-1a387b4d-knowledge"
SLUG = "chose-jwt-over-oauth"
PATH = "knowledge/chose-jwt-over-oauth.md"
BODY = "---\nid: chose-jwt-over-oauth\n---\n\n# Chose JWT\n"


def _knowledge_deps(
    *,
    store=None,
    gitea=None,
    vector=None,
    graph=None,
    member_gate=None,
) -> KnowledgeDependencies:
    """Exactly what main's ``_knowledge_dependencies()`` factory composes.

    Before R1.B03 each of these arrived by patching a module global on
    ``orchestrator.main`` — ``postgres_db``, ``gitea_client``, ``vector_db``,
    ``_get_knowledge_graph``, ``require_project_member``. The router handler
    now takes them through this dataclass, so the same objects are passed in
    here. ``member_gate=None`` leaves the dataclass default in place, which is
    the REAL ``require_project_member``; ``require_internal`` is always left
    real, because the ``X-Internal-Key`` tests below are about that gate.
    """
    log = logging.getLogger("tests.kb_materialize")
    vector = MagicMock() if vector is None else vector
    store = AsyncMock() if store is None else store
    gitea = MagicMock() if gitea is None else gitea
    return KnowledgeDependencies(
        store=store,
        operations=KnowledgeOperationDependencies(
            store=store,
            vector_db=vector,
            gitea_client=gitea,
            logger=log,
            # No Neo4j attached, which is what the old
            # `_get_knowledge_graph -> None` patches modelled.
            graph=SimpleNamespace(get=lambda: None) if graph is None else graph,
            knowledge_index=KnowledgeIndexDependencies(
                store=store,
                vector_db=vector,
                gitea_client=gitea,
                logger=log,
                tasks=KbDatasourceTaskRegistry(),
                inject_system_kb_embedding_profile=AsyncMock(return_value=None),
            ),
        ),
        require_project_member=(
            require_project_member if member_gate is None else member_gate
        ),
    )


# What the member gate returns: ``(user, project)``. An admin, because a PATCH
# that adds ``ready`` needs owner authority (D9) and these tests are about the
# materialization behind the gate, not the gate.
_ADMIN_CALLER = ({"id": "u-admin", "is_admin": True}, {"id": "p-1"})


def _blob_sha(text: str) -> str:
    data = text.encode("utf-8")
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()


def _make_gitea(
    *,
    tree_paths: dict[str, str] | None = None,
    tree_ok: bool = True,
    tree_raises: bool = False,
    change_results: list[bool] | None = None,
    change_raises: bool = False,
) -> MagicMock:
    """Mocked gitea surface.

    ``tree_paths`` maps blob path → blob sha on the target branch (drives the
    create-vs-update choice and the unchanged short-circuit);
    ``change_results`` is the per-attempt return of ``change_files`` so the
    flip-retry can be driven.
    """
    tree_paths = tree_paths if tree_paths is not None else {}
    results = list(change_results if change_results is not None else [True])

    g = MagicMock()

    async def _list_tree(repo: str, ref: str):
        if tree_raises:
            raise RuntimeError("gitea down")
        if not tree_ok:
            return None
        return [
            {"path": p, "type": "blob", "sha": sha} for p, sha in tree_paths.items()
        ]

    async def _change_files(repo, branch, files, message):
        if change_raises:
            raise RuntimeError("gitea down")
        return results.pop(0) if results else False

    g.list_tree = AsyncMock(side_effect=_list_tree)
    g.change_files = AsyncMock(side_effect=_change_files)
    return g


def _patch_resolve(value):
    return patch(
        "orchestrator.services.kb_materialize.resolve_kb_repo",
        AsyncMock(return_value=value),
    )


def _ref(branch: str | None = "main") -> KbRepoRef:
    return KbRepoRef(
        forge="gitea",
        repo_url="",
        owner="",
        repo=REPO,
        branch=branch,  # type: ignore[arg-type] - exercises runtime fallback
    )


def _ledger_db() -> AsyncMock:
    db = AsyncMock()
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
    db.finish_knowledge_projection.return_value = {"id": intent_id}
    return db


async def _run(gitea, *, slug=SLUG, content=BODY, job_id=JOB, resolved=None):
    if resolved is None:
        resolved = _ref()
    db = _ledger_db()
    with _patch_resolve(resolved) as resolve:
        result = await materialize_knowledge_note(
            postgres_db=db,
            gitea_client=gitea,
            project_id=PROJECT,
            slug=slug,
            content=content,
            job_id=job_id,
        )
    return result, resolve, db


# =============================================================================
# Path + slug vocabulary
# =============================================================================


class TestNotePath:
    def test_path_uses_the_reindexers_vault_prefix(self):
        """One prefix, one source: the writer and the sweep must agree."""
        from orchestrator.services.kb_reindex import KNOWLEDGE_PREFIX

        assert note_repo_path(SLUG) == f"{KNOWLEDGE_PREFIX}{SLUG}.md"
        assert note_repo_path(SLUG) == PATH

    @pytest.mark.parametrize(
        "slug",
        ["", "   ", "../../etc/passwd", "nested/note", "back\\slash", ".hidden", "-x"],
    )
    def test_unsafe_slugs_are_refused(self, slug):
        assert slug_error(slug) is not None

    @pytest.mark.parametrize("slug", ["index", "LOG"])
    def test_reserved_okf_basenames_are_refused(self, slug):
        """``index.md``/``log.md`` are generated nav the reindexer skips — a
        note written there would be permanently invisible."""
        assert slug_error(slug) is not None

    @pytest.mark.parametrize(
        "slug", [SLUG, "kb_reindex_watermark_never_advances", "note.v2", "a"]
    )
    def test_real_world_slugs_pass(self, slug):
        assert slug_error(slug) is None


# =============================================================================
# The write path
# =============================================================================


class TestMaterializeCreate:
    @pytest.mark.asyncio
    async def test_absent_path_commits_as_create(self):
        g = _make_gitea(tree_paths={"knowledge/other.md": "beef" * 10})

        result, resolve, db = await _run(g)

        assert result["status"] == "committed"
        assert result["operation"] == "create"
        assert result["repo"] == REPO
        assert result["branch"] == "main"
        assert result["path"] == PATH
        assert result["reason"] is None
        # Resolution goes through the reindexer's helper, not a reimplementation.
        resolve.assert_awaited_once_with(db, PROJECT)
        g.list_tree.assert_awaited_once_with(REPO, "main")

    @pytest.mark.asyncio
    async def test_commit_payload_is_base64_and_single_file(self):
        g = _make_gitea()

        await _run(g)

        args = g.change_files.await_args
        assert args.args[0] == REPO
        assert args.args[1] == "main"
        files = args.args[2]
        assert len(files) == 1
        assert files[0]["path"] == PATH
        assert base64.b64decode(files[0]["content_b64"]).decode() == BODY
        assert files[0]["operation"] == "create"

    @pytest.mark.asyncio
    async def test_commit_message_attributes_the_writing_job(self):
        g = _make_gitea()

        await _run(g)

        message = g.change_files.await_args.kwargs["message"]
        assert message.startswith(f"kb: {SLUG} (job {JOB[:8]})")
        # Full UUID in the body keeps attribution machine-recoverable.
        assert f"job: {JOB}" in message

    @pytest.mark.asyncio
    async def test_commit_message_without_a_job(self):
        """Persistent sessions have no job — the note still materialises."""
        g = _make_gitea()

        result, _, _ = await _run(g, job_id=None)

        assert result["status"] == "committed"
        assert g.change_files.await_args.kwargs["message"] == f"kb: {SLUG}"

    @pytest.mark.asyncio
    async def test_unreadable_tree_still_attempts_a_create(self):
        """An empty/unreadable tree is a guess, not a stop — the flip-retry
        below is what covers a wrong guess."""
        g = _make_gitea(tree_ok=False)

        result, _, _ = await _run(g)

        assert result["status"] == "committed"
        assert result["operation"] == "create"

    @pytest.mark.asyncio
    async def test_tree_probe_raising_does_not_fail_the_write(self):
        g = _make_gitea(tree_raises=True)

        result, _, _ = await _run(g)

        assert result["status"] == "committed"
        assert result["operation"] == "create"


class TestMaterializeUpdate:
    @pytest.mark.asyncio
    async def test_existing_path_commits_as_update(self):
        """``create`` on an existing path is a Gitea 422 — the operation is
        chosen per file against the target tree (the curated-merge rule)."""
        g = _make_gitea(tree_paths={PATH: "stale" + "0" * 35})

        result, _, _ = await _run(g)

        assert result["status"] == "committed"
        assert result["operation"] == "update"
        file = g.change_files.await_args.args[2][0]
        assert file["operation"] == "update"
        assert file["sha"] == "stale" + "0" * 35

    @pytest.mark.asyncio
    async def test_github_descriptor_selects_external_client_not_gitea(self):
        gitea = _make_gitea()
        github = _make_gitea(tree_paths={PATH: "stale" + "0" * 35})
        ref = KbRepoRef(
            forge="github",
            repo_url="https://github.com/acme/design-vault.git",
            owner="acme",
            repo="design-vault",
            branch="main",
            credential_ref="55555555-6666-7777-8888-999999999999",
        )
        db = _ledger_db()
        with (
            _patch_resolve(ref),
            patch(
                "orchestrator.services.kb_materialize.kb_client_for_repo",
                AsyncMock(return_value=github),
            ) as select,
        ):
            result = await materialize_knowledge_note(
                postgres_db=db,
                gitea_client=gitea,
                project_id=PROJECT,
                slug=SLUG,
                content=BODY,
                job_id=JOB,
            )

        assert result["status"] == "committed"
        select.assert_awaited_once_with(db, gitea, ref)
        gitea.list_tree.assert_not_awaited()
        gitea.change_files.assert_not_awaited()
        github.change_files.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_identical_bytes_are_not_recommitted(self):
        """A no-op commit would pollute the very per-job history §3 wants
        readable — and the tree already tells us the blob sha."""
        g = _make_gitea(tree_paths={PATH: _blob_sha(BODY)})

        result, _, _ = await _run(g)

        assert result["status"] == "skipped"
        assert result["reason"] == "unchanged"
        assert result["repo"] == REPO
        assert result["path"] == PATH
        g.change_files.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_changed_bytes_at_the_same_path_do_commit(self):
        g = _make_gitea(tree_paths={PATH: _blob_sha("something else entirely")})

        result, _, _ = await _run(g)

        assert result["status"] == "committed"
        assert result["operation"] == "update"


class TestOperationFlipRetry:
    @pytest.mark.asyncio
    async def test_refused_create_is_retried_as_update(self):
        """Concurrent writer (or an unreadable tree) landed the path between
        the probe and the commit: the 422 is recovered, not lost."""
        g = _make_gitea(change_results=[False, True])

        result, _, _ = await _run(g)

        assert result["status"] == "committed"
        assert result["operation"] == "update"
        assert g.change_files.await_count == 2
        ops = [c.args[2][0]["operation"] for c in g.change_files.await_args_list]
        assert ops == ["create", "update"]

    @pytest.mark.asyncio
    async def test_refused_update_is_retried_as_create(self):
        g = _make_gitea(
            tree_paths={PATH: "stale" + "0" * 35}, change_results=[False, True]
        )

        result, _, _ = await _run(g)

        assert result["status"] == "committed"
        assert result["operation"] == "create"
        ops = [c.args[2][0]["operation"] for c in g.change_files.await_args_list]
        assert ops == ["update", "create"]


# =============================================================================
# Skips and failures — none of them raise
# =============================================================================


class TestSkips:
    @pytest.mark.asyncio
    async def test_no_repo_is_a_permanent_unrecorded_failure(self):
        """A repo-less project can never satisfy the intent — say so, don't retry."""
        g = _make_gitea()
        db = _ledger_db()
        with _patch_resolve(None):
            result = await materialize_knowledge_note(
                postgres_db=db,
                gitea_client=g,
                project_id=PROJECT,
                slug=SLUG,
                content=BODY,
                job_id=JOB,
            )
        assert result["status"] == "failed"
        assert result["reason"] == "no-repo"
        assert result["canonical_state"] == "failed"
        assert result["retry_state"] == "permanent"
        assert result["recorded"] is False
        assert result["path"] == PATH and result["repo"] is None
        db.begin_knowledge_materialization.assert_not_awaited()
        g.list_tree.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_sweep_retry_discovering_no_repo_is_also_permanent(self):
        """The mainstream trigger for the *deep* no-repo branch: kb_reindex's
        sweep dispatches a previously-retryable content intent through
        ``retry_knowledge_materialization_intent``, which calls
        ``_attempt_content_intent`` directly — the intent already exists, so
        the early short-circuit in ``_materialize_note_canonical`` never runs.
        If ``resolve_kb_repo`` now answers "no repo", this must still land as
        permanent — via the ledger this time, since a row already exists."""
        from orchestrator.services.kb_materialize import (
            retry_knowledge_materialization_intent,
        )

        g = _make_gitea()
        db = _ledger_db()
        intent = {
            "id": uuid.uuid4(),
            "project_id": PROJECT,
            "note_id": SLUG,
            "content": BODY,
            "job_id": JOB,
            "attempt_token": uuid.uuid4(),
        }
        with _patch_resolve(None):
            result = await retry_knowledge_materialization_intent(
                postgres_db=db,
                gitea_client=g,
                intent=intent,
            )
        assert result["status"] == "failed"
        assert result["reason"] == "no-repo"
        assert result["canonical_state"] == "failed"
        assert result["retry_state"] == "permanent"
        assert result["recorded"] is True
        assert (
            db.finish_knowledge_materialization.await_args.kwargs["permanent"] is True
        )
        g.list_tree.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_repo_wins_over_malformed_frontmatter(self):
        """``_materialize_note_canonical`` resolves the repo before anything
        parses the note, so a repo-less project reports ``no-repo`` even for
        content that would separately fail as ``malformed-frontmatter``.
        Pinning this precedence: both are permanent, but only the repo check
        runs before an intent would be opened."""
        g = _make_gitea()
        db = _ledger_db()
        with _patch_resolve(None):
            result = await materialize_knowledge_note(
                postgres_db=db,
                gitea_client=g,
                project_id=PROJECT,
                slug=SLUG,
                content="---\nid: [unterminated\n---\n# Broken\n",
                job_id=JOB,
            )
        assert result["status"] == "failed"
        assert result["reason"] == "no-repo"
        assert result["recorded"] is False
        db.begin_knowledge_materialization.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_branch_falls_back_to_main(self):
        g = _make_gitea()

        result, _, _ = await _run(g, resolved=_ref(None))

        assert result["branch"] == "main"
        g.list_tree.assert_awaited_once_with(REPO, "main")


class TestCompareAndSwap:
    """`expected_blob_sha` on writes (kb_gardening G3): a stale writer fails
    loudly instead of winning, and a rewrite of a removed note does not
    re-create it."""

    NEW_BODY = "---\nid: chose-jwt-over-oauth\n---\n\n# Chose JWT (revised)\n"

    async def _write(self, gitea, *, expected):
        db = _ledger_db()
        with _patch_resolve(_ref()):
            result = await materialize_knowledge_note(
                postgres_db=db,
                gitea_client=gitea,
                project_id=PROJECT,
                slug=SLUG,
                content=self.NEW_BODY,
                job_id=JOB,
                expected_blob_sha=expected,
            )
        return result, db

    @pytest.mark.asyncio
    async def test_matching_token_commits_as_update(self):
        current = _blob_sha(BODY)
        g = _make_gitea(tree_paths={PATH: current})
        result, _ = await self._write(g, expected=current)
        assert result["status"] == "committed"
        assert result["operation"] == "update"

    @pytest.mark.asyncio
    async def test_stale_token_is_refused_permanently_and_never_reaches_git(self):
        g = _make_gitea(tree_paths={PATH: _blob_sha(BODY)})
        result, db = await self._write(g, expected="f" * 40)
        assert result["status"] == "failed"
        assert result["reason"] == "precondition-failed"
        assert result["retry_state"] == "permanent"
        assert result["indexed"] is False
        g.change_files.assert_not_awaited()
        assert (
            db.finish_knowledge_materialization.await_args.kwargs["permanent"] is True
        )

    @pytest.mark.asyncio
    async def test_token_on_a_removed_note_does_not_recreate_it(self):
        """The resurrection hole: the row still exists, the file is gone, a
        stale kb_update used to `create` the file back."""
        g = _make_gitea(tree_paths={})
        result, _ = await self._write(g, expected=_blob_sha(BODY))
        assert result["status"] == "failed"
        assert result["reason"] == "precondition-failed"
        g.change_files.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unreadable_tree_with_a_token_is_retryable_not_guessed(self):
        g = _make_gitea(tree_ok=False)
        result, _ = await self._write(g, expected=_blob_sha(BODY))
        assert result["status"] == "failed"
        assert result["reason"] == "tree-unreadable"
        assert result["retry_state"] == "retryable"
        g.change_files.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_forge_refusal_of_a_conditional_write_is_precondition_failed(self):
        """E4 S2: two writers whose tree probes both preceded either commit.
        The probe passes for both; the forge (given the SHA) refuses the
        second. That refusal is final — no flip to `create`, no retry."""
        current = _blob_sha(BODY)
        g = _make_gitea(tree_paths={PATH: current}, change_results=[False, False])
        result, db = await self._write(g, expected=current)
        assert result["status"] == "failed"
        assert result["reason"] == "precondition-failed"
        assert result["retry_state"] == "permanent"
        assert g.change_files.await_count == 1  # no opposite-operation retry
        sent = g.change_files.await_args.args[2][0]
        assert sent["sha"] == current and sent["operation"] == "update"

    @pytest.mark.asyncio
    async def test_no_token_keeps_last_writer_wins(self):
        """Unconditional writes are unchanged: a mismatch is not even looked at."""
        g = _make_gitea(tree_paths={PATH: "0" * 40})
        result, _ = await self._write(g, expected=None)
        assert result["status"] == "committed"

    @pytest.mark.asyncio
    async def test_endpoint_forwards_the_token(self, fake_request):
        from orchestrator.routers.knowledge import materialize_knowledge_note
        from orchestrator.schemas.knowledge import KnowledgeMaterializeRequest

        g = _make_gitea(tree_paths={PATH: _blob_sha(BODY)})
        fake_request.headers = {"X-Internal-Key": "secret"}
        body = KnowledgeMaterializeRequest(
            slug=SLUG, content=self.NEW_BODY, job_id=JOB, expected_blob_sha="e" * 40
        )
        deps = _knowledge_deps(store=_ledger_db(), gitea=g)
        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            _patch_resolve(_ref()),
        ):
            result = await materialize_knowledge_note(
                fake_request, PROJECT, body, dependencies=deps
            )
        assert result["reason"] == "precondition-failed"
        g.change_files.assert_not_awaited()


class TestMaterializeDelete:
    """The purge primitive (kb_gardening G2): a file-removal commit, idempotent
    on an absent path, compare-and-swap on the blob SHA, ledgered like writes."""

    @staticmethod
    def _store() -> AsyncMock:
        store = AsyncMock()
        store.delete_kb_note.return_value = True
        store.delete_note.return_value = False
        return store

    async def _delete(self, gitea, *, store=None, **kwargs):
        db = _ledger_db()
        with _patch_resolve(_ref()):
            result = await materialize_knowledge_note_delete(
                postgres_db=db,
                gitea_client=gitea,
                project_id=PROJECT,
                slug=SLUG,
                job_id=JOB,
                store=store,
                **kwargs,
            )
        return result, db

    @pytest.mark.asyncio
    async def test_existing_file_is_removed_with_the_tree_sha_and_the_row_dropped(self):
        sha = _blob_sha(BODY)
        g = _make_gitea(tree_paths={PATH: sha})
        g.delete_path = AsyncMock(return_value="deleted")
        store = self._store()

        result, db = await self._delete(g, store=store, reason="near-duplicate of x")

        assert result["status"] == "committed"
        assert result["operation"] == "delete"
        assert result["path"] == PATH
        assert result["canonical_state"] == "canonical"
        assert result["row_deleted"] is True
        args = g.delete_path.await_args
        assert args.args[0] == REPO
        assert args.args[1] == "main"
        assert args.args[2] == PATH
        assert args.kwargs["expected_sha"] == sha
        message = args.args[3]
        assert message.startswith(f"kb: delete {SLUG}")
        assert "near-duplicate of x" in message
        assert JOB in message
        store.delete_kb_note.assert_awaited_once_with(uuid.UUID(PROJECT), PATH)
        db.begin_knowledge_materialization.assert_awaited_once()
        assert (
            db.begin_knowledge_materialization.await_args.kwargs["operation"]
            == "delete"
        )

    @pytest.mark.asyncio
    async def test_absent_file_is_success_without_a_commit(self):
        """Idempotent: delete-of-missing is the desired state, not an error."""
        g = _make_gitea(tree_paths={})
        g.delete_path = AsyncMock(return_value="deleted")
        store = self._store()

        result, _ = await self._delete(g, store=store)

        assert result["status"] == "skipped"
        assert result["reason"] == "absent"
        assert result["canonical_state"] == "canonical"
        g.delete_path.assert_not_awaited()
        # The row (a legacy pathless twin, or a row the sweep has not reaped
        # yet) is still cleaned up — the caller asked for the note to be gone.
        store.delete_kb_note.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_expected_sha_mismatch_is_a_permanent_precondition_failure(self):
        """CAS (G3): the caller read one version, the tree holds another."""
        g = _make_gitea(tree_paths={PATH: _blob_sha(BODY)})
        g.delete_path = AsyncMock(return_value="deleted")
        store = self._store()

        result, db = await self._delete(g, store=store, expected_blob_sha="0" * 40)

        assert result["status"] == "failed"
        assert result["reason"] == "precondition-failed"
        assert result["retry_state"] == "permanent"
        g.delete_path.assert_not_awaited()
        store.delete_kb_note.assert_not_awaited()
        assert (
            db.finish_knowledge_materialization.await_args.kwargs["permanent"] is True
        )

    @pytest.mark.asyncio
    async def test_forge_conflict_is_a_permanent_precondition_failure(self):
        """The tree matched at probe time but the forge refused the SHA — a
        concurrent writer landed in between. Never retried blindly."""
        g = _make_gitea(tree_paths={PATH: _blob_sha(BODY)})
        g.delete_path = AsyncMock(return_value="conflict")
        store = self._store()

        result, db = await self._delete(g, store=store)

        assert result["status"] == "failed"
        assert result["reason"] == "precondition-failed"
        assert result["retry_state"] == "permanent"
        store.delete_kb_note.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_forge_error_is_retryable_and_leaves_the_row(self):
        g = _make_gitea(tree_paths={PATH: _blob_sha(BODY)})
        g.delete_path = AsyncMock(return_value="error")
        store = self._store()

        result, _ = await self._delete(g, store=store)

        assert result["status"] == "failed"
        assert result["reason"] == "commit-error"
        assert result["retry_state"] == "retryable"
        store.delete_kb_note.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_forge_reports_absent_after_probe_said_present(self):
        """Probe saw the file, the forge 404s on delete: someone else removed
        it first. Still the desired state."""
        g = _make_gitea(tree_paths={PATH: _blob_sha(BODY)})
        g.delete_path = AsyncMock(return_value="absent")
        store = self._store()

        result, _ = await self._delete(g, store=store)

        assert result["status"] == "skipped"
        assert result["reason"] == "absent"
        assert result["canonical_state"] == "canonical"
        store.delete_kb_note.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_unreadable_tree_is_retryable_and_never_deletes(self):
        g = _make_gitea(tree_ok=False)
        g.delete_path = AsyncMock(return_value="deleted")
        store = self._store()

        result, _ = await self._delete(g, store=store)

        assert result["status"] == "failed"
        assert result["reason"] == "tree-unreadable"
        assert result["retry_state"] == "retryable"
        g.delete_path.assert_not_awaited()
        store.delete_kb_note.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unsafe_slug_never_reaches_the_forge(self):
        g = _make_gitea(tree_paths={PATH: _blob_sha(BODY)})
        g.delete_path = AsyncMock(return_value="deleted")
        db = _ledger_db()
        with _patch_resolve(_ref()):
            result = await materialize_knowledge_note_delete(
                postgres_db=db,
                gitea_client=g,
                project_id=PROJECT,
                slug="../etc/passwd",
                store=self._store(),
            )
        assert result["status"] == "failed"
        assert result["reason"] == "invalid-slug"
        g.delete_path.assert_not_awaited()
        g.list_tree.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_row_cleanup_failure_does_not_undo_the_commit(self):
        """The file is gone; the sweep reaps the row on its next tree-diff."""
        g = _make_gitea(tree_paths={PATH: _blob_sha(BODY)})
        g.delete_path = AsyncMock(return_value="deleted")
        store = self._store()
        store.delete_kb_note.side_effect = RuntimeError("vector db down")

        result, _ = await self._delete(g, store=store)

        assert result["status"] == "committed"
        assert result["canonical_state"] == "canonical"
        assert result["row_deleted"] is False

    @pytest.mark.asyncio
    async def test_without_a_store_the_commit_still_lands(self):
        g = _make_gitea(tree_paths={PATH: _blob_sha(BODY)})
        g.delete_path = AsyncMock(return_value="deleted")

        result, _ = await self._delete(g, store=None)

        assert result["status"] == "committed"
        assert result["row_deleted"] is False

    @pytest.mark.asyncio
    async def test_a_previously_refused_identical_delete_reports_the_refusal(self):
        """E6 run 3: the purge lane re-asked with the same stale token; the
        ledger held a permanent intent for that payload and the op said
        `attempt-in-progress`. It must say what actually happened."""
        g = _make_gitea(tree_paths={PATH: _blob_sha(BODY)})
        g.delete_path = AsyncMock(return_value="deleted")
        db = _ledger_db()

        async def _begin(**kwargs):
            return {
                "id": uuid.uuid4(),
                "canonical_state": "failed",
                "projection_state": "pending",
                "retry_state": "permanent",
                "attempt_claimed": False,
                "last_error": "precondition-failed",
                "path": PATH,
            }

        db.begin_knowledge_materialization.side_effect = _begin
        store = self._store()
        with _patch_resolve(_ref()):
            result = await materialize_knowledge_note_delete(
                postgres_db=db,
                gitea_client=g,
                project_id=PROJECT,
                slug=SLUG,
                expected_blob_sha="0" * 40,
                store=store,
            )
        assert result["status"] == "failed"
        assert result["reason"] == "precondition-failed"
        assert result["retry_state"] == "permanent"
        assert result["row_deleted"] is False
        g.delete_path.assert_not_awaited()
        store.delete_kb_note.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_retry_dispatch_routes_a_delete_intent(self):
        from orchestrator.services.kb_materialize import (
            retry_knowledge_materialization_intent,
        )

        g = _make_gitea(tree_paths={PATH: _blob_sha(BODY)})
        g.delete_path = AsyncMock(return_value="deleted")
        db = _ledger_db()
        intent = {
            "id": uuid.uuid4(),
            "project_id": PROJECT,
            "note_id": SLUG,
            "job_id": JOB,
            "operation": "delete",
            "content": '{"reason": "stale", "expected_blob_sha": null}',
            "attempt_token": uuid.uuid4(),
        }
        with _patch_resolve(_ref()):
            result = await retry_knowledge_materialization_intent(
                postgres_db=db, gitea_client=g, intent=intent
            )
        assert result["status"] == "committed"
        assert result["operation"] == "delete"
        assert "stale" in g.delete_path.await_args.args[3]


class TestFailures:
    @pytest.mark.asyncio
    async def test_malformed_frontmatter_is_permanent_and_never_reaches_git(self):
        g = _make_gitea()

        result, _, db = await _run(
            g,
            content="---\nid: [unterminated\n---\n# Broken\n",
        )

        assert result["status"] == "failed"
        assert result["reason"] == "malformed-frontmatter"
        assert result["canonical_state"] == "failed"
        assert result["retry_state"] == "permanent"
        assert (
            db.finish_knowledge_materialization.await_args.kwargs["permanent"] is True
        )
        g.change_files.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unexpected_materializer_exception_is_durable_and_retryable(self):
        g = _make_gitea()
        with patch(
            "orchestrator.services.kb_materialize._materialize_knowledge_note_once",
            AsyncMock(side_effect=RuntimeError("materializer exploded")),
        ):
            result, _, db = await _run(g)

        assert result["status"] == "failed"
        assert result["reason"] == "materializer-exception"
        assert result["canonical_state"] == "pending_sync"
        assert result["retry_state"] == "retryable"
        assert (
            db.finish_knowledge_materialization.await_args.kwargs["permanent"] is False
        )

    @pytest.mark.asyncio
    async def test_intent_store_failure_is_explicitly_projection_only(self):
        db = _ledger_db()
        db.begin_knowledge_materialization.side_effect = RuntimeError("ledger down")

        with _patch_resolve(_ref()):
            result = await materialize_knowledge_note(
                postgres_db=db,
                gitea_client=_make_gitea(),
                project_id=PROJECT,
                slug=SLUG,
                content=BODY,
                job_id=JOB,
            )

        assert result["status"] == "failed"
        assert result["reason"] == "intent-store-error"
        assert result["canonical_state"] == "failed"
        assert result["projection_state"] == "projection_only"
        assert result["retry_state"] == "retryable"

    @pytest.mark.asyncio
    async def test_retry_reuses_exact_canonical_ready_timestamp(self):
        from orchestrator.services.kb_materialize import (
            retry_knowledge_materialization_intent,
        )

        ready_at = "2026-08-17T10:11:12+00:00"
        g = _make_gitea()
        content = (
            "---\nid: bp08-ready\ntype: feature\nstatus: active\n"
            f"tags: [ready]\nready_at: {ready_at}\n---\n# Ready\n"
        )
        db = _ledger_db()
        seen: list[str] = []

        async def _materialize_once(**kwargs):
            seen.append(kwargs["content"])
            return {
                "status": "committed",
                "reason": None,
                "path": "knowledge/bp08-ready.md",
            }

        intent = {
            "id": uuid.uuid4(),
            "project_id": PROJECT,
            "note_id": "bp08-ready",
            "content": content,
            "job_id": None,
            "attempt_token": uuid.uuid4(),
        }
        with patch(
            "orchestrator.services.kb_materialize._materialize_knowledge_note_once",
            side_effect=_materialize_once,
        ):
            result = await retry_knowledge_materialization_intent(
                postgres_db=db,
                gitea_client=g,
                intent=intent,
            )

        assert result["canonical_state"] == "canonical"
        assert seen == [content]
        assert seen[0].count(ready_at) == 1

    @pytest.mark.asyncio
    async def test_ready_metadata_returns_exact_canonical_projection_values(self):
        current = (
            "---\nid: chose-jwt-over-oauth\ntype: feature\nstatus: active\n"
            "tags: [category:executor]\n---\n# Ready next\n"
        )
        g = _make_gitea(tree_paths={PATH: _blob_sha(current)})
        g.get_file_content = AsyncMock(return_value=current)
        db = _ledger_db()

        with _patch_resolve(_ref()):
            result = await materialize_knowledge_metadata_update(
                postgres_db=db,
                gitea_client=g,
                project_id=PROJECT,
                slug=SLUG,
                add_tags=["ready"],
            )

        payload = g.change_files.await_args.args[2][0]
        committed = base64.b64decode(payload["content_b64"]).decode()
        assert result["canonical_state"] == "canonical"
        assert result["canonical_metadata_complete"] is True
        assert result["canonical_tags"] == ["category:executor", "ready"]
        assert isinstance(result["canonical_ready_at"], datetime)
        assert f"ready_at: '{result['canonical_ready_at'].isoformat()}'" in committed
        assert committed.count("ready_at:") == 1

    @pytest.mark.asyncio
    async def test_remove_then_readd_ready_creates_one_new_generation(self):
        old_ready_at = "2026-08-16T10:11:12+00:00"
        current = (
            "---\nid: chose-jwt-over-oauth\ntype: feature\nstatus: active\n"
            "tags: [category:executor, ready]\n"
            f"ready_at: '{old_ready_at}'\n---\n# Re-arm\n"
        )
        remove_gitea = _make_gitea(tree_paths={PATH: _blob_sha(current)})
        remove_gitea.get_file_content = AsyncMock(return_value=current)
        with _patch_resolve(_ref()):
            removed = await materialize_knowledge_metadata_update(
                postgres_db=_ledger_db(),
                gitea_client=remove_gitea,
                project_id=PROJECT,
                slug=SLUG,
                remove_tags=["ready"],
            )

        remove_payload = remove_gitea.change_files.await_args.args[2][0]
        without_ready = base64.b64decode(remove_payload["content_b64"]).decode()
        assert removed["canonical_tags"] == ["category:executor"]
        assert removed["canonical_ready_at"] is None
        assert "ready_at:" not in without_ready

        add_gitea = _make_gitea(tree_paths={PATH: _blob_sha(without_ready)})
        add_gitea.get_file_content = AsyncMock(return_value=without_ready)
        with _patch_resolve(_ref()):
            readded = await materialize_knowledge_metadata_update(
                postgres_db=_ledger_db(),
                gitea_client=add_gitea,
                project_id=PROJECT,
                slug=SLUG,
                add_tags=["ready"],
            )

        add_payload = add_gitea.change_files.await_args.args[2][0]
        rearmed = base64.b64decode(add_payload["content_b64"]).decode()
        assert readded["canonical_tags"] == ["category:executor", "ready"]
        assert readded["canonical_ready_at"].isoformat() != old_ready_at
        assert rearmed.count("ready_at:") == 1

    @pytest.mark.asyncio
    async def test_combined_status_and_tag_mutation_returns_one_exact_snapshot(self):
        current = (
            "---\nid: chose-jwt-over-oauth\ntype: feature\nstatus: active\n"
            "tags: [category:executor]\n---\n# Combined\n"
        )
        g = _make_gitea(tree_paths={PATH: _blob_sha(current)})
        g.get_file_content = AsyncMock(return_value=current)

        with _patch_resolve(_ref()):
            result = await materialize_knowledge_metadata_update(
                postgres_db=_ledger_db(),
                gitea_client=g,
                project_id=PROJECT,
                slug=SLUG,
                status="resolved",
                add_tags=["ready"],
            )

        assert result["canonical_metadata_complete"] is True
        assert result["canonical_status"] == "resolved"
        assert result["canonical_tags"] == ["category:executor", "ready"]
        assert isinstance(result["canonical_ready_at"], datetime)

    @pytest.mark.asyncio
    async def test_already_canonical_metadata_rereads_complete_current_truth(self):
        ready_at = "2026-08-17T10:11:12+00:00"
        current = (
            "---\nid: chose-jwt-over-oauth\ntype: feature\nstatus: active\n"
            "tags: [category:executor, ready]\n"
            f"ready_at: '{ready_at}'\n---\n# Ready next\n"
        )
        g = _make_gitea(tree_paths={PATH: _blob_sha(current)})
        g.get_file_content = AsyncMock(return_value=current)
        db = _ledger_db()
        intent_id = uuid.uuid4()
        db.begin_knowledge_materialization.side_effect = None
        db.begin_knowledge_materialization.return_value = {
            "id": intent_id,
            "canonical_state": "canonical",
            "projection_state": "pending",
            "retry_state": "none",
        }

        with _patch_resolve(_ref()):
            result = await materialize_knowledge_metadata_update(
                postgres_db=db,
                gitea_client=g,
                project_id=PROJECT,
                slug=SLUG,
                add_tags=["ready"],
            )

        assert result == {
            "status": "skipped",
            "reason": "already-canonical",
            "repo": REPO,
            "branch": "main",
            "path": PATH,
            "operation": "metadata-update",
            "canonical_status": "active",
            "canonical_tags": ["category:executor", "ready"],
            "canonical_ready_at": datetime(
                2026, 8, 17, 10, 11, 12, tzinfo=timezone.utc
            ),
            "canonical_metadata_complete": True,
            "recorded": True,
            "intent_id": str(intent_id),
            "canonical_state": "canonical",
            "projection_state": "pending",
            "retry_state": "none",
        }
        g.get_file_content.assert_awaited_once_with(REPO, PATH)
        g.change_files.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_already_canonical_missing_file_never_manufactures_metadata(self):
        g = _make_gitea()
        g.get_file_content = AsyncMock(return_value=None)
        db = _ledger_db()
        db.begin_knowledge_materialization.side_effect = None
        db.begin_knowledge_materialization.return_value = {
            "id": uuid.uuid4(),
            "canonical_state": "canonical",
            "projection_state": "pending",
            "retry_state": "none",
        }

        with _patch_resolve(_ref()):
            result = await materialize_knowledge_metadata_update(
                postgres_db=db,
                gitea_client=g,
                project_id=PROJECT,
                slug=SLUG,
                add_tags=["ready"],
            )

        assert result["status"] == "failed"
        assert result["reason"] == "canonical-file-missing"
        assert result["canonical_state"] == "canonical"
        assert "canonical_tags" not in result
        assert "canonical_ready_at" not in result

    @pytest.mark.asyncio
    async def test_both_operations_refused_reports_commit_refused(self):
        g = _make_gitea(change_results=[False, False])

        result, _, _ = await _run(g)

        assert result["status"] == "failed"
        assert result["reason"] == "commit-refused"
        assert result["repo"] == REPO
        assert result["path"] == PATH
        assert g.change_files.await_count == 2

    @pytest.mark.asyncio
    async def test_raising_gitea_reports_commit_error(self):
        g = _make_gitea(change_raises=True)

        result, _, _ = await _run(g)

        assert result["status"] == "failed"
        assert result["reason"] == "commit-error"

    @pytest.mark.asyncio
    async def test_resolution_failure_is_not_fatal(self):
        g = _make_gitea()
        db = _ledger_db()
        with patch(
            "orchestrator.services.kb_materialize.resolve_kb_repo",
            AsyncMock(side_effect=RuntimeError("db down")),
        ):
            result = await materialize_knowledge_note(
                postgres_db=db,
                gitea_client=g,
                project_id=PROJECT,
                slug=SLUG,
                content=BODY,
                job_id=JOB,
            )

        assert result["status"] == "failed"
        assert result["reason"] == "resolve-error"
        g.change_files.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unsafe_slug_never_reaches_gitea(self):
        g = _make_gitea()

        result, resolve, _ = await _run(g, slug="../../../etc/passwd")

        assert result["status"] == "failed"
        assert result["reason"] == "invalid-slug"
        resolve.assert_not_awaited()
        g.change_files.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_empty_body_is_refused(self):
        """An empty note file parses as a frontmatter-less note and would be
        indexed as junk."""
        g = _make_gitea()

        result, _, _ = await _run(g, content="   \n")

        assert result["status"] == "failed"
        assert result["reason"] == "empty-content"
        g.change_files.assert_not_awaited()


# =============================================================================
# Endpoint — POST /api/projects/{project_id}/knowledge/materialize
# =============================================================================


class TestMaterializeEndpoint:
    @pytest.mark.asyncio
    async def test_rejects_calls_without_the_internal_key(self, fake_request):
        """Agent-internal (P4b): same gate as the job /complete callback."""
        from orchestrator.routers.knowledge import materialize_knowledge_note
        from orchestrator.schemas.knowledge import KnowledgeMaterializeRequest

        fake_request.headers = {}
        body = KnowledgeMaterializeRequest(slug=SLUG, content=BODY, job_id=JOB)
        with patch.object(access_module, "_INTERNAL_KEY", "secret"):
            with pytest.raises(HTTPException) as exc:
                await materialize_knowledge_note(
                    fake_request, PROJECT, body, dependencies=_knowledge_deps()
                )
        assert exc.value.status_code == 401

    @pytest.mark.asyncio
    async def test_internal_call_commits_and_returns_the_service_result(
        self, fake_request
    ):
        from orchestrator.routers.knowledge import materialize_knowledge_note
        from orchestrator.schemas.knowledge import KnowledgeMaterializeRequest

        g = _make_gitea()
        fake_request.headers = {"X-Internal-Key": "secret"}
        body = KnowledgeMaterializeRequest(slug=SLUG, content=BODY, job_id=JOB)
        deps = _knowledge_deps(store=_ledger_db(), gitea=g)
        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            _patch_resolve(_ref()),
        ):
            result = await materialize_knowledge_note(
                fake_request, PROJECT, body, dependencies=deps
            )

        assert result["status"] == "committed"
        assert result["path"] == PATH
        assert g.change_files.await_count == 1

    @pytest.mark.asyncio
    async def test_failure_is_a_200_body_not_an_http_error(self, fake_request):
        """The internal transport returns structured retry truth to callers."""
        from orchestrator.routers.knowledge import materialize_knowledge_note
        from orchestrator.schemas.knowledge import KnowledgeMaterializeRequest

        g = _make_gitea(change_raises=True)
        fake_request.headers = {"X-Internal-Key": "secret"}
        body = KnowledgeMaterializeRequest(slug=SLUG, content=BODY, job_id=JOB)
        deps = _knowledge_deps(store=_ledger_db(), gitea=g)
        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            _patch_resolve(_ref()),
        ):
            result = await materialize_knowledge_note(
                fake_request, PROJECT, body, dependencies=deps
            )

        assert result["status"] == "failed"
        assert result["reason"] == "commit-error"


class TestDeleteEndpoints:
    """kb_gardening G2: the internal purge endpoint and the rerouted member
    DELETE both go through the ledgered file-removal op."""

    @pytest.mark.asyncio
    async def test_internal_delete_requires_the_internal_key(self, fake_request):
        from orchestrator.routers.knowledge import delete_knowledge_note_internal
        from orchestrator.schemas.knowledge import KnowledgeDeleteRequest

        fake_request.headers = {}
        with patch.object(access_module, "_INTERNAL_KEY", "secret"):
            with pytest.raises(HTTPException) as exc:
                await delete_knowledge_note_internal(
                    fake_request,
                    PROJECT,
                    KnowledgeDeleteRequest(slug=SLUG),
                    dependencies=_knowledge_deps(),
                )
        assert exc.value.status_code == 401

    @pytest.mark.asyncio
    async def test_internal_delete_forwards_token_and_reason(self, fake_request):
        from orchestrator.routers.knowledge import delete_knowledge_note_internal
        from orchestrator.schemas.knowledge import KnowledgeDeleteRequest

        sha = _blob_sha(BODY)
        g = _make_gitea(tree_paths={PATH: sha})
        g.delete_path = AsyncMock(return_value="deleted")
        fake_request.headers = {"X-Internal-Key": "secret"}
        body = KnowledgeDeleteRequest(
            slug=SLUG, job_id=JOB, reason="purge test", expected_blob_sha=sha
        )
        store_cls = MagicMock()
        store = MagicMock()
        store.delete_kb_note = AsyncMock(return_value=True)
        store.delete_note = AsyncMock(return_value=False)
        store_cls.return_value = store
        deps = _knowledge_deps(store=_ledger_db(), gitea=g, vector=MagicMock())
        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            patch("shared.runtime.services.knowledge_store.KnowledgeStore", store_cls),
            _patch_resolve(_ref()),
        ):
            result = await delete_knowledge_note_internal(
                fake_request, PROJECT, body, dependencies=deps
            )
        assert result["status"] == "committed"
        assert result["operation"] == "delete"
        assert result["row_deleted"] is True
        assert "purge test" in g.delete_path.await_args.args[3]
        assert g.delete_path.await_args.kwargs["expected_sha"] == sha

    @pytest.mark.asyncio
    async def test_member_delete_removes_the_file_unconditionally(self, fake_request):
        """A human's delete carries no CAS token: the cockpit button is the
        authority. It used to remove only the row, which the next sweep
        resurrected from the untouched file."""
        from orchestrator.routers.knowledge import delete_knowledge_note

        g = _make_gitea(tree_paths={PATH: _blob_sha(BODY)})
        g.delete_path = AsyncMock(return_value="deleted")
        store_cls = MagicMock()
        store = MagicMock()
        store.delete_kb_note = AsyncMock(return_value=True)
        store.delete_note = AsyncMock(return_value=False)
        store_cls.return_value = store
        deps = _knowledge_deps(
            member_gate=AsyncMock(return_value=_ADMIN_CALLER),
            store=_ledger_db(),
            gitea=g,
            vector=MagicMock(),
            graph=SimpleNamespace(get=lambda: None),
        )
        with (
            patch("shared.runtime.services.knowledge_store.KnowledgeStore", store_cls),
            _patch_resolve(_ref()),
        ):
            result = await delete_knowledge_note(
                fake_request, PROJECT, SLUG, dependencies=deps
            )
        assert result == {
            "status": "deleted",
            "file_removed": True,
            "row_deleted": True,
            "path": PATH,
        }
        assert g.delete_path.await_args.kwargs["expected_sha"] == _blob_sha(BODY)
        assert "cockpit" in g.delete_path.await_args.args[3]

    @pytest.mark.asyncio
    async def test_member_delete_of_a_note_that_exists_nowhere_is_404(
        self, fake_request
    ):
        from orchestrator.routers.knowledge import delete_knowledge_note

        g = _make_gitea(tree_paths={})
        g.delete_path = AsyncMock(return_value="deleted")
        store_cls = MagicMock()
        store = MagicMock()
        store.delete_kb_note = AsyncMock(return_value=False)
        store.delete_note = AsyncMock(return_value=False)
        store_cls.return_value = store
        deps = _knowledge_deps(
            member_gate=AsyncMock(return_value=_ADMIN_CALLER),
            store=_ledger_db(),
            gitea=g,
            vector=MagicMock(),
            graph=SimpleNamespace(get=lambda: None),
        )
        with (
            patch("shared.runtime.services.knowledge_store.KnowledgeStore", store_cls),
            _patch_resolve(_ref()),
        ):
            with pytest.raises(HTTPException) as exc:
                await delete_knowledge_note(
                    fake_request, PROJECT, SLUG, dependencies=deps
                )
        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_member_delete_forge_failure_is_502_not_a_row_only_delete(
        self, fake_request
    ):
        from orchestrator.routers.knowledge import delete_knowledge_note

        g = _make_gitea(tree_paths={PATH: _blob_sha(BODY)})
        g.delete_path = AsyncMock(return_value="error")
        store_cls = MagicMock()
        store = MagicMock()
        store.delete_kb_note = AsyncMock(return_value=True)
        store_cls.return_value = store
        deps = _knowledge_deps(
            member_gate=AsyncMock(return_value=_ADMIN_CALLER),
            store=_ledger_db(),
            gitea=g,
            vector=MagicMock(),
        )
        with (
            patch("shared.runtime.services.knowledge_store.KnowledgeStore", store_cls),
            _patch_resolve(_ref()),
        ):
            with pytest.raises(HTTPException) as exc:
                await delete_knowledge_note(
                    fake_request, PROJECT, SLUG, dependencies=deps
                )
        assert exc.value.status_code == 502
        store.delete_kb_note.assert_not_awaited()


class TestKnowledgeMutationEndpoint:
    @staticmethod
    def _vector(row=None, projected=SLUG):
        conn = MagicMock()
        conn.fetchrow = AsyncMock(return_value=row or {"note_id": SLUG})
        conn.fetchval = AsyncMock(return_value=projected)
        vector = MagicMock()
        vector.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
        vector.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        return vector, conn

    @pytest.mark.asyncio
    async def test_authorization_precedes_any_materialization(self, fake_request):
        from orchestrator.routers.knowledge import update_knowledge_note
        from orchestrator.schemas.knowledge import KnowledgeNoteUpdate

        materialize = AsyncMock()
        deps = _knowledge_deps(
            member_gate=AsyncMock(side_effect=HTTPException(status_code=403))
        )
        with (
            patch(
                "orchestrator.services.kb_materialize.materialize_knowledge_metadata_update",
                materialize,
            ),
        ):
            with pytest.raises(HTTPException) as exc:
                await update_knowledge_note(
                    fake_request,
                    PROJECT,
                    SLUG,
                    KnowledgeNoteUpdate(status="resolved"),
                    dependencies=deps,
                )

        assert exc.value.status_code == 403
        materialize.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_pending_canonical_write_returns_409_and_leaves_index_unchanged(
        self, fake_request
    ):
        from orchestrator.routers.knowledge import update_knowledge_note
        from orchestrator.schemas.knowledge import KnowledgeNoteUpdate

        vector, conn = self._vector()
        db = AsyncMock()
        pending = {
            "status": "failed",
            "reason": "commit-error",
            "intent_id": str(uuid.uuid4()),
            "canonical_state": "pending_sync",
            "projection_state": "pending",
            "retry_state": "retryable",
        }
        deps = _knowledge_deps(
            member_gate=AsyncMock(return_value=_ADMIN_CALLER), vector=vector, store=db
        )
        with (
            patch(
                "orchestrator.services.kb_materialize.materialize_knowledge_metadata_update",
                AsyncMock(return_value=pending),
            ),
        ):
            with pytest.raises(HTTPException) as exc:
                await update_knowledge_note(
                    fake_request,
                    PROJECT,
                    SLUG,
                    KnowledgeNoteUpdate(status="resolved"),
                    dependencies=deps,
                )

        assert exc.value.status_code == 409
        assert exc.value.detail["status"] == "pending_sync"
        assert exc.value.detail["retry_state"] == "retryable"
        conn.fetchval.assert_not_awaited()
        db.finish_knowledge_projection.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_success_names_canonical_and_projection_truth(self, fake_request):
        from orchestrator.routers.knowledge import update_knowledge_note
        from orchestrator.schemas.knowledge import KnowledgeNoteUpdate

        vector, conn = self._vector()
        intent_id = str(uuid.uuid4())
        db = AsyncMock()
        db.finish_knowledge_projection.return_value = {
            "id": intent_id,
            "canonical_state": "canonical",
            "projection_state": "synced",
        }
        canonical = {
            "status": "committed",
            "intent_id": intent_id,
            "canonical_state": "canonical",
            "projection_state": "pending",
            "retry_state": "none",
            "canonical_status": "resolved",
            "canonical_tags": [],
            "canonical_ready_at": None,
            "canonical_metadata_complete": True,
        }
        deps = _knowledge_deps(
            member_gate=AsyncMock(return_value=_ADMIN_CALLER),
            vector=vector,
            store=db,
            graph=SimpleNamespace(get=lambda: None),
        )
        with (
            patch(
                "orchestrator.services.kb_materialize.materialize_knowledge_metadata_update",
                AsyncMock(return_value=canonical),
            ),
        ):
            result = await update_knowledge_note(
                fake_request,
                PROJECT,
                SLUG,
                KnowledgeNoteUpdate(status="resolved"),
                dependencies=deps,
            )

        assert result == {
            "status": "updated",
            "canonical_state": "canonical",
            "projection_state": "synced",
            "intent_id": intent_id,
        }
        conn.fetchval.assert_awaited_once()
        db.finish_knowledge_projection.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_tag_projection_uses_exact_canonical_tags_and_ready_time(
        self, fake_request
    ):
        from orchestrator.routers.knowledge import update_knowledge_note
        from orchestrator.schemas.knowledge import KnowledgeNoteUpdate

        vector, conn = self._vector()
        intent_id = str(uuid.uuid4())
        ready_at = datetime(2026, 8, 17, 10, 11, 12, tzinfo=timezone.utc)
        db = AsyncMock()
        db.finish_knowledge_projection.return_value = {
            "id": intent_id,
            "canonical_state": "canonical",
            "projection_state": "synced",
        }
        canonical = {
            "status": "committed",
            "intent_id": intent_id,
            "canonical_state": "canonical",
            "projection_state": "pending",
            "retry_state": "none",
            "canonical_tags": ["category:executor", "ready"],
            "canonical_ready_at": ready_at,
            "canonical_metadata_complete": True,
        }
        deps = _knowledge_deps(
            member_gate=AsyncMock(return_value=_ADMIN_CALLER),
            vector=vector,
            store=db,
            graph=SimpleNamespace(get=lambda: None),
        )
        with (
            patch(
                "orchestrator.services.kb_materialize.materialize_knowledge_metadata_update",
                AsyncMock(return_value=canonical),
            ),
        ):
            result = await update_knowledge_note(
                fake_request,
                PROJECT,
                SLUG,
                KnowledgeNoteUpdate(add_tags=["ready"]),
                dependencies=deps,
            )

        assert result["projection_state"] == "synced"
        assert conn.fetchval.await_args.args[3:] == (
            ["category:executor", "ready"],
            ready_at,
        )

    @pytest.mark.asyncio
    async def test_ready_removal_projects_canonical_null_generation(self, fake_request):
        from orchestrator.routers.knowledge import update_knowledge_note
        from orchestrator.schemas.knowledge import KnowledgeNoteUpdate

        vector, conn = self._vector()
        intent_id = str(uuid.uuid4())
        db = AsyncMock()
        db.finish_knowledge_projection.return_value = {
            "id": intent_id,
            "canonical_state": "canonical",
            "projection_state": "synced",
        }
        canonical = {
            "status": "committed",
            "intent_id": intent_id,
            "canonical_state": "canonical",
            "projection_state": "pending",
            "retry_state": "none",
            "canonical_tags": ["category:executor"],
            "canonical_ready_at": None,
            "canonical_metadata_complete": True,
        }
        deps = _knowledge_deps(
            member_gate=AsyncMock(return_value=_ADMIN_CALLER),
            vector=vector,
            store=db,
            graph=SimpleNamespace(get=lambda: None),
        )
        with (
            patch(
                "orchestrator.services.kb_materialize.materialize_knowledge_metadata_update",
                AsyncMock(return_value=canonical),
            ),
        ):
            result = await update_knowledge_note(
                fake_request,
                PROJECT,
                SLUG,
                KnowledgeNoteUpdate(remove_tags=["ready"]),
                dependencies=deps,
            )

        assert result["projection_state"] == "synced"
        assert conn.fetchval.await_args.args[3:] == (["category:executor"], None)

    @pytest.mark.asyncio
    async def test_missing_canonical_snapshot_returns_409_without_projection(
        self, fake_request
    ):
        from orchestrator.routers.knowledge import update_knowledge_note
        from orchestrator.schemas.knowledge import KnowledgeNoteUpdate

        vector, conn = self._vector()
        intent_id = str(uuid.uuid4())
        db = AsyncMock()
        canonical_without_snapshot = {
            "status": "skipped",
            "reason": "already-canonical",
            "intent_id": intent_id,
            "canonical_state": "canonical",
            "projection_state": "pending",
            "retry_state": "none",
        }
        deps = _knowledge_deps(
            member_gate=AsyncMock(return_value=_ADMIN_CALLER), vector=vector, store=db
        )
        with (
            patch(
                "orchestrator.services.kb_materialize.materialize_knowledge_metadata_update",
                AsyncMock(return_value=canonical_without_snapshot),
            ),
        ):
            with pytest.raises(HTTPException) as exc:
                await update_knowledge_note(
                    fake_request,
                    PROJECT,
                    SLUG,
                    KnowledgeNoteUpdate(add_tags=["ready"]),
                    dependencies=deps,
                )

        assert exc.value.status_code == 409
        assert exc.value.detail["reason"] == "already-canonical"
        conn.fetchval.assert_not_awaited()
        db.finish_knowledge_projection.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_invalid_canonical_ready_time_returns_409_without_projection(
        self, fake_request
    ):
        from orchestrator.routers.knowledge import update_knowledge_note
        from orchestrator.schemas.knowledge import KnowledgeNoteUpdate

        vector, conn = self._vector()
        db = AsyncMock()
        canonical = {
            "status": "committed",
            "intent_id": str(uuid.uuid4()),
            "canonical_state": "canonical",
            "projection_state": "pending",
            "retry_state": "none",
            "canonical_tags": ["ready"],
            "canonical_ready_at": "not-a-timestamp",
            "canonical_metadata_complete": True,
        }
        deps = _knowledge_deps(
            member_gate=AsyncMock(return_value=_ADMIN_CALLER), vector=vector, store=db
        )
        with (
            patch(
                "orchestrator.services.kb_materialize.materialize_knowledge_metadata_update",
                AsyncMock(return_value=canonical),
            ),
        ):
            with pytest.raises(HTTPException) as exc:
                await update_knowledge_note(
                    fake_request,
                    PROJECT,
                    SLUG,
                    KnowledgeNoteUpdate(add_tags=["ready"]),
                    dependencies=deps,
                )

        assert exc.value.status_code == 409
        assert exc.value.detail["reason"] == "canonical-ready-at-invalid"
        conn.fetchval.assert_not_awaited()


# Inline indexing (Slice A) — the endpoint indexes the note it just committed,
# so kb_search/kb_read find it when the write returns instead of after the
# next 900s sweep.

import asyncio  # noqa: E402
from contextlib import asynccontextmanager  # noqa: E402

from orchestrator.services.kb_reindex import NoteIndexOutcome  # noqa: E402


def _indexing_store(*, lock_claimed: bool = True) -> MagicMock:
    """A store whose only job is to lend (or refuse) the per-KB reindex lock."""
    store = MagicMock()

    @asynccontextmanager
    async def _lock(_kb_id):
        yield lock_claimed

    store.try_reindex_lock = _lock
    return store


class TestInlineIndexOnMaterialize:
    def test_a_committed_note_is_indexed_before_the_call_returns(self):
        db = _ledger_db()
        gitea = _make_gitea(change_results=[True])
        indexed = {}

        async def _fake_index(**kwargs):
            indexed.update(kwargs)
            return NoteIndexOutcome(status="indexed", note_id=SLUG, chunks=1)

        with (
            _patch_resolve(_ref()),
            patch(
                "orchestrator.services.kb_materialize.index_single_note", _fake_index
            ),
        ):
            result = asyncio.run(
                materialize_knowledge_note(
                    postgres_db=db,
                    gitea_client=gitea,
                    project_id=PROJECT,
                    slug=SLUG,
                    content=BODY,
                    store=_indexing_store(),
                    embedding_service=AsyncMock(),
                )
            )

        assert result["status"] == "committed"
        assert result["indexed"] is True
        assert result["index_reason"] is None
        assert indexed["path"] == PATH
        assert indexed["kb_id"] == uuid.UUID(PROJECT)
        assert indexed["blob_sha"] == _blob_sha(BODY)
        assert indexed["max_chunks"] is not None
        db.finish_knowledge_projection.assert_awaited_once()

    def test_the_writing_job_reaches_the_indexed_row(self):
        # The endpoint already had job_id (it names the commit); this carries
        # it the last hop onto knowledge_index.job_id, which backs
        # kb_list(job_id=...). Parsed to a UUID here because the request body
        # carries it as a string and the column is UUID.
        db = _ledger_db()
        gitea = _make_gitea(change_results=[True])
        job = "6f1d5a2e-0b3c-4d5e-8a9b-1c2d3e4f5a6b"
        indexed = {}

        async def _fake_index(**kwargs):
            indexed.update(kwargs)
            return NoteIndexOutcome(status="indexed", note_id=SLUG, chunks=1)

        with (
            _patch_resolve(_ref()),
            patch(
                "orchestrator.services.kb_materialize.index_single_note", _fake_index
            ),
        ):
            result = asyncio.run(
                materialize_knowledge_note(
                    postgres_db=db,
                    gitea_client=gitea,
                    project_id=PROJECT,
                    slug=SLUG,
                    content=BODY,
                    job_id=job,
                    store=_indexing_store(),
                    embedding_service=AsyncMock(),
                )
            )

        assert result["indexed"] is True
        assert indexed["job_id"] == uuid.UUID(job)

    def test_a_note_written_without_a_job_forwards_none(self):
        # Persistent sessions have no job. None is the sentinel meaning "leave
        # the stored value alone" — never a zero UUID, never a string.
        gitea = _make_gitea(change_results=[True])
        indexed = {}

        async def _fake_index(**kwargs):
            indexed.update(kwargs)
            return NoteIndexOutcome(status="indexed", note_id=SLUG, chunks=1)

        with (
            _patch_resolve(_ref()),
            patch(
                "orchestrator.services.kb_materialize.index_single_note", _fake_index
            ),
        ):
            asyncio.run(
                materialize_knowledge_note(
                    postgres_db=_ledger_db(),
                    gitea_client=gitea,
                    project_id=PROJECT,
                    slug=SLUG,
                    content=BODY,
                    store=_indexing_store(),
                    embedding_service=AsyncMock(),
                )
            )
        assert indexed["job_id"] is None

    def test_an_unparseable_job_id_still_indexes_the_note(self):
        # Provenance is not worth losing the index over: the commit has
        # already happened, so a malformed id degrades to no provenance
        # rather than to a note nobody can find.
        gitea = _make_gitea(change_results=[True])
        indexed = {}

        async def _fake_index(**kwargs):
            indexed.update(kwargs)
            return NoteIndexOutcome(status="indexed", note_id=SLUG, chunks=1)

        with (
            _patch_resolve(_ref()),
            patch(
                "orchestrator.services.kb_materialize.index_single_note", _fake_index
            ),
        ):
            result = asyncio.run(
                materialize_knowledge_note(
                    postgres_db=_ledger_db(),
                    gitea_client=gitea,
                    project_id=PROJECT,
                    slug=SLUG,
                    content=BODY,
                    job_id="not-a-uuid",
                    store=_indexing_store(),
                    embedding_service=AsyncMock(),
                )
            )
        assert result["indexed"] is True
        assert indexed["job_id"] is None

    def test_without_a_store_the_note_commits_and_defers(self):
        gitea = _make_gitea(change_results=[True])
        with _patch_resolve(_ref()):
            result = asyncio.run(
                materialize_knowledge_note(
                    postgres_db=_ledger_db(),
                    gitea_client=gitea,
                    project_id=PROJECT,
                    slug=SLUG,
                    content=BODY,
                )
            )
        assert result["status"] == "committed"
        assert result["indexed"] is False
        assert result["index_reason"] == "no-indexer"

    def test_a_held_reindex_lock_defers_instead_of_blocking(self, caplog):
        gitea = _make_gitea(change_results=[True])
        caplog.set_level(logging.INFO)
        with (
            _patch_resolve(_ref()),
            patch(
                "orchestrator.services.kb_materialize.index_single_note",
                AsyncMock(
                    side_effect=AssertionError("must not index under a held lock")
                ),
            ),
        ):
            result = asyncio.run(
                materialize_knowledge_note(
                    postgres_db=_ledger_db(),
                    gitea_client=gitea,
                    project_id=PROJECT,
                    slug=SLUG,
                    content=BODY,
                    store=_indexing_store(lock_claimed=False),
                    embedding_service=AsyncMock(),
                )
            )
        assert result["status"] == "committed"
        assert result["indexed"] is False
        assert result["index_reason"] == "reindex-running"
        # The refusal must not be silent (review finding): the note is
        # canonical but unsearchable until the sweep heals it, and the holder
        # may be a concurrent inline write to this same project, not only the
        # sweep — the message must not blame the sweep alone.
        assert PATH in caplog.text
        assert "index lock held" in caplog.text
        assert "sweep or concurrent write" in caplog.text

    def test_an_index_failure_leaves_the_note_canonical_and_deferred(self):
        db = _ledger_db()
        gitea = _make_gitea(change_results=[True])
        with (
            _patch_resolve(_ref()),
            patch(
                "orchestrator.services.kb_materialize.index_single_note",
                AsyncMock(side_effect=RuntimeError("embedding backend down")),
            ),
        ):
            result = asyncio.run(
                materialize_knowledge_note(
                    postgres_db=db,
                    gitea_client=gitea,
                    project_id=PROJECT,
                    slug=SLUG,
                    content=BODY,
                    store=_indexing_store(),
                    embedding_service=AsyncMock(),
                )
            )
        assert result["status"] == "committed"
        assert result["canonical_state"] == "canonical"
        assert result["indexed"] is False
        assert result["index_reason"] == "index-error"
        db.finish_knowledge_projection.assert_not_awaited()

    def test_a_failed_commit_is_never_indexed(self):
        gitea = _make_gitea(change_results=[False, False])
        with (
            _patch_resolve(_ref()),
            patch(
                "orchestrator.services.kb_materialize.index_single_note",
                AsyncMock(side_effect=AssertionError("must not index a failed commit")),
            ),
        ):
            result = asyncio.run(
                materialize_knowledge_note(
                    postgres_db=_ledger_db(),
                    gitea_client=gitea,
                    project_id=PROJECT,
                    slug=SLUG,
                    content=BODY,
                    store=_indexing_store(),
                    embedding_service=AsyncMock(),
                )
            )
        assert result["status"] == "failed"
        assert result["indexed"] is False
        assert result["index_reason"] == "not-canonical"
