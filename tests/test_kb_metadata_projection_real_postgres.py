"""BP-13: real-asyncpg proof for canonical metadata projection retries.

Mocks accepted an ISO string for ``$n::timestamptz`` while asyncpg rejected the
same value on main dev after Git had committed it.  These tests keep Git and
the app-ledger in process, but run the supported metadata endpoint against the
real migrated pgvector schema so the driver codec and projection SQL are part
of the contract.
"""

from __future__ import annotations

import base64
import hashlib
import logging
import re
import uuid
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pytest
import pytest_asyncio
from fastapi import HTTPException

pytest.importorskip("testcontainers.postgres")

from orchestrator.database.migrate import run_migrations  # noqa: E402
from orchestrator.routers.knowledge import KnowledgeDependencies  # noqa: E402
from orchestrator.services.kb_materialize import retry_knowledge_materialization_intent  # noqa: E402
from orchestrator.services.kb_reindex import KbRepoRef  # noqa: E402
from orchestrator.services.kb_task_registry import KbDatasourceTaskRegistry  # noqa: E402
from orchestrator.services.knowledge_index import KnowledgeIndexDependencies  # noqa: E402
from orchestrator.services.knowledge_operations import (  # noqa: E402
    KnowledgeOperationDependencies,
)
from shared.runtime.knowledge.gardener import parse_note_md  # noqa: E402

PG_IMAGE = "pgvector/pgvector:pg15"
VECTOR_MIGRATIONS = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "migrations"
    / "vector"
)
NOTE_ID = "bp13-ready-projection"
NOTE_PATH = f"knowledge/{NOTE_ID}.md"


@pytest.fixture(scope="module")
def pg_dsn():
    from testcontainers.postgres import PostgresContainer

    container = PostgresContainer(PG_IMAGE)
    try:
        container.start()
    except Exception as exc:  # pragma: no cover - environment without a runtime
        pytest.skip(f"no container runtime for testcontainers: {exc}")
    try:
        yield re.sub(
            r"^postgresql\+\w+://", "postgresql://", container.get_connection_url()
        )
    finally:
        container.stop()


@pytest_asyncio.fixture
async def vector_pool(pg_dsn):
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
    try:
        await run_migrations(pool, VECTOR_MIGRATIONS)
        yield pool
    finally:
        await pool.close()


class _Ledger:
    """Small stateful app-ledger double; vector persistence remains real."""

    def __init__(self) -> None:
        self.intent_id = uuid.uuid4()
        self.canonical = False
        self.projection_state = "pending"
        self.content = ""
        self.finish_calls = 0

    async def begin_knowledge_materialization(self, **kwargs):
        self.content = str(kwargs["content"])
        return {
            "id": self.intent_id,
            "project_id": kwargs["project_id"],
            "note_id": kwargs["note_id"],
            "canonical_state": "canonical" if self.canonical else "pending_sync",
            "projection_state": self.projection_state,
            "retry_state": "none" if self.canonical else "retryable",
            "attempt_claimed": not self.canonical,
            "attempt_token": uuid.uuid4(),
        }

    async def finish_knowledge_materialization(
        self, _intent_id, *, canonical, permanent, **_kwargs
    ):
        self.finish_calls += 1
        self.canonical = bool(canonical)
        return {
            "id": self.intent_id,
            "canonical_state": (
                "canonical"
                if canonical
                else ("failed" if permanent else "pending_sync")
            ),
            "projection_state": self.projection_state,
            "retry_state": (
                "none" if canonical else ("permanent" if permanent else "retryable")
            ),
        }

    async def finish_knowledge_projection(
        self, _intent_id, *, project_id, synced, error=None
    ):
        del project_id, error
        self.projection_state = "synced" if synced else "failed"
        return {
            "id": self.intent_id,
            "canonical_state": "canonical" if self.canonical else "pending_sync",
            "projection_state": self.projection_state,
        }


def _blob_sha(text: str) -> str:
    body = text.encode("utf-8")
    return hashlib.sha1(b"blob %d\0" % len(body) + body).hexdigest()  # noqa: S324


def _stateful_gitea(initial: str, *, fail_first: bool = False):
    state = {"content": initial, "failures": 1 if fail_first else 0}
    client = MagicMock()

    async def list_tree(_repo, _ref):
        return [{"path": NOTE_PATH, "type": "blob", "sha": _blob_sha(state["content"])}]

    async def get_file_content(_repo, path):
        return state["content"] if path == NOTE_PATH else None

    async def change_files(_repo, _branch, files, *, message):
        del message
        if state["failures"]:
            state["failures"] -= 1
            raise RuntimeError("injected forge outage")
        state["content"] = base64.b64decode(files[0]["content_b64"]).decode()
        return True

    client.list_tree = AsyncMock(side_effect=list_tree)
    client.get_file_content = AsyncMock(side_effect=get_file_content)
    client.change_files = AsyncMock(side_effect=change_files)
    return client, state


async def _seed_note(pool, project_id: uuid.UUID, content: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO knowledge_index (
                note_id, project_id, title, note_type, status, tags, content,
                created_at, modified_at
            ) VALUES ($1, $2, 'BP-13 ticket', 'feature', 'active',
                      ARRAY['category:executor'], $3, now(), now())
            """,
            NOTE_ID,
            project_id,
            content,
        )


async def _projection(pool, project_id: uuid.UUID):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT status, tags, ready_at FROM knowledge_index "
            "WHERE project_id = $1 AND note_id = $2",
            project_id,
            NOTE_ID,
        )


def _knowledge_deps(*, ledger, vector_pool, gitea) -> KnowledgeDependencies:
    """The collaborators main's ``_knowledge_dependencies()`` factory supplies.

    Before R1.B03 each of these arrived by patching a module global on
    ``orchestrator.main`` (``postgres_db``, ``vector_db``, ``gitea_client``,
    ``_get_knowledge_graph``). The handler now takes them through this
    dataclass, so the same objects are passed in here instead — same fakes,
    same real ``update_knowledge_note`` body below them.
    """
    return KnowledgeDependencies(
        store=ledger,
        operations=KnowledgeOperationDependencies(
            store=ledger,
            vector_db=vector_pool,
            gitea_client=gitea,
            logger=logging.getLogger("tests.kb_metadata_projection"),
            # Neo4j absent, exactly as the old return_value=None patch modelled.
            graph=SimpleNamespace(get=lambda: None),
            knowledge_index=KnowledgeIndexDependencies(
                store=ledger,
                vector_db=vector_pool,
                gitea_client=gitea,
                logger=logging.getLogger("tests.kb_metadata_projection"),
                tasks=KbDatasourceTaskRegistry(),
                inject_system_kb_embedding_profile=AsyncMock(return_value=None),
            ),
        ),
        # An admin: adding ``ready`` arms dispatch and needs owner authority
        # (D9), which is what these BP-13 scenarios model.
        require_project_member=AsyncMock(
            return_value=({"id": "u-admin", "is_admin": True}, {})
        ),
    )


def _repo_ref() -> KbRepoRef:
    return KbRepoRef(
        forge="gitea",
        repo_url="",
        owner="",
        repo="bp13-knowledge",
        branch="main",
    )


@pytest.mark.asyncio
async def test_first_ready_update_commits_and_projects_typed_timestamp(vector_pool):
    from orchestrator.routers.knowledge import update_knowledge_note
    from orchestrator.schemas.knowledge import KnowledgeNoteUpdate

    project_id = uuid.uuid4()
    initial = (
        f"---\nid: {NOTE_ID}\ntype: feature\nstatus: active\n"
        "tags: [category:executor]\n---\n# BP-13\n"
    )
    await _seed_note(vector_pool, project_id, initial)
    ledger = _Ledger()
    gitea, git = _stateful_gitea(initial)

    deps = _knowledge_deps(ledger=ledger, vector_pool=vector_pool, gitea=gitea)
    with patch(
        "orchestrator.services.kb_materialize.resolve_kb_repo",
        AsyncMock(return_value=_repo_ref()),
    ):
        result = await update_knowledge_note(
            MagicMock(),
            str(project_id),
            NOTE_ID,
            KnowledgeNoteUpdate(add_tags=["ready"]),
            dependencies=deps,
        )

    frontmatter, _ = parse_note_md(git["content"])
    row = await _projection(vector_pool, project_id)
    assert result["projection_state"] == "synced"
    assert ledger.canonical is True
    assert ledger.projection_state == "synced"
    assert frontmatter is not None
    assert frontmatter["tags"] == ["category:executor", "ready"]
    canonical_ready = datetime.fromisoformat(str(frontmatter["ready_at"]))
    assert row["tags"] == ["category:executor", "ready"]
    assert isinstance(row["ready_at"], datetime)
    assert row["ready_at"] == canonical_ready
    assert git["content"].count("ready_at:") == 1


@pytest.mark.asyncio
async def test_sweeper_wins_retry_then_client_preserves_exact_canonical_snapshot(
    vector_pool,
):
    from orchestrator.routers.knowledge import update_knowledge_note
    from orchestrator.schemas.knowledge import KnowledgeNoteUpdate

    project_id = uuid.uuid4()
    initial = (
        f"---\nid: {NOTE_ID}\ntype: feature\nstatus: active\n"
        "tags: [category:executor]\n---\n# BP-13 retry\n"
    )
    await _seed_note(vector_pool, project_id, initial)
    ledger = _Ledger()
    gitea, git = _stateful_gitea(initial, fail_first=True)

    deps = _knowledge_deps(ledger=ledger, vector_pool=vector_pool, gitea=gitea)
    with patch(
        "orchestrator.services.kb_materialize.resolve_kb_repo",
        AsyncMock(return_value=_repo_ref()),
    ):
        with pytest.raises(HTTPException) as exc:
            await update_knowledge_note(
                MagicMock(),
                str(project_id),
                NOTE_ID,
                KnowledgeNoteUpdate(add_tags=["ready"]),
                dependencies=deps,
            )
        assert exc.value.status_code == 409
        assert (await _projection(vector_pool, project_id))["ready_at"] is None

        # Model the leader sweep claiming the durable retry before the client
        # repeats its request.  It commits exactly once and leaves projection
        # pending for reindex or the idempotent client retry.
        retried = await retry_knowledge_materialization_intent(
            postgres_db=ledger,
            gitea_client=gitea,
            intent={
                "id": ledger.intent_id,
                "project_id": str(project_id),
                "note_id": NOTE_ID,
                "operation": "metadata-update",
                "content": ledger.content,
                "attempt_token": uuid.uuid4(),
            },
        )
        assert retried["canonical_state"] == "canonical"
        canonical_after_sweep = git["content"]

        result = await update_knowledge_note(
            MagicMock(),
            str(project_id),
            NOTE_ID,
            KnowledgeNoteUpdate(add_tags=["ready"]),
            dependencies=deps,
        )

    frontmatter, _ = parse_note_md(canonical_after_sweep)
    row = await _projection(vector_pool, project_id)
    assert result["projection_state"] == "synced"
    assert frontmatter is not None
    canonical_ready = datetime.fromisoformat(str(frontmatter["ready_at"]))
    assert row["tags"] == ["category:executor", "ready"]
    assert row["ready_at"] == canonical_ready
    assert git["content"] == canonical_after_sweep
    assert git["content"].count("ready_at:") == 1
    assert gitea.change_files.await_count == 2  # one injected fault, one commit
    assert ledger.projection_state == "synced"
