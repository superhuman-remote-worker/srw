"""Ordering and degradation contracts for the KB chunk-index lifecycle.

Before R1.B03 these operations were private helpers in ``main``;
``tests/test_kb_datasource_delete_race.py`` pinned the delete race against
``main``'s own names. This file pins the same guarantees against the extracted
service, plus the three that a move can silently drop:

* the **delete-before-late-write fence** — the in-flight rebuild for that
  source is cancelled first, and the per-KB advisory claim is then held across
  the vector purge *and* the app-row delete, because the two live in separate
  databases and only the claim orders them;
* the **purge** variant drops the disposable index but never the row (a
  project's own KB has already been re-marked and must survive);
* **no key, no index** — ``build_kb_embedding_service`` returns ``None`` rather
  than a partially configured service, and every caller skips instead of
  writing vectorless rows.
"""

import asyncio
import uuid
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.services import knowledge_index
from orchestrator.services.kb_reindex import kb_index_lock
from orchestrator.services.kb_task_registry import KbDatasourceTaskRegistry


PROJECT_ID = "99999999-8888-7777-6666-555555555555"


@pytest.fixture
def datasource_id() -> str:
    """A KB id nothing else in the suite has used.

    ``kb_reindex._kb_locks`` caches one process-global ``asyncio.Lock`` per KB
    id and never prunes it, so two tests that share an id also share a lock
    bound to whichever event loop touched it first.
    """
    return str(uuid.uuid4())


class _CoordinatedStore:
    """Small concrete store whose advisory contexts share one test lock."""

    def __init__(self, events: list[str]):
        self.events = events
        self.advisory_lock = asyncio.Lock()
        self.delete_kb_index = AsyncMock(side_effect=self._delete_index)
        self.set_watermark_status = AsyncMock()
        self.get_watermark = AsyncMock(return_value=None)

    async def _delete_index(self, _kb_id: uuid.UUID, *, conn=None) -> None:
        assert conn is self
        self.events.append("vector-delete")

    @asynccontextmanager
    async def try_reindex_lock(self, _kb_id: uuid.UUID):
        if self.advisory_lock.locked():
            yield False
            return
        await self.advisory_lock.acquire()
        try:
            yield True
        finally:
            self.advisory_lock.release()

    @asynccontextmanager
    async def reindex_lock(self, _kb_id: uuid.UUID):
        async with self.advisory_lock:
            yield self


def _dependencies(
    *,
    store=None,
    vector_db=None,
    gitea_client=None,
    tasks=None,
    inject=None,
) -> knowledge_index.KnowledgeIndexDependencies:
    return knowledge_index.KnowledgeIndexDependencies(
        store=store if store is not None else MagicMock(),
        vector_db=vector_db if vector_db is not None else MagicMock(),
        gitea_client=gitea_client if gitea_client is not None else MagicMock(),
        logger=MagicMock(),
        tasks=tasks if tasks is not None else KbDatasourceTaskRegistry(),
        inject_system_kb_embedding_profile=inject or AsyncMock(return_value=None),
    )


# =============================================================================
# delete_kb_datasource_with_index — the fence
# =============================================================================


@pytest.mark.asyncio
async def test_delete_waits_for_an_in_flight_writer_then_orders_both_databases(
    datasource_id: str,
):
    kb_id = uuid.UUID(datasource_id)
    events: list[str] = []
    writer_started = asyncio.Event()
    release_writer = asyncio.Event()
    knowledge_store = _CoordinatedStore(events)

    async def delete_row(
        _datasource_id: str,
        *,
        authority_project_scope_id: str | None = None,
        deleted_by: str | None = None,
    ) -> bool:
        assert authority_project_scope_id == PROJECT_ID
        assert deleted_by == "user-7"
        events.append("app-delete")
        return True

    db = MagicMock()
    db.delete_datasource = AsyncMock(side_effect=delete_row)

    async def in_flight_writer() -> None:
        async with kb_index_lock(knowledge_store, kb_id):
            events.append("writer-start")
            writer_started.set()
            await release_writer.wait()
            events.append("writer-finish")

    writer = asyncio.create_task(in_flight_writer())
    await writer_started.wait()

    dependencies = _dependencies(store=db)
    with patch(
        "shared.runtime.services.knowledge_store.KnowledgeStore",
        return_value=knowledge_store,
    ):
        deletion = asyncio.create_task(
            knowledge_index.delete_kb_datasource_with_index(
                datasource_id,
                authority_project_scope_id=PROJECT_ID,
                deleted_by="user-7",
                dependencies=dependencies,
            )
        )
        await asyncio.sleep(0)
        assert not deletion.done(), "delete passed an in-flight KB writer"

        release_writer.set()
        assert await deletion is True

    await writer
    assert events == ["writer-start", "writer-finish", "vector-delete", "app-delete"]


@pytest.mark.asyncio
async def test_delete_cancels_the_scheduled_reindex_for_that_source_only(
    datasource_id: str,
):
    tasks = KbDatasourceTaskRegistry()
    knowledge_store = _CoordinatedStore([])
    db = MagicMock()
    db.delete_datasource = AsyncMock(return_value=True)
    dependencies = _dependencies(store=db, tasks=tasks)

    async def never_finishes() -> None:
        await asyncio.Event().wait()

    doomed = tasks.schedule(datasource_id, never_finishes(), name="doomed")
    spared = tasks.schedule("other-source", never_finishes(), name="spared")

    with patch(
        "shared.runtime.services.knowledge_store.KnowledgeStore",
        return_value=knowledge_store,
    ):
        assert (
            await knowledge_index.delete_kb_datasource_with_index(
                datasource_id, dependencies=dependencies
            )
            is True
        )

    assert doomed.cancelled()
    assert not spared.done()
    await tasks.drain()


@pytest.mark.asyncio
async def test_delete_reports_a_missing_row_without_skipping_the_purge(
    datasource_id: str,
):
    knowledge_store = _CoordinatedStore([])
    db = MagicMock()
    db.delete_datasource = AsyncMock(return_value=False)
    dependencies = _dependencies(store=db)

    with patch(
        "shared.runtime.services.knowledge_store.KnowledgeStore",
        return_value=knowledge_store,
    ):
        assert (
            await knowledge_index.delete_kb_datasource_with_index(
                datasource_id, dependencies=dependencies
            )
            is False
        )

    knowledge_store.delete_kb_index.assert_awaited_once()


# =============================================================================
# purge_kb_datasource_index — index only, row survives
# =============================================================================


@pytest.mark.asyncio
async def test_purge_drops_the_index_but_never_the_row(datasource_id: str):
    events: list[str] = []
    knowledge_store = _CoordinatedStore(events)
    db = MagicMock()
    db.delete_datasource = AsyncMock()
    tasks = KbDatasourceTaskRegistry()
    dependencies = _dependencies(store=db, tasks=tasks)

    async def never_finishes() -> None:
        await asyncio.Event().wait()

    doomed = tasks.schedule(datasource_id, never_finishes(), name="doomed")

    with patch(
        "shared.runtime.services.knowledge_store.KnowledgeStore",
        return_value=knowledge_store,
    ):
        await knowledge_index.purge_kb_datasource_index(
            datasource_id, dependencies=dependencies
        )

    assert events == ["vector-delete"]
    db.delete_datasource.assert_not_awaited()
    # The purge fences against a straggler for the same reason the delete does.
    assert doomed.cancelled()


@pytest.mark.asyncio
async def test_cancel_kb_datasource_reindexes_never_cancels_the_calling_task(
    datasource_id: str,
):
    """The request task running a delete may itself be registered."""
    tasks = KbDatasourceTaskRegistry()
    dependencies = _dependencies(tasks=tasks)
    sibling_started = asyncio.Event()

    async def sibling() -> None:
        sibling_started.set()
        await asyncio.Event().wait()

    async def deleter() -> str:
        await sibling_started.wait()
        await knowledge_index.cancel_kb_datasource_reindexes(
            datasource_id, dependencies=dependencies
        )
        return "survived"

    doomed = tasks.schedule(datasource_id, sibling(), name="sibling")
    caller = tasks.schedule(datasource_id, deleter(), name="deleter")

    assert await caller == "survived"
    assert doomed.cancelled()


# =============================================================================
# build_kb_embedding_service — no key, no index
# =============================================================================


@pytest.mark.asyncio
async def test_build_kb_embedding_service_returns_none_without_a_resolved_key():
    dependencies = _dependencies(inject=AsyncMock(return_value=None))
    assert (
        await knowledge_index.build_kb_embedding_service(dependencies=dependencies)
        is None
    )


@pytest.mark.asyncio
async def test_build_kb_embedding_service_uses_the_injected_profile():
    async def inject(env: dict) -> None:
        env.update(
            {
                "KB_EMBEDDING_API_KEY": "sk-test",
                "KB_EMBEDDING_MODEL": "embed-1",
                "KB_EMBEDDING_BASE_URL": "https://embed.example",
                "KB_EMBEDDING_PROVIDER": "openai",
                "KB_EMBEDDING_DIMENSIONS": "1024",
                "KB_EMBEDDING_PROFILE_ID": "profile-9",
            }
        )

    dependencies = _dependencies(inject=inject)
    built = MagicMock()
    with patch(
        "shared.runtime.services.embedding_service.EmbeddingService",
        return_value=built,
    ) as factory:
        assert (
            await knowledge_index.build_kb_embedding_service(dependencies=dependencies)
            is built
        )
    assert factory.call_args.kwargs["api_key"] == "sk-test"
    assert factory.call_args.kwargs["expected_dimensions"] == 1024
    assert factory.call_args.kwargs["profile_identity"] == "profile-9"
    assert factory.call_args.kwargs["served_model"] is None


@pytest.mark.asyncio
async def test_build_kb_embedding_service_sends_the_rows_served_model():
    async def inject(env: dict) -> None:
        env.update(
            {
                "KB_EMBEDDING_API_KEY": "sk-test",
                "KB_EMBEDDING_MODEL": "embed-1-eu",
                "KB_EMBEDDING_SERVED_MODEL": "embed-1",
                "KB_EMBEDDING_BASE_URL": "https://embed.example",
                "KB_EMBEDDING_PROVIDER": "openai",
            }
        )

    service = await knowledge_index.build_kb_embedding_service(
        dependencies=_dependencies(inject=inject)
    )
    assert service.catalog_model == "embed-1-eu"
    assert service.model == "embed-1"


@pytest.mark.asyncio
async def test_reindex_project_kb_skips_when_no_embedding_service_resolves():
    db = MagicMock()
    db.mark_knowledge_projections_synced = AsyncMock()
    dependencies = _dependencies(store=db, inject=AsyncMock(return_value=None))

    with patch("orchestrator.services.kb_reindex.reindex_kb", AsyncMock()) as reindex:
        result = await knowledge_index.reindex_project_kb(
            PROJECT_ID, repo_name="vault", branch="main", dependencies=dependencies
        )

    assert result == {"status": "no-embedding-service"}
    reindex.assert_not_awaited()
    db.mark_knowledge_projections_synced.assert_not_awaited()


@pytest.mark.asyncio
async def test_reindex_project_kb_reports_no_repo_before_touching_embeddings():
    inject = AsyncMock(return_value=None)
    dependencies = _dependencies(inject=inject)

    with patch(
        "orchestrator.services.kb_reindex.resolve_kb_repo", AsyncMock(return_value=None)
    ):
        result = await knowledge_index.reindex_project_kb(
            PROJECT_ID, dependencies=dependencies
        )

    assert result == {"status": "no-repo"}
    inject.assert_not_awaited()


@pytest.mark.asyncio
async def test_reindex_project_kb_settles_the_projection_ledger_on_success():
    db = MagicMock()
    db.mark_knowledge_projections_synced = AsyncMock(return_value=3)
    dependencies = _dependencies(store=db)

    async def inject(env: dict) -> None:
        env["KB_EMBEDDING_API_KEY"] = "sk-test"

    dependencies = _dependencies(store=db, inject=inject)
    with (
        patch(
            "shared.runtime.services.embedding_service.EmbeddingService",
            return_value=MagicMock(),
        ),
        patch("shared.runtime.services.knowledge_store.KnowledgeStore", MagicMock()),
        patch(
            "orchestrator.services.kb_reindex.reindex_kb",
            AsyncMock(return_value={"status": "completed", "upserted": 2}),
        ),
    ):
        result = await knowledge_index.reindex_project_kb(
            PROJECT_ID, repo_name="vault", branch="main", dependencies=dependencies
        )

    assert result["status"] == "completed"
    assert result["projection_intents_synced"] == 3


@pytest.mark.asyncio
async def test_reindex_now_marks_the_watermark_failed_when_no_key_resolves(
    datasource_id: str,
):
    events: list[str] = []
    knowledge_store = _CoordinatedStore(events)
    db = MagicMock()
    db.get_datasource = AsyncMock(return_value={"id": datasource_id, "type": "kb"})
    dependencies = _dependencies(store=db, inject=AsyncMock(return_value=None))

    with patch(
        "shared.runtime.services.knowledge_store.KnowledgeStore",
        return_value=knowledge_store,
    ):
        result = await knowledge_index.reindex_kb_datasource_now(
            {"id": datasource_id, "type": "kb", "default_branch": "main"},
            dependencies=dependencies,
        )

    assert result["status"] == "no-embedding-service"
    assert result["errors"] == 1
    status_kwargs = knowledge_store.set_watermark_status.call_args
    assert status_kwargs.args[1] == "failed"
    assert status_kwargs.kwargs["last_error"] == "No embedding service is configured"


@pytest.mark.asyncio
async def test_reindex_now_reports_source_deleted_when_the_row_is_gone(
    datasource_id: str,
):
    knowledge_store = _CoordinatedStore([])
    db = MagicMock()
    db.get_datasource = AsyncMock(return_value=None)
    dependencies = _dependencies(store=db, inject=AsyncMock(return_value=None))

    with patch(
        "shared.runtime.services.knowledge_store.KnowledgeStore",
        return_value=knowledge_store,
    ):
        result = await knowledge_index.reindex_kb_datasource_now(
            {"id": datasource_id, "type": "kb"}, dependencies=dependencies
        )

    assert result["status"] == "source-deleted"
    knowledge_store.set_watermark_status.assert_not_awaited()


@pytest.mark.asyncio
async def test_mark_pending_degrades_instead_of_failing_the_crud_call(
    datasource_id: str,
):
    db = MagicMock()
    db.get_datasource = AsyncMock(side_effect=RuntimeError("vector down"))
    dependencies = _dependencies(store=db)

    with patch(
        "shared.runtime.services.knowledge_store.KnowledgeStore",
        side_effect=RuntimeError("vector down"),
    ):
        await knowledge_index.mark_kb_datasource_pending(
            datasource_id, dependencies=dependencies
        )

    dependencies.logger.warning.assert_called_once()


@pytest.mark.asyncio
async def test_scheduled_reindex_skips_a_native_project_connector(datasource_id: str):
    db = MagicMock()
    db.get_datasource = AsyncMock(
        return_value={
            "id": datasource_id,
            "type": "kb",
            "config": {"native_project_id": PROJECT_ID},
        }
    )
    dependencies = _dependencies(store=db)

    with patch(
        "orchestrator.services.knowledge_index.reindex_kb_datasource_now", AsyncMock()
    ) as now:
        await knowledge_index.run_scheduled_kb_datasource_reindex(
            datasource_id, force_full=True, dependencies=dependencies
        )

    now.assert_not_awaited()
    dependencies.logger.debug.assert_called_once()


@pytest.mark.asyncio
async def test_schedule_registers_through_the_task_registry(datasource_id: str):
    tasks = KbDatasourceTaskRegistry()
    db = MagicMock()
    db.get_datasource = AsyncMock(return_value=None)
    dependencies = _dependencies(store=db, tasks=tasks)

    knowledge_index.schedule_kb_datasource_reindex(
        datasource_id, force_full=True, dependencies=dependencies
    )
    pending = tasks.pending()
    assert len(pending) == 1
    assert pending[0].get_name() == f"kb-datasource-reindex-{datasource_id[:8]}"

    await asyncio.gather(*pending)
