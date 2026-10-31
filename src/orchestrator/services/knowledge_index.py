"""KB chunk-index lifecycle: build, schedule, cancel, purge, delete, reindex.

Extracted verbatim from ``orchestrator.main`` (R1.B03 lane D). Two orderings
live here and neither is incidental:

* the **delete-before-late-write fence** — the app and vector schemas are
  separate databases, so the shared per-KB advisory claim (``kb_index_lock``)
  is what orders the vector purge against the app-row delete;
* the **cancel-then-claim** sequence — an in-flight rebuild is cancelled
  before the claim is taken, so a straggler cannot write chunks back after
  the index is dropped.

Every collaborator arrives through :class:`KnowledgeIndexDependencies`, built
per invocation by the application, because ``postgres_db``/``vector_db``/
``gitea_client`` are rebound during ``lifespan``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from orchestrator.services.kb_task_registry import KbDatasourceTaskRegistry


@dataclass(frozen=True)
class KnowledgeIndexDependencies:
    """Collaborators for one KB index operation, resolved per invocation."""

    store: Any
    vector_db: Any
    gitea_client: Any
    logger: Any
    tasks: KbDatasourceTaskRegistry
    inject_system_kb_embedding_profile: Callable[[dict[str, Any]], Awaitable[Any]]


async def build_kb_embedding_service(
    *, dependencies: KnowledgeIndexDependencies
) -> Any | None:
    """EmbeddingService for the KB reindexer, catalog-first (slice 3 PR3).

    The orchestrator pod carries no EMBEDDING_* env (models_yaml_removal), so
    the credential comes from the same place dispatch gets it for agents: the
    admin-curated system embedding in the catalog
    (``resolve_default_for_capability`` + ``_inject_env_key_credentials``).
    Env-based construction is the fallback for dev/compose stacks that still
    set EMBEDDING_*. Returns ``None`` when no usable key resolves — the caller
    must skip reindexing (a keyless run could only write vectorless rows).
    """
    from shared.runtime.core.transport_resolution import served_model_from_env
    from shared.runtime.services.embedding_service import EmbeddingService

    env: dict[str, Any] = {}
    await dependencies.inject_system_kb_embedding_profile(env)
    if env.get("KB_EMBEDDING_API_KEY"):
        return EmbeddingService(
            model=env.get("KB_EMBEDDING_MODEL"),
            served_model=served_model_from_env("KB_EMBEDDING", env),
            base_url=env.get("KB_EMBEDDING_BASE_URL"),
            api_key=env.get("KB_EMBEDDING_API_KEY"),
            provider=env.get("KB_EMBEDDING_PROVIDER"),
            expected_dimensions=int(env.get("KB_EMBEDDING_DIMENSIONS", "4096")),
            profile_identity=env.get("KB_EMBEDDING_PROFILE_ID"),
        )
    # `_inject_system_kb_embedding_profile` already materializes the dev/env
    # fallback when (and only when) there is no catalog pin. Reaching here means
    # either no embedding transport exists or a selected catalog profile is
    # incomplete. Never silently index with a different EMBEDDING_* model: the
    # dispatched KB_* profile would then query incompatible vectors.
    return None


async def mark_kb_datasource_pending(
    datasource_id: str, *, dependencies: KnowledgeIndexDependencies
) -> None:
    """Best-effort initial/update state; datasource CRUD remains available."""
    try:
        from shared.runtime.services.knowledge_store import KnowledgeStore

        from orchestrator.services.kb_reindex import kb_index_lock

        kb_id = UUID(datasource_id)
        store = KnowledgeStore(db=dependencies.vector_db, embedding_service=None)
        async with kb_index_lock(store, kb_id, wait=True) as lock_conn:
            if await kb_datasource_is_active(datasource_id, dependencies=dependencies):
                await store.set_watermark_status(
                    kb_id,
                    "pending",
                    repo_name=f"datasource:{datasource_id}",
                    conn=lock_conn,
                )
    except Exception as exc:
        dependencies.logger.warning(
            "Could not mark KB datasource %s pending: %s", datasource_id, exc
        )


async def kb_datasource_is_active(
    datasource_id: str, *, dependencies: KnowledgeIndexDependencies
) -> bool:
    """Re-check external-source ownership at the index mutation boundary."""
    datasource = await dependencies.store.get_datasource(datasource_id)
    return bool(datasource and datasource.get("type") == "kb")


async def reindex_kb_datasource_now(
    datasource: dict[str, Any],
    *,
    force_full: bool = False,
    dependencies: KnowledgeIndexDependencies,
) -> dict[str, Any]:
    """Resolve embeddings and run one credential-contained external rebuild."""
    from shared.runtime.services.knowledge_store import KnowledgeStore

    from orchestrator.services.kb_datasources import reindex_kb_datasource
    from orchestrator.services.kb_reindex import kb_index_lock

    datasource_id = str(datasource["id"])
    kb_id = UUID(datasource_id)
    store = KnowledgeStore(db=dependencies.vector_db, embedding_service=None)
    svc = await build_kb_embedding_service(dependencies=dependencies)
    if svc is None:
        indexed_commit: str | None = None
        async with kb_index_lock(store, kb_id) as claimed:
            if not claimed:
                return {
                    "status": "already-indexing",
                    "indexed_commit": None,
                    "full": False,
                    "upserted": 0,
                    "deleted": 0,
                    "skipped": 0,
                    "errors": 0,
                }
            if not await kb_datasource_is_active(
                datasource_id, dependencies=dependencies
            ):
                return {
                    "status": "source-deleted",
                    "indexed_commit": None,
                    "full": False,
                    "upserted": 0,
                    "deleted": 0,
                    "skipped": 0,
                    "errors": 0,
                }
            watermark = await store.get_watermark(kb_id)
            indexed_commit = watermark.indexed_commit if watermark else None
            await store.set_watermark_status(
                kb_id,
                "failed",
                repo_name=f"datasource:{datasource_id}",
                branch=datasource.get("default_branch") or None,
                last_error="No embedding service is configured",
            )
        return {
            "status": "no-embedding-service",
            "indexed_commit": indexed_commit,
            "full": False,
            "upserted": 0,
            "deleted": 0,
            "skipped": 0,
            "errors": 1,
        }
    store.embedding_service = svc
    return await reindex_kb_datasource(
        datasource,
        store=store,
        embedding_service=svc,
        force_full=force_full,
        is_active=lambda: kb_datasource_is_active(
            datasource_id, dependencies=dependencies
        ),
    )


async def run_scheduled_kb_datasource_reindex(
    datasource_id: str,
    *,
    force_full: bool,
    dependencies: KnowledgeIndexDependencies,
) -> None:
    from orchestrator.services.kb_datasources import native_kb_project_id

    try:
        datasource = await dependencies.store.get_datasource(datasource_id)
        if not datasource or datasource.get("type") != "kb":
            return
        if native_kb_project_id(datasource):
            # Its project's sweep already indexes these notes under the
            # project id; a second pass here would duplicate every one of them.
            dependencies.logger.debug(
                "Skipping external reindex of native project KB datasource %s",
                datasource_id,
            )
            return
        await reindex_kb_datasource_now(
            datasource, force_full=force_full, dependencies=dependencies
        )
    except Exception as exc:
        # Source exceptions are credential-redacted by kb_git_source. Store a
        # bounded diagnostic while retaining the previous indexed_commit.
        dependencies.logger.warning(
            "KB datasource %s reindex failed: %s", datasource_id, exc
        )
        try:
            from shared.runtime.services.knowledge_store import KnowledgeStore

            from orchestrator.services.kb_reindex import kb_index_lock

            kb_id = UUID(datasource_id)
            store = KnowledgeStore(db=dependencies.vector_db, embedding_service=None)
            async with kb_index_lock(store, kb_id) as claimed:
                if claimed and await kb_datasource_is_active(
                    datasource_id, dependencies=dependencies
                ):
                    await store.set_watermark_status(
                        kb_id,
                        "failed",
                        repo_name=f"datasource:{datasource_id}",
                        last_error=(str(exc)[-2000:] or "Indexing failed"),
                    )
        except Exception as status_exc:
            dependencies.logger.warning(
                "Could not persist KB datasource %s failure status: %s",
                datasource_id,
                status_exc,
            )


def schedule_kb_datasource_reindex(
    datasource_id: str, *, force_full: bool, dependencies: KnowledgeIndexDependencies
) -> None:
    dependencies.tasks.schedule(
        datasource_id,
        run_scheduled_kb_datasource_reindex(
            datasource_id, force_full=force_full, dependencies=dependencies
        ),
        name=f"kb-datasource-reindex-{datasource_id[:8]}",
    )


async def cancel_kb_datasource_reindexes(
    datasource_id: str, *, dependencies: KnowledgeIndexDependencies
) -> None:
    """Cancel and drain request-spawned rebuilds for one external source."""
    await dependencies.tasks.cancel_for_datasource(datasource_id)


async def purge_kb_datasource_index(
    datasource_id: str, *, dependencies: KnowledgeIndexDependencies
) -> None:
    """Drop the chunk index a connector accumulated under its own UUID.

    Deliberately not shared with ``delete_kb_datasource_with_index``: that one
    holds the claim across the index purge *and* the app-row delete to order
    the two databases. Here the row survives — it has already been marked as a
    project's own KB — so only the disposable index is dropped, under the same
    per-KB claim so an in-flight sweeper cannot write chunks back afterwards.
    """
    from shared.runtime.services.knowledge_store import KnowledgeStore

    from orchestrator.services.kb_reindex import kb_index_lock

    await cancel_kb_datasource_reindexes(datasource_id, dependencies=dependencies)
    kb_id = UUID(datasource_id)
    store = KnowledgeStore(db=dependencies.vector_db, embedding_service=None)
    async with kb_index_lock(store, kb_id, wait=True) as lock_conn:
        await store.delete_kb_index(kb_id, conn=lock_conn)


async def delete_kb_datasource_with_index(
    datasource_id: str,
    *,
    authority_project_scope_id: str | None = None,
    deleted_by: str | None = None,
    dependencies: KnowledgeIndexDependencies,
) -> bool:
    """Order datasource deletion after every writer of its disposable index.

    The app and vector schemas live in separate databases, so this cannot be a
    single SQL transaction. Holding the shared per-KB advisory claim across
    vector cleanup and app-row deletion supplies the required ordering. A
    stale sweeper that captured the row earlier subsequently fails its
    under-lock liveness check and cannot recreate notes or a watermark.

    ``deleted_by`` is passed straight through to the app-row delete's
    tombstone write (Task 12 item C) — this is the KB half of the same
    endpoint the non-kb branch already attributes, not a separate decision.
    """
    from shared.runtime.services.knowledge_store import KnowledgeStore

    from orchestrator.services.kb_reindex import kb_index_lock

    await cancel_kb_datasource_reindexes(datasource_id, dependencies=dependencies)
    kb_id = UUID(datasource_id)
    store = KnowledgeStore(db=dependencies.vector_db, embedding_service=None)
    async with kb_index_lock(store, kb_id, wait=True) as lock_conn:
        await store.delete_kb_index(kb_id, conn=lock_conn)
        return await dependencies.store.delete_datasource(
            datasource_id,
            authority_project_scope_id=authority_project_scope_id,
            deleted_by=deleted_by,
        )


async def reindex_project_kb(
    project_id: str,
    *,
    repo_name: str | None = None,
    branch: str | None = None,
    force_full: bool = False,
    dependencies: KnowledgeIndexDependencies,
) -> dict[str, Any]:
    """Bring a project KB's chunk index up to its repo HEAD (slice 3 PR3).

    Shared entry for the post-write trigger and the operator reindex endpoint.
    Resolves the dedicated knowledge vault (with a legacy jobs-role fallback)
    when not supplied, builds the
    catalog-resolved embedding service, and runs the tree-diff reindex. Returns
    the reindex summary dict; ``status`` carries the honesty signal
    (no-repo / no-embedding-service / up-to-date / completed / partial / ...).
    """
    from shared.runtime.services.knowledge_store import KnowledgeStore

    from orchestrator.services.kb_forge import kb_client_for_repo
    from orchestrator.services.kb_reindex import reindex_kb, resolve_kb_repo

    repo_client = dependencies.gitea_client
    if not repo_name:
        resolved = await resolve_kb_repo(dependencies.store, project_id)
        if not resolved:
            return {"status": "no-repo"}
        repo_name, branch = resolved.repo, resolved.branch
        repo_client = await kb_client_for_repo(
            dependencies.store, dependencies.gitea_client, resolved
        )
    svc = await build_kb_embedding_service(dependencies=dependencies)
    if svc is None:
        dependencies.logger.warning(
            "kb_reindex: no embedding service resolvable for project %s — "
            "skipping (check the system embedding in Admin → Models)",
            project_id,
        )
        return {"status": "no-embedding-service"}
    store = KnowledgeStore(db=dependencies.vector_db, embedding_service=svc)
    result = await reindex_kb(
        gitea_client=repo_client,
        store=store,
        embedding_service=svc,
        kb_id=UUID(project_id),
        repo_name=repo_name,
        branch=branch or "main",
        force_full=force_full,
    )
    if result.get("status") in {"completed", "up-to-date"}:
        # A successful rebuild is also the recovery path for a canonical Git
        # mutation whose direct projection failed.  The scheduled sweep
        # already settles this ledger; manual/post-write reindex must expose
        # the same truth rather than leaving an eligibility-blocking intent.
        result = dict(result)
        result[
            "projection_intents_synced"
        ] = await dependencies.store.mark_knowledge_projections_synced(project_id)
    return result
