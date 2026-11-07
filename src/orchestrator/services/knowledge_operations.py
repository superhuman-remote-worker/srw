"""Project knowledge-base reads and commands.

The canonical store for a project's knowledge is its KB git repo; the
``knowledge_index`` table in the vector pool is a projection of it and Neo4j is
an optional derived graph. Every mutation here therefore goes through the git
boundary first (``orchestrator.services.kb_materialize``) and only then
projects, which is why the write paths answer 409 ``pending_sync`` rather than
mutating the index behind a failed commit.

Reads degrade: a missing Neo4j costs relationships, not the note.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from fastapi import HTTPException

from orchestrator.schemas.knowledge import (
    KnowledgeDeleteRequest,
    KnowledgeMaterializeRequest,
    KnowledgeNoteUpdate,
    KnowledgeProjectionRequest,
    KnowledgeSearchRequest,
)
from orchestrator.services.knowledge_projection import KnowledgeGraphHandle
from shared.backlog_tags import OFFICER_ONLY_TAGS, normalize_tag

if TYPE_CHECKING:  # pragma: no cover - import cycle-free typing only
    from orchestrator.services.knowledge_index import KnowledgeIndexDependencies


@dataclass(frozen=True)
class KnowledgeOperationDependencies:
    """Collaborators for the project knowledge reads and commands.

    ``store`` is ``postgres_db`` (projection ledger + KB repo resolution),
    ``vector_db`` the pgvector pool holding ``knowledge_index``, and
    ``knowledge_index`` the nested dependency value for
    :mod:`orchestrator.services.knowledge_index`.
    """

    store: Any
    vector_db: Any
    gitea_client: Any
    logger: Any
    graph: KnowledgeGraphHandle
    knowledge_index: KnowledgeIndexDependencies


# =============================================================================
# Reads
# =============================================================================


async def get_knowledge_summary(
    project_id: str, *, dependencies: KnowledgeOperationDependencies
) -> dict[str, Any]:
    """Get knowledge base summary statistics for a project. F5: member-only."""
    try:
        async with dependencies.vector_db.acquire() as conn:
            # Counts by type
            type_rows = await conn.fetch(
                "SELECT note_type, COUNT(*) as cnt FROM knowledge_index "
                "WHERE project_id = $1 GROUP BY note_type ORDER BY cnt DESC",
                project_id,
            )
            by_type = {r["note_type"]: r["cnt"] for r in type_rows}

            # Counts by status
            status_rows = await conn.fetch(
                "SELECT status, COUNT(*) as cnt FROM knowledge_index "
                "WHERE project_id = $1 GROUP BY status ORDER BY cnt DESC",
                project_id,
            )
            by_status = {r["status"]: r["cnt"] for r in status_rows}

            # Total
            total = sum(by_type.values())

            # Recent notes (last 5)
            recent_rows = await conn.fetch(
                "SELECT note_id, title, note_type, status, modified_at "
                "FROM knowledge_index WHERE project_id = $1 "
                "ORDER BY modified_at DESC LIMIT 5",
                project_id,
            )
            recent = [dict(r) for r in recent_rows]

        # kb_gardening G10: hygiene counters beside the raw totals, so the
        # cockpit and MCP summary can show retired-but-on-disk, orphaned
        # nursery, and the oldest tombstone. Best-effort — never fails the
        # summary.
        gardening: dict[str, Any] | None = None
        try:
            from uuid import UUID as _UUID

            from shared.runtime.services.knowledge_store import KnowledgeStore

            gardening = await KnowledgeStore(
                db=dependencies.vector_db, embedding_service=None
            ).get_gardening_health(_UUID(project_id))
        except Exception:
            dependencies.logger.debug(
                "knowledge summary: gardening health unavailable for %s",
                project_id,
                exc_info=True,
            )

        return {
            "total": total,
            "by_type": by_type,
            "by_status": by_status,
            "recent": recent,
            "gardening": gardening,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def list_knowledge_notes(
    project_id: str,
    *,
    note_type: str | None,
    status: str | None,
    tag: str | None,
    job_id: str | None,
    limit: int,
    offset: int,
    dependencies: KnowledgeOperationDependencies,
) -> dict[str, Any]:
    """List knowledge notes for a project with optional filters. F5: member-only."""
    try:
        async with dependencies.vector_db.acquire() as conn:
            conditions = ["project_id = $1"]
            params: list[Any] = [project_id]
            idx = 2

            if note_type:
                conditions.append(f"note_type = ${idx}")
                params.append(note_type)
                idx += 1
            if status:
                conditions.append(f"status = ${idx}")
                params.append(status)
                idx += 1
            if tag:
                conditions.append(f"${idx} = ANY(tags)")
                params.append(tag)
                idx += 1
            if job_id:
                conditions.append(f"job_id = ${idx}::uuid")
                params.append(job_id)
                idx += 1

            where = " AND ".join(conditions)

            # Count total
            count_row = await conn.fetchrow(
                f"SELECT COUNT(*) as cnt FROM knowledge_index WHERE {where}",
                *params,
            )
            total = count_row["cnt"] if count_row else 0

            # Fetch page
            params.extend([limit, offset])
            rows = await conn.fetch(
                f"SELECT id, note_id, title, note_type, status, confidence, "
                f"tags, keywords, job_id, phase, "
                f"LEFT(content, 300) as content_preview, "
                f"created_at, modified_at "
                f"FROM knowledge_index WHERE {where} "
                f"ORDER BY modified_at DESC "
                f"LIMIT ${idx} OFFSET ${idx + 1}",
                *params,
            )
            notes = [dict(r) for r in rows]

        return {"notes": notes, "total": total, "limit": limit, "offset": offset}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def get_knowledge_note(
    project_id: str, note_id: str, *, dependencies: KnowledgeOperationDependencies
) -> dict[str, Any]:
    """Get a single knowledge note with full content. F5: member-only."""
    try:
        async with dependencies.vector_db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM knowledge_index WHERE project_id = $1 AND note_id = $2",
                project_id,
                note_id,
            )
        if not row:
            raise HTTPException(
                status_code=404,
                detail=f"Note '{note_id}' not found in project '{project_id}'",
            )
        result = dict(row)
        # Remove binary/vector fields from response
        result.pop("embedding", None)
        result.pop("search_doc", None)

        # Fetch relationships from Neo4j if available
        kg = dependencies.graph.get()
        if kg:
            try:
                neo4j_note = kg.read_note(project_id, note_id)
                if neo4j_note:
                    result["relationships"] = neo4j_note.get("relationships", [])
            except Exception:
                result["relationships"] = []
        else:
            result["relationships"] = []

        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


# =============================================================================
# Commands
# =============================================================================


async def reindex_knowledge(
    project_id: str, *, full: bool, dependencies: KnowledgeOperationDependencies
) -> dict[str, Any]:
    """Rebuild/refresh the KB chunk index from the vault repo (slice 3 PR3).

    The ``kb reindex --full`` operator hatch (§5): ``full=true`` re-embeds the
    whole vault (model/chunker migration, corrupt-index recovery); the default
    incremental run only touches notes whose git blob changed. F5: member-only,
    same gate as the sibling knowledge endpoints.
    """
    from orchestrator.services.knowledge_index import reindex_project_kb

    try:
        return await reindex_project_kb(
            project_id, force_full=full, dependencies=dependencies.knowledge_index
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def materialize_knowledge_note(
    project_id: str,
    body: KnowledgeMaterializeRequest,
    *,
    dependencies: KnowledgeOperationDependencies,
) -> dict[str, Any]:
    """Commit one rendered note to ``knowledge/<slug>.md`` in the project's KB
    repo. **Internal** (P4b) — requires ``X-Internal-Key``.

    Step 3 of knowledge-base/knowledge/features/knowledge_base_repo_separation.md: the agent used
    to write this file into its workspace checkout, which welded the vault to
    the jobs repo and skipped entirely wherever there was no git (persistent
    sessions, lite tiers, repo-less projects). The write is server-side now —
    one note, one commit, into whichever repo ``resolve_kb_repo`` picks.

    Always answers 200 with the service's ``{status, reason, repo, branch,
    path, operation, indexed, index_reason}``; the failure vocabulary lives in
    the body, not the HTTP code. ``indexed`` reports whether the note is
    searchable already — ``false`` with an ``index_reason`` means the commit
    landed and the sweep will finish the job.
    """
    from orchestrator.services.kb_materialize import (
        materialize_knowledge_note as _materialize_note,
    )

    # Slice A: hand the materialiser an indexer so the note is searchable when
    # this call returns. Both legs are optional by design — a deployment with
    # no resolvable embedding service commits the note and lets the sweep
    # index it, which is exactly the pre-Slice-A behaviour.
    store = None
    svc = None
    try:
        from orchestrator.services.knowledge_index import build_kb_embedding_service

        svc = await build_kb_embedding_service(
            dependencies=dependencies.knowledge_index
        )
        if svc is not None:
            from shared.runtime.services.knowledge_store import KnowledgeStore

            store = KnowledgeStore(db=dependencies.vector_db, embedding_service=svc)
        else:
            # INFO, not WARNING: a deployment with no system embedding is a
            # valid degraded configuration, not a fault. But it silently
            # disables inline indexing for EVERY write in the deployment, so
            # it needs one log line an operator can find — without it the
            # only evidence is `indexed=deferred:no-indexer` in a transcript.
            dependencies.logger.info(
                "kb-materialize: no embedding service resolvable for project "
                "%s — the note will commit and be indexed by the next sweep "
                "(check the system embedding in Admin → Models)",
                project_id,
            )
    except Exception:
        dependencies.logger.warning(
            "kb-materialize: no inline indexer for project %s — the note will "
            "commit and be indexed by the next sweep",
            project_id,
            exc_info=True,
        )
        store = None
        svc = None

    return await _materialize_note(
        postgres_db=dependencies.store,
        gitea_client=dependencies.gitea_client,
        project_id=project_id,
        slug=body.slug,
        content=body.content,
        job_id=body.job_id,
        store=store,
        embedding_service=svc,
        retrieval_messages=body.retrieval_messages,
        expected_blob_sha=body.expected_blob_sha,
        mode=body.mode,
    )


async def record_knowledge_projection(
    project_id: str,
    intent_id: str,
    body: KnowledgeProjectionRequest,
    *,
    dependencies: KnowledgeOperationDependencies,
) -> dict[str, Any]:
    """Record projection truth after a canonical internal knowledge write."""
    result = await dependencies.store.finish_knowledge_projection(
        intent_id,
        project_id=project_id,
        synced=body.synced,
        error=body.error,
    )
    if result is None:
        raise HTTPException(status_code=404, detail="canonical intent not found")
    return result


async def search_knowledge(
    project_id: str,
    body: KnowledgeSearchRequest,
    *,
    dependencies: KnowledgeOperationDependencies,
) -> dict[str, Any]:
    """Hybrid search over project knowledge base. F5: member-only."""
    try:
        async with dependencies.vector_db.acquire() as conn:
            # Try dense+sparse search if embedding service available
            embedding = None
            try:
                from shared.runtime.services.embedding_service import (
                    get_embedding_service,
                )

                svc = get_embedding_service()
                embedding = await svc.embed(body.query)
            except Exception:
                pass  # Fall back to sparse-only search

            if embedding:
                rows = await conn.fetch(
                    "SELECT * FROM knowledge_hybrid_search($1, $2::vector, $3, $4)",
                    body.query,
                    str(embedding),
                    project_id,
                    body.limit,
                )
            else:
                # Sparse-only fallback: tsvector keyword search
                rows = await conn.fetch(
                    "SELECT * FROM knowledge_index "
                    "WHERE project_id = $1 AND search_doc @@ websearch_to_tsquery($2) "
                    "ORDER BY ts_rank_cd(search_doc, websearch_to_tsquery($2)) DESC "
                    "LIMIT $3",
                    project_id,
                    body.query,
                    body.limit,
                )

            notes = []
            for r in rows:
                d = dict(r)
                d.pop("embedding", None)
                d.pop("search_doc", None)
                notes.append(d)

        return {"notes": notes, "query": body.query, "total": len(notes)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def update_knowledge_note(
    project_id: str,
    note_id: str,
    body: KnowledgeNoteUpdate,
    *,
    dependencies: KnowledgeOperationDependencies,
    owner_authority: Callable[[], Awaitable[bool]] | None = None,
) -> dict[str, Any]:
    """Update a knowledge note's status or tags (the router requires editor).

    ``owner_authority`` answers whether the caller holds the project's owner
    authority (owner or admin). It is asked only when the request adds or
    removes an officer-only tag or targets a charter note; without it, both
    are refused (403). ``ready`` arms backlog dispatch and the charter steers
    the project's officer, so neither is an editor's call (D9).
    """
    valid_statuses = {"active", "resolved", "superseded", "archived"}
    if body.status and body.status not in valid_statuses:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid status '{body.status}'. Must be one of: {valid_statuses}",
        )

    if not (body.status or body.add_tags or body.remove_tags):
        return {"status": "no_changes"}

    async def _has_owner_authority() -> bool:
        return owner_authority is not None and bool(await owner_authority())

    requested_tags = {
        normalize_tag(str(tag))
        for tag in [*(body.add_tags or []), *(body.remove_tags or [])]
    }
    officer_tags = sorted(requested_tags & OFFICER_ONLY_TAGS)
    if officer_tags and not await _has_owner_authority():
        raise HTTPException(
            status_code=403,
            detail=(
                f"Only the project owner can add or remove the {', '.join(officer_tags)} "
                "tag: it controls backlog dispatch"
            ),
        )

    materialization: dict[str, Any] | None = None
    try:
        # The index proves the public note exists, but is never mutated before
        # the canonical git boundary succeeds.
        async with dependencies.vector_db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT note_id, note_type, path FROM knowledge_index "
                "WHERE project_id = $1 AND note_id = $2 "
                "ORDER BY path IS NULL LIMIT 1",
                project_id,
                note_id,
            )
            if not row:
                raise HTTPException(
                    status_code=404,
                    detail=f"Note '{note_id}' not found in project '{project_id}'",
                )
        if row.get("note_type") == "charter" and not await _has_owner_authority():
            raise HTTPException(
                status_code=403,
                detail="Only the project owner can change the charter note",
            )

        from orchestrator.services.kb_materialize import (
            materialize_knowledge_metadata_update,
        )

        materialization = await materialize_knowledge_metadata_update(
            postgres_db=dependencies.store,
            gitea_client=dependencies.gitea_client,
            project_id=project_id,
            slug=note_id,
            status=body.status,
            add_tags=body.add_tags,
            remove_tags=body.remove_tags,
            # Where the row says the file is: a nested note, or one whose id
            # is not its filename, is changed in place (G14).
            path_hint=row.get("path"),
        )
        if materialization.get("canonical_state") != "canonical":
            raise HTTPException(
                status_code=409,
                detail={
                    "status": "pending_sync",
                    "reason": materialization.get("reason"),
                    "retry_state": materialization.get("retry_state"),
                    "intent_id": materialization.get("intent_id"),
                },
            )

        required_metadata = []
        if body.status:
            required_metadata.append("canonical_status")
        if body.add_tags or body.remove_tags:
            required_metadata.extend(("canonical_tags", "canonical_ready_at"))
        if materialization.get("canonical_metadata_complete") is not True or any(
            field not in materialization for field in required_metadata
        ):
            raise HTTPException(
                status_code=409,
                detail={
                    "status": "pending_sync",
                    "reason": materialization.get("reason")
                    or "canonical-metadata-unavailable",
                    "retry_state": materialization.get("retry_state"),
                    "intent_id": materialization.get("intent_id"),
                },
            )

        async with dependencies.vector_db.acquire() as conn:
            updates = []
            params: list[Any] = [project_id, note_id]
            idx = 3

            if body.status:
                canonical_status = materialization["canonical_status"]
                if canonical_status is None:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "status": "pending_sync",
                            "reason": "canonical-status-unavailable",
                            "retry_state": materialization.get("retry_state"),
                            "intent_id": materialization.get("intent_id"),
                        },
                    )
                updates.append(f"status = ${idx}")
                params.append(str(canonical_status))
                idx += 1

            if body.add_tags or body.remove_tags:
                canonical_tags = materialization["canonical_tags"]
                if not isinstance(canonical_tags, list):
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "status": "pending_sync",
                            "reason": "canonical-tags-invalid",
                            "retry_state": materialization.get("retry_state"),
                            "intent_id": materialization.get("intent_id"),
                        },
                    )
                raw_ready_at = materialization["canonical_ready_at"]
                if isinstance(raw_ready_at, datetime):
                    canonical_ready_at = raw_ready_at
                elif raw_ready_at is None:
                    canonical_ready_at = None
                else:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "status": "pending_sync",
                            "reason": "canonical-ready-at-invalid",
                            "retry_state": materialization.get("retry_state"),
                            "intent_id": materialization.get("intent_id"),
                        },
                    )
                if canonical_ready_at is not None and canonical_ready_at.tzinfo is None:
                    canonical_ready_at = canonical_ready_at.replace(tzinfo=timezone.utc)
                updates.append(f"tags = ${idx}::text[]")
                params.append([str(tag) for tag in canonical_tags])
                idx += 1
                updates.append(f"ready_at = ${idx}::timestamptz")
                params.append(canonical_ready_at)
                idx += 1

            updates.append("modified_at = NOW()")
            set_clause = ", ".join(updates)
            projected = await conn.fetchval(
                f"UPDATE knowledge_index SET {set_clause} "
                f"WHERE project_id = $1 AND note_id = $2 RETURNING note_id",
                *params,
            )
            if projected is None:
                raise RuntimeError("knowledge projection row disappeared")

        # Neo4j is an optional derived graph. The durable projection leg is the
        # pgvector index above because the canonical Git reindexer can rebuild
        # it. Do not leave the intent permanently pending for an optional graph
        # outage the reindexer cannot repair.
        kg = dependencies.graph.get()
        if kg:
            try:
                update_kwargs: dict[str, Any] = {}
                if body.status:
                    update_kwargs["status"] = str(materialization["canonical_status"])
                if body.add_tags:
                    update_kwargs["add_tags"] = body.add_tags
                if body.remove_tags:
                    update_kwargs["remove_tags"] = body.remove_tags
                # Only agent-written notes have a graph copy (the reindexer
                # never writes Neo4j), so a miss is the normal case for a
                # hand-written note, not a failure.
                if update_kwargs and not kg.update_note(
                    project_id, note_id, **update_kwargs
                ):
                    dependencies.logger.debug(
                        "Neo4j has no copy of %s; nothing to update", note_id
                    )
            except Exception as e:
                dependencies.logger.warning(
                    "Neo4j update failed for %s: %s", note_id, e
                )

        projection = await dependencies.store.finish_knowledge_projection(
            str(materialization["intent_id"]),
            project_id=project_id,
            synced=True,
        )
        if projection is None:
            raise RuntimeError("knowledge projection ledger did not converge")

        return {
            "status": "updated",
            "canonical_state": "canonical",
            "projection_state": "synced",
            "intent_id": materialization.get("intent_id"),
        }
    except HTTPException:
        raise
    except Exception as e:
        if materialization and materialization.get("intent_id"):
            try:
                await dependencies.store.finish_knowledge_projection(
                    str(materialization["intent_id"]),
                    project_id=project_id,
                    synced=False,
                    error=str(e),
                )
            except Exception:
                dependencies.logger.exception(
                    "failed to persist knowledge projection failure"
                )
        raise HTTPException(status_code=500, detail=str(e)) from e


async def _indexed_note_path(
    dependencies: KnowledgeOperationDependencies, project_id: str, note_id: str
) -> str | None:
    """The file the index says holds the note, or None (best-effort).

    Lets the purge find a nested note, or one whose id is not its filename;
    without it the delete falls back to locating the file by name.
    """
    try:
        async with dependencies.vector_db.acquire() as conn:
            path = await conn.fetchval(
                "SELECT path FROM knowledge_index "
                "WHERE project_id = $1 AND note_id = $2 AND path IS NOT NULL "
                "LIMIT 1",
                project_id,
                note_id,
            )
    except Exception:  # noqa: BLE001 — the resolver still finds it by name
        dependencies.logger.debug(
            "indexed path lookup failed for %s/%s", project_id, note_id, exc_info=True
        )
        return None
    return str(path) if path else None


async def delete_knowledge_note(
    project_id: str, note_id: str, *, dependencies: KnowledgeOperationDependencies
) -> dict[str, Any]:
    """Purge a knowledge note: file-removal commit, then both index stores.

    F5: member-only. A human's delete is immediate and unconditional (no
    compare-and-swap token) — that authority is the point of the cockpit
    button. Before kb_gardening G2 this removed only the index row, and the
    next sweep resurrected the note from the file it never touched.
    """
    from orchestrator.services.kb_materialize import materialize_knowledge_note_delete
    from shared.runtime.services.knowledge_store import KnowledgeStore

    store = KnowledgeStore(db=dependencies.vector_db, embedding_service=None)
    path_hint = await _indexed_note_path(dependencies, project_id, note_id)
    try:
        result = await materialize_knowledge_note_delete(
            postgres_db=dependencies.store,
            gitea_client=dependencies.gitea_client,
            project_id=project_id,
            slug=note_id,
            reason="deleted from the cockpit",
            store=store,
            path_hint=path_hint,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e

    status, reason = result.get("status"), result.get("reason")
    if status == "failed":
        raise HTTPException(
            status_code=409 if reason == "precondition-failed" else 502,
            detail=f"Note '{note_id}' was not removed from the knowledge repo ({reason}).",
        )
    if status == "skipped" and reason not in {"absent", "already-canonical", "no-repo"}:
        raise HTTPException(
            status_code=409,
            detail=f"Note '{note_id}' delete is still in progress ({reason}).",
        )
    file_removed = status == "committed"
    row_deleted = bool(result.get("row_deleted"))
    if not file_removed and not row_deleted and reason in {"absent", "no-repo"}:
        raise HTTPException(
            status_code=404,
            detail=f"Note '{note_id}' not found in project '{project_id}'",
        )

    # Delete from Neo4j if available (optional projection; best-effort).
    kg = dependencies.graph.get()
    if kg:
        try:
            kg._db.execute_write(
                "MATCH (n:Note {project_id: $pid, id: $nid}) DETACH DELETE n",
                {"pid": project_id, "nid": note_id},
            )
        except Exception as e:
            dependencies.logger.warning(f"Neo4j delete failed for {note_id}: {e}")

    return {
        "status": "deleted",
        "file_removed": file_removed,
        "row_deleted": row_deleted,
        "path": result.get("path"),
    }


async def delete_knowledge_note_internal(
    project_id: str,
    body: KnowledgeDeleteRequest,
    *,
    dependencies: KnowledgeOperationDependencies,
) -> dict[str, Any]:
    """Purge one note from the KB repo and its index. **Internal** — requires
    ``X-Internal-Key``. Same result vocabulary as the materialize endpoint
    (HTTP 200 always; ``status``/``reason`` carry the outcome) plus
    ``row_deleted``. Agents do not call this directly — ``kb_delete`` is a
    tombstone — it exists for the purge lane and for operator tooling.
    """
    from orchestrator.services.kb_materialize import materialize_knowledge_note_delete
    from shared.runtime.services.knowledge_store import KnowledgeStore

    store = KnowledgeStore(db=dependencies.vector_db, embedding_service=None)
    return await materialize_knowledge_note_delete(
        postgres_db=dependencies.store,
        gitea_client=dependencies.gitea_client,
        project_id=project_id,
        slug=body.slug,
        job_id=body.job_id,
        reason=body.reason,
        expected_blob_sha=body.expected_blob_sha,
        store=store,
    )


async def export_knowledge(
    project_id: str,
    project: dict[str, Any],
    *,
    dependencies: KnowledgeOperationDependencies,
) -> dict[str, Any]:
    """Export project knowledge base as Obsidian-compatible markdown files.

    F5: member-only. Same access requirement as the per-note read endpoint
    — the bulk export is equivalent to scraping the list and getting each
    note individually, so a tighter gate wouldn't close a real gap.
    """
    kg = dependencies.graph.get()
    if not kg:
        raise HTTPException(
            status_code=503,
            detail="Neo4j not available — cannot export knowledge base",
        )

    try:
        import tempfile

        export_dir = Path(tempfile.mkdtemp(prefix="kb_export_"))
        notes = kg.get_all_notes_for_export(project_id)

        for note in notes:
            # Build frontmatter
            fm_lines = ["---"]
            fm_lines.append(f"id: {note['id']}")
            fm_lines.append(f"type: {note['type']}")
            if note.get("tags"):
                fm_lines.append(f"tags: [{', '.join(note['tags'])}]")
            if note.get("keywords"):
                fm_lines.append(f"keywords: [{', '.join(note['keywords'])}]")
            if note.get("confidence"):
                fm_lines.append(f"confidence: {note['confidence']}")
            fm_lines.append(f"status: {note.get('status', 'active')}")
            if note.get("job_id"):
                fm_lines.append(f"job_id: {note['job_id']}")
            if note.get("phase"):
                fm_lines.append(f"phase: {note['phase']}")
            if note.get("created"):
                fm_lines.append(f"created: {note['created']}")
            if note.get("modified"):
                fm_lines.append(f"modified: {note['modified']}")
            fm_lines.append("---")
            fm_lines.append("")

            # Title and content
            fm_lines.append(f"# {note.get('title', note['id'])}")
            fm_lines.append("")
            if note.get("content"):
                fm_lines.append(note["content"])
                fm_lines.append("")

            # Relationships as wikilinks
            if note.get("relationships"):
                by_type: dict[str, list[str]] = {}
                for rel in note["relationships"]:
                    rtype = rel.get("type", "REFERENCES")
                    target = rel.get("target", "")
                    by_type.setdefault(rtype, []).append(target)
                for rtype, targets in by_type.items():
                    links = ", ".join(f"[[{t}]]" for t in targets)
                    fm_lines.append(f"**{rtype}:** {links}")

            file_name = f"{note['id']}.md"
            (export_dir / file_name).write_text("\n".join(fm_lines), encoding="utf-8")

        return {
            "status": "exported",
            "path": str(export_dir),
            "note_count": len(notes),
            "project_name": project.get("name", ""),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
