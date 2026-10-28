"""Citation and source-library reads over the vector pool.

The library lives in the vector database (``sources``, ``citations``,
``job_sources``, ``source_annotations``, ``source_tags``, ``memories``) while
job/thread ownership lives in the app database, so nothing here can decide
visibility on its own: every visibility rule arrives as an ``authorize``
callback the router builds from its injected gate, and the query flow calls it
at exactly the point main did — after the row is read, before it is returned.

Citation existence is answered with 404 rather than 403 on purpose: a probe
must not learn that a citation exists in a job it cannot see.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import quote

from fastapi import HTTPException, Response

from orchestrator.services.cloud.identity import resolve_user_identity_cached


@dataclass(frozen=True)
class CitationDependencies:
    """Collaborators for the citation/source/memory reads.

    ``store`` is ``postgres_db`` (cloud-identity cache on the drift path);
    ``vector_db`` is the pool every query here runs against.
    """

    store: Any
    vector_db: Any
    snapshot_service: Any
    main_cloud_router: Any
    logger: Any


def _source_cloud_meta(metadata: Any) -> dict[str, Any]:
    """Extract the ``metadata.cloud`` block from a ``sources.metadata`` value.

    The vector pool may hand back JSONB as a dict (codec) or a JSON string;
    coerce both, and return ``{}`` when there's no cloud snapshot-anchor.
    """
    if isinstance(metadata, (str, bytes)):
        try:
            metadata = json.loads(metadata)
        except (ValueError, TypeError):
            return {}
    if not isinstance(metadata, dict):
        return {}
    cloud = metadata.get("cloud")
    return cloud if isinstance(cloud, dict) else {}


#: Longest snapshot download name kept, before encoding.
_SNAPSHOT_FILENAME_MAX = 200


def _snapshot_content_disposition(source_name: Any, citation_id: int) -> str:
    """``attachment`` naming the cited file, safe for any stored source name.

    The source name is agent-written. Only its last path segment is kept,
    without control characters. The quoted ``filename`` is an ASCII fallback
    (``"``, backslash and non-ASCII become ``_``) and ``filename*`` carries
    the exact name percent-encoded (RFC 6266), so no name can end the header
    value or fail its latin-1 encoding.
    """
    name = str(source_name or "").replace("\\", "/").rsplit("/", 1)[-1]
    name = "".join(ch for ch in name if ch.isprintable()).strip()
    name = name[:_SNAPSHOT_FILENAME_MAX] or f"citation-{citation_id}"
    fallback = "".join(
        ch if " " <= ch <= "~" and ch not in '"\\' else "_" for ch in name
    )
    encoded = quote(name, safe="")
    return f"attachment; filename=\"{fallback}\"; filename*=UTF-8''{encoded}"


def _home_relative_path(anchor_webdav_url: str, home_webdav_url: str) -> Optional[str]:
    """Path of a cited file relative to the viewing user's home, if it's under it.

    Phase 3c (D7) only re-fetches a cited cloud file for the drift check when it
    is provably inside the *viewing user's own* cloud home — i.e. the anchor's
    WebDAV URL starts with the user's home WebDAV URL. Returns the home-relative
    path (no leading slash) in that case, else ``None`` (→ ``live_state =
    unreachable``: an external datasource or a different cloud the orchestrator
    can't fetch on the user's behalf). This both guards against comparing a
    same-named-but-different file and yields the path for
    ``get_project_folder_file_bytes``.
    """
    if not anchor_webdav_url or not home_webdav_url:
        return None
    base = home_webdav_url.rstrip("/")
    if not anchor_webdav_url.startswith(base):
        return None
    return anchor_webdav_url[len(base) :].lstrip("/") or None


# =============================================================================
# Sources
# =============================================================================


async def list_sources(
    *,
    job_id: str | None,
    type: str | None,
    limit: int,
    offset: int,
    dependencies: CitationDependencies,
) -> dict[str, Any]:
    """List sources, optionally filtered by job and/or type."""
    try:
        async with dependencies.vector_db.acquire() as conn:
            conditions = []
            params: list[Any] = []
            idx = 1

            if job_id:
                conditions.append(
                    f"s.id IN (SELECT source_id FROM job_sources WHERE job_id = ${idx}::uuid)"
                )
                params.append(job_id)
                idx += 1
            if type:
                conditions.append(f"s.type::text = ${idx}")
                params.append(type)
                idx += 1

            where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

            # Count total
            count_row = await conn.fetchrow(
                f"SELECT COUNT(*) as total FROM sources s {where}", *params
            )
            total = count_row["total"] if count_row else 0

            # Fetch sources
            params.append(limit)
            params.append(offset)
            rows = await conn.fetch(
                f"""SELECT s.id, s.type::text as type, s.identifier, s.name,
                       s.version, s.content_hash,
                       LEFT(s.content, 200) as content_preview,
                       s.metadata, s.created_at
                FROM sources s {where}
                ORDER BY s.created_at DESC
                LIMIT ${idx} OFFSET ${idx + 1}""",
                *params,
            )

            sources = [dict(r) for r in rows]

            # If querying across jobs, include job IDs for each source
            if not job_id:
                for src in sources:
                    job_rows = await conn.fetch(
                        "SELECT job_id FROM job_sources WHERE source_id = $1",
                        src["id"],
                    )
                    src["job_ids"] = [str(r["job_id"]) for r in job_rows]

            return {"sources": sources, "total": total}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def get_source_detail(
    source_id: int,
    *,
    content_limit: int,
    authorize: Callable[[Sequence[str]], Awaitable[bool]],
    dependencies: CitationDependencies,
) -> dict[str, Any]:
    """Get full detail for a single source.

    Visibility (G3): the source is visible if the caller can access at
    least one job linked to it via ``job_sources``. Admins (without an
    MCP project: scope) bypass without enumerating links.
    """
    try:
        async with dependencies.vector_db.acquire() as conn:
            if content_limit > 0:
                row = await conn.fetchrow(
                    """SELECT id, type::text as type, identifier, name, version,
                          LEFT(content, $2) as content, content_hash, metadata, created_at,
                          LENGTH(content) as full_content_length
                    FROM sources WHERE id = $1""",
                    source_id,
                    content_limit,
                )
            else:
                row = await conn.fetchrow(
                    """SELECT id, type::text as type, identifier, name, version,
                          content, content_hash, metadata, created_at,
                          LENGTH(content) as full_content_length
                    FROM sources WHERE id = $1""",
                    source_id,
                )

            if not row:
                raise HTTPException(
                    status_code=404, detail=f"Source {source_id} not found"
                )

            result = dict(row)
            result["content_truncated"] = (
                content_limit > 0
                and result.get("full_content_length", 0) > content_limit
            )

            # Include linked job IDs
            job_rows = await conn.fetch(
                "SELECT job_id FROM job_sources WHERE source_id = $1", source_id
            )
            result["job_ids"] = [str(r["job_id"]) for r in job_rows]

            # G3 visibility: caller must be able to access at least one
            # linked job. Admins with no MCP project: scope skip the loop.
            if not await authorize(result["job_ids"]):
                raise HTTPException(
                    status_code=403, detail="Not authorized to access this source"
                )

            return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


# =============================================================================
# Citations
# =============================================================================


async def list_job_citations(
    job_id: str,
    *,
    source_id: int | None,
    status: str | None,
    limit: int,
    offset: int,
    dependencies: CitationDependencies,
) -> dict[str, Any]:
    """List citations for a job with optional filters."""
    try:
        async with dependencies.vector_db.acquire() as conn:
            conditions = ["c.job_id = $1::uuid"]
            params: list[Any] = [job_id]
            idx = 2

            if source_id is not None:
                conditions.append(f"c.source_id = ${idx}")
                params.append(source_id)
                idx += 1
            if status:
                conditions.append(f"c.verification_status::text = ${idx}")
                params.append(status)
                idx += 1

            where = "WHERE " + " AND ".join(conditions)

            count_row = await conn.fetchrow(
                f"SELECT COUNT(*) as total FROM citations c {where}", *params
            )
            total = count_row["total"] if count_row else 0

            params.append(limit)
            params.append(offset)
            rows = await conn.fetch(
                f"""SELECT c.id, LEFT(c.claim, 200) as claim, c.source_id,
                       s.name as source_name, s.type::text as source_type,
                       c.verification_status::text as verification_status,
                       c.confidence::text as confidence,
                       c.extraction_method::text as extraction_method,
                       c.similarity_score, c.created_at
                FROM citations c
                JOIN sources s ON c.source_id = s.id
                {where}
                ORDER BY c.created_at DESC
                LIMIT ${idx} OFFSET ${idx + 1}""",
                *params,
            )

            return {"citations": [dict(r) for r in rows], "total": total}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def store_citation_snapshot(
    *,
    read_body: Callable[[], Awaitable[bytes]],
    content_type: str | None,
    dependencies: CitationDependencies,
) -> dict[str, Any]:
    """Persist the original bytes of a cited cloud document (Phase 3, D7).

    The request body is the raw file bytes; ``content_type`` is an optional
    query param used when the blob is later served back. Returns a
    content-addressed ``snapshot_blob_key`` the agent records onto the citation
    source's ``metadata.cloud`` so the original can be retrieved on view.
    """
    if not dependencies.snapshot_service.is_available:
        raise HTTPException(status_code=503, detail="Snapshot store unavailable")
    data = await read_body()
    if not data:
        raise HTTPException(status_code=400, detail="Empty body")
    content_type = content_type or "application/octet-stream"
    key = await dependencies.snapshot_service.save_blob(
        data, prefix="citations", content_type=content_type
    )
    if not key:
        raise HTTPException(status_code=500, detail="Snapshot store write failed")
    return {"snapshot_blob_key": key, "size_bytes": len(data)}


async def get_citation_detail(
    citation_id: int,
    *,
    authorize: Callable[[str | None], Awaitable[bool]],
    dependencies: CitationDependencies,
) -> dict[str, Any]:
    """Get full citation record with source info and verification details.

    **P4e** — visible if the caller can access the citation's linked job
    (mirrors G3's ``get_source_detail`` pattern). Admins without an MCP
    ``project:<uuid>`` scope bypass. Missing/unauthorized → 404 to avoid
    leaking citation existence via probe.
    """
    try:
        async with dependencies.vector_db.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT c.id, c.job_id, c.claim, c.verbatim_quote, c.quote_context,
                      c.quote_language, c.relevance_reasoning,
                      c.confidence::text as confidence,
                      c.extraction_method::text as extraction_method,
                      c.source_id, s.name as source_name, s.type::text as source_type,
                      s.identifier as source_identifier,
                      c.locator, c.verification_status::text as verification_status,
                      c.verification_notes, c.similarity_score, c.matched_location,
                      c.created_at, c.created_by
                FROM citations c
                JOIN sources s ON c.source_id = s.id
                WHERE c.id = $1""",
                citation_id,
            )

            if not row:
                raise HTTPException(
                    status_code=404, detail=f"Citation {citation_id} not found"
                )

            result = dict(row)
            job_id = result.get("job_id")
            if not await authorize(str(job_id) if job_id else None):
                # 404 instead of 403 — don't leak that the citation exists.
                raise HTTPException(
                    status_code=404, detail=f"Citation {citation_id} not found"
                )

            return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def get_citation_snapshot(
    citation_id: int,
    *,
    authorize: Callable[[str | None], Awaitable[bool]],
    dependencies: CitationDependencies,
) -> Response:
    """Serve the backed-up original bytes of a cited cloud document (Phase 3c, D7).

    Viewing-user auth (same gate as ``get_citation_detail``). Returns the copy
    SRW stored at cite-time (``metadata.cloud.snapshot_blob_key``) so a citation
    can show the exact version cited even when the live source changed or is
    unreachable. 404 if the citation is unknown/unauthorized or has no snapshot
    (404 over 403 so citation existence isn't leaked by probing).
    """
    try:
        async with dependencies.vector_db.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT c.job_id, s.name AS source_name, s.metadata
                   FROM citations c JOIN sources s ON c.source_id = s.id
                   WHERE c.id = $1""",
                citation_id,
            )
        if not row:
            raise HTTPException(
                status_code=404, detail=f"Citation {citation_id} not found"
            )
        job_id = row["job_id"]
        if not await authorize(str(job_id) if job_id else None):
            raise HTTPException(
                status_code=404, detail=f"Citation {citation_id} not found"
            )

        cloud = _source_cloud_meta(row["metadata"])
        key = cloud.get("snapshot_blob_key")
        if not key:
            raise HTTPException(
                status_code=404, detail="No snapshot stored for this citation"
            )
        data = await dependencies.snapshot_service.get_blob(key)
        if data is None:
            raise HTTPException(status_code=404, detail="Snapshot blob not found")

        # The stored type is the cited file's and may be ``text/html``: the
        # bytes are always a download, never rendered or sniffed on the API
        # origin (N51).
        media_type = cloud.get("content_type") or "application/octet-stream"
        return Response(
            content=data,
            media_type=media_type,
            headers={
                "Content-Disposition": _snapshot_content_disposition(
                    row["source_name"], citation_id
                ),
                "X-Content-Type-Options": "nosniff",
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def get_citation_drift(
    citation_id: int,
    *,
    caller: dict[str, Any],
    authorize: Callable[[str | None], Awaitable[bool]],
    dependencies: CitationDependencies,
) -> dict[str, Any]:
    """On-view drift check for a cited cloud document (Phase 3c, D7).

    Viewing-user auth. Compares the live source against what was cited. A
    best-effort re-fetch runs **only** when the cited file is provably inside the
    viewing user's own cloud home (so the credentials are the user's, never the
    agent's expired ones, and we never compare a same-named different file).
    Returns:

    - ``live_state``: ``unchanged`` | ``changed`` | ``unreachable`` | ``unknown``
    - ``snapshot_available``: whether ``/snapshot`` can serve the backed-up copy
    - ``cited``: the drift fingerprint captured at cite-time

    ``unreachable`` (external datasource / different cloud / no access) is the
    spec's "fall back to the snapshot" branch.
    """
    try:
        async with dependencies.vector_db.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT c.job_id, s.metadata
                   FROM citations c JOIN sources s ON c.source_id = s.id
                   WHERE c.id = $1""",
                citation_id,
            )
        if not row:
            raise HTTPException(
                status_code=404, detail=f"Citation {citation_id} not found"
            )
        job_id = row["job_id"]
        if not await authorize(str(job_id) if job_id else None):
            raise HTTPException(
                status_code=404, detail=f"Citation {citation_id} not found"
            )

        cloud = _source_cloud_meta(row["metadata"])
        if not cloud:
            raise HTTPException(
                status_code=400, detail="Citation has no cloud source to drift-check"
            )

        cited_sha = cloud.get("file_sha256")
        result: dict[str, Any] = {
            "citation_id": citation_id,
            "live_state": "unknown",
            "snapshot_available": bool(cloud.get("snapshot_blob_key")),
            "cited": {
                "etag": cloud.get("etag"),
                "file_sha256": cited_sha,
                "captured_at": cloud.get("captured_at"),
                "webdav_url": cloud.get("webdav_url"),
                "backend": cloud.get("backend"),
            },
        }

        # Best-effort live re-fetch via the viewing user's own cloud home.
        try:
            backend = dependencies.main_cloud_router.for_owner(caller)
            user_id = await resolve_user_identity_cached(
                dependencies.store, caller, backend
            )
            home = await backend.get_user_home(user_id) if user_id else None
            rel = (
                _home_relative_path(
                    cloud.get("webdav_url") or "", home.webdav_url or ""
                )
                if home and home.webdav_url
                else None
            )
            if rel is None:
                result["live_state"] = "unreachable"
                result["reason"] = "live source not reachable from your account"
                return result
            live_bytes = await backend.get_project_folder_file_bytes(
                home.handle, path=rel
            )
            live_sha = hashlib.sha256(live_bytes).hexdigest()
            result["live"] = {"file_sha256": live_sha, "size_bytes": len(live_bytes)}
            result["live_state"] = (
                "unchanged" if cited_sha and live_sha == cited_sha else "changed"
            )
            return result
        except Exception as e:
            dependencies.logger.info(
                "Citation %s drift live-fetch unreachable: %s", citation_id, e
            )
            result["live_state"] = "unreachable"
            result["reason"] = "live source could not be fetched"
            return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


# =============================================================================
# Per-job source annotations, tags and statistics
# =============================================================================


async def get_source_annotations(
    job_id: str,
    source_id: int,
    *,
    type: str | None,
    dependencies: CitationDependencies,
) -> list[dict[str, Any]]:
    """Get annotations for a source within a job."""
    try:
        async with dependencies.vector_db.acquire() as conn:
            if type:
                rows = await conn.fetch(
                    """SELECT id, annotation_type, content, page_reference, created_at, created_by
                    FROM source_annotations
                    WHERE source_id = $1 AND job_id = $2::uuid AND annotation_type = $3
                    ORDER BY created_at""",
                    source_id,
                    job_id,
                    type,
                )
            else:
                rows = await conn.fetch(
                    """SELECT id, annotation_type, content, page_reference, created_at, created_by
                    FROM source_annotations
                    WHERE source_id = $1 AND job_id = $2::uuid
                    ORDER BY created_at""",
                    source_id,
                    job_id,
                )

            return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def get_source_tags(
    job_id: str, source_id: int, *, dependencies: CitationDependencies
) -> list[str]:
    """Get tags for a source within a job."""
    try:
        async with dependencies.vector_db.acquire() as conn:
            rows = await conn.fetch(
                "SELECT tag FROM source_tags WHERE source_id = $1 AND job_id = $2::uuid ORDER BY tag",
                source_id,
                job_id,
            )
            return [r["tag"] for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def get_citation_stats(
    job_id: str, *, dependencies: CitationDependencies
) -> dict[str, Any]:
    """Get citation statistics for a job."""
    try:
        async with dependencies.vector_db.acquire() as conn:
            # Sources by type
            source_rows = await conn.fetch(
                """SELECT s.type::text as type, COUNT(*) as count
                FROM sources s
                JOIN job_sources js ON s.id = js.source_id
                WHERE js.job_id = $1::uuid
                GROUP BY s.type""",
                job_id,
            )
            sources_by_type = {r["type"]: r["count"] for r in source_rows}
            total_sources = sum(sources_by_type.values())

            # Citations by verification status
            status_rows = await conn.fetch(
                """SELECT verification_status::text as status, COUNT(*) as count
                FROM citations WHERE job_id = $1::uuid
                GROUP BY verification_status""",
                job_id,
            )
            by_status = {r["status"]: r["count"] for r in status_rows}

            # Citations by confidence
            conf_rows = await conn.fetch(
                """SELECT confidence::text as confidence, COUNT(*) as count
                FROM citations WHERE job_id = $1::uuid
                GROUP BY confidence""",
                job_id,
            )
            by_confidence = {r["confidence"]: r["count"] for r in conf_rows}

            # Citations by extraction method
            method_rows = await conn.fetch(
                """SELECT extraction_method::text as method, COUNT(*) as count
                FROM citations WHERE job_id = $1::uuid
                GROUP BY extraction_method""",
                job_id,
            )
            by_method = {r["method"]: r["count"] for r in method_rows}

            total_citations = sum(by_status.values())

            return {
                "job_id": job_id,
                "total_sources": total_sources,
                "sources_by_type": sources_by_type,
                "total_citations": total_citations,
                "citations_by_verification_status": by_status,
                "citations_by_confidence": by_confidence,
                "citations_by_extraction_method": by_method,
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


# =============================================================================
# Memories
# =============================================================================


async def get_memory_stats(
    job_id: str, *, dependencies: CitationDependencies
) -> dict[str, Any]:
    """Get memory statistics for a job."""
    try:
        async with dependencies.vector_db.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT
                    COUNT(*) AS total,
                    COALESCE(SUM(token_count), 0) AS total_tokens,
                    COALESCE(SUM(access_count), 0) AS total_accesses,
                    COUNT(*) FILTER (WHERE memory_type = 'factual') AS factual,
                    COUNT(*) FILTER (WHERE memory_type = 'procedural') AS procedural,
                    COUNT(*) FILTER (WHERE memory_type = 'error_solution') AS error_solution,
                    COUNT(*) FILTER (WHERE memory_type = 'vocabulary') AS vocabulary,
                    COUNT(*) FILTER (WHERE memory_type = 'relational') AS relational,
                    COUNT(*) FILTER (WHERE source = 'observer') AS from_observer,
                    COUNT(*) FILTER (WHERE source = 'todo') AS from_todo,
                    COUNT(*) FILTER (WHERE source = 'compaction') AS from_compaction,
                    COUNT(*) FILTER (WHERE source = 'phase_archive') AS from_phase_archive,
                    COUNT(*) FILTER (WHERE source = 'tool_error') AS from_tool_error,
                    AVG(importance) AS avg_importance
                FROM memories
                WHERE job_id = $1::uuid
                """,
                job_id,
            )
            if row:
                result = dict(row)
                # Convert Decimal avg_importance to float for JSON serialization
                if result.get("avg_importance") is not None:
                    result["avg_importance"] = float(result["avg_importance"])
                result["job_id"] = job_id
                return result
            return {"job_id": job_id, "total": 0, "total_tokens": 0}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def get_project_memory_stats(
    project_id: str, *, dependencies: CitationDependencies
) -> dict[str, Any]:
    """Get memory statistics for a project (all memories scoped to this project)."""
    try:
        async with dependencies.vector_db.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT
                    COUNT(*) AS total,
                    COALESCE(SUM(token_count), 0) AS total_tokens,
                    COALESCE(SUM(access_count), 0) AS total_accesses,
                    COUNT(*) FILTER (WHERE memory_type = 'factual') AS factual,
                    COUNT(*) FILTER (WHERE memory_type = 'procedural') AS procedural,
                    COUNT(*) FILTER (WHERE memory_type = 'error_solution') AS error_solution,
                    COUNT(*) FILTER (WHERE memory_type = 'vocabulary') AS vocabulary,
                    COUNT(*) FILTER (WHERE memory_type = 'relational') AS relational,
                    COUNT(*) FILTER (WHERE source = 'observer') AS from_observer,
                    COUNT(*) FILTER (WHERE source = 'todo') AS from_todo,
                    COUNT(*) FILTER (WHERE source = 'compaction') AS from_compaction,
                    COUNT(*) FILTER (WHERE source = 'phase_archive') AS from_phase_archive,
                    COUNT(*) FILTER (WHERE source = 'tool_error') AS from_tool_error,
                    AVG(importance) AS avg_importance
                FROM memories
                WHERE project_id = $1::uuid
                """,
                project_id,
            )
            if row:
                result = dict(row)
                if result.get("avg_importance") is not None:
                    result["avg_importance"] = float(result["avg_importance"])
                result["project_id"] = project_id
                return result
            return {"project_id": project_id, "total": 0, "total_tokens": 0}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def list_job_memories(
    job_id: str,
    *,
    memory_type: str | None,
    source: str | None,
    search: str | None,
    sort_by: str,
    sort_order: str,
    limit: int,
    offset: int,
    dependencies: CitationDependencies,
) -> dict[str, Any]:
    """List individual memories for a job with optional filters and pagination."""
    # Validate sort parameters
    valid_sort_fields = {
        "created_at",
        "importance",
        "access_count",
        "token_count",
        "last_accessed",
    }
    if sort_by not in valid_sort_fields:
        sort_by = "created_at"
    if sort_order not in {"asc", "desc"}:
        sort_order = "desc"

    try:
        async with dependencies.vector_db.acquire() as conn:
            conditions = ["job_id = $1::uuid"]
            params: list[Any] = [job_id]
            idx = 2

            if memory_type:
                conditions.append(f"memory_type = ${idx}")
                params.append(memory_type)
                idx += 1
            if source:
                conditions.append(f"source = ${idx}")
                params.append(source)
                idx += 1
            if search:
                conditions.append(f"(content ILIKE ${idx} OR summary ILIKE ${idx})")
                params.append(f"%{search}%")
                idx += 1

            where = " AND ".join(conditions)

            # Count total
            count_row = await conn.fetchrow(
                f"SELECT COUNT(*) as cnt FROM memories WHERE {where}",
                *params,
            )
            total = count_row["cnt"] if count_row else 0

            # Fetch page
            params.extend([limit, offset])
            rows = await conn.fetch(
                f"SELECT id, job_id, project_id, agent_id, "
                f"LEFT(content, 300) as content_preview, summary, "
                f"memory_type, source, keywords, importance, "
                f"source_turn_start, source_turn_end, source_phase, "
                f"token_count, access_count, created_at, last_accessed "
                f"FROM memories WHERE {where} "
                f"ORDER BY {sort_by} {sort_order} "
                f"LIMIT ${idx} OFFSET ${idx + 1}",
                *params,
            )
            memories = []
            for r in rows:
                m = dict(r)
                # Convert UUIDs and datetimes for JSON serialization
                for k in ("id", "job_id", "project_id"):
                    if m.get(k) is not None:
                        m[k] = str(m[k])
                for k in ("created_at", "last_accessed"):
                    if m.get(k) is not None:
                        m[k] = m[k].isoformat()
                if m.get("importance") is not None:
                    m["importance"] = float(m["importance"])
                memories.append(m)

        return {"memories": memories, "total": total, "limit": limit, "offset": offset}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


# =============================================================================
# Source search
# =============================================================================


async def search_job_sources(
    job_id: str,
    *,
    query: str,
    mode: str,
    source_type: str | None,
    tags: str | None,
    top_k: int,
    dependencies: CitationDependencies,
) -> dict[str, Any]:
    """Search a job's source library using keyword search.

    Falls back to SQL keyword search. Semantic/hybrid modes require
    the CitationEngine with pgvector.
    """
    try:
        async with dependencies.vector_db.acquire() as conn:
            # Build conditions for source filtering
            conditions = ["js.job_id = $1::uuid"]
            params: list[Any] = [job_id]
            idx = 2

            if source_type:
                conditions.append(f"s.type::text = ${idx}")
                params.append(source_type)
                idx += 1

            # Tag filtering: find sources that have ALL specified tags
            if tags:
                tag_list = [t.strip() for t in tags.split(",") if t.strip()]
                for tag in tag_list:
                    conditions.append(
                        f"EXISTS (SELECT 1 FROM source_tags st "
                        f"WHERE st.source_id = s.id AND st.job_id = $1::uuid AND st.tag = ${idx})"
                    )
                    params.append(tag)
                    idx += 1

            where = "WHERE " + " AND ".join(conditions)

            # Keyword search using PostgreSQL full-text search
            params.append(query)
            query_param_idx = idx
            idx += 1
            params.append(top_k)

            rows = await conn.fetch(
                f"""SELECT s.id, s.name, s.type::text as type, s.identifier,
                       ts_rank(to_tsvector('simple', s.content),
                               plainto_tsquery('simple', ${query_param_idx})) as rank,
                       ts_headline('simple', s.content,
                                   plainto_tsquery('simple', ${query_param_idx}),
                                   'MaxFragments=2,MaxWords=60,MinWords=20') as snippet
                FROM sources s
                JOIN job_sources js ON s.id = js.source_id
                {where}
                  AND to_tsvector('simple', s.content) @@ plainto_tsquery('simple', ${query_param_idx})
                ORDER BY rank DESC
                LIMIT ${idx}""",
                *params,
            )

            results = []
            for r in rows:
                rank = float(r["rank"]) if r["rank"] else 0.0
                if rank > 0.1:
                    evidence = "HIGH"
                elif rank > 0.01:
                    evidence = "MEDIUM"
                else:
                    evidence = "LOW"

                results.append(
                    {
                        "source_id": r["id"],
                        "source_name": r["name"],
                        "source_type": r["type"],
                        "identifier": r["identifier"],
                        "evidence_label": evidence,
                        "rank": rank,
                        "snippet": r["snippet"],
                    }
                )

            return {
                "job_id": job_id,
                "query": query,
                "mode": "keyword",
                "results": results,
                "total": len(results),
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
