"""Owner-facing session history and citation reads (R1.B10).

Both reads are owner-gated and cache-sensitive. History rows and their cache
fence (``events_epoch`` / ``conversation_revision``) come from one repeatable-
read snapshot so a rewind can never pair a newer fence with an older page.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response

from orchestrator.services import citations as citations_operations
from orchestrator.services.thread_projection import stamp_tool_categories

router = APIRouter()


@dataclass(frozen=True, slots=True)
class ThreadHistoryDependencies:
    """Application-owned collaborators for one history/citation read."""

    store: Any
    vector_db: Any
    require_thread_owner: Callable[
        [Request, Any, str], Awaitable[tuple[dict[str, Any], dict[str, Any]]]
    ]


def get_thread_history_dependencies(request: Request) -> ThreadHistoryDependencies:
    return request.app.state.thread_history_dependencies_factory()


@router.get("/api/persistent/threads/{thread_id}/citations")
async def get_thread_citations(
    thread_id: str,
    request: Request,
    limit: int = Query(default=200, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    *,
    dependencies: ThreadHistoryDependencies = Depends(get_thread_history_dependencies),
) -> dict[str, Any]:
    """List citations created in a persistent session, for inline ``[N]`` rendering.

    The citation engine stores a session's citations with ``job_id = thread_id``
    (it maps ``CitationContext.session_id`` → ``job_id``), so the thread UUID *is*
    the ``job_id`` — there is no separate thread column. Owner-only (the by-job
    endpoint 404s for a thread since no ``jobs`` row exists). The marker the agent
    emits is the citation ``id``; the cockpit renumbers for display and resolves
    each ``[id]`` to a row returned here.
    """
    await dependencies.require_thread_owner(request, dependencies.store, thread_id)
    try:
        async with dependencies.vector_db.acquire() as conn:
            count_row = await conn.fetchrow(
                "SELECT COUNT(*) AS total FROM citations WHERE job_id = $1::uuid",
                thread_id,
            )
            total = count_row["total"] if count_row else 0
            rows = await conn.fetch(
                """SELECT c.id, LEFT(c.claim, 300) AS claim, c.source_id,
                       s.name AS source_name, s.type::text AS source_type,
                       s.identifier AS source_identifier,
                       c.verification_status::text AS verification_status,
                       c.confidence::text AS confidence,
                       c.created_at, s.metadata
                FROM citations c
                JOIN sources s ON c.source_id = s.id
                WHERE c.job_id = $1::uuid
                ORDER BY c.id ASC
                LIMIT $2 OFFSET $3""",
                thread_id,
                limit,
                offset,
            )
            citations = []
            for r in rows:
                d = dict(r)
                # Cloud-document citations (cite_document with a snapshot-anchor)
                # can offer "view original" (/snapshot) + on-view drift (/drift);
                # web citations have neither. Surface the two flags so the cockpit
                # only renders those controls where they apply. The raw metadata
                # isn't returned (internal blob keys / anchor URLs).
                cloud = citations_operations._source_cloud_meta(d.pop("metadata", None))
                d["has_cloud_anchor"] = bool(cloud)
                d["has_snapshot"] = bool(cloud.get("snapshot_blob_key"))
                citations.append(d)
            return {
                "citations": citations,
                "total": total,
                "thread_id": thread_id,
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/api/persistent/threads/{thread_id}/messages")
async def get_thread_messages_history(
    thread_id: str,
    request: Request,
    response: Response = None,
    limit: Optional[int] = None,
    before: Optional[str] = None,
    after: Optional[str] = None,
    offset: int = 0,
    *,
    dependencies: ThreadHistoryDependencies = Depends(get_thread_history_dependencies),
) -> dict[str, Any]:
    """Load message history for a persistent thread, ascending (chronological).

    Default (no params) returns the **entire** conversation — the cockpit caches
    the full thread client-side and windows the render itself, so the display
    must not be truncated. Cursor paging (mutually exclusive, ISO-8601):

    - ``before=<ts>``: backfill — newest messages at-or-before the cursor, up to
      ``limit``.
    - ``after=<ts>``:  catch-up — messages at-or-after the cursor, up to ``limit``.

    A bare ``limit`` with no cursor keeps the legacy oldest-first paged read
    (``offset`` honored) used by the MCP inspection tool. Returns
    ``{messages, total, has_more, thread_id}``.
    """
    store = dependencies.store
    user, thread = await dependencies.require_thread_owner(request, store, thread_id)
    if response is not None:
        response.headers["Cache-Control"] = "private, no-store"

    def _parse_cursor(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise HTTPException(
                status_code=400, detail=f"Invalid ISO-8601 timestamp: {value!r}"
            )

    before_dt = _parse_cursor(before)
    after_dt = _parse_cursor(after)
    if before_dt is not None and after_dt is not None:
        raise HTTPException(
            status_code=400, detail="Pass at most one of 'before' / 'after'"
        )

    capped_limit = min(limit, 500) if limit is not None else None

    # Rows and their cache fence come from one repeatable-read snapshot. A
    # rewind cannot therefore pair its newer epoch/revision with an older page
    # that still contains tombstoned messages (or vice versa).
    async with store.acquire() as conn:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            history_state = await conn.fetchrow(
                "SELECT events_epoch, conversation_revision FROM threads WHERE id=$1",
                thread_id,
            )
            if history_state is None:
                raise HTTPException(status_code=404, detail="Thread not found")
            if before_dt is not None or after_dt is not None:
                messages, has_more = await store.get_thread_messages_page(
                    thread_id=thread_id,
                    before=before_dt,
                    after=after_dt,
                    limit=capped_limit,
                    conn=conn,
                )
                # A cursor window carries no cheap true total; no consumer reads it here.
                total = len(messages)
            else:
                messages = await store.get_thread_messages_history(
                    thread_id=thread_id,
                    limit=capped_limit,
                    offset=offset,
                    conn=conn,
                )
                # Legacy paged read: a full page implies there may be more.
                has_more = capped_limit is not None and len(messages) == capped_limit
                if capped_limit is None:
                    total = len(messages)
                else:
                    total = await store.get_thread_message_count(thread_id, conn=conn)

    stamp_tool_categories(messages)

    return {
        "messages": messages,
        "total": total,
        "has_more": has_more,
        "thread_id": thread_id,
        "events_epoch": int(history_state["events_epoch"] or 0),
        "conversation_revision": int(history_state["conversation_revision"] or 0),
    }


__all__ = [
    "ThreadHistoryDependencies",
    "get_thread_history_dependencies",
    "router",
]
