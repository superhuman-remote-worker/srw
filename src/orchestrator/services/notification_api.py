"""The notification feed's read/write surface and its SSE stream.

Extracted verbatim from ``orchestrator.main`` (R1.B07 lane N, census group
``J_notifications``; unified_notification_system.md). Seven operations over the
one durable feed — there is no legacy view to merge any more, every producer
records here.

What travels with them:

* :func:`list_notifications` — keyset paging (``before`` is the previous page's
  ``next_before``), repeatable ``category``, the ``status`` vocabulary and the
  ``source_kind`` + ``source_id`` pair that the officer card uses. The pair is
  all-or-nothing: one without the other is a 400.
* :func:`notification_sse_events` — the live stream. Its first byte is a
  ``: open`` comment so ``EventSource.onopen`` fires at once and buffering
  proxies do not idle-timeout before the 30 s keepalive.
* :func:`get_notification_detail` — the row plus its source's presentation
  payload, loaded through the ``source_kind`` registry so the centre never
  learns what a job or a sudo request *is*, plus the row's deferred channel
  steps so the pane can say what will happen next.
* :func:`act_on_notification` — 404 when the row is not this user's, 400 when
  the row does not declare the action, and a **loud 500** when the category
  declares it but nothing handles it: a silent no-op there would look exactly
  like a working button.

Each operation receives the already-resolved principal. The approval gate and
the outer error envelope stay on the route declaration, where
``scripts/check_endpoint_auth.py`` reads the audited gate and where the
envelope's exact shape (HTTPException re-raised, anything else a logged 500)
was before the extraction.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from orchestrator.schemas.notifications import (
    NotificationActRequest,
    NotificationSeenRequest,
)
from orchestrator.services.notification_catalog import (
    serialize_notification,
    source_loader,
)
from orchestrator.services.notification_service import (
    ActionNotDeclared,
    ActionUnregistered,
    NotificationNotFound,
)

logger = logging.getLogger(__name__)


@dataclass
class NotificationApiDependencies:
    """The store and the notification authority, resolved per invocation."""

    store: Any
    notifier: Any


async def list_notifications(
    request: Request,
    before: str | None = Query(None),
    limit: int = Query(50, le=200),
    category: list[str] | None = Query(None),
    status: str = Query("all"),
    source_kind: str | None = Query(None),
    source_id: str | None = Query(None),
    *,
    dependencies: NotificationApiDependencies,
    user: dict[str, Any],
) -> dict[str, Any]:
    """The current user's notification feed (unified notification system).

    ``items`` is the durable feed: keyset-paged newest first (``before`` is
    the ``next_before`` cursor of the previous page), filterable by
    ``category`` (repeatable), ``status`` (pending | resolved | unread |
    unseen | archived | all) and a ``source_kind`` + ``source_id`` pair
    (e.g. the officer card listing the pages about one officer thread).
    ``counts`` drives the bell. The feed is the only store: every producer
    records here, so there is no legacy view to merge any more.
    """
    try:
        if (source_kind is None) != (source_id is None):
            raise HTTPException(
                status_code=400, detail="source_kind and source_id go together"
            )
        try:
            page = await dependencies.notifier.get_feed_page(
                recipient_kind="user",
                recipient_id=str(user["id"]),
                before=before,
                limit=limit,
                categories=category or None,
                status=status,
                source_kind=source_kind,
                source_id=source_id,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        return page
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Failed to list notifications: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


async def mark_notifications_seen(
    request: Request,
    body: NotificationSeenRequest,
    *,
    dependencies: NotificationApiDependencies,
    user: dict[str, Any],
) -> dict[str, Any]:
    """Batch seen-stamp — the cockpit posts the ids that rendered in the feed.
    Never regresses an earlier stamp; unknown or foreign ids are ignored."""
    try:
        ids = [str(i) for i in body.ids][:200]
        updated = await dependencies.notifier.mark_seen(
            recipient_kind="user", recipient_id=str(user["id"]), ids=ids
        )
        return {"updated": updated}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Failed to mark notifications seen: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


async def notification_sse_events(
    request: Request, *, user_id: str
) -> StreamingResponse:
    """SSE endpoint for real-time notification updates.

    Clients connect via EventSource to receive live notification events.
    Events: new_message, reply_delivered.
    """
    from orchestrator.services.notification_feed import notification_feed

    queue = notification_feed.subscribe_sse(user_id)

    async def event_stream():
        try:
            # Kickstart: flush immediately so EventSource.onopen fires at once and
            # buffering proxies (Cloudflare Tunnel, Traefik) don't idle-timeout
            # before the first byte — otherwise the next byte is the 30s keepalive
            # below. Comments (`:`-prefixed) are ignored by EventSource.
            yield ": open\n\n"
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=30)
                    if event is None:
                        break  # bridge lost its LISTEN authority; EventSource reconnects
                    yield f"data: {json.dumps(event)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
                except asyncio.CancelledError:
                    break
        finally:
            notification_feed.unsubscribe_sse(user_id, queue)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# The declaration-order rule this operation depends on lives with the routes
# that enforce it: `routers/notifications.py` registers
# `GET /api/notifications/events` BEFORE `GET /api/notifications/{notification_id}`,
# because FastAPI matches in declaration order and the path parameter would
# otherwise swallow the SSE path.
async def get_notification_detail(
    request: Request,
    notification_id: str,
    *,
    dependencies: NotificationApiDependencies,
    user: dict[str, Any],
) -> dict[str, Any]:
    """One feed row plus its source's presentation payload (the detail pane).
    The source loader is registered per ``source_kind``; the center never
    learns what a job or a sudo request is."""
    try:
        row = await dependencies.store.get_notification(notification_id)
        if (
            not row
            or row.get("recipient_kind") != "user"
            or str(row.get("recipient_id")) != str(user["id"])
        ):
            raise HTTPException(status_code=404, detail="Notification not found")
        source = None
        loader = source_loader(row.get("source_kind"))
        if loader is not None:
            try:
                source = await loader(
                    dependencies.store, str(row.get("source_id")), user
                )
            except HTTPException:
                source = None
            except Exception:
                logger.debug(
                    "source loader failed for notification %s",
                    notification_id,
                    exc_info=True,
                )
        # The row's deferred channel steps ("email in 12 min unless you look
        # or someone settles it") — the detail pane can say what will happen.
        try:
            steps = await dependencies.notifier.describe_steps(str(row["id"]))
        except Exception:
            logger.debug("step listing failed for %s", notification_id, exc_info=True)
            steps = []
        return {
            "notification": serialize_notification(row),
            "source": source,
            "steps": steps,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Failed to load notification {notification_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


async def mark_notification_read_v2(
    request: Request,
    notification_id: str,
    *,
    dependencies: NotificationApiDependencies,
    user: dict[str, Any],
) -> dict[str, Any]:
    """Explicit read stamp (also stamps seen). Idempotent."""
    try:
        row = await dependencies.notifier.mark_read(
            recipient_kind="user",
            recipient_id=str(user["id"]),
            notification_id=notification_id,
        )
        if row is None:
            raise HTTPException(status_code=404, detail="Notification not found")
        return {"notification": row}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Failed to mark notification read: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


async def archive_notification(
    request: Request,
    notification_id: str,
    *,
    dependencies: NotificationApiDependencies,
    user: dict[str, Any],
) -> dict[str, Any]:
    """Hide a row from the feed without touching its resolution. Idempotent."""
    try:
        row = await dependencies.notifier.archive(
            recipient_kind="user",
            recipient_id=str(user["id"]),
            notification_id=notification_id,
        )
        if row is None:
            raise HTTPException(status_code=404, detail="Notification not found")
        return {"notification": row}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Failed to archive notification: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


async def act_on_notification(
    request: Request,
    notification_id: str,
    body: NotificationActRequest,
    *,
    dependencies: NotificationApiDependencies,
    user: dict[str, Any],
) -> dict[str, Any]:
    """Run one of the row's declared actions through its registered handler.

    404 when the row is not this user's; 400 when the row does not declare
    the action; 500 when the category declares it but nothing handles it —
    loud, like ``_run_completion_effect``'s registry gate, because a silent
    no-op here would look exactly like a working button.
    """
    try:
        try:
            outcome = await dependencies.notifier.act(
                notification_id=notification_id,
                user=user,
                action_type=body.action_type,
                params=body.params,
            )
        except NotificationNotFound:
            raise HTTPException(status_code=404, detail="Notification not found")
        except ActionNotDeclared as e:
            raise HTTPException(
                status_code=400,
                detail=f"Action {e} is not declared on this notification",
            )
        except ActionUnregistered as e:
            logger.error("unregistered notification action %s", e)
            raise HTTPException(
                status_code=500, detail=f"unregistered notification action {e}"
            )
        return {"status": "ok", **outcome}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Notification action failed for {notification_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


def notification_jsonable(value: Any) -> Any:
    """Source-loader payloads cross the wire as-is; coerce the asyncpg types."""
    if isinstance(value, dict):
        return {k: notification_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [notification_jsonable(v) for v in value]
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


__all__ = [
    "NotificationApiDependencies",
    "act_on_notification",
    "archive_notification",
    "get_notification_detail",
    "list_notifications",
    "mark_notification_read_v2",
    "mark_notifications_seen",
    "notification_jsonable",
    "notification_sse_events",
]
