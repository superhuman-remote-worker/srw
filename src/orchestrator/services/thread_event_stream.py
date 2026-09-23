"""Session event stream: SSE replay, live tail and client presence (R1.B10).

SSE replaces the WebSocket as the primary server→client path; the existing
``/ws/persistent/{thread_id}`` stays as a fallback. Per
knowledge-base/knowledge/features/headless_persistent_sessions.md.

A stream replays the thread's durable event journal from an ``<epoch>:<seq>``
cursor and then tails it. A stateless session additionally treats an attached
stream as client presence — but only for a caller who could answer
(:func:`orchestrator.security.token_scopes.tethers_session`). A read-only token
may stream and is re-authorized on the renewal cadence, yet never establishes
or renews presence.

The three tunables are module globals read at call time, so tests can
monkeypatch them on this module.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Optional

from fastapi import HTTPException, Request
from fastapi.responses import StreamingResponse

from orchestrator.security.token_scopes import tethers_session
from shared.thread_presence import (
    DEFAULT_PRESENCE_RENEW_SECONDS,
    DEFAULT_PRESENCE_TTL_SECONDS,
    refresh_thread_presence,
)

logger = logging.getLogger(__name__)


# How much *accumulated idle time* (seconds with no new rows) a live SSE stream
# tolerates before it re-reads `events_epoch` to detect a mid-stream bump. The
# epoch is bumped when an agent (re-)attaches; a generator opened before the
# bump would otherwise poll the dead old epoch forever, delivering nothing but
# keepalive pings that fool the client watchdog into thinking the stream is
# healthy (the "stale → refresh to fix" zombie). Read as a module global so
# tests can monkeypatch it to 0 to force a re-check on the first empty poll.
THREAD_EVENTS_EPOCH_RECHECK_S: float = float(
    os.environ.get("THREAD_EVENTS_EPOCH_RECHECK_S", "2.0")
)
THREAD_CLIENT_PRESENCE_RENEW_S: float = max(
    1.0,
    float(
        os.environ.get(
            "THREAD_CLIENT_PRESENCE_RENEW_S",
            str(DEFAULT_PRESENCE_RENEW_SECONDS),
        )
    ),
)
THREAD_CLIENT_PRESENCE_TTL_S: float = max(
    THREAD_CLIENT_PRESENCE_RENEW_S * 2.0,
    float(
        os.environ.get(
            "THREAD_CLIENT_PRESENCE_TTL_S",
            str(DEFAULT_PRESENCE_TTL_SECONDS),
        )
    ),
)


@dataclass(frozen=True, slots=True)
class ThreadEventStreamDependencies:
    """Application-owned collaborators for one session event stream."""

    store: Any
    require_thread_owner: Callable[
        [Request, Any, str], Awaitable[tuple[dict[str, Any], dict[str, Any]]]
    ]


async def no_cursor_replay_start(conn, thread_id: str, epoch: int) -> int:
    """Replay floor (exclusive) for an SSE attach that carries no cursor.

    A fresh client — opening the session on a second device, or any client
    with no cached cursor for this thread — has already painted the thread's
    completed turns from REST history. Replaying the whole epoch from seq 0
    would re-deliver each completed turn as a *live* copy the cockpit reducer
    can't reconcile (history turns are keyed by message id, replayed turns by
    turn_id), so the last assistant turn renders twice, split by a spurious
    "SESSION RESUMED" divider — the cold-attach twin of the gone_beyond_horizon
    duplicate render.

    Anchor instead just past the last turn-terminal event (``turn.completed`` /
    ``turn.error``, both of which persist their turn to ``thread_messages``), so
    the replay carries only the in-flight, not-yet-persisted turn. Returns 0
    when no turn has finished yet (first turn still streaming) so that turn —
    absent from REST history — still replays from the start.
    """
    anchor = await conn.fetchval(
        "SELECT COALESCE(MAX(seq), 0) FROM thread_events "
        "WHERE thread_id = $1 AND epoch = $2 "
        "AND kind IN ('turn.completed', 'turn.error')",
        thread_id,
        epoch,
    )
    return int(anchor or 0)


async def open_thread_event_stream(
    thread_id: str,
    request: Request,
    *,
    user: dict[str, Any],
    thread: dict[str, Any],
    dependencies: ThreadEventStreamDependencies,
) -> StreamingResponse:
    """Open the stream for an already owner-authorized ``thread``.

    See ``GET /api/persistent/threads/{thread_id}/stream`` for the wire
    contract; ``user``/``thread`` are that route's authorization result.
    """
    store = dependencies.store

    # The existing owner-gated SSE connection is the lane-agnostic client
    # attachment signal. No lane field crosses the wire. Pinned streams keep
    # their exact behavior; a stateless stream must establish its durable TTL
    # before the browser can believe it is attached. Only a caller who could
    # answer tethers (tethers_session): a read-only token still streams and is
    # re-authorized on the renewal cadence, but records no presence.
    stateless_stream = thread.get("execution_lane") == "stateless"
    track_presence = stateless_stream and tethers_session(user)
    if track_presence:
        try:
            presence = await refresh_thread_presence(
                store,
                thread_id=thread_id,
                ttl_seconds=THREAD_CLIENT_PRESENCE_TTL_S,
                establish=True,
            )
        except Exception as exc:
            logger.warning(
                "thread_event_stream presence establish failed (thread=%s): %s",
                thread_id,
                exc,
            )
            raise HTTPException(
                status_code=503,
                detail="Session presence is temporarily unavailable",
            ) from exc
        if not presence.served:
            # The row changed lane or disappeared after the owner lookup. A
            # reconnect re-runs authorization and resolves the current lane.
            raise HTTPException(status_code=409, detail="Session lane changed")

    server_epoch = int(thread.get("events_epoch") or 0)

    # Parse Last-Event-ID. Format: "<epoch>:<seq>". Missing/malformed → no
    # cursor, so the replay floor is computed by no_cursor_replay_start below
    # (anchored past the last completed turn, not seq 0).
    #
    # EventSource doesn't let the browser set custom request headers, so the
    # cockpit hands us the cached cursor via `?last_event_id=` for the
    # initial connection. On automatic reconnect, the browser appends the
    # `Last-Event-ID` header from the latest `id:` line we yielded — that
    # path is fully native and doesn't need the query param.
    last_event_id = (
        request.headers.get("Last-Event-ID")
        or request.headers.get("last-event-id")
        or request.query_params.get("last_event_id")
    )
    cursor_epoch: Optional[int] = None
    cursor_seq: Optional[int] = None
    if last_event_id:
        try:
            e_str, s_str = last_event_id.split(":", 1)
            cursor_epoch = int(e_str)
            cursor_seq = int(s_str)
        except (ValueError, AttributeError):
            cursor_epoch = None
            cursor_seq = None

    async def event_stream():
        # Kickstart: flush a comment immediately so the browser EventSource
        # fires `onopen` at once and buffering intermediaries (Cloudflare
        # Tunnel, Traefik) don't hold the response headers / idle-timeout the
        # connection waiting for the first body byte. Without this, a connect
        # whose cursor is already at the tail sends nothing until the ~20s
        # keepalive ping below — stalling the SSE receive path ~20s. Comments
        # (lines starting with `:`) are ignored by EventSource, so this is
        # side-effect-free on the client.
        yield ": open\n\n"

        next_presence_renew = time.monotonic() + THREAD_CLIENT_PRESENCE_RENEW_S

        # Mismatched epoch → force re-sync.
        if cursor_epoch is not None and cursor_epoch != server_epoch:
            async with store.acquire() as conn:
                tail = await conn.fetchval(
                    "SELECT COALESCE(MAX(seq), 0) FROM thread_events "
                    "WHERE thread_id = $1 AND epoch = $2",
                    thread_id,
                    server_epoch,
                )
            payload = json.dumps(
                {
                    "method": "gone_beyond_horizon",
                    "params": {
                        "epoch": server_epoch,
                        "server_seq": int(tail or 0),
                        "reason": "epoch_mismatch",
                    },
                }
            )
            yield f"id: {server_epoch}:0\nevent: gone_beyond_horizon\ndata: {payload}\n\n"
            return

        # Retention floor for the current epoch.
        async with store.acquire() as conn:
            min_seq = await conn.fetchval(
                "SELECT MIN(seq) FROM thread_events "
                "WHERE thread_id = $1 AND epoch = $2",
                thread_id,
                server_epoch,
            )
        min_seq = int(min_seq) if min_seq is not None else 0

        # Cursor older than retention → also force re-sync.
        if cursor_seq is not None and min_seq > 0 and cursor_seq < min_seq - 1:
            async with store.acquire() as conn:
                tail = await conn.fetchval(
                    "SELECT COALESCE(MAX(seq), 0) FROM thread_events "
                    "WHERE thread_id = $1 AND epoch = $2",
                    thread_id,
                    server_epoch,
                )
            payload = json.dumps(
                {
                    "method": "gone_beyond_horizon",
                    "params": {
                        "epoch": server_epoch,
                        "server_seq": int(tail or 0),
                        "retention_min_seq": min_seq,
                        "reason": "cursor_older_than_retention",
                    },
                }
            )
            yield f"id: {server_epoch}:0\nevent: gone_beyond_horizon\ndata: {payload}\n\n"
            return

        # Replay floor. With a cursor, resume right after it. Without one, a
        # fresh attach has already loaded completed turns from REST history, so
        # anchor past the last completed turn instead of replaying the whole
        # epoch from 0 (which doubles the last assistant turn + shows a spurious
        # "SESSION RESUMED" divider — see no_cursor_replay_start).
        if cursor_seq is not None:
            last_sent_seq = cursor_seq
        else:
            async with store.acquire() as conn:
                last_sent_seq = await no_cursor_replay_start(
                    conn, thread_id, server_epoch
                )
        empty_polls = 0
        idle_keepalive_at = 0.0
        epoch_idle = 0.0
        cancelled = False
        try:
            while not cancelled:
                if await request.is_disconnected():
                    break
                if stateless_stream and time.monotonic() >= next_presence_renew:
                    # A long-lived stream does not retain authorization from
                    # its opening handshake forever. Re-run the same BFF-cookie
                    # owner gate before every attested renewal; expiry or an
                    # ownership change closes the stream and writes no TTL.
                    renew_user, renew_thread = await dependencies.require_thread_owner(
                        request, store, thread_id
                    )
                    if renew_thread.get("execution_lane") != "stateless":
                        return
                    if track_presence and tethers_session(renew_user):
                        presence = await refresh_thread_presence(
                            store,
                            thread_id=thread_id,
                            ttl_seconds=THREAD_CLIENT_PRESENCE_TTL_S,
                            establish=False,
                        )
                        if not presence.served:
                            # Lane change/deletion: close. EventSource
                            # reconnects through require_thread_owner and
                            # current DB truth.
                            return
                    next_presence_renew = (
                        time.monotonic() + THREAD_CLIENT_PRESENCE_RENEW_S
                    )
                async with store.acquire() as conn:
                    rows = await conn.fetch(
                        "SELECT seq, kind, payload "
                        "FROM thread_events "
                        "WHERE thread_id = $1 AND epoch = $2 AND seq > $3 "
                        "ORDER BY seq ASC "
                        "LIMIT 500",
                        thread_id,
                        server_epoch,
                        last_sent_seq,
                    )
                    # Zombie-epoch guard: after enough accumulated idle time
                    # with no new rows, re-read events_epoch on the SAME
                    # connection (no extra acquire). If an agent re-attached and
                    # bumped the epoch, this generator has been polling a dead
                    # epoch — terminate deterministically so the client
                    # re-anchors, instead of feeding it pings forever.
                    if not rows and epoch_idle >= THREAD_EVENTS_EPOCH_RECHECK_S:
                        epoch_idle = 0.0
                        current_epoch = await conn.fetchval(
                            "SELECT events_epoch FROM threads WHERE id = $1",
                            thread_id,
                        )
                        if current_epoch is None:
                            # Thread deleted mid-stream — terminate silently;
                            # the client's reconnect hits require_thread_owner
                            # → 404 and it drops the thread.
                            return
                        if int(current_epoch) != server_epoch:
                            new_epoch = int(current_epoch)
                            # Anchor past the last completed turn of the NEW
                            # epoch, not its tail: the bump lands mid-turn and
                            # the client's history reload only carries completed
                            # turns, so a tail anchor would drop the in-flight
                            # turn's already-journaled frames.
                            anchor = await no_cursor_replay_start(
                                conn, thread_id, new_epoch
                            )
                            logger.info(
                                "thread_event_stream epoch bump %d→%d "
                                "(thread=%s), re-anchoring client to seq %d",
                                server_epoch,
                                new_epoch,
                                thread_id,
                                anchor,
                            )
                            payload = json.dumps(
                                {
                                    "method": "gone_beyond_horizon",
                                    "params": {
                                        "epoch": new_epoch,
                                        "server_seq": anchor,
                                        "reason": "epoch_bumped_mid_stream",
                                    },
                                }
                            )
                            # The `id:` line carries the new epoch's floor so a
                            # browser-native reconnect (bypassing the app
                            # handler) converges to the same replay start
                            # instead of replaying the new epoch from :0.
                            yield (
                                f"id: {new_epoch}:{anchor}\n"
                                f"event: gone_beyond_horizon\n"
                                f"data: {payload}\n\n"
                            )
                            return
                if rows:
                    empty_polls = 0
                    epoch_idle = 0.0
                    for row in rows:
                        seq = int(row["seq"])
                        # row["payload"] is a JSONB column — asyncpg may
                        # return it as str or already-parsed dict depending
                        # on codec registration.
                        raw_payload = row["payload"]
                        if isinstance(raw_payload, str):
                            payload_obj = json.loads(raw_payload)
                        else:
                            payload_obj = raw_payload
                        frame = {
                            "method": row["kind"],
                            "params": payload_obj,
                        }
                        body = json.dumps(frame)
                        yield f"id: {server_epoch}:{seq}\ndata: {body}\n\n"
                        last_sent_seq = seq
                    idle_keepalive_at = 0.0
                else:
                    # Adaptive backoff: 200ms × 5 empty polls, then 1s.
                    empty_polls += 1
                    wait = 1.0 if empty_polls >= 5 else 0.2
                    epoch_idle += wait
                    # Typed `ping` event every ~20s of idle. A bare `:`
                    # comment would keep the socket warm but never fire
                    # `onmessage` in the browser, leaving silent network
                    # drops undetectable client-side. A typed event with no
                    # `id:` line lets the cockpit watchdog observe liveness
                    # without advancing the replay cursor.
                    idle_keepalive_at += wait
                    if idle_keepalive_at >= 20.0:
                        yield "event: ping\ndata: {}\n\n"
                        idle_keepalive_at = 0.0
                    try:
                        await asyncio.sleep(wait)
                    except asyncio.CancelledError:
                        cancelled = True
                        break
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.warning("thread_event_stream error (thread=%s): %s", thread_id, e)
            return

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


__all__ = [
    "THREAD_CLIENT_PRESENCE_RENEW_S",
    "THREAD_CLIENT_PRESENCE_TTL_S",
    "THREAD_EVENTS_EPOCH_RECHECK_S",
    "ThreadEventStreamDependencies",
    "no_cursor_replay_start",
    "open_thread_event_stream",
]
