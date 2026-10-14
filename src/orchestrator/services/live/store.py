"""Live session persistence; knowledge-base/knowledge/features/live_session_jobs.md §4.3.

A ``live_sessions`` row is one session a live connector (a phone line)
received, bound to at most one job. Its writers are the public webhook
(insert), admission (bind, reject, fail) and the connector's pod through the
lease exchange (active, attached, ended, failed, usage, its session lease).
Every status update is monotonic: it names the statuses it may follow.

``live_session_events`` is the session's journal; ``event_id`` dedupes a
replay and is returned in every read, so the job's voice subagent can
deduplicate its backfill against the pod's live stream.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID

from shared.runtime.live.contract import (
    JOURNAL_KINDS,
    SESSION_STATUS_PREDECESSORS,
    SessionStatus,
)
from shared.runtime.live.webhook import IncomingCall, sip_uri_user

#: The session lease a pod holds while it acts for a session (renewed every
#: 10 s by the pod).
LEASE_SECONDS = 30
#: Ids of the appends the pod makes itself: adoption must not repeat them.
POD_EVENT_PREFIXES = ("srw_greeting_", "srw_fallback_", "srw_max_duration_")
SESSION_FIELDS = (
    "id connector_id provider_session_id family transport direction origin "
    "status automation_id job_id project_id model created_at accepted_at "
    "attached_at ended_at close_reason error usage_seconds"
).split()
MAX_EVENTS_PER_WRITE = 500


def _uuid(value: Any) -> UUID | None:
    return UUID(str(value)) if value else None


def record(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    result = dict(row)
    for key in ("metadata", "payload", "handoff"):
        if isinstance(result.get(key), str):
            result[key] = json.loads(result[key])
    return result


def session_view(row: dict[str, Any]) -> dict[str, Any]:
    """What the cockpit and the agent see of a session: no handoff (the
    instructions), the caller's numbers from its metadata."""
    view = {key: row.get(key) for key in SESSION_FIELDS}
    metadata = row.get("metadata") or {}
    view["from_number"] = metadata.get("from_number")
    view["to_number"] = metadata.get("to_number")
    return view


class LiveSessionStore:
    def __init__(self, db: Any):
        self.db = db

    # -- sessions -----------------------------------------------------------

    async def insert_incoming(
        self, connector_id: str, incoming: IncomingCall
    ) -> dict[str, Any] | None:
        """The webhook's ``ringing`` row; ``None`` for a duplicate delivery
        (the same session or the same webhook id)."""
        metadata = {
            "from_number": sip_uri_user(incoming.sip_header("From")),
            "to_number": sip_uri_user(incoming.sip_header("To")),
            "sip_headers": [
                {"name": name, "value": value} for name, value in incoming.sip_headers
            ],
        }
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                """INSERT INTO live_sessions(connector_id, provider_session_id,
                    webhook_id, origin, status, metadata)
                VALUES($1, $2, $3, 'webhook', 'ringing', $4::jsonb)
                ON CONFLICT DO NOTHING RETURNING *""",
                UUID(str(connector_id)),
                incoming.session_id,
                incoming.webhook_id,
                json.dumps(metadata),
            )
        return record(row)

    async def ensure_session(
        self, conn: Any, *, connector_id: str, provider_session_id: str, origin: str
    ) -> dict[str, Any]:
        """The connector's session row for ``provider_session_id``, created
        ``ringing`` when the caller (an external server, a manifest) is the
        first to name it."""
        row = await conn.fetchrow(
            """INSERT INTO live_sessions(connector_id, provider_session_id,
                origin, status)
            VALUES($1, $2, $3, 'ringing')
            ON CONFLICT (connector_id, provider_session_id) DO NOTHING
            RETURNING *""",
            UUID(str(connector_id)),
            provider_session_id,
            origin,
        )
        if row is None:
            row = await conn.fetchrow(
                "SELECT * FROM live_sessions WHERE connector_id=$1 "
                "AND provider_session_id=$2",
                UUID(str(connector_id)),
                provider_session_id,
            )
        return record(row)

    async def get(self, session_id: Any, *, job_id: Any = None) -> dict | None:
        async with self.db.acquire() as conn:
            return record(
                await conn.fetchrow(
                    "SELECT * FROM live_sessions WHERE id=$1 "
                    "AND ($2::uuid IS NULL OR job_id=$2)",
                    UUID(str(session_id)),
                    _uuid(job_id),
                )
            )

    async def job_sessions(self, job_id: Any) -> list[dict[str, Any]]:
        async with self.db.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM live_sessions WHERE job_id=$1 ORDER BY created_at, id",
                UUID(str(job_id)),
            )
        return [session_view(record(row)) for row in rows]

    async def bind_job(self, conn: Any, session_id: Any, job_id: Any) -> bool:
        """One session, one job: binds only an unbound session."""
        return bool(
            await conn.fetchval(
                """UPDATE live_sessions SET job_id=$2
                WHERE id=$1 AND job_id IS NULL RETURNING id""",
                UUID(str(session_id)),
                UUID(str(job_id)),
            )
        )

    async def note_admission(
        self,
        session_id: Any,
        *,
        automation_id: Any = None,
        project_id: Any = None,
        model: str | None = None,
        handoff: dict[str, Any] | None = None,
    ) -> None:
        """What admission decided: the automation that fired, the project,
        the engine model and the handoff (the pod's adoption input; no
        secrets)."""
        async with self.db.acquire() as conn:
            await conn.execute(
                """UPDATE live_sessions SET
                    automation_id=COALESCE($2, automation_id),
                    project_id=COALESCE($3, project_id),
                    model=COALESCE($4, model),
                    handoff=CASE WHEN $5::jsonb IS NULL THEN handoff ELSE $5::jsonb END
                WHERE id=$1""",
                UUID(str(session_id)),
                _uuid(automation_id),
                _uuid(project_id),
                model,
                json.dumps(handoff) if handoff is not None else None,
            )

    async def transition(
        self,
        session_id: Any,
        status: str,
        *,
        accepted_at: datetime | None = None,
        attached_at: datetime | None = None,
        ended_at: datetime | None = None,
        close_reason: str | None = None,
        usage_seconds: float | None = None,
        error: str | None = None,
    ) -> tuple[bool, dict[str, Any] | None]:
        """A monotonic status update: ``(applied, row)``.

        ``active`` on an active session records its first ``attached_at``
        only (the job's voice subagent reached the pod). A status that may
        not follow the current one is not applied; the row is returned as
        it is.
        """
        status = SessionStatus(status)
        sid = UUID(str(session_id))
        async with self.db.acquire() as conn:
            if status == SessionStatus.ACTIVE:
                row = await conn.fetchrow(
                    """UPDATE live_sessions SET status='active',
                        accepted_at=COALESCE(accepted_at, $2, now()),
                        attached_at=COALESCE(attached_at, $3)
                    WHERE id=$1 AND status IN ('ringing', 'active')
                    RETURNING *""",
                    sid,
                    accepted_at,
                    attached_at,
                )
            else:
                predecessors = sorted(SESSION_STATUS_PREDECESSORS[status])
                row = await conn.fetchrow(
                    """UPDATE live_sessions SET status=$2,
                        ended_at=COALESCE($3, now()),
                        close_reason=COALESCE($4, close_reason),
                        usage_seconds=COALESCE($5, usage_seconds),
                        error=COALESCE($6, error),
                        owner_identity_id=NULL, lease_until=NULL
                    WHERE id=$1 AND status = ANY($7::text[])
                    RETURNING *""",
                    sid,
                    str(status),
                    ended_at,
                    close_reason,
                    usage_seconds,
                    error[:1000] if error else None,
                    predecessors,
                )
            if row is not None:
                return True, record(row)
            current = await conn.fetchrow(
                "SELECT * FROM live_sessions WHERE id=$1", sid
            )
        return False, record(current)

    # -- the pod's session lease ----------------------------------------------

    async def lease(
        self, session_id: Any, identity_id: Any, *, release: bool = False
    ) -> tuple[bool, datetime | None]:
        """Claim or renew (or release) the pod's lease on a live session.
        Only the holder acts for the session."""
        sid, owner = UUID(str(session_id)), UUID(str(identity_id))
        async with self.db.acquire() as conn:
            if release:
                await conn.execute(
                    """UPDATE live_sessions SET owner_identity_id=NULL,
                        lease_until=NULL WHERE id=$1 AND owner_identity_id=$2""",
                    sid,
                    owner,
                )
                return False, None
            until = await conn.fetchval(
                f"""UPDATE live_sessions SET owner_identity_id=$2,
                    lease_until=now() + interval '{LEASE_SECONDS} seconds'
                WHERE id=$1 AND status IN ('ringing', 'active')
                  AND (owner_identity_id IS NULL OR owner_identity_id=$2
                       OR lease_until IS NULL OR lease_until < now())
                RETURNING lease_until""",
                sid,
                owner,
            )
        return until is not None, until

    async def adoptable(self, connector_id: Any, identity_id: Any) -> list[dict]:
        """The connector's active sessions no other pod holds: a restarted
        or superseding pod re-attaches them."""
        async with self.db.acquire() as conn:
            rows = await conn.fetch(
                """SELECT * FROM live_sessions
                WHERE connector_id=$1 AND status='active'
                  AND (owner_identity_id IS NULL OR owner_identity_id=$2
                       OR lease_until IS NULL OR lease_until < now())
                ORDER BY created_at, id""",
                UUID(str(connector_id)),
                UUID(str(identity_id)),
            )
            sessions = [record(row) for row in rows]
            sent: dict[UUID, list[str]] = {}
            if sessions:
                marks = await conn.fetch(
                    """SELECT session_id, event_id FROM live_session_events
                    WHERE session_id = ANY($1::uuid[]) AND event_id IS NOT NULL
                      AND (event_id LIKE 'srw\\_greeting\\_%'
                           OR event_id LIKE 'srw\\_fallback\\_%'
                           OR event_id LIKE 'srw\\_max\\_duration\\_%')""",
                    [row["id"] for row in sessions],
                )
                for mark in marks:
                    sent.setdefault(mark["session_id"], []).append(mark["event_id"])
        for session in sessions:
            session["sent_event_ids"] = sorted(sent.get(session["id"], []))
        return sessions

    # -- the journal ---------------------------------------------------------

    async def events(
        self, session_id: Any, after_id: int = 0, limit: int = 500
    ) -> list[dict[str, Any]]:
        async with self.db.acquire() as conn:
            rows = await conn.fetch(
                """SELECT id, event_id, kind, speaker, text, start_ms, end_ms,
                    delegation_id, payload, created_at
                FROM live_session_events WHERE session_id=$1 AND id>$2
                ORDER BY id LIMIT $3""",
                UUID(str(session_id)),
                after_id,
                limit,
            )
        return [record(row) for row in rows]

    async def append_event(self, conn: Any, session_id: Any, event: dict) -> bool:
        if event["kind"] not in JOURNAL_KINDS:
            raise ValueError("Unsupported live session event kind")
        return bool(
            await conn.fetchval(
                """INSERT INTO live_session_events(session_id, event_id, kind,
                    speaker, text, start_ms, end_ms, delegation_id, payload)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)
                ON CONFLICT (session_id, event_id) WHERE event_id IS NOT NULL
                DO NOTHING RETURNING id""",
                UUID(str(session_id)),
                event.get("event_id"),
                event["kind"],
                event.get("speaker"),
                event.get("text"),
                event.get("start_ms"),
                event.get("end_ms"),
                event.get("delegation_id"),
                json.dumps(event.get("payload") or {}),
            )
        )

    async def write_events(self, session_id: Any, events: list[dict]) -> int:
        if len(events) > MAX_EVENTS_PER_WRITE:
            raise ValueError("Too many live session events in one write")
        async with self.db.acquire() as conn, conn.transaction():
            exists = await conn.fetchval(
                "SELECT id FROM live_sessions WHERE id=$1 FOR KEY SHARE",
                UUID(str(session_id)),
            )
            if not exists:
                raise LookupError("Live session not found")
            inserted = 0
            for event in events:
                inserted += await self.append_event(conn, session_id, event)
            return inserted
