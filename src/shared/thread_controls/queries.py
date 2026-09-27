"""Owner-fenced reads and terminalization for ``thread_control_requests``.

Admission is orchestrator-owned because it performs user authorization and
grant checks. Consumption is runtime-owned: the exact stateless lease holder or
the exact reciprocal pinned agent binding applies the request, writes the
result through its in-process journal allocator, and only then calls the
terminalizer here.

The request has no transient ``claimed`` state. A crash before the result event
leaves it pending and safe to retry. Scalar assignments are naturally
idempotent; workspace undo carries its request UUID in the resulting Git commit
so a successor can recognize the already-applied effect. A crash after the
event commit is recovered through the unique
``thread_events.control_request_id`` receipt without journaling a second time;
the new owner re-converges any in-memory scalar before terminalization.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID


@dataclass(frozen=True, slots=True)
class ControlRequest:
    id: UUID
    thread_id: UUID
    request_seq: int
    client_request_id: UUID
    verb: str
    payload: dict[str, Any]
    accepted_agent_id: UUID | None
    runtime_generation: UUID | None


@dataclass(frozen=True, slots=True)
class ControlReceipt:
    epoch: int
    seq: int
    kind: str
    payload: dict[str, Any]


_NEXT_STATELESS_SQL = """
SELECT r.id, r.thread_id, r.request_seq, r.client_request_id, r.verb,
       r.payload, r.accepted_agent_id, r.runtime_generation
FROM thread_control_requests r
JOIN threads t ON t.id = r.thread_id
JOIN run_queue q ON q.unit_id = r.thread_id
WHERE r.thread_id = $1::uuid
  AND r.outcome IS NULL
  AND r.accepted_agent_id IS NULL
  AND t.execution_lane = 'stateless'
  AND q.unit_kind = 'session_turn'
  AND q.state = 'leased'
  AND q.lease_token = $2::bigint
ORDER BY r.request_seq
LIMIT 1
"""

_NEXT_PINNED_SQL = """
WITH oldest AS MATERIALIZED (
    SELECT request.*
    FROM thread_control_requests request
    WHERE request.thread_id = $1::uuid
      AND request.outcome IS NULL
    ORDER BY request.request_seq
    LIMIT 1
)
SELECT r.id, r.thread_id, r.request_seq, r.client_request_id, r.verb,
       r.payload, r.accepted_agent_id, r.runtime_generation
FROM oldest r
JOIN threads t ON t.id = r.thread_id
JOIN agents a ON a.id = t.agent_id AND a.thread_id = t.id
WHERE (
        r.accepted_agent_id = $2::uuid
        OR EXISTS (
            SELECT 1 FROM thread_events receipt
            WHERE receipt.thread_id = r.thread_id
              AND receipt.control_request_id = r.id
        )
      )
  AND t.execution_lane = 'pinned'
  AND t.agent_id = $2::uuid
  AND t.runtime_generation = $3::uuid
  AND t.runtime_attach_token IS NOT DISTINCT FROM $4::uuid
  AND t.runtime_retirement_token IS NULL
  AND r.runtime_generation = $3::uuid
"""

_ADOPT_PINNED_SQL = """
WITH oldest AS MATERIALIZED (
    SELECT request.id
    FROM thread_control_requests request
    WHERE request.thread_id = $1::uuid
      AND request.outcome IS NULL
      AND request.runtime_generation = $3::uuid
    ORDER BY request.request_seq
    LIMIT 1
    FOR UPDATE
)
UPDATE thread_control_requests request
SET accepted_agent_id = $2::uuid
FROM oldest,
     threads thread,
     agents agent
WHERE request.id = oldest.id
  AND request.accepted_agent_id IS DISTINCT FROM $2::uuid
  AND NOT EXISTS (
      SELECT 1 FROM thread_events receipt
      WHERE receipt.thread_id = request.thread_id
        AND receipt.control_request_id = request.id
  )
  AND thread.id = request.thread_id
  AND thread.execution_lane = 'pinned'
  AND thread.agent_id = $2::uuid
  AND thread.runtime_generation = $3::uuid
  AND thread.runtime_attach_token IS NOT DISTINCT FROM $4::uuid
  AND thread.runtime_retirement_token IS NULL
  AND agent.id = thread.agent_id
  AND agent.thread_id = thread.id
RETURNING request.id
"""

_STATELESS_FENCE_SQL = """
SELECT 1
FROM threads t
JOIN run_queue q ON q.unit_id = t.id
WHERE t.id = $1::uuid
  AND t.execution_lane = 'stateless'
  AND q.unit_kind = 'session_turn'
  AND q.state = 'leased'
  AND q.lease_token = $2::bigint
FOR SHARE OF t, q
"""

_PINNED_FENCE_SQL = """
SELECT 1
FROM threads t
JOIN agents a ON a.id = t.agent_id AND a.thread_id = t.id
WHERE t.id = $1::uuid
  AND t.execution_lane = 'pinned'
  AND t.agent_id = $2::uuid
  AND t.runtime_generation = $3::uuid
  AND t.runtime_attach_token IS NOT DISTINCT FROM $4::uuid
  AND t.runtime_retirement_token IS NULL
FOR SHARE OF t, a
"""

_RECEIPT_SQL = """
SELECT epoch, seq, kind, payload
FROM thread_events
WHERE thread_id = $1::uuid AND control_request_id = $2::uuid
"""


def _uuid(value: UUID | str) -> UUID:
    return value if isinstance(value, UUID) else UUID(str(value))


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _request(row: Any) -> ControlRequest | None:
    if row is None:
        return None
    return ControlRequest(
        id=row["id"],
        thread_id=row["thread_id"],
        request_seq=int(row["request_seq"]),
        client_request_id=row["client_request_id"],
        verb=str(row["verb"]),
        payload=_json_object(row["payload"]),
        accepted_agent_id=row["accepted_agent_id"],
        runtime_generation=row["runtime_generation"],
    )


def applied_control_scalar(
    *,
    request_id: UUID | str,
    client_request_id: UUID | str,
    request_seq: int,
    verb: str,
    request_payload: Any,
    event_kind: str,
    event_payload: Any,
) -> tuple[str, str] | None:
    """Validate an applied result and return its first-class column/value.

    Both finalization and the REST snapshot use this closed mapping. A journal
    row linked by foreign key is necessary but insufficient: version skew or a
    corrupt writer must not publish a different verb, sequence, or value.
    """

    request_payload = _json_object(request_payload)
    event_payload = _json_object(event_payload)
    requested_mode = request_payload.get("mode")
    expected_kind: str
    column: str
    allowed: set[str]
    if verb == "mode.set":
        expected_kind = "mode.changed"
        column = "permission_mode"
        allowed = {"supervised", "auto_accept", "autonomous"}
    elif verb == "narration.set":
        expected_kind = "narration.changed"
        column = "narration_mode"
        allowed = {"silent", "verbose", "auto"}
    else:
        return None

    try:
        receipt_request_seq = int(event_payload.get("request_seq"))
    except (TypeError, ValueError):
        return None
    if (
        event_kind != expected_kind
        or str(event_payload.get("request_id") or "") != str(request_id)
        or str(event_payload.get("client_request_id") or "") != str(client_request_id)
        or receipt_request_seq != int(request_seq)
        or str(event_payload.get("method") or "") != verb
        or requested_mode not in allowed
        or event_payload.get("mode") != requested_mode
    ):
        return None
    return column, str(requested_mode)


def control_receipt_result(
    *,
    request_id: UUID | str,
    client_request_id: UUID | str,
    request_seq: int,
    verb: str,
    request_payload: Any,
    event_kind: str,
    event_payload: Any,
) -> tuple[str, str | None, tuple[str, str] | None] | None:
    """Return a fully validated receipt outcome, or ``None`` on mismatch."""

    scalar = applied_control_scalar(
        request_id=request_id,
        client_request_id=client_request_id,
        request_seq=request_seq,
        verb=verb,
        request_payload=request_payload,
        event_kind=event_kind,
        event_payload=event_payload,
    )
    if scalar is not None:
        return "applied", None, scalar

    event_payload = _json_object(event_payload)
    try:
        receipt_request_seq = int(event_payload.get("request_seq"))
    except (TypeError, ValueError):
        return None
    envelope_matches = (
        str(event_payload.get("request_id") or "") == str(request_id)
        and str(event_payload.get("client_request_id") or "") == str(client_request_id)
        and receipt_request_seq == int(request_seq)
        and str(event_payload.get("method") or "") == verb
    )

    # ``workspace.undo`` is a non-scalar control effect. Its durable journal
    # receipt is still validated through the same closed verb mapping before
    # terminalization can advance the stateless control watermark. Full Git
    # object IDs are accepted for both SHA-1 and SHA-256 repositories.
    if verb == "workspace.undo":
        request_payload = _json_object(request_payload)
        paths = event_payload.get("paths")
        restored_to_sha = event_payload.get("restored_to_sha")
        restore_commit_sha = event_payload.get("restore_commit_sha")

        def _git_oid(value: Any) -> bool:
            return (
                isinstance(value, str)
                and len(value) in {40, 64}
                and all(char in "0123456789abcdef" for char in value)
            )

        def _workspace_path(value: Any) -> bool:
            if not isinstance(value, str) or not value or "\x00" in value:
                return False
            return not value.startswith("/") and ".." not in value.split("/")

        if (
            event_kind == "files.restored"
            and envelope_matches
            and request_payload == {}
            and isinstance(paths, list)
            and all(_workspace_path(path) for path in paths)
            and len(paths) == len(set(paths))
            and _git_oid(restored_to_sha)
            and _git_oid(restore_commit_sha)
        ):
            return "applied", None, None

    # ``compact`` (stateless manual compaction) is receipted by the same
    # ``context.compacted`` frame the transcript renders: a folded summary, or
    # ``summary: null`` when there was nothing to fold. The checkpoint row it
    # describes was written under the same lease before this receipt.
    if verb == "compact":
        request_payload = _json_object(request_payload)
        summary = event_payload.get("summary")
        if (
            event_kind == "context.compacted"
            and envelope_matches
            and set(request_payload) <= {"focus", "boundary_message_id"}
            and all(
                isinstance(value, str) and value for value in request_payload.values()
            )
            and event_payload.get("trigger") == "manual"
            and (summary is None or (isinstance(summary, str) and summary))
        ):
            return "applied", None, None

    error_code = str(event_payload.get("error_code") or "")
    if event_kind != "control.rejected" or not envelope_matches or not error_code:
        return None
    return "rejected", error_code, None


async def fetch_next_control_request(
    conn: Any,
    *,
    thread_id: UUID | str,
    lease_token: int | None = None,
    agent_id: UUID | str | None = None,
    runtime_generation: UUID | str | None = None,
    runtime_attach_token: UUID | str | None = None,
) -> ControlRequest | None:
    """Return the oldest pending request iff ``conn`` observes exact ownership.

    Exactly one owner credential is required. The queries deliberately do not
    use ``SKIP LOCKED``: request order is part of the user-visible semantics and
    no consumer may pass an older request merely because another task touched
    it. In-process callers serialize drains separately.
    """
    if (lease_token is None) == (agent_id is None):
        raise ValueError("exactly one of lease_token or agent_id is required")
    if lease_token is not None:
        row = await conn.fetchrow(
            _NEXT_STATELESS_SQL, _uuid(thread_id), int(lease_token)
        )
    else:
        if runtime_generation is None:
            return None
        row = await conn.fetchrow(
            _NEXT_PINNED_SQL,
            _uuid(thread_id),
            _uuid(agent_id),
            _uuid(runtime_generation),
            _uuid(runtime_attach_token) if runtime_attach_token is not None else None,
        )
    return _request(row)


async def adopt_next_pinned_control_request(
    conn: Any,
    *,
    thread_id: UUID | str,
    agent_id: UUID | str,
    runtime_generation: UUID | str,
    runtime_attach_token: UUID | str | None = None,
) -> bool:
    """Transfer the oldest unreceipted request to the exact current owner.

    A pinned pod can disappear after admission. The successor must not skip
    that request, while the predecessor must not retain authority. This one
    statement locks the oldest row, proves the new reciprocal binding, and
    changes its journal credential only when no durable receipt exists. A
    receipt stays attributed to the owner that actually wrote it and is
    recovered by the successor instead.
    """

    adopted = await conn.fetchval(
        _ADOPT_PINNED_SQL,
        _uuid(thread_id),
        _uuid(agent_id),
        _uuid(runtime_generation),
        _uuid(runtime_attach_token) if runtime_attach_token is not None else None,
    )
    return adopted is not None


async def owner_fence_current(
    conn: Any,
    *,
    thread_id: UUID | str,
    lease_token: int | None = None,
    agent_id: UUID | str | None = None,
    runtime_generation: UUID | str | None = None,
    runtime_attach_token: UUID | str | None = None,
) -> bool:
    """Check the exact current owner, taking a transaction-scoped share lock."""
    if (lease_token is None) == (agent_id is None):
        raise ValueError("exactly one of lease_token or agent_id is required")
    if lease_token is not None:
        row = await conn.fetchrow(
            _STATELESS_FENCE_SQL, _uuid(thread_id), int(lease_token)
        )
    else:
        if runtime_generation is None:
            return False
        row = await conn.fetchrow(
            _PINNED_FENCE_SQL,
            _uuid(thread_id),
            _uuid(agent_id),
            _uuid(runtime_generation),
            _uuid(runtime_attach_token) if runtime_attach_token is not None else None,
        )
    return row is not None


async def fetch_control_receipt(
    conn: Any, *, thread_id: UUID | str, request_id: UUID | str
) -> ControlReceipt | None:
    """Read the unique durable journal receipt for one request, if present."""
    row = await conn.fetchrow(_RECEIPT_SQL, _uuid(thread_id), _uuid(request_id))
    if row is None:
        return None
    return ControlReceipt(
        epoch=int(row["epoch"]),
        seq=int(row["seq"]),
        kind=str(row["kind"]),
        payload=_json_object(row["payload"]),
    )


async def finalize_control_request(
    conn: Any,
    *,
    request_id: UUID | str,
    lease_token: int | None = None,
    agent_id: UUID | str | None = None,
    runtime_generation: UUID | str | None = None,
    runtime_attach_token: UUID | str | None = None,
    outcome: str = "applied",
    error_code: str | None = None,
) -> str:
    """Terminalize one request from its durable event receipt under a fence.

    Run inside an explicit transaction. Returns ``applied``,
    ``already_terminal``, ``missing_receipt``, ``invalid_receipt``,
    ``lost_owner`` or ``watermark_gap``. For stateless requests, publishing the
    applied scalar, terminalization, and advancement of
    ``control_consumed_seq`` are the same transaction, so a snapshot or
    completion cannot observe only part of that state.
    """
    if outcome not in {"applied", "rejected"}:
        raise ValueError("outcome must be applied or rejected")
    if (lease_token is None) == (agent_id is None):
        raise ValueError("exactly one of lease_token or agent_id is required")

    rid = _uuid(request_id)
    row = await conn.fetchrow(
        "SELECT * FROM thread_control_requests WHERE id = $1 FOR UPDATE", rid
    )
    if row is None:
        return "missing_request"

    thread_id = row["thread_id"]
    # A caller can race another recovery finalizer after fetching a pending
    # request. Never let ``already_terminal`` bypass the current-owner fence:
    # the caller uses that result to decide whether it may converge live RAM.
    if row["outcome"] is not None:
        if lease_token is not None:
            if row["accepted_agent_id"] is not None:
                return "lost_owner"
            fenced = await owner_fence_current(
                conn, thread_id=thread_id, lease_token=lease_token
            )
        else:
            if row["accepted_agent_id"] is None:
                return "lost_owner"
            if runtime_generation is None or str(
                row["runtime_generation"] or ""
            ) != str(_uuid(runtime_generation)):
                return "lost_owner"
            fenced = await owner_fence_current(
                conn,
                thread_id=thread_id,
                agent_id=_uuid(agent_id),
                runtime_generation=runtime_generation,
                runtime_attach_token=runtime_attach_token,
            )
        return "already_terminal" if fenced else "lost_owner"

    receipt = await fetch_control_receipt(conn, thread_id=thread_id, request_id=rid)
    if receipt is None:
        return "missing_receipt"

    receipt_result = control_receipt_result(
        request_id=rid,
        client_request_id=row["client_request_id"],
        request_seq=int(row["request_seq"]),
        verb=str(row["verb"]),
        request_payload=row["payload"],
        event_kind=receipt.kind,
        event_payload=receipt.payload,
    )
    if receipt_result is None:
        return "invalid_receipt"
    receipt_outcome, receipt_error_code, scalar = receipt_result
    if receipt_outcome != outcome or (
        outcome == "rejected" and receipt_error_code != error_code
    ):
        return "invalid_receipt"

    if lease_token is not None:
        if row["accepted_agent_id"] is not None:
            return "lost_owner"
        fenced = await owner_fence_current(
            conn, thread_id=thread_id, lease_token=lease_token
        )
        if not fenced:
            return "lost_owner"
        queue = await conn.fetchrow(
            "SELECT control_consumed_seq FROM run_queue "
            "WHERE unit_id = $1::uuid FOR UPDATE",
            thread_id,
        )
        if (
            queue is None
            or int(queue["control_consumed_seq"] or 0) != int(row["request_seq"]) - 1
        ):
            return "watermark_gap"
        applied_lease_token: int | None = int(lease_token)
        applied_agent_id: UUID | None = None
    else:
        aid = _uuid(agent_id)
        accepted_agent_id = row["accepted_agent_id"]
        if (
            accepted_agent_id is None
            or runtime_generation is None
            or str(row["runtime_generation"] or "") != str(_uuid(runtime_generation))
        ):
            return "lost_owner"
        fenced = await owner_fence_current(
            conn,
            thread_id=thread_id,
            agent_id=aid,
            runtime_generation=runtime_generation,
            runtime_attach_token=runtime_attach_token,
        )
        if not fenced:
            return "lost_owner"
        older_pending = await conn.fetchval(
            "SELECT 1 FROM thread_control_requests "
            "WHERE thread_id = $1::uuid AND outcome IS NULL "
            "AND request_seq < $2 LIMIT 1",
            thread_id,
            int(row["request_seq"]),
        )
        if older_pending is not None:
            return "watermark_gap"
        applied_lease_token = None
        # The receipt was written under ``accepted_agent_id``. A newer exact
        # reciprocal owner may finish crash recovery, but attribution remains
        # with the owner whose fenced journal INSERT performed the action.
        applied_agent_id = accepted_agent_id

    result = json.dumps(
        {
            "event": receipt.kind,
            "params": receipt.payload,
        }
    )
    updated = await conn.fetchval(
        "UPDATE thread_control_requests SET "
        "outcome = $2, result = $3::jsonb, applied_at = now(), "
        "applied_lease_token = $4, applied_agent_id = $5, "
        "journal_epoch = $6, journal_seq = $7, acknowledged_at = now(), "
        "error_code = $8 "
        "WHERE id = $1 AND outcome IS NULL RETURNING id",
        rid,
        outcome,
        result,
        applied_lease_token,
        applied_agent_id,
        receipt.epoch,
        receipt.seq,
        error_code,
    )
    if updated is None:
        return "already_terminal"

    if scalar is not None:
        column, value = scalar
        # ``column`` comes only from the closed mapping above; values are still
        # parameters. Keep this in the receipt/fence transaction so REST state
        # never exposes admission intent as though the owner had applied it.
        await conn.execute(
            f"UPDATE threads SET {column} = $2 WHERE id = $1::uuid",
            thread_id,
            value,
        )

    if lease_token is not None:
        await conn.execute(
            "UPDATE run_queue SET control_consumed_seq = $2 WHERE unit_id = $1::uuid",
            thread_id,
            int(row["request_seq"]),
        )
    return outcome
