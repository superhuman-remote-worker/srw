"""Durable, transport-independent persistent-session state snapshots.

The legacy ``session.state`` welcome frame is assembled from agent-process
memory and sent directly over the per-session WebSocket.  A stateless thread
has no stable pod to query, so the orchestrator's REST contract is deliberately
DB-authoritative instead: one repeatable-read snapshot of the current journal
epoch, transcript, and permission rows.

Journal persistence is asynchronous and bounded with respect to the agent's
in-memory callbacks.  Consequently, ``turn_in_flight`` and ``running_tool``
mean "durably observed as of ``event_cursor``", not "agent RAM right now".  The
Cockpit applies this snapshot before opening SSE, then replays from the returned
``replay_cursor`` just before the latest surviving turn boundary.  That rebuilds
the turn consistently even when REST history raced its completion, while a
frame still waiting in the ordered writer advances the snapshot in wire order.
A completed stateless queue row also clears a stranded journal-open edge if a
terminal frame was dropped.  This is the freshness contract that makes the read
lane-agnostic without inventing pod routing from ``run_queue.leased_by`` (a
diagnostic field, not a reachable address or ownership credential).  During
coexistence, pinned sessions still receive the later exact in-memory welcome
frame over their WebSocket.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any, Awaitable, Callable

from shared.thread_controls import applied_control_scalar


_TURN_LIFECYCLE_KINDS = (
    "turn.started",
    "turn.completed",
    "turn.error",
    "turn.interrupted",
    "turn.parked",
    "ready",
)

_TURN_TERMINAL_KINDS = (
    "turn.completed",
    "turn.error",
    "turn.interrupted",
    "turn.parked",
    "session.ended",
    "ready",
)

_TOOL_CLEAR_KINDS = (*_TURN_TERMINAL_KINDS, "turn.started")


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


def _temperature(value: Any) -> float | int | None:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (float, int)) and not isinstance(value, bool):
        return value
    return None


def _integer(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _iso_timestamp(value: Any) -> str | None:
    """Serialize a PostgreSQL timestamp like ``SessionTask.to_dict``."""

    if value is None:
        return None
    if isinstance(value, str):
        return value
    isoformat = getattr(value, "isoformat", None)
    return isoformat() if callable(isoformat) else str(value)


def _usage_snapshot(rows: list[Any]) -> dict[str, Any] | None:
    """Rebuild the Cockpit usage panel from one turn's ``usage.updated`` frames.

    ``rows`` are the frames of the latest turn that carried usage, in seq order
    (see the query in ``build_session_state_snapshot``).  The aggregation rule
    mirrors the Cockpit handler exactly, because the two have to agree: input
    and the limits are *latest wins* (sticky — a frame that omits one keeps the
    previous value), while output and reasoning **accumulate across the turn's
    calls**, and ``reasoning_estimated`` is sticky-true once any call estimated.

    Restoring only the newest frame would under-report a tool-using turn's
    output by every call but the last, which is why this aggregates rather than
    just reading the tail.
    """

    payloads = [p for p in (_json_object(row["payload"]) for row in rows) if p]
    if not payloads:
        return None

    def latest(key: str) -> int | None:
        for payload in reversed(payloads):
            value = _integer(payload.get(key))
            if value is not None:
                return value
        return None

    output_tokens = 0
    reasoning_tokens = 0
    reasoning_estimated = False
    for payload in payloads:
        output_tokens += _integer(payload.get("output_tokens")) or 0
        reasoning_tokens += _integer(payload.get("reasoning_tokens")) or 0
        reasoning_estimated = reasoning_estimated or bool(
            payload.get("reasoning_estimated")
        )

    return {
        "turn": latest("turn"),
        "input_tokens": latest("input_tokens"),
        "output_tokens": output_tokens,
        "reasoning_tokens": reasoning_tokens,
        "reasoning_estimated": reasoning_estimated,
        "ctx_limit_tokens": latest("ctx_limit_tokens"),
        "compaction_threshold_tokens": latest("compaction_threshold_tokens"),
    }


def _session_task(row: Any) -> dict[str, Any]:
    """Map one migration-0133 row onto the stable Cockpit task shape."""

    source = dict(row)
    return {
        "id": f"task_{int(source['task_number'])}",
        "description": str(source.get("description") or ""),
        "status": str(source.get("status") or "pending"),
        "priority": str(source.get("priority") or "medium"),
        "notes": str(source.get("notes") or ""),
        "created_at": _iso_timestamp(source.get("created_at")),
        "completed_at": _iso_timestamp(source.get("completed_at")),
    }


async def build_session_state_snapshot(
    db: Any,
    thread_id: str,
    *,
    resolved_config: dict[str, Any] | None = None,
    config_resolver: Callable[
        [dict[str, Any], dict[str, Any]], Awaitable[dict[str, Any] | None]
    ]
    | None = None,
) -> dict[str, Any] | None:
    """Return the durable ``session.state`` params shape for ``thread_id``.

    ``None`` means the thread disappeared after the caller's ownership check.
    ``resolved_config`` is a test/convenience seam for safe display fields that
    are not first-class columns yet. Production passes ``config_resolver``:
    it runs after the repeatable-read transaction against the exact thread row
    captured with ``event_cursor``. A later config write is therefore replayed
    above that cursor instead of being hidden beneath scalars from a different
    revision. Secrets and the resolved blob itself never enter the result.
    """

    if resolved_config is not None and config_resolver is not None:
        raise ValueError("pass resolved_config or config_resolver, not both")

    async with db.acquire() as conn:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            thread = await conn.fetchrow(
                """
                SELECT thread.*, queue.state AS queue_state
                FROM threads AS thread
                LEFT JOIN run_queue AS queue
                  ON queue.unit_id = thread.id
                 AND queue.unit_kind = 'session_turn'
                WHERE thread.id = $1
                """,
                thread_id,
            )
            if thread is None:
                return None
            thread_source = dict(thread)

            epoch = int(thread["events_epoch"] or 0)
            hwm = int(thread_source.get("events_seq_hwm") or 0)
            message_count = int(
                await conn.fetchval(
                    "SELECT COUNT(*) FROM thread_messages "
                    "WHERE thread_id = $1 AND rewound_at IS NULL",
                    thread_id,
                )
                or 0
            )
            # Input rows are durable before execution. Multiple queued humans
            # can therefore carry numbers ahead of the turn the loop is
            # currently serving. Only execution-produced rows are a safe
            # fallback when this epoch has no turn lifecycle event.
            persisted_turn_count = int(
                await conn.fetchval(
                    "SELECT COALESCE(MAX(turn_number), 0) "
                    "FROM thread_messages "
                    "WHERE thread_id = $1 AND rewound_at IS NULL "
                    "AND role NOT IN ('human', 'event')",
                    thread_id,
                )
                or 0
            )
            lifecycle = await conn.fetchrow(
                """
                SELECT
                    (
                        SELECT event.kind
                        FROM thread_events AS event
                        WHERE event.thread_id = $1
                          AND event.epoch = $2
                          AND event.kind = ANY($3::text[])
                        ORDER BY event.seq DESC
                        LIMIT 1
                    ) AS latest_kind,
                    (
                        SELECT event.payload->>'turn_id'
                        FROM thread_events AS event
                        WHERE event.thread_id = $1
                          AND event.epoch = $2
                          AND event.kind IN (
                              'turn.started', 'turn.completed', 'turn.error',
                              'turn.interrupted', 'turn.parked'
                          )
                          AND event.payload ? 'turn_id'
                        ORDER BY event.seq DESC
                        LIMIT 1
                    ) AS latest_turn_id,
                    (
                        SELECT event.seq
                        FROM thread_events AS event
                        WHERE event.thread_id = $1
                          AND event.epoch = $2
                          AND event.kind = 'turn.started'
                        ORDER BY event.seq DESC
                        LIMIT 1
                    ) AS latest_turn_start_seq
                """,
                thread_id,
                epoch,
                list(_TURN_LIFECYCLE_KINDS),
            )
            running_row = await conn.fetchrow(
                """
                SELECT started.payload
                FROM thread_events AS started
                WHERE started.thread_id = $1
                  AND started.epoch = $2
                  AND started.kind = 'tool.started'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM thread_events AS later
                      WHERE later.thread_id = started.thread_id
                        AND later.epoch = started.epoch
                        AND later.seq > started.seq
                        AND (
                            later.kind = ANY($3::text[])
                            OR (
                                later.kind = 'tool.completed'
                                AND later.payload->>'id' = started.payload->>'id'
                            )
                        )
                  )
                ORDER BY started.seq DESC
                LIMIT 1
                """,
                thread_id,
                epoch,
                list(_TOOL_CLEAR_KINDS),
            )
            permission_rows = await conn.fetch(
                """
                SELECT id, tool_call_id, tool_name, tool_args
                FROM thread_permission_requests
                WHERE thread_id = $1 AND status = 'pending'
                ORDER BY requested_at ASC, id ASC
                """,
                thread_id,
            )
            task_rows = await conn.fetch(
                """
                SELECT task_number, description, status, priority, notes,
                       created_at, completed_at
                FROM thread_session_tasks
                WHERE thread_id = $1
                ORDER BY task_number ASC
                """,
                thread_id,
            )
            # Token telemetry for the composer's usage panel: every frame of
            # the newest turn that reported usage, oldest first.
            #
            # Bounded by the same ``hwm`` that becomes ``event_cursor``, so
            # "aggregated here" and the client's ``coveredBySnapshot`` test
            # partition the journal on the identical seq. Frames at or below it
            # are folded in below and the client drops them from replay; frames
            # above it are replayed and accumulate on top. Without that exact
            # agreement the latest turn's output would be counted twice.
            usage_rows = await conn.fetch(
                """
                WITH latest AS (
                    SELECT event.payload->>'turn' AS turn
                    FROM thread_events AS event
                    WHERE event.thread_id = $1
                      AND event.epoch = $2
                      AND event.kind = 'usage.updated'
                      AND event.seq <= $3
                    ORDER BY event.seq DESC
                    LIMIT 1
                )
                SELECT event.payload
                FROM thread_events AS event, latest
                WHERE event.thread_id = $1
                  AND event.epoch = $2
                  AND event.kind = 'usage.updated'
                  AND event.seq <= $3
                  AND event.payload->>'turn' IS NOT DISTINCT FROM latest.turn
                ORDER BY event.seq ASC
                """,
                thread_id,
                epoch,
                hwm,
            )
            # Normally owner finalization has already copied each applied
            # control into the first-class thread scalar. There is one
            # intentional crash window after the owner-fenced result event
            # commits and before that finalization transaction. Fold only
            # matching, still-pending receipts into this same repeatable-read
            # snapshot. Finalized historical events must never overwrite a
            # newer scalar, and malformed/version-skewed receipts fail closed.
            pending_control_receipts = await conn.fetch(
                """
                SELECT request.id AS request_id,
                       request.client_request_id,
                       request.request_seq,
                       request.verb,
                       request.payload AS request_payload,
                       event.kind AS event_kind,
                       event.payload AS event_payload
                FROM thread_control_requests AS request
                JOIN thread_events AS event
                  ON event.thread_id = request.thread_id
                 AND event.control_request_id = request.id
                WHERE request.thread_id = $1
                  AND request.outcome IS NULL
                ORDER BY request.request_seq ASC
                """,
                thread_id,
            )

    metadata = _json_object(thread_source.get("metadata"))
    if config_resolver is not None:
        resolved_config = await config_resolver(thread_source, metadata)

    stored_override = _json_object(metadata.get("config_override"))
    stored_interactive = _json_object(stored_override.get("interactive"))
    stored_llm = _json_object(stored_override.get("llm"))
    resolved = _json_object(resolved_config)
    # ``serialize_resolved_config`` wraps the actual AgentConfig beneath
    # ``agent``; reading top-level ``llm``/``interactive`` would silently turn
    # a resolved model into the config filename (usually ``session_base``).
    resolved_agent = _json_object(resolved.get("agent"))
    resolved_interactive = _json_object(resolved_agent.get("interactive"))
    resolved_llm = _json_object(resolved_agent.get("llm"))

    queue_state = thread_source.get("queue_state")
    # run_queue rows live as long as their unit and can outlive an operator's
    # detached lane transition. Only the stateless lane may use its queue row
    # to correct a dropped terminal journal edge; pinned/unknown lanes trust
    # the journal (and pinned gets the exact WS state immediately afterward).
    queue_proves_idle = thread_source.get(
        "execution_lane"
    ) == "stateless" and queue_state in {
        "queued",
        "done",
        "parked",
    }
    # Whitelist states in which the runtime is allowed to exist. Legacy
    # ``idle`` is terminal-equivalent, and an unknown future/corrupt state must
    # not resurrect a stale journal-open edge.
    status_proves_idle = thread_source.get("status") not in {
        "created",
        "active",
        "awaiting_user",
    }
    runtime_proves_idle = queue_proves_idle or status_proves_idle

    running_tool: dict[str, Any] | None = None
    if running_row is not None and not runtime_proves_idle:
        payload = _json_object(running_row["payload"])
        if payload.get("tool"):
            running_tool = {
                "id": str(payload.get("id") or ""),
                "tool": str(payload["tool"]),
                "args": _json_object(payload.get("args")),
            }

    pending_permissions: list[dict[str, Any]] = []
    for row in permission_rows:
        pending_permissions.append(
            {
                "id": str(row["tool_call_id"] or ""),
                "approval_id": str(row["id"]),
                "tool": str(row["tool_name"] or ""),
                "args": _json_object(row["tool_args"]),
            }
        )

    permission_mode = (
        thread_source.get("permission_mode")
        or resolved_interactive.get("permission_mode")
        or stored_interactive.get("permission_mode")
        or "supervised"
    )
    narration_mode = (
        thread_source.get("narration_mode")
        or resolved_interactive.get("narration_mode")
        or stored_interactive.get("narration_mode")
        or "auto"
    )
    for receipt in pending_control_receipts:
        scalar = applied_control_scalar(
            request_id=receipt["request_id"],
            client_request_id=receipt["client_request_id"],
            request_seq=int(receipt["request_seq"]),
            verb=str(receipt["verb"]),
            request_payload=receipt["request_payload"],
            event_kind=str(receipt["event_kind"]),
            event_payload=receipt["event_payload"],
        )
        if scalar is None:
            continue
        column, value = scalar
        if column == "permission_mode":
            permission_mode = value
        elif column == "narration_mode":
            narration_mode = value
    model = resolved_llm.get("model") or stored_llm.get("model")
    temperature = _temperature(
        resolved_llm.get("temperature", stored_llm.get("temperature"))
    )
    latest_kind = lifecycle["latest_kind"] if lifecycle is not None else None
    latest_turn_id = (
        _integer(lifecycle["latest_turn_id"]) if lifecycle is not None else None
    )
    turn_count = latest_turn_id if latest_turn_id is not None else persisted_turn_count
    latest_turn_start_seq = (
        _integer(lifecycle["latest_turn_start_seq"]) if lifecycle is not None else None
    )
    # Exclusive floor for reconstructing the latest turn. It closes both a
    # cached-cursor-in-the-middle-of-a-token-stream gap and the race where a
    # turn finishes after REST history but before SSE chooses its floor.
    replay_seq = (
        hwm
        if latest_kind == "turn.started" and runtime_proves_idle
        else (max(0, latest_turn_start_seq - 1) if latest_turn_start_seq else hwm)
    )

    return {
        "thread_id": str(thread_source["id"]),
        "conversation_revision": int(thread_source.get("conversation_revision") or 0),
        "permission_mode": str(permission_mode),
        "narration_mode": str(narration_mode),
        "turn_count": turn_count,
        "turn_in_flight": bool(
            latest_kind == "turn.started" and not runtime_proves_idle
        ),
        "message_count": message_count,
        "model": str(model) if model is not None else None,
        "temperature": temperature,
        "running_tool": running_tool,
        "pending_permissions": pending_permissions,
        "tasks": [_session_task(row) for row in task_rows],
        # Presence-authoritative, including an explicit null: a thread that has
        # never reported usage actively clears whatever the panel was showing,
        # which is the second, independent kill for the cross-session leak in
        # knowledge-history/done/session_usage_panel_leaks_previous_session_counters.md.
        "usage": _usage_snapshot(list(usage_rows)),
        "event_cursor": {
            "epoch": epoch,
            "seq": hwm,
        },
        "replay_cursor": {
            "epoch": epoch,
            "seq": replay_seq,
        },
        "snapshot_source": "durable_journal",
    }


__all__ = ["build_session_state_snapshot"]
