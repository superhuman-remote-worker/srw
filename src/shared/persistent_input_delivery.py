"""Durable execution authority for persistent-session input.

``thread_messages`` is the transcript, not an inbox: restore presents those
rows as context and never schedules them.  This module keeps the smallest
separate state needed to make a persisted input reclaimable and to prove when
its one paid turn crossed provider admission.

Pinned mutators use ``thread -> agent -> delivery``; stateless mutators use
``thread -> run_queue -> delivery``. Runtime identities and queue leases are
server-issued observations; no model-visible tool schema contains them.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid5


_THREAD_MESSAGE_ID_NAMESPACE = UUID("4b9d8f7e-2c3a-5d6b-8e1f-0a1b2c3d4e5f")


class InputDeliveryAuthorityLost(RuntimeError):
    """The caller is not the exact current delivery authority."""


class InputDeliveryConflict(RuntimeError):
    """A stable delivery identity was reused for different input."""


def raw_message_id(delivery_id: str | UUID) -> str:
    return f"msg_delivery_{UUID(str(delivery_id)).hex}"


def message_row_id(delivery_id: str | UUID) -> UUID:
    return uuid5(_THREAD_MESSAGE_ID_NAMESPACE, raw_message_id(delivery_id))


def _dict(row: Any) -> dict[str, Any]:
    return dict(row) if row is not None else {}


async def lock_runtime_authority(
    conn: Any,
    *,
    thread_id: str | UUID,
    agent_id: str | UUID,
    pod_uid: str,
    session_runtime_generation: str | UUID,
    runtime_attach_token: str | UUID,
) -> dict[str, Any]:
    """Lock and prove exact reciprocal thread/agent/pod authority."""

    thread_uuid = UUID(str(thread_id))
    agent_uuid = UUID(str(agent_id))
    runtime_uuid = UUID(str(session_runtime_generation))
    attach_uuid = UUID(str(runtime_attach_token))
    pod = str(pod_uid or "").strip()
    if not pod:
        raise InputDeliveryAuthorityLost("runtime pod identity is unavailable")

    thread = await conn.fetchrow(
        "SELECT id, agent_id, status, execution_lane, runtime_generation, "
        "runtime_attach_token, runtime_retirement_token, user_id, total_turns, "
        "conversation_revision "
        "FROM threads "
        "WHERE id = $1 FOR UPDATE",
        thread_uuid,
    )
    if (
        thread is None
        or str(thread["agent_id"] or "") != str(agent_uuid)
        or str(thread["execution_lane"] or "pinned") == "stateless"
        or str(thread["status"] or "") in {"ended", "suspended"}
        or str(thread["runtime_generation"] or "") != str(runtime_uuid)
        or str(thread["runtime_attach_token"] or "") != str(attach_uuid)
        or thread["runtime_retirement_token"] is not None
    ):
        raise InputDeliveryAuthorityLost("thread runtime authority was lost")

    agent = await conn.fetchrow(
        "SELECT id, thread_id, pod_uid, status FROM agents WHERE id = $1 FOR SHARE",
        agent_uuid,
    )
    if (
        agent is None
        or str(agent["thread_id"] or "") != str(thread_uuid)
        or str(agent["pod_uid"] or "") != pod
        or str(agent["status"] or "") in {"offline", "deleted"}
    ):
        raise InputDeliveryAuthorityLost("agent runtime authority was lost")
    return _dict(thread)


async def _lock_stateless_runtime_authority(
    conn: Any,
    *,
    thread_id: str | UUID,
    lease_token: int,
    executor_id: str,
    pod_uid: str,
    for_update: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Lock and prove one exact stateless ``session_turn`` claimant.

    Lock order is ``thread -> run_queue``. The delivery row is locked by the
    caller only after this helper returns, matching orchestrator-side event
    admission (``thread -> run_queue -> delivery``).
    """

    thread_uuid = UUID(str(thread_id))
    executor = str(executor_id or "").strip()
    pod = str(pod_uid or "").strip()
    if int(lease_token) <= 0 or not executor or not pod:
        raise InputDeliveryAuthorityLost("stateless runtime identity is incomplete")
    lock_clause = "FOR UPDATE" if for_update else "FOR SHARE"
    thread = await conn.fetchrow(
        "SELECT id, agent_id, status, execution_lane, user_id, total_turns, "
        "metadata, conversation_revision "
        f"FROM threads WHERE id = $1 {lock_clause}",
        thread_uuid,
    )
    if (
        thread is None
        or str(thread["execution_lane"] or "") != "stateless"
        or thread["agent_id"] is not None
        or str(thread["status"] or "") in {"ended", "suspended"}
    ):
        raise InputDeliveryAuthorityLost("stateless thread authority was lost")
    # Queue rows identify a pod by its reusable name.  The credential bundle
    # additionally stamps the immutable Kubernetes UID in thread metadata;
    # require the exact three-part claim so a restarted pod with the same name
    # cannot inherit another incarnation's DB mutation authority.
    from shared.session_retirement import active_claim_authority

    try:
        active_claim = active_claim_authority(thread["metadata"])
    except RuntimeError as exc:
        raise InputDeliveryAuthorityLost("stateless active claim is malformed") from exc
    if (
        active_claim is None
        or int(active_claim[0]) != int(lease_token)
        or active_claim[1].pod != executor
        or active_claim[1].pod_uid != pod
    ):
        raise InputDeliveryAuthorityLost("stateless pod incarnation was lost")
    queue = await conn.fetchrow(
        "SELECT unit_id, unit_kind, state, lease_token, leased_by, "
        "input_delivery_capable_lease_token FROM run_queue "
        f"WHERE unit_id = $1 {lock_clause}",
        thread_uuid,
    )
    if (
        queue is None
        or str(queue["unit_kind"] or "") != "session_turn"
        or str(queue["state"] or "") != "leased"
        or int(queue["lease_token"] or 0) != int(lease_token)
        or str(queue["leased_by"] or "") != executor
        or int(queue["input_delivery_capable_lease_token"] or 0) != int(lease_token)
    ):
        raise InputDeliveryAuthorityLost("stateless queue lease was lost")
    return _dict(thread), _dict(queue)


async def persist_input_delivery(
    conn: Any,
    *,
    thread_id: str | UUID,
    delivery_id: str | UUID,
    role: str,
    content: str,
    source: str,
    turn_number: int | None,
    agent_id: str | UUID | None = None,
    pod_uid: str | None = None,
    runtime_generation: str | UUID | None = None,
    session_runtime_generation: str | UUID | None = None,
    runtime_attach_token: str | UUID | None = None,
    allow_stateless_subagent_event: bool = False,
    supersedes_input_seq: int | None = None,
) -> dict[str, Any]:
    """Atomically persist one transcript row and optionally claim execution.

    With no runtime identity this is an orchestrator-side durable persist only.
    With all three identity fields it also establishes/recovers the exact
    process claim.  Partial identity is always rejected.
    """

    delivery_uuid = UUID(str(delivery_id))
    thread_uuid = UUID(str(thread_id))
    row_id = message_row_id(delivery_uuid)
    source_value = str(source or "unknown")[:80]
    if isinstance(supersedes_input_seq, bool) or (
        supersedes_input_seq is not None
        and (not isinstance(supersedes_input_seq, int) or supersedes_input_seq <= 0)
    ):
        raise InputDeliveryConflict("superseded input sequence is invalid")
    session_runtime_generation = session_runtime_generation or runtime_generation
    identity = (
        agent_id,
        pod_uid,
        runtime_generation,
        session_runtime_generation,
        runtime_attach_token,
    )
    has_identity = all(value is not None for value in identity)
    if any(value is not None for value in identity) and not has_identity:
        raise InputDeliveryAuthorityLost("incomplete runtime identity")

    # Lock the parent before the delivery row in every path.  Do not require
    # live runtime ownership yet: an admitted/settled delivery is an immutable
    # historical receipt whose response may have been lost immediately before
    # End cleared the live binding.
    thread_row = await conn.fetchrow(
        "SELECT id, agent_id, status, execution_lane, runtime_generation, "
        "runtime_attach_token, runtime_retirement_token, user_id, total_turns, "
        "conversation_revision "
        "FROM threads WHERE id = $1 FOR UPDATE",
        thread_uuid,
    )
    if thread_row is None:
        raise InputDeliveryAuthorityLost("thread no longer exists")
    thread = _dict(thread_row)

    execution_lane = str(thread.get("execution_lane") or "pinned")
    if execution_lane not in {"pinned", "stateless"}:
        raise InputDeliveryAuthorityLost("thread execution lane is unsupported")

    # A provider-admitted delivery is an immutable execution receipt. Its
    # outbox response may have been lost and the thread may legitimately move
    # lanes before the stable retry arrives. Resolve that retry against the
    # historical ledger lane/identity, not the thread's new lane, and return
    # without touching either queue. Pending rows deliberately stay on the
    # current-lane path below so a lane mismatch cannot re-arm or launder them.
    terminal_replay = await conn.fetchrow(
        "SELECT delivery.*, message.seq, message.thread_id AS message_thread_id, "
        "message.role, message.content, message.turn_number, message.rewound_at "
        "FROM thread_input_deliveries AS delivery "
        "JOIN thread_messages AS message ON message.id=delivery.message_id "
        "WHERE delivery.delivery_id=$1 "
        "AND delivery.state IN ('admitted','settled') "
        "FOR UPDATE OF delivery",
        delivery_uuid,
    )
    if terminal_replay is not None:
        if (
            str(terminal_replay["thread_id"]) != str(thread_uuid)
            or str(terminal_replay["message_thread_id"]) != str(thread_uuid)
            or str(terminal_replay["message_id"]) != str(row_id)
            or str(terminal_replay["source"]) != source_value
            or str(terminal_replay["role"]) != str(role)
            or str(terminal_replay["execution_lane"] or "")
            not in {"pinned", "stateless"}
            or terminal_replay.get("supersedes_input_seq") != supersedes_input_seq
            or (
                turn_number is not None
                and terminal_replay.get("turn_number") != turn_number
            )
        ):
            raise InputDeliveryConflict(
                "stable input identity conflicts with terminal delivery"
            )
        stored_content = str(terminal_replay["content"] or "")
        if stored_content != str(content) and source_value != "officer_wake":
            raise InputDeliveryConflict(
                "stable input identity conflicts with transcript"
            )
        if has_identity and (
            str(terminal_replay["execution_lane"] or "") != "pinned"
            or str(terminal_replay["owner_agent_id"] or "") != str(agent_id)
            or str(terminal_replay["owner_pod_uid"] or "") != str(pod_uid)
            or str(terminal_replay["owner_runtime_generation"] or "")
            != str(runtime_generation)
        ):
            raise InputDeliveryAuthorityLost(
                "terminal delivery belongs to another runtime"
            )
        result = _dict(terminal_replay)
        result.update(
            {
                "message_id": str(row_id),
                "message_row_id": str(row_id),
                "seq": int(terminal_replay["seq"]),
                "transcript_inserted": False,
                "content": stored_content,
                "role": str(terminal_replay["role"]),
                "turn_number": terminal_replay["turn_number"],
                # This is provenance, not the thread's newly selected lane.
                "execution_lane": str(terminal_replay["execution_lane"]),
                "queue_state": None,
                "execution_disposition": (
                    "historical"
                    if terminal_replay["rewound_at"] is not None
                    else "current"
                ),
            }
        )
        return result

    historical = await conn.fetchrow(
        "SELECT delivery.*, message.seq, message.thread_id AS message_thread_id, "
        "message.role, message.content, message.turn_number, message.rewound_at "
        "FROM thread_input_deliveries AS delivery "
        "JOIN thread_messages AS message ON message.id=delivery.message_id "
        "WHERE delivery.delivery_id=$1 FOR UPDATE OF delivery",
        delivery_uuid,
    )
    if historical is not None and historical["rewound_at"] is not None:
        if (
            str(historical["thread_id"]) != str(thread_uuid)
            or str(historical["message_thread_id"]) != str(thread_uuid)
            or str(historical["message_id"]) != str(row_id)
            or str(historical["source"]) != source_value
            or str(historical["role"]) != str(role)
            or historical.get("supersedes_input_seq") != supersedes_input_seq
        ):
            raise InputDeliveryConflict(
                "stable input identity conflicts with historical delivery"
            )
        stored_content = str(historical["content"] or "")
        if stored_content != str(content) and source_value != "officer_wake":
            raise InputDeliveryConflict(
                "stable input identity conflicts with historical transcript"
            )
        result = _dict(historical)
        result.update(
            {
                "message_id": str(row_id),
                "message_row_id": str(row_id),
                "seq": int(historical["seq"]),
                "transcript_inserted": False,
                "content": stored_content,
                "role": str(historical["role"]),
                "turn_number": historical["turn_number"],
                "execution_lane": str(historical["execution_lane"]),
                "queue_state": None,
                "execution_disposition": "superseded",
            }
        )
        return result

    if has_identity:
        # New work still needs the current reciprocal binding, attach token,
        # open generation, and live agent row.  This second read reuses the
        # parent lock already held above and keeps the standalone helper strict.
        thread = await lock_runtime_authority(
            conn,
            thread_id=thread_uuid,
            agent_id=str(agent_id),
            pod_uid=str(pod_uid),
            session_runtime_generation=str(session_runtime_generation),
            runtime_attach_token=str(runtime_attach_token),
        )

    if execution_lane == "pinned" and (
        thread.get("runtime_retirement_token") is not None
        or str(thread.get("status") or "") in {"ending", "ended", "suspended"}
    ):
        # A terminal replay above remains observable after End. New input does
        # not: End and persistence serialize on the same thread row, so the
        # winner is the only truthful durable outcome.
        raise InputDeliveryAuthorityLost("pinned thread retirement owns input")

    if has_identity and execution_lane != "pinned":
        raise InputDeliveryAuthorityLost("pinned runtime cannot claim stateless input")
    if not has_identity and execution_lane == "stateless":
        accepted_source = source_value == "officer_wake" or (
            allow_stateless_subagent_event and source_value == "subagent"
        )
        if str(role) != "event" or not accepted_source:
            raise InputDeliveryAuthorityLost(
                "stateless durable input is reserved for server events"
            )
        if thread.get("agent_id") is not None:
            raise InputDeliveryAuthorityLost("stateless thread is unexpectedly bound")

    # Stable delivery identity owns the immutable transcript row. A retry may
    # be rendered after the session's turn counter advanced, or may carry a
    # newly derived turn hint; neither is allowed to reinterpret that row.
    existing_turn_number = await conn.fetchval(
        "SELECT turn_number FROM thread_messages WHERE id=$1 AND thread_id=$2",
        row_id,
        thread_uuid,
    )
    effective_turn_number = (
        int(existing_turn_number) if existing_turn_number is not None else turn_number
    )
    if execution_lane == "stateless" and (
        isinstance(effective_turn_number, bool)
        or not isinstance(effective_turn_number, int)
        or effective_turn_number <= 0
    ):
        # The first stateless writer derives its turn exactly once. Concurrent
        # retries then take the committed branch above.
        effective_turn_number = int(thread.get("total_turns") or 0) + 1

    inserted = await conn.fetchrow(
        """
        INSERT INTO thread_messages
            (id, thread_id, role, content, turn_number)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (id) DO NOTHING
        RETURNING id, seq, thread_id, role, content, turn_number
        """,
        row_id,
        thread_uuid,
        str(role),
        str(content),
        effective_turn_number,
    )
    transcript_inserted = inserted is not None
    message = inserted or await conn.fetchrow(
        "SELECT id, seq, thread_id, role, content, turn_number "
        "FROM thread_messages WHERE id = $1",
        row_id,
    )
    if (
        message is None
        or str(message["thread_id"]) != str(thread_uuid)
        or str(message["role"]) != str(role)
        or message.get("turn_number") != effective_turn_number
    ):
        raise InputDeliveryConflict("stable input identity conflicts with transcript")

    if transcript_inserted:
        await conn.execute(
            "UPDATE threads SET last_activity = CURRENT_TIMESTAMP, "
            "total_turns = GREATEST(total_turns, COALESCE($2, 0)) WHERE id = $1",
            thread_uuid,
            effective_turn_number,
        )

    existing_delivery = await conn.fetchrow(
        "SELECT * FROM thread_input_deliveries WHERE delivery_id = $1",
        delivery_uuid,
    )
    existing_state = (
        str(existing_delivery["state"] or "") if existing_delivery is not None else ""
    )

    queue_state: str | None = None
    queue_deferred_reason: str | None = None
    if (
        not has_identity
        and execution_lane == "stateless"
        and existing_state not in {"admitted", "settled"}
    ):
        status = str(thread.get("status") or "")
        if status == "suspended":
            woke = await conn.fetchval(
                "UPDATE threads SET status = 'created', agent_id = NULL, "
                "control_admission_agent_id = NULL, awaiting_user_since = NULL, "
                "extend_count = 0 WHERE id = $1 AND execution_lane = 'stateless' "
                "AND status = 'suspended' RETURNING id",
                thread_uuid,
            )
            if woke is None:
                raise InputDeliveryAuthorityLost(
                    "stateless suspended-input wake lost thread authority"
                )
            status = "created"
        if status in {"created", "active", "awaiting_user"}:
            queue = await conn.fetchrow(
                "SELECT unit_kind, state, lease_token, "
                "input_delivery_capable_lease_token FROM run_queue "
                "WHERE unit_id = $1 FOR UPDATE",
                thread_uuid,
            )
            # A rolling-old executor that already owns this unit cannot see an
            # event row. Leave the durable input unqueued until that exact
            # claim finishes; the outbox's stable retry then admits it without
            # disturbing the old turn's watermark.
            if queue is not None and str(queue["unit_kind"] or "") != "session_turn":
                raise InputDeliveryAuthorityLost("run_queue unit kind is incompatible")
            if (
                queue is not None
                and str(queue["state"] or "") == "leased"
                and int(queue["input_delivery_capable_lease_token"] or 0)
                != int(queue["lease_token"] or 0)
            ):
                queue_deferred_reason = "rolling_old_executor"
            else:
                from shared.run_queue import (
                    UNIT_KIND_SESSION_TURN,
                    record_input_seq,
                )

                queue_state = await record_input_seq(
                    conn,
                    unit_id=thread_uuid,
                    unit_kind=UNIT_KIND_SESSION_TURN,
                    input_seq=int(message["seq"]),
                    fair_key=(
                        str(thread["user_id"])
                        if thread.get("user_id") is not None
                        else None
                    ),
                )
                if queue_state == "parked":
                    queue_deferred_reason = "run_queue_parked"
        elif status != "ended":
            raise InputDeliveryAuthorityLost(
                f"stateless thread does not accept event input ({status or 'unknown'})"
            )

    await conn.execute(
        """
        INSERT INTO thread_input_deliveries
            (delivery_id, thread_id, message_id, source, execution_lane,
             supersedes_input_seq, conversation_revision)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (delivery_id) DO NOTHING
        """,
        delivery_uuid,
        thread_uuid,
        row_id,
        source_value,
        execution_lane,
        supersedes_input_seq,
        int(thread.get("conversation_revision") or 0),
    )
    delivery = await conn.fetchrow(
        "SELECT * FROM thread_input_deliveries WHERE delivery_id = $1 FOR UPDATE",
        delivery_uuid,
    )
    if (
        delivery is None
        or str(delivery["thread_id"]) != str(thread_uuid)
        or str(delivery["message_id"]) != str(row_id)
        or str(delivery["source"]) != source_value
        or str(delivery["execution_lane"] or "") != execution_lane
        or delivery.get("supersedes_input_seq") != supersedes_input_seq
    ):
        raise InputDeliveryConflict("stable input identity conflicts with delivery")

    # A wake renderer may observe newer project state on an ambiguous-response
    # retry. The first committed transcript payload is canonical for that
    # server-owned delivery identity; replay it rather than rejecting the retry
    # or queueing different text under one identity. Human delivery identities
    # retain the stricter invariant so internal misuse cannot alias two inputs.
    stored_content = str(message["content"] or "")
    if stored_content != str(content) and source_value != "officer_wake":
        raise InputDeliveryConflict("stable input identity conflicts with transcript")

    if (
        not has_identity
        and execution_lane == "stateless"
        and str(delivery["state"]) not in {"admitted", "settled", "cancelled"}
    ):
        if queue_deferred_reason is not None:
            delivery = await conn.fetchrow(
                "UPDATE thread_input_deliveries SET state = 'deferred', "
                "deferred_reason = $2, deferred_at = statement_timestamp(), "
                "queued_at = NULL, updated_at = statement_timestamp() "
                "WHERE delivery_id = $1 AND execution_lane = 'stateless' "
                "AND state IN ('persisted', 'queued', 'deferred') RETURNING *",
                delivery_uuid,
                queue_deferred_reason,
            )
        elif queue_state is not None:
            delivery = await conn.fetchrow(
                "UPDATE thread_input_deliveries SET state = 'queued', "
                "queued_at = COALESCE(queued_at, statement_timestamp()), "
                "deferred_reason = NULL, deferred_at = NULL, "
                "updated_at = statement_timestamp() "
                "WHERE delivery_id = $1 AND execution_lane = 'stateless' "
                "AND state IN ('persisted', 'queued', 'deferred') RETURNING *",
                delivery_uuid,
            )
        if delivery is None:
            raise InputDeliveryAuthorityLost("stateless input admission was lost")

    if has_identity and str(delivery["state"]) not in {
        "admitted",
        "settled",
        "cancelled",
    }:
        same_runtime = (
            str(delivery["owner_agent_id"] or "") == str(agent_id)
            and str(delivery["owner_pod_uid"] or "") == str(pod_uid)
            and str(delivery["owner_runtime_generation"] or "")
            == str(runtime_generation)
        )
        if not same_runtime or str(delivery["state"]) == "deferred":
            delivery = await conn.fetchrow(
                """
                UPDATE thread_input_deliveries
                   SET state = 'owned',
                       claim_generation = claim_generation + 1,
                       owner_agent_id = $2,
                       owner_pod_uid = $3,
                       owner_runtime_generation = $4,
                       owned_at = statement_timestamp(),
                       queued_at = NULL,
                       deferred_reason = NULL,
                       deferred_at = NULL,
                       updated_at = statement_timestamp()
                 WHERE delivery_id = $1
                   AND state IN ('persisted', 'owned', 'queued', 'deferred')
                RETURNING *
                """,
                delivery_uuid,
                UUID(str(agent_id)),
                str(pod_uid),
                UUID(str(runtime_generation)),
            )
            if delivery is None:
                raise InputDeliveryAuthorityLost("delivery claim was lost")

    result = _dict(delivery)
    result.update(
        {
            "message_id": str(row_id),
            "message_row_id": str(row_id),
            "seq": int(message["seq"]),
            "transcript_inserted": transcript_inserted,
            "content": stored_content,
            "role": str(role),
            "turn_number": message.get("turn_number"),
            "execution_lane": execution_lane,
            "queue_state": queue_state,
            "execution_disposition": "current",
        }
    )
    return result


async def claim_stateless_input_delivery(
    conn: Any,
    *,
    thread_id: str | UUID,
    delivery_id: str | UUID,
    lease_token: int,
    executor_id: str,
    pod_uid: str,
) -> dict[str, Any] | None:
    """Bind one pending event to the exact current stateless queue lease."""

    thread_uuid = UUID(str(thread_id))
    delivery_uuid = UUID(str(delivery_id))
    thread, _queue = await _lock_stateless_runtime_authority(
        conn,
        thread_id=thread_uuid,
        lease_token=lease_token,
        executor_id=executor_id,
        pod_uid=pod_uid,
    )
    row = await conn.fetchrow(
        "SELECT delivery.*, message.seq, message.role, message.content, "
        "message.turn_number, message.rewound_at "
        "FROM thread_input_deliveries AS delivery "
        "JOIN thread_messages AS message ON message.id = delivery.message_id "
        "WHERE delivery.delivery_id = $1 AND delivery.thread_id = $2 "
        "AND message.rewound_at IS NULL "
        "AND (delivery.conversation_revision=$3 OR "
        "(delivery.conversation_revision IS NULL AND $3=0)) "
        "FOR UPDATE OF delivery",
        delivery_uuid,
        thread_uuid,
        int(thread.get("conversation_revision") or 0),
    )
    if (
        row is None
        or str(row["execution_lane"] or "") != "stateless"
        or str(row["role"] or "") != "event"
        or str(row["state"] or "") not in {"persisted", "queued", "deferred"}
    ):
        return None
    same_claim = (
        int(row["owner_run_queue_lease_token"] or 0) == int(lease_token)
        and str(row["owner_executor"] or "") == str(executor_id)
        and str(row["owner_executor_pod_uid"] or "") == str(pod_uid)
    )
    claimed = await conn.fetchrow(
        "UPDATE thread_input_deliveries SET state = 'queued', "
        "claim_generation = claim_generation + $2::bigint, "
        "owner_run_queue_lease_token = $3, owner_executor = $4, "
        "owner_executor_pod_uid = $5, owned_at = statement_timestamp(), "
        "queued_at = COALESCE(queued_at, statement_timestamp()), "
        "deferred_reason = NULL, deferred_at = NULL, "
        "updated_at = statement_timestamp() WHERE delivery_id = $1 "
        "AND execution_lane = 'stateless' "
        "AND state IN ('persisted', 'queued', 'deferred') RETURNING *",
        delivery_uuid,
        0 if same_claim else 1,
        int(lease_token),
        str(executor_id),
        str(pod_uid),
    )
    if claimed is None:
        return None
    result = _dict(claimed)
    result.update(
        {
            "message_id": str(row["message_id"]),
            "seq": int(row["seq"]),
            "role": str(row["role"]),
            "content": str(row["content"] or ""),
            "turn_number": row["turn_number"],
        }
    )
    return result


async def transition_stateless_input_delivery(
    conn: Any,
    *,
    thread_id: str | UUID,
    delivery_id: str | UUID,
    lease_token: int,
    executor_id: str,
    pod_uid: str,
    claim_generation: int,
    transition: str,
    turn_number: int | None = None,
    reason: str | None = None,
) -> bool:
    """CAS one stateless event through provider admission and settlement."""

    await _lock_stateless_runtime_authority(
        conn,
        thread_id=thread_id,
        lease_token=lease_token,
        executor_id=executor_id,
        pod_uid=pod_uid,
    )
    if transition == "admitted":
        if (
            isinstance(turn_number, bool)
            or not isinstance(turn_number, int)
            or turn_number <= 0
        ):
            raise ValueError("admitted input requires a positive turn number")
        states = ("queued",)
        assignments = (
            "state = 'admitted', admitted_at = statement_timestamp(), "
            "admitted_turn_number = input_params.turn_number, "
            "deferred_reason = NULL, "
            "deferred_at = NULL, updated_at = statement_timestamp()"
        )
    elif transition == "settled":
        states = ("admitted",)
        assignments = (
            "state = 'settled', settled_at = statement_timestamp(), "
            "updated_at = statement_timestamp()"
        )
    elif transition == "deferred":
        states = ("queued",)
        assignments = (
            "state = 'deferred', deferred_reason = "
            "LEFT(COALESCE(input_params.reason, 'retryable'), 120), "
            "deferred_at = statement_timestamp(), queued_at = NULL, "
            "updated_at = statement_timestamp()"
        )
    elif transition == "unadmit":
        states = ("admitted",)
        assignments = (
            "state = 'deferred', admitted_at = NULL, admitted_turn_number = NULL, "
            "deferred_reason = LEFT(COALESCE(input_params.reason, "
            "'provider_not_started'), 120), "
            "deferred_at = statement_timestamp(), queued_at = NULL, "
            "updated_at = statement_timestamp()"
        )
    else:  # pragma: no cover - caller contract
        raise ValueError(f"unsupported input delivery transition: {transition}")
    updated = await conn.fetchval(
        "WITH input_params AS ("
        "SELECT $7::bigint AS turn_number, $8::text AS reason) "
        f"UPDATE thread_input_deliveries SET {assignments} FROM input_params "
        "WHERE delivery_id = $1 AND thread_id = $2 "
        "AND execution_lane = 'stateless' AND state = ANY($3::text[]) "
        "AND claim_generation = $4 "
        "AND owner_run_queue_lease_token = $5 "
        "AND owner_executor = $6 AND owner_executor_pod_uid = $9 "
        "RETURNING delivery_id",
        UUID(str(delivery_id)),
        UUID(str(thread_id)),
        list(states),
        int(claim_generation),
        int(lease_token),
        str(executor_id),
        turn_number,
        reason,
        str(pod_uid),
    )
    return updated is not None


async def claim_pending_input_deliveries(
    conn: Any,
    *,
    thread_id: str | UUID,
    agent_id: str | UUID,
    pod_uid: str,
    runtime_generation: str | UUID,
    session_runtime_generation: str | UUID | None = None,
    runtime_attach_token: str | UUID,
) -> list[dict[str, Any]]:
    """Claim every persisted/unadmitted input for one attached runtime."""

    thread_uuid = UUID(str(thread_id))
    agent_uuid = UUID(str(agent_id))
    runtime_uuid = UUID(str(runtime_generation))
    attach_uuid = UUID(str(runtime_attach_token))
    session_runtime = session_runtime_generation or runtime_generation
    thread = await lock_runtime_authority(
        conn,
        thread_id=thread_uuid,
        agent_id=agent_uuid,
        pod_uid=pod_uid,
        session_runtime_generation=session_runtime,
        runtime_attach_token=attach_uuid,
    )
    rows = await conn.fetch(
        """
        SELECT delivery.*, message.seq, message.role, message.content
          FROM thread_input_deliveries AS delivery
          JOIN thread_messages AS message ON message.id = delivery.message_id
         WHERE delivery.thread_id = $1
           AND delivery.state IN ('persisted', 'owned', 'queued', 'deferred')
           AND message.rewound_at IS NULL
           AND (delivery.conversation_revision=$2 OR
                (delivery.conversation_revision IS NULL AND $2=0))
         ORDER BY
             CASE WHEN delivery.source = 'subagent'
                        AND delivery.supersedes_input_seq IS NOT NULL
                  THEN 0 ELSE 1 END,
             message.seq,
             delivery.delivery_id
         FOR UPDATE OF delivery
        """,
        thread_uuid,
        int(thread.get("conversation_revision") or 0),
    )
    result: list[dict[str, Any]] = []
    for raw in rows:
        row = _dict(raw)
        same_runtime = (
            str(row.get("owner_agent_id") or "") == str(agent_uuid)
            and str(row.get("owner_pod_uid") or "") == str(pod_uid)
            and str(row.get("owner_runtime_generation") or "") == str(runtime_uuid)
        )
        if not same_runtime or str(row.get("state")) == "deferred":
            updated = await conn.fetchrow(
                """
                UPDATE thread_input_deliveries
                   SET state = 'owned', claim_generation = claim_generation + 1,
                       owner_agent_id = $2, owner_pod_uid = $3,
                       owner_runtime_generation = $4,
                       owned_at = statement_timestamp(), queued_at = NULL,
                       deferred_reason = NULL, deferred_at = NULL,
                       updated_at = statement_timestamp()
                 WHERE delivery_id = $1
                   AND state IN ('persisted', 'owned', 'queued', 'deferred')
                RETURNING *
                """,
                row["delivery_id"],
                agent_uuid,
                str(pod_uid),
                runtime_uuid,
            )
            if updated is None:
                continue
            preserved = {
                "seq": row["seq"],
                "role": row["role"],
                "content": row["content"],
            }
            row = {**_dict(updated), **preserved}
        row["message_id"] = str(row["message_id"])
        result.append(row)
    return result


async def mark_input_delivery_queued(
    conn: Any,
    *,
    delivery_id: str | UUID,
    agent_id: str | UUID,
    pod_uid: str,
    runtime_generation: str | UUID,
    session_runtime_generation: str | UUID | None = None,
    runtime_attach_token: str | UUID,
    claim_generation: int,
) -> bool:
    session_runtime = session_runtime_generation or runtime_generation
    row = await conn.fetchrow(
        """
        UPDATE thread_input_deliveries delivery
           SET state = 'queued', queued_at = COALESCE(queued_at, statement_timestamp()),
               updated_at = statement_timestamp()
          FROM threads thread, agents agent
         WHERE delivery.delivery_id = $1
           AND delivery.state IN ('owned', 'queued')
           AND delivery.claim_generation = $2
           AND delivery.owner_agent_id = $3
           AND delivery.owner_pod_uid = $4
           AND delivery.owner_runtime_generation = $5
           AND thread.id = delivery.thread_id
           AND thread.agent_id = delivery.owner_agent_id
           AND thread.runtime_generation = $6
           AND thread.runtime_attach_token = $7
           AND thread.runtime_retirement_token IS NULL
           AND agent.id = delivery.owner_agent_id
           AND agent.thread_id = thread.id
           AND agent.pod_uid = delivery.owner_pod_uid
           AND agent.status NOT IN ('offline', 'deleted')
        RETURNING delivery.delivery_id
        """,
        UUID(str(delivery_id)),
        int(claim_generation),
        UUID(str(agent_id)),
        str(pod_uid),
        UUID(str(runtime_generation)),
        UUID(str(session_runtime)),
        UUID(str(runtime_attach_token)),
    )
    return row is not None


async def transition_input_delivery(
    conn: Any,
    *,
    delivery_id: str | UUID,
    agent_id: str | UUID,
    pod_uid: str,
    runtime_generation: str | UUID,
    session_runtime_generation: str | UUID | None = None,
    runtime_attach_token: str | UUID,
    claim_generation: int,
    transition: str,
    turn_number: int | None = None,
    reason: str | None = None,
) -> bool:
    """CAS one exact owner through admitted, settled, deferred, or cancelled."""

    session_runtime = session_runtime_generation or runtime_generation
    if transition == "admitted":
        states = ("owned", "queued")
        assignments = (
            "state = 'admitted', admitted_at = statement_timestamp(), "
            "admitted_turn_number = $6, deferred_reason = NULL, "
            "deferred_at = NULL, updated_at = statement_timestamp()"
        )
    elif transition == "settled":
        states = ("admitted",)
        assignments = (
            "state = 'settled', settled_at = statement_timestamp(), "
            "updated_at = statement_timestamp()"
        )
    elif transition == "deferred":
        states = ("owned", "queued")
        assignments = (
            "state = 'deferred', deferred_reason = LEFT(COALESCE($7, 'retryable'), 120), "
            "deferred_at = statement_timestamp(), queued_at = NULL, "
            "updated_at = statement_timestamp()"
        )
    elif transition == "unadmit":
        states = ("admitted",)
        assignments = (
            "state = 'deferred', admitted_at = NULL, admitted_turn_number = NULL, "
            "deferred_reason = LEFT(COALESCE($7, 'provider_not_started'), 120), "
            "deferred_at = statement_timestamp(), queued_at = NULL, "
            "updated_at = statement_timestamp()"
        )
    elif transition == "cancelled":
        states = ("owned", "queued")
        assignments = (
            "state = 'cancelled', cancelled_at = statement_timestamp(), "
            "cancelled_turn_number = $6, "
            "cancelled_reason = LEFT(COALESCE($7, 'human_stop_before_provider'), 120), "
            "queued_at = NULL, deferred_reason = NULL, deferred_at = NULL, "
            "updated_at = statement_timestamp()"
        )
    else:  # pragma: no cover - caller contract
        raise ValueError(f"unsupported input delivery transition: {transition}")

    source_fence = (
        "AND delivery.source = 'direct_human'" if transition == "cancelled" else ""
    )

    row = await conn.fetchrow(
        f"""
        UPDATE thread_input_deliveries delivery
           SET {assignments}
          FROM threads thread, agents agent
         WHERE delivery.delivery_id = $1
           AND delivery.state = ANY($2::text[])
           AND delivery.claim_generation = $3
           AND delivery.owner_agent_id = $4
           AND delivery.owner_pod_uid = $5
           AND delivery.owner_runtime_generation = $8
           {source_fence}
           AND thread.id = delivery.thread_id
           AND thread.agent_id = delivery.owner_agent_id
           AND thread.runtime_generation = $9
           AND thread.runtime_attach_token = $10
           AND thread.runtime_retirement_token IS NULL
           AND agent.id = delivery.owner_agent_id
           AND agent.thread_id = thread.id
           AND agent.pod_uid = delivery.owner_pod_uid
           AND agent.status NOT IN ('offline', 'deleted')
           AND ($6::bigint IS NULL OR TRUE)
           AND ($7::text IS NULL OR TRUE)
        RETURNING delivery.delivery_id
        """,
        UUID(str(delivery_id)),
        list(states),
        int(claim_generation),
        UUID(str(agent_id)),
        str(pod_uid),
        turn_number,
        reason,
        UUID(str(runtime_generation)),
        UUID(str(session_runtime)),
        UUID(str(runtime_attach_token)),
    )
    return row is not None


async def get_input_delivery(
    conn: Any, delivery_id: str | UUID
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        "SELECT delivery.*, message.rewound_at, "
        "thread.conversation_revision AS current_conversation_revision "
        "FROM thread_input_deliveries AS delivery "
        "JOIN thread_messages AS message ON message.id=delivery.message_id "
        "JOIN threads AS thread ON thread.id=delivery.thread_id "
        "WHERE delivery.delivery_id = $1",
        UUID(str(delivery_id)),
    )
    if row is None:
        return None
    result = _dict(row)
    if row["rewound_at"] is not None:
        result["execution_disposition"] = (
            "historical"
            if str(row["state"] or "") in {"admitted", "settled"}
            else "superseded"
        )
    else:
        result["execution_disposition"] = "current"
    return result
