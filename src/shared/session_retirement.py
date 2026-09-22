"""Exact claimant-quiescence acknowledgement for stateless session End.

Public End advances the queue token immediately, but the losing executor may
still have synchronous SFTP work admitted in another thread.  It may
acknowledge quiescence only after the turn has unwound and its remote backend
has been retired locally.  This helper writes that acknowledgement under the
same ``threads -> run_queue`` lock order as session admission.
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, AsyncIterator
from uuid import UUID


CLAIM_RETIREMENT_KEY = "_stateless_claim_retirement"
WORKSPACE_RETIREMENT_PENDING_KEY = "_stateless_workspace_retirement_pending"
WORKSPACE_RETIREMENT_SETTLED_KEY = "_stateless_workspace_retirement_settled"
CLAIM_LOSS_LEDGER_KEY = "_stateless_claim_losses"
CLAIM_LOSS_HOLD_KEY = "_stateless_claim_loss_hold"
# Bounded, diagnostic-only trail of how claimant-loss debt was settled
# (Kubernetes exact-terminal proof or an operator attestation). Never
# authority: nothing reads it to decide anything.
CLAIM_LOSS_RECEIPTS_KEY = "_stateless_claim_loss_receipts"
CLAIM_LOSS_RECEIPT_LIMIT = 8
ACTIVE_CLAIM_KEY = "_stateless_active_claim"
RESIDENT_RETIREMENT_ACK_KEY = "_stateless_resident_retirement_ack"
SHELL_RETIREMENT_ACK_KEY = "_stateless_shell_retirement_ack"
STATELESS_STOP_KEYS = frozenset(
    {
        WORKSPACE_RETIREMENT_PENDING_KEY,
        CLAIM_RETIREMENT_KEY,
        CLAIM_LOSS_LEDGER_KEY,
        CLAIM_LOSS_HOLD_KEY,
    }
)


@dataclass(frozen=True, slots=True)
class ClaimantAuthority:
    """Immutable Kubernetes identity for one credential-bearing claimant."""

    pod: str
    pod_uid: str
    eviction_requested: bool = False


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("stateless metadata root is malformed") from exc
        if isinstance(parsed, dict):
            return dict(parsed)
    raise RuntimeError("stateless metadata root is malformed")


def stateless_stop_markers(metadata: Any) -> frozenset[str]:
    """Return every present orchestrator stop marker; malformed root refuses."""

    root = _json_object(metadata)
    return frozenset(key for key in STATELESS_STOP_KEYS if key in root)


def _exact_bool(value: Any, *, label: str) -> bool:
    if type(value) is not bool:
        raise RuntimeError(f"{label} is malformed")
    return value


def _exact_nonnegative_int(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RuntimeError(f"{label} is malformed")
    return value


def _optional_authority_text(value: Any, *, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"{label} is malformed")
    return value


def _retirement_ack_matches(
    root: dict[str, Any],
    marker: dict[str, Any],
    *,
    ack_key: str,
    retired_by_key: str,
) -> bool:
    """Validate one exact remote/runtime retirement acknowledgement.

    Protocol acknowledgements bind all SSH authority fields.  An observed
    exact-runtime terminal state has no live SSH endpoint to attest, but it
    still binds the immutable runtime UID and terminal queue token.  Merely
    setting a boolean flag can therefore never authorize workspace teardown.
    """

    retired_by = marker.get(retired_by_key)
    ack = root.get(ack_key)
    if retired_by not in {"protocol", "workspace_runtime_terminal"}:
        return False
    if not isinstance(ack, dict):
        return False
    try:
        ack_token = _exact_nonnegative_int(
            ack.get("terminal_token"), label=f"{ack_key} terminal token"
        )
    except RuntimeError:
        return False
    if ack_token != marker["terminal_token"]:
        return False
    if ack.get("runtime_incarnation") != marker.get("runtime_incarnation"):
        return False
    if ack.get("kind") != retired_by:
        return False
    if retired_by == "protocol":
        return (
            ack.get("workspace_generation") == marker.get("workspace_generation")
            and ack.get("endpoint_generation") == marker.get("endpoint_generation")
            and ack.get("host_key_fingerprint") == marker.get("host_key_fingerprint")
        )
    return True


def stateless_retirement_authority(
    metadata: Any,
    *,
    require_pending: bool = True,
) -> dict[str, Any] | None:
    """Parse and validate one pending terminal-retirement authority.

    Every orchestrator-owned boolean is an exact JSON boolean.  PostgreSQL's
    text-to-boolean cast accepts strings such as ``"yes"`` and ``"1"``;
    accepting those here would let malformed metadata skip claimant, resident,
    or shell retirement.  Callers must never use truthiness for these fields.
    """

    root = _json_object(metadata)
    pending_present = WORKSPACE_RETIREMENT_PENDING_KEY in root
    retirement_present = CLAIM_RETIREMENT_KEY in root
    if not pending_present:
        if retirement_present:
            raise RuntimeError(
                "stateless retirement authority exists without its marker"
            )
        if require_pending:
            return None
        return None
    if root[WORKSPACE_RETIREMENT_PENDING_KEY] is not True:
        raise RuntimeError("stateless retirement marker is malformed")
    marker = root.get(CLAIM_RETIREMENT_KEY)
    if not isinstance(marker, dict):
        raise RuntimeError("stateless retirement authority is malformed")
    marker = dict(marker)
    marker["terminal_token"] = _exact_nonnegative_int(
        marker.get("terminal_token"), label="terminal retirement token"
    )
    for field in (
        "claimant_quiesced",
        "shell_retirement_required",
        "resident_cleanup_required",
        "residents_retired",
        "remote_retired",
        "permanent",
    ):
        marker[field] = _exact_bool(
            marker.get(field), label=f"terminal retirement {field}"
        )
    marker["workspace_absence_proven"] = _exact_bool(
        marker.get("workspace_absence_proven", False),
        label="terminal retirement workspace_absence_proven",
    )
    for field in (
        "workspace_generation",
        "runtime_incarnation",
        "host_key_fingerprint",
        "endpoint_generation",
    ):
        marker[field] = _optional_authority_text(
            marker.get(field), label=f"terminal retirement {field}"
        )
    if marker["workspace_absence_proven"]:
        raise RuntimeError("terminal workspace absence proof is unsupported")

    # Compatibility for acknowledgements written by the first S2 build: its
    # nested jsonb_set used create_if_missing=false, so PostgreSQL could commit
    # the exact ACK plus retired=true while silently omitting the new
    # ``*_retired_by`` field.  Only infer an *absent* field from the ACK's
    # explicit kind. An explicit null/malformed field remains fail-closed, and
    # the normalized copy below still takes the full protocol drift checks.
    for retired_key, retired_by_key, ack_key in (
        ("residents_retired", "residents_retired_by", RESIDENT_RETIREMENT_ACK_KEY),
        ("remote_retired", "remote_retired_by", SHELL_RETIREMENT_ACK_KEY),
    ):
        if marker[retired_key] and retired_by_key not in marker:
            ack = root.get(ack_key)
            if isinstance(ack, dict) and ack.get("kind") in {
                "protocol",
                "workspace_runtime_terminal",
            }:
                marker[retired_by_key] = ack["kind"]

    losses = unresolved_claim_losses(root)
    if marker["claimant_quiesced"] is not (not losses):
        raise RuntimeError(
            "terminal claimant proof disagrees with unresolved claimant losses"
        )
    if not marker["resident_cleanup_required"]:
        if marker["residents_retired"] is not True:
            raise RuntimeError("unneeded resident retirement is not settled")
    elif marker["residents_retired"] and not _retirement_ack_matches(
        root,
        marker,
        ack_key=RESIDENT_RETIREMENT_ACK_KEY,
        retired_by_key="residents_retired_by",
    ):
        raise RuntimeError("resident retirement acknowledgement is malformed")
    elif not marker["residents_retired"] and RESIDENT_RETIREMENT_ACK_KEY in root:
        raise RuntimeError("resident retirement acknowledgement is premature")

    if not marker["shell_retirement_required"]:
        if marker["remote_retired"] is not True:
            raise RuntimeError("unneeded shell retirement is not settled")
    elif marker["remote_retired"] and not _retirement_ack_matches(
        root,
        marker,
        ack_key=SHELL_RETIREMENT_ACK_KEY,
        retired_by_key="remote_retired_by",
    ):
        raise RuntimeError("shell retirement acknowledgement is malformed")
    elif not marker["remote_retired"] and SHELL_RETIREMENT_ACK_KEY in root:
        raise RuntimeError("shell retirement acknowledgement is premature")

    # A live SSH protocol proof is meaningful only for the exact attested
    # workspace tuple.  Refuse metadata drift after acknowledgement and before
    # snapshot/delete.
    if (
        marker.get("residents_retired_by") == "protocol"
        or marker.get("remote_retired_by") == "protocol"
    ):
        binding = root.get("_workspace_binding")
        workspace = root.get("workspace_container")
        if not isinstance(binding, dict) or not isinstance(workspace, dict):
            raise RuntimeError("terminal workspace authority is malformed")
        current_runtime = workspace.get("_runtime_incarnation")
        runtime_matches = current_runtime == marker.get("runtime_incarnation")
        runtime_is_durably_released = bool(
            current_runtime is None
            and str(workspace.get("status") or "") in {"deleted", "released"}
        )
        if (
            marker.get("endpoint_generation") != marker.get("workspace_generation")
            or binding.get("generation") != marker.get("workspace_generation")
            or workspace.get("_canvas_workspace_generation")
            != marker.get("endpoint_generation")
            or binding.get("ssh_host_key_fingerprint")
            != marker.get("host_key_fingerprint")
            or not (runtime_matches or runtime_is_durably_released)
        ):
            raise RuntimeError("terminal workspace authority changed")
    return marker


def stateless_retirement_release_authorized(metadata: Any) -> dict[str, Any]:
    """Return exact terminal authority only when external cleanup may begin."""

    marker = stateless_retirement_authority(metadata)
    if marker is None:
        raise RuntimeError("stateless retirement authority is absent")
    if not marker["claimant_quiesced"]:
        raise RuntimeError("terminal claimant is not quiesced")
    if marker["resident_cleanup_required"] and not marker["residents_retired"]:
        raise RuntimeError("workspace residents are not retired")
    if marker["shell_retirement_required"] and not marker["remote_retired"]:
        raise RuntimeError("workspace shell is not retired")
    return marker


def stateless_settled_retirement_authority(
    metadata: Any,
) -> dict[str, Any] | None:
    """Validate the durable no-repeat proof left by a completed soft End."""

    root = _json_object(metadata)
    if WORKSPACE_RETIREMENT_SETTLED_KEY not in root:
        return None
    if (
        WORKSPACE_RETIREMENT_PENDING_KEY in root
        or CLAIM_RETIREMENT_KEY in root
        or RESIDENT_RETIREMENT_ACK_KEY in root
        or SHELL_RETIREMENT_ACK_KEY in root
    ):
        raise RuntimeError("settled and pending retirement authority overlap")
    raw = root[WORKSPACE_RETIREMENT_SETTLED_KEY]
    if not isinstance(raw, dict):
        raise RuntimeError("settled retirement authority is malformed")
    settled = dict(raw)
    settled["terminal_token"] = _exact_nonnegative_int(
        settled.get("terminal_token"), label="settled retirement token"
    )
    settled["cleanup_complete"] = _exact_bool(
        settled.get("cleanup_complete"), label="settled cleanup proof"
    )
    settled["permanent"] = _exact_bool(
        settled.get("permanent"), label="settled permanent intent"
    )
    settled["snapshot_restore_required"] = _exact_bool(
        settled.get("snapshot_restore_required"),
        label="settled snapshot proof",
    )
    settled["workspace_absence_proven"] = _exact_bool(
        settled.get("workspace_absence_proven", False),
        label="settled workspace absence proof",
    )
    if settled["workspace_absence_proven"]:
        raise RuntimeError("settled workspace absence proof is unsupported")
    backing_id = settled.get("backing_id")
    if backing_id is not None and not isinstance(backing_id, str):
        raise RuntimeError("settled workspace backing is malformed")
    settled["runtime_incarnation"] = _optional_authority_text(
        settled.get("runtime_incarnation"),
        label="settled runtime incarnation",
    )
    if settled["cleanup_complete"] is not True:
        raise RuntimeError("settled retirement lacks cleanup proof")
    return settled


def _claimant_authority(value: Any, *, label: str) -> ClaimantAuthority:
    if not isinstance(value, dict):
        raise RuntimeError(f"{label} is malformed")
    pod = str(value.get("pod") or "").strip()
    pod_uid = str(value.get("pod_uid") or "").strip()
    if not pod or not pod_uid:
        raise RuntimeError(f"{label} lacks immutable pod authority")
    return ClaimantAuthority(
        pod=pod,
        pod_uid=pod_uid,
        eviction_requested=value.get("eviction_requested_at") is not None,
    )


def active_claim_authority(metadata: Any) -> tuple[int, ClaimantAuthority] | None:
    """Return the credential-bound active claimant, if one was published."""

    root = _json_object(metadata)
    if ACTIVE_CLAIM_KEY not in root:
        return None
    raw = root[ACTIVE_CLAIM_KEY]
    authority = _claimant_authority(raw, label="stateless active claim")
    raw_token = raw.get("lease_token")
    if isinstance(raw_token, bool) or not isinstance(raw_token, int):
        raise RuntimeError("stateless active claim token is malformed")
    token = raw_token
    if token <= 0:
        raise RuntimeError("stateless active claim token is malformed")
    return token, authority


def unresolved_claim_losses(metadata: Any) -> dict[int, ClaimantAuthority]:
    """Return the exact unresolved ``lease token -> pod identity`` ledger.

    Session reaping records an entry in the same transaction that advances the
    queue token. Entries are removed only after the old claimant drains its
    local backend or its exact UID is observed with all containers terminated. Treat a
    malformed entry as unresolved but un-actionable by raising: silently
    dropping corrupt retirement authority would permit workspace teardown
    while an old SFTP operation can still be in flight.
    """

    root = _json_object(metadata)
    if CLAIM_LOSS_LEDGER_KEY not in root:
        return {}
    raw = root[CLAIM_LOSS_LEDGER_KEY]
    if not isinstance(raw, dict) or not raw:
        raise RuntimeError("stateless claim-loss ledger is malformed")
    losses: dict[int, ClaimantAuthority] = {}
    for raw_token, raw_entry in raw.items():
        try:
            token = int(raw_token)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("stateless claim-loss token is malformed") from exc
        if raw_token != str(token) or token in losses:
            raise RuntimeError("stateless claim-loss token is noncanonical")
        if token <= 0 or not isinstance(raw_entry, dict):
            raise RuntimeError("stateless claim-loss entry is malformed")
        if raw_entry.get("quiesced") is not False:
            raise RuntimeError("stateless claim-loss entry lacks exact authority")
        losses[token] = _claimant_authority(
            raw_entry, label="stateless claim-loss entry"
        )
    return losses


def claim_loss_hold(metadata: dict[str, Any]) -> dict[str, Any] | None:
    if CLAIM_LOSS_HOLD_KEY not in metadata:
        return None
    raw = metadata[CLAIM_LOSS_HOLD_KEY]
    if not isinstance(raw, dict):
        raise RuntimeError("stateless claim-loss hold is malformed")
    raw_token = raw.get("lease_token")
    raw_attempts = raw.get("attempts_since_completion")
    if (
        isinstance(raw_token, bool)
        or not isinstance(raw_token, int)
        or isinstance(raw_attempts, bool)
        or not isinstance(raw_attempts, int)
    ):
        raise RuntimeError("stateless claim-loss hold is malformed")
    token = raw_token
    attempts = raw_attempts
    state = str(raw.get("intended_state") or "")
    if token <= 0 or attempts < 0 or state not in {"queued", "parked", "done"}:
        raise RuntimeError("stateless claim-loss hold is malformed")
    return dict(raw)


@asynccontextmanager
async def _connection(source: Any) -> AsyncIterator[Any]:
    acquire = getattr(source, "acquire", None)
    if acquire is None:
        yield source
        return
    async with acquire() as conn:
        yield conn


async def acknowledge_session_claim_quiesced(
    db: Any,
    *,
    thread_id: UUID | str,
    previous_lease_token: int,
    leased_by: str,
    pod_uid: str,
    quiesced_by: str = "claimant",
    expected_terminal_token: int | None = None,
    receipt: dict[str, Any] | None = None,
) -> bool:
    """ACK one terminally fenced claimant after all local I/O has drained.

    A reaper records the loss before publishing its token bump, so this ACK is
    also the ordinary post-reap settlement path (not only public End).  The
    unresolved-only ledger keeps every stolen generation until an exact
    claimant/pod proof removes it; a later End cannot mistake queued/parked for
    local-I/O quiescence.

    ``receipt`` (optional evidence fields) is appended, in the same metadata
    write that removes the debt, to the bounded ``CLAIM_LOSS_RECEIPTS_KEY``
    trail, so the proof that settled a hold is durable before anything that
    depended on it (the executor Pod's retention finalizer) is released.
    """

    tid = thread_id if isinstance(thread_id, UUID) else UUID(str(thread_id))
    token = int(previous_lease_token)
    owner = str(leased_by).strip()
    owner_uid = str(pod_uid).strip()
    if token <= 0 or not owner or not owner_uid:
        return False
    async with _connection(db) as conn:
        async with conn.transaction():
            thread = await conn.fetchrow(
                """
                SELECT status::text AS status, metadata
                FROM threads
                WHERE id = $1::uuid
                  AND execution_lane = 'stateless'
                FOR UPDATE
                """,
                tid,
            )
            if thread is None:
                return False
            metadata = _json_object(thread["metadata"])
            losses = unresolved_claim_losses(metadata)
            loss = losses.get(token)
            if loss is None or loss.pod != owner or loss.pod_uid != owner_uid:
                return False
            queue = await conn.fetchrow(
                """
                SELECT state, lease_token, attempts_since_completion,
                       input_seq, consumed_seq,
                       control_input_seq, control_consumed_seq
                FROM run_queue
                WHERE unit_id = $1::uuid AND unit_kind = 'session_turn'
                FOR UPDATE
                """,
                tid,
            )
            # The thread lock is the global first lock; taking the queue lock
            # even though its token may have advanced keeps this metadata
            # settlement serialized with End and admission.
            if queue is None:
                return False

            raw_ledger = dict(metadata.get(CLAIM_LOSS_LEDGER_KEY) or {})
            raw_ledger.pop(str(token), None)
            retirement = metadata.get(CLAIM_RETIREMENT_KEY)
            retirement_pending = (
                isinstance(retirement, dict)
                and "_stateless_workspace_retirement_pending" in metadata
                and metadata.get("_stateless_workspace_retirement_pending") is True
            )
            if "_stateless_workspace_retirement_pending" in metadata and not (
                retirement_pending
            ):
                raise RuntimeError("stateless retirement marker is malformed")
            if expected_terminal_token is not None:
                parsed_retirement = stateless_retirement_authority(metadata)
                if (
                    str(thread["status"] or "") != "ended"
                    or parsed_retirement is None
                    or parsed_retirement["terminal_token"]
                    != int(expected_terminal_token)
                ):
                    return False
                if int(queue["lease_token"] or 0) != int(expected_terminal_token):
                    return False

            if retirement_pending:
                retirement = dict(retirement)
                all_quiesced = not raw_ledger
                retirement["claimant_quiesced"] = all_quiesced
                retirement["quiesced_by"] = quiesced_by if all_quiesced else None

            hold = claim_loss_hold(metadata)
            restore: dict[str, Any] | None = None
            if not raw_ledger and hold is not None:
                if int(hold["lease_token"]) != int(queue["lease_token"] or 0):
                    raise RuntimeError(
                        "claim-loss hold disagrees with the current queue token"
                    )
                if not retirement_pending:
                    intended_state = str(hold["intended_state"])
                    pending_human = queue["input_seq"] is not None and (
                        queue["consumed_seq"] is None
                        or int(queue["input_seq"]) > int(queue["consumed_seq"])
                    )
                    pending_control = int(queue["control_input_seq"] or 0) > int(
                        queue["control_consumed_seq"] or 0
                    )
                    if intended_state == "done" and (pending_human or pending_control):
                        intended_state = "queued"
                    restore = {**hold, "intended_state": intended_state}

            next_metadata = dict(metadata)
            if raw_ledger:
                next_metadata[CLAIM_LOSS_LEDGER_KEY] = raw_ledger
            else:
                next_metadata.pop(CLAIM_LOSS_LEDGER_KEY, None)
                next_metadata.pop(CLAIM_LOSS_HOLD_KEY, None)
            if retirement_pending:
                next_metadata[CLAIM_RETIREMENT_KEY] = retirement
            if receipt is not None:
                prior = next_metadata.get(CLAIM_LOSS_RECEIPTS_KEY)
                receipts = (
                    [item for item in prior if isinstance(item, dict)]
                    if isinstance(prior, list)
                    else []
                )
                receipts.append(
                    {
                        **receipt,
                        "lease_token": token,
                        "pod": owner,
                        "pod_uid": owner_uid,
                        "quiesced_by": quiesced_by,
                        "settled_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                next_metadata[CLAIM_LOSS_RECEIPTS_KEY] = receipts[
                    -CLAIM_LOSS_RECEIPT_LIMIT:
                ]
            updated = await conn.fetchval(
                """
                UPDATE threads
                SET metadata = $3::jsonb
                WHERE id = $1::uuid
                  AND metadata #>> ARRAY[
                      '_stateless_claim_losses', $2::text, 'pod'
                  ] = $4::text
                  AND metadata #>> ARRAY[
                      '_stateless_claim_losses', $2::text, 'pod_uid'
                  ] = $5::text
                  AND metadata #> ARRAY[
                      '_stateless_claim_losses', $2::text, 'quiesced'
                  ] = 'false'::jsonb
                RETURNING id
                """,
                tid,
                str(token),
                json.dumps(next_metadata, sort_keys=True, separators=(",", ":")),
                owner,
                owner_uid,
            )
            if updated is None:
                return False
            if restore is not None:
                restored = await conn.fetchval(
                    """
                    UPDATE run_queue
                    SET state = $3::text,
                        attempts_since_completion = $4::integer,
                        run_after = COALESCE($5::text::timestamptz, run_after),
                        queued_at = CASE WHEN $6::text IS NULL THEN queued_at
                                         ELSE $6::text::timestamptz END
                    WHERE unit_id = $1::uuid
                      AND unit_kind = 'session_turn'
                      AND state = 'parked'
                      AND leased_by IS NULL
                      AND lease_token = $2::bigint
                    RETURNING state
                    """,
                    tid,
                    int(restore["lease_token"]),
                    str(restore["intended_state"]),
                    int(restore["attempts_since_completion"]),
                    restore.get("run_after"),
                    restore.get("queued_at"),
                )
                if restored is None:
                    raise RuntimeError("claim-loss settlement lost its parked hold")
            return True


async def mark_session_claim_eviction_requested(
    db: Any,
    *,
    thread_id: UUID | str,
    previous_lease_token: int,
    leased_by: str,
    pod_uid: str,
) -> bool:
    """Durably mark a graceful Pod-termination attempt before issuing it.

    Kubernetes may remove a Pod API object without proving its process stopped
    on a partitioned node.  Once an eviction was requested, a later 404 or
    same-name replacement is therefore not claimant-quiescence evidence; only
    an exact terminal-container observation or the claimant's own local ACK is.
    """

    tid = thread_id if isinstance(thread_id, UUID) else UUID(str(thread_id))
    token = int(previous_lease_token)
    owner = str(leased_by or "").strip()
    owner_uid = str(pod_uid or "").strip()
    if token <= 0 or not owner or not owner_uid:
        return False
    async with _connection(db) as conn:
        async with conn.transaction():
            thread = await conn.fetchrow(
                "SELECT metadata FROM threads WHERE id = $1::uuid "
                "AND execution_lane = 'stateless' FOR UPDATE",
                tid,
            )
            if thread is None:
                return False
            loss = unresolved_claim_losses(thread["metadata"]).get(token)
            if loss is None or loss.pod != owner or loss.pod_uid != owner_uid:
                return False
            queue = await conn.fetchrow(
                "SELECT 1 FROM run_queue WHERE unit_id = $1::uuid "
                "AND unit_kind = 'session_turn' FOR SHARE",
                tid,
            )
            if queue is None:
                return False
            if loss.eviction_requested:
                return True
            marked = await conn.fetchval(
                """
                UPDATE threads
                SET metadata = jsonb_set(
                    metadata,
                    ARRAY['_stateless_claim_losses', $2::text,
                          'eviction_requested_at'],
                    to_jsonb(now()),
                    true
                )
                WHERE id = $1::uuid
                  AND metadata #>> ARRAY[
                      '_stateless_claim_losses', $2::text, 'pod'
                  ] = $3::text
                  AND metadata #>> ARRAY[
                      '_stateless_claim_losses', $2::text, 'pod_uid'
                  ] = $4::text
                  AND NOT ((metadata #> ARRAY[
                      '_stateless_claim_losses', $2::text
                  ]) ? 'eviction_requested_at')
                RETURNING id
                """,
                tid,
                str(token),
                owner,
                owner_uid,
            )
            return marked is not None


async def update_stateless_claim_status(
    db: Any,
    *,
    thread_id: UUID | str,
    lease_token: int,
    status: str,
) -> bool:
    """Publish active/pause presence only under the exact serving lease.

    This is deliberately separate from the generic internal status route.
    Stateless thread lifecycle is orchestrator-owned; an agent may report
    presence, but it may never reopen an ended/retiring thread.
    """

    if status not in {"active", "awaiting_user"}:
        return False
    tid = thread_id if isinstance(thread_id, UUID) else UUID(str(thread_id))
    token = int(lease_token)
    if token <= 0:
        return False

    async with _connection(db) as conn:
        async with conn.transaction():
            thread = await conn.fetchrow(
                "SELECT status, execution_lane, metadata FROM threads "
                "WHERE id = $1::uuid FOR UPDATE",
                tid,
            )
            if (
                thread is None
                or str(thread["execution_lane"] or "") != "stateless"
                or str(thread["status"] or "")
                not in {"created", "active", "awaiting_user"}
            ):
                return False
            metadata = thread["metadata"]
            try:
                if stateless_stop_markers(metadata):
                    return False
            except RuntimeError:
                return False
            try:
                active = active_claim_authority(metadata)
            except RuntimeError:
                return False
            if active is None or int(active[0]) != token:
                return False
            queue = await conn.fetchrow(
                "SELECT 1 FROM run_queue WHERE unit_id = $1::uuid "
                "AND unit_kind = 'session_turn' AND state = 'leased' "
                "AND lease_token = $2::bigint FOR SHARE",
                tid,
                token,
            )
            if queue is None:
                return False
            if status == "active":
                updated = await conn.fetchval(
                    "UPDATE threads SET status = 'active', "
                    "awaiting_user_since = NULL, extend_count = 0, "
                    "last_activity = CURRENT_TIMESTAMP "
                    "WHERE id = $1::uuid AND status IN "
                    "('created', 'active', 'awaiting_user') "
                    "AND NOT (COALESCE(metadata, '{}'::jsonb) "
                    "? '_stateless_workspace_retirement_pending') "
                    "AND NOT (COALESCE(metadata, '{}'::jsonb) "
                    "? '_stateless_claim_retirement') "
                    "AND NOT (COALESCE(metadata, '{}'::jsonb) "
                    "? '_stateless_claim_losses') "
                    "AND NOT (COALESCE(metadata, '{}'::jsonb) "
                    "? '_stateless_claim_loss_hold') RETURNING id",
                    tid,
                )
            else:
                updated = await conn.fetchval(
                    "UPDATE threads SET status = 'awaiting_user', "
                    "awaiting_user_since = CASE WHEN status = 'awaiting_user' "
                    "THEN awaiting_user_since ELSE now() END, "
                    "extend_count = CASE WHEN status = 'awaiting_user' "
                    "THEN extend_count ELSE 0 END, "
                    "last_activity = CURRENT_TIMESTAMP "
                    "WHERE id = $1::uuid AND status IN "
                    "('created', 'active', 'awaiting_user') "
                    "AND NOT (COALESCE(metadata, '{}'::jsonb) "
                    "? '_stateless_workspace_retirement_pending') "
                    "AND NOT (COALESCE(metadata, '{}'::jsonb) "
                    "? '_stateless_claim_retirement') "
                    "AND NOT (COALESCE(metadata, '{}'::jsonb) "
                    "? '_stateless_claim_losses') "
                    "AND NOT (COALESCE(metadata, '{}'::jsonb) "
                    "? '_stateless_claim_loss_hold') RETURNING id",
                    tid,
                )
            return updated is not None


async def mark_stateless_claim_active(
    db: Any, *, thread_id: UUID | str, lease_token: int
) -> bool:
    """Exact-lease active transition used by stateless attach."""

    return await update_stateless_claim_status(
        db,
        thread_id=thread_id,
        lease_token=lease_token,
        status="active",
    )


__all__ = [
    "ACTIVE_CLAIM_KEY",
    "CLAIM_LOSS_HOLD_KEY",
    "CLAIM_LOSS_LEDGER_KEY",
    "CLAIM_LOSS_RECEIPTS_KEY",
    "CLAIM_LOSS_RECEIPT_LIMIT",
    "CLAIM_RETIREMENT_KEY",
    "RESIDENT_RETIREMENT_ACK_KEY",
    "SHELL_RETIREMENT_ACK_KEY",
    "STATELESS_STOP_KEYS",
    "WORKSPACE_RETIREMENT_PENDING_KEY",
    "WORKSPACE_RETIREMENT_SETTLED_KEY",
    "ClaimantAuthority",
    "active_claim_authority",
    "acknowledge_session_claim_quiesced",
    "claim_loss_hold",
    "mark_stateless_claim_active",
    "mark_session_claim_eviction_requested",
    "stateless_stop_markers",
    "stateless_retirement_authority",
    "stateless_retirement_release_authorized",
    "stateless_settled_retirement_authority",
    "unresolved_claim_losses",
    "update_stateless_claim_status",
]
