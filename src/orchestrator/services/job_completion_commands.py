"""Durable admission for agent job-completion reports.

The short accept transaction is the ownership boundary described by
``knowledge-base/knowledge/features/stateless_agents.md`` section 5.4.5.  It deliberately performs
no completion side effects: it validates the lane-specific fence, records the
immutable command as its first write, advances the per-job commit-order cursor,
and closes a stateless worker lease before committing.

Every transaction that can touch both queue and job rows locks them in that
order.  The queue row uses ``FOR UPDATE`` rather than the design's earlier
``FOR SHARE`` wording because accept now also terminalizes that row (B4).  A
shared lock followed by an update creates a two-reporter lock-upgrade deadlock;
the stronger lock preserves the prescribed order and makes the upgrade
unnecessary.
"""

from __future__ import annotations

import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator, Mapping
from uuid import UUID, uuid5

from orchestrator.services.workspace_idle_completion_events import (
    ACCEPTED_IDLE_WAIT_SOURCE_KEY,
    capture_completion_wait_on_conn,
)
from shared.run_queue import complete_unit

# ``v1`` is the already-shipped status-first workflow.  A reordered command
# must carry a version an old image will refuse rather than silently executing
# with its v1 order.  New executors intentionally support both while rolling;
# explicit test/incident executors may still opt into one exact version.
COMPLETION_CODE_VERSION = "job-completion-v1"
COMPLETION_STATUS_REORDER_CODE_VERSION = "job-completion-v2"
COMPLETION_SUPPORTED_CODE_VERSIONS = (
    COMPLETION_CODE_VERSION,
    COMPLETION_STATUS_REORDER_CODE_VERSION,
)
COMPLETION_DEADLINE_SECONDS = 24 * 60 * 60
# Keep a newly accepted row away from the background candidate scan long
# enough for the request's exact-ID inline claim.  The inline claim ignores
# run_after, so cancellation before that claim still leaves a bounded drain.
COMPLETION_INLINE_GRACE_SECONDS = 2.0

# Stable, application-owned namespace.  The fallback exists only for rolling
# compatibility with old agents; it is allocated after report_seq while the
# jobs row is locked and therefore cannot be used to identify an HTTP retry.
_FALLBACK_REPORT_NAMESPACE = UUID("7072cfbc-d685-4d52-9942-d954f2914652")

# Server-owned acceptance evidence.  This is deliberately stored beside the
# immutable report instead of in a new mutable jobs.context field: a replay
# must retain the identity of the exact journaled decision that existed while
# admission held the jobs-row lock.  It is excluded from the caller payload
# digest and stripped before reconstructing JobCompleteRequest.
ACCEPTED_COMPLETION_DECISION_KEY = "_accepted_completion_decision"


class CompletionCommandError(RuntimeError):
    """Base class for accept failures with an HTTP-level retry policy."""


class CompletionCommandNotFound(CompletionCommandError):
    """The target job does not exist."""


class CompletionFenceRejected(CompletionCommandError):
    """The caller does not own the lane-specific completion fence."""


class CompletionPayloadMismatch(CompletionCommandError):
    """An idempotency key was reused with a different operation payload."""


class CompletionNonTerminalReport(CompletionPayloadMismatch):
    """A stateless report whose payload is not a terminal stop.

    §5.4.5 decision (6): only a TERMINAL report may close the queue row in
    the accept transaction. Rotations and recoverable-error continues go
    through the queue and must never reach ``/complete`` on this lane —
    half-accepting one would terminalize the unit under a job that stays
    ``processing``, wedging it invisibly: the done command escapes the
    exclusion view, the lane has no assigned agent for the orphan sweep and
    no jobs-row lease for the expiry sweep. Found live by the step-5
    hand-check (job a61d9940, command 2b028d0c). Subclasses
    ``CompletionPayloadMismatch`` for its 422 mapping: the client must not
    retry this payload.
    """


class CompletionInProgress(CompletionCommandError):
    """An exact duplicate exists but has not reached a terminal state."""

    def __init__(self, command_id: str, state: str) -> None:
        self.command_id = command_id
        self.state = state
        super().__init__(f"completion command {command_id} is {state}")


class CompletionTeardownInProgress(CompletionCommandError):
    """Fresh admission is fenced by an authorized external S36 callback."""

    def __init__(self, command_id: str, report_seq: int) -> None:
        self.command_id = str(command_id)
        self.report_seq = int(report_seq)
        super().__init__(
            f"completion command {self.command_id} workspace teardown is finalizing"
        )


class CompletionControlInProgress(CompletionCommandError):
    """Fresh admission lost to a durably claimed human control."""

    def __init__(self) -> None:
        super().__init__("job control is in progress")


@dataclass(frozen=True, slots=True)
class CompletionAcceptResult:
    """The durable result of fresh admission or an exact-key replay."""

    disposition: str
    command_id: str
    job_id: str
    report_seq: int
    state: str
    stored_payload: dict[str, Any]
    outcome: dict[str, Any] | None
    winning_report_seq: int | None
    abandoned_effects: tuple[str, ...]
    client_report_id: str
    queue_terminalized: bool
    accepted_job_status: str | None
    status_reorder_enabled: bool = False

    @property
    def replayed(self) -> bool:
        return self.disposition != "fresh"


def canonical_completion_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return only completion-operation fields, excluding all transport fences."""

    return {
        str(key): value
        for key, value in payload.items()
        if key
        not in {
            "lease_token",
            "agent_id",
            "client_report_id",
            ACCEPTED_COMPLETION_DECISION_KEY,
            ACCEPTED_IDLE_WAIT_SOURCE_KEY,
        }
    }


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def completion_payload_digest(job_id: str, payload: Mapping[str, Any]) -> str:
    """Fingerprint operation identity plus the canonical completion body."""

    operation = {
        "job_id": str(UUID(str(job_id))),
        "payload": canonical_completion_payload(payload),
    }
    return hashlib.sha256(_canonical_json(operation).encode("utf-8")).hexdigest()


def fallback_client_report_id(job_id: str, report_seq: int) -> str:
    """Synthesize the rolling-upgrade fallback required by section 5.4.5."""

    identity = f"{UUID(str(job_id))}:{int(report_seq)}"
    return str(uuid5(_FALLBACK_REPORT_NAMESPACE, identity))


def _json_object(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, str):
        parsed = json.loads(value)
        return dict(parsed) if isinstance(parsed, dict) else None
    return dict(value) if isinstance(value, Mapping) else None


def accepted_completion_decision_tool_call_id(
    payload: Mapping[str, Any] | None,
) -> str | None:
    """Return the server-captured completion-decision identity, if proven."""

    marker = (payload or {}).get(ACCEPTED_COMPLETION_DECISION_KEY)
    if not isinstance(marker, Mapping):
        return None
    tool_call_id = str(marker.get("tool_call_id") or "").strip()
    return tool_call_id or None


def _str_tuple(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(str(item) for item in value)


def _outcome_metadata(
    outcome: dict[str, Any] | None,
) -> tuple[int | None, tuple[str, ...]]:
    if not outcome:
        return None, ()
    winning = outcome.get("winning_report_seq")
    try:
        winning_seq = int(winning) if winning is not None else None
    except (TypeError, ValueError):
        winning_seq = None
    return winning_seq, _str_tuple(outcome.get("abandoned_effects"))


def _result_from_row(
    row: Any, *, disposition: str, queue_terminalized: bool
) -> CompletionAcceptResult:
    payload = _json_object(row["payload"]) or {}
    outcome = _json_object(row["outcome"])
    winning_report_seq, abandoned_effects = _outcome_metadata(outcome)
    return CompletionAcceptResult(
        disposition=disposition,
        command_id=str(row["id"]),
        job_id=str(row["job_id"]),
        report_seq=int(row["report_seq"]),
        state=str(row["state"]),
        stored_payload=payload,
        outcome=outcome,
        winning_report_seq=winning_report_seq,
        abandoned_effects=abandoned_effects,
        client_report_id=str(row["client_report_id"]),
        queue_terminalized=queue_terminalized,
        accepted_job_status=(
            str(row["accepted_job_status"])
            if row["accepted_job_status"] is not None
            else None
        ),
        status_reorder_enabled=bool(row.get("status_reorder_enabled", False)),
    )


@asynccontextmanager
async def _connection(source: Any) -> AsyncIterator[Any]:
    acquire = getattr(source, "acquire", None)
    if acquire is None:
        yield source
        return
    async with acquire() as conn:
        yield conn


def _same_uuid(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is right
    try:
        return UUID(str(left)) == UUID(str(right))
    except (TypeError, ValueError):
        return False


def _validate_replay_fence(
    row: Any,
    *,
    lease_token: int | None,
    agent_id: str | None,
) -> None:
    accepted_token = row["accepted_lease_token"]
    accepted_agent = row["accepted_agent_id"]
    if accepted_token is not None:
        if (
            agent_id is not None
            or lease_token is None
            or int(accepted_token) != int(lease_token)
        ):
            raise CompletionFenceRejected(
                "completion retry does not match accepted lease"
            )
        return
    if (
        lease_token is not None
        or agent_id is None
        or not _same_uuid(accepted_agent, agent_id)
    ):
        raise CompletionFenceRejected("completion retry does not match accepted agent")


def _replay_disposition(state: str) -> str:
    return {
        "done": "replay_done",
        "parked": "replay_parked",
        "superseded": "replay_superseded",
        "force_resolved": "replay_force_resolved",
    }[state]


async def accept_completion_command(
    db: Any,
    *,
    job_id: str,
    payload: Mapping[str, Any],
    lease_token: int | None,
    agent_id: str | None,
    client_report_id: str | None,
    requested_by: str,
    pinned_delivery_id: UUID | None = None,
    pinned_projection_digest: str | None = None,
    pinned_delivery_proof: str | None = None,
    pinned_process_generation: str | None = None,
    pinned_pod_uid: str | None = None,
    code_version: str | None = None,
    status_reorder_enabled: bool = False,
) -> CompletionAcceptResult:
    """Accept one immutable completion report in a short fenced transaction."""

    job_uuid = UUID(str(job_id))
    canonical_payload = canonical_completion_payload(payload)
    digest = completion_payload_digest(str(job_uuid), canonical_payload)
    supplied_report_uuid = UUID(str(client_report_id)) if client_report_id else None
    required_code_version = (
        COMPLETION_STATUS_REORDER_CODE_VERSION
        if status_reorder_enabled
        else COMPLETION_CODE_VERSION
    )
    if code_version is not None and str(code_version) != required_code_version:
        raise ValueError(
            "completion code version does not match persisted status-reorder capability"
        )
    selected_code_version = required_code_version

    async with _connection(db) as conn:
        async with conn.transaction():
            # Binding global order: queue first, jobs second.  Pinned jobs have
            # no queue row, but the lookup still precedes the jobs lock.
            queue = await conn.fetchrow(
                """
                SELECT unit_id, unit_kind, state, lease_token, input_seq
                FROM run_queue
                WHERE unit_id = $1::uuid
                FOR UPDATE
                """,
                job_uuid,
            )
            job = await conn.fetchrow(
                """
                SELECT id, status::text AS status, execution_lane,
                       assigned_agent_id, completion_seq_hwm, context,
                       parent_job_id, config_override, resolved_config,
                       (SELECT execution.harness_adapter FROM srw_execution_specs execution
                        WHERE execution.work_kind='Job' AND execution.work_id=jobs.id)
                       AS execution_harness_adapter,
                       extract(epoch FROM now())::float8 AS db_now_epoch
                FROM jobs
                WHERE id = $1::uuid
                FOR UPDATE
                """,
                job_uuid,
            )
            if job is None:
                raise CompletionCommandNotFound(f"job '{job_uuid}' not found")
            if job.get("execution_harness_adapter") not in (None, "srw/v1"):
                raise CompletionFenceRejected(
                    "This execution is owned by its manifest harness runtime"
                )

            # Exact-key lookup happens before current-owner validation.  The
            # accepted fence is immutable and remains replayable after accept
            # closes the queue or finalization clears agent assignment.
            if supplied_report_uuid is not None:
                existing = await conn.fetchrow(
                    """
                    SELECT id, job_id, report_seq, client_report_id, payload,
                           payload_digest, accepted_lease_token,
                           accepted_agent_id, accepted_job_status,
                           status_reorder_enabled, state, outcome
                    FROM job_completion_commands
                    WHERE job_id = $1::uuid AND client_report_id = $2::uuid
                    """,
                    job_uuid,
                    supplied_report_uuid,
                )
                if existing is not None:
                    if str(existing["payload_digest"]) != digest:
                        raise CompletionPayloadMismatch(
                            "client_report_id was reused with a different completion payload"
                        )
                    _validate_replay_fence(
                        existing,
                        lease_token=lease_token,
                        agent_id=agent_id,
                    )
                    state = str(existing["state"])
                    if state in {"pending", "finalizing"}:
                        raise CompletionInProgress(str(existing["id"]), state)
                    return _result_from_row(
                        existing,
                        disposition=_replay_disposition(state),
                        queue_terminalized=existing["accepted_lease_token"] is not None,
                    )

            # Exact-key replay stays valid after a human control fences the
            # old executor. Fresh admission, however, must lose before any
            # INSERT/HWM write. The owner fence is also rotated by claim_job;
            # this explicit marker check provides the truthful collision.
            from orchestrator.services.completion_control import (
                completion_control_claim_active,
            )

            if completion_control_claim_active(
                job.get("context"), now_epoch=job.get("db_now_epoch")
            ):
                raise CompletionControlInProgress

            if (
                job["execution_lane"] == "pinned"
                and await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM vm_idle_operations "
                    "WHERE owner_kind='job' AND owner_id=$1 "
                    "AND closed_at IS NULL)", job_uuid,
                )
            ):
                raise CompletionFenceRejected(
                    "pinned idle operation owns this Job's report authority"
                )

            lane = str(job["execution_lane"] or "pinned")
            accepted_token: int | None = None
            accepted_agent: UUID | None = None
            if lane == "stateless":
                if agent_id is not None or lease_token is None:
                    raise CompletionFenceRejected(
                        "stateless completion requires only a lease_token fence"
                    )
                if (
                    queue is None
                    or str(queue["unit_kind"]) != "worker_batch"
                    or str(queue["state"]) != "leased"
                    or int(queue["lease_token"]) != int(lease_token)
                ):
                    raise CompletionFenceRejected(
                        "completion report does not hold the current worker lease"
                    )
                accepted_token = int(lease_token)
                # Only a terminal report may pass: accept B4-terminalizes the
                # queue row below, and doing that for a continue-shaped
                # payload strands the job in `processing` with no rescuer
                # route (see CompletionNonTerminalReport). Pinned keeps
                # accepting should_stop=false — the loop-continue report is a
                # real pinned path.
                if not bool(canonical_payload.get("should_stop")):
                    raise CompletionNonTerminalReport(
                        "stateless completion accepts only terminal reports "
                        "(should_stop must be true); rotations and continues "
                        "release through the queue, never /complete"
                    )
            else:
                if lease_token is not None or agent_id is None:
                    raise CompletionFenceRejected(
                        "pinned completion requires only an agent_id fence"
                    )
                try:
                    accepted_agent = UUID(str(agent_id))
                except (TypeError, ValueError) as exc:
                    raise CompletionFenceRejected(
                        "pinned completion agent_id is invalid"
                    ) from exc
                if not _same_uuid(job["assigned_agent_id"], accepted_agent):
                    raise CompletionFenceRejected(
                        "completion report does not match the assigned agent"
                    )

            pinned_delivery = None
            if accepted_agent is not None and pinned_delivery_id is not None:
                from orchestrator.services.pinned_job_delivery import (
                    accept_pinned_report_on_conn,
                )

                pinned_delivery = await accept_pinned_report_on_conn(
                    conn, job_id=job_uuid, agent_id=accepted_agent,
                    delivery_id=pinned_delivery_id,
                    projection_digest=pinned_projection_digest,
                    process_generation=pinned_process_generation,
                    pod_uid=pinned_pod_uid,
                    delivery_proof=pinned_delivery_proof,
                    source_kind="completion",
                )

            # An authorized S36 callback may resume after every finalizer clock
            # has expired.  The jobs-row lock shared with its authorization is
            # the linearization point: exact-key replays above remain
            # replayable, while a fresh higher report is rejected before an HWM
            # bump or command INSERT can make teardown ownership ambiguous.
            from orchestrator.services.completion_teardown_authority import (
                active_workspace_teardown_authorization,
            )

            active_teardown = await active_workspace_teardown_authorization(
                conn, job_id=str(job_uuid)
            )
            if active_teardown is not None:
                raise CompletionTeardownInProgress(
                    active_teardown.command_id,
                    active_teardown.report_seq,
                )

            report_seq = int(job["completion_seq_hwm"] or 0) + 1
            report_uuid = supplied_report_uuid or UUID(
                fallback_client_report_id(str(job_uuid), report_seq)
            )

            # Capture the exact durable job_complete decision observed by
            # admission.  A later whole-command supersede may clear that
            # decision only when this identity still matches; a different or
            # unproven decision is parked for an operator instead of being
            # discarded.  The marker is server evidence, not caller input,
            # and therefore is not part of the idempotency digest above.
            stored_payload = dict(canonical_payload)
            job_context = _json_object(job.get("context")) or {}
            completion_decision = job_context.get("completion_decision")
            if isinstance(completion_decision, Mapping):
                tool_call_id = str(
                    completion_decision.get("tool_call_id") or ""
                ).strip()
                if tool_call_id:
                    stored_payload[ACCEPTED_COMPLETION_DECISION_KEY] = {
                        "tool_call_id": tool_call_id,
                    }
            idle_source = await capture_completion_wait_on_conn(
                conn,
                job=job,
                report=canonical_payload,
                lease_token=accepted_token,
                pinned_delivery=pinned_delivery,
                # Preserve legacy marker behavior above, but never invent new
                # idle evidence by stringifying malformed historical JSON.
                decision_tool_call_id=(
                    completion_decision.get("tool_call_id")
                    if isinstance(completion_decision, Mapping)
                    else None
                ),
            )
            if idle_source is not None:
                stored_payload[ACCEPTED_IDLE_WAIT_SOURCE_KEY] = idle_source
            payload_json = _canonical_json(stored_payload)

            # FIRST database write of the handler.  Cursor and queue writes
            # follow only after the immutable command is present.
            command = await conn.fetchrow(
                """
                INSERT INTO job_completion_commands (
                    job_id, report_seq, client_report_id, payload,
                    payload_digest, accepted_lease_token, accepted_agent_id,
                    accepted_job_status, origin, requested_by, deadline_at,
                    code_version, run_after, status_reorder_enabled
                ) VALUES (
                    $1::uuid, $2::bigint, $3::uuid, $4::jsonb,
                    $5::text, $6::bigint, $7::uuid,
                    $8::text, 'agent', $9::text,
                    now() + make_interval(secs => $10::float8), $11::text,
                    now() + make_interval(secs => $12::float8), $13::boolean
                )
                RETURNING id, job_id, report_seq, client_report_id, payload,
                          payload_digest, accepted_lease_token,
                          accepted_agent_id, accepted_job_status,
                          status_reorder_enabled, state, outcome
                """,
                job_uuid,
                report_seq,
                report_uuid,
                payload_json,
                digest,
                accepted_token,
                accepted_agent,
                str(job["status"]),
                requested_by,
                float(COMPLETION_DEADLINE_SECONDS),
                selected_code_version,
                COMPLETION_INLINE_GRACE_SECONDS,
                bool(status_reorder_enabled),
            )
            if idle_source is not None and pinned_delivery is not None:
                from orchestrator.services.pinned_job_delivery import (
                    record_pinned_wait_receipt_on_conn,
                )

                if await record_pinned_wait_receipt_on_conn(
                    conn, delivery=pinned_delivery, source_kind="completion",
                    source_id=command["id"],
                ) is None:
                    raise CompletionFenceRejected("pinned completion source changed")
            await conn.execute(
                """
                UPDATE jobs
                SET completion_seq_hwm = $2::bigint
                WHERE id = $1::uuid
                """,
                job_uuid,
                report_seq,
            )

            queue_terminalized = False
            if accepted_token is not None:
                queue_state = await complete_unit(
                    conn,
                    unit_id=job_uuid,
                    lease_token=accepted_token,
                    consumed_seq=queue["input_seq"],
                )
                if queue_state is None:
                    raise CompletionFenceRejected(
                        "worker lease changed before completion command commit"
                    )
                if queue_state != "done":
                    raise CompletionFenceRejected(
                        "new worker input arrived before terminal accept committed"
                    )
                queue_terminalized = True

            return _result_from_row(
                command,
                disposition="fresh",
                queue_terminalized=queue_terminalized,
            )


__all__ = [
    "ACCEPTED_COMPLETION_DECISION_KEY",
    "COMPLETION_CODE_VERSION",
    "COMPLETION_STATUS_REORDER_CODE_VERSION",
    "COMPLETION_SUPPORTED_CODE_VERSIONS",
    "COMPLETION_INLINE_GRACE_SECONDS",
    "CompletionAcceptResult",
    "CompletionCommandError",
    "CompletionCommandNotFound",
    "CompletionControlInProgress",
    "CompletionFenceRejected",
    "CompletionInProgress",
    "CompletionPayloadMismatch",
    "CompletionTeardownInProgress",
    "accept_completion_command",
    "accepted_completion_decision_tool_call_id",
    "canonical_completion_payload",
    "completion_payload_digest",
    "fallback_client_report_id",
]
