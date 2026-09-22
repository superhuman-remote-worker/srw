"""Command-aware admission barrier for job resume/control verbs.

The jobs row is the serialization point shared with completion-command accept.
Callers either use :meth:`guard_job` as an early HTTP preflight or use
:meth:`classify_on_conn` inside their mutation transaction.  Feature-flag
gating stays at the caller so the default-off path never names Gate-3 tables.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
from typing import Any
from uuid import UUID, uuid4

from orchestrator.services.operator_pause_hold import (
    OPERATOR_PAUSE_HOLD_CONTEXT_KEY,
    operator_pause_hold_jsonb_sql,
)


COMPLETION_FINALIZING_DETAIL = "completion finalizing"
COMPLETION_CONTROL_CLAIM_KEY = "_completion_control_claim"
COMPLETION_CONTROL_CLAIM_VERSION = 1
COMPLETION_DELIVERY_CONTROL_SOURCE = "completion_delivery"
# Every claimed external section is time-bounded below 30 minutes. Keep a
# generous lease margin for scheduler stalls while retaining bounded automatic
# recovery after an orchestrator crash.
COMPLETION_CONTROL_CLAIM_SECONDS = 2 * 60 * 60


class CompletionControlClaimConflict(RuntimeError):
    """A control lost its jobs-row/command serialization point."""


@dataclass(frozen=True, slots=True)
class CompletionControlClaim:
    job_id: str
    claim_id: str
    source: str
    expected_status: str
    expected_lane: str
    fence_kind: str
    fence_value: str


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def completion_control_claim_active(
    context: Any,
    *,
    now_epoch: float | None = None,
) -> bool:
    """Fail-closed active-marker test shared with completion acceptance."""

    parsed = _json_object(context)
    if COMPLETION_CONTROL_CLAIM_KEY not in parsed:
        return False
    marker = parsed[COMPLETION_CONTROL_CLAIM_KEY]
    if not isinstance(marker, dict):
        return True
    # A malformed reserved marker is evidence of an interrupted/unknown
    # controller. Never let an agent report silently route around it.
    if marker.get("version") != COMPLETION_CONTROL_CLAIM_VERSION:
        return True
    raw_expiry = marker.get("expires_epoch")
    if not isinstance(raw_expiry, (int, float)) or isinstance(raw_expiry, bool):
        return True
    try:
        expiry = float(raw_expiry)
    except (OverflowError, TypeError, ValueError):
        return True
    if not math.isfinite(expiry):
        return True
    import time

    observed = float(now_epoch) if now_epoch is not None else time.time()
    return expiry > observed


def completion_control_claim_detail(context: Any) -> str:
    """Describe an active claim for control 409s; the bare prefix is load-bearing."""

    detail = "job control is in progress"
    marker = _json_object(context).get(COMPLETION_CONTROL_CLAIM_KEY)
    if not isinstance(marker, dict):
        return detail
    if marker.get("version") != COMPLETION_CONTROL_CLAIM_VERSION:
        return detail
    source = marker.get("source")
    if not isinstance(source, str) or not 0 < len(source) <= 64:
        return detail
    raw_expiry = marker.get("expires_epoch")
    if not isinstance(raw_expiry, (int, float)) or isinstance(raw_expiry, bool):
        return detail
    try:
        expires_at = datetime.fromtimestamp(float(raw_expiry), tz=timezone.utc)
    except (OSError, OverflowError, ValueError):
        return detail
    stamp = expires_at.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return f"{detail} (source={source}, expires_at={stamp})"


def completion_control_claim_owned_active(
    context: Any,
    claim_id: str,
    *,
    now_epoch: float,
) -> bool:
    """Require this exact claim and a valid, unexpired fixed-shape marker."""

    parsed = _json_object(context)
    marker = parsed.get(COMPLETION_CONTROL_CLAIM_KEY)
    if not isinstance(marker, dict) or marker.get("claim_id") != claim_id:
        return False
    if marker.get("version") != COMPLETION_CONTROL_CLAIM_VERSION:
        return False
    raw_expiry = marker.get("expires_epoch")
    if not isinstance(raw_expiry, (int, float)) or isinstance(raw_expiry, bool):
        return False
    try:
        expiry = float(raw_expiry)
    except (OverflowError, TypeError, ValueError):
        return False
    return math.isfinite(expiry) and expiry > float(now_epoch)


def completion_delivery_control_claim_owned_active(
    context: Any,
    command_id: str,
    *,
    now_epoch: float,
) -> bool:
    """Require the fixed command-owned marker used by completion delivery.

    ``claim_id`` is the stable crash-adoptable owner.  The separate fence
    fields make this reserved marker distinguishable from a human-control
    claim that happens to reuse an identifier, and let operator resolution
    retain or clear only this command's exact delivery barrier.
    """

    canonical_command_id = str(command_id)
    if not completion_control_claim_owned_active(
        context,
        canonical_command_id,
        now_epoch=now_epoch,
    ):
        return False
    marker = _json_object(context).get(COMPLETION_CONTROL_CLAIM_KEY)
    return bool(
        isinstance(marker, dict)
        and marker.get("source") == COMPLETION_DELIVERY_CONTROL_SOURCE
        and marker.get("fence_kind") == "completion_command"
        and marker.get("fence_value") == canonical_command_id
    )


@dataclass(frozen=True, slots=True)
class CompletionControlDecision:
    job_id: str
    blocked: bool
    route: str | None = None
    command_id: str | None = None

    @property
    def actionable(self) -> bool:
        return self.route in {"resume_finalizer", "park_alert", "alert_only"}


class CompletionControl:
    """Classify controls against the authoritative unfinished-command view."""

    def __init__(self, db: Any, router: Any) -> None:
        self.db = db
        self.router = router

    async def classify_on_conn(
        self, conn: Any, job_id: UUID | str
    ) -> CompletionControlDecision:
        canonical = UUID(str(job_id))
        locked = await conn.fetchval(
            "SELECT id FROM jobs WHERE id=$1::uuid FOR UPDATE",
            canonical,
        )
        if locked is None:
            return CompletionControlDecision(str(canonical), blocked=False)
        row = await conn.fetchrow(
            """
            SELECT command_id, route
            FROM job_completion_sweep_exclusions
            WHERE job_id=$1::uuid
            """,
            canonical,
        )
        if row is None:
            return CompletionControlDecision(str(canonical), blocked=False)
        return CompletionControlDecision(
            str(canonical),
            blocked=True,
            route=str(row["route"]),
            command_id=str(row["command_id"]),
        )

    async def guard_job(
        self, job_id: UUID | str, *, source: str
    ) -> CompletionControlDecision:
        """Classify under the jobs lock and durably nudge non-live routes."""

        async with self.db.acquire() as conn:
            async with conn.transaction():
                decision = await self.classify_on_conn(conn, job_id)
        if decision.blocked and decision.actionable:
            # Allocation is bounded DB work only.  The background router owns
            # finalizer execution, so a public control never waits on a
            # potentially 900-second completion workflow.
            await self.router.enqueue_job(job_id, source=source)
        return decision

    async def claim_job(
        self,
        job_id: UUID | str,
        *,
        source: str,
        expected_status: str,
        expected_lane: str,
    ) -> CompletionControlClaim:
        """Fence the current executor and durably claim one human mutation.

        The marker is intentionally a small fixed-shape object, not an
        append-only log.  ``assigned_agent_id`` / ``run_queue.lease_token``
        remain the actual agent fences.  An expired marker can therefore be
        recovered without ever making the predecessor's report valid again.
        """

        canonical = UUID(str(job_id))
        bounded_source = str(source).strip()
        if not bounded_source or len(bounded_source) > 64:
            raise ValueError("completion control source must contain 1-64 characters")
        if expected_lane not in {"pinned", "stateless"}:
            raise ValueError("completion control lane must be pinned or stateless")
        if not expected_status or len(expected_status) > 64:
            raise ValueError("completion control expected status is invalid")

        blocked: CompletionControlDecision | None = None
        claim: CompletionControlClaim | None = None
        async with self.db.acquire() as conn:
            async with conn.transaction():
                # Global order: queue first, jobs second. Pinned jobs normally
                # have no row; the lookup still precedes the jobs lock.
                queue = await conn.fetchrow(
                    """
                    SELECT unit_kind, state, lease_token
                    FROM run_queue
                    WHERE unit_id=$1::uuid
                    FOR UPDATE
                    """,
                    canonical,
                )
                job = await conn.fetchrow(
                    """
                    SELECT status::text AS status, execution_lane,
                           assigned_agent_id, context,
                           extract(epoch FROM clock_timestamp())::float8
                               AS db_now_epoch
                    FROM jobs
                    WHERE id=$1::uuid
                    FOR UPDATE
                    """,
                    canonical,
                )
                if job is None:
                    raise CompletionControlClaimConflict("job not found")
                if (
                    str(job["status"]) != expected_status
                    or str(job["execution_lane"] or "pinned") != expected_lane
                ):
                    raise CompletionControlClaimConflict(
                        "job changed while control was being claimed"
                    )

                route = await conn.fetchrow(
                    """
                    SELECT command_id, route
                    FROM job_completion_sweep_exclusions
                    WHERE job_id=$1::uuid
                    """,
                    canonical,
                )
                if route is not None:
                    blocked = CompletionControlDecision(
                        str(canonical),
                        blocked=True,
                        route=str(route["route"]),
                        command_id=str(route["command_id"]),
                    )
                elif completion_control_claim_active(
                    job["context"], now_epoch=job["db_now_epoch"]
                ):
                    raise CompletionControlClaimConflict(
                        "job control is already in progress"
                    )
                else:
                    claim_id = uuid4()
                    if expected_lane == "stateless":
                        if queue is None or str(queue["unit_kind"]) != "worker_batch":
                            raise CompletionControlClaimConflict(
                                "stateless worker queue is missing"
                            )
                        fence_kind = "worker_lease"
                        fence_value = str(int(queue["lease_token"]))
                        closed = await conn.fetchrow(
                            """
                            UPDATE run_queue
                            SET state='done', lease_token=lease_token + 1,
                                attempts_since_completion=0,
                                leased_by=NULL, last_leased_by=NULL,
                                leased_until=NULL,
                                interrupt_admission_lease_token=NULL,
                                interrupt_admission_turn_id=NULL,
                                run_after=now(), queued_at=now()
                            WHERE unit_id=$1::uuid
                              AND unit_kind='worker_batch'
                              AND lease_token=$2::bigint
                            RETURNING lease_token
                            """,
                            canonical,
                            int(queue["lease_token"]),
                        )
                        if closed is None:
                            raise CompletionControlClaimConflict(
                                "worker ownership changed while control was claimed"
                            )
                    else:
                        fence_kind = "assigned_agent"
                        fence_value = (
                            str(job["assigned_agent_id"])
                            if job["assigned_agent_id"] is not None
                            else "unassigned"
                        )

                    # The pinned lane fences on the assigned agent, so the
                    # claim detaches it. The stateless lane fences on the queue
                    # lease closed just above and keeps assigned_agent_id NULL
                    # by construction — naming the column there would drag this
                    # release into migration 0175's dispatch trigger, which
                    # classifies a processing/stateless/unassigned row as a
                    # worker claim and refuses it for lacking a dispatch marker.
                    release_agent_sql = (
                        ""
                        if expected_lane == "stateless"
                        else "assigned_agent_id=NULL,"
                    )
                    updated = await conn.fetchrow(
                        f"""
                        UPDATE jobs
                        SET {release_agent_sql}
                            context=jsonb_set(
                                COALESCE(context, '{{}}'::jsonb),
                                '{{{COMPLETION_CONTROL_CLAIM_KEY}}}',
                                jsonb_build_object(
                                    'version', $2::int,
                                    'claim_id', $3::text,
                                    'source', $4::text,
                                    'expected_status', $5::text,
                                    'expected_lane', $6::text,
                                    'fence_kind', $7::text,
                                    'fence_value', $8::text,
                                    'claimed_at', to_jsonb(now()),
                                    'expires_epoch', to_jsonb(
                                        extract(epoch FROM now()) + $9::float8
                                    )
                                ),
                                true
                            ),
                            updated_at=CURRENT_TIMESTAMP
                        WHERE id=$1::uuid
                          AND status::text=$5::text
                          AND execution_lane=$6::text
                        RETURNING id
                        """,
                        canonical,
                        COMPLETION_CONTROL_CLAIM_VERSION,
                        str(claim_id),
                        bounded_source,
                        expected_status,
                        expected_lane,
                        fence_kind,
                        fence_value,
                        float(COMPLETION_CONTROL_CLAIM_SECONDS),
                    )
                    if updated is None:
                        raise CompletionControlClaimConflict(
                            "job changed while control was being claimed"
                        )
                    claim = CompletionControlClaim(
                        job_id=str(canonical),
                        claim_id=str(claim_id),
                        source=bounded_source,
                        expected_status=expected_status,
                        expected_lane=expected_lane,
                        fence_kind=fence_kind,
                        fence_value=fence_value,
                    )

        if blocked is not None:
            if blocked.actionable:
                await self.router.enqueue_job(canonical, source=bounded_source)
            raise CompletionControlClaimConflict(COMPLETION_FINALIZING_DETAIL)
        if claim is None:  # pragma: no cover - defensive exhaustiveness
            raise CompletionControlClaimConflict("control claim was not created")
        return claim

    async def claim_pause_job(
        self,
        job_id: UUID | str,
        *,
        source: str,
        expected_agent_id: str | None,
        operator_hold: bool = False,
        paused_by: str | None = None,
    ) -> CompletionControlClaim:
        """Publish a pinned pause and retain an external-I/O exclusion marker.

        Pause deliberately does not reject an already accepted completion
        command: whichever jobs-row status write wins is authoritative. The
        marker only spans the subsequent old-agent/VM pause calls, preventing
        a dispatcher or another control from exposing a successor to stale I/O.

        ``operator_hold`` also stamps the durable operator pause hold in the
        same write (see :mod:`orchestrator.services.operator_pause_hold`); it
        outlives this claim so the released row is not redispatched.
        """

        canonical = UUID(str(job_id))
        bounded_source = str(source).strip()
        if not bounded_source or len(bounded_source) > 64:
            raise ValueError("completion control source must contain 1-64 characters")
        expected_agent = (
            UUID(str(expected_agent_id)) if expected_agent_id is not None else None
        )
        claim_context = f"""jsonb_set(
                            COALESCE(context, '{{}}'::jsonb),
                            '{{{COMPLETION_CONTROL_CLAIM_KEY}}}',
                            jsonb_build_object(
                                'version', $2::int,
                                'claim_id', $3::text,
                                'source', $4::text,
                                'expected_status', 'paused',
                                'expected_lane', 'pinned',
                                'fence_kind', 'assigned_agent',
                                'fence_value', $5::text,
                                'claimed_at', to_jsonb(now()),
                                'expires_epoch', to_jsonb(
                                    extract(epoch FROM now()) + $6::float8
                                )
                            ),
                            true
                        )"""
        hold_args: tuple[Any, ...] = ()
        if operator_hold:
            claim_context = (
                f"jsonb_set({claim_context}, "
                f"'{{{OPERATOR_PAUSE_HOLD_CONTEXT_KEY}}}', "
                + operator_pause_hold_jsonb_sql(
                    hold_id_parameter="$8",
                    source_parameter="$4",
                    paused_by_parameter="$9",
                )
                + ", true)"
            )
            hold_args = (str(uuid4()), paused_by)

        async with self.db.acquire() as conn:
            async with conn.transaction():
                await conn.fetchrow(
                    "SELECT unit_id FROM run_queue WHERE unit_id=$1::uuid FOR UPDATE",
                    canonical,
                )
                job = await conn.fetchrow(
                    """
                    SELECT status::text AS status, execution_lane,
                           assigned_agent_id, context,
                           extract(epoch FROM clock_timestamp())::float8
                               AS db_now_epoch
                    FROM jobs WHERE id=$1::uuid FOR UPDATE
                    """,
                    canonical,
                )
                if (
                    job is None
                    or str(job["status"]) != "processing"
                    or str(job["execution_lane"] or "pinned") != "pinned"
                    or (
                        UUID(str(job["assigned_agent_id"]))
                        if job["assigned_agent_id"] is not None
                        else None
                    )
                    != expected_agent
                ):
                    raise CompletionControlClaimConflict(
                        "job changed while pause was being claimed"
                    )
                if completion_control_claim_active(
                    job["context"], now_epoch=job["db_now_epoch"]
                ):
                    raise CompletionControlClaimConflict(
                        "job control is already in progress"
                    )

                claim_id = uuid4()
                fence_value = (
                    str(job["assigned_agent_id"])
                    if job["assigned_agent_id"] is not None
                    else "unassigned"
                )
                updated = await conn.fetchrow(
                    f"""
                    UPDATE jobs
                    SET status='paused', assigned_agent_id=NULL,
                        context={claim_context},
                        updated_at=CURRENT_TIMESTAMP
                    WHERE id=$1::uuid
                      AND status='processing'
                      AND execution_lane='pinned'
                      AND assigned_agent_id IS NOT DISTINCT FROM $7::uuid
                    RETURNING id
                    """,
                    canonical,
                    COMPLETION_CONTROL_CLAIM_VERSION,
                    str(claim_id),
                    bounded_source,
                    fence_value,
                    float(COMPLETION_CONTROL_CLAIM_SECONDS),
                    expected_agent,
                    *hold_args,
                )
                if updated is None:
                    raise CompletionControlClaimConflict(
                        "job changed while pause was being claimed"
                    )
        return CompletionControlClaim(
            job_id=str(canonical),
            claim_id=str(claim_id),
            source=bounded_source,
            expected_status="paused",
            expected_lane="pinned",
            fence_kind="assigned_agent",
            fence_value=fence_value,
        )

    @asynccontextmanager
    async def finish_claim(self, claim: CompletionControlClaim):
        """Yield the winning mutation transaction, consuming its marker.

        Callers perform only bounded database mutation in this context. Slow
        cloud/file calls happen after :meth:`claim_job` and before this method.
        Any exception rolls the mutation back and leaves the marker for the
        caller's explicit ``abort_claim`` recovery policy.
        """

        canonical = UUID(claim.job_id)
        async with self.db.acquire() as conn:
            async with conn.transaction():
                await conn.fetchrow(
                    "SELECT unit_id FROM run_queue WHERE unit_id=$1::uuid FOR UPDATE",
                    canonical,
                )
                job = await conn.fetchrow(
                    """
                    SELECT status::text AS status, execution_lane, context,
                           extract(epoch FROM clock_timestamp())::float8
                               AS db_now_epoch
                    FROM jobs WHERE id=$1::uuid FOR UPDATE
                    """,
                    canonical,
                )
                if (
                    job is None
                    or str(job["status"]) != claim.expected_status
                    or str(job["execution_lane"] or "pinned") != claim.expected_lane
                    or not completion_control_claim_owned_active(
                        job["context"],
                        claim.claim_id,
                        now_epoch=float(job["db_now_epoch"]),
                    )
                ):
                    raise CompletionControlClaimConflict(
                        "job changed while control was being committed"
                    )
                route = await conn.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM job_completion_sweep_exclusions
                        WHERE job_id=$1::uuid
                    )
                    """,
                    canonical,
                )
                if route:
                    raise CompletionControlClaimConflict(COMPLETION_FINALIZING_DETAIL)
                yield conn, job
                cleared = await conn.fetchrow(
                    f"""
                    UPDATE jobs
                    SET context=COALESCE(context, '{{}}'::jsonb)
                                - '{COMPLETION_CONTROL_CLAIM_KEY}',
                        updated_at=CURRENT_TIMESTAMP
                    WHERE id=$1::uuid
                      AND context->'{COMPLETION_CONTROL_CLAIM_KEY}'
                                 ->>'claim_id'=$2::text
                      AND jsonb_typeof(
                            context->'{COMPLETION_CONTROL_CLAIM_KEY}'
                                      ->'expires_epoch'
                          )='number'
                      AND (
                            context->'{COMPLETION_CONTROL_CLAIM_KEY}'
                                      ->'expires_epoch'#>>'{{}}'
                          )::numeric > extract(epoch FROM clock_timestamp())
                    RETURNING id
                    """,
                    canonical,
                    claim.claim_id,
                )
                if cleared is None:
                    raise CompletionControlClaimConflict(
                        "control claim changed before commit"
                    )

    async def abort_claim(self, claim: CompletionControlClaim) -> bool:
        """Release only the caller's exact marker; owner fences stay rotated."""

        canonical = UUID(claim.job_id)
        async with self.db.acquire() as conn:
            async with conn.transaction():
                await conn.fetchrow(
                    "SELECT unit_id FROM run_queue WHERE unit_id=$1::uuid FOR UPDATE",
                    canonical,
                )
                row = await conn.fetchrow(
                    f"""
                    UPDATE jobs
                    SET context=COALESCE(context, '{{}}'::jsonb)
                                - '{COMPLETION_CONTROL_CLAIM_KEY}',
                        updated_at=CURRENT_TIMESTAMP
                    WHERE id=$1::uuid
                      AND context->'{COMPLETION_CONTROL_CLAIM_KEY}'
                                 ->>'claim_id'=$2::text
                    RETURNING id
                    """,
                    canonical,
                    claim.claim_id,
                )
        return row is not None


__all__ = [
    "COMPLETION_FINALIZING_DETAIL",
    "COMPLETION_CONTROL_CLAIM_KEY",
    "CompletionControlClaim",
    "CompletionControlClaimConflict",
    "CompletionControl",
    "CompletionControlDecision",
    "completion_control_claim_active",
    "completion_control_claim_detail",
    "completion_control_claim_owned_active",
]
