"""Connector credential leases: issue, re-deliver, renew, revoke (slice C2).

A connector whose driver delivers by lease (``DriverSpec.credential_delivery
== "lease"``) never sends its upstream credential to an agent. SRW issues one
lease per workspace-owning execution and connector and delivers the lease's
``scl_`` token in the ``lease_token`` form; a driver exchanges it for the
credential on the dedicated exchange port
(:mod:`orchestrator.services.connector_lease_exchange`).

* **Owner.** The workspace-owning execution: a Job or a session thread. A
  child Job on its parent's workspace uses the parent's lease
  (``stateless_worker_workspace_owner``), so a child's completion never
  revokes what the parent's workspace holds.
* **Re-delivery, not rotation.** The token is stored as a SHA-256 digest for
  lookup and as an ``APP_ENCRYPTION_KEY`` ciphertext, so every claim, attach
  and pod recycle receives the same token. Rotation happens only when a lease
  ends and a new one is issued (resume after a pause or an End).
* **Expiry.** A lease expires ``LEASE_TTL_SECONDS`` after its last renewal.
  Only :func:`connector_lease_sweeper` renews, and only leases whose
  execution is live in durable state (a processing Job, or one with a
  processing child on its workspace; a thread that has not ended, idle or
  not). It writes only leases in the second half of their window, as the
  runtime-actor liveness slide does. A lease never renews itself, paused and
  ``pending_review`` Jobs lapse, and there is no hard maximum.
* **Revocation** is an UPDATE at the execution's terminal transactions (End,
  cancel, delete, completion, a live detach); pod events never revoke. The
  revoke functions take the caller's connection so they commit with the
  terminal decision.
* **Audit.** Issue, revoke, every denied exchange and the first exchange of
  a lease go to ``security_events``; later exchanges only update counters.

Design: knowledge-base/knowledge/features/connector_drivers.md, "The lease
service".
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from shared.connectors.builtin import spec_for_row
from shared.connectors.contract import DriverSpec
from shared.connectors.leases import (
    LEASE_TOKEN_PREFIX,
    last_four,
    lease_access,
    mint_token,
    token_digest,
    token_shape_valid,
)

logger = logging.getLogger(__name__)

#: Seconds a lease lives after its last renewal (decision 9: 15 minutes).
LEASE_TTL_SECONDS = max(60, int(os.environ.get("CONNECTOR_LEASE_TTL_SECONDS", "900")))
#: The renewal throttle: only leases with less than this left are written.
RENEW_BELOW_SECONDS = LEASE_TTL_SECONDS // 2
#: Seconds between sweeps. A quarter of the TTL at most, so a live lease is
#: renewed at least once in its second half.
SWEEP_INTERVAL_SECONDS = max(
    5.0,
    min(
        float(os.environ.get("CONNECTOR_LEASE_SWEEP_INTERVAL_SECONDS", "60")),
        LEASE_TTL_SECONDS / 4,
    ),
)

#: Why a lease ended; ``expired`` is written by the sweeper, never revoked.
RevokeReason = Literal[
    "session_end",
    "job_cancelled",
    "job_deleted",
    "job_completed",
    "job_failed",
    "connector_detached",
    "execution_ended",
    "execution_terminal",
]
EXPIRED = "expired"
_OWNER_COLUMNS = {"job": "job_id", "thread": "thread_id"}


class LeaseDeliveryError(RuntimeError):
    """A lease could not be issued or its stored token could not be read."""


@dataclass(frozen=True, slots=True)
class LeaseOwner:
    """The workspace-owning execution a lease belongs to."""

    kind: Literal["job", "thread"]
    id: str

    def __post_init__(self) -> None:
        if self.kind not in _OWNER_COLUMNS:
            raise ValueError(f"unknown lease owner kind {self.kind!r}")

    @classmethod
    def job(cls, job_id: str) -> LeaseOwner:
        return cls("job", str(job_id))

    @classmethod
    def thread(cls, thread_id: str) -> LeaseOwner:
        return cls("thread", str(thread_id))

    @classmethod
    def of_workspace(cls, owner: Any) -> LeaseOwner:
        """From a ``WorkspaceOwner`` (``job`` or ``session``)."""
        return cls("thread" if owner.kind == "session" else "job", str(owner.id))

    @property
    def column(self) -> str:
        return _OWNER_COLUMNS[self.kind]


def job_lease_owner(job: Mapping[str, Any]) -> LeaseOwner:
    """The lease owner of a Job: its parent when it runs on the parent's
    workspace (``stateless_worker_workspace_owner``), else itself."""
    from orchestrator.services.job_workspace_runtime import (
        stateless_worker_workspace_owner,
    )

    return LeaseOwner.job(stateless_worker_workspace_owner(dict(job)).id)


@dataclass(frozen=True, slots=True)
class DeliveredLease:
    """A live lease and its token, for one delivery."""

    id: str
    token: str
    connector_id: str
    access: str
    expires_at: datetime
    issued: bool


# =============================================================================
# Audit
# =============================================================================


async def record_lease_event(
    conn: Any,
    *,
    event_type: str,
    resource_type: str,
    resource_id: str | None,
    detail: str,
) -> None:
    """Write one ``security_events`` row on ``conn``. Never raises.

    The structured log line goes first, as ``log_security_event`` does. The
    insert runs in a savepoint, so a failed audit write neither aborts the
    caller's transaction nor blocks the decision it documents; inside that
    transaction it commits (or rolls back) together with the decision.
    """
    logger.warning(
        "security-event %s: resource=%s/%s detail=%s",
        event_type,
        resource_type,
        resource_id,
        detail,
    )
    try:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO security_events
                    (event_type, resource_type, resource_id, detail)
                VALUES ($1, $2, $3, $4)
                """,
                event_type,
                resource_type,
                resource_id,
                detail,
            )
    except Exception as exc:
        logger.error("security-event DB write failed (decision proceeds): %s", exc)


def _detail(**fields: Any) -> str:
    return " ".join(f"{key}={value}" for key, value in fields.items() if value)


# =============================================================================
# Issue and re-delivery
# =============================================================================


def lease_spec(entry: Mapping[str, Any]) -> DriverSpec | None:
    """The driver spec of a payload entry that is delivered by lease."""
    spec = spec_for_row(entry)
    return spec if spec is not None and spec.credential_delivery == "lease" else None


def needs_leases(entries: Sequence[Any] | None) -> bool:
    """Whether any entry of a datasources payload is delivered by lease."""
    return any(isinstance(e, Mapping) and lease_spec(e) for e in entries or ())


def _encrypt(token: str) -> str:
    from orchestrator.security.crypto import encrypt

    return encrypt(token)


def _decrypt(ciphertext: Any) -> str:
    from orchestrator.security.crypto import DecryptionError, decrypt

    try:
        token = decrypt(str(ciphertext))
    except (DecryptionError, RuntimeError, ValueError, TypeError) as exc:
        raise LeaseDeliveryError("A stored lease token cannot be read") from exc
    if not token_shape_valid(token, LEASE_TOKEN_PREFIX):
        raise LeaseDeliveryError("A stored lease token is malformed")
    return token


async def issue_or_redeliver(
    conn: Any,
    *,
    owner: LeaseOwner,
    connector_id: str,
    driver: str,
    access: str,
    image_digest: str | None = None,
    ttl_seconds: int | None = None,
) -> DeliveredLease:
    """The live lease of ``owner`` for ``connector_id``, issuing one if none.

    An expired lease is retired first (``revoked_at`` = its expiry, reason
    ``expired``), so the one-live-lease index admits the new one. A live lease
    is delivered again with its stored token; its access level follows the
    connector's current one. A concurrent issuer that wins the unique index is
    read back, never duplicated.
    """
    ttl = int(ttl_seconds or LEASE_TTL_SECONDS)
    connector_uuid = UUID(str(connector_id))
    owner_uuid = UUID(owner.id)
    column = owner.column
    async with conn.transaction():
        await conn.execute(
            f"""
            UPDATE connector_credential_leases
               SET revoked_at = expires_at, revoke_reason = '{EXPIRED}'
             WHERE {column} = $1 AND connector_id = $2
               AND revoked_at IS NULL AND expires_at <= now()
            """,
            owner_uuid,
            connector_uuid,
        )
        live = await _live_lease(conn, column, owner_uuid, connector_uuid)
        if live is None:
            if not await _owner_accepts_leases(conn, owner):
                # A terminal decision (End, cancel, completion) revoked the
                # execution's leases; a delivery racing it must not mint one
                # the sweeper would then keep alive.
                raise LeaseDeliveryError("The execution no longer accepts leases")
            token = mint_token(LEASE_TOKEN_PREFIX)
            inserted = await conn.fetchrow(
                f"""
                INSERT INTO connector_credential_leases
                    (token_hash, token_ciphertext, token_last_four, {column},
                     connector_id, driver, image_digest, access, expires_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
                        now() + make_interval(secs => $9::int))
                ON CONFLICT ({column}, connector_id)
                    WHERE revoked_at IS NULL AND {column} IS NOT NULL
                    DO NOTHING
                RETURNING id, access, expires_at
                """,
                token_digest(token),
                _encrypt(token),
                last_four(token),
                owner_uuid,
                connector_uuid,
                driver,
                image_digest,
                access,
                ttl,
            )
            if inserted is not None:
                await record_lease_event(
                    conn,
                    event_type="connector_lease_issued",
                    resource_type="connector_lease",
                    resource_id=str(inserted["id"]),
                    detail=_detail(
                        owner=f"{owner.kind}:{owner.id}",
                        connector=connector_id,
                        driver=driver,
                        access=access,
                        token_last_four=last_four(token),
                    ),
                )
                return DeliveredLease(
                    id=str(inserted["id"]),
                    token=token,
                    connector_id=str(connector_uuid),
                    access=str(inserted["access"]),
                    expires_at=inserted["expires_at"],
                    issued=True,
                )
            live = await _live_lease(conn, column, owner_uuid, connector_uuid)
            if live is None:
                raise LeaseDeliveryError("A concurrent lease vanished during issue")
        if str(live["access"]) != access:
            await conn.execute(
                "UPDATE connector_credential_leases SET access = $2 WHERE id = $1",
                live["id"],
                access,
            )
        return DeliveredLease(
            id=str(live["id"]),
            token=_decrypt(live["token_ciphertext"]),
            connector_id=str(connector_uuid),
            access=access,
            expires_at=live["expires_at"],
            issued=False,
        )


async def _owner_accepts_leases(conn: Any, owner: LeaseOwner) -> bool:
    if owner.kind == "job":
        query = (
            "SELECT 1 FROM jobs WHERE id = $1::uuid "
            "AND status::text NOT IN ('completed', 'failed', 'cancelled')"
        )
    else:
        query = (
            "SELECT 1 FROM threads WHERE id = $1::uuid "
            "AND status::text <> 'ended' AND runtime_retirement_token IS NULL"
        )
    return await conn.fetchval(query, UUID(owner.id)) is not None


async def _live_lease(
    conn: Any, column: str, owner_uuid: UUID, connector_uuid: UUID
) -> Any:
    return await conn.fetchrow(
        f"""
        SELECT id, token_ciphertext, access, expires_at
          FROM connector_credential_leases
         WHERE {column} = $1 AND connector_id = $2
           AND revoked_at IS NULL AND expires_at > now()
         FOR UPDATE
        """,
        owner_uuid,
        connector_uuid,
    )


async def deliver_connector_leases(
    conn: Any,
    entries: Sequence[Any] | None,
    *,
    owner: LeaseOwner,
    ttl_seconds: int | None = None,
) -> int:
    """Put a lease token into every lease-delivered entry, in place.

    ``entries`` is a datasources payload built by the drivers' ``bind``. A
    lease entry's ``credentials`` become ``{"lease": {id, connector_id,
    token}}`` and nothing else, so no upstream secret can ride along; an
    entry SRW cannot lease delivers no token. Returns how many entries carry
    a lease. The caller holds the connection (and the transaction the
    delivery belongs to).
    """
    delivered = 0
    for entry in entries or ():
        if not isinstance(entry, dict):
            continue
        spec = lease_spec(entry)
        if spec is None:
            continue
        entry["credentials"] = {}
        connector_id = str(entry.get("datasource_id") or "")
        access = lease_access(entry, spec)
        try:
            UUID(connector_id)
        except ValueError:
            logger.warning(
                "A %s connector entry names no connector id; no lease delivered",
                spec.name,
            )
            continue
        if access is None:
            logger.warning(
                "Driver %s has no access levels; no lease delivered", spec.name
            )
            continue
        lease = await issue_or_redeliver(
            conn,
            owner=owner,
            connector_id=connector_id,
            driver=spec.name,
            access=access,
            ttl_seconds=ttl_seconds,
        )
        entry["credentials"] = {
            "lease": {
                "id": lease.id,
                "connector_id": lease.connector_id,
                "token": lease.token,
            }
        }
        delivered += 1
    return delivered


async def deliver_connector_leases_with(
    db: Any,
    entries: Sequence[Any] | None,
    *,
    owner: LeaseOwner,
) -> int:
    """:func:`deliver_connector_leases` on a connection from ``db``.

    A payload without lease entries never touches the database, so stores
    that are fakes in a test keep working. Inside ``transaction_scope`` the
    acquired connection is the scope's.
    """
    if not needs_leases(entries):
        return 0
    async with db.acquire() as conn:
        return await deliver_connector_leases(conn, entries, owner=owner)


# =============================================================================
# Revocation
# =============================================================================


async def _revoke(
    conn: Any,
    *,
    where: str,
    args: Sequence[Any],
    reason: str,
) -> list[str]:
    """Revoke the live leases matching ``where``; audit the revoked ones.

    A lease already past its expiry is retired as ``expired`` instead: it was
    unusable before this decision, so it is not counted as revoked.
    """
    rows = await conn.fetch(
        f"""
        UPDATE connector_credential_leases AS lease
           SET revoked_at = LEAST(now(), lease.expires_at),
               revoke_reason = CASE WHEN lease.expires_at <= now()
                                    THEN '{EXPIRED}'
                                    ELSE ${len(args) + 1}::text END
         WHERE lease.revoked_at IS NULL AND ({where})
        RETURNING lease.id, lease.job_id, lease.thread_id, lease.connector_id,
                  lease.driver, lease.token_last_four, lease.revoke_reason
        """,
        *args,
        reason,
    )
    revoked: list[str] = []
    for row in rows:
        if row["revoke_reason"] == EXPIRED:
            continue
        revoked.append(str(row["id"]))
        owner = (
            f"job:{row['job_id']}" if row["job_id"] else f"thread:{row['thread_id']}"
        )
        await record_lease_event(
            conn,
            event_type="connector_lease_revoked",
            resource_type="connector_lease",
            resource_id=str(row["id"]),
            detail=_detail(
                owner=owner,
                connector=row["connector_id"],
                driver=row["driver"],
                reason=reason,
                token_last_four=row["token_last_four"],
            ),
        )
    return revoked


async def revoke_execution_leases(
    conn: Any,
    *,
    job_id: Any = None,
    thread_id: Any = None,
    reason: RevokeReason,
) -> list[str]:
    """Revoke every live lease of one execution; returns the revoked ids.

    Exactly one of ``job_id`` / ``thread_id``. Runs on the caller's connection
    so the revocation commits with the terminal decision it belongs to.
    """
    if (job_id is None) == (thread_id is None):
        raise ValueError("revoke_execution_leases needs a job_id or a thread_id")
    column, owner = (
        ("job_id", job_id) if job_id is not None else ("thread_id", thread_id)
    )
    return await _revoke(
        conn,
        where=f"lease.{column} = $1::uuid",
        args=(str(owner),),
        reason=reason,
    )


async def revoke_execution_leases_with(
    db: Any,
    *,
    job_id: Any = None,
    thread_id: Any = None,
    reason: RevokeReason,
) -> None:
    """:func:`revoke_execution_leases` on ``db`` after a decision committed
    elsewhere; best effort, since the lease already stopped renewing."""
    try:
        async with db.acquire() as conn:
            await revoke_execution_leases(
                conn, job_id=job_id, thread_id=thread_id, reason=reason
            )
    except Exception:
        logger.warning(
            "Lease revoke failed after a terminal decision (%s)", reason, exc_info=True
        )


async def revoke_connector_leases(
    conn: Any,
    *,
    owner: LeaseOwner,
    connector_ids: Sequence[str],
    reason: RevokeReason = "connector_detached",
) -> list[str]:
    """Revoke the leases of ``owner`` for the given connectors (live detach)."""
    ids = [str(value) for value in connector_ids]
    if not ids:
        return []
    return await _revoke(
        conn,
        where=(
            f"lease.{owner.column} = $1::uuid AND lease.connector_id = ANY($2::uuid[])"
        ),
        args=(owner.id, ids),
        reason=reason,
    )


async def revoke_terminal_execution_leases(
    conn: Any, *, owner: LeaseOwner
) -> list[str]:
    """The idempotent backstop: revoke only if the execution is terminal.

    Called where a workspace is torn down (process zero at pod delete). A
    pod delete is not an execution event: an idle session's workspace may be
    torn down while the session lives, and its lease must survive. So this
    revokes only when durable state already says the execution ended.
    """
    if owner.kind == "job":
        terminal = (
            "EXISTS (SELECT 1 FROM jobs WHERE jobs.id = lease.job_id "
            "AND jobs.status::text IN ('completed', 'failed', 'cancelled'))"
        )
    else:
        terminal = (
            "EXISTS (SELECT 1 FROM threads WHERE threads.id = lease.thread_id "
            "AND threads.status::text = 'ended')"
        )
    return await _revoke(
        conn,
        where=f"lease.{owner.column} = $1::uuid AND {terminal}",
        args=(owner.id,),
        reason="execution_terminal",
    )


async def revoke_terminal_execution_leases_with(db: Any, *, owner: LeaseOwner) -> None:
    """:func:`revoke_terminal_execution_leases` on ``db``; best effort."""
    try:
        async with db.acquire() as conn:
            await revoke_terminal_execution_leases(conn, owner=owner)
    except Exception:
        logger.warning(
            "Lease backstop revoke failed for %s %s",
            owner.kind,
            owner.id,
            exc_info=True,
        )


# =============================================================================
# Renewal
# =============================================================================


async def renew_live_leases(
    conn: Any,
    *,
    ttl_seconds: int | None = None,
    renew_below_seconds: int | None = None,
) -> int:
    """Renew the leases whose execution is live in durable state.

    Only leases in the second half of their window are written (the
    runtime-actor throttle), and never an expired one: expiry is terminal.
    A Job is live while it is ``processing`` or a ``processing`` child runs
    on its workspace; ``paused`` and ``pending_review`` Jobs are not, so
    their leases lapse. A thread is live until it has ``ended`` or begun a
    pinned retirement, idle or not.
    """
    ttl = int(ttl_seconds or LEASE_TTL_SECONDS)
    below = int(renew_below_seconds or (ttl // 2))
    rows = await conn.fetch(
        """
        UPDATE connector_credential_leases AS lease
           SET expires_at = now() + make_interval(secs => $1::int),
               last_renewed_at = now()
         WHERE lease.revoked_at IS NULL
           AND lease.expires_at > now()
           AND lease.expires_at < now() + make_interval(secs => $2::int)
           AND (
                EXISTS (
                    SELECT 1 FROM jobs AS job
                     WHERE job.id = lease.job_id
                       AND (
                            job.status::text = 'processing'
                            OR EXISTS (
                                SELECT 1 FROM jobs AS child
                                 WHERE child.parent_job_id = job.id
                                   AND child.status::text = 'processing'
                                   AND child.context->>'inherits_parent_workspace'
                                       = 'true'
                            )
                       )
                )
                OR EXISTS (
                    SELECT 1 FROM threads AS thread
                     WHERE thread.id = lease.thread_id
                       AND thread.status::text <> 'ended'
                       AND thread.runtime_retirement_token IS NULL
                )
           )
        RETURNING lease.id
        """,
        ttl,
        below,
    )
    return len(rows)


async def retire_expired_leases(conn: Any) -> int:
    """Mark live leases past their expiry as ``expired`` (housekeeping)."""
    rows = await conn.fetch(
        f"""
        UPDATE connector_credential_leases
           SET revoked_at = expires_at, revoke_reason = '{EXPIRED}'
         WHERE revoked_at IS NULL AND expires_at <= now()
        RETURNING id
        """
    )
    return len(rows)


async def connector_lease_sweeper(
    shutdown_event: asyncio.Event,
    *,
    store: Any,
    interval_seconds: float | None = None,
) -> None:
    """Leader-gated loop: renew live leases, retire expired ones.

    Best effort: a failed pass is logged and the next one runs on time. A
    renewal is a pure server-side UPDATE re-derived from durable state, so a
    pass cancelled by a leadership change is simply run again by the next
    leader.
    """
    interval = float(interval_seconds or SWEEP_INTERVAL_SECONDS)
    logger.info(
        "Connector lease sweeper started (ttl=%ds, interval=%.0fs)",
        LEASE_TTL_SECONDS,
        interval,
    )
    while not shutdown_event.is_set():
        try:
            async with store.acquire() as conn:
                renewed = await renew_live_leases(conn)
                expired = await retire_expired_leases(conn)
            if renewed or expired:
                logger.info("connector leases: renewed=%d expired=%d", renewed, expired)
        except Exception as exc:
            logger.warning("connector lease sweep error (non-fatal): %s", exc)
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=interval)
            break
        except asyncio.TimeoutError:
            pass
    logger.info("Connector lease sweeper stopped")


__all__ = [
    "EXPIRED",
    "LEASE_TTL_SECONDS",
    "RENEW_BELOW_SECONDS",
    "SWEEP_INTERVAL_SECONDS",
    "DeliveredLease",
    "LeaseDeliveryError",
    "LeaseOwner",
    "connector_lease_sweeper",
    "deliver_connector_leases",
    "deliver_connector_leases_with",
    "issue_or_redeliver",
    "lease_spec",
    "needs_leases",
    "record_lease_event",
    "renew_live_leases",
    "retire_expired_leases",
    "revoke_connector_leases",
    "revoke_execution_leases",
    "revoke_terminal_execution_leases",
    "revoke_terminal_execution_leases_with",
]
