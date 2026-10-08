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
  and pod recycle receives the same token. A stored copy that no longer
  decrypts to its own digest is retired and replaced. Rotation happens only
  when a lease ends and a new one is issued (resume after a pause or an End).
* **Atomic issue.** Every issue and re-delivery first locks the owner's row
  ``FOR SHARE`` and refuses a terminal or retiring execution. The terminal
  transactions lock the same row ``FOR UPDATE`` before they revoke, so an
  issue either commits before the revoke (and is revoked by it) or sees the
  terminal state.
* **Expiry.** A lease expires ``configure_lease_window``'s TTL after its last
  renewal (``DeploymentSettings``, 15 minutes by default). Only
  :func:`connector_lease_sweeper` renews, and only leases whose execution is
  live in durable state (a processing Job, or one with a processing child on
  its workspace; a thread that has not ended or been authorized to retire,
  idle or not). It writes only leases in the second half of their window, as
  the runtime-actor liveness slide does. A lease never renews itself, paused
  and ``pending_review`` Jobs lapse, and there is no hard maximum. The same
  sweep revokes any live lease whose execution durable state already shows as
  terminal, which bounds a missed terminal write by one sweep.
* **Revocation** is an UPDATE at the execution's terminal transactions (an
  authorized End or suspend, cancel, delete, completion, a live detach, a
  connector delete); pod events never revoke. The revoke functions take the
  caller's connection so they commit with the terminal decision.
* **Audit.** Issue, revoke, an access change, a denied exchange of a known
  identity and the first exchange of a lease go to ``security_events``;
  later exchanges only update counters.

Design: knowledge-base/knowledge/features/connector_drivers.md, "The lease
service".
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from shared.connectors.builtin import driver_spec_for_row, git_swap_entry
from shared.connectors.contract import (
    DriverSpec,
    effective_access,
    managed_mcp_driver,
)
from shared.connectors.leases import (
    LEASE_TOKEN_PREFIX,
    last_four,
    mint_token,
    token_digest,
    token_shape_valid,
)

logger = logging.getLogger(__name__)

#: Decision 9: a lease lives 15 minutes after its last renewal.
DEFAULT_TTL_SECONDS = 900
DEFAULT_SWEEP_SECONDS = 60.0
MIN_TTL_SECONDS = 60
MIN_SWEEP_SECONDS = 5.0
#: The process's lease window, set from ``DeploymentSettings`` when the
#: application is built (the most recently built application's settings
#: answer, as ``set_provisioning_backends`` does).
_window: dict[str, float] = {
    "ttl": float(DEFAULT_TTL_SECONDS),
    "sweep": DEFAULT_SWEEP_SECONDS,
}

#: Why a lease ended; ``expired`` is written for a lapse, never revoked.
RevokeReason = Literal[
    "session_end",
    "session_suspended",
    "job_cancelled",
    "job_deleted",
    "job_completed",
    "job_failed",
    "connector_detached",
    "connector_deleted",
    "execution_ended",
    "execution_terminal",
    "unreadable",
    "served_by_fallback",
]
EXPIRED = "expired"
_OWNER_COLUMNS = {"job": "job_id", "thread": "thread_id"}
_TERMINAL_JOB_STATUSES = ("completed", "failed", "cancelled")
#: The thread statuses whose leases the sweep renews (the live set the
#: thread file and upload routes use); ``suspended`` and ``ended`` lapse.
_LIVE_THREAD_STATUSES = ("created", "active", "idle", "awaiting_user")


def sweep_interval(ttl_seconds: int, sweep_seconds: float) -> float:
    """The sweep interval actually used: at most a quarter of the TTL, so a
    live lease is renewed at least once in the second half of its window."""
    return max(MIN_SWEEP_SECONDS, min(float(sweep_seconds), ttl_seconds / 4))


def configure_lease_window(*, ttl_seconds: int, sweep_seconds: float) -> None:
    """Set the TTL new leases are issued with and the sweep interval."""
    ttl = max(MIN_TTL_SECONDS, int(ttl_seconds))
    _window["ttl"] = float(ttl)
    _window["sweep"] = sweep_interval(ttl, sweep_seconds)


def lease_ttl_seconds() -> int:
    return int(_window["ttl"])


def lease_sweep_seconds() -> float:
    return float(_window["sweep"])


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
    """The driver spec of a payload entry that is delivered by lease (the
    entry's own driver: a token repository bound through the git swap
    driver is the swap's)."""
    spec = driver_spec_for_row(entry)
    return spec if spec is not None and spec.credential_delivery == "lease" else None


def harness_credentials(entry: Mapping[str, Any], spec: DriverSpec) -> dict[str, Any]:
    """What a lease entry still carries for the agent process: the slots its
    driver names in ``harness_credentials`` (each under its own key), never
    anything else of the upstream secret. Empty for every lease driver but
    the git swap, whose forge token the pull-request tools call the forge
    API with."""
    credentials = entry.get("credentials")
    if not isinstance(credentials, Mapping):
        return {}
    return {
        name: credentials[name]
        for name in spec.harness_credentials
        if isinstance(credentials.get(name), str) and credentials[name]
    }


def needs_leases(entries: Sequence[Any] | None) -> bool:
    """Whether any entry of a datasources payload is delivered by lease."""
    return any(isinstance(e, Mapping) and lease_spec(e) for e in entries or ())


def _encrypt(token: str) -> str:
    from orchestrator.security.crypto import encrypt

    return encrypt(token)


def _stored_token(row: Mapping[str, Any]) -> str | None:
    """The token a lease row stores, or ``None`` when its copy is unusable.

    The ciphertext is bound to its row by the row's own digest: a copy that
    does not decrypt, is no lease token, or decrypts to another token (a
    ciphertext moved between rows) is unusable.
    """
    from orchestrator.security.crypto import DecryptionError, decrypt

    try:
        token = decrypt(str(row["token_ciphertext"]))
    except (DecryptionError, RuntimeError, ValueError, TypeError):
        return None
    if not token_shape_valid(token, LEASE_TOKEN_PREFIX):
        return None
    if token_digest(token) != bytes(row["token_hash"]):
        return None
    return token


async def _lock_live_owner(conn: Any, owner: LeaseOwner) -> bool:
    """Lock the owner's row ``FOR SHARE``; whether it may hold a lease.

    The terminal transactions lock the same row ``FOR UPDATE`` before they
    revoke, so this serializes an issue against them. A thread whose
    retirement is only a hidden preflight still accepts leases: the
    authorization edge revokes everything it holds.
    """
    if owner.kind == "job":
        row = await conn.fetchrow(
            "SELECT status::text AS status FROM jobs WHERE id = $1::uuid FOR SHARE",
            UUID(owner.id),
        )
        return row is not None and row["status"] not in _TERMINAL_JOB_STATUSES
    row = await conn.fetchrow(
        "SELECT status::text AS status, runtime_retirement_token, "
        "runtime_retirement_authorized_at FROM threads WHERE id = $1::uuid "
        "FOR SHARE",
        UUID(owner.id),
    )
    return (
        row is not None
        and row["status"] != "ended"
        and not (
            row["runtime_retirement_token"] is not None
            and row["runtime_retirement_authorized_at"] is not None
        )
    )


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

    The owner's row is locked first; a terminal or retiring execution gets
    nothing. An expired lease is retired (``revoked_at`` = its expiry, reason
    ``expired``), and a live one whose stored copy is unusable is revoked
    (``unreadable``), so the one-live-lease index admits a new one. A live
    lease is delivered again with its stored token; its access level follows
    the connector's current one, audited when it changes. A concurrent issuer
    that wins the unique index is read back, never duplicated.
    """
    ttl = int(ttl_seconds or lease_ttl_seconds())
    connector_uuid = UUID(str(connector_id))
    owner_uuid = UUID(owner.id)
    column = owner.column
    async with conn.transaction():
        if not await _lock_live_owner(conn, owner):
            # A terminal decision (End, cancel, completion) revoked the
            # execution's leases; a delivery racing it must not mint one
            # the sweeper would then keep alive.
            raise LeaseDeliveryError("The execution no longer accepts leases")
        # The connector next, before any lease row, in the order the connector
        # delete takes them (its row, then its leases).
        if (
            await conn.fetchval(
                "SELECT 1 FROM datasources WHERE id = $1 FOR KEY SHARE",
                connector_uuid,
            )
            is None
        ):
            raise LeaseDeliveryError("The connector no longer exists")
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
        token = _stored_token(live) if live is not None else None
        if live is not None and token is None:
            await _revoke(
                conn,
                where="lease.id = $1::uuid",
                args=(str(live["id"]),),
                reason="unreadable",
            )
            live = None
        if live is None:
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
            token = _stored_token(live) if live is not None else None
            if live is None or token is None:
                raise LeaseDeliveryError("A concurrent lease vanished during issue")
        if str(live["access"]) != access:
            await conn.execute(
                "UPDATE connector_credential_leases SET access = $2 WHERE id = $1",
                live["id"],
                access,
            )
            await record_lease_event(
                conn,
                event_type="connector_lease_access_changed",
                resource_type="connector_lease",
                resource_id=str(live["id"]),
                detail=_detail(
                    owner=f"{owner.kind}:{owner.id}",
                    connector=connector_id,
                    access_from=live["access"],
                    access_to=access,
                ),
            )
        return DeliveredLease(
            id=str(live["id"]),
            token=token,
            connector_id=str(connector_uuid),
            access=access,
            expires_at=live["expires_at"],
            issued=False,
        )


async def _live_lease(
    conn: Any, column: str, owner_uuid: UUID, connector_uuid: UUID
) -> Any:
    return await conn.fetchrow(
        f"""
        SELECT id, token_hash, token_ciphertext, access, expires_at
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
    token}}`` and nothing else but the driver's ``harness_credentials``
    (the git swap's forge token, for the agent process), so no other
    upstream secret can ride along; an entry SRW cannot lease delivers no
    token. A git swap candidate is decided here, per entry
    (:func:`connector_git_swap_delivery.git_swap_problem`, then its image):
    served, it gets the driver's URL and SRW's certificate authority in its
    ``git_swap`` block; not, it takes the installation's fallback and says
    why, and never fails the delivery. The access level is the one the
    agent binds the entry at (``effective_access``). Returns how many
    entries carry a lease. The caller holds the connection (and the
    transaction the delivery belongs to).

    Entries are issued in connector-id order (the payload keeps its own
    order), so every transaction that takes several connectors' rows takes
    them in one order.
    """
    delivered = 0
    lease_entries = [
        entry
        for entry in entries or ()
        if isinstance(entry, dict) and lease_spec(entry) is not None
    ]
    # Lowercase hex sorts as PostgreSQL sorts the uuid.
    lease_entries.sort(key=lambda entry: str(entry.get("datasource_id") or "").lower())
    for entry in lease_entries:
        spec = lease_spec(entry)
        if spec is None:
            continue
        connector_id = str(entry.get("datasource_id") or "")
        swap = git_swap_entry(entry)
        if swap:
            # Decided per entry, before any credential is touched: a
            # candidate the driver cannot serve keeps its pre-C3 entry.
            from orchestrator.services import connector_git_swap_delivery as swaps

            try:
                UUID(connector_id)
            except ValueError:
                swaps.apply_fallback(entry, swaps.Problem("no_connector_id"))
                continue
            problem = await swaps.git_swap_problem(
                conn, entry, connector_id=connector_id, owner=owner
            )
            if problem is not None:
                await _fall_back(conn, entry, problem, owner=owner)
                continue
        kept = harness_credentials(entry, spec)
        access = effective_access(entry, spec)
        try:
            UUID(connector_id)
        except ValueError:
            entry["credentials"] = dict(kept)
            logger.warning(
                "A %s connector entry names no connector id; no lease delivered",
                spec.name,
            )
            continue
        if access is None:
            entry["credentials"] = dict(kept)
            logger.warning(
                "Driver %s has no access levels; no lease delivered", spec.name
            )
            continue
        image_digest = None
        if spec.plane == "service":
            # A service driver's binding runs on one image digest, resolved
            # (and checked when it moved) at bind; it keys the shared pod.
            from orchestrator.services.connector_service_images import (
                bind_service_image,
            )

            try:
                image_digest = await bind_service_image(
                    conn, spec=spec, connector_id=connector_id, owner=owner
                )
            except LeaseDeliveryError as exc:
                if not swap:
                    entry["credentials"] = dict(kept)
                    raise
                from orchestrator.services import connector_git_swap_delivery as swaps

                await _fall_back(
                    conn,
                    entry,
                    swaps.Problem("image_unavailable", str(exc)),
                    owner=owner,
                )
                continue
        if swap:
            from orchestrator.services import connector_git_swap_delivery as swaps

            swaps.serve_through_swap(entry)
        entry["credentials"] = dict(kept)
        lease = await issue_or_redeliver(
            conn,
            owner=owner,
            connector_id=connector_id,
            driver=spec.name,
            access=access,
            image_digest=image_digest,
            ttl_seconds=ttl_seconds,
        )
        entry["credentials"] = {
            **kept,
            "lease": {
                "id": lease.id,
                "connector_id": lease.connector_id,
                "token": lease.token,
            },
        }
        if managed_mcp_driver(spec):
            # The agent process connects to the connector's endpoint at this
            # digest with the lease token as its bearer (D5a).
            entry["connection_url"] = _managed_mcp_url(spec, connector_id, image_digest)
        elif git_swap_entry(entry):
            # The workspace's git reaches the driver at this digest (C3).
            try:
                entry["git_swap"] = _git_swap_block(
                    spec, connector_id, image_digest, entry.get("connection_url")
                )
            except LeaseDeliveryError as exc:
                # Checked before the lease; a late failure still falls back
                # (and revokes the lease just delivered).
                from orchestrator.services import connector_git_swap_delivery as swaps

                entry["credentials"] = dict(kept)
                await _fall_back(
                    conn,
                    entry,
                    swaps.Problem("endpoint_unavailable", str(exc)),
                    owner=owner,
                )
                continue
        if getattr(lease, "issued", False) and spec.plane == "service":
            # A new binding: its pod starts on the reconciler's next pass;
            # ask for that pass when this transaction commits (S1).
            await _ask_for_reconcile(conn)
        delivered += 1
    return delivered


async def _fall_back(
    conn: Any, entry: dict[str, Any], problem: Any, *, owner: LeaseOwner
) -> None:
    """A git swap candidate on the installation's fallback: its entry says
    why, and the lease its owner held for the connector (an earlier attach
    served it through the driver) is revoked, so a workspace wired then
    stops reaching the driver with it."""
    from orchestrator.services import connector_git_swap_delivery as swaps

    swaps.apply_fallback(entry, problem)
    await revoke_connector_leases(
        conn,
        owner=owner,
        connector_ids=[str(entry.get("datasource_id"))],
        reason="served_by_fallback",
    )


async def _ask_for_reconcile(conn: Any) -> None:
    """Wake the service reconciler once the delivery commits: a NOTIFY in a
    transaction is sent at its commit and never on a rollback, to whichever
    replica leads (``connector_service_hosting.RECONCILE_CHANNEL``)."""
    from orchestrator.services.connector_service_hosting import RECONCILE_CHANNEL

    execute = getattr(conn, "execute", None)
    if not callable(execute):
        return
    try:
        await execute("SELECT pg_notify($1, '')", RECONCILE_CHANNEL)
    except Exception:
        logger.debug("Asking the reconciler for a pass failed", exc_info=True)


def _git_swap_block(
    spec: DriverSpec, connector_id: str, digest: str | None, upstream_url: Any
) -> dict[str, Any]:
    """What a git swap binding's workspace needs: the driver's URL for the
    connector's repository (what ``insteadOf`` rewrites the clean upstream
    URL to), the certificate authority it trusts for that URL only, and how
    long a first clone waits for the connector's pod."""
    from orchestrator.services.connector_driver_ca import driver_ca
    from orchestrator.services.connector_service_images import (
        service_image_settings,
    )
    from orchestrator.services.connector_service_launch import endpoint_url
    from shared.connectors.git_swap import (
        UnservedUpstream,
        driver_repository_url,
        swap_upstream,
    )

    settings = service_image_settings()
    ca = driver_ca()
    if not settings.service_namespace or not digest or spec.service is None:
        raise LeaseDeliveryError(
            f"Service-pod hosting is off; {spec.name} cannot be served"
        )
    if ca is None:
        raise LeaseDeliveryError(
            f"No connector driver certificate authority; {spec.name} cannot be served"
        )
    try:
        upstream = swap_upstream(upstream_url)
    except UnservedUpstream as exc:
        raise LeaseDeliveryError(f"{spec.name} cannot serve this repository: {exc}")
    endpoint = endpoint_url(
        namespace=settings.service_namespace,
        connector_id=connector_id,
        digest=digest,
        port=spec.service.port,
        scheme="https",
    )
    return {
        "url": driver_repository_url(endpoint, connector_id, upstream),
        "ca": ca.certificate_pem,
        "wait_seconds": int(settings.service_start_seconds),
    }


def _managed_mcp_url(spec: DriverSpec, connector_id: str, digest: str | None) -> str:
    """Where a managed MCP binding's client reaches the server's front."""
    from orchestrator.services.connector_service_images import (
        service_image_settings,
    )
    from orchestrator.services.connector_service_launch import endpoint_url
    from shared.connectors.mcp import FRONT_PATH

    namespace = service_image_settings().service_namespace
    if not namespace or not digest or spec.service is None:
        raise LeaseDeliveryError(
            f"Service-pod hosting is off; {spec.name} cannot be served"
        )
    return endpoint_url(
        namespace=namespace,
        connector_id=connector_id,
        digest=digest,
        port=spec.service.port,
        path=FRONT_PATH,
    )


async def prepare_lease_delivery(
    db: Any, entries: Sequence[Any] | None, *, owner: LeaseOwner
) -> None:
    """What a delivery needs done before its caller opens a transaction.

    A new binding of a service-plane driver runs on an image digest the
    registry answers (D5): the lookup, the image row and a refusal audit
    happen here, on ``db``'s own connections, so the delivery inside the
    caller's transaction does no network and no write of its own. A git
    swap candidate's upstream is checked here too (its egress and its TLS,
    C3). No-op without such an entry; never raises (the delivery applies
    the outcome).
    """
    if not any(
        isinstance(entry, Mapping)
        and (spec := lease_spec(entry)) is not None
        and spec.plane == "service"
        for entry in entries or ()
    ):
        return
    from orchestrator.services.connector_git_swap_delivery import (
        prepare_git_swap_delivery,
    )
    from orchestrator.services.connector_service_images import (
        prepare_service_images,
    )

    await prepare_service_images(entries, owner=owner, store=db)
    await prepare_git_swap_delivery(db, entries)


async def prepare_thread_lease_delivery(db: Any, thread_id: str) -> None:
    """:func:`prepare_lease_delivery` for a session's stored selection, for
    the delivery paths that build their payload under the thread's
    datasource lock (the pinned attach and the pinned workspace poll): the
    network part (a driver image, a git swap upstream's DNS and TLS) runs
    here, before that lock, a pool connection and an attach reservation are
    taken; the delivery under them then finds the answers remembered. Reads
    only the selected repository rows' URL and config (no credential).
    Never raises."""
    try:
        from orchestrator.services.connector_git_swap_delivery import (
            candidate_entry,
            git_swap_delivery_settings,
        )

        if not git_swap_delivery_settings().installed:
            return
        async with db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT d.id, d.type, d.connection_url, d.config
                  FROM threads AS t
                  JOIN datasources AS d
                    ON d.id::text IN (
                        SELECT jsonb_array_elements_text(
                            CASE WHEN jsonb_typeof(t.metadata->'datasource_ids')
                                      = 'array'
                                 THEN t.metadata->'datasource_ids'
                                 ELSE '[]'::jsonb END)
                    )
                 WHERE t.id = $1::uuid
                """,
                UUID(str(thread_id)),
            )
        entries = [
            entry for row in rows if (entry := candidate_entry(dict(row))) is not None
        ]
        if entries:
            await prepare_lease_delivery(
                db, entries, owner=LeaseOwner.thread(str(thread_id))
            )
    except Exception:
        logger.warning(
            "Preparing the lease delivery of thread %s failed", thread_id, exc_info=True
        )


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
    skip_locked: bool = False,
) -> list[str]:
    """Revoke the live leases matching ``where``; audit the revoked ones.

    A lease already past its expiry is retired as ``expired`` instead: it was
    unusable before this decision, so it is not counted as revoked.

    ``skip_locked`` is for the sweeper, which spans many executions: it
    takes only the lease rows no transaction holds (an issue holding one
    lease row and waiting for another would otherwise deadlock with a
    single UPDATE locking them in scan order). What it skips, the next pass
    revokes.
    """
    reason_arg = f"${len(args) + 1}::text"
    if skip_locked:
        target = f"""
        WITH picked AS (
            SELECT lease.id FROM connector_credential_leases AS lease
             WHERE lease.revoked_at IS NULL AND ({where})
             ORDER BY lease.id
             FOR UPDATE OF lease SKIP LOCKED
        )
        UPDATE connector_credential_leases AS lease
           SET revoked_at = LEAST(now(), lease.expires_at),
               revoke_reason = CASE WHEN lease.expires_at <= now()
                                    THEN '{EXPIRED}'
                                    ELSE {reason_arg} END
          FROM picked
         WHERE lease.id = picked.id
        """
    else:
        target = f"""
        UPDATE connector_credential_leases AS lease
           SET revoked_at = LEAST(now(), lease.expires_at),
               revoke_reason = CASE WHEN lease.expires_at <= now()
                                    THEN '{EXPIRED}'
                                    ELSE {reason_arg} END
         WHERE lease.revoked_at IS NULL AND ({where})
        """
    rows = await conn.fetch(
        f"""{target}
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


async def revoke_all_connector_leases(
    conn: Any, *, connector_id: Any, reason: RevokeReason = "connector_deleted"
) -> list[str]:
    """Revoke every live lease of one connector, for every execution.

    The connector delete transaction calls this (with the connector's driver
    identities) before the row goes: the foreign keys cascade, so revocation
    is written first, as ``delete_job`` does.
    """
    return await _revoke(
        conn,
        where="lease.connector_id = $1::uuid",
        args=(str(connector_id),),
        reason=reason,
    )


_TERMINAL_OWNER = f"""(
    EXISTS (
        SELECT 1 FROM jobs WHERE jobs.id = lease.job_id
           AND jobs.status::text IN {_TERMINAL_JOB_STATUSES!r}
    )
    OR EXISTS (
        SELECT 1 FROM threads WHERE threads.id = lease.thread_id
           AND (threads.status::text = 'ended'
                OR (threads.runtime_retirement_token IS NOT NULL
                    AND threads.runtime_retirement_authorized_at IS NOT NULL))
    )
)"""


async def revoke_terminal_execution_leases(
    conn: Any, *, owner: LeaseOwner
) -> list[str]:
    """The idempotent backstop: revoke only if the execution is terminal.

    Called where a workspace is torn down (process zero at pod delete). A
    pod delete is not an execution event: an idle session's workspace may be
    torn down while the session lives, and its lease must survive. So this
    revokes only when durable state already says the execution ended.
    """
    return await _revoke(
        conn,
        where=f"lease.{owner.column} = $1::uuid AND {_TERMINAL_OWNER}",
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


async def revoke_leases_of_terminal_executions(conn: Any) -> list[str]:
    """The sweeper's backstop: revoke every live lease whose execution durable
    state already shows as terminal (a terminal write no revoke point saw).
    Rows another transaction holds are skipped until the next pass."""
    return await _revoke(
        conn,
        where=_TERMINAL_OWNER,
        args=(),
        reason="execution_terminal",
        skip_locked=True,
    )


# =============================================================================
# Renewal
# =============================================================================


async def renew_live_leases(conn: Any, *, ttl_seconds: int | None = None) -> int:
    """Renew the leases whose execution is live in durable state.

    Only leases in the second half of their window are written (the
    runtime-actor throttle), and never an expired one: expiry is terminal.
    A Job is live while it is ``processing``, or while a ``processing``
    child runs on its workspace and the Job itself is not terminal;
    ``paused`` and ``pending_review`` Jobs are not, so their leases lapse. A
    thread is live in a live status (created, active, idle, awaiting_user),
    idle or not, unless its retirement is authorized; a ``suspended``
    thread's lease lapses (every resume leaves ``suspended`` before its
    runtime attaches, and that attach issues anew).

    The due rows are taken ``FOR UPDATE SKIP LOCKED``: a transaction issuing
    several leases holds some of them while it waits for the next, and one
    UPDATE locking rows in scan order would deadlock with it. A skipped row
    is still in the second half of its window, so the next pass renews it.
    """
    ttl = int(ttl_seconds or lease_ttl_seconds())
    rows = await conn.fetch(
        f"""
        WITH due AS (
        SELECT lease.id FROM connector_credential_leases AS lease
         WHERE lease.revoked_at IS NULL
           AND lease.expires_at > now()
           AND lease.expires_at < now() + make_interval(secs => $2::int)
           AND (
                EXISTS (
                    SELECT 1 FROM jobs AS job
                     WHERE job.id = lease.job_id
                       AND (
                            job.status::text = 'processing'
                            OR (
                                job.status::text NOT IN {_TERMINAL_JOB_STATUSES!r}
                                AND EXISTS (
                                    SELECT 1 FROM jobs AS child
                                     WHERE child.parent_job_id = job.id
                                       AND child.status::text = 'processing'
                                       AND child.context->>'inherits_parent_workspace'
                                           = 'true'
                                )
                            )
                       )
                )
                OR EXISTS (
                    SELECT 1 FROM threads AS thread
                     WHERE thread.id = lease.thread_id
                       AND thread.status::text IN {_LIVE_THREAD_STATUSES!r}
                       AND NOT (
                            thread.runtime_retirement_token IS NOT NULL
                            AND thread.runtime_retirement_authorized_at IS NOT NULL
                       )
                )
           )
         ORDER BY lease.id
         FOR UPDATE OF lease SKIP LOCKED
        )
        UPDATE connector_credential_leases AS lease
           SET expires_at = now() + make_interval(secs => $1::int),
               last_renewed_at = now()
          FROM due
         WHERE lease.id = due.id
        RETURNING lease.id
        """,
        ttl,
        ttl // 2,
    )
    return len(rows)


async def retire_expired_leases(conn: Any) -> int:
    """Mark live leases past their expiry as ``expired`` (housekeeping).

    Like the renewal, it skips rows another transaction holds (an issue
    retires its own expired copy): the next pass takes them.
    """
    rows = await conn.fetch(
        f"""
        WITH lapsed AS (
            SELECT id FROM connector_credential_leases
             WHERE revoked_at IS NULL AND expires_at <= now()
             ORDER BY id
             FOR UPDATE SKIP LOCKED
        )
        UPDATE connector_credential_leases AS lease
           SET revoked_at = lease.expires_at, revoke_reason = '{EXPIRED}'
          FROM lapsed
         WHERE lease.id = lapsed.id
        RETURNING lease.id
        """
    )
    return len(rows)


async def connector_lease_sweeper(
    shutdown_event: asyncio.Event,
    *,
    store: Any,
    ttl_seconds: int | None = None,
    interval_seconds: float | None = None,
) -> None:
    """Leader-gated loop: revoke leases of terminal executions, renew live
    ones, retire expired ones.

    Best effort: a failed pass is logged and the next one runs on time. Every
    step is a server-side UPDATE re-derived from durable state, so a pass
    cancelled by a leadership change is simply run again by the next leader.
    """
    ttl = int(ttl_seconds or lease_ttl_seconds())
    # Settings already floor the interval; only the TTL cap applies here.
    interval = min(float(interval_seconds or lease_sweep_seconds()), ttl / 4)
    logger.info(
        "Connector lease sweeper started (ttl=%ds, interval=%.0fs)", ttl, interval
    )
    while not shutdown_event.is_set():
        try:
            async with store.acquire() as conn:
                terminal = await revoke_leases_of_terminal_executions(conn)
                renewed = await renew_live_leases(conn, ttl_seconds=ttl)
                expired = await retire_expired_leases(conn)
            if terminal or renewed or expired:
                logger.info(
                    "connector leases: terminal=%d renewed=%d expired=%d",
                    len(terminal),
                    renewed,
                    expired,
                )
        except Exception as exc:
            logger.warning("connector lease sweep error (non-fatal): %s", exc)
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=interval)
            break
        except asyncio.TimeoutError:
            pass
    logger.info("Connector lease sweeper stopped")


__all__ = [
    "DEFAULT_SWEEP_SECONDS",
    "DEFAULT_TTL_SECONDS",
    "EXPIRED",
    "DeliveredLease",
    "LeaseDeliveryError",
    "LeaseOwner",
    "configure_lease_window",
    "connector_lease_sweeper",
    "deliver_connector_leases",
    "deliver_connector_leases_with",
    "harness_credentials",
    "issue_or_redeliver",
    "lease_spec",
    "lease_sweep_seconds",
    "lease_ttl_seconds",
    "needs_leases",
    "prepare_lease_delivery",
    "record_lease_event",
    "renew_live_leases",
    "retire_expired_leases",
    "revoke_all_connector_leases",
    "revoke_connector_leases",
    "revoke_execution_leases",
    "revoke_leases_of_terminal_executions",
    "revoke_terminal_execution_leases",
    "revoke_terminal_execution_leases_with",
    "sweep_interval",
]
