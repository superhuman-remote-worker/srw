"""Connection-owned VM access leases on the existing idle lifecycle ledger.

The caller authenticates the user before calling ``acquire``.  This store
serializes a fresh Ready admission with idle nomination and owner retirement;
status reads cannot create or extend authority.  A live channel must renew its
own lease ID, while a disconnected channel closes only that ID.
"""

from __future__ import annotations

import json
import asyncio
from collections.abc import Mapping
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID, uuid4


def _uuid(value: Any) -> UUID | None:
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _object(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return {}
    return value if isinstance(value, dict) else {}


def _identity(row: Mapping[str, Any]) -> tuple[UUID, UUID] | None:
    context = _object(row["context"] if row["owner_kind"] == "job" else row["metadata"])
    vm = _object(context.get("vm"))
    generation = _uuid(vm.get("provision_generation"))
    vm_uid = _uuid(vm.get("vm_uid"))
    if (
        vm.get("status") != "ready"
        or vm.get("identity_authenticated") is not True
        or _uuid(vm.get("identity_provision_generation")) != generation
        or generation is None
        or vm_uid is None
        or _uuid(vm.get("vmi_uid")) is None
        or _uuid(vm.get("active_pod_uid")) is None
        or _uuid(vm.get("rootdisk_pvc_uid")) is None
        or vm.get("_suspend_remote_io_closed") is not None
        or vm.get("retirement_cleanup_pending") is True
        or (
            row["owner_kind"] == "job"
            and any(
                key in context
                for key in (
                    "_completion_control_claim",
                    "_stateless_delete_pending",
                    "_stateless_cancel_cleanup_pending",
                )
            )
        )
    ):
        return None
    return generation, vm_uid


class VMIdleAccessLost(RuntimeError):
    """The exact in-flight IDE writer lost its bounded operation claim."""


class VMIdleAccessStore:
    """A per-connection lease, never a user-wide or presence lease."""

    def __init__(self, db: Any) -> None:
        self.db = db

    @asynccontextmanager
    async def ide_operation(
        self,
        tab_lease_id: str,
        *,
        owner_kind: str,
        owner_id: str,
        user_id: str,
        heartbeat_seconds: float = 30,
    ):
        """Hold an independent durable writer/WS row until transport drains."""
        lease = await self.begin_ide_operation(
            tab_lease_id,
            owner_kind=owner_kind,
            owner_id=owner_id,
            user_id=user_id,
        )
        if lease is None:
            raise VMIdleAccessLost("ide_operation_unavailable")
        active = asyncio.current_task()
        lost = asyncio.Event()

        async def heartbeat() -> None:
            while True:
                await asyncio.sleep(heartbeat_seconds)
                try:
                    valid = await self.renew(
                        str(lease["id"]),
                        owner_kind=owner_kind,
                        owner_id=owner_id,
                        kind="ide",
                        claimant=lease["claimed_by"],
                    )
                except Exception:
                    valid = False
                if not valid:
                    lost.set()
                    if active is not None:
                        active.cancel()
                    return

        task = asyncio.create_task(heartbeat())
        try:
            yield lease
        except asyncio.CancelledError:
            if lost.is_set():
                raise VMIdleAccessLost("ide_operation_expired") from None
            raise
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await self.close(
                str(lease["id"]),
                owner_kind=owner_kind,
                owner_id=owner_id,
                kind="ide",
                claimant=lease["claimed_by"],
            )

    async def request(
        self,
        *,
        owner_kind: str,
        owner_id: str,
        kind: str,
        user_id: str,
        connection_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Admit a fresh authenticated connection, joining an open wake.

        Every call gets a new claimant.  In particular, an old expired tab
        cannot revive itself by repeating an access POST.
        """
        if _uuid(user_id) is None:
            return None
        claimant = f"{user_id}:{connection_id or uuid4()}"
        if connection_id is not None:
            # A signed gateway request is repeatable for one connection, but
            # replay must neither mint a second lease nor renew an old one.
            owner = self._arguments(owner_kind, owner_id, kind, claimant)
            if owner is None:
                return None
            async with self.db.acquire() as conn, conn.transaction():
                if await self._locked_owner(conn, owner_kind, owner) is None:
                    return None
                existing = await conn.fetchrow(
                    "SELECT * FROM vm_idle_access_leases WHERE owner_kind=$1 "
                    "AND owner_id=$2 AND kind=$3 AND claimed_by=$4 "
                    "ORDER BY acquired_at DESC,id DESC LIMIT 1 FOR UPDATE",
                    owner_kind,
                    owner,
                    kind,
                    claimant,
                )
                if existing is not None:
                    return (
                        dict(existing)
                        if (
                            existing["closed_at"] is None
                            and existing["expires_at"]
                            > await conn.fetchval("SELECT clock_timestamp()")
                        )
                        else None
                    )
        ready = await self.acquire(
            owner_kind=owner_kind, owner_id=owner_id, kind=kind, claimant=claimant
        )
        if ready is not None:
            return ready
        from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

        idle = VMIdleLifecycleStore(self.db)
        if owner_kind == "job":
            wake = await idle.request_wake(
                owner_id,
                execution_requested=False,
                access_kind=kind,
                access_claimant=claimant,
            )
        elif owner_kind == "thread":
            wake = await idle.request_thread_wake(
                owner_id,
                execution_requested=False,
                access_kind=kind,
                access_claimant=claimant,
            )
        else:
            return None
        if wake is None:
            return None
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM vm_idle_access_leases WHERE owner_kind=$1 "
                "AND owner_id=$2 AND kind=$3 AND claimed_by=$4 AND wake_id=$5 "
                "AND closed_at IS NULL AND expires_at>clock_timestamp() "
                "ORDER BY acquired_at DESC,id DESC LIMIT 1",
                owner_kind,
                _uuid(owner_id),
                kind,
                claimant,
                wake["wake_id"],
            )
            return dict(row) if row else None

    @staticmethod
    def _arguments(
        owner_kind: str, owner_id: str, kind: str, claimant: str
    ) -> UUID | None:
        if (
            owner_kind not in {"job", "thread"}
            or kind not in {"ssh", "sftp", "ide"}
            or not isinstance(claimant, str)
            or not 1 <= len(claimant) <= 256
        ):
            return None
        return _uuid(owner_id)

    @staticmethod
    async def _locked_owner(
        conn: Any, owner_kind: str, owner_id: UUID
    ) -> dict[str, Any] | None:
        if owner_kind == "job":
            # The idle lifecycle uses queue -> Job -> operation everywhere.
            await conn.fetchrow(
                "SELECT unit_id FROM run_queue WHERE unit_id=$1 FOR UPDATE",
                owner_id,
            )
            row = await conn.fetchrow(
                "SELECT status,execution_lane,context FROM jobs WHERE id=$1 FOR UPDATE",
                owner_id,
            )
            if (
                row is None
                or row["execution_lane"] not in {"stateless", "pinned"}
                or row["status"]
                in {
                    "completed",
                    "failed",
                    "cancelled",
                }
            ):
                return None
        else:
            row = await conn.fetchrow(
                "SELECT status,execution_lane,metadata,runtime_retirement_token "
                "FROM threads WHERE id=$1 FOR UPDATE",
                owner_id,
            )
            if (
                row is None
                or row["execution_lane"] != "pinned"
                or row["status"]
                not in {
                    "active",
                    "awaiting_user",
                    "suspended",
                    "created",
                }
                or row["runtime_retirement_token"] is not None
            ):
                return None
        return {**dict(row), "owner_kind": owner_kind}

    @staticmethod
    async def _ready_identity(
        conn: Any, owner_kind: str, owner_id: UUID, row: Mapping[str, Any]
    ) -> tuple[UUID, UUID, UUID | None] | None:
        identity = _identity(row)
        if identity is None:
            return None
        # An open release/wake owns this runtime.  request_wake inserts its
        # pending hold in that same transaction; never make an unrelated Ready
        # lease from a stale context while the operation is open.
        operation = await conn.fetchrow(
            "SELECT id FROM vm_idle_operations WHERE owner_kind=$1 AND owner_id=$2 "
            "AND closed_at IS NULL FOR UPDATE",
            owner_kind,
            owner_id,
        )
        if operation is not None:
            return None
        if await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM vm_workspace_cleanup_admissions "
            "WHERE owner_kind=$1 AND owner_id=$2 AND completed_at IS NULL)",
            owner_kind,
            owner_id,
        ):
            return None
        prior = await conn.fetchrow(
            "SELECT id,phase,wake_id,wake_generation,wake_ready_at,pvc_uid "
            "FROM vm_idle_operations WHERE owner_kind=$1 AND owner_id=$2 "
            "ORDER BY admitted_at DESC,id DESC LIMIT 1 FOR UPDATE",
            owner_kind,
            owner_id,
        )
        if prior is None:
            return identity[0], identity[1], None
        context = _object(row["context"] if owner_kind == "job" else row["metadata"])
        vm = _object(context.get("vm"))
        if (
            prior["phase"] != "ready"
            or prior["wake_ready_at"] is None
            or prior["wake_generation"] != identity[0]
            or vm.get("idle_wake_operation_id") != str(prior["id"])
            or _uuid(vm.get("rootdisk_pvc_uid")) != prior["pvc_uid"]
        ):
            return None
        return identity[0], identity[1], prior["wake_id"]

    async def acquire(
        self, *, owner_kind: str, owner_id: str, kind: str, claimant: str
    ) -> dict[str, Any] | None:
        owner = self._arguments(owner_kind, owner_id, kind, claimant)
        if owner is None:
            return None
        async with self.db.acquire() as conn, conn.transaction():
            row = await self._locked_owner(conn, owner_kind, owner)
            if row is None:
                return None
            existing = await conn.fetchrow(
                "SELECT * FROM vm_idle_access_leases WHERE owner_kind=$1 "
                "AND owner_id=$2 AND kind=$3 AND claimed_by=$4 "
                "ORDER BY acquired_at DESC,id DESC LIMIT 1 FOR UPDATE",
                owner_kind,
                owner,
                kind,
                claimant,
            )
            if existing is not None:
                return (
                    dict(existing)
                    if (
                        existing["closed_at"] is None
                        and existing["expires_at"]
                        > await conn.fetchval("SELECT clock_timestamp()")
                    )
                    else None
                )
            identity = await self._ready_identity(conn, owner_kind, owner, row)
            if identity is None:
                return None
            lease = await conn.fetchrow(
                "INSERT INTO vm_idle_access_leases "
                "(owner_kind,owner_id,provision_generation,vm_uid,wake_id,kind,claimed_by,"
                "expires_at,max_expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,"
                "clock_timestamp()+interval '2 minutes',"
                "clock_timestamp()+interval '1 hour') RETURNING *",
                owner_kind,
                owner,
                *identity,
                kind,
                claimant,
            )
            return dict(lease)

    async def begin_ide_operation(
        self,
        tab_lease_id: str,
        *,
        owner_kind: str,
        owner_id: str,
        user_id: str,
    ) -> dict[str, Any] | None:
        """Reserve a distinct writer/WS lease before any upstream I/O.

        Closing a tab cannot close this row.  The exact owner row lock orders
        it with End/recycle and idle release; a foreground heartbeat keeps it
        alive for the duration of the request, up to the database one-hour cap.
        """
        owner = self._arguments(owner_kind, owner_id, "ide", f"{user_id}:x")
        tab_id = _uuid(tab_lease_id)
        if owner is None or tab_id is None or _uuid(user_id) is None:
            return None
        async with self.db.acquire() as conn, conn.transaction():
            row = await self._locked_owner(conn, owner_kind, owner)
            if row is None:
                return None
            identity = await self._ready_identity(conn, owner_kind, owner, row)
            if identity is None:
                return None
            tab = await conn.fetchrow(
                "SELECT claimed_by FROM vm_idle_access_leases WHERE id=$1 "
                "AND owner_kind=$2 AND owner_id=$3 AND kind='ide' "
                "AND claimed_by LIKE $4 AND closed_at IS NULL "
                "AND provision_generation=$5 AND vm_uid=$6 "
                "AND wake_id IS NOT DISTINCT FROM $7 "
                "AND expires_at>clock_timestamp() AND max_expires_at>clock_timestamp() "
                "FOR UPDATE",
                tab_id,
                owner_kind,
                owner,
                f"{user_id}:%",
                *identity,
            )
            if tab is None or ":operation:" in tab["claimed_by"]:
                return None
            claimant = f"{user_id}:operation:{uuid4()}"
            lease = await conn.fetchrow(
                "INSERT INTO vm_idle_access_leases "
                "(owner_kind,owner_id,provision_generation,vm_uid,wake_id,kind,claimed_by,"
                "expires_at,max_expires_at) VALUES($1,$2,$3,$4,$5,'ide',$6,"
                "clock_timestamp()+interval '2 minutes',"
                "clock_timestamp()+interval '1 hour') RETURNING *",
                owner_kind,
                owner,
                *identity,
                claimant,
            )
            return dict(lease)

    async def inspect(
        self, lease_id: str, *, owner_kind: str, owner_id: str, kind: str, claimant: str
    ) -> dict[str, Any] | None:
        owner = self._arguments(owner_kind, owner_id, kind, claimant)
        lease_uuid = _uuid(lease_id)
        if owner is None or lease_uuid is None:
            return None
        async with self.db.acquire() as conn, conn.transaction():
            row = await self._locked_owner(conn, owner_kind, owner)
            if row is None:
                return None
            identity = await self._ready_identity(conn, owner_kind, owner, row)
            if identity is None:
                return None
            lease = await conn.fetchrow(
                "SELECT * FROM vm_idle_access_leases WHERE id=$1 AND owner_kind=$2 "
                "AND owner_id=$3 AND kind=$4 AND claimed_by=$5 AND closed_at IS NULL "
                "AND provision_generation=$6 AND vm_uid=$7 "
                "AND wake_id IS NOT DISTINCT FROM $8 "
                "AND expires_at>clock_timestamp() AND max_expires_at>clock_timestamp()",
                lease_uuid,
                owner_kind,
                owner,
                kind,
                claimant,
                *identity,
            )
            return dict(lease) if lease else None

    async def inspect_for_user(
        self, lease_id: str, *, owner_kind: str, owner_id: str, kind: str, user_id: str
    ) -> dict[str, Any] | None:
        """Read one current caller's lease; never extend it on a poll."""
        owner = self._arguments(owner_kind, owner_id, kind, f"{user_id}:x")
        lease_uuid = _uuid(lease_id)
        if owner is None or lease_uuid is None or _uuid(user_id) is None:
            return None
        async with self.db.acquire() as conn:
            claimant = await conn.fetchval(
                "SELECT claimed_by FROM vm_idle_access_leases WHERE id=$1 "
                "AND owner_kind=$2 AND owner_id=$3 AND kind=$4",
                lease_uuid,
                owner_kind,
                owner,
                kind,
            )
        if not isinstance(claimant, str) or not claimant.startswith(f"{user_id}:"):
            return None
        return await self.inspect(
            lease_id,
            owner_kind=owner_kind,
            owner_id=owner_id,
            kind=kind,
            claimant=claimant,
        )

    async def close_for_user(
        self, lease_id: str, *, owner_kind: str, owner_id: str, kind: str, user_id: str
    ) -> bool:
        owner = self._arguments(owner_kind, owner_id, kind, f"{user_id}:x")
        lease_uuid = _uuid(lease_id)
        if owner is None or lease_uuid is None or _uuid(user_id) is None:
            return False
        async with self.db.acquire() as conn:
            claimant = await conn.fetchval(
                "SELECT claimed_by FROM vm_idle_access_leases WHERE id=$1 "
                "AND owner_kind=$2 AND owner_id=$3 AND kind=$4",
                lease_uuid,
                owner_kind,
                owner,
                kind,
            )
        if (
            not isinstance(claimant, str)
            or not claimant.startswith(f"{user_id}:")
            or ":operation:" in claimant
        ):
            return False
        return await self.close(
            lease_id,
            owner_kind=owner_kind,
            owner_id=owner_id,
            kind=kind,
            claimant=claimant,
        )

    async def renew(
        self, lease_id: str, *, owner_kind: str, owner_id: str, kind: str, claimant: str
    ) -> bool:
        owner = self._arguments(owner_kind, owner_id, kind, claimant)
        lease_uuid = _uuid(lease_id)
        if owner is None or lease_uuid is None:
            return False
        async with self.db.acquire() as conn, conn.transaction():
            row = await self._locked_owner(conn, owner_kind, owner)
            if row is None:
                return False
            identity = await self._ready_identity(conn, owner_kind, owner, row)
            if identity is None:
                return False
            lease = await conn.fetchrow(
                "UPDATE vm_idle_access_leases SET "
                "expires_at=LEAST(max_expires_at,clock_timestamp()+interval '2 minutes') "
                "WHERE id=$1 AND owner_kind=$2 AND owner_id=$3 AND kind=$4 "
                "AND claimed_by=$5 AND provision_generation=$6 AND vm_uid=$7 "
                "AND wake_id IS NOT DISTINCT FROM $8 AND closed_at IS NULL "
                "AND expires_at>clock_timestamp() AND max_expires_at>clock_timestamp() "
                "RETURNING id",
                lease_uuid,
                owner_kind,
                owner,
                kind,
                claimant,
                *identity,
            )
            return lease is not None

    async def close(
        self, lease_id: str, *, owner_kind: str, owner_id: str, kind: str, claimant: str
    ) -> bool:
        owner = self._arguments(owner_kind, owner_id, kind, claimant)
        lease_uuid = _uuid(lease_id)
        if owner is None or lease_uuid is None:
            return False
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                "UPDATE vm_idle_access_leases SET closed_at=clock_timestamp() "
                "WHERE id=$1 AND owner_kind=$2 AND owner_id=$3 AND kind=$4 "
                "AND claimed_by=$5 AND closed_at IS NULL RETURNING id",
                lease_uuid,
                owner_kind,
                owner,
                kind,
                claimant,
            )
            return row is not None
