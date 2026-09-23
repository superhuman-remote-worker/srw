"""Explicit installed-policy transitions for the Job resource runtime."""

from dataclasses import dataclass
import json
from typing import Literal

from shared.vm_resource_admission import ResourceAdmissionError
from shared.vm_resource_policy import (
    EnforcementResourcePolicySnapshot,
    validate_enforcement_resource_policy_snapshot,
)


PolicyMode = Literal["off", "shadow", "enforce", "drain"]


@dataclass(frozen=True, slots=True)
class ResourcePolicyReceipt:
    cluster_id: str
    namespace: str
    policy_digest: str
    revision: int
    mode: PolicyMode


def _canonical(value) -> bytes:
    if isinstance(value, str):
        value = json.loads(value)
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode()


def _valid_receipt(value) -> bool:
    return (
        type(value) is ResourcePolicyReceipt
        and isinstance(value.cluster_id, str)
        and isinstance(value.namespace, str)
        and isinstance(value.policy_digest, str)
        and type(value.revision) is int
        and 1 <= value.revision < 2**63
        and type(value.mode) is str
        and value.mode in {"off", "shadow", "enforce", "drain"}
    )


class VMResourcePolicyLifecycleStore:
    """Own explicit lifecycle transitions without creating runtime authority."""

    def __init__(self, db, *, snapshot):
        self.db = db
        self.snapshot: EnforcementResourcePolicySnapshot = (
            validate_enforcement_resource_policy_snapshot(snapshot)
        )

    def _receipt(self, row) -> ResourcePolicyReceipt:
        try:
            if (
                row is None
                or row["cluster_id"] != self.snapshot.inventory.cluster_id
                or row["namespace"] != self.snapshot.inventory.namespace
                or row["policy_digest"] != self.snapshot.policy_digest
                or _canonical(row["document"]) != self.snapshot.canonical_document
                or type(row["revision"]) is not int
                or not 1 <= row["revision"] < 2**63
                or row["mode"] not in {"off", "shadow", "enforce", "drain"}
            ):
                raise ValueError
            return ResourcePolicyReceipt(
                row["cluster_id"],
                row["namespace"],
                row["policy_digest"],
                row["revision"],
                row["mode"],
            )
        except (ValueError, TypeError, KeyError, UnicodeError, json.JSONDecodeError):
            raise ResourceAdmissionError("resource_policy_changed") from None

    async def ensure_shadow(self) -> ResourcePolicyReceipt:
        """Install once, or replay only the same already-shadow policy."""
        async with self.db.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                "INSERT INTO vm_resource_admission_policy(cluster_id,namespace,policy_digest,document,mode) "
                "VALUES($1,$2,$3,$4::jsonb,'shadow') "
                "ON CONFLICT (cluster_id) DO NOTHING RETURNING *",
                self.snapshot.inventory.cluster_id,
                self.snapshot.inventory.namespace,
                self.snapshot.policy_digest,
                self.snapshot.canonical_document.decode(),
            )
            if row is None:
                row = await conn.fetchrow(
                    "SELECT * FROM vm_resource_admission_policy WHERE cluster_id=$1 FOR UPDATE",
                    self.snapshot.inventory.cluster_id,
                )
            receipt = self._receipt(row)
            if receipt.mode != "shadow":
                raise ResourceAdmissionError("resource_policy_changed")
            return receipt

    async def begin_drain(self, *, expected) -> ResourcePolicyReceipt:
        """Move one exact shadow/enforce epoch to drain, or replay its receipt."""
        if not _valid_receipt(expected):
            raise ResourceAdmissionError("resource_policy_changed")
        async with self.db.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                "SELECT * FROM vm_resource_admission_policy WHERE cluster_id=$1 FOR UPDATE",
                self.snapshot.inventory.cluster_id,
            )
            current = self._receipt(row)
            if current != expected:
                raise ResourceAdmissionError("resource_policy_changed")
            if current.mode == "drain":
                return current
            if current.mode not in {"shadow", "enforce"}:
                raise ResourceAdmissionError("resource_policy_changed")
            row = await conn.fetchrow(
                "UPDATE vm_resource_admission_policy "
                "SET mode='drain',revision=revision+1 "
                "WHERE cluster_id=$1 AND revision=$2 RETURNING *",
                current.cluster_id,
                current.revision,
            )
            if row is None:
                raise ResourceAdmissionError("resource_policy_changed")
            return self._receipt(row)

    async def _unclassified_occupancy_absent(self, conn):
        """Use one fresh signed whole-cluster sample; unknown is never empty."""
        from orchestrator.services.vm_resource_inventory_store import (
            VMResourceInventoryStore,
        )
        from shared.vm_resource_accounting import account_inventory
        from shared.vm_resource_inventory import snapshot_is_fresh

        settings = self.snapshot.inventory
        inventory = VMResourceInventoryStore(
            self.db,
            cluster_id=settings.cluster_id,
            namespace=settings.namespace,
            policy_digest=settings.policy_digest,
            label_keys=settings.label_keys,
            max_items=settings.max_items,
            max_bytes=settings.max_bytes,
            stale_after_seconds=settings.stale_after_seconds,
            history_limit=settings.history_limit,
            protocol=settings.protocol,
            kubevirt_namespace=settings.kubevirt_namespace,
            kubevirt_name=settings.kubevirt_name,
        )
        head = await conn.fetchrow(
            "SELECT * FROM vm_resource_inventory_heads WHERE cluster_id=$1 "
            "AND policy_digest=$2 FOR UPDATE",
            settings.cluster_id, settings.policy_digest,
        )
        if head is None or head["current_snapshot_id"] is None or head["observation_conflict"]:
            raise ResourceAdmissionError("inventory_unavailable")
        observed = await conn.fetchrow(
            "SELECT * FROM vm_resource_inventory_snapshots WHERE snapshot_id=$1",
            head["current_snapshot_id"],
        )
        if observed is None:
            raise ResourceAdmissionError("inventory_unavailable")
        document = observed["document"]
        if isinstance(document, str):
            document = json.loads(document)
        snapshot = inventory._snapshot(document, observed["digest"])
        now = await conn.fetchval("SELECT clock_timestamp()")
        if not snapshot["complete"] or not snapshot_is_fresh(
            snapshot, received_at=observed["received_at"], now=now,
            stale_after_seconds=settings.stale_after_seconds,
        ):
            raise ResourceAdmissionError("inventory_unavailable")
        installed = snapshot["installed_profile"]
        if (
            installed["namespace"] != settings.kubevirt_namespace
            or installed["name"] != settings.kubevirt_name
            or _canonical(installed["profile"])
            != _canonical(self.snapshot.launcher_profile)
        ):
            raise ResourceAdmissionError("installed_launcher_profile_changed")
        # With no held rows, every SRW-attributable VM/VMI/Pod must make the
        # accounting function refuse; unrelated external Pods remain charged
        # only against their Node.
        account_inventory(snapshot, [], headroom=self.snapshot.headroom)

    async def activate_enforce(self, *, expected) -> ResourcePolicyReceipt:
        """Activate an exact shadow policy only after a fresh empty-owner audit."""
        if not _valid_receipt(expected):
            raise ResourceAdmissionError("resource_policy_changed")
        async with self.db.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                "SELECT * FROM vm_resource_admission_policy WHERE cluster_id=$1 FOR UPDATE",
                self.snapshot.inventory.cluster_id,
            )
            current = self._receipt(row)
            if current != expected or current.mode != "shadow":
                raise ResourceAdmissionError("resource_policy_changed")
            if await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM vm_resource_reservations "
                "WHERE cluster_id=$1 AND state<>'released')",
                current.cluster_id,
            ):
                raise ResourceAdmissionError("resource_charge_unresolved")
            await self._unclassified_occupancy_absent(conn)
            updated = await conn.fetchrow(
                "UPDATE vm_resource_admission_policy SET mode='enforce',"
                "revision=revision+1 WHERE cluster_id=$1 AND revision=$2 RETURNING *",
                current.cluster_id, current.revision,
            )
            if updated is None:
                raise ResourceAdmissionError("resource_policy_changed")
            return self._receipt(updated)

    async def finalize_off(self, *, expected) -> ResourcePolicyReceipt:
        """Retire drain only after no reservation, waiter or SRW VM remains."""
        if not _valid_receipt(expected):
            raise ResourceAdmissionError("resource_policy_changed")
        async with self.db.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                "SELECT * FROM vm_resource_admission_policy WHERE cluster_id=$1 FOR UPDATE",
                self.snapshot.inventory.cluster_id,
            )
            current = self._receipt(row)
            if current != expected or current.mode != "drain":
                raise ResourceAdmissionError("resource_policy_changed")
            if await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM vm_resource_reservations "
                "WHERE cluster_id=$1 AND state<>'released') OR "
                "EXISTS(SELECT 1 FROM vm_resource_waiters WHERE cluster_id=$1 "
                "AND state NOT IN ('cancelled','released'))",
                current.cluster_id,
            ):
                raise ResourceAdmissionError("resource_charge_unresolved")
            await self._unclassified_occupancy_absent(conn)
            updated = await conn.fetchrow(
                "UPDATE vm_resource_admission_policy SET mode='off',"
                "revision=revision+1 WHERE cluster_id=$1 AND revision=$2 RETURNING *",
                current.cluster_id, current.revision,
            )
            if updated is None:
                raise ResourceAdmissionError("resource_policy_changed")
            return self._receipt(updated)
