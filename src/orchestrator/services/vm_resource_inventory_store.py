"""Immutable inventory publication; deliberately no Job or admission locks."""

import hmac
import json
from uuid import UUID

from shared.vm_resource_inventory import (
    InventoryError,
    canonical_snapshot,
    inventory_time,
    snapshot_digest,
    snapshot_is_fresh,
)


class VMResourceInventoryStore:
    def __init__(
        self,
        db,
        *,
        cluster_id,
        namespace,
        policy_digest,
        label_keys,
        max_items,
        max_bytes,
        stale_after_seconds,
        history_limit,
    ):
        self.db = db
        self.cluster_id, self.namespace, self.policy_digest = (
            cluster_id,
            namespace,
            policy_digest,
        )
        self.label_keys = sorted(label_keys)
        self.max_items, self.max_bytes = max_items, max_bytes
        self.stale_after_seconds, self.history_limit = (
            stale_after_seconds,
            history_limit,
        )
        for limit in (max_items, max_bytes, stale_after_seconds, history_limit):
            if type(limit) is not int or not 1 <= limit < 2**63:
                raise InventoryError("invalid_inventory_configuration")

    def _snapshot(self, value, digest):
        value = canonical_snapshot(
            value, max_items=self.max_items, max_bytes=self.max_bytes
        )
        if (
            value["cluster_id"] != self.cluster_id
            or value["namespace"] != self.namespace
            or value["policy_digest"] != self.policy_digest
            or value["label_keys"] != self.label_keys
        ):
            raise InventoryError("inventory_scope_changed")
        if not isinstance(digest, str) or not hmac.compare_digest(
            snapshot_digest(value), digest
        ):
            raise InventoryError("inventory_digest_changed")
        return value

    async def publish(self, *, snapshot, digest):
        value = self._snapshot(snapshot, digest)
        snapshot_id = UUID(value["snapshot_id"])
        started, finished = (
            inventory_time(value["started_at"]),
            inventory_time(value["finished_at"]),
        )
        result = None
        async with self.db.acquire() as conn, conn.transaction():
            await conn.execute(
                "INSERT INTO vm_resource_inventory_heads(cluster_id,policy_digest,namespace,label_keys) "
                "VALUES($1,$2,$3,$4::jsonb) ON CONFLICT(cluster_id,policy_digest) DO NOTHING",
                self.cluster_id,
                self.policy_digest,
                self.namespace,
                json.dumps(self.label_keys),
            )
            head = await conn.fetchrow(
                "SELECT * FROM vm_resource_inventory_heads WHERE cluster_id=$1 AND policy_digest=$2 FOR UPDATE",
                self.cluster_id,
                self.policy_digest,
            )
            labels = head["label_keys"]
            if isinstance(labels, str):
                labels = json.loads(labels)
            if head["namespace"] != self.namespace or labels != self.label_keys:
                raise InventoryError("inventory_scope_changed")
            now = await conn.fetchval("SELECT clock_timestamp()")
            existing = await conn.fetchrow(
                "SELECT digest,received_at,cluster_id,policy_digest FROM vm_resource_inventory_snapshots WHERE snapshot_id=$1",
                snapshot_id,
            )
            if existing is not None:
                if (
                    existing["digest"] != digest
                    or existing["cluster_id"] != self.cluster_id
                    or existing["policy_digest"] != self.policy_digest
                ):
                    raise InventoryError("inventory_snapshot_conflict")
                return self._receipt(
                    snapshot_id,
                    digest,
                    existing["received_at"],
                    head["current_snapshot_id"] == snapshot_id,
                )
            if finished > now:
                raise InventoryError("inventory_future_observation")
            high_water = head["observed_high_water"]
            if high_water is not None and started < high_water:
                raise InventoryError("inventory_observation_old")
            if started == high_water:
                # Commit this refusal state. Raising inside the transaction
                # would roll back the invalidation and leave old capacity usable.
                await conn.execute(
                    "UPDATE vm_resource_inventory_heads SET observation_conflict=TRUE "
                    "WHERE cluster_id=$1 AND policy_digest=$2",
                    self.cluster_id,
                    self.policy_digest,
                )
            else:
                await conn.execute(
                    "INSERT INTO vm_resource_inventory_snapshots(snapshot_id,cluster_id,policy_digest,"
                    "controller_id,sequence,digest,started_at,finished_at,received_at,complete,document) "
                    "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb)",
                    snapshot_id,
                    self.cluster_id,
                    self.policy_digest,
                    UUID(value["controller_id"]),
                    value["sequence"],
                    digest,
                    started,
                    finished,
                    now,
                    value["complete"],
                    json.dumps(value),
                )
                await conn.execute(
                    "UPDATE vm_resource_inventory_heads SET current_snapshot_id=$3,observed_high_water=$4,"
                    "observation_conflict=FALSE WHERE cluster_id=$1 AND policy_digest=$2",
                    self.cluster_id,
                    self.policy_digest,
                    snapshot_id,
                    started,
                )
                # D3 must preserve referenced snapshots when reservations are
                # introduced. Today only the current pointer references history.
                await conn.execute(
                    "DELETE FROM vm_resource_inventory_snapshots WHERE snapshot_id IN ("
                    "SELECT snapshot_id FROM vm_resource_inventory_snapshots "
                    "WHERE cluster_id=$1 AND policy_digest=$2 ORDER BY started_at DESC OFFSET $3)",
                    self.cluster_id,
                    self.policy_digest,
                    self.history_limit,
                )
                result = self._receipt(snapshot_id, digest, now, True)
        if result is None:
            raise InventoryError("inventory_observation_conflict")
        return result

    @staticmethod
    def _receipt(snapshot_id, digest, received_at, current):
        return {
            "accepted": True,
            "snapshot_id": str(snapshot_id),
            "digest": digest,
            "received_at": received_at.isoformat(),
            "current": current,
        }

    async def current(self):
        """Diagnostic read only. D3 admission must lock and recheck its snapshot."""
        row = await self.db.fetchrow(
            "SELECT h.observation_conflict,s.document,s.digest,s.received_at,clock_timestamp() AS now "
            "FROM vm_resource_inventory_heads h LEFT JOIN vm_resource_inventory_snapshots s "
            "ON s.snapshot_id=h.current_snapshot_id WHERE h.cluster_id=$1 AND h.policy_digest=$2",
            self.cluster_id,
            self.policy_digest,
        )
        if row is None or row["document"] is None:
            return {"available": False, "reason": "inventory_missing", "snapshot": None}
        value = row["document"]
        if isinstance(value, str):
            value = json.loads(value)
        value = self._snapshot(value, row["digest"])
        reason = None
        if row["observation_conflict"]:
            reason = "inventory_observation_conflict"
        elif not value["complete"]:
            reason = "inventory_incomplete"
        elif not snapshot_is_fresh(
            value,
            received_at=row["received_at"],
            now=row["now"],
            stale_after_seconds=self.stale_after_seconds,
        ):
            reason = "inventory_stale"
        return {
            "available": reason is None,
            "reason": reason,
            "snapshot": value,
            "received_at": row["received_at"],
            "digest": row["digest"],
        }
