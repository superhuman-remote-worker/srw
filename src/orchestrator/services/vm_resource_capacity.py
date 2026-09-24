"""Read-only admin projection of installed VM policy and durable occupancy.

The repeatable-read snapshot is diagnostic, never admission authority. No owner
locks, Kubernetes calls or mutations occur here. Resource arithmetic is shared
with admission; unavailable inventory does not erase outstanding reservations.
"""

import json

from orchestrator.services.vm_resource_inventory_store import VMResourceInventoryStore
from orchestrator.services.vm_resource_reservation_store import (
    _charge_vector,
    _node_document,
)
from shared.vm_resource_accounting import (
    ReservationCharge,
    account_inventory,
    reservation_category,
)
from shared.vm_resource_admission import ResourceAdmissionError, ResourceVector
from shared.vm_resource_inventory import (
    InventoryError,
    inventory_time,
    snapshot_is_fresh,
)
from shared.vm_resource_placement import node_exclusion
from shared.vm_resource_policy import validate_enforcement_resource_policy


_CATEGORIES = ("unbound", "bound_reserved", "active", "warm", "teardown")
_NODE_CATEGORIES = (
    "allocatable",
    "headroom",
    "external",
    *_CATEGORIES,
    "available",
    "shortfall",
)
_ZERO = ResourceVector(0, 0, 0)
# A diagnostic threshold, not permission to expire/release a reservation.
TEARDOWN_OVERDUE_SECONDS = 300


def _json(value):
    return json.loads(value) if isinstance(value, str) else value


def _age(now, observed):
    return max(0.0, (now - observed).total_seconds()) if observed is not None else None


def _held(rows):
    totals = {key: _ZERO for key in _CATEGORIES}
    unknown = set()
    for row in rows:
        key = reservation_category(
            row["state"],
            vm_uid=row["vm_uid"],
            vmi_uid=row["current_vmi_uid"],
            launcher_uid=row["current_launcher_uid"],
        )
        totals[key] += _charge_vector(row)
        if row["resource_version"] != 2:
            unknown.add(key)
    result = {key: value.to_six_dict() for key, value in totals.items()}
    result["total"] = sum(totals.values(), _ZERO).to_six_dict()
    for key in unknown | ({"total"} if unknown else set()):
        for dimension in (
            "ephemeral_storage_bytes",
            "tun_devices",
            "vhost_net_devices",
        ):
            result[key][dimension] = None
    return result


def _charges(rows):
    result = []
    for row in rows:
        waiter = _json(row["waiter"])
        kind = waiter.get("owner_kind", "job")
        result.append(
            ReservationCharge(
                reservation_id=str(row["id"]),
                node_uid=str(row["node_uid"]),
                node_name=row["node_name"],
                owner_kind=kind,
                owner_id=str(
                    waiter.get("thread_id") if kind == "thread" else waiter["job_id"]
                ),
                provision_generation=str(waiter["provision_generation"]),
                vector=_charge_vector(row),
                state=row["state"],
                version=row["resource_version"],
                vm_uid=str(row["vm_uid"]) if row["vm_uid"] else None,
                vmi_uid=str(row["current_vmi_uid"]) if row["current_vmi_uid"] else None,
                launcher_uid=str(row["current_launcher_uid"])
                if row["current_launcher_uid"]
                else None,
            )
        )
    return result


def _waiting(rows, now):
    oldest = min((row["enqueued_at"] for row in rows), default=None)
    return {
        "count": sum(row["state"] == "waiting" for row in rows),
        "nonfit": sum(row["state"] == "nonfit" for row in rows),
        "oldest_age_seconds": _age(now, oldest),
        "bypasses": sum(row["bypasses"] for row in rows),
        "protected": sum(row["protected_order"] is not None for row in rows),
    }


def _teardown(rows, now):
    held = [row for row in rows if row["state"] == "teardown"]
    ages = [_age(now, row["teardown_progress_at"]) for row in held]
    known = [age for age in ages if age is not None]
    return {
        "count": len(held),
        "unknown_age": ages.count(None),
        "overdue": sum(age >= TEARDOWN_OVERDUE_SECONDS for age in known),
        "oldest_progress_age_seconds": max(known, default=None),
        "overdue_after_seconds": TEARDOWN_OVERDUE_SECONDS,
    }


def _project(policy, observation, reservations, waiters, now):
    result = {
        "cluster_id": policy["cluster_id"],
        "namespace": policy["namespace"],
        "mode": policy["mode"],
        "policy_digest": policy["policy_digest"],
        "available": False,
        "reason": "inventory_missing",
        "inventory": None,
        "held": _held(reservations),
        "waiting": _waiting(waiters, now),
        "teardown": _teardown(reservations, now),
        "totals": None,
        "nodes": None,
        "orphaned_held": None,
        "pending_external": None,
        # The controller's count cap is not in the installed resource policy.
        # Never substitute a process default or reservation count for it.
        "count_backstop": {
            "maximum": None,
            "observed": None,
            "reason": "maximum_not_observed",
        },
    }
    try:
        parsed = validate_enforcement_resource_policy(_json(policy["document"]))
        settings = parsed.inventory
        if (settings.cluster_id, settings.namespace, parsed.policy_digest) != (
            policy["cluster_id"],
            policy["namespace"],
            policy["policy_digest"],
        ):
            raise ResourceAdmissionError("resource_policy_changed")
        if observation is None or observation["document"] is None:
            return result
        inventory = VMResourceInventoryStore(
            None,
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
        snapshot = inventory._snapshot(
            _json(observation["document"]), observation["digest"]
        )
        fresh = snapshot_is_fresh(
            snapshot,
            received_at=observation["received_at"],
            now=now,
            stale_after_seconds=settings.stale_after_seconds,
        )
        result["inventory"] = {
            "observed_at": snapshot["finished_at"],
            "received_at": observation["received_at"].isoformat(),
            "age_seconds": _age(now, inventory_time(snapshot["started_at"])),
            "complete": snapshot["complete"],
            "fresh": fresh,
            "stale_after_seconds": settings.stale_after_seconds,
        }
        if observation["observation_conflict"]:
            raise InventoryError("inventory_observation_conflict")
        if not snapshot["complete"]:
            raise InventoryError("inventory_incomplete")
        if not fresh:
            raise InventoryError("inventory_stale")
        if settings.protocol != 2:
            raise ResourceAdmissionError("legacy_occupancy_unclassified")
        installed = snapshot["installed_profile"]
        if (installed["namespace"], installed["name"], installed["profile"]) != (
            settings.kubevirt_namespace,
            settings.kubevirt_name,
            parsed.launcher_profile,
        ):
            raise ResourceAdmissionError("installed_launcher_profile_changed")
        accounting = account_inventory(
            snapshot, _charges(reservations), headroom=parsed.headroom
        )
        result["totals"] = {
            key: sum(
                (getattr(node, key) for node in accounting.nodes.values()), _ZERO
            ).to_six_dict()
            for key in _NODE_CATEGORIES
        }
        result["nodes"] = []
        for node in snapshot["nodes"]:
            value = accounting.nodes[node["uid"]]
            raw = _node_document(node)
            # Taints/selectors/affinity belong to each request. General health
            # must not label a tainted node unusable for all tolerated requests.
            raw["spec"]["taints"] = []
            reason = value.blocked_reason or node_exclusion(raw)
            result["nodes"].append(
                {
                    "name": node["name"],
                    "general_exclusion": reason,
                    "request_fit_required": True,
                    "resources": {
                        key: getattr(value, key).to_six_dict()
                        for key in _NODE_CATEGORIES
                    },
                }
            )
        result["orphaned_held"] = {
            "count": len(accounting.orphaned_held),
            "resources": sum(accounting.orphaned_held.values(), _ZERO).to_six_dict(),
        }
        result["pending_external"] = accounting.pending_external.to_six_dict()
        result["count_backstop"]["observed"] = sum(
            not vm["deleting"]
            and vm["name"].startswith("agent-vm-")
            and not vm["name"].startswith("agent-vm-golden-")
            for vm in snapshot["vms"]
        )
        result.update(available=True, reason=None)
    except (ResourceAdmissionError, InventoryError) as exc:
        result["reason"] = str(exc)
    except (ValueError, TypeError, KeyError):
        result["reason"] = "invalid_resource_snapshot"
    return result


async def vm_capacity_snapshot(db):
    """Read policy, inventory and all held generations at one MVCC boundary."""
    async with (
        db.acquire() as conn,
        conn.transaction(isolation="repeatable_read", readonly=True),
    ):
        policies = await conn.fetch(
            "SELECT * FROM vm_resource_admission_policy ORDER BY cluster_id"
        )
        clusters = []
        # Sampling after the initial read avoids projecting observations from
        # after this snapshot. No lock acquisition can stall this timestamp.
        now = await conn.fetchval("SELECT clock_timestamp()")
        for policy in policies:
            observation = await conn.fetchrow(
                "SELECT h.observation_conflict,s.document,s.digest,s.received_at "
                "FROM vm_resource_inventory_heads h LEFT JOIN vm_resource_inventory_snapshots s "
                "ON s.snapshot_id=h.current_snapshot_id "
                "WHERE h.cluster_id=$1 AND h.policy_digest=$2",
                policy["cluster_id"],
                policy["policy_digest"],
            )
            reservations = await conn.fetch(
                "SELECT r.*,to_jsonb(w) AS waiter,"
                "COALESCE(s.successor_vmi_uid,r.vmi_uid) AS current_vmi_uid,"
                "COALESCE(s.successor_launcher_uid,r.launcher_uid) AS current_launcher_uid,"
                "o.last_progress_at AS teardown_progress_at "
                "FROM vm_resource_reservations r JOIN vm_resource_waiters w USING(request_id) "
                "LEFT JOIN LATERAL (SELECT successor_vmi_uid,successor_launcher_uid "
                "FROM vm_resource_recovery_successors WHERE reservation_id=r.id "
                "ORDER BY ordinal DESC LIMIT 1) s ON true "
                "LEFT JOIN LATERAL (SELECT last_progress_at FROM vm_idle_operations "
                "WHERE owner_kind=COALESCE(to_jsonb(w)->>'owner_kind','job') "
                "AND owner_id=COALESCE((to_jsonb(w)->>'thread_id')::uuid,w.job_id) "
                "AND provision_generation=w.provision_generation AND vm_uid=r.vm_uid "
                "AND vmi_uid=COALESCE(s.successor_vmi_uid,r.vmi_uid) "
                "AND launcher_uid=COALESCE(s.successor_launcher_uid,r.launcher_uid) "
                "AND phase IN ('releasing','release_held') AND closed_at IS NULL "
                "ORDER BY admitted_at DESC LIMIT 1) o ON true "
                "WHERE r.cluster_id=$1 AND r.state<>'released' ORDER BY r.id",
                policy["cluster_id"],
            )
            waiters = await conn.fetch(
                "SELECT state,enqueued_at,bypasses,protected_order FROM vm_resource_waiters "
                "WHERE cluster_id=$1 AND state IN ('waiting','nonfit')",
                policy["cluster_id"],
            )
            clusters.append(_project(policy, observation, reservations, waiters, now))
    return {
        "available": bool(clusters)
        and all(cluster["available"] for cluster in clusters),
        "reason": (
            None
            if clusters and all(cluster["available"] for cluster in clusters)
            else "resource_inventory_unavailable"
            if clusters
            else "resource_policy_missing"
        ),
        "observed_at": now.isoformat(),
        "clusters": clusters,
    }
