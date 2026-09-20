"""Internal reservation transactions; no runtime caller or enablement yet.

Waiters must be projected from authenticated creation configuration by the D4
integration. This store consumes that immutable projection, never caller-supplied
fit or occupancy. Policy installation/drain, binding and physical release are
separate protocols; none is implied by constructing this service.
"""

from copy import deepcopy
import hashlib
import json
from uuid import UUID, uuid4

from orchestrator.services.vm_creation_retry_store import (
    VMCreationRetryConflict,
    VMCreationRetryStore,
)
from shared.kubernetes_quantities import normalize_byte_quantity
from shared.vm_resource_accounting import ReservationCharge, account_inventory
from shared.vm_resource_admission import (
    ResourceAdmissionError,
    ResourceVector,
)
from shared.vm_resource_fairness import Waiter, choose_waiter
from shared.vm_resource_policy import parse_resource_policy_values
from shared.vm_resource_inventory import InventoryError, snapshot_is_fresh
from shared.vm_resource_placement import (
    affinity_label_keys,
    node_exclusion,
    preferred_taint_count,
)


def _json(value):
    return json.loads(value) if isinstance(value, str) else value


def _encoded(value):
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode()


def _positive(value):
    if type(value) is not int or not 1 <= value < 2**63:
        raise ResourceAdmissionError("invalid_resource_policy")
    return value


def _policy_values(document, inventory):
    policy = document["policy"]
    if (
        document["mode"] != "same-cluster"
        or document["namespace"] != inventory.namespace
        or policy["stableClusterId"] != inventory.cluster_id
        or any(
            policy[key] is not True
            for key in (
                "observerEnabled",
                "shadowEnabled",
                "enforcementEnabled",
                "clusterWidePodReadAcknowledged",
            )
        )
    ):
        raise ResourceAdmissionError("invalid_resource_policy")
    limits = policy["inventory"]
    for name, expected in (
        ("maxItems", inventory.max_items),
        ("maxBytes", inventory.max_bytes),
        ("staleAfterSeconds", inventory.stale_after_seconds),
        ("historyLimit", inventory.history_limit),
    ):
        if type(limits[name]) is not int or limits[name] != expected:
            raise ResourceAdmissionError("invalid_resource_policy")
    if sorted(limits["nodeLabelKeys"]) != inventory.label_keys:
        raise ResourceAdmissionError("invalid_resource_policy")
    return parse_resource_policy_values(policy)


def _node_document(node):
    vector = node["allocatable"]
    return {
        "metadata": {
            "uid": node["uid"],
            "name": node["name"],
            "labels": node["labels"],
        },
        "spec": {"unschedulable": node["unschedulable"], "taints": node["taints"]},
        "status": {
            "conditions": [
                {"type": "Ready", "status": "True" if node["ready"] else "False"}
            ],
            "allocatable": {
                "cpu": str(vector["cpu_millicores"]) + "m",
                "memory": str(vector["memory_bytes"]),
                "devices.kubevirt.io/kvm": str(vector["kvm_devices"]),
            },
        },
    }


def _fit(snapshot, waiter, accounting, headroom):
    """Unknown topology/placement stays waiting, never becomes size nonfit."""
    placement = _json(waiter["placement"])
    if (
        not isinstance(placement, dict)
        or set(placement)
        != {
            "version",
            "selector",
            "tolerations",
            "required_affinity",
            "storage_class",
            "retained_pvc_uid",
        }
        or type(placement["version"]) is not int
        or placement["version"] != 1
    ):
        raise ResourceAdmissionError("invalid_resource_placement")
    selector, tolerations = placement["selector"], placement["tolerations"]
    if not isinstance(selector, dict) or not isinstance(tolerations, list):
        raise ResourceAdmissionError("invalid_resource_placement")
    required = placement["required_affinity"]
    keys = set(selector) | affinity_label_keys(required)
    if not keys <= set(snapshot["label_keys"]):
        return [], False
    storage_class = placement["storage_class"]
    if not isinstance(storage_class, str) or not 1 <= len(storage_class) <= 253:
        raise ResourceAdmissionError("invalid_resource_placement")
    classes = {item["name"]: item for item in snapshot["storage_classes"]}
    sc = classes.get(storage_class)
    if sc is None:
        return [], False
    pv_affinity = None
    retained = placement["retained_pvc_uid"]
    if retained is not None:
        try:
            if not isinstance(retained, str) or str(UUID(retained)) != retained:
                raise ValueError
        except ValueError:
            raise ResourceAdmissionError("invalid_resource_placement") from None
        pvc = next((item for item in snapshot["pvcs"] if item["uid"] == retained), None)
        if (
            pvc is None
            or pvc["phase"] != "Bound"
            or pvc["storage_class_uid"] != sc["uid"]
        ):
            return [], False
        pv = next(
            (item for item in snapshot["pvs"] if item["uid"] == pvc["pv_uid"]), None
        )
        if pv is None or pv["name"] != pvc["pv_name"] or pv["claim_uid"] != retained:
            return [], False
        pv_affinity = pv["required_affinity"]
    demand = ResourceVector(
        waiter["cpu_millicores"], waiter["memory_bytes"], waiter["kvm_devices"]
    )
    eligible, fits, transient = [], [], False
    for node in snapshot["nodes"]:
        raw = _node_document(node)
        reason = node_exclusion(
            raw,
            selector=selector,
            tolerations=tolerations,
            required_affinity=required,
            pv_affinity=pv_affinity,
        )
        if reason is None:
            reason = node_exclusion(
                raw,
                selector=selector,
                tolerations=tolerations,
                pv_affinity=sc["allowed_topology"],
            )
        if reason == "invalid_placement":
            raise ResourceAdmissionError("invalid_resource_placement")
        if reason is not None:
            transient |= reason in {
                "node_identity",
                "node_not_ready",
                "node_cordoned",
                "kvm_unavailable",
            }
            continue
        if node["labels"].get("kubernetes.io/hostname") != node["name"]:
            transient = True
            continue
        # Static capacity excludes occupancy, but includes explicit headroom.
        capacity = ResourceVector(
            *(
                max(0, a - b)
                for a, b in zip(
                    ResourceVector(**node["allocatable"]).components,
                    headroom.components,
                )
            )
        )
        eligible.append(demand.fits(capacity))
        if demand.fits(accounting.nodes[node["uid"]].available):
            fits.append(
                (preferred_taint_count(raw, tolerations), node["name"], node["uid"])
            )
    return [item[2] for item in sorted(fits)], bool(eligible) and not any(
        eligible
    ) and not transient


class VMResourceReservationStore:
    def __init__(self, db, *, inventory, policy_document, policy_revision):
        self.db, self.inventory = db, inventory
        self.policy_document = deepcopy(policy_document)
        self.policy_revision = _positive(policy_revision)
        try:
            encoded = _encoded(policy_document)
            if (
                "sha256:" + hashlib.sha256(encoded).hexdigest()
                != inventory.policy_digest
            ):
                raise ResourceAdmissionError("resource_policy_changed")
            self.cost, self.headroom, self.max_bypasses, self.aging_seconds = (
                _policy_values(self.policy_document, inventory)
            )
            self.policy_encoded = encoded
        except (ValueError, TypeError, KeyError):
            raise ResourceAdmissionError("invalid_resource_policy") from None
        self.creation = VMCreationRetryStore(db)

    async def admit(self, *, request_id):
        try:
            async with self.db.acquire() as conn, conn.transaction():
                return await self._admit(conn, request_id)
        except (VMCreationRetryConflict, ResourceAdmissionError, InventoryError) as exc:
            return {"action": "unavailable", "reason": str(exc)}

    async def _lock_policy(self, conn, *, allow_drain=False):
        policy = await conn.fetchrow(
            "SELECT * FROM vm_resource_admission_policy WHERE cluster_id=$1 FOR UPDATE",
            self.inventory.cluster_id,
        )
        if (
            policy is None
            or policy["namespace"] != self.inventory.namespace
            or policy["policy_digest"] != self.inventory.policy_digest
            or policy["revision"] != self.policy_revision
            or _encoded(_json(policy["document"])) != self.policy_encoded
            or policy["mode"]
            not in ({"enforce", "drain"} if allow_drain else {"enforce"})
        ):
            raise ResourceAdmissionError("resource_policy_changed")
        return policy

    async def _admit(self, conn, request_id):
        retry, job = await self.creation._effect_scope(conn, request_id)
        await self.creation._current(
            conn, job, retry["provision_generation"], retry=retry
        )
        effects = await conn.fetch(
            "SELECT * FROM vm_creation_effects WHERE request_id=$1 ORDER BY effect_number FOR UPDATE",
            retry["request_id"],
        )
        inventory = self.inventory
        policy = await self._lock_policy(conn)
        # All legitimate writers serialize on policy before taking these rows.
        # Read idempotence before inventory, but never before request authority.
        held = await conn.fetchrow(
            "SELECT * FROM vm_resource_reservations WHERE request_id=$1 AND state<>'released'",
            retry["request_id"],
        )
        if retry["state"] not in {"queued", "reconciling", "succeeded"}:
            raise ResourceAdmissionError("creation_request_ineligible")
        if held is not None:
            if (
                held["cluster_id"] != inventory.cluster_id
                or held["policy_digest"] != inventory.policy_digest
            ):
                raise ResourceAdmissionError("resource_policy_changed")
            if held["state"] == "teardown":
                raise ResourceAdmissionError("reservation_teardown")
            await self._deadline(conn, retry)
            return self._reserved(held)
        if retry["state"] == "succeeded" or effects:
            raise ResourceAdmissionError("creation_effect_already_issued")
        head = await conn.fetchrow(
            "SELECT * FROM vm_resource_inventory_heads WHERE cluster_id=$1 AND policy_digest=$2 FOR UPDATE",
            inventory.cluster_id,
            inventory.policy_digest,
        )
        if head is None or head["current_snapshot_id"] is None:
            raise ResourceAdmissionError("inventory_missing")
        if (
            head["namespace"] != inventory.namespace
            or _json(head["label_keys"]) != inventory.label_keys
        ):
            raise ResourceAdmissionError("inventory_scope_changed")
        observation = await conn.fetchrow(
            "SELECT * FROM vm_resource_inventory_snapshots WHERE snapshot_id=$1",
            head["current_snapshot_id"],
        )
        snapshot = inventory._snapshot(
            _json(observation["document"]), observation["digest"]
        )
        for node in sorted(snapshot["nodes"], key=lambda item: item["uid"]):
            await conn.execute(
                "INSERT INTO vm_resource_nodes(cluster_id,node_uid,node_name) VALUES($1,$2,$3) ON CONFLICT(cluster_id,node_uid) DO NOTHING",
                inventory.cluster_id,
                UUID(node["uid"]),
                node["name"],
            )
        nodes = await conn.fetch(
            "SELECT * FROM vm_resource_nodes WHERE cluster_id=$1 ORDER BY node_uid FOR UPDATE",
            inventory.cluster_id,
        )
        node_names = {str(row["node_uid"]): row["node_name"] for row in nodes}
        if any(node_names[node["uid"]] != node["name"] for node in snapshot["nodes"]):
            raise ResourceAdmissionError("reservation_node_identity")
        waiters = await conn.fetch(
            "SELECT * FROM vm_resource_waiters WHERE cluster_id=$1 AND policy_digest=$2 AND state IN ('waiting','nonfit') ORDER BY request_id FOR UPDATE",
            inventory.cluster_id,
            inventory.policy_digest,
        )
        reservations = await conn.fetch(
            "SELECT r.*,w.job_id,w.provision_generation FROM vm_resource_reservations r JOIN vm_resource_waiters w ON w.request_id=r.request_id WHERE r.cluster_id=$1 AND r.state<>'released' ORDER BY r.id FOR UPDATE OF r",
            inventory.cluster_id,
        )
        owners = await conn.fetch(
            "SELECT * FROM vm_resource_owner_fairness WHERE cluster_id=$1 ORDER BY owner_key FOR UPDATE",
            inventory.cluster_id,
        )
        # The head lock holds its pointer stable. Sample time only after all
        # potentially waiting resource locks; execution identity is already held.
        now = await self._deadline(conn, retry)
        if head["observation_conflict"]:
            raise ResourceAdmissionError("inventory_observation_conflict")
        if not snapshot["complete"]:
            raise ResourceAdmissionError("inventory_incomplete")
        if not snapshot_is_fresh(
            snapshot,
            received_at=observation["received_at"],
            now=now,
            stale_after_seconds=inventory.stale_after_seconds,
        ):
            raise ResourceAdmissionError("inventory_stale")
        target = next(
            (row for row in waiters if row["request_id"] == retry["request_id"]), None
        )
        if target is None:
            raise ResourceAdmissionError("resource_waiter_missing")
        request = retry["canonical_request"]
        expected = self.cost.cost(request["cpu_cores"], request["memory"])
        if (
            target["job_id"] != retry["job_id"]
            or target["provision_generation"] != retry["provision_generation"]
            or target["request_digest"] != retry["request_digest"]
            or target["guest_vcpus"] != request["cpu_cores"]
            or target["guest_memory_bytes"]
            != normalize_byte_quantity(request["memory"]).normalized_value
            or ResourceVector(
                target["cpu_millicores"], target["memory_bytes"], target["kvm_devices"]
            )
            != expected
        ):
            raise ResourceAdmissionError("resource_waiter_changed")
        charges = [
            ReservationCharge(
                reservation_id=str(row["id"]),
                node_uid=str(row["node_uid"]),
                node_name=row["node_name"],
                owner_kind="job",
                owner_id=str(row["job_id"]),
                provision_generation=str(row["provision_generation"]),
                vector=ResourceVector(
                    row["cpu_millicores"], row["memory_bytes"], row["kvm_devices"]
                ),
                state=row["state"],
                **{
                    key: str(row[key]) if row[key] else None
                    for key in ("vm_uid", "vmi_uid", "launcher_uid")
                },
            )
            for row in reservations
        ]
        accounting = account_inventory(snapshot, charges, headroom=self.headroom)
        candidates, fits, nonfit = [], {}, set()
        for row in waiters:
            if (
                row["state"] == "nonfit"
                and row["evaluated_snapshot_id"] == observation["snapshot_id"]
            ):
                continue
            identity = str(row["request_id"])
            fit, impossible = _fit(snapshot, row, accounting, self.headroom)
            fits[identity] = fit
            if impossible:
                nonfit.add(identity)
            candidates.append(
                Waiter(
                    identity,
                    row["owner_key"],
                    row["enqueued_at"],
                    row["priority"],
                    row["bypasses"],
                    row["protected_order"],
                )
            )
        choice = choose_waiter(
            candidates,
            fit_nodes=fits,
            nonfit_ids=nonfit,
            owner_last_admitted={
                row["owner_key"]: row["last_admitted_sequence"] for row in owners
            },
            now=now,
            aging_seconds=self.aging_seconds,
            max_bypasses=self.max_bypasses,
        )
        if choice.request_id is not None and choice.request_id != request_id:
            return {"action": "nominate", "request_id": choice.request_id}
        if (
            target["state"] == "nonfit"
            and target["evaluated_snapshot_id"] != observation["snapshot_id"]
        ):
            await conn.execute(
                "UPDATE vm_resource_waiters SET state='waiting',reason=NULL,revision=revision+1 WHERE request_id=$1",
                retry["request_id"],
            )
        if choice.action == "nonfit":
            await conn.execute(
                "UPDATE vm_resource_waiters SET state='nonfit',reason='resource_size_nonfit',evaluated_snapshot_id=$2,revision=revision+1 WHERE request_id=$1",
                retry["request_id"],
                observation["snapshot_id"],
            )
            return {"action": "nonfit"}
        if choice.action == "protect":
            await conn.execute(
                "UPDATE vm_resource_waiters SET protected_order=COALESCE(protected_order,$2),revision=revision+1 WHERE request_id=$1",
                retry["request_id"],
                policy["admission_sequence"],
            )
            return {"action": "protected"}
        if choice.action != "admit":
            return {"action": "wait"}
        sequence = policy["admission_sequence"] + 1
        _positive(sequence)
        reservation = await conn.fetchrow(
            "INSERT INTO vm_resource_reservations(id,request_id,revision,cluster_id,policy_digest,node_uid,node_name,cpu_millicores,memory_bytes,kvm_devices,snapshot_id,snapshot_digest) "
            "VALUES($1,$2,(SELECT COALESCE(max(revision),0)+1 FROM vm_resource_reservations WHERE request_id=$2),$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *",
            uuid4(),
            retry["request_id"],
            inventory.cluster_id,
            inventory.policy_digest,
            UUID(choice.node_uid),
            node_names[choice.node_uid],
            expected.cpu_millicores,
            expected.memory_bytes,
            expected.kvm_devices,
            observation["snapshot_id"],
            observation["digest"],
        )
        await conn.execute(
            "UPDATE vm_resource_waiters SET state='admitted',reason=NULL,evaluated_snapshot_id=$2,revision=revision+1 WHERE request_id=$1",
            retry["request_id"],
            observation["snapshot_id"],
        )
        for identity in sorted(choice.bypassed):
            await conn.execute(
                "UPDATE vm_resource_waiters SET bypasses=bypasses+1,protected_order=CASE WHEN bypasses+1 >= $2 THEN COALESCE(protected_order,$3) ELSE protected_order END,revision=revision+1 WHERE request_id=$1",
                UUID(identity),
                self.max_bypasses,
                sequence,
            )
        await conn.execute(
            "UPDATE vm_resource_admission_policy SET admission_sequence=$2 WHERE cluster_id=$1",
            inventory.cluster_id,
            sequence,
        )
        await conn.execute(
            "INSERT INTO vm_resource_owner_fairness(cluster_id,owner_key,last_admitted_sequence) VALUES($1,$2,$3) ON CONFLICT(cluster_id,owner_key) DO UPDATE SET last_admitted_sequence=EXCLUDED.last_admitted_sequence",
            inventory.cluster_id,
            target["owner_key"],
            sequence,
        )
        return self._reserved(reservation)

    @staticmethod
    async def _deadline(conn, retry):
        now = await conn.fetchval("SELECT clock_timestamp()")
        if (
            retry["admission_deadline"] is not None
            and retry["admission_deadline"] <= now
        ):
            raise VMCreationRetryConflict("job_admission_expired")
        return now

    @staticmethod
    def _reserved(row):
        return {
            "action": "admitted",
            "reservation_id": str(row["id"]),
            "revision": row["revision"],
            "node_uid": str(row["node_uid"]),
            "node_name": row["node_name"],
        }
