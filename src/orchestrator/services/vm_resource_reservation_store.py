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
from shared.vm_creation_issuance import canonical_configuration_digest
from shared.vm_creation_retry import canonical_request_digest
from shared.vm_resource_accounting import ReservationCharge, account_inventory
from shared.vm_resource_admission import (
    ResourceAdmissionError,
    ResourceVector,
)
from shared.vm_resource_fairness import Waiter, choose_waiter
from shared.vm_resource_policy import (
    parse_resource_policy_values,
    parse_whole_launcher_policy_values,
)
from shared.vm_resource_inventory import InventoryError, snapshot_is_fresh
from shared.vm_resource_configuration import validate_resource_configuration
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
    if inventory.protocol == 2 and (
        limits["kubevirtNamespace"] != inventory.kubevirt_namespace
        or limits["kubevirtName"] != inventory.kubevirt_name
    ):
        raise ResourceAdmissionError("invalid_resource_policy")
    return (
        parse_whole_launcher_policy_values(policy)
        if inventory.protocol == 2 else parse_resource_policy_values(policy)
    )


def _expected_waiter_request_fields(retry, *, inventory, policy_document, cost):
    """Rebuild waiter request fields; fairness metadata is write-once separately."""
    try:
        request = _json(retry["canonical_request"])
        if (
            not isinstance(request, dict)
            or canonical_request_digest(request) != retry["request_digest"]
        ):
            raise ValueError
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        raise ResourceAdmissionError("resource_waiter_changed") from None

    configuration = _json(retry.get("controller_configuration"))
    if (
        not isinstance(configuration, dict)
        or type(configuration.get("version")) is not int
        or configuration["version"] != (3 if inventory.protocol == 2 else 2)
    ):
        raise ResourceAdmissionError("resource_configuration_unavailable")
    try:
        if (
            canonical_configuration_digest(configuration)
            != retry["controller_configuration_digest"]
        ):
            raise ValueError
        resource = configuration["resource_admission"]
        validate_resource_configuration(resource, configuration)
        profile = resource["template_profile"]
        mapping = resource["host_mapping"]
        if (
            configuration["namespace"] != inventory.namespace
            or resource["cluster_id"] != inventory.cluster_id
            or resource["policy_digest"] != inventory.policy_digest
            or _encoded(mapping["policy"])
            != _encoded(policy_document["policy"]["hostCost"])
            or type(request["cpu_cores"]) is not int
            or profile["guest_vcpus"] != request["cpu_cores"]
        ):
            raise ValueError
        if inventory.protocol == 2 and (
            resource["version"] != 2
            or _encoded(resource["launcher_profile"])
            != _encoded(policy_document["policy"]["launcherProfile"])
            or _encoded(resource["host_mapping"]["policy"])
            != _encoded(policy_document["policy"]["hostCost"])
        ):
            raise ValueError
        memory = normalize_byte_quantity(request["memory"]).normalized_value
        if profile["guest_memory_bytes"] != memory:
            raise ValueError
        expected_vector = cost.cost(request["cpu_cores"], request["memory"])
        frozen_vector = (
            ResourceVector.from_six_dict(mapping["vector"])
            if inventory.protocol == 2 else ResourceVector(**mapping["vector"])
        )
        if any(type(value) is not int for value in frozen_vector.components) or (
            frozen_vector != expected_vector
        ):
            raise ValueError
    except (
        ValueError,
        TypeError,
        KeyError,
        UnicodeError,
        RecursionError,
        ResourceAdmissionError,
    ):
        raise ResourceAdmissionError("resource_configuration_changed") from None

    fields = {
        "request_id": retry["request_id"],
        "job_id": retry["job_id"],
        "provision_generation": retry["provision_generation"],
        "cluster_id": inventory.cluster_id,
        "policy_digest": inventory.policy_digest,
        "request_digest": retry["request_digest"],
        "guest_vcpus": profile["guest_vcpus"],
        "guest_memory_bytes": profile["guest_memory_bytes"],
        **(
            expected_vector.to_six_dict()
            if inventory.protocol == 2 else expected_vector.to_dict()
        ),
        "placement": {
            "version": 1,
            "selector": deepcopy(profile["selector"]),
            "tolerations": deepcopy(profile["tolerations"]),
            "required_affinity": deepcopy(profile["required_affinity"]),
            "storage_class": profile["storage_class"],
            "retained_pvc_uid": (
                str(retry["expected_pvc_uid"])
                if retry["expected_pvc_uid"] is not None
                else None
            ),
        },
    }
    if inventory.protocol == 2:
        fields["resource_version"] = 2
    return fields


def _waiter_request_fields_match(actual, expected):
    for key, value in expected.items():
        if key == "placement":
            try:
                if _encoded(_json(actual[key])) != _encoded(value):
                    return False
            except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
                return False
        elif type(actual[key]) is not type(value) or actual[key] != value:
            return False
    return True


def _node_document(node):
    vector = node["allocatable"]
    allocatable = {
        "cpu": str(vector["cpu_millicores"]) + "m",
        "memory": str(vector["memory_bytes"]),
        "devices.kubevirt.io/kvm": str(vector["kvm_devices"]),
    }
    if "ephemeral_storage_bytes" in vector:
        allocatable.update({
            "ephemeral-storage": str(vector["ephemeral_storage_bytes"]),
            "devices.kubevirt.io/tun": str(vector["tun_devices"]),
            "devices.kubevirt.io/vhost-net": str(vector["vhost_net_devices"]),
        })
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
            "allocatable": allocatable,
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
    six = snapshot["protocol"] == 2
    demand = _row_vector(waiter, six=six)
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
        if six and node["labels"].get("kubernetes.io/arch") != "amd64":
            continue
        # Static capacity excludes occupancy, but includes explicit headroom.
        capacity = ResourceVector(
            *(
                max(0, a - b)
                for a, b in zip(
                    ResourceVector.from_six_dict(node["allocatable"]).components
                    if six else ResourceVector(**node["allocatable"]).components,
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


def _row_vector(row, *, six):
    value = {
        key: row[key] for key in ("cpu_millicores", "memory_bytes", "kvm_devices")
    }
    if six:
        value.update({
            key: row[key]
            for key in ("ephemeral_storage_bytes", "tun_devices", "vhost_net_devices")
        })
        return ResourceVector.from_six_dict(value)
    return ResourceVector(**value)


def _charge_vector(row):
    vector = _row_vector(row, six=row["resource_version"] == 2)
    if row["resource_version"] != 2 or row["observed_cpu_millicores"] is None:
        return vector
    return vector.maximum(ResourceVector(
        row["observed_cpu_millicores"], row["observed_memory_bytes"],
        row["observed_kvm_devices"], row["observed_ephemeral_storage_bytes"],
        row["observed_tun_devices"], row["observed_vhost_net_devices"],
    ))


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
            parsed = _policy_values(self.policy_document, inventory)
            if inventory.protocol == 2:
                (
                    self.cost, self.headroom, self.installation_budget,
                    self.owner_budget, self.max_bypasses, self.aging_seconds,
                    self.launcher_profile,
                ) = parsed
            else:
                self.cost, self.headroom, self.max_bypasses, self.aging_seconds = parsed
                self.installation_budget = self.owner_budget = self.launcher_profile = None
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

    async def grant_on_conn(self, conn, *, retry, job):
        """Recheck an already-held v2 grant after source locks, before issuance.

        This never allocates capacity. The caller must own the retry's exact
        Job/source/effect scope; policy serialization follows that scope.
        """
        await self._lock_policy(conn)
        head = await conn.fetchrow(
            "SELECT * FROM vm_resource_inventory_heads WHERE cluster_id=$1 "
            "AND policy_digest=$2 FOR UPDATE",
            self.inventory.cluster_id, self.inventory.policy_digest,
        )
        if head is None or head["current_snapshot_id"] is None or head["observation_conflict"]:
            raise ResourceAdmissionError("inventory_missing")
        observation = await conn.fetchrow(
            "SELECT * FROM vm_resource_inventory_snapshots WHERE snapshot_id=$1",
            head["current_snapshot_id"],
        )
        if observation is None:
            raise ResourceAdmissionError("inventory_missing")
        snapshot = self.inventory._snapshot(
            _json(observation["document"]), observation["digest"]
        )
        expected = _expected_waiter_request_fields(
            retry, inventory=self.inventory,
            policy_document=self.policy_document, cost=self.cost,
        )
        waiter = await conn.fetchrow(
            "SELECT * FROM vm_resource_waiters WHERE request_id=$1 FOR UPDATE",
            retry["request_id"],
        )
        if waiter is None or not _waiter_request_fields_match(waiter, expected):
            raise ResourceAdmissionError("resource_waiter_changed")
        owner_key = "system" if job["user_id"] is None else "user:" + str(job["user_id"])
        if waiter["owner_key"] != owner_key:
            raise ResourceAdmissionError("resource_owner_changed")
        held = await conn.fetchrow(
            "SELECT * FROM vm_resource_reservations WHERE request_id=$1 "
            "AND state<>'released' FOR UPDATE", retry["request_id"],
        )
        if held is None:
            raise ResourceAdmissionError("resource_reservation_missing")
        if (
            waiter["state"] != "admitted"
            or
            held["state"] != "reserved"
            or held["resource_version"] != 2
            or held["cluster_id"] != self.inventory.cluster_id
            or held["policy_digest"] != self.inventory.policy_digest
            or _row_vector(held, six=True) != _row_vector(expected, six=True)
        ):
            raise ResourceAdmissionError("resource_reservation_changed")
        node = await conn.fetchrow(
            "SELECT node_name FROM vm_resource_nodes WHERE cluster_id=$1 "
            "AND node_uid=$2", held["cluster_id"], held["node_uid"],
        )
        if node is None or node["node_name"] != held["node_name"]:
            raise ResourceAdmissionError("reservation_node_identity")
        now = await self._deadline(conn, retry)
        if not snapshot["complete"] or not snapshot_is_fresh(
            snapshot, received_at=observation["received_at"], now=now,
            stale_after_seconds=self.inventory.stale_after_seconds,
        ):
            raise ResourceAdmissionError("inventory_stale")
        grant = {
            "version": 1,
            "id": str(held["id"]),
            "revision": held["revision"],
            "cluster_id": held["cluster_id"],
            "policy_digest": held["policy_digest"],
            "node_uid": str(held["node_uid"]),
            "node_name": held["node_name"],
            "vector": _row_vector(held, six=True).to_six_dict(),
            "headroom": self.headroom.to_six_dict(),
            "snapshot_id": str(held["snapshot_id"]),
            "snapshot_digest": held["snapshot_digest"],
        }
        from shared.vm_resource_effect_node import validate_resource_effect_node

        validate_resource_effect_node(
            snapshot, grant=grant,
            resource=retry["controller_configuration"]["resource_admission"],
            expected_pvc_uid=(
                str(retry["expected_pvc_uid"])
                if retry["expected_pvc_uid"] else None
            ),
        )
        return grant

    async def bind_ready_on_conn(self, conn, *, retry, vm, job_id, generation):
        """Bind one genuine signed-inventory launcher before Job Ready commits.

        The caller already owns the Job lock and its Ready identity CAS. A
        refusal can still commit observed high-water, so this returns False
        instead of raising when authentic demand exceeds the reservation.
        """
        await self._lock_policy(conn, allow_off=True)
        if retry["state"] != "succeeded":
            return False
        head = await conn.fetchrow(
            "SELECT * FROM vm_resource_inventory_heads WHERE cluster_id=$1 "
            "AND policy_digest=$2 FOR UPDATE",
            self.inventory.cluster_id, self.inventory.policy_digest,
        )
        if head is None or head["current_snapshot_id"] is None or head["observation_conflict"]:
            return False
        observation = await conn.fetchrow(
            "SELECT * FROM vm_resource_inventory_snapshots WHERE snapshot_id=$1",
            head["current_snapshot_id"],
        )
        if observation is None:
            return False
        snapshot = self.inventory._snapshot(
            _json(observation["document"]), observation["digest"]
        )
        waiter = await conn.fetchrow(
            "SELECT * FROM vm_resource_waiters WHERE request_id=$1 FOR UPDATE",
            retry["request_id"],
        )
        reservation = await conn.fetchrow(
            "SELECT * FROM vm_resource_reservations WHERE request_id=$1 "
            "AND state<>'released' FOR UPDATE", retry["request_id"],
        )
        if waiter is None or reservation is None or (
            waiter["state"] != "admitted"
            or reservation["resource_version"] != 2
            or reservation["state"] not in {"reserved", "active", "warm"}
        ):
            return False
        expected = _expected_waiter_request_fields(
            retry, inventory=self.inventory,
            policy_document=self.policy_document, cost=self.cost,
        )
        if not _waiter_request_fields_match(waiter, expected) or (
            _row_vector(reservation, six=True) != _row_vector(expected, six=True)
        ):
            return False
        successor = await conn.fetchrow(
            "SELECT successor_vmi_uid,successor_launcher_uid "
            "FROM vm_resource_recovery_successors WHERE reservation_id=$1 "
            "ORDER BY ordinal DESC LIMIT 1", reservation["id"],
        )
        current_vmi_uid = (
            successor["successor_vmi_uid"] if successor else reservation["vmi_uid"]
        )
        current_launcher_uid = (
            successor["successor_launcher_uid"] if successor
            else reservation["launcher_uid"]
        )
        now = await conn.fetchval("SELECT clock_timestamp()")
        if not snapshot["complete"] or not snapshot_is_fresh(
            snapshot, received_at=observation["received_at"], now=now,
            stale_after_seconds=self.inventory.stale_after_seconds,
        ):
            return False
        vm_uid = vm.get("vm_uid")
        vmi_uid = vm.get("vmi_uid")
        launcher_uid = vm.get("active_pod_uid")
        pvc_uid = vm.get("rootdisk_pvc_uid")
        if not all(isinstance(value, str) for value in (
            vm_uid, vmi_uid, launcher_uid, pvc_uid,
        )) or (
            vm_uid != str(retry["observed_vm_uid"])
            or pvc_uid != str(retry["observed_pvc_uid"] or retry["expected_pvc_uid"])
        ):
            return False
        matches = tuple(
            next((item for item in snapshot[key] if item["uid"] == uid), None)
            for key, uid in (
                ("vms", vm_uid), ("vmis", vmi_uid), ("pods", launcher_uid),
            )
        )
        actual_vm, actual_vmi, pod = matches
        node_uid = str(reservation["node_uid"])
        if (
            actual_vm is None or actual_vmi is None or pod is None
            or actual_vm["owner_kind"] != "job"
            or actual_vm["owner_id"] != job_id
            or actual_vm["provision_generation"] != generation
            or actual_vm["name"] != "agent-vm-" + job_id
            or actual_vm["deleting"]
            or actual_vmi["vm_uid"] != vm_uid
            or actual_vmi["node_uid"] != node_uid
            or actual_vmi["node_name"] != reservation["node_name"]
            or actual_vmi["deleting"]
            or pod["terminal"] or pod["deleting"]
            or pod["vmi_uid"] != vmi_uid
            or pod["reservation_id"] != str(reservation["id"])
            or pod["provision_generation"] != generation
            or pod["node_uid"] != node_uid
            or pod["node_name"] != reservation["node_name"]
        ):
            return False
        if (
            reservation["vm_uid"] is not None
            and str(reservation["vm_uid"]) != vm_uid
            or current_vmi_uid is not None and str(current_vmi_uid) != vmi_uid
            or current_launcher_uid is not None
            and str(current_launcher_uid) != launcher_uid
        ):
            return False
        demand = ResourceVector.from_six_dict(pod["requests"])
        high = (
            ResourceVector(
                reservation["observed_cpu_millicores"],
                reservation["observed_memory_bytes"],
                reservation["observed_kvm_devices"],
                reservation["observed_ephemeral_storage_bytes"],
                reservation["observed_tun_devices"],
                reservation["observed_vhost_net_devices"],
            ) if reservation["observed_cpu_millicores"] is not None
            else ResourceVector(0, 0, 0)
        ).maximum(demand)
        fits = high.fits(_row_vector(reservation, six=True))
        await conn.execute(
            "UPDATE vm_resource_reservations SET "
            "vm_uid=COALESCE(vm_uid,$2),vmi_uid=COALESCE(vmi_uid,$3),"
            "launcher_uid=COALESCE(launcher_uid,$4),"
            "observed_cpu_millicores=$5,observed_memory_bytes=$6,"
            "observed_kvm_devices=$7,observed_ephemeral_storage_bytes=$8,"
            "observed_tun_devices=$9,observed_vhost_net_devices=$10,"
            "state=CASE WHEN $11 THEN 'active' ELSE state END "
            "WHERE id=$1",
            reservation["id"], UUID(vm_uid), UUID(vmi_uid), UUID(launcher_uid),
            high.cpu_millicores, high.memory_bytes, high.kvm_devices,
            high.ephemeral_storage_bytes, high.tun_devices,
            high.vhost_net_devices, fits,
        )
        return fits

    async def release_never_issued_on_conn(
        self, conn, *, retry, disposition_complete=False,
    ):
        """Release a held generation only after its source proves no effect began.

        The caller owns the normal creation source locks and commits the retry
        settlement in this transaction. Reconciliation remains valid in drain
        or off mode; changing the flag cannot abandon an existing charge.
        """
        if retry["state"] != "cancel_requested" or retry["observed_vm_uid"]:
            raise ResourceAdmissionError("creation_effect_unresolved")
        effects_present = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM vm_creation_effects WHERE request_id=$1)",
            retry["request_id"],
        )
        if effects_present:
            if not disposition_complete or not await conn.fetchval(
                "SELECT public.valid_vm_creation_disposition_evidence(r) "
                "FROM vm_creation_retries r WHERE request_id=$1",
                retry["request_id"],
            ) or await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM vm_creation_effects WHERE request_id=$1 "
                "AND effect_kind='vm' AND state<>'rejected')",
                retry["request_id"],
            ):
                raise ResourceAdmissionError("creation_effect_unresolved")
            permit = await conn.fetchrow(
                "SELECT completed_at,outcome FROM vm_workspace_cleanup_admissions "
                "WHERE id=$1", retry["creation_admission_id"],
            )
            if permit is None or permit["completed_at"] is None or (
                permit["outcome"] != "creation_disposed"
            ):
                raise ResourceAdmissionError("creation_disposition_unproven")
        await self._lock_policy(conn, allow_off=True)
        waiter = await conn.fetchrow(
            "SELECT * FROM vm_resource_waiters WHERE request_id=$1 FOR UPDATE",
            retry["request_id"],
        )
        reservation = await conn.fetchrow(
            "SELECT * FROM vm_resource_reservations WHERE request_id=$1 "
            "AND state<>'released' FOR UPDATE", retry["request_id"],
        )
        if waiter is None or waiter["cluster_id"] != self.inventory.cluster_id:
            raise ResourceAdmissionError("resource_waiter_changed")
        expected = _expected_waiter_request_fields(
            retry, inventory=self.inventory,
            policy_document=self.policy_document, cost=self.cost,
        )
        if not _waiter_request_fields_match(waiter, expected):
            raise ResourceAdmissionError("resource_waiter_changed")
        if reservation is None:
            return False
        if (
            reservation["state"] != "reserved"
            or reservation["vm_uid"] is not None
            or reservation["vmi_uid"] is not None
            or reservation["launcher_uid"] is not None
            or reservation["resource_version"] != 2
            or reservation["cluster_id"] != self.inventory.cluster_id
            or reservation["policy_digest"] != self.inventory.policy_digest
            or _row_vector(reservation, six=True) != _row_vector(expected, six=True)
            or waiter["state"] != "admitted"
        ):
            raise ResourceAdmissionError("reservation_release_unproven")
        evidence = {
            "kind": "never_vm_issued",
            "request_id": str(retry["request_id"]),
            "job_id": str(retry["job_id"]),
            "provision_generation": str(retry["provision_generation"]),
        }
        if effects_present:
            disposition = _json(retry["cancellation_disposition"])
            if not isinstance(disposition, dict) or not isinstance(
                disposition.get("disposition_id"), str
            ):
                raise ResourceAdmissionError("creation_disposition_unproven")
            evidence.update(
                disposition_id=disposition["disposition_id"],
                creation_admission_id=str(retry["creation_admission_id"]),
            )
        await conn.execute(
            "UPDATE vm_resource_reservations SET state='released',"
            "released_at=clock_timestamp(),release_evidence=$2::jsonb WHERE id=$1",
            reservation["id"], json.dumps(evidence),
        )
        await conn.execute(
            "UPDATE vm_resource_waiters SET state='released',revision=revision+1 "
            "WHERE request_id=$1", retry["request_id"],
        )
        return True

    async def append_recovery_successor_on_conn(
        self, conn, *, retry, operation, final_observation,
    ):
        """Bind the final authenticated successor to this one charged lineage.

        Called after recovery's final resolved CAS and owner projection, but
        before its transaction commits. The native append-only guard checks the
        operation, exact stop receipt and previous successor chain again.
        """
        await self._lock_policy(conn, allow_off=True)
        reservation = await conn.fetchrow(
            "SELECT * FROM vm_resource_reservations WHERE request_id=$1 "
            "AND state<>'released' FOR UPDATE", retry["request_id"],
        )
        if reservation is None or (
            reservation["resource_version"] != 2
            or reservation["state"] not in {"active", "warm"}
            or retry["state"] != "succeeded"
            or retry["job_id"] != operation["owner_id"]
            or retry["provision_generation"] != operation["provision_generation"]
            or retry["observed_vm_uid"] != operation["vm_uid"]
            or retry["observed_pvc_uid"] != operation["root_pvc_uid"]
            or reservation["vm_uid"] != operation["vm_uid"]
        ):
            raise ResourceAdmissionError("resource_successor_unproven")
        prior = await conn.fetchrow(
            "SELECT ordinal,successor_vmi_uid,successor_launcher_uid "
            "FROM vm_resource_recovery_successors WHERE reservation_id=$1 "
            "ORDER BY ordinal DESC LIMIT 1", reservation["id"],
        )
        vmi = prior["successor_vmi_uid"] if prior else reservation["vmi_uid"]
        launcher = (
            prior["successor_launcher_uid"] if prior else reservation["launcher_uid"]
        )
        successor = final_observation.get("successor")
        if not isinstance(successor, dict) or (
            vmi is None or launcher is None
            or vmi != operation["prior_vmi_uid"]
            or launcher != operation["prior_launcher_uid"]
        ):
            raise ResourceAdmissionError("resource_successor_predecessor_changed")
        try:
            next_vmi = UUID(successor["vmi_uid"])
            next_launcher = UUID(successor["launcher_uid"])
        except (ValueError, TypeError, KeyError, AttributeError):
            raise ResourceAdmissionError("resource_successor_unproven") from None
        if next_vmi == vmi and next_launcher == launcher:
            return False
        if next_vmi == vmi or next_launcher == launcher:
            raise ResourceAdmissionError("resource_successor_unproven")
        stop_digest = final_observation.get("stop_receipt_digest")
        if not isinstance(stop_digest, str):
            raise ResourceAdmissionError("resource_successor_unproven")
        digest = "sha256:" + hashlib.sha256(_encoded(final_observation)).hexdigest()
        await conn.execute(
            "INSERT INTO vm_resource_recovery_successors "
            "(recovery_id,reservation_id,ordinal,owner_id,provision_generation,"
            "vm_uid,root_pvc_uid,prior_vmi_uid,prior_launcher_uid,"
            "successor_vmi_uid,successor_launcher_uid,stop_receipt_digest,"
            "final_attestation_digest) "
            "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
            operation["id"], reservation["id"],
            prior["ordinal"] + 1 if prior else 1,
            operation["owner_id"], operation["provision_generation"],
            operation["vm_uid"], operation["root_pvc_uid"],
            vmi, launcher, next_vmi, next_launcher, stop_digest, digest,
        )
        return True

    async def _idle_charge_on_conn(self, conn, *, retry, operation):
        # C owns the operation lock before calling this helper. Re-read that
        # durable row ahead of the policy lock; a caller-supplied identity is
        # never itself cleanup authority, and this preserves source->policy.
        if not isinstance(operation.get("id"), UUID):
            raise ResourceAdmissionError("resource_idle_operation_changed")
        current = await conn.fetchrow(
            "SELECT * FROM vm_idle_operations WHERE id=$1 FOR UPDATE",
            operation["id"],
        )
        if (
            current is None or current["closed_at"] is not None
            or current["phase"] not in {"releasing", "release_held", "suspended"}
            or any(current[key] != operation.get(key) for key in (
                "owner_kind", "owner_id", "phase", "provision_generation",
                "vm_uid", "vmi_uid", "launcher_uid", "pvc_uid",
                "stop_evidence", "stop_verified_at",
            ))
        ):
            raise ResourceAdmissionError("resource_idle_operation_changed")
        await self._lock_policy(conn, allow_off=True)
        charge = await conn.fetchrow(
            "SELECT * FROM vm_resource_reservations WHERE request_id=$1 "
            "AND state<>'released' FOR UPDATE", retry["request_id"],
        )
        if charge is None or (
            charge["resource_version"] != 2
            or retry["state"] != "succeeded"
            or retry["job_id"] != operation["owner_id"]
            or retry["provision_generation"] != operation["provision_generation"]
            or retry["observed_vm_uid"] != operation["vm_uid"]
            or retry["observed_pvc_uid"] != operation["pvc_uid"]
            or charge["vm_uid"] != operation["vm_uid"]
            or operation["owner_kind"] != "job"
        ):
            raise ResourceAdmissionError("resource_idle_identity_unproven")
        successor = await conn.fetchrow(
            "SELECT successor_vmi_uid,successor_launcher_uid "
            "FROM vm_resource_recovery_successors WHERE reservation_id=$1 "
            "ORDER BY ordinal DESC LIMIT 1", charge["id"],
        )
        current_vmi = (
            successor["successor_vmi_uid"] if successor else charge["vmi_uid"]
        )
        current_launcher = (
            successor["successor_launcher_uid"] if successor
            else charge["launcher_uid"]
        )
        if (
            current_vmi is None or current_launcher is None
            or current_vmi != operation["vmi_uid"]
            or current_launcher != operation["launcher_uid"]
        ):
            raise ResourceAdmissionError("resource_idle_identity_unproven")
        return charge

    async def mark_idle_teardown_on_conn(self, conn, *, retry, operation):
        """Keep the charge while one exact idle release owns cleanup."""
        charge = await self._idle_charge_on_conn(
            conn, retry=retry, operation=operation,
        )
        if operation["phase"] not in {"releasing", "release_held"}:
            raise ResourceAdmissionError("resource_idle_release_unproven")
        if charge["state"] == "teardown":
            return False
        if charge["state"] not in {"active", "warm"}:
            raise ResourceAdmissionError("resource_idle_release_unproven")
        await conn.execute(
            "UPDATE vm_resource_reservations SET state='teardown' WHERE id=$1",
            charge["id"],
        )
        return True

    async def release_idle_compute_on_conn(self, conn, *, retry, operation):
        """Debit only after C commits authenticated physical absence on this conn."""
        charge = await self._idle_charge_on_conn(
            conn, retry=retry, operation=operation,
        )
        evidence = _json(operation["stop_evidence"])
        if (
            charge["state"] != "teardown"
            or operation["phase"] != "suspended"
            or operation["stop_verified_at"] is None
            or not isinstance(evidence, dict)
            or evidence.get("version") != 1
            or evidence.get("kind") != "vm_idle_physical_stop"
            or any(evidence.get(key) != str(operation[source]) for key, source in (
                ("operation_id", "id"), ("generation", "provision_generation"),
                ("vm_uid", "vm_uid"), ("vmi_uid", "vmi_uid"),
                ("launcher_uid", "launcher_uid"), ("pvc_uid", "pvc_uid"),
            ))
            or any(evidence.get(key) is not True for key in (
                "vm_absent", "vmi_absent", "launcher_absent", "retained_pvc",
                "controller_authenticated",
            ))
            or evidence.get("same_generation_replacement") is not False
        ):
            raise ResourceAdmissionError("resource_physical_release_unproven")
        if not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM managed_repository_process_zero_receipts "
            "WHERE owner_kind='job' AND owner_id=$1 AND scope='vm' "
            "AND provisioner='vm' AND runtime_incarnation=$2)",
            operation["owner_id"], str(operation["provision_generation"]),
        ):
            raise ResourceAdmissionError("resource_physical_release_unproven")
        digest = await conn.fetchval(
            "SELECT 'sha256:'||encode(sha256(convert_to(stop_evidence::text,'UTF8')),'hex') "
            "FROM vm_idle_operations WHERE id=$1", operation["id"],
        )
        release = {
            "kind": "exact_compute_absent",
            "operation_id": str(operation["id"]),
            "job_id": str(operation["owner_id"]),
            "provision_generation": str(operation["provision_generation"]),
            "vm_uid": str(operation["vm_uid"]),
            "vmi_uid": str(operation["vmi_uid"]),
            "launcher_uid": str(operation["launcher_uid"]),
            "pvc_uid": str(operation["pvc_uid"]),
            "stop_evidence_digest": digest,
        }
        await conn.execute(
            "UPDATE vm_resource_reservations SET state='released',"
            "released_at=clock_timestamp(),release_evidence=$2::jsonb WHERE id=$1",
            charge["id"], json.dumps(release),
        )
        await conn.execute(
            "UPDATE vm_resource_waiters SET state='released',revision=revision+1 "
            "WHERE request_id=$1 AND state='admitted'", retry["request_id"],
        )
        return True

    async def _cleanup_charge_on_conn(self, conn, *, retry, job, cleanup, intent):
        """Recheck an ordinary cleanup against the charged current incarnation."""
        if (
            cleanup is None or cleanup["owner_kind"] != "job"
            or cleanup["owner_id"] != job["id"]
            or cleanup["id"] != UUID(str(intent["admission_id"]))
            or cleanup["request_id"] != UUID(str(intent["request_id"]))
            or cleanup["intent_digest"] != intent["intent_digest"]
            or retry["job_id"] != job["id"]
            or retry["state"] != "succeeded"
            or str(retry["provision_generation"])
            != intent["intent"]["provision_generation"]
            or str(retry["observed_vm_uid"]) != intent["intent"]["vm_uid"]
            or str(retry["observed_pvc_uid"]) != intent["intent"]["pvc_uid"]
            or cleanup["pvc_uid"] != retry["observed_pvc_uid"]
            or cleanup["source"] != intent["intent"]["source"]
            or cleanup["source"] == "vm_idle_release"
        ):
            raise ResourceAdmissionError("resource_cleanup_identity_unproven")
        context = _json(job["context"])
        vm = context.get("vm") if isinstance(context, dict) else None
        if not isinstance(vm, dict) or any(
            vm.get(key) != value for key, value in (
                ("provision_generation", str(retry["provision_generation"])),
                ("vm_uid", str(retry["observed_vm_uid"])),
                ("rootdisk_pvc_uid", str(retry["observed_pvc_uid"])),
            )
        ):
            raise ResourceAdmissionError("resource_cleanup_identity_unproven")
        await self._lock_policy(conn, allow_off=True)
        charge = await conn.fetchrow(
            "SELECT * FROM vm_resource_reservations WHERE request_id=$1 "
            "AND state<>'released' FOR UPDATE", retry["request_id"],
        )
        if charge is None or (
            charge["resource_version"] != 2
            or charge["vm_uid"] != retry["observed_vm_uid"]
            or charge["state"] not in {"active", "warm", "teardown"}
        ):
            raise ResourceAdmissionError("resource_cleanup_charge_unproven")
        successor = await conn.fetchrow(
            "SELECT successor_vmi_uid,successor_launcher_uid "
            "FROM vm_resource_recovery_successors WHERE reservation_id=$1 "
            "ORDER BY ordinal DESC LIMIT 1", charge["id"],
        )
        vmi = successor["successor_vmi_uid"] if successor else charge["vmi_uid"]
        launcher = (
            successor["successor_launcher_uid"] if successor else charge["launcher_uid"]
        )
        if (
            vmi is None or launcher is None
            or vm.get("vmi_uid") != str(vmi)
            or vm.get("active_pod_uid") != str(launcher)
        ):
            raise ResourceAdmissionError("resource_cleanup_successor_changed")
        return charge, vmi, launcher

    async def mark_cleanup_teardown_on_conn(
        self, conn, *, retry, job, cleanup, intent,
    ):
        charge, vmi, launcher = await self._cleanup_charge_on_conn(
            conn, retry=retry, job=job, cleanup=cleanup, intent=intent,
        )
        if cleanup["completed_at"] is not None and cleanup["outcome"] != "completed":
            raise ResourceAdmissionError("resource_cleanup_outcome_changed")
        if charge["state"] in {"active", "warm"}:
            await conn.execute(
                "UPDATE vm_resource_reservations SET state='teardown' WHERE id=$1",
                charge["id"],
            )
        return {
            "job_id": str(job["id"]),
            "provision_generation": str(retry["provision_generation"]),
            "vm_uid": str(charge["vm_uid"]),
            "vmi_uid": str(vmi),
            "launcher_uid": str(launcher),
            "pvc_uid": str(retry["observed_pvc_uid"]),
            "purge_disk": intent["intent"]["purge_disk"],
        }

    async def release_cleanup_compute_on_conn(
        self, conn, *, retry, job, cleanup, intent, proof,
    ):
        charge, vmi, launcher = await self._cleanup_charge_on_conn(
            conn, retry=retry, job=job, cleanup=cleanup, intent=intent,
        )
        expected = {
            "version": 1, "kind": "vm_cleanup_physical_stop",
            "job_id": str(job["id"]),
            "provision_generation": str(retry["provision_generation"]),
            "vm_uid": str(charge["vm_uid"]),
            "vmi_uid": str(vmi), "launcher_uid": str(launcher),
            "pvc_uid": str(retry["observed_pvc_uid"]),
            "vm_absent": True, "vmi_absent": True,
            "launcher_absent": True,
            "same_generation_replacement": False,
            "pvc_disposition": (
                "purged" if intent["intent"]["purge_disk"] else "retained"
            ),
            "controller_authenticated": True,
        }
        if (
            charge["state"] != "teardown"
            or proof != expected
            or cleanup["completed_at"] is not None
            and cleanup["outcome"] != "completed"
            or not await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM managed_repository_process_zero_receipts "
                "WHERE owner_kind='job' AND owner_id=$1 AND scope='vm' "
                "AND provisioner='vm' AND runtime_incarnation=$2)",
                job["id"], str(retry["provision_generation"]),
            )
        ):
            raise ResourceAdmissionError("resource_cleanup_stop_unproven")
        if cleanup["completed_at"] is None:
            await conn.execute(
                "UPDATE vm_workspace_cleanup_admissions SET "
                "completed_at=clock_timestamp(),outcome='completed' WHERE id=$1",
                cleanup["id"],
            )
        await conn.execute(
            "INSERT INTO vm_resource_cleanup_stop_receipts "
            "(cleanup_admission_id,reservation_id,request_id,job_id,"
            "provision_generation,vm_uid,vmi_uid,launcher_uid,pvc_uid,"
            "intent_digest,stop_evidence) "
            "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb)",
            cleanup["id"], charge["id"], retry["request_id"], job["id"],
            retry["provision_generation"], charge["vm_uid"], vmi, launcher,
            retry["observed_pvc_uid"], cleanup["intent_digest"], json.dumps(proof),
        )
        digest = await conn.fetchval(
            "SELECT 'sha256:'||encode(sha256(convert_to(stop_evidence::text,'UTF8')),'hex') "
            "FROM vm_resource_cleanup_stop_receipts WHERE cleanup_admission_id=$1",
            cleanup["id"],
        )
        evidence = {
            "kind": "exact_cleanup_compute_absent",
            "cleanup_admission_id": str(cleanup["id"]),
            "job_id": str(job["id"]),
            "provision_generation": str(retry["provision_generation"]),
            "vm_uid": str(charge["vm_uid"]),
            "vmi_uid": str(vmi), "launcher_uid": str(launcher),
            "pvc_uid": str(retry["observed_pvc_uid"]),
            "stop_evidence_digest": digest,
        }
        await conn.execute(
            "UPDATE vm_resource_reservations SET state='released',"
            "released_at=clock_timestamp(),release_evidence=$2::jsonb WHERE id=$1",
            charge["id"], json.dumps(evidence),
        )
        await conn.execute(
            "UPDATE vm_resource_waiters SET state='released',revision=revision+1 "
            "WHERE request_id=$1 AND state='admitted'", retry["request_id"],
        )
        return True

    async def _lock_policy(self, conn, *, allow_drain=False, allow_off=False):
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
            not in (
                {"enforce", "drain", "off"} if allow_off else
                {"enforce", "drain"} if allow_drain else {"enforce"}
            )
        ):
            raise ResourceAdmissionError("resource_policy_changed")
        return policy

    async def _write_waiter_on_conn(self, conn, *, retry, job, create):
        """Project one waiter inside the retry admission transaction."""
        if (
            type(create) is not bool
            or retry["job_id"] != job["id"]
            or retry["state"] not in {"queued", "reconciling"}
        ):
            raise ResourceAdmissionError("creation_request_ineligible")
        await self.creation._current(
            conn, job, retry["provision_generation"], retry=retry
        )
        effects = await conn.fetch(
            "SELECT effect_number FROM vm_creation_effects WHERE request_id=$1 ORDER BY effect_number FOR UPDATE",
            retry["request_id"],
        )
        if effects:
            raise ResourceAdmissionError("creation_effect_already_issued")
        await self._lock_policy(conn)
        expected = _expected_waiter_request_fields(
            retry,
            inventory=self.inventory,
            policy_document=self.policy_document,
            cost=self.cost,
        )
        row = await conn.fetchrow(
            "SELECT * FROM vm_resource_waiters WHERE request_id=$1 FOR UPDATE",
            retry["request_id"],
        )
        if create:
            if row is not None:
                raise ResourceAdmissionError("resource_waiter_changed")
            owner_key = (
                "system" if job["user_id"] is None else "user:" + str(job["user_id"])
            )
            values = (
                expected["request_id"], expected["job_id"],
                expected["provision_generation"], expected["cluster_id"],
                expected["policy_digest"], owner_key, job["project_id"],
                job["priority"], expected["request_digest"],
                expected["guest_vcpus"], expected["guest_memory_bytes"],
                expected["cpu_millicores"], expected["memory_bytes"],
                expected["kvm_devices"], json.dumps(expected["placement"]),
            )
            if self.inventory.protocol == 2:
                row = await conn.fetchrow(
                    "INSERT INTO vm_resource_waiters(request_id,job_id,provision_generation,cluster_id,policy_digest,owner_key,project_id,priority,request_digest,guest_vcpus,guest_memory_bytes,cpu_millicores,memory_bytes,kvm_devices,placement,ephemeral_storage_bytes,tun_devices,vhost_net_devices,resource_version) "
                    "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb,$16,$17,$18,2) RETURNING *",
                    *values,
                    expected["ephemeral_storage_bytes"],
                    expected["tun_devices"],
                    expected["vhost_net_devices"],
                )
            else:
                row = await conn.fetchrow(
                    "INSERT INTO vm_resource_waiters(request_id,job_id,provision_generation,cluster_id,policy_digest,owner_key,project_id,priority,request_digest,guest_vcpus,guest_memory_bytes,cpu_millicores,memory_bytes,kvm_devices,placement) "
                    "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb) RETURNING *",
                    *values,
                )
        elif row is None:
            raise ResourceAdmissionError("resource_waiter_missing")
        if not _waiter_request_fields_match(row, expected):
            raise ResourceAdmissionError("resource_waiter_changed")
        await self._deadline(conn, retry)
        return dict(row)

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
        expected_waiter = _expected_waiter_request_fields(
            retry,
            inventory=inventory,
            policy_document=self.policy_document,
            cost=self.cost,
        )
        # All legitimate writers serialize on policy before taking these rows.
        # Read idempotence before inventory, but never before request authority.
        held = await conn.fetchrow(
            "SELECT * FROM vm_resource_reservations WHERE request_id=$1 AND state<>'released'",
            retry["request_id"],
        )
        if retry["state"] not in {"queued", "reconciling", "succeeded"}:
            raise ResourceAdmissionError("creation_request_ineligible")
        if held is not None:
            target = await conn.fetchrow(
                "SELECT * FROM vm_resource_waiters WHERE request_id=$1 FOR UPDATE",
                retry["request_id"],
            )
            if target is None:
                raise ResourceAdmissionError("resource_waiter_missing")
            if not _waiter_request_fields_match(target, expected_waiter):
                raise ResourceAdmissionError("resource_waiter_changed")
            if (
                held["cluster_id"] != inventory.cluster_id
                or held["policy_digest"] != inventory.policy_digest
            ):
                raise ResourceAdmissionError("resource_policy_changed")
            if (
                held["request_id"] != expected_waiter["request_id"]
                or (inventory.protocol == 2 and held["resource_version"] != 2)
                or _row_vector(held, six=inventory.protocol == 2)
                != _row_vector(expected_waiter, six=inventory.protocol == 2)
            ):
                raise ResourceAdmissionError("resource_waiter_changed")
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
            "SELECT r.*,w.job_id,w.provision_generation,w.owner_key,"
            "COALESCE(successor.successor_vmi_uid,r.vmi_uid) AS current_vmi_uid,"
            "COALESCE(successor.successor_launcher_uid,r.launcher_uid) AS current_launcher_uid "
            "FROM vm_resource_reservations r "
            "JOIN vm_resource_waiters w ON w.request_id=r.request_id "
            "LEFT JOIN LATERAL (SELECT successor_vmi_uid,successor_launcher_uid "
            "FROM vm_resource_recovery_successors WHERE reservation_id=r.id "
            "ORDER BY ordinal DESC LIMIT 1) successor ON true "
            "WHERE r.cluster_id=$1 AND r.state<>'released' "
            "ORDER BY r.id FOR UPDATE OF r",
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
        if inventory.protocol == 2:
            installed = snapshot["installed_profile"]
            if (
                installed["namespace"] != inventory.kubevirt_namespace
                or installed["name"] != inventory.kubevirt_name
                or _encoded(installed["profile"]) != _encoded(self.launcher_profile)
            ):
                raise ResourceAdmissionError("installed_launcher_profile_changed")
        target = next(
            (row for row in waiters if row["request_id"] == retry["request_id"]), None
        )
        if target is None:
            raise ResourceAdmissionError("resource_waiter_missing")
        if not _waiter_request_fields_match(target, expected_waiter):
            raise ResourceAdmissionError("resource_waiter_changed")
        owner_key = "system" if job["user_id"] is None else "user:" + str(job["user_id"])
        if inventory.protocol == 2 and target["owner_key"] != owner_key:
            raise ResourceAdmissionError("resource_waiter_changed")
        if inventory.protocol == 2 and any(
            row["resource_version"] != 2 for row in reservations
        ):
            raise ResourceAdmissionError("legacy_occupancy_unclassified")
        expected = _row_vector(expected_waiter, six=inventory.protocol == 2)
        charges = [
            ReservationCharge(
                reservation_id=str(row["id"]),
                node_uid=str(row["node_uid"]),
                node_name=row["node_name"],
                owner_kind="job",
                owner_id=str(row["job_id"]),
                provision_generation=str(row["provision_generation"]),
                vector=_row_vector(row, six=inventory.protocol == 2),
                state=row["state"],
                version=row["resource_version"] if inventory.protocol == 2 else 1,
                observed_high_water=(
                    ResourceVector(
                        row["observed_cpu_millicores"], row["observed_memory_bytes"],
                        row["observed_kvm_devices"], row["observed_ephemeral_storage_bytes"],
                        row["observed_tun_devices"], row["observed_vhost_net_devices"],
                    ) if inventory.protocol == 2 and row["observed_cpu_millicores"] is not None else None
                ),
                vm_uid=str(row["vm_uid"]) if row["vm_uid"] else None,
                vmi_uid=(
                    str(row["current_vmi_uid"]) if row["current_vmi_uid"] else None
                ),
                launcher_uid=(
                    str(row["current_launcher_uid"])
                    if row["current_launcher_uid"] else None
                ),
            )
            for row in reservations
        ]
        accounting = account_inventory(snapshot, charges, headroom=self.headroom)
        installation_held = ResourceVector(0, 0, 0)
        owner_held = {}
        if inventory.protocol == 2:
            for row in reservations:
                charge = _charge_vector(row)
                installation_held += charge
                owner_held[row["owner_key"]] = owner_held.get(
                    row["owner_key"], ResourceVector(0, 0, 0)
                ) + charge
        candidates, fits, nonfit = [], {}, set()
        for row in waiters:
            if (
                row["state"] == "nonfit"
                and row["evaluated_snapshot_id"] == observation["snapshot_id"]
            ):
                continue
            identity = str(row["request_id"])
            fit, impossible = _fit(snapshot, row, accounting, self.headroom)
            if inventory.protocol == 2:
                row_demand = _row_vector(row, six=True)
                if not (installation_held + row_demand).fits(self.installation_budget) or not (
                    owner_held.get(row["owner_key"], ResourceVector(0, 0, 0)) + row_demand
                ).fits(self.owner_budget):
                    fit = []
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
            if inventory.protocol == 2:
                if not (installation_held + expected).fits(self.installation_budget):
                    return {"action": "wait", "reason": "installation_budget"}
                if not (
                    owner_held.get(target["owner_key"], ResourceVector(0, 0, 0)) + expected
                ).fits(self.owner_budget):
                    return {"action": "wait", "reason": "owner_budget"}
            return {"action": "wait"}
        if inventory.protocol == 2:
            if not (installation_held + expected).fits(self.installation_budget):
                return {"action": "wait", "reason": "installation_budget"}
            if not (
                owner_held.get(target["owner_key"], ResourceVector(0, 0, 0)) + expected
            ).fits(self.owner_budget):
                return {"action": "wait", "reason": "owner_budget"}
        sequence = policy["admission_sequence"] + 1
        _positive(sequence)
        values = (
            uuid4(), retry["request_id"], inventory.cluster_id,
            inventory.policy_digest, UUID(choice.node_uid),
            node_names[choice.node_uid], expected.cpu_millicores,
            expected.memory_bytes, expected.kvm_devices,
            observation["snapshot_id"], observation["digest"],
        )
        if inventory.protocol == 2:
            reservation = await conn.fetchrow(
                "INSERT INTO vm_resource_reservations(id,request_id,revision,cluster_id,policy_digest,node_uid,node_name,cpu_millicores,memory_bytes,kvm_devices,snapshot_id,snapshot_digest,ephemeral_storage_bytes,tun_devices,vhost_net_devices,resource_version) "
                "VALUES($1,$2,(SELECT COALESCE(max(revision),0)+1 FROM vm_resource_reservations WHERE request_id=$2),$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,2) RETURNING *",
                *values, expected.ephemeral_storage_bytes,
                expected.tun_devices, expected.vhost_net_devices,
            )
        else:
            reservation = await conn.fetchrow(
                "INSERT INTO vm_resource_reservations(id,request_id,revision,cluster_id,policy_digest,node_uid,node_name,cpu_millicores,memory_bytes,kvm_devices,snapshot_id,snapshot_digest) "
                "VALUES($1,$2,(SELECT COALESCE(max(revision),0)+1 FROM vm_resource_reservations WHERE request_id=$2),$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *",
                *values,
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
