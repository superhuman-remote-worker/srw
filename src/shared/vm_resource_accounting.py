"""Conservative accounting of validated inventory and trusted reservation rows.

The caller must validate snapshot scope/completeness/freshness under its admission
locks and obtain reservation identities from durable authenticated evidence.
This pure calculation neither binds a launcher nor releases a reservation.
"""

from dataclasses import dataclass
from uuid import UUID

from shared.vm_resource_admission import ResourceAdmissionError, ResourceVector


ZERO = ResourceVector(0, 0, 0)
_HELD = {"reserved", "active", "warm", "teardown"}


def reservation_category(state, *, vm_uid, vmi_uid, launcher_uid):
    """Separate durable observed placement from a successful Ready transition."""
    if state == "reserved":
        return "bound_reserved" if all((vm_uid, vmi_uid, launcher_uid)) else "unbound"
    return state


def _uuid(value, *, nullable=False):
    if value is None and nullable:
        return
    try:
        if not isinstance(value, str) or str(UUID(value)) != value:
            raise ValueError
    except ValueError:
        raise ResourceAdmissionError("reservation_identity") from None


@dataclass(frozen=True)
class ReservationCharge:
    reservation_id: str
    node_uid: str
    node_name: str
    owner_kind: str
    owner_id: str
    provision_generation: str
    vector: ResourceVector
    state: str
    vm_uid: str | None = None
    vmi_uid: str | None = None
    launcher_uid: str | None = None
    version: int = 1
    observed_high_water: ResourceVector | None = None

    def __post_init__(self):
        for key in ("reservation_id", "node_uid", "owner_id", "provision_generation"):
            _uuid(getattr(self, key))
        for key in ("vm_uid", "vmi_uid", "launcher_uid"):
            _uuid(getattr(self, key), nullable=True)
        if (
            self.owner_kind not in {"job", "thread"}
            or self.state not in _HELD | {"released"}
            or not isinstance(self.vector, ResourceVector)
            or type(self.version) is not int
            or self.version not in (1, 2)
            or (
                self.observed_high_water is not None
                and not isinstance(self.observed_high_water, ResourceVector)
            )
            or (self.version == 1 and self.observed_high_water is not None)
            or not isinstance(self.node_name, str)
            or not 1 <= len(self.node_name) <= 253
            or self.node_name != self.node_name.strip()
        ):
            raise ResourceAdmissionError("reservation_identity")


@dataclass(frozen=True)
class NodeAccounting:
    allocatable: ResourceVector
    headroom: ResourceVector
    external: ResourceVector
    unbound: ResourceVector
    bound_reserved: ResourceVector
    active: ResourceVector
    warm: ResourceVector
    teardown: ResourceVector
    available: ResourceVector
    shortfall: ResourceVector
    blocked_reason: str | None

    @property
    def held(self):
        return (
            self.unbound + self.bound_reserved + self.active + self.warm + self.teardown
        )


@dataclass(frozen=True)
class InventoryAccounting:
    nodes: dict[str, NodeAccounting]
    orphaned_held: dict[str, ResourceVector]
    pending_external: ResourceVector


def _exact_launcher(reservation, pod, vmis, vms):
    if pod is None or pod["terminal"]:
        return False
    vmi, vm = vmis.get(reservation.vmi_uid), vms.get(reservation.vm_uid)
    return bool(
        vmi
        and vm
        and pod["uid"] == reservation.launcher_uid
        and pod["vmi_uid"] == reservation.vmi_uid
        and pod["reservation_id"] == reservation.reservation_id
        and pod["provision_generation"] == reservation.provision_generation
        and pod["node_uid"] == reservation.node_uid
        and pod["node_name"] == reservation.node_name
        and vmi["vm_uid"] == reservation.vm_uid
        and vmi["node_uid"] == reservation.node_uid
        and vmi["node_name"] == reservation.node_name
        and vm["owner_kind"] == reservation.owner_kind
        and vm["owner_id"] == reservation.owner_id
        and vm["provision_generation"] == reservation.provision_generation
    )


def account_inventory(snapshot, reservations, *, headroom):
    """Do not interpret an absent/terminal Pod as permission to release demand."""
    if snapshot["complete"] is not True:
        raise ResourceAdmissionError("inventory_incomplete")
    if not isinstance(headroom, ResourceVector):
        raise ResourceAdmissionError("invalid_headroom")
    nodes = {node["uid"]: node for node in snapshot["nodes"]}
    pods = {pod["uid"]: pod for pod in snapshot["pods"]}
    vmis = {vmi["uid"]: vmi for vmi in snapshot["vmis"]}
    vms = {vm["uid"]: vm for vm in snapshot["vms"]}
    charges = {
        uid: {
            key: ZERO
            for key in (
                "external",
                "unbound",
                "bound_reserved",
                "active",
                "warm",
                "teardown",
            )
        }
        for uid in nodes
    }
    seen, launchers, excluded, blocked_names, orphaned = set(), set(), set(), set(), {}
    held_vms, held_vmis, held_generations = set(), set(), set()
    held_by_vm = {}
    for reservation in reservations:
        if not isinstance(reservation, ReservationCharge):
            raise ResourceAdmissionError("reservation_identity")
        if reservation.state == "released":
            continue
        if (snapshot["protocol"] == 2 and reservation.version != 2) or (
            snapshot["protocol"] == 1 and reservation.version != 1
        ):
            # A historical three-field charge cannot become zero ephemeral or
            # device demand by silently joining a six-field inventory.
            raise ResourceAdmissionError("legacy_occupancy_unclassified")
        generation_key = (
            reservation.owner_kind,
            reservation.owner_id,
            reservation.provision_generation,
        )
        if (
            reservation.reservation_id in seen
            or (
                reservation.launcher_uid is not None
                and reservation.launcher_uid in launchers
            )
            or (reservation.vm_uid is not None and reservation.vm_uid in held_vms)
            or (reservation.vmi_uid is not None and reservation.vmi_uid in held_vmis)
            or generation_key in held_generations
        ):
            raise ResourceAdmissionError("reservation_identity_conflict")
        seen.add(reservation.reservation_id)
        held_generations.add(generation_key)
        if reservation.vm_uid is not None:
            held_vms.add(reservation.vm_uid)
            held_by_vm[reservation.vm_uid] = reservation
        if reservation.vmi_uid is not None:
            held_vmis.add(reservation.vmi_uid)
        if reservation.launcher_uid is not None:
            launchers.add(reservation.launcher_uid)
        charge = reservation.vector
        if reservation.observed_high_water is not None:
            charge = charge.maximum(reservation.observed_high_water)
        node = nodes.get(reservation.node_uid)
        if node is None:
            orphaned[reservation.reservation_id] = charge
            blocked_names.add(reservation.node_name)
            continue
        if node["name"] != reservation.node_name:
            raise ResourceAdmissionError("reservation_node_identity")
        pod = pods.get(reservation.launcher_uid)
        if _exact_launcher(reservation, pod, vmis, vms):
            charge = charge.maximum(ResourceVector(**pod["requests"]))
            excluded.add(pod["uid"])
        category = reservation_category(
            reservation.state,
            vm_uid=reservation.vm_uid,
            vmi_uid=reservation.vmi_uid,
            launcher_uid=reservation.launcher_uid,
        )
        charges[reservation.node_uid][category] += charge
    if snapshot["protocol"] == 2:
        # A known SRW VM is installation/owner occupancy, even when its Pod
        # vanished. External Pod arithmetic alone cannot charge those budgets.
        attributable = {}
        attributable_by_name = {}
        for vm in snapshot["vms"]:
            if vm["owner_kind"] not in {"job", "thread"}:
                if vm["name"].startswith("agent-vm-") and not vm["name"].startswith(
                    "agent-vm-golden-"
                ):
                    # Legacy guests may predate owner labels. Neither a
                    # missing launcher nor a deletion timestamp proves that
                    # their logical or physical occupancy has ended.
                    raise ResourceAdmissionError("legacy_occupancy_unclassified")
                continue
            reservation = held_by_vm.get(vm["uid"])
            if (
                reservation is None
                or reservation.version != 2
                or reservation.owner_kind != vm["owner_kind"]
                or reservation.owner_id != vm["owner_id"]
                or reservation.provision_generation != vm["provision_generation"]
            ):
                raise ResourceAdmissionError("legacy_occupancy_unclassified")
            attributable[vm["uid"]] = reservation
            attributable_by_name[vm["name"]] = vm["uid"]
        attributable_vmis = {}
        for vmi in snapshot["vmis"]:
            if (
                vmi["name"] in attributable_by_name
                and vmi["vm_uid"] != attributable_by_name[vmi["name"]]
            ):
                raise ResourceAdmissionError("legacy_occupancy_unclassified")
            reservation = attributable.get(vmi["vm_uid"])
            if reservation is None:
                continue
            if reservation.vmi_uid != vmi["uid"] or (
                vmi["node_uid"] is not None
                and (
                    reservation.node_uid != vmi["node_uid"]
                    or reservation.node_name != vmi["node_name"]
                )
            ):
                raise ResourceAdmissionError("legacy_occupancy_unclassified")
            attributable_vmis[vmi["uid"]] = reservation
        for pod in snapshot["pods"]:
            if pod["terminal"]:
                continue
            reservation = attributable_vmis.get(pod["vmi_uid"])
            if reservation is not None:
                if _exact_launcher(reservation, pod, vmis, vms):
                    continue
                raise ResourceAdmissionError("legacy_occupancy_unclassified")
            if (
                pod["reservation_id"] is not None
                or pod["provision_generation"] is not None
            ):
                # These are SRW-specific launcher markers. A missing VMI/VM
                # owner link cannot turn marked occupancy into external-only
                # node charge while logical budgets are enforced.
                raise ResourceAdmissionError("legacy_occupancy_unclassified")
    pending = ZERO
    for pod in snapshot["pods"]:
        if pod["terminal"] or pod["uid"] in excluded:
            continue
        request = ResourceVector(**pod["requests"])
        if pod["node_uid"] is None:
            pending += request
        elif (
            pod["node_uid"] not in nodes
            or nodes[pod["node_uid"]]["name"] != pod["node_name"]
        ):
            raise ResourceAdmissionError("inventory_node_identity")
        else:
            charges[pod["node_uid"]]["external"] += request
    result = {}
    for uid, node in nodes.items():
        allocatable = ResourceVector(**node["allocatable"])
        occupied = headroom
        for vector in charges[uid].values():
            occupied += vector
        available = ResourceVector(
            *(
                max(0, a - b)
                for a, b in zip(allocatable.components, occupied.components)
            )
        )
        shortfall = ResourceVector(
            *(
                max(0, b - a)
                for a, b in zip(allocatable.components, occupied.components)
            )
        )
        reason = "prior_node_identity_held" if node["name"] in blocked_names else None
        result[uid] = NodeAccounting(
            allocatable=allocatable,
            headroom=headroom,
            **charges[uid],
            available=ZERO if reason else available,
            shortfall=shortfall,
            blocked_reason=reason,
        )
    return InventoryAccounting(
        nodes=result, orphaned_held=orphaned, pending_external=pending
    )
