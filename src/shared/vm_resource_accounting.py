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
    active: ResourceVector
    warm: ResourceVector
    teardown: ResourceVector
    available: ResourceVector
    shortfall: ResourceVector
    blocked_reason: str | None

    @property
    def held(self):
        return self.unbound + self.active + self.warm + self.teardown


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
            key: ZERO for key in ("external", "unbound", "active", "warm", "teardown")
        }
        for uid in nodes
    }
    seen, launchers, excluded, blocked_names, orphaned = set(), set(), set(), set(), {}
    held_vms, held_vmis, held_generations = set(), set(), set()
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
        if reservation.vmi_uid is not None:
            held_vmis.add(reservation.vmi_uid)
        if reservation.launcher_uid is not None:
            launchers.add(reservation.launcher_uid)
        node = nodes.get(reservation.node_uid)
        if node is None:
            orphaned[reservation.reservation_id] = reservation.vector
            blocked_names.add(reservation.node_name)
            continue
        if node["name"] != reservation.node_name:
            raise ResourceAdmissionError("reservation_node_identity")
        pod = pods.get(reservation.launcher_uid)
        charge = reservation.vector
        if reservation.observed_high_water is not None:
            charge = charge.maximum(reservation.observed_high_water)
        if _exact_launcher(reservation, pod, vmis, vms):
            charge = charge.maximum(ResourceVector(**pod["requests"]))
            excluded.add(pod["uid"])
        category = "unbound" if reservation.state == "reserved" else reservation.state
        charges[reservation.node_uid][category] += charge
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
