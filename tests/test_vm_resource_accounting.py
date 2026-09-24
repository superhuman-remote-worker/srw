from copy import deepcopy
from dataclasses import replace
from uuid import uuid4

import pytest

from shared.vm_resource_admission import ResourceAdmissionError, ResourceVector
from shared.vm_resource_accounting import ReservationCharge, account_inventory
from tests.test_vm_resource_inventory_contract import snapshot


ZERO = ResourceVector(0, 0, 0)


def setup():
    value = snapshot()
    node = value["nodes"][0]
    reservation = ReservationCharge(
        reservation_id=str(uuid4()),
        node_uid=node["uid"],
        node_name=node["name"],
        owner_kind="job",
        owner_id=str(uuid4()),
        provision_generation=str(uuid4()),
        vector=ResourceVector(1000, 1024**3, 1),
        state="active",
        vm_uid=str(uuid4()),
        vmi_uid=str(uuid4()),
        launcher_uid=str(uuid4()),
    )
    value["vms"] = [
        {
            "uid": reservation.vm_uid,
            "name": "vm",
            "owner_kind": "job",
            "owner_id": reservation.owner_id,
            "provision_generation": reservation.provision_generation,
            "deleting": False,
        }
    ]
    value["vmis"] = [
        {
            "uid": reservation.vmi_uid,
            "name": "vmi",
            "vm_uid": reservation.vm_uid,
            "node_uid": node["uid"],
            "node_name": node["name"],
            "phase": "Running",
            "deleting": False,
        }
    ]
    value["pods"] = [
        {
            "uid": reservation.launcher_uid,
            "name": "launcher",
            "namespace": value["namespace"],
            "node_uid": node["uid"],
            "node_name": node["name"],
            "terminal": False,
            "deleting": False,
            "requests": ResourceVector(1200, 512 * 1024**2, 1).to_dict(),
            "vmi_uid": reservation.vmi_uid,
            "reservation_id": reservation.reservation_id,
            "provision_generation": reservation.provision_generation,
        }
    ]
    return value, reservation


def result(value, reservations, headroom=ZERO):
    return account_inventory(value, reservations, headroom=headroom)


def test_exact_bound_launcher_charged_once_at_component_max():
    value, reservation = setup()
    facts = result(value, [reservation])
    node = facts.nodes[reservation.node_uid]
    assert node.external == ZERO
    assert node.active == ResourceVector(1200, 1024**3, 1)
    assert node.available == ResourceVector(2800, 7 * 1024**3, 9)


@pytest.mark.parametrize("deleting", [False, True])
@pytest.mark.parametrize("launcher_present", [False, True])
def test_unlabelled_srw_vm_never_becomes_external_only_capacity(deleting, launcher_present):
    value, reservation = setup()
    value["protocol"] = 2
    value["vms"][0].update(name="agent-vm-" + str(uuid4()), owner_kind=None,
                           owner_id=None, provision_generation=None, deleting=deleting)
    value["pods"][0].update(reservation_id=None, provision_generation=None,
                            requests=ResourceVector(1200, 512 * 1024**2, 1, 50000000, 1, 1).to_six_dict())
    if not launcher_present:
        value["pods"] = []
    value["nodes"][0]["allocatable"].update(ephemeral_storage_bytes=1000000000,
                                           tun_devices=8, vhost_net_devices=8)
    with pytest.raises(ResourceAdmissionError, match="legacy_occupancy_unclassified"):
        result(value, [])


def test_protocol_two_external_launcher_keeps_node_charge_without_srw_budget_identity():
    value, reservation = setup()
    value["protocol"] = 2
    value["vms"][0].update(
        owner_kind=None, owner_id=None, provision_generation=None,
    )
    request = ResourceVector(1200, 512 * 1024**2, 1, 50000000, 1, 1)
    value["pods"][0]["requests"] = request.to_six_dict()
    value["pods"][0]["reservation_id"] = None
    value["pods"][0]["provision_generation"] = None
    value["nodes"][0]["allocatable"].update(
        ephemeral_storage_bytes=1000000000, tun_devices=8, vhost_net_devices=8,
    )
    node = result(value, []).nodes[reservation.node_uid]
    assert node.external == request
    assert node.held == ResourceVector(0, 0, 0)


def test_protocol_two_rejects_extra_live_launcher_for_attributable_vm():
    value, reservation = setup()
    value["protocol"] = 2
    value["pods"].append({**value["pods"][0], "uid": str(uuid4())})
    with pytest.raises(ResourceAdmissionError, match="legacy_occupancy_unclassified"):
        result(value, [replace(reservation, version=2)])


def test_protocol_two_retains_authenticated_high_water_across_missing_pod_and_phases():
    from shared.vm_launcher_profile import default_launcher_profile

    value, reservation = setup()
    value["protocol"] = 2
    value["label_keys"].append("kubernetes.io/arch")
    value["nodes"][0]["labels"]["kubernetes.io/arch"] = "amd64"
    value["nodes"][0]["allocatable"].update(
        ephemeral_storage_bytes=1000000000, tun_devices=8, vhost_net_devices=8,
    )
    value["resource_versions"].update(kubevirt="10", limitranges="10")
    value["installed_profile"] = {
        "uid": str(uuid4()), "namespace": "kubevirt", "name": "kubevirt",
        "generation": 1, "observedGeneration": 1, "version": "v1.6.6",
        "deploymentID": "settled", "profile": default_launcher_profile(),
    }
    value["pods"] = []
    reserved = ResourceVector(1000, 1024**3, 1, 50000000, 1, 1)
    high = ResourceVector(1200, 2 * 1024**3, 1, 70000000, 2, 1)
    for state in ("reserved", "active", "warm", "teardown"):
        held = replace(reservation, state=state, version=2, vector=reserved,
                       observed_high_water=high)
        node = result(value, [held]).nodes[reservation.node_uid]
        assert node.held == high
        assert node.available.ephemeral_storage_bytes == 930000000
    with pytest.raises(ResourceAdmissionError):
        result(value, [reservation])


@pytest.mark.parametrize(
    "field",
    [
        "launcher_uid",
        "vmi_uid",
        "vm_uid",
        "owner_id",
        "provision_generation",
        "reservation_id",
    ],
)
def test_copied_or_conflicting_identity_never_excludes_external_pod(field):
    value, reservation = setup()
    reservation = replace(reservation, **{field: str(uuid4())})
    node = result(value, [reservation]).nodes[reservation.node_uid]
    assert node.external == ResourceVector(**value["pods"][0]["requests"])
    assert node.active == reservation.vector


@pytest.mark.parametrize("state", ["reserved", "active", "warm", "teardown"])
def test_missing_or_terminal_launcher_retains_every_held_state(state):
    value, reservation = setup()
    reservation = replace(reservation, state=state)
    for pods in ([], [{**value["pods"][0], "terminal": True}]):
        value["pods"] = pods
        node = result(value, [reservation]).nodes[reservation.node_uid]
        assert node.held == reservation.vector
        assert node.external == ZERO


def test_unbound_reservation_and_untrusted_launcher_both_remain_charged():
    value, reservation = setup()
    reservation = replace(
        reservation, vm_uid=None, vmi_uid=None, launcher_uid=None, state="reserved"
    )
    node = result(value, [reservation]).nodes[reservation.node_uid]
    assert node.unbound == reservation.vector
    assert node.external == ResourceVector(**value["pods"][0]["requests"])


def test_reserved_with_observed_launcher_is_bound_even_before_ready():
    value, reservation = setup()
    reservation = replace(reservation, state="reserved")
    node = result(value, [reservation]).nodes[reservation.node_uid]
    assert node.unbound == ZERO
    assert node.bound_reserved == ResourceVector(1200, 1024**3, 1)
    assert node.external == ZERO
    assert node.held == node.bound_reserved
    assert node.available == ResourceVector(2800, 7 * 1024**3, 9)


def test_deleting_external_pod_counts_until_terminal_and_unscheduled_has_no_node_charge():
    value, reservation = setup()
    pod = value["pods"][0]
    pod.update(
        vmi_uid=None, reservation_id=None, provision_generation=None, deleting=True
    )
    assert result(value, []).nodes[reservation.node_uid].external == ResourceVector(
        **pod["requests"]
    )
    pod["terminal"] = True
    assert result(value, []).nodes[reservation.node_uid].external == ZERO
    pod.update(terminal=False, node_uid=None, node_name=None)
    facts = result(value, [])
    assert facts.nodes[reservation.node_uid].external == ZERO
    assert facts.pending_external == ResourceVector(**pod["requests"])


def test_node_replacement_holds_old_charge_and_blocks_reused_name():
    value, reservation = setup()
    new_uid = str(uuid4())
    value["nodes"][0]["uid"] = new_uid
    value["pods"] = []
    value["vmis"] = []
    value["vms"] = []
    facts = result(value, [reservation])
    assert facts.nodes[new_uid].blocked_reason == "prior_node_identity_held"
    assert facts.nodes[new_uid].available == ZERO
    assert facts.orphaned_held == {reservation.reservation_id: reservation.vector}


def test_new_same_name_node_is_available_only_after_caller_proves_release():
    value, reservation = setup()
    value["nodes"][0]["uid"] = str(uuid4())
    value["pods"] = value["vmis"] = value["vms"] = []
    facts = result(value, [replace(reservation, state="released")])
    node = next(iter(facts.nodes.values()))
    assert node.available == ResourceVector(**value["nodes"][0]["allocatable"])
    assert node.blocked_reason is None and facts.orphaned_held == {}


@pytest.mark.parametrize("state", ["reserved", "active", "warm", "teardown"])
@pytest.mark.parametrize("replacement", [False, True])
def test_missing_node_retains_observed_whole_launcher_high_water(state, replacement):
    value, reservation = setup()
    value["protocol"] = 2
    reservation = replace(
        reservation, version=2, state=state,
        vector=ResourceVector(1000, 2 * 1024**3, 2, 50000000, 1, 2),
        observed_high_water=ResourceVector(1500, 1024**3, 1, 70000000, 2, 1),
    )
    value["pods"] = value["vmis"] = value["vms"] = []
    if replacement:
        value["nodes"][0]["uid"] = str(uuid4())
    else:
        value["nodes"] = []
    facts = result(value, [reservation])
    assert facts.orphaned_held == {
        reservation.reservation_id: ResourceVector(
            1500, 2 * 1024**3, 2, 70000000, 2, 2,
        ),
    }
    if replacement:
        node = next(iter(facts.nodes.values()))
        assert node.blocked_reason == "prior_node_identity_held"
        assert node.available == ZERO
    else:
        assert facts.nodes == {}
    assert result(value, [replace(reservation, state="released")]).orphaned_held == {}


def test_extra_pod_with_copied_owner_annotations_still_counts_as_external():
    value, reservation = setup()
    copied = deepcopy(value["pods"][0])
    copied.update(uid=str(uuid4()), name="second-launcher")
    value["pods"].append(copied)
    node = result(value, [reservation]).nodes[reservation.node_uid]
    assert node.active == ResourceVector(1200, 1024**3, 1)
    assert node.external == ResourceVector(**copied["requests"])


def test_overcommitted_component_saturates_available_without_hiding_shortfall():
    value, reservation = setup()
    value["pods"][0]["requests"]["cpu_millicores"] = 6000
    node = result(value, [reservation], headroom=ResourceVector(100, 0, 0)).nodes[
        reservation.node_uid
    ]
    assert node.available.cpu_millicores == 0
    assert node.shortfall == ResourceVector(2100, 0, 0)
    assert node.active.cpu_millicores == 6000


def test_conflicting_reservations_cannot_claim_same_launcher():
    value, reservation = setup()
    with pytest.raises(ResourceAdmissionError, match="identity"):
        result(value, [reservation, replace(reservation, reservation_id=str(uuid4()))])


def test_incomplete_inventory_is_never_arithmetic_authority():
    value, reservation = setup()
    value["complete"] = False
    with pytest.raises(ResourceAdmissionError, match="incomplete"):
        result(value, [reservation])


def test_unapproved_node_move_charges_destination_externally_and_keeps_source_hold():
    value, reservation = setup()
    destination = deepcopy(value["nodes"][0])
    destination.update(uid=str(uuid4()), name="node-b")
    value["nodes"].append(destination)
    value["pods"][0].update(node_uid=destination["uid"], node_name=destination["name"])
    value["vmis"][0].update(node_uid=destination["uid"], node_name=destination["name"])
    facts = result(value, [reservation])
    assert facts.nodes[reservation.node_uid].held == reservation.vector
    assert facts.nodes[destination["uid"]].external == ResourceVector(
        **value["pods"][0]["requests"]
    )


def test_accounting_overflow_refuses_instead_of_dropping_a_charge():
    value, reservation = setup()
    value["nodes"][0]["allocatable"]["cpu_millicores"] = 2**63 - 1
    value["pods"][0]["requests"]["cpu_millicores"] = 2**63 - 1
    with pytest.raises(ResourceAdmissionError, match="integer"):
        result(value, [reservation], headroom=ResourceVector(1, 0, 0))


@pytest.mark.parametrize("shared", ["vm_uid", "vmi_uid", "owner_generation"])
def test_conflicting_durable_vm_or_owner_generation_ownership_refuses(shared):
    value, first = setup()
    second = replace(
        first,
        reservation_id=str(uuid4()),
        launcher_uid=str(uuid4()),
        vm_uid=str(uuid4()),
        vmi_uid=str(uuid4()),
        owner_id=str(uuid4()),
    )
    if shared == "owner_generation":
        second = replace(second, owner_id=first.owner_id)
    else:
        second = replace(second, **{shared: getattr(first, shared)})
    with pytest.raises(ResourceAdmissionError, match="identity_conflict"):
        result(value, [first, second])
    # Historical release is established by the caller's durable protocol.
    assert result(value, [first, replace(second, state="released")]).nodes[
        first.node_uid
    ].held == ResourceVector(1200, 1024**3, 1)
