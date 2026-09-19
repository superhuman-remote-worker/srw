"""Phase evidence must never turn disk or placement delay into boot timeout."""

import copy
import math

import pytest

from shared.vm_provisioning_phases import (
    observe_provisioning,
    provisioning_decision,
)


def evidence(**changes):
    result = {
        "version": 1,
        "owner_kind": "job",
        "owner_id": "00000000-0000-4000-8000-000000000001",
        "namespace": "srw",
        "provision_generation": "00000000-0000-4000-8000-000000000002",
        "vm_uid": "00000000-0000-4000-8000-000000000003",
        "vmi_uid": None,
        "rootdisk_dv_name": "rootdisk",
        "rootdisk_dv_uid": "00000000-0000-4000-8000-000000000004",
        "rootdisk_pvc_name": "rootdisk",
        "rootdisk_pvc_uid": "00000000-0000-4000-8000-000000000005",
        "disk_mode": "clone",
        "disk_phase": "transferring",
        "disk_progress": 10.0,
        "vmi_phase": "absent",
    }
    result.update(changes)
    return result


def running(**changes):
    return evidence(
        **{
            "disk_phase": "ready",
            "disk_progress": 100.0,
            "vmi_phase": "running",
            "vmi_uid": "00000000-0000-4000-8000-000000000006",
            **changes,
        }
    )


def test_slow_but_progressing_clone_never_spends_boot_budget():
    state = observe_provisioning(None, evidence(), now=100)
    assert provisioning_decision(state, now=701).action == "wait"
    state = observe_provisioning(state, evidence(disk_progress=20), now=2700)
    assert state["first_guest_started_at"] is None
    assert provisioning_decision(state, now=5300).action == "wait"


def test_repeated_progress_and_phase_oscillation_do_not_refresh_stall_clock():
    state = observe_provisioning(None, evidence(), now=100)
    state = observe_provisioning(state, evidence(), now=1500)
    state = observe_provisioning(
        state, evidence(disk_phase="preparing", disk_progress=99), now=2000
    )
    state = observe_provisioning(state, evidence(), now=2500)
    decision = provisioning_decision(state, now=2801)
    assert (decision.action, decision.reason) == ("attention", "vm_rootdisk_stalled")
    assert state["last_real_progress_at"] == 100


def test_new_forward_stage_counts_once_as_progress():
    state = observe_provisioning(
        None, evidence(disk_phase="preparing", disk_progress=90), now=100
    )
    state = observe_provisioning(state, evidence(), now=2700)
    assert state["last_real_progress_at"] == 2700
    assert provisioning_decision(state, now=5300).action == "wait"


@pytest.mark.parametrize("vmi_phase", ["absent", "pending", "scheduling", "scheduled"])
def test_placement_wait_over_26_hours_never_starts_boot(vmi_phase):
    state = observe_provisioning(
        None,
        evidence(
            disk_phase="waiting_for_consumer",
            disk_progress=None,
            vmi_phase=vmi_phase,
            vmi_uid=None if vmi_phase == "absent" else running()["vmi_uid"],
        ),
        now=100,
    )
    assert state["phase"] == "placement"
    assert state["first_guest_started_at"] is None
    assert provisioning_decision(state, now=100000).action == "wait"


def test_placement_wait_does_not_spend_active_disk_stall_budget():
    state = observe_provisioning(None, evidence(), now=100)
    state = observe_provisioning(
        state, evidence(disk_phase="waiting_for_consumer"), now=200
    )
    state = observe_provisioning(state, evidence(), now=100000)
    assert provisioning_decision(state, now=102599).action == "wait"
    assert provisioning_decision(state, now=102601).reason == "vm_rootdisk_stalled"


def test_boot_start_is_once_and_survives_serialized_restart():
    state = observe_provisioning(None, evidence(disk_phase="ready"), now=100)
    state = observe_provisioning(state, running(), now=10000)
    state = observe_provisioning(copy.deepcopy(state), running(), now=10500)
    assert state["first_guest_started_at"] == 10000
    assert provisioning_decision(state, now=10600).action == "wait"
    assert provisioning_decision(state, now=10601).action == "boot_timeout"


def test_same_generation_vmi_restart_cannot_buy_new_boot_budget():
    state = observe_provisioning(None, running(), now=100)
    with pytest.raises(ValueError, match="identity"):
        observe_provisioning(
            state,
            running(vmi_uid="00000000-0000-4000-8000-000000000099"),
            now=600,
        )
    assert state["first_guest_started_at"] == 100


@pytest.mark.parametrize(
    "field,value",
    [
        ("owner_id", "00000000-0000-4000-8000-000000000099"),
        ("provision_generation", "00000000-0000-4000-8000-000000000099"),
        ("vm_uid", "00000000-0000-4000-8000-000000000099"),
        ("rootdisk_dv_uid", "00000000-0000-4000-8000-000000000099"),
        ("rootdisk_pvc_uid", "00000000-0000-4000-8000-000000000099"),
        ("rootdisk_pvc_uid", None),
        ("namespace", "other"),
        ("disk_mode", "retained"),
    ],
)
def test_identity_changes_are_refused_without_mutating_bound_state(field, value):
    state = observe_provisioning(None, evidence(), now=100)
    saved = copy.deepcopy(state)
    with pytest.raises(ValueError, match="identity"):
        observe_provisioning(state, evidence(**{field: value}), now=200)
    assert state == saved


def test_retained_disk_without_datavolume_can_boot():
    state = observe_provisioning(
        None,
        running(disk_mode="retained", rootdisk_dv_name=None, rootdisk_dv_uid=None),
        now=100,
    )
    assert provisioning_decision(state, now=701).action == "boot_timeout"


@pytest.mark.parametrize("phase", ["failed", "unknown"])
def test_unproven_or_failed_disk_never_authorizes_recycle(phase):
    state = observe_provisioning(None, evidence(disk_phase=phase), now=100)
    decision = provisioning_decision(state, now=100000)
    assert decision.action == "attention"


@pytest.mark.parametrize(
    "changes",
    [
        {"version": True},
        {"disk_progress": True},
        {"disk_progress": math.nan},
        {"disk_progress": -1},
        {"disk_progress": 101},
        {"vmi_phase": "running", "vmi_uid": None},
        {"rootdisk_pvc_uid": "not-a-uid"},
        {"disk_phase": "ready", "rootdisk_pvc_uid": None},
    ],
)
def test_malformed_evidence_is_refused(changes):
    with pytest.raises(ValueError):
        observe_provisioning(None, evidence(**changes), now=100)


def test_first_allocated_ids_bind_once():
    state = observe_provisioning(
        None, evidence(rootdisk_pvc_uid=None, rootdisk_dv_uid=None), now=100
    )
    state = observe_provisioning(state, evidence(), now=200)
    assert state["identity"]["rootdisk_pvc_uid"] == evidence()["rootdisk_pvc_uid"]


@pytest.mark.parametrize("state", [None, {}, {"version": 1}, {"version": 2}])
def test_unknown_stored_state_is_attention_not_boot_timeout(state):
    assert provisioning_decision(state, now=100000).action == "attention"


def test_future_or_corrupt_clock_cannot_authorize_cleanup():
    state = observe_provisioning(None, running(), now=100)
    for field, bad in [
        ("first_guest_started_at", 100001),
        ("first_guest_started_at", -1),
        ("first_guest_started_at", "1"),
        ("first_guest_started_at", True),
        ("first_guest_started_at", math.nan),
        ("observed_at", 100001),
    ]:
        invalid = {**state, field: bad}
        assert provisioning_decision(invalid, now=100000).action == "attention"


def test_phase_regression_after_boot_does_not_reset_clock_or_authorize_blind_delete():
    state = observe_provisioning(None, running(), now=100)
    state = observe_provisioning(
        state, running(vmi_phase="pending", disk_phase="ready"), now=500
    )
    assert state["first_guest_started_at"] == 100
    assert provisioning_decision(state, now=1000).action == "attention"


@pytest.mark.parametrize(
    "field", ["owner_kind", "disk_mode", "disk_phase", "vmi_phase"]
)
def test_structurally_invalid_observation_has_a_bounded_refusal(field):
    with pytest.raises(ValueError):
        observe_provisioning(None, evidence(**{field: []}), now=100)


def test_unbounded_number_and_unrecognized_attention_reason_are_not_exposed():
    with pytest.raises(ValueError):
        observe_provisioning(None, evidence(disk_progress=10**1000), now=100)
    state = observe_provisioning(None, evidence(disk_phase="unknown"), now=100)
    state["attention_reason"] = "https://internal-server/private-token"
    assert provisioning_decision(state, now=200).reason == "vm_phase_unproven"
