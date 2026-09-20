"""A scheduling response is not an admitted VM generation."""

from uuid import uuid4

import pytest

from shared.vm_admission_accounting import admission_updates


GENERATION, VM = str(uuid4()), str(uuid4())


def context(**changes):
    return {
        "admission_accounting_version": 1,
        "provision_generation": GENERATION,
        "provision_admission": None,
        "provision_attempts": 2,
        "status": "starting",
        **changes,
    }


def test_first_exact_admission_counts_once_and_replay_after_ready_stays_zero():
    vm = context()
    vm.update(admission_updates(vm, generation=GENERATION, vm_uid=VM))
    assert vm["provision_attempts"] == 3
    assert admission_updates(vm, generation=GENERATION, vm_uid=VM) == {}
    marker = vm["provision_admission"]
    vm.update(status="ready", provision_attempts=0)
    assert admission_updates(vm, generation=GENERATION, vm_uid=VM) == {}
    assert vm["provision_admission"] == marker


def test_first_observation_after_verified_ready_is_not_a_failed_attempt():
    updates = admission_updates(
        context(status="ready"), generation=GENERATION, vm_uid=VM
    )
    assert updates["provision_attempts"] == 0
    assert updates["provision_admission"] == {
        "provision_generation": GENERATION,
        "vm_uid": VM,
    }


def test_historical_generation_is_not_reinterpreted():
    vm = context()
    del vm["admission_accounting_version"]
    assert admission_updates(vm, generation=GENERATION, vm_uid=VM) == {}


@pytest.mark.parametrize("bad", [True, -1, "2", 2.5, None, 2**63])
def test_invalid_counter_cannot_be_coerced_or_reset(bad):
    with pytest.raises(ValueError):
        admission_updates(
            context(provision_attempts=bad), generation=GENERATION, vm_uid=VM
        )


@pytest.mark.parametrize(
    "marker", [[], {}, {"provision_generation": GENERATION, "vm_uid": str(uuid4())}]
)
def test_unknown_or_changed_admission_is_not_a_second_vm(marker):
    with pytest.raises(ValueError):
        admission_updates(
            context(provision_admission=marker), generation=GENERATION, vm_uid=VM
        )


@pytest.mark.parametrize(
    "field,bad",
    [("generation", str(uuid4())), ("generation", True), ("vm_uid", "opaque")],
)
def test_only_exact_canonical_generation_and_uid_can_count(field, bad):
    args = {"generation": GENERATION, "vm_uid": VM, field: bad}
    with pytest.raises(ValueError):
        admission_updates(context(), **args)
