"""Once-per-generation accounting after authenticated VM admission observation."""

from collections.abc import Mapping
from uuid import UUID


def _uuid(value):
    try:
        return type(value) is str and str(UUID(value)) == value
    except (ValueError, TypeError, AttributeError):
        return False


def admission_updates(vm: Mapping, *, generation: str, vm_uid: str) -> dict:
    """Caller owns authenticity, exact phase identity and transaction fencing.

    Historical generations remain outside this accounting version. This marker
    is bookkeeping only; it grants neither create nor retirement authority.
    """
    if "admission_accounting_version" not in vm:
        return {}
    if (
        type(vm["admission_accounting_version"]) is not int
        or vm["admission_accounting_version"] != 1
    ):
        raise ValueError("VM admission accounting version is unproven")
    if (
        not _uuid(generation)
        or not _uuid(vm_uid)
        or generation != vm.get("provision_generation")
    ):
        raise ValueError("VM admission identity is unproven")
    attempts = vm.get("provision_attempts", 0)
    if type(attempts) is not int or not 0 <= attempts < 2**63 - 1:
        raise ValueError("VM admission budget is unproven")
    identity = {"provision_generation": generation, "vm_uid": vm_uid}
    previous = vm.get("provision_admission")
    if previous is not None:
        if previous != identity:
            raise ValueError("VM admission identity changed")
        return {}
    return {
        "provision_admission": identity,
        "provision_attempts": 0 if vm.get("status") == "ready" else attempts + 1,
    }
