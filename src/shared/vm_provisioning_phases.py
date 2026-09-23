"""Conservative clocks for authenticated, exact-identity provisioning evidence.

The caller verifies the controller envelope and Kubernetes ownership, then applies
this reducer with database time in a generation/revision-fenced transaction.
These decisions neither confer execution authority nor perform cleanup. A boot
timeout may only request the existing exact-generation retirement operation.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from uuid import UUID

PROVISIONING_PHASE_VERSION = 1
_IDENTITY_FIELDS = (
    "owner_kind",
    "owner_id",
    "namespace",
    "provision_generation",
    "vm_uid",
    "vmi_uid",
    "rootdisk_dv_name",
    "rootdisk_dv_uid",
    "rootdisk_pvc_name",
    "rootdisk_pvc_uid",
    "disk_mode",
)
_NULLABLE_UIDS = {"vmi_uid", "rootdisk_dv_uid", "rootdisk_pvc_uid"}
_UID_FIELDS = _NULLABLE_UIDS | {"owner_id", "provision_generation", "vm_uid"}
_DISK_STAGES = {"pending": 0, "preparing": 1, "transferring": 2, "ready": 3}
_DISK_PHASES = {*_DISK_STAGES, "waiting_for_consumer", "failed", "unknown"}
_VMI_PHASES = {
    "absent",
    "pending",
    "scheduling",
    "scheduled",
    "running",
    "succeeded",
    "failed",
    "unknown",
}
_PHASES = {"disk_preparation", "placement", "boot", "attention"}
_NAME = re.compile(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?\Z")


@dataclass(frozen=True, slots=True)
class ProvisioningDecision:
    action: str
    reason: str | None = None


def _number(value: object, *, minimum: float = 0) -> bool:
    try:
        return type(value) in (int, float) and math.isfinite(value) and value >= minimum
    except OverflowError:
        return False


def _clock(value: object, now: float) -> bool:
    return _number(value) and 0 < value <= now


def _identity(raw: Mapping) -> dict:
    if not all(key in raw for key in _IDENTITY_FIELDS):
        raise ValueError("provisioning identity is incomplete")
    result = {key: raw[key] for key in _IDENTITY_FIELDS}
    if result["owner_kind"] not in ("job", "thread"):
        raise ValueError("provisioning owner identity is invalid")
    if result["disk_mode"] not in ("clone", "retained"):
        raise ValueError("provisioning disk mode is invalid")
    for key in _UID_FIELDS:
        value = result[key]
        if value is None and key in _NULLABLE_UIDS:
            continue
        try:
            if type(value) is not str or str(UUID(value)) != value:
                raise ValueError
        except (ValueError, TypeError, AttributeError) as exc:
            raise ValueError(f"provisioning {key} identity is invalid") from exc
    for key in ("namespace", "rootdisk_pvc_name", "rootdisk_dv_name"):
        value = result[key]
        if key == "rootdisk_dv_name" and value is None:
            if result["rootdisk_dv_uid"] is not None:
                raise ValueError("provisioning DV identity has no name")
            continue
        if type(value) is not str or not _NAME.fullmatch(value):
            raise ValueError(f"provisioning {key} identity is invalid")
    return result


def _stored_state(previous: Mapping, now: float) -> dict:
    if not isinstance(previous, Mapping):
        raise ValueError("provisioning clock state is missing")
    if type(previous.get("version")) is not int or previous["version"] != 1:
        raise ValueError("unsupported provisioning clock state")
    identity = previous.get("identity")
    if not isinstance(identity, Mapping):
        raise ValueError("provisioning clock identity is missing")
    _identity(identity)
    if type(previous.get("phase")) is not str or previous["phase"] not in _PHASES:
        raise ValueError("provisioning clock phase is invalid")
    for key in ("phase_started_at", "last_real_progress_at", "observed_at"):
        if not _clock(previous.get(key), now):
            raise ValueError("provisioning clock is invalid")
    started = previous.get("first_guest_started_at")
    if started is not None and (
        not _clock(started, now) or identity["vmi_uid"] is None
    ):
        raise ValueError("provisioning boot clock is invalid")
    if previous["phase"] == "boot" and started is None:
        raise ValueError("provisioning boot clock is missing")
    if not _number(previous.get("disk_stall_elapsed_s")):
        raise ValueError("provisioning disk clock is invalid")
    stage = previous.get("disk_stage_high_water")
    if type(stage) is not int or not -1 <= stage <= 3:
        raise ValueError("provisioning disk stage is invalid")
    if started is not None and (identity["rootdisk_pvc_uid"] is None or stage != 3):
        raise ValueError("provisioning boot history has no ready disk identity")
    progress = previous.get("disk_progress_high_water")
    if progress is not None and (not _number(progress) or progress > 100):
        raise ValueError("provisioning disk progress is invalid")
    if any(
        previous[key] > previous["observed_at"]
        for key in ("phase_started_at", "last_real_progress_at")
    ) or (started is not None and started > previous["observed_at"]):
        raise ValueError("provisioning clocks are inconsistent")
    return dict(previous)


def rebind_recovered_provisioning(
    previous: Mapping,
    *,
    expected_identity: Mapping,
    successor_vmi_uid: str,
    now: float,
) -> dict:
    """Carry valid clocks across an exact, separately authorized recovery release.

    The recovery store must first prove the predecessor stop and authenticated
    successor. Ordinary observations must continue to reject bound VMI changes.
    """
    state = _stored_state(previous, now)
    identity = dict(state["identity"])
    for field in (
        "owner_kind",
        "owner_id",
        "namespace",
        "provision_generation",
        "vm_uid",
        "vmi_uid",
        "rootdisk_pvc_uid",
    ):
        if identity[field] != expected_identity.get(field):
            raise ValueError("recovery predecessor phase identity changed")
    identity["vmi_uid"] = successor_vmi_uid
    _identity(identity)
    state["identity"] = identity
    return _stored_state(state, now)


def observe_provisioning(
    previous: Mapping | None, observation: Mapping, *, now: float
) -> dict:
    """Reduce one authenticated observation without mutating either argument.

    Invalid or conflicting evidence raises ValueError. The adapter must retain
    authority and publish attention, never reinterpret this as absent state.
    Bound identities may not disappear; new allocated identities bind once.
    """
    if not _clock(now, now) or not isinstance(observation, Mapping):
        raise ValueError("provisioning observation/time is invalid")
    if type(observation.get("version")) is not int or observation["version"] != 1:
        raise ValueError("unsupported provisioning observation")
    identity = _identity(observation)
    disk, vmi = observation.get("disk_phase"), observation.get("vmi_phase")
    progress = observation.get("disk_progress")
    if (
        type(disk) is not str
        or type(vmi) is not str
        or disk not in _DISK_PHASES
        or vmi not in _VMI_PHASES
    ):
        raise ValueError("provisioning evidence phase is invalid")
    if progress is not None and (not _number(progress) or progress > 100):
        raise ValueError("provisioning evidence progress is invalid")
    if (vmi == "absent") != (identity["vmi_uid"] is None):
        raise ValueError("provisioning VMI identity is inconsistent")
    if disk == "ready" and identity["rootdisk_pvc_uid"] is None:
        raise ValueError("ready disk identity is missing")
    old = _stored_state(previous, now) if previous is not None else None
    if old:
        for key in _IDENTITY_FIELDS:
            bound = old["identity"][key]
            if bound != identity[key] and not (key in _NULLABLE_UIDS and bound is None):
                raise ValueError(f"provisioning {key} identity changed")
    if vmi in {"failed", "succeeded", "unknown"} or disk in {"failed", "unknown"}:
        phase, reason = "attention", "vm_phase_unproven"
    elif vmi == "running":
        phase, reason = (
            ("boot", None) if disk == "ready" else ("attention", "vm_phase_conflict")
        )
    elif old and old["first_guest_started_at"] is not None:
        phase, reason = "attention", "vm_runtime_changed"
    elif disk in {"ready", "waiting_for_consumer"}:
        phase, reason = "placement", None
    else:
        phase, reason = "disk_preparation", None

    stage = _DISK_STAGES.get(disk, -1)
    high_stage = old["disk_stage_high_water"] if old else -1
    high_progress = old["disk_progress_high_water"] if old else None
    progressed = old is None or stage > high_stage
    if stage > high_stage:
        high_stage, high_progress = stage, progress
    elif stage == high_stage and stage >= 0 and progress is not None:
        if high_progress is None or progress > high_progress:
            high_progress, progressed = progress, True
    elapsed = old["disk_stall_elapsed_s"] if old else 0.0
    if old and old["phase"] == "disk_preparation":
        elapsed += now - old["observed_at"]
    if progressed:
        elapsed = 0.0
    started = old["first_guest_started_at"] if old else None
    if phase == "boot" and started is None:
        started = now
    return {
        "version": 1,
        "identity": identity,
        "phase": phase,
        "phase_started_at": old["phase_started_at"]
        if old and old["phase"] == phase
        else now,
        "last_real_progress_at": now if progressed else old["last_real_progress_at"],
        "observed_at": now,
        "first_guest_started_at": started,
        "disk_stage_high_water": high_stage,
        "disk_progress_high_water": high_progress,
        "disk_stall_elapsed_s": elapsed,
        "attention_reason": reason,
    }


def provisioning_decision(
    state: Mapping | None,
    *,
    now: float,
    boot_timeout_s: float = 600,
    rootdisk_stall_timeout_s: float = 2700,
) -> ProvisioningDecision:
    """Return wait, retained attention, or a guarded boot-timeout request.

    Initialization, readiness, deadline, recovery and cleanup precedence belong
    to the dispatcher. Missing/invalid phase state never permits recycling.
    """
    try:
        if not _clock(now, now) or not all(
            _number(value) and value > 0
            for value in (boot_timeout_s, rootdisk_stall_timeout_s)
        ):
            raise ValueError("invalid provisioning budget")
        current = _stored_state(state, now)
    except (ValueError, TypeError, KeyError):
        return ProvisioningDecision("attention", "vm_phase_unproven")
    phase = current["phase"]
    if phase == "attention":
        reason = current.get("attention_reason")
        if reason not in (
            "vm_phase_unproven",
            "vm_phase_conflict",
            "vm_runtime_changed",
        ):
            reason = "vm_phase_unproven"
        return ProvisioningDecision("attention", reason)
    if phase == "boot" and now - current["first_guest_started_at"] > boot_timeout_s:
        return ProvisioningDecision("boot_timeout", "vm_boot_timeout")
    if phase == "disk_preparation" and (
        current["disk_stall_elapsed_s"] + now - current["observed_at"]
        > rootdisk_stall_timeout_s
    ):
        return ProvisioningDecision("attention", "vm_rootdisk_stalled")
    return ProvisioningDecision("wait")
