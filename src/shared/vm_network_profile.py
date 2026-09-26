"""One explicit network contract for clean, single-NIC retained VM disks."""

from __future__ import annotations

import hashlib
import os
import re
from collections.abc import Mapping
from uuid import UUID


NETWORK_DATA = (
    "version: 2\n"
    "ethernets:\n"
    "  enp1s0:\n"
    "    match:\n"
    "      name: enp1s0\n"
    "    dhcp4: true\n"
    "    dhcp6: true\n"
)
NETWORK_PROFILE = {
    "version": 1,
    "kind": "nocloud-dhcp-by-interface-name",
    "interface": "enp1s0",
    "network_data_sha256": "sha256:" + hashlib.sha256(NETWORK_DATA.encode()).hexdigest(),
}
_IMAGE = re.compile(r"^[^\s@]+@sha256:[0-9a-f]{64}$")


def validate_network_profile(value: object) -> None:
    if not isinstance(value, Mapping) or dict(value) != NETWORK_PROFILE:
        raise ValueError("Unsupported VM network profile")


def compatible_image(image: object, *, allowlist: str | None = None) -> bool:
    """Operator attestations apply to immutable image refs only."""
    if not isinstance(image, str) or _IMAGE.fullmatch(image) is None:
        return False
    if allowlist is None:
        allowlist = os.environ.get("VM_NETWORK_PROFILE_IMAGE_ALLOWLIST", "")
    return image in {item.strip() for item in allowlist.split(",") if item.strip()}


def selected_profile(image: object, *, prepared: bool = False) -> dict | None:
    """Default-off admission for a new, clean registry/golden rootdisk."""
    if (
        os.environ.get("VM_NETWORK_PROFILE_ENABLED", "false").lower() != "true"
        or prepared
        or not compatible_image(image)
    ):
        return None
    return dict(NETWORK_PROFILE)


def reusable_profile_evidence(
    value: object, profile: object, *, provision_generation: object,
    vm_uid: object, pvc_uid: object, vmi_uid: object = None,
    launcher_uid: object = None, interface_mac: object = None,
) -> bool:
    """A first-boot pinned-SSH receipt for this exact disk and runtime."""
    try:
        validate_network_profile(profile)
    except ValueError:
        return False
    if not isinstance(value, Mapping):
        return False
    for key, identity in (
        ("provision_generation", provision_generation),
        ("vm_uid", vm_uid),
        ("pvc_uid", pvc_uid),
    ):
        if not isinstance(identity, str) or value.get(key) != identity:
            return False
    for key, identity in (("vmi_uid", vmi_uid), ("launcher_uid", launcher_uid)):
        observed = value.get(key)
        if not isinstance(observed, str) or not observed:
            return False
        if identity is not None and observed != identity:
            return False
    if interface_mac is not None and value.get("interface_mac") != interface_mac:
        return False
    try:
        boot_id = str(UUID(value["guest_boot_id"]))
    except (KeyError, TypeError, ValueError, AttributeError):
        return False
    instance_id = value.get("cloud_init_instance_id")
    return (
        value.get("profile") == NETWORK_PROFILE
        and value.get("name_only_dhcp") is True
        and boot_id == value["guest_boot_id"]
        and isinstance(instance_id, str)
        and bool(instance_id)
        and value.get("cloud_init_cached_instance_id") == instance_id
        and isinstance(value.get("network_file_sha256"), str)
        and re.fullmatch(r"[0-9a-f]{64}", value["network_file_sha256"]) is not None
    )
