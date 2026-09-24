"""Pure controller rootdisk quantity and floor resolution."""

import re


_QUANTITY = re.compile(r"^(\d+)(Ki|Mi|Gi|Ti|K|M|G|T)?$")
_MULTIPLIER = {
    None: 1,
    "K": 10**3,
    "M": 10**6,
    "G": 10**9,
    "T": 10**12,
    "Ki": 2**10,
    "Mi": 2**20,
    "Gi": 2**30,
    "Ti": 2**40,
}


def quantity_bytes(value: object) -> int | None:
    match = _QUANTITY.match(str(value).strip()) if value is not None else None
    if match is None:
        return None
    return int(match.group(1)) * _MULTIPLIER[match.group(2)]


def resolved_disk_size(requested: object, floor: str) -> str:
    """Apply the controller's exact default and minimum-size rule."""
    floor_bytes = quantity_bytes(floor)
    if not isinstance(floor, str) or floor_bytes is None or floor_bytes <= 0:
        raise ValueError("Controller disk floor is invalid")
    if requested in (None, ""):
        return floor
    requested_bytes = quantity_bytes(requested)
    if requested_bytes is None or requested_bytes < floor_bytes:
        return floor
    return str(requested).strip()
