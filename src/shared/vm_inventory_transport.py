"""Bounded JSON transport for sanitized resource inventory only."""

import json
import re

from shared.vm_lifecycle_auth import AUTH_FIELD
from shared.vm_resource_inventory import InventoryError


OPERATION = "vm_resource_inventory_publish"
ENVELOPE_ALLOWANCE = 2048
RECEIPT_MAX_BYTES = 4096
_AUTH_FIELDS = {
    "version",
    "direction",
    "operation",
    "issued_at",
    "request_id",
    "correlation_id",
    "signature",
}


def _refuse(*_):
    raise InventoryError("invalid_inventory_transport")


def _integer(value):
    if len(value) > 20:
        _refuse()
    number = int(value)
    if not -(2**63) <= number < 2**63:
        _refuse()
    return number


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            _refuse()
        result[key] = value
    return result


def decode_document(raw: bytes, *, max_bytes: int):
    """Bound nesting before parsing; refuse duplicate keys and non-integer numbers."""
    if len(raw) > max_bytes:
        raise InventoryError("byte_limit")
    depth, in_string, escaped = 0, False, False
    for byte in raw:
        if in_string:
            if escaped:
                escaped = False
            elif byte == 92:
                escaped = True
            elif byte == 34:
                in_string = False
        elif byte == 34:
            in_string = True
        elif byte in (91, 123):
            depth += 1
            if depth > 32:
                _refuse()
        elif byte in (93, 125):
            depth -= 1
            if depth < 0:
                _refuse()
    try:
        return json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_object,
            parse_constant=_refuse,
            parse_float=_refuse,
            parse_int=_integer,
        )
    except (ValueError, UnicodeError, RecursionError):
        _refuse()


def validate_envelope(value, *, payload_fields, direction):
    if not isinstance(value, dict) or set(value) != {*payload_fields, AUTH_FIELD}:
        _refuse()
    auth = value[AUTH_FIELD]
    if not isinstance(auth, dict) or set(auth) != _AUTH_FIELDS:
        _refuse()
    if type(auth["issued_at"]) is not int or not 0 <= auth["issued_at"] < 2**63:
        _refuse()
    for key in ("version", "direction", "operation", "request_id", "signature"):
        if not isinstance(auth[key], str) or not 1 <= len(auth[key]) <= 80:
            _refuse()
    if re.fullmatch(r"[0-9a-f]{64}", auth["signature"]) is None:
        _refuse()
    if direction == "request":
        if auth["correlation_id"] is not None:
            _refuse()
    elif (
        not isinstance(auth["correlation_id"], str) or len(auth["correlation_id"]) != 36
    ):
        _refuse()
    return auth
