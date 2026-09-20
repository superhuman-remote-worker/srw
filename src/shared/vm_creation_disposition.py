"""Strict cancellation correlation identity; never a cleanup grant by itself."""

from collections.abc import Mapping
import re
from uuid import UUID

IDENTITY_FIELDS = (
    "request_id",
    "job_id",
    "provision_generation",
    "request_digest",
    "controller_configuration_digest",
)


def disposition_identity(row):
    result = {"version": 1, **{key: str(row[key]) for key in IDENTITY_FIELDS}}
    validate_disposition_request(result)
    return result


def validate_disposition_request(value):
    if (
        not isinstance(value, Mapping)
        or set(value) != {"version", *IDENTITY_FIELDS}
        or type(value["version"]) is not int
        or value["version"] != 1
    ):
        raise ValueError("Cancellation identity is incomplete")
    for key in IDENTITY_FIELDS[:3]:
        if not isinstance(value[key], str) or str(UUID(value[key])) != value[key]:
            raise ValueError("Cancellation identity is invalid")
    for key in IDENTITY_FIELDS[3:]:
        if not isinstance(value[key], str) or not re.fullmatch(
            r"sha256:[0-9a-f]{64}", value[key]
        ):
            raise ValueError("Cancellation digest is invalid")
    return dict(value)
