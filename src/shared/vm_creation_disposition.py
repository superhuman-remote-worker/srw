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
    if row.get("owner_kind") == "thread":
        if row.get("thread_id") is None and row.get("job_id") is None:
            raise ValueError("Thread cancellation source is malformed")
        if row.get("thread_id") is not None and row.get("job_id") is not None \
                and str(row["thread_id"]) != str(row["job_id"]):
            raise ValueError("Thread cancellation owner changed")
        result["job_id"] = str(row.get("thread_id") or row["job_id"])
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
