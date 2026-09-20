"""Exact child cleanup identity derived only from a frozen cancellation ledger."""

import hashlib
import json
from collections.abc import Mapping
from uuid import UUID, uuid5, NAMESPACE_URL


def root_cleanup(disposition):
    """Derive the existing controller cleanup protocol's full immutable intent."""
    if (
        not isinstance(disposition, Mapping)
        or type(disposition.get("version")) is not int
        or disposition["version"] != 1
        or disposition.get("disk_policy") != "purge_new_job_disk"
        or disposition.get("workspace_storage") is not None
        or disposition.get("workspace_instance_id") is not None
    ):
        raise ValueError("Cancellation does not authorize disk purge")
    root = disposition["objects"]["rootdisk"]
    if (
        root["outcome"] != "observed"
        or root["name"] != f"agent-vm-{disposition['job_id']}-rootdisk"
        or root["namespace"] != disposition["namespace"]
    ):
        raise ValueError("Cancellation root identity changed")
    for value in (
        disposition["request_id"],
        disposition["disposition_id"],
        disposition["job_id"],
        disposition["provision_generation"],
        disposition["admission_id"],
        root["uid"],
        root["pvc_uid"],
    ):
        if not isinstance(value, str) or str(UUID(value)) != value:
            raise ValueError("Cancellation identity is invalid")
    identity = {
        "source": "controller_rootdisk_delete",
        "owner_kind": "job",
        "owner_id": disposition["job_id"],
        "pvc_uid": root["pvc_uid"],
        "dv_uid": root["uid"],
        "provision_generation": disposition["provision_generation"],
    }
    canonical = json.dumps(identity, sort_keys=True, separators=(",", ":"))
    request_id = uuid5(
        NAMESPACE_URL,
        "srw-controller-cleanup:"
        + canonical
        + ":parent:"
        + disposition["admission_id"],
    )
    return {
        **identity,
        "request_id": str(request_id),
        "intent_digest": "sha256:" + hashlib.sha256(canonical.encode()).hexdigest(),
    }


def exact_root_child(child, disposition):
    """No arbitrary descendant is exempted from the existing cleanup hold."""
    try:
        expected = root_cleanup(disposition)
        return str(child["parent_admission_id"]) == disposition["admission_id"] and all(
            str(child[key]) == expected[key]
            for key in (
                "source",
                "owner_kind",
                "owner_id",
                "pvc_uid",
                "request_id",
                "intent_digest",
            )
        )
    except (KeyError, TypeError, ValueError):
        return False


def disposition_parent_identity(proof):
    if (
        not isinstance(proof, Mapping)
        or set(proof)
        != {
            "version",
            "kind",
            "admission_id",
            "request_id",
            "intent_digest",
            "retry_request_id",
            "disposition_id",
        }
        or type(proof["version"]) is not int
        or proof["version"] != 1
        or proof["kind"] != "creation_disposition"
    ):
        raise ValueError("Cancellation parent is invalid")
    for key in ("admission_id", "request_id", "retry_request_id", "disposition_id"):
        if str(UUID(proof[key])) != proof[key]:
            raise ValueError("Cancellation parent identity is invalid")
    return (
        UUID(proof["admission_id"]),
        UUID(proof["request_id"]),
        proof["intent_digest"],
        "controller_vm_create",
    )


async def validate_disposition_child(
    conn,
    proof,
    *,
    owner_kind,
    owner_id,
    pvc_uid,
    request_id,
    source,
    intent_digest,
    provision_generation,
    expected_vm_uid,
):
    """Caller holds owner/PVC and original parent admission locks.

    The parent serializes every disposition transition. This read deliberately
    takes no retry lock before the existing queue/Job scope.
    """
    try:
        disposition_parent_identity(proof)
        row = await conn.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1 AND creation_admission_id=$2",
            UUID(proof["retry_request_id"]),
            UUID(proof["admission_id"]),
        )
        if (
            row is None
            or row["state"] != "cancel_requested"
            or row["expected_pvc_uid"] is not None
        ):
            return False
        from orchestrator.services.vm_creation_retry_store import _creation_intent
        from orchestrator.services.vm_workspace_recovery_store import (
            cleanup_intent_digest,
        )

        # Supplied and stored hashes agreeing cannot replace the full immutable
        # creation ledger, including its original request and exact new-disk mode.
        if proof["intent_digest"] != cleanup_intent_digest(
            _creation_intent(row)
        ) or proof["request_id"] != str(
            uuid5(NAMESPACE_URL, "vm-create:" + str(row["request_id"]))
        ):
            return False
        disposition = row["cancellation_disposition"]
        if isinstance(disposition, str):
            disposition = json.loads(disposition)
        expected = root_cleanup(disposition)
        return (
            expected_vm_uid is None
            and disposition["job_id"] == str(row["job_id"])
            and disposition["provision_generation"] == str(row["provision_generation"])
            and disposition["carrier_uid"] == str(row["creation_carrier_uid"])
            and disposition["namespace"] == row["creation_carrier_namespace"]
            and disposition["disposition_id"] == proof["disposition_id"]
            and disposition["request_id"] == proof["retry_request_id"]
            and disposition["admission_id"] == proof["admission_id"]
            and expected
            == dict(
                source=source,
                owner_kind=owner_kind,
                owner_id=str(owner_id),
                pvc_uid=str(pvc_uid),
                dv_uid=expected["dv_uid"],
                provision_generation=provision_generation,
                request_id=str(request_id),
                intent_digest=intent_digest,
            )
        )
    except (KeyError, TypeError, ValueError):
        return False
