"""Installed Job resource policy, resolved from durable authority under SQL locks.

The frozen creation configuration selects a policy identity; it never installs
one. Every writer constructs its reservation service from the matching durable
policy row, so a process flag change cannot erase an admitted request's gate.
"""

import json
import os

from shared.vm_resource_admission import ResourceAdmissionError
from shared.vm_resource_policy import validate_enforcement_resource_policy
from orchestrator.services.vm_resource_inventory_store import VMResourceInventoryStore
from orchestrator.services.vm_resource_reservation_store import VMResourceReservationStore


def configured_enforcement_policy(source=None):
    env = os.environ if source is None else source
    raw = env.get("VM_RESOURCE_ADMISSION_CONFIG", "")
    if not raw:
        return None
    try:
        document = json.loads(raw)
        if document["policy"]["enforcementEnabled"] is not True:
            return None
        return validate_enforcement_resource_policy(document)
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        raise ResourceAdmissionError("invalid_resource_policy") from None


async def installed_job_resource_store(conn, db, configuration, *, fresh=True):
    """Return the exact installed store or refuse a source/configuration gap.

    Callers already hold their owner, Job and retry source locks. This lookup
    adds no lock before those rows; the store itself takes the policy lock.
    """
    selected = configured_enforcement_policy()
    if isinstance(configuration, str):
        try:
            configuration = json.loads(configuration)
        except (ValueError, TypeError, UnicodeError, RecursionError):
            raise ResourceAdmissionError("resource_configuration_unavailable") from None
    if not isinstance(configuration, dict) or configuration.get("version") != 3:
        if selected is not None:
            raise ResourceAdmissionError("resource_configuration_unavailable")
        return None
    try:
        resource = configuration["resource_admission"]
        cluster_id = resource["cluster_id"]
        digest = resource["policy_digest"]
        row = await conn.fetchrow(
            "SELECT * FROM vm_resource_admission_policy WHERE cluster_id=$1",
            cluster_id,
        )
        if row is None or row["policy_digest"] != digest:
            raise ValueError
        document = row["document"]
        if isinstance(document, str):
            document = json.loads(document)
        snapshot = validate_enforcement_resource_policy(document)
        if snapshot.policy_digest != digest or snapshot.inventory.protocol != 2:
            raise ValueError
        if selected is not None and selected.policy_digest != digest:
            raise ValueError
        if row["mode"] != "enforce" and (
            fresh or row["mode"] not in {"drain", "off"}
        ):
            raise ValueError
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        raise ResourceAdmissionError("resource_policy_changed") from None
    settings = snapshot.inventory
    inventory = VMResourceInventoryStore(
        db,
        cluster_id=settings.cluster_id,
        namespace=settings.namespace,
        policy_digest=settings.policy_digest,
        label_keys=settings.label_keys,
        max_items=settings.max_items,
        max_bytes=settings.max_bytes,
        stale_after_seconds=settings.stale_after_seconds,
        history_limit=settings.history_limit,
        protocol=settings.protocol,
        kubevirt_namespace=settings.kubevirt_namespace,
        kubevirt_name=settings.kubevirt_name,
    )
    return VMResourceReservationStore(
        db, inventory=inventory, policy_document=document,
        policy_revision=row["revision"],
    )
