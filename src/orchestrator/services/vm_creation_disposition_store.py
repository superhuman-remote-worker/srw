"""Freeze partial creation cleanup evidence under the existing create authority.

This store does not grant Kubernetes writes or complete an admission. An issued
effect is possible actuation forever until exact observation/rejection settles
it. In particular, absence and claim expiry are not non-issuance evidence.
"""

import json
from uuid import UUID, uuid4, uuid5, NAMESPACE_URL

from orchestrator.services.vm_creation_retry_store import (
    VMCreationRetryConflict,
    _json,
)


def source_resolution(row, source):
    """An empty effect ledger cannot disprove pre-grant source publication."""
    if row["controller_configuration"] is None:
        return "unknown"
    if source is not None:
        return (
            "required" if source["kind"] in {"golden", "prepared"} else "not_required"
        )
    if row["canonical_request"].get("preparation") is not None or (
        row["controller_configuration"].get("golden_enabled") is not False
        and row["expected_pvc_uid"] is None
    ):
        return "unknown"
    return "not_required"


class VMCreationDispositionStore:
    def __init__(self, retries):
        self.retries = retries
        self.db = retries.db

    async def prepare(self, *, request_id: str) -> dict:
        """Identify a cancellation-only carrier under an already held admission.

        This never inserts an effect or grants creation. Deterministic proposal
        identity lets an interrupted initial Lease publication be resumed.
        """
        from shared.vm_creation_issuance import canonical_configuration_digest
        from shared.vm_workspace_storage import storage_name

        async with self.db.acquire() as conn:
            async with conn.transaction():
                row, _ = await self.retries._effect_scope(conn, request_id)
                if row["state"] != "cancel_requested":
                    raise VMCreationRetryConflict("job_not_cancelled")
                permit = await self.retries._creation_permit_on_conn(conn, row)
                configuration = row["controller_configuration"]
                if (
                    configuration is None
                    or configuration.get("persistent_rootdisk") is not True
                    or canonical_configuration_digest(configuration)
                    != row["controller_configuration_digest"]
                ):
                    raise VMCreationRetryConflict("creation_configuration_unproven")
                if (
                    row["creation_carrier_uid"] is not None
                    or row["expected_pvc_uid"] is not None
                    or row["observed_pvc_uid"] is not None
                    or await conn.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM vm_creation_effects WHERE request_id=$1)",
                        row["request_id"],
                    )
                ):
                    raise VMCreationRetryConflict("creation_carrier_required")
                binding = row["canonical_request"].get("workspace_storage")
                return {
                    "actuation_allowed": False,
                    "namespace": configuration["namespace"],
                    "carrier_intent": {
                        "version": 1,
                        "source": "controller_vm_create",
                        "admission_id": str(permit["id"]),
                        "reservation_request_id": str(permit["request_id"]),
                        "intent_digest": permit["intent_digest"],
                        "retry_request_id": str(row["request_id"]),
                        "job_id": str(row["job_id"]),
                        "provision_generation": str(row["provision_generation"]),
                        "request_digest": row["request_digest"],
                        "controller_configuration_digest": row[
                            "controller_configuration_digest"
                        ],
                        "expected_pvc_uid": None,
                        "retained_dv_uid": None,
                        "current_dv_uid": None,
                        "current_pvc_uid": None,
                        "current_secret_uid": None,
                        "effect_kind": "rootdisk",
                        "effect_nonce": str(
                            uuid5(NAMESPACE_URL, "vm-cancel-carrier:" + request_id)
                        ),
                        "object_name": storage_name(binding)
                        if binding
                        else f"agent-vm-{row['job_id']}-rootdisk",
                    },
                }

    async def freeze(self, *, request_id: str, carrier: dict) -> dict:
        values = self.retries._carrier(carrier)
        async with self.db.acquire() as conn:
            async with conn.transaction():
                row, _ = await self.retries._effect_scope(conn, request_id)
                if row["state"] != "cancel_requested":
                    raise VMCreationRetryConflict("job_not_cancelled")
                await self.retries._check_carrier(conn, row, carrier, values)
                prior = _json(row["cancellation_disposition"])
                if prior is not None:
                    return {"frozen": True, "disposition": prior}
                effects = await conn.fetch(
                    "SELECT * FROM vm_creation_effects WHERE request_id=$1 ORDER BY effect_number FOR UPDATE",
                    row["request_id"],
                )
                if any(effect["state"] == "issued" for effect in effects):
                    return {"frozen": False, "reason": "creation_effect_unresolved"}
                if row["observed_vm_uid"] is not None or any(
                    effect["effect_kind"] == "vm" and effect["state"] != "rejected"
                    for effect in effects
                ):
                    return {"frozen": False, "reason": "creation_vm_requires_adoption"}
                binding = row["canonical_request"].get("workspace_storage")
                instance = None
                if binding is not None:
                    from orchestrator.services.vm_creation_attachment_store import (
                        attachment_instance_on_conn,
                    )

                    # Cancellation may have moved the exact Retain instance to
                    # Deleting, but cannot erase its recipe, generation or link.
                    instance = await attachment_instance_on_conn(
                        conn, row, adoption=True
                    )
                objects = {}
                frozen_effects = []
                source = None
                for effect in effects:
                    intent = _json(effect["carrier_intent"])
                    evidence = _json(effect["evidence"])
                    frozen_effects.append(
                        {
                            "effect_nonce": str(effect["effect_nonce"]),
                            "effect_kind": effect["effect_kind"],
                            "state": effect["state"],
                            "carrier_intent": intent,
                            "evidence": evidence,
                        }
                    )
                    if effect["effect_kind"] == "rootdisk":
                        source = intent.get("rootdisk_source")
                    if effect["state"] == "observed":
                        if effect["effect_kind"] in objects:
                            raise VMCreationRetryConflict("creation_effect_changed")
                        objects[effect["effect_kind"]] = evidence
                root = objects.get("rootdisk")
                if root is not None and root["pvc_uid"] != str(
                    row["expected_pvc_uid"] or row["observed_pvc_uid"]
                ):
                    raise VMCreationRetryConflict("retained_disk_changed")
                disposition = {
                    "version": 1,
                    "disposition_id": str(uuid4()),
                    "request_id": str(row["request_id"]),
                    "job_id": str(row["job_id"]),
                    "provision_generation": str(row["provision_generation"]),
                    "admission_id": str(row["creation_admission_id"]),
                    "carrier_uid": carrier["metadata"]["uid"],
                    "namespace": carrier["metadata"]["namespace"],
                    "disk_policy": "retain"
                    if binding or row["expected_pvc_uid"]
                    else "purge_new_job_disk",
                    "workspace_storage": binding,
                    "workspace_instance_id": str(instance["id"]) if instance else None,
                    "objects": objects,
                    "effects": frozen_effects,
                    "source": source,
                    "source_resolution": source_resolution(row, source),
                }
                await conn.execute(
                    "UPDATE vm_creation_retries SET cancellation_disposition=$2::jsonb,creation_carrier_uid=$3,creation_carrier_namespace=$4,revision=revision+1,updated_at=clock_timestamp() WHERE request_id=$1",
                    row["request_id"],
                    json.dumps(disposition),
                    UUID(carrier["metadata"]["uid"]),
                    carrier["metadata"]["namespace"],
                )
                return {"frozen": True, "disposition": disposition}
