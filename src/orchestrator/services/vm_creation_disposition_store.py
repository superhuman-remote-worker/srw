"""Freeze partial creation cleanup evidence under the existing create authority.

Fixed-UID teardown authority requires a frozen no-VM-issuance disposition; this
store never grants creation or completes the parent admission. An issued create
is possible actuation until exact observation/rejection settles it. In particular,
absence and claim expiry are not non-issuance evidence.
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

    async def _locked(self, conn, request_id, carrier):
        values = self.retries._carrier(carrier)
        row, _ = await self.retries._effect_scope(conn, request_id)
        if row["state"] != "cancel_requested":
            raise VMCreationRetryConflict("job_not_cancelled")
        await self.retries._check_carrier(conn, row, carrier, values)
        disposition = _json(row["cancellation_disposition"])
        if disposition is None:
            raise VMCreationRetryConflict("creation_disposition_unproven")
        # The freeze trigger prevents new effects, but recheck possible issuance
        # here too: no absence observation may substitute for this SQL authority.
        if row["observed_vm_uid"] is not None or await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM vm_creation_effects WHERE request_id=$1 AND (state='issued' OR (effect_kind='vm' AND state<>'rejected')))",
            row["request_id"],
        ):
            raise VMCreationRetryConflict("creation_effect_unresolved")
        return row, disposition

    async def _grant(self, conn, row, disposition, stage, *, create_child):
        """Only fixed identities; source and retained attachment stay unsupported."""
        if stage not in {"rootdisk", "cloud_init"}:
            raise VMCreationRetryConflict("creation_disposition_stage_unavailable")
        resource = disposition["objects"].get(stage)
        if resource is None:
            if any(
                e["effect_kind"] == stage and e["state"] != "rejected"
                for e in disposition["effects"]
            ) or (
                stage == "rootdisk"
                and (
                    row["expected_pvc_uid"] is not None
                    or row["observed_pvc_uid"] is not None
                )
            ):
                raise VMCreationRetryConflict("creation_disposition_stage_unavailable")
            from shared.vm_workspace_storage import storage_name

            binding = row["canonical_request"].get("workspace_storage")
            name = (
                storage_name(binding)
                if stage == "rootdisk" and binding
                else (
                    "agent-vm-"
                    + str(row["job_id"])
                    + ("-rootdisk" if stage == "rootdisk" else "-cloudinit")
                )
            )
            return {
                "operation": "confirm_absent",
                "completion": {
                    "version": 1,
                    "disposition_id": disposition["disposition_id"],
                    "kind": "rootdisk_never_issued"
                    if stage == "rootdisk"
                    else "secret_never_issued",
                    "name": name,
                    "namespace": disposition["namespace"],
                },
            }
        common = {
            "version": 1,
            "disposition_id": disposition["disposition_id"],
            "namespace": resource["namespace"],
            "name": resource["name"],
            "uid": resource["uid"],
        }
        if stage == "cloud_init":
            return {
                "operation": "delete_secret",
                "resource": resource,
                "completion": {**common, "kind": "secret_absent"},
            }
        if disposition["disk_policy"] == "retain":
            return {
                "operation": "retain_rootdisk",
                "resource": resource,
                "completion": {
                    **common,
                    "kind": "rootdisk_retained",
                    "pvc_uid": resource["pvc_uid"],
                },
            }
        from orchestrator.services.vm_creation_disposition_cleanup import root_cleanup

        try:
            intent = root_cleanup(disposition)
        except (ValueError, KeyError, TypeError) as exc:
            raise VMCreationRetryConflict(
                "creation_disposition_stage_unavailable"
            ) from exc
        parent = await self.retries._creation_permit_on_conn(conn, row)
        proof = {
            "version": 1,
            "kind": "creation_disposition",
            "admission_id": str(parent["id"]),
            "request_id": str(parent["request_id"]),
            "intent_digest": parent["intent_digest"],
            "retry_request_id": str(row["request_id"]),
            "disposition_id": disposition["disposition_id"],
        }
        if create_child:
            child = await self.retries.cleanup.acquire_cleanup_permit_on_conn(
                conn,
                owner_kind=intent["owner_kind"],
                owner_id=UUID(intent["owner_id"]),
                pvc_uid=UUID(intent["pvc_uid"]),
                request_id=UUID(intent["request_id"]),
                source=intent["source"],
                intent_digest=intent["intent_digest"],
                parent_cleanup=proof,
                parent_provision_generation=intent["provision_generation"],
            )
            if not child.allowed:
                raise VMCreationRetryConflict(child.reason)
            if child.completed_outcome not in {None, "deleted"}:
                raise VMCreationRetryConflict("creation_disposition_incomplete")
            child_id = child.admission_id
        else:
            child_id = await conn.fetchval(
                "SELECT id FROM vm_workspace_cleanup_admissions WHERE parent_admission_id=$1 AND request_id=$2 AND owner_kind='job' AND owner_id=$3 AND pvc_uid=$4 AND source=$5 AND intent_digest=$6",
                parent["id"],
                UUID(intent["request_id"]),
                row["job_id"],
                UUID(intent["pvc_uid"]),
                intent["source"],
                intent["intent_digest"],
            )
            if child_id is None:
                raise VMCreationRetryConflict("creation_disposition_incomplete")
        cleanup = {**intent, "admission_id": str(child_id), "parent_cleanup": proof}
        return {
            "operation": "purge_rootdisk",
            "resource": resource,
            "cleanup": cleanup,
            "completion": {
                **common,
                "kind": "rootdisk_purged",
                "pvc_uid": resource["pvc_uid"],
                "admission_id": str(child_id),
                "request_id": intent["request_id"],
                "intent_digest": intent["intent_digest"],
            },
        }

    async def _source_intent(self, conn, row, disposition, source, target_observation):
        """Freeze resolution before I/O; this immutable stage is NOT completion."""
        from shared.vm_creation_source_disposition import source_disposition_plan
        from shared.vm_workspace_storage import storage_name

        if source is not None and not isinstance(source, dict):
            raise VMCreationRetryConflict("creation_disposition_source_unproven")
        progress = _json(row["cancellation_progress"])
        prior = progress.get("source")
        if prior is not None:
            if prior.get("kind") != "source_disposition_planned" or (
                source is not None and source != prior["source"]
            ):
                raise VMCreationRetryConflict("creation_disposition_evidence_changed")
            return {"operation": "dispose_source", "plan": prior}
        frozen_source = disposition["source"]
        if frozen_source is not None:
            if source is not None and source != frozen_source:
                raise VMCreationRetryConflict("creation_disposition_evidence_changed")
            source = frozen_source
        if source is None and disposition["source_resolution"] != "not_required":
            raise VMCreationRetryConflict("creation_disposition_source_unproven")
        target = None
        if (
            source
            and source.get("kind")
            in {"golden", "prepared", "preparation_never_delivered"}
            and source.get("mode") != "retained"
        ):
            root = disposition["objects"].get("rootdisk")
            if root is not None and disposition["disk_policy"] == "retain":
                from shared.vm_creation_source_disposition import (
                    completed_source_target,
                )

                try:
                    target = completed_source_target(disposition, target_observation)
                except (ValueError, TypeError, KeyError, StopIteration) as exc:
                    raise VMCreationRetryConflict(
                        "creation_disposition_incomplete"
                    ) from exc
            elif root is not None:
                grant = await self._grant(
                    conn, row, disposition, "rootdisk", create_child=False
                )
                target = grant["completion"]
                child = await conn.fetchrow(
                    "SELECT completed_at,outcome FROM vm_workspace_cleanup_admissions WHERE id=$1",
                    UUID(grant["cleanup"]["admission_id"]),
                )
                if (
                    progress.get("rootdisk") != target
                    or child["completed_at"] is None
                    or child["outcome"] != "deleted"
                ):
                    raise VMCreationRetryConflict("creation_disposition_incomplete")
            else:
                if (
                    row["expected_pvc_uid"] is not None
                    or row["observed_pvc_uid"] is not None
                    or any(
                        effect["effect_kind"] == "rootdisk"
                        and effect["state"] != "rejected"
                        for effect in disposition["effects"]
                    )
                ):
                    raise VMCreationRetryConflict(
                        "creation_disposition_source_unproven"
                    )
                binding = row["canonical_request"].get("workspace_storage")
                target = {
                    "kind": "rootdisk_never_issued",
                    "namespace": disposition["namespace"],
                    "name": storage_name(binding)
                    if binding
                    else f"agent-vm-{row['job_id']}-rootdisk",
                }
        try:
            plan = source_disposition_plan(row, disposition, source, target)
        except (ValueError, KeyError, TypeError) as exc:
            raise VMCreationRetryConflict(
                "creation_disposition_source_unproven"
            ) from exc
        await conn.execute(
            "UPDATE vm_creation_retries SET cancellation_progress=cancellation_progress || $2::jsonb,revision=revision+1,updated_at=clock_timestamp() WHERE request_id=$1",
            row["request_id"],
            json.dumps({"source": plan}),
        )
        return {"operation": "dispose_source", "plan": plan}

    async def authorize(
        self,
        *,
        request_id: str,
        carrier: dict,
        stage: str,
        source: dict | None = None,
        target: dict | None = None,
    ) -> dict:
        """Grant repeatable exact-UID teardown, never a CREATE or terminal release.

        The controller must fence consumers at every destructive API boundary.
        The authenticated controller actuator supplies fresh consumer checks and
        exact readback; the grant itself is not completion evidence.
        """
        async with self.db.acquire() as conn:
            async with conn.transaction():
                row, disposition = await self._locked(conn, request_id, carrier)
                if stage == "workspace_attachment":
                    if source is not None:
                        raise VMCreationRetryConflict(
                            "creation_disposition_stage_unavailable"
                        )
                    return await self._attachment_intent(conn, row, disposition, target)
                if stage == "source":
                    return await self._source_intent(
                        conn, row, disposition, source, target
                    )
                if source is not None or target is not None:
                    raise VMCreationRetryConflict(
                        "creation_disposition_stage_unavailable"
                    )
                return await self._grant(
                    conn, row, disposition, stage, create_child=True
                )

    async def record(
        self, *, request_id: str, carrier: dict, stage: str, evidence: dict
    ) -> dict:
        """Accept authenticated exact absence; a root also requires SQL completion.

        The authenticated controller records only after exact readback and fresh
        consumer fencing. A typed result cannot release the parent.
        """
        async with self.db.acquire() as conn:
            async with conn.transaction():
                row, disposition = await self._locked(conn, request_id, carrier)
                if stage == "workspace_attachment":
                    return await self._record_attachment(
                        conn, row, disposition, evidence
                    )
                if stage == "source":
                    return await self._record_source(conn, row, disposition, evidence)
                grant = await self._grant(
                    conn, row, disposition, stage, create_child=False
                )
                if (
                    evidence != grant["completion"]
                    or type(evidence.get("version")) is not int
                ):
                    raise VMCreationRetryConflict(
                        "creation_disposition_evidence_changed"
                    )
                if grant["operation"] == "purge_rootdisk":
                    child = await conn.fetchrow(
                        "SELECT completed_at,outcome FROM vm_workspace_cleanup_admissions WHERE id=$1",
                        UUID(grant["cleanup"]["admission_id"]),
                    )
                    if child["completed_at"] is None or child["outcome"] != "deleted":
                        raise VMCreationRetryConflict("creation_disposition_incomplete")
                progress = _json(row["cancellation_progress"])
                if stage in progress and progress[stage] != evidence:
                    raise VMCreationRetryConflict(
                        "creation_disposition_evidence_changed"
                    )
                if stage not in progress:
                    await conn.execute(
                        "UPDATE vm_creation_retries SET cancellation_progress=cancellation_progress || $2::jsonb,revision=revision+1,updated_at=clock_timestamp() WHERE request_id=$1",
                        row["request_id"],
                        json.dumps({stage: evidence}),
                    )
                completed = _json(row["cancellation_completion"])
                if stage in completed and completed[stage] != evidence:
                    raise VMCreationRetryConflict(
                        "creation_disposition_evidence_changed"
                    )
                if stage not in completed:
                    await conn.execute(
                        "UPDATE vm_creation_retries SET cancellation_completion=cancellation_completion || $2::jsonb,revision=revision+1,updated_at=clock_timestamp() WHERE request_id=$1",
                        row["request_id"],
                        json.dumps({stage: evidence}),
                    )
                return {"recorded": True, "stage": stage, "evidence": evidence}

    async def _record_source(self, conn, row, disposition, evidence):
        from shared.vm_creation_source_completion import validate_source_completion
        from shared.vm_creation_source_disposition import source_disposition_plan

        progress = _json(row["cancellation_progress"])
        plan = progress.get("source")
        try:
            if (
                plan is None
                or source_disposition_plan(
                    row, disposition, plan["source"], plan["target"]
                )
                != plan
            ):
                raise ValueError("Source intent no longer matches its authority")
            validate_source_completion(plan, evidence)
        except (ValueError, TypeError, KeyError) as exc:
            raise VMCreationRetryConflict(
                "creation_disposition_evidence_changed"
            ) from exc
        target = plan["target"]
        if target and target["kind"] == "rootdisk_purged":
            grant = await self._grant(
                conn, row, disposition, "rootdisk", create_child=False
            )
            child = await conn.fetchrow(
                "SELECT completed_at,outcome FROM vm_workspace_cleanup_admissions WHERE id=$1",
                UUID(grant["cleanup"]["admission_id"]),
            )
            if (
                target != grant["completion"]
                or progress.get("rootdisk") != target
                or child["completed_at"] is None
                or child["outcome"] != "deleted"
            ):
                raise VMCreationRetryConflict("creation_disposition_incomplete")
        elif target and target["kind"] == "rootdisk_completed":
            root = disposition["objects"].get("rootdisk")
            if (
                disposition["disk_policy"] != "retain"
                or root is None
                or any(
                    target[key] != root[key]
                    for key in ("name", "namespace", "uid", "pvc_uid")
                )
            ):
                raise VMCreationRetryConflict("creation_disposition_incomplete")
        elif target and target["kind"] == "rootdisk_never_issued":
            if (
                row["expected_pvc_uid"] is not None
                or row["observed_pvc_uid"] is not None
                or disposition["objects"].get("rootdisk") is not None
                or any(
                    e["effect_kind"] == "rootdisk" and e["state"] != "rejected"
                    for e in disposition["effects"]
                )
            ):
                raise VMCreationRetryConflict("creation_disposition_incomplete")
        completion = _json(row["cancellation_completion"])
        prior = completion.get("source")
        if prior is not None:
            validate_source_completion(plan, prior)
            # Accepted actual proof is durable; later GC or an unrelated source
            # RV does not replace it. The caller's full current proof still had
            # to validate against the same immutable plan above.
            return {"recorded": True, "stage": "source", "evidence": prior}
        await conn.execute(
            "UPDATE vm_creation_retries SET cancellation_completion=cancellation_completion || $2::jsonb,revision=revision+1,updated_at=clock_timestamp() WHERE request_id=$1",
            row["request_id"],
            json.dumps({"source": evidence}),
        )
        return {"recorded": True, "stage": "source", "evidence": evidence}

    async def _attachment_intent(self, conn, row, disposition, target):
        from shared.vm_creation_attachment_disposition import attachment_plan
        from orchestrator.services.vm_creation_attachment_store import (
            attachment_instance_on_conn,
        )

        if disposition["workspace_storage"] is not None:
            await attachment_instance_on_conn(conn, row, adoption=True)
        prior = _json(row["cancellation_progress"]).get("workspace_attachment")
        if prior is not None:
            return {"operation": "dispose_attachment", "plan": prior}
        try:
            plan = attachment_plan(
                row, disposition, _json(row["cancellation_completion"]), target
            )
        except (ValueError, TypeError, KeyError, StopIteration) as exc:
            raise VMCreationRetryConflict(
                "creation_disposition_attachment_unproven"
            ) from exc
        await conn.execute(
            "UPDATE vm_creation_retries SET cancellation_progress=cancellation_progress || $2::jsonb,revision=revision+1,updated_at=clock_timestamp() WHERE request_id=$1",
            row["request_id"],
            json.dumps({"workspace_attachment": plan}),
        )
        return {"operation": "dispose_attachment", "plan": plan}

    async def _record_attachment(self, conn, row, disposition, evidence):
        from shared.vm_creation_attachment_disposition import attachment_completion
        from orchestrator.services.vm_creation_attachment_store import (
            attachment_instance_on_conn,
        )

        if disposition["workspace_storage"] is not None:
            await attachment_instance_on_conn(conn, row, adoption=True)
        plan = _json(row["cancellation_progress"]).get("workspace_attachment")
        if plan is None:
            raise VMCreationRetryConflict("creation_disposition_attachment_unproven")
        try:
            actual = attachment_completion(plan, evidence)
        except (ValueError, TypeError, KeyError) as exc:
            raise VMCreationRetryConflict(
                "creation_disposition_evidence_changed"
            ) from exc
        prior = _json(row["cancellation_completion"]).get("workspace_attachment")
        if prior is not None:
            if prior != actual:
                raise VMCreationRetryConflict("creation_disposition_evidence_changed")
        else:
            await conn.execute(
                "UPDATE vm_creation_retries SET cancellation_completion=cancellation_completion || $2::jsonb,revision=revision+1,updated_at=clock_timestamp() WHERE request_id=$1",
                row["request_id"],
                json.dumps({"workspace_attachment": actual}),
            )
        return {"recorded": True, "evidence": actual}

    async def settle(self, *, request_id: str, carrier: dict) -> dict:
        """Commit exact instance, original admission and no-VM retry together."""
        from orchestrator.services.vm_creation_attachment_store import attachment_instance_on_conn

        async with self.db.acquire() as conn:
            async with conn.transaction():
                row, _ = await self.retries._effect_scope(conn, request_id)
                values = self.retries._carrier(carrier)
                await self.retries._check_carrier(conn, row, carrier, values, allow_completed=True)
                if row["state"] == "settled":
                    if row["reason"] != "creation_disposed":
                        raise VMCreationRetryConflict("creation_already_settled")
                    return {"settled": True, "disposition": "creation_disposed"}
                row, disposition = await self._locked(conn, request_id, carrier)
                if not await conn.fetchval(
                    "SELECT public.valid_vm_creation_disposition_evidence(r) FROM vm_creation_retries r WHERE request_id=$1",
                    row["request_id"],
                ):
                    raise VMCreationRetryConflict("creation_disposition_incomplete")
                completed = _json(row["cancellation_completion"])
                attachment = completed["workspace_attachment"]
                if disposition["workspace_storage"] is not None:
                    instance = await attachment_instance_on_conn(conn, row, adoption=True)
                    receipt = {
                        "version": 1,
                        "kind": "retained_creation_disposition",
                        "request_id": str(row["request_id"]),
                        "disposition_id": disposition["disposition_id"],
                        "provision_generation": str(row["provision_generation"]),
                        "execution_id": str(row["execution_id"]),
                        "attachment": attachment,
                        "rootdisk": completed["rootdisk"],
                    }
                    pvc = completed["rootdisk"].get("pvc_uid")
                    await conn.execute(
                        "UPDATE srw_workspace_instances SET status=$2,execution_id=NULL,pvc_uid=$3,"
                        "backend_state=backend_state || jsonb_build_object('retained_creation_disposition',$4::jsonb),updated_at=clock_timestamp() WHERE id=$1",
                        instance["id"],
                        "Detached" if attachment["outcome"] == "detached" else "Released",
                        pvc, json.dumps(receipt),
                    )
                await conn.execute(
                    "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),outcome='creation_disposed' WHERE id=$1",
                    row["creation_admission_id"],
                )
                await conn.execute(
                    "UPDATE vm_creation_retries SET state='settled',reason='creation_disposed',revision=revision+1,claim_token=NULL,claim_expires_at=NULL,resolved_at=clock_timestamp(),updated_at=clock_timestamp() WHERE request_id=$1",
                    row["request_id"],
                )
                return {"settled": True, "disposition": "creation_disposed"}
