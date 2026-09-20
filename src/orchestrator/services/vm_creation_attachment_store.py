"""Locked instance authority for the fixed retained-attachment creation stage."""

import json
from uuid import UUID

from shared.vm_workspace_storage import storage_binding, storage_name


def _object(value):
    return json.loads(value) if isinstance(value, str) else value


async def attachment_instance_on_conn(conn, row, *, adoption=False):
    """Compose after existing owner/PVC, queue/job, execution and retry locks.

    Inherited ownership additionally requires the immutable handoff proof and
    its separately acquired sorted owner/PVC scope.
    No catalog lock or external I/O is acquired here.
    """
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict

    binding = storage_binding(row["canonical_request"]["workspace_storage"])
    if binding["owner_kind"] != "job":
        raise VMCreationRetryConflict("creation_attachment_lineage_unproven")
    if binding["owner_id"] != str(row["job_id"]):
        from orchestrator.services.vm_creation_lineage import prove

        if row["canonical_request"].get("preparation") is not None:
            raise VMCreationRetryConflict("creation_attachment_lineage_unproven")
        await prove(
            conn,
            job_id=row["job_id"],
            binding=binding,
            expected=row["predecessor_evidence"],
            adoption=adoption and row["state"] == "cancel_requested",
        )
    link = await conn.fetchrow(
        "SELECT instance_id FROM srw_execution_workspace_bindings WHERE execution_id=$1 FOR SHARE",
        row["execution_id"],
    )
    if not link or link["instance_id"] != UUID(binding["uid"]):
        raise VMCreationRetryConflict("creation_attachment_authority_unproven")
    instance = await conn.fetchrow(
        "SELECT * FROM srw_workspace_instances WHERE id=$1 FOR UPDATE",
        link["instance_id"],
    )
    state = _object(instance["backend_state"]) if instance else {}
    recipe = _object(instance["recipe"]) if instance else {}
    expected_pvc = row["expected_pvc_uid"] or row["observed_pvc_uid"]
    expected_pvc = str(expected_pvc) if expected_pvc else None
    try:
        recorded = storage_binding(state.get("storage"))
    except (ValueError, TypeError):
        raise VMCreationRetryConflict("creation_attachment_binding_changed") from None
    # Legacy record_created captures the PVC in the instance column while its
    # original first-create backend binding remains nullable. Preserve every
    # other field and require both any recorded PVC and the column to agree.
    recorded_pvc = recorded["pvc_uid"]
    recorded["pvc_uid"] = binding["pvc_uid"]
    statuses = {"Reserved", "Attached"}
    if adoption and row["state"] == "cancel_requested":
        statuses.add("Deleting")
    if (
        not instance
        or instance["execution_id"] != row["execution_id"]
        or instance["generation"] != binding["generation"]
        or instance["status"] not in statuses
        or recipe.get("backend") != "vm"
        or recipe.get("retention") != "Retain"
        or recorded != binding
        or recorded_pvc not in (None, expected_pvc)
        or state.get("namespace")
        not in (None, row["controller_configuration"]["namespace"])
        or instance["pvc_name"] != storage_name(binding)
        or instance["pvc_uid"] not in (None, expected_pvc)
        or binding["pvc_uid"] is not None
        and instance["pvc_uid"] != binding["pvc_uid"]
    ):
        raise VMCreationRetryConflict("creation_attachment_binding_changed")
    return instance


async def record_attachment_adoption_on_conn(conn, row, *, pvc_uid, namespace):
    instance = await attachment_instance_on_conn(conn, row, adoption=True)
    await conn.execute(
        "UPDATE srw_workspace_instances SET pvc_uid=$2,status=CASE WHEN status='Deleting' THEN status ELSE 'Attached' END,"
        "backend_state=backend_state || jsonb_build_object('namespace',$3::text),updated_at=clock_timestamp() WHERE id=$1",
        instance["id"],
        pvc_uid,
        namespace,
    )
