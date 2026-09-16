"""Manifest ownership and exact disk handoff for retained SRW Job workspaces."""

from copy import deepcopy
import json
import os
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from fastapi import HTTPException

from orchestrator.services.manifest_authority import ManifestAuthority
from orchestrator.services.manifest_store import ManifestStore
from shared.manifests.resolution import content_revision
from shared.vm_workspace_storage import storage_binding, storage_name


def object_value(value):
    return json.loads(value) if isinstance(value, str) else dict(value or {})


def require_retained_vm_hosting():
    from orchestrator.services.vm_provisioner import vm_persistent_rootdisk_enabled

    if (
        os.environ.get("VM_MODE") != "same-cluster"
        or not vm_persistent_rootdisk_enabled()
    ):
        raise HTTPException(
            422,
            "Retained VM workspaces require same-cluster hosting with persistent rootdisks.",
        )


async def read_instance(db, uid, user, *, project_id, request=None, detached=True):
    try:
        uid = UUID(str(uid))
    except ValueError:
        raise HTTPException(
            422, "Workspace instance references require a UUID."
        ) from None
    row = await db.fetchrow("SELECT * FROM srw_workspace_instances WHERE id=$1", uid)
    if not row:
        raise HTTPException(404, "Workspace instance does not exist.")
    row = dict(row)
    authority = ManifestAuthority(db, user, request=request)
    scope = (
        {"kind": "Project", "name": str(row["project_id"])}
        if row["project_id"]
        else {"kind": "Account", "name": str(row["owner_id"])}
    )
    await authority.scope(scope, write=True)
    if str(row["project_id"] or "") != str(project_id or "") or (
        not project_id and str(row["owner_id"]) != str(user["id"])
    ):
        raise HTTPException(
            403, "Retained VM reuse requires the same Account or Project scope."
        )
    row["recipe"] = object_value(row["recipe"])
    if (
        row["recipe"].get("backend") != "vm"
        or row["recipe"].get("retention") != "Retain"
    ):
        raise HTTPException(422, "The SRW VM adapter requires a retained VM instance.")
    if detached and (row["execution_id"] is not None or row["status"] != "Detached"):
        raise HTTPException(
            409, "Workspace instance is still attached or awaiting process fencing."
        )
    return row


async def reserve(db, snapshot):
    """Called only inside the execution's insertion transaction."""
    if snapshot["harness_adapter"] != "srw/v1":
        return
    workspace = snapshot["resolved"]["spec"]["execution"]["workspace"]
    recipe = (workspace or {}).get("template", {}).get("inline", {})
    if not workspace or (
        "instanceRef" not in workspace and recipe.get("retention") != "Retain"
    ):
        return
    require_retained_vm_hosting()
    if snapshot["work_kind"] != "Job":
        raise HTTPException(
            422,
            "Retained VM instances currently support Jobs; Sessions retain their own workspace through suspend/resume.",
        )
    await ManifestStore(db).lock_catalog()
    existing = await db.fetchrow(
        "SELECT instance_id FROM srw_execution_workspace_bindings WHERE execution_id=$1",
        snapshot["id"],
    )
    if existing:
        return
    user = await db.get_user(str(snapshot["owner_id"]))
    if not user:
        raise HTTPException(409, "Workspace execution owner is unavailable.")
    scope = snapshot["resolved"]["metadata"]["scope"]
    project_id = scope["name"] if scope["kind"] == "Project" else None
    if "instanceRef" in workspace:
        uid = UUID(workspace["instanceRef"]["uid"])
        await db.fetchrow(
            "SELECT id FROM srw_workspace_instances WHERE id=$1 FOR UPDATE", uid
        )
        row = await read_instance(db, uid, user, project_id=project_id)
        recipe = row["recipe"]
        from orchestrator.services.manifest_workspace_selection import (
            srw_workspace_config,
        )
        from orchestrator.services.manifest_execution_snapshot import (
            srw_snapshot_config,
        )

        _, policy = srw_snapshot_config(snapshot)
        expected = srw_workspace_config(workspace, instance_recipe=recipe)
        if any(
            (policy.get("workspace") or {}).get(key) != value
            for key, value in expected.items()
        ):
            raise HTTPException(409, "Workspace recipe changed during admission.")
        binding = storage_binding(object_value(row["backend_state"])["storage"])
        binding.update(generation=row["generation"] + 1, pvc_uid=row["pvc_uid"])
        binding = storage_binding(binding)
        await db.execute(
            """UPDATE srw_workspace_instances SET execution_id=$2, generation=$3,
            status='Reserved', backend_state=jsonb_set(backend_state,'{storage}',$4::jsonb), updated_at=now() WHERE id=$1""",
            uid,
            snapshot["id"],
            binding["generation"],
            json.dumps(binding),
        )
    else:
        if recipe.get("backend") != "vm":
            raise HTTPException(422, "The SRW adapter retains VM workspaces only.")
        uid = uuid4()
        binding = storage_binding(
            {
                "uid": str(uid),
                "generation": 1,
                "pvc_uid": None,
                "owner_id": str(snapshot["work_id"]),
                "owner_kind": "job",
            }
        )
        await db.execute(
            """INSERT INTO srw_workspace_instances
            (id, owner_id, project_id, recipe, revision, pvc_name, execution_id, generation, backend_state)
            VALUES($1,$2,$3,$4::jsonb,$5,$6,$7,1,$8::jsonb)""",
            uid,
            UUID(str(user["id"])),
            UUID(project_id) if project_id else None,
            json.dumps(recipe),
            content_revision(recipe),
            storage_name(binding),
            snapshot["id"],
            json.dumps({"storage": binding}),
        )
    await db.execute(
        "INSERT INTO srw_execution_workspace_bindings(execution_id,instance_id) VALUES($1,$2)",
        snapshot["id"],
        uid,
    )


async def provision_binding(db, job_id):
    row = await db.fetchrow(
        """SELECT i.*,s.id AS requested_execution FROM srw_execution_specs s
        JOIN srw_execution_workspace_bindings b ON b.execution_id=s.id
        JOIN srw_workspace_instances i ON i.id=b.instance_id
        WHERE s.work_kind='Job' AND s.work_id=$1""",
        UUID(str(job_id)),
    )
    if not row or object_value(row["recipe"]).get("backend") != "vm":
        return None
    if row["execution_id"] != row["requested_execution"] or row["status"] not in {
        "Reserved",
        "Attached",
    }:
        raise HTTPException(409, "This Job no longer owns its retained workspace.")
    binding = deepcopy(object_value(row["backend_state"])["storage"])
    binding["pvc_uid"] = row["pvc_uid"]
    return storage_binding(binding)


async def record_created(db, job_id, binding, pvc_uid, *, namespace=None):
    """Only an authenticated controller response supplies the storage identity."""
    if not pvc_uid:
        return
    await db.execute(
        """UPDATE srw_workspace_instances i SET pvc_uid=$4,status='Attached',updated_at=now(),
        backend_state=i.backend_state || CASE WHEN $5::text IS NULL THEN '{}'::jsonb ELSE jsonb_build_object('namespace',$5::text) END
        FROM srw_execution_specs s WHERE i.execution_id=s.id AND s.work_kind='Job'
        AND s.work_id=$1 AND i.id=$2 AND i.generation=$3 AND i.status IN ('Reserved','Attached')
        AND (i.pvc_uid IS NULL OR i.pvc_uid=$4)
        AND (i.backend_state->>'namespace' IS NULL OR i.backend_state->>'namespace'=$5)""",
        UUID(str(job_id)),
        UUID(binding["uid"]),
        binding["generation"],
        pvc_uid,
        namespace,
    )


async def record_detached(db, job_id, binding):
    """Called after process retirement AND exact VM/VMI/launcher absence."""
    await db.execute(
        """UPDATE srw_workspace_instances i SET execution_id=NULL,status='Detached',
        initialized=i.initialized OR COALESCE(j.context->'vm'->'initialization_receipt'->>'phase','')='Succeeded',updated_at=now()
        FROM srw_execution_specs s JOIN jobs j ON s.work_kind='Job' AND s.work_id=j.id
        WHERE i.execution_id=s.id AND s.work_id=$1 AND i.id=$2 AND i.generation=$3
        AND i.status IN ('Reserved','Attached') AND i.pvc_uid IS NOT NULL AND j.status IN ('completed','failed','cancelled')
        AND NOT EXISTS (SELECT 1 FROM vm_workspace_recovery_jobs wrj
            WHERE wrj.job_id=j.id AND wrj.resolved_at IS NULL)""",
        UUID(str(job_id)),
        UUID(binding["uid"]),
        binding["generation"],
    )


async def guest_attachment_is_current(db, job_id, value):
    binding = storage_binding(value)
    return bool(
        await db.fetchval(
            """SELECT EXISTS(SELECT 1 FROM srw_workspace_instances i
        JOIN srw_execution_specs s ON s.id=i.execution_id
        WHERE s.work_kind='Job' AND s.work_id=$1 AND i.id=$2 AND i.generation=$3
        AND i.status IN ('Reserved','Attached'))""",
            UUID(str(job_id)),
            UUID(binding["uid"]),
            binding["generation"],
        )
    )


async def reconcile_detached(db, provisioner):
    """Finish a handoff when Job completion followed its VM teardown.

    This never kills a VM or substitutes a missing response for process-zero.
    Normal execution lifecycle code remains responsible for retiring work.
    """
    rows = await db.fetch(
        """SELECT j.id, j.context FROM srw_workspace_instances i
        JOIN srw_execution_specs s ON s.id=i.execution_id
        JOIN jobs j ON s.work_kind='Job' AND s.work_id=j.id
        WHERE i.recipe->>'backend'='vm' AND i.recipe->>'retention'='Retain'
        AND i.status IN ('Reserved','Attached') AND i.pvc_uid IS NOT NULL AND j.status IN ('completed','failed','cancelled')
        AND NOT EXISTS (SELECT 1 FROM vm_workspace_recovery_jobs wrj
            WHERE wrj.job_id=j.id AND wrj.resolved_at IS NULL)
        ORDER BY i.updated_at LIMIT 20"""
    )
    for row in rows:
        vm = object_value(object_value(row["context"]).get("vm"))
        binding = vm.get("workspace_storage")
        generation = vm.get("provision_generation")
        if binding is None or generation is None:
            continue
        if not await db.managed_repository_workspace_process_zero_is_current(
            str(row["id"]),
            owner_kind="job",
            scope="vm",
            provisioner="vm",
            runtime_incarnation=generation,
        ):
            continue
        probe = await provisioner._probe_vm_teardown_identity(
            str(row["id"]), generation
        )
        if probe.disposition != "absent" or not probe.rootdisk_identity_known:
            continue
        current = await provisioner._storage_context(str(row["id"]))
        if (
            current
            and probe.identity
            and probe.identity.rootdisk_pvc_uid == current["pvc_uid"]
        ):
            from orchestrator.services.vm_workspace_recovery_store import (
                VMWorkspaceRecoveryStore,
            )

            try:
                pvc_uid = UUID(str(current["pvc_uid"]))
            except (TypeError, ValueError, AttributeError):
                continue
            recovery_store = VMWorkspaceRecoveryStore(db)
            cleanup = await recovery_store.acquire_cleanup_permit(
                owner_kind="job",
                owner_id=UUID(str(row["id"])),
                pvc_uid=pvc_uid,
                request_id=uuid5(
                    NAMESPACE_URL,
                    f"retained-workspace-detach:{row['id']}:{generation}:{pvc_uid}",
                ),
                source="retained_workspace_detach",
            )
            if not cleanup.allowed:
                continue
            await provisioner._record_retained_detach(str(row["id"]), current)
            if cleanup.admission_id is not None:
                await recovery_store.complete_cleanup_permit(
                    cleanup.admission_id, outcome="retained_workspace_detached"
                )
