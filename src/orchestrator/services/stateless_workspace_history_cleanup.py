"""Complete exact historical workspace receipts before permanent Session deletion."""

from __future__ import annotations

import json
from uuid import UUID

from orchestrator.database.postgres import PostgresDB
from orchestrator.services.container_provisioner import (
    ContainerProvisioner,
    WorkspaceRuntimeAuthorityError,
    WorkspaceTeardownIdentity,
)
from orchestrator.services.workspace_lifecycle import WorkspaceOwner


async def reclaim_stateless_workspace_history(
    db: PostgresDB, provisioner: ContainerProvisioner, thread_id: str
) -> bool:
    """Append terminal cleanup proof for previously preserved exact Pod UIDs.

    Soft End settles a preserve intent before Resume rotates the generation.
    Its process-zero receipt remains authoritative, but cannot authorize owner
    deletion. After the current permanent reclaim has completed, verify the
    captured Pod, seed, PVC and Service names are all absent and append a separate
    terminal intent for each preserved runtime. Keep every original receipt,
    captured UID and the current workspace projection unchanged.

    The physical mutation guard and owner/queue row locks span the bounded,
    read-only Kubernetes capture. No external deletion or new process-zero
    claim is made here. Unknown historical locations, changed namespaces and
    other incomplete evidence stay retryable.
    """

    owner_id = UUID(thread_id)
    async with db.workspace_runtime_mutation_lock(
        thread_id, owner_kind="thread", scope="workspace_container"
    ) as acquired:
        if not acquired:
            return False
        async with db.acquire() as conn:
            async with conn.transaction():
                thread = await conn.fetchrow(
                    "SELECT status::text AS status, execution_lane, "
                    "runtime_generation, metadata FROM threads "
                    "WHERE id=$1 FOR UPDATE",
                    owner_id,
                )
                if thread is None:
                    return True
                metadata = thread["metadata"]
                if isinstance(metadata, str):
                    metadata = json.loads(metadata)
                if (
                    thread["status"] != "ended"
                    or thread["execution_lane"] != "stateless"
                    or not isinstance(metadata, dict)
                ):
                    return False
                if await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM "
                    "managed_repository_workspace_cleanup_intents "
                    "WHERE owner_kind='thread' AND owner_id=$1 AND settled_at IS NULL) "
                    "OR EXISTS(SELECT 1 FROM "
                    "managed_repository_workspace_creation_reservations "
                    "WHERE owner_kind='thread' AND owner_id=$1 AND settled_at IS NULL)",
                    owner_id,
                ):
                    return False
                runtimes = await conn.fetch(
                    "SELECT DISTINCT r.runtime_incarnation FROM "
                    "managed_repository_process_zero_receipts r "
                    "WHERE r.owner_kind='thread' AND r.owner_id=$1 "
                    "AND r.provisioner='k8s' "
                    "AND r.scope IN ('workspace_container','stateless_workspace') "
                    "AND NOT EXISTS(SELECT 1 FROM "
                    "managed_repository_workspace_cleanup_intents i "
                    "WHERE i.owner_kind=r.owner_kind AND i.owner_id=r.owner_id "
                    "AND i.scope='workspace_container' "
                    "AND i.runtime_incarnation::text=r.runtime_incarnation "
                    "AND i.resource_policy='terminal_reclaim' "
                    "AND i.target_disposition='deleted' "
                    "AND i.result_kind='settled' AND i.settled_at IS NOT NULL "
                    "AND i.cleanup_completed_at IS NOT NULL)",
                    owner_id,
                )
                if not runtimes:
                    return True
                terminal_token = await db._lock_terminal_workspace_reclaim_authority(
                    conn,
                    owner_kind="thread",
                    owner_id=owner_id,
                    owner_status=thread["status"],
                    owner_state=metadata,
                )
                if terminal_token is None:
                    return False
                workspace = metadata.get("workspace_container")
                if not isinstance(workspace, dict) or (
                    workspace.get("provisioner") != "k8s"
                    or workspace.get("status") != "deleted"
                ):
                    return False
                location = provisioner.workspace_cleanup_location(
                    WorkspaceOwner.session(thread_id)
                )
                current = await conn.fetchrow(
                    "SELECT * FROM managed_repository_workspace_cleanup_intents "
                    "WHERE owner_kind='thread' AND owner_id=$1 "
                    "AND thread_runtime_generation=$2 AND scope='workspace_container' "
                    "AND runtime_incarnation::text=$3 AND terminal_queue_token=$4 "
                    "AND resource_policy='terminal_reclaim' "
                    "AND reclaim_shared_resources AND target_disposition='deleted' "
                    "AND capture_complete AND resources_captured_at IS NOT NULL "
                    "AND result_kind='settled' AND settled_at IS NOT NULL "
                    "AND cleanup_completed_at IS NOT NULL "
                    "ORDER BY intent_generation DESC LIMIT 1 FOR UPDATE",
                    owner_id,
                    thread["runtime_generation"],
                    workspace.get("_runtime_incarnation"),
                    terminal_token,
                )
                if current is not None:
                    current_location = current["resource_location"]
                    if isinstance(current_location, str):
                        current_location = json.loads(current_location)
                    if current_location != location:
                        return False
                    reclaimed_by = {"terminal_reclaim_intent_id": str(current["id"])}
                else:
                    reclaimed_by = await _refused_resume_reclaim(
                        conn, owner_id, metadata, location
                    )
                    if reclaimed_by is None:
                        return False
                preserved = []
                for runtime in runtimes:
                    source = await conn.fetchrow(
                        "SELECT i.* FROM managed_repository_workspace_cleanup_intents i "
                        "WHERE i.owner_kind='thread' AND i.owner_id=$1 "
                        "AND i.scope='workspace_container' "
                        "AND i.runtime_incarnation::text=$2 "
                        "AND i.pod_uid=i.runtime_incarnation "
                        "AND i.resource_policy='preserve' "
                        "AND i.target_disposition IN ('deleted','suspended') "
                        "AND i.capture_complete AND i.resources_captured_at IS NOT NULL "
                        "AND i.result_kind='settled' AND i.settled_at IS NOT NULL "
                        "AND i.cleanup_completed_at IS NOT NULL "
                        "AND EXISTS(SELECT 1 FROM "
                        "managed_repository_process_zero_receipts r "
                        "WHERE r.owner_kind=i.owner_kind AND r.owner_id=i.owner_id "
                        "AND r.scope='workspace_container' AND r.provisioner='k8s' "
                        "AND r.runtime_incarnation=i.runtime_incarnation::text) "
                        "ORDER BY i.intent_generation DESC LIMIT 1 FOR UPDATE",
                        owner_id,
                        runtime["runtime_incarnation"],
                    )
                    if source is None:
                        return False
                    source_location = source["resource_location"]
                    if isinstance(source_location, str):
                        source_location = json.loads(source_location)
                    if source_location != location:
                        return False
                    preserved.append(source)

                try:
                    absence = await provisioner.capture_workspace_teardown_identity(
                        WorkspaceOwner.session(thread_id)
                    )
                except WorkspaceRuntimeAuthorityError:
                    return False
                if not isinstance(absence, WorkspaceTeardownIdentity) or any(
                    value is not None
                    for value in (
                        absence.pod_uid,
                        absence.seed_configmap_uid,
                        absence.pvc_uid,
                        absence.service_uid,
                    )
                ):
                    return False

                for source in preserved:
                    # Admission belongs to the current permanent generation;
                    # the old generation and captured resources remain linked
                    # to their unchanged source intent in the audit record.
                    intent_id = await conn.fetchval(
                        "INSERT INTO managed_repository_workspace_cleanup_intents ("
                        "owner_kind,owner_id,thread_runtime_generation,scope,"
                        "runtime_incarnation,intent_source,admission_source,"
                        "target_disposition,resource_policy,reclaim_shared_resources,"
                        "lifecycle_fingerprint,terminal_queue_token,pod_uid,"
                        "seed_configmap_uid,pvc_uid,service_uid,capture_complete,"
                        "resources_captured_at,phase,resource_location) VALUES("
                        "'thread',$1,$2,'workspace_container',$3,'historical','explicit',"
                        "'deleted','terminal_reclaim',TRUE,$4::jsonb,$5,$3,"
                        "$6,$7,$8,TRUE,now(),'captured',$9::jsonb) RETURNING id",
                        owner_id,
                        thread["runtime_generation"],
                        source["runtime_incarnation"],
                        json.dumps(
                            {
                                "admitted_by": "terminal_history_absence",
                                "preserve_intent_id": str(source["id"]),
                                "retired_thread_runtime_generation": str(
                                    source["thread_runtime_generation"]
                                ),
                                **reclaimed_by,
                            }
                        ),
                        terminal_token,
                        source["seed_configmap_uid"],
                        source["pvc_uid"],
                        source["service_uid"],
                        json.dumps(location),
                    )
                    # The existing trigger stamps projection_transaction_id.
                    # No thread projection is changed for a historical UID.
                    await conn.execute(
                        "UPDATE managed_repository_workspace_cleanup_intents "
                        "SET cleanup_completed_at=now(),settled_at=now(),"
                        "result_kind='settled',phase='settled' WHERE id=$1",
                        intent_id,
                    )
                return True


async def _refused_resume_reclaim(conn, owner_id, metadata, location):
    """The creation that reclaimed a Resumed Session's kept PVC, if any.

    When the cluster refused a Resumed Session's Pod, no current runtime ever
    existed: permanent End's cancellation of that creation (or the reclaim
    after a soft End) removed the PVC the earlier runtime left behind.
    """
    from shared.session_retirement import unpublished_creation_retirement_authority

    marker = metadata.get("_stateless_claim_retirement") or metadata.get(
        "_stateless_workspace_retirement_settled"
    )
    if not isinstance(marker, dict) or "unpublished_creation" not in marker:
        return None
    try:
        unpublished = unpublished_creation_retirement_authority(
            marker["unpublished_creation"]
        )
    except RuntimeError:
        return None
    retained = unpublished.get("retained")
    if retained is None or unpublished["namespace"] != location["namespace"]:
        return None
    reclaimed = await conn.fetchval(
        "SELECT id FROM managed_repository_workspace_creation_reservations "
        "WHERE owner_kind='thread' AND owner_id=$1 AND scope='workspace_container' "
        "AND id=$2::uuid AND reservation_generation=("
        "SELECT max(reservation_generation) FROM "
        "managed_repository_workspace_creation_reservations "
        "WHERE owner_kind='thread' AND owner_id=$1 AND scope='workspace_container') "
        "AND operation_kind='create' AND result_kind='aborted' "
        "AND settled_at IS NOT NULL AND pod_uid IS NULL "
        "AND runtime_incarnation IS NULL AND pvc_uid=$3::uuid "
        "AND cancel_resource_policy='terminal_reclaim' "
        "AND cancel_cleanup_completed_at IS NOT NULL FOR UPDATE",
        owner_id,
        unpublished["reservation_id"],
        retained["pvc_uid"],
    )
    if reclaimed is None:
        return None
    return {"terminal_reclaim_creation_id": str(reclaimed)}
