"""Adopt historical sessions only when delivering a newly resolved attachment."""

from copy import deepcopy
from uuid import UUID

from fastapi import HTTPException

from shared.permission_modes import DEFAULT_PERMISSION_MODE
from orchestrator.services.manifest_execution_snapshot import (
    object_value,
    rendered_srw_snapshot,
)
from orchestrator.services.manifest_store import ManifestStore
from orchestrator.services.manifest_runtime_ownership import (
    require_srw_expert_configuration,
)
from shared.manifests.resolution import content_revision


def prepare_delivery_snapshot(
    thread, metadata, blob, policy, *, image, config_name, expert=None, refs=None
):
    """Capture the exact authorized resolver output, before credential injection."""
    require_srw_expert_configuration(expert, interactive=True, trusted_image=image)
    dependencies = []
    if (expert or {}).get("project_dependency"):
        dependencies.append(deepcopy(expert["project_dependency"]))
    for row in [expert, *(refs or {}).values()]:
        if row and row.get("harness_adapter") == "srw/v1":
            require_srw_expert_configuration(row, trusted_image=image)
        if row and row.get("manifest_uid"):
            dependency = {
                "uid": row["manifest_uid"],
                "revision": row["manifest_revision"],
            }
            if dependency not in dependencies:
                dependencies.append(dependency)
    selection = object_value(metadata.get("datasource_selection"))
    prepared = rendered_srw_snapshot(
        blob,
        policy,
        work_kind="Session",
        work_id=str(thread["id"]),
        owner_id=str(thread["user_id"]) if thread.get("user_id") else None,
        project_ids=[str(thread["project_id"])] if thread.get("project_id") else [],
        config_name=config_name,
        description=thread.get("title") or "Session attachment",
        datasource_ids=selection.get(
            "datasource_ids", metadata.get("datasource_ids", [])
        )
        or [],
        policy_revisions=selection.get("policy_revisions", {}),
        image=image,
        dependencies=dependencies,
        asset_name=(expert or {}).get("harness_asset_name"),
    )
    for key in ("document", "resolved"):
        prepared[key]["metadata"]["annotations"]["srw.io/import-source"] = (
            "first-manifest-attachment"
        )
    return prepared


async def capture_session_delivery(db, thread, resolved, status, *, project_ids):
    """Commit before delivery; a read-only preflight never calls this function.

    Both delivery callers already hold the datasource lock. The configuration
    transaction is reentrant in that task and serializes with Session PATCH.
    A concurrent canonical generation must be re-resolved, never relabeled.
    """
    candidate = status.get("_manifest_snapshot")
    if candidate is None or resolved is None or resolved.get("execution_snapshot"):
        return resolved
    prepared = deepcopy(candidate)
    # Materialized controls are overlaid by delivery. Store that same state in
    # both the hydration blob and the permission policy, without live bindings.
    controls = {
        "permission_mode": str(thread.get("permission_mode") or DEFAULT_PERMISSION_MODE)
    }
    if thread.get("narration_mode") is not None:
        controls["narration_mode"] = str(thread["narration_mode"])
    for key in ("document", "resolved"):
        private = prepared[key]["spec"]["execution"]["expert"]["inline"]["runtime"][
            "config"
        ]
        for fragment in (private["resolved"]["agent"], private["policy"]):
            fragment["interactive"] = {**fragment.get("interactive", {}), **controls}
    prepared["revision"] = content_revision(prepared["resolved"]["spec"])
    async with db.thread_configuration_transaction(str(thread["id"])) as conn:
        current = await conn.fetchrow(
            "SELECT * FROM threads WHERE id=$1 FOR UPDATE", UUID(str(thread["id"]))
        )
        if current is None:
            raise HTTPException(
                409, "Session disappeared before configuration admission."
            )
        source_metadata = object_value(thread.get("metadata"))
        current_metadata = object_value(current.get("metadata"))
        columns = (
            "user_id",
            "project_id",
            "config_name",
            "permission_mode",
            "narration_mode",
        )
        selections = (
            "expert_id",
            "config_override",
            "datasource_ids",
            "datasource_selection",
        )
        if any(current.get(key) != thread.get(key) for key in columns) or any(
            current_metadata.get(key) != source_metadata.get(key) for key in selections
        ):
            raise HTTPException(
                409, "Session settings changed; prepare the attachment again."
            )
        store = ManifestStore(db)
        if await store.execution("Session", str(thread["id"])):
            raise HTTPException(
                409, "Session configuration changed; prepare the attachment again."
            )
        execution = await store.freeze_execution(
            work_kind="Session",
            work_id=str(thread["id"]),
            owner_id=str(thread["user_id"]) if thread.get("user_id") else None,
            project_ids=project_ids,
            conn=conn,
            **prepared,
        )
    delivered = deepcopy(resolved)
    delivered["agent"]["interactive"] = {
        **delivered["agent"].get("interactive", {}),
        **controls,
    }
    delivered["execution_snapshot"] = {
        "id": str(execution["id"]),
        "generation": execution["generation"],
        "revision": execution["revision"],
    }
    return delivered
