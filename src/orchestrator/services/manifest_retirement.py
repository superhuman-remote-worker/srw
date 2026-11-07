"""Retire Project desired state without losing live execution authority."""

import json
from fastapi import HTTPException

from orchestrator.services.manifest_execution_retirement import (
    execution_references_block_retirement,
    lock_manifest_execution_catalog,
)


async def _assert_no_expert_pointers(conn, resources, *, deleting_project=None):
    expert_ids = [
        row["linked_id"]
        for row in resources
        if row["kind"] == "Expert" and row.get("linked_id")
    ]
    if not expert_ids:
        return
    if await conn.fetchval(
        """SELECT
          EXISTS(SELECT 1 FROM experts WHERE id=ANY($1::uuid[]) AND managed_key IS NOT NULL)
          OR EXISTS(SELECT 1 FROM application_expert_defaults WHERE expert_id=ANY($1::uuid[]))
          OR EXISTS(SELECT 1 FROM user_expert_defaults WHERE expert_id=ANY($1::uuid[]))
          OR EXISTS(SELECT 1 FROM automations WHERE expert_id=ANY($1::uuid[]))
          OR EXISTS(SELECT 1 FROM project_experts WHERE expert_id=ANY($1::uuid[])
            AND default_for IS NOT NULL AND ($2::uuid IS NULL OR project_id<>$2))
          OR EXISTS(SELECT 1 FROM threads WHERE status<>'ended' AND metadata->>'expert_id'=ANY($3::text[]))
          OR EXISTS(SELECT 1 FROM jobs WHERE expert_id=ANY($1::uuid[]) AND status IN ('created','waiting'))""",
        expert_ids,
        deleting_project,
        [str(value) for value in expert_ids],
    ):
        raise HTTPException(
            409,
            "Repoint active Expert references and defaults before removing these definitions.",
        )


async def retire_removed_children(db, manager_id, retained_ids):
    """The parent's version owns removal as well as updates of inline children.

    Call after saving the complete candidate and updating its domain defaults,
    inside the same catalog transaction. Any blocker rolls the generation back.
    """
    rows = await db.fetch(
        "SELECT * FROM srw_resources WHERE managed_by=$1 AND deleted_at IS NULL AND NOT(id=ANY($2::uuid[])) FOR UPDATE",
        manager_id,
        retained_ids,
    )
    if not rows:
        return
    ids = [row["id"] for row in rows]
    text_ids = [str(value) for value in ids]
    await _assert_no_expert_pointers(db, rows)
    if await execution_references_block_retirement(
        db, resource_ids=ids, dependency_ids=text_ids
    ):
        raise HTTPException(
            409, "Removed Project definitions are referenced by unfinished work."
        )
    if await db.fetchval(
        """SELECT EXISTS(SELECT 1 FROM srw_resources r WHERE r.deleted_at IS NULL
        AND NOT(r.id=ANY($1::uuid[])) AND (r.managed_by=ANY($1::uuid[]) OR EXISTS(
          SELECT 1 FROM jsonb_array_elements(r.dependencies) d WHERE d->>'uid'=ANY($2::text[]))))""",
        ids,
        text_ids,
    ):
        raise HTTPException(
            409, "A saved resource still references a removed Project definition."
        )
    await db.execute(
        "UPDATE srw_resources SET deleted_at=now(),updated_at=now(),resource_version=resource_version+1 WHERE id=ANY($1::uuid[])",
        ids,
    )


async def assert_project_retirable(conn, project_id):
    """Raise the 409 that refuses the Project's deletion; change nothing.

    Runs in the caller's transaction and holds its locks until that ends. The
    project delete also runs it before it deletes anything outside the
    database, so a refused delete leaves the Project whole.
    """
    # The same lock orders legacy Project deletion against native apply.
    await lock_manifest_execution_catalog(conn)
    resources = await conn.fetch(
        "SELECT id,kind,linked_id FROM srw_resources WHERE project_id=$1 AND deleted_at IS NULL FOR UPDATE",
        project_id,
    )
    ids = [row["id"] for row in resources]
    await _assert_no_expert_pointers(conn, resources, deleting_project=project_id)
    active = await execution_references_block_retirement(
        conn,
        project_id=project_id,
        resource_ids=ids,
        dependency_ids=[str(value) for value in ids],
    )
    if active:
        raise HTTPException(
            409,
            "Project resources are referenced by unfinished work or resumable sessions.",
        )
    if await conn.fetchval(
        "SELECT EXISTS(SELECT 1 FROM srw_workspace_instances WHERE project_id=$1 AND (status<>'Released' OR execution_id IS NOT NULL))",
        project_id,
    ):
        raise HTTPException(
            409, "Release retained workspace instances before deleting the Project."
        )
    for resource_id in ids:
        if await conn.fetchval(
            """SELECT EXISTS(SELECT 1 FROM srw_resources WHERE deleted_at IS NULL AND NOT(id=ANY($1::uuid[]))
            AND (managed_by=$2 OR dependencies @> $3::jsonb))""",
            ids,
            resource_id,
            json.dumps([{"uid": str(resource_id)}]),
        ):
            raise HTTPException(
                409,
                "A saved resource outside this Project still references its definitions.",
            )


async def retire_project_resources(conn, project_id):
    await assert_project_retirable(conn, project_id)
    await conn.execute(
        """UPDATE srw_resources SET deleted_at=COALESCE(deleted_at,now()),project_id=NULL,
        updated_at=now(),resource_version=resource_version+1 WHERE project_id=$1""",
        project_id,
    )
    await conn.execute(
        "UPDATE srw_workspace_instances SET project_id=NULL WHERE project_id=$1",
        project_id,
    )
    await conn.execute(
        "DELETE FROM srw_resource_secrets WHERE scope_kind='Project' AND scope_name=$1",
        str(project_id),
    )
