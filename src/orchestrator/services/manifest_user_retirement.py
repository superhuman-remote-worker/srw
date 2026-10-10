"""Remove account authority while retaining manifest and execution history."""

from uuid import UUID

from fastapi import HTTPException

from orchestrator.services.manifest_execution_retirement import (
    execution_references_block_retirement,
    lock_manifest_execution_catalog,
)
from shared.connectors.platform import native_project_sql


async def retire_user_manifests(conn, user_id: UUID) -> bool:
    """Prepare the existing user DELETE in its transaction, without cloud effects.

    Account definitions retire with the account. Project definitions belong to
    their membership scope, so the creator's deletion does not retire them.
    Released workspace receipts and immutable execution revisions remain for
    history; live processes and retained storage must be released first.
    """
    await lock_manifest_execution_catalog(conn)
    if not await conn.fetchval("SELECT id FROM users WHERE id=$1 FOR UPDATE", user_id):
        return False

    resources = await conn.fetch(
        "SELECT * FROM srw_resources WHERE owner_id=$1 FOR UPDATE", user_id
    )
    await conn.fetch(
        "SELECT id FROM srw_execution_specs WHERE owner_id=$1 FOR UPDATE", user_id
    )
    workspaces = await conn.fetch(
        """SELECT * FROM srw_workspace_instances WHERE owner_id=$1
        OR execution_id IN (SELECT id FROM srw_execution_specs WHERE owner_id=$1)
        FOR UPDATE""",
        user_id,
    )
    if any(
        row["status"] != "Released" or row["execution_id"] or row["pod_uid"]
        for row in workspaces
    ):
        raise HTTPException(
            409,
            "Release this user's retained workspace instances and fence their processes before deleting the user.",
        )

    retired = [
        row["id"]
        for row in resources
        if row["scope_kind"] == "Account"
        and row["kind"] != "Project"
        and row["deleted_at"] is None
    ]
    retired_text = [str(value) for value in retired]
    if await execution_references_block_retirement(
        conn,
        owner_id=user_id,
        retiring_owner_id=user_id,
        resource_ids=retired,
        dependency_ids=retired_text,
    ):
        raise HTTPException(
            409,
            "Finish or cancel this user's manifest work, end its sessions, and fence remaining processes before deleting the user.",
        )
    if retired and await conn.fetchval(
        """SELECT EXISTS(SELECT 1 FROM srw_resources r WHERE r.deleted_at IS NULL
        AND NOT(r.id=ANY($1::uuid[])) AND (r.managed_by=ANY($1::uuid[]) OR EXISTS(
          SELECT 1 FROM jsonb_array_elements(r.dependencies) d
          WHERE d->>'uid'=ANY($2::text[]))))""",
        retired,
        retired_text,
    ):
        raise HTTPException(
            409,
            "A shared resource still references this account's definitions; repoint it before deleting the user.",
        )

    # A Project-scoped Expert's domain owner is bookkeeping for the existing
    # picker. Preserve its UUID/default links under an already-authorized owner.
    # Account Experts keep the legacy user DELETE cascade instead.
    for row in resources:
        if row["deleted_at"] is not None or row["kind"] != "Expert":
            continue
        if row["scope_kind"] == "Account" or not row["linked_id"]:
            continue
        successor = None
        if row["scope_kind"] == "Project":
            successor = await conn.fetchval(
                """SELECT user_id FROM project_members
                WHERE project_id=$1 AND role='owner' AND user_id<>$2
                ORDER BY added_at,user_id LIMIT 1 FOR SHARE""",
                row["project_id"],
                user_id,
            )
        if successor is None:
            raise HTTPException(
                409,
                "Transfer this user's shared Expert definitions to an existing authorized owner before deleting the user.",
            )
        await conn.execute(
            "UPDATE experts SET owner_id=$2 WHERE id=$1 AND owner_id=$3",
            row["linked_id"],
            successor,
            user_id,
        )
        await conn.execute(
            "UPDATE srw_resources SET owner_id=$2 WHERE id=$1", row["id"], successor
        )

    if await conn.fetchval(
        """SELECT EXISTS(SELECT 1 FROM experts e WHERE e.owner_id=$1 AND (
          EXISTS(SELECT 1 FROM application_expert_defaults d WHERE d.expert_id=e.id)
          OR EXISTS(SELECT 1 FROM user_expert_defaults d WHERE d.expert_id=e.id AND d.user_id<>$1)
          OR EXISTS(SELECT 1 FROM project_experts d WHERE d.expert_id=e.id
            AND d.default_for IS NOT NULL AND EXISTS(SELECT 1 FROM project_members m
              WHERE m.project_id=d.project_id AND m.user_id<>$1))
          OR EXISTS(SELECT 1 FROM automations a WHERE a.expert_id=e.id AND a.owner_id<>$1)))""",
        user_id,
    ):
        raise HTTPException(
            409,
            "Repoint shared Expert defaults and other users' automations before deleting the user.",
        )

    await conn.execute(
        """UPDATE srw_resources SET deleted_at=now(),updated_at=now(),
        resource_version=resource_version+1 WHERE id=ANY($1::uuid[])""",
        retired,
    )
    await conn.execute(
        "UPDATE srw_resources SET owner_id=NULL WHERE owner_id=$1", user_id
    )
    await conn.execute(
        "UPDATE srw_execution_specs SET owner_id=NULL WHERE owner_id=$1", user_id
    )
    await conn.execute(
        """UPDATE srw_workspace_instances SET owner_id=NULL,ssh_ciphertext=NULL,
        updated_at=now() WHERE owner_id=$1""",
        user_id,
    )
    await conn.execute("DELETE FROM srw_manifest_operations WHERE owner_id=$1", user_id)
    await conn.execute(
        "DELETE FROM srw_resource_secrets WHERE scope_kind='Account' AND scope_name=$1",
        str(user_id),
    )
    # An administrator may have created a secret in another live Account. Its
    # scope, rather than that administrator's provenance, determines ownership.
    await conn.execute(
        """UPDATE srw_resource_secrets SET owner_id=scope_name::uuid
        WHERE owner_id=$1 AND scope_kind='Account'""",
        user_id,
    )
    # The creator is provenance, not the authority for a Project-scoped secret.
    await conn.execute(
        "UPDATE srw_resource_secrets SET owner_id=NULL WHERE owner_id=$1", user_id
    )
    # A project's own connector (its knowledge base, its cloud folder) is
    # owned by its project, not by the user who happened to create that
    # project.  Its ``created_by`` column is audit provenance, but the
    # historical FK is restrictive.  Detach only the exact synthetic shape
    # that project deletion itself owns: one project named by its managed key
    # (or a knowledge base's older marker) with one reciprocal project link
    # and no others.  Personal/shared connectors keep their creator and
    # continue to refuse account deletion until their ownership is handled
    # explicitly.
    native_project = native_project_sql("datasource")
    await conn.execute(
        f"""
        UPDATE datasources AS datasource
           SET created_by = NULL,
               updated_at = now()
         WHERE datasource.created_by = $1
           AND datasource.scope_mode = 'projects'
           AND COALESCE(datasource.is_global, false) = false
           AND datasource.job_id IS NULL
           AND {native_project} IS NOT NULL
           AND EXISTS (
               SELECT 1
                 FROM project_datasources AS link
                WHERE link.datasource_id = datasource.id
                  AND link.project_id::text = {native_project}
           )
           AND NOT EXISTS (
               SELECT 1
                 FROM project_datasources AS other_link
                WHERE other_link.datasource_id = datasource.id
                  AND other_link.project_id::text <> {native_project}
           )
        """,
        user_id,
    )
    return True
