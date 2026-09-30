"""Transactional resource persistence using the application's existing database."""

from copy import deepcopy
import json
from uuid import UUID, uuid4

from orchestrator.services.manifest_execution_retirement import (
    execution_references_block_retirement,
    lock_manifest_execution_catalog,
)

from fastapi import HTTPException


def resource_key(document):
    metadata = document["metadata"]
    scope = metadata["scope"]
    return f"{document['kind']}/{scope['kind']}/{scope['name']}/{metadata['name']}"


def decoded(row):
    if row is None:
        return None
    result = dict(row)
    for key in ("document", "resolved", "dependencies"):
        if isinstance(result.get(key), str):
            result[key] = json.loads(result[key])
    return result


INSTALLATION_MANAGED_MESSAGE = (
    "This template is managed by the installation. Duplicate it to change it."
)


def resource_view(row):
    """Keep the portable document separate from observed database identity."""
    return {
        "resource": deepcopy(row["document"]),
        "uid": str(row["id"]),
        "resourceVersion": row["resource_version"],
        "revision": row["revision"],
        "activeRevision": row.get("active_revision"),
        "installationManaged": bool(row.get("installation_managed")),
    }


class ManifestStore:
    def __init__(self, db):
        self.db = db

    async def lock_catalog(self):
        """Serialize desired-state commits, including multi-resource activation.

        A single transaction lock keeps the alpha's whole-resource ownership
        model explicit. Execution reconciliation does not hold this lock.
        """
        await lock_manifest_execution_catalog(self.db)

    async def by_name(self, kind, scope, name, *, revision=None):
        row = decoded(
            await self.db.fetchrow(
                "SELECT * FROM srw_resources WHERE kind=$1 AND scope_kind=$2 AND scope_name=$3 AND name=$4 AND deleted_at IS NULL",
                kind,
                scope["kind"],
                scope["name"],
                name,
            )
        )
        if row and revision is not None:
            frozen = decoded(
                await self.db.fetchrow(
                    "SELECT * FROM srw_resource_revisions WHERE resource_id=$1 AND revision=$2 ORDER BY resource_version DESC LIMIT 1",
                    row["id"],
                    revision,
                )
            )
            if not frozen:
                return None
            row.update(
                {
                    key: frozen[key]
                    for key in (
                        "document",
                        "resolved",
                        "dependencies",
                        "resource_version",
                        "revision",
                    )
                }
            )
        return row

    async def by_id(self, resource_id):
        return decoded(
            await self.db.fetchrow(
                "SELECT * FROM srw_resources WHERE id=$1 AND deleted_at IS NULL",
                UUID(str(resource_id)),
            )
        )

    async def by_link(self, kind, linked_id):
        return decoded(
            await self.db.fetchrow(
                "SELECT * FROM srw_resources WHERE kind=$1 AND linked_id=$2 AND deleted_at IS NULL",
                kind,
                UUID(str(linked_id)),
            )
        )

    async def list_scope(self, scope, *, kind=None, limit=500):
        rows = await self.db.fetch(
            "SELECT * FROM srw_resources WHERE scope_kind=$1 AND scope_name=$2 AND ($3::text IS NULL OR kind=$3) AND deleted_at IS NULL ORDER BY kind,name LIMIT $4",
            scope["kind"],
            scope["name"],
            kind,
            limit,
        )
        return [decoded(row) for row in rows]

    async def lock_identity(self, document):
        # Stable lock order is supplied by multi-resource apply, including new
        # identities which have no row to lock yet.
        await self.db.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            "srw-resource:" + resource_key(document),
        )

    async def save(
        self,
        document,
        resolved,
        revision,
        dependencies,
        *,
        owner_id,
        project_id=None,
        linked_id=None,
        managed_by=None,
        expected_version=None,
        uid=None,
        installation_managed=False,
    ):
        """Call inside transaction_scope, with this identity already locked."""
        metadata = document["metadata"]
        old = await self.by_name(document["kind"], metadata["scope"], metadata["name"])
        if old:
            if bool(old.get("installation_managed")) != installation_managed:
                # The installation's rows and everyone else's never change
                # hands, in either direction. Check this before the version
                # comparison below: an admin applying a stale version against a
                # built-in must see the managed refusal, not a version-changed
                # retry that only leads back here.
                raise HTTPException(
                    409,
                    INSTALLATION_MANAGED_MESSAGE
                    if old.get("installation_managed")
                    else "A resource with this name exists and isn't managed "
                    "by the installation.",
                )
            if (
                expected_version is not None
                and old["resource_version"] != expected_version
            ):
                raise HTTPException(
                    409,
                    "Resource version changed; read the current resource and retry.",
                )
            if (
                old["document"] == document
                and old["resolved"] == resolved
                and old["dependencies"] == dependencies
            ):
                return old, False
            if expected_version is None:
                raise HTTPException(
                    409, "Updating a resource requires its expected resource version."
                )
            if (
                old["kind"] == "Job"
                and (
                    old["document"]["spec"] != document["spec"]
                    or old["resolved"]["spec"] != resolved["spec"]
                    or old["dependencies"] != dependencies
                )
                and await self.db.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM srw_execution_specs WHERE resource_id=$1)",
                    old["id"],
                )
            ):
                raise HTTPException(
                    409,
                    "An admitted Job's specification is immutable; create a new Job name.",
                )
            resource_id = old["id"]
            version = old["resource_version"] + 1
            if managed_by is not None and old.get("managed_by") not in (
                None,
                UUID(str(managed_by)),
            ):
                raise HTTPException(
                    409, "Resource belongs to a different project definition."
                )
            row = await self.db.fetchrow(
                "UPDATE srw_resources SET document=$2::jsonb,resolved=$3::jsonb,revision=$4,dependencies=$5::jsonb,resource_version=$6,updated_at=now() WHERE id=$1 RETURNING *",
                resource_id,
                json.dumps(document),
                json.dumps(resolved),
                revision,
                json.dumps(dependencies),
                version,
            )
        else:
            if expected_version is not None:
                raise HTTPException(
                    409, "Expected an existing resource; the identity is absent."
                )
            resource_id, version = UUID(str(uid)) if uid else uuid4(), 1
            row = await self.db.fetchrow(
                """INSERT INTO srw_resources(id,kind,scope_kind,scope_name,name,owner_id,project_id,linked_id,managed_by,document,resolved,revision,dependencies,installation_managed)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,$12,$13::jsonb,$14) RETURNING *""",
                resource_id,
                document["kind"],
                metadata["scope"]["kind"],
                metadata["scope"]["name"],
                metadata["name"],
                UUID(str(owner_id)) if owner_id else None,
                UUID(str(project_id)) if project_id else None,
                UUID(str(linked_id)) if linked_id else None,
                UUID(str(managed_by)) if managed_by else None,
                json.dumps(document),
                json.dumps(resolved),
                revision,
                json.dumps(dependencies),
                installation_managed,
            )
        await self.db.execute(
            "INSERT INTO srw_resource_revisions(resource_id,resource_version,document,resolved,revision,dependencies) VALUES($1,$2,$3::jsonb,$4::jsonb,$5,$6::jsonb)",
            resource_id,
            version,
            json.dumps(document),
            json.dumps(resolved),
            revision,
            json.dumps(dependencies),
        )
        return decoded(row), True

    async def delete(self, row, *, expected_version):
        async with self.db.transaction_scope():
            await self.lock_catalog()
            await self.lock_identity(row["document"])
            current = await self.by_id(row["id"])
            if not current or current["resource_version"] != expected_version:
                raise HTTPException(
                    409,
                    "Resource version changed; read the current resource and retry.",
                )
            if current.get("installation_managed"):
                raise HTTPException(409, INSTALLATION_MANAGED_MESSAGE)
            if current.get("managed_by"):
                raise HTTPException(
                    409, "Remove this resource through its owning Project definition."
                )
            if current["kind"] == "Expert" and current.get("linked_id"):
                managed_key = await self.db.fetchval(
                    "SELECT managed_key FROM experts WHERE id=$1", current["linked_id"]
                )
                if managed_key:
                    raise HTTPException(
                        409,
                        "Managed platform experts cannot be deleted; change the application default instead.",
                    )
                if await self.db.expert_delete_blockers(str(current["linked_id"])):
                    raise HTTPException(
                        409,
                        "Repoint active Expert references and defaults before deleting this resource.",
                    )
            blocker = await self._retirement_blocker(row)
            if blocker:
                raise HTTPException(409, blocker)
            await self.db.execute(
                "UPDATE srw_resources SET deleted_at=now(),updated_at=now(),resource_version=resource_version+1 WHERE id=$1",
                row["id"],
            )
            if current["kind"] == "Project" and current.get("linked_id"):
                from orchestrator.services.project_workspace_defaults import (
                    release_manifest_defaults,
                )

                await release_manifest_defaults(self.db, current["linked_id"])

    async def _retirement_blocker(self, row) -> str | None:
        """Why a resource can't be retired yet, or None."""
        if await execution_references_block_retirement(
            self.db,
            resource_ids=[row["id"]],
            dependency_ids=[str(row["id"])],
        ):
            return "Resource is referenced by unfinished work."
        if await self.db.fetchval(
            "SELECT EXISTS(SELECT 1 FROM srw_resources WHERE deleted_at IS NULL AND (managed_by=$1 OR dependencies @> $2::jsonb))",
            row["id"],
            json.dumps([{"uid": str(row["id"])}]),
        ):
            return "Resource is still owned or referenced by another saved definition."
        return None

    async def installation_managed(self, kind):
        rows = await self.db.fetch(
            "SELECT * FROM srw_resources WHERE kind=$1 AND installation_managed AND deleted_at IS NULL ORDER BY name",
            kind,
        )
        return [decoded(row) for row in rows]

    async def retired_installation_managed(self, kind, scope, name):
        return decoded(
            await self.db.fetchrow(
                "SELECT * FROM srw_resources WHERE kind=$1 AND scope_kind=$2 AND scope_name=$3 AND name=$4 AND installation_managed AND deleted_at IS NOT NULL ORDER BY updated_at DESC LIMIT 1",
                kind,
                scope["kind"],
                scope["name"],
                name,
            )
        )

    async def restore_installation_managed(self, row):
        """Call inside transaction_scope, with the catalog locked."""
        return decoded(
            await self.db.fetchrow(
                "UPDATE srw_resources SET deleted_at=NULL,updated_at=now(),resource_version=resource_version+1 WHERE id=$1 AND installation_managed RETURNING *",
                row["id"],
            )
        )

    async def retire_installation_managed(self, row) -> bool:
        """Soft-delete an installation's row unless something still uses it.

        Call inside transaction_scope, with the catalog locked.
        """
        if not row.get("installation_managed"):
            raise ValueError("Only installation-managed resources are retired here.")
        if await self._retirement_blocker(row):
            return False
        await self.db.execute(
            "UPDATE srw_resources SET deleted_at=now(),updated_at=now(),resource_version=resource_version+1 WHERE id=$1",
            row["id"],
        )
        return True

    async def execution(self, work_kind, work_id):
        return decoded(
            await self.db.fetchrow(
                "SELECT * FROM srw_execution_specs WHERE work_kind=$1 AND work_id=$2",
                work_kind,
                UUID(str(work_id)),
            )
        )

    async def freeze_execution(
        self,
        *,
        work_kind,
        work_id,
        document,
        resolved,
        revision,
        dependencies,
        owner_id,
        project_ids,
        harness_adapter,
        resource=None,
        conn=None,
    ):
        """Write the first immutable execution revision in the admission transaction."""
        connection = conn if conn is not None else self.db
        row = await connection.fetchrow(
            """INSERT INTO srw_execution_specs(resource_id,resource_version,work_kind,work_id,owner_id,project_ids,document,resolved,revision,dependencies,harness_adapter)
            VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9,$10::jsonb,$11)
            ON CONFLICT(work_kind,work_id) DO NOTHING RETURNING *""",
            resource["id"] if resource else None,
            resource["resource_version"] if resource else None,
            work_kind,
            UUID(str(work_id)),
            UUID(str(owner_id)) if owner_id else None,
            [UUID(str(p)) for p in project_ids],
            json.dumps(document),
            json.dumps(resolved),
            revision,
            json.dumps(dependencies),
            harness_adapter,
        )
        if row:
            result = decoded(row)
            await self._record_execution_revision(result, conn=connection)
            return result
        existing = decoded(
            await connection.fetchrow(
                "SELECT * FROM srw_execution_specs WHERE work_kind=$1 AND work_id=$2",
                work_kind,
                UUID(str(work_id)),
            )
        )
        if existing is None or any(
            (
                existing["resolved"] != resolved,
                existing["document"] != document,
                existing["dependencies"] != dependencies,
                existing["revision"] != revision,
                existing["harness_adapter"] != harness_adapter,
                str(existing.get("owner_id") or "") != str(owner_id or ""),
                {str(value) for value in existing.get("project_ids", [])}
                != {str(value) for value in project_ids},
                str(existing.get("resource_id") or "")
                != str(resource["id"] if resource else ""),
                existing.get("resource_version")
                != (resource["resource_version"] if resource else None),
            )
        ):
            raise HTTPException(409, "Execution configuration is already frozen.")
        return existing

    async def _record_execution_revision(self, row, *, conn):
        await conn.execute(
            """INSERT INTO srw_execution_spec_revisions
            (execution_id,generation,document,resolved,dependencies,revision)
            VALUES($1,$2,$3::jsonb,$4::jsonb,$5::jsonb,$6)""",
            row["id"],
            row["generation"],
            json.dumps(row["document"]),
            json.dumps(row["resolved"]),
            json.dumps(row["dependencies"]),
            row["revision"],
        )

    async def update_session_execution(
        self,
        *,
        work_id,
        document,
        resolved,
        revision,
        dependencies,
        owner_id,
        project_ids,
        harness_adapter,
        resource=None,
        conn,
        expected_generation=None,
    ):
        """Publish one session configuration generation after its update checks.

        Existing turn/attach recipients retain their delivered generation. The
        immutable revision ledger remains available after the current pointer
        changes; this method never changes a finite Job.
        """
        current = decoded(
            await conn.fetchrow(
                "SELECT * FROM srw_execution_specs WHERE work_kind='Session' AND work_id=$1 FOR UPDATE",
                UUID(str(work_id)),
            )
        )
        if current is None:
            if expected_generation is not None:
                raise HTTPException(409, "Session configuration generation changed.")
            return await self.freeze_execution(
                work_kind="Session",
                work_id=work_id,
                document=document,
                resolved=resolved,
                revision=revision,
                dependencies=dependencies,
                owner_id=owner_id,
                project_ids=project_ids,
                harness_adapter=harness_adapter,
                resource=resource,
                conn=conn,
            )
        if (
            expected_generation is not None
            and current["generation"] != expected_generation
        ):
            raise HTTPException(409, "Session configuration generation changed.")
        if (
            str(current.get("owner_id") or "") != str(owner_id or "")
            or current["harness_adapter"] != harness_adapter
        ):
            raise HTTPException(
                409, "A config update cannot change execution ownership or harness."
            )
        if (
            current["document"] == document
            and current["resolved"] == resolved
            and current["dependencies"] == dependencies
            and current["revision"] == revision
            and {str(value) for value in current.get("project_ids", [])}
            == {str(value) for value in project_ids}
        ):
            return current
        updated = decoded(
            await conn.fetchrow(
                """UPDATE srw_execution_specs SET document=$2::jsonb,resolved=$3::jsonb,
            dependencies=$4::jsonb,revision=$5,project_ids=$6,generation=generation+1
            WHERE id=$1 RETURNING *""",
                current["id"],
                json.dumps(document),
                json.dumps(resolved),
                json.dumps(dependencies),
                revision,
                [UUID(str(value)) for value in project_ids],
            )
        )
        await self._record_execution_revision(updated, conn=conn)
        return updated
