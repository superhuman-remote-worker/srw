"""Transactional desired-state operations shared by HTTP, MCP and CLI."""

from copy import deepcopy
import json
from uuid import UUID, uuid4

from fastapi import HTTPException

from orchestrator.security.access import redact_public_config_override
from orchestrator.security.crypto import encrypt
from orchestrator.services.datasource_policy_errors import (
    DatasourceMaterializationAuthorizationError,
    DatasourceProjectAuthorizationError,
    DatasourceScopeAuthorizationError,
    DatasourcePolicyConflictError,
    DatasourcePolicyValidationError,
)
from orchestrator.services.manifest_authority import ManifestAuthority
from orchestrator.services.manifest_resolution import LiveManifestResolver
from orchestrator.services.manifest_store import (
    ManifestStore,
    resource_key,
    resource_view,
)
from shared.manifests import API_VERSION, parse_documents
from shared.manifests.resolution import content_revision


class ManifestResourceService:
    def __init__(self, db, *, admit_job=None, project_activation=None):
        self.db = db
        self.store = ManifestStore(db)
        self.admit_job = admit_job
        self.project_activation = project_activation

    async def full_fidelity(self, row, user):
        """Whether ``user`` reads ``row``'s stored document unredacted.

        An admin, or the owner of the scope the resource lives in: the Account
        itself, or the Project's owner role (a Project row answers to its
        linked project, not the Account scope its document was authored in).
        Export and re-apply need that fidelity. Every reader below owner — a
        Project viewer or editor, a caller who only sees an Expert — gets the
        view ``public_project`` serves, through the same redactor, because a
        migrated project's documents carry its config override verbatim.
        """
        if user.get("is_admin"):
            return True
        if row["kind"] == "Project" and row.get("linked_id"):
            scope = {"kind": "Project", "name": str(row["linked_id"])}
        else:
            scope = row["document"]["metadata"].get("scope") or {}
        if scope.get("kind") == "Account":
            return str(scope.get("name")) == str(user["id"])
        if scope.get("kind") == "Project":
            role = await self.db.get_user_role_in_project(
                str(scope["name"]), str(user["id"])
            )
            return role == "owner"
        return False

    async def reader_view(self, row, user, **extra):
        view = {**resource_view(row), **extra}
        if not await self.full_fidelity(row, user):
            view["resource"] = redact_public_config_override(view["resource"])
        return view

    async def resolve(self, documents, user, *, request=None, default_scope=None):
        authority = ManifestAuthority(self.db, user, request=request)
        resolver = LiveManifestResolver(self.store, authority)
        return await resolver.prepare(documents, default_scope=default_scope)

    async def preview(
        self, source, user, *, request=None, format="yaml", default_scope=None
    ):
        resolver = await self.resolve(
            parse_documents(source, format=format),
            user,
            request=request,
            default_scope=default_scope,
        )
        resolved = [
            resolver.prepared[resource_key(doc)]["resolved"]
            for doc in resolver.documents
        ]
        if not user.get("is_admin"):
            # Resolution inlines every referenced stored resource after only a
            # READ check, so a viewer's preview could carry another scope's
            # override layers. The caller's own ``documents`` are unchanged.
            resolved = [redact_public_config_override(doc) for doc in resolved]
        return {
            "apiVersion": API_VERSION,
            "operation": "preview",
            "resolution": "stored",
            "admissionReady": False,
            "documents": resolver.documents,
            "resolved": resolved,
            "dependencies": resolver.observed,
            "planRevision": resolver.plan_revision(default_scope),
            "pendingChecks": [
                "backendAvailability",
                "imageResolution",
                "workspaceInstances",
                "executionAdmission",
            ],
            "effects": [],
        }

    async def apply(
        self,
        source,
        user,
        *,
        request=None,
        format="yaml",
        default_scope=None,
        expected_versions=None,
        plan_revision=None,
        idempotency_key=None,
    ):
        documents = parse_documents(source, format=format)
        expected_versions = expected_versions or {}
        request_revision = content_revision(
            {
                "documents": documents,
                "scope": default_scope,
                "expectedVersions": expected_versions,
                "planRevision": plan_revision,
            }
        )
        async with self.db.transaction_scope():
            await self.store.lock_catalog()
            if idempotency_key:
                old_operation = await self.db.fetchrow(
                    "SELECT request_revision,result FROM srw_manifest_operations WHERE owner_id=$1 AND idempotency_key=$2",
                    UUID(str(user["id"])),
                    idempotency_key,
                )
                if old_operation:
                    if old_operation["request_revision"] != request_revision:
                        raise HTTPException(
                            409,
                            "Idempotency key was used for a different apply request.",
                        )
                    result = (
                        json.loads(old_operation["result"])
                        if isinstance(old_operation["result"], str)
                        else old_operation["result"]
                    )
                    authority = ManifestAuthority(self.db, user, request=request)
                    for item in result["resources"]:
                        current = await self.store.by_id(item["uid"])
                        if not current:
                            raise HTTPException(
                                409, "An operation resource has since been deleted."
                            )
                        await authority.resource(current)
                        if not await self.full_fidelity(current, user):
                            item["resource"] = redact_public_config_override(
                                item["resource"]
                            )
                    return result
            resolver = await self.resolve(
                documents, user, request=request, default_scope=default_scope
            )
            if plan_revision and plan_revision != resolver.plan_revision(default_scope):
                raise HTTPException(
                    409,
                    {
                        "code": "DependencyChanged",
                        "message": "The reviewed plan changed; preview it again.",
                    },
                )
            unknown_versions = set(expected_versions) - set(resolver.candidates)
            if unknown_versions:
                raise HTTPException(
                    422,
                    "An expected resource version does not identify a resource in this apply.",
                )
            identities = {}
            old_rows = {}
            for key, doc in resolver.candidates.items():
                old = await self.store.by_name(
                    doc["kind"], doc["metadata"]["scope"], doc["metadata"]["name"]
                )
                old_rows[key] = old
                identities[key] = old["id"] if old else uuid4()
            # All validation precedes project/domain identity materialization.
            for project_id, project in resolver.projects.items():
                prepared = resolver.prepared[resource_key(project)]
                if self.project_activation:
                    await self.project_activation(
                        prepared, user, request=request, validate_only=True
                    )
                elif (
                    prepared["resolved"]["spec"].get("team", {}).get("state")
                    == "Active"
                ):
                    raise HTTPException(
                        422,
                        "Active team reconciliation is not available on this installation.",
                    )
                if project_id in resolver.authority.new_projects:
                    await self.db.create_project(
                        project["metadata"]["name"],
                        description=project["spec"].get("description"),
                        manifest_project_id=UUID(project_id),
                    )
                    await self.db.add_project_member(
                        project_id,
                        project["metadata"]["scope"]["name"],
                        "owner",
                        defer_manifest=True,
                    )

            saved = {}
            ordered = sorted(
                resolver.candidates,
                key=lambda key: (resolver.candidates[key]["kind"] != "Project", key),
            )
            for key in ordered:
                prepared = resolver.prepared[key]
                doc, old = prepared["document"], old_rows[key]
                manager_key = resolver.managed.get(key)
                manager_id = identities[manager_key] if manager_key else None
                if old and old.get("managed_by") != manager_id:
                    raise HTTPException(
                        409,
                        "Resource management differs; an inline child cannot adopt or overwrite an independent definition.",
                    )
                dependencies = []
                for dependency in prepared["dependencies"]:
                    dependency = deepcopy(dependency)
                    if dependency.get("key") in identities:
                        dependency["uid"] = str(identities[dependency["key"]])
                    if dependency not in dependencies:
                        dependencies.append(dependency)
                prepared["dependencies"] = dependencies
                expected = expected_versions.get(key)
                if manager_key and old:
                    # Parent desired-state version owns all managed child updates.
                    parent_old = old_rows[manager_key]
                    if (
                        parent_old
                        and expected_versions.get(manager_key)
                        != parent_old["resource_version"]
                        and (
                            old["document"] != doc
                            or old["resolved"] != prepared["resolved"]
                        )
                    ):
                        raise HTTPException(
                            409,
                            "Updating managed definitions requires the owning Project's expected version.",
                        )
                    expected = old["resource_version"]
                scope = doc["metadata"]["scope"]
                await self.store.lock_identity(doc)
                row, changed = await self.store.save(
                    doc,
                    prepared["resolved"],
                    prepared["revision"],
                    dependencies,
                    owner_id=scope["name"]
                    if scope["kind"] == "Account"
                    else user["id"],
                    project_id=scope["name"]
                    if scope["kind"] == "Project"
                    else prepared.get("project_id"),
                    linked_id=prepared.get("project_id"),
                    managed_by=manager_id,
                    expected_version=expected,
                    uid=identities[key],
                )
                saved[key] = (row, changed)
            from orchestrator.services.manifest_experts import sync_expert_identity
            from orchestrator.services.manifest_projects import sync_project_identity

            for row, changed in saved.values():
                if changed and row["kind"] == "Expert":
                    await sync_expert_identity(self.db, row)
            # An active Project points only at a complete committed candidate.
            for project_id, project in resolver.projects.items():
                key = resource_key(project)
                row, _ = saved[key]
                prepared = resolver.prepared[key]
                await sync_project_identity(self.db, row)
                if self.project_activation:
                    await self.project_activation(
                        prepared, user, request=request, validate_only=False
                    )
                from orchestrator.services.manifest_retirement import (
                    retire_removed_children,
                )

                await retire_removed_children(
                    self.db,
                    row["id"],
                    [
                        identities[child_key]
                        for child_key, manager_key in resolver.managed.items()
                        if manager_key == key
                    ],
                )
                await self.db.execute(
                    "UPDATE srw_resources SET active_revision=$2 WHERE id=$1",
                    row["id"],
                    row["revision"],
                )
                row["active_revision"] = row["revision"]

            executions = {}
            for key, (row, _) in saved.items():
                if row["kind"] != "Job":
                    continue
                existing = await self.db.fetchrow(
                    "SELECT work_id FROM srw_execution_specs WHERE resource_id=$1",
                    row["id"],
                )
                if existing:
                    executions[key] = str(existing["work_id"])
                elif self.admit_job:
                    try:
                        admitted = await self.admit_job(
                            resolver.prepared[key], row, user, request=request
                        )
                    except (
                        DatasourceMaterializationAuthorizationError,
                        DatasourceProjectAuthorizationError,
                        DatasourceScopeAuthorizationError,
                    ):
                        raise HTTPException(
                            403,
                            "Execution owner or connector authority changed before admission.",
                        ) from None
                    except DatasourcePolicyConflictError:
                        raise HTTPException(
                            409, "Connector policy changed; preview and apply again."
                        ) from None
                    except DatasourcePolicyValidationError:
                        raise HTTPException(
                            422, "Invalid connector policy selection."
                        ) from None
                    executions[key] = str(admitted)
                else:
                    raise HTTPException(
                        503, "Manifest execution admission is unavailable."
                    )
            result = {
                "apiVersion": API_VERSION,
                "operation": "apply",
                "operationId": str(uuid4()),
                "resources": [
                    {**resource_view(row), "changed": changed}
                    for row, changed in saved.values()
                ],
                "executions": executions,
            }
            await self.db.execute(
                "INSERT INTO srw_manifest_operations(id,owner_id,idempotency_key,request_revision,result) VALUES($1,$2,$3,$4,$5::jsonb)",
                UUID(result["operationId"]),
                UUID(str(user["id"])),
                idempotency_key,
                request_revision,
                json.dumps(result),
            )
            return {
                **result,
                "resources": [
                    await self.reader_view(row, user, changed=changed)
                    for row, changed in saved.values()
                ],
            }

    async def get(self, resource_id, user, *, request=None):
        row = await self.store.by_id(resource_id)
        if not row:
            raise HTTPException(404, "Resource does not exist.")
        await ManifestAuthority(self.db, user, request=request).resource(row)
        result = await self.reader_view(row, user)
        if row["kind"] == "Job":
            job = await self.db.fetchrow(
                "SELECT j.id,j.status,j.error_message FROM srw_execution_specs s JOIN jobs j ON s.work_id=j.id WHERE s.resource_id=$1",
                row["id"],
            )
            result["status"] = (
                {"workId": str(job["id"]), "phase": job["status"]}
                if job
                else {"phase": "PendingAdmission"}
            )
            if job:
                snapshot = await self.store.execution("Job", str(job["id"]))
                result["status"]["executionId"] = str(snapshot["id"])
                result["status"]["harnessAdapter"] = snapshot["harness_adapter"]
                attempt = await self.db.fetchrow(
                    "SELECT attempt,phase,exit_code,image_id,reported_outcome,cleaned_at FROM srw_execution_attempts WHERE execution_id=$1 ORDER BY attempt DESC LIMIT 1",
                    snapshot["id"],
                )
                if attempt:
                    result["status"]["attempt"] = {
                        "number": attempt["attempt"],
                        "phase": attempt["phase"],
                        "exitCode": attempt["exit_code"],
                        "imageId": attempt["image_id"],
                        "reportedOutcome": attempt["reported_outcome"],
                        "cleaned": attempt["cleaned_at"] is not None,
                    }
                workspace = await self.db.fetchrow(
                    "SELECT i.id,i.status,i.generation FROM srw_execution_workspace_bindings b JOIN srw_workspace_instances i ON i.id=b.instance_id WHERE b.execution_id=$1",
                    snapshot["id"],
                )
                if workspace:
                    result["status"]["workspace"] = {
                        "uid": str(workspace["id"]),
                        "status": workspace["status"],
                        "generation": workspace["generation"],
                    }
        return result

    async def list(self, user, *, scope=None, kind=None, request=None):
        authority = ManifestAuthority(self.db, user, request=request)
        scope = await authority.scope(scope)
        visible = []
        for row in await self.store.list_scope(scope, kind=kind):
            try:
                await authority.resource(row)
            except HTTPException as error:
                if error.status_code in (403, 404):
                    continue
                raise
            visible.append(await self.reader_view(row, user))
        return {"resources": visible}

    async def delete(self, resource_id, user, *, expected_version, request=None):
        row = await self.store.by_id(resource_id)
        if not row:
            raise HTTPException(404, "Resource does not exist.")
        await ManifestAuthority(self.db, user, request=request).resource(
            row, write=True
        )
        if row["kind"] == "Project" and row.get("linked_id"):
            role = await self.db.get_user_role_in_project(
                str(row["linked_id"]), str(user["id"])
            )
            if role != "owner" and not user.get("is_admin"):
                await ManifestAuthority(self.db, user, request=request).deny(
                    "Project deletion requires its owner."
                )
            async with self.db.transaction_scope():
                await self.store.lock_catalog()
                current = await self.store.by_id(resource_id)
                if not current or current["resource_version"] != expected_version:
                    raise HTTPException(
                        409,
                        "Resource version changed; read the current resource and retry.",
                    )
                await self.db.delete_project(str(row["linked_id"]))
            return {"deleted": True, "uid": str(row["id"])}
        await self.store.delete(row, expected_version=expected_version)
        return {"deleted": True, "uid": str(row["id"])}

    async def put_secret(
        self, user, *, scope, name, values, expected_version=None, request=None
    ):
        authority = ManifestAuthority(self.db, user, request=request)
        scope = await authority.scope(scope, write=True)
        await authority.secret(scope)
        async with self.db.transaction_scope():
            await self.store.lock_catalog()
            old = await self.db.fetchrow(
                "SELECT id,version FROM srw_resource_secrets WHERE scope_kind=$1 AND scope_name=$2 AND name=$3 FOR UPDATE",
                scope["kind"],
                scope["name"],
                name,
            )
            if (
                old
                and old["version"] != expected_version
                or not old
                and expected_version is not None
            ):
                raise HTTPException(
                    409,
                    "Credential version changed; supply the current version to replace it.",
                )
            ciphertext = encrypt(json.dumps(values))
            row = await self.db.fetchrow(
                """INSERT INTO srw_resource_secrets(scope_kind,scope_name,name,owner_id,ciphertext,keys)
                VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(scope_kind,scope_name,name)
                DO UPDATE SET ciphertext=EXCLUDED.ciphertext,keys=EXCLUDED.keys,version=srw_resource_secrets.version+1,updated_at=now()
                RETURNING id,version""",
                scope["kind"],
                scope["name"],
                name,
                UUID(str(user["id"])),
                ciphertext,
                sorted(values),
            )
            return {
                "uid": str(row["id"]),
                "resourceVersion": row["version"],
                "scope": scope,
                "name": name,
                "keys": sorted(values),
            }
