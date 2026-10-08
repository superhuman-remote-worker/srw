"""Resolve authored resources against authorized immutable database revisions.

The bundle resolver remains useful offline. This service expands only declared
platform selections; it never traverses or merges a harness's private config.
"""

from copy import deepcopy
from uuid import UUID, uuid4

from fastapi import HTTPException

from orchestrator.services.manifest_store import resource_key
from orchestrator.services.project_connectors import datasource_binding
from shared.manifests import API_VERSION, preview_documents, validate_documents
from shared.manifests.resolution import content_revision
from shared.manifests.validation import MAX_NODES, check_json_value

REFERENCE_MISSING = (
    "Referenced resource or requested immutable revision does not exist."
)
RESOURCE_MAPS = {
    "experts": "Expert",
    "workspaces": "WorkspaceTemplate",
    "connectors": "Connector",
}


class LiveManifestResolver:
    def __init__(self, store, authority):
        self.store, self.authority = store, authority
        self.documents = []
        self.candidates = {}
        self.projects = {}
        self.project_aliases = {}
        self.prepared = {}
        self.observed = {}
        self.secrets = {}
        self.linked_connectors = {}
        self.expansion_budget = [MAX_NODES]

    async def scope(self, value, *, write=False):
        value = deepcopy(value)
        if value and value["kind"] == "Project":
            value["name"] = self.project_aliases.get(value["name"], value["name"])
        return await self.authority.scope(value, write=write)

    async def prepare(self, documents, *, default_scope=None):
        self.original = validate_documents(documents)
        self.documents = deepcopy(self.original)
        # Allocate prospective project identities without provisioning or writing.
        # The preview token records existing identities, not these allocations.
        for doc in self.documents:
            if doc["kind"] != "Project":
                continue
            scope = deepcopy(
                doc["metadata"].get("scope", default_scope) or self.authority.account
            )
            if scope["name"] in ("me", "personal"):
                scope = deepcopy(self.authority.account)
            if scope["kind"] != "Account":
                raise HTTPException(422, "Project manifests require Account scope.")
            doc["metadata"]["scope"] = scope
            old = await self.store.by_name("Project", scope, doc["metadata"]["name"])
            project_id = str(old["linked_id"]) if old else str(uuid4())
            alias = doc["metadata"]["name"]
            if alias in self.project_aliases:
                raise HTTPException(
                    422, "A bundle cannot contain ambiguous Project scope names."
                )
            self.project_aliases[alias] = project_id
            self.projects[project_id] = doc
            if old:
                await self.authority.resource(old, write=True)
                self.observe(old)
            else:
                await self.authority.scope(scope, write=True)
                self.authority.new_projects.add(project_id)

        for doc in self.documents:
            if doc["kind"] != "Project":
                doc["metadata"]["scope"] = await self.scope(
                    doc["metadata"].get("scope", default_scope), write=True
                )
            await self.add(doc)
        self.managed = {}
        for project_id, project in self.projects.items():
            for category, kind in RESOURCE_MAPS.items():
                for alias, selection in (
                    project["spec"]["resources"].get(category, {}).items()
                ):
                    if "inline" not in selection:
                        continue
                    child = {
                        "apiVersion": API_VERSION,
                        "kind": kind,
                        "metadata": {
                            "name": alias,
                            "scope": {"kind": "Project", "name": project_id},
                        },
                        "spec": deepcopy(selection["inline"]),
                    }
                    await self.add(child)
                    self.managed[resource_key(child)] = resource_key(project)

        for key in self.candidates:
            await self.resolve(key)
        return self

    async def add(self, doc):
        key = resource_key(doc)
        if key in self.candidates:
            raise HTTPException(
                422, "Duplicate resource identity in the candidate configuration."
            )
        self.candidates[key] = doc
        row = await self.store.by_name(
            doc["kind"], doc["metadata"]["scope"], doc["metadata"]["name"]
        )
        if row:
            await self.authority.resource(row, write=True)
            self.observe(row)
            if row["kind"] == "Connector" and row.get("linked_id"):
                # An edit of a datasource's Connector names that Connector's
                # own secret; the store then refuses the edit itself, and the
                # caller who could write it is let through to that refusal.
                self.linked_connectors[key] = row

    def observe(self, row):
        self.observed[resource_key(row["document"])] = {
            "uid": str(row["id"]),
            "resourceVersion": row["resource_version"],
            "revision": row["revision"],
        }

    async def authorize_dependencies(self, dependencies):
        for dependency in dependencies:
            if not dependency.get("uid"):
                continue
            current = await self.store.by_id(dependency["uid"])
            if not current:
                raise HTTPException(
                    409, "A frozen resource dependency has been deleted."
                )
            await self.authority.resource(current)
            self.observe(current)

    async def selection(self, kind, selection, scope, dependencies):
        if "inline" in selection:
            return {
                "inline": await self.spec(
                    kind, deepcopy(selection["inline"]), scope, dependencies
                )
            }
        ref = deepcopy(selection["ref"])
        target_scope = deepcopy(ref.get("scope", scope))
        # A saved Expert may have an explicit global/project grant while its
        # authored identity remains in its owner's Account scope. Resolve the
        # identity first, then consult that domain grant in resource(). A
        # datasource's Connector is shared the same way (public, or linked to
        # a project), by the connector policy, in an Account or a Project.
        connector = kind == "Connector" and target_scope["kind"] != "Catalog"
        if (
            kind in ("Expert", "Connector")
            and target_scope["kind"] == "Account"
            and target_scope["name"] not in ("me", "personal")
        ):
            try:
                UUID(target_scope["name"])
            except ValueError:
                raise HTTPException(
                    422, "Live Account resource references require a UUID."
                ) from None
            ref["scope"] = target_scope
        elif connector and target_scope["kind"] == "Project":
            target_scope["name"] = self.project_aliases.get(
                target_scope["name"], target_scope["name"]
            )
            ref["scope"] = target_scope
        else:
            ref["scope"] = await self.scope(target_scope)
        key = f"{kind}/{ref['scope']['kind']}/{ref['scope']['name']}/{ref['name']}"
        if key in self.candidates and (
            "revision" not in ref
            or (await self.resolve(key))["revision"] == ref["revision"]
        ):
            prepared = await self.resolve(key)
            dependencies.append({"key": key, "revision": prepared["revision"]})
            dependencies.extend(deepcopy(prepared["dependencies"]))
            return {"inline": deepcopy(prepared["resolved"]["spec"])}
        row = await self.store.by_name(
            kind, ref["scope"], ref["name"], revision=ref.get("revision")
        )
        if not row:
            raise HTTPException(422, REFERENCE_MISSING)
        try:
            await self.authority.resource(row, reference=connector)
        except HTTPException as exc:
            # A Connector the caller may not see answers as one that does not
            # exist: a ref is never a probe for someone else's connectors.
            if not connector or exc.status_code != 403:
                raise
            raise HTTPException(422, REFERENCE_MISSING) from None
        self.observe(row)
        if kind == "Connector" and row.get("linked_id"):
            # A datasource's Connector binds as its datasource, the form the
            # SRW snapshot records: re-authorized by the connector policy
            # wherever work uses it, never frozen as a dependency (decision
            # 12: deleting it never waits for work), its credentials the
            # datasource's, delivered under that policy.
            return datasource_binding(row["linked_id"])
        await self.authorize_dependencies(row["dependencies"])
        dependencies.append(
            {
                "key": key,
                "uid": str(row["id"]),
                "resourceVersion": row["resource_version"],
                "revision": row["revision"],
            }
        )
        dependencies.extend(deepcopy(row["dependencies"]))
        # Recheck credential scope/key existence today, even for an old revision.
        # A datasource's Connector lends its secret by the connector policy,
        # for work in the selecting resource's project (decision 11).
        linked = kind == "Connector" and row.get("linked_id")
        return {
            "inline": await self.spec(
                kind,
                deepcopy(row["resolved"]["spec"]),
                ref["scope"],
                dependencies,
                connector=row if linked else None,
                project_ids=[scope["name"]] if scope["kind"] == "Project" else [],
            )
        }

    async def secret_values(
        self, values, scope, *, connector=None, project_ids=(), edit=False
    ):
        for value in values.values():
            if not isinstance(value, dict) or "secretRef" not in value:
                continue
            ref = value["secretRef"]
            if connector is None:
                ref["scope"] = await self.scope(ref.get("scope", scope))
                await self.authority.secret(ref["scope"], name=ref["name"])
            else:
                ref["scope"] = deepcopy(ref.get("scope", scope))
                await self.authority.connector_secret(
                    ref, connector, project_ids=project_ids, edit=edit
                )
            row = await self.store.db.fetchrow(
                "SELECT id,version,keys FROM srw_resource_secrets WHERE scope_kind=$1 AND scope_name=$2 AND name=$3",
                ref["scope"]["kind"],
                ref["scope"]["name"],
                ref["name"],
            )
            if not row or ref["key"] not in row["keys"]:
                raise HTTPException(
                    422, "A declared credential reference or key does not exist."
                )
            self.secrets[str(row["id"])] = row["version"]

    async def project_for_job(self, scope, dependencies):
        project_id = scope["name"]
        if project_id in self.projects:
            key = resource_key(self.projects[project_id])
            project = await self.resolve(key)
            dependencies.append({"key": key, "revision": project["revision"]})
            dependencies.extend(deepcopy(project["dependencies"]))
            return project["resolved"]["spec"]
        row = await self.store.by_link("Project", project_id)
        if not row or not row.get("active_revision"):
            return None
        await self.authority.resource(row)
        self.observe(row)
        active = await self.store.by_name(
            "Project",
            row["document"]["metadata"]["scope"],
            row["name"],
            revision=row["active_revision"],
        )
        await self.authorize_dependencies(active["dependencies"])
        dependencies.append(
            {
                "key": resource_key(row["document"]),
                "uid": str(row["id"]),
                "resourceVersion": active["resource_version"],
                "revision": active["revision"],
            }
        )
        dependencies.extend(deepcopy(active["dependencies"]))
        return deepcopy(active["resolved"]["spec"])

    async def spec(
        self,
        kind,
        spec,
        scope,
        dependencies,
        *,
        project_id=None,
        connector=None,
        project_ids=(),
        edit=False,
    ):
        if kind == "Expert":
            await self.secret_values(spec["runtime"].get("env", {}), scope)
        elif kind == "Connector":
            await self.secret_values(
                spec.get("credentials", {}),
                scope,
                connector=connector,
                project_ids=project_ids,
                edit=edit,
            )
        elif kind == "WorkspaceTemplate":
            if "network" in spec:
                ref = spec["network"]["profileRef"]
                ref["scope"] = await self.scope(ref.get("scope", scope))
        elif kind == "Project":
            child_scope = {"kind": "Project", "name": project_id}
            for category, resource_kind in RESOURCE_MAPS.items():
                for alias, selection in list(
                    spec["resources"].get(category, {}).items()
                ):
                    # Managed inline definitions have the same stable identity
                    # when selected by alias or referenced by a Job.
                    if "inline" in selection:
                        selection = {"ref": {"name": alias, "scope": child_scope}}
                    spec["resources"][category][alias] = await self.selection(
                        resource_kind, selection, child_scope, dependencies
                    )
        elif kind == "Job":
            execution = spec["execution"]
            project = (
                await self.project_for_job(scope, dependencies)
                if scope["kind"] == "Project"
                else None
            )
            if project:
                defaults = project.get("defaults", {})
                resources = project["resources"]
                # defaults.workspace is not copied: an omitted workspace
                # resolves the workspace defaults chain at admission.
                for field, value in defaults.items():
                    if field in execution:
                        continue
                    if field == "expert":
                        execution[field] = deepcopy(resources["experts"][value])
                    elif field == "connectors":
                        execution[field] = {
                            alias: deepcopy(resources["connectors"][alias])
                            for alias in value
                        }
            if "expert" not in execution:
                raise HTTPException(
                    422,
                    "Select an Expert or activate a Project with an Expert default.",
                )
            execution["expert"] = await self.selection(
                "Expert", execution["expert"], scope, dependencies
            )
            workspace = execution.get("workspace")
            if workspace and "template" in workspace:
                workspace["template"] = await self.selection(
                    "WorkspaceTemplate", workspace["template"], scope, dependencies
                )
            for alias, selection in list(
                execution.setdefault("connectors", {}).items()
            ):
                execution["connectors"][alias] = await self.selection(
                    "Connector", selection, scope, dependencies
                )
        # One pure validator/defaulting implementation for offline and live
        # resolution. All database selections have already become inline values.
        doc = {
            "apiVersion": API_VERSION,
            "kind": kind,
            "metadata": {"name": "resolved", "scope": scope},
            "spec": spec,
        }
        return preview_documents([doc])["resolved"][0]["spec"]

    async def resolve(self, key):
        if key in self.prepared:
            return self.prepared[key]
        doc = self.candidates[key]
        if doc["kind"] == "Job":
            old = await self.store.by_name(
                "Job", doc["metadata"]["scope"], doc["metadata"]["name"]
            )
            if (
                old
                and old["document"]["spec"] == doc["spec"]
                and await self.store.db.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM srw_execution_specs WHERE resource_id=$1)",
                    old["id"],
                )
            ):
                # Reapply and classification edits acknowledge the existing
                # finite assignment. They cannot refresh its dependency generation.
                await self.authorize_dependencies(old["dependencies"])
                result = {
                    key: deepcopy(old[key])
                    for key in ("document", "resolved", "revision", "dependencies")
                }
                result["document"] = deepcopy(doc)
                result["resolved"]["metadata"] = deepcopy(doc["metadata"])
                result["project_id"] = None
                self.prepared[key] = result
                return result
        dependencies = []
        project_id = (
            self.project_aliases.get(doc["metadata"]["name"])
            if doc["kind"] == "Project"
            else None
        )
        resolved = deepcopy(doc)
        scope = doc["metadata"]["scope"]
        resolved["spec"] = await self.spec(
            doc["kind"],
            deepcopy(doc["spec"]),
            scope,
            dependencies,
            project_id=project_id,
            connector=self.linked_connectors.get(key),
            project_ids=[scope["name"]] if scope["kind"] == "Project" else [],
            edit=key in self.linked_connectors,
        )
        result = {
            "document": deepcopy(doc),
            "resolved": resolved,
            "revision": content_revision(resolved["spec"]),
            "dependencies": dependencies,
            "project_id": project_id,
        }
        check_json_value(result, budget=self.expansion_budget)
        self.prepared[key] = result
        return result

    def plan_revision(self, default_scope):
        return content_revision(
            {
                "documents": self.original,
                "defaultScope": default_scope,
                "versions": self.observed,
                "secrets": self.secrets,
            }
        )
