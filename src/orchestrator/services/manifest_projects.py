"""Project compositions behind the existing membership/default identities.

Legacy fields are migration inputs and editor projections. An active Project
revision contains complete Expert definitions; a server-owned source recipe in
its revision ledger lets the SRW editor rebuild those definitions deliberately.
It is never interpreted as authored metadata or as an authorization grant.
"""

from copy import deepcopy
import json
import logging
from uuid import UUID, uuid4

from fastapi import HTTPException

from orchestrator.services.manifest_experts import (
    bind_bundled_expert_identity,
    hydrate_expert_rows,
    project_expert_resource,
)
from orchestrator.services.manifest_store import ManifestStore, resource_key
from shared.manifests import API_VERSION, validate_documents
from shared.manifests.resolution import content_revision
from shared.runtime.core.srw_manifest_config import (
    SRW_HARNESS_ADAPTER,
    srw_private_config,
)

SOURCE_FORMAT = "srw/project-source-v1"
logger = logging.getLogger(__name__)
_KINDS = {
    "experts": "Expert",
    "workspaces": "WorkspaceTemplate",
    "connectors": "Connector",
}
_DEFAULT_FIELDS = {"worker": "expert", "session": "sessionExpert"}


def _object(value):
    if isinstance(value, str):
        value = json.loads(value)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("Project configuration must be a JSON object")
    return deepcopy(value)


def source_recipe(resource):
    return next(
        (
            deepcopy(item)
            for item in resource.get("dependencies", [])
            if item.get("format") == SOURCE_FORMAT
        ),
        None,
    )


async def active_project_resource(db, project_id):
    store = ManifestStore(db)
    row = await store.by_link("Project", project_id)
    if row is None or not row.get("active_revision"):
        return None
    active = await store.by_name(
        "Project",
        row["document"]["metadata"]["scope"],
        row["name"],
        revision=row["active_revision"],
    )
    if active is None:
        raise HTTPException(409, "The active Project revision is unavailable.")
    return active


def _dependency(resource):
    return {
        "uid": str(resource["id"]),
        "revision": resource["revision"],
        "resourceVersion": resource["resource_version"],
        "key": resource_key(resource["document"]),
    }


def _identity(row):
    fields = (
        "id",
        "name",
        "display_name",
        "description",
        "icon",
        "color",
        "tags",
        "expert_type",
        "owner_id",
        "is_global",
        "managed_key",
        "version",
    )
    return {
        key: (
            str(row[key]) if isinstance(row.get(key), UUID) else deepcopy(row.get(key))
        )
        for key in fields
    }


def compose_srw_expert(document, *, shared=None, linked=None, role="worker"):
    """Freeze authored layers, leaving account defaults to execution admission."""
    result = deepcopy(document)
    runtime = result["spec"]["runtime"]
    layers = ([shared] if role == "worker" and shared else []) + (
        [linked] if linked else []
    )
    if not layers:
        return result
    if runtime.get("adapter") != SRW_HARNESS_ADAPTER:
        raise HTTPException(
            422,
            "SRW Project overrides require an srw/v1 Expert; use an explicit generic Expert definition.",
        )
    private = srw_private_config(result)
    private["layers"] = private.get("layers", []) + deepcopy(layers)
    runtime["config"] = private
    return result


def project_document(
    project,
    *,
    owner_id,
    experts,
    post=None,
    datasources=(),
    previous=None,
    project_changes=None,
    link_changes=None,
):
    """Build a complete portable migration document and its private source recipe."""
    prior = source_recipe(previous) if previous else None
    project_changes, link_changes = project_changes or {}, link_changes or {}
    if previous and prior is None:
        raise HTTPException(409, "Edit this Project composition through its manifest.")
    shared = _object(
        project_changes.get(
            "default_config_override",
            project.get("default_config_override")
            if project.get("default_config_override") is not None
            else (prior or {}).get("sharedConfig"),
        )
    )
    config_name = project_changes.get(
        "default_config_name",
        project.get("default_config_name")
        if project.get("default_config_name") is not None
        else (prior or {}).get("defaultConfigName"),
    )
    doc = {
        "apiVersion": API_VERSION,
        "kind": "Project",
        "metadata": {
            "name": f"project-{project['id']}",
            "scope": {"kind": "Account", "name": str(owner_id)},
            "annotations": {"srw.io/display-name": str(project["name"])},
        },
        "spec": {
            "description": project.get("description") or "",
            "resources": {"experts": {}},
        },
    }
    if previous:
        doc["metadata"] = deepcopy(previous["document"]["metadata"])
        doc["metadata"].setdefault("annotations", {})["srw.io/display-name"] = str(
            project["name"]
        )
    recipe = {
        "format": SOURCE_FORMAT,
        "defaultConfigName": config_name,
        "sharedConfig": shared,
        "experts": {},
    }
    defaults = {}
    for row in experts:
        source_id = str(row["id"])
        old_link = (prior or {}).get("experts", {}).get(source_id, {})
        linked = _object(
            link_changes.get(
                source_id,
                row.get("config_override")
                if row.get("config_override") is not None
                else old_link.get("override"),
            )
        )
        if not row.get("manifest"):
            raise RuntimeError("Migrate stored Experts before migrating their Projects")
        entry = {
            "source": _identity(row),
            "override": linked,
            "defaultFor": row.get("default_for"),
            "aliases": {},
            "sourceRevision": {
                "uid": row["manifest_uid"],
                "revision": row["manifest_revision"],
            },
        }
        # Explicit cross-role selection is supported by SRW. Keep the worker's
        # shared layer out of the session composition without flattening either.
        for role, default_field in _DEFAULT_FIELDS.items():
            alias = f"expert-{source_id.replace('-', '')}-{role}"
            frozen = compose_srw_expert(
                row["manifest"], shared=shared, linked=linked, role=role
            )
            doc["spec"]["resources"]["experts"][alias] = {"inline": frozen["spec"]}
            entry["aliases"][role] = alias
            if row.get("default_for") == role:
                defaults[default_field] = alias
        recipe["experts"][source_id] = entry
    if defaults:
        doc["spec"]["defaults"] = defaults
    if datasources:
        # Availability is distinct from selection: historical auto-attach is
        # actor-specific and stays under the existing datasource admission gate.
        doc["spec"]["resources"]["connectors"] = {
            f"datasource-{str(row['id']).replace('-', '')}": {
                "inline": {
                    "driver": "srw.datasource/v1",
                    "config": {
                        "datasourceId": str(row["id"]),
                        "policyRevision": row["policy_revision"],
                    },
                }
            }
            for row in datasources
        }
    if post:
        config = _object(post.get("config_override"))
        officer = config.get("officer")
        if isinstance(officer, dict):
            officer.pop("hold", None)
            officer.pop("last_respawn_at", None)
        policy = _object(post.get("communication_policy"))
        if config or policy:
            doc["spec"]["team"] = {
                "state": "Active" if post.get("is_active") else "Held",
                "controller": {
                    "type": "srw/officer-v1",
                    "config": {"config": config, "communicationPolicy": policy},
                },
            }
    validate_documents([doc])
    return doc, recipe


async def hydrate_project_row(db, row):
    if row is None:
        return None
    result = dict(row)
    resource = await active_project_resource(db, row["id"])
    if resource is None:
        if row.get("manifest_resource_id"):
            raise HTTPException(409, "The Project has no active manifest revision.")
        return result
    recipe = source_recipe(resource) or {}
    result.update(
        manifest=deepcopy(resource["document"]),
        manifest_uid=str(resource["id"]),
        manifest_revision=resource["revision"],
        manifest_composed=True,
        default_config_name=recipe.get("defaultConfigName"),
        default_config_override=deepcopy(recipe.get("sharedConfig") or {}),
    )
    return result


async def project_link_projection(db, project_id, row, *, key="config_override"):
    if row is None:
        return None
    resource = await active_project_resource(db, project_id)
    if resource is None:
        return dict(row)
    result = dict(row)
    source_id = str(row.get("expert_id") or row.get("id"))
    recipe = source_recipe(resource) or {}
    entry = recipe.get("experts", {}).get(source_id)
    result[key] = deepcopy(entry.get("override") or {}) if entry else {}
    result["project_manifest_composed"] = True
    return result


async def project_expert_for_execution(
    db, project_id, expert_id=None, role="worker", config_name=None
):
    """Select frozen Project content before consulting a mutable global Expert.

    Callers still authorize project membership and the chosen domain identity.
    A matching frozen source is available independently of subsequent source
    edits. ``project_composed`` means shared/link layers are already present.
    """
    if role not in _DEFAULT_FIELDS:
        raise ValueError("Project Expert role must be worker or session")
    resource = await active_project_resource(db, project_id)
    if resource is None:
        return None
    spec, recipe = resource["resolved"]["spec"], source_recipe(resource) or {}
    if expert_id is None and config_name:
        from shared.runtime.core.loader import ROOT_NAMES, canonical_config_name
        from orchestrator.services.config_overrides import validated_config_name

        selector = validated_config_name(canonical_config_name(config_name))
        if selector not in ROOT_NAMES:
            # An explicit bundled selector outranks the Project's default. The
            # active Project contributes its frozen shared SRW layer only.
            parts = selector.split("/")
            if "subagents" in parts and len(parts) > parts.index("subagents") + 1:
                catalog_name = "subagent-" + parts[parts.index("subagents") + 1]
            elif "experts" in parts and len(parts) > parts.index("experts") + 1:
                catalog_name = parts[parts.index("experts") + 1]
            else:
                catalog_name = selector
            source = await ManifestStore(db).by_name(
                "Expert", {"kind": "Catalog", "name": "shared"}, catalog_name
            )
            if source:
                document = compose_srw_expert(
                    source["document"], shared=recipe.get("sharedConfig"), role=role
                )
                projected = project_expert_resource(
                    {"expert_type": role}, {**source, "document": document}
                )
            else:
                if await db.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM srw_resources WHERE kind='Expert' AND scope_kind='Catalog' AND scope_name='shared' AND name=$1 AND deleted_at IS NOT NULL)",
                    catalog_name,
                ):
                    raise HTTPException(
                        409, "The selected bundled Expert resource is retired."
                    )
                projected = _base_project_expert(
                    resource, project_id, role, selector, recipe
                )
            projected.update(
                project_composed=True, project_dependency=_dependency(resource)
            )
            return projected
    entries = recipe.get("experts", {})
    entry = (
        entries.get(str(expert_id))
        if expert_id
        else next(
            (item for item in entries.values() if item.get("defaultFor") == role), None
        )
    )
    alias = entry["aliases"][role] if entry else None
    identity = deepcopy(entry["source"]) if entry else None
    source_dependency = None
    if alias is None and expert_id:
        # Use source identity only to locate the alias; its content stays frozen.
        # Tombstone metadata is sufficient to locate an already-frozen source.
        child = await db.fetchrow(
            """SELECT id,name,scope_kind,scope_name,project_id FROM srw_resources
            WHERE kind='Expert' AND linked_id=$1 ORDER BY deleted_at NULLS FIRST,resource_version DESC LIMIT 1""",
            UUID(str(expert_id)),
        )
        matches = []
        if child:
            for candidate, authored in (
                resource["document"]["spec"]["resources"].get("experts", {}).items()
            ):
                ref = authored.get("ref")
                scope = (
                    ref.get("scope", {"kind": "Project", "name": str(project_id)})
                    if ref
                    else None
                )
                if (
                    "inline" in authored
                    and candidate == child["name"]
                    and str(child.get("project_id")) == str(project_id)
                ) or (
                    ref
                    and ref["name"] == child["name"]
                    and scope
                    == {"kind": child["scope_kind"], "name": child["scope_name"]}
                ):
                    matches.append(candidate)
        if matches:
            default = spec.get("defaults", {}).get(_DEFAULT_FIELDS[role])
            if len(matches) > 1 and default not in matches:
                raise HTTPException(
                    422,
                    "Select the Project Expert alias explicitly; this source has multiple compositions.",
                )
            alias = default if default in matches else matches[0]
            raw = await db.fetchrow(
                "SELECT * FROM experts WHERE id=$1", UUID(str(expert_id))
            )
            identity = dict(raw) if raw else None
            source_dependency = next(
                (
                    item
                    for item in resource["dependencies"]
                    if item.get("uid") == str(child["id"])
                ),
                None,
            )
    if alias is None and expert_id is None:
        alias = spec.get("defaults", {}).get(_DEFAULT_FIELDS[role])
        if alias:
            raw = await db.fetchrow(
                """SELECT e.* FROM experts e JOIN project_experts pe ON pe.expert_id=e.id
                WHERE pe.project_id=$1 AND pe.default_for=$2""",
                UUID(str(project_id)),
                role,
            )
            identity = dict(raw) if raw else None
    selection = (
        spec.get("resources", {}).get("experts", {}).get(alias) if alias else None
    )
    if selection and "inline" in selection:
        document = {
            "apiVersion": API_VERSION,
            "kind": "Expert",
            "metadata": {
                "name": alias,
                "scope": {"kind": "Project", "name": str(project_id)},
            },
            "spec": deepcopy(selection["inline"]),
        }
        projected = project_expert_resource(
            identity or {"expert_type": role},
            {
                "id": resource["id"],
                "revision": resource["revision"],
                "resource_version": resource["resource_version"],
                "document": document,
            },
        )
        if entry:
            projected["manifest_uid"] = entry["sourceRevision"]["uid"]
            projected["manifest_revision"] = entry["sourceRevision"]["revision"]
        elif source_dependency:
            projected["manifest_uid"] = source_dependency["uid"]
            projected["manifest_revision"] = source_dependency["revision"]
    elif expert_id:
        current = await db.get_expert_by_id(str(expert_id))
        if current is None:
            return None
        document = compose_srw_expert(
            current["manifest"], shared=recipe.get("sharedConfig"), role=role
        )
        projected = project_expert_resource(
            current,
            {
                "id": current["manifest_uid"],
                "revision": current["manifest_revision"],
                "resource_version": current["manifest_resource_version"],
                "document": document,
            },
        )
    else:
        if config_name and recipe.get("sharedConfig"):
            projected = _base_project_expert(
                resource, project_id, role, config_name, recipe
            )
        else:
            return None
    projected.update(project_composed=True, project_dependency=_dependency(resource))
    return projected


def _base_project_expert(resource, project_id, role, config_name, recipe):
    document = {
        "apiVersion": API_VERSION,
        "kind": "Expert",
        "metadata": {
            "name": "project-base",
            "scope": {"kind": "Project", "name": str(project_id)},
        },
        "spec": {
            "runtime": {
                "adapter": SRW_HARNESS_ADAPTER,
                "config": {"config_name": config_name, "config": {}},
            }
        },
    }
    document = compose_srw_expert(
        document, shared=recipe.get("sharedConfig"), role=role
    )
    return project_expert_resource(
        {"expert_type": role}, {**resource, "document": document}
    )


async def persist_project_resource(
    db, project_id, *, project_changes=None, link_changes=None, owner_id=None
):
    """Rebuild and activate one legacy-authored Project under the catalog lock."""
    store = ManifestStore(db)
    await store.lock_catalog()
    project = await db.fetchrow(
        "SELECT * FROM projects WHERE id=$1 FOR UPDATE", UUID(str(project_id))
    )
    if project is None:
        return None
    previous = await store.by_link("Project", project_id)
    if previous is None and project.get("manifest_resource_id"):
        raise HTTPException(
            409, "Restore or replace the Project manifest before editing its defaults."
        )
    configuration_change = link_changes is not None or bool(
        set(project_changes or {}) & {"default_config_name", "default_config_override"}
    )
    if previous and (
        source_recipe(previous) is None
        or (project_changes and not configuration_change)
    ):
        if source_recipe(previous) is None and configuration_change:
            raise HTTPException(
                409, "Edit this Project's defaults through its manifest."
            )
        document, resolved = (
            deepcopy(previous["document"]),
            deepcopy(previous["resolved"]),
        )
        for item in (document, resolved):
            item["metadata"].setdefault("annotations", {})["srw.io/display-name"] = (
                project["name"]
            )
            item["spec"]["description"] = project.get("description") or ""
        await store.lock_identity(document)
        resource, _ = await store.save(
            document,
            resolved,
            content_revision(resolved["spec"]),
            previous["dependencies"],
            owner_id=previous["owner_id"],
            project_id=project_id,
            linked_id=project_id,
            expected_version=previous["resource_version"],
        )
        await db.execute(
            "UPDATE srw_resources SET active_revision=$2 WHERE id=$1",
            resource["id"],
            resource["revision"],
        )
        from orchestrator.services.project_workspace_defaults import (
            sync_manifest_defaults,
        )

        await sync_manifest_defaults(db, resource)
        return resource
    owner_id = (
        (previous.get("owner_id") if previous else None)
        or owner_id
        or await db.fetchval(
            "SELECT user_id FROM project_members WHERE project_id=$1 AND role='owner' ORDER BY added_at,user_id LIMIT 1",
            UUID(str(project_id)),
        )
    )
    if owner_id is None:
        raise HTTPException(
            409, "A Project needs an owner before its manifest can be created."
        )
    raw_experts = await db.fetch(
        """SELECT e.*,pe.default_for,pe.config_override
        FROM project_experts pe JOIN experts e ON e.id=pe.expert_id WHERE pe.project_id=$1 ORDER BY e.id""",
        UUID(str(project_id)),
    )
    # Managed copies belong to the composition, not to its migration recipe.
    if previous:
        raw_experts = [
            row
            for row in raw_experts
            if str(row["id"]) in (source_recipe(previous) or {}).get("experts", {})
            or row.get("default_for") is not None
        ]
    experts = await hydrate_expert_rows(db, raw_experts)
    if len(experts) != len(raw_experts):
        raise HTTPException(409, "A Project Expert definition is unavailable.")
    post = await db.fetchrow(
        """SELECT po.config_override,po.communication_policy,
        (t.id IS NOT NULL AND t.status <> 'ended' AND
         COALESCE(t.metadata #>> '{config_override,officer,enabled}', 'false')='true' AND
         (t.metadata #> '{config_override,officer,hold}' IS NULL OR
          t.metadata #> '{config_override,officer,hold}' = 'null'::jsonb)) AS is_active
        FROM project_officers po LEFT JOIN threads t ON t.id=po.thread_id WHERE po.project_id=$1""",
        UUID(str(project_id)),
    )
    datasources = await db.fetch(
        """SELECT d.id,d.policy_revision FROM datasources d
        JOIN project_datasources pd ON pd.datasource_id=d.id WHERE pd.project_id=$1 ORDER BY d.id""",
        UUID(str(project_id)),
    )
    document, recipe = project_document(
        dict(project),
        owner_id=owner_id,
        experts=experts,
        post=dict(post) if post else None,
        datasources=datasources,
        previous=previous,
        project_changes=project_changes,
        link_changes=link_changes,
    )
    uid = previous["id"] if previous else uuid4()
    children, dependencies = [], [recipe]
    for category, kind in _KINDS.items():
        for alias, selection in document["spec"]["resources"].get(category, {}).items():
            child = {
                "apiVersion": API_VERSION,
                "kind": kind,
                "metadata": {
                    "name": alias,
                    "scope": {"kind": "Project", "name": str(project_id)},
                },
                "spec": deepcopy(selection["inline"]),
            }
            if category == "experts":
                entry = next(
                    item
                    for item in recipe["experts"].values()
                    if alias in item["aliases"].values()
                )
                role = next(
                    role for role, name in entry["aliases"].items() if name == alias
                )
                source = entry["source"]
                child["metadata"]["annotations"] = {
                    "srw.io/expert-type": role,
                    "srw.io/display-name": source.get("display_name") or source["name"],
                    "srw.io/description": source.get("description") or "",
                    "srw.io/icon": source.get("icon") or "smart_toy",
                    "srw.io/color": source.get("color") or "#6B7280",
                }
                child["metadata"]["tags"] = list(source.get("tags") or [])
            old_child = await store.by_name(kind, child["metadata"]["scope"], alias)
            child_uid = old_child["id"] if old_child else uuid4()
            revision = content_revision(child["spec"])
            dependencies.append(
                {
                    "uid": str(child_uid),
                    "revision": revision,
                    "key": resource_key(child),
                }
            )
            children.append((child, old_child, child_uid, revision))
    await store.lock_identity(document)
    resource, _ = await store.save(
        document,
        deepcopy(document),
        content_revision(document["spec"]),
        dependencies,
        owner_id=owner_id,
        project_id=project_id,
        linked_id=project_id,
        uid=uid,
        expected_version=previous["resource_version"] if previous else None,
    )
    for child, old_child, child_uid, revision in children:
        await store.lock_identity(child)
        await store.save(
            child,
            deepcopy(child),
            revision,
            [],
            owner_id=owner_id,
            project_id=project_id,
            managed_by=uid,
            uid=child_uid,
            expected_version=old_child["resource_version"] if old_child else None,
        )
    await db.execute(
        "UPDATE srw_resources SET active_revision=$2 WHERE id=$1",
        uid,
        resource["revision"],
    )
    resource["active_revision"] = resource["revision"]
    from orchestrator.services.project_workspace_defaults import (
        sync_manifest_defaults,
    )

    await sync_manifest_defaults(db, resource)
    await db.execute(
        "UPDATE projects SET manifest_resource_id=$2,default_config_name=NULL,default_config_override=NULL WHERE id=$1",
        UUID(str(project_id)),
        uid,
    )
    await db.execute(
        "UPDATE project_experts SET config_override=NULL WHERE project_id=$1",
        UUID(str(project_id)),
    )
    return resource


async def sync_project_identity(db, resource):
    """Project manifests own configuration; domain rows own membership/runtime."""
    if resource["kind"] != "Project" or not resource.get("linked_id"):
        return
    project_id = resource["linked_id"]
    metadata, spec = resource["document"]["metadata"], resource["resolved"]["spec"]
    name = metadata.get("annotations", {}).get("srw.io/display-name", metadata["name"])
    if not isinstance(name, str) or not name or len(name) > 200:
        raise HTTPException(
            422, "Project display name must contain 1 to 200 characters."
        )
    await db.execute(
        """UPDATE projects SET name=$2,description=$3,manifest_resource_id=$4,
        default_config_name=NULL,default_config_override=NULL WHERE id=$1""",
        project_id,
        name,
        spec.get("description", ""),
        resource["id"],
    )
    # Native Project defaults use the stable child/source domain UUIDs. The
    # legacy migration keeps its existing pointers and corresponding recipe.
    if source_recipe(resource):
        return
    defaults = spec.get("defaults", {})
    if defaults.get("expert") and defaults.get("expert") == defaults.get(
        "sessionExpert"
    ):
        raise HTTPException(
            422, "Worker and session default slots require distinct Expert identities."
        )
    await db.execute(
        "UPDATE project_experts SET default_for=NULL,config_override=NULL WHERE project_id=$1",
        project_id,
    )
    for role, field in _DEFAULT_FIELDS.items():
        alias = spec.get("defaults", {}).get(field)
        if not alias:
            continue
        child = await ManifestStore(db).by_name(
            "Expert", {"kind": "Project", "name": str(project_id)}, alias
        )
        if child and child.get("linked_id"):
            expert_id = child["linked_id"]
        else:
            selection = resource["document"]["spec"]["resources"]["experts"][alias]
            ref = selection.get("ref")
            source = (
                await ManifestStore(db).by_name(
                    "Expert",
                    ref.get("scope", {"kind": "Project", "name": str(project_id)}),
                    ref["name"],
                )
                if ref
                else None
            )
            if (
                source
                and not source.get("linked_id")
                and source.get("owner_id") is None
            ):
                source = await bind_bundled_expert_identity(db, source)
            expert_id = source.get("linked_id") if source else None
        if expert_id is None:
            raise HTTPException(
                422, "Project default requires an Expert with a stable domain identity."
            )
        current_role = await db.fetchval(
            "SELECT expert_type FROM experts WHERE id=$1", expert_id
        )
        if (
            current_role != role
            and child
            and str(child.get("managed_by")) == str(resource["id"])
        ):
            # An inline Expert spec has no catalog role field. Its owning
            # Project supplies that role at initial binding. Never change a
            # source Expert, or a child already serving another typed default.
            used = await db.fetchval(
                """SELECT EXISTS(
                SELECT 1 FROM application_expert_defaults WHERE expert_id=$1
                UNION ALL SELECT 1 FROM user_expert_defaults WHERE expert_id=$1
                UNION ALL SELECT 1 FROM project_experts WHERE expert_id=$1 AND default_for IS NOT NULL)""",
                expert_id,
            )
            if not used:
                await db.execute(
                    "UPDATE experts SET expert_type=$2::text,tags=array_append(array_remove(tags,$3::text),$2::text) WHERE id=$1",
                    expert_id,
                    role,
                    current_role,
                )
                current_role = role
        if current_role != role:
            raise HTTPException(
                422, "Project default Expert role does not match its slot."
            )
        await db.execute(
            """INSERT INTO project_experts(project_id,expert_id,default_for)
            VALUES($1,$2,$3) ON CONFLICT(project_id,expert_id) DO UPDATE SET default_for=EXCLUDED.default_for""",
            project_id,
            expert_id,
            role,
        )


async def persist_officer_controller(db, project_id, post):
    """Publish an existing authorized Officer kit edit in the Project revision."""
    store = ManifestStore(db)
    await store.lock_catalog()
    resource = await store.by_link("Project", project_id)
    if resource is None:
        resource = await persist_project_resource(db, project_id)
    if resource is None:
        raise HTTPException(404, "Project does not exist.")
    document, resolved = deepcopy(resource["document"]), deepcopy(resource["resolved"])
    for item in (document, resolved):
        team = item["spec"].setdefault("team", {"state": "Held"})
        if set(team) & {"officer", "slots", "backlog", "jobPolicy"}:
            raise HTTPException(
                409, "Edit this team's settings through its Project manifest."
            )
        team["controller"] = {
            "type": "srw/officer-v1",
            "config": {
                "config": _object(post.get("config_override")),
                "communicationPolicy": _object(post.get("communication_policy")),
            },
        }
    validate_documents([document])
    await store.lock_identity(document)
    saved, _ = await store.save(
        document,
        resolved,
        content_revision(resolved["spec"]),
        resource["dependencies"],
        owner_id=resource["owner_id"],
        project_id=project_id,
        linked_id=project_id,
        expected_version=resource["resource_version"],
    )
    await db.execute(
        "UPDATE srw_resources SET active_revision=$2 WHERE id=$1",
        saved["id"],
        saved["revision"],
    )
    # team.controller is part of the content revision: the manifest-owned
    # workspace defaults row must follow the new active revision.
    from orchestrator.services.project_workspace_defaults import (
        sync_manifest_defaults,
    )

    await sync_manifest_defaults(db, saved)
    return saved


async def validate_project_activation(
    db,
    prepared,
    user,
    *,
    validate_only,
    request=None,
    validate_post_patch=None,
    enforce_auto_pull=None,
):
    """Validate/apply supported SRW controller settings without provisioning.

    Composition can activate independently of a team runtime. The current
    controller supports kit edits on its durable Post; bootstrapping arbitrary
    Officer images, fixed-expert slot dispatch and a new global job policy are
    refused until their lifecycle mappings exist.
    """
    project_id = str(prepared["project_id"])
    spec = prepared["resolved"]["spec"]
    if "limits" in spec:
        raise HTTPException(
            422,
            "Native Project limits are not enforced by this installation; configure the supported SRW Officer controller allocation.",
        )
    team = spec.get("team")
    if not team:
        current = await ManifestStore(db).by_link("Project", project_id)
        if current and current["document"]["spec"].get("team"):
            raise HTTPException(
                422,
                "Removing a team controller requires an explicit supported lifecycle transition.",
            )
        return
    if "limits" in spec or set(team) & {"officer", "slots", "backlog", "jobPolicy"}:
        raise HTTPException(
            422,
            "This installation supports team configuration through srw/officer-v1; native Officer selection, slots, backlog, limits and job policy are not implemented.",
        )
    controller = team.get("controller")
    if not controller:
        current = await ManifestStore(db).by_link("Project", project_id)
        if current and current["document"]["spec"].get("team", {}).get("controller"):
            raise HTTPException(
                422,
                "Removing a team controller requires an explicit supported lifecycle transition.",
            )
        if team.get("state") == "Active":
            raise HTTPException(422, "An Active team needs an installed controller.")
        return
    role = await db.get_user_role_in_project(project_id, str(user["id"]))
    project = await db.fetchrow(
        "SELECT id,status FROM projects WHERE id=$1", UUID(project_id)
    )
    if not user.get("is_admin") and (
        (project and role != "owner")
        or (
            not project
            and prepared["resolved"]["metadata"]["scope"]["name"] != str(user["id"])
        )
    ):
        raise HTTPException(
            403,
            "Only a Project owner or administrator may change its Officer controller.",
        )
    if project and project.get("status") == "archived":
        raise HTTPException(
            409, "Restore the Project before changing its Officer controller."
        )
    payload = controller.get("config")
    if not isinstance(payload, dict) or set(payload) - {
        "config",
        "communicationPolicy",
    }:
        raise HTTPException(
            422,
            "SRW Officer controller config accepts config and communicationPolicy only.",
        )
    desired = _object(payload.get("config"))
    policy = _object(payload.get("communicationPolicy"))
    post = await db.fetchrow(
        "SELECT * FROM project_officers WHERE project_id=$1", UUID(project_id)
    )
    current = _object(post.get("config_override")) if post else {}
    old_policy = _object(post.get("communication_policy")) if post else {}
    officer = _object(desired.get("officer"))
    old_officer = _object(current.get("officer"))
    editable = {
        "slots",
        "auto_pull",
        "worker_spend_ceiling_daily",
        "max_concurrent_workers",
        "daily_token_ceiling",
        "sleep_min_minutes",
        "sleep_max_minutes",
        "max_actions_per_wake",
    }
    if set(officer) & {"hold", "last_respawn_at"}:
        raise HTTPException(
            422, "Officer hold and incarnation state are runtime-owned."
        )
    for key in (set(current) | set(desired)) - {"officer", "llm"}:
        if current.get(key) != desired.get(key):
            raise HTTPException(
                422,
                "This Officer setting needs a supported commissioning or runtime update operation.",
            )
    for key in (set(old_officer) | set(officer)) - editable:
        if old_officer.get(key) != officer.get(key):
            raise HTTPException(
                422,
                "This Officer setting is not editable through the Project controller.",
            )
    llm, old_llm = _object(desired.get("llm")), _object(current.get("llm"))
    if any(
        llm.get(key) != old_llm.get(key)
        for key in (set(llm) | set(old_llm)) - {"model", "reasoning_level"}
    ):
        raise HTTPException(
            422,
            "The Officer controller can select a brain model and reasoning level only.",
        )
    patch = {
        key: officer.get(key)
        for key in editable
        if officer.get(key) != old_officer.get(key)
    }
    brain = {
        key: llm.get(key)
        for key in ("model", "reasoning_level")
        if llm.get(key) != old_llm.get(key)
    }
    if brain:
        patch["brain"] = brain
    if policy != old_policy:
        if not policy:
            raise HTTPException(
                422, "Officer communication policy requires explicit values."
            )
        patch["communication_policy"] = policy
    if patch and validate_post_patch is None:
        raise HTTPException(503, "The SRW Officer controller validator is unavailable.")
    fragment, comm, _ = validate_post_patch(patch) if patch else ({}, None, {})
    if enforce_auto_pull:
        enforce_auto_pull(officer.get("auto_pull"))
    elif officer.get("auto_pull"):
        raise HTTPException(
            503, "The unattended-operation release check is unavailable."
        )
    if officer.get("auto_pull") and not await db.user_can_run_unattended_operations(
        user, project_id
    ):
        raise HTTPException(
            403,
            "Officer auto-pull requires the unattended_operations capability grant.",
        )
    live = await db.get_officer_thread_for_project(project_id) if project else None
    if team["state"] == "Active" and not live:
        raise HTTPException(
            422,
            "Commission the SRW Officer before activating this controller; automatic commissioning is not implemented.",
        )
    # A desired Held/Active transition is not just a config edit: holds own
    # wake delivery, route draining and recycle fences. Keep their current API
    # authoritative until that reconciler is available to manifest activation.
    if live:
        metadata = _object(live.get("metadata"))
        hold = _object(metadata.get("config_override")).get("officer", {}).get("hold")
        if (team["state"] == "Held") != bool(hold):
            raise HTTPException(
                422,
                "Use the Officer hold/release operation before changing the Project team state.",
            )
    if validate_only or not patch:
        return
    if post is None:
        await db.execute(
            "INSERT INTO project_officers(project_id) VALUES($1) ON CONFLICT DO NOTHING",
            UUID(project_id),
        )
    await db.update_project_officer_post(
        project_id,
        config_updates=fragment or None,
        communication_policy_patch=comm,
        project_manifest_write=True,
    )


async def migrate_projects(db):
    migrated = preserved = deferred = 0
    async with db.transaction_scope():
        await ManifestStore(db).lock_catalog()
        projects = await db.fetch(
            "SELECT id,manifest_resource_id,default_config_name,default_config_override FROM projects ORDER BY id FOR UPDATE"
        )
        for row in projects:
            existing = await ManifestStore(db).by_link("Project", row["id"])
            if existing:
                if (
                    row.get("default_config_name") is not None
                    or row.get("default_config_override") is not None
                    or await db.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM project_experts WHERE project_id=$1 AND config_override IS NOT NULL)",
                        row["id"],
                    )
                ):
                    raise RuntimeError(
                        "Project has both a manifest and legacy defaults or link overrides; reconcile before migration"
                    )
                preserved += 1
                continue
            if row.get("manifest_resource_id") is None and await db.fetchval(
                """SELECT NOT EXISTS(
                    SELECT 1 FROM project_members WHERE project_id=$1
                ) AND NOT EXISTS(
                    SELECT 1 FROM users WHERE default_project_id=$1
                )""",
                row["id"],
            ):
                # Historical user deletion can leave an unclaimed Project.
                # Its data has no Account authority for a portable manifest;
                # keep it intact until ownership is explicitly established.
                # Referenced Projects and broken canonical links still fail.
                deferred += 1
                logger.warning(
                    "Deferred manifest migration for unclaimed Project %s: "
                    "assign an owner before migrating this Project",
                    row["id"],
                )
                continue
            await persist_project_resource(db, row["id"])
            migrated += 1
    return {"migrated": migrated, "preserved": preserved, "deferred": deferred}
