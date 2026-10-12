"""Canonical Expert resources behind the existing identity and grant tables.

The ``experts`` table retains stable IDs, ownership, sharing and default links.
Its old config/prompt columns are cleared after their resource is committed.
Compatibility callers receive a projection of the reference harness's private
settings; arbitrary harness settings never enter that projection or its loader.
"""

from copy import deepcopy
import json
import os
from pathlib import Path
import re
from typing import Any
from uuid import UUID, uuid4

from fastapi import HTTPException

from orchestrator.services.manifest_store import ManifestStore, decoded
from shared.manifests import parse_documents, preview_documents
from shared.manifests.resolution import content_revision
from shared.runtime.core.srw_manifest_config import (
    SRW_HARNESS_ADAPTER,
    srw_private_config,
)

_ANNOTATIONS = {
    "display_name": "srw.io/display-name",
    "description": "srw.io/description",
    "icon": "srw.io/icon",
    "color": "srw.io/color",
    "expert_type": "srw.io/expert-type",
}


def installed_srw_image() -> str:
    """Same operator-controlled image setting as the reference provisioner."""
    return os.environ.get(
        "AGENT_IMAGE",
        os.environ.get(
            "PERSISTENT_AGENT_IMAGE",
            "ghcr.io/superhuman-remote-worker/srw-agent:latest",
        ),
    )


def trusted_srw_image(store: Any) -> str:
    """The installed image an explicit inline SRW ``image`` must equal.

    The same source ``prepare_srw_snapshot`` records in the execution: the
    image the application bound to its store at startup, else the operator
    setting. Admission and the snapshot therefore agree on what is installed.
    """
    image = getattr(store, "manifest_runtime_image", None)
    if isinstance(image, str) and image.strip():
        return image
    return installed_srw_image()


def _object(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("Expert private settings must be JSON objects")
    return deepcopy(value)


def expert_resource_name(row: dict[str, Any]) -> str:
    """Preserve ordinary names; disambiguate legacy underscores/long names."""
    name = str(row["name"])
    if re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", name):
        return name
    stem = re.sub(r"[^a-z0-9-]+", "-", name.lower()).strip("-") or "expert"
    return f"{stem[:50].rstrip('-')}-{str(row['id']).replace('-', '')[:12]}"


def expert_manifest(
    row: dict[str, Any],
    *,
    image: str | None = None,
    existing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Convert a reference-harness row, preserving private nulls and tool keys.

    This is an explicit SRW adapter, not a generic manifest transformation.
    New legacy-authored Experts follow the installed harness without copying a
    rollout tag. Existing and explicitly supplied image constraints stay owned.
    """
    if existing is not None:
        private = srw_private_config(existing)
        document = deepcopy(existing)
    else:
        private = {}
        scope = (
            {"kind": "Account", "name": str(row["owner_id"])}
            if row.get("owner_id")
            else {"kind": "Catalog", "name": "shared"}
        )
        document = {
            "apiVersion": "srw/v1alpha1",
            "kind": "Expert",
            "metadata": {"name": expert_resource_name(row), "scope": scope},
            "spec": {
                "runtime": {
                    "adapter": SRW_HARNESS_ADAPTER,
                }
            },
        }
    metadata = document["metadata"]
    annotations = metadata.setdefault("annotations", {})
    for field, key in _ANNOTATIONS.items():
        annotations[key] = str(row.get(field) or "")
    annotations["srw.io/legacy-name"] = str(row["name"])
    metadata["tags"] = list(row.get("tags") or [])
    runtime = document["spec"]["runtime"]
    if image is not None:
        runtime["image"] = image
    private.setdefault(
        "config_name",
        "session_base" if row["expert_type"] == "session" else "worker_base",
    )
    private["config"] = _object(row.get("config"))
    private["prompts"] = _object(row.get("prompts"))
    if row.get("harness_config_name"):
        private["config_name"] = row["harness_config_name"]
    if row.get("harness_asset_name"):
        private["asset_name"] = row["harness_asset_name"]
    if "harness_config_layers" in row:
        private["layers"] = deepcopy(row["harness_config_layers"])
    runtime["config"] = private
    srw_private_config(document)
    from shared.runtime.core.workspace_selection import (
        migrate_expert_workspace_preference,
    )

    return migrate_expert_workspace_preference(document)


def project_expert_resource(
    row: dict[str, Any],
    resource: dict[str, Any],
) -> dict[str, Any]:
    """Project canonical content while retaining the independently checked ACL."""
    result = dict(row)
    document = deepcopy(resource["document"])
    runtime = document["spec"]["runtime"]
    adapter = runtime.get("adapter")
    result.update(
        {
            "manifest": document,
            "manifest_uid": str(resource["id"]),
            "manifest_revision": resource["revision"],
            "manifest_resource_version": resource["resource_version"],
            "workspace_preference": deepcopy(
                document["spec"].get("workspacePreference")
            ),
            "harness_adapter": adapter,
            "harness_config_name": None,
            "harness_asset_name": None,
            "harness_config_layers": [],
            "config": {},
            "prompts": {},
        }
    )
    for field, key in _ANNOTATIONS.items():
        value = document["metadata"].get("annotations", {}).get(key)
        if value is not None:
            result[field] = value
    result["tags"] = list(document["metadata"].get("tags", []))
    if adapter == SRW_HARNESS_ADAPTER:
        private = srw_private_config(document)
        result["config"] = private.get("config", {})
        result["prompts"] = private.get("prompts", {})
        result["harness_config_name"] = private.get("config_name")
        result["harness_asset_name"] = private.get("asset_name")
        result["harness_config_layers"] = private.get("layers", [])
    return result


async def bundled_expert_for_execution(db, selector: str) -> dict[str, Any] | None:
    """Resolve a shipped selector from its canonical Catalog definition.

    Private role bases and non-catalog deployment configs remain SRW adapter
    inputs. A shipped Expert's file identifies installed assets only; it never
    restores configuration after that Catalog definition was edited or retired.
    """
    from shared.runtime.core.loader import (
        ROOT_NAMES,
        canonical_config_name,
        resolve_config_path,
    )
    from orchestrator.services.config_overrides import validated_config_name

    selector = validated_config_name(canonical_config_name(selector))
    if selector in ROOT_NAMES:
        return None
    path, deployment_dir = resolve_config_path(selector)
    installed = (
        Path(deployment_dir) if deployment_dir and Path(path).is_file() else None
    )
    catalog_asset = installed is not None and installed.parent.name in {
        "experts",
        "subagents",
    }
    name = (
        ("subagent-" if installed.parent.name == "subagents" else "") + installed.name
        if catalog_asset
        else selector.removeprefix("experts/")
    )
    resource = await ManifestStore(db).by_name(
        "Expert", {"kind": "Catalog", "name": "shared"}, name
    )
    if resource is None:
        if catalog_asset:
            raise HTTPException(
                409, "The selected bundled Expert is retired or unavailable."
            )
        return None
    return project_expert_resource(
        {
            "id": resource.get("linked_id") or resource["id"],
            "name": name,
            "expert_type": "worker",
            "owner_id": resource.get("owner_id"),
            "is_global": True,
        },
        resource,
    )


async def hydrate_expert_row(db, row) -> dict[str, Any] | None:
    if row is None:
        return None
    resource = await ManifestStore(db).by_link("Expert", row["id"])
    if resource is None:
        if row.get("manifest_resource_id"):
            return None
        # Startup migration reads pre-cutover rows. Never fabricate a resource
        # revision or mistake a failed/absent migration for a frozen execution.
        return dict(row)
    return project_expert_resource(dict(row), resource)


async def hydrate_expert_rows(db, rows) -> list[dict[str, Any]]:
    if not rows:
        return []
    resources = await db.fetch(
        "SELECT * FROM srw_resources WHERE kind='Expert' AND linked_id=ANY($1::uuid[]) AND deleted_at IS NULL",
        [UUID(str(row["id"])) for row in rows],
    )
    by_link = {str(row["linked_id"]): decoded(row) for row in resources}
    return [
        project_expert_resource(dict(row), by_link[str(row["id"])])
        if str(row["id"]) in by_link
        else dict(row)
        for row in rows
        if str(row["id"]) in by_link or not row.get("manifest_resource_id")
    ]


async def persist_expert_resource(
    db,
    row: dict[str, Any],
    *,
    image: str | None = None,
) -> dict[str, Any]:
    """Inside a catalog-locked transaction, save config then clear old payloads."""
    store = ManifestStore(db)
    previous = await store.by_link("Expert", row["id"])
    if previous is None and row.get("manifest_resource_id"):
        raise HTTPException(
            409,
            "The Expert manifest is retired or unavailable; restore it through the resource API.",
        )
    if previous and previous.get("managed_by"):
        raise HTTPException(
            409, "Edit this expert through its owning Project manifest."
        )
    if (
        previous
        and previous["document"]["spec"]["runtime"].get("adapter")
        != SRW_HARNESS_ADAPTER
    ):
        raise HTTPException(409, "Edit this harness through its Expert manifest.")
    document = expert_manifest(
        row, image=image, existing=previous["document"] if previous else None
    )
    preview = preview_documents([document])
    resolved = preview["resolved"][0]
    await store.lock_identity(document)
    resource, _ = await store.save(
        document,
        resolved,
        content_revision(resolved["spec"]),
        [],
        owner_id=row.get("owner_id"),
        linked_id=row["id"],
        expected_version=previous["resource_version"] if previous else None,
    )
    await db.execute(
        "UPDATE experts SET config='{}'::jsonb,prompts='{}'::jsonb,manifest_resource_id=$2 WHERE id=$1",
        UUID(str(row["id"])),
        resource["id"],
    )
    return project_expert_resource(
        {**row, "manifest_resource_id": resource["id"]}, resource
    )


def _native_expert_fields(document, existing=None):
    annotations = document["metadata"].get("annotations", {})
    existing = existing or {}
    fields = {
        "display_name": annotations.get(
            "srw.io/display-name",
            existing.get("display_name") or document["metadata"]["name"],
        ),
        "description": annotations.get(
            "srw.io/description", existing.get("description")
        ),
        "icon": annotations.get("srw.io/icon", existing.get("icon") or "smart_toy"),
        "color": annotations.get("srw.io/color", existing.get("color") or "#6B7280"),
        "expert_type": annotations.get(
            "srw.io/expert-type", existing.get("expert_type") or "worker"
        ),
    }
    if not 1 <= len(fields["display_name"]) <= 200 or len(fields["icon"]) > 100:
        raise HTTPException(422, "Expert display metadata exceeds the catalog limits.")
    if not re.fullmatch(r"#[0-9A-Fa-f]{6}", fields["color"]):
        raise HTTPException(422, "Expert catalog color must use #RRGGBB.")
    if fields["expert_type"] not in ("worker", "session"):
        raise HTTPException(422, "Expert catalog role must be worker or session.")
    return fields


async def bind_native_expert_identity(db, resource: dict[str, Any]) -> dict[str, Any]:
    """Give a newly authored resource the stable ID existing pickers consume.

    Authored annotations cannot request ownership/global visibility. Those come
    from the already-authorized stored scope and owner. Ownerless imported
    bundles remain in the bundled catalog; that provenance is server-owned.
    """
    if (
        resource.get("kind") != "Expert"
        or resource.get("linked_id")
        or not resource.get("owner_id")
    ):
        return resource
    document = resource["document"]
    scope = document["metadata"]["scope"]
    fields = _native_expert_fields(document)
    expert_id = uuid4()
    # The domain alias is distinct across project/catalog scopes even when one
    # owner creates several resources with the same portable name.
    name = f"{document['metadata']['name']}-{str(resource['id']).replace('-', '')}"
    await db.execute(
        """INSERT INTO experts(id,name,display_name,description,icon,color,tags,expert_type,
        owner_id,is_global,manifest_resource_id,config,prompts)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'{}'::jsonb,'{}'::jsonb)""",
        expert_id,
        name,
        fields["display_name"],
        fields["description"],
        fields["icon"],
        fields["color"],
        list(document["metadata"].get("tags", [])),
        fields["expert_type"],
        resource["owner_id"],
        scope["kind"] == "Catalog",
        resource["id"],
    )
    await db.execute(
        "UPDATE srw_resources SET linked_id=$2 WHERE id=$1", resource["id"], expert_id
    )
    if scope["kind"] == "Project":
        project_id = resource.get("project_id")
        if not project_id or str(project_id) != scope["name"]:
            raise HTTPException(
                409, "Project expert scope is not bound to its project identity."
            )
        await db.execute(
            "INSERT INTO project_experts(project_id,expert_id) VALUES($1,$2) ON CONFLICT DO NOTHING",
            project_id,
            expert_id,
        )
    resource["linked_id"] = expert_id
    return resource


async def bind_bundled_expert_identity(db, resource: dict[str, Any]) -> dict[str, Any]:
    """Lazily bind a shipped Catalog definition used by a typed Project default.

    This references the same source resource, without copying it into a Project.
    Ownerless Catalog provenance can only originate in the trusted importer;
    public apply always records an owner, regardless of authored annotations.
    """
    if resource.get("linked_id"):
        return resource
    document = resource["document"]
    if (
        resource.get("owner_id") is not None
        or resource.get("kind") != "Expert"
        or document["metadata"]["scope"] != {"kind": "Catalog", "name": "shared"}
    ):
        raise HTTPException(
            409, "Only imported Catalog definitions can acquire a bundled identity."
        )
    fields = _native_expert_fields(document)
    expert_id = uuid4()
    await db.execute(
        """INSERT INTO experts(id,name,display_name,description,icon,color,tags,expert_type,
        owner_id,is_global,managed_key,manifest_resource_id,config,prompts)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,NULL,TRUE,$9,$10,'{}'::jsonb,'{}'::jsonb)""",
        expert_id,
        document["metadata"]["name"],
        fields["display_name"],
        fields["description"],
        fields["icon"],
        fields["color"],
        list(document["metadata"].get("tags", [])),
        fields["expert_type"],
        "bundled-resource:" + str(resource["id"]),
        resource["id"],
    )
    await db.execute(
        "UPDATE srw_resources SET linked_id=$2 WHERE id=$1", resource["id"], expert_id
    )
    resource["linked_id"] = expert_id
    return resource


async def sync_expert_identity(db, resource: dict[str, Any]) -> None:
    """Mirror searchable metadata after native apply; config stays resource-owned.

    Call in the same catalog-locked transaction as the resource write. Stable
    legacy names and role/default slots cannot be renamed through annotations.
    Ownership and sharing are intentionally absent from the authored projection.
    """
    if resource.get("kind") != "Expert":
        return
    if not resource.get("linked_id"):
        await bind_native_expert_identity(db, resource)
        return
    row = await db.fetchrow(
        "SELECT * FROM experts WHERE id=$1 FOR UPDATE", resource["linked_id"]
    )
    if not row:
        raise HTTPException(409, "The linked expert identity no longer exists.")
    document = resource["document"]
    annotations = document["metadata"].get("annotations", {})
    if annotations.get("srw.io/expert-type", row["expert_type"]) != row["expert_type"]:
        raise HTTPException(409, "An existing expert's default-slot role is immutable.")
    fields = _native_expert_fields(document, row)
    await db.execute(
        """UPDATE experts SET manifest_resource_id=$2,config='{}'::jsonb,prompts='{}'::jsonb,
        display_name=$3,description=$4,icon=$5,color=$6,tags=$7,version=version+1,updated_at=now()
        WHERE id=$1""",
        row["id"],
        resource["id"],
        fields["display_name"],
        fields["description"],
        fields["icon"],
        fields["color"],
        list(document["metadata"].get("tags", [])),
    )


async def migrate_stored_experts(db, *, image: str | None = None) -> dict[str, int]:
    """Idempotently move old payloads without changing identity/default/grants.

    One transaction prevents admission from observing a half-migrated catalogue.
    Existing resource content wins on reruns; old payloads are never restored on
    top of an operator's later manifest edit.
    """
    migrated = 0
    preserved = 0
    async with db.transaction_scope():
        store = ManifestStore(db)
        await store.lock_catalog()
        rows = await db.fetch("SELECT * FROM experts ORDER BY id FOR UPDATE")
        for raw in rows:
            row = dict(raw)
            existing = await store.by_link("Expert", row["id"])
            if existing is None and row.get("manifest_resource_id"):
                retired = await db.fetchrow(
                    "SELECT id,kind,linked_id,deleted_at FROM srw_resources WHERE id=$1",
                    row["manifest_resource_id"],
                )
                if (
                    not retired
                    or retired["kind"] != "Expert"
                    or str(retired.get("linked_id")) != str(row["id"])
                    or retired.get("deleted_at") is None
                ):
                    raise RuntimeError(
                        "An Expert's canonical resource identity is inconsistent; reconcile before migration."
                    )
                if _object(row.get("config")) or _object(row.get("prompts")):
                    raise RuntimeError(
                        "A retired Expert still has legacy payload; reconcile before migration."
                    )
                preserved += 1
                continue
            if existing:
                if _object(row.get("config")) or _object(row.get("prompts")):
                    raise RuntimeError(
                        "An Expert has both a manifest and legacy payload; reconcile it before migration."
                    )
                preserved += 1
                continue
            await persist_expert_resource(db, row, image=image)
            migrated += 1
    return {"migrated": migrated, "preserved": preserved}


async def seed_bundled_expert_manifests(
    db, config_dir: Path, *, image: str | None = None
) -> dict[str, int]:
    """Insert missing shipped manifests; an existing catalog revision stays owned.

    Upgrades are reviewable manifest updates. Restarting the server must never
    silently overwrite an operator's changes to a previously imported resource.
    """
    added = 0
    preserved = 0
    async with db.transaction_scope():
        store = ManifestStore(db)
        await store.lock_catalog()
        for group in ("experts", "subagents"):
            for path in sorted((config_dir / group).glob("*/config.yaml")):
                document = parse_documents(path.read_text(encoding="utf-8"))[0]
                srw_private_config(document)
                if image is not None:
                    document["spec"]["runtime"]["image"] = image
                await store.lock_identity(document)
                metadata = document["metadata"]
                if await store.by_name("Expert", metadata["scope"], metadata["name"]):
                    preserved += 1
                    continue
                if await db.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM srw_resources WHERE kind='Expert'
                    AND scope_kind=$1 AND scope_name=$2 AND name=$3 AND deleted_at IS NOT NULL)""",
                    metadata["scope"]["kind"],
                    metadata["scope"]["name"],
                    metadata["name"],
                ):
                    # A deployment restart must not undo an operator's explicit
                    # deletion of a shipped definition.
                    preserved += 1
                    continue
                preview = preview_documents([document])
                resolved = preview["resolved"][0]
                await store.save(
                    document,
                    resolved,
                    content_revision(resolved["spec"]),
                    [],
                    owner_id=None,
                )
                added += 1
    return {"added": added, "preserved": preserved}
