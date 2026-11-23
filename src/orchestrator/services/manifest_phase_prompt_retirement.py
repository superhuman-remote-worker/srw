"""Strip the retired strategic/tactical expert prompts from current manifests.

Migration 0567 strips them from ``experts.prompts``. A manifest-backed Expert
keeps its prompts in its ``srw_resources`` document instead, and a Project
keeps a frozen copy of each of its Experts inline (plus one managed Expert
child per copy). This pass re-saves each affected resource through the store
as a new revision without the two keys. Nothing reads them any more
(``config_resolver._OVERLAY_PROMPT_KEYS``), so the new revision changes no
behaviour.

Earlier revisions (``srw_resource_revisions``), admitted executions
(``srw_execution_specs``) and frozen job configs are history: content-addressed
and immutable, they keep what they had.

A resource is rewritten only when its stored revision is the digest of its
stored resolved spec (the formula every writer uses); anything else is skipped
and logged rather than guessed at. Each resource commits on its own, so one
failure never blocks the rest, and a rerun finds nothing left to do.
"""

from __future__ import annotations

from copy import deepcopy
import logging
from typing import Any, Callable
from uuid import UUID

from orchestrator.schemas.expert_catalog import RETIRED_EXPERT_PROMPT_KEYS
from orchestrator.services.manifest_store import ManifestStore, decoded
from shared.manifests.resolution import content_revision

logger = logging.getLogger(__name__)

# Lax mode: a member accessor on a scalar yields nothing instead of raising.
_HAS_RETIRED = (
    "'$.**.runtime.config.prompts ? (exists(@.strategic) || exists(@.tactical))'"
)
_CANDIDATES = f"""
SELECT id FROM srw_resources
WHERE kind = $1 AND deleted_at IS NULL
  AND (NOT $2::boolean OR managed_by IS NULL)
  AND (jsonb_path_exists(document, {_HAS_RETIRED})
    OR jsonb_path_exists(resolved, {_HAS_RETIRED}))
ORDER BY id
"""


class _Skip(Exception):
    """A resource this pass leaves alone (logged, never fatal)."""


def strip_retired_prompts(value: Any) -> bool:
    """Remove the retired keys from every Expert spec's ``runtime.config.prompts``
    inside ``value`` (an Expert spec, or a Project spec with inline copies), in
    place. True when anything was removed."""
    changed = False
    if isinstance(value, dict):
        runtime = value.get("runtime")
        config = runtime.get("config") if isinstance(runtime, dict) else None
        prompts = config.get("prompts") if isinstance(config, dict) else None
        if isinstance(prompts, dict):
            for key in RETIRED_EXPERT_PROMPT_KEYS:
                if key in prompts:
                    del prompts[key]
                    changed = True
        for child in value.values():
            changed = strip_retired_prompts(child) or changed
    elif isinstance(value, list):
        for child in value:
            changed = strip_retired_prompts(child) or changed
    return changed


#: Rewrites an Expert spec, or a Project spec with inline copies, in place and
#: says whether it changed anything. ``expert_phases_backfill`` reuses this
#: module's save machinery with its own.
SpecTransform = Callable[[Any], bool]


def _stripped(
    row: dict[str, Any], transform: SpecTransform = strip_retired_prompts
) -> tuple[dict, dict, str] | None:
    """``(document, resolved, revision)`` after ``transform`` (by default:
    without the retired keys), or None when it changes nothing in the row.
    Raises :class:`_Skip` when the stored revision is not the digest of the
    stored resolved spec."""
    document = deepcopy(row["document"])
    resolved = deepcopy(row["resolved"])
    changed = transform(document.get("spec"))
    changed = transform(resolved.get("spec")) or changed
    if not changed:
        return None
    if content_revision(row["resolved"]["spec"]) != row["revision"]:
        raise _Skip(
            f"{row['kind']} {row['id']}: stored revision is not its content digest"
        )
    return document, resolved, content_revision(resolved["spec"])


async def _save(store: ManifestStore, row, document, resolved, revision, deps):
    await store.lock_identity(document)
    saved, _ = await store.save(
        document,
        resolved,
        revision,
        deps,
        owner_id=row.get("owner_id"),
        project_id=row.get("project_id"),
        linked_id=row.get("linked_id"),
        managed_by=row.get("managed_by"),
        expected_version=row["resource_version"],
        installation_managed=bool(row.get("installation_managed")),
        platform_managed=row.get("platform_managed"),
    )
    return saved


async def _retire_project(
    db,
    store: ManifestStore,
    uid: UUID,
    transform: SpecTransform = strip_retired_prompts,
) -> bool:
    row = await store.by_id(uid)
    if row is None or row["kind"] != "Project":
        return False
    own = _stripped(row, transform)
    children = [
        decoded(child)
        for child in await db.fetch(
            "SELECT * FROM srw_resources WHERE managed_by=$1 AND kind='Expert' "
            "AND deleted_at IS NULL ORDER BY id",
            row["id"],
        )
    ]
    child_revisions: dict[str, str] = {}
    for child in children:
        stripped = _stripped(child, transform)
        if stripped is None:
            continue
        document, resolved, revision = stripped
        await _save(store, child, document, resolved, revision, child["dependencies"])
        child_revisions[str(child["id"])] = revision
    if own is None and not child_revisions:
        return False
    if own is None:
        # A child carried the keys but the Project's own copy did not: the two
        # disagree already. Leave the Project's revision alone.
        raise _Skip(f"Project {row['id']}: managed copies disagree with the Project")
    document, resolved, revision = own
    dependencies = deepcopy(row["dependencies"])
    for dependency in dependencies:
        if isinstance(dependency, dict) and dependency.get("uid") in child_revisions:
            dependency["revision"] = child_revisions[dependency["uid"]]
    saved = await _save(store, row, document, resolved, revision, dependencies)
    if row.get("active_revision") == row["revision"]:
        await db.execute(
            "UPDATE srw_resources SET active_revision=$2 WHERE id=$1",
            saved["id"],
            saved["revision"],
        )
    return True


async def _retire_expert(
    db,
    store: ManifestStore,
    uid: UUID,
    transform: SpecTransform = strip_retired_prompts,
) -> bool:
    row = await store.by_id(uid)
    if row is None or row["kind"] != "Expert" or row.get("managed_by"):
        return False
    stripped = _stripped(row, transform)
    if stripped is None:
        return False
    document, resolved, revision = stripped
    # Identity fields (name, role, display metadata) are unchanged, so the
    # experts row needs no sync; its prompts column is already empty.
    await _save(store, row, document, resolved, revision, row["dependencies"])
    return True


async def retire_manifest_phase_prompts(db) -> dict[str, int]:
    """Run once per start; idempotent. Projects first, so each managed Expert
    child is rewritten together with the Project that pins its revision."""
    counts = {"projects": 0, "experts": 0, "skipped": 0, "managed_left": 0}
    for kind, unmanaged_only, retire in (
        ("Project", False, _retire_project),
        ("Expert", True, _retire_expert),
    ):
        for record in await db.fetch(_CANDIDATES, kind, unmanaged_only):
            try:
                async with db.transaction_scope():
                    store = ManifestStore(db)
                    await store.lock_catalog()
                    if await retire(db, store, record["id"]):
                        counts["projects" if kind == "Project" else "experts"] += 1
            except _Skip as exc:
                counts["skipped"] += 1
                logger.warning("Retired expert prompts left in place: %s", exc)
            except Exception:
                counts["skipped"] += 1
                logger.exception(
                    "Retired expert prompts were not removed from %s %s; "
                    "it retries at the next start",
                    kind,
                    record["id"],
                )
    counts["managed_left"] = await db.fetchval(
        "SELECT count(*) FROM srw_resources WHERE kind='Expert' AND deleted_at IS NULL "
        f"AND managed_by IS NOT NULL AND jsonb_path_exists(document, {_HAS_RETIRED})"
    )
    if counts["managed_left"]:
        logger.warning(
            "%s Project-managed Expert(s) still carry retired prompts (their "
            "Project was skipped, or its own copy has none)",
            counts["managed_left"],
        )
    return counts


__all__ = ["retire_manifest_phase_prompts", "strip_retired_prompts"]
