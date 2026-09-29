"""Built-in WorkspaceTemplates that the installation's chart declares.

The chart renders the built-ins into ``WORKSPACE_BUILTIN_TEMPLATES``. At every
start the orchestrator makes the shared Catalog match that declaration. The
rows are marked installation-managed, which makes them read-only through the
API (see ``ManifestStore.save`` and ``ManifestStore.delete``).
"""

import json
import logging
import os

from fastapi import HTTPException

from orchestrator.services.manifest_store import ManifestStore
from orchestrator.services.manifest_workspace_selection import srw_workspace_config
from shared.manifests import preview_documents
from shared.manifests.errors import ManifestError
from shared.manifests.resolution import content_revision

logger = logging.getLogger(__name__)

ENV_NAME = "WORKSPACE_BUILTIN_TEMPLATES"
KIND = "WorkspaceTemplate"
SCOPE = {"kind": "Catalog", "name": "shared"}
_OUTCOMES = (
    "created",
    "updated",
    "unchanged",
    "restored",
    "retired",
    "kept",
    "conflicts",
    "invalid",
)


def declared_builtin_templates(environ=None) -> list[dict] | None:
    """The chart's declaration, or None when the installation has none."""
    raw = (os.environ if environ is None else environ).get(ENV_NAME)
    if raw is None or not raw.strip():
        return None
    try:
        declared = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ValueError(f"{ENV_NAME} is not valid JSON: {error}") from None
    if not isinstance(declared, list) or not all(
        isinstance(document, dict) for document in declared
    ):
        raise ValueError(f"{ENV_NAME} must be a JSON list of manifest documents.")
    return declared


def _declared_name(document: dict) -> str:
    metadata = document.get("metadata")
    name = metadata.get("name") if isinstance(metadata, dict) else None
    return name if isinstance(name, str) and name else "<unnamed>"


def _resolve(document: dict) -> dict:
    """Validate one document exactly as an API apply and an admission would."""
    metadata = document.get("metadata")
    if (
        document.get("kind") != KIND
        or not isinstance(metadata, dict)
        or metadata.get("scope") != SCOPE
    ):
        raise ValueError(
            "A built-in must be a WorkspaceTemplate in the shared Catalog."
        )
    resolved = preview_documents([document])["resolved"][0]
    srw_workspace_config({"template": {"inline": resolved["spec"]}})
    return resolved


async def reconcile_builtin_workspace_templates(
    db, declared: list[dict]
) -> dict[str, list[str]]:
    """Make the Catalog's installation-managed templates match ``declared``."""
    summary: dict[str, list[str]] = {outcome: [] for outcome in _OUTCOMES}
    # Every declared name is protected from retirement, valid or not: a broken
    # declaration must never read as "no longer declared".
    names = {_declared_name(document) for document in declared}
    async with db.transaction_scope():
        store = ManifestStore(db)
        await store.lock_catalog()
        for document in declared:
            name = _declared_name(document)
            try:
                resolved = _resolve(document)
            except (
                ManifestError,
                HTTPException,
                ValueError,
                KeyError,
                TypeError,
            ) as error:
                logger.error(
                    "Built-in workspace template %s is invalid and was skipped: %s",
                    name,
                    getattr(error, "detail", error),
                )
                summary["invalid"].append(name)
                continue
            await store.lock_identity(document)
            old = await store.by_name(KIND, SCOPE, name)
            if old and not old.get("installation_managed"):
                logger.error(
                    "Built-in workspace template %s was skipped: a resource with "
                    "that name exists in the shared Catalog. Rename or delete it "
                    "to get the built-in.",
                    name,
                )
                summary["conflicts"].append(name)
                continue
            restored = False
            if old is None:
                retired = await store.retired_installation_managed(KIND, SCOPE, name)
                if retired:
                    old = await store.restore_installation_managed(retired)
                    restored = True
            _, changed = await store.save(
                document,
                resolved,
                content_revision(resolved["spec"]),
                [],
                owner_id=None,
                expected_version=old["resource_version"] if old else None,
                installation_managed=True,
            )
            if restored:
                outcome = "restored"
            elif old is None:
                outcome = "created"
            else:
                outcome = "updated" if changed else "unchanged"
            summary[outcome].append(name)
        for row in await store.installation_managed(KIND):
            if row["name"] in names:
                continue
            if await store.retire_installation_managed(row):
                summary["retired"].append(row["name"])
            else:
                logger.warning(
                    "Built-in workspace template %s is no longer shipped but is "
                    "still referenced; it was kept.",
                    row["name"],
                )
                summary["kept"].append(row["name"])
    return summary


async def reconcile_builtin_workspace_templates_at_startup(db) -> dict | None:
    """Reconcile from the environment. Never raises.

    A broken declaration or a database error must not stop the orchestrator:
    workspaces without a template don't need the built-ins.
    """
    try:
        declared = declared_builtin_templates()
        if declared is None:
            return None
        summary = await reconcile_builtin_workspace_templates(db, declared)
    except Exception:
        logger.exception("Built-in workspace templates were not reconciled.")
        return None
    logger.info("Built-in workspace templates reconciled: %s", summary)
    return summary
