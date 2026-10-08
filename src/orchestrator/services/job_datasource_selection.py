"""Job-side connector selection, revalidation and exact resolution.

Extracted verbatim from ``orchestrator.main`` (R1.B05 lane J, census group
``R_WORKSPACE``). This is *policy over* the connector authorities, never a
second copy of them: ``services.datasource_policy`` still classifies and
authorizes a selection, and the thread-shaped authorization entry point
(``_authorize_thread_datasource_selection``, B06) is injected rather than
re-derived here.

What this module owns is the job-shaped part:

* the immutable ``context.datasource_selection`` snapshot is compared against
  the ``job_datasources`` junction on every delivery, and any disagreement is
  one generic refusal rather than a silently reduced attach;
* ``require_exact_datasource_resolution`` is the last gate before a credential
  payload is built — a deleted connector, a changed policy revision, a
  duplicate resolver row or any silent reduction fails closed;
* an inherited selection is *presence*-authoritative: a parent thread that
  persisted ``datasource_ids: []`` means "none", not "unset". See
  [[reference_empty_datasource_ids_is_authoritative]].

``revalidate_selection`` appears as a dependency field even though this module
defines ``revalidate_job_datasource_selection`` itself. That is deliberate: the
application still owns the seam callers (and their tests) steer, so the two
functions that consume a revalidation take it through the dependency object
instead of resolving it in this module's namespace.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Protocol
from uuid import UUID

from fastapi import HTTPException

from orchestrator.services.datasource_policy import workspace_tier_refuses
from shared.backend_kinds import LITE_BACKENDS


class JobDatasourceSelectionStore(Protocol):
    async def get_thread(self, thread_id: str) -> dict[str, Any] | None: ...

    async def get_job(self, job_id: str) -> dict[str, Any] | None: ...

    async def get_user(self, user_id: str) -> dict[str, Any] | None: ...

    async def list_job_datasource_ids(self, job_id: str) -> list[str]: ...

    async def get_datasource_policy_rows(
        self, datasource_ids: list[str]
    ) -> list[dict[str, Any]]: ...

    async def resolve_datasources_for_job(
        self, job_id: str, *, project_id: str | None
    ) -> list[dict[str, Any]]: ...


class AuthorizeThreadDatasourceSelection(Protocol):
    async def __call__(
        self,
        actor: dict[str, Any] | None,
        datasource_ids: list[str],
        *,
        workspace_backend: str | None,
        target_project_ids: list[str],
        effective_work_owner_id: str | None,
        trusted_system_inheritance: bool,
        legacy_job_id: str | None = ...,
    ) -> tuple[list[str], dict[str, int]]: ...


@dataclass(frozen=True)
class JobDatasourceSelectionDependencies:
    """Per-invocation collaborators for job connector selection."""

    store: JobDatasourceSelectionStore
    authorize_thread_datasource_selection: AuthorizeThreadDatasourceSelection
    backend_from_override: Callable[[Any], str | None]
    revalidate_selection: Callable[
        [dict[str, Any]], Awaitable[tuple[list[str], dict[str, int]]]
    ]


def shell_connector_names(datasources: Any, workspace_backend: str | None) -> list[str]:
    """Names of the connectors ``workspace_backend`` cannot serve.

    Repositories need a clone target, and credential and generic
    environments, SSH keys and credential files need a shell; the lite tiers
    provide neither (§4/§7). The rule
    is the drivers' ``supported_backends`` (``workspace_tier_refuses``).
    Returns a (possibly empty) list of human-readable names for the error.
    """
    names: list[str] = []
    for ds in datasources or []:
        if not isinstance(ds, dict):
            continue
        if workspace_tier_refuses(ds, workspace_backend):
            names.append(str(ds.get("name") or ds.get("id") or "?"))
    return names


async def inherit_parent_datasource_ids(
    *,
    thread_id: str | None,
    parent_job_id: str | None,
    dependencies: JobDatasourceSelectionDependencies,
) -> list[str]:
    """Datasource IDs a parented subjob inherits when it passes no explicit
    selection (delegation keeps working without force-attaching anything).

    Prefers the parent thread's persisted selection
    (``threads.metadata.datasource_ids``), then the parent job's
    immutable datasource-selection snapshot. Returns [] when neither parent
    actually exists/yields a selection; database and policy failures propagate.
    """
    if thread_id:
        thread = await dependencies.store.get_thread(thread_id)
        if thread:
            meta = thread.get("metadata") or {}
            if isinstance(meta, str):
                try:
                    meta = json.loads(meta)
                except (json.JSONDecodeError, TypeError):
                    meta = {}
            # Presence is authoritative, including an explicit empty list.
            # Falling through on ``[]`` would resurrect the parent job's
            # connectors and make a deliberate opt-out impossible.
            if "datasource_ids" in meta:
                ids = meta.get("datasource_ids") or []
                return [str(x) for x in ids]
    if parent_job_id:
        parent = await dependencies.store.get_job(parent_job_id)
        if parent is None:
            return []
        selected, _policy_revisions = await dependencies.revalidate_selection(parent)
        return selected
    return []


async def filter_implicit_lite_datasource_ids(
    datasource_ids: list[str],
    workspace_backend: str | None,
    *,
    dependencies: JobDatasourceSelectionDependencies,
) -> list[str]:
    """Drop sources requiring a shell from an implicit/default lite selection.

    Explicit selections fail loudly in the central policy service. Inherited
    and automatic choices are creation-time seeds, so lite tiers keep the
    usable connectors while omitting the ones their drivers say need a shell.
    Missing IDs remain in the list and therefore still fail closed when the
    complete set is authorized.
    """
    if workspace_backend not in LITE_BACKENDS or not datasource_ids:
        return datasource_ids
    rows = await dependencies.store.get_datasource_policy_rows(datasource_ids)
    repositories = {
        str(row["id"]) for row in rows if workspace_tier_refuses(row, workspace_backend)
    }
    return [value for value in datasource_ids if str(value) not in repositories]


async def datasource_selection_provenance(
    *,
    datasource_ids: list[str],
    policy_revisions: dict[str, int],
    origin: str,
    effective_work_owner_id: str | None,
    actor: dict[str, Any] | None,
    project_ids: list[str],
    creation_path: str,
) -> dict[str, Any]:
    """Build the credential-free audit stamp materialized with work."""
    return {
        "origin": origin,
        "creation_path": creation_path,
        "effective_work_owner_id": effective_work_owner_id,
        "initiating_actor_id": str(actor.get("id"))
        if actor and actor.get("id")
        else None,
        "project_ids": list(project_ids),
        "datasource_ids": list(datasource_ids),
        "policy_revisions": dict(policy_revisions),
        "materialized_at": datetime.now(timezone.utc).isoformat(),
    }


async def revalidate_job_datasource_selection(
    job: dict[str, Any],
    *,
    dependencies: JobDatasourceSelectionDependencies,
) -> tuple[list[str], dict[str, int]]:
    """Reauthorize a job and return the exact connector policy snapshot."""
    job_id = str(job["id"])
    selected = await dependencies.store.list_job_datasource_ids(job_id)
    job_context = job.get("context") or {}
    if isinstance(job_context, str):
        try:
            job_context = json.loads(job_context)
        except (json.JSONDecodeError, TypeError) as exc:
            raise HTTPException(
                status_code=403,
                detail="One or more selected connectors are unavailable",
            ) from exc
    materialized = (
        job_context.get("datasource_selection")
        if isinstance(job_context, dict)
        else None
    )
    raw_ids = None
    if isinstance(materialized, dict):
        # ``datasource_ids`` is the shipped provenance key.  Accept the
        # feature-document name too so both writers share the same immutable
        # materialization contract during rollout.
        raw_ids = materialized.get("selected_ids", materialized.get("datasource_ids"))
    try:
        if not isinstance(raw_ids, list):
            raise ValueError
        snapshot_ids = [str(UUID(str(value))) for value in raw_ids]
        junction_ids = [str(UUID(str(value))) for value in selected]
        if len(snapshot_ids) != len(set(snapshot_ids)):
            raise ValueError
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=403,
            detail="One or more selected connectors are unavailable",
        ) from exc
    if set(snapshot_ids) != set(junction_ids) or len(snapshot_ids) != len(junction_ids):
        raise HTTPException(
            status_code=403,
            detail="One or more selected connectors are unavailable",
        )
    selected = snapshot_ids
    owner_id = str(job["user_id"]) if job.get("user_id") else None
    actor = await dependencies.store.get_user(owner_id) if owner_id else None
    config_override = job.get("config_override") or {}
    if isinstance(config_override, str):
        try:
            config_override = json.loads(config_override)
        except (json.JSONDecodeError, TypeError):
            config_override = {}
    return await dependencies.authorize_thread_datasource_selection(
        actor,
        selected,
        workspace_backend=dependencies.backend_from_override(config_override),
        target_project_ids=([str(job["project_id"])] if job.get("project_id") else []),
        effective_work_owner_id=owner_id,
        trusted_system_inheritance=owner_id is None,
        legacy_job_id=job_id,
    )


def require_exact_datasource_resolution(
    selected_ids: list[str],
    policy_revisions: dict[str, int],
    resolved: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Fail closed unless resolution matches the authorized snapshot exactly.

    This is the last gate before credential payload construction.  A deleted
    connector, a revision change, duplicate resolver rows, or any silent
    reduction is one unavailable data contract rather than a partial attach.
    """
    unavailable = HTTPException(
        status_code=403,
        detail="One or more selected connectors are unavailable",
    )
    try:
        expected_ids = [str(UUID(str(value))) for value in selected_ids]
        expected_revisions = {
            str(UUID(str(datasource_id))): int(revision)
            for datasource_id, revision in policy_revisions.items()
        }
        rows = list(resolved or [])
        actual_ids = [str(UUID(str(row["id"]))) for row in rows]
        actual_revisions = {
            str(UUID(str(row["id"]))): int(row.get("policy_revision") or 0)
            for row in rows
        }
    except (KeyError, TypeError, ValueError) as exc:
        raise unavailable from exc

    if (
        len(expected_ids) != len(set(expected_ids))
        or len(actual_ids) != len(set(actual_ids))
        or len(rows) != len(expected_ids)
        or set(actual_ids) != set(expected_ids)
        or set(expected_revisions) != set(expected_ids)
        or actual_revisions != expected_revisions
    ):
        raise unavailable
    return rows


async def resolve_authorized_job_datasources(
    job: dict[str, Any],
    *,
    dependencies: JobDatasourceSelectionDependencies,
) -> list[dict[str, Any]]:
    """Authorize and exactly resolve a job's immutable connector snapshot."""
    selected, policy_revisions = await dependencies.revalidate_selection(job)
    resolved = await dependencies.store.resolve_datasources_for_job(
        str(job["id"]),
        project_id=(str(job["project_id"]) if job.get("project_id") else None),
    )
    return require_exact_datasource_resolution(selected, policy_revisions, resolved)


async def revalidate_job_datasource_ids(
    job: dict[str, Any],
    *,
    dependencies: JobDatasourceSelectionDependencies,
) -> list[str]:
    """Reauthorize a job's materialized set before credential delivery."""
    selected, _revisions = await dependencies.revalidate_selection(job)
    return selected
