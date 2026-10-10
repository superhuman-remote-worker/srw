"""A job's main-cloud connectors: the project folder's copy and the outbox.

Before slice 6, every job of a cloud project got the project's folder copied
into its repository (Mode A) and nothing said so: the agent was never told
where the copy was or what happened to its edits, and 432 jobs on dev
produced no write-back (main_cloud_as_connectors.md, "Mode A and outbox
re-check"). Now the copy is the ``cloud_folder_checkout`` connector a job
has or has not, and the job's outbox (its deliverables copied into a shared
folder at completion) is the ``cloud_outbox`` connector:

* ``context.cloud_checkout = {datasource_id, access}``: the attached
  checkout and its level. Provisioning seeds the copy only for a job that
  carries it (or a loop, whose copy is unconditional: ``loop_floor``), and
  completion captures a diff only at ``reviewed_write_back``. A job without
  the key and with a baseline is an older one, created when every job of a
  cloud project got the copy implicitly at reviewed write-back
  (:func:`legacy_checkout`): it keeps that rule to the end.
* ``context.cloud_connectors``: what the job is told about both
  (``shared.job_cloud_connectors``), rendered into its task brief.

Admission decides both before the row exists (:func:`admission_stamp`), so a
copy's ``cloud_baseline: seeding`` is on the row from its first instant and
the dispatcher never starts the job before the copy lands. Work admitted
elsewhere (automations, an officer's auto-pull) is decided at provisioning
(:func:`provisioning_stamp`), from the rows attached to the job; it never
gets the outbox, which is for work a user starts.

Only the job's own project's checkout copies anything, and only the job
owner's outbox delivers; a subjob gets neither (its branch already holds its
parent's copy, edits included).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any
from uuid import UUID

from orchestrator.services.connector_drivers.base import AccessContext
from orchestrator.services.connector_drivers.cloud_folder_checkout import (
    REVIEWED_WRITE_BACK,
    CloudFolderCheckout,
    CloudFolderCheckoutDriver,
)
from orchestrator.services.connector_drivers.cloud_outbox import is_outbox_row
from shared.connectors.contract import read_only_ceiling
from shared.connectors.platform import native_user
from shared.job_cloud_connectors import (
    CLOUD_CONNECTORS_CONTEXT_KEY,
    EDITS_APPLIED,
    EDITS_DISCARDED,
    EDITS_REVIEWED,
    checkout_entry,
    outbox_entry,
)

logger = logging.getLogger(__name__)

CHECKOUT_CONTEXT_KEY = "cloud_checkout"
#: The completion copy's record (the S16b effect, ``job_outbox_delivery``).
OUTBOX_CONTEXT_KEY = "job_outbox"
#: The stored types this module reads attached rows of.
CHECKOUT_TYPE = "cloud_folder_checkout"
OUTBOX_TYPE = "cloud_outbox"

_ATTACHED_ROWS_SQL = """
    SELECT d.id, d.name, d.type, d.config, d.managed_key, d.read_only,
           d.is_global, d.created_by, pd.read_only AS project_read_only,
           pd.access AS project_access
      FROM datasources d
      LEFT JOIN project_datasources pd
        ON pd.datasource_id = d.id AND pd.project_id = $2::uuid
     WHERE d.id = ANY($1::uuid[])
       AND d.type IN ('cloud_folder_checkout', 'cloud_outbox')
     ORDER BY d.id
"""
_JOB_ROWS_SQL = """
    SELECT d.id, d.name, d.type, d.config, d.managed_key, d.read_only,
           d.is_global, d.created_by, pd.read_only AS project_read_only,
           pd.access AS project_access
      FROM job_datasources jd
      JOIN datasources d ON d.id = jd.datasource_id
      LEFT JOIN project_datasources pd
        ON pd.datasource_id = d.id AND pd.project_id = $2::uuid
     WHERE jd.job_id = $1::uuid
       AND d.type IN ('cloud_folder_checkout', 'cloud_outbox')
     ORDER BY d.id
"""


def job_context(job: Mapping[str, Any]) -> dict[str, Any]:
    """``job['context']`` as a dict (asyncpg hands JSONB over as text)."""
    raw = job.get("context")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            return {}
    return dict(raw) if isinstance(raw, Mapping) else {}


def checkout_context(job: Mapping[str, Any]) -> dict[str, Any] | None:
    """The job's ``context.cloud_checkout``, or ``None`` without one."""
    value = job_context(job).get(CHECKOUT_CONTEXT_KEY)
    return dict(value) if isinstance(value, Mapping) else None


def legacy_checkout(job: Mapping[str, Any]) -> bool:
    """Whether the job is an older one with an implicit copy: no
    ``cloud_checkout`` key, and a baseline the seed recorded."""
    return CHECKOUT_CONTEXT_KEY not in job_context(job) and bool(
        job.get("cloud_diff_baseline_commit")
    )


def capture_applies(job: Mapping[str, Any]) -> bool:
    """Whether completion captures the copy's diff for review: a checkout
    at ``reviewed_write_back``, or an older job (:func:`legacy_checkout`).
    A read-only copy's changes are never captured: SRW never applies them."""
    checkout = checkout_context(job)
    if checkout is not None:
        return checkout.get("access") == REVIEWED_WRITE_BACK
    return legacy_checkout(job)


def checkout_edits(access: str | None, *, loop: bool) -> str:
    """What happens to edits under the copy (``shared.job_cloud_connectors``)."""
    if access is not None:
        return EDITS_REVIEWED if access == REVIEWED_WRITE_BACK else EDITS_DISCARDED
    return EDITS_APPLIED if loop else EDITS_REVIEWED


def project_has_folder(project: Mapping[str, Any] | None) -> bool:
    return bool(
        project
        and project.get("main_cloud_folder_handle")
        and project.get("main_cloud_backend")
    )


def _uuids(values: Any) -> list[UUID]:
    out: list[UUID] = []
    for value in values or ():
        try:
            out.append(UUID(str(value)))
        except (TypeError, ValueError):
            continue
    return out


async def attached_cloud_rows(
    store: Any,
    *,
    project_id: str | None,
    datasource_ids: list[str] | None = None,
    job_id: str | None = None,
) -> list[dict[str, Any]]:
    """The checkout and outbox rows among ``datasource_ids`` (or attached to
    ``job_id``), with the project link's read-only flag (none for a job
    without a project)."""
    try:
        project_uuid = UUID(str(project_id)) if project_id else None
    except (TypeError, ValueError):
        project_uuid = None
    async with store.acquire() as conn:
        if job_id is not None:
            rows = await conn.fetch(_JOB_ROWS_SQL, UUID(str(job_id)), project_uuid)
        else:
            ids = _uuids(datasource_ids)
            if not ids:
                return []
            rows = await conn.fetch(_ATTACHED_ROWS_SQL, ids, project_uuid)
    out = []
    for row in rows:
        item = dict(row)
        item["id"] = str(item["id"])
        if isinstance(item.get("config"), str):
            try:
                item["config"] = json.loads(item["config"])
            except (TypeError, ValueError):
                item["config"] = None
        out.append(item)
    return out


async def owner_role(
    store: Any, user_id: str | None, project_id: str | None
) -> str | None:
    """The job owner's role in its project (``None``: not a member, as an
    administrator may be)."""
    if not user_id or not project_id:
        return None
    roles = await store.get_user_project_roles(str(user_id), [str(project_id)])
    return roles.get(str(project_id))


def resolve_checkout(
    rows: list[dict[str, Any]],
    *,
    project: Mapping[str, Any],
    workspace_backend: str | None = None,
    role: str | None = None,
) -> CloudFolderCheckout | None:
    """The job's checkout among its attached ``rows``, at its level; ``None``
    when none of them copies the job's project's folder (none attached, the
    project has no folder, or the level is refused: logged, never
    substituted).

    The level is the project link's default, else the highest the provider
    offers; a project viewer's job gets at most ``read_only`` (the project
    folder's viewer ceiling, ``datasource_policy.access_ceiling_for``): a
    reviewed write-back would let the viewer write the folder by accepting
    their own job's diff.
    """
    if not project_has_folder(project):
        return None
    driver = CloudFolderCheckoutDriver()
    ceiling = read_only_ceiling(driver.spec) if role == "viewer" else None
    ctx = AccessContext(
        workspace_backend,
        execution="job",
        main_cloud_backend=str(project.get("main_cloud_backend")),
    )
    for row in rows:
        if row.get("type") != CHECKOUT_TYPE:
            continue
        bound = driver.checkout_binding(
            row, project_id=str(project.get("id")), ctx=ctx, ceiling=ceiling
        )
        if isinstance(bound, CloudFolderCheckout):
            return bound
        logger.warning(
            "cloud checkout %s copies nothing for project %s: %s (%s)",
            bound.connector_id,
            project.get("id"),
            bound.reason,
            bound.detail,
        )
    return None


def owners_outbox(
    rows: list[dict[str, Any]], *, user_id: str | None
) -> dict[str, Any] | None:
    """The job owner's own outbox among ``rows``, or ``None``."""
    if not user_id:
        return None
    return next(
        (
            row
            for row in rows
            if row.get("type") == OUTBOX_TYPE
            and is_outbox_row(row)
            and native_user(row) == str(user_id)
        ),
        None,
    )


def cloud_connectors(
    *,
    project: Mapping[str, Any] | None,
    checkout: CloudFolderCheckout | None = None,
    implicit_copy: bool = False,
    loop: bool = False,
    outbox: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """``context.cloud_connectors`` for a job: its copy (from ``checkout``,
    or ``implicit_copy`` for a loop's unconditional one) and its outbox."""
    from orchestrator.services.job_cloud_baseline import slugify_project_name

    entries: list[dict[str, Any]] = []
    if project_has_folder(project) and (checkout is not None or implicit_copy):
        name = str(project.get("name") or "project")
        entries.append(
            checkout_entry(
                project_slug=slugify_project_name(name),
                folder=name,
                edits=checkout_edits(
                    checkout.access if checkout is not None else None, loop=loop
                ),
                access=checkout.access if checkout is not None else None,
                datasource_id=checkout.connector_id if checkout is not None else None,
            )
        )
    if outbox is not None:
        entries.append(outbox_entry(datasource_id=str(outbox.get("id"))))
    return entries


async def admission_stamp(
    store: Any,
    *,
    datasource_ids: list[str],
    project_id: str | None,
    user_id: str | None,
    workspace_backend: str | None,
    outbox_allowed: bool,
) -> dict[str, Any]:
    """The context keys a root job's admission adds before the row exists.

    ``cloud_checkout`` and the copy's ``cloud_baseline: seeding`` (which
    holds dispatch until the seed lands) when the job's own project's
    checkout is attached; ``cloud_connectors`` naming the copy and, when
    ``outbox_allowed`` (work a user starts), the owner's attached outbox.
    Empty when neither applies. A read failure costs the job its copy and
    outbox, logged, never its admission.
    """
    if not datasource_ids:
        return {}
    try:
        project = await store.get_project(str(project_id)) if project_id else None
        rows = await attached_cloud_rows(
            store, project_id=project_id, datasource_ids=datasource_ids
        )
        role = await owner_role(store, user_id, project_id) if project else None
    except Exception:
        logger.warning(
            "cloud connectors: could not read the attached rows of a job in "
            "project %s; it gets no copy and no outbox",
            project_id,
            exc_info=True,
        )
        return {}
    checkout = (
        resolve_checkout(
            rows, project=project, workspace_backend=workspace_backend, role=role
        )
        if project is not None
        else None
    )
    outbox = owners_outbox(rows, user_id=user_id) if outbox_allowed else None
    stamp: dict[str, Any] = {}
    if checkout is not None:
        stamp[CHECKOUT_CONTEXT_KEY] = checkout.context()
        stamp["cloud_baseline"] = {"state": "seeding"}
    entries = cloud_connectors(project=project, checkout=checkout, outbox=outbox)
    if entries:
        stamp[CLOUD_CONNECTORS_CONTEXT_KEY] = entries
    return stamp


async def provisioning_stamp(
    store: Any,
    *,
    job_row: Mapping[str, Any],
    project: Mapping[str, Any],
    loop_floor: bool,
) -> dict[str, Any] | None:
    """What provisioning records for a project job admission did not decide.

    A loop (``loop_floor``) always gets its copy; it is told so here when
    nothing told it yet. A root job without ``cloud_checkout`` (work
    admitted outside admission) gets one when its own project's checkout is
    attached to it. Returns the context keys to merge, or ``None`` when
    there is nothing to add.
    """
    context = job_context(job_row)
    if not project_has_folder(project):
        return None
    if loop_floor:
        if CLOUD_CONNECTORS_CONTEXT_KEY in context:
            return None
        checkout = checkout_context(job_row)
        access = checkout.get("access") if checkout else None
        name = str(project.get("name") or "project")
        from orchestrator.services.job_cloud_baseline import slugify_project_name

        return {
            CLOUD_CONNECTORS_CONTEXT_KEY: [
                checkout_entry(
                    project_slug=slugify_project_name(name),
                    folder=name,
                    edits=checkout_edits(access, loop=bool(context.get("loop_id"))),
                    access=access,
                    datasource_id=(checkout or {}).get("datasource_id"),
                )
            ]
        }
    if CHECKOUT_CONTEXT_KEY in context or job_row.get("parent_job_id"):
        return None
    try:
        rows = await attached_cloud_rows(
            store, project_id=str(project.get("id")), job_id=str(job_row["id"])
        )
        role = await owner_role(
            store, job_row.get("user_id"), str(project.get("id") or "")
        )
    except Exception:
        logger.warning(
            "cloud connectors: could not read job %s's attached rows; no copy",
            job_row.get("id"),
            exc_info=True,
        )
        return None
    checkout = resolve_checkout(rows, project=project, role=role)
    if checkout is None:
        return None
    return {
        CHECKOUT_CONTEXT_KEY: checkout.context(),
        "cloud_baseline": {"state": "seeding"},
        CLOUD_CONNECTORS_CONTEXT_KEY: cloud_connectors(
            project=project, checkout=checkout
        ),
    }


__all__ = [
    "CHECKOUT_CONTEXT_KEY",
    "CHECKOUT_TYPE",
    "OUTBOX_CONTEXT_KEY",
    "OUTBOX_TYPE",
    "admission_stamp",
    "attached_cloud_rows",
    "capture_applies",
    "checkout_context",
    "checkout_edits",
    "cloud_connectors",
    "job_context",
    "legacy_checkout",
    "owner_role",
    "owners_outbox",
    "project_has_folder",
    "provisioning_stamp",
    "resolve_checkout",
]
