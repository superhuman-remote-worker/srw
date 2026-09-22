"""Job-creation helper for automations.

Translates an automation row (one of the templates stored in the
``automations`` table) into an actual job by calling ``db.create_job()``
— the same DB write that the cockpit's ``POST /api/jobs`` handler issues.
Once the job lands in the table the existing dispatcher picks it up like
any other job; nothing in the agent, workspace, or job-detail view needs
to know the job came from an automation.

The reverse link is ``jobs.context->>'automation_id'``, set here on the
new job. ``automations.last_job_id`` is the forward link, written by
``db.advance_automation_after_fire`` in the cron dispatcher.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from orchestrator.security.access import project_is_archived

from orchestrator.services.agent_pod_entrypoint import (
    InvalidConfigNameError,
    validate_config_name,
)
from orchestrator.services.datasource_policy import default_datasource_selection
from orchestrator.services.default_experts import (
    ExpertSelectionError,
    resolve_root_expert,
)
from shared.runtime.core.loader import canonical_config_name, deep_merge

logger = logging.getLogger(__name__)


def _experts_db_enabled() -> bool:
    return os.getenv("EXPERTS_DB_ENABLED", "true").lower().strip() in {
        "true",
        "1",
        "yes",
    }


def _uuid_text(value: Any) -> str | None:
    if not value:
        return None
    try:
        return str(UUID(str(value)))
    except (TypeError, ValueError, AttributeError):
        return None


def _workspace_backend(config_override: dict[str, Any]) -> str | None:
    """Return the explicitly selected workspace tier, if the template has one."""
    workspace = config_override.get("workspace")
    if not isinstance(workspace, dict):
        return None
    backend = workspace.get("backend")
    return str(backend) if backend else None


async def _resolve_worker_selection(
    db: Any,
    *,
    owner_id: str,
    project_id: str | None,
    explicit_expert_id: str | None = None,
    caller_is_admin: bool = True,
):
    owner = await db.get_user(owner_id)
    return await resolve_root_expert(
        db,
        expert_type="worker",
        user_id=owner_id,
        project_id=project_id,
        explicit_expert_id=explicit_expert_id,
        is_admin=bool((owner or {}).get("is_admin")) and caller_is_admin,
    )


async def validate_automation_expert_selection(
    db: Any,
    *,
    owner_id: str,
    project_id: str | None,
    expert: str,
    expert_id: str | None,
    caller_is_admin: bool,
) -> str:
    """Validate and normalize an automation's persisted expert source.

    Bundled experts live in ``expert``. A DB-backed worker expert must use
    ``expert_id`` while ``expert`` stays at the structural ``worker_base``.
    The visibility check runs as the automation owner because that is the
    identity used when a future scheduled fire creates its job — bounded by
    the storing request's own ``caller_is_admin``, which is already narrowed
    (a PAT without the ``admin`` scope, the view-as shadow), so a request
    cannot pin an expert only the owner's unrestricted admin flag could see.

    The bundled selector also gets the pod entrypoint's allow-list here: this
    is the write boundary for a value that is copied into ``jobs.config_name``
    by every future scheduled fire, long after the request that stored it.
    """
    try:
        validate_config_name(expert)
    except InvalidConfigNameError as exc:
        raise ExpertSelectionError(str(exc)) from exc
    config_name = canonical_config_name(expert)
    if not expert_id:
        return config_name
    if config_name != "worker_base":
        raise ExpertSelectionError(
            "expert_id cannot be combined with a bundled worker expert; "
            "select one expert source"
        )
    normalized_id = _uuid_text(expert_id)
    if not normalized_id:
        raise ExpertSelectionError("Expert is unavailable")
    if not _experts_db_enabled():
        raise ExpertSelectionError("Database-backed experts are unavailable")
    await _resolve_worker_selection(
        db,
        owner_id=owner_id,
        project_id=project_id,
        explicit_expert_id=normalized_id,
        caller_is_admin=caller_is_admin,
    )
    return config_name


async def create_job_from_automation(
    db: Any,
    automation: dict[str, Any],
    *,
    trigger_kind: str = "cron",
) -> dict[str, Any] | None:
    """Materialize a job from an automation template.

    Copies ``expert`` → ``config_name``, ``prompt`` → ``description``,
    and stamps ``context.automation_id`` / ``context.automation_name``
    so the cockpit "Runs" view (``list_automation_runs``) can join back.

    ``autonomy`` is injected into ``config_override`` because the jobs
    schema has no top-level autonomy column — dispatch reads it from
    ``config_override['autonomy']`` (src/orchestrator/main.py:933-935).

    Args:
        db: PostgresDB instance.
        automation: A row from ``automations`` (the dict returned by
            ``db.fetch_next_due_cron_automation`` or ``db.get_automation``).
        trigger_kind: "cron" today; v0.5 will pass "event" so the context
            tag can disambiguate. Stored as ``context.automation_trigger``.

    Returns:
        The created job dict (as ``db.create_job`` returns it), with at
        minimum ``id``, ``status``, and the template-derived fields — or
        ``None`` when the automation's project is archived and the fire was
        skipped.

    Archived projects (§4.3 of
    knowledge-base/knowledge/features/project_and_job_list_filtering.md): this
    is the seam, not ``_resolve_automation_or_404``, because that guard runs
    only for non-owner, non-admin callers — the common case (cron, and the
    owner's own "Run now") bypasses it entirely. Skip and log rather than
    raise: a cron tick has no HTTP caller to receive a 409, and
    auto-disabling the automation would silently destroy the owner's
    configuration. The cron dispatcher turns ``None`` into a schedule
    advance; the ``run-now`` endpoint turns it into the 409 its caller can
    actually act on.
    """
    project_id_for_gate = automation.get("project_id")
    if project_id_for_gate:
        project = await db.get_project(str(project_id_for_gate))
        if project_is_archived(project):
            logger.info(
                "Automation %s skipped (%s): project %s is archived — "
                "the automation stays enabled; unarchive to resume firing",
                automation.get("id"),
                trigger_kind,
                project_id_for_gate,
            )
            return None

    # config_override starts as a copy of the automation's template; we
    # then layer the autonomy choice on top. Templates that explicitly
    # set autonomy via config_override take precedence (callers can
    # override per-automation defaults that way).
    config_override = dict(automation.get("config_override") or {})
    if "autonomy" not in config_override and automation.get("autonomy"):
        config_override["autonomy"] = automation["autonomy"]

    # Context tags drive the run-history join and give downstream code
    # (audit log, cockpit job-detail badge) the breadcrumb back to the
    # automation that fired the job.
    automation_id = str(automation["id"])
    context = {
        "automation_id": automation_id,
        "automation_name": automation.get("name") or "",
        "automation_trigger": trigger_kind,
    }

    project_id = automation.get("project_id")

    # DB-backed selections use the explicit UUID column and remain pinned.
    # `worker_base` without a UUID resolves the owner's effective default at
    # fire time. Legacy expert-name rows remain live name references, while
    # bundled names fall through to config_name when no DB row matches.
    expert_id = None
    stored_expert = str(automation.get("expert") or "worker_base")
    config_name = canonical_config_name(stored_expert)
    pinned_expert_id = _uuid_text(automation.get("expert_id"))
    legacy_uuid_id = _uuid_text(stored_expert) if not pinned_expert_id else None

    if (pinned_expert_id or legacy_uuid_id) and not _experts_db_enabled():
        raise ExpertSelectionError("Database-backed experts are unavailable")

    if _experts_db_enabled():
        from shared.runtime.core.expert_resolution import pick_expert_by_name

        owner_id = str(automation["owner_id"])
        pids = [str(project_id)] if project_id else []
        try:
            selection = None
            if pinned_expert_id or legacy_uuid_id:
                selection = await _resolve_worker_selection(
                    db,
                    owner_id=owner_id,
                    project_id=str(project_id) if project_id else None,
                    explicit_expert_id=pinned_expert_id or legacy_uuid_id,
                )
            elif config_name == "worker_base":
                selection = await _resolve_worker_selection(
                    db,
                    owner_id=owner_id,
                    project_id=str(project_id) if project_id else None,
                )

            if selection:
                expert_id = str(selection.expert["id"])
                config_name = "worker_base"
                context["expert_selection"] = {
                    "source": selection.source,
                    "expert_id": expert_id,
                }
                if selection.project_override:
                    # Project expert settings sit below the automation's own
                    # per-run override, matching the normal root-job path.
                    config_override = deep_merge(
                        selection.project_override, config_override
                    )
            else:
                candidates = await db.list_experts_visible(
                    user_id=owner_id, project_ids=pids, expert_type="worker"
                )
                matches = [c for c in candidates if c["name"] == stored_expert]
                winner = pick_expert_by_name(matches, owner_id, set(pids))
                if winner:
                    selection = await _resolve_worker_selection(
                        db,
                        owner_id=owner_id,
                        project_id=str(project_id) if project_id else None,
                        explicit_expert_id=str(winner["id"]),
                    )
                    expert_id = str(selection.expert["id"])
                    config_name = "worker_base"
                    context["expert_selection"] = {
                        "source": "legacy_name",
                        "expert_id": expert_id,
                    }
                    if selection.project_override:
                        config_override = deep_merge(
                            selection.project_override, config_override
                        )
        except Exception as e:
            # Explicit IDs and the unpinned default contract must fail loud.
            # Only a legacy name may safely fall through to a bundled config.
            if pinned_expert_id or legacy_uuid_id or config_name == "worker_base":
                raise
            logger.warning(
                "Automation expert resolution failed (using config_name): %s", e
            )

    owner_id = str(automation["owner_id"])
    target_project_ids = [str(project_id)] if project_id else []
    from orchestrator.services.manifest_workspace_selection import (
        select_project_workspace_default,
    )

    config_override, workspace_selection = await select_project_workspace_default(
        db,
        owner_id,
        project_id,
        config_override,
    )
    workspace_backend = _workspace_backend(config_override)

    # Automations intentionally store no connector selection in v1. Resolve
    # the owner's live defaults for every fire (cron and Run now), after the
    # effective project and workspace tier are known. The policy service also
    # verifies that the stored owner remains approved and a member of every
    # target project. Let that denial propagate: creating no job is safer than
    # running an unattended automation with a reduced or over-broad contract.
    datasource_ids, datasource_policy_revisions = await default_datasource_selection(
        db,
        owner_id,
        target_project_ids,
        workspace_backend,
    )

    job = await db.create_job(
        **(
            {"workspace_selection": workspace_selection}
            if workspace_selection is not None
            else {}
        ),
        origin="automation",
        description=automation["prompt"],
        config_name=config_name,
        config_override=config_override,
        context=context,
        user_id=owner_id,
        project_id=str(project_id) if project_id else None,
        priority=int(automation.get("priority", 5)),
        expert_id=expert_id,
        datasource_ids=datasource_ids,
        datasource_selection_provenance={
            "origin": "default",
            "creation_path": "automation",
            "trigger_kind": trigger_kind,
            "effective_work_owner_id": owner_id,
            "target_project_ids": target_project_ids,
            "datasource_ids": datasource_ids,
            "policy_revisions": datasource_policy_revisions,
            "resolved_at": datetime.now(timezone.utc).isoformat(),
        },
        datasource_policy_revisions=datasource_policy_revisions,
        authority_user_id=owner_id,
        authority_project_ids=target_project_ids,
    )

    logger.info(
        "Automation %s fired (%s) → job %s (datasource_defaults=%s)",
        automation_id,
        trigger_kind,
        job.get("id"),
        len(datasource_ids),
    )
    return job


__all__ = [
    "create_job_from_automation",
    "validate_automation_expert_selection",
]
