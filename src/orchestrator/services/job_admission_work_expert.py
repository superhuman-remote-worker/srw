"""Staff an unchosen worker from what the work asks for.

A work category is a property of the WORK, and ``default_expert`` maps it onto
a worker that can produce that work — an executor gets a shell. The backlog
tick always applied it (``work_categories.resolve_expert``: the ticket's pin,
else the category default); a job created through the admission funnel —
Officer hand-dispatch, the only mode in use while auto-pull is off — never did
and fell through to the application default, a worker with ``shell: []``. See
knowledge-base/knowledge/issues/category_expert_default_skipped_on_direct_dispatch.md.

Runs after the Officer stage, because only that stage knows the slot and the
claimed ticket. Performs no writes: the one probe is a dry run of the snapshot
renderer that job creation itself runs, so a default is never chosen that
creation would then refuse.
"""

from __future__ import annotations

from dataclasses import replace
import logging
from typing import TYPE_CHECKING, Any

from fastapi import HTTPException

from orchestrator.services.officer_admission import apply_prepared_slot_config
from orchestrator.services.work_categories import default_expert, normalize_category

if TYPE_CHECKING:
    from orchestrator.services.job_admission_config import (
        JobAdmissionConfig,
        PreviewExpertRefusals,
    )
    from orchestrator.services.job_admission_officer import JobAdmissionOfficer

logger = logging.getLogger(__name__)

#: Which pre-selected worker each default may displace. The ticket's pin is an
#: explicit choice about this work, so it outranks project and personal
#: defaults, as it does on the tick. The category default is only a better
#: fallback, so it displaces nothing a person configured.
_DISPLACES: dict[str, frozenset[str]] = {
    "ticket": frozenset({"scope", "fallback"}),
    "category": frozenset({"fallback"}),
}


async def apply_work_expert_default(
    config: JobAdmissionConfig,
    officer: JobAdmissionOfficer,
    *,
    requested_category: str | None,
    owner_id: str | None,
    preview_refusals: PreviewExpertRefusals,
) -> tuple[JobAdmissionConfig, JobAdmissionOfficer]:
    """Return the prepared pair, re-staffed when the work names a worker.

    Precedence: a named expert, then the claimed ticket's ``expert:`` pin, then
    a project or personal default, then the category default, then the
    deployment-wide application default.

    The category is the slot's, else the claimed ticket's, else the request's
    ``work_category``. The slot's category is the contract the worker is held
    to (§6) and is what the claim records; the ticket's is the durable
    description of the work, which ``resolve_expert`` reads on the tick; the
    request's is intent the kickoff names but never enforces.
    ``CATEGORY_EXPERTS`` membership is not consulted: it is warn-not-forbid.

    A default is chosen for the owner, not by them, so it must never turn a
    job that would have been created into one that is refused. Each candidate
    is previewed through the same snapshot renderer and grant decision
    creation runs; a refusal — missing grants, a retired bundle — or a failed
    preview keeps the pre-selected worker and is recorded under
    ``expert_selection.denied_defaults``.
    """
    if config.expert_source == "caller":
        return config, officer
    preparation = officer.preparation
    category = (
        normalize_category(preparation.category if preparation else None)
        or normalize_category(officer.ticket_category)
        or normalize_category(requested_category)
    )
    candidates: list[tuple[str, str]] = []
    if officer.ticket_expert:
        candidates.append(("ticket", officer.ticket_expert))
    if category is not None:
        candidates.append(("category", default_expert(category)))
    candidates = [
        (source, expert)
        for source, expert in candidates
        if config.expert_source in _DISPLACES[source]
    ]
    if not candidates:
        return config, officer

    # Displacing the pre-selected expert also drops its project_experts
    # overlay; the slot's pins are re-applied exactly as the Officer stage did.
    config_override = officer.config_override
    if config.unselected_config_override is not None:
        config_override = config.unselected_config_override
        if preparation is not None:
            config_override = apply_prepared_slot_config(config_override, preparation)

    denied: list[dict[str, Any]] = []
    for source, expert in candidates:
        try:
            refusals = await preview_refusals(
                config_name=expert,
                owner_id=owner_id,
                project_id=config.project_id,
                config_override=config_override,
            )
        except Exception as exc:
            # A preview outage must not become a failed create: the worker the
            # caller would have got without this stage is still admissible.
            logger.warning(
                "%s default expert %r could not be previewed (%s); keeping the "
                "pre-selected expert",
                source,
                expert,
                type(exc).__name__,
                exc_info=True,
            )
            reasons = [f"preview unavailable: {type(exc).__name__}"]
            denied.append({"source": source, "expert": expert, "reasons": reasons})
            continue
        if refusals:
            logger.warning(
                "%s default expert %r refused for owner %s (%s); keeping the "
                "pre-selected expert",
                source,
                expert,
                owner_id,
                ", ".join(sorted({reason.split(":", 1)[0] for reason in refusals})),
            )
            denied.append({"source": source, "expert": expert, "reasons": refusals})
            continue
        selection: dict[str, Any] = {"source": source, "expert": expert}
        if category is not None:
            selection["category"] = category
        if denied:
            selection["denied_defaults"] = denied
        officer.context["expert_selection"] = selection
        return (
            replace(
                config,
                config_name=expert,
                expert_id=None,
                expert_source="caller",
                unselected_config_override=None,
            ),
            replace(officer, config_override=config_override),
        )

    kept = dict(officer.context.get("expert_selection") or {"source": "fallback"})
    kept["denied_defaults"] = denied
    officer.context["expert_selection"] = kept
    return config, officer


async def preview_expert_refusals(
    db: Any,
    *,
    config_name: str,
    owner_id: str | None,
    project_id: str | None,
    config_override: dict[str, Any] | None,
) -> list[str]:
    """Why job creation would refuse ``config_name`` for this owner.

    A dry run of ``prepare_srw_snapshot`` — the renderer and grant decision
    ``postgres.create_job`` runs on every insert, and whose frozen policy
    dispatch re-checks — so this preview and the real admission cannot drift.
    Its inputs mirror ``create_admitted_job``'s: the default ``user`` runner
    and no workspace selection, whose catalogue lock belongs to the insert.
    """
    from uuid import uuid4

    from orchestrator.services.manifest_execution_snapshot import (
        ExecutionGrantDenied,
        prepare_srw_snapshot,
    )

    try:
        await prepare_srw_snapshot(
            db,
            work_kind="Job",
            work_id=str(uuid4()),
            owner_id=owner_id,
            project_ids=[project_id] if project_id else [],
            config_name=config_name,
            expert_id=None,
            config_override=config_override,
            description="",
            datasource_ids=[],
            policy_revisions={},
        )
    except ExecutionGrantDenied as denial:
        return list(denial.violations)
    except HTTPException as refusal:
        return [f"refused ({refusal.status_code}): {refusal.detail}"]
    return []


__all__ = ["apply_work_expert_default", "preview_expert_refusals"]
