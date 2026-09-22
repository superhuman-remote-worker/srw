"""Prepare manual Officer dispatch without acquiring admission authority.

The caller supplies an authenticated scope and resolved job configuration.
Candidate detection preserves the existing best-effort thread/lineage reads;
Officer candidates must then pass the existing coherent post snapshot policy.
Ticket reads happen before the final app transaction. The returned snapshot
never replaces locked revalidation, capacity, claim and exact job insertion in
``officer_admission.admit_and_create_job``. HTTP exceptions remain the current
compatibility contract, as in the preceding admission stages.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from typing import Any, Awaitable, Callable, Protocol, TYPE_CHECKING

from fastapi import HTTPException

from orchestrator.services.officer_admission import (
    OfficerAdmissionConflict,
    OfficerAdmissionPreparation,
    SlotAdmissionError,
    apply_prepared_slot_config,
)
from orchestrator.services.officer_metadata import (
    officer_meta_enabled,
    thread_officer_meta,
)

if TYPE_CHECKING:
    from orchestrator.schemas.job_create import JobCreate
    from orchestrator.services.job_admission_config import JobAdmissionConfig

logger = logging.getLogger(__name__)


class JobOfficerStore(Protocol):
    async def get_thread(self, thread_id: str) -> dict[str, Any] | None: ...

    async def get_project_officer_lineage(self, project_id: str) -> list[str]: ...


class PrepareOfficerSnapshot(Protocol):
    async def __call__(
        self,
        *,
        project_id: str,
        thread_id: str,
        requested_slot: str | None,
        requested_config_override: dict[str, Any] | None,
    ) -> OfficerAdmissionPreparation: ...


@dataclass(frozen=True)
class JobAdmissionOfficerDependencies:
    store: JobOfficerStore
    prepare_officer: PrepareOfficerSnapshot
    fetch_ticket: Callable[[str, str], Awaitable[dict[str, Any] | None]]


@dataclass(frozen=True)
class JobAdmissionOfficer:
    """Prepared inputs only; no claim, transaction or provisioning has occurred."""

    context: dict[str, Any]
    config_override: dict[str, Any] | None
    preparation: OfficerAdmissionPreparation | None
    ticket_ready_at: datetime | None
    # The claimed ticket's validated ``expert:`` pin. It outranks the
    # category default, as in ``work_categories.resolve_expert``.
    ticket_expert: str | None = None


async def prepare_job_admission_officer(
    *,
    command: JobCreate,
    config: JobAdmissionConfig,
    dependencies: JobAdmissionOfficerDependencies,
) -> JobAdmissionOfficer:
    context = config.context
    project_id = config.project_id
    config_override = config.config_override
    request_config_override = config.request_config_override
    root_creation = config.root_creation

    # Officer admission preparation (BP-02/BP-03/BP-04). Expensive grant,
    # datasource and provisioning inputs are resolved after this snapshot
    # but before the authoritative transaction. The caller's final INSERT
    # locks project_officers -> current thread, revalidates this exact
    # incarnation/config/lineage, recomputes all-non-terminal capacity and
    # writes the job on that same connection. Ordinary sessions never take
    # the post lock.
    officer_slot_name: str | None = None
    officer_admission_preparation = None
    officer_ticket_ready_at: datetime | None = None
    officer_ticket_expert: str | None = None
    thread_id = (
        str(command.thread_id) if (command.thread_id and root_creation) else None
    )
    if thread_id:
        try:
            _admit_thread = await dependencies.store.get_thread(thread_id)
        except Exception:
            _admit_thread = None
        officer_meta = thread_officer_meta(_admit_thread or {})

        # Every ordinary session materializes officer.enabled=false, so
        # that flag alone cannot distinguish a retired officer from a
        # plain session. The durable post lineage can. Enabled orphan /
        # duplicate threads are also candidates so the authoritative
        # post check refuses them instead of letting them dispatch as an
        # ordinary session.
        _officer_lineage_member = False
        if project_id:
            try:
                _officer_lineage_member = thread_id in set(
                    await dependencies.store.get_project_officer_lineage(project_id)
                )
            except Exception:
                _officer_lineage_member = False
        if officer_meta_enabled(officer_meta) or _officer_lineage_member:
            if not project_id:
                raise HTTPException(
                    status_code=409,
                    detail="Officer dispatch requires its durable project post.",
                )
            requested_slot = context.get("officer_slot")
            try:
                officer_admission_preparation = await dependencies.prepare_officer(
                    project_id=project_id,
                    thread_id=thread_id,
                    requested_slot=str(requested_slot) if requested_slot else None,
                    requested_config_override=request_config_override,
                )
            except OfficerAdmissionConflict as exc:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": exc.code,
                        "message": exc.detail,
                        **exc.fields,
                    },
                ) from exc
            except SlotAdmissionError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            officer_slot_name = officer_admission_preparation.slot_name
            if officer_slot_name:
                context["officer_slot"] = officer_slot_name
            config_override = apply_prepared_slot_config(
                config_override, officer_admission_preparation
            )
            # One claim ledger for both dispatch paths. Without this the
            # officer manually working the top ready ticket races his own
            # tick into double-work on the very next cycle
            # (officer_backlog_pools.md §5.3).
            if command.ticket:
                context["ticket_note_id"] = str(command.ticket)

                # ``ticket=`` selects a current ready backlog note; it
                # never supplies dispatch authority. Resolve the exact
                # project-scoped row and its database-owned generation
                # before the short app-Postgres transaction. The final
                # post lock consumes this value atomically with the claim
                # and job INSERTs. No ready_at field is model-selectable.
                from orchestrator.services.work_categories import (
                    BACKLOG_NOTE_TYPES,
                    classify_ticket,
                )

                try:
                    ticket_state = await dependencies.fetch_ticket(
                        str(project_id),
                        str(command.ticket),
                    )
                except Exception as exc:
                    raise HTTPException(
                        status_code=503,
                        detail=(
                            "Backlog ticket authority is unavailable; "
                            "no claim or job was created."
                        ),
                    ) from exc
                if ticket_state is None:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"Backlog ticket '{command.ticket}' does not exist "
                            "in this Officer Post's project."
                        ),
                    )
                if str(ticket_state.get("project_id") or "") != str(project_id):
                    raise HTTPException(
                        status_code=409,
                        detail="Backlog ticket belongs to a different project.",
                    )
                if (
                    str(ticket_state.get("status") or "") != "active"
                    or str(ticket_state.get("note_type") or "")
                    not in BACKLOG_NOTE_TYPES
                ):
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"Backlog ticket '{command.ticket}' is not an "
                            "active backlog ticket."
                        ),
                    )
                classification = classify_ticket(ticket_state.get("tags"))
                if classification.problems:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"Backlog ticket '{command.ticket}' is ambiguous: "
                            + "; ".join(classification.problems)
                        ),
                    )
                ready_value = ticket_state.get("ready_at")
                if not classification.ready or not isinstance(ready_value, datetime):
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"Backlog ticket '{command.ticket}' is not ready "
                            "with trusted Officer provenance."
                        ),
                    )
                officer_ticket_ready_at = (
                    ready_value
                    if ready_value.tzinfo
                    else ready_value.replace(tzinfo=timezone.utc)
                )
                officer_ticket_expert = classification.expert

            # Precedence law (§6): the SLOT's category decides the contract
            # this worker is held to. Explicit model/backend choices must
            # match the slot (validated above and again under the Post
            # lock); they are never silently replaced. A cross-category
            # dispatch — sending a slot into work its category does not
            # describe — remains warn-not-forbid and is named in the
            # kickoff instead.
            _slot_category = officer_admission_preparation.category
            if _slot_category:
                context.setdefault("work_category", _slot_category)
                context["kickoff_message"] = compose_category_kickoff(
                    _slot_category,
                    context.get("kickoff_message"),
                    requested_category=command.work_category,
                    slot=officer_slot_name,
                    thread_id=thread_id,
                )

    if command.ticket and officer_admission_preparation is None:
        raise HTTPException(
            status_code=409,
            detail=(
                "Backlog ticket claims require the exact current commissioned "
                "Officer Post incarnation. Ad-hoc jobs must omit ticket."
            ),
        )

    return JobAdmissionOfficer(
        context=context,
        config_override=config_override,
        preparation=officer_admission_preparation,
        ticket_ready_at=officer_ticket_ready_at,
        ticket_expert=officer_ticket_expert,
    )


def compose_category_kickoff(
    category: str,
    existing_kickoff: str | None,
    *,
    requested_category: str | None = None,
    slot: str | None = None,
    thread_id: str | None = None,
) -> str:
    """Prepend the slot's category contract to an officer-authored kickoff.

    The contract goes in the KICKOFF, never in ``instructions`` — that
    parameter replaces the rendered instructions.md template wholesale, which
    would cost the worker everything else it needs to operate.

    A mismatch between what the officer asked for and what the slot pins is
    stated in the text rather than refused (§6 warn-not-forbid). Silence would
    be the bad outcome: the worker would read an executor's delivery contract
    while sitting in a researcher slot and have no way to know which one the
    officer meant.
    """
    from orchestrator.services.work_categories import category_block, normalize_category

    parts = [category_block(category)]
    asked = normalize_category(requested_category)
    if asked and asked != category:
        note = (
            f"NOTE: the officer dispatched this as {asked} work into the "
            f"{slot or category} slot, whose contract is {category} — the "
            "contract above is the one you are held to. If that reads as a "
            "mistake, say so in your completion report rather than guessing."
        )
        parts.append(note)
        logger.info(
            "officer=%s slot=%s cross-category dispatch: asked=%s slot_contract=%s",
            str(thread_id or "")[:8],
            slot,
            asked,
            category,
        )
    if existing_kickoff:
        parts.append(str(existing_kickoff))
    return "\n\n".join(parts)
