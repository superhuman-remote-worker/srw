"""Agent-owned knowledge curation and convergence using auxiliary LLM tasks.

The common auxiliary executor accepts tools supplied by callers. This module
wires the agent's live knowledge tools and context into those reusable tasks.
"""

import logging
from typing import Any, List, Optional

from shared.runtime.services.auxiliary import (
    AssembleKnowledgeTask,
    AuxiliaryLLM,
    CurateKnowledgeTask,
    CurationResult,
    KnowledgeAssemblyResult,
)

logger = logging.getLogger(__name__)


def _curation_target(tool_context: Any) -> Optional[str]:
    """The knowledge base curation reads and writes: this work's own
    project's, and only when its connector is attached. A job that runs with
    another knowledge base bound, or with none, curates nothing — the
    context's project id alone names no knowledge base."""
    from agent.tools.knowledge.knowledge_tools import _get_project_id

    return _get_project_id(tool_context)


async def curate_and_store_knowledge(
    auxiliary_llm: "AuxiliaryLLM",
    tool_context: Any,
    phase_data: str,
    workspace_md: str,
    plan_md: str,
    curation_prompt: str,
    verdict_service: Any = None,
    verdict_prompt: Optional[str] = None,
) -> Optional["CurationResult"]:
    """Run inline knowledge curation via AuxiliaryLLM agent mode.

    Replaces the curator subjob. Extracts knowledge notes from phase artifacts
    and writes them to the project knowledge base (Neo4j + pgvector).

    Args:
        auxiliary_llm: AuxiliaryLLM instance
        tool_context: ToolContext with knowledge_graph and knowledge_store
        phase_data: Formatted phase context (archive path, completed todos)
        workspace_md: Current workspace.md content
        plan_md: Current plan.md content
        curation_prompt: System prompt for knowledge curation

    Returns:
        CurationResult on success, None on failure or if KB not available
    """
    try:
        kg = tool_context.knowledge_graph
        ks = tool_context.knowledge_store
        project_id = _curation_target(tool_context)

        # Neo4j is optional (kb_gardening G9, matching has_knowledge()): the
        # store is canonical for retrieval and the files for content. Before
        # this, a pod that could not reach the graph silently skipped every
        # curation pass.
        if not ks or not project_id:
            return None

        # Get existing notes for duplicate-aware context
        existing_notes = await _list_note_lines(kg, ks, project_id)

        # Create KB tools for the curation agent. When the verdict gate is wired
        # (curate_knowledge.verdict enabled), kb_write routes each candidate
        # through the ingestion verdict before writing (OKF KB slice 2 PR2).
        from agent.tools.knowledge.knowledge_tools import create_kb_tools

        kb_tools = create_kb_tools(
            tool_context,
            verdict_service=verdict_service,
            verdict_prompt=verdict_prompt,
        )

        task = CurateKnowledgeTask(
            phase_data=phase_data,
            workspace_md=workspace_md,
            plan_md=plan_md,
            existing_notes=existing_notes,
            kb_tools=kb_tools,
            prompt=curation_prompt,
        )

        result = await auxiliary_llm.agent(task)

        logger.info(
            f"Inline curation complete: {result.notes_created} created, "
            f"{result.notes_updated} updated — {result.summary}"
        )
        auxiliary_llm.health.record_success("knowledge_curation")
        return result

    except Exception as e:
        auxiliary_llm.health.record_failure("knowledge_curation", e)
        logger.warning(
            "Inline curation failed (non-fatal): %s: %s", type(e).__name__, e
        )
        return None


async def assemble_and_converge_knowledge(
    auxiliary_llm: "AuxiliaryLLM",
    tool_context: Any,
    knowledge_assembler_prompt: str,
    current_cycle: Optional[int] = None,
) -> Optional["KnowledgeAssemblyResult"]:
    """Re-verify the KB stale queue and converge it (KB convergence, F13).

    Counterpart to :func:`curate_and_store_knowledge`: while curation POPULATES
    the KB, this pass CONVERGES it — re-verifying notes whose cycle TTL ran out
    and superseding / merging / archiving the dead ones via kb_update. **Gated on
    a non-empty stale queue**: if nothing expired this returns immediately with no
    aux-LLM call. Survivors (stale notes the agent left ``active``) have their TTL
    reset by the store afterwards, so the queue drains.

    See knowledge-base/knowledge/features/kb_convergence_ttl_reverification.md.

    Args:
        auxiliary_llm: AuxiliaryLLM instance
        tool_context: ToolContext with knowledge_graph and knowledge_store
        knowledge_assembler_prompt: System prompt for the convergence pass
        current_cycle: Loop cycle number (total_jobs_run), stamped on survivors

    Returns:
        KnowledgeAssemblyResult on success, None if KB unavailable or queue empty
    """
    try:
        ks = tool_context.knowledge_store
        kg = tool_context.knowledge_graph  # optional (kb_gardening G9)
        project_id = _curation_target(tool_context)

        if not ks or not project_id:
            return None

        import uuid as _uuid

        project_uuid = (
            _uuid.UUID(project_id) if isinstance(project_id, str) else project_id
        )

        # kb_gardening G4: exactly one consolidator per KB at a time. Fan-out
        # loop members and orchestrator replicas all reach this seam; without
        # the claim they converge the same stale queue concurrently and reset
        # each other's TTLs. Non-blocking: the loser skips, the queue waits.
        async with ks.try_converge_lock(project_uuid) as claimed:
            if not claimed:
                logger.info(
                    "Knowledge assembly skipped for project %s: another "
                    "convergence pass holds the lock",
                    project_id,
                )
                return None
            return await _converge_locked(
                auxiliary_llm=auxiliary_llm,
                tool_context=tool_context,
                ks=ks,
                kg=kg,
                project_uuid=project_uuid,
                project_id=project_id,
                prompt=knowledge_assembler_prompt,
                current_cycle=current_cycle,
            )

    except Exception as e:
        auxiliary_llm.health.record_failure("knowledge_assembly", e)
        logger.warning(
            "Knowledge assembly failed (non-fatal): %s: %s", type(e).__name__, e
        )
        return None


#: Per-run ceiling on notes shown to the convergence pass (kb_gardening G4).
#: The stale queue may be deeper; the remainder waits for the next boundary.
KB_CONVERGE_MAX_NOTES_DEFAULT = 25
#: Blast-radius cap: never put more than this share of a KB's *active* notes
#: in front of the model in one run, whatever the queue holds (OpenClaw's
#: ``maxPriorEntryLossFraction`` is the precedent).
KB_CONVERGE_MAX_ACTIVE_SHARE = 0.25


def _converge_max_notes() -> int:
    import os

    try:
        return max(
            1,
            int(os.getenv("KB_CONVERGE_MAX_NOTES", str(KB_CONVERGE_MAX_NOTES_DEFAULT))),
        )
    except (TypeError, ValueError):
        return KB_CONVERGE_MAX_NOTES_DEFAULT


async def _list_note_lines(
    kg: Any, ks: Any, project_id: Any, limit: int = 50
) -> List[str]:
    """``- id: title (type)`` lines for the aux tasks' context, from the graph
    when there is one, else from the store (kb_gardening G9)."""
    try:
        if kg:
            notes = kg.list_notes(project_id=project_id, limit=limit)
        else:
            import uuid as _uuid

            kb = _uuid.UUID(project_id) if isinstance(project_id, str) else project_id
            notes = await ks.list_notes(kb, status="active", limit=limit)
        return [
            f"- {n.get('id', '?')}: {n.get('title', '?')} ({n.get('type', '?')})"
            for n in notes
            if isinstance(n, dict)
        ]
    except Exception as e:
        logger.debug(f"Could not fetch notes for aux context: {e}")
        return []


async def _converge_locked(
    *,
    auxiliary_llm: "AuxiliaryLLM",
    tool_context: Any,
    ks: Any,
    kg: Any,
    project_uuid: Any,
    project_id: Any,
    prompt: str,
    current_cycle: Optional[int],
) -> Optional["KnowledgeAssemblyResult"]:
    """The convergence pass proper, run by the lock holder."""
    max_notes = _converge_max_notes()

    # Gate: only run when something actually expired (no LLM call otherwise).
    stale = await ks.get_stale_notes(project_uuid, limit=max_notes)
    if not stale:
        return None

    # Blast-radius cap against the live corpus size.
    active_total = 0
    try:
        summary = await ks.get_summary(project_uuid)
        active_total = int((summary or {}).get("active") or 0)
    except Exception:
        active_total = 0
    if active_total:
        share_cap = max(1, int(active_total * KB_CONVERGE_MAX_ACTIVE_SHARE))
        if len(stale) > share_cap:
            logger.info(
                "Knowledge assembly: capping this run to %d of %d stale notes "
                "(%d active in the KB)",
                share_cap,
                len(stale),
                active_total,
            )
            stale = stale[:share_cap]

    stale_lines = [
        f"- {n.note_id} [{n.note_type}] {n.title}: {(n.content or '')[:300]}"
        for n in stale
    ]

    # Other active notes give the agent context for dedup / supersede calls.
    stale_ids = {n.note_id for n in stale}
    related_lines = [
        line
        for line in await _list_note_lines(kg, ks, project_id)
        if line.split(":", 1)[0].removeprefix("- ") not in stale_ids
    ]

    from agent.tools.knowledge.knowledge_tools import create_kb_tools

    kb_tools = create_kb_tools(tool_context)

    task = AssembleKnowledgeTask(
        stale_notes=stale_lines,
        related_notes=related_lines,
        kb_tools=kb_tools,
        prompt=prompt,
    )

    result = await auxiliary_llm.agent(task)

    # Deterministic TTL bookkeeping: any stale note STILL active survived
    # re-verification → reset its TTL. refresh_ttl's status filter skips the
    # ones the agent superseded / archived, so the queue drains either way.
    refreshed = await ks.refresh_ttl(project_uuid, stale, current_cycle=current_cycle)

    # The run report (G4): what the pass looked at and what it did, in one
    # greppable line, so a wrong retirement can be traced to its run.
    logger.info(
        "Knowledge assembly [project %s, cycle %s]: %d stale shown (queue cap %d), "
        "%d refreshed, retired/superseded=%d, merged=%d — %s | notes: %s",
        project_id,
        current_cycle,
        len(stale),
        max_notes,
        refreshed,
        len(stale) - refreshed,
        getattr(result, "notes_merged", 0) or 0,
        getattr(result, "summary", ""),
        ", ".join(sorted(stale_ids)),
    )
    auxiliary_llm.health.record_success("knowledge_assembly")
    return result
