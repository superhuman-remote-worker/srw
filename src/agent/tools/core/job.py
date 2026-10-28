"""Job lifecycle tools for the Universal Agent.

This module provides completion signaling tools:
- mark_complete: Signals that a task/phase is complete (used by phase transitions)
- job_complete: Marks the phase as final, job completes after remaining todos are done

The job_complete tool implements a final phase pattern (journal-before-observe,
knowledge-base/knowledge/issues/job_finalization_decisions_held_only_in_process_memory.md):
1. Rejects if called from tactical phase (must be in strategic phase)
2. Durably journals the decision on the job row (orchestrator POST, idempotent
   on (job_id, tool_call_id)) BEFORE returning to the model
3. Caches the decision in ``_final_phase_data`` (process cache, not the
   source of truth); the audited tool node mirrors it into graph state
   (``is_final_phase=True`` + ``completion_decision``) so a checkpoint that
   contains the tool result also contains the decision
4. Agent completes remaining strategic todos (summarize, update plan)
5. When all todos complete, on_strategic_phase_complete detects the final
   phase and freezes the job
"""

import json
import logging
import uuid as _uuid
from datetime import datetime, timezone
from typing import Annotated, Any, Dict, List, Optional

from langchain_core.tools import InjectedToolCallId, tool

from shared.runtime.core.workspace_backend import WorkspaceUnavailableError
from agent.tools.context import ToolContext

from shared.tool_catalog.definitions import (
    JOB_TOOLS_METADATA as JOB_TOOLS_METADATA,
)

logger = logging.getLogger(__name__)


# Process-level CACHE of the final phase data (set by job_complete, read by
# finalize_job). The durable record lives on the job row
# (``jobs.context.completion_decision``, written through the orchestrator
# before the tool returns); this dict only saves the common no-restart case a
# DB round-trip. Re-seeded on resume by agent-side hydration.
_final_phase_data: Dict[str, Dict[str, Any]] = {}


# Tool metadata for registry
# Phase availability:
#   - "strategic": Only in strategic mode (planning)
#   - "tactical": Only in tactical mode (execution)
#   - Both: Available in both modes (default if not specified)


def create_job_tools(context: ToolContext) -> List[Any]:
    """Create job lifecycle tools.

    Args:
        context: Tool context with workspace manager

    Returns:
        List of job tools

    Raises:
        ValueError: If context doesn't have a workspace_manager
    """
    if not context.has_workspace():
        raise ValueError("ToolContext must have a workspace_manager for job tools")

    workspace = context.workspace_manager

    @tool
    async def mark_complete(
        summary: str,
        deliverables: List[str],
        confidence: float = 1.0,
        notes: Optional[str] = None,
    ) -> str:
        """Write a completion report to output/completion.json.

        This records your assessment of task completion but does NOT stop
        the agent loop. Use job_complete instead to actually finish the
        job and stop execution.

        Args:
            summary: Brief description of what was accomplished (1-3 sentences)
            deliverables: List of output files or artifacts created (e.g., ["output/requirements.json", "notes/analysis.md"])
            confidence: Your confidence the task is truly complete (0.0-1.0, default 1.0)
            notes: Optional notes about limitations, assumptions, or follow-up suggestions

        Returns:
            Confirmation message
        """
        try:
            # Validate confidence
            confidence = max(0.0, min(1.0, confidence))

            # Build completion report
            completion_data = {
                "status": "completed",
                "timestamp": datetime.now().isoformat(),
                "summary": summary,
                "deliverables": deliverables,
                "confidence": confidence,
            }

            if notes:
                completion_data["notes"] = notes

            # Write to output/completion.json
            output_path = "output/completion.json"
            workspace.write_file(
                output_path, json.dumps(completion_data, indent=2, ensure_ascii=False)
            )

            logger.info(f"Task marked complete: {summary}")
            logger.info(f"Deliverables: {deliverables}")

            # Return message that triggers completion detection
            return f"Wrote file: output/completion.json - Task complete. Summary: {summary}"

        except WorkspaceUnavailableError:
            # Same lifecycle-signal rule as job_complete below: this tool also
            # writes to the workspace, so a dead VM must propagate rather than
            # become a result string. (Defect 8)
            raise
        except Exception as e:
            logger.error(f"Failed to mark complete: {e}")
            return f"Error marking complete: {str(e)}"

    @tool
    async def job_complete(
        summary: str,
        deliverables: List[str],
        confidence: float = 1.0,
        notes: Optional[str] = None,
        evidence: Optional[List[Dict[str, str]]] = None,
        tool_call_id: Annotated[Optional[str], InjectedToolCallId] = None,
    ) -> str:
        """Signal that the job is complete and ready for human review.

        This tool marks the current strategic phase as "final". The job will
        complete after all remaining strategic todos are done (summarize,
        record learnings, update plan.md).

        IMPORTANT: This tool can only be called during a strategic phase.
        If called from tactical phase, it will be rejected.

        Args:
            summary: Brief description of what was accomplished across all phases (2-5 sentences)
            deliverables: List of ALL output files created during the job
            confidence: Overall confidence the job is truly complete (0.0-1.0, default 1.0)
            notes: Optional notes about limitations, edge cases, or recommendations
            evidence: Optional declared evidence entries for supervision review.
                Each entry: {"kind": "test_report"|"screenshot"|"change_summary",
                "label": "<short label>", "media_type": "<MIME type>",
                "source": "<workspace-relative committed file path>"}. The
                orchestrator resolves each path at the completion commit and
                publishes it in the job's bounded evidence manifest.

        Returns:
            Confirmation that phase is marked as final, or error if in tactical phase
        """
        try:
            # Check if we're in a strategic phase
            is_strategic = True
            if context.has_todo():
                is_strategic = context.todo_manager.is_strategic_phase

            if not is_strategic:
                logger.warning("job_complete rejected: called from tactical phase")
                return (
                    "ERROR: job_complete can only be called during a strategic phase.\n\n"
                    "Complete all tactical todos first. When all tactical todos are done,\n"
                    "you will automatically transition to a strategic phase where you can\n"
                    "call job_complete."
                )

            # Background children and their committed-but-not-yet-absorbed
            # reports are part of this parent job.  Refuse the local decision
            # before it clears staged work or journals success.  The
            # orchestrator repeats this check transactionally; this process
            # seam gives the model the useful explanation without relying on
            # a race-prone local flag for correctness.
            runtime = getattr(context, "subagent_runtime", None)
            blockers = getattr(runtime, "has_completion_blockers", None)
            if callable(blockers):
                try:
                    blocked = bool(blockers())
                except Exception:
                    logger.exception(
                        "job_complete could not inspect background subagents"
                    )
                    return (
                        "Error: background-subagent completion state could not "
                        "be verified. The job is NOT marked as final. Retry "
                        "after the runtime recovers; reports push automatically."
                    )
                if blocked:
                    return (
                        "ERROR: job_complete is blocked while a background "
                        "subagent is queued/running or a completed child report "
                        "has not yet been absorbed. Reports push automatically; "
                        "continue useful work or wait once, then incorporate the "
                        "evidence before completing the job."
                    )

            # Check if already in final phase
            if context.job_id in _final_phase_data:
                logger.info(
                    f"job_complete called again for job {context.job_id} - already marked as final"
                )
                return (
                    "Phase is already marked as final. Complete your remaining todos\n"
                    "to finish the job."
                )

            # Auto-clear stale staged todos — the agent explicitly decided the job is done.
            if context.has_todo() and context.todo_manager.has_staged_todos():
                context.todo_manager.clear_staged_todos()
                logger.info("job_complete: cleared stale staged todos")

            # Validate confidence
            confidence = max(0.0, min(1.0, confidence))

            # Validation gate: cross-check deliverables before accepting.
            # Paths are NORMALIZED, not pedantically matched (F14): a missing
            # or extra `repo/` prefix (or `./`) must never fail a seal when the
            # file exists under either spelling — that exact rejection forced a
            # COMPLETE job (58027ee7) into a 0.45 honest-floor seal.
            # knowledge-base/knowledge/issues/officer_blind_reads_and_worker_bureaucracy.md
            from agent.core.deliverables import (
                KB_DELIVERABLE_PREFIX,
                resolve_workspace_deliverable,
            )

            validation_warnings = []
            found_paths: List[str] = []

            # Check that listed deliverables exist and are non-empty
            for deliverable in deliverables:
                resolved, found = resolve_workspace_deliverable(workspace, deliverable)
                if resolved and resolved.startswith(KB_DELIVERABLE_PREFIX):
                    # Knowledge-note deliverable — verified server-side by the
                    # orchestrator's gate, not against the workspace.
                    continue
                if not found:
                    validation_warnings.append(
                        f"Deliverable '{deliverable}' does not exist"
                    )
                else:
                    found_paths.append(resolved)
                    try:
                        content = workspace.read_file(resolved)
                        if len(content.strip()) < 50:
                            validation_warnings.append(
                                f"Deliverable '{deliverable}' appears empty or trivial ({len(content)} bytes)"
                            )
                    except WorkspaceUnavailableError:
                        # Not a "bad deliverable" — the whole workspace is gone.
                        # Degrading it to a validation warning would report the
                        # job's own output as unreadable and reject its
                        # completion. Propagate. (Defect 8)
                        raise
                    except Exception:
                        validation_warnings.append(
                            f"Deliverable '{deliverable}' could not be read"
                        )

            # Reject high confidence with validation warnings
            if validation_warnings and confidence > 0.5:
                warning_list = "\n".join(f"  - {w}" for w in validation_warnings)
                return (
                    f"ERROR: Cannot accept confidence {confidence:.0%} with deliverable issues:\n"
                    f"{warning_list}\n\n"
                    "Either fix the deliverables, or lower confidence below 0.5 to acknowledge "
                    "the issues. Re-call job_complete after addressing these problems."
                )

            # Delivery: Markdown deliverables get footnote definitions for the
            # citation handles they use, and the report states which citations
            # are still unverified (citation engine overhaul WP4).
            citation_delivery = await _deliver_citations(
                context, workspace, found_paths
            )

            # Build the decision record. tool_call_id is the idempotency
            # discriminator; ToolNode injects the real one, direct invocations
            # (tests, scripts) get a synthesized local id.
            final_data = {
                "summary": summary,
                "deliverables": deliverables,
                "confidence": confidence,
                "job_id": context.job_id,
                "tool_call_id": tool_call_id or f"local-{_uuid.uuid4().hex[:12]}",
                "recorded_at": datetime.now(timezone.utc).isoformat(),
            }
            if notes:
                final_data["notes"] = notes
            if citation_delivery and citation_delivery["citations"]["total"]:
                final_data["citations"] = citation_delivery["citations"]
            # E4: declared evidence rides the freeze into the completion
            # contract; the ORCHESTRATOR resolves/pins/measures each entry —
            # nothing here is trusted beyond being a declaration.
            if evidence:
                final_data["evidence"] = [
                    dict(entry) for entry in evidence if isinstance(entry, dict)
                ][:20]

            # Journal-before-observe: the decision must be durable BEFORE the
            # model sees success. Nothing is cached locally until the
            # orchestrator has committed it — a failure here reports
            # NOT-recorded so the model can retry, never a false success.
            client = getattr(context, "orchestrator_client", None)
            if client is not None:
                from agent.api.orchestrator_client import CompletionDecisionError

                try:
                    journal = await client.record_completion_decision(
                        job_id=context.job_id,
                        tool_call_id=final_data["tool_call_id"],
                        summary=summary,
                        deliverables=deliverables,
                        confidence=confidence,
                        notes=notes,
                    )
                    if journal.get("replay"):
                        logger.info(
                            f"Completion decision for job {context.job_id} was "
                            f"already journaled (tool_call_id="
                            f"{final_data['tool_call_id']}) — replay no-op"
                        )
                except CompletionDecisionError as e:
                    logger.error(
                        f"Completion decision for job {context.job_id} could "
                        f"NOT be journaled: {e}"
                    )
                    return (
                        f"Error: the completion decision could NOT be durably "
                        f"recorded ({e}). The job is NOT marked as final. "
                        f"Re-call job_complete to retry; if this persists, "
                        f"report the problem instead of proceeding."
                    )
            else:
                logger.warning(
                    f"job_complete for job {context.job_id}: no orchestrator "
                    f"client — decision held in process memory only (NOT "
                    f"crash-durable)"
                )

            _final_phase_data[context.job_id] = final_data

            logger.info(f"Job {context.job_id} marked as final phase")
            logger.info(f"Summary: {summary}")
            logger.info(f"Deliverables: {deliverables}")

            # Check if there are no pending todos - if so, job will complete immediately
            # after this tool returns (on_strategic_phase_complete will be called)
            pending_count = 0
            if context.has_todo():
                pending = context.todo_manager.list_pending()
                pending_count = len(pending)

            citation_line = _citation_delivery_line(citation_delivery)
            if pending_count == 0:
                return (
                    "Phase marked as final. No remaining todos - job will complete now.\n\n"
                    f"Summary: {summary}\n"
                    f"Deliverables: {len(deliverables)} files\n"
                    f"Confidence: {confidence:.0%}" + citation_line
                )
            else:
                return (
                    f"Phase marked as final. Complete your {pending_count} remaining todo(s) to finish the job.\n\n"
                    f"Summary: {summary}\n"
                    f"Deliverables: {len(deliverables)} files\n"
                    f"Confidence: {confidence:.0%}" + citation_line + "\n\n"
                    "Once all todos are complete, the job will be frozen for human review."
                )

        except WorkspaceUnavailableError:
            # A dead workspace is a LIFECYCLE signal, not a tool error. Swallowing
            # it into a result string is what let job c6dd288d call job_complete
            # five times over 13 minutes against an already-deleted VM before an
            # unrelated tool finally let the exception propagate. Re-raise so the
            # fast-freeze path classifies it.
            # knowledge-base/knowledge/issues/transient_db_error_hard_fails_job_and_destroys_vm.md (Defect 8)
            raise
        except Exception as e:
            logger.error(f"Failed to mark job as final: {e}")
            return f"Error marking job as final: {str(e)}"

    return [mark_complete, job_complete]


async def _deliver_citations(
    context: ToolContext, workspace: Any, paths: List[str]
) -> Optional[Dict[str, Any]]:
    """Footnotes for the Markdown deliverables; None when the job cites nothing.

    Best-effort: a citation problem never blocks completion. A dead workspace
    still propagates (Defect 8).
    """
    from agent.citation_engine.delivery import MARKDOWN_SUFFIXES, deliver_documents
    from shared.citation_handles import handles_in

    markdown = [p for p in paths if p.lower().endswith(MARKDOWN_SUFFIXES)]
    try:
        engine = getattr(context, "citation_engine", None)
        if engine is None:
            # The engine is built on first use; after a restart a job can have
            # citations in its documents without having cited in this process.
            if not any(handles_in(workspace.read_file(p)) for p in markdown):
                return None
            engine = context.get_citation_engine()
        return await deliver_documents(engine, workspace, markdown)
    except WorkspaceUnavailableError:
        raise
    except Exception as e:
        logger.warning(f"Citation footnotes at completion skipped: {e}")
        return None


def _citation_delivery_line(delivery: Optional[Dict[str, Any]]) -> str:
    """What job_complete tells the agent about its citations."""
    if not delivery:
        return ""
    report = delivery["citations"]
    files = delivery["files"]
    line = ""
    if files:
        names = ", ".join(sorted(files))
        line += f"\nCitations: footnotes for {sum(files.values())} handle(s) appended to {names}."
    if report["unverified_count"]:
        handles = ", ".join(u["handle"] for u in report["unverified"])
        line += (
            f"\n{report['unverified_count']} citation(s) are UNVERIFIED and are "
            f"listed in the completion report: {handles}. Fix them with "
            "edit_citation or withdraw them with withdraw_citation if you can."
        )
    return line


def get_final_phase_data(job_id: str) -> Optional[Dict[str, Any]]:
    """Get the final phase data for a job, if it exists.

    Called by phase.py to check if a job has been marked as final.

    Args:
        job_id: The job ID to check

    Returns:
        Final phase data dict if job is marked as final, None otherwise
    """
    return _final_phase_data.get(job_id)


def clear_final_phase_data(job_id: str) -> None:
    """Clear the final phase data for a job.

    Called by phase.py after job finalization is complete.

    Args:
        job_id: The job ID to clear
    """
    if job_id in _final_phase_data:
        del _final_phase_data[job_id]
        logger.debug(f"Cleared final phase data for job {job_id}")


def seed_final_phase_data(job_id: str, decision: Dict[str, Any]) -> None:
    """Re-seed the process cache from the durable record (resume hydration).

    Called by the agent's resume path after fetching the journaled decision
    from the orchestrator, so a restarted process finalizes with the recorded
    decision instead of treating "I decided" as "no decision was made". Never
    called on feedback resumes — those demand new work and void the decision.

    Args:
        job_id: The job ID to seed
        decision: The journaled decision record
    """
    _final_phase_data[job_id] = dict(decision)
    logger.info(
        f"Hydrated completion decision for job {job_id} from the durable "
        f"record (tool_call_id={decision.get('tool_call_id')}, "
        f"recorded_at={decision.get('recorded_at')})"
    )
