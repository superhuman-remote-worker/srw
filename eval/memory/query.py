"""Question-time retrieval: one assemble() against the ingested project.

Mirrors the live read path (a fresh "answering session" scoped to the
question's project, query = the question text), then maps each injected
memory back to the haystack session it came from (job_id provenance) and
scores the collapsed session ranking against the evidence labels.
"""

import logging
import uuid
from typing import Any, Dict, List

from eval.memory.arms import ArmSpec
from eval.memory.datasets import LMEQuestion
from eval.memory.infra import project_uuid, session_uuid
from eval.memory.ingest import HarnessHandles, _form_query_text
from eval.memory.metrics import collapse_to_sessions, question_metrics
from shared.runtime.core.context_entries import memory_handle
from shared.runtime.services.recall_store import RecallStore

logger = logging.getLogger(__name__)

#: Pseudo session id for the answering-side scope — never ingested, so no
#: memory rows ever carry it.
READER_SESSION = "__reader__"

#: Keep result rows reviewable: full ranking capped, per-item detail capped.
MAX_RANKED_SESSIONS = 50
MAX_ITEM_DETAIL = 15


async def _load_provenance(db: Any, project: uuid.UUID) -> Dict[str, uuid.UUID]:
    """memory row id (str) → source job uuid, for the whole project scope."""
    rows = await db.fetch(
        "SELECT id, job_id FROM memories WHERE project_id = $1", project
    )
    return {str(row["id"]): row["job_id"] for row in rows}


def session_ranking(
    payload: Any,
    record_to_job: Dict[str, uuid.UUID],
    job_to_session: Dict[uuid.UUID, str],
) -> List[str]:
    """Injected memory items, in injection order, collapsed to session ids.

    Items whose record can't be resolved (shouldn't happen — every
    ingested row carries its session's job_id) are skipped with a
    warning rather than silently polluting the ranking.
    """
    ranked: List[str] = []
    for block in payload.blocks:
        if block.kind != "memory":
            continue
        for item in block.items:
            record_id = str(item.get("record_id"))
            job = record_to_job.get(record_id)
            sid = job_to_session.get(job) if job is not None else None
            if sid is None:
                logger.warning("Unresolvable injected memory %s", record_id)
                continue
            ranked.append(sid)
    return collapse_to_sessions(ranked)


async def answer_retrieval(
    question: LMEQuestion,
    arm: ArmSpec,
    handles: HarnessHandles,
) -> dict:
    """Run the question against the ingested store; return the result row."""
    from langchain_core.messages import HumanMessage

    from agent.services.memory import AssembleRequest

    project = project_uuid(handles.run_id, question.question_id)
    reader_job = session_uuid(handles.run_id, question.question_id, READER_SESSION)
    store = handles.make_store(reader_job, project)
    manager = handles.make_manager(store, reader_job, project)

    # Same flag switch as the live read path: the answering session is one
    # HumanMessage deep, so legacy and digest both reduce to the question
    # text — wiring it keeps digest arms honest end-to-end.
    query_text = _form_query_text(handles, [HumanMessage(content=question.question)])

    payload = await manager.assemble(AssembleRequest(query_text=query_text))

    job_to_session = {
        session_uuid(handles.run_id, question.question_id, s.session_id): s.session_id
        for s in question.sessions
    }
    record_to_job: Dict[str, uuid.UUID] = {}
    if handles.db is not None:
        record_to_job = await _load_provenance(handles.db, project)

    ranked = session_ranking(payload, record_to_job, job_to_session)
    metrics = question_metrics(ranked, question.evidence_session_ids, ks=arm.ks)

    memory_items = [
        item
        for block in payload.blocks
        if block.kind == "memory"
        for item in block.items
    ]
    item_detail = [
        {
            "record_id": item.get("record_id"),
            "session": job_to_session.get(
                record_to_job.get(str(item.get("record_id")))
            ),
            "tokens": item.get("token_count"),
            # Final combined score (scorers write it; 0.0 when no scorer
            # ran) — kept per item so threshold/calibration analysis works
            # post-hoc from results.jsonl alone.
            "score": item.get("score"),
        }
        for item in memory_items[:MAX_ITEM_DETAIL]
    ]

    stats = payload.stats
    return {
        "question_id": question.question_id,
        "question_type": question.question_type,
        "is_abstention": question.is_abstention,
        "question": question.question,
        "answer": question.answer,
        "question_date": question.question_date,
        # The memory entry body production would append — captured here
        # because --cleanup deletes the rows, and the end-task reader/judge
        # slice needs the context post-hoc.
        "injected_context": "\n\n".join(
            RecallStore.render_memory_entry(
                [(record, memory_handle(record.id), False) for record in block.records]
            )
            for block in payload.blocks
            if block.kind == "memory" and block.records
        ),
        "evidence": sorted(question.evidence_session_ids),
        "ranked_sessions": ranked[:MAX_RANKED_SESSIONS],
        "metrics": metrics,
        "cost": {
            "tokens_injected": stats.tokens_injected,
            "assemble_latency_ms": round(stats.latency_ms, 2),
            "memories_injected": len(memory_items),
            "candidates_total": stats.candidates_total,
        },
        "items": item_detail,
        "assemble_errors": list(stats.errors),
    }
