"""Legacy current-behaviour plugins — the Phase-1 transplant.

These reproduce the pre-seam read path exactly (a strangler refactor,
not an improvement — agent_memory_overhaul.md §5 Phase 1): same store
calls, same arguments, same containment semantics as the inline blocks
in graph.py's execute node and persistent_graph.py's _execute_turn. The
equivalence fixtures (tests/test_memory_worker_equivalence.py) pin the
assembled payload against the legacy sequence byte-for-byte.

Honest composite names by design: the §6 sketch names (dense, sparse,
rrf, …) describe the Phase-3+ decomposition, but today RRF fusion lives
*inside* the SQL functions — `recall_two_tier` and `kb_notes` are the
real units of current behaviour.
"""

import asyncio
import logging
import uuid
from typing import Any, Awaitable, List, Optional

from langchain_core.messages import BaseMessage

from agent.services.memory.registry import register_memory_plugin
from agent.services.memory.types import (
    AssembleRequest,
    Candidate,
    MemoryRuntime,
    TaskFrame,
)
from shared.runtime.core.context_entries import last_user_text

logger = logging.getLogger(__name__)


async def _bounded(coro: Awaitable[Any], timeout: Optional[float]) -> Any:
    """Apply the legacy persistent per-call guard; unbounded when None."""
    if timeout is None:
        return await coro
    return await asyncio.wait_for(coro, timeout=timeout)


def build_worker_query_text(frame: TaskFrame) -> str:
    """Legacy worker query formation (graph.py execute node, transplanted).

    Top pending todo + phase descriptor, space-joined. The unified
    request digest (§4) replaces this behind `memory.query.digest`
    (default off) in a later phase — until measured, this stays the
    worker's exact query.
    """
    parts: List[str] = []
    if frame.top_todo:
        parts.append(frame.top_todo)
    parts.append(
        f"phase {frame.phase_number} "
        f"{'strategic' if frame.is_strategic else 'tactical'}"
    )
    return " ".join(parts)


def build_persistent_query_text(messages: List[BaseMessage]) -> str:
    """Legacy persistent query formation (persistent_graph._execute_turn).

    The most recent HumanMessage's content, string-coerced (multimodal
    content lists become their str() form, exactly as the legacy scan
    does); empty string when no HumanMessage exists — the legacy path
    still retrieves with "" rather than skipping. Injected context is
    skipped, so retrieval never queries with its own earlier results.
    """
    return last_user_text(messages)


class RecallTwoTierRetriever:
    """Wraps RecallStore.retrieve() unchanged: TTL-pinned tier first, then
    hybrid search, budget-fit to the store's configured budget.

    Also transplants the decrement-then-retrieve TTL tick the legacy
    execute path performs (a write-on-read; Phase 3's always-inject core
    replaces TTL-pinning, until then the tick must keep happening exactly
    once per assemble). The decrement has its own containment — a failed
    tick never blocks retrieval, matching the legacy split try/excepts.

    ``timeout`` reproduces the persistent path's per-call 5 s guard
    (None for the worker path, which runs unbounded): a timed-out
    retrieve propagates to the manager's containment, skipping memory
    while the KB retriever still runs — same outcome as the legacy
    split wait_fors.
    """

    def __init__(self, recall_store: Any, timeout: Optional[float] = None) -> None:
        self.recall_store = recall_store
        self.timeout = timeout

    async def retrieve(self, req: AssembleRequest) -> List[Candidate]:
        store = self.recall_store
        if store is None:
            return []

        # A read-only retrieval (the session's idle-time prefetch,
        # ``AssembleRequest.ttl_tick=False``) leaves the TTL tier alone: the
        # turn's own retrieval keeps the one tick per turn.
        if getattr(req, "ttl_tick", True) is not False:
            try:
                await _bounded(store.decrement_ttl(), self.timeout)
            except Exception as e:
                logger.warning(
                    "TTL decrement failed (non-fatal): %s: %s", type(e).__name__, e
                )

        # No budget kwarg — the legacy call relies on the store's
        # constructed budget; passing req.budget_tokens here would change
        # behaviour before Phase 3's unified budget measures it.
        memories = await _bounded(store.retrieve(req.query_text), self.timeout)
        return [
            Candidate(
                kind="memory",
                text=memory.content,
                token_count=memory.token_count or 0,
                record=memory,
            )
            for memory in memories
        ]


class KbNotesRetriever:
    """Wraps KnowledgeStore.hybrid_search(): top-5 notes for the scope.

    Worker passes a single project_id, persistent a list — hybrid_search
    normalizes both to the same single/multi SQL paths, so one retriever
    serves both graphs' legacy calling conventions. token_count stays 0:
    the legacy KB block is uncapped (B5) — Phase 3 unifies budgets.
    """

    def __init__(
        self,
        knowledge_store: Any,
        project_id: Optional[str] = None,
        project_ids: Optional[List[str]] = None,
        timeout: Optional[float] = None,
    ) -> None:
        self.knowledge_store = knowledge_store
        self.project_id = project_id
        self.project_ids = list(project_ids or [])
        self.timeout = timeout

    def _effective_project_uuids(self) -> List[uuid.UUID]:
        effective = self.project_ids or ([self.project_id] if self.project_id else [])
        return [
            pid if isinstance(pid, uuid.UUID) else uuid.UUID(str(pid))
            for pid in effective
        ]

    async def retrieve(self, req: AssembleRequest) -> List[Candidate]:
        if self.knowledge_store is None:
            return []
        project_uuids = self._effective_project_uuids()
        if not project_uuids:
            return []

        notes = await _bounded(
            self.knowledge_store.hybrid_search(
                project_ids=project_uuids,
                query=req.query_text,
                match_count=5,
            ),
            self.timeout,
        )
        return [
            Candidate(kind="knowledge", text=note.content, token_count=0, record=note)
            for note in notes
        ]


@register_memory_plugin(
    "retriever",
    "recall_two_tier",
    description="Legacy two-tier RecallStore.retrieve (TTL-pinned + hybrid, budget-fit)",
)
def _build_recall_two_tier(runtime: MemoryRuntime) -> RecallTwoTierRetriever:
    if runtime.recall_store is None:
        # Legacy semantics: no store (memory disabled) = quietly no
        # memories — but say so once at bind time instead of never.
        logger.info(
            "recall_two_tier bound without a recall_store — contributes nothing"
        )
    return RecallTwoTierRetriever(
        runtime.recall_store, timeout=runtime.retrieval_timeout
    )


@register_memory_plugin(
    "retriever",
    "kb_notes",
    description="Legacy top-5 project knowledge notes (hybrid search)",
)
def _build_kb_notes(runtime: MemoryRuntime) -> KbNotesRetriever:
    if runtime.knowledge_store is None:
        logger.info("kb_notes bound without a knowledge_store — contributes nothing")
    elif not (runtime.project_ids or runtime.project_id):
        logger.info("kb_notes bound without project scope — contributes nothing")
    return KbNotesRetriever(
        runtime.knowledge_store,
        project_id=runtime.project_id,
        project_ids=runtime.project_ids,
        timeout=runtime.retrieval_timeout,
    )
