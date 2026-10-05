"""RecallStore — Memory Light Phase 1: storage + retrieval.

Provides hybrid search (dense vector + sparse keyword + recency) over
agent memories stored in PostgreSQL with pgvector.

See knowledge-base/knowledge/features/memory_light.md for full architecture.

Usage:
    ```python
    from shared.runtime.services.recall_store import RecallStore
    from shared.runtime.services.embedding_service import get_embedding_service

    store = RecallStore(
        db=postgres_db,
        embedding_service=get_embedding_service(),
        job_id=job_id,
        config=agent_config.memory,
    )

    # Store a memory
    mem_id = await store.store(
        content="The users table uses soft deletes via deleted_at.",
        summary="Users table has soft deletes",
        keywords=["users", "soft_delete", "deleted_at"],
        importance=0.8,
    )

    # Retrieve relevant memories
    memories = await store.retrieve(
        context_text="How should I query the users table?",
        budget_tokens=5000,
    )
    ```
"""

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

from shared.runtime.core.context_entries import UPDATED_ITEM_MARKER

logger = logging.getLogger(__name__)


# Mirrors the ``valid_memory_type`` CHECK constraint in
# orchestrator/database/vector_schema.sql. Held as a Python constant so the two
# cannot drift silently, and so an out-of-set value is COERCED rather than
# raising CheckViolationError and discarding the whole memory.
#
# ``memory_type`` is LLM-authored (the extractor's ``mem.type``), so it is
# untrusted input: job c6dd288d lost an extraction to
# ``CheckViolationError: valid_memory_type`` on the value "factial" — a typo for
# "factual". knowledge-base/knowledge/issues/transient_db_error_hard_fails_job_and_destroys_vm.md
# (Defect 7).
#
# Deliberately NOT fuzzy nearest-match: "factial" is obviously "factual", but
# nearest-match could silently mis-file a genuinely wrong type. A known default
# plus a loud log keeps the mistake visible.
VALID_MEMORY_TYPES = frozenset(
    {"factual", "procedural", "error_solution", "vocabulary", "relational"}
)
DEFAULT_MEMORY_TYPE = "factual"


def coerce_memory_type(memory_type: Optional[str]) -> str:
    """Return a constraint-safe ``memory_type``, logging any substitution."""
    if memory_type in VALID_MEMORY_TYPES:
        return memory_type
    logger.warning(
        "Invalid memory_type %r coerced to %r (valid: %s) — the memory is kept; "
        "before this guard the CHECK constraint discarded the whole row",
        memory_type,
        DEFAULT_MEMORY_TYPE,
        ", ".join(sorted(VALID_MEMORY_TYPES)),
    )
    return DEFAULT_MEMORY_TYPE


# English stopwords — small hardcoded set for keyword extraction
_STOPWORDS = frozenset(
    {
        "a",
        "an",
        "the",
        "and",
        "or",
        "but",
        "in",
        "on",
        "at",
        "to",
        "for",
        "of",
        "with",
        "by",
        "from",
        "is",
        "was",
        "are",
        "were",
        "be",
        "been",
        "being",
        "have",
        "has",
        "had",
        "do",
        "does",
        "did",
        "will",
        "would",
        "could",
        "should",
        "may",
        "might",
        "shall",
        "can",
        "this",
        "that",
        "these",
        "those",
        "it",
        "its",
        "i",
        "me",
        "my",
        "we",
        "our",
        "you",
        "your",
        "he",
        "she",
        "his",
        "her",
        "they",
        "them",
        "their",
        "not",
        "no",
        "if",
        "then",
        "else",
        "when",
        "up",
        "out",
        "so",
        "as",
        "all",
        "each",
        "every",
        "both",
        "few",
        "more",
        "most",
        "other",
        "some",
        "such",
        "into",
        "over",
        "after",
        "before",
        "between",
        "under",
        "about",
        "than",
    }
)


def extract_keywords(text: str, max_keywords: int = 8) -> List[str]:
    """Extract keywords from text by filtering stopwords.

    Splits on whitespace/punctuation, removes stopwords, lowercases,
    deduplicates, and returns the first ``max_keywords`` tokens.

    Args:
        text: Input text
        max_keywords: Maximum number of keywords to return

    Returns:
        List of keyword strings
    """
    import re

    tokens = re.split(r"[\s\W]+", text.lower())
    seen: set = set()
    result: List[str] = []
    for token in tokens:
        if len(token) < 2 or token in _STOPWORDS or token in seen:
            continue
        seen.add(token)
        result.append(token)
        if len(result) >= max_keywords:
            break
    return result


@dataclass
class MemoryRecord:
    """A single memory entry."""

    id: Optional[uuid.UUID] = None
    job_id: Optional[uuid.UUID] = None
    project_id: Optional[uuid.UUID] = None
    agent_id: Optional[str] = None
    content: str = ""
    summary: Optional[str] = None
    memory_type: str = "factual"
    source: str = "observer"
    keywords: List[str] = field(default_factory=list)
    embedding: Optional[List[float]] = None
    importance: float = 0.5
    source_turn_start: Optional[int] = None
    source_turn_end: Optional[int] = None
    source_phase: Optional[int] = None
    token_count: int = 0
    access_count: int = 0
    remaining_turns: Optional[int] = None
    created_at: Optional[datetime] = None
    last_accessed: Optional[datetime] = None
    # Bi-temporal supersede (overhaul Phase 4, migration vector/0006).
    # valid_to IS NULL == currently valid == served by default retrieval.
    valid_from: Optional[datetime] = None
    valid_to: Optional[datetime] = None
    superseded_at: Optional[datetime] = None
    superseded_by: Optional[uuid.UUID] = None
    # Transient: populated by similarity searches (find_similar_many), not a
    # stored column. Lets the ingestion adjudicator see how close each
    # neighbour is.
    similarity: Optional[float] = None

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "MemoryRecord":
        """Create a MemoryRecord from a database row dict."""
        return cls(
            id=row.get("id"),
            job_id=row.get("job_id"),
            project_id=row.get("project_id"),
            agent_id=row.get("agent_id"),
            content=row.get("content", ""),
            summary=row.get("summary"),
            memory_type=row.get("memory_type", "factual"),
            source=row.get("source", "observer"),
            keywords=row.get("keywords") or [],
            importance=row.get("importance", 0.5),
            source_turn_start=row.get("source_turn_start"),
            source_turn_end=row.get("source_turn_end"),
            source_phase=row.get("source_phase"),
            token_count=row.get("token_count", 0),
            access_count=row.get("access_count", 0),
            remaining_turns=row.get("remaining_turns"),
            created_at=row.get("created_at"),
            last_accessed=row.get("last_accessed"),
            valid_from=row.get("valid_from"),
            valid_to=row.get("valid_to"),
            superseded_at=row.get("superseded_at"),
            superseded_by=row.get("superseded_by"),
            similarity=row.get("similarity"),
        )


# Sleep between access-stat write retries after a deadlock. Module-level so
# tests can zero it; length bounds the retry count (len + 1 attempts total).
_ACCESS_STAT_RETRY_DELAYS = (0.05, 0.15)


def _is_deadlock(exc: BaseException) -> bool:
    """Match asyncpg's DeadlockDetectedError without importing asyncpg."""
    return type(exc).__name__ == "DeadlockDetectedError"


class MemoryHealth:
    """Process-wide counters for contained memory-store failures.

    Concurrent same-project jobs deadlock on the shared memory rows (138
    contained retrieval deadlocks in one five-job batch — see
    knowledge-base/knowledge/issues/project_scoped_memory_deadlocks_under_parallel_jobs.md).
    Containment keeps the jobs alive but was visible only in pod logs; these
    counters ride the agent heartbeat into ``agents.metadata`` so contained
    degradation reaches operator telemetry. Counting only — never control flow.
    """

    _KINDS = (
        "ttl_decrement_deadlock",
        "access_stats_deadlock",
        "access_stats_error",
        "retrieval_deadlock",
    )

    def __init__(self) -> None:
        self._counts: Dict[str, int] = dict.fromkeys(self._KINDS, 0)

    def increment(self, kind: str) -> None:
        self._counts[kind] = self._counts.get(kind, 0) + 1

    def snapshot(self) -> Optional[Dict[str, int]]:
        """Nonzero counters only; None when all zero (healthy = no payload)."""
        counts = {kind: n for kind, n in self._counts.items() if n}
        return counts or None

    def reset(self) -> None:
        """Zero all counters (tests)."""
        self._counts = dict.fromkeys(self._KINDS, 0)


memory_health = MemoryHealth()


class RecallStore:
    """Memory storage and retrieval with hybrid search.

    Manages agent memories in PostgreSQL with three retrieval channels:
    1. Dense vector search (cosine similarity via pgvector)
    2. Sparse keyword search (PostgreSQL tsvector/tsquery)
    3. Recency (most recently created memories)

    Results are fused via Reciprocal Rank Fusion (RRF) using the
    `memory_hybrid_search()` SQL function.
    """

    def __init__(
        self,
        db,
        embedding_service,
        job_id: uuid.UUID,
        config=None,
        agent_id: Optional[str] = None,
        project_id: Optional[uuid.UUID] = None,
        project_ids: Optional[List[uuid.UUID]] = None,
        archiver=None,
        strict_writes: bool = False,
    ):
        """Initialize RecallStore.

        Args:
            db: PostgresDB instance (agent-side or orchestrator-side)
            embedding_service: EmbeddingService for generating vectors
            job_id: Job UUID (memories are scoped per job)
            config: MemoryConfig dataclass (optional, uses defaults if None)
            agent_id: Optional agent identifier for cross-job memory (Phase 5)
            project_id: Optional project UUID for project-scoped memory sharing
            project_ids: Optional list of project UUIDs for multi-project sessions
            archiver: Optional LLMArchiver for audit logging
            strict_writes: Propagate auxiliary sub-write failures instead of
                containing them. Intended for callers that wrap the complete
                memory mutation in their own transaction (for example the
                stateless-session destination ledger); defaults to the legacy
                best-effort behavior everywhere else.
        """
        self.db = db
        self.embedding_service = embedding_service
        self.job_id = job_id
        self.agent_id = agent_id
        self.project_id = project_id
        self.project_ids = project_ids or ([project_id] if project_id else [])
        self._archiver = archiver
        self.strict_writes = bool(strict_writes)
        self.project_scoped = (
            getattr(config, "project_scoped", False) if config else False
        )
        # Ingestion-verdict adjudicator (overhaul Phase 4). Late-bound by
        # src.services.memory.ingestion.maybe_attach_ingestion_verdict at the
        # manager-construction sites when memory.ingestion.enabled. While None
        # (the default everywhere today), store() keeps the legacy cosine-0.85
        # dedup-merge byte-for-byte — the equivalence suites pin that path.
        self.ingestion_verdict = None

        # Config defaults (matches MemoryConfig dataclass)
        self.dedup_threshold = 0.85
        self.importance_threshold = 0.3
        self.budget_tokens = 10000
        self.max_memories_per_injection = 150
        self.retrieval_importance_floor = 0.4
        self.default_ttl = 10
        # Write-time importance gate (overhaul Phase 4). True = legacy floor;
        # memory.extraction.write_gate: false drops it (completeness over
        # precision — relevance is gated at retrieval instead).
        self.write_gate = True
        # Pre-insert dedup re-check threshold (memory-extraction Slice 0). The
        # verdict path re-runs the neighbour lookup at this high cosine floor
        # immediately before an ADD insert; a twin that appeared *since* the
        # first check (the dual-trigger race, §4.7) downgrades the ADD to a
        # NOOP+bump. High by design — only a near-identical concurrent write
        # should collapse; genuinely distinct neighbours the verdict already
        # cleared for ADD must not be re-bumped.
        self.recheck_threshold = 0.9

        if config is not None:
            self.dedup_threshold = getattr(config, "dedup_threshold", 0.85)
            self.importance_threshold = getattr(config, "importance_threshold", 0.3)
            self.budget_tokens = getattr(config, "budget_tokens", 10000)
            self.max_memories_per_injection = getattr(
                config, "max_memories_per_injection", 150
            )
            self.retrieval_importance_floor = getattr(
                config, "retrieval_importance_floor", 0.4
            )
            self.default_ttl = getattr(config, "default_ttl", 10)
            self.recheck_threshold = getattr(config, "recheck_threshold", 0.9)
            _ext = getattr(config, "extraction", None)
            if _ext is not None:
                self.write_gate = getattr(_ext, "write_gate", True)

    @property
    def _scope_filter(self):
        """Return (column, value) for scoping queries by project or job."""
        if self.project_scoped and self.project_ids:
            if len(self.project_ids) == 1:
                return "project_id", self.project_ids[0]
            return "project_id", self.project_ids
        if self.project_scoped and self.project_id:
            return "project_id", self.project_id
        return "job_id", self.job_id

    def _scope_where(self, param_index: int):
        """Return (WHERE clause fragment, param value) for scoping.

        Handles both single-value (= $N) and multi-value (= ANY($N)) scoping.
        """
        scope_col, scope_val = self._scope_filter
        if isinstance(scope_val, list):
            return f"{scope_col} = ANY(${param_index})", scope_val
        return f"{scope_col} = ${param_index}", scope_val

    # =========================================================================
    # Storage
    # =========================================================================

    async def store(
        self,
        content: str,
        summary: Optional[str] = None,
        keywords: Optional[List[str]] = None,
        importance: float = 0.5,
        memory_type: str = "factual",
        source: str = "observer",
        source_turn_start: Optional[int] = None,
        source_turn_end: Optional[int] = None,
        source_phase: Optional[int] = None,
        token_count: Optional[int] = None,
        remaining_turns: Optional[int] = None,
        retrieval_messages: Optional[List[str]] = None,
    ) -> Optional[uuid.UUID]:
        """Store a memory with automatic embedding and dedup.

        If a semantically similar memory already exists (cosine > dedup_threshold),
        updates the existing memory's access_count, last_accessed, and TTL instead.

        Args:
            content: Memory content text
            summary: One-line summary (optional)
            keywords: Keywords for sparse search
            importance: Importance score 0-1
            memory_type: factual, procedural, error_solution, vocabulary, relational
            source: observer, todo, compaction, phase_archive, tool_error
            source_turn_start: Start turn of source conversation
            source_turn_end: End turn of source conversation
            source_phase: Phase number when extracted
            token_count: Pre-counted tokens (estimated if None)
            remaining_turns: TTL in turns (default: self.default_ttl)

        Returns:
            UUID of stored/updated memory, or None if below importance threshold
        """
        if self.write_gate and importance < self.importance_threshold:
            logger.debug(
                f"Memory below importance threshold ({importance} < "
                f"{self.importance_threshold}), skipping"
            )
            return None

        # Generate embedding
        embedding = await self.embedding_service.embed(content)

        # Ingestion-verdict write path (overhaul Phase 4): aux-LLM adjudication
        # + bi-temporal supersede, replacing the cosine dedup-merge below. Only
        # active when a verdict service is wired (memory.ingestion.enabled);
        # otherwise the legacy path runs byte-for-byte (equivalence suites).
        if self.ingestion_verdict is not None:
            return await self._store_with_verdict(
                embedding=embedding,
                content=content,
                summary=summary,
                keywords=keywords,
                importance=importance,
                memory_type=memory_type,
                source=source,
                source_turn_start=source_turn_start,
                source_turn_end=source_turn_end,
                source_phase=source_phase,
                token_count=token_count,
                remaining_turns=remaining_turns,
                retrieval_messages=retrieval_messages,
            )

        # Legacy dedup: a near-duplicate (cosine > dedup_threshold) bumps the
        # existing row in place instead of inserting a new one.
        existing = await self.find_similar(embedding, self.dedup_threshold)
        if existing:
            logger.debug(
                f"Dedup: updating existing memory {existing.id} instead of creating new"
            )
            return await self._bump_existing(
                existing.id,
                importance,
                remaining_turns,
                source,
                similarity=existing.similarity,
            )

        return await self._insert(
            embedding=embedding,
            content=content,
            summary=summary,
            keywords=keywords,
            importance=importance,
            memory_type=memory_type,
            source=source,
            source_turn_start=source_turn_start,
            source_turn_end=source_turn_end,
            source_phase=source_phase,
            token_count=token_count,
            remaining_turns=remaining_turns,
            retrieval_messages=retrieval_messages,
        )

    async def _insert(
        self,
        *,
        embedding: List[float],
        content: str,
        summary: Optional[str] = None,
        keywords: Optional[List[str]] = None,
        importance: float = 0.5,
        memory_type: str = "factual",
        source: str = "observer",
        source_turn_start: Optional[int] = None,
        source_turn_end: Optional[int] = None,
        source_phase: Optional[int] = None,
        token_count: Optional[int] = None,
        remaining_turns: Optional[int] = None,
        retrieval_messages: Optional[List[str]] = None,
    ) -> Optional[uuid.UUID]:
        """INSERT a new memory row — the shared tail of every ADD path."""
        # Estimate token count if not provided (~4 chars per token)
        if token_count is None:
            token_count = len(content) // 4

        ttl = remaining_turns if remaining_turns is not None else self.default_ttl

        # Single funnel for all three store paths — coerce here so no caller can
        # bypass the CHECK constraint guard. (Defect 7)
        memory_type = coerce_memory_type(memory_type)

        keywords_list = keywords or []
        keywords_text = " ".join(keywords_list) + " " + content

        mem_id = await self.db.fetchval(
            """
            INSERT INTO memories (
                job_id, project_id, agent_id, content, summary, memory_type, source,
                keywords, embedding, sparse_keywords,
                importance, source_turn_start, source_turn_end, source_phase,
                token_count, remaining_turns
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7,
                $8, $9, to_tsvector('english', $10),
                $11, $12, $13, $14,
                $15, $16
            )
            RETURNING id
            """,
            self.job_id,
            self.project_id,
            self.agent_id,
            content,
            summary[:500] if summary else None,
            memory_type,
            source,
            keywords_list,
            embedding,
            keywords_text,
            importance,
            source_turn_start,
            source_turn_end,
            source_phase,
            token_count,
            ttl,
        )

        # Store retrieval messages (trigger phrases) if provided
        if retrieval_messages and mem_id:
            try:
                rm_count = await self.store_retrieval_messages(
                    mem_id, retrieval_messages
                )
                logger.debug(
                    f"Stored {rm_count} retrieval messages for memory {mem_id}"
                )
            except Exception as e:
                if self.strict_writes:
                    raise
                logger.warning(f"Failed to store retrieval messages for {mem_id}: {e}")

        logger.debug(
            f"Stored memory {mem_id} (type={memory_type}, source={source}, "
            f"importance={importance})"
        )
        if self._archiver:
            self._archiver.audit_step(
                job_id=str(self.job_id),
                agent_type=self.agent_id or "",
                step_type="memory_store",
                node_name="recall_store",
                iteration=0,
                data={
                    "id": str(mem_id),
                    "type": memory_type,
                    "source": source,
                    "importance": importance,
                    "tokens": token_count,
                },
            )
        return mem_id

    async def _bump_existing(
        self,
        existing_id: uuid.UUID,
        importance: float,
        remaining_turns: Optional[int],
        source: str,
        similarity: Optional[float] = None,
    ) -> uuid.UUID:
        """Bump an existing memory in place (legacy dedup hit / verdict NOOP)."""
        ttl = remaining_turns if remaining_turns is not None else self.default_ttl
        await self.db.execute(
            """
            UPDATE memories
            SET access_count = access_count + 1,
                last_accessed = CURRENT_TIMESTAMP,
                importance = GREATEST(importance, $1),
                remaining_turns = GREATEST(COALESCE(remaining_turns, 0), $3)
            WHERE id = $2
            """,
            importance,
            existing_id,
            ttl,
        )
        if self._archiver:
            self._archiver.audit_step(
                job_id=str(self.job_id),
                agent_type=self.agent_id or "",
                step_type="memory_dedup",
                node_name="recall_store",
                iteration=0,
                data={
                    "existing_id": str(existing_id),
                    "source": source,
                    "similarity": (
                        round(similarity, 3) if similarity is not None else None
                    ),
                },
            )
        return existing_id

    async def _store_with_verdict(
        self,
        *,
        embedding: List[float],
        content: str,
        summary: Optional[str],
        keywords: Optional[List[str]],
        importance: float,
        memory_type: str,
        source: str,
        source_turn_start: Optional[int],
        source_turn_end: Optional[int],
        source_phase: Optional[int],
        token_count: Optional[int],
        remaining_turns: Optional[int],
        retrieval_messages: Optional[List[str]],
    ) -> Optional[uuid.UUID]:
        """Adjudicate a candidate against neighbours, then ADD/NOOP/UPDATE/MERGE.

        Cost guard: the verdict LLM is consulted only when a neighbour scores at
        or above the service's review_floor. No near-duplicate → straight ADD,
        zero LLM calls.
        """
        svc = self.ingestion_verdict

        def _add():
            return self._insert(
                embedding=embedding,
                content=content,
                summary=summary,
                keywords=keywords,
                importance=importance,
                memory_type=memory_type,
                source=source,
                source_turn_start=source_turn_start,
                source_turn_end=source_turn_end,
                source_phase=source_phase,
                token_count=token_count,
                remaining_turns=remaining_turns,
                retrieval_messages=retrieval_messages,
            )

        async def _add_guarded(exclude):
            """ADD terminal with a pre-insert dedup re-check (§4.7 race guard).

            Between the first neighbour lookup and this insert, a concurrent
            writer (the async interval extractor vs this boundary flush) can
            commit the same fact, so both find no dup and both ADD. Re-run the
            lookup at ``recheck_threshold`` immediately before inserting, reusing
            the already-computed embedding (a cheap SELECT, no re-embed). A twin
            that appeared *since* the first check — i.e. one not in ``exclude``,
            the set of neighbours the verdict already cleared for ADD — bumps
            instead of inserting. Excluding the adjudicated set keeps the guard
            from second-guessing an explicit ADD verdict, so a non-racing write
            sees the same state twice and behaves exactly as before.
            """
            twin = await self._recheck_twin(embedding, exclude)
            if twin is not None:
                self._audit_verdict("NOOP", [twin.id], None, "recheck-twin")
                return await self._bump_existing(
                    twin.id, importance, remaining_turns, source, twin.similarity
                )
            return await _add()

        try:
            similar = await self.find_similar_many(
                embedding, k=svc.top_k, min_similarity=svc.review_floor
            )
        except Exception as e:
            if self.strict_writes:
                raise
            logger.warning(
                "Ingestion verdict: neighbour lookup failed (%s: %s); ADD",
                type(e).__name__,
                e,
            )
            return await _add_guarded(frozenset())

        existing_ids = {r.id for r in similar}

        # Cost guard: nothing close enough to adjudicate → new fact, just ADD
        # (still race-guarded: a concurrent identical write may have committed).
        if not similar:
            return await _add_guarded(existing_ids)

        verdict = await svc.adjudicate(
            content=content,
            summary=summary,
            keywords=keywords,
            similar=similar,
        )
        action = (getattr(verdict, "action", "ADD") or "ADD").strip().upper()
        reason = getattr(verdict, "reason", "") or ""

        targets = []
        for idx in getattr(verdict, "target_indices", None) or []:
            if isinstance(idx, int) and 1 <= idx <= len(similar):
                targets.append(similar[idx - 1])

        if action == "NOOP":
            target = targets[0] if targets else similar[0]
            self._audit_verdict("NOOP", [target.id], None, reason)
            return await self._bump_existing(
                target.id, importance, remaining_turns, source, target.similarity
            )

        if action in ("UPDATE", "MERGE"):
            retire = [t.id for t in targets]
            if not retire:
                # UPDATE/MERGE must name the rows it supersedes; a verdict that
                # names none is malformed. Degrade to a conservative ADD rather
                # than guess a row to retire — wrongly retiring loses a fact,
                # keeping both only costs a (downstream-gated) duplicate.
                self._audit_verdict("ADD", [], None, f"{action}-without-targets→ADD")
                return await _add_guarded(existing_ids)
            if action == "MERGE":
                merged = (getattr(verdict, "merged_content", None) or "").strip()
                if merged and merged != content:
                    new_emb = await self.embedding_service.embed(merged)
                    new_id = await self._insert(
                        embedding=new_emb,
                        content=merged,
                        summary=summary,
                        keywords=keywords,
                        importance=importance,
                        memory_type=memory_type,
                        source=source,
                        source_turn_start=source_turn_start,
                        source_turn_end=source_turn_end,
                        source_phase=source_phase,
                        token_count=None,  # re-estimate from merged content
                        remaining_turns=remaining_turns,
                        retrieval_messages=retrieval_messages,
                    )
                else:
                    new_id = await _add()
            else:
                new_id = await _add()
            if new_id:
                await self.supersede(retire, new_id)
                self._audit_verdict(action, retire, new_id, reason)
            return new_id

        # ADD and any unrecognized action → conservative ADD (race-guarded).
        self._audit_verdict("ADD", [], None, reason)
        return await _add_guarded(existing_ids)

    async def _recheck_twin(self, embedding, exclude):
        """Re-run the neighbour lookup just before an ADD insert (§4.7 race guard).

        Reuses ``embedding`` (no re-embed) to find the closest currently-valid
        neighbour at or above ``recheck_threshold`` whose id is **not** in
        ``exclude`` (the neighbours the verdict already adjudicated). A hit is a
        twin that a concurrent writer committed since the first check; the caller
        bumps it instead of inserting a duplicate. Best-effort — a lookup failure
        returns ``None`` so the ADD still proceeds (never lose a write to the
        guard).
        """
        try:
            neighbours = await self.find_similar_many(
                embedding,
                k=len(exclude) + 1,
                min_similarity=self.recheck_threshold,
            )
        except Exception as e:
            if self.strict_writes:
                raise
            logger.debug(
                "Pre-insert dedup re-check failed (%s: %s); proceeding with ADD",
                type(e).__name__,
                e,
            )
            return None
        for rec in neighbours:
            if rec.id not in exclude:
                return rec
        return None

    def _audit_verdict(self, action, target_ids, new_id, reason):
        """Audit an ingestion verdict (no-op without an archiver)."""
        if not self._archiver:
            return
        self._archiver.audit_step(
            job_id=str(self.job_id),
            agent_type=self.agent_id or "",
            step_type="memory_verdict",
            node_name="recall_store",
            iteration=0,
            data={
                "action": action,
                "retired": [str(t) for t in target_ids],
                "new_id": str(new_id) if new_id else None,
                "reason": reason[:200],
            },
        )

    async def store_retrieval_messages(
        self,
        memory_id: uuid.UUID,
        messages: List[str],
    ) -> int:
        """Store retrieval messages (trigger phrases) for a memory.

        Each message is embedded and stored so that hybrid search can match
        against "when would this memory be useful?" queries in addition to
        the memory's own content embedding.

        Args:
            memory_id: UUID of the parent memory
            messages: List of trigger phrase strings

        Returns:
            Number of messages successfully stored
        """
        if not messages:
            return 0

        # Batch embed all messages in one API call
        embeddings = await self.embedding_service.embed_batch(messages)
        if self.strict_writes and len(embeddings) != len(messages):
            raise RuntimeError(
                "retrieval-message embedding count does not match input count"
            )

        stored = 0
        for msg, emb in zip(messages, embeddings):
            try:
                await self.db.execute(
                    """
                    INSERT INTO memory_retrieval_messages (memory_id, message, embedding)
                    VALUES ($1, $2, $3)
                    """,
                    memory_id,
                    msg,
                    emb,
                )
                stored += 1
            except Exception as e:
                if self.strict_writes:
                    raise
                logger.warning(
                    f"Failed to store retrieval message for {memory_id}: {e}"
                )

        return stored

    # =========================================================================
    # Deduplication
    # =========================================================================

    async def find_similar(
        self,
        embedding: List[float],
        threshold: Optional[float] = None,
    ) -> Optional[MemoryRecord]:
        """Find a semantically similar existing memory.

        Args:
            embedding: Query embedding vector
            threshold: Cosine similarity threshold (default: self.dedup_threshold)

        Returns:
            Most similar MemoryRecord if above threshold, else None
        """
        threshold = threshold if threshold is not None else self.dedup_threshold

        scope_clause, scope_val = self._scope_where(2)
        row = await self.db.fetchrow(
            f"""
            SELECT *, 1 - (embedding <=> $1) AS similarity
            FROM memories
            WHERE {scope_clause}
              AND embedding IS NOT NULL
              AND valid_to IS NULL
              AND 1 - (embedding <=> $1) > $3
            ORDER BY similarity DESC
            LIMIT 1
            """,
            embedding,
            scope_val,
            threshold,
        )

        if row:
            return MemoryRecord.from_row(dict(row))
        return None

    async def find_similar_many(
        self,
        embedding: List[float],
        k: int = 5,
        min_similarity: float = 0.6,
    ) -> List[MemoryRecord]:
        """Return the top-``k`` currently-valid neighbours above ``min_similarity``.

        Feeds the ingestion adjudicator (overhaul Phase 4). Only currently-valid
        rows (``valid_to IS NULL``) are candidates — a verdict never compares a
        new fact against an already-retired one. Each returned record carries a
        transient ``.similarity``.
        """
        scope_clause, scope_val = self._scope_where(2)
        rows = await self.db.fetch(
            f"""
            SELECT *, 1 - (embedding <=> $1) AS similarity
            FROM memories
            WHERE {scope_clause}
              AND embedding IS NOT NULL
              AND valid_to IS NULL
              AND 1 - (embedding <=> $1) >= $3
            ORDER BY similarity DESC
            LIMIT $4
            """,
            embedding,
            scope_val,
            min_similarity,
            k,
        )
        return [MemoryRecord.from_row(dict(row)) for row in rows]

    async def supersede(
        self,
        old_ids: List[uuid.UUID],
        new_id: uuid.UUID,
    ) -> int:
        """Retire memories ``old_ids``, pointing them at their replacement.

        Sets the bi-temporal markers (``valid_to``/``superseded_at`` = now,
        ``superseded_by`` = ``new_id``) and zeroes any TTL so a retired pinned
        memory stops being injected. Idempotent: already-retired rows
        (``valid_to IS NOT NULL``) are skipped. Returns the number retired.
        """
        if not old_ids:
            return 0
        result = await self.db.execute(
            """
            UPDATE memories
            SET valid_to = CURRENT_TIMESTAMP,
                superseded_at = CURRENT_TIMESTAMP,
                superseded_by = $2,
                remaining_turns = 0
            WHERE id = ANY($1) AND valid_to IS NULL
            """,
            old_ids,
            new_id,
        )
        try:
            count = int(str(result).split()[-1])
        except (ValueError, IndexError):
            count = 0
        logger.debug(f"Superseded {count} memory(ies) → {new_id}")
        return count

    # =========================================================================
    # TTL Management
    # =========================================================================

    async def get_ttl_active(self) -> List[MemoryRecord]:
        """Fetch all memories with remaining_turns > 0 (guaranteed injection).

        These memories bypass hybrid search relevance and are always included
        in the injection block until their TTL expires.

        Returns:
            List of TTL-active MemoryRecord objects ordered by importance
        """
        scope_clause, scope_val = self._scope_where(1)
        rows = await self.db.fetch(
            f"""
            SELECT *
            FROM memories
            WHERE {scope_clause} AND remaining_turns > 0 AND valid_to IS NULL
            ORDER BY importance DESC
            """,
            scope_val,
        )
        return [MemoryRecord.from_row(dict(row)) for row in rows]

    async def decrement_ttl(self) -> int:
        """Decrement remaining_turns for all TTL-active memories in scope.

        Called once per turn in the execute node, before memory retrieval.

        Locks the target rows in id order before updating: concurrent
        same-project consumers otherwise acquire the overlapping tuple locks
        in divergent orders and deadlock. A residual DeadlockDetectedError is
        counted for telemetry and re-raised — callers already contain it.

        Returns:
            Number of memories whose TTL was decremented
        """
        scope_clause, scope_val = self._scope_where(1)
        try:
            result = await self.db.fetchval(
                f"""
                WITH target AS (
                    SELECT id FROM memories
                    WHERE {scope_clause} AND remaining_turns > 0 AND valid_to IS NULL
                    ORDER BY id
                    FOR UPDATE
                ), updated AS (
                    UPDATE memories m
                    SET remaining_turns = m.remaining_turns - 1
                    FROM target t
                    WHERE m.id = t.id
                    RETURNING m.id
                )
                SELECT COUNT(*) FROM updated
                """,
                scope_val,
            )
        except Exception as e:
            if _is_deadlock(e):
                memory_health.increment("ttl_decrement_deadlock")
            raise
        return result or 0

    async def boost_ttl(self, memory_id: uuid.UUID, turns: int) -> bool:
        """Increase remaining_turns for a specific memory.

        Used by the assembler to pin/extend memories that are currently relevant.

        Args:
            memory_id: UUID of the memory to boost
            turns: Number of turns to add

        Returns:
            True if the memory was found and updated
        """
        result = await self.db.execute(
            """
            UPDATE memories
            SET remaining_turns = COALESCE(remaining_turns, 0) + $1
            WHERE id = $2
            """,
            turns,
            memory_id,
        )
        return result != "UPDATE 0"

    async def deprecate_ttl(self, memory_id: uuid.UUID, turns: int) -> bool:
        """Decrease remaining_turns for a specific memory (floor at 0).

        Used by the assembler to fade memories that are no longer relevant.

        Args:
            memory_id: UUID of the memory to deprecate
            turns: Number of turns to subtract

        Returns:
            True if the memory was found and updated
        """
        result = await self.db.execute(
            """
            UPDATE memories
            SET remaining_turns = GREATEST(COALESCE(remaining_turns, 0) - $1, 0)
            WHERE id = $2
            """,
            turns,
            memory_id,
        )
        return result != "UPDATE 0"

    async def hybrid_search(
        self,
        query_text: str,
        query_embedding: List[float],
        match_count: Optional[int] = None,
        dense_weight: float = 0.6,
        sparse_weight: float = 0.3,
        recency_weight: float = 0.1,
        importance_floor: Optional[float] = None,
    ) -> List[MemoryRecord]:
        """Execute hybrid search using the SQL RRF function.

        Combines dense vector, sparse keyword, and recency channels
        via Reciprocal Rank Fusion.

        Args:
            query_text: Text query for sparse channel
            query_embedding: Embedding vector for dense channel
            match_count: Max results (default: max_memories_per_injection)
            dense_weight: Weight for dense vector channel
            sparse_weight: Weight for sparse keyword channel
            recency_weight: Weight for recency channel
            importance_floor: Minimum importance to include (default: self.retrieval_importance_floor)

        Returns:
            List of MemoryRecord objects ranked by RRF score
        """
        match_count = match_count or self.max_memories_per_injection
        importance_floor = (
            importance_floor
            if importance_floor is not None
            else self.retrieval_importance_floor
        )

        if self.project_scoped and self.project_ids and len(self.project_ids) > 1:
            func_name = "memory_multi_project_hybrid_search"
            scope_val = self.project_ids
        elif self.project_scoped and self.project_ids:
            func_name = "memory_project_hybrid_search"
            scope_val = self.project_ids[0]
        elif self.project_scoped and self.project_id:
            func_name = "memory_project_hybrid_search"
            scope_val = self.project_id
        else:
            func_name = "memory_hybrid_search"
            scope_val = self.job_id

        try:
            rows = await self.db.fetch(
                f"""
                SELECT * FROM {func_name}(
                    $1, $2, $3, $4, $5, $6, $7, importance_floor => $8
                )
                """,
                query_text,
                query_embedding,
                scope_val,
                match_count,
                dense_weight,
                sparse_weight,
                recency_weight,
                importance_floor,
            )
        except Exception as e:
            if _is_deadlock(e):
                memory_health.increment("retrieval_deadlock")
            raise

        # Update access tracking. This deliberately does NOT re-arm
        # remaining_turns any more.
        #
        # It used to also set `remaining_turns = GREATEST(COALESCE(
        # remaining_turns, 0), default_ttl)`, which re-pinned every row this
        # search FETCHED — up to match_count (= max_memories_per_injection,
        # 150) rows per turn — while decrement_ttl only ticks -1 per turn. A
        # memory therefore expired only if it stayed out of the top-150 for 10
        # consecutive turns, so one retrieval stream sustained ~150 x 10 =
        # ~1,500 permanently-pinned rows, and concurrent jobs sharing a
        # project scope stacked on top (one project reached ~2,400).
        #
        # Worse, the re-arm fired on rows FETCHED, not rows INJECTED: retrieve()
        # fills the budget from the pinned tier first, then still runs this
        # search and re-pins all 150 candidates before the budget loop discards
        # most of them. Rows the model never saw came back pinned, growing the
        # pinned tier, which ate more budget, which discarded more
        # freshly-pinned candidates — a self-sustaining ratchet that starved
        # relevance retrieval entirely (get_ttl_active is injected first, so
        # once it fills the budget, hybrid search results never reach the LLM).
        #
        # All three runtimes (worker, session, MemoryManager) funnel through
        # this one seam, so this covers every path. Pinning is now what its
        # docstring claims: set at write, or by an explicit boost_ttl, decayed
        # once per turn.
        if rows:
            await self._record_access_stats([row["id"] for row in rows])

        results = [MemoryRecord.from_row(dict(row)) for row in rows]

        if self._archiver and results:
            self._archiver.audit_step(
                job_id=str(self.job_id),
                agent_type=self.agent_id or "",
                step_type="memory_retrieve",
                node_name="recall_store",
                iteration=0,
                data={
                    "count": len(results),
                    "total_tokens": sum(m.token_count for m in results),
                },
            )

        return results

    async def _record_access_stats(self, ids: List[Any]) -> None:
        """Best-effort access_count/last_accessed bump for retrieved rows.

        Sorted ids feeding an id-ordered FOR UPDATE lock CTE keep concurrent
        consumers' lock acquisition aligned; a residual DeadlockDetectedError
        gets a bounded retry (one attempt per _ACCESS_STAT_RETRY_DELAYS entry).
        Every failure is contained and counted — losing one access-stat update
        is better than losing the retrieval it annotates.
        """
        ordered = sorted(ids)
        attempts = len(_ACCESS_STAT_RETRY_DELAYS) + 1
        for attempt in range(1, attempts + 1):
            try:
                await self.db.execute(
                    """
                    WITH target AS (
                        SELECT id FROM memories
                        WHERE id = ANY($1)
                        ORDER BY id
                        FOR UPDATE
                    )
                    UPDATE memories m
                    SET access_count = m.access_count + 1,
                        last_accessed = CURRENT_TIMESTAMP
                    FROM target t
                    WHERE m.id = t.id
                    """,
                    ordered,
                )
                return
            except Exception as e:
                if _is_deadlock(e):
                    memory_health.increment("access_stats_deadlock")
                    if attempt < attempts:
                        await asyncio.sleep(_ACCESS_STAT_RETRY_DELAYS[attempt - 1])
                        continue
                else:
                    memory_health.increment("access_stats_error")
                logger.warning(
                    "Memory access-stat write failed (contained, attempt %d/%d): "
                    "%s: %s",
                    attempt,
                    attempts,
                    type(e).__name__,
                    e,
                )
                return

    async def retrieve(
        self,
        context_text: str,
        budget_tokens: Optional[int] = None,
    ) -> List[MemoryRecord]:
        """High-level retrieval: two-tier (TTL-guaranteed + hybrid search), fit to budget.

        This is the main entry point for memory retrieval. It:
        1. Fetches TTL-active memories (guaranteed injection, remaining_turns > 0)
        2. Embeds context and runs hybrid search for the remaining budget
        3. Deduplicates and trims to fit within the token budget

        Args:
            context_text: Current context (e.g., current todo + recent messages)
            budget_tokens: Token budget for memory injection

        Returns:
            List of MemoryRecord objects that fit within budget.
            TTL-active (pinned) memories come first, then hybrid search results.
        """
        budget = budget_tokens or self.budget_tokens

        # Tier 1: TTL-active memories (guaranteed injection)
        pinned = await self.get_ttl_active()
        result = []
        tokens_used = 0
        pinned_ids = set()

        for memory in pinned:
            if tokens_used + memory.token_count > budget:
                logger.warning(
                    f"TTL-active memories exceed budget ({tokens_used} + "
                    f"{memory.token_count} > {budget}), truncating"
                )
                break
            result.append(memory)
            tokens_used += memory.token_count
            pinned_ids.add(memory.id)

        # Tier 2: Hybrid search with remaining budget
        remaining_budget = budget - tokens_used
        if remaining_budget > 0:
            context_embedding = await self.embedding_service.embed(context_text)
            candidates = await self.hybrid_search(
                query_text=context_text,
                query_embedding=context_embedding,
            )

            for memory in candidates:
                # Deduplicate against pinned memories
                if memory.id in pinned_ids:
                    continue
                if tokens_used + memory.token_count > budget:
                    break
                result.append(memory)
                tokens_used += memory.token_count

        pinned_count = len(pinned_ids & {m.id for m in result})
        retrieved_count = len(result) - pinned_count
        logger.debug(
            f"Retrieved {len(result)} memories ({tokens_used} tokens): "
            f"{pinned_count} pinned + {retrieved_count} from hybrid search "
            f"(budget={budget})"
        )
        return result

    # =========================================================================
    # Assembly
    # =========================================================================

    @staticmethod
    def format_memory(
        memory: MemoryRecord,
        index: int,
        *,
        handle: Optional[str] = None,
        updated: bool = False,
    ) -> str:
        """Format a single memory for injection.

        The text is static (D11): it carries no per-turn data, so the same
        memory renders to the same bytes whatever its remaining TTL.
        Importance is a retrieval-time value frozen into the rendered text.

        Args:
            memory: MemoryRecord to format
            index: Display index (1-based), the label when no handle is given
            handle: Display handle (``m:3f9a2c``, D30); labels the memory
                as ``[m:3f9a2c]`` instead of its index
            updated: Mark the memory as a newer version of one already in
                the history (D5)

        Returns:
            Formatted memory string
        """
        meta_parts = []
        if memory.importance is not None:
            meta_parts.append(f"importance: {memory.importance:.1f}")
        if memory.source_phase is not None:
            meta_parts.append(f"phase {memory.source_phase}")
        if memory.memory_type and memory.memory_type != "factual":
            meta_parts.append(memory.memory_type)

        label = handle if handle else index
        meta = f" ({', '.join(meta_parts)})" if meta_parts else ""
        marker = f" {UPDATED_ITEM_MARKER}" if updated else ""
        return f"[{label}]{meta}{marker}\n{memory.content}"

    @classmethod
    def render_memory_entry(
        cls,
        items: Sequence[Tuple[MemoryRecord, Optional[str], bool]],
        model: Optional[str] = None,
    ) -> str:
        """Body of one append-only memory entry (append-only injection, D2).

        Unlike :meth:`assemble_memory_block` there is no pinned/retrieved
        split and no token footer: an appended entry stays in the history
        unchanged, so it carries nothing that varies per turn (D11).

        Args:
            items: ``(memory, handle, updated)`` per memory, in rank order
            model: Model id used to resolve the family's header nudge

        Returns:
            The entry body, or "" when there is nothing to render
        """
        if not items:
            return ""

        from shared.runtime.services.guardrails import format_nudge

        parts = [format_nudge("memory_entry_header", model=model)]
        for index, (memory, handle, updated) in enumerate(items, 1):
            parts.append(
                cls.format_memory(memory, index, handle=handle, updated=updated)
            )
        return "\n\n".join(parts)

    @classmethod
    def assemble_memory_block(
        cls,
        memories: List[MemoryRecord],
        budget_tokens: int = 10000,
        model: Optional[str] = None,
    ) -> str:
        """Assemble formatted memory block for injection.

        Separates TTL-active (pinned) memories from retrieval-based memories
        for clear visual distinction.

        Args:
            memories: List of MemoryRecord objects (pinned first, then retrieved)
            budget_tokens: Token budget (for display in footer)
            model: Model id used to resolve family-specific block headers
                / footer. Falls through to the default family if None.

        Returns:
            Formatted memory block string
        """
        if not memories:
            return ""

        pinned = [m for m in memories if m.remaining_turns and m.remaining_turns > 0]
        retrieved = [
            m for m in memories if not (m.remaining_turns and m.remaining_turns > 0)
        ]
        tokens_used = sum(m.token_count for m in memories)

        from shared.runtime.services.guardrails import format_nudge

        lines = []
        idx = 1

        if pinned:
            lines.append(format_nudge("memory_block_header_pinned", model=model))
            lines.append("")
            for memory in pinned:
                lines.append(cls.format_memory(memory, idx))
                lines.append("")
                idx += 1

        if retrieved:
            lines.append(format_nudge("memory_block_header_retrieved", model=model))
            lines.append("")
            for memory in retrieved:
                lines.append(cls.format_memory(memory, idx))
                lines.append("")
                idx += 1

        lines.append(
            format_nudge(
                "memory_block_footer",
                model=model,
                count=f"{len(memories)} items: {len(pinned)} pinned + {len(retrieved)} retrieved",
                tokens=f"{tokens_used:,}",
            )
        )
        return "\n".join(lines)

    # =========================================================================
    # Stats
    # =========================================================================

    async def get_stats(self) -> Dict[str, Any]:
        """Get memory statistics for this job.

        Returns:
            Dict with counts by type, source, total tokens, etc.
        """
        scope_clause, scope_val = self._scope_where(1)
        row = await self.db.fetchrow(
            f"""
            SELECT
                COUNT(*) AS total,
                COALESCE(SUM(token_count), 0) AS total_tokens,
                COALESCE(SUM(access_count), 0) AS total_accesses,
                COUNT(*) FILTER (WHERE memory_type = 'factual') AS factual,
                COUNT(*) FILTER (WHERE memory_type = 'procedural') AS procedural,
                COUNT(*) FILTER (WHERE memory_type = 'error_solution') AS error_solution,
                COUNT(*) FILTER (WHERE memory_type = 'vocabulary') AS vocabulary,
                COUNT(*) FILTER (WHERE memory_type = 'relational') AS relational,
                COUNT(*) FILTER (WHERE source = 'observer') AS from_observer,
                COUNT(*) FILTER (WHERE source = 'todo') AS from_todo,
                COUNT(*) FILTER (WHERE source = 'compaction') AS from_compaction,
                COUNT(*) FILTER (WHERE source = 'phase_archive') AS from_phase_archive,
                COUNT(*) FILTER (WHERE source = 'tool_error') AS from_tool_error,
                AVG(importance) AS avg_importance,
                COUNT(*) FILTER (WHERE remaining_turns > 0 AND valid_to IS NULL) AS ttl_active,
                COUNT(*) FILTER (WHERE valid_to IS NULL) AS current,
                COUNT(*) FILTER (WHERE valid_to IS NOT NULL) AS superseded
            FROM memories
            WHERE {scope_clause}
            """,
            scope_val,
        )

        if row:
            return dict(row)
        return {"total": 0, "total_tokens": 0}
