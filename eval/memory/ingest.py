"""Incremental ingestion: haystack sessions → memories, through the seam.

seam mode replays each session against MemoryManager exactly like the
live persistent loop (persistent_graph.py:549-581 read path,
:426-441 turn_end capture, persistent_app teardown session_end):

    per round:  append HumanMessage
                assemble(query=last user msg)        # read_path_per_turn
                append AIMessage
                turn_count += 1
                capture(turn_end, messages, turn_count)
    at end:     capture(session_end, messages)

so TTL decrement, access-resets, the elapsed extraction gate, and the
final teardown extraction all accrue as in production. capture() is
awaited (production fire-and-forgets the turn_end task) — same writer
effects, deterministic completion.

verbatim mode skips the extraction LLM entirely: each user-assistant
round is stored directly, so question-time retrieval is pure hybrid
search over a flat round-granularity index (the cheap smoke arm + published-baseline arm).

Scoping: one question = one project (cross-session sharing), one
haystack session = one job — each memory row's job_id IS its source
session, which is what the retrieval metrics collapse on.
"""

import logging
import uuid
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Callable, List, Optional

from eval.memory.arms import ArmSpec
from eval.memory.datasets import LMEQuestion, LMESession
from eval.memory.infra import (
    build_manager,
    make_recall_store,
    project_uuid,
    session_uuid,
)

logger = logging.getLogger(__name__)

#: (job_id, project_id) -> RecallStore-like
StoreFactory = Callable[[uuid.UUID, uuid.UUID], Any]
#: (store, job_id, project_id) -> MemoryManager-like
ManagerFactory = Callable[[Any, uuid.UUID, uuid.UUID], Any]


@dataclass
class HarnessHandles:
    """Shared per-run handles; factories are injectable for offline tests."""

    config: Any
    run_id: str = "dev"
    db: Any = None
    embedding: Any = None
    aux_llm: Any = None
    extraction_prompt: str = ""
    store_factory: Optional[StoreFactory] = None
    manager_factory: Optional[ManagerFactory] = None

    def make_store(
        self,
        job_id: uuid.UUID,
        project_id: uuid.UUID,
        embedding: Optional[Any] = None,
    ) -> Any:
        if self.store_factory is not None:
            return self.store_factory(job_id, project_id)
        return make_recall_store(
            self.db, embedding or self.embedding, self.config, job_id, project_id
        )

    def make_manager(self, store: Any, job_id: uuid.UUID, project_id: uuid.UUID) -> Any:
        if self.manager_factory is not None:
            return self.manager_factory(store, job_id, project_id)
        return build_manager(
            self.config,
            recall_store=store,
            auxiliary_llm=self.aux_llm,
            extraction_prompt=self.extraction_prompt,
            job_id=job_id,
            project_id=project_id,
        )


@dataclass
class IngestResult:
    question_id: str
    sessions: int = 0
    turns: int = 0
    assembles: int = 0
    #: Rows in the question's project scope after ingestion (None when no
    #: real DB handle, i.e. unit tests).
    stored_memories: Optional[int] = None
    #: Contained plugin failures surfaced by the per-turn read path
    #: (AssembleStats.errors). Writer failures log via the manager and
    #: land in the run log; a near-zero stored_memories count is the
    #: companion signal.
    errors: List[str] = field(default_factory=list)


def _first_user_prefix(session: LMESession, arm: ArmSpec) -> str:
    if arm.ingestion.date_prefix and session.date:
        return f"[Session date: {session.date}] "
    return ""


#: One embeddings request per chunk; large enough to collapse a ~250-round
#: question into a handful of requests, small enough for any serving stack.
EMBED_BATCH_SIZE = 32


class PrimedEmbedding:
    """Embedding wrapper that serves embed() from a pre-computed cache.

    The shared inference router schedules fairly at *request* granularity:
    under concurrent load a single-text embed() stream paces at the queue
    rate (~2 req/s observed) regardless of text size, while one batch
    request carries EMBED_BATCH_SIZE texts through the same slot. Verbatim
    ingestion knows every text it will store up front, so priming per
    question turns ~250 requests into ~8 with bit-identical vectors.

    Scoped per question (the cache holds that question's vectors only);
    misses fall through to the real service, so store() behavior is
    unchanged — just pre-fed.
    """

    def __init__(self, inner: Any, batch_size: int = EMBED_BATCH_SIZE) -> None:
        self._inner = inner
        self._batch_size = batch_size
        self._cache: dict = {}

    def __getattr__(self, name: str) -> Any:  # model, embed_batch, …
        return getattr(self._inner, name)

    async def prime(self, texts: List[str]) -> None:
        todo = [t for t in dict.fromkeys(texts) if t not in self._cache]
        for i in range(0, len(todo), self._batch_size):
            chunk = todo[i : i + self._batch_size]
            vectors = await self._inner.embed_batch(chunk)
            self._cache.update(zip(chunk, vectors))

    async def embed(self, text: str) -> Any:
        vector = self._cache.get(text)
        if vector is None:
            return await self._inner.embed(text)
        return vector


#: Rare tail rounds exceed the embedding server's context (the k3d
#: deployment serves n_ctx=8192), which 400s the whole store. Char caps
#: alone don't bound tokens: the observed offender was 8264 *qwen*
#: tokens at only 5816 cl100k tokens (qwen ≈ 1.42× cl100k on that
#: content) while well under the char cap. So cap by cl100k count with
#: that ratio priced in: 5000 × 1.42 ≈ 7100 < 8192. The prefix keeps
#: the salient content and the same capped text flows through priming,
#: store(), and injection.
VERBATIM_MAX_CHARS = 24_000
EMBED_TOKEN_CAP = 5_000


@lru_cache(maxsize=1)
def _encoding():
    import tiktoken

    return tiktoken.get_encoding("cl100k_base")


def _truncate_for_embedding(text: str) -> str:
    if len(text) > VERBATIM_MAX_CHARS:
        text = text[:VERBATIM_MAX_CHARS] + " …[truncated]"
    if len(text) < EMBED_TOKEN_CAP:  # ≥1 char/token: cannot exceed the cap
        return text
    tokens = _encoding().encode(text, disallowed_special=())
    if len(tokens) <= EMBED_TOKEN_CAP:
        return text
    return _encoding().decode(tokens[:EMBED_TOKEN_CAP]) + " …[truncated]"


def _verbatim_round_texts(session: LMESession, arm: ArmSpec) -> List[str]:
    """The exact content strings verbatim replay stores, in round order.

    Single source of truth for both batch priming and the replay itself —
    a primed text that didn't match the stored text would silently fall
    back to one request per round.
    """
    prefix = _first_user_prefix(session, arm)
    texts = []
    for i, (user, assistant) in enumerate(session.rounds()):
        parts = [f"User: {(prefix + user.content) if i == 0 else user.content}"]
        if assistant is not None:
            parts.append(f"Assistant: {assistant.content}")
        texts.append(_truncate_for_embedding("\n".join(parts)))
    return texts


def _form_query_text(handles: HarnessHandles, messages: List[Any]) -> str:
    """Production-faithful query formation: the persistent loop's exact
    flag switch (memory.query.digest → unified digest, else legacy
    last-user-message) so digest arms measure the real read path."""
    from agent.services.memory.plugins.legacy import build_persistent_query_text

    query_cfg = getattr(handles.config.memory, "query", None)
    if query_cfg is not None and query_cfg.digest:
        from agent.services.memory.query import build_digest_query_text

        return build_digest_query_text(
            messages,
            None,
            window=query_cfg.digest_window,
            max_chars_per_message=query_cfg.digest_max_chars_per_message,
        )
    return build_persistent_query_text(messages)


async def _replay_session_seam(
    session: LMESession,
    arm: ArmSpec,
    handles: HarnessHandles,
    manager: Any,
    result: IngestResult,
) -> None:
    from langchain_core.messages import AIMessage, HumanMessage

    from agent.services.memory import AssembleRequest, CaptureEvent

    messages: List[Any] = []
    turn_count = 0
    prefix = _first_user_prefix(session, arm)

    for user, assistant in session.rounds():
        content = (prefix + user.content) if not messages else user.content
        messages.append(HumanMessage(content=content))

        if arm.ingestion.read_path_per_turn:
            payload = await manager.assemble(
                AssembleRequest(query_text=_form_query_text(handles, messages))
            )
            result.assembles += 1
            result.errors.extend(payload.stats.errors)

        if assistant is not None:
            messages.append(AIMessage(content=assistant.content))
        turn_count += 1
        await manager.capture(
            CaptureEvent(
                kind="turn_end",
                messages=list(messages),
                turn_count=turn_count,
            )
        )

    await manager.capture(CaptureEvent(kind="session_end", messages=list(messages)))
    result.turns += turn_count


async def _replay_session_verbatim(
    session: LMESession,
    arm: ArmSpec,
    store: Any,
    result: IngestResult,
) -> None:
    for i, text in enumerate(_verbatim_round_texts(session, arm)):
        await store.store(
            content=text,
            importance=arm.ingestion.verbatim_importance,
            memory_type="factual",
            source="observer",
            source_turn_start=i,
            source_turn_end=i + 1,
        )
        result.turns += 1


async def ingest_question(
    question: LMEQuestion,
    arm: ArmSpec,
    handles: HarnessHandles,
) -> IngestResult:
    """Ingest one question's haystack, session by session, in order.

    Sessions are deliberately sequential (recency rank and TTL dynamics
    depend on insertion order, exactly as memories accrue over time in
    production); parallelism belongs at the question level.
    """
    result = IngestResult(question_id=question.question_id)
    project = project_uuid(handles.run_id, question.question_id)

    embedding: Optional[Any] = None
    if arm.ingestion.mode == "verbatim" and handles.embedding is not None:
        embedding = PrimedEmbedding(handles.embedding)
        await embedding.prime(
            [t for s in question.sessions for t in _verbatim_round_texts(s, arm)]
        )

    for session in question.sessions:
        job = session_uuid(handles.run_id, question.question_id, session.session_id)
        store = handles.make_store(job, project, embedding=embedding)

        if arm.ingestion.mode == "verbatim":
            await _replay_session_verbatim(session, arm, store, result)
        else:
            manager = handles.make_manager(store, job, project)
            await _replay_session_seam(session, arm, handles, manager, result)
        result.sessions += 1

    if handles.db is not None:
        result.stored_memories = await handles.db.fetchval(
            "SELECT COUNT(*) FROM memories WHERE project_id = $1", project
        )
        if arm.ingestion.mode == "seam" and not result.stored_memories:
            logger.warning(
                "%s: seam ingestion stored 0 memories across %d sessions — "
                "extraction is likely failing (check the run log)",
                question.question_id,
                result.sessions,
            )
    return result
