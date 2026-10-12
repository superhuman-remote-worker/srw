"""Shared fixtures for the Phase-1 memory-equivalence suites.

Used by tests/test_memory_worker_equivalence.py and
tests/test_memory_persistent_equivalence.py — same fixture records, store
mocks, golden block snapshots, and message normalization on both sides so
the worker and persistent parity suites pin the identical transplanted
pipeline. The asynchronous retrieval helpers (``GatedRetriever`` and
friends) serve tests/test_memory_async_retrieval.py and the append_only
graph suites. Not a test module (underscore prefix, like _fs_backend.py).
"""

import asyncio
import uuid as uuid_module
from unittest.mock import AsyncMock

from langchain_core.messages import AIMessage, ToolMessage

from agent.core.knowledge_injection import KNOWLEDGE_TOOL_CALL_ID_PREFIX
from agent.core.memory_injection import MEMORY_TOOL_CALL_ID_PREFIX
from shared.runtime.services.knowledge_store import KnowledgeRecord
from shared.runtime.services.recall_store import MemoryRecord

PROJECT_ID = "12345678-1234-5678-1234-567812345678"

# Golden snapshots of the rendered blocks for the fixture records below,
# generated from the real assemblers pre-refactor (2026-06-11). They pin
# the shared renderers against drift while the legacy path is frozen.
# Append-only context injection 2e (D11, spec §G/O12) removed the per-turn
# "pinned, N turns left" clause from the memory line; the pinned/retrieved
# split and the footer stay.
GOLDEN_MEMORY_BLOCK = (
    "--- Pinned Memories (TTL-active) ---\n"
    "\n"
    "[1] (importance: 0.8, phase 2, preference)\n"
    "User prefers ruff with line length 88.\n"
    "\n"
    "--- Retrieved Memories (relevance-ranked) ---\n"
    "\n"
    "[2] (importance: 0.5)\n"
    "The API key lives in the orchestrator env, not the workspace.\n"
    "\n"
    "--- End Memories (2 items: 1 pinned + 1 retrieved, ~26 tokens) ---"
)
GOLDEN_KNOWLEDGE_BLOCK = (
    "--- Project Knowledge ---\n"
    "\n"
    "[1] (decision, high confidence) Tags: deploy\n"
    "Use helm upgrade, never kubectl patch.\n"
    "\n"
    "[2] (learning)\n"
    "Keycloak tokens need scope=openid.\n"
    "\n"
    "--- End Knowledge (2 notes, ~17 tokens) ---"
)


def make_memories():
    """Pinned-first ordering, matching RecallStore.retrieve()'s contract."""
    return [
        MemoryRecord(
            id=uuid_module.UUID(int=1),
            content="User prefers ruff with line length 88.",
            memory_type="preference",
            importance=0.8,
            token_count=12,
            remaining_turns=3,
            source_phase=2,
        ),
        MemoryRecord(
            id=uuid_module.UUID(int=2),
            content="The API key lives in the orchestrator env, not the workspace.",
            importance=0.5,
            token_count=14,
            remaining_turns=0,
        ),
    ]


def make_notes():
    return [
        KnowledgeRecord(
            id=uuid_module.UUID(int=3),
            note_id="n-001",
            title="Deploy procedure",
            note_type="decision",
            content="Use helm upgrade, never kubectl patch.",
            confidence="high",
            tags=["deploy"],
        ),
        KnowledgeRecord(
            id=uuid_module.UUID(int=4),
            note_id="n-002",
            title="Auth quirk",
            note_type="learning",
            content="Keycloak tokens need scope=openid.",
        ),
    ]


def make_recall_mock(memories=None, retrieve_error=None, decrement_error=None):
    recall = AsyncMock()
    if retrieve_error:
        recall.retrieve.side_effect = retrieve_error
    else:
        recall.retrieve.return_value = list(memories or [])
    if decrement_error:
        recall.decrement_ttl.side_effect = decrement_error
    else:
        recall.decrement_ttl.return_value = 1
    return recall


def make_kb_mock(notes=None):
    kb = AsyncMock()
    kb.hybrid_search.return_value = list(notes or [])
    return kb


# ---------------------------------------------------------------------------
# Asynchronous retrieval (append_only, WP3)
# ---------------------------------------------------------------------------


class GatedRetriever:
    """Memory retriever whose every call waits until the test releases it.

    Drives the manager's off-request-path retrieval deterministically: a
    call is in flight until ``release()``; ``auto`` releases each call at
    once. Records the query of every call and how many were cancelled.
    """

    def __init__(self, memories=None, *, auto: bool = False):
        self.memories = list(make_memories() if memories is None else memories)
        self.auto = auto
        self.queries = []
        self.gates = []
        self.cancelled = 0

    async def retrieve(self, req):
        from agent.services.memory import Candidate

        self.queries.append(req.query_text)
        gate = asyncio.Event()
        self.gates.append(gate)
        if self.auto:
            gate.set()
        try:
            await gate.wait()
        except asyncio.CancelledError:
            self.cancelled += 1
            raise
        return [
            Candidate(
                kind="memory",
                text=memory.content,
                token_count=memory.token_count or 0,
                record=memory,
            )
            for memory in self.memories
        ]

    def release(self, index: int = -1) -> None:
        self.gates[index].set()


class BrokenScorer:
    """A scorer that fails structurally (``ValueError``) or transiently."""

    def __init__(self, error: Exception):
        self.error = error
        self.calls = 0

    async def score(self, req, items):
        self.calls += 1
        if self.error is None:
            return items
        raise self.error


def make_async_manager(retriever, *, scorer=None, job_id="job-async", agent_type=None):
    """A MemoryManager with one retriever (and optionally one scorer)."""
    from agent.services.memory import MemoryManager, MemoryRuntime

    return MemoryManager(
        MemoryRuntime(job_id=job_id, agent_type=agent_type),
        retrievers=[("gated", retriever)],
        scorers=[("reranker", scorer)] if scorer is not None else None,
    )


async def settle_retrieval(manager, rounds: int = 100) -> None:
    """Yield to the loop until the manager's retrieval task is done."""
    for _ in range(rounds):
        if not manager.retrieval_in_flight:
            # One more turn so the done callback has run too.
            await asyncio.sleep(0)
            return
        await asyncio.sleep(0)
    raise AssertionError("the memory retrieval did not settle")


def _id_prefix(call_id):
    for prefix in (MEMORY_TOOL_CALL_ID_PREFIX, KNOWLEDGE_TOOL_CALL_ID_PREFIX):
        if call_id.startswith(prefix):
            return prefix
    return call_id


def normalize(messages):
    """Structure + content with random tool_call_id suffixes stripped."""
    out = []
    for msg in messages:
        if isinstance(msg, AIMessage):
            out.append(
                (
                    "ai",
                    msg.content,
                    [
                        (
                            tc["name"],
                            tuple(sorted(tc["args"].items())),
                            _id_prefix(tc["id"]),
                        )
                        for tc in msg.tool_calls
                    ],
                )
            )
        elif isinstance(msg, ToolMessage):
            out.append(("tool", msg.content, _id_prefix(msg.tool_call_id)))
        else:  # pragma: no cover - nothing else should appear
            out.append(("other", type(msg).__name__))
    return out
