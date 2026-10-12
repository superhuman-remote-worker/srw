"""The ``memory_search`` extension: the model's pull path into memory.

Design: knowledge-base/knowledge/features/append_only_context_injection.md
(D25, D30, D34) and its plan (WP5). Relevant memories are pushed to the
model as context entries; this extension lets the model look up more
itself, with a real tool (``agent.tools.memory``). Workers and sessions
bind it alike through ``memory.pipeline.extensions`` and ``tools.memory``.

What it runs is the push path's read pipeline, scoped the same way:
- the candidates come from the job's or session's own ``RecallStore``
  (project-scoped like the push), through its hybrid search with the query
  the model wrote. Unlike ``recall_two_tier`` there is no TTL-pinned tier
  and no TTL tick: a search is relevance only and writes nothing but the
  access stats every search hit records;
- the same configured scorers and policies (``memory.pipeline.scorers`` and
  ``.policies``: reranker, gate, bounded), resolved from the registry with
  the same config, so a search ranks and gates like the push;
- then the model's ``limit``.

Results render as ``RecallStore.render_memory_list``: one block per memory
labelled with its display handle (``[m:3f9a2c]``), static text with no TTL
or score (D11). The tool result is plain text, never a
``<srw_context kind="memory">`` entry (D30). The presence scan reads the
handles and content hashes back from it, so a memory the model fetched is
not pushed again (D3, ``context_injection.scan_presence``).

Failures never fail the turn: a transient reranker fault falls back to
hybrid order (as on the push path); anything else is reported in the tool
result.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, List, Optional, Tuple

from agent.services.memory.registry import (
    register_memory_plugin,
    resolve_memory_plugin,
)
from agent.services.memory.types import (
    AssembleRequest,
    Candidate,
    MemoryRuntime,
    Scored,
    TransientScorerError,
)
from shared.runtime.core.context_entries import ENTRY_SEPARATOR, normalize_memory_handle
from shared.runtime.services.recall_store import RecallStore
from shared.tool_catalog.names import MEMORY_SEARCH_TOOL_NAME

logger = logging.getLogger(__name__)

NamedPlugin = Tuple[str, Any]


async def _bounded(coro: Awaitable[Any], timeout: Optional[float]) -> Any:
    """The runtime's per-store-call guard (5 s in sessions; None = unbounded)."""
    if timeout is None:
        return await coro
    return await asyncio.wait_for(coro, timeout=timeout)


def _clamp_limit(limit: Any) -> int:
    from agent.tools.memory import DEFAULT_LIMIT, MAX_LIMIT

    try:
        value = int(limit)
    except (TypeError, ValueError):
        return DEFAULT_LIMIT
    return max(1, min(MAX_LIMIT, value))


class MemorySearchExtension:
    """MemoryExtension protocol: contributes the ``memory_search`` tool."""

    def __init__(self, runtime: MemoryRuntime) -> None:
        self.runtime = runtime
        self._pipeline: Optional[Tuple[List[NamedPlugin], List[NamedPlugin]]] = None
        self._tools: Optional[List[Any]] = None

    # -- MemoryExtension ------------------------------------------------------

    def tools(self) -> List[Any]:
        if self._tools is None:
            from agent.tools.memory import build_memory_search_tool

            self._tools = [build_memory_search_tool(self.run)]
        return list(self._tools)

    # -- The tool -------------------------------------------------------------

    async def run(
        self,
        query: str = "",
        limit: int = 5,
        handle: Optional[str] = None,
    ) -> str:
        """Search memory for ``query``, or fetch one memory by ``handle``."""
        from agent.tools.memory import MEMORY_UNAVAILABLE

        store = self.runtime.recall_store
        if store is None:
            return MEMORY_UNAVAILABLE
        handle_text = str(handle or "").strip()
        query_text = str(query or "").strip()
        try:
            if handle_text:
                return await self._fetch(store, handle_text)
            if not query_text:
                return (
                    "Error: give a query (what you want to recall), or the "
                    "handle of one memory (e.g. m:3f9a2c)."
                )
            return await self._search(store, query_text, _clamp_limit(limit))
        except (asyncio.TimeoutError, TimeoutError):
            logger.warning("memory_search timed out (query=%r)", query_text)
            return "Memory search timed out. Continue without it, or try again later."
        except Exception as exc:
            logger.warning(
                "memory_search failed: %s: %s", type(exc).__name__, exc, exc_info=True
            )
            return f"Memory search failed ({type(exc).__name__}). Continue without it."

    async def _fetch(self, store: Any, handle_text: str) -> str:
        handle = normalize_memory_handle(handle_text)
        if handle is None:
            return (
                f'Error: "{handle_text}" is not a memory handle. Handles look '
                "like m:3f9a2c."
            )
        record = await _bounded(
            store.get_by_handle(handle), self.runtime.retrieval_timeout
        )
        if record is None:
            return (
                f"No current memory has the handle {handle} (it may have been "
                "replaced or retired). Search by topic instead."
            )
        body = RecallStore.render_memory_list([record])
        return f"Memory {handle}:{ENTRY_SEPARATOR}{body}"

    async def _search(self, store: Any, query: str, limit: int) -> str:
        records = await self.search_records(store, query, limit)
        if not records:
            return f'No memories match "{query}".'
        return (
            f'Memories matching "{query}", best match first:'
            f"{ENTRY_SEPARATOR}{RecallStore.render_memory_list(records)}"
        )

    # -- The pipeline ---------------------------------------------------------

    def _bind_pipeline(self) -> Tuple[List[NamedPlugin], List[NamedPlugin]]:
        """The configured scorers and policies, built once from the registry."""
        if self._pipeline is None:
            pipeline = getattr(self.runtime.memory_config, "pipeline", None)

            def _bind(kind: str, names: Any) -> List[NamedPlugin]:
                return [
                    (name, resolve_memory_plugin(kind, name).factory(self.runtime))
                    for name in list(names or [])
                ]

            self._pipeline = (
                _bind("scorer", getattr(pipeline, "scorers", None)),
                _bind("policy", getattr(pipeline, "policies", None)),
            )
        return self._pipeline

    async def search_records(self, store: Any, query: str, limit: int) -> List[Any]:
        """Memory rows for ``query``, best first: hybrid → scorers → policies → limit."""
        timeout = self.runtime.retrieval_timeout
        embedding = await _bounded(store.embedding_service.embed(query), timeout)
        found = await _bounded(
            store.hybrid_search(query_text=query, query_embedding=embedding),
            timeout,
        )
        items = [
            Scored(
                candidate=Candidate(
                    kind="memory",
                    text=record.content,
                    token_count=getattr(record, "token_count", 0) or 0,
                    record=record,
                    retriever=MEMORY_SEARCH_TOOL_NAME,
                )
            )
            for record in found or []
        ]
        if not items:
            return []
        scorers, policies = self._bind_pipeline()
        request = AssembleRequest(query_text=query)
        for name, scorer in scorers:
            try:
                items = await scorer.score(request, items)
            except TransientScorerError as exc:
                # Same as the push path: a transport blip that outlasted the
                # scorer's retries serves this call in hybrid order.
                logger.warning(
                    "memory_search: scorer '%s' failed transiently, hybrid "
                    "order kept: %s",
                    name,
                    exc,
                )
        for name, policy in policies:
            try:
                items = await policy.apply(request, items)
            except Exception as exc:
                logger.warning(
                    "memory_search: policy '%s' failed (passed through): %s: %s",
                    name,
                    type(exc).__name__,
                    exc,
                )
        return [
            item.candidate.record
            for item in items
            if item.candidate.kind == "memory" and item.candidate.record is not None
        ][:limit]


@register_memory_plugin(
    "extension",
    MEMORY_SEARCH_TOOL_NAME,
    description="memory_search tool: the model searches project memory itself "
    "(hybrid search + the configured scorers and policies, or one memory by "
    "its handle)",
)
def _build_memory_search(runtime: MemoryRuntime) -> MemorySearchExtension:
    return MemorySearchExtension(runtime)


__all__ = ["MemorySearchExtension"]
