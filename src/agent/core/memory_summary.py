"""The up-front memory summary of append-only context injection (D35).

Design: knowledge-base/knowledge/features/append_only_context_injection.md
(D11, D21, D25, D34, D35). Next to the pushed memories and the model's own
``memory_search`` (WP5a), the conversation gets one small block up front:
how many memories the project holds, by type, its most frequent topics and
a one-line hint to call ``memory_search``. It is computed once at
conversation start, never changes per turn and is given again only after a
compaction removed it.

This module loads that body for the planner
(``ContextSources.memory_summary``); the planner appends it while the
history lacks it (``context_injection._plan_memory_summary``). The rules:

- **Only with the tool.** The summary exists for its hint, so it is loaded
  only while ``memory_search`` is bound for this conversation and the
  MemoryManager that serves it is there (memory on, manager on). Without
  the tool there is no summary at all.
- **Once per conversation runtime.** The first load is cached for the
  manager's lifetime (the worker's graph build, the session's memory
  setup), keyed weakly on the manager, so no request or turn queries again;
  after a compaction removed the entry, the cached body goes in again
  without a query.
- **Not even once when the history holds it.** A resumed worker or a
  restored session already has the entry in its history (the checkpoint, the
  ``context`` row), so nothing is loaded and nothing is appended: the
  summary stays the one the conversation started with, even if memory grew
  since. Only if that runtime later sees it evicted does it load a body
  (one query) to put back.
- **Never fails a request.** A failed or slow query (5 s) logs a warning and
  leaves the conversation without a summary; the outcome is cached too, so
  an outage costs one attempt per runtime, not one per request.
"""

from __future__ import annotations

import asyncio
import logging
import weakref
from typing import Any, Collection, Iterable, Mapping, Optional, Union

from shared.runtime.core.context_entries import MEMORY_SUMMARY_KIND, entry_kind
from shared.tool_catalog.names import MEMORY_SEARCH_TOOL_NAME

logger = logging.getLogger(__name__)

#: Upper bound of the summary query; it runs on the request path once.
MEMORY_SUMMARY_TIMEOUT_S = 5.0

# Loaded bodies per MemoryManager ("" = loaded, nothing to show). Weak keys:
# the cache lives exactly as long as the manager, i.e. one conversation
# runtime, without touching the manager.
_LOADED: "weakref.WeakKeyDictionary[Any, str]" = weakref.WeakKeyDictionary()


def memory_summary_enabled(
    memory_service: Any, tool_names: Union[Collection[str], Mapping[str, Any], None]
) -> bool:
    """Whether this conversation gets a memory summary at all.

    ``tool_names`` are the tools bound for the conversation (a list of
    names, or the session's name-to-tool map): the summary goes with a bound
    ``memory_search`` and the manager behind it.
    """
    return memory_service is not None and MEMORY_SEARCH_TOOL_NAME in (tool_names or ())


def _cached(memory_service: Any) -> Optional[str]:
    try:
        return _LOADED.get(memory_service)
    except TypeError:  # not weakly referenceable (a test double)
        return None


def _remember(memory_service: Any, body: str) -> None:
    try:
        _LOADED[memory_service] = body
    except TypeError:
        pass


def _history_holds_summary(messages: Iterable[Any]) -> bool:
    return any(entry_kind(msg) == MEMORY_SUMMARY_KIND for msg in messages)


async def _load(memory_service: Any, *, model: Optional[str]) -> str:
    from shared.runtime.services.recall_store import RecallStore

    runtime = getattr(memory_service, "runtime", None)
    store = getattr(runtime, "recall_store", None)
    if store is None:
        return ""
    try:
        stats = await asyncio.wait_for(
            store.summary_stats(), timeout=MEMORY_SUMMARY_TIMEOUT_S
        )
        return RecallStore.render_memory_summary(stats, model=model)
    except Exception as exc:
        logger.warning(
            "Memory summary unavailable (non-fatal; this conversation goes "
            "on without it): %s: %s",
            type(exc).__name__,
            exc,
        )
        return ""


async def conversation_memory_summary(
    memory_service: Any,
    messages: Iterable[Any],
    *,
    tool_names: Union[Collection[str], Mapping[str, Any], None],
    model: Optional[str],
) -> str:
    """The memory summary body for the planner ("" = nothing to append).

    ``messages`` is the history the request is planned on. See the module
    docstring for when this queries: at most once per conversation runtime,
    and not at all while the history holds the summary and nothing was
    loaded yet.
    """
    if not memory_summary_enabled(memory_service, tool_names):
        return ""
    cached = _cached(memory_service)
    if cached is not None:
        return cached
    if _history_holds_summary(messages):
        return ""
    body = await _load(memory_service, model=model)
    _remember(memory_service, body)
    return body


__all__ = [
    "MEMORY_SUMMARY_TIMEOUT_S",
    "conversation_memory_summary",
    "memory_summary_enabled",
]
