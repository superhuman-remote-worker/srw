"""Memory toolkit: the model's own way into project memory.

Design: knowledge-base/knowledge/features/append_only_context_injection.md
(D25 the pull path, D30 different names for push and pull, D34 workers and
sessions alike). Relevant memories are pushed on their own as harness context
(``<srw_context kind="memory">`` entries); ``memory_search`` lets the model
look up what the push did not bring, e.g. an earlier decision the new user
message asks about.

The search itself is a MemoryManager extension
(``agent.services.memory.plugins.memory_search``, bound from
``memory.pipeline.extensions``), so it runs the manager's retrieval pipeline
with the job's or session's own stores. Both runtimes bind their tools before
that manager exists (a session sets up memory after binding its LLM), so the
tool bound here is a thin front: at call time it resolves the extension's
tool through ``context.memory_service.extension_tools()`` and delegates to
it. One schema and one description serve both (:func:`build_memory_search_tool`).
"""

from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, List, Optional

from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field

from shared.tool_catalog.definitions import MEMORY_TOOLS_METADATA
from shared.tool_catalog.names import MEMORY_SEARCH_TOOL_NAME

logger = logging.getLogger(__name__)

#: Default and ceiling of ``limit``; the ceiling matches the pushed path's
#: ``memory.bounded.max_items`` default, the same pipeline caps both.
DEFAULT_LIMIT = 5
MAX_LIMIT = 10

MEMORY_SEARCH_DESCRIPTION = (
    "Search the project's long-term memory: facts, decisions, preferences "
    "and fixes extracted from earlier jobs and sessions in this project.\n\n"
    "Relevant memories already arrive on their own as context. Call this when "
    "you need something from earlier work that the conversation does not "
    "hold, for example what was decided about a topic, how an error was "
    "fixed before, or a preference the user stated in another session. "
    "Results are ranked best match first; each memory is labelled with a "
    "short handle like [m:3f9a2c].\n\n"
    "Args:\n"
    "  query: what you want to recall, in plain words (e.g. 'which region "
    "the production cluster runs in').\n"
    f"  limit: how many memories to return, 1-{MAX_LIMIT} (default "
    f"{DEFAULT_LIMIT}).\n"
    "  handle: fetch one memory by its handle instead of searching, e.g. "
    "'m:3f9a2c'."
)

#: The answer when no memory extension can serve the call: memory failed to
#: initialise, or this runtime has no MemoryManager.
MEMORY_UNAVAILABLE = (
    "Memory search is unavailable in this run (project memory is not "
    "connected). Continue without it."
)


class MemorySearchInput(BaseModel):
    """Arguments of ``memory_search``."""

    query: str = Field(
        default="",
        description="What you want to recall, in plain words.",
    )
    limit: int = Field(
        default=DEFAULT_LIMIT,
        description=f"How many memories to return, 1-{MAX_LIMIT}.",
    )
    handle: Optional[str] = Field(
        default=None,
        description="Fetch one memory by its handle (e.g. 'm:3f9a2c') instead of searching.",
    )


SearchFn = Callable[..., Awaitable[str]]


def build_memory_search_tool(run: SearchFn) -> StructuredTool:
    """The ``memory_search`` tool over ``run(query=, limit=, handle=)``.

    Used by the extension (its real implementation) and by
    :func:`create_memory_tools` (the front that delegates to it), so the
    bound schema and description are the extension's own.
    """
    return StructuredTool.from_function(
        coroutine=run,
        name=MEMORY_SEARCH_TOOL_NAME,
        description=MEMORY_SEARCH_DESCRIPTION,
        args_schema=MemorySearchInput,
    )


def _extension_tool(context: Any) -> Optional[Any]:
    """The extension's ``memory_search`` tool, or None when none is bound."""
    manager = getattr(context, "memory_service", None)
    if manager is None:
        return None
    try:
        tools = manager.extension_tools() or []
    except Exception as exc:  # pragma: no cover - extension_tools contains its own
        logger.warning(
            "memory_search: extension_tools() failed: %s: %s",
            type(exc).__name__,
            exc,
        )
        return None
    for tool in tools:
        if getattr(tool, "name", None) == MEMORY_SEARCH_TOOL_NAME and callable(
            getattr(tool, "coroutine", None)
        ):
            return tool
    return None


def create_memory_tools(context: Any) -> List[Any]:
    """The memory tools bound by config name (``tools.memory``).

    Each delegates at call time to the same-named extension tool of
    ``context.memory_service``; without one it answers
    :data:`MEMORY_UNAVAILABLE` instead of failing the turn.
    """

    async def memory_search(
        query: str = "",
        limit: int = DEFAULT_LIMIT,
        handle: Optional[str] = None,
    ) -> str:
        tool = _extension_tool(context)
        if tool is None:
            return MEMORY_UNAVAILABLE
        return await tool.coroutine(query=query, limit=limit, handle=handle)

    return [build_memory_search_tool(memory_search)]


def get_memory_metadata() -> dict:
    """Metadata of the memory tools (catalog view)."""
    return MEMORY_TOOLS_METADATA


__all__ = [
    "DEFAULT_LIMIT",
    "MAX_LIMIT",
    "MEMORY_SEARCH_DESCRIPTION",
    "MEMORY_UNAVAILABLE",
    "MemorySearchInput",
    "build_memory_search_tool",
    "create_memory_tools",
    "get_memory_metadata",
]
