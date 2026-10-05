"""Helper functions for transient injection as synthetic tool calls.

This module provides utilities to inject content as fake tool call results,
making it appear as if the agent already read certain files. This approach:

1. Avoids duplication (agent won't redundantly call read_file)
2. Ensures injected content isn't included in summarization (re-injected fresh each turn)

Handles injection of:
- Todo lists (as transient HumanMessage) — no longer produced: the worker's
  todo list lives in the history (todo tool results, phase-start messages,
  the post-compaction restatement; append-only context injection D17-D19).
  ``create_todos_human_message`` and ``TODOS_INJECTION_CONTENT_PREFIX`` stay
  so ``is_workspace_injection_message`` still recognises legacy rows
- Phase instruction blocks delivered once at a concrete phase start — NOT
  transient: ``create_phase_instruction_message`` builds a persistent,
  protected HumanMessage (see src/shared/runtime/core/message_markers.py) that the graph
  appends to state and the context manager keeps out of every compaction
  strategy
- Memory and knowledge injection use their own modules (memory_injection.py, knowledge_injection.py)
"""

import hashlib
from typing import List, Tuple

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage

from shared.runtime.core.context_entries import is_context_injection
from shared.runtime.core.injection_markers import (
    INSTRUCTION_TOOL_CALL_ID_PREFIX as INSTRUCTION_TOOL_CALL_ID_PREFIX,
    TODOS_INJECTION_CONTENT_PREFIX as TODOS_INJECTION_CONTENT_PREFIX,
)
from shared.runtime.core.message_markers import (
    INSTRUCTION_PATH_KEY,
    PERSIST_ROLE_EVENT,
    PERSIST_ROLE_KEY,
    PHASE_KEY,
    PROTECTED_KEY,
)


def content_hash_id(content: str) -> str:
    """Deterministic 8-hex-char id suffix derived from the injected content.

    Injection tool_call_ids must be deterministic (not uuid4): identical
    injected content must produce a byte-identical request payload so
    provider prompt caches can reuse the prefix, and so payloads are
    reproducible when debugging. Each injection type is injected at most
    once per request under a distinct prefix, so collisions within one
    payload are not possible.
    """
    return hashlib.sha1(content.encode("utf-8", errors="replace")).hexdigest()[:8]


def find_tail_injection_anchor(messages: List[BaseMessage]) -> int:
    """Index at which to insert the transient injection block, at the tail.

    Transient injections (memory, knowledge, citation feedback, guidance,
    instruction files) are placed AFTER the conversation, not before it:
    provider prompt caches match on a strict left-to-right prefix, so a
    block that changes every turn must sit below the stable history or it
    invalidates the cache for everything after it.

    The anchor is the position just after the last Human/Tool message
    rather than blindly ``len(messages)``: the injected memory/knowledge
    pairs are synthetic ``AIMessage(tool_call)`` + ``ToolMessage`` turns,
    and Gemini rejects a function-call turn that does not immediately
    follow a user or function-response turn ("Please ensure that function
    call turn comes immediately after a user turn or after a function
    response turn."). Every real path into an LLM call ends the history
    with a HumanMessage (new task/turn, transition, reminder) or the
    ToolMessages of the previous iteration — so the anchor is normally the
    end of the list; the walk-back only matters for degenerate histories
    that end in a bare model turn.
    """
    for i in range(len(messages), 0, -1):
        if isinstance(messages[i - 1], (HumanMessage, ToolMessage)):
            return i
    return len(messages)


def create_todos_human_message(todos_content: str) -> HumanMessage:
    """Create the legacy transient ``<active_tasks>`` todo HumanMessage.

    Legacy: the worker graph no longer produces it. It used to be rebuilt at
    the very end of every request, which rewrote the previous request's tail
    and defeated prefix prompt caches (append-only context injection, D17).
    Kept so tests and legacy rows keep one definition of the shape that
    ``is_workspace_injection_message`` recognises.

    Args:
        todos_content: Formatted todo list from TodoManager.format_for_injection()

    Returns:
        HumanMessage with content prefixed by TODOS_INJECTION_CONTENT_PREFIX
    """
    return HumanMessage(
        content=f"{TODOS_INJECTION_CONTENT_PREFIX}{todos_content}\n</active_tasks>"
    )


PHASE_INSTRUCTION_CONTENT_PREFIX = "[phase: "


def create_phase_instruction_message(
    file_path: str,
    content: str,
    phase_name: str,
    phase_key: str,
) -> HumanMessage:
    """Build the persistent, protected phase instruction block.

    Delivered ONCE per concrete phase instance by the execute node, appended
    to the graph's ``messages`` before compaction so it lands in state and
    the checkpoint. It is a ``HumanMessage`` for every model family (the
    ``role=event`` convention: Anthropic rejects non-consecutive system
    messages and Gemini relocates a mid-history SystemMessage into the
    system slot, which breaks the cached prefix on every turn). The markers
    in ``additional_kwargs`` make it *protected*: skipped by tool-result
    clearing, trimming, elision and the summariser's input, and re-seated
    right after the summary while its phase is current.

    Content is deterministic for identical inputs (prompt-cache hygiene).

    Args:
        file_path: Workspace-relative path of the instruction artifact
        content: Body of the artifact (already rendered)
        phase_name: ``"strategic"`` or ``"tactical"``
        phase_key: ``"<phase_number>:<phase_name>"`` of the concrete phase
    """
    body = (
        f"{PHASE_INSTRUCTION_CONTENT_PREFIX}{phase_name}] Phase instructions "
        f"(from {file_path}). They apply for the whole phase.\n\n{content}"
    )
    return HumanMessage(
        content=body,
        additional_kwargs={
            PERSIST_ROLE_KEY: PERSIST_ROLE_EVENT,
            PROTECTED_KEY: True,
            PHASE_KEY: phase_key,
            INSTRUCTION_PATH_KEY: file_path,
        },
    )


def create_instruction_tool_messages(
    file_path: str,
    content: str,
) -> Tuple[AIMessage, ToolMessage]:
    """Create synthetic AIMessage + ToolMessage pair for instruction file injection.

    .. deprecated::
        The worker's phase-start delivery uses
        :func:`create_phase_instruction_message` (a persistent, protected
        HumanMessage in state) since U2 WP1. This pair is a *transient*
        shape — it is dropped before summarisation and never written to
        state — and is kept only for callers and tests that still build the
        legacy layout (archiver context-frame descriptors, rewind fixtures).

    Creates a fake tool call that makes it appear as if the agent already
    called read_file on the instruction file and received the content.

    Args:
        file_path: Workspace-relative path of the instruction file
        content: Content of the instruction file

    Returns:
        Tuple of (AIMessage with tool_call, ToolMessage with instruction content)
    """
    id_suffix = content_hash_id(file_path + "\n" + content)
    tool_call_id = f"{INSTRUCTION_TOOL_CALL_ID_PREFIX}{id_suffix}"

    ai_message = AIMessage(
        content="",
        tool_calls=[
            {
                "name": "read_file",
                "args": {"path": file_path},
                "id": tool_call_id,
            }
        ],
    )

    tool_message = ToolMessage(
        content=content,
        tool_call_id=tool_call_id,
    )

    return ai_message, tool_message


# The one predicate for "injected context, not conversation" lives in
# context_entries (typed entries plus every legacy tail shape, guidance and
# the App Guide boundary included). The old name stays importable.
is_workspace_injection_message = is_context_injection
