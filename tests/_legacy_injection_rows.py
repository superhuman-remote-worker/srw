"""Legacy injection pairs, as the per-request tail injected them.

Append-only context injection (WP6) removed the tail and its creators from
``src/``. Histories restored from before the change, archived rows and old
checkpoints still carry these synthetic tool-call pairs, so the readers that
recognise them (``is_legacy_injection``, the archiver, compaction, the
extraction filter) keep their tests. These builders make the same messages,
byte for byte, for those tests.
"""

from typing import Any, Dict, Tuple

from langchain_core.messages import AIMessage, BaseMessage, ToolMessage

from shared.runtime.core.injection_markers import (
    CHARTER_TOOL_CALL_ID_PREFIX,
    CITATION_FEEDBACK_TOOL_CALL_ID_PREFIX,
    GUIDANCE_TOOL_CALL_ID_PREFIX,
    KNOWLEDGE_TOOL_CALL_ID_PREFIX,
    MEMORY_TOOL_CALL_ID_PREFIX,
)
from shared.runtime.core.workspace_injection import content_hash_id


def _pair(
    prefix: str, name: str, args: Dict[str, Any], content: str
) -> Tuple[AIMessage, ToolMessage]:
    tool_call_id = f"{prefix}{content_hash_id(content)}"
    ai_message = AIMessage(
        content="",
        tool_calls=[{"name": name, "args": args, "id": tool_call_id}],
    )
    return ai_message, ToolMessage(content=content, tool_call_id=tool_call_id)


def create_memory_injection_messages(content: str) -> Tuple[AIMessage, ToolMessage]:
    return _pair(
        MEMORY_TOOL_CALL_ID_PREFIX,
        "recall_memories",
        {"context": "current_task"},
        content,
    )


def create_knowledge_injection_messages(
    content: str,
) -> Tuple[AIMessage, ToolMessage]:
    return _pair(
        KNOWLEDGE_TOOL_CALL_ID_PREFIX,
        "kb_search",
        {"query": "current_task_context"},
        content,
    )


def create_charter_injection_messages(content: str) -> Tuple[AIMessage, ToolMessage]:
    return _pair(CHARTER_TOOL_CALL_ID_PREFIX, "kb_read", {"note": "charter"}, content)


def create_citation_feedback_injection_messages(
    content: str,
) -> Tuple[AIMessage, ToolMessage]:
    return _pair(
        CITATION_FEEDBACK_TOOL_CALL_ID_PREFIX,
        "check_citation_verification",
        {},
        content,
    )


def create_guidance_injection_messages(
    content: str,
) -> Tuple[AIMessage, ToolMessage]:
    return _pair(
        GUIDANCE_TOOL_CALL_ID_PREFIX,
        "supervisor_guidance",
        {"scope": "current_run"},
        content,
    )


def _in_pair(message: BaseMessage, prefix: str) -> bool:
    if isinstance(message, ToolMessage):
        return (getattr(message, "tool_call_id", "") or "").startswith(prefix)
    if isinstance(message, AIMessage):
        return any(
            (tc.get("id") or "").startswith(prefix) for tc in message.tool_calls or []
        )
    return False


def is_memory_injection_message(message: BaseMessage) -> bool:
    return _in_pair(message, MEMORY_TOOL_CALL_ID_PREFIX)


def is_knowledge_injection_message(message: BaseMessage) -> bool:
    return _in_pair(message, KNOWLEDGE_TOOL_CALL_ID_PREFIX)


def is_citation_feedback_injection_message(message: BaseMessage) -> bool:
    return _in_pair(message, CITATION_FEEDBACK_TOOL_CALL_ID_PREFIX)


def is_guidance_injection_message(message: BaseMessage) -> bool:
    return _in_pair(message, GUIDANCE_TOOL_CALL_ID_PREFIX)
