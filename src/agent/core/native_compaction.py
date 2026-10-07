"""Native compaction: the working model writes its own summary (compaction WP5/WP6).

Each conversation keeps its last main request exactly as sent, in memory, next
to its model client (``ContextManager.record_main_request``). A native
compaction sends that request unchanged, then the messages added since, then
the recipe's instruction as the final user message, through the same client:
same tools, settings and reasoning (D4). The previous request is an exact
prefix of this one, which is what a cache hit on the GPT proxy path needs
(F17, D13).

Native runs only where the caller allows it and the kept request is still
there. Anything else (nothing kept, the history changed under it, the fork
does not fit, the reply is a tool call, empty, cut off or a refusal, a
provider error) yields a fallback reason, and the caller runs the auxiliary
fold instead (D5). Either way the summary goes back in the same hand-back
wrapper (WP9).

See knowledge-base/knowledge/features/compaction_refactor_fidelity_and_fork_strategy.md
(§6 WP5, WP6 and the design pass decisions of 2026-10-07).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Callable, FrozenSet, Iterable, List, Optional, Sequence, Tuple

from langchain_core.messages import (
    BaseMessage,
    HumanMessage,
    RemoveMessage,
    SystemMessage,
    ToolMessage,
)

from agent.core.summarizer import SummaryRejected, validate_summary_message
from shared.runtime.core.context_entries import (
    fold_context_entries,
    is_legacy_injection,
)
from shared.runtime.core.message_markers import is_compaction_summary

logger = logging.getLogger(__name__)

STRATEGY_NATIVE = "native"
STRATEGY_AUXILIARY = "auxiliary"

# Recipes with an instruction file under config/prompts/compaction/<name>/.
# Only these names are ever turned into a path.
KNOWN_RECIPES = ("codex",)

# Output room assumed when the client's own output cap cannot be read
# (Codex keeps the same 16k buffer for its own fallback).
DEFAULT_OUTPUT_RESERVE = 16_384


@dataclass(frozen=True)
class CompactionSettings:
    """The family's compaction strategy (``llm.compaction`` in the matrix)."""

    strategy: str = STRATEGY_AUXILIARY
    recipe: Optional[str] = None

    @property
    def native(self) -> bool:
        return self.strategy == STRATEGY_NATIVE


def compaction_settings(raw: Any) -> CompactionSettings:
    """Read ``{strategy, recipe}``; anything that is not native is the fold."""
    if not isinstance(raw, dict):
        return CompactionSettings()
    strategy = str(raw.get("strategy") or STRATEGY_AUXILIARY).strip().lower()
    if strategy != STRATEGY_NATIVE:
        return CompactionSettings()
    recipe = raw.get("recipe")
    return CompactionSettings(
        STRATEGY_NATIVE, str(recipe).strip().lower() if recipe else None
    )


@lru_cache(maxsize=None)
def recipe_instruction(name: Optional[str]) -> Optional[str]:
    """The recipe's instruction text, or None for an unknown recipe."""
    if name not in KNOWN_RECIPES:
        return None
    from shared.runtime.core.loader import get_project_root

    path = get_project_root() / "config" / "prompts" / "compaction" / name / "prompt.md"
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        logger.error(f"Compaction recipe {name!r} has no instruction file: {e}")
        return None
    return text.strip() or None


def instruction_with_focus(instruction: str, focus: Optional[str]) -> str:
    """Append a manual ``/compact <focus>`` to the recipe instruction."""
    if not focus:
        return instruction
    return (
        f"{instruction}\n\nThe user asked this summary to focus on: {focus}\n"
        "Give that topic the most detail."
    )


def _keys(message: BaseMessage) -> Tuple[Tuple[str, Any], ...]:
    keys: List[Tuple[str, Any]] = [("obj", id(message))]
    message_id = getattr(message, "id", None)
    if message_id:
        keys.append(("id", str(message_id)))
    return tuple(keys)


@dataclass
class LastRequest:
    """A conversation's last main request, as sent (WP5).

    ``history`` holds every message object that fed the request (the request
    as sent, before and after the carrier fold, and the history list it was
    built from). It keeps them referenced, so ``seen`` (their ids and object
    identities) cannot match a new object at a reused address.
    """

    messages: List[BaseMessage]
    llm: Any
    history: List[BaseMessage]
    input_tokens: Optional[int] = None
    timeout: Optional[float] = None
    seen: FrozenSet[Tuple[str, Any]] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        if not self.seen:
            self.seen = frozenset(k for m in self.history for k in _keys(m))

    def carried(self, message: BaseMessage) -> bool:
        return any(key in self.seen for key in _keys(message))


def _durable(message: BaseMessage) -> bool:
    """A history message a main request is built from."""
    if isinstance(message, RemoveMessage) or is_legacy_injection(message):
        return False
    return not (
        isinstance(message, SystemMessage) and not is_compaction_summary(message)
    )


def messages_since(
    last: LastRequest, messages: Sequence[BaseMessage]
) -> Tuple[Optional[List[BaseMessage]], Optional[str]]:
    """The messages added to ``messages`` since ``last`` was sent.

    They must be a suffix: every history message up to the newest one the
    last request carried was in that request. Otherwise the history changed
    under it (a repair, a rewrite), the kept request no longer describes it,
    and the reason is ``history_changed``. A tool call still waiting for its
    result cannot be followed by the instruction (``pending_tool_calls``).
    """
    durable = [m for m in messages if _durable(m)]
    anchor = next(
        (i for i in range(len(durable) - 1, -1, -1) if last.carried(durable[i])),
        None,
    )
    if anchor is None or not all(last.carried(m) for m in durable[:anchor]):
        return None, "history_changed"
    added = durable[anchor + 1 :]
    answered = {m.tool_call_id for m in added if isinstance(m, ToolMessage)}
    for message in added:
        for call in getattr(message, "tool_calls", None) or []:
            if call.get("id") not in answered:
                return None, "pending_tool_calls"
    return added, None


def build_fork(
    last: LastRequest, added: Sequence[BaseMessage], instruction: str
) -> List[BaseMessage]:
    """The previous request unchanged, the new messages, the instruction.

    The new messages go through the carrier fold on their own: an entry with
    no carrier among them becomes a standalone user message rather than
    rewriting the last message of the previous request.
    """
    return [
        *last.messages,
        *fold_context_entries(list(added)),
        HumanMessage(content=instruction),
    ]


def _positive_int(source: Any, name: str) -> Optional[int]:
    value = (
        source.get(name) if isinstance(source, dict) else getattr(source, name, None)
    )
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    return None


def output_cap(llm: Any) -> Optional[int]:
    """The output token cap the client sends, through any binding layers."""
    target = llm
    for _ in range(5):
        if target is None:
            break
        for source in (target, getattr(target, "kwargs", None)):
            if source is None:
                continue
            for name in ("max_tokens", "max_completion_tokens", "max_output_tokens"):
                value = _positive_int(source, name)
                if value:
                    return value
        target = getattr(target, "bound", None)
    return None


def fork_input_tokens(
    last: LastRequest,
    tail: Sequence[BaseMessage],
    count: Callable[[List[BaseMessage]], int],
) -> int:
    """The fork's input size: the provider's count for the previous request
    (it includes the tool schemas a local count cannot see) plus a local
    count of what follows it."""
    base = last.input_tokens if last.input_tokens else count(list(last.messages))
    return base + count(list(tail))


def summary_from_reply(reply: Any) -> Tuple[Optional[str], Optional[str]]:
    """The summary text of the fork's reply, or the reason it is unusable."""
    if getattr(reply, "tool_calls", None):
        return None, "tool_call"
    metadata = getattr(reply, "response_metadata", None) or {}
    finish = str(metadata.get("finish_reason") or metadata.get("stop_reason") or "")
    if (getattr(reply, "additional_kwargs", None) or {}).get("refusal") or (
        finish.lower() == "refusal"
    ):
        return None, "refusal"
    try:
        return validate_summary_message(reply, expect_sections=False), None
    except SummaryRejected as e:
        return None, e.reason


def history_for(
    sent: Iterable[BaseMessage], *more: Iterable[BaseMessage]
) -> List[BaseMessage]:
    """Every message object behind a request: as sent, plus the lists it was
    built from (the unfolded request, the history list)."""
    out: List[BaseMessage] = list(sent)
    for group in more:
        out.extend(m for m in (group or []) if isinstance(m, BaseMessage))
    return out


__all__ = [
    "CompactionSettings",
    "DEFAULT_OUTPUT_RESERVE",
    "KNOWN_RECIPES",
    "LastRequest",
    "STRATEGY_AUXILIARY",
    "STRATEGY_NATIVE",
    "build_fork",
    "compaction_settings",
    "fork_input_tokens",
    "history_for",
    "instruction_with_focus",
    "messages_since",
    "output_cap",
    "recipe_instruction",
    "summary_from_reply",
]
