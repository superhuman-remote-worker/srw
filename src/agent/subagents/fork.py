"""``fork=true`` seeding: the child starts from the parent's durable history.

A pure function over a message list. The seed is the parent's DURABLE
``messages`` (the compacted list — never a prepared/transient copy):

1. preserve every durable ``SystemMessage`` (the parent's prompt is not
   durable; a leading one is a legacy compaction summary) and the compaction
   summary's user-role hand-back, which steps 2 and 3 leave alone;
2. drop protected phase blocks (``is_protected_message``), injected context
   (``is_context_injection``: the parent's context entries are the parent's;
   the child's own builds inject what it needs) and ``RemoveMessage``
   markers;
3. drop a trailing assistant message with open tool calls (the loop's
   ``repair_tool_pairing`` would strip it anyway; dropping keeps the durable
   child transcript honest) and repair any other orphan;
4. sanitize for the provider boundary when the child's model family differs
   from the parent's (signed reasoning blocks, tool-call id formats), then
   remint every message id and tool-call id, including provider-native raw
   tool-call representations;
5. append the fork notice as a ``role=event`` HumanMessage — the brief follows
   as the child's first input.

The returned objects share no mutable state with the parent. Their fresh ids
allow the exact seed to be inserted into the child's globally-keyed
``thread_messages`` rows before its first provider call.

Cost note for the tool description (WP2): a fork re-sends the parent's whole
prefix on every child call.
"""

from __future__ import annotations

import copy
import uuid
from typing import Any, Dict, List, Mapping, Optional, Sequence

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage

from agent.core.context import (
    _conforming_tool_call_id,
    repair_tool_pairing,
    sanitize_history_for_provider_boundary,
)
from shared.runtime.core.context_entries import is_context_injection
from shared.runtime.core.message_markers import (
    PERSIST_ROLE_EVENT,
    PERSIST_ROLE_KEY,
    is_protected_message,
)

FORK_NOTICE = (
    "You are a fork of the parent agent: its conversation history precedes this "
    "message. It is context, not your task — the brief for your own work follows."
)


def _is_remove_marker(message: Any) -> bool:
    return type(message).__name__ == "RemoveMessage"


def _families_differ(child_model: Optional[str], parent_model: Optional[str]) -> bool:
    if not child_model or not parent_model:
        return False
    if child_model == parent_model:
        return False
    from shared.runtime.core.model_registry import family_of

    return family_of(child_model) != family_of(parent_model)


def _mint_message_id() -> str:
    """Mint in the same local id space as the persistent loop."""
    return f"msg_{uuid.uuid4().hex[:24]}"


def _mint_tool_call_id(child_model: Optional[str]) -> str:
    """Mint an id accepted by the child's provider family.

    Mistral is the narrowest transport we support (exactly nine
    alphanumerics). Every other current family accepts the ordinary OpenAI
    shaped ``call_<24 hex>`` id.
    """
    from shared.runtime.core.model_registry import family_of

    family = family_of(child_model or "")
    candidate = f"call_{uuid.uuid4().hex[:24]}"
    return _conforming_tool_call_id(candidate, family)


_RAW_TOOL_CALL_TYPES = frozenset(
    {
        "function_call",
        "tool_call",
        "tool_use",
        "server_tool_call",
    }
)


def _raw_call_name(entry: Mapping[str, Any]) -> str:
    function = entry.get("function")
    if isinstance(function, Mapping):
        return str(function.get("name") or "")
    return str(entry.get("name") or "")


def _looks_like_raw_tool_call(entry: Any) -> bool:
    if not isinstance(entry, Mapping):
        return False
    if str(entry.get("type") or "").lower() in _RAW_TOOL_CALL_TYPES:
        return True
    function = entry.get("function")
    return isinstance(function, Mapping) and bool(
        entry.get("id") or entry.get("call_id")
    )


def _rewrite_raw_call(
    entry: Dict[str, Any],
    *,
    index: Optional[int],
    calls: Sequence[Mapping[str, str]],
    aliases: Mapping[str, str],
) -> None:
    """Rewrite one provider-native representation of a canonical call."""
    raw_call_id = str(entry.get("call_id") or entry.get("id") or "")
    fresh = aliases.get(raw_call_id)
    if fresh is None:
        name = _raw_call_name(entry)
        matches = [call["fresh"] for call in calls if call["name"] == name and name]
        if len(matches) == 1:
            fresh = matches[0]
    if fresh is None and index is not None and index < len(calls):
        fresh = calls[index]["fresh"]
    if fresh is None:
        return

    # Responses uses ``call_id`` for pairing and a separate server-issued item
    # ``id`` (normally ``fc_...``). The item id is not a tool-call identity and
    # must be omitted in a self-contained fork: retaining it ties the child to
    # an output item in the parent's response, while minting a fake ``fc_`` id
    # would claim server authority we do not have. LangChain accepts the item
    # without it and pairs the following function_call_output by ``call_id``.
    if "call_id" in entry:
        entry["call_id"] = fresh
        entry.pop("id", None)
    elif "id" in entry:
        entry["id"] = fresh

    # LangChain v1 stores the same Responses item identity under
    # tool_call.extras.item_id and reconstructs a raw ``function_call.id``
    # from it. Remove that alternate representation for the same reason.
    extras = entry.get("extras")
    if isinstance(extras, dict):
        extras.pop("item_id", None)

    function = entry.get("function")
    if isinstance(function, dict):
        if "call_id" in function:
            function["call_id"] = fresh
        if "id" in function:
            function["id"] = fresh


def _rewrite_raw_tool_calls(
    value: Any,
    *,
    calls: Sequence[Mapping[str, str]],
    aliases: Mapping[str, str],
) -> Any:
    """Deep-copy and rewrite raw tool calls embedded in provider metadata.

    LangChain keeps canonical calls in ``AIMessage.tool_calls``, but can also
    retain Chat Completions calls in ``additional_kwargs['tool_calls']``,
    Anthropic ``tool_use`` blocks in content, and Responses ``function_call``
    items in raw output. A fork must not leave the parent's id in any of those
    parallel representations.
    """
    out = copy.deepcopy(value)

    def _visit(node: Any) -> None:
        if isinstance(node, list):
            raw_index = 0
            for item in node:
                if _looks_like_raw_tool_call(item):
                    _rewrite_raw_call(
                        item,
                        index=raw_index,
                        calls=calls,
                        aliases=aliases,
                    )
                    raw_index += 1
                _visit(item)
            return
        if not isinstance(node, dict):
            return
        if _looks_like_raw_tool_call(node):
            _rewrite_raw_call(node, index=None, calls=calls, aliases=aliases)
        for child in node.values():
            _visit(child)

    _visit(out)
    return out


def _rewrite_raw_tool_references(value: Any, aliases: Mapping[str, str]) -> Any:
    """Deep-copy provider-native result blocks and remap their call links."""
    out = copy.deepcopy(value)
    reference_keys = ("call_id", "tool_call_id", "tool_use_id")

    def _visit(node: Any) -> None:
        if isinstance(node, list):
            for item in node:
                _visit(item)
            return
        if not isinstance(node, dict):
            return
        for key in reference_keys:
            raw_id = node.get(key)
            if isinstance(raw_id, str) and raw_id in aliases:
                node[key] = aliases[raw_id]
        for child in node.values():
            _visit(child)

    _visit(out)
    return out


def _remint_seed(
    messages: Sequence[BaseMessage], *, child_model: Optional[str]
) -> List[BaseMessage]:
    """Give a sanitized seed child-owned message and tool-call identities."""
    call_id_map: Dict[str, str] = {}
    reminted: List[BaseMessage] = []

    for message in messages:
        update: Dict[str, Any] = {"id": _mint_message_id()}
        if isinstance(message, AIMessage):
            calls: List[Dict[str, str]] = []
            new_tool_calls: List[Dict[str, Any]] = []
            for tool_call in getattr(message, "tool_calls", None) or []:
                old_id = str(tool_call.get("id") or "")
                fresh = call_id_map.get(old_id)
                if fresh is None:
                    fresh = _mint_tool_call_id(child_model)
                    if old_id:
                        call_id_map[old_id] = fresh
                # A raw representation already rewritten to the fresh id is a
                # harmless no-op if the recursive walker encounters it again.
                call_id_map[fresh] = fresh
                name = str(tool_call.get("name") or "")
                calls.append({"old": old_id, "fresh": fresh, "name": name})
                new_tool_calls.append({**tool_call, "id": fresh})
            update["tool_calls"] = new_tool_calls
            update["additional_kwargs"] = _rewrite_raw_tool_calls(
                message.additional_kwargs,
                calls=calls,
                aliases=call_id_map,
            )
            update["response_metadata"] = _rewrite_raw_tool_calls(
                message.response_metadata,
                calls=calls,
                aliases=call_id_map,
            )
            if isinstance(message.content, list):
                update["content"] = _rewrite_raw_tool_calls(
                    message.content,
                    calls=calls,
                    aliases=call_id_map,
                )
        elif isinstance(message, ToolMessage):
            old_id = str(getattr(message, "tool_call_id", "") or "")
            fresh = call_id_map.get(old_id)
            if fresh is not None:
                update["tool_call_id"] = fresh
                update["additional_kwargs"] = _rewrite_raw_tool_references(
                    message.additional_kwargs, call_id_map
                )
                update["response_metadata"] = _rewrite_raw_tool_references(
                    message.response_metadata, call_id_map
                )
                if isinstance(message.content, list):
                    update["content"] = _rewrite_raw_tool_references(
                        message.content, call_id_map
                    )
        reminted.append(message.model_copy(update=update, deep=True))

    return reminted


def seed_fork_history(
    parent_messages: List[Any],
    *,
    child_model: Optional[str] = None,
    parent_model: Optional[str] = None,
) -> List[BaseMessage]:
    """The child's initial ``messages`` for a fork (see module docstring)."""
    seed: List[BaseMessage] = []
    for message in parent_messages or []:
        if not isinstance(message, BaseMessage) or _is_remove_marker(message):
            continue
        if is_protected_message(message) or is_context_injection(message):
            continue
        seed.append(copy.deepcopy(message))
    while (
        seed
        and isinstance(seed[-1], AIMessage)
        and getattr(seed[-1], "tool_calls", None)
    ):
        seed.pop()
    seed = list(repair_tool_pairing(seed))
    if _families_differ(child_model, parent_model):
        seed = list(sanitize_history_for_provider_boundary(seed, child_model or ""))
    seed.append(
        HumanMessage(
            content=FORK_NOTICE,
            additional_kwargs={PERSIST_ROLE_KEY: PERSIST_ROLE_EVENT},
        )
    )
    return _remint_seed(seed, child_model=child_model)


__all__ = ["FORK_NOTICE", "seed_fork_history"]
