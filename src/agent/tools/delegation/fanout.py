"""What THIS parent may delegate at once (parallel_subagents.md §6.4, §12).

One place answers "may this session parent fan out" and "how many children
run at once", so the ``delegate_agent`` description and the runtime semaphore
read the same values. Every function reads the parent's context LIVE: a
session's ``tool_context.config`` is refreshed in place by a live config
update, and the runtime's limiter re-reads the cap at every admission.

A session parent may fan out only when all three hold:

1. the configuration gate is on for its lane (``delegation.session_fanout``;
   the pinned lane also needs ``delegation.session_fanout_pinned``, D5);
2. the orchestrator advertised ``session_subagent_batch_settle_contract: 1``
   in the attach payload (published on the context as
   ``_session_subagent_batch_settle_contract`` before tools load), so an
   interrupted batch can be settled once;
3. the parent is a session (``_subagent_parent_kind``) on a known lane
   (``_subagent_execution_lane``).

Workers are unaffected: their cap stays ``delegation.max_concurrent``.

Light on purpose (shared imports only): the tool module and
``agent.subagents.runtime`` both import it.
"""

from __future__ import annotations

from typing import Any, Mapping

from shared.runtime.core.delegation_settings import (
    session_fanout_configured,
    session_max_calls_per_turn,
    session_max_concurrent,
)

#: The worker cap when ``delegation.max_concurrent`` is unset or unusable.
WORKER_MAX_CONCURRENT_DEFAULT = 4


def _delegation(context: Any) -> Mapping[str, Any]:
    config = getattr(context, "config", None) or {}
    block = config.get("delegation") if isinstance(config, Mapping) else None
    return block if isinstance(block, Mapping) else {}


def is_session_parent(context: Any) -> bool:
    return getattr(context, "_subagent_parent_kind", None) == "session"


def session_fanout_allowed(context: Any) -> bool:
    """True when this session parent may run several delegate calls per response.

    Gate on for its lane AND the orchestrator can settle a batch AND the
    parent is a session. False for every worker.
    """
    if not is_session_parent(context):
        return False
    if getattr(context, "_session_subagent_batch_settle_contract", False) is not True:
        return False
    return session_fanout_configured(
        _delegation(context), getattr(context, "_subagent_execution_lane", None)
    )


def delegation_max_concurrent(context: Any) -> int:
    """How many children this parent runs at once — the description's cap and
    the semaphore's size.

    A session allowed to fan out uses its own cap (explicit value, else the
    parent family's, else 6; 1..20). Every other parent keeps the worker cap
    ``delegation.max_concurrent`` (default 4, floor 1), so a session with the
    gate off behaves exactly as before.
    """
    delegation = _delegation(context)
    if session_fanout_allowed(context):
        return session_max_concurrent(delegation)
    raw = delegation.get("max_concurrent")
    try:
        cap = WORKER_MAX_CONCURRENT_DEFAULT if raw is None else int(raw)
    except (TypeError, ValueError):
        cap = WORKER_MAX_CONCURRENT_DEFAULT
    return max(1, cap)


def delegation_max_calls_per_turn(context: Any) -> int:
    """Total delegate calls one session parent turn may make across batches
    (default 20). Enforced by the live loop (WP3b)."""
    return session_max_calls_per_turn(_delegation(context))


def parent_parallel_tool_calls(context: Any) -> bool:
    """Whether the parent's model may put several tool calls in one message.

    The session tool config carries the live ``parallel_tool_calls`` (it is
    re-derived on a model switch); otherwise the context's LLM config. A
    family with ``parallel_tool_calls: false`` issues one call per message.
    """
    config = getattr(context, "config", None) or {}
    if isinstance(config, Mapping):
        value = config.get("parallel_tool_calls")
        if isinstance(value, bool):
            return value
    llm_config = getattr(context, "_llm_config", None)
    value = getattr(llm_config, "parallel_tool_calls", None)
    return value if isinstance(value, bool) else True


__all__ = [
    "WORKER_MAX_CONCURRENT_DEFAULT",
    "delegation_max_calls_per_turn",
    "delegation_max_concurrent",
    "is_session_parent",
    "parent_parallel_tool_calls",
    "session_fanout_allowed",
]
