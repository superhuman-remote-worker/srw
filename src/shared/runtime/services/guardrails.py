"""Guardrails resolution and injection for model-family-specific surface forms.

This module is the runtime side of the guardrails matrix design
(``knowledge-base/knowledge/design/guardrails_matrix.md``). It does three things:

1. **Tool docstring injection** — ``apply_guardrails_to_tools`` rewrites the
   ``Examples:`` block in each bound tool's description so the model sees
   call examples in the wire format its inference engine expects (Python
   parens for OpenAI-compatible parsers, Gemma canonical brace form for
   vLLM ``gemma4``, etc.).

2. **Runtime nudge resolution** — ``format_nudge`` looks up a nudge by key,
   resolves the family-specific template via the matrix, and applies
   ``str.format(**placeholders)`` with placeholder validation against
   ``KNOWN_NUDGES``.

3. **Block detection / replacement** — ``_replace_examples_block`` is the
   regex helper used by ``apply_guardrails_to_tools``; exposed for tests.
"""

from __future__ import annotations

import logging
import re
from copy import copy as shallow_copy
from typing import Any, Dict, Iterable, List, Optional, Set

from shared.runtime.core.loader import resolve_guardrails

logger = logging.getLogger(__name__)


# Registry of every nudge key in use, with its required placeholder set.
# `format_nudge` validates against this map: missing placeholders raise
# KeyError (already raised by str.format), unknown keys log a warning,
# unknown placeholders for a known key raise ValueError. New nudge sites
# MUST add their key here.
KNOWN_NUDGES: Dict[str, Set[str]] = {
    # src/graph.py recovery + phase nudges
    "todo_action": {"todo_id"},
    "degenerate_recovery_assistant": set(),
    "degenerate_recovery_user": {"pattern_detail"},
    "loop_warning_suffix": set(),
    "tool_not_found_suffix": set(),
    "phase_transition_strategic_to_tactical": {
        "phase_number",
        "phase_name",
        "todo_count",
    },
    "phase_transition_tactical_to_strategic": {"phase_number"},
    "act_ratio_nudge": {"count"},
    # src/graph.py audited_tools — the per-call phase gate (U2)
    "phase_gate_strategic_tool_in_tactical": {"tool", "phase_number"},
    "phase_gate_tactical_tool_in_strategic": {"tool", "phase_number"},
    "phase_gate_batch_note": set(),
    # src/managers/todo.py
    "todo_list_footer": set(),
    # src/services/recall_store.py
    "memory_block_header_pinned": set(),
    "memory_block_header_retrieved": set(),
    "memory_block_footer": {"count", "tokens"},
    "memory_entry_header": set(),
    "memory_summary_header": {"count"},
    "memory_summary_hint": set(),
    # src/services/knowledge_store.py
    "knowledge_block_header": set(),
    "knowledge_block_footer": {"count", "tokens"},
    # src/agent/core/citation_feedback_injection.py, src/agent/core/
    # context_injection.py (append-only cleared renderings)
    "citation_feedback_resolved": set(),
    "active_subagents_none": set(),
    # src/persistent_graph.py
    "empty_response_recovery": set(),
    # src/tools/context.py, src/tools/registry.py, src/tools/workspace/files.py
    "read_file_required_error": {"file_path", "tool_name"},
}


# =============================================================================
# Tool docstring injection
# =============================================================================


# Matches an "Examples:" or "Example:" heading at any indentation, through
# the indented body that follows, ending at the first dedented non-empty
# line OR end of string. Body lines are either blank-or-whitespace-only or
# indented (any depth). The final line in the body may lack a trailing
# newline (`\Z` anchor) — common when the block sits at the end of a
# docstring.
_EXAMPLES_BLOCK_RE = re.compile(
    r"""
    (?P<lead>\n*)                                  # optional leading newlines
    (?P<indent>[ \t]*)Examples?:[ \t]*\n           # heading line
    (?P<body>(?:                                   # body: blank lines or
        (?:[ \t]*\n)                               #   indented lines (any depth);
        |                                          #   the last line may lack
        (?:[ \t]+[^\n]*(?:\n|\Z))                  #   a trailing newline
    )*)
    """,
    re.VERBOSE,
)


def _replace_examples_block(description: str, replacement: str) -> str:
    """Replace the first ``Examples:`` (or ``Example:``) block in a docstring.

    Strips the existing block (heading + indented body). Appends the
    ``replacement`` content separated by a blank line. If no examples block
    is found, the replacement is appended at the end with a leading blank
    line so it stays visually distinct.

    The replacement is used verbatim (it should already be a complete block,
    typically including its own ``Examples:`` heading).
    """
    if not description:
        return replacement.strip("\n")

    rstripped_replacement = replacement.rstrip()
    match = _EXAMPLES_BLOCK_RE.search(description)
    if match is None:
        return description.rstrip() + "\n\n" + rstripped_replacement

    before = description[: match.start()].rstrip()
    after = description[match.end() :]
    parts = [before, rstripped_replacement]
    after = after.lstrip("\n")
    if after:
        parts.append(after)
    return "\n\n".join(p for p in parts if p)


def _tool_with_description(tool: Any, new_description: str) -> Any:
    """Return a tool-like with `description` swapped, without poisoning the source.

    LangChain's StructuredTool exposes `.copy(update={...})` on Pydantic v2.
    We try that first, then fall back to a shallow copy + attribute set.
    Mutating the original is avoided because @tool definitions are global.
    """
    try:
        # Pydantic v2 model_copy
        return tool.model_copy(update={"description": new_description})
    except AttributeError:
        pass
    try:
        # Pydantic v1 copy
        return tool.copy(update={"description": new_description})
    except (AttributeError, TypeError):
        pass
    new_tool = shallow_copy(tool)
    try:
        new_tool.description = new_description
    except Exception:
        logger.warning(
            "Could not set description on tool %r; returning original",
            getattr(tool, "name", tool),
        )
        return tool
    return new_tool


def apply_guardrails_to_tools(
    tools: Iterable[Any],
    family: Optional[str] = None,
    *,
    model: Optional[str] = None,
    deployment_dir: Optional[str] = None,
    guardrails: Optional[Dict[str, Any]] = None,
) -> List[Any]:
    """Return tools with descriptions rewritten for the model family.

    For each tool whose name appears in ``guardrails.tool_examples``, replace
    the ``Examples:`` block in its description with the family-specific
    block. Tools without an entry are returned unchanged.

    Resolution: if ``guardrails`` is provided it is used as-is. Otherwise the
    module resolves it via ``resolve_guardrails(model, deployment_dir)``.
    Either ``family`` or ``model`` must be supplied (``family`` is accepted
    for callers who already resolved it; ``model`` is the common path).

    Args:
        tools: Iterable of LangChain tool instances (StructuredTool, Tool, or
            anything with ``.name`` and a settable ``.description``).
        family: Resolved family string (e.g. ``"gemma"``). When provided,
            ``model`` is ignored for resolution.
        model: Model name; used with ``deployment_dir`` to resolve guardrails
            when ``family`` is not given.
        deployment_dir: Optional expert directory for per-expert overlay.
        guardrails: Pre-resolved guardrails dict (escape hatch for tests).

    Returns:
        New list of tools with rewritten descriptions; original tools are
        not mutated.
    """
    if guardrails is None:
        # None/empty model → fall back to the `default` family. This matches
        # the bind-time call sites (agent.py, persistent_session.py) which
        # already tolerate a missing model by passing "" through `family_of`.
        if model:
            guardrails = resolve_guardrails(model, deployment_dir)
        elif family:
            from shared.runtime.core.loader import _load_guardrails_matrix, deep_merge

            matrix = _load_guardrails_matrix(deployment_dir)
            default_guardrails = matrix.get("default", {})
            family_guardrails = matrix.get(family, {}) if family != "default" else {}
            guardrails = deep_merge(default_guardrails, family_guardrails)
        else:
            guardrails = resolve_guardrails("", deployment_dir)

    tool_examples = guardrails.get("tool_examples") or {}
    out: List[Any] = []
    for tool in tools:
        name = getattr(tool, "name", None)
        replacement = tool_examples.get(name) if isinstance(name, str) else None
        if not replacement:
            out.append(tool)
            continue
        original = getattr(tool, "description", None)
        if not isinstance(original, str):
            # Tool isn't a real LangChain tool (e.g. a test mock); leave it.
            out.append(tool)
            continue
        new_description = _replace_examples_block(original, replacement)
        if new_description == original:
            out.append(tool)
            continue
        out.append(_tool_with_description(tool, new_description))
    return out


# =============================================================================
# Runtime nudge resolution
# =============================================================================


class GuardrailFormatError(ValueError):
    """Raised when a nudge template's placeholders don't match the call site."""


def format_nudge(
    key: str,
    *,
    family: Optional[str] = None,
    model: Optional[str] = None,
    deployment_dir: Optional[str] = None,
    guardrails: Optional[Dict[str, Any]] = None,
    **placeholders: Any,
) -> str:
    """Resolve and format a runtime nudge by key.

    Looks up ``guardrails.nudges[key]`` and applies ``str.format(**placeholders)``.
    The set of ``placeholders`` must exactly match ``KNOWN_NUDGES[key]``
    (extra keys raise ``GuardrailFormatError``; missing keys raise the usual
    ``KeyError`` from ``str.format``).

    Args:
        key: Nudge key (must be in ``KNOWN_NUDGES``).
        family / model / deployment_dir / guardrails: Same resolution rules
            as ``apply_guardrails_to_tools``.
        **placeholders: Values for ``{name}`` slots in the template.

    Returns:
        Formatted string ready to be put into a HumanMessage / AIMessage /
        ToolMessage / footer block.

    Raises:
        GuardrailFormatError: Unknown key, or extra placeholders.
        KeyError: Missing placeholder for the template.
    """
    if key not in KNOWN_NUDGES:
        raise GuardrailFormatError(
            f"Unknown nudge key {key!r}. Add it to KNOWN_NUDGES in "
            "src/services/guardrails.py before using it at a new call site."
        )
    expected = KNOWN_NUDGES[key]
    extra = set(placeholders) - expected
    if extra:
        raise GuardrailFormatError(
            f"Nudge {key!r} got unexpected placeholders {sorted(extra)}; "
            f"expected only {sorted(expected)}."
        )

    if guardrails is None:
        # None/empty model → fall back to the `default` family. Call sites
        # at the edge of the agent (managers, services, ToolContext) may not
        # always have the model name in scope; the default family is the
        # safe baseline.
        if model:
            guardrails = resolve_guardrails(model, deployment_dir)
        elif family:
            from shared.runtime.core.loader import _load_guardrails_matrix, deep_merge

            matrix = _load_guardrails_matrix(deployment_dir)
            default_guardrails = matrix.get("default", {})
            family_guardrails = matrix.get(family, {}) if family != "default" else {}
            guardrails = deep_merge(default_guardrails, family_guardrails)
        else:
            guardrails = resolve_guardrails("", deployment_dir)

    template = (guardrails.get("nudges") or {}).get(key)
    if not template:
        raise GuardrailFormatError(
            f"Nudge {key!r} not present in resolved guardrails. "
            "Add it to config/guardrails/default.yaml."
        )
    return template.format(**placeholders)
