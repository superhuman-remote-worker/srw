"""An MCP tool's input schema, as every model provider accepts it.

A server writes its tool's ``inputSchema`` for its own validator. A provider
validates the schema again when the agent binds the tool, with its own
rules, and one schema it rejects fails the whole request, so every turn of
the Session fails, not only that tool.

The regular expressions are the trap: a ``pattern`` is written in some
regex dialect, and a provider checks it with its own engine. DeepSeek
rejected Civitai's ``^https\\:\\/\\/developer\\.civitai\\.com\\/`` ("is not a
regex": ``\\:`` is not a valid escape in the ECMAScript dialect it checks
with). So a pattern is moved into the field's description, which every
model reads and no provider parses. The server still checks its input
itself; nothing it enforces is lost, only the provider's copy of the check.
"""

from __future__ import annotations

import copy
from typing import Any

#: Keywords whose value is one subschema.
_ONE = (
    "items",
    "additionalProperties",
    "additionalItems",
    "unevaluatedProperties",
    "unevaluatedItems",
    "not",
    "if",
    "then",
    "else",
    "contains",
    "propertyNames",
)
#: Keywords whose value maps names to subschemas.
_BY_NAME = ("properties", "$defs", "definitions", "dependentSchemas")
#: Keywords whose value lists subschemas.
_LISTED = ("anyOf", "oneOf", "allOf", "prefixItems", "items")


def model_safe_schema(schema: Any) -> Any:
    """A copy of ``schema`` without regular expressions a provider would
    check: each ``pattern`` becomes a sentence in its description, and
    ``patternProperties`` a sentence on the object."""
    if not isinstance(schema, dict):
        return schema
    cleaned = copy.deepcopy(schema)
    _clean(cleaned)
    return cleaned


def _clean(node: Any) -> None:
    if not isinstance(node, dict):
        return
    notes: list[str] = []
    pattern = node.get("pattern")
    if isinstance(pattern, str):
        del node["pattern"]
        notes.append(f"Must match the regular expression {pattern}")
    by_pattern = node.get("patternProperties")
    if isinstance(by_pattern, dict):
        del node["patternProperties"]
        if by_pattern:
            notes.append(
                "Keys matching "
                + " or ".join(str(key) for key in by_pattern)
                + " are also allowed"
            )
            if node.get("additionalProperties") is False:
                # Those keys were allowed; do not tell the model none are.
                node["additionalProperties"] = True
    if notes:
        description = node.get("description")
        text = ". ".join(notes) + "."
        node["description"] = (
            f"{description.rstrip()} {text}"
            if isinstance(description, str) and description.strip()
            else text
        )
    for key in _ONE:
        if isinstance(node.get(key), dict):
            _clean(node[key])
    for key in _BY_NAME:
        children = node.get(key)
        if isinstance(children, dict):
            for child in children.values():
                _clean(child)
    for key in _LISTED:
        children = node.get(key)
        if isinstance(children, list):
            for child in children:
                _clean(child)


__all__ = ["model_safe_schema"]
