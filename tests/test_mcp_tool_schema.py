"""An MCP tool's input schema as every model provider accepts it.

DeepSeek refused a whole Session turn (400 "Invalid schema for function
'mcp__civitai_browse__send_platform_feedback': ... is not a regex") because
one of Civitai's tools wrote ``pattern`` in a dialect its validator rejects
(``\\:``). A pattern now travels as a sentence in the description; the
server still checks its own input.
"""

from __future__ import annotations

from types import SimpleNamespace

from agent.tools.mcp.manager import MCPManager, _ServerHandle
from agent.tools.mcp.schema import model_safe_schema

CIVITAI_FEEDBACK = {
    "type": "object",
    "properties": {
        "ref": {"type": "string", "pattern": "^[a-z0-9][a-z0-9._-]{0,79}$"},
        "docUrl": {
            "type": "string",
            "description": "The page the feedback is about.",
            "pattern": "^https\\:\\/\\/developer\\.civitai\\.com\\/",
        },
        "refs": {"type": "array", "items": {"type": "string", "pattern": "^[A-Z]+$"}},
    },
    "required": ["ref"],
}


def _patterns(node) -> list[str]:
    found = []
    if isinstance(node, dict):
        for key, value in node.items():
            if key in ("pattern", "patternProperties") and not (
                key == "pattern" and isinstance(value, dict)
            ):
                found.append(key)
            found += _patterns(value)
    elif isinstance(node, list):
        for value in node:
            found += _patterns(value)
    return found


def test_a_pattern_becomes_a_sentence_in_its_description():
    safe = model_safe_schema(CIVITAI_FEEDBACK)
    assert _patterns(safe) == []
    properties = safe["properties"]
    assert properties["ref"]["description"] == (
        "Must match the regular expression ^[a-z0-9][a-z0-9._-]{0,79}$."
    )
    assert properties["docUrl"]["description"] == (
        "The page the feedback is about. Must match the regular expression "
        "^https\\:\\/\\/developer\\.civitai\\.com\\/."
    )
    assert properties["refs"]["items"]["description"] == (
        "Must match the regular expression ^[A-Z]+$."
    )
    assert safe["required"] == ["ref"]
    # The server's own schema is untouched.
    assert "pattern" in CIVITAI_FEEDBACK["properties"]["docUrl"]


def test_data_named_pattern_is_left_alone():
    schema = {
        "type": "object",
        "properties": {
            # A field called "pattern" is a property, not the keyword.
            "pattern": {"type": "string", "default": "x*"},
            "filter": {
                "type": "object",
                "default": {"pattern": "keep me"},
                "examples": [{"pattern": "and me"}],
            },
        },
    }
    safe = model_safe_schema(schema)
    assert safe["properties"]["pattern"] == {"type": "string", "default": "x*"}
    assert safe["properties"]["filter"]["default"] == {"pattern": "keep me"}
    assert safe["properties"]["filter"]["examples"] == [{"pattern": "and me"}]


def test_pattern_properties_become_a_sentence_and_keep_their_keys_allowed():
    schema = {
        "type": "object",
        "additionalProperties": False,
        "patternProperties": {"^x-": {"type": "string"}},
        "anyOf": [{"properties": {"a": {"type": "string", "pattern": "^a"}}}],
    }
    safe = model_safe_schema(schema)
    assert _patterns(safe) == []
    assert safe["additionalProperties"] is True
    assert safe["description"] == "Keys matching ^x- are also allowed."
    assert safe["anyOf"][0]["properties"]["a"]["description"] == (
        "Must match the regular expression ^a."
    )


def test_the_bound_tool_carries_the_safe_schema():
    manager = MCPManager([])
    handle = _ServerHandle(
        ds={"name": "Civitai browse"}, config=None, slug="civitai_browse"
    )
    listed = [
        SimpleNamespace(
            name="send_platform_feedback",
            description="Send feedback.",
            input_schema=CIVITAI_FEEDBACK,
            annotations=None,
        )
    ]
    (tool,) = manager._wrap_tools(handle, listed)
    assert _patterns(tool.args_schema) == []
    assert tool.args_schema["required"] == ["ref"]
