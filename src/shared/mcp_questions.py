"""Questions an MCP server asks mid-call, and what SRW answers by itself.

An MCP server may ask the person behind its client a question while a tool
call runs ("elicitation"): Civitai asks "Spend 12 Buzz to run imageGen?"
before it spends anything. Each MCP connector carries a setting for these,
``server_questions`` (owner decisions of 2026-11-17 in
knowledge-base/knowledge/features/mcp_client_2026_07_28_elicitation.md):

- ``decline`` (the default): a job declines and the model is told why. A
  Session in ``autonomous`` mode does the same; a Session in ``read_only`` or
  ``auto_accept`` mode shows the question to the user, with "Always accept in
  this session".
- ``accept``: a yes/no confirmation is accepted without asking anyone. A
  question that needs typed input, or a link to open, still goes to a person
  in an interactive Session and is declined everywhere else.

A connector without its own value follows the installation's default, an
admin setting. A Session's "Always accept in this session" turns one
connector to ``accept`` for that Session.

The form rules are the 2026-07-28 specification's: a flat object whose
properties are strings (``minLength``, ``maxLength``, ``format`` email, uri,
date or date-time), numbers or integers (``minimum``, ``maximum``), booleans,
a single-select enum (``enum``, or ``oneOf`` of ``const``/``title``) or a
multi-select enum (an array of those), each with an optional ``default``.
Framework-free: the agent, the orchestrator and their tests share it.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

DECLINE = "decline"
ACCEPT = "accept"
#: The values a connector's setting may hold.
SETTINGS = (DECLINE, ACCEPT)
#: The connector ``config`` key, and the agent payload entry's.
SERVER_QUESTIONS_KEY = "server_questions"
#: The ``system_settings`` row holding the installation's default.
SYSTEM_SETTING_KEY = "mcp.server_questions_default"
#: What a connector follows when neither it nor the admin chose.
FACTORY_DEFAULT = DECLINE
#: The ``threads.metadata`` key of a Session's own choices, by connector id.
SESSION_METADATA_KEY = "mcp_server_questions"

_STRING_FORMATS = frozenset({"email", "uri", "date", "date-time"})
_ANNOTATIONS = frozenset({"title", "description"})
_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DATE_TIME = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:\d{2})$"
)
_URI = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:\S+$")


def setting_or_none(value: Any) -> str | None:
    """A valid setting, or ``None`` for anything else (an absent value
    follows the default)."""
    return value if isinstance(value, str) and value in SETTINGS else None


def resolve_setting(own: Any, default: Any, session: Any = None) -> str:
    """The setting a connector runs with: the Session's ``accept``, else the
    connector's own value, else the installation default, else
    :data:`FACTORY_DEFAULT`."""
    if setting_or_none(session) == ACCEPT:
        return ACCEPT
    return setting_or_none(own) or setting_or_none(default) or FACTORY_DEFAULT


# -- the form a server may ask for ------------------------------------------


def _options(prop: Mapping[str, Any]) -> list[Any] | None:
    """A single-select property's allowed values, else ``None``."""
    if isinstance(prop.get("enum"), list):
        return list(prop["enum"])
    one_of = prop.get("oneOf")
    if isinstance(one_of, list) and all(
        isinstance(option, Mapping) and "const" in option for option in one_of
    ):
        return [option["const"] for option in one_of]
    return None


def _item_options(prop: Mapping[str, Any]) -> list[Any] | None:
    items = prop.get("items")
    if not isinstance(items, Mapping):
        return None
    if isinstance(items.get("enum"), list):
        return list(items["enum"])
    any_of = items.get("anyOf")
    if isinstance(any_of, list) and all(
        isinstance(option, Mapping) and "const" in option for option in any_of
    ):
        return [option["const"] for option in any_of]
    return None


def _property_problem(name: str, prop: Any) -> str | None:
    if not isinstance(prop, Mapping):
        return f"property {name!r} is not an object"
    kind = prop.get("type")
    if kind == "string":
        options = _options(prop)
        if options is not None:
            if not options or not all(isinstance(value, str) for value in options):
                return f"property {name!r} has an empty or non-text choice list"
            return None
        if "format" in prop and prop["format"] not in _STRING_FORMATS:
            return f"property {name!r} has an unsupported format"
        return None
    if kind in ("number", "integer", "boolean"):
        return None
    if kind == "array":
        options = _item_options(prop)
        if not options or not all(isinstance(value, str) for value in options):
            return f"property {name!r} is a list without a fixed set of choices"
        return None
    return f"property {name!r} has an unsupported type ({kind!r})"


def form_schema_problem(schema: Any) -> str | None:
    """Why a requested form is outside the specification's flat rules, or
    ``None`` when it can be shown. A form SRW cannot show is declined."""
    if not isinstance(schema, Mapping) or schema.get("type") != "object":
        return "the form is not an object"
    properties = schema.get("properties", {})
    if not isinstance(properties, Mapping):
        return "the form's properties are not an object"
    required = schema.get("required", [])
    if not isinstance(required, list) or any(
        not isinstance(name, str) or name not in properties for name in required
    ):
        return "the form requires a field it does not define"
    for name, prop in properties.items():
        problem = _property_problem(str(name), prop)
        if problem:
            return problem
    return None


def confirmation_content(schema: Any) -> dict[str, Any] | None:
    """What accepting a plain yes/no confirmation sends, or ``None`` when the
    form needs typed input and nobody can give it automatically.

    A confirmation is a form with no fields, a form whose every field has a
    default (sent as is), or a form with exactly one boolean field (sent as
    ``true``, the "yes"). fastmcp's ``ctx.elicit(message, bool)`` sends the
    last shape.
    """
    if form_schema_problem(schema) is not None:
        return None
    properties = dict(schema.get("properties") or {})
    if not properties:
        return {}
    if all("default" in prop for prop in properties.values()):
        return {name: prop["default"] for name, prop in properties.items()}
    if len(properties) == 1:
        ((name, prop),) = properties.items()
        if prop.get("type") == "boolean":
            return {name: True}
    return None


# -- an answer to it ---------------------------------------------------------


def _value_problem(name: str, prop: Mapping[str, Any], value: Any) -> str | None:
    kind = prop.get("type")
    if kind == "string":
        if not isinstance(value, str):
            return f"{name!r} must be text"
        options = _options(prop)
        if options is not None:
            return None if value in options else f"{name!r} is not one of the choices"
        if isinstance(prop.get("minLength"), int) and len(value) < prop["minLength"]:
            return f"{name!r} is shorter than {prop['minLength']} characters"
        if isinstance(prop.get("maxLength"), int) and len(value) > prop["maxLength"]:
            return f"{name!r} is longer than {prop['maxLength']} characters"
        fmt = prop.get("format")
        pattern = {
            "email": _EMAIL,
            "uri": _URI,
            "date": _DATE,
            "date-time": _DATE_TIME,
        }.get(fmt)
        if pattern is not None and not pattern.match(value):
            return f"{name!r} is not a valid {fmt}"
        return None
    if kind in ("number", "integer"):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return f"{name!r} must be a number"
        if kind == "integer" and not float(value).is_integer():
            return f"{name!r} must be a whole number"
        minimum, maximum = prop.get("minimum"), prop.get("maximum")
        if isinstance(minimum, (int, float)) and value < minimum:
            return f"{name!r} is below {minimum}"
        if isinstance(maximum, (int, float)) and value > maximum:
            return f"{name!r} is above {maximum}"
        return None
    if kind == "boolean":
        return None if isinstance(value, bool) else f"{name!r} must be true or false"
    if kind == "array":
        options = _item_options(prop) or []
        if not isinstance(value, list) or not all(item in options for item in value):
            return f"{name!r} must be a list of the choices"
        if isinstance(prop.get("minItems"), int) and len(value) < prop["minItems"]:
            return f"{name!r} needs at least {prop['minItems']} choices"
        if isinstance(prop.get("maxItems"), int) and len(value) > prop["maxItems"]:
            return f"{name!r} allows at most {prop['maxItems']} choices"
        return None
    return f"{name!r} has an unsupported type"


def content_problem(schema: Any, content: Any) -> str | None:
    """Why an answer does not fit the form, or ``None`` when it does."""
    problem = form_schema_problem(schema)
    if problem:
        return problem
    if not isinstance(content, Mapping):
        return "the answer is not an object"
    properties = dict(schema.get("properties") or {})
    for name in content:
        if name not in properties:
            return f"{name!r} is not a field of the form"
    for name in schema.get("required") or []:
        if name not in content:
            return f"{name!r} is required"
    for name, value in content.items():
        problem = _value_problem(str(name), properties[name], value)
        if problem:
            return problem
    return None


# -- the card a Session shows -------------------------------------------------

#: The keys of a question row's ``request`` the Cockpit card reads.
QUESTION_FIELDS = (
    "server",
    "server_slug",
    "tool",
    "mode",
    "message",
    "schema",
    "url",
    "confirmation",
    "field_order",
)


def question_card(tool_call_id: Any, approval_id: Any, request: Any) -> dict[str, Any]:
    """The ``question.request`` payload (and a ``pending_questions`` entry) for
    a question row: its tool call, its row id, and what the card shows."""
    request = request if isinstance(request, Mapping) else {}
    card: dict[str, Any] = {
        "id": str(tool_call_id or ""),
        "approval_id": str(approval_id),
    }
    for key in QUESTION_FIELDS:
        card[key] = request.get(key)
    return card


__all__ = [
    "ACCEPT",
    "DECLINE",
    "FACTORY_DEFAULT",
    "QUESTION_FIELDS",
    "SERVER_QUESTIONS_KEY",
    "SESSION_METADATA_KEY",
    "SETTINGS",
    "SYSTEM_SETTING_KEY",
    "confirmation_content",
    "content_problem",
    "form_schema_problem",
    "question_card",
    "resolve_setting",
    "setting_or_none",
]
