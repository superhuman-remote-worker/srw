"""The binding descriptor: what one ``bind`` delivers, as data.

A descriptor names its driver and carries entries.  Each entry says who
receives it (``recipient``), in which delivery ``form`` with a ``value`` of
that form's shape, and what happens when two entries collide, when the
execution's backend changes (``refresh``) and when the binding ends
(``retire``).

Built-in and image drivers are checked against one set of rules, kept in
:data:`VALUE_FIELDS` and the top-level tables below.  Both checks are built
from them: :func:`binding_schema` produces the JSON Schema shipped next to
this module as ``binding.schema.json`` (for image driver authors), and
:func:`validate_binding` is the stdlib check of the same rules.  A test keeps
the shipped file equal to the builder and runs every case through both.

D1 keeps the agent payload byte-identical: the agent builds descriptors from
today's payload entries, and a ``binding`` key on the wire waits for D5.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from functools import lru_cache
from importlib.resources import files
from typing import Any, Literal, Mapping, get_args

from .contract import (
    DELIVERY_FORMS,
    DRIVER_NAME_PATTERN,
    DeliveryForm,
    validate_driver_name,
)

Recipient = Literal["harness", "agent_pod", "workspace", "harness_pod"]
Collision = Literal["error", "skip_existing", "suffix", "last_wins"]
Refresh = Literal["none", "renew", "on_backend_swap"]
Retire = Literal["none", "remove", "close", "drop_registration"]
RECIPIENTS: tuple[str, ...] = get_args(Recipient)
COLLISIONS: tuple[str, ...] = get_args(Collision)
REFRESHES: tuple[str, ...] = get_args(Refresh)
RETIRES: tuple[str, ...] = get_args(Retire)

#: Value types, as JSON Schema has them.  ``integer`` is any whole number
#: (``384.0`` included, as JSON Schema counts it) and never a boolean.
ValueType = Literal[
    "string",
    "string_or_null",
    "integer",
    "boolean",
    "object",
    "string_list",
    "string_map",
]

#: Per form: field -> (type, required, secret).  Secrets are ``writeOnly``
#: in the schema and never shown in a descriptor's repr.
VALUE_FIELDS: dict[str, dict[str, tuple[ValueType, bool, bool]]] = {
    "env_file": {"name": ("string", True, False), "value": ("string", True, True)},
    "credential_file": {
        "path": ("string", True, False),
        "content": ("string", True, True),
        "mode": ("integer", False, False),
        "env_var": ("string_or_null", False, False),
        "transform": ("string_or_null", False, False),
        "merge_group": ("string_or_null", False, False),
    },
    "checkout": {
        "url": ("string", True, False),
        "name_hint": ("string", True, False),
        "auth": ("string", True, False),
        "secret": ("string_or_null", False, True),
        "default_branch": ("string_or_null", False, False),
        "require_default_branch": ("boolean", False, False),
        "forge": ("string_or_null", False, False),
        "datasource_id": ("string_or_null", False, False),
        "read_only": ("boolean", True, False),
    },
    "managed_connection": {
        "kind": ("string", True, False),
        "url": ("string_or_null", True, False),
        "credentials": ("object", True, True),
        "config": ("object", True, False),
        "read_only": ("boolean", True, False),
    },
    "mcp_client": {
        "transport": ("string", True, False),
        "url": ("string_or_null", False, False),
        "headers": ("string_map", False, True),
        "command": ("string_or_null", False, False),
        "args": ("string_list", False, False),
        "env": ("string_map", False, True),
    },
    "knowledge_index": {
        "datasource_id": ("string", True, False),
        "config": ("object", True, False),
        "native_project_id": ("string_or_null", False, False),
    },
    "pod_env": {"name": ("string", True, False), "value": ("string", True, True)},
    "pod_file": {"path": ("string", True, False), "content": ("string", True, True)},
}
#: Closed value enums inside a form (``null`` stays allowed where the type
#: allows it).  A checkout's ``auth`` names how the credential reaches git:
#: a token in the clone URL, or a key loaded into the workspace's ssh-agent
#: (slice C1).  The git swap driver (C3) will add its own value.
VALUE_ENUMS: dict[tuple[str, str], tuple[str, ...]] = {
    ("checkout", "auth"): ("none", "token_in_url", "ssh_agent"),
    ("credential_file", "transform"): ("kubeconfig_prefix",),
    ("mcp_client", "transport"): ("http", "sse", "stdio"),
}
#: Entry keys: key -> (allowed values, required).
ENTRY_FIELDS: dict[str, tuple[tuple[str, ...], bool]] = {
    "recipient": (RECIPIENTS, True),
    "collision": (COLLISIONS, True),
    "refresh": (REFRESHES, False),
    "retire": (RETIRES, False),
}
#: Descriptor keys besides ``driver``, ``name`` and ``entries``: all strings
#: or null; ``access`` must be present (null for a driver without levels).
DESCRIPTOR_TEXT_FIELDS: dict[str, bool] = {
    "access": True,
    "connector_id": False,
    "description": False,
    "cli_hint": False,
}


@dataclass(frozen=True, slots=True)
class BindingEntry:
    recipient: Recipient
    form: DeliveryForm
    value: Mapping[str, Any] = field(repr=False)
    collision: Collision = "error"
    refresh: Refresh = "none"
    retire: Retire = "none"

    def to_json(self) -> dict[str, Any]:
        return {
            "recipient": self.recipient,
            "form": self.form,
            "value": dict(self.value),
            "collision": self.collision,
            "refresh": self.refresh,
            "retire": self.retire,
        }


@dataclass(frozen=True, slots=True)
class BindingDescriptor:
    """Everything one binding of one connector delivers.

    ``name``, ``description`` and ``cli_hint`` are what the agent shows about
    the connector; ``access`` is the effective access level id (``None`` for
    a driver without levels).
    """

    driver: str
    name: str
    entries: tuple[BindingEntry, ...]
    access: str | None = None
    connector_id: str | None = None
    description: str | None = None
    cli_hint: str | None = None

    def to_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "driver": self.driver,
            "name": self.name,
            "access": self.access,
            "entries": [entry.to_json() for entry in self.entries],
        }
        for key in ("connector_id", "description", "cli_hint"):
            if getattr(self, key) is not None:
                out[key] = getattr(self, key)
        return out


# =============================================================================
# The JSON Schema, built from the tables
# =============================================================================

_SCHEMA_TYPES: dict[str, dict[str, Any]] = {
    "string": {"type": "string"},
    "string_or_null": {"type": ["string", "null"]},
    "integer": {"type": "integer"},
    "boolean": {"type": "boolean"},
    "object": {"type": "object"},
    "string_list": {"type": "array", "items": {"type": "string"}},
    "string_map": {"type": "object", "additionalProperties": {"type": "string"}},
}


def _value_schema(form: str) -> dict[str, Any]:
    properties: dict[str, Any] = {}
    for name, (value_type, _required, secret) in VALUE_FIELDS[form].items():
        prop = dict(_SCHEMA_TYPES[value_type])
        allowed = VALUE_ENUMS.get((form, name))
        if allowed:
            prop["enum"] = list(allowed) + (
                [None] if value_type == "string_or_null" else []
            )
        if secret:
            prop["writeOnly"] = True
        properties[name] = prop
    return {
        "type": "object",
        "properties": properties,
        "required": [
            n for n, (_, required, _s) in VALUE_FIELDS[form].items() if required
        ],
        "additionalProperties": False,
    }


def binding_schema() -> dict[str, Any]:
    """The descriptor's JSON Schema (2020-12), built from the tables."""
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://srw.dev/schemas/connectors/binding/v1",
        "title": "SRW connector binding descriptor",
        "description": (
            "What one bind of one connector delivers. Built from "
            "shared.connectors.binding; validate_binding() checks the same "
            "rules without a schema library."
        ),
        "type": "object",
        "required": ["driver", "name", "entries"]
        + [key for key, required in DESCRIPTOR_TEXT_FIELDS.items() if required],
        "additionalProperties": False,
        "properties": {
            # Python's re (which jsonschema uses) lets "$" match before a
            # trailing newline; the "not" keeps that out as fullmatch does.
            "driver": {
                "type": "string",
                "pattern": DRIVER_NAME_PATTERN,
                "not": {"pattern": r"\s"},
            },
            "name": {"type": "string", "minLength": 1},
            **{key: {"type": ["string", "null"]} for key in DESCRIPTOR_TEXT_FIELDS},
            "entries": {"type": "array", "items": {"$ref": "#/$defs/entry"}},
        },
        "$defs": {
            "entry": {
                "type": "object",
                "required": ["form", "value"]
                + [key for key, (_, required) in ENTRY_FIELDS.items() if required],
                "additionalProperties": False,
                "properties": {
                    "form": {"enum": list(VALUE_FIELDS)},
                    "value": {"type": "object"},
                    **{
                        key: {"enum": list(allowed)}
                        for key, (allowed, _) in ENTRY_FIELDS.items()
                    },
                },
                "oneOf": [
                    {
                        "properties": {
                            "form": {"const": form},
                            "value": _value_schema(form),
                        }
                    }
                    for form in VALUE_FIELDS
                ],
            }
        },
    }


@lru_cache(maxsize=1)
def _schema_text() -> str:
    return files(__package__).joinpath("binding.schema.json").read_text()


def load_binding_schema() -> dict[str, Any]:
    """The shipped schema file (equal to :func:`binding_schema`)."""
    return json.loads(_schema_text())


# =============================================================================
# The stdlib validator, from the same tables
# =============================================================================


def _type_ok(value: Any, value_type: str) -> bool:
    if value_type == "string":
        return isinstance(value, str)
    if value_type == "string_or_null":
        return value is None or isinstance(value, str)
    if value_type == "integer":
        if isinstance(value, bool):
            return False
        return isinstance(value, int) or (
            isinstance(value, float) and value.is_integer()
        )
    if value_type == "boolean":
        return isinstance(value, bool)
    if value_type == "object":
        return isinstance(value, Mapping)
    if value_type == "string_list":
        return isinstance(value, list) and all(isinstance(v, str) for v in value)
    if value_type == "string_map":
        return isinstance(value, Mapping) and all(
            isinstance(v, str) for v in value.values()
        )
    raise ValueError(f"unknown value type {value_type!r}")


def validate_entry(entry: Mapping[str, Any], *, at: str = "entry") -> list[str]:
    if not isinstance(entry, Mapping):
        return [f"{at} must be an object"]
    problems: list[str] = []
    unknown = sorted(set(entry) - {"form", "value", *ENTRY_FIELDS})
    if unknown:
        problems.append(f"{at} has unknown fields {unknown}")
    for key, (allowed, required) in ENTRY_FIELDS.items():
        if (key in entry or required) and entry.get(key) not in allowed:
            problems.append(f"{at}.{key} {entry.get(key)!r} is not one of {allowed}")
    form = entry.get("form")
    if form not in DELIVERY_FORMS or form not in VALUE_FIELDS:
        return problems + [f"{at}.form {form!r} is not one of {tuple(VALUE_FIELDS)}"]
    value = entry.get("value")
    if not isinstance(value, Mapping):
        return problems + [f"{at}.value must be an object"]
    fields = VALUE_FIELDS[form]
    for name in sorted(set(value) - set(fields)):
        problems.append(f"{at}.value has unknown field {name!r} for {form}")
    for name, (value_type, required, _secret) in fields.items():
        if name not in value:
            if required:
                problems.append(f"{at}.value.{name} is required for {form}")
            continue
        if not _type_ok(value[name], value_type):
            problems.append(f"{at}.value.{name} must be {value_type}")
            continue
        allowed = VALUE_ENUMS.get((form, name))
        if allowed and value[name] is not None and value[name] not in allowed:
            problems.append(
                f"{at}.value.{name} {value[name]!r} is not one of {allowed}"
            )
    return problems


def validate_binding(descriptor: Mapping[str, Any]) -> list[str]:
    """Every problem with a descriptor object (empty when valid)."""
    if not isinstance(descriptor, Mapping):
        return ["binding must be an object"]
    problems: list[str] = []
    unknown = sorted(
        set(descriptor) - {"driver", "name", "entries", *DESCRIPTOR_TEXT_FIELDS}
    )
    if unknown:
        problems.append(f"binding has unknown fields {unknown}")
    problems += validate_driver_name(descriptor.get("driver"))
    if not isinstance(descriptor.get("name"), str) or not descriptor.get("name"):
        problems.append("binding.name is required")
    for key, required in DESCRIPTOR_TEXT_FIELDS.items():
        if key not in descriptor:
            if required:
                problems.append(f"binding.{key} is required (null when none)")
        elif descriptor[key] is not None and not isinstance(descriptor[key], str):
            problems.append(f"binding.{key} must be a string or null")
    entries = descriptor.get("entries")
    if not isinstance(entries, list):
        return problems + ["binding.entries must be a list"]
    for index, entry in enumerate(entries):
        problems += validate_entry(entry, at=f"entries[{index}]")
    return problems
