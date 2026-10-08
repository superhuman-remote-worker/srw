"""The driver protocol's messages as JSON Schemas, for driver authors (D6).

An image driver reads one request and writes typed JSON lines; it declares
its spec as JSON. These builders produce the JSON Schemas (2020-12) of those
three messages from the same vocabularies the stdlib validators use
(:mod:`.contract`, :mod:`.envelope`, :mod:`.registration`), and the files
shipped next to this module are their output, kept equal by a test:

* ``request.schema.json``: what ``/run/srw/request.json`` holds
  (:func:`~.envelope.validate_request`);
* ``output-line.schema.json``: one stdout line
  (:func:`~.envelope.parse_output_line`);
* ``spec.schema.json``: the ``io.srw.driver.spec`` label
  (:func:`~.registration.spec_from_json`; whether a spec is *valid* for a
  registration is :func:`~.registration.custom_driver_problems`);
* ``binding.schema.json`` (from :mod:`.binding`): what ``bind`` returns.

``scripts/srw-driver-test.py`` checks a driver's messages against them.

Design: knowledge-base/knowledge/features/connector_drivers.md, "Trust and
registration" (the author test kit).
"""

from __future__ import annotations

import json
from functools import lru_cache
from importlib.resources import files
from typing import Any

from .contract import (
    CREDENTIAL_DELIVERIES,
    CREDENTIAL_DELIVERY_MODES,
    CREDENTIAL_KINDS,
    CREDENTIAL_UPDATES,
    DELIVERY_FORMS,
    DRIVER_NAME_PATTERN,
    OPERATIONS,
    OPTIONAL_OPERATIONS,
    PLANES,
    SERVICE_CALLERS,
    SUPPORTED_PROTOCOL_MAJORS,
    WORKSPACE_BACKENDS,
)
from .envelope import BINDING_OPERATIONS, ERROR_CLASSES

_BASE = "https://srw.dev/schemas/connectors"
_DIALECT = "https://json-schema.org/draft/2020-12/schema"
#: ``MAJOR.MINOR`` of a protocol version SRW speaks.
_SUPPORTED_PROTOCOL = (
    r"^(?:"
    + "|".join(str(major) for major in sorted(SUPPORTED_PROTOCOL_MAJORS))
    + r")\.(0|[1-9][0-9]*)$"
)
_ANY_PROTOCOL = r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$"
_LOG_LEVELS = ("debug", "info", "warning", "error")
#: The file names the builders are shipped as.
SCHEMA_FILES: dict[str, str] = {
    "request": "request.schema.json",
    "output_line": "output-line.schema.json",
    "spec": "spec.schema.json",
}


def _strings() -> dict[str, Any]:
    return {"type": "array", "items": {"type": "string"}}


def request_schema() -> dict[str, Any]:
    """``/run/srw/request.json``."""
    return {
        "$schema": _DIALECT,
        "$id": f"{_BASE}/request/v1",
        "title": "SRW connector driver request",
        "type": "object",
        "required": ["protocol_version", "operation", "connector", "credentials"],
        "properties": {
            "protocol_version": {"type": "string", "pattern": _SUPPORTED_PROTOCOL},
            "operation": {"enum": list(OPERATIONS)},
            "binding_id": {"type": ["string", "null"]},
            "connector": {
                "type": "object",
                "required": ["config"],
                "properties": {
                    "config": {"type": "object"},
                    "access": {"type": ["string", "null"]},
                },
            },
            "credentials": {"type": "object"},
            "driver_state": {"type": "string"},
            "execution": {
                "type": "object",
                "required": ["kind", "id"],
                "properties": {
                    "kind": {"enum": ["job", "session"]},
                    "id": {"type": "string"},
                    "project_id": {"type": ["string", "null"]},
                    "workspace_backend": {"type": ["string", "null"]},
                },
            },
            "live_binding_ids": _strings(),
        },
        "allOf": [
            {
                "if": {
                    "properties": {"operation": {"enum": sorted(BINDING_OPERATIONS)}}
                },
                "then": {
                    "required": ["binding_id"],
                    "properties": {"binding_id": {"type": "string", "minLength": 1}},
                },
            },
            {
                "if": {"properties": {"operation": {"const": "gc"}}},
                "then": {"required": ["live_binding_ids"]},
            },
        ],
    }


def output_line_schema() -> dict[str, Any]:
    """One line a driver writes to stdout."""
    error = {
        "type": "object",
        "required": ["class", "message"],
        "properties": {
            "class": {"enum": list(ERROR_CLASSES)},
            "message": {"type": "string", "minLength": 1},
            "detail": {"type": ["string", "null"]},
            "field": {"type": ["string", "null"]},
            "retry_after_s": {"type": ["integer", "null"], "minimum": 0},
        },
    }
    return {
        "$schema": _DIALECT,
        "$id": f"{_BASE}/output-line/v1",
        "title": "SRW connector driver output line",
        "type": "object",
        "required": ["type"],
        "oneOf": [
            {
                "properties": {
                    "type": {"const": "result"},
                    "result": {"type": "object"},
                    "driver_state": {"type": ["string", "null"]},
                },
                "required": ["result"],
            },
            {
                "properties": {
                    "type": {"const": "log"},
                    "level": {"enum": list(_LOG_LEVELS)},
                    "message": {"type": "string"},
                },
                "required": ["level", "message"],
            },
            {
                "properties": {"type": {"const": "error"}, "error": error},
                "required": ["error"],
            },
            {
                "properties": {
                    "type": {"const": "update"},
                    "target": {"const": "credential"},
                    "slot": {"type": "string"},
                },
                "required": ["target", "slot", "value"],
            },
            {
                "properties": {
                    "type": {"const": "update"},
                    "target": {"const": "config"},
                    "value": {"type": "object"},
                },
                "required": ["target", "value"],
            },
        ],
    }


def spec_schema() -> dict[str, Any]:
    """The ``io.srw.driver.spec`` label's JSON."""
    level = {
        "type": "object",
        "additionalProperties": False,
        "required": ["id", "rank", "enforced_by"],
        "properties": {
            "id": {"type": "string", "minLength": 1},
            "rank": {"type": "integer"},
            "enforced_by": {"type": "string", "minLength": 1},
            "tools": {"oneOf": [{"const": "*"}, _strings()]},
            "advisory": {"type": "boolean"},
        },
    }
    slot = {
        "type": "object",
        "additionalProperties": False,
        "required": ["name", "kind"],
        "properties": {
            "name": {"type": "string", "minLength": 1},
            "kind": {"enum": list(CREDENTIAL_KINDS)},
            "schema": {"type": "object"},
            "required": {"type": "boolean"},
            "rotatable": {"type": "boolean"},
            "access_levels": _strings(),
            "delivery": {"enum": [*CREDENTIAL_DELIVERIES, None]},
            "update": {"enum": list(CREDENTIAL_UPDATES)},
        },
    }
    egress = {
        "type": "object",
        "additionalProperties": False,
        "required": ["host", "ports"],
        "properties": {
            "host": {"type": "string", "minLength": 1},
            "ports": {
                "type": "array",
                "minItems": 1,
                "items": {
                    "oneOf": [
                        {"type": "integer", "minimum": 1, "maximum": 65535},
                        {
                            "type": "string",
                            "pattern": r"^\$\{config\.[a-z][a-z0-9_]*\}$",
                        },
                    ]
                },
            },
            "protocol": {"enum": ["tcp", "udp"]},
        },
    }
    service = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "instancing": {"enum": ["shared", "per_execution"]},
            "resources": {"type": "object"},
            "start_seconds": {"type": "integer", "minimum": 1},
            "mcp": {"type": ["object", "null"]},
            "port": {"type": "integer", "minimum": 1, "maximum": 65535},
            "callers": {
                "type": "array",
                "minItems": 1,
                "items": {"enum": list(SERVICE_CALLERS)},
            },
        },
    }
    return {
        "$schema": _DIALECT,
        "$id": f"{_BASE}/spec/v1",
        "title": "SRW connector driver spec (the io.srw.driver.spec label)",
        "type": "object",
        "additionalProperties": False,
        "required": ["name", "title", "protocol_version", "plane", "delivery_forms"],
        "properties": {
            "name": {
                "type": "string",
                "pattern": DRIVER_NAME_PATTERN,
                "not": {"pattern": r"^srw\."},
            },
            "title": {"type": "string", "minLength": 1},
            "protocol_version": {"type": "string", "pattern": _ANY_PROTOCOL},
            "plane": {"enum": [plane for plane in PLANES if plane != "harness"]},
            "delivery_forms": {
                "type": "array",
                "minItems": 1,
                "items": {"enum": list(DELIVERY_FORMS)},
            },
            "config_schema": {"type": "object"},
            "credential_slots": {"type": "array", "items": slot},
            "access_levels": {"type": "array", "items": level},
            "default_access": {"type": ["string", "null"]},
            "supported_backends": {
                "type": "array",
                "items": {"enum": sorted(WORKSPACE_BACKENDS)},
            },
            "workspace_requirements": {"type": "string"},
            "operations": {
                "type": "array",
                "items": {"enum": sorted(OPTIONAL_OPERATIONS)},
            },
            "egress": {"type": "array", "items": egress},
            "needs_dns": {"type": ["string", "null"]},
            "holds_upstream_credentials": {"type": "boolean"},
            "credential_delivery": {"enum": list(CREDENTIAL_DELIVERY_MODES)},
            "tool_category": {"type": ["string", "null"]},
            "service": service,
        },
    }


BUILDERS = {
    "request": request_schema,
    "output_line": output_line_schema,
    "spec": spec_schema,
}


@lru_cache(maxsize=None)
def _text(name: str) -> str:
    return files(__package__).joinpath(SCHEMA_FILES[name]).read_text()


def load_message_schema(name: str) -> dict[str, Any]:
    """A shipped message schema (``request``, ``output_line`` or ``spec``)."""
    return json.loads(_text(name))


__all__ = [
    "BUILDERS",
    "SCHEMA_FILES",
    "load_message_schema",
    "output_line_schema",
    "request_schema",
    "spec_schema",
]
