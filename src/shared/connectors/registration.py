"""Registered image drivers: the spec as JSON, names, trust and bind output (D6).

A driver image declares its :class:`~.contract.DriverSpec` as JSON, in the
``io.srw.driver.spec`` label (or as the result of its ``spec`` operation).
:func:`spec_from_json` reads that JSON into a spec and :func:`spec_to_json`
writes one back; :func:`custom_driver_problems` holds the rules a driver
someone registers must follow on top of :func:`~.contract.validate_spec`:

* **Names.** ``srw.`` is SRW's own namespace: no registration may use it.
* **Planes.** A registered image runs in its own pod, never in SRW's process
  (``harness``). The in-pod plane needs privilege: a repository the operator
  trusts (``connectors.drivers.trustedRepositories``, matched on a path
  boundary by :func:`repository_trusted`) or the operator's switch
  (``connectors.customDrivers.privileged``). A bind-time driver returns data
  only, in the forms :data:`IMAGE_BIND_FORMS` names, which SRW's own
  materializers deliver to the workspace. A service driver is a managed MCP
  server (imported from its ``server.json``).
* **Environment names.** A bind-time driver that sets variables declares
  every name its bind may return (``env_names`` in its spec), so the names
  are visible when it is registered and on its connector, and none may be
  on the list of known tool hooks (``env_names.driver_env_problem``: names
  tools read to run code, redirect traffic or loosen TLS). The list is a
  best-effort lint, not a sandbox: the image's author is the trust
  boundary, as a workspace image's is.
* **Schemas.** A registered schema is validated on SRW's event loop, so it
  may not hold a regular expression (``pattern``, ``patternProperties``,
  ``format: regex``) or a reference outside itself, and it is bounded in
  size and depth (:func:`schema_problems`). A driver checks a pattern in its
  own ``check``.

What one ``bind`` of a bind-time image returns is checked by
:func:`image_binding_problems` (the one check SRW and the author test kit
both run) and turned into the wire entry the agent already reads for its
stored type (:func:`wire_credentials`). A file goes to ``~/.srw-files/``,
``~/.netrc`` or ``~/.pgpass`` only (:data:`IMAGE_FILE_TARGETS`): the other
credential-file locations hold formats that run a command (a kubeconfig's
``exec``, an AWS ``credential_process``), which a shared driver's output
must not carry until the owner rules on it. A moved tag's new spec is
compared with the one bound before by :func:`moved_spec_problems`.

Design: knowledge-base/knowledge/features/connector_drivers.md, "Trust and
registration", "The driver contract" and slice D6.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from typing import Any

from .binding import validate_binding
from .contract import (
    AccessLevel,
    CredentialSlot,
    DriverSpec,
    EgressRule,
    ServiceSpec,
    managed_mcp_driver,
    validate_spec,
)
from .env_names import (
    ENV_NAME,
    MAX_DRIVER_ENV_NAME,
    driver_env_problem,
    env_value_problem,
)
from .file_targets import STORED_HOME, mode_problem, target_problem
from .images import SPEC_LABEL, ImageReference, SpecContract, compatibility_problems

#: The namespace of SRW's own drivers; no registration may use it.
RESERVED_NAMESPACE = "srw"
#: What a bind-time image driver may return: data SRW's materializers
#: deliver to the workspace over SRW's own channel (no driver gets a shell).
IMAGE_BIND_FORMS: tuple[str, ...] = ("env_file", "credential_file")
#: The workspace backends a bind-time image driver can deliver to.
SHELL_BACKENDS: frozenset[str] = frozenset({"sandbox", "vm"})
#: Keys a spec's JSON may carry; anything else is refused, never ignored.
SPEC_KEYS: frozenset[str] = frozenset(
    {
        "name",
        "title",
        "protocol_version",
        "plane",
        "delivery_forms",
        "config_schema",
        "credential_slots",
        "access_levels",
        "default_access",
        "supported_backends",
        "workspace_requirements",
        "operations",
        "egress",
        "needs_dns",
        "holds_upstream_credentials",
        "credential_delivery",
        "tool_category",
        "service",
        "env_names",
    }
)
_SLOT_KEYS = frozenset(
    {
        "name",
        "kind",
        "schema",
        "required",
        "rotatable",
        "access_levels",
        "delivery",
        "update",
    }
)
_LEVEL_KEYS = frozenset({"id", "rank", "enforced_by", "tools", "advisory"})
_EGRESS_KEYS = frozenset({"host", "ports", "protocol"})
_SERVICE_KEYS = frozenset(
    {"instancing", "resources", "start_seconds", "mcp", "port", "callers"}
)
#: The most entries one bind may deliver, and names a spec may declare.
MAX_BINDING_ENTRIES = 100
MAX_ENV_NAMES = 100
#: Where a bind-time image driver's file may land (home-relative): a
#: directory SRW owns, and the two login files curl, git and libpq read.
IMAGE_FILE_DIRECTORY = ".srw-files"
IMAGE_FILE_NAMES: tuple[str, ...] = (".netrc", ".pgpass")
IMAGE_FILE_TARGETS = f"~/{IMAGE_FILE_DIRECTORY}/, ~/.netrc or ~/.pgpass"
#: Bounds on a registered schema (its canonical JSON, and its nesting).
MAX_SCHEMA_BYTES = 32 * 1024
MAX_SCHEMA_DEPTH = 12
#: The most schema nodes one validation may apply, every ``$ref`` followed
#: (each reference counts its target again), and how deep they may nest
#: so: what one instance value costs at most. With
#: :data:`MAX_INSTANCE_NODES` one validation stays under about a second
#: (measured: 243 failing ``anyOf`` branches against 510 values, 0.8 s).
MAX_SCHEMA_NODES = 256
MAX_EXPANDED_DEPTH = 32
#: The widest ``allOf``, ``anyOf``, ``oneOf`` or ``prefixItems``.
MAX_SCHEMA_WIDTH = 64
#: Bounds on what is validated against a registered schema: a stored
#: config's canonical JSON, and any value's JSON nodes. With
#: :data:`MAX_SCHEMA_NODES` they bound one validation's work.
MAX_CONFIG_BYTES = 64 * 1024
MAX_INSTANCE_NODES = 512
#: How long SRW waits for one validation, which runs off its event loop.
VALIDATION_SECONDS = 5.0


def reserved_name(name: Any) -> bool:
    """Whether ``name`` is in SRW's own driver namespace."""
    return isinstance(name, str) and name.split(".", 1)[0] == RESERVED_NAMESPACE


# =============================================================================
# The spec as JSON
# =============================================================================


def _unknown(where: str, value: Mapping[str, Any], allowed: frozenset[str]) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ValueError(f"{where} has unknown keys {unknown}")


def _object(value: Any, where: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{where} must be an object")
    return value


def _strings(value: Any, where: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise ValueError(f"{where} must be a list of strings")
    return tuple(value)


def _text(value: Any, where: str, *, required: bool = True) -> str | None:
    if value is None and not required:
        return None
    if not isinstance(value, str) or (required and not value):
        raise ValueError(f"{where} must be a non-empty string")
    return value


def _flag(value: Any, where: str, default: bool) -> bool:
    if value is None:
        return default
    if not isinstance(value, bool):
        raise ValueError(f"{where} must be true or false")
    return value


def _level(value: Any, index: int) -> AccessLevel:
    where = f"access_levels[{index}]"
    level = _object(value, where)
    _unknown(where, level, _LEVEL_KEYS)
    rank = level.get("rank")
    if isinstance(rank, bool) or not isinstance(rank, int):
        raise ValueError(f"{where}.rank must be an integer")
    tools = level.get("tools", [])
    return AccessLevel(
        id=_text(level.get("id"), f"{where}.id") or "",
        rank=rank,
        enforced_by=_text(level.get("enforced_by"), f"{where}.enforced_by") or "",
        tools="*" if tools == "*" else _strings(tools, f"{where}.tools"),
        advisory=_flag(level.get("advisory"), f"{where}.advisory", False),
    )


def _slot(value: Any, index: int) -> CredentialSlot:
    where = f"credential_slots[{index}]"
    slot = _object(value, where)
    _unknown(where, slot, _SLOT_KEYS)
    return CredentialSlot(
        name=_text(slot.get("name"), f"{where}.name") or "",
        kind=_text(slot.get("kind"), f"{where}.kind"),
        schema=_object(slot.get("schema", {"type": "object"}), f"{where}.schema"),
        required=_flag(slot.get("required"), f"{where}.required", False),
        rotatable=_flag(slot.get("rotatable"), f"{where}.rotatable", False),
        access_levels=_strings(slot.get("access_levels"), f"{where}.access_levels"),
        delivery=_text(slot.get("delivery"), f"{where}.delivery", required=False),
        update=_text(slot.get("update", "replace"), f"{where}.update"),
    )


def _egress(value: Any, index: int) -> EgressRule:
    where = f"egress[{index}]"
    rule = _object(value, where)
    _unknown(where, rule, _EGRESS_KEYS)
    ports = rule.get("ports")
    if not isinstance(ports, list) or not all(
        isinstance(port, str) or (isinstance(port, int) and not isinstance(port, bool))
        for port in ports
    ):
        raise ValueError(f"{where}.ports must be a list of ports")
    return EgressRule(
        host=_text(rule.get("host"), f"{where}.host") or "",
        ports=tuple(ports),
        protocol=_text(rule.get("protocol", "tcp"), f"{where}.protocol"),
    )


def _service(value: Any) -> ServiceSpec | None:
    if value is None:
        return None
    service = _object(value, "service")
    _unknown("service", service, _SERVICE_KEYS)
    start = service.get("start_seconds", 30)
    port = service.get("port", 8080)
    for key, number in (("start_seconds", start), ("port", port)):
        if isinstance(number, bool) or not isinstance(number, int):
            raise ValueError(f"service.{key} must be an integer")
    mcp = service.get("mcp")
    return ServiceSpec(
        instancing=_text(service.get("instancing", "shared"), "service.instancing"),
        resources=_object(service.get("resources", {}), "service.resources"),
        start_seconds=start,
        mcp=_object(mcp, "service.mcp") if mcp is not None else None,
        port=port,
        callers=_strings(service.get("callers", ["harness"]), "service.callers"),
    )


def spec_from_json(value: Any) -> DriverSpec:
    """The :class:`DriverSpec` a driver image declares as JSON.

    Raises ``ValueError`` for a shape SRW cannot read (an unknown key, a
    value of the wrong type). Whether the spec is *valid* is
    :func:`custom_driver_problems`' question.
    """
    spec = _object(value, "the spec")
    _unknown("the spec", spec, SPEC_KEYS)
    levels = spec.get("access_levels", [])
    slots = spec.get("credential_slots", [])
    egress = spec.get("egress", [])
    for key, items in (
        ("access_levels", levels),
        ("credential_slots", slots),
        ("egress", egress),
    ):
        if not isinstance(items, list):
            raise ValueError(f"{key} must be a list")
    backends = _strings(spec.get("supported_backends"), "supported_backends")
    declared_env_names(spec)
    return DriverSpec(
        name=_text(spec.get("name"), "name") or "",
        title=_text(spec.get("title"), "title") or "",
        protocol_version=_text(spec.get("protocol_version"), "protocol_version") or "",
        plane=_text(spec.get("plane"), "plane"),
        delivery_forms=_strings(spec.get("delivery_forms"), "delivery_forms"),
        config_schema=_object(
            spec.get("config_schema", {"type": "object"}), "config_schema"
        ),
        access_levels=tuple(_level(item, i) for i, item in enumerate(levels)),
        supported_backends=frozenset(backends),
        workspace_requirements=_text(
            spec.get("workspace_requirements") or "",
            "workspace_requirements",
            required=False,
        )
        or "",
        credential_slots=tuple(_slot(item, i) for i, item in enumerate(slots)),
        default_access=_text(
            spec.get("default_access"), "default_access", required=False
        ),
        operations=_strings(spec.get("operations"), "operations"),
        egress=tuple(_egress(item, i) for i, item in enumerate(egress)),
        needs_dns=_text(spec.get("needs_dns"), "needs_dns", required=False),
        holds_upstream_credentials=_flag(
            spec.get("holds_upstream_credentials"), "holds_upstream_credentials", True
        ),
        credential_delivery=_text(
            spec.get("credential_delivery", "inline"), "credential_delivery"
        ),
        tool_category=_text(spec.get("tool_category"), "tool_category", required=False),
        service=_service(spec.get("service")),
    )


def declared_env_names(value: Mapping[str, Any]) -> tuple[str, ...]:
    """The environment names a spec's JSON declares its bind may return.

    ``ValueError`` when ``env_names`` is not a list of distinct variable
    names, or holds more than :data:`MAX_ENV_NAMES`. Whether a driver may set
    each is :func:`custom_driver_problems`' question.
    """
    names = _strings(value.get("env_names"), "env_names")
    if len(names) > MAX_ENV_NAMES:
        raise ValueError(f"env_names holds at most {MAX_ENV_NAMES} names")
    if len(set(names)) != len(names):
        raise ValueError("env_names lists a name twice")
    for name in names:
        if not ENV_NAME.fullmatch(name) or len(name) > MAX_DRIVER_ENV_NAME:
            raise ValueError(
                f"env_names: {name[:40]!r} is not a variable name of at most "
                f"{MAX_DRIVER_ENV_NAME} characters"
            )
    return names


def spec_to_json(spec: DriverSpec, *, env_names: Iterable[str] = ()) -> dict[str, Any]:
    """``spec`` as the JSON a driver image declares (round-trips through
    :func:`spec_from_json` for every key in :data:`SPEC_KEYS`)."""
    out: dict[str, Any] = {
        "name": spec.name,
        "title": spec.title,
        "protocol_version": spec.protocol_version,
        "plane": spec.plane,
        "delivery_forms": list(spec.delivery_forms),
        "config_schema": _plain(spec.config_schema),
        "credential_slots": [
            {
                "name": slot.name,
                "kind": slot.kind,
                "schema": _plain(slot.schema),
                "required": slot.required,
                "rotatable": slot.rotatable,
                "access_levels": list(slot.access_levels),
                "delivery": slot.delivery,
                "update": slot.update,
            }
            for slot in spec.credential_slots
        ],
        "access_levels": [
            {
                "id": level.id,
                "rank": level.rank,
                "enforced_by": level.enforced_by,
                "tools": level.tools if level.tools == "*" else list(level.tools),
                "advisory": level.advisory,
            }
            for level in spec.access_levels
        ],
        "default_access": spec.default_access,
        "supported_backends": sorted(spec.supported_backends),
        "workspace_requirements": spec.workspace_requirements,
        "operations": list(spec.operations),
        "egress": [
            {"host": rule.host, "ports": list(rule.ports), "protocol": rule.protocol}
            for rule in spec.egress
        ],
        "needs_dns": spec.needs_dns,
        "holds_upstream_credentials": spec.holds_upstream_credentials,
        "credential_delivery": spec.credential_delivery,
        "tool_category": spec.tool_category,
        "env_names": list(env_names),
    }
    if spec.service is not None:
        out["service"] = {
            "instancing": spec.service.instancing,
            "resources": _plain(spec.service.resources),
            "start_seconds": spec.service.start_seconds,
            "mcp": _plain(spec.service.mcp) if spec.service.mcp is not None else None,
            "port": spec.service.port,
            "callers": list(spec.service.callers),
        }
    return out


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


def canonical_spec(value: Mapping[str, Any]) -> str:
    """A spec's JSON in one canonical spelling (what its hash is taken of)."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


# =============================================================================
# The rules for a registered driver
# =============================================================================


_SUBSCHEMA_MAPS = frozenset(
    {"properties", "patternProperties", "$defs", "definitions", "dependentSchemas"}
)
_SUBSCHEMA_LISTS = frozenset({"allOf", "anyOf", "oneOf", "prefixItems"})
_SUBSCHEMAS = frozenset(
    {
        "items",
        "additionalItems",
        "additionalProperties",
        "unevaluatedItems",
        "unevaluatedProperties",
        "propertyNames",
        "contains",
        "contentSchema",
        "not",
        "if",
        "then",
        "else",
    }
)
#: Keywords that name a schema resource or an anchor a ``$ref`` could reach
#: from anywhere in the document, or a reference resolved at run time:
#: a registered schema uses none, so every reference is one SRW can read.
_RESOLUTION_KEYWORDS = (
    "$id",
    "$anchor",
    "$dynamicAnchor",
    "$dynamicRef",
    "$recursiveAnchor",
    "$recursiveRef",
    "$vocabulary",
)
#: The one dialect a registered schema is validated as.
_DIALECT = "https://json-schema.org/draft/2020-12/schema"
#: The only references a registered schema may hold: into its own ``$defs``
#: or ``definitions``, by a plain name.
_LOCAL_REF = re.compile(r"#/(\$defs|definitions)/([A-Za-z0-9_][A-Za-z0-9_.-]{0,127})\Z")
#: Keywords whose cost grows faster than the document it validates: SRW
#: refuses them in a registered schema (a driver checks them in ``check``).
_COSTLY_KEYWORDS = ("unevaluatedProperties", "unevaluatedItems")
#: How deeply JSON containers (a default's, an enum's) may nest at all.
_MAX_CONTAINER_DEPTH = 64


def _regex_problems(schema: Mapping[str, Any], where: str) -> list[str]:
    """Every mapping anywhere in ``schema`` that holds a regular expression
    or a resolution keyword, whatever key it sits under: a value no keyword
    names is never reached by a validator, but a ``$ref`` could make it one,
    so nothing is left to the structure (belt and braces)."""
    problems: list[str] = []
    stack: list[tuple[Any, str, int]] = [(schema, where, 0)]
    while stack:
        node, path, depth = stack.pop()
        if depth > _MAX_CONTAINER_DEPTH:
            problems.append(f"{where} nests JSON deeper than {_MAX_CONTAINER_DEPTH}")
            break
        if isinstance(node, list):
            for index, child in enumerate(node):
                if isinstance(child, (Mapping, list)):
                    stack.append((child, f"{path}[{index}]", depth + 1))
            continue
        if not isinstance(node, Mapping):
            continue
        if isinstance(node.get("pattern"), str):
            problems.append(
                f"{path} uses pattern: a registered schema runs no regular "
                "expression (check it in the driver's check)"
            )
        if isinstance(node.get("patternProperties"), Mapping):
            problems.append(
                f"{path} uses patternProperties: a registered schema runs no "
                "regular expression (check it in the driver's check)"
            )
        if node.get("format") == "regex":
            problems.append(f"{path} uses format regex")
        for keyword in _RESOLUTION_KEYWORDS:
            if keyword in node:
                problems.append(
                    f"{path} uses {keyword}: a registered schema refers only to "
                    "its own $defs"
                )
        for key, child in node.items():
            if isinstance(child, (Mapping, list)):
                stack.append((child, f"{path}.{key}", depth + 1))
    return problems


def schema_problems(schema: Any, where: str) -> list[str]:
    """Why a registered JSON Schema cannot be validated on SRW's own loop.

    A regular expression (``pattern``, ``patternProperties``, a ``regex``
    format) can take time exponential in its input, so none may appear in
    any mapping of the document, whatever key holds it. A ``$ref`` may point
    only at ``#/$defs/<name>`` or ``#/definitions/<name>`` that exists, and
    no ``$id``, ``$anchor``, ``$dynamicAnchor``, ``$dynamicRef`` or
    ``$recursiveRef`` may name another target: a reference can reach
    nothing the structural walk did not check, and never fetches. Size and
    depth bound the document (:data:`MAX_SCHEMA_BYTES`,
    :data:`MAX_SCHEMA_DEPTH`), and the cost of one validation is bounded
    too: no ``$ref`` loops, no combinator is wider than
    :data:`MAX_SCHEMA_WIDTH`, at most :data:`MAX_SCHEMA_NODES` schemas apply
    (:data:`MAX_EXPANDED_DEPTH` deep) once every ``$ref`` is followed, and
    no ``uniqueItems``, ``unevaluatedProperties`` or ``unevaluatedItems``
    (their cost grows faster than the document). What is validated is
    bounded by :func:`instance_problem`.
    """
    if not isinstance(schema, Mapping):
        return [f"{where} must be an object"]
    try:
        size = len(canonical_spec(schema).encode("utf-8"))
    except (TypeError, ValueError, RecursionError):
        return [f"{where} is not plain JSON"]
    problems: list[str] = []
    if size > MAX_SCHEMA_BYTES:
        problems.append(f"{where} exceeds {MAX_SCHEMA_BYTES // 1024} KiB")
        return problems
    problems += _regex_problems(schema, where)
    dialect = schema.get("$schema")
    if dialect is not None and dialect != _DIALECT:
        problems.append(f"{where}.$schema is {_DIALECT} or absent")
    definitions = {
        keyword: schema.get(keyword)
        for keyword in ("$defs", "definitions")
        if isinstance(schema.get(keyword), Mapping)
    }

    def walk(node: Any, path: str, depth: int) -> None:
        if isinstance(node, bool):
            return
        if not isinstance(node, Mapping):
            problems.append(f"{path} is not a schema")
            return
        if depth > MAX_SCHEMA_DEPTH:
            problems.append(f"{where} nests deeper than {MAX_SCHEMA_DEPTH} levels")
            return
        if "$schema" in node and depth > 1:
            problems.append(f"{path}.$schema is the root's only")
        for keyword in _COSTLY_KEYWORDS:
            if keyword in node:
                problems.append(
                    f"{path} uses {keyword}: its cost grows faster than the "
                    "document (use additionalProperties or items)"
                )
        if node.get("uniqueItems") is True:
            problems.append(
                f"{path} uses uniqueItems: its cost is quadratic in the array "
                "(check it in the driver's check)"
            )
        if "$ref" in node:
            ref = node["$ref"]
            match = _LOCAL_REF.fullmatch(ref) if isinstance(ref, str) else None
            if match is None:
                problems.append(
                    f"{path}.$ref must point inside the schema, at "
                    "#/$defs/<name> or #/definitions/<name>"
                )
            elif match.group(2) not in definitions.get(match.group(1), {}):
                problems.append(f"{path}.$ref names {ref}, which the schema lacks")
        for keyword, value in node.items():
            at = f"{path}.{keyword}"
            if keyword in _SUBSCHEMA_MAPS and isinstance(value, Mapping):
                for name, child in value.items():
                    walk(child, f"{at}.{name}", depth + 1)
            elif keyword in _SUBSCHEMA_LISTS and isinstance(value, list):
                for index, child in enumerate(value):
                    walk(child, f"{at}[{index}]", depth + 1)
            elif keyword in _SUBSCHEMAS:
                if isinstance(value, list):
                    for index, child in enumerate(value):
                        walk(child, f"{at}[{index}]", depth + 1)
                else:
                    walk(value, at, depth + 1)

    walk(schema, where, 1)
    if not problems:
        problems += _cost_problems(schema, where)
    return list(dict.fromkeys(problems))


def _applied(
    node: Any,
) -> tuple[int, int, list[tuple[tuple[str, str], int]], list[str]]:
    """What validating against ``node`` applies without following a
    ``$ref``: its schema nodes, how deep they nest, the references it makes
    (each with the depth it is made at) and the keywords wider than
    :data:`MAX_SCHEMA_WIDTH`. A nested ``$defs`` is applied only by
    reference, so it is not counted here."""
    count = 0
    deepest = 0
    refs: list[tuple[tuple[str, str], int]] = []
    wide: list[str] = []
    stack: list[tuple[Any, int]] = [(node, 1)]
    while stack:
        current, depth = stack.pop()
        if not isinstance(current, (Mapping, bool)):
            continue
        count += 1
        deepest = max(deepest, depth)
        if isinstance(current, bool):
            continue
        ref = current.get("$ref")
        match = _LOCAL_REF.fullmatch(ref) if isinstance(ref, str) else None
        if match is not None:
            refs.append(((match.group(1), match.group(2)), depth))
        for keyword, value in current.items():
            if keyword in ("$defs", "definitions"):
                continue
            if keyword in _SUBSCHEMA_MAPS and isinstance(value, Mapping):
                stack.extend((child, depth + 1) for child in value.values())
            elif keyword in _SUBSCHEMA_LISTS or (
                keyword in _SUBSCHEMAS and isinstance(value, list)
            ):
                if isinstance(value, list):
                    if len(value) > MAX_SCHEMA_WIDTH:
                        wide.append(keyword)
                    stack.extend((child, depth + 1) for child in value)
            elif keyword in _SUBSCHEMAS:
                stack.append((value, depth + 1))
    return count, deepest, refs, wide


def _cost_problems(schema: Mapping[str, Any], where: str) -> list[str]:
    """Why validating against ``schema`` could cost more than SRW allows:
    a ``$ref`` that loops (a definition reaching itself), a combinator
    wider than :data:`MAX_SCHEMA_WIDTH`, or more than
    :data:`MAX_SCHEMA_NODES` schema nodes (or deeper than
    :data:`MAX_EXPANDED_DEPTH`) once every reference is followed, each
    definition's cost computed once."""
    definitions = {
        (keyword, str(name)): child
        for keyword in ("$defs", "definitions")
        if isinstance(schema.get(keyword), Mapping)
        for name, child in schema[keyword].items()
    }
    local = {key: _applied(child) for key, child in definitions.items()}
    root = _applied(schema)
    problems = [
        f"{where}: a {keyword} holds more than {MAX_SCHEMA_WIDTH} schemas"
        for keyword in sorted(
            set(root[3]).union(*(entry[3] for entry in local.values()))
        )
    ]
    # Each definition's cost once every reference is followed, computed in
    # dependency order (Kahn): one that never becomes computable loops.
    cost: dict[tuple[str, str], tuple[int, int]] = {}
    waiting = {
        key: {target for target, _depth in entry[2] if target in local}
        for key, entry in local.items()
    }
    users: dict[tuple[str, str], set[tuple[str, str]]] = {key: set() for key in local}
    for key, targets in waiting.items():
        for target in targets:
            users[target].add(key)
    ready = [key for key, targets in waiting.items() if not targets]
    cap = MAX_SCHEMA_NODES + 1
    while ready:
        key = ready.pop()
        count, deepest, refs, _wide = local[key]
        for target, depth in refs:
            if target in cost:
                count = min(cap, count + cost[target][0])
                deepest = max(deepest, depth + cost[target][1])
        cost[key] = (count, min(deepest, MAX_EXPANDED_DEPTH + 1))
        for user in users[key]:
            waiting[user].discard(key)
            if not waiting[user]:
                ready.append(user)
    looping = sorted(f"#/{kind}/{name}" for kind, name in set(local) - set(cost))
    if looping:
        problems.append(
            f"{where} has a $ref that loops (through {', '.join(looping[:5])})"
        )
        return problems
    count, deepest, refs, _wide = root
    for target, depth in refs:
        if target in cost:
            count = min(cap, count + cost[target][0])
            deepest = max(deepest, depth + cost[target][1])
    if count > MAX_SCHEMA_NODES:
        problems.append(
            f"{where} applies more than {MAX_SCHEMA_NODES} schemas once its "
            "$refs are followed"
        )
    if deepest > MAX_EXPANDED_DEPTH:
        problems.append(
            f"{where} nests deeper than {MAX_EXPANDED_DEPTH} levels once its "
            "$refs are followed"
        )
    return problems


def instance_problem(
    value: Any, what: str, *, max_bytes: int | None = None
) -> str | None:
    """Why ``value`` is not validated against a registered schema: more
    than :data:`MAX_INSTANCE_NODES` JSON nodes, or (``max_bytes``) a larger
    canonical JSON. ``None`` when it may be."""
    nodes = 0
    stack = [value]
    while stack:
        current = stack.pop()
        nodes += 1
        if nodes > MAX_INSTANCE_NODES:
            return f"{what} holds more than {MAX_INSTANCE_NODES} values"
        if isinstance(current, Mapping):
            stack.extend(current.values())
        elif isinstance(current, (list, tuple)):
            stack.extend(current)
    if max_bytes is not None:
        try:
            size = len(canonical_spec(value).encode("utf-8"))
        except (TypeError, ValueError, RecursionError):
            return f"{what} is not plain JSON"
        if size > max_bytes:
            return f"{what} exceeds {max_bytes // 1024} KiB"
    return None


def custom_driver_problems(
    spec: DriverSpec, *, privileged: bool, env_names: Iterable[str] = ()
) -> list[str]:
    """Every reason ``spec`` cannot be registered as an image driver.

    ``privileged`` is whether the image may have privilege: its repository
    is trusted, or the operator turned privilege on for custom drivers.
    ``env_names`` are the variable names the spec declares
    (:func:`declared_env_names`).
    """
    env_names = tuple(env_names)
    problems = validate_spec(spec)
    problems += schema_problems(spec.config_schema, "config_schema")
    for slot in spec.credential_slots:
        problems += schema_problems(slot.schema, f"credential slot {slot.name}")
    if reserved_name(spec.name):
        problems.append(
            f"driver names under {RESERVED_NAMESPACE}. are SRW's own; "
            "register yours under your own namespace"
        )
    if spec.plane == "harness":
        problems.append(
            "an image driver runs in its own pod: its plane is bind_time or service"
        )
    elif spec.plane == "in_pod":
        problems.append(
            "the in-pod plane is not available in this release"
            if privileged
            else "the in-pod plane needs a repository the operator trusts "
            "(connectors.drivers.trustedRepositories) or "
            "connectors.customDrivers.privileged"
        )
    elif spec.plane == "bind_time":
        problems += _bind_time_problems(spec, env_names)
    elif spec.plane == "service" and not managed_mcp_driver(spec):
        problems.append(
            "a registered service driver is a managed MCP server in this "
            "release: import its server.json"
        )
    if env_names and spec.plane != "bind_time":
        problems.append("only a bind-time driver declares env_names")
    return problems


def _bind_time_problems(spec: DriverSpec, env_names: tuple[str, ...]) -> list[str]:
    problems: list[str] = []
    if "env_file" in spec.delivery_forms and not env_names:
        problems.append(
            "a bind-time image driver that sets variables declares every name "
            "its bind may return in env_names"
        )
    problems += [
        f"env_names: {why}" for why in map(driver_env_problem, env_names) if why
    ]
    extra = sorted(set(spec.delivery_forms) - set(IMAGE_BIND_FORMS))
    if extra:
        problems.append(
            f"a bind-time image driver returns data in {list(IMAGE_BIND_FORMS)} "
            f"only, not {extra}"
        )
    if spec.credential_delivery != "inline":
        problems.append("a bind-time image driver receives its credentials inline")
    if spec.tool_category:
        problems.append("a bind-time image driver binds no tools")
    if not spec.supported_backends or not spec.supported_backends <= SHELL_BACKENDS:
        problems.append(
            "a bind-time image driver delivers into a shell workspace: "
            f"supported_backends is a non-empty subset of {sorted(SHELL_BACKENDS)}"
        )
    return problems


def repository_trusted(reference: str, trusted: Iterable[str]) -> bool:
    """Whether the image ``reference`` is in a trusted repository.

    ``trusted`` holds repositories (a full reference's tag and digest are
    ignored). A repository matches itself and everything below it on a path
    boundary: ``ghcr.io/org`` trusts ``ghcr.io/org/driver`` but never
    ``ghcr.io/org-evil``. Docker Hub's other host names are Docker Hub. A
    registry host alone, a wildcard or an unreadable entry trusts nothing,
    and an unreadable reference is never trusted.
    """
    try:
        name = _hub(ImageReference.parse(reference).name)
    except ValueError:
        return False
    for item in trusted:
        root = _repository_root(item)
        if root and (name == root or name.startswith(root + "/")):
            return True
    return False


def _repository_root(item: str) -> str | None:
    """A trusted repository as a path prefix of image names.

    A tag or digest is dropped. A path with a registry host is taken as
    written (``docker.io/acme`` is the ``acme`` organisation); one without
    is on Docker Hub, and a bare name is an official image
    (``busybox`` is ``docker.io/library/busybox``).
    """
    if not isinstance(item, str):
        return None
    name = item.strip().split("@", 1)[0]
    head, slash, last = name.rpartition("/")
    name = f"{head}{slash}{last.split(':', 1)[0]}"
    first, slash, rest = name.partition("/")
    if slash and ("." in first or ":" in first or first == "localhost"):
        root = f"{first}/{rest}"
    elif slash:
        root = f"docker.io/{name}"
    else:
        root = f"docker.io/library/{name}"
    root = root.rstrip("/")
    try:
        ImageReference.parse(root)
    except ValueError:
        return None
    return _hub(root)


#: Docker Hub's other host names.
_HUB_ALIASES = ("index.docker.io/", "registry-1.docker.io/")


def _hub(name: str) -> str:
    for alias in _HUB_ALIASES:
        if name.startswith(alias):
            return "docker.io/" + name[len(alias) :]
    return name


# =============================================================================
# What one bind returns
# =============================================================================


def image_binding_problems(
    descriptor: Any, spec: DriverSpec, *, env_names: Iterable[str] = ()
) -> list[str]:
    """Why a bind-time image driver's descriptor cannot be delivered.

    The one check of a bind's output, which SRW and the author test kit both
    run: the descriptor schema's rules, plus: it names the registered
    driver, every entry goes to the workspace in a form the spec declares
    (and SRW delivers for images), it stays within
    :data:`MAX_BINDING_ENTRIES`, every variable it sets (a file's
    ``env_var`` included) is one the spec declares in ``env_names`` and one
    a driver may set (``env_names.driver_env_problem``), with a value the
    workspace takes, no name is set twice, and every file lands at
    :data:`IMAGE_FILE_TARGETS` (one path once, at most
    :data:`MAX_IMAGE_FILE_PATH` characters), never executable, a ``.netrc``
    without a ``macdef``. The messages never show a value.
    """
    problems = validate_binding(descriptor)
    if problems:
        return problems
    declared = set(env_names)
    if descriptor.get("driver") != spec.name:
        problems.append(
            f"the binding names driver {descriptor.get('driver')!r}, not {spec.name}"
        )
    entries = descriptor["entries"]
    if len(entries) > MAX_BINDING_ENTRIES:
        problems.append(f"a binding holds at most {MAX_BINDING_ENTRIES} entries")
    allowed = set(spec.delivery_forms) & set(IMAGE_BIND_FORMS)
    named: set[str] = set()
    paths: set[str] = set()
    for index, entry in enumerate(entries):
        if entry["recipient"] != "workspace":
            problems.append(f"entries[{index}] must go to the workspace")
        if entry["form"] not in allowed:
            problems.append(
                f"entries[{index}].form {entry['form']!r} is not one this "
                f"driver declares ({sorted(allowed)})"
            )
        value = entry["value"]
        name: Any = None
        if entry["form"] == "env_file":
            name = value.get("name")
            if isinstance(name, str):
                why = env_value_problem(name, value.get("value"))
                if why is not None:
                    problems.append(f"entries[{index}]: {why}")
        elif entry["form"] == "credential_file":
            found = _file_problems(value, index)
            problems += found
            if not found:
                relative, _why = target_problem(str(value.get("path") or ""))
                if relative in paths:
                    problems.append(
                        f"entries[{index}] writes ~/{relative}, which another "
                        "entry writes too"
                    )
                paths.add(str(relative))
            name = value.get("env_var") or None
        if name is None:
            continue
        why = driver_env_problem(name)
        if why is not None:
            problems.append(f"entries[{index}]: {why}")
        elif name not in declared:
            problems.append(
                f"entries[{index}] sets {name}, which the driver's spec does not "
                "declare in env_names"
            )
        elif name in named:
            problems.append(f"the binding sets {name} twice")
        named.add(str(name))
    return problems


#: The longest path a driver's file may name.
MAX_IMAGE_FILE_PATH = 255
#: A ``.netrc`` macro: ftp runs the ``init`` one on login.
_NETRC_MACRO = re.compile(r"(?:^|\s)macdef(?:\s|$)")


def _file_problems(value: Mapping[str, Any], index: int) -> list[str]:
    at = f"entries[{index}].value"
    problems: list[str] = []
    path = str(value.get("path") or "")
    if len(path) > MAX_IMAGE_FILE_PATH:
        return [f"{at}.path is longer than {MAX_IMAGE_FILE_PATH} characters"]
    relative, why = target_problem(path)
    if why is not None:
        problems.append(f"{at}.path is refused: {why}")
    elif relative not in IMAGE_FILE_NAMES and not relative.startswith(
        IMAGE_FILE_DIRECTORY + "/"
    ):
        problems.append(
            f"{at}.path is refused: a driver's file goes to {IMAGE_FILE_TARGETS}"
        )
    mode = value.get("mode")
    if mode is not None:
        why = mode_problem(int(mode))
        if why is not None:
            problems.append(f"{at}.mode is refused: {why}")
    if value.get("transform") is not None or value.get("merge_group") is not None:
        problems.append(f"{at}: transform and merge_group are SRW's own")
    content = value.get("content")
    if (
        relative == ".netrc"
        and isinstance(content, str)
        and _NETRC_MACRO.search(content)
    ):
        problems.append(
            f"{at}: a .netrc may not define a macro (macdef): ftp runs one on login"
        )
    return problems


def wire_credentials(descriptor: Mapping[str, Any]) -> dict[str, Any]:
    """A checked descriptor as the ``credentials`` of the agent's wire entry.

    The stored type of an image driver's connector delivers through the
    agent's existing materializers, which read today's entry shape:
    ``env_vars`` (name to value) and ``files`` (target, contents, mode as
    octal text, the variable that names it). A name two entries set is a
    ``ValueError``.
    """
    env: dict[str, str] = {}
    files: list[dict[str, Any]] = []
    for entry in descriptor.get("entries") or ():
        value = entry["value"]
        if entry["form"] == "env_file":
            name = str(value["name"])
            if name in env:
                raise ValueError(f"the binding sets {name} twice")
            env[name] = str(value["value"])
        elif entry["form"] == "credential_file":
            relative, _why = target_problem(str(value["path"]))
            item: dict[str, Any] = {
                "name": (relative or "").rsplit("/", 1)[-1],
                "target_path": f"{STORED_HOME}/{relative}",
                "contents": str(value["content"]),
                "mode": format(int(value.get("mode", 0o600)), "04o"),
            }
            if value.get("env_var"):
                item["env_var"] = str(value["env_var"])
            files.append(item)
    out: dict[str, Any] = {}
    if env:
        out["env_vars"] = env
    if files:
        out["files"] = files
    return out


# =============================================================================
# A moved tag
# =============================================================================


def _slots(value: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {
        str(slot.get("name")): slot
        for slot in value.get("credential_slots") or []
        if isinstance(slot, Mapping)
    }


def _levels(value: Mapping[str, Any]) -> set[str]:
    return {
        str(level.get("id"))
        for level in value.get("access_levels") or []
        if isinstance(level, Mapping)
    }


def _hosts(value: Mapping[str, Any]) -> set[str]:
    return {
        canonical_spec(rule)
        for rule in value.get("egress") or []
        if isinstance(rule, Mapping)
    }


def moved_spec_problems(
    previous: Mapping[str, Any], new: Mapping[str, Any] | None
) -> list[str]:
    """Why the spec of an image a tag moved to cannot replace ``previous``.

    ``previous`` is the spec the connector last bound with (its
    registration's before any bind); ``new`` the new digest's label, or
    ``None`` when it carries none (an empty label is none). Refused: no
    label at all (SRW cannot check a contract it cannot read; an unlabelled
    image stays at the digest it was registered at), a spec that does not
    read, another driver name or plane, an unsupported or other protocol
    major, a slot that disappeared, a new required slot, a slot whose kind
    or schema changed, an access level or a workspace backend dropped (a
    connector or an execution may hold it), and more reach than before: new
    environment names, delivery forms, egress or DNS. The stored config
    against the new ``config_schema`` is the caller's (it owns the JSON
    Schema validator).
    """
    if new is None:
        return [
            f"the new image carries no {SPEC_LABEL} label, so SRW cannot check "
            "it keeps the driver's contract"
        ]
    try:
        spec_from_json(new)
        new_contract = SpecContract.of_label(new)
        previous_contract = SpecContract.of_label(previous)
        new_names = set(declared_env_names(new))
        old_names = set(declared_env_names(previous))
    except ValueError as exc:
        return [f"its spec label is malformed ({exc})"]
    problems = compatibility_problems(previous_contract, new_contract)
    if new.get("plane") != previous.get("plane"):
        problems.append(
            f"the plane changed ({previous.get('plane')} to {new.get('plane')})"
        )
    old_slots, new_slots = _slots(previous), _slots(new)
    for name, slot in sorted(new_slots.items()):
        before = old_slots.get(name)
        if before is None:
            if slot.get("required"):
                problems.append(f"a new credential slot is required: {name}")
        elif canonical_spec(
            {key: slot.get(key) for key in ("kind", "schema")}
        ) != canonical_spec({key: before.get(key) for key in ("kind", "schema")}):
            problems.append(f"credential slot {name} changed its kind or schema")
    added = sorted(new_names - old_names)
    if added:
        problems.append(f"it sets new environment names: {', '.join(added)}")
    forms = sorted(
        set(new.get("delivery_forms") or ()) - set(previous.get("delivery_forms") or ())
    )
    if forms:
        problems.append(f"it delivers new forms: {', '.join(map(str, forms))}")
    if _hosts(new) - _hosts(previous):
        problems.append("it reaches new egress destinations")
    if new.get("needs_dns") and not previous.get("needs_dns"):
        problems.append("it newly needs DNS")
    dropped = sorted(_levels(previous) - _levels(new))
    if dropped:
        problems.append(f"it drops access levels: {', '.join(dropped)}")
    narrowed = sorted(
        set(previous.get("supported_backends") or ())
        - set(new.get("supported_backends") or ())
    )
    if narrowed:
        problems.append(
            f"it no longer supports workspace backends: {', '.join(narrowed)}"
        )
    return problems


__all__ = [
    "IMAGE_BIND_FORMS",
    "IMAGE_FILE_TARGETS",
    "MAX_BINDING_ENTRIES",
    "MAX_ENV_NAMES",
    "MAX_CONFIG_BYTES",
    "MAX_EXPANDED_DEPTH",
    "MAX_IMAGE_FILE_PATH",
    "MAX_INSTANCE_NODES",
    "MAX_SCHEMA_BYTES",
    "MAX_SCHEMA_DEPTH",
    "MAX_SCHEMA_NODES",
    "MAX_SCHEMA_WIDTH",
    "RESERVED_NAMESPACE",
    "SPEC_KEYS",
    "canonical_spec",
    "custom_driver_problems",
    "declared_env_names",
    "image_binding_problems",
    "instance_problem",
    "moved_spec_problems",
    "repository_trusted",
    "reserved_name",
    "schema_problems",
    "spec_from_json",
    "spec_to_json",
    "VALIDATION_SECONDS",
    "wire_credentials",
]
