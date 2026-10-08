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

What one ``bind`` of a bind-time image returns is checked by
:func:`image_binding_problems` and turned into the wire entry the agent
already reads for its stored type (:func:`wire_credentials`).

Design: knowledge-base/knowledge/features/connector_drivers.md, "Trust and
registration", "The driver contract" and slice D6.
"""

from __future__ import annotations

import json
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
from .file_targets import STORED_HOME, mode_problem, target_problem
from .images import ImageReference

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
#: The most entries one bind may deliver.
MAX_BINDING_ENTRIES = 100


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


def spec_to_json(spec: DriverSpec) -> dict[str, Any]:
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


def custom_driver_problems(spec: DriverSpec, *, privileged: bool) -> list[str]:
    """Every reason ``spec`` cannot be registered as an image driver.

    ``privileged`` is whether the image may have privilege: its repository
    is trusted, or the operator turned privilege on for custom drivers.
    """
    problems = validate_spec(spec)
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
        problems += _bind_time_problems(spec)
    elif spec.plane == "service" and not managed_mcp_driver(spec):
        problems.append(
            "a registered service driver is a managed MCP server in this "
            "release: import its server.json"
        )
    return problems


def _bind_time_problems(spec: DriverSpec) -> list[str]:
    problems: list[str] = []
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
    ``ghcr.io/org-evil``. An unreadable reference is never trusted.
    """
    try:
        name = ImageReference.parse(reference).name
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
    return root


# =============================================================================
# What one bind returns
# =============================================================================


def image_binding_problems(descriptor: Any, spec: DriverSpec) -> list[str]:
    """Why a bind-time image driver's descriptor cannot be delivered.

    The descriptor schema's rules, plus: it names the registered driver,
    every entry goes to the workspace in a form the spec declares (and SRW
    delivers for images), and it stays within :data:`MAX_BINDING_ENTRIES`.
    """
    problems = validate_binding(descriptor)
    if problems:
        return problems
    if descriptor.get("driver") != spec.name:
        problems.append(
            f"the binding names driver {descriptor.get('driver')!r}, not {spec.name}"
        )
    entries = descriptor["entries"]
    if len(entries) > MAX_BINDING_ENTRIES:
        problems.append(f"a binding holds at most {MAX_BINDING_ENTRIES} entries")
    allowed = set(spec.delivery_forms) & set(IMAGE_BIND_FORMS)
    for index, entry in enumerate(entries):
        if entry["recipient"] != "workspace":
            problems.append(f"entries[{index}] must go to the workspace")
        if entry["form"] not in allowed:
            problems.append(
                f"entries[{index}].form {entry['form']!r} is not one this "
                f"driver declares ({sorted(allowed)})"
            )
        if entry["form"] == "credential_file":
            problems += _file_problems(entry["value"], index)
    return problems


def _file_problems(value: Mapping[str, Any], index: int) -> list[str]:
    at = f"entries[{index}].value"
    problems: list[str] = []
    _relative, why = target_problem(str(value.get("path") or ""))
    if why is not None:
        problems.append(f"{at}.path is refused: {why}")
    mode = value.get("mode")
    if mode is not None:
        why = mode_problem(int(mode))
        if why is not None:
            problems.append(f"{at}.mode is refused: {why}")
    if value.get("transform") is not None or value.get("merge_group") is not None:
        problems.append(f"{at}: transform and merge_group are SRW's own")
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


__all__ = [
    "IMAGE_BIND_FORMS",
    "MAX_BINDING_ENTRIES",
    "RESERVED_NAMESPACE",
    "SPEC_KEYS",
    "canonical_spec",
    "custom_driver_problems",
    "image_binding_problems",
    "repository_trusted",
    "reserved_name",
    "spec_from_json",
    "spec_to_json",
    "wire_credentials",
]
