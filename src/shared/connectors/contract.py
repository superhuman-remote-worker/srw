"""What a connector driver declares about itself: ``DriverSpec``.

A *driver* is the pluggable unit (``srw.postgresql/v1``,
``community.neo4j-mcp/v2``); a *connector* is one configured instance of it.
The spec depends only on the driver, never on a connector's config, so SRW can
cache it per image digest and build the capability matrix from it.

The vocabularies are plain strings so a spec round-trips through JSON as an
image label does. :func:`validate_spec` is the one place that checks them.

Design: knowledge-base/knowledge/features/connector_drivers.md, "The driver
contract".
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, get_args

#: The driver protocol SRW speaks: the envelope, not a driver's config
#: contract (``/vN`` in its name) and not its image tag.
PROTOCOL_VERSION = "1.0"
SUPPORTED_PROTOCOL_MAJORS: frozenset[int] = frozenset({1})

#: Where the driver's work runs.  ``harness`` is SRW's own process: the
#: orchestrator answers the control plane and the agent process delivers.
Plane = Literal["harness", "bind_time", "service", "in_pod"]
#: How a binding reaches its recipient.  The agent keys its materializers by
#: form, never by driver, so an image driver can only return these.
DeliveryForm = Literal[
    "env_file",
    "credential_file",
    "checkout",
    "managed_connection",
    "mcp_client",
    "knowledge_index",
    "pod_env",
    "pod_file",
    "ssh_identity",
    "lease_token",
]
CredentialKind = Literal[
    "secret_string", "file", "ssh_private_key", "oauth2", "kubeconfig"
]
CredentialDelivery = Literal["env", "file", "ssh_agent"]
CredentialUpdate = Literal["keep_if_blank", "merge", "replace"]
#: How a connector's upstream credential is handed out.  ``inline``: the
#: credential itself travels in the binding (every built-in driver today).
#: ``lease``: SRW issues a short-lived lease token instead, delivered in the
#: ``lease_token`` form, and the driver exchanges it for the credential
#: (slice C2, "The lease service").
CredentialDeliveryMode = Literal["inline", "lease"]
LiveDetach = Literal["immediate", "next_attach", "refused"]
Operation = Literal[
    "spec", "check", "bind", "revoke", "renew", "discover", "gc", "status", "reindex"
]

PLANES: tuple[str, ...] = get_args(Plane)
DELIVERY_FORMS: tuple[str, ...] = get_args(DeliveryForm)
CREDENTIAL_KINDS: tuple[str, ...] = get_args(CredentialKind)
CREDENTIAL_DELIVERIES: tuple[str, ...] = get_args(CredentialDelivery)
CREDENTIAL_UPDATES: tuple[str, ...] = get_args(CredentialUpdate)
CREDENTIAL_DELIVERY_MODES: tuple[str, ...] = get_args(CredentialDeliveryMode)
LIVE_DETACH: tuple[str, ...] = get_args(LiveDetach)
OPERATIONS: tuple[str, ...] = get_args(Operation)
REQUIRED_OPERATIONS: frozenset[str] = frozenset({"spec", "check", "bind", "revoke"})
OPTIONAL_OPERATIONS: frozenset[str] = frozenset(OPERATIONS) - REQUIRED_OPERATIONS
#: Workspace backends, as ``shared.workspace_contract`` names them.
WORKSPACE_BACKENDS: frozenset[str] = frozenset({"sandbox", "vm", "virtual", "none"})

#: ``<namespace>.<driver>/v<major>``; the namespace has at least one label.
#: Plain enough to mean the same in Python and ECMA-262 (JSON Schema).
DRIVER_NAME_PATTERN = r"^[a-z][a-z0-9-]*(?:\.[a-z][a-z0-9-]*)+/v[1-9][0-9]*$"
_DRIVER_NAME = re.compile(DRIVER_NAME_PATTERN)
_PROTOCOL = re.compile(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")
_EGRESS_PROTOCOLS = frozenset({"tcp", "udp"})
_FIELD_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
#: Fields a connector read already carries: the datasource row's columns and
#: what the API adds to it. A slot's ``names_field`` may not shadow one.
RESERVED_READ_FIELDS: frozenset[str] = frozenset(
    {
        "id",
        "name",
        "description",
        "type",
        "connection_url",
        "connection_url_redacted",
        "credentials",
        "cli_hint",
        "default_branch",
        "config",
        "job_id",
        "project_id",
        "created_by",
        "created_at",
        "updated_at",
        "is_global",
        "read_only",
        "scope_mode",
        "auto_attach",
        "policy_revision",
        "project_ids",
        "project_count",
        "project_read_only",
        "default_selected",
        "unavailable",
    }
)


@dataclass(frozen=True, slots=True)
class AccessLevel:
    """One access level a connector of this driver can be bound at.

    ``enforced_by`` names the mechanism that makes the level true, in one
    line; ``advisory`` marks a level that is only told to the agent.
    ``tools`` lists the built-in tools the level binds, or ``"*"`` for tools
    discovered at runtime.  Across connectors of one category the highest
    ``rank`` wins.
    """

    id: str
    rank: int
    enforced_by: str
    tools: tuple[str, ...] | Literal["*"] = ()
    advisory: bool = False


@dataclass(frozen=True, slots=True)
class CredentialSlot:
    """A named part of a connector's credentials object.

    ``schema`` is a JSON Schema for the keys the slot owns; secret values carry
    ``"writeOnly": true``.  ``access_levels`` lists the levels that need it
    (empty: every level).  ``delivery`` is how the credential reaches the
    workspace (an environment file, a file, or a key in its ssh-agent), or
    ``None`` when it never does (SRW or the agent process holds it).  ``update`` is
    how an edit treats the stored value: a blank edit keeps it, a merge adds
    keys, a replace swaps the whole object.  ``names_field``, when set, is the
    field a connector read shows the slot's key names under (never their
    values), so an editor can tell what a merge adds to.  It needs the slot's
    value to be an object, and may not shadow a field the read already has.
    """

    name: str
    kind: CredentialKind
    schema: Mapping[str, Any]
    required: bool = False
    rotatable: bool = False
    access_levels: tuple[str, ...] = ()
    delivery: CredentialDelivery | None = None
    update: CredentialUpdate = "keep_if_blank"
    names_field: str | None = None


@dataclass(frozen=True, slots=True)
class EgressRule:
    """A destination the driver's own pod connects to.

    ``host`` is a literal host, a CIDR, or ``${config.<key>}``; ports may be
    ``${config.<key>}`` too.
    """

    host: str
    ports: tuple[int | str, ...]
    protocol: Literal["tcp", "udp"] = "tcp"


#: Who calls a service driver's pod: the agent process (``harness``: managed
#: MCP) or the bound workspace (``workspace``: the git and HTTP swaps).
ServiceCaller = Literal["harness", "workspace"]
SERVICE_CALLERS: tuple[str, ...] = get_args(ServiceCaller)
#: The one named port every service driver pod listens on; static rules in
#: the chart open exactly this name into the driver namespace.
SERVICE_PORT_NAME = "srw-driver"


@dataclass(frozen=True, slots=True)
class ServiceSpec:
    """How a service-plane driver's pod is run (connector drivers D5).

    ``instancing`` is ``per_execution`` when the driver keeps per-caller state
    or hands out handles only one caller understands.  ``port`` is the
    container port served under the fixed name ``srw-driver``; ``callers``
    say who reaches it (agent pods, or each binding's own workspace).
    ``resources`` are Kubernetes ``requests``/``limits``, capped by the
    installation.  ``mcp`` carries the managed-MCP block (transport, port,
    path, protocol, stdio mode, limits, tool pinning) for MCP server images.
    """

    instancing: Literal["shared", "per_execution"] = "shared"
    resources: Mapping[str, Any] = field(default_factory=dict)
    start_seconds: int = 30
    mcp: Mapping[str, Any] | None = None
    port: int = 8080
    callers: tuple[ServiceCaller, ...] = ("workspace",)


@dataclass(frozen=True, slots=True)
class DriverSpec:
    """Everything SRW knows about a driver without running a connector.

    ``legacy_type`` is the ``datasources.type`` a built-in driver serves
    (``None`` for drivers with no datasource row).  ``config_schema`` is JSON
    Schema 2020-12: choices are a ``oneOf`` whose branches carry one ``const``
    discriminator, and UI hints use ``x-srw-*`` keys.

    ``legacy_connection_url`` is for built-in drivers only: whether their
    datasource row's ``connection_url`` column is required, optional or
    unused.  It is storage, not contract: an image driver keeps everything in
    ``config`` (D3 maps the column to a driver-owned config key), and the
    request envelope has no slot for it.

    The behaviour flags replace type checks outside the driver:
    ``publishable`` (may be made public), ``forced_read_only``,
    ``live_attach``, ``live_detach``, ``delete_while_attached``,
    ``max_per_execution``, ``needs_knowledge_profile``, ``deployment_gate``
    (an installation switch that must be on) and
    ``holds_upstream_credentials``.  ``service`` is set for service-plane
    drivers only.

    ``credential_delivery`` is ``lease`` for a driver whose upstream
    credential never reaches the agent: SRW issues one lease per
    workspace-owning execution and delivers its token in the
    ``lease_token`` form (which such a driver must list, and no other may).
    """

    name: str
    title: str
    plane: Plane
    delivery_forms: tuple[DeliveryForm, ...]
    config_schema: Mapping[str, Any]
    access_levels: tuple[AccessLevel, ...]
    supported_backends: frozenset[str]
    workspace_requirements: str
    protocol_version: str = PROTOCOL_VERSION
    legacy_type: str | None = None
    guide_topic: str | None = None
    credential_slots: tuple[CredentialSlot, ...] = ()
    legacy_connection_url: Literal["required", "optional", "forbidden"] = "forbidden"
    default_access: str | None = None
    tool_category: str | None = None
    operations: tuple[Operation, ...] = ()
    egress: tuple[EgressRule, ...] = ()
    needs_dns: str | None = None
    publishable: bool = True
    forced_read_only: bool = False
    live_attach: bool = True
    live_detach: LiveDetach = "immediate"
    delete_while_attached: bool = True
    max_per_execution: int | None = None
    needs_knowledge_profile: bool = False
    deployment_gate: str | None = None
    holds_upstream_credentials: bool = False
    service: ServiceSpec | None = None
    credential_delivery: CredentialDeliveryMode = "inline"

    def access_level(self, level_id: str) -> AccessLevel | None:
        return next(
            (level for level in self.access_levels if level.id == level_id), None
        )

    def ranked_access_ids(self) -> tuple[str, ...]:
        """Access level ids from the lowest rank to the highest."""
        return tuple(
            level.id for level in sorted(self.access_levels, key=lambda lv: lv.rank)
        )


def effective_access(entry: Mapping[str, Any], spec: DriverSpec) -> str | None:
    """The access level a delivered entry binds at, from the driver's levels.

    The one rule for the agent (a binding's ``access``) and SRW (the level a
    credential lease is issued at). A read-only project link clamps to the
    lowest level. Otherwise the level the connector's config names (a driver
    whose config has an ``access`` property, such as email), else the
    driver's default, else its highest; a name the driver does not offer
    fails closed to the lowest.
    """
    levels = spec.ranked_access_ids()
    if not levels:
        return None
    if entry.get("project_read_only", False):
        return levels[0]
    requested = None
    properties = spec.config_schema.get("properties")
    config = entry.get("config")
    if isinstance(properties, Mapping) and "access" in properties:
        requested = config.get("access") if isinstance(config, Mapping) else None
    if requested is None:
        requested = spec.default_access or levels[-1]
    return requested if requested in levels else levels[0]


def protocol_major(version: str) -> int | None:
    """The major of a ``MAJOR.MINOR`` protocol version, or ``None`` if malformed."""
    match = _PROTOCOL.fullmatch(version or "")
    return int(match.group(1)) if match else None


def protocol_supported(version: str) -> bool:
    return protocol_major(version) in SUPPORTED_PROTOCOL_MAJORS


def validate_driver_name(name: str) -> list[str]:
    if not isinstance(name, str) or not _DRIVER_NAME.fullmatch(name):
        return [f"driver name {name!r} is not '<namespace>.<driver>/v<major>'"]
    return []


def validate_spec(spec: DriverSpec) -> list[str]:
    """Every problem with ``spec``, as human-readable lines (empty when valid)."""
    problems = validate_driver_name(spec.name)
    if not spec.title:
        problems.append("title is required")
    if not protocol_supported(spec.protocol_version):
        problems.append(f"protocol_version {spec.protocol_version!r} is not supported")
    if spec.plane not in PLANES:
        problems.append(f"plane {spec.plane!r} is not one of {PLANES}")
    if not spec.delivery_forms:
        problems.append("delivery_forms is empty")
    problems += [
        f"delivery form {form!r} is not one of {DELIVERY_FORMS}"
        for form in spec.delivery_forms
        if form not in DELIVERY_FORMS
    ]
    if not isinstance(spec.config_schema, Mapping) or (
        spec.config_schema.get("type") != "object"
    ):
        problems.append("config_schema must be an object schema")
    problems += [
        f"operation {op!r} is not an optional operation"
        for op in spec.operations
        if op not in OPTIONAL_OPERATIONS
    ]
    unknown_backends = sorted(set(spec.supported_backends) - WORKSPACE_BACKENDS)
    if unknown_backends:
        problems.append(f"unknown workspace backends {unknown_backends}")
    if spec.legacy_connection_url not in ("required", "optional", "forbidden"):
        problems.append(
            f"legacy_connection_url {spec.legacy_connection_url!r} is invalid"
        )
    elif spec.legacy_type is None and spec.legacy_connection_url != "forbidden":
        problems.append("only a built-in datasource driver has a connection_url column")
    if spec.live_detach not in LIVE_DETACH:
        problems.append(f"live_detach {spec.live_detach!r} is not one of {LIVE_DETACH}")
    if spec.max_per_execution is not None and spec.max_per_execution < 1:
        problems.append("max_per_execution must be positive")
    if spec.credential_delivery not in CREDENTIAL_DELIVERY_MODES:
        problems.append(
            f"credential_delivery {spec.credential_delivery!r} is not one of "
            f"{CREDENTIAL_DELIVERY_MODES}"
        )
    elif (spec.credential_delivery == "lease") != (
        "lease_token" in spec.delivery_forms or managed_mcp_driver(spec)
    ):
        problems.append(
            "a driver delivers in the lease_token form exactly when its "
            "credential_delivery is 'lease' (a managed MCP driver's lease is "
            "its client's bearer token instead)"
        )
    problems += _access_problems(spec)
    problems += _slot_problems(spec)
    if (spec.service is not None) != (spec.plane == "service"):
        problems.append("service is set exactly when plane is 'service'")
    elif spec.service is not None:
        service = spec.service
        if service.instancing not in ("shared", "per_execution"):
            problems.append(f"instancing {service.instancing!r} is invalid")
        if (
            isinstance(service.port, bool)
            or not isinstance(service.port, int)
            or not 1 <= service.port <= 65535
        ):
            problems.append(f"service port {service.port!r} is not a port")
        if not service.callers or set(service.callers) - set(SERVICE_CALLERS):
            problems.append(
                f"service callers {service.callers!r} must be a non-empty subset "
                f"of {SERVICE_CALLERS}"
            )
        if service.mcp is not None:
            problems += _managed_mcp_problems(spec)
    for rule in spec.egress:
        if not rule.host or not rule.ports:
            problems.append("an egress rule needs a host and at least one port")
        if rule.protocol not in _EGRESS_PROTOCOLS:
            problems.append(f"egress protocol {rule.protocol!r} is invalid")
    return problems


def managed_mcp_driver(spec: DriverSpec) -> bool:
    """Whether ``spec`` is a managed MCP driver: a service driver with an
    ``mcp`` block whose client is the agent process (D5a)."""
    return bool(
        spec.service is not None
        and spec.service.mcp is not None
        and "mcp_client" in spec.delivery_forms
    )


def _managed_mcp_problems(spec: DriverSpec) -> list[str]:
    """A managed MCP driver: the agent process is the client, through the
    front, with a lease token as its bearer; never the workspace."""
    from .mcp import mcp_problems

    service = spec.service
    problems = mcp_problems(
        service.mcp,
        access_levels=[level.id for level in spec.access_levels],
        front_port=service.port,
    )
    if tuple(service.callers) != ("harness",):
        problems.append("a managed MCP server is called by the agent process only")
    if spec.delivery_forms != ("mcp_client",):
        problems.append("a managed MCP driver delivers in the mcp_client form only")
    if spec.credential_delivery != "lease":
        problems.append(
            "a managed MCP driver delivers by lease: the front authenticates "
            "each call with the caller's lease token"
        )
    return problems


def _access_problems(spec: DriverSpec) -> list[str]:
    problems: list[str] = []
    ids = [level.id for level in spec.access_levels]
    if len(ids) != len(set(ids)):
        problems.append("access level ids are not unique")
    ranks = [level.rank for level in spec.access_levels]
    if len(ranks) != len(set(ranks)):
        problems.append("access level ranks are not unique")
    for level in spec.access_levels:
        if not level.enforced_by:
            problems.append(f"access level {level.id!r} has no enforced_by line")
        if level.tools != "*" and not isinstance(level.tools, tuple):
            problems.append(f"access level {level.id!r} tools must be a tuple or '*'")
    if spec.default_access is not None and spec.default_access not in ids:
        problems.append(f"default_access {spec.default_access!r} is not a level")
    if spec.tool_category and not spec.access_levels:
        problems.append("a tool category needs access levels that bind tools")
    return problems


def _slot_problems(spec: DriverSpec) -> list[str]:
    problems: list[str] = []
    names = [slot.name for slot in spec.credential_slots]
    if len(names) != len(set(names)):
        problems.append("credential slot names are not unique")
    level_ids = {level.id for level in spec.access_levels}
    for slot in spec.credential_slots:
        if slot.kind not in CREDENTIAL_KINDS:
            problems.append(f"slot {slot.name!r} kind {slot.kind!r} is invalid")
        if slot.delivery is not None and slot.delivery not in CREDENTIAL_DELIVERIES:
            problems.append(f"slot {slot.name!r} delivery {slot.delivery!r} is invalid")
        if slot.update not in CREDENTIAL_UPDATES:
            problems.append(f"slot {slot.name!r} update {slot.update!r} is invalid")
        if not isinstance(slot.schema, Mapping):
            problems.append(f"slot {slot.name!r} schema must be an object")
        if slot.names_field is not None:
            problems += _names_field_problems(slot)
        unknown = sorted(set(slot.access_levels) - level_ids)
        if unknown:
            problems.append(f"slot {slot.name!r} names unknown access levels {unknown}")
    fields = [slot.names_field for slot in spec.credential_slots if slot.names_field]
    if len(fields) != len(set(fields)):
        problems.append("credential slot names_field values are not unique")
    return problems


def _names_field_problems(slot: CredentialSlot) -> list[str]:
    """A names field shows the keys of an object-valued slot, nowhere else."""
    problems: list[str] = []
    field_name = slot.names_field or ""
    if not _FIELD_NAME.fullmatch(field_name):
        problems.append(f"slot {slot.name!r} names_field is not a field name")
    elif field_name in RESERVED_READ_FIELDS:
        problems.append(
            f"slot {slot.name!r} names_field {field_name!r} is a connector read field"
        )
    schema = slot.schema if isinstance(slot.schema, Mapping) else {}
    properties = schema.get("properties")
    value = properties.get(slot.name) if isinstance(properties, Mapping) else None
    if (
        schema.get("type") != "object"
        or not isinstance(value, Mapping)
        or value.get("type") != "object"
    ):
        problems.append(
            f"slot {slot.name!r} names_field needs an object-typed "
            f"{slot.name!r} in the slot schema"
        )
    return problems
