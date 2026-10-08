"""The generated capability matrix: every installed driver, from its spec.

``GET /api/datasources/drivers`` returns it; the cockpit's matrix page, the
connector form (which access levels to offer, the generic form's schema) and
the MCP server read it.  Design: connector_drivers.md, "A generated capability
matrix" and slice D2.

It is assembled from the registry alone, never from a connector, so it holds
no credential value: a credential slot says what a secret looks like, never
what it is.  A driver's author could still write a value into a schema, so
:func:`public_schema` drops, as keywords and never as property names:

* ``default`` and ``examples`` on any schema that is or contains a
  ``writeOnly`` one (a parent's default can hold its secret child's value);
* ``const`` and ``enum`` inside a ``writeOnly`` schema;
* every ``$ref``: a local one is inlined, so a secret pointing at a shared
  definition loses that definition's default too; a remote or circular one
  becomes ``{}``.

Trust: the built-in drivers are SRW's own code, so their claims are SRW's.
A development driver (the lease probe or the echo service, on only where a
deployment switch installs it) is SRW's too but tier ``development``, never
trusted; a service-plane one shows the image its pods run. A driver
outside the trusted list (registration arrives in D6) is marked
``claims_declared_by_author``: SRW does not verify foreign images, the same
as for workspace images.

Egress has three columns.  ``declared`` is the spec's.  ``enforced`` (the
mechanism and the DNS status) and ``installation`` (whether this cluster's
enforcement is verified, and the start-up wait) are filled for service-plane
drivers when this installation hosts driver pods (D5): each pod's own
NetworkPolicy pins the declared hosts to the addresses resolved when it was
created. The pinned addresses and their resolution time belong to one
connector's pod, so they are shown per connector
(``GET /api/datasources/{id}/egress``), never here: this matrix reads no
connector. A driver that runs inside the SRW process has neither column; the
matrix says so instead of inventing a value.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from orchestrator.services.connector_drivers.base import (
    DatasourceDriver,
    ManifestDeliveryDriver,
)
from orchestrator.services.connector_drivers.registry import (
    ConnectorDriver,
    ConnectorDriverRegistry,
)
from shared.connectors.builtin import BUILTIN_SPECS, DEVELOPMENT_SPECS
from shared.connectors.contract import (
    PROTOCOL_VERSION,
    AccessLevel,
    CredentialSlot,
    DriverSpec,
    EgressRule,
    ServiceSpec,
)

_BUILTIN_NAMES = frozenset(spec.name for spec in BUILTIN_SPECS)
#: Drivers SRW ships for development only, installed when a deployment
#: switch turns them on (the lease probe).
_DEVELOPMENT_NAMES = frozenset(spec.name for spec in DEVELOPMENT_SPECS)
#: Keywords carrying an example or a fallback value: dropped on a schema that
#: is or holds a secret.
_VALUE_KEYWORDS = frozenset({"default", "examples"})
#: Keywords spelling out the allowed values: dropped inside a secret.
_SECRET_VALUE_KEYWORDS = frozenset({"const", "enum"})
#: Where JSON Schema 2020-12 nests subschemas, by shape.
_SCHEMA_MAPS = frozenset({"properties", "patternProperties", "dependentSchemas"})
_SCHEMA_LISTS = frozenset({"allOf", "anyOf", "oneOf", "prefixItems"})
_SCHEMA_VALUES = frozenset(
    {
        "items",
        "additionalProperties",
        "propertyNames",
        "contains",
        "not",
        "if",
        "then",
        "else",
        "unevaluatedItems",
        "unevaluatedProperties",
    }
)
#: Inlined at each ``$ref``, so not repeated.
_DEFINITIONS = frozenset({"$defs", "definitions"})
#: Why a column has no value for a driver that runs in SRW's own process.
IN_PROCESS = {"status": "not_applicable", "reason": "runs_in_srw_process"}
#: How a hosted driver pod's egress is enforced (lane 8 §1).
ENFORCEMENT_MECHANISM = "networkpolicy_ipblock"


@dataclass(frozen=True)
class HostingStatus:
    """This installation's service-plane hosting, for the egress columns.

    ``enforcement_verified`` is the operator's word that the cluster's CNI
    enforced the start-up probe harness
    (``connectors.servicePods.networkEnforcementVerified``); SRW cannot see
    that a NetworkPolicy object is enforced.
    """

    enabled: bool = False
    enforcement_verified: bool = False


def capability_matrix(
    registry: ConnectorDriverRegistry, *, hosting: HostingStatus | None = None
) -> dict[str, Any]:
    """The matrix of every installed driver, in registration order."""
    return {
        "protocol_version": PROTOCOL_VERSION,
        "drivers": [
            driver_entry(driver, hosting=hosting) for driver in registry.drivers()
        ],
    }


def egress_columns(
    spec: DriverSpec, *, in_process: bool, hosting: HostingStatus | None
) -> tuple[dict[str, Any], dict[str, Any]]:
    """The ``enforced`` and ``installation`` columns of one driver."""
    if spec.plane == "service":
        if hosting is None or not hosting.enabled:
            reason = {"status": "not_enforced", "reason": "service_hosting_disabled"}
            return dict(reason), dict(reason)
        dns = spec.needs_dns is not None
        enforced = {
            "status": "enforced",
            "reason": "pinned_per_pod_with_dns" if dns else "pinned_per_pod",
            "mechanism": ENFORCEMENT_MECHANISM,
            # A pod that may resolve names may also leak data through them.
            "dns": "cluster_resolver" if dns else "none",
        }
        verified = hosting.enforcement_verified
        installation = {
            "status": "verified" if verified else "unverified",
            "reason": (
                "start_up_wait_verified" if verified else "start_up_wait_unverified"
            ),
            "namespace_default_deny": True,
            "start_up_wait": True,
        }
        return enforced, installation
    if in_process:
        return dict(IN_PROCESS), dict(IN_PROCESS)
    return _not_hosted(), _not_hosted()


def driver_entry(
    driver: ConnectorDriver, *, hosting: HostingStatus | None = None
) -> dict[str, Any]:
    """One driver's row: its spec, its trust and its egress columns."""
    spec = driver.spec
    in_process = isinstance(driver, (DatasourceDriver, ManifestDeliveryDriver))
    enforced, installation = egress_columns(
        spec, in_process=in_process, hosting=hosting
    )
    return {
        "name": spec.name,
        "title": spec.title,
        "legacy_type": spec.legacy_type,
        # Whether this driver owns its stored type (the type's catalogue
        # entry and form). False for a variant serving some of the type's
        # rows: srw.mcp-remote/v1 next to srw.mcp/v1 for the mcp type.
        "serves_stored_type": (
            isinstance(driver, DatasourceDriver) and driver.serves_stored_type
        ),
        "protocol_version": spec.protocol_version,
        "plane": spec.plane,
        "delivery_forms": list(spec.delivery_forms),
        "supported_backends": sorted(spec.supported_backends),
        "workspace_requirements": spec.workspace_requirements,
        "tool_category": spec.tool_category,
        "operations": list(spec.operations),
        "access_levels": [
            _access_level(level)
            for level in sorted(spec.access_levels, key=lambda lv: lv.rank)
        ],
        "default_access": spec.default_access,
        "forced_read_only": spec.forced_read_only,
        "holds_upstream_credentials": spec.holds_upstream_credentials,
        "credential_slots": [_slot(slot) for slot in spec.credential_slots],
        "config_schema": public_schema(spec.config_schema),
        "legacy_connection_url": spec.legacy_connection_url,
        "egress": {
            "declared": {
                "rules": [_egress_rule(rule) for rule in spec.egress],
                "needs_dns": spec.needs_dns,
            },
            "enforced": enforced,
            "installation": installation,
        },
        "publishable": spec.publishable,
        "live_attach": spec.live_attach,
        "live_detach": spec.live_detach,
        "delete_while_attached": spec.delete_while_attached,
        "max_per_execution": spec.max_per_execution,
        "needs_knowledge_profile": spec.needs_knowledge_profile,
        "deployment_gate": spec.deployment_gate,
        "service": _service(spec.service),
        "trust": _trust(
            spec,
            in_process=in_process,
            image=getattr(driver, "image_reference", None) or None,
        ),
    }


def public_schema(schema: Any) -> Any:
    """``schema`` as JSON with no value a secret could hide in (module doc)."""
    return _public_node(schema, schema, secret=False, refs=())


def _public_node(node: Any, root: Any, *, secret: bool, refs: tuple[str, ...]) -> Any:
    if not isinstance(node, Mapping):
        return _json(node)  # a boolean schema
    node, refs = _inline_ref(node, root, refs)
    secret = secret or node.get("writeOnly") is True
    holds_secret = secret or _holds_write_only(node, root, refs)
    out: dict[str, Any] = {}
    for key, value in node.items():
        if key in _DEFINITIONS:
            continue
        if key in _VALUE_KEYWORDS and holds_secret:
            continue
        if key in _SECRET_VALUE_KEYWORDS and secret:
            continue
        if key in _SCHEMA_MAPS and isinstance(value, Mapping):
            out[key] = {
                name: _public_node(sub, root, secret=secret, refs=refs)
                for name, sub in value.items()
            }
        elif key in _SCHEMA_LISTS and isinstance(value, (list, tuple)):
            out[key] = [
                _public_node(sub, root, secret=secret, refs=refs) for sub in value
            ]
        elif key in _SCHEMA_VALUES:
            out[key] = (
                [_public_node(sub, root, secret=secret, refs=refs) for sub in value]
                if isinstance(value, (list, tuple))
                else _public_node(value, root, secret=secret, refs=refs)
            )
        else:
            out[key] = _json(value)
    return out


def _inline_ref(
    node: Mapping[str, Any], root: Any, refs: tuple[str, ...]
) -> tuple[Mapping[str, Any], tuple[str, ...]]:
    """``node`` with its ``$ref`` replaced by the target, its siblings winning.

    Only a local reference resolves; a remote, missing or circular one leaves
    ``{}`` (any value), which shows nothing an author put behind it.
    """
    while isinstance(node.get("$ref"), str):
        ref = node["$ref"]
        target = _resolve_pointer(root, ref) if ref not in refs else None
        if not isinstance(target, Mapping):
            return {}, refs
        refs = (*refs, ref)
        siblings = {key: value for key, value in node.items() if key != "$ref"}
        node = {**target, **siblings}
    return node, refs


def _resolve_pointer(root: Any, ref: str) -> Any:
    if not ref.startswith("#"):
        return None
    current = root
    for part in ref[1:].split("/")[1:]:
        part = part.replace("~1", "/").replace("~0", "~")
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current


def _holds_write_only(node: Any, root: Any, refs: tuple[str, ...]) -> bool:
    """Whether ``node`` or any subschema under it is ``writeOnly``."""
    if not isinstance(node, Mapping):
        return False
    node, refs = _inline_ref(node, root, refs)
    if node.get("writeOnly") is True:
        return True
    for key, value in node.items():
        if key in _SCHEMA_MAPS and isinstance(value, Mapping):
            subs = list(value.values())
        elif key in _SCHEMA_LISTS or key in _SCHEMA_VALUES:
            subs = list(value) if isinstance(value, (list, tuple)) else [value]
        else:
            continue
        if any(_holds_write_only(sub, root, refs) for sub in subs):
            return True
    return False


def _json(value: Any) -> Any:
    """Plain data as JSON: mappings to dicts, tuples to lists."""
    if isinstance(value, Mapping):
        return {key: _json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json(item) for item in value]
    return value


def _access_level(level: AccessLevel) -> dict[str, Any]:
    return {
        "id": level.id,
        "rank": level.rank,
        "tools": level.tools if level.tools == "*" else list(level.tools),
        "enforced_by": level.enforced_by,
        "advisory": level.advisory,
    }


def _slot(slot: CredentialSlot) -> dict[str, Any]:
    return {
        "name": slot.name,
        "kind": slot.kind,
        "required": slot.required,
        "rotatable": slot.rotatable,
        "access_levels": list(slot.access_levels),
        "delivery": slot.delivery,
        "update": slot.update,
        # The read field listing the slot's key names, never their values.
        "names_field": slot.names_field,
        "schema": public_schema(slot.schema),
    }


def _egress_rule(rule: EgressRule) -> dict[str, Any]:
    return {"host": rule.host, "ports": list(rule.ports), "protocol": rule.protocol}


def _not_hosted() -> dict[str, str]:
    """An image driver before D5: SRW runs no driver pod, so enforces nothing."""
    return {"status": "not_enforced", "reason": "driver_hosting_not_available"}


def _service(service: ServiceSpec | None) -> dict[str, Any] | None:
    if service is None:
        return None
    return {
        "instancing": service.instancing,
        "resources": _json(service.resources),
        "start_seconds": service.start_seconds,
        "mcp": _json(service.mcp) if service.mcp is not None else None,
        "port": service.port,
        "callers": list(service.callers),
    }


def _trust(
    spec: DriverSpec, *, in_process: bool, image: str | None = None
) -> dict[str, Any]:
    """Built-in drivers are SRW's; anything else is its author's word.

    A service driver shows the image reference its pods run.
    """
    if in_process and spec.name in _BUILTIN_NAMES:
        return {
            "tier": "builtin",
            "trusted": True,
            "image": None,
            "claims_declared_by_author": False,
        }
    if in_process and spec.name in _DEVELOPMENT_NAMES:
        # SRW's own code, so its claims are SRW's, but installed for
        # development and gates only (the lease probe, the echo service):
        # never a trusted driver for real connectors.
        return {
            "tier": "development",
            "trusted": False,
            "image": image,
            "claims_declared_by_author": False,
        }
    # D6 registers image drivers with their image reference and checks it
    # against the operator's trusted repositories; nothing else exists yet.
    return {
        "tier": "custom",
        "trusted": False,
        "image": image,
        "claims_declared_by_author": True,
    }
