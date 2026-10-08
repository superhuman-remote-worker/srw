"""The generated capability matrix: every installed driver, from its spec.

``GET /api/datasources/drivers`` returns it; the cockpit's matrix page, the
connector form (which access levels to offer, the generic form's schema) and
the MCP server read it.  Design: connector_drivers.md, "A generated capability
matrix" and slice D2.

It is assembled from the registry alone, never from a connector, so it holds
no credential value: a credential slot says what a secret looks like, never
what it is.  ``default`` and ``examples`` are dropped from every ``writeOnly``
schema all the same, in case a driver's author put a value there.

Trust: the built-in drivers are SRW's own code, so their claims are SRW's.
A driver outside the trusted list (registration arrives in D6) is marked
``claims_declared_by_author``: SRW does not verify foreign images, the same
as for workspace images.

Egress has three columns.  ``declared`` is the spec's.  ``enforced`` (the
pinned addresses and the mechanism) and ``installation`` (whether this
cluster's enforcement is verified) need driver pods with their own
NetworkPolicy, which service-plane hosting (D5) builds.  A driver that runs
inside the SRW process has neither; the matrix says so instead of inventing
a value.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from orchestrator.services.connector_drivers.base import (
    DatasourceDriver,
    ManifestDeliveryDriver,
)
from orchestrator.services.connector_drivers.registry import (
    ConnectorDriver,
    ConnectorDriverRegistry,
)
from shared.connectors.builtin import BUILTIN_SPECS
from shared.connectors.contract import (
    PROTOCOL_VERSION,
    AccessLevel,
    CredentialSlot,
    DriverSpec,
    EgressRule,
    ServiceSpec,
)

_BUILTIN_NAMES = frozenset(spec.name for spec in BUILTIN_SPECS)
#: Keys that carry an example or a fallback value, never shown for a secret.
_VALUE_KEYS = ("default", "examples")
#: Why a column has no value for a driver that runs in SRW's own process.
IN_PROCESS = {"status": "not_applicable", "reason": "runs_in_srw_process"}


def capability_matrix(registry: ConnectorDriverRegistry) -> dict[str, Any]:
    """The matrix of every installed driver, in registration order."""
    return {
        "protocol_version": PROTOCOL_VERSION,
        "drivers": [driver_entry(driver) for driver in registry.drivers()],
    }


def driver_entry(driver: ConnectorDriver) -> dict[str, Any]:
    """One driver's row: its spec, its trust and its egress columns."""
    spec = driver.spec
    in_process = isinstance(driver, (DatasourceDriver, ManifestDeliveryDriver))
    return {
        "name": spec.name,
        "title": spec.title,
        "legacy_type": spec.legacy_type,
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
            "enforced": dict(IN_PROCESS) if in_process else _not_hosted(),
            "installation": dict(IN_PROCESS) if in_process else _not_hosted(),
        },
        "publishable": spec.publishable,
        "live_attach": spec.live_attach,
        "live_detach": spec.live_detach,
        "delete_while_attached": spec.delete_while_attached,
        "max_per_execution": spec.max_per_execution,
        "needs_knowledge_profile": spec.needs_knowledge_profile,
        "deployment_gate": spec.deployment_gate,
        "service": _service(spec.service),
        "trust": _trust(spec, in_process=in_process),
    }


def public_schema(schema: Any, *, secret: bool = False) -> Any:
    """``schema`` as JSON, without a value under any ``writeOnly`` node."""
    if isinstance(schema, Mapping):
        secret = secret or schema.get("writeOnly") is True
        return {
            key: public_schema(value, secret=secret)
            for key, value in schema.items()
            if not (secret and key in _VALUE_KEYS)
        }
    if isinstance(schema, (list, tuple)):
        return [public_schema(item, secret=secret) for item in schema]
    return schema


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
        "resources": public_schema(service.resources),
        "start_seconds": service.start_seconds,
        "mcp": public_schema(service.mcp) if service.mcp is not None else None,
    }


def _trust(spec: DriverSpec, *, in_process: bool) -> dict[str, Any]:
    """Built-in drivers are SRW's; anything else is its author's word."""
    if in_process and spec.name in _BUILTIN_NAMES:
        return {
            "tier": "builtin",
            "trusted": True,
            "image": None,
            "claims_declared_by_author": False,
        }
    # D6 registers image drivers with their image reference and checks it
    # against the operator's trusted repositories; nothing else exists yet.
    return {
        "tier": "custom",
        "trusted": False,
        "image": None,
        "claims_declared_by_author": True,
    }
