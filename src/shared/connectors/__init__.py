"""The connector driver contract, shared by every application (stdlib only).

* :mod:`.contract` — ``DriverSpec`` and the vocabularies it uses;
* :mod:`.binding` — the binding descriptor, its JSON Schema and validator;
* :mod:`.envelope` — the request, the typed output lines and error classes;
* :mod:`.builtin` — the specs of the drivers SRW ships.

The control-plane drivers live in ``orchestrator.services.connector_drivers``;
the agent's materializers will live in ``agent.connectors``.  Import-linter
keeps this package free of application and ``shared.runtime`` imports, so the
MCP server and generic hosting can read the same specs.
"""

from .binding import (
    BindingDescriptor,
    BindingEntry,
    binding_schema,
    load_binding_schema,
    validate_binding,
    validate_entry,
)
from .builtin import (
    BUILTIN_SPECS,
    DATASOURCE_SPECS,
    LEGACY_TYPE_IDS,
    MANIFEST_SPECS,
    legacy_types_with_form,
    spec_for_type,
    tool_map,
)
from .contract import (
    PROTOCOL_VERSION,
    AccessLevel,
    CredentialSlot,
    DriverSpec,
    EgressRule,
    ServiceSpec,
    protocol_supported,
    validate_driver_name,
    validate_spec,
)
from .envelope import (
    ERROR_CLASSES,
    DriverError,
    DriverOutcome,
    DriverRequest,
    EnvelopeError,
    ExecutionRef,
    parse_output_line,
    read_output,
    validate_request,
    validate_result,
)

__all__ = [
    "BUILTIN_SPECS",
    "DATASOURCE_SPECS",
    "ERROR_CLASSES",
    "LEGACY_TYPE_IDS",
    "MANIFEST_SPECS",
    "PROTOCOL_VERSION",
    "AccessLevel",
    "BindingDescriptor",
    "BindingEntry",
    "CredentialSlot",
    "DriverError",
    "DriverOutcome",
    "DriverRequest",
    "DriverSpec",
    "EgressRule",
    "EnvelopeError",
    "ExecutionRef",
    "ServiceSpec",
    "binding_schema",
    "legacy_types_with_form",
    "load_binding_schema",
    "parse_output_line",
    "protocol_supported",
    "read_output",
    "spec_for_type",
    "tool_map",
    "validate_binding",
    "validate_driver_name",
    "validate_entry",
    "validate_request",
    "validate_result",
    "validate_spec",
]
