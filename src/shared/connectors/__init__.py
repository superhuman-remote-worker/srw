"""The connector driver contract, shared by every application (stdlib only).

* :mod:`.contract` — ``DriverSpec`` and the vocabularies it uses;
* :mod:`.binding` — the binding descriptor, its JSON Schema and validator;
* :mod:`.envelope` — the request, the typed output lines and error classes;
* :mod:`.builtin` — the specs of the drivers SRW ships;
* :mod:`.leases` — credential lease and driver identity tokens (C2).

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
    DEVELOPMENT_SPECS,
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
    effective_access,
    protocol_supported,
    validate_driver_name,
    validate_spec,
)
from .envelope import (
    API_CHECK_STATUSES,
    ERROR_CLASSES,
    DriverError,
    DriverOutcome,
    DriverRequest,
    EnvelopeError,
    ExecutionRef,
    api_check_result,
    parse_output_line,
    read_output,
    unsupported_check,
    validate_request,
    validate_result,
)

__all__ = [
    "API_CHECK_STATUSES",
    "BUILTIN_SPECS",
    "DATASOURCE_SPECS",
    "DEVELOPMENT_SPECS",
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
    "api_check_result",
    "binding_schema",
    "effective_access",
    "legacy_types_with_form",
    "load_binding_schema",
    "parse_output_line",
    "protocol_supported",
    "read_output",
    "spec_for_type",
    "tool_map",
    "unsupported_check",
    "validate_binding",
    "validate_driver_name",
    "validate_entry",
    "validate_request",
    "validate_result",
    "validate_spec",
]
