"""The runtime half of the connector drivers: materializers by delivery form.

The contract lives in ``shared.connectors`` and the control plane in
``orchestrator.services.connector_drivers``. Here the agent applies what a
binding delivers:

* :mod:`.legacy` reads today's payload entries as binding descriptors
  (``binding_from_legacy_entry``, the one agent-side place that maps a stored
  type to forms);
* one materializer per form: :mod:`.env`, :mod:`.files`, :mod:`.checkout`,
  :mod:`.ssh_identity`, :mod:`.connections` (with the closed set of managed
  connection factories), :mod:`.mcp`, :mod:`.knowledge`;
* :mod:`.registry` runs them in the order each entry point needs.

Import-linter keeps this package free of ``agent.agent``, ``agent.graph``
and ``langgraph``: session attach imports it.

Design: knowledge-base/knowledge/features/connector_drivers.md (D1b).
"""

from agent.connectors.base import (
    AGENT_HOME,
    ConnectionFactory,
    Delivery,
    FactsLines,
    Materializer,
    RuntimeContext,
)
from agent.connectors.legacy import binding_from_legacy_entry, deliveries_from_payload
from agent.connectors.registry import ConnectorRegistry, connector_registry

__all__ = [
    "AGENT_HOME",
    "ConnectionFactory",
    "ConnectorRegistry",
    "Delivery",
    "FactsLines",
    "Materializer",
    "RuntimeContext",
    "binding_from_legacy_entry",
    "connector_registry",
    "deliveries_from_payload",
]
