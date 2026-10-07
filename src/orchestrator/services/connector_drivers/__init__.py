"""Control-plane connector drivers: one per connector type.

The contract they implement is in :mod:`.base`; the shared types they
declare (``DriverSpec``, the binding descriptor, the envelope) are in
``shared.connectors``.  The application builds one
:class:`~.registry.ConnectorDriverRegistry` and hands it to services through
their dependency dataclasses; nothing here reads application state.
"""

from orchestrator.services.connector_drivers.registry import (
    ConnectorDriverRegistry,
    builtin_connector_drivers,
)

__all__ = ["ConnectorDriverRegistry", "builtin_connector_drivers"]
