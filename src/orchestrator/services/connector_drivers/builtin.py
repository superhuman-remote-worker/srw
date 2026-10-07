"""The control-plane drivers SRW ships, one per built-in spec."""

from __future__ import annotations

from orchestrator.services.connector_drivers import credential_files, managed
from orchestrator.services.connector_drivers.base import (
    DatasourceDriver,
    ManifestDeliveryDriver,
)
from orchestrator.services.connector_drivers.env import (
    CredentialsDriver,
    GenericDriver,
)
from orchestrator.services.connector_drivers.kb import KnowledgeBaseDriver
from orchestrator.services.connector_drivers.legacy import LegacyDatasourceDriver
from orchestrator.services.connector_drivers.mail import EmailDriver
from orchestrator.services.connector_drivers.mcp_client import McpDriver
from orchestrator.services.connector_drivers.manifest import EnvDriver, FilesDriver
from shared.connectors.builtin import DATASOURCE_SPECS


def drivers() -> tuple[DatasourceDriver | ManifestDeliveryDriver, ...]:
    """Fresh instances: the generic-hosting drivers, then catalogue order.

    A datasource type without a driver of its own is served by the legacy
    adapter, which keeps the code it had before drivers existed.
    """
    own: dict[str, DatasourceDriver] = {
        driver.spec.name: driver
        for driver in (
            GenericDriver(),
            CredentialsDriver(),
            *credential_files.drivers(),
            *managed.drivers(),
            EmailDriver(),
            McpDriver(),
            KnowledgeBaseDriver(),
        )
    }
    return (EnvDriver(), FilesDriver()) + tuple(
        own.get(spec.name) or LegacyDatasourceDriver(spec) for spec in DATASOURCE_SPECS
    )
