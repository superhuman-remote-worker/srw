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
from orchestrator.services.connector_drivers.mail import EmailDriver
from orchestrator.services.connector_drivers.manifest import EnvDriver, FilesDriver
from orchestrator.services.connector_drivers.mcp_client import McpDriver
from orchestrator.services.connector_drivers.repository import RepositoryDriver
from orchestrator.services.connector_drivers.ssh_key import SshKeyDriver
from shared.connectors.builtin import BUILTIN_SPECS, MCP_REMOTE_SPEC


def drivers() -> tuple[DatasourceDriver | ManifestDeliveryDriver, ...]:
    """Fresh instances: the generic-hosting drivers, then catalogue order
    (a type's variant drivers right after the one that owns the type)."""
    own: dict[str, DatasourceDriver] = {
        driver.spec.name: driver
        for driver in (
            GenericDriver(),
            CredentialsDriver(),
            RepositoryDriver(),
            KnowledgeBaseDriver(),
            *managed.drivers(),
            EmailDriver(),
            McpDriver(),
            McpDriver(MCP_REMOTE_SPEC, serves_stored_type=False),
            *credential_files.drivers(),
            SshKeyDriver(),
        )
    }
    stored = [spec for spec in BUILTIN_SPECS if spec.legacy_type]
    missing = [spec.name for spec in stored if spec.name not in own]
    if missing:
        raise RuntimeError(f"Built-in specs without a driver: {missing}")
    return (EnvDriver(), FilesDriver()) + tuple(own[spec.name] for spec in stored)
