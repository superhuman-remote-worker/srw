"""The installed connector drivers, by driver name and by stored type.

One registry per application lives on ``ApplicationResources`` and reaches
services through their dependency dataclasses.  It is immutable: a future
image driver is registered by building the registry with it, never by
mutating a shared one.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from orchestrator.services.connector_drivers.base import (
    DatasourceDriver,
    ManifestDeliveryDriver,
)
from shared.connectors.contract import DriverSpec, validate_spec

ConnectorDriver = DatasourceDriver | ManifestDeliveryDriver


class ConnectorDriverRegistry:
    """Drivers keyed by ``spec.name`` and, for datasource drivers, by type."""

    def __init__(self, drivers: Iterable[ConnectorDriver]) -> None:
        self._by_name: dict[str, ConnectorDriver] = {}
        self._by_type: dict[str, DatasourceDriver] = {}
        for driver in drivers:
            problems = validate_spec(driver.spec)
            if problems:
                raise ValueError(f"{driver.spec.name}: {'; '.join(problems)}")
            if driver.spec.name in self._by_name:
                raise ValueError(f"Driver {driver.spec.name} is registered twice")
            self._by_name[driver.spec.name] = driver
            if isinstance(driver, DatasourceDriver) and driver.serves_stored_type:
                if driver.type_id in self._by_type:
                    raise ValueError(f"Type {driver.type_id} has two drivers")
                self._by_type[driver.type_id] = driver
        variants = [
            driver.spec.name
            for driver in self._by_name.values()
            if isinstance(driver, DatasourceDriver)
            and not driver.serves_stored_type
            and driver.type_id not in self._by_type
        ]
        if variants:
            raise ValueError(f"{variants} serve a type no driver owns")

    def get(self, name: str) -> ConnectorDriver | None:
        return self._by_name.get(name)

    def for_type(self, type_id: str | None) -> DatasourceDriver | None:
        """The driver serving a stored ``datasources.type``, if installed.

        A type with several drivers (``mcp``: stdio and remote) answers with
        the one that owns it; the others serve some of its rows and are found
        by name.
        """
        return self._by_type.get(type_id or "")

    def manifest_driver(self, name: str) -> ManifestDeliveryDriver | None:
        driver = self._by_name.get(name)
        return driver if isinstance(driver, ManifestDeliveryDriver) else None

    def specs(self) -> tuple[DriverSpec, ...]:
        return tuple(driver.spec for driver in self._by_name.values())

    def drivers(self) -> tuple[ConnectorDriver, ...]:
        """Every installed driver, in registration order."""
        return tuple(self._by_name.values())

    def type_ids(self) -> tuple[str, ...]:
        """Stored types with a driver, in registration order."""
        return tuple(self._by_type)


def builtin_connector_drivers(
    *,
    lease_probe: bool = False,
    echo_service_image: str | None = None,
    managed_mcp_images: Mapping[str, str] | None = None,
) -> ConnectorDriverRegistry:
    """The drivers SRW ships, in catalogue order.

    ``lease_probe`` adds the development lease probe driver after them
    (``orchestrator.connectorLeases.probeDriver``, slice C2);
    ``echo_service_image`` adds the development echo service driver running
    that image (``connectors.drivers.echo``, D5); ``managed_mcp_images``
    adds each managed MCP server it names, by driver name, running that image
    (``connectors.drivers.managedMcp`` and ``mcpTest``, D5a). A name that is
    no managed MCP driver SRW ships is refused.
    """
    from orchestrator.services.connector_drivers import builtin

    drivers: tuple[ConnectorDriver, ...] = builtin.drivers()
    if lease_probe:
        from orchestrator.services.connector_drivers.lease_probe import (
            LeaseProbeDriver,
        )

        drivers += (LeaseProbeDriver(),)
    if echo_service_image:
        from orchestrator.services.connector_drivers.echo_service import (
            EchoServiceDriver,
        )

        drivers += (EchoServiceDriver(echo_service_image),)
    if managed_mcp_images:
        from orchestrator.services.connector_drivers.managed_mcp import (
            ManagedMcpDriver,
        )
        from shared.connectors.builtin import (
            DEVELOPMENT_SPECS,
            MANAGED_MCP_SPECS,
        )
        from shared.connectors.contract import managed_mcp_driver

        shipped = {
            spec.name: spec
            for spec in MANAGED_MCP_SPECS + DEVELOPMENT_SPECS
            if managed_mcp_driver(spec)
        }
        unknown = sorted(set(managed_mcp_images) - set(shipped))
        if unknown:
            raise ValueError(f"{unknown} are no managed MCP drivers SRW ships")
        drivers += tuple(
            ManagedMcpDriver(spec, managed_mcp_images[name])
            for name, spec in shipped.items()
            if managed_mcp_images.get(name)
        )
    return ConnectorDriverRegistry(drivers)
