"""Validation of the connectors SRW creates itself.

Platform rows (the ``DEFAULT_DS_*`` defaults ``init`` seeds, a new user's
personal cloud storage) used to go straight to the store and skip their
driver's validation [L1 §6 #9].  They now go through the same ``validate`` a
create does, with a context that grants nothing: no deployment gate is open
and no owner may send unattended.  A refusal is a ``ValueError`` carrying the
API's detail string, since there is no HTTP caller to answer.
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from orchestrator.services.connector_drivers.base import (
    ConnectorDraft,
    DeploymentGates,
    DriverEnvironment,
    NormalizedConnector,
    ValidationContext,
)
from orchestrator.services.connector_drivers.registry import ConnectorDriverRegistry


def _closed() -> bool:
    return False


async def _no_autonomous_send() -> bool:
    return False


def _refuse_mcp(_url: str | None, _credentials: dict[str, Any]) -> None:
    raise ValueError("SRW does not create MCP connectors itself")


PLATFORM_ENVIRONMENT = DriverEnvironment(
    gates=DeploymentGates(mcp_datasources_enabled=_closed),
    validate_mcp_datasource=_refuse_mcp,
)


async def validate_platform_connector(
    registry: ConnectorDriverRegistry,
    ds_type: str,
    *,
    name: str,
    connection_url: str | None,
    credentials: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
) -> NormalizedConnector:
    """What a platform row of ``ds_type`` stores, as its driver normalizes it."""
    driver = registry.for_type(ds_type)
    if driver is None:
        raise ValueError(f"No connector driver serves type {ds_type!r}")
    draft = ConnectorDraft(
        name=name,
        connection_url=connection_url,
        credentials=credentials,
        config=config,
        read_only=None,
        is_global=None,
        default_branch=None,
        supplied=frozenset({"name", "connection_url", "credentials", "config"}),
    )
    try:
        driver.require_enabled(PLATFORM_ENVIRONMENT.gates)
        await driver.prevalidate(draft, PLATFORM_ENVIRONMENT)
        return await driver.validate(
            draft,
            existing=None,
            ctx=ValidationContext(
                environment=PLATFORM_ENVIRONMENT,
                can_autonomous_send=_no_autonomous_send,
            ),
        )
    except HTTPException as exc:
        raise ValueError(str(exc.detail)) from exc
