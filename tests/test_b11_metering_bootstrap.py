"""R1.B11: the infrastructure-metering bootstrap moved out of the lifespan.

The block had no direct test while it lived inside startup. These cases pin
the decisions it makes from settings, schema capabilities and the audit tier;
statement-level parity with the base lifespan is recorded separately.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from orchestrator.services.infrastructure_metering import bootstrap
from orchestrator.services.infrastructure_metering.ingestion import (
    InfrastructureIngestionService,
)
from orchestrator.services.infrastructure_metering.inventory import InventoryStore


class _Capabilities(SimpleNamespace):
    """Schema capabilities: every slice absent unless named."""

    def __getattr__(self, item: str):
        if item.startswith("storage_identity_key"):
            return None
        return False

    def diagnostics(self) -> dict[str, str]:
        return {"probe": "stub"}


@pytest.fixture
def metering_env(monkeypatch):
    import os

    for key in list(os.environ):
        if key.startswith("INFRASTRUCTURE_METERING_"):
            monkeypatch.delenv(key)
    return monkeypatch


async def _bootstrap(monkeypatch, capabilities, *, audit_pool=None, identity=False):
    probe = AsyncMock(return_value=capabilities)
    monkeypatch.setattr(bootstrap, "probe_schema_capabilities", probe)
    reads: list[bool] = []

    def _identity() -> bool:
        reads.append(identity)
        return identity

    app_pool = object()
    result = await bootstrap.bootstrap_infrastructure_metering(
        app_pool=app_pool,
        audit_usage_pool=audit_pool,
        usage_ledger=object(),
        canonical_usage_rates=object(),
        lifecycle_identity_authenticated=_identity,
    )
    return result, probe, app_pool, reads


@pytest.mark.asyncio
async def test_a_dark_tier_builds_no_metering_collaborator(metering_env):
    partitions = AsyncMock()
    metering_env.setattr(bootstrap, "ensure_audit_partitions", partitions)
    result, probe, app_pool, reads = await _bootstrap(metering_env, _Capabilities())

    probe.assert_awaited_once_with(app_pool, None)
    partitions.assert_not_awaited()
    assert reads == [False]
    assert result.infrastructure_inventory_store is None
    assert result.infrastructure_ingestion_service is None
    assert result.infrastructure_metering_runtime is None
    assert result.infrastructure_workspace_cutover is None
    assert result.infrastructure_usage_materializer is None
    assert result.infrastructure_usage_day_sealer is None
    assert result.infrastructure_coverage_waivers is None
    assert result.infrastructure_usage_rollup is None
    assert result.infrastructure_storage_assets is None
    assert result.infrastructure_storage_mapping is None
    assert result.infrastructure_compute_activation is None
    assert result.infrastructure_compute_scope_diagnostics == {}
    assert result.infrastructure_durable_compute_activation_keys == frozenset()
    assert result.infrastructure_storage_source_activation_ready is False
    assert result.infrastructure_durable_reporting_policy_ready is True
    assert result.capabilities.slice1_inventory_ready is False


@pytest.mark.asyncio
async def test_audit_partition_preflight_failure_degrades_and_continues(
    metering_env, caplog
):
    audit_pool = object()
    metering_env.setattr(
        bootstrap,
        "ensure_audit_partitions",
        AsyncMock(side_effect=RuntimeError("partition DDL refused")),
    )
    caplog.set_level(logging.WARNING)
    result, probe, app_pool, _ = await _bootstrap(
        metering_env, _Capabilities(), audit_pool=audit_pool
    )

    bootstrap.ensure_audit_partitions.assert_awaited_once_with(audit_pool)
    probe.assert_awaited_once_with(app_pool, audit_pool)
    assert "Audit partition preflight failed" in caplog.text
    assert result.infrastructure_metering_runtime is None


def _collector_settings(monkeypatch) -> None:
    monkeypatch.setenv("INFRASTRUCTURE_METERING_COLLECTOR_ENABLED", "true")
    monkeypatch.setenv("INFRASTRUCTURE_METERING_STABLE_CLUSTER_ID", "dev-cluster")
    monkeypatch.setenv("INFRASTRUCTURE_METERING_NAMESPACE_ALLOWLIST", "srw")
    monkeypatch.setenv("INFRASTRUCTURE_METERING_INGESTION_KEY", "k" * 48)


@pytest.mark.asyncio
async def test_collector_with_inventory_schema_builds_store_and_ingestion(
    metering_env,
):
    _collector_settings(metering_env)
    metering_env.setattr(bootstrap, "ensure_audit_partitions", AsyncMock())
    result, _, _, _ = await _bootstrap(
        metering_env, _Capabilities(slice1_inventory_ready=True)
    )

    assert isinstance(result.infrastructure_inventory_store, InventoryStore)
    assert isinstance(
        result.infrastructure_ingestion_service, InfrastructureIngestionService
    )


@pytest.mark.asyncio
async def test_collector_without_inventory_schema_stays_unavailable(
    metering_env, caplog
):
    _collector_settings(metering_env)
    metering_env.setattr(bootstrap, "ensure_audit_partitions", AsyncMock())
    caplog.set_level(logging.ERROR)
    result, _, _, _ = await _bootstrap(metering_env, _Capabilities())

    assert result.infrastructure_inventory_store is None
    assert result.infrastructure_ingestion_service is None
    assert "Slice 1 Pod inventory" in caplog.text


@pytest.mark.asyncio
async def test_the_fencing_generation_advances_on_the_leader_session():
    conn = AsyncMock()
    conn.fetchval = AsyncMock(return_value=7)
    assert await bootstrap.allocate_metering_generation(conn) == 7
    sql = conn.fetchval.await_args.args[0]
    assert "leader_generation=leader_generation+1" in sql
    conn.fetchval = AsyncMock(return_value=None)
    with pytest.raises(RuntimeError, match="control row is missing"):
        await bootstrap.allocate_metering_generation(conn)
