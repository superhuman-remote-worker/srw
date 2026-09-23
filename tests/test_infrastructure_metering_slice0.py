from dataclasses import replace
from datetime import datetime, timedelta, timezone
import logging
from decimal import Decimal
import inspect
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from orchestrator.services.infrastructure_metering.capabilities import (
    REQUIRED_APP_INDEX_RELATIONS,
    REQUIRED_APP_TABLES,
    REQUIRED_APP_TRIGGER_RELATIONS,
    REQUIRED_APP_TRIGGERS,
    REQUIRED_AUDIT_INDEX_RELATIONS,
    REQUIRED_SLICE1_APP_INDEX_RELATIONS,
    REQUIRED_SLICE1_APP_TABLES,
    REQUIRED_SLICE1_APP_TRIGGER_RELATIONS,
    REQUIRED_SLICE1_APP_TRIGGERS,
    REQUIRED_SLICE1_RUNTIME_APP_COLUMNS,
    REQUIRED_SLICE1_RUNTIME_APP_CONSTRAINT_RELATIONS,
    REQUIRED_SLICE1_RUNTIME_APP_INDEX_RELATIONS,
    REQUIRED_SLICE1_RUNTIME_APP_TABLES,
    REQUIRED_SLICE1_RUNTIME_APP_TRIGGER_RELATIONS,
    REQUIRED_AUDIT_COLUMNS,
    REQUIRED_AUDIT_CONSTRAINTS,
    REQUIRED_AUDIT_INDEXES,
    REQUIRED_AUDIT_TABLES,
    MeteringSchemaCapabilities,
    probe_schema_capabilities,
)
from orchestrator.services.infrastructure_metering.config import (
    InfrastructureMeteringSettings,
)
from orchestrator.services.infrastructure_metering.queries import (
    UsageV2QueryService,
    UsageVisibility,
)
from orchestrator.services.infrastructure_metering.types import (
    UsageCoverageV2,
    UsageLedgerCostV2,
    UsageRowV2,
    UsageWindowV2,
    decimal_text,
    ledger_cost,
)
from orchestrator.schemas.infrastructure_admin import (
    InfrastructureComputeActivationRequest,
    InfrastructureComputeActivationScheduleRequest,
    InfrastructureCorrectionDeltaRequest,
    InfrastructureCorrectionRequest,
    InfrastructureCoverageWaiverRequest,
    InfrastructureCutoverPrepareRequest,
    InfrastructureStorageActivationRequest,
    InfrastructureStorageActivationScheduleRequest,
    InfrastructureStorageDestructionRequest,
)
from orchestrator.routers import infrastructure_admin as infrastructure_admin_routes
from orchestrator.routers import usage_reporting as usage_reporting_routes
from orchestrator.services import infrastructure_activation_policy as activation_policy
from orchestrator.services import infrastructure_admin as infrastructure_admin_service
from orchestrator.services import usage_reporting as usage_reporting_service
from orchestrator.services.infrastructure_metering.compute_activation import (
    ComputeActivation,
)
from orchestrator.services.infrastructure_metering.materializer import (
    StoragePublicationAuthority,
    StoragePublicationPolicy,
)
from orchestrator.services.infrastructure_metering.storage_assets import (
    StorageActivation,
    StorageSourceActivation,
    StorageSourceRequirementSpec,
)
from orchestrator.services.usage_ledger import UsageLedger, UsageRates


def _read_capabilities() -> MeteringSchemaCapabilities:
    return MeteringSchemaCapabilities(
        audit_tables=REQUIRED_AUDIT_TABLES,
        audit_columns=REQUIRED_AUDIT_COLUMNS,
        audit_constraints=REQUIRED_AUDIT_CONSTRAINTS,
        audit_indexes=REQUIRED_AUDIT_INDEXES,
        half_even_function=True,
        dirty_day_trigger=True,
        append_only_trigger=True,
    )


def _admin_dependencies(
    *,
    settings: InfrastructureMeteringSettings | None = None,
    admin: dict | None = None,
    audit: AsyncMock | None = None,
    require_admin: AsyncMock | None = None,
    leader_generation=None,
    scope_project_id=None,
    store=None,
    **operations,
) -> infrastructure_admin_routes.InfrastructureAdminDependencies:
    """Build the router bundle over explicit doubles, never module globals."""
    resolved = infrastructure_admin_service.InfrastructureAdminDependencies(
        store=MagicMock() if store is None else store,
        logger=logging.getLogger("test-infrastructure-admin"),
        settings=settings if settings is not None else InfrastructureMeteringSettings(),
        audit=audit if audit is not None else AsyncMock(),
        leader_generation=leader_generation or (lambda: 1),
        scope_project_id=scope_project_id or (lambda _user: None),
        **operations,
    )
    if require_admin is None:
        require_admin = AsyncMock(
            return_value=admin
            if admin is not None
            else {"id": str(uuid4()), "is_admin": True}
        )
    return infrastructure_admin_routes.InfrastructureAdminDependencies(
        operations=resolved,
        require_admin=require_admin,
    )


def _usage_dependencies(
    *,
    settings: InfrastructureMeteringSettings | None = None,
    require_approved_user: AsyncMock | None = None,
    scope_project_id=None,
    store=None,
    **reports,
) -> usage_reporting_routes.UsageReportingDependencies:
    """Build the usage router bundle over explicit doubles."""
    resolved_store = MagicMock() if store is None else store
    resolved = usage_reporting_service.UsageReportingDependencies(
        store=resolved_store,
        audit_reader=MagicMock(),
        logger=logging.getLogger("test-usage-reporting"),
        visible_project_ids=AsyncMock(return_value=[]),
        scope_project_id=scope_project_id or (lambda _user: None),
        **reports,
    )
    return usage_reporting_routes.UsageReportingDependencies(
        store=resolved_store,
        reports=resolved,
        require_admin=AsyncMock(),
        metering_settings=(
            settings if settings is not None else InfrastructureMeteringSettings()
        ),
        scope_project_id=scope_project_id or (lambda _user: None),
        require_approved_user=require_approved_user or AsyncMock(),
    )


def test_cutover_wiring_uses_configured_inventory_freshness() -> None:
    from orchestrator.services.infrastructure_metering import bootstrap

    source = inspect.getsource(bootstrap.bootstrap_infrastructure_metering)
    assert "max_scope_age=timedelta(" in source
    assert "seconds=infrastructure_metering_settings.stale_after_seconds" in source
    assert "max_collector_clock_skew=timedelta(" in source
    assert "infrastructure_metering_settings.max_collector_clock_skew_seconds" in source


def test_storage_publication_resources_require_effective_activation() -> None:
    from orchestrator.services.infrastructure_metering.storage_assets import (
        StorageActivation,
    )

    settings = InfrastructureMeteringSettings(
        collector_enabled=True,
        shadow_enabled=True,
        publication_enabled=True,
        pvc_inventory_enabled=True,
        pv_inventory_enabled=True,
        pvc_shadow_enabled=True,
        pv_shadow_enabled=True,
        pvc_publication_enabled=True,
        pv_publication_enabled=True,
        volume_identity_key_version="storage-v1",
        stable_cluster_id="dev-cluster",
        namespace_allowlist=("srw",),
    )
    capabilities = MagicMock(
        slice2_claim_inventory_ready=True,
        slice2_volume_inventory_ready=True,
        slice3_storage_lifecycle_ready=True,
        storage_identity_key_version="storage-v1",
    )
    boundary = datetime(2026, 8, 7, tzinfo=timezone.utc)
    before = StorageActivation(
        measurement_basis="claim-requested",
        state="active",
        activated_at=boundary,
        database_time=boundary - timedelta(microseconds=1),
    )
    effective_claim = StorageActivation(
        measurement_basis="claim-requested",
        state="active",
        activated_at=boundary,
        database_time=boundary,
    )
    effective_volume = StorageActivation(
        measurement_basis="volume-provisioned",
        state="active",
        activated_at=boundary,
        database_time=boundary + timedelta(hours=1),
    )
    before_source = StorageSourceActivation(
        measurement_basis="claim-requested",
        collector_id="kubernetes-pods",
        source_cluster="dev-cluster",
        state="active",
        activated_at=boundary,
        database_time=boundary - timedelta(microseconds=1),
    )
    effective_claim_source = StorageSourceActivation(
        measurement_basis="claim-requested",
        collector_id="kubernetes-pods",
        source_cluster="dev-cluster",
        state="active",
        activated_at=boundary,
        database_time=boundary,
    )
    effective_volume_source = StorageSourceActivation(
        measurement_basis="volume-provisioned",
        collector_id="kubernetes-pods",
        source_cluster="dev-cluster",
        state="active",
        activated_at=boundary,
        database_time=boundary + timedelta(hours=1),
    )
    requested_policy = activation_policy.requested_storage_publication_policy(settings)

    before_policy = activation_policy.capability_gated_storage_publication_policy(
        requested_policy,
        capabilities,
        claim_activation=before,
        volume_activation=effective_volume,
        source_activations={
            ("claim-requested", "kubernetes-pods", "dev-cluster"): before_source,
        },
        volume_mapping_ready=True,
        volume_identity_key_matches=True,
    )
    assert before_policy.authorities == ()
    enabled_policy = activation_policy.capability_gated_storage_publication_policy(
        requested_policy,
        capabilities,
        claim_activation=effective_claim,
        volume_activation=effective_volume,
        source_activations={
            (
                "claim-requested",
                "kubernetes-pods",
                "dev-cluster",
            ): effective_claim_source,
            (
                "volume-provisioned",
                "kubernetes-pods",
                "dev-cluster",
            ): effective_volume_source,
        },
        volume_mapping_ready=True,
        volume_identity_key_matches=True,
    )
    enabled = activation_policy.capability_gated_infrastructure_publication_resources(
        settings,
        capabilities,
        mapped_volume_resources=("block_volume_longhorn_ephemeral",),
        storage_publication_policy=enabled_policy,
    )
    assert enabled == (
        "workspace_pod",
        *activation_policy.INFRASTRUCTURE_PVC_RESOURCES,
        *activation_policy.INFRASTRUCTURE_PV_RESOURCES,
        "block_volume_longhorn_ephemeral",
    )
    capabilities.storage_identity_key_version = "another-key"
    assert activation_policy.capability_gated_infrastructure_publication_resources(
        settings,
        capabilities,
        mapped_volume_resources=("block_volume_longhorn_ephemeral",),
        storage_publication_policy=enabled_policy,
    ) == ("workspace_pod", *activation_policy.INFRASTRUCTURE_PVC_RESOURCES)


def test_durable_reporting_policy_survives_write_gate_and_auth_loss() -> None:
    boundary = datetime(2026, 8, 8, tzinfo=timezone.utc)
    before_boundary = boundary - timedelta(hours=1)
    claim = StorageActivation(
        measurement_basis="claim-requested",
        state="active",
        activated_at=boundary,
        database_time=before_boundary,
    )
    volume = StorageActivation(
        measurement_basis="volume-provisioned",
        state="active",
        activated_at=boundary,
        database_time=before_boundary,
    )
    sources = (
        StorageSourceActivation(
            measurement_basis="claim-requested",
            collector_id="kubernetes-pods",
            source_cluster="main-dev",
            state="active",
            activated_at=boundary,
            database_time=before_boundary,
        ),
        StorageSourceActivation(
            measurement_basis="volume-provisioned",
            collector_id="kubevirt-storage",
            source_cluster="vm-cluster",
            state="active",
            activated_at=boundary,
            database_time=before_boundary,
        ),
    )
    storage_policy = activation_policy.durable_storage_reporting_policy(
        claim_activation=claim,
        volume_activation=volume,
        source_activations=sources,
    )
    assert storage_policy.authorities == (
        StoragePublicationAuthority("claim-requested", "kubernetes-pods", "main-dev"),
        StoragePublicationAuthority(
            "volume-provisioned", "kubevirt-storage", "vm-cluster"
        ),
    )

    compute = {
        key: ComputeActivation(
            activation_key=key,
            state="active",
            activated_at=boundary,
            database_time=before_boundary,
        )
        for key in ("agent_pod", "ide_workspace_pod", "workspace_vm")
    }
    capabilities = MagicMock(
        slice3_compute_inventory_ready=True,
        slice3_storage_lifecycle_ready=True,
        slice2_volume_inventory_ready=True,
    )
    resources = activation_policy.durable_infrastructure_reporting_resources(
        capabilities,
        mapped_volume_resources=("block_volume_stackit",),
        compute_activations=compute,
        storage_reporting_policy=storage_policy,
    )
    assert resources == (
        "workspace_pod",
        "agent_pod",
        "workspace_vm",
        *activation_policy.INFRASTRUCTURE_PVC_RESOURCES,
        *activation_policy.INFRASTRUCTURE_PV_RESOURCES,
        "block_volume_stackit",
    )
    assert activation_policy.compute_activation_is_durable(compute["ide_workspace_pod"])

    # Current write controls remain independently dark and unauthenticated.
    settings = InfrastructureMeteringSettings()
    assert activation_policy.enabled_infrastructure_publication_resources(
        settings,
        mapped_volume_resources=("block_volume_stackit",),
    ) == ("workspace_pod",)
    assert (
        activation_policy.requested_storage_publication_policy(
            settings,
            vm_lifecycle_authenticated=False,
        ).authorities
        == ()
    )


def test_durable_volume_reporting_fails_closed_without_mapping_registry() -> None:
    capabilities = MagicMock(
        slice3_compute_inventory_ready=True,
        slice3_storage_lifecycle_ready=True,
        slice2_volume_inventory_ready=True,
    )
    policy = StoragePublicationPolicy(
        (
            StoragePublicationAuthority(
                "volume-provisioned", "kubernetes-pods", "main-dev"
            ),
        )
    )
    with pytest.raises(ValueError, match="mapping registry"):
        activation_policy.durable_infrastructure_reporting_resources(
            capabilities,
            volume_mapping_ready=False,
            storage_reporting_policy=policy,
        )


def test_storage_shadow_configuration_must_match_frozen_source_scopes() -> None:
    from orchestrator.services.infrastructure_metering.storage_assets import (
        StorageSourceRequirement,
    )

    settings = InfrastructureMeteringSettings(
        collector_enabled=True,
        shadow_enabled=True,
        pvc_inventory_enabled=True,
        pvc_shadow_enabled=True,
        stable_cluster_id="dev-cluster",
        namespace_allowlist=("srw",),
    )
    assert activation_policy.storage_source_configuration_errors(settings, ()) == (
        "primary/claim-requested durable source shadow activation",
    )

    activation = StorageSourceActivation(
        measurement_basis="claim-requested",
        collector_id="kubernetes-pods",
        source_cluster="dev-cluster",
        state="shadow",
        activated_at=None,
        requirements=(
            StorageSourceRequirement(
                inventory_scope_id=uuid4(),
                api_resource="core/v1/persistentvolumeclaims",
                namespace="srw",
                requirement_role="quantity",
            ),
        ),
        database_time=datetime(2026, 8, 7, tzinfo=timezone.utc),
    )
    assert (
        activation_policy.storage_source_configuration_errors(
            settings,
            (activation,),
        )
        == ()
    )

    expanded = replace(settings, namespace_allowlist=("srw", "new-namespace"))
    assert activation_policy.storage_source_configuration_errors(
        expanded,
        (activation,),
    ) == ("primary/claim-requested frozen inventory scope set",)


def _source_aware_capabilities(*, slice1: bool) -> MeteringSchemaCapabilities:
    return MeteringSchemaCapabilities(
        app_tables=(
            REQUIRED_APP_TABLES
            | (
                REQUIRED_SLICE1_APP_TABLES | REQUIRED_SLICE1_RUNTIME_APP_TABLES
                if slice1
                else frozenset()
            )
        ),
        app_indexes=(
            frozenset(REQUIRED_APP_INDEX_RELATIONS)
            | (
                frozenset(REQUIRED_SLICE1_APP_INDEX_RELATIONS)
                | frozenset(REQUIRED_SLICE1_RUNTIME_APP_INDEX_RELATIONS)
                if slice1
                else frozenset()
            )
        ),
        app_triggers=(
            REQUIRED_APP_TRIGGERS
            | (
                REQUIRED_SLICE1_APP_TRIGGERS
                | frozenset(REQUIRED_SLICE1_RUNTIME_APP_TRIGGER_RELATIONS)
                if slice1
                else frozenset()
            )
        ),
        app_columns=(REQUIRED_SLICE1_RUNTIME_APP_COLUMNS if slice1 else frozenset()),
        app_constraints=(
            frozenset(REQUIRED_SLICE1_RUNTIME_APP_CONSTRAINT_RELATIONS)
            if slice1
            else frozenset()
        ),
        audit_tables=REQUIRED_AUDIT_TABLES,
        audit_columns=REQUIRED_AUDIT_COLUMNS,
        audit_constraints=REQUIRED_AUDIT_CONSTRAINTS,
        audit_indexes=REQUIRED_AUDIT_INDEXES,
        app_seed_rows_ready=True,
        half_even_function=True,
        dirty_day_trigger=True,
        append_only_trigger=True,
        target_partitions_ready=True,
    )


class _CapabilityPool:
    def __init__(
        self,
        *,
        app: bool,
        append_mode: str = "O",
        slice1: bool = False,
        slice1_runtime: bool = False,
        unusable_indexes: set[str] | None = None,
        wrong_relation_indexes: set[str] | None = None,
    ):
        self.app = app
        self.append_mode = append_mode
        self.slice1 = slice1
        self.slice1_runtime = slice1_runtime
        self.unusable_indexes = unusable_indexes or set()
        self.wrong_relation_indexes = wrong_relation_indexes or set()

    async def fetch(self, sql, *params):
        wanted = set(params[0]) if params else set()
        if "information_schema.tables" in sql:
            present = REQUIRED_APP_TABLES if self.app else REQUIRED_AUDIT_TABLES
            if self.app and self.slice1:
                present |= REQUIRED_SLICE1_APP_TABLES
            if self.app and self.slice1_runtime:
                present |= REQUIRED_SLICE1_RUNTIME_APP_TABLES
            return [{"table_name": name} for name in wanted & present]
        if "FROM pg_catalog.pg_index" in sql:
            assert "index_state.indisvalid" in sql
            assert "index_state.indisready" in sql
            assert "index_state.indislive" in sql
            relations = (
                dict(REQUIRED_APP_INDEX_RELATIONS)
                if self.app
                else dict(REQUIRED_AUDIT_INDEX_RELATIONS)
            )
            if self.app and self.slice1:
                relations.update(REQUIRED_SLICE1_APP_INDEX_RELATIONS)
            if self.app and self.slice1_runtime:
                relations.update(REQUIRED_SLICE1_RUNTIME_APP_INDEX_RELATIONS)
            return [
                {
                    "indexname": name,
                    "tablename": (
                        "wrong_relation"
                        if name in self.wrong_relation_indexes
                        else relations[name]
                    ),
                }
                for name in wanted & set(relations) - self.unusable_indexes
            ]
        if "information_schema.columns" in sql:
            if self.app:
                if not self.slice1_runtime:
                    return []
                return [
                    {"table_name": table, "column_name": column}
                    for item in REQUIRED_SLICE1_RUNTIME_APP_COLUMNS
                    for table, column in [item.split(".", 1)]
                    if table in wanted
                ]
            return [{"column_name": name} for name in wanted & REQUIRED_AUDIT_COLUMNS]
        if "pg_constraint" in sql:
            if self.app:
                if not self.slice1_runtime:
                    return []
                return [
                    {"conname": name, "relname": relation}
                    for name, relation in (
                        REQUIRED_SLICE1_RUNTIME_APP_CONSTRAINT_RELATIONS.items()
                    )
                    if name in wanted
                ]
            return [{"conname": name} for name in wanted & REQUIRED_AUDIT_CONSTRAINTS]
        if "FROM pg_trigger" in sql:
            if self.app:
                relations = dict(REQUIRED_APP_TRIGGER_RELATIONS)
                if self.slice1:
                    relations.update(REQUIRED_SLICE1_APP_TRIGGER_RELATIONS)
                if self.slice1_runtime:
                    relations.update(REQUIRED_SLICE1_RUNTIME_APP_TRIGGER_RELATIONS)
                return [
                    {
                        "tgname": name,
                        "enabled": "O",
                        "relname": relations[name],
                    }
                    for name in wanted & set(relations)
                    if name in relations
                ]
            return [
                {
                    "tgname": "usage_events_rollup_dirty_days",
                    "enabled": "O",
                    "relname": "usage_events",
                },
                {
                    "tgname": "usage_events_append_only_v2",
                    "enabled": self.append_mode,
                    "relname": "usage_events",
                },
            ]
        raise AssertionError(f"unexpected capability fetch: {sql}")

    async def fetchval(self, sql, *_params):
        if "infra_metering_control" in sql:
            return True
        if "to_regprocedure" in sql or "WITH wanted AS" in sql:
            return True
        raise AssertionError(f"unexpected capability fetchval: {sql}")


@pytest.mark.asyncio
async def test_capability_probe_requires_normal_write_triggers_and_seed_rows():
    app = _CapabilityPool(app=True)
    disabled_append = _CapabilityPool(app=False, append_mode="D")
    capabilities = await probe_schema_capabilities(app, disabled_append)  # type: ignore[arg-type]
    assert not capabilities.append_only_trigger
    assert not capabilities.v2_reads_ready
    assert not capabilities.slice0_ready

    capabilities = await probe_schema_capabilities(
        app,
        _CapabilityPool(app=False, append_mode="A"),  # type: ignore[arg-type]
    )
    assert capabilities.append_only_trigger
    assert capabilities.app_seed_rows_ready
    assert capabilities.slice0_ready
    assert not capabilities.slice1_inventory_ready

    capabilities = await probe_schema_capabilities(
        _CapabilityPool(app=True, slice1=True),  # type: ignore[arg-type]
        None,
    )
    assert capabilities.slice1_inventory_ready
    assert not capabilities.slice0_ready


@pytest.mark.asyncio
async def test_capability_probe_separates_inventory_from_slice1_runtime():
    audit = _CapabilityPool(app=False, append_mode="A")
    inventory_only = await probe_schema_capabilities(
        _CapabilityPool(app=True, slice1=True),  # type: ignore[arg-type]
        audit,  # type: ignore[arg-type]
    )
    assert inventory_only.slice1_inventory_ready
    assert not inventory_only.slice1_runtime_ready
    assert "infra_usage_day_state.coverage_sequence" in (
        inventory_only.missing_slice1_runtime_app_columns
    )

    complete = await probe_schema_capabilities(
        _CapabilityPool(  # type: ignore[arg-type]
            app=True,
            slice1=True,
            slice1_runtime=True,
        ),
        audit,  # type: ignore[arg-type]
    )
    assert complete.slice1_runtime_ready


@pytest.mark.parametrize(
    "index_name",
    [
        "resource_publication_plans_period_idx",
        "resource_intervals_overlap_idx",
    ],
)
@pytest.mark.asyncio
async def test_capability_probe_rejects_unusable_concurrent_index(index_name):
    capabilities = await probe_schema_capabilities(
        _CapabilityPool(
            app=True,
            slice1=True,
            unusable_indexes={index_name},
        ),  # type: ignore[arg-type]
        None,
    )

    assert index_name in capabilities.missing_app_indexes
    assert not capabilities.slice1_inventory_ready


@pytest.mark.parametrize(
    "index_name",
    [
        "resource_inventory_snapshots_complete_received_idx",
        "resource_inventory_watch_events_invalid_received_idx",
    ],
)
@pytest.mark.asyncio
async def test_capability_probe_rejects_unusable_slice1_sealing_index(index_name):
    capabilities = await probe_schema_capabilities(
        _CapabilityPool(
            app=True,
            slice1=True,
            unusable_indexes={index_name},
        ),  # type: ignore[arg-type]
        None,
    )

    assert index_name in capabilities.missing_slice1_app_indexes
    assert not capabilities.slice1_inventory_ready


@pytest.mark.asyncio
async def test_capability_probe_rejects_expected_index_on_wrong_relation():
    index_name = "resource_intervals_overlap_idx"
    capabilities = await probe_schema_capabilities(
        _CapabilityPool(
            app=True,
            slice1=True,
            wrong_relation_indexes={index_name},
        ),  # type: ignore[arg-type]
        None,
    )

    assert index_name in capabilities.missing_app_indexes
    assert not capabilities.slice1_inventory_ready


def test_settings_are_off_by_default_and_publication_fails_closed():
    assert InfrastructureMeteringSettings.from_env({}) == (
        InfrastructureMeteringSettings()
    )

    with pytest.raises(ValueError, match="requires collector, shadow"):
        InfrastructureMeteringSettings.from_env(
            {"INFRASTRUCTURE_METERING_PUBLICATION_ENABLED": "true"}
        )

    with pytest.raises(ValueError, match="shadow mode requires"):
        InfrastructureMeteringSettings.from_env(
            {"INFRASTRUCTURE_METERING_SHADOW_ENABLED": "yes"}
        )


def test_settings_accept_only_explicit_boolean_values():
    with pytest.raises(ValueError, match="must be a boolean"):
        InfrastructureMeteringSettings.from_env(
            {"INFRASTRUCTURE_METERING_V2_READS_ENABLED": "sometimes"}
        )

    settings = InfrastructureMeteringSettings.from_env(
        {
            "INFRASTRUCTURE_METERING_COLLECTOR_ENABLED": "1",
            "INFRASTRUCTURE_METERING_SHADOW_ENABLED": "true",
            "INFRASTRUCTURE_METERING_PUBLICATION_ENABLED": "yes",
            "INFRASTRUCTURE_METERING_STABLE_CLUSTER_ID": " dev-cluster ",
            "INFRASTRUCTURE_METERING_NAMESPACE_ALLOWLIST": "srw",
        }
    )
    assert settings.publication_enabled
    assert settings.stable_cluster_id == "dev-cluster"

    with pytest.raises(ValueError, match="stable cluster id"):
        InfrastructureMeteringSettings.from_env(
            {"INFRASTRUCTURE_METERING_STABLE_CLUSTER_ID": "not a cluster/id"}
        )


def test_cutover_and_source_aware_read_gates_are_independent_and_fail_closed():
    with pytest.raises(ValueError, match="source-aware reads require v2 reads"):
        InfrastructureMeteringSettings.from_env(
            {"INFRASTRUCTURE_METERING_SOURCE_AWARE_READS_ENABLED": "true"}
        )

    with pytest.raises(ValueError, match="cutover requires collector, shadow"):
        InfrastructureMeteringSettings.from_env(
            {"INFRASTRUCTURE_METERING_CUTOVER_ENABLED": "true"}
        )

    settings = InfrastructureMeteringSettings.from_env(
        {
            "INFRASTRUCTURE_METERING_V2_READS_ENABLED": "true",
            "INFRASTRUCTURE_METERING_SOURCE_AWARE_READS_ENABLED": "true",
            "INFRASTRUCTURE_METERING_COLLECTOR_ENABLED": "true",
            "INFRASTRUCTURE_METERING_SHADOW_ENABLED": "true",
            "INFRASTRUCTURE_METERING_CUTOVER_ENABLED": "true",
            "INFRASTRUCTURE_METERING_STABLE_CLUSTER_ID": "dev-cluster",
            "INFRASTRUCTURE_METERING_NAMESPACE_ALLOWLIST": "srw",
        }
    )
    assert settings.source_aware_reads_enabled is True
    assert settings.cutover_enabled is True
    assert settings.publication_enabled is False


def test_slice2_storage_gates_are_independent_and_fail_closed():
    defaults = InfrastructureMeteringSettings.from_env({})
    assert defaults.pvc_inventory_enabled is False
    assert defaults.pv_inventory_enabled is False
    assert defaults.pvc_shadow_enabled is False
    assert defaults.pv_shadow_enabled is False
    assert defaults.pvc_publication_enabled is False
    assert defaults.pv_publication_enabled is False
    assert defaults.volume_identity_key_version == ""

    with pytest.raises(ValueError, match="PVC inventory requires its collector"):
        InfrastructureMeteringSettings.from_env(
            {"INFRASTRUCTURE_METERING_PVC_INVENTORY_ENABLED": "true"}
        )

    collector_env = {
        "INFRASTRUCTURE_METERING_COLLECTOR_ENABLED": "true",
        "INFRASTRUCTURE_METERING_STABLE_CLUSTER_ID": "dev-cluster",
        "INFRASTRUCTURE_METERING_NAMESPACE_ALLOWLIST": "srw",
    }
    with pytest.raises(ValueError, match="requires a volume identity key version"):
        InfrastructureMeteringSettings.from_env(
            {
                **collector_env,
                "INFRASTRUCTURE_METERING_PV_INVENTORY_ENABLED": "true",
            }
        )
    with pytest.raises(ValueError, match="volume identity key version must be"):
        InfrastructureMeteringSettings.from_env(
            {
                "INFRASTRUCTURE_METERING_VOLUME_IDENTITY_KEY_VERSION": "bad/key",
            }
        )
    with pytest.raises(ValueError, match="PVC shadow mode requires global shadow"):
        InfrastructureMeteringSettings.from_env(
            {
                **collector_env,
                "INFRASTRUCTURE_METERING_PVC_INVENTORY_ENABLED": "true",
                "INFRASTRUCTURE_METERING_PVC_SHADOW_ENABLED": "true",
            }
        )
    with pytest.raises(ValueError, match="PVC publication requires global publication"):
        InfrastructureMeteringSettings.from_env(
            {
                **collector_env,
                "INFRASTRUCTURE_METERING_SHADOW_ENABLED": "true",
                "INFRASTRUCTURE_METERING_PVC_INVENTORY_ENABLED": "true",
                "INFRASTRUCTURE_METERING_PVC_SHADOW_ENABLED": "true",
                "INFRASTRUCTURE_METERING_PVC_PUBLICATION_ENABLED": "true",
            }
        )

    settings = InfrastructureMeteringSettings.from_env(
        {
            **collector_env,
            "INFRASTRUCTURE_METERING_SHADOW_ENABLED": "true",
            "INFRASTRUCTURE_METERING_PUBLICATION_ENABLED": "true",
            "INFRASTRUCTURE_METERING_PVC_INVENTORY_ENABLED": "true",
            "INFRASTRUCTURE_METERING_PV_INVENTORY_ENABLED": "true",
            "INFRASTRUCTURE_METERING_PVC_SHADOW_ENABLED": "true",
            "INFRASTRUCTURE_METERING_PV_SHADOW_ENABLED": "true",
            "INFRASTRUCTURE_METERING_PVC_PUBLICATION_ENABLED": "true",
            "INFRASTRUCTURE_METERING_PV_PUBLICATION_ENABLED": "true",
            "INFRASTRUCTURE_METERING_VOLUME_IDENTITY_KEY_VERSION": "storage-v1",
            "INFRASTRUCTURE_METERING_VOLUME_RESOURCE_MAPPINGS_JSON": (
                '[{"mappingVersion":"longhorn-v1",'
                '"storageClass":"longhorn-ephemeral",'
                '"csiDriver":"driver.longhorn.io",'
                '"volumeMode":"filesystem",'
                '"resource":"block_volume_longhorn_ephemeral"}]'
            ),
        }
    )
    assert settings.pvc_publication_enabled is True
    assert settings.pv_publication_enabled is True
    assert settings.volume_identity_key_version == "storage-v1"
    assert settings.volume_resource_mappings[0].resource == (
        "block_volume_longhorn_ephemeral"
    )

    with pytest.raises(ValueError, match="contain exactly"):
        InfrastructureMeteringSettings.from_env(
            {
                **collector_env,
                "INFRASTRUCTURE_METERING_VOLUME_RESOURCE_MAPPINGS_JSON": (
                    '[{"mappingVersion":"v1","storageClass":"standard",'
                    '"csiDriver":"","volumeMode":"filesystem",'
                    '"resource":"block_volume_local","wildcard":"no"}]'
                ),
            }
        )


def test_vm_storage_dark_gates_are_independent_and_cannot_activate():
    defaults = InfrastructureMeteringSettings.from_env({})
    assert defaults.vm_pvc_inventory_enabled is False
    assert defaults.vm_pv_inventory_enabled is False
    assert defaults.vm_pvc_shadow_enabled is False
    assert defaults.vm_pv_shadow_enabled is False
    assert defaults.vm_pvc_publication_enabled is False
    assert defaults.vm_pv_publication_enabled is False
    assert defaults.vm_pv_cluster_wide_rbac_acknowledged is False

    base = {
        "INFRASTRUCTURE_METERING_COLLECTOR_ENABLED": "true",
        "INFRASTRUCTURE_METERING_STABLE_CLUSTER_ID": "dev-cluster",
        "INFRASTRUCTURE_METERING_NAMESPACE_ALLOWLIST": "srw",
        "INFRASTRUCTURE_METERING_VM_STABLE_CLUSTER_ID": "vm-cluster",
        "INFRASTRUCTURE_METERING_VM_NAMESPACE": "agent-vms",
    }
    with pytest.raises(ValueError, match="cluster-wide PV RBAC acknowledgement"):
        InfrastructureMeteringSettings.from_env(
            {
                **base,
                "INFRASTRUCTURE_METERING_VM_PV_INVENTORY_ENABLED": "true",
                "INFRASTRUCTURE_METERING_VOLUME_IDENTITY_KEY_VERSION": "storage-v1",
            }
        )
    with pytest.raises(ValueError, match="volume identity key version"):
        InfrastructureMeteringSettings.from_env(
            {
                **base,
                "INFRASTRUCTURE_METERING_VM_PV_INVENTORY_ENABLED": "true",
                "INFRASTRUCTURE_METERING_VM_PV_CLUSTER_WIDE_RBAC_ACKNOWLEDGED": "true",
            }
        )
    with pytest.raises(ValueError, match="VM PVC shadow mode requires global shadow"):
        InfrastructureMeteringSettings.from_env(
            {
                **base,
                "INFRASTRUCTURE_METERING_VM_PVC_INVENTORY_ENABLED": "true",
                "INFRASTRUCTURE_METERING_VM_PVC_SHADOW_ENABLED": "true",
            }
        )
    with pytest.raises(ValueError, match="VM PVC publication requires"):
        InfrastructureMeteringSettings.from_env(
            {
                **base,
                "INFRASTRUCTURE_METERING_SHADOW_ENABLED": "true",
                "INFRASTRUCTURE_METERING_VM_PVC_INVENTORY_ENABLED": "true",
                "INFRASTRUCTURE_METERING_VM_PVC_SHADOW_ENABLED": "true",
                "INFRASTRUCTURE_METERING_VM_PVC_PUBLICATION_ENABLED": "true",
            }
        )

    settings = InfrastructureMeteringSettings.from_env(
        {
            **base,
            "INFRASTRUCTURE_METERING_SHADOW_ENABLED": "true",
            "INFRASTRUCTURE_METERING_VM_PVC_INVENTORY_ENABLED": "true",
            "INFRASTRUCTURE_METERING_VM_PV_INVENTORY_ENABLED": "true",
            "INFRASTRUCTURE_METERING_VM_PVC_SHADOW_ENABLED": "true",
            "INFRASTRUCTURE_METERING_VM_PV_SHADOW_ENABLED": "true",
            "INFRASTRUCTURE_METERING_VM_PV_CLUSTER_WIDE_RBAC_ACKNOWLEDGED": "true",
            "INFRASTRUCTURE_METERING_VOLUME_IDENTITY_KEY_VERSION": "storage-v1",
        }
    )
    dependencies = _admin_dependencies(settings=settings).operations

    # The existing activation API intentionally remains local-cluster-only.
    # Remote dark gates cannot make either measurement basis schedulable.
    assert (
        infrastructure_admin_service.storage_basis_inventory_enabled(
            "claim-requested", dependencies=dependencies
        )
        is False
    )
    assert (
        infrastructure_admin_service.storage_basis_inventory_enabled(
            "volume-provisioned", dependencies=dependencies
        )
        is False
    )
    assert settings.pvc_publication_enabled is False
    assert settings.pv_publication_enabled is False


def test_slice3_compute_gates_are_independent_and_fail_closed():
    defaults = InfrastructureMeteringSettings.from_env({})
    assert defaults.ide_pod_shadow_enabled is False
    assert defaults.agent_pod_shadow_enabled is False
    assert defaults.ide_pod_publication_enabled is False
    assert defaults.agent_pod_publication_enabled is False
    assert defaults.vm_inventory_enabled is False
    assert defaults.vm_shadow_enabled is False
    assert defaults.vm_publication_enabled is False

    collector_env = {
        "INFRASTRUCTURE_METERING_COLLECTOR_ENABLED": "true",
        "INFRASTRUCTURE_METERING_STABLE_CLUSTER_ID": "dev-cluster",
        "INFRASTRUCTURE_METERING_NAMESPACE_ALLOWLIST": "srw",
    }
    with pytest.raises(ValueError, match="agent Pod shadow mode requires"):
        InfrastructureMeteringSettings.from_env(
            {
                **collector_env,
                "INFRASTRUCTURE_METERING_AGENT_POD_SHADOW_ENABLED": "true",
            }
        )
    with pytest.raises(ValueError, match="IDE Pod publication requires"):
        InfrastructureMeteringSettings.from_env(
            {
                **collector_env,
                "INFRASTRUCTURE_METERING_SHADOW_ENABLED": "true",
                "INFRASTRUCTURE_METERING_IDE_POD_SHADOW_ENABLED": "true",
                "INFRASTRUCTURE_METERING_IDE_POD_PUBLICATION_ENABLED": "true",
            }
        )
    with pytest.raises(ValueError, match="VM inventory requires VM stable"):
        InfrastructureMeteringSettings.from_env(
            {
                **collector_env,
                "INFRASTRUCTURE_METERING_VM_INVENTORY_ENABLED": "true",
            }
        )
    with pytest.raises(ValueError, match="VM namespace must be"):
        InfrastructureMeteringSettings.from_env(
            {"INFRASTRUCTURE_METERING_VM_NAMESPACE": "Not_Valid"}
        )

    settings = InfrastructureMeteringSettings.from_env(
        {
            **collector_env,
            "INFRASTRUCTURE_METERING_SHADOW_ENABLED": "true",
            "INFRASTRUCTURE_METERING_PUBLICATION_ENABLED": "true",
            "INFRASTRUCTURE_METERING_IDE_POD_SHADOW_ENABLED": "true",
            "INFRASTRUCTURE_METERING_AGENT_POD_SHADOW_ENABLED": "true",
            "INFRASTRUCTURE_METERING_IDE_POD_PUBLICATION_ENABLED": "true",
            "INFRASTRUCTURE_METERING_AGENT_POD_PUBLICATION_ENABLED": "true",
            "INFRASTRUCTURE_METERING_VM_INVENTORY_ENABLED": "true",
            "INFRASTRUCTURE_METERING_VM_SHADOW_ENABLED": "true",
            "INFRASTRUCTURE_METERING_VM_PUBLICATION_ENABLED": "true",
            "INFRASTRUCTURE_METERING_VM_STABLE_CLUSTER_ID": "vm-dev",
            "INFRASTRUCTURE_METERING_VM_NAMESPACE": "srw-vms",
        }
    )
    assert settings.agent_pod_publication_enabled is True
    assert settings.ide_pod_publication_enabled is True
    assert settings.vm_publication_enabled is True
    assert settings.vm_stable_cluster_id == "vm-dev"
    assert settings.vm_namespace == "srw-vms"


def test_slice1_collector_settings_are_bounded_and_fail_closed():
    settings = InfrastructureMeteringSettings.from_env(
        {
            "INFRASTRUCTURE_METERING_COLLECTOR_ENABLED": "true",
            "INFRASTRUCTURE_METERING_STABLE_CLUSTER_ID": "dev-cluster",
            "INFRASTRUCTURE_METERING_NAMESPACE_ALLOWLIST": "srw,agents,srw",
            "INFRASTRUCTURE_METERING_LIST_PAGE_SIZE": "250",
            "INFRASTRUCTURE_METERING_MAX_SNAPSHOT_ITEMS": "25000",
            "INFRASTRUCTURE_METERING_SNAPSHOT_ITEM_RETENTION_DAYS": "14",
            "INFRASTRUCTURE_METERING_DIAGNOSTIC_RETENTION_DAYS": "42",
            "INFRASTRUCTURE_METERING_CLEANUP_INTERVAL_SECONDS": "120",
            "INFRASTRUCTURE_METERING_MAX_COLLECTOR_CLOCK_SKEW_SECONDS": "45",
        }
    )
    assert settings.namespace_allowlist == ("srw", "agents")
    assert settings.list_page_size == 250
    assert settings.max_snapshot_items == 25_000
    assert settings.snapshot_item_retention_days == 14
    assert settings.diagnostic_retention_days == 42
    assert settings.cleanup_interval_seconds == 120
    assert settings.max_collector_clock_skew_seconds == 45

    with pytest.raises(ValueError, match="stable cluster id"):
        InfrastructureMeteringSettings.from_env(
            {"INFRASTRUCTURE_METERING_COLLECTOR_ENABLED": "true"}
        )
    with pytest.raises(ValueError, match="at least one namespace"):
        InfrastructureMeteringSettings.from_env(
            {
                "INFRASTRUCTURE_METERING_COLLECTOR_ENABLED": "true",
                "INFRASTRUCTURE_METERING_STABLE_CLUSTER_ID": "dev-cluster",
            }
        )
    with pytest.raises(ValueError, match="in-process collection is not implemented"):
        InfrastructureMeteringSettings.from_env(
            {
                "INFRASTRUCTURE_METERING_COLLECTOR_ENABLED": "true",
                "INFRASTRUCTURE_METERING_STABLE_CLUSTER_ID": "dev-cluster",
                "INFRASTRUCTURE_METERING_NAMESPACE_ALLOWLIST": "srw",
                "INFRASTRUCTURE_METERING_DEPLOYMENT_MODE": "in-process",
            }
        )
    with pytest.raises(ValueError, match="invalid Kubernetes namespaces"):
        InfrastructureMeteringSettings.from_env(
            {"INFRASTRUCTURE_METERING_NAMESPACE_ALLOWLIST": "srw,Not_Valid"}
        )
    with pytest.raises(ValueError, match="between 15 and 86400"):
        InfrastructureMeteringSettings.from_env(
            {"INFRASTRUCTURE_METERING_RELIST_INTERVAL_SECONDS": "1"}
        )
    with pytest.raises(ValueError, match="between 1 and 3600"):
        InfrastructureMeteringSettings.from_env(
            {"INFRASTRUCTURE_METERING_MAX_COLLECTOR_CLOCK_SKEW_SECONDS": "3601"}
        )
    with pytest.raises(ValueError, match="stale-after"):
        InfrastructureMeteringSettings.from_env(
            {
                "INFRASTRUCTURE_METERING_RELIST_INTERVAL_SECONDS": "600",
                "INFRASTRUCTURE_METERING_STALE_AFTER_SECONDS": "300",
            }
        )
    with pytest.raises(ValueError, match="diagnostic retention"):
        InfrastructureMeteringSettings.from_env(
            {
                "INFRASTRUCTURE_METERING_SNAPSHOT_ITEM_RETENTION_DAYS": "30",
                "INFRASTRUCTURE_METERING_DIAGNOSTIC_RETENTION_DAYS": "14",
            }
        )


def test_decimal_wire_values_are_exact_and_non_exponent():
    assert decimal_text(Decimal("12.500000")) == "12.5"
    assert decimal_text(Decimal("1E-18")) == "0.000000000000000001"
    assert decimal_text(Decimal("-0.000")) == "0"
    assert decimal_text(Decimal("0.0000000000000000005")) == "0"
    assert decimal_text(Decimal("1.0000000000000000015")) == "1.000000000000000002"
    assert (
        decimal_text(Decimal("99999999999999999999.999999999999999999"))
        == "99999999999999999999.999999999999999999"
    )

    with pytest.raises(ValueError, match=r"NUMERIC\(38,18\)"):
        decimal_text(Decimal("100000000000000000000"))


@pytest.mark.parametrize("value", [0.1, True, "", "NaN", "Infinity"])
def test_decimal_wire_values_reject_inexact_or_non_finite_inputs(value):
    with pytest.raises(ValueError):
        decimal_text(value)


def test_cost_coverage_distinguishes_free_unpriced_and_partial():
    free = ledger_cost(amount=0, priced_quantity="4", unpriced_quantity="0")
    assert free.status == "priced"
    assert free.amount == "0"

    unpriced = ledger_cost(amount=None, priced_quantity="0", unpriced_quantity="4")
    assert unpriced.status == "unpriced"
    assert unpriced.amount is None

    partial = ledger_cost(amount="1.25", priced_quantity="2", unpriced_quantity="2")
    assert partial.status == "partially-priced"
    assert partial.amount == "1.25"

    zero_quantity_unpriced = ledger_cost(
        amount=None,
        priced_quantity="0",
        unpriced_quantity="0",
        priced_events=0,
        unpriced_events=1,
    )
    assert zero_quantity_unpriced.status == "unpriced"

    corrected = ledger_cost(
        amount="1",
        priced_quantity="4",
        unpriced_quantity="0",
        priced_events=1,
        unpriced_events=2,
    )
    assert corrected.status == "priced"

    empty = ledger_cost(amount=None, priced_quantity="0", unpriced_quantity="0")
    assert empty.status == "priced"
    assert empty.amount == "0"

    with pytest.raises(ValidationError, match="requires an amount"):
        ledger_cost(amount=None, priced_quantity="1", unpriced_quantity="0")


def test_typed_contracts_reject_impossible_cross_field_states():
    with pytest.raises(ValidationError, match="requires an amount"):
        UsageLedgerCostV2(
            status="priced",
            amount=None,
            priced_quantity="1",
            unpriced_quantity="0",
        )

    with pytest.raises(ValidationError, match="ledger quantity buckets"):
        UsageRowV2(
            category="compute",
            measurement_basis="scheduler-request",
            cost_domain="workload-allocation",
            resource_class="kubernetes-pod",
            measurement_algorithm="fixture-v1",
            resource="workspace_pod",
            unit="vcpu-hour",
            attribution_scope="customer",
            quantity="2",
            finalized_quantity="1",
            confirmed_provisional_quantity="1",
            ledger_cost=ledger_cost(
                amount=None, priced_quantity="0", unpriced_quantity="1"
            ),
            events=1,
        )

    now = datetime(2026, 8, 5, tzinfo=timezone.utc)
    with pytest.raises(ValidationError, match="end must be after start"):
        UsageWindowV2(start=now, end=now, as_of=now, data_through=None)
    with pytest.raises(ValidationError, match="cannot exceed"):
        UsageCoverageV2(
            status="partial",
            includes_provisional=False,
            required_sources_ok=2,
            required_sources_total=1,
        )


def test_query_adapter_derives_total_from_quantized_pricing_buckets():
    row = UsageV2QueryService._row(
        {
            "category": "compute",
            "measurement_basis": "scheduler-request",
            "cost_domain": "workload-allocation",
            "resource_class": "kubernetes-pod",
            "measurement_algorithm": "legacy-end-stamped-v1",
            "resource": "workspace_pod",
            "unit": "vcpu-hour",
            "attribution_scope": "customer",
            "quantity": Decimal("0.000000000000000001"),
            "cost_usd": Decimal("0"),
            "priced_quantity": Decimal("0.0000000000000000005"),
            "unpriced_quantity": Decimal("0.0000000000000000005"),
            "priced_events": 1,
            "unpriced_events": 1,
            "events": 2,
        }
    )

    assert row.quantity == "0"
    assert row.ledger_cost.priced_quantity == "0"
    assert row.ledger_cost.unpriced_quantity == "0"


def test_usage_row_rejects_an_unknown_typed_dimension():
    with pytest.raises(ValidationError):
        UsageRowV2(
            category="compute",
            measurement_basis="made-up",
            cost_domain="workload-allocation",
            resource_class="kubernetes-pod",
            measurement_algorithm="fixture-v1",
            resource="workspace_pod",
            unit="vcpu-hour",
            attribution_scope="customer",
            quantity="1",
            finalized_quantity="1",
            confirmed_provisional_quantity="0",
            ledger_cost=ledger_cost(
                amount=None, priced_quantity="0", unpriced_quantity="1"
            ),
            events=1,
        )


class _AuditPool:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    async def fetch(self, sql, *params):
        self.calls.append((sql, params))
        return self.rows


class _LedgerPool:
    def __init__(self):
        self.calls = []

    def acquire(self):
        pool = self

        class _Acquire:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

            async def fetch(self, sql, *params):
                pool.calls.append((sql, params))
                return []

        return _Acquire()


@pytest.mark.asyncio
async def test_v2_summary_keeps_cpu_and_memory_as_separate_decimal_rows():
    audit = _AuditPool(
        [
            {
                "category": "compute",
                "measurement_basis": "scheduler-request",
                "cost_domain": "workload-allocation",
                "resource_class": "kubernetes-pod",
                "measurement_algorithm": "legacy-end-stamped-v1",
                "resource": "workspace_pod",
                "unit": "vcpu-hour",
                "attribution_scope": "customer",
                "quantity": Decimal("8.000000"),
                "cost_usd": None,
                "priced_quantity": Decimal("0"),
                "unpriced_quantity": Decimal("8"),
                "priced_events": 0,
                "unpriced_events": 1,
                "events": 1,
            },
            {
                "category": "compute",
                "measurement_basis": "scheduler-request",
                "cost_domain": "workload-allocation",
                "resource_class": "kubernetes-pod",
                "measurement_algorithm": "legacy-end-stamped-v1",
                "resource": "workspace_pod",
                "unit": "gib-hour",
                "attribution_scope": "customer",
                "quantity": Decimal("16.000000"),
                "cost_usd": Decimal("0"),
                "priced_quantity": Decimal("16"),
                "unpriced_quantity": Decimal("0"),
                "priced_events": 1,
                "unpriced_events": 0,
                "events": 1,
            },
        ]
    )
    service = UsageV2QueryService(audit, _read_capabilities())
    start = datetime(2026, 8, 5, tzinfo=timezone.utc)
    end = datetime(2026, 8, 6, tzinfo=timezone.utc)
    user_id = str(uuid4())

    result = await service.summary(
        from_ts=start,
        to_ts=end,
        visibility=UsageVisibility(owner_user_id=user_id),
        as_of=end,
    )

    assert [(row.unit, row.quantity) for row in result.rows] == [
        ("vcpu-hour", "8"),
        ("gib-hour", "16"),
    ]
    assert result.rows[0].ledger_cost.status == "unpriced"
    assert result.rows[1].ledger_cost.status == "priced"
    assert result.rows[1].ledger_cost.amount == "0"
    assert result.coverage.status == "partial"
    assert "live-resource-inventory" in result.coverage.excluded_domains

    sql, params = audit.calls[0]
    assert "period_start IS NULL AND ts >= $1 AND ts < $2" in sql
    assert "EXTRACT(EPOCH" not in sql
    assert "attribution_scope = 'customer'" in sql
    assert params[:3] == (start, end, False)
    assert params[3].hex == user_id.replace("-", "")
    assert result.window.data_through is None
    assert "typed-infrastructure-intervals" in result.coverage.excluded_domains


@pytest.mark.asyncio
async def test_v2_project_scope_narrows_the_identity_visibility_union():
    audit = _AuditPool([])
    service = UsageV2QueryService(
        audit,
        _read_capabilities(),
    )
    start = datetime(2026, 8, 5, tzinfo=timezone.utc)
    end = datetime(2026, 8, 6, tzinfo=timezone.utc)
    owner_id = str(uuid4())
    visible_project_id = str(uuid4())
    scope_project_id = str(uuid4())

    await service.summary(
        from_ts=start,
        to_ts=end,
        visibility=UsageVisibility(
            owner_user_id=owner_id,
            visible_project_ids=(visible_project_id,),
            scope_project_id=scope_project_id,
        ),
        as_of=end,
    )

    sql, params = audit.calls[0]
    assert "user_id = $4" in sql
    assert "project_id = ANY($5::uuid[])" in sql
    assert "project_id = $6" in sql
    assert params[3] == UUID(owner_id)
    assert params[4] == [UUID(visible_project_id)]
    assert params[5] == UUID(scope_project_id)


@pytest.mark.asyncio
async def test_v1_queries_are_frozen_to_llm_and_workspace_cpu_memory():
    pool = _LedgerPool()
    ledger = UsageLedger(pool, UsageRates(None))
    start = datetime(2026, 8, 5, tzinfo=timezone.utc)
    end = datetime(2026, 8, 6, tzinfo=timezone.utc)

    await ledger.query_usage(from_ts=start, to_ts=end)
    await ledger.query_grouped(from_ts=start, to_ts=end, group_by="model")

    usage_sql = pool.calls[0][0]
    model_sql = pool.calls[1][0]
    assert "category IN ('llm', 'tts', 'stt')" in usage_sql
    assert "resource = 'workspace_pod'" in usage_sql
    assert "unit IN ('vcpu-hour', 'gib-hour')" in usage_sql
    assert "category = 'llm'" in model_sql


@pytest.mark.asyncio
async def test_v1_project_scope_narrows_summary_and_strict_self_views():
    pool = _LedgerPool()
    ledger = UsageLedger(pool, UsageRates(None))
    start = datetime(2026, 8, 5, tzinfo=timezone.utc)
    end = datetime(2026, 8, 6, tzinfo=timezone.utc)
    owner_id = str(uuid4())
    visible_project_id = str(uuid4())
    scope_project_id = str(uuid4())

    await ledger.query_usage(
        from_ts=start,
        to_ts=end,
        owner_user_id=owner_id,
        visible_project_ids=(visible_project_id,),
        scope_project_id=scope_project_id,
    )
    await ledger.query_grouped(
        from_ts=start,
        to_ts=end,
        group_by="user",
        owner_user_id=owner_id,
        scope_project_id=scope_project_id,
    )

    summary_sql, summary_params = pool.calls[0]
    breakdown_sql, breakdown_params = pool.calls[1]
    assert "user_id = $3" in summary_sql
    assert "project_id = ANY($4::uuid[])" in summary_sql
    assert "project_id = $5" in summary_sql
    assert summary_params[2] == UUID(owner_id)
    assert summary_params[3] == [UUID(visible_project_id)]
    assert summary_params[4] == UUID(scope_project_id)
    assert "user_id = $3" in breakdown_sql
    assert "project_id = $4" in breakdown_sql
    assert breakdown_params[2] == UUID(owner_id)
    assert breakdown_params[3] == UUID(scope_project_id)


@pytest.mark.asyncio
async def test_v2_service_refuses_reads_without_audit_capability():
    service = UsageV2QueryService(_AuditPool([]), MeteringSchemaCapabilities())
    now = datetime.now(timezone.utc)
    with pytest.raises(RuntimeError, match="schema is unavailable"):
        await service.summary(
            from_ts=now,
            to_ts=now.replace(year=now.year + 1),
            visibility=UsageVisibility(),
        )


def test_source_aware_service_requires_both_slice0_and_slice1_readiness():
    audit = _AuditPool([])
    app = MagicMock()
    slice0_only = UsageV2QueryService(
        audit,
        _source_aware_capabilities(slice1=False),
        app,
        source_aware_reads_enabled=True,
    )
    ready = UsageV2QueryService(
        audit,
        _source_aware_capabilities(slice1=True),
        app,
        source_aware_reads_enabled=True,
    )
    missing_app_pool = UsageV2QueryService(
        audit,
        _source_aware_capabilities(slice1=True),
        source_aware_reads_enabled=True,
    )

    assert slice0_only.source_aware_reads_enabled is True
    assert slice0_only.is_available is False
    assert ready.is_available is True
    assert missing_app_pool.is_available is False


def test_legacy_v2_readiness_is_unchanged_while_source_aware_gate_is_off():
    service = UsageV2QueryService(
        _AuditPool([]),
        _read_capabilities(),
        source_aware_reads_enabled=False,
    )

    assert service.source_aware_reads_enabled is False
    assert service.is_available is True


@pytest.mark.asyncio
async def test_usage_v2_route_is_hidden_while_its_gate_is_off():
    auth = AsyncMock()
    dependencies = _usage_dependencies(
        settings=InfrastructureMeteringSettings(),
        require_approved_user=auth,
    )

    with pytest.raises(HTTPException) as raised:
        await usage_reporting_routes.get_usage_v2(
            MagicMock(),
            days=30,
            from_date=None,
            to_date=None,
            ref_id=None,
            include_non_customer=False,
            dependencies=dependencies,
        )

    assert raised.value.status_code == 404
    auth.assert_not_awaited()


@pytest.mark.asyncio
async def test_usage_v2_restricts_non_customer_rows_to_fleet_admin():
    dependencies = _usage_dependencies(
        settings=InfrastructureMeteringSettings(v2_reads_enabled=True),
        require_approved_user=AsyncMock(
            return_value={"id": str(uuid4()), "is_admin": False, "scopes": []}
        ),
    )

    with pytest.raises(HTTPException) as raised:
        await usage_reporting_routes.get_usage_v2(
            MagicMock(),
            days=30,
            from_date=None,
            to_date=None,
            ref_id=None,
            include_non_customer=True,
            dependencies=dependencies,
        )

    assert raised.value.status_code == 403


@pytest.mark.asyncio
async def test_usage_v2_fleet_admin_passes_explicit_visibility():
    now = datetime(2026, 8, 6, tzinfo=timezone.utc)
    response = UsageV2QueryService._row(
        {
            "category": "compute",
            "measurement_basis": "scheduler-request",
            "cost_domain": "workload-allocation",
            "resource_class": "kubernetes-pod",
            "measurement_algorithm": "fixture-v1",
            "resource": "workspace_pod",
            "unit": "vcpu-hour",
            "attribution_scope": "customer",
            "quantity": Decimal("1"),
            "cost_usd": None,
            "priced_quantity": Decimal("0"),
            "unpriced_quantity": Decimal("1"),
            "priced_events": 0,
            "unpriced_events": 1,
            "events": 1,
        }
    )
    summary = {
        "schema_version": 2,
        "window": {
            "start": now,
            "end": now.replace(day=7),
            "as_of": now,
            "data_through": now,
        },
        "rows": [response],
        "coverage": {
            "status": "partial",
            "includes_provisional": False,
            "required_sources_ok": 0,
            "required_sources_total": 0,
            "unknown_ranges": [],
            "excluded_domains": [],
        },
    }

    class _Service:
        is_available = True

        def __init__(self):
            self.kwargs = None

        async def summary(self, **kwargs):
            self.kwargs = kwargs
            return summary

    service = _Service()

    class _Rollup:
        async def bootstrap_state(self):
            return MagicMock(read_ready=True)

    dependencies = _usage_dependencies(
        settings=InfrastructureMeteringSettings(v2_reads_enabled=True),
        infrastructure_usage_v2=service,
        infrastructure_usage_rollup=_Rollup(),
        require_approved_user=AsyncMock(
            return_value={"id": str(uuid4()), "is_admin": True, "scopes": []}
        ),
    )

    result = await usage_reporting_routes.get_usage_v2(
        MagicMock(),
        days=1,
        from_date="2026-08-05T00:00:00Z",
        to_date="2026-08-06T00:00:00Z",
        ref_id=None,
        include_non_customer=True,
        dependencies=dependencies,
    )

    assert result == summary
    assert service.kwargs["visibility"].include_non_customer is True


@pytest.mark.asyncio
async def test_usage_v2_refuses_reads_until_bootstrap_is_complete():
    class _Service:
        is_available = True

    class _Rollup:
        async def bootstrap_state(self):
            return MagicMock(read_ready=False)

    dependencies = _usage_dependencies(
        settings=InfrastructureMeteringSettings(v2_reads_enabled=True),
        infrastructure_usage_v2=_Service(),
        infrastructure_usage_rollup=_Rollup(),
        require_approved_user=AsyncMock(
            return_value={"id": str(uuid4()), "is_admin": True, "scopes": []}
        ),
    )

    with pytest.raises(HTTPException) as raised:
        await usage_reporting_routes.get_usage_v2(
            MagicMock(),
            days=1,
            from_date="2026-08-05T00:00:00Z",
            to_date="2026-08-06T00:00:00Z",
            ref_id=None,
            include_non_customer=False,
            dependencies=dependencies,
        )

    assert raised.value.status_code == 503
    assert "bootstrap incomplete" in raised.value.detail


@pytest.mark.asyncio
async def test_usage_v2_does_not_echo_server_contract_failures_as_client_errors():
    class _Service:
        is_available = True

        async def summary(self, **_kwargs):
            raise ValueError("sensitive-invalid-ledger-value")

    class _Rollup:
        async def bootstrap_state(self):
            return MagicMock(read_ready=True)

    dependencies = _usage_dependencies(
        settings=InfrastructureMeteringSettings(v2_reads_enabled=True),
        infrastructure_usage_v2=_Service(),
        infrastructure_usage_rollup=_Rollup(),
        require_approved_user=AsyncMock(
            return_value={"id": str(uuid4()), "is_admin": True, "scopes": []}
        ),
    )

    with pytest.raises(HTTPException) as raised:
        await usage_reporting_routes.get_usage_v2(
            MagicMock(),
            days=1,
            from_date="2026-08-05T00:00:00Z",
            to_date="2026-08-06T00:00:00Z",
            ref_id=None,
            include_non_customer=False,
            dependencies=dependencies,
        )

    assert raised.value.status_code == 500
    assert raised.value.detail == "Usage API v2 query failed"


def test_internal_inventory_ingestion_routes_are_hidden_from_openapi():
    import orchestrator.main as orchestrator_main
    from tests._route_inventory import mounted_route_objects

    prefix = "/api/internal/infrastructure-metering/v1"
    expected = {
        f"{prefix}/tickets",
        f"{prefix}/snapshots/begin",
        f"{prefix}/snapshots/items",
        f"{prefix}/snapshots/finalize",
        f"{prefix}/watch/apply",
        f"{prefix}/watch/finish",
    }
    routes = {
        route.path: route
        for route in mounted_route_objects(orchestrator_main.app)
        if route.path in expected
    }

    assert set(routes) == expected
    assert all(route.methods == {"POST"} for route in routes.values())
    assert all(not route.include_in_schema for route in routes.values())


@pytest.mark.asyncio
async def test_infrastructure_admin_operations_require_real_fleet_view():
    """A view-as-user admin and a project-scoped MCP admin are both refused."""

    request = MagicMock()
    request.url.path = "/api/admin/usage/v2/infrastructure-cutover"
    audit = AsyncMock()

    shadowed = _admin_dependencies(
        audit=audit,
        admin={"id": str(uuid4()), "real_is_admin": True, "is_admin": False},
    )
    with pytest.raises(HTTPException) as raised:
        await infrastructure_admin_routes._require_infrastructure_fleet_admin(
            request, shadowed
        )

    assert raised.value.status_code == 403
    assert audit.await_args.kwargs["event_type"] == "admin_denied"

    scoped_admin = _admin_dependencies(
        audit=audit,
        admin={"id": str(uuid4()), "real_is_admin": True, "is_admin": True},
        scope_project_id=lambda _user: uuid4(),
    )
    with pytest.raises(HTTPException) as scoped:
        await infrastructure_admin_routes._require_infrastructure_fleet_admin(
            request, scoped_admin
        )
    assert scoped.value.status_code == 403
    assert audit.await_count == 2


@pytest.mark.asyncio
async def test_cutover_prepare_is_explicit_gated_idempotent_admin_operation():
    from orchestrator.services.infrastructure_metering.cutover import (
        CutoverPhase,
        CutoverStatus,
    )

    actor_id, request_id = uuid4(), uuid4()
    request = MagicMock()
    request.url.path = "/api/admin/usage/v2/infrastructure-cutover/prepare"
    status = CutoverStatus(
        state="preparing",
        phase=CutoverPhase.LEGACY_DRAINING,
        leader_generation=9,
        cutover_at=datetime(2026, 8, 6, 12, tzinfo=timezone.utc),
        request_id=request_id,
        actor_id=actor_id,
        reason="reviewed shadow window",
        unplanned_intervals=1,
        planned=0,
        published=0,
        conflicts=0,
        open_legacy_intervals=0,
        cutover_error=None,
    )
    coordinator = MagicMock()
    coordinator.prepare = AsyncMock(return_value=status)
    audit = AsyncMock()
    dependencies = _admin_dependencies(
        settings=InfrastructureMeteringSettings(cutover_enabled=True),
        admin={"id": str(actor_id), "is_admin": True},
        audit=audit,
        leader_generation=lambda: 9,
        durable_reporting_policy_ready=True,
        workspace_cutover=coordinator,
    )

    result = await infrastructure_admin_routes.prepare_infrastructure_metering_cutover(
        request,
        InfrastructureCutoverPrepareRequest(
            idempotency_key=request_id,
            reason="reviewed shadow window",
        ),
        dependencies=dependencies,
    )

    coordinator.prepare.assert_awaited_once_with(
        9,
        actor_id=actor_id,
        reason="reviewed shadow window",
        idempotency_key=request_id,
    )
    assert result["phase"] == "legacy-draining"
    assert result["request_id"] == request_id
    assert audit.await_args.kwargs["event_type"] == (
        "infrastructure_metering_cutover_prepared"
    )


@pytest.mark.asyncio
async def test_coverage_waiver_route_maps_result_and_audits():
    from orchestrator.services.infrastructure_metering.coverage import (
        CoverageDayDegradation,
        CoverageGapWaiverResult,
    )

    actor_id, gap_id, request_id = uuid4(), uuid4(), uuid4()
    resolved_at = datetime(2026, 8, 6, 12, tzinfo=timezone.utc)
    request = MagicMock()
    request.url.path = f"/api/admin/usage/v2/coverage-gaps/{gap_id}/waive"
    service = MagicMock()
    service.waive = AsyncMock(
        return_value=CoverageGapWaiverResult(
            gap_id=gap_id,
            actor_id=actor_id,
            idempotency_key=request_id,
            reason="durable journal unavailable",
            resolved_at=resolved_at,
            replayed=False,
            degraded_days=(
                CoverageDayDegradation(
                    day=resolved_at.date(),
                    coverage_sequence=2,
                    coverage_revision="waiver-v1:revision",
                    added_range=(resolved_at, resolved_at + timedelta(minutes=5)),
                ),
            ),
        )
    )
    audit = AsyncMock()
    dependencies = _admin_dependencies(
        admin={"id": str(actor_id), "is_admin": True},
        audit=audit,
        coverage_waivers=service,
    )

    route = infrastructure_admin_routes.waive_infrastructure_metering_coverage_gap
    result = await route(
        request,
        gap_id,
        InfrastructureCoverageWaiverRequest(
            idempotency_key=request_id,
            reason="durable journal unavailable",
        ),
        dependencies=dependencies,
    )

    service.waive.assert_awaited_once_with(
        gap_id,
        actor_id,
        "durable journal unavailable",
        request_id,
    )
    assert result["degraded_days"][0]["coverage_sequence"] == 2
    assert audit.await_args.kwargs["event_type"] == (
        "infrastructure_metering_coverage_gap_waived"
    )


@pytest.mark.asyncio
async def test_correction_route_is_idempotent_fleet_admin_operation():
    actor_id, correction_id = uuid4(), uuid4()
    period_start = datetime(2026, 8, 5, tzinfo=timezone.utc)
    period_end = period_start + timedelta(hours=1)
    request = MagicMock()
    request.url.path = "/api/admin/usage/v2/infrastructure-corrections"
    plan = MagicMock(
        id=correction_id,
        correction_group_id=correction_id,
        state="planned",
        plan_revision=2,
        period_start=period_start,
        period_end=period_end,
        events=(object(), object()),
        event_set_hash="a" * 64,
        rate_selection_hash="b" * 64,
    )
    materializer = MagicMock()
    materializer.create_correction = AsyncMock(return_value=plan)
    audit = AsyncMock()
    dependencies = _admin_dependencies(
        settings=InfrastructureMeteringSettings(
            collector_enabled=True,
            shadow_enabled=True,
            publication_enabled=True,
            stable_cluster_id="dev-cluster",
            namespace_allowlist=("srw",),
        ),
        admin={"id": str(actor_id), "is_admin": True},
        audit=audit,
        leader_generation=lambda: 11,
        usage_materializer=materializer,
    )
    original_ts = datetime(2026, 8, 5, tzinfo=timezone.utc)

    route = infrastructure_admin_routes.create_infrastructure_metering_correction
    result = await route(
        request,
        InfrastructureCorrectionRequest(
            idempotency_key=correction_id,
            reason="reviewed attribution repair",
            deltas=[
                InfrastructureCorrectionDeltaRequest(
                    source="infra-allocation-v2",
                    source_id="original-source-id",
                    unit="vcpu-hour",
                    ts=original_ts,
                    expected_payload_hash="c" * 64,
                    quantity=Decimal("-4"),
                ),
                InfrastructureCorrectionDeltaRequest(
                    source="infra-allocation-v2",
                    source_id="original-source-id",
                    unit="vcpu-hour",
                    ts=original_ts,
                    expected_payload_hash="c" * 64,
                    quantity=Decimal("4"),
                    payload_overrides={"user_id": str(uuid4())},
                ),
            ],
        ),
        dependencies=dependencies,
    )

    call = materializer.create_correction.await_args
    assert call.args[0] == 11
    assert [delta.quantity for delta in call.args[1]] == [Decimal("-4"), Decimal("4")]
    assert call.kwargs == {
        "correction_reason": "reviewed attribution repair",
        "correction_actor_id": actor_id,
        "correction_id": correction_id,
    }
    assert result["plan_id"] == correction_id
    assert result["event_count"] == 2
    assert audit.await_args.kwargs["event_type"] == (
        "infrastructure_metering_correction_reviewed"
    )


def test_infrastructure_admin_routes_are_explicit_and_publicly_documented():
    import orchestrator.main as orchestrator_main
    from tests._route_inventory import mounted_route_objects

    expected = {
        "/api/admin/usage/v2/infrastructure-cutover": {"GET"},
        "/api/admin/usage/v2/infrastructure-cutover/prepare": {"POST"},
        "/api/admin/usage/v2/coverage-gaps/{gap_id}/waive": {"POST"},
        "/api/admin/usage/v2/infrastructure-corrections": {"POST"},
        "/api/admin/usage/v2/storage-activation": {"GET"},
        "/api/admin/usage/v2/storage-activation/{measurement_basis}/shadow": {"POST"},
        "/api/admin/usage/v2/storage-activation/{measurement_basis}/schedule": {"POST"},
        (
            "/api/admin/usage/v2/storage-source-activation/"
            "{source}/{measurement_basis}/shadow"
        ): {"POST"},
        (
            "/api/admin/usage/v2/storage-source-activation/"
            "{source}/{measurement_basis}/schedule"
        ): {"POST"},
        "/api/admin/usage/v2/compute-activation": {"GET"},
        "/api/admin/usage/v2/compute-activation/{activation_key}/shadow": {"POST"},
        "/api/admin/usage/v2/compute-activation/{activation_key}/schedule": {"POST"},
        "/api/admin/usage/v2/compute-activation/{activation_key}/rollover": {"POST"},
        "/api/admin/usage/v2/storage-assets/{asset_id}/destroy": {"POST"},
    }
    routes = {
        route.path: route.methods
        for route in mounted_route_objects(orchestrator_main.app)
        if route.path in expected
    }
    assert routes == expected


@pytest.mark.asyncio
async def test_storage_activation_routes_are_explicit_and_audited():
    actor_id = uuid4()
    request = MagicMock()
    request.url.path = "/api/admin/usage/v2/storage-activation/claim-requested/shadow"
    now = datetime(2026, 8, 6, 12, tzinfo=timezone.utc)
    store = MagicMock()
    store.enter_source_shadow = AsyncMock(
        return_value=StorageSourceActivation(
            measurement_basis="claim-requested",
            collector_id="kubernetes-pods",
            source_cluster="dev-cluster",
            state="shadow",
            activated_at=None,
            database_time=now,
        )
    )
    store.read_activations = AsyncMock(
        return_value=(
            StorageActivation("claim-requested", "shadow", None, now),
            StorageActivation("volume-provisioned", "disabled", None, now),
        )
    )
    audit = AsyncMock()
    dependencies = _admin_dependencies(
        settings=InfrastructureMeteringSettings(
            collector_enabled=True,
            pvc_inventory_enabled=True,
            stable_cluster_id="dev-cluster",
            namespace_allowlist=("srw",),
        ),
        admin={"id": str(actor_id), "is_admin": True},
        audit=audit,
        storage_assets=store,
        storage_source_activation_ready=True,
    )

    result = await infrastructure_admin_routes.enter_infrastructure_storage_shadow(
        request,
        "claim-requested",
        InfrastructureStorageActivationRequest(reason="inventory soak is healthy"),
        dependencies=dependencies,
    )

    store.enter_source_shadow.assert_awaited_once_with(
        measurement_basis="claim-requested",
        collector_id="kubernetes-pods",
        source_cluster="dev-cluster",
        requirements=(
            StorageSourceRequirementSpec(
                api_resource="core/v1/persistentvolumeclaims",
                namespace="srw",
                requirement_role="quantity",
            ),
        ),
    )
    assert result["state"] == "shadow"
    assert result["effective"] is False
    assert audit.await_args.kwargs["event_type"] == (
        "infrastructure_storage_shadow_entered"
    )


@pytest.mark.asyncio
async def test_storage_activation_schedule_is_generation_fenced():
    actor_id = uuid4()
    request = MagicMock()
    request.url.path = "/api/admin/usage/v2/storage-activation/claim-requested/schedule"
    boundary = datetime(2026, 8, 7, tzinfo=timezone.utc)
    store = MagicMock()
    store.schedule_source_activation = AsyncMock(
        return_value=StorageSourceActivation(
            measurement_basis="claim-requested",
            collector_id="kubernetes-pods",
            source_cluster="dev-cluster",
            state="active",
            activated_at=boundary,
            database_time=boundary - timedelta(hours=1),
        )
    )
    store.read_activations = AsyncMock(
        return_value=(
            StorageActivation(
                "claim-requested",
                "active",
                boundary,
                boundary - timedelta(hours=1),
            ),
            StorageActivation(
                "volume-provisioned",
                "disabled",
                None,
                boundary - timedelta(hours=1),
            ),
        )
    )
    dependencies = _admin_dependencies(
        settings=InfrastructureMeteringSettings(
            collector_enabled=True,
            shadow_enabled=True,
            pvc_inventory_enabled=True,
            pvc_shadow_enabled=True,
            stable_cluster_id="dev-cluster",
            namespace_allowlist=("srw",),
        ),
        admin={"id": str(actor_id), "is_admin": True},
        leader_generation=MagicMock(return_value=9),
        storage_assets=store,
        storage_source_activation_ready=True,
    )

    route = infrastructure_admin_routes.schedule_infrastructure_storage_activation
    await route(
        request,
        "claim-requested",
        InfrastructureStorageActivationScheduleRequest(
            reason="shadow proof reviewed",
            activated_at=boundary,
        ),
        dependencies=dependencies,
    )

    store.schedule_source_activation.assert_awaited_once_with(
        measurement_basis="claim-requested",
        collector_id="kubernetes-pods",
        source_cluster="dev-cluster",
        activated_at=boundary,
        max_scope_age=timedelta(seconds=900),
        expected_generation=9,
        identity_key_version=None,
    )


@pytest.mark.asyncio
async def test_vm_volume_source_shadow_freezes_quantity_and_attribution_scopes():
    request = MagicMock()
    request.url.path = (
        "/api/admin/usage/v2/storage-source-activation/vm/volume-provisioned/shadow"
    )
    now = datetime(2026, 8, 6, 12, tzinfo=timezone.utc)
    store = MagicMock()
    store.enter_source_shadow = AsyncMock(
        return_value=StorageSourceActivation(
            measurement_basis="volume-provisioned",
            collector_id="kubevirt-storage",
            source_cluster="vm-dev-cluster",
            state="shadow",
            activated_at=None,
            database_time=now,
        )
    )
    store.read_activations = AsyncMock(
        return_value=(
            StorageActivation("claim-requested", "shadow", None, now),
            StorageActivation("volume-provisioned", "shadow", None, now),
        )
    )
    audit = AsyncMock()
    dependencies = _admin_dependencies(
        settings=InfrastructureMeteringSettings(
            collector_enabled=True,
            vm_pvc_inventory_enabled=True,
            vm_pv_inventory_enabled=True,
            vm_stable_cluster_id="vm-dev-cluster",
            vm_namespace="srw-vms",
        ),
        audit=audit,
        storage_assets=store,
        storage_source_activation_ready=True,
    )

    route = infrastructure_admin_routes.enter_infrastructure_storage_source_shadow
    result = await route(
        request,
        "vm",
        "volume-provisioned",
        InfrastructureStorageActivationRequest(
            reason="remote inventory proof reviewed"
        ),
        dependencies=dependencies,
    )

    store.enter_source_shadow.assert_awaited_once_with(
        measurement_basis="volume-provisioned",
        collector_id="kubevirt-storage",
        source_cluster="vm-dev-cluster",
        requirements=(
            StorageSourceRequirementSpec(
                api_resource="core/v1/persistentvolumes",
                namespace=None,
                requirement_role="quantity",
            ),
            StorageSourceRequirementSpec(
                api_resource="core/v1/persistentvolumeclaims",
                namespace="srw-vms",
                requirement_role="attribution",
            ),
        ),
    )
    assert result["collector_id"] == "kubevirt-storage"
    assert result["effective"] is False
    assert audit.await_args.kwargs["resource_id"] == "vm:volume-provisioned"


@pytest.mark.asyncio
async def test_compute_activation_shadow_is_class_gated_and_audited():
    actor_id = uuid4()
    request = MagicMock()
    request.url.path = "/api/admin/usage/v2/compute-activation/agent_pod/shadow"
    now = datetime(2026, 8, 6, 12, tzinfo=timezone.utc)
    store = MagicMock()
    store.enter_shadow = AsyncMock(
        return_value=ComputeActivation(
            activation_key="agent_pod",
            state="shadow",
            activated_at=None,
            database_time=now,
        )
    )
    audit = AsyncMock()
    dependencies = _admin_dependencies(
        settings=InfrastructureMeteringSettings(
            collector_enabled=True,
            stable_cluster_id="dev-cluster",
            namespace_allowlist=("srw",),
        ),
        admin={"id": str(actor_id), "is_admin": True},
        audit=audit,
        compute_activation=store,
    )

    result = await infrastructure_admin_routes.enter_infrastructure_compute_shadow(
        request,
        "agent_pod",
        InfrastructureComputeActivationRequest(reason="agent shadow soak approved"),
        dependencies=dependencies,
    )

    store.enter_shadow.assert_awaited_once_with("agent_pod")
    assert result["state"] == "shadow"
    assert result["effective"] is False
    assert audit.await_args.kwargs["event_type"] == (
        "infrastructure_compute_shadow_entered"
    )


@pytest.mark.asyncio
async def test_vm_compute_shadow_transition_requires_inventory_not_shadow_config():
    request = MagicMock()
    request.url.path = "/api/admin/usage/v2/compute-activation/workspace_vm/shadow"
    actor_id = uuid4()
    store = MagicMock()
    store.enter_shadow = AsyncMock(
        return_value=ComputeActivation(
            activation_key="workspace_vm",
            state="shadow",
            activated_at=None,
            database_time=datetime(2026, 8, 6, 12, tzinfo=timezone.utc),
        )
    )
    base = InfrastructureMeteringSettings(
        collector_enabled=True,
        stable_cluster_id="dev-cluster",
        namespace_allowlist=("srw",),
    )
    admin = {"id": str(actor_id), "is_admin": True}
    dark = _admin_dependencies(settings=base, admin=admin, compute_activation=store)
    with pytest.raises(HTTPException) as missing_inventory:
        await infrastructure_admin_routes.enter_infrastructure_compute_shadow(
            request,
            "workspace_vm",
            InfrastructureComputeActivationRequest(
                reason="inventory must precede durable shadow"
            ),
            dependencies=dark,
        )
    assert missing_inventory.value.status_code == 404
    store.enter_shadow.assert_not_awaited()

    enabled = _admin_dependencies(
        settings=replace(
            base,
            vm_inventory_enabled=True,
            vm_stable_cluster_id="vm-cluster",
            vm_namespace="agent-vms",
        ),
        admin=admin,
        compute_activation=store,
    )
    result = await infrastructure_admin_routes.enter_infrastructure_compute_shadow(
        request,
        "workspace_vm",
        InfrastructureComputeActivationRequest(
            reason="inventory verified before enabling shadow config"
        ),
        dependencies=enabled,
    )
    assert result["state"] == "shadow"
    store.enter_shadow.assert_awaited_once_with("workspace_vm")


@pytest.mark.asyncio
async def test_vm_compute_activation_uses_remote_scope_and_collector():
    from orchestrator.services.infrastructure_metering.compute_activation import (
        ComputeActivationScheduleResult,
        ComputeEpochPromotion,
    )

    request = MagicMock()
    request.url.path = "/api/admin/usage/v2/compute-activation/workspace_vm/schedule"
    actor_id = uuid4()
    boundary = datetime(2026, 8, 8, tzinfo=timezone.utc)
    request_id = uuid4()
    store = MagicMock()
    store.schedule_activation = AsyncMock(
        return_value=ComputeActivationScheduleResult(
            activation=ComputeActivation(
                activation_key="workspace_vm",
                state="active",
                activated_at=boundary,
                database_time=boundary - timedelta(hours=1),
            ),
            promotion=ComputeEpochPromotion(
                request_id=request_id,
                activation_key="workspace_vm",
                request_kind="initial-activation",
                promoted_at=boundary - timedelta(hours=1),
                actor_id=actor_id,
                audit_reason="remote VMI shadow proof reviewed",
                replayed=False,
                authorities=(),
            ),
        )
    )
    dependencies = _admin_dependencies(
        settings=InfrastructureMeteringSettings(
            collector_enabled=True,
            shadow_enabled=True,
            vm_inventory_enabled=True,
            vm_shadow_enabled=True,
            stable_cluster_id="dev-cluster",
            namespace_allowlist=("srw",),
            vm_stable_cluster_id="vm-cluster",
            vm_namespace="agent-vms",
        ),
        admin={"id": str(actor_id), "is_admin": True},
        leader_generation=MagicMock(return_value=11),
        compute_activation=store,
    )

    route = infrastructure_admin_routes.schedule_infrastructure_compute_activation
    await route(
        request,
        "workspace_vm",
        InfrastructureComputeActivationScheduleRequest(
            idempotency_key=request_id,
            reason="remote VMI shadow proof reviewed",
            activated_at=boundary,
        ),
        dependencies=dependencies,
    )

    store.schedule_activation.assert_awaited_once_with(
        activation_key="workspace_vm",
        activated_at=boundary,
        source_cluster="vm-cluster",
        namespaces=("agent-vms",),
        max_scope_age=timedelta(seconds=900),
        expected_generation=11,
        request_id=request_id,
        actor_id=actor_id,
        audit_reason="remote VMI shadow proof reviewed",
        collector_id="kubevirt-vmis",
    )


@pytest.mark.asyncio
async def test_storage_destruction_assertion_is_idempotent_and_audited():
    from orchestrator.services.infrastructure_metering.storage_assets import (
        BackendDestructionResult,
    )

    actor_id, asset_id, request_id, assertion_id = (
        uuid4(),
        uuid4(),
        uuid4(),
        uuid4(),
    )
    effective_at = datetime(2026, 8, 6, 12, tzinfo=timezone.utc)
    request = MagicMock()
    request.url.path = f"/api/admin/usage/v2/storage-assets/{asset_id}/destroy"
    store = MagicMock()
    store.assert_destroyed = AsyncMock(
        return_value=BackendDestructionResult(
            assertion_id=assertion_id,
            idempotency_key=request_id,
            asset_id=asset_id,
            effective_at=effective_at,
            request_hash="b" * 64,
            replayed=True,
        )
    )
    audit = AsyncMock()
    dependencies = _admin_dependencies(
        admin={"id": str(actor_id), "is_admin": True},
        audit=audit,
        storage_assets=store,
    )

    body = InfrastructureStorageDestructionRequest(
        idempotency_key=request_id,
        effective_at=effective_at,
        evidence_kind="operator-attested",
        evidence_digest="a" * 64,
        reason_code="provider-console-review",
        reason="reviewed by the fleet operator",
    )
    route = infrastructure_admin_routes.assert_infrastructure_storage_asset_destroyed
    result = await route(
        request,
        asset_id,
        body,
        dependencies=dependencies,
    )

    store.assert_destroyed.assert_awaited_once_with(
        idempotency_key=request_id,
        asset_id=asset_id,
        effective_at=effective_at,
        evidence_kind="operator-attested",
        evidence_digest="a" * 64,
        actor_kind="user",
        actor_id=actor_id,
        reason_code="provider-console-review",
    )
    assert result["replayed"] is True
    assert audit.await_args.kwargs["event_type"] == (
        "infrastructure_storage_destruction_replayed"
    )


@pytest.mark.asyncio
async def test_storage_asset_operator_list_and_detail_are_safe_and_bounded():
    from orchestrator.services.infrastructure_metering.storage_assets import (
        BackendUnverifiedAssetPage,
        BackendUnverifiedAssetRecord,
        StorageAssetDetailRecord,
        StorageAssetGapDetail,
        StorageAssetIncarnationDetail,
    )

    asset_id, gap_id, epoch_id = uuid4(), uuid4(), uuid4()
    now = datetime(2026, 8, 6, 12, tzinfo=timezone.utc)
    request = MagicMock()
    request.url.path = "/api/admin/usage/v2/storage-assets/backend-unverified"
    require_admin = AsyncMock(return_value={"id": str(uuid4()), "is_admin": True})
    record = BackendUnverifiedAssetRecord(
        asset_id=asset_id,
        source_cluster="dev-cluster",
        identity_scheme="csi-hmac-sha256-v1",
        identity_key_version="storage-v1",
        csi_driver="driver.example.test",
        first_observed_at=now - timedelta(hours=1),
        last_observed_at=now - timedelta(minutes=5),
        backend_unverified_at=now,
        gap_id=gap_id,
        gap_start=now,
        reason_code="retain-pv-absent",
        storage_class_name="standard",
        reclaim_policy="retain",
        backend_deletion_finalizer_observed=True,
        volume_mode="filesystem",
        capacity_bytes=4 * 1024**3,
        detached_at=now,
        detach_reason="pv-deleted",
    )
    detail = StorageAssetDetailRecord(
        asset_id=asset_id,
        source_cluster="dev-cluster",
        identity_scheme="csi-hmac-sha256-v1",
        identity_key_version="storage-v1",
        csi_driver="driver.example.test",
        lifecycle_state="backend-unverified",
        first_observed_at=now - timedelta(hours=1),
        last_observed_at=now - timedelta(minutes=5),
        backend_unverified_at=now,
        destroyed_at=None,
        incarnations=(
            StorageAssetIncarnationDetail(
                storage_class_name="standard",
                reclaim_policy="retain",
                backend_deletion_finalizer_observed=True,
                volume_mode="filesystem",
                capacity_bytes=4 * 1024**3,
                first_observed_at=now - timedelta(hours=1),
                last_observed_at=now - timedelta(minutes=5),
                detached_at=now,
                detach_reason="pv-deleted",
            ),
        ),
        gaps=(
            StorageAssetGapDetail(
                gap_id=gap_id,
                scope_epoch_id=epoch_id,
                gap_start=now,
                gap_end=None,
                reason_code="retain-pv-absent",
                resolution="unresolved",
                resolution_assertion_id=None,
                resolved_at=None,
            ),
        ),
        assertions=(),
        history_truncated=False,
    )
    store = MagicMock()
    store.list_backend_unverified = AsyncMock(
        return_value=BackendUnverifiedAssetPage(items=(record,), next_cursor=asset_id)
    )
    store.read_asset_detail = AsyncMock(return_value=detail)
    dependencies = _admin_dependencies(
        require_admin=require_admin,
        storage_assets=store,
    )

    routes = infrastructure_admin_routes
    listed = await routes.list_infrastructure_backend_unverified_storage_assets(
        request,
        limit=25,
        cursor=None,
        dependencies=dependencies,
    )
    rendered = await routes.get_infrastructure_storage_asset_detail(
        request,
        asset_id,
        history_limit=25,
        dependencies=dependencies,
    )

    store.list_backend_unverified.assert_awaited_once_with(
        limit=25,
        after_asset_id=None,
    )
    store.read_asset_detail.assert_awaited_once_with(
        asset_id=asset_id,
        history_limit=25,
    )
    assert listed["items"][0]["asset_id"] == asset_id
    assert rendered["gaps"][0]["gap_id"] == gap_id
    serialized = repr((listed, rendered)).lower()
    assert "volume_handle" not in serialized
    assert "volume_attributes" not in serialized
    assert require_admin.await_count == 2
