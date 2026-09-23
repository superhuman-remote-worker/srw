"""Infrastructure-metering bootstrap: which metering paths this process runs.

R1.B11 moved this out of the application lifespan unchanged. Every path is
independently gated by its setting and additionally by the schema
capabilities both databases report after migrations, plus the durable
activation rows. The result is one immutable snapshot; the application
lifespan assigns it to the module state its reporting and administration
routes read, and starts the leader-owned loops only for the pieces that
exist.

Degradation is local: a failed probe or registration disables only the
affected path (logged), never startup.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from orchestrator.services.audit_partitions import (
    ensure_partitions as ensure_audit_partitions,
)
from orchestrator.services.infrastructure_activation_policy import (
    capability_gated_infrastructure_publication_resources as _capability_gated_infrastructure_publication_resources,
    capability_gated_storage_publication_policy as _capability_gated_storage_publication_policy,
    compute_activation_is_durable as _compute_activation_is_durable,
    compute_activation_is_effective as _compute_activation_is_effective,
    compute_scope_configuration as _compute_scope_configuration,
    durable_collection_settings as _durable_collection_settings,
    durable_infrastructure_reporting_resources as _durable_infrastructure_reporting_resources,
    durable_storage_reporting_policy as _durable_storage_reporting_policy,
    enabled_infrastructure_publication_resources as _enabled_infrastructure_publication_resources,
    requested_storage_publication_policy as _requested_storage_publication_policy,
    storage_source_configuration_errors as _storage_source_configuration_errors,
)
from orchestrator.services.infrastructure_metering import (
    CoverageGapWaiverService,
    InfrastructureMeteringRuntime,
    InfrastructureMeteringSettings,
    InfrastructureUsageDaySealer,
    InfrastructureUsageMaterializer,
    InfrastructureWorkspaceCutover,
    LegacyWorkspaceUsageLedgerAdapter,
    TypedUsageDailyRollup,
    UsageV2QueryService,
    probe_schema_capabilities,
)
from orchestrator.services.infrastructure_metering.compute_activation import (
    ComputeActivation,
    ComputeActivationStore,
    compute_scope_configuration_diagnostic,
)
from orchestrator.services.infrastructure_metering.ingestion import (
    InfrastructureIngestionService,
)
from orchestrator.services.infrastructure_metering.inventory import InventoryStore
from orchestrator.services.infrastructure_metering.materializer import (
    PublicationContractError,
    StoragePublicationPolicy,
)
from orchestrator.services.infrastructure_metering.storage_assets import (
    StorageActivation,
    StorageAssetStore,
    StorageSourceActivation,
)
from orchestrator.services.infrastructure_metering.storage_mapping import (
    StorageResourceMappingStore,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class InfrastructureMeteringBootstrap:
    """What infrastructure metering this process runs, decided once at startup.

    Field names match the application module state they populate.
    ``capabilities`` is the schema probe result; startup also reads it to
    decide whether leadership allocates the metering fencing generation.
    """

    capabilities: Any
    infrastructure_metering_settings: InfrastructureMeteringSettings
    infrastructure_usage_v2: UsageV2QueryService | None
    infrastructure_usage_rollup: TypedUsageDailyRollup | None
    infrastructure_inventory_store: InventoryStore | None
    infrastructure_ingestion_service: InfrastructureIngestionService | None
    infrastructure_workspace_cutover: InfrastructureWorkspaceCutover | None
    infrastructure_usage_materializer: InfrastructureUsageMaterializer | None
    infrastructure_usage_day_sealer: InfrastructureUsageDaySealer | None
    infrastructure_metering_runtime: InfrastructureMeteringRuntime | None
    infrastructure_coverage_waivers: CoverageGapWaiverService | None
    infrastructure_storage_assets: StorageAssetStore | None
    infrastructure_storage_mapping: StorageResourceMappingStore | None
    infrastructure_compute_activation: ComputeActivationStore | None
    infrastructure_compute_scope_diagnostics: dict[str, str]
    infrastructure_durable_compute_activation_keys: frozenset[str]
    infrastructure_durable_reporting_policy_ready: bool
    infrastructure_storage_source_activation_ready: bool


async def allocate_metering_generation(conn: Any) -> int:
    """Advance the metering fencing generation on the leader's own session.

    Runs as ``run_as_leader``'s ``on_acquired`` callback, on the exact
    advisory-lock connection and before ``is_leader`` becomes visible, so the
    collector, cutover, publisher and sealer share one tenure token.
    """

    generation = await conn.fetchval(
        "UPDATE infra_metering_control "
        "SET leader_generation=leader_generation+1, "
        "updated_at=statement_timestamp() "
        "WHERE singleton=TRUE RETURNING leader_generation"
    )
    if generation is None:
        raise RuntimeError("infrastructure metering control row is missing")
    return int(generation)


async def bootstrap_infrastructure_metering(
    *,
    app_pool: Any,
    audit_usage_pool: Any,
    usage_ledger: Any,
    canonical_usage_rates: Any,
    lifecycle_identity_authenticated: Callable[[], bool],
) -> InfrastructureMeteringBootstrap:
    """Probe capabilities and build the enabled metering collaborators.

    ``audit_usage_pool`` is the audit pool when the audit tier connected and
    migrated, else ``None``. ``lifecycle_identity_authenticated`` is read at
    the same point of startup the lifespan used to read the NATS bridge.
    """

    if audit_usage_pool is not None:
        try:
            await ensure_audit_partitions(audit_usage_pool)
        except Exception:
            logger.warning(
                "Audit partition preflight failed; infrastructure metering "
                "capability probing will remain fail-closed",
                exc_info=True,
            )
    infrastructure_metering_settings = InfrastructureMeteringSettings.from_env()
    metering_capabilities = await probe_schema_capabilities(
        app_pool,
        audit_usage_pool,
    )
    infrastructure_storage_source_activation_ready = (
        metering_capabilities.slice3_storage_lifecycle_ready
    )
    infrastructure_storage_mapping = None
    registered_storage_resources: tuple[str, ...] = ()
    configured_storage_resources = tuple(
        dict.fromkeys(
            rule.resource
            for rule in infrastructure_metering_settings.volume_resource_mappings
        )
    )
    storage_mapping_ready = not (
        infrastructure_metering_settings.pv_inventory_enabled
        or infrastructure_metering_settings.vm_pv_inventory_enabled
    )
    if metering_capabilities.slice2_volume_schema_ready:
        candidate_storage_mapping = StorageResourceMappingStore(app_pool)
        try:
            await candidate_storage_mapping.register(
                infrastructure_metering_settings.volume_resource_mappings
            )
            registered_storage_resources = await candidate_storage_mapping.resources()
        except Exception:
            logger.error(
                "Infrastructure storage resource mapping registration failed; "
                "PV collection/publication remain unavailable",
                exc_info=True,
            )
            storage_mapping_ready = False
        else:
            infrastructure_storage_mapping = candidate_storage_mapping
            storage_mapping_ready = True
    infrastructure_storage_assets = None
    claim_storage_activation: StorageActivation | None = None
    volume_storage_activation: StorageActivation | None = None
    storage_source_activations: tuple[StorageSourceActivation, ...] = ()
    storage_reporting_state_ready = True
    if metering_capabilities.slice2_volume_schema_ready:
        candidate_storage_assets = StorageAssetStore(app_pool)
        try:
            (
                claim_storage_activation,
                volume_storage_activation,
            ) = await candidate_storage_assets.read_activations()
        except Exception:
            logger.warning(
                "Infrastructure storage activation probe failed; storage "
                "operations remain unavailable",
                exc_info=True,
            )
            storage_reporting_state_ready = False
        else:
            infrastructure_storage_assets = candidate_storage_assets
            if metering_capabilities.slice3_storage_lifecycle_ready:
                try:
                    storage_source_activations = (
                        await candidate_storage_assets.source_status()
                    )
                except Exception:
                    logger.warning(
                        "Infrastructure storage source activation probe failed; "
                        "source publication and historical reporting remain "
                        "unavailable",
                        exc_info=True,
                    )
                    storage_reporting_state_ready = False
    infrastructure_compute_activation = None
    infrastructure_compute_scope_diagnostics = {}
    infrastructure_durable_compute_activation_keys = frozenset()
    compute_activations: dict[str, ComputeActivation] = {}
    publication_compute_activations: dict[str, ComputeActivation] = {}
    compute_reporting_state_ready = True
    if metering_capabilities.slice3_compute_inventory_ready:
        candidate_compute_activation = ComputeActivationStore(app_pool)
        try:
            compute_activation_rows = await candidate_compute_activation.status()
            compute_scope_requirements = (
                await candidate_compute_activation.requirements()
            )
            compute_epoch_authorities = await candidate_compute_activation.authorities()
        except Exception:
            logger.warning(
                "Infrastructure compute activation probe failed; Slice 3 "
                "activation operations and historical reporting remain "
                "unavailable",
                exc_info=True,
            )
            compute_reporting_state_ready = False
        else:
            infrastructure_compute_activation = candidate_compute_activation
            compute_activations = {
                activation.activation_key: activation
                for activation in compute_activation_rows
            }
            infrastructure_durable_compute_activation_keys = frozenset(
                key
                for key, activation in compute_activations.items()
                if _compute_activation_is_durable(activation)
            )
            publication_compute_activations = dict(compute_activations)
            for activation in compute_activation_rows:
                source_cluster, namespaces, collector_id = _compute_scope_configuration(
                    activation.activation_key,
                    infrastructure_metering_settings,
                )
                diagnostic = compute_scope_configuration_diagnostic(
                    activation,
                    compute_scope_requirements,
                    source_cluster=source_cluster,
                    namespaces=namespaces,
                    collector_id=collector_id,
                    authorities=compute_epoch_authorities,
                )
                if diagnostic is None:
                    continue
                infrastructure_compute_scope_diagnostics[activation.activation_key] = (
                    diagnostic
                )
                publication_compute_activations[activation.activation_key] = (
                    ComputeActivation(
                        activation_key=activation.activation_key,
                        state="disabled",
                        activated_at=None,
                        database_time=activation.database_time,
                    )
                )
                logger.error(
                    "Infrastructure compute class %s is incompatible with "
                    "its configured exact scope; interval mutation and "
                    "publication remain disabled: %s",
                    activation.activation_key,
                    diagnostic,
                )
    vm_lifecycle_authenticated = bool(lifecycle_identity_authenticated())
    if (
        infrastructure_metering_settings.vm_publication_enabled
        or infrastructure_metering_settings.vm_pvc_publication_enabled
        or infrastructure_metering_settings.vm_pv_publication_enabled
    ) and not vm_lifecycle_authenticated:
        logger.error(
            "VM compute/storage publication requested without authenticated "
            "lifecycle identity; remote publication authorities remain disabled"
        )
    requested_storage_publication_policy = _requested_storage_publication_policy(
        infrastructure_metering_settings,
        vm_lifecycle_authenticated=vm_lifecycle_authenticated,
    )
    storage_source_activation_map = {
        (
            activation.measurement_basis,
            activation.collector_id,
            activation.source_cluster,
        ): activation
        for activation in storage_source_activations
    }
    enabled_storage_publication_policy = _capability_gated_storage_publication_policy(
        requested_storage_publication_policy,
        metering_capabilities,
        claim_activation=claim_storage_activation,
        volume_activation=volume_storage_activation,
        source_activations=storage_source_activation_map,
        volume_mapping_ready=storage_mapping_ready,
        volume_identity_key_matches=(
            metering_capabilities.storage_identity_key_version
            == infrastructure_metering_settings.volume_identity_key_version
        ),
    )
    requested_infrastructure_resources = _enabled_infrastructure_publication_resources(
        infrastructure_metering_settings,
        mapped_volume_resources=tuple(
            dict.fromkeys(
                (*configured_storage_resources, *registered_storage_resources)
            )
        ),
    )
    enabled_infrastructure_resources = (
        _capability_gated_infrastructure_publication_resources(
            infrastructure_metering_settings,
            metering_capabilities,
            mapped_volume_resources=registered_storage_resources,
            volume_mapping_ready=storage_mapping_ready,
            compute_activations=publication_compute_activations,
            storage_publication_policy=enabled_storage_publication_policy,
            vm_lifecycle_authenticated=vm_lifecycle_authenticated,
        )
    )
    ide_publication_ready = bool(
        infrastructure_metering_settings.ide_pod_publication_enabled
        and metering_capabilities.slice3_compute_inventory_ready
        and _compute_activation_is_effective(
            publication_compute_activations.get("ide_workspace_pod")
        )
    )
    durable_ide_reporting_enabled = _compute_activation_is_durable(
        compute_activations.get("ide_workspace_pod")
    )
    durable_reporting_policy_ready = bool(
        compute_reporting_state_ready and storage_reporting_state_ready
    )
    try:
        durable_storage_reporting_policy = _durable_storage_reporting_policy(
            claim_activation=claim_storage_activation,
            volume_activation=volume_storage_activation,
            source_activations=storage_source_activations,
        )
        durable_reporting_resources = _durable_infrastructure_reporting_resources(
            metering_capabilities,
            mapped_volume_resources=registered_storage_resources,
            volume_mapping_ready=storage_mapping_ready,
            compute_activations=compute_activations,
            storage_reporting_policy=durable_storage_reporting_policy,
        )
    except (PublicationContractError, ValueError) as exc:
        durable_reporting_policy_ready = False
        durable_storage_reporting_policy = StoragePublicationPolicy()
        durable_reporting_resources = ("workspace_pod",)
        logger.error(
            "Infrastructure historical reporting policy is unavailable: %s",
            exc,
        )
    infrastructure_durable_reporting_policy_ready = durable_reporting_policy_ready
    if (
        infrastructure_metering_settings.ide_pod_publication_enabled
        and not ide_publication_ready
    ):
        logger.error(
            "Infrastructure IDE Pod publication requested before its schema "
            "or activation boundary is ready; IDE intervals remain excluded"
        )
    unavailable_infrastructure_resources = sorted(
        set(requested_infrastructure_resources) - set(enabled_infrastructure_resources)
    )
    if unavailable_infrastructure_resources:
        logger.error(
            "Infrastructure resource publication requested before its schema, "
            "identity, or activation boundary is ready; excluded resources=%s",
            ",".join(unavailable_infrastructure_resources),
        )
    unavailable_storage_authorities = tuple(
        authority
        for authority in requested_storage_publication_policy.authorities
        if authority not in enabled_storage_publication_policy.authorities
    )
    if unavailable_storage_authorities:
        logger.error(
            "Infrastructure storage publication requested before its exact "
            "source schema, identity, or activation boundary is ready; "
            "excluded authorities=%s",
            ",".join(
                f"{authority.measurement_basis}/"
                f"{authority.collector_id}/"
                f"{authority.source_cluster}"
                for authority in unavailable_storage_authorities
            ),
        )
    infrastructure_usage_v2 = (
        UsageV2QueryService(
            audit_usage_pool,
            metering_capabilities,
            app_pool,
            source_aware_reads_enabled=(
                infrastructure_metering_settings.source_aware_reads_enabled
            ),
            enabled_resources=durable_reporting_resources,
            ide_workspace_pod_enabled=durable_ide_reporting_enabled,
            storage_publication_policy=durable_storage_reporting_policy,
        )
        if durable_reporting_policy_ready
        else None
    )
    infrastructure_usage_rollup = (
        TypedUsageDailyRollup(
            audit_usage_pool,
            app_pool,
        )
        if metering_capabilities.slice0_ready
        else None
    )
    if infrastructure_metering_settings.v2_reads_enabled:
        if not durable_reporting_policy_ready:
            logger.error(
                "Infrastructure metering v2 reads requested but durable "
                "historical reporting policy is unavailable"
            )
        elif not metering_capabilities.slice0_ready:
            logger.error(
                "Infrastructure metering v2 reads requested but schema "
                "capabilities are incomplete: %s",
                metering_capabilities.diagnostics(),
            )
        elif infrastructure_usage_rollup is not None:
            try:
                bootstrap = await infrastructure_usage_rollup.bootstrap_state()
                if bootstrap.read_ready:
                    logger.info("Infrastructure metering v2 reads enabled (Slice 0)")
                else:
                    logger.warning(
                        "Infrastructure metering v2 reads requested but "
                        "bootstrap is %s; route remains unavailable",
                        bootstrap.status.value,
                    )
            except Exception:
                logger.warning(
                    "Infrastructure metering v2 bootstrap readiness probe failed; "
                    "route remains unavailable",
                    exc_info=True,
                )
    collection_runtime_settings = _durable_collection_settings(
        infrastructure_metering_settings,
        compute_activations=compute_activations,
        claim_activation=claim_storage_activation,
        volume_activation=volume_storage_activation,
        source_activations=storage_source_activations,
    )
    infrastructure_inventory_store = None
    infrastructure_ingestion_service = None
    if infrastructure_metering_settings.collector_enabled:
        collection_capability_errors: list[str] = []
        if not metering_capabilities.slice1_inventory_ready:
            collection_capability_errors.append("Slice 1 Pod inventory")
        if (
            infrastructure_metering_settings.pvc_inventory_enabled
            and not metering_capabilities.slice2_claim_inventory_ready
        ):
            collection_capability_errors.append("Slice 2 PVC inventory")
        if (
            infrastructure_metering_settings.vm_pvc_inventory_enabled
            and not metering_capabilities.slice2_claim_inventory_ready
        ):
            collection_capability_errors.append("Slice 3 VM PVC inventory")
        if (
            infrastructure_metering_settings.pv_inventory_enabled
            and not metering_capabilities.slice2_volume_schema_ready
        ):
            collection_capability_errors.append("Slice 2 PV lifecycle schema")
        if (
            infrastructure_metering_settings.vm_pv_inventory_enabled
            and not metering_capabilities.slice2_volume_schema_ready
        ):
            collection_capability_errors.append("Slice 3 VM PV lifecycle schema")
        if (
            infrastructure_metering_settings.pv_inventory_enabled
            or infrastructure_metering_settings.vm_pv_inventory_enabled
        ) and not storage_mapping_ready:
            collection_capability_errors.append("Slice 2 PV resource mapping")
        if (
            (
                infrastructure_metering_settings.pv_inventory_enabled
                or infrastructure_metering_settings.vm_pv_inventory_enabled
            )
            and metering_capabilities.storage_identity_key_registered
            and metering_capabilities.storage_identity_key_version
            != infrastructure_metering_settings.volume_identity_key_version
        ):
            collection_capability_errors.append(
                "Slice 2 PV identity key version mismatch"
            )
        if (
            collection_runtime_settings.ide_pod_shadow_enabled
            or collection_runtime_settings.agent_pod_shadow_enabled
            or collection_runtime_settings.vm_inventory_enabled
        ) and not metering_capabilities.slice3_compute_inventory_ready:
            collection_capability_errors.append("Slice 3 compute inventory")
        if (
            collection_runtime_settings.pvc_shadow_enabled
            or collection_runtime_settings.pv_shadow_enabled
            or collection_runtime_settings.vm_pvc_shadow_enabled
            or collection_runtime_settings.vm_pv_shadow_enabled
            or infrastructure_metering_settings.pvc_publication_enabled
            or infrastructure_metering_settings.pv_publication_enabled
            or infrastructure_metering_settings.vm_pvc_publication_enabled
            or infrastructure_metering_settings.vm_pv_publication_enabled
        ) and not metering_capabilities.slice3_storage_lifecycle_ready:
            collection_capability_errors.append(
                "Slice 3 exact-source storage lifecycle"
            )
        elif metering_capabilities.slice3_storage_lifecycle_ready:
            collection_capability_errors.extend(
                _storage_source_configuration_errors(
                    collection_runtime_settings,
                    storage_source_activations,
                )
            )
        if collection_capability_errors:
            logger.error(
                "Infrastructure metering collection requested but capabilities "
                "are incomplete (%s): %s",
                ", ".join(collection_capability_errors),
                metering_capabilities.diagnostics(),
            )
        else:
            ingestion_key = os.environ.get("INFRASTRUCTURE_METERING_INGESTION_KEY", "")
            additional_ingestion_keys: dict[str, str] = {}
            if infrastructure_metering_settings.vm_inventory_enabled:
                additional_ingestion_keys["kubevirt-vmis"] = os.environ.get(
                    "INFRASTRUCTURE_METERING_VMI_INGESTION_KEY", ""
                )
            if (
                infrastructure_metering_settings.vm_pvc_inventory_enabled
                or infrastructure_metering_settings.vm_pv_inventory_enabled
            ):
                additional_ingestion_keys["kubevirt-storage"] = os.environ.get(
                    "INFRASTRUCTURE_METERING_VM_STORAGE_INGESTION_KEY", ""
                )
            try:
                candidate_store = InventoryStore(
                    app_pool,
                    max_collector_clock_skew=timedelta(
                        seconds=(
                            infrastructure_metering_settings.max_collector_clock_skew_seconds
                        )
                    ),
                    max_batch_items=500,
                    max_batch_bytes=min(
                        2 * 1024 * 1024,
                        infrastructure_metering_settings.max_snapshot_bytes,
                    ),
                    max_snapshot_items=(
                        infrastructure_metering_settings.max_snapshot_items
                    ),
                    max_snapshot_bytes=(
                        infrastructure_metering_settings.max_snapshot_bytes
                    ),
                    max_error_items=2_000,
                    ticket_ttl=timedelta(
                        seconds=(
                            infrastructure_metering_settings.ingestion_ticket_ttl_seconds
                        )
                    ),
                    watch_session_ttl=timedelta(
                        seconds=(
                            infrastructure_metering_settings.ingestion_ticket_ttl_seconds
                        )
                    ),
                    max_watch_events=(
                        # Reserve one durable control-event slot so an
                        # ambiguous final object-event ACK can still record a
                        # history gap instead of being blocked by the bound it
                        # may have just reached.
                        infrastructure_metering_settings.watch_queue_size + 1
                    ),
                    max_watch_event_bytes=min(
                        2 * 1024 * 1024,
                        infrastructure_metering_settings.max_snapshot_bytes,
                    ),
                    max_watch_bytes=(
                        # The history-gap control event is zero-byte, but an
                        # extra byte keeps the session live when the last
                        # allowed source event lands exactly on the collector
                        # byte ceiling and its response is lost.
                        infrastructure_metering_settings.max_snapshot_bytes + 1
                    ),
                )
                candidate_service = InfrastructureIngestionService(
                    app_pool,
                    candidate_store,
                    collection_runtime_settings,
                    ingestion_key=ingestion_key,
                    additional_ingestion_keys=additional_ingestion_keys or None,
                )
            except (TypeError, ValueError):
                logger.error(
                    "Infrastructure metering ingestion configuration is invalid; "
                    "collector requests remain unavailable",
                    exc_info=True,
                )
            else:
                infrastructure_inventory_store = candidate_store
                infrastructure_ingestion_service = candidate_service
                logger.info(
                    "Infrastructure metering ingestion enabled mode=%s",
                    "shadow"
                    if collection_runtime_settings.shadow_enabled
                    else "inventory-only",
                )

    infrastructure_workspace_cutover = None
    infrastructure_usage_materializer = None
    infrastructure_usage_day_sealer = None
    infrastructure_metering_runtime = None
    infrastructure_coverage_waivers = None
    if metering_capabilities.slice1_runtime_ready:
        infrastructure_coverage_waivers = CoverageGapWaiverService(app_pool)
        if (
            infrastructure_metering_settings.stable_cluster_id
            and infrastructure_metering_settings.namespace_allowlist
            and audit_usage_pool is not None
        ):
            legacy_cutover_ledger = LegacyWorkspaceUsageLedgerAdapter(
                audit_usage_pool,
                usage_ledger,
                canonical_usage_rates,
            )
            infrastructure_workspace_cutover = InfrastructureWorkspaceCutover(
                app_pool,
                legacy_cutover_ledger,
                source_cluster=infrastructure_metering_settings.stable_cluster_id,
                namespace_allowlist=(
                    infrastructure_metering_settings.namespace_allowlist
                ),
                max_scope_age=timedelta(
                    seconds=infrastructure_metering_settings.stale_after_seconds
                ),
            )
        if (
            infrastructure_metering_settings.publication_enabled
            and durable_reporting_policy_ready
        ):
            infrastructure_usage_materializer = InfrastructureUsageMaterializer(
                app_pool,
                usage_ledger,
                publication_enabled=True,
                enabled_resources=enabled_infrastructure_resources,
                ide_workspace_pod_enabled=ide_publication_ready,
                storage_publication_policy=enabled_storage_publication_policy,
            )
            infrastructure_usage_day_sealer = InfrastructureUsageDaySealer(
                app_pool,
                sealing_enabled=True,
                enabled_resources=durable_reporting_resources,
                ide_workspace_pod_enabled=durable_ide_reporting_enabled,
                storage_publication_policy=durable_storage_reporting_policy,
            )
        if (
            infrastructure_workspace_cutover is not None
            or infrastructure_usage_materializer is not None
            or infrastructure_usage_day_sealer is not None
        ):
            infrastructure_metering_runtime = InfrastructureMeteringRuntime(
                app_pool,
                cutover=(
                    infrastructure_workspace_cutover
                    if durable_reporting_policy_ready
                    else None
                ),
                materializer=infrastructure_usage_materializer,
                sealer=infrastructure_usage_day_sealer,
            )
    elif (
        infrastructure_metering_settings.cutover_enabled
        or infrastructure_metering_settings.publication_enabled
        or infrastructure_metering_settings.source_aware_reads_enabled
    ):
        logger.error(
            "Infrastructure metering Slice 1 runtime requested but schema "
            "capabilities are incomplete: %s",
            metering_capabilities.diagnostics(),
        )

    if (
        infrastructure_metering_settings.cutover_enabled
        and infrastructure_workspace_cutover is None
    ):
        logger.error(
            "Infrastructure metering cutover requested but its stable source, "
            "audit ledger, or runtime schema is unavailable"
        )
    if infrastructure_metering_settings.publication_enabled:
        if infrastructure_usage_materializer is None:
            logger.error(
                "Infrastructure metering publication requested but runtime "
                "capabilities are unavailable"
            )
        else:
            logger.info(
                "Infrastructure metering strict publication enabled resources=%s",
                ",".join(enabled_infrastructure_resources),
            )

    return InfrastructureMeteringBootstrap(
        capabilities=metering_capabilities,
        infrastructure_metering_settings=infrastructure_metering_settings,
        infrastructure_usage_v2=infrastructure_usage_v2,
        infrastructure_usage_rollup=infrastructure_usage_rollup,
        infrastructure_inventory_store=infrastructure_inventory_store,
        infrastructure_ingestion_service=infrastructure_ingestion_service,
        infrastructure_workspace_cutover=infrastructure_workspace_cutover,
        infrastructure_usage_materializer=infrastructure_usage_materializer,
        infrastructure_usage_day_sealer=infrastructure_usage_day_sealer,
        infrastructure_metering_runtime=infrastructure_metering_runtime,
        infrastructure_coverage_waivers=infrastructure_coverage_waivers,
        infrastructure_storage_assets=infrastructure_storage_assets,
        infrastructure_storage_mapping=infrastructure_storage_mapping,
        infrastructure_compute_activation=infrastructure_compute_activation,
        infrastructure_compute_scope_diagnostics=(
            infrastructure_compute_scope_diagnostics
        ),
        infrastructure_durable_compute_activation_keys=(
            infrastructure_durable_compute_activation_keys
        ),
        infrastructure_durable_reporting_policy_ready=(
            infrastructure_durable_reporting_policy_ready
        ),
        infrastructure_storage_source_activation_ready=(
            infrastructure_storage_source_activation_ready
        ),
    )
