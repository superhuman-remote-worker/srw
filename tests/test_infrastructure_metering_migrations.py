"""Static contracts for the Slice 0 infrastructure-metering migrations."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest

from orchestrator.database.migrate import discover, run_migrations
from orchestrator.services.infrastructure_metering.materializer import (
    InfrastructureUsageMaterializer,
    PublicationConflictError,
)
from orchestrator.services.infrastructure_metering.inventory import (
    InventoryConflictError,
    InventoryContractError,
    InventoryFenceError,
    InventoryItem,
    InventoryRecoveryRequired,
    InventoryScopeIdentity,
    InventoryStore,
    SanitizedInventoryError,
    ShadowComparison,
    ShadowComparisonStatus,
    SnapshotFinalization,
    TransportNonceClaim,
    WatchEventKind,
    WatchIntervalMutationContext,
    WatchMutationAction,
    WatchObjectEvent,
    inventory_manifest_digest,
)
from orchestrator.services.infrastructure_metering.pod_intervals import (
    PodIntervalReconciler,
)
from orchestrator.services.infrastructure_metering.queries import UsageVisibility
from orchestrator.services.infrastructure_metering.read_model import (
    SourceAwareUsageReadModel,
)
from orchestrator.services.infrastructure_metering.sealer import (
    DaySealDisposition,
    InfrastructureUsageDaySealer,
)
from orchestrator.services.usage_ledger import StrictUsagePublishResult
from shared.persistent_input_delivery import message_row_id, persist_input_delivery


ROOT = Path(__file__).parents[1]
APP_MIGRATION = (
    ROOT
    / "src/orchestrator/database/migrations/app/0086_infrastructure_metering_foundations.sql"
)
APP_INGESTION_MIGRATION = (
    ROOT
    / "src/orchestrator/database/migrations/app/0087_inventory_ingestion_foundations.sql"
)
APP_INGESTION_SIZE_FIX = (
    ROOT
    / "src/orchestrator/database/migrations/app/0088_inventory_ingestion_logical_size.sql"
)
APP_PLAN_PERIOD_INDEX = (
    ROOT
    / "src/orchestrator/database/migrations/app/0089_infrastructure_plan_period_idx.notx.sql"
)
APP_INTERVAL_OVERLAP_INDEX = (
    ROOT
    / "src/orchestrator/database/migrations/app/0090_infrastructure_interval_overlap_idx.notx.sql"
)
APP_COMPLETE_SNAPSHOT_RECEIVED_INDEX = (
    ROOT
    / "src/orchestrator/database/migrations/app/0091_inventory_complete_received_idx.notx.sql"
)
APP_INVALID_WATCH_RECEIVED_INDEX = (
    ROOT
    / "src/orchestrator/database/migrations/app/0092_inventory_invalid_watch_received_idx.notx.sql"
)
APP_DAY_SEQUENCE_BACKFILL_PREP = (
    ROOT
    / "src/orchestrator/database/migrations/app/0092z_infrastructure_day_sequence_backfill_prep.sql"
)
APP_TERMINAL_EVIDENCE_MIGRATION = (
    ROOT
    / "src/orchestrator/database/migrations/app/0100_infrastructure_terminal_evidence_single_boundary.sql"
)
APP_REFERENCED_RATE_GUARD_MIGRATION = (
    ROOT
    / "src/orchestrator/database/migrations/app/0101_usage_rates_v2_referenced_range_guard.sql"
)
APP_STORAGE_FOUNDATION_MIGRATION = (
    ROOT / "src/orchestrator/database/migrations/app/0102_storage_asset_foundations.sql"
)
APP_COMPUTE_FOUNDATION_MIGRATION = (
    ROOT
    / "src/orchestrator/database/migrations/app/0103_compute_metering_foundations.sql"
)
APP_AGENT_METERING_LOCK_ORDER_MIGRATION = (
    ROOT / "src/orchestrator/database/migrations/app/0104_agent_metering_lock_order.sql"
)
APP_STORAGE_SOURCE_ACTIVATION_MIGRATION = (
    ROOT / "src/orchestrator/database/migrations/app/0105_storage_source_activation.sql"
)
APP_COMPUTE_SCOPE_EPOCH_GUARD_MIGRATION = (
    ROOT / "src/orchestrator/database/migrations/app/0106_compute_scope_epoch_guard.sql"
)
APP_COMPUTE_SCOPE_AUTHORIZATION_MIGRATION = (
    ROOT
    / "src/orchestrator/database/migrations/app/0107_compute_scope_authorization.sql"
)
APP_COMPUTE_EXACT_EPOCH_AUTHORITY_MIGRATION = (
    ROOT
    / "src/orchestrator/database/migrations/app/0108_compute_exact_epoch_authority.sql"
)
APP_COMPUTE_EXACT_EPOCH_LIFECYCLE_MIGRATION = (
    ROOT
    / "src/orchestrator/database/migrations/app/0109_compute_exact_epoch_lifecycle.sql"
)
APP_COMPUTE_EPOCH_ROLLOVER_MIGRATION = (
    ROOT
    / "src/orchestrator/database/migrations/app/0112_compute_epoch_rollover_authority.sql"
)
APP_COMPUTE_AUTHORITY_CONFIRMATION_GAP_MIGRATION = (
    ROOT
    / "src/orchestrator/database/migrations/app/0113_compute_authority_confirmation_gap.sql"
)
APP_COMPUTE_INTERVAL_EPOCH_SHAPE_REPAIR_MIGRATION = (
    ROOT
    / "src/orchestrator/database/migrations/app/0114_compute_interval_epoch_shape_repair.sql"
)
# Bump this whenever a new app migration lands — the assertion below is the
# tripwire that says "a migration was added; check the snapshot was regenerated
# and nothing was renumbered". Head as of the merged stateless work: the S3
# worker-lane partition (0118), session control inbox (0119-0121), and the
# stateless cloud-generation fence/content baseline (0122-0124), and durable
# owner-gated client presence (0125), and lane-independent Canvas editor
# awareness (0126), the exact-lease interrupt inbox (0127-0129), and the
# verification-critic dedupe/concurrent unique-index rollout (0130-0132), and
# the S2 durable persistent-session residue tables (0133), and the Gate-3
# completion command substrate (0140; 0134-0139 are reserved), and the routed
# completion-sweep substrate (0141).
APP_DATASOURCE_TOMBSTONES_MIGRATION = (
    ROOT / "src/orchestrator/database/migrations/app/0115_datasource_tombstones.sql"
)
APP_JOBS_EXECUTION_LANE_MIGRATION = (
    ROOT / "src/orchestrator/database/migrations/app/0118_jobs_execution_lane.sql"
)
APP_THREAD_CONTROL_INBOX_MIGRATION = (
    ROOT / "src/orchestrator/database/migrations/app/0119_thread_control_inbox.sql"
)
APP_THREAD_CONTROL_RECEIPT_INDEX = (
    ROOT
    / "src/orchestrator/database/migrations/app/0120_thread_control_receipt_idx.notx.sql"
)
APP_THREAD_CONTROL_VALIDATION = (
    ROOT
    / "src/orchestrator/database/migrations/app/0121_thread_control_validate_constraints.sql"
)
APP_THREAD_CLOUD_SYNC_GENERATIONS = (
    ROOT
    / "src/orchestrator/database/migrations/app/0122_thread_cloud_sync_generations.sql"
)
APP_THREAD_CLOUD_SYNC_BASELINES = (
    ROOT
    / "src/orchestrator/database/migrations/app/0123_thread_cloud_sync_baselines.sql"
)
APP_CLOUD_SYNC_MARKER_COMMENT = (
    ROOT / "src/orchestrator/database/migrations/app/0124_cloud_sync_marker_comment.sql"
)
APP_THREAD_CLIENT_PRESENCE = (
    ROOT / "src/orchestrator/database/migrations/app/0125_thread_client_presence.sql"
)
APP_CANVAS_EDITOR_AWARENESS = (
    ROOT / "src/orchestrator/database/migrations/app/0126_canvas_editor_awareness.sql"
)
APP_THREAD_INTERRUPT_INBOX = (
    ROOT / "src/orchestrator/database/migrations/app/0127_thread_interrupt_inbox.sql"
)
APP_THREAD_INTERRUPT_RECEIPT_INDEX = (
    ROOT
    / "src/orchestrator/database/migrations/app/0128_thread_interrupt_receipt_idx.notx.sql"
)
APP_THREAD_INTERRUPT_VALIDATION = (
    ROOT
    / "src/orchestrator/database/migrations/app/0129_thread_interrupt_validate_constraints.sql"
)
APP_JOBS_VERIFICATION_DEDUPE = (
    ROOT / "src/orchestrator/database/migrations/app/0130_jobs_verification_dedupe.sql"
)
APP_JOBS_VERIFICATION_DROP_INDEX = (
    ROOT
    / "src/orchestrator/database/migrations/app/0131_drop_jobs_verification_uniq.notx.sql"
)
APP_JOBS_VERIFICATION_INDEX = (
    ROOT
    / "src/orchestrator/database/migrations/app/0132_jobs_verification_uniq.notx.sql"
)
APP_THREAD_SESSION_DURABLE_STATE = (
    ROOT
    / "src/orchestrator/database/migrations/app/0133_thread_session_durable_state.sql"
)
APP_JOB_COMPLETION_COMMANDS = (
    ROOT / "src/orchestrator/database/migrations/app/0140_job_completion_commands.sql"
)
APP_JOB_COMPLETION_SWEEP_ROUTING = (
    ROOT
    / "src/orchestrator/database/migrations/app/0141_job_completion_sweep_routing.sql"
)
APP_JOB_COMPLETION_SWEEP_ROUTE_PRECEDENCE = (
    ROOT
    / "src/orchestrator/database/migrations/app/0142_job_completion_sweep_route_precedence.sql"
)
APP_JOB_COMPLETION_ACCEPT_STATUS = (
    ROOT
    / "src/orchestrator/database/migrations/app/0143_job_completion_accept_status.sql"
)
APP_JOB_COMPLETION_STATUS_REORDER = (
    ROOT
    / "src/orchestrator/database/migrations/app/0144_job_completion_status_reorder.sql"
)
APP_MANAGED_REPOSITORY_AUTHORITIES = (
    ROOT
    / "src/orchestrator/database/migrations/app/0176_managed_repository_authorities.sql"
)
APP_MANAGED_REPOSITORY_THREAD_DETACH = (
    ROOT
    / "src/orchestrator/database/migrations/app/0177_managed_repository_thread_detach.sql"
)
APP_SUDO_REQUESTS_THREAD_SCOPE = (
    ROOT
    / "src/orchestrator/database/migrations/app/0178_sudo_requests_thread_scope.sql"
)
APP_SUDO_REQUESTS_ENTITY_CHECK = (
    ROOT
    / "src/orchestrator/database/migrations/app/0179_sudo_requests_entity_check.sql"
)
APP_SUDO_REQUESTS_THREAD_INDEX = (
    ROOT
    / "src/orchestrator/database/migrations/app/0180_sudo_requests_thread_idx.notx.sql"
)
APP_SUDO_REQUESTS_VALIDATE_CONSTRAINTS = (
    ROOT
    / "src/orchestrator/database/migrations/app/0181_sudo_requests_validate_constraints.sql"
)
APP_DELIVERABLE_CONTRACT_AUTHORITY = (
    ROOT
    / "src/orchestrator/database/migrations/app/0182_deliverable_contract_authority.sql"
)
APP_PERSISTENT_INPUT_DELIVERY_CANCELLATION = (
    ROOT
    / "src/orchestrator/database/migrations/app/0183_persistent_input_delivery_cancellation.sql"
)
APP_THREAD_ENDED_TRANSITION_FENCE = (
    ROOT
    / "src/orchestrator/database/migrations/app/0184_thread_ended_transition_fence.sql"
)
APP_THREAD_RUNTIME_GENERATION_RETIREMENT = (
    ROOT
    / "src/orchestrator/database/migrations/app/0185_thread_runtime_generation_retirement.sql"
)
APP_PROTECTED_CLOUD_INSTANCE_AUTHORITY = (
    ROOT
    / "src/orchestrator/database/migrations/app/0186_protected_cloud_instance_authority.sql"
)
APP_DEPLOYED_PRE_REGISTRATION_SANDBOX_ZERO = (
    ROOT
    / "src/orchestrator/database/migrations/app/0187_pre_registration_sandbox_zero.sql"
)
APP_DEPLOYED_PRE_REGISTRATION_DELETE_SANDBOX_ZERO = (
    ROOT
    / "src/orchestrator/database/migrations/app/0188_pre_registration_delete_sandbox_zero.sql"
)
APP_COMPUTE_INITIAL_RECOVERY_AUTHORITY = (
    ROOT
    / "src/orchestrator/database/migrations/app/0189_compute_initial_recovery_epoch_authority.sql"
)
APP_MANAGED_REPOSITORY_LEGACY_RECONCILIATION = (
    ROOT
    / "src/orchestrator/database/migrations/app/0190_managed_repository_legacy_reconciliation.sql"
)
APP_STATELESS_INPUT_DELIVERIES = (
    ROOT
    / "src/orchestrator/database/migrations/app/0191_stateless_input_deliveries.sql"
)
APP_STATELESS_INPUT_DELIVERY_VALIDATION = (
    ROOT
    / "src/orchestrator/database/migrations/app/0192_stateless_input_delivery_validate.sql"
)
APP_MANAGED_REPOSITORY_PROCESS_ZERO_AUTHORITY = (
    ROOT
    / "src/orchestrator/database/migrations/app/0193_managed_repository_process_zero_authority.sql"
)
APP_NOTIFICATIONS_MIGRATION = (
    ROOT / "src/orchestrator/database/migrations/app/0194_notifications.sql"
)
APP_NOTIFICATION_STEPS_MIGRATION = (
    ROOT / "src/orchestrator/database/migrations/app/0195_notification_steps.sql"
)
APP_NOTIFICATIONS_CUTOVER_MIGRATION = (
    ROOT / "src/orchestrator/database/migrations/app/0196_notifications_cutover.sql"
)
# Unified notification feed (slice 3: the cutover backfill) — the head after
# the 0184-0192 managed-repository / stateless-input lane and the slice-1/2
# feed tables; bump when the next lands.
APP_PINNED_RECYCLE_AUTHORITY_MIGRATION = (
    ROOT
    / "src/orchestrator/database/migrations/app/0200_pinned_agent_recycle_authority.sql"
)
APP_USER_SSH_KEYS = (
    ROOT / "src/orchestrator/database/migrations/app/0201_user_ssh_keys.sql"
)
APP_THREADS_SSH_HANDLE = (
    ROOT / "src/orchestrator/database/migrations/app/0202_threads_ssh_handle.sql"
)
APP_THREADS_SSH_HANDLE_IDX = (
    ROOT
    / "src/orchestrator/database/migrations/app/0203_threads_ssh_handle_idx.notx.sql"
)
APP_SSH_ATTACHMENTS = (
    ROOT / "src/orchestrator/database/migrations/app/0204_ssh_attachments.sql"
)
# U1 config unification: the experts.tags role backfill (data only) — the head
# after the 0201-0204 SSH lane; bump when the next lands.
APP_EXPERTS_ROLE_TAGS_BACKFILL = (
    ROOT
    / "src/orchestrator/database/migrations/app/0205_experts_role_tags_backfill.sql"
)
# U3 subagents (WP3): the threads kind / parent / subagent columns, their
# partial parent-job index and the validation of the NOT VALID constraints —
# the head after the 0205 role-tag backfill; bump when the next lands.
APP_THREADS_SUBAGENT_KIND = (
    ROOT / "src/orchestrator/database/migrations/app/0206_threads_subagent_kind.sql"
)
APP_THREADS_PARENT_JOB_IDX = (
    ROOT
    / "src/orchestrator/database/migrations/app/0207_threads_parent_job_idx.notx.sql"
)
APP_THREADS_SUBAGENT_VALIDATE = (
    ROOT / "src/orchestrator/database/migrations/app/0208_threads_subagent_validate.sql"
)
APP_EXPERT_PERSONA_IDENTITY_BACKFILL = (
    ROOT
    / "src/orchestrator/database/migrations/app/0209_expert_persona_identity_backfill.sql"
)
APP_THREAD_TERMINAL_RECLAIM_PROJECTION = (
    ROOT
    / "src/orchestrator/database/migrations/app/0210_thread_terminal_reclaim_projection.sql"
)
APP_IMAGE_DELIVERY_ROWS_EVENT_ROLE = (
    ROOT
    / "src/orchestrator/database/migrations/app/0211_image_delivery_rows_event_role.sql"
)
# Research search/fetch capability resolution. Renumbered twice: 0205 -> 0210 during
# the 2026-08-30 rebase (0205 was taken by 0205_experts_role_tags_backfill.sql), then
# 0210 -> 0212 during the 2026-09-01 rebase onto the rewritten develop, where 0210 and
# 0211 had both been taken upstream. Two migrations sharing a prefix hard-fails the
# duplicate-prefix check and the runner.
APP_SEARCH_FETCH_CAPABILITIES = (
    ROOT / "src/orchestrator/database/migrations/app/0212_search_fetch_capabilities.sql"
)
# Supersedes 0183's constraint shape without editing 0183. Renumbered 0206 -> 0211
# during the 2026-08-30 rebase onto develop: 0206 was already taken by
# 0206_threads_subagent_kind.sql, which landed first.
# Renumbered 0211 -> 0213 during the 2026-09-01 rebase onto the rewritten develop:
# 0211 was taken upstream by 0211_image_delivery_rows_event_role.sql.
APP_INPUT_DELIVERY_CONSTRAINTS_NOT_VALID = (
    ROOT
    / "src/orchestrator/database/migrations/app/0213_input_delivery_constraints_not_valid.sql"
)
APP_CURRENT_MIGRATION_HEAD = (
    ROOT
    / "src/orchestrator/database/migrations/app/0281_vm_thread_retained_creation_disposition.sql"
)
AUDIT_EXPANSION = (
    ROOT
    / "src/orchestrator/database/migrations/audit/0003_infrastructure_usage_events_v2.sql"
)
AUDIT_VALIDATION = (
    ROOT
    / "src/orchestrator/database/migrations/audit/0004_validate_and_seed_infrastructure_usage_v2.sql"
)
AUDIT_PROJECT_INDEX = (
    ROOT
    / "src/orchestrator/database/migrations/audit/0005_usage_events_project_ts_idx.sql"
)


def _compact(sql: str) -> str:
    return " ".join(sql.split())


def _asyncpg_dsn(url: str) -> str:
    return re.sub(r"^postgresql\+\w+://", "postgresql://", url)


def _swap_db(dsn: str, dbname: str) -> str:
    head, _, tail = dsn.rpartition("/")
    query = ""
    if "?" in tail:
        query = "?" + tail.split("?", 1)[1]
    return f"{head}/{dbname}{query}"


def test_migration_discovery_orders_emergency_interstitial_versions(
    tmp_path: Path,
) -> None:
    for name in (
        "0093_later.sql",
        "0092_base.sql",
        "0092z_bridge.sql",
        "0091_before.sql",
    ):
        (tmp_path / name).write_text("SELECT 1;\n")

    assert [path.name for path in discover(tmp_path)] == [
        "0091_before.sql",
        "0092_base.sql",
        "0092z_bridge.sql",
        "0093_later.sql",
    ]


def test_migration_discovery_rejects_duplicate_interstitial_version(
    tmp_path: Path,
) -> None:
    (tmp_path / "0092z_first.sql").write_text("SELECT 1;\n")
    (tmp_path / "0092z_second.sql").write_text("SELECT 2;\n")

    with pytest.raises(RuntimeError, match="duplicate migration prefix '0092z'"):
        discover(tmp_path)


@pytest.fixture(scope="module")
def app_pg_dsn() -> str:
    testcontainers = pytest.importorskip("testcontainers.postgres")
    try:
        container = testcontainers.PostgresContainer("postgres:16")
        container.start()
    except Exception as exc:
        pytest.skip(f"no container runtime for app migration test: {exc}")
    try:
        yield _asyncpg_dsn(container.get_connection_url())
    finally:
        container.stop()


def _transport(kind: str, *, nonce=None) -> TransportNonceClaim:
    return TransportNonceClaim(
        collector_id="kubernetes",
        request_nonce=nonce or uuid4(),
        request_kind=kind,
        request_digest="9" * 64,
    )


def _workspace_pod_item(
    *,
    owner_id,
    source_uid: str,
    name: str,
    revision_hash: str,
    transition_at: datetime,
    accrues: bool = True,
    overhead_cpu_millicores: int = 0,
    overhead_memory_bytes: int = 0,
) -> InventoryItem:
    def iso(value: datetime) -> str:
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    return InventoryItem(
        source_kind="pod",
        source_uid=source_uid,
        revision_hash=revision_hash,
        valid_for_metering=True,
        normalized_item={
            "source_kind": "pod",
            "uid": source_uid,
            "api_version": "v1",
            "resource_version": "rv-object",
            "namespace": "workers",
            "name": name,
            "labels": {
                "app": "srw-workspace",
                "srw/component": "workspace",
                "srw/job-id": str(owner_id),
            },
            "lifecycle": {
                "accrues": accrues,
                "terminal": False,
                "creation_timestamp": iso(transition_at - timedelta(minutes=1)),
                "start_time": iso(transition_at) if accrues else None,
                "pod_scheduled_condition": {
                    "status": "True" if accrues else "False",
                    "last_transition_time": iso(transition_at),
                },
            },
            "capacity": {
                "cpu_millicores": 750,
                "memory_bytes": 2 * 1024**3,
                "capacity_quality": "exact",
                "measurement_algorithm": "pod-requests-test-v1",
                "overhead_cpu_millicores": overhead_cpu_millicores,
                "overhead_memory_bytes": overhead_memory_bytes,
            },
        },
    )


async def _insert_open_test_interval(
    conn: asyncpg.Connection,
    *,
    inventory_scope_id,
    source_cluster: str,
    namespace: str,
    source_uid: str,
    source_revision: str,
    observed_at: datetime,
    lifecycle_id=None,
    revision_no: int = 1,
):
    lifecycle_id = lifecycle_id or uuid4()
    interval_id = uuid4()
    if revision_no == 1:
        await conn.execute(
            "INSERT INTO resource_lifecycle_heads (source_lifecycle_id) VALUES ($1)",
            lifecycle_id,
        )
    await conn.execute(
        "INSERT INTO resource_intervals ("
        "id, inventory_scope_id, source_cluster, source_kind, source_uid, "
        "source_api_version, source_resource_version, source_lifecycle_id, "
        "revision_no, source_revision, namespace, name, category, resource, "
        "measurement_basis, cost_domain, resource_class, attribution_scope, "
        "owner_kind, owner_id, attribution_source, attribution_quality, "
        "lifecycle_confidence, cpu_millicores, memory_bytes, capacity_source, "
        "capacity_quality, measurement_algorithm, started_at, "
        "start_time_source, start_uncertainty_us, last_seen_at, "
        "last_confirmed_at, materialized_through) VALUES ("
        "$1, $2, $3, 'pod', $4, 'v1', 'rv-test', $5, $6, $7, $8, $4, "
        "'compute', 'vcpu', 'scheduler-request', 'workload-allocation', "
        "'kubernetes-pod', 'shared-platform', 'platform', 'platform', "
        "'test-fixture', 'exact', 'kubernetes-visible', 1000, 1073741824, "
        "'scheduler-request', 'exact', 'test-v1', $9, 'inventory-receipt', "
        "0, $10, $10, $9)",
        interval_id,
        inventory_scope_id,
        source_cluster,
        source_uid,
        lifecycle_id,
        revision_no,
        source_revision,
        namespace,
        observed_at - timedelta(minutes=1),
        observed_at,
    )
    await conn.execute(
        "UPDATE resource_lifecycle_heads SET latest_revision_no=$2, "
        "current_interval_id=$3, updated_at=statement_timestamp() "
        "WHERE source_lifecycle_id=$1",
        lifecycle_id,
        revision_no,
        interval_id,
    )
    return interval_id


def test_app_migration_has_inventory_and_restrict_relationships() -> None:
    sql = APP_MIGRATION.read_text()

    for table in (
        "resource_inventory_scopes",
        "resource_inventory_scope_epochs",
        "resource_inventory_snapshots",
        "resource_inventory_snapshot_items",
        "resource_inventory_coverage_gaps",
        "infra_metering_control",
    ):
        assert f"CREATE TABLE {table}" in sql

    assert "resource_inventory_scopes_identity_uq" in sql
    assert "NULLS NOT DISTINCT" in sql
    assert "resource_inventory_scope_epochs_active_uq" in sql
    assert "WHERE retired_at IS NULL" in sql
    assert "resource_inventory_scopes_id_cluster_uq" in sql
    assert "valid_for_metering AND revision_hash IS NOT NULL" in _compact(sql)
    assert "FOREIGN KEY (last_complete_snapshot_id, id)" in sql
    assert "resource_inventory_snapshots_epoch_scope_fkey" in sql
    assert "manifest_state IN ('sealed', 'items-expired')" in sql
    assert "CREATE TRIGGER resource_inventory_snapshots_seal_only" in sql
    assert "BEFORE INSERT OR UPDATE ON resource_inventory_snapshots" in sql
    assert "CREATE TRIGGER resource_inventory_snapshot_items_staging_only" in sql
    assert "BEFORE INSERT OR UPDATE OR DELETE" in sql
    assert "INTERVAL '7 days'" in sql
    assert "ON DELETE CASCADE" not in sql
    assert sql.count("ON DELETE RESTRICT") >= 10


def test_app_ingestion_migration_has_fenced_replay_and_watch_contracts() -> None:
    sql = APP_INGESTION_MIGRATION.read_text()
    compact = _compact(sql)

    for table in (
        "resource_inventory_ingest_tickets",
        "resource_inventory_transport_nonces",
        "resource_inventory_watch_sessions",
        "resource_inventory_watch_events",
        "resource_inventory_shadow_comparisons",
    ):
        assert f"CREATE TABLE {table}" in sql
    assert "PRIMARY KEY (collector_id, request_nonce)" in sql
    assert "resource_inventory_transport_nonces_expiry_idx" in sql
    assert "max_snapshot_bytes" in sql
    assert "staged_bytes" in sql
    assert "resource_inventory_snapshot_item_size_bytes" in sql
    assert "resource_inventory_watch_events_session_ordinal_uq" in sql
    assert "resource_inventory_watch_sessions_live_scope_idx" in sql
    assert "expected_resource_version" in sql
    assert "event_type = 'bookmark'" in compact
    assert "event_type = 'history-lost'" in compact
    assert "mutation_action = 'history-gap'" in compact
    assert "resource_inventory_watch_events_immutable" in sql
    assert "resource_inventory_watch_sessions_one_way" in sql
    assert "recovery_from_epoch_id" in sql
    assert "resource_inventory_shadow_comparisons_unresolved_idx" in sql
    assert "legacy_started_at" in sql
    assert "observed_start_time_source" in sql
    assert "observed_start_uncertainty_us" in sql
    assert "start_delta_us" in sql
    assert "'lifetime-mismatch'" in sql
    assert "'start-semantics'" in sql
    assert "inventory watch event interval/gap postcondition failed" in sql
    assert "manifest_state = 'staging-expired'" in sql
    assert "INTERVAL '24 hours'" in sql
    assert sql.count("INTERVAL '7 days'") >= 4
    assert "only expired unbound inventory ingest tickets may be deleted" in sql


def test_app_ingestion_size_fix_uses_logical_json_bytes() -> None:
    sql = APP_INGESTION_SIZE_FIX.read_text()
    function_body = sql.split("AS $$", 1)[1]

    assert (
        "CREATE OR REPLACE FUNCTION resource_inventory_snapshot_item_size_bytes" in sql
    )
    assert "octet_length(normalized_item::TEXT)" in function_body
    assert "octet_length(item_error::TEXT)" in function_body
    assert "pg_column_size" not in function_body


def test_app_read_and_seal_indexes_are_online_and_source_specific() -> None:
    plan_sql = APP_PLAN_PERIOD_INDEX.read_text()
    interval_sql = APP_INTERVAL_OVERLAP_INDEX.read_text()
    snapshot_sql = APP_COMPLETE_SNAPSHOT_RECEIVED_INDEX.read_text()
    watch_sql = APP_INVALID_WATCH_RECEIVED_INDEX.read_text()
    sql = plan_sql + interval_sql + snapshot_sql + watch_sql
    compact = _compact(sql)

    assert "transactional: no" in sql
    assert plan_sql.count("CREATE INDEX CONCURRENTLY IF NOT EXISTS") == 1
    assert interval_sql.count("CREATE INDEX CONCURRENTLY IF NOT EXISTS") == 1
    assert snapshot_sql.count("CREATE INDEX CONCURRENTLY IF NOT EXISTS") == 1
    assert watch_sql.count("CREATE INDEX CONCURRENTLY IF NOT EXISTS") == 1
    assert "resource_publication_plans_period_idx" in sql
    assert "tstzrange(period_start, period_end, '[)')" in compact
    assert "resource_intervals_overlap_idx" in sql
    assert "tstzrange(started_at, ended_at, '[)')" in compact
    assert "resource_inventory_snapshots_complete_received_idx" in sql
    assert (
        "ON resource_inventory_snapshots (scope_epoch_id, received_at, id)" in compact
    )
    assert (
        "WHERE complete IS TRUE AND manifest_state IN ('sealed', 'items-expired')"
        in compact
    )
    assert "resource_inventory_watch_events_invalid_received_idx" in sql
    assert (
        "ON resource_inventory_watch_events (scope_epoch_id, received_at, id)"
        in compact
    )
    assert (
        "WHERE valid_for_metering IS FALSE "
        "AND mutation_action = 'presence-invalid'" in compact
    )
    assert "depends-on:    0090_infrastructure_interval_overlap_idx.notx.sql" in (
        snapshot_sql
    )
    assert "depends-on:    0091_inventory_complete_received_idx.notx.sql" in watch_sql
    assert snapshot_sql.count("INVALID index") == 1
    assert watch_sql.count("INVALID index") == 1


def test_interval_constraints_bind_identity_dimensions_and_time() -> None:
    sql = _compact(APP_MIGRATION.read_text())

    assert "resource_intervals_lifecycle_no_overlap EXCLUDE USING gist" in sql
    assert "source_lifecycle_id WITH =" in sql
    assert "resource_intervals_open_lifecycle_uq" in sql
    assert "resource_intervals_open_uq" in sql
    assert "resource_intervals_inventory_scope_cluster_fkey" in sql
    assert "resource_intervals_last_seen_snapshot_fkey" in sql
    assert "CREATE TRIGGER resource_intervals_scope_identity" in sql
    assert "CREATE TRIGGER resource_intervals_immutable_revision" in sql
    assert "FOREIGN KEY (current_interval_id, source_lifecycle_id)" in sql
    assert "AND (ended_at IS NULL OR ended_at >= started_at)" in sql
    assert "owner_kind IS NOT NULL AND owner_kind IN ('job', 'thread')" in sql
    assert "owner_id IS NOT NULL AND owner_id <> '' AND user_id IS NOT NULL" in sql
    assert "attribution_quality IN ('exact', 'derived')" in sql
    assert "resource_class = 'kubernetes-pod'" in sql
    assert "resource_class = 'virtual-machine'" in sql
    assert "resource_class = 'persistent-volume-claim'" in sql
    assert "resource_class = 'persistent-volume'" in sql
    assert "capacity_quality NOT IN ('unsupported', 'unknown', 'invalid')" in sql
    for confidence in (
        "backend-confirmed",
        "kubernetes-visible",
        "backend-unverified",
    ):
        assert confidence in sql


def test_app_migration_has_frozen_publication_and_rate_versions() -> None:
    sql = APP_MIGRATION.read_text()

    for table in (
        "usage_rates_v2",
        "usage_rate_card_versions_v2",
        "usage_rate_components_v2",
        "resource_lifecycle_heads",
        "resource_intervals",
        "resource_publication_plans",
        "resource_publication_plan_events",
    ):
        assert f"CREATE TABLE {table}" in sql

    assert "usage_rates_v2_no_overlap EXCLUDE USING gist" in sql
    assert "resource <> '*'" in sql
    assert "component_count" in sql
    assert "component_manifest_hash" in sql
    assert "usage_rate_card_versions_v2_manifest_complete" in sql
    assert "usage_rate_components_v2_manifest_complete" in sql
    assert "canonical_rate_version_id" in sql
    assert "UNIQUE (source, source_id, unit, ts)" in sql
    assert "resource_publication_plans_interval_revision_fkey" in sql
    assert "resource_publication_plan_events_plan_kind_time_fkey" in sql
    assert "event_set_hash" in sql
    assert "rate_selection_hash" in sql
    for trigger in (
        "resource_publication_plans_manifest_complete",
        "resource_publication_plan_events_manifest_complete",
    ):
        assert f"CREATE CONSTRAINT TRIGGER {trigger}" in sql
    for trigger in (
        "resource_publication_plans_frozen_intent",
        "resource_publication_plan_events_frozen",
        "usage_rates_v2_immutable",
        "usage_rate_card_versions_v2_immutable",
        "usage_rate_components_v2_immutable",
    ):
        assert f"CREATE TRIGGER {trigger}" in sql


def test_app_migration_has_v2_daily_and_bootstrap_gates() -> None:
    sql = _compact(APP_MIGRATION.read_text())

    for table in (
        "infra_usage_day_state",
        "usage_daily_v2",
        "usage_rollup_day_state",
        "usage_rollup_v2_bootstrap_state",
    ):
        assert f"CREATE TABLE {table}" in sql

    for measure in (
        "quantity NUMERIC(38, 18) NOT NULL",
        "cost_usd NUMERIC(38, 18)",
        "priced_quantity NUMERIC(38, 18) NOT NULL",
        "unpriced_quantity NUMERIC(38, 18) NOT NULL",
        "priced_events BIGINT NOT NULL",
        "unpriced_events BIGINT NOT NULL",
    ):
        assert measure in sql
    assert "usage_daily_v2_dims_uq" in sql
    assert "NULLS NOT DISTINCT" in sql
    assert "priced_events = 0 AND cost_usd IS NULL" in sql
    assert "priced_events > 0 AND cost_usd IS NOT NULL" in sql
    assert "VALUES (TRUE, 'pending')" in sql
    assert "VALUES ('usage_daily_v2', NULL)" in sql
    assert "reconciled_through_day = seeded_through_day" in sql
    assert "completed_at >= started_at" in sql
    assert "CREATE TRIGGER infra_usage_day_state_one_way_seal" in sql
    assert "OLD.state = 'sealed'" in sql
    assert "NEW.state NOT IN ('open', 'sealing')" in sql
    assert "NEW.state NOT IN ('sealing', 'sealed')" in sql


def test_audit_expansion_is_nullable_typed_and_validated_separately() -> None:
    expansion = AUDIT_EXPANSION.read_text()
    validation = AUDIT_VALIDATION.read_text()

    for column in (
        "period_start",
        "period_end",
        "measurement_basis",
        "cost_domain",
        "resource_class",
        "attribution_scope",
        "measurement_algorithm",
        "source_capacity_value",
        "source_capacity_unit",
        "source_cluster",
        "source_kind",
        "source_uid",
        "source_lifecycle_id",
        "source_interval_id",
        "event_kind",
        "corrects_source",
        "corrects_source_id",
        "corrects_unit",
        "corrects_ts",
        "correction_group_id",
        "correction_reason",
        "correction_actor_id",
        "discovered_at",
        "payload_hash",
    ):
        assert f"ADD COLUMN IF NOT EXISTS {column}" in expansion

    constraints = (
        "usage_events_period_bounds_v2_check",
        "usage_events_infra_v2_contract_check",
        "usage_events_event_kind_v2_check",
    )
    for constraint in constraints:
        assert f"ADD CONSTRAINT {constraint}" in expansion
        assert f"VALIDATE CONSTRAINT {constraint}" in validation
    assert expansion.count(") NOT VALID;") == len(constraints)
    assert "source_capacity_value = trunc(source_capacity_value)" in expansion
    assert "CREATE OR REPLACE FUNCTION round_half_even_v2" in expansion
    assert "SET search_path = pg_catalog" in expansion
    assert "abs(quantity) < 100000000000000000000::NUMERIC" in expansion
    assert "quantity = trunc(quantity, 18)" in expansion
    assert "abs(rate_usd)" in expansion
    assert "rate_usd = trunc(rate_usd, 18)" in expansion
    assert "cost_usd = round_half_even_v2(" in expansion
    assert "unit = 'vcpu-hour'" in expansion
    assert "source_capacity_unit = 'millicore'" in expansion
    assert "unit = 'claim-hour'" in expansion
    assert "unit = 'volume-hour'" in expansion
    assert "source_capacity_unit = 'instance'" in expansion
    assert "source_capacity_value = 1" in expansion
    assert "corrects_unit = unit" in expansion
    assert "corrects_ts = period_start" in expansion
    for numeric_column in (
        "source_capacity_value",
        "quantity",
        "rate_usd",
        "cost_usd",
    ):
        assert f"{numeric_column} NOT IN (" in expansion


def test_audit_dirty_day_trigger_is_statement_batched_and_bootstrapped() -> None:
    expansion = _compact(AUDIT_EXPANSION.read_text())
    validation = _compact(AUDIT_VALIDATION.read_text())

    assert "CREATE TABLE IF NOT EXISTS usage_rollup_dirty_days" in expansion
    assert "CREATE TRIGGER usage_events_rollup_dirty_days" in expansion
    assert "REFERENCING NEW TABLE AS inserted_usage_events" in expansion
    assert "FOR EACH STATEMENT" in expansion
    assert "GROUP BY (inserted.ts AT TIME ZONE 'UTC')::DATE" in expansion
    assert "usage_rollup_dirty_days.revision + 1" in expansion
    assert "CREATE TRIGGER usage_events_append_only_v2" in expansion
    assert "BEFORE UPDATE OR DELETE ON usage_events" in expansion
    assert "FROM usage_events GROUP BY (ts AT TIME ZONE 'UTC')::DATE" in validation
    assert "ON CONFLICT (day) DO NOTHING" in validation


def test_audit_project_window_index_documents_partitioned_build() -> None:
    sql = _compact(AUDIT_PROJECT_INDEX.read_text())

    assert "CREATE INDEX IF NOT EXISTS usage_events_project_ts_idx" in sql
    assert "ON usage_events (project_id, ts)" in sql
    assert "PostgreSQL 16 cannot" in AUDIT_PROJECT_INDEX.read_text()


def test_migration_heads_are_unique_and_snapshots_are_not_the_contract() -> None:
    app_files = discover(
        ROOT / "src" / "orchestrator" / "database" / "migrations" / "app"
    )
    audit_files = discover(
        ROOT / "src" / "orchestrator" / "database" / "migrations" / "audit"
    )

    for files in (app_files, audit_files):
        prefixes = [path.name.split("_", 1)[0] for path in files]
        assert len(prefixes) == len(set(prefixes))
    assert app_files[-1].name == APP_CURRENT_MIGRATION_HEAD.name
    assert audit_files[-1].name == AUDIT_PROJECT_INDEX.name
    assert "schema_current" not in APP_MIGRATION.read_text()
    assert "schema_current" not in APP_INGESTION_MIGRATION.read_text()
    assert "schema_current" not in APP_INGESTION_SIZE_FIX.read_text()
    assert "schema_current" not in APP_PLAN_PERIOD_INDEX.read_text()
    assert "schema_current" not in APP_INTERVAL_OVERLAP_INDEX.read_text()
    assert "schema_current" not in APP_COMPLETE_SNAPSHOT_RECEIVED_INDEX.read_text()
    assert "schema_current" not in APP_INVALID_WATCH_RECEIVED_INDEX.read_text()
    assert "schema_current" not in APP_DAY_SEQUENCE_BACKFILL_PREP.read_text()
    assert "schema_current" not in APP_TERMINAL_EVIDENCE_MIGRATION.read_text()
    assert "schema_current" not in APP_REFERENCED_RATE_GUARD_MIGRATION.read_text()
    assert "schema_current" not in APP_STORAGE_FOUNDATION_MIGRATION.read_text()
    assert "schema_current" not in APP_COMPUTE_FOUNDATION_MIGRATION.read_text()
    assert "schema_current" not in APP_AGENT_METERING_LOCK_ORDER_MIGRATION.read_text()
    assert "schema_current" not in APP_STORAGE_SOURCE_ACTIVATION_MIGRATION.read_text()
    assert "schema_current" not in APP_COMPUTE_SCOPE_EPOCH_GUARD_MIGRATION.read_text()
    assert "schema_current" not in APP_COMPUTE_SCOPE_AUTHORIZATION_MIGRATION.read_text()
    assert (
        "schema_current" not in APP_COMPUTE_EXACT_EPOCH_AUTHORITY_MIGRATION.read_text()
    )
    assert (
        "schema_current" not in APP_COMPUTE_EXACT_EPOCH_LIFECYCLE_MIGRATION.read_text()
    )
    assert "schema_current" not in APP_COMPUTE_EPOCH_ROLLOVER_MIGRATION.read_text()
    assert "schema_current" not in APP_COMPUTE_INITIAL_RECOVERY_AUTHORITY.read_text()
    assert (
        "schema_current"
        not in APP_COMPUTE_AUTHORITY_CONFIRMATION_GAP_MIGRATION.read_text()
    )
    assert (
        "schema_current"
        not in APP_COMPUTE_INTERVAL_EPOCH_SHAPE_REPAIR_MIGRATION.read_text()
    )
    assert "schema_current" not in APP_DATASOURCE_TOMBSTONES_MIGRATION.read_text()
    assert "schema_current" not in APP_THREAD_SESSION_DURABLE_STATE.read_text()
    assert (
        "schema_current" not in APP_PERSISTENT_INPUT_DELIVERY_CANCELLATION.read_text()
    )
    assert "schema_current" not in APP_THREAD_ENDED_TRANSITION_FENCE.read_text()
    assert "schema_current" not in APP_THREAD_RUNTIME_GENERATION_RETIREMENT.read_text()
    assert "audit_schema_current" not in AUDIT_EXPANSION.read_text()


def test_0183_persistent_input_cancellation_is_terminal_and_source_scoped() -> None:
    raw = APP_PERSISTENT_INPUT_DELIVERY_CANCELLATION.read_text()
    sql = _compact(raw)

    assert "-- migration:     0183_persistent_input_delivery_cancellation.sql" in raw
    assert "-- depends-on:    0182_deliverable_contract_authority.sql" in raw
    assert "PERSISTENT_INPUT_CANCELLATION_ENABLED" in raw
    assert "ADD COLUMN cancelled_at TIMESTAMPTZ" in sql
    assert "ADD COLUMN cancelled_turn_number BIGINT" in sql
    assert "ADD COLUMN cancelled_reason TEXT" in sql
    assert "'cancelled'" in sql
    assert "state = 'cancelled' AND source = 'direct_human'" in sql
    assert "cancelled_at IS NOT NULL" in sql
    assert "cancelled_turn_number > 0" in sql
    assert "btrim(cancelled_reason) <> ''" in sql
    assert "length(cancelled_reason) <= 120" in sql
    assert "UPDATE public.thread_messages" not in sql


def test_0184_ended_transition_fence_is_mixed_version_compatible() -> None:
    raw = APP_THREAD_ENDED_TRANSITION_FENCE.read_text()
    sql = _compact(raw)

    assert "-- migration:     0184_thread_ended_transition_fence.sql" in raw
    assert "-- depends-on:    0183_persistent_input_delivery_cancellation.sql" in raw
    assert "OLD.status = 'ended'" in sql
    assert "NEW.status NOT IN ('ended', 'created')" in sql
    assert "ERRCODE = '23514'" in sql
    assert "CONSTRAINT = 'threads_ended_transition_fence'" in sql
    assert "BEFORE UPDATE OF status ON public.threads" in sql
    assert "srw.explicit_thread_resume" not in raw

    snapshot = (ROOT / "src/orchestrator/database/schema_current.sql").read_text()
    assert "CREATE FUNCTION public.enforce_thread_ended_transition()" in snapshot
    assert "CREATE TRIGGER threads_ended_transition_fence BEFORE UPDATE OF" in snapshot


def test_0185_runtime_generation_and_retirement_authority_contract() -> None:
    raw = APP_THREAD_RUNTIME_GENERATION_RETIREMENT.read_text()
    sql = _compact(raw)

    assert "-- migration:     0185_thread_runtime_generation_retirement.sql" in raw
    assert "-- depends-on:    0184_thread_ended_transition_fence.sql" in raw
    assert "-- maintenance-gate: pinned-runtime-authority-v1" in raw
    assert "runtime_generation uuid" in sql
    assert "ALTER COLUMN runtime_generation SET NOT NULL" in sql
    assert "runtime_retirement_token uuid" in sql
    assert "runtime_retirement_authorized_at timestamptz" in sql
    assert "runtime_retirement_context jsonb" in sql
    assert "runtime_retirement_stage_receipt jsonb" in sql
    assert "runtime_retirement_local_quiescence jsonb" in sql
    assert "runtime_retirement_external_cleanup jsonb" in sql
    assert "runtime_authority_exposed boolean NOT NULL DEFAULT false" in sql
    assert "runtime_attach_token uuid" in sql
    assert "runtime_attach_abort_receipt jsonb" in sql
    assert "ALTER TABLE public.cloud_ro_mounts" in sql
    assert "ADD COLUMN IF NOT EXISTS runtime_generation uuid" in sql
    assert "ADD COLUMN IF NOT EXISTS engage_attempt uuid" in sql
    assert "ALTER TABLE public.thread_control_requests" in sql
    assert "CREATE TABLE IF NOT EXISTS public.thread_runtime_retirement_outcomes" in sql
    assert "PRIMARY KEY (thread_id, runtime_generation, retirement_token)" in sql
    assert (
        "CREATE INDEX IF NOT EXISTS idx_thread_runtime_retirement_outcomes_process"
        in sql
    )
    assert (
        "CREATE TABLE IF NOT EXISTS public.thread_runtime_attach_abort_outcomes" in sql
    )
    assert "CREATE TABLE IF NOT EXISTS public.thread_workspace_provision_intents" in sql
    assert (
        "CREATE OR REPLACE FUNCTION public.enforce_thread_workspace_provision_intent()"
        in sql
    )
    assert "thread_workspace_provision_intent_authority" in sql
    assert "status IN ('planned', 'published', 'revoking', 'fenced', 'retired')" in sql
    assert "gc_after >= fenced_at + interval '10 minutes'" in sql
    assert "pinned_retirement_workspace_provision_intent_retired" in sql
    assert "workspace_provision_fence_v1" in sql
    assert "threads_runtime_retirement_external_cleanup_shape" in sql
    assert "threads_runtime_retirement_external_cleanup_immutable" in sql
    assert "PRIMARY KEY (thread_id, runtime_generation, runtime_attach_token)" in sql
    for append_only_table in (
        "thread_runtime_retirement_outcomes",
        "thread_runtime_attach_abort_outcomes",
    ):
        table_definition = re.search(
            rf"CREATE TABLE IF NOT EXISTS public\.{append_only_table}\s*"
            r"\((.*?)\n\);",
            raw,
            re.DOTALL,
        )
        assert table_definition is not None
        assert "REFERENCES public.threads" not in table_definition.group(1)
    assert "threads_runtime_retirement_shape" in sql
    assert "threads_runtime_retirement_stage_receipt_shape" in sql
    assert "threads_runtime_retirement_local_quiescence_shape" in sql
    assert "NEW.runtime_generation := public.uuid_generate_v4()" in sql
    assert "threads_runtime_generation_immutable" in sql
    assert "threads_runtime_authority_exposed_immutable" in sql
    assert "threads_runtime_attach_abort_receipt" in sql
    assert "threads_runtime_retirement_authorized_immutable" in sql
    assert "threads_runtime_retirement_local_quiescence_identity" in sql
    assert "threads_runtime_retirement_stage_receipt_pending" in sql
    assert "threads_runtime_retirement_stage_receipt_source" in sql
    assert "threads_runtime_retirement_pending" in sql
    assert "workspace_process_zero_v1" in sql
    assert "agent_runtime_zero_v1" in sql
    assert "workspace_actuator_zero_v1" in sql

    snapshot = (ROOT / "src/orchestrator/database/schema_current.sql").read_text()
    assert "runtime_generation uuid" in snapshot
    assert "runtime_retirement_token uuid" in snapshot
    assert "runtime_retirement_authorized_at timestamp with time zone" in snapshot
    assert "runtime_retirement_stage_receipt jsonb" in snapshot
    assert "runtime_retirement_local_quiescence jsonb" in snapshot
    assert "runtime_authority_exposed boolean DEFAULT false NOT NULL" in snapshot
    assert "runtime_attach_abort_receipt jsonb" in snapshot
    assert "CREATE TABLE public.thread_runtime_retirement_outcomes" in snapshot
    assert "CREATE TABLE public.thread_runtime_attach_abort_outcomes" in snapshot
    assert "idx_thread_runtime_retirement_outcomes_process" in snapshot
    assert "threads_runtime_retirement_shape" in snapshot
    assert "threads_runtime_retirement_authorized_immutable" in snapshot
    assert "threads_runtime_authority_exposed_immutable" in snapshot


def test_0186_protected_cloud_instance_and_attempt_authority_contract() -> None:
    raw = APP_PROTECTED_CLOUD_INSTANCE_AUTHORITY.read_text()
    sql = _compact(raw)

    assert "-- migration:     0186_protected_cloud_instance_authority.sql" in raw
    assert "-- depends-on:    0185_thread_runtime_generation_retirement.sql" in raw
    assert "-- maintenance-gate: pinned-runtime-authority-v1" in raw
    assert "CREATE TABLE IF NOT EXISTS public.main_cloud_backend_instances" in sql
    assert "CREATE TABLE IF NOT EXISTS public.main_cloud_active_backend" in sql
    assert "CREATE TABLE IF NOT EXISTS public.cloud_ro_effect_intents" in sql
    assert "ADD COLUMN IF NOT EXISTS backend_instance_id uuid" in sql
    assert "ADD COLUMN IF NOT EXISTS source_binding jsonb" in sql
    assert "ADD COLUMN IF NOT EXISTS source_binding_sha256 text" in sql
    assert "status IN ('engaging', 'active', 'revoking', 'revoked')" in sql
    assert "cloud_ro_mounts_instance_migration_authority" in sql
    assert "enforce_cloud_ro_mount_authority_shape" in sql
    assert "cloud_ro_mounts_selected_source" in sql
    assert "'srw-reader-a-' || replace(NEW.engage_attempt::text, '-', '')" in sql
    assert "'srw-rog-a-' || replace(NEW.engage_attempt::text, '-', '')" in sql
    assert "pg_catalog.sha256" in sql
    assert "cloud_ro_mounts_attempt_retained" in sql
    assert "enforce_cloud_ro_effect_intent_insert_authority" in sql
    assert "cloud_ro_effect_intents_insert_authority" in sql
    assert "cloud_ro_effect_intents_horizon_authority" in sql
    assert "effect.safe_after > clock_timestamp()" in sql


def test_managed_repository_legacy_reconciliation_migration_is_additive() -> None:
    raw = APP_MANAGED_REPOSITORY_LEGACY_RECONCILIATION.read_text()
    sql = _compact(raw)

    assert "depends-on:    0189_compute_initial_recovery_epoch_authority.sql" in raw
    assert "transactional: yes" in raw
    assert "SET LOCAL lock_timeout = '2s'" in sql
    assert "CREATE TABLE public.managed_repository_legacy_reconciliations" in sql
    assert "CREATE SEQUENCE public.managed_repository_legacy_reconcile_claim_seq" in sql
    assert "managed_repository_legacy_source_unique" in sql
    assert "managed_repository_legacy_claim_shape_check" in sql
    assert "managed_repository_legacy_completion_shape_check" in sql
    assert "UPDATE jobs" not in sql
    assert "UPDATE threads" not in sql
    assert "UPDATE project_repositories" not in sql


def test_stateless_input_delivery_migration_fences_lane_and_old_claims() -> None:
    raw = APP_STATELESS_INPUT_DELIVERIES.read_text()
    sql = _compact(raw)

    assert "depends-on:    0190_managed_repository_legacy_reconciliation.sql" in raw
    assert "transactional: yes" in raw
    assert "SET LOCAL lock_timeout = '2s'" in sql
    thread_lock = "LOCK TABLE public.threads IN SHARE ROW EXCLUSIVE MODE"
    queue_lock = "LOCK TABLE public.run_queue IN SHARE ROW EXCLUSIVE MODE"
    delivery_lock = (
        "LOCK TABLE public.thread_input_deliveries IN SHARE ROW EXCLUSIVE MODE"
    )
    assert thread_lock in sql
    assert queue_lock in sql
    assert delivery_lock in sql
    assert sql.index(thread_lock) < sql.index(queue_lock) < sql.index(delivery_lock)
    assert "ADD COLUMN execution_lane TEXT NOT NULL DEFAULT 'pinned'" in sql
    assert "stateless_input_delivery_history_ambiguous" in sql
    assert "SET execution_lane = thread.execution_lane" not in sql
    assert "SET execution_lane = 'stateless'" in sql
    assert "delivery.state = 'persisted'" in sql
    assert "delivery.claim_generation = 0" in sql
    assert sql.count("message.turn_number IS NULL") == 3
    assert (
        "SET turn_number = (eligible.base_turn + eligible.turn_offset)::integer" in sql
    )
    assert sql.count("NOT VALID") == 3
    assert "ADD COLUMN input_delivery_capable_lease_token BIGINT" in sql
    assert "trg_input_delivery_lane_authority" in sql
    assert "trg_stateless_input_delivery_claim" in sql
    assert "stateless_input_delivery_requires_capable_claim" in sql
    assert "stateless_input_delivery_requires_admission" in sql

    validation = _compact(APP_STATELESS_INPUT_DELIVERY_VALIDATION.read_text())
    assert "depends-on:    0191_stateless_input_deliveries.sql" in (
        APP_STATELESS_INPUT_DELIVERY_VALIDATION.read_text()
    )
    assert validation.count("VALIDATE CONSTRAINT thread_input_deliveries_") == 3


@pytest.mark.asyncio
async def test_0185_serializes_real_predecessor_rows_with_lane_changes(
    app_pg_dsn: str,
    tmp_path: Path,
) -> None:
    """Genuine 0184 pending and terminal history keep distinct authority."""

    dbname = f"stateless_delivery_0185_{uuid4().hex[:12]}"
    admin = await asyncpg.connect(app_pg_dsn)
    try:
        await admin.execute(f'CREATE DATABASE "{dbname}"')
    finally:
        await admin.close()

    dsn = _swap_db(app_pg_dsn, dbname)
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
    through_0184 = tmp_path / "through-0184"
    through_0184.mkdir()
    blocker = updater = observer = None
    migration_task = update_task = None
    try:
        for path in discover(
            ROOT / "src" / "orchestrator" / "database" / "migrations" / "app"
        ):
            if path.name >= APP_STATELESS_INPUT_DELIVERIES.name:
                break
            (through_0184 / path.name).write_bytes(path.read_bytes())
        await run_migrations(pool, through_0184)

        thread_id = uuid4()
        delivery_id = uuid4()
        message_id = message_row_id(delivery_id)
        terminal_thread_id = uuid4()
        terminal_message_id = uuid4()
        terminal_delivery_id = uuid4()
        terminal_agent_id = uuid4()
        terminal_runtime_generation = uuid4()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO threads (id,status,execution_lane,config_name) "
                "VALUES ($1,'active','stateless','default')",
                thread_id,
            )
            await conn.execute(
                "INSERT INTO thread_messages "
                "(id,thread_id,role,content,turn_number) "
                "VALUES ($1,$2,'event','genuine pre-0185 wake',NULL)",
                message_id,
                thread_id,
            )
            # This is the exact shape the preceding release produced: 0174's
            # pinned-only ledger had no lane column, while orchestrator-side
            # wake persistence could still target a stateless thread.
            await conn.execute(
                "INSERT INTO thread_input_deliveries "
                "(delivery_id,thread_id,message_id,source,state) "
                "VALUES ($1,$2,$3,'officer_wake','persisted')",
                delivery_id,
                thread_id,
                message_id,
            )
            # This is the other genuine predecessor shape: a pinned runtime
            # admitted and settled its delivery, after which the detached
            # thread moved to stateless. The current thread lane must never
            # rewrite that immutable pinned execution receipt.
            await conn.execute(
                "INSERT INTO threads "
                "(id,status,execution_lane,config_name,total_turns) "
                "VALUES ($1,'active','pinned','default',2)",
                terminal_thread_id,
            )
            await conn.execute(
                "INSERT INTO thread_messages "
                "(id,thread_id,role,content,turn_number) "
                "VALUES ($1,$2,'event','settled pinned wake',2)",
                terminal_message_id,
                terminal_thread_id,
            )
            await conn.execute(
                "INSERT INTO thread_input_deliveries "
                "(delivery_id,thread_id,message_id,source,state,claim_generation,"
                " owner_agent_id,owner_pod_uid,owner_runtime_generation,"
                " admitted_turn_number,admitted_at,settled_at) "
                "VALUES ($1,$2,$3,'officer_wake','settled',1,$4,'old-pod',$5,"
                " 2,now(),now())",
                terminal_delivery_id,
                terminal_thread_id,
                terminal_message_id,
                terminal_agent_id,
                terminal_runtime_generation,
            )
            await conn.execute(
                "UPDATE threads SET execution_lane='stateless' WHERE id=$1",
                terminal_thread_id,
            )

        blocker = await asyncpg.connect(dsn)
        updater = await asyncpg.connect(dsn)
        observer = await asyncpg.connect(dsn)
        await blocker.execute("BEGIN")
        # Hold the migration after it takes the thread/run_queue prefix but
        # before it can alter/backfill the delivery table.
        await blocker.execute(
            "LOCK TABLE thread_input_deliveries IN ACCESS EXCLUSIVE MODE"
        )
        # LOAD-BEARING: replay the LIVE migrations directory, not a bounded
        # copy. This test seeds threads rows and then replays a real upgrade
        # span, which is the only condition in the suite that reproduces a
        # migration whose DDL collides with deferred trigger events queued by
        # an earlier migration in the same transactional pass (they share one
        # transaction). 0202 hit exactly that and would have shipped a boot
        # hard-fail had this replay been narrowed to dodge it. Narrowing it
        # again removes the coverage; fix the migration instead.
        migration_task = asyncio.create_task(
            run_migrations(
                pool, ROOT / "src" / "orchestrator" / "database" / "migrations" / "app"
            )
        )

        for _ in range(100):
            migration_holds_thread = await observer.fetchval(
                "SELECT EXISTS ("
                "SELECT 1 FROM pg_locks "
                "WHERE database=(SELECT oid FROM pg_database "
                "WHERE datname=current_database()) "
                "AND relation='public.threads'::regclass "
                "AND mode='ShareRowExclusiveLock' AND granted)"
            )
            if migration_holds_thread:
                break
            await asyncio.sleep(0.005)
        assert migration_holds_thread

        update_task = asyncio.create_task(
            updater.execute(
                "UPDATE threads SET execution_lane='pinned' WHERE id=$1",
                thread_id,
            )
        )
        await asyncio.sleep(0.05)
        assert not update_task.done()

        await blocker.execute("COMMIT")
        await asyncio.wait_for(migration_task, timeout=15)
        with pytest.raises(asyncpg.CheckViolationError, match="durable input"):
            await asyncio.wait_for(update_task, timeout=5)

        row = await observer.fetchrow(
            "SELECT thread.execution_lane AS thread_lane, "
            "delivery.execution_lane AS delivery_lane, delivery.state, "
            "message.turn_number, thread.total_turns "
            "FROM threads AS thread "
            "JOIN thread_input_deliveries AS delivery "
            "ON delivery.thread_id=thread.id "
            "JOIN thread_messages AS message ON message.id=delivery.message_id "
            "WHERE delivery.delivery_id=$1",
            delivery_id,
        )
        assert dict(row) == {
            "thread_lane": "stateless",
            "delivery_lane": "stateless",
            "state": "persisted",
            "turn_number": 1,
            "total_turns": 1,
        }
        async with observer.transaction():
            replay = await persist_input_delivery(
                observer,
                thread_id=thread_id,
                delivery_id=delivery_id,
                role="event",
                content="genuine pre-0185 wake",
                source="officer_wake",
                turn_number=None,
            )
        assert replay["transcript_inserted"] is False
        assert replay["execution_lane"] == "stateless"
        assert replay["state"] == "queued"
        assert replay["turn_number"] == 1
        assert (
            await observer.fetchval(
                "SELECT state FROM run_queue WHERE unit_id=$1", thread_id
            )
            == "queued"
        )
        terminal = await observer.fetchrow(
            "SELECT thread.execution_lane AS thread_lane, "
            "delivery.execution_lane AS delivery_lane, delivery.state, "
            "delivery.claim_generation, delivery.owner_agent_id, "
            "message.turn_number, thread.total_turns "
            "FROM threads AS thread "
            "JOIN thread_input_deliveries AS delivery "
            "ON delivery.thread_id=thread.id "
            "JOIN thread_messages AS message ON message.id=delivery.message_id "
            "WHERE delivery.delivery_id=$1",
            terminal_delivery_id,
        )
        assert dict(terminal) == {
            "thread_lane": "stateless",
            "delivery_lane": "pinned",
            "state": "settled",
            "claim_generation": 1,
            "owner_agent_id": terminal_agent_id,
            "turn_number": 2,
            "total_turns": 2,
        }
        assert await observer.fetchval(
            "SELECT success FROM schema_migrations WHERE filename=$1",
            APP_STATELESS_INPUT_DELIVERIES.name,
        )
        assert await observer.fetchval(
            "SELECT success FROM schema_migrations WHERE filename=$1",
            APP_STATELESS_INPUT_DELIVERY_VALIDATION.name,
        )
        assert (
            await observer.fetchval(
                "SELECT count(*) FROM pg_constraint "
                "WHERE conname = ANY($1::text[]) AND convalidated",
                [
                    "thread_input_deliveries_lane_check",
                    "thread_input_deliveries_owner_shape",
                    "thread_input_deliveries_claim_shape",
                ],
            )
            == 3
        )
    finally:
        if migration_task is not None and not migration_task.done():
            migration_task.cancel()
        if update_task is not None and not update_task.done():
            update_task.cancel()
        if blocker is not None:
            await blocker.close()
        if updater is not None:
            await updater.close()
        if observer is not None:
            await observer.close()
        await pool.close()
        admin = await asyncpg.connect(app_pg_dsn)
        try:
            await admin.execute(f'DROP DATABASE "{dbname}" WITH (FORCE)')
        finally:
            await admin.close()


@pytest.mark.asyncio
async def test_0185_refuses_claimed_pending_history_on_stateless_thread(
    app_pg_dsn: str,
    tmp_path: Path,
) -> None:
    """Do not guess that a pinned predecessor claim became stateless work."""

    dbname = f"stateless_delivery_0185_ambiguous_{uuid4().hex[:8]}"
    admin = await asyncpg.connect(app_pg_dsn)
    try:
        await admin.execute(f'CREATE DATABASE "{dbname}"')
    finally:
        await admin.close()

    dsn = _swap_db(app_pg_dsn, dbname)
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
    through_0184 = tmp_path / "ambiguous-through-0184"
    through_0184.mkdir()
    try:
        for path in discover(
            ROOT / "src" / "orchestrator" / "database" / "migrations" / "app"
        ):
            if path.name >= APP_STATELESS_INPUT_DELIVERIES.name:
                break
            (through_0184 / path.name).write_bytes(path.read_bytes())
        await run_migrations(pool, through_0184)

        thread_id = uuid4()
        message_id = uuid4()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO threads (id,status,execution_lane,config_name) "
                "VALUES ($1,'active','stateless','default')",
                thread_id,
            )
            await conn.execute(
                "INSERT INTO thread_messages "
                "(id,thread_id,role,content,turn_number) "
                "VALUES ($1,$2,'event','claimed predecessor wake',NULL)",
                message_id,
                thread_id,
            )
            await conn.execute(
                "INSERT INTO thread_input_deliveries "
                "(delivery_id,thread_id,message_id,source,state,claim_generation,"
                " owner_agent_id,owner_pod_uid,owner_runtime_generation) "
                "VALUES ($1,$2,$3,'officer_wake','owned',1,$4,'old-pod',$5)",
                uuid4(),
                thread_id,
                message_id,
                uuid4(),
                uuid4(),
            )

        with pytest.raises(
            asyncpg.CheckViolationError,
            match="Pre-0191 stateless input history is ambiguous",
        ):
            await run_migrations(
                pool, ROOT / "src" / "orchestrator" / "database" / "migrations" / "app"
            )

        async with pool.acquire() as conn:
            # Transactional failure rolls every 0185 catalog mutation back.
            assert not await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
                "WHERE table_schema='public' "
                "AND table_name='thread_input_deliveries' "
                "AND column_name='execution_lane')"
            )
            failure = await conn.fetchrow(
                "SELECT success, error FROM schema_migrations WHERE filename=$1",
                APP_STATELESS_INPUT_DELIVERIES.name,
            )
            assert failure is not None
            assert failure["success"] is False
            assert (
                "stateless input history is ambiguous" in str(failure["error"]).lower()
            )
            assert not await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE filename=$1)",
                APP_STATELESS_INPUT_DELIVERY_VALIDATION.name,
            )
    finally:
        await pool.close()
        admin = await asyncpg.connect(app_pg_dsn)
        try:
            await admin.execute(f'DROP DATABASE "{dbname}" WITH (FORCE)')
        finally:
            await admin.close()


@pytest.mark.asyncio
async def test_0185_refuses_numbered_pending_history_on_stateless_thread(
    app_pg_dsn: str,
    tmp_path: Path,
) -> None:
    """A numbered predecessor input is not the known durable-wake shape."""

    dbname = f"stateless_delivery_0185_numbered_{uuid4().hex[:8]}"
    admin = await asyncpg.connect(app_pg_dsn)
    try:
        await admin.execute(f'CREATE DATABASE "{dbname}"')
    finally:
        await admin.close()

    dsn = _swap_db(app_pg_dsn, dbname)
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
    through_0184 = tmp_path / "numbered-through-0184"
    through_0184.mkdir()
    try:
        for path in discover(
            ROOT / "src" / "orchestrator" / "database" / "migrations" / "app"
        ):
            if path.name >= APP_STATELESS_INPUT_DELIVERIES.name:
                break
            (through_0184 / path.name).write_bytes(path.read_bytes())
        await run_migrations(pool, through_0184)

        thread_id = uuid4()
        message_id = uuid4()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO threads "
                "(id,status,execution_lane,config_name,total_turns) "
                "VALUES ($1,'active','stateless','default',7)",
                thread_id,
            )
            await conn.execute(
                "INSERT INTO thread_messages "
                "(id,thread_id,role,content,turn_number) "
                "VALUES ($1,$2,'event','numbered predecessor wake',7)",
                message_id,
                thread_id,
            )
            await conn.execute(
                "INSERT INTO thread_input_deliveries "
                "(delivery_id,thread_id,message_id,source,state,claim_generation) "
                "VALUES ($1,$2,$3,'officer_wake','persisted',0)",
                uuid4(),
                thread_id,
                message_id,
            )

        with pytest.raises(
            asyncpg.CheckViolationError,
            match="Pre-0191 stateless input history is ambiguous",
        ):
            await run_migrations(
                pool, ROOT / "src" / "orchestrator" / "database" / "migrations" / "app"
            )

        async with pool.acquire() as conn:
            assert not await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
                "WHERE table_schema='public' "
                "AND table_name='thread_input_deliveries' "
                "AND column_name='execution_lane')"
            )
    finally:
        await pool.close()
        admin = await asyncpg.connect(app_pg_dsn)
        try:
            await admin.execute(f'DROP DATABASE "{dbname}" WITH (FORCE)')
        finally:
            await admin.close()


def test_deployed_pre_registration_migration_history_is_immutable() -> None:
    assert hashlib.sha256(
        APP_DEPLOYED_PRE_REGISTRATION_SANDBOX_ZERO.read_bytes()
    ).hexdigest() == (
        "433c41d5e575d8d9da93dde29218a24bd7d3fb57c99e99e9dedcd46d35f22de2"
    )
    assert hashlib.sha256(
        APP_DEPLOYED_PRE_REGISTRATION_DELETE_SANDBOX_ZERO.read_bytes()
    ).hexdigest() == (
        "6f0ed6731a055d9795ef69e07147a7c8931efeff77ef6b25e7f38808b540bb7d"
    )
    assert (
        "-- depends-on:    0186_protected_cloud_instance_authority.sql"
        in APP_DEPLOYED_PRE_REGISTRATION_SANDBOX_ZERO.read_text()
    )
    assert (
        "-- depends-on:    0187_pre_registration_sandbox_zero.sql"
        in APP_DEPLOYED_PRE_REGISTRATION_DELETE_SANDBOX_ZERO.read_text()
    )


@pytest.mark.asyncio
async def test_deployed_0188_history_upgrades_to_current_head(
    app_pg_dsn: str,
    tmp_path: Path,
) -> None:
    """The exact main-dev ledger advances without renaming applied files."""

    dbname = f"deployed_0188_upgrade_{uuid4().hex[:12]}"
    admin = await asyncpg.connect(app_pg_dsn)
    try:
        await admin.execute(f'CREATE DATABASE "{dbname}"')
    finally:
        await admin.close()

    dsn = _swap_db(app_pg_dsn, dbname)
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
    deployed = tmp_path / "deployed-through-0188"
    deployed.mkdir()
    try:
        for path in discover(
            ROOT / "src" / "orchestrator" / "database" / "migrations" / "app"
        ):
            if path.name > APP_DEPLOYED_PRE_REGISTRATION_DELETE_SANDBOX_ZERO.name:
                break
            (deployed / path.name).write_bytes(path.read_bytes())

        await run_migrations(pool, deployed)
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT filename, checksum FROM schema_migrations "
                "WHERE filename IN ($1,$2) ORDER BY filename",
                APP_DEPLOYED_PRE_REGISTRATION_SANDBOX_ZERO.name,
                APP_DEPLOYED_PRE_REGISTRATION_DELETE_SANDBOX_ZERO.name,
            )
            assert [(row["filename"], row["checksum"]) for row in rows] == [
                (
                    APP_DEPLOYED_PRE_REGISTRATION_SANDBOX_ZERO.name,
                    "433c41d5e575d8d9da93dde29218a24bd7d3fb57c99e99e9dedcd46d35f22de2",
                ),
                (
                    APP_DEPLOYED_PRE_REGISTRATION_DELETE_SANDBOX_ZERO.name,
                    "6f0ed6731a055d9795ef69e07147a7c8931efeff77ef6b25e7f38808b540bb7d",
                ),
            ]

        await run_migrations(
            pool, ROOT / "src" / "orchestrator" / "database" / "migrations" / "app"
        )
        async with pool.acquire() as conn:
            assert await conn.fetchval(
                "SELECT success FROM schema_migrations WHERE filename=$1",
                APP_CURRENT_MIGRATION_HEAD.name,
            )
            assert not await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE success=FALSE)"
            )
            ended_definition = await conn.fetchval(
                "SELECT pg_get_functiondef("
                "'public.enforce_thread_ended_transition()'::regprocedure)"
            )
            delete_definition = await conn.fetchval(
                "SELECT pg_get_functiondef("
                "'public.enforce_pinned_thread_delete_authority()'::regprocedure)"
            )
            assert "sandbox_actuator_zero_v1" in ended_definition
            assert "IS DISTINCT FROM expected_protocol" in delete_definition
    finally:
        await pool.close()
        admin = await asyncpg.connect(app_pg_dsn)
        try:
            await admin.execute(f'DROP DATABASE "{dbname}" WITH (FORCE)')
        finally:
            await admin.close()


def test_0177_is_bounded_thread_only_and_keeps_0176_immutable() -> None:
    raw = APP_MANAGED_REPOSITORY_THREAD_DETACH.read_text()
    sql = _compact(raw)

    assert "-- migration:     0177_managed_repository_thread_detach.sql" in raw
    assert "-- depends-on:    0176_managed_repository_authorities.sql" in raw
    assert "-- expected:" in raw
    assert "-- locks:" in raw
    assert "-- transactional: yes" in raw
    assert "SET LOCAL lock_timeout = '2s'" in sql
    assert "SET LOCAL statement_timeout = '15min'" in sql
    assert "SET LOCAL idle_in_transaction_session_timeout = '5min'" in sql
    assert "SET LOCAL timezone = 'UTC'" in sql
    assert "AND OLD.agent_id IS DISTINCT FROM NEW.agent_id" in sql
    assert "AND NEW.agent_id IS NOT NULL" in sql
    assert "DROP TRIGGER trg_managed_thread_repository_url_authority" in sql
    assert "ON public.threads" in sql
    assert "ON public.jobs" not in sql
    assert "ON public.project_repositories" not in sql
    assert hashlib.sha256(
        APP_MANAGED_REPOSITORY_AUTHORITIES.read_bytes()
    ).hexdigest() == (
        "4f74a15db19b9234100b2c3cc93b756bda179b93db5c7d691080d0c7fb1d726e"
    )


@pytest.mark.asyncio
async def test_0177_repairs_deployed_legacy_thread_detach_without_opening_attach(
    app_pg_dsn: str,
    tmp_path: Path,
) -> None:
    """Exercise the exact 0175 history -> 0176 failure -> 0177 repair."""

    dbname = f"managed_thread_detach_{uuid4().hex[:12]}"
    admin = await asyncpg.connect(app_pg_dsn)
    try:
        await admin.execute(f'CREATE DATABASE "{dbname}"')
    finally:
        await admin.close()

    dsn = _swap_db(app_pg_dsn, dbname)
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
    through_0175 = tmp_path / "through-0175"
    through_0176 = tmp_path / "through-0176"
    through_0177 = tmp_path / "through-0177"
    through_0175.mkdir()
    through_0176.mkdir()
    through_0177.mkdir()
    try:
        for path in discover(
            ROOT / "src" / "orchestrator" / "database" / "migrations" / "app"
        ):
            if path.name < APP_MANAGED_REPOSITORY_AUTHORITIES.name:
                (through_0175 / path.name).write_bytes(path.read_bytes())
            if path.name <= APP_MANAGED_REPOSITORY_AUTHORITIES.name:
                (through_0176 / path.name).write_bytes(path.read_bytes())
            if path.name <= APP_MANAGED_REPOSITORY_THREAD_DETACH.name:
                (through_0177 / path.name).write_bytes(path.read_bytes())

        await run_migrations(pool, through_0175)
        thread_id = uuid4()
        old_agent_id = uuid4()
        replacement_agent_id = uuid4()
        repo_name = f"thread-{str(thread_id)[:8]}"
        legacy_url = f"http://admin:shared-secret@gitea:3000/srw/{repo_name}.git"
        async with pool.acquire() as conn:
            # Exact prior-release shape: already-bound thread, ordinary
            # workspace_container coordinates, no 0176 authority state.
            await conn.execute(
                "INSERT INTO threads "
                "(id, status, execution_lane, config_name, metadata) "
                "VALUES ($1, 'active', 'pinned', 'centurion', "
                "jsonb_build_object('workspace_container', "
                "jsonb_build_object('repo_name', $2::text, "
                "'git_remote_url', $3::text)))",
                thread_id,
                repo_name,
                legacy_url,
            )
            await conn.execute(
                "INSERT INTO agents "
                "(id, config_name, hostname, status, agent_mode, thread_id) "
                "VALUES ($1, 'centurion', 'legacy-officer-pod', 'session', "
                "'persistent', $2)",
                old_agent_id,
                thread_id,
            )
            await conn.execute(
                "UPDATE threads SET agent_id=$2 WHERE id=$1",
                thread_id,
                old_agent_id,
            )

        await run_migrations(pool, through_0176)
        async with pool.acquire() as conn:
            with pytest.raises(asyncpg.exceptions.CheckViolationError) as deployed:
                await conn.execute(
                    "UPDATE threads SET agent_id=NULL WHERE id=$1",
                    thread_id,
                )
            assert (
                deployed.value.constraint_name
                == "managed_repository_url_must_be_credential_free"
            )

        await run_migrations(pool, through_0177)
        async with pool.acquire() as conn:
            assert (
                await conn.execute(
                    "UPDATE threads SET agent_id=NULL WHERE id=$1 AND agent_id=$2",
                    thread_id,
                    old_agent_id,
                )
                == "UPDATE 1"
            )
            assert (
                await conn.fetchval(
                    "SELECT metadata->'workspace_container'->>'git_remote_url' "
                    "FROM threads WHERE id=$1",
                    thread_id,
                )
                == legacy_url
            )
            await conn.execute(
                "INSERT INTO agents "
                "(id, config_name, hostname, status, agent_mode, thread_id) "
                "VALUES ($1, 'centurion', 'replacement-officer-pod', "
                "'session', 'persistent', $2)",
                replacement_agent_id,
                thread_id,
            )
            with pytest.raises(asyncpg.exceptions.CheckViolationError) as attach:
                await conn.execute(
                    "UPDATE threads SET agent_id=$2 WHERE id=$1",
                    thread_id,
                    replacement_agent_id,
                )
            assert (
                attach.value.constraint_name
                == "managed_repository_url_must_be_credential_free"
            )
            assert await conn.fetchval(
                "SELECT success FROM schema_migrations WHERE filename=$1",
                APP_MANAGED_REPOSITORY_THREAD_DETACH.name,
            )
    finally:
        await pool.close()
        admin = await asyncpg.connect(app_pg_dsn)
        try:
            await admin.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        finally:
            await admin.close()


def test_control_inbox_migration_preserves_and_constrains_narration_receipts() -> None:
    sql = _compact(APP_THREAD_CONTROL_INBOX_MIGRATION.read_text())
    index_sql = _compact(APP_THREAD_CONTROL_RECEIPT_INDEX.read_text())
    validation_sql = _compact(APP_THREAD_CONTROL_VALIDATION.read_text())

    assert "ADD COLUMN narration_mode TEXT" in sql
    assert "ADD COLUMN control_admission_agent_id UUID" in sql
    assert "control_admission_open" not in sql
    assert "narration_mode TEXT NOT NULL" not in sql
    assert "narration_mode TEXT NOT NULL DEFAULT 'auto'" not in sql
    assert (
        "metadata #>> '{config_override,interactive,narration_mode}'" in validation_sql
    )
    assert "IN ('silent', 'verbose', 'auto')" in validation_sql
    assert "CONSTRAINT valid_narration_mode" in sql
    assert "CHECK (narration_mode IN ('silent', 'verbose', 'auto')) NOT VALID" in sql
    assert "CONSTRAINT uq_thread_control_identity UNIQUE (id, thread_id)" in sql
    assert "FOREIGN KEY (control_request_id, thread_id)" in sql
    assert "REFERENCES thread_control_requests(id, thread_id) NOT VALID" in sql
    assert "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS" in index_sql
    assert "idx_thread_events_control_request" in index_sql
    assert "VALIDATE CONSTRAINT valid_narration_mode" in validation_sql
    assert (
        "VALIDATE CONSTRAINT thread_events_control_request_thread_fkey"
        in validation_sql
    )


def test_stateless_cloud_sync_migrations_keep_resource_and_baseline_fences() -> None:
    generations = _compact(APP_THREAD_CLOUD_SYNC_GENERATIONS.read_text())
    baselines = _compact(APP_THREAD_CLOUD_SYNC_BASELINES.read_text())
    marker_comment = _compact(APP_CLOUD_SYNC_MARKER_COMMENT.read_text())

    assert "PRIMARY KEY (thread_id, mount_id)" in generations
    assert "acknowledged_generation <= required_generation" in generations
    assert "WHERE acknowledged_generation < required_generation" in generations
    assert "workspace_generation TEXT NOT NULL" in generations
    assert "sync_scope_sha256 CHAR(64) NOT NULL" in generations
    assert "baseline_manifest JSONB NOT NULL DEFAULT '{}'::jsonb" in baselines
    assert "octet_length(baseline_manifest::text) <= 4194304" in baselines
    assert "baseline_sha256 CHAR(64) NOT NULL" in baselines
    assert "thread_cloud_sync_baseline_digest_shape" in baselines
    assert "The resource commit" in marker_comment
    assert "marker binds this digest" in marker_comment
    assert "Resource marker v2" not in marker_comment


def test_thread_client_presence_is_ttl_only_and_not_an_authority() -> None:
    presence = _compact(APP_THREAD_CLIENT_PRESENCE.read_text())

    assert "thread_id UUID PRIMARY KEY" in presence
    assert "REFERENCES threads(id) ON DELETE CASCADE" in presence
    assert "CHECK (expires_at > refreshed_at)" in presence
    assert "idx_thread_client_presence_expires_at" in presence
    assert "disconnect never deletes it" in presence
    assert "never authorization, queue ownership" in presence
    assert "fencing token" in presence


def test_canvas_editor_awareness_is_monotonic_ttl_courtesy_state() -> None:
    awareness = _compact(APP_CANVAS_EDITOR_AWARENESS.read_text())

    assert "PRIMARY KEY (thread_id, canvas_id, editing_session_id)" in awareness
    assert "REFERENCES canvases (thread_id, canvas_id) ON DELETE CASCADE" in awareness
    assert "UNIQUE (sender_id)" in awareness
    assert "CHECK (canvas_id = 'main')" in awareness
    assert "CHECK (client_seq > 0)" in awareness
    assert "state IN ('editing', 'idle')" in awareness
    assert "state = 'idle' AND expires_at = refreshed_at" in awareness
    assert "idx_canvas_editor_awareness_expires_at" in awareness
    assert "UX state only, never authorization or execution lease" in awareness


def test_interrupt_inbox_is_exact_lease_turn_and_durably_receipted() -> None:
    inbox = _compact(APP_THREAD_INTERRUPT_INBOX.read_text())
    index_sql = _compact(APP_THREAD_INTERRUPT_RECEIPT_INDEX.read_text())
    validation_sql = _compact(APP_THREAD_INTERRUPT_VALIDATION.read_text())

    assert "ADD COLUMN interrupt_admission_lease_token BIGINT" in inbox
    assert "ADD COLUMN interrupt_admission_turn_id INTEGER" in inbox
    assert "CONSTRAINT run_queue_interrupt_admission_shape" in inbox
    assert "interrupt_admission_lease_token = lease_token" in inbox
    assert "unit_kind = 'session_turn'" in inbox
    assert "state = 'leased'" in inbox
    assert "CREATE TABLE thread_interrupt_requests" in inbox
    assert "UNIQUE (thread_id, client_request_id)" in inbox
    assert "UNIQUE (id, thread_id)" in inbox
    assert "UNIQUE (thread_id, accepted_lease_token, target_turn_id)" not in inbox
    assert "accepted_lease_token BIGINT NOT NULL" in inbox
    assert "target_turn_id INTEGER NOT NULL" in inbox
    assert "outcome IN ('applied', 'rejected')" in inbox
    assert "applied_mode IN ('hard', 'graceful')" in inbox
    assert "applied_lease_token = accepted_lease_token" in inbox
    assert "FOREIGN KEY (interrupt_request_id, thread_id)" in inbox
    assert "REFERENCES thread_interrupt_requests(id, thread_id) NOT VALID" in inbox
    assert "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS" in index_sql
    assert "idx_thread_events_interrupt_request" in index_sql
    assert "VALIDATE CONSTRAINT run_queue_interrupt_admission_shape" in validation_sql
    assert (
        "VALIDATE CONSTRAINT thread_events_interrupt_request_thread_fkey"
        in validation_sql
    )


def test_compute_foundation_is_immutable_and_lock_fix_is_superseding() -> None:
    foundation_bytes = APP_COMPUTE_FOUNDATION_MIGRATION.read_bytes()
    assert hashlib.sha256(foundation_bytes).hexdigest() == (
        "f08a7adbad52368001fbf3a7a3b332b2d4c6f0ef36d1249b005d04cec7147893"
    )
    lock_fix_bytes = APP_AGENT_METERING_LOCK_ORDER_MIGRATION.read_bytes()
    assert hashlib.sha256(lock_fix_bytes).hexdigest() == (
        "b18ca34108ae46b7da5cf07a9a9a08a78fba96cebb02f4204165826cb67b6d12"
    )

    foundation = APP_COMPUTE_FOUNDATION_MIGRATION.read_text()
    lock_fix = APP_AGENT_METERING_LOCK_ORDER_MIGRATION.read_text()
    compact_fix = _compact(lock_fix)

    assert "depends-on:    0103_compute_metering_foundations.sql" in lock_fix
    assert "FOR NO KEY UPDATE OF agent SKIP LOCKED" in foundation
    assert "FOR NO KEY UPDATE OF agent SKIP LOCKED" not in lock_fix
    assert "LOCK TABLE agents, jobs, threads IN SHARE ROW EXCLUSIVE MODE" in compact_fix
    for function_name in (
        "converge_agent_metering_from_agent_row",
        "converge_agent_metering_from_job_row",
        "converge_agent_metering_from_thread_row",
    ):
        assert f"CREATE OR REPLACE FUNCTION public.{function_name}()" in lock_fix
    assert "CREATE TABLE" not in lock_fix
    assert "CREATE TRIGGER" not in lock_fix


def test_compute_epoch_guards_are_immutable_and_0109_is_the_lifecycle_predecessor() -> (
    None
):
    immutable_hashes = {
        APP_COMPUTE_SCOPE_EPOCH_GUARD_MIGRATION: (
            "9f15214d2b3b695e33ee168869c9b47e4fcf1992e9d75984b4e4f356d3076540"
        ),
        APP_COMPUTE_SCOPE_AUTHORIZATION_MIGRATION: (
            "eb13dd19aeecc2aafc921a98294fdc4e5759c12107d2f3ec6249ac7aa43e38ef"
        ),
        APP_COMPUTE_EXACT_EPOCH_AUTHORITY_MIGRATION: (
            "9b95a52ba8f319988230ef9c8541d8303dc52a7b02801a21f180ff121ba04757"
        ),
    }
    for path, expected in immutable_hashes.items():
        assert hashlib.sha256(path.read_bytes()).hexdigest() == expected

    lifecycle = APP_COMPUTE_EXACT_EPOCH_LIFECYCLE_MIGRATION.read_text()
    compact = _compact(lifecycle)
    assert "depends-on:    0108_compute_exact_epoch_authority.sql" in lifecycle
    assert (
        "LOCK TABLE resource_inventory_scope_epochs, compute_metering_activation"
        in compact
    )
    for superseded in (
        "resource_intervals_compute_activation_guard",
        "resource_intervals_compute_scope_epoch_guard",
        "resource_intervals_compute_exact_epoch_guard",
    ):
        assert f"DROP TRIGGER {superseded}" in lifecycle
    assert (
        "CREATE TRIGGER resource_intervals_compute_exact_epoch_lifecycle_guard"
        in lifecycle
    )
    assert "BEFORE INSERT OR UPDATE ON resource_intervals" in compact
    assert lifecycle.index("FOR SHARE OF epoch") < lifecycle.index(
        "FROM public.compute_metering_activation"
    )
    assert "NEW.last_confirmed_at > epoch_retired_at" in lifecycle
    assert "NEW.materialized_through > epoch_retired_at" in lifecycle


def test_compute_epoch_rollover_is_audited_append_only_and_directly_bound() -> None:
    sql = APP_COMPUTE_EPOCH_ROLLOVER_MIGRATION.read_text()
    compact = _compact(sql)

    assert "0109_compute_exact_epoch_lifecycle.sql" in sql
    assert "0111_thread_messages_live_index.notx.sql" in sql
    assert hashlib.sha256(
        APP_COMPUTE_EPOCH_ROLLOVER_MIGRATION.read_bytes()
    ).hexdigest() == (
        "3112549f0613c96a8012cea6a1f06f4b6249ec43c7b1c4a70161ba27b18af37e"
    )
    assert "cannot safely backfill append-only compute epoch authority" in sql
    assert "CREATE TABLE compute_metering_epoch_promotion_requests" in sql
    assert "CREATE TABLE compute_metering_epoch_authorities" in sql
    assert "request_kind IN ('initial-activation', 'recovery-rollover')" in compact
    assert "compute_epoch_promotion_requests_immutable" in sql
    assert "compute_metering_epoch_authorities_immutable" in sql
    assert "compute epoch promotion requests are immutable" in sql
    assert "compute epoch authorities are append-only" in sql
    assert "WITH RECURSIVE lineage AS" in sql
    assert "lineage_reaches_prior" in sql
    assert "missing_shadow_count" in sql
    assert "orphan_shadow_count" in sql
    assert "current_generation IS DISTINCT FROM NEW.proof_generation" in sql
    assert "ADD COLUMN compute_scope_epoch_id UUID" in sql
    assert "resource_intervals_compute_scope_epoch_shape_check" in sql
    assert "resource_intervals_compute_epoch_authority_guard" in sql
    assert (
        "NEW.compute_scope_epoch_id IS DISTINCT FROM OLD.compute_scope_epoch_id" in sql
    )
    assert "resource_inventory_epochs_compute_retirement" in sql
    assert "end_reason = 'inventory-epoch-retired'" in sql
    assert "current_interval_id = NULL" in sql
    assert "resource_inventory_epochs_recovery_identity_immutable" in sql
    assert "compute activation requires audited exact epoch authority" in sql
    assert (
        "DROP TRIGGER resource_intervals_compute_exact_epoch_lifecycle_guard" in compact
    )


def test_initial_compute_authority_accepts_only_proven_recovery_coverage() -> None:
    raw = APP_COMPUTE_INITIAL_RECOVERY_AUTHORITY.read_text()
    sql = _compact(raw)

    assert "depends-on:    0188_pre_registration_delete_sandbox_zero.sql" in raw
    assert (
        "CREATE OR REPLACE FUNCTION public."
        "protect_compute_metering_epoch_authority()" in sql
    )
    assert "epoch_continuity_health IS DISTINCT FROM 'healthy'" in sql
    assert "epoch_reliable_from IS NULL" in sql
    assert "epoch_reliable_from > NEW.effective_from" in sql
    assert "epoch_continuous_since IS NULL" in sql
    assert "epoch_continuous_since > NEW.effective_from" in sql
    assert "FROM public.resource_inventory_coverage_gaps AS gap" in sql
    assert "gap.resolution = 'unresolved'" in sql
    assert "epoch_recovery_from IS NOT NULL" in sql
    assert "DROP TRIGGER" not in raw
    assert "CREATE TABLE" not in raw


def test_compute_authority_confirmation_gap_supersedes_deployed_rollover() -> None:
    sql = APP_COMPUTE_AUTHORITY_CONFIRMATION_GAP_MIGRATION.read_text()
    compact = _compact(sql)

    assert "depends-on:    0112_compute_epoch_rollover_authority.sql" in sql
    assert hashlib.sha256(
        APP_COMPUTE_AUTHORITY_CONFIRMATION_GAP_MIGRATION.read_bytes()
    ).hexdigest() == (
        "c8b530ada1583b17d81507f4571c6e2b51e82afe17911c4d22db4323ec448466"
    )
    assert "CREATE FUNCTION record_compute_authority_confirmation_gap()" in sql
    assert "CREATE TRIGGER compute_epoch_authority_confirmation_gap" in sql
    assert "AFTER INSERT ON compute_metering_epoch_authorities" in compact
    assert "compute-authority-awaiting-confirmation:" in sql
    assert "authority.authority_sequence = 1" in sql
    assert "THEN authority.effective_from ELSE predecessor.retired_at END" in compact
    assert "NEW.authority_sequence = 1" in sql
    assert "authority_gap_start := NEW.effective_from" in sql
    assert "backfilled_by_migration', TRUE" in sql
    assert "predecessor.retired_at IS NULL" in sql
    assert "0111_thread_messages_live_index.notx.sql" not in sql


def test_compute_interval_epoch_shape_repair_closes_sql_null_bypass() -> None:
    sql = APP_COMPUTE_INTERVAL_EPOCH_SHAPE_REPAIR_MIGRATION.read_text()

    assert "depends-on:    0113_compute_authority_confirmation_gap.sql" in sql
    assert hashlib.sha256(
        APP_COMPUTE_INTERVAL_EPOCH_SHAPE_REPAIR_MIGRATION.read_bytes()
    ).hexdigest() == (
        "5bbcb4e288e31ed742fde7d6bb5d4a108a0142297ad679a80ac368fc5b5418b1"
    )
    assert "DROP CONSTRAINT resource_intervals_compute_scope_epoch_shape_check" in sql
    assert "ADD CONSTRAINT resource_intervals_compute_scope_epoch_shape_check" in sql
    assert "COALESCE(details->>'product_class', '') = 'ide-session'" in sql
    assert "NULL product classes cannot bypass the shape" in sql


@pytest.mark.asyncio
async def test_compute_shape_repair_upgrades_deployed_0112_and_0113_checksums(
    app_pg_dsn: str,
    tmp_path: Path,
) -> None:
    dbname = f"metering_0114_upgrade_{uuid4().hex[:12]}"
    admin = await asyncpg.connect(app_pg_dsn)
    try:
        await admin.execute(f'CREATE DATABASE "{dbname}"')
    finally:
        await admin.close()

    dsn = _swap_db(app_pg_dsn, dbname)
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
    staged_dir = tmp_path / "through-0113"
    repaired_dir = tmp_path / "through-0114"
    staged_dir.mkdir()
    repaired_dir.mkdir()
    try:
        for path in discover(
            ROOT / "src" / "orchestrator" / "database" / "migrations" / "app"
        ):
            if path.name <= APP_COMPUTE_AUTHORITY_CONFIRMATION_GAP_MIGRATION.name:
                (staged_dir / path.name).write_bytes(path.read_bytes())
            if path.name <= APP_COMPUTE_INTERVAL_EPOCH_SHAPE_REPAIR_MIGRATION.name:
                (repaired_dir / path.name).write_bytes(path.read_bytes())

        await run_migrations(pool, staged_dir)
        async with pool.acquire() as conn:
            assert (
                await conn.fetchval(
                    "SELECT checksum FROM schema_migrations WHERE filename=$1",
                    APP_COMPUTE_EPOCH_ROLLOVER_MIGRATION.name,
                )
                == "3112549f0613c96a8012cea6a1f06f4b6249ec43c7b1c4a70161ba27b18af37e"
            )
            assert (
                await conn.fetchval(
                    "SELECT checksum FROM schema_migrations WHERE filename=$1",
                    APP_COMPUTE_AUTHORITY_CONFIRMATION_GAP_MIGRATION.name,
                )
                == "c8b530ada1583b17d81507f4571c6e2b51e82afe17911c4d22db4323ec448466"
            )
            assert not await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE filename=$1)",
                APP_COMPUTE_INTERVAL_EPOCH_SHAPE_REPAIR_MIGRATION.name,
            )

        await run_migrations(pool, repaired_dir)
        await run_migrations(pool, repaired_dir)
        async with pool.acquire() as conn:
            assert (
                await conn.fetchval(
                    "SELECT checksum FROM schema_migrations WHERE filename=$1",
                    APP_COMPUTE_EPOCH_ROLLOVER_MIGRATION.name,
                )
                == "3112549f0613c96a8012cea6a1f06f4b6249ec43c7b1c4a70161ba27b18af37e"
            )
            assert await conn.fetchval(
                "SELECT success FROM schema_migrations WHERE filename=$1",
                APP_COMPUTE_INTERVAL_EPOCH_SHAPE_REPAIR_MIGRATION.name,
            )
            assert await conn.fetchval(
                "SELECT EXISTS ("
                "SELECT 1 FROM pg_trigger "
                "WHERE tgname='compute_epoch_authority_confirmation_gap' "
                "AND NOT tgisinternal)"
            )
            constraint_definition = await conn.fetchval(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                "WHERE conname='resource_intervals_compute_scope_epoch_shape_check'"
            )
            assert "COALESCE" in constraint_definition
    finally:
        await pool.close()
        admin = await asyncpg.connect(app_pg_dsn)
        try:
            await admin.execute(f'DROP DATABASE "{dbname}" WITH (FORCE)')
        finally:
            await admin.close()


def test_referenced_rate_range_guard_is_bounded_and_conflict_aware() -> None:
    sql = _compact(APP_REFERENCED_RATE_GUARD_MIGRATION.read_text())

    assert (
        "depends-on: 0100_infrastructure_terminal_evidence_single_boundary.sql" in sql
    )
    assert "LOCK TABLE usage_rates_v2 IN SHARE ROW EXCLUSIVE MODE" in sql
    assert (
        "LOCK TABLE resource_publication_plan_events IN SHARE ROW EXCLUSIVE MODE" in sql
    )
    assert "resource_publication_plan_events_rate_reference_idx" in sql
    assert "plan.state IN ('planned', 'published', 'conflict')" in sql
    assert "plan.period_end > NEW.effective_to" in sql
    assert sql.count("CREATE TRIGGER usage_rates_v2_referenced_range_guard") == 1
    assert "BEFORE UPDATE OF effective_to ON usage_rates_v2" in sql
    assert "ERRCODE = '55000'" in sql


@pytest.mark.asyncio
async def test_referenced_rate_guard_allows_boundary_close_and_blocks_retained_plans(
    app_pg_dsn: str,
) -> None:
    dbname = f"metering_rate_guard_{uuid4().hex[:12]}"
    admin = await asyncpg.connect(app_pg_dsn)
    try:
        await admin.execute(f'CREATE DATABASE "{dbname}"')
    finally:
        await admin.close()

    conn = await asyncpg.connect(_swap_db(app_pg_dsn, dbname))
    try:
        await conn.execute('CREATE EXTENSION "uuid-ossp"')
        await conn.execute("CREATE TABLE usage_rate_cards (id TEXT PRIMARY KEY)")
        await conn.execute(
            "CREATE TABLE rollup_state (name TEXT PRIMARY KEY, last_closed_day DATE)"
        )
        await conn.execute(APP_MIGRATION.read_text())

        scope_id = uuid4()
        await conn.execute(
            "INSERT INTO resource_inventory_scopes "
            "(id, collector_id, source_cluster, api_resource, namespace) "
            "VALUES ($1, 'test', 'cluster-a', 'pods', 'workers')",
            scope_id,
        )
        period_end = datetime(2026, 8, 6, 12, tzinfo=timezone.utc)
        period_start = period_end - timedelta(hours=1)
        interval_id = await _insert_open_test_interval(
            conn,
            inventory_scope_id=scope_id,
            source_cluster="cluster-a",
            namespace="workers",
            source_uid="rate-guard-pod",
            source_revision="a" * 64,
            observed_at=period_end,
        )

        async def insert_rate(resource: str) -> object:
            rate_id = uuid4()
            await conn.execute(
                "INSERT INTO usage_rates_v2 ("
                "id, cost_domain, measurement_basis, category, resource_class, "
                "resource, unit, effective_from, usd_per_unit, source, "
                "source_version) VALUES ("
                "$1, 'workload-allocation', 'scheduler-request', 'compute', "
                "'kubernetes-pod', $2, 'vcpu-hour', $3, 0.25, "
                "'test', 'v1')",
                rate_id,
                resource,
                period_start - timedelta(days=1),
            )
            return rate_id

        ordinary_rate = await insert_rate("ordinary-resource")
        correction_rate = await insert_rate("correction-resource")

        async def insert_plan(kind: str, rate_id: object, source_id: str) -> object:
            plan_id = uuid4()
            correction = kind == "correction"
            async with conn.transaction():
                await conn.execute(
                    "INSERT INTO resource_publication_plans ("
                    "id, source_interval_id, source_revision, plan_kind, "
                    "plan_revision, advances_cursor, previous_materialized_through, "
                    "correction_group_id, period_start, period_end, "
                    "expected_event_count, payload_schema_version, event_set_hash, "
                    "rate_selection_hash, creator_generation) VALUES ("
                    "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1, 1, $11, $12, 1)",
                    plan_id,
                    interval_id,
                    "a" * 64,
                    kind,
                    1 if correction else 0,
                    not correction,
                    None if correction else period_start,
                    plan_id if correction else None,
                    period_start,
                    period_end,
                    "b" * 64,
                    "c" * 64,
                )
                await conn.execute(
                    "INSERT INTO resource_publication_plan_events ("
                    "plan_id, ordinal, source, source_id, unit, ts, event_kind, "
                    "canonical_rate_version_id, row_hash, event_payload) VALUES ("
                    "$1, 0, $2, $3, 'vcpu-hour', $4, $5, $6, $7, $8::jsonb)",
                    plan_id,
                    (
                        "infra-allocation-correction-v2"
                        if correction
                        else "infra-allocation-v2"
                    ),
                    source_id,
                    period_start,
                    kind,
                    rate_id,
                    "d" * 64 if correction else "e" * 64,
                    json.dumps(
                        (
                            {
                                "quantity": "-4",
                                "corrects_source": "infra-allocation-v2",
                                "corrects_source_id": "ordinary-event",
                                "corrects_unit": "vcpu-hour",
                                "corrects_ts": period_start.isoformat(
                                    timespec="microseconds"
                                ).replace("+00:00", "Z"),
                            }
                            if correction
                            else {"quantity": "10"}
                        )
                    ),
                )
            return plan_id

        ordinary_plan = await insert_plan("usage", ordinary_rate, "ordinary-event")
        correction_plan = await insert_plan(
            "correction", correction_rate, "correction-event"
        )
        await conn.execute(
            "UPDATE resource_publication_plans SET state='published', "
            "attempt_count=1, last_attempt_at=statement_timestamp(), "
            "published_at=statement_timestamp() WHERE id=$1",
            ordinary_plan,
        )

        await conn.execute(APP_REFERENCED_RATE_GUARD_MIGRATION.read_text())

        def negative_candidate(quantity: str) -> SimpleNamespace:
            payload = {
                "quantity": quantity,
                "corrects_source": "infra-allocation-v2",
                "corrects_source_id": "ordinary-event",
                "corrects_unit": "vcpu-hour",
                "corrects_ts": period_start.isoformat(timespec="microseconds").replace(
                    "+00:00", "Z"
                ),
                "details": {
                    "corrects_quantity": "10",
                    "corrects_payload_hash": "e" * 64,
                },
            }
            return SimpleNamespace(
                id=uuid4(),
                source_interval_id=interval_id,
                events=(SimpleNamespace(event=SimpleNamespace(payload=payload)),),
            )

        await InfrastructureUsageMaterializer._validate_correction_negative_bounds(
            conn, negative_candidate("-3")
        )
        with pytest.raises(PublicationConflictError, match="exceed original quantity"):
            await InfrastructureUsageMaterializer._validate_correction_negative_bounds(
                conn, negative_candidate("-7")
            )

        unsafe_close = period_end - timedelta(microseconds=1)
        for rate_id in (ordinary_rate, correction_rate):
            with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
                await conn.execute(
                    "UPDATE usage_rates_v2 SET effective_to=$1 WHERE id=$2",
                    unsafe_close,
                    rate_id,
                )

        await conn.execute(
            "UPDATE resource_publication_plans SET state='conflict', "
            "attempt_count=1, last_attempt_at=statement_timestamp(), "
            "sanitized_error='{}'::jsonb WHERE id=$1",
            correction_plan,
        )
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await conn.execute(
                "UPDATE usage_rates_v2 SET effective_to=$1 WHERE id=$2",
                unsafe_close,
                correction_rate,
            )

        for rate_id in (ordinary_rate, correction_rate):
            await conn.execute(
                "UPDATE usage_rates_v2 SET effective_to=$1 WHERE id=$2",
                period_end,
                rate_id,
            )
        closed = await conn.fetchval(
            "SELECT count(*) FROM usage_rates_v2 WHERE effective_to=$1",
            period_end,
        )
        assert closed == 2
    finally:
        await conn.close()
        admin = await asyncpg.connect(app_pg_dsn)
        try:
            await admin.execute(f'DROP DATABASE IF EXISTS "{dbname}" WITH (FORCE)')
        finally:
            await admin.close()


@pytest.mark.asyncio
async def test_app_ingestion_migration_applies_after_slice0_on_postgres16(
    app_pg_dsn: str,
) -> None:
    dbname = f"metering_ingest_{uuid4().hex[:12]}"
    admin = await asyncpg.connect(app_pg_dsn)
    try:
        await admin.execute(f'CREATE DATABASE "{dbname}"')
    finally:
        await admin.close()

    conn = await asyncpg.connect(_swap_db(app_pg_dsn, dbname))
    try:
        await conn.execute('CREATE EXTENSION "uuid-ossp"')
        await conn.execute("CREATE TABLE usage_rate_cards (id TEXT PRIMARY KEY)")
        await conn.execute(
            "CREATE TABLE rollup_state (name TEXT PRIMARY KEY, last_closed_day DATE)"
        )
        await conn.execute(APP_MIGRATION.read_text())
        await conn.execute(APP_INGESTION_MIGRATION.read_text())
        await conn.execute(APP_INGESTION_SIZE_FIX.read_text())
        await conn.execute(APP_PLAN_PERIOD_INDEX.read_text())
        await conn.execute(APP_INTERVAL_OVERLAP_INDEX.read_text())
        await conn.execute(APP_COMPLETE_SNAPSHOT_RECEIVED_INDEX.read_text())
        await conn.execute(APP_INVALID_WATCH_RECEIVED_INDEX.read_text())

        await conn.execute(
            "CREATE TEMP TABLE inventory_size_probe ("
            "source_kind TEXT, source_uid TEXT, revision_hash TEXT, "
            "normalized_item JSONB, item_error JSONB)"
        )
        returning_bytes = await conn.fetchval(
            "WITH inserted AS ("
            "INSERT INTO inventory_size_probe SELECT "
            "'pod', 'toast-probe', repeat('0', 64), "
            "jsonb_build_object('payload', repeat('compressible-value-', 2000)), "
            "NULL RETURNING *) "
            "SELECT resource_inventory_snapshot_item_size_bytes("
            "source_kind, source_uid, revision_hash, normalized_item, item_error) "
            "FROM inserted"
        )
        stored_bytes = await conn.fetchval(
            "SELECT resource_inventory_snapshot_item_size_bytes("
            "source_kind, source_uid, revision_hash, normalized_item, item_error) "
            "FROM inventory_size_probe"
        )
        assert returning_bytes == stored_bytes
        assert returning_bytes > 30_000

        tables = await conn.fetch(
            "SELECT tablename FROM pg_tables WHERE schemaname='public' "
            "AND tablename LIKE 'resource_inventory_%'"
        )
        names = {row["tablename"] for row in tables}
        assert "resource_inventory_transport_nonces" in names
        assert "resource_inventory_watch_sessions" in names
        assert "resource_inventory_watch_events" in names
        indexes = {
            row["indexname"]
            for row in await conn.fetch(
                "SELECT indexname FROM pg_indexes WHERE schemaname='public'"
            )
        }
        assert "resource_publication_plans_period_idx" in indexes
        assert "resource_intervals_overlap_idx" in indexes
        assert "resource_inventory_snapshots_complete_received_idx" in indexes
        assert "resource_inventory_watch_events_invalid_received_idx" in indexes

        await conn.execute(
            "UPDATE infra_metering_control SET leader_generation=1, "
            "updated_at=statement_timestamp() WHERE singleton"
        )
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await conn.execute(
                "UPDATE infra_metering_control SET leader_generation=0 WHERE singleton"
            )
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_snapshot_ticket_turns_abandoned_watch_into_recovery_gap(
    app_pg_dsn: str,
) -> None:
    dbname = f"metering_abandoned_{uuid4().hex[:12]}"
    admin = await asyncpg.connect(app_pg_dsn)
    try:
        await admin.execute(f'CREATE DATABASE "{dbname}"')
    finally:
        await admin.close()
    dsn = _swap_db(app_pg_dsn, dbname)
    setup = await asyncpg.connect(dsn)
    pool = None
    try:
        await setup.execute('CREATE EXTENSION "uuid-ossp"')
        await setup.execute("CREATE TABLE usage_rate_cards (id TEXT PRIMARY KEY)")
        await setup.execute(
            "CREATE TABLE rollup_state (name TEXT PRIMARY KEY, last_closed_day DATE)"
        )
        await setup.execute(APP_MIGRATION.read_text())
        await setup.execute(APP_INGESTION_MIGRATION.read_text())
        await setup.execute(APP_INGESTION_SIZE_FIX.read_text())

        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
        store = InventoryStore(pool)
        generation = await store.activate_generation()
        scope_id, epoch_id = uuid4(), uuid4()
        scope = InventoryScopeIdentity(
            collector_id="kubernetes",
            source_cluster="cluster-a",
            api_resource="core/v1/pods",
            namespace="workers",
        )
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO resource_inventory_scopes "
                "(id, collector_id, source_cluster, api_resource, namespace) "
                "VALUES ($1, $2, $3, $4, $5)",
                scope_id,
                scope.collector_id,
                scope.source_cluster,
                scope.api_resource,
                scope.namespace,
            )
            await conn.execute(
                "INSERT INTO resource_inventory_scope_epochs "
                "(id, scope_id, epoch_number, coverage_mode) "
                "VALUES ($1, $2, 1, 'list-watch')",
                epoch_id,
                scope_id,
            )

        baseline_ticket = await store.issue_ingest_ticket(
            epoch_id,
            "1" * 64,
            scope=scope,
            transport=_transport("snapshot-ticket"),
            require_healthy_continuity=True,
        )
        baseline_snapshot = uuid4()
        await store.begin_snapshot(
            baseline_ticket.token,
            baseline_ticket.id,
            baseline_snapshot,
            datetime.now(timezone.utc),
            scope=scope,
            transport=_transport("snapshot-begin"),
        )
        await store.stage_items(
            baseline_ticket.token,
            baseline_ticket.id,
            baseline_snapshot,
            (),
            scope=scope,
            transport=_transport("snapshot-items"),
        )
        await store.finalize_snapshot(
            baseline_ticket.token,
            baseline_ticket.id,
            baseline_snapshot,
            SnapshotFinalization(
                collection_completed_at=datetime.now(timezone.utc),
                complete=True,
                item_count=0,
                item_digest=inventory_manifest_digest(()),
                resource_version="rv-list",
            ),
            scope=scope,
            transport=_transport("snapshot-finalize"),
            reconcile_intervals=False,
        )
        session = await store.issue_watch_session(
            epoch_id,
            "2" * 64,
            "rv-list",
            scope=scope,
            transport=_transport("watch-session"),
            max_events=10,
            max_bytes=100,
        )
        await store.apply_watch_event(
            session.token,
            session.id,
            uuid4(),
            "3" * 64,
            "rv-list",
            WatchObjectEvent(
                WatchEventKind.BOOKMARK,
                "rv-watch",
                datetime.now(timezone.utc),
                1,
            ),
            scope=scope,
            transport=_transport("watch-event"),
        )

        abandoned_claim = _transport("snapshot-ticket")
        with pytest.raises(InventoryRecoveryRequired, match="broken WATCH"):
            await store.issue_ingest_ticket(
                epoch_id,
                "4" * 64,
                scope=scope,
                transport=abandoned_claim,
            )

        async with pool.acquire() as conn:
            gap = await conn.fetchrow(
                "SELECT id, gap_start, gap_end, reason, resolution_details "
                "FROM resource_inventory_coverage_gaps "
                "WHERE scope_epoch_id=$1",
                epoch_id,
            )
            epoch = await conn.fetchrow(
                "SELECT continuity_health, backend_health, last_resource_version, "
                "sanitized_error FROM resource_inventory_scope_epochs WHERE id=$1",
                epoch_id,
            )
            assert gap["reason"] == "watch-session-abandoned"
            assert gap["gap_end"] is None
            gap_details = json.loads(gap["resolution_details"])
            epoch_error = json.loads(epoch["sanitized_error"])
            assert gap_details["watch_session_id"] == str(session.id)
            assert gap_details["server_committed_resource_version"] == ("rv-watch")
            assert epoch["continuity_health"] == "gap"
            assert epoch["backend_health"] == "degraded"
            assert epoch["last_resource_version"] == "rv-watch"
            assert epoch_error["coverage_gap_id"] == str(gap["id"])
            assert await conn.fetchval(
                "SELECT consumed_at IS NULL FROM resource_inventory_watch_sessions "
                "WHERE id=$1",
                session.id,
            )
            assert not await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM resource_inventory_ingest_tickets "
                "WHERE request_digest=$1)",
                "4" * 64,
            )
            assert await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM resource_inventory_transport_nonces "
                "WHERE collector_id=$1 AND request_nonce=$2)",
                abandoned_claim.collector_id,
                abandoned_claim.request_nonce,
            )

        # This reason is generated only by snapshot admission; a collector may
        # not forge it through the public WATCH-finish contract.
        with pytest.raises(InventoryContractError, match="gap reason"):
            await store.record_watch_gap(
                session.token,
                session.id,
                uuid4(),
                "5" * 64,
                "rv-watch",
                gap_reason="watch-session-abandoned",
                scope=scope,
                transport=_transport("watch-history-lost"),
            )

        # HTTP snapshot admission rechecks continuity while holding the epoch
        # lock, closing the race with a WATCH request that records a gap after
        # the service's initial epoch read. Direct diagnostic callers retain
        # the default opt-out used by the retention fixtures below.
        gap_epoch_claim = _transport("snapshot-ticket")
        with pytest.raises(InventoryRecoveryRequired, match="healthy continuity"):
            await store.issue_ingest_ticket(
                epoch_id,
                "5" * 64,
                scope=scope,
                transport=gap_epoch_claim,
                require_healthy_continuity=True,
            )
        async with pool.acquire() as conn:
            assert not await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM resource_inventory_ingest_tickets "
                "WHERE request_digest=$1)",
                "5" * 64,
            )
            assert not await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM resource_inventory_transport_nonces "
                "WHERE collector_id=$1 AND request_nonce=$2)",
                gap_epoch_claim.collector_id,
                gap_epoch_claim.request_nonce,
            )

        recovery = await store.start_watch_recovery_epoch(
            epoch_id,
            scope=scope,
            transport=_transport("scope-recovery"),
        )
        recovery_ticket = await store.issue_ingest_ticket(
            recovery.scope_epoch_id,
            "6" * 64,
            scope=scope,
            transport=_transport("snapshot-ticket"),
        )
        recovery_snapshot = uuid4()
        await store.begin_snapshot(
            recovery_ticket.token,
            recovery_ticket.id,
            recovery_snapshot,
            datetime.now(timezone.utc),
            scope=scope,
            transport=_transport("snapshot-begin"),
        )
        await store.stage_items(
            recovery_ticket.token,
            recovery_ticket.id,
            recovery_snapshot,
            (),
            scope=scope,
            transport=_transport("snapshot-items"),
        )
        await store.finalize_snapshot(
            recovery_ticket.token,
            recovery_ticket.id,
            recovery_snapshot,
            SnapshotFinalization(
                collection_completed_at=datetime.now(timezone.utc),
                complete=True,
                item_count=0,
                item_digest=inventory_manifest_digest(()),
                resource_version="rv-recovered",
            ),
            scope=scope,
            transport=_transport("snapshot-finalize"),
            reconcile_intervals=False,
        )
        async with pool.acquire() as conn:
            assert await conn.fetchval(
                "SELECT gap_end IS NOT NULL FROM resource_inventory_coverage_gaps "
                "WHERE id=$1",
                gap["id"],
            )
        assert await store.deactivate_generation(generation)
    finally:
        if pool is not None:
            await pool.close()
        if not setup.is_closed():
            await setup.close()
        admin = await asyncpg.connect(app_pg_dsn)
        try:
            await admin.execute(f'DROP DATABASE IF EXISTS "{dbname}" WITH (FORCE)')
        finally:
            await admin.close()


@pytest.mark.asyncio
async def test_day_sealer_executes_against_postgres16(app_pg_dsn: str) -> None:
    dbname = f"metering_sealer_{uuid4().hex[:12]}"
    admin = await asyncpg.connect(app_pg_dsn)
    try:
        await admin.execute(f'CREATE DATABASE "{dbname}"')
    finally:
        await admin.close()

    dsn = _swap_db(app_pg_dsn, dbname)
    setup = await asyncpg.connect(dsn)
    pool = None
    try:
        await setup.execute('CREATE EXTENSION "uuid-ossp"')
        await setup.execute("CREATE TABLE usage_rate_cards (id TEXT PRIMARY KEY)")
        await setup.execute(
            "CREATE TABLE rollup_state (name TEXT PRIMARY KEY, last_closed_day DATE)"
        )
        await setup.execute(APP_MIGRATION.read_text())
        await setup.execute(APP_INGESTION_MIGRATION.read_text())
        await setup.execute(APP_INGESTION_SIZE_FIX.read_text())
        await setup.execute(APP_PLAN_PERIOD_INDEX.read_text())
        await setup.execute(APP_INTERVAL_OVERLAP_INDEX.read_text())
        await setup.execute(APP_COMPLETE_SNAPSHOT_RECEIVED_INDEX.read_text())
        await setup.execute(APP_INVALID_WATCH_RECEIVED_INDEX.read_text())

        day = datetime.now(timezone.utc).date() - timedelta(days=2)
        day_start = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
        day_end = day_start + timedelta(days=1)
        scope_id = uuid4()
        await setup.execute(
            "UPDATE infra_metering_control SET leader_generation=1, "
            "cutover_state='active', cutover_at=$1, "
            "updated_at=statement_timestamp() WHERE singleton",
            day_start,
        )
        await setup.execute(
            "INSERT INTO resource_inventory_scopes "
            "(id, collector_id, source_cluster, api_resource, namespace) "
            "VALUES ($1, 'kubernetes', 'test-cluster', 'core/v1/pods', 'workers')",
            scope_id,
        )
        epoch_id = await setup.fetchval(
            "INSERT INTO resource_inventory_scope_epochs ("
            "scope_id, epoch_number, reliable_from, required_for_rollup, "
            "required_from, coverage_mode, leader_generation, continuous_since, "
            "complete_through, snapshot_health, continuity_health, item_health, "
            "backend_health, publication_health) VALUES ("
            "$1, 1, $2, TRUE, $2, 'list-watch', 1, $2, $3, "
            "'healthy', 'healthy', 'healthy', 'healthy', 'initializing') "
            "RETURNING id",
            scope_id,
            day_start,
            day_end,
        )
        # This fixture exercises the sealer's SQL evidence query rather than
        # the already-covered ticket/finalization path. Bypass only the ingest
        # fence while installing an otherwise valid immutable boundary proof.
        await setup.execute(
            "ALTER TABLE resource_inventory_snapshots DISABLE TRIGGER "
            "resource_inventory_snapshots_generation_fence"
        )
        snapshot_id = await setup.fetchval(
            "INSERT INTO resource_inventory_snapshots ("
            "scope_epoch_id, inventory_scope_id, collection_started_at, "
            "collection_completed_at, received_at, complete, leader_generation, "
            "item_count) VALUES ($1, $2, $3, $3, $3, FALSE, 1, 0) "
            "RETURNING id",
            epoch_id,
            scope_id,
            day_end - timedelta(seconds=1),
        )
        await setup.execute(
            "UPDATE resource_inventory_snapshots SET "
            "collection_completed_at=$2, received_at=$2, complete=TRUE, "
            "item_digest=$3, manifest_state='sealed', sealed_at=$2 "
            "WHERE id=$1",
            snapshot_id,
            day_end,
            "0" * 64,
        )
        await setup.execute(
            "ALTER TABLE resource_inventory_snapshots ENABLE TRIGGER "
            "resource_inventory_snapshots_generation_fence"
        )

        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
        sealer = InfrastructureUsageDaySealer(pool, sealing_enabled=True)
        first = await sealer.seal_day(day, 1)
        replay = await sealer.seal_day(day, 1)

        persisted = await setup.fetchrow(
            "SELECT state, coverage_status, coverage_revision, unknown_ranges "
            "FROM infra_usage_day_state WHERE day=$1",
            day,
        )
        assert first.disposition is DaySealDisposition.SEALED
        assert replay.disposition is DaySealDisposition.ALREADY_SEALED
        assert replay.coverage_revision == first.coverage_revision
        assert persisted is not None
        assert persisted["state"] == "sealed"
        assert persisted["coverage_status"] == "complete"
        assert persisted["coverage_revision"] == first.coverage_revision
        assert persisted["unknown_ranges"] == "[]"
    finally:
        if pool is not None:
            await pool.close()
        await setup.close()
        admin = await asyncpg.connect(app_pg_dsn)
        try:
            await admin.execute(f'DROP DATABASE IF EXISTS "{dbname}" WITH (FORCE)')
        finally:
            await admin.close()


@pytest.mark.asyncio
async def test_source_aware_read_sql_executes_against_postgres16(
    app_pg_dsn: str,
) -> None:
    dbname = f"metering_reader_{uuid4().hex[:12]}"
    admin = await asyncpg.connect(app_pg_dsn)
    try:
        await admin.execute(f'CREATE DATABASE "{dbname}"')
    finally:
        await admin.close()

    dsn = _swap_db(app_pg_dsn, dbname)
    setup = await asyncpg.connect(dsn)
    pool = None
    try:
        await setup.execute('CREATE EXTENSION "uuid-ossp"')
        await setup.execute("CREATE TABLE usage_rate_cards (id TEXT PRIMARY KEY)")
        await setup.execute(
            "CREATE TABLE rollup_state (name TEXT PRIMARY KEY, last_closed_day DATE)"
        )
        await setup.execute(APP_MIGRATION.read_text())
        await setup.execute(APP_INGESTION_MIGRATION.read_text())
        await setup.execute(APP_INGESTION_SIZE_FIX.read_text())
        await setup.execute(APP_PLAN_PERIOD_INDEX.read_text())
        await setup.execute(APP_INTERVAL_OVERLAP_INDEX.read_text())
        await setup.execute(APP_COMPLETE_SNAPSHOT_RECEIVED_INDEX.read_text())
        await setup.execute(APP_INVALID_WATCH_RECEIVED_INDEX.read_text())
        await setup.execute(
            "CREATE FUNCTION round_half_even_v2(value numeric, scale integer) "
            "RETURNS numeric LANGUAGE sql IMMUTABLE STRICT AS "
            "'SELECT round(value, scale)'"
        )
        await setup.execute(
            "CREATE TABLE usage_events ("
            "ts TIMESTAMPTZ NOT NULL, user_id UUID, project_id UUID, "
            "ref_kind TEXT, ref_id UUID, category TEXT NOT NULL, "
            "resource TEXT NOT NULL, quantity NUMERIC NOT NULL, unit TEXT NOT NULL, "
            "rate_usd NUMERIC, cost_usd NUMERIC, source TEXT NOT NULL, "
            "period_start TIMESTAMPTZ, period_end TIMESTAMPTZ, "
            "measurement_basis TEXT, cost_domain TEXT, resource_class TEXT, "
            "attribution_scope TEXT, measurement_algorithm TEXT, "
            "source_capacity_value BIGINT, source_capacity_unit TEXT, "
            "event_kind TEXT)"
        )

        from_ts = datetime.combine(
            datetime.now(timezone.utc).date() - timedelta(days=2),
            datetime.min.time(),
            tzinfo=timezone.utc,
        )
        to_ts = from_ts + timedelta(hours=1)
        scope_id = uuid4()
        user_id = uuid4()
        await setup.execute(
            "UPDATE infra_metering_control SET leader_generation=1, "
            "cutover_state='active', cutover_at=$1, "
            "updated_at=statement_timestamp() WHERE singleton",
            from_ts,
        )
        await setup.execute(
            "INSERT INTO resource_inventory_scopes "
            "(id, collector_id, source_cluster, api_resource, namespace) "
            "VALUES ($1, 'kubernetes', 'test-cluster', 'core/v1/pods', 'workers')",
            scope_id,
        )
        await setup.execute(
            "INSERT INTO resource_inventory_scope_epochs ("
            "scope_id, epoch_number, reliable_from, required_for_rollup, "
            "required_from, coverage_mode, leader_generation, continuous_since, "
            "complete_through, snapshot_health, continuity_health, item_health, "
            "backend_health, publication_health) VALUES ("
            "$1, 1, $2, TRUE, $2, 'list-watch', 1, $2, $3, "
            "'healthy', 'healthy', 'healthy', 'healthy', 'healthy')",
            scope_id,
            from_ts,
            to_ts,
        )
        await setup.execute(
            "INSERT INTO usage_events ("
            "ts, user_id, category, resource, quantity, unit, rate_usd, "
            "cost_usd, source, measurement_basis, cost_domain, resource_class, "
            "attribution_scope, measurement_algorithm) VALUES ("
            "$1, $2, 'llm', 'openrouter/test', 100, 'tokens', 0.000001, "
            "0.0001, 'openrouter', 'api-consumed', 'external-service', "
            "'llm-model', 'customer', 'provider-reported-v1')",
            from_ts + timedelta(minutes=30),
            user_id,
        )

        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
        summary = await SourceAwareUsageReadModel(pool, pool).summary(
            from_ts=from_ts,
            to_ts=to_ts,
            visibility=UsageVisibility(),
            as_of=to_ts,
        )

        assert summary.coverage.status == "complete"
        assert summary.window.data_through == to_ts
        assert len(summary.rows) == 1
        assert summary.rows[0].quantity == "100"
        assert summary.rows[0].finalized_quantity == "100"
        assert summary.rows[0].confirmed_provisional_quantity == "0"
        assert summary.rows[0].ledger_cost.amount == "0.0001"
    finally:
        if pool is not None:
            await pool.close()
        await setup.close()
        admin = await asyncpg.connect(app_pg_dsn)
        try:
            await admin.execute(f'DROP DATABASE IF EXISTS "{dbname}" WITH (FORCE)')
        finally:
            await admin.close()


@pytest.mark.asyncio
async def test_strict_materializer_freezes_and_finalizes_on_postgres16(
    app_pg_dsn: str,
) -> None:
    dbname = f"metering_publish_{uuid4().hex[:12]}"
    admin = await asyncpg.connect(app_pg_dsn)
    try:
        await admin.execute(f'CREATE DATABASE "{dbname}"')
    finally:
        await admin.close()
    dsn = _swap_db(app_pg_dsn, dbname)
    setup = await asyncpg.connect(dsn)
    pool = None
    try:
        await setup.execute('CREATE EXTENSION "uuid-ossp"')
        await setup.execute("CREATE TABLE usage_rate_cards (id TEXT PRIMARY KEY)")
        await setup.execute(
            "CREATE TABLE rollup_state (name TEXT PRIMARY KEY, last_closed_day DATE)"
        )
        await setup.execute(APP_MIGRATION.read_text())
        await setup.execute(APP_INGESTION_MIGRATION.read_text())
        await setup.execute(APP_INGESTION_SIZE_FIX.read_text())

        cutover = datetime(2026, 8, 5, 10, tzinfo=timezone.utc)
        scope_id, lifecycle_id, interval_id = uuid4(), uuid4(), uuid4()
        owner_id, user_id, project_id = uuid4(), uuid4(), uuid4()
        await setup.execute(
            "UPDATE infra_metering_control SET leader_generation=7, "
            "cutover_state='active', cutover_at=$1, "
            "updated_at=statement_timestamp() WHERE singleton",
            cutover,
        )
        await setup.execute(
            "INSERT INTO resource_inventory_scopes "
            "(id, collector_id, source_cluster, api_resource, namespace) "
            "VALUES ($1, 'kubernetes', 'cluster-a', 'core/v1/pods', 'workers')",
            scope_id,
        )
        await setup.execute(
            "INSERT INTO resource_lifecycle_heads "
            "(source_lifecycle_id, latest_revision_no) VALUES ($1, 1)",
            lifecycle_id,
        )
        await setup.execute(
            "INSERT INTO resource_intervals ("
            "id, inventory_scope_id, source_cluster, source_kind, source_uid, "
            "source_api_version, source_lifecycle_id, revision_no, "
            "source_revision, namespace, name, category, resource, "
            "measurement_basis, cost_domain, resource_class, "
            "attribution_scope, owner_kind, owner_id, user_id, project_id, "
            "attribution_source, attribution_quality, lifecycle_confidence, "
            "cpu_millicores, memory_bytes, capacity_source, capacity_quality, "
            "measurement_algorithm, started_at, start_time_source, "
            "start_uncertainty_us, ended_at, end_time_source, "
            "end_uncertainty_us, last_seen_at, last_confirmed_at, "
            "materialized_through, end_reason, details) VALUES ("
            "$1, $2, 'cluster-a', 'pod', 'pod-a', 'v1', $3, 1, $4, "
            "'workers', 'workspace-a', 'compute', 'workspace_pod', "
            "'scheduler-request', 'workload-allocation', 'kubernetes-pod', "
            "'customer', 'job', $5, $6, $7, 'job-label-db', 'exact', "
            "'kubernetes-visible', 2000, $8, 'pod-requests-v1', 'exact', "
            "'kubernetes-pod-requests-v1', $9, 'cutover-barrier', 0, $10, "
            "'backend-close', 0, $10, $10, $9, 'cutover-test', '{}'::jsonb)",
            interval_id,
            scope_id,
            lifecycle_id,
            "a" * 64,
            str(owner_id),
            user_id,
            project_id,
            4 * 1024**3,
            cutover,
            cutover + timedelta(hours=1),
        )
        await setup.execute(
            "UPDATE resource_lifecycle_heads SET current_interval_id=$1 "
            "WHERE source_lifecycle_id=$2",
            interval_id,
            lifecycle_id,
        )
        await setup.close()

        class StrictLedgerStub:
            def __init__(self) -> None:
                self.calls = 0

            async def publish_frozen_events(self, events):
                self.calls += 1
                return StrictUsagePublishResult(
                    expected=len(events), inserted=len(events), verified=len(events)
                )

        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
        ledger = StrictLedgerStub()
        materializer = InfrastructureUsageMaterializer(
            pool,
            ledger,  # type: ignore[arg-type]
            publication_enabled=True,
            batch_size=5,
        )

        plans = await materializer.plan_batch(7)
        assert len(plans) == 1
        assert len(plans[0].events) == 2
        assert (
            await pool.fetchval(
                "SELECT count(*) FROM resource_publication_plan_events "
                "WHERE plan_id=$1",
                plans[0].id,
            )
            == 2
        )
        pending = await materializer.next_pending_plan()
        assert pending == plans[0]

        published = await materializer.publish_one(7)
        assert published is not None and published.cursor_advanced
        assert ledger.calls == 1
        assert await pool.fetchval(
            "SELECT materialized_through FROM resource_intervals WHERE id=$1",
            interval_id,
        ) == cutover + timedelta(hours=1)
        state = await pool.fetchrow(
            "SELECT state, attempt_count, sanitized_error "
            "FROM resource_publication_plans WHERE id=$1",
            plans[0].id,
        )
        assert tuple(state) == ("published", 1, None)
        assert await materializer.publish_one(7) is None
    finally:
        if pool is not None:
            await pool.close()
        if not setup.is_closed():
            await setup.close()
        admin = await asyncpg.connect(app_pg_dsn)
        try:
            await admin.execute(f'DROP DATABASE IF EXISTS "{dbname}" WITH (FORCE)')
        finally:
            await admin.close()


@pytest.mark.asyncio
async def test_pod_reconciler_revalidates_attribution_and_lifetime_evidence(
    app_pg_dsn: str,
) -> None:
    dbname = f"metering_pods_{uuid4().hex[:12]}"
    admin = await asyncpg.connect(app_pg_dsn)
    try:
        await admin.execute(f'CREATE DATABASE "{dbname}"')
    finally:
        await admin.close()
    dsn = _swap_db(app_pg_dsn, dbname)
    setup = await asyncpg.connect(dsn)
    pool = None
    try:
        await setup.execute('CREATE EXTENSION "uuid-ossp"')
        await setup.execute("CREATE TABLE usage_rate_cards (id TEXT PRIMARY KEY)")
        await setup.execute(
            "CREATE TABLE rollup_state (name TEXT PRIMARY KEY, last_closed_day DATE)"
        )
        await setup.execute(APP_MIGRATION.read_text())
        await setup.execute(APP_INGESTION_MIGRATION.read_text())
        await setup.execute(APP_INGESTION_SIZE_FIX.read_text())
        await setup.execute(
            "CREATE TABLE jobs ("
            "id UUID PRIMARY KEY, user_id UUID, project_id UUID, "
            "context JSONB NOT NULL DEFAULT '{}'::jsonb)"
        )
        await setup.execute(
            "CREATE TABLE threads ("
            "id UUID PRIMARY KEY, user_id UUID, project_id UUID, "
            "metadata JSONB NOT NULL DEFAULT '{}'::jsonb)"
        )
        await setup.execute(
            "CREATE TABLE workspace_intervals ("
            "id BIGSERIAL PRIMARY KEY, owner_kind TEXT NOT NULL, "
            "owner_id UUID NOT NULL, cpu_millicores BIGINT NOT NULL, "
            "mem_bytes BIGINT NOT NULL, started_at TIMESTAMPTZ NOT NULL, "
            "ended_at TIMESTAMPTZ)"
        )

        scope_id, epoch_id = uuid4(), uuid4()
        scope = InventoryScopeIdentity(
            collector_id="kubernetes",
            source_cluster="cluster-pods",
            api_resource="core/v1/pods",
            namespace="workers",
        )
        await setup.execute(
            "INSERT INTO resource_inventory_scopes "
            "(id, collector_id, source_cluster, api_resource, namespace) "
            "VALUES ($1, $2, $3, $4, $5)",
            scope_id,
            scope.collector_id,
            scope.source_cluster,
            scope.api_resource,
            scope.namespace,
        )
        await setup.execute(
            "INSERT INTO resource_inventory_scope_epochs "
            "(id, scope_id, epoch_number, coverage_mode) "
            "VALUES ($1, $2, 1, 'list-watch')",
            epoch_id,
            scope_id,
        )
        await setup.close()

        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
        store = InventoryStore(
            pool,
            max_batch_bytes=100_000,
            max_snapshot_items=100,
            max_snapshot_bytes=1_000_000,
        )
        assert await store.activate_generation() == 1
        reconciler = PodIntervalReconciler(shadow_enabled=True)

        generic_ticket = await store.issue_ingest_ticket(
            epoch_id,
            "e" * 64,
            scope=scope,
            transport=_transport("snapshot-ticket"),
        )
        generic_snapshot_id = uuid4()
        generic_collection_start = datetime.now(timezone.utc)
        await store.begin_snapshot(
            generic_ticket.token,
            generic_ticket.id,
            generic_snapshot_id,
            generic_collection_start,
            scope=scope,
            transport=_transport("snapshot-begin"),
        )
        assert (
            await store.stage_items(
                generic_ticket.token,
                generic_ticket.id,
                generic_snapshot_id,
                (),
                scope=scope,
                transport=_transport("snapshot-items"),
            )
        ).total == 0
        generic_legacy_start = datetime.now(timezone.utc) - timedelta(minutes=2)
        generic_observed_start = generic_legacy_start + timedelta(seconds=30)
        generic_comparison = ShadowComparison(
            source_uid="generic-shadow-lifetime",
            status=ShadowComparisonStatus.LIFETIME_MISMATCH,
            reason_code="start-semantics",
            explained=False,
            comparison_at=datetime.now(timezone.utc),
            owner_trusted=True,
            owner_kind="job",
            owner_id=uuid4(),
            legacy_interval_id=1,
            legacy_cpu_millicores=700,
            legacy_memory_bytes=2 * 1024**3 - 64 * 1024**2,
            legacy_started_at=generic_legacy_start,
            observed_cpu_millicores=750,
            observed_memory_bytes=2 * 1024**3,
            observed_started_at=generic_observed_start,
            observed_start_time_source="app-db-received",
            observed_start_uncertainty_us=30_000_000,
            start_delta_us=30_000_000,
        )
        first_generic_stage = await store.stage_shadow_comparisons(
            generic_ticket.token,
            generic_ticket.id,
            generic_snapshot_id,
            (generic_comparison,),
            scope=scope,
            transport=_transport("snapshot-shadow"),
        )
        assert (first_generic_stage.inserted, first_generic_stage.total) == (1, 1)
        replayed_generic_stage = await store.stage_shadow_comparisons(
            generic_ticket.token,
            generic_ticket.id,
            generic_snapshot_id,
            (generic_comparison,),
            scope=scope,
            transport=_transport("snapshot-shadow"),
        )
        assert (replayed_generic_stage.inserted, replayed_generic_stage.total) == (
            0,
            1,
        )
        with pytest.raises(InventoryConflictError, match="shadow comparison"):
            await store.stage_shadow_comparisons(
                generic_ticket.token,
                generic_ticket.id,
                generic_snapshot_id,
                (
                    replace(
                        generic_comparison, observed_start_uncertainty_us=30_000_001
                    ),
                ),
                scope=scope,
                transport=_transport("snapshot-shadow"),
            )
        await store.finalize_snapshot(
            generic_ticket.token,
            generic_ticket.id,
            generic_snapshot_id,
            SnapshotFinalization(
                collection_completed_at=datetime.now(timezone.utc),
                complete=True,
                item_count=0,
                item_digest=inventory_manifest_digest(()),
                resource_version="rv-generic-shadow",
            ),
            scope=scope,
            transport=_transport("snapshot-finalize"),
        )
        async with pool.acquire() as conn:
            generic_persisted = await conn.fetchrow(
                "SELECT legacy_started_at, observed_started_at, "
                "observed_start_time_source, observed_start_uncertainty_us, "
                "start_delta_us, status, explained "
                "FROM resource_inventory_shadow_comparisons "
                "WHERE snapshot_id=$1 AND source_uid='generic-shadow-lifetime'",
                generic_snapshot_id,
            )
        assert generic_persisted["legacy_started_at"] == generic_legacy_start
        assert generic_persisted["observed_started_at"] == generic_observed_start
        assert generic_persisted["observed_start_time_source"] == "app-db-received"
        assert generic_persisted["observed_start_uncertainty_us"] == 30_000_000
        assert generic_persisted["start_delta_us"] == 30_000_000
        assert generic_persisted["status"] == "lifetime-mismatch"
        assert generic_persisted["explained"] is False

        snapshot_number = 0

        async def complete_snapshot(
            items: tuple[InventoryItem, ...], *, compare: bool = False
        ):
            nonlocal snapshot_number
            snapshot_number += 1
            request_digest = f"{snapshot_number:064x}"
            ticket = await store.issue_ingest_ticket(
                epoch_id,
                request_digest,
                scope=scope,
                transport=_transport("snapshot-ticket"),
            )
            snapshot_id = uuid4()
            collection_started_at = datetime.now(timezone.utc)
            await store.begin_snapshot(
                ticket.token,
                ticket.id,
                snapshot_id,
                collection_started_at,
                scope=scope,
                transport=_transport("snapshot-begin"),
            )
            await store.stage_items(
                ticket.token,
                ticket.id,
                snapshot_id,
                items,
                scope=scope,
                transport=_transport("snapshot-items"),
            )
            result = await store.finalize_snapshot(
                ticket.token,
                ticket.id,
                snapshot_id,
                SnapshotFinalization(
                    collection_completed_at=datetime.now(timezone.utc),
                    complete=True,
                    item_count=len(items),
                    item_digest=inventory_manifest_digest(items),
                    resource_version=f"rv-list-{snapshot_number}",
                ),
                scope=scope,
                transport=_transport("snapshot-finalize"),
                interval_mutator=reconciler.apply_snapshot,
                observation_hook=(reconciler.observe_snapshot if compare else None),
            )
            async with pool.acquire() as conn:
                received_at = await conn.fetchval(
                    "SELECT received_at FROM resource_inventory_snapshots WHERE id=$1",
                    snapshot_id,
                )
            return result, snapshot_id, received_at

        owner_id, user_id, project_id = uuid4(), uuid4(), uuid4()
        pod_name = "workspace-attribution-test"
        lifecycle_transition = datetime.now(timezone.utc) - timedelta(minutes=2)
        same_hash_item = _workspace_pod_item(
            owner_id=owner_id,
            source_uid="pod-attribution",
            name=pod_name,
            revision_hash="a" * 64,
            transition_at=lifecycle_transition,
            overhead_cpu_millicores=50,
            overhead_memory_bytes=64 * 1024**2,
        )

        _, first_snapshot_id, first_received_at = await complete_snapshot(
            (same_hash_item,)
        )
        async with pool.acquire() as conn:
            initial = await conn.fetchrow(
                "SELECT * FROM resource_intervals "
                "WHERE source_uid='pod-attribution' AND ended_at IS NULL"
            )
        assert initial["attribution_scope"] == "unknown"
        assert initial["started_at"] == first_received_at
        assert initial["start_time_source"] == "app-db-received"
        expected_uncertainty = first_received_at - lifecycle_transition
        expected_uncertainty_us = (
            expected_uncertainty.days * 86_400_000_000
            + expected_uncertainty.seconds * 1_000_000
            + expected_uncertainty.microseconds
        )
        assert initial["start_uncertainty_us"] == expected_uncertainty_us
        assert initial["start_uncertainty_us"] > 0
        assert initial["last_seen_snapshot_id"] == first_snapshot_id
        unknown_interval_id = initial["id"]

        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO jobs (id, user_id, project_id, context) VALUES ("
                "$1, $2, $3, jsonb_build_object('workspace_container', "
                "jsonb_build_object('pod_name', $4::text, "
                "'namespace', 'workers'))) ",
                owner_id,
                user_id,
                project_id,
                pod_name,
            )
        _, _, customer_received_at = await complete_snapshot((same_hash_item,))
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM resource_intervals "
                "WHERE source_uid='pod-attribution' ORDER BY revision_no"
            )
        assert len(rows) == 2
        assert rows[0]["id"] == unknown_interval_id
        assert rows[0]["ended_at"] == customer_received_at
        assert rows[0]["end_reason"] == "attribution-changed"
        assert rows[1]["attribution_scope"] == "customer"
        assert rows[1]["owner_id"] == str(owner_id)
        assert rows[1]["user_id"] == user_id
        assert rows[1]["project_id"] == project_id
        assert rows[1]["attribution_source"] == "app-db-owner-binding"
        assert rows[1]["started_at"] == customer_received_at
        customer_interval_id = rows[1]["id"]

        _, _, confirmation_received_at = await complete_snapshot((same_hash_item,))
        async with pool.acquire() as conn:
            confirmed = await conn.fetchrow(
                "SELECT id, last_confirmed_at FROM resource_intervals "
                "WHERE source_uid='pod-attribution' AND ended_at IS NULL"
            )
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM resource_intervals "
                    "WHERE source_uid='pod-attribution'"
                )
                == 2
            )
        assert confirmed["id"] == customer_interval_id
        assert confirmed["last_confirmed_at"] == confirmation_received_at

        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM jobs WHERE id=$1", owner_id)
        _, _, unknown_received_at = await complete_snapshot((same_hash_item,))
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM resource_intervals "
                "WHERE source_uid='pod-attribution' ORDER BY revision_no"
            )
        assert len(rows) == 3
        assert rows[1]["ended_at"] == unknown_received_at
        assert rows[1]["end_reason"] == "attribution-changed"
        assert rows[2]["attribution_scope"] == "unknown"
        assert rows[2]["started_at"] == unknown_received_at

        legacy_started_at = first_received_at - timedelta(seconds=30)
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO jobs (id, user_id, project_id, context) VALUES ("
                "$1, $2, $3, jsonb_build_object('workspace_container', "
                "jsonb_build_object('pod_name', $4::text, "
                "'namespace', 'workers'))) ",
                owner_id,
                user_id,
                project_id,
                pod_name,
            )
            await conn.execute(
                "INSERT INTO workspace_intervals "
                "(owner_kind, owner_id, cpu_millicores, mem_bytes, started_at) "
                "VALUES ('job', $1, 700, $2, $3)",
                owner_id,
                2 * 1024**3 - 64 * 1024**2,
                legacy_started_at,
            )
        _, shadow_snapshot_id, shadow_received_at = await complete_snapshot(
            (same_hash_item,), compare=True
        )
        async with pool.acquire() as conn:
            shadow = await conn.fetchrow(
                "SELECT * FROM resource_inventory_shadow_comparisons "
                "WHERE snapshot_id=$1 AND source_uid='pod-attribution'",
                shadow_snapshot_id,
            )
        # Capacity divergence is the primary blocker. Lifetime evidence is
        # still persisted below, but it must not hide a quantity mismatch.
        assert shadow["status"] == "capacity-mismatch"
        assert shadow["reason_code"] == "capacity-difference"
        assert shadow["explained"] is False
        assert shadow["observed_cpu_millicores"] - shadow["legacy_cpu_millicores"] == 50
        assert (
            shadow["observed_memory_bytes"] - shadow["legacy_memory_bytes"]
            == 64 * 1024**2
        )
        assert shadow["legacy_started_at"] == legacy_started_at
        assert shadow["observed_started_at"] == shadow_received_at
        assert shadow["observed_start_time_source"] == "app-db-received"
        assert shadow["observed_start_uncertainty_us"] == 0
        shadow_delta = shadow_received_at - legacy_started_at
        assert shadow["start_delta_us"] == (
            shadow_delta.days * 86_400_000_000
            + shadow_delta.seconds * 1_000_000
            + shadow_delta.microseconds
        )

        pending_owner, added_owner, modified_owner = uuid4(), uuid4(), uuid4()
        pending_name = "workspace-watch-modified"
        pending_item = _workspace_pod_item(
            owner_id=pending_owner,
            source_uid="pod-watch-modified",
            name=pending_name,
            revision_hash="b" * 64,
            transition_at=datetime.now(timezone.utc) - timedelta(seconds=1),
            accrues=False,
        )
        _, _, baseline_received_at = await complete_snapshot(
            (same_hash_item, pending_item)
        )

        added_name = "workspace-watch-added"
        added_transition = baseline_received_at + timedelta(seconds=1)
        modified_transition = baseline_received_at + timedelta(seconds=2)
        added_item = _workspace_pod_item(
            owner_id=added_owner,
            source_uid="pod-watch-added",
            name=added_name,
            revision_hash="c" * 64,
            transition_at=added_transition,
        )
        modified_item = _workspace_pod_item(
            owner_id=modified_owner,
            source_uid="pod-watch-modified",
            name=pending_name,
            revision_hash="d" * 64,
            transition_at=modified_transition,
        )
        async with pool.acquire() as conn:
            for watch_owner, watch_user, watch_project, watch_name in (
                (added_owner, uuid4(), uuid4(), added_name),
                (modified_owner, uuid4(), uuid4(), pending_name),
            ):
                await conn.execute(
                    "INSERT INTO jobs (id, user_id, project_id, context) VALUES ("
                    "$1, $2, $3, jsonb_build_object('workspace_container', "
                    "jsonb_build_object('pod_name', $4::text, "
                    "'namespace', 'workers'))) ",
                    watch_owner,
                    watch_user,
                    watch_project,
                    watch_name,
                )
            async with conn.transaction():
                added_interval_id = await reconciler.apply_watch(
                    conn,
                    WatchIntervalMutationContext(
                        scope_epoch_id=epoch_id,
                        inventory_scope_id=scope_id,
                        source_cluster=scope.source_cluster,
                        namespace=scope.namespace,
                        event_type=WatchEventKind.ADDED,
                        received_at=baseline_received_at + timedelta(seconds=3),
                        existing_interval_id=None,
                        existing_source_revision=None,
                    ),
                    added_item,
                )
                modified_interval_id = await reconciler.apply_watch(
                    conn,
                    WatchIntervalMutationContext(
                        scope_epoch_id=epoch_id,
                        inventory_scope_id=scope_id,
                        source_cluster=scope.source_cluster,
                        namespace=scope.namespace,
                        event_type=WatchEventKind.MODIFIED,
                        received_at=baseline_received_at + timedelta(seconds=4),
                        existing_interval_id=None,
                        existing_source_revision=None,
                    ),
                    modified_item,
                )
            watch_rows = await conn.fetch(
                "SELECT id, source_uid, started_at, start_time_source, "
                "start_uncertainty_us FROM resource_intervals "
                "WHERE id=ANY($1::uuid[]) ORDER BY source_uid",
                [added_interval_id, modified_interval_id],
            )
        by_uid = {row["source_uid"]: row for row in watch_rows}
        assert by_uid["pod-watch-added"]["id"] == added_interval_id
        assert by_uid["pod-watch-added"]["started_at"] == added_transition
        assert (
            by_uid["pod-watch-added"]["start_time_source"] == "pod-scheduled-transition"
        )
        assert by_uid["pod-watch-added"]["start_uncertainty_us"] == 0
        assert by_uid["pod-watch-modified"]["id"] == modified_interval_id
        assert by_uid["pod-watch-modified"]["started_at"] == modified_transition
        assert (
            by_uid["pod-watch-modified"]["start_time_source"]
            == "pod-scheduled-transition"
        )
        assert by_uid["pod-watch-modified"]["start_uncertainty_us"] == 0
    finally:
        if not setup.is_closed():
            await setup.close()
        if pool is not None:
            await pool.close()
        admin = await asyncpg.connect(app_pg_dsn)
        try:
            await admin.execute(f'DROP DATABASE IF EXISTS "{dbname}" WITH (FORCE)')
        finally:
            await admin.close()


@pytest.mark.asyncio
async def test_inventory_store_snapshot_watch_and_recovery_are_atomic(
    app_pg_dsn: str,
) -> None:
    dbname = f"metering_store_{uuid4().hex[:12]}"
    admin = await asyncpg.connect(app_pg_dsn)
    try:
        await admin.execute(f'CREATE DATABASE "{dbname}"')
    finally:
        await admin.close()
    dsn = _swap_db(app_pg_dsn, dbname)
    setup = await asyncpg.connect(dsn)
    try:
        await setup.execute('CREATE EXTENSION "uuid-ossp"')
        await setup.execute("CREATE TABLE usage_rate_cards (id TEXT PRIMARY KEY)")
        await setup.execute(
            "CREATE TABLE rollup_state (name TEXT PRIMARY KEY, last_closed_day DATE)"
        )
        await setup.execute(APP_MIGRATION.read_text())
        await setup.execute(APP_INGESTION_MIGRATION.read_text())
        await setup.execute(APP_INGESTION_SIZE_FIX.read_text())
    finally:
        await setup.close()

    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
    assert pool is not None
    scope_id, epoch_id = uuid4(), uuid4()
    scope = InventoryScopeIdentity(
        collector_id="kubernetes",
        source_cluster="cluster-a",
        api_resource="core/v1/pods",
        namespace="workers",
    )
    original_proof = datetime.now(timezone.utc) - timedelta(minutes=2)
    revisions = {key: key * 64 for key in ("a", "b", "c", "d", "e")}
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO resource_inventory_scopes "
            "(id, collector_id, source_cluster, api_resource, namespace) "
            "VALUES ($1, $2, $3, $4, $5)",
            scope_id,
            scope.collector_id,
            scope.source_cluster,
            scope.api_resource,
            scope.namespace,
        )
        await conn.execute(
            "INSERT INTO resource_inventory_scope_epochs "
            "(id, scope_id, epoch_number, coverage_mode) "
            "VALUES ($1, $2, 1, 'list-watch')",
            epoch_id,
            scope_id,
        )
        interval_ids = {}
        for uid, revision in (
            ("present", revisions["a"]),
            ("absent", revisions["b"]),
            ("invalid", revisions["c"]),
        ):
            interval_ids[uid] = await _insert_open_test_interval(
                conn,
                inventory_scope_id=scope_id,
                source_cluster=scope.source_cluster,
                namespace=scope.namespace or "",
                source_uid=uid,
                source_revision=revision,
                observed_at=original_proof,
            )

    leader = InventoryStore(
        pool,
        max_batch_bytes=100_000,
        max_snapshot_items=100,
        max_snapshot_bytes=1_000_000,
    )
    consumer = InventoryStore(
        pool,
        max_batch_bytes=100_000,
        max_snapshot_items=100,
        max_snapshot_bytes=1_000_000,
    )
    generation = await leader.activate_generation()
    assert generation == 1

    ticket = await leader.issue_ingest_ticket(
        epoch_id,
        "1" * 64,
        scope=scope,
        transport=_transport("snapshot-ticket"),
    )
    snapshot_id = uuid4()
    started_at = datetime.now(timezone.utc) - timedelta(seconds=5)
    await consumer.begin_snapshot(
        ticket.token,
        ticket.id,
        snapshot_id,
        started_at,
        scope=scope,
        transport=_transport("snapshot-begin"),
    )
    items = (
        InventoryItem(
            "pod",
            "present",
            revisions["a"],
            {"source_kind": "pod", "uid": "present", "namespace": "workers"},
            True,
        ),
        InventoryItem(
            "pod",
            "invalid",
            None,
            {"source_kind": "pod", "uid": "invalid", "namespace": "workers"},
            False,
            SanitizedInventoryError("capacity-invalid"),
        ),
        InventoryItem(
            "pod",
            "new",
            revisions["d"],
            {
                "source_kind": "pod",
                "uid": "new",
                "namespace": "workers",
                # Cross PostgreSQL's TOAST threshold. Before migration 0088,
                # INSERT ... RETURNING counted this expanded datum while the
                # ticket trigger compared its compressed stored form.
                "diagnostic_padding": "compressible-value-" * 2_000,
            },
            True,
        ),
        InventoryItem(
            "pod",
            "platform",
            revisions["e"],
            {"source_kind": "pod", "uid": "platform", "namespace": "workers"},
            True,
        ),
    )
    first_stage_claim = _transport("snapshot-items")
    staged = await consumer.stage_items(
        ticket.token,
        ticket.id,
        snapshot_id,
        items,
        scope=scope,
        transport=first_stage_claim,
    )
    assert (staged.inserted, staged.total) == (4, 4)
    with pytest.raises(InventoryConflictError, match="nonce"):
        await consumer.stage_items(
            ticket.token,
            ticket.id,
            snapshot_id,
            items,
            scope=scope,
            transport=first_stage_claim,
        )
    replay_stage = await consumer.stage_items(
        ticket.token,
        ticket.id,
        snapshot_id,
        items,
        scope=scope,
        transport=_transport("snapshot-items"),
    )
    assert (replay_stage.inserted, replay_stage.total) == (0, 4)

    async def mutate_snapshot(conn, context, item):
        if item.source_uid == "platform":
            return None
        if context.existing_source_revision == item.revision_hash:
            assert context.existing_interval_id is not None
            return await conn.fetchval(
                "UPDATE resource_intervals SET last_seen_at=$2, "
                "last_confirmed_at=$2, updated_at=statement_timestamp() "
                "WHERE id=$1 RETURNING id",
                context.existing_interval_id,
                context.received_at,
            )
        assert item.source_uid == "new"
        return await _insert_open_test_interval(
            conn,
            inventory_scope_id=context.inventory_scope_id,
            source_cluster=context.source_cluster,
            namespace=context.namespace or "",
            source_uid=item.source_uid,
            source_revision=item.revision_hash or "",
            observed_at=context.received_at,
        )

    observed_uids = []

    async def compare_snapshot(conn, context, item):
        observed_uids.append(item.source_uid)
        if item.source_uid == "platform":
            status, reason = "not-applicable", "non-workspace-pod"
        elif not item.valid_for_metering:
            status, reason = "invalid-observation", "capacity-invalid"
        else:
            status, reason = "matched", "exact-match"
        await conn.execute(
            "INSERT INTO resource_inventory_shadow_comparisons ("
            "snapshot_id, inventory_scope_id, source_uid, owner_trusted, "
            "status, reason_code, explained, comparison_at) VALUES ("
            "$1, $2, $3, FALSE, $4, $5, TRUE, $6)",
            context.snapshot_id,
            context.inventory_scope_id,
            item.source_uid,
            status,
            reason,
            context.received_at,
        )

    final = SnapshotFinalization(
        collection_completed_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        complete=True,
        item_count=len(items),
        item_digest=inventory_manifest_digest(items),
        resource_version="rv-list",
    )
    reconciled = await consumer.finalize_snapshot(
        ticket.token,
        ticket.id,
        snapshot_id,
        final,
        scope=scope,
        transport=_transport("snapshot-finalize"),
        interval_mutator=mutate_snapshot,
        observation_hook=compare_snapshot,
    )
    assert reconciled.closed_intervals == 1
    assert reconciled.invalid_items == 1
    assert reconciled.confirmed_intervals == 2
    assert reconciled.pending_valid_items == 0
    assert reconciled.shadow_comparisons == 4
    assert set(observed_uids) == {"present", "invalid", "new", "platform"}

    replayed = await consumer.finalize_snapshot(
        ticket.token,
        ticket.id,
        snapshot_id,
        final,
        scope=scope,
        transport=_transport("snapshot-finalize"),
    )
    assert replayed.replayed
    async with pool.acquire() as conn:
        snapshot = await conn.fetchrow(
            "SELECT received_at, manifest_state FROM resource_inventory_snapshots "
            "WHERE id=$1",
            snapshot_id,
        )
        assert snapshot["manifest_state"] == "sealed"
        invalid = await conn.fetchrow(
            "SELECT last_seen_at, last_confirmed_at, ended_at "
            "FROM resource_intervals WHERE id=$1",
            interval_ids["invalid"],
        )
        assert invalid["ended_at"] is None
        assert invalid["last_seen_at"] == snapshot["received_at"]
        assert invalid["last_confirmed_at"] == original_proof
        assert await conn.fetchval(
            "SELECT ended_at IS NOT NULL FROM resource_intervals WHERE id=$1",
            interval_ids["absent"],
        )
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM resource_inventory_shadow_comparisons "
                "WHERE snapshot_id=$1",
                snapshot_id,
            )
            == 4
        )
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await conn.execute(
                "UPDATE resource_inventory_shadow_comparisons SET explained=FALSE "
                "WHERE snapshot_id=$1",
                snapshot_id,
            )

    incomplete_ticket = await leader.issue_ingest_ticket(
        epoch_id,
        "2" * 64,
        scope=scope,
        transport=_transport("snapshot-ticket"),
    )
    incomplete_id = uuid4()
    await consumer.begin_snapshot(
        incomplete_ticket.token,
        incomplete_ticket.id,
        incomplete_id,
        datetime.now(timezone.utc),
        scope=scope,
        transport=_transport("snapshot-begin"),
    )
    assert (
        await consumer.stage_items(
            incomplete_ticket.token,
            incomplete_ticket.id,
            incomplete_id,
            (),
            scope=scope,
            transport=_transport("snapshot-items"),
        )
    ).total == 0
    incomplete = await consumer.finalize_snapshot(
        incomplete_ticket.token,
        incomplete_ticket.id,
        incomplete_id,
        SnapshotFinalization(
            collection_completed_at=datetime.now(timezone.utc),
            complete=False,
            item_count=0,
            item_digest=None,
            fatal_errors=(SanitizedInventoryError("collector-timeout"),),
        ),
        scope=scope,
        transport=_transport("snapshot-finalize"),
    )
    assert not incomplete.complete and incomplete.closed_intervals == 0
    async with pool.acquire() as conn:
        assert await conn.fetchval(
            "SELECT ended_at IS NULL FROM resource_intervals WHERE id=$1",
            interval_ids["present"],
        )
        assert await conn.fetchval(
            "SELECT last_complete_snapshot_id=$1 "
            "FROM resource_inventory_scope_epochs WHERE id=$2",
            snapshot_id,
            epoch_id,
        )

    bounded_ticket = await leader.issue_ingest_ticket(
        epoch_id,
        "3" * 64,
        scope=scope,
        transport=_transport("snapshot-ticket"),
        max_snapshot_bytes=80,
    )
    bounded_id = uuid4()
    await consumer.begin_snapshot(
        bounded_ticket.token,
        bounded_ticket.id,
        bounded_id,
        datetime.now(timezone.utc),
        scope=scope,
        transport=_transport("snapshot-begin"),
    )
    failed_claim = _transport("snapshot-items")
    with pytest.raises(InventoryContractError, match="cumulative byte"):
        await consumer.stage_items(
            bounded_ticket.token,
            bounded_ticket.id,
            bounded_id,
            (items[0],),
            scope=scope,
            transport=failed_claim,
        )
    async with pool.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM resource_inventory_snapshot_items "
                "WHERE snapshot_id=$1",
                bounded_id,
            )
            == 0
        )
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM resource_inventory_transport_nonces "
                "WHERE collector_id=$1 AND request_nonce=$2",
                failed_claim.collector_id,
                failed_claim.request_nonce,
            )
            == 0
        )

    inventory_only_ticket = await leader.issue_ingest_ticket(
        epoch_id,
        "b" * 64,
        scope=scope,
        transport=_transport("snapshot-ticket"),
    )
    inventory_only_id = uuid4()
    await consumer.begin_snapshot(
        inventory_only_ticket.token,
        inventory_only_ticket.id,
        inventory_only_id,
        datetime.now(timezone.utc),
        scope=scope,
        transport=_transport("snapshot-begin"),
    )
    await consumer.stage_items(
        inventory_only_ticket.token,
        inventory_only_ticket.id,
        inventory_only_id,
        (),
        scope=scope,
        transport=_transport("snapshot-items"),
    )
    inventory_only = await consumer.finalize_snapshot(
        inventory_only_ticket.token,
        inventory_only_ticket.id,
        inventory_only_id,
        SnapshotFinalization(
            collection_completed_at=datetime.now(timezone.utc),
            complete=True,
            item_count=0,
            item_digest=inventory_manifest_digest(()),
            resource_version="rv-inventory-only",
        ),
        scope=scope,
        transport=_transport("snapshot-finalize"),
        reconcile_intervals=False,
    )
    assert inventory_only.closed_intervals == 0
    assert inventory_only.observed_intervals == 0
    assert inventory_only.shadow_comparisons == 0
    async with pool.acquire() as conn:
        assert await conn.fetchval(
            "SELECT ended_at IS NULL FROM resource_intervals WHERE id=$1",
            interval_ids["present"],
        )

    session = await leader.issue_watch_session(
        epoch_id,
        "4" * 64,
        "rv-inventory-only",
        scope=scope,
        transport=_transport("watch-session"),
        max_events=10,
        max_bytes=100,
    )
    before_bookmark = None
    async with pool.acquire() as conn:
        before_bookmark = await conn.fetchval(
            "SELECT last_confirmed_at FROM resource_intervals WHERE id=$1",
            interval_ids["present"],
        )
    bookmark = await consumer.apply_watch_event(
        session.token,
        session.id,
        uuid4(),
        "5" * 64,
        "rv-inventory-only",
        WatchObjectEvent(
            WatchEventKind.BOOKMARK,
            "rv-bookmark",
            datetime.now(timezone.utc),
            1,
        ),
        scope=scope,
        transport=_transport("watch-event"),
    )
    assert bookmark.mutation_action is WatchMutationAction.BOOKMARK
    async with pool.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT last_confirmed_at FROM resource_intervals WHERE id=$1",
                interval_ids["present"],
            )
            == before_bookmark
        )

    not_applicable = InventoryItem(
        "pod",
        "platform-pod",
        "e" * 64,
        {"source_kind": "pod", "uid": "platform-pod", "namespace": "workers"},
        True,
    )

    async def ignore_non_workspace(_conn, _context, _item):
        return None

    ignored = await consumer.apply_watch_event(
        session.token,
        session.id,
        uuid4(),
        "6" * 64,
        "rv-bookmark",
        WatchObjectEvent(
            WatchEventKind.ADDED,
            "rv-platform",
            datetime.now(timezone.utc),
            1,
            item=not_applicable,
        ),
        scope=scope,
        transport=_transport("watch-event"),
        interval_mutator=ignore_non_workspace,
    )
    assert ignored.mutation_action is WatchMutationAction.NOT_APPLICABLE

    # Simulate a lost HTTP response after the server committed this cursor.
    # The collector still reports its last acknowledged cursor plus the
    # attempted cursor; gap persistence must accept either server outcome and
    # force recovery instead of guessing whether the mutation landed.
    ambiguous_apply = await consumer.apply_watch_event(
        session.token,
        session.id,
        uuid4(),
        "7" * 64,
        "rv-platform",
        WatchObjectEvent(
            WatchEventKind.BOOKMARK,
            "rv-attempted",
            datetime.now(timezone.utc),
            1,
        ),
        scope=scope,
        transport=_transport("watch-event"),
    )
    assert ambiguous_apply.resource_version == "rv-attempted"
    history_event_id = uuid4()
    history = await consumer.record_watch_gap(
        session.token,
        session.id,
        history_event_id,
        "8" * 64,
        "rv-platform",
        gap_reason="ambiguous-watch-apply",
        alternate_expected_resource_version="rv-attempted",
        scope=scope,
        transport=_transport("watch-history-lost"),
    )
    assert history.mutation_action is WatchMutationAction.HISTORY_GAP
    assert history.resource_version == "rv-attempted"
    replayed_history = await consumer.record_watch_gap(
        session.token,
        session.id,
        history_event_id,
        "8" * 64,
        "rv-platform",
        gap_reason="ambiguous-watch-apply",
        alternate_expected_resource_version="rv-attempted",
        scope=scope,
        transport=_transport("watch-history-lost"),
    )
    assert replayed_history.replayed
    async with pool.acquire() as conn:
        assert await conn.fetchval(
            "SELECT reason='ambiguous-watch-apply' "
            "FROM resource_inventory_coverage_gaps WHERE id=$1",
            history.coverage_gap_id,
        )
    recovery = await leader.start_watch_recovery_epoch(
        epoch_id,
        scope=scope,
        transport=_transport("scope-recovery"),
    )
    assert recovery.recovery_from_epoch_id == epoch_id
    recovery_ticket = await leader.issue_ingest_ticket(
        recovery.scope_epoch_id,
        "9" * 64,
        scope=scope,
        transport=_transport("snapshot-ticket"),
    )
    recovery_snapshot = uuid4()
    await consumer.begin_snapshot(
        recovery_ticket.token,
        recovery_ticket.id,
        recovery_snapshot,
        datetime.now(timezone.utc),
        scope=scope,
        transport=_transport("snapshot-begin"),
    )
    await consumer.stage_items(
        recovery_ticket.token,
        recovery_ticket.id,
        recovery_snapshot,
        (),
        scope=scope,
        transport=_transport("snapshot-items"),
    )
    await consumer.finalize_snapshot(
        recovery_ticket.token,
        recovery_ticket.id,
        recovery_snapshot,
        SnapshotFinalization(
            collection_completed_at=datetime.now(timezone.utc),
            complete=True,
            item_count=0,
            item_digest=inventory_manifest_digest(()),
            resource_version="rv-recovered",
        ),
        scope=scope,
        transport=_transport("snapshot-finalize"),
    )
    resumed = await leader.issue_watch_session(
        recovery.scope_epoch_id,
        "a" * 64,
        "rv-recovered",
        scope=scope,
        transport=_transport("watch-session"),
    )
    assert resumed.scope_epoch_id == recovery.scope_epoch_id
    finishable = await leader.issue_watch_session(
        recovery.scope_epoch_id,
        "b" * 64,
        "rv-recovered",
        scope=scope,
        transport=_transport("watch-session"),
    )
    # Symmetric ambiguous-ACK case: the attempted apply did not commit, so the
    # server still has the collector's acknowledged cursor. Persisting the gap
    # and replaying it must use that actual server cursor, not the attempted one.
    not_committed_event_id = uuid4()
    not_committed_history = await consumer.record_watch_gap(
        resumed.token,
        resumed.id,
        not_committed_event_id,
        "b" * 64,
        "rv-recovered",
        gap_reason="ambiguous-watch-apply",
        alternate_expected_resource_version="rv-not-committed",
        scope=scope,
        transport=_transport("watch-history-lost"),
    )
    assert not_committed_history.resource_version == "rv-recovered"
    not_committed_replay = await consumer.record_watch_gap(
        resumed.token,
        resumed.id,
        not_committed_event_id,
        "b" * 64,
        "rv-recovered",
        gap_reason="ambiguous-watch-apply",
        alternate_expected_resource_version="rv-not-committed",
        scope=scope,
        transport=_transport("watch-history-lost"),
    )
    assert not_committed_replay.replayed
    assert not_committed_replay.resource_version == "rv-recovered"
    async with pool.acquire() as conn:
        not_committed_details = await conn.fetchrow(
            "SELECT event.expected_resource_version, "
            "gap.resolution_details->>'collector_committed_resource_version' "
            "AS collector_cursor, "
            "gap.resolution_details->>'attempted_resource_version' "
            "AS attempted_cursor, "
            "gap.resolution_details->>'server_committed_resource_version' "
            "AS server_cursor "
            "FROM resource_inventory_watch_events event "
            "JOIN resource_inventory_coverage_gaps gap "
            "ON gap.id=event.coverage_gap_id "
            "WHERE event.watch_session_id=$1 AND event.id=$2",
            resumed.id,
            not_committed_event_id,
        )
    assert not_committed_details["expected_resource_version"] == "rv-recovered"
    assert not_committed_details["collector_cursor"] == "rv-recovered"
    assert not_committed_details["attempted_cursor"] == "rv-not-committed"
    assert not_committed_details["server_cursor"] == "rv-recovered"
    assert await consumer.finish_watch_session(
        finishable.token,
        finishable.id,
        scope=scope,
        transport=_transport("watch-finish"),
    )
    assert not await consumer.finish_watch_session(
        finishable.token,
        finishable.id,
        scope=scope,
        transport=_transport("watch-finish"),
    )
    async with pool.acquire() as conn:
        gap = await conn.fetchrow(
            "SELECT gap_end, resolution FROM resource_inventory_coverage_gaps "
            "WHERE id=$1",
            history.coverage_gap_id,
        )
        assert gap["gap_end"] is not None
        assert gap["resolution"] == "unresolved"

    abandoned_ticket = await leader.issue_ingest_ticket(
        recovery.scope_epoch_id,
        "c" * 64,
        scope=scope,
        transport=_transport("snapshot-ticket"),
    )
    abandoned_snapshot_id = uuid4()
    await consumer.begin_snapshot(
        abandoned_ticket.token,
        abandoned_ticket.id,
        abandoned_snapshot_id,
        datetime.now(timezone.utc),
        scope=scope,
        transport=_transport("snapshot-begin"),
    )
    abandoned_item = InventoryItem(
        "pod",
        "abandoned",
        "f" * 64,
        {"source_kind": "pod", "uid": "abandoned", "namespace": "workers"},
        True,
    )
    await consumer.stage_items(
        abandoned_ticket.token,
        abandoned_ticket.id,
        abandoned_snapshot_id,
        (abandoned_item,),
        scope=scope,
        transport=_transport("snapshot-items"),
    )
    unbound_ticket = await leader.issue_ingest_ticket(
        recovery.scope_epoch_id,
        "d" * 64,
        scope=scope,
        transport=_transport("snapshot-ticket"),
    )

    retained_tables = (
        "resource_inventory_scopes",
        "resource_inventory_scope_epochs",
        "resource_inventory_coverage_gaps",
        "resource_intervals",
        "resource_publication_plans",
        "resource_publication_plan_events",
    )
    async with pool.acquire() as conn:
        retained_before = {
            table: int(await conn.fetchval(f"SELECT count(*) FROM {table}"))
            for table in retained_tables
        }
        snapshot_count_before = int(
            await conn.fetchval("SELECT count(*) FROM resource_inventory_snapshots")
        )
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await conn.execute(
                "DELETE FROM resource_inventory_ingest_tickets WHERE id=$1",
                unbound_ticket.id,
            )
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await conn.execute(
                "UPDATE resource_inventory_snapshots SET "
                "manifest_state='staging-expired', "
                "items_expired_at=statement_timestamp() WHERE id=$1",
                abandoned_snapshot_id,
            )

        for table in (
            "resource_inventory_snapshots",
            "resource_inventory_shadow_comparisons",
            "resource_inventory_watch_events",
            "resource_inventory_watch_sessions",
            "resource_inventory_ingest_tickets",
            "resource_inventory_transport_nonces",
        ):
            await conn.execute(f"ALTER TABLE {table} DISABLE TRIGGER USER")
        try:
            await conn.execute(
                "UPDATE resource_inventory_snapshots SET "
                "collection_started_at=collection_started_at-INTERVAL '40 days', "
                "collection_completed_at=collection_completed_at-INTERVAL '40 days', "
                "received_at=received_at-INTERVAL '40 days', "
                "sealed_at=sealed_at-INTERVAL '40 days', "
                "created_at=created_at-INTERVAL '40 days' WHERE id=$1",
                snapshot_id,
            )
            await conn.execute(
                "UPDATE resource_inventory_snapshots SET "
                "collection_started_at=collection_started_at-INTERVAL '2 days', "
                "collection_completed_at=collection_completed_at-INTERVAL '2 days', "
                "received_at=received_at-INTERVAL '2 days', "
                "created_at=created_at-INTERVAL '2 days' WHERE id=$1",
                abandoned_snapshot_id,
            )
            await conn.execute(
                "UPDATE resource_inventory_shadow_comparisons SET "
                "comparison_at=comparison_at-INTERVAL '40 days', "
                "created_at=created_at-INTERVAL '40 days' WHERE snapshot_id=$1",
                snapshot_id,
            )
            await conn.execute(
                "UPDATE resource_inventory_watch_events SET "
                "collector_observed_at=collector_observed_at-INTERVAL '40 days', "
                "received_at=received_at-INTERVAL '40 days', "
                "created_at=created_at-INTERVAL '40 days' "
                "WHERE watch_session_id=$1",
                session.id,
            )
            await conn.execute(
                "UPDATE resource_inventory_watch_sessions SET "
                "created_at=created_at-INTERVAL '40 days', "
                "updated_at=updated_at-INTERVAL '40 days', "
                "expires_at=expires_at-INTERVAL '40 days', "
                "consumed_at=consumed_at-INTERVAL '40 days' "
                "WHERE id=ANY($1::uuid[])",
                [session.id, resumed.id],
            )
            await conn.execute(
                "UPDATE resource_inventory_ingest_tickets SET "
                "created_at=created_at-INTERVAL '2 days', "
                "expires_at=expires_at-INTERVAL '2 days', "
                "bound_at=bound_at-INTERVAL '2 days' "
                "WHERE id=ANY($1::uuid[])",
                [abandoned_ticket.id, unbound_ticket.id],
            )
            await conn.execute(
                "UPDATE resource_inventory_transport_nonces SET "
                "received_at=received_at-INTERVAL '40 days', "
                "expires_at=expires_at-INTERVAL '40 days'"
            )
        finally:
            for table in reversed(
                (
                    "resource_inventory_snapshots",
                    "resource_inventory_shadow_comparisons",
                    "resource_inventory_watch_events",
                    "resource_inventory_watch_sessions",
                    "resource_inventory_ingest_tickets",
                    "resource_inventory_transport_nonces",
                )
            ):
                await conn.execute(f"ALTER TABLE {table} ENABLE TRIGGER USER")

    assert await leader.purge_expired_transport_nonces(limit=2) == 2
    assert await leader.purge_expired_transport_nonces(limit=10_000) > 0

    purge_totals = {
        "sealed": 0,
        "abandoned": 0,
        "items": 0,
        "shadow": 0,
        "events": 0,
        "sessions": 0,
        "tickets": 0,
    }
    for _ in range(10):
        purged = await leader.purge_diagnostics(generation, limit=2)
        purge_totals["sealed"] += purged.sealed_snapshots_expired
        purge_totals["abandoned"] += purged.abandoned_snapshots_expired
        purge_totals["items"] += purged.snapshot_items_deleted
        purge_totals["shadow"] += purged.shadow_comparisons_deleted
        purge_totals["events"] += purged.watch_events_deleted
        purge_totals["sessions"] += purged.watch_sessions_deleted
        purge_totals["tickets"] += purged.unbound_tickets_deleted
        if not purged.might_have_more:
            break
    else:
        pytest.fail("bounded inventory retention did not drain")

    assert purge_totals == {
        "sealed": 1,
        "abandoned": 1,
        "items": 5,
        "shadow": 4,
        "events": 5,
        "sessions": 2,
        "tickets": 1,
    }
    async with pool.acquire() as conn:
        assert {
            table: int(await conn.fetchval(f"SELECT count(*) FROM {table}"))
            for table in retained_tables
        } == retained_before
        assert (
            await conn.fetchval("SELECT count(*) FROM resource_inventory_snapshots")
            == snapshot_count_before
        )
        assert await conn.fetchval(
            "SELECT manifest_state='items-expired' "
            "FROM resource_inventory_snapshots WHERE id=$1",
            snapshot_id,
        )
        assert await conn.fetchval(
            "SELECT manifest_state='staging-expired' "
            "FROM resource_inventory_snapshots WHERE id=$1",
            abandoned_snapshot_id,
        )
        assert not await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM resource_inventory_snapshot_items "
            "WHERE snapshot_id=ANY($1::uuid[]))",
            [snapshot_id, abandoned_snapshot_id],
        )
        assert not await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM resource_inventory_watch_events "
            "WHERE watch_session_id=$1)",
            session.id,
        )
        assert not await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM resource_inventory_watch_sessions "
            "WHERE id=ANY($1::uuid[]))",
            [session.id, resumed.id],
        )
        assert not await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM resource_inventory_ingest_tickets "
            "WHERE id=$1)",
            unbound_ticket.id,
        )
        assert await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM resource_inventory_ingest_tickets "
            "WHERE id=$1)",
            abandoned_ticket.id,
        )
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await conn.execute(
                "UPDATE resource_inventory_snapshots SET item_count=0 WHERE id=$1",
                abandoned_snapshot_id,
            )

    mismatch_ticket = await leader.issue_ingest_ticket(
        recovery.scope_epoch_id,
        "e" * 64,
        scope=scope,
        transport=_transport("snapshot-ticket"),
    )
    mismatch_snapshot_id = uuid4()
    await consumer.begin_snapshot(
        mismatch_ticket.token,
        mismatch_ticket.id,
        mismatch_snapshot_id,
        datetime.now(timezone.utc),
        scope=scope,
        transport=_transport("snapshot-begin"),
    )
    await consumer.finalize_snapshot(
        mismatch_ticket.token,
        mismatch_ticket.id,
        mismatch_snapshot_id,
        SnapshotFinalization(
            collection_completed_at=datetime.now(timezone.utc),
            complete=False,
            item_count=0,
            item_digest=None,
            fatal_errors=(SanitizedInventoryError("resource-version-mismatch"),),
        ),
        scope=scope,
        transport=_transport("snapshot-finalize"),
        reconcile_intervals=False,
    )
    async with pool.acquire() as conn:
        mismatch_gap = await conn.fetchrow(
            "SELECT gap.reason, epoch.continuity_health "
            "FROM resource_inventory_coverage_gaps gap "
            "JOIN resource_inventory_scope_epochs epoch "
            "ON epoch.id=gap.scope_epoch_id "
            "WHERE gap.scope_epoch_id=$1 "
            "AND gap.reason='list-resource-version-mismatch'",
            recovery.scope_epoch_id,
        )
        assert mismatch_gap["continuity_health"] == "gap"

    second_recovery = await leader.start_watch_recovery_epoch(
        recovery.scope_epoch_id,
        scope=scope,
        transport=_transport("scope-recovery"),
    )
    second_recovery_ticket = await leader.issue_ingest_ticket(
        second_recovery.scope_epoch_id,
        "f" * 64,
        scope=scope,
        transport=_transport("snapshot-ticket"),
    )
    second_recovery_snapshot = uuid4()
    await consumer.begin_snapshot(
        second_recovery_ticket.token,
        second_recovery_ticket.id,
        second_recovery_snapshot,
        datetime.now(timezone.utc),
        scope=scope,
        transport=_transport("snapshot-begin"),
    )
    await consumer.finalize_snapshot(
        second_recovery_ticket.token,
        second_recovery_ticket.id,
        second_recovery_snapshot,
        SnapshotFinalization(
            collection_completed_at=datetime.now(timezone.utc),
            complete=True,
            item_count=0,
            item_digest=inventory_manifest_digest(()),
            resource_version="rv-second-recovery",
        ),
        scope=scope,
        transport=_transport("snapshot-finalize"),
        reconcile_intervals=False,
    )
    async with pool.acquire() as conn:
        closed_mismatch_gap = await conn.fetchrow(
            "SELECT gap_end, resolution "
            "FROM resource_inventory_coverage_gaps "
            "WHERE scope_epoch_id=$1 "
            "AND reason='list-resource-version-mismatch'",
            recovery.scope_epoch_id,
        )
        assert closed_mismatch_gap["gap_end"] is not None
        assert closed_mismatch_gap["resolution"] == "unresolved"

    assert await leader.deactivate_generation(generation)
    with pytest.raises(InventoryFenceError, match="not active locally"):
        await leader.purge_diagnostics(generation)
    async with pool.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT leader_generation FROM infra_metering_control WHERE singleton"
            )
            == generation
        )
    await pool.close()


@pytest.mark.asyncio
async def test_app_composite_foreign_keys_fail_closed_on_postgres16(
    app_pg_dsn: str,
) -> None:
    dbname = f"metering_t_{uuid4().hex[:12]}"
    admin = await asyncpg.connect(app_pg_dsn)
    try:
        await admin.execute(f'CREATE DATABASE "{dbname}"')
    finally:
        await admin.close()

    conn = await asyncpg.connect(_swap_db(app_pg_dsn, dbname))
    try:
        await conn.execute('CREATE EXTENSION "uuid-ossp"')
        await conn.execute("CREATE TABLE usage_rate_cards (id TEXT PRIMARY KEY)")
        await conn.execute(
            "CREATE TABLE rollup_state (name TEXT PRIMARY KEY, last_closed_day DATE)"
        )
        await conn.execute(APP_MIGRATION.read_text())

        scope_a, scope_b = uuid4(), uuid4()
        epoch_a, epoch_b = uuid4(), uuid4()
        snapshot_a, snapshot_b, snapshot_incomplete = uuid4(), uuid4(), uuid4()
        await conn.execute(
            "INSERT INTO resource_inventory_scopes "
            "(id, collector_id, source_cluster, api_resource) VALUES "
            "($1, 'kubernetes', 'cluster-a', 'core/v1/pods'), "
            "($2, 'kubernetes', 'cluster-b', 'core/v1/pods')",
            scope_a,
            scope_b,
        )
        await conn.execute(
            "INSERT INTO resource_inventory_scope_epochs "
            "(id, scope_id, epoch_number, coverage_mode) VALUES "
            "($1, $2, 1, 'list-watch'), ($3, $4, 1, 'list-watch')",
            epoch_a,
            scope_a,
            epoch_b,
            scope_b,
        )
        observed = (datetime.now(timezone.utc) - timedelta(days=1)).replace(
            hour=12, minute=0, second=0, microsecond=0
        )
        collection_started = observed - timedelta(seconds=1)
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await conn.execute(
                "INSERT INTO resource_inventory_snapshots "
                "(scope_epoch_id, inventory_scope_id, collection_started_at, "
                "collection_completed_at, received_at, complete, "
                "leader_generation, item_count, item_digest, manifest_state, "
                "sealed_at) VALUES "
                "($1, $2, $3, $4, $4, TRUE, 1, 0, $5, 'sealed', now())",
                epoch_a,
                scope_a,
                collection_started,
                observed,
                "f" * 64,
            )
        metering_day = observed.date()
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await conn.execute(
                "INSERT INTO infra_usage_day_state "
                "(day, state, coverage_status, coverage_revision, sealed_at) "
                "VALUES ($1, 'sealed', 'complete', 'revision-0', now())",
                metering_day,
            )
        await conn.execute(
            "INSERT INTO infra_usage_day_state (day) VALUES ($1)",
            metering_day,
        )
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await conn.execute(
                "UPDATE infra_usage_day_state SET state='sealed', "
                "coverage_status='complete', coverage_revision='revision-1', "
                "sealed_at=now() WHERE day=$1",
                metering_day,
            )
        await conn.execute(
            "UPDATE infra_usage_day_state SET state='sealing' WHERE day=$1",
            metering_day,
        )
        await conn.execute(
            "UPDATE infra_usage_day_state SET state='sealed', "
            "coverage_status='complete', coverage_revision='revision-1', "
            "sealed_at=now(), updated_at=now() WHERE day=$1",
            metering_day,
        )
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await conn.execute(
                "UPDATE infra_usage_day_state SET coverage_status='partial', "
                "coverage_revision='revision-2' WHERE day=$1",
                metering_day,
            )
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await conn.execute(
                "DELETE FROM infra_usage_day_state WHERE day=$1",
                metering_day,
            )
        await conn.execute(
            "INSERT INTO resource_inventory_snapshots "
            "(id, scope_epoch_id, inventory_scope_id, collection_started_at, "
            "collection_completed_at, received_at, complete, "
            "leader_generation, item_count) "
            "VALUES ($1, $2, $3, $4, $4, $4, FALSE, 1, 0), "
            "($5, $6, $7, $4, $4, $4, FALSE, 1, 0), "
            "($8, $2, $3, $4, $4, $4, FALSE, 1, 0)",
            snapshot_a,
            epoch_a,
            scope_a,
            collection_started,
            snapshot_b,
            epoch_b,
            scope_b,
            snapshot_incomplete,
        )
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "INSERT INTO resource_inventory_snapshot_items "
                "(snapshot_id, source_kind, source_uid, normalized_item, "
                "valid_for_metering) VALUES ($1, 'pod', 'pod-a', '{}', TRUE)",
                snapshot_a,
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE resource_inventory_snapshots "
                "SET collection_completed_at=$2, received_at=$2, "
                "complete=TRUE, item_count=1, item_digest=$3, "
                "manifest_state='sealed', sealed_at=statement_timestamp() "
                "WHERE id=$1",
                snapshot_incomplete,
                observed,
                "e" * 64,
            )
        await conn.execute(
            "UPDATE resource_inventory_snapshots "
            "SET collection_completed_at=$2, received_at=$2, "
            "complete=TRUE, item_digest=$3, manifest_state='sealed', "
            "sealed_at=statement_timestamp() WHERE id = ANY($1::uuid[])",
            [snapshot_a, snapshot_b],
            observed,
            "0" * 64,
        )
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await conn.execute(
                "INSERT INTO resource_inventory_snapshot_items "
                "(snapshot_id, source_kind, source_uid, revision_hash, "
                "normalized_item, valid_for_metering) VALUES "
                "($1, 'pod', 'late-pod', $2, '{}', TRUE)",
                snapshot_a,
                "1" * 64,
            )
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await conn.execute(
                "UPDATE resource_inventory_snapshots SET item_digest=$2 WHERE id=$1",
                snapshot_a,
                "2" * 64,
            )
        await conn.execute(
            "UPDATE resource_inventory_scope_epochs "
            "SET last_complete_snapshot_id=$1 WHERE id=$2",
            snapshot_a,
            epoch_a,
        )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await conn.execute(
                "UPDATE resource_inventory_scope_epochs "
                "SET last_complete_snapshot_id=$1 WHERE id=$2",
                snapshot_a,
                epoch_b,
            )

        lifecycle_a, lifecycle_b = uuid4(), uuid4()
        interval_a = uuid4()
        revision_a = "a" * 64
        await conn.execute(
            "INSERT INTO resource_lifecycle_heads (source_lifecycle_id) "
            "VALUES ($1), ($2)",
            lifecycle_a,
            lifecycle_b,
        )
        interval_sql = """
            INSERT INTO resource_intervals (
                id, inventory_scope_id, source_cluster, source_kind,
                source_uid, source_api_version, source_lifecycle_id,
                revision_no, source_revision, name, category, resource,
                measurement_basis, cost_domain, resource_class,
                attribution_scope, owner_kind, owner_id, user_id,
                attribution_source, attribution_quality,
                lifecycle_confidence, cpu_millicores, memory_bytes,
                capacity_source, capacity_quality, measurement_algorithm,
                started_at, start_time_source, start_uncertainty_us,
                last_seen_at, last_confirmed_at, last_seen_snapshot_id,
                materialized_through
            ) VALUES (
                $1, $2, $3, 'pod', $4, 'v1', $5, 1, $6,
                'workspace-pod', 'compute', 'workspace_pod',
                'scheduler-request', 'workload-allocation',
                'kubernetes-pod', 'customer', 'job', $7, $8,
                'db-owner', 'exact', 'kubernetes-visible', 1000,
                4294967296, 'admitted-request', 'exact',
                'pod-requests-test-v1', $9, 'first-observation', 0,
                $10, $10, $11, $9
            )
        """
        owner_id, user_id = str(uuid4()), uuid4()
        await conn.execute(
            interval_sql,
            interval_a,
            scope_a,
            "cluster-a",
            "pod-a",
            lifecycle_a,
            revision_a,
            owner_id,
            user_id,
            observed,
            observed + timedelta(hours=1),
            snapshot_a,
        )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await conn.execute(
                interval_sql,
                uuid4(),
                scope_a,
                "cluster-b",
                "pod-b",
                lifecycle_b,
                "b" * 64,
                str(uuid4()),
                uuid4(),
                observed,
                observed + timedelta(hours=1),
                snapshot_a,
            )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await conn.execute(
                "UPDATE resource_intervals SET last_seen_snapshot_id=$1 WHERE id=$2",
                snapshot_b,
                interval_a,
            )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await conn.execute(
                "UPDATE resource_intervals SET last_seen_snapshot_id=$1 WHERE id=$2",
                snapshot_incomplete,
                interval_a,
            )
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await conn.execute(
                "UPDATE resource_intervals SET namespace='wrong-scope' WHERE id=$1",
                interval_a,
            )
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await conn.execute(
                "UPDATE resource_intervals SET cpu_millicores=2000 WHERE id=$1",
                interval_a,
            )
        advanced_seen = observed + timedelta(hours=1, minutes=1)
        await conn.execute(
            "UPDATE resource_intervals SET last_seen_at=$2, "
            "last_confirmed_at=$2, updated_at=statement_timestamp() WHERE id=$1",
            interval_a,
            advanced_seen,
        )
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await conn.execute(
                "UPDATE resource_intervals SET last_seen_at=$2 WHERE id=$1",
                interval_a,
                observed + timedelta(hours=1),
            )
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await conn.execute(
                "DELETE FROM resource_intervals WHERE id=$1",
                interval_a,
            )
        await conn.execute(
            "UPDATE resource_lifecycle_heads SET latest_revision_no=1, "
            "current_interval_id=$1 WHERE source_lifecycle_id=$2",
            interval_a,
            lifecycle_a,
        )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await conn.execute(
                "UPDATE resource_lifecycle_heads SET current_interval_id=$1 "
                "WHERE source_lifecycle_id=$2",
                interval_a,
                lifecycle_b,
            )

        plan_id = uuid4()
        plan_sql = """
            INSERT INTO resource_publication_plans (
                id, source_interval_id, source_revision, plan_kind,
                plan_revision, advances_cursor, previous_materialized_through,
                period_start, period_end, expected_event_count,
                payload_schema_version, event_set_hash, rate_selection_hash,
                creator_generation
            ) VALUES (
                $1, $2, $3, 'usage', 0, TRUE, $4, $4, $5, 1, 1,
                $6, $7, 1
            )
        """
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await conn.execute(
                plan_sql,
                uuid4(),
                interval_a,
                "f" * 64,
                observed + timedelta(minutes=30),
                observed + timedelta(minutes=45),
                "e" * 64,
                "f" * 64,
            )

        event_sql = (
            "INSERT INTO resource_publication_plan_events "
            "(plan_id, ordinal, source, source_id, unit, ts, event_kind, "
            "row_hash, event_payload) VALUES "
            "($1, $2, 'infra-allocation-v2', $3, $4, $5, $6, $7, '{}')"
        )
        async with conn.transaction():
            await conn.execute(
                plan_sql,
                plan_id,
                interval_a,
                revision_a,
                observed,
                observed + timedelta(minutes=30),
                "c" * 64,
                "d" * 64,
            )
            await conn.execute(
                event_sql,
                plan_id,
                0,
                "event-0",
                "vcpu-hour",
                observed,
                "usage",
                "1" * 64,
            )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await conn.execute(
                event_sql,
                plan_id,
                1,
                "event-wrong-kind",
                "gib-hour",
                observed,
                "late-usage",
                "2" * 64,
            )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await conn.execute(
                event_sql,
                plan_id,
                1,
                "event-wrong-time",
                "gib-hour",
                observed + timedelta(minutes=1),
                "usage",
                "3" * 64,
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                event_sql,
                plan_id,
                1,
                "event-late-append",
                "gib-hour",
                observed,
                "usage",
                "4" * 64,
            )
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await conn.execute(
                "UPDATE resource_publication_plan_events SET row_hash=$2 "
                "WHERE plan_id=$1",
                plan_id,
                "5" * 64,
            )
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await conn.execute(
                "UPDATE resource_publication_plans SET event_set_hash=$2 WHERE id=$1",
                plan_id,
                "6" * 64,
            )
        await conn.execute(
            "UPDATE resource_publication_plans SET state='published', "
            "attempt_count=1, last_attempt_at=statement_timestamp(), "
            "published_at=statement_timestamp() WHERE id=$1",
            plan_id,
        )
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await conn.execute(
                "UPDATE resource_publication_plans SET attempt_count=2, "
                "last_attempt_at=statement_timestamp() WHERE id=$1",
                plan_id,
            )
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await conn.execute(
                event_sql,
                plan_id,
                1,
                "event-after-publish",
                "gib-hour",
                observed,
                "usage",
                "7" * 64,
            )
        await conn.execute(
            "DELETE FROM resource_publication_plan_events WHERE plan_id=$1",
            plan_id,
        )
        await conn.execute(
            "DELETE FROM resource_publication_plans WHERE id=$1",
            plan_id,
        )

        rate_a, rate_b = uuid4(), uuid4()
        rate_start = observed - timedelta(days=1)
        rate_boundary = observed + timedelta(days=1)
        rate_insert_sql = """
            INSERT INTO usage_rates_v2 (
                id, cost_domain, measurement_basis, category,
                resource_class, resource, unit, effective_from,
                usd_per_unit, source, source_version
            ) VALUES (
                $1, 'workload-allocation', 'scheduler-request', 'compute',
                'kubernetes-pod', 'workspace_pod', 'vcpu-hour', $2,
                $3, 'operator', $4
            )
        """
        await conn.execute(rate_insert_sql, rate_a, rate_start, Decimal("0.1"), "v1")
        with pytest.raises(asyncpg.ExclusionViolationError):
            await conn.execute(
                rate_insert_sql, rate_b, rate_boundary, Decimal("0.2"), "v2"
            )
        async with conn.transaction():
            await conn.execute(
                "UPDATE usage_rates_v2 SET effective_to=$1 WHERE id=$2",
                rate_boundary,
                rate_a,
            )
            await conn.execute(
                rate_insert_sql, rate_b, rate_boundary, Decimal("0.2"), "v2"
            )
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await conn.execute(
                "UPDATE usage_rates_v2 SET usd_per_unit=0.3 WHERE id=$1",
                rate_b,
            )

        await conn.execute("INSERT INTO usage_rate_cards (id) VALUES ('card-a')")
        version_sql = """
            INSERT INTO usage_rate_card_versions_v2 (
                id, card_id, provider, target_service, target_region,
                currency, pricing_basis, calculator, aggregation_scope,
                shape_change_policy, provider_effective_from, observed_at,
                source_version, source_checksum, component_count,
                component_manifest_hash
            ) VALUES (
                $1, 'card-a', 'stackit', 'ske', 'eu01', 'EUR',
                'historical-public-list', 'linear_v1', 'lifecycle',
                'continue', $2, $2, $3, $4, 1, $5
            )
        """
        with pytest.raises(asyncpg.CheckViolationError):
            async with conn.transaction():
                await conn.execute(
                    version_sql,
                    uuid4(),
                    rate_start,
                    "incomplete",
                    "source-a",
                    "4" * 64,
                )

        version_id = uuid4()
        async with conn.transaction():
            await conn.execute(
                version_sql,
                version_id,
                rate_start,
                "complete",
                "source-b",
                "5" * 64,
            )
            await conn.execute(
                "INSERT INTO usage_rate_components_v2 "
                "(version_id, component, billing_unit, unit_size, unit_price) "
                "VALUES ($1, 'cpu', 'vcpu-hour', 1, 0.1)",
                version_id,
            )
        with pytest.raises(asyncpg.CheckViolationError):
            async with conn.transaction():
                await conn.execute(
                    "INSERT INTO usage_rate_components_v2 "
                    "(version_id, component, billing_unit, unit_size, "
                    "unit_price) VALUES ($1, 'memory', 'gib-hour', 1, 0.1)",
                    version_id,
                )
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await conn.execute(
                "UPDATE usage_rate_components_v2 SET unit_price=0.2 "
                "WHERE version_id=$1",
                version_id,
            )
    finally:
        await conn.close()
        admin = await asyncpg.connect(app_pg_dsn)
        try:
            await admin.execute(f'DROP DATABASE IF EXISTS "{dbname}" WITH (FORCE)')
        finally:
            await admin.close()


def test_sudo_thread_scope_chain_defers_every_table_scan_to_0181() -> None:
    """0178-0181 broaden sudo ownership without ever scanning under a lock.

    The split is not cosmetic: the FK and the CHECK land NOT VALID, the lookup
    index is built CONCURRENTLY in its own non-transactional file, and only the
    last file pays the validation scans. Collapsing any two of them back
    together reintroduces an ACCESS EXCLUSIVE scan on the sudo gate's table.
    """

    scope = APP_SUDO_REQUESTS_THREAD_SCOPE.read_text()
    check = APP_SUDO_REQUESTS_ENTITY_CHECK.read_text()
    index = APP_SUDO_REQUESTS_THREAD_INDEX.read_text()
    validate = APP_SUDO_REQUESTS_VALIDATE_CONSTRAINTS.read_text()

    chain = (
        (scope, "0178_sudo_requests_thread_scope.sql", None),
        (check, "0179_sudo_requests_entity_check.sql", scope),
        (index, "0180_sudo_requests_thread_idx.notx.sql", check),
        (validate, "0181_sudo_requests_validate_constraints.sql", index),
    )
    previous_name = "0177_managed_repository_thread_detach.sql"
    for raw, name, _ in chain:
        assert f"-- migration:     {name}" in raw
        assert f"-- depends-on:    {previous_name}" in raw
        assert "-- expected:" in raw
        assert "-- locks:" in raw
        previous_name = name

    for raw in (scope, check, validate):
        sql = _compact(raw)
        assert "-- transactional: yes" in raw
        assert "SET LOCAL lock_timeout = '2s'" in sql
        assert "SET LOCAL statement_timeout = '15min'" in sql
        assert "SET LOCAL idle_in_transaction_session_timeout = '5min'" in sql
        assert "SET LOCAL timezone = 'UTC'" in sql

    scope_sql = _compact(scope)
    assert "ALTER COLUMN job_id DROP NOT NULL" in scope_sql
    assert "ADD COLUMN thread_id uuid" in scope_sql
    assert "REFERENCES public.threads(id) ON DELETE CASCADE NOT VALID" in scope_sql
    assert "squawk-ignore ban-drop-not-null" in scope

    assert "CHECK (num_nonnulls(job_id, thread_id) = 1) NOT VALID" in _compact(check)

    index_body = _compact(
        "\n".join(
            line for line in index.splitlines() if not line.lstrip().startswith("--")
        )
    )
    assert "-- transactional: NO" in index
    assert "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sudo_requests_thread" in (
        index_body
    )
    assert "BEGIN;" not in index_body
    # The runner sends a .notx.sql file as one simple query, and Postgres wraps
    # a multi-statement simple query in an implicit transaction — which is
    # exactly what CREATE INDEX CONCURRENTLY cannot run inside.
    assert index_body.count(";") == 1

    validate_sql = _compact(validate)
    assert "VALIDATE CONSTRAINT sudo_approval_requests_thread_id_fkey" in validate_sql
    assert "VALIDATE CONSTRAINT sudo_approval_requests_one_entity" in validate_sql
    assert "NOT VALID" not in validate_sql
