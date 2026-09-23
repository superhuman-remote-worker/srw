"""FastAPI backend for the Debug Cockpit.

Run with:
    uvicorn orchestrator.main:app --reload --port 8085
"""

import asyncio
import functools
import json
import logging
import os
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

if os.environ.get("LICENSE_TERMS_ACCEPTED", "").strip().lower() != "true":
    raise SystemExit(
        "License terms not accepted. Set LICENSE_TERMS_ACCEPTED=true to run. "
        "See https://github.com/superhuman-remote-worker/srw/blob/main/LICENSE"
    )

# Configure application-level logging (Uvicorn only configures its own loggers).
# JSON when LOG_FORMAT=json (cluster), text otherwise (local/dev). When DEBUG,
# only app namespaces get DEBUG; third-party stays at INFO (DEBUG_ALL=1 to
# include it). See knowledge-base/knowledge/features/centralized_logging.md.
from orchestrator.logging_config import (  # noqa: E402
    CorrelationIdMiddleware,
    bind_log_context,
    configure_logging,
    reset_log_context,
)

configure_logging(
    component="orchestrator",
    app_namespaces=("orchestrator", "shared"),
    disable_uvicorn_access=True,
)

from datetime import date, datetime, timedelta, timezone  # noqa: E402
from decimal import Decimal  # noqa: E402
from collections.abc import Mapping  # noqa: E402
from typing import Any, Optional  # noqa: E402
from uuid import UUID  # noqa: E402

from fastapi import (  # noqa: E402
    FastAPI,
    HTTPException,
    Query,
    Request,
)
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from fastapi.responses import (  # noqa: E402
    JSONResponse,
)


from orchestrator.database import (  # noqa: E402
    PostgresDB,
    AuditStore,
    MIGRATIONS_VECTOR_DIR,
    MIGRATIONS_AUDIT_DIR,
)
from orchestrator.database.postgres import (  # noqa: E402
    KNOWN_JOB_ORIGINS,
    JOB_STATUS_FILTER_VALUES,
    DatasourcePolicyConflictError,
    _completion_control_active_sql,
    _completion_control_owned_active_sql,
)
from orchestrator.security.auth import (  # noqa: E402
    get_current_user,
    require_approved_user,
    cleanup_expired_tokens,
    cleanup_expired_sessions,
    ensure_user_provisioned,
    set_provisioning_backends,
)
from orchestrator.security.access import (  # noqa: E402
    externalize_gitea_url,
    vm_workspaces_on_pod_network,
    is_internal_call,
    log_security_event,
    require_admin as require_admin_gate,
    mcp_scope_project_id,
    redact_config_override,
    require_datasource_access,
    require_datasource_owner,
    require_internal,
    require_internal_or_job_access,
    require_job_access,
    require_personal_scope,
    require_project_member,
    require_project_owner,
    require_sudo_request_authority,
    require_thread_owner,
    user_can_access_any_job,
    user_can_access_job,
    user_can_access_ide_entity,
    user_can_access_job_or_thread,
    user_visible_project_ids,
)
from orchestrator.security.csrf import CSRFMiddleware  # noqa: E402
from shared.anti_framing import (  # noqa: E402
    TrustedParentAntiFramingMiddleware,
)
from orchestrator.auth import bff_router  # noqa: E402
from orchestrator.routers import automations_router  # noqa: E402
from orchestrator.routers import automations as automations_router_module  # noqa: E402
from orchestrator.routers import canvases_router, internal_canvases_router, wopi_router  # noqa: E402
from orchestrator.services.canvas_office import warm_collabora_discovery  # noqa: E402
from orchestrator.routers import project_loops_router  # noqa: E402
from orchestrator.routers import (  # noqa: E402
    project_loops as project_loops_router_module,
)
from orchestrator.routers import product_capabilities_router  # noqa: E402
from orchestrator.routers import shared_browser_router  # noqa: E402
from orchestrator.routers import vm_guest_router  # noqa: E402
from orchestrator.routers import sessions as sessions_routes  # noqa: E402
from orchestrator.routers.sessions import router as sessions_router  # noqa: E402
from orchestrator.routers.contacts import ContactsDependencies  # noqa: E402
from orchestrator.routers import job_reads as job_reads_routes  # noqa: E402
from orchestrator.services import job_projection, job_queries, job_reads  # noqa: E402
from orchestrator.routers import provider_catalog as provider_catalog_routes  # noqa: E402
from orchestrator.routers import model_catalog as model_catalog_routes  # noqa: E402
from orchestrator.routers import config_catalog as config_catalog_routes  # noqa: E402
from orchestrator.routers import manifests as manifest_routes  # noqa: E402
from orchestrator.services.provider_catalog import (  # noqa: E402
    ProviderCatalogService,
)
from orchestrator.services.model_catalog import ModelCatalogService  # noqa: E402
from orchestrator.services.config_catalog import ConfigCatalogService  # noqa: E402
from orchestrator.routers import diagnostics as diagnostics_routes  # noqa: E402
from orchestrator.routers import job_artifacts as job_artifacts_routes  # noqa: E402
from orchestrator.routers import job_audit as job_audit_routes  # noqa: E402
from orchestrator.routers import job_inspection as job_inspection_routes  # noqa: E402
from orchestrator.services import diagnostics as diagnostics_operations  # noqa: E402
from orchestrator.services import job_artifacts as job_artifacts_operations  # noqa: E402
from orchestrator.services import job_evidence as job_evidence_operations  # noqa: E402
from orchestrator.services import job_inspection as job_inspection_operations  # noqa: E402
from orchestrator.routers import access_tokens as access_token_routes  # noqa: E402
from orchestrator.routers import identity as identity_routes  # noqa: E402
from orchestrator.routers import ssh_access as ssh_access_routes  # noqa: E402
from orchestrator.services import access_tokens as access_token_operations  # noqa: E402
from orchestrator.services import ssh_access as ssh_access_operations  # noqa: E402
from orchestrator.routers import infrastructure_admin as infrastructure_admin_routes  # noqa: E402
from orchestrator.routers import usage_reporting as usage_reporting_routes  # noqa: E402
from orchestrator.services import (  # noqa: E402
    infrastructure_admin as infrastructure_admin_operations,
)
from orchestrator.services import usage_reporting as usage_reporting_operations  # noqa: E402
from orchestrator.routers import (  # noqa: E402
    provider_credentials as provider_credentials_routes,
)
from orchestrator.routers import (  # noqa: E402
    subscription_management as subscription_management_routes,
)
from orchestrator.routers import voice as voice_routes  # noqa: E402
from orchestrator.routers import system_settings as system_settings_routes  # noqa: E402
from orchestrator.routers import (  # noqa: E402
    vm_workspace_cleanup_authority as vm_workspace_cleanup_authority_routes,
    vm_creation_retry_authority as vm_creation_retry_authority_routes,
    vm_resource_inventory as vm_resource_inventory_routes,
)
from orchestrator.routers import capacity as capacity_routes  # noqa: E402
from orchestrator.routers import (  # noqa: E402
    user_administration as user_administration_routes,
)
from orchestrator.routers import job_diagnostics as job_diagnostics_routes  # noqa: E402

# R1.B03 — projects, datasources, knowledge, citations and the media proxy.
from orchestrator.routers import datasources as datasources_routes  # noqa: E402
from orchestrator.routers import projects as projects_routes  # noqa: E402
from orchestrator.routers import knowledge as knowledge_routes  # noqa: E402
from orchestrator.routers import citations as citations_routes  # noqa: E402
from orchestrator.routers import media as media_routes  # noqa: E402

# R1.B04 — job/thread workspace access, files and the IDE/browser proxy.
from orchestrator.routers import ide as ide_routes  # noqa: E402
from orchestrator.routers import (  # noqa: E402
    workspace_access as workspace_access_routes,
)
from orchestrator.routers import thread_files as thread_files_routes  # noqa: E402
from orchestrator.services import (  # noqa: E402
    workspace_access as workspace_access_operations,
)
from orchestrator.routers import job_repo as job_repo_routes  # noqa: E402
from orchestrator.routers import job_diff as job_diff_routes  # noqa: E402
from orchestrator.routers import job_review as job_review_routes  # noqa: E402
from orchestrator.routers import (  # noqa: E402
    agent_cloud_stage as agent_cloud_stage_routes,
    agent_thread_workspace as agent_thread_workspace_routes,
    job_assignment,
    job_assignment as job_assignment_routes,
)
from orchestrator.routers import (  # noqa: E402
    thread_cloud_diff as thread_cloud_diff_routes,
)
from orchestrator.routers import (  # noqa: E402
    agent_thread_status as agent_thread_status_routes,
    run_queue_admin as run_queue_admin_routes,
    unit_claim as unit_claim_routes,
)
from orchestrator.services import (  # noqa: E402
    agent_thread_status as agent_thread_status_service,
    run_queue_admin as run_queue_admin_service,
    unit_claim_bundle as unit_claim_bundle_service,
)
from orchestrator.services.stateless_claimant_attestation import (  # noqa: E402
    build_claimant_attestor as _build_stateless_claimant_attestor,
)
from orchestrator.services.vm_workspace_recovery_store import (  # noqa: E402
    VMWorkspaceRecoveryStore,
)
from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore  # noqa: E402
from orchestrator.services.vm_creation_retry import VMCreationRetryService  # noqa: E402
from orchestrator.services.vm_workspace_recovery import (  # noqa: E402
    VMWorkspaceRecoveryService,
)
from orchestrator.services.vm_workspace_recovery_config import (  # noqa: E402
    VMWorkspaceRecoverySettings,
    automatic_reconciler_enabled,
)
from orchestrator.services import (  # noqa: E402
    commissioned_officer_provisioning as commissioned_officer_provisioning_service,
    pinned_session_mutation_target as pinned_session_mutation_target_service,
    provision_or_assign as provision_or_assign_service,
    session_attach_binding as session_attach_binding_service,
    session_attach_recovery as session_attach_recovery_service,
)
from orchestrator.services.session_attach_binding import (  # noqa: E402
    SessionAttachReleaseOutcome,
    WarmBindingReservationPending as _WarmBindingReservationPending,  # noqa: F401
)
from orchestrator.services.pinned_session_mutation_target import (  # noqa: E402
    PinnedSessionMutationTarget as _PinnedSessionMutationTarget,  # noqa: F401
    agent_process_generation as _agent_process_generation,  # noqa: F401
    local_pinned_session_target_matches as _local_pinned_session_target_matches,  # noqa: F401
)
from orchestrator.services.session_attach_recovery import (  # noqa: E402
    current_attach_abort_successor as _current_attach_abort_successor,  # noqa: F401
)
from orchestrator.services.session_runtime_identity import (  # noqa: E402
    expected_agent_shas as _expected_agent_shas,  # noqa: F401
    thread_accepts_runtime as _thread_accepts_runtime,
    thread_uses_pinned_execution as _thread_uses_pinned_execution,
)
from orchestrator.routers import (  # noqa: E402
    thread_admission as thread_admission_routes,
    thread_config as thread_config_routes,
)
from orchestrator.routers import (  # noqa: E402
    agent_child_threads as agent_child_threads_routes,
    agent_registration as agent_registration_routes,
    officer_runtime_verification as officer_runtime_verification_routes,
)
from orchestrator.services import (  # noqa: E402
    agent_child_threads as agent_child_threads_service,
    agent_registration as agent_registration_service,
    officer_runtime_verification as officer_runtime_verification_service,
)

# R1.B07 — message routing, notifications/actions, Officer post and loop
# operations. Lane M (messaging) first; the schemas are re-imported here
# because production and test consumers still name them through ``main``.
from orchestrator.routers import (  # noqa: E402
    actions as actions_routes,
    messaging as messaging_routes,
)
from orchestrator.services import (  # noqa: E402
    agent_messaging as agent_messaging_service,
    inbound_reply as inbound_reply_service,
    job_freeze_notifications as job_freeze_notification_service,
    job_guidance as job_guidance_service,
    message_thread_reads as message_thread_read_service,
    officer_message_actions as officer_message_action_service,
    pending_actions as pending_actions_service,
)
from orchestrator.schemas.messaging import (  # noqa: E402,F401
    GuidanceAckRequest,
    MessageReplyRequest,
    MessageSendRequest,
    OfficerMessageAckRequest,
    OfficerMessageEscalateRequest,
    OfficerMessageReplyRequest,
)

# R1.B07 lane O — the Officer Post: conference, roster/card, lifecycle, paging
# and the watchdog.
from orchestrator.routers import (  # noqa: E402
    agent_officer as agent_officer_routes,
    officers as officer_routes,
)
from orchestrator.services import (  # noqa: E402
    officer_conference as officer_conference_service,
    officer_paging as officer_paging_service,
    officer_post_lifecycle as officer_post_lifecycle_service,
    officer_post_policy as officer_post_policy_service,
    officer_post_views as officer_post_view_service,
    officer_watchdog as officer_watchdog_service,
)
from orchestrator.schemas.officer_post import (  # noqa: E402,F401
    OfficerDecommissionRequest,
    OfficerHoldRequest,
    OfficerNoteRequest,
    OfficerNotifyRequest,
    OfficerWakeRequest,
)
from orchestrator.services.officer_post_policy import (  # noqa: E402,F401
    OFFICER_CONFIG_NAME,
    OFFICER_NOTE_MAX_CHARS,
    OFFICER_PERMISSION_MODE,
)

# R1.B07 lane N — the unified notification feed and its action registry.
from orchestrator.routers import notifications as notification_routes  # noqa: E402
from orchestrator.services import (  # noqa: E402
    notification_actions as notification_action_service,
    notification_api as notification_api_service,
)
from orchestrator.schemas.notifications import (  # noqa: E402,F401
    NotificationActRequest,
    NotificationSeenRequest,
)

# R1.B07 lane L — the project-loop engine: spawn, advance, campaign, handoff.
from orchestrator.routers import loop_plan as loop_plan_routes  # noqa: E402
from orchestrator.services import sitrep as sitrep_service  # noqa: E402
from orchestrator.services import (  # noqa: E402
    curation_final_pass as curation_final_pass_service,
    loop_plan_filing as loop_plan_filing_service,
    project_loop_advance as project_loop_advance_service,
    project_loop_spawn as project_loop_spawn_service,
)
from orchestrator.schemas.project_loops import LoopPlanRequest  # noqa: E402,F401

# R1.B08 — completion composition, verification decisions, subjob output and
# recovery policy.  Main owns only application wiring and B11-owned task
# lifecycle; policy and effect ordering live in these domain modules.
from orchestrator.routers import job_completion as job_completion_routes  # noqa: E402
from orchestrator.routers import job_controls as job_control_routes  # noqa: E402
from orchestrator.routers import job_lifecycle as job_lifecycle_routes  # noqa: E402
from orchestrator.routers import thread_lifecycle as thread_lifecycle_routes  # noqa: E402
from orchestrator.routers import thread_rewind as thread_rewind_routes  # noqa: E402
from orchestrator.routers import verification as verification_routes  # noqa: E402

# R1.B10 — session projections, history, transport, permissions and the
# attention/wake bodies. Main composes their dependencies and keeps route
# positions, task creation and leader gating (B11).
from orchestrator.routers import thread_history as thread_history_routes  # noqa: E402
from orchestrator.routers import thread_permissions as thread_permission_routes  # noqa: E402
from orchestrator.routers import thread_session as thread_session_routes  # noqa: E402
from orchestrator.routers import thread_transport as thread_transport_routes  # noqa: E402
from orchestrator.services import (  # noqa: E402
    pinned_forwarding as pinned_forwarding_operations,
    session_attention as session_attention_operations,
    session_tool_view as session_tool_view_operations,
    stateless_input_admission as stateless_input_operations,
    thread_permissions as thread_permission_operations,
    thread_projection as thread_projection_operations,
)
from orchestrator.services.thread_turn_locks import ThreadTurnLocks  # noqa: E402
from orchestrator.services import (  # noqa: E402
    completion_effects as completion_effect_operations,
    completion_recovery as completion_recovery_operations,
    job_completion as job_completion_operations,
    job_controls as job_control_operations,
    job_control_delivery as job_delivery_operations,
    job_mutation_controls as job_mutation_operations,
    legacy_job_completion as legacy_job_completion_operations,
    subjob_completion as subjob_completion_operations,
    subjob_output as subjob_output_operations,
    verification_workflow as verification_operations,
)
from orchestrator.services import (  # noqa: E402
    thread_resume as thread_resume_operations,
    thread_rewind as thread_rewind_operations,
    thread_retirement as thread_retirement_operations,
)
from orchestrator.schemas.job_controls import (  # noqa: E402,F401
    JobApproveRequest,
    JobResumeRequest,
    SudoApproveRequest,
    SudoDenyRequest,
    SudoRuleCreateRequest,
)
from orchestrator.schemas.thread_lifecycle import (  # noqa: E402,F401
    ThreadResumeRequest,
    ThreadRewindRequest,
)
from orchestrator.services.completion_runtime import (  # noqa: E402
    CompletionAlertDependencies,
    CompletionAlerts,
    CompletionControlBoundary,
    CompletionRuntime,
    CompletionRuntimeDependencies,
)
from orchestrator.services.completion_session_memory import (  # noqa: E402
    SessionMemoryDependencies,
    SessionMemoryRuntime,
)

# Re-exported for suites that still resolve these request models on main.
from orchestrator.schemas.agent_child_threads import (  # noqa: E402,F401
    AgentSessionSubagentByCallRequest,
    AgentSessionSubagentCreateRequest,
    AgentSessionSubagentQueryRequest,
    AgentSessionSubagentReopenRequest,
    AgentSessionSubagentTerminalRequest,
    AgentSubagentThreadCreateRequest,
    AgentSubagentThreadQueryRequest,
    AgentSubagentThreadReopenRequest,
    AgentSubagentThreadTerminalRequest,
    AgentThreadCreateRequest,
    AgentThreadMessageRequest,
)
from orchestrator.schemas.agent_registration import (  # noqa: E402,F401
    PodRuntimeActorRequest,
)

# R1.B06 compatibility surface. Pure re-exports, not wrappers: unchanged types
# and dependency-free helpers whose last *main* caller left with their batch,
# but which callers and suites still resolve on ``orchestrator.main``. B05's
# rule applies — an alias is enough where an operation needs no dependency
# object, and an alias cannot drift from its target the way a wrapper can.
from orchestrator.schemas.agent_thread_status import (  # noqa: E402,F401
    AgentThreadStatusRequest,
)
from orchestrator.services.job_datasource_selection import (  # noqa: E402,F401
    require_exact_datasource_resolution as _require_exact_datasource_resolution,
)
from orchestrator.services.job_workspace_runtime import (  # noqa: E402,F401
    inject_container_workspace_config as _inject_container_workspace_config,
    inject_vm_workspace_config as _inject_vm_workspace_config,
    stateless_worker_workspace_owner as _stateless_worker_workspace_owner,
)
from orchestrator.services.session_create_overrides import (  # noqa: E402,F401
    effective_officer_post_owned_refusal as _effective_officer_post_owned_refusal,
)
from orchestrator.services.session_workspace_policy import (  # noqa: E402,F401
    validated_session_workspace_override as _validated_session_workspace_override,
)
from shared.session_subagent_authority import (  # noqa: E402,F401
    SessionParentAuthorityRefused,
)
from shared.subagent_parent_authority import (  # noqa: E402,F401
    ParentExecutionAuthority,
    ParentExecutionAuthorityRefused,
)
from orchestrator.schemas.officer_runtime_verification import (  # noqa: E402,F401
    OfficerRuntimeVerificationPlanRequest,
    RuntimeActorAuthorizationRequest,
)
from orchestrator.services.agent_child_threads import (  # noqa: E402,F401
    session_subagent_authority_wire as _session_subagent_authority_wire,
)
from orchestrator.services.officer_runtime_verification import (  # noqa: E402,F401
    runtime_verification_http_error as _runtime_verification_http_error,
)
from orchestrator.services import (  # noqa: E402
    thread_admission as thread_admission_service,
    thread_config_update as thread_config_update_service,
    thread_datasource_authorization as thread_datasource_authorization_service,
    thread_project_authorization as thread_project_authorization_service,
)

# Re-exported for callers and suites that still resolve these names on main.
from orchestrator.schemas.thread_admission import (  # noqa: E402,F401
    ThreadCreateRequest,
    ThreadUpdateRequest,
    TrustedThreadSeed,
)
from orchestrator.schemas.thread_config import (  # noqa: E402,F401
    AgentThreadConfigUpdateRequest,
    ThreadConfigPatchRequest,
    ThreadWorkspaceUpgradeRequest,
)
from orchestrator.services.thread_project_authorization import (  # noqa: E402,F401
    thread_creation_project_ids as _thread_creation_project_ids,
)
from orchestrator.services.thread_config_update import (  # noqa: E402,F401
    config_change_summary as _config_change_summary,
    protected_cloud_mutation_marker as _protected_cloud_mutation_marker,
    require_unprotected_workspace_upgrade as _require_unprotected_workspace_upgrade,
)
from orchestrator.routers import (  # noqa: E402
    main_cloud_settings as main_cloud_settings_routes,
)
from orchestrator.services import job_repo_reads  # noqa: E402
from orchestrator.services import job_diff_review  # noqa: E402
from orchestrator.services import job_export  # noqa: E402
from orchestrator.services import job_review_session  # noqa: E402
from orchestrator.services import (  # noqa: E402
    thread_cloud_diff as thread_cloud_diff_operations,
)
from orchestrator.services import (  # noqa: E402
    main_cloud_settings as main_cloud_settings_operations,
)
from orchestrator.services import agent_cloud_mounts  # noqa: E402
from orchestrator.services import datasource_config  # noqa: E402
from orchestrator.services import session_create_overrides  # noqa: E402

# Retained re-export: five suites construct `main.JobStartRequest` directly.
# The schema's owner is `orchestrator.schemas.job_runtime`; this line exists
# only so those callers keep resolving, and goes when they are re-pointed.
from orchestrator.schemas.job_runtime import JobStartRequest  # noqa: E402,F401

# Retained re-export, pinned by `test_preferences_dependency_isolation`: the
# tier constants' owner is `session_workspace_policy`, and main re-exports
# them so the settings surfaces resolve one set of values.
from orchestrator.services.session_workspace_policy import (  # noqa: E402,F401
    SESSION_CREATE_WORKSPACE_BACKENDS,
    SESSION_DEFAULT_WORKSPACE_BACKEND,
    SESSION_WORKSPACE_BACKENDS,
)
from orchestrator.services import agent_datasource_payload  # noqa: E402

# ---------------------------------------------------------------------------
# R1.B05 retained aliases: names this module no longer uses itself, but that
# compatibility suites still resolve through `orchestrator.main`.
# They are plain aliases — pure symbols with no application dependency (§P4) —
# and each one goes when the batch that owns its callers re-points them.
# ---------------------------------------------------------------------------
from orchestrator.services.agent_toolset_probe import (  # noqa: E402,F401
    AGENT_TOOLSET_BUDGET_S as _AGENT_TOOLSET_BUDGET_S,
    Measurement as _Measurement,
)
from orchestrator.services.dispatch_credentials import (  # noqa: E402,F401
    provider_of_model as _provider_of_model,
)
from orchestrator.services.grant_enforcement import (  # noqa: E402,F401
    strip_acknowledged_grants as _strip_acknowledged_grants,
)
from orchestrator.services.job_create_ingress import (  # noqa: E402,F401
    PUBLIC_JOB_CONFIG_RESERVED_KEYS as _PUBLIC_JOB_CONFIG_RESERVED_KEYS,
    PUBLIC_JOB_CONTEXT_RESERVED_KEYS as _PUBLIC_JOB_CONTEXT_RESERVED_KEYS,
)
from orchestrator.services.job_start_bundle import (  # noqa: E402,F401
    mask_repository_transport as _mask_repository_transport,
)
from orchestrator.services.job_workspace_authority import (  # noqa: E402,F401
    INHERIT_WORKSPACE_MAX_WAIT_S as _INHERIT_WORKSPACE_MAX_WAIT_S,
    PinnedK8sJobWorkspaceAuthority as _PinnedK8sJobWorkspaceAuthority,
)
from orchestrator.services.job_workspace_runtime import (  # noqa: E402,F401
    container_ssh_key_path as _container_ssh_key_path,
)
from orchestrator.services.session_class_policy import (  # noqa: E402,F401
    protected_cloud_officer_active as _protected_cloud_officer_active,
)
from orchestrator.services.session_tool_policy import (  # noqa: E402,F401
    SESSION_TOOL_DISABLED_MARKERS as _SESSION_TOOL_DISABLED_MARKERS,
    agent_catalog_explicitly_disabled as _agent_catalog_explicitly_disabled,
    fleet_management_explicitly_disabled as _fleet_management_explicitly_disabled,
    legacy_session_tool_groups as _legacy_session_tool_groups,
    merged_session_tool_groups as _merged_session_tool_groups,
    session_tool_group_disabled_markers as _session_tool_group_disabled_markers,
    validated_session_fleet_tools_override as _validated_session_fleet_tools_override,
    workflows_explicitly_disabled as _workflows_explicitly_disabled,
)
from orchestrator.services.session_workspace_policy import (  # noqa: E402,F401
    default_session_workspace_backend as _default_session_workspace_backend,
)
from orchestrator.services.virtual_workspace import (  # noqa: E402,F401
    object_store_startup_warning as _object_store_startup_warning,
)
from orchestrator.services import audit_usage  # noqa: E402
from orchestrator.services import agent_toolset_probe  # noqa: E402
from orchestrator.services import dispatch_credentials  # noqa: E402
from orchestrator.services import grant_enforcement  # noqa: E402
from orchestrator.services import job_datasource_selection  # noqa: E402
from orchestrator.services import job_dispatch_credentials  # noqa: E402
from orchestrator.services import job_start_bundle  # noqa: E402
from orchestrator.services import job_workspace_authority  # noqa: E402
from orchestrator.services import job_workspace_runtime  # noqa: E402
from orchestrator.services import protected_cloud_engage  # noqa: E402
from orchestrator.services import session_class_policy  # noqa: E402
from orchestrator.services import session_config_resolution  # noqa: E402
from orchestrator.services import vm_workspace_policy  # noqa: E402
from orchestrator.services import session_attach_payload  # noqa: E402
from orchestrator.services import stateless_workspace_scheduler  # noqa: E402
from orchestrator.services import thread_mount_rows  # noqa: E402
from orchestrator.services import thread_workspace_delivery  # noqa: E402
from orchestrator.services.cloud_task_registry import (  # noqa: E402
    CloudTaskRegistry,
)

# Called by code main still owns (B05 payloads and B06 attach/create/resume).
# Imported so ``main.<name>`` stays the same module attribute the
# call sites and their tests resolve; the thin wrappers below keep the
# pre-extraction signatures and supply the dependencies.
from orchestrator.services import datasources as datasources_operations  # noqa: E402
from orchestrator.services import projects as projects_operations  # noqa: E402
from orchestrator.services import (  # noqa: E402
    project_provisioning as project_provisioning_operations,
)
from orchestrator.services import (  # noqa: E402
    knowledge_operations as knowledge_operations_module,
)
from orchestrator.services import (  # noqa: E402
    knowledge_projection as knowledge_projection_operations,
)
from orchestrator.services import citations as citations_operations  # noqa: E402
from orchestrator.services import knowledge_index as knowledge_index_operations  # noqa: E402
from orchestrator.services.kb_task_registry import (  # noqa: E402
    KbDatasourceTaskRegistry,
)
from orchestrator.services.knowledge_projection import (  # noqa: E402
    KnowledgeGraphHandle,
)
from orchestrator.services import (  # noqa: E402
    provider_credentials as provider_credentials_operations,
)
from orchestrator.services import (  # noqa: E402
    subscription_management as subscription_management_operations,
)
from orchestrator.services import voice as voice_operations  # noqa: E402
from orchestrator.services import (  # noqa: E402
    system_settings as system_settings_operations,
)
from orchestrator.services import (  # noqa: E402
    user_administration as user_administration_operations,
)
from orchestrator.services import job_diagnostics as job_diagnostics_operations  # noqa: E402
from orchestrator.services.infrastructure_activation_policy import (  # noqa: E402
    workspace_metering_attribution,
)
from orchestrator.routers import expert_catalog as expert_catalog_routes  # noqa: E402
from orchestrator.services.expert_catalog import (  # noqa: E402
    ExpertCatalogService,
    role_base_or_empty as _role_base_or_empty,
)
from orchestrator.services.expert_authoring import ExpertAuthoringService  # noqa: E402
from orchestrator.services.expert_catalog_contracts import (  # noqa: E402
    ExpertCatalogDependencies,
    ExpertCatalogState,
    ExpertWritePolicy,
)
from orchestrator.services.catalogue_resources import (  # noqa: E402
    CatalogueResources,
    resolve_config_dir,
)
from orchestrator.schemas.agent_runtime import (  # noqa: E402
    AgentHeartbeat,  # noqa: F401
    AgentRegistration,  # noqa: F401
)
from orchestrator.schemas.job_runtime import (  # noqa: E402
    JobCompleteRequest,  # noqa: F401 - temporary public schema re-export
)
from orchestrator.services.job_queries import (  # noqa: E402
    JOBS_MAX_OFFSET as JOBS_MAX_OFFSET,
    JOBS_MAX_PROJECT_FILTERS as JOBS_MAX_PROJECT_FILTERS,
    JobProjectFilters as _JobProjectFilters,  # noqa: F401 - compatibility export
    parse_job_project_filters as _parse_job_project_filters,  # noqa: F401 - compatibility export
)
from orchestrator.routers.preferences import (  # noqa: E402
    PreferencesDependencies,
    UserSettingsUpdate as UserSettingsUpdate,
    router as preferences_router,
)
from orchestrator.services.preference_defaults import (  # noqa: E402
    resolve_preference_defaults as _resolve_app_preference_defaults,
)
from orchestrator.routers.tables import TablesDependencies  # noqa: E402
from orchestrator.routers.tables import router as tables_router  # noqa: E402
from orchestrator.routers.contacts import project_router as contacts_project_router  # noqa: E402
from orchestrator.routers.contacts import router as contacts_router  # noqa: E402
from orchestrator.services.cron_dispatcher import cron_dispatcher_loop  # noqa: E402
from orchestrator.services.project_loop_sweeper import project_loop_sweeper_loop  # noqa: E402
from orchestrator.services.session_wake import (  # noqa: E402
    bind_officer_wake_metering,
    deliver_officer_note as _deliver_officer_note,
    kick_drain as _kick_session_wake_drain,
    kick_event_drain as _kick_officer_event_drain,
    maybe_wake_session,
    notify_all_officers,
    notify_officer,
    session_wake_sweeper_loop,
)
from shared.pinned_session_identity import PinnedSessionBinding  # noqa: E402
from shared.pinned_session_identity import PinnedJobRecipient  # noqa: E402, F401
from orchestrator.services.stateless_workspace_gate import (  # noqa: E402
    thread_metadata_object,
)
from orchestrator.services.stale_verification_sweeper import (  # noqa: E402
    stale_verification_sweeper_loop,
)
from orchestrator.services.cloud_pricing import (  # noqa: E402
    CloudCostEstimator,
    cloud_pricing_sync_loop,
)
from orchestrator.services.infrastructure_metering import (  # noqa: E402
    CoverageGapWaiverService,
    InfrastructureMeteringSettings,
    InfrastructureMeteringRuntime,
    InfrastructureUsageDaySealer,
    InfrastructureUsageMaterializer,
    InfrastructureWorkspaceCutover,
    TypedUsageDailyRollup,
    UsageV2QueryService,
    infrastructure_metering_runtime_loop,
    typed_usage_rollup_loop,
)
from orchestrator.services.startup_backfills import run_startup_backfills  # noqa: E402
from orchestrator.services import job_dispatcher  # noqa: E402
from orchestrator.services import retention_sweepers  # noqa: E402
from orchestrator.services.agent_provisioner import agent_pool_reconciler  # noqa: E402
from orchestrator.services.ide_session import ide_session_ttl_sweeper  # noqa: E402
from orchestrator.services.ide_settings import code_server_settings_sweeper  # noqa: E402
from orchestrator.services.imap_poller import imap_poll_loop  # noqa: E402
from orchestrator.services.lifecycle.reconciler import lifecycle_reconciler_loop  # noqa: E402
from orchestrator.services.ro_reader_reconciler import ro_reader_reconciler_loop  # noqa: E402
from orchestrator.services.session_provisioner import workspace_idle_sweeper  # noqa: E402
from orchestrator.services.snapshot_service import snapshot_gc_sweeper  # noqa: E402
from orchestrator.services.sudo_gate import sudo_expiration_sweeper  # noqa: E402
from orchestrator.services import (  # noqa: E402
    pinned_k8s_reconciliation as pinned_k8s_reconciliation_service,
)
from orchestrator.services import stale_agent_detector as stale_agent_detector_service  # noqa: E402
from orchestrator.services.infrastructure_metering.bootstrap import (  # noqa: E402
    bootstrap_infrastructure_metering,
)
from orchestrator.services.infrastructure_metering.compute_activation import (  # noqa: E402
    ComputeActivationStore,
)
from orchestrator.services.infrastructure_metering.ingestion import (  # noqa: E402
    InfrastructureIngestionService,
    run_inventory_generation_loop,
)
from orchestrator.services.infrastructure_metering.inventory import InventoryStore  # noqa: E402
from orchestrator.services.infrastructure_metering.storage_assets import (  # noqa: E402
    StorageAssetStore,
)
from orchestrator.services.infrastructure_metering.storage_mapping import (  # noqa: E402
    StorageResourceMappingStore,
)
from orchestrator.services.openrouter_pricing import llm_pricing_sync_loop  # noqa: E402
from orchestrator.services.audit_partitions import (  # noqa: E402
    maintenance_loop as audit_maintenance_loop,
)
from orchestrator.services import workspace_metering  # noqa: E402
from orchestrator.services.usage_ledger import (  # noqa: E402
    UsageLedger,
    UsageRates,
)
from orchestrator.services.usage_rollup import UsageRollup, usage_rollup_loop  # noqa: E402
from orchestrator.services.virtual_workspace import (  # noqa: E402
    # R1.B05 lane P: main's `_virtual_workspace_rclone_spec` was already a
    # bare pass-through to this import, so the wrapper is gone and the
    # alias carries its name. Consumers reach the spec through the module,
    # which keeps one patch point.
    virtual_workspace_rclone_spec as _virtual_workspace_rclone_spec,
)
from orchestrator.services.workspace import workspace_service  # noqa: E402
from orchestrator.services.gitea import (  # noqa: E402
    GiteaClient,
    GiteaPathError,
)
from orchestrator.services.managed_repository_authority import (  # noqa: E402
    authorize_job_repository_transport,
    prepare_job_primary_repository_authority,
    prepare_project_repository_authority,
    prepare_thread_repository_authority,
    revoke_and_delete_managed_repository,
)
from orchestrator.services.keycloak_admin import KeycloakGroupSync  # noqa: E402
from orchestrator.services.cloud import (  # noqa: E402
    MainCloudRouter,
    UserId,
    build_backend,
)
from orchestrator.services.cloud.identity import (  # noqa: E402
    resolve_user_identity_cached,
)
from orchestrator.services.cloud.reload import (  # noqa: E402
    _reload_from_db_and_swap,
    run_listen_loop,
)
from orchestrator.services.cloud.instance_registry import (  # noqa: E402
    initialize_main_cloud_instance_authority,
    preload_retained_main_cloud_instances,
)
from orchestrator.services.llm_endpoint_probe import probe_endpoint_models  # noqa: E402
from orchestrator.services import discovery as discovery_service  # noqa: E402
from orchestrator.services import family_matcher  # noqa: E402
from orchestrator.services import readiness as readiness_service  # noqa: E402
from orchestrator.seed.llm_config import (  # noqa: E402
    ensure_subscription_proxy_endpoint,
)
from orchestrator.services import subscription_discovery  # noqa: E402

# Registry helpers live in src/ and stay there — the orchestrator imports
# them here so callers don't each do lazy imports.
from shared.runtime.core.model_registry import (  # noqa: E402
    UnknownModelError,
    resolve_model as _resolve_model,
)

# Lite (no-workspace-pod) backend names. Canonical set lives agent-side in the
# backend factory; imported (not re-declared) so the dispatch/provisioning
# branches here can't drift from what the agent actually constructs.
# (no_workspace_agent_mode.md §4) — importing the frozenset is cheap; the heavy
# backend modules are lazy-imported inside the factory's functions.
from shared.workspace_contract import (  # noqa: E402
    WORKSPACE_CONTRACT_CONTEXT_KEY as WORKSPACE_CONTRACT_CONTEXT_KEY,
    WORKSPACE_DISPATCH_AUTHORITY_CONTEXT_KEY as WORKSPACE_DISPATCH_AUTHORITY_CONTEXT_KEY,
    resolve_workspace_contract,
)

# Datasource type → tool-category map, shared with the agent's session attach
# path so the two boundaries can't drift (live_session_settings.md P0.2).

# Tool -> category, for annotating replayed history (_stamp_tool_categories).
# Same registry the agent's live SSE frames read, so the two can't disagree.
from orchestrator.services.nats_bridge import nats_bridge  # noqa: E402
from orchestrator.services.vm_provisioner import vm_provisioner  # noqa: E402
from orchestrator.services.vm_readiness import vm_readiness_prober  # noqa: E402
from orchestrator.services.container_provisioner import (  # noqa: E402
    WORKSPACE_RUNTIME_INCARNATION_KEY,
    WorkspaceRuntimeAttestation,  # noqa: F401
    WorkspaceTeardownIdentity,  # noqa: F401 - shared teardown identity re-export
    container_provisioner,
)
from orchestrator.services.workspace_lifecycle import (  # noqa: E402
    ensure_workspace,
)
from orchestrator.services.session_provisioner import (  # noqa: E402
    ensure_session_workspace,
)
from orchestrator.services.docker_provisioner import docker_provisioner  # noqa: E402
from orchestrator.services.persistent_provisioner import persistent_provisioner  # noqa: E402
from orchestrator.services.persistent_recycler import (  # noqa: E402
    PersistentThreadRecycler,
)
from orchestrator.services.pinned_agent_authority import (  # noqa: E402
    release_pinned_warm_binding_protection,
    reserve_pinned_warm_agent_binding,
)
from orchestrator.services.pinned_retirement import (  # noqa: E402
    PinnedRetirementDependencies,
    PinnedRetirementOperations,
)
from orchestrator.services import resolve_ssh_key_path  # noqa: E402
from orchestrator.services.agent_provisioner import agent_provisioner  # noqa: E402
from orchestrator.services.runtime_actor import (  # noqa: E402
    authorize_runtime_actor_request,
    mint_thread_runtime_actor,
    mint_worker_runtime_actor,
    refresh_runtime_actor_exchange,
    slide_thread_grant_on_liveness,
)
from orchestrator.services.runtime_actor_verification import (  # noqa: E402
    RuntimeVerificationPlanError,  # noqa: F401
    create_plan as create_runtime_verification_plan,
    get_plan as get_runtime_verification_plan,
    transition_plan as transition_runtime_verification_plan,
)
from orchestrator.services.config_resolver import (  # noqa: E402
    inject_blob_credentials,
    resolve_config,
)
from orchestrator.services.default_experts import (  # noqa: E402
    resolve_root_expert,
    seed_managed_default_experts,
)
from shared.runtime.core.loader import (  # noqa: E402
    canonical_config_name,
)
from orchestrator.services.session_router import SessionRouterService  # noqa: E402
from orchestrator.services.session_tokens import SessionTokenService  # noqa: E402
from orchestrator.services.lifecycle import (  # noqa: E402
    AgentInstanceManager,
    InstanceLifecycleReconciler,
    PersistentAgentInstanceManager,
    VMInstanceManager,
    WorkspaceInstanceManager,
)
from orchestrator.services.workspace_suspension import (  # noqa: E402
    _thread_is_vm_tier,
    workspace_suspension_service,
)
from orchestrator.services.snapshot_service import snapshot_service  # noqa: E402
from orchestrator.services.ide_session import ide_session_service  # noqa: E402
from orchestrator.services.ide_proxy import (  # noqa: E402
    ide_proxy_service,
)
from orchestrator.services.email import email_service  # noqa: E402
from orchestrator.services.imap_poller import imap_poller  # noqa: E402
from orchestrator.services.notification_service import (  # noqa: E402
    notification_service,
)
from orchestrator.services.notification_steps import notification_steps_loop  # noqa: E402
import httpx  # noqa: E402
from orchestrator.graph_routes import (  # noqa: E402
    router as graph_router,
    set_audit_reader,
    set_postgres_db,
)  # noqa: E402
from orchestrator.uploads import authorize_upload_reference, router as uploads_router  # noqa: E402

logger = logging.getLogger(__name__)

# =============================================================================
# Database Instances (singleton pattern)
# =============================================================================

postgres_db = PostgresDB()
_persistent_thread_recycler: PersistentThreadRecycler | None = None
gitea_client = GiteaClient()
keycloak_groups = KeycloakGroupSync()
main_cloud_router = MainCloudRouter(build_backend())

# Bind the optional background provisioning backends used by the JIT/approval
# paths in ``security.auth``. Those run as detached tasks with no request
# scope, so they cannot take these through a request dependency; two named
# callables keep the direction of dependency pointing away from this module
# (R1.B02 caller-boundary closure). Resolved per call, so a later rebind of
# either singleton is visible.
set_provisioning_backends(
    cloud_router=lambda: main_cloud_router,
    forge=lambda: gitea_client,
)

# Vector DB — separate pgvector instance for citations, memories + knowledge_index.
from shared.db_url import build_postgres_url as _build_pg_url  # noqa: E402

_vector_url = _build_pg_url("VECTOR_POSTGRES", fallback_env="VECTOR_DB_URL")
if not _vector_url:
    raise RuntimeError(
        "Vector DB credentials missing — set VECTOR_POSTGRES_USER + "
        "VECTOR_POSTGRES_PASSWORD (with VECTOR_POSTGRES_HOST/PORT/DB from "
        "ConfigMap), or fall back to VECTOR_DB_URL"
    )
vector_db = PostgresDB(
    connection_string=_vector_url,
    migrations_dir=MIGRATIONS_VECTOR_DIR,
    env_prefix="VECTOR_POSTGRES",
    default_min_connections=1,
    default_max_connections=5,
)

# Audit DB — observability-tier instance holding the audit trail
# (llm_requests / agent_audit / chat_history). Unlike the vector DB this is
# NON-load-bearing: when its credentials are absent (AUDIT_POSTGRES_* unset /
# databases.audit.enabled=false) the orchestrator runs without it — no
# migrations, no partition maintenance — reads and writes degrade gracefully.
# Hence: skip silently, never raise.
_audit_url = _build_pg_url("AUDIT_POSTGRES", fallback_env="AUDIT_DB_URL")
audit_db = (
    PostgresDB(
        connection_string=_audit_url,
        migrations_dir=MIGRATIONS_AUDIT_DIR,
        env_prefix="AUDIT_POSTGRES",
        default_min_connections=1,
        default_max_connections=4,
    )
    if _audit_url
    else None
)

# Audit READS: the cockpit-facing read backend, served by the Postgres
# AuditStore. AuditStore is null-safe: is_available stays False (the read
# endpoints' degraded shapes, never a crash) until connect() runs on a real
# DSN in the lifespan.
audit_store = AuditStore(_audit_url)
audit_reader = audit_store

# One host-key parse memo per application. An unauthenticated endpoint
# (GET /api/ssh/host-keys) reads through it, so it must outlive the request
# and belong to exactly one application: a module-level cache would let one
# app's operator configuration decide another's published host keys.
# Constructing it inside the dependency factory would hand every request a
# fresh empty cache and silently delete the memoization.
_ssh_gateway_host_key_cache = ssh_access_operations.SshGatewayHostKeyCache(
    logger=logger
)

# Usage-metering ledger (Slice 4). Instantiated in the lifespan once the audit +
# app pools and the usage_rates migration are ready; None until then (and on
# deployments without the audit tier — metering disabled, non-load-bearing).
usage_ledger: UsageLedger | None = None
# Rollup writer + rollup-aware read surface over usage_ledger. Built alongside
# usage_ledger once both pools are ready; None until then (and without the audit
# tier). The 3 /api/usage endpoints read through it (rollup for closed days, raw
# for the tail); a daily leader-only pass maintains the app-DB usage_daily mirror.
usage_rollup: UsageRollup | None = None
# Public-cloud comparison rate cards live in the app DB and reprice aggregate
# quantities at read time. They never overwrite the canonical cost snapshotted
# on usage_events. None only until app migrations/pool initialization completes.
usage_cloud_estimator: CloudCostEstimator | None = None
# Slice 0 infrastructure-metering foundations. All gates default off; the typed
# v2 reader additionally requires its runtime schema capability probe to pass.
infrastructure_metering_settings = InfrastructureMeteringSettings()
infrastructure_usage_v2: UsageV2QueryService | None = None
infrastructure_usage_rollup: TypedUsageDailyRollup | None = None
infrastructure_inventory_store: InventoryStore | None = None
infrastructure_ingestion_service: InfrastructureIngestionService | None = None
infrastructure_workspace_cutover: InfrastructureWorkspaceCutover | None = None
infrastructure_usage_materializer: InfrastructureUsageMaterializer | None = None
infrastructure_usage_day_sealer: InfrastructureUsageDaySealer | None = None
infrastructure_metering_runtime: InfrastructureMeteringRuntime | None = None
infrastructure_coverage_waivers: CoverageGapWaiverService | None = None
infrastructure_storage_assets: StorageAssetStore | None = None
infrastructure_storage_mapping: StorageResourceMappingStore | None = None
infrastructure_compute_activation: ComputeActivationStore | None = None
infrastructure_compute_scope_diagnostics: dict[str, str] = {}
infrastructure_durable_compute_activation_keys: frozenset[str] = frozenset()
infrastructure_durable_reporting_policy_ready = False
infrastructure_storage_source_activation_ready = False


# Session router singletons — see knowledge-base/knowledge/features/direct_session_websockets.md
import json as _session_json  # noqa: E402

_session_annotations_raw = os.environ.get("SESSION_INGRESS_ANNOTATIONS", "{}")
try:
    _session_annotations = _session_json.loads(_session_annotations_raw)
    if not isinstance(_session_annotations, dict):
        _session_annotations = {}
except (ValueError, TypeError):
    logger.warning(
        "SESSION_INGRESS_ANNOTATIONS env not valid JSON: %r — falling back to {}",
        _session_annotations_raw,
    )
    _session_annotations = {}

session_router = SessionRouterService(
    namespace=os.environ.get("SESSION_INGRESS_NAMESPACE", "default"),
    ingress_host=os.environ.get("SESSION_INGRESS_HOST", "api.example.com"),
    ingress_class=os.environ.get("SESSION_INGRESS_CLASS", "traefik"),
    annotations=_session_annotations,
    tls_secret_name=os.environ.get("SESSION_INGRESS_TLS_SECRET") or None,
    single_origin=os.environ.get("SESSION_INGRESS_SINGLE_ORIGIN", "").lower()
    in {"1", "true"},
    db=postgres_db,
)


def _pinned_retirement_operations() -> PinnedRetirementOperations:
    """Bind shared retirement authority to this application's collaborators."""

    return PinnedRetirementOperations(
        PinnedRetirementDependencies(
            store=postgres_db,
            agent_provisioner=agent_provisioner,
            persistent_provisioner=persistent_provisioner,
            container_provisioner=container_provisioner,
            docker_provisioner=docker_provisioner,
            vm_provisioner=vm_provisioner,
            recovery_store=VMWorkspaceRecoveryStore(postgres_db),
            session_router=session_router,
            resolve_protected_reader_backend=functools.partial(
                protected_cloud_engage._resolve_protected_reader_backend,
                dependencies=_protected_cloud_engage_dependencies(),
            ),
            resolve_ssh_key_path=resolve_ssh_key_path,
            logger=logger,
        )
    )


_session_jwt_secret = os.environ.get("SESSION_JWT_SECRET", "")
if _session_jwt_secret:
    session_tokens = SessionTokenService(
        secret=_session_jwt_secret,
        ttl_seconds=int(os.environ.get("SESSION_JWT_TTL_S", "60")),
    )
else:
    # Allow boot without session_tokens (e.g., during chart install before
    # the Secret is set). Calls to GET /connection will fail at runtime
    # with a clear error.
    session_tokens = None  # type: ignore[assignment]
    logger.warning("SESSION_JWT_SECRET not set — direct WS session endpoints will fail")


# Background Tasks
# =============================================================================

# Flag to signal shutdown to background tasks
_shutdown_event: asyncio.Event | None = None

# Auto-assignment toggle (env var, default true)
AUTO_ASSIGN_ENABLED = os.environ.get("AUTO_ASSIGN_ENABLED", "true").lower() in (
    "true",
    "1",
    "yes",
)

# Session admission reads the same default-off gate that renders the generic
# stateless executor Deployment. Helm supplies this explicitly to the
# orchestrator; an unset/local process preserves pinned behavior.
STATELESS_SESSION_ENABLED = os.environ.get(
    "STATELESS_SESSION_ENABLED", "false"
).lower() in ("true", "1", "yes")

# Worker jobs remain pinned unless this independent admission gate is opened.
# Session-pool enablement is intentionally not sufficient: worker rollout and
# rollback have different safety gates and capacity requirements.
STATELESS_WORKER_ENABLED = os.environ.get(
    "STATELESS_WORKER_ENABLED", "false"
).lower() in ("true", "1", "yes")

# Worker-lane defaulting is subordinate to admission. If this is enabled while
# worker admission remains disabled, omitted root jobs silently stay pinned.
STATELESS_WORKER_DEFAULT_ENABLED = os.environ.get(
    "STATELESS_WORKER_DEFAULT_ENABLED", "false"
).lower() in ("true", "1", "yes")

# Gate-3 completion commands ship dark.  Helm wires this value explicitly in
# M4; local/tests may opt in before then without changing the legacy path.
COMPLETION_COMMANDS_ENABLED = os.environ.get(
    "COMPLETION_COMMANDS_ENABLED", "false"
).lower() in ("true", "1", "yes")

# Step 4 reorders only newly accepted completion commands.  The admission
# decision is persisted on the command row so a config flip or rolling restart
# cannot change the ordering contract of work already in flight.
COMPLETION_STATUS_REORDER_ENABLED = os.environ.get(
    "COMPLETION_STATUS_REORDER_ENABLED", "false"
).lower() in ("true", "1", "yes")

# First-rollout safety fence for the interim dedicated Officer-pod owner.
# Read-only drift observation and the authorized manual recycle operation stay
# available while automatic drift/missing-pod mutation is dark.
PERSISTENT_AGENT_RECONCILIATION_ENABLED = os.environ.get(
    "PERSISTENT_AGENT_RECONCILIATION_ENABLED", "false"
).lower() in ("true", "1", "yes")

# Dark-by-default, admin-only deployed verification seam for the exact current
# Officer runtime binding. The service performs no lookup or hot-path work
# unless this rollout flag is explicitly enabled through Helm.
OFFICER_RUNTIME_VERIFICATION_ENABLED = os.environ.get(
    "OFFICER_RUNTIME_VERIFICATION_ENABLED", "false"
).lower() in ("true", "1", "yes")

# BP-01 release fence. The owner-facing control may ship while the wider
# unattended-release scorecard is still open, but a stored/manual JSON edit
# must not make the money-spending tick live. The tick and every supported
# false -> true writer consume this same immutable deployment policy.
# ``false`` always remains writable so an operator can stand a century down
# even during rollback or a mixed-version incident.
OFFICER_AUTO_PULL_RELEASE_ENABLED = os.environ.get(
    "OFFICER_AUTO_PULL_RELEASE_ENABLED", "false"
).lower() in ("true", "1", "yes")

# Local-only crash-recovery proof hook. Production/chart defaults keep this at
# zero; a positive value makes the accept -> force-delete window deterministic.
COMPLETION_FINALIZER_INLINE_DELAY_SECONDS = max(
    0.0,
    float(os.environ.get("COMPLETION_FINALIZER_INLINE_DELAY_SECONDS", "0")),
)

# S36 explicitly overrides the workspace Pod's ordinary 120-second grace with
# a 10-second UID-preconditioned delete. Keep the exact-absence proof below the
# pinned agent's 60-second report timeout while leaving room for API latency.
_COMPLETION_S36_EXACT_ABSENCE_TIMEOUT_SECONDS = 45.0

# One application-owned dispatch state (R1.B11): the lock that prevents a
# double-assignment, and the pause-pending set preemption shares with
# job-control delivery (which discards a job once its pause lands).
_job_dispatch_state = job_dispatcher.JobDispatchState()

# One app-owned registry for the request-spawned KB datasource reindexes
# (R1.B03). It holds both views the two former globals held — the shutdown
# ownership set and the per-datasource cancellation index — so a delete can
# fence only its own source while `lifespan` still drains everything.
kb_datasource_tasks = KbDatasourceTaskRegistry()

# App-owned lazy Neo4j handle, replacing the `_knowledge_graph_db` global.
# Constructing it here is free; nothing connects until the first `.get()`.
_knowledge_graph = KnowledgeGraphHandle(logger=logger)

# Per-project heal locks and background-repair throttling, owned by this
# application rather than by the project-provisioning module, so a second app
# instance in the same process does not share them.
_project_repair_state = project_provisioning_operations.ProjectRepairState()
# R1.B04 — replaces main's ``_protected_engage_tasks`` and
# ``_cloud_stage_tasks`` module dicts. One instance per application, so two
# applications in one process do not share in-flight task state.
cloud_task_registry = CloudTaskRegistry()


def _stale_agent_detector_dependencies() -> (
    stale_agent_detector_service.StaleAgentDetectorDependencies
):
    """Bind agent reconciliation and the durable retirement retry (R1.B11).

    Retirement operations are providers: they are recomposed per call from
    current application state, exactly as the former in-module calls did.
    """

    return stale_agent_detector_service.StaleAgentDetectorDependencies(
        store=postgres_db,
        agent_provisioner=agent_provisioner,
        docker_provisioner=docker_provisioner,
        audit_reader=audit_reader,
        completion_commands_enabled=COMPLETION_COMMANDS_ENABLED,
        trigger_dispatch=_trigger_dispatch,
        schedule_attach_abort_successor=_schedule_attach_abort_successor,
        thread_retirement_operations=_thread_retirement_operations,
        pinned_retirement_operations=_pinned_retirement_operations,
    )


_stateless_workspace_ensure_registry = (
    stateless_workspace_scheduler.StatelessWorkspaceEnsureRegistry()
)


def _stateless_workspace_schedule_dependencies() -> (
    stateless_workspace_scheduler.StatelessWorkspaceScheduleDependencies
):
    """Rebuilt per call, except the registry: that is the one field which must
    be the *same* object across builds, or single-flighting would not."""

    return stateless_workspace_scheduler.StatelessWorkspaceScheduleDependencies(
        store=postgres_db,
        provisioner=container_provisioner,
        suspension=workspace_suspension_service,
        registry=_stateless_workspace_ensure_registry,
        ensure_session_workspace=ensure_session_workspace,
    )


def _schedule_stateless_workspace_ensure(thread_id: str) -> asyncio.Task[None]:
    """Bridge to ``services/stateless_workspace_scheduler.py`` (R1.B05)."""
    return stateless_workspace_scheduler.schedule_stateless_workspace_ensure(
        thread_id, dependencies=_stateless_workspace_schedule_dependencies()
    )


def _build_vm_idle_service() -> Any:
    """The VM idle release/wake adapter the workspace idle sweeper drives."""
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleService

    return VMIdleLifecycleService(
        postgres_db,
        vm_provisioner,
        VMWorkspaceRecoveryStore(postgres_db),
        claimant=f"{os.getenv('HOSTNAME', 'orchestrator')}:vm-idle",
        agent_provisioner=agent_provisioner,
        thread_retirement=_thread_retirement_operations(),
        thread_workspace_suspension=workspace_suspension_service,
        thread_prepare=lambda thread_id, operation_id: (
            sessions_routes.prepare_woken_pinned_session(
                thread_id,
                operation_id,
                dependencies=_sessions_dependencies(),
            )
        ),
        terminal_publication_handler=lambda operation: (
            _job_control_operations().publish_terminal_review(operation)
        ),
    )


def _pinned_k8s_reconciliation_dependencies() -> (
    pinned_k8s_reconciliation_service.PinnedK8sReconciliationDependencies
):
    return pinned_k8s_reconciliation_service.PinnedK8sReconciliationDependencies(
        store=postgres_db,
        agent_provisioner=agent_provisioner,
        persistent_provisioner=persistent_provisioner,
        container_provisioner=container_provisioner,
    )


async def _begin_pinned_thread_retirement(
    thread_id: str, **kwargs: Any
) -> dict[str, Any]:
    return await _pinned_retirement_operations().begin_pinned_thread_retirement(
        thread_id, **kwargs
    )


# =============================================================================
# Job Auto-Assignment Dispatcher
# =============================================================================


from orchestrator.services.deployment_gates import (  # noqa: E402
    is_experts_db_enabled as _is_experts_db_enabled,
)


from orchestrator.services.deployment_gates import (  # noqa: E402
    is_skills_db_enabled as _is_skills_db_enabled,
)


from orchestrator.services.deployment_gates import (  # noqa: E402
    mcp_datasources_enabled as _mcp_datasources_enabled,
)


from orchestrator.services.deployment_gates import (  # noqa: E402
    datasource_defaults_on_omission as _datasource_defaults_on_omission,
)


from orchestrator.services.deployment_gates import (  # noqa: E402
    datasource_scope_auto_attach_v1_enabled as _datasource_scope_auto_attach_v1_enabled,
)


from orchestrator.services.deployment_gates import (  # noqa: E402
    mcp_stdio_enabled as _mcp_stdio_enabled,
)


def _validate_mcp_datasource(connection_url: Any, credentials: dict[str, Any]) -> None:
    return datasource_config.validate_mcp_datasource(connection_url, credentials)


from orchestrator.services.deployment_gates import (  # noqa: E402
    is_protected_cloud_mode_enabled as _is_protected_cloud_mode_enabled,
)


from orchestrator.services.deployment_gates import (  # noqa: E402
    require_pinned_status_identity as _require_pinned_status_identity,
)

from orchestrator.services.deployment_gates import (  # noqa: E402
    stateless_idle_conversation_rewind_enabled as _stateless_idle_conversation_rewind_enabled,
)


def _session_config_dependencies() -> (
    session_config_resolution.SessionConfigDependencies
):
    """Rebuilt per call.

    Every collaborator is read from this module's namespace at call time, which
    is the only reason an existing ``patch("orchestrator.main._x")`` still
    steers the service: a factory that captured them at import would leave
    those suites green and inert (§P3).
    """

    return session_config_resolution.SessionConfigDependencies(
        store=postgres_db,
        is_experts_db_enabled=_is_experts_db_enabled,
        user_experts_enabled=_user_experts_enabled,
        resolve_runner_grants=_resolve_runner_grants,
        enforce_dispatch_grants=_enforce_dispatch_grants,
        gather_in_scope_skills=_gather_in_scope_skills,
        seed_registry_model_overrides=_seed_registry_model_overrides,
        inject_thread_dispatch_credentials=_inject_thread_dispatch_credentials,
        thread_project_ids=_thread_project_ids,
        thread_has_knowledge_scope=_thread_has_knowledge_scope,
    )


async def _resolve_default_models(user_id: str | None) -> dict[str, Any]:
    return await session_config_resolution.resolve_default_models(
        user_id, dependencies=_session_config_dependencies()
    )


async def _prefetch_roster_refs(*args: Any, **kwargs: Any) -> Any:
    return await session_config_resolution.prefetch_roster_refs(
        *args, **kwargs, dependencies=_session_config_dependencies()
    )


async def _account_defaults_layer(*args: Any, **kwargs: Any) -> Any:
    return await session_config_resolution.account_defaults_layer(
        *args, **kwargs, dependencies=_session_config_dependencies()
    )


async def _acknowledged_grant_strip(*args: Any, **kwargs: Any) -> Any:
    return await session_config_resolution.acknowledged_grant_strip(
        *args, **kwargs, dependencies=_session_config_dependencies()
    )


async def _resolve_session_config(*args: Any, **kwargs: Any) -> Any:
    return await session_config_resolution.resolve_session_config(
        *args, **kwargs, dependencies=_session_config_dependencies()
    )


#: TOTAL wall-clock budget for the agent toolset probe, both hops included.
#: Enforced with ``asyncio.wait_for``, not with httpx's timeout: httpx's is
#: per-operation, so a 404 on ``/session/toolset`` followed by the ``/status``
#: fallback would otherwise cost two full budgets, and a pod trickling bytes
#: could exceed either without ever tripping one. The cockpit blocks its
#: settings pane on this endpoint, so the bound has to be a real deadline.

#: Agent rows in these states keep a ``pod_ip`` that no longer routes.

#: Registered but not yet serving: nothing is bound, so there is nothing to
#: measure and the probe would only spend budget discovering that. Kept apart
#: from the terminal set so the reason we report is the true one.

#: NOTE on what is deliberately NOT skipped: ``ready``. An agent stays ``ready``
#: for up to one heartbeat interval (60s) after attach, so gating the probe on
#: ``status == "session"`` would report a prediction for the first minute of
#: every session — the exact silently-wrong answer this endpoint removes.


def _agent_toolset_dependencies() -> agent_toolset_probe.AgentToolsetDependencies:
    return agent_toolset_probe.AgentToolsetDependencies(store=postgres_db)


async def _agent_toolset_measurement(*args: Any, **kwargs: Any) -> Any:
    return await agent_toolset_probe.agent_toolset_measurement(
        *args, **kwargs, dependencies=_agent_toolset_dependencies()
    )


from orchestrator.services.config_overrides import looks_like_uuid as _looks_like_uuid  # noqa: E402


def _dispatch_credential_dependencies() -> (
    dispatch_credentials.DispatchCredentialDependencies
):
    """Rebuilt per call. ``resolve_model`` is read from this module because
    several suites monkeypatch ``main._resolve_model``; passing the shared
    import instead would leave them green and inert (§P3)."""

    return dispatch_credentials.DispatchCredentialDependencies(
        store=postgres_db, logger=logger, resolve_model=_resolve_model
    )


async def _seed_registry_model_overrides(*args: Any, **kwargs: Any) -> Any:
    return await dispatch_credentials.seed_registry_model_overrides(
        *args, **kwargs, dependencies=_dispatch_credential_dependencies()
    )


from orchestrator.services.dispatch_credentials import (  # noqa: E402
    nested_model_slots as _nested_model_slots,
)


def _job_dispatch_credential_dependencies() -> (
    job_dispatch_credentials.DispatchCredentialDependencies
):
    """Composition only: every injector is lane C's, and each is bound to
    **main's own wrapper** so the suites that patch them keep steering."""

    return job_dispatch_credentials.DispatchCredentialDependencies(
        store=postgres_db,
        logger=logger,
        resolve_model=_resolve_model,
        inject_model_credentials=_inject_model_credentials,
        inject_env_key_credentials=_inject_env_key_credentials,
        inject_search_credentials=_inject_search_credentials,
        inject_system_kb_embedding_profile=_inject_system_kb_embedding_profile,
        dispatch_llm_provider_fallback=_dispatch_llm_provider_fallback,
        nested_model_slots=_nested_model_slots,
    )


async def _inject_dispatch_credentials(*args: Any, **kwargs: Any) -> Any:
    return await job_dispatch_credentials.inject_dispatch_credentials(
        *args, **kwargs, dependencies=_job_dispatch_credential_dependencies()
    )


def _job_start_bundle_dependencies() -> job_start_bundle.JobStartBundleDependencies:
    """Rebuilt per call. ``mint_worker_runtime_actor``,
    ``authorize_job_repository_transport`` and ``inject_blob_credentials`` are
    fields rather than imports because existing suites patch them on ``main``
    and then drive the job-start owner (§P3)."""

    return job_start_bundle.JobStartBundleDependencies(
        store=postgres_db,
        logger=logger,
        forge=gitea_client,
        workspace_runtime=_job_workspace_runtime_dependencies(),
        inject_dispatch_credentials=_inject_dispatch_credentials,
        resolve_authorized_job_datasources=_resolve_authorized_job_datasources,
        job_project_repositories=_job_project_repositories,
        apply_cloud_storage_override=_apply_cloud_storage_override,
        build_datasources_payload=_build_datasources_payload,
        build_datasource_tool_override=_build_datasource_tool_override,
        prepare_job_primary_repository_authority=(
            prepare_job_primary_repository_authority
        ),
        prepare_project_repository_authority=prepare_project_repository_authority,
        authorize_job_repository_transport=authorize_job_repository_transport,
        mint_worker_runtime_actor=mint_worker_runtime_actor,
        inject_blob_credentials=inject_blob_credentials,
        grant_denied_error=GrantDenied,
        lite_workspace_config_error=LiteWorkspaceConfigError,
        backend_from_override=_backend_from_override,
        inject_lite_workspace_config=_inject_lite_workspace_config,
        is_experts_db_enabled=_is_experts_db_enabled,
        user_experts_enabled=_user_experts_enabled,
        enforce_dispatch_grants=_enforce_dispatch_grants,
        grant_violations_detail=_grant_violations_detail,
        resolve_default_models=_resolve_default_models,
        prefetch_roster_refs=_prefetch_roster_refs,
        seed_registry_model_overrides=_seed_registry_model_overrides,
        gather_in_scope_skills=_gather_in_scope_skills,
        resolve_config=resolve_config,
        vm_workspaces_on_pod_network=vm_workspaces_on_pod_network,
    )


async def _job_project_repositories(*args: Any, **kwargs: Any) -> Any:
    return await job_start_bundle.job_project_repositories(
        *args, **kwargs, dependencies=_job_start_bundle_dependencies()
    )


async def _prepare_job_repository_before_claim(*args: Any, **kwargs: Any) -> Any:
    return await job_start_bundle.prepare_job_repository_before_claim(
        *args, **kwargs, dependencies=_job_start_bundle_dependencies()
    )


from orchestrator.services.job_start_bundle import (  # noqa: E402
    redispatch_livelock_trip as _redispatch_livelock_trip,
)


from orchestrator.services.job_mutation_target import (  # noqa: E402
    PinnedJobMutationTarget as _PinnedJobMutationTarget,
)


from orchestrator.services.job_mutation_target import (  # noqa: E402, F401
    FRESH_PINNED_RECIPIENT_ATTESTATION_ATTEMPTS as _FRESH_PINNED_RECIPIENT_ATTESTATION_ATTEMPTS,
)
from orchestrator.services.job_mutation_target import (  # noqa: E402, F401
    FRESH_PINNED_RECIPIENT_ATTESTATION_DELAY_S as _FRESH_PINNED_RECIPIENT_ATTESTATION_DELAY_S,
)


async def _prepare_pinned_job_mutation_target(
    *,
    agent_id: str,
    job_id: str,
    require_idle: bool,
) -> _PinnedJobMutationTarget | None:
    from orchestrator.services import job_mutation_target

    return await job_mutation_target.prepare_pinned_job_mutation_target(
        agent_id=agent_id,
        job_id=job_id,
        require_idle=require_idle,
        dependencies=job_mutation_target.PinnedJobMutationTargetDependencies(
            store=postgres_db,
            agent_provisioner=agent_provisioner,
            logger=logger,
            http_client_factory=httpx.AsyncClient,
            sleep=asyncio.sleep,
        ),
    )


_attach_abort_successor_tasks: dict[
    tuple[str, str, str, str], "asyncio.Task[None]"
] = {}


async def _capture_session_delivery(thread, resolved, status, *, project_ids):
    from orchestrator.services.manifest_session_delivery import capture_session_delivery

    return await capture_session_delivery(
        postgres_db, thread, resolved, status, project_ids=project_ids
    )


def _session_attach_payload_dependencies() -> (
    session_attach_payload.SessionAttachPayloadDependencies
):
    """Rebuilt per call, reading every collaborator from this module."""

    return session_attach_payload.SessionAttachPayloadDependencies(
        store=postgres_db,
        GrantDenied=GrantDenied,
        LiteWorkspaceConfigError=LiteWorkspaceConfigError,
        await_protected_cloud_runtime_ready=_await_protected_cloud_runtime_ready,
        build_datasource_tool_override=_build_datasource_tool_override,
        build_datasources_payload=_build_datasources_payload,
        build_protected_cloud_mount=_build_protected_cloud_mount,
        inject_lite_workspace_config=_inject_lite_workspace_config,
        mint_thread_runtime_actor=mint_thread_runtime_actor,
        protected_mount_selection_identity=_protected_mount_selection_identity,
        require_pinned_status_identity=_require_pinned_status_identity,
        resolve_authorized_thread_datasources=_resolve_authorized_thread_datasources,
        resolve_session_config=_resolve_session_config,
        revalidate_thread_project_ids=_revalidate_thread_project_ids,
        ro_mount_matches_protected_selection=_ro_mount_matches_protected_selection,
        thread_accepts_runtime=_thread_accepts_runtime,
        thread_project_ids=_thread_project_ids,
        capture_session_config=_capture_session_delivery,
    )


from orchestrator.services.workspace_tier_policy import (  # noqa: E402
    LiteWorkspaceConfigError as LiteWorkspaceConfigError,
)


from orchestrator.services.workspace_tier_policy import (  # noqa: E402
    backend_from_override as _backend_from_override,
)


from orchestrator.services.workspace_tier_policy import (  # noqa: E402
    thread_workspace_backend as _thread_workspace_backend,
)


from orchestrator.services.session_class_policy import (  # noqa: E402
    session_class_pinned_refusal as _stateless_session_class_refusal,
)


from orchestrator.services.session_class_policy import (  # noqa: E402
    require_stateless_workspace as _require_stateless_workspace,
)


from orchestrator.services.session_class_policy import (  # noqa: E402
    require_stateless_end_workspace as _require_stateless_end_workspace,
)


from orchestrator.services.workspace_tier_policy import (  # noqa: E402
    is_lite_config_override as _is_lite_config_override,
)


def _execution_lane_dependencies() -> session_class_policy.ExecutionLaneDependencies:
    """The pool gate is passed as a **callable**, not a value: it is an
    import-time constant B11 owns, and a suite that rebinds it on ``main`` must
    still steer the service (§P1)."""

    return session_class_policy.ExecutionLaneDependencies(
        stateless_session_enabled=lambda: STATELESS_SESSION_ENABLED,
        container_provisioner=container_provisioner,
        virtual_workspace_rclone_spec=_virtual_workspace_rclone_spec,
    )


# Reasoning-effort vocabulary accepted at session create. The superset across
# families — the family capability clamps to what the chosen model actually
# supports at attach (loader._clamp_reasoning_level), so over-asking degrades
# gracefully; garbage fails loud here instead of being silently dropped.


# These values authorize unattended work or bound its money spend. They are
# owned by the durable Officer Post and must never be accepted from the generic
# session-create/config surfaces. Explicit commission carries them through the
# non-model-selectable ``_officer_post_config_snapshot`` seam below.


from orchestrator.services.session_create_overrides import (  # noqa: E402
    validated_session_officer_override as _validated_session_officer_override,
)


from orchestrator.services.session_tool_policy import (  # noqa: E402
    validated_tool_overrides as _validated_tool_overrides,
)


from orchestrator.services.session_tool_policy import (  # noqa: E402
    with_validated_tool_overrides as _with_validated_tool_overrides,
)
from orchestrator.services.manifest_runtime_ownership import (  # noqa: E402
    require_srw_runtime,
)


from orchestrator.services.virtual_workspace import (  # noqa: E402
    check_object_store_config as _check_object_store_config,
)


from orchestrator.services.workspace_tier_policy import (  # noqa: E402
    inject_lite_workspace_config as _inject_lite_workspace_config,
)


from orchestrator.services.job_datasource_selection import (  # noqa: E402
    repository_datasource_names as _repository_datasource_names,
)


def _job_datasource_selection_dependencies() -> (
    job_datasource_selection.JobDatasourceSelectionDependencies
):
    """``revalidate_selection`` is deliberately bound to **main's own wrapper**,
    not to the service function: it is consumed by a *different* function in
    that module, so this cannot recurse, and binding the service directly would
    destroy a patch seam live tests use."""

    return job_datasource_selection.JobDatasourceSelectionDependencies(
        store=postgres_db,
        authorize_thread_datasource_selection=_authorize_thread_datasource_selection,
        backend_from_override=_backend_from_override,
        revalidate_selection=_revalidate_job_datasource_selection,
    )


async def _inherit_parent_datasource_ids(*args: Any, **kwargs: Any) -> Any:
    return await job_datasource_selection.inherit_parent_datasource_ids(
        *args, **kwargs, dependencies=_job_datasource_selection_dependencies()
    )


async def _filter_implicit_lite_datasource_ids(*args: Any, **kwargs: Any) -> Any:
    return await job_datasource_selection.filter_implicit_lite_datasource_ids(
        *args, **kwargs, dependencies=_job_datasource_selection_dependencies()
    )


from orchestrator.services.job_datasource_selection import (  # noqa: E402
    datasource_selection_provenance as _datasource_selection_provenance,
)


async def _revalidate_job_datasource_selection(*args: Any, **kwargs: Any) -> Any:
    return await job_datasource_selection.revalidate_job_datasource_selection(
        *args, **kwargs, dependencies=_job_datasource_selection_dependencies()
    )


async def _resolve_authorized_job_datasources(*args: Any, **kwargs: Any) -> Any:
    return await job_datasource_selection.resolve_authorized_job_datasources(
        *args, **kwargs, dependencies=_job_datasource_selection_dependencies()
    )


from orchestrator.services.job_workspace_runtime import job_needs_vm as _job_needs_vm  # noqa: E402


from orchestrator.services.job_workspace_runtime import (  # noqa: E402
    get_vm_context as _get_vm_context,
)


from orchestrator.services.job_workspace_runtime import (  # noqa: E402
    get_infra_transient_context as _get_infra_transient_context,
)


def _job_workspace_runtime_dependencies() -> (
    job_workspace_runtime.JobWorkspaceRuntimeDependencies
):
    """``vm_mode`` and the worker gate are **callables** — one is a provisioner
    attribute that changes at runtime, the other an import-time B11 flag."""

    return job_workspace_runtime.JobWorkspaceRuntimeDependencies(
        store=postgres_db,
        vm_mode=lambda: vm_provisioner.mode,
        workspace_provisioner=container_provisioner,
        vm_workspaces_on_pod_network=vm_workspaces_on_pod_network,
        stateless_worker_enabled=lambda: STATELESS_WORKER_ENABLED,
        backend_from_override=_backend_from_override,
    )


async def _fail_vm_parked_job(*args: Any, **kwargs: Any) -> Any:
    return await job_workspace_runtime.fail_vm_parked_job(
        *args, **kwargs, dependencies=_job_workspace_runtime_dependencies()
    )


def _job_needs_sandbox(*args: Any, **kwargs: Any) -> Any:
    return job_workspace_runtime.job_needs_sandbox(
        *args, **kwargs, dependencies=_job_workspace_runtime_dependencies()
    )


def _resolve_requested_job_execution_lane(*args: Any, **kwargs: Any) -> Any:
    return job_workspace_runtime.resolve_requested_job_execution_lane(
        *args, **kwargs, dependencies=_job_workspace_runtime_dependencies()
    )


from orchestrator.services.job_workspace_runtime import (  # noqa: E402
    get_container_context as _get_container_context,
)


def _job_workspace_authority_dependencies() -> (
    job_workspace_authority.JobWorkspaceAuthorityDependencies
):
    """Four fields are bound to **main's own wrappers** on purpose:
    ``resolve_inherited_workspace``, ``fail_subjob_and_unblock_parent`` and
    ``workspace_runtime_unchanged_before_delivery`` are each consumed by a
    *different* function in that module, and `test_pinned_job_recipient.py`
    patches the last of them on ``main`` and then calls a sibling directly."""

    return job_workspace_authority.JobWorkspaceAuthorityDependencies(
        store=postgres_db,
        logger=logger,
        workspace_provisioner=container_provisioner,
        vm_provisioner=vm_provisioner,
        vm_mode=lambda: vm_provisioner.mode,
        # `ensure_workspace`, not `ensure_session_workspace`: the scholar
        # parent path provisions a *job* workspace and passes
        # `current_status=`, which the session helper does not accept.
        ensure_workspace=ensure_workspace,
        workspace_suspension=workspace_suspension_service,
        handle_scholar_completion=(
            lambda job, actions: subjob_completion_operations.handle_scholar_completion(
                job,
                actions,
                dependencies=_scholar_completion_dependencies(),
            )
        ),
        handle_delegation_child_completion=(
            lambda job, actions: (
                subjob_completion_operations.handle_delegation_child_completion(
                    job,
                    actions,
                    dependencies=_delegation_completion_dependencies(),
                )
            )
        ),
        resolve_inherited_workspace=_resolve_subjob_inherited_workspace,
        fail_subjob_and_unblock_parent=_fail_subjob_and_unblock_parent,
        workspace_runtime_unchanged_before_delivery=(
            _workspace_runtime_unchanged_before_delivery
        ),
    )


async def _workspace_runtime_unchanged_before_delivery(
    *args: Any, **kwargs: Any
) -> Any:
    return await job_workspace_authority.workspace_runtime_unchanged_before_delivery(
        *args, **kwargs, dependencies=_job_workspace_authority_dependencies()
    )


# Job context key holding each remote tier's live workspace, by tier name.
from orchestrator.services.job_workspace_runtime import (  # noqa: E402
    WORKSPACE_CONTEXT_KEYS as _WORKSPACE_CONTEXT_KEYS,
)


def _scholar_should_provision_parent_container(*args: Any, **kwargs: Any) -> Any:
    return job_workspace_runtime.scholar_should_provision_parent_container(
        *args, **kwargs, dependencies=_job_workspace_runtime_dependencies()
    )


# Bounded wait for a subjob to inherit its parent's provisioned workspace.
# Parent container/VM readiness is an async event that lands AFTER the subjob is
# spawned (a scholar is created ~3s after its parent, mid-provisioning), so we
# resolve from the parent's live row every dispatch tick. This bounds how long
# we wait before giving up with a diagnosable failure instead of stranding the job.


async def _resolve_subjob_inherited_workspace(*args: Any, **kwargs: Any) -> Any:
    return await job_workspace_authority.resolve_subjob_inherited_workspace(
        *args, **kwargs, dependencies=_job_workspace_authority_dependencies()
    )


async def _prepare_job_workspace_runtime(*args: Any, **kwargs: Any) -> Any:
    return await job_workspace_authority.prepare_job_workspace_runtime(
        *args, **kwargs, dependencies=_job_workspace_authority_dependencies()
    )


async def _fail_subjob_and_unblock_parent(*args: Any, **kwargs: Any) -> Any:
    return await job_workspace_authority.fail_subjob_and_unblock_parent(
        *args, **kwargs, dependencies=_job_workspace_authority_dependencies()
    )


async def _provision_parent_workspace_for_scholar(*args: Any, **kwargs: Any) -> Any:
    return await job_workspace_authority.provision_parent_workspace_for_scholar(
        *args, **kwargs, dependencies=_job_workspace_authority_dependencies()
    )


from orchestrator.services.job_workspace_runtime import (  # noqa: E402
    apply_sticky_sudo_denial as _apply_sticky_sudo_denial,
)


# =============================================================================
# Capability grants (User-Defined Experts, Slice 2) — PEPs + helpers
#
# One pure PDP (src/core/capability_grants.evaluate) is enforced at four points:
# save-time (the 3 expert endpoints), job dispatch, job resume, and session
# attach. Deny-by-default for security keys; existing approved users were
# grandfathered by migration 0030 (shell_tools + delegation). See
# knowledge-history/done/global_expert_management.md (decisions 8, 9, 19, 21-23).
# =============================================================================


from orchestrator.services.grant_enforcement import GrantDenied as GrantDenied  # noqa: E402


def _grant_enforcement_dependencies() -> grant_enforcement.GrantEnforcementDependencies:
    """Rebuilt per call, reading every collaborator from this module."""

    return grant_enforcement.GrantEnforcementDependencies(
        store=postgres_db,
        user_experts_enabled=_user_experts_enabled,
        resolve_runner_grants=_resolve_runner_grants,
        enforce_dispatch_grants=_enforce_dispatch_grants,
        check_vm_permission=_check_vm_permission,
    )


async def _user_experts_enabled(*args: Any, **kwargs: Any) -> Any:
    return await grant_enforcement.user_experts_enabled(
        *args, **kwargs, dependencies=_grant_enforcement_dependencies()
    )


from orchestrator.services.grant_enforcement import (  # noqa: E402
    grant_violations_detail as _grant_violations_detail,
)


from orchestrator.services.config_overrides import (  # noqa: E402
    deep_merge_dicts as _deep_merge_dicts,
)
from orchestrator.services.job_admission import (  # noqa: E402
    JobAdmissionDependencies,
    admit_job,
)
from orchestrator.services.job_admission_datasources import (  # noqa: E402
    JobAdmissionDatasourcesDependencies,
)
from orchestrator.services.job_admission_delivery import (  # noqa: E402
    JobAdmissionDeliveryDependencies,
)
from orchestrator.services.job_admission_creation import (  # noqa: E402
    JobAdmissionCreationDependencies,
)
from orchestrator.services.job_admission_config import (  # noqa: E402
    JobAdmissionConfigDependencies,
)
from orchestrator.services.job_admission_workspace import (  # noqa: E402
    JobAdmissionWorkspaceDependencies,
)
from orchestrator.services.job_admission_officer import (  # noqa: E402
    JobAdmissionOfficerDependencies,
    compose_category_kickoff as _compose_category_kickoff,  # noqa: F401 -- compatibility export
)
from orchestrator.services.job_admission_scope import (  # noqa: E402
    JobAdmissionActor,
    JobAdmissionScopeDependencies,
    _INTERNAL_JOB_SCOPE_DENIED,
)
from orchestrator.services.job_create_ingress import (  # noqa: E402
    _SERVER_OWNED_OFFICER_CONTEXT_KEYS as _SERVER_OWNED_OFFICER_CONTEXT_KEYS,
    _SERVER_OWNED_REPOSITORY_CONTEXT_KEYS as _SERVER_OWNED_REPOSITORY_CONTEXT_KEYS,
    _strip_raw_repository_authority as _strip_raw_repository_authority,
)


from orchestrator.services.job_create_ingress import (  # noqa: E402
    strip_public_job_reserved_markers as _strip_public_job_reserved_markers,
)


from orchestrator.services.job_create_ingress import (  # noqa: E402
    strip_raw_officer_claim_context as _strip_raw_officer_claim_context,
)


async def _grant_project_ids(*args: Any, **kwargs: Any) -> Any:
    return await grant_enforcement.grant_project_ids(
        *args, **kwargs, dependencies=_grant_enforcement_dependencies()
    )


async def _strip_save_grants(*args: Any, **kwargs: Any) -> Any:
    return await grant_enforcement.strip_save_grants(
        *args, **kwargs, dependencies=_grant_enforcement_dependencies()
    )


async def _enforce_expert_save_prelude(*args: Any, **kwargs: Any) -> Any:
    return await grant_enforcement.enforce_expert_save_prelude(
        *args, **kwargs, dependencies=_grant_enforcement_dependencies()
    )


async def _enforce_expert_save(*args: Any, **kwargs: Any) -> Any:
    return await grant_enforcement.enforce_expert_save(
        *args, **kwargs, dependencies=_grant_enforcement_dependencies()
    )


async def _resolve_runner_grants(*args: Any, **kwargs: Any) -> Any:
    return await grant_enforcement.resolve_runner_grants(
        *args, **kwargs, dependencies=_grant_enforcement_dependencies()
    )


async def _enforce_dispatch_grants(*args: Any, **kwargs: Any) -> Any:
    return await grant_enforcement.enforce_dispatch_grants(
        *args, **kwargs, dependencies=_grant_enforcement_dependencies()
    )


async def _enforce_session_create_grants(*args: Any, **kwargs: Any) -> Any:
    return await grant_enforcement.enforce_session_create_grants(
        *args, **kwargs, dependencies=_grant_enforcement_dependencies()
    )


async def _enforce_job_create_grants(*args: Any, **kwargs: Any) -> Any:
    return await grant_enforcement.enforce_job_create_grants(
        *args, **kwargs, dependencies=_grant_enforcement_dependencies()
    )


def _vm_permission_dependencies() -> vm_workspace_policy.VmPermissionDependencies:
    return vm_workspace_policy.VmPermissionDependencies(store=postgres_db)


async def _check_vm_permission(*args: Any, **kwargs: Any) -> Any:
    return await vm_workspace_policy.check_vm_permission(
        *args, **kwargs, dependencies=_vm_permission_dependencies()
    )


async def _enforce_job_workspace_upgrade_grants(*args: Any, **kwargs: Any) -> Any:
    return await grant_enforcement.enforce_job_workspace_upgrade_grants(
        *args, **kwargs, dependencies=_grant_enforcement_dependencies()
    )


from orchestrator.services.vm_workspace_policy import (  # noqa: E402
    vm_needs_release as _vm_needs_release,
)


# Threads with a suspend currently in flight. Two triggers can race on the
# same thread within a second (e.g. the disconnect watchdog and the agent's
# own status→ended PUT); without this guard the loser found the workspace
# already suspended, misread it as a failure, and deleted the agent pod a
# second time (knowledge-base/knowledge/issues/session_silent_failure_audit.md #13).
_threads_suspending: set[str] = set()


async def _inject_model_credentials(*args: Any, **kwargs: Any) -> Any:
    return await dispatch_credentials.inject_model_credentials(
        *args, **kwargs, dependencies=_dispatch_credential_dependencies()
    )


async def _inject_env_key_credentials(*args: Any, **kwargs: Any) -> Any:
    return await dispatch_credentials.inject_env_key_credentials(
        *args, **kwargs, dependencies=_dispatch_credential_dependencies()
    )


async def _inject_search_credentials(*args: Any, **kwargs: Any) -> Any:
    return await dispatch_credentials.inject_search_credentials(
        *args, **kwargs, dependencies=_dispatch_credential_dependencies()
    )


async def _inject_system_kb_embedding_profile(*args: Any, **kwargs: Any) -> Any:
    return await dispatch_credentials.inject_system_kb_embedding_profile(
        *args, **kwargs, dependencies=_dispatch_credential_dependencies()
    )


async def _inject_thread_dispatch_credentials(*args: Any, **kwargs: Any) -> Any:
    return await dispatch_credentials.inject_thread_dispatch_credentials(
        *args, **kwargs, dependencies=_dispatch_credential_dependencies()
    )


from orchestrator.services.dispatch_credentials import (  # noqa: E402
    dispatch_llm_provider_fallback as _dispatch_llm_provider_fallback,
)


def _job_dispatch_dependencies() -> job_dispatcher.JobDispatchDependencies:
    """Bind dispatch scheduling to this application's collaborators.

    Rebuilt per trigger (and once per lifespan for the periodic loop), so the
    deployment flags and collaborators are the ones current at that moment;
    the dispatch state is the one application-owned object.
    """

    return job_dispatcher.JobDispatchDependencies(
        state=_job_dispatch_state,
        store=postgres_db,
        completion_control_boundary=_completion_control_boundary,
        agent_provisioner=agent_provisioner,
        vm_provisioner=vm_provisioner,
        container_provisioner=container_provisioner,
        docker_provisioner=docker_provisioner,
        workspace_suspension=workspace_suspension_service,
        auto_assign_enabled=AUTO_ASSIGN_ENABLED,
        stateless_worker_enabled=STATELESS_WORKER_ENABLED,
        manifest_execution_service=_manifest_execution_service,
        prepare_job_workspace_runtime=_prepare_job_workspace_runtime,
        fail_subjob_and_unblock_parent=_fail_subjob_and_unblock_parent,
        check_vm_permission=_check_vm_permission,
        fail_vm_parked_job=_fail_vm_parked_job,
        job_needs_sandbox=_job_needs_sandbox,
        provision_parent_workspace_for_scholar=_provision_parent_workspace_for_scholar,
        prepare_job_repository_before_claim=_prepare_job_repository_before_claim,
        job_delivery_operations=_job_delivery_operations,
    )


def _trigger_dispatch() -> None:
    """The application's dispatch trigger. Safe to call from any endpoint.

    Fire-and-forget and leader-gated (M1): see
    ``services/job_dispatcher.trigger_dispatch`` (R1.B11).
    """
    job_dispatcher.trigger_dispatch(dependencies=_job_dispatch_dependencies())


# =============================================================================
# Pydantic Models for Job Management
# =============================================================================


from orchestrator.schemas.job_create import (  # noqa: E402
    JobCreate,
    PublicJobCreateBody,
)


class CustomJSONEncoder(json.JSONEncoder):
    """JSON encoder that handles PostgreSQL types."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, datetime):
            # Ensure timestamps include UTC indicator for proper browser parsing
            if obj.tzinfo is None:
                # Naive datetime - assume UTC and add Z suffix
                return obj.isoformat() + "Z"
            else:
                # Timezone-aware - convert to UTC and use Z suffix
                utc_dt = obj.astimezone(timezone.utc)
                return utc_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        if isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


class CustomJSONResponse(JSONResponse):
    """JSON response that uses custom encoder."""

    def render(self, content: Any) -> bytes:
        return json.dumps(
            content,
            cls=CustomJSONEncoder,
            ensure_ascii=False,
            allow_nan=False,
            indent=None,
            separators=(",", ":"),
        ).encode("utf-8")


def _bind_officer_wake_metering() -> None:
    """Bind this application's store to its usage ledger for Officer wakes.

    The daily-ceiling brake runs inside the session-wake drain, which every
    caller reaches with only the store. The provider reads this module's
    ``usage_ledger`` per check, so a ledger built later in startup (or never,
    without the audit tier) is seen exactly as the former application lookup
    saw it (R1.B10 caller closure: ``session_wake`` no longer imports ``main``).
    """
    bind_officer_wake_metering(postgres_db, lambda: usage_ledger)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    global _shutdown_event
    global _persistent_thread_recycler

    # Reordering is an execution mode of durable completion commands, never a
    # standalone legacy-path feature. The admission bit is persisted, so this
    # process flag changes only fresh commands. Reject the invalid combination
    # before opening either database so a bad rollout fails loudly and
    # side-effect free.
    if COMPLETION_STATUS_REORDER_ENABLED and not COMPLETION_COMMANDS_ENABLED:
        logger.error(
            "COMPLETION_STATUS_REORDER_ENABLED requires COMPLETION_COMMANDS_ENABLED"
        )
        sys.exit(1)

    # Hard-fail if the legacy LLM_BASE_URL env var is set. The env-var-driven
    # routing for self-hosted "Local" group models was removed in chunk 6 of
    # the models_yaml_removal work. Operators currently relying on it must
    # migrate to a helm-seeded llm_endpoints row + catalog rows referencing
    # it. ERROR + sys.exit(1) (not WARN + ignore) because the var being set
    # with no consumer is an active misconfiguration that won't self-heal —
    # the legacy code path silently fell through to api.openai.com with
    # `not-needed` (the bug captured in knowledge-base/knowledge/llm_routing_issues.md).
    if os.getenv("LLM_BASE_URL"):
        logger.error(
            "LLM_BASE_URL is set but no longer honoured. Self-hosted models "
            "must now be configured via Admin → Providers (system endpoint) "
            "+ Admin → Models (catalog row) or via "
            "helm.llm.seed.systemEndpoints[]. Unset LLM_BASE_URL and seed "
            "the endpoint in helm to migrate. See "
            "knowledge-base/knowledge/features/models_yaml_removal.md."
        )
        sys.exit(1)

    # Connect to databases
    await postgres_db.connect()
    await vector_db.connect()
    if os.getenv("COLLABORA_ENABLED", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        if await warm_collabora_discovery():
            logger.info("Collabora discovery cache warmed")
        else:
            logger.warning(
                "Collabora is enabled but discovery is unavailable; "
                "Office Canvas capability remains dark"
            )

    # Audit DB is the non-load-bearing observability tier: a connect failure
    # must NOT abort startup (unlike the control-plane + vector DBs above).
    # Log loudly, then degrade — product flow survives the audit store's outage.
    audit_ready = False
    if audit_db is None:
        logger.info(
            "Audit DB disabled (AUDIT_POSTGRES_* unset) — Postgres audit store "
            "inactive; audit writes and reads no-op until it is configured."
        )
    else:
        try:
            await audit_db.connect()
            audit_ready = True
        except Exception:
            logger.exception(
                "Audit DB connect failed — continuing without the audit store. "
                "Check AUDIT_POSTGRES_* and the srw-auditdb server."
            )

    # Audit reads are served by the Postgres AuditStore (audit_reader is bound to
    # it at construction). Connect its read pool when the tier is present; a
    # connect failure leaves is_available=False -> the endpoints' degraded shapes,
    # never fatal (non-load-bearing tier).
    if audit_db is not None:
        await audit_store.connect()
        logger.info("Audit reads served by Postgres AuditStore")

    # Apply pending migrations on each DB. Each PostgresDB instance is
    # bound to its migrations directory at construction time; the runner
    # serializes via pg_advisory_xact_lock and refuses to proceed on
    # checksum drift or a dirty row from a prior failure (see
    # knowledge-base/knowledge/db_migration.md §Operational runbook for repair steps).
    await postgres_db.apply_migrations()
    from orchestrator.services.manifest_experts import (
        installed_srw_image,
        migrate_stored_experts,
        seed_bundled_expert_manifests,
    )

    postgres_db.manifest_runtime_image = installed_srw_image()
    postgres_db.manifest_skills_provider = _gather_in_scope_skills
    await migrate_stored_experts(postgres_db)
    managed_defaults = await seed_managed_default_experts(
        postgres_db, _get_config_dir()
    )
    await seed_bundled_expert_manifests(postgres_db, _get_config_dir())
    from orchestrator.services.manifest_projects import migrate_projects

    await migrate_projects(postgres_db)
    postgres_db.manifests_ready = True
    logger.info(
        "Managed expert defaults ready: worker=%s session=%s",
        managed_defaults.get("worker"),
        managed_defaults.get("session"),
    )
    await vector_db.apply_migrations()
    if audit_db is not None and audit_ready:
        try:
            await audit_db.apply_migrations()
        except Exception:
            logger.exception(
                "Audit DB migrations failed — disabling audit store for this "
                "process (non-load-bearing)."
            )
            audit_ready = False
    logger.info("Database migrations applied")

    # Promote the legacy Tavily secret before any bundled SearXNG seed. This
    # preserves Tavily as primary on upgrades and leaves SearXNG available for
    # the optional fallback slot.
    try:
        from orchestrator.seed.llm_config import ensure_tavily_search_endpoint

        if await ensure_tavily_search_endpoint(postgres_db):
            logger.info("Tavily search provider registered from TAVILY_API_KEY")
    except Exception:
        logger.warning("ensure_tavily_search_endpoint failed at startup", exc_info=True)

    # Auto-wire the ElevenLabs TTS provider when ELEVENLABS_API_KEY is present,
    # so the read-aloud voice provider appears with no manual Admin step (same
    # pattern as the codex proxy). Best-effort: never blocks startup.
    try:
        from orchestrator.seed.llm_config import ensure_elevenlabs_tts_endpoint

        if await ensure_elevenlabs_tts_endpoint(postgres_db):
            logger.info("ElevenLabs TTS provider registered from ELEVENLABS_API_KEY")
    except Exception:
        logger.warning(
            "ensure_elevenlabs_tts_endpoint failed at startup", exc_info=True
        )

    # Pin a default for each required capability that has catalog rows but no
    # pin (bare-metal init.py rows, installs upgraded with rows never pinned).
    # Best-effort: logs and leaves the pin to the admin on failure.
    await readiness_service.try_auto_pin_required_defaults(postgres_db)

    # Usage-metering ledger (Slice 4). Writes go to the auditdb usage_events
    # table (None → no-op when the audit tier is absent); rates resolve against
    # the app-DB usage_rates table created by the migration above. Built here so
    # both pools + the schema are ready. Emitters (compute / LLM materialization)
    # and /api/usage read this singleton.
    global usage_ledger
    audit_usage_pool = audit_db.pool if (audit_db is not None and audit_ready) else None
    canonical_usage_rates = UsageRates(postgres_db.pool)
    usage_ledger = UsageLedger(
        audit_usage_pool,
        canonical_usage_rates,
    )
    _bind_officer_wake_metering()
    # Rollup over the ledger (Phase 6 / D-1): aggregates the auditdb usage_events
    # firehose into the app-DB usage_daily mirror (+ rollup_state watermark) and
    # serves /api/usage from it for closed days, raw for the open tail. Same
    # availability posture as the ledger (needs both pools).
    global usage_rollup
    usage_rollup = UsageRollup(
        audit_db.pool if (audit_db is not None and audit_ready) else None,
        postgres_db.pool,
        usage_ledger,
    )
    global usage_cloud_estimator
    usage_cloud_estimator = CloudCostEstimator(postgres_db.pool)

    # Infrastructure metering paths are independently gated and additionally
    # schema-probed. Both DBs are probed after migrations because app/audit
    # migration order must never be inferred from one process's startup order.
    # Heal the rolling current+2 partition window before the one-shot probe. If
    # a pod first restarts after a UTC month boundary, waiting for the later
    # maintenance task would otherwise freeze Slice 0 unavailable until another
    # restart even though maintenance successfully creates the missing leaf.
    # Infrastructure metering: which paths this process runs is decided once
    # here, after both databases migrated (R1.B11 moved the bootstrap to its
    # domain; this module keeps the state the reporting routes read).
    metering = await bootstrap_infrastructure_metering(
        app_pool=postgres_db.pool,
        audit_usage_pool=audit_usage_pool,
        usage_ledger=usage_ledger,
        canonical_usage_rates=canonical_usage_rates,
        lifecycle_identity_authenticated=lambda: (
            nats_bridge.lifecycle_identity_authenticated
        ),
    )
    metering_capabilities = metering.capabilities
    global infrastructure_metering_settings, infrastructure_usage_v2
    global infrastructure_usage_rollup, infrastructure_inventory_store
    global infrastructure_ingestion_service
    global infrastructure_workspace_cutover, infrastructure_usage_materializer
    global infrastructure_usage_day_sealer, infrastructure_metering_runtime
    global infrastructure_coverage_waivers
    global infrastructure_storage_assets
    global infrastructure_storage_mapping
    global infrastructure_compute_activation
    global infrastructure_compute_scope_diagnostics
    global infrastructure_durable_compute_activation_keys
    global infrastructure_durable_reporting_policy_ready
    global infrastructure_storage_source_activation_ready
    infrastructure_metering_settings = metering.infrastructure_metering_settings
    infrastructure_usage_v2 = metering.infrastructure_usage_v2
    infrastructure_usage_rollup = metering.infrastructure_usage_rollup
    infrastructure_inventory_store = metering.infrastructure_inventory_store
    infrastructure_ingestion_service = metering.infrastructure_ingestion_service
    infrastructure_workspace_cutover = metering.infrastructure_workspace_cutover
    infrastructure_usage_materializer = metering.infrastructure_usage_materializer
    infrastructure_usage_day_sealer = metering.infrastructure_usage_day_sealer
    infrastructure_metering_runtime = metering.infrastructure_metering_runtime
    infrastructure_coverage_waivers = metering.infrastructure_coverage_waivers
    infrastructure_storage_assets = metering.infrastructure_storage_assets
    infrastructure_storage_mapping = metering.infrastructure_storage_mapping
    infrastructure_compute_activation = metering.infrastructure_compute_activation
    infrastructure_compute_scope_diagnostics = (
        metering.infrastructure_compute_scope_diagnostics
    )
    infrastructure_durable_compute_activation_keys = (
        metering.infrastructure_durable_compute_activation_keys
    )
    infrastructure_durable_reporting_policy_ready = (
        metering.infrastructure_durable_reporting_policy_ready
    )
    infrastructure_storage_source_activation_ready = (
        metering.infrastructure_storage_source_activation_ready
    )

    # Idempotent data backfills (R1.B11: moved to services/startup_backfills).
    await run_startup_backfills(postgres_db)

    # Wire the model registry's catalog lookup to the DB. The registry lives
    # in src/core/ and must not import orchestrator/, so the hook is injected
    # here (and unset on shutdown below). custom/system lookups were retired
    # along with user_llm_endpoint_models — the catalog covers both scopes.
    from shared.runtime.core.model_registry import register_catalog_lookup

    register_catalog_lookup(postgres_db.resolve_catalog_model)

    # Share the selected audit reader + the app DB with graph_routes.
    set_audit_reader(audit_reader)
    set_postgres_db(postgres_db)

    # Initialize Gitea workspace delivery (graceful if unavailable)
    await gitea_client.ensure_initialized()

    # Configure Gitea OIDC auth source (graceful if unconfigured)
    await gitea_client.ensure_oidc_configured()

    # Initialize Keycloak group sync (graceful if unavailable)
    await keycloak_groups.ensure_initialized()

    # Adopt the exact durable backend installation before any main-cloud
    # effect. The legacy system_settings row is read only as one-time input
    # when 0186 has no active instance yet; afterward the immutable instance
    # snapshot + singleton CAS pointer are the sole routing authority.
    try:
        _persisted_overlay = await postgres_db.get_system_setting("main_cloud")
    except Exception as _e:
        logger.warning("Legacy main cloud overlay read failed at startup: %s", _e)
        _persisted_overlay = None
    try:
        await initialize_main_cloud_instance_authority(
            postgres_db,
            main_cloud_router,
            legacy_overlay=_persisted_overlay,
        )
        await preload_retained_main_cloud_instances(postgres_db, main_cloud_router)
        if _persisted_overlay is not None:
            try:
                await postgres_db.delete_system_setting("main_cloud")
            except Exception:
                logger.warning(
                    "Failed to remove inert legacy main_cloud setting",
                    exc_info=True,
                )
    except Exception as _e:
        # Main cloud is optional for the rest of the orchestrator, but an
        # unbound env adapter must never become a fallback routing authority.
        # It remains uninitialized and every cloud effect fails closed.
        logger.error(
            "Main cloud installation authority is unavailable; cloud effects "
            "remain disabled: %s",
            _e,
        )

    # Issue 5: warn loudly if the *active* backend's required secrets are not
    # present in the env — it is silently running on built-in DEV credentials
    # and will fail at the first cloud call. Non-fatal (graceful-degradation
    # convention + local/dev stacks legitimately set their own secrets), but no
    # longer silent. The PUT/test endpoints refuse this at swap time; this
    # catches a Helm-misconfigured deployment that booted straight into it.
    try:
        from orchestrator.services.cloud.config import (
            missing_secret_envs,
            warn_main_cloud_missing_secret_config,
        )

        _active_id = main_cloud_router.active.backend_id
        _missing_secrets = missing_secret_envs(_active_id, _persisted_overlay)
        warn_main_cloud_missing_secret_config(_missing_secrets, logger=logger)
    except Exception as _e:
        logger.debug("Main cloud secret presence check skipped at startup: %s", _e)

    # Sudo gate: DB-connect UNCONDITIONALLY — vm_upgrade approval requests are
    # pure DB rows (NULL reply subject) and their REST decision surface must
    # work without NATS. The NATS bridge below re-connects the gate WITH the
    # NATS handle when available (live sudo_command daemon requests need it).
    # Pre-fix, no NATS meant every /api/sudo/* endpoint 404'd and the
    # vm_upgrade freeze never even created its approval row.
    sudo_gate.connect(db=postgres_db)

    # Initialize NATS bridge for VM lifecycle (graceful if unavailable)
    await nats_bridge.connect(db=postgres_db, on_vm_ready=_trigger_dispatch)

    # Initialize S3 snapshot service (graceful if S3 not configured)
    await snapshot_service.connect(db=postgres_db)

    # One loud, early signal when either object-store seam is unconfigured,
    # replacing the scattered late failures (virtual-session dispatch, snapshot
    # no-ops). Fail-closed (raise, crash-loop) when OBJECT_STORE_REQUIRED is
    # set; warn-only otherwise. knowledge-history/done/s3_object_store_bundled_fallback.md.
    _store_warning = _check_object_store_config()
    if _store_warning:
        logger.warning(_store_warning)

    # Initialize VM provisioner (uses NATS if available, else direct K8s)
    vm_provisioner.connect(db=postgres_db, snapshot_service=snapshot_service)

    # Initialize container provisioner for workspace containers (direct K8s)
    container_provisioner.connect(db=postgres_db, snapshot_service=snapshot_service)

    # Initialize Docker Compose provisioner (static workspace pool, used when k8s unavailable)
    docker_provisioner.connect(db=postgres_db, snapshot_service=snapshot_service)

    # Log deployment mode.
    # Priority: K8s in-cluster → Docker Compose → K8s via kubeconfig.
    # A local kubeconfig should not shadow Docker Compose when running outside the cluster.
    if container_provisioner.is_available and container_provisioner.in_cluster:
        logger.info(
            "Deployment mode: KUBERNETES (in-cluster) — dynamic provisioning via k8s API"
        )
    elif docker_provisioner.is_available:
        logger.info(
            "Deployment mode: DOCKER COMPOSE — static workspace pool (%s)",
            ",".join(docker_provisioner.workspace_hosts),
        )
        if container_provisioner.is_available:
            logger.info(
                "Deployment mode: Kubernetes also reachable via kubeconfig "
                "but Docker Compose takes priority (not running in-cluster)"
            )
    elif container_provisioner.is_available:
        logger.info(
            "Deployment mode: KUBERNETES (kubeconfig) — dynamic provisioning via k8s API"
        )
    else:
        logger.warning(
            "Deployment mode: NO WORKSPACE PROVISIONER — "
            "neither k8s API nor WORKSPACE_HOSTS available. "
            "Jobs requiring workspaces will fail."
        )

    # Initialize IDE session service
    ide_session_service.connect(
        db=postgres_db,
        snapshot_service=snapshot_service,
        vm_provisioner=vm_provisioner,
        gitea_client=gitea_client,
        container_provisioner=container_provisioner,
    )

    # Initialize persistent agent provisioner (legacy, kept for backward compat)
    persistent_provisioner.connect(db=postgres_db)

    async def _persistent_recycle_failure_page(
        project_id: str, thread_id: str, failure_class: str
    ) -> bool:
        thread = await postgres_db.get_thread(thread_id)
        if not thread or str(thread.get("project_id") or "") != project_id:
            return False
        return await _dispatch_officer_page(
            thread,
            thread_id,
            category="officer_runtime",
            dedup_key=(
                f"officer_recycle:{thread_id}:{failure_class}:"
                f"{datetime.now(timezone.utc).date().isoformat()}"
            ),
            subject="Officer runtime recycle requires attention",
            message_md=(
                "The dedicated Officer runtime is held while its bounded "
                f"recycle retries (`{failure_class}`). Durable Post, thread, "
                "and queued wakes remain intact."
            ),
        )

    async def _persistent_recycle_complete(project_id: str, thread_id: str) -> None:
        if project_id:
            _kick_officer_event_drain(postgres_db)

    _persistent_thread_recycler = PersistentThreadRecycler(
        db=postgres_db,
        provisioner=persistent_provisioner,
        failure_notifier=_persistent_recycle_failure_page,
        on_complete=_persistent_recycle_complete,
    )

    # Initialize unified agent provisioner (on-demand pods for jobs + sessions)
    agent_provisioner.connect(db=postgres_db)

    # Initialize workspace suspension service (idle timeout → S3 snapshot → pod deletion)
    workspace_suspension_service.connect(
        db=postgres_db,
        snapshot_service=snapshot_service,
        container_provisioner=container_provisioner,
        docker_provisioner=docker_provisioner,
        vm_provisioner=vm_provisioner,
        agent_provisioner=agent_provisioner,
    )

    # Initialize IDE proxy service. Kubernetes coordinates are freshly
    # control-plane-attested, VM browser relay is contained pending a guest
    # tunnel, and only explicit local-Docker targets may use its short cache.
    ide_proxy_service.connect(
        db=postgres_db,
        container_provisioner=container_provisioner,
        vm_provisioner=vm_provisioner,
    )

    # Initialize notification feed (SSE broadcast for cockpit)
    from orchestrator.services.notification_feed import notification_feed

    # Initialize notification service (unified dispatcher for email + webhooks)
    notification_service.connect(
        db=postgres_db,
        email_service=email_service,
        notification_feed=notification_feed,
    )
    # The sitrep's optional sections read these three handles; bind them once
    # here rather than letting that module reach back into this one (R1.B07
    # caller closure). Every entry point still accepts explicit overrides.
    sitrep_service.bind_reporting_handles(
        sitrep_service.ReportingHandles(
            audit_reader=audit_reader,
            usage_ledger=usage_ledger,
            vector_db=vector_db,
        )
    )
    # Unified feed: bind (category, action) handlers and source loaders.
    notification_action_service.register_notification_actions(
        dependencies=_notification_action_dependencies()
    )

    # Initialize IMAP poller for email reply routing (graceful if unconfigured)
    async def _imap_reply_handler(
        job_id: str,
        thread_id: str,
        message: str,
        sender_email: str | None = None,
        email_message_id: str | None = None,
    ) -> str:
        """Adapter: strips the sequence number from the reply funnel's return.

        The poller outlives this request, so it carries its collaborators
        explicitly rather than looking anything up later.
        """
        strategy, _seq = await inbound_reply_service.route_inbound_reply(
            job_id,
            thread_id,
            message,
            sender_email=sender_email,
            email_message_id=email_message_id,
            dependencies=_inbound_reply_dependencies(),
        )
        return strategy

    imap_poller.connect(db=postgres_db, reply_handler=_imap_reply_handler)

    # Start background tasks
    _shutdown_event = asyncio.Event()
    # Leader election (M1): this replica contends for the singleton-loop
    # leadership lock; the run_when_leader-wrapped loops below run only while
    # this replica holds it. See services/leader_election.py.
    from orchestrator.database.lock_ids import LEADER_ID
    from orchestrator.services.checkpoint_retention import run_retention_sweeper
    from orchestrator.services.datasource_reconciliation import (
        run_datasource_project_reconciler,
    )
    from orchestrator.services.leader_election import (
        get_leader_generation,
        is_leader,
        run_as_leader,
        run_when_leader,
    )

    async def _strict_datasource_sync(
        project_id: str, datasource: dict[str, Any]
    ) -> None:
        # Build the dependency value inside the call: `vector_db` is rebound
        # during this same lifespan, so a value captured when the reconciler
        # was constructed would be the unconnected pool.
        await knowledge_projection_operations.sync_datasource_knowledge(
            project_id,
            datasource,
            strict=True,
            dependencies=_knowledge_projection_dependencies(),
        )

    async def _strict_datasource_delete(project_id: str, datasource_id: str) -> None:
        await knowledge_projection_operations.delete_datasource_knowledge(
            project_id,
            datasource_id,
            strict=True,
            dependencies=_knowledge_projection_dependencies(),
        )

    async def _allocate_metering_generation(conn: Any) -> int:
        generation = await conn.fetchval(
            "UPDATE infra_metering_control "
            "SET leader_generation=leader_generation+1, "
            "updated_at=statement_timestamp() "
            "WHERE singleton=TRUE RETURNING leader_generation"
        )
        if generation is None:
            raise RuntimeError("infrastructure metering control row is missing")
        return int(generation)

    # Allocate the infrastructure fencing token on the exact advisory-lock
    # session, before is_leader becomes visible to any singleton loop. This
    # gives collector, cutover, publisher, and sealer one shared tenure token.
    metering_generation_callback = (
        _allocate_metering_generation
        if metering_capabilities.slice1_inventory_ready
        else None
    )
    leader_task = asyncio.create_task(
        run_as_leader(
            postgres_db,
            LEADER_ID,
            _shutdown_event,
            on_acquired=metering_generation_callback,
        )
    )

    async def _inventory_generation_coro(stop: asyncio.Event) -> None:
        generation = get_leader_generation()
        if generation is None:
            raise RuntimeError("metering leader generation is unavailable")
        await run_inventory_generation_loop(
            stop,
            infrastructure_inventory_store,
            generation=generation,
            cleanup_interval_seconds=(
                infrastructure_metering_settings.cleanup_interval_seconds
            ),
            snapshot_item_retention=timedelta(
                days=(infrastructure_metering_settings.snapshot_item_retention_days)
            ),
            diagnostic_retention=timedelta(
                days=infrastructure_metering_settings.diagnostic_retention_days
            ),
        )

    infrastructure_inventory_generation_task = (
        asyncio.create_task(
            run_when_leader(
                _inventory_generation_coro,
                _shutdown_event,
            )
        )
        if infrastructure_inventory_store is not None
        else None
    )

    infrastructure_metering_runtime_task = (
        asyncio.create_task(
            run_when_leader(
                lambda stop: infrastructure_metering_runtime_loop(
                    stop,
                    infrastructure_metering_runtime,
                    get_leader_generation,
                ),
                _shutdown_event,
            )
        )
        if infrastructure_metering_runtime is not None
        else None
    )
    datasource_reconciliation_task = asyncio.create_task(
        run_datasource_project_reconciler(
            postgres_db,
            _shutdown_event,
            is_leader.is_set,
            sync_fn=_strict_datasource_sync,
            delete_fn=_strict_datasource_delete,
        )
    )
    stale_detector_task = asyncio.create_task(
        run_when_leader(
            functools.partial(
                stale_agent_detector_service.stale_agent_detector,
                dependencies=_stale_agent_detector_dependencies(),
            ),
            _shutdown_event,
        )
    )
    token_cleanup_task = asyncio.create_task(
        cleanup_expired_tokens(postgres_db, _shutdown_event)
    )
    session_cleanup_task = asyncio.create_task(
        cleanup_expired_sessions(postgres_db, _shutdown_event)
    )
    dispatcher_task = asyncio.create_task(
        run_when_leader(
            functools.partial(
                job_dispatcher.auto_assign_dispatcher,
                dependencies=_job_dispatch_dependencies(),
            ),
            _shutdown_event,
        )
    )
    vm_readiness_task = (
        asyncio.create_task(
            run_when_leader(
                lambda shutdown: vm_readiness_prober(
                    shutdown,
                    db=postgres_db,
                    provisioner=vm_provisioner,
                    trigger_dispatch=_trigger_dispatch,
                ),
                _shutdown_event,
            )
        )
        if os.getenv("VM_MODE", "off").strip().lower() == "same-cluster"
        else None
    )
    vm_workspace_recovery_settings = VMWorkspaceRecoverySettings.from_env()
    vm_creation_retry_task = (
        asyncio.create_task(
            run_when_leader(
                VMCreationRetryService(postgres_db, vm_provisioner).run,
                _shutdown_event,
            ),
            name="vm-creation-retry",
        )
        if os.getenv("VM_MODE", "off").strip().lower() == "same-cluster"
        else None
    )
    vm_workspace_recovery_store = VMWorkspaceRecoveryStore(postgres_db)
    vm_workspace_recovery_task = (
        asyncio.create_task(
            run_when_leader(
                VMWorkspaceRecoveryService.from_settings(
                    vm_workspace_recovery_store,
                    vm_provisioner,
                    settings=vm_workspace_recovery_settings,
                ).run,
                _shutdown_event,
            ),
            name="vm-workspace-recovery",
        )
        if automatic_reconciler_enabled()
        else None
    )
    sudo_sweeper_task = asyncio.create_task(
        sudo_expiration_sweeper(
            _shutdown_event,
            gate=sudo_gate,
            # Built per tick inside the sweeper's own isolated try.
            fail_expired_vm_upgrade_jobs=lambda: (
                _job_control_operations().fail_expired_vm_upgrade_jobs()
            ),
        )
    )
    thread_events_prune_task = asyncio.create_task(
        retention_sweepers.thread_events_prune_sweeper(
            _shutdown_event, store=postgres_db
        )
    )
    # Stateless-lane lease reaper (stateless_agents.md §5.2): leader-gated on
    # its OWN advisory lock (RUN_QUEUE_REAPER_ID — not run_when_leader, so the
    # sweep can survive a main-leader handover independently); per-row CAS
    # steals + turn.interrupted/turn.parked journal frames.
    from orchestrator.services.run_queue_reaper import run_queue_reaper_loop

    run_queue_reaper_task = asyncio.create_task(
        run_queue_reaper_loop(postgres_db, _shutdown_event)
    )
    # Pod-deletion-cost reconciler (capacity_ux_and_queue_autoscaling.md §2):
    # leader-gated on its OWN advisory lock (STATELESS_DELETION_COST_ID), it
    # stamps lease-holding stateless pods expensive-to-delete so an HPA
    # scale-down removes idle executors first. Off-cluster it idles.
    from orchestrator.services.stateless_pod_deletion_cost import (
        reconciler_enabled as _deletion_cost_reconciler_enabled,
        stateless_pod_deletion_cost_loop,
    )

    stateless_deletion_cost_task = (
        asyncio.create_task(
            stateless_pod_deletion_cost_loop(postgres_db, _shutdown_event),
            name="stateless-pod-deletion-cost",
        )
        if _deletion_cost_reconciler_enabled()
        else None
    )
    # Stateless turn memory is its own transactional-outbox ownership domain.
    # It is always resident and never hidden behind completion-command flags or
    # advisory leadership: row leases serialize replicas and survive handover.
    session_memory_effect_task = asyncio.create_task(
        _session_memory_runtime.drain().run_drain(_shutdown_event),
        name="session-memory-effect-drain",
    )
    # Gate-3 completion drain uses its own observable River-style lease row;
    # it must never be wrapped in the orchestrator advisory-leader helper.
    # Keep the finalizer/router module imports dark while the gate is closed.
    completion_finalizer_task = None
    completion_sweep_router_task = None
    # Queue age is a worker-availability signal, so the monitor must remain
    # alive when fresh worker admission or Gate-3 commands are disabled.
    # Its commands-off sampler is explicitly run_queue-only.
    completion_monitor_task = asyncio.create_task(
        _completion_runtime.monitor().run(_shutdown_event),
        name="completion-monitor",
    )
    from shared.cloud_push_tasks import enabled as cloud_push_recovery_enabled

    if COMPLETION_COMMANDS_ENABLED or cloud_push_recovery_enabled():
        completion_finalizer = _completion_runtime.finalizer()

        async def cloud_push_sweep():
            from orchestrator.services.cloud_push_recovery import (
                sweep_stale_cloud_pushes,
            )

            return await sweep_stale_cloud_pushes(postgres_db)

        completion_finalizer_task = asyncio.create_task(
            completion_finalizer.run_drain(
                _shutdown_event,
                drain_commands=COMPLETION_COMMANDS_ENABLED,
                background_sweep=cloud_push_sweep
                if cloud_push_recovery_enabled()
                else None,
            ),
            name="completion-finalizer-drain",
        )
    if COMPLETION_COMMANDS_ENABLED:
        completion_sweep_router_task = asyncio.create_task(
            _completion_runtime.sweep_router().run(_shutdown_event),
            name="completion-sweep-router",
        )
    security_events_prune_task = asyncio.create_task(
        retention_sweepers.security_events_prune_sweeper(
            _shutdown_event, store=postgres_db
        )
    )
    # Not leader-gated, matching security_events_prune_task above: a
    # delete-by-age is idempotent, so two replicas racing it is harmless —
    # the second finds nothing.
    ssh_attachments_prune_task = asyncio.create_task(
        retention_sweepers.ssh_attachments_prune_sweeper(
            _shutdown_event, store=postgres_db
        )
    )
    # In-flight checkpoint retention: bound every live thread's LangGraph
    # checkpoints to the newest N while it runs (leader-gated), so a long job
    # can't fill the checkpointer PVC before it terminates.
    checkpoint_retention_task = asyncio.create_task(
        run_retention_sweeper(postgres_db, _shutdown_event, is_leader.is_set)
    )
    headless_notify_task = asyncio.create_task(
        run_when_leader(
            functools.partial(
                session_attention_operations.thread_permission_notify_sweeper,
                dependencies=_session_attention_dependencies(),
            ),
            _shutdown_event,
        )
    )
    # Leader-gated: both snapshot/teardown idle workspaces (attention-sleep) or
    # delete idle IDE VMs/pods (ide-sweeper) after a plain SELECT, with no
    # per-row claim. Under replicas:2 two unguarded copies would double-snapshot
    # to the same S3 key and race teardown against an in-flight snapshot. Gating
    # mirrors the lifecycle reconciler, which already owns the parallel idle
    # workspace-teardown path. See knowledge-base/knowledge/tests/orchestrator_ha_background_loop_sweep.md.
    attention_sleep_task = asyncio.create_task(
        run_when_leader(
            functools.partial(
                session_attention_operations.attention_sleep_sweeper,
                dependencies=_session_attention_dependencies(),
            ),
            _shutdown_event,
        )
    )
    # Officer (centurion) lifecycle: implicit-timer filing, overdue kicks,
    # rate-limited respawn. Leader-gated — respawn must be single-flight.
    officer_watchdog_task = asyncio.create_task(
        run_when_leader(
            functools.partial(
                officer_watchdog_service.officer_watchdog,
                dependencies=_officer_watchdog_dependencies(),
            ),
            _shutdown_event,
        )
    )
    # Worker-message route reconciler (officer_message_routing.md §5.2):
    # officer-SLA escalation, the total blocking timeout, and delivery repair.
    # Leader-gated — per-route CAS gives exactly-once, the gate keeps N
    # replicas from redundantly scanning and double-dispatching user emails.
    from orchestrator.services.message_route_reconciler import (
        message_route_reconciler_loop,
    )

    message_route_reconciler_task = asyncio.create_task(
        run_when_leader(
            lambda ev: message_route_reconciler_loop(
                postgres_db,
                ev,
                resume_job=lambda *args, **kwargs: (
                    _job_control_operations().internal_resume_job(*args, **kwargs)
                ),
            ),
            _shutdown_event,
        )
    )
    # Officer auto-pull tick (officer_backlog_pools.md §5): fill a pool's free
    # slot from its ready, categorized, unclaimed tickets. Leader-gated as an
    # optimization only — correctness is the advisory-locked claim+create
    # transaction plus uq_jobs_active_ticket_claim, because dual-leader windows
    # are real. Dormant until a century sets officer.auto_pull (ships off).
    from orchestrator.services.officer_backlog import officer_backlog_tick_loop

    officer_backlog_task = asyncio.create_task(
        run_when_leader(
            lambda ev: officer_backlog_tick_loop(
                postgres_db,
                vector_db,
                ev,
                release_enabled=OFFICER_AUTO_PULL_RELEASE_ENABLED,
                provision_repo=_provision_officer_ticket_repo,
                trigger_dispatch=_trigger_dispatch,
                enforce_grants=(
                    lambda *args, **kwargs: (
                        project_loop_spawn_service.enforce_officer_ticket_grants(
                            *args,
                            **kwargs,
                            dependencies=_project_loop_dependencies(),
                        )
                    )
                ),
                usage_ledger=usage_ledger,
                notify=notify_officer,
            ),
            _shutdown_event,
        )
    )
    ide_sweeper_task = asyncio.create_task(
        run_when_leader(
            functools.partial(
                ide_session_ttl_sweeper, ide_sessions=ide_session_service
            ),
            _shutdown_event,
        )
    )
    ws_sweeper_task = asyncio.create_task(
        workspace_idle_sweeper(
            _shutdown_event,
            store=postgres_db,
            provisioner=container_provisioner,
            suspension=workspace_suspension_service,
            vm_idle_service_factory=_build_vm_idle_service,
            terminal_vm_controls_factory=_job_mutation_operations,
        )
    )
    # Leader-gated: serially SSH-dials every active workspace and captures IDE
    # profiles to per-user S3 keys — two replicas would double-dial each
    # workspace and race the signature-gated capture.
    ide_settings_sweeper_task = asyncio.create_task(
        run_when_leader(
            functools.partial(
                code_server_settings_sweeper,
                db=postgres_db,
                container_provisioner=container_provisioner,
                snapshot_service=snapshot_service,
                vm_provisioner=vm_provisioner,
            ),
            _shutdown_event,
        )
    )
    gc_sweeper_task = asyncio.create_task(
        snapshot_gc_sweeper(_shutdown_event, snapshots=snapshot_service)
    )
    pinned_create_intent_reconciler_task = asyncio.create_task(
        run_when_leader(
            functools.partial(
                pinned_k8s_reconciliation_service.pinned_agent_create_intent_reconciler,
                dependencies=_pinned_k8s_reconciliation_dependencies(),
            ),
            _shutdown_event,
        )
    )
    pinned_create_fence_gc_task = asyncio.create_task(
        run_when_leader(
            functools.partial(
                pinned_k8s_reconciliation_service.pinned_k8s_create_fence_gc_sweeper,
                dependencies=_pinned_k8s_reconciliation_dependencies(),
            ),
            _shutdown_event,
        )
    )
    imap_task = asyncio.create_task(
        run_when_leader(
            functools.partial(imap_poll_loop, poller=imap_poller), _shutdown_event
        )
    )
    # Unified feed: run the deferred channel steps ("mail after the officer's
    # window unless seen/resolved", quiet-hours deferrals, batched digests).
    notification_steps_task = asyncio.create_task(
        run_when_leader(
            lambda stop: notification_steps_loop(
                stop, postgres_db, notification_service
            ),
            _shutdown_event,
        )
    )
    delegation_timeout_task = asyncio.create_task(
        run_when_leader(
            functools.partial(
                completion_recovery_operations.delegation_timeout_sweeper,
                dependencies=_completion_recovery_dependencies(),
                interval_seconds=60,
            ),
            _shutdown_event,
        )
    )
    # Re-dispatch worker jobs paused for a transient LLM outage once their
    # backoff timer is due (fail-loud past the give-up ceiling). Leader-gated —
    # per-row CAS + run_when_leader keep N replicas from double-dispatching.
    # knowledge-base/knowledge/features/llm_outage_pause_and_backoff_redispatch.md
    llm_outage_task = asyncio.create_task(
        run_when_leader(
            functools.partial(
                completion_recovery_operations.llm_outage_redispatch_sweeper,
                dependencies=_completion_recovery_dependencies(),
                interval_seconds=float(
                    (os.getenv("LLM_OUTAGE_SWEEP_SECONDS") or "").strip() or 30
                ),
            ),
            _shutdown_event,
        )
    )
    infra_transient_task = asyncio.create_task(
        run_when_leader(
            functools.partial(
                completion_recovery_operations.infra_transient_redispatch_sweeper,
                dependencies=_completion_recovery_dependencies(),
                interval_seconds=float(
                    (os.getenv("INFRA_TRANSIENT_SWEEP_SECONDS") or "").strip() or 30
                ),
            ),
            _shutdown_event,
        )
    )
    pool_reconciler_task = asyncio.create_task(
        run_when_leader(
            functools.partial(agent_pool_reconciler, provisioner=agent_provisioner),
            _shutdown_event,
        )
    )
    # Cleanup authority is independent of fresh protected-mode admission. A
    # feature/config disable must never strand an already durable reader or
    # pre-dispatch effect intent.
    ro_reader_reconciler_task = asyncio.create_task(
        run_when_leader(
            functools.partial(
                ro_reader_reconciler_loop,
                store=postgres_db,
                # Read per tick, as the module global always was.
                router=lambda: main_cloud_router,
            ),
            _shutdown_event,
        )
    )
    automation_cron_task = asyncio.create_task(
        cron_dispatcher_loop(
            postgres_db,
            _shutdown_event,
            on_job_created=_trigger_dispatch,
            # The loop outlives every request, so it carries the provisioning
            # adapter explicitly (R1.B07 caller closure).
            provision_repo=_provision_cron_job_repo,
        )
    )
    # Safety-net for project self-improvement loops: recover any loop whose
    # current job went terminal without the completion hook advancing it.
    project_loop_sweeper_task = asyncio.create_task(
        project_loop_sweeper_loop(
            postgres_db,
            _shutdown_event,
            advance_fn=(
                lambda *args, **kwargs: (
                    project_loop_advance_service.advance_project_loop(
                        *args,
                        **kwargs,
                        dependencies=_project_loop_dependencies(),
                    )
                )
            ),
            **(
                {
                    "completion_commands_enabled": True,
                    "reconcile_handoff_fn": (_reconcile_atomic_project_loop_handoff),
                }
                if COMPLETION_COMMANDS_ENABLED
                else {}
            ),
        )
    )
    # Reap orphaned verification (critic) subjobs that would otherwise linger as
    # priority-10 dispatchable jobs and parasitically preempt real work. See
    # knowledge-history/done/preemption_before_first_checkpoint_replays_job_opening.md.
    stale_verification_sweeper_task = asyncio.create_task(
        stale_verification_sweeper_loop(
            postgres_db,
            _shutdown_event,
            stateless_cancel_fn=(
                postgres_db.cancel_and_settle_stale_stateless_verification_subjob
            ),
            completion_commands_enabled=COMPLETION_COMMANDS_ENABLED,
        )
    )
    # Backstop for session wakes: deliver any completion notice whose
    # opportunistic post-commit send was lost, or whose terminal path has no
    # hook at all. Deliberately NOT run_when_leader — single-firing comes from
    # the row claim, which works from every replica, and leader-gating would
    # make this a SPOF across a handover. See services/session_wake.py.
    session_wake_sweeper_task = asyncio.create_task(
        session_wake_sweeper_loop(postgres_db, _shutdown_event)
    )

    # Slice-3 KB index freshness sweep: catch out-of-band vault edits (human
    # pushes, recovered partial reindexes) the post-merge trigger can't see.
    # Leader-gated — two replicas replace-note-chunks'ing the same KB would
    # interleave delete+insert batches. The store rides the vector pool; the
    # embedding service is catalog-re-resolved per tick inside the loop.
    def _kb_sweeper_coro(ev: asyncio.Event):
        from shared.runtime.services.knowledge_store import KnowledgeStore

        from orchestrator.services.kb_reindex import kb_reindex_sweeper_loop

        return kb_reindex_sweeper_loop(
            postgres_db,
            KnowledgeStore(db=vector_db, embedding_service=None),
            gitea_client,
            ev,
            embedding_service_factory=(
                lambda: knowledge_index_operations.build_kb_embedding_service(
                    dependencies=_knowledge_index_dependencies()
                )
            ),
        )

    kb_reindex_sweeper_task = asyncio.create_task(
        run_when_leader(_kb_sweeper_coro, _shutdown_event)
    )

    # LLM $/token pricing sync: seed usage_rates from OpenRouter × the model
    # catalog (params_json.pricing_id) so record_events can cost the audit-
    # sourced token rows. Slow (6h) + change-only; no-op without the app pool.
    pricing_sync_task = asyncio.create_task(
        llm_pricing_sync_loop(
            _shutdown_event, postgres_db.pool, postgres_db.list_models
        )
    )

    # Public-cloud comparison prices: AWS/Azure publish machine-readable list
    # prices. Refresh change-only once per day; STACKIT's PDF-backed reference
    # card is source-labelled and seeded by app migration 0082.
    cloud_pricing_sync_task = asyncio.create_task(
        cloud_pricing_sync_loop(_shutdown_event, postgres_db.pool)
    )

    # Workspace compute metering (Slice 4b): materialize CLOSED workspace
    # intervals into the usage ledger + reconcile leaked opens. Self-disables
    # when the app pool or ledger is absent (non-load-bearing tier).
    workspace_metering_task = asyncio.create_task(
        run_when_leader(
            lambda stop: workspace_metering.workspace_metering_loop(
                stop,
                postgres_db,
                usage_ledger,
                lambda owner_kind, owner_id: workspace_metering_attribution(
                    owner_kind, owner_id, store=postgres_db
                ),
            ),
            _shutdown_event,
        )
    )

    # LLM usage materialization (Slice 4c): materialize audit llm_requests into
    # usage ledger rows. Self-disables when audit/app pools or the ledger are absent.
    # R1.B05 lane C moved the loop body beside the work it drives
    # (`services/audit_usage.py`, like the pricing and metering loops).
    # B11 still owns the scheduling; only the call shape changed.
    llm_usage_task = asyncio.create_task(
        audit_usage.llm_usage_poll_loop(
            _shutdown_event,
            audit_db=audit_db,
            app_store=postgres_db,
            usage_ledger=usage_ledger,
            logger=logger,
        )
    )

    # Usage rollup (Phase 6 / D-1): re-aggregate closed days from the auditdb
    # usage_events firehose into the app-DB usage_daily mirror + advance the
    # rollup_state watermark. Leader-only (the upsert is idempotent, but there's
    # no value in every replica re-aggregating); self-disables without both pools.
    usage_rollup_task = asyncio.create_task(
        run_when_leader(lambda se: usage_rollup_loop(se, usage_rollup), _shutdown_event)
    )

    # Typed v2 bootstrap/dirty-day reconciliation is also leader-owned and
    # non-load-bearing. It runs while the public read gate is off so operators
    # can enable v2 only after the durable bootstrap state reports complete.
    infrastructure_usage_rollup_task = (
        asyncio.create_task(
            run_when_leader(
                lambda se: typed_usage_rollup_loop(se, infrastructure_usage_rollup),
                _shutdown_event,
            )
        )
        if infrastructure_usage_rollup is not None
        else None
    )

    # Audit-store partition maintenance (creation + ANALYZE + lookahead alarms;
    # retention deferred — see services/audit_partitions.py). Only when the
    # audit DB is configured; otherwise the store is inactive and there is
    # nothing to maintain.
    audit_maintenance_task = (
        asyncio.create_task(audit_maintenance_loop(audit_db.pool, _shutdown_event))
        if (audit_db is not None and audit_ready)
        else None
    )

    # Unified instance lifecycle reconciler (drift-based draining and,
    # in future phases, crash recovery + cross-kind primitives). Runs
    # peer to agent_pool_reconciler — pool owns capacity, lifecycle
    # owns version/health.
    lifecycle_reconciler = InstanceLifecycleReconciler()
    lifecycle_reconciler.register(
        AgentInstanceManager(provisioner=agent_provisioner, db=postgres_db)
    )
    lifecycle_reconciler.register(
        PersistentAgentInstanceManager(
            provisioner=persistent_provisioner,
            db=postgres_db,
            recycler=_persistent_thread_recycler,
            automatic_enabled=PERSISTENT_AGENT_RECONCILIATION_ENABLED,
        )
    )
    lifecycle_reconciler.register(
        WorkspaceInstanceManager(
            container_provisioner=container_provisioner,
            suspension_service=workspace_suspension_service,
            snapshot_service=snapshot_service,
            db=postgres_db,
            completion_commands_enabled=COMPLETION_COMMANDS_ENABLED,
            completion_router=(
                _completion_runtime.sweep_router()
                if COMPLETION_COMMANDS_ENABLED
                else None
            ),
        )
    )
    lifecycle_reconciler.register(
        VMInstanceManager(
            vm_provisioner=vm_provisioner,
            suspension_service=workspace_suspension_service,
            snapshot_service=snapshot_service,
            db=postgres_db,
            completion_commands_enabled=COMPLETION_COMMANDS_ENABLED,
            completion_router=(
                _completion_runtime.sweep_router()
                if COMPLETION_COMMANDS_ENABLED
                else None
            ),
        )
    )
    # Startup reconciliation: rebuild the in-memory view from K8s
    # before the heartbeat endpoint starts accepting traffic. Phase 1b
    # logs the discovered pod set; future phases may also flag DB-row
    # divergence and reap pods that lack a registration.
    try:
        startup_pods = await lifecycle_reconciler.managers[0].list_pods()
        logger.info(
            "Lifecycle startup: discovered %d agent pod(s) from K8s",
            len(startup_pods),
        )
    except Exception:
        logger.exception("Lifecycle startup reconciliation failed (non-fatal)")
    lifecycle_reconciler_task = asyncio.create_task(
        run_when_leader(
            lambda se: lifecycle_reconciler_loop(se, lifecycle_reconciler),
            _shutdown_event,
        )
    )

    # Phase 4: main-cloud config LISTEN task — reacts to pg_notify when
    # an admin PUTs a new config via /api/admin/system-settings/main_cloud.
    async def _main_cloud_reload_callback() -> None:
        await _reload_from_db_and_swap(postgres_db, main_cloud_router)

    main_cloud_listen_task = asyncio.create_task(
        run_listen_loop(postgres_db, _main_cloud_reload_callback, _shutdown_event)
    )

    yield

    # Signal shutdown to background tasks
    _shutdown_event.set()
    await leader_task
    if infrastructure_inventory_generation_task is not None:
        await infrastructure_inventory_generation_task
    await datasource_reconciliation_task
    await stale_detector_task
    await token_cleanup_task
    await session_cleanup_task
    await dispatcher_task
    if vm_readiness_task is not None:
        await vm_readiness_task
    if vm_creation_retry_task is not None:
        await vm_creation_retry_task
    if vm_workspace_recovery_task is not None:
        await vm_workspace_recovery_task
    await sudo_sweeper_task
    await thread_events_prune_task
    await run_queue_reaper_task
    if stateless_deletion_cost_task is not None:
        await stateless_deletion_cost_task
    await session_memory_effect_task
    if completion_finalizer_task is not None:
        await completion_finalizer_task
    if completion_sweep_router_task is not None:
        await completion_sweep_router_task
    await completion_monitor_task
    await security_events_prune_task
    await ssh_attachments_prune_task
    await checkpoint_retention_task
    await headless_notify_task
    await attention_sleep_task
    await officer_watchdog_task
    await message_route_reconciler_task
    await officer_backlog_task
    await ide_sweeper_task
    await ws_sweeper_task
    await ide_settings_sweeper_task
    await gc_sweeper_task
    await pinned_create_intent_reconciler_task
    await pinned_create_fence_gc_task
    await imap_task
    await notification_steps_task
    await delegation_timeout_task
    await llm_outage_task
    await infra_transient_task
    await pool_reconciler_task
    if ro_reader_reconciler_task is not None:
        await ro_reader_reconciler_task
    await lifecycle_reconciler_task
    await main_cloud_listen_task
    await automation_cron_task
    await project_loop_sweeper_task
    await stale_verification_sweeper_task
    await session_wake_sweeper_task
    await kb_reindex_sweeper_task
    await pricing_sync_task
    await cloud_pricing_sync_task
    await workspace_metering_task
    await llm_usage_task
    await usage_rollup_task
    if infrastructure_usage_rollup_task is not None:
        await infrastructure_usage_rollup_task
    if infrastructure_metering_runtime_task is not None:
        await infrastructure_metering_runtime_task
    if audit_maintenance_task is not None:
        await audit_maintenance_task

    # Initial/manual datasource reindexes are request-spawned rather than loop
    # tasks. Cancel them before closing git/vector clients; the source context
    # removes temporary repositories and auth material in its cancellation path.
    await kb_datasource_tasks.drain()

    # Cleanup clients
    await nats_bridge.disconnect()
    await vm_provisioner.disconnect()
    await gitea_client.close()

    # Unregister the registry's DB hook before disconnecting the pool so
    # any stragglers don't hit a closed connection.
    from shared.runtime.core.model_registry import register_catalog_lookup

    register_catalog_lookup(None)

    # Disconnect from databases
    await vector_db.disconnect()
    if audit_store is not None:
        await audit_store.disconnect()
    if audit_db is not None:
        await audit_db.disconnect()
    await postgres_db.disconnect()
    _completion_runtime.reset()
    _session_memory_runtime.reset()


app = FastAPI(
    title="Debug Cockpit API",
    description="Backend API for the Superhuman Remote Worker Cockpit",
    version="0.1.0",
    lifespan=lifespan,
    default_response_class=CustomJSONResponse,
)

app.state.catalogue_resources = CatalogueResources(config_dir=lambda: _get_config_dir())
app.state.contacts_dependencies = ContactsDependencies(db=postgres_db)
# The application database, for routers that live outside this module and
# previously reached it with a function-local ``from orchestrator.main import
# postgres_db`` (auth BFF, product capabilities). One named collaborator, not a
# service registry (R1.B02 caller-boundary closure).
app.state.store = postgres_db
app.state.job_reads_dependencies_factory = lambda: _job_reads_dependencies()
app.state.provider_catalog_dependencies_factory = (
    lambda: _provider_catalog_dependencies()
)
app.state.model_catalog_dependencies_factory = lambda: _model_catalog_dependencies()
app.state.config_catalog_dependencies_factory = lambda: _config_catalog_dependencies()
app.state.manifest_dependencies_factory = lambda: _manifest_dependencies()
app.state.job_inspection_dependencies_factory = lambda: _job_inspection_dependencies()
app.state.job_audit_dependencies_factory = lambda: _job_audit_dependencies()
app.state.job_artifacts_dependencies_factory = lambda: _job_artifacts_dependencies()
app.state.diagnostics_dependencies_factory = lambda: _diagnostics_dependencies()
app.state.capacity_dependencies_factory = lambda: _capacity_dependencies()
app.state.identity_dependencies_factory = lambda: _identity_dependencies()
app.state.access_token_dependencies_factory = lambda: _access_token_dependencies()
app.state.ssh_access_dependencies_factory = lambda: _ssh_access_dependencies()
app.state.usage_reporting_dependencies_factory = lambda: _usage_reporting_dependencies()
app.state.infrastructure_admin_dependencies_factory = (
    lambda: _infrastructure_admin_dependencies()
)
app.state.provider_credentials_dependencies_factory = (
    lambda: _provider_credentials_dependencies()
)
app.state.subscription_management_dependencies_factory = (
    lambda: _subscription_management_dependencies()
)
app.state.voice_dependencies_factory = lambda: _voice_dependencies()
app.state.system_settings_dependencies_factory = lambda: _system_settings_dependencies()
app.state.user_administration_dependencies_factory = (
    lambda: _user_administration_dependencies()
)
app.state.job_diagnostics_dependencies_factory = lambda: _job_diagnostics_dependencies()
app.state.datasources_dependencies_factory = lambda: _datasources_dependencies()
app.state.projects_dependencies_factory = lambda: _projects_dependencies()
app.state.knowledge_dependencies_factory = lambda: _knowledge_dependencies()
app.state.citations_dependencies_factory = lambda: _citations_dependencies()
app.state.media_dependencies_factory = lambda: _media_dependencies()
app.state.ide_dependencies_factory = lambda: _ide_dependencies()
app.state.workspace_access_dependencies_factory = (
    lambda: _workspace_access_dependencies()
)
app.state.thread_files_dependencies_factory = lambda: _thread_files_dependencies()
app.state.job_repo_dependencies_factory = lambda: _job_repo_dependencies()
app.state.job_diff_dependencies_factory = lambda: _job_diff_dependencies()
app.state.job_review_dependencies_factory = lambda: _job_review_dependencies()
app.state.agent_cloud_stage_dependencies_factory = (
    lambda: _agent_cloud_stage_dependencies()
)
app.state.thread_cloud_diff_dependencies_factory = (
    lambda: _thread_cloud_diff_dependencies()
)
app.state.main_cloud_settings_dependencies_factory = (
    lambda: _main_cloud_settings_dependencies()
)
app.state.expert_catalog_state = ExpertCatalogState()
app.state.thread_workspace_delivery_dependencies_factory = (
    lambda: _thread_workspace_delivery_dependencies()
)


# --------------------------------------------------------------------------- #
# R1.B06 lane A — the pinned attach surface.
#
# Five of these fields are bound to main's OWN bridges rather than to the
# service functions directly, and that is deliberate: each one is a call made
# from a *different* function in the same module, and the attach suites steer
# them by patching ``orchestrator.main``. Resolving them in-module would make
# those patches green but inert. ``successor_tasks`` stays main's dict for the
# same reason — the port contract forbids a lane creating its own registry.
# --------------------------------------------------------------------------- #
def _session_attach_binding_dependencies() -> (
    session_attach_binding_service.SessionAttachBindingDependencies
):
    return session_attach_binding_service.SessionAttachBindingDependencies(
        store=postgres_db,
        gitea_client=gitea_client,
        agent_provisioner=agent_provisioner,
        persistent_provisioner=persistent_provisioner,
        reserve_pinned_warm_agent_binding=reserve_pinned_warm_agent_binding,
        release_pinned_warm_binding_protection=release_pinned_warm_binding_protection,
        await_protected_cloud_runtime_ready=_await_protected_cloud_runtime_ready,
        prepare_thread_repository_authority=prepare_thread_repository_authority,
        assemble_session_attach_payload=(
            lambda *args, **kwargs: (
                session_attach_payload.assemble_session_attach_payload(
                    *args,
                    **kwargs,
                    dependencies=_session_attach_payload_dependencies(),
                )
            )
        ),
        schedule_attach_abort_successor=_schedule_attach_abort_successor,
        prepare_pinned_session_mutation_target=_prepare_pinned_session_mutation_target,
        pinned_session_mutation_target_is_current=(
            _pinned_session_mutation_target_is_current
        ),
        reserve_session_attach_binding=_reserve_session_attach_binding,
        release_session_attach_binding=_release_session_attach_binding,
        send_session_attach_locked=_send_session_attach_locked,
    )


def _sessions_dependencies() -> sessions_routes.SessionsDependencies:
    """Compose the two ``/api/sessions`` endpoints' collaborators.

    ``_await_late_cloud_setup`` stays an injected callable: it reads this
    module's in-process late-setup task registry, which is a composition
    concern rather than a session-admission one. The rest reach their owning
    services directly.
    """
    return sessions_routes.SessionsDependencies(
        store=postgres_db,
        agent_provisioner=agent_provisioner,
        container_provisioner=container_provisioner,
        workspace_suspension_service=workspace_suspension_service,
        session_router=session_router,
        session_tokens=session_tokens,
        ensure_session_workspace=ensure_session_workspace,
        await_late_cloud_setup=lambda thread_id: (
            _thread_resume_operations().await_late_cloud_setup(thread_id)
        ),
        await_protected_cloud_runtime_ready=(
            lambda thread_id, **kwargs: (
                protected_cloud_engage._await_protected_cloud_runtime_ready(
                    thread_id,
                    **kwargs,
                    dependencies=_protected_cloud_engage_dependencies(),
                )
            )
        ),
        session_grant_violations=(
            lambda *args, **kwargs: session_config_resolution.session_grant_violations(
                *args, **kwargs, dependencies=_session_config_dependencies()
            )
        ),
        session_endpoint_violations=(
            lambda *args, **kwargs: (
                session_config_resolution.session_endpoint_violations(
                    *args, **kwargs, dependencies=_session_config_dependencies()
                )
            )
        ),
        find_idle_persistent_agent=(
            lambda: session_attach_binding_service.find_idle_persistent_agent(
                dependencies=_session_attach_binding_dependencies()
            )
        ),
        send_session_attach=(
            lambda *args, **kwargs: (
                session_attach_binding_service.send_session_attach(
                    *args, **kwargs, dependencies=_session_attach_binding_dependencies()
                )
            )
        ),
    )


def _provision_or_assign_dependencies() -> (
    provision_or_assign_service.ProvisionOrAssignDependencies
):
    """Compose the create-path binder's collaborators.

    Every callable below reaches its owning service directly, carrying that
    service's own dependency object built at call time. Routing them through
    this module's compatibility wrappers instead would put a second hop in
    the path for no decision — the wrappers are there for *inbound* callers
    of ``main``, not for services calling one another.
    """
    return provision_or_assign_service.ProvisionOrAssignDependencies(
        store=postgres_db,
        agent_provisioner=agent_provisioner,
        await_protected_cloud_runtime_ready=(
            lambda thread_id: (
                protected_cloud_engage._await_protected_cloud_runtime_ready(
                    thread_id, dependencies=_protected_cloud_engage_dependencies()
                )
            )
        ),
        session_grant_violations=(
            lambda *args, **kwargs: session_config_resolution.session_grant_violations(
                *args, **kwargs, dependencies=_session_config_dependencies()
            )
        ),
        session_endpoint_violations=(
            lambda *args, **kwargs: (
                session_config_resolution.session_endpoint_violations(
                    *args, **kwargs, dependencies=_session_config_dependencies()
                )
            )
        ),
        find_idle_persistent_agent=(
            lambda: session_attach_binding_service.find_idle_persistent_agent(
                dependencies=_session_attach_binding_dependencies()
            )
        ),
        send_session_attach=(
            lambda *args, **kwargs: (
                session_attach_binding_service.send_session_attach(
                    *args, **kwargs, dependencies=_session_attach_binding_dependencies()
                )
            )
        ),
    )


async def _provision_or_assign(*args: Any, **kwargs: Any) -> None:
    """Schedule-time entry point for the create-path binder.

    Session admission and attach-abort recovery both hand this to their own
    dependency object, so neither service has to know how the binder's
    collaborators are built.
    """
    await provision_or_assign_service.provision_or_assign(
        *args, **kwargs, dependencies=_provision_or_assign_dependencies()
    )


def _session_attach_recovery_dependencies() -> (
    session_attach_recovery_service.SessionAttachRecoveryDependencies
):
    return session_attach_recovery_service.SessionAttachRecoveryDependencies(
        store=postgres_db,
        container_provisioner=container_provisioner,
        docker_provisioner=docker_provisioner,
        workspace_suspension_service=workspace_suspension_service,
        ensure_session_workspace=ensure_session_workspace,
        thread_project_ids=_thread_project_ids,
        reconcile_attach_abort_successor=_reconcile_attach_abort_successor,
        provision_or_assign=_provision_or_assign,
        successor_tasks=_attach_abort_successor_tasks,
    )


def _pinned_session_mutation_target_dependencies() -> (
    pinned_session_mutation_target_service.PinnedSessionMutationTargetDependencies
):
    return (
        pinned_session_mutation_target_service.PinnedSessionMutationTargetDependencies(
            store=postgres_db,
            agent_provisioner=agent_provisioner,
            persistent_provisioner=persistent_provisioner,
            attest_pinned_session_mutation_pod=_attest_pinned_session_mutation_pod,
            pinned_session_mutation_target_is_current=(
                _pinned_session_mutation_target_is_current
            ),
        )
    )


def _commissioned_officer_dependencies() -> (
    commissioned_officer_provisioning_service.CommissionedOfficerDependencies
):
    return commissioned_officer_provisioning_service.CommissionedOfficerDependencies(
        store=postgres_db,
        persistent_provisioner=persistent_provisioner,
        emit_session_provisioning_failure=_emit_session_provisioning_failure,
    )


async def _bind_registered_persistent_agent(
    thread_id: str,
    agent_id: str,
    expected_agent_id: str | None,
    expected_runtime_generation: str,
) -> str | None:
    return await session_attach_binding_service.bind_registered_persistent_agent(
        thread_id,
        agent_id,
        expected_agent_id,
        expected_runtime_generation,
        dependencies=_session_attach_binding_dependencies(),
    )


async def _find_idle_persistent_agent() -> Optional[dict]:
    return await session_attach_binding_service.find_idle_persistent_agent(
        dependencies=_session_attach_binding_dependencies(),
    )


async def _send_session_attach(
    agent: dict,
    thread_id: str,
    config_override: Optional[dict] = None,
    project_ids: Optional[list] = None,
    datasources: Optional[list] = None,
    config_name: Optional[str] = None,
    expected_runtime_generation: str | None = None,
) -> bool:
    return await session_attach_binding_service.send_session_attach(
        agent,
        thread_id,
        config_override,
        project_ids,
        datasources,
        config_name,
        expected_runtime_generation,
        dependencies=_session_attach_binding_dependencies(),
    )


async def _reserve_session_attach_binding(
    agent_id: str, thread_id: str, *, expected_runtime_generation: str
) -> str | None:
    return await session_attach_binding_service.reserve_session_attach_binding(
        agent_id,
        thread_id,
        expected_runtime_generation=expected_runtime_generation,
        dependencies=_session_attach_binding_dependencies(),
    )


async def _release_session_attach_binding(
    agent_id: str,
    thread_id: str,
    *,
    expected_runtime_generation: str,
    expected_attach_token: str,
    pre_delivery: bool = False,
    expected_agent_pod_uid: str | None = None,
    local_runtime_quiesced: bool = False,
    local_quiescence_protocol: str | None = None,
    workspace_generation: str | None = None,
    workspace_runtime_incarnation: str | None = None,
) -> SessionAttachReleaseOutcome:
    return await session_attach_binding_service.release_session_attach_binding(
        agent_id,
        thread_id,
        expected_runtime_generation=expected_runtime_generation,
        expected_attach_token=expected_attach_token,
        pre_delivery=pre_delivery,
        expected_agent_pod_uid=expected_agent_pod_uid,
        local_runtime_quiesced=local_runtime_quiesced,
        local_quiescence_protocol=local_quiescence_protocol,
        workspace_generation=workspace_generation,
        workspace_runtime_incarnation=workspace_runtime_incarnation,
        dependencies=_session_attach_binding_dependencies(),
    )


async def _acknowledge_retiring_failed_attach(
    agent_id: str,
    thread_id: str,
    *,
    expected_runtime_generation: str,
    expected_attach_token: str,
    expected_agent_pod_uid: str,
    local_quiescence_protocol: str,
    workspace_generation: str | None,
    workspace_runtime_incarnation: str | None,
) -> bool:
    return await session_attach_binding_service.acknowledge_retiring_failed_attach(
        agent_id,
        thread_id,
        expected_runtime_generation=expected_runtime_generation,
        expected_attach_token=expected_attach_token,
        expected_agent_pod_uid=expected_agent_pod_uid,
        local_quiescence_protocol=local_quiescence_protocol,
        workspace_generation=workspace_generation,
        workspace_runtime_incarnation=workspace_runtime_incarnation,
        dependencies=_session_attach_binding_dependencies(),
    )


async def _send_session_attach_locked(
    agent: dict,
    thread_id: str,
    config_override: Optional[dict] = None,
    project_ids: Optional[list] = None,
    datasources: Optional[list] = None,
    config_name: Optional[str] = None,
    expected_runtime_generation: str | None = None,
) -> bool:
    return await session_attach_binding_service.send_session_attach_locked(
        agent,
        thread_id,
        config_override,
        project_ids,
        datasources,
        config_name,
        expected_runtime_generation,
        dependencies=_session_attach_binding_dependencies(),
    )


async def _reconcile_attach_abort_successor(candidate: Mapping[str, Any]) -> bool:
    return await session_attach_recovery_service.reconcile_attach_abort_successor(
        candidate,
        dependencies=_session_attach_recovery_dependencies(),
    )


def _schedule_attach_abort_successor(
    thread_id: str,
    *,
    retired_runtime_generation: str,
    retired_attach_token: str,
    retired_agent_id: str,
) -> "asyncio.Task[None]":
    return session_attach_recovery_service.schedule_attach_abort_successor(
        thread_id,
        retired_runtime_generation=retired_runtime_generation,
        retired_attach_token=retired_attach_token,
        retired_agent_id=retired_agent_id,
        dependencies=_session_attach_recovery_dependencies(),
    )


async def _attest_pinned_session_mutation_pod(*, binding: PinnedSessionBinding) -> bool:
    return (
        await pinned_session_mutation_target_service.attest_pinned_session_mutation_pod(
            binding=binding,
            dependencies=_pinned_session_mutation_target_dependencies(),
        )
    )


async def _prepare_pinned_session_mutation_target(
    *, thread_id: str, agent_id: str, runtime_generation: str, attach_token: str
) -> _PinnedSessionMutationTarget | None:
    return await pinned_session_mutation_target_service.prepare_pinned_session_mutation_target(
        thread_id=thread_id,
        agent_id=agent_id,
        runtime_generation=runtime_generation,
        attach_token=attach_token,
        dependencies=_pinned_session_mutation_target_dependencies(),
    )


async def _pinned_session_mutation_target_is_current(
    target: _PinnedSessionMutationTarget,
) -> bool:
    return await pinned_session_mutation_target_service.pinned_session_mutation_target_is_current(
        target,
        dependencies=_pinned_session_mutation_target_dependencies(),
    )


async def _emit_session_provisioning_failure(
    thread_id: str, user_id: str | None, runtime_authority: Any | None, reason: str
) -> None:
    return await commissioned_officer_provisioning_service.emit_session_provisioning_failure(
        thread_id,
        user_id,
        runtime_authority,
        reason,
        dependencies=_commissioned_officer_dependencies(),
    )


async def _provision_commissioned_officer(
    thread_id: str, *, user_id: str, config_name: str, runtime_authority: Any
) -> None:
    return (
        await commissioned_officer_provisioning_service.provision_commissioned_officer(
            thread_id,
            user_id=user_id,
            config_name=config_name,
            runtime_authority=runtime_authority,
            dependencies=_commissioned_officer_dependencies(),
        )
    )


# --------------------------------------------------------------------------- #
# R1.B06 lane B — session admission, datasource/project authorization, config.
#
# ``_apply_thread_config_update_locked`` is injected rather than moved: the
# concurrent manifest lane created it and holds most of the old
# ``_apply_thread_config_update`` body, so it is not in B06's census.
# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #
# R1.B06 lane C — agent registration/heartbeat, child APIs, Officer runtime
# verification. Both feature gates arrive as lambdas (port contract P1): read
# at import they would freeze whatever value the process happened to hold.
# --------------------------------------------------------------------------- #
def _agent_registration_dependencies() -> (
    agent_registration_service.AgentRegistrationDependencies
):
    return agent_registration_service.AgentRegistrationDependencies(
        store=postgres_db,
        gitea_client=gitea_client,
        logger=logger,
        require_internal=require_internal,
        require_admin=_require_admin,
        is_internal_call=is_internal_call,
        log_security_event=log_security_event,
        completion_commands_enabled=lambda: COMPLETION_COMMANDS_ENABLED,
        require_pinned_status_identity=_require_pinned_status_identity,
        thread_uses_pinned_execution=_thread_uses_pinned_execution,
        thread_accepts_runtime=_thread_accepts_runtime,
        protected_cloud_delivery_state=_protected_cloud_delivery_state,
        bind_registered_persistent_agent=_bind_registered_persistent_agent,
        slide_thread_grant_on_liveness=slide_thread_grant_on_liveness,
        trigger_dispatch=_trigger_dispatch,
    )


def _agent_child_threads_dependencies() -> (
    agent_child_threads_service.AgentChildThreadDependencies
):
    return agent_child_threads_service.AgentChildThreadDependencies(
        store=postgres_db,
        gitea_client=gitea_client,
        container_provisioner=container_provisioner,
        logger=logger,
        require_internal=require_internal,
        is_experts_db_enabled=_is_experts_db_enabled,
        resolve_config=resolve_config,
        prefetch_roster_refs=_prefetch_roster_refs,
        resolve_session_account_defaults=(
            lambda *args, **kwargs: (
                session_config_resolution.resolve_session_account_defaults(
                    *args, **kwargs, dependencies=_session_config_dependencies()
                )
            )
        ),
        backend_from_override=_backend_from_override,
    )


def _officer_runtime_verification_dependencies() -> (
    officer_runtime_verification_service.OfficerRuntimeVerificationDependencies
):
    return officer_runtime_verification_service.OfficerRuntimeVerificationDependencies(
        store=postgres_db,
        logger=logger,
        require_internal=require_internal,
        require_admin=_require_admin,
        log_security_event=log_security_event,
        officer_runtime_verification_enabled=(
            lambda: OFFICER_RUNTIME_VERIFICATION_ENABLED
        ),
        authorize_runtime_actor_request=authorize_runtime_actor_request,
        refresh_runtime_actor_exchange=refresh_runtime_actor_exchange,
        create_runtime_verification_plan=create_runtime_verification_plan,
        get_runtime_verification_plan=get_runtime_verification_plan,
        transition_runtime_verification_plan=transition_runtime_verification_plan,
        kick_officer_event_drain=_kick_officer_event_drain,
    )


def _thread_datasource_authorization_dependencies() -> (
    thread_datasource_authorization_service.ThreadDatasourceAuthorizationDependencies
):
    return thread_datasource_authorization_service.ThreadDatasourceAuthorizationDependencies(
        store=postgres_db,
        thread_project_ids=_thread_project_ids,
    )


def _thread_project_authorization_dependencies() -> (
    thread_project_authorization_service.ThreadProjectAuthorizationDependencies
):
    return thread_project_authorization_service.ThreadProjectAuthorizationDependencies(
        store=postgres_db,
    )


def _thread_config_update_dependencies() -> (
    thread_config_update_service.ThreadConfigUpdateDependencies
):
    return thread_config_update_service.ThreadConfigUpdateDependencies(
        store=postgres_db,
        vm_provisioner=vm_provisioner,
        container_provisioner=container_provisioner,
        recovery_store=VMWorkspaceRecoveryStore(postgres_db),
        apply_thread_config_update_locked=_apply_thread_config_update_locked,
        enforce_workspace_upgrade_grants=(
            lambda *args, **kwargs: (
                grant_enforcement.enforce_workspace_upgrade_grants(
                    *args, **kwargs, dependencies=_grant_enforcement_dependencies()
                )
            )
        ),
        require_internal=require_internal,
        require_thread_owner=require_thread_owner,
    )


def _thread_admission_dependencies() -> (
    thread_admission_service.ThreadAdmissionDependencies
):
    return thread_admission_service.ThreadAdmissionDependencies(
        store=postgres_db,
        gitea_client=gitea_client,
        main_cloud_router=main_cloud_router,
        agent_provisioner=agent_provisioner,
        container_provisioner=container_provisioner,
        docker_provisioner=docker_provisioner,
        persistent_provisioner=persistent_provisioner,
        vm_provisioner=vm_provisioner,
        enforce_readiness_gate=_enforce_readiness_gate,
        require_approved_user=require_approved_user,
        is_experts_db_enabled=_is_experts_db_enabled,
        user_experts_enabled=_user_experts_enabled,
        datasource_defaults_on_omission=_datasource_defaults_on_omission,
        is_protected_cloud_mode_enabled=_is_protected_cloud_mode_enabled,
        authorize_thread_project_ids=_authorize_thread_project_ids,
        authorize_thread_datasource_selection=_authorize_thread_datasource_selection,
        resolve_session_account_defaults=(
            lambda *args, **kwargs: (
                session_config_resolution.resolve_session_account_defaults(
                    *args, **kwargs, dependencies=_session_config_dependencies()
                )
            )
        ),
        prefetch_roster_refs=_prefetch_roster_refs,
        resolve_thread_execution_lane=(
            lambda *args, **kwargs: (
                session_class_policy.resolve_thread_execution_lane(
                    *args, **kwargs, dependencies=_execution_lane_dependencies()
                )
            )
        ),
        build_thread_mount_rows=(
            lambda *args, **kwargs: thread_mount_rows.build_thread_mount_rows(
                *args, **kwargs, dependencies=_thread_mount_dependencies()
            )
        ),
        should_skip_session_folder=_should_skip_session_folder,
        enforce_session_create_grants=_enforce_session_create_grants,
        check_vm_permission=_check_vm_permission,
        resolve_cloud_session_url=_resolve_cloud_session_url,
        validated_post_owned_officer_create_fragment=(
            lambda *args, **kwargs: (
                session_create_overrides.validated_post_owned_officer_create_fragment(
                    *args,
                    **kwargs,
                    validated_officer_post_patch=_validated_officer_post_patch,
                )
            )
        ),
        enforce_officer_auto_pull_release=_enforce_officer_auto_pull_release,
        can_manage_project_officer=(
            lambda *args, **kwargs: (
                officer_post_view_service.can_manage_project_officer(
                    *args, **kwargs, dependencies=_officer_post_view_dependencies()
                )
            )
        ),
        find_open_conference_thread=_find_open_conference_thread,
        inherit_conference_brain=officer_conference_service.inherit_conference_brain,
        hold_officer_for_conference=_hold_officer_for_conference,
        provision_commissioned_officer=_provision_commissioned_officer,
        end_thread_flow=lambda *args, **kwargs: (
            _thread_retirement_operations().end_thread_flow(*args, **kwargs)
        ),
        schedule_stateless_workspace_ensure=_schedule_stateless_workspace_ensure,
        schedule_protected_engage=_schedule_protected_engage,
        record_protected_error=_record_protected_error,
        find_idle_persistent_agent=_find_idle_persistent_agent,
        send_session_attach=_send_session_attach,
        provision_or_assign=_provision_or_assign,
        redact_thread_metadata=thread_projection_operations.redact_thread_metadata,
    )


async def _authorize_thread_datasource_selection(
    user: dict[str, Any] | None,
    datasource_ids: list[str] | None,
    *,
    workspace_backend: str | None,
    target_project_ids: list[str] | None = None,
    effective_work_owner_id: str | None = None,
    trusted_system_inheritance: bool = False,
    legacy_job_id: str | None = None,
) -> tuple[list[str], dict[str, int]]:
    return await thread_datasource_authorization_service.authorize_thread_datasource_selection(
        user,
        datasource_ids,
        workspace_backend=workspace_backend,
        target_project_ids=target_project_ids,
        effective_work_owner_id=effective_work_owner_id,
        trusted_system_inheritance=trusted_system_inheritance,
        legacy_job_id=legacy_job_id,
        dependencies=_thread_datasource_authorization_dependencies(),
    )


async def _resolve_authorized_thread_datasources(
    thread: dict[str, Any],
    datasource_ids: list[str] | None,
    *,
    target_project_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    return await thread_datasource_authorization_service.resolve_authorized_thread_datasources(
        thread,
        datasource_ids,
        target_project_ids=target_project_ids,
        dependencies=_thread_datasource_authorization_dependencies(),
    )


async def _authorize_thread_project_ids(
    user: dict[str, Any], project_ids: list[str] | None
) -> list[str]:
    return await thread_project_authorization_service.authorize_thread_project_ids(
        user,
        project_ids,
        dependencies=_thread_project_authorization_dependencies(),
    )


async def _revalidate_thread_project_ids(
    thread: dict[str, Any], project_ids: list[str] | None
) -> list[str]:
    return await thread_project_authorization_service.revalidate_thread_project_ids(
        thread,
        project_ids,
        dependencies=_thread_project_authorization_dependencies(),
    )


async def _thread_has_knowledge_scope(
    *, project_ids: list[str] | None, datasource_ids: list[str] | None
) -> bool:
    return await thread_project_authorization_service.thread_has_knowledge_scope(
        project_ids=project_ids,
        datasource_ids=datasource_ids,
        dependencies=_thread_project_authorization_dependencies(),
    )


async def create_thread(
    request_body: ThreadCreateRequest, request: Request
) -> dict[str, Any]:
    """Kept on main because the bench review path calls it directly and B07's
    ``officer_post_lifecycle`` takes it as its ``create_thread`` port; the route
    itself lives in ``routers/thread_admission``."""
    return await thread_admission_service.create_thread(
        request_body,
        request,
        dependencies=_thread_admission_dependencies(),
    )


def _unit_claim_bundle_dependencies() -> (
    unit_claim_bundle_service.UnitClaimBundleDependencies
):
    """R1.B06 root lane. The four ``*_dependencies`` entries are B05 *factories*,
    not bound operations: passing them lets the service call the B05 operations
    directly, which is what let main retire the thin wrappers it used to keep
    for `_assemble_session_attach_payload` and the stateless attestations."""
    return unit_claim_bundle_service.UnitClaimBundleDependencies(
        db=postgres_db,
        require_internal=require_internal,
        send_session_attach=_send_session_attach,
        thread_has_knowledge_scope=_thread_has_knowledge_scope,
        thread_project_ids=_thread_project_ids,
        resolve_background_push_workspace=lambda thread: (
            _thread_resume_operations().resolve_background_push_workspace(thread)
        ),
        session_attach_payload_dependencies=_session_attach_payload_dependencies,
        job_workspace_authority_dependencies=_job_workspace_authority_dependencies,
        job_start_bundle_dependencies=_job_start_bundle_dependencies,
        dispatch_credential_dependencies=_dispatch_credential_dependencies,
        recovery_store=VMWorkspaceRecoveryStore(postgres_db),
        attest_stateless_claimant=_build_stateless_claimant_attestor(),
    )


def _run_queue_admin_dependencies() -> (
    run_queue_admin_service.RunQueueAdminDependencies
):
    """R1.B06 root lane. ``COMPLETION_COMMANDS_ENABLED`` is read through a
    lambda, never captured: the port contract's P1 exists because a module that
    freezes an import-time flag answers with whatever value happened to be set
    when it was first imported."""
    return run_queue_admin_service.RunQueueAdminDependencies(
        db=postgres_db,
        require_admin=_require_admin,
        completion_commands_enabled=lambda: COMPLETION_COMMANDS_ENABLED,
        get_completion_command_resolution=_completion_runtime.command_resolution,
    )


def _agent_thread_status_dependencies() -> (
    agent_thread_status_service.AgentThreadStatusDependencies
):
    """R1.B06 root lane. B09 keeps the retirement decisions and B07 the
    conference hold; both arrive as callables so this batch never owns them."""
    return agent_thread_status_service.AgentThreadStatusDependencies(
        db=postgres_db,
        persistent_thread_recycler=_persistent_thread_recycler,
        require_internal=require_internal,
        thread_accepts_runtime=_thread_accepts_runtime,
        release_session_attach_binding=_release_session_attach_binding,
        acknowledge_retiring_failed_attach=_acknowledge_retiring_failed_attach,
        schedule_attach_abort_successor=_schedule_attach_abort_successor,
        begin_pinned_thread_retirement=_begin_pinned_thread_retirement,
        end_thread_flow=lambda *args, **kwargs: (
            _thread_retirement_operations().end_thread_flow(*args, **kwargs)
        ),
        suspend_thread_resources=lambda thread_id: (
            _thread_retirement_operations().suspend_thread_resources(thread_id)
        ),
        conclude_conference_if_any=_conclude_conference_if_any,
    )


async def _provision_cron_job_repo(job_row: dict[str, Any], db: Any) -> None:
    """The Gitea/cloud provisioning adapter the cron dispatcher is handed.

    Parity with the ``POST /api/jobs`` handler and with automation run-now; the
    dispatcher keeps it best-effort, so a Gitea outage leaves the fired job
    repo-less rather than undoing the committed fire.
    """
    from orchestrator.services.job_provisioning import provision_job_repo

    await provision_job_repo(
        job_row=job_row,
        gitea_client=gitea_client,
        postgres_db=db,
        main_cloud_router=main_cloud_router,
    )


# --- R1.B07 lane L: the project-loop engine --------------------------------
def _project_loop_dependencies() -> project_loop_spawn_service.ProjectLoopDependencies:
    """R1.B07 lane L. One dependency object for the whole loop engine. The
    completion flag and sweep router are callables (§P1) because B08 owns both
    and suites rebind them on ``main``; the knowledge reindex is B03's
    operation reached through its own dependency object."""
    return project_loop_spawn_service.ProjectLoopDependencies(
        store=postgres_db,
        vector_store=vector_db,
        notifier=notification_service,
        gitea_client=gitea_client,
        main_cloud_router=main_cloud_router,
        trigger_dispatch=_trigger_dispatch,
        kick_officer_event_drain=_kick_officer_event_drain,
        enforce_dispatch_grants=_enforce_dispatch_grants,
        reindex_project_kb=(
            lambda project_id: knowledge_index_operations.reindex_project_kb(
                project_id, dependencies=_knowledge_index_dependencies()
            )
        ),
        completion_commands_enabled=lambda: COMPLETION_COMMANDS_ENABLED,
        completion_sweep_router=_completion_runtime.sweep_router,
    )


def _curation_final_pass_dependencies() -> (
    curation_final_pass_service.CurationFinalPassDependencies
):
    return curation_final_pass_service.CurationFinalPassDependencies(
        store=postgres_db,
        trigger_dispatch=_trigger_dispatch,
        internal_resume_job=lambda *args, **kwargs: (
            _job_control_operations().internal_resume_job(*args, **kwargs)
        ),
    )


def _loop_plan_filing_dependencies() -> (
    loop_plan_filing_service.LoopPlanFilingDependencies
):
    return loop_plan_filing_service.LoopPlanFilingDependencies(
        store=postgres_db,
        vector_store=vector_db,
    )


async def _provision_officer_ticket_repo(*args: Any, **kwargs: Any) -> Any:
    """Compatibility wrapper: the officer backlog tick and the job-admission
    officer path both take this as their repo/cloud provisioning adapter."""
    return await project_loop_spawn_service.provision_officer_ticket_repo(
        *args, **kwargs, dependencies=_project_loop_dependencies()
    )


async def _reconcile_atomic_project_loop_handoff(*args: Any, **kwargs: Any) -> Any:
    """Compatibility wrapper: B11's loop sweeper takes this as its
    ``reconcile_handoff_fn``."""
    return await project_loop_advance_service.reconcile_atomic_project_loop_handoff(
        *args, **kwargs, dependencies=_project_loop_dependencies()
    )


def _automations_dependencies() -> automations_router_module.AutomationsDependencies:
    """R1.B07 caller closure: the automations router's eleven late
    ``orchestrator.main`` imports are now this one object."""
    return automations_router_module.AutomationsDependencies(
        store=postgres_db,
        gitea_client=gitea_client,
        main_cloud_router=main_cloud_router,
        trigger_dispatch=_trigger_dispatch,
    )


def _project_loops_dependencies() -> (
    project_loops_router_module.ProjectLoopsDependencies
):
    """R1.B07 caller closure: the project-loops router's nine late
    ``orchestrator.main`` imports are now this one object. The three loop
    operations are bound to the owning service with its own dependency object,
    so the router and the completion hook share one engine."""
    return project_loops_router_module.ProjectLoopsDependencies(
        store=postgres_db,
        vector_store=vector_db,
        spawn_loop_stage=(
            lambda *args, **kwargs: project_loop_spawn_service.spawn_loop_stage(
                *args, **kwargs, dependencies=_project_loop_dependencies()
            )
        ),
        writeback_loop_stage=(
            lambda *args, **kwargs: project_loop_spawn_service.writeback_loop_stage(
                *args, **kwargs, dependencies=_project_loop_dependencies()
            )
        ),
        resume_project_loop=(
            lambda *args, **kwargs: project_loop_advance_service.resume_project_loop(
                *args, **kwargs, dependencies=_project_loop_dependencies()
            )
        ),
        check_vm_permission=_check_vm_permission,
    )


# --- R1.B07 lane N: the unified notification feed --------------------------
def _notification_api_dependencies() -> (
    notification_api_service.NotificationApiDependencies
):
    return notification_api_service.NotificationApiDependencies(
        store=postgres_db,
        notifier=notification_service,
    )


def _notification_action_dependencies() -> (
    notification_action_service.NotificationActionDependencies
):
    """R1.B07 lane N. Built once at startup and captured by the registered
    closures, so every field is either a singleton the application owns for its
    whole life or a callable that resolves its own dependencies per call. The
    job-control handlers (B09), the permission decision (B10) and the two reply
    operations (lane M) all arrive as ports and are never re-implemented."""
    return notification_action_service.NotificationActionDependencies(
        store=postgres_db,
        notifier=notification_service,
        sudo_gate=sudo_gate,
        kick_officer_event_drain=_kick_officer_event_drain,
        deliver_officer_note=_deliver_officer_note,
        route_inbound_reply=(
            lambda *args, **kwargs: inbound_reply_service.route_inbound_reply(
                *args, **kwargs, dependencies=_inbound_reply_dependencies()
            )
        ),
        resolve_job_notifications=lambda *args, **kwargs: (
            job_freeze_notification_service.resolve_job_notifications(
                *args,
                **kwargs,
                dependencies=_job_freeze_notification_dependencies(),
            )
        ),
        resume_job_internal=lambda *args, **kwargs: (
            _job_control_operations().resume_job_internal(*args, **kwargs)
        ),
        approve_job_internal=lambda *args, **kwargs: (
            _job_control_operations().approve_job_internal(*args, **kwargs)
        ),
        apply_vm_upgrade_decision=lambda *args, **kwargs: (
            _job_control_operations().apply_vm_upgrade_decision(*args, **kwargs)
        ),
        decide_permission_request=(
            lambda *args,
            **kwargs: thread_permission_operations.decide_permission_request(
                postgres_db, *args, **kwargs
            )
        ),
        job_resume_request=JobResumeRequest,
        job_approve_request=JobApproveRequest,
    )


# --- R1.B07 lane O: the Officer Post ---------------------------------------
# The shared operations below survive on purpose. Each has multiple composition sites —
# a dependency factory and a task-wiring site, or two factories — so the
# name is what those sites share rather than a hop they go through.
# Inlining would duplicate the same dependency expression twice, which is
# the trade B06 already declined. The ones that fed exactly one consumer
# are gone: that consumer binds the owning service directly.
def _officer_post_policy_dependencies() -> (
    officer_post_policy_service.OfficerPostPolicyDependencies
):
    """The auto-pull release fence, read live (§P1)."""
    return officer_post_policy_service.OfficerPostPolicyDependencies(
        auto_pull_release_enabled=lambda: OFFICER_AUTO_PULL_RELEASE_ENABLED,
    )


def _officer_conference_dependencies() -> (
    officer_conference_service.OfficerConferenceDependencies
):
    return officer_conference_service.OfficerConferenceDependencies(
        store=postgres_db,
        kick_officer_event_drain=_kick_officer_event_drain,
    )


def _officer_post_view_dependencies() -> (
    officer_post_view_service.OfficerPostViewDependencies
):
    """R1.B07 lane O. The two deployment flags are callables so a rebind on
    ``main`` still steers the read; the conference lookup is a constructed port
    so the card and the create funnel share one reading of it."""
    return officer_post_view_service.OfficerPostViewDependencies(
        store=postgres_db,
        vector_store=vector_db,
        usage_ledger=usage_ledger,
        persistent_provisioner=persistent_provisioner,
        auto_pull_release_enabled=lambda: OFFICER_AUTO_PULL_RELEASE_ENABLED,
        persistent_agent_reconciliation_enabled=(
            lambda: PERSISTENT_AGENT_RECONCILIATION_ENABLED
        ),
        find_open_conference_thread=_find_open_conference_thread,
    )


def _officer_post_lifecycle_dependencies() -> (
    officer_post_lifecycle_service.OfficerPostLifecycleDependencies
):
    """R1.B07 lane O. ``create_thread`` (B06's one session funnel) and
    ``end_thread_flow`` (B09's stand-down) are consumed as ports, never
    re-implemented."""
    return officer_post_lifecycle_service.OfficerPostLifecycleDependencies(
        store=postgres_db,
        persistent_provisioner=persistent_provisioner,
        persistent_thread_recycler=_persistent_thread_recycler,
        policy=_officer_post_policy_dependencies(),
        kick_officer_event_drain=_kick_officer_event_drain,
        deliver_officer_note=_deliver_officer_note,
        create_thread=create_thread,
        end_thread_flow=lambda *args, **kwargs: (
            _thread_retirement_operations().end_thread_flow(*args, **kwargs)
        ),
    )


def _officer_paging_dependencies() -> officer_paging_service.OfficerPagingDependencies:
    return officer_paging_service.OfficerPagingDependencies(
        store=postgres_db,
        notifier=notification_service,
    )


def _officer_watchdog_dependencies() -> (
    officer_watchdog_service.OfficerWatchdogDependencies
):
    """R1.B07 lane O. Built once when the task starts, so the recycler — which
    startup assigns after the provisioners — arrives as a callable the tick
    re-reads, exactly as the module global was re-read before."""
    return officer_watchdog_service.OfficerWatchdogDependencies(
        store=postgres_db,
        persistent_provisioner=persistent_provisioner,
        persistent_thread_recycler=lambda: _persistent_thread_recycler,
        kick_officer_event_drain=_kick_officer_event_drain,
        dispatch_officer_page=_dispatch_officer_page,
        conclude_conference_if_any=_conclude_conference_if_any,
        officer_runtime_verification_enabled=(
            lambda: OFFICER_RUNTIME_VERIFICATION_ENABLED
        ),
        persistent_agent_reconciliation_enabled=(
            lambda: PERSISTENT_AGENT_RECONCILIATION_ENABLED
        ),
    )


async def _find_open_conference_thread(project_id: str) -> dict[str, Any] | None:
    """Compatibility wrapper: B06's session-create funnel reattaches to the
    open conference through this name, and three suites patch it here."""
    return await officer_conference_service.find_open_conference_thread(
        project_id, dependencies=_officer_conference_dependencies()
    )


async def _hold_officer_for_conference(project_id: str, conference_id: str) -> None:
    """Compatibility wrapper: B06's create funnel stamps the conference hold
    through this name."""
    await officer_conference_service.hold_officer_for_conference(
        project_id, conference_id, dependencies=_officer_conference_dependencies()
    )


async def _conclude_conference_if_any(thread: dict[str, Any]) -> None:
    """Compatibility wrapper: B06's thread-status service and B09's End flow
    conclude a conference through this name, and six suites patch it here."""
    await officer_conference_service.conclude_conference_if_any(
        thread, dependencies=_officer_conference_dependencies()
    )


def _enforce_officer_auto_pull_release(desired: Any) -> None:
    """Compatibility wrapper: B06's create funnel fences unattended enablement
    through this name."""
    officer_post_policy_service.enforce_officer_auto_pull_release(
        desired, dependencies=_officer_post_policy_dependencies()
    )


def _validated_officer_post_patch(
    body: Any,
) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, str]]:
    """Compatibility wrapper: B05's ``validated_post_owned_officer_create_fragment``
    takes this validator as an argument, and one suite drives it here."""
    return officer_post_policy_service.validated_officer_post_patch(body)


async def _dispatch_officer_page(*args: Any, **kwargs: Any) -> Any:
    """Compatibility wrapper: the persistent recycler's respawn-failure alert
    and the watchdog's runtime-authorization incident both page through this
    name, and two suites patch it here."""
    return await officer_paging_service.dispatch_officer_page(
        *args, **kwargs, dependencies=_officer_paging_dependencies()
    )


# --- R1.B07 lane M: message routing, guidance and pending actions ---------
# The application owns the 5 s count cache and hands it to the operation, which
# keeps nothing between calls. It is deliberately still one dict per module,
# not per `app.state`: the store is resolved through the factory below so a
# suite rebinding `postgres_db` here still steers the read, and the same
# reasoning keeps the cache reachable at `main._pending_actions_cache` for the
# suites that clear it.
_pending_actions_cache: dict[str, dict[str, Any]] = {}


def _agent_messaging_dependencies() -> (
    agent_messaging_service.AgentMessagingDependencies
):
    """R1.B07 lane M. ``completion_commands_enabled`` is a callable because it
    is a B08-owned import-time flag suites rebind on ``main`` (§P1)."""
    return agent_messaging_service.AgentMessagingDependencies(
        store=postgres_db,
        notifier=notification_service,
        require_internal=require_internal,
        completion_commands_enabled=lambda: COMPLETION_COMMANDS_ENABLED,
        kick_officer_event_drain=_kick_officer_event_drain,
    )


def _inbound_reply_dependencies() -> inbound_reply_service.InboundReplyDependencies:
    """R1.B07 lane M. The completion-control guard and the resume funnel are
    B08's and B09's authorities; this batch consumes them, never re-derives."""
    return inbound_reply_service.InboundReplyDependencies(
        store=postgres_db,
        notifier=notification_service,
        guard_completion_control=_completion_control_boundary.guard,
        completion_dispatch_guard_kwargs=(
            _completion_control_boundary.dispatch_guard_kwargs
        ),
        internal_resume_job=lambda *args, **kwargs: (
            _job_control_operations().internal_resume_job(*args, **kwargs)
        ),
        kick_officer_event_drain=_kick_officer_event_drain,
    )


def _officer_message_action_dependencies() -> (
    officer_message_action_service.OfficerMessageActionDependencies
):
    """R1.B07 lane M. The reply lane arrives as two constructed ports so the
    officer actions deliver through the existing funnel rather than a copy."""
    return officer_message_action_service.OfficerMessageActionDependencies(
        store=postgres_db,
        notifier=notification_service,
        require_internal=require_internal,
        authorize_runtime_actor_request=authorize_runtime_actor_request,
        route_inbound_reply=(
            lambda *args, **kwargs: inbound_reply_service.route_inbound_reply(
                *args, **kwargs, dependencies=_inbound_reply_dependencies()
            )
        ),
        record_route_reply_resolution=(
            lambda *args, **kwargs: (
                inbound_reply_service.record_route_reply_resolution(
                    *args, **kwargs, dependencies=_inbound_reply_dependencies()
                )
            )
        ),
    )


def _job_guidance_dependencies() -> job_guidance_service.JobGuidanceDependencies:
    return job_guidance_service.JobGuidanceDependencies(
        store=postgres_db,
        require_internal=require_internal,
    )


def _message_thread_read_dependencies() -> (
    message_thread_read_service.MessageThreadReadDependencies
):
    return message_thread_read_service.MessageThreadReadDependencies(store=postgres_db)


def _pending_actions_dependencies() -> (
    pending_actions_service.PendingActionsDependencies
):
    return pending_actions_service.PendingActionsDependencies(
        store=postgres_db,
        cache=_pending_actions_cache,
    )


def _job_freeze_notification_dependencies() -> (
    job_freeze_notification_service.JobFreezeNotificationDependencies
):
    return job_freeze_notification_service.JobFreezeNotificationDependencies(
        notifier=notification_service,
    )


# --- R1.B08: completion, verification, subjobs and recovery -----------------
def _subjob_output_dependencies() -> subjob_output_operations.SubjobOutputDependencies:
    return subjob_output_operations.SubjobOutputDependencies(
        store=postgres_db,
        forge=gitea_client,
    )


def _scholar_completion_dependencies() -> (
    subjob_completion_operations.ScholarCompletionDependencies
):
    return subjob_completion_operations.ScholarCompletionDependencies(
        store=postgres_db,
        forge=gitea_client,
        trigger_dispatch=_trigger_dispatch,
        resolve_workspace_backend=(
            lambda job: resolve_workspace_contract(job).assigned_backend
        ),
        is_lite_config_override=_is_lite_config_override,
        should_provision_parent_container=_scholar_should_provision_parent_container,
        revalidate_datasource_selection=_revalidate_job_datasource_selection,
        datasource_selection_provenance=_datasource_selection_provenance,
        prepare_primary_repository_authority=functools.partial(
            prepare_job_primary_repository_authority,
            postgres_db,
            gitea_client,
        ),
        completion_resume_guard_kwargs=(
            lambda: _completion_control_boundary.resume_guard_kwargs()
        ),
        maybe_wake_session=(
            lambda job_id, status: maybe_wake_session(postgres_db, job_id, status)
        ),
        kick_session_wake_drain=lambda: _kick_session_wake_drain(postgres_db),
        notify_review_returned=notification_service.record_review_returned,
    )


def _delegation_completion_dependencies() -> (
    subjob_completion_operations.DelegationCompletionDependencies
):
    return subjob_completion_operations.DelegationCompletionDependencies(
        store=postgres_db,
        trigger_dispatch=_trigger_dispatch,
        completion_resume_guard_kwargs=(
            lambda: _completion_control_boundary.resume_guard_kwargs()
        ),
    )


def _verification_dependencies() -> verification_operations.VerificationDependencies:
    from orchestrator.database.postgres import _stateless_resume_context
    from shared import worker_queue
    from shared.run_queue import unpark_unit

    return verification_operations.VerificationDependencies(
        store=postgres_db,
        transaction=verification_operations.VerificationTransactionPorts(
            revalidate_datasource_selection=_revalidate_job_datasource_selection,
            datasource_selection_provenance=_datasource_selection_provenance,
            resolve_workspace_contract=resolve_workspace_contract,
            deep_merge_dicts=_deep_merge_dicts,
            is_lite_config_override=_is_lite_config_override,
            enqueue_worker_batch_wake=worker_queue.enqueue_worker_batch_wake,
            reset_worker_batch_attempts=worker_queue.reset_worker_batch_attempts,
            unpark_unit=unpark_unit,
            stateless_resume_context=_stateless_resume_context,
        ),
        effects=verification_operations.VerificationEffectPorts(
            forge=gitea_client,
            notifier=notification_service,
            prepare_job_repository_authority=functools.partial(
                prepare_job_primary_repository_authority,
                postgres_db,
                gitea_client,
            ),
            trigger_dispatch=_trigger_dispatch,
            maybe_wake_session=maybe_wake_session,
            kick_session_wake_drain=_kick_session_wake_drain,
            trigger_curation_final_pass=(
                lambda *args, **kwargs: (
                    curation_final_pass_service.trigger_curation_final_pass(
                        *args,
                        **kwargs,
                        dependencies=_curation_final_pass_dependencies(),
                    )
                )
            ),
            set_target_to_autonomy_status=(
                lambda job_id: subjob_completion_operations.set_target_to_autonomy_status(
                    job_id,
                    dependencies=_scholar_completion_dependencies(),
                )
            ),
            escalate_target=(
                lambda job_id,
                job,
                reason: subjob_completion_operations.escalate_target(
                    job_id,
                    job,
                    reason,
                    dependencies=_scholar_completion_dependencies(),
                )
            ),
            internal_resume_job=lambda *args, **kwargs: (
                _job_control_operations().internal_resume_job(*args, **kwargs)
            ),
        ),
    )


def _completion_effect_dependencies() -> (
    completion_effect_operations.CompletionEffectDependencies
):
    return completion_effect_operations.CompletionEffectDependencies(
        store=postgres_db,
        container_provisioner=container_provisioner,
        vm_provisioner=vm_provisioner,
        get_container_context=_get_container_context,
        get_vm_context=_get_vm_context,
        archive_and_cleanup_workspace=(
            _thread_retirement_operations().archive_and_cleanup_workspace
        ),
        s36_exact_absence_timeout_seconds=(
            lambda: _COMPLETION_S36_EXACT_ABSENCE_TIMEOUT_SECONDS
        ),
        logger=logger,
        recovery_store=VMWorkspaceRecoveryStore(postgres_db),
    )


def _legacy_completion_dependencies() -> (
    legacy_job_completion_operations.LegacyCompletionDependencies
):
    verification = _verification_dependencies()
    scholar = _scholar_completion_dependencies()
    delegation = _delegation_completion_dependencies()
    output = _subjob_output_dependencies()
    return legacy_job_completion_operations.LegacyCompletionDependencies(
        persistence=legacy_job_completion_operations.LegacyPersistenceDependencies(
            store=postgres_db,
            vector_store=vector_db,
            forge=gitea_client,
        ),
        workspace=legacy_job_completion_operations.LegacyWorkspaceDependencies(
            container_provisioner=container_provisioner,
            vm_provisioner=vm_provisioner,
            recovery_store=VMWorkspaceRecoveryStore(postgres_db),
            cloud_router=main_cloud_router,
            sudo_gate=sudo_gate,
            get_container_context=_get_container_context,
            get_vm_context=_get_vm_context,
            get_infra_transient_context=_get_infra_transient_context,
            job_needs_vm=_job_needs_vm,
            check_vm_permission=_check_vm_permission,
            capture_freeze_snapshot=lambda *args, **kwargs: (
                _job_control_operations().capture_workspace_snapshot_for_freeze(
                    *args, **kwargs
                )
            ),
            unmerged_pr_gate_reason=lambda *args, **kwargs: (
                _job_control_operations().unmerged_pr_gate_reason(*args, **kwargs)
            ),
        ),
        verification=legacy_job_completion_operations.LegacyVerificationDependencies(
            handle_critic_verdict=functools.partial(
                verification_operations.handle_critic_verdict_on_complete,
                dependencies=verification,
            ),
            materialize_critic_verdict=functools.partial(
                verification_operations.materialize_critic_verdict_transactional,
                dependencies=verification,
            ),
            run_critic_verdict_followups=functools.partial(
                verification_operations.run_critic_verdict_followups,
                dependencies=verification,
            ),
            trigger_verification=functools.partial(
                verification_operations.trigger_verification_on_complete,
                dependencies=verification,
            ),
            materialize_verification_critic=functools.partial(
                verification_operations.materialize_verification_critic_transactional,
                dependencies=verification,
            ),
            run_verification_critic_handoff=functools.partial(
                verification_operations.run_verification_critic_handoff,
                dependencies=verification,
            ),
            verification_rounds=verification_operations.verification_rounds,
        ),
        subjobs=legacy_job_completion_operations.LegacySubjobDependencies(
            graft_completed_subjob=functools.partial(
                subjob_output_operations.maybe_graft_completed_subjob,
                dependencies=output,
            ),
            handle_scholar_completion=functools.partial(
                subjob_completion_operations.handle_scholar_completion,
                dependencies=scholar,
            ),
            handle_delegation_completion=functools.partial(
                subjob_completion_operations.handle_delegation_child_completion,
                dependencies=delegation,
            ),
        ),
        post_commit=legacy_job_completion_operations.LegacyPostCommitDependencies(
            internal_resume_job=lambda *args, **kwargs: (
                _job_control_operations().internal_resume_job(*args, **kwargs)
            ),
            resume_job_without_vm=lambda *args, **kwargs: (
                _job_control_operations().resume_job_without_vm_internal(
                    *args, **kwargs
                )
            ),
            notify_operator_freeze=(
                lambda *args, **kwargs: (
                    job_freeze_notification_service.notify_operator_freeze(
                        *args,
                        **kwargs,
                        dependencies=_job_freeze_notification_dependencies(),
                    )
                )
            ),
            trigger_curation_final_pass=(
                lambda *args, **kwargs: (
                    curation_final_pass_service.trigger_curation_final_pass(
                        *args,
                        **kwargs,
                        dependencies=_curation_final_pass_dependencies(),
                    )
                )
            ),
            advance_project_loop=(
                lambda *args, **kwargs: (
                    project_loop_advance_service.advance_project_loop(
                        *args,
                        **kwargs,
                        dependencies=_project_loop_dependencies(),
                    )
                )
            ),
            prepare_project_loop_advance=(
                lambda *args, **kwargs: (
                    project_loop_advance_service.prepare_atomic_project_loop_advance(
                        *args,
                        **kwargs,
                        dependencies=_project_loop_dependencies(),
                    )
                )
            ),
            materialize_project_loop_advance=(
                lambda *args, **kwargs: (
                    project_loop_advance_service.materialize_prepared_project_loop_advance(
                        *args,
                        **kwargs,
                        dependencies=_project_loop_dependencies(),
                    )
                )
            ),
            execute_project_loop_handoff=(
                lambda *args, **kwargs: (
                    project_loop_advance_service.execute_persisted_project_loop_handoff(
                        *args,
                        **kwargs,
                        dependencies=_project_loop_dependencies(),
                    )
                )
            ),
            project_loop_handoff_error_output=(
                project_loop_advance_service.project_loop_handoff_error_output
            ),
            maybe_wake_session=maybe_wake_session,
            trigger_dispatch=_trigger_dispatch,
            kick_session_wake_drain=_kick_session_wake_drain,
        ),
        effects=legacy_job_completion_operations.LegacyCompletionEffectOperations(
            run=completion_effect_operations.run_completion_effect,
            run_workspace_teardown=(
                lambda *args, **kwargs: (
                    completion_effect_operations.run_completion_workspace_teardown(
                        *args,
                        **kwargs,
                        dependencies=_completion_effect_dependencies(),
                    )
                )
            ),
            dedup_key=completion_effect_operations.completion_effect_dedup_key,
        ),
        require_internal=require_internal,
        require_srw_runtime=require_srw_runtime,
        completion_commands_enabled=lambda: COMPLETION_COMMANDS_ENABLED,
        logger=logger,
    )


async def _run_legacy_completion(*args: Any, **kwargs: Any) -> dict[str, Any]:
    """Application adapter for the intact legacy completion operation."""

    return await legacy_job_completion_operations.complete_job_legacy(
        *args,
        **kwargs,
        dependencies=_legacy_completion_dependencies(),
    )


async def _run_persisted_completion(effect_runner: Any) -> dict[str, Any]:
    return await job_completion_operations.run_persisted_completion_workflow(
        effect_runner,
        dependencies=job_completion_operations.PersistedCompletionDependencies(
            legacy_complete=_run_legacy_completion,
        ),
    )


_completion_alerts = CompletionAlerts(
    CompletionAlertDependencies(
        store=postgres_db,
        notify_all_officers=notify_all_officers,
        kick_officer_event_drain=_kick_officer_event_drain,
    )
)
_completion_runtime = CompletionRuntime(
    CompletionRuntimeDependencies(
        store=postgres_db,
        workflow=_run_persisted_completion,
        commands_enabled=lambda: COMPLETION_COMMANDS_ENABLED,
        status_reorder_enabled=lambda: COMPLETION_STATUS_REORDER_ENABLED,
        sweep_alert=_completion_alerts.sweep,
        resolution_alert=_completion_alerts.resolution,
        monitor_alert=_completion_alerts.monitor,
        max_queued_session_age_seconds=(
            lambda: float(
                os.getenv("STATELESS_SESSION_QUEUED_AGE_ALARM_S", "60") or "60"
            )
        ),
        logger=logger,
    )
)
_completion_control_boundary = CompletionControlBoundary(_completion_runtime)


def _job_control_operations() -> job_control_operations.JobControlOperations:
    """Compose VM, sudo, Resume and approval controls around B08's boundary."""

    return job_control_operations.JobControlOperations(
        job_control_operations.JobControlDependencies(
            store=postgres_db,
            logger=logger,
            completion_control=_completion_control_boundary,
            completion_commands_enabled=lambda: COMPLETION_COMMANDS_ENABLED,
            completion_control_active_sql=_completion_control_active_sql,
            completion_control_owned_active_sql=_completion_control_owned_active_sql,
            sudo_gate=sudo_gate,
            workspace=workspace_service,
            snapshots=snapshot_service,
            ide_sessions=ide_session_service,
            vm_provisioner=vm_provisioner,
            forge=gitea_client,
            vector_store=vector_db,
            subjob_output=subjob_output_operations,
            subjob_output_dependencies=_subjob_output_dependencies,
            authorize_runtime_actor_request=authorize_runtime_actor_request,
            redispatch_livelock_trip=_redispatch_livelock_trip,
            user_experts_enabled=_user_experts_enabled,
            resolve_default_models=_resolve_default_models,
            prefetch_roster_refs=_prefetch_roster_refs,
            resolve_config=resolve_config,
            canonical_config_name=canonical_config_name,
            enforce_dispatch_grants=_enforce_dispatch_grants,
            grant_violations_detail=_grant_violations_detail,
            prepare_job_workspace_runtime=_prepare_job_workspace_runtime,
            resume_missing_workspace=lambda *args, **kwargs: (
                job_workspace_runtime.resume_missing_workspace(
                    *args,
                    **kwargs,
                    dependencies=_job_workspace_runtime_dependencies(),
                )
            ),
            workspace_context_keys=_WORKSPACE_CONTEXT_KEYS,
            prepare_job_repository_before_claim=(_prepare_job_repository_before_claim),
            resume_job_on_agent=lambda job, agent: _job_delivery_operations().resume(
                job, agent
            ),
            trigger_dispatch=_trigger_dispatch,
            resolve_job_notifications=lambda *args, **kwargs: (
                job_freeze_notification_service.resolve_job_notifications(
                    *args,
                    **kwargs,
                    dependencies=_job_freeze_notification_dependencies(),
                )
            ),
            maybe_wake_session=maybe_wake_session,
            kick_session_wake_drain=_kick_session_wake_drain,
            get_container_context=_get_container_context,
            get_vm_context=_get_vm_context,
            recovery_store=VMWorkspaceRecoveryStore(postgres_db),
        )
    )


def _job_delivery_operations() -> job_delivery_operations.JobDeliveryOperations:
    """Compose pinned start/resume delivery without owning scheduler tasks."""

    return job_delivery_operations.JobDeliveryOperations(
        job_delivery_operations.JobDeliveryDependencies(
            store=postgres_db,
            logger=logger,
            completion_commands_enabled=lambda: COMPLETION_COMMANDS_ENABLED,
            http_client_factory=httpx.AsyncClient,
            completion_control=_completion_control_boundary,
            pause_pending_job_ids=_job_dispatch_state.pause_pending_job_ids,
            gitea_client=gitea_client,
            workspace_context_keys=_WORKSPACE_CONTEXT_KEYS,
            prepare_job_workspace_runtime=_prepare_job_workspace_runtime,
            attest_pinned_k8s_job_workspace=lambda *args, **kwargs: (
                job_workspace_authority.attest_pinned_k8s_job_workspace(
                    *args,
                    **kwargs,
                    dependencies=_job_workspace_authority_dependencies(),
                )
            ),
            build_job_start_request=lambda *args, **kwargs: (
                job_start_bundle.build_job_start_request(
                    *args,
                    **kwargs,
                    dependencies=_job_start_bundle_dependencies(),
                )
            ),
            pinned_k8s_job_workspace_authority_is_current=lambda *args, **kwargs: (
                job_workspace_authority.pinned_k8s_job_workspace_authority_is_current(
                    *args,
                    **kwargs,
                    dependencies=_job_workspace_authority_dependencies(),
                )
            ),
            prepare_pinned_job_mutation_target=_prepare_pinned_job_mutation_target,
            redispatch_livelock_trip=_redispatch_livelock_trip,
            bind_log_context=bind_log_context,
            reset_log_context=reset_log_context,
            resume_missing_workspace=lambda *args, **kwargs: (
                job_workspace_runtime.resume_missing_workspace(
                    *args,
                    **kwargs,
                    dependencies=_job_workspace_runtime_dependencies(),
                )
            ),
            resolve_authorized_job_datasources=_resolve_authorized_job_datasources,
            apply_cloud_storage_override=_apply_cloud_storage_override,
            build_datasources_payload=_build_datasources_payload,
            job_project_repositories=_job_project_repositories,
            build_datasource_tool_override=_build_datasource_tool_override,
            inject_matching_workspace_config=lambda *args, **kwargs: (
                job_workspace_runtime.inject_matching_workspace_config(
                    *args,
                    **kwargs,
                    dependencies=_job_workspace_runtime_dependencies(),
                )
            ),
            get_container_context=_get_container_context,
            get_vm_context=_get_vm_context,
            authorize_job_repository_transport=authorize_job_repository_transport,
            apply_sticky_sudo_denial=_apply_sticky_sudo_denial,
            backend_from_override=_backend_from_override,
            repository_datasource_names=_repository_datasource_names,
            inject_lite_workspace_config=_inject_lite_workspace_config,
            is_experts_db_enabled=_is_experts_db_enabled,
            user_experts_enabled=_user_experts_enabled,
            enforce_dispatch_grants=_enforce_dispatch_grants,
            gather_in_scope_skills=_gather_in_scope_skills,
            seed_registry_model_overrides=_seed_registry_model_overrides,
            resolve_default_models=_resolve_default_models,
            prefetch_roster_refs=_prefetch_roster_refs,
            inject_dispatch_credentials=_inject_dispatch_credentials,
            grant_violations_detail=_grant_violations_detail,
            mint_worker_runtime_actor=mint_worker_runtime_actor,
            resume_reject_should_requeue=(
                job_control_operations.resume_reject_should_requeue
            ),
        )
    )


def _job_mutation_operations() -> job_mutation_operations.JobControlOperations:
    """Compose destructive job controls around shared mutation authority."""

    return job_mutation_operations.JobControlOperations(
        job_mutation_operations.JobControlDependencies(
            store=postgres_db,
            logger=logger,
            completion_commands_enabled=lambda: COMPLETION_COMMANDS_ENABLED,
            completion_control=_completion_control_boundary,
            manifest_cancel=lambda job_id: _manifest_execution_service().cancel(job_id),
            prepare_pinned_mutation_target=_prepare_pinned_job_mutation_target,
            archive_and_cleanup_workspace=(
                _thread_retirement_operations().archive_and_cleanup_workspace
            ),
            http_client_factory=httpx.AsyncClient,
            handle_scholar_completion=lambda job: (
                subjob_completion_operations.handle_scholar_completion(
                    job,
                    [],
                    dependencies=_scholar_completion_dependencies(),
                )
            ),
            maybe_wake_session=lambda job_id, status: maybe_wake_session(
                postgres_db, job_id, status
            ),
            kick_session_wake_drain=lambda: _kick_session_wake_drain(postgres_db),
            trigger_dispatch=_trigger_dispatch,
            resolve_job_notifications=lambda *args, **kwargs: (
                job_freeze_notification_service.resolve_job_notifications(
                    *args,
                    **kwargs,
                    dependencies=_job_freeze_notification_dependencies(),
                )
            ),
            snapshot_service=snapshot_service,
            gitea_client=gitea_client,
            revoke_and_delete_managed_repository=(revoke_and_delete_managed_repository),
            vector_db=vector_db,
        )
    )


def _job_control_route_dependencies() -> job_control_routes.JobControlRouteDependencies:
    return job_control_routes.JobControlRouteDependencies(
        operations=_job_control_operations(),
        store=postgres_db,
        require_admin=_require_admin,
        require_job_access=require_job_access,
        require_internal_or_job_access=require_internal_or_job_access,
        require_approved_user=require_approved_user,
        require_sudo_request_authority=require_sudo_request_authority,
        user_can_access_job_or_thread=user_can_access_job_or_thread,
        mcp_scope_project_id=mcp_scope_project_id,
    )


def _job_mutation_route_dependencies() -> (
    job_lifecycle_routes.JobControlRouteDependencies
):
    return job_lifecycle_routes.JobControlRouteDependencies(
        operations=_job_mutation_operations(),
        store=postgres_db,
        require_job_access=require_job_access,
        require_internal_or_job_access=require_internal_or_job_access,
        require_internal=require_internal,
    )


def _job_lifecycle_route_dependencies() -> (
    job_lifecycle_routes.JobLifecycleRouteDependencies
):
    return job_lifecycle_routes.JobLifecycleRouteDependencies(
        store=postgres_db,
        logger=logger,
        is_internal_call=is_internal_call,
        require_approved_user=require_approved_user,
        require_project_member=require_project_member,
        require_internal=require_internal,
        strip_raw_officer_claim_context=_strip_raw_officer_claim_context,
        strip_public_job_reserved_markers=_strip_public_job_reserved_markers,
        admit_job=admit_job,
        job_admission_dependencies=lambda request: _job_admission_dependencies(
            lambda: _job_admission_scope_dependencies(request)
        ),
        graft_subjob_output=lambda job_id: (
            subjob_output_operations.graft_subjob_output(
                job_id,
                dependencies=_subjob_output_dependencies(),
            )
        ),
    )


def _thread_retirement_operations() -> (
    thread_retirement_operations.ThreadRetirementOperations
):
    """Compose shared pinned/stateless retirement with captured authority."""

    return thread_retirement_operations.ThreadRetirementOperations(
        thread_retirement_operations.ThreadRetirementDependencies(
            store=postgres_db,
            agent_provisioner=agent_provisioner,
            persistent_provisioner=persistent_provisioner,
            container_provisioner=container_provisioner,
            vm_provisioner=vm_provisioner,
            recovery_store=VMWorkspaceRecoveryStore(postgres_db),
            docker_provisioner=docker_provisioner,
            workspace_suspension_service=workspace_suspension_service,
            snapshot_service=snapshot_service,
            gitea_client=gitea_client,
            main_cloud_router=main_cloud_router,
            pinned_retirement=_pinned_retirement_operations(),
            build_agent_cloud_mount=lambda *args, **kwargs: (
                agent_cloud_mounts._build_agent_cloud_mount(
                    *args,
                    **kwargs,
                    dependencies=_agent_cloud_mount_dependencies(),
                )
            ),
            get_container_context=_get_container_context,
            get_vm_context=_get_vm_context,
            vm_needs_release=_vm_needs_release,
            thread_uses_pinned_execution=_thread_uses_pinned_execution,
            threads_suspending=_threads_suspending,
            require_stateless_end_workspace=_require_stateless_end_workspace,
            decommission_officer_post=lambda *args, **kwargs: (
                officer_post_lifecycle_service.decommission_officer_post(
                    *args,
                    **kwargs,
                    dependencies=_officer_post_lifecycle_dependencies(),
                )
            ),
            conclude_conference_if_any=_conclude_conference_if_any,
            logger=logger,
        )
    )


def _thread_resume_operations(
    retirement: thread_retirement_operations.ThreadRetirementOperations | None = None,
) -> thread_resume_operations.ThreadResumeOperations:
    """Compose Resume while retaining application-owned task registries."""

    retirement = retirement or _thread_retirement_operations()
    return thread_resume_operations.ThreadResumeOperations(
        thread_resume_operations.ThreadResumeDependencies(
            store=postgres_db,
            agent_provisioner=agent_provisioner,
            persistent_provisioner=persistent_provisioner,
            container_provisioner=container_provisioner,
            workspace_suspension_service=workspace_suspension_service,
            main_cloud_router=main_cloud_router,
            officer_conference_service=officer_conference_service,
            retirement=retirement,
            late_cloud_setup_tasks=_late_cloud_setup_tasks,
            late_cloud_setup_attach_timeout_s=(
                lambda: LATE_CLOUD_SETUP_ATTACH_TIMEOUT_S
            ),
            classify_thread_project_ids=lambda *args, **kwargs: (
                thread_project_authorization_service.classify_thread_project_ids(
                    *args,
                    **kwargs,
                    dependencies=_thread_project_authorization_dependencies(),
                )
            ),
            resolve_session_config=_resolve_session_config,
            thread_project_ids=_thread_project_ids,
            require_stateless_workspace=_require_stateless_workspace,
            require_supported_protected_session_class=(
                lambda *args, **kwargs: (
                    session_config_resolution.require_supported_protected_session_class(
                        *args,
                        **kwargs,
                        dependencies=_session_config_dependencies(),
                    )
                )
            ),
            thread_workspace_backend=_thread_workspace_backend,
            hold_officer_for_conference=_hold_officer_for_conference,
            is_protected_cloud_mode_enabled=_is_protected_cloud_mode_enabled,
            schedule_protected_engage=_schedule_protected_engage,
            should_skip_session_folder=_should_skip_session_folder,
            await_protected_cloud_runtime_ready=_await_protected_cloud_runtime_ready,
            find_idle_persistent_agent=_find_idle_persistent_agent,
            thread_has_knowledge_scope=_thread_has_knowledge_scope,
            inject_thread_dispatch_credentials=_inject_thread_dispatch_credentials,
            send_session_attach=_send_session_attach,
            emit_session_provisioning_failure=_emit_session_provisioning_failure,
            thread_uses_pinned_execution=_thread_uses_pinned_execution,
            schedule_stateless_workspace_ensure=_schedule_stateless_workspace_ensure,
            create_task=asyncio.create_task,
            agent_get_thread_workspace_locked=lambda *args, **kwargs: (
                thread_workspace_delivery.agent_get_thread_workspace_locked(
                    *args,
                    **kwargs,
                    dependencies=_thread_workspace_delivery_dependencies(),
                )
            ),
            inject_lite_workspace_config=_inject_lite_workspace_config,
            logger=logger,
        )
    )


def _thread_lifecycle_dependencies() -> (
    thread_lifecycle_routes.ThreadLifecycleRouteDependencies
):
    retirement = _thread_retirement_operations()
    return thread_lifecycle_routes.ThreadLifecycleRouteDependencies(
        store=postgres_db,
        retirement=retirement,
        resume=_thread_resume_operations(retirement),
        require_thread_owner=require_thread_owner,
    )


def _thread_rewind_dependencies() -> thread_rewind_routes.ThreadRewindDependencies:
    return thread_rewind_routes.ThreadRewindDependencies(
        store=postgres_db,
        service=thread_rewind_operations.ThreadRewindService(
            postgres_db,
            _stateless_idle_conversation_rewind_enabled,
        ),
        require_thread_owner=require_thread_owner,
    )


async def _end_thread_flow(*args: Any, **kwargs: Any) -> dict[str, Any]:
    """B12 bridge for the stateless-wake operator acceptance harness."""

    return await _thread_retirement_operations().end_thread_flow(*args, **kwargs)


_session_memory_runtime = SessionMemoryRuntime(
    SessionMemoryDependencies(
        store=postgres_db,
        vector_store=vector_db,
        authorize_thread_project_ids=(
            lambda *args, **kwargs: _authorize_thread_project_ids(*args, **kwargs)
        ),
        resolve_session_config=(
            lambda *args, **kwargs: _resolve_session_config(*args, **kwargs)
        ),
    )
)


def _job_completion_dependencies() -> (
    job_completion_operations.JobCompletionDependencies
):
    async def accept_completion_command(*args: Any, **kwargs: Any) -> Any:
        from orchestrator.services.job_completion_commands import (
            accept_completion_command as operation,
        )

        return await operation(*args, **kwargs)

    return job_completion_operations.JobCompletionDependencies(
        store=postgres_db,
        require_internal=require_internal,
        commands_enabled=lambda: COMPLETION_COMMANDS_ENABLED,
        status_reorder_enabled=lambda: COMPLETION_STATUS_REORDER_ENABLED,
        inline_delay_seconds=lambda: COMPLETION_FINALIZER_INLINE_DELAY_SECONDS,
        accept_command=accept_completion_command,
        finalizer=_completion_runtime.finalizer,
        legacy_complete=_run_legacy_completion,
        logger=logger,
        sleep=asyncio.sleep,
    )


def _completion_recovery_dependencies() -> (
    completion_recovery_operations.CompletionRecoveryDependencies
):
    return completion_recovery_operations.CompletionRecoveryDependencies(
        store=postgres_db,
        completion_commands_enabled=lambda: COMPLETION_COMMANDS_ENABLED,
        trigger_dispatch=_trigger_dispatch,
        completion_resume_guard_kwargs=(
            lambda: _completion_control_boundary.resume_guard_kwargs()
        ),
        completion_dispatch_guard_kwargs=(
            lambda: _completion_control_boundary.dispatch_guard_kwargs()
        ),
        wait_for_stateless_cancel_settle=lambda job_id: (
            _job_mutation_operations().wait_for_stateless_cancel_settle(job_id)
        ),
        notify_operator_freeze=(
            lambda *args, **kwargs: (
                job_freeze_notification_service.notify_operator_freeze(
                    *args,
                    **kwargs,
                    dependencies=_job_freeze_notification_dependencies(),
                )
            )
        ),
        handle_scholar_completion=(
            lambda job, actions: subjob_completion_operations.handle_scholar_completion(
                job,
                actions,
                dependencies=_scholar_completion_dependencies(),
            )
        ),
        handle_delegation_child_completion=(
            lambda job, actions: (
                subjob_completion_operations.handle_delegation_child_completion(
                    job,
                    actions,
                    dependencies=_delegation_completion_dependencies(),
                )
            )
        ),
    )


app.state.agent_registration_dependencies_factory = (
    lambda: _agent_registration_dependencies()
)
app.state.agent_child_threads_dependencies_factory = (
    lambda: _agent_child_threads_dependencies()
)
app.state.officer_runtime_verification_dependencies_factory = (
    lambda: _officer_runtime_verification_dependencies()
)
app.state.thread_admission_dependencies_factory = (
    lambda: _thread_admission_dependencies()
)
app.state.thread_config_dependencies_factory = (
    lambda: _thread_config_update_dependencies()
)
app.state.unit_claim_bundle_dependencies_factory = (
    lambda: _unit_claim_bundle_dependencies()
)
app.state.run_queue_admin_dependencies_factory = lambda: _run_queue_admin_dependencies()
app.state.sessions_dependencies_factory = lambda: _sessions_dependencies()
app.state.agent_thread_status_dependencies_factory = (
    lambda: _agent_thread_status_dependencies()
)
app.state.agent_messaging_dependencies_factory = lambda: _agent_messaging_dependencies()
app.state.inbound_reply_dependencies_factory = lambda: _inbound_reply_dependencies()
app.state.officer_message_action_dependencies_factory = (
    lambda: _officer_message_action_dependencies()
)
app.state.job_guidance_dependencies_factory = lambda: _job_guidance_dependencies()
app.state.message_thread_read_dependencies_factory = (
    lambda: _message_thread_read_dependencies()
)
app.state.pending_actions_dependencies_factory = lambda: _pending_actions_dependencies()
app.state.officer_post_view_dependencies_factory = (
    lambda: _officer_post_view_dependencies()
)
app.state.officer_post_lifecycle_dependencies_factory = (
    lambda: _officer_post_lifecycle_dependencies()
)
app.state.officer_paging_dependencies_factory = lambda: _officer_paging_dependencies()
app.state.notification_api_dependencies_factory = (
    lambda: _notification_api_dependencies()
)
app.state.loop_plan_filing_dependencies_factory = (
    lambda: _loop_plan_filing_dependencies()
)
app.state.job_completion_dependencies_factory = lambda: _job_completion_dependencies()
app.state.verification_route_dependencies_factory = (
    lambda: verification_routes.VerificationRouteDependencies(
        workflow=_verification_dependencies(),
        require_internal=require_internal,
    )
)
app.state.automations_dependencies_factory = lambda: _automations_dependencies()
app.state.project_loops_dependencies_factory = lambda: _project_loops_dependencies()
app.state.job_control_dependencies_factory = lambda: _job_control_route_dependencies()
app.state.job_control_route_dependencies_factory = (
    lambda: _job_mutation_route_dependencies()
)
app.state.job_lifecycle_route_dependencies_factory = (
    lambda: _job_lifecycle_route_dependencies()
)
app.state.thread_lifecycle_dependencies_factory = (
    lambda: _thread_lifecycle_dependencies()
)
app.state.thread_rewind_dependencies_factory = lambda: _thread_rewind_dependencies()
app.state.job_assignment_dependencies_factory = lambda: _job_assignment_dependencies()
app.state.expert_catalog_dependencies_factory = lambda: _expert_catalog_dependencies()
app.state.tables_dependencies = TablesDependencies(db=postgres_db)
app.state.preferences_dependencies = PreferencesDependencies(
    db=postgres_db,
    role_base=lambda role: _role_base_or_empty(role),
    environ=os.environ,
)

# CSRF defense for the cookie BFF. Middleware order matters: Starlette
# runs the OUTERMOST `add_middleware` last, so we add CSRF first and CORS
# second. Result: incoming request → CORS preflight/origin handling →
# CSRF check → app. That means OPTIONS preflights are still answered by
# CORS (which is good — preflights are unauthenticated), while real
# POST/PUT/DELETE/PATCH requests get the layered Sec-Fetch-Site +
# X-CSRF + Origin allowlist check before they reach any handler.
app.add_middleware(CSRFMiddleware)

# CORS for Angular frontend (dev server on 4200, production/SSR on 4000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:4200",
        "http://127.0.0.1:4200",
        "http://localhost:4000",
        "http://127.0.0.1:4000",
    ]
    + [o for o in os.environ.get("CORS_ORIGINS", "").split(",") if o],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    # Canvas uses strong state/content ETags as mutation preconditions. The
    # local Cockpit dev server (4200) calls the orchestrator (8085)
    # cross-origin, so both non-secret response headers must be readable by
    # HttpClient there. Production remains same-origin behind the BFF.
    expose_headers=["ETag", "X-Canvas-Content-ETag", "X-Canvas-Mutation-Changed"],
)


@app.exception_handler(UnknownModelError)
async def _unknown_model_handler(
    request: Request, exc: UnknownModelError
) -> JSONResponse:
    """Translate registry misses into a helpful 400 instead of a 500.

    Fires whenever a request references a model ID that isn't in the
    admin-curated catalog. Points operators at the admin surface where they
    can register the model.
    """
    return JSONResponse(
        status_code=400,
        content={
            "detail": str(exc),
            "model_id": exc.model_id,
            "hint": (
                "Register this model under Admin → Models (anchored to a "
                "system provider key or a system endpoint from Admin → "
                "Providers), or pick an ID from /api/models."
            ),
        },
    )


@app.exception_handler(GiteaPathError)
async def _gitea_path_error_handler(
    request: Request, exc: GiteaPathError
) -> JSONResponse:
    """Refuse a caller-shaped repository path or name with a 400, not a 500.

    ``services.gitea`` validates every path, ref and repository name before
    a request leaves the process (the client authenticates as the Gitea
    instance administrator, so an unencoded ``..`` would re-target another
    owner's repository). The refusal is about the caller's input, not a
    server fault, so surface it as such -- this covers every route that
    reaches the sink, including the MCP-facing ``/repo/file`` and
    ``/repo/contents`` proxies.
    """
    return JSONResponse(status_code=400, content={"detail": str(exc)})


@app.exception_handler(Exception)
async def _unhandled_exception_handler(
    request: Request, exc: Exception
) -> JSONResponse:
    """Return a CORS-friendly 500 for any otherwise-unhandled exception.

    Without this, Starlette's outermost ServerErrorMiddleware produces a
    bare 500 that skips the CORSMiddleware on the way out. Browsers then
    drop the response (no Access-Control-Allow-Origin header) and Angular
    surfaces it as a status-0 "network failure" instead of a real 5xx,
    which makes server-side bugs look like client-side connectivity
    issues. Handling here keeps the response inside the middleware stack
    so CORS headers are attached.

    HTTPException / RequestValidationError / UnknownModelError are
    dispatched to their own handlers first, so they don't reach here.
    """
    logger.exception("Unhandled exception on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error"},
    )


# Request logging middleware — replaces uvicorn's shallow access log with
# app-level logging that includes response timing and error tracebacks.
_SILENT_PATHS = {"/api/health"}
_SILENT_PREFIXES = ("/api/ide/",)  # suppress per-asset log spam from IDE proxy


@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    path = request.url.path
    if path in _SILENT_PATHS or path.startswith(_SILENT_PREFIXES):
        return await call_next(request)

    method = request.method
    start = time.perf_counter()
    # request_id is bound upstream by CorrelationIdMiddleware (outermost), so it
    # tags both this access line and the route handler's logs.
    try:
        response = await call_next(request)
    except Exception:
        elapsed = (time.perf_counter() - start) * 1000
        logger.exception(
            "%s %s 500 (%dms) — unhandled exception", method, path, elapsed
        )
        return JSONResponse(
            status_code=500, content={"detail": "Internal server error"}
        )

    elapsed = (time.perf_counter() - start) * 1000
    status = response.status_code
    if status >= 500:
        logger.warning("%s %s %d (%dms)", method, path, status, elapsed)
    else:
        logger.info("%s %s %d (%dms)", method, path, status, elapsed)
    return response


# Trusted Cockpit/BFF responses must never become documents inside an untrusted
# Canvas app iframe. Register this outside route/CORS/CSRF handling so redirects,
# errors, and same-origin API responses receive the same response boundary. It
# appends (rather than replaces) any route-specific CSP.
#
# The IDE proxy is the one intentional frame-based application on this ASGI
# service (VS Code webviews). It receives a same-origin-only framing policy on
# the exact, separately hosted IDE/API authority. The same path on a Cockpit
# authority is still denied, closing the Canvas self-navigation boundary without
# breaking code-server webviews on api.<domain>.
def _origin_authority(value: str) -> tuple[str, int] | None:
    try:
        parsed = urlparse(value.strip())
    except ValueError:
        return None
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.path not in {"", "/"}
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        return None
    if parsed.username is not None or parsed.password is not None:
        return None
    try:
        hostname = parsed.hostname
        port = parsed.port
        if not hostname:
            return None
        hostname = hostname.rstrip(".").encode("idna").decode("ascii").lower()
    except (UnicodeError, ValueError):
        return None
    if not hostname:
        return None
    return hostname, port or (443 if parsed.scheme == "https" else 80)


def _origin_host_headers(value: str) -> tuple[str, ...]:
    """Host spellings a proxy may preserve for one configured web origin."""

    authority = _origin_authority(value)
    if authority is None:
        return ()
    hostname, port = authority
    host_literal = f"[{hostname}]" if ":" in hostname else hostname
    parsed = urlparse(value.strip())
    default_port = 443 if parsed.scheme == "https" else 80
    if port == default_port:
        return host_literal, f"{host_literal}:{port}"
    return (f"{host_literal}:{port}",)


def _isolated_ide_frame_authorities() -> dict[str, tuple[str, ...]]:
    cockpit_origins = {
        "http://localhost:4200",
        "http://127.0.0.1:4200",
        "http://localhost:4000",
        "http://127.0.0.1:4000",
        os.environ.get("SRW_SPA_BASE_URL", ""),
        *os.environ.get("CORS_ORIGINS", "").split(","),
    }
    cockpit_authorities = {
        authority
        for origin in cockpit_origins
        if (authority := _origin_authority(origin)) is not None
    }
    ide_origin = os.environ.get("IDE_PROXY_BASE_URL", "http://localhost:8085")
    ide_authority = _origin_authority(ide_origin)
    if ide_authority is None or ide_authority in cockpit_authorities:
        return {}
    return {"/api/ide/": _origin_host_headers(ide_origin)}


app.add_middleware(
    TrustedParentAntiFramingMiddleware,
    same_origin_frameable_path_authorities=_isolated_ide_frame_authorities(),
)


# request_id correlation — added last so it is OUTERMOST (wraps both the
# access-log and anti-framing middleware above). See CorrelationIdMiddleware in
# logging_config.
app.add_middleware(CorrelationIdMiddleware)


# Include routers
from orchestrator.routers.bench import router as bench_router  # noqa: E402

app.include_router(bench_router)
app.include_router(bff_router)
app.include_router(graph_router)
app.include_router(uploads_router)
app.include_router(automations_router)
app.include_router(canvases_router)
app.include_router(internal_canvases_router)
app.include_router(wopi_router)
app.include_router(project_loops_router)
app.include_router(product_capabilities_router)
app.include_router(shared_browser_router)
app.include_router(vm_guest_router)
app.include_router(sessions_router)
app.include_router(thread_rewind_routes.router)
app.include_router(contacts_router)
app.include_router(contacts_project_router)
app.include_router(tables_router)
app.include_router(preferences_router)
app.include_router(job_reads_routes.router)
app.include_router(provider_catalog_routes.router)
app.include_router(model_catalog_routes.router)
app.include_router(config_catalog_routes.router)
app.include_router(manifest_routes.router)
app.include_router(job_inspection_routes.router)
app.include_router(job_audit_routes.router)
app.include_router(job_artifacts_routes.router)
app.include_router(diagnostics_routes.router)
app.include_router(identity_routes.router)
app.include_router(access_token_routes.router)
app.include_router(ssh_access_routes.router)
app.include_router(usage_reporting_routes.router)
app.include_router(infrastructure_admin_routes.router)
app.include_router(provider_credentials_routes.router)
app.include_router(subscription_management_routes.router)
app.include_router(voice_routes.router)
app.include_router(system_settings_routes.router)
vm_workspace_cleanup_authority_routes.configure(
    store_factory=lambda: VMWorkspaceRecoveryStore(postgres_db)
)
app.include_router(vm_workspace_cleanup_authority_routes.router)
vm_creation_retry_authority_routes.configure(
    store_factory=lambda: VMCreationRetryStore(postgres_db)
)
app.include_router(vm_creation_retry_authority_routes.router)
vm_resource_inventory_routes.configure_from_environment(postgres_db)
app.include_router(vm_resource_inventory_routes.router)
app.include_router(capacity_routes.router)
app.include_router(user_administration_routes.router)
app.include_router(job_diagnostics_routes.router)
app.include_router(expert_catalog_routes.router)
# The connector router carries the only literal `/api/projects/<literal>` route
# in the application (`/api/projects/linkable-datasource-targets`). Starlette
# matches in list order, so it must stay ahead of the project router's
# `/api/projects/{project_id}` or the literal would be swallowed as an id.
app.include_router(datasources_routes.router)
app.include_router(projects_routes.router)
app.include_router(knowledge_routes.router)
app.include_router(citations_routes.router)
app.include_router(media_routes.router)
app.include_router(ide_routes.router)
app.include_router(workspace_access_routes.router)
app.include_router(thread_files_routes.router)
app.include_router(job_repo_routes.router)
app.include_router(job_diff_routes.router)
app.include_router(job_review_routes.router)
app.include_router(agent_cloud_stage_routes.router)
app.include_router(thread_cloud_diff_routes.router)
app.include_router(main_cloud_settings_routes.router)
app.include_router(agent_thread_workspace_routes.router)
app.include_router(job_assignment_routes.router)
app.include_router(unit_claim_routes.router)
app.include_router(thread_admission_routes.router)
app.include_router(agent_registration_routes.router)
app.include_router(agent_child_threads_routes.router)
app.include_router(officer_runtime_verification_routes.router)
app.include_router(thread_config_routes.router)
app.include_router(run_queue_admin_routes.router)
app.include_router(agent_thread_status_routes.router)
app.include_router(messaging_routes.router)
app.include_router(actions_routes.router)
app.include_router(officer_routes.router)
app.include_router(agent_officer_routes.router)
app.include_router(notification_routes.router)
app.include_router(loop_plan_routes.router)


def _resolve_submitted_job_origin(
    *,
    context: dict[str, Any] | None,
    parent_job_id: Any,
    thread_id: Any,
) -> str:
    """Classify a job arriving through ``POST /api/jobs``.

    That endpoint is not "the user path". It is the shared funnel for human
    submissions, session launches, delegation/critic children forwarded over
    the internal key, and the job bench — all of which arrive with the same
    request shape, which is why origin has to be resolved here rather than
    assumed. (Officer admissions also pass through, but they branch earlier
    and are stamped by ``admit_and_create_job``.)

    Bench is recognised by ``context['bench']``, which
    ``services/bench.py::build_bench_job_payload`` already sets — otherwise
    benchmark traffic is byte-identical to a normal internal submission and
    would land in every user's job list and spend attribution.

    The order mirrors migration 0172's backfill so historic rows and new ones
    are classified the same way.
    """
    if context and "bench" in context:
        return "bench"
    if parent_job_id:
        return "subjob"
    if thread_id:
        return "session"
    return "user"


def _job_inspection_dependencies() -> job_inspection_routes.JobInspectionDependencies:
    return job_inspection_routes.JobInspectionDependencies(
        store=postgres_db,
        inspections=job_inspection_operations.JobInspectionDependencies(
            store=postgres_db,
            audit_reader=audit_reader,
            user_visible_project_ids=user_visible_project_ids,
            mcp_scope_project_id=mcp_scope_project_id,
            active_job_statuses=job_inspection_operations.ME_ACTIVE_JOB_STATUSES,
        ),
        require_approved_user=require_approved_user,
        require_job_access=require_job_access,
        require_thread_owner=require_thread_owner,
        require_internal=require_internal,
    )


def _job_audit_dependencies() -> job_audit_routes.JobAuditDependencies:
    return job_audit_routes.JobAuditDependencies(
        store=postgres_db,
        audit_reader=audit_reader,
        require_admin=_require_admin,
        require_approved_user=require_approved_user,
        require_job_access=require_job_access,
    )


def _job_artifacts_dependencies() -> job_artifacts_routes.JobArtifactDependencies:
    return job_artifacts_routes.JobArtifactDependencies(
        store=postgres_db,
        artifacts=job_artifacts_operations.JobArtifactDependencies(
            store=postgres_db,
            forge=gitea_client,
            resolve_job_repo=(
                lambda job_id: subjob_output_operations.resolve_job_repo(
                    job_id, dependencies=_subjob_output_dependencies()
                )
            ),
            evidence=job_evidence_operations,
        ),
        require_job_access=require_job_access,
    )


def _provider_credentials_dependencies() -> (
    provider_credentials_routes.ProviderCredentialsDependencies
):
    """Resolve the credential store per invocation.

    ``postgres_db`` is rebound during ``lifespan``; a factory that captured it
    at import would bind the unconnected instance forever.
    """
    return provider_credentials_routes.ProviderCredentialsDependencies(
        store=postgres_db,
        operations=provider_credentials_operations.ProviderCredentialDependencies(
            store=postgres_db
        ),
        require_approved_user=require_approved_user,
    )


def _subscription_management_dependencies() -> (
    subscription_management_routes.SubscriptionManagementDependencies
):
    """Compose the subscription adapters over the current store and logger."""
    return subscription_management_routes.SubscriptionManagementDependencies(
        operations=subscription_management_operations.SubscriptionManagementDependencies(
            store=postgres_db,
            logger=logger,
            ensure_proxy_endpoint=ensure_subscription_proxy_endpoint,
        ),
        require_admin=_require_admin,
    )


def _voice_dependencies() -> voice_routes.VoiceDependencies:
    """Resolve the metering ledger per invocation — ``usage_ledger`` is ``None``
    until ``lifespan`` builds it."""
    return voice_routes.VoiceDependencies(
        store=postgres_db,
        operations=voice_operations.VoiceDependencies(
            store=postgres_db,
            logger=logger,
            ledger=usage_ledger,
        ),
        require_approved_user=require_approved_user,
        require_thread_owner=require_thread_owner,
    )


def _system_settings_dependencies() -> (
    system_settings_routes.SystemSettingsDependencies
):
    return system_settings_routes.SystemSettingsDependencies(
        operations=system_settings_operations.SystemSettingsDependencies(
            store=postgres_db
        ),
        require_admin=_require_admin,
    )


def _capacity_dependencies() -> capacity_routes.CapacityDependencies:
    """Admin capacity read (capacity_ux_and_queue_autoscaling.md §2)."""
    from orchestrator.services.stateless_capacity import capacity_snapshot
    from orchestrator.services.vm_resource_capacity import vm_capacity_snapshot

    return capacity_routes.CapacityDependencies(
        snapshot=lambda: capacity_snapshot(postgres_db),
        require_admin=_require_admin,
        vm_snapshot=lambda: vm_capacity_snapshot(postgres_db),
    )


def _user_administration_dependencies() -> (
    user_administration_routes.UserAdministrationDependencies
):
    """Compose grant, capability and user-administration ports.

    Cloud routing, notification authority and the deployment feature flags stay
    owned by this application; the router receives them as callables so a
    per-request resolution always sees the current binding.
    """
    return user_administration_routes.UserAdministrationDependencies(
        store=postgres_db,
        operations=user_administration_operations.UserAdministrationDependencies(
            store=postgres_db,
            logger=logger,
            main_cloud_router=main_cloud_router,
            user_id_type=UserId,
            provision_default_project_knowledge=(
                lambda user, project: (
                    project_provisioning_operations.provision_default_project_knowledge(
                        user,
                        project,
                        dependencies=_project_provisioning_dependencies(),
                    )
                )
            ),
            ensure_user_provisioned=ensure_user_provisioned,
            notification_service=notification_service,
            grant_project_ids=_grant_project_ids,
            is_protected_cloud_mode_enabled=_is_protected_cloud_mode_enabled,
            datasource_scope_auto_attach_v1_enabled=(
                _datasource_scope_auto_attach_v1_enabled
            ),
            datasource_defaults_on_omission=_datasource_defaults_on_omission,
        ),
        require_admin=_require_admin,
        require_approved_user=require_approved_user,
    )


def _job_diagnostics_dependencies() -> (
    job_diagnostics_routes.JobDiagnosticsDependencies
):
    """Resolve the log, archive and audit readers per invocation.

    ``audit_reader`` and the workspace/snapshot services are application
    singletons that tests replace wholesale; binding them at import would pin
    the pre-lifespan objects.
    """
    return job_diagnostics_routes.JobDiagnosticsDependencies(
        store=postgres_db,
        operations=job_diagnostics_operations.JobDiagnosticsDependencies(
            workspace=workspace_service,
            snapshots=snapshot_service,
            audit_reader=audit_reader,
            prepare_pinned_job_mutation_target=_prepare_pinned_job_mutation_target,
        ),
        require_job_access=require_job_access,
        require_thread_owner=require_thread_owner,
    )


# =============================================================================
# R1.B03 — projects, datasources, knowledge, citations and media
#
# Every factory below resolves its collaborators *per invocation*. ``postgres_db``,
# ``vector_db``, ``gitea_client``, ``keycloak_groups``, ``main_cloud_router`` and
# ``snapshot_service`` are all rebound during ``lifespan``; a factory that captured
# one at import would bind the unconnected instance forever.
# =============================================================================


def _knowledge_index_dependencies() -> (
    knowledge_index_operations.KnowledgeIndexDependencies
):
    """Collaborators for one KB index operation.

    Shared by three consumers — datasource CRUD, project provisioning's vault
    adoption, and the knowledge reindex/materialize commands — so that the
    advisory-claim ordering and the delete-before-late-write fence have exactly
    one implementation. ``tasks`` is the single app-owned registry.
    """
    return knowledge_index_operations.KnowledgeIndexDependencies(
        store=postgres_db,
        vector_db=vector_db,
        gitea_client=gitea_client,
        logger=logger,
        tasks=kb_datasource_tasks,
        inject_system_kb_embedding_profile=_inject_system_kb_embedding_profile,
    )


def _datasources_dependencies() -> datasources_routes.DatasourcesDependencies:
    """Compose the connector CRUD adapters over the current stores."""
    return datasources_routes.DatasourcesDependencies(
        store=postgres_db,
        operations=datasources_operations.DatasourceDependencies(
            store=postgres_db,
            vector_db=vector_db,
            knowledge_index=_knowledge_index_dependencies(),
            mcp_datasources_enabled=_mcp_datasources_enabled,
            validate_mcp_datasource=_validate_mcp_datasource,
        ),
        require_approved_user=require_approved_user,
        require_project_member=require_project_member,
        require_project_owner=require_project_owner,
        require_datasource_access=require_datasource_access,
        require_datasource_owner=require_datasource_owner,
        require_job_access=require_job_access,
    )


def _project_provisioning_dependencies() -> (
    project_provisioning_operations.ProjectProvisioningDependencies
):
    """Optional-tier provisioning ports: forge, Keycloak groups, main cloud.

    ``repair`` is the one long-lived value here — the per-project heal locks
    must be the *same* map across requests or two concurrent heals would each
    create a Space.
    """
    return project_provisioning_operations.ProjectProvisioningDependencies(
        store=postgres_db,
        forge=gitea_client,
        keycloak_groups=keycloak_groups,
        main_cloud_router=main_cloud_router,
        logger=logger,
        repair=_project_repair_state,
        knowledge_index=_knowledge_index_dependencies(),
    )


def _projects_dependencies() -> projects_routes.ProjectsDependencies:
    """Compose the project lifecycle, membership and repository adapters.

    ``with_validated_tool_overrides`` is injected rather than duplicated: job
    create, session create and project create must keep answering identically
    about which tool categories a stored override may name.
    """
    return projects_routes.ProjectsDependencies(
        store=postgres_db,
        operations=projects_operations.ProjectDependencies(
            store=postgres_db,
            vector_db=vector_db,
            forge=gitea_client,
            keycloak_groups=keycloak_groups,
            main_cloud_router=main_cloud_router,
            logger=logger,
            provisioning=_project_provisioning_dependencies(),
            with_validated_tool_overrides=_with_validated_tool_overrides,
        ),
        require_admin=_require_admin,
        require_approved_user=require_approved_user,
        require_project_member=require_project_member,
        require_project_owner=require_project_owner,
        require_job_access=require_job_access,
    )


def _knowledge_projection_dependencies() -> (
    knowledge_projection_operations.KnowledgeProjectionDependencies
):
    """Ports for the connector-knowledge projection.

    ``store`` here is the **vector** pool: this projection writes only
    ``knowledge_index``, and the graph leg goes through ``graph``.
    """
    return knowledge_projection_operations.KnowledgeProjectionDependencies(
        store=vector_db,
        logger=logger,
        graph=_knowledge_graph,
    )


def _knowledge_dependencies() -> knowledge_routes.KnowledgeDependencies:
    """Compose the project knowledge reads and commands."""
    return knowledge_routes.KnowledgeDependencies(
        store=postgres_db,
        operations=knowledge_operations_module.KnowledgeOperationDependencies(
            store=postgres_db,
            vector_db=vector_db,
            gitea_client=gitea_client,
            logger=logger,
            graph=_knowledge_graph,
            knowledge_index=_knowledge_index_dependencies(),
        ),
        require_project_member=require_project_member,
        require_internal=require_internal,
    )


def _citations_dependencies() -> citations_routes.CitationsDependencies:
    """Compose the source, citation and memory reads."""
    return citations_routes.CitationsDependencies(
        store=postgres_db,
        operations=citations_operations.CitationDependencies(
            store=postgres_db,
            vector_db=vector_db,
            snapshot_service=snapshot_service,
            main_cloud_router=main_cloud_router,
            logger=logger,
        ),
        require_approved_user=require_approved_user,
        require_job_access=require_job_access,
        require_project_member=require_project_member,
        require_internal=require_internal,
        user_can_access_any_job=user_can_access_any_job,
        user_can_access_job_or_thread=user_can_access_job_or_thread,
    )


def _media_dependencies() -> media_routes.MediaDependencies:
    """The media proxy needs only the store its approved-user gate reads."""
    return media_routes.MediaDependencies(
        store=postgres_db,
        require_approved_user=require_approved_user,
    )


def _ide_dependencies() -> ide_routes.IdeDependencies:
    """Bind the IDE session and proxy services this application owns."""
    from orchestrator.services.vm_ide_transport import VMIDETransport

    return ide_routes.IdeDependencies(
        store=postgres_db,
        ide_sessions=ide_session_service,
        ide_proxy=ide_proxy_service,
        vm_ide_transport=VMIDETransport(vm_provisioner),
    )


def _workspace_access_dependencies() -> (
    workspace_access_routes.WorkspaceAccessDependencies
):
    """Compose snapshot reads, forge access grants and workspace provisioning.

    The repository resolver belongs to B08's output service and is injected
    with the application-owned store and forge collaborators.
    """
    return workspace_access_routes.WorkspaceAccessDependencies(
        store=postgres_db,
        forge=gitea_client,
        workspace=workspace_service,
        snapshots=snapshot_service,
        operations=workspace_access_operations.WorkspaceOperationDependencies(
            store=postgres_db,
            forge=gitea_client,
            container_provisioner=container_provisioner,
            enforce_job_workspace_upgrade_grants=(
                _enforce_job_workspace_upgrade_grants
            ),
        ),
        resolve_job_repo=(
            lambda job_id: subjob_output_operations.resolve_job_repo(
                job_id, dependencies=_subjob_output_dependencies()
            )
        ),
        require_admin=_require_admin,
    )


def _thread_files_dependencies() -> thread_files_routes.ThreadFilesDependencies:
    """Bind both workspace provisioners plus B05's backend/lane resolvers."""
    from orchestrator.services.vm_ide_transport import VMIDETransport

    return thread_files_routes.ThreadFilesDependencies(
        store=postgres_db,
        container_provisioner=container_provisioner,
        vm_provisioner=vm_provisioner,
        thread_workspace_backend=_thread_workspace_backend,
        require_stateless_workspace=_require_stateless_workspace,
        vm_ide_transport=VMIDETransport(vm_provisioner),
    )


def _job_repo_dependencies() -> job_repo_routes.JobRepoDependencies:
    """Forge reads for one job's workspace repository."""
    return job_repo_routes.JobRepoDependencies(
        store=postgres_db,
        repo_reads=job_repo_reads.JobRepoReadDependencies(
            forge=gitea_client,
            resolve_job_repo=(
                lambda job_id: subjob_output_operations.resolve_job_repo(
                    job_id, dependencies=_subjob_output_dependencies()
                )
            ),
        ),
    )


def _job_diff_dependencies() -> job_diff_routes.JobDiffDependencies:
    """Diff review, including the completion-control authority B08 owns.

    The four control callables are injected, never re-derived: accept/reject
    must claim, guard and abort through the *same* authority the completion
    endpoint uses, and a second implementation of that policy is how two
    writers end up disagreeing about a terminal job.
    """
    return job_diff_routes.JobDiffDependencies(
        store=postgres_db,
        diff_review=job_diff_review.JobDiffReviewDependencies(
            store=postgres_db,
            vector_store=vector_db,
            forge=gitea_client,
            cloud_router=main_cloud_router,
            get_completion_control=_completion_runtime.control,
            guard_completion_control=_completion_control_boundary.guard,
            claim_completion_control=_completion_control_boundary.claim,
            abort_completion_control_claim=_completion_control_boundary.abort,
            advance_project_loop=(
                lambda *args, **kwargs: (
                    project_loop_advance_service.advance_project_loop(
                        *args,
                        **kwargs,
                        dependencies=_project_loop_dependencies(),
                    )
                )
            ),
        ),
    )


def _job_review_dependencies() -> job_review_routes.JobReviewDependencies:
    """Cloud export plus the review-session create B06 still owns."""
    return job_review_routes.JobReviewDependencies(
        store=postgres_db,
        export=job_export.JobExportDependencies(
            store=postgres_db,
            forge=gitea_client,
            cloud_router=main_cloud_router,
            resolve_job_repo=(
                lambda job_id: subjob_output_operations.resolve_job_repo(
                    job_id, dependencies=_subjob_output_dependencies()
                )
            ),
        ),
        review_session=job_review_session.JobReviewSessionDependencies(
            store=postgres_db,
            create_thread=create_thread,
            thread_create_request=ThreadCreateRequest,
            trusted_thread_seed=TrustedThreadSeed,
            bundled_expert_bundle=_bundled_expert_bundle,
        ),
    )


def _agent_cloud_stage_dependencies() -> (
    agent_cloud_stage_routes.AgentCloudStageDependencies
):
    """Agent-facing stage trigger and the two retirement reads beside it."""
    return agent_cloud_stage_routes.AgentCloudStageDependencies(
        store=postgres_db,
        snapshots=snapshot_service,
        vm_provisioner=vm_provisioner,
        cloud_tasks=cloud_task_registry,
        is_protected_cloud_mode_enabled=_is_protected_cloud_mode_enabled,
        require_pinned_workspace_credential_owner=(
            _require_pinned_workspace_credential_owner
        ),
    )


def _agent_cloud_mount_dependencies() -> agent_cloud_mounts.AgentCloudMountDependencies:
    """Collaborators for one cloud-payload build.

    Rebuilt per call: ``postgres_db`` and ``main_cloud_router`` are rebound
    during ``lifespan``, and the protected-mode flag is read live.
    """
    return agent_cloud_mounts.AgentCloudMountDependencies(
        store=postgres_db,
        cloud_router=main_cloud_router,
        cloud_tasks=cloud_task_registry,
        is_protected_cloud_mode_enabled=_is_protected_cloud_mode_enabled,
        cloud_workspace_driver=_cloud_workspace_driver,
        slugify_mount_name=_slugify_mount_name,
    )


def _protected_cloud_engage_dependencies() -> (
    protected_cloud_engage.ProtectedCloudEngageDependencies
):
    """Collaborators for one protected-cloud engage/await/report.

    ``is_protected_cloud_mode_enabled`` is the *same* callable the mount and
    diff dependencies get, so the flag cannot disagree with itself inside one
    request.
    """
    return protected_cloud_engage.ProtectedCloudEngageDependencies(
        store=postgres_db,
        cloud_router=main_cloud_router,
        cloud_tasks=cloud_task_registry,
        is_protected_cloud_mode_enabled=_is_protected_cloud_mode_enabled,
        thread_workspace_backend=_thread_workspace_backend,
    )


def _thread_cloud_diff_dependencies() -> (
    thread_cloud_diff_routes.ThreadCloudDiffRouteDependencies
):
    """Owner-facing cloud-diff review over the protected-cloud engage ports."""
    return thread_cloud_diff_routes.ThreadCloudDiffRouteDependencies(
        store=postgres_db,
        operations=thread_cloud_diff_operations.ThreadCloudDiffDependencies(
            store=postgres_db,
            cloud_router=main_cloud_router,
            snapshot_service=snapshot_service,
            vm_provisioner=vm_provisioner,
            cloud_tasks=cloud_task_registry,
            protected_cloud=_protected_cloud_engage_dependencies(),
            is_protected_cloud_mode_enabled=_is_protected_cloud_mode_enabled,
        ),
    )


def _rebind_main_cloud_router(router: Any) -> None:
    """Rebind the application's cloud router.

    Nothing calls this today — ``MainCloudRouter`` is mutated in place by
    ``replace_active`` and ``main_cloud_router`` is assigned exactly once. The
    seam exists so a service that *does* swap the object never reassigns a
    global it does not own.
    """
    global main_cloud_router
    main_cloud_router = router


# =============================================================================
# R1.B04 compatibility wrappers — moved code that main still calls
# =============================================================================
# These eleven names moved to ``services/agent_cloud_mounts.py``,
# ``services/protected_cloud_engage.py`` and ``services/cloud_stage_authority.py``
# in R1.B04, but their remaining callers here belong to later batches. Each
# wrapper keeps the pre-extraction signature and supplies the dependencies, so
# the call sites read unchanged and ``main.<name>`` is still the attribute a
# caller (or a test) resolves.
#
# Owners and removal batch:
#   B05 payload preparation  — _build_agent_cloud_sync, _build_agent_cloud_mount,
#                              _build_protected_cloud_mount, _resolve_cloud_session_url,
#                              _protected_mount_selection_identity,
#                              _ro_mount_matches_protected_selection
#   B06 attach/create/resume — _await_protected_cloud_runtime_ready,
#                              _schedule_protected_engage, _record_protected_error,
#                              _protected_cloud_delivery_state,
#                              _protected_workspace_wait_payload
# Their remaining consumers belong to B10/B12 composition. Each disappears
# with that consumer; none is a public API.


def _resolve_cloud_session_url(
    thread: dict[str, Any],
    mount_rows: list[dict[str, Any]] | None = None,
) -> str | None:
    return agent_cloud_mounts._resolve_cloud_session_url(
        thread, mount_rows, dependencies=_agent_cloud_mount_dependencies()
    )


def _build_agent_cloud_sync(
    thread: dict[str, Any],
    *,
    mount_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    return agent_cloud_mounts._build_agent_cloud_sync(
        thread,
        mount_rows=mount_rows,
        dependencies=_agent_cloud_mount_dependencies(),
    )


def _build_protected_cloud_mount(
    row: dict[str, Any], *, thread_id: str
) -> dict[str, Any] | None:
    return agent_cloud_mounts._build_protected_cloud_mount(row, thread_id=thread_id)


async def _build_agent_cloud_mount(
    thread: dict[str, Any],
    *,
    mount_rows: list[dict[str, Any]] | None,
    metadata: dict[str, Any],
    terminal_retirement_token: int | None = None,
) -> dict[str, Any] | None:
    return await agent_cloud_mounts._build_agent_cloud_mount(
        thread,
        mount_rows=mount_rows,
        metadata=metadata,
        terminal_retirement_token=terminal_retirement_token,
        dependencies=_agent_cloud_mount_dependencies(),
    )


def _protected_mount_selection_identity(
    row: Mapping[str, Any] | None,
) -> tuple[str, ...] | None:
    return protected_cloud_engage._protected_mount_selection_identity(row)


def _ro_mount_matches_protected_selection(
    ro_row: Mapping[str, Any] | None,
    mount_rows: list[dict[str, Any]] | None,
    *,
    thread_id: str,
    user_id: str,
    runtime_generation: str,
) -> bool:
    return protected_cloud_engage._ro_mount_matches_protected_selection(
        ro_row,
        mount_rows,
        thread_id=thread_id,
        user_id=user_id,
        runtime_generation=runtime_generation,
    )


def _protected_workspace_wait_payload(
    *, state: str, error_code: str | None = None
) -> dict[str, Any]:
    return protected_cloud_engage._protected_workspace_wait_payload(
        state=state, error_code=error_code
    )


async def _protected_cloud_delivery_state(
    thread: dict[str, Any], metadata: dict[str, Any]
) -> tuple[str, str | None]:
    return await protected_cloud_engage._protected_cloud_delivery_state(
        thread, metadata, dependencies=_protected_cloud_engage_dependencies()
    )


async def _await_protected_cloud_runtime_ready(
    thread_id: str,
    *,
    timeout_s: float | None = None,
    allow_schedule: bool = True,
) -> bool:
    return await protected_cloud_engage._await_protected_cloud_runtime_ready(
        thread_id,
        timeout_s=timeout_s,
        allow_schedule=allow_schedule,
        dependencies=_protected_cloud_engage_dependencies(),
    )


def _schedule_protected_engage(
    thread_id: str,
    *,
    user_id: str,
    mount_rows: list[dict[str, Any]] | None,
    metadata: dict[str, Any] | None = None,
    runtime_generation: str,
) -> "asyncio.Task[None]":
    return protected_cloud_engage._schedule_protected_engage(
        thread_id,
        user_id=user_id,
        mount_rows=mount_rows,
        metadata=metadata,
        runtime_generation=runtime_generation,
        dependencies=_protected_cloud_engage_dependencies(),
    )


async def _record_protected_error(
    thread_id: str,
    message: str,
    *,
    code: str = "engage_failed",
    expected_runtime_generation: str | None = None,
) -> None:
    await protected_cloud_engage._record_protected_error(
        thread_id,
        message,
        code=code,
        expected_runtime_generation=expected_runtime_generation,
        dependencies=_protected_cloud_engage_dependencies(),
    )


def _main_cloud_settings_dependencies() -> (
    main_cloud_settings_routes.MainCloudSettingsRouteDependencies
):
    """Admin-only main-cloud configuration; installation authority preserved."""
    return main_cloud_settings_routes.MainCloudSettingsRouteDependencies(
        operations=main_cloud_settings_operations.MainCloudSettingsDependencies(
            store=postgres_db,
            cloud_router=main_cloud_router,
            rebind_cloud_router=_rebind_main_cloud_router,
            thread_mount_dependencies=_thread_mount_dependencies,
        ),
        require_admin=_require_admin,
    )


def _usage_reporting_dependencies() -> (
    usage_reporting_routes.UsageReportingDependencies
):
    """Compose the reporting ports without freezing a pre-startup ``None``.

    ``usage_ledger``, ``usage_rollup``, ``usage_cloud_estimator`` and the typed
    v2 collaborators are all assigned during ``lifespan``. Resolve them per
    invocation; a factory that captured them at import would report "metering is
    off" forever. Store lifecycle and visibility policy stay owned here.
    """
    store = postgres_db
    return usage_reporting_routes.UsageReportingDependencies(
        store=store,
        reports=usage_reporting_operations.UsageReportingDependencies(
            store=store,
            audit_reader=audit_reader,
            logger=logger,
            usage_ledger=usage_ledger,
            usage_rollup=usage_rollup,
            usage_cloud_estimator=usage_cloud_estimator,
            infrastructure_usage_v2=infrastructure_usage_v2,
            infrastructure_usage_rollup=infrastructure_usage_rollup,
            visible_project_ids=lambda actor: user_visible_project_ids(actor, store),
            scope_project_id=mcp_scope_project_id,
        ),
        require_admin=_require_admin,
        metering_settings=infrastructure_metering_settings,
        scope_project_id=mcp_scope_project_id,
        require_approved_user=require_approved_user,
        require_job_access=require_job_access,
        user_can_access_job_or_thread=user_can_access_job_or_thread,
    )


def _infrastructure_admin_dependencies() -> (
    infrastructure_admin_routes.InfrastructureAdminDependencies
):
    """Resolve the activation stores per invocation; every one is ``None`` at import.

    The three readiness values are wiring facts settled during ``lifespan``, so a
    request reads the state this process actually booted with rather than
    re-deriving it. ``leader_generation`` defaults to the service's own fence.
    """
    return infrastructure_admin_routes.InfrastructureAdminDependencies(
        operations=infrastructure_admin_operations.InfrastructureAdminDependencies(
            store=postgres_db,
            logger=logger,
            settings=infrastructure_metering_settings,
            durable_compute_activation_keys=(
                infrastructure_durable_compute_activation_keys
            ),
            durable_reporting_policy_ready=(
                infrastructure_durable_reporting_policy_ready
            ),
            storage_source_activation_ready=(
                infrastructure_storage_source_activation_ready
            ),
            storage_assets=infrastructure_storage_assets,
            compute_activation=infrastructure_compute_activation,
            workspace_cutover=infrastructure_workspace_cutover,
            usage_materializer=infrastructure_usage_materializer,
            coverage_waivers=infrastructure_coverage_waivers,
            ingestion_service=infrastructure_ingestion_service,
            audit=log_security_event,
            scope_project_id=mcp_scope_project_id,
        ),
        require_admin=_require_admin,
    )


def _identity_dependencies() -> identity_routes.IdentityDependencies:
    return identity_routes.IdentityDependencies(
        store=postgres_db,
        get_current_user=get_current_user,
    )


def _access_token_dependencies() -> access_token_routes.AccessTokenDependencies:
    return access_token_routes.AccessTokenDependencies(
        store=postgres_db,
        tokens=access_token_operations.AccessTokenDependencies(store=postgres_db),
        require_approved_user=require_approved_user,
        require_internal=require_internal,
    )


def _ssh_access_dependencies() -> ssh_access_routes.SshAccessDependencies:
    """Resolve the SSH collaborators per invocation.

    ``postgres_db``, ``notification_service``, ``logger`` and
    ``_session_jwt_secret`` are application globals; reading them in the body
    (never as defaults) is what keeps a later assignment visible. The host-key
    memo is application-owned and built once at module level -- building it
    here would hand every request an empty cache and silently delete the
    memoization this unauthenticated endpoint depends on.
    """
    from orchestrator.services.vm_idle_access import VMIdleAccessStore

    return ssh_access_routes.SshAccessDependencies(
        store=postgres_db,
        operations=ssh_access_operations.SshAccessDependencies(
            store=postgres_db,
            session_jwt_secret=_session_jwt_secret,
            notifier=notification_service,
            logger=logger,
            host_keys=_ssh_gateway_host_key_cache,
            thread_is_vm_tier=_thread_is_vm_tier,
        ),
        require_approved_user=require_approved_user,
        require_internal=require_internal,
        require_personal_scope=require_personal_scope,
        user_can_access_ide_entity=user_can_access_ide_entity,
        vm_access_store=VMIdleAccessStore(postgres_db),
        vm_provisioner=vm_provisioner,
    )


def _diagnostics_dependencies() -> diagnostics_routes.DiagnosticsDependencies:
    return diagnostics_routes.DiagnosticsDependencies(
        operations=diagnostics_operations.DiagnosticDependencies(
            workspace=workspace_service,
            email_renderer=email_service,
            getenv=os.getenv,
        ),
        require_admin=_require_admin,
    )


def _job_reads_dependencies() -> job_reads_routes.JobReadsDependencies:
    """Compose read/auth ports without evaluating store methods before auth.

    Main's legacy direct callers patch these application collaborators. Resolve
    them per invocation; independently mounted routers supply their own factory.
    Store lifecycle, canonical filter vocabularies and cloud/workspace authority
    remain owned by this application.
    """
    store = postgres_db
    audit = audit_reader
    queries = job_queries.JobQueryDependencies(
        query_jobs=lambda **kwargs: store.query_jobs(**kwargs),
        get_job_statistics=lambda **kwargs: store.get_job_statistics(**kwargs),
        visible_project_ids=lambda actor: user_visible_project_ids(actor, store),
        scope_project_id=mcp_scope_project_id,
        audit_available=lambda: audit.is_available,
        audit_counts=lambda ids: audit.get_audit_counts(ids),
        project_job=lambda job: _with_cloud_review_mode(
            _redact_job_config_override(job)
        ),
        now=lambda: datetime.now(timezone.utc),
        status_filter_values=JOB_STATUS_FILTER_VALUES,
        known_origins=KNOWN_JOB_ORIGINS,
    )
    reads = job_reads.JobReadDependencies(
        store=store,
        audit_reader=audit,
        redact_job=_redact_job_config_override,
        with_cloud_review_mode=_with_cloud_review_mode,
        status_filter_values=JOB_STATUS_FILTER_VALUES,
    )
    return job_reads_routes.JobReadsDependencies(
        store=store,
        queries=queries,
        reads=reads,
        require_approved_user=require_approved_user,
        require_job_access=require_job_access,
        require_project_member=require_project_member,
    )


async def list_jobs(
    request: Request,
    status: list[str] | None = Query(
        default=None,
        description="Lifecycle status(es) to keep (repeatable)",
    ),
    origin: list[str] | None = Query(
        default=None,
        description=(
            "Where the job came from (repeatable): user, session, automation, "
            "loop, officer, subjob, lifecycle, bench. No server-side default — "
            "omit it and every origin is returned."
        ),
    ),
    project_id: list[str] | None = Query(
        default=None,
        description=(
            "Project(s) to keep (repeatable). Pass 'none' for jobs with no project."
        ),
    ),
    has_project: bool | None = Query(
        default=None,
        description="true keeps only jobs with a project, false only those without",
    ),
    include_archived_projects: bool = Query(
        default=False,
        description="Include jobs belonging to archived projects",
    ),
    search: str | None = Query(
        default=None,
        max_length=200,
        description="Match against job description (substring) or id (prefix)",
    ),
    as_of: datetime | None = Query(
        default=None,
        description=(
            "Freeze the window at this creation-time watermark so paging is "
            "not shifted by concurrent inserts. Echoed in the response; pass "
            "it back on subsequent pages."
        ),
    ),
    user_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    include_total: bool = Query(
        default=True,
        description="Compute the capped total. Pass false when paging.",
    ),
) -> dict[str, Any]:
    """Compatibility entry point for existing direct callers of main."""
    return await job_reads_routes.list_jobs(
        request,
        status=status,
        origin=origin,
        project_id=project_id,
        has_project=has_project,
        include_archived_projects=include_archived_projects,
        search=search,
        as_of=as_of,
        user_id=user_id,
        limit=limit,
        offset=offset,
        include_total=include_total,
        dependencies=_job_reads_dependencies(),
    )


def _redact_job_config_override(job: dict[str, Any]) -> dict[str, Any]:
    """Compatibility projection for admission and other existing job callers."""
    return job_projection.redact_job_config_override(
        job,
        vm_mode=lambda: vm_provisioner.mode,
        runtime_incarnation_key=WORKSPACE_RUNTIME_INCARNATION_KEY,
        redact_config_override=redact_config_override,
    )


def _resolve_exported_folder_url(handle_str: str | None) -> Optional[str]:
    """Resolve export URLs through the application-owned cloud router."""
    return job_projection.resolve_exported_folder_url(
        handle_str,
        resolve_backend=lambda backend_id: main_cloud_router.for_backend(backend_id),
    )


def _with_cloud_review_mode(job: dict[str, Any]) -> dict[str, Any]:
    """Compatibility projection for existing job and admission callers."""
    return job_projection.with_cloud_review_mode(
        job, resolve_folder_url=_resolve_exported_folder_url
    )


def _job_admission_scope_dependencies(
    request: Request,
) -> JobAdmissionScopeDependencies:
    """Bind the existing application collaborators without moving their lifecycle."""
    db = postgres_db
    authenticate = require_approved_user

    async def authenticate_forwarded_user() -> tuple[dict[str, Any], str | None]:
        principal = await authenticate(request, db)
        scoped_project = mcp_scope_project_id(principal)
        return principal, str(scoped_project) if scoped_project is not None else None

    return JobAdmissionScopeDependencies(
        store=db,
        thread_project_ids=_thread_project_ids,
        revalidate_thread_project_ids=_revalidate_thread_project_ids,
        authenticate_forwarded_user=authenticate_forwarded_user,
        authorize_upload_reference=authorize_upload_reference,
    )


def _bundled_job_expert_exists(config_name: str) -> bool:
    """Read the application-owned catalogue only when an explicit slug needs it."""
    catalog = _expert_catalog_service()
    if catalog.state.experts is None:
        catalog.state.experts = catalog.scan_experts()
    return any(e.id == config_name for e in catalog.state.experts)


def _job_admission_config_dependencies() -> JobAdmissionConfigDependencies:
    """Bind current collaborators; gates and the catalogue remain deferred."""
    from functools import partial

    from orchestrator.services.job_admission_work_expert import (
        preview_expert_refusals,
    )

    return JobAdmissionConfigDependencies(
        store=postgres_db,
        require_project_access=_require_job_project_access,
        bundled_expert_exists=_bundled_job_expert_exists,
        experts_db_enabled=_is_experts_db_enabled,
        user_experts_enabled=_user_experts_enabled,
        resolve_worker_expert=partial(
            resolve_root_expert, postgres_db, expert_type="worker"
        ),
        preview_expert_refusals=partial(preview_expert_refusals, postgres_db),
    )


def _job_admission_officer_dependencies() -> JobAdmissionOfficerDependencies:
    """Capture application stores; defer ticket imports and reads until needed."""
    from functools import partial

    from orchestrator.services.officer_admission import prepare_officer_admission

    db = postgres_db
    ticket_db = vector_db

    async def fetch_ticket(project_id: str, note_id: str) -> dict[str, Any] | None:
        from orchestrator.services.project_backlog import fetch_ticket_state

        return await fetch_ticket_state(ticket_db, project_id, note_id)

    return JobAdmissionOfficerDependencies(
        store=db,
        prepare_officer=partial(prepare_officer_admission, db),
        fetch_ticket=fetch_ticket,
    )


def _job_admission_workspace_dependencies() -> JobAdmissionWorkspaceDependencies:
    """Bind existing policy owners without evaluating flags or capabilities."""
    return JobAdmissionWorkspaceDependencies(
        store=postgres_db,
        needs_vm=_job_needs_vm,
        needs_sandbox=_job_needs_sandbox,
        check_vm_permission=_check_vm_permission,
        resolve_execution_lane=_resolve_requested_job_execution_lane,
        stateless_default_enabled=lambda: STATELESS_WORKER_DEFAULT_ENABLED,
        stateless_enabled=lambda: STATELESS_WORKER_ENABLED,
        vm_workspaces_on_pod_network=vm_workspaces_on_pod_network,
        provisioner=container_provisioner,
        enforce_grants=_enforce_job_create_grants,
    )


def _job_admission_datasources_dependencies() -> JobAdmissionDatasourcesDependencies:
    """Bind current connector authorities without evaluating selection defaults."""
    from functools import partial

    from orchestrator.services.datasource_policy import default_datasource_selection

    return JobAdmissionDatasourcesDependencies(
        backend_from_override=_backend_from_override,
        inherit_parent_ids=_inherit_parent_datasource_ids,
        filter_implicit_lite_ids=_filter_implicit_lite_datasource_ids,
        authorize_selection=_authorize_thread_datasource_selection,
        default_selection=partial(default_datasource_selection, postgres_db),
        defaults_on_omission=_datasource_defaults_on_omission,
        selection_provenance=_datasource_selection_provenance,
    )


def _job_admission_delivery_dependencies() -> JobAdmissionDeliveryDependencies:
    """Bind the existing delivery-refusal transaction to the current store."""
    from functools import partial

    from orchestrator.services.officer_admission import (
        record_rejected_ticket_delivery_requirement,
    )

    db = postgres_db
    return JobAdmissionDeliveryDependencies(
        store=db,
        record_rejected_ticket_delivery_requirement=partial(
            record_rejected_ticket_delivery_requirement, db
        ),
    )


def _job_admission_creation_dependencies() -> JobAdmissionCreationDependencies:
    """Bind creation owners; defer provisioning imports until their operation."""
    db, forge, cloud = postgres_db, gitea_client, main_cloud_router

    async def admit_officer(**kwargs):
        from orchestrator.services.officer_admission import admit_and_create_job

        return await admit_and_create_job(db, **kwargs)

    async def activate_officer(job_row, **kwargs):
        from orchestrator.services.officer_preflight import ensure_officer_job_activated

        return await ensure_officer_job_activated(db, job_row, **kwargs)

    async def provision_repo(*, job_row):
        from orchestrator.services.job_provisioning import provision_job_repo

        return await provision_job_repo(
            job_row=job_row,
            gitea_client=forge,
            postgres_db=db,
            main_cloud_router=cloud,
        )

    return JobAdmissionCreationDependencies(
        store=db,
        admit_officer=admit_officer,
        activate_officer=activate_officer,
        provision_officer=_provision_officer_ticket_repo,
        provision_repo=provision_repo,
        spawn_scholar=(
            lambda *args, **kwargs: subjob_completion_operations.spawn_scholar_subjob(
                *args,
                **kwargs,
                dependencies=_scholar_completion_dependencies(),
            )
        ),
        resolve_origin=_resolve_submitted_job_origin,
        trigger_dispatch=_trigger_dispatch,
    )


def _job_admission_dependencies(scope_factory) -> JobAdmissionDependencies:
    """Construct the shared operation without eagerly binding later stages."""
    return JobAdmissionDependencies(
        validate_tool_overrides=_with_validated_tool_overrides,
        enforce_readiness=_enforce_readiness_gate,
        scope=scope_factory,
        config=_job_admission_config_dependencies,
        officer=_job_admission_officer_dependencies,
        workspace=_job_admission_workspace_dependencies,
        datasources=_job_admission_datasources_dependencies,
        delivery=_job_admission_delivery_dependencies,
        creation=_job_admission_creation_dependencies,
        redact_result=_redact_job_config_override,
    )


async def _create_bench_job(creator_id: str, command: JobCreate) -> dict[str, Any]:
    """Compose trusted in-process admission with deferred creator revalidation."""
    from functools import partial

    from orchestrator.services.job_admission_creator import authenticate_job_creator

    _strip_raw_officer_claim_context(command)

    def scope_factory() -> JobAdmissionScopeDependencies:
        db = postgres_db
        return JobAdmissionScopeDependencies(
            store=db,
            thread_project_ids=_thread_project_ids,
            revalidate_thread_project_ids=_revalidate_thread_project_ids,
            authenticate_forwarded_user=partial(
                authenticate_job_creator, creator_id, db
            ),
            authorize_upload_reference=authorize_upload_reference,
        )

    return await admit_job(
        command=command,
        actor=JobAdmissionActor(forwarded_user_id=creator_id),
        origin="internal_rest",
        dependencies=_job_admission_dependencies(scope_factory),
    )


async def _cancel_bench_job(job_id: str, caller: dict[str, Any]) -> dict[str, str]:
    """Revalidate one run member, then invoke the application control operation."""

    job = await postgres_db.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    if not await user_can_access_job(caller, postgres_db, job_id):
        raise HTTPException(status_code=403, detail="Not authorized to access this job")
    return await _job_mutation_operations().cancel(job_id, job=job)


def _bench_dependencies():
    """Bind the benchmark task to this application's creation operation."""
    from orchestrator.routers.bench import BenchDependencies
    from orchestrator.services.bench import BenchStore

    return BenchDependencies(
        store=BenchStore(postgres_db),
        create_job=_create_bench_job,
        validate_tool_overrides=_with_validated_tool_overrides,
        audit_reader=audit_reader,
        forge=gitea_client,
        resolve_job_repo=(
            lambda job_id: subjob_output_operations.resolve_job_repo(
                job_id,
                dependencies=subjob_output_operations.SubjobOutputDependencies(
                    store=postgres_db,
                    forge=gitea_client,
                ),
            )
        ),
        cancel_job=_cancel_bench_job,
    )


app.state.bench_dependencies_factory = _bench_dependencies


async def _require_job_project_access(
    principal: dict[str, Any] | None,
    project_id: str | None,
    *,
    denial_detail: str = _INTERNAL_JOB_SCOPE_DENIED,
) -> None:
    """Require current editor access for a user-bound project job."""
    if principal is None or project_id is None or principal.get("is_admin"):
        return
    scoped_project = mcp_scope_project_id(principal)
    if scoped_project is not None and str(scoped_project) != str(project_id):
        raise HTTPException(status_code=403, detail=denial_detail)
    role = await postgres_db.get_user_role_in_project(
        str(project_id), str(principal["id"])
    )
    if role not in {"editor", "owner"}:
        raise HTTPException(status_code=403, detail=denial_detail)


async def get_job(request: Request, job_id: str) -> dict[str, Any]:
    """Compatibility entry point for existing direct callers of main."""
    return await job_reads_routes.get_job(
        request,
        job_id=job_id,
        dependencies=_job_reads_dependencies(),
    )


app.include_router(job_lifecycle_routes.router)


# =============================================================================
# VM Lifecycle Endpoints (optional — requires NATS)
# =============================================================================


app.include_router(job_control_routes.router)


# =============================================================================
# Sudo Approval Gate
# =============================================================================

from orchestrator.services.sudo_gate import sudo_gate  # noqa: E402


# =============================================================================
# Job Completion Handling (orchestrator-side)
# =============================================================================


app.include_router(job_completion_routes.router)


# =============================================================================
# Bulk Fetch Endpoints for Client-Side Caching
# =============================================================================


# NOTE: the former bulk audit/chat/graph endpoints
# (GET /api/jobs/{id}/{audit,chat,graph}/bulk, limit up to 5000) were removed.
# They materialized whole-job histories — including per-row metadata
# (resolved_config, ~127 kB/row) that nothing rendered — and OOM'd the
# orchestrator on large jobs. Consumers now page the lean endpoints instead:
# GET /api/jobs/{id}/audit?lean=true&offset=&limit= (per-step detail via
# /audit/step/{id}), GET /api/jobs/{id}/chat?offset=&limit=, and
# GET /api/graph/changes/{id} for the graph timeline. See
# knowledge-base/knowledge/features/debug_audit_view_refactor.md and
# knowledge-base/knowledge/issues/audit_metadata_config_duplication_ooms_orchestrator.md.


# =============================================================================
# Job Assignment Endpoints
# =============================================================================


def _datasource_payload_dependencies() -> (
    agent_datasource_payload.DatasourcePayloadDependencies
):
    """The two connector gates are **callables**: they are deployment env
    reads, and a suite that rebinds them on ``main`` must still steer."""

    return agent_datasource_payload.DatasourcePayloadDependencies(
        logger=logger,
        mcp_datasources_enabled=_mcp_datasources_enabled,
        mcp_stdio_enabled=_mcp_stdio_enabled,
    )


def _build_datasource_tool_override(*args: Any, **kwargs: Any) -> Any:
    return agent_datasource_payload.build_datasource_tool_override(
        *args, **kwargs, dependencies=_datasource_payload_dependencies()
    )


from orchestrator.services.agent_datasource_payload import (  # noqa: E402
    apply_cloud_storage_override as _apply_cloud_storage_override,
)


def _build_datasources_payload(*args: Any, **kwargs: Any) -> Any:
    return agent_datasource_payload.build_datasources_payload(
        *args, **kwargs, dependencies=_datasource_payload_dependencies()
    )


def _job_assignment_dependencies() -> job_assignment.JobAssignmentDependencies:
    """Rebuilt per call; the completion-control operations are B08's four
    injected callables, the same boundary B04 established rather than a
    second control authority."""

    return job_assignment.JobAssignmentDependencies(
        store=postgres_db,
        logger=logger,
        require_admin=_require_admin,
        vm_mode=lambda: vm_provisioner.mode,
        completion_commands_enabled=lambda: COMPLETION_COMMANDS_ENABLED,
        prepare_job_workspace_runtime=_prepare_job_workspace_runtime,
        prepare_job_repository_before_claim=_prepare_job_repository_before_claim,
        resume_missing_workspace=lambda *args, **kwargs: (
            job_workspace_runtime.resume_missing_workspace(
                *args,
                **kwargs,
                dependencies=_job_workspace_runtime_dependencies(),
            )
        ),
        guard_completion_control=_completion_control_boundary.guard,
        claim_completion_control=_completion_control_boundary.claim,
        abort_completion_control_claim=_completion_control_boundary.abort,
        completion_resume_guard_kwargs=_completion_control_boundary.resume_guard_kwargs,
        dispatch_job_to_agent=lambda job, agent: _job_delivery_operations().dispatch(
            job, agent
        ),
        resume_job_on_agent=lambda job, agent: _job_delivery_operations().resume(
            job, agent
        ),
        trigger_dispatch=_trigger_dispatch,
    )


# =============================================================================
# Statistics Endpoints
# =============================================================================


app.include_router(verification_routes.router)


# --- Agent-facing thread endpoints (no auth, same as /api/agents/register) ---


# ---------------------------------------------------------------------------
# R1.B05 root lane — thread mount rows and cold-session workspace delivery
#
# The implementations live in ``services/thread_mount_rows.py``,
# ``services/thread_workspace_delivery.py`` and
# ``routers/agent_thread_workspace.py``. The wrappers below keep the
# pre-extraction signatures because main code owned by later batches still
# calls them, and because they are what the dependency factories inject: a test
# that patches ``main._thread_project_ids`` must still steer every service that
# consumes it, which only holds while the factory reads the name from this
# module at call time.
#
# Owners of the remaining callers:
#   B06 create/resume/attach — _thread_project_ids, _should_skip_session_folder,
#                              _build_thread_mount_rows,
#                              _assemble_session_attach_payload
#   B04 cloud stage/mounts   — _require_pinned_workspace_credential_owner,
#                              _slugify_mount_name, _cloud_workspace_driver
#   B10 session wake         — the remaining late-bound delivery bridge
# ---------------------------------------------------------------------------


def _thread_mount_dependencies() -> thread_mount_rows.ThreadMountDependencies:
    """Rebuilt per call: ``postgres_db`` and ``main_cloud_router`` are rebound
    during ``lifespan``, and the two payload collaborators belong to B06 and to
    B05's job-preparation lane."""

    return thread_mount_rows.ThreadMountDependencies(
        store=postgres_db,
        cloud_router=main_cloud_router,
        resolve_user_identity_cached=resolve_user_identity_cached,
        externalize_gitea_url=externalize_gitea_url,
        resolve_authorized_thread_datasources=_resolve_authorized_thread_datasources,
        build_datasources_payload=_build_datasources_payload,
        cloud_workspace_driver=_cloud_workspace_driver,
    )


def _slugify_mount_name(name: str) -> str:
    return thread_mount_rows.slugify_mount_name(name)


def _cloud_workspace_driver() -> str:
    return os.getenv("CLOUD_WORKSPACE_DRIVER", "sync").strip().lower() or "sync"


def _should_skip_session_folder(mounts: list[dict[str, Any]]) -> bool:
    return thread_mount_rows.should_skip_session_folder(
        mounts, dependencies=_thread_mount_dependencies()
    )


async def _thread_project_ids(thread_id: str) -> list[str]:
    return await thread_mount_rows.thread_project_ids(
        thread_id, dependencies=_thread_mount_dependencies()
    )


async def _resolve_thread_datasources(
    thread: dict[str, Any],
    metadata: dict[str, Any],
    *,
    project_ids: list[str] | None = None,
) -> list[dict[str, Any]] | None:
    return await thread_mount_rows.resolve_thread_datasources(
        thread,
        metadata,
        project_ids=project_ids,
        dependencies=_thread_mount_dependencies(),
    )


async def _resolve_thread_repositories(
    project_ids: list[str] | None,
    *,
    externalize_urls: bool = False,
) -> list[dict[str, Any]] | None:
    return await thread_mount_rows.resolve_thread_repositories(
        project_ids,
        externalize_urls=externalize_urls,
        dependencies=_thread_mount_dependencies(),
    )


def _thread_workspace_delivery_dependencies() -> (
    thread_workspace_delivery.ThreadWorkspaceDeliveryDependencies
):
    """Rebuilt per call. Every callable is read from this module's namespace so
    an existing ``patch("orchestrator.main._x")`` still steers the service."""

    return thread_workspace_delivery.ThreadWorkspaceDeliveryDependencies(
        store=postgres_db,
        cloud_router=main_cloud_router,
        gitea_client=gitea_client,
        container_provisioner=container_provisioner,
        GrantDenied=GrantDenied,
        LiteWorkspaceConfigError=LiteWorkspaceConfigError,
        backend_from_override=_backend_from_override,
        build_agent_cloud_mount=_build_agent_cloud_mount,
        build_agent_cloud_sync=_build_agent_cloud_sync,
        build_protected_cloud_mount=_build_protected_cloud_mount,
        cloud_workspace_driver=_cloud_workspace_driver,
        grant_violations_detail=_grant_violations_detail,
        inject_lite_workspace_config=_inject_lite_workspace_config,
        inject_thread_dispatch_credentials=_inject_thread_dispatch_credentials,
        protected_cloud_delivery_state=_protected_cloud_delivery_state,
        protected_workspace_wait_payload=_protected_workspace_wait_payload,
        require_pinned_status_identity=_require_pinned_status_identity,
        resolve_session_config=_resolve_session_config,
        resolve_thread_datasources=_resolve_thread_datasources,
        resolve_thread_repositories=_resolve_thread_repositories,
        revalidate_thread_project_ids=_revalidate_thread_project_ids,
        ro_mount_matches_protected_selection=_ro_mount_matches_protected_selection,
        schedule_stateless_workspace_ensure=_schedule_stateless_workspace_ensure,
        thread_accepts_runtime=_thread_accepts_runtime,
        thread_project_ids=_thread_project_ids,
        thread_workspace_backend=_thread_workspace_backend,
        virtual_workspace_rclone_spec=_virtual_workspace_rclone_spec,
        vm_workspaces_on_pod_network=vm_workspaces_on_pod_network,
        require_internal=require_internal,
        capture_session_config=_capture_session_delivery,
    )


async def _require_pinned_workspace_credential_owner(
    thread: dict[str, Any],
    presented_agent_id: str | None,
    presented_runtime_generation: str | None,
    presented_attach_token: str | None,
    *,
    expected_protected_ro_row: Mapping[str, Any] | None = None,
) -> str | None:
    return await thread_workspace_delivery.require_pinned_workspace_credential_owner(
        thread,
        presented_agent_id,
        presented_runtime_generation,
        presented_attach_token,
        expected_protected_ro_row=expected_protected_ro_row,
        dependencies=_thread_workspace_delivery_dependencies(),
    )


async def _apply_thread_config_update_locked(
    thread_id: str,
    thread_row: dict[str, Any] | None,
    config_override: dict[str, Any],
    datasource_ids: list[str] | None,
    *,
    request: Request,
    actor: dict[str, Any] | None,
) -> tuple[dict[str, Any], list[str] | None]:
    """Validate → authorize → enrich → persist a thread config change.

    Shared core of the internal live-session PATCH
    (``agent_update_thread_config``) and the owner-facing
    disconnected-session PATCH (live_session_settings.md Slice C) — the two
    callers differ only in auth, connection gating, and response redaction.
    Authorization (datasource selection + capability grants) is keyed to the
    THREAD OWNER in both cases, so an API caller can never exceed what the
    live pane allows.

    Returns ``(config_override, selected_datasource_ids)`` where the fragment
    is enriched with resolved model transport (``base_url``/``api_key`` +
    explicit ``None`` sentinels) — the internal caller returns it verbatim to
    the agent; browser-facing callers MUST redact it. Persistence is always
    redacted. Emits a ``session_config_updated`` security event on success
    (``actor`` is the resolved caller for owner-facing requests, None for
    internal ones — the recorded path distinguishes the two).
    """
    protected_marker = _protected_cloud_mutation_marker(thread_row)
    if protected_marker == "on" and {
        "workspace",
        "officer",
    }.intersection(config_override):
        # A protected session is safe only while its effective runtime remains
        # the supported Container/non-Officer class.  Generic live config
        # mutation is not an atomic protected-runtime transition protocol, so
        # reject both security-relevant blocks before grant resolution, audit,
        # or persistence.  Even a seemingly harmless partial/no-op block is
        # refused: defaults and expert inheritance make a fragment alone an
        # insufficient proof of the resulting class.
        raise HTTPException(
            status_code=409,
            detail={
                "code": "protected_cloud_runtime_class_fixed",
                "message": (
                    "Protected cloud sessions cannot change workspace tier or "
                    "Officer mode."
                ),
            },
        )
    if "officer" in config_override and thread_row and thread_row.get("project_id"):
        post = await postgres_db.get_project_officer(str(thread_row["project_id"]))
        if post and str(post.get("thread_id") or "") == str(thread_id):
            raise HTTPException(
                status_code=409,
                detail=(
                    "The commissioned officer block is owned by the Officer "
                    "Post; use the project Officer Post endpoint so durable "
                    "and runtime configuration change atomically."
                ),
            )
    if thread_row and thread_row.get("execution_lane") == "stateless":
        # Generic config mutation is not the workspace-upgrade protocol. Refuse
        # an already-drifted row and allow only same-tier workspace tuning.
        current_backend = _require_stateless_workspace(thread_row)
        if "officer" in config_override:
            # Runtime PATCH historically accepted this block without the
            # create-time validator. Normalize booleans and reject unknown
            # fields before evaluating the proposed immutable session class.
            normalized_officer = _validated_session_officer_override(config_override)
            config_override["officer"] = normalized_officer or {}

        metadata = thread_metadata_object(thread_row)
        persisted_override = metadata.get("config_override") or {}
        if not isinstance(persisted_override, dict):
            persisted_override = {}
        proposed_override = _deep_merge_dicts(
            persisted_override,
            config_override,
        )
        class_refusal = _stateless_session_class_refusal(proposed_override)
        if class_refusal is not None:
            # Creation materializes the fully resolved class booleans into the
            # request layer, so this merge is authoritative even if the expert
            # or account default changes later. Never persist a fragment that
            # would move a live queue-served thread onto pinned-only wake
            # machinery without an atomic lane-transition protocol.
            raise HTTPException(
                status_code=409,
                detail=(
                    "A stateless session cannot enable pinned-only lifecycle "
                    f"behavior ({class_refusal})"
                ),
            )
        if "workspace" in config_override:
            workspace_patch = config_override.get("workspace")
            if not isinstance(workspace_patch, dict) or (
                "backend" in workspace_patch
                and workspace_patch.get("backend") != current_backend
            ):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "A stateless session cannot change its workspace tier "
                        "through generic config mutation"
                    ),
                )

    if "tools" in config_override:
        # Runtime updates use the same registry vocabulary as session creation
        # and job creation.  A fragment this boundary will not honour is a 400
        # from here, not a silent discard: the previous closed-group filter
        # replaced `tools` with the four accepted groups and dropped the rest,
        # so a live "turn research off" was acknowledged and never applied.
        # The names are also checked against their own category, because the
        # loader resolves a name against the global registry rather than the
        # key it arrived under.
        accepted_tools = _validated_tool_overrides(config_override)
        if accepted_tools:
            config_override["tools"] = accepted_tools
        else:
            # `tools: {}` only — it asks for nothing, so there is nothing to
            # honour and nothing to report as changed.
            config_override.pop("tools", None)

    if "delegation" in config_override:
        # The gate half of the Delegation toggle (see create_thread). Same
        # validator as create, so the two write paths cannot disagree about
        # what the block means; malformed is a 400, never a silent drop.
        from orchestrator.services.session_create_overrides import (
            SessionOverrideError,
            validate_delegation_override,
        )

        try:
            accepted_delegation = validate_delegation_override(
                config_override["delegation"]
            )
        except SessionOverrideError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if accepted_delegation:
            config_override["delegation"] = accepted_delegation
        else:
            config_override.pop("delegation", None)

    # Audit summary is computed PRE-enrichment so it names only the keys the
    # caller actually sent (enrichment adds llm.api_key/base_url internally).
    change_summary = _config_change_summary(config_override, datasource_ids)

    # Live datasource change (live_session_settings.md Slice B): authorize
    # the requested full selection exactly like create does — including the
    # lite-tier/repository rule against the thread's CURRENT workspace
    # backend (a live add is create-like; only the attach-time
    # revalidation deliberately passes None) — then fold the resulting
    # datasource tool-category flip into the grant-checked fragment so a
    # datasource_tools-denied principal fails HERE at the PATCH, not at
    # the next attach.
    selected_ds_ids: list[str] | None = None
    selected_ds_revisions: dict[str, int] | None = None
    datasource_selection_provenance: dict[str, Any] | None = None
    grant_fragment = config_override
    if datasource_ids is not None:
        if thread_row is None:
            raise HTTPException(status_code=404, detail="Thread not found")
        requested_ids = [str(v) for v in datasource_ids]
        current_metadata = thread_metadata_object(thread_row)
        try:
            canonical_requested = {str(UUID(value)) for value in requested_ids}
        except (TypeError, ValueError) as exc:
            raise HTTPException(
                status_code=403,
                detail="One or more selected connectors are unavailable",
            ) from exc
        removed_ids = (
            set(current_metadata.get("datasource_ids") or []) - canonical_requested
        )
        if removed_ids:
            removed_rows = await postgres_db.get_datasource_policy_rows(
                list(removed_ids)
            )
            if any(row.get("type") == "credentials" for row in removed_rows):
                raise HTTPException(
                    status_code=409,
                    detail="Credential connectors stay attached for the lifetime of the session",
                )
        target_project_ids = await _thread_project_ids(thread_id)
        if thread_row.get("user_id"):
            owner = await postgres_db.get_user(str(thread_row["user_id"]))
            if owner is None:
                # Same generic denial as create — no enumeration oracle.
                raise HTTPException(
                    status_code=403,
                    detail="One or more selected connectors are unavailable",
                )
            (
                selected_ds_ids,
                selected_ds_revisions,
            ) = await _authorize_thread_datasource_selection(
                owner,
                requested_ids,
                workspace_backend=_thread_workspace_backend(thread_row),
                target_project_ids=target_project_ids,
                effective_work_owner_id=str(thread_row["user_id"]),
            )
        else:
            # Ownerless/system threads have no ambient authority. A live edit
            # may narrow or preserve the already-materialized set, but cannot
            # use the trusted-inheritance seam to add an arbitrary UUID.
            metadata = thread_row.get("metadata") or {}
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except (json.JSONDecodeError, TypeError):
                    metadata = {}
            persisted_ids = (
                metadata.get("datasource_ids") if isinstance(metadata, dict) else []
            ) or []
            try:
                requested_set = {str(UUID(str(value))) for value in requested_ids}
                persisted_set = {str(UUID(str(value))) for value in persisted_ids}
            except (TypeError, ValueError) as exc:
                raise HTTPException(
                    status_code=403,
                    detail="One or more selected connectors are unavailable",
                ) from exc
            if not requested_set.issubset(persisted_set):
                raise HTTPException(
                    status_code=403,
                    detail="One or more selected connectors are unavailable",
                )
            (
                selected_ds_ids,
                selected_ds_revisions,
            ) = await _authorize_thread_datasource_selection(
                None,
                requested_ids,
                workspace_backend=_thread_workspace_backend(thread_row),
                target_project_ids=target_project_ids,
                trusted_system_inheritance=True,
            )

        resolved_ds = await postgres_db.resolve_datasources_for_thread(
            datasource_ids=selected_ds_ids,
            project_ids=target_project_ids,
        )
        flip = _build_datasource_tool_override(resolved_ds, None)
        # THE FLIP WINS, and the order is load-bearing. It used to be the other
        # way round, safe only because the request's tools were filtered down
        # to four non-connector groups first. Now that every category is
        # honoured, a request could send `tools.sql: []` and mask a
        # datasource_tools violation from the PDP below — while attach applies
        # the flip LAST anyway (_build_datasource_tool_override updates the
        # request's tools with the datasource categories), so the session would
        # get connector tools the grant check never saw. Modelling attach
        # exactly is what keeps this fragment honest.
        grant_fragment = {
            **config_override,
            "tools": {
                **(config_override.get("tools") or {}),
                **flip.get("tools", {}),
            },
        }
        datasource_selection_provenance = await _datasource_selection_provenance(
            datasource_ids=selected_ds_ids,
            policy_revisions=selected_ds_revisions,
            origin="explicit",
            effective_work_owner_id=(
                str(thread_row["user_id"]) if thread_row.get("user_id") else None
            ),
            actor=actor,
            project_ids=target_project_ids,
            creation_path=(
                "live_thread_internal" if actor is None else "live_thread_rest"
            ),
        )

    # Layer 2 (fail loud): a runtime config change must also fit the owner's
    # grants — reject a denied permission_mode/model with 422 instead of
    # persisting a config the session can't run (an API-direct or stale-UI
    # escalation past the user's ceiling; the cockpit greys these out
    # client-side). Ownerless/standalone threads (user_id NULL) aren't
    # subject to a user's grants — skip. Admin owner bypasses.
    # knowledge-base/knowledge/issues/session_permission_mode_grant_denied_ready_timeout.md
    if thread_row and thread_row.get("user_id"):
        await _enforce_session_create_grants(
            grant_fragment,
            user_id=str(thread_row["user_id"]),
            project_ids=(
                [str(thread_row["project_id"])] if thread_row.get("project_id") else []
            ),
        )

    # Enrich endpoint-backed model swaps with base_url + api_key so the
    # persisted override is complete. Without this, a hot-swap to a
    # custom-endpoint model leaves the next session attach pointing at
    # the default OpenAI base.
    llm_section = config_override.get("llm")
    if llm_section and llm_section.get("model"):
        if thread_row:
            user_id = str(thread_row["user_id"]) if thread_row.get("user_id") else None
            project_id = (
                str(thread_row["project_id"]) if thread_row.get("project_id") else None
            )
            resolved_keys = await postgres_db.resolve_api_keys_for_job(
                user_id=user_id, project_id=project_id
            )
            llm_section = dict(llm_section)
            await _inject_model_credentials(
                section=llm_section,
                model_id=llm_section["model"],
                user_id=user_id,
                resolved_keys=resolved_keys,
            )
            # A model swap must fully determine its transport. Any field
            # resolution didn't set becomes an explicit None so the
            # agent-side deep_merge CLEARS the previous model's value
            # instead of inheriting it (e.g. swapping off an
            # endpoint-backed model must not keep its base_url).
            for transport_key in ("provider", "base_url", "api_key"):
                llm_section.setdefault(transport_key, None)
            config_override["llm"] = llm_section

    # Persist WITHOUT secrets — the agent rebuilds its LLM from the enriched
    # dict returned below, and resume re-injects from source. The explicit
    # None transport sentinels stay in the stored copy so the deep-merge
    # clears the previous model's transport; resume re-injection treats them
    # as absent (see _inject_thread_dispatch_credentials).
    ok = await postgres_db.merge_thread_config_override(
        thread_id, redact_config_override(config_override)
    )
    if not ok:
        raise HTTPException(status_code=404, detail="Thread not found")

    # Persist the accepted selection only after every check above passed.
    # The category flip is NOT merged into config_override — the closed
    # session tools vocabulary would drop it anyway; the agent re-fetches
    # GET /api/agents/threads/{id}/workspace and applies the categories
    # directly to its live session config, and every attach path re-derives
    # them from metadata.datasource_ids.
    if selected_ds_ids is not None:
        from shared.credential_connectors import CredentialConnectorAttachedError

        try:
            updated = await postgres_db.set_thread_datasource_ids(
                thread_id,
                selected_ds_ids,
                datasource_policy_revisions=selected_ds_revisions,
                datasource_selection_provenance=datasource_selection_provenance,
            )
        except CredentialConnectorAttachedError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except DatasourcePolicyConflictError as exc:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Connector policy changed while updating the session; "
                    "retry the request"
                ),
            ) from exc
        if not updated:
            raise HTTPException(status_code=404, detail="Thread not found")

    # Config-change audit (live_session_settings.md Slice C): key paths only,
    # fired after every persist step succeeded. log_security_event never
    # raises, so a broken audit trail can't fail the update it documents.
    await log_security_event(
        postgres_db,
        resource_type="thread",
        event_type="session_config_updated",
        user=actor,
        resource_id=thread_id,
        detail=change_summary,
        request=request,
    )
    if selected_ds_ids is not None:
        # The snapshot policy and pinned merge use the same authorized derived
        # categories. Persisted author overrides still omit this live binding.
        config_override = {
            **config_override,
            "tools": grant_fragment.get("tools", {}),
        }
    return config_override, selected_ds_ids


# =============================================================================
# Persistent Agent — session surface (R1.B10)
# =============================================================================
#
# Detail, state, controls, tool groups/preview and rename are served by
# routers/thread_session.py; history and citations by routers/thread_history.py;
# stream, input, queue and interrupt by routers/thread_transport.py; permission
# decisions and magic links by routers/thread_permissions.py. Each router is
# included where its routes used to be declared, so route order is unchanged.
# The factories are rebuilt per call and read this module's collaborators at
# call time.


def _session_tool_view_dependencies() -> (
    session_tool_view_operations.SessionToolViewDependencies
):
    return session_tool_view_operations.SessionToolViewDependencies(
        store=postgres_db,
        user_experts_enabled=_user_experts_enabled,
        resolve_runner_grants=_resolve_runner_grants,
        acknowledged_grant_strip=_acknowledged_grant_strip,
        prefetch_roster_refs=_prefetch_roster_refs,
        agent_toolset_measurement=_agent_toolset_measurement,
        session_config_dependencies=_session_config_dependencies,
    )


def _thread_session_dependencies() -> thread_session_routes.ThreadSessionDependencies:
    return thread_session_routes.ThreadSessionDependencies(
        store=postgres_db,
        require_thread_owner=require_thread_owner,
        require_approved_user=require_approved_user,
        resolve_cloud_session_url=_resolve_cloud_session_url,
        resolve_session_config=_resolve_session_config,
        enforce_session_create_grants=_enforce_session_create_grants,
        tool_view=_session_tool_view_dependencies(),
    )


app.state.thread_session_dependencies_factory = lambda: _thread_session_dependencies()
app.include_router(thread_session_routes.router)


app.include_router(thread_lifecycle_routes.end_router)


# Resume-time session-folder provisioning is fire-and-forget so /resume stays
# fast — but the agent reads its cloud config within ~150ms of attach, while
# provisioning needs several WebDAV round-trips (~5s measured). The attach won
# that race every time, so the agent read NULL handle columns, got
# ``cloud_sync_degraded``, and ran with ``workspace_sync = None`` for the
# session's WHOLE life (persistent_app.py has no rebuild path). Registering the
# task lets the attach paths await it instead of racing it.
# knowledge-history/done/session_resume_cloud_sync_race_late_provision.md
_late_cloud_setup_tasks: dict[str, "asyncio.Task[None]"] = {}

# Ceiling on how long an attach waits for in-flight session-folder
# provisioning. Comfortably above the ~5s observed cost, low enough that a
# wedged cloud delays a resume rather than hanging it — on timeout we fall
# through to the pre-fix behaviour (attach, possibly degraded).
LATE_CLOUD_SETUP_ATTACH_TIMEOUT_S = 15


app.include_router(thread_lifecycle_routes.resume_router)


app.include_router(thread_lifecycle_routes.rewind_router)


def _thread_history_dependencies() -> thread_history_routes.ThreadHistoryDependencies:
    return thread_history_routes.ThreadHistoryDependencies(
        store=postgres_db,
        vector_db=vector_db,
        require_thread_owner=require_thread_owner,
    )


app.state.thread_history_dependencies_factory = lambda: _thread_history_dependencies()
app.include_router(thread_history_routes.router)


# The per-turn pinned input locks are process state; this application owns one
# registry (R1.B10). The stateless lane never uses it.
_thread_turn_locks = ThreadTurnLocks()


def _pinned_forwarding_dependencies() -> (
    pinned_forwarding_operations.PinnedForwardingDependencies
):
    return pinned_forwarding_operations.PinnedForwardingDependencies(
        store=postgres_db,
        workspace_suspension=workspace_suspension_service,
        protected_cloud_delivery_state=_protected_cloud_delivery_state,
    )


def _stateless_input_dependencies() -> (
    stateless_input_operations.StatelessInputDependencies
):
    return stateless_input_operations.StatelessInputDependencies(
        store=postgres_db,
        schedule_stateless_workspace_ensure=_schedule_stateless_workspace_ensure,
    )


def _thread_transport_dependencies() -> (
    thread_transport_routes.ThreadTransportDependencies
):
    return thread_transport_routes.ThreadTransportDependencies(
        store=postgres_db,
        require_thread_owner=require_thread_owner,
        require_approved_user=require_approved_user,
        forwarding=_pinned_forwarding_dependencies(),
        stateless_input=_stateless_input_dependencies(),
        turn_locks=_thread_turn_locks,
    )


app.state.thread_transport_dependencies_factory = (
    lambda: _thread_transport_dependencies()
)
app.include_router(thread_transport_routes.router)


async def _thread_input_stateless(
    thread: dict,
    content: str,
    expected_conversation_revision: int | None = None,
) -> dict[str, Any]:
    """Compatibility entry for ``operator_cli/stateless_wake_acceptance.py`` (B12).

    The operator harness bootstraps this module and admits fixture turns
    through it; the operation lives in ``services/stateless_input_admission``.
    """
    return await stateless_input_operations.admit_stateless_input(
        thread,
        content,
        expected_conversation_revision,
        dependencies=_stateless_input_dependencies(),
    )


def _magic_link_cockpit_url() -> str:
    return email_service.cockpit_url or "http://localhost:4200"


def _session_attention_dependencies() -> (
    session_attention_operations.SessionAttentionDependencies
):
    """Attention sleep, permission reminders and permission-decision wake.

    The recycler is read through a provider because startup assigns it after
    the provisioners; retirement operations are recomposed per call.
    """
    return session_attention_operations.SessionAttentionDependencies(
        store=postgres_db,
        container_provisioner=container_provisioner,
        workspace_suspension=workspace_suspension_service,
        persistent_provisioner=persistent_provisioner,
        persistent_thread_recycler=lambda: _persistent_thread_recycler,
        emit_session_provisioning_failure=_emit_session_provisioning_failure,
        thread_retirement_operations=_thread_retirement_operations,
        notification_service=notification_service,
        cockpit_url=_magic_link_cockpit_url,
    )


def _thread_permission_dependencies() -> (
    thread_permission_routes.ThreadPermissionDependencies
):
    return thread_permission_routes.ThreadPermissionDependencies(
        store=postgres_db,
        require_thread_owner=require_thread_owner,
        notification_service=notification_service,
        cockpit_url=_magic_link_cockpit_url,
        wake_after_permission_decision=functools.partial(
            session_attention_operations.wake_after_permission_decision,
            dependencies=_session_attention_dependencies(),
        ),
    )


app.state.thread_permission_dependencies_factory = (
    lambda: _thread_permission_dependencies()
)
app.include_router(thread_permission_routes.router)


# =============================================================================
# Catalogue composition and remaining configuration callers
# =============================================================================


def _get_config_dir() -> Path:
    """Compatibility reader for startup and pending configuration callers (B12)."""
    return resolve_config_dir(__file__)


def _provider_catalog_dependencies() -> (
    provider_catalog_routes.ProviderCatalogDependencies
):
    return provider_catalog_routes.ProviderCatalogDependencies(
        service=ProviderCatalogService(
            store=postgres_db,
            discovery=discovery_service,
            probe=probe_endpoint_models,
            subscriptions=subscription_discovery,
        ),
        require_admin=_require_admin,
    )


def _model_catalog_dependencies() -> model_catalog_routes.ModelCatalogDependencies:
    from shared.runtime.core import loader

    resources = app.state.catalogue_resources

    async def approved_user(request: Request) -> dict[str, Any]:
        return await require_approved_user(request, postgres_db)

    return model_catalog_routes.ModelCatalogDependencies(
        service=ModelCatalogService(
            store=postgres_db,
            probe=probe_endpoint_models,
            get_config_dir=resources.get_config_dir,
            load_settings_matrix=resources.load_settings_matrix,
            settings_for_family=loader.bundled_settings_for_family,
            family_detector=family_matcher.detect_family,
            reasoning_capability=loader.reasoning_capability,
        ),
        require_admin=_require_admin,
        require_approved_user=approved_user,
    )


def _config_catalog_dependencies() -> config_catalog_routes.ConfigCatalogDependencies:
    from shared.runtime.core import loader

    return config_catalog_routes.ConfigCatalogDependencies(
        service=ConfigCatalogService(
            store=postgres_db,
            # Preserve the original catalogue's loader project-root path;
            # do not substitute _get_config_dir if its override differs.
            project_root=loader.get_project_root,
            settings_for_family=loader.bundled_settings_for_family,
            guardrails_for_family=loader.bundled_guardrails_for_family,
            prompt_resolver=loader.PromptMatrixResolver,
            instruction_resolver=loader.InstructionMatrixResolver,
        ),
        require_admin=_require_admin,
    )


def _manifest_dependencies() -> manifest_routes.ManifestDependencies:
    from orchestrator.services.manifests import ManifestService
    from orchestrator.services.manifest_resources import ManifestResourceService

    async def approved_user(request: Request) -> dict[str, Any]:
        return await require_approved_user(request, postgres_db)

    async def admit_manifest(prepared, resource, user, *, request=None):
        return await _manifest_execution_service().admit(
            prepared, resource, user, request=request
        )

    async def activate_project(prepared, user, *, request=None, validate_only):
        from orchestrator.services.manifest_projects import validate_project_activation

        return await validate_project_activation(
            postgres_db,
            prepared,
            user,
            request=request,
            validate_only=validate_only,
            validate_post_patch=_validated_officer_post_patch,
            enforce_auto_pull=_enforce_officer_auto_pull_release,
        )

    return manifest_routes.ManifestDependencies(
        service=ManifestService(),
        require_approved_user=approved_user,
        resources=ManifestResourceService(
            postgres_db, admit_job=admit_manifest, project_activation=activate_project
        ),
        execution=_manifest_execution_service,
        trigger_dispatch=_trigger_dispatch,
    )


def _manifest_execution_service():
    from kubernetes.client import NetworkingV1Api
    from orchestrator.services.generic_harness_runtime import GenericHarnessRuntime
    from orchestrator.services.manifest_execution import ManifestExecutionService
    from orchestrator.services.manifest_workspace_runtime import (
        ManifestWorkspaceRuntime,
    )
    from orchestrator.services.manifest_workspaces import ManifestWorkspaceService

    if not agent_provisioner._k8s_available:
        raise HTTPException(503, "Kubernetes manifest hosting is unavailable.")
    network_api = NetworkingV1Api()
    namespace = os.environ.get(
        "MANIFEST_NAMESPACE", agent_provisioner._namespace + "-native"
    )
    workspace_namespace = namespace
    workspaces = ManifestWorkspaceService(
        postgres_db,
        ManifestWorkspaceRuntime(
            container_provisioner._core_api, network_api, namespace=workspace_namespace
        ),
        namespace=workspace_namespace,
        default_image=container_provisioner._workspace_image,
        storage_class_name=container_provisioner._storage_class,
        harness_namespace=namespace,
        vm_provisioner=vm_provisioner,
    )
    return ManifestExecutionService(
        postgres_db,
        runtime=GenericHarnessRuntime(
            agent_provisioner._core_api, network_api, namespace=namespace
        ),
        namespace=namespace,
        workspace=workspaces,
        srw_image=agent_provisioner._agent_image,
        authorize_datasources=_authorize_thread_datasource_selection,
        cancel_srw=lambda job, **guard: _job_mutation_operations().cancel(
            str(job["id"]), job=job, **guard
        ),
        native_hosting_enabled=os.environ.get(
            "MANIFEST_NETWORK_ISOLATION_VERIFIED", "false"
        ).lower()
        == "true",
        harness_egress=os.environ.get("MANIFEST_HARNESS_EGRESS", "[]"),
    )


def _expert_catalog_service() -> ExpertCatalogService:
    """Bind current stores/policy to this application's shared catalogue state."""
    resources = app.state.catalogue_resources
    from orchestrator.services.manifest_store import ManifestStore

    return ExpertCatalogService(
        ExpertCatalogDependencies(
            store=postgres_db,
            manifests=ManifestStore(postgres_db)
            if getattr(postgres_db, "manifests_ready", False) is True
            else None,
            state=app.state.expert_catalog_state,
            get_config_dir=resources.get_config_dir,
            load_settings_matrix=resources.load_settings_matrix,
            experts_enabled=_is_experts_db_enabled,
            skills_enabled=_is_skills_db_enabled,
            account_defaults_layer=_account_defaults_layer,
            visible_project_ids=user_visible_project_ids,
            with_validated_tool_overrides=_with_validated_tool_overrides,
            looks_like_uuid=_looks_like_uuid,
            forge=gitea_client,
        )
    )


def _expert_catalog_dependencies() -> (
    expert_catalog_routes.ExpertCatalogRouteDependencies
):
    """Compose catalogue HTTP guards and request-bound canonical save policy."""
    from functools import partial

    catalog = _expert_catalog_service()
    authoring = ExpertAuthoringService(
        store=postgres_db,
        catalog=catalog,
        resolve_default_models=_resolve_default_models,
        prefetch_roster_refs=_prefetch_roster_refs,
    )

    async def approved_user(request: Request) -> dict[str, Any]:
        return await require_approved_user(request, postgres_db)

    async def project_member(
        request: Request, project_id: str, *, allow_archived: bool = True
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        return await require_project_member(
            request, postgres_db, project_id, allow_archived=allow_archived
        )

    async def project_owner(
        request: Request, project_id: str, *, allow_archived: bool = True
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        return await require_project_owner(
            request, postgres_db, project_id, allow_archived=allow_archived
        )

    return expert_catalog_routes.ExpertCatalogRouteDependencies(
        catalog=catalog,
        authoring=authoring,
        require_approved_user=approved_user,
        require_admin=_require_admin,
        require_project_member=project_member,
        require_project_owner=project_owner,
        write_policy_factory=lambda request: ExpertWritePolicy(
            enforce_save=partial(_enforce_expert_save, request),
            enforce_save_prelude=partial(_enforce_expert_save_prelude, request),
            strip_save_grants=_strip_save_grants,
        ),
    )


async def _gather_in_scope_skills(
    user_id: str | None, project_ids: list[str] | None = None
) -> dict[str, Any]:
    """B05 compatibility for session preparation, job dispatch and resume."""
    return await _expert_catalog_service().gather_in_scope_skills(user_id, project_ids)


def _bundled_expert_bundle(expert_id: str) -> dict[str, Any] | None:
    """B05 compatibility for session configuration review."""
    return _expert_catalog_service().bundled_expert_bundle(expert_id)


# =============================================================================
# Auth Endpoints
# =============================================================================


# =============================================================================
# System readiness
# =============================================================================


# nosec: public auth-bootstrap (Bearer-required, intentionally pre-approval — onboarding first paint)
@app.get("/api/system/readiness")
async def system_readiness(request: Request) -> dict[str, Any]:
    """Return the cockpit-facing readiness signal.

    Authenticated, but not admin-gated — the onboarding screen calls this
    on first paint. Auth-required because the response leaks details
    about whether catalog rows exist (a low-stakes leak, but still
    user-scoped). See ``readiness_service.compute_readiness`` for the
    payload shape.
    """
    await get_current_user(request, postgres_db)
    return await readiness_service.compute_readiness(postgres_db)


async def _enforce_readiness_gate() -> None:
    """Raise 503 when the LLM stack isn't ready.

    Called from ``POST /api/jobs`` and ``POST /api/persistent/threads``
    so dispatch hard-fails rather than silently routing to a chat model
    that doesn't exist. The error body carries the same ``missing_*``
    fields the cockpit reads from ``/api/system/readiness`` so the UI
    can deep-link to the right admin page from either source.
    """
    readiness = await readiness_service.compute_readiness(postgres_db)
    if readiness.get("ready"):
        return
    raise HTTPException(
        status_code=503,
        detail=readiness_service.gate_error_detail(readiness),
    )


async def _resolve_preference_defaults() -> dict[str, Any]:
    """Compatibility adapter for app-global callers of preference defaults."""
    return await _resolve_app_preference_defaults(
        postgres_db, role_base=_role_base_or_empty, environ=os.environ
    )


# =============================================================================
# AI Subscriptions — proxy management surface (Admin-only)
# =============================================================================
# Settings → AI Subscriptions and Admin → Models both sit on this surface.
# Every upstream call goes through orchestrator.services.subscriptions, which
# owns the management credential; nothing below ever puts a key, an OAuth token
# or an authorization code into a response body or a log line.
#
# The legacy ``/api/codex/*`` routes are kept at the bottom as Codex-scoped
# compatibility wrappers for clients that have not moved yet. They filter to
# Codex accounts explicitly — a mixed pool must never leak into a Codex answer.
#
# Design: knowledge-base/knowledge/features/subscription_proxy.md §5, §8.


async def _require_admin(request: Request) -> dict[str, Any]:
    """Retain call-time composition bindings for remaining main-module routes."""
    return await require_admin_gate(
        request,
        postgres_db,
        resolve_user=require_approved_user,
        audit=log_security_event,
    )


# =============================================================================
# Capability grants — admin CRUD + audit + kill-switch + self-introspection
# (User-Defined Experts, Slice 2; decisions 8, 9, 23). The PEPs live near
# _check_vm_permission; these are the management/read surfaces.
# =============================================================================


# =============================================================================
# Project Endpoints
# =============================================================================


# -- Project Datasources (N:M) -----------------------------------------------


@app.post(
    "/api/projects/{project_id}/jobs",
    operation_id="create_project_job_api_projects__project_id__jobs_post",
)
async def create_project_job(
    request: Request, project_id: str, job: PublicJobCreateBody
) -> dict[str, Any]:
    """Create a job within a project — delegates to create_job. Requires editor or higher."""
    await require_project_member(
        request, postgres_db, project_id, min_role="editor", allow_archived=False
    )
    job.project_id = project_id
    return await job_lifecycle_routes.admit_job_request(
        request,
        job,
        dependencies=_job_lifecycle_route_dependencies(),
    )


async def list_project_jobs(
    request: Request,
    project_id: str,
    status: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> list[dict[str, Any]]:
    """Compatibility entry point for existing direct callers of main."""
    return await job_reads_routes.list_project_jobs(
        request,
        project_id=project_id,
        status=status,
        limit=limit,
        dependencies=_job_reads_dependencies(),
    )


# =============================================================================
# Project Knowledge Base Endpoints
# =============================================================================
