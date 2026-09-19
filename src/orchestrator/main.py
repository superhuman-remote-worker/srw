"""FastAPI backend for the Debug Cockpit.

Run with:
    uvicorn orchestrator.main:app --reload --port 8085
"""

import asyncio
import functools
import html
import json
import logging
import os
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path
import urllib.parse
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
from typing import Any, Literal, Optional  # noqa: E402
from uuid import UUID, uuid4  # noqa: E402

from fastapi import (  # noqa: E402
    FastAPI,
    HTTPException,
    Query,
    Request,
)
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from fastapi.responses import (  # noqa: E402
    HTMLResponse,
    JSONResponse,
    Response,
    StreamingResponse,
)

from pydantic import (  # noqa: E402
    BaseModel,
    ConfigDict,
    Field,
    model_validator,
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
from orchestrator.services.vm_workspace_recovery_store import (  # noqa: E402
    VMWorkspaceRecoveryStore,
)
from orchestrator.services.vm_provisioning_cleanup import recycle_provisioning_vm  # noqa: E402
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
    agent_sha_is_current as _agent_sha_is_current,
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
    deliver_officer_note as _deliver_officer_note,
    kick_drain as _kick_session_wake_drain,
    kick_event_drain as _kick_officer_event_drain,
    maybe_wake_session,
    notify_all_officers,
    notify_officer,
    notify_owning_officers,
    session_wake_sweeper_loop,
)
from orchestrator.services.session_state_snapshot import (  # noqa: E402
    build_session_state_snapshot,
)
from orchestrator.services.thread_control_inbox import (  # noqa: E402
    ControlAdmissionError,
    ControlAdmissionNotReady,
    admit_thread_control,
    find_existing_thread_control,
)
from orchestrator.services.thread_interrupt_inbox import (  # noqa: E402
    InterruptAdmissionError,
    admit_thread_interrupt,
    find_existing_thread_interrupt,
)
from shared.thread_presence import (  # noqa: E402
    DEFAULT_PRESENCE_RENEW_SECONDS,
    DEFAULT_PRESENCE_TTL_SECONDS,
    promote_expired_stateless_pauses,
    refresh_thread_presence,
)
from shared.pinned_session_identity import PinnedSessionBinding  # noqa: E402
from shared.pinned_session_identity import PinnedJobRecipient  # noqa: E402, F401
from orchestrator.services.stateless_workspace_gate import (  # noqa: E402
    thread_metadata_object,
)
from orchestrator.services.stale_verification_sweeper import (  # noqa: E402
    stale_verification_sweeper_loop,
)
from orchestrator.services.dispatch_guards import (  # noqa: E402
    VM_CAPACITY_POLL,
    VM_GOLDEN_POLL,
    VM_PREPARATION_POLL,
    VM_PARK_PREPARATION,
    VM_HEADSCALE_POLL,
    VM_PARK_CAPACITY,
    VM_PARK_EXHAUSTED,
    VM_PARK_GOLDEN,
    VM_PARK_INITIALIZATION,
    VM_PARK_HEADSCALE,
    VM_PARKED,
    VM_PROVISION,
    VM_RECYCLE,
    VM_WAIT,
    preemption_blocked_reason,
    resume_lane_applies,
    vm_provisioning_decision,
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
    LegacyWorkspaceUsageLedgerAdapter,
    TypedUsageDailyRollup,
    UsageV2QueryService,
    probe_schema_capabilities,
    infrastructure_metering_runtime_loop,
    typed_usage_rollup_loop,
)
from orchestrator.services.infrastructure_metering.compute_activation import (  # noqa: E402
    ComputeActivation,
    ComputeActivationStore,
    compute_scope_configuration_diagnostic,
)
from orchestrator.services.infrastructure_metering.ingestion import (  # noqa: E402
    InfrastructureIngestionService,
    run_inventory_generation_loop,
)
from orchestrator.services.infrastructure_metering.inventory import InventoryStore  # noqa: E402
from orchestrator.services.infrastructure_metering.materializer import (  # noqa: E402
    PublicationContractError,
    StoragePublicationPolicy,
)
from orchestrator.services.infrastructure_metering.storage_assets import (  # noqa: E402
    StorageActivation,
    StorageAssetStore,
    StorageSourceActivation,
)
from orchestrator.services.infrastructure_metering.storage_mapping import (  # noqa: E402
    StorageResourceMappingStore,
)
from orchestrator.services.openrouter_pricing import llm_pricing_sync_loop  # noqa: E402
from orchestrator.services.audit_partitions import (  # noqa: E402
    ensure_partitions as ensure_audit_partitions,
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
from orchestrator.services.session_runtime_admission import (  # noqa: E402
    ThreadRuntimeAuthority,
    pinned_binding_invalid_detail,
    protected_cloud_marker_state,
    same_thread_runtime_authority,
    thread_runtime_authority,
    thread_runtime_refusal_detail,
)
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
    resolve_workspace_runtime,
)

# Datasource type → tool-category map, shared with the agent's session attach
# path so the two boundaries can't drift (live_session_settings.md P0.2).
from shared.runtime.core.tool_policy import (  # noqa: E402
    enumerate_only_members,
)
from shared.runtime.core.tool_report import (  # noqa: E402
    compose_tool_view,
    tool_groups_from_view,
)

# Tool -> category, for annotating replayed history (_stamp_tool_categories).
# Same registry the agent's live SSE frames read, so the two can't disagree.
from shared.tool_catalog import TOOL_REGISTRY  # noqa: E402
from orchestrator.services.nats_bridge import nats_bridge  # noqa: E402
from orchestrator.services.vm_provisioner import vm_provisioner  # noqa: E402
from orchestrator.services.vm_workspace_config import vm_provisioning_options  # noqa: E402
from orchestrator.services.vm_readiness import vm_readiness_prober  # noqa: E402
from orchestrator.services.container_provisioner import (  # noqa: E402
    WORKSPACE_RUNTIME_INCARNATION_KEY,
    WorkspaceRuntimeAttestation,  # noqa: F401
    WorkspaceTeardownIdentity,  # noqa: F401 - shared teardown identity re-export
    container_provisioner,
)
from orchestrator.services.workspace_lifecycle import (  # noqa: E402
    EnsureOutcome,
    WorkspaceOwner,
    ensure_workspace,
)
from orchestrator.services.session_provisioner import (  # noqa: E402
    ensure_session_workspace,
    reconcile_session_workspaces,
)
from orchestrator.services.docker_provisioner import docker_provisioner  # noqa: E402
from orchestrator.services.persistent_provisioner import persistent_provisioner  # noqa: E402
from orchestrator.services.persistent_recycler import (  # noqa: E402
    PersistentThreadRecycler,
    read_recycle_record,
)
from orchestrator.services.pinned_agent_authority import (  # noqa: E402
    reconcile_legacy_pinned_agent_authority,
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
from orchestrator.services import headless_notifications  # noqa: E402
from orchestrator.services.brand import TRAVERTINE as _BRAND  # noqa: E402
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

# Durable pinned retirement is retried only after the exact local agent is
# absent/offline and this grace has elapsed.  This is deliberately longer than
# ordinary local teardown: a live runtime owns memory/git/event-writer drain
# after Begin and before its final settlement request.
_PINNED_RETIREMENT_RETRY_GRACE_SECONDS = max(
    0, int(os.environ.get("PINNED_RETIREMENT_RETRY_GRACE_SECONDS", "900"))
)
_PINNED_RETIREMENT_PREFLIGHT_GRACE_SECONDS = max(
    1, int(os.environ.get("PINNED_RETIREMENT_PREFLIGHT_GRACE_SECONDS", "300"))
)

# S36 explicitly overrides the workspace Pod's ordinary 120-second grace with
# a 10-second UID-preconditioned delete. Keep the exact-absence proof below the
# pinned agent's 60-second report timeout while leaving room for API latency.
_COMPLETION_S36_EXACT_ABSENCE_TIMEOUT_SECONDS = 45.0

# Dispatcher lock prevents concurrent dispatch (double-assignment)
_dispatch_lock = asyncio.Lock()

# Track jobs with pending pause requests (prevent re-preemption)
_pause_pending_job_ids: set[str] = set()

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


async def _retire_orphaned_pinned_runtime(candidate: Mapping[str, Any]) -> bool:
    """Retire one offline incarnation through the normal pinned End funnel.

    The candidate is a read-only hint. ``begin_pinned_thread_retirement``
    rechecks the exact generation, agent, attach attempt and offline state in
    its row transaction before closing admission. A recovered/rebound agent is
    therefore preserved even when it changes immediately after the sweep.
    """

    thread_id = str(candidate.get("id") or "")
    generation = str(candidate.get("runtime_generation") or "")
    agent_id = str(candidate.get("agent_id") or "")
    attach_token = (
        str(candidate.get("runtime_attach_token"))
        if candidate.get("runtime_attach_token") is not None
        else None
    )
    if not thread_id or not generation or not agent_id:
        return False
    settle_status: Literal["ended", "suspended"] = (
        "suspended"
        if str(candidate.get("status") or "") in {"awaiting_user", "suspended"}
        else "ended"
    )

    thread = await postgres_db.get_thread(thread_id)
    if not isinstance(thread, Mapping):
        return False
    try:
        await _thread_retirement_operations().end_thread_flow(
            thread_id,
            dict(thread),
            permanent=False,
            force=True,
            expected_runtime_generation=generation,
            expected_agent_id=agent_id,
            expected_attach_token=attach_token,
            require_expected_agent_offline=True,
            settle_status=settle_status,
        )
    except HTTPException as exc:
        if exc.status_code == 409:
            logger.info(
                "Offline-runtime retirement lost authority for thread %s; "
                "preserving the current runtime",
                thread_id,
            )
            return False
        raise
    return True


async def _retry_pending_pinned_retirement(candidate: Mapping[str, Any]) -> bool:
    """Retry one exact durable retirement after its local actor disappeared."""

    context = candidate.get("runtime_retirement_context") or {}
    if isinstance(context, str):
        try:
            context = json.loads(context)
        except (TypeError, ValueError):
            return False
    if not isinstance(context, Mapping):
        return False
    thread_id = str(candidate.get("id") or "")
    generation = str(candidate.get("runtime_generation") or "")
    token = str(candidate.get("runtime_retirement_token") or "")
    settle_status = str(context.get("settle_status") or "")
    if (
        not thread_id
        or not generation
        or not token
        or settle_status
        not in {
            "ended",
            "suspended",
        }
    ):
        return False
    if str(context.get("generation") or "") != generation:
        return False
    permanent = bool(candidate.get("runtime_retirement_permanent"))
    if permanent and settle_status != "ended":
        return False
    expected_agent_id = (
        str(context.get("agent_id")) if context.get("agent_id") is not None else None
    )
    expected_attach_token = (
        str(context.get("runtime_attach_token"))
        if context.get("runtime_attach_token") is not None
        else None
    )
    thread = await postgres_db.get_thread(thread_id)
    if not isinstance(thread, Mapping):
        return False
    runtime_exposed = _retirement_context_runtime_exposed(
        {
            "generation": generation,
            "token": token,
            "permanent": permanent,
            "context": context,
        }
    )
    if runtime_exposed and not _retirement_has_exact_local_quiescence(
        {
            "generation": generation,
            "token": token,
            "permanent": permanent,
            "context": context,
        },
        thread,
    ):
        recovered = await _recover_captured_sandbox_process_zero(
            {
                "generation": generation,
                "token": token,
                "permanent": permanent,
                "context": context,
            }
        )
        if not recovered:
            logger.warning(
                "Pinned retirement crash recovery could not prove process zero "
                "for thread %s (backend %r); the durable marker stays pending",
                thread_id,
                context.get("workspace_backend"),
            )
            return False
        thread = await postgres_db.get_thread(thread_id)
        if thread is None:
            return True
        if str(thread.get("runtime_retirement_token") or "") != token:
            return str(thread.get("status") or "") in {"ended", "suspended"}
    try:
        result = await _thread_retirement_operations().end_thread_flow(
            thread_id,
            dict(thread),
            permanent=permanent,
            force=True,
            expected_runtime_generation=generation,
            expected_agent_id=expected_agent_id,
            expected_attach_token=expected_attach_token,
            # Exact physical process zero above supersedes the lossy `offline`
            # status hint.  A token itself rejects heartbeats, so offline can
            # never be used as a quiescence proof.
            require_expected_agent_offline=False,
            settle_status=settle_status,
            local_runtime_quiesced=runtime_exposed,
        )
    except HTTPException as exc:
        # Exact authority loss is a successful refusal; a retryable cleanup
        # failure remains represented by the durable marker for a later pass.
        # Either way it is worth a line — an all-refusing sweep used to be
        # indistinguishable from an idle one.
        if exc.status_code in {409, 503}:
            logger.warning(
                "Durable pinned retirement retry refused for thread %s (HTTP %s): %s",
                thread_id,
                exc.status_code,
                exc.detail,
            )
            return False
        raise
    return str(result.get("status") or "") in {
        "deleted" if permanent else settle_status
    }


async def stale_agent_detector(shutdown_event: asyncio.Event) -> None:
    """Background task that reconciles agent state every 60 seconds.

    Two dimensions of reconciliation:

    1. Heartbeat freshness — agents that stopped reporting get marked offline,
       which in turn flips their threads to 'ended' and pauses their jobs.
    2. Self-reported consistency — agents that *are* heartbeating but report
       internally inconsistent state (working with no job, session bound to
       an ended thread) get flipped back to 'ready' so the dispatcher can
       reuse the slot. These zombies pass the heartbeat check and would
       otherwise hold pool slots indefinitely.

    Finally, offline agents older than 24h are GC'd to keep the table small.
    """
    logger.info("Stale agent detector started")

    async def _step(name: str, coro) -> Any:
        """Run one reconciliation step isolated from its siblings.

        Every step here repairs an INDEPENDENT inconsistency; a bug in one
        must degrade only that dimension. The 2026-07-11 incident proved the
        alternative: a bind-type bug in the graph-progress sweep silently
        disabled orphan-job recovery (and everything else after it) for ~36h
        because all steps shared one try block. See
        knowledge-history/done/stale_agent_detector_sql_crash_disables_recovery_sweeps.md.
        Returns None on failure — callers treat that as "no rows".
        """
        try:
            return await coro
        except Exception as e:
            logger.error(f"Stale agent detector step '{name}' failed: {e}")
            return None

    while not shutdown_event.is_set():
        try:
            # 1. Heartbeat-based: mark non-responsive agents offline
            offline_agents = await _step(
                "offline_marking",
                postgres_db.mark_stale_agents_offline(timeout_minutes=3),
            )
            if offline_agents:
                logger.info(
                    f"Marked {len(offline_agents)} agent(s) as offline due to "
                    "missed heartbeats"
                )
                # Officer wake (centurion S4), scoped to the project each dead
                # agent was serving (derived from its assigned/last job): a
                # failing agent in one project is that officer's news, not the
                # whole roster's (owner ruling, 2026-08). Agents with no
                # derivable project keep the historical fleet-wide fan-out —
                # a warm-pool agent dying genuinely is capacity news for every
                # officer. 10-min debounce on 'fleet' keeps a flapping node
                # from spamming.
                offline_by_project: dict[str, int] = {}
                unattributed_offline = 0
                for agent_row in offline_agents:
                    agent_project = agent_row.get("project_id")
                    if agent_project:
                        offline_by_project[str(agent_project)] = (
                            offline_by_project.get(str(agent_project), 0) + 1
                        )
                    else:
                        unattributed_offline += 1
                if offline_by_project:
                    await _step(
                        "officer_fleet_offline",
                        notify_owning_officers(
                            postgres_db,
                            {
                                project_id: {
                                    "summary": (
                                        f"{n} agent(s) marked offline "
                                        "(missed heartbeats)"
                                    )
                                }
                                for project_id, n in offline_by_project.items()
                            },
                            source="fleet",
                            dedup_key="fleet:agents_offline",
                        ),
                    )
                if unattributed_offline:
                    await _step(
                        "officer_fleet_offline",
                        notify_all_officers(
                            postgres_db,
                            source="fleet",
                            dedup_key="fleet:agents_offline",
                            payload={
                                "summary": (
                                    f"{unattributed_offline} agent(s) marked "
                                    "offline (missed heartbeats)"
                                )
                            },
                        ),
                    )
                _kick_officer_event_drain(postgres_db)

            # 2. Consistency-based: release slots held by zombie agents
            stuck_working = await _step(
                "stuck_working", postgres_db.mark_stuck_working_agents_ready()
            )
            if stuck_working:
                logger.info(
                    f"Released {stuck_working} agent(s) stuck in 'working' with no job"
                )
                _trigger_dispatch()
            stalled_working = await _step(
                "graph_progress_stall",
                postgres_db.mark_stalled_working_agents_by_graph_progress(
                    stall_minutes=10
                ),
            )
            if stalled_working:
                logger.info(
                    "Released %d working agent(s) with no graph-progress "
                    "for the stall interval",
                    stalled_working,
                )
                _trigger_dispatch()
            stuck_session = await _step(
                "stuck_session", postgres_db.mark_stuck_session_agents_ready()
            )
            if stuck_session:
                logger.info(
                    f"Released {stuck_session} agent(s) stuck in 'session' "
                    f"on ended thread"
                )

            # 2b. STOPGAP — reap session agents wedged with NO bound thread/job.
            # mark_stuck_session_agents_ready (above) can't reach these: its
            # predicate needs thread_id IS NOT NULL, and a *live* agent
            # re-asserts 'session' on every 5s heartbeat so a flip-to-ready
            # never sticks — deleting the pod is the only actuation that does.
            # Scoped to thread_id + current_job_id both NULL (holds nothing
            # user-visible), so it never touches a thread-bound live session
            # (the 2026-06-10 incident). Proper fix = the intent/observed split
            # in knowledge-base/knowledge/features/unified_instance_lifecycle.md. Tracking:
            # knowledge-base/knowledge/issues/lifecycle_session_agents_without_thread_never_drain.md
            orphaned_sessions = await _step(
                "orphaned_session_reap",
                postgres_db.reap_orphaned_session_agents(grace_minutes=5),
            )
            for orphan in orphaned_sessions or []:
                deleted = await _step(
                    "orphaned_session_pod_delete",
                    agent_provisioner.delete_agent_pod(
                        orphan["hostname"],
                        expected_pod_uid=str(orphan.get("pod_uid") or ""),
                    ),
                )
                logger.warning(
                    "Reaped orphaned session agent %s (pod=%s, deleted=%s): "
                    "'session' with no thread/job past grace",
                    orphan["id"],
                    orphan["hostname"],
                    deleted,
                )

            # 2c. A failed warm attach rotates G1 -> unbound G2 and records an
            # append-only outcome. The request-local scheduler is only the
            # latency fast path; this durable scan is the restart/transient-
            # failure owner for headless sessions. Every task remains keyed to
            # the exact retired tuple and may provision only the named G2.
            attach_abort_successors = await _step(
                "attach_abort_successors",
                postgres_db.list_retryable_thread_attach_abort_successors(limit=25),
            )
            for successor in attach_abort_successors or []:
                if not isinstance(successor, Mapping):
                    continue
                _schedule_attach_abort_successor(
                    str(successor.get("thread_id") or ""),
                    retired_runtime_generation=str(
                        successor.get("retired_runtime_generation") or ""
                    ),
                    retired_attach_token=str(
                        successor.get("retired_attach_token") or ""
                    ),
                    retired_agent_id=str(successor.get("retired_agent_id") or ""),
                )

            # 3. Propagate: exact pinned runtimes bound to offline agents go
            # through begin -> exact cleanup -> settle. The old set-based
            # status write made Resume visible before cleanup and let stale
            # name deletes destroy its successor.
            ended_candidates = await _step(
                "orphaned_threads_ended", postgres_db.mark_orphaned_threads_ended()
            )
            if ended_candidates:
                retired = 0
                for candidate in ended_candidates:
                    if isinstance(candidate, Mapping) and await _step(
                        "retire_orphaned_pinned_runtime",
                        _retire_orphaned_pinned_runtime(candidate),
                    ):
                        retired += 1
                if retired:
                    logger.info(
                        "Retired %d offline pinned runtime(s) through exact End",
                        retired,
                    )

            # 3b. Paused offline runtimes use the same safe funnel. They settle
            # as resumable ended sessions rather than exposing an automatic
            # suspended wake before exact cleanup has completed.
            suspended_candidates = await _step(
                "orphaned_threads_suspended",
                postgres_db.mark_orphaned_threads_suspended(),
            )
            if suspended_candidates:
                retired = 0
                for candidate in suspended_candidates:
                    if isinstance(candidate, Mapping) and await _step(
                        "retire_orphaned_paused_runtime",
                        _retire_orphaned_pinned_runtime(candidate),
                    ):
                        retired += 1
                if retired:
                    logger.info(
                        "Retired %d paused offline pinned runtime(s) through exact End",
                        retired,
                    )

            # 3c. A hidden Begin is a short-lived admission preflight, not an
            # End instruction. If its owner dies before the append-only
            # authorization edge, exact expiry reopens the same runtime. The
            # row lock makes authorize-vs-expire choose exactly one outcome.
            expired_preflights = await _step(
                "stale_pinned_retirement_preflights",
                postgres_db.abort_stale_pinned_retirement_preflights(
                    grace_seconds=_PINNED_RETIREMENT_PREFLIGHT_GRACE_SECONDS,
                    limit=25,
                ),
            )
            if expired_preflights:
                logger.warning(
                    "Reopened %d abandoned pinned retirement preflight(s)",
                    len(expired_preflights),
                )

            # 3d. Authorized Begin is durable.  If an orchestrator/agent dies after it
            # closes admission, Resume must remain blocked until another
            # replica finishes the immutable captured disposition.  Only
            # sufficiently old markers whose exact actor is absent/offline
            # are nominated; the shared advisory lock serializes replicas.
            pending_retirements = await _step(
                "pending_pinned_retirements",
                postgres_db.list_retryable_pinned_retirements(
                    grace_seconds=_PINNED_RETIREMENT_RETRY_GRACE_SECONDS,
                    limit=25,
                ),
            )
            if pending_retirements:
                retired = 0
                for candidate in pending_retirements:
                    if isinstance(candidate, Mapping) and await _step(
                        "retry_pending_pinned_retirement",
                        _retry_pending_pinned_retirement(candidate),
                    ):
                        retired += 1
                if retired:
                    logger.info(
                        "Completed %d durable pinned retirement retry(s)", retired
                    )
                unresolved = len(pending_retirements) - retired
                if unresolved:
                    logger.warning(
                        "%d durable pinned retirement(s) remain unresolved after "
                        "this pass; each refusal is logged above",
                        unresolved,
                    )

            # Static Docker containers survive owner termination.  Their
            # exact inventory lease plus the terminal job/thread row is the
            # durable retry owner for managed-repository ssh-agent process
            # retirement.  This sweep closes crashes between a terminal DB
            # transition and cleanup, retries typed retirement failures, and
            # reclaims an external operation whose bounded deadline elapsed.
            docker_retirement_claims = await _step(
                "terminal_docker_workspace_retirement_claim",
                postgres_db.claim_terminal_docker_workspace_retirements(),
            )
            for claim in docker_retirement_claims or []:
                await _step(
                    "terminal_docker_workspace_retirement_settle",
                    docker_provisioner.settle_claimed_terminal_workspace_retirement(
                        claim
                    ),
                )

            # 4. Legacy compatibility: pre-lease pinned jobs assigned to
            # offline/non-working agents -> paused. The database predicate
            # excludes every non-NULL lease; ordering cannot steal a leased
            # row from the authoritative expiry circuit below.
            recovered = await _step(
                "orphaned_job_recovery",
                postgres_db.recover_orphaned_jobs(
                    completion_commands_enabled=COMPLETION_COMMANDS_ENABLED
                ),
            )
            if recovered:
                logger.info(
                    f"Recovered {recovered.count} orphaned job(s) from offline agents"
                )
                # Scoped to each job's owning project officer (owner ruling,
                # 2026-08): a recovered job is not fleet news. Jobs with no
                # project — or projects with no commissioned officer — notify
                # nobody.
                orphans_by_project: dict[str, list[str]] = {}
                for job in recovered.recovered_jobs:
                    if job.project_id:
                        orphans_by_project.setdefault(job.project_id, []).append(
                            job.job_id
                        )
                if orphans_by_project:
                    await _step(
                        "officer_fleet_orphans",
                        notify_owning_officers(
                            postgres_db,
                            {
                                project_id: {
                                    "summary": (
                                        f"{len(job_ids)} orphaned job(s) "
                                        "auto-paused for re-dispatch "
                                        "(agent offline): "
                                        + ", ".join(
                                            str(job_id)[:8] for job_id in job_ids[:5]
                                        )
                                    )
                                }
                                for project_id, job_ids in orphans_by_project.items()
                            },
                            source="fleet",
                            dedup_key="fleet:orphans_recovered",
                        ),
                    )
                    _kick_officer_event_drain(postgres_db)
                _trigger_dispatch()

            # 4b. Job execution lease: expired lease == orphaned, decided
            # purely by the DB clock — no agents-table join, no dependency on
            # step 1 having run. This is the sole automatic
            # infrastructure-loss authority for leased pinned rows; step 4 is
            # constrained to genuine pre-lease NULL-lease compatibility rows.
            lease_recovery_kwargs: dict[str, Any] = {
                "completion_commands_enabled": COMPLETION_COMMANDS_ENABLED,
            }
            if getattr(audit_reader, "is_available", False):
                lease_recovery_kwargs["audit_fingerprint_provider"] = (
                    audit_reader.get_audit_counts_strict
                )
            lease_recovery = await _step(
                "lease_expiry_recovery",
                postgres_db.recover_expired_lease_jobs(**lease_recovery_kwargs),
            )
            recovered_lease_ids = (
                lease_recovery.recovered_job_ids if lease_recovery is not None else ()
            )
            circuit_trips = (
                lease_recovery.circuit_trips if lease_recovery is not None else ()
            )
            for _job_id in recovered_lease_ids:
                logger.warning(
                    "Job %s recovered by lease expiry — its agent stopped "
                    "renewing (pod died, wedged, or a failed dispatch handoff); "
                    "re-queued for dispatch",
                    _job_id,
                )
            if recovered_lease_ids:
                # Recoveries below the containment threshold notify only each
                # job's owning project officer (owner ruling, 2026-08: one
                # livelocked job must not wake every officer each sweep —
                # ~10-min all night, in one observed case). Jobs with no
                # project, or projects with no commissioned officer, notify
                # nobody. The circuit-trip event below is unchanged: it is
                # inserted transactionally at the owning project's post.
                leases_by_project: dict[str, list[str]] = {}
                for job in lease_recovery.recovered_jobs:
                    if job.project_id:
                        leases_by_project.setdefault(job.project_id, []).append(
                            job.job_id
                        )
                if leases_by_project:
                    await _step(
                        "officer_fleet_leases",
                        notify_owning_officers(
                            postgres_db,
                            {
                                project_id: {
                                    "summary": (
                                        f"{len(job_ids)} job(s) recovered by "
                                        "lease expiry: "
                                        + ", ".join(
                                            str(job_id)[:8] for job_id in job_ids[:5]
                                        )
                                    )
                                }
                                for project_id, job_ids in leases_by_project.items()
                            },
                            source="fleet",
                            dedup_key="fleet:lease_recovered",
                        ),
                    )
                    _kick_officer_event_drain(postgres_db)
            for trip in circuit_trips:
                logger.error(
                    "Job %s parked by redispatch circuit after %s unchanged "
                    "lease recoveries (project=%s, officer_route=%s, queued=%s)",
                    trip.job_id,
                    trip.unchanged_recoveries,
                    trip.project_id,
                    trip.officer_destination,
                    trip.notification_queued,
                )
            if circuit_trips:
                # The recovery transaction already inserted the owning
                # project's durable outbox row. This is only a fast drain kick;
                # vacant posts retain the same incident in their durable ledger.
                _kick_officer_event_drain(postgres_db)
            if recovered_lease_ids:
                _trigger_dispatch()

            # 5. GC: drop offline agent rows older than 24h
            gc_count = await _step(
                "offline_gc", postgres_db.gc_offline_agents(retention_hours=24)
            )
            if gc_count:
                logger.info(f"GC'd {gc_count} offline agent record(s) > 24h old")
        except Exception as e:
            # Last resort — individual steps are isolated above, so anything
            # landing here is a bug in the loop scaffolding itself.
            logger.error(f"Error in stale agent detector: {e}")

        # Wait 60 seconds or until shutdown
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=60.0)
            break  # Shutdown signaled
        except asyncio.TimeoutError:
            pass  # Continue loop

    logger.info("Stale agent detector stopped")


async def agent_pool_reconciler(shutdown_event: asyncio.Event) -> None:
    """Background task that maintains the dynamic agent pool.

    Runs every 60 seconds:
    - Ensures MIN_AGENTS warm pods exist (instant dispatch)
    - Reaps completed / stale / unstartable agent pods (single dispatcher)

    Drift-based draining lives in ``lifecycle_reconciler_loop`` now —
    this loop only owns capacity (warm pool + scale-down) and crash GC.
    """
    logger.info("Agent pool reconciler started")
    while not shutdown_event.is_set():
        try:
            if agent_provisioner.is_available:
                await agent_provisioner.ensure_warm_pool()
                await agent_provisioner.reap_pods()
                await agent_provisioner.scale_down_idle()
        except Exception as e:
            logger.error("Error in agent pool reconciler: %s", e)

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=60.0)
            break
        except asyncio.TimeoutError:
            pass

    logger.info("Agent pool reconciler stopped")


async def lifecycle_reconciler_loop(
    shutdown_event: asyncio.Event,
    reconciler: InstanceLifecycleReconciler,
) -> None:
    """Background task driving the unified instance lifecycle reconciler.

    Runs every 60 seconds. The reconciler delegates to per-kind
    managers (``AgentInstanceManager`` etc.) for drift detection and
    drain. Crash detection still flows through ``reap_pods`` in the
    sibling ``agent_pool_reconciler`` for now; consolidation is a
    follow-up.
    """
    logger.info("Lifecycle reconciler loop started")
    while not shutdown_event.is_set():
        try:
            await reconciler.tick()
        except Exception:
            logger.exception("Lifecycle reconciler tick failed")

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=60.0)
            break
        except asyncio.TimeoutError:
            pass

    logger.info("Lifecycle reconciler loop stopped")


async def sudo_expiration_sweeper(shutdown_event: asyncio.Event) -> None:
    """Background task that denies expired sudo approval requests.

    Runs every 15 seconds. For each expired request, publishes a denial
    to the stored NATS reply subject so the daemon unblocks. Expired
    vm_upgrade requests additionally fail their frozen job loudly
    (``vm_upgrade_expired``) instead of leaving it invisibly wedged.
    """
    from orchestrator.services.sudo_gate import sudo_gate  # noqa: E402

    logger.info("Sudo expiration sweeper started")
    while not shutdown_event.is_set():
        try:
            await sudo_gate.sweep_expired()
        except Exception as e:
            logger.error("Error in sudo expiration sweeper: %s", e)

        try:
            await _job_control_operations().fail_expired_vm_upgrade_jobs()
        except Exception as e:
            logger.error("Error failing expired vm_upgrade jobs: %s", e)

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=15.0)
            break
        except asyncio.TimeoutError:
            pass

    logger.info("Sudo expiration sweeper stopped")


async def ide_session_ttl_sweeper(shutdown_event: asyncio.Event) -> None:
    """Background task that expires IDE sessions past their TTL.

    Runs every 60 seconds. Checks active/idle sessions for:
    - Max lifetime exceeded (default: 4 hours)
    - Idle timeout exceeded (default: 30 minutes, only for 'idle' status)
    """
    logger.info("IDE session TTL sweeper started")
    while not shutdown_event.is_set():
        try:
            expired = await ide_session_service.check_ttl_all()
            if expired:
                logger.info("IDE session sweeper: expired %d sessions", expired)
        except Exception as e:
            logger.error("Error in IDE session TTL sweeper: %s", e)

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=60.0)
            break
        except asyncio.TimeoutError:
            pass

    logger.info("IDE session TTL sweeper stopped")


async def ro_reader_reconciler_loop(shutdown_event: asyncio.Event) -> None:
    """Leader-gated periodic sweep of orphaned protected-mode RO grants.

    Revoke-on-teardown alone is not enough (a crash/killed pod can skip it), so
    this independently revokes any active ``cloud_ro_mounts`` grant whose thread
    is gone/ended (design §8.1.4). Runs every 15 minutes.
    """
    from orchestrator.services.ro_reader_reconciler import reconcile_orphaned_ro_mounts

    logger.info("RO reader reconciler started")
    while not shutdown_event.is_set():
        try:
            await reconcile_orphaned_ro_mounts(
                postgres_db=postgres_db, router=main_cloud_router
            )
        except Exception as e:
            logger.error("Error in RO reader reconciler: %s", e)

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=900.0)
            break
        except asyncio.TimeoutError:
            pass

    logger.info("RO reader reconciler stopped")


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


async def workspace_idle_sweeper(shutdown_event: asyncio.Event) -> None:
    """Background loop: reconciles failed/missing session workspaces.

    Idle suspension and teardown now live in the lifecycle reconciler's reap
    path (``services/lifecycle/reconciler.py`` → ``WorkspaceInstanceManager``),
    which snapshots-then-deletes reapable workspaces and force-deletes ones it
    can never reach (bounded retry) instead of keeping them alive forever.

    This loop retains only the session-workspace recovery reconcile —
    recreating failed/missing workspaces for active sessions — which is
    independent of idle policy. Runs every 60 seconds.
    """
    logger.info("Workspace idle sweeper started (reconcile-only)")
    while not shutdown_event.is_set():
        # Session workspace reconcile (safety-net): recreate failed/missing
        # workspaces for active sessions. Runs regardless of whether idle
        # suspension is enabled — recovering a wedged workspace is independent
        # of idle policy. This is the session-side equivalent of the job
        # dispatcher's per-cycle workspace reconcile.
        # (reconcile_session_workspaces never raises; the try/except is a
        # belt-and-suspenders guard so a future change can't kill this loop.)
        try:
            await reconcile_session_workspaces(
                db=postgres_db,
                provisioner=container_provisioner,
                suspension=workspace_suspension_service,
            )
        except Exception as e:
            logger.error("Error in session workspace reconcile: %s", e)

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=60.0)
            break
        except asyncio.TimeoutError:
            pass

    logger.info("Workspace idle sweeper stopped")


async def code_server_settings_sweeper(shutdown_event: asyncio.Event) -> None:
    """Background loop: reconcile per-user code-server IDE settings.

    Workspaces are network-isolated from the orchestrator (egress is denied), so
    instead of the workspace pushing changes, the orchestrator pulls inward on a
    ~10-minute cycle: it reads each active workspace's code-server config files
    (settings.json, keybindings.json, snippets) over SSH and merges any newer
    edits into the owning user's stored settings (``users.settings['ide']``).
    Conflict resolution is by filesystem mtime — newest wins, per file — so the
    cycle order across a user's workspaces doesn't matter. See
    orchestrator/services/ide_settings.py.
    """
    if os.environ.get("IDE_SETTINGS_SYNC_ENABLED", "true").lower() not in (
        "1",
        "true",
        "yes",
    ):
        # Park instead of returning: run_when_leader re-creates a loop that
        # exits on its next poll (~1s), which would respawn+log this every
        # second for the whole leadership tenure.
        logger.info("Code-server settings sweeper disabled (IDE_SETTINGS_SYNC_ENABLED)")
        await shutdown_event.wait()
        return

    from orchestrator.services.ide_settings import (
        IdeSettingsStore,
        OpenVsxClassifier,
        _coerce_context,
        capture_ide_profile,
        evict_dead_workspaces,
        list_ide_extensions,
        pull_ide_config,
        reconcile_extensions,
        reconcile_ide_settings,
        reconcile_vm_ide_workspace,
        resolve_ssh_target,
        is_vm_capture_context,
    )

    interval = float(os.environ.get("IDE_SETTINGS_SYNC_INTERVAL_S", "600"))
    store = IdeSettingsStore(postgres_db)
    classifier = OpenVsxClassifier()  # cache persists across cycles for this process
    logger.info("Code-server settings sweeper started (interval=%.0fs)", interval)
    while not shutdown_event.is_set():
        try:
            workspaces = await postgres_db.list_active_ide_workspaces()
            vm_workspaces = [
                workspace
                for workspace in workspaces
                if is_vm_capture_context(workspace.get("context"))
            ]
            workspaces = [
                workspace
                for workspace in workspaces
                if not is_vm_capture_context(workspace.get("context"))
            ]
            workspaces = await evict_dead_workspaces(
                workspaces, container_provisioner, postgres_db
            )
            if workspaces or vm_workspaces:
                # Dial-target visibility: stable Service DNS survives pod
                # restarts; raw IPs are legacy rows predating the headless
                # Service and go stale with the pod.
                _dns_dials = sum(
                    1
                    for w in workspaces
                    if str(
                        (
                            (_coerce_context(w.get("context")) or {}).get(
                                "workspace_container"
                            )
                            or {}
                        ).get("host")
                        or ""
                    ).endswith(".svc.cluster.local")
                )
                logger.info(
                    "IDE settings sweeper: %d workspace(s), %d dialed via "
                    "stable service DNS",
                    len(workspaces),
                    _dns_dials,
                )
                count = await reconcile_ide_settings(store, workspaces, pull_ide_config)
                if count:
                    logger.info("IDE settings sweeper: synced %d file(s)", count)
                try:
                    ext_changed = await reconcile_extensions(
                        store, workspaces, list_ide_extensions, classifier
                    )
                    if ext_changed:
                        logger.info(
                            "IDE settings sweeper: synced %d extension(s)", ext_changed
                        )
                except Exception as e:  # noqa: BLE001
                    logger.error("Error reconciling extensions: %s", e)

                # Capture license/globalStorage + non-Open-VSX bytes to S3 when a
                # workspace's content signature changed (Phase B). Signature-gated
                # inside capture_ide_profile so most cycles are a cheap no-op.
                if snapshot_service.is_available:
                    from orchestrator.services.ide_profile_store import IdeProfileStore

                    profile = IdeProfileStore(
                        snapshot_service._s3, snapshot_service._bucket
                    )
                    for ws in workspaces:
                        uid = ws.get("user_id")
                        if not uid:
                            continue
                        tgt = resolve_ssh_target(_coerce_context(ws.get("context")))
                        if not tgt:
                            continue
                        try:
                            await capture_ide_profile(
                                store, str(uid), tgt[0], tgt[1], profile
                            )
                        except Exception as e:  # noqa: BLE001
                            logger.warning("ide profile capture failed: %s", e)
                    for ws in vm_workspaces:
                        try:
                            await reconcile_vm_ide_workspace(
                                store=store,
                                workspace=ws,
                                db=postgres_db,
                                vm_provisioner=vm_provisioner,
                                classifier=classifier,
                                profile_store=profile,
                            )
                        except Exception as e:  # noqa: BLE001
                            logger.warning("VM IDE capture failed: %s", e)
                else:
                    for ws in vm_workspaces:
                        await reconcile_vm_ide_workspace(
                            store=store,
                            workspace=ws,
                            db=postgres_db,
                            vm_provisioner=vm_provisioner,
                            classifier=classifier,
                        )
        except Exception as e:
            logger.error("Error in code-server settings sweeper: %s", e)

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=interval)
            break
        except asyncio.TimeoutError:
            pass

    logger.info("Code-server settings sweeper stopped")


async def snapshot_gc_sweeper(shutdown_event: asyncio.Event) -> None:
    """Background task that runs snapshot garbage collection daily.

    Applies retention policies, soft-deletes expired snapshots, and
    purges items past the 7-day grace period.
    """
    logger.info("Snapshot GC sweeper started")
    gc_interval = 24 * 3600  # 24 hours

    while not shutdown_event.is_set():
        try:
            if snapshot_service.is_available:
                stats = await snapshot_service.run_gc()
                if stats.get("soft_deleted") or stats.get("purged"):
                    logger.info("Snapshot GC: %s", stats)
        except Exception as e:
            logger.error("Error in snapshot GC sweeper: %s", e)

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=gc_interval)
            break
        except asyncio.TimeoutError:
            pass

    logger.info("Snapshot GC sweeper stopped")


async def pinned_agent_create_intent_reconciler(
    shutdown_event: asyncio.Event,
) -> None:
    """Promote exact response-lost Pod/PVC creates after process restart.

    This leader never originates a credential-bearing Kubernetes effect.  It
    only observes the immutable labels/UID of an already-committed object and
    lets the row-locked publication CAS adopt it while T/G remains open.
    Retirement owns the complementary revoke/fence path.
    """

    interval_s = max(
        5,
        int(os.getenv("PINNED_AGENT_CREATE_RECONCILE_INTERVAL_SECONDS", "15")),
    )
    logger.info("Pinned agent create-intent reconciler started")
    while not shutdown_event.is_set():
        try:
            legacy = await reconcile_legacy_pinned_agent_authority(
                postgres_db,
                agent_provisioner=agent_provisioner,
                persistent_provisioner=persistent_provisioner,
                limit=50,
            )
            if legacy.unresolved:
                logger.warning(
                    "Pinned legacy Kubernetes authority remains unresolved "
                    "for %d row(s)",
                    legacy.unresolved,
                )
            rows = await postgres_db.list_pinned_agent_create_intents_for_reconcile(
                limit=50
            )
            for row in rows:
                try:
                    provisioner = str(row.get("provisioner") or "")
                    provider = (
                        persistent_provisioner
                        if provisioner == "persistent"
                        else agent_provisioner
                        if provisioner == "agent"
                        else None
                    )
                    if provider is None or not provider.is_available:
                        continue
                    thread_id = str(row.get("thread_id") or "")
                    generation = str(row.get("runtime_generation") or "")
                    attempt_id = str(row.get("attempt_id") or "")
                    pod_name = str(row.get("pod_name") or "")
                    namespace = str(row.get("namespace") or "")
                    if (
                        not all(
                            (thread_id, generation, attempt_id, pod_name, namespace)
                        )
                        or str(row.get("protection_protocol") or "") != "finalizer_v1"
                    ):
                        continue

                    claim = row.get("workspace_claim")
                    if claim is not None:
                        if not isinstance(claim, Mapping):
                            continue
                        claim_id = str(claim.get("claim_id") or "")
                        claim_generation = str(
                            claim.get("created_runtime_generation") or ""
                        )
                        claim_attempt = str(claim.get("create_attempt") or "")
                        claim_name = str(claim.get("pvc_name") or "")
                        claim_status = str(claim.get("status") or "")
                        claim_uid = str(claim.get("pvc_uid") or "")
                        claim_namespace = str(claim.get("namespace") or "")
                        if (
                            not all(
                                (
                                    claim_id,
                                    claim_generation,
                                    claim_attempt,
                                    claim_name,
                                    claim_namespace,
                                )
                            )
                            or claim_status not in {"planned", "ready"}
                            or not (
                                claim_namespace == namespace
                                and str(claim.get("protection_protocol") or "")
                                == "finalizer_v1"
                            )
                        ):
                            continue
                        observed_claim = await provider.agent_workspace_claim_authority(
                            claim_name,
                            expected_thread_id=thread_id,
                            expected_runtime_generation=claim_generation,
                            expected_claim_id=claim_id,
                            expected_create_attempt=claim_attempt,
                            namespace=claim_namespace,
                            expected_pvc_uid=claim_uid or None,
                        )
                        observed_claim_uid = str(
                            (observed_claim or {}).get("pvc_uid") or ""
                        )
                        if not (
                            str((observed_claim or {}).get("state") or "")
                            == "exact_present"
                            and observed_claim_uid
                            and (not claim_uid or observed_claim_uid == claim_uid)
                        ):
                            continue
                        if (
                            claim_status == "planned"
                            and not await postgres_db.publish_pinned_agent_workspace_claim(
                                thread_id,
                                expected_runtime_generation=generation,
                                claim_id=claim_id,
                                pvc_name=claim_name,
                                pvc_uid=observed_claim_uid,
                                namespace=claim_namespace,
                            )
                        ):
                            continue

                    observed_pod = await provider.agent_pod_provision_intent_authority(
                        pod_name,
                        expected_thread_id=thread_id,
                        expected_runtime_generation=generation,
                        expected_attempt_id=attempt_id,
                        namespace=namespace,
                    )
                    pod_uid = str((observed_pod or {}).get("pod_uid") or "")
                    if not (
                        str((observed_pod or {}).get("state") or "") == "exact_present"
                        and pod_uid
                    ):
                        continue
                    await postgres_db.publish_pinned_agent_pod_provision_intent(
                        thread_id,
                        expected_runtime_generation=generation,
                        attempt_id=attempt_id,
                        pod_name=pod_name,
                        pod_uid=pod_uid,
                        namespace=namespace,
                    )
                except Exception:
                    logger.exception(
                        "Pinned agent create-intent reconciliation failed for %s",
                        row.get("attempt_id"),
                    )
        except Exception:
            logger.exception("Pinned agent create-intent reconciliation pass failed")
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=interval_s)
        except TimeoutError:
            pass
    logger.info("Pinned agent create-intent reconciler stopped")


async def _begin_pinned_thread_retirement(
    thread_id: str, **kwargs: Any
) -> dict[str, Any]:
    return await _pinned_retirement_operations().begin_pinned_thread_retirement(
        thread_id, **kwargs
    )


async def pinned_k8s_create_fence_gc_sweeper(
    shutdown_event: asyncio.Event,
) -> None:
    """Exact-delete post-horizon Pod/PVC name fences and retire their rows.

    Fence rows deliberately survive thread deletion and process restart. A
    due timestamp is necessary but not sufficient: the sweeper reattests the
    immutable labels and recorded UID, issues a UID-preconditioned delete, and
    marks the work item terminal only after the Kubernetes name is absent.
    """

    interval_s = max(
        5,
        int(os.getenv("PINNED_K8S_CREATE_FENCE_GC_INTERVAL_SECONDS", "30")),
    )
    logger.info("Pinned Kubernetes create-fence GC started (interval=%ds)", interval_s)
    while not shutdown_event.is_set():
        try:
            rows = await postgres_db.list_due_pinned_k8s_create_fences(limit=50)
            for row in rows:
                provisioner = str(row.get("provisioner") or "")
                provider = (
                    persistent_provisioner
                    if provisioner == "persistent"
                    else agent_provisioner
                    if provisioner == "agent"
                    else None
                )
                if provider is None or not provider.is_available:
                    continue
                resource_kind = str(row.get("resource_kind") or "")
                resource_name = str(row.get("resource_name") or "")
                resource_uid = str(row.get("resource_uid") or "")
                thread_id = str(row.get("thread_id") or "")
                generation = str(row.get("runtime_generation") or "")
                create_attempt = str(row.get("create_attempt") or "")
                authority_id = str(row.get("authority_id") or "")
                namespace = str(row.get("namespace") or "")
                if (
                    not all(
                        (
                            resource_name,
                            resource_uid,
                            thread_id,
                            generation,
                            create_attempt,
                            authority_id,
                            namespace,
                        )
                    )
                    or str(row.get("protection_protocol") or "") != "finalizer_v1"
                ):
                    continue
                if resource_kind == "pod":
                    observed = await provider.agent_pod_provision_intent_authority(
                        resource_name,
                        expected_thread_id=thread_id,
                        expected_runtime_generation=generation,
                        expected_attempt_id=create_attempt,
                        namespace=namespace,
                    )
                    state = str((observed or {}).get("state") or "")
                    observed_uid = str((observed or {}).get("pod_uid") or "")
                    if state == "exact_fence" and observed_uid == resource_uid:
                        deleted = (
                            await persistent_provisioner.delete_agent_pod_exact(
                                thread_id,
                                expected_pod_uid=resource_uid,
                                namespace=namespace,
                            )
                            if provisioner == "persistent"
                            else await agent_provisioner.delete_agent_pod_exact(
                                resource_name,
                                expected_pod_uid=resource_uid,
                                namespace=namespace,
                            )
                        )
                        if not deleted:
                            continue
                        released = (
                            await persistent_provisioner.release_agent_pod_finalizer_exact(
                                thread_id,
                                expected_pod_uid=resource_uid,
                                namespace=namespace,
                                terminal_required=False,
                            )
                            if provisioner == "persistent"
                            else await agent_provisioner.release_agent_pod_finalizer_exact(
                                resource_name,
                                expected_pod_uid=resource_uid,
                                namespace=namespace,
                                terminal_required=False,
                            )
                        )
                        if not released:
                            continue
                        observed = await provider.agent_pod_provision_intent_authority(
                            resource_name,
                            expected_thread_id=thread_id,
                            expected_runtime_generation=generation,
                            expected_attempt_id=create_attempt,
                            namespace=namespace,
                        )
                        state = str((observed or {}).get("state") or "")
                    if state != "exact_absent":
                        continue
                elif resource_kind == "pvc":
                    observed = await provider.agent_workspace_claim_authority(
                        resource_name,
                        expected_thread_id=thread_id,
                        expected_runtime_generation=generation,
                        expected_claim_id=authority_id,
                        expected_create_attempt=create_attempt,
                        namespace=namespace,
                        expected_pvc_uid=resource_uid,
                    )
                    state = str((observed or {}).get("state") or "")
                    observed_uid = str((observed or {}).get("pvc_uid") or "")
                    if state == "exact_fence" and observed_uid == resource_uid:
                        if not await provider.delete_agent_workspace_claim_exact(
                            resource_name,
                            expected_pvc_uid=resource_uid,
                            namespace=namespace,
                        ):
                            continue
                        if not await provider.release_agent_workspace_claim_finalizer_exact(
                            resource_name,
                            expected_pvc_uid=resource_uid,
                            namespace=namespace,
                        ):
                            continue
                        observed = await provider.agent_workspace_claim_authority(
                            resource_name,
                            expected_thread_id=thread_id,
                            expected_runtime_generation=generation,
                            expected_claim_id=authority_id,
                            expected_create_attempt=create_attempt,
                            namespace=namespace,
                            expected_pvc_uid=resource_uid,
                        )
                        state = str((observed or {}).get("state") or "")
                    if state != "exact_absent":
                        continue
                else:
                    continue
                await postgres_db.complete_pinned_k8s_create_fence_gc(
                    resource_kind=resource_kind,
                    authority_id=authority_id,
                    expected_resource_uid=resource_uid,
                )
            if container_provisioner.is_available:
                workspace_rows = await postgres_db.list_pinned_thread_workspace_provision_fences_for_gc(
                    limit=50
                )
                for workspace_row in workspace_rows:
                    if not await container_provisioner.delete_pinned_workspace_provision_fences_exact(
                        workspace_row
                    ):
                        continue
                    await postgres_db.retire_pinned_thread_workspace_provision_fence(
                        str(workspace_row.get("attempt_id") or ""),
                        expected_fence_pod_uid=str(
                            workspace_row.get("fence_pod_uid") or ""
                        ),
                        expected_fence_pvc_uid=(
                            str(workspace_row.get("fence_pvc_uid") or "") or None
                        ),
                        expected_fence_configmap_uid=(
                            str(workspace_row.get("fence_configmap_uid") or "") or None
                        ),
                        expected_fence_service_uid=(
                            str(workspace_row.get("fence_service_uid") or "") or None
                        ),
                    )
        except Exception:
            logger.exception("Pinned Kubernetes create-fence GC pass failed")
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=interval_s)
        except TimeoutError:
            pass
    logger.info("Pinned Kubernetes create-fence GC stopped")


async def imap_poll_loop(shutdown_event: asyncio.Event) -> None:
    """Background task that polls IMAP for inbound email replies.

    Runs every IMAP_POLL_INTERVAL seconds (default: 30).
    Gracefully disabled when IMAP is not configured.
    """
    if not imap_poller.is_available:
        logger.info("IMAP poller not started (not configured)")
        # Park until shutdown instead of returning: run_when_leader re-invokes a
        # loop coroutine that returns (recreating the task every poll_seconds),
        # so a bare return here re-logs this line ~once per second on the leader.
        await shutdown_event.wait()
        return

    logger.info("IMAP poller started (interval=%ds)", imap_poller.poll_interval)
    while not shutdown_event.is_set():
        try:
            count = await imap_poller.poll_once()
            if count > 0:
                logger.info("IMAP poller: processed %d email reply(ies)", count)
        except Exception as e:
            logger.error("IMAP poller error: %s", e)

        try:
            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=imap_poller.poll_interval,
            )
            break
        except asyncio.TimeoutError:
            pass

    logger.info("IMAP poller stopped")


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


from orchestrator.services.session_tool_policy import (  # noqa: E402
    merged_session_tool_policy as _merged_session_tool_policy,
)


from orchestrator.services.session_tool_policy import (  # noqa: E402
    legacy_session_tool_policy as _legacy_session_tool_policy,
)


from orchestrator.services.agent_toolset_probe import unmeasured as _unmeasured  # noqa: E402


from orchestrator.services.agent_toolset_probe import origin_fields as _origin_fields  # noqa: E402


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


from orchestrator.services.job_workspace_runtime import (  # noqa: E402
    scholar_provision_parent_id as _scholar_provision_parent_id,
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


def _retirement_context_runtime_exposed(
    retirement: Mapping[str, Any],
) -> bool:
    return _pinned_retirement_operations().retirement_context_runtime_exposed(
        retirement
    )


def _retirement_has_exact_local_quiescence(
    retirement: Mapping[str, Any], thread: Mapping[str, Any]
) -> bool:
    return _pinned_retirement_operations().retirement_has_exact_local_quiescence(
        retirement, thread
    )


async def _recover_captured_sandbox_process_zero(
    retirement: Mapping[str, Any],
) -> bool:
    return await _pinned_retirement_operations().recover_captured_process_zero(
        retirement
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


async def _try_dispatch_pending_jobs() -> None:
    """Core dispatcher: match pending jobs to available agents.

    Phase 1: Direct assignment (free agents → highest priority pending jobs)
    Phase 2: Preemption (remaining high-priority jobs → lowest-priority running jobs)

    VM-aware: jobs needing a VM are auto-provisioned and held until the VM
    registers as ready. Jobs with a ready VM get workspace config injected
    into config_override before dispatch.
    """
    if (
        getattr(postgres_db, "manifests_ready", False) is True
        and agent_provisioner._k8s_available
    ):
        await _manifest_execution_service().reconcile()
    if not AUTO_ASSIGN_ENABLED and not STATELESS_WORKER_ENABLED:
        return

    async with _dispatch_lock:
        try:
            # Both lanes retain the same leader-owned workspace preflight. The
            # pinned set proceeds to registered-agent matching; a ready
            # stateless set is admitted to run_queue and never reaches that
            # half of this function.
            pending_jobs = (
                await postgres_db.get_dispatchable_jobs(
                    limit=50,
                    **_completion_control_boundary.dispatch_guard_kwargs(),
                )
                if AUTO_ASSIGN_ENABLED
                else []
            )
            if STATELESS_WORKER_ENABLED:
                pending_jobs.extend(
                    await postgres_db.get_admittable_stateless_jobs(
                        limit=50,
                        **_completion_control_boundary.dispatch_guard_kwargs(),
                    )
                )
            if not pending_jobs:
                return

            # Pre-filter: auto-provision VMs/containers for jobs that need one
            dispatchable_jobs = []
            for job in pending_jobs:
                job_id = str(job["id"])
                stateless_worker = job.get("execution_lane") == "stateless"
                (
                    workspace_action,
                    job,
                    workspace_recovery_reason,
                ) = await _prepare_job_workspace_runtime(job)
                if workspace_action == "wait":
                    logger.warning(
                        "Dispatcher: job %s waiting for live workspace authority "
                        "recovery (%s)",
                        job_id,
                        workspace_recovery_reason or "retryable",
                    )
                    continue
                if workspace_action == "fail":
                    await _fail_subjob_and_unblock_parent(
                        job,
                        workspace_recovery_reason
                        or "Inherited workspace authority is unavailable.",
                    )
                    continue
                workspace_decision = resolve_workspace_runtime(
                    job, vm_mode=vm_provisioner.mode
                )
                if (
                    workspace_decision.contract is None
                    or workspace_decision.state == "invalid"
                ):
                    logger.error(
                        "Dispatcher: refusing job %s with invalid workspace "
                        "authority (%s)",
                        job_id,
                        workspace_decision.reason or workspace_decision.state,
                    )
                    await postgres_db.update_job_status(
                        job_id,
                        status="failed",
                        error_message=(
                            "Workspace contract is ambiguous or invalid; "
                            "refusing dispatch"
                        ),
                        expected_status=str(job.get("status")),
                    )
                    continue

                job_needs_vm = _job_needs_vm(job)
                stateless_same_cluster_vm = bool(
                    stateless_worker and job_needs_vm and vm_workspaces_on_pod_network()
                )

                # Defense for inherited/operator-created rows. External VMs
                # still belong to the mesh-enabled registered-agent plane; a
                # same-cluster VM remains on the pool lane below.
                if (
                    stateless_worker
                    and job_needs_vm
                    and not vm_workspaces_on_pod_network()
                ):
                    moved_to_pinned = False
                    async with postgres_db.acquire() as conn:
                        async with conn.transaction():
                            await conn.fetchrow(
                                "SELECT state FROM run_queue "
                                "WHERE unit_id = $1::uuid "
                                "AND unit_kind = 'worker_batch' FOR UPDATE",
                                job_id,
                            )
                            moved = await conn.fetchrow(
                                "UPDATE jobs SET execution_lane = 'pinned', "
                                "updated_at = CURRENT_TIMESTAMP "
                                "WHERE id = $1::uuid "
                                "AND execution_lane = 'stateless' "
                                "AND status::text = $2::text "
                                "RETURNING id",
                                job_id,
                                str(job.get("status") or ""),
                            )
                            if moved is not None:
                                moved_to_pinned = True
                                await conn.execute(
                                    "UPDATE run_queue SET state = 'done', "
                                    "lease_token = lease_token + 1, "
                                    "leased_by = NULL, last_leased_by = NULL, "
                                    "leased_until = NULL, run_after = now(), "
                                    "queued_at = now() "
                                    "WHERE unit_id = $1::uuid "
                                    "AND unit_kind = 'worker_batch'",
                                    job_id,
                                )
                    if moved_to_pinned:
                        logger.info(
                            "Dispatcher: moved VM job %s from stateless to pinned lane",
                            job_id,
                        )
                    else:
                        logger.debug(
                            "Dispatcher: VM lane repair lost the status CAS for job %s",
                            job_id,
                        )
                    continue
                _stateless_container = _get_container_context(job)
                _stateless_has_k8s_workspace = (
                    _stateless_container.get("status") == "ready"
                    and _stateless_container.get("provisioner") == "k8s"
                    and bool(
                        _stateless_container.get("host")
                        or _stateless_container.get("pod_ip")
                    )
                )
                if stateless_worker and not (
                    stateless_same_cluster_vm
                    or _job_needs_sandbox(job)
                    or _stateless_has_k8s_workspace
                ):
                    logger.error(
                        "Dispatcher: refusing stateless job %s without a compatible "
                        "workspace",
                        job_id,
                    )
                    await postgres_db.update_job_status(
                        job_id,
                        status="failed",
                        error_message=(
                            "Stateless workers currently require a Kubernetes "
                            "sandbox or same-cluster VM workspace"
                        ),
                        expected_status=str(job.get("status")),
                    )
                    continue
                if (
                    stateless_worker
                    and not stateless_same_cluster_vm
                    and not (
                        container_provisioner.is_available
                        and container_provisioner.in_cluster
                    )
                ):
                    logger.warning(
                        "Dispatcher: stateless job %s waiting for the in-cluster "
                        "Kubernetes workspace provisioner",
                        job_id,
                    )
                    continue

                if job_needs_vm:
                    # Admin-gated permission check (kill-switch + per-user grant).
                    # Re-verified here in case a grant was revoked or the
                    # kill-switch flipped after the job was submitted. Already
                    # running VMs aren't torn down by this check — the gate
                    # only blocks jobs that haven't been dispatched yet.
                    creator = None
                    creator_id = job.get("user_id")
                    if creator_id:
                        try:
                            creator = await postgres_db.get_user(str(creator_id))
                        except Exception:
                            creator = None
                    try:
                        await _check_vm_permission(creator, job_needs_vm=True)
                    except HTTPException as permission_error:
                        logger.error(
                            "Dispatcher: job %s denied VM workspace: %s",
                            job_id,
                            permission_error.detail,
                        )
                        await postgres_db.update_job_status(
                            job_id,
                            status="failed",
                            error_message=str(permission_error.detail),
                        )
                        continue
                    vm_ctx = _get_vm_context(job)
                    vm_status = vm_ctx.get("status")
                    # Bounded provisioning retries. A VM that never reaches 'ready'
                    # (real infra failure) must park after N attempts instead of
                    # re-provisioning forever against the shared VM cluster. The
                    # counter is monotonic in context.vm and reset to 0 once the VM
                    # boots (VM_READY below), so it survives the async controller
                    # status callbacks that a status-based park cannot. Decision
                    # logic is extracted + unit-tested in dispatch_guards.
                    provision_attempts = int(vm_ctx.get("provision_attempts") or 0)
                    max_provision_attempts = int(
                        os.environ.get("VM_PROVISION_MAX_ATTEMPTS", "3")
                    )
                    timeout_s = int(os.environ.get("VM_PROVISION_TIMEOUT_S", "600"))
                    golden_timeout_s = int(
                        os.environ.get("VM_GOLDEN_WAIT_TIMEOUT_S", "2700")
                    )
                    capacity_timeout_s = int(
                        os.environ.get("VM_CAPACITY_WAIT_TIMEOUT_S", "2700")
                    )
                    headscale_timeout_s = int(
                        os.environ.get("VM_HEADSCALE_WAIT_TIMEOUT_S", "900")
                    )
                    vm_decision = vm_provisioning_decision(
                        vm_ctx,
                        provision_attempts=provision_attempts,
                        max_provision_attempts=max_provision_attempts,
                        now=time.time(),
                        timeout_s=timeout_s,
                        golden_timeout_s=golden_timeout_s,
                        capacity_timeout_s=capacity_timeout_s,
                        headscale_timeout_s=headscale_timeout_s,
                    )
                    if vm_decision == VM_PARK_EXHAUSTED:
                        # Retries used up — park the VM context AND fail the job.
                        # 'failed' is terminal for the dispatcher (VM_PARKED) and
                        # skipped by the reconciler (_PARKED_VM_STATUSES), so the
                        # park holds. The job itself must go terminal too: leaving
                        # it 'created' with nothing scheduled to change its state
                        # is an invisible wedge (a loop's current_job never turns
                        # terminal → the loop stalls forever). See knowledge-base/knowledge/issues/
                        # vm_ssh_readiness_probe_unroutable_from_orchestrator.md.
                        park_error = (
                            f"provisioning exhausted after "
                            f"{provision_attempts} attempts "
                            f"(never reached 'ready')"
                        )
                        logger.warning(
                            "Dispatcher: job %s VM provisioning exhausted "
                            "(%d/%d attempts) — failing job",
                            job_id,
                            provision_attempts,
                            max_provision_attempts,
                        )
                        await postgres_db.merge_vm_context(
                            job_id,
                            {"status": "failed", "error": park_error},
                        )
                        await _fail_vm_parked_job(job_id, park_error)
                        continue
                    if vm_decision == VM_PROVISION:
                        # VM needed but absent — never provisioned, torn down while
                        # parked ('deleted': deploy-drain / crash recovery release
                        # it; work survives in the pushed branch + checkpoint), or
                        # recycled by the timeout. Without re-provisioning here a
                        # paused VM job waits forever on a VM nothing will create.
                        if not vm_provisioner.is_available:
                            # VM explicitly requested but no provisioner — fail
                            logger.error(
                                "Dispatcher: job %s requires VM workspace but VM "
                                "provisioner is unavailable for VM_MODE=%s. "
                                "Failing job.",
                                job_id,
                                vm_provisioner.mode,
                            )
                            await postgres_db.update_job_status(
                                job_id,
                                status="failed",
                                error_message=(
                                    "VM workspace requested but VM provisioner is not "
                                    f"available for VM_MODE={vm_provisioner.mode!r}. "
                                    "Configure VM_MODE and its required controller, use "
                                    "workspace.backend='container', or "
                                    "remove the explicit backend override."
                                ),
                            )
                            continue
                        config_override = job.get("config_override") or {}
                        if isinstance(config_override, str):
                            config_override = json.loads(config_override)
                        vm_options = await vm_provisioning_options(
                            postgres_db, "Job", job, fallback=config_override
                        )
                        ok = await vm_provisioner.create_vm(
                            job_id=job_id,
                            agent_config=canonical_config_name(
                                job.get("config_name", "worker_base")
                            ),
                            **vm_options,
                            description=job.get("description", ""),
                        )
                        if ok:
                            # Count the attempt so a VM that never boots parks
                            # after max_provision_attempts. create_vm stamped a
                            # fresh provisioned_at; the timeout recycles this
                            # attempt if it stalls.
                            await postgres_db.merge_vm_context(
                                job_id,
                                {"provision_attempts": provision_attempts + 1},
                            )
                            logger.info(
                                "Dispatcher: auto-provisioned VM for job %s "
                                "(attempt %d/%d)",
                                job_id,
                                provision_attempts + 1,
                                max_provision_attempts,
                            )
                        else:
                            logger.warning(
                                "Dispatcher: VM provisioning failed for job %s",
                                job_id,
                            )
                        continue  # Skip this job — wait for VM to register
                    if vm_decision == VM_PARKED:
                        # Provisioning failed terminally — do NOT hot-retry every
                        # tick (shared VM cluster). The job is still non-terminal
                        # (it's in the dispatchable list), which means something
                        # left it parked-but-alive: an older build's park, or a
                        # controller-callback race with PARK_EXHAUSTED. Heal it
                        # to 'failed' so it stops wedging its loop.
                        vm_error = vm_ctx.get("error") or "VM provisioning failed"
                        logger.warning(
                            "Dispatcher: job %s VM parked (%s) — failing job",
                            job_id,
                            vm_error,
                        )
                        await _fail_vm_parked_job(job_id, vm_error)
                        continue
                    if vm_decision == VM_PARK_PREPARATION:
                        park_error = (
                            "Workspace preparation did not complete within its deadline"
                        )
                        await postgres_db.merge_vm_context(
                            job_id, {"status": "failed", "error": park_error}
                        )
                        await _fail_vm_parked_job(job_id, park_error)
                        continue
                    if vm_decision in (
                        VM_GOLDEN_POLL,
                        VM_CAPACITY_POLL,
                        VM_PREPARATION_POLL,
                    ):
                        # No VM exists yet — the controller is waiting on a
                        # shared golden-image import (cold import after an
                        # agent-vm-base bump: ~30 min, longer than timeout_s).
                        # Re-issue create as the poll: the controller answers
                        # waiting_golden (cheap DV GET) until the golden is
                        # Succeeded, then actually builds the VM. fresh=False
                        # keeps the golden budget anchor + counters and does
                        # NOT consume a provision attempt — the attempt budget
                        # bounds VM boots, and no boot is happening. See
                        # knowledge-history/done/
                        # golden_image_cold_import_fails_inflight_vm_jobs.md.
                        wait_anchor = (
                            "preparation_wait_started_at"
                            if vm_decision == VM_PREPARATION_POLL
                            else "capacity_wait_started_at"
                            if vm_decision == VM_CAPACITY_POLL
                            else "golden_wait_started_at"
                        )
                        if not vm_ctx.get(wait_anchor):
                            await postgres_db.merge_vm_context(
                                job_id,
                                {wait_anchor: time.time()},
                            )
                        config_override = job.get("config_override") or {}
                        if isinstance(config_override, str):
                            config_override = json.loads(config_override)
                        vm_options = await vm_provisioning_options(
                            postgres_db, "Job", job, fallback=config_override
                        )
                        await vm_provisioner.create_vm(
                            job_id=job_id,
                            agent_config=canonical_config_name(
                                job.get("config_name", "worker_base")
                            ),
                            **vm_options,
                            description=job.get("description", ""),
                            fresh=False,
                        )
                        if vm_decision == VM_PREPARATION_POLL:
                            logger.info(
                                "Dispatcher: job %s waiting on workspace preparation",
                                job_id,
                            )
                        elif vm_decision == VM_CAPACITY_POLL:
                            logger.info(
                                "Dispatcher: job %s waiting on VM capacity (%s/%s) "
                                "— polling",
                                job_id,
                                vm_ctx.get("running_vms") or "?",
                                vm_ctx.get("max_concurrent_vms") or "?",
                            )
                        else:
                            logger.info(
                                "Dispatcher: job %s waiting on golden image %s "
                                "(%s) — polling",
                                job_id,
                                vm_ctx.get("golden") or "?",
                                vm_ctx.get("golden_progress")
                                or vm_ctx.get("golden_phase")
                                or "importing",
                            )
                        continue
                    if vm_decision == VM_PARK_CAPACITY:
                        elapsed = int(
                            time.time()
                            - float(vm_ctx.get("capacity_wait_started_at") or 0)
                        )
                        park_error = (
                            "VM capacity did not become available within "
                            f"{capacity_timeout_s}s (running "
                            f"{vm_ctx.get('running_vms') or 'unknown'}/"
                            f"{vm_ctx.get('max_concurrent_vms') or 'unknown'}, "
                            f"waited {elapsed}s) — VM never created"
                        )
                        await postgres_db.merge_vm_context(
                            job_id, {"status": "failed", "error": park_error}
                        )
                        await _fail_vm_parked_job(job_id, park_error)
                        continue
                    if vm_decision == VM_PARK_GOLDEN:
                        # The golden import outlived even the golden budget —
                        # CDI is wedged or the registry is unreachable. No VM
                        # was ever created, so there is nothing to recycle;
                        # fail the job with the truth (not the misleading
                        # "provisioning exhausted after N attempts").
                        elapsed = int(
                            time.time()
                            - float(vm_ctx.get("golden_wait_started_at") or 0)
                        )
                        park_error = (
                            f"golden image import did not complete within "
                            f"{golden_timeout_s}s (golden "
                            f"{vm_ctx.get('golden') or 'unknown'}, last progress "
                            f"{vm_ctx.get('golden_progress') or 'unknown'}, "
                            f"waited {elapsed}s) — VM never created"
                        )
                        logger.warning(
                            "Dispatcher: job %s golden wait exhausted — "
                            "failing job (%s)",
                            job_id,
                            park_error,
                        )
                        await postgres_db.merge_vm_context(
                            job_id,
                            {"status": "failed", "error": park_error},
                        )
                        await _fail_vm_parked_job(job_id, park_error)
                        continue
                    if vm_decision == VM_HEADSCALE_POLL:
                        # No VM exists yet — the controller refused to build one
                        # while Headscale is unreachable, because a VM with no
                        # tailnet pre-auth key boots and heartbeats but is never
                        # reachable over SSH. Poll create (fresh=False, no
                        # attempt consumed) until the mesh recovers; the
                        # controller then builds the VM on the very next poll.
                        if not vm_ctx.get("headscale_wait_started_at"):
                            await postgres_db.merge_vm_context(
                                job_id,
                                {"headscale_wait_started_at": time.time()},
                            )
                        config_override = job.get("config_override") or {}
                        if isinstance(config_override, str):
                            config_override = json.loads(config_override)
                        vm_options = await vm_provisioning_options(
                            postgres_db, "Job", job, fallback=config_override
                        )
                        await vm_provisioner.create_vm(
                            job_id=job_id,
                            agent_config=canonical_config_name(
                                job.get("config_name", "worker_base")
                            ),
                            **vm_options,
                            description=job.get("description", ""),
                            fresh=False,
                        )
                        logger.info(
                            "Dispatcher: job %s waiting on Headscale (%s) — polling",
                            job_id,
                            vm_ctx.get("headscale_error") or "mesh VPN unavailable",
                        )
                        continue
                    if vm_decision == VM_PARK_HEADSCALE:
                        # Headscale never came back inside its budget. No VM was
                        # ever created, so there is nothing to recycle — fail
                        # with the real cause rather than the misleading
                        # "provisioning exhausted after N attempts".
                        elapsed = int(
                            time.time()
                            - float(vm_ctx.get("headscale_wait_started_at") or 0)
                        )
                        park_error = (
                            f"Headscale (mesh VPN) unavailable for {elapsed}s "
                            f"(budget {headscale_timeout_s}s, last error: "
                            f"{vm_ctx.get('headscale_error') or 'unknown'}) — "
                            f"VM never created"
                        )
                        logger.warning(
                            "Dispatcher: job %s Headscale wait exhausted — "
                            "failing job (%s)",
                            job_id,
                            park_error,
                        )
                        await postgres_db.merge_vm_context(
                            job_id,
                            {"status": "failed", "error": park_error},
                        )
                        await _fail_vm_parked_job(job_id, park_error)
                        continue
                    if vm_decision == VM_PARK_INITIALIZATION:
                        park_error = "Workspace initialization did not complete within its deadline"
                        await postgres_db.merge_vm_context(
                            job_id, {"status": "failed", "error": park_error}
                        )
                        await _fail_vm_parked_job(job_id, park_error)
                        continue
                    if vm_decision == VM_RECYCLE:
                        # Stuck short of 'ready' past the budget — tear it down so
                        # the next tick re-provisions (VM_PROVISION) or parks
                        # (VM_PARK_EXHAUSTED). With the reconciler now handing off
                        # provisioning VMs (is_reapable=False for dispatchable jobs)
                        # nothing else would time it out.
                        logger.warning(
                            "Dispatcher: job %s VM stuck in '%s' — checking "
                            "cleanup for provision attempt %d/%d",
                            job_id,
                            vm_status,
                            provision_attempts,
                            max_provision_attempts,
                        )
                        await recycle_provisioning_vm(
                            job_id,
                            vm_ctx,
                            db=postgres_db,
                            provisioner=vm_provisioner,
                            recovery_store=VMWorkspaceRecoveryStore(postgres_db),
                            now=time.time(),
                        )
                        continue
                    if vm_decision == VM_WAIT:
                        # Provisioning / creating / deleting in flight — wait.
                        continue
                    # VM_READY: proceed with dispatch.
                    if provision_attempts:
                        # VM booted — clear the retry budget so a later
                        # re-provision (crash recovery) starts fresh.
                        await postgres_db.merge_vm_context(
                            job_id, {"provision_attempts": 0}
                        )
                    logger.info("Dispatcher: job %s using VM workspace", job_id)
                elif _job_needs_sandbox(job):
                    # Phase 1: a pre-agent scholar spawned before its parent had a
                    # workspace provisions the parent's ONE shared pod under the
                    # parent's identity and rides it, instead of self-provisioning
                    # a throwaway pod. k8s only — VM/docker parents fall through to
                    # the normal self-provision path below.
                    provision_parent_id = _scholar_provision_parent_id(job)
                    if (
                        provision_parent_id
                        and container_provisioner.is_available
                        and container_provisioner.in_cluster
                    ):
                        await _provision_parent_workspace_for_scholar(
                            job, provision_parent_id
                        )
                        # wait → retry next tick; promoted → dispatches next tick
                        # via the inherit path; fail → already failed + unblocked.
                        continue
                    container_ctx = _get_container_context(job)
                    container_status = container_ctx.get("status")
                    # K8s in-cluster takes priority; a local kubeconfig must not
                    # shadow Docker Compose when running outside the cluster.
                    use_k8s = container_provisioner.is_available and (
                        container_provisioner.in_cluster
                        or not docker_provisioner.is_available
                    )
                    # States that mean "no live workspace yet" → (re)create.
                    needs_create = container_status in (None, "", "deleted", "none")
                    if needs_create and not use_k8s:
                        # Docker Compose pool / no-provisioner CREATE path (unchanged).
                        if docker_provisioner.is_available:
                            logger.info(
                                "Dispatcher: job %s assigning workspace from "
                                "Docker Compose pool",
                                job_id,
                            )
                            result = await docker_provisioner.assign_workspace(job_id)
                            if not result:
                                logger.warning(
                                    "Dispatcher: no free workspace for job %s "
                                    "— all containers occupied, will retry",
                                    job_id,
                                )
                        else:
                            logger.error(
                                "Dispatcher: job %s needs workspace but no "
                                "provisioner available. Failing job.",
                                job_id,
                            )
                            await postgres_db.update_job_status(
                                job_id,
                                status="failed",
                                error_message=(
                                    "No workspace provisioner available. "
                                    "Neither Kubernetes API nor WORKSPACE_HOSTS "
                                    "configured."
                                ),
                            )
                        continue  # Skip — wait for container to become ready
                    # K8s create (when status absent) + all lifecycle states route
                    # through the shared, owner-agnostic state machine.
                    config_override = job.get("config_override") or {}
                    if isinstance(config_override, str):
                        config_override = json.loads(config_override)
                    ws_cfg = config_override.get("workspace", {}).get("container", {})
                    res = await ensure_workspace(
                        WorkspaceOwner.job(job_id),
                        provisioner=container_provisioner,
                        suspension=workspace_suspension_service,
                        current_status=container_status,
                        ws_config={
                            k: ws_cfg[k]
                            for k in (
                                "cpu",
                                "memory",
                                "cpu_limit",
                                "memory_limit",
                                "image",
                            )
                            if k in ws_cfg
                        },
                    )
                    if res.outcome is EnsureOutcome.FAILED:
                        failed_ctx = container_ctx
                        if container_status != "failed":
                            # create_workspace records the concrete failure in
                            # context before returning False. Refresh once so a
                            # first-attempt auth/RBAC/image failure reaches the
                            # job error instead of being replaced by the generic
                            # "could not be created" wrapper.
                            try:
                                refreshed_job = await postgres_db.get_job(job_id)
                                if refreshed_job:
                                    failed_ctx = _get_container_context(refreshed_job)
                            except Exception:
                                logger.warning(
                                    "Dispatcher: could not refresh failed workspace "
                                    "context for job %s",
                                    job_id,
                                    exc_info=True,
                                )
                        error = failed_ctx.get("error")
                        if error:
                            msg = f"Workspace container failed: {error}"
                        else:
                            msg = (
                                "Workspace container could not be created. Check "
                                "orchestrator logs for details (image pull failures, "
                                "insufficient resources, RBAC issues)."
                            )
                        logger.error(
                            "Dispatcher: workspace ensure failed for job %s: %s. "
                            "Failing job.",
                            job_id,
                            msg,
                        )
                        await postgres_db.update_job_status(
                            job_id,
                            status="failed",
                            error_message=msg,
                            expected_status=(
                                str(job.get("status")) if stateless_worker else None
                            ),
                        )
                        continue
                    if res.outcome is EnsureOutcome.PENDING:
                        if container_status not in (
                            None,
                            "",
                            "deleted",
                            "none",
                            "created",
                            "creating",
                            "restoring",
                            "suspending",
                            "pending",
                        ):
                            logger.warning(
                                "Dispatcher: job %s has unexpected workspace "
                                "container status %r — waiting",
                                job_id,
                                container_status,
                            )
                        continue  # in progress — wait for next cycle
                    # READY → proceed with dispatch
                    logger.info("Dispatcher: job %s using workspace container", job_id)
                else:
                    # No VM or container provisioning needed — check if a workspace
                    # was already assigned (e.g. Docker provisioner assigned it on a
                    # previous cycle and the job is now ready for dispatch).
                    existing_ctx = _get_container_context(job)
                    if existing_ctx.get("status") == "ready":
                        logger.info(
                            "Dispatcher: job %s using pre-assigned workspace",
                            job_id,
                        )
                    else:
                        logger.debug(
                            "Dispatcher: job %s — no workspace provisioner needed",
                            job_id,
                        )
                if not await _prepare_job_repository_before_claim(job):
                    # Retry on the next dispatcher tick. A Gitea/SSH outage is
                    # not a worker failure and must not make the job cross the
                    # processing boundary with unproven repository authority.
                    continue
                if stateless_worker:
                    (
                        admitted,
                        queue_result,
                    ) = await postgres_db.admit_stateless_worker_job(
                        job_id,
                        fair_key=(str(job["user_id"]) if job.get("user_id") else None),
                        priority=int(job.get("priority") or 0),
                        allow_vm_workspace=vm_workspaces_on_pod_network(),
                        **_completion_control_boundary.dispatch_guard_kwargs(),
                    )
                    if not admitted:
                        logger.warning(
                            "Dispatcher: stateless admission CAS lost for job %s; "
                            "workspace/lane/status changed after preflight",
                            job_id,
                        )
                        continue
                    logger.info(
                        "Dispatcher: admitted stateless worker job %s "
                        "(queue=%s, workspace=k8s-ready)",
                        job_id,
                        queue_result,
                    )
                    continue
                dispatchable_jobs.append(job)

            if not dispatchable_jobs:
                return

            # Get available agents (ready, cooldown passed), skip stale images
            all_agents = await postgres_db.get_available_agents(limit=50)
            available_agents = []
            for ag in all_agents:
                meta = ag.get("metadata") or {}
                if isinstance(meta, str):
                    try:
                        meta = json.loads(meta)
                    except (json.JSONDecodeError, ValueError):
                        meta = {}
                if _agent_sha_is_current(meta):
                    available_agents.append(ag)
                else:
                    # Stale-SHA agents are skipped here; the lifecycle
                    # reconciler is responsible for draining them.
                    logger.debug(
                        "Skipping stale worker agent %s (build_sha=%s)",
                        ag["id"],
                        meta.get("build_sha", ""),
                    )

            # Phase 1: Direct assignment
            matched_job_ids = set()
            matched_agent_ids = set()

            agents_iter = iter(available_agents)
            for job in dispatchable_jobs:
                agent = next(agents_iter, None)
                if agent is None:
                    break  # No more free agents

                job_id = str(job["id"])
                # Atomically claim the job for this agent BEFORE notifying the
                # pod. Closes the dual-leader double-assign that leader election
                # cannot fence (M1): two transient leaders may both scan the same
                # candidate, but only one CAS wins — the loser skips. The claim
                # sets status='processing'+assigned_agent_id; a failed
                # dispatch/resume below self-heals via recover_orphaned_jobs.
                if not await postgres_db.claim_job_for_agent(
                    job_id,
                    str(agent["id"]),
                    **_completion_control_boundary.dispatch_guard_kwargs(),
                ):
                    logger.debug(
                        "Dispatcher: job %s already claimed by another replica; skipping",
                        job_id,
                    )
                    continue
                if resume_lane_applies(
                    job,
                    has_checkpoint=await postgres_db.job_has_checkpoint(job_id),
                ):
                    success = await _job_delivery_operations().resume(job, agent)
                else:
                    if job["status"] == "paused":
                        logger.info(
                            "Dispatcher: job %s is paused with no checkpoint "
                            "to resume from (never started, or pruned at a "
                            "terminal state) — dispatching via the fresh "
                            "/job/start lane",
                            job_id,
                        )
                    success = await _job_delivery_operations().dispatch(job, agent)

                if success:
                    matched_job_ids.add(job_id)
                    matched_agent_ids.add(str(agent["id"]))

            # Phase 1.5: Provision agent pods for unmatched jobs (K8s only)
            remaining = [
                j for j in dispatchable_jobs if str(j["id"]) not in matched_job_ids
            ]
            if remaining and agent_provisioner.is_available:
                for job in remaining:
                    if (
                        await agent_provisioner.active_count()
                        >= agent_provisioner.max_agents
                    ):
                        break
                    pod_name = await agent_provisioner.provision_agent(purpose="job")
                    if pod_name:
                        logger.info(
                            "Provisioned agent %s for pending job %s",
                            pod_name,
                            str(job["id"]),
                        )
                    else:
                        break  # At capacity or error
                    # Don't assign yet — pod needs to register first.
                    # Agent heartbeats "ready" → _trigger_dispatch() → next
                    # cycle matches it.

            # Phase 2: Preemption (non-blocking). D1 placeability guard: only
            # workspace-ready jobs (dispatchable_jobs, not the full pending set)
            # may drive preemption — pausing a running job to free an agent is
            # pointless for a job that has no workspace to run in.
            remaining = [
                j for j in dispatchable_jobs if str(j["id"]) not in matched_job_ids
            ]
            if not remaining:
                return

            candidates = await postgres_db.get_preemption_candidates()
            if not candidates:
                return

            for pending_job in remaining:
                pending_priority = pending_job.get("priority", 5)
                pending_job_id = str(pending_job["id"])

                # D1 Guard 2: a verification/critic subjob whose parent is
                # already terminal can never run, so it must not preempt — even
                # if it slipped past Guard 1 by inheriting a (now-dead) parent
                # workspace. Costs one lookup, only for sub-jobs reaching Phase 2.
                parent_status = None
                parent_id = pending_job.get("parent_job_id")
                if parent_id:
                    parent = await postgres_db.get_job(str(parent_id))
                    parent_status = parent.get("status") if parent else None
                block_reason = preemption_blocked_reason(pending_job, parent_status)
                if block_reason:
                    logger.warning(
                        "Preempt: skipping pending job %s — %s",
                        pending_job_id,
                        block_reason,
                    )
                    continue

                # Find lowest-priority running job that can be preempted
                for candidate in candidates:
                    candidate_id = str(candidate["id"])
                    candidate_priority = candidate.get("priority", 5)

                    # Only preempt if strictly higher priority
                    if pending_priority <= candidate_priority:
                        continue

                    # Skip if already being paused
                    if candidate_id in _pause_pending_job_ids:
                        continue

                    # Skip if already matched (agent taken)
                    if str(candidate.get("assigned_agent_id", "")) in matched_agent_ids:
                        continue

                    # Initiate preemption (fire-and-forget)
                    _pause_pending_job_ids.add(candidate_id)
                    asyncio.create_task(
                        _job_delivery_operations().initiate_pause(candidate)
                    )
                    logger.info(
                        f"Preempt: pausing job {candidate_id} (priority={candidate_priority}) "
                        f"for pending job {pending_job_id} (priority={pending_priority})"
                    )
                    # Remove this candidate so it's not preempted again in this cycle
                    candidates.remove(candidate)
                    break  # One preemption per pending job per cycle

        except Exception as e:
            logger.error(f"Dispatcher error: {e}", exc_info=True)


async def auto_assign_dispatcher(shutdown_event: asyncio.Event) -> None:
    """Background task that periodically dispatches pending jobs to available agents.

    Runs every 30 seconds as a catch-all. Event-driven triggers (job creation,
    agent heartbeat) also call _try_dispatch_pending_jobs() for faster response.
    """
    logger.info(
        "Auto-assign dispatcher started (pinned=%s, stateless_workers=%s)",
        AUTO_ASSIGN_ENABLED,
        STATELESS_WORKER_ENABLED,
    )
    while not shutdown_event.is_set():
        try:
            await _try_dispatch_pending_jobs()
        except Exception as e:
            logger.error(f"Error in auto-assign dispatcher: {e}")

        # Wait 30 seconds or until shutdown
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=30.0)
            break
        except asyncio.TimeoutError:
            pass

    logger.info("Auto-assign dispatcher stopped")


def _trigger_dispatch() -> None:
    """Fire-and-forget trigger for the dispatcher. Safe to call from any endpoint.

    Gated on leadership (M1): only the elected leader dispatches, so a job
    created via a REST handler on a non-leader replica is picked up by the
    leader's periodic dispatcher loop rather than dispatched here.
    """
    from orchestrator.services.leader_election import is_leader

    if (AUTO_ASSIGN_ENABLED or STATELESS_WORKER_ENABLED) and is_leader.is_set():
        asyncio.create_task(_try_dispatch_pending_jobs())


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
    if audit_db is not None and audit_ready:
        try:
            await ensure_audit_partitions(audit_db.pool)
        except Exception:
            logger.warning(
                "Audit partition preflight failed; infrastructure metering "
                "capability probing will remain fail-closed",
                exc_info=True,
            )
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
    infrastructure_metering_settings = InfrastructureMeteringSettings.from_env()
    metering_capabilities = await probe_schema_capabilities(
        postgres_db.pool,
        audit_db.pool if (audit_db is not None and audit_ready) else None,
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
        candidate_storage_mapping = StorageResourceMappingStore(postgres_db.pool)
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
        candidate_storage_assets = StorageAssetStore(postgres_db.pool)
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
        candidate_compute_activation = ComputeActivationStore(postgres_db.pool)
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
    vm_lifecycle_authenticated = bool(nats_bridge.lifecycle_identity_authenticated)
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
            postgres_db.pool,
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
            postgres_db.pool,
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
                    postgres_db.pool,
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
                    postgres_db.pool,
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
        infrastructure_coverage_waivers = CoverageGapWaiverService(postgres_db.pool)
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
                postgres_db.pool,
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
                postgres_db.pool,
                usage_ledger,
                publication_enabled=True,
                enabled_resources=enabled_infrastructure_resources,
                ide_workspace_pod_enabled=ide_publication_ready,
                storage_publication_policy=enabled_storage_publication_policy,
            )
            infrastructure_usage_day_sealer = InfrastructureUsageDaySealer(
                postgres_db.pool,
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
                postgres_db.pool,
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

    # Encrypt any legacy plaintext datasource credentials. Idempotent — once
    # all rows are v1 ciphertexts this is a fast no-op. Lives in lifespan
    # (not init.py) because init.py is not reliably invoked at deploy time, and
    # this is data-integrity critical for the encryption-at-rest guarantee.
    try:
        _bf = await postgres_db.backfill_encrypt_datasource_credentials()
        if _bf["encrypted"] > 0:
            logger.info(
                "Encrypted %d legacy plaintext datasource credentials "
                "(%d skipped, %d errors)",
                _bf["encrypted"],
                _bf["skipped"],
                _bf["errors"],
            )
        elif _bf["errors"] > 0:
            logger.warning(
                "Datasource credentials backfill: %d errors (%d skipped)",
                _bf["errors"],
                _bf["skipped"],
            )
    except Exception as _e:
        logger.error("Datasource credentials backfill failed: %s", _e)

    # Strip any legacy plaintext secrets from threads.metadata.config_override.
    # Persistent-session credentials are injected in-flight at attach/resume and
    # must never be stored (see redact_config_override). Idempotent — once all
    # rows are secret-free this is a fast no-op. Lives in lifespan (not init.py)
    # for the same reason as the datasource backfill above.
    try:
        _sf = await postgres_db.backfill_strip_thread_config_secrets()
        if _sf["stripped"] > 0:
            logger.info(
                "Stripped secrets from %d thread config_override(s) "
                "(%d skipped, %d errors)",
                _sf["stripped"],
                _sf["skipped"],
                _sf["errors"],
            )
        elif _sf["errors"] > 0:
            logger.warning(
                "Thread config_override strip backfill: %d errors (%d skipped)",
                _sf["errors"],
                _sf["skipped"],
            )
    except Exception as _e:
        logger.error("Thread config_override strip backfill failed: %s", _e)

    # Dev-only: seed a fixed admin MCP token from MCP_DEV_TOKEN so a committed
    # .mcp.json works out of the box against a local cluster. Only fires when
    # MCP_DEV_TOKEN is set (unset in prod → no-op, no surprise auto-generated
    # token). Lives in lifespan (not init.py) for the same reason as the
    # backfill above — init.py is not reliably invoked at deploy time. Idempotent
    # and no-ops on a fresh DB with no admin yet; the JIT-provision path in
    # security/auth.py re-fires it the moment the admin user is first created.
    if os.environ.get("MCP_DEV_TOKEN", "").strip():
        try:
            from orchestrator.init import _seed_admin_mcp_token

            await _seed_admin_mcp_token(postgres_db)
        except Exception as _e:
            logger.warning("MCP dev token seed at startup failed (non-fatal): %s", _e)

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
        run_when_leader(stale_agent_detector, _shutdown_event)
    )
    token_cleanup_task = asyncio.create_task(
        cleanup_expired_tokens(postgres_db, _shutdown_event)
    )
    session_cleanup_task = asyncio.create_task(
        cleanup_expired_sessions(postgres_db, _shutdown_event)
    )
    dispatcher_task = asyncio.create_task(
        run_when_leader(auto_assign_dispatcher, _shutdown_event)
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
    sudo_sweeper_task = asyncio.create_task(sudo_expiration_sweeper(_shutdown_event))
    thread_events_prune_task = asyncio.create_task(
        thread_events_prune_sweeper(_shutdown_event)
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
        security_events_prune_sweeper(_shutdown_event)
    )
    # Not leader-gated, matching security_events_prune_task above: a
    # delete-by-age is idempotent, so two replicas racing it is harmless —
    # the second finds nothing.
    ssh_attachments_prune_task = asyncio.create_task(
        ssh_attachments_prune_sweeper(_shutdown_event)
    )
    # In-flight checkpoint retention: bound every live thread's LangGraph
    # checkpoints to the newest N while it runs (leader-gated), so a long job
    # can't fill the checkpointer PVC before it terminates.
    checkpoint_retention_task = asyncio.create_task(
        run_retention_sweeper(postgres_db, _shutdown_event, is_leader.is_set)
    )
    headless_notify_task = asyncio.create_task(
        run_when_leader(thread_permission_notify_sweeper, _shutdown_event)
    )
    # Leader-gated: both snapshot/teardown idle workspaces (attention-sleep) or
    # delete idle IDE VMs/pods (ide-sweeper) after a plain SELECT, with no
    # per-row claim. Under replicas:2 two unguarded copies would double-snapshot
    # to the same S3 key and race teardown against an in-flight snapshot. Gating
    # mirrors the lifecycle reconciler, which already owns the parallel idle
    # workspace-teardown path. See knowledge-base/knowledge/tests/orchestrator_ha_background_loop_sweep.md.
    attention_sleep_task = asyncio.create_task(
        run_when_leader(attention_sleep_sweeper, _shutdown_event)
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
        run_when_leader(ide_session_ttl_sweeper, _shutdown_event)
    )
    ws_sweeper_task = asyncio.create_task(workspace_idle_sweeper(_shutdown_event))
    # Leader-gated: serially SSH-dials every active workspace and captures IDE
    # profiles to per-user S3 keys — two replicas would double-dial each
    # workspace and race the signature-gated capture.
    ide_settings_sweeper_task = asyncio.create_task(
        run_when_leader(code_server_settings_sweeper, _shutdown_event)
    )
    gc_sweeper_task = asyncio.create_task(snapshot_gc_sweeper(_shutdown_event))
    pinned_create_intent_reconciler_task = asyncio.create_task(
        run_when_leader(pinned_agent_create_intent_reconciler, _shutdown_event)
    )
    pinned_create_fence_gc_task = asyncio.create_task(
        run_when_leader(pinned_k8s_create_fence_gc_sweeper, _shutdown_event)
    )
    imap_task = asyncio.create_task(run_when_leader(imap_poll_loop, _shutdown_event))
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
        run_when_leader(agent_pool_reconciler, _shutdown_event)
    )
    # Cleanup authority is independent of fresh protected-mode admission. A
    # feature/config disable must never strand an already durable reader or
    # pre-dispatch effect intent.
    ro_reader_reconciler_task = asyncio.create_task(
        run_when_leader(ro_reader_reconciler_loop, _shutdown_event)
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
        redact_thread_metadata=_redact_thread_metadata,
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
        decide_permission_request=_decide_permission_request,
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
            pause_pending_job_ids=_pause_pending_job_ids,
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

    return capacity_routes.CapacityDependencies(
        snapshot=lambda: capacity_snapshot(postgres_db),
        require_admin=_require_admin,
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
    return ide_routes.IdeDependencies(
        store=postgres_db,
        ide_sessions=ide_session_service,
        ide_proxy=ide_proxy_service,
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
    return thread_files_routes.ThreadFilesDependencies(
        store=postgres_db,
        container_provisioner=container_provisioner,
        vm_provisioner=vm_provisioner,
        thread_workspace_backend=_thread_workspace_backend,
        require_stateless_workspace=_require_stateless_workspace,
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


def _redact_nested_workspace_state(
    record: dict[str, Any], *, field: str
) -> dict[str, Any]:
    """Shared thread/job redaction; policy lives in job_projection."""
    return job_projection.redact_nested_workspace_state(
        record, field=field, runtime_incarnation_key=WORKSPACE_RUNTIME_INCARNATION_KEY
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

    return JobAdmissionConfigDependencies(
        store=postgres_db,
        require_project_access=_require_job_project_access,
        bundled_expert_exists=_bundled_job_expert_exists,
        experts_db_enabled=_is_experts_db_enabled,
        user_experts_enabled=_user_experts_enabled,
        resolve_worker_expert=partial(
            resolve_root_expert, postgres_db, expert_type="worker"
        ),
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


def _redact_thread_metadata(thread: dict[str, Any]) -> dict[str, Any]:
    """Parse and strip credential fields from a thread's ``metadata`` before
    it leaves over REST.

    ``metadata`` is a JSONB column asyncpg hands back as a JSON *string*.
    This helper used to "re-serialize to the original representation", which
    meant the owner-facing thread endpoints returned metadata as a string —
    silently breaking every Cockpit consumer typed against
    ``metadata?: Record<string, unknown>`` (settings-pane config/tools
    prefill, the attached-datasource default, and the REST model/temperature
    seeding — the long-standing "model shows the config name until the
    welcome frame" oddity). The contract is now: metadata always leaves as a
    parsed OBJECT (unparseable/absent → ``{}``).
    """
    raw_retirement_context = thread.get("runtime_retirement_context") or {}
    if isinstance(raw_retirement_context, str):
        try:
            raw_retirement_context = json.loads(raw_retirement_context)
        except (json.JSONDecodeError, TypeError):
            raw_retirement_context = {}
    # The token is installed before abortable turn/Officer preflight.  Only
    # the append-only authorized edge is a public `ending` state; exposing the
    # hidden preflight would make Cockpit retire control even when a non-force
    # End is about to abort as an observational no-op.
    retirement_pending = bool(
        thread.get("runtime_retirement_token") is not None
        and thread.get("runtime_retirement_authorized_at") is not None
    )
    retirement_disposition: str | None = None
    if retirement_pending and isinstance(raw_retirement_context, Mapping):
        candidate = str(raw_retirement_context.get("settle_status") or "")
        if candidate in {"ended", "suspended"}:
            retirement_disposition = candidate

    thread = _redact_nested_workspace_state(thread, field="metadata")
    md = thread.get("metadata")
    if isinstance(md, str):
        try:
            md = json.loads(md)
        except (json.JSONDecodeError, TypeError):
            md = {}
    if not isinstance(md, dict):
        md = {}
    thread = dict(thread)
    md = dict(md)
    if "config_override" in md:
        md["config_override"] = redact_config_override(md["config_override"])
    md.pop("_workspace_binding", None)
    md.pop("_stateless_workspace_process_zero_observation", None)
    thread["metadata"] = md
    # These are internal capabilities or immutable physical cleanup evidence,
    # not owner API fields.  Never let a broad SELECT * list/detail response
    # leak them.  Cockpit gets only the durable, non-secret lifecycle shape.
    for internal_key in (
        "runtime_generation",
        "runtime_attach_token",
        "runtime_attach_abort_receipt",
        "runtime_authority_exposed",
        "runtime_retirement_token",
        "runtime_retirement_permanent",
        "runtime_retirement_started_at",
        "runtime_retirement_authorized_at",
        "runtime_retirement_context",
        "runtime_retirement_stage_receipt",
        "runtime_retirement_local_quiescence",
        "runtime_retirement_external_cleanup",
    ):
        thread.pop(internal_key, None)
    thread["runtime_retirement_pending"] = retirement_pending
    thread["retirement_disposition"] = retirement_disposition
    return thread


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
# Persistent Agent — Thread CRUD + WebSocket Proxy
# =============================================================================


@app.get("/api/persistent/threads/{thread_id}")
async def get_thread(thread_id: str, request: Request) -> dict[str, Any]:
    """Get thread status and metadata (auth: owner only).

    Phase 1 of cloud_collaboration_model.md §9 surfaces the thread's
    attached mounts here so the Cockpit "Project files" panel can render
    them without a second round-trip. ``project_ids`` is the derived
    list-of-strings view kept stable for callers that only need scoping.

    Threads created before migration 0202 have ``ssh_handle IS NULL``; mint
    one lazily here on first view rather than showing an empty SSH panel
    forever. Deliberately not done on the list endpoint — minting up to 50
    handles as a side effect of rendering a list is unwanted write
    amplification.

    The mint is guarded (M-1): it's a write on an otherwise read-only view,
    so a write failure here (a read-only replica, a full disk — this
    deployment has actually had one) must not turn the whole thread view
    into a 500 for the sake of one SSH-panel field. Caught broadly since
    ``ensure_thread_ssh_handle`` can raise asyncpg errors or its own
    exhausted-retries ``RuntimeError``; either way the response degrades to
    a null handle (the panel already renders "unavailable" for that).
    """
    user, thread = await require_thread_owner(request, postgres_db, thread_id)
    result = _redact_thread_metadata(dict(thread))
    if not result.get("ssh_handle"):
        try:
            result["ssh_handle"] = await postgres_db.ensure_thread_ssh_handle(thread_id)
        except Exception:
            logger.warning(
                "ensure_thread_ssh_handle failed for thread %s (non-fatal)",
                str(thread_id)[:8],
                exc_info=True,
            )
    mounts = await postgres_db.list_thread_mounts(thread_id)
    result["cloud_session_url"] = _resolve_cloud_session_url(thread, mounts)
    result["mounts"] = [
        {
            "id": str(m["id"]),
            "mount_kind": m["mount_kind"],
            "target_path": m["target_path"],
            "source_kind": m["source_kind"],
            "source_ref": str(m["source_ref"]) if m.get("source_ref") else None,
            "backend_id": m.get("backend_id"),
        }
        for m in mounts
    ]
    result["project_ids"] = [
        str(m["source_ref"])
        for m in mounts
        if m.get("mount_kind") == "project" and m.get("source_ref")
    ]
    return result


@app.get("/api/persistent/threads/{thread_id}/state")
async def get_thread_session_state(
    thread_id: str, request: Request, response: Response
) -> dict[str, Any]:
    """Lane-agnostic, owner-gated current state for a session Cockpit.

    This is the REST twin of the agent's direct ``session.state`` welcome
    frame.  It intentionally reads durable state for *both* execution lanes;
    no lane or pod identity crosses the wire.  Journal-derived fields are
    point-in-time values at ``event_cursor``.  A client must apply the snapshot
    before replaying the journal from ``replay_cursor`` so the latest logical
    turn is rebuilt before any not-yet-flushed agent edge advances it.
    """

    started = time.perf_counter()
    _user, _thread = await require_thread_owner(request, postgres_db, thread_id)
    auth_done = time.perf_counter()

    # Model/temperature/narration are not all first-class thread columns yet.
    # Resolve from the exact thread row captured inside the snapshot's
    # repeatable-read transaction. A later config write then lands above the
    # returned event cursor and SSE replays it, instead of the cursor hiding a
    # scalar resolved from a different metadata revision.
    config_seconds = 0.0

    async def _resolve_snapshot_config(
        snapshot_thread: dict[str, Any], snapshot_metadata: dict[str, Any]
    ) -> dict[str, Any] | None:
        nonlocal config_seconds
        config_started = time.perf_counter()
        try:
            return await _resolve_session_config(snapshot_thread, snapshot_metadata)
        except GrantDenied:
            logger.warning(
                "Session-state config resolve denied for thread %s; using stored "
                "display fields",
                thread_id,
            )
            return None
        finally:
            config_seconds += time.perf_counter() - config_started

    snapshot = await build_session_state_snapshot(
        postgres_db,
        thread_id,
        config_resolver=_resolve_snapshot_config,
    )
    if snapshot is None:
        raise HTTPException(status_code=404, detail="Thread not found")
    # Pending permissions include tool arguments. Never let a browser or an
    # intermediary retain one user's current control state for another read.
    response.headers["Cache-Control"] = "private, no-store"
    finished = time.perf_counter()
    logger.info(
        "session-state timing: thread=%s auth=%.3fs config=%.3fs "
        "snapshot=%.3fs total=%.3fs",
        thread_id,
        auth_done - started,
        config_seconds,
        max(0.0, finished - auth_done - config_seconds),
        finished - started,
    )
    return snapshot


class ThreadControlRequest(BaseModel):
    """Strict public envelope for the durable control-inbox subset."""

    client_request_id: UUID
    method: Literal["mode.set", "narration.set", "workspace.undo"]
    session_runtime_generation: UUID | None = Field(
        None,
        description=(
            "Runtime generation rendered with the current session. Pinned "
            "admission compares it under the thread-row lock."
        ),
    )
    mode: (
        Literal[
            "supervised",
            "auto_accept",
            "autonomous",
            "silent",
            "verbose",
            "auto",
        ]
        | None
    ) = None

    @model_validator(mode="after")
    def validate_method_mode_pair(self) -> "ThreadControlRequest":
        permission_modes = {"supervised", "auto_accept", "autonomous"}
        narration_modes = {"silent", "verbose", "auto"}
        if self.method == "mode.set" and self.mode not in permission_modes:
            raise ValueError("mode.set requires a permission mode")
        if self.method == "narration.set" and self.mode not in narration_modes:
            raise ValueError("narration.set requires a narration mode")
        if self.method == "workspace.undo" and self.mode is not None:
            raise ValueError("workspace.undo does not accept a mode")
        return self

    def control_payload(self) -> dict[str, Any]:
        """Canonical payload used for idempotency and durable admission."""

        return {} if self.method == "workspace.undo" else {"mode": self.mode}


@app.post(
    "/api/persistent/threads/{thread_id}/controls",
    status_code=202,
)
async def submit_thread_control(
    thread_id: str,
    body: ThreadControlRequest,
    request: Request,
) -> dict[str, Any]:
    """Admit an owner-authorized control for the exact serving owner.

    This endpoint serves both execution lanes and deliberately exposes neither
    one. It persists a commit-ordered request, but neither the desired scalar
    nor a journal frame: the current lease owner (or exact reciprocal pinned
    binding) applies the request and journals the result with its own allocator.
    """
    from shared.run_queue import LANE_STATELESS

    started = time.perf_counter()
    user, thread = await require_thread_owner(request, postgres_db, thread_id)
    thread_owner_id = thread.get("user_id")
    policy_user_id = str(thread_owner_id or user["id"])
    control_payload = body.control_payload()
    control_metadata = thread_metadata_object(thread)
    require_control_generation = bool(
        thread.get("execution_lane") == "pinned"
        and (
            protected_cloud_marker_state(control_metadata) != "off"
            or _require_pinned_status_identity()
        )
    )

    try:
        existing = await find_existing_thread_control(
            postgres_db,
            thread_id=thread_id,
            owner_user_id=thread_owner_id,
            client_request_id=body.client_request_id,
            verb=body.method,
            payload=control_payload,
        )
    except ControlAdmissionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    # A new stateless control can create a control-only queue claim just like
    # human input. Refuse unsupported workspace bindings before that durable
    # admission can wake an executor. Exact idempotent retries remain observable
    # even if the thread's lane/tier changed after their commit.
    if existing is None and thread.get("execution_lane") == LANE_STATELESS:
        _require_stateless_workspace(thread)

    if body.method == "mode.set" and existing is None:
        # Same PDP as create/attach/config.update. A stale or direct client
        # cannot persist a permission mode above the owner's current ceiling.
        # A retry of an already committed UUID bypasses mutable policy: a lost
        # 202 must stay observable even if grants changed afterward.
        try:
            await _enforce_session_create_grants(
                {"interactive": {"permission_mode": body.mode}},
                user_id=policy_user_id,
                project_ids=(
                    [str(thread["project_id"])] if thread.get("project_id") else []
                ),
            )
        except HTTPException:
            # Close the concurrent masked-commit race between the preflight
            # and PDP without weakening authorization for a genuinely new id.
            try:
                existing = await find_existing_thread_control(
                    postgres_db,
                    thread_id=thread_id,
                    owner_user_id=thread_owner_id,
                    client_request_id=body.client_request_id,
                    verb=body.method,
                    payload=control_payload,
                )
            except ControlAdmissionError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            if existing is None:
                raise

    actor_id = str(user.get("id") or user.get("sub") or "rest_client")
    try:
        admitted = await admit_thread_control(
            postgres_db,
            thread_id=thread_id,
            owner_user_id=thread_owner_id,
            client_request_id=body.client_request_id,
            verb=body.method,
            payload=control_payload,
            requested_by=actor_id,
            expected_runtime_generation=body.session_runtime_generation,
            require_pinned_runtime_generation=require_control_generation,
        )
    except ControlAdmissionNotReady as exc:
        # Registration intentionally keeps the exact pinned-owner capability
        # closed until its writer and first inbox drain are ready.  A control
        # clicked during that window is not a semantic conflict: 425 tells the
        # lane-free client to retry the same UUID after its bounded backoff.
        raise HTTPException(status_code=425, detail=str(exc)) from exc
    except ControlAdmissionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    await log_security_event(
        postgres_db,
        resource_type="thread",
        event_type="session_control_requested",
        user=user,
        resource_id=thread_id,
        detail=f"verb={body.method} request_seq={admitted.request_seq}",
        request=request,
    )
    logger.info(
        "session-control admission: thread=%s verb=%s seq=%d duplicate=%s total=%.3fs",
        thread_id,
        body.method,
        admitted.request_seq,
        admitted.duplicate,
        time.perf_counter() - started,
    )
    return {
        "accepted": True,
        "request_id": str(admitted.id),
        "client_request_id": str(admitted.client_request_id),
        "request_seq": admitted.request_seq,
        "method": admitted.verb,
        "state": admitted.state,
        "duplicate": admitted.duplicate,
        "session_runtime_generation": (
            str(admitted.runtime_generation)
            if admitted.runtime_generation is not None
            else None
        ),
    }


async def _session_tool_grants(thread: dict[str, Any]) -> dict[str, Any] | None:
    """The owner's capability grants, for explaining an ``unavailable``.

    ``None`` means "impose no grant-based restriction" — both for an admin
    (``_resolve_runner_grants`` returns ``None``) and for a lookup failure. A
    read surface must never INVENT a denial: the PDP at attach and dispatch is
    the enforcement, this is only the explanation, and a fabricated
    "unavailable — needs the shell_tools grant" is its own D1 violation.
    """
    try:
        project_ids = [str(thread["project_id"])] if thread.get("project_id") else []
        return await _resolve_runner_grants(
            runner_user_id=str(thread.get("user_id"))
            if thread.get("user_id")
            else None,
            project_ids=project_ids,
        )
    except Exception:
        logger.warning(
            "Tool-group grant lookup failed for thread %s; reporting no "
            "grant-based restrictions",
            thread.get("id"),
        )
        return None


@app.get("/api/persistent/threads/{thread_id}/tool-groups")
async def get_thread_tool_groups(thread_id: str, request: Request) -> dict[str, Any]:
    """What toolset does this session's agent actually have? (auth: owner)

    D6: **the answer comes from the agent.** The orchestrator asks the bound
    pod what it bound and serves that; it does not recompute it. Only the agent
    sees the runtime injection layer (``persistent_session._load_tools_for_backend``
    appends the session-task trio, the product guide, the fleet/catalog/workflow
    lists, ``srw_cloud_status``, the officer pair, the datasource categories),
    ``filter_tools_by_backend``, and ``load_tools``'s per-tool fallback. A
    config-only view over-reports by dozens of names, and the divergence
    between two implementations of one fact is the original bug here.

    ``origin`` is the field that matters, and callers MUST branch on it:

    - ``agent`` — **measured, in full**. A running pod enumerated its bound
      tools and returned the structured report. ``observed_at`` and ``backend``
      are set.
    - ``agent_partial`` — **measured, names only**. The pod answered but its
      image predates ``GET /session/toolset``, so the bound names come from
      ``/status`` with no timestamp, no workspace capabilities and no
      agent-side categorisation. ``degraded_reason`` says so. The names are as
      trustworthy as ``agent``; do NOT render a workspace-tier explanation from
      this answer, and do NOT infer measured-ness from ``observed_at``, which
      is legitimately null here.
    - ``prediction`` — **forecast** from the merged config, because there is no
      agent to ask (a new session, a suspended one, an unreachable pod).
      ``prediction_reason`` says which. Structurally weaker, not merely older:
      it cannot see the three layers listed above. Rendering it as fact is D1
      violated at a new seam.

    ``categories`` answers for ALL of them (25, ``mcp`` included), each with
    ``state`` (``on``/``off``/``unavailable``), ``reason`` when not settable,
    ``settable``, ``decided_by`` (the layer that produced the answer) and
    ``tools``. Measured entries also carry ``configured``, so a caller can see
    the merge and the measurement disagree instead of having to trust one.

    ``off`` is a promise that ticking the box would work, and it is only made
    when it can be kept: on a measurement, a category whose merged config
    grants tools while the agent bound none is ``unavailable``. See
    ``compose_tool_view``.

    ``source`` is unchanged and still describes the PREDICTION's model —
    ``resolved`` / ``legacy`` / ``error``. It says nothing about ``origin``:
    a measured answer is a measured answer whichever path the config took.

    ``tool_groups`` (the closed groups, booleans) is retained for the
    cockpit and is now DERIVED from ``categories`` rather than computed beside
    it, so the endpoint cannot disagree with itself.

    ``enumerate_only`` answers the *write* half of the same question: which
    categories refuse ``tools.<c>: true`` at the write boundary, and the
    registry-derived enumeration a caller must send instead
    (``{"shell": ["cancel_command", ...]}``). Without it the only way for the
    New Session form to offer "shell on" would be a hand-maintained tool-name
    list in the cockpit — a fifth parallel list, in the change that deletes
    four. See :func:`src.core.tool_policy.enumerate_only_members`.

    Deliberately NOT a field on ``GET /api/persistent/threads/{id}``: that
    endpoint is hot and this answer costs a config resolve plus a pod probe.
    """
    user, thread = await require_thread_owner(request, postgres_db, thread_id)
    metadata = thread.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (json.JSONDecodeError, TypeError):
            metadata = {}
    request_override = metadata.get("config_override") or None

    base = canonical_config_name(thread.get("config_name") or "session_base")
    if _looks_like_uuid(base):
        # Sentinel / cockpit-conflated expert UUID → the real session base.
        base = "session_base"

    m = await _agent_toolset_measurement(thread)
    grants = await _session_tool_grants(thread)

    from shared.runtime.core.subagent_roster import roster_summary

    source = "resolved"
    configured: dict[str, Any] = {}
    provenance: dict[str, str] = {}
    # What a Delegation tick reaches: the expert's materialised roster, or an
    # empty one the pane can name as such. None only when the resolve failed.
    roster: dict[str, Any] | None = None

    if not _is_experts_db_enabled() or not await _user_experts_enabled():
        source = "legacy"
        configured, provenance = await asyncio.to_thread(
            _legacy_session_tool_policy, base, request_override
        )
        # The legacy path merges a public base only; none carries a roster.
        roster = roster_summary(None)
    else:
        try:
            expert_id = metadata.get("expert_id")
            expert_row = (
                await postgres_db.get_expert_by_id(str(expert_id))
                if expert_id
                else None
            )
            project_id = str(thread["project_id"]) if thread.get("project_id") else None
            project_overrides = None
            if project_id and expert_id:
                link = await postgres_db.get_project_expert_link(
                    project_id=project_id, expert_id=str(expert_id)
                )
                if link:
                    project_overrides = link.get("config_override") or None
                    if isinstance(project_overrides, str):
                        project_overrides = json.loads(project_overrides)
            # Owner-correct, same as _session_tool_grants below: an admin
            # viewing another user's thread must see THAT owner's
            # acknowledged grants, not their own (see _acknowledged_grant_strip
            # and the resume-time owner-vs-caller fix it mirrors).
            grant_strip = await _acknowledged_grant_strip(
                metadata,
                user_id=str(thread["user_id"]) if thread.get("user_id") else None,
                project_id=project_id,
            )
            # The same roster rows the attach prefetches: a DB `$ref` entry
            # the resolve cannot see is dropped, and the pane would then
            # report a roster the agent does bind as missing.
            db_refs = await _prefetch_roster_refs(
                expert_row=expert_row,
                overrides=[project_overrides, request_override],
                user_id=str(thread["user_id"]) if thread.get("user_id") else None,
                project_ids=[project_id] if project_id else [],
            )
            capture: dict[str, Any] = {}
            configured, provenance = await asyncio.to_thread(
                _merged_session_tool_policy,
                base_config_name=base,
                expert_row=expert_row,
                project_overrides=project_overrides,
                request_override=request_override,
                grant_strip=grant_strip,
                db_refs=db_refs,
                capture=capture,
            )
            roster = roster_summary(
                (capture.get("merged_fragment") or {}).get("subagents")
            )
        except Exception:
            logger.exception("Tool-group resolve failed for thread %s", thread_id)
            source = "error"
            if m.categories is None:
                # No measurement AND no resolve: there is nothing honest to
                # report. A resolve error REFUSES the attach (fail closed), so
                # there is no agent answer either.
                return {
                    "thread_id": thread_id,
                    "source": "error",
                    **_origin_fields(m),
                    "tool_groups": None,
                    "categories": None,
                    "subagents": None,
                }

    # Only a MEASURED answer carries backend capabilities: they come from the
    # agent's own report. A prediction has no provisioned workspace to inspect,
    # which is one of the three reasons it over-reports (the live gate saw it
    # over-report by 14 execution tools on a no-shell tier).
    view = compose_tool_view(
        measured=m.categories,
        configured=configured,
        provenance=provenance,
        backend_caps=m.backend,
        grants=grants,
    )
    return {
        "thread_id": thread_id,
        "source": source,
        **_origin_fields(m),
        "enumerate_only": enumerate_only_members(),
        "tool_groups": tool_groups_from_view(view),
        "categories": view,
        "subagents": roster,
    }


class ToolGroupPreviewRequest(BaseModel):
    """What would a session or job created with THIS config bind? (a prediction)"""

    config_name: Optional[str] = None
    expert_id: Optional[str] = None
    project_id: Optional[str] = None
    config_override: Optional[dict[str, Any]] = None
    workspace: Optional[dict[str, Any]] = None
    workspace_preference: Literal["none", "virtual", "sandbox", "vm"] | None = None
    #: Which surface is asking. ``worker`` is the job-create form and defaults
    #: the base to ``worker_base``; ``session`` is the New Session form. Default
    #: stays ``session`` so the shipped cockpit's payloads keep their meaning.
    expert_type: Literal["worker", "session"] = "session"


@app.post("/api/persistent/tool-groups/preview")
async def preview_tool_groups(
    body: ToolGroupPreviewRequest, request: Request
) -> dict[str, Any]:
    """The New Session form's read. **Always a prediction, by construction.**

    There is no agent yet, so this endpoint can never return ``origin:
    "agent"`` — and that is the point of it being a separate route rather than
    a mode of the thread endpoint. D6's consequence is that the creation form
    forecasts while the live pane measures; making the difference structural
    (two routes, one of which cannot ever say "measured") is cheaper to keep
    honest than a flag someone forgets to read.

    ``source`` models the same three agent paths as the thread endpoint and is
    NOT hardcoded: with the experts feature or the per-user kill switch off, a
    created session takes the legacy path, where the compatibility groups are
    APPENDED unless explicitly disabled — the opposite of the resolved path for
    an unset group. Predicting "off" and labelling it ``resolved`` on such a
    deployment would be this series' own defect, rebuilt in the form that
    predicts it.

    Same ``categories`` shape as the thread endpoint, so one renderer serves
    both surfaces.
    """
    user = await require_approved_user(request, postgres_db)
    is_worker = body.expert_type == "worker"
    default_base = "worker_base" if is_worker else "session_base"
    base = canonical_config_name(body.config_name or default_base)
    if _looks_like_uuid(base):
        base = default_base

    expert_row = None
    project_overrides = None
    legacy = not _is_experts_db_enabled() or not await _user_experts_enabled()
    try:
        if body.expert_id and not legacy:
            expert_row = await postgres_db.get_expert_by_id(str(body.expert_id))
            if body.project_id:
                link = await postgres_db.get_project_expert_link(
                    project_id=str(body.project_id), expert_id=str(body.expert_id)
                )
                if link:
                    project_overrides = link.get("config_override") or None
                    if isinstance(project_overrides, str):
                        project_overrides = json.loads(project_overrides)
    except Exception:
        logger.warning("Tool-group preview could not load the expert/project layer")

    from orchestrator.services.manifest_workspace_selection import (
        select_execution_workspace,
    )
    from shared.runtime.core.workspace_selection import bind_execution_workspace

    account = (
        await session_config_resolution.resolve_session_account_defaults(
            str(user["id"]), dependencies=_session_config_dependencies()
        )
        if not is_worker
        else {}
    )
    workspace_config, workspace_selection = await select_execution_workspace(
        postgres_db,
        user,
        project_id=body.project_id,
        role=body.expert_type,
        workspace=body.workspace,
        supplied="workspace" in body.model_fields_set,
        config_override=body.config_override,
        account_defaults=account,
        request=request,
    )
    workspace_source = (
        "project"
        if workspace_selection and workspace_selection.get("project_revision")
        else "request"
        if "workspace" in body.model_fields_set
        or "backend" in ((body.config_override or {}).get("workspace") or {})
        else "default"
    )
    # A creation client can ask to preview its proposed recommendation. It must
    # materialize that choice in the submitted execution; admission never reads it.
    if workspace_source == "default" and body.workspace_preference is not None:
        workspace_config["backend"] = body.workspace_preference
        workspace_source = "recommendation"
    preview_override = bind_execution_workspace(
        body.config_override or {}, workspace_config
    )
    preview_workspace = {
        "backend": workspace_config["backend"],
        "source": workspace_source,
        "binding": workspace_selection["document"]
        if workspace_selection
        else (
            None
            if workspace_config["backend"] == "none"
            else {"template": {"inline": {"backend": workspace_config["backend"]}}}
        ),
    }

    # The legacy branch models ONE agent's behaviour: persistent_session's
    # re-adding of the closed group lists when no disable marker is present.
    # Worker jobs have no such step, so on the worker surface "experts off" only
    # means there is no expert layer to merge — the resolved path already answers
    # that correctly. Routing a worker preview through the session legacy policy
    # would predict appended session groups for a job that cannot hold them.
    use_legacy = legacy and not is_worker
    from shared.runtime.core.subagent_roster import roster_summary

    roster: dict[str, Any] = roster_summary(None)
    try:
        if use_legacy:
            configured, provenance = await asyncio.to_thread(
                _legacy_session_tool_policy, base, preview_override
            )
        else:
            # No grant_strip here: this is a not-yet-created session, so
            # there is no thread and no metadata.config_drift_ack to have
            # acknowledged anything against — unlike the thread endpoint
            # above, omitting it is not a gap to close, it is the correct
            # answer for a config that cannot yet have drifted.
            db_refs = await _prefetch_roster_refs(
                expert_row=expert_row,
                overrides=[project_overrides, body.config_override],
                user_id=str(user["id"]),
                project_ids=[str(body.project_id)] if body.project_id else [],
            )
            capture: dict[str, Any] = {}
            configured, provenance = await asyncio.to_thread(
                _merged_session_tool_policy,
                base_config_name=base,
                expert_row=expert_row,
                project_overrides=project_overrides,
                request_override=preview_override,
                expert_type=body.expert_type,
                db_refs=db_refs,
                capture=capture,
            )
            roster = roster_summary(
                (capture.get("merged_fragment") or {}).get("subagents")
            )
    except Exception:
        logger.exception("Tool-group preview resolve failed")
        raise HTTPException(
            status_code=422,
            detail="This configuration cannot be resolved, so its toolset "
            "cannot be predicted.",
        )

    try:
        grants = await _resolve_runner_grants(
            runner_user_id=str(user["id"]),
            project_ids=[str(body.project_id)] if body.project_id else [],
        )
    except Exception:
        logger.warning("Tool-group preview grant lookup failed")
        grants = None

    view = compose_tool_view(
        measured=None,
        configured=configured,
        provenance=provenance,
        backend_caps={
            "supports_shell": workspace_config["backend"] in ("sandbox", "vm"),
            "supports_file_tools": workspace_config["backend"] != "none",
            "supports_canvas_presentation": workspace_config["backend"] != "none",
        },
        grants=grants,
    )
    return {
        "workspace": preview_workspace,
        "source": "legacy" if use_legacy else "resolved",
        **_origin_fields(
            _unmeasured(
                "no agent exists for an unsaved job"
                if is_worker
                else "no agent exists for an unsaved session"
            )
        ),
        "enumerate_only": enumerate_only_members(),
        "tool_groups": tool_groups_from_view(view),
        "categories": view,
        "subagents": roster,
    }


@app.patch("/api/persistent/threads/{thread_id}")
async def update_thread(
    thread_id: str, body: ThreadUpdateRequest, request: Request
) -> dict[str, str]:
    """Rename a persistent thread (auth: owner only).

    The title was previously settable only at creation and auto-generated
    once by the LLM after the first turn; this lets the user rename a session
    inline from the Cockpit. A user-chosen title naturally blocks the
    auto-titler, which only overwrites empty / "Untitled Session" / "Local
    Session" titles (src/api/persistent_app.py).
    """
    user, thread = await require_thread_owner(request, postgres_db, thread_id)
    title = (body.title or "").strip()
    if not title:
        raise HTTPException(status_code=400, detail="Title cannot be empty")
    if len(title) > 200:
        raise HTTPException(status_code=400, detail="Title too long (max 200)")
    await postgres_db.update_thread_title(thread_id, title)
    return {"status": "updated", "title": title}


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


@app.get("/api/persistent/threads/{thread_id}/citations")
async def get_thread_citations(
    thread_id: str,
    request: Request,
    limit: int = Query(default=200, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    """List citations created in a persistent session, for inline ``[N]`` rendering.

    The citation engine stores a session's citations with ``job_id = thread_id``
    (it maps ``CitationContext.session_id`` → ``job_id``), so the thread UUID *is*
    the ``job_id`` — there is no separate thread column. Owner-only (the by-job
    endpoint 404s for a thread since no ``jobs`` row exists). The marker the agent
    emits is the citation ``id``; the cockpit renumbers for display and resolves
    each ``[id]`` to a row returned here.
    """
    await require_thread_owner(request, postgres_db, thread_id)
    try:
        async with vector_db.acquire() as conn:
            count_row = await conn.fetchrow(
                "SELECT COUNT(*) AS total FROM citations WHERE job_id = $1::uuid",
                thread_id,
            )
            total = count_row["total"] if count_row else 0
            rows = await conn.fetch(
                """SELECT c.id, LEFT(c.claim, 300) AS claim, c.source_id,
                       s.name AS source_name, s.type::text AS source_type,
                       s.identifier AS source_identifier,
                       c.verification_status::text AS verification_status,
                       c.confidence::text AS confidence,
                       c.created_at, s.metadata
                FROM citations c
                JOIN sources s ON c.source_id = s.id
                WHERE c.job_id = $1::uuid
                ORDER BY c.id ASC
                LIMIT $2 OFFSET $3""",
                thread_id,
                limit,
                offset,
            )
            citations = []
            for r in rows:
                d = dict(r)
                # Cloud-document citations (cite_document with a snapshot-anchor)
                # can offer "view original" (/snapshot) + on-view drift (/drift);
                # web citations have neither. Surface the two flags so the cockpit
                # only renders those controls where they apply. The raw metadata
                # isn't returned (internal blob keys / anchor URLs).
                cloud = citations_operations._source_cloud_meta(d.pop("metadata", None))
                d["has_cloud_anchor"] = bool(cloud)
                d["has_snapshot"] = bool(cloud.get("snapshot_blob_key"))
                citations.append(d)
            return {
                "citations": citations,
                "total": total,
                "thread_id": thread_id,
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


def _stamp_tool_categories(messages: list[dict[str, Any]]) -> None:
    """Annotate replayed tool calls with their registry category, in place.

    The live SSE ``tool.started`` frame carries ``category`` (see graph.py's
    ``_get_tool_category``), but the stored ``thread_messages.tool_calls`` JSONB
    never did. Without this the cockpit's folded-chip summary buckets every
    replayed call as "other", so one turn reads
    "19× citations · 12× searches" while streaming and "38× steps" after a
    reload — same turn, same data, different answer.

    Derived at read time rather than persisted so that re-categorising a tool
    doesn't need a backfill of historical rows. Unknown tools (renamed, removed,
    or from another deployment) simply get no category and fall back to the
    cockpit's "other" bucket, which is the honest answer.
    """
    for m in messages:
        for tc in m.get("tool_calls") or []:
            category = TOOL_REGISTRY.get(tc.get("name") or "", {}).get("category")
            if category:
                tc["category"] = category


@app.get("/api/persistent/threads/{thread_id}/messages")
async def get_thread_messages_history(
    thread_id: str,
    request: Request,
    response: Response = None,
    limit: Optional[int] = None,
    before: Optional[str] = None,
    after: Optional[str] = None,
    offset: int = 0,
) -> dict[str, Any]:
    """Load message history for a persistent thread, ascending (chronological).

    Default (no params) returns the **entire** conversation — the cockpit caches
    the full thread client-side and windows the render itself, so the display
    must not be truncated. Cursor paging (mutually exclusive, ISO-8601):

    - ``before=<ts>``: backfill — newest messages at-or-before the cursor, up to
      ``limit``.
    - ``after=<ts>``:  catch-up — messages at-or-after the cursor, up to ``limit``.

    A bare ``limit`` with no cursor keeps the legacy oldest-first paged read
    (``offset`` honored) used by the MCP inspection tool. Returns
    ``{messages, total, has_more, thread_id}``.
    """
    user, thread = await require_thread_owner(request, postgres_db, thread_id)
    if response is not None:
        response.headers["Cache-Control"] = "private, no-store"

    def _parse_cursor(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise HTTPException(
                status_code=400, detail=f"Invalid ISO-8601 timestamp: {value!r}"
            )

    before_dt = _parse_cursor(before)
    after_dt = _parse_cursor(after)
    if before_dt is not None and after_dt is not None:
        raise HTTPException(
            status_code=400, detail="Pass at most one of 'before' / 'after'"
        )

    capped_limit = min(limit, 500) if limit is not None else None

    # Rows and their cache fence come from one repeatable-read snapshot. A
    # rewind cannot therefore pair its newer epoch/revision with an older page
    # that still contains tombstoned messages (or vice versa).
    async with postgres_db.acquire() as conn:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            history_state = await conn.fetchrow(
                "SELECT events_epoch, conversation_revision FROM threads WHERE id=$1",
                thread_id,
            )
            if history_state is None:
                raise HTTPException(status_code=404, detail="Thread not found")
            if before_dt is not None or after_dt is not None:
                messages, has_more = await postgres_db.get_thread_messages_page(
                    thread_id=thread_id,
                    before=before_dt,
                    after=after_dt,
                    limit=capped_limit,
                    conn=conn,
                )
                # A cursor window carries no cheap true total; no consumer reads it here.
                total = len(messages)
            else:
                messages = await postgres_db.get_thread_messages_history(
                    thread_id=thread_id,
                    limit=capped_limit,
                    offset=offset,
                    conn=conn,
                )
                # Legacy paged read: a full page implies there may be more.
                has_more = capped_limit is not None and len(messages) == capped_limit
                if capped_limit is None:
                    total = len(messages)
                else:
                    total = await postgres_db.get_thread_message_count(
                        thread_id, conn=conn
                    )

    _stamp_tool_categories(messages)

    return {
        "messages": messages,
        "total": total,
        "has_more": has_more,
        "thread_id": thread_id,
        "events_epoch": int(history_state["events_epoch"] or 0),
        "conversation_revision": int(history_state["conversation_revision"] or 0),
    }


# =============================================================================
# Headless persistent sessions — Phase 2 SSE + REST transport
# =============================================================================
#
# SSE replaces the WebSocket as the primary server→client path; the existing
# /ws/persistent/{thread_id} stays as a fallback. Per
# knowledge-base/knowledge/features/headless_persistent_sessions.md.
#
# The per-turn input lock guards against duplicate POSTs from concurrent
# cockpit tabs racing on the same turn. Single-instance orchestrator, so a
# module-level dict is enough; entries auto-clean 5 min after release.

_thread_turn_locks: dict[tuple[str, int], asyncio.Lock] = {}
_thread_turn_inflight: dict[str, int] = {}


def _ensure_thread_turn_lock(thread_id: str, turn_id: int) -> asyncio.Lock:
    """Get or create the lock for (thread_id, turn_id). Concurrent callers
    landing on the same tuple share the same Lock object."""
    key = (thread_id, turn_id)
    lock = _thread_turn_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _thread_turn_locks[key] = lock
    return lock


def _schedule_turn_lock_cleanup(thread_id: str, turn_id: int) -> None:
    """Remove the lock entry 5 minutes after release. Memory-leak guard
    for long-lived sessions accumulating per-turn locks."""

    async def _later() -> None:
        await asyncio.sleep(300)
        _thread_turn_locks.pop((thread_id, turn_id), None)
        if _thread_turn_inflight.get(thread_id) == turn_id:
            _thread_turn_inflight.pop(thread_id, None)

    asyncio.create_task(_later(), name=f"turn-lock-cleanup-{thread_id[:8]}")


async def _resolve_thread_for_forwarding(
    thread_id: str, user: dict
) -> tuple[dict, PinnedSessionBinding]:
    """Resolve one owner-visible thread and its exact pinned runtime binding.

    Stateless callers branch before this helper.  All agent/endpoint fields in
    the result come from one reciprocal DB snapshot rather than independent
    thread and agent reads.  A suspended pinned workspace is restored before
    that final snapshot.
    """
    thread = await postgres_db.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    # Fail-closed for orphans (user_id IS NULL); admins bypass.
    if not user.get("is_admin") and str(thread.get("user_id") or "") != str(user["id"]):
        raise HTTPException(status_code=403, detail="Not your thread")
    if not _thread_accepts_runtime(thread):
        raise HTTPException(
            status_code=409, detail=thread_runtime_refusal_detail(thread)
        )
    if thread.get("execution_lane") != "pinned":
        raise HTTPException(
            status_code=409,
            detail="Thread execution lane does not support direct forwarding",
        )

    async def _refresh_runtime_authority() -> dict[str, Any]:
        current = await postgres_db.get_thread(thread_id)
        if not _thread_accepts_runtime(current):
            raise HTTPException(
                status_code=409, detail=thread_runtime_refusal_detail(current)
            )
        if not user.get("is_admin") and str(current.get("user_id") or "") != str(
            user["id"]
        ):
            raise HTTPException(status_code=403, detail="Not your thread")
        if current.get("execution_lane") != "pinned":
            raise HTTPException(
                status_code=409,
                detail="Thread execution lane does not support direct forwarding",
            )
        marker = protected_cloud_marker_state(thread_metadata_object(current))
        if marker == "malformed":
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "protected_cloud_malformed",
                    "message": "Protected cloud session state is invalid.",
                },
            )
        if marker == "on":
            state, code = await _protected_cloud_delivery_state(
                current, thread_metadata_object(current)
            )
            if state != "ready":
                raise HTTPException(
                    status_code=425,
                    detail={
                        "code": "protected_cloud_not_ready",
                        "state": state,
                        "reason": code,
                    },
                )
        return current

    thread = await _refresh_runtime_authority()

    # Restore suspended workspace before forwarding (mirrors persistent_ws_proxy)
    metadata = thread.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (json.JSONDecodeError, TypeError):
            metadata = {}
    ws_ctx = metadata.get("workspace_container") or {}
    if ws_ctx.get("status") == "suspended" and workspace_suspension_service.is_enabled:
        logger.info("Restoring suspended workspace for thread %s", thread_id)
        ok = await workspace_suspension_service.restore_thread_workspace(thread_id)
        if not ok:
            raise HTTPException(
                status_code=503,
                detail="Failed to restore suspended workspace",
            )
        thread = await _refresh_runtime_authority()

    runtime_authority = thread_runtime_authority(thread)
    if runtime_authority is None:  # _refresh_runtime_authority proves this
        raise HTTPException(
            status_code=409, detail=thread_runtime_refusal_detail(thread)
        )
    binding = await postgres_db.get_pinned_session_binding(
        thread_id,
        expected_runtime_generation=runtime_authority.generation,
    )
    if binding is None:
        raise HTTPException(
            status_code=409,
            detail=pinned_binding_invalid_detail(runtime_authority),
        )
    _require_forwardable_pinned_binding(binding)
    return thread, binding


def _require_forwardable_pinned_binding(binding: PinnedSessionBinding) -> None:
    """Require a currently live agent status without freezing status equality."""

    if binding.agent_status not in {"ready", "working", "session"}:
        raise HTTPException(status_code=425, detail="session not ready")


def _binding_runtime_authority(
    binding: PinnedSessionBinding,
) -> ThreadRuntimeAuthority:
    return ThreadRuntimeAuthority(
        thread_id=binding.thread_id,
        generation=binding.runtime_generation,
    )


async def _revalidate_pinned_forwarding_binding(
    binding: PinnedSessionBinding,
) -> PinnedSessionBinding:
    """Re-read and compare every immutable DB/routing coordinate."""

    current = await postgres_db.get_pinned_session_binding(
        binding.thread_id,
        expected_runtime_generation=binding.runtime_generation,
    )
    if current is None or current.target_key != binding.target_key:
        raise HTTPException(
            status_code=409,
            detail=pinned_binding_invalid_detail(_binding_runtime_authority(binding)),
        )
    _require_forwardable_pinned_binding(current)
    return current


async def _forward_to_agent(
    binding: PinnedSessionBinding,
    path: str,
    payload: dict,
    timeout: float = 30.0,
) -> dict[str, Any]:
    """POST to one exact pinned Pod after a client-boundary DB reread."""

    identity_fingerprint = binding.session_identity_fingerprint
    forwarded_payload = dict(payload)
    supplied_fingerprint = forwarded_payload.get("session_identity_fingerprint")
    if supplied_fingerprint not in (None, identity_fingerprint):
        raise ValueError("forwarded session identity does not match its binding")
    forwarded_payload["session_identity_fingerprint"] = identity_fingerprint
    agent_url = f"http://{binding.pod_ip}:{binding.pod_port}{path}"
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            # Client/pool entry may await.  Re-read after it so no stale target
            # receives an effect merely because it was authoritative before
            # transport setup.  The endpoint validates the fingerprint again
            # across the final network race.
            await _revalidate_pinned_forwarding_binding(binding)
            response = await client.post(agent_url, json=forwarded_payload)
    except HTTPException:
        raise
    except Exception as e:
        logger.warning(
            "Agent forward failed: %s %s -> %s",
            path,
            binding.agent_id,
            e,
        )
        raise HTTPException(status_code=503, detail=f"Agent unreachable: {e}") from e
    try:
        response_body = response.json()
    except Exception:
        response_body = None
    if (
        response.status_code == 409
        and isinstance(response_body, dict)
        and response_body.get("error") == "session_identity_mismatch"
    ):
        raise HTTPException(
            status_code=409,
            detail=pinned_binding_invalid_detail(_binding_runtime_authority(binding)),
        )
    if response.status_code == 503:
        if (
            isinstance(response_body, dict)
            and response_body.get("error") == "runtime_terminating"
        ):
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "runtime_terminating",
                    "retryable": True,
                    "message": "The runtime is terminating; retry on its replacement.",
                },
                headers={"Retry-After": response.headers.get("Retry-After", "5")},
            )
    if response.status_code >= 500:
        raise HTTPException(
            status_code=502,
            detail=f"Agent error: {response.status_code} {response.text[:200]}",
        )
    if response.status_code >= 400:
        raise HTTPException(
            status_code=response.status_code,
            detail=response.text[:200],
        )
    return (
        response_body
        if isinstance(response_body, dict)
        else {"raw": response.text[:500]}
    )


async def _no_cursor_replay_start(conn, thread_id: str, epoch: int) -> int:
    """Replay floor (exclusive) for an SSE attach that carries no cursor.

    A fresh client — opening the session on a second device, or any client
    with no cached cursor for this thread — has already painted the thread's
    completed turns from REST history. Replaying the whole epoch from seq 0
    would re-deliver each completed turn as a *live* copy the cockpit reducer
    can't reconcile (history turns are keyed by message id, replayed turns by
    turn_id), so the last assistant turn renders twice, split by a spurious
    "SESSION RESUMED" divider — the cold-attach twin of the gone_beyond_horizon
    duplicate render.

    Anchor instead just past the last turn-terminal event (``turn.completed`` /
    ``turn.error``, both of which persist their turn to ``thread_messages``), so
    the replay carries only the in-flight, not-yet-persisted turn. Returns 0
    when no turn has finished yet (first turn still streaming) so that turn —
    absent from REST history — still replays from the start.
    """
    anchor = await conn.fetchval(
        "SELECT COALESCE(MAX(seq), 0) FROM thread_events "
        "WHERE thread_id = $1 AND epoch = $2 "
        "AND kind IN ('turn.completed', 'turn.error')",
        thread_id,
        epoch,
    )
    return int(anchor or 0)


# How much *accumulated idle time* (seconds with no new rows) a live SSE stream
# tolerates before it re-reads `events_epoch` to detect a mid-stream bump. The
# epoch is bumped when an agent (re-)attaches; a generator opened before the
# bump would otherwise poll the dead old epoch forever, delivering nothing but
# keepalive pings that fool the client watchdog into thinking the stream is
# healthy (the "stale → refresh to fix" zombie). Read as a module global so
# tests can monkeypatch it to 0 to force a re-check on the first empty poll.
THREAD_EVENTS_EPOCH_RECHECK_S: float = float(
    os.environ.get("THREAD_EVENTS_EPOCH_RECHECK_S", "2.0")
)
THREAD_CLIENT_PRESENCE_RENEW_S: float = max(
    1.0,
    float(
        os.environ.get(
            "THREAD_CLIENT_PRESENCE_RENEW_S",
            str(DEFAULT_PRESENCE_RENEW_SECONDS),
        )
    ),
)
THREAD_CLIENT_PRESENCE_TTL_S: float = max(
    THREAD_CLIENT_PRESENCE_RENEW_S * 2.0,
    float(
        os.environ.get(
            "THREAD_CLIENT_PRESENCE_TTL_S",
            str(DEFAULT_PRESENCE_TTL_SECONDS),
        )
    ),
)


@app.get("/api/persistent/threads/{thread_id}/stream")
async def thread_event_stream(thread_id: str, request: Request) -> StreamingResponse:
    """SSE: stream this thread's event log with replay-from-cursor.

    The client sends `Last-Event-ID: <epoch>:<seq>` to resume from a known
    point. If the cursor's epoch doesn't match the server, or its seq is
    older than retention, the server emits a single `gone_beyond_horizon`
    event and closes — the client must drop its cursor and re-sync.

    Otherwise: replay everything since the cursor, then switch to live
    mode (200ms poll, adaptive backoff to 1s after 5 empty polls).
    """
    user, thread = await require_thread_owner(request, postgres_db, thread_id)

    # The existing owner-gated SSE connection is the lane-agnostic client
    # attachment signal. No lane field crosses the wire. Pinned streams keep
    # their exact behavior; a stateless stream must establish its durable TTL
    # before the browser can believe it is attached.
    track_presence = thread.get("execution_lane") == "stateless"
    if track_presence:
        try:
            presence = await refresh_thread_presence(
                postgres_db,
                thread_id=thread_id,
                ttl_seconds=THREAD_CLIENT_PRESENCE_TTL_S,
                establish=True,
            )
        except Exception as exc:
            logger.warning(
                "thread_event_stream presence establish failed (thread=%s): %s",
                thread_id,
                exc,
            )
            raise HTTPException(
                status_code=503,
                detail="Session presence is temporarily unavailable",
            ) from exc
        if not presence.served:
            # The row changed lane or disappeared after the owner lookup. A
            # reconnect re-runs authorization and resolves the current lane.
            raise HTTPException(status_code=409, detail="Session lane changed")

    server_epoch = int(thread.get("events_epoch") or 0)

    # Parse Last-Event-ID. Format: "<epoch>:<seq>". Missing/malformed → no
    # cursor, so the replay floor is computed by _no_cursor_replay_start below
    # (anchored past the last completed turn, not seq 0).
    #
    # EventSource doesn't let the browser set custom request headers, so the
    # cockpit hands us the cached cursor via `?last_event_id=` for the
    # initial connection. On automatic reconnect, the browser appends the
    # `Last-Event-ID` header from the latest `id:` line we yielded — that
    # path is fully native and doesn't need the query param.
    last_event_id = (
        request.headers.get("Last-Event-ID")
        or request.headers.get("last-event-id")
        or request.query_params.get("last_event_id")
    )
    cursor_epoch: Optional[int] = None
    cursor_seq: Optional[int] = None
    if last_event_id:
        try:
            e_str, s_str = last_event_id.split(":", 1)
            cursor_epoch = int(e_str)
            cursor_seq = int(s_str)
        except (ValueError, AttributeError):
            cursor_epoch = None
            cursor_seq = None

    async def event_stream():
        # Kickstart: flush a comment immediately so the browser EventSource
        # fires `onopen` at once and buffering intermediaries (Cloudflare
        # Tunnel, Traefik) don't hold the response headers / idle-timeout the
        # connection waiting for the first body byte. Without this, a connect
        # whose cursor is already at the tail sends nothing until the ~20s
        # keepalive ping below — stalling the SSE receive path ~20s. Comments
        # (lines starting with `:`) are ignored by EventSource, so this is
        # side-effect-free on the client.
        yield ": open\n\n"

        next_presence_renew = time.monotonic() + THREAD_CLIENT_PRESENCE_RENEW_S

        # Mismatched epoch → force re-sync.
        if cursor_epoch is not None and cursor_epoch != server_epoch:
            async with postgres_db.acquire() as conn:
                tail = await conn.fetchval(
                    "SELECT COALESCE(MAX(seq), 0) FROM thread_events "
                    "WHERE thread_id = $1 AND epoch = $2",
                    thread_id,
                    server_epoch,
                )
            payload = json.dumps(
                {
                    "method": "gone_beyond_horizon",
                    "params": {
                        "epoch": server_epoch,
                        "server_seq": int(tail or 0),
                        "reason": "epoch_mismatch",
                    },
                }
            )
            yield f"id: {server_epoch}:0\nevent: gone_beyond_horizon\ndata: {payload}\n\n"
            return

        # Retention floor for the current epoch.
        async with postgres_db.acquire() as conn:
            min_seq = await conn.fetchval(
                "SELECT MIN(seq) FROM thread_events "
                "WHERE thread_id = $1 AND epoch = $2",
                thread_id,
                server_epoch,
            )
        min_seq = int(min_seq) if min_seq is not None else 0

        # Cursor older than retention → also force re-sync.
        if cursor_seq is not None and min_seq > 0 and cursor_seq < min_seq - 1:
            async with postgres_db.acquire() as conn:
                tail = await conn.fetchval(
                    "SELECT COALESCE(MAX(seq), 0) FROM thread_events "
                    "WHERE thread_id = $1 AND epoch = $2",
                    thread_id,
                    server_epoch,
                )
            payload = json.dumps(
                {
                    "method": "gone_beyond_horizon",
                    "params": {
                        "epoch": server_epoch,
                        "server_seq": int(tail or 0),
                        "retention_min_seq": min_seq,
                        "reason": "cursor_older_than_retention",
                    },
                }
            )
            yield f"id: {server_epoch}:0\nevent: gone_beyond_horizon\ndata: {payload}\n\n"
            return

        # Replay floor. With a cursor, resume right after it. Without one, a
        # fresh attach has already loaded completed turns from REST history, so
        # anchor past the last completed turn instead of replaying the whole
        # epoch from 0 (which doubles the last assistant turn + shows a spurious
        # "SESSION RESUMED" divider — see _no_cursor_replay_start).
        if cursor_seq is not None:
            last_sent_seq = cursor_seq
        else:
            async with postgres_db.acquire() as conn:
                last_sent_seq = await _no_cursor_replay_start(
                    conn, thread_id, server_epoch
                )
        empty_polls = 0
        idle_keepalive_at = 0.0
        epoch_idle = 0.0
        cancelled = False
        try:
            while not cancelled:
                if await request.is_disconnected():
                    break
                if track_presence and time.monotonic() >= next_presence_renew:
                    # A long-lived stream does not retain authorization from
                    # its opening handshake forever. Re-run the same BFF-cookie
                    # owner gate before every attested renewal; expiry or an
                    # ownership change closes the stream and writes no TTL.
                    _renew_user, renew_thread = await require_thread_owner(
                        request, postgres_db, thread_id
                    )
                    if renew_thread.get("execution_lane") != "stateless":
                        return
                    presence = await refresh_thread_presence(
                        postgres_db,
                        thread_id=thread_id,
                        ttl_seconds=THREAD_CLIENT_PRESENCE_TTL_S,
                        establish=False,
                    )
                    if not presence.served:
                        # Lane change/deletion: close. EventSource reconnects
                        # through require_thread_owner and current DB truth.
                        return
                    next_presence_renew = (
                        time.monotonic() + THREAD_CLIENT_PRESENCE_RENEW_S
                    )
                async with postgres_db.acquire() as conn:
                    rows = await conn.fetch(
                        "SELECT seq, kind, payload "
                        "FROM thread_events "
                        "WHERE thread_id = $1 AND epoch = $2 AND seq > $3 "
                        "ORDER BY seq ASC "
                        "LIMIT 500",
                        thread_id,
                        server_epoch,
                        last_sent_seq,
                    )
                    # Zombie-epoch guard: after enough accumulated idle time
                    # with no new rows, re-read events_epoch on the SAME
                    # connection (no extra acquire). If an agent re-attached and
                    # bumped the epoch, this generator has been polling a dead
                    # epoch — terminate deterministically so the client
                    # re-anchors, instead of feeding it pings forever.
                    if not rows and epoch_idle >= THREAD_EVENTS_EPOCH_RECHECK_S:
                        epoch_idle = 0.0
                        current_epoch = await conn.fetchval(
                            "SELECT events_epoch FROM threads WHERE id = $1",
                            thread_id,
                        )
                        if current_epoch is None:
                            # Thread deleted mid-stream — terminate silently;
                            # the client's reconnect hits require_thread_owner
                            # → 404 and it drops the thread.
                            return
                        if int(current_epoch) != server_epoch:
                            new_epoch = int(current_epoch)
                            # Anchor past the last completed turn of the NEW
                            # epoch, not its tail: the bump lands mid-turn and
                            # the client's history reload only carries completed
                            # turns, so a tail anchor would drop the in-flight
                            # turn's already-journaled frames.
                            anchor = await _no_cursor_replay_start(
                                conn, thread_id, new_epoch
                            )
                            logger.info(
                                "thread_event_stream epoch bump %d→%d "
                                "(thread=%s), re-anchoring client to seq %d",
                                server_epoch,
                                new_epoch,
                                thread_id,
                                anchor,
                            )
                            payload = json.dumps(
                                {
                                    "method": "gone_beyond_horizon",
                                    "params": {
                                        "epoch": new_epoch,
                                        "server_seq": anchor,
                                        "reason": "epoch_bumped_mid_stream",
                                    },
                                }
                            )
                            # The `id:` line carries the new epoch's floor so a
                            # browser-native reconnect (bypassing the app
                            # handler) converges to the same replay start
                            # instead of replaying the new epoch from :0.
                            yield (
                                f"id: {new_epoch}:{anchor}\n"
                                f"event: gone_beyond_horizon\n"
                                f"data: {payload}\n\n"
                            )
                            return
                if rows:
                    empty_polls = 0
                    epoch_idle = 0.0
                    for row in rows:
                        seq = int(row["seq"])
                        # row["payload"] is a JSONB column — asyncpg may
                        # return it as str or already-parsed dict depending
                        # on codec registration.
                        raw_payload = row["payload"]
                        if isinstance(raw_payload, str):
                            payload_obj = json.loads(raw_payload)
                        else:
                            payload_obj = raw_payload
                        frame = {
                            "method": row["kind"],
                            "params": payload_obj,
                        }
                        body = json.dumps(frame)
                        yield f"id: {server_epoch}:{seq}\ndata: {body}\n\n"
                        last_sent_seq = seq
                    idle_keepalive_at = 0.0
                else:
                    # Adaptive backoff: 200ms × 5 empty polls, then 1s.
                    empty_polls += 1
                    wait = 1.0 if empty_polls >= 5 else 0.2
                    epoch_idle += wait
                    # Typed `ping` event every ~20s of idle. A bare `:`
                    # comment would keep the socket warm but never fire
                    # `onmessage` in the browser, leaving silent network
                    # drops undetectable client-side. A typed event with no
                    # `id:` line lets the cockpit watchdog observe liveness
                    # without advancing the replay cursor.
                    idle_keepalive_at += wait
                    if idle_keepalive_at >= 20.0:
                        yield "event: ping\ndata: {}\n\n"
                        idle_keepalive_at = 0.0
                    try:
                        await asyncio.sleep(wait)
                    except asyncio.CancelledError:
                        cancelled = True
                        break
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.warning("thread_event_stream error (thread=%s): %s", thread_id, e)
            return

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


class ThreadInputRequest(BaseModel):
    """Body for POST /api/persistent/threads/{thread_id}/input."""

    content: str
    turn_id: Optional[int] = None
    expected_conversation_revision: int | None = Field(default=None, ge=0)


async def _load_thread_for_owner(thread_id: str, user: dict) -> dict:
    """Load a thread under the same owner gate ``_resolve_thread_for_forwarding``
    applies (404 unknown; fail-closed 403 for orphans and non-owners; admin
    bypass) — WITHOUT its agent-resolution / workspace-restore side effects.

    Used by the stateless-lane branches: queue-lane threads have no bound
    agent, so the forwarding resolver's 503 would mask the lane entirely.
    """
    thread = await postgres_db.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    if not user.get("is_admin") and str(thread.get("user_id") or "") != str(user["id"]):
        raise HTTPException(status_code=403, detail="Not your thread")
    return thread


async def _thread_input_stateless(
    thread: dict,
    content: str,
    expected_conversation_revision: int | None = None,
) -> dict[str, Any]:
    """Admit one user turn for a stateless-lane thread (stateless_agents.md
    §5.3.1): persist the message, advance the input watermark, and queue the
    unit — all in ONE transaction, so "message durable ⟺ watermark advanced"
    can never tear and a signal can never be lost.

    The message row is indistinguishable from the agent's accept-time persist
    of a plain-text human message (``src/api/persistent_app._accept_user_input``
    → ``src/database/postgres_db.save_thread_message``): same ``msg_`` id mint
    with the agent's own uuid5 row-id coercion, ``role='human'``,
    ``turn_number = total_turns + 1``, all other columns at their NULL
    defaults, and the same ``threads`` last_activity/total_turns bump.

    Admission is ``record_input_seq`` — the input-during-anything path: it
    creates a fresh ``'queued'`` row, revives ``'done'``, merges the watermark
    into ``'queued'``, bumps ONLY the watermark on ``'leased'`` (the running
    turn's completion re-queues via ``input_seq > consumed_seq``), and records
    input on ``'parked'`` without reviving it (explicit unpark only). No
    separate ``enqueue_unit`` call is needed: every branch leaves the unit
    queued, leased-with-watermark, or deliberately parked.
    """
    from shared.row_identity import _coerce_row_id
    from orchestrator.services.stateless_queue_state import queue_block
    from shared.run_queue import (
        LANE_STATELESS,
        UNIT_KIND_SESSION_TURN,
        queue_depth_for,
        queue_state_for,
        record_input_seq,
    )

    # The unlocked preflight provides a fast refusal. The locked copy below is
    # authoritative against lane/tier/lifecycle changes before message commit.
    _require_stateless_workspace(thread)

    thread_id = str(thread["id"])
    # Mirror the agent's accept-time mint exactly; the row id is the same
    # deterministic uuid5 the agent-side coercion would derive from this raw
    # id, so a later executor re-persist upserts onto this row (ON CONFLICT
    # (id)) instead of duplicating the user bubble.
    raw_msg_id = f"msg_{uuid4().hex[:24]}"
    row_id = _coerce_row_id(raw_msg_id)

    async with postgres_db.acquire() as conn:
        async with conn.transaction():
            locked_thread = await conn.fetchrow(
                "SELECT id, user_id, execution_lane, agent_id, status, "
                "       total_turns, metadata, conversation_revision "
                "FROM threads WHERE id = $1 FOR UPDATE",
                thread_id,
            )
            if (
                locked_thread is None
                or str(locked_thread["execution_lane"] or "") != LANE_STATELESS
                or locked_thread["agent_id"] is not None
            ):
                raise HTTPException(
                    status_code=409,
                    detail="Thread is no longer eligible for stateless admission",
                )
            locked_thread_dict = dict(locked_thread)
            current_revision = int(locked_thread_dict.get("conversation_revision") or 0)
            if expected_conversation_revision is None:
                revision_matches = current_revision == 0
            else:
                revision_matches = (
                    int(expected_conversation_revision) == current_revision
                )
            if not revision_matches:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": "session_view_stale",
                        "reason": "conversation_revision_changed",
                        "conversation_revision": current_revision,
                    },
                )
            locked_backend = _require_stateless_workspace(locked_thread_dict)
            locked_status = str(locked_thread["status"] or "")
            if locked_status not in {
                "created",
                "active",
                "awaiting_user",
                "suspended",
            }:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "Thread is not currently accepting stateless input "
                        f"(status={locked_status or 'unknown'})"
                    ),
                )
            needs_workspace_ensure = locked_backend == "sandbox"
            if locked_status == "suspended":
                # Wake and enqueue are one lifecycle transaction. Workspace
                # restore remains a post-commit side effect, but no claimant
                # can observe a runnable queue paired with a still-suspended
                # thread (the claim/credential boundary correctly refuses
                # suspended rows).
                woke = await conn.fetchval(
                    "UPDATE threads SET status = 'created', "
                    "agent_id = NULL, control_admission_agent_id = NULL, "
                    "awaiting_user_since = NULL, extend_count = 0 "
                    "WHERE id = $1::uuid AND execution_lane = 'stateless' "
                    "AND status = 'suspended' RETURNING id",
                    thread_id,
                )
                if woke is None:
                    raise RuntimeError(
                        "stateless suspended-input wake lost thread authority"
                    )
            turn_number = int(locked_thread["total_turns"] or 0) + 1
            fair_key = (
                str(locked_thread["user_id"])
                if locked_thread["user_id"] is not None
                else None
            )
            seq = await conn.fetchval(
                """
                INSERT INTO thread_messages (id, thread_id, role, content, turn_number)
                VALUES ($1, $2, 'human', $3, $4)
                RETURNING seq
                """,
                row_id,
                thread_id,
                content,
                turn_number,
            )
            # Same activity bump the agent's save_thread_message performs.
            await conn.execute(
                """
                UPDATE threads
                SET last_activity = CURRENT_TIMESTAMP,
                    total_turns   = GREATEST(total_turns, COALESCE($2, 0))
                WHERE id = $1
                """,
                thread_id,
                turn_number,
            )
            state = await record_input_seq(
                conn,
                unit_id=thread_id,
                unit_kind=UNIT_KIND_SESSION_TURN,
                input_seq=int(seq),
                fair_key=fair_key,
            )

        if needs_workspace_ensure:
            # Queue admission commits before this side effect. The claimant may
            # arrive first, but its internal workspace poll independently
            # suppresses cached Ready credentials until the exact Pod UID is
            # live. Always schedule sandbox reconciliation: a DB-Ready row can
            # be stale even though its lifecycle string looks terminally good.
            _schedule_stateless_workspace_ensure(thread_id)
        # Post-commit watermark read (same conn): §5.3.1 response parity —
        # queue_depth comes from unconsumed watermarks, not a process queue.
        wm = await queue_depth_for(conn, unit_id=thread_id)
        queue_state = await queue_state_for(conn, unit_id=thread_id)

    queue_depth = 1 if (wm is not None and wm.has_pending_input) else 0
    # Lifecycle block (stateless_turn_resilience.md step 2): the SAME shape
    # /connection and GET …/queue return, so a parked unit is never mistaken
    # for a busy pool by the client.
    lifecycle = queue_block(queue_state, thread.get("metadata"))
    logger.info(
        "run_queue enqueue: thread=%s turn=%d input_seq=%d state=%s",
        thread_id,
        turn_number,
        int(seq),
        state,
    )
    return {
        "accepted": True,
        "turn_id": turn_number,
        "conversation_revision": current_revision,
        "queue": {
            "state": state,
            "queue_depth": queue_depth,
            "message_id": raw_msg_id,
            "input_seq": int(seq),
            "park_reason": lifecycle["park_reason"],
            "parked_at": lifecycle["parked_at"],
            "retryable": lifecycle["retryable"],
            "attempts": lifecycle["attempts"],
            "pending_input": lifecycle["pending_input"],
        },
    }


@app.post("/api/persistent/threads/{thread_id}/input")
async def thread_input(
    thread_id: str, body: ThreadInputRequest, request: Request
) -> dict[str, Any]:
    """Submit user input to a thread. Per-turn lock returns 409 on dupes."""
    from shared.run_queue import LANE_STATELESS

    user = await require_approved_user(request, postgres_db)

    # Stateless-lane admission (stateless_agents.md §5.3.1) resolves BEFORE
    # agent forwarding — queue-lane threads have no bound agent, so
    # _resolve_thread_for_forwarding would 503 on them. Owner gate identical
    # to the resolver's; the pinned path below is untouched (its resolver
    # re-loads the thread and re-applies the same checks).
    lane_thread = await _load_thread_for_owner(thread_id, user)
    if lane_thread.get("execution_lane") == LANE_STATELESS:
        if not body.content or not isinstance(body.content, str):
            raise HTTPException(
                status_code=400, detail="content must be a non-empty string"
            )
        # The per-turn in-process lock below is deliberately SKIPPED on this
        # lane: the run_queue itself serializes turns (input during a leased
        # turn only advances the watermark; one row per unit dedups the
        # queue), and the lock dict is per-process state — replica-unsafe
        # under the 2-replica topology anyway. body.turn_id is ignored: the
        # queue lane derives the turn number from DB truth (total_turns + 1).
        return await _thread_input_stateless(
            lane_thread,
            body.content,
            body.expected_conversation_revision,
        )

    thread, binding = await _resolve_thread_for_forwarding(thread_id, user)

    if not body.content or not isinstance(body.content, str):
        raise HTTPException(
            status_code=400, detail="content must be a non-empty string"
        )

    # Turn id defaults to the thread's current total_turns + 1. Reject
    # arbitrarily-large values to bound the lock dict.
    total_turns = int(thread.get("total_turns") or 0)
    if body.turn_id is None:
        turn_id = total_turns + 1
    else:
        turn_id = body.turn_id
        if turn_id < 0 or turn_id > total_turns + 5:
            raise HTTPException(
                status_code=400,
                detail=f"turn_id out of range "
                f"(thread at turn {total_turns}, max accepted "
                f"{total_turns + 5})",
            )

    lock = _ensure_thread_turn_lock(thread_id, turn_id)
    if lock.locked():
        in_flight = _thread_turn_inflight.get(thread_id, turn_id)
        return JSONResponse(
            status_code=409,
            content={
                "error": "turn_in_flight",
                "turn_id": in_flight,
                "thread_id": thread_id,
            },
        )
    async with lock:
        _thread_turn_inflight[thread_id] = turn_id
        try:
            # Waiting for another tab's turn lock is an authority boundary.
            # Refuse a same-G Pod/attach/endpoint rotation before constructing
            # the HTTP client; _forward_to_agent performs the final reread
            # after client entry as well.
            await _revalidate_pinned_forwarding_binding(binding)
            result = await _forward_to_agent(
                binding,
                "/api/input",
                {"content": body.content, "turn_id": turn_id},
            )
        finally:
            _schedule_turn_lock_cleanup(thread_id, turn_id)
    return {
        "accepted": True,
        "turn_id": turn_id,
        "agent": result,
    }


@app.get("/api/persistent/threads/{thread_id}/queue")
async def thread_queue_state(thread_id: str, request: Request) -> dict[str, Any]:
    """Owner read of the unit's queue lifecycle
    (stateless_turn_resilience.md step 2) — the same ``queue`` block that
    ``/input`` and ``/connection`` carry, for polling while a turn is awaited.
    A thread that never enqueued (pinned lane, or no turn yet) reports
    ``state='none'``.
    """
    from orchestrator.services.stateless_queue_state import queue_block_for_thread

    try:
        UUID(str(thread_id))
    except (ValueError, TypeError):
        raise HTTPException(status_code=404, detail="Thread not found") from None
    _user, thread = await require_thread_owner(request, postgres_db, thread_id)
    async with postgres_db.acquire() as conn:
        block = await queue_block_for_thread(conn, thread)
    return {"thread_id": thread_id, "queue": block}


@app.post("/api/persistent/threads/{thread_id}/queue/retry")
async def thread_queue_retry(thread_id: str, request: Request) -> dict[str, Any]:
    """Owner verb: revive a parked, retryable unit
    (stateless_turn_resilience.md step 2). ``parked`` + retryable →
    ``unpark_unit`` (attempts reset, park_reason cleared) → 200
    ``{state:'queued'}``; 409 ``{code}`` under stop markers / a claim-loss
    hold / a non-retryable reason; 404 when not parked. Audited. The admin
    verb ``POST /api/admin/run-queue/{unit_id}/unpark`` remains the
    operator path for the non-retryable reasons.
    """
    from orchestrator.services.stateless_queue_state import park_retry_refusal
    from shared.run_queue import STATE_PARKED, queue_state_for, unpark_unit

    try:
        UUID(str(thread_id))
    except (ValueError, TypeError):
        raise HTTPException(status_code=404, detail="Thread not found") from None
    user, _thread = await require_thread_owner(request, postgres_db, thread_id)
    async with postgres_db.acquire() as conn:
        async with conn.transaction():
            authority = await conn.fetchrow(
                "SELECT execution_lane, metadata FROM threads "
                "WHERE id = $1::uuid FOR UPDATE",
                thread_id,
            )
            if authority is None:
                raise HTTPException(status_code=404, detail="Thread not found")
            queue_state = await queue_state_for(conn, unit_id=thread_id)
            if queue_state is None or queue_state.get("state") != STATE_PARKED:
                raise HTTPException(status_code=404, detail="Unit is not parked")
            park_reason = queue_state.get("park_reason")
            refusal = park_retry_refusal(park_reason, authority["metadata"])
            if refusal is not None:
                raise HTTPException(
                    status_code=409,
                    detail={"code": refusal, "park_reason": park_reason},
                )
            ok = await unpark_unit(conn, unit_id=thread_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Unit is not parked")
    attempts = int(queue_state.get("attempts") or 0)
    logger.info(
        "run_queue retry (owner): unit=%s park_reason=%s attempts=%d",
        thread_id,
        park_reason,
        attempts,
    )
    await log_security_event(
        postgres_db,
        resource_type="thread",
        event_type="queue_retry",
        user=user,
        resource_id=thread_id,
        detail=f"owner unpark park_reason={park_reason} attempts={attempts}",
        request=request,
    )
    return {
        "thread_id": thread_id,
        "unit_id": thread_id,
        "state": "queued",
        "park_reason": park_reason,
    }


class ThreadInterruptRequest(BaseModel):
    """Optional correlated envelope; an empty body is pinned back-compat."""

    model_config = ConfigDict(extra="forbid")

    client_request_id: UUID | None = None
    target_turn_id: int | None = Field(
        default=None,
        ge=1,
        le=2_147_483_647,
        strict=True,
    )

    @model_validator(mode="after")
    def validate_complete_envelope(self) -> "ThreadInterruptRequest":
        if (self.client_request_id is None) != (self.target_turn_id is None):
            raise ValueError(
                "client_request_id and target_turn_id must be supplied together"
            )
        return self


@app.post(
    "/api/persistent/threads/{thread_id}/interrupt",
    responses={202: {"description": "Stateless interrupt admitted"}},
)
async def thread_interrupt(
    thread_id: str,
    request: Request,
    body: ThreadInterruptRequest | None = None,
) -> Any:
    """Interrupt one exact in-flight turn without exposing its execution lane.

    Pinned sessions retain their direct agent forward. Every forwarded body is
    bound to the exact runtime fingerprint; an otherwise-empty legacy command
    still targets the active turn observed by that runtime. A correlated
    client is forwarded intact so the agent can reject a retry aimed at an
    older turn. Stateless sessions commit an exact-lease request for the
    serving executor and return admission only — that owner applies the verb
    and journals the authoritative ack.
    """
    from shared.run_queue import LANE_STATELESS

    user, lane_thread = await require_thread_owner(request, postgres_db, thread_id)
    correlated = body is not None and body.client_request_id is not None
    if correlated and body is not None and body.target_turn_id is not None:
        try:
            existing = await find_existing_thread_interrupt(
                postgres_db,
                thread_id=thread_id,
                owner_user_id=lane_thread.get("user_id"),
                client_request_id=body.client_request_id,
                target_turn_id=body.target_turn_id,
            )
        except InterruptAdmissionError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        if existing is not None:
            return JSONResponse(
                status_code=202,
                content={
                    "accepted": True,
                    "request_id": str(existing.id),
                    "client_request_id": str(existing.client_request_id),
                    "target_turn_id": existing.target_turn_id,
                    "state": existing.state,
                    "duplicate": True,
                },
            )
    if lane_thread.get("execution_lane") == LANE_STATELESS:
        if not correlated or body is None or body.target_turn_id is None:
            # Stateless interrupt did not exist for legacy clients. Refuse an
            # uncorrelated command rather than letting it strike whichever
            # lease/turn happens to be current.
            raise HTTPException(
                status_code=422,
                detail=(
                    "client_request_id and target_turn_id are required for "
                    "stateless interrupt"
                ),
            )
        try:
            admitted = await admit_thread_interrupt(
                postgres_db,
                thread_id=thread_id,
                owner_user_id=lane_thread.get("user_id"),
                client_request_id=body.client_request_id,
                target_turn_id=body.target_turn_id,
                requested_by=str(user.get("id") or user.get("sub") or "rest_client"),
            )
        except InterruptAdmissionError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

        logger.info(
            "session-interrupt admission: thread=%s turn=%d token=%d duplicate=%s",
            thread_id,
            admitted.target_turn_id,
            admitted.accepted_lease_token,
            admitted.duplicate,
        )
        return JSONResponse(
            status_code=202,
            content={
                "accepted": True,
                "request_id": str(admitted.id),
                "client_request_id": str(admitted.client_request_id),
                "target_turn_id": admitted.target_turn_id,
                "state": admitted.state,
                "duplicate": admitted.duplicate,
            },
        )

    _, binding = await _resolve_thread_for_forwarding(thread_id, user)
    payload: dict[str, Any] = {}
    if correlated and body is not None and body.target_turn_id is not None:
        payload = {
            "client_request_id": str(body.client_request_id),
            "target_turn_id": body.target_turn_id,
        }
    result = await _forward_to_agent(binding, "/api/interrupt", payload)
    return {"accepted": True, "agent": result}


class ThreadApproveRequest(BaseModel):
    """Body for POST /api/persistent/threads/{id}/approve/{approval_id}."""

    decision: str  # "approve" or "deny"


@app.post("/api/persistent/threads/{thread_id}/approve/{approval_id}")
async def thread_approve(
    thread_id: str,
    approval_id: str,
    body: ThreadApproveRequest,
    request: Request,
) -> dict[str, Any]:
    """Resolve a pending permission gate by updating thread_permission_requests
    directly. The DB trigger fires NOTIFY → the agent's LISTEN wakes its
    permission_check. No agent forwarding hop — this endpoint is the
    canonical resolution path for magic-link approvals and MCP clients
    alike. The cockpit WS approve method does the same UPDATE inside the
    agent for back-compat.

    Returns:
        200 — request resolved (status flipped)
        400 — invalid decision
        403 — not thread owner
        404 — approval_id not found, or wrong thread, or no pending request
        409 — request already decided (idempotent re-clicks land here)
    """
    user, thread = await require_thread_owner(request, postgres_db, thread_id)
    decided_by = str(user.get("id") or user.get("sub") or "rest_client")
    outcome = await _decide_permission_request(
        thread_id, approval_id, body.decision, decided_by=decided_by
    )
    await notification_service.resolve_source(
        "permission_request", approval_id, resolved_by=f"user:{decided_by}"
    )
    return outcome


async def _decide_permission_request(
    thread_id: str, approval_id: str, decision: str, *, decided_by: str
) -> dict[str, Any]:
    """The one UPDATE that decides a permission gate — shared by the REST
    endpoint and the notification's approve/deny actions. Raises the
    endpoint's HTTP errors: 400 bad decision, 404 unknown, 409 decided."""
    if decision == "approve":
        new_status = "approved"
    elif decision == "deny":
        new_status = "denied"
    else:
        raise HTTPException(
            status_code=400,
            detail="decision must be 'approve' or 'deny'",
        )

    async with postgres_db.acquire() as conn:
        # Lookup-then-update so we can distinguish 404 (wrong id/thread)
        # from 409 (already decided).
        existing = await conn.fetchrow(
            "SELECT id, status, tool_call_id FROM thread_permission_requests "
            "WHERE id = $1 AND thread_id = $2",
            approval_id,
            thread_id,
        )
        if existing is None:
            raise HTTPException(
                status_code=404,
                detail="Permission request not found for this thread",
            )
        if existing["status"] != "pending":
            raise HTTPException(
                status_code=409,
                detail=f"Already {existing['status']}",
            )
        row = await conn.fetchrow(
            "UPDATE thread_permission_requests "
            "SET status = $2, decided_at = now(), decided_by = $3 "
            "WHERE id = $1 AND status = 'pending' "
            "RETURNING id, status, tool_call_id",
            approval_id,
            new_status,
            decided_by,
        )
    if row is None:
        # Lost the race — somebody else just decided this. Idempotency.
        raise HTTPException(
            status_code=409,
            detail="Already decided (race lost)",
        )
    return {
        "accepted": True,
        "decision": decision,
        "approval_id": str(row["id"]),
        "status": row["status"],
        "tool_call_id": row["tool_call_id"],
    }


async def thread_events_prune_sweeper(
    shutdown_event: asyncio.Event,
) -> None:
    """Background task that prunes the thread_events log on retention.

    Runs every THREAD_EVENTS_PRUNE_INTERVAL_S (default 300s). Two queries:
      - DELETE rows for threads in 'ended' status older than 24h.
      - DELETE rows for threads NOT in 'ended' older than 7 days.
      - Preserve any event that is still the only durable receipt for a
        pending session-control or exact-turn interrupt request; crash
        recovery terminalizes it first.

    Best-effort. Survives transient DB errors by logging and continuing.
    """
    interval_s = int(os.environ.get("THREAD_EVENTS_PRUNE_INTERVAL_S", "300"))
    logger.info("Thread-events prune sweeper started (interval=%ds)", interval_s)
    while not shutdown_event.is_set():
        try:
            async with postgres_db.acquire() as conn:
                ended_deleted = await conn.fetchval(
                    "WITH deleted AS ("
                    "  DELETE FROM thread_events "
                    "  WHERE thread_id IN ("
                    "    SELECT id FROM threads WHERE status = 'ended'"
                    "  ) "
                    "  AND created_at < now() - interval '24 hours' "
                    "  AND NOT EXISTS ("
                    "    SELECT 1 FROM thread_control_requests request "
                    "    WHERE request.id = thread_events.control_request_id "
                    "      AND request.outcome IS NULL"
                    "  ) "
                    "  AND NOT EXISTS ("
                    "    SELECT 1 FROM thread_interrupt_requests request "
                    "    WHERE request.id = thread_events.interrupt_request_id "
                    "      AND (request.outcome IS NULL "
                    "           OR (request.outcome = 'applied' "
                    "               AND NOT (COALESCE(request.result, '{}'::jsonb) "
                    "                        ? 'consumed_input_seq')))"
                    "  ) "
                    "  RETURNING 1"
                    ") SELECT COUNT(*) FROM deleted"
                )
                active_deleted = await conn.fetchval(
                    "WITH deleted AS ("
                    "  DELETE FROM thread_events "
                    "  WHERE thread_id IN ("
                    "    SELECT id FROM threads WHERE status <> 'ended'"
                    "  ) "
                    "  AND created_at < now() - interval '7 days' "
                    "  AND NOT EXISTS ("
                    "    SELECT 1 FROM thread_control_requests request "
                    "    WHERE request.id = thread_events.control_request_id "
                    "      AND request.outcome IS NULL"
                    "  ) "
                    "  AND NOT EXISTS ("
                    "    SELECT 1 FROM thread_interrupt_requests request "
                    "    WHERE request.id = thread_events.interrupt_request_id "
                    "      AND (request.outcome IS NULL "
                    "           OR (request.outcome = 'applied' "
                    "               AND NOT (COALESCE(request.result, '{}'::jsonb) "
                    "                        ? 'consumed_input_seq')))"
                    "  ) "
                    "  RETURNING 1"
                    ") SELECT COUNT(*) FROM deleted"
                )
            if (ended_deleted or 0) + (active_deleted or 0) > 0:
                logger.info(
                    "thread_events prune: ended=%d active=%d",
                    int(ended_deleted or 0),
                    int(active_deleted or 0),
                )
        except Exception as e:
            logger.warning("thread_events prune error (non-fatal): %s", e)
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=float(interval_s))
            break
        except asyncio.TimeoutError:
            pass
    logger.info("Thread-events prune sweeper stopped")


async def security_events_prune_sweeper(shutdown_event: asyncio.Event) -> None:
    """Background task that prunes the security_events audit log on retention.

    Runs hourly (SECURITY_EVENTS_PRUNE_INTERVAL_S, default 3600). Deletes
    rows older than SECURITY_EVENTS_RETENTION_DAYS (default 90). Bounds
    table growth — writes happen on the post-auth 403 path, so any flood
    is tied to a real account, but retention still caps the worst case.
    Best-effort: survives transient DB errors by logging and continuing.
    """
    interval_s = int(os.environ.get("SECURITY_EVENTS_PRUNE_INTERVAL_S", "3600"))
    retention_days = int(os.environ.get("SECURITY_EVENTS_RETENTION_DAYS", "90"))
    logger.info(
        "Security-events prune sweeper started (interval=%ds, retention=%dd)",
        interval_s,
        retention_days,
    )
    while not shutdown_event.is_set():
        try:
            deleted = await postgres_db.prune_security_events(retention_days)
            if deleted:
                logger.info("security_events prune: deleted=%d", deleted)
        except Exception as e:
            logger.warning("security_events prune error (non-fatal): %s", e)
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=float(interval_s))
            break
        except asyncio.TimeoutError:
            pass
    logger.info("Security-events prune sweeper stopped")


async def ssh_attachments_prune_sweeper(shutdown_event: asyncio.Event) -> None:
    """Background task that prunes the ssh_attachments audit log on retention.

    Runs hourly (SSH_ATTACHMENTS_PRUNE_INTERVAL_S, default 3600). Deletes
    rows older than SSH_ATTACHMENTS_RETENTION_DAYS (default 90). thread_id
    on this table is ON DELETE SET NULL rather than CASCADE (see 0204's
    header), so ending a session no longer prunes its attach history —
    this sweeper is what bounds the table's growth instead.

    Not leader-gated, matching security_events_prune_task: a delete-by-age
    is idempotent, so two replicas racing it is harmless — the second finds
    nothing. Best-effort: survives transient DB errors by logging and
    continuing.
    """
    interval_s = int(os.environ.get("SSH_ATTACHMENTS_PRUNE_INTERVAL_S", "3600"))
    retention_days = int(os.environ.get("SSH_ATTACHMENTS_RETENTION_DAYS", "90"))
    logger.info(
        "SSH-attachments prune sweeper started (interval=%ds, retention=%dd)",
        interval_s,
        retention_days,
    )
    while not shutdown_event.is_set():
        try:
            deleted = await postgres_db.prune_ssh_attachments(retention_days)
            if deleted:
                logger.info("ssh_attachments prune: deleted=%d", deleted)
        except Exception as e:
            logger.warning("ssh_attachments prune error (non-fatal): %s", e)
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=float(interval_s))
            break
        except asyncio.TimeoutError:
            pass
    logger.info("SSH-attachments prune sweeper stopped")


# =============================================================================
# Headless persistent sessions — Phase 4 magic-link routes + watcher
# =============================================================================
#
# Email magic-links land at /magic/approve/{token}. GET renders a
# confirmation page (read-only, prefetch-safe). POST consumes the token
# and UPDATEs thread_permission_requests via the same trigger path as
# the cockpit WS approve handler.
#
# Background watcher (thread_permission_notify_sweeper) detects pending
# requests older than 30s with no notification on record and dispatches
# the email via services.headless_notifications.


# Phase 5: per-thread cap on /magic/extend clicks. 4 × 60min = 4h total
# awaiting_user before unconditional suspension. Configurable via env for
# ops tuning during incident response.
_MAGIC_EXTEND_CAP: int = int(os.environ.get("HEADLESS_EXTEND_CAP", "4"))


def _magic_link_confirmation_page(
    *,
    tool_name: str,
    tool_args_preview: str,
    intended_decision: Optional[str],
    token: str,
    extend_status: Optional[str] = None,
    extends_remaining: Optional[int] = None,
) -> str:
    """Render the GET landing page. Single button POSTs back to the same
    URL with the actual decision; this is what prevents email-link
    prefetchers (Outlook Safe Links, Gmail) from auto-consuming tokens.

    Phase 5: a second form lets the user POST /magic/extend/{token} to
    bump the attention-sleep clock by 60 min without consuming the
    approval token. extend_status (when set) drives an inline toast:
    'extended' on success, 'cap_reached' when extend_count >= cap,
    'not_awaiting' when the thread is no longer in awaiting_user.
    """
    # Both values come from the agent's pending tool call and land in element
    # content; the token below lands in an attribute. html.escape(quote=True)
    # covers & < > " ' in one pass — the hand-rolled chains here missed ">" on
    # the tool name and the quotes on both, which is the reflected-XSS hole.
    safe_args = html.escape(tool_args_preview, quote=True)
    safe_tool = html.escape(tool_name, quote=True)
    if intended_decision == "approved":
        button_label = "Confirm: Approve"
        button_color = _BRAND["success"]
    elif intended_decision == "denied":
        button_label = "Confirm: Deny"
        button_color = _BRAND["danger"]
    else:
        button_label = "Confirm decision"
        button_color = _BRAND["accent-color"]

    # The token lands in a form ``action`` attribute. Percent-encoding already
    # removes every character that could close the attribute; escaping the
    # result as well is a no-op on that output but keeps the sanitizer
    # explicit at the sink rather than inferred from the encoder.
    quoted_token = html.escape(urllib.parse.quote(token, safe=""), quote=True)

    # Extend banner copy — friendly, action-specific.
    extend_banner_html = ""
    if extend_status == "extended":
        remaining_str = (
            f" — {extends_remaining} extends remaining"
            if extends_remaining is not None
            else ""
        )
        extend_banner_html = (
            f'<div style="background: {_BRAND["surface-0"]}; border: 1px solid {_BRAND["success"]}; '
            "padding: 10px 12px; margin: 0 0 12px 0; "
            f'color: {_BRAND["success"]}; font-size: 13px;">Window extended by 60 minutes'
            f"{remaining_str}.</div>"
        )
    elif extend_status == "cap_reached":
        extend_banner_html = (
            f'<div style="background: {_BRAND["surface-0"]}; border: 1px solid {_BRAND["text-secondary"]}; '
            "padding: 10px 12px; margin: 0 0 12px 0; "
            f'color: {_BRAND["text-secondary"]}; font-size: 13px;">Extend limit reached — please '
            "approve, deny, or open the cockpit.</div>"
        )
    elif extend_status == "not_awaiting":
        extend_banner_html = (
            f'<div style="background: {_BRAND["surface-0"]}; border: 1px solid {_BRAND["accent-color"]}; '
            "padding: 10px 12px; margin: 0 0 12px 0; "
            f'color: {_BRAND["accent-color"]}; font-size: 13px;">No extend needed — the agent '
            "is already active.</div>"
        )

    # Disable the extend button if we already know the cap was hit.
    #
    # The disabled look MUST be merged into the button's own style attribute.
    # HTML keeps the FIRST style= on an element and ignores every later one,
    # so emitting a second one meant the cap_reached branch -- and only that
    # branch -- rendered a button with opacity/cursor and none of the brand
    # colours, border or type scale.
    _extend_cap_reached = extend_status == "cap_reached"
    extend_disabled_attr = " disabled" if _extend_cap_reached else ""
    extend_button_style = (
        f"background: transparent; color: {_BRAND['accent-color']}; "
        f"padding: 10px 20px; border: 1px solid {_BRAND['accent-color']}; "
        f"font-weight: 600; font-size: 14px; "
        + (
            "opacity: 0.5; cursor: not-allowed;"
            if _extend_cap_reached
            else "cursor: pointer;"
        )
    )

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>SRW — Confirm Decision</title></head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: {_BRAND["app-bg"]}; color: {_BRAND["text-primary"]}; padding: 40px 20px;">
  <div style="max-width: 600px; margin: 0 auto; background: {_BRAND["panel-bg"]}; border: 1px solid {_BRAND["border-color"]}; overflow: hidden;">
    <div style="background: {_BRAND["surface-0"]}; padding: 16px 20px; border-bottom: 1px solid {_BRAND["border-color"]};">
      <h2 style="margin: 0; color: {_BRAND["accent-color"]}; font-size: 16px;">Confirm tool decision</h2>
    </div>
    <div style="padding: 20px; font-size: 14px; line-height: 1.6;">
      {extend_banner_html}
      <p>The agent wants to call <code style="background: {_BRAND["surface-0"]}; padding: 2px 6px;">{safe_tool}</code> with these arguments:</p>
      <pre style="background: {_BRAND["surface-0"]}; padding: 12px; overflow-x: auto; font-size: 12px; color: {_BRAND["success"]};">{safe_args}</pre>
    </div>
    <div style="background: {_BRAND["surface-0"]}; padding: 16px 20px; border-top: 1px solid {_BRAND["border-color"]}; text-align: center;">
      <form method="POST" action="/magic/approve/{quoted_token}" style="display: inline;">
        <button type="submit" style="background: {button_color}; color: {_BRAND["on-accent"]}; padding: 10px 28px; border: 0; cursor: pointer; font-weight: 600; font-size: 14px;">{button_label}</button>
      </form>
      <form method="POST" action="/magic/extend/{quoted_token}" style="display: inline; margin-left: 8px;">
        <button type="submit"{extend_disabled_attr} style="{extend_button_style}">I'm reviewing — extend 60min</button>
      </form>
      <p style="margin: 16px 0 0 0; color: {_BRAND["text-secondary"]}; font-size: 12px;">Approve link is single-use and expires in 30 minutes.</p>
    </div>
  </div>
</body></html>"""


def _magic_link_result_page(
    *,
    title: str,
    body: str,
    cockpit_url: str,
    is_error: bool = False,
) -> str:
    accent = _BRAND["danger"] if is_error else _BRAND["success"]
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>SRW — {title}</title></head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: {_BRAND["app-bg"]}; color: {_BRAND["text-primary"]}; padding: 40px 20px;">
  <div style="max-width: 600px; margin: 0 auto; background: {_BRAND["panel-bg"]}; border: 1px solid {_BRAND["border-color"]}; overflow: hidden;">
    <div style="background: {_BRAND["surface-0"]}; padding: 16px 20px; border-bottom: 1px solid {_BRAND["border-color"]};">
      <h2 style="margin: 0; color: {accent}; font-size: 16px;">{title}</h2>
    </div>
    <div style="padding: 20px; font-size: 14px; line-height: 1.6;">
      <p>{body}</p>
      <p style="margin-top: 16px;"><a href="{cockpit_url}" style="color: {_BRAND["accent-color"]};">Open the cockpit</a></p>
    </div>
  </div>
</body></html>"""


@app.get("/magic/approve/{token}")
async def magic_link_get(token: str) -> HTMLResponse:
    """Show a confirmation page for the magic-link token.

    Does NOT consume the token (POST does). This separation is critical:
    email link previewers (Outlook Safe Links, Gmail) auto-fetch URLs
    server-side; a GET-executes link would be consumed by a bot before
    the human ever clicks.
    """
    cockpit_external_url = email_service.cockpit_url or "http://localhost:4200"

    row = await headless_notifications.validate_magic_link(postgres_db, token)
    if row is None:
        return HTMLResponse(
            _magic_link_result_page(
                title="Link expired or already used",
                body=(
                    "This approval link is no longer valid. It may have "
                    "expired, been used already, or been invalidated by a "
                    "newer approval. Open the cockpit to see the current "
                    "state."
                ),
                cockpit_url=cockpit_external_url,
                is_error=True,
            ),
            status_code=404,
        )

    # Fetch tool details for the confirmation page.
    async with postgres_db.acquire() as conn:
        permission_row = await conn.fetchrow(
            "SELECT id, tool_name, tool_args, status "
            "FROM thread_permission_requests WHERE id = $1",
            row["approval_id"],
        )

    if permission_row is None or permission_row["status"] != "pending":
        return HTMLResponse(
            _magic_link_result_page(
                title="Already decided",
                body=(
                    "The agent's request has already been resolved. No "
                    "further action is needed."
                ),
                cockpit_url=cockpit_external_url,
            ),
            status_code=409,
        )

    tool_args = permission_row["tool_args"]
    if isinstance(tool_args, str):
        try:
            tool_args = json.loads(tool_args)
        except Exception:
            tool_args = {}
    elif tool_args is None:
        tool_args = {}
    args_preview = json.dumps(tool_args, indent=2, default=str)
    if len(args_preview) > 600:
        args_preview = args_preview[:600] + "\n… (truncated)"

    page = _magic_link_confirmation_page(
        tool_name=permission_row["tool_name"],
        tool_args_preview=args_preview,
        intended_decision=row.get("intended_decision"),
        token=token,
    )
    return HTMLResponse(page)


@app.post("/magic/approve/{token}")
async def magic_link_post(token: str) -> HTMLResponse:
    """Consume the token and resolve the permission request.

    CAS UPDATE on magic_link_tokens (single-use) + a second UPDATE on
    thread_permission_requests (which the agent's LISTEN picks up via
    the existing trigger). Distinguishes 404 (invalid) from 409 (token
    already used or request already decided) for clean UX on double-clicks.
    """
    cockpit_external_url = email_service.cockpit_url or "http://localhost:4200"

    row = await headless_notifications.validate_magic_link(postgres_db, token)
    if row is None:
        return HTMLResponse(
            _magic_link_result_page(
                title="Link expired or already used",
                body=(
                    "This approval link is no longer valid. It may have "
                    "expired or been used already."
                ),
                cockpit_url=cockpit_external_url,
                is_error=True,
            ),
            status_code=404,
        )

    decision = row.get("intended_decision") or "approved"

    consumed = await headless_notifications.consume_magic_link(
        postgres_db, str(row["id"]), decision
    )
    if consumed is None:
        return HTMLResponse(
            _magic_link_result_page(
                title="Already used",
                body=(
                    "This link has already been used. The agent's request "
                    "is being processed."
                ),
                cockpit_url=cockpit_external_url,
            ),
            status_code=409,
        )

    # Resolve the permission request. CAS-style UPDATE so we don't race
    # with the cockpit having already decided it.
    decided_by_label = "magic_link"
    if consumed.get("user_id"):
        decided_by_label = f"user:{consumed['user_id']}"
    async with postgres_db.acquire() as conn:
        permission_row = await conn.fetchrow(
            "UPDATE thread_permission_requests "
            "SET status = $2, decided_at = now(), decided_by = $3 "
            "WHERE id = $1 AND status = 'pending' "
            "RETURNING id, status, tool_call_id, tool_name, thread_id",
            consumed["approval_id"],
            decision,
            decided_by_label,
        )

    if permission_row is None:
        return HTMLResponse(
            _magic_link_result_page(
                title="Already decided",
                body=(
                    "The agent's request was already resolved by another "
                    "approval path (cockpit click, REST, or expired). "
                    "Your action was not needed."
                ),
                cockpit_url=cockpit_external_url,
            ),
            status_code=409,
        )

    # Phase 5: if attention sleep fired since the email was sent, wake through
    # the thread's existing execution plane. Pinned sessions retain workspace
    # restore + agent-pod re-creation. Stateless sessions retain their exact
    # queued/leased turn and converge the workspace without binding a pod. The
    # permission-row id is the wake task's freshness fence.
    asyncio.create_task(
        _phase5_wake_if_suspended(
            str(permission_row["thread_id"]),
            permission_request_id=str(permission_row["id"]),
        ),
        name=f"phase5-wake-{str(permission_row['thread_id'])[:8]}",
    )

    pretty = "approved" if decision == "approved" else "denied"
    return HTMLResponse(
        _magic_link_result_page(
            title=f"Tool {pretty}",
            body=(
                f"The agent's request to call "
                f"<code>{permission_row['tool_name']}</code> has been "
                f"{pretty}. The agent will resume shortly."
            ),
            cockpit_url=cockpit_external_url,
        )
    )


@app.post("/magic/extend/{token}")
async def magic_link_extend(token: str) -> HTMLResponse:
    """Extend the attention-sleep window for the thread bound to this token.

    Validates the token (same hash + expiry + single-use checks as
    /magic/approve) but does NOT consume it — the user is signaling
    "I'm still reviewing" without making the approve decision. Bumps
    threads.awaiting_user_since forward by 60 minutes per click, capped
    at HEADLESS_EXTEND_CAP (default 4 = 4h total ceiling).

    Re-renders the confirmation page with a toast so the user can still
    click approve/deny on the same screen. Status_code 200 throughout —
    the page itself carries the success/cap/not-awaiting signal.

    Why a separate route and not "extend ↔ approve same POST": the
    approve handler consumes the token (single-use CAS). If extend
    shared that path, every extend click would burn the approval token
    and the user couldn't approve afterward.
    """
    cockpit_external_url = email_service.cockpit_url or "http://localhost:4200"

    row = await headless_notifications.validate_magic_link(postgres_db, token)
    if row is None:
        return HTMLResponse(
            _magic_link_result_page(
                title="Link expired or already used",
                body=(
                    "This link is no longer valid. Open the cockpit to "
                    "review the agent's current state."
                ),
                cockpit_url=cockpit_external_url,
                is_error=True,
            ),
            status_code=404,
        )

    thread_id = row.get("thread_id")
    if thread_id is None:
        return HTMLResponse(
            _magic_link_result_page(
                title="Cannot extend",
                body="This link is not bound to a thread.",
                cockpit_url=cockpit_external_url,
                is_error=True,
            ),
            status_code=400,
        )

    # Bump awaiting_user_since iff the thread is still in awaiting_user
    # and extend_count < cap. The CAS UPDATE returns the new row state so
    # we can show the right banner. status='active' or 'suspended' means
    # there's nothing to extend — the agent has either woken up already
    # or moved beyond awaiting_user.
    async with postgres_db.acquire() as conn:
        updated = await conn.fetchrow(
            "UPDATE threads "
            "SET awaiting_user_since = now(), "
            "    extend_count = extend_count + 1 "
            "WHERE id = $1 "
            "  AND status = 'awaiting_user' "
            "  AND extend_count < $2 "
            "RETURNING extend_count",
            str(thread_id),
            _MAGIC_EXTEND_CAP,
        )

    if updated is None:
        # Distinguish cap_reached from not_awaiting for the banner copy.
        async with postgres_db.acquire() as conn:
            row_state = await conn.fetchrow(
                "SELECT status, extend_count FROM threads WHERE id = $1",
                str(thread_id),
            )
        if row_state is None:
            extend_status = "not_awaiting"
        elif row_state["status"] != "awaiting_user":
            extend_status = "not_awaiting"
        elif row_state["extend_count"] >= _MAGIC_EXTEND_CAP:
            extend_status = "cap_reached"
        else:
            # Edge case — concurrent change between our UPDATE and SELECT.
            # Render not_awaiting which is the gentler banner.
            extend_status = "not_awaiting"
        extends_remaining = None
    else:
        extend_status = "extended"
        extends_remaining = max(0, _MAGIC_EXTEND_CAP - int(updated["extend_count"]))

    # Re-render the confirmation page with the banner. Load the permission
    # row again (status may have changed underneath us).
    approval_id = row.get("approval_id")
    if approval_id is not None:
        async with postgres_db.acquire() as conn:
            permission_row = await conn.fetchrow(
                "SELECT tool_name, tool_args, status FROM "
                "thread_permission_requests WHERE id = $1",
                approval_id,
            )
    else:
        permission_row = None

    if permission_row is None or permission_row["status"] != "pending":
        return HTMLResponse(
            _magic_link_result_page(
                title="Already decided",
                body=(
                    "The agent's request has been resolved. No further "
                    "action is needed."
                ),
                cockpit_url=cockpit_external_url,
            ),
            status_code=200,
        )

    tool_args = permission_row["tool_args"]
    if isinstance(tool_args, str):
        try:
            tool_args = json.loads(tool_args)
        except Exception:
            tool_args = {}
    elif tool_args is None:
        tool_args = {}
    args_preview = json.dumps(tool_args, indent=2, default=str)
    if len(args_preview) > 600:
        args_preview = args_preview[:600] + "\n… (truncated)"

    page = _magic_link_confirmation_page(
        tool_name=permission_row["tool_name"],
        tool_args_preview=args_preview,
        intended_decision=row.get("intended_decision"),
        token=token,
        extend_status=extend_status,
        extends_remaining=extends_remaining,
    )
    return HTMLResponse(page)


async def _phase5_wake_stateless_if_suspended(
    thread_id: str,
    *,
    permission_request_id: str | None,
) -> None:
    """Wake one queue-served permission continuation without binding a pod.

    A magic-link task can run well after its originating request was resolved.
    Revalidate every authority under the global ``threads -> run_queue`` lock
    order: exact stateless lane/class/tier, no pinned-agent binding, the exact
    terminal permission row, and a queued/leased session turn whose human input
    is still unconsumed.  ``done`` is deliberately not revived: no durable
    permission-continuation watermark exists yet, so a done row would hit the
    executor's skip-if-answered edge and falsely claim the tool resumed.

    The queue row itself is left untouched.  A live lease keeps ownership; a
    queued retry keeps its token/fairness/affinity.  Workspace convergence uses
    the owner-keyed session provisioner, which restores a Kubernetes sandbox,
    refreshes a virtual binding, and is a no-op for ``none``.  It never creates
    a persistent agent pod.
    """
    from shared.run_queue import (
        LANE_STATELESS,
        STATE_LEASED,
        STATE_QUEUED,
        UNIT_KIND_SESSION_TURN,
    )

    if permission_request_id is None:
        logger.warning(
            "magic-link wake: refusing unfenced stateless wake for thread %s",
            thread_id,
        )
        return

    should_ensure_workspace = False
    async with postgres_db.acquire() as conn:
        async with conn.transaction():
            locked_thread = await conn.fetchrow(
                "SELECT id, execution_lane, agent_id, status, metadata "
                "FROM threads WHERE id = $1::uuid FOR UPDATE",
                thread_id,
            )
            if locked_thread is None:
                return
            thread = dict(locked_thread)
            if (
                thread.get("execution_lane") != LANE_STATELESS
                or thread.get("agent_id") is not None
            ):
                logger.warning(
                    "magic-link wake: stateless authority moved for thread %s "
                    "(lane=%r agent_id=%r)",
                    thread_id,
                    thread.get("execution_lane"),
                    thread.get("agent_id"),
                )
                return
            try:
                _require_stateless_workspace(thread)
            except HTTPException as exc:
                logger.warning(
                    "magic-link wake: refusing stateless workspace/class for "
                    "thread %s: %s",
                    thread_id,
                    exc.detail,
                )
                return

            # Keep the repository-wide threads -> run_queue lock order.  The
            # lock makes the pending-input test atomic with a concurrent claim,
            # completion, release or reaper steal.
            queue = await conn.fetchrow(
                "SELECT state, input_seq, consumed_seq "
                "FROM run_queue "
                "WHERE unit_id = $1::uuid AND unit_kind = $2 "
                "FOR UPDATE",
                thread_id,
                UNIT_KIND_SESSION_TURN,
            )
            if queue is None:
                logger.warning(
                    "magic-link wake: no session queue authority for thread %s",
                    thread_id,
                )
                return
            queue_state = str(queue["state"] or "")
            input_seq = queue["input_seq"]
            consumed_seq = queue["consumed_seq"]
            has_unconsumed_input = input_seq is not None and (
                consumed_seq is None or int(input_seq) > int(consumed_seq)
            )
            if (
                queue_state not in {STATE_QUEUED, STATE_LEASED}
                or not has_unconsumed_input
            ):
                logger.warning(
                    "magic-link wake: refusing stale stateless continuation for "
                    "thread %s (queue_state=%s input_seq=%r consumed_seq=%r)",
                    thread_id,
                    queue_state,
                    input_seq,
                    consumed_seq,
                )
                return

            decision = await conn.fetchval(
                "SELECT status FROM thread_permission_requests "
                "WHERE id = $2::uuid AND thread_id = $1::uuid "
                "  AND status IN ('approved', 'denied')",
                thread_id,
                permission_request_id,
            )
            if decision not in {"approved", "denied"}:
                logger.warning(
                    "magic-link wake: exact permission fence rejected thread %s "
                    "request %s",
                    thread_id,
                    permission_request_id,
                )
                return

            thread_status = str(thread.get("status") or "")
            if thread_status not in {"active", "awaiting_user", "suspended"}:
                logger.warning(
                    "magic-link wake: thread %s is not resumable (status=%r)",
                    thread_id,
                    thread_status,
                )
                return

            if thread_status in {"awaiting_user", "suspended"}:
                updated = await conn.fetchval(
                    "UPDATE threads "
                    "SET status = 'active', "
                    "    awaiting_user_since = NULL, "
                    "    extend_count = 0, "
                    "    control_admission_agent_id = NULL "
                    "WHERE id = $1::uuid "
                    "  AND execution_lane = $2 "
                    "  AND agent_id IS NULL "
                    "  AND status IN ('suspended', 'awaiting_user') "
                    "RETURNING id",
                    thread_id,
                    LANE_STATELESS,
                )
                if updated is None:
                    return
            should_ensure_workspace = True

    if not should_ensure_workspace:
        return
    # Queue/lifecycle admission commits before this potentially slow side
    # effect.  A claimant may arrive first, but its attach path polls the same
    # durable workspace lifecycle until it is ready.
    await ensure_session_workspace(
        thread_id,
        db=postgres_db,
        provisioner=container_provisioner,
        suspension=workspace_suspension_service,
    )
    logger.info(
        "magic-link wake: stateless permission continuation admitted for "
        "thread %s request %s",
        thread_id,
        permission_request_id,
    )


async def _phase5_wake_if_suspended(
    thread_id: str,
    *,
    permission_request_id: str | None = None,
) -> None:
    """Wake a suspended thread after a magic-link decision.

    Fire-and-forget — the HTTP response has already returned. Stateless
    sessions delegate to the queue-fenced, topology-neutral helper above.
    Pinned sessions preserve the historical resume pattern: restore from S3,
    then spawn the agent pod if the persistent provisioner is wired.
    """
    try:
        thread = await postgres_db.get_thread(thread_id)
        if not thread:
            return
        if thread.get("execution_lane") == "stateless":
            await _phase5_wake_stateless_if_suspended(
                thread_id,
                permission_request_id=permission_request_id,
            )
            return
        if not _thread_uses_pinned_execution(thread):
            logger.warning(
                "magic-link wake: refusing pinned wake for thread %s on "
                "execution lane %r",
                thread_id,
                thread.get("execution_lane"),
            )
            return
        wake_authority = thread_runtime_authority(thread)
        if wake_authority is None:
            return
        metadata = thread.get("metadata") or {}
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except (json.JSONDecodeError, TypeError):
                metadata = {}
        recycle = read_recycle_record(metadata)
        if isinstance(recycle, dict) and recycle.get("phase") not in {
            None,
            "",
            "complete",
            "cancelled",
        }:
            if _persistent_thread_recycler is not None:
                await _persistent_thread_recycler.request_and_reconcile(
                    thread_id=thread_id,
                    reason="resume_during_recycle",
                    expected_build_sha=persistent_provisioner.expected_build_sha,
                    expected_project_id=(
                        str(thread.get("project_id"))
                        if thread.get("project_id")
                        else None
                    ),
                )
            return
        ws_ctx = metadata.get("workspace_container") or {}
        ws_status = ws_ctx.get("status")
        if ws_status == "suspended" and workspace_suspension_service.is_enabled:
            logger.info(
                "magic-link wake: restoring suspended workspace for thread %s",
                thread_id,
            )
            restored = await ensure_session_workspace(
                thread_id,
                db=postgres_db,
                provisioner=container_provisioner,
                suspension=workspace_suspension_service,
                expected_runtime_generation=wake_authority.generation,
            )
            if restored is None or restored.outcome is EnsureOutcome.FAILED:
                logger.warning(
                    "magic-link wake: workspace restore failed or lost authority "
                    "for thread %s",
                    thread_id,
                )
                return

        # Publish wake only to the exact post-suspension generation. A G2
        # restore delayed across another End/Resume cannot wake G3.
        async with postgres_db.acquire() as conn:
            woke = await conn.fetchval(
                "UPDATE threads "
                "SET status = 'active', "
                "    awaiting_user_since = NULL, "
                "    extend_count = 0, "
                "    control_admission_agent_id = NULL "
                "WHERE id = $1::uuid "
                "  AND execution_lane='pinned' "
                "  AND runtime_generation=$2::uuid "
                "  AND runtime_retirement_token IS NULL "
                "  AND status IN ('suspended', 'awaiting_user') "
                "RETURNING id",
                thread_id,
                wake_authority.generation,
            )
        if woke is None and not same_thread_runtime_authority(
            await postgres_db.get_thread(thread_id), wake_authority
        ):
            return

        # Agent pod may also have been deleted on suspension
        # (workspace_suspension.py:502-504). Re-provision if a persistent
        # provisioner is configured. fire-and-forget — the agent's boot
        # will restore the LangGraph checkpoint and re-enter permission_check
        # for the same tool_call_id, where the select-first guard picks up
        # the decision we just UPDATEd.
        current = await postgres_db.get_thread(thread_id)
        if not same_thread_runtime_authority(current, wake_authority):
            return
        if persistent_provisioner is not None and not current.get("agent_id"):
            config_name = canonical_config_name(
                thread.get("config_name", "session_base")
            )

            async def _create_after_magic_link() -> None:
                # This closure sits lexically inside the wake handler's
                # try/except, but it is scheduled as its own task — so that
                # handler NEVER sees anything raised here. Its own guard is the
                # only thing between a raise and a silently vanished wake.
                try:
                    result = await persistent_provisioner.create_agent_pod(
                        thread_id,
                        config_name=config_name,
                        expected_runtime_generation=wake_authority.generation,
                    )
                    if not result.usable:
                        logger.warning(
                            "magic-link persistent provisioning for thread %s "
                            "is %s (%s)",
                            thread_id,
                            result.status.value,
                            result.failure_class or "no-detail",
                        )
                        await _emit_session_provisioning_failure(
                            thread_id,
                            str(thread.get("user_id") or "") or None,
                            wake_authority,
                            f"magic-link wake provisioning {result.status.value}"
                            f" ({result.failure_class or 'no-detail'})",
                        )
                except Exception as exc:
                    logger.exception(
                        "magic-link persistent provisioning for thread %s raised: %s",
                        thread_id,
                        exc,
                    )
                    await _emit_session_provisioning_failure(
                        thread_id,
                        str(thread.get("user_id") or "") or None,
                        wake_authority,
                        str(exc),
                    )

            asyncio.create_task(
                _create_after_magic_link(),
                name=f"phase5-create-agent-{thread_id[:8]}",
            )
    except Exception as e:
        logger.warning(
            "magic-link wake task failed for thread %s: %s",
            thread_id,
            e,
        )


async def thread_permission_notify_sweeper(
    shutdown_event: asyncio.Event,
) -> None:
    """Background task: a permission request that has waited longer than
    HEADLESS_NOTIFY_AGE_S without a decision becomes a ``session_permission``
    feed row for the thread owner — ``high``, so the mail (with the two magic
    links) goes out now, and the row resolves when the gate is decided by any
    path. In-session gates are answered within seconds through the agent's
    LISTEN, so only abandoned ones ever get here.

    Runs every HEADLESS_NOTIFY_INTERVAL_S (default 30s). Idempotent: the
    feed row is keyed on the request id, and rows already recorded are
    filtered out so the magic-link tokens are minted once.

    Best-effort. Survives transient errors by logging and continuing.
    """
    interval_s = int(os.environ.get("HEADLESS_NOTIFY_INTERVAL_S", "30"))
    age_threshold_s = int(os.environ.get("HEADLESS_NOTIFY_AGE_S", "30"))
    logger.info(
        "Headless permission-notify sweeper started (interval=%ds, age_threshold=%ds)",
        interval_s,
        age_threshold_s,
    )
    cockpit_external_url = email_service.cockpit_url or "http://localhost:4200"

    while not shutdown_event.is_set():
        try:
            async with postgres_db.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT r.id, r.thread_id, r.tool_name, r.tool_args, "
                    "       r.requested_at, t.user_id, t.title "
                    "FROM thread_permission_requests r "
                    "JOIN threads t ON t.id = r.thread_id "
                    "WHERE r.status = 'pending' "
                    "  AND r.requested_at < now() - ($1::int * interval '1 second') "
                    "  AND NOT EXISTS ("
                    "    SELECT 1 FROM notifications n "
                    "    WHERE n.source_kind = 'permission_request' "
                    "      AND n.source_id = r.id::text"
                    "  ) "
                    "ORDER BY r.requested_at ASC "
                    "LIMIT 50",
                    age_threshold_s,
                )
            for row in rows:
                try:
                    result = await headless_notifications.record_permission_pending(
                        postgres_db,
                        notification_service,
                        row=dict(row),
                        cockpit_external_url=cockpit_external_url,
                    )
                    if result.get("status") == "recorded":
                        logger.info(
                            "Recorded permission-pending notification "
                            "(thread=%s req=%s)",
                            str(row["thread_id"])[:8],
                            str(row["id"])[:8],
                        )
                except Exception as e:
                    logger.warning(
                        "Permission-pending notification failed (req=%s): %s",
                        str(row["id"])[:8],
                        e,
                    )
        except Exception as e:
            logger.warning("headless permission-notify sweep error: %s", e)
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=float(interval_s))
            break
        except asyncio.TimeoutError:
            pass
    logger.info("Headless permission-notify sweeper stopped")


# =============================================================================
# Phase 5 — Attention sleep watchdog
# =============================================================================
#
# Suspends thread workspaces (and the bound agent pod) when the agent has
# been in `awaiting_user` for longer than HEADLESS_ATTENTION_SLEEP_MINUTES.
# State machine:
#   active ─→ awaiting_user (agent: natural pause + no WS subscriber)
#   awaiting_user ─→ suspended (this watchdog after TTL)
#   awaiting_user ─→ active (agent: subscriber reattach, clears timer)
#   suspended ─→ active (magic-link wake or REST reattach restores workspace)
#
# Magic-link "extend window" POSTs bump awaiting_user_since forward so the
# watchdog re-arms; threads.extend_count caps the bumps at 4 (4h total).
#
# Today's "tethered" signal is WS-only — Phase 5 v1 ships before the
# cockpit migrates from WS to SSE. SSE-only consumers (MCP, curl) do not
# block suspension; they should rely on magic-link wake to bring the
# session back. When cockpit moves to SSE, this watchdog will need to
# consult the orchestrator's in-process SSE attach registry too.


_ATTENTION_SLEEP_INTERVAL_S: int = int(
    os.environ.get("HEADLESS_ATTENTION_SLEEP_INTERVAL_S", "60")
)
_ATTENTION_SLEEP_MINUTES: int = int(
    os.environ.get("HEADLESS_ATTENTION_SLEEP_MINUTES", "60")
)


async def attention_sleep_sweeper(shutdown_event: asyncio.Event) -> None:
    """Background task: suspend threads stuck in awaiting_user past their TTL.

    Runs every HEADLESS_ATTENTION_SLEEP_INTERVAL_S (default 60s). Each
    qualifying pinned generation enters the same durable retirement funnel as
    owner End, settling to ``suspended`` only after generation-fenced staging
    and exact resource cleanup. Resume stays closed for the entire operation.

    Best-effort: a transient failure (DB unavailable, suspend service
    error) is logged and retried on the next tick.
    """
    interval_s = _ATTENTION_SLEEP_INTERVAL_S
    ttl_minutes = _ATTENTION_SLEEP_MINUTES
    logger.info(
        "Attention-sleep sweeper started (interval=%ds, ttl=%dmin)",
        interval_s,
        ttl_minutes,
    )

    while not shutdown_event.is_set():
        try:
            # A disconnect intentionally leaves a short TTL grace so reloads
            # and multi-tab handoffs never flicker. If a turn reached its
            # natural pause inside that grace, converge it once the queue is
            # durably done and the final client TTL has expired. This is
            # independent of workspace suspension being enabled.
            try:
                promoted = await promote_expired_stateless_pauses(postgres_db, limit=50)
                if promoted:
                    logger.info(
                        "presence expiry promoted %d stateless thread(s) "
                        "to awaiting_user",
                        len(promoted),
                    )
            except Exception as exc:
                # Presence convergence is additive. It must never suppress the
                # pre-existing awaiting_user suspension sweep on the same tick.
                logger.warning("presence expiry promotion failed: %s", exc)
            if workspace_suspension_service.is_enabled:
                async with postgres_db.acquire() as conn:
                    # Phase 6: per-thread TTL resolution. Priority order is
                    # (1) thread.metadata.config_override.headless overrides,
                    # (2) users.settings.persistent_agent overrides,
                    # (3) the global HEADLESS_ATTENTION_SLEEP_MINUTES default.
                    # ttl <= 0 disables the watchdog for that thread, matching
                    # the cockpit UX of "Never auto-suspend".
                    rows = await conn.fetch(
                        "SELECT t.id, t.status, t.execution_lane, "
                        "       t.runtime_generation, t.agent_id, "
                        "       t.runtime_attach_token "
                        "FROM threads t "
                        "LEFT JOIN users u ON u.id = t.user_id "
                        "WHERE t.status = 'awaiting_user' "
                        "  AND t.execution_lane <> 'stateless' "
                        "  AND t.awaiting_user_since IS NOT NULL "
                        # Officer sessions never sleep via attention-sleep —
                        # their lifecycle belongs to the officer watchdog
                        # (centurion.md §4). Belt-and-suspenders: the agent
                        # side already skips the awaiting_user flip for them.
                        "  AND COALESCE(t.metadata->'config_override'->'officer'"
                        "->>'enabled','false') <> 'true' "
                        "  AND COALESCE("
                        "    NULLIF(t.metadata->'config_override'->'headless'->>'attention_sleep_minutes', '')::int, "
                        "    NULLIF(u.settings->'persistent_agent'->>'headless_attention_sleep_minutes', '')::int, "
                        "    $1::int"
                        "  ) > 0 "
                        "  AND t.awaiting_user_since < now() - make_interval(mins => COALESCE("
                        "    NULLIF(t.metadata->'config_override'->'headless'->>'attention_sleep_minutes', '')::int, "
                        "    NULLIF(u.settings->'persistent_agent'->>'headless_attention_sleep_minutes', '')::int, "
                        "    $1::int"
                        "  )) "
                        "ORDER BY t.awaiting_user_since ASC "
                        "LIMIT 50",
                        int(ttl_minutes),
                    )

                for row in rows:
                    thread_id = str(row["id"])
                    try:
                        result = await _thread_retirement_operations().end_thread_flow(
                            thread_id,
                            dict(row),
                            permanent=False,
                            force=False,
                            expected_runtime_generation=str(row["runtime_generation"]),
                            expected_agent_id=(
                                str(row["agent_id"])
                                if row["agent_id"] is not None
                                else None
                            ),
                            expected_attach_token=(
                                str(row["runtime_attach_token"])
                                if row["runtime_attach_token"] is not None
                                else None
                            ),
                            settle_status="suspended",
                        )
                        if result.get("status") == "suspended":
                            logger.info(
                                "attention-sleep: thread %s suspended (was "
                                "awaiting_user >%dm)",
                                thread_id,
                                ttl_minutes,
                            )
                        else:
                            logger.info(
                                "attention-sleep: exact retirement declined "
                                "for thread %s (%s)",
                                thread_id,
                                result.get("status"),
                            )
                    except Exception as e:
                        logger.warning(
                            "attention-sleep: suspend failed for thread %s: %s",
                            thread_id,
                            e,
                        )
        except Exception as e:
            logger.warning("attention-sleep sweep error: %s", e)

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=float(interval_s))
            break
        except asyncio.TimeoutError:
            pass

    logger.info("Attention-sleep sweeper stopped")


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
        cancel_srw=lambda job: _job_mutation_operations().cancel(
            str(job["id"]), job=job
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
