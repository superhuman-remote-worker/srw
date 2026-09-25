"""The application's background tasks, in their fixed start order (R1.B11).

Loop bodies live in their domain modules. This module only decides which run,
under which gate, with which collaborators, and whether leadership gates them;
``BACKGROUND_TASK_SHUTDOWN_ORDER`` is the order shutdown awaits them.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import os
from datetime import timedelta
from typing import Any

from orchestrator.application import (
    completion as completion_composition,
    controls as controls_composition,
    jobs as jobs_composition,
    projects as projects_composition,
    transport as transport_composition,
    workflows as workflows_composition,
    workspace as workspace_composition,
)
from orchestrator.application.resources import ApplicationResources, bound
from orchestrator.security import auth
from orchestrator.services import (
    agent_provisioner as agent_provisioner_module,
    audit_partitions,
    audit_usage,
    cloud_pricing,
    completion_recovery as completion_recovery_operations,
    container_provisioner as container_provisioner_module,
    cron_dispatcher,
    ide_session,
    ide_settings,
    imap_poller as imap_poller_module,
    infrastructure_activation_policy,
    infrastructure_metering,
    job_dispatcher,
    knowledge_index as knowledge_index_operations,
    knowledge_projection as knowledge_projection_operations,
    lifecycle as instance_lifecycle,
    notification_service as notification_service_module,
    notification_steps,
    officer_watchdog as officer_watchdog_service,
    openrouter_pricing,
    persistent_provisioner as persistent_provisioner_module,
    pinned_k8s_reconciliation as pinned_k8s_reconciliation_service,
    project_loop_advance as project_loop_advance_service,
    project_loop_spawn as project_loop_spawn_service,
    project_loop_sweeper,
    retention_sweepers,
    ro_reader_reconciler,
    session_attention as session_attention_operations,
    session_provisioner,
    session_wake,
    snapshot_service as snapshot_service_module,
    stale_agent_detector as stale_agent_detector_service,
    stale_verification_sweeper,
    sudo_gate as sudo_gate_module,
    usage_rollup,
    vm_provisioner as vm_provisioner_module,
    vm_readiness,
    vm_workspace_recovery_config,
    vm_workspace_recovery_store as vm_workspace_recovery_store_module,
    workspace_metering,
    workspace_suspension,
)
from orchestrator.services.application_tasks import ApplicationTaskSet
from orchestrator.services.cloud import reload
from orchestrator.services.infrastructure_metering import bootstrap, ingestion
from orchestrator.services.lifecycle import (
    AgentInstanceManager,
    PersistentAgentInstanceManager,
    VMInstanceManager,
    WorkspaceInstanceManager,
    reconciler,
)
from orchestrator.services.vm_creation_retry import VMCreationRetryService
from orchestrator.services.vm_workspace_recovery import VMWorkspaceRecoveryService
from orchestrator.services.vm_workspace_recovery_config import (
    VMWorkspaceRecoverySettings,
)

logger = logging.getLogger(__name__)

BACKGROUND_TASK_SHUTDOWN_ORDER: tuple[str, ...] = (
    "leader",
    "infrastructure_inventory_generation",
    "datasource_reconciliation",
    "stale_detector",
    "token_cleanup",
    "session_cleanup",
    "dispatcher",
    "vm_readiness",
    "vm_creation_retry",
    "vm_workspace_recovery",
    "sudo_sweeper",
    "thread_events_prune",
    "run_queue_reaper",
    "stateless_deletion_cost",
    "session_memory_effect",
    "completion_finalizer",
    "completion_sweep_router",
    "completion_monitor",
    "security_events_prune",
    "ssh_attachments_prune",
    "checkpoint_retention",
    "headless_notify",
    "attention_sleep",
    "officer_watchdog",
    "message_route_reconciler",
    "officer_backlog",
    "ide_sweeper",
    "ws_sweeper",
    "ide_settings_sweeper",
    "gc_sweeper",
    "pinned_create_intent_reconciler",
    "pinned_create_fence_gc",
    "imap",
    "notification_steps",
    "delegation_timeout",
    "llm_outage",
    "infra_transient",
    "pool_reconciler",
    "ro_reader_reconciler",
    "lifecycle_reconciler",
    "main_cloud_listen",
    "automation_cron",
    "project_loop_sweeper",
    "stale_verification_sweeper",
    "session_wake_sweeper",
    "kb_reindex_sweeper",
    "pricing_sync",
    "cloud_pricing_sync",
    "workspace_metering",
    "llm_usage",
    "usage_rollup",
    "infrastructure_usage_rollup",
    "infrastructure_metering_runtime",
    "audit_maintenance",
)


async def start_background_tasks(
    resources: ApplicationResources,
    tasks: ApplicationTaskSet,
    *,
    audit_ready: bool,
    metering_capabilities: Any,
) -> None:
    """Start the lifecycle's background tasks, in their fixed order."""

    # Start background tasks. The lifecycle's task set owns them: leader-only
    # loops go through run_when_leader, and shutdown awaits every started task
    # in _BACKGROUND_TASK_SHUTDOWN_ORDER (R1.B11).
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
            dependencies=projects_composition.knowledge_projection_dependencies(
                resources
            ),
        )

    async def _strict_datasource_delete(project_id: str, datasource_id: str) -> None:
        await knowledge_projection_operations.delete_datasource_knowledge(
            project_id,
            datasource_id,
            strict=True,
            dependencies=projects_composition.knowledge_projection_dependencies(
                resources
            ),
        )

    # Allocate the infrastructure fencing token on the exact advisory-lock
    # session, before is_leader becomes visible to any singleton loop. This
    # gives collector, cutover, publisher, and sealer one shared tenure token.
    metering_generation_callback = (
        bootstrap.allocate_metering_generation
        if metering_capabilities.slice1_inventory_ready
        else None
    )
    tasks.start(
        "leader",
        run_as_leader(
            resources.postgres_db,
            LEADER_ID,
            resources.shutdown_event,
            on_acquired=metering_generation_callback,
        ),
    )

    async def _inventory_generation_coro(stop: asyncio.Event) -> None:
        generation = get_leader_generation()
        if generation is None:
            raise RuntimeError("metering leader generation is unavailable")
        await ingestion.run_inventory_generation_loop(
            stop,
            resources.metering.infrastructure_inventory_store,
            generation=generation,
            cleanup_interval_seconds=(
                resources.metering.infrastructure_metering_settings.cleanup_interval_seconds
            ),
            snapshot_item_retention=timedelta(
                days=(
                    resources.metering.infrastructure_metering_settings.snapshot_item_retention_days
                )
            ),
            diagnostic_retention=timedelta(
                days=resources.metering.infrastructure_metering_settings.diagnostic_retention_days
            ),
        )

    if resources.metering.infrastructure_inventory_store is not None:
        tasks.start_leader_gated(
            "infrastructure_inventory_generation",
            _inventory_generation_coro,
        )

    if resources.metering.infrastructure_metering_runtime is not None:
        tasks.start_leader_gated(
            "infrastructure_metering_runtime",
            lambda stop: infrastructure_metering.infrastructure_metering_runtime_loop(
                stop,
                resources.metering.infrastructure_metering_runtime,
                get_leader_generation,
            ),
        )
    tasks.start(
        "datasource_reconciliation",
        run_datasource_project_reconciler(
            resources.postgres_db,
            resources.shutdown_event,
            is_leader.is_set,
            sync_fn=_strict_datasource_sync,
            delete_fn=_strict_datasource_delete,
        ),
    )
    tasks.start_leader_gated(
        "stale_detector",
        functools.partial(
            stale_agent_detector_service.stale_agent_detector,
            dependencies=controls_composition.stale_agent_detector_dependencies(
                resources
            ),
        ),
    )
    tasks.start(
        "token_cleanup",
        auth.cleanup_expired_tokens(resources.postgres_db, resources.shutdown_event),
    )
    tasks.start(
        "session_cleanup",
        auth.cleanup_expired_sessions(resources.postgres_db, resources.shutdown_event),
    )
    tasks.start_leader_gated(
        "dispatcher",
        functools.partial(
            job_dispatcher.auto_assign_dispatcher,
            dependencies=jobs_composition.job_dispatch_dependencies(resources),
        ),
    )
    if os.getenv("VM_MODE", "off").strip().lower() == "same-cluster":
        tasks.start_leader_gated(
            "vm_readiness",
            lambda shutdown: vm_readiness.vm_readiness_prober(
                shutdown,
                db=resources.postgres_db,
                provisioner=vm_provisioner_module.vm_provisioner,
                trigger_dispatch=bound(
                    job_dispatcher.trigger_dispatch,
                    jobs_composition.job_dispatch_dependencies,
                    resources,
                ),
            ),
        )
    vm_workspace_recovery_settings = VMWorkspaceRecoverySettings.from_env()
    if os.getenv("VM_MODE", "off").strip().lower() == "same-cluster":
        tasks.start_leader_gated(
            "vm_creation_retry",
            VMCreationRetryService(
                resources.postgres_db, vm_provisioner_module.vm_provisioner
            ).run,
            name="vm-creation-retry",
        )
    vm_workspace_recovery_store = (
        vm_workspace_recovery_store_module.VMWorkspaceRecoveryStore(
            resources.postgres_db
        )
    )
    if vm_workspace_recovery_config.automatic_reconciler_enabled():
        tasks.start_leader_gated(
            "vm_workspace_recovery",
            VMWorkspaceRecoveryService.from_settings(
                vm_workspace_recovery_store,
                vm_provisioner_module.vm_provisioner,
                settings=vm_workspace_recovery_settings,
            ).run,
            name="vm-workspace-recovery",
        )
    tasks.start(
        "sudo_sweeper",
        sudo_gate_module.sudo_expiration_sweeper(
            resources.shutdown_event,
            gate=sudo_gate_module.sudo_gate,
            # Built per tick inside the sweeper's own isolated try.
            fail_expired_vm_upgrade_jobs=lambda: (
                controls_composition.job_control_operations(
                    resources
                ).fail_expired_vm_upgrade_jobs()
            ),
        ),
    )
    tasks.start(
        "thread_events_prune",
        retention_sweepers.thread_events_prune_sweeper(
            resources.shutdown_event, store=resources.postgres_db
        ),
    )
    # Stateless-lane lease reaper (stateless_agents.md §5.2): leader-gated on
    # its OWN advisory lock (RUN_QUEUE_REAPER_ID — not run_when_leader, so the
    # sweep can survive a main-leader handover independently); per-row CAS
    # steals + turn.interrupted/turn.parked journal frames.
    from orchestrator.services.run_queue_reaper import run_queue_reaper_loop

    tasks.start(
        "run_queue_reaper",
        run_queue_reaper_loop(resources.postgres_db, resources.shutdown_event),
    )
    # Pod-deletion-cost reconciler (capacity_ux_and_queue_autoscaling.md §2):
    # leader-gated on its OWN advisory lock (STATELESS_DELETION_COST_ID), it
    # stamps lease-holding stateless pods expensive-to-delete so an HPA
    # scale-down removes idle executors first. Off-cluster it idles.
    from orchestrator.services.stateless_pod_deletion_cost import (
        reconciler_enabled as _deletion_cost_reconciler_enabled,
        stateless_pod_deletion_cost_loop,
    )

    if _deletion_cost_reconciler_enabled():
        tasks.start(
            "stateless_deletion_cost",
            stateless_pod_deletion_cost_loop(
                resources.postgres_db, resources.shutdown_event
            ),
            name="stateless-pod-deletion-cost",
        )
    # Stateless turn memory is its own transactional-outbox ownership domain.
    # It is always resident and never hidden behind completion-command flags or
    # advisory leadership: row leases serialize replicas and survive handover.
    tasks.start(
        "session_memory_effect",
        resources.session_memory_runtime.drain().run_drain(resources.shutdown_event),
        name="session-memory-effect-drain",
    )
    # Gate-3 completion drain uses its own observable River-style lease row;
    # it must never be wrapped in the orchestrator advisory-leader helper.
    # Keep the finalizer/router module imports dark while the gate is closed.
    # Queue age is a worker-availability signal, so the monitor must remain
    # alive when fresh worker admission or Gate-3 commands are disabled.
    # Its commands-off sampler is explicitly run_queue-only.
    tasks.start(
        "completion_monitor",
        resources.completion_runtime.monitor().run(resources.shutdown_event),
        name="completion-monitor",
    )
    from shared.cloud_push_tasks import enabled as cloud_push_recovery_enabled

    if resources.settings.completion_commands_enabled or cloud_push_recovery_enabled():
        completion_finalizer = resources.completion_runtime.finalizer()

        async def cloud_push_sweep():
            from orchestrator.services.cloud_push_recovery import (
                sweep_stale_cloud_pushes,
            )

            return await sweep_stale_cloud_pushes(resources.postgres_db)

        tasks.start(
            "completion_finalizer",
            completion_finalizer.run_drain(
                resources.shutdown_event,
                drain_commands=resources.settings.completion_commands_enabled,
                background_sweep=cloud_push_sweep
                if cloud_push_recovery_enabled()
                else None,
            ),
            name="completion-finalizer-drain",
        )
    if resources.settings.completion_commands_enabled:
        tasks.start(
            "completion_sweep_router",
            resources.completion_runtime.sweep_router().run(resources.shutdown_event),
            name="completion-sweep-router",
        )
    tasks.start(
        "security_events_prune",
        retention_sweepers.security_events_prune_sweeper(
            resources.shutdown_event, store=resources.postgres_db
        ),
    )
    # Not leader-gated, matching security_events_prune_task above: a
    # delete-by-age is idempotent, so two replicas racing it is harmless —
    # the second finds nothing.
    tasks.start(
        "ssh_attachments_prune",
        retention_sweepers.ssh_attachments_prune_sweeper(
            resources.shutdown_event, store=resources.postgres_db
        ),
    )
    # In-flight checkpoint retention: bound every live thread's LangGraph
    # checkpoints to the newest N while it runs (leader-gated), so a long job
    # can't fill the checkpointer PVC before it terminates.
    tasks.start(
        "checkpoint_retention",
        run_retention_sweeper(
            resources.postgres_db, resources.shutdown_event, is_leader.is_set
        ),
    )
    tasks.start_leader_gated(
        "headless_notify",
        functools.partial(
            session_attention_operations.thread_permission_notify_sweeper,
            dependencies=transport_composition.session_attention_dependencies(
                resources
            ),
        ),
    )
    # Leader-gated: both snapshot/teardown idle workspaces (attention-sleep) or
    # delete idle IDE VMs/pods (ide-sweeper) after a plain SELECT, with no
    # per-row claim. Under replicas:2 two unguarded copies would double-snapshot
    # to the same S3 key and race teardown against an in-flight snapshot. Gating
    # mirrors the lifecycle reconciler, which already owns the parallel idle
    # workspace-teardown path. See knowledge-base/knowledge/tests/orchestrator_ha_background_loop_sweep.md.
    tasks.start_leader_gated(
        "attention_sleep",
        functools.partial(
            session_attention_operations.attention_sleep_sweeper,
            dependencies=transport_composition.session_attention_dependencies(
                resources
            ),
        ),
    )
    # Officer (centurion) lifecycle: implicit-timer filing, overdue kicks,
    # rate-limited respawn. Leader-gated — respawn must be single-flight.
    tasks.start_leader_gated(
        "officer_watchdog",
        functools.partial(
            officer_watchdog_service.officer_watchdog,
            dependencies=workflows_composition.officer_watchdog_dependencies(resources),
        ),
    )
    # Worker-message route reconciler (officer_message_routing.md §5.2):
    # officer-SLA escalation, the total blocking timeout, and delivery repair.
    # Leader-gated — per-route CAS gives exactly-once, the gate keeps N
    # replicas from redundantly scanning and double-dispatching user emails.
    from orchestrator.services.message_route_reconciler import (
        message_route_reconciler_loop,
    )

    tasks.start_leader_gated(
        "message_route_reconciler",
        lambda ev: message_route_reconciler_loop(
            resources.postgres_db,
            ev,
            resume_job=lambda *args, **kwargs: (
                controls_composition.job_control_operations(
                    resources
                ).internal_resume_job(*args, **kwargs)
            ),
        ),
    )
    # Officer auto-pull tick (officer_backlog_pools.md §5): fill a pool's free
    # slot from its ready, categorized, unclaimed tickets. Leader-gated as an
    # optimization only — correctness is the advisory-locked claim+create
    # transaction plus uq_jobs_active_ticket_claim, because dual-leader windows
    # are real. Dormant until a century sets officer.auto_pull (ships off).
    from orchestrator.services.officer_backlog import officer_backlog_tick_loop

    tasks.start_leader_gated(
        "officer_backlog",
        lambda ev: officer_backlog_tick_loop(
            resources.postgres_db,
            resources.vector_db,
            ev,
            release_enabled=resources.settings.officer_auto_pull_release_enabled,
            provision_repo=bound(
                project_loop_spawn_service.provision_officer_ticket_repo,
                workflows_composition.project_loop_dependencies,
                resources,
            ),
            trigger_dispatch=bound(
                job_dispatcher.trigger_dispatch,
                jobs_composition.job_dispatch_dependencies,
                resources,
            ),
            enforce_grants=(
                lambda *args, **kwargs: (
                    project_loop_spawn_service.enforce_officer_ticket_grants(
                        *args,
                        **kwargs,
                        dependencies=workflows_composition.project_loop_dependencies(
                            resources
                        ),
                    )
                )
            ),
            usage_ledger=resources.usage_ledger,
            notify=session_wake.notify_officer,
        ),
    )
    tasks.start_leader_gated(
        "ide_sweeper",
        functools.partial(
            ide_session.ide_session_ttl_sweeper,
            ide_sessions=ide_session.ide_session_service,
        ),
    )
    tasks.start(
        "ws_sweeper",
        session_provisioner.workspace_idle_sweeper(
            resources.shutdown_event,
            store=resources.postgres_db,
            provisioner=container_provisioner_module.container_provisioner,
            suspension=workspace_suspension.workspace_suspension_service,
            vm_idle_service_factory=functools.partial(
                workspace_composition.vm_idle_service, resources
            ),
            terminal_vm_controls_factory=functools.partial(
                controls_composition.job_mutation_operations, resources
            ),
        ),
    )
    # Leader-gated: serially SSH-dials every active workspace and captures IDE
    # profiles to per-user S3 keys — two replicas would double-dial each
    # workspace and race the signature-gated capture.
    tasks.start_leader_gated(
        "ide_settings_sweeper",
        functools.partial(
            ide_settings.code_server_settings_sweeper,
            db=resources.postgres_db,
            container_provisioner=container_provisioner_module.container_provisioner,
            snapshot_service=snapshot_service_module.snapshot_service,
            vm_provisioner=vm_provisioner_module.vm_provisioner,
        ),
    )
    tasks.start(
        "gc_sweeper",
        snapshot_service_module.snapshot_gc_sweeper(
            resources.shutdown_event, snapshots=snapshot_service_module.snapshot_service
        ),
    )
    tasks.start_leader_gated(
        "pinned_create_intent_reconciler",
        functools.partial(
            pinned_k8s_reconciliation_service.pinned_agent_create_intent_reconciler,
            dependencies=controls_composition.pinned_k8s_reconciliation_dependencies(
                resources
            ),
        ),
    )
    tasks.start_leader_gated(
        "pinned_create_fence_gc",
        functools.partial(
            pinned_k8s_reconciliation_service.pinned_k8s_create_fence_gc_sweeper,
            dependencies=controls_composition.pinned_k8s_reconciliation_dependencies(
                resources
            ),
        ),
    )
    tasks.start_leader_gated(
        "imap",
        functools.partial(
            imap_poller_module.imap_poll_loop, poller=imap_poller_module.imap_poller
        ),
    )
    # Unified feed: run the deferred channel steps ("mail after the officer's
    # window unless seen/resolved", quiet-hours deferrals, batched digests).
    tasks.start_leader_gated(
        "notification_steps",
        lambda stop: notification_steps.notification_steps_loop(
            stop,
            resources.postgres_db,
            notification_service_module.notification_service,
        ),
    )
    tasks.start_leader_gated(
        "delegation_timeout",
        functools.partial(
            completion_recovery_operations.delegation_timeout_sweeper,
            dependencies=completion_composition.completion_recovery_dependencies(
                resources
            ),
            interval_seconds=60,
        ),
    )
    # Re-dispatch worker jobs paused for a transient LLM outage once their
    # backoff timer is due (fail-loud past the give-up ceiling). Leader-gated —
    # per-row CAS + run_when_leader keep N replicas from double-dispatching.
    # knowledge-base/knowledge/features/llm_outage_pause_and_backoff_redispatch.md
    tasks.start_leader_gated(
        "llm_outage",
        functools.partial(
            completion_recovery_operations.llm_outage_redispatch_sweeper,
            dependencies=completion_composition.completion_recovery_dependencies(
                resources
            ),
            interval_seconds=float(
                (os.getenv("LLM_OUTAGE_SWEEP_SECONDS") or "").strip() or 30
            ),
        ),
    )
    tasks.start_leader_gated(
        "infra_transient",
        functools.partial(
            completion_recovery_operations.infra_transient_redispatch_sweeper,
            dependencies=completion_composition.completion_recovery_dependencies(
                resources
            ),
            interval_seconds=float(
                (os.getenv("INFRA_TRANSIENT_SWEEP_SECONDS") or "").strip() or 30
            ),
        ),
    )
    tasks.start_leader_gated(
        "pool_reconciler",
        functools.partial(
            agent_provisioner_module.agent_pool_reconciler,
            provisioner=agent_provisioner_module.agent_provisioner,
        ),
    )
    # Cleanup authority is independent of fresh protected-mode admission. A
    # feature/config disable must never strand an already durable reader or
    # pre-dispatch effect intent.
    tasks.start_leader_gated(
        "ro_reader_reconciler",
        functools.partial(
            ro_reader_reconciler.ro_reader_reconciler_loop,
            store=resources.postgres_db,
            # Read per tick: the application's router can be rebound.
            router=lambda: resources.main_cloud_router,
        ),
    )
    tasks.start(
        "automation_cron",
        cron_dispatcher.cron_dispatcher_loop(
            resources.postgres_db,
            resources.shutdown_event,
            on_job_created=bound(
                job_dispatcher.trigger_dispatch,
                jobs_composition.job_dispatch_dependencies,
                resources,
            ),
            # The loop outlives every request, so it carries the provisioning
            # adapter explicitly (R1.B07 caller closure).
            provision_repo=functools.partial(
                workflows_composition.provision_cron_job_repo, resources
            ),
        ),
    )
    # Safety-net for project self-improvement loops: recover any loop whose
    # current job went terminal without the completion hook advancing it.
    tasks.start(
        "project_loop_sweeper",
        project_loop_sweeper.project_loop_sweeper_loop(
            resources.postgres_db,
            resources.shutdown_event,
            advance_fn=(
                lambda *args, **kwargs: (
                    project_loop_advance_service.advance_project_loop(
                        *args,
                        **kwargs,
                        dependencies=workflows_composition.project_loop_dependencies(
                            resources
                        ),
                    )
                )
            ),
            **(
                {
                    "completion_commands_enabled": True,
                    "reconcile_handoff_fn": (
                        bound(
                            project_loop_advance_service.reconcile_atomic_project_loop_handoff,
                            workflows_composition.project_loop_dependencies,
                            resources,
                        )
                    ),
                }
                if resources.settings.completion_commands_enabled
                else {}
            ),
        ),
    )
    # Reap orphaned verification (critic) subjobs that would otherwise linger as
    # priority-10 dispatchable jobs and parasitically preempt real work. See
    # knowledge-history/done/preemption_before_first_checkpoint_replays_job_opening.md.
    tasks.start(
        "stale_verification_sweeper",
        stale_verification_sweeper.stale_verification_sweeper_loop(
            resources.postgres_db,
            resources.shutdown_event,
            stateless_cancel_fn=(
                resources.postgres_db.cancel_and_settle_stale_stateless_verification_subjob
            ),
            completion_commands_enabled=resources.settings.completion_commands_enabled,
        ),
    )
    # Backstop for session wakes: deliver any completion notice whose
    # opportunistic post-commit send was lost, or whose terminal path has no
    # hook at all. Deliberately NOT run_when_leader — single-firing comes from
    # the row claim, which works from every replica, and leader-gating would
    # make this a SPOF across a handover. See services/session_wake.py.
    tasks.start(
        "session_wake_sweeper",
        session_wake.session_wake_sweeper_loop(
            resources.postgres_db, resources.shutdown_event
        ),
    )

    # Slice-3 KB index freshness sweep: catch out-of-band vault edits (human
    # pushes, recovered partial reindexes) the post-merge trigger can't see.
    # Leader-gated — two replicas replace-note-chunks'ing the same KB would
    # interleave delete+insert batches. The store rides the vector pool; the
    # embedding service is catalog-re-resolved per tick inside the loop.
    def _kb_sweeper_coro(ev: asyncio.Event):
        from orchestrator.services.kb_reindex import kb_reindex_sweeper_loop
        from shared.runtime.services.knowledge_store import KnowledgeStore

        return kb_reindex_sweeper_loop(
            resources.postgres_db,
            KnowledgeStore(db=resources.vector_db, embedding_service=None),
            resources.gitea_client,
            ev,
            embedding_service_factory=(
                lambda: knowledge_index_operations.build_kb_embedding_service(
                    dependencies=projects_composition.knowledge_index_dependencies(
                        resources
                    )
                )
            ),
        )

    tasks.start_leader_gated(
        "kb_reindex_sweeper",
        _kb_sweeper_coro,
    )

    # LLM $/token pricing sync: seed usage_rates from OpenRouter × the model
    # catalog (params_json.pricing_id) so record_events can cost the audit-
    # sourced token rows. Slow (6h) + change-only; no-op without the app pool.
    tasks.start(
        "pricing_sync",
        openrouter_pricing.llm_pricing_sync_loop(
            resources.shutdown_event,
            resources.postgres_db.pool,
            resources.postgres_db.list_models,
        ),
    )

    # Public-cloud comparison prices: AWS/Azure publish machine-readable list
    # prices. Refresh change-only once per day; STACKIT's PDF-backed reference
    # card is source-labelled and seeded by app migration 0082.
    tasks.start(
        "cloud_pricing_sync",
        cloud_pricing.cloud_pricing_sync_loop(
            resources.shutdown_event, resources.postgres_db.pool
        ),
    )

    # Workspace compute metering (Slice 4b): materialize CLOSED workspace
    # intervals into the usage ledger + reconcile leaked opens. Self-disables
    # when the app pool or ledger is absent (non-load-bearing tier).
    tasks.start_leader_gated(
        "workspace_metering",
        lambda stop: workspace_metering.workspace_metering_loop(
            stop,
            resources.postgres_db,
            resources.usage_ledger,
            lambda owner_kind,
            owner_id: infrastructure_activation_policy.workspace_metering_attribution(
                owner_kind, owner_id, store=resources.postgres_db
            ),
        ),
    )

    # LLM usage materialization (Slice 4c): materialize audit llm_requests into
    # usage ledger rows. Self-disables when audit/app pools or the ledger are absent.
    # R1.B05 lane C moved the loop body beside the work it drives
    # (`services/audit_usage.py`, like the pricing and metering loops).
    # B11 still owns the scheduling; only the call shape changed.
    tasks.start(
        "llm_usage",
        audit_usage.llm_usage_poll_loop(
            resources.shutdown_event,
            audit_db=resources.audit_db,
            app_store=resources.postgres_db,
            usage_ledger=resources.usage_ledger,
            logger=logger,
        ),
    )

    # Usage rollup (Phase 6 / D-1): re-aggregate closed days from the auditdb
    # usage_events firehose into the app-DB usage_daily mirror + advance the
    # rollup_state watermark. Leader-only (the upsert is idempotent, but there's
    # no value in every replica re-aggregating); self-disables without both pools.
    tasks.start_leader_gated(
        "usage_rollup",
        lambda se: usage_rollup.usage_rollup_loop(se, resources.usage_rollup),
    )

    # Typed v2 bootstrap/dirty-day reconciliation is also leader-owned and
    # non-load-bearing. It runs while the public read gate is off so operators
    # can enable v2 only after the durable bootstrap state reports complete.
    if resources.metering.infrastructure_usage_rollup is not None:
        tasks.start_leader_gated(
            "infrastructure_usage_rollup",
            lambda se: infrastructure_metering.typed_usage_rollup_loop(
                se, resources.metering.infrastructure_usage_rollup
            ),
        )

    # Audit-store partition maintenance (creation + ANALYZE + lookahead alarms;
    # retention deferred — see services/audit_partitions.py). Only when the
    # audit DB is configured; otherwise the store is inactive and there is
    # nothing to maintain.
    if resources.audit_db is not None and audit_ready:
        tasks.start(
            "audit_maintenance",
            audit_partitions.maintenance_loop(
                resources.audit_db.pool, resources.shutdown_event
            ),
        )

    # Unified instance lifecycle reconciler (drift-based draining and,
    # in future phases, crash recovery + cross-kind primitives). Runs
    # peer to agent_pool_reconciler — pool owns capacity, lifecycle
    # owns version/health.
    lifecycle_reconciler = instance_lifecycle.InstanceLifecycleReconciler()
    lifecycle_reconciler.register(
        AgentInstanceManager(
            provisioner=agent_provisioner_module.agent_provisioner,
            db=resources.postgres_db,
        )
    )
    lifecycle_reconciler.register(
        PersistentAgentInstanceManager(
            provisioner=persistent_provisioner_module.persistent_provisioner,
            db=resources.postgres_db,
            recycler=resources.persistent_thread_recycler,
            automatic_enabled=resources.settings.persistent_agent_reconciliation_enabled,
        )
    )
    lifecycle_reconciler.register(
        WorkspaceInstanceManager(
            container_provisioner=container_provisioner_module.container_provisioner,
            suspension_service=workspace_suspension.workspace_suspension_service,
            snapshot_service=snapshot_service_module.snapshot_service,
            db=resources.postgres_db,
            completion_commands_enabled=resources.settings.completion_commands_enabled,
            completion_router=(
                resources.completion_runtime.sweep_router()
                if resources.settings.completion_commands_enabled
                else None
            ),
        )
    )
    lifecycle_reconciler.register(
        VMInstanceManager(
            vm_provisioner=vm_provisioner_module.vm_provisioner,
            suspension_service=workspace_suspension.workspace_suspension_service,
            snapshot_service=snapshot_service_module.snapshot_service,
            db=resources.postgres_db,
            completion_commands_enabled=resources.settings.completion_commands_enabled,
            completion_router=(
                resources.completion_runtime.sweep_router()
                if resources.settings.completion_commands_enabled
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
    tasks.start_leader_gated(
        "lifecycle_reconciler",
        lambda se: reconciler.lifecycle_reconciler_loop(se, lifecycle_reconciler),
    )

    # Phase 4: main-cloud config LISTEN task — reacts to pg_notify when
    # an admin PUTs a new config via /api/admin/system-settings/main_cloud.
    async def _main_cloud_reload_callback() -> None:
        await reload._reload_from_db_and_swap(
            resources.postgres_db, resources.main_cloud_router
        )

    tasks.start(
        "main_cloud_listen",
        reload.run_listen_loop(
            resources.postgres_db, _main_cloud_reload_callback, resources.shutdown_event
        ),
    )
