"""Application startup and shutdown (R1.B11 phases).

``lifespan`` runs the two preflight refusals, then ``start_application``
(``open_stores`` → ``bind_services`` → ``start_background_tasks``), yields, and
runs ``stop_application``. A failed startup runs the ordinary shutdown and
re-raises; a failed background task does not stop the rest of shutdown.
Late-bound collaborators (usage ledger, metering bootstrap, persistent thread
recycler, shutdown event) are assigned on the application's resources here.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI

from orchestrator import graph_routes
from orchestrator.application import (
    background_tasks as background_tasks_composition,
    catalogue as catalogue_composition,
    jobs as jobs_composition,
    workflows as workflows_composition,
)
from orchestrator.application.resources import ApplicationResources, bound
from orchestrator.services import (
    agent_provisioner as agent_provisioner_module,
    canvas_office,
    container_provisioner as container_provisioner_module,
    default_experts,
    docker_provisioner as docker_provisioner_module,
    email,
    ide_proxy,
    ide_session,
    imap_poller as imap_poller_module,
    inbound_reply as inbound_reply_service,
    job_dispatcher,
    nats_bridge as nats_bridge_module,
    notification_actions as notification_action_service,
    notification_service as notification_service_module,
    officer_paging as officer_paging_service,
    persistent_provisioner as persistent_provisioner_module,
    readiness as readiness_service,
    session_wake,
    sitrep as sitrep_service,
    snapshot_service as snapshot_service_module,
    startup_backfills,
    sudo_gate as sudo_gate_module,
    virtual_workspace,
    vm_provisioner as vm_provisioner_module,
    workspace_suspension,
)
from orchestrator.services.application_tasks import ApplicationTaskSet
from orchestrator.services.cloud import instance_registry
from orchestrator.services.cloud_pricing import CloudCostEstimator
from orchestrator.services.infrastructure_metering import bootstrap
from orchestrator.services.persistent_recycler import PersistentThreadRecycler
from orchestrator.services.usage_ledger import UsageLedger, UsageRates
from orchestrator.services.usage_rollup import UsageRollup

logger = logging.getLogger(__name__)


def bind_officer_wake_metering(resources: ApplicationResources) -> None:
    """Bind this application's store to its usage ledger for Officer wakes.

    The daily-ceiling brake runs inside the session-wake drain, which every
    caller reaches with only the store. The provider reads this
    application's ``usage_ledger`` per check, so a ledger built later in startup
    (or never, without the audit tier) is seen (R1.B10 per-store binding).
    """
    session_wake.bind_officer_wake_metering(
        resources.postgres_db, lambda: resources.usage_ledger
    )


async def open_stores(resources: ApplicationResources) -> tuple[bool, Any]:
    """Connect the databases, run migrations and seeds, build the usage ledger
    and the metering bootstrap, and run the startup backfills.

    Returns whether the audit tier is ready and the metering schema
    capabilities; the background tasks need both.
    """

    # Connect to databases
    await resources.postgres_db.connect()
    await resources.vector_db.connect()
    if os.getenv("COLLABORA_ENABLED", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        if await canvas_office.warm_collabora_discovery():
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
    if resources.audit_db is None:
        logger.info(
            "Audit DB disabled (AUDIT_POSTGRES_* unset) — Postgres audit store "
            "inactive; audit writes and reads no-op until it is configured."
        )
    else:
        try:
            await resources.audit_db.connect()
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
    if resources.audit_db is not None:
        await resources.audit_store.connect()
        logger.info("Audit reads served by Postgres AuditStore")

    # Apply pending migrations on each DB. Each PostgresDB instance is
    # bound to its migrations directory at construction time; the runner
    # serializes via pg_advisory_xact_lock and refuses to proceed on
    # checksum drift or a dirty row from a prior failure (see
    # knowledge-base/knowledge/db_migration.md §Operational runbook for repair steps).
    await resources.postgres_db.apply_migrations()
    from orchestrator.services.manifest_experts import (
        installed_srw_image,
        migrate_stored_experts,
        seed_bundled_expert_manifests,
    )

    resources.postgres_db.manifest_runtime_image = installed_srw_image()
    resources.postgres_db.manifest_skills_provider = (
        lambda *args, **kwargs: catalogue_composition.expert_catalog_service(
            resources
        ).gather_in_scope_skills(*args, **kwargs)
    )
    await migrate_stored_experts(resources.postgres_db)
    managed_defaults = await default_experts.seed_managed_default_experts(
        resources.postgres_db, catalogue_composition.get_config_dir()
    )
    await seed_bundled_expert_manifests(
        resources.postgres_db, catalogue_composition.get_config_dir()
    )
    from orchestrator.services.manifest_projects import migrate_projects

    await migrate_projects(resources.postgres_db)
    resources.postgres_db.manifests_ready = True
    logger.info(
        "Managed expert defaults ready: worker=%s session=%s",
        managed_defaults.get("worker"),
        managed_defaults.get("session"),
    )
    await resources.vector_db.apply_migrations()
    if resources.audit_db is not None and audit_ready:
        try:
            await resources.audit_db.apply_migrations()
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

        if await ensure_tavily_search_endpoint(resources.postgres_db):
            logger.info("Tavily search provider registered from TAVILY_API_KEY")
    except Exception:
        logger.warning("ensure_tavily_search_endpoint failed at startup", exc_info=True)

    # Auto-wire the ElevenLabs TTS provider when ELEVENLABS_API_KEY is present,
    # so the read-aloud voice provider appears with no manual Admin step (same
    # pattern as the codex proxy). Best-effort: never blocks startup.
    try:
        from orchestrator.seed.llm_config import ensure_elevenlabs_tts_endpoint

        if await ensure_elevenlabs_tts_endpoint(resources.postgres_db):
            logger.info("ElevenLabs TTS provider registered from ELEVENLABS_API_KEY")
    except Exception:
        logger.warning(
            "ensure_elevenlabs_tts_endpoint failed at startup", exc_info=True
        )

    # Pin a default for each required capability that has catalog rows but no
    # pin (bare-metal init.py rows, installs upgraded with rows never pinned).
    # Best-effort: logs and leaves the pin to the admin on failure.
    await readiness_service.try_auto_pin_required_defaults(resources.postgres_db)

    # Usage-metering ledger (Slice 4). Writes go to the auditdb usage_events
    # table (None → no-op when the audit tier is absent); rates resolve against
    # the app-DB usage_rates table created by the migration above. Built here so
    # both pools + the schema are ready. Emitters (compute / LLM materialization)
    # and /api/usage read this singleton.
    audit_usage_pool = (
        resources.audit_db.pool
        if (resources.audit_db is not None and audit_ready)
        else None
    )
    canonical_usage_rates = UsageRates(resources.postgres_db.pool)
    resources.usage_ledger = UsageLedger(
        audit_usage_pool,
        canonical_usage_rates,
    )
    bind_officer_wake_metering(resources)
    # Rollup over the ledger (Phase 6 / D-1): aggregates the auditdb usage_events
    # firehose into the app-DB usage_daily mirror (+ rollup_state watermark) and
    # serves /api/usage from it for closed days, raw for the open tail. Same
    # availability posture as the ledger (needs both pools).
    resources.usage_rollup = UsageRollup(
        resources.audit_db.pool
        if (resources.audit_db is not None and audit_ready)
        else None,
        resources.postgres_db.pool,
        resources.usage_ledger,
    )
    resources.usage_cloud_estimator = CloudCostEstimator(resources.postgres_db.pool)

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
    metering = await bootstrap.bootstrap_infrastructure_metering(
        app_pool=resources.postgres_db.pool,
        audit_usage_pool=audit_usage_pool,
        usage_ledger=resources.usage_ledger,
        canonical_usage_rates=canonical_usage_rates,
        lifecycle_identity_authenticated=lambda: (
            nats_bridge_module.nats_bridge.lifecycle_identity_authenticated
        ),
    )
    metering_capabilities = metering.capabilities
    resources.metering = metering

    # Idempotent data backfills (R1.B11: moved to services/startup_backfills).
    await startup_backfills.run_startup_backfills(resources.postgres_db)
    return audit_ready, metering_capabilities


async def bind_services(resources: ApplicationResources) -> None:
    """Bind clients, provisioners and notification services to the stores."""

    # Wire the model registry's catalog lookup to the DB. The registry lives
    # in src/core/ and must not import orchestrator/, so the hook is injected
    # here (and unset on shutdown below). custom/system lookups were retired
    # along with user_llm_endpoint_models — the catalog covers both scopes.
    from shared.runtime.core.model_registry import register_catalog_lookup

    register_catalog_lookup(resources.postgres_db.resolve_catalog_model)

    # Share the selected audit reader + the app DB with graph_routes.
    graph_routes.set_audit_reader(resources.audit_reader)
    graph_routes.set_postgres_db(resources.postgres_db)

    # Initialize Gitea workspace delivery (graceful if unavailable)
    await resources.gitea_client.ensure_initialized()

    # Configure Gitea OIDC auth source (graceful if unconfigured)
    await resources.gitea_client.ensure_oidc_configured()

    # Initialize Keycloak group sync (graceful if unavailable)
    await resources.keycloak_groups.ensure_initialized()

    # Adopt the exact durable backend installation before any main-cloud
    # effect. The legacy system_settings row is read only as one-time input
    # when 0186 has no active instance yet; afterward the immutable instance
    # snapshot + singleton CAS pointer are the sole routing authority.
    try:
        _persisted_overlay = await resources.postgres_db.get_system_setting(
            "main_cloud"
        )
    except Exception as _e:
        logger.warning("Legacy main cloud overlay read failed at startup: %s", _e)
        _persisted_overlay = None
    try:
        await instance_registry.initialize_main_cloud_instance_authority(
            resources.postgres_db,
            resources.main_cloud_router,
            legacy_overlay=_persisted_overlay,
        )
        await instance_registry.preload_retained_main_cloud_instances(
            resources.postgres_db, resources.main_cloud_router
        )
        if _persisted_overlay is not None:
            try:
                await resources.postgres_db.delete_system_setting("main_cloud")
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

        _active_id = resources.main_cloud_router.active.backend_id
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
    sudo_gate_module.sudo_gate.connect(db=resources.postgres_db)

    # Initialize NATS bridge for VM lifecycle (graceful if unavailable)
    await nats_bridge_module.nats_bridge.connect(
        db=resources.postgres_db,
        on_vm_ready=bound(
            job_dispatcher.trigger_dispatch,
            jobs_composition.job_dispatch_dependencies,
            resources,
        ),
    )

    # Initialize S3 snapshot service (graceful if S3 not configured)
    await snapshot_service_module.snapshot_service.connect(db=resources.postgres_db)

    # One loud, early signal when either object-store seam is unconfigured,
    # replacing the scattered late failures (virtual-session dispatch, snapshot
    # no-ops). Fail-closed (raise, crash-loop) when OBJECT_STORE_REQUIRED is
    # set; warn-only otherwise. knowledge-history/done/s3_object_store_bundled_fallback.md.
    _store_warning = virtual_workspace.check_object_store_config()
    if _store_warning:
        logger.warning(_store_warning)

    # Initialize VM provisioner (uses NATS if available, else direct K8s)
    vm_provisioner_module.vm_provisioner.connect(
        db=resources.postgres_db,
        snapshot_service=snapshot_service_module.snapshot_service,
    )

    # Initialize container provisioner for workspace containers (direct K8s)
    container_provisioner_module.container_provisioner.connect(
        db=resources.postgres_db,
        snapshot_service=snapshot_service_module.snapshot_service,
    )

    # Initialize Docker Compose provisioner (static workspace pool, used when k8s unavailable)
    docker_provisioner_module.docker_provisioner.connect(
        db=resources.postgres_db,
        snapshot_service=snapshot_service_module.snapshot_service,
    )

    # Log deployment mode.
    # Priority: K8s in-cluster → Docker Compose → K8s via kubeconfig.
    # A local kubeconfig should not shadow Docker Compose when running outside the cluster.
    if (
        container_provisioner_module.container_provisioner.is_available
        and container_provisioner_module.container_provisioner.in_cluster
    ):
        logger.info(
            "Deployment mode: KUBERNETES (in-cluster) — dynamic provisioning via k8s API"
        )
    elif docker_provisioner_module.docker_provisioner.is_available:
        logger.info(
            "Deployment mode: DOCKER COMPOSE — static workspace pool (%s)",
            ",".join(docker_provisioner_module.docker_provisioner.workspace_hosts),
        )
        if container_provisioner_module.container_provisioner.is_available:
            logger.info(
                "Deployment mode: Kubernetes also reachable via kubeconfig "
                "but Docker Compose takes priority (not running in-cluster)"
            )
    elif container_provisioner_module.container_provisioner.is_available:
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
    ide_session.ide_session_service.connect(
        db=resources.postgres_db,
        snapshot_service=snapshot_service_module.snapshot_service,
        vm_provisioner=vm_provisioner_module.vm_provisioner,
        gitea_client=resources.gitea_client,
        container_provisioner=container_provisioner_module.container_provisioner,
    )

    # Initialize persistent agent provisioner (legacy, kept for backward compat)
    persistent_provisioner_module.persistent_provisioner.connect(
        db=resources.postgres_db
    )

    async def _persistent_recycle_failure_page(
        project_id: str, thread_id: str, failure_class: str
    ) -> bool:
        thread = await resources.postgres_db.get_thread(thread_id)
        if not thread or str(thread.get("project_id") or "") != project_id:
            return False
        return await officer_paging_service.dispatch_officer_page(
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
            dependencies=workflows_composition.officer_paging_dependencies(resources),
        )

    async def _persistent_recycle_complete(project_id: str, thread_id: str) -> None:
        if project_id:
            session_wake.kick_event_drain(resources.postgres_db)

    resources.persistent_thread_recycler = PersistentThreadRecycler(
        db=resources.postgres_db,
        provisioner=persistent_provisioner_module.persistent_provisioner,
        failure_notifier=_persistent_recycle_failure_page,
        on_complete=_persistent_recycle_complete,
    )

    # Initialize unified agent provisioner (on-demand pods for jobs + sessions)
    agent_provisioner_module.agent_provisioner.connect(db=resources.postgres_db)

    # Initialize workspace suspension service (idle timeout → S3 snapshot → pod deletion)
    workspace_suspension.workspace_suspension_service.connect(
        db=resources.postgres_db,
        snapshot_service=snapshot_service_module.snapshot_service,
        container_provisioner=container_provisioner_module.container_provisioner,
        docker_provisioner=docker_provisioner_module.docker_provisioner,
        vm_provisioner=vm_provisioner_module.vm_provisioner,
        agent_provisioner=agent_provisioner_module.agent_provisioner,
    )

    # Initialize IDE proxy service. Kubernetes coordinates are freshly
    # control-plane-attested, VM browser relay is contained pending a guest
    # tunnel, and only explicit local-Docker targets may use its short cache.
    ide_proxy.ide_proxy_service.connect(
        db=resources.postgres_db,
        container_provisioner=container_provisioner_module.container_provisioner,
        vm_provisioner=vm_provisioner_module.vm_provisioner,
    )

    # Initialize notification feed (SSE broadcast for cockpit)
    from orchestrator.services.notification_feed import notification_feed

    # Initialize notification service (unified dispatcher for email + webhooks)
    notification_service_module.notification_service.connect(
        db=resources.postgres_db,
        email_service=email.email_service,
        notification_feed=notification_feed,
    )
    # The sitrep's optional sections read these three handles; bind them once
    # here rather than letting that module reach back into this one (R1.B07
    # caller closure). Every entry point still accepts explicit overrides.
    sitrep_service.bind_reporting_handles(
        sitrep_service.ReportingHandles(
            audit_reader=resources.audit_reader,
            usage_ledger=resources.usage_ledger,
            vector_db=resources.vector_db,
        )
    )
    # Unified feed: bind (category, action) handlers and source loaders.
    notification_action_service.register_notification_actions(
        dependencies=workflows_composition.notification_action_dependencies(resources)
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
            dependencies=workflows_composition.inbound_reply_dependencies(resources),
        )
        return strategy

    imap_poller_module.imap_poller.connect(
        db=resources.postgres_db, reply_handler=_imap_reply_handler
    )


async def start_application(
    resources: ApplicationResources, tasks: ApplicationTaskSet
) -> None:
    """Startup, in order: stores, service binding, background tasks
    (R1.B11 split of ``lifespan``)."""

    audit_ready, metering_capabilities = await open_stores(resources)
    await bind_services(resources)
    await background_tasks_composition.start_background_tasks(
        resources,
        tasks,
        audit_ready=audit_ready,
        metering_capabilities=metering_capabilities,
    )


async def stop_application(
    resources: ApplicationResources, tasks: ApplicationTaskSet
) -> None:
    """Stop the lifecycle's tasks, drain the registries, then close clients and
    stores in order (R1.B11 split of ``lifespan``).

    The drains run after every background task has stopped and before any
    client or store closes, in this fixed order (R1.B12):

    1. dispatch passes and preemptions (``job_dispatch_state``);
    2. KB datasource reindexes (``kb_datasource_tasks``);
    3. attach-abort successor provisioning (``attach_abort_successor_tasks``);
    4. stateless workspace reconciles (``stateless_workspace_ensure_registry``);
    5. late session-folder provisioning (``late_cloud_setup_tasks``);
    6. protected-cloud engages and cloud stages (``cloud_task_registry``);
    7. background project repairs (``project_repair_state``).

    Work that provisions or awaits other work is stopped before the leaf work
    it may be waiting on. A drain that fails is logged and the next one still
    runs; like a failed background task, the first failure is re-raised only
    once every store is closed.
    """

    from orchestrator.services.application_tasks import drain_task_mapping

    # Signal shutdown to background tasks and wait for each of them. A task
    # that ended with an error does not stop the rest of shutdown; it is
    # re-raised once every store is closed.
    first_failure = await tasks.stop(
        background_tasks_composition.BACKGROUND_TASK_SHUTDOWN_ORDER
    )

    # Dispatch passes and preemptions started by triggers belong to the
    # application too: stop them before any store closes. The leader loop has
    # exited above, so no new trigger can start one.
    # Initial/manual datasource reindexes are request-spawned rather than loop
    # tasks. Cancel them before closing git/vector clients; the source context
    # removes temporary repositories and auth material in its cancellation path.
    # The remaining registries hold request-spawned tasks that use the same
    # clients and stores; each is cancelled and awaited here. A drain only
    # cancels: it never writes or clears durable intent, such as the
    # attach-abort outcome row the stale-agent detector re-schedules from.
    drains = (
        ("dispatch passes", lambda: resources.job_dispatch_state.drain()),
        ("KB datasource reindexes", lambda: resources.kb_datasource_tasks.drain()),
        (
            "attach-abort successors",
            lambda: drain_task_mapping(resources.attach_abort_successor_tasks),
        ),
        (
            "stateless workspace reconciles",
            lambda: resources.stateless_workspace_ensure_registry.drain(),
        ),
        (
            "late session-folder provisioning",
            lambda: drain_task_mapping(resources.late_cloud_setup_tasks),
        ),
        ("cloud engage and stage tasks", lambda: resources.cloud_task_registry.drain()),
        ("background project repairs", lambda: resources.project_repair_state.drain()),
    )
    for label, drain in drains:
        try:
            await drain()
        except Exception as exc:
            logger.error("Draining %s failed; shutdown continues", label, exc_info=exc)
            if first_failure is None:
                first_failure = exc

    # Cleanup clients
    await nats_bridge_module.nats_bridge.disconnect()
    await vm_provisioner_module.vm_provisioner.disconnect()
    await resources.gitea_client.close()

    # Unregister the registry's DB hook before disconnecting the pool so
    # any stragglers don't hit a closed connection.
    from shared.runtime.core.model_registry import register_catalog_lookup

    register_catalog_lookup(None)

    # Disconnect from databases
    await resources.vector_db.disconnect()
    if resources.audit_store is not None:
        await resources.audit_store.disconnect()
    if resources.audit_db is not None:
        await resources.audit_db.disconnect()
    await resources.postgres_db.disconnect()
    resources.completion_runtime.reset()
    resources.session_memory_runtime.reset()
    if first_failure is not None:
        raise first_failure


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    resources: ApplicationResources = app.state.resources

    # Reordering is an execution mode of durable completion commands, never a
    # standalone legacy-path feature. The admission bit is persisted, so this
    # process flag changes only fresh commands. Reject the invalid combination
    # before opening either database so a bad rollout fails loudly and
    # side-effect free.
    if (
        resources.settings.completion_status_reorder_enabled
        and not resources.settings.completion_commands_enabled
    ):
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

    resources.shutdown_event = asyncio.Event()
    tasks = ApplicationTaskSet(resources.shutdown_event)
    try:
        await start_application(resources, tasks)
    except BaseException:
        # A failed startup must not leave the tasks it already started running
        # or its pools open: run the ordinary shutdown (every close is a no-op
        # for something never opened), then report the original failure.
        try:
            await stop_application(resources, tasks)
        except Exception:
            logger.exception("Cleanup after the failed startup also failed")
        raise
    yield
    await stop_application(resources, tasks)
