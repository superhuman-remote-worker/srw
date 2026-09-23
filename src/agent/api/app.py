"""FastAPI application for Universal Agent.

Provides HTTP endpoints for health checks, agent status, and orchestrator
integration. Jobs are received from the orchestrator via /job/start endpoint.
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from uuid import uuid4

from fastapi import FastAPI, HTTPException, BackgroundTasks

from agent.api.models import (
    HealthResponse,
    HealthStatus,
    ReadyResponse,
    AgentStatusResponse,
    ErrorResponse,
    MetricsResponse,
    JobStartRequest,
    JobStartResponse,
    JobCancelByOrchestratorRequest,
    JobResumeRequest,
    PinnedJobRecipient,
    pinned_job_recipient_matches,
)
from agent.api.job_stream import close_job_stream as _close_stream
from agent.api.orchestrator_client import (
    OrchestratorClient,
    create_orchestrator_client_from_env,
)
from agent.agent import UniversalAgent
from shared.runtime.core.loader import resolve_config_path
from agent.core.workspace import get_logs_path
from shared.runtime.core.workspace_backend import completion_error_payload
from shared.workspace_contract import (
    WorkspaceContractError,
    validate_worker_workspace_projection,
)
from shared.subagent_lifecycle import (
    SubagentLifecycleError,
)

logger = logging.getLogger(__name__)

# Global state
_agent: Optional[UniversalAgent] = None
_shutdown_requested = False
_config_path: Optional[str] = None

# Orchestrator integration state
_orchestrator_client: Optional[OrchestratorClient] = None
_heartbeat_task: Optional[asyncio.Task] = None
_current_job_id: Optional[str] = None
_current_job_task: Optional[asyncio.Task] = None
_pending_fatal_exit_task: Optional[asyncio.Task] = None

# Cooperative stop mechanism (checked between graph iterations)
# Used by both pause and cancel — _stop_reason discriminates the action.
_stop_requested: asyncio.Event = asyncio.Event()  # Signals streaming loop to break
_stop_reason: Optional[str] = None  # "pause" or "cancel"
_stop_completed: asyncio.Event = (
    asyncio.Event()
)  # Signals waiting endpoint that stop finished

_PINNED_RECIPIENT_MISMATCH = {"code": "pinned_recipient_mismatch"}
_JOB_STREAM_CLOSE_TIMEOUT_SECONDS = 60.0
_FATAL_EXIT_DEREGISTER_TIMEOUT_SECONDS = 5.0


async def _close_job_stream(streaming_gen: Any, *, job_id: str) -> None:
    """Finalize through one close owner with this app's timeout and logger."""
    await _close_stream(
        streaming_gen,
        job_id=job_id,
        timeout_seconds=_JOB_STREAM_CLOSE_TIMEOUT_SECONDS,
        logger=logger,
    )


def _schedule_fatal_exit(delay: float = 1.0) -> None:
    """Retire this pinned owner after an unrecoverable local lifecycle fault."""

    global _pending_fatal_exit_task
    if _pending_fatal_exit_task and not _pending_fatal_exit_task.done():
        return

    async def _exit() -> None:
        await asyncio.sleep(delay)
        client = _orchestrator_client
        if client is not None:
            client.stop_heartbeat()
            heartbeat = _heartbeat_task
            if (
                heartbeat is not None
                and not heartbeat.done()
                and heartbeat is not asyncio.current_task()
            ):
                heartbeat.cancel()
            if client.agent_id:
                try:
                    await asyncio.wait_for(
                        client.deregister(),
                        timeout=_FATAL_EXIT_DEREGISTER_TIMEOUT_SECONDS,
                    )
                except Exception:
                    logger.exception(
                        "Pinned owner deregistration failed before fatal exit"
                    )
        os._exit(1)

    _pending_fatal_exit_task = asyncio.create_task(
        _exit(), name="pinned-subagent-lifecycle-fatal-exit"
    )


async def _fail_closed_subagent_lifecycle(job_id: str) -> None:
    """Make an unsettled pinned parent non-dispatchable, then retire it.

    No completion report is emitted.  The current process first closes local
    child work without writes, advertises draining, and then deregisters/exits
    so the orchestrator can recover the still-processing parent on a successor.
    """

    global _shutdown_requested
    _shutdown_requested = True
    # Arm bounded owner retirement before any potentially wedged cleanup or
    # transport await.  Cancellation/hung abandon must not preserve this owner.
    _schedule_fatal_exit()
    logger.critical(
        "Pinned job child lifecycle could not settle; draining owner without "
        "completion report: job=%s",
        job_id,
    )
    agent = _agent
    abandon = getattr(agent, "abandon_worker_subagents", None)
    if callable(abandon):
        try:
            await abandon("pinned parent lifecycle failed")
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Pinned child runtime abandon retry failed")
    client = _orchestrator_client
    if client is not None and client.agent_id:
        try:
            await client.heartbeat(
                status="draining",
                job_id=job_id,
                metrics=_get_agent_metrics(),
            )
        except Exception:
            logger.exception("Could not publish pinned lifecycle drain heartbeat")


def _require_pinned_job_recipient(
    recipient: Optional[PinnedJobRecipient],
    *,
    job_id: Optional[str],
) -> None:
    """Fail closed unless this exact registered process owns the mutation."""

    client = _orchestrator_client
    if not pinned_job_recipient_matches(
        recipient,
        agent_id=getattr(client, "agent_id", None),
        pod_uid=os.environ.get("POD_UID"),
        process_generation=getattr(client, "dispatch_process_generation", None),
        job_id=job_id,
    ):
        raise HTTPException(status_code=409, detail=_PINNED_RECIPIENT_MISMATCH)


def _request_stop(reason: str) -> None:
    """Request cooperative stop. Cancel overrides a pending pause (higher severity)."""
    global _stop_reason
    if _stop_reason == "cancel" and reason == "pause":
        return  # Don't downgrade cancel to pause
    _stop_reason = reason
    _stop_completed.clear()
    _stop_requested.set()


def _clear_stop() -> None:
    """Reset all stop state for a new job."""
    global _stop_reason
    _stop_reason = None
    _stop_requested.clear()
    _stop_completed.clear()


# Statuses that mean "this job is no longer ours to run". Deliberately a
# DENY-list: an unrecognised or new status leaves the run alone (fail-open),
# because wrongly stopping a healthy run is worse than a late stop, and the
# push signal remains the fast path either way.
_PREEMPTED_JOB_STATUSES = {
    "failed": "cancel",
    "cancelled": "cancel",
    "paused": "pause",
}


def _on_heartbeat_response(response: dict) -> None:
    """Stop the run if the orchestrator says our job was taken away.

    The heartbeat used to be one-directional — the agent asserted liveness and
    learned nothing back. So when something terminated a job out-of-band, the
    running agent never found out: job c6dd288d kept streaming for 21 minutes
    and 45 LLM calls after it had been failed, and only noticed when its VM was
    collected underneath it, which it reasonably misread as "my workspace died"
    rather than "my job was killed".

    The orchestrator's push stop signal stays the fast path. This is the
    backstop for the 13+ call sites that can write a terminal status without
    sending one, and it costs no new call — the heartbeat already runs on the
    right cadence.
    knowledge-base/knowledge/issues/transient_db_error_hard_fails_job_and_destroys_vm.md (Defect 3)
    """
    job_id = _current_job_id
    if not job_id or _stop_requested.is_set():
        return
    if not isinstance(response, dict):
        return
    job_status = response.get("job_status")
    reason = _PREEMPTED_JOB_STATUSES.get(job_status)
    if reason is None:
        # Includes job_status=None (older orchestrator, or the lookup failed) —
        # degrade to the previous push-only behaviour rather than guessing.
        return
    logger.warning(
        "Job %s is '%s' on the orchestrator but this agent is still running it "
        "— stopping (reason=%s). The job was terminated out-of-band; work since "
        "then was not attributable to it.",
        job_id,
        job_status,
        reason,
    )
    _request_stop(reason)


def set_config_path(path: str) -> None:
    """Set the configuration path for the agent.

    Call this before creating the app to specify which config to use.
    """
    global _config_path
    _config_path = path


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager.

    Handles startup and shutdown of the agent and its components.
    Agents receive jobs from orchestrator (ORCHESTRATOR_URL defaults to localhost:8085).
    """
    global _agent, _shutdown_requested
    global _orchestrator_client, _heartbeat_task

    # Startup
    logger.info("Starting Universal Agent application...")

    # Get config path from environment or global setting
    config_path = _config_path or os.getenv("AGENT_CONFIG", "worker_base")
    resolved_path, deployment_dir = resolve_config_path(config_path)

    logger.info(f"Loading agent configuration from: {resolved_path}")

    # Create and initialize agent - pass original config_path, not resolved tuple
    _agent = UniversalAgent.from_config(config_path)
    await _agent.initialize()

    # NOTE: experts no longer resolve agent-side. The orchestrator emits a fully
    # resolved config blob (services/config_resolver.py) delivered via the
    # session-attach payload; the agent is a pure executor. from_config(config_name)
    # above is the migration fallback when no blob is present.

    # Register with orchestrator and start heartbeat
    _orchestrator_client = create_orchestrator_client_from_env(_agent.config.agent_id)

    logger.info("Registering with orchestrator...")
    await _orchestrator_client.connect()

    # Attempt initial registration (non-fatal if it fails)
    if await _orchestrator_client.register():
        logger.info("Registered with orchestrator")
    else:
        logger.warning("Initial registration failed - will keep retrying in background")

    # Always start the heartbeat loop (handles registration retries too)
    _heartbeat_task = asyncio.create_task(
        _orchestrator_client.run_heartbeat_loop(
            get_status=_get_agent_status_for_heartbeat,
            get_job_id=_get_current_job_id,
            get_metrics=_get_agent_metrics,
            on_response=_on_heartbeat_response,
        )
    )
    logger.info("Orchestrator heartbeat loop started")

    yield

    # Shutdown — graceful drain with cooperative stop
    logger.info("Shutting down Universal Agent application...")
    _shutdown_requested = True

    # Phase 1: Send immediate "draining" heartbeat so the orchestrator
    # stops assigning new jobs (don't wait for the next heartbeat tick)
    if _orchestrator_client and _orchestrator_client.agent_id:
        try:
            await _orchestrator_client.heartbeat(
                status="draining",
                job_id=_current_job_id,
                metrics=_get_agent_metrics(),
            )
        except Exception:
            pass  # Best-effort; stale detector will catch us if this fails

    # Phase 2: If a job is running, use cooperative stop and notify
    # the orchestrator to pause it (so it re-enters the dispatch queue).
    releasing_job_id = None
    if _current_job_task and not _current_job_task.done():
        releasing_job_id = _current_job_id
        logger.info("Requesting cooperative stop for graceful shutdown...")
        _request_stop("pause")

        try:
            await asyncio.wait_for(_stop_completed.wait(), timeout=120.0)
            logger.info("Job stopped cooperatively during shutdown")
        except asyncio.TimeoutError:
            logger.warning("Cooperative stop timed out after 120s — hard cancelling")
            _current_job_task.cancel()
            try:
                await _current_job_task
            except asyncio.CancelledError:
                pass

    # Phase 2b: Tell the orchestrator to pause the job we just released.
    # This is essential — without it the job stays in 'processing' and
    # relies on the deregister → ON DELETE SET NULL → recover_orphaned_jobs
    # chain which can fail if the orchestrator is also restarting.
    if releasing_job_id and _orchestrator_client:
        try:
            await _orchestrator_client.report_pause(releasing_job_id)
        except Exception:
            logger.warning(
                f"Could not notify orchestrator to pause job {releasing_job_id} — "
                "stale detector will recover it"
            )

    # Phase 3: Stop heartbeat loop and deregister
    if _orchestrator_client:
        logger.info("Stopping orchestrator heartbeat and deregistering...")
        _orchestrator_client.stop_heartbeat()

        if _heartbeat_task:
            _heartbeat_task.cancel()
            try:
                await _heartbeat_task
            except asyncio.CancelledError:
                pass

        await _orchestrator_client.deregister()
        await _orchestrator_client.close()

    if _agent:
        await _agent.shutdown()

    logger.info("Universal Agent application shutdown complete")


def _get_agent_status_for_heartbeat() -> str:
    """Get current agent status for heartbeat reporting."""
    if _shutdown_requested:
        return "draining"

    if _agent is None:
        return "booting"

    if _current_job_id is not None:
        return "working"

    status = _agent.get_status()
    if not status.get("initialized"):
        return "booting"

    return "ready"


def _get_current_job_id() -> Optional[str]:
    """Get current job ID for heartbeat reporting."""
    return _current_job_id


def _get_agent_metrics() -> Optional[Dict[str, Any]]:
    """Get agent metrics for heartbeat reporting."""
    metrics: Dict[str, Any] = {}
    try:
        graph_progress = (
            _agent._tool_context.get_graph_progress()
            if _agent is not None and _agent._tool_context is not None
            else None
        )
        if graph_progress is not None:
            metrics["graph_progress"] = graph_progress
    except Exception:
        pass

    try:
        import psutil

        process = psutil.Process()
        listening = [
            c for c in psutil.net_connections(kind="inet") if c.status == "LISTEN"
        ]
        metrics.update(
            {
                "memory_mb": process.memory_info().rss / (1024 * 1024),
                "cpu_percent": process.cpu_percent(),
                "listening_ports": len(listening),
                "process_count": len(psutil.pids()),
            }
        )
    except ImportError:
        pass  # psutil not installed
    except Exception as e:
        logger.debug(f"Failed to collect metrics: {e}")

    # Auxiliary-task health → the orchestrator persists the degraded flag and
    # surfaces an admin badge (aux Phase 2). Best-effort; never fail heartbeat.
    aux = _aux_health_for_heartbeat()
    if aux is not None:
        metrics["aux"] = aux

    # Contained memory-store failures (deadlock containment). Best-effort;
    # never fail heartbeat.
    memory = _memory_health_for_heartbeat()
    if memory is not None:
        metrics["memory"] = memory

    return metrics or None


def _memory_health_for_heartbeat() -> Optional[Dict[str, Any]]:
    """Contained memory-store failure counters for the heartbeat.

    Returns None while every counter is zero so the healthy common case adds
    no payload (and the orchestrator persists nothing new).
    """
    try:
        from shared.runtime.services.recall_store import memory_health

        return memory_health.snapshot()
    except Exception:
        return None


def _aux_health_for_heartbeat() -> Optional[Dict[str, Any]]:
    """Compact auxiliary-model health for the heartbeat (aux Phase 2).

    Returns None when no auxiliary LLM is wired yet (e.g. booting) so the
    orchestrator leaves any previously-persisted ``aux_degraded`` untouched
    rather than clearing it on incomplete information.
    """
    aux_llm = getattr(_agent, "_auxiliary_llm", None) if _agent is not None else None
    if aux_llm is None:
        return None
    try:
        return aux_llm.health.heartbeat_summary()
    except Exception:
        return None


def _collect_system_info() -> Dict[str, Any]:
    """Collect comprehensive system information for the /system/info endpoint."""
    import psutil

    # CPU
    cpu_info = {
        "percent": psutil.cpu_percent(interval=0.1),
        "cores": psutil.cpu_count(),
    }

    # Memory
    mem = psutil.virtual_memory()
    memory_info = {
        "total_mb": round(mem.total / (1024 * 1024)),
        "used_mb": round(mem.used / (1024 * 1024)),
        "percent": mem.percent,
    }

    # Disk
    disk = psutil.disk_usage("/")
    disk_info = {
        "total_gb": round(disk.total / (1024**3), 1),
        "used_gb": round(disk.used / (1024**3), 1),
        "percent": disk.percent,
    }

    # Listening ports
    listening_ports = []
    try:
        for c in psutil.net_connections(kind="inet"):
            if c.status == "LISTEN":
                listening_ports.append(
                    {
                        "port": c.laddr.port,
                        "address": c.laddr.ip,
                        "pid": c.pid,
                    }
                )
    except (psutil.AccessDenied, PermissionError):
        pass

    # Top 20 processes by memory
    processes = []
    try:
        for proc in sorted(
            psutil.process_iter(
                ["pid", "name", "cmdline", "memory_info", "cpu_percent"]
            ),
            key=lambda p: (p.info.get("memory_info") or type("", (), {"rss": 0})).rss,
            reverse=True,
        )[:20]:
            info = proc.info
            mem_info = info.get("memory_info")
            cmdline = info.get("cmdline") or []
            processes.append(
                {
                    "pid": info["pid"],
                    "name": info.get("name", ""),
                    "cmd": " ".join(cmdline[:5]) if cmdline else "",
                    "memory_mb": round(mem_info.rss / (1024 * 1024), 1)
                    if mem_info
                    else 0,
                    "cpu_percent": info.get("cpu_percent", 0),
                }
            )
    except (psutil.AccessDenied, PermissionError):
        pass

    # Established TCP connections (limit 50)
    network_connections = []
    try:
        for c in psutil.net_connections(kind="inet"):
            if c.status == "ESTABLISHED" and len(network_connections) < 50:
                network_connections.append(
                    {
                        "local": f"{c.laddr.ip}:{c.laddr.port}" if c.laddr else "",
                        "remote": f"{c.raddr.ip}:{c.raddr.port}" if c.raddr else "",
                        "pid": c.pid,
                    }
                )
    except (psutil.AccessDenied, PermissionError):
        pass

    # Agent info
    agent_info: Dict[str, Any] = {"agent_id": None, "current_job": None}
    if _agent:
        agent_info["agent_id"] = _agent.config.agent_id
    agent_info["current_job"] = _current_job_id

    return {
        "cpu": cpu_info,
        "memory": memory_info,
        "disk": disk_info,
        "listening_ports": listening_ports,
        "processes": processes,
        "network_connections": network_connections,
        "agent": agent_info,
    }


class _FlushingFileHandler(logging.FileHandler):
    """File handler that flushes after every emit for crash safety."""

    def emit(self, record):
        super().emit(record)
        self.flush()


def _setup_job_file_logging(job_id: str) -> Path:
    """Set up file logging for a specific job in server mode.

    Creates a log file in the logs directory. Uses FlushingFileHandler
    to ensure logs are written immediately for crash safety.

    Args:
        job_id: The job identifier

    Returns:
        Path to the log file
    """
    logs_dir = get_logs_path()  # Creates workspace/logs/ if needed
    log_file = logs_dir / f"job_{job_id}.log"

    # Get level from env var
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    # Create flushing file handler for crash safety
    file_handler = _FlushingFileHandler(log_file, mode="a")
    file_handler.setLevel(level)
    from agent.core.logging_config import build_formatter

    # Same formatter as stdout — JSON in-cluster (LOG_FORMAT=json).
    file_handler.setFormatter(build_formatter(component="agent"))

    # Add to root logger
    root_logger = logging.getLogger()
    root_logger.addHandler(file_handler)

    logger.info(f"Job log file: {log_file}")

    return log_file


def _cleanup_job_file_handler(job_id: str) -> None:
    """Remove job-specific file handler from root logger.

    Args:
        job_id: The job identifier
    """
    root = logging.getLogger()
    for handler in root.handlers[:]:
        if isinstance(handler, logging.FileHandler):
            if f"job_{job_id}.log" in str(handler.baseFilename):
                handler.close()
                root.removeHandler(handler)


# NOTE: Verification config, status determination, critic verdict handling,
# and verification job spawning have been removed from the agent.
# The orchestrator is the single authority for these — see
# orchestrator/services/completion.py and orchestrator/main.py complete_job().


async def _process_orchestrator_job(
    job_id: str,
    description: str,
    upload_id: Optional[str] = None,
    config_upload_id: Optional[str] = None,
    instructions_upload_id: Optional[str] = None,
    document_path: Optional[str] = None,
    document_dir: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
    instructions: Optional[str] = None,
    config_name: Optional[str] = None,
    expert_id: Optional[str] = None,
    config_override: Optional[Dict[str, Any]] = None,
    resolved_config: Optional[Dict[str, Any]] = None,
    git_remote_url: Optional[str] = None,
    datasources: Optional[list] = None,
    repositories: Optional[list] = None,
    managed_repository_credentials: Optional[list] = None,
    branch_name: Optional[str] = None,
    project_id: Optional[str] = None,
    runtime_actor: Optional[Dict[str, Any]] = None,
) -> None:
    """Process a job assigned by the orchestrator.

    This runs in the background after accepting a job from the orchestrator.
    Uses streaming mode with iteration logging and per-job file logging.
    """
    global _current_job_id

    if _agent is None:
        logger.error("Cannot process job - agent not initialized")
        return

    from agent.core.logging_config import bind_log_context, reset_log_context

    # Tag every log line for this job with job_id (correlation).
    _log_token = bind_log_context(job_id=job_id)
    try:
        # Set up per-job file logging for crash safety
        _setup_job_file_logging(job_id)

        logger.info(f"Starting orchestrator job {job_id}")

        # Build metadata
        metadata: Dict[str, Any] = {"description": description}
        if upload_id:
            metadata["upload_id"] = upload_id
        if config_upload_id:
            metadata["config_upload_id"] = config_upload_id
        if instructions_upload_id:
            metadata["instructions_upload_id"] = instructions_upload_id
        if document_path:
            metadata["document_path"] = document_path
        if document_dir:
            metadata["document_dir"] = document_dir
        if context:
            metadata.update(context)
        if instructions:
            metadata["instructions"] = instructions
        if config_name and config_name != "worker_base":
            metadata["config_name"] = config_name
        if expert_id:
            metadata["expert_id"] = expert_id
        if config_override:
            metadata["config_override"] = config_override
        if resolved_config:
            metadata["resolved_config"] = resolved_config
        if git_remote_url:
            metadata["git_remote_url"] = git_remote_url
        if datasources:
            metadata["datasources"] = datasources
        if repositories:
            metadata["repositories"] = repositories
        if managed_repository_credentials:
            metadata["managed_repository_credentials"] = managed_repository_credentials
        if branch_name:
            metadata["branch_name"] = branch_name
        if project_id:
            metadata["project_id"] = project_id
        if runtime_actor:
            metadata["runtime_actor"] = runtime_actor

        # Reset stop flags for this job
        _clear_stop()

        # Inject orchestrator client so delegation tools can reach the API
        _agent._orchestrator_client = _orchestrator_client

        # Process the job with streaming for iteration logging
        final_state = None
        last_iteration = "?"
        streaming_gen = await _agent.process_job(job_id, metadata, stream=True)
        try:
            async for state in streaming_gen:
                final_state = state
                if isinstance(state, dict):
                    iteration = state.get("iteration")
                    if iteration is not None:
                        last_iteration = iteration
                    has_error = state.get("error") is not None
                    logger.info(
                        f"[Iteration {last_iteration}] job={job_id} error={has_error}"
                    )

                # Cooperative stop check: exit after the current node completes
                if _stop_requested.is_set():
                    logger.info(
                        f"Stop requested ({_stop_reason}) for job {job_id} — stopping after current node"
                    )
                    break
        finally:
            await _close_job_stream(streaming_gen, job_id=job_id)

        # Handle cooperative stop (pause or cancel) vs normal completion.
        # NOTE: The orchestrator already set the DB status (cancelled/paused)
        # before sending the stop signal — no DB write needed here.
        if _stop_requested.is_set():
            reason = _stop_reason
            _clear_stop()
            logger.info(f"Job {job_id} stopped gracefully (reason={reason})")
            _current_job_id = None
            _stop_completed.set()  # Signal the waiting endpoint
            _cleanup_job_file_handler(job_id)
            return

        result = final_state or {}
        logger.info(f"Orchestrator job {job_id} completed: {result.get('should_stop')}")

        # Report completion FIRST, then mark the agent available. The reverse
        # order was a live race: a ready(job_id=None) heartbeat lets
        # recover_orphaned_jobs pause this still-'processing' job ("agent says
        # ready" — no current_job_id check, no grace), after which the
        # completion report is DISCARDED with a 400 (the rescue path is
        # failed-only) and the dispatcher re-runs the finished job. The 30s
        # dispatcher cooldown means slot availability is not on the critical
        # path, so reporting first costs nothing.
        # knowledge-base/knowledge/research/stateless_agents/gate3_adversarial_review.md (B8).
        _current_job_id = None
        if _orchestrator_client:
            try:
                await _orchestrator_client.report_completion(
                    job_id,
                    result,
                    agent_id=_orchestrator_client.agent_id,
                )
            except Exception as e:
                logger.error(f"Failed to report completion for job {job_id}: {e}")

        if _orchestrator_client and _orchestrator_client.agent_id:
            await _orchestrator_client.heartbeat(
                status="ready",
                job_id=None,
                metrics=_get_agent_metrics(),
            )

    except asyncio.CancelledError:
        logger.info(f"Orchestrator job {job_id} was cancelled")
        raise
    except SubagentLifecycleError:
        await _fail_closed_subagent_lifecycle(job_id)
    except Exception as e:
        logger.error(f"Orchestrator job {job_id} failed: {e}", exc_info=True)
        # Report error to orchestrator
        error_result = completion_error_payload(e)
        if _orchestrator_client:
            try:
                await _orchestrator_client.report_completion(
                    job_id,
                    error_result,
                    agent_id=_orchestrator_client.agent_id,
                )
            except Exception:
                logger.error(f"Failed to report error for job {job_id}")
    finally:
        # Only clear if this job still owns the slot — a new job may
        # have been dispatched while post-completion handlers were running.
        if _current_job_id == job_id:
            _current_job_id = None
        _cleanup_job_file_handler(job_id)
        reset_log_context(_log_token)


def create_app(config_path: Optional[str] = None) -> FastAPI:
    """Create the FastAPI application.

    Args:
        config_path: Path to agent config file (or name like "creator")

    Returns:
        Configured FastAPI application
    """
    if config_path:
        set_config_path(config_path)

    app = FastAPI(
        title="Universal Agent API",
        description="REST API for the Universal Agent",
        version="1.0.0",
        lifespan=lifespan,
    )

    # Health endpoints

    @app.get("/health", response_model=HealthResponse, tags=["Health"])
    async def health_check() -> HealthResponse:
        """Liveness probe - check if the service is running."""
        if _agent is None:
            return HealthResponse(
                status=HealthStatus.UNHEALTHY,
                agent_id="unknown",
                agent_name="Unknown",
                uptime_seconds=0,
                checks={"initialized": False},
            )

        status = _agent.get_status()

        # Determine health status
        health = HealthStatus.HEALTHY
        if not status["initialized"]:
            health = HealthStatus.UNHEALTHY
        elif not status["connections"]["postgres"]:
            health = HealthStatus.DEGRADED

        return HealthResponse(
            status=health,
            agent_id=status["agent_id"],
            agent_name=status["display_name"],
            uptime_seconds=status["uptime_seconds"],
            checks={
                "initialized": status["initialized"],
                "postgres": status["connections"].get("postgres", False),
            },
        )

    @app.get("/ready", response_model=ReadyResponse, tags=["Health"])
    async def readiness_check() -> ReadyResponse:
        """Readiness probe - check if the service can accept requests."""
        if _shutdown_requested:
            return ReadyResponse(
                ready=False,
                message="Agent is draining (shutting down)",
                connections={},
            )

        if _agent is None:
            return ReadyResponse(
                ready=False,
                message="Agent not initialized",
                connections={},
            )

        status = _agent.get_status()

        # Ready if initialized and has required connections
        ready = status["initialized"] and status["connections"]["postgres"]

        return ReadyResponse(
            ready=ready,
            message="Ready to accept jobs" if ready else "Not ready",
            connections=status["connections"],
            capabilities={
                "resolved_config_resume": True,
                "pinned_recipient_binding": True,
            },
        )

    @app.get("/status", response_model=AgentStatusResponse, tags=["Health"])
    async def agent_status() -> AgentStatusResponse:
        """Get detailed agent status."""
        if _agent is None:
            raise HTTPException(status_code=503, detail="Agent not initialized")

        status = _agent.get_status()
        return AgentStatusResponse(**status)

    # Metrics endpoint

    @app.get("/metrics", response_model=MetricsResponse, tags=["Monitoring"])
    async def get_metrics() -> MetricsResponse:
        """Get agent metrics for monitoring."""
        if _agent is None:
            raise HTTPException(status_code=503, detail="Agent not initialized")

        status = _agent.get_status()

        # Get job statistics from database
        jobs_success = 0
        jobs_failed = 0

        if _agent.postgres_conn:
            try:
                success_count = await _agent.postgres_conn.fetchval(
                    "SELECT COUNT(*) FROM jobs WHERE status = 'complete'"
                )
                failed_count = await _agent.postgres_conn.fetchval(
                    "SELECT COUNT(*) FROM jobs WHERE status = 'failed'"
                )
                jobs_success = success_count or 0
                jobs_failed = failed_count or 0
            except Exception as e:
                logger.warning(f"Error fetching metrics: {e}")

        return MetricsResponse(
            agent_id=status["agent_id"],
            timestamp=datetime.utcnow(),
            jobs_total=status["jobs_processed"],
            jobs_success=jobs_success,
            jobs_failed=jobs_failed,
            average_duration_seconds=None,  # TODO: Calculate from job history
            current_iterations=0,  # Would need to track in agent
            uptime_seconds=status["uptime_seconds"],
        )

    # =========================================================================
    # System Monitoring
    # =========================================================================

    @app.get("/system/info", tags=["Monitoring"])
    async def system_info() -> Dict[str, Any]:
        """Get comprehensive system information for container monitoring.

        Returns CPU, memory, disk usage, listening ports, top processes,
        established network connections, and agent state.
        """
        try:
            return _collect_system_info()
        except ImportError:
            raise HTTPException(
                status_code=501,
                detail="psutil not installed — system monitoring unavailable",
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/system/shell-state", tags=["Monitoring"])
    async def shell_state(recipient: PinnedJobRecipient) -> Dict[str, Any]:
        """Get current shell tab state including recent output.

        Returns the list of open terminal tabs with their type and
        recent output lines. Useful for inspecting what the agent is
        doing in its shell sessions.
        """
        _require_pinned_job_recipient(recipient, job_id=_current_job_id)

        if _agent is None:
            return {"tabs": [], "message": "Agent not initialized"}

        shell_manager = getattr(_agent, "_shell_manager", None)
        if shell_manager is None:
            return {"tabs": [], "message": "No active shell sessions"}

        try:
            tab_list = shell_manager.list_tabs()
            tabs = []
            for tab_meta in tab_list:
                name = tab_meta.get("name", "unknown")
                try:
                    read_result = shell_manager.read_with_offset(name, lines=30)
                    recent_output = (
                        read_result.get("output", "")
                        if isinstance(read_result, dict)
                        else str(read_result)
                    )
                    total_lines = (
                        read_result.get("total_lines", 0)
                        if isinstance(read_result, dict)
                        else 0
                    )
                except Exception:
                    recent_output = ""
                    total_lines = 0

                tabs.append(
                    {
                        "name": name,
                        "type": tab_meta.get("type", "unknown"),
                        "created_at": tab_meta.get("created_at", ""),
                        "total_lines": total_lines,
                        "recent_output": recent_output,
                    }
                )

            return {"tabs": tabs}
        except Exception:
            error_ref = uuid4().hex[:12]
            logger.exception("Failed to get shell state (error_ref=%s)", error_ref)
            return {
                "tabs": [],
                "message": "Error: shell state unavailable",
                "error_ref": error_ref,
            }

    # =========================================================================
    # Orchestrator Integration Endpoints
    # =========================================================================

    @app.post(
        "/job/start",
        response_model=JobStartResponse,
        status_code=202,
        tags=["Orchestrator"],
        responses={
            409: {"model": ErrorResponse, "description": "Agent is busy"},
            503: {"model": ErrorResponse, "description": "Agent not initialized"},
        },
    )
    async def start_job_from_orchestrator(
        request: JobStartRequest,
        background_tasks: BackgroundTasks,
    ) -> JobStartResponse:
        """Receive and start a job from the orchestrator.

        This endpoint is called by the orchestrator to assign a job to this agent.
        The job is processed in the background and the endpoint returns immediately
        with a 202 Accepted status.
        """
        global _current_job_id, _current_job_task

        _require_pinned_job_recipient(request.recipient, job_id=request.job_id)

        if _agent is None:
            raise HTTPException(status_code=503, detail="Agent not initialized")

        if _shutdown_requested:
            raise HTTPException(status_code=503, detail="Agent is shutting down")

        from agent.api.pinned_delivery import accepted_pinned_job_delivery

        if _current_job_id == request.job_id:
            acknowledgement = accepted_pinned_job_delivery(
                request, _orchestrator_client, retry=True,
            )
            return JobStartResponse(
                job_id=request.job_id,
                status="accepted",
                message="Job was already accepted by this runtime",
                **acknowledgement,
            )

        try:
            validate_worker_workspace_projection(
                config_override=request.config_override,
                resolved_config=request.resolved_config,
                workspace_runtime=request.workspace_runtime,
            )
        except WorkspaceContractError as exc:
            raise HTTPException(status_code=409, detail={"code": exc.code}) from exc

        # Check if already processing a job
        if _current_job_id is not None:
            raise HTTPException(
                status_code=409,
                detail=f"Agent is busy processing job {_current_job_id}",
            )

        acknowledgement = accepted_pinned_job_delivery(
            request, _orchestrator_client, retry=False,
        )

        # Accept the job — reset stop state
        _current_job_id = request.job_id
        _clear_stop()

        # Start processing in background
        start_context = dict(request.context or {})
        start_context["workspace_runtime"] = request.workspace_runtime
        for field in (
            "workspace_provisioner",
            "workspace_generation",
            "workspace_runtime_incarnation",
            "workspace_ssh_host_key_fingerprint",
            "workspace_owner_kind",
            "workspace_owner_id",
        ):
            value = getattr(request, field)
            if value:
                start_context[field] = value
        _current_job_task = asyncio.create_task(
            _process_orchestrator_job(
                job_id=request.job_id,
                description=request.description,
                upload_id=request.upload_id,
                config_upload_id=request.config_upload_id,
                instructions_upload_id=request.instructions_upload_id,
                document_path=request.document_path,
                document_dir=request.document_dir,
                context=start_context,
                instructions=request.instructions,
                config_name=request.config_name,
                expert_id=request.expert_id,
                config_override=request.config_override,
                resolved_config=request.resolved_config,
                git_remote_url=request.git_remote_url,
                datasources=request.datasources,
                repositories=request.repositories,
                managed_repository_credentials=(request.managed_repository_credentials),
                branch_name=request.branch_name,
                project_id=request.project_id,
                runtime_actor=request.runtime_actor,
            )
        )

        logger.info(f"Accepted job {request.job_id} from orchestrator")

        return JobStartResponse(
            job_id=request.job_id,
            status="accepted",
            message="Job processing started",
            **acknowledgement,
        )

    @app.post(
        "/job/cancel",
        tags=["Orchestrator"],
        responses={
            404: {"model": ErrorResponse, "description": "No job running"},
            408: {
                "model": ErrorResponse,
                "description": "Cancel timed out (hard-killed)",
            },
        },
    )
    async def cancel_current_job(
        request: Optional[JobCancelByOrchestratorRequest] = None,
    ) -> Dict[str, Any]:
        """Cancel the currently running job (cooperative with hard-kill fallback).

        Sets a cooperative flag checked between graph node executions.
        The agent finishes its current node, LangGraph saves the checkpoint,
        then processing stops. If the agent doesn't stop within 120 seconds,
        falls back to a hard cancel (task.cancel()).
        """
        global _current_job_id, _current_job_task

        if _current_job_id is None:
            raise HTTPException(
                status_code=404,
                detail="No job currently running",
            )

        _require_pinned_job_recipient(
            request.recipient if request is not None else None,
            job_id=_current_job_id,
        )

        job_id = _current_job_id
        reason = (
            request.reason if request is not None else None
        ) or "Cancelled by orchestrator"

        # Signal the streaming loop to stop after the current node
        _request_stop("cancel")

        logger.info(f"Cancel requested for job {job_id} — waiting for graceful stop")

        # Wait for cooperative stop (with timeout)
        try:
            await asyncio.wait_for(_stop_completed.wait(), timeout=120.0)
            logger.info(f"Job {job_id} cancelled gracefully: {reason}")
            return {
                "job_id": job_id,
                "status": "cancelled",
                "reason": reason,
                "graceful": True,
            }
        except asyncio.TimeoutError:
            # Cooperative stop failed — fall back to hard kill
            logger.warning(f"Graceful cancel timed out for job {job_id} — hard killing")

            if _current_job_task and not _current_job_task.done():
                _current_job_task.cancel()
                try:
                    await _current_job_task
                except asyncio.CancelledError:
                    pass

            _current_job_id = None
            _current_job_task = None
            _clear_stop()

            return {
                "job_id": job_id,
                "status": "cancelled",
                "reason": f"{reason} (hard-killed after timeout)",
                "graceful": False,
            }

    @app.post(
        "/job/pause",
        tags=["Orchestrator"],
        responses={
            404: {"model": ErrorResponse, "description": "No job running"},
            408: {"model": ErrorResponse, "description": "Pause timed out"},
        },
    )
    async def pause_current_job(
        request: Optional[JobCancelByOrchestratorRequest] = None,
    ) -> Dict[str, Any]:
        """Gracefully pause the currently running job.

        Sets a cooperative flag that is checked between graph node executions.
        The agent finishes its current node, LangGraph saves the checkpoint,
        then processing stops and the agent becomes available.

        Waits up to 120 seconds for the job to actually pause.
        """
        if _current_job_id is None:
            raise HTTPException(
                status_code=404,
                detail="No job currently running",
            )

        _require_pinned_job_recipient(
            request.recipient if request is not None else None,
            job_id=_current_job_id,
        )

        job_id = _current_job_id

        # Signal the streaming loop to stop after the current node
        _request_stop("pause")

        logger.info(f"Pause requested for job {job_id} — waiting for graceful stop")

        # Wait for the job to actually pause (with timeout)
        try:
            await asyncio.wait_for(_stop_completed.wait(), timeout=120.0)
        except asyncio.TimeoutError:
            # The flag persists, so the job will still pause after the current node
            # finishes — but we can't wait any longer
            logger.warning(
                f"Pause timed out for job {job_id} — flag still set, will pause after current node"
            )
            raise HTTPException(
                status_code=408,
                detail=f"Pause timed out after 120s. Job {job_id} will pause after current node completes.",
            )

        return {
            "job_id": job_id,
            "status": "paused",
        }

    @app.post(
        "/job/resume",
        response_model=JobStartResponse,
        status_code=202,
        tags=["Orchestrator"],
        responses={
            409: {"model": ErrorResponse, "description": "Agent is busy"},
            503: {"model": ErrorResponse, "description": "Agent not initialized"},
        },
    )
    async def resume_job(
        request: JobResumeRequest,
        background_tasks: BackgroundTasks,
    ) -> JobStartResponse:
        """Resume a job from last completed phase snapshot.

        This endpoint resumes a previously started job from its last phase snapshot.
        Optional feedback can be injected before resuming.
        """
        global _current_job_id, _current_job_task

        _require_pinned_job_recipient(request.recipient, job_id=request.job_id)

        if _agent is None:
            raise HTTPException(status_code=503, detail="Agent not initialized")

        if _shutdown_requested:
            raise HTTPException(status_code=503, detail="Agent is shutting down")

        if _current_job_id == request.job_id:
            return JobStartResponse(
                job_id=request.job_id,
                status="accepted",
                message="Job resume was already accepted by this runtime",
            )

        try:
            validate_worker_workspace_projection(
                config_override=request.config_override,
                resolved_config=request.resolved_config,
                workspace_runtime=request.workspace_runtime,
            )
        except WorkspaceContractError as exc:
            raise HTTPException(status_code=409, detail={"code": exc.code}) from exc

        # Log config mismatch as warning (don't reject - checkpoint discovery handles it)
        if request.config_name and request.config_name != _agent.config.agent_id:
            logger.warning(
                f"Config mismatch: job has config '{request.config_name}' but this agent is '{_agent.config.agent_id}'. "
                f"Will attempt to discover correct checkpoint."
            )

        # Check if already processing a job
        if _current_job_id is not None:
            raise HTTPException(
                status_code=409,
                detail=f"Agent is busy processing job {_current_job_id}",
            )

        # Accept the resume request
        _current_job_id = request.job_id

        # Capture for closure
        feedback = request.feedback
        config_name = request.config_name
        previous_status = request.previous_status

        # Build metadata with config info for resume
        resume_metadata = {}
        if request.config_upload_id:
            resume_metadata["config_upload_id"] = request.config_upload_id
        if request.config_override:
            resume_metadata["config_override"] = request.config_override
        if request.resolved_config:
            resume_metadata["resolved_config"] = request.resolved_config
        if request.datasources:
            resume_metadata["datasources"] = request.datasources
        if request.repositories:
            resume_metadata["repositories"] = request.repositories
        if request.managed_repository_credentials:
            resume_metadata["managed_repository_credentials"] = (
                request.managed_repository_credentials
            )
        if request.project_id:
            resume_metadata["project_id"] = request.project_id
        if request.runtime_actor:
            resume_metadata["runtime_actor"] = request.runtime_actor
        resume_metadata["workspace_runtime"] = request.workspace_runtime
        for field in (
            "workspace_provisioner",
            "workspace_generation",
            "workspace_runtime_incarnation",
            "workspace_ssh_host_key_fingerprint",
            "workspace_owner_kind",
            "workspace_owner_id",
        ):
            value = getattr(request, field)
            if value:
                resume_metadata[field] = value
        if request.git_remote_url:
            # Feeds the pod-handoff clone fallback in _setup_job_workspace
            # (resume_fresh_workspace_no_clone_fallback.md).
            resume_metadata["git_remote_url"] = request.git_remote_url

        # Start processing in background
        async def _resume_job():
            global _current_job_id
            _setup_job_file_logging(request.job_id)
            _clear_stop()
            _agent._orchestrator_client = _orchestrator_client
            try:
                # Use streaming for cooperative stop support (pause/cancel)
                final_state = None
                streaming_gen = await _agent.process_job(
                    request.job_id,
                    metadata=resume_metadata if resume_metadata else None,
                    resume=True,
                    feedback=feedback,
                    original_config_name=config_name,
                    previous_status=previous_status,
                    stream=True,
                )
                last_iteration = "?"
                try:
                    async for state in streaming_gen:
                        final_state = state
                        if isinstance(state, dict):
                            iteration = state.get("iteration")
                            if iteration is not None:
                                last_iteration = iteration
                            logger.info(
                                f"[Resume iteration {last_iteration}] job={request.job_id}"
                            )

                        # Cooperative stop check
                        if _stop_requested.is_set():
                            logger.info(
                                f"Stop requested ({_stop_reason}) for resumed job {request.job_id}"
                            )
                            break
                finally:
                    await _close_job_stream(streaming_gen, job_id=request.job_id)

                # Handle cooperative stop vs normal completion.
                # NOTE: The orchestrator already set the DB status (cancelled/paused)
                # before sending the stop signal — no DB write needed here.
                if _stop_requested.is_set():
                    reason = _stop_reason
                    _clear_stop()
                    logger.info(
                        f"Resumed job {request.job_id} stopped gracefully (reason={reason})"
                    )
                    _current_job_id = None
                    _stop_completed.set()
                    _cleanup_job_file_handler(request.job_id)
                    return

                result = final_state or {}
                logger.info(
                    f"Resumed job {request.job_id} completed: {result.get('should_stop')}"
                )

                # Report completion FIRST, then mark available — same race as
                # the primary job path (see the comment there; review B8).
                _current_job_id = None
                if _orchestrator_client:
                    try:
                        await _orchestrator_client.report_completion(
                            request.job_id,
                            result,
                            agent_id=_orchestrator_client.agent_id,
                        )
                    except Exception as e:
                        logger.error(
                            f"Failed to report completion for resumed job {request.job_id}: {e}"
                        )

                if _orchestrator_client and _orchestrator_client.agent_id:
                    await _orchestrator_client.heartbeat(
                        status="ready",
                        job_id=None,
                        metrics=_get_agent_metrics(),
                    )

            except asyncio.CancelledError:
                logger.info(f"Resumed job {request.job_id} was cancelled")
                raise
            except SubagentLifecycleError:
                await _fail_closed_subagent_lifecycle(request.job_id)
            except Exception as e:
                logger.error(f"Resumed job {request.job_id} failed: {e}", exc_info=True)
                error_result = completion_error_payload(e)
                if _orchestrator_client:
                    try:
                        await _orchestrator_client.report_completion(
                            request.job_id,
                            error_result,
                            agent_id=_orchestrator_client.agent_id,
                        )
                    except Exception:
                        logger.error(
                            f"Failed to report error for resumed job {request.job_id}"
                        )
            finally:
                # Only clear if this job still owns the slot — a new job may
                # have been dispatched while post-completion handlers were running.
                if _current_job_id == request.job_id:
                    _current_job_id = None
                _cleanup_job_file_handler(request.job_id)

        _current_job_task = asyncio.create_task(_resume_job())

        logger.info(f"Accepted resume request for job {request.job_id}")

        return JobStartResponse(
            job_id=request.job_id,
            status="accepted",
            message="Job resume started",
        )

    @app.get("/job/current", tags=["Orchestrator"])
    async def get_current_job() -> Dict[str, Any]:
        """Get information about the currently running job."""
        return {
            "job_id": _current_job_id,
            "is_busy": _current_job_id is not None,
        }

    return app


# Default app instance (uses environment config)
app = create_app()
