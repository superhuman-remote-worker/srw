"""Agent Implementation.

This Agent is a configurable, workspace-centric autonomous agent
that can be deployed as Creator, Validator, or any future agent type by
changing its configuration file.

Key Features:
- Config-driven behavior from JSON files
- Workspace-centric architecture with filesystem for strategic planning
- TodoManager for tactical execution with archiving
- Dynamic tool loading based on configuration
- Simplified 4-node LangGraph workflow
"""

import asyncio
import logging
import math
import os
import time
import zipfile
from dataclasses import asdict, replace as _dc_replace
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Tuple

import aiosqlite
from langchain_core.language_models import BaseChatModel
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from langgraph.checkpoint.base import BaseCheckpointSaver

from shared.runtime.core.loader import (
    AgentConfig,
    LLMConfig,
    load_agent_config,
    load_config_from_resolved,
    create_llm,
    get_all_tool_names,
    resolve_config_path,
    resolve_model_settings,
    supports_parallel_tool_calls,
)
from shared.runtime.core.loader import get_project_root
from agent.core.phase_snapshot import PhaseSnapshotManager
from agent.core.state import UniversalAgentState, create_initial_state
from agent.core.workspace import (
    WorkspaceManager,
    WorkspaceManagerConfig,
    get_checkpoints_path,
)
from shared.runtime.core.workspace_backend import WorkspaceUnavailableError
from agent.graph import (
    WORKER_BATCH_MIN_WALL_SECONDS,
    build_phase_alternation_graph,
    hydrate_todo_manager_from_state,
    run_graph_with_streaming,
)
from agent.managers import TodoManager
from shared.job_freeze_types import AUTO_CONTINUE_FREEZE_TYPES
from shared.subagent_lifecycle import (
    SubagentAbandonError,
    SubagentLifecycleError,
    SubagentQuiescenceError,
    SubagentRecoveryError,
)
from agent.tools import ToolContext, load_tools, apply_instruction_enforcement
from agent.tools.description_manager import (
    apply_description_overrides,
    apply_phase_description_prefixes,
)
from shared.db_url import (
    checkpointer_backend,
    resolve_checkpoint_url,
    resolve_fenced_checkpoint_url,
)

# Set True once per agent process after the Postgres checkpoint schema has been
# ensured. AsyncPostgresSaver.setup() is idempotent, but there's no need to run
# it on every job.
_PG_CHECKPOINT_SCHEMA_READY = False


def _remote_workspace_authority(
    metadata: Dict[str, Any],
    worker_lease_token: Optional[int],
    *,
    backend: str,
) -> Dict[str, Any]:
    """Build exact Kubernetes SSH authority for stateless and pinned workers."""

    provisioner = str(metadata.get("workspace_provisioner") or "").strip().lower()
    if backend == "sandbox" and provisioner not in {"k8s", "docker"}:
        raise WorkspaceUnavailableError(
            "A sandbox worker requires server-derived workspace provisioner authority"
        )
    requires_exact_k8s_authority = worker_lease_token is not None or (
        backend == "sandbox" and provisioner == "k8s"
    )
    if not requires_exact_k8s_authority:
        return {}
    fields = {
        "workspace_generation": metadata.get("workspace_generation"),
        "runtime_incarnation": metadata.get("workspace_runtime_incarnation"),
        "expected_host_key_fingerprint": metadata.get(
            "workspace_ssh_host_key_fingerprint"
        ),
        "workspace_owner_kind": metadata.get("workspace_owner_kind"),
        "workspace_owner_id": metadata.get("workspace_owner_id"),
    }
    if any(
        not isinstance(value, str) or not value.strip() for value in fields.values()
    ):
        raise WorkspaceUnavailableError(
            "A Kubernetes worker requires an orchestrator-attested workspace "
            "owner, backing, runtime incarnation, and SSH host identity"
        )
    if fields["workspace_owner_kind"] != "job":
        raise WorkspaceUnavailableError(
            "A worker requires a job-owned workspace authority"
        )
    fields["require_host_key_fingerprint"] = True
    return fields  # RemoteBackend performs canonical UUID/fingerprint validation.


def _stateless_worker_remote_authority(
    metadata: Dict[str, Any], worker_lease_token: Optional[int]
) -> Dict[str, Any]:
    """Compatibility wrapper for focused stateless authority tests."""

    if worker_lease_token is None:
        return {}
    return _remote_workspace_authority(
        metadata,
        worker_lease_token,
        backend="sandbox",
    )


# >>> TEMPORARY QUICKFIX (2026-07-30) — delete with the upstream fix.
# knowledge-history/done/codex_stream_disconnect_shape_nudge.md
# Injected as a user turn when the orchestrator has seen N byte-identical
# upstream rejections of the SAME payload. Its only job is to make the next
# request differ, so the wording is secondary to its existence — but it has
# three jobs beyond that, and each earns its line:
#   1. It must not read as a real instruction, or the agent re-plans.
#   2. It must not imply the agent erred (it did not — openai/codex#9995).
#   3. It must say NOTHING WAS LOST. The freeze point is side-effect-clean (the
#      LLM call failed, so the tools node never ran), but an agent that is not
#      told so will burn a turn on git_status/read_file re-verifying a workspace
#      that never changed.
# Formatted as a "## heading + prose" notice, like every other message this
# codebase injects into a running conversation.
_SHAPE_NUDGE_TEXT = (
    "## Transport Notice\n"
    "\n"
    "The model provider closed the response stream on the previous request — "
    "repeatedly, and identically each time. This is a known fault on their "
    "side. It is not a problem with your work, your plan, or your last tool "
    "call.\n"
    "\n"
    "Nothing was lost. The failure happened before the model replied, so no "
    "tool ran and no file, commit, or todo changed. The workspace is exactly "
    "as you left it — there is nothing to verify or repair.\n"
    "\n"
    "This message exists only so the retried request is no longer "
    "byte-identical to the one being rejected. Ignore it and carry on exactly "
    "where you left off: do not restart, re-plan, redo completed work, or "
    "reply to this message."
)


def _merge_worker_resume_updates(
    target: Dict[str, Any], incoming: Dict[str, Any]
) -> None:
    """Merge staged resume state without clobbering reducer-backed messages."""

    prior_messages = list(target.get("messages") or [])
    incoming_messages = list(incoming.get("messages") or [])
    target.update(incoming)
    if prior_messages or incoming_messages:
        target["messages"] = [*prior_messages, *incoming_messages]


def _valid_worker_batch_arm(values: Dict[str, Any]) -> bool:
    """Whether a pending update checkpoint carries a complete arm envelope."""

    fields = {
        "worker_batch_started_at",
        "worker_batch_start_iteration",
        "worker_batch_target_wall_seconds",
        "worker_batch_min_wall_seconds",
        "worker_batch_iteration_cap",
    }
    if not fields.issubset(values):
        return False

    started_at = values.get("worker_batch_started_at")
    start_iteration = values.get("worker_batch_start_iteration")
    target = values.get("worker_batch_target_wall_seconds")
    floor = values.get("worker_batch_min_wall_seconds")
    cap = values.get("worker_batch_iteration_cap")
    numeric_values = (started_at, target, floor)
    if any(isinstance(value, bool) for value in numeric_values):
        return False
    try:
        started_at_value, target_value, floor_value = map(float, numeric_values)
    except (TypeError, ValueError):
        return False
    finite_values = (
        started_at_value,
        target_value,
        floor_value,
    )
    if not all(math.isfinite(value) for value in finite_values):
        return False
    if started_at_value <= 0 or target_value <= 0 or floor_value < 0:
        return False
    if isinstance(start_iteration, bool) or not isinstance(start_iteration, int):
        return False
    if cap is not None and (
        isinstance(cap, bool) or not isinstance(cap, int) or cap <= 0
    ):
        return False
    return True


class _AiosqliteConnectionWrapper:
    """Wrapper for aiosqlite.Connection that adds is_alive() method.

    langgraph-checkpoint-sqlite 3.x expects connections to have is_alive(),
    but aiosqlite.Connection doesn't provide it. This wrapper adds compatibility.
    """

    def __init__(self, conn: aiosqlite.Connection):
        self._conn = conn

    def is_alive(self) -> bool:
        """Check if connection is alive (always True for established connections)."""
        return True

    def __getattr__(self, name):
        """Delegate all other attributes to the wrapped connection."""
        return getattr(self._conn, name)


logger = logging.getLogger(__name__)


# The branch a job's work belongs on when the job row carries no explicit
# ``branch_name``. NULL does not mean "any branch is fine" — it means the job
# lives on the repo's default branch, which is exactly how every *reader*
# resolves it (``job.get("branch_name") or "main"`` in
# orchestrator/services/diff_source.py, deliverable_gate.py, ide_session.py,
# job_cloud_baseline.py and orchestrator/main.py).
DEFAULT_JOB_BRANCH = "main"


def ensure_job_branch(
    git_mgr,
    metadata: Optional[Dict[str, Any]],
    job_id: str = "",
    create: bool = False,
):
    """Put a re-attached working tree back on the branch this job owns.

    Only the paths that attach to a *pre-existing* tree need this. A fresh
    clone already lands on the branch it was told to check out, but a workspace
    that is re-attached (PVC reattach) or resumed in place keeps whatever branch
    the previous occupant left checked out — and the previous occupant may have
    been a *subjob*. Critic/scholar subjobs run on
    ``subjob/<short_id>/<config>`` branches (orchestrator/services/
    job_provisioning.py:164), so a parent resumed after one of them can silently
    continue on the subjob's branch. Its commits then push to that branch and
    ``main`` never advances, while every reader (critic, cockpit, MCP,
    ``get_workspace_file``, and any later re-clone) still reads ``main`` and
    correctly reports the work missing.

    See knowledge-history/done/resumed_job_inherits_subjob_git_branch.md — job
    6df02f64, where a ``## Sources`` append was committed and pushed to
    ``subjob/50dee4ae/critic`` and was never visible on ``main``.

    Never raises: a workspace we cannot re-point is still usable, and failing
    the job over it would be worse than the drift. Failures are logged at
    WARNING because this whole bug class is defined by its silence.

    Returns:
        The branch the tree is on afterwards, or None if it could not be read.
    """
    if git_mgr is None:
        return None

    # A NULL branch_name is the *standalone job* case, not an opt-out: that job
    # owns the default branch. Treating it as "leave the tree wherever it is" is
    # what let a parent inherit a critic subjob's branch.
    expected = (metadata or {}).get("branch_name") or DEFAULT_JOB_BRANCH

    try:
        current = git_mgr.current_branch()
        if current == expected:
            return current
        if not git_mgr.checkout_branch(expected, create=create):
            logger.warning(
                f"[{job_id}] Resume: could not switch workspace from branch "
                f"{current!r} to {expected!r} — commits will land on {current!r} "
                f"and {expected!r} will not advance"
            )
            return current
        logger.info(f"[{job_id}] Switched to expected branch: {expected}")
        return expected
    except Exception as e:  # noqa: BLE001 - never fail a resume over branch drift
        logger.warning(f"[{job_id}] Resume: branch ensure failed: {e}")
        return None


class UniversalAgent:
    """
    Configurable autonomous agent using workspace-centric architecture.

    The Universal Agent reads its behavior from a JSON configuration file,
    enabling a single implementation to serve as Creator, Validator, or
    any other agent type.

    Architecture:
    - Strategic Planning: Filesystem-based plans in workspace/plans/
    - Tactical Execution: TodoManager with next_phase_todos()
    - Context Management: Automatic compaction and summarization
    - Tool Loading: Dynamic based on config.tools
    """

    def __init__(
        self,
        config: AgentConfig,
        postgres_conn: Optional[Any] = None,
    ):
        """
        Initialize the Universal Agent.

        Args:
            config: Agent configuration (from JSON file)
            postgres_conn: PostgreSQL connection (optional, created if needed)
        """
        self.config = config
        self._base_config = config  # Immutable snapshot for reset between jobs
        self.postgres_conn = postgres_conn

        # Components (initialized lazily or via initialize())
        self._llm: Optional[BaseChatModel] = None
        self._llm_with_tools: Optional[BaseChatModel] = None
        self._tools: Optional[List] = None
        self._graph = None
        self._checkpointer: Optional[BaseCheckpointSaver] = None
        self._checkpoint_conn: Optional[Any] = None
        # Non-None only while the stateless worker driver owns an immutable
        # worker_batch lease.  The saver and remote shell both bind to it.
        self._worker_lease_token: Optional[int] = None
        self._defer_job_cleanup = False
        self._worker_checkpoint_post_commit = None
        self._worker_env_restore: Dict[str, Optional[str]] = {}
        # A durable completion accept closes run_queue before its background
        # finalizer has chosen the job disposition.  During that gap the
        # worker must make every local tool inert while retaining the exact
        # shell/backend handles needed to enact a later terminal outcome.
        self._worker_finalization_held = False
        self._worker_finalization_backend: Any | None = None
        self._worker_terminal_shell_cleanup: Callable[[], None] | None = None
        self._worker_shell_admission_retired = False
        # The runtime itself also fences these transitions, but retaining the
        # exact object identity here makes the agent-side lifecycle idempotent:
        # cleanup has several defensive call sites and must never terminalize a
        # child generation twice.
        self._subagent_recovered_runtime: Any | None = None
        self._subagent_quiesced_runtime: Any | None = None
        self._subagent_abandoned_runtime: Any | None = None

        # Phase-specific LLMs (created if phase overrides configured)
        self._strategic_llm: Optional[BaseChatModel] = None
        self._tactical_llm: Optional[BaseChatModel] = None
        self._summarization_llm: Optional[BaseChatModel] = None
        self._strategic_llm_with_tools: Optional[BaseChatModel] = None
        self._tactical_llm_with_tools: Optional[BaseChatModel] = None

        # Auxiliary LLM for support tasks (summarization, memory extraction, curation)
        self._auxiliary_llm = None

        # Citation verification (Phase 2): an AuxiliaryLLM on the citation model
        # (dedicated CITATION_LLM model, or the auxiliary model fallback) + the
        # matrix-resolved prompt. Threaded onto ToolContext so the citation
        # engine schedules async verdict write-back.
        self._citation_verify_aux = None
        self._citation_verification_prompt = ""

        # Tool context (for phase-aware behavior)
        self._tool_context: Optional[ToolContext] = None

        # Current job state
        self._workspace_manager: Optional[WorkspaceManager] = None
        self._todo_manager: Optional[TodoManager] = None
        self._current_job_id: Optional[str] = None
        self._job_metadata: Optional[Dict[str, Any]] = None
        # Upload-sourced instructions.md content, resolved eagerly by
        # _resolve_uploaded_instructions() because a virtual-file provider's
        # read() is synchronous and the download is not. None means "no
        # upload for this job" (or not yet resolved) — the instructions
        # provider then falls back to inline metadata, then the template.
        self._resolved_instructions_md: Optional[str] = None
        # Agent-authored workspace files (path → content) that a pod re-provision
        # would drop (bound skills). Re-asserted on SSH reconnect via
        # RemoteBackend's on_reconnect hook. See
        # knowledge-base/knowledge/issues/reviewing_parent_pod_reaped_under_critic.md (Issue 4).
        # instructions.md / task_brief.md used to be re-asserted here too, but
        # a virtual file cannot be lost, so re-assertion narrows to genuinely
        # seeded real files (knowledge-base/knowledge/features/virtual_directories.md).
        self._agent_seed_files: Dict[str, str] = {}
        self._datasource_connections: Dict[str, Any] = {}
        self._datasource_clients: Dict[
            str, Any
        ] = {}  # Parent clients for cleanup (e.g. MongoClient)
        # Manifest of materialized credential files (kubeconfig / ssh_key /
        # generic_file). Populated by process_credential_files() at job start;
        # consumed by cleanup_credential_files() at job end.
        self._datasource_files_manifest: Optional[Dict[str, Any]] = None

        # Orchestrator client (injected by app layer for delegation/reporting)
        self._orchestrator_client = None

        # Persistent shell sessions (tmux-backed)
        self._shell_manager = None

        # Background document registration task
        self._doc_registration_task: Optional[asyncio.Task] = None

        # Knowledge base connection (for inline curation)
        self._knowledge_graph = None

        # Control flags
        self._initialized = False
        self._shutdown_requested = False

        # Metrics
        self._jobs_processed = 0
        self._start_time = datetime.utcnow()

        logger.info(f"Created {config.display_name} (agent_id={config.agent_id})")

    @property
    def agent_id(self) -> str:
        """Get the agent ID."""
        return self.config.agent_id

    @property
    def display_name(self) -> str:
        """Get the display name."""
        return self.config.display_name

    @classmethod
    def from_config(
        cls,
        config_path: str,
        postgres_conn: Optional[Any] = None,
    ) -> "UniversalAgent":
        """
        Create an agent from a configuration file.

        Args:
            config_path: Path to config file or config name (e.g., "creator")
            postgres_conn: Optional PostgreSQL connection

        Returns:
            UniversalAgent instance
        """
        resolved_path, deployment_dir = resolve_config_path(config_path)
        config = load_agent_config(resolved_path, deployment_dir)
        return cls(config, postgres_conn)

    @classmethod
    def from_resolved(
        cls,
        resolved_config: dict,
        postgres_conn: Optional[Any] = None,
    ) -> "UniversalAgent":
        """Create an agent from an orchestrator-resolved config blob.

        The blob (``serialize_resolved_config`` shape) is already fully merged
        and frozen by the orchestrator — bundled base + expert + override layers
        + settings matrix, with resolved prompts/instructions inline. No disk or
        DB resolution happens here: ``load_config_from_resolved`` hydrates the
        ``AgentConfig`` and seeds ``config.extra['_resolved_prompts'/
        '_resolved_instructions']`` so the render path uses the frozen text (and
        fences a DB persona via the ``_persona_source`` marker). This supersedes
        agent-side resolution (Decision 6); ``from_config`` remains the fallback
        when no blob is delivered.

        Args:
            resolved_config: Resolved config blob from the orchestrator.
            postgres_conn: Optional PostgreSQL connection.

        Returns:
            UniversalAgent instance
        """
        config = load_config_from_resolved(resolved_config)
        return cls(config, postgres_conn)

    async def initialize(self) -> None:
        """
        Initialize the agent and its components.

        This must be called before processing jobs. Sets up:
        - Database connections (if not provided)
        - LLM instance
        - Context manager
        - Base tools (workspace, to-do)

        Raises:
            RuntimeError: If required connections cannot be established
        """
        if self._initialized:
            logger.warning("Agent already initialized")
            return

        logger.info(f"Initializing {self.config.display_name}...")

        # Set up database connections if needed
        await self._setup_connections()

        # Create LLM(s) with context limit validation (Layer 0 safety)
        self._create_phase_llms()

        self._initialized = True
        logger.info(f"{self.config.display_name} initialized successfully")

    def _create_phase_llms(self) -> None:
        """Create the job's LLM client(s) from the single ``llm.model``.

        U1 collapsed the strategic/tactical tiers: every phase runs the one
        configured model, so ``_llm`` / ``_strategic_llm`` / ``_tactical_llm``
        are the same client (the two phase attributes stay until U2 collapses
        the phase bindings). ``llm.summarization`` is the only remaining
        override — it gets its own client only when it resolves to a config
        that differs from the main model's.
        """
        llm_config = self.config.llm
        limits = self.config.limits
        # Reset each call (idempotent across boot-time + per-job recreation).
        # Still the warning surface read when the resolved config is frozen,
        # even though the phase-model reconciliation that used to fill it went
        # with the tiers.
        self._model_config_warnings: List[str] = []

        self._llm = create_llm(llm_config, limits=limits)
        self._strategic_llm = self._llm
        self._tactical_llm = self._llm
        logger.info(f"Created LLM for all phases: {llm_config.model}")

        summarization_config = llm_config.get_phase_config("summarization")
        if summarization_config is llm_config or summarization_config == _dc_replace(
            llm_config, summarization=None
        ):
            # No override, or one that resolves to the main model — reuse the
            # client rather than opening a second one to the same endpoint.
            self._summarization_llm = self._llm
            logger.info(f"Summarization LLM: reusing main ({llm_config.model})")
        else:
            self._summarization_llm = create_llm(summarization_config, limits=limits)
            logger.info(f"Created summarization LLM: {summarization_config.model}")

        # Create AuxiliaryLLM for support tasks (summarization, memory, curation)
        self._initialize_auxiliary_llm(llm_config, limits)
        # Citation verifier (Phase 2) — built on the aux LLM, so after it.
        self._initialize_citation_verifier(limits)

    def _initialize_auxiliary_llm(self, llm_config, limits) -> None:
        """Create the AuxiliaryLLM instance for support tasks.

        Uses auxiliary.model/base_url if configured, otherwise falls back
        to the summarization LLM (which itself falls back to strategic LLM).

        Rebuild-safe: ``_setup_job_workspace`` recreates the phase LLMs for
        every dispatched job (credential-injected ``config_override`` makes the
        config dirty; the frozen-blob branch recreates too), which lands HERE
        and used to replace ``self._auxiliary_llm`` with a fresh instance whose
        ``set_job_context`` wiring was lost. process_job wires the archiver
        BEFORE that rebuild, so every worker aux call (memory extraction,
        assembly, the rest) ran with ``_archiver=None`` — real provider spend
        with no ``llm_requests`` row and no metering. Found by the lane-ab-01
        bench: pinned jobs stored observer memories with zero audited
        extraction calls. Any rebuild must therefore carry the previous
        instance's job wiring forward; ``_wire_aux_job_context`` below does.
        """
        from shared.runtime.services.auxiliary import AuxiliaryLLM

        _prev_aux = self._auxiliary_llm

        aux_config = self.config.auxiliary
        # The summarizer's budgeting authority: the aux model's own window when
        # a dedicated model is configured, else the main working window (the
        # fallback summarizer IS the main/summarization LLM there). See
        # knowledge-base/knowledge/features/context_summarization_rework.md (S1).
        main_window = getattr(limits, "model_max_context_tokens", None)
        summarization_config = llm_config.get_phase_config("summarization")
        summarization_settings = resolve_model_settings(
            summarization_config.model, self.config._deployment_dir
        )
        summarization_structured_output_method = summarization_settings.get(
            "structured_output_method", "json_schema"
        )

        if not aux_config.enabled:
            # Wrap summarization LLM as fallback even when auxiliary is disabled
            self._auxiliary_llm = AuxiliaryLLM(
                llm=self._summarization_llm,
                max_context_tokens=main_window,
                structured_output_method=summarization_structured_output_method,
            )
            self._wire_aux_job_context(_prev_aux, self._auxiliary_llm)
            logger.info("AuxiliaryLLM disabled, using summarization LLM as fallback")
            return

        if aux_config.model:
            # Dedicated auxiliary model — resolve settings matrix for its family
            model_settings = resolve_model_settings(
                aux_config.model, self.config._deployment_dir
            )
            # AuxiliaryConfig fields (temperature) take precedence;
            # settings matrix provides top_p, top_k, model_max_context_tokens
            aux_llm_config = LLMConfig(
                model=aux_config.model,
                base_url=aux_config.base_url,
                api_key=aux_config.api_key,
                provider=aux_config.provider,
                extra_headers=aux_config.extra_headers,
                temperature=aux_config.temperature,
                top_p=model_settings.get("top_p"),
                top_k=model_settings.get("top_k"),
                model_max_context_tokens=model_settings.get("model_max_context_tokens"),
                extra_body=model_settings.get("extra_body"),
                max_retries=1,
            )
            aux_llm = create_llm(aux_llm_config, limits=limits)
            aux_window = aux_llm_config.model_max_context_tokens or main_window
            # Drop-in fallback for a dead/unreachable dedicated aux model: the
            # summarization LLM (main working model). Keeps compaction + memory
            # alive instead of crashing the job when the aux endpoint fails.
            aux_fallback = self._summarization_llm
            aux_fallback_method = summarization_structured_output_method
            logger.info(
                f"Created auxiliary LLM: {aux_config.model}"
                f" (settings matrix: top_p={aux_llm_config.top_p},"
                f" top_k={aux_llm_config.top_k},"
                f" max_ctx={aux_llm_config.model_max_context_tokens})"
            )
            aux_structured_output_method = model_settings.get(
                "structured_output_method", "json_schema"
            )
        else:
            # Reuse summarization LLM (which is already the best fallback chain)
            aux_llm = self._summarization_llm
            aux_window = main_window
            # aux already IS the main model — nothing to fall back to.
            aux_fallback = None
            aux_fallback_method = None
            aux_structured_output_method = summarization_structured_output_method
            logger.info("AuxiliaryLLM: reusing summarization LLM")

        self._auxiliary_llm = AuxiliaryLLM(
            llm=aux_llm,
            max_iterations=aux_config.max_iterations,
            timeout=aux_config.timeout,
            max_context_tokens=aux_window,
            fallback_llm=aux_fallback,
            structured_output_method=aux_structured_output_method,
            fallback_structured_output_method=aux_fallback_method,
        )
        self._wire_aux_job_context(_prev_aux, self._auxiliary_llm)

    @staticmethod
    def _wire_aux_job_context(prev, new) -> None:
        """Copy a mid-job archiver wiring from a replaced AuxiliaryLLM.

        No-op at boot (no previous instance / no wiring yet). During a per-job
        LLM rebuild the previous instance carries the archiver + job identity
        that ``process_job`` wired before the rebuild; without this copy the
        replacement silently drops every auxiliary call from the audit trail
        and the cost pipeline (both read ``llm_requests``).
        """
        if prev is None or new is None or prev is new:
            return
        archiver = getattr(prev, "_archiver", None)
        job_id = getattr(prev, "_job_id", None)
        if archiver is None or not job_id:
            return
        new.set_job_context(
            archiver=archiver,
            job_id=job_id,
            agent_type=getattr(prev, "_agent_type", None) or "",
        )

    def _initialize_citation_verifier(self, limits) -> None:
        """Build the citation-verification AuxiliaryLLM (D6) + load its prompt.

        Uses a dedicated citation model when one is dispatched
        (``CITATION_LLM_MODEL`` — set by the orchestrator from the per-job
        override / Admin default), else reuses the auxiliary model. Gated by
        ``auxiliary.tasks.verify_citations``. The citation engine schedules
        verification as a background ``AuxiliaryLLM`` chain task (async,
        eventually-consistent).
        """
        import os

        from shared.runtime.core.transport_resolution import (
            CitationTransportError,
            resolve_citation_transport,
        )
        from shared.runtime.services.auxiliary import AuxiliaryLLM

        aux_cfg = self.config.auxiliary
        _prev_verify = self._citation_verify_aux
        task_cfg = aux_cfg.tasks.get("verify_citations")
        if not aux_cfg.enabled or task_cfg is None or not task_cfg.enabled:
            self._citation_verify_aux = None
            logger.info(
                "Citation verification disabled (auxiliary.tasks.verify_citations)"
            )
            return

        citation_model = os.getenv("CITATION_LLM_MODEL")
        if citation_model and self._auxiliary_llm is not None:
            # Dedicated citation model (D6) — resolve its family settings matrix.
            model_settings = resolve_model_settings(
                citation_model, self.config._deployment_dir
            )
            try:
                # Credential isolation: CITATION_LLM_API_KEY is the citation
                # client's key whenever it is set; OPENAI_API_KEY is only ever
                # used for api.openai.com itself. A custom endpoint without its
                # own key raises CitationTransportError (handled below) — never
                # a silent fallback to the chat model's key.
                transport = resolve_citation_transport()
                verify_cfg = LLMConfig(
                    model=citation_model,
                    base_url=transport.base_url,
                    api_key=transport.api_key,
                    temperature=0.0,
                    top_p=model_settings.get("top_p"),
                    top_k=model_settings.get("top_k"),
                    model_max_context_tokens=model_settings.get(
                        "model_max_context_tokens"
                    ),
                    extra_body=model_settings.get("extra_body"),
                    max_retries=1,
                )
                verify_llm = create_llm(verify_cfg, limits=limits)
                self._citation_verify_aux = AuxiliaryLLM(
                    llm=verify_llm,
                    structured_output_method=model_settings.get(
                        "structured_output_method", "json_schema"
                    ),
                    timeout=aux_cfg.timeout,
                    max_context_tokens=verify_cfg.model_max_context_tokens,
                )
                # Same mid-job rebuild hazard as _initialize_auxiliary_llm.
                self._wire_aux_job_context(_prev_verify, self._citation_verify_aux)
                prompt_model = citation_model
                logger.info(
                    "Citation verifier: dedicated model %s (endpoint=%s, key=%s)",
                    citation_model,
                    transport.base_url or "api.openai.com",
                    transport.key_source,
                )
            except CitationTransportError as e:
                logger.error(
                    "Citation verifier: %s — not building a dedicated citation "
                    "client for '%s'; falling back to the auxiliary model",
                    e,
                    citation_model,
                )
                self._citation_verify_aux = self._auxiliary_llm
                prompt_model = aux_cfg.model or self.config.llm.model
            except Exception as e:
                logger.warning(
                    f"Could not build dedicated citation model '{citation_model}' "
                    f"({e}); falling back to the auxiliary model"
                )
                self._citation_verify_aux = self._auxiliary_llm
                prompt_model = aux_cfg.model or self.config.llm.model
        else:
            # Fall back to the auxiliary model.
            self._citation_verify_aux = self._auxiliary_llm
            prompt_model = aux_cfg.model or self.config.llm.model
            logger.info("Citation verifier: reusing auxiliary model")

        # Resolve the verification prompt via the matrix (model-family aware).
        try:
            from shared.runtime.core.loader import load_auxiliary_prompt

            self._citation_verification_prompt = load_auxiliary_prompt(
                self.config, "citation_verification", model=prompt_model or ""
            )
        except Exception as e:
            logger.warning(f"Could not load citation_verification prompt: {e}")
            self._citation_verification_prompt = ""

    async def _setup_connections(self) -> None:
        """Set up required database connections.

        Falls back to environment variables for configuration.
        External datasources (Neo4j, MongoDB, etc.) are resolved per-job
        via the datasource connector system — see knowledge-base/knowledge/datasources.md.
        """
        from shared.db_url import build_postgres_url

        # PostgreSQL connection (always required for job management)
        if self.postgres_conn is None and self.config.connections.postgres:
            from agent.database.postgres_db import PostgresDB

            db_url = build_postgres_url("POSTGRES", fallback_env="DATABASE_URL")
            if db_url:
                self.postgres_conn = PostgresDB(connection_string=db_url)
                await self.postgres_conn.connect()
                logger.info("PostgreSQL connection established (PostgresDB)")
            else:
                logger.warning(
                    "Postgres credentials not set "
                    "(POSTGRES_USER/PASSWORD or DATABASE_URL), "
                    "PostgreSQL unavailable"
                )

        # Vector DB connection (for citations, memories + knowledge index)
        vector_url = build_postgres_url("VECTOR_POSTGRES", fallback_env="VECTOR_DB_URL")
        if vector_url:
            from agent.database.postgres_db import PostgresDB as _VectorDB

            self.vector_conn = _VectorDB(connection_string=vector_url)
            await self.vector_conn.connect()
            logger.info("Vector DB connection established (separate instance)")
        else:
            logger.warning(
                "Vector DB credentials not set "
                "(VECTOR_POSTGRES_USER/PASSWORD or VECTOR_DB_URL), "
                "vector features unavailable"
            )
            self.vector_conn = None

    async def process_job(
        self,
        job_id: str,
        metadata: Optional[Dict[str, Any]] = None,
        stream: bool = False,
        resume: bool = False,
        feedback: Optional[str] = None,
        feedback_reason: Optional[str] = None,
        original_config_name: Optional[str] = None,
        previous_status: Optional[str] = None,
        worker_lease_token: Optional[int] = None,
        worker_batch_target_wall_seconds: Optional[float] = None,
        worker_batch_min_wall_seconds: Optional[float] = None,
        worker_batch_iteration_cap: Optional[int] = None,
        worker_resume_id: Optional[str] = None,
        worker_retry_exhausted: bool = False,
        defer_cleanup: bool = False,
        worker_checkpoint_post_commit=None,
    ) -> Dict[str, Any]:
        """Process a single job.

        Creates a workspace for the job, loads tools, builds the graph,
        and executes until completion.

        Args:
            job_id: Unique job identifier
            metadata: Job-specific data (document_path, requirement_id, etc.)
            stream: If True, return an async iterator of state updates
            resume: If True, resume from last completed phase snapshot
            feedback: Optional feedback message to inject when resuming a frozen job
            feedback_reason: Why the job was resumed with feedback (rendered in
                the [FEEDBACK_RESUME] banner; None -> honest generic fallback)
            original_config_name: Original config name used when job was created
                (for legacy checkpoint lookup when resuming old jobs)
            previous_status: Job status before resume. Graceful stops (cancelled,
                paused, pending_review) skip snapshot recovery; crash states
                (processing, failed, None) use snapshot recovery.

        Returns:
            Final state dictionary with results

        Example:
            ```python
            result = await agent.process_job(
                job_id="job_123",
                metadata={"document_path": "/data/doc.pdf"}
            )
            print(result["should_stop"])  # True if completed
            ```
        """
        if not self._initialized:
            await self.initialize()

        stateless_worker = worker_lease_token is not None
        if stateless_worker:
            if int(worker_lease_token) <= 0:
                raise ValueError("worker_lease_token must be positive")
            if (
                getattr(self, "_worker_finalization_held", False)
                or getattr(self, "_worker_finalization_backend", None) is not None
                or getattr(self, "_worker_terminal_shell_cleanup", None) is not None
            ):
                raise RuntimeError(
                    "A new worker claim cannot replace a finalization-pending hold"
                )
            if (
                worker_batch_target_wall_seconds is None
                or float(worker_batch_target_wall_seconds) <= 0
            ):
                raise ValueError("worker batch target must be positive")
            self._worker_lease_token = int(worker_lease_token)
            self._defer_job_cleanup = bool(defer_cleanup)
            self._worker_checkpoint_post_commit = worker_checkpoint_post_commit
            self._worker_shell_admission_retired = False
        else:
            self._worker_lease_token = None
            self._defer_job_cleanup = False
            self._worker_checkpoint_post_commit = None

        # Reset config to base snapshot before applying per-job overrides
        self.config = self._base_config

        self._current_job_id = job_id
        self._job_metadata = metadata or {}
        if stateless_worker:
            self._capture_worker_environment(self._job_metadata)
        self._datasource_connections = {}
        self._datasource_clients = {}
        logger.info(f"Processing job {job_id}")

        # JobResumeRequest ships no description/deliverables/kickoff, so a
        # resumed job would serve an empty virtual task_brief.md for the rest
        # of its life. Backfill from the orchestrator/DB before the brief
        # provider registers (fresh_job_dispatched_as_resume_skips_seeding.md).
        if resume and not self._job_metadata.get("description"):
            await self._hydrate_job_brief(job_id)

        # Wire archiver + job context into AuxiliaryLLM for auxiliary call logging
        if self._auxiliary_llm:
            from agent.core.archiver import get_archiver as _get_archiver_for_aux

            self._auxiliary_llm.set_job_context(
                archiver=_get_archiver_for_aux(),
                job_id=job_id,
                agent_type=self.config.agent_id,
            )
            # The dedicated citation verifier (if distinct) needs the same
            # archiver/job context so its calls land in the debug view.
            if (
                self._citation_verify_aux is not None
                and self._citation_verify_aux is not self._auxiliary_llm
            ):
                self._citation_verify_aux.set_job_context(
                    archiver=_get_archiver_for_aux(),
                    job_id=job_id,
                    agent_type=self.config.agent_id,
                )

        try:
            # Create workspace for this job
            # Base path comes from WORKSPACE_PATH env var or defaults
            # This also copies documents to workspace and returns updated metadata
            updated_metadata = await self._setup_job_workspace(
                job_id, metadata, resume=resume
            )

            # Retired framework bookkeeping from P1-C/F13. Old job branches,
            # inherited project snapshots, and resumed workspaces may still
            # carry this tracked file. Remove it before the model receives its
            # workspace so a stale boundary snapshot cannot masquerade as live
            # deliverable state. The contract itself remains in task_brief.md;
            # job_complete and the orchestrator gate validate the real files.
            self._remove_legacy_manifest_status(job_id)

            # Handle frozen job resume. Backend-aware check: a local Path.exists()
            # never sees the marker on remote workspaces.
            if resume:
                if self._workspace_manager.exists("output/job_frozen.json"):
                    logger.info(f"Resuming frozen job {job_id}")
                    # Remove the frozen marker so the graph can continue
                    self._workspace_manager.delete_file("output/job_frozen.json")
                    logger.info("Removed job_frozen.json to allow continuation")
                    # NOTE: Status is set to 'processing' by the orchestrator
                    # when it dispatches/resumes the job — no DB write needed here.

            # Load tools for this job
            await self._setup_job_tools()
            if self._tool_context is not None:
                self._tool_context._stateless_worker = stateless_worker
                self._tool_context._worker_lease_token = (
                    int(worker_lease_token) if stateless_worker else None
                )

            # Phase 0: commit + push the fully seeded workspace so the job's
            # inputs (instructions, brief, documents, README) are visible in
            # the repo before the first phase archive commit.
            if not resume:
                self._commit_workspace_seed(job_id)

            # Fail-closed guard: a memory-required job must not run "blind" if its
            # embedding-backed stores failed to initialize (e.g. the dispatch
            # dropped EMBEDDING_API_KEY). Pause for bounded re-dispatch — the
            # orchestrator caps retries then fails — instead of silently running
            # with memory + KB disabled. See
            # knowledge-history/done/embedding_key_missing_silently_disables_memory_and_kb.md.
            _has_kb_scope = bool(
                getattr(getattr(self, "_tool_context", None), "knowledge_bindings", [])
            )
            _memory_missing = getattr(self, "_memory_degraded", False) or (
                _has_kb_scope and getattr(self, "_kb_degraded", False)
            )
            if self.config.memory.required and _memory_missing:
                logger.error(
                    f"[{job_id}] memory.required=true but the embedding-backed "
                    f"stores failed to initialize "
                    f"(memory_degraded={getattr(self, '_memory_degraded', False)}, "
                    f"kb_degraded={getattr(self, '_kb_degraded', False)}) — pausing "
                    f"for re-dispatch instead of running without memory/KB"
                )
                # Tool setup may already have installed a durable child
                # runtime.  Reconcile and settle it before shell/datasource
                # teardown or before returning a reportable freeze state.
                await self._settle_subagent_runtime(
                    "parent setup stopped: memory unavailable"
                )
                if not getattr(self, "_defer_job_cleanup", False):
                    self._cleanup_shell_manager()
                    self._close_datasource_connections()
                    await self._cleanup_checkpointer()
                    self._current_job_id = None
                freeze_state = {
                    "job_id": job_id,
                    "should_stop": True,
                    "freeze_data": {
                        "freeze_type": "memory_unavailable",
                        "reason": (
                            "Embedding service unavailable at startup — memory "
                            "(RecallStore)/KB failed to initialize and this job "
                            "requires memory (memory.required=true). Check the "
                            "embedding model/endpoint (Admin → Models)."
                        ),
                    },
                }
                if stream:
                    return self._yield_error_state(freeze_state)
                return freeze_state

            # Create checkpointer for this job (enables resume after crash, and
            # — with CHECKPOINTER_BACKEND=postgres — cross-pod resume).
            await self._make_checkpointer(job_id)

            # Create snapshot manager for phase recovery
            # Pass workspace backend so snapshots are extracted from VM to pod-local storage
            ws_backend = (
                self._workspace_manager.backend if self._workspace_manager else None
            )
            snapshot_manager = PhaseSnapshotManager(
                job_id,
                workspace_backend=ws_backend,
            )

            # Build graph for this job
            self._graph = build_phase_alternation_graph(
                llm_with_tools=self._llm_with_tools,
                tools=self._tools,
                config=self.config,
                workspace=self._workspace_manager,
                todo_manager=self._todo_manager,
                workspace_template="",
                checkpointer=self._checkpointer,
                auxiliary_llm=self._auxiliary_llm,
                snapshot_manager=snapshot_manager,
                tool_context=self._tool_context,
                postgres_db=self.postgres_conn,
            )

            # Execute graph
            # Use job_id as thread_id (new format), with fallback to legacy format for old jobs
            thread_id = job_id
            thread_config = {
                "configurable": {
                    "thread_id": thread_id,
                },
                "recursion_limit": 1000000,  # Effectively unlimited
            }

            # Check if we should resume from phase snapshot or use checkpoint directly
            # Graceful stops (cancel/pause/review) have a valid checkpoint.db with current todos,
            # so we skip snapshot recovery. Crash states need snapshot recovery because the
            # checkpoint may be corrupted or incomplete.
            GRACEFUL_STOP_STATUSES = {
                "cancelled",
                "paused",
                "pending_review",
                "waiting",
            }
            graph_input = None
            worker_terminal_state: Optional[Dict[str, Any]] = None
            if stateless_worker:
                # Shared Postgres is the canonical crash/rotation lane.  Never
                # rewind a stateless worker through the pod-local phase snapshot
                # fallback, including prior_status='processing' steals.
                checkpoint_state = await self._graph.aget_state(thread_config)
                if checkpoint_state and checkpoint_state.values:
                    graph_input = None
                    resume = True
                    logger.info(
                        "[%s] Stateless worker resuming canonical Postgres checkpoint",
                        job_id,
                    )
                else:
                    graph_input = create_initial_state(
                        job_id=job_id,
                        workspace_path=str(self._workspace_manager.path),
                        metadata=updated_metadata,
                    )
                    logger.info(
                        "[%s] Stateless worker found no checkpoint; starting fresh",
                        job_id,
                    )
            elif resume:
                is_graceful = previous_status in GRACEFUL_STOP_STATUSES
                if is_graceful:
                    logger.info(
                        f"Graceful resume from '{previous_status}' — using checkpoint directly "
                        f"(skipping snapshot recovery to preserve in-progress todos)"
                    )
                    # Use checkpoint.db as-is — discover thread_id and verify checkpoint exists
                    (
                        graph_input,
                        thread_id,
                        thread_config,
                    ) = await self._resume_from_checkpoint(
                        job_id,
                        thread_id,
                        thread_config,
                        original_config_name,
                        updated_metadata,
                    )
                    if graph_input is not None:
                        # Checkpoint lookup failed — fall back to snapshot recovery
                        logger.warning(
                            f"No valid checkpoint found for graceful resume from '{previous_status}', "
                            f"falling back to snapshot recovery"
                        )
                        (
                            graph_input,
                            thread_id,
                            thread_config,
                        ) = await self._resume_from_snapshot(
                            job_id,
                            snapshot_manager,
                            thread_id,
                            thread_config,
                            original_config_name,
                            updated_metadata,
                        )
                else:
                    # Crash/failure recovery — use snapshot (more reliable than potentially corrupted checkpoint)
                    if previous_status:
                        logger.info(
                            f"Crash recovery from '{previous_status}' — using snapshot recovery"
                        )
                    (
                        graph_input,
                        thread_id,
                        thread_config,
                    ) = await self._resume_from_snapshot(
                        job_id,
                        snapshot_manager,
                        thread_id,
                        thread_config,
                        original_config_name,
                        updated_metadata,
                    )
            else:
                # Fresh start - create initial state
                graph_input = create_initial_state(
                    job_id=job_id,
                    workspace_path=str(self._workspace_manager.path),
                    metadata=updated_metadata,
                )

            if resume and graph_input is not None:
                # Every lookup failed: resume=True with nothing to resume from.
                await self._note_resume_without_checkpoint(job_id, previous_status)

            checkpoint_values: Dict[str, Any] = {}
            if resume and graph_input is None:
                # Process-local TodoManager state is not reconstructed by
                # LangGraph itself.  A mid-loop checkpoint can resume at its
                # pending next node and bypass route_entry/restore_todo_state,
                # so hydrate it explicitly on every resume.
                snapshot = await self._graph.aget_state(thread_config)
                values = snapshot.values or {}
                checkpoint_values = dict(values)
                if hydrate_todo_manager_from_state(self._todo_manager, values):
                    logger.info(
                        "[%s] Hydrated TodoManager before checkpoint resume",
                        job_id,
                    )
                delivered_reply_keys = values.get("delivered_reply_keys") or []
                if (
                    stateless_worker
                    and self._tool_context is not None
                    and isinstance(delivered_reply_keys, list)
                ):
                    self._tool_context._delivered_reply_keys = {
                        str(key) for key in delivered_reply_keys if key is not None
                    }
                if stateless_worker and self._tool_context is not None:
                    restored_instruction_reads = self._restore_worker_instruction_reads(
                        values
                    )
                    if restored_instruction_reads:
                        logger.info(
                            "[%s] Restored %d checkpointed instruction-read receipt(s)",
                            job_id,
                            restored_instruction_reads,
                        )
                if stateless_worker and self._worker_checkpoint_post_commit:
                    checkpoint_id = ""
                    snapshot_config = getattr(snapshot, "config", None)
                    if isinstance(snapshot_config, dict):
                        configurable = snapshot_config.get("configurable")
                        if isinstance(configurable, dict):
                            checkpoint_id = str(configurable.get("checkpoint_id") or "")
                    try:
                        await self._worker_checkpoint_post_commit.reconcile_values(
                            values,
                            checkpoint_id=checkpoint_id,
                        )
                    except Exception:
                        logger.warning(
                            "[%s] Claim-time steering ack reconciliation failed; "
                            "a later checkpoint/claim will retry",
                            job_id,
                            exc_info=True,
                        )

            # Stateless routing and batch arming must be one durable update.
            # A second LangGraph update can consume the pending task selected
            # by feedback/auto-continue before any node runs.
            worker_resume_updates: Dict[str, Any] = {}
            worker_resume_as_node: Optional[str] = None

            # Inject feedback into graph state via aupdate_state. This sets
            # resume_feedback so route_entry routes to restore_from_feedback.
            if resume and feedback and (graph_input is None or stateless_worker):
                selected_node = await self._inject_resume_feedback(
                    job_id=job_id,
                    stateless_worker=stateless_worker,
                    graph_input=graph_input,
                    thread_config=thread_config,
                    checkpoint_values=checkpoint_values,
                    feedback=feedback,
                    feedback_reason=feedback_reason,
                    metadata=updated_metadata,
                    deferred_updates=(
                        worker_resume_updates
                        if stateless_worker and graph_input is None
                        else None
                    ),
                )
                if selected_node is not None:
                    worker_resume_as_node = selected_node

            # Auto-continue resume (version_upgrade / llm_unavailable / memory /
            # workspace-upgrade): a graceful re-dispatch with NO feedback. The
            # prior in-graph freeze persisted should_stop=True +
            # freeze_data in the checkpoint and the run reached END. ainvoke(None)
            # on an ended thread with should_stop=True runs ZERO nodes and returns
            # the terminal frozen state — so restore_todo_state (which clears the
            # stop flags on resume) never runs and the job re-freezes forever with
            # no progress. Pinned clears the terminal envelope here. Stateless
            # stages its clear + optional shape nudge so _arm_worker_batch can
            # commit clear + nudge + arm in ONE START update. Scoped to
            # auto-continue freeze types so human-review stops are untouched. See
            # knowledge-base/knowledge/issues/version_upgrade_drain_livelock.md.
            if resume and graph_input is None and not feedback:
                selected_node = await self._prepare_auto_continue_resume(
                    job_id=job_id,
                    thread_config=thread_config,
                    updated_metadata=updated_metadata,
                    stateless_worker=stateless_worker,
                    deferred_updates=(
                        worker_resume_updates
                        if stateless_worker and graph_input is None
                        else None
                    ),
                )
                if selected_node is not None:
                    worker_resume_as_node = selected_node

            if stateless_worker:
                # Arm last, after feedback/auto-continue has chosen the resume
                # frontier.  A plain state update on a mid-loop
                # checkpoint must preserve its pending next node; START is
                # reserved for a clean END re-entry below.
                worker_terminal_state = await self._arm_worker_batch(
                    job_id=job_id,
                    graph_input=graph_input,
                    thread_config=thread_config,
                    target_wall_seconds=worker_batch_target_wall_seconds,
                    min_wall_seconds=worker_batch_min_wall_seconds,
                    iteration_cap=worker_batch_iteration_cap,
                    resume_id=worker_resume_id,
                    retry_exhausted=worker_retry_exhausted,
                    resume_updates=worker_resume_updates,
                    resume_as_node=worker_resume_as_node,
                )

            # Durable-first resume hydration (journal-before-observe):
            # a restarted process lost the in-memory completion decision, so
            # re-seed the cache from the journaled record. Never on feedback
            # resumes — those demand new work and the decision is void (the
            # orchestrator also drops it in queue_job_for_resume). Never on
            # fresh dispatches — a fresh run must not inherit a stale
            # decision and insta-finalize. Non-fatal: without it the
            # checkpointed state mirror and the model's own re-issued
            # job_complete (idempotent) still recover.
            if resume and not feedback and self._orchestrator_client:
                try:
                    decision = (
                        await self._orchestrator_client.fetch_completion_decision(
                            job_id
                        )
                    )
                    if decision:
                        from agent.tools.core.job import seed_final_phase_data

                        seed_final_phase_data(job_id, decision)
                except Exception as e:
                    logger.warning(
                        f"[{job_id}] Completion-decision hydration failed "
                        f"(non-fatal, state mirror still applies): {e}"
                    )

            if stream:
                if worker_terminal_state is not None:
                    # An error-release after a failed terminal HTTP report
                    # reclaims an already-ended checkpoint.  LangGraph emits
                    # zero stream items for ainvoke(None) at END, so surface
                    # the durable values once without re-running any node.
                    return self._yield_error_state(worker_terminal_state)
                # For streaming, cleanup happens inside the generator
                return self._process_job_streaming(graph_input, thread_config)
            else:
                try:
                    # Reconcile predecessor child generations before the graph
                    # can resume at any provider/tool frontier.  The runtime's
                    # recovery lock and our object-identity guard make this an
                    # exactly-once transition for the claim.  Already-terminal
                    # checkpoint mirrors also need this before completion can
                    # pass the orchestrator's live-child barrier.
                    await self._recover_subagent_orphans()
                    if worker_terminal_state is not None:
                        return worker_terminal_state
                    final_state = await self._graph.ainvoke(
                        graph_input,
                        config=thread_config,
                    )
                    self._jobs_processed += 1
                    return dict(final_state)
                finally:
                    await self._quiesce_subagent_runtime("parent graph ended")
                    if not getattr(self, "_defer_job_cleanup", False):
                        self._current_job_id = None
                        self._cleanup_shell_manager()
                        self._close_datasource_connections()
                        await self._cleanup_checkpointer()

        except SubagentLifecycleError:
            # Never turn an unsettled child generation into a reportable parent
            # error.  Worker/session drivers own the fail-closed disposition.
            raise
        except Exception as e:
            # Failures after tool setup can otherwise tear down the only
            # reference to a durable runtime without recovering predecessor
            # generations or joining current children.
            await self._settle_subagent_runtime("parent setup or graph failed")
            from shared.runtime.core.workspace_backend import completion_error_payload

            error = completion_error_payload(e)["error"]
            if error["type"] == "workspace_unavailable":
                logger.error(
                    f"Job {job_id}: workspace unavailable — will request recovery: {e}"
                )
            elif error["type"] == "workspace_authentication":
                logger.error(
                    f"Job {job_id}: workspace authentication failed "
                    f"(non-retryable): {e}"
                )
            else:
                logger.error(f"Job {job_id} failed: {e}", exc_info=True)

            if not getattr(self, "_defer_job_cleanup", False):
                self._cleanup_shell_manager()
                self._close_datasource_connections()
                await self._cleanup_checkpointer()
                self._current_job_id = None
            error_state = {
                "job_id": job_id,
                "error": error,
                "should_stop": True,
            }
            if stream:
                # Return async generator that yields the error state
                return self._yield_error_state(error_state)
            return error_state

    def _restore_worker_instruction_reads(self, values: Dict[str, Any]) -> int:
        """Restore only checkpoint-safe instruction receipts for this claim."""

        if self._tool_context is None:
            return 0
        return self._tool_context.restore_instruction_read_receipts(
            values.get("instruction_read_receipts")
        )

    async def _prepare_auto_continue_resume(
        self,
        *,
        job_id: str,
        thread_config: Dict[str, Any],
        updated_metadata: Optional[Dict[str, Any]],
        stateless_worker: bool,
        deferred_updates: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """Prepare an ended machine stop for resume.

        Pinned retains its historical updates. A checkpoint-backed stateless
        caller stages its terminal clear and optional shape nudge so batch
        arming can join the same START update.
        """

        try:
            snapshot = await self._graph.aget_state(thread_config)
            values = snapshot.values or {}
            frozen = values.get("freeze_data") or {}
            freeze_type = (
                frozen.get("freeze_type") if isinstance(frozen, dict) else None
            )
            if not (
                values.get("should_stop") and freeze_type in AUTO_CONTINUE_FREEZE_TYPES
            ):
                return None

            outage_meta = (updated_metadata or {}).get("llm_outage")
            pending_shape_nudge = bool(
                isinstance(outage_meta, dict) and outage_meta.get("pending_shape_nudge")
            )
            clear_updates: Dict[str, Any] = {
                "should_stop": False,
                "goal_achieved": False,
                "is_final_phase": False,
                "freeze_data": None,
                "error": None,
                "client_report_id": None,
                "completion_report_payload": None,
            }
            if stateless_worker and pending_shape_nudge:
                from langchain_core.messages import HumanMessage

                clear_updates["messages"] = [HumanMessage(content=_SHAPE_NUDGE_TEXT)]
            if stateless_worker and deferred_updates is not None:
                _merge_worker_resume_updates(deferred_updates, clear_updates)
                logger.info(
                    "[%s] Staged auto-continue resume (%s) for atomic "
                    "stateless resume + arm",
                    job_id,
                    freeze_type,
                )
                if pending_shape_nudge:
                    logger.warning(
                        "[%s] Staged request-shape nudge for the stateless "
                        "auto-continue + arm update",
                        job_id,
                    )
                return "__start__"

            await self._graph.aupdate_state(
                thread_config,
                clear_updates,
                as_node="__start__",
            )
            logger.info(
                "[%s] Auto-continue resume (%s) — cleared terminal stop "
                "flags so the graph re-enters and resumes from its checkpoint",
                job_id,
                freeze_type,
            )
            if stateless_worker:
                if pending_shape_nudge:
                    logger.warning(
                        "[%s] Injected request-shape nudge in the stateless "
                        "auto-continue START update",
                        job_id,
                    )
                return None
            # >>> TEMPORARY QUICKFIX — remove with the upstream fix.
            # knowledge-history/done/codex_stream_disconnect_shape_nudge.md
            if pending_shape_nudge:
                from langchain_core.messages import HumanMessage

                await self._graph.aupdate_state(
                    thread_config,
                    {"messages": [HumanMessage(content=_SHAPE_NUDGE_TEXT)]},
                    as_node="__start__",
                )
                logger.warning(
                    "[%s] Injected request-shape nudge — the previous payload "
                    "was rejected upstream on every retry; appending a turn "
                    "to change it",
                    job_id,
                )
        except Exception as exc:
            logger.warning(
                "[%s] Failed to clear stop flags on auto-continue resume "
                "(job may re-freeze without progress): %s",
                job_id,
                exc,
            )
        return None

    async def _inject_resume_feedback(
        self,
        *,
        job_id: str,
        stateless_worker: bool,
        graph_input: Optional[UniversalAgentState],
        thread_config: Dict[str, Any],
        checkpoint_values: Dict[str, Any],
        feedback: str,
        feedback_reason: Optional[str],
        metadata: Optional[Dict[str, Any]],
        deferred_updates: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """Inject one exact feedback generation before resumed graph work.

        A stateless resume may legitimately have no checkpoint yet (for
        example, a pre-graph failure). In that case the first fenced
        checkpoint must contain both the feedback and its delivery key; an
        ``aupdate_state`` is impossible because the thread does not exist.
        """
        from shared.job_steering import context_delivery_key

        feedback_key = context_delivery_key(
            "feedback",
            feedback,
            delivery_id=(metadata or {}).get("queued_feedback_delivery_id"),
            companion=feedback_reason,
        )
        delivered_feedback_keys = {
            str(value)
            for value in checkpoint_values.get("delivered_feedback_keys") or []
            if value is not None
        }
        if stateless_worker and feedback_key in delivered_feedback_keys:
            logger.info(
                "[%s] Suppressed checkpointed feedback generation %s",
                job_id,
                feedback_key,
            )
            return None

        feedback_update: Dict[str, Any] = {
            "should_stop": False,
            "goal_achieved": False,
            "is_final_phase": False,
            # A feedback resume voids any journaled finalization decision from
            # the previous round (restore_from_feedback clears process caches).
            "completion_decision": None,
            "verdict_decision": None,
            "client_report_id": None,
            "completion_report_payload": None,
        }
        if stateless_worker:
            feedback_update["delivered_feedback_keys"] = sorted(
                delivered_feedback_keys | {feedback_key}
            )
        if graph_input is None:
            feedback_update.update(
                {
                    "resume_feedback": feedback,
                    "resume_reason": feedback_reason,
                }
            )
            if deferred_updates is not None:
                _merge_worker_resume_updates(deferred_updates, feedback_update)
                logger.info(
                    "[%s] Staged feedback for atomic stateless resume + arm",
                    job_id,
                )
            else:
                await self._graph.aupdate_state(
                    thread_config,
                    feedback_update,
                    as_node="__start__",
                )
                logger.info("Injected feedback into graph state via aupdate_state")
        else:
            # A no-checkpoint resume is still the job's first initialization.
            # Setting resume_feedback would route around init_workspace and
            # init_strategic_todos, losing the original task and its initial
            # todo plan. Keep the fresh route and add feedback as a
            # supplemental durable HumanMessage instead.
            from langchain_core.messages import HumanMessage

            reason = (feedback_reason or "").strip() or (
                "This job was resumed with feedback from its operator."
            )
            existing_messages = list(graph_input.get("messages") or [])
            feedback_message = HumanMessage(
                content=(
                    f"[FEEDBACK_RESUME] {reason}\n\n"
                    f"## Feedback\n\n{feedback}\n\n"
                    "Apply this feedback while initializing and carrying out "
                    "the original task below."
                )
            )
            graph_input.update(feedback_update)
            graph_input["messages"] = [*existing_messages, feedback_message]
            logger.info(
                "[%s] Added feedback to fresh stateless initialization input",
                job_id,
            )
        checkpoint_values.update(feedback_update)
        return (
            "__start__"
            if graph_input is None and deferred_updates is not None
            else None
        )

    async def _arm_worker_batch(
        self,
        *,
        job_id: str,
        graph_input: Optional[UniversalAgentState],
        thread_config: Dict[str, Any],
        target_wall_seconds: Optional[float],
        min_wall_seconds: Optional[float],
        iteration_cap: Optional[int],
        resume_id: Optional[str] = None,
        retry_exhausted: bool = False,
        resume_updates: Optional[Dict[str, Any]] = None,
        resume_as_node: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Atomically route and arm a claim, or adopt a durable prior arm."""

        target = float(target_wall_seconds or 0)
        if not math.isfinite(target) or target <= 0:
            raise ValueError("worker batch target must be a positive finite number")
        floor = (
            WORKER_BATCH_MIN_WALL_SECONDS
            if min_wall_seconds is None
            else float(min_wall_seconds)
        )
        if not math.isfinite(floor) or floor < 0:
            raise ValueError("worker batch floor must be a finite non-negative number")
        cap = None
        if iteration_cap is not None:
            if isinstance(iteration_cap, bool) or int(iteration_cap) <= 0:
                raise ValueError("worker batch iteration cap must be positive")
            cap = int(iteration_cap)

        values: Dict[str, Any]
        snapshot = None
        if graph_input is None:
            snapshot = await self._graph.aget_state(thread_config)
            values = dict(snapshot.values or {})
        else:
            values = graph_input
        iteration = values.get("iteration", 0)
        if isinstance(iteration, bool) or not isinstance(iteration, int):
            try:
                iteration = int(iteration)
            except (TypeError, ValueError):
                iteration = 0

        updates: Dict[str, Any] = {
            "worker_batch_started_at": time.time(),
            "worker_batch_start_iteration": iteration,
            "worker_batch_target_wall_seconds": target,
            "worker_batch_min_wall_seconds": floor,
            "worker_batch_iteration_cap": cap,
        }
        if resume_updates:
            _merge_worker_resume_updates(updates, resume_updates)
        resume_route_pending = bool(resume_updates and resume_as_node)
        applied_resume_id = values.get("worker_resume_id")
        resume_intent_pending = bool(
            resume_id and str(resume_id) != str(applied_resume_id or "")
        )
        if resume_intent_pending:
            updates["worker_resume_id"] = str(resume_id)
        frozen = values.get("freeze_data") or {}
        freeze_type = frozen.get("freeze_type") if isinstance(frozen, dict) else None
        error = values.get("error")
        recoverable_error = bool(
            isinstance(error, dict) and error.get("recoverable") is True
        )
        pending_frontier = bool(snapshot is not None and tuple(snapshot.next or ()))
        ended = bool(snapshot is not None and not pending_frontier)
        snapshot_metadata = getattr(snapshot, "metadata", None)
        checkpoint_source = (
            snapshot_metadata.get("source")
            if isinstance(snapshot_metadata, dict)
            else None
        )
        human_freeze = bool(
            freeze_type and freeze_type not in AUTO_CONTINUE_FREEZE_TYPES
        )
        recoverable_end = bool(
            ended
            and not human_freeze
            and (recoverable_error or freeze_type in AUTO_CONTINUE_FREEZE_TYPES)
        )
        if retry_exhausted:
            if ended and values.get("should_stop") and not recoverable_end:
                logger.info(
                    "[%s] Worker retry budget exhausted, but canonical "
                    "checkpoint is terminal/human END; re-reporting it unchanged",
                    job_id,
                )
                return values
            # The last real attempt has already been spent. Surface a
            # recoverable driver envelope to the caller, which converts it to
            # the factual non-recoverable worker_retry_exhausted report. Do not
            # mutate the checkpoint or execute another graph node.
            logger.error(
                "[%s] Worker retry budget exhausted before a terminal/human "
                "checkpoint; suppressing further graph work",
                job_id,
            )
            return {
                "job_id": job_id,
                "should_stop": True,
                "goal_achieved": False,
                "error": {
                    "type": "worker_retry_budget_exhausted",
                    "recoverable": True,
                    "message": "worker queue retry budget exhausted",
                },
            }

        if (
            pending_frontier
            and checkpoint_source == "update"
            and _valid_worker_batch_arm(values)
            and not resume_updates
            and not resume_intent_pending
        ):
            # A predecessor durably committed route + arm and died before the
            # graph consumed the selected task. Reuse that exact envelope.
            # Any second update here can erase the frontier; a Command(update)
            # is also unsafe if a prior invocation already wrote this step.
            logger.info(
                "[%s] Adopted pending stateless resume frontier with durable "
                "worker arm; checkpoint update skipped",
                job_id,
            )
            return None
        clean_end_reentry = bool(
            ended
            and (
                resume_route_pending
                or (
                    values.get("should_stop")
                    and (
                        freeze_type in AUTO_CONTINUE_FREEZE_TYPES
                        or recoverable_error
                        or resume_intent_pending
                    )
                )
            )
        )
        if clean_end_reentry:
            # Batch/outage/recoverable stops are machine continuations on this
            # lane. Clear their END envelope and route through START exactly
            # once. Human-facing and terminal END checkpoints are untouched;
            # the driver re-reports them rather than re-running the graph.
            updates.update(
                {
                    "freeze_data": None,
                    "should_stop": False,
                    "goal_achieved": False,
                    "is_final_phase": False,
                    "error": None,
                    "client_report_id": None,
                    "completion_report_payload": None,
                }
            )

        if graph_input is not None:
            graph_input.update(updates)
        elif ended and not clean_end_reentry and not resume_route_pending:
            logger.info(
                "[%s] Worker checkpoint is a terminal/human END; leaving it "
                "unchanged for report retry",
                job_id,
            )
            return values
        elif resume_route_pending or clean_end_reentry or resume_intent_pending:
            selected_node = resume_as_node if resume_route_pending else "__start__"
            await self._graph.aupdate_state(
                thread_config,
                updates,
                as_node=selected_node,
            )
        elif pending_frontier and checkpoint_source == "loop":
            # A loop checkpoint records the node that produced this frontier,
            # so LangGraph can infer the same node for this single arm update.
            await self._graph.aupdate_state(thread_config, updates)
        elif pending_frontier:
            logger.error(
                "[%s] Refusing to arm an unadoptable pending update frontier "
                "(source=%s); releasing without mutating its selected task",
                job_id,
                checkpoint_source,
            )
            return {
                "job_id": job_id,
                "should_stop": True,
                "goal_achieved": False,
                "error": {
                    "type": "worker_resume_frontier_unarmed",
                    "recoverable": True,
                    "message": (
                        "pending worker resume frontier has no valid durable batch arm"
                    ),
                },
            }
        else:
            await self._graph.aupdate_state(thread_config, updates)
        logger.info(
            "[%s] Armed worker batch: target=%.3fs floor=%.3fs "
            "start_iteration=%d iteration_cap=%s",
            job_id,
            target,
            floor,
            iteration,
            cap,
        )
        return None

    async def _make_checkpointer(self, job_id: str) -> None:
        """Create the LangGraph checkpointer for this job per CHECKPOINTER_BACKEND.

        Sets self._checkpointer and self._checkpoint_conn. backend='postgres'
        stores graph state in shared Postgres (keyed by thread_id=job_id) so a
        resume on any pod reads it via the graph's aget_state — fixing cross-pod
        cold-starts. Default 'sqlite' keeps the legacy pod-local checkpoint.
        """
        backend = checkpointer_backend()
        if self._worker_lease_token is not None:
            if backend != "postgres":
                raise RuntimeError(
                    "Stateless workers require CHECKPOINTER_BACKEND=postgres"
                )
            from agent.core.fenced_checkpointer import make_fenced_checkpointer

            url = resolve_fenced_checkpoint_url()
            if not url:
                raise RuntimeError(
                    "Stateless worker checkpoints require the authoritative "
                    "application Postgres URL (POSTGRES_* or DATABASE_URL)"
                )
            self._checkpoint_conn = None
            self._checkpointer = await make_fenced_checkpointer(
                url,
                unit_id=job_id,
                lease_token=self._worker_lease_token,
                post_commit=self._worker_checkpoint_post_commit,
            )
            logger.info(
                "Checkpointer initialized (fenced postgres, thread_id=%s token=%d)",
                job_id,
                self._worker_lease_token,
            )
            return
        if backend == "postgres":
            from psycopg import AsyncConnection
            from psycopg.rows import dict_row
            from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

            url = resolve_checkpoint_url()
            if not url:
                raise RuntimeError(
                    "CHECKPOINTER_BACKEND=postgres but no checkpoint DB URL "
                    "configured (set CHECKPOINT_DB_URL, CHECKPOINT_*, or "
                    "POSTGRES_*/DATABASE_URL)."
                )
            conn = await AsyncConnection.connect(
                url, autocommit=True, prepare_threshold=0, row_factory=dict_row
            )
            self._checkpoint_conn = conn
            self._checkpointer = AsyncPostgresSaver(conn)
            await self._ensure_pg_checkpoint_schema()
            logger.info("Checkpointer initialized (postgres, thread_id=%s)", job_id)
        else:
            checkpoint_path = self._get_checkpoint_path(job_id)
            self._checkpoint_conn = await aiosqlite.connect(checkpoint_path)
            # Wrap to add is_alive() for langgraph-checkpoint-sqlite 3.x.
            wrapped_conn = _AiosqliteConnectionWrapper(self._checkpoint_conn)
            self._checkpointer = AsyncSqliteSaver(wrapped_conn)
            logger.info(f"Checkpointer initialized at {checkpoint_path}")

    async def _ensure_pg_checkpoint_schema(self) -> None:
        """Run AsyncPostgresSaver.setup() once per process (idempotent DDL)."""
        global _PG_CHECKPOINT_SCHEMA_READY
        if _PG_CHECKPOINT_SCHEMA_READY:
            return
        await self._checkpointer.setup()
        _PG_CHECKPOINT_SCHEMA_READY = True
        logger.info("Postgres checkpoint schema ensured")

    async def _cleanup_checkpointer(self) -> None:
        """Clean up checkpointer connection."""
        checkpoint_conn = getattr(self, "_checkpoint_conn", None)
        if checkpoint_conn:
            try:
                await checkpoint_conn.close()
            except Exception as e:
                logger.warning(f"Error closing checkpointer connection: {e}")
        # Fenced worker savers borrow from a lifespan-owned process pool and
        # therefore have no per-job connection to close.  Dropping the saver is
        # still required so a stale immutable token cannot be reused.
        self._checkpoint_conn = None
        self._checkpointer = None

    def _cleanup_shell_manager(self) -> None:
        """Clean up ShellManager (kill tmux session)."""
        if getattr(self, "_shell_manager", None):
            try:
                self._shell_manager.cleanup()
            except Exception:
                # Terminal job disposition may already have deleted a root
                # workspace, and terminal rows are not reclaimable for another
                # cleanup attempt. Keep teardown best-effort; Kubernetes exact-
                # UID deletion owns root process death. Shared workspaces still
                # use the child-shell-only retirement command below this seam.
                logger.warning(
                    "ShellManager cleanup was not acknowledged", exc_info=True
                )
            self._shell_manager = None

    def _capture_worker_environment(self, metadata: Dict[str, Any]) -> None:
        """Snapshot every per-job env key before this worker can overwrite it.

        A stateless process serves unrelated jobs sequentially.  Config
        ``env_keys`` are intentionally open-ended, while managed datasource
        CLIs populate a small fixed set.  Recording the pre-claim values lets
        teardown restore the pod baseline instead of leaking one tenant's
        credentials into the next claim.
        """

        self._restore_worker_environment()
        env_keys = (metadata.get("config_override") or {}).get("env_keys")
        if not env_keys:
            env_keys = ((metadata.get("resolved_config") or {}).get("agent") or {}).get(
                "env_keys"
            )
        keys = set(env_keys) if isinstance(env_keys, dict) else set()
        datasource_env = {
            "postgresql": {"PGHOST", "PGPORT", "PGUSER", "PGPASSWORD", "PGDATABASE"},
            "neo4j": {"NEO4J_URI", "NEO4J_USERNAME", "NEO4J_PASSWORD"},
            "mongodb": {"MONGOSH_URI"},
        }
        for datasource in metadata.get("datasources") or []:
            if isinstance(datasource, dict):
                keys.update(datasource_env.get(str(datasource.get("type")), set()))
        self._worker_env_restore = {key: os.environ.get(key) for key in keys}

    def _restore_worker_environment(self) -> None:
        restore = getattr(self, "_worker_env_restore", {})
        self._worker_env_restore = {}
        for key, value in restore.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        if restore:
            # Both embedding service variants cache clients derived from env.
            # Resetting them after the values are restored prevents a secret
            # surviving through a singleton even though os.environ is clean.
            try:
                import shared.runtime.services.embedding_service as embedding_service

                embedding_service._embedding_service = None
                embedding_service._kb_embedding_service = None
                embedding_service._kb_embedding_profile = None
            except Exception:
                logger.debug("Worker embedding singleton scrub failed", exc_info=True)

    def _worker_workspace_backend(self, *, strict: bool = False) -> Any | None:
        """Return the exact backend retained for this worker disposition."""

        retained = getattr(self, "_worker_finalization_backend", None)
        if retained is not None:
            return retained
        workspace_manager = getattr(self, "_workspace_manager", None)
        if workspace_manager is None:
            return None
        try:
            from agent.core.virtual_dirs import unwrap_backend

            return unwrap_backend(workspace_manager.backend)
        except Exception:
            if strict:
                raise
            logger.debug("Could not unwrap worker workspace backend", exc_info=True)
            return None

    def _retire_worker_shell_admission(
        self, backend: Any | None, *, strict: bool = False
    ) -> None:
        """Close local tmux admission once, retaining terminal cleanup power."""

        if backend is None:
            return
        retire_shell_owner = getattr(backend, "retire_shell_owner", None)
        if strict and not callable(retire_shell_owner):
            raise SubagentQuiescenceError("worker backend has no shell admission gate")
        if getattr(self, "_worker_shell_admission_retired", False):
            return
        if retire_shell_owner is None:
            self._worker_shell_admission_retired = True
            return
        try:
            # Cancelled synchronous work may still hold this object, but can
            # no longer submit tmux I/O after this returns.  RemoteBackend's
            # terminal shell_cleanup deliberately remains available after the
            # local admission bit is retired.
            retire_shell_owner()
        except Exception:
            if strict:
                raise
            logger.warning("Worker shell admission retirement failed", exc_info=True)
        else:
            self._worker_shell_admission_retired = True

    def _current_subagent_runtime(self) -> Any | None:
        """Return the claim-local child runtime without retaining its context."""

        context = getattr(self, "_tool_context", None)
        return getattr(context, "subagent_runtime", None)

    async def _recover_subagent_orphans(self) -> None:
        """Recover durable predecessor generations once before graph resume.

        A null/in-memory ledger has no durable predecessor state to reconcile.
        Checking for the strict live-list seam keeps bare/local parents working
        while every DB-backed worker and pinned job takes the recovery gate.
        """

        runtime = self._current_subagent_runtime()
        if (
            runtime is None
            or getattr(self, "_subagent_recovered_runtime", None) is runtime
        ):
            return
        ledger = getattr(runtime, "ledger", None)
        if not callable(getattr(ledger, "list_live", None)):
            return
        recover = getattr(runtime, "recover_orphans", None)
        if not callable(recover):
            raise SubagentRecoveryError(
                "durable subagent runtime has no orphan recovery gate"
            )
        try:
            await recover()
        except asyncio.CancelledError:
            raise
        except SubagentLifecycleError:
            raise
        except Exception as exc:
            raise SubagentRecoveryError(
                "durable subagent orphan recovery failed"
            ) from exc
        self._subagent_recovered_runtime = runtime

    async def _quiesce_subagent_runtime(self, reason: str) -> None:
        """Settle a claim's children once while parent authority is current."""

        runtime = self._current_subagent_runtime()
        if runtime is None:
            return
        if getattr(self, "_subagent_abandoned_runtime", None) is runtime:
            return
        if getattr(self, "_subagent_quiesced_runtime", None) is runtime:
            return
        quiesce = getattr(runtime, "quiesce", None)
        if not callable(quiesce):
            raise SubagentQuiescenceError("subagent runtime has no quiescence gate")
        try:
            await quiesce(reason)
        except asyncio.CancelledError:
            raise
        except SubagentLifecycleError:
            raise
        except Exception as exc:
            raise SubagentQuiescenceError(
                f"subagent runtime could not quiesce: {reason}"
            ) from exc
        self._subagent_quiesced_runtime = runtime

    async def _settle_subagent_runtime(self, reason: str) -> None:
        """Recover predecessors, then terminally join this runtime once."""

        await self._recover_subagent_orphans()
        await self._quiesce_subagent_runtime(reason)

    async def abandon_worker_subagents(
        self, reason: str = "worker lease authority lost"
    ) -> None:
        """Cancel claim-local children without writes after local lease loss."""

        runtime = self._current_subagent_runtime()
        if (
            runtime is None
            or getattr(self, "_subagent_abandoned_runtime", None) is runtime
        ):
            return
        abandon = getattr(runtime, "abandon", None)
        if not callable(abandon):
            raise SubagentAbandonError("subagent runtime has no authority-loss gate")
        try:
            await abandon(reason)
        except asyncio.CancelledError:
            raise
        except SubagentLifecycleError:
            raise
        except Exception as exc:
            raise SubagentAbandonError(
                f"subagent runtime could not abandon after authority loss: {reason}"
            ) from exc
        self._subagent_abandoned_runtime = runtime

    async def _scrub_worker_claim_locals(self) -> None:
        """Remove tenant-local state without deciding the remote shell fate."""

        # This is the last invariant belt before ToolContext (and therefore the
        # only reference to the child runtime) is dropped.  Normal streaming
        # completion already quiesced in the generator; defensive cleanup paths
        # safely collapse here through the identity guard.
        await self._quiesce_subagent_runtime("worker claim cleanup")

        tool_context = getattr(self, "_tool_context", None)
        if tool_context is not None:
            tool_context.shell_manager = None
            tool_context.citation_verdict_callback = None

        if getattr(self, "_doc_registration_task", None) is not None:
            task = self._doc_registration_task
            self._doc_registration_task = None
            if not task.done():
                task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.debug(
                    "Worker document registration cleanup failed", exc_info=True
                )

        # Legacy/unit-test ``__new__`` instances may predate optional
        # datasource attributes.  Full initialized workers always take the
        # production cleanup path; sparse instances still get an idempotent
        # hold instead of failing before admission is closed.
        if all(
            hasattr(self, name)
            for name in (
                "_knowledge_graph",
                "_datasource_connections",
                "_datasource_clients",
                "_datasource_files_manifest",
            )
        ):
            self._close_datasource_connections()
        await self._cleanup_checkpointer()
        self._restore_worker_environment()

        # Keep only the exact job id/token for bounded hold logging and the
        # separately retained shell/backend handles.  Graph, tool, todo,
        # credential and checkpoint state must not survive while this shared
        # executor waits for an orchestrator outcome.
        self._job_metadata = None
        self._todo_manager = None
        self._tool_context = None
        self._tools = None
        self._graph = None
        self._worker_checkpoint_post_commit = None
        self._defer_job_cleanup = False

    async def hold_worker_finalization(self, *, strict: bool = False) -> None:
        """Enter the inert, bounded hold after durable completion acceptance.

        This is deliberately not ``cleanup_worker_claim(preserve_shell=True)``:
        that handoff drops terminal cleanup authority.  The hold drains and
        retires the original backend plus all tenant state, retaining only a
        cleanup-only clone with the exact immutable runtime fence.  A later
        :meth:`cleanup_worker_claim` performs the one final disposition.

        Strict mode also prepares ordinary VM retirement, which can race a
        recovery receipt. No handle is scrubbed until local quiescence is proven.
        """

        if getattr(self, "_worker_finalization_held", False):
            if strict:
                await asyncio.to_thread(
                    self._retire_worker_shell_admission,
                    self._worker_workspace_backend(strict=True),
                    strict=True,
                )
            return
        # Child terminal evidence must be committed while parent authority and
        # its transport are still live.  Never retire the shell/backend first.
        await self._quiesce_subagent_runtime("worker finalization hold")
        backend = self._worker_workspace_backend(strict=strict)
        self._worker_finalization_backend = backend
        if strict:
            await asyncio.to_thread(
                self._retire_worker_shell_admission, backend, strict=True
            )
        else:
            self._retire_worker_shell_admission(backend)

        # Retain only a cleanup-only clone carrying immutable workspace/job/
        # runtime/token authority.  The original backend is then fully retired,
        # which drains admitted resource/SFTP calls and prevents cancelled
        # worker threads from reconnecting after the B4 acceptance boundary.
        if getattr(self, "_shell_manager", None) is not None:
            make_cleanup = getattr(
                backend, "make_terminal_shell_cleanup_capability", None
            )
            if make_cleanup is None:
                raise RuntimeError(
                    "Finalization hold requires a cleanup-only shell capability"
                )
            self._worker_terminal_shell_cleanup = make_cleanup()
        retire = getattr(backend, "retire", None) if backend else None
        if strict and backend is not None and not callable(retire):
            raise SubagentQuiescenceError("worker backend has no retirement gate")
        if retire is not None:
            await asyncio.to_thread(retire)
        await self._scrub_worker_claim_locals()
        self._workspace_manager = None
        self._shell_manager = None
        self._worker_finalization_held = True

    async def quiesce_worker_workspace_recovery(self) -> None:
        """Strict local retirement; no remote process-stop claim is implied.

        Preserve the original handles if any join fails so a quarantined
        executor cannot scrub away the only evidence of live local work.
        """
        from agent.core.virtual_dirs import unwrap_backend

        manager = getattr(self, "_workspace_manager", None)
        backend = getattr(self, "_worker_finalization_backend", None)
        if backend is None and manager is not None:
            backend = unwrap_backend(manager.backend)
        # Closing shell admission may wait for synchronous shell I/O. Keep it
        # off the event loop so the driver can quarantine on its join deadline.
        if backend is not None:
            close_admission = getattr(backend, "retire_shell_owner", None)
            if callable(close_admission):
                await asyncio.to_thread(close_admission)
        await self.abandon_worker_subagents("workspace recovery handoff")
        if backend is not None:
            retire = getattr(backend, "retire", None)
            if not callable(retire):
                raise SubagentQuiescenceError(
                    "workspace backend has no retirement gate"
                )
            # Includes resource/SFTP joins. A failure MUST escape this path.
            await asyncio.to_thread(retire)
        await self._scrub_worker_claim_locals()
        self._shell_manager = None
        self._workspace_manager = None
        self._current_job_id = None
        self._worker_lease_token = None
        self._worker_finalization_held = False
        self._worker_finalization_backend = None
        self._worker_terminal_shell_cleanup = None
        self._worker_shell_admission_retired = False
        self._subagent_recovered_runtime = None
        self._subagent_quiesced_runtime = None
        self._subagent_abandoned_runtime = None

    async def cleanup_worker_claim(self, *, preserve_shell: bool) -> None:
        """Retire all claim-local runtime state under the driver's disposition.

        ``preserve_shell=True`` is used for rotation, retry and lease handoff:
        it closes this Python owner's admission and SSH transport without
        killing the durable workspace tmux session.  A genuine terminal stop
        passes ``False`` and keeps the historical destructive shell cleanup.
        """

        # This is the disposition boundary: settle children before retiring
        # shell admission, transport, or the ToolContext that owns the runtime.
        await self._quiesce_subagent_runtime("worker claim cleanup")
        backend = self._worker_workspace_backend()
        backend_already_retired = bool(
            getattr(self, "_worker_finalization_held", False)
            and backend is getattr(self, "_worker_finalization_backend", None)
        )
        self._retire_worker_shell_admission(backend)

        terminal_cleanup = getattr(self, "_worker_terminal_shell_cleanup", None)
        if terminal_cleanup is not None:
            if not preserve_shell:
                try:
                    await asyncio.to_thread(terminal_cleanup)
                except Exception:
                    logger.warning(
                        "Worker terminal shell retirement was not acknowledged",
                        exc_info=True,
                    )
            self._worker_terminal_shell_cleanup = None
            self._shell_manager = None
        elif preserve_shell:
            if getattr(self, "_shell_manager", None) is not None:
                logger.info(
                    "Preserving remote shell for worker handoff: job=%s token=%s",
                    self._current_job_id,
                    self._worker_lease_token,
                )
            self._shell_manager = None
        else:
            self._cleanup_shell_manager()

        await self._scrub_worker_claim_locals()

        if backend is not None and not backend_already_retired:
            retire = getattr(backend, "retire", None)
            disconnect = getattr(backend, "disconnect", None)
            try:
                if retire is not None:
                    await asyncio.to_thread(retire)
                elif disconnect is not None:
                    await asyncio.to_thread(disconnect)
            except Exception:
                logger.warning("Worker backend retirement failed", exc_info=True)

        self._current_job_id = None
        self._job_metadata = None
        self._workspace_manager = None
        self._todo_manager = None
        self._tool_context = None
        self._tools = None
        self._graph = None
        self._worker_lease_token = None
        self._worker_checkpoint_post_commit = None
        self._defer_job_cleanup = False
        self._worker_finalization_held = False
        self._worker_finalization_backend = None
        self._worker_terminal_shell_cleanup = None
        self._worker_shell_admission_retired = False
        self._subagent_recovered_runtime = None
        self._subagent_quiesced_runtime = None
        self._subagent_abandoned_runtime = None

    async def _yield_error_state(
        self, error_state: Dict[str, Any]
    ) -> AsyncIterator[Dict[str, Any]]:
        """Yield a single error state for streaming mode."""
        try:
            # A checkpoint that is already terminal still has to reconcile
            # predecessor children before the orchestrator's completion barrier
            # can accept it; this path deliberately performs no provider work.
            await self._recover_subagent_orphans()
            yield error_state
        finally:
            await self._quiesce_subagent_runtime("parent stream ended")

    async def _process_job_streaming(
        self,
        graph_input: Optional[UniversalAgentState],
        config: Dict[str, Any],
    ) -> AsyncIterator[Dict[str, Any]]:
        """Process job with streaming state updates.

        Args:
            graph_input: Initial state for new jobs, or None to resume from checkpoint
            config: LangGraph config with thread_id

        If a run ends with a ``workspace_upgrade_required`` freeze (target
        ``sandbox``), the job is upgraded IN PROCESS — provision → seed → swap →
        retool → rebuild graph — and the same graph is re-streamed from the local
        checkpoint with shell/git now available, mirroring the session live swap
        (workspace_tier_upgrade.md §4.3 W1). No re-dispatch, no checkpoint move.
        On upgrade failure the freeze is surfaced unchanged so the orchestrator
        pauses the job.
        """
        try:
            # Recovery happens before the first graph step.  In particular, a
            # resumed checkpoint may enter directly at a provider/tool node and
            # cannot be allowed to race predecessor child generations.
            await self._recover_subagent_orphans()
            # At most one in-process upgrade per run: virtual → sandbox flips the
            # backend to supports_shell=True, after which the request tool drops
            # out and the supports_shell guard below short-circuits anyway.
            upgraded = False
            while True:
                final_state: Optional[Dict[str, Any]] = None
                graph_stream = run_graph_with_streaming(
                    self._graph, graph_input, config
                )
                try:
                    async for state in graph_stream:
                        final_state = state
                        yield state
                finally:
                    # Closing an outer async generator does not implicitly wait
                    # for the inner graph iterator's ``finally`` block.  Join it
                    # explicitly before child quiescence and before the worker
                    # driver can report or rotate this claim.
                    close_graph_stream = getattr(graph_stream, "aclose", None)
                    if callable(close_graph_stream):
                        await close_graph_stream()

                freeze = (final_state or {}).get("freeze_data") or {}
                wants_upgrade = (
                    not upgraded
                    and isinstance(freeze, dict)
                    and freeze.get("freeze_type") == "workspace_upgrade_required"
                    and (freeze.get("target_tier") or "sandbox") == "sandbox"
                    and self._workspace_manager is not None
                    and not getattr(
                        self._workspace_manager.backend, "supports_shell", False
                    )
                )
                if not wants_upgrade:
                    break

                upgraded = True
                ok = await self._perform_inprocess_workspace_upgrade(
                    freeze.get("target_tier") or "sandbox"
                )
                if not ok:
                    # Provision/seed failed — surface the freeze unchanged; the
                    # orchestrator routes it (pauses the job for re-dispatch).
                    break

                # Prime the rebuilt graph to resume from the local checkpoint
                # (the proven feedback-resume pattern): clear the stale freeze +
                # stop flags as a __start__ update, then re-stream with no input
                # so route_entry → restore_todo_state continues the loop with
                # shell/git now available. restore_todo_state also clears
                # should_stop/goal_achieved; clearing freeze_data here keeps the
                # stale upgrade-freeze from reaching the orchestrator on real
                # completion.
                await self._graph.aupdate_state(
                    config,
                    {
                        "freeze_data": None,
                        "should_stop": False,
                        "goal_achieved": False,
                        "client_report_id": None,
                        "completion_report_payload": None,
                    },
                    as_node="__start__",
                )
                graph_input = None

            self._jobs_processed += 1
        except SubagentLifecycleError:
            # Recovery/quiescence is a parent-disposition fence, not a graph
            # error that may be serialized and reported as terminal.
            raise
        except Exception as e:
            # Mirror the non-streaming handler: classify the failure and yield
            # a TYPED error state instead of letting the exception escape the
            # generator. An escaping exception lands in the app-layer's generic
            # `except Exception`, which reports `{"error": {"message": ...}}` —
            # stripping the `workspace_unavailable` type the orchestrator's
            # recovery arm routes on, so a dead-workspace job hard-fails (and
            # its VM is torn down) instead of pause → reprovision → resume.
            # knowledge-base/knowledge/issues/streaming_strips_workspace_unavailable_type.md
            from shared.runtime.core.workspace_backend import completion_error_payload

            job_id = self._current_job_id
            error = completion_error_payload(e)["error"]
            if error["type"] == "workspace_unavailable":
                logger.error(
                    f"Job {job_id}: workspace unavailable mid-stream — "
                    f"will request recovery: {e}"
                )
            elif error["type"] == "workspace_authentication":
                logger.error(
                    f"Job {job_id}: workspace authentication failed mid-stream "
                    f"(non-retryable): {e}"
                )
            else:
                logger.error(f"Job {job_id} failed mid-stream: {e}", exc_info=True)
            yield {
                "job_id": job_id,
                "error": error,
                "should_stop": True,
            }
        finally:
            # The worker driver does not report/rotate until it observes this
            # generator's StopAsyncIteration (and explicitly closes it on every
            # other exit).  Settling children here therefore preserves the hard
            # generator-close-before-disposition ordering while the parent lease
            # is still live.
            await self._quiesce_subagent_runtime("parent stream ended")

            # Drain in-flight memory captures (the fire-and-forget pre_compaction
            # extraction scheduled via capture_nowait) BEFORE tearing down
            # connections, so a compaction on the last LLM call before freeze
            # still persists. Bounded by the aux call timeout — a hung endpoint
            # must never wedge job completion (OQ-C,
            # memory_extraction_before_compaction.md §8).
            mgr = getattr(self._graph, "_srw_memory_service", None)
            if mgr is not None:
                try:
                    aux_timeout = getattr(self._auxiliary_llm, "timeout", None)
                    drained = await mgr.drain_background(timeout=aux_timeout or 60.0)
                    if drained:
                        logger.info(
                            f"[{self._current_job_id}] Drained {drained} in-flight "
                            f"memory task(s) at job-end"
                        )
                except Exception as e:
                    logger.debug(f"Memory drain at job-end failed (non-fatal): {e}")

            # Worker claims defer teardown until their driver classifies the
            # exit (rotation/lease loss preserves tmux; genuine end destroys
            # it).  Every other caller keeps the historical eager cleanup.
            if not getattr(self, "_defer_job_cleanup", False):
                self._current_job_id = None
                self._cleanup_shell_manager()
                self._close_datasource_connections()
                await self._cleanup_checkpointer()

    async def _poll_job_workspace_ready(
        self, job_id: str, timeout: int = 300, poll_interval: float = 2.0
    ) -> Optional[Dict[str, Any]]:
        """Poll the orchestrator for a running job's upgraded-workspace readiness.

        The worker analogue of ``persistent_app._poll_workspace_ready`` (sandbox
        only — the worker MVP upgrades ``virtual → sandbox``; ``vm`` is the
        operator-gated re-dispatch path). Returns the
        ``{"backend":"sandbox","remote":{...}}`` block, or None on
        timeout/failure (workspace_tier_upgrade.md §4.3 W1).
        """
        import time

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            ws = await self._orchestrator_client.get_job_workspace_status(job_id)
            if not ws:
                return None
            # SSH key: orchestrator sends the path it resolved; fall back to the
            # K8s default secret mount.
            ssh_key = ws.get("ssh_key_path") or "/run/secrets/vm-ssh-key"
            status = ws.get("status", "none")
            if status == "ready" and ws.get("pod_ip"):
                return {
                    "backend": "sandbox",
                    "remote": {
                        "host": ws["pod_ip"],
                        "port": ws.get("pod_port") or 30022,
                        "username": "agent-host",
                        "key_path": ssh_key,
                        "workspace_path": "/home/agent-host/workspace",
                    },
                }
            if status == "failed":
                # Keep internal readiness payloads out of logs.  They can gain
                # server-owned transport material as provisioning evolves;
                # the stable status is all this poller needs to report.
                logger.warning(
                    "[%s] Workspace provisioning failed (status=%s)",
                    job_id,
                    status,
                )
                return None
            if status == "none":
                # No workspace_container recorded — nothing is provisioning.
                return None
            # Still pending/creating/created — wait and poll again.
            await asyncio.sleep(poll_interval)

        logger.warning(
            f"[{job_id}] Workspace upgrade polling timed out after {timeout}s"
        )
        return None

    async def _perform_inprocess_workspace_upgrade(self, target_tier: str) -> bool:
        """Upgrade a running lite (``virtual``/``none``) job to a real ``sandbox``
        workspace IN PROCESS, mirroring the session live swap
        (workspace_tier_upgrade.md §4.3 W1).

        Provision via the orchestrator → poll → connect a sandbox
        ``RemoteBackend`` → seed the still-live virtual files into it (both
        backends live) → swap the ``WorkspaceManager`` backend → re-derive
        tools/shell → rebuild the graph on the SAME checkpointer. The caller then
        re-streams the graph from the checkpoint.

        Returns True on success (graph rebuilt, ready to resume on the new
        backend); False on any failure (the caller surfaces the freeze so the
        orchestrator pauses the job). ``config_override`` / ``resolved_config``
        are deliberately NOT rewritten (frozen at first dispatch, §4.1) — the
        swap is in-process and ephemeral.
        """
        job_id = self._current_job_id
        # The REAL backend, not the overlay: swap_backend() rebinds the overlay
        # in place, so an overlay reference held across the swap would resolve
        # to the NEW backend — and `old_backend.disconnect()` below would tear
        # down the workspace we just upgraded onto. Unwrapping also keeps the
        # seed copy on the real filesystem (no virtual files materialized into
        # the sandbox).
        from agent.core.virtual_dirs import unwrap_backend

        old_backend = (
            unwrap_backend(self._workspace_manager.backend)
            if self._workspace_manager
            else None
        )
        if old_backend is None:
            return False
        # Guard: already a real (shell-capable) workspace — nothing to upgrade.
        if getattr(old_backend, "supports_shell", False):
            return True
        if not self._orchestrator_client:
            logger.warning(
                f"[{job_id}] Workspace upgrade requested but no orchestrator "
                f"client — cannot provision; surfacing freeze"
            )
            return False

        if target_tier == "sandbox":
            # A live upgrade would deliver repository credentials and seed
            # bytes after polling an endpoint with no durable exact-runtime
            # receipt. Pause so normal redispatch can build an attested bundle.
            logger.warning(
                "[%s] In-process Kubernetes workspace upgrade is disabled: "
                "exact runtime delivery authority is unavailable",
                job_id,
            )
            return False

        logger.info(
            f"[{job_id}] In-process workspace upgrade requested → {target_tier}"
        )

        # 1. Provision (server-side grant-gated, fail-closed). The job stays
        #    'processing' — no pause, no re-dispatch.
        try:
            ok = await self._orchestrator_client.request_job_workspace_upgrade(
                job_id, target_tier
            )
        except Exception as e:
            logger.error(f"[{job_id}] Workspace upgrade provision request errored: {e}")
            return False
        if not ok:
            logger.warning(
                f"[{job_id}] Workspace upgrade refused or failed at the orchestrator"
            )
            return False

        # 2. Poll for the ready connection block.
        ws_config = await self._poll_job_workspace_ready(job_id, timeout=300)
        if not ws_config or not ws_config.get("remote"):
            logger.warning(
                f"[{job_id}] Upgraded workspace did not become ready in time"
            )
            return False

        # 3. Build + connect the new sandbox backend. Connect BEFORE the seed so
        #    BOTH backends are live for the copy (mirrors the session handler).
        try:
            from shared.runtime.core.backends.remote import RemoteBackend

            remote = ws_config["remote"]
            shell_config = self.config.extra.get("shell", {})
            new_backend = RemoteBackend(
                host=remote["host"],
                port=remote.get("port", 22),
                username=remote.get("username", "agent-host"),
                key_path=remote.get("key_path"),
                workspace_path=remote.get(
                    "workspace_path", "/home/agent-host/workspace"
                ),
                job_id=job_id,
                scrollback_limit=shell_config.get("scrollback_limit", 5000),
                default_timeout=shell_config.get("default_timeout", 120),
                max_tabs=shell_config.get("max_tabs", 15),
                blocked_commands=shell_config.get("blocked_commands"),
                connect_timeout=remote.get("connect_timeout", 30),
                max_retries=remote.get("max_retries", 5),
                retry_timeouts_as_booting=remote.get(
                    "retry_timeouts_as_booting", False
                ),
                # sandbox keeps the sudo gate ("freeze") so its sudo→VM
                # escalation path still fires; only a vm target would set "allow".
                sudo_action=shell_config.get("sudo_action", "freeze"),
                sudo_block_message=shell_config.get("sudo_block_message"),
            )
            await asyncio.to_thread(new_backend.connect)
        except Exception as e:
            logger.error(f"[{job_id}] Failed to connect upgraded backend: {e}")
            return False

        # 4. Seed virtual → sandbox (both live), verify-before-flip.
        try:
            from agent.core.backends.seed import seed_workspace

            n = await asyncio.to_thread(seed_workspace, old_backend, new_backend)
            logger.info(f"[{job_id}] Seeded {n} file(s) into upgraded workspace")
        except Exception as e:
            logger.error(f"[{job_id}] Workspace seed failed: {e}")
            try:
                await asyncio.to_thread(new_backend.disconnect)
            except Exception:
                pass
            return False

        # 5. Swap the backend on the WorkspaceManager, drop the old virtual one.
        #    swap_backend() (not `_backend = ...`) so the virtual overlay is
        #    rebound onto the new backend and the already-registered providers
        #    keep serving — a direct assignment unwraps the overlay and every
        #    virtual path, deferred-tool docs included, 404s from here on.
        self._workspace_manager.swap_backend(new_backend)
        try:
            await asyncio.to_thread(old_backend.disconnect)
        except Exception:
            pass
        # Remove the stale freeze marker the graph wrote on freeze (seeded
        # across): the job continues in-process, it is NOT frozen for the
        # orchestrator.
        try:
            if new_backend.exists("output/job_frozen.json"):
                new_backend.delete_file("output/job_frozen.json")
        except Exception:
            pass

        # 6. Re-derive tools + shell manager for the new (shell-capable) backend.
        self._cleanup_shell_manager()
        await self._setup_job_tools()

        # 7. Rebuild the graph with the new tools/LLMs/tool_context but the SAME
        #    checkpointer, so the re-invoke resumes from the local checkpoint.
        snapshot_manager = PhaseSnapshotManager(job_id, workspace_backend=new_backend)
        self._graph = build_phase_alternation_graph(
            llm_with_tools=self._llm_with_tools,
            tools=self._tools,
            config=self.config,
            workspace=self._workspace_manager,
            todo_manager=self._todo_manager,
            workspace_template="",
            checkpointer=self._checkpointer,
            auxiliary_llm=self._auxiliary_llm,
            snapshot_manager=snapshot_manager,
            tool_context=self._tool_context,
            postgres_db=self.postgres_conn,
        )
        logger.info(
            f"[{job_id}] Workspace upgraded to {target_tier}; graph rebuilt with "
            f"shell/git tools — resuming in process"
        )
        return True

    def _load_workspace_template(self) -> str:
        """Load the workspace.md template for the nested loop graph.

        Uses InstructionMatrixResolver for model-aware resolution.

        Returns:
            Template content for workspace.md
        """
        from shared.runtime.core.loader import InstructionMatrixResolver
        from shared.runtime.core.model_registry import family_of

        # Check for pre-resolved content
        resolved = self.config.extra.get("_resolved_instructions", {})
        if resolved.get("workspace_template"):
            return resolved["workspace_template"]

        model_family = family_of(self.config.llm.model)
        resolver = InstructionMatrixResolver(self.config._deployment_dir, model_family)
        return resolver.load("workspace_template")

    async def _resolve_uploaded_instructions(
        self, metadata: Dict[str, Any]
    ) -> Optional[str]:
        """Resolve upload-sourced instructions.md content (async I/O).

        A virtual file's read() is synchronous, so the upload source (HTTP
        download via the orchestrator, with a local ``uploads/<id>`` fallback)
        must be resolved eagerly, here, rather than lazily inside the
        provider. Inline instructions need no such care — the provider reads
        ``metadata["instructions"]`` live at serve time.

        Returns:
            The resolved content, or None when there is no upload id or the
            upload could not be found by either path (the caller's template
            fallback then applies).
        """
        # Priority inline > upload (mirrors the deleted if/elif): don't pay for
        # a download — HTTP round-trip plus a local glob — when inline content
        # will win anyway.
        if (metadata.get("instructions") or "").strip():
            return None

        instr_upload_id = metadata.get("instructions_upload_id")
        if not instr_upload_id:
            return None

        from agent.core.workspace import get_workspace_base_path
        import tempfile

        instructions_written = False
        resolved_content: Optional[str] = None

        # Try HTTP download first
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            downloaded_files = await self._download_upload_files(
                instr_upload_id, temp_path, logger
            )

            if downloaded_files:
                # Find the instructions file from downloaded files
                instr_files = list(temp_path.glob("*.md")) + list(
                    temp_path.glob("*.txt")
                )
                if instr_files:
                    uploaded_instr_path = instr_files[0]
                    resolved_content = uploaded_instr_path.read_text(encoding="utf-8")
                    logger.info(
                        f"Copied uploaded instructions (HTTP): {uploaded_instr_path.name}"
                    )
                    instructions_written = True

        # Fall back to local filesystem
        if not instructions_written:
            instr_uploads_dir = get_workspace_base_path() / "uploads" / instr_upload_id

            if instr_uploads_dir.exists():
                # Find the instructions file (.md or .txt)
                instr_files = list(instr_uploads_dir.glob("*.md")) + list(
                    instr_uploads_dir.glob("*.txt")
                )
                if instr_files:
                    uploaded_instr_path = instr_files[0]
                    resolved_content = uploaded_instr_path.read_text(encoding="utf-8")
                    logger.info(
                        f"Copied uploaded instructions (local): {uploaded_instr_path.name}"
                    )
                    instructions_written = True
                else:
                    logger.warning(
                        f"No .md/.txt files found in instructions upload: {instr_upload_id}"
                    )
            else:
                logger.warning(
                    f"Instructions upload directory not found: {instr_uploads_dir}"
                )

        # instructions_written stays False here only when both sources failed;
        # resolved_content is already None in that case (the caller's template
        # fallback then applies).
        return resolved_content

    def _reseed_from_snapshot_if_fresh(self, job_id: str, workspace_backend) -> bool:
        """Restore the last phase snapshot onto a workspace that lost its content.

        Only for a genuinely fresh workspace. ``recover_to_phase`` overwrites
        ``checkpoint.db``, ``plan.md``, ``todos.yaml`` and ``archive/``
        (``src/agent/core/phase_snapshot.py``), so firing it on a same-pod resume
        (cooldown pause/resume, freeze-continue, outage-sweeper redispatch)
        silently rewinds the job to the last phase boundary. The seeded-content
        marker is what distinguishes the two; probing a *virtual* file would
        answer "unseeded" on every resume and rewind every one of them.

        Extracted from ``_setup_job_workspace`` so the decision is testable on
        its own. Never raises.

        Returns:
            True when a snapshot was pushed onto the workspace.
        """
        try:
            from agent.core.backends.seed import workspace_is_seeded

            if workspace_is_seeded(workspace_backend):
                return False

            logger.info(
                f"VM workspace is fresh — seeding from last snapshot for job {job_id}"
            )
            from agent.core.phase_snapshot import PhaseSnapshotManager

            recovery_mgr = PhaseSnapshotManager(
                job_id, workspace_backend=workspace_backend
            )
            latest = recovery_mgr.get_latest_snapshot()
            if not latest:
                logger.warning("No snapshots available to seed VM workspace")
                return False

            # Ensure base directories exist on VM
            for subdir in self.config.workspace.structure:
                try:
                    workspace_backend.mkdir(subdir.rstrip("/"))
                except Exception:
                    pass
            # Push snapshot files to VM
            recovery_mgr.recover_to_phase(
                latest.phase_number,
                workspace_manager=self._workspace_manager,
            )
            logger.info(
                f"Seeded VM workspace from phase {latest.phase_number} snapshot"
            )
            return True
        except Exception as e:
            logger.warning(f"VM workspace seeding failed: {e}")
            return False

    async def _hydrate_dispatched_config(
        self,
        job_id: str,
        metadata: Dict[str, Any],
        *,
        resume: bool,
    ) -> bool:
        """Hydrate the authoritative config snapshot for one dispatch.

        An in-flight orchestrator blob wins over the database snapshot because
        it contains credentials re-injected specifically for this delivery.
        Older orchestrators omit the field, preserving the existing resume
        fallback to the write-once database snapshot (and then local config).
        """
        delivered = metadata.get("resolved_config")
        if delivered:
            self.config = load_config_from_resolved(delivered)
            logger.info(f"Hydrated orchestrator-resolved config for job {job_id}")
            return True

        if resume and self.postgres_conn:
            try:
                import uuid as _uuid

                resolved = await self.postgres_conn.jobs.get_resolved_config(
                    _uuid.UUID(job_id)
                )
                if resolved:
                    self.config = load_config_from_resolved(resolved)
                    logger.info(f"Loaded frozen config for resumed job {job_id}")
                    return True
            except Exception as e:
                logger.warning(
                    f"Failed to load frozen config, falling back to disk: {e}"
                )

        return False

    async def _setup_job_workspace(
        self,
        job_id: str,
        metadata: Optional[Dict[str, Any]] = None,
        resume: bool = False,
    ) -> Dict[str, Any]:
        """Set up the workspace for a job.

        Creates the workspace directory structure, copies initial files
        (instructions, documents, etc.), and returns updated metadata
        with workspace-relative paths.

        Base path is determined by:
        1. WORKSPACE_PATH environment variable
        2. /workspace (container mode)
        3. ./workspace (development mode)

        Args:
            job_id: Unique job identifier
            metadata: Job metadata (may contain document_path, etc.)

        Returns:
            Updated metadata with workspace-relative paths
        """
        metadata = metadata or {}
        # This internal bearer must not survive into graph/checkpoint metadata.
        # Pop it before config resolution, logging, or any early-return branch.
        managed_repository_credentials = metadata.pop(
            "managed_repository_credentials", None
        )
        # Fresh capture per job: files we write on top of the pod's git clone,
        # re-asserted if a pod re-provision drops them (see the reconnect hook).
        self._agent_seed_files = {}

        # Resolve upload-sourced instructions.md eagerly, once, before any of
        # the branches below (fresh init, resume-with-existing-workspace,
        # pod-handoff clone, PVC reattach) can return early. Every one of
        # those branches leads to _deploy_instruction_files() registering a
        # virtual instructions.md provider that reads
        # self._resolved_instructions_md — a virtual file persists nothing
        # between runs, so a resumed job whose instructions came from an
        # upload would otherwise silently fall back to the template on
        # whichever branch happened to fire. `metadata` is read-only with
        # respect to these keys for the rest of this function (verified: only
        # `updated_metadata`, a separate copy, is mutated below), so resolving
        # here is equivalent to resolving in each branch — just impossible to
        # accidentally miss one. Cheap when there is no upload (the helper
        # returns immediately); non-fatal on failure (falls through to
        # inline/template).
        try:
            self._resolved_instructions_md = await self._resolve_uploaded_instructions(
                metadata
            )
        except Exception as e:
            logger.warning(f"Instructions upload resolution failed (non-fatal): {e}")
            self._resolved_instructions_md = None

        # A delivered blob is already fully merged, credential-injected, and
        # frozen by the orchestrator. It must win over the secret-free database
        # snapshot on resume; otherwise config_override=None would leave the
        # hydrated LLM without the credentials carried by the delivery blob.
        # Absent blob -> the historical DB/local fallback remains unchanged.
        _config_from_db = await self._hydrate_dispatched_config(
            job_id, metadata, resume=resume
        )

        # Handle expert config name - load the named config (tools, prompts, workspace settings)
        # This must happen before config_upload_id and config_override so those can further override
        if not _config_from_db and metadata.get("config_name"):
            from shared.runtime.core.loader import (
                _apply_settings_matrix,
                authored_llm_keys,
                load_and_merge_config,
                load_agent_config_from_dict,
                resolve_bundled_config_path,
            )

            expert_name = metadata["config_name"]
            try:
                config_path, deployment_dir = resolve_bundled_config_path(expert_name)
                logger.info(f"Loading expert config '{expert_name}' from {config_path}")
                merged_config_data = load_and_merge_config(config_path)

                # Apply settings_matrix (model-family defaults) — mirrors
                # load_agent_config(): the leaf's own llm keys are explicit (for
                # a role root, the overlay + expert_base pair).
                raw_expert_llm_keys: set[str] = authored_llm_keys(config_path)
                _apply_settings_matrix(
                    merged_config_data, raw_expert_llm_keys, deployment_dir
                )
                # Materialise the subagent roster like load_agent_config()
                # does (disk refs; a stale ref must not kill a running pod).
                if merged_config_data.get("subagents") is not None:
                    from shared.runtime.core.subagent_roster import (
                        resolve_subagent_roster,
                    )

                    merged_config_data = resolve_subagent_roster(
                        merged_config_data,
                        db_refs={},
                        deployment_dir=deployment_dir,
                        on_missing="drop",
                    )

                self.config = load_agent_config_from_dict(
                    merged_config_data, deployment_dir=deployment_dir
                )
                logger.info(
                    f"Applied expert config '{expert_name}' (tools: {list(self.config.tools.__dict__.keys())})"
                )
            except Exception as e:
                logger.warning(
                    f"Failed to load expert config '{expert_name}': {e}. Continuing with current config."
                )

        # Handle config upload - load and merge with defaults BEFORE workspace setup
        # This must happen first since config affects workspace settings
        if not _config_from_db and metadata.get("config_upload_id"):
            config_upload_id = metadata["config_upload_id"]
            from agent.core.workspace import get_workspace_base_path
            from shared.runtime.core.loader import (
                load_uploaded_config,
                load_agent_config_from_dict,
            )
            import tempfile

            config_loaded = False

            # Try HTTP download first
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                downloaded_files = await self._download_upload_files(
                    config_upload_id, temp_path, logger
                )

                if downloaded_files:
                    # Find the YAML file from downloaded files
                    yaml_files = list(temp_path.glob("*.yaml")) + list(
                        temp_path.glob("*.yml")
                    )
                    if yaml_files:
                        uploaded_config_path = yaml_files[0]
                        logger.info(
                            f"Loading uploaded config (HTTP): {uploaded_config_path.name}"
                        )

                        # Load and merge with defaults
                        merged_config_data = load_uploaded_config(uploaded_config_path)

                        # Create new config object (replaces self.config for this job)
                        self.config = load_agent_config_from_dict(merged_config_data)
                        logger.info("Applied uploaded config overrides")
                        config_loaded = True

            # Fall back to local filesystem
            if not config_loaded:
                config_uploads_dir = (
                    get_workspace_base_path() / "uploads" / config_upload_id
                )

                if config_uploads_dir.exists():
                    # Find the YAML file
                    yaml_files = list(config_uploads_dir.glob("*.yaml")) + list(
                        config_uploads_dir.glob("*.yml")
                    )
                    if yaml_files:
                        uploaded_config_path = yaml_files[0]
                        logger.info(
                            f"Loading uploaded config (local): {uploaded_config_path.name}"
                        )

                        # Load and merge with defaults
                        merged_config_data = load_uploaded_config(uploaded_config_path)

                        # Create new config object (replaces self.config for this job)
                        self.config = load_agent_config_from_dict(merged_config_data)
                        logger.info("Applied uploaded config overrides")
                    else:
                        logger.warning(
                            f"No YAML files found in config upload: {config_upload_id}"
                        )
                else:
                    logger.warning(
                        f"Config upload directory not found: {config_uploads_dir}"
                    )

        # Handle inline config override - merge on top of current config.
        # Runs even when _config_from_db (resume path) so the orchestrator's
        # re-injected credentials (api_key/base_url stripped from frozen
        # config) can layer back onto the merged config before LLMs are
        # created. Without this, resumed jobs hit the user's router with
        # api_key=None and 401.
        if metadata.get("config_override"):
            from shared.runtime.core.loader import (
                _apply_settings_matrix,
                deep_merge,
                load_agent_config_from_dict,
                normalize_delegation_block,
                normalize_llm_tiers,
                strip_loader_owned_keys,
            )
            from shared.runtime.core.tool_policy import normalize_tool_policy
            import dataclasses

            # Non-hydrated dispatch delivers the request-layer override raw, so
            # the agent is where its tool policy has to be resolved. (On the
            # orchestrator-resolved path config_override is None and the blob
            # already carries canonical lists.) Same seam for a legacy
            # llm.strategic/tactical pin: the whole block — model plus the
            # transport the dispatcher injected into it — lifts to llm.model.
            # It is caller-authored either way: loader-owned keys (the DB-
            # prompt fence markers, pre-resolved content) are stripped before
            # it can merge onto config.extra.
            config_override = strip_loader_owned_keys(
                normalize_delegation_block(
                    normalize_llm_tiers(
                        normalize_tool_policy(
                            metadata["config_override"], source="job-override"
                        ),
                        source="job-override",
                    ),
                    source="job-override",
                )
            )
            logger.info(
                f"Applying inline config override: {list(config_override.keys())}"
            )

            # Convert current config to dict, merge, and reload
            # Preserve _deployment_dir so instruction file resolution still works
            prev_deployment_dir = self.config._deployment_dir
            current_config_dict = dataclasses.asdict(self.config)
            merged_config_data = deep_merge(current_config_dict, config_override)

            # If the override changes the model, re-apply settings_matrix for the
            # new model family. Override LLM keys are treated as "explicitly set"
            # so the matrix won't overwrite them.
            if config_override.get("llm"):
                override_llm_keys = set((config_override.get("llm") or {}).keys())
                _apply_settings_matrix(
                    merged_config_data, override_llm_keys, prev_deployment_dir
                )

            self.config = load_agent_config_from_dict(
                merged_config_data, deployment_dir=prev_deployment_dir
            )
            logger.info("Applied inline config overrides")

        # Apply env_keys overrides (user/project API keys for non-LLM providers).
        # In the blob-delivery path the orchestrator sends config_override=None and
        # carries the credentials inside resolved_config.agent.env_keys (via
        # inject_blob_credentials). Fall back to the blob so embedding / vision /
        # whisper / tts / citation keys still reach os.environ — otherwise the
        # embedding-backed memory + KB silently fail for every blob-delivered job.
        # knowledge-history/done/embedding_key_missing_silently_disables_memory_and_kb.md
        env_keys = (metadata.get("config_override") or {}).get("env_keys")
        if not env_keys:
            env_keys = ((metadata.get("resolved_config") or {}).get("agent") or {}).get(
                "env_keys"
            )
        # KB embedding transport is authoritative per dispatch. Pool/loop agents
        # can process a later job after the system profile changes (or a job with
        # no knowledge scope), so clear every old field before applying the new
        # in-flight block. Otherwise an omitted BASE_URL/API_KEY can survive and
        # pair a new model with the previous endpoint/credential.
        import os as _os
        from shared.runtime.services.embedding_service import (
            KB_EMBEDDING_ENV_KEYS,
            apply_kb_embedding_env,
        )

        apply_kb_embedding_env(env_keys)
        if env_keys:
            for k, v in env_keys.items():
                if k not in KB_EMBEDDING_ENV_KEYS:
                    _os.environ[k] = v
            # Log the key NAMES (never values) so a missing credential — e.g.
            # EMBEDDING_API_KEY, which silently disables memory + KB — is
            # greppable in the agent log.
            logger.info(
                f"Applied {len(env_keys)} env key override(s): "
                f"{sorted(env_keys.keys())}"
            )

        # Recreate LLMs if config was modified for this job. On resume we
        # always recreate when frozen config was loaded — frozen config has
        # api_key stripped and the override applied above carries the
        # re-injected credentials. Without recreation the strategic/tactical
        # LLMs would still hold whatever was built at agent boot (if any) or
        # would never be created at all.
        config_dirty = bool(
            metadata.get("config_name")
            or metadata.get("config_upload_id")
            or metadata.get("config_override")
        )

        # Load DB config overrides (flag-gated; fail-open). MUST precede
        # _create_phase_llms() so settings overrides reach the LLMs, and precede
        # the freeze so they're captured. Prompts/instructions/guardrails resolve
        # lazily from the process map; settings are eager -> apply onto self.config.
        if self.postgres_conn and not resume and not _config_from_db:
            from shared.runtime.core.loader import (
                apply_settings_overrides,
                set_config_overrides,
                _is_config_db_overrides_enabled,
            )

            if _is_config_db_overrides_enabled():
                try:
                    from shared.runtime.core.model_registry import family_of

                    _family = family_of(self.config.llm.model)
                    _rows = await self.postgres_conn.config_overrides.list_overrides_for_family(
                        _family
                    )
                    set_config_overrides(_rows)
                    if apply_settings_overrides(self.config):
                        config_dirty = True
                    logger.info(
                        f"Loaded {len(_rows)} config override(s) for family {_family}"
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to load config overrides (using bundled): {e}"
                    )

        if (not _config_from_db and config_dirty) or _config_from_db:
            logger.info("Config changed for this job — recreating LLMs")
            self._create_phase_llms()

        # Freeze resolved config on first run (not resume). The overrides loaded
        # above are captured here: settings via self.config, prompts/instructions
        # via the resolver reading the process map.
        if self.postgres_conn and not resume and not _config_from_db:
            try:
                import uuid as _uuid

                from shared.runtime.core.loader import serialize_resolved_config

                resolved = serialize_resolved_config(
                    self.config, model=self.config.llm.model
                )
                # Backend warning surface: mismatch notes from phase-model
                # reconciliation (mixed family/window/multimodal). Extra top-level
                # key — load_config_from_resolved only reads resolved["agent"], so
                # this is ignored on reload.
                warnings_list = getattr(self, "_model_config_warnings", [])
                if warnings_list:
                    resolved["model_config_warnings"] = warnings_list
                await self.postgres_conn.jobs.store_resolved_config(
                    _uuid.UUID(job_id), resolved
                )
                logger.info(f"Froze resolved config for job {job_id}")
            except Exception as e:
                logger.warning(f"Failed to freeze resolved config: {e}")

        # Create workspace backend. The no-workspace tiers (virtual/none) run
        # with no workspace pod — build the lite backend directly. Otherwise
        # the agent never operates on its own filesystem: sandbox/vm require
        # SSH credentials injected by the orchestrator at dispatch time.
        from agent.core.backends.factory import LITE_BACKENDS, create_lite_backend

        from shared.workspace_contract import (
            stateless_worker_backend_admissible,
            vm_mode_from_env,
        )

        if (
            self._worker_lease_token is not None
            and not stateless_worker_backend_admissible(
                self.config.workspace.backend, vm_mode=vm_mode_from_env()
            )
        ):
            raise RuntimeError(
                "The stateless worker lane admits Kubernetes-pod sandbox "
                "workspaces and same-cluster VM workspaces; external VM and "
                "lite jobs remain pinned"
            )

        if self.config.workspace.backend in LITE_BACKENDS:
            try:
                workspace_backend = create_lite_backend(
                    self.config.workspace, job_id=job_id
                )
                workspace_backend.connect()
                logger.info(
                    "Lite workspace backend ready (backend=%s, no workspace pod)",
                    self.config.workspace.backend,
                )
            except Exception as e:
                logger.error(f"Failed to create lite backend: {e}")
                raise
        else:
            if self.config.workspace.backend not in ("sandbox", "vm"):
                raise RuntimeError(
                    f"Unsupported workspace.backend={self.config.workspace.backend!r}. "
                    f"The agent requires backend='sandbox' or 'vm' with SSH "
                    f"credentials injected by the orchestrator."
                )
            if not self.config.workspace.remote:
                raise RuntimeError(
                    f"workspace.backend={self.config.workspace.backend!r} but no "
                    f"workspace.remote config was provided. The orchestrator must "
                    f"inject SSH credentials pointing at a provisioned workspace "
                    f"container or VM."
                )

            try:
                from shared.runtime.core.backends.remote import RemoteBackend

                remote_cfg = self.config.workspace.remote
                shell_config = self.config.extra.get("shell", {})
                worker_remote_authority = _remote_workspace_authority(
                    metadata,
                    self._worker_lease_token,
                    backend=self.config.workspace.backend,
                )
                workspace_backend = RemoteBackend(
                    host=remote_cfg["host"],
                    port=remote_cfg.get("port", 22),
                    username=remote_cfg.get("username", "agent-host"),
                    key_path=remote_cfg.get("key_path"),
                    workspace_path=remote_cfg.get(
                        "workspace_path", "/home/agent-host/workspace"
                    ),
                    job_id=job_id,
                    scrollback_limit=shell_config.get("scrollback_limit", 5000),
                    default_timeout=shell_config.get("default_timeout", 120),
                    max_tabs=shell_config.get("max_tabs", 15),
                    blocked_commands=shell_config.get("blocked_commands"),
                    connect_timeout=remote_cfg.get("connect_timeout", 30),
                    max_retries=remote_cfg.get("max_retries", 5),
                    retry_timeouts_as_booting=remote_cfg.get(
                        "retry_timeouts_as_booting", False
                    ),
                    sudo_action=shell_config.get("sudo_action", "freeze"),
                    sudo_block_message=shell_config.get("sudo_block_message"),
                    **worker_remote_authority,
                )
                if self._worker_lease_token is not None:
                    workspace_backend.set_shell_owner_token(self._worker_lease_token)
                workspace_backend.connect()
                if self._worker_lease_token is not None:
                    # Eager promotion fences a predecessor even when this batch
                    # happens to be LLM-only and never opens a shell tool.
                    workspace_backend.claim_shell_owner()
                logger.info(
                    f"Remote workspace backend connected to {remote_cfg['host']}"
                )
            except Exception as e:
                logger.error(f"Failed to create remote backend: {e}")
                raise

        from shared.runtime.core.managed_repository import (
            ManagedRepositoryMaterializationError,
            materialize_managed_repository_credentials,
            repository_url_has_credentials,
        )
        from urllib.parse import urlparse

        runtime_repository_urls = materialize_managed_repository_credentials(
            managed_repository_credentials, workspace_backend
        )
        del managed_repository_credentials
        primary_url = metadata.get("git_remote_url")
        if repository_url_has_credentials(primary_url):
            raise ManagedRepositoryMaterializationError(
                "credentialed_managed_repository_url_refused"
            )
        if primary_url:
            primary_name = (
                urlparse(str(primary_url))
                .path.rstrip("/")
                .rsplit("/", 1)[-1]
                .removesuffix(".git")
            )
            if str(primary_url).startswith("ssh://srw-repo-"):
                if primary_name not in runtime_repository_urls:
                    raise ManagedRepositoryMaterializationError(
                        "managed_repository_transport_mismatch"
                    )
                metadata["git_remote_url"] = runtime_repository_urls[primary_name]
        rendered_repositories: list[dict[str, Any]] = []
        for raw_repository in metadata.get("repositories") or []:
            repository = dict(raw_repository)
            repo_url = repository.get("repo_url")
            if repository.get("is_managed") and repository_url_has_credentials(
                repo_url
            ):
                raise ManagedRepositoryMaterializationError(
                    "credentialed_managed_repository_url_refused"
                )
            if repository.get("is_managed"):
                repo_name = str(repository.get("name") or "")
                role = str(repository.get("role") or "")
                runtime_url = runtime_repository_urls.get(repo_name)
                if runtime_url is not None:
                    repository["repo_url"] = runtime_url
                elif role not in {"knowledge", "jobs"}:
                    # Source/reference repositories are cloned into the agent
                    # workspace, so a managed row without its exact scoped
                    # runtime authority must fail closed. Knowledge and the
                    # project jobs ledger are server-side planes that
                    # WorkspaceManager deliberately does not clone.
                    raise ManagedRepositoryMaterializationError(
                        "managed_repository_transport_mismatch"
                    )
                repository["credentials"] = None
            rendered_repositories.append(repository)
        if rendered_repositories:
            metadata["repositories"] = rendered_repositories

        # Worktree creation: subjobs on shared VM/container get a git worktree
        # instead of a full clone. The worktree is created on the remote machine.
        worktree_path = metadata.get("worktree_path")
        if worktree_path and workspace_backend and workspace_backend.supports_shell:
            parent_workspace = "/home/agent-host/workspace"
            branch_name = metadata.get("branch_name", "main")
            try:
                # Fetch the subjob branch (created by orchestrator on Gitea)
                workspace_backend._exec(
                    f"git -C {parent_workspace} fetch origin {branch_name}",
                    timeout=60,
                )
                # Create worktree directory parent
                workspace_backend._exec(
                    f"mkdir -p $(dirname {worktree_path})",
                    timeout=10,
                )
                # Create worktree for the subjob's branch
                workspace_backend._exec(
                    f"git -C {parent_workspace} worktree add {worktree_path} {branch_name}",
                    timeout=30,
                )
                logger.info(
                    f"Created git worktree at {worktree_path} (branch: {branch_name})"
                )
            except Exception as e:
                logger.warning(
                    f"Failed to create git worktree at {worktree_path}: {e}. "
                    "Falling back to standard workspace init."
                )
                # Clear worktree_path so we fall through to normal init
                worktree_path = None
                metadata.pop("worktree_path", None)

        # Create workspace manager
        # Lite tiers (virtual/none) have no git: their backend storage is not
        # the local anchor path git would track, and git needs a shell they
        # lack (no_workspace_agent_mode.md §8). Force versioning off regardless
        # of the resolved config so a virtual/none job never attempts git init.
        _git_versioning = self.config.workspace.git_versioning and (
            self.config.workspace.backend not in LITE_BACKENDS
        )
        self._workspace_manager = WorkspaceManager(
            job_id=job_id,
            config=WorkspaceManagerConfig(
                structure=self.config.workspace.structure,
                git_versioning=_git_versioning,
                git_remote_url=metadata.get("git_remote_url"),
                branch_name=metadata.get("branch_name"),
                repositories=metadata.get("repositories"),
            ),
            backend=workspace_backend,
        )

        # Re-seed agent-authored files after a pod tear-down + re-provision. The
        # pod comes back with its git clone (instructions.md, tools/) but not the
        # files the agent wrote on top (task_brief.md, bound skills); a genuine
        # SSH reconnect fires this hook, which restores whatever is now absent
        # from self._agent_seed_files (populated by the seed tail below and
        # _deploy_instruction_files). Read at fire time, so ordering is fine.
        # Lite/virtual backends have no pod to lose and lack the hook.
        if hasattr(workspace_backend, "set_reconnect_hook"):
            from agent.core.backends.seed import reseed_missing_files

            workspace_backend.set_reconnect_hook(
                lambda b=workspace_backend: reseed_missing_files(
                    b, self._agent_seed_files
                )
            )

        # VM recovery: seed fresh VM workspace from last snapshot if needed
        _seeded_from_snapshot = False
        if resume and workspace_backend and workspace_backend.supports_shell:
            _seeded_from_snapshot = self._reseed_from_snapshot_if_fresh(
                job_id, workspace_backend
            )

        # G2: reattached remote workspace (PVC reattach on crash-recovery). The
        # working tree already lives on the REMOTE backend root, so the
        # local-path gates below would miss it. Detect a real working tree on the backend
        # (`.git`; a fresh/empty PVC has none, so first dispatch still
        # initializes) and PRESERVE it: attach a git handle to the existing repo
        # and resume on the intact files. Gated on
        # `resume`, so any content present belongs to THIS job's continuation
        # (PVCs are owner-keyed by UUID).
        # See knowledge-base/knowledge/features/workspace_pvc_branch_a_implementation.md (G2 / Phase 2).
        _reattached = False
        if (
            resume
            and workspace_backend
            and getattr(workspace_backend, "supports_shell", False)
        ):
            try:
                _reattached = workspace_backend.exists(".git")
            except Exception as e:
                logger.warning(f"Reattach probe failed for job {job_id}: {e}")
                _reattached = False
        if _reattached:
            logger.info(
                f"Reattached workspace detected for job {job_id} — preserving "
                f"existing files (no clone, no re-init)"
            )
            if _git_versioning and self._workspace_manager.git_manager is None:
                from agent.managers.git_manager import GitManager

                # Attach a handle to the existing remote repo. No clone (the dir
                # is non-empty); the repo's own git config persists on the PVC.
                git_mgr = GitManager(
                    self._workspace_manager.path, backend=workspace_backend
                )
                self._workspace_manager._git_manager = git_mgr
                self._workspace_manager._initialized = True
                if metadata.get("git_remote_url"):
                    git_mgr.add_remote("origin", metadata["git_remote_url"])
            # Re-point the tree at the branch this job owns — whether the handle
            # was just attached above or one already existed. A re-attached PVC
            # keeps whatever branch its previous occupant (possibly a subjob)
            # left checked out.
            ensure_job_branch(self._workspace_manager.git_manager, metadata, job_id)
            # instructions.md is virtual (knowledge-base/knowledge/features/virtual_directories.md)
            # and cannot go missing, so the old "rewrite if vanished" guard is
            # gone; the upload source (if any) was already resolved at the top
            # of this function, before this branch could return early.
            self._todo_manager = TodoManager(
                workspace=self._workspace_manager,
                min_todos=self.config.phase_settings.min_todos,
                max_todos=self.config.phase_settings.max_todos,
                model_name=self.config.llm.model,
            )
            logger.info(f"[{job_id}] workspace_init_path=reattach")
            logger.debug(
                f"Resumed job {job_id} on reattached workspace at "
                f"{workspace_backend.root}"
            )
            return metadata or {}

        def _backend_has(rel: str) -> bool:
            """Probe the REAL workspace backend, treating failures as absent.

            Bypasses the virtual overlay on purpose: instructions.md and
            task_brief.md are virtual and always "exist", so probing through
            the overlay would report every fresh pod as seeded. The question
            here is strictly "did real seeded content survive?".

            The gates below used local ``Path.exists()`` checks, which are
            always False for a remote workspace — pod handoff degenerated to
            "always try the clone" and the resume-existing branch was dead
            code, letting content-bearing git-less workspaces fall through to
            initialize()'s ``rm -rf``.
            """
            from agent.core.backends.overlay import unwrap_backend

            probe = unwrap_backend(self._workspace_manager.backend)
            try:
                return probe.exists(rel)
            except Exception as e:
                logger.warning(f"Workspace probe for {rel!r} failed: {e}")
                return False

        from agent.core.backends.seed import mark_workspace_seeded, workspace_is_seeded

        # Pod handoff: clone workspace from Gitea if resuming on a new pod
        # (no git working tree yet — G2 above already preserved and returned
        # for any tree that has one).
        if resume and metadata.get("git_remote_url") and not _backend_has(".git"):
            from agent.managers.git_manager import GitManager

            logger.info(f"Pod handoff: cloning workspace for job {job_id}")
            git_mgr = GitManager.clone(
                metadata["git_remote_url"],
                self._workspace_manager.path,
                backend=self._workspace_manager.backend,
            )
            if git_mgr:
                # Land on the branch this job owns. `create=True` because the
                # clone has the remote: provisioning logs a failed branch
                # creation but still writes `branch_name` to the DB
                # (services/job_provisioning.py:167-188), so the DB can name a
                # branch Gitea lacks. A plain checkout returns False silently
                # there, leaving the tree on the clone default — work lands on
                # `main` while every reader resolves `job/<short_id>`. Creating
                # it locally lets the first push publish it, which is what
                # WorkspaceManager already does (core/workspace.py:550, :641).
                ensure_job_branch(git_mgr, metadata, job_id, create=True)

                self._workspace_manager._git_manager = git_mgr
                self._workspace_manager._initialized = True

                # Clone source/reference repos if project workspace
                if metadata.get("repositories"):
                    self._workspace_manager._clone_auxiliary_repos()

                self._todo_manager = TodoManager(
                    workspace=self._workspace_manager,
                    min_todos=self.config.phase_settings.min_todos,
                    max_todos=self.config.phase_settings.max_todos,
                    model_name=self.config.llm.model,
                )
                logger.info(f"[{job_id}] workspace_init_path=clone")
                logger.info(f"Pod handoff complete for job {job_id}")
                return metadata or {}
            logger.warning(
                f"Pod handoff clone failed for job {job_id}, falling through to normal init"
            )

        # Check if resuming an existing workspace. Content probe via the real
        # backend (the seeded-content marker — the same probe the VM snapshot
        # seeding uses): preserves a seeded-but-git-less workspace instead of
        # letting the fresh-init path below wipe it with initialize()'s
        # `rm -rf`. Probing a virtual file instead would answer "unseeded"
        # forever and wipe every git-less resume.
        if resume and workspace_is_seeded(self._workspace_manager.backend):
            logger.info(
                f"[{job_id}] workspace_init_path="
                f"{'snapshot' if _seeded_from_snapshot else 'existing'}"
            )
            logger.info(f"Resuming job {job_id} with existing workspace")
            # instructions.md is virtual (knowledge-base/knowledge/features/virtual_directories.md)
            # and cannot go missing; the upload source (if any) was already
            # resolved at the top of this function, before this branch could
            # return early.

            # Initialize git manager if git versioning is enabled (safe on existing repos)
            if (
                self._workspace_manager.config.git_versioning
                and self._workspace_manager.git_manager is None
            ):
                self._workspace_manager._initialize_git()
                self._workspace_manager._initialized = True

            # Ensure git remote is configured for workspace delivery
            if metadata.get("git_remote_url") and self._workspace_manager.git_manager:
                self._workspace_manager.git_manager.add_remote(
                    "origin", metadata["git_remote_url"]
                )

            # Ensure the tree is on the branch this job owns (not whatever a
            # previous occupant — e.g. a critic subjob — left checked out).
            ensure_job_branch(self._workspace_manager.git_manager, metadata, job_id)

            # Create todo manager for this workspace
            self._todo_manager = TodoManager(
                workspace=self._workspace_manager,
                min_todos=self.config.phase_settings.min_todos,
                max_todos=self.config.phase_settings.max_todos,
                model_name=self.config.llm.model,
            )

            logger.debug(f"Resumed workspace at {self._workspace_manager.path}")
            return metadata or {}

        # Initialize workspace (creates directories)
        if resume:
            # Every resume source struck out: no reattached tree, no job-repo
            # clone (git_remote_url absent or clone failed), no snapshot.
            # Loudly distinguishable from a legitimate first dispatch —
            # knowledge-base/knowledge/issues/resume_fresh_workspace_no_clone_fallback.md.
            logger.warning(
                f"[{job_id}] workspace_init_path=blank — resume found no "
                f"reattached tree, no clonable job repo, and no phase "
                f"snapshot; starting from an empty workspace"
            )
        if metadata.get("repositories"):
            self._workspace_manager.initialize_project_workspace()
        else:
            self._workspace_manager.initialize()

        # instructions.md / task_brief.md are virtual
        # (knowledge-base/knowledge/features/virtual_directories.md): served live by providers
        # registered in _deploy_instruction_files(), never written here.
        # Priority for instructions.md is inline > upload > template — inline
        # is read live from metadata at serve time, and the upload source (if
        # any) was already resolved at the top of this function.

        # Seeded-content marker, written exactly where the real task_brief.md
        # used to be: on the fresh-init path, after every resume branch has
        # returned. It is what the two probes above read, so it must mean "this
        # workspace was seeded for this job", not "a process booted".
        mark_workspace_seeded(self._workspace_manager.backend)

        # The repo landing page (README.md workspace-facts block) is written in
        # _setup_job_tools(), after documents are copied and connectors are
        # cloned, so it can list them. It carries facts only — the task stays
        # in the virtual task_brief.md.

        # Process initial_files from config (templates seeded into the workspace)
        if self.config.workspace.initial_files:
            config_dir = Path(__file__).resolve().parents[2] / "config" / "agents"
            for dest_path, source_path in self.config.workspace.initial_files.items():
                # Skip instructions.md - already handled above
                if dest_path == "instructions.md":
                    continue
                try:
                    source_full = config_dir / source_path
                    if source_full.exists():
                        content = source_full.read_text(encoding="utf-8")
                        self._workspace_manager.write_file(dest_path, content)
                        logger.debug(
                            f"Initialized file: {dest_path} from {source_path}"
                        )
                    else:
                        logger.warning(
                            f"Initial file template not found: {source_path}"
                        )
                except Exception as e:
                    logger.warning(f"Failed to initialize {dest_path}: {e}")

        # Clone git repository if git_url is provided (for coding agents)
        if metadata.get("git_url") and not resume:
            git_url = metadata["git_url"]
            git_branch = metadata.get("git_branch", "main")
            repo_dir = self._workspace_manager.get_path("repo")
            logger.info(
                f"Cloning {git_url} (branch: {git_branch}) into workspace/repo/"
            )
            try:
                import subprocess

                # Clone the repository
                clone_result = subprocess.run(
                    ["git", "clone", "--branch", git_branch, git_url, str(repo_dir)],
                    capture_output=True,
                    text=True,
                    timeout=300,
                )
                if clone_result.returncode == 0:
                    logger.info(f"Repository cloned successfully into {repo_dir}")
                else:
                    # Try cloning without --branch (branch might not exist yet)
                    clone_result2 = subprocess.run(
                        ["git", "clone", git_url, str(repo_dir)],
                        capture_output=True,
                        text=True,
                        timeout=300,
                    )
                    if clone_result2.returncode == 0:
                        # Create and checkout the branch
                        subprocess.run(
                            ["git", "checkout", "-b", git_branch],
                            cwd=str(repo_dir),
                            capture_output=True,
                            text=True,
                        )
                        logger.info(f"Repository cloned, created branch: {git_branch}")
                    else:
                        logger.error(f"Git clone failed: {clone_result2.stderr}")
            except subprocess.TimeoutExpired:
                logger.error("Git clone timed out after 300s")
            except Exception as e:
                logger.error(f"Git clone failed: {e}")

        # Copy documents to workspace if provided
        updated_metadata = dict(metadata)

        # Handle upload_id (files uploaded via orchestrator UI)
        if metadata.get("upload_id"):
            upload_id = metadata["upload_id"]
            from agent.core.workspace import get_workspace_base_path
            import tempfile

            copied_paths = []
            original_paths = []

            # Ensure documents directory exists (use backend for remote compat)
            backend = self._workspace_manager.backend
            backend.mkdir("documents")

            upload_source_dir = None

            # Try HTTP download first
            temp_dir_obj = tempfile.TemporaryDirectory()
            temp_path = Path(temp_dir_obj.name)
            downloaded_files = await self._download_upload_files(
                upload_id, temp_path, logger
            )

            if downloaded_files:
                upload_source_dir = temp_path
                logger.info(f"Processing documents from HTTP download: {upload_id}")
            else:
                # Fall back to local filesystem
                local_uploads_dir = get_workspace_base_path() / "uploads" / upload_id
                if local_uploads_dir.exists():
                    upload_source_dir = local_uploads_dir
                    logger.info(
                        f"Processing documents from local path: {local_uploads_dir}"
                    )
                else:
                    logger.warning(
                        f"Upload directory not found locally or via HTTP: {upload_id}"
                    )

            if upload_source_dir:
                for file_path in sorted(upload_source_dir.iterdir()):
                    # Skip metadata.json
                    if file_path.name == "metadata.json":
                        continue
                    if file_path.is_file():
                        # Check if zip - extract instead of copy
                        if file_path.suffix.lower() == ".zip":
                            extracted = self._extract_zip(
                                file_path, "documents", logger
                            )
                            copied_paths.extend(extracted)
                            original_paths.extend([str(file_path)] * len(extracted))
                            logger.info(
                                f"Processed zip file: {file_path.name} ({len(extracted)} files extracted)"
                            )
                        else:
                            # Regular file - copy via backend with conflict handling
                            dest_name = file_path.name
                            counter = 1
                            stem = Path(file_path.name).stem
                            suffix = Path(file_path.name).suffix
                            while backend.exists(f"documents/{dest_name}"):
                                dest_name = f"{stem}_{counter}{suffix}"
                                counter += 1

                            dest_relative = f"documents/{dest_name}"
                            backend.write_file(dest_relative, file_path.read_bytes())
                            logger.info(
                                f"Copied uploaded file to workspace: {dest_relative}"
                            )

                            copied_paths.append(dest_relative)
                            original_paths.append(str(file_path))

            # Clean up temp directory
            temp_dir_obj.cleanup()

            if copied_paths:
                updated_metadata["document_paths"] = copied_paths
                updated_metadata["original_document_paths"] = original_paths
                # For backwards compatibility, set document_path to first document
                updated_metadata["document_path"] = copied_paths[0]
                updated_metadata["original_document_path"] = original_paths[0]

        # Handle multiple documents (document_paths list)
        elif metadata.get("document_paths"):
            copied_paths = []
            original_paths = []

            # Ensure documents directory exists (use backend for remote compat)
            backend = self._workspace_manager.backend
            backend.mkdir("documents")

            for doc_path in metadata["document_paths"]:
                source_path = Path(doc_path)
                if source_path.exists():
                    # Check if zip - extract instead of copy
                    if source_path.suffix.lower() == ".zip":
                        extracted = self._extract_zip(source_path, "documents", logger)
                        copied_paths.extend(extracted)
                        original_paths.extend([str(source_path)] * len(extracted))
                        logger.info(
                            f"Processed zip file: {source_path.name} ({len(extracted)} files extracted)"
                        )
                    else:
                        # Regular file - copy via backend with conflict handling
                        dest_name = source_path.name
                        counter = 1
                        stem = Path(source_path.name).stem
                        suffix = Path(source_path.name).suffix
                        while backend.exists(f"documents/{dest_name}"):
                            dest_name = f"{stem}_{counter}{suffix}"
                            counter += 1

                        dest_relative = f"documents/{dest_name}"
                        backend.write_file(dest_relative, source_path.read_bytes())
                        logger.info(f"Copied document to workspace: {dest_relative}")

                        copied_paths.append(dest_relative)
                        original_paths.append(str(source_path))
                else:
                    logger.warning(f"Document not found: {source_path}")

            if copied_paths:
                updated_metadata["document_paths"] = copied_paths
                updated_metadata["original_document_paths"] = original_paths
                # For backwards compatibility, set document_path to first document
                updated_metadata["document_path"] = copied_paths[0]
                updated_metadata["original_document_path"] = original_paths[0]

        # Handle single document (document_path) - backwards compatibility
        elif metadata.get("document_path"):
            source_path = Path(metadata["document_path"])
            if source_path.exists():
                # Ensure documents directory exists (use backend for remote compat)
                backend = self._workspace_manager.backend
                backend.mkdir("documents")

                # Check if zip - extract instead of copy
                if source_path.suffix.lower() == ".zip":
                    extracted = self._extract_zip(source_path, "documents", logger)
                    if extracted:
                        updated_metadata["document_paths"] = extracted
                        updated_metadata["original_document_paths"] = [
                            str(source_path)
                        ] * len(extracted)
                        updated_metadata["document_path"] = extracted[0]
                        updated_metadata["original_document_path"] = str(source_path)
                    logger.info(
                        f"Processed zip file: {source_path.name} ({len(extracted)} files extracted)"
                    )
                else:
                    # Regular file - copy via backend to documents/ folder
                    dest_relative = f"documents/{source_path.name}"
                    backend.write_file(dest_relative, source_path.read_bytes())
                    logger.info(f"Copied document to workspace: {dest_relative}")

                    # Update metadata to use workspace-relative path
                    updated_metadata["document_path"] = dest_relative
                    updated_metadata["original_document_path"] = str(source_path)
            else:
                logger.warning(f"Document not found: {source_path}")

        # Write requirement data to workspace if provided (for validator agent)
        if metadata.get("requirement_data"):
            req = metadata["requirement_data"]
            requirement_md = self._format_requirement_as_markdown(req)
            self._workspace_manager.write_file(
                "analysis/requirement_input.md", requirement_md
            )
            logger.info("Wrote requirement to analysis/requirement_input.md")

        # Create todo manager for this workspace
        self._todo_manager = TodoManager(
            workspace=self._workspace_manager,
            min_todos=self.config.phase_settings.min_todos,
            max_todos=self.config.phase_settings.max_todos,
            model_name=self.config.llm.model,
        )

        # Instruction files (bound skills like todo-guide, instruction_files, template-based
        # instructions.md) are deployed in _deploy_instruction_files() after tools are loaded, so that
        # Jinja2 conditionals can reference which tools are actually available.

        logger.debug(f"Workspace created at {self._workspace_manager.path}")

        return updated_metadata

    async def _setup_job_tools(self) -> None:
        """Set up tools for the current job.

        Loads tools based on configuration and injects dependencies
        (workspace manager, todo manager, connections).

        Also creates datasource connections from job metadata (sent by orchestrator)
        and injects them into the ToolContext.
        """
        # Process datasources from job metadata (sent by orchestrator)
        from agent.core.datasource_setup import (
            clone_repository_datasources,
            inject_workspace_facts,
            install_workspace_credentials,
            process_credential_files,
            process_datasources,
        )

        ds_configs = (
            self._job_metadata.get("datasources", []) if self._job_metadata else []
        )
        ws = self._workspace_manager
        install_workspace_credentials(ds_configs, ws)

        # Repository datasources clone onto the workspace backend — never
        # locally in the agent pod (the subprocess git-clone branch was
        # removed; see knowledge-base/knowledge/features/no_workspace_agent_mode.md §9.4).
        repo_datasources = [ds for ds in ds_configs if ds.get("type") == "repository"]
        kb_datasources = [ds for ds in ds_configs if ds.get("type") == "kb"]
        non_repo_datasources = [
            ds for ds in ds_configs if ds.get("type") not in ("repository", "kb")
        ]

        datasources_dict, client_registry, cli_ds_types = process_datasources(
            non_repo_datasources
        )
        # Track connections for cleanup
        self._datasource_connections.update(datasources_dict)
        self._datasource_clients.update(client_registry)

        from agent.tools.registry import register_mcp_tools

        # Discovery must finish before rendering the README.md facts block and loading
        # tools. MCPManager degrades individual server failures internally.
        mcp_manager = datasources_dict.get("mcp")
        if mcp_manager is not None:
            try:
                await mcp_manager.connect_all()
            except Exception as e:
                logger.warning(
                    "Unexpected MCP discovery failure (%s); continuing without MCP",
                    type(e).__name__,
                )
            try:
                register_mcp_tools(mcp_manager)
                mcp_manager.annotate_configs()
            except Exception as e:
                logger.warning(
                    "Could not register MCP tools (%s); continuing without MCP",
                    type(e).__name__,
                )
        else:
            # Loop-mode workers are process-reused; do not retain the prior
            # job's dynamic registry entries.
            register_mcp_tools(None)

        if repo_datasources:
            clone_repository_datasources(repo_datasources, ws)

        # Materialize credential files (kubeconfig, ssh_key, generic_file).
        # Tracked in a manifest so _close_datasource_connections() can undo it.
        try:
            self._datasource_files_manifest = process_credential_files(ds_configs)
        except Exception as e:
            logger.warning("Failed to materialize credential files: %s", e)
            self._datasource_files_manifest = None

        # README.md workspace-facts block (connectors, materials, layout).
        # Regenerated on every init — resume included — so it reflects the
        # current connector set; it replaced the connector index file and the
        # description-bearing job README.
        if ws is not None:
            metadata = self._job_metadata or {}
            context = metadata.get("context")
            project_name = metadata.get("project_name") or (
                context.get("project_name") if isinstance(context, dict) else None
            )
            readme = inject_workspace_facts(
                ds_configs,
                ws,
                project_name=project_name,
                expert=getattr(self.config, "display_name", None),
            )
            if readme:
                self._agent_seed_files["README.md"] = readme

        if cli_ds_types:
            self.config.extra["_cli_datasources"] = cli_ds_types

        # Create tool context with dependencies
        # Merge agent_id and LLM settings into config for tools
        tool_config = {
            **self.config.extra,
            "agent_id": self.config.agent_id,
            "multimodal": self.config.llm.multimodal,  # For vision-aware file reading
            # Lets bulk readers cap a single tool result relative to the main
            # model's window (session_silent_failure_audit.md #5).
            "model_max_context_tokens": self.config.limits.model_max_context_tokens,
            # Per-family page-render DPI (None -> renderer default 150).
            "pdf_render_dpi": getattr(self.config.limits, "pdf_render_dpi", None),
            # The delegate_agent factory reads its settings from this plain
            # dict (enabled = the binding gate, max_concurrent = the per-parent
            # cap). "delegation" is a parsed/known config field, so it is NOT
            # part of config.extra — inject the typed config back in explicitly
            # or the tool sees an empty dict.
            "delegation": asdict(self.config.delegation),
            # Same for the built-in subagents (U1): the roster-wide llm and the
            # resolved roster the subagent runtime reads. Plain dict — no
            # dataclass instances in the tool-visible config.
            "subagents": asdict(self.config.subagents),
            "tags": list(self.config.tags),
        }
        # Build job metadata for delegation tool access
        job_metadata = {
            "job_id": self._current_job_id,
            "project_id": (self._job_metadata or {}).get("project_id"),
            "priority": (self._job_metadata or {}).get("priority", 5),
            "config_name": (self._job_metadata or {}).get(
                "config_name", self.config.agent_id
            ),
            "repo_name": (self._job_metadata or {}).get("repo_name"),
        }

        from agent.services.knowledge.bindings import build_knowledge_bindings
        from shared.runtime_actor import RuntimeActorContext

        raw_project_id = (
            self._job_metadata.get("project_id") if self._job_metadata else None
        )
        native_project_ids = [str(raw_project_id)] if raw_project_id else []
        runtime_actor = RuntimeActorContext.from_payload(
            self._job_metadata.get("runtime_actor") if self._job_metadata else None
        )
        knowledge_bindings = build_knowledge_bindings(
            project_ids=native_project_ids,
            datasources=kb_datasources,
            runtime_actor=runtime_actor,
        )

        context = ToolContext(
            workspace_manager=self._workspace_manager,
            todo_manager=self._todo_manager,
            postgres_db=self.postgres_conn,
            vector_db=getattr(self, "vector_conn", None),
            verify_aux=self._citation_verify_aux,
            verify_citation_prompt=self._citation_verification_prompt,
            datasources=datasources_dict,
            config=tool_config,
            _job_id=self._current_job_id,
            _llm_config=self.config.llm,
            _instruction_files=self.config.instruction_files,
            knowledge_bindings=knowledge_bindings,
            runtime_actor=runtime_actor,
            orchestrator_client=self._orchestrator_client,
            _job_metadata=job_metadata,
        )
        context.project_ids = native_project_ids

        # Progress durability. Phase boundaries used to be the only thing that
        # pushed the workspace to its remote, which made every external view of
        # a running job as stale as the last boundary. Wire the committer here,
        # where the workspace and config are both resolved, so the todo tool and
        # the turn loop share one push clock.
        try:
            from agent.core.progress_commit import ProgressCommitter

            _limits = self.config.limits
            _ws = self._workspace_manager
            context.progress_committer = ProgressCommitter(
                # Resolved per call, not captured: a workspace re-init or tier
                # upgrade swaps the GitManager out from under us mid-job.
                lambda: _ws.git_manager,
                job_id=self._current_job_id or "unknown",
                push_interval=_limits.progress_push_interval_seconds,
                wip_after=_limits.progress_wip_commit_after_seconds,
            )
        except Exception as e:
            logger.warning(f"ProgressCommitter unavailable (non-fatal): {e}")

        self._tool_context = context

        # Initialize ShellManager for persistent terminal sessions. Shells run
        # only on the workspace — there is no local (in-pod) tmux fallback.
        ws_backend = self._workspace_manager.backend
        if getattr(ws_backend, "supports_shell", False):
            try:
                from agent.tools.shell.shell_manager import ShellManager

                shell_config = self.config.extra.get("shell", {})
                # sudo_action comes from config (default "freeze").
                # The orchestrator injects "allow" for VM workspaces
                # (where the sudo gate handles approval via NATS).
                sudo_action = shell_config.get("sudo_action", "freeze")

                shell_manager = ShellManager(
                    job_id=self._current_job_id,
                    max_tabs=shell_config.get("max_tabs", 15),
                    scrollback_limit=shell_config.get("scrollback_limit", 5000),
                    default_timeout=shell_config.get("default_timeout", 120),
                    blocked_commands=shell_config.get("blocked_commands"),
                    sandbox_cwd=str(self._workspace_manager.path)
                    if shell_config.get("sandbox", True)
                    else None,
                    backend=ws_backend,
                    sudo_action=sudo_action,
                    sudo_block_message=shell_config.get("sudo_block_message"),
                )
                context.shell_manager = shell_manager
                self._shell_manager = shell_manager
                logger.info(f"ShellManager initialized for job {self._current_job_id}")
            except Exception as e:
                logger.warning(f"Failed to initialize ShellManager (non-fatal): {e}")
        else:
            logger.info(
                "Workspace backend does not support shell — shell tools "
                "disabled (no local fallback)"
            )

        # Initialize RecallStore for Memory Light (if enabled). These flags let
        # the process_job guard fail-closed (pause for re-dispatch) when a
        # memory-required job loses its embedding-backed stores. See
        # knowledge-history/done/embedding_key_missing_silently_disables_memory_and_kb.md.
        self._memory_degraded = False
        self._kb_degraded = False
        if self.config.memory.enabled:
            try:
                from shared.runtime.services.embedding_service import (
                    get_embedding_service,
                )
                from shared.runtime.services.recall_store import RecallStore
                import uuid as _uuid

                # Resolve project_id for project-scoped memory sharing
                project_id_for_memory = None
                if self.config.memory.project_scoped:
                    raw_pid = (
                        self._job_metadata.get("project_id")
                        if self._job_metadata
                        else None
                    )
                    if raw_pid:
                        project_id_for_memory = _uuid.UUID(str(raw_pid))

                from agent.core.archiver import get_archiver as _get_archiver

                embedding_service = get_embedding_service()
                recall_store = RecallStore(
                    db=self.vector_conn,
                    embedding_service=embedding_service,
                    job_id=_uuid.UUID(self._current_job_id),
                    config=self.config.memory,
                    agent_id=self.config.agent_id,
                    project_id=project_id_for_memory,
                    archiver=_get_archiver(),
                )
                context.recall_store = recall_store
                scope_msg = (
                    f"project {project_id_for_memory}"
                    if project_id_for_memory
                    else f"job {self._current_job_id}"
                )
                logger.info(f"RecallStore initialized (scope: {scope_msg})")
                # Memory extraction is now handled by AuxiliaryLLM in the graph
                # (see extract_and_store_memories in src/services/auxiliary.py)

                # B4 guard: background-probe the endpoint's dimensionality so
                # a misconfigured provider surfaces as one ERROR at init
                # instead of a swallowed WARNING per write.
                asyncio.create_task(embedding_service.verify_dimensions())

            except Exception as e:
                self._memory_degraded = True
                _provider = os.environ.get("EMBEDDING_PROVIDER", "local")
                _model = os.environ.get("EMBEDDING_MODEL", "unknown")
                from agent.core.archiver import audit_unavailable as _audit_unavailable

                _audit_unavailable(
                    job_id=self._current_job_id,
                    agent_type=self.config.agent_id,
                    step_type="memory_unavailable",
                    component="RecallStore",
                    error=e,
                    node_name="setup_job_tools",
                    extra={
                        "embedding_provider": _provider,
                        "embedding_model": _model,
                    },
                )
                logger.warning(
                    f"Failed to initialize RecallStore (non-fatal): {e} "
                    f"[embedding_provider={_provider}, model={_model}]"
                )

        # Initialize the pgvector KnowledgeStore independently from the optional
        # Neo4j Graph tier. Retrieval/read tools only require the store.
        if knowledge_bindings:
            self._setup_job_knowledge(
                context, str(raw_project_id) if raw_project_id else None
            )

        # Load tools from registry, gated by what the workspace backend can
        # actually support (no_workspace_agent_mode.md §3.2/§7): the lite tiers
        # declare supports_shell=False so shell/browser/git are dropped, and
        # none's ScratchBackend (supports_file_tools=False) also drops the file
        # tools — enforcement-by-construction, independent of the config lists.
        from agent.tools.registry import expand_tool_wildcards, filter_tools_by_backend

        tool_names = expand_tool_wildcards(get_all_tool_names(self.config))
        tool_names = filter_tools_by_backend(
            tool_names, self._workspace_manager.backend
        )

        # Expose the in-process upgrade control tool ONLY on a lite (no-shell)
        # backend — the W1 trigger (workspace_tier_upgrade.md §4.3): a lite worker
        # that needs a real environment calls request_workspace_upgrade, which
        # sets a workspace_upgrade_required freeze the agent intercepts and
        # upgrades in place. Mirrors the session path
        # (persistent_session._load_tools_for_backend); it isn't in any config's
        # tool list, so without this a lite job could never request an upgrade.
        # After a virtual→sandbox swap supports_shell=True, so the re-derive on
        # the new backend drops it (nothing left to upgrade to).
        if not getattr(self._workspace_manager.backend, "supports_shell", False):
            if "request_workspace_upgrade" not in tool_names:
                tool_names.append("request_workspace_upgrade")

        try:
            self._tools = load_tools(tool_names, context)
        except ValueError as e:
            # Some tools might not be implemented yet
            logger.warning(f"Tool loading warning: {e}")
            # Load only implemented tools
            implemented_tools = []
            for name in tool_names:
                try:
                    implemented_tools.extend(load_tools([name], context))
                except ValueError:
                    logger.debug(f"Tool not implemented: {name}")

            self._tools = implemented_tools

        # Tool docs are a virtual directory (knowledge-base/knowledge/features/virtual_directories.md):
        # served from the live tool list, never written to the workspace.
        from agent.core.virtual_dirs import ToolsProvider, sweep_legacy_tools_dir

        # CRITICAL: hold the PRE-override tool objects. Further down,
        # `self._tools = apply_description_overrides(self._tools)` rebinds the
        # attribute to copies whose deferred-tool descriptions are short
        # blurbs. A provider reading `self._tools` at call time would render
        # those blurbs into tools/<name>.md and defeat the whole deferred-tool
        # design (short in context, FULL on disk). apply_description_overrides
        # returns copies, so the originals this list holds stay full.
        self._full_description_tools = self._tools
        self._workspace_manager.register_virtual_provider(
            ToolsProvider(lambda: self._full_description_tools)
        )
        if self._workspace_manager.virtual_overlay is not None:
            sweep_legacy_tools_dir(self._workspace_manager.virtual_overlay.inner)

        # contacts/ is virtual and project-scoped (knowledge-history/done/contacts_registry.md).
        # Only registered when the job has a project — without one, `contacts/`
        # is never reserved and the path falls through to the real filesystem.
        # `os` is already imported at module level (line 17) — a local re-import
        # here would shadow it for this whole method and break the earlier
        # os.environ.get() calls above (ruff F823).
        import httpx

        from agent.core.virtual_dirs import ContactsProvider

        orchestrator_url = os.getenv("ORCHESTRATOR_URL", "").rstrip("/")
        job_id = self._current_job_id
        if orchestrator_url and job_id and raw_project_id:

            def _fetch_contacts():
                response = httpx.get(
                    f"{orchestrator_url}/api/contacts/internal/list",
                    params={"job_id": job_id},
                    headers={"X-Internal-Key": os.getenv("MCP_INTERNAL_KEY", "")},
                    timeout=3.0,
                )
                response.raise_for_status()
                return response.json().get("contacts", [])

            self._workspace_manager.register_virtual_provider(
                ContactsProvider(_fetch_contacts)
            )

        loaded_tool_names = [t.name for t in self._tools]

        # Stash the resolved tool list + limits: a subagent child's allowlist is
        # intersected with the parent's loaded names (the runtime ceiling) and
        # its LLM is built with the parent's limits. Read at spawn time — the
        # delegate_agent factory already ran during load_tools() above but the
        # runtime reads these lazily. (context is self._tool_context here.)
        context._resolved_tool_names = loaded_tool_names
        context._limits = self.config.limits

        # Built-in subagents (U3): the parent host + per-job runtime behind
        # delegate_agent. Installed after load_tools so the runtime sees the
        # resolved tool names; the tool reads it lazily at call time.
        self._install_subagent_runtime(context)

        # Capability-scoped bundled skills are resolved only after the final
        # backend gate. In particular, worker jobs must never advertise or
        # materialize present-with-canvas because Canvas is session-only.
        from shared.runtime.core.skill_resolution import scope_skills_for_tools

        skill_catalog = self.config.extra.get(
            "_unscoped_resolved_skills",
            self.config.extra.get("_resolved_skills", {}),
        )
        self.config.extra = {
            **self.config.extra,
            "_unscoped_resolved_skills": skill_catalog,
            "_resolved_skills": scope_skills_for_tools(
                skill_catalog, loaded_tool_names
            ),
        }
        context.config["_resolved_skills"] = self.config.extra["_resolved_skills"]

        # Deploy instruction files with Jinja2 rendering (after tools loaded)
        self._deploy_instruction_files(loaded_tool_names)

        # Apply description overrides for deferred tools
        # Domain tools get short descriptions; agent reads full docs from workspace
        self._tools = apply_description_overrides(self._tools)

        # Apply instruction file enforcement wrappers (before_tool triggers)
        self._tools = apply_instruction_enforcement(self._tools, context)

        # Bind one stable union of the tools to the job's LLM for every phase.
        self._bind_job_tools()

        # Auto-register input documents as CitationEngine sources (background)
        self._doc_registration_task = asyncio.create_task(
            self._register_initial_documents_background(context)
        )

    def _install_subagent_runtime(self, context: ToolContext) -> None:
        """Wire the built-in subagents onto the parent's ToolContext (U3 B.13).

        Always stashes what a child needs from the parent regardless of the
        grant — the auxiliary LLM (children compact with it) and the
        provider-admission fence (a drain ends every child at its next
        provider call without spend). The ``WorkerHost`` + ``SubagentRuntime``
        pair is built only when ``delegation.enabled`` (nothing else binds
        ``delegate_agent``). The runtime's ledger is the DB one
        (``DbSubagentLedger``: a ``threads`` row per child + its transcript,
        WP3) when the context carries both the orchestrator client and the
        agent-side pool, else the null ledger. Lazy import: ``agent.subagents``
        pulls ``persistent_graph``. Non-fatal — the tool builds its own
        runtime on first use if this failed.
        """
        from agent.graph import _is_drain_requested
        from shared.subagent_parent_authority import ParentExecutionAuthority

        context.auxiliary_llm = self._auxiliary_llm
        context.provider_admission = lambda: not _is_drain_requested()
        delegation = (context.config or {}).get("delegation") or {}
        if not isinstance(delegation, dict) or delegation.get("enabled") is not True:
            context.subagent_runtime = None
            context._parent_host = None
            return
        # Capture once, before the ledger is constructed.  In worker mode
        # ``process_job`` has already stored the exact lease on ``self`` but
        # stamps ``context._worker_lease_token`` only after this whole setup
        # method returns; reading the context here would silently downgrade
        # every stateless child to an unfenced writer.
        try:
            worker_lease_token = getattr(self, "_worker_lease_token", None)
            if context._parent_execution_authority is not None:
                # A caller may capture even earlier (for example a future
                # host factory); never replace an already immutable value from
                # mutable client/environment state.
                pass
            elif worker_lease_token is not None:
                context._parent_execution_authority = ParentExecutionAuthority(
                    execution_lane="stateless",
                    parent_job_id=str(self._current_job_id or ""),
                    worker_lease_token=int(worker_lease_token),
                )
            else:
                client = context.orchestrator_client
                context._parent_execution_authority = ParentExecutionAuthority(
                    execution_lane="pinned",
                    parent_job_id=str(self._current_job_id or ""),
                    agent_id=getattr(client, "agent_id", None),
                    pod_uid=os.environ.get("POD_UID"),
                    dispatch_process_generation=getattr(
                        client, "dispatch_process_generation", None
                    ),
                )
        except (TypeError, ValueError) as exc:
            # A local/bare test parent may have no registered process identity.
            # It can still delegate with the in-memory NullLedger, but no child
            # generation or transcript is exposed without exact authority.
            logger.warning(
                "Subagent durable authority unavailable for job %s: %s",
                self._current_job_id,
                exc,
            )
            context._parent_execution_authority = None
        try:
            from agent.subagents.host import WorkerHost
            from agent.subagents.ledger import NullLedger
            from agent.subagents.persistence import DbSubagentLedger
            from agent.subagents.runtime import SubagentRuntime

            host = WorkerHost(
                job_id=str(self._current_job_id or ""),
                agent_type=self.config.agent_id,
                tool_context=context,
                thread_id=None,
                user_id=getattr(context, "user_id", None),
                auxiliary_llm=self._auxiliary_llm,
                live_llm_config=self.config.llm,
                postgres=self.postgres_conn,
            )
            context._parent_host = host
            ledger = DbSubagentLedger.from_context(context)
            context.subagent_runtime = SubagentRuntime.from_context(
                context, host, ledger=ledger if ledger is not None else NullLedger()
            )
            logger.info(
                "Subagent runtime installed for job %s: roster=%s "
                "max_concurrent=%s ledger=%s",
                self._current_job_id,
                context.subagent_runtime.roster_names,
                context.subagent_runtime.max_concurrent,
                "db" if ledger is not None else "null",
            )
        except Exception as e:
            logger.warning(f"Subagent runtime unavailable (non-fatal): {e}")
            context.subagent_runtime = None

    def _bind_job_tools(self) -> None:
        """Bind the loaded tools to the job's LLM.

        ONE binding carries the union of both phases' tools —
        ``_llm_with_tools``. The phase is enforced per call by the runtime
        gate in the graph, and single-phase tools state their phase in the
        bound description (``apply_phase_description_prefixes``, applied to
        the bound copies like the guardrail Examples — the ToolNode's tool
        objects and full-description originals stay untouched). The two old
        phase attributes remain aliases for one release.
        """
        # parallel_tool_calls is an OpenAI Chat Completions param — suppressed
        # for providers/models that reject it (Google GenAI's
        # GenerateContentConfig, OpenAI o-series). Defaults to False so a
        # weak model cannot flood the loop with 20+ simultaneous calls. One
        # model runs every phase (U1), so the gate is applied once.
        llm_cfg = self.config.llm
        bind_kwargs: Dict[str, Any] = {}
        if supports_parallel_tool_calls(llm_cfg.provider, llm_cfg.model):
            bind_kwargs["parallel_tool_calls"] = llm_cfg.parallel_tool_calls

        # Family-specific Examples blocks go into the descriptions before
        # binding: this is where the model first sees the tool catalog and
        # decides on a wire format — knowledge-base/knowledge/design/guardrails_matrix.md.
        from shared.runtime.services.guardrails import apply_guardrails_to_tools
        from agent.tools.registry import filter_tools_by_phase

        all_names = [t.name for t in self._tools]
        strategic_names = set(filter_tools_by_phase(all_names, "strategic"))
        tactical_names = set(filter_tools_by_phase(all_names, "tactical"))

        union = strategic_names | tactical_names
        dropped = [n for n in all_names if n not in union]
        if dropped:
            # Same as before: a tool without registry phases was never bound.
            logger.debug(f"Tools without a registry phase are not bound: {dropped}")
        bound_tools = apply_phase_description_prefixes(
            [t for t in self._tools if t.name in union]
        )
        bound_tools = apply_guardrails_to_tools(bound_tools, model=llm_cfg.model)
        self._llm_with_tools = self._llm.bind_tools(bound_tools, **bind_kwargs)
        self._strategic_llm_with_tools = self._llm_with_tools
        self._tactical_llm_with_tools = self._llm_with_tools
        logger.info(
            f"Loaded {len(self._tools)} tools, bound once for every phase "
            f"({len(bound_tools)} in the schema: "
            f"{len(strategic_names - tactical_names)} strategic-only, "
            f"{len(tactical_names - strategic_names)} tactical-only, "
            f"{len(strategic_names & tactical_names)} both)"
        )

    def _setup_job_knowledge(
        self, context: ToolContext, project_id: Optional[str]
    ) -> None:
        """Attach the required vector store and optional Graph tier to a job."""
        if project_id:
            context.project_id = project_id

        try:
            if self.vector_conn is None:
                raise RuntimeError("Vector database connection is unavailable")

            from shared.runtime.services.embedding_service import (
                get_kb_embedding_service,
            )
            from shared.runtime.services.knowledge_store import KnowledgeStore

            embedding_service = get_kb_embedding_service()
            context.knowledge_store = KnowledgeStore(
                db=self.vector_conn,
                embedding_service=embedding_service,
            )
            logger.info(
                "Knowledge store initialized for %d KB binding(s)",
                len(getattr(context, "knowledge_bindings", []) or [])
                or (1 if project_id else 0),
            )
        except Exception as e:
            self._kb_degraded = True
            from agent.core.archiver import audit_unavailable as _audit_unavailable

            _audit_unavailable(
                job_id=self._current_job_id,
                agent_type=self.config.agent_id,
                step_type="kb_unavailable",
                component="KnowledgeStore",
                error=e,
                node_name="setup_job_tools",
                extra={
                    "project_id": project_id,
                    "kb_ids": getattr(
                        context, "kb_ids", [project_id] if project_id else []
                    ),
                    "embedding_provider": os.environ.get(
                        "KB_EMBEDDING_PROVIDER",
                        os.environ.get("EMBEDDING_PROVIDER", "local"),
                    ),
                },
            )
            logger.warning(
                f"Failed to initialize knowledge store (non-fatal): {e} "
                f"[embedding_provider="
                f"{os.environ.get('KB_EMBEDDING_PROVIDER', os.environ.get('EMBEDDING_PROVIDER', 'local'))}]"
            )

        if not project_id:
            return

        try:
            from shared.runtime.services.knowledge_graph import KnowledgeGraphDB

            kg = KnowledgeGraphDB()
            if kg.connect():
                context.knowledge_graph = kg
                self._knowledge_graph = kg  # Track for cleanup
                logger.info(
                    f"Knowledge Graph tier initialized for project {project_id}"
                )
            else:
                logger.warning("Failed to connect to Neo4j — Graph tier disabled")
        except Exception as e:
            # Neo4j is optional: do not mark vector search/read as degraded.
            logger.warning(f"Failed to initialize Neo4j Graph tier (non-fatal): {e}")

    async def _hydrate_job_brief(self, job_id: str) -> None:
        """Backfill description/required_deliverables/kickoff_message.

        ``JobResumeRequest`` carries none of them, so a resumed job would
        serve an empty virtual ``task_brief.md`` for the rest of its life.
        Sources: orchestrator internal ``/brief`` endpoint first, the agent's
        own DB handle second; both non-fatal. Never overwrites fields the
        dispatch already provided.
        knowledge-base/knowledge/issues/fresh_job_dispatched_as_resume_skips_seeding.md
        """
        row = None
        if self._orchestrator_client:
            try:
                row = await self._orchestrator_client.get_job_brief(job_id)
            except Exception as e:
                logger.warning(
                    f"[{job_id}] brief hydration via orchestrator failed: {e}"
                )
        if not row and self.postgres_conn:
            try:
                import json as _json
                import uuid as _uuid

                job = await self.postgres_conn.jobs.get(_uuid.UUID(job_id))
                ctx = (job or {}).get("context") or {}
                if isinstance(ctx, str):
                    ctx = _json.loads(ctx)
                row = {
                    "description": (job or {}).get("description"),
                    "required_deliverables": ctx.get("required_deliverables"),
                    "kickoff_message": ctx.get("kickoff_message"),
                }
            except Exception as e:
                logger.warning(f"[{job_id}] brief hydration via DB failed: {e}")
        keys = ("description", "required_deliverables", "kickoff_message")
        if not row or not any(row.get(k) for k in keys):
            logger.error(
                f"[{job_id}] resume: task brief could not be hydrated — the "
                f"virtual task_brief.md will serve empty"
            )
            return
        if self._job_metadata is None:
            self._job_metadata = {}
        for key in keys:
            if row.get(key) and not self._job_metadata.get(key):
                self._job_metadata[key] = row[key]
        logger.info(
            f"[{job_id}] hydrated task brief on resume "
            f"(description={len(row.get('description') or '')} chars)"
        )

    async def _note_resume_without_checkpoint(
        self, job_id: str, previous_status: Optional[str]
    ) -> None:
        """Tripwire: ``resume=True`` but no checkpoint or snapshot was found.

        The orchestrator routed a job with nothing to resume down the
        ``/job/resume`` lane — almost always a never-started job (that lane
        ships no brief fields). Fall toward fresh seeding: backfill the brief
        and commit the Phase-0 seed the fresh path would have committed.
        knowledge-base/knowledge/issues/fresh_job_dispatched_as_resume_skips_seeding.md
        """
        logger.error(
            f"[{job_id}] resume=True but no checkpoint or snapshot was found "
            f"(previous_status={previous_status!r}) — treating as a fresh "
            f"start; the job was probably dispatched down the wrong lane"
        )
        await self._hydrate_job_brief(job_id)
        self._commit_workspace_seed(job_id)

    def _commit_workspace_seed(self, job_id: str) -> None:
        """Commit and push the seeded workspace as the Phase 0 baseline.

        Runs after all seeding (instructions, task brief, documents, README,
        bound skills) so the job's inputs are visible in the repo immediately,
        instead of first appearing in the phase 1 archive commit.
        """
        git_mgr = (
            self._workspace_manager.git_manager if self._workspace_manager else None
        )
        if not git_mgr or not git_mgr.is_active:
            return
        try:
            committed = git_mgr.commit(
                "[Phase 0 Seed] Workspace seeded: instructions and input files",
                allow_empty=False,
            )
            if committed:
                git_mgr.push()
        except Exception as e:
            logger.warning(f"[{job_id}] Phase 0 seed commit failed (non-fatal): {e}")

    def _remove_legacy_manifest_status(self, job_id: str) -> None:
        """Remove the retired agent-visible phase-boundary status file.

        Existing repositories and resumed workspaces may contain a tracked copy
        created by older workers. Cleanup is best-effort: inability to delete
        obsolete observability data must not prevent the job from starting.
        """
        path = "output/manifest_status.json"
        try:
            if self._workspace_manager and self._workspace_manager.exists(path):
                self._workspace_manager.delete_file(path)
                logger.info(f"[{job_id}] Removed retired workspace file {path}")
        except Exception as e:
            logger.warning(f"[{job_id}] Could not remove retired file {path}: {e}")

    def _deploy_instruction_files(self, loaded_tool_names: List[str]) -> None:
        """Deploy instruction files to workspace with Jinja2 rendering.

        Called after tools are loaded so that template conditionals like
        ``{% if has_tool("kb_write") %}`` resolve correctly.

        Deploys:
        - instructions.md / task_brief.md as virtual providers (served live,
          never written — see knowledge-base/knowledge/features/virtual_directories.md)
        - Additional instruction_files from config (literal files + bound skills)
        - In-scope skill directories (Slice 2)
        """
        from shared.runtime.core.loader import (
            FileResolver,
            render_instruction_content,
            load_instructions,
        )

        # instructions.md / task_brief.md are virtual
        # (knowledge-base/knowledge/features/virtual_directories.md): served from the job record or
        # the rendered template, never written to the workspace. This deletes
        # the exists()-probe precedence dance and the "rewrite if it vanished"
        # repair path — a virtual file cannot go missing.
        from agent.core.virtual_dirs import build_instruction_providers
        from agent.core.deliverables import format_deliverable_contract_block

        # Providers read self._job_metadata LIVE (not a bound alias): resume
        # hydration may replace or backfill it after these closures are
        # registered, and a stale alias would serve an empty brief forever
        # (knowledge-base/knowledge/issues/fresh_job_dispatched_as_resume_skips_seeding.md).
        def _uploaded_instructions():
            # Priority inline > upload (the template is the caller's fallback).
            # Inline is read live so it survives the resume path; upload content
            # was resolved eagerly at boot because its I/O is async.
            inline = (self._job_metadata or {}).get("instructions")
            if inline and inline.strip():
                return inline
            return self._resolved_instructions_md

        def _rendered_template():
            content = load_instructions(self.config, model=self.config.llm.model)
            return render_instruction_content(
                content,
                loaded_tool_names,
                origin=f"expert {getattr(self.config, 'agent_id', '?')!r} instructions",
            )

        def _task_brief():
            meta = self._job_metadata or {}
            description = meta.get("description", "")
            kickoff_message = meta.get("kickoff_message", "")
            parts = [f"# Task Brief\n\n## Description\n\n{description}"]
            if kickoff_message:
                parts.append(f"\n\n## Kickoff Message\n\n{kickoff_message}")
            # Deliverable contract (P1-C): render the job's required_deliverables
            # manifest as an explicit block — workers can't be held to a floor
            # they were never shown. Empty string when the job has no manifest.
            contract_block = format_deliverable_contract_block(
                meta.get("required_deliverables")
            )
            if contract_block:
                parts.append(contract_block)
            return "".join(parts)

        instruction_providers = build_instruction_providers(
            uploaded=_uploaded_instructions,
            template=_rendered_template,
            brief=_task_brief,
        )
        for provider in instruction_providers:
            self._workspace_manager.register_virtual_provider(provider)

        # There is deliberately no materialize-to-disk fallback here. Writing
        # these two into the workspace root is what dropped a critic's brief
        # into the root its TARGET reads from, on every subjob that inherits
        # its parent's workspace
        # (knowledge-history/done/critic_brief_lands_in_shared_workspace_and_misleads_target.md).
        # The fallback existed to stop an agent booting with no task; graph.py
        # now refuses to start when both briefs resolve empty, which covers
        # every cause rather than the single one this branch handled.

        # todo_guide is now the bundled "todo-guide" skill, bound via instruction_files
        # (before_tool:next_phase_todos) and materialized in the loop below — no matrix
        # special-case. See knowledge-base/knowledge/features/agent_skills.md (Slice 3).

        # Additional instruction files (config-driven)
        if self.config.instruction_files:
            templates_dir = get_project_root() / "config" / "templates"
            file_resolver = FileResolver(
                deployment_dir=self.config._deployment_dir,
                framework_dir=templates_dir,
            )
            resolved_instructions = self.config.extra.get("_resolved_instructions", {})
            deployed_paths: set[str] = set()
            for entry in self.config.instruction_files:
                if entry.path in deployed_paths:
                    continue
                try:
                    if entry.skill:
                        # Bound skill: content from the (flag-independent) instructions
                        # channel, written to skills/<skill>/SKILL.md. The catalog
                        # materialization path (Slice 2) is filtered out for bound
                        # skills, so this is the single delivery path.
                        content = resolved_instructions.get(entry.skill)
                        if not content:
                            logger.warning(
                                f"Bound skill content missing from blob: {entry.skill}"
                            )
                            continue
                        content = render_instruction_content(
                            content,
                            loaded_tool_names,
                            origin=f"bound skill {entry.skill!r}",
                        )
                        parent_dir = str(Path(entry.path).parent)
                        if parent_dir and parent_dir != ".":
                            self._workspace_manager.backend.mkdir(parent_dir)
                        self._workspace_manager.write_file(entry.path, content)
                        self._agent_seed_files[entry.path] = content
                        deployed_paths.add(entry.path)
                        logger.debug(f"Deployed bound skill to workspace: {entry.path}")
                        continue
                    # Check resolved config first (resumed jobs)
                    basename = Path(entry.file).stem
                    content = resolved_instructions.get(basename)
                    if not content:
                        content = file_resolver.load(Path(entry.file).name)
                    content = render_instruction_content(
                        content,
                        loaded_tool_names,
                        origin=f"instruction file {entry.file!r}",
                    )
                    # Ensure parent directory exists (use backend, not local mkdir)
                    parent_dir = str(Path(entry.file).parent)
                    if parent_dir and parent_dir != ".":
                        self._workspace_manager.backend.mkdir(parent_dir)
                    self._workspace_manager.write_file(entry.file, content)
                    deployed_paths.add(entry.path)
                    logger.debug(
                        f"Deployed instruction file to workspace: {entry.file}"
                    )
                except FileNotFoundError:
                    logger.warning(f"Instruction file not found: {entry.file}")

        # Skill directories (Slice 2): materialize in-scope skills into
        # skills/<name>/<path> so use_skill (L2) and read_file/run_command (L3)
        # can reach them. Same write_file/mkdir path as instruction files.
        from shared.runtime.core.skill_resolution import skill_files_to_workspace

        skills_files = self.config.extra.get("_resolved_skills", {}).get("files", {})
        for ws_path, content in skill_files_to_workspace(skills_files).items():
            parent_dir = str(Path(ws_path).parent)
            if parent_dir and parent_dir != ".":
                self._workspace_manager.backend.mkdir(parent_dir)
            self._workspace_manager.write_file(ws_path, content)
            logger.debug(f"Deployed skill file to workspace: {ws_path}")

    async def _register_initial_documents_background(
        self, context: "ToolContext"
    ) -> None:
        """Register input documents in documents/ as CitationEngine sources.

        Scans the documents/ directory for supported file types and registers
        each as a source concurrently on the agent's shared async vector pool,
        enabling hybrid vector search via search_library. Skips
        documents/external/ (web content is registered separately by the
        research tools).

        Runs as a background task so the agent's ReAct loop starts immediately.
        Concurrency is bounded by a semaphore (the pool + embedding endpoint do
        the rest). Non-fatal: failures are logged but never block the job.

        Args:
            context: ToolContext with the workspace + vector pool.
        """
        if not context.has_workspace():
            return
        if context.vector_db is None:
            logger.debug("No vector pool attached; skipping document auto-registration")
            return

        SUPPORTED_EXTENSIONS = {
            ".pdf",
            ".txt",
            ".md",
            ".docx",
            ".doc",
            ".pptx",
            ".html",
            ".htm",
            ".csv",
            ".json",
            ".xml",
            ".rtf",
        }

        try:
            ws = context.workspace_manager
            # Walk via the backend: a local Path.exists()/rglob() never sees a
            # remote workspace, which silently disabled auto-registration on
            # every remote-backend job. Off the event loop — SFTP descent isn't
            # the "quick local walk" this used to be.
            if not await asyncio.to_thread(ws.exists, "documents"):
                return

            files: List[Tuple[str, str]] = []
            for rel_path in await asyncio.to_thread(ws.backend.walk, "documents"):
                # Skip documents/external/ (web content, registered by research tools)
                if rel_path.startswith("documents/external/"):
                    continue

                if Path(rel_path).suffix.lower() not in SUPPORTED_EXTENSIONS:
                    continue

                # Workspace-relative path: matches the registry key the citation
                # tools use, and get_or_register_doc_source materializes a local
                # copy for the engine when the backend is remote.
                files.append((rel_path, Path(rel_path).name))

            if not files:
                return

            start_time = time.monotonic()
            logger.info(
                f"Starting background registration of {len(files)} document(s)..."
            )

            # Register concurrently on the shared vector pool. The engine borrows
            # the pool (no per-instance connection), so a single context-owned
            # engine is reused across all files; get_or_register_doc_source
            # populates context._source_registry as it goes.
            sem = asyncio.Semaphore(4)

            async def _register(rel_path: str, name: str) -> bool:
                async with sem:
                    try:
                        await context.get_or_register_doc_source(rel_path, name=name)
                        return True
                    except Exception as e:
                        logger.debug(f"Could not register document {name}: {e}")
                        return False

            results = await asyncio.gather(*(_register(fp, name) for fp, name in files))
            registered_count = sum(1 for ok in results if ok)

            elapsed = time.monotonic() - start_time
            if registered_count > 0:
                logger.info(
                    f"Registered {registered_count} document(s) in {elapsed:.1f}s (async pool)"
                )

        except Exception as e:
            logger.warning(
                f"Auto-registration of input documents failed (non-fatal): {e}"
            )

    def _inject_typed_env_vars(self, ds_type: str, ds: Dict[str, Any]) -> None:
        """Inject well-known environment variables for managed connector CLI access."""
        url = ds.get("connection_url", "")
        creds = ds.get("credentials") or {}

        if ds_type == "postgresql":
            # Parse connection URL into PG* env vars
            from urllib.parse import urlparse

            parsed = urlparse(url)
            if parsed.hostname:
                os.environ["PGHOST"] = parsed.hostname
            if parsed.port:
                os.environ["PGPORT"] = str(parsed.port)
            if parsed.username:
                os.environ["PGUSER"] = parsed.username
            password = parsed.password or creds.get("password", "")
            if password:
                os.environ["PGPASSWORD"] = password
            db_name = parsed.path.lstrip("/").split("?")[0]
            if db_name:
                os.environ["PGDATABASE"] = db_name

        elif ds_type == "neo4j":
            os.environ["NEO4J_URI"] = url
            os.environ["NEO4J_USERNAME"] = creds.get("username", "neo4j")
            os.environ["NEO4J_PASSWORD"] = creds.get("password", "")

        elif ds_type == "mongodb":
            os.environ["MONGOSH_URI"] = url

    def _close_datasource_connections(self) -> None:
        """Close all datasource connections opened for the current job."""
        # Close knowledge graph connection (inline curation)
        if self._knowledge_graph:
            try:
                self._knowledge_graph.close()
                logger.debug("Closed knowledge graph connection")
            except Exception as e:
                logger.warning(f"Error closing knowledge graph: {e}")
            self._knowledge_graph = None

        for ds_type, conn in self._datasource_connections.items():
            try:
                if hasattr(conn, "close"):
                    conn.close()
                    logger.debug(f"Closed {ds_type} datasource connection")
            except Exception as e:
                logger.warning(f"Error closing {ds_type} datasource: {e}")
        self._datasource_connections = {}
        # Close parent clients (e.g. MongoClient) that aren't in _datasource_connections
        for ds_type, client in self._datasource_clients.items():
            try:
                if hasattr(client, "close"):
                    client.close()
                    logger.debug(f"Closed {ds_type} datasource client")
            except Exception as e:
                logger.warning(f"Error closing {ds_type} datasource client: {e}")
        self._datasource_clients = {}

        # Remove credential files materialized for this job (best-effort).
        if self._datasource_files_manifest:
            try:
                from agent.core.datasource_setup import cleanup_credential_files

                cleanup_credential_files(self._datasource_files_manifest)
            except Exception as e:
                logger.warning(f"Error cleaning up credential files: {e}")
            self._datasource_files_manifest = None

    async def _resume_from_checkpoint(
        self,
        job_id: str,
        thread_id: str,
        thread_config: Dict[str, Any],
        original_config_name: Optional[str],
        updated_metadata: Dict[str, Any],
    ) -> tuple:
        """Resume using existing checkpoint.db directly (no snapshot recovery).

        Used for graceful stops where the checkpoint has valid in-progress state.

        Returns:
            (graph_input, thread_id, thread_config) — graph_input is None on success
            (meaning resume from checkpoint), or an initial state dict if no checkpoint found.
        """
        logger = logging.getLogger(__name__)
        from agent.core.phase_snapshot import discover_thread_id_from_checkpoint

        # Try to discover the correct thread_id from checkpoint DB
        checkpoint_path = self._get_checkpoint_path(job_id)
        discovered_thread_id = discover_thread_id_from_checkpoint(
            checkpoint_path, job_id
        )
        if discovered_thread_id:
            logger.info(f"Discovered thread_id from checkpoint: {discovered_thread_id}")
            thread_id = discovered_thread_id
            thread_config = {
                "configurable": {"thread_id": thread_id},
                "recursion_limit": 1000000,
            }
            checkpoint_state = await self._graph.aget_state(thread_config)
            if checkpoint_state and checkpoint_state.values:
                logger.info(f"Found checkpoint with thread_id: {thread_id}")
                return None, thread_id, thread_config

        # Fallback: try job_id as thread_id (new format)
        checkpoint_state = await self._graph.aget_state(thread_config)
        if checkpoint_state and checkpoint_state.values:
            logger.debug(f"Found checkpoint with thread_id: {thread_id}")
            return None, thread_id, thread_config

        # Fallback: try legacy format
        legacy_config_name = original_config_name or self.config.agent_id
        legacy_thread_id = f"{legacy_config_name}_{job_id}"
        legacy_config = {
            "configurable": {"thread_id": legacy_thread_id},
            "recursion_limit": 1000000,
        }
        legacy_state = await self._graph.aget_state(legacy_config)
        if legacy_state and legacy_state.values:
            logger.info(f"Using legacy thread_id format: {legacy_thread_id}")
            return None, legacy_thread_id, legacy_config

        # No checkpoint found — return initial state so caller can fall back
        logger.warning("No checkpoint found with any thread_id format")
        graph_input = create_initial_state(
            job_id=job_id,
            workspace_path=str(self._workspace_manager.path),
            metadata=updated_metadata,
        )
        return graph_input, thread_id, thread_config

    async def _resume_from_snapshot(
        self,
        job_id: str,
        snapshot_manager,
        thread_id: str,
        thread_config: Dict[str, Any],
        original_config_name: Optional[str],
        updated_metadata: Dict[str, Any],
    ) -> tuple:
        """Resume using phase snapshot recovery (overwrites checkpoint.db).

        Used for crash/failure recovery where the checkpoint may be corrupted.

        Returns:
            (graph_input, thread_id, thread_config) — graph_input is None on success
            (meaning resume from checkpoint), or an initial state dict if recovery fails.
        """
        logger = logging.getLogger(__name__)

        latest_snapshot = snapshot_manager.get_latest_snapshot()
        if not latest_snapshot:
            logger.warning(f"No phase snapshots found for job {job_id}, starting fresh")
            graph_input = create_initial_state(
                job_id=job_id,
                workspace_path=str(self._workspace_manager.path),
                metadata=updated_metadata,
            )
            return graph_input, thread_id, thread_config

        logger.info(
            f"Resuming from phase {latest_snapshot.phase_number} snapshot "
            f"(iteration={latest_snapshot.iteration})"
        )

        if not snapshot_manager.recover_to_phase(latest_snapshot.phase_number):
            logger.warning(
                f"Failed to recover from phase {latest_snapshot.phase_number} snapshot, starting fresh"
            )
            graph_input = create_initial_state(
                job_id=job_id,
                workspace_path=str(self._workspace_manager.path),
                metadata=updated_metadata,
            )
            return graph_input, thread_id, thread_config

        # Delete any stale snapshots from failed runs after this phase
        deleted = snapshot_manager.delete_snapshots_after(latest_snapshot.phase_number)
        if deleted:
            logger.info(
                f"Deleted {deleted} stale snapshot(s) after phase {latest_snapshot.phase_number}"
            )

        # Determine the correct thread_id for checkpoint lookup
        # Priority: 1) snapshot.thread_id, 2) discover from checkpoint DB, 3) try known formats
        discovered_thread_id = None

        if latest_snapshot.thread_id:
            discovered_thread_id = latest_snapshot.thread_id
            logger.info(f"Using thread_id from snapshot: {discovered_thread_id}")
        else:
            from agent.core.phase_snapshot import discover_thread_id_from_checkpoint

            checkpoint_path = self._get_checkpoint_path(job_id)
            discovered_thread_id = discover_thread_id_from_checkpoint(
                checkpoint_path, job_id
            )
            if discovered_thread_id:
                logger.info(
                    f"Discovered thread_id from checkpoint: {discovered_thread_id}"
                )

        if discovered_thread_id:
            thread_id = discovered_thread_id
            thread_config = {
                "configurable": {"thread_id": thread_id},
                "recursion_limit": 1000000,
            }
            checkpoint_state = await self._graph.aget_state(thread_config)
            if checkpoint_state and checkpoint_state.values:
                logger.info(f"Found checkpoint with thread_id: {thread_id}")
                return None, thread_id, thread_config
            else:
                logger.warning(
                    f"Discovered thread_id {thread_id} has no checkpoint data, starting fresh"
                )
                graph_input = create_initial_state(
                    job_id=job_id,
                    workspace_path=str(self._workspace_manager.path),
                    metadata=updated_metadata,
                )
                return graph_input, thread_id, thread_config
        else:
            # Fallback: try new format then legacy format
            checkpoint_state = await self._graph.aget_state(thread_config)
            if checkpoint_state and checkpoint_state.values:
                logger.debug(f"Found checkpoint with new thread_id format: {job_id}")
                return None, thread_id, thread_config
            else:
                legacy_config_name = original_config_name or self.config.agent_id
                legacy_thread_id = f"{legacy_config_name}_{job_id}"
                legacy_config = {
                    "configurable": {"thread_id": legacy_thread_id},
                    "recursion_limit": 1000000,
                }
                legacy_state = await self._graph.aget_state(legacy_config)
                if legacy_state and legacy_state.values:
                    logger.info(f"Using legacy thread_id format: {legacy_thread_id}")
                    return None, legacy_thread_id, legacy_config
                else:
                    logger.warning(
                        "No checkpoint found with any thread_id format, starting fresh"
                    )
                    graph_input = create_initial_state(
                        job_id=job_id,
                        workspace_path=str(self._workspace_manager.path),
                        metadata=updated_metadata,
                    )
                    return graph_input, thread_id, thread_config

    def _get_checkpoint_path(self, job_id: str) -> Path:
        """Get SQLite checkpoint file path for a job.

        Args:
            job_id: Unique job identifier

        Returns:
            Path to SQLite checkpoint file (e.g., workspace/checkpoints/job_<id>.db)
        """
        return get_checkpoints_path() / f"job_{job_id}.db"

    def _extract_job_metadata(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """Extract metadata from a job record for processing.

        Handles both:
        - Jobs table rows (for Creator): extracts document_path, prompt, etc.
        - Requirements table rows (for Validator): wraps as requirement_data
        """
        metadata = {}

        # Check if this is a requirements table row (has 'text' field but no 'prompt')
        # Requirements have: id, text, name, type, priority, gobd_relevant, etc.
        if "text" in job and "prompt" not in job:
            # This is a requirement row from polling - wrap it as requirement_data
            metadata["requirement_data"] = job
            logger.debug(
                f"Extracted requirement data: {job.get('name', job.get('id', 'unknown'))}"
            )
            return metadata

        # Otherwise, handle as jobs table row
        metadata_fields = [
            "document_path",
            "prompt",
            "requirement_id",
            "requirement_data",
            "source_document",
            "config",
            "options",
        ]

        for field in metadata_fields:
            if field in job:
                metadata[field] = job[field]

        # Include job-specific data if present
        if "data" in job and isinstance(job["data"], dict):
            metadata.update(job["data"])

        return metadata

    def _format_requirement_as_markdown(self, req: Dict[str, Any]) -> str:
        """Format requirement data as markdown for the workspace.

        Creates a structured markdown document from requirement data
        for the validator agent to read from analysis/requirement_input.md.

        Args:
            req: Requirement dictionary from PostgreSQL

        Returns:
            Formatted markdown string
        """
        import json

        lines = [
            "# Requirement Input",
            "",
            f"**ID:** `{req.get('id', 'N/A')}`",
            f"**Name:** {req.get('name', 'Unnamed')}",
            "",
            "## Text",
            "",
            req.get("text", "(No text provided)"),
            "",
            "## Metadata",
            "",
            f"- **Type:** {req.get('type', 'N/A')}",
            f"- **Priority:** {req.get('priority', 'N/A')}",
            f"- **GoBD Relevant:** {req.get('gobd_relevant', False)}",
            f"- **GDPR Relevant:** {req.get('gdpr_relevant', False)}",
            f"- **Confidence:** {req.get('confidence', 'N/A')}",
            "",
            "## Source",
            "",
            f"- **Document:** {req.get('source_document', 'N/A')}",
        ]

        # Handle source_location which may be JSON string or dict
        source_location = req.get("source_location")
        if source_location:
            if isinstance(source_location, str):
                try:
                    source_location = json.loads(source_location)
                except (json.JSONDecodeError, TypeError):
                    pass
            lines.append(f"- **Location:** {source_location}")
        else:
            lines.append("- **Location:** N/A")

        lines.append("")

        if req.get("reasoning"):
            lines.extend(
                [
                    "## Extraction Reasoning",
                    "",
                    req["reasoning"],
                    "",
                ]
            )

        if req.get("research_notes"):
            lines.extend(
                [
                    "## Research Notes",
                    "",
                    req["research_notes"],
                    "",
                ]
            )

        return "\n".join(lines)

    def _extract_zip(
        self,
        zip_path: Path,
        dest_dir_relative: str,
        job_logger: logging.Logger,
    ) -> List[str]:
        """Extract zip file contents preserving directory structure.

        Uses the workspace backend for writes so this works with both
        local and remote (SSH/SFTP) backends.  Skips hidden files and
        macOS __MACOSX folders.

        Args:
            zip_path: Path to the zip file (local to the agent container)
            dest_dir_relative: Destination directory relative to workspace
                root (e.g. "documents")
            job_logger: Logger instance

        Returns:
            List of relative paths to extracted files (e.g., ["documents/subdir/file.pdf"])
        """
        extracted_paths = []
        backend = self._workspace_manager.backend

        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                for zip_info in zf.infolist():
                    # Skip directories (created implicitly)
                    if zip_info.is_dir():
                        continue

                    # Get relative path within zip
                    relative_path = Path(zip_info.filename)

                    # Skip hidden files and macOS metadata
                    if any(part.startswith(".") for part in relative_path.parts):
                        continue
                    if "__MACOSX" in zip_info.filename:
                        continue

                    # Skip empty filenames
                    if not relative_path.name:
                        continue

                    # Build workspace-relative path (e.g. "documents/subdir/file.pdf")
                    ws_relative = f"{dest_dir_relative}/{relative_path}"

                    # Extract file content and write via backend
                    with zf.open(zip_info) as source:
                        backend.write_file(ws_relative, source.read())

                    extracted_paths.append(ws_relative)
                    job_logger.debug(f"Extracted: {zip_info.filename} -> {ws_relative}")

            job_logger.info(
                f"Extracted {len(extracted_paths)} files from {zip_path.name}"
            )

        except zipfile.BadZipFile as e:
            job_logger.error(f"Invalid zip file {zip_path.name}: {e}")
        except Exception as e:
            job_logger.error(f"Failed to extract zip {zip_path.name}: {e}")

        return extracted_paths

    async def _download_upload_files(
        self,
        upload_id: str,
        dest_dir: Path,
        job_logger: logging.Logger,
    ) -> Optional[List[str]]:
        """Download files from orchestrator upload via HTTP.

        Attempts to download files from the orchestrator API. If the orchestrator
        is not configured or the download fails, returns None to signal that the
        caller should fall back to local filesystem access.

        Args:
            upload_id: Upload identifier
            dest_dir: Destination directory for downloaded files
            job_logger: Logger instance

        Returns:
            List of downloaded filenames, or None if HTTP download failed/unavailable
        """
        # Use same default as orchestrator_client.py
        orchestrator_url = os.getenv("ORCHESTRATOR_URL", "http://localhost:8085")

        # Import here to avoid circular imports
        from agent.api.orchestrator_client import OrchestratorClient

        # Create a temporary client for downloads (no registration needed)
        client = OrchestratorClient(
            orchestrator_url=orchestrator_url,
            pod_ip="",  # Not needed for downloads
            pod_port=0,
            hostname="",
            config_name="",
        )

        try:
            await client.connect()

            # Get upload info
            upload_info = await client.get_upload_info(upload_id)
            if not upload_info:
                job_logger.info(
                    f"Upload {upload_id} not found on orchestrator, will try local"
                )
                return None

            # Ensure destination directory exists
            dest_dir.mkdir(parents=True, exist_ok=True)

            downloaded_files = []
            for file_info in upload_info.files:
                # Skip metadata.json
                if file_info.name == "metadata.json":
                    continue

                content = await client.download_file(upload_id, file_info.name)
                if content is None:
                    job_logger.warning(
                        f"Failed to download {file_info.name} from {upload_id}, will try local"
                    )
                    return None

                # Save file
                dest_path = dest_dir / file_info.name
                dest_path.write_bytes(content)
                downloaded_files.append(file_info.name)
                job_logger.debug(
                    f"Downloaded via HTTP: {upload_id}/{file_info.name} ({len(content)} bytes)"
                )

            job_logger.info(
                f"Downloaded {len(downloaded_files)} files from orchestrator for upload {upload_id}"
            )
            return downloaded_files

        except Exception as e:
            job_logger.warning(
                f"HTTP download failed for {upload_id}: {e}, will try local"
            )
            return None
        finally:
            await client.close()

    async def approve_frozen_job(self, job_id: str) -> Dict[str, Any]:
        """Approve a frozen job, marking it as truly completed.

        This method is called when a human operator reviews a frozen job
        and decides it is ready to be marked as completed.

        Delegates to the orchestrator's approve endpoint, which handles
        all DB writes (status, completed_at, freeze_data) and workspace
        file management (job_frozen.json → job_completion.json).

        Args:
            job_id: The job ID to approve

        Returns:
            Dict with approval result

        Raises:
            ValueError: If approval fails
        """
        import os
        from agent.api.orchestrator_client import OrchestratorClient

        orchestrator_url = os.getenv("ORCHESTRATOR_URL", "http://localhost:8085")
        client = OrchestratorClient(
            orchestrator_url=orchestrator_url,
            pod_ip="",
            pod_port=0,
            hostname="",
            config_name="",
        )

        try:
            await client.connect()
            success = await client.approve_job(job_id)
            if not success:
                raise ValueError(f"Orchestrator failed to approve job {job_id}")

            return {
                "job_id": job_id,
                "status": "approved",
            }
        finally:
            await client.close()

    async def shutdown(self) -> None:
        """Shutdown the agent and cleanup resources."""
        logger.info(f"Shutting down {self.config.display_name}...")
        self._shutdown_requested = True

        # Close database connections
        if (
            hasattr(self, "vector_conn")
            and self.vector_conn
            and self.vector_conn is not self.postgres_conn
        ):
            try:
                await self.vector_conn.close()
            except Exception as e:
                logger.warning(f"Error closing Vector DB: {e}")

        if self.postgres_conn:
            try:
                # PostgresDB uses close() not disconnect()
                await self.postgres_conn.close()
            except Exception as e:
                logger.warning(f"Error closing PostgreSQL: {e}")

        self._initialized = False
        logger.info(f"{self.config.display_name} shutdown complete")

    def get_status(self) -> Dict[str, Any]:
        """Get current agent status and metrics."""
        uptime = (datetime.utcnow() - self._start_time).total_seconds()

        # Auxiliary-task health (memory/curation/titles). Surfaced here so a
        # silently-degraded auxiliary model is visible on the status endpoint
        # instead of only in rotating WARNING logs.
        aux_llm = getattr(self, "_auxiliary_llm", None)
        aux_health = aux_llm.health.snapshot() if aux_llm is not None else None

        # Embedding-path health (B4): degraded == dimension mismatch latched.
        from shared.runtime.services.embedding_service import peek_embedding_service
        from agent.tools.research.utils.provider_health import (
            get_paper_provider_health,
        )

        emb_service = peek_embedding_service()

        return {
            "agent_id": self.config.agent_id,
            "display_name": self.config.display_name,
            "initialized": self._initialized,
            "shutdown_requested": self._shutdown_requested,
            "current_job": self._current_job_id,
            "jobs_processed": self._jobs_processed,
            "uptime_seconds": uptime,
            "connections": {
                "postgres": self.postgres_conn is not None,
            },
            "config": {
                "model": self.config.llm.model,
            },
            "auxiliary": aux_health,
            "embedding": emb_service.health_snapshot()
            if emb_service is not None
            else None,
            # Local arXiv compatibility plus the latest real Semantic Scholar
            # result. This snapshot never triggers provider I/O and contains
            # credential presence only, so heartbeat/status polling stays cheap
            # and secret-free. Deployment acceptance can populate it with
            # ``python -m src.tools.research.utils.provider_health`` inside the
            # worker image.
            "research_providers": get_paper_provider_health(),
        }
