"""Persistent Agent Session State.

Encapsulates all state for an interactive persistent agent session.
Created once during lifespan startup, lives until the session ends.

Composes around UniversalAgent — reuses its initialized LLMs, DB connections,
and config without subclassing or modifying it.
"""

import asyncio
import hashlib
import json
import logging
import os
import time
from dataclasses import asdict as _dc_asdict
from dataclasses import dataclass, field
from dataclasses import is_dataclass as _dc_is_dataclass
from dataclasses import replace as _dc_replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import BaseMessage

from agent.core.context import ContextConfig, ContextManager
from shared.runtime.core.loader import (
    AgentConfig,
    FileResolver,
    get_all_tool_names,
    get_phase_system_prompt,
    get_project_root,
    render_instruction_content,
    supports_parallel_tool_calls,
)
from shared.runtime.core.product_capabilities import ProductComponent
from shared.runtime.core.runtime_provenance import (
    component_provenance_from_environment,
    inherited_content_provenance,
    unavailable_component_provenance,
)
from shared.runtime.core.skill_resolution import (
    APP_GUIDE_LOADER_TOOL,
    APP_GUIDE_SKILL,
    skill_bundle_digest,
)
from agent.core.workspace import WorkspaceManager, WorkspaceManagerConfig
from shared.runtime.core.workspace_backend import (
    WorkspaceAuthenticationError,
    WorkspaceUnavailableError,
)
from shared.runtime.services.memory_prompts import (
    resolve_citation_verification_prompt,
    resolve_memory_extraction_prompt,
)
from agent.tools import ToolContext, load_tools, apply_instruction_enforcement
from agent.tools.context import SessionRuntimeFacts
from agent.tools.description_manager import apply_description_overrides

logger = logging.getLogger(__name__)


class MemoryUnavailableError(RuntimeError):
    """A configured (required) memory component could not be set up.

    Raised during session setup when the memory pipeline is configured but its
    embedding-backed stores failed to initialize, or a plugin factory could not
    resolve its transport (e.g. reranker with no reachable endpoint). Treated
    like the worker path's ``memory.required`` freeze: the session must NOT run
    half-working (silently without memory/reranking) — it fails loud. The
    lifespan handler exits the pod cleanly (status 0, no crash-loop) and the
    cockpit re-surfaces the reason via the orchestrator's create/prepare
    pre-flight. See
    knowledge-base/knowledge/issues/openrouter_auxiliary_crashes_session_via_memory_reranker.md.
    """


class OfficerKnowledgeBindingError(RuntimeError):
    """A background-officer attach violates the project-binding invariant.

    officer_knowledge_plane.md §3.1: a commissioned background officer has
    exactly one project and exactly one matching native writable
    KnowledgeBinding; every other KB is read-only. A session that fails this
    is a mis-bound officer — booting it would let his notes/backlog land in
    the wrong (or no) project truth, so the attach fails loudly instead.
    Distinct from a KB *outage*, which is survivable (degraded) and must NOT
    kill the officer.
    """


class CloudOverlayUnavailable(Exception):
    """Precondition signal for ``POST /cloud-overlay/reset``: no session, no
    overlay manager, or an inactive overlay — the reset target simply isn't
    there (route maps it to 404 "give up", never retry).

    Deliberately NOT a ``RuntimeError`` subclass: the mount managers' real
    failure types (``OverlayMountError``, ``RcloneMountError``) both subclass
    ``RuntimeError``, so a RuntimeError-based precondition would let a genuine
    remount/vfs-refresh failure be swallowed into the 404 branch instead of
    surfacing as a 500 (retry/alert) to the orchestrator caller.
    """


# Phase-specific tools that don't apply to interactive mode
_EXCLUDED_TOOLS = frozenset(
    {
        "next_phase_todos",
        "todo_complete",
        "todo_list",
        "request_replan",
        "mark_complete",
        "job_complete",
    }
)

_FLEET_MANAGEMENT_DISABLED_KEY = "_fleet_management_disabled"
_JOB_CONTROL_DISABLED_KEY = "_job_control_disabled"
_JOB_INSPECTION_DISABLED_KEY = "_job_inspection_disabled"
_AGENT_CATALOG_DISABLED_KEY = "_agent_catalog_disabled"
_WORKFLOWS_DISABLED_KEY = "_workflows_disabled"
_CANVAS_DISABLED_KEY = "_canvas_disabled"
_FLEET_MANAGEMENT_CONTROL_TOOLS = {"request_workspace_upgrade"}
_CANVAS_SKILL_NAME = "present-with-canvas"
_CANVAS_SKILL_MANIFEST = f"skills/{_CANVAS_SKILL_NAME}/.srw-managed.json"
_CANVAS_SKILL_MANIFEST_OWNER = "srw-present-with-canvas-v1"

# ENOTCONN watchdog probe interval for the protected-mode capture overlay
# (design §11.6 #3). Module-level so tests can monkeypatch it down to a tiny
# value instead of sleeping through the real 60s.
_CLOUD_OVERLAY_MONITOR_INTERVAL_SECONDS = 60.0

# Entries that can exist in a genuinely-fresh workspace root and must not
# make the attach guard treat it as content-bearing (ext4 PVCs grow a
# lost+found at mount).
_BENIGN_WORKSPACE_ENTRIES = frozenset({"lost+found"})


def _fleet_management_enabled(config: Any) -> bool:
    """Return whether SRW control-plane tools should be exposed.

    Existing session configs predate a UI toggle for these app-control tools,
    so absence of the marker means enabled. The marker is written only when the
    user explicitly disables ``tools.orchestrator`` in the session config
    override. Expert/skill catalog visibility is controlled separately by
    ``tools.agent_catalog``.
    """
    extra = getattr(config, "extra", {}) or {}
    return extra.get(_FLEET_MANAGEMENT_DISABLED_KEY) is not True


def _job_control_enabled(config: Any) -> bool:
    """Return whether the descriptor-backed job-control group is enabled."""
    extra = getattr(config, "extra", {}) or {}
    return extra.get(_JOB_CONTROL_DISABLED_KEY) is not True


def _job_inspection_enabled(config: Any) -> bool:
    """Return whether the descriptor-backed job-inspection group is enabled."""
    extra = getattr(config, "extra", {}) or {}
    return extra.get(_JOB_INSPECTION_DISABLED_KEY) is not True


def _agent_catalog_enabled(config: Any) -> bool:
    """Return whether expert/skill catalog tools should be exposed."""
    extra = getattr(config, "extra", {}) or {}
    return extra.get(_AGENT_CATALOG_DISABLED_KEY) is not True


def _workflows_enabled(config: Any) -> bool:
    """Return whether automation/project-loop workflow tools should be exposed."""
    extra = getattr(config, "extra", {}) or {}
    return extra.get(_WORKFLOWS_DISABLED_KEY) is not True


def _canvas_enabled(config: Any) -> bool:
    """Return whether the session's independent Canvas tool group is enabled."""
    extra = getattr(config, "extra", {}) or {}
    return extra.get(_CANVAS_DISABLED_KEY) is not True


@dataclass
class PersistentSession:
    """State for an interactive persistent agent session.

    Holds all components needed for the persistent loop:
    workspace, tools, LLM, context manager, and conversation history.
    """

    thread_id: str
    config: AgentConfig
    # Set only by the stateless executor. It must reach RemoteBackend before
    # workspace/Git initialization can issue its first tmux command.
    shell_owner_token: Optional[int] = None
    # Exact attach-time safety requirement.  When true, setup must establish
    # the sole read-only Nextcloud lower and its capture overlay before tools
    # or readiness can exist; pinned degraded-mount fallback is forbidden.
    protected_cloud_required: bool = False
    # New pinned-runtime contract. Physical workspaces must carry an exact
    # generation/incarnation/SSH identity and terminal cleanup must produce the
    # tier-specific local zero-writer proof before settlement can be ACKed.
    pinned_runtime_identity_required: bool = False

    # U5 session-parent delegation.  These are injected by persistent_app so
    # this state object never imports process-global owner/lease machinery.
    orchestrator_client: Optional[Any] = None
    session_parent_authority_provider: Optional[Callable[[], Any]] = None
    subagent_provider_admission: Optional[Callable[[], bool]] = None
    subagent_effect_authority: Optional[Callable[[], Any]] = None
    subagent_settlement_authority: Optional[Callable[[], Any]] = None
    subagent_event_callback: Optional[Callable[[str], Any]] = None

    # Permission mode (switchable at runtime)
    permission_mode: str = "supervised"
    # Narration mode (switchable at runtime)
    narration_mode: str = "auto"

    # Conversation state
    messages: List[BaseMessage] = field(default_factory=list)
    turn_count: int = 0

    # Initialized during setup()
    workspace_manager: Optional[WorkspaceManager] = None
    tools: Optional[List[Any]] = None
    llm_with_tools: Optional[BaseChatModel] = None
    context_manager: Optional[ContextManager] = None
    tool_context: Optional[ToolContext] = None
    system_prompt: str = ""
    auxiliary_llm: Optional[Any] = None
    # Matrix-resolved prompt for memory extraction; threaded into the loop
    # and the teardown extraction sites (MemoryConfig carries no prompt).
    memory_extraction_prompt: str = ""
    # Citation verification (Phase 2): the AuxiliaryLLM used to verify citations
    # (the aux model in sessions) + its matrix-resolved prompt. Threaded onto
    # ToolContext so the citation engine schedules async verdict write-back.
    citation_verify_aux: Optional[Any] = None
    citation_verification_prompt: str = ""
    shell_manager: Optional[Any] = None

    # DB connections (for message persistence + memory)
    postgres_conn: Optional[Any] = None
    vector_conn: Optional[Any] = None

    # Memory/knowledge stores (initialized during setup)
    recall_store: Optional[Any] = None
    knowledge_store: Optional[Any] = None
    # MemoryManager seam (src.services.memory) — bound in _setup_memory()
    # behind memory.manager.enabled; None keeps the legacy direct-store
    # paths in persistent_graph.py and persistent_app.py.
    memory_service: Optional[Any] = None
    # Set only after every detached memory/citation writer has been terminally
    # joined.  Queue transitions and claimant-loss ACKs depend on this proof.
    _background_tasks_quiesced: bool = False
    _subagent_runtime_quiesced: bool = False
    # B11 double-extraction guard: set after a manager-path session_end/
    # idle_archive capture so _terminate_session doesn't re-extract.
    final_memory_extracted: bool = False
    _knowledge_graph: Optional[Any] = None
    project_ids: List[str] = field(default_factory=list)
    # Owning user UUID (read from the threads row during setup). Plumbed
    # into ToolContext so agent-initiated orchestrator calls (jobs.py,
    # messaging.py) can forward X-MCP-User-Id and reach user-scoped
    # endpoints like GET /api/jobs/{id} without 401-ing.
    user_id: Optional[str] = None

    # Raw LLM (without tools bound, for summarization fallback)
    _llm: Optional[BaseChatModel] = None

    # Session task manager (lightweight in-session todos)
    session_task_manager: Optional[Any] = None

    # Exact hashes of companion-skill files this runtime created. The persisted
    # manifest carries the same ownership proof across session-agent restarts.
    _managed_canvas_skill_files: Dict[str, str] = field(default_factory=dict)
    _canvas_skill_manifest_owned: bool = False

    # Nextcloud workspace sync (initialized if session has nc_session_folder)
    workspace_sync: Optional[Any] = None
    # Stateless-only immutable requirements armed at turn start. A background
    # generation push captures this dict rather than the mutable LeaseHandle,
    # whose token is repointed when lite affinity claims the next turn.
    cloud_sync_requirements: Dict[str, Any] = field(default_factory=dict)
    # Orchestrator-attested binding generation, retained even when the current
    # claim has no cloud target so pending rows for a removed/degraded mount
    # cannot be hidden merely by omitting the coordinator payload.
    cloud_sync_workspace_generation: str = ""
    workspace_generation: str = ""
    workspace_runtime_incarnation: str = ""
    workspace_backend_tier: str = ""
    # Backend constructed/connected before WorkspaceManager publication. A
    # mid-setup failure must still retire it and prove zero writers before a
    # failed-attach release rotates the runtime generation.
    _workspace_backend_for_cleanup: Optional[Any] = None
    # Lazy rclone cloud mounts (initialized from cloud_mount payload)
    cloud_mount_manager: Optional[Any] = None
    cloud_mount_error: Optional[str] = None
    # Capture overlay stacked on the RO lower for protected sessions (B9)
    overlay_mount_manager: Optional[Any] = None
    # Exact physical workspace identity that authorized this protected
    # overlay. Retained for destructive reset fencing; never inferred from a
    # mutable remote endpoint at decision time.
    protected_workspace_generation: str = ""
    protected_workspace_runtime_incarnation: str = ""
    # Set only after strict protected cleanup proves that the dedicated
    # workspace UID has no remaining writer (including detached browser/IDE
    # and tag-cleared setsid descendants). The agent may echo this exact
    # protocol in the final retirement ACK; absence always fails closed.
    local_quiescence_protocol: str = ""
    # mount_id of the protected_lower rclone mount, read from the mount
    # payload at overlay-creation time (never re-derived) so the ENOTCONN
    # monitor can target restart_mount() at the right mount (Task 11).
    _protected_mount_id: Optional[str] = None
    # ENOTCONN watchdog task for the capture overlay; started alongside the
    # overlay mount, cancelled in cleanup() (Task 11).
    _cloud_overlay_monitor_task: Optional[asyncio.Task] = None
    # Controller ownership (``active``) is not a liveness proof: a mounted
    # overlay remains active while its dead lower returns ENOTCONN. Runtime
    # admission additionally requires this exact post-probe health latch.
    _protected_cloud_health_ready: bool = False

    # Datasource connections keyed by type (for ToolContext)
    datasources: Dict[str, Any] = field(default_factory=dict)
    # Authorized native + selected external OKF KB scopes. External entries
    # carry ids/display metadata only; repository credentials stay orchestrator-side.
    knowledge_bindings: List[Any] = field(default_factory=list)
    # Hidden server-derived identity used by job-surface and knowledge PEPs.
    runtime_actor: Optional[Any] = None
    # Parent clients for cleanup (e.g. MongoClient)
    _datasource_clients: Dict[str, Any] = field(default_factory=dict)
    # Raw datasource payloads (orchestrator-shaped dicts) currently attached —
    # the diff baseline + README.md facts-block input for live datasource changes
    # (live_session_settings.md Slice B). Set at attach; replaced by
    # resetup_datasources().
    datasource_configs: List[Dict[str, Any]] = field(default_factory=list)

    # Per-tool-call approval decisions (tool_call_id -> 'approved'|'denied').
    # Populated by the WS permission_check; consumed at turn save so the
    # decision is persisted alongside the tool call in thread_messages.
    tool_decisions: Dict[str, str] = field(default_factory=dict)

    @property
    def project_id(self) -> Optional[str]:
        """Primary project (first in list) for backward compat."""
        return self.project_ids[0] if self.project_ids else None

    async def setup(
        self,
        llm: BaseChatModel,
        auxiliary_llm: Optional[Any] = None,
        postgres_conn: Optional[Any] = None,
        vector_conn: Optional[Any] = None,
        workspace_override: Optional[Dict[str, Any]] = None,
        git_remote_url: Optional[str] = None,
        cloud_mount_cfg: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize session resources.

        Creates workspace, loads tools, binds LLM, and builds system prompt.
        Reuses the same infrastructure as UniversalAgent but without phase
        alternation components.

        Args:
            llm: Base LLM instance (will be bound with tools)
            auxiliary_llm: For summarization during compaction
            postgres_conn: PostgreSQL connection (for DB tools)
            vector_conn: Vector DB connection (for citations/memory)
            workspace_override: Remote workspace config from orchestrator
                (e.g. {"backend": "remote", "remote": {"host": ..., "port": 22, ...}})
            git_remote_url: Gitea repo URL for workspace versioning
        """
        workspace_override = dict(workspace_override or {})
        managed_repository_credentials = workspace_override.pop(
            "managed_repository_credentials", None
        )
        self._llm = llm
        self.auxiliary_llm = auxiliary_llm
        self.postgres_conn = postgres_conn
        self.vector_conn = vector_conn
        self.permission_mode = self.config.interactive.permission_mode
        self.narration_mode = self.config.interactive.narration_mode
        self.memory_extraction_prompt = resolve_memory_extraction_prompt(self.config)
        # Citation verification (Phase 2): reuse the auxiliary model in sessions,
        # gated by auxiliary.tasks.verify_citations.
        _verify_task = self.config.auxiliary.tasks.get("verify_citations")
        if (
            self.config.auxiliary.enabled
            and _verify_task is not None
            and _verify_task.enabled
        ):
            self.citation_verify_aux = self.auxiliary_llm
            self.citation_verification_prompt = resolve_citation_verification_prompt(
                self.config
            )
        else:
            self.citation_verify_aux = None
            self.citation_verification_prompt = ""

        _steps: Dict[str, float] = {}
        _t = time.perf_counter()

        try:
            await self._setup_steps(
                _steps,
                _t,
                llm=llm,
                auxiliary_llm=auxiliary_llm,
                postgres_conn=postgres_conn,
                vector_conn=vector_conn,
                workspace_override=workspace_override,
                git_remote_url=git_remote_url,
                managed_repository_credentials=(managed_repository_credentials),
                cloud_mount_cfg=cloud_mount_cfg,
            )
        finally:
            # Close the scoped metadata index opened in _setup_workspace, on
            # every path — a failed setup must not leave a cached view behind
            # for whatever runs next on this backend object.
            self._end_backend_read_cache()

    async def _setup_steps(
        self,
        _steps: Dict[str, float],
        _t: float,
        *,
        llm: BaseChatModel,
        auxiliary_llm: Optional[Any],
        postgres_conn: Optional[Any],
        vector_conn: Optional[Any],
        workspace_override: Optional[Dict[str, Any]],
        git_remote_url: Optional[str],
        cloud_mount_cfg: Optional[Dict[str, Any]],
        managed_repository_credentials: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """The ordered setup steps of :meth:`setup` (see its docstring).

        Split out only so ``setup`` can wrap the whole sequence in the
        scoped-index try/finally without indenting every step.
        """
        # 0. Background-officer project-binding invariant
        #    (officer_knowledge_plane.md §3.1, K1). Runs before any resource
        #    is created so a mis-bound officer fails the attach outright.
        self._enforce_officer_knowledge_invariant()
        if self.protected_cloud_required and (
            not isinstance(workspace_override, dict)
            or workspace_override.get("backend") != "sandbox"
        ):
            raise WorkspaceUnavailableError(
                "protected cloud requires a sandbox workspace"
            )

        # 1. Create workspace (with optional remote backend + git)
        await self._setup_workspace(
            workspace_override=workspace_override,
            git_remote_url=git_remote_url,
            managed_repository_credentials=managed_repository_credentials,
        )
        await self._seed_workspace_baseline_commit(postgres_conn)
        _steps["workspace"] = time.perf_counter() - _t
        _t = time.perf_counter()

        # 2. Set up lazy cloud mounts before shell/tools so `/workspace/cloud`
        #    exists when the agent starts using the workspace.
        await self._setup_cloud_mount(cloud_mount_cfg)
        _steps["cloud_mount"] = time.perf_counter() - _t
        _t = time.perf_counter()

        # 3. Set up shell manager BEFORE tools so shell tools can detect it
        self._setup_shell_manager()
        _steps["shell"] = time.perf_counter() - _t
        _t = time.perf_counter()

        # 4. Initialize knowledge base connections BEFORE tools so knowledge
        #    tools can detect them via ToolContext.has_knowledge()
        self._setup_knowledge(vector_conn)
        _steps["knowledge"] = time.perf_counter() - _t
        _t = time.perf_counter()

        # 4b. Resolve the owning user from the thread row so tools can forward
        #     X-MCP-User-Id on orchestrator calls (fixes the agent's read-job
        #     401 against require_approved_user / require_job_access endpoints).
        if postgres_conn is not None and self.user_id is None:
            try:
                thread = await postgres_conn.get_thread(self.thread_id)
                if thread and thread.get("user_id"):
                    self.user_id = str(thread["user_id"])
            except Exception as e:
                # Non-fatal — tools that need user_id will fall back to the
                # internal-only auth path. Lifecycle calls keep working.
                logger.warning(
                    "Could not resolve user_id for thread %s: %s",
                    self.thread_id,
                    e,
                )

        _steps["user_id"] = time.perf_counter() - _t
        _t = time.perf_counter()

        # 5. Create tool context and load tools
        self._setup_tools(postgres_conn)
        await self._hydrate_durable_session_state()
        _steps["tools"] = time.perf_counter() - _t
        _t = time.perf_counter()

        # 6. Bind tools to LLM
        self._bind_tools()
        _steps["bind"] = time.perf_counter() - _t
        _t = time.perf_counter()

        # 7. Create context manager
        self._setup_context_manager()
        self._wire_subagent_context_probe()
        _steps["context"] = time.perf_counter() - _t
        _t = time.perf_counter()

        # 8. Build system prompt (interactive mode has its own prompt files)
        self.system_prompt = get_phase_system_prompt(
            self.config,
            is_strategic=False,
            model=self.config.llm.model or "",
            tool_names=[t.name for t in self.tools] if self.tools else None,
            prompt_type="interactive",
        )
        _steps["prompt"] = time.perf_counter() - _t
        _t = time.perf_counter()

        # 9. Set up memory (RecallStore) if enabled
        self._setup_memory(postgres_conn, vector_conn)
        self._refresh_runtime_facts()
        _steps["memory"] = time.perf_counter() - _t

        logger.info(
            f"PersistentSession initialized: thread={self.thread_id}, "
            f"tools={len(self.tools or [])}, "
            f"mode={self.permission_mode}"
        )
        logger.info(
            "setup steps: %s | store: %s",
            " ".join(f"{k}={v:.2f}s" for k, v in _steps.items() if v >= 0.01),
            self._drain_store_stats(),
        )

    async def _seed_workspace_baseline_commit(self, postgres_conn: Any) -> None:
        """Seed seq=0 with the Git HEAD visible before the first turn.

        The row is create-once in Postgres, so every later pod observes the
        same pre-first-turn baseline.  Stateless sandbox attach fails closed
        when an active Git workspace cannot durably publish that baseline;
        pinned sessions retain their historical best-effort setup behavior.
        Git-disabled virtual/none workspaces intentionally have no baseline
        and their undo surface remains unavailable.
        """

        git_manager = getattr(self.workspace_manager, "git_manager", None)
        if git_manager is None or not git_manager.is_active:
            return

        try:
            if postgres_conn is None:
                raise RuntimeError("Postgres is unavailable for workspace baseline")
            commit_sha = await asyncio.to_thread(git_manager.get_current_commit)
            if not commit_sha:
                raise RuntimeError("Git HEAD is unavailable for workspace baseline")
            await postgres_conn.seed_workspace_baseline_commit(
                self.thread_id,
                commit_sha,
            )
            # Reconcile the current durable HEAD to the latest transcript seq
            # on every attach.  This heals the crash window where a prior pod
            # pushed a turn/undo commit but died before its ledger upsert.  On
            # the first attach it simply records the same baseline beside the
            # already accepted user row; seq=0 remains the immutable fallback.
            await postgres_conn.record_turn_commit(self.thread_id, commit_sha)
            logger.debug(
                "Workspace baseline/head are durable: thread=%s commit=%s",
                self.thread_id,
                commit_sha,
            )
        except Exception:
            if self.shell_owner_token is not None:
                logger.error(
                    "Stateless workspace baseline seed failed: thread=%s",
                    self.thread_id,
                    exc_info=True,
                )
                raise
            logger.warning(
                "Pinned workspace baseline seed skipped after failure: thread=%s",
                self.thread_id,
                exc_info=True,
            )

    def _unwrapped_backend(self, backend: Any = None) -> Any:
        """The real backend behind any virtual overlay wrapper."""
        try:
            from agent.core.backends.overlay import unwrap_backend

            if backend is None:
                backend = getattr(self.workspace_manager, "backend", None)
            return unwrap_backend(backend) if backend is not None else None
        except Exception:
            return None

    def _begin_backend_read_cache(self, backend: Any = None) -> None:
        """Open the backend's scoped metadata index, if it has one.

        Only the virtual (object-store) backend implements this — it is the
        one whose metadata probes are process spawns. Every other backend
        no-ops, and a failure here is never fatal: the index is an
        optimization, not a correctness requirement.
        """
        target = self._unwrapped_backend(backend)
        begin = getattr(target, "begin_read_cache", None)
        if begin is None:
            return
        try:
            begin()
        except Exception:
            logger.debug("backend read-cache priming skipped", exc_info=True)

    def _end_backend_read_cache(self, backend: Any = None) -> None:
        target = self._unwrapped_backend(backend)
        end = getattr(target, "end_read_cache", None)
        if end is None:
            return
        try:
            end()
        except Exception:
            logger.debug("backend read-cache close failed", exc_info=True)

    def _drain_store_stats(self) -> str:
        """Object-store op tally for the phase that just ran, if the backend
        keeps one (rclone-backed virtual workspaces do — every op there is a
        process spawn, so the count IS the cost)."""
        try:
            from agent.core.backends.overlay import unwrap_backend

            backend = getattr(self.workspace_manager, "backend", None)
            if backend is None:
                return "n/a"
            store = getattr(unwrap_backend(backend), "_store", None)
            drain = getattr(store, "drain_op_stats", None)
            return drain() if drain is not None else "n/a"
        except Exception:
            return "n/a"

    async def _setup_workspace(
        self,
        workspace_override: Optional[Dict[str, Any]] = None,
        git_remote_url: Optional[str] = None,
        managed_repository_credentials: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """Create workspace using a remote backend (required).

        Persistent sessions always require an isolated workspace container or
        VM. If the remote backend is not immediately reachable (e.g. sshd
        still starting), retries with exponential backoff for up to 5 minutes
        before raising ``WorkspaceUnavailableError``.

        Args:
            workspace_override: If provided, overrides config workspace settings.
                Expected shape: {"backend": "remote", "remote": {"host": ..., "port": 22, ...}}
            git_remote_url: Gitea repo URL for workspace versioning (clones on init)
        """
        ws_data = self.config.workspace
        base_path = os.getenv("WORKSPACE_PATH", "./workspace")

        effective_backend = (workspace_override or {}).get("backend") or ws_data.backend
        self.workspace_backend_tier = str(effective_backend or "")
        remote_cfg = (workspace_override or {}).get("remote") or ws_data.remote
        workspace_provisioner = (workspace_override or {}).get("workspace_provisioner")
        workspace_generation = (workspace_override or {}).get("workspace_generation")
        workspace_runtime_incarnation = (workspace_override or {}).get(
            "workspace_runtime_incarnation"
        )
        workspace_ssh_host_key_fingerprint = (workspace_override or {}).get(
            "workspace_ssh_host_key_fingerprint"
        )
        self.workspace_generation = str(workspace_generation or "")
        self.workspace_runtime_incarnation = str(workspace_runtime_incarnation or "")

        # No-workspace tiers (virtual/none): no SSH workspace pod. Build the
        # lite backend directly, with git off (§8 — lite tiers have no git).
        from agent.core.backends.factory import LITE_BACKENDS, create_lite_backend

        if effective_backend in LITE_BACKENDS:
            from types import SimpleNamespace

            lite_cfg = SimpleNamespace(
                backend=effective_backend,
                mounts=(workspace_override or {}).get("mounts") or ws_data.mounts,
            )
            workspace_backend = create_lite_backend(lite_cfg, job_id=self.thread_id)
            self._workspace_backend_for_cleanup = workspace_backend
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, workspace_backend.connect)
            # Setup asks the store "does this exist?" dozens of times about
            # one small tree (scaffolding, instruction files, skill files,
            # the legacy-tools sweep) and every ask is an rclone spawn.
            # Answer them all from one listing; the scope closes at the end
            # of setup() so tool work is never served from a cache.
            self._begin_backend_read_cache(workspace_backend)
            self.workspace_manager = WorkspaceManager(
                job_id=self.thread_id,
                base_path=base_path,
                config=WorkspaceManagerConfig(
                    base_path=base_path,
                    structure=ws_data.structure,
                    git_versioning=False,
                ),
                backend=workspace_backend,
            )
            self.workspace_manager.initialize()
            self._deploy_instruction_files()
            logger.info(
                "Lite workspace ready (backend=%s, no workspace pod)",
                effective_backend,
            )
            return

        if not (effective_backend in ("sandbox", "vm", "remote") and remote_cfg):
            raise RuntimeError(
                "No workspace configured. Persistent sessions require "
                "an isolated workspace (sandbox or vm) with SSH credentials."
            )
        normalized_provisioner = str(workspace_provisioner or "").strip().lower()
        if effective_backend == "sandbox" and normalized_provisioner not in {
            "k8s",
            "docker",
        }:
            raise WorkspaceUnavailableError(
                "A sandbox session requires server-derived workspace "
                "provisioner authority"
            )
        physical_identity_required = bool(
            self.shell_owner_token is not None
            or self.protected_cloud_required
            or self.pinned_runtime_identity_required
            or (effective_backend == "sandbox" and normalized_provisioner == "k8s")
        )
        if physical_identity_required and (
            not workspace_generation
            or not workspace_runtime_incarnation
            or not workspace_ssh_host_key_fingerprint
        ):
            raise WorkspaceUnavailableError(
                "An exact pinned/stateless physical session requires an "
                "orchestrator-attested workspace backing, runtime incarnation, "
                "and SSH host identity"
            )

        from shared.runtime.core.backends.remote import RemoteBackend

        shell_config = self.config.extra.get("shell", {})
        max_duration = 300  # 5 minutes
        start = time.monotonic()
        backoff = 5.0
        attempt = 0
        workspace_backend = None

        while True:
            attempt += 1
            try:
                workspace_backend = RemoteBackend(
                    host=remote_cfg["host"],
                    port=remote_cfg.get("port", 22),
                    username=remote_cfg.get("username", "agent-host"),
                    key_path=remote_cfg.get("key_path", "/run/secrets/vm-ssh-key"),
                    workspace_path=remote_cfg.get(
                        "workspace_path", "/home/agent-host/workspace"
                    ),
                    job_id=self.thread_id,
                    default_timeout=shell_config.get("default_timeout", 120),
                    max_tabs=shell_config.get("max_tabs", 15),
                    connect_timeout=remote_cfg.get("connect_timeout", 30),
                    max_retries=remote_cfg.get("max_retries", 5),
                    retry_timeouts_as_booting=remote_cfg.get(
                        "retry_timeouts_as_booting", False
                    ),
                    sudo_action=shell_config.get("sudo_action", "freeze"),
                    # Current pinned attaches also carry the provisioner-owned
                    # backing/runtime pair.  Shell fencing remains stateless-
                    # only, but managed-repository receipts use this pair to
                    # distinguish a remounted PVC from same-runtime PID reuse.
                    workspace_generation=(
                        workspace_generation if physical_identity_required else None
                    ),
                    runtime_incarnation=(
                        workspace_runtime_incarnation
                        if physical_identity_required
                        else None
                    ),
                    expected_host_key_fingerprint=(
                        workspace_ssh_host_key_fingerprint
                        if physical_identity_required
                        else None
                    ),
                    require_host_key_fingerprint=physical_identity_required,
                    workspace_tier=str(effective_backend),
                )
                self._workspace_backend_for_cleanup = workspace_backend
                if self.shell_owner_token is not None:
                    workspace_backend.set_shell_owner_token(self.shell_owner_token)
                # Only the internal attach API can attest that this exact SSH
                # endpoint is paired to a Canvas generation and pinned host
                # identity. Direct remote config, VMs, and legacy overrides all
                # remain fail-closed even when the connection itself is usable.
                workspace_backend.supports_canvas_presentation = (
                    workspace_override or {}
                ).get("canvas_presentation_available") is True
                workspace_backend.supports_canvas_live_apps = (
                    workspace_override or {}
                ).get("canvas_live_apps_available") is True
                workspace_backend.supports_canvas_shared_browser = (
                    workspace_override or {}
                ).get("canvas_shared_browser_available") is True
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, workspace_backend.connect)
                if self.shell_owner_token is not None:
                    # Promotion is eager, not lazy on the first shell tool: as
                    # soon as claim N+1 attaches, a stale N backend must fail
                    # even during an LLM-only turn.
                    await loop.run_in_executor(
                        None,
                        workspace_backend.claim_shell_owner,
                    )
                logger.info(
                    f"Remote workspace backend connected to {remote_cfg['host']}"
                )
                break
            except Exception as e:
                if physical_identity_required and isinstance(
                    e, WorkspaceAuthenticationError
                ):
                    raise WorkspaceUnavailableError(
                        "Pinned/stateless workspace SSH identity attestation "
                        f"failed: {e}"
                    ) from e
                elapsed = time.monotonic() - start
                if elapsed >= max_duration:
                    raise WorkspaceUnavailableError(
                        f"Failed to connect to workspace {remote_cfg['host']} "
                        f"after {attempt} attempts ({elapsed:.0f}s): {e}"
                    ) from e
                logger.warning(
                    "Workspace connect attempt %d failed (%.0fs elapsed, "
                    "retrying in %.0fs): %s",
                    attempt,
                    elapsed,
                    backoff,
                    e,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

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
        if repository_url_has_credentials(git_remote_url):
            raise ManagedRepositoryMaterializationError(
                "credentialed_managed_repository_url_refused"
            )
        if git_remote_url and str(git_remote_url).startswith("ssh://srw-repo-"):
            primary_name = (
                urlparse(str(git_remote_url))
                .path.rstrip("/")
                .rsplit("/", 1)[-1]
                .removesuffix(".git")
            )
            if primary_name not in runtime_repository_urls:
                raise ManagedRepositoryMaterializationError(
                    "managed_repository_transport_mismatch"
                )
            git_remote_url = runtime_repository_urls[primary_name]

        ws_config = WorkspaceManagerConfig(
            base_path=base_path,
            structure=ws_data.structure,
            git_versioning=ws_data.git_versioning,
            git_remote_url=git_remote_url,
        )
        self.workspace_manager = WorkspaceManager(
            job_id=self.thread_id,
            base_path=base_path,
            config=ws_config,
            backend=workspace_backend,
        )
        preserve_path = self._attach_existing_workspace(
            workspace_backend, git_remote_url
        )
        if preserve_path:
            logger.info(
                f"[thread {self.thread_id}] session_workspace_init_path="
                f"{preserve_path} — existing content preserved (no wipe, no clone)"
            )
        else:
            logger.info(f"[thread {self.thread_id}] session_workspace_init_path=fresh")
            self.workspace_manager.initialize()
        self._deploy_instruction_files()
        logger.info(
            f"Workspace created at {self.workspace_manager.path} (backend=remote)"
        )

    def _attach_existing_workspace(
        self, backend: Any, git_remote_url: Optional[str]
    ) -> Optional[str]:
        """Content probe ported from the job path's reattach guard.

        ``WorkspaceManager.initialize()`` empties the workspace root before
        cloning (``git clone`` needs an empty target). Session workspaces are
        PVC-backed and orchestrator-restored, so on attach any existing
        content belongs to THIS thread and must be preserved — the thread's
        Gitea repo only ever holds the scaffold, so the wipe is unrecoverable
        (knowledge-base/knowledge/issues/session_workspace_wiped_by_agent_clone_on_attach.md).

        Mirrors ``src/agent/agent.py``'s G2 reattach + resume-existing branches: a
        ``.git`` tree gets a git handle attached in place (no clone, no
        wipe); a git-less but content-bearing root gets git initialized
        around it (the clone inside ``_initialize_git`` fails on a non-empty
        target and falls back to ``git init``, same as the job path). Probe
        failures count as content: wrongly skipping the wipe degrades git,
        wrongly wiping loses user data.

        Returns the preserve path taken (for logging), or None when the root
        is genuinely empty and the caller should run ``initialize()``.
        """
        if not getattr(backend, "supports_shell", False):
            return None

        has_git: Optional[bool]
        try:
            has_git = backend.exists(".git")
        except Exception as e:
            logger.warning(
                f"[thread {self.thread_id}] workspace .git probe failed "
                f"(treating as content-bearing, skipping init): {e}"
            )
            has_git = None

        if has_git is False:
            entries: Optional[List[str]]
            try:
                entries = [
                    name
                    for name in backend.list_dir("")
                    if name not in _BENIGN_WORKSPACE_ENTRIES
                ]
            except Exception as e:
                logger.warning(
                    f"[thread {self.thread_id}] workspace content probe failed "
                    f"(treating as content-bearing, skipping init): {e}"
                )
                entries = None
            if entries is not None and not entries:
                return None

            # Content-bearing but git-less (e.g. uploads landed before first
            # attach, or a partial restore): initialize git around the
            # existing files instead of wiping them.
            if (
                self.workspace_manager.config.git_versioning
                and self.workspace_manager.git_manager is None
            ):
                self.workspace_manager._initialize_git()
            for subdir in self.workspace_manager.config.structure:
                try:
                    backend.mkdir(subdir)
                except Exception:
                    pass
            self.workspace_manager._initialized = True
            return "attach-content"

        # `.git` present (or unknowable): attach a handle to the existing
        # repo — no clone (the dir is non-empty), no rm -rf.
        if (
            self.workspace_manager.config.git_versioning
            and self.workspace_manager.git_manager is None
        ):
            from agent.managers.git_manager import GitManager

            git_mgr = GitManager(self.workspace_manager.path, backend=backend)
            self.workspace_manager._git_manager = git_mgr
            if git_remote_url:
                git_mgr.add_remote("origin", git_remote_url)
        self.workspace_manager._initialized = True
        return "reattach"

    async def _setup_cloud_mount(
        self, cloud_mount_cfg: Optional[Dict[str, Any]]
    ) -> None:
        """Adopt or start cloud mounts before exposing shell/tools.

        Pinned sessions retain their historical degraded-mode behaviour when a
        mount cannot be established.  A stateless claim cannot do that: the
        prior agent may have left workspace-side rclone/overlay processes for
        this thread, and using the workspace before the new claimant has
        adopted or healed them would expose stale credentials or a dead FUSE
        mount. ``RcloneMountManager.start_all`` therefore includes the first
        real directory probe and this method propagates ambiguous failures for
        stateless claims so :meth:`setup` stops before shell/tool construction.
        An ordinary non-protected mount may degrade only after the manager
        proves exact rollback of every newly-owned resident.
        """
        self._protected_cloud_health_ready = False
        if self.protected_cloud_required and not self._protected_cloud_config_valid(
            cloud_mount_cfg
        ):
            raise WorkspaceUnavailableError(
                "protected-cloud mount contract is missing or malformed"
            )
        if not cloud_mount_cfg:
            return
        # Background officer (officer_knowledge_plane.md §4): the project/cloud
        # folder is object plane — never mounted for a commissioned officer,
        # even when a provisioning payload carries a mount config. Without a
        # manager, the srw_cloud_status append never fires either (and the
        # officer capability ceiling would drop the tool regardless).
        from agent.tools.registry import officer_ceiling_active

        if officer_ceiling_active(getattr(self.config, "officer", None)):
            if self.protected_cloud_required:
                raise WorkspaceUnavailableError(
                    "protected cloud is not supported for an officer runtime"
                )
            logger.info(
                "Background officer session: refusing project cloud mount "
                "(object plane; officer_knowledge_plane.md §4)"
            )
            return
        if not self.workspace_manager:
            if self.shell_owner_token is not None or self.protected_cloud_required:
                raise WorkspaceUnavailableError(
                    "cloud mount requires an attached workspace"
                )
            return
        from shared.runtime.services.cloud_mount import (
            RcloneMountCleanFailure,
            RcloneMountManager,
        )

        try:
            self.cloud_mount_manager = RcloneMountManager(
                thread_id=self.thread_id,
                cloud_cfg=cloud_mount_cfg,
                workspace_backend=self.workspace_manager.backend,
                workspace_root=self.workspace_manager.path,
            )
            await self.cloud_mount_manager.start_all()
            logger.info(
                "Cloud mount manager started with %d mount(s)",
                len(self.cloud_mount_manager.mounts),
            )
        except Exception as e:
            self.cloud_mount_error = str(e)
            self.cloud_mount_manager = None
            logger.warning("Failed to start cloud mount manager: %s", e)
            if self.shell_owner_token is not None:
                # A stateless workspace normally fails closed because an
                # attach error may conceal an old resident/FUSE mount.  The
                # manager emits this subtype only after strict, exact cleanup
                # of every newly-owned resident. Optional ordinary cloud can
                # then degrade without blocking unrelated repository/shell
                # work. Protected cloud never degrades.
                if (
                    isinstance(e, RcloneMountCleanFailure)
                    and cloud_mount_cfg.get("required") is False
                    and not bool(cloud_mount_cfg.get("protected"))
                ):
                    logger.warning(
                        "Stateless optional cloud mount degraded after exact "
                        "resident cleanup: %s",
                        e,
                    )
                    return
                raise
            if self.protected_cloud_required:
                raise
            return

        if cloud_mount_cfg.get("protected") and cloud_mount_cfg.get("overlay"):
            try:
                from shared.runtime.services.cloud_overlay import OverlayMountManager

                # Read the protected_lower mount_id from the payload the
                # session actually received — never re-derive the
                # f"protected-{thread_id}" format (Task 11).
                self._protected_mount_id = next(
                    (
                        str(m.get("mount_id"))
                        for m in cloud_mount_cfg.get("mounts") or []
                        if m.get("mount_kind") == "protected_lower"
                    ),
                    None,
                )
                self.overlay_mount_manager = OverlayMountManager(
                    thread_id=self.thread_id,
                    overlay_cfg=cloud_mount_cfg["overlay"],
                    workspace_backend=self.workspace_manager.backend,
                    workspace_root=self.workspace_manager.path,
                )
                # runs the mount script on the workspace pod over SSH
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(
                    None,
                    self.overlay_mount_manager.mount,
                    lambda: self.cloud_mount_manager.restart_mount(
                        self._protected_mount_id
                    ),
                )
                if not await asyncio.to_thread(self.overlay_mount_manager.health_check):
                    raise WorkspaceUnavailableError(
                        "protected-cloud overlay failed its initial health proof"
                    )
                self._protected_cloud_health_ready = True
                logger.info(
                    "Capture overlay mounted for protected session %s", self.thread_id
                )
                # ENOTCONN watchdog (design §11.6 #3) — started here alongside
                # the overlay, cancelled in cleanup() alongside the unmount.
                self._cloud_overlay_monitor_task = asyncio.create_task(
                    self._cloud_overlay_monitor_loop(),
                    name=f"cloud-overlay-monitor-{self.thread_id[:8]}",
                )
            except Exception as e:
                self._protected_cloud_health_ready = False
                self.cloud_mount_error = f"overlay: {e}"
                # ``mount()`` can publish the remote overlay successfully and
                # then fail while creating the workspace-facing symlink.  The
                # manager is therefore potentially live even though the call
                # raised.  Retire that exact partially-started controller
                # before dropping our only handle to it; otherwise cleanup of
                # the lower alone leaves a resident overlay behind.
                failed_overlay = self.overlay_mount_manager
                if failed_overlay is not None:
                    try:
                        if self.shell_owner_token is not None:
                            await asyncio.to_thread(
                                failed_overlay.rollback_failed_mount
                            )
                        else:
                            await asyncio.to_thread(failed_overlay.unmount)
                    except Exception as overlay_close_err:
                        logger.warning(
                            "Error tearing down partial overlay after mount "
                            "failure: %s",
                            overlay_close_err,
                        )
                self.overlay_mount_manager = None
                self._protected_mount_id = None
                # Fail-safe: a protected session whose overlay failed to mount
                # must NOT keep running against the raw RO lower. Pinned keeps
                # its historical degraded-mode teardown. Stateless fails the
                # whole attach closed and locally detaches from an adopted
                # resident lower without destroying it for the successor.
                if self.shell_owner_token is not None:
                    logger.warning(
                        "Failed to mount capture overlay: %s — retiring this "
                        "claim's local lower controller; resident lower is "
                        "left for successor convergence",
                        e,
                    )
                else:
                    logger.warning(
                        "Failed to mount capture overlay: %s — tearing down RO "
                        "lower to avoid a half-protected session",
                        e,
                    )
                try:
                    if self.shell_owner_token is not None:
                        # The lower may be a healthy resident adopted from the
                        # predecessor. Attach failure owns neither terminal
                        # thread lifecycle nor permission to destroy that
                        # durable workspace resource. Retire only this claim's
                        # local refresh/client authority; the next claimant
                        # converges the overlay.
                        await self.cloud_mount_manager.detach_for_handoff()
                    else:
                        await self.cloud_mount_manager.aclose()
                except Exception as close_err:
                    logger.warning(
                        "Error tearing down RO lower after overlay failure: %s",
                        close_err,
                    )
                self.cloud_mount_manager = None
                if self.shell_owner_token is not None or self.protected_cloud_required:
                    raise

        if self.protected_cloud_required and not self.protected_cloud_ready():
            self._protected_cloud_health_ready = False
            try:
                if self.overlay_mount_manager is not None:
                    await asyncio.to_thread(self.overlay_mount_manager.unmount)
            finally:
                self.overlay_mount_manager = None
                if self.cloud_mount_manager is not None:
                    await self.cloud_mount_manager.aclose()
                self.cloud_mount_manager = None
                self._protected_mount_id = None
            raise WorkspaceUnavailableError(
                "protected-cloud lower and capture overlay are not both active"
            )

    @staticmethod
    def _protected_cloud_config_valid(payload: Any) -> bool:
        """Validate every protected field that can drive mount/delete work."""

        if not isinstance(payload, dict):
            return False
        overlay = payload.get("overlay")
        mounts = payload.get("mounts")
        if (
            type(payload.get("version")) is not int
            or payload.get("version") != 1
            or payload.get("driver") != "rclone"
            or payload.get("protected") is not True
            or payload.get("skip_workspace_links") is not True
            or payload.get("fallback") is not False
            or not isinstance(overlay, dict)
            or overlay.get("lower") != "/cloud/lower"
            or overlay.get("upper") != "/home/agent-host/.overlay/upper"
            or overlay.get("work") != "/home/agent-host/.overlay/work"
            or overlay.get("merged") != "/cloud/merged"
            or not isinstance(overlay.get("quota_bytes"), int)
            or isinstance(overlay.get("quota_bytes"), bool)
            or overlay.get("quota_bytes") <= 0
            or not isinstance(mounts, list)
            or len(mounts) != 1
        ):
            return False
        lower = mounts[0]
        if not isinstance(lower, dict):
            return False
        source = lower.get("source")
        source_config = source.get("config") if isinstance(source, dict) else None
        auth = lower.get("auth")
        return bool(
            isinstance(lower.get("mount_id"), str)
            and lower.get("mount_id")
            and lower.get("mount_kind") == "protected_lower"
            and lower.get("backend") == "nextcloud"
            and lower.get("target_path") == "/cloud/lower"
            and lower.get("workspace_name") == "lower"
            and lower.get("access") == "read_only"
            and isinstance(source, dict)
            and source.get("type") == "webdav"
            and isinstance(source_config, dict)
            and source_config.get("vendor") == "nextcloud"
            and isinstance(source_config.get("url"), str)
            and source_config.get("url")
            and isinstance(source_config.get("user"), str)
            and source_config.get("user")
            and isinstance(auth, dict)
            and auth.get("type") == "basic"
            and isinstance(auth.get("password"), str)
            and auth.get("password")
        )

    def protected_cloud_ready(self) -> bool:
        """Joined lower-controller + overlay readiness invariant."""

        if not self.protected_cloud_required:
            return True
        manager = self.cloud_mount_manager
        overlay = self.overlay_mount_manager
        if (
            manager is None
            or getattr(manager, "active", False) is not True
            or overlay is None
            or getattr(overlay, "active", False) is not True
            or not isinstance(self._protected_mount_id, str)
            or not self._protected_mount_id
            or self._protected_cloud_health_ready is not True
        ):
            return False
        states = getattr(manager, "mounts", None)
        if not isinstance(states, list) or len(states) != 1:
            return False
        state = states[0]
        return bool(
            getattr(state, "mount_id", None) == self._protected_mount_id
            and getattr(state, "mount_kind", None) == "protected_lower"
            and getattr(state, "target_path", None) == "/cloud/lower"
            and getattr(overlay, "lower", None) == "/cloud/lower"
            and getattr(overlay, "merged", None) == "/cloud/merged"
        )

    async def _cloud_overlay_monitor_loop(self) -> None:
        """ENOTCONN watchdog for the protected overlay (design §11.6 #3).

        Every probe interval, when the overlay is active: probe with
        ``health_check()``; on a dead lower, log and heal via
        ``overlay.heal(remount_lower=...)``, where the callback restarts the
        one rclone mount backing the lower (never the whole manager). Started
        alongside the overlay mount in ``_setup_cloud_mount`` and cancelled in
        ``cleanup()``. Must never die from an exception — a health_check or
        heal failure is logged and simply retried on the next tick.
        """
        while True:
            # Everything after the sleep is guarded (mirrors
            # RcloneMountManager._token_refresh_loop): an unexpected failure
            # anywhere here — including reading overlay_mount_manager/.active,
            # not just the health_check/heal calls — must not kill the task.
            try:
                await asyncio.sleep(_CLOUD_OVERLAY_MONITOR_INTERVAL_SECONDS)
                overlay = self.overlay_mount_manager
                if overlay is None or not overlay.active:
                    self._protected_cloud_health_ready = False
                    continue
                healthy = await asyncio.to_thread(overlay.health_check)
                if healthy:
                    self._protected_cloud_health_ready = True
                    continue
                # Close all runtime effect admission before a heal touches the
                # lower. Overlay.active intentionally stays true while the
                # resident controller is being repaired.
                self._protected_cloud_health_ready = False
                logger.warning(
                    "cloud overlay unhealthy (ENOTCONN) — healing thread=%s",
                    self.thread_id,
                )
                # Capture the exact claim-local controller before entering a
                # worker thread. Cleanup deliberately clears the session
                # attributes, but cancelling ``to_thread`` cannot stop an
                # already-running heal. The captured manager's retired
                # claim-resource admission is what prevents its later remote
                # steps from mutating a successor.
                cloud_mount_manager = self.cloud_mount_manager
                protected_mount_id = self._protected_mount_id
                if cloud_mount_manager is None or protected_mount_id is None:
                    logger.error(
                        "cloud overlay heal lacks its exact lower mount: thread=%s",
                        self.thread_id,
                    )
                    continue
                await asyncio.to_thread(
                    overlay.heal,
                    lambda: cloud_mount_manager.restart_mount(protected_mount_id),
                )
                if not await asyncio.to_thread(overlay.health_check):
                    raise WorkspaceUnavailableError(
                        "protected-cloud overlay remained unhealthy after heal"
                    )
                self._protected_cloud_health_ready = True
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._protected_cloud_health_ready = False
                logger.error("overlay heal failed (will retry next probe): %s", e)

    def reset_cloud_overlay(self) -> None:
        """Post-apply/reject reset: discard the staged upperdir and remount
        with a fresh workdir, then refresh the RO lower.

        Blocking (runs remote scripts over SSH) — the route/caller must run
        this via ``asyncio.to_thread``. Called by the orchestrator via
        ``POST /cloud-overlay/reset`` after a user applies or rejects a
        staged cloud diff (Task 10).
        """
        overlay = self.overlay_mount_manager
        if overlay is None or not overlay.active:
            raise CloudOverlayUnavailable("no active cloud overlay")
        self._protected_cloud_health_ready = False
        try:
            overlay.reset_upper(
                refresh_lower=lambda: self.cloud_mount_manager.refresh_vfs()
            )
            if not overlay.health_check():
                raise CloudOverlayUnavailable(
                    "cloud overlay remained unhealthy after reset"
                )
            self._protected_cloud_health_ready = True
        except Exception:
            self._protected_cloud_health_ready = False
            raise

    def _scope_skills_for_tool_names(self, tool_names: List[str]) -> None:
        """Apply capability-aware optional-skill scope without mutating a base config."""
        from shared.runtime.core.skill_resolution import (
            APP_GUIDE_SKILL,
            add_persistent_system_skills,
            scope_skills_for_tools,
        )

        skill_catalog = self.config.extra.get(
            "_unscoped_resolved_skills",
            self.config.extra.get("_resolved_skills", {}),
        )
        # Persistent product skills are available independently of the optional
        # DB skill catalog. Re-installing them here also replaces stale frozen
        # app-guide bytes with the current running bundle on session resume.
        # The scope call below still withholds each skill until its actual
        # loader/capability tools instantiate.
        skill_catalog = add_persistent_system_skills(skill_catalog)
        scoped = scope_skills_for_tools(skill_catalog, tool_names)
        next_extra = {
            **self.config.extra,
            "_unscoped_resolved_skills": skill_catalog,
            "_resolved_skills": scoped,
        }
        resolved_instructions = next_extra.get("_resolved_instructions")
        if (
            isinstance(resolved_instructions, dict)
            and APP_GUIDE_SKILL in resolved_instructions
        ):
            resolved_instructions = dict(resolved_instructions)
            resolved_instructions.pop(APP_GUIDE_SKILL, None)
            next_extra["_resolved_instructions"] = resolved_instructions

        # A pre-M1 frozen expert may have bound ``skill: app-guide`` as an
        # ordinary workspace instruction. Drop that stale delivery/enforcement
        # path so it cannot force the model to read mutable product guidance.
        instruction_files = []
        removed_reserved_binding = False
        for entry in getattr(self.config, "instruction_files", []) or []:
            skill_name = (
                entry.get("skill")
                if isinstance(entry, dict)
                else getattr(entry, "skill", None)
            )
            entry_path = (
                entry.get("file", "")
                if isinstance(entry, dict)
                else getattr(entry, "path", "")
            )
            if skill_name == APP_GUIDE_SKILL or str(entry_path).startswith(
                f"skills/{APP_GUIDE_SKILL}/"
            ):
                removed_reserved_binding = True
                continue
            instruction_files.append(entry)
        if removed_reserved_binding:
            logger.warning("Ignored reserved mutable app-guide instruction binding")

        if _dc_is_dataclass(self.config):
            self.config = _dc_replace(
                self.config,
                extra=next_extra,
                instruction_files=instruction_files,
            )
        else:  # Lightweight test/config adapters may not be real dataclasses.
            self.config.extra = next_extra
            self.config.instruction_files = instruction_files
        if self.tool_context is not None:
            # use_skill authorizes by the CURRENT scoped menu, not by stale
            # workspace bytes. Keep its long-lived ToolContext synchronized on
            # every backend/config rebind.
            self.tool_context.config["_resolved_skills"] = scoped

    def _deploy_catalog_skill_files(
        self, only_names: Optional[set[str]] = None
    ) -> None:
        """Materialize currently scoped model-invoked skill files.

        Ordinary catalog skills retain their historical add-only behavior.
        The capability-scoped Canvas companion is reconciled in both
        directions: files created by SRW are withdrawn when either required
        tool disappears, while modified files and unrelated files in the same
        directory are treated as user content and preserved. The managed
        app-guide is never materialized here: its dedicated tool reads the
        current digest-stamped runtime bundle, so stale workspace bytes are
        inert.
        """
        from shared.runtime.core.skill_resolution import (
            APP_GUIDE_SKILL,
            skill_files_to_workspace,
        )

        skills_files = self.config.extra.get("_resolved_skills", {}).get("files", {})
        if only_names is not None:
            skills_files = {
                name: files
                for name, files in skills_files.items()
                if name in only_names
            }
        ordinary_files = {
            name: files
            for name, files in skills_files.items()
            if name not in {_CANVAS_SKILL_NAME, APP_GUIDE_SKILL}
        }
        for ws_path, content in skill_files_to_workspace(ordinary_files).items():
            if self.workspace_manager.exists(ws_path):
                continue  # don't overwrite on session resume
            parent_dir = str(Path(ws_path).parent)
            if parent_dir and parent_dir != ".":
                self.workspace_manager.backend.mkdir(parent_dir)
            self.workspace_manager.write_file(ws_path, content)
            logger.debug(f"Deployed skill file to workspace: {ws_path}")

        if only_names is None or _CANVAS_SKILL_NAME in only_names:
            desired = skills_files.get(_CANVAS_SKILL_NAME)
            self._reconcile_canvas_skill_files(
                desired if isinstance(desired, dict) else {}
            )

    @staticmethod
    def _skill_file_digest(content: str) -> str:
        return hashlib.sha256(content.encode("utf-8")).hexdigest()

    def _load_canvas_skill_manifest(self, allowed_paths: set[str]) -> Dict[str, str]:
        """Load only a manifest carrying SRW's exact ownership marker."""

        self._canvas_skill_manifest_owned = False
        try:
            if not self.workspace_manager.exists(_CANVAS_SKILL_MANIFEST):
                return {}
            payload = json.loads(
                self.workspace_manager.read_file(_CANVAS_SKILL_MANIFEST)
            )
            if payload.get("managed_by") != _CANVAS_SKILL_MANIFEST_OWNER:
                return {}
            raw_files = payload.get("files")
            if not isinstance(raw_files, dict):
                return {}
            from shared.runtime.core.skill_format import validate_skill_path

            loaded: Dict[str, str] = {}
            for rel_path, digest in raw_files.items():
                validate_skill_path(rel_path)
                if not isinstance(digest, str) or len(digest) != 64:
                    return {}
                int(digest, 16)
                workspace_path = f"skills/{_CANVAS_SKILL_NAME}/{rel_path}"
                # The manifest is an ownership record, not authority to delete
                # arbitrary sibling files. Only currently resolved bundle paths
                # may ever be managed.
                if workspace_path in allowed_paths:
                    loaded[workspace_path] = digest
            self._canvas_skill_manifest_owned = True
            return loaded
        except Exception:
            # An unrecognized file at the reserved path may be user content.
            # Never overwrite or delete it without our ownership marker.
            logger.warning("Could not load the managed Canvas skill manifest")
            return {}

    def _store_canvas_skill_manifest(self, managed: Dict[str, str]) -> None:
        prefix = f"skills/{_CANVAS_SKILL_NAME}/"
        if not managed:
            if self._canvas_skill_manifest_owned and self.workspace_manager.exists(
                _CANVAS_SKILL_MANIFEST
            ):
                self.workspace_manager.delete_file(_CANVAS_SKILL_MANIFEST)
            self._canvas_skill_manifest_owned = False
            return

        if (
            self.workspace_manager.exists(_CANVAS_SKILL_MANIFEST)
            and not self._canvas_skill_manifest_owned
        ):
            # Preserve a pre-existing unowned marker-shaped path. In-memory
            # ownership still makes withdrawal safe for this runtime.
            return
        payload = {
            "managed_by": _CANVAS_SKILL_MANIFEST_OWNER,
            "files": {
                path.removeprefix(prefix): digest
                for path, digest in sorted(managed.items())
                if path.startswith(prefix)
            },
        }
        self.workspace_manager.backend.mkdir(prefix.rstrip("/"))
        self.workspace_manager.write_file(
            _CANVAS_SKILL_MANIFEST,
            json.dumps(payload, sort_keys=True, separators=(",", ":")),
        )
        self._canvas_skill_manifest_owned = True

    def _reconcile_canvas_skill_files(self, desired_files: Dict[str, str]) -> None:
        """Converge the one capability-scoped skill without deleting user work."""

        from shared.runtime.core.skill_format import validate_skill_path

        prefix = f"skills/{_CANVAS_SKILL_NAME}/"
        unscoped_files = (
            self.config.extra.get("_unscoped_resolved_skills", {})
            .get("files", {})
            .get(_CANVAS_SKILL_NAME, {})
        )
        allowed_paths: set[str] = set()
        if isinstance(unscoped_files, dict):
            for rel_path in unscoped_files:
                if not isinstance(rel_path, str):
                    continue
                try:
                    allowed_paths.add(f"{prefix}{validate_skill_path(rel_path)}")
                except ValueError:
                    logger.warning(
                        "Ignoring unsafe Canvas skill path in resolved catalog: %r",
                        rel_path,
                    )

        desired: Dict[str, str] = {}
        for rel_path, content in desired_files.items():
            if not isinstance(rel_path, str) or not isinstance(content, str):
                logger.warning("Ignoring malformed Canvas skill file entry")
                continue
            try:
                path = f"{prefix}{validate_skill_path(rel_path)}"
            except ValueError:
                logger.warning(
                    "Ignoring unsafe Canvas skill path in scoped catalog: %r",
                    rel_path,
                )
                continue
            desired[path] = content
            allowed_paths.add(path)

        managed = self._load_canvas_skill_manifest(allowed_paths)
        managed.update(
            {
                path: digest
                for path, digest in self._managed_canvas_skill_files.items()
                if path in allowed_paths
            }
        )

        # Withdraw obsolete/disabled files only when their current bytes still
        # match the hash SRW recorded when it wrote them. A modified file has
        # become user content and is deliberately released from management.
        for path, digest in list(managed.items()):
            if path in desired:
                continue
            try:
                if not self.workspace_manager.exists(path):
                    managed.pop(path, None)
                    continue
                unchanged = (
                    self._skill_file_digest(self.workspace_manager.read_file(path))
                    == digest
                )
                if not unchanged:
                    # Modified managed bytes have become user content.
                    managed.pop(path, None)
                    continue
                if self.workspace_manager.delete_file(path) is False:
                    logger.warning(
                        "Could not withdraw managed Canvas skill file: %s", path
                    )
                    continue
            except Exception:
                logger.warning("Could not withdraw managed Canvas skill file: %s", path)
                # Retain ownership after a transient workspace failure so a
                # later reconciliation can retry the safe digest-checked delete.
                continue
            managed.pop(path, None)

        # Never overwrite a pre-existing or user-modified file. An unchanged
        # SRW-owned file may be upgraded when bundled desired bytes change.
        for path, content in desired.items():
            expected = self._skill_file_digest(content)
            if self.workspace_manager.exists(path):
                try:
                    current = self._skill_file_digest(
                        self.workspace_manager.read_file(path)
                    )
                except Exception:
                    continue
                recorded = managed.get(path)
                if recorded != current:
                    managed.pop(path, None)
                    continue
                if current != expected:
                    self.workspace_manager.write_file(path, content)
                    managed[path] = expected
                    logger.debug("Upgraded managed Canvas skill file: %s", path)
                continue
            parent_dir = str(Path(path).parent)
            self.workspace_manager.backend.mkdir(parent_dir)
            self.workspace_manager.write_file(path, content)
            managed[path] = expected
            logger.debug("Deployed managed Canvas skill file: %s", path)

        self._managed_canvas_skill_files = managed
        self._store_canvas_skill_manifest(managed)

    def _deploy_instruction_files(self) -> None:
        """Deploy instruction files from config to workspace.

        Mirrors the worker-mode pattern in agent.py._deploy_instruction_files().
        Copies files like design_guide.md from the expert config directory into
        the workspace so the agent can read them via workspace tools.
        """
        # Workspace creation precedes ToolContext identity resolution and tool
        # instantiation. Fail closed here: deploy ordinary catalog skills now,
        # but admit present-with-canvas only after the actual loaded tool list is
        # known in _load_tools_for_backend().
        self._scope_skills_for_tool_names([])

        # Skill directories (Slice 2) — mirror of the agent.py worker path. Runs
        # before the instruction-files guard since skills come from the frozen
        # blob (_resolved_skills), not the expert config dir.
        self._deploy_catalog_skill_files()

        if not self.config.instruction_files:
            return

        # file_resolver is only needed for literal ``file:`` instruction entries
        # (loaded from the deployment/templates dir). Bound ``skill:`` entries come
        # from the frozen ``_resolved_instructions`` blob and need no deployment
        # dir — so DON'T gate the whole loop on ``_deployment_dir`` (which is None
        # for sessions), or bound skills silently fail to deploy and their
        # ``before_tool`` enforce gate bricks the tool (the cite-as-you-write case).
        file_resolver = None
        if self.config._deployment_dir:
            templates_dir = get_project_root() / "config" / "templates"
            file_resolver = FileResolver(
                deployment_dir=self.config._deployment_dir,
                framework_dir=templates_dir,
            )
        missing_bound_skills: list[str] = []
        for entry in self.config.instruction_files:
            try:
                if entry.skill:
                    # Bound skill: flag-independent instructions channel → skills/<skill>/SKILL.md.
                    # Backend-aware check: get_path().exists() tests the agent
                    # pod's local filesystem and is always False on remote
                    # workspaces, which would redeploy (clobber) on resume.
                    if self.workspace_manager.exists(entry.path):
                        continue  # don't overwrite on session resume
                    content = (
                        self.config.extra.get("_resolved_instructions", {}) or {}
                    ).get(entry.skill)
                    if not content:
                        logger.warning(
                            f"Bound skill content missing from blob: {entry.skill}"
                        )
                        if entry.skill not in missing_bound_skills:
                            missing_bound_skills.append(entry.skill)
                        continue
                    content = render_instruction_content(
                        content, [], origin=f"bound skill {entry.skill!r}"
                    )
                    parent_dir = str(Path(entry.path).parent)
                    if parent_dir and parent_dir != ".":
                        self.workspace_manager.backend.mkdir(parent_dir)
                    self.workspace_manager.write_file(entry.path, content)
                    logger.debug(f"Deployed bound skill to workspace: {entry.path}")
                    continue
                # Skip if already present (don't overwrite on session resume).
                # Backend-aware for the same reason as the skill branch above.
                if self.workspace_manager.exists(entry.file):
                    continue
                if file_resolver is None:
                    logger.warning(
                        f"Cannot deploy instruction file {entry.file}: "
                        "no deployment dir"
                    )
                    continue
                content = file_resolver.load(Path(entry.file).name)
                content = render_instruction_content(
                    content, [], origin=f"instruction file {entry.file!r}"
                )
                # Ensure parent directory exists
                parent_dir = str(Path(entry.file).parent)
                if parent_dir and parent_dir != ".":
                    self.workspace_manager.backend.mkdir(parent_dir)
                self.workspace_manager.write_file(entry.file, content)
                logger.debug(f"Deployed instruction file to workspace: {entry.file}")
            except FileNotFoundError:
                logger.warning(f"Instruction file not found: {entry.file}")
            except Exception as e:
                logger.warning(f"Failed to deploy instruction file {entry.file}: {e}")

        if missing_bound_skills:
            # Fail closed when a fresh session needs a bound skill the frozen
            # blob cannot supply. Otherwise the gate bricks the tool while
            # read_file can only report "not found" — an unbounded loop.
            # Retained files on resume keep their historical skip above.
            raise RuntimeError(
                "Bound skill content missing from frozen blob: "
                f"{sorted(missing_bound_skills)}. The session's mandatory "
                "instruction gate cannot be satisfied without it; failing "
                "closed rather than demanding an impossible read."
            )

    def _enforce_officer_knowledge_invariant(self) -> None:
        """Fail the attach when a background officer is mis-bound (K1, §3.1).

        The invariant for a commissioned background officer
        (``officer.enabled is True`` — the runtime fact, never agent_id):

        1. exactly one ``project_id``;
        2. exactly one writable KnowledgeBinding, native, and keyed to that
           project — the sole write target no request/config override can
           replace (``build_knowledge_bindings`` constructs externals
           read-only; this guard refuses anything that got past it);
        3. every other binding is read-only.

        Sessions without explicit bindings (legacy/direct construction) are
        judged on ``project_ids`` alone: the knowledge tools synthesize the
        same first-native-writable binding from them. Conferences
        (``officer.conference`` with enabled False) and ordinary sessions are
        untouched. This is a *binding shape* check only — a KB outage keeps
        bindings intact and must stay survivable (degraded), never an attach
        failure.
        """
        from agent.tools.registry import officer_ceiling_active

        if not officer_ceiling_active(getattr(self.config, "officer", None)):
            return

        project_ids = [str(p) for p in (self.project_ids or []) if p]
        if len(project_ids) != 1:
            raise OfficerKnowledgeBindingError(
                "Background officer attach refused: expected exactly one "
                f"project binding, got {len(project_ids)} "
                f"({project_ids or 'none'}). Commission an officer onto one "
                "project post (officer_knowledge_plane.md §3.1)."
            )

        from agent.services.knowledge.bindings import KnowledgeBinding

        bindings = [
            b
            for b in (self.knowledge_bindings or [])
            if isinstance(b, KnowledgeBinding)
        ]
        if not bindings:
            # No explicit bindings travelled with this construction path; the
            # single project id above synthesizes the sole native writable KB.
            return

        writable = [b for b in bindings if b.writable]
        if len(writable) != 1 or not writable[0].is_native:
            shape = [
                f"{b.alias}({b.kind},{'rw' if b.writable else 'ro'})" for b in bindings
            ]
            raise OfficerKnowledgeBindingError(
                "Background officer attach refused: expected exactly one "
                "native writable knowledge binding, got "
                f"[{', '.join(shape)}]. External knowledge bases must be "
                "read-only and no override may replace the project write "
                "target (officer_knowledge_plane.md §3.1)."
            )
        if str(writable[0].kb_id) != project_ids[0]:
            raise OfficerKnowledgeBindingError(
                "Background officer attach refused: the writable knowledge "
                f"binding targets KB {writable[0].kb_id}, not the officer's "
                f"project {project_ids[0]} — the write target cannot be "
                "replaced (officer_knowledge_plane.md §3.1)."
            )

    def _setup_knowledge(self, vector_conn: Optional[Any]) -> None:
        """Initialize the pgvector store and optional Neo4j Graph tier.

        Must be called BEFORE _setup_tools() so that the ToolContext
        has knowledge_graph and knowledge_store set, allowing knowledge
        tools to pass the has_knowledge() guard in load_tools().

        Mirrors the worker agent pattern in agent.py._setup_job_tools().
        """
        if not self.knowledge_bindings and not self.project_ids:
            return  # No native or selected external knowledge scope

        self._kb_degraded = False
        try:
            if vector_conn is None:
                raise RuntimeError("Vector database connection is unavailable")

            from shared.runtime.services.embedding_service import (
                get_kb_embedding_service,
            )
            from shared.runtime.services.knowledge_store import KnowledgeStore

            embedding_service = get_kb_embedding_service()
            self.knowledge_store = KnowledgeStore(
                db=vector_conn,
                embedding_service=embedding_service,
            )
            logger.info(
                "Knowledge store initialized for %d KB binding(s)",
                len(self.knowledge_bindings) or len(self.project_ids),
            )
        except Exception as e:
            self._kb_degraded = True
            from agent.core.archiver import audit_unavailable as _audit_unavailable

            _audit_unavailable(
                job_id=self.thread_id,
                agent_type=self.config.agent_id,
                step_type="kb_unavailable",
                component="KnowledgeStore",
                error=e,
                node_name="session_setup",
                extra={
                    "embedding_provider": os.environ.get(
                        "KB_EMBEDDING_PROVIDER",
                        os.environ.get("EMBEDDING_PROVIDER", "local"),
                    ),
                },
            )
            logger.warning(
                f"Failed to initialize knowledge store (non-fatal): {e} "
                f"[embedding_provider={os.environ.get('KB_EMBEDDING_PROVIDER', os.environ.get('EMBEDDING_PROVIDER', 'local'))}]"
            )

        if not self.project_ids:
            return

        try:
            from shared.runtime.services.knowledge_graph import KnowledgeGraphDB

            kg = KnowledgeGraphDB()
            if kg.connect():
                self._knowledge_graph = kg
                logger.info(
                    f"Knowledge Graph tier initialized for project(s) "
                    f"{self.project_ids}"
                )
            else:
                logger.warning("Failed to connect to Neo4j — Graph tier disabled")
        except Exception as e:
            # Neo4j is optional: do not mark vector search/read as degraded.
            logger.warning(f"Failed to initialize Neo4j Graph tier (non-fatal): {e}")

    def _setup_tools(self, postgres_conn: Optional[Any]) -> None:
        """Load tools from config, excluding phase-specific ones."""
        from agent.tools.registry import officer_ceiling_active

        tool_config = {
            **self.config.extra,
            # Background-officer runtime fact (officer_knowledge_plane.md §4):
            # lets tools that survive the capability ceiling trim object-plane
            # affordances from their OUTPUT too (get_current_project keeps
            # identity metadata but drops the cloud-folder link).
            "officer_session": officer_ceiling_active(
                getattr(self.config, "officer", None)
            ),
            "agent_id": self.config.agent_id,
            "multimodal": self.config.llm.multimodal,
            # Lets bulk readers cap a single tool result relative to the main
            # model's window (session_silent_failure_audit.md #5).
            "model_max_context_tokens": self.config.limits.model_max_context_tokens,
            # Per-family page-render DPI (None -> renderer default 150).
            "pdf_render_dpi": getattr(self.config.limits, "pdf_render_dpi", None),
            # Delegation tools read their settings from this plain dict —
            # "delegation" is a parsed/known config field, so it is NOT part
            # of config.extra (mirrors agent.py's worker tool_config).
            "delegation": _dc_asdict(self.config.delegation),
            # Built-in subagents (U1): roster-wide llm + resolved roster, a
            # parsed field like `delegation` (mirrors agent.py).
            "subagents": _dc_asdict(self.config.subagents),
            "tags": list(self.config.tags),
            "cloud_mount": {
                "active": bool(
                    self.cloud_mount_manager and self.cloud_mount_manager.active
                ),
                "root": "/cloud",
                "workspace_entry": "/workspace/cloud",
                "scan_guard": self.config.extra.get("cloud_scan_guard", "block"),
                "_manager": self.cloud_mount_manager,
                "protected": bool(
                    self.overlay_mount_manager and self.overlay_mount_manager.active
                ),
                "_overlay_manager": self.overlay_mount_manager,
            },
        }
        # Initialize session task manager
        from agent.managers.session_tasks import SessionTaskManager

        self.session_task_manager = SessionTaskManager(
            thread_id=self.thread_id,
            postgres=postgres_conn,
        )

        self.tool_context = ToolContext(
            workspace_manager=self.workspace_manager,
            todo_manager=None,  # No TodoManager in persistent mode
            postgres_db=postgres_conn,
            vector_db=self.vector_conn,  # Citations live in srw_vector
            verify_aux=self.citation_verify_aux,
            verify_citation_prompt=self.citation_verification_prompt,
            datasources=self.datasources,
            config=tool_config,
            _job_id=self.thread_id,
            _thread_id=self.thread_id,
            user_id=self.user_id,
            _llm_config=self.config.llm,
            _instruction_files=self.config.instruction_files,
            shell_manager=self.shell_manager,  # Set before tool loading
            session_task_manager=self.session_task_manager,
            knowledge_graph=self._knowledge_graph,
            knowledge_store=self.knowledge_store,
            knowledge_bindings=list(self.knowledge_bindings),
            runtime_actor=self.runtime_actor,
            orchestrator_client=self.orchestrator_client,
        )
        # Mark the parent before factories close over this context.  Even if a
        # malformed config exposes a delegation control without a runtime,
        # ensure_runtime must fail closed instead of constructing WorkerHost
        # from the session's legacy `_job_id = thread_id` alias.
        self.tool_context._subagent_parent_kind = "session"
        self.tool_context._session_parent_authority_provider = (
            self.session_parent_authority_provider
        )
        self.tool_context.provider_admission = self.subagent_provider_admission
        self.tool_context.auxiliary_llm = self.auxiliary_llm
        self.tool_context._limits = self.config.limits
        if self.project_ids:
            self.tool_context.project_ids = self.project_ids

        if postgres_conn is not None:

            async def _persist_cloud_anchor(
                workspace_path: str, anchor: Dict[str, Any]
            ) -> None:
                await postgres_conn.upsert_thread_cloud_anchor(
                    self.thread_id,
                    workspace_path,
                    anchor,
                )

            self.tool_context.cloud_anchor_persist_callback = _persist_cloud_anchor

        self._load_tools_for_backend()
        self._install_session_subagent_runtime()

    def _install_session_subagent_runtime(self) -> None:
        """Install the strict thread-parent host/ledger when tools require it."""

        context = self.tool_context
        if context is None:
            return
        runtime_tools = {
            "delegate_agent",
            "list_agents",
            "wait_agent",
            "message_agent",
            "stop_agent",
        }
        loaded = set(getattr(context, "_resolved_tool_names", None) or [])
        if context.subagent_runtime is not None:
            return
        delegation_enabled = bool(loaded.intersection(runtime_tools))
        authority_wired = not (
            self.orchestrator_client is None
            or self.postgres_conn is None
            or not callable(self.session_parent_authority_provider)
            or not callable(self.subagent_provider_admission)
            or not callable(self.subagent_effect_authority)
        )
        if not authority_wired and not delegation_enabled:
            return
        if not authority_wired:
            raise RuntimeError(
                "delegation-enabled session lacks exact durable parent authority"
            )

        # Install the hidden lifecycle runtime even when the current config no
        # longer exposes delegation controls. A prior session life may have
        # durable live children; config revocation must not strand those rows
        # by skipping attach-time orphan recovery. No model-facing tool is
        # added here, so a session without the grant still cannot delegate.

        from agent.subagents.host import SessionHost
        from agent.subagents.runtime import SubagentRuntime
        from agent.subagents.session_persistence import SessionSubagentLedger

        ledger = SessionSubagentLedger.from_context(context)
        if ledger is None:
            raise RuntimeError(
                "delegation-enabled session could not construct its durable ledger"
            )
        host = SessionHost(
            thread_id=self.thread_id,
            # Audit/metering treats this field as the execution tier.  The
            # expert config's agent_id may itself be a UUID, which would be
            # misclassified as a child job instead of this parent session.
            agent_type="persistent",
            tool_context=context,
            user_id=self.user_id,
            auxiliary_llm=self.auxiliary_llm,
            live_llm_config=self.config.llm,
            postgres=self.postgres_conn,
            admission_fn=self.subagent_provider_admission,
            effect_authority_fn=self.subagent_effect_authority,
            settlement_authority_fn=self.subagent_settlement_authority,
            event_fn=self.subagent_event_callback,
        )
        context._parent_host = host
        context.subagent_runtime = SubagentRuntime.from_context(
            context,
            host,
            ledger=ledger,
        )

    def _wire_subagent_context_probe(self) -> None:
        """Expose live parent headroom after ContextManager construction."""

        context = self.tool_context
        manager = self.context_manager
        if context is None or manager is None:
            return

        def _probe():
            from agent.subagents.host import ContextProbe

            live = manager.state
            config = manager.config
            return ContextProbe(
                last_provider_input_tokens=live.last_provider_input_tokens,
                current_token_count=int(live.current_token_count or 0),
                compaction_threshold_tokens=int(config.compaction_threshold_tokens),
                model_max_context_tokens=int(config.model_max_context_tokens),
            )

        context.parent_context_probe = _probe

    async def recover_subagents(self) -> None:
        """Reconcile predecessor child generations before parent readiness."""

        runtime = getattr(self.tool_context, "subagent_runtime", None)
        if runtime is None:
            return
        recover = getattr(runtime, "recover_orphans", None)
        if not callable(recover):
            raise RuntimeError("session subagent runtime has no orphan recovery")
        await recover()

    async def quiesce_subagents(self, reason: str) -> None:
        """Close child admission/work while this session still has authority."""

        if self._subagent_runtime_quiesced:
            return
        runtime = getattr(self.tool_context, "subagent_runtime", None)
        if runtime is not None:
            quiesce = getattr(runtime, "quiesce", None)
            if not callable(quiesce):
                raise RuntimeError("session subagent runtime has no quiesce boundary")
            await quiesce(reason)
        self._subagent_runtime_quiesced = True

    async def resume_subagents(self) -> None:
        """Re-arm a settled child runtime after exact retirement abort proof."""

        if not self._subagent_runtime_quiesced:
            return
        runtime = getattr(self.tool_context, "subagent_runtime", None)
        if runtime is not None:
            resume = getattr(runtime, "resume", None)
            if not callable(resume):
                raise RuntimeError("session subagent runtime has no resume boundary")
            await resume()
        self._subagent_runtime_quiesced = False

    async def _hydrate_durable_session_state(self) -> None:
        """Restore migration-0133 state before this claimant serves tools.

        The in-process objects are only claim-local views.  Failing setup is
        safer than silently starting with an empty task list or missing cloud
        provenance: both would make a healthy pod handoff look like data loss.
        """

        if self.session_task_manager is not None:
            await self.session_task_manager.hydrate()
        if self.postgres_conn is None or self.tool_context is None:
            return
        anchors = await self.postgres_conn.list_thread_cloud_anchors(self.thread_id)
        for workspace_path, anchor in anchors.items():
            self.tool_context.record_cloud_anchor(workspace_path, anchor)
        logger.info(
            "Restored cloud citation anchors: thread=%s anchors=%d",
            self.thread_id,
            len(anchors),
        )

    @staticmethod
    def _runtime_backend_id(backend: Any) -> str | None:
        """Map the active backend object onto SRW's public four-tier IDs."""

        if backend is None:
            return None
        supports_shell = bool(getattr(backend, "supports_shell", False))
        supports_files = bool(getattr(backend, "supports_file_tools", True))
        if supports_shell:
            return (
                "vm" if getattr(backend, "sudo_action", None) == "allow" else "sandbox"
            )
        return "virtual" if supports_files else "none"

    def _refresh_runtime_facts(
        self,
        loaded_tool_names: Optional[List[str]] = None,
    ) -> None:
        """Atomically publish one redacted live-session observation.

        All raw datasource and workspace objects stay on ``PersistentSession``.
        The model-facing capability tool receives only this immutable aggregate
        snapshot through ``ToolContext``.
        """

        context = self.tool_context
        if context is None:
            return

        if loaded_tool_names is None:
            loaded_tool_names = list(getattr(context, "_resolved_tool_names", []) or [])
        safe_tool_names = [
            name for name in loaded_tool_names if isinstance(name, str) and name
        ]

        datasource_types = tuple(
            sorted(
                {
                    str(item.get("type"))
                    for item in self.datasource_configs
                    if isinstance(item, dict) and item.get("type")
                }
            )
        )
        email_configs = [
            item
            for item in self.datasource_configs
            if isinstance(item, dict) and item.get("type") == "email"
        ]
        email_tier = None
        if email_configs:
            from agent.core.datasource_setup import (
                EMAIL_TIER_ORDER,
                email_effective_access,
            )

            email_tier = max(
                (email_effective_access(item) for item in email_configs),
                key=EMAIL_TIER_ORDER.index,
            )
            live_email_tier = getattr(
                self.datasources.get("email"),
                "access",
                None,
            )
            if live_email_tier in EMAIL_TIER_ORDER:
                email_tier = live_email_tier

        backend = getattr(self.workspace_manager, "backend", None)
        cloud_active = bool(
            self.cloud_mount_manager
            and getattr(self.cloud_mount_manager, "active", False)
        )
        protected_active = bool(
            cloud_active
            and self.overlay_mount_manager
            and getattr(self.overlay_mount_manager, "active", False)
        )
        supports_file_tools = bool(
            backend is not None and getattr(backend, "supports_file_tools", True)
        )

        backend_id = self._runtime_backend_id(backend)
        agent_provenance = component_provenance_from_environment(
            os.environ,
            ProductComponent.AGENT,
            include_common=True,
        )
        guide_provenance = unavailable_component_provenance()
        scoped_skills = self.config.extra.get("_resolved_skills", {})
        if isinstance(scoped_skills, dict):
            menu = scoped_skills.get("menu")
            files_by_skill = scoped_skills.get("files")
            guide_entry = (
                next(
                    (
                        item
                        for item in menu
                        if isinstance(item, dict)
                        and item.get("name") == APP_GUIDE_SKILL
                        and item.get("system_managed") is True
                        and item.get("loader_tool") == APP_GUIDE_LOADER_TOOL
                    ),
                    None,
                )
                if isinstance(menu, list)
                else None
            )
            guide_files = (
                files_by_skill.get(APP_GUIDE_SKILL)
                if isinstance(files_by_skill, dict)
                else None
            )
            if (
                isinstance(guide_entry, dict)
                and isinstance(guide_files, dict)
                and all(
                    isinstance(path, str) and isinstance(content, str)
                    for path, content in guide_files.items()
                )
            ):
                expected_digest = guide_entry.get("bundle_digest")
                actual_digest = skill_bundle_digest(guide_files)
                if expected_digest == actual_digest:
                    guide_provenance = inherited_content_provenance(
                        agent_provenance,
                        content_digest=f"sha256:{actual_digest}",
                    )

        workspace_provenance = unavailable_component_provenance()
        if backend_id in {"sandbox", "vm"}:
            workspace_provenance = component_provenance_from_environment(
                os.environ,
                ProductComponent.WORKSPACE,
            )

        try:
            facts = SessionRuntimeFacts(
                observed_at=datetime.now(timezone.utc),
                backend_id=backend_id,
                backend_supports_shell=bool(getattr(backend, "supports_shell", False)),
                backend_supports_file_tools=supports_file_tools,
                backend_supports_canvas_presentation=bool(
                    getattr(backend, "supports_canvas_presentation", False)
                ),
                backend_supports_canvas_live_apps=bool(
                    getattr(backend, "supports_canvas_live_apps", False)
                ),
                backend_supports_shared_browser=bool(
                    getattr(backend, "supports_canvas_shared_browser", False)
                ),
                attached_datasource_types=datasource_types,
                email_access_tier=email_tier,
                email_connection_failed=bool(
                    email_configs and self.datasources.get("email") is None
                ),
                email_direct_send_enabled=bool(
                    email_configs
                    and getattr(
                        self.datasources.get("email"),
                        "unattended_send",
                        False,
                    )
                ),
                knowledge_binding_available=bool(self.knowledge_bindings),
                knowledge_store_available=self.knowledge_store is not None,
                memory_available=self.recall_store is not None,
                cloud_mount_active=cloud_active,
                protected_cloud_active=protected_active,
                loaded_tool_names=tuple(safe_tool_names),
                runtime_component_provenance=(
                    (ProductComponent.AGENT, agent_provenance),
                    (ProductComponent.GUIDE, guide_provenance),
                    (ProductComponent.WORKSPACE, workspace_provenance),
                ),
            )
        except Exception as exc:
            logger.warning(
                "Could not publish redacted session runtime facts (%s)",
                type(exc).__name__,
            )
            context.session_runtime_facts = None
            return

        context.session_runtime_facts = facts

    def _load_tools_for_backend(self) -> None:
        """Resolve, filter, load, document, and post-process the toolset for
        the CURRENT workspace backend, setting ``self.tools``.

        Factored out of ``_setup_tools`` so ``resetup_tools_for_backend`` can
        re-derive the toolset after a live backend swap (e.g. ``virtual`` →
        ``sandbox``) WITHOUT rebuilding ``tool_context`` or resetting
        ``session_task_manager``. Reads only instance state already set by
        ``_setup_tools`` (``tool_context``, ``config``, ``workspace_manager``,
        ``cloud_mount_manager``), so it is safe to call again post-swap.
        """
        # Get all tool names and filter out phase-specific ones
        from agent.tools.registry import expand_tool_wildcards

        all_names = expand_tool_wildcards(get_all_tool_names(self.config))
        tool_names = [n for n in all_names if n not in _EXCLUDED_TOOLS]

        # Always include session task tools in persistent mode
        for name in ["task_add", "task_complete", "task_list"]:
            if name not in tool_names:
                tool_names.append(name)

        # The managed product guide is a persistent-session floor, independent
        # of expert/catalog feature flags and workspace tier.
        from shared.runtime.core.skill_resolution import (
            APP_GUIDE_LOADER_TOOL,
            app_guide_break_glass_disabled,
        )

        if app_guide_break_glass_disabled():
            # The operator escape hatch is fail-closed even when a frozen
            # expert explicitly requested the reader.
            tool_names = [name for name in tool_names if name != APP_GUIDE_LOADER_TOOL]
        elif APP_GUIDE_LOADER_TOOL not in tool_names:
            tool_names.append(APP_GUIDE_LOADER_TOOL)

        # Runtime introspection is a separate persistent-session floor. Its
        # canary gate is operator-owned and independent of both the managed
        # guide break-glass switch and every user-selectable tool group.
        from agent.tools.product_capabilities import (
            PRODUCT_CAPABILITIES_TOOL_NAME,
            product_capabilities_tool_enabled,
        )

        if product_capabilities_tool_enabled():
            if PRODUCT_CAPABILITIES_TOOL_NAME not in tool_names:
                tool_names.append(PRODUCT_CAPABILITIES_TOOL_NAME)
        else:
            tool_names = [
                name for name in tool_names if name != PRODUCT_CAPABILITIES_TOOL_NAME
            ]

        # Fleet Management is the UI-facing group for SRW control-plane tools.
        # Experts & Skills and Automations & Loops are separate groups keyed by
        # ``tools.agent_catalog`` and ``tools.workflows``. New resolved configs
        # carry explicit off-markers from their complete merged tool policy;
        # marker absence stays enabled only for legacy-session compatibility.
        from agent.tools.registry import get_tools_by_category, officer_ceiling_active
        from shared.orch_surface.jobs import caller_default_names

        fleet_management_enabled = _fleet_management_enabled(self.config)
        job_control_enabled = _job_control_enabled(self.config)
        job_inspection_enabled = _job_inspection_enabled(self.config)
        agent_catalog_enabled = _agent_catalog_enabled(self.config)
        workflows_enabled = _workflows_enabled(self.config)
        fleet_management_tools = set(get_tools_by_category("orchestrator"))
        fleet_management_tools.update(_FLEET_MANAGEMENT_CONTROL_TOOLS)
        job_control_tools = set(get_tools_by_category("job_control"))
        job_inspection_tools = set(get_tools_by_category("job_inspection"))
        agent_catalog_tools = set(get_tools_by_category("agent_catalog"))
        workflow_tools = set(get_tools_by_category("workflows"))
        canvas_tools = set(get_tools_by_category("canvas"))

        if not fleet_management_enabled:
            tool_names = [
                name for name in tool_names if name not in fleet_management_tools
            ]
        else:
            _ORCHESTRATOR_TOOLS = [
                "get_session_context",
                "get_current_project",
                "list_project_jobs",
                "list_project_repositories",
                "get_default_project_repository",
            ]
            for name in _ORCHESTRATOR_TOOLS:
                if name not in tool_names:
                    tool_names.append(name)

        # officer_supervision_surface E2: a commissioned background officer
        # resolves its job-tool defaults on the OFFICER lane — the generated
        # observability/evidence grant — instead of the interactive-session
        # subset. Same strict `is True` fact that stamps officer_session.
        job_tool_lane = (
            "officer"
            if officer_ceiling_active(getattr(self.config, "officer", None))
            else "session"
        )
        if not job_control_enabled:
            tool_names = [name for name in tool_names if name not in job_control_tools]
        else:
            for name in sorted(caller_default_names(job_tool_lane, "job_control")):
                if name not in tool_names:
                    tool_names.append(name)
        if not job_inspection_enabled:
            tool_names = [
                name for name in tool_names if name not in job_inspection_tools
            ]
        else:
            for name in sorted(caller_default_names(job_tool_lane, "job_inspection")):
                if name not in tool_names:
                    tool_names.append(name)

        if not agent_catalog_enabled:
            tool_names = [
                name for name in tool_names if name not in agent_catalog_tools
            ]
        else:
            _AGENT_CATALOG_TOOLS = [
                "list_experts",
                "get_expert",
                "list_skills",
                "search_skills",
                "get_skill",
            ]
            for name in _AGENT_CATALOG_TOOLS:
                if name not in tool_names:
                    tool_names.append(name)

        if not workflows_enabled:
            tool_names = [name for name in tool_names if name not in workflow_tools]
        else:
            _WORKFLOW_DEFAULT_TOOLS = [
                "list_automations",
                "get_automation",
                "list_automation_runs",
                "propose_automation",
                "get_project_loop",
                "list_project_loop_jobs",
                "explain_project_loop",
            ]
            for name in _WORKFLOW_DEFAULT_TOOLS:
                if name not in tool_names:
                    tool_names.append(name)

        if not _canvas_enabled(self.config):
            tool_names = [name for name in tool_names if name not in canvas_tools]

        if self.cloud_mount_manager and self.cloud_mount_manager.active:
            if "srw_cloud_status" not in tool_names:
                tool_names.append("srw_cloud_status")

        # Lite-tier only: expose the agent-initiated upgrade request
        # (workspace_tier_upgrade.md §4.2 S5) so a no-shell session can ASK for a
        # real sandbox. Gated on the backend lacking a shell — after a virtual →
        # sandbox swap this re-derives against the now-shell-capable backend and
        # the tool drops out (nothing left to upgrade to).
        _backend = getattr(self.workspace_manager, "backend", None)
        if (
            fleet_management_enabled
            and _backend is not None
            and getattr(_backend, "supports_shell", False)
        ):
            if "checkout_project_repository" not in tool_names:
                tool_names.append("checkout_project_repository")
        if (
            fleet_management_enabled
            and _backend is not None
            and not getattr(_backend, "supports_shell", False)
        ):
            if "request_workspace_upgrade" not in tool_names:
                tool_names.append("request_workspace_upgrade")

        # Officer (centurion) sessions get the sleep tool — their park verb —
        # and notify_user, their communication contract. Gated on config, not
        # backend (knowledge-base/knowledge/features/centurion.md §4/§6). Strict `is True` so
        # MagicMock configs in tests can't enable them.
        if getattr(getattr(self.config, "officer", None), "enabled", False) is True:
            for officer_tool in ("sleep", "notify_user"):
                if officer_tool not in tool_names:
                    tool_names.append(officer_tool)

        # Capability gate: drop tools the workspace backend can't support (lite
        # tiers — no_workspace_agent_mode.md §3.2/§7). Mirrors the worker path.
        # On a backend swap this RE-FILTERS against the new backend, so an
        # upgrade (virtual → sandbox) re-admits shell/git/file tools the lite
        # tier had stripped.
        from agent.tools.registry import filter_tools_by_backend

        tool_names = filter_tools_by_backend(
            tool_names, getattr(self.workspace_manager, "backend", None)
        )

        # Background-officer capability ceiling
        # (knowledge-base/knowledge/features/officer_knowledge_plane.md §4, K3): a commissioned
        # background officer (officer.enabled is True — the runtime fact, not
        # agent_id) never sees object-plane tools, no matter what the config
        # override or the backend filter admitted. Applied LAST so the runtime
        # appends above (request_workspace_upgrade, checkout_project_repository,
        # srw_cloud_status, repository discovery) and any override-granted
        # shell/file/git/browser/canvas/repo/kb_export names are all subject to
        # it. Conferences (officer.conference with enabled False) are ordinary
        # sessions and pass through unchanged.
        from agent.tools.registry import (
            apply_officer_tool_ceiling,
            officer_ceiling_active,
        )

        _officer_session_active = officer_ceiling_active(
            getattr(self.config, "officer", None)
        )
        tool_names = apply_officer_tool_ceiling(
            tool_names, getattr(self.config, "officer", None)
        )

        try:
            self.tools = load_tools(tool_names, self.tool_context)
        except ValueError as e:
            logger.warning(f"Tool loading warning: {e}")
            # Load only implemented tools individually
            self.tools = []
            for name in tool_names:
                try:
                    self.tools.extend(load_tools([name], self.tool_context))
                except ValueError:
                    logger.debug(f"Tool not implemented: {name}")

        # A name survives the bind only if it is BOTH configured and produced by
        # its category factory. When those two disagree — e.g. the name list
        # resolves `shell.mode` stateless while the factory built the persistent
        # tools — the intersection can empty a whole category with nothing in the
        # log. `tool_names` is already backend-filtered above, so a miss here is
        # a genuine anomaly rather than the capability gate doing its job.
        # See knowledge-base/knowledge/issues/live_config_update_buries_extra_and_empties_the_shell_group.md.
        unbound = [
            name
            for name in dict.fromkeys(tool_names)
            if name not in {getattr(t, "name", None) for t in self.tools or []}
        ]
        if unbound:
            logger.warning(
                "%d configured tool(s) did not bind: %s",
                len(unbound),
                ", ".join(unbound),
            )

        if not self.tools:
            # Floor rule (live_session_settings.md Slice B, provider research):
            # never rebind to an EMPTY toolset when history may contain tool
            # calls — proxies 400, Anthropic degrades to empty responses,
            # strict chat templates crash. Structurally the session task tools
            # are always appended above, so this only fires if every candidate
            # failed to instantiate — keep a minimal built-in belt anyway.
            logger.warning(
                "Toolset resolved empty — binding minimal session task tools "
                "(never-bind-zero floor)"
            )
            self.tools = load_tools(
                ["task_add", "task_complete", "task_list"], self.tool_context
            )

        # Degraded knowledge availability (officer_knowledge_plane.md §3.1,
        # K1): when a background officer's granted KB tools could not bind
        # because the knowledge store is unavailable (vector/KB outage), bind
        # fail-closed stand-ins instead of silently shrinking the grant. The
        # officer keeps supervising and paging; every KB call answers with a
        # clear `project knowledge unavailable` error. Guarded on a REAL
        # missing store (`has_knowledge()` strictly False) so mocked contexts
        # in tests never grow stub tools.
        if _officer_session_active and self.tool_context is not None:
            _has_knowledge = getattr(self.tool_context, "has_knowledge", None)
            if callable(_has_knowledge) and _has_knowledge() is False:
                _knowledge_names = set(get_tools_by_category("knowledge"))
                _bound = {getattr(t, "name", None) for t in self.tools or []}
                _missing_kb = [
                    n
                    for n in dict.fromkeys(tool_names)
                    if n in _knowledge_names and n not in _bound
                ]
                if _missing_kb:
                    from agent.tools.knowledge.knowledge_tools import (
                        create_degraded_knowledge_tools,
                    )

                    self.tools.extend(create_degraded_knowledge_tools(_missing_kb))
                    logger.error(
                        "Background officer session: project knowledge "
                        "unavailable — bound %d fail-closed KB tool(s): %s",
                        len(_missing_kb),
                        _missing_kb,
                    )

        # The model-facing skill menu follows tools that actually instantiated,
        # not merely configured candidates. This fails closed if a Canvas
        # adapter or persistent identity was unavailable during registration.
        loaded_tool_names = [tool.name for tool in self.tools]
        self._scope_skills_for_tool_names(loaded_tool_names)
        # This is the first point at which present-with-canvas may touch the
        # workspace. Re-running after a none→workspace upgrade is idempotent and
        # admits it only when both use_skill and set_canvas actually loaded.
        self._deploy_catalog_skill_files({"present-with-canvas"})

        # Tool docs are virtual (knowledge-base/knowledge/features/virtual_directories.md).
        from agent.core.virtual_dirs import ToolsProvider, sweep_legacy_tools_dir

        # Pre-override objects — see the CRITICAL note in the agent.py step:
        # `self.tools = apply_description_overrides(self.tools)` below rebinds
        # the attribute to short-description copies.
        self._full_description_tools = self.tools
        self.workspace_manager.register_virtual_provider(
            ToolsProvider(lambda: self._full_description_tools)
        )
        if self.workspace_manager.virtual_overlay is not None:
            sweep_legacy_tools_dir(self.workspace_manager.virtual_overlay.inner)

        # contacts/ is virtual and project-scoped (knowledge-history/done/contacts_registry.md).
        # Only registered when the session has a project — without one,
        # `contacts/` is never reserved and the path falls through to the real
        # filesystem. `os` is already imported at module level (line 14) — reuse
        # it rather than shadowing it with a local import (ruff F823 risk).
        import httpx

        from agent.core.virtual_dirs import ContactsProvider

        orchestrator_url = os.getenv("ORCHESTRATOR_URL", "").rstrip("/")
        thread_id = self.thread_id
        if orchestrator_url and thread_id and self.project_id:

            def _fetch_contacts():
                response = httpx.get(
                    f"{orchestrator_url}/api/contacts/internal/list",
                    params={"thread_id": thread_id},
                    headers={"X-Internal-Key": os.getenv("MCP_INTERNAL_KEY", "")},
                    timeout=3.0,
                )
                response.raise_for_status()
                return response.json().get("contacts", [])

            self.workspace_manager.register_virtual_provider(
                ContactsProvider(_fetch_contacts)
            )

        # Apply description overrides and enforcement
        self.tools = apply_description_overrides(self.tools)
        self.tools = apply_instruction_enforcement(self.tools, self.tool_context)
        final_tool_names = [
            tool.name
            for tool in self.tools
            if isinstance(getattr(tool, "name", None), str)
        ]
        self.tool_context._resolved_tool_names = list(final_tool_names)
        self._refresh_runtime_facts(final_tool_names)

        logger.info(f"Loaded {len(self.tools)} tools for persistent session")

    def resetup_tools_for_backend(self) -> None:
        """Re-derive + rebind the toolset after a live backend swap.

        ``swap_backend`` rebuilds the ShellManager (and, when the new backend
        supports a shell, repoints ``tool_context.shell_manager``) but leaves
        ``self.tools`` / ``self.llm_with_tools`` bound to the OLD backend's
        (lite-filtered) toolset. After a ``virtual`` → ``sandbox`` upgrade the
        new backend supports a shell + file tools, so shell, git, and file
        tools must be re-derived; this recomputes the tool list against the new
        backend and rebinds the LLM — without touching ``session_task_manager``
        or rebuilding ``tool_context`` the way a full ``_setup_tools`` would
        (that would drop in-flight session state).

        The per-turn ``get_current_tools()`` re-read in ``persistent_graph``
        then exposes the new tools on the next turn with no further plumbing.
        Safe to call only after ``_setup_tools`` has run once.
        """
        if not self.tool_context:
            logger.warning(
                "resetup_tools_for_backend called before tool setup — skipping"
            )
            return
        # swap_backend rebuilds self.shell_manager but only repoints
        # tool_context.shell_manager when the NEW backend supports a shell.
        # Repoint unconditionally so a swap to a no-shell backend (downgrade /
        # kill-switch) clears the stale manager too.
        self.tool_context.shell_manager = self.shell_manager
        # The WorkspaceManager object is unchanged by swap_backend (only its
        # ._backend flips), so tool_context.workspace_manager stays valid.
        self._load_tools_for_backend()
        self._bind_tools()
        if self.system_prompt:
            # Capability-scoped skill menus are embedded in the system prompt.
            # Rebuild it so a none→workspace upgrade that admits Canvas also
            # makes present-with-canvas discoverable on the next turn.
            self.system_prompt = get_phase_system_prompt(
                self.config,
                is_strategic=False,
                model=self.config.llm.model or "",
                tool_names=[tool.name for tool in self.tools],
                prompt_type="interactive",
            )
        backend_name = type(getattr(self.workspace_manager, "backend", None)).__name__
        logger.info(
            f"Re-derived {len(self.tools)} tools after backend swap ({backend_name})"
        )

    async def resetup_datasources(
        self, new_datasources: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Apply a live datasource selection change (live_session_settings.md
        Slice B).

        Rebuilds the type-keyed connection registry from the NEW full payload
        through the same path attach takes (``process_datasources`` sorts
        read-only first, so a read-write entry wins a mixed same-type slot and
        multi-same-type stays correct by construction), applies the derived
        tool categories directly to ``config.tools`` (the validated session
        tools override's closed vocabulary silently drops sql/graph/mongodb/
        webdav, so they must never ride ``config.update``), clones added
        repositories, rewrites the README.md workspace-facts block, and re-derives +
        rebinds the toolset — which also rebuilds the system prompt for the
        per-turn ``messages[0]`` refresh (P0.1).

        The REPLACED connections are NOT closed here because a tool call may
        already be using one. Newly invoked email tools detect that their
        captured connection is no longer the current shared binding and fail
        closed; calls already inside an external operation retain their
        resource until they unwind. Replaced resources are returned under
        ``stale_connections`` / ``stale_clients`` and the caller closes them
        once no turn is executing.

        kb-type datasources are out of scope for live changes (v1): their
        knowledge bindings wire into memory/KB machinery that ToolContext
        holds a copy of. Existing kb entries pass through untouched; a changed
        kb selection takes effect on the next attach.

        Repository removals keep their clone + SSH key on the workspace
        (cheap honesty — scrubbing is not a security boundary) but drop the
        ``source_repos`` registration. A live repository ADD whose clone name
        collides with an existing clone fails that one clone with a warning
        (the existing clone is never touched); a resume re-resolves suffixed
        names over the full list.

        Args:
            new_datasources: Full datasource payload for the thread, as
                returned by ``GET /api/agents/threads/{id}/workspace``.

        Returns:
            Summary dict: ``added``/``removed`` display names (transcript
            stamp), ``stale_connections``/``stale_clients`` (caller's
            deferred close), ``kb_deferred`` when a kb change was skipped.
        """
        from agent.core.datasource_setup import (
            clone_repository_datasources,
            datasource_tool_categories,
            inject_workspace_facts,
            install_workspace_credentials,
            process_datasources,
            resolve_repo_clone_names,
        )

        if not self.tool_context:
            logger.warning("resetup_datasources called before tool setup — skipping")
            return {
                "added": [],
                "removed": [],
                "stale_connections": {},
                "stale_clients": {},
            }

        new_configs = list(new_datasources or [])
        old_configs = list(self.datasource_configs or [])
        await asyncio.to_thread(
            install_workspace_credentials, new_configs, self.workspace_manager
        )

        # The internal payload strips datasource ids, so identity for the
        # add/remove summary is (type, name) — unique enough for display and
        # for repository clone bookkeeping (clone names derive from names).
        def _key(ds: Dict[str, Any]) -> str:
            return f"{ds.get('type')}:{ds.get('name')}"

        old_keys = {_key(ds) for ds in old_configs}
        new_keys = {_key(ds) for ds in new_configs}
        added = [ds for ds in new_configs if _key(ds) not in old_keys]
        removed = [ds for ds in old_configs if _key(ds) not in new_keys]

        kb_changed = any(ds.get("type") == "kb" for ds in added + removed)
        if kb_changed:
            logger.warning(
                "kb-type datasource selection changed live — knowledge "
                "bindings apply on the next attach, not mid-session"
            )

        non_repo = [
            ds for ds in new_configs if ds.get("type") not in ("repository", "kb")
        ]
        new_conns, new_clients, cli_ds_types = process_datasources(non_repo)

        from agent.tools.registry import register_mcp_tools

        mcp_manager = new_conns.get("mcp")
        if mcp_manager is not None:
            try:
                await mcp_manager.connect_all()
            except Exception as e:
                logger.warning(
                    "Unexpected live MCP discovery failure (%s); continuing",
                    type(e).__name__,
                )
            mcp_manager.annotate_configs()
        register_mcp_tools(mcp_manager)

        stale_connections = dict(self.datasources)
        stale_clients = dict(self._datasource_clients)
        # ToolContext shares this dict by REFERENCE — mutate in place, never
        # rebind, or live tools keep reading the orphaned old registry.
        self.datasources.clear()
        self.datasources.update(new_conns)
        self._datasource_clients = new_clients

        for category, names in datasource_tool_categories(new_configs).items():
            setattr(self.config.tools, category, list(names))
        self.config.extra["_cli_datasources"] = cli_ds_types

        added_repos = [ds for ds in added if ds.get("type") == "repository"]
        removed_repos = {_key(ds) for ds in removed if ds.get("type") == "repository"}
        if added_repos and self.workspace_manager:
            try:
                clone_repository_datasources(added_repos, self.workspace_manager)
            except Exception as e:
                logger.warning("Live repository clone failed: %s", e)
        if removed_repos and self.workspace_manager:
            # Resolve clone names over the OLD full repo list (payload order)
            # so collision suffixes match what attach actually registered.
            old_repos = [ds for ds in old_configs if ds.get("type") == "repository"]
            for ds, clone_name in zip(old_repos, resolve_repo_clone_names(old_repos)):
                if _key(ds) in removed_repos:
                    self.workspace_manager.source_repos.pop(clone_name, None)
                    # source_repo_meta holds the repository's plaintext token;
                    # leaving it behind keeps a detached credential live on the
                    # workspace manager for the rest of the session.
                    self.workspace_manager.source_repo_meta.pop(clone_name, None)

        self.datasource_configs = new_configs
        self._refresh_runtime_facts()

        if self.workspace_manager:
            # inject_workspace_facts replaces the marked README.md block, so
            # connection names stay truthful for the next turn — including the
            # explicit "no connectors" state after a remove-all.
            try:
                inject_workspace_facts(
                    new_configs,
                    self.workspace_manager,
                    expert=getattr(self.config, "display_name", None),
                )
            except Exception as e:
                logger.warning("Failed to rewrite workspace facts: %s", e)

        self.resetup_tools_for_backend()

        summary: Dict[str, Any] = {
            "added": [ds.get("name", "unnamed") for ds in added],
            "removed": [ds.get("name", "unnamed") for ds in removed],
            "stale_connections": stale_connections,
            "stale_clients": stale_clients,
        }
        if kb_changed:
            summary["kb_deferred"] = True
        logger.info(
            "Datasources re-set up live: %d attached (%d added, %d removed), "
            "%d connections",
            len(new_configs),
            len(added),
            len(removed),
            len(new_conns),
        )
        return summary

    def _bind_tools(self) -> None:
        """Bind tools to LLM."""
        if not self._llm or not self.tools:
            return

        bind_kwargs = {}
        if supports_parallel_tool_calls(
            self.config.llm.provider, self.config.llm.model
        ):
            bind_kwargs["parallel_tool_calls"] = self.config.llm.parallel_tool_calls

        from shared.runtime.services.guardrails import apply_guardrails_to_tools

        bound_tools = apply_guardrails_to_tools(self.tools, model=self.config.llm.model)
        self.llm_with_tools = self._llm.bind_tools(bound_tools, **bind_kwargs)

    # --- Durable workspace undo ---

    async def undo_turn(
        self,
        turn_id: Optional[int] = None,
        *,
        control_request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Restore the preceding Git/turn-ledger workspace checkpoint.

        ``turn_id`` remains accepted for the pinned WebSocket contract, but the
        durable operation is intentionally the latest completed-turn undo; the
        old process-local per-file checkpoint map could not survive handoff and
        has been removed. Stateless callers supply the durable control UUID so
        a successor can recover an effect committed before journal ack.
        """

        from uuid import uuid4

        from agent.services.workspace_undo import apply_workspace_undo

        if turn_id is not None:
            logger.debug(
                "Workspace undo uses the latest durable turn checkpoint; "
                "legacy turn_id=%s is advisory",
                turn_id,
            )
        request_id = control_request_id or str(uuid4())
        result = await apply_workspace_undo(
            thread_id=self.thread_id,
            request_id=request_id,
            postgres=self.postgres_conn,
            workspace_manager=self.workspace_manager,
        )
        return result.event_params()

    def _build_context_config(self, config: Optional[Any] = None) -> ContextConfig:
        """Derive the ContextConfig from a config's limits (default: current).

        Shared by initial construction, hot-swap refresh, and the model-swap
        fit ladder (which derives the *candidate* config's thresholds before
        the swap is applied) so all agree on how thresholds derive from
        ``config.limits``.
        """
        cfg = config if config is not None else self.config
        ctx = cfg.context_management
        lim = cfg.limits
        return ContextConfig(
            compaction_threshold_tokens=lim.context_threshold_tokens,
            summarization_threshold_tokens=lim.context_threshold_tokens,
            message_count_threshold=lim.message_count_threshold,
            message_count_min_tokens=lim.message_count_min_tokens,
            keep_recent_tool_results=ctx.keep_recent_tool_results,
            keep_recent_messages=ctx.keep_recent_messages,
            keep_window_max_tool_result_chars=ctx.keep_window_max_tool_result_chars,
            # Safety-layer constant (model-aware; see loader fractions).
            # Summarization budgets are computed at call time from the
            # aux model's window (src/core/summarizer.py).
            model_max_context_tokens=lim.model_max_context_tokens,
            # Per-family image-token estimator (matrix settings.image_tokens).
            image_tokens=lim.image_tokens,
        )

    def _setup_context_manager(self) -> None:
        """Create context manager for token counting and compaction.

        Mirrors the worker path (``graph.py::build_phase_alternation_graph``):
        the token thresholds come from the model-aware values the loader derives
        as fractions of ``model_max_context_tokens`` (``config.limits.*``), NOT
        the ``ContextConfig`` defaults. Without this a 1M-context session would
        compact at the 80k fallback regardless of its real window. The
        ``LimitsConfig`` defaults equal the ``ContextConfig`` defaults, so this
        is a no-op when the derivation didn't fire (no regression).
        """
        self.context_manager = ContextManager(
            config=self._build_context_config(),
            model=self.config.llm.model or "gpt-4",
            summarization_call_timeout=(
                self.config.auxiliary.summarization_call_timeout
            ),
            # The loop adopts a compaction wholesale (no reducer): the kept
            # window must keep its ids and turn stamps, or the turn-end
            # reconcile cannot find this turn's rows after a mid-turn summary.
            preserve_message_identity=True,
        )

    def refresh_context_limits(self) -> None:
        """Re-derive context thresholds after a config/model hot-swap.

        Updates the EXISTING ContextManager in place (``update_limits``) so the
        running loop's captured reference stays valid and the provider-usage
        anchor survives — a downswitch to a smaller-window model then compacts
        on the next turn instead of dead-ending in empty responses. See
        knowledge-history/done/session_model_switch_stale_context_manager_empty_response.md.
        """
        if getattr(self, "context_manager", None) is None:
            return
        self.context_manager.update_limits(
            self._build_context_config(),
            self.config.llm.model or "gpt-4",
        )

    def _setup_shell_manager(self) -> None:
        """Initialize the shell manager over a shell-capable workspace backend.

        Shells run only on the workspace — there is no local (in-pod) tmux
        fallback. Without a shell-capable backend, shell tools stay disabled.
        """
        ws_backend = self.workspace_manager.backend if self.workspace_manager else None

        if not getattr(ws_backend, "supports_shell", False):
            logger.info(
                "Workspace backend does not support shell — shell tools "
                "disabled (no local fallback)"
            )
            return

        try:
            from agent.tools.shell.shell_manager import ShellManager

            shell_config = self.config.extra.get("shell", {})
            self.shell_manager = ShellManager(
                job_id=self.thread_id,
                max_tabs=shell_config.get("max_tabs", 15),
                scrollback_limit=shell_config.get("scrollback_limit", 5000),
                default_timeout=shell_config.get("default_timeout", 120),
                blocked_commands=shell_config.get("blocked_commands"),
                sandbox_cwd=str(self.workspace_manager.path)
                if shell_config.get("sandbox", True)
                else None,
                backend=ws_backend,
                sudo_action=shell_config.get("sudo_action", "freeze"),
            )
            if self.tool_context:
                self.tool_context.shell_manager = self.shell_manager
            logger.info("ShellManager initialized (backend delegation)")
        except Exception as e:
            logger.warning(f"Failed to initialize ShellManager (non-fatal): {e}")

    def set_shell_owner_token(self, lease_token: Optional[int]) -> None:
        """Fence remote-shell mutations to the current stateless claim."""
        if not self.workspace_manager:
            return
        from agent.core.virtual_dirs import unwrap_backend

        backend = unwrap_backend(self.workspace_manager.backend)
        setter = getattr(backend, "set_shell_owner_token", None)
        if setter is not None:
            setter(lease_token)

    @property
    def stateless_warm_reuse_safe(self) -> bool:
        """Whether this Python session may span stateless queue leases.

        A shell-capable backend carries lease-fenced mutable state and may
        still be referenced by cancelled sync work.  Its owner token must
        therefore never be repointed in place for a later claim; lite
        backends have no such agent-local physical-shell state.
        """
        backend = self._unwrapped_backend()
        return backend is not None and not bool(
            getattr(backend, "supports_shell", False)
        )

    def retire_shell_owner(self) -> None:
        """Close this session's local admission to its remote shell."""
        if not self.workspace_manager:
            return
        from agent.core.virtual_dirs import unwrap_backend

        backend = unwrap_backend(self.workspace_manager.backend)
        retire = getattr(backend, "retire_shell_owner", None)
        if retire is not None:
            retire()

    def _setup_memory(
        self,
        postgres_conn: Optional[Any],
        vector_conn: Optional[Any],
    ) -> None:
        """Initialize RecallStore and KnowledgeStore if enabled.

        Raises MemoryUnavailableError when a *configured* (required) memory
        component can't be set up — a store that won't init or a plugin whose
        transport won't resolve. "Configured ⇒ required": if the manager
        pipeline is on (or memory.required is set) the session must fail loud
        rather than run half-working. See
        knowledge-base/knowledge/issues/openrouter_auxiliary_crashes_session_via_memory_reranker.md.
        """
        if not vector_conn:
            return

        # Degradation flags — mirror the worker path (src/agent.py). A store
        # that fails to init flips its flag; the required-gate below turns that
        # into a loud MemoryUnavailableError instead of a silent half-session.
        self._memory_degraded = False
        self._kb_degraded = False

        # RecallStore (memory injection/extraction)
        if self.config.memory.enabled:
            try:
                from shared.runtime.services.embedding_service import (
                    get_embedding_service,
                )
                from shared.runtime.services.recall_store import RecallStore
                import uuid as _uuid

                embedding_service = get_embedding_service()
                self.recall_store = RecallStore(
                    db=vector_conn,
                    embedding_service=embedding_service,
                    job_id=_uuid.UUID(self.thread_id),
                    config=self.config.memory,
                    agent_id=self.config.agent_id,
                    project_id=_uuid.UUID(self.project_ids[0])
                    if self.project_ids
                    else None,
                    project_ids=[_uuid.UUID(p) for p in self.project_ids]
                    if self.project_ids
                    else None,
                )
                if self.tool_context:
                    self.tool_context.recall_store = self.recall_store
                logger.info("RecallStore initialized for persistent session")

                # B4 guard: background-probe the endpoint's dimensionality so
                # a misconfigured provider surfaces as one ERROR at init
                # instead of a swallowed WARNING per write.
                asyncio.create_task(embedding_service.verify_dimensions())
            except Exception as e:
                from agent.core.archiver import audit_unavailable as _audit_unavailable

                _provider = os.environ.get("EMBEDDING_PROVIDER", "local")
                _audit_unavailable(
                    job_id=self.thread_id,
                    agent_type=self.config.agent_id,
                    step_type="memory_unavailable",
                    component="RecallStore",
                    error=e,
                    node_name="session_setup",
                    extra={
                        "embedding_provider": _provider,
                        "embedding_model": os.environ.get("EMBEDDING_MODEL", "unknown"),
                    },
                )
                logger.warning(
                    f"Failed to initialize RecallStore (non-fatal): {e} "
                    f"[embedding_provider={_provider}]"
                )
                self._memory_degraded = True

        # KnowledgeStore (knowledge injection, project-scoped)
        # Skip if already initialized by _setup_knowledge() (for tool loading)
        if self.knowledge_store is None:
            try:
                from shared.runtime.services.embedding_service import (
                    get_kb_embedding_service,
                )
                from shared.runtime.services.knowledge_store import KnowledgeStore

                embedding_service = get_kb_embedding_service()
                self.knowledge_store = KnowledgeStore(
                    db=vector_conn,
                    embedding_service=embedding_service,
                )
                logger.info("KnowledgeStore initialized for persistent session")
            except Exception as e:
                from agent.core.archiver import audit_unavailable as _audit_unavailable

                _audit_unavailable(
                    job_id=self.thread_id,
                    agent_type=self.config.agent_id,
                    step_type="kb_unavailable",
                    component="KnowledgeStore",
                    error=e,
                    node_name="session_setup",
                    extra={
                        "embedding_provider": os.environ.get(
                            "KB_EMBEDDING_PROVIDER",
                            os.environ.get("EMBEDDING_PROVIDER", "local"),
                        ),
                    },
                )
                logger.warning(
                    f"Failed to initialize KnowledgeStore (non-fatal): {e} "
                    f"[embedding_provider={os.environ.get('KB_EMBEDDING_PROVIDER', os.environ.get('EMBEDDING_PROVIDER', 'local'))}]"
                )
                self._kb_degraded = True

        # Configured ⇒ required. If the manager pipeline is on (or
        # memory.required is set), a store that failed to init must not run the
        # session blind — fail loud, exactly like the worker path's
        # memory.required freeze (src/agent.py). The lifespan handler turns this
        # into a clean pod exit + a cockpit-surfaced reason via the orchestrator
        # pre-flight (no crash-loop).
        _pipeline = getattr(self.config.memory, "pipeline", None)
        _memory_required = self.config.memory.required or (
            self.config.memory.manager_enabled
            and bool(
                getattr(_pipeline, "scorers", None)
                or getattr(_pipeline, "retrievers", None)
            )
        )
        # Background officer (officer_knowledge_plane.md §3.1, K1): a
        # vector/KB outage must NOT kill the officer — sitrep, job
        # supervision, and paging continue while KB mutations fail closed and
        # the wake carries `project knowledge unavailable`. So the
        # configured⇒required gates below downgrade to a loud log for a
        # commissioned officer instead of failing the attach. Mis-BINDING (the
        # invariant) still fails the attach; only store *outages* degrade.
        from agent.tools.registry import officer_ceiling_active as _officer_active

        _officer_session = _officer_active(getattr(self.config, "officer", None))
        if _memory_required and self._memory_degraded:
            if _officer_session:
                logger.error(
                    "Background officer session: required RecallStore failed "
                    "to initialize — continuing DEGRADED (officer availability "
                    "outranks memory; recollection is unavailable, project "
                    "truth stays in the KB/control plane)."
                )
            else:
                raise MemoryUnavailableError(
                    "memory is required for this session but the embedding-backed "
                    "RecallStore failed to initialize "
                    f"(EMBEDDING_BASE_URL={os.environ.get('EMBEDDING_BASE_URL', 'unset')}, "
                    f"EMBEDDING_MODEL={os.environ.get('EMBEDDING_MODEL', 'unset')}). "
                    "Check the embedding model/endpoint (Admin → Models)."
                )
        if _memory_required and self.knowledge_bindings and self._kb_degraded:
            if _officer_session:
                logger.error(
                    "Background officer session: KnowledgeStore failed to "
                    "initialize — continuing DEGRADED. KB tools fail closed "
                    "with 'project knowledge unavailable'; supervision and "
                    "paging continue."
                )
            else:
                raise MemoryUnavailableError(
                    "memory is required for this session but the KnowledgeStore "
                    "failed to initialize — the embedding endpoint is unavailable "
                    f"(EMBEDDING_BASE_URL={os.environ.get('EMBEDDING_BASE_URL', 'unset')})."
                )

        # MemoryManager seam (memory overhaul Phase 1, behind
        # memory.manager.enabled). Constructed after both stores so the
        # retriever factories bind real handles; the writers read
        # auxiliary_llm/extraction_prompt from the runtime at event time,
        # which is what lets the config.update handler hot-swap them
        # (persistent_app.py keeps runtime in lockstep). A configured pipeline
        # is required — a bind failure (unknown plugin name, or a plugin whose
        # transport won't resolve, e.g. the reranker endpoint) fails the session
        # loud rather than silently mid-turn. Sessions without a vector_conn
        # return above and keep the legacy (no-op) paths.
        if self.config.memory.manager_enabled:
            from agent.services.memory import MemoryManager as MemorySeamManager
            from agent.services.memory import MemoryRuntime

            try:
                self.memory_service = MemorySeamManager.from_config(
                    self.config.memory,
                    MemoryRuntime(
                        recall_store=self.recall_store,
                        knowledge_store=self.knowledge_store,
                        auxiliary_llm=self.auxiliary_llm,
                        memory_config=self.config.memory,
                        auxiliary_config=self.config.auxiliary,
                        extraction_prompt=self.memory_extraction_prompt,
                        assembler_prompt=None,  # persistent mode has no assembler
                        job_id=self.thread_id,
                        project_id=self.project_id,
                        project_ids=list(self.project_ids),
                        # The legacy persistent path bounds each store call at
                        # 5 s (_RETRIEVAL_TIMEOUT in persistent_graph.py).
                        retrieval_timeout=5.0,
                        extra=(
                            {
                                "claim_persistent_extraction_interval": (
                                    lambda turn_count,
                                    interval: postgres_conn.claim_memory_extraction_interval(
                                        self.thread_id,
                                        turn_count=turn_count,
                                        interval=interval,
                                    )
                                )
                            }
                            if postgres_conn is not None
                            else {}
                        ),
                    ),
                )
            except Exception as e:
                if _officer_session:
                    # Same officer availability rule as the store gates above:
                    # a transport that won't resolve (reranker endpoint down)
                    # is outage-shaped. memory_service stays None, so the
                    # legacy direct-store paths (or nothing) carry the session.
                    self.memory_service = None
                    logger.error(
                        "Background officer session: memory pipeline failed to "
                        "bind (%s: %s) — continuing DEGRADED without the "
                        "manager seam.",
                        type(e).__name__,
                        e,
                    )
                else:
                    raise MemoryUnavailableError(
                        "memory pipeline failed to bind: "
                        f"{type(e).__name__}: {e}. A configured memory plugin could "
                        "not resolve its transport (e.g. the reranker endpoint). Fix "
                        "the config or drop the plugin from memory.pipeline."
                    ) from e

        # Ingestion verdicts + bi-temporal supersede (overhaul Phase 4). Wired
        # onto the store independently of the manager cutover — a write-path
        # change behind memory.ingestion.enabled, used by legacy + seam writers.
        from shared.runtime.services.memory.ingestion import (
            maybe_attach_ingestion_verdict,
        )

        maybe_attach_ingestion_verdict(
            self.recall_store,
            getattr(self, "auxiliary_llm", None),
            self.config.memory,
        )

    def swap_backend(self, new_backend: Any) -> None:
        """Hot-swap workspace backend at runtime (e.g. container → VM).

        Connects the new backend, disconnects the old one, replaces
        the WorkspaceManager's backend, and rebuilds the ShellManager.

        Args:
            new_backend: A connected (or connectable) WorkspaceBackend instance.
        """
        if not self.workspace_manager:
            raise RuntimeError("No workspace manager to swap backend on")

        # The REAL backend, not the overlay: swap_backend() rebinds the overlay
        # in place, so an overlay reference held across the swap would follow it
        # onto the new backend.
        from agent.core.virtual_dirs import unwrap_backend

        old_backend = unwrap_backend(self.workspace_manager.backend)

        # Connect new backend first (fail fast)
        if (
            hasattr(new_backend, "connect")
            and not getattr(new_backend, "is_connected", lambda: False)()
        ):
            new_backend.connect()

        # ENV files belong to the physical workspace. Restore the bindings on
        # the new host before retiring the old one or exposing its tools.
        from shared.credential_connectors import collect_credential_env

        credential_env = collect_credential_env(self.datasource_configs or [])
        if credential_env:
            new_backend.install_credential_environment(credential_env)

        # This is a genuine backend retirement, not a queue-claim detach. The
        # deterministic tmux session belongs to the old workspace and must not
        # leak after the live tier swap. Destroy it explicitly before the now
        # transport-only disconnect.
        if getattr(old_backend, "supports_shell", False):
            try:
                retire_shell_owner = getattr(old_backend, "retire_shell_owner", None)
                if retire_shell_owner is not None:
                    retire_shell_owner()
                old_backend.shell_cleanup()
            except Exception as e:
                logger.warning(f"Old backend shell cleanup error: {e}")

        # Permanently retire the old Python backend instance. A cancelled sync
        # tool may still hold it in a worker thread; retirement prevents that
        # stale object from reconnecting after the swap.
        if hasattr(old_backend, "retire"):
            try:
                old_backend.retire()
            except Exception as e:
                logger.warning(f"Old backend retirement error: {e}")
        elif hasattr(old_backend, "disconnect") and hasattr(
            old_backend, "is_connected"
        ):
            try:
                if old_backend.is_connected():
                    old_backend.disconnect()
            except Exception as e:
                logger.warning(f"Old backend disconnect error: {e}")

        # Swap on WorkspaceManager. swap_backend() rebinds the virtual overlay
        # onto the new backend and keeps the registered providers; assigning
        # `_backend` directly unwraps the overlay and 404s every virtual path
        # (knowledge-base/knowledge/features/virtual_directories.md).
        self.workspace_manager.swap_backend(new_backend)

        # Rebuild ShellManager with new backend
        self._setup_shell_manager()
        self._refresh_runtime_facts()

        logger.info(
            f"Backend swapped to {type(new_backend).__name__} "
            f"({getattr(new_backend, '_host', 'local')})"
        )

    async def quiesce_background_tasks(self) -> None:
        """Close every session-scoped detached writer before owner release.

        The persistent process can attach a different thread immediately after
        cleanup.  CitationEngine copies its callback and MemoryManager retains
        pre-compaction tasks, so clearing ToolContext fields alone is not a
        boundary.  This method is idempotent and deliberately propagates an
        unjoinable-task failure for stateless callers: the queue lease must stay
        held until the reaper fences the claimant.
        """

        if self._background_tasks_quiesced:
            return

        await self.quiesce_subagents("session background work quiescing")

        if self.tool_context is not None:
            citation_engine = getattr(self.tool_context, "citation_engine", None)
            if citation_engine is not None:
                close_engine = getattr(citation_engine, "aclose", None)
                if close_engine is None:
                    if self.shell_owner_token is not None:
                        raise RuntimeError(
                            "stateless citation engine lacks terminal close"
                        )
                else:
                    await close_engine()
                # aclose disarms and joins first; only now may the cached
                # engine/source registry be released.
                self.tool_context.close_citation_engine()

        if self.memory_service is not None:
            close_memory = getattr(self.memory_service, "close_background", None)
            if close_memory is None:
                if self.shell_owner_token is not None:
                    raise RuntimeError("stateless memory manager lacks terminal close")
            else:
                await close_memory()

        self._background_tasks_quiesced = True

    async def cleanup(
        self,
        *,
        preserve_shell: bool = False,
        preserve_workspace_daemons: bool = False,
    ) -> None:
        """Clean up agent-local resources and disconnect the backend transport.

        ``preserve_shell`` is the queue-claim/ownership handoff disposition:
        the remote tmux session remains on the workspace for a later claimant.
        Genuine thread end and backend retirement keep the default destructive
        shell cleanup.

        ``preserve_workspace_daemons`` is deliberately independent.  It is set
        only for a stateless physical-claim handoff: the retiring Python owner
        cancels its overlay monitor and rclone token refresher, closes its
        Keycloak clients, and retires its mutation fence, while leaving the
        workspace-side rclone/overlay processes resident for the next claim to
        adopt or heal.  Pinned detach/drain and genuine thread end keep their
        historical destructive unmount behaviour.
        """
        # Revoke input/provider/tool admission synchronously before any await
        # or remote cleanup. The monitor/controller active flags may remain
        # true until their teardown completes.
        self._protected_cloud_health_ready = False
        if not preserve_shell and not preserve_workspace_daemons:
            self.local_quiescence_protocol = ""
        if preserve_workspace_daemons and self.shell_owner_token is None:
            # Cleanup must remain best-effort and complete, so do not raise in
            # this late teardown path.  Ignore the invalid disposition and
            # retain pinned's destructive semantics instead of risking that a
            # future caller accidentally strands its mounts.
            logger.error(
                "Ignoring workspace-daemon preservation for non-stateless "
                "session: thread=%s",
                self.thread_id,
            )
            preserve_workspace_daemons = False

        backend_for_cleanup = None
        shell_retirement_error: Exception | None = None
        backend_retirement_error: Exception | None = None
        resident_cleanup_error: Exception | None = None
        browser_shutdown_error: Exception | None = None
        local_handoff_error: Exception | None = None
        strict_local_handoff = bool(
            self.shell_owner_token is not None and preserve_workspace_daemons
        )
        exact_pinned_terminal = bool(
            (
                self.pinned_runtime_identity_required is True
                or self.protected_cloud_required is True
            )
            and self.shell_owner_token is None
            and not preserve_shell
            and not preserve_workspace_daemons
        )
        strict_pinned_physical_cleanup = bool(
            exact_pinned_terminal
            and self.workspace_backend_tier in {"sandbox", "vm", "remote"}
        )
        strict_sandbox_cleanup = bool(
            strict_pinned_physical_cleanup and self.workspace_backend_tier == "sandbox"
        )
        strict_terminal_cleanup = bool(
            self.shell_owner_token is not None or exact_pinned_terminal
        )
        strict_resident_cleanup = bool(
            strict_terminal_cleanup and not preserve_workspace_daemons
        )
        if self.workspace_manager:
            from agent.core.virtual_dirs import unwrap_backend

            backend_for_cleanup = unwrap_backend(self.workspace_manager.backend)
            retire_shell_owner = getattr(
                backend_for_cleanup, "retire_shell_owner", None
            )
            if retire_shell_owner is not None:
                # Close shell admission before any potentially slow mount,
                # datasource, memory or Git teardown. Cancelled sync tool work
                # may still hold this object in a thread; it must not submit
                # another tmux command during the handoff.
                retire_shell_owner()
        elif self._workspace_backend_for_cleanup is not None:
            backend_for_cleanup = self._workspace_backend_for_cleanup
            retire_shell_owner = getattr(
                backend_for_cleanup, "retire_shell_owner", None
            )
            if retire_shell_owner is not None:
                retire_shell_owner()

        # Belt for partial-attach and non-standard cleanup call sites. The
        # ordinary app teardown invokes this before its journal closes; this
        # idempotent call ensures no alternate path can skip the RAM barrier.
        await self.quiesce_background_tasks()

        # Ask the browser daemon to stop cleanly before the kernel-level UID
        # sweep below. A lost/malformed daemon acknowledgement is not itself a
        # zero-writer proof, so retain the error until strict shell retirement
        # either proves the whole dedicated workspace UID empty or fails.
        if strict_pinned_physical_cleanup and self.tool_context is not None:
            try:
                await self.tool_context.close_browser(strict=True)
            except Exception as exc:
                browser_shutdown_error = exc
                logger.warning(
                    "Protected browser shutdown was not acknowledged; "
                    "requiring workspace UID zero proof: %s",
                    exc,
                )

        if self.tool_context is not None:
            self.tool_context.citation_verdict_callback = None
            self.tool_context.canvas_event_callback = None
            self.tool_context._resolved_tool_names = []
            self.tool_context.session_runtime_facts = None

        if self._cloud_overlay_monitor_task is not None:
            self._cloud_overlay_monitor_task.cancel()
            try:
                await self._cloud_overlay_monitor_task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                if strict_resident_cleanup:
                    resident_cleanup_error = exc
                    logger.error("Workspace overlay monitor did not stop: %s", exc)
                elif strict_local_handoff:
                    local_handoff_error = exc
                    logger.error("Stateless overlay monitor did not stop: %s", exc)
                else:
                    logger.debug("cloud overlay monitor task exit", exc_info=True)
            if local_handoff_error is None:
                self._cloud_overlay_monitor_task = None

        if self.overlay_mount_manager is not None:
            try:
                if preserve_workspace_daemons:
                    # Local state only.  In particular, do not run any remote
                    # unmount script after a successor may own the workspace.
                    self.overlay_mount_manager.detach_local()
                else:
                    loop = asyncio.get_running_loop()
                    if strict_resident_cleanup:
                        await loop.run_in_executor(
                            None,
                            lambda: self.overlay_mount_manager.unmount(strict=True),
                        )
                    else:
                        await loop.run_in_executor(
                            None, self.overlay_mount_manager.unmount
                        )
                self.overlay_mount_manager = None
            except Exception as exc:
                if strict_resident_cleanup:
                    resident_cleanup_error = exc
                    logger.error("Stateless overlay resident did not retire: %s", exc)
                elif strict_local_handoff:
                    local_handoff_error = exc
                    logger.error("Stateless overlay controller did not detach: %s", exc)
                else:
                    logger.debug("overlay cleanup failed", exc_info=True)
                    self.overlay_mount_manager = None

        if self.cloud_mount_manager:
            if strict_resident_cleanup and resident_cleanup_error is not None:
                logger.error(
                    "Skipping rclone retirement because the dependent overlay "
                    "is still resident"
                )
            else:
                try:
                    if preserve_workspace_daemons:
                        await self.cloud_mount_manager.detach_for_handoff()
                    elif strict_resident_cleanup:
                        await self.cloud_mount_manager.aclose(strict=True)
                    else:
                        await self.cloud_mount_manager.aclose()
                    self.cloud_mount_manager = None
                except Exception as exc:
                    if strict_resident_cleanup:
                        resident_cleanup_error = exc
                        logger.error(
                            "Stateless rclone resident did not retire: %s", exc
                        )
                    elif strict_local_handoff:
                        local_handoff_error = exc
                        logger.error(
                            "Stateless rclone controller did not detach: %s", exc
                        )
                    else:
                        logger.warning(f"Cloud mount cleanup error: {exc}")
                        self.cloud_mount_manager = None

        if self.shell_manager and not preserve_shell:
            try:
                if strict_pinned_physical_cleanup:
                    self.shell_manager.cleanup(strict=True)
                else:
                    self.shell_manager.cleanup()
            except Exception as e:
                if strict_terminal_cleanup:
                    # Stateless and protected terminal teardown are
                    # acknowledged remote mutations, not best-effort local
                    # cleanup. Keep the backend retryable and surface failure
                    # after the remaining local resources have been made inert.
                    shell_retirement_error = e
                    logger.error("Session shell retirement was not acknowledged: %s", e)
                else:
                    logger.warning(f"Shell cleanup error: {e}")
        elif self.shell_manager:
            logger.info(
                "Preserving remote shell for session handoff: thread=%s",
                self.thread_id,
            )

        if (
            strict_sandbox_cleanup
            and self.shell_manager is None
            and backend_for_cleanup is not None
            and shell_retirement_error is None
        ):
            try:
                protocol = backend_for_cleanup.protected_workspace_zero_cleanup_strict()
                if protocol != "workspace_process_zero_v1":
                    raise WorkspaceUnavailableError(
                        "Sandbox workspace process-zero proof is unavailable"
                    )
            except Exception as exc:
                shell_retirement_error = exc

        if strict_pinned_physical_cleanup and shell_retirement_error is None:
            protocol = getattr(
                backend_for_cleanup,
                "terminal_local_quiescence_protocol",
                None,
            )
            if strict_sandbox_cleanup and protocol == "workspace_process_zero_v1":
                # The UID proof covers browser-exec, Chromium, code-server and
                # detached writers even when their cooperative tag was
                # cleared. It is strictly stronger than the daemon response.
                self.local_quiescence_protocol = protocol
                browser_shutdown_error = None
            elif strict_sandbox_cleanup:
                shell_retirement_error = WorkspaceUnavailableError(
                    "Sandbox workspace process-zero proof is unavailable"
                )
            else:
                # VM/remote process-zero belongs to the orchestrator actuator,
                # which can prove the exact VM generation/UID after this live
                # agent is absent. Never forge that stronger proof locally.
                shell_retirement_error = WorkspaceUnavailableError(
                    "Workspace actuator zero proof is required"
                )

        # Close datasource connections
        if self.datasources or self._datasource_clients:
            from agent.core.datasource_setup import close_datasource_connections

            close_datasource_connections(self.datasources, self._datasource_clients)
            self.datasources = {}
            self._datasource_clients = {}

        # Close knowledge graph connection
        if self._knowledge_graph:
            try:
                self._knowledge_graph.close()
                logger.debug("Closed knowledge graph connection")
            except Exception as e:
                logger.warning(f"Error closing knowledge graph: {e}")
            self._knowledge_graph = None

        # Retire remote backend instances terminally. Merely disconnecting is a
        # transport reset and would let cancelled sync work reconnect after the
        # claim/session handoff.
        if (
            backend_for_cleanup is not None
            and resident_cleanup_error is None
            and local_handoff_error is None
        ):
            backend = backend_for_cleanup
            if shell_retirement_error is not None:
                retire_claim_resource_owner = getattr(
                    backend, "retire_claim_resource_owner", None
                )
                if retire_claim_resource_owner is not None:
                    try:
                        retire_claim_resource_owner()
                    except Exception as e:
                        logger.warning(f"Claim-resource retirement error: {e}")
                # Do not call retire(): it permanently prevents the same session
                # object from reconnecting for a terminal-retirement retry.
                # Shell admission and claim-resource admission are already
                # closed, so a transport reset is sufficient local containment.
                if hasattr(backend, "disconnect"):
                    try:
                        backend.disconnect()
                    except Exception as e:
                        logger.warning(f"Backend disconnect error: {e}")
            elif hasattr(backend, "retire"):
                try:
                    backend.retire()
                    self._workspace_backend_for_cleanup = None
                    logger.info("Remote workspace backend retired")
                except Exception as e:
                    if strict_terminal_cleanup:
                        backend_retirement_error = e
                        logger.error(
                            "Session backend retirement was not acknowledged: %s",
                            e,
                        )
                    else:
                        logger.warning(f"Backend retirement error: {e}")
            elif hasattr(backend, "disconnect") and hasattr(backend, "is_connected"):
                try:
                    if backend.is_connected():
                        backend.disconnect()
                        logger.info("Remote workspace backend disconnected")
                except Exception as e:
                    if strict_terminal_cleanup:
                        backend_retirement_error = e
                        logger.error(
                            "Session backend disconnect was not acknowledged: %s",
                            e,
                        )
                    else:
                        logger.warning(f"Backend disconnect error: {e}")

        if (
            exact_pinned_terminal
            and self.workspace_backend_tier in {"virtual", "none"}
            and shell_retirement_error is None
            and browser_shutdown_error is None
            and resident_cleanup_error is None
            and local_handoff_error is None
            and backend_retirement_error is None
        ):
            # No external workspace exists. The common teardown has already
            # joined the loop, watchdogs, side tasks and ordered event writer;
            # cleanup's own background-task and backend retirement is the final
            # local actor proof.
            self.local_quiescence_protocol = "agent_runtime_zero_v1"

        if (
            exact_pinned_terminal
            and not self.workspace_backend_tier
            and backend_for_cleanup is None
            and self.shell_manager is None
            and self.overlay_mount_manager is None
            and self.cloud_mount_manager is None
            and shell_retirement_error is None
            and browser_shutdown_error is None
            and resident_cleanup_error is None
            and local_handoff_error is None
            and backend_retirement_error is None
        ):
            # Attach failed before any external workspace/backend was
            # constructed. The already-joined agent tasks/writer are the
            # complete effect surface for this partial delivery.
            self.local_quiescence_protocol = "agent_runtime_zero_v1"

        if (
            exact_pinned_terminal
            and not self.local_quiescence_protocol
            and shell_retirement_error is None
            and browser_shutdown_error is None
            and resident_cleanup_error is None
            and local_handoff_error is None
            and backend_retirement_error is None
        ):
            shell_retirement_error = WorkspaceUnavailableError(
                "No trusted local quiescence protocol is available for this "
                "workspace tier"
            )

        logger.info(f"PersistentSession cleaned up: thread={self.thread_id}")
        if shell_retirement_error is not None:
            raise WorkspaceUnavailableError(
                "Session shell retirement remains unacknowledged"
            ) from shell_retirement_error
        if browser_shutdown_error is not None:
            raise WorkspaceUnavailableError(
                "Workspace browser retirement remains unacknowledged"
            ) from browser_shutdown_error
        if resident_cleanup_error is not None:
            raise WorkspaceUnavailableError(
                "Workspace residents remain active"
            ) from resident_cleanup_error
        if local_handoff_error is not None:
            raise WorkspaceUnavailableError(
                "Stateless local workspace controllers remain active"
            ) from local_handoff_error
        if backend_retirement_error is not None:
            raise WorkspaceUnavailableError(
                "Session backend retirement remains unacknowledged"
            ) from backend_retirement_error
