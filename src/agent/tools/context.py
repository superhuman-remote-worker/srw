"""Tool context for dependency injection.

Provides a container for dependencies that tools need access to,
such as workspace managers, database connections, and configuration.
"""

import asyncio
import hashlib
import logging
import posixpath
import re
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Deque,
    Dict,
    List,
    Literal,
    Optional,
    Set,
)
from urllib.parse import urlparse

from shared.runtime.core.datasource_catalog import DATASOURCE_TYPES
from shared.runtime.core.product_capabilities import (
    ComponentProvenance,
    ProductComponent,
    ProvenanceStatus,
)
from agent.core.workspace import WorkspaceManager

logger = logging.getLogger(__name__)

# Avoid circular imports with TYPE_CHECKING
if TYPE_CHECKING:
    from agent.database.postgres_db import PostgresDB
    from agent.services.knowledge.bindings import KnowledgeBinding
    from shared.subagent_parent_authority import ParentExecutionAuthority


WorkspaceBackendId = Literal["sandbox", "vm", "virtual", "none"]
EmailAccessTier = Literal["read", "read_write", "draft", "send"]


@dataclass(frozen=True, slots=True)
class SessionRuntimeFacts:
    """One immutable, redacted observation of a persistent session runtime.

    The capability tool consumes this object instead of inspecting mutable
    attach payloads or live connection objects during a model call. It carries
    only public aggregate facts: no datasource/resource names or IDs,
    accounts, folders, credentials, hosts, URLs, project IDs, or mount paths.
    Replacing the reference on ``ToolContext`` is the atomic publication step.
    """

    observed_at: datetime
    backend_id: WorkspaceBackendId | None
    backend_supports_shell: bool
    backend_supports_file_tools: bool
    backend_supports_canvas_presentation: bool
    backend_supports_canvas_live_apps: bool
    backend_supports_shared_browser: bool
    attached_datasource_types: tuple[str, ...] = ()
    email_access_tier: EmailAccessTier | None = None
    email_connection_failed: bool = False
    email_direct_send_enabled: bool = False
    knowledge_binding_available: bool = False
    knowledge_store_available: bool = False
    memory_available: bool = False
    cloud_mount_active: bool = False
    protected_cloud_active: bool = False
    loaded_tool_names: tuple[str, ...] = ()
    runtime_component_provenance: tuple[
        tuple[ProductComponent, ComponentProvenance], ...
    ] = ()

    def __post_init__(self) -> None:
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("SessionRuntimeFacts.observed_at must be timezone-aware")
        object.__setattr__(
            self,
            "observed_at",
            self.observed_at.astimezone(timezone.utc),
        )

        if self.backend_id not in {None, "sandbox", "vm", "virtual", "none"}:
            raise ValueError("SessionRuntimeFacts contains an unknown backend ID")
        if self.email_access_tier not in {
            None,
            "read",
            "read_write",
            "draft",
            "send",
        }:
            raise ValueError("SessionRuntimeFacts contains an unknown email tier")

        datasource_types = tuple(sorted(set(self.attached_datasource_types)))
        if any(item not in DATASOURCE_TYPES for item in datasource_types):
            raise ValueError("SessionRuntimeFacts contains an unknown datasource type")
        object.__setattr__(self, "attached_datasource_types", datasource_types)

        tool_names = tuple(sorted(set(self.loaded_tool_names)))
        if any(
            not isinstance(name, str)
            or not name
            or len(name) > 120
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,119}", name) is None
            for name in tool_names
        ):
            raise ValueError("SessionRuntimeFacts contains an invalid tool name")
        object.__setattr__(self, "loaded_tool_names", tool_names)

        allowed_components = {
            ProductComponent.AGENT,
            ProductComponent.GUIDE,
            ProductComponent.WORKSPACE,
        }
        component_provenance: dict[ProductComponent, ComponentProvenance] = {}
        for item in self.runtime_component_provenance:
            if not isinstance(item, tuple) or len(item) != 2:
                raise ValueError(
                    "SessionRuntimeFacts contains invalid component provenance"
                )
            component, provenance = item
            if (
                component not in allowed_components
                or not isinstance(provenance, ComponentProvenance)
                or provenance.provenance_status is ProvenanceStatus.VERIFIED
                or component in component_provenance
            ):
                raise ValueError(
                    "SessionRuntimeFacts contains invalid component provenance"
                )
            component_provenance[component] = provenance
        object.__setattr__(
            self,
            "runtime_component_provenance",
            tuple(sorted(component_provenance.items(), key=lambda item: item[0].value)),
        )

        email_attached = "email" in datasource_types
        if email_attached and self.email_access_tier is None:
            raise ValueError("attached email requires an effective access tier")
        if self.email_access_tier is not None and not email_attached:
            raise ValueError("email_access_tier requires an attached email datasource")
        if self.email_connection_failed and not email_attached:
            raise ValueError(
                "email_connection_failed requires an attached email datasource"
            )
        if self.email_direct_send_enabled and not email_attached:
            raise ValueError(
                "email_direct_send_enabled requires an attached email datasource"
            )
        if self.protected_cloud_active and not self.cloud_mount_active:
            raise ValueError("protected cloud requires an active cloud mount")


@dataclass
class ToolContext:
    """Context container for tool dependencies.

    This class holds all dependencies that tools may need during execution.
    It's passed to tool creation functions to enable dependency injection
    without global state.

    Attributes:
        workspace_manager: WorkspaceManager for file operations
        todo_manager: TodoManager for task tracking (optional)
        postgres_db: PostgresDB instance for orchestrator database operations
        datasources: Dictionary of external datasource connections keyed by type
            (e.g. {"neo4j": Neo4jDB(...), "postgresql": asyncpg_pool, ...})
        config: Additional configuration dictionary
        job_id: Override job ID (if not using workspace_manager)
        citation_engine: CitationEngine instance for citation management
        _source_registry: Cache of registered source identifiers to source IDs

    Example:
        ```python
        workspace = WorkspaceManager(job_id="job-123")
        workspace.initialize()

        context = ToolContext(
            workspace_manager=workspace,
            config={"max_file_size": 1024 * 1024}
        )

        tools = create_workspace_tools(context)
        ```
    """

    workspace_manager: Optional[WorkspaceManager] = None
    todo_manager: Optional[Any] = (
        None  # TodoManager, imported later to avoid circular deps
    )
    postgres_db: Optional["PostgresDB"] = None
    vector_db: Optional["PostgresDB"] = (
        None  # Vector-store pool (srw_vector) — citations live here, NOT in
        # postgres_db (the main app DB). Injected from agent.vector_conn.
    )
    verify_aux: Optional[Any] = (
        None  # AuxiliaryLLM for citation verification (Phase 2). When set,
        # cite_* schedules async verdict write-back; None = verification off.
    )
    verify_citation_prompt: Optional[str] = (
        None  # Matrix-resolved citation-verification system prompt.
    )
    citation_verdict_callback: Optional[Callable[[int, str], None]] = (
        None  # (citation_id, status) listener fired when a background
        # verification lands a verdict. Persistent sessions wire this to a
        # WS/SSE broadcast (live citations-panel update); worker jobs leave it
        # None. Threaded to CitationEngine(on_verdict=...).
    )
    canvas_event_callback: Optional[Callable[[str, Dict[str, Any]], Any]] = (
        None  # (method, params) post-commit Canvas invalidation hook. Persistent
        # sessions wire this to their ordered _broadcast path; worker jobs leave
        # it unset. REST state remains authoritative if the callback fails.
    )
    datasources: Dict[str, Any] = field(default_factory=dict)
    config: Dict[str, Any] = field(default_factory=dict)
    redaction_secrets: tuple = ()  # Credential values to withhold from this context's tool output that its
    # own workspace does not reveal — a worktree child's fresh WorkspaceManager
    # knows none of the parent's repository tokens (tool_output_redaction.py).
    _job_id: Optional[str] = None  # Direct job_id override
    citation_engine: Optional[Any] = None  # CitationEngine, imported lazily
    _source_registry: Dict[str, int] = field(
        default_factory=dict
    )  # path/url -> source_id
    _cloud_anchors: Dict[str, Dict[str, Any]] = field(
        default_factory=dict
    )  # resolved local path -> cloud snapshot-anchor (Phase 3, D7): the
    # drift fingerprint (etag, file_sha256) + best-effort live pointer
    # (backend, path, webdav_url) captured when a cloud file is read, so
    # cite_* can persist it onto the source's metadata.cloud block.
    _cloud_anchor_write_locks: Dict[str, asyncio.Lock] = field(
        default_factory=dict
    )  # Per-canonical-path serialization for the workspace write + durable
    # anchor update. Without this, concurrent downloads to the same target can
    # leave bytes from one source paired with the other source's provenance.
    cloud_anchor_persist_callback: Optional[
        Callable[[str, Dict[str, Any]], Awaitable[None]]
    ] = (
        None  # Persistent sessions bind this to a per-thread Postgres upsert.
        # Pinned workers and focused tests may leave it unset and retain the
        # historical claim-local anchor cache.
    )
    _inaccessible_sources: Dict[str, str] = field(
        default_factory=dict
    )  # url -> error message
    _recent_reads: Deque[str] = field(
        default_factory=lambda: deque(maxlen=10)
    )  # Recently read file paths
    _pinned_reads: Set[str] = field(
        default_factory=set
    )  # Instruction-file paths, exempt from FIFO eviction. The path remains
    # known after one read; phase/freshness-scoped gates separately validate
    # _instruction_read_stamps and may still re-arm. Write authorization
    # (recent_read_matches) deliberately does not consult this set.
    _recent_read_versions: Dict[str, str] = field(
        default_factory=dict
    )  # Optional sha256 of the full text bytes observed for a recent path
    _instruction_read_stamps: Dict[str, Dict[str, Any]] = field(
        default_factory=dict
    )  # path -> LLM turn + concrete phase instance at the most recent read
    _current_phase: Optional[str] = None
    _current_phase_number: Optional[int] = None
    _current_turn_count: int = 0
    # Exact durable parent rows for a session delegation.  The persistent
    # loop publishes these only while executing one assistant tool-call batch;
    # session child creation copies them into immutable spawn metadata so
    # crash recovery never has to infer identity from a reusable turn number.
    _current_input_message_id: Optional[str] = None
    _current_ai_message_id: Optional[str] = None
    _llm_config: Optional[Any] = None  # LLMConfig for phase-aware multimodal
    _instruction_files: List[Any] = field(
        default_factory=list
    )  # List[InstructionFileEntry]
    recall_store: Optional[Any] = None  # RecallStore instance (Memory Light)
    shell_manager: Optional[Any] = None  # ShellManager (persistent terminal sessions)
    progress_committer: Optional[Any] = (
        None  # ProgressCommitter (src/core/progress_commit.py). Shared by the
        # todo_complete tool and the graph's turn loop so both triggers use one
        # push clock. Left None for persistent sessions, which already commit
        # and push per turn (src/persistent_graph.py:949).
    )
    session_task_manager: Optional[Any] = (
        None  # SessionTaskManager (persistent session todos)
    )
    knowledge_graph: Optional[Any] = (
        None  # KnowledgeGraphDB (system Neo4j for knowledge base)
    )
    knowledge_store: Optional[Any] = None  # KnowledgeStore (pgvector search index)
    knowledge_bindings: List["KnowledgeBinding"] = field(
        default_factory=list
    )  # Native + selected external OKF KB scopes
    runtime_actor: Optional[Any] = (
        None  # Hidden server-derived RuntimeActorContext; never a tool argument
    )
    _project_id: Optional[str] = None  # Project UUID for knowledge scoping
    _project_ids: List[str] = field(
        default_factory=list
    )  # Multi-project UUIDs for persistent sessions
    _graph_progress: int = 0
    _pending_memories: List[Dict[str, Any]] = field(
        default_factory=list
    )  # Sync-safe memory queue
    _freeze_request: Optional[Dict[str, Any]] = (
        None  # Tool-requested job freeze (blocking send_message)
    )
    _officer_sleep_request: Optional[Dict[str, Any]] = (
        None  # Officer sleep tool parked a wake request (centurion sessions)
    )
    _replan_request: Optional[str] = (
        None  # Reason string parked by request_replan: the tactical phase has
        # learned something that changes the approach and wants the strategic
        # phase early. Consumed by check_todos, which ends the phase. Note this
        # is the ONLY way to leave a tactical phase without completing every
        # todo, so it is also the only in-flight adaptation path once phases
        # get large.
    )
    _reply_drain_requested: bool = (
        False  # A todo just completed — a natural break at which to deliver
        # queued (non-urgent) replies. Set by todo_complete, consumed by
        # audited_tools. Replaces the tactical->strategic boundary as the
        # drain point, which stops firing often enough as phases grow.
    )
    _delivered_reply_keys: Set[str] = field(
        default_factory=set
    )  # Content keys of queued replies already appended to the conversation.
    # Pinned workers use this process-locally while their ack is in flight.
    # Stateless workers hydrate it from checkpointed delivered_reply_keys.
    _stateless_worker: bool = False
    _worker_lease_token: Optional[int] = None
    _parent_execution_authority: Optional["ParentExecutionAuthority"] = (
        None  # Immutable worker-job authority captured before subagent ledger
        # construction.  Child persistence must never reconstruct this later
        # from mutable client/environment state.
    )
    _snapshot_callback: Optional[Any] = (
        None  # Callable[[str], None] — pre-write file snapshot for undo
    )
    orchestrator_client: Optional[Any] = None  # OrchestratorClient for delegation
    _thread_id: Optional[str] = (
        None  # Persistent-session thread UUID. Set by persistent_session so
        # session-spawned worker jobs (create_job) can carry the
        # session's thread back to the orchestrator, which derives the
        # owning user_id + project_id and applies their model preferences
        # during dispatch. Unset in worker-job mode.
    )
    user_id: Optional[str] = (
        None  # Originating user UUID. Set by persistent_session from the
        # thread row's owner so agent-initiated calls to the orchestrator
        # (jobs.py, messaging.py, orchestrator_client.py) can forward
        # `X-MCP-User-Id` alongside `X-Internal-Key`. The orchestrator's
        # `_get_user_from_mcp_headers` then resolves the user and the
        # call is accepted by `require_approved_user` / `require_job_access`
        # instead of 401-ing. Worker jobs derive it below from their hidden,
        # server-minted runtime actor rather than from model-visible config.
    )
    _job_metadata: Dict[str, Any] = field(
        default_factory=dict
    )  # job_id, project_id, priority, config_name, repo_name
    _resolved_tool_names: List[str] = field(
        default_factory=list
    )  # Parent's actually-loaded tool names, stashed post-load: the runtime
    # ceiling a subagent child's allowlist is intersected with (U3 B.2).
    # Empty until _setup_job_tools finishes loading.
    session_runtime_facts: Optional[SessionRuntimeFacts] = (
        None  # Atomically replaced redacted persistent-session observation.
        # Worker jobs and sessions still setting up/tearing down leave it None.
    )
    _limits: Optional[Any] = None  # Parent LimitsConfig — carried for the
    # subagent child build (create_llm(child_cfg, limits=...)).
    # --- Built-in subagents (U3). The parent's tool node / agent.py stamp
    # these; ``delegate_agent`` and ``src.subagents`` read them lazily. ---
    subagent_runtime: Optional[Any] = (
        None  # SubagentRuntime — one per parent job (roster, semaphore,
        # handles, idempotent re-execution). Installed by agent.py after the
        # tools are loaded; the delegate_agent tool builds one on first use if
        # it is still None (sessions in U5 take that path).
    )
    _parent_host: Optional[Any] = (
        None  # ParentHost (WorkerHost for jobs) the runtime hands to children.
    )
    _subagent_parent_kind: Optional[str] = (
        None  # Explicit ``job``/``session`` discriminator.  Session contexts
        # set this before tool loading so lazy runtime construction can never
        # mistake their thread UUID for the legacy ``_job_id`` alias.
    )
    _session_parent_authority_provider: Optional[Callable[[], Any]] = (
        None  # Fresh pinned/stateless SessionParentAuthority for every child
        # persistence operation.  Stateless warm sessions repoint leases
        # between turns, so this must be a provider rather than a captured
        # token.
    )
    _session_parent_authority: Optional[Any] = (
        None  # Fixed pinned authority convenience for tests/embedders.
    )
    _stateless_subagent_recovery_active: bool = (
        False  # True only while a stateless executor is feeding an orphaned
        # foreground child's evidence back through the abandoned parent turn.
        # delegate_agent fails closed for that turn: recovery evidence must
        # not recursively create replacement child work.
    )
    parent_context_probe: Optional[Callable[[], Any]] = (
        None  # () -> ContextProbe of the parent's live ContextManager, stashed
        # by build_phase_alternation_graph; the return envelope's headroom
        # share reads it (B.5). None = no parent accounting (entry budget).
    )
    auxiliary_llm: Optional[Any] = (
        None  # The parent's AuxiliaryLLM — children compact with it (a child
        # with none fast-fails its summarizer and keeps its raw history).
    )
    provider_admission: Optional[Callable[[], bool]] = (
        None  # () -> bool: False once the parent is draining. Every child
        # checks it before each provider call (before_provider_admission) and
        # ends its turn without spend when it is closed.
    )
    _fork_source: Optional[List[Any]] = (
        None  # The parent's DURABLE state["messages"], stamped by the tool
        # node before a delegation batch; ``fork=true`` children seed from it.
    )
    _parent_audit_metadata: Optional[Dict[str, Any]] = (
        None  # state["metadata"] stamped per delegation batch — merged under
        # the child's subagent_* keys on every child tool audit row.
    )

    def __post_init__(self):
        """Validate context after initialization."""
        # A worker's runtime actor is minted from the durable job row by the
        # orchestrator and delivered on the internal start wire.  Treat that
        # hidden actor as the authoritative identity source for application
        # calls made by worker tools.  Without this bridge, lifecycle critics
        # receive the job-inspection tools but omit X-MCP-User-Id, so every
        # require_job_access endpoint truthfully returns 401.
        actor_user_id = getattr(self.runtime_actor, "user_id", None)
        if self.user_id is None and actor_user_id:
            self.user_id = str(actor_user_id)

        # Workspace manager is required for workspace tools
        if (
            self.workspace_manager is not None
            and not self.workspace_manager.is_initialized
        ):
            raise ValueError(
                "WorkspaceManager must be initialized before creating ToolContext. "
                "Call workspace_manager.initialize() first."
            )

    @property
    def job_id(self) -> Optional[str]:
        """Get the current job ID.

        Returns job_id from _job_id override, or from workspace_manager if available.
        """
        if self._job_id:
            return self._job_id
        if self.workspace_manager:
            return self.workspace_manager.job_id
        return None

    @job_id.setter
    def job_id(self, value: Optional[str]) -> None:
        """Set the job ID directly."""
        self._job_id = value

    @property
    def thread_id(self) -> Optional[str]:
        """Persistent-session thread UUID (None outside session mode)."""
        return self._thread_id

    @thread_id.setter
    def thread_id(self, value: Optional[str]) -> None:
        self._thread_id = value

    def has_workspace(self) -> bool:
        """Check if workspace manager is available."""
        return self.workspace_manager is not None

    def has_todo(self) -> bool:
        """Check if todo manager is available."""
        return self.todo_manager is not None

    def has_postgres(self) -> bool:
        """Check if PostgreSQL connection is available."""
        return self.postgres_db is not None

    def has_datasource(self, ds_type: str) -> bool:
        """Check if a datasource of the given type is available.

        Args:
            ds_type: Datasource type (e.g. "neo4j", "postgresql", "mongodb")

        Returns:
            True if datasource is available
        """
        return ds_type in self.datasources and self.datasources[ds_type] is not None

    def get_datasource(self, ds_type: str) -> Optional[Any]:
        """Get a datasource connection by type.

        Args:
            ds_type: Datasource type (e.g. "neo4j", "postgresql", "mongodb")

        Returns:
            Datasource connection object, or None if not available
        """
        return self.datasources.get(ds_type)

    def next_graph_progress(self) -> int:
        """Advance and return the graph-progress marker.

        This marker is emitted in heartbeat metrics and used by the orchestrator
        to detect a worker that is heartbeating but not advancing graph work.
        """
        self._graph_progress += 1
        return self._graph_progress

    def get_graph_progress(self) -> int:
        """Return the current graph-progress marker."""
        return self._graph_progress

    def has_git(self) -> bool:
        """Check if git manager is available and active.

        Returns True only if workspace_manager exists, has a git_manager,
        and the git_manager is active (git available and repo initialized).
        """
        if not self.has_workspace():
            return False
        gm = self.workspace_manager.git_manager
        return gm is not None and gm.is_active

    def has_shell(self) -> bool:
        """Check if ShellManager is available for persistent terminal sessions."""
        return self.shell_manager is not None

    def has_shell_tool(self) -> bool:
        """Whether a shell-execution tool is actually bound for this agent.

        Not the same as :meth:`has_shell`: a ShellManager exists whenever the
        backend supports a shell, including for experts granted no shell tool
        (writer, curator, ... with ``shell: []``). This reads the bound tool
        names with the predicate that gates the prompt's ``{% if has_shell %}``
        blocks. Empty until tool loading finishes, so it never over-claims.
        """
        from shared.runtime.core.loader import _has_shell_tools

        return _has_shell_tools(set(self._resolved_tool_names or ()))

    def has_knowledge(self) -> bool:
        """Check if the knowledge base is available.

        Neo4j is OPTIONAL (OKF slice-3 PR4c): the pgvector store is canonical for
        retrieval and the OKF files for content, so the KB works graph-less — the
        graph-shaped tools degrade honestly (see ``create_kb_tools``). Only the
        store is required. On a Neo4j-enabled deployment both are present and the
        full graph path runs unchanged.
        """
        return self.knowledge_store is not None

    @property
    def project_id(self) -> Optional[str]:
        """Get the project ID for knowledge scoping."""
        return self._project_id

    @project_id.setter
    def project_id(self, value: Optional[str]) -> None:
        """Set the project ID."""
        self._project_id = value

    @property
    def project_ids(self) -> List[str]:
        """Get all project IDs for multi-project scoping."""
        if self._project_ids:
            return self._project_ids
        if self._project_id:
            return [self._project_id]
        return []

    @project_ids.setter
    def project_ids(self, value: List[str]) -> None:
        """Set project IDs (also updates primary project_id)."""
        self._project_ids = value or []
        self._project_id = value[0] if value else None

    @property
    def kb_ids(self) -> List[str]:
        """Authorized KB ids, falling back to legacy project scoping."""
        if self.knowledge_bindings:
            return [str(binding.kb_id) for binding in self.knowledge_bindings]
        return list(self.project_ids)

    def knowledge_binding(self, selector: str) -> Optional["KnowledgeBinding"]:
        """Resolve a KB alias or UUID inside the authorized binding set."""
        needle = str(selector or "").strip().lower()
        for binding in self.knowledge_bindings:
            if binding.alias.lower() == needle or str(binding.kb_id).lower() == needle:
                return binding
        return None

    @property
    def writable_knowledge_binding(self) -> Optional["KnowledgeBinding"]:
        """The native write target; external Slice 4 bindings are read-only."""
        for binding in self.knowledge_bindings:
            if binding.writable:
                return binding
        return None

    @property
    def db(self) -> Optional["PostgresDB"]:
        """Get PostgresDB instance.

        Returns:
            PostgresDB instance if available, None otherwise
        """
        return self.postgres_db

    def get_config(self, key: str, default: Any = None) -> Any:
        """Get a configuration value.

        Args:
            key: Configuration key
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        return self.config.get(key, default)

    def get_citation_engine(self) -> Any:
        """Lazily construct the CitationEngine bound to SRW's vector pool.

        The engine is async and performs all I/O on the shared vector-store
        pool (``srw_vector``, ``self.vector_db``); construction itself does no
        I/O, so this stays synchronous. Returns a cached instance.

        Raises:
            ImportError: If the citation_engine package is not importable.
            RuntimeError: If no vector-store pool is attached.
        """
        if self.citation_engine is None:
            from agent.citation_engine import CitationContext, CitationEngine

            if self.vector_db is None:
                raise RuntimeError(
                    "Citations require the vector store (srw_vector); no vector "
                    "pool is attached (VECTOR_POSTGRES_* unset)."
                )

            # Create context for audit trails using job_id as session
            ctx = CitationContext(
                session_id=self.job_id or "unknown",
                agent_id=self.config.get("agent_id", "unknown"),
            )
            self.citation_engine = CitationEngine(
                db=self.vector_db,
                context=ctx,
                verify_aux=self.verify_aux,
                verify_prompt=self.verify_citation_prompt,
                on_verdict=self.citation_verdict_callback,
            )

        return self.citation_engine

    def _normalize_anchor_key(self, path: str) -> str:
        """Normalize a local or workspace-relative citation path."""
        if self.workspace_manager is not None:
            return self.workspace_manager.workspace_relative_path(path)
        try:
            candidate = Path(path)
            if candidate.is_absolute():
                return str(candidate.resolve())
            # Workspace paths are POSIX-like on every backend, including flat
            # object-store keys. Do not anchor them to this pod's cwd.
            return posixpath.normpath(path)
        except (OSError, ValueError, RuntimeError):
            return str(path)

    def record_cloud_anchor(self, file_path: str, anchor: Dict[str, Any]) -> None:
        """Stash a cloud snapshot-anchor for a downloaded file (Phase 3, D7).

        Called by cloud read tools (e.g. ``webdav_read``) once a file lands in
        the workspace, so a later ``cite_*`` on that path can persist the anchor
        onto the source's ``metadata.cloud`` block. The key may be a real local
        path or a workspace-relative backend path; producers and consumers use
        the same normalized identity.
        """
        if not file_path or not anchor:
            return
        self._cloud_anchors[self._normalize_anchor_key(file_path)] = anchor

    def get_cloud_anchor(self, file_path: str) -> Optional[Dict[str, Any]]:
        """Return the stashed cloud anchor for a local or workspace path."""
        if not file_path:
            return None
        return self._cloud_anchors.get(self._normalize_anchor_key(file_path))

    def cloud_anchor_write_lock(self, file_path: str) -> asyncio.Lock:
        """Return the claim-local lock for one canonical workspace path.

        Datasource tools hold this across both the backend write and
        ``persist_cloud_anchor``. ``ToolContext`` is event-loop-local, so lock
        creation cannot interleave before the dictionary entry is published.
        """

        workspace_path = self._normalize_anchor_key(file_path)
        lock = self._cloud_anchor_write_locks.get(workspace_path)
        if lock is None:
            lock = asyncio.Lock()
            self._cloud_anchor_write_locks[workspace_path] = lock
        return lock

    async def persist_cloud_anchor(
        self,
        file_path: str,
        anchor: Dict[str, Any],
    ) -> None:
        """Record an anchor locally and await its optional durable sink.

        The callback seam lets persistent sessions bind a thread-scoped
        Postgres upsert without coupling datasource tools to the database.
        Callback errors propagate: a configured durable lane must not report a
        successful cloud read while silently dropping its provenance anchor.
        """
        workspace_path = self._normalize_anchor_key(file_path)
        self.record_cloud_anchor(workspace_path, anchor)
        callback = self.cloud_anchor_persist_callback
        if callback is not None:
            await callback(workspace_path, anchor)

    async def snapshot_cloud_source_bytes(
        self, file_path: str, anchor: Dict[str, Any]
    ) -> Optional[str]:
        """Persist a cited cloud file's original bytes to the snapshot store (D7).

        The agent holds no blob-store credentials, so the bytes are round-tripped
        through the orchestrator (``OrchestratorClient.save_citation_snapshot`` →
        ``POST /api/citations/snapshot``), which returns a content-addressed
        ``snapshot_blob_key``. The key is written back onto ``anchor`` in place so
        a re-cite of the same file doesn't re-upload, and so the source is
        registered with the key already present (Phase 3b).

        ``file_path`` may be a local path or a workspace-relative backend path.
        The latter is materialized with ``WorkspaceManager.local_copy`` before
        byte access. Best-effort: returns the key, or ``None`` when there's no
        orchestrator client, the file can't be read, or the upload fails — the
        extracted-text copy remains the citation's verification anchor either
        way.
        """
        if anchor.get("snapshot_blob_key"):
            return anchor["snapshot_blob_key"]
        client = self.orchestrator_client
        if client is None:
            return None

        def _read_bytes() -> bytes:
            if self.workspace_manager is not None:
                # Workspace identity is authoritative whenever a workspace is
                # bound.  A same-named file in the agent CWD/image must never
                # substitute for remote/virtual workspace bytes.
                workspace_path = self.workspace_manager.workspace_relative_path(
                    file_path
                )
                with self.workspace_manager.local_copy(workspace_path) as local_path:
                    return local_path.read_bytes()
            return Path(file_path).read_bytes()

        try:
            data = await asyncio.to_thread(_read_bytes)
        except Exception as e:
            logger.debug("Cloud snapshot read failed for %s: %s", file_path, e)
            return None
        content_type = anchor.get("content_type") or "application/octet-stream"
        try:
            key = await client.save_citation_snapshot(data, content_type=content_type)
        except Exception as e:  # never let a snapshot upload break citation creation
            logger.debug("Cloud snapshot upload failed for %s: %s", file_path, e)
            return None
        if key:
            anchor["snapshot_blob_key"] = key
        return key

    async def get_or_register_doc_source(
        self,
        file_path: str,
        name: Optional[str] = None,
        cloud_metadata: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Get cached source_id or register new document source.

        Checks the source registry first to avoid re-registering the same document.

        When the document was read from a user's cloud, ``cloud_metadata`` (the
        snapshot-anchor: drift fingerprint + best-effort live pointer) is stored
        on the new source's ``metadata.cloud`` block (Phase 3, D7). If not passed
        explicitly, any anchor previously stashed for this path via
        ``record_cloud_anchor`` is used.

        Args:
            file_path: Path to the document file
            name: Optional human-readable name for the source
            cloud_metadata: Optional cloud snapshot-anchor to persist on the source

        Returns:
            source_id for use in citations

        Raises:
            FileNotFoundError: If document doesn't exist
        """
        source_key = self._normalize_anchor_key(file_path)
        if source_key in self._source_registry:
            return self._source_registry[source_key]

        if cloud_metadata is None:
            cloud_metadata = self.get_cloud_anchor(source_key)

        metadata = {"cloud": cloud_metadata} if cloud_metadata else None

        engine = self.get_citation_engine()
        # The engine performs local filesystem I/O, while this value identifies
        # a workspace object.  Always materialize through the workspace backend:
        # host ``os.path.exists`` is not a locality signal and a CWD/image decoy
        # with the same relative name must not replace remote/virtual bytes.
        if self.workspace_manager is not None:
            with self.workspace_manager.local_copy(source_key) as local_path:
                source = await engine.add_doc_source(
                    str(local_path),
                    name=name or posixpath.basename(source_key),
                    metadata=metadata,
                )
        else:
            source = await engine.add_doc_source(
                file_path, name=name, metadata=metadata
            )
        self._source_registry[source_key] = source.id
        return source.id

    async def get_or_register_web_source(
        self,
        url: str,
        name: Optional[str] = None,
        content: Optional[str] = None,
    ) -> tuple[int, Optional[str]]:
        """Get cached source_id or register new web source.

        Checks the source registry first to avoid re-registering the same URL.
        Provider-returned ``content`` is archived without dereferencing ``url``.
        Legacy callers that omit content retain the citation engine's fetch
        behavior. If that fetch fails, the source is still registered with
        metadata only and a fetch_error is returned.

        Args:
            url: URL of the web source
            name: Optional human-readable name for the source
            content: Optional content already returned by an off-pod provider

        Returns:
            Tuple of (source_id, fetch_error). fetch_error is None if content
            was fetched successfully, or a string describing the error.
        """
        if url in self._source_registry:
            source_id = self._source_registry[url]
            fetch_error = self._inaccessible_sources.get(url)
            return source_id, fetch_error

        engine = self.get_citation_engine()
        source = await engine.add_web_source(url, name=name, content=content)
        self._source_registry[url] = source.id

        # Check if content was actually fetched
        fetch_error = None
        if source.metadata and source.metadata.get("fetch_error"):
            fetch_error = source.metadata["fetch_error"]
            self._inaccessible_sources[url] = fetch_error

        return source.id, fetch_error

    def save_web_content_to_disk(
        self,
        url: str,
        content: str,
        title: Optional[str] = None,
        source_id: Optional[int] = None,
    ) -> Optional[str]:
        """Save web content as a markdown file with YAML front-matter.

        Generates deterministic filenames from URL (same URL = same file).
        Skips write if file already exists (first save wins).

        Args:
            url: Source URL
            content: Raw text content to save
            title: Optional page title
            source_id: Optional CitationEngine source ID

        Returns:
            Workspace-relative path (e.g. "documents/external/example_com_a1b2c3d4.md"),
            or None if no workspace is available.
        """
        if not self.has_workspace():
            return None

        # Generate deterministic filename from URL
        url_hash = hashlib.sha256(url.encode()).hexdigest()[:8]
        parsed = urlparse(url)
        domain = parsed.netloc or "unknown"
        # Sanitize domain for filesystem
        safe_domain = re.sub(r"[^a-zA-Z0-9_.-]", "_", domain)
        filename = f"{safe_domain}_{url_hash}.md"
        relative_path = f"documents/external/{filename}"

        # Skip if file already exists (first save wins)
        if self.workspace_manager.exists(relative_path):
            return relative_path

        # Ensure directory exists (use backend, not local mkdir)
        self.workspace_manager.backend.mkdir("documents/external")

        # Build YAML front-matter
        front_matter_lines = [
            "---",
            f"url: {url}",
        ]
        if title:
            # Escape quotes in title for YAML
            safe_title = title.replace('"', '\\"')
            front_matter_lines.append(f'title: "{safe_title}"')
        front_matter_lines.append(
            f"fetched_at: {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}"
        )
        if source_id is not None:
            front_matter_lines.append(f"source_id: {source_id}")
        front_matter_lines.append("---")
        front_matter_lines.append("")

        file_content = "\n".join(front_matter_lines) + content

        try:
            self.workspace_manager.write_file(relative_path, file_content)
            logger.debug(f"Saved web content to {relative_path}")
        except Exception as e:
            logger.warning(f"Failed to save web content to disk: {e}")
            return None

        return relative_path

    def queue_memory(
        self,
        content: str,
        keywords: Optional[List[str]] = None,
        importance: float = 0.5,
        source: str = "observer",
        source_phase: Optional[int] = None,
        memory_type: str = "factual",
    ) -> None:
        """Queue a memory for async storage by the graph's audited_tools node.

        This is sync-safe — can be called from ``@tool`` functions which run
        synchronously. The actual async ``RecallStore.store()`` happens when
        ``drain_pending_memories()`` is called from the async graph node.

        Args:
            content: Memory content text
            keywords: Keywords for sparse search
            importance: Importance score 0-1
            source: Memory source (todo, compaction, phase_archive, tool_error)
            source_phase: Phase number when memory was created
            memory_type: factual, procedural, error_solution, vocabulary, relational
        """
        self._pending_memories.append(
            {
                "content": content,
                "keywords": keywords,
                "importance": importance,
                "source": source,
                "source_phase": source_phase,
                "memory_type": memory_type,
            }
        )

    def drain_pending_memories(self) -> List[Dict[str, Any]]:
        """Return and clear all pending memories.

        Called from async graph nodes to flush queued memories into RecallStore.

        Returns:
            List of memory dicts ready for RecallStore.store(**mem)
        """
        pending = self._pending_memories[:]
        self._pending_memories.clear()
        return pending

    def request_freeze(self, freeze_data: Dict[str, Any]) -> None:
        """Request a job freeze from a tool (e.g., blocking send_message).

        The freeze is consumed by the graph's audited_tools node after
        tool execution completes. This is sync-safe — can be called from
        ``@tool`` functions.

        Args:
            freeze_data: Freeze metadata dict (freeze_type, thread_id, etc.)
        """
        self._freeze_request = freeze_data

    def consume_freeze_request(self) -> Optional[Dict[str, Any]]:
        """Return and clear any pending freeze request.

        Called from the async audited_tools graph node after tool execution.

        Returns:
            Freeze data dict if a freeze was requested, None otherwise.
        """
        req = self._freeze_request
        self._freeze_request = None
        return req

    def request_replan(self, reason: str) -> None:
        """Ask for the strategic phase early, without discarding todo state.

        Called from ``request_replan``. Sync-safe; consumed by ``check_todos``.
        """
        self._replan_request = reason

    def consume_replan_request(self) -> Optional[str]:
        """Return and clear any pending replan request."""
        reason = self._replan_request
        self._replan_request = None
        return reason

    def request_reply_drain(self) -> None:
        """Mark a natural break at which queued replies may be delivered.

        Called from ``todo_complete``: a finished todo is the break the old
        ``next_strategic_phase`` default was really trying to express — finish
        the current unit of work, then read your mail. Sync-safe.
        """
        self._reply_drain_requested = True

    def consume_reply_drain(self) -> bool:
        """Return and clear the natural-break flag.

        Called from the async audited_tools node after tool execution.
        """
        requested = self._reply_drain_requested
        self._reply_drain_requested = False
        return requested

    def request_officer_sleep(self, sleep_data: Dict[str, Any]) -> None:
        """Record the officer sleep tool's wake request (sync-safe).

        The turn loop PEEKS this after the tool batch to end the turn instead
        of paying another LLM iteration; the transport CONSUMES it at park
        time to file the durable wake with the orchestrator
        (knowledge-base/knowledge/features/centurion.md §4).
        """
        self._officer_sleep_request = sleep_data

    def peek_officer_sleep(self) -> Optional[Dict[str, Any]]:
        """Non-destructive read of a pending officer sleep request."""
        return self._officer_sleep_request

    def consume_officer_sleep(self) -> Optional[Dict[str, Any]]:
        """Return and clear any pending officer sleep request."""
        req = self._officer_sleep_request
        self._officer_sleep_request = None
        return req

    def close_citation_engine(self) -> None:
        """Drop the cached CitationEngine reference.

        The engine no longer owns a DB connection — it borrows the agent's
        shared vector pool (``vector_db``), which the agent closes on shutdown.
        So there is nothing to close here; just release the reference and clear
        the per-job source cache.
        """
        if self.citation_engine is not None:
            self.citation_engine = None
            self._source_registry.clear()

    def record_file_read(self, path: str, content: str | bytes | None = None) -> None:
        """Record that a file was read, optionally with its full-text version.

        This is called by read_file to track which files have been recently
        accessed. The tracking window is limited to the last N reads (default 10).
        Callers that only need path-based instruction enforcement may omit
        ``content``; text ``read_file`` calls pass the complete bytes so later
        writes can detect an out-of-band change even if an invalidation event
        was missed.

        Args:
            path: Path to the file that was read
            content: Complete text content observed by the reader, when available
        """
        normalized = path.lstrip("/").strip()
        content_version = None
        if content is not None:
            raw = content.encode("utf-8") if isinstance(content, str) else content
            content_version = "sha256:" + hashlib.sha256(raw).hexdigest()
        # Instruction files are pinned: unrelated reads must not make a
        # job-scoped gate look unread merely because the 10-entry FIFO cycled.
        # Phase/freshness-scoped gates use the independent stamp below.
        if self._is_instruction_path(normalized):
            self._pinned_reads.add(normalized)
            stamp: Dict[str, Any] = {
                "phase": self._current_phase,
                "phase_number": self._current_phase_number,
                "turn_count": self._current_turn_count,
            }
            if content_version is not None:
                stamp["content_version"] = content_version
            self._instruction_read_stamps[normalized] = stamp
        evicted = None
        # Remove if already present (we'll re-add at the end)
        if normalized in self._recent_reads:
            self._recent_reads.remove(normalized)
        elif (
            self._recent_reads.maxlen is not None
            and self._recent_reads.maxlen > 0
            and len(self._recent_reads) >= self._recent_reads.maxlen
        ):
            evicted = self._recent_reads[0]
        self._recent_reads.append(normalized)
        if evicted is not None:
            self._recent_read_versions.pop(evicted, None)
        if normalized not in self._recent_reads:
            # A deque configured with maxlen=0 cannot retain path or version.
            self._recent_read_versions.pop(normalized, None)
        elif content is None:
            # Preserve the legacy path-only contract for instruction files and
            # other callers that do not have authoritative full text.
            self._recent_read_versions.pop(normalized, None)
        else:
            self._recent_read_versions[normalized] = content_version

    def _is_instruction_path(self, normalized: str) -> bool:
        """Whether a normalized path is a configured instruction file."""
        for entry in self._instruction_files:
            entry_path = (getattr(entry, "path", "") or "").lstrip("/").strip()
            if entry_path and entry_path == normalized:
                return True
        return False

    def was_recently_read(self, path: str) -> bool:
        """Check if file was read within the tracking window.

        Used by edit_file and write_file to enforce read-before-write
        discipline. Instruction files stay present once pinned regardless of
        how many reads followed. Binding-specific phase/freshness checks happen
        separately in instruction_read_is_valid().

        Args:
            path: Path to check

        Returns:
            True if the file was recently read, False otherwise
        """
        normalized = path.lstrip("/").strip()
        return normalized in self._recent_reads or normalized in self._pinned_reads

    def recent_read_matches(self, path: str, content: str | bytes) -> bool:
        """Check path recency and any recorded full-text content version.

        Path-only records deliberately remain visible to
        :meth:`was_recently_read` for instruction-file enforcement, but cannot
        authorize a text write. ``read_file`` must have recorded a version and
        the current full text must still match it.
        """

        normalized = path.lstrip("/").strip()
        if normalized not in self._recent_reads:
            return False
        expected = self._recent_read_versions.get(normalized)
        if expected is None:
            return False
        raw = content.encode("utf-8") if isinstance(content, str) else content
        current = "sha256:" + hashlib.sha256(raw).hexdigest()
        return current == expected

    def invalidate_recent_read(self, path: str) -> bool:
        """Forget a file read after an out-of-band user edit.

        Returns whether the normalized path was present. Keeping this as a
        public ToolContext operation prevents transports from mutating the
        private deque and makes read-before-write enforcement immediately
        require a fresh agent read.
        """

        normalized = path.lstrip("/").strip()
        present = normalized in self._recent_reads or normalized in self._pinned_reads
        if normalized in self._recent_reads:
            self._recent_reads.remove(normalized)
        self._pinned_reads.discard(normalized)
        self._recent_read_versions.pop(normalized, None)
        self._instruction_read_stamps.pop(normalized, None)
        return present

    def export_instruction_read_receipts(self) -> Dict[str, Dict[str, Any]]:
        """Return safe instruction-read receipts for a worker checkpoint.

        Only configured instruction paths are exported. Ordinary recent-file
        reads and their write-authorizing versions remain claim-local: carrying
        those across a handoff could authorize a stale edit. The optional
        content version here is used solely to reject a receipt when the
        instruction changed between images/claims.
        """

        receipts: Dict[str, Dict[str, Any]] = {}
        for path, stamp in self._instruction_read_stamps.items():
            if path not in self._pinned_reads or not self._is_instruction_path(path):
                continue
            receipt: Dict[str, Any] = {
                "phase": stamp.get("phase"),
                "phase_number": stamp.get("phase_number"),
                "turn_count": int(stamp.get("turn_count") or 0),
            }
            content_version = stamp.get("content_version")
            if content_version:
                receipt["content_version"] = content_version
            receipts[path] = receipt
        return receipts

    def restore_instruction_read_receipts(self, value: Any) -> int:
        """Hydrate checkpointed instruction receipts into this claim.

        Invalid paths/shapes and receipts for changed instruction content are
        ignored fail-closed. This restores only enforcement visibility and its
        phase/turn stamp; it never restores ``_recent_reads`` or
        ``_recent_read_versions``, so read-before-write authorization cannot
        cross a worker lease.
        """

        if not isinstance(value, dict):
            return 0

        configured_paths = {
            (getattr(entry, "path", "") or "").lstrip("/").strip()
            for entry in self._instruction_files
        }
        configured_paths.discard("")
        for path in configured_paths:
            if path in self._recent_reads:
                self._recent_reads.remove(path)
            self._pinned_reads.discard(path)
            self._recent_read_versions.pop(path, None)
            self._instruction_read_stamps.pop(path, None)

        restored = 0
        for raw_path, raw_receipt in value.items():
            path = str(raw_path or "").lstrip("/").strip()
            if path not in configured_paths or not isinstance(raw_receipt, dict):
                continue
            phase = raw_receipt.get("phase")
            phase_number = raw_receipt.get("phase_number")
            turn_count = raw_receipt.get("turn_count")
            if phase is not None and not isinstance(phase, str):
                continue
            if phase_number is not None and (
                isinstance(phase_number, bool) or not isinstance(phase_number, int)
            ):
                continue
            if (
                isinstance(turn_count, bool)
                or not isinstance(turn_count, int)
                or turn_count < 0
            ):
                continue

            content_version = raw_receipt.get("content_version")
            if content_version is not None:
                if not (
                    isinstance(content_version, str)
                    and content_version.startswith("sha256:")
                ):
                    continue
                try:
                    content = self.workspace_manager.read_file(path)
                except Exception:
                    continue
                raw = content.encode("utf-8") if isinstance(content, str) else content
                current_version = "sha256:" + hashlib.sha256(raw).hexdigest()
                if current_version != content_version:
                    continue

            self._pinned_reads.add(path)
            stamp: Dict[str, Any] = {
                "phase": phase,
                "phase_number": phase_number,
                "turn_count": turn_count,
            }
            if content_version is not None:
                stamp["content_version"] = content_version
            self._instruction_read_stamps[path] = stamp
            restored += 1
        return restored

    def get_read_tracking_limit(self) -> int:
        """Get the tracking window size from config or default.

        Returns:
            Number of recent reads to track (default 10)
        """
        return self.get_config("read_tracking_limit", 10)

    def instruction_entry_applies(self, entry: Any) -> bool:
        """Whether an enforced binding applies in the current phase kind."""
        phases = getattr(entry, "phases", None)
        return not phases or self._current_phase in phases

    def instruction_read_is_valid(self, entry: Any) -> bool:
        """Whether an instruction read satisfies one enforced binding.

        Ordinary bindings retain the historical job-scoped pin. Bindings may
        additionally require a read in the current concrete phase instance and
        may expire after a bounded number of LLM turns.
        """
        path = (entry.path or "").lstrip("/").strip()
        if not self.was_recently_read(path):
            return False

        read_scope = getattr(entry, "read_scope", "job")
        max_age = getattr(entry, "max_read_age_turns", None)
        if read_scope == "job" and max_age is None:
            return True

        stamp = self._instruction_read_stamps.get(path)
        if stamp is None:
            return False
        if read_scope == "phase" and (
            stamp.get("phase") != self._current_phase
            or stamp.get("phase_number") != self._current_phase_number
        ):
            return False
        if max_age is not None:
            age = self._current_turn_count - int(stamp.get("turn_count", 0))
            if age < 0 or age > max_age:
                return False
        return True

    def get_enforcement_entries(self, tool_name: str) -> List[Any]:
        """Get active enforced instruction bindings for a tool."""
        return [
            entry
            for entry in self._instruction_files
            if entry.enforce
            and entry.trigger_type == "before_tool"
            and entry.trigger_target == tool_name
            and self.instruction_entry_applies(entry)
        ]

    def get_enforcement_files(self, tool_name: str) -> List[str]:
        """Get instruction files that must be read before using a tool.

        Checks instruction_files config for entries with trigger type
        'before_tool' matching the given tool name and enforce=True.

        Args:
            tool_name: Name of the tool being called

        Returns:
            List of workspace-relative file paths that must be read first
        """
        return [entry.path for entry in self.get_enforcement_entries(tool_name)]

    def check_tool_enforcement(self, tool_name: str) -> Optional[str]:
        """Check if a tool's instruction file enforcement requirements are met.

        Returns an error message if any required instruction files have not
        been recently read. Returns None if all requirements are met.

        Args:
            tool_name: Name of the tool being called

        Returns:
            Error message string if enforcement fails, None if OK
        """
        for entry in self.get_enforcement_entries(tool_name):
            if not self.instruction_read_is_valid(entry):
                from shared.runtime.services.guardrails import format_nudge

                model = self._llm_config.model if self._llm_config is not None else None
                return format_nudge(
                    "read_file_required_error",
                    model=model,
                    file_path=entry.path,
                    tool_name=tool_name,
                )
        return None

    def get_phase_instruction_files(self, phase: str) -> List[Any]:
        """Get instruction files eligible for once-only phase-start delivery.

        Returns ``phase_start`` entries plus the legacy ``phase`` alias. The
        graph's checkpoint ledger suppresses subsequent delivery in the same
        concrete phase instance.

        Args:
            phase: Phase name ('strategic' or 'tactical')

        Returns:
            List of InstructionFileEntry objects for this phase
        """
        return [
            entry
            for entry in self._instruction_files
            if entry.trigger_type in {"phase", "phase_start"}
            and entry.trigger_target == phase
        ]

    def set_current_phase(
        self,
        phase: str,
        phase_number: Optional[int] = None,
        turn_count: Optional[int] = None,
    ) -> None:
        """Set the current execution position for phase-aware behavior.

        Args:
            phase: Phase name ("strategic" or "tactical")
            phase_number: Concrete phase-instance number, when available
            turn_count: Current checkpointed LLM-turn counter, when available
        """
        self._current_phase = phase
        self._current_phase_number = phase_number
        if turn_count is not None:
            self._current_turn_count = turn_count

    def get_phase_multimodal(self) -> bool:
        """Get the effective multimodal setting for the running model.

        One model runs every phase (U1), so with an LLMConfig attached and a
        phase set this is simply ``llm.multimodal``. Otherwise falls back to
        the static tool-config value (which the worker/session wiring seeds
        from the same field).

        Returns:
            True if the model supports multimodal input
        """
        if self._llm_config is not None and self._current_phase is not None:
            return self._llm_config.multimodal
        return self.get_config("multimodal", False)

    # ── Browser session lifecycle ────────────────────────────────────

    def should_include_screenshots(self) -> bool:
        """Whether browser tools should include screenshots in results.

        Resolves the 'auto' setting: true if the current model is multimodal.
        """
        browser_cfg = self.config.get("browser", {})
        snapshot_cfg = browser_cfg.get("snapshot", {})
        setting = snapshot_cfg.get("include_screenshot", "auto")
        if setting == "auto":
            return self.get_phase_multimodal()
        return bool(setting)

    def get_max_dom_chars(self) -> int:
        """Max DOM text characters to return from browser tools."""
        browser_cfg = self.config.get("browser", {})
        snapshot_cfg = browser_cfg.get("snapshot", {})
        return snapshot_cfg.get("max_dom_chars", 40000)

    async def browser_exec(self, action: str, **args: Any) -> Dict[str, Any]:
        """Run one browser action on the workspace via the browser-exec helper.

        Drives Chromium on the workspace over SSH rather than speaking CDP
        across the pod boundary. The workspace daemon holds the persistent
        session so element refs survive between calls. Stateless workspaces
        route the command through the same exact-claim resource fence as other
        workspace-resident daemons; the base implementation preserves the
        historical unfenced behavior for pinned/custom backends. Returns the
        parsed JSON result, or an ``{"error": ...}`` dict on any failure.

        See knowledge-base/knowledge/features/browser_workspace_executor.md.
        """
        import asyncio
        import json as _json
        import shlex

        if not self.has_workspace():
            return {
                "error": (
                    "browser tools require a workspace running the "
                    "browser-exec daemon; no workspace backend is attached"
                )
            }

        backend = self.workspace_manager.backend
        payload = _json.dumps(args)
        cmd = f"browser-exec {shlex.quote(action)} --json {shlex.quote(payload)}"
        try:
            # Workspace command execution is blocking; keep the event loop
            # responsive.  Every production WorkspaceBackend exposes the
            # claim-resource seam.  The callable fallback retains support for
            # older/custom duck-typed backends without silently bypassing the
            # stateless RemoteBackend fence.
            claim_exec = getattr(backend, "exec_claim_resource", None)
            if callable(claim_exec):
                out = await asyncio.to_thread(
                    claim_exec,
                    cmd,
                    200,
                    operation=f"browser-exec {action}",
                )
            else:
                out = await asyncio.to_thread(backend.exec_command, cmd, 200)
        except Exception as e:
            return {"error": f"browser-exec call failed: {e}"}

        out = (out or "").strip()
        if not out:
            return {"error": "browser-exec returned no output"}
        try:
            # The client prints exactly one JSON line to stdout.
            return _json.loads(out.splitlines()[-1])
        except Exception:
            return {"error": f"browser-exec returned non-JSON: {out[:500]}"}

    async def close_browser(self, *, strict: bool = False) -> None:
        """Shut down the workspace browser-exec daemon.

        Normal tool cleanup retains the historical best-effort behavior.
        Terminal stateless retirement passes ``strict=True`` and requires the
        workspace client to attest that its daemon and exact-profile Chromium
        processes are absent before snapshot/release may continue.
        """
        if not self.has_workspace():
            return
        try:
            result = await self.browser_exec("shutdown")
            if strict and (
                not isinstance(result, dict)
                or result.get("ok") is not True
                or result.get("shutdown_complete") is not True
            ):
                raise RuntimeError("browser-exec shutdown was not acknowledged")
        except Exception as e:
            if strict:
                raise
            logger.debug(f"browser-exec shutdown failed: {e}")
        logger.info("Browser cleaned up")
