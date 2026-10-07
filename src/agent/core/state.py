"""Universal Agent State.

Defines the state structure for the nested loop graph architecture.
The state supports:
- Initialization flow (runs once at job start)
- Outer loop (strategic planning at phase transitions)
- Inner loop (tactical execution with todos)

Persistence across context compaction comes from the memory/knowledge
systems and file-based artifacts (plan.md, notes/), while state fields
control loop flow.
"""

from typing import Annotated, Any, Dict, List, Mapping, Optional
from urllib.parse import urlsplit, urlunsplit

from typing_extensions import TypedDict
from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages


# Hidden dispatch fields that are pure credential transport. Their consumers
# pop them before the graph starts; dropping them here is the backstop.
_CREDENTIAL_METADATA_KEYS = ("managed_repository_credentials",)
# Metadata lists whose entries carry a plaintext ``credentials`` object (Git
# tokens, SSH keys, env values, credential-file contents) and a URL that may
# embed userinfo.
_CREDENTIAL_ENTRY_URLS = {"datasources": "connection_url", "repositories": "repo_url"}


def _url_without_userinfo(value: Any) -> Any:
    """Drop ``user:secret@`` from a URL; a malformed one is dropped whole."""
    if not isinstance(value, str) or "@" not in value:
        return value
    try:
        parts = urlsplit(value)
        if not parts.scheme or "@" not in parts.netloc:
            return value
        netloc = parts.netloc.rpartition("@")[2]
        return urlunsplit(parts._replace(netloc=netloc))
    except ValueError:
        return ""


def checkpoint_safe_metadata(
    metadata: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Return job metadata without its connector credentials.

    Graph state is persisted by the checkpointer (shared Postgres by default
    and always for stateless workers, pod-local SQLite in the legacy mode), so
    secret material must never become state. Each ``datasources`` and ``repositories`` entry
    loses its ``credentials`` and the userinfo of its URL; the other fields
    stay. The runtime reads credentials from the dispatch payload it holds in
    process memory (``UniversalAgent._job_metadata``), never from state, and
    every resume or claim delivers that payload again. The input is not
    mutated: the runtime shares its entry dicts.
    """
    if not metadata:
        return {}
    safe = {
        key: value
        for key, value in metadata.items()
        if key not in _CREDENTIAL_METADATA_KEYS
    }
    for list_key, url_key in _CREDENTIAL_ENTRY_URLS.items():
        entries = safe.get(list_key)
        if not isinstance(entries, list):
            continue
        cleaned = []
        for entry in entries:
            if isinstance(entry, Mapping):
                entry = {k: v for k, v in entry.items() if k != "credentials"}
                if url_key in entry:
                    entry[url_key] = _url_without_userinfo(entry[url_key])
            cleaned.append(entry)
        safe[list_key] = cleaned
    return safe


class CompletionReportPayload(TypedDict):
    """Exact operation payload persisted for one completion-report retry set."""

    should_stop: bool
    goal_achieved: bool
    error: Optional[Dict[str, Any]]
    freeze_data: Optional[Dict[str, Any]]


class UniversalAgentState(TypedDict):
    """State for nested loop graph architecture.

    The Universal Agent uses a nested loop structure:
    1. Initialization: Set up workspace, read instructions, create plan
    2. Outer loop: Read plan, update memory, create todos for phase
    3. Inner loop: Execute todos until phase complete
    4. Goal check: Continue outer loop or end

    File-based context:
    - memory system + knowledge base: long-term memory, injected each call
    - plan.md: Strategic direction, read at phase transitions
    - archive/: Completed todos by phase

    Attributes:
        messages: Conversation history with automatic deduplication
        job_id: Unique identifier for the current job
        workspace_path: Path to the job's workspace directory

        # Loop control (nested loop architecture)
        initialized: Whether initialization has completed
        phase_complete: Inner loop exit condition (all todos done)
        goal_achieved: Outer loop exit condition (plan complete)
        iteration: Current iteration count (for tracking/logging)

        # Phase alternation (strategic/tactical mode)
        is_strategic_phase: True = strategic mode (planning), False = tactical mode (execution)
        phase_number: Increments at each phase transition (for tracking/logging)
        is_final_phase: True when job_complete was called, job completes when todos done
        turn_count: LLM call counter, used by memory extraction to trigger every N turns
        phase_instruction_injections: Checkpointed once-per-phase delivery ledger
            ("<n>:<phase>:<path>") of the phase_start instruction blocks; the
            block itself is a protected HumanMessage in ``messages`` (see
            src/shared/runtime/core/message_markers.py) — the ledger records delivery, the
            execute node checks presence and self-heals a ledger-only entry
        last_observed_turn: Last turn when memory extraction ran (for interval tracking)
        delivered_reply_keys: Durable identities of queued replies already
            appended to message history (worker handoff dedup)
        delivered_guidance_ids: Guidance entries absorbed by an execute-node
            response checkpoint
        delivered_feedback_keys: Queued-feedback generations absorbed on resume
        delivered_delegation_keys: Delegation-result generations absorbed on resume
        instruction_read_receipts: Checkpointed instruction-file read receipts
            used to preserve phase/freshness enforcement across worker claims

        # File-based context
        workspace_memory: Legacy; unused (workspace.md removed), always ""

        # Execution control
        error: Error information if something went wrong
        should_stop: Flag to signal workflow termination
        consecutive_llm_errors: Count of consecutive LLM failures
        client_report_id: Random idempotency key minted once for a genuine stop
        completion_report_payload: Exact four-field completion operation payload

        # Stateless worker batch budget (missing/None means unarmed)
        worker_batch_started_at: Epoch timestamp when this claim began
        worker_batch_start_iteration: Execute-iteration value at claim start
        worker_batch_target_wall_seconds: Preferred wall-clock batch duration
        worker_batch_min_wall_seconds: Effective floor for this claim's target
        worker_batch_iteration_cap: Optional execute-iteration delta cap
        worker_resume_id: Durable explicit-resume generation applied to this
            checkpoint, preventing an ambiguous paused report from resuming

        # Job metadata
        metadata: Job-specific data (document_path, prompt, etc.)

        # Context management
        context_stats: Token counts and compaction statistics
        tool_retry_state: Failed tool call tracking

        # Legacy (for backwards compatibility)
        phase_transition: Old phase transition state
    """

    # Core LangGraph state - messages are automatically merged/deduped
    messages: Annotated[List[BaseMessage], add_messages]

    # Job identification
    job_id: str
    workspace_path: str

    # Loop control (nested loop architecture)
    initialized: bool  # Has initialization completed
    phase_complete: bool  # Inner loop exit: all todos done
    goal_achieved: bool  # Outer loop exit: plan complete
    iteration: int  # Current iteration count

    # Phase alternation (strategic/tactical mode)
    is_strategic_phase: bool  # True = strategic mode, False = tactical mode
    phase_number: int  # Increments at each phase transition
    is_final_phase: bool  # True when job_complete called, awaiting todo completion

    # Finalization-decision mirrors (journal-before-observe). Written by the
    # audited tool node AFTER the decision was durably journaled through the
    # orchestrator, so any checkpoint containing the tool result also carries
    # the decision. Mirrors of the durable record, never the source of truth:
    # jobs.context.completion_decision / the target's verification ledger are.
    # Nulled on feedback resume — a new round voids the old decision.
    completion_decision: Optional[Dict[str, Any]]  # journaled job_complete payload
    verdict_decision: Optional[Dict[str, Any]]  # journaled critic verdict (cache)

    # Exactly-once guard for archive_phase: "<phase_number>:<strategic|tactical>"
    # of the last archived phase instance. A transition rejection retries the
    # completion; this stops the retry re-archiving the same boundary.
    last_archived_phase: Optional[str]
    turn_count: int  # LLM call counter (for memory extraction interval)
    phase_instruction_injections: List[str]  # Once-per-phase delivery ledger
    last_observed_turn: int  # Last turn when memory extraction ran
    last_assembled_turn: int  # Last turn when memory assembler ran
    delivered_reply_keys: List[str]  # Checkpoint-coupled queued-reply dedup
    delivered_guidance_ids: List[str]  # Checkpoint-coupled urgent guidance
    delivered_feedback_keys: List[str]  # Checkpoint-coupled resume feedback
    delivered_delegation_keys: List[str]  # Checkpoint-coupled child results
    instruction_read_receipts: Dict[str, Dict[str, Any]]

    # File-based context (read from workspace into state)
    workspace_memory: str  # Legacy; unused (workspace.md removed), always ""

    # Execution control
    error: Optional[Dict[str, Any]]
    should_stop: bool
    consecutive_llm_errors: int

    # Completion report retry envelope. A graph node writes both immediately
    # before END, so a successor re-reporting the durable checkpoint reuses the
    # same random identity and exact operation payload. Genuine resume nodes
    # clear both before any new work can produce another stop.
    client_report_id: Optional[str]
    completion_report_payload: Optional[CompletionReportPayload]

    # Stateless worker batches are armed explicitly by their claim driver.
    # Keeping these fields in the checkpoint makes the boundary decision
    # restart-safe; None preserves legacy/session behavior and old checkpoints.
    worker_batch_started_at: Optional[float]
    worker_batch_start_iteration: Optional[int]
    worker_batch_target_wall_seconds: Optional[float]
    worker_batch_min_wall_seconds: Optional[float]
    worker_batch_iteration_cap: Optional[int]
    worker_resume_id: Optional[str]

    # Job metadata (flexible, agent-type specific)
    # For Creator: document_path, prompt, etc.
    # For Validator: requirement_id, requirement_data, etc.
    metadata: Dict[str, Any]

    # Context management state
    # Tracks token counts, compaction operations, summaries generated
    context_stats: Optional[Dict[str, Any]]

    # Tool retry state
    # Tracks failed tool calls and retry attempts
    tool_retry_state: Optional[Dict[str, Any]]

    # Phase transition state (legacy, for backwards compatibility)
    # Set when a phase transition is triggered by todo_complete or request_replan
    # Contains: transition_type, trigger_summarization, metadata
    phase_transition: Optional[Dict[str, Any]]

    # Why the tactical phase asked to end early (request_replan tool).
    # Set by check_todos when it consumes the request, read by handle_transition
    # so the incoming strategic phase is told what changed. Cleared at the
    # transition — a stale reason must not steer a later phase.
    replan_reason: Optional[str]

    # Todo persistence (for checkpoint/resume)
    # These fields sync TodoManager state to LangGraph checkpoints
    todos: Optional[List[Dict[str, Any]]]
    staged_todos: Optional[List[Dict[str, Any]]]
    todo_next_id: Optional[int]

    # Freeze/completion data for orchestrator
    # Set by handle_transition when finalize_job/freeze_for_review produces freeze_data.
    # Flows through the graph state → report_completion() → orchestrator.
    freeze_data: Optional[Dict[str, Any]]

    # Resume from feedback
    # Set via aupdate_state when resuming a frozen job with --feedback
    # Consumed by restore_from_feedback node, then cleared
    resume_feedback: Optional[str]

    # Why the job was resumed with feedback (critic return, supervisor
    # escalation, reviewer feedback, ...). Free-form; rendered verbatim in
    # the [FEEDBACK_RESUME] banner. Set alongside resume_feedback, consumed
    # by restore_from_feedback, then cleared.
    resume_reason: Optional[str]


def create_initial_state(
    job_id: str,
    workspace_path: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> UniversalAgentState:
    """Create an initial state for a new job.

    The state is initialized for the nested loop graph:
    - initialized=False: Triggers initialization flow
    - phase_complete=False: Inner loop will run
    - goal_achieved=False: Outer loop will continue
    - workspace_memory="": Legacy field, no longer populated

    Args:
        job_id: Unique job identifier
        workspace_path: Path to job workspace
        metadata: Optional job-specific data. Stored through
            :func:`checkpoint_safe_metadata`, so connector credentials never
            reach the checkpoint.

    Returns:
        Initial UniversalAgentState ready for graph invocation

    Example:
        ```python
        state = create_initial_state(
            job_id="abc123",
            workspace_path="job_abc123",
            metadata={"document_path": "/data/doc.pdf"}
        )
        result = await graph.ainvoke(state)
        ```
    """
    return UniversalAgentState(
        # Core
        messages=[],
        job_id=job_id,
        workspace_path=workspace_path,
        # Loop control
        initialized=False,
        phase_complete=False,
        goal_achieved=False,
        iteration=0,
        # Phase alternation (start in strategic mode)
        is_strategic_phase=True,
        phase_number=1,
        is_final_phase=False,
        completion_decision=None,
        verdict_decision=None,
        last_archived_phase=None,
        turn_count=0,
        phase_instruction_injections=[],
        last_observed_turn=0,
        last_assembled_turn=0,
        delivered_reply_keys=[],
        delivered_guidance_ids=[],
        delivered_feedback_keys=[],
        delivered_delegation_keys=[],
        instruction_read_receipts={},
        # File-based context
        workspace_memory="",
        # Execution control
        error=None,
        should_stop=False,
        consecutive_llm_errors=0,
        client_report_id=None,
        completion_report_payload=None,
        # Worker batch budget (default-unarmed)
        worker_batch_started_at=None,
        worker_batch_start_iteration=None,
        worker_batch_target_wall_seconds=None,
        worker_batch_min_wall_seconds=None,
        worker_batch_iteration_cap=None,
        worker_resume_id=None,
        # Metadata
        metadata=checkpoint_safe_metadata(metadata),
        # Context management
        context_stats=None,
        tool_retry_state=None,
        # Legacy
        phase_transition=None,
        # Todo persistence
        todos=None,
        staged_todos=None,
        todo_next_id=1,
        # Freeze/completion data
        freeze_data=None,
        # Resume from feedback
        resume_feedback=None,
        resume_reason=None,
    )
