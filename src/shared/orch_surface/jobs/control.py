"""Canonical job-control handlers."""

from __future__ import annotations

from typing import Any

from shared.expert_reference import ExpertReferenceConflict, resolve_expert_selection
from shared.orch_surface import formatters as fmt
from shared.orch_surface.client import AsyncCockpitClient
from shared.orch_surface.jobs._utils import (
    format_action_error,
    resolve_job_id,
    short_id,
    transport_key_paths,
)
from shared.orch_surface.jobs.descriptors import CallerCtx, descriptor
from shared.orch_surface.jobs.envelope import (
    friendly_reason,
    http_status_of,
    response_detail,
)
from shared.orch_surface.workspace_choice import (
    WorkspaceArgumentError,
    workspace_field,
)

_ALL = frozenset({"mcp", "session", "officer"})
_MCP = frozenset({"mcp"})
_MCP_OFFICER = frozenset({"mcp", "officer"})
_EXPLICIT_GATE = (
    "named explicitly by the caller configuration; lane defaults come from the "
    "descriptor caller-default policy (officer_supervision_surface E2)"
)
_SERVER_OWNED_OFFICER_CONTEXT_KEYS = frozenset(
    {
        "ticket_note_id",
        "officer_admission",
        "ticket_ready_at",
        "ready_generation_at",
        "ticket_claim_source",
        "claim_source",
        "officer_thread_id",
        "officer_incarnation",
    }
)
_SERVER_OWNED_CREATE_CONTEXT_KEYS = _SERVER_OWNED_OFFICER_CONTEXT_KEYS | {
    "evidence_manifest",
    "pull_request",
    "required_deliverables",
    "deliverable_contract_provenance",
    "prior_deliverable_contract",
    "required_pr_repositories",
}


@descriptor(group="job_control", plane="job_control", caller_defaults=_ALL)
async def approve_job(
    client: AsyncCockpitClient, caller: CallerCtx, job_id: str
) -> str:
    """Approve a frozen job, marking it as completed.

    MUTATION: This marks the job as completed, writes job_completion.json,
    and deletes job_frozen.json. The job must be in 'pending_review' status.
    This action cannot be undone.

    Args:
        job_id: Job UUID to approve

    Returns:
        Approval result with completion details
    """
    try:
        resolved = await resolve_job_id(client, caller, job_id)
        result = await client.approve_job(resolved)
        return fmt.format_action_result("approve", resolved, result)
    except Exception as error:
        return format_action_error("approve", job_id, error)


@descriptor(group="job_control", plane="job_control", caller_defaults=_ALL)
async def resume_job_with_feedback(
    client: AsyncCockpitClient,
    caller: CallerCtx,
    job_id: str,
    feedback: str | None = None,
) -> str:
    """Resume a frozen/failed job from its checkpoint, optionally injecting feedback.

    MUTATION — and with feedback, DESTRUCTIVE: the worker force-compacts its
    conversation context, archives its in-flight todos, and re-plans from
    scratch against the feedback. Use it when the plan itself is wrong; for a
    mid-run course correction use send_message_to_job (non-destructive, lands
    in the worker's next LLM turn with urgent=true). Completed jobs and jobs
    terminalized as blocked/undelivered cannot be resumed. If the originally
    assigned agent is unavailable, the orchestrator auto-selects a ready agent.

    Args:
        job_id: Job UUID to resume
        feedback: Natural language feedback to inject into the agent's context

    Returns:
        Resume result with status
    """
    try:
        resolved = await resolve_job_id(client, caller, job_id)
        result = await client.resume_job(resolved, feedback=feedback)
        return fmt.format_action_result("resume", resolved, result, feedback=feedback)
    except Exception as error:
        return format_action_error("resume", job_id, error)


@descriptor(group="job_control", plane="job_control", caller_defaults=_ALL)
async def cancel_job(client: AsyncCockpitClient, caller: CallerCtx, job_id: str) -> str:
    """Cancel a running job.

    MUTATION: This cancels the job and sends a cancel signal to the agent pod
    if one is assigned. The job must not already be completed or cancelled.
    In-progress work may be lost.

    Args:
        job_id: Job UUID to cancel

    Returns:
        Cancellation result
    """
    try:
        resolved = await resolve_job_id(client, caller, job_id)
        result = await client.cancel_job(resolved)
        return fmt.format_action_result("cancel", resolved, result)
    except Exception as error:
        return format_action_error("cancel", job_id, error)


@descriptor(group="job_control", plane="job_control", caller_defaults=_ALL)
async def pause_job(client: AsyncCockpitClient, caller: CallerCtx, job_id: str) -> str:
    """Pause a running job.

    MUTATION: This sends a graceful pause request to the agent. The agent
    finishes its current node, saves a checkpoint, and becomes available
    for other work. The job must be in 'processing' status.

    Args:
        job_id: Job UUID to pause

    Returns:
        Pause result with status
    """
    try:
        resolved = await resolve_job_id(client, caller, job_id)
        result = await client.pause_job(resolved)
        return fmt.format_action_result("pause", resolved, result)
    except Exception as error:
        return format_action_error("pause", job_id, error)


@descriptor(group="job_control", plane="job_control", caller_defaults=_ALL)
async def create_job(
    client: AsyncCockpitClient,
    caller: CallerCtx,
    description: str,
    expert: str | None = None,
    config_name: str = "worker_base",
    expert_id: str | None = None,
    instructions: str | None = None,
    kickoff_message: str | None = None,
    config_override: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    project_id: str | None = None,
    priority: int = 5,
    required_deliverables: list[str] | None = None,
    slot: str | None = None,
    ticket: str | None = None,
    work_category: str | None = None,
    workspace: str | dict[str, Any] | None = None,
) -> str:
    """Create a new job for agent execution.

    MUTATION: This creates a job record and a Gitea repository. The job starts
    in 'created' status and is automatically queued for workspace provisioning
    and assignment to a ready agent. Monitor it with get_job; do not use
    assign_job for the normal start path. Jobs requiring input documents should
    use the cockpit UI instead.

    Args:
        description: Natural language task description
        expert: Which expert runs this job — the worker profile that decides
            its tools, prompts and model. Takes either form the catalogue
            prints, so a bundled expert id ("developer") and a database expert
            UUID are both valid here and you never need to know which store an
            entry came from. Use the `list_experts` tool to discover valid
            values and `get_expert` to inspect one before choosing. Omit to
            accept this deployment's default worker.
        config_name: DEPRECATED alias for `expert`, bundled experts only.
            Kept working for existing callers; new calls should use `expert`.
        expert_id: DEPRECATED alias for `expert`, database experts only.
            Kept working for existing callers; new calls should use `expert`.
        instructions: Additional inline markdown instructions
        kickoff_message: Opening task brief sent to the agent
        config_override: Per-job config overrides as JSON. To set the model,
            use {"llm": {"model": "<model_id>"}} — e.g.
            {"llm": {"model": "codex/gpt-5.3-codex-spark"}}.
            Use the list_models tool to discover available model IDs.
            A typed Officer slot may pin both model and workspace backend. An
            explicit model/backend that contradicts the selected slot is
            refused before job/ticket-claim creation; select a compatible slot
            instead of retrying the same override.
        context: Additional context dictionary
        project_id: Project UUID to file the job under. Omit to use the
            caller's own project lineage. Membership is validated
            server-side, so only projects the caller can access are accepted.
        priority: Dispatch priority from 0 (low) to 10 (high), default 5
        required_deliverables: Immutable deliverable contract — workspace-
            relative artifact paths (e.g. "output/report.md"), "kb:<slug>"
            notes, or one exact "pr:<owner>/<repository>" publication bound
            to a writable attached repository. Files under repos/<alias>/
            are not a verifiable contract and are refused; select the named
            compatible pr: contract rather than substituting a note. Missing
            PR proof can end blocked/undelivered, never completed.
        slot: Officer roster slot for this dispatch. Translated to
            context.officer_slot. Slot-pinned model/backend values are an
            allocation contract, not silent overrides: conflicting explicit
            choices are returned as stable slot_model_conflict or
            slot_backend_conflict errors.
        ticket: Backlog ticket this job claims — the knowledge-note slug of
            the ready ticket being dispatched. REQUIRED when working a
            backlog ticket: the server resolves the current ready generation
            and writes the durable claim atomically with the job. A context
            key like "backlog_ticket" is not claim authority.
        work_category: Explicit category for this dispatch (researcher,
            tester, executor). Optional — the slot's category governs the
            worker's contract; this records your stated intent and is named
            in the kickoff when the two disagree.
        workspace: The workspace this job runs in. Omit it to use the project's
            default (or, without a project, the installation's). "none" runs the
            job with no workspace. A template name such as "container-minimal",
            or one listed by manifest_list kind=WorkspaceTemplate, is looked up
            in the project, then your own templates, then the shared catalog.
            A child job inherits its parent's workspace and can't set this.

    Returns:
        Created job details with ID
    """
    offending = transport_key_paths(config_override)
    if offending:
        return (
            "Refusing to create job: config_override may not set credential "
            f"or transport keys ({', '.join(sorted(offending))}). Routing is "
            "resolved server-side from the model ID — pass "
            '{"llm": {"model": "<id>"}} and drop these keys.'
        )
    # One catalogue, one selector. `expert` accepts a bundled slug or a DB
    # UUID and resolves to the (base config, DB overlay) pair the funnel
    # persists; the two deprecated aliases resolve through the same helper, so
    # the mutual-exclusion refusal they need is stated once (see
    # src/shared/expert_reference.py and
    # knowledge-base/knowledge/issues/experts_one_catalogue_two_selection_paths.md).
    try:
        choice = resolve_expert_selection(
            expert=expert, config_name=config_name, expert_id=expert_id
        )
    except ExpertReferenceConflict as conflict:
        return f"Refusing to create job: {conflict}"

    merged_context = dict(context) if context is not None else None
    if merged_context is not None:
        for key in _SERVER_OWNED_CREATE_CONTEXT_KEYS:
            merged_context.pop(key, None)
    if slot is not None:
        merged_context = merged_context or {}
        merged_context["officer_slot"] = str(slot)

    try:
        try:
            workspace_supplied, workspace_binding = await workspace_field(
                client, workspace, project_id=project_id or caller.lineage_project_id
            )
        except WorkspaceArgumentError as problem:
            return f"Refusing to create job: {problem}"
        result = await client.create_job(
            description=description,
            config_name=choice.config_name,
            expert_id=choice.expert_id,
            # Deliberately not a parameter on this plane. The only callers are
            # dispatchers (officer, session) with no basis for connector
            # surgery, and the one thing the field ever did in anger was let a
            # model pass [] — "attach none" — because the schema advertised an
            # array and the empty one reads as the neutral value. None here
            # makes the client send use_datasource_defaults, so the job gets
            # the project's auto-attach defaults resolved at dispatch time.
            # Narrowing still exists on the general surfaces (MCP, REST,
            # cockpit), which is where a human reviews the choice.
            datasource_ids=None,
            instructions=instructions,
            kickoff_message=kickoff_message,
            config_override=config_override,
            context=merged_context,
            parent_job_id=caller.parent_job_id,
            # An explicitly passed project wins over the hidden lineage
            # default (the pre-unification agent-lane create tool accepted it
            # too); the orchestrator validates membership server-side.
            project_id=project_id or caller.lineage_project_id,
            user_id=caller.user_id,
            thread_id=caller.thread_id,
            priority=priority,
            required_deliverables=required_deliverables,
            ticket=ticket,
            work_category=work_category,
            workspace=workspace_binding,
            workspace_supplied=workspace_supplied,
        )
        return fmt.format_created_job(
            result, choice.config_name, expert=choice.reference
        )
    except Exception as error:
        return format_action_error("create", "N/A", error)


@descriptor(
    group="job_control",
    plane="job_control",
    caller_defaults=_MCP,
    grant="explicit",
    gate=_EXPLICIT_GATE,
)
async def delete_job(client: AsyncCockpitClient, caller: CallerCtx, job_id: str) -> str:
    """Delete a job and its associated data.

    MUTATION: This permanently deletes the job record and its requirements.
    Any job can be deleted regardless of status. WARNING: Deleting a job in
    'processing' status may leave an orphaned agent. This action is irreversible.
    A backlog-ticket claim is retained permanently: deleting the job never
    makes that ticket dispatchable again; only a newer Officer re-ready
    generation can do so, and a deleted non-terminal claim remains blocked.

    Args:
        job_id: Job UUID to delete

    Returns:
        Deletion result
    """
    try:
        resolved = await resolve_job_id(client, caller, job_id)
        result = await client.delete_job(resolved)
        return fmt.format_action_result("delete", resolved, result)
    except Exception as error:
        return format_action_error("delete", job_id, error)


@descriptor(
    group="job_control",
    plane="job_control",
    caller_defaults=_MCP,
    grant="explicit",
    gate=_EXPLICIT_GATE,
)
async def assign_job(
    client: AsyncCockpitClient,
    caller: CallerCtx,
    job_id: str,
    agent_id: str,
) -> str:
    """Administratively request assignment to a ready agent.

    MUTATION, ADMIN OVERRIDE: Normal created jobs are provisioned and assigned
    automatically. If this job has no live managed workspace, the request is
    queued through normal provisioning and the requested agent is not reserved.
    With a live workspace, it starts/resumes directly on the requested ready
    agent. The job must be created, failed, or paused.

    Args:
        job_id: Job UUID to assign
        agent_id: Agent UUID to assign to

    Returns:
        Assignment result
    """
    try:
        resolved = await resolve_job_id(client, caller, job_id)
        result = await client.assign_job(resolved, agent_id)
        extra = {"agent_id": agent_id} if result.get("status") == "assigned" else {}
        return fmt.format_action_result("assign", resolved, result, **extra)
    except Exception as error:
        return format_action_error("assign", job_id, error)


@descriptor(
    group="job_control",
    plane="job_control",
    caller_defaults=_MCP,
    grant="explicit",
    gate=_EXPLICIT_GATE,
)
async def promote_job(
    client: AsyncCockpitClient,
    caller: CallerCtx,
    job_id: str,
    name: str,
    user_id: str,
    description: str | None = None,
    goal: str | None = None,
) -> str:
    """Promote a completed job into a dedicated project.

    Creates a new project, seeds its repo from the job's branch
    (preserving git history), and moves the job. The job must be
    completed and in a default project.

    Args:
        job_id: Job UUID (must be completed)
        name: Name for the new project
        user_id: Owner user UUID
        description: Project description (optional)
        goal: Project goal (optional)

    Returns:
        Promotion summary with new project ID
    """
    resolved = await resolve_job_id(client, caller, job_id)
    result = await client.promote_job(
        job_id=resolved,
        name=name,
        user_id=user_id,
        description=description,
        goal=goal,
    )
    project_id = result.get("project_id", "unknown")
    project_name = result.get("project_name", name)
    return (
        f"Job {resolved} promoted to project '{project_name}'.\n"
        f"  New project ID: {project_id}\n"
        "  Git history preserved from job branch."
    )


@descriptor(
    group="job_control",
    plane="job_control",
    # Officer default per officer_supervision_surface §3.1: replying on a
    # worker's message thread is a bounded orchestrator action.
    caller_defaults=_MCP_OFFICER,
    grant="explicit",
    gate=_EXPLICIT_GATE,
)
async def send_message_to_job(
    client: AsyncCockpitClient,
    caller: CallerCtx,
    job_id: str,
    thread_id: str,
    message: str,
    urgent: bool = False,
) -> str:
    """Send a reply to an agent's message thread (as a human).

    Routes the reply to the agent. If the agent is waiting for a reply
    on this thread (blocking mode), it resumes immediately. ``urgent``
    delivers into the worker's next LLM turn WITHOUT destroying its
    context (guidance lane, ~≤60s + one turn; strategy
    ``guidance_next_turn``) — only a job with no live run gets resumed
    to deliver an urgent message. Non-urgent replies are injected at the
    next tactical→strategic phase boundary. To force a full re-plan
    instead, use resume_job_with_feedback (destructive).

    Args:
        job_id: Job UUID
        thread_id: Thread ID to reply to
        message: Reply body text
        urgent: If true, deliver into the worker's next LLM turn

    Returns:
        Delivery confirmation with strategy used
    """
    try:
        resolved = await resolve_job_id(client, caller, job_id)
        result = await client.reply_to_message(
            resolved, thread_id, message, urgent=urgent
        )
        strategy = result.get("delivery_strategy", "unknown")
        sequence = result.get("sequence", "?")
        return (
            f"Reply delivered to thread {thread_id} (message #{sequence}).\n"
            f"Delivery strategy: {strategy}"
        )
    except Exception as error:
        return f"Failed to send reply: {friendly_reason(error)}"


_OFFICER_IDENTITY_ERROR = (
    "This caller carries no session thread identity, so it cannot act as a "
    "commissioned officer. Officer message actions (reply/escalate/"
    "acknowledge) run only from the project's commissioned officer session; "
    "use send_message_to_job for a plain human-side reply."
)


@descriptor(
    group="job_control",
    plane="job_control",
    # officer_message_routing.md §4: bounded officer inbox actions. Officer +
    # MCP lanes by default; the plain session lane stays out — the server
    # verifies the caller IS the commissioned officer thread, which an
    # ordinary session never is.
    caller_defaults=_MCP_OFFICER,
    grant="explicit",
    gate=_EXPLICIT_GATE,
)
async def reply_to_job_message(
    client: AsyncCockpitClient,
    caller: CallerCtx,
    job_id: str,
    thread_id: str,
    message: str,
) -> str:
    """Answer a worker's routed message as the project's officer.

    For a blocking route the waiting worker resumes exactly once with your
    answer; the route is recorded ``resolved_by_officer``. Your reply is
    guidance, never authorization — it cannot approve jobs, add backlog
    ready-marks, or waive deliverables, and the worker's original message is
    never erased. Only the commissioned officer session of the job's project
    may use this (verified server-side against the durable post row).

    Args:
        job_id: Job UUID (or unique visible prefix)
        thread_id: Worker message thread ID (from your wake/sitrep inbox)
        message: Your answer, delivered verbatim to the worker

    Returns:
        Delivery confirmation with the route's resulting state
    """
    if not caller.thread_id:
        return _OFFICER_IDENTITY_ERROR
    try:
        resolved = await resolve_job_id(client, caller, job_id)
        result = await client.officer_reply_to_job_message(resolved, thread_id, message)
        return (
            f"Reply delivered to worker thread {thread_id} of job "
            f"{short_id(resolved)} (strategy: "
            f"{result.get('delivery_strategy', 'queued')}; route now "
            f"{result.get('route_state', 'unknown')})."
        )
    except Exception as error:
        status = http_status_of(error)
        if status is not None:
            detail = response_detail(error)
            return (
                f"Officer reply failed ({status}): {detail}"
                if detail
                else f"Officer reply failed ({status})."
            )
        return f"Officer reply failed: {friendly_reason(error)}"


@descriptor(
    group="job_control",
    plane="job_control",
    caller_defaults=_MCP_OFFICER,
    grant="explicit",
    gate=_EXPLICIT_GATE,
)
async def escalate_job_message(
    client: AsyncCockpitClient,
    caller: CallerCtx,
    job_id: str,
    thread_id: str,
    context: str | None = None,
) -> str:
    """Hand a worker's routed message to the user, with your context attached.

    The user receives the ORIGINAL worker message plus your clearly
    delimited context on the SAME thread — their reply resumes the worker
    directly. Use this when the question needs the user's authority or
    knowledge; to inform the user in your own voice instead, use notify_user.

    Args:
        job_id: Job UUID (or unique visible prefix)
        thread_id: Worker message thread ID
        context: Optional context for the user (why you escalated, what you
            recommend). Delivered clearly separated from the worker's text.

    Returns:
        Escalation confirmation with delivery status
    """
    if not caller.thread_id:
        return _OFFICER_IDENTITY_ERROR
    try:
        resolved = await resolve_job_id(client, caller, job_id)
        result = await client.officer_escalate_job_message(
            resolved, thread_id, context=context
        )
        note = result.get("note")
        return (
            f"Thread {thread_id} of job {short_id(resolved)} escalated to the "
            f"user (delivered={result.get('delivered')})."
            + (f" Note: {note}" if note else "")
        )
    except Exception as error:
        status = http_status_of(error)
        if status is not None:
            detail = response_detail(error)
            return (
                f"Escalation failed ({status}): {detail}"
                if detail
                else f"Escalation failed ({status})."
            )
        return f"Escalation failed: {friendly_reason(error)}"


@descriptor(
    group="job_control",
    plane="job_control",
    caller_defaults=_MCP_OFFICER,
    grant="explicit",
    gate=_EXPLICIT_GATE,
)
async def acknowledge_job_message(
    client: AsyncCockpitClient,
    caller: CallerCtx,
    job_id: str,
    thread_id: str,
    note: str | None = None,
) -> str:
    """Close an ASYNC worker message from your inbox without replying.

    For status updates that need no answer. Refused for blocking routes — a
    frozen worker needs reply_to_job_message or escalate_job_message, never
    a silent ack pretending nobody waited.

    Args:
        job_id: Job UUID (or unique visible prefix)
        thread_id: Worker message thread ID
        note: Optional note recorded on the route's audit trail

    Returns:
        Acknowledge confirmation
    """
    if not caller.thread_id:
        return _OFFICER_IDENTITY_ERROR
    try:
        resolved = await resolve_job_id(client, caller, job_id)
        result = await client.officer_acknowledge_job_message(
            resolved, thread_id, note=note
        )
        return (
            f"Thread {thread_id} of job {short_id(resolved)} acknowledged "
            f"(route now {result.get('route_state', 'resolved_by_officer')})."
        )
    except Exception as error:
        status = http_status_of(error)
        if status is not None:
            detail = response_detail(error)
            return (
                f"Acknowledge failed ({status}): {detail}"
                if detail
                else f"Acknowledge failed ({status})."
            )
        return f"Acknowledge failed: {friendly_reason(error)}"


@descriptor(
    group="job_control",
    plane="job_control",
    caller_defaults=_MCP_OFFICER,
    grant="explicit",
    gate=_EXPLICIT_GATE,
)
async def steer_job(
    client: AsyncCockpitClient,
    caller: CallerCtx,
    job_id: str,
    message: str,
    urgent: bool = False,
) -> str:
    """Send guidance to a running job without stopping it.

    Non-destructive either way — the worker keeps its context, todos,
    and plan. ``urgent=True`` lands the message in the worker's next
    LLM turn; ``urgent=False`` delivers at the next phase boundary. Use
    this for course corrections. If the plan itself is wrong and the worker
    must re-plan, use resume_job_with_feedback; that destructive path costs
    the worker its in-flight work. An urgent message only resumes a job when
    there is no live run to receive it.

    Args:
        job_id: Job UUID (or unique visible prefix for agent/session callers)
        message: Concrete guidance delivered verbatim to the worker
        urgent: Deliver into the worker's next LLM turn

    Returns:
        Guidance delivery confirmation and strategy
    """
    try:
        resolved = await resolve_job_id(client, caller, job_id)
        result = await client.reply_to_message(
            resolved, "officer", str(message), urgent=bool(urgent)
        )
        return (
            f"Guidance delivered to job {short_id(resolved)} "
            f"(strategy: {result.get('delivery_strategy', 'queued')})."
        )
    except Exception as error:
        # F6: the response body (e.g. the 409 reason naming why steering was
        # refused) must reach the caller; raw httpx text embeds internal URLs.
        status = http_status_of(error)
        if status is not None:
            detail = response_detail(error)
            return (
                f"Steer failed ({status}): {detail}"
                if detail
                else f"Steer failed ({status})."
            )
        return f"Steer failed: {friendly_reason(error)}"
