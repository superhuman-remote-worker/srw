"""Shared formatting functions for API responses.

Used by both the MCP server and the builder dispatch module to convert
raw API response dicts/lists into human-readable text.

All functions are pure (dict/list in → str out) with no side effects.
"""

from __future__ import annotations

import json
import re
from typing import Any


def _mask_url(url: str) -> str:
    """Mask password in a connection URL for safe display."""
    return re.sub(r"(://[^:]+:)[^@]+(@)", r"\1***\2", url)


def _fmt_confidence(value: Any) -> str:
    """Render a confidence value that may be a 0-1 float, another number, or an enum string.

    The knowledge-notes store uses a Postgres enum (`'high'`/`'medium'`/`'low'`),
    while freeze data emits numeric confidences. Applying a `%` float format spec
    to a string raises `ValueError: Unknown format code '%'`, so callers must route
    through here instead of formatting the raw value.
    """
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, (int, float)):
        return f"{value:.0%}" if 0 <= value <= 1 else str(value)
    return str(value)


# =============================================================================
# Job & Audit Formatters
# =============================================================================


def _short_id(value: Any) -> str:
    text = str(value or "")
    return text[:8] if text else "unknown"


def truncate_text(value: Any, *, limit: int = 140) -> str:
    text = str(value or "").strip()
    if not text:
        return "N/A"
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _compact_dict_value(data: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = data.get(key)
        if value not in (None, "", [], {}):
            return value
    return None


def _freeze_lines(job: dict[str, Any]) -> list[str]:
    """Freeze type/reason/requires-review lines from a job row (F4/F5).

    ``freeze_data`` may live on the row or inside ``context`` (and either may
    arrive as a JSON string from JSONB columns).
    """
    freeze_data = job.get("freeze_data")
    if isinstance(freeze_data, str):
        try:
            freeze_data = json.loads(freeze_data)
        except (json.JSONDecodeError, ValueError):
            freeze_data = None
    if not isinstance(freeze_data, dict):
        context = job.get("context")
        if isinstance(context, str):
            try:
                context = json.loads(context)
            except (json.JSONDecodeError, ValueError):
                context = None
        if isinstance(context, dict):
            candidate = context.get("freeze_data")
            if isinstance(candidate, dict):
                freeze_data = candidate
    if not isinstance(freeze_data, dict):
        return []

    lines: list[str] = []
    freeze_type = _compact_dict_value(freeze_data, "freeze_type", "type")
    reason = _compact_dict_value(
        freeze_data,
        "reason",
        "message",
        "status_message",
        "review_reason",
        "pause_reason",
    )
    if freeze_type:
        lines.append(f"Freeze type: {freeze_type}")
    if reason:
        lines.append(f"Freeze reason: {truncate_text(reason, limit=240)}")
    if freeze_data.get("requires_review") is not None:
        lines.append(f"Requires review: {freeze_data.get('requires_review')}")
    return lines


def format_job_list_item(job: dict[str, Any]) -> list[str]:
    """Rich per-job list rendering (F5) — description, lineage, freeze, error."""
    from shared.job_outcome import effective_job_status

    job_id = job.get("id", "unknown")
    lines = [
        f"--- {job_id} (short: {_short_id(job_id)}) ---",
        f"  Status: {effective_job_status(job)}",
        f"  Description: {truncate_text(job.get('description'), limit=140)}",
    ]
    if job.get("config_name") or job.get("config"):
        lines.append(f"  Config: {job.get('config_name') or job.get('config')}")
    workspace = job.get("workspace_contract")
    if isinstance(workspace, dict):
        requested = workspace.get("requested_backend") or "default"
        assigned = workspace.get("assigned_backend") or "unknown"
        effective = workspace.get("effective_backend") or "unavailable"
        state = workspace.get("state") or "unknown"
        lines.append(
            "  Workspace: "
            f"requested={requested}, assigned={assigned}, "
            f"effective={effective} ({state})"
        )
        if workspace.get("stale_backend"):
            lines.append(
                f"  Workspace residue: stale {workspace['stale_backend']} runtime"
            )
    if job.get("project_id"):
        lines.append(f"  Project ID: {job['project_id']}")
    if job.get("parent_job_id"):
        lines.append(f"  Parent job ID: {job['parent_job_id']}")
    if job.get("assigned_agent_id"):
        lines.append(f"  Agent: {job['assigned_agent_id']}")
    if job.get("updated_at"):
        lines.append(f"  Updated: {job['updated_at']}")
    elif job.get("created_at"):
        lines.append(f"  Created: {job['created_at']}")
    if job.get("audit_count") is not None:
        lines.append(f"  Audit entries: {job['audit_count']}")
    for freeze_line in _freeze_lines(job):
        lines.append(f"  {freeze_line}")
    if job.get("error_message"):
        lines.append(f"  Error: {truncate_text(job['error_message'], limit=180)}")
    return lines


def _applied_filter_phrases(
    status: str | None, filters: dict[str, Any] | None
) -> tuple[str, list[str]]:
    """What narrowed a jobs query, as (status phrase, other phrases).

    Includes the server-side defaults, not just what the caller typed. An
    agent that reads "no jobs found" without being told archived-project
    jobs were excluded will conclude the job does not exist instead of
    widening the query — hiding must never be silent. Status stays separate
    so the long-standing "No jobs found with status='failed'." wording is
    preserved verbatim when it is the only filter in play.
    """
    filters = filters or {}
    phrases: list[str] = []

    statuses = filters.get("status") or ([status] if status else [])
    status_phrase = ""
    if statuses:
        rendered = ", ".join(f"'{value}'" for value in statuses)
        status_phrase = f"status={rendered}"
    if filters.get("search"):
        phrases.append(f"search='{filters['search']}'")
    if filters.get("project_id"):
        phrases.append(f"{len(filters['project_id'])} project filter(s)")
    if filters.get("has_project") is False:
        phrases.append("only jobs with no project")
    elif filters.get("has_project") is True:
        phrases.append("only jobs with a project")
    if filters.get("include_archived_projects") is False:
        phrases.append("archived-project jobs excluded")
    return status_phrase, phrases


def format_jobs(
    jobs: list[dict[str, Any]],
    status: str | None = None,
    *,
    total: int | None = None,
    total_is_capped: bool = False,
    filters: dict[str, Any] | None = None,
) -> str:
    """Format job list for display.

    E1/F5: restores the decision-grade per-job rendering (description,
    project/parent lineage, agent, freeze summaries) that the session lane
    always had. An empty result names the filters — including the ones the
    server applied by default — so "no jobs with status='failed', archived
    excluded" is distinguishable from "no jobs at all".

    ``jobs`` is a plain list; callers unwrap the ``/api/jobs`` envelope and
    pass ``total``/``filters`` through so the rendering can say how much of
    the result set is on screen and what was filtered out.
    """
    status_phrase, phrases = _applied_filter_phrases(status, filters)
    suffix = f" with {status_phrase}" if status_phrase else ""
    if phrases:
        suffix += f" ({'; '.join(phrases)})"

    if not jobs:
        return f"No jobs found{suffix}."

    if total is not None and total > len(jobs):
        counted = f"{total}+" if total_is_capped else str(total)
        header = f"Found {len(jobs)} of {counted} job(s){suffix}"
    else:
        header = f"Found {len(jobs)} job(s){suffix}"

    lines = [f"{header}:\n"]
    for job in jobs:
        lines.extend(format_job_list_item(job))
        lines.append("")
    return "\n".join(lines)


def format_job_detail(job: dict[str, Any]) -> str:
    """Format single job details.

    E1/F4: decision-grade detail — the real endpoint shape uses
    ``config_name`` (the old formatter read ``config`` and printed
    'Config: N/A' forever), and supervision needs lineage, owner, priority,
    agent, repo/branch, and the freeze type/reason/requires-review facts.
    """
    from shared.job_outcome import effective_job_status

    job_id = job.get("id", "unknown")
    lines = [
        f"Job: {job_id}",
        f"Short ID: {_short_id(job_id)}",
        f"Status: {effective_job_status(job)}",
    ]
    if job.get("description"):
        lines.append(f"Description: {truncate_text(job.get('description'), limit=500)}")
    if job.get("config_name") or job.get("config"):
        lines.append(f"Config: {job.get('config_name') or job.get('config')}")
    workspace = job.get("workspace_contract")
    if isinstance(workspace, dict):
        lines.append(
            "Workspace: "
            f"requested={workspace.get('requested_backend') or 'default'}, "
            f"assigned={workspace.get('assigned_backend') or 'unknown'}, "
            f"effective={workspace.get('effective_backend') or 'unavailable'}, "
            f"state={workspace.get('state') or 'unknown'}"
        )
        if workspace.get("failure"):
            lines.append(f"Workspace mismatch/failure: {workspace['failure']}")
        if workspace.get("stale_backend"):
            lines.append(
                f"Workspace residue: stale {workspace['stale_backend']} runtime"
            )
    if job.get("project_id"):
        lines.append(f"Project ID: {job['project_id']}")
    if job.get("user_id"):
        lines.append(f"Owner user ID: {job['user_id']}")
    if job.get("parent_job_id"):
        lines.append(f"Parent job ID: {job['parent_job_id']}")
    if job.get("priority") is not None:
        lines.append(f"Priority: {job['priority']}")
    if job.get("assigned_agent_id"):
        lines.append(f"Agent: {job['assigned_agent_id']}")
    if job.get("created_at"):
        lines.append(f"Created: {job['created_at']}")
    if job.get("updated_at"):
        lines.append(f"Updated: {job['updated_at']}")
    if job.get("repo_name"):
        lines.append(f"Repo: {job['repo_name']}")
    if job.get("branch_name"):
        lines.append(f"Branch: {job['branch_name']}")
    if job.get("audit_count") is not None:
        lines.append(f"Audit entries: {job['audit_count']}")
    lines.extend(_freeze_lines(job))
    lines.extend(_operator_pause_hold_lines(job))
    error_message = job.get("error_message") or job.get("error")
    if error_message:
        lines.append(f"Error: {truncate_text(error_message, limit=300)}")
    return "\n".join(lines)


def _operator_pause_hold_lines(job: dict[str, Any]) -> list[str]:
    """Say so when an operator pause holds the job until an explicit resume."""
    context = job.get("context")
    if isinstance(context, str):
        try:
            context = json.loads(context)
        except (json.JSONDecodeError, ValueError):
            return []
    hold = context.get("_operator_pause_hold") if isinstance(context, dict) else None
    if hold is None:
        return []
    detail = hold if isinstance(hold, dict) else {}
    return [
        "Operator pause hold: held until an explicit resume "
        f"(paused_by={detail.get('paused_by') or 'internal'}, "
        f"since {detail.get('paused_at') or 'unknown'})"
    ]


def _format_audit_entry(entry: dict[str, Any]) -> str:
    """Format a single audit entry into a one-line summary."""
    step_type = entry.get("step_type", "unknown")
    step_num = entry.get("step_number", "?")
    timestamp = entry.get("timestamp", "")

    if step_type == "llm":
        llm = entry.get("llm", {})
        request_id = llm.get("request_id")
        model = llm.get("model", "")
        preview = llm.get("response_content_preview", "") or ""
        tool_calls = llm.get("tool_calls") or []

        header = f"[{step_num}] LLM ({model})"
        if request_id:
            header += f" doc_id={request_id}"
        if tool_calls:
            tool_names = [tc.get("name", "?") for tc in tool_calls]
            header += f" -> {', '.join(tool_names)}"
        elif preview:
            text = preview[:200]
            if len(preview) > 200:
                text += "..."
            header += f": {text}"
        return header

    elif step_type == "tool":
        tool = entry.get("tool", {})
        tool_name = tool.get("name", "unknown")
        result_preview = tool.get("result_preview")
        success = tool.get("success")

        if result_preview is not None:
            result = str(result_preview)[:150]
            if len(str(result_preview)) > 150:
                result += "..."
            status = "ok" if success else "FAIL"
            return f"[{step_num}] Tool [{status}] {tool_name}: {result}"
        else:
            args = tool.get("arguments", {})
            args_preview = json.dumps(args)[:100]
            if len(json.dumps(args)) > 100:
                args_preview += "..."
            return f"[{step_num}] Tool Call: {tool_name}({args_preview})"

    elif step_type == "error":
        error = entry.get("error", "Unknown error")
        return f"[{step_num}] ERROR: {error}"

    else:
        return f"[{step_num}] {step_type}: {timestamp}"


def format_audit(audit: dict[str, Any]) -> str:
    """Format audit trail entries."""
    entries = audit.get("entries", [])
    total = audit.get("total", 0)
    page = audit.get("page", 1)
    has_more = audit.get("hasMore", False)

    if audit.get("error"):
        return f"Audit unavailable: {audit['error']}"

    if not entries:
        return "No audit entries found."

    lines = [f"Audit trail (page {page}, showing {len(entries)} of {total} entries):\n"]

    for entry in entries:
        lines.append(_format_audit_entry(entry))

    if has_more:
        lines.append(f"\n... more entries available (use page={page + 1})")

    return "\n".join(lines)


def format_audit_bulk(data: dict[str, Any]) -> str:
    """Format bulk audit entries (offset/limit based)."""
    entries = data.get("entries", [])
    total = data.get("total", 0)
    offset = data.get("offset", 0)
    limit = data.get("limit", 500)
    has_more = data.get("hasMore", False)

    if data.get("error"):
        return f"Audit unavailable: {data['error']}"

    if not entries:
        return "No audit entries found."

    lines = [
        f"Audit trail (offset {offset}, showing {len(entries)} of {total} entries):\n"
    ]

    for entry in entries:
        lines.append(_format_audit_entry(entry))

    if has_more:
        lines.append(f"\n... more entries available (use offset={offset + limit})")

    return "\n".join(lines)


# Transient-injection markers, mirrored from src/core/*_injection.py (the MCP
# image doesn't ship src/): legacy chat_history rows stored the re-injected
# block verbatim as human/tool inputs; newer rows store type="context"
# descriptors. Both collapse to one [context] line per turn.
_INJECT_KIND_BY_PREFIX = (
    ("instruction_inject_", "instruction"),
    ("memory_inject_", "memory"),
    ("knowledge_inject_", "knowledge"),
    ("citation_feedback_inject_", "citation_feedback"),
)


def _context_label(msg: dict[str, Any]) -> str | None:
    """Kind label when an inputs element is injected context, else None."""
    kind = msg.get("kind")
    if msg.get("type") == "context":
        return str(kind) if kind else "context"
    if msg.get("type") == "human":
        content = msg.get("content_preview") or msg.get("content") or ""
        if isinstance(content, str) and content.startswith("<active_tasks>"):
            return "todos"
        # Persistent phase instruction block (src/core/message_markers.py):
        # newer archivers store it as type="context" kind="phase_instruction";
        # this prefix catches a row stored verbatim by an older writer.
        if isinstance(content, str) and content.startswith("[phase: "):
            return "phase_instruction"
    if msg.get("type") == "tool":
        tcid = msg.get("tool_call_id") or ""
        for prefix, k in _INJECT_KIND_BY_PREFIX:
            if tcid.startswith(prefix):
                return k
    return None


def _format_chat_entry(entry: dict[str, Any], turn_number: int) -> list[str]:
    """Format a single chat entry into display lines."""
    lines = [f"--- Turn {entry.get('turn_number', turn_number)} ---"]

    # Input messages; the re-injected context frame collapses to one line.
    inputs = entry.get("inputs", entry.get("input_messages", []))
    context_kinds: list[str] = []
    for msg in inputs:
        ctx = _context_label(msg)
        if ctx is not None:
            if ctx not in context_kinds:
                context_kinds.append(ctx)
            continue
        role = msg.get("type", msg.get("role", "unknown"))
        content = msg.get("content_preview") or msg.get("content", "")
        if isinstance(content, str):
            preview = content[:300]
            if len(content) > 300:
                preview += "..."
        else:
            preview = str(content)[:300]
        lines.append(f"[{role}]: {preview}")
    if context_kinds:
        lines.append(f"[context]: {', '.join(context_kinds)} (re-injected each turn)")

    # Response
    response = entry.get("response", {})
    resp_content = response.get("content_preview") or response.get("content", "")
    if isinstance(resp_content, str):
        preview = resp_content[:300]
        if len(resp_content) > 300:
            preview += "..."
    else:
        preview = str(resp_content)[:300]
    tool_calls = response.get("tool_calls", [])
    if tool_calls:
        tool_names = ", ".join(tc.get("name", "?") for tc in tool_calls)
        lines.append(
            f"[assistant]: {preview}"
            if preview.strip()
            else f"[assistant]: (tool calls: {tool_names})"
        )
    elif preview.strip():
        lines.append(f"[assistant]: {preview}")

    lines.append("")
    return lines


def format_chat_history(chat: dict[str, Any]) -> str:
    """Format chat history entries."""
    entries = chat.get("entries", [])
    total = chat.get("total", 0)
    page = chat.get("page", 1)
    has_more = chat.get("hasMore", False)

    if chat.get("error"):
        return f"Chat history unavailable: {chat['error']}"

    if not entries:
        return "No chat history found."

    lines = [f"Chat history (page {page}, {len(entries)} of {total} turns):\n"]

    for i, entry in enumerate(entries, 1):
        lines.extend(_format_chat_entry(entry, i))

    if has_more:
        lines.append(f"... more turns available (use page={page + 1})")

    return "\n".join(lines)


def format_chat_bulk(data: dict[str, Any]) -> str:
    """Format bulk chat history entries (offset/limit based)."""
    entries = data.get("entries", [])
    total = data.get("total", 0)
    offset = data.get("offset", 0)
    limit = data.get("limit", 500)
    has_more = data.get("hasMore", False)

    if data.get("error"):
        return f"Chat history unavailable: {data['error']}"

    if not entries:
        return "No chat history found."

    lines = [
        f"Chat history (offset {offset}, showing {len(entries)} of {total} turns):\n"
    ]

    for i, entry in enumerate(entries, offset + 1):
        lines.extend(_format_chat_entry(entry, i))

    if has_more:
        lines.append(f"... more turns available (use offset={offset + limit})")

    return "\n".join(lines)


def _todo_label(todo: dict[str, Any]) -> str:
    """Display label for a todo item.

    The Gitea-backed /todos* routes key items on ``content`` (todos.yaml
    schema and archive parser alike); ``subject`` is kept as a fallback for
    older payloads.
    """
    return todo.get("subject") or todo.get("content") or "Untitled"


def _summary_source(envelope: dict[str, Any], name: str) -> dict[str, Any]:
    for source in envelope.get("sources", []):
        if source.get("name") == name:
            return source
    return {"name": name, "status": "fresh"}


def _section_unavailable(source: dict[str, Any]) -> str | None:
    if source.get("status") == "unavailable":
        return f"  (unavailable: {source.get('reason') or 'source unreachable'})"
    return None


def format_job_summary(job_id: str, envelope: dict[str, Any]) -> str:
    """Format a combined job summary from a truthful-read envelope (E1).

    The envelope carries five sources (control_db, liveness, job_repo_todos,
    job_repo, audit_db) and their data. A failed source renders as an explicit
    ``(unavailable: …)`` section — the partial distinction — and the header
    names the observation time. Schema repairs vs. the old formatter:
    ``config_name`` (not ``config``), and the liveness contract instead of the
    stubbed phase/elapsed/eta fields that never existed on the endpoint.
    """
    data = envelope.get("data") or {}
    job = data.get("job")
    progress = data.get("progress")
    todos = data.get("todos")
    workspace = data.get("workspace")
    recent_audit = data.get("recent_audit")

    lines: list[str] = [
        f"Job summary: {job_id} (observed {envelope.get('observed_at', 'unknown')})"
    ]
    unavailable_sections = [
        source.get("name")
        for source in envelope.get("sources", [])
        if source.get("status") == "unavailable"
    ]
    if unavailable_sections:
        lines.append(
            "PARTIAL summary — unavailable sources: "
            + ", ".join(str(name) for name in unavailable_sections)
        )
    lines.append("")

    # --- Status & Config ---
    lines.append("=== Status & Config ===")
    note = _section_unavailable(_summary_source(envelope, "control_db"))
    if note:
        lines.append(note)
    elif job:
        from shared.job_outcome import effective_job_status

        display_status = effective_job_status(job)
        status_icon = {
            "created": "📝",
            "processing": "🔄",
            "paused": "⏸",
            "pending_review": "👁",
            "completed": "✅",
            "failed": "❌",
            "cancelled": "⛔",
            "blocked_undelivered": "🚫",
        }.get(display_status, "❓")
        lines.append(f"  {status_icon} Status: {display_status}")
        lines.append(
            f"  Config: {job.get('config_name') or job.get('config') or 'N/A'}"
        )
        if job.get("project_id"):
            lines.append(f"  Project ID: {job['project_id']}")
        if job.get("parent_job_id"):
            lines.append(f"  Parent job ID: {job['parent_job_id']}")
        if job.get("assigned_agent_id"):
            lines.append(f"  Agent: {job['assigned_agent_id']}")
        lines.append(f"  Created: {job.get('created_at', 'N/A')}")
        lines.append(f"  Updated: {job.get('updated_at', 'N/A')}")
        desc = job.get("description", "")
        if desc:
            lines.append(
                f"  Description: {desc[:200]}{'...' if len(desc) > 200 else ''}"
            )
        for freeze_line in _freeze_lines(job):
            lines.append(f"  {freeze_line}")
        error_message = job.get("error_message") or job.get("error")
        if error_message:
            lines.append(f"  Error: {error_message}")
    else:
        lines.append("  (no data)")
    lines.append("")

    # --- Liveness (shared contract; never a manufactured percentage) ---
    lines.append("=== Liveness ===")
    note = _section_unavailable(_summary_source(envelope, "liveness"))
    if note:
        lines.append(note)
    elif progress:
        lines.extend(f"  {line}" for line in _liveness_lines(progress))
    else:
        lines.append("  (no data)")
    lines.append("")

    # --- Current Todos ---
    lines.append("=== Current Todos (Gitea-backed: as of the worker's last push) ===")
    note = _section_unavailable(_summary_source(envelope, "job_repo_todos"))
    if note:
        lines.append(note)
    elif todos:
        current = todos.get("current")
        if current:
            todo_list = current.get("todos", [])
            for t in todo_list:
                icon = {
                    "pending": "○",
                    "in_progress": "◐",
                    "completed": "●",
                    "skipped": "⊘",
                }.get(t.get("status", ""), "?")
                lines.append(f"  {icon} {_todo_label(t)}")
        else:
            lines.append("  (no current todos)")
        archives = todos.get("archives", [])
        if archives:
            lines.append(f"  Archived phases: {len(archives)}")
    else:
        lines.append("  (no data)")
    lines.append("")

    # --- Workspace ---
    lines.append("=== Workspace (Gitea-backed: as of the worker's last push) ===")
    note = _section_unavailable(_summary_source(envelope, "job_repo"))
    if note:
        lines.append(note)
    elif workspace:
        files = workspace.get("files", [])
        if files:
            lines.append(f"  Files: {len(files)}")
            for f in files[:10]:
                name = f.get("name", f.get("path", "?"))
                size = f.get("size")
                lines.append(f"    - {name}" + (f" ({size}b)" if size else ""))
            if len(files) > 10:
                lines.append(f"    ... and {len(files) - 10} more")
        ws_preview = workspace.get(
            "workspace_md", workspace.get("workspace_preview", "")
        )
        if ws_preview:
            lines.append(f"  workspace.md preview: {str(ws_preview)[:200]}...")
        plan_preview = workspace.get("plan_md", workspace.get("plan_preview", ""))
        if plan_preview:
            lines.append(f"  plan.md preview: {str(plan_preview)[:200]}...")
    else:
        lines.append("  (no data)")
    lines.append("")

    # --- Recent Activity ---
    lines.append("=== Recent Activity (last 10 tool calls) ===")
    note = _section_unavailable(_summary_source(envelope, "audit_db"))
    if note:
        lines.append(note)
    elif recent_audit:
        if recent_audit.get("error"):
            lines.append(f"  (unavailable: {recent_audit['error']})")
        else:
            entries = recent_audit.get("entries", [])
            if entries:
                for entry in entries[-10:]:
                    lines.append(f"  {_format_audit_entry(entry)}")
            else:
                lines.append("  (no entries)")
    else:
        lines.append("  (no data)")

    return "\n".join(lines)


def format_todos(todos: dict[str, Any]) -> str:
    """Format todos (current + archives)."""
    lines = [f"Todos for job: {todos.get('job_id', 'unknown')}\n"]

    if not todos.get("has_workspace"):
        return "No workspace found for this job."

    # Current todos
    current = todos.get("current")
    if current:
        lines.append("=== Current Todos ===")
        for todo in current.get("todos", []):
            status_icon = {
                "pending": "○",
                "in_progress": "◐",
                "completed": "●",
                "skipped": "⊘",
            }.get(todo.get("status", ""), "?")
            lines.append(f"  {status_icon} {_todo_label(todo)}")
            if todo.get("description"):
                desc = todo["description"][:100]
                if len(todo["description"]) > 100:
                    desc += "..."
                lines.append(f"      {desc}")
        lines.append("")

    # Archives
    archives = todos.get("archives", [])
    if archives:
        lines.append(f"=== Archived Phases ({len(archives)}) ===")
        for archive in archives:
            lines.append(f"  - {archive.get('filename', 'unknown')}")
            if archive.get("phase_name"):
                lines.append(f"    Phase: {archive['phase_name']}")

    if not current and not archives:
        lines.append("No todos found.")

    return "\n".join(lines)


def _format_agent_log(subject: str, data: dict[str, Any]) -> str:
    """Shared job/thread log formatting."""
    log_lines = data.get("lines", [])
    total = data.get("total_lines", 0)
    filtered = data.get("filtered", False)
    archived = data.get("archived", False)

    header = f"Log for {subject}"
    if archived:
        header += " (archived — pod is gone, served from S3)"
    if filtered:
        header += " (filtered)"
    header += f" — showing {len(log_lines)} of {total} lines"

    lines = [header, ""]
    for line in log_lines:
        lines.append(line)

    return "\n".join(lines)


def format_job_log(job_id: str, data: dict[str, Any]) -> str:
    """Format job log output."""
    return _format_agent_log(f"job {job_id}", data)


def format_thread_log(thread_id: str, data: dict[str, Any]) -> str:
    """Format session (persistent thread) log output."""
    return _format_agent_log(f"session {thread_id}", data)


def format_llm_requests(job_id: str, data: dict[str, Any]) -> str:
    """Format LLM request listing as a compact table."""
    entries = data.get("entries", [])
    total = data.get("total", 0)
    offset = data.get("offset", 0)
    has_more = data.get("hasMore", False)

    if not entries:
        return f"No LLM requests found for job {job_id}."

    lines = [
        f"LLM requests for job {job_id} (offset {offset}, showing {len(entries)} of {total}):\n"
    ]

    for i, entry in enumerate(entries, offset + 1):
        model = entry.get("model", "?")
        timestamp = entry.get("timestamp", "?")
        tokens = entry.get("token_usage", {})
        prompt_t = tokens.get("prompt_tokens", tokens.get("prompt", "?"))
        comp_t = tokens.get("completion_tokens", tokens.get("completion", "?"))
        total_t = tokens.get("total_tokens", tokens.get("total", "?"))
        tool_calls = entry.get("tool_calls", [])
        tool_names = (
            ", ".join(
                (tc.get("name", "?") if isinstance(tc, dict) else str(tc))
                for tc in tool_calls
            )
            if tool_calls
            else "(none)"
        )
        doc_id = entry.get("id", entry.get("_id", "?"))
        iteration = entry.get("iteration", "?")

        lines.append(
            f"[{i}] {model} {timestamp} tokens=[{prompt_t}/{comp_t}/{total_t}] "
            f"iter={iteration} -> {tool_names} (doc_id: {doc_id})"
        )

    if has_more:
        lines.append(f"\n... more entries (use offset={offset + len(entries)})")

    return "\n".join(lines)


def format_shell_state(job_id: str, data: dict[str, Any]) -> str:
    """Format shell state from an agent."""
    tabs = data.get("tabs", [])
    message = data.get("message", "")

    if not tabs:
        return f"Shell state for job {job_id}: {message or 'No active shell sessions'}"

    lines = [f"Shell state for job {job_id} ({len(tabs)} tab(s)):\n"]

    for tab in tabs:
        name = tab.get("name", "?")
        tab_type = tab.get("type", "?")
        total = tab.get("total_lines", "?")
        withheld = (
            f" ({tab['redacted_count']} secret-shaped value(s) redacted)"
            if tab.get("redacted")
            else ""
        )
        lines.append(f"  [{name}] type={tab_type} lines={total}{withheld}")
        output = tab.get("recent_output", "")
        if output:
            # Show last 20 lines, indented
            recent = output.splitlines()[-20:]
            for line in recent:
                lines.append(f"    | {line}")
        lines.append("")

    return "\n".join(lines)


def format_graph_changes(changes: dict[str, Any]) -> str:
    """Format graph changes timeline."""
    summary = changes.get("summary", {})
    deltas = changes.get("deltas", [])
    time_range = changes.get("timeRange")

    lines = [f"Graph changes for job: {changes.get('jobId', 'unknown')}\n"]

    # Summary
    lines.append("=== Summary ===")
    lines.append(f"  Total tool calls: {summary.get('totalToolCalls', 0)}")
    lines.append(f"  Graph operations: {summary.get('graphToolCalls', 0)}")
    lines.append(f"  Nodes created: {summary.get('nodesCreated', 0)}")
    lines.append(f"  Nodes deleted: {summary.get('nodesDeleted', 0)}")
    lines.append(f"  Nodes modified: {summary.get('nodesModified', 0)}")
    lines.append(f"  Relationships created: {summary.get('relationshipsCreated', 0)}")
    lines.append(f"  Relationships deleted: {summary.get('relationshipsDeleted', 0)}")

    if time_range:
        lines.append(
            f"\nTime range: {time_range.get('start')} to {time_range.get('end')}"
        )

    # Recent deltas (last 10)
    if deltas:
        lines.append(f"\n=== Recent Operations (last 10 of {len(deltas)}) ===")
        for delta in deltas[-10:]:
            idx = delta.get("toolCallIndex", "?")
            query = delta.get("cypherQuery", "")[:80]
            if len(delta.get("cypherQuery", "")) > 80:
                query += "..."
            changes_summary = delta.get("changes", {})
            nodes_created = len(changes_summary.get("nodesCreated", []))
            rels_created = len(changes_summary.get("relationshipsCreated", []))
            lines.append(f"  [{idx}] {query}")
            if nodes_created or rels_created:
                lines.append(f"       +{nodes_created} nodes, +{rels_created} rels")

    return "\n".join(lines)


def format_llm_request(request: dict[str, Any]) -> str:
    """Format LLM request/response from an llm_requests row."""
    lines = [f"LLM Request: {request.get('id', request.get('_id', 'unknown'))}\n"]

    lines.append(f"Job: {request.get('job_id', 'N/A')}")
    lines.append(f"Model: {request.get('model', 'N/A')}")
    lines.append(f"Timestamp: {request.get('timestamp', 'N/A')}")
    if request.get("iteration") is not None:
        lines.append(f"Iteration: {request['iteration']}")
    if request.get("latency_ms") is not None:
        lines.append(f"Latency: {request['latency_ms']}ms")

    # Token usage from metrics
    metrics = request.get("metrics", {})
    usage = metrics.get("token_usage", {})
    if usage:
        parts = [f"{usage.get('prompt_tokens', 0)} prompt"]
        parts.append(f"{usage.get('completion_tokens', 0)} completion")
        if usage.get("reasoning_tokens"):
            parts.append(f"{usage['reasoning_tokens']} reasoning")
        lines.append(f"Tokens: {', '.join(parts)}")

    # Response metadata
    resp = request.get("response", {})
    resp_meta = resp.get("response_metadata", {})
    if resp_meta.get("finish_reason"):
        lines.append(f"Finish reason: {resp_meta['finish_reason']}")
    if resp_meta.get("system_fingerprint"):
        lines.append(f"System fingerprint: {resp_meta['system_fingerprint']}")

    # Tool definitions
    req_data = request.get("request", {})
    tools = req_data.get("tools", [])
    if tools:
        tool_names = [t.get("function", {}).get("name", "?") for t in tools]
        lines.append(f"\n=== Tool Definitions ({len(tools)}) ===")
        lines.append(", ".join(tool_names))

    # Model kwargs
    model_kwargs = req_data.get("model_kwargs", {})
    if model_kwargs:
        lines.append("\n=== Model Parameters ===")
        for k, v in model_kwargs.items():
            lines.append(f"  {k}: {v}")

    # Messages (last 5)
    messages = req_data.get("messages", [])
    if messages:
        lines.append(f"\n=== Messages ({len(messages)}) ===")
        for msg in messages[-5:]:
            role = msg.get("role", "unknown")
            content = msg.get("content", "")
            if isinstance(content, str):
                preview = content[:200]
                if len(content) > 200:
                    preview += "..."
            else:
                preview = str(content)[:200]
            lines.append(f"[{role}]: {preview}")

    # Response content
    if resp:
        lines.append("\n=== Response ===")
        content = resp.get("content", "")
        if content:
            preview = content[:500]
            if len(content) > 500:
                preview += "..."
            lines.append(preview)

        # Tool calls in response
        tool_calls = resp.get("tool_calls", [])
        if tool_calls:
            lines.append(f"\nTool calls ({len(tool_calls)}):")
            for tc in tool_calls:
                lines.append(f"  - {tc.get('name', '?')} (id: {tc.get('id', '?')})")

    return "\n".join(lines)


# =============================================================================
# Search Helpers
# =============================================================================


def entry_matches(entry: dict[str, Any], query: str) -> bool:
    """Check if audit entry matches search query (case-insensitive)."""
    # Check tool name/args/result
    tool = entry.get("tool", {})
    if tool:
        if query in tool.get("name", "").lower():
            return True
        if query in json.dumps(tool.get("arguments", {})).lower():
            return True
        if query in str(tool.get("result_preview", "") or "").lower():
            return True
        if query in str(tool.get("error", "") or "").lower():
            return True

    # Check LLM content
    llm = entry.get("llm", {})
    if llm:
        if query in (llm.get("response_content_preview", "") or "").lower():
            return True
        # Search tool call names within LLM responses
        for tc in llm.get("tool_calls", []) or []:
            if query in tc.get("name", "").lower():
                return True

    # Check error
    if query in str(entry.get("error", "") or "").lower():
        return True

    return False


# =============================================================================
# Action Tool Formatters
# =============================================================================


def format_action_result(
    action: str,
    job_id: str,
    result: dict[str, Any],
    **extra: Any,
) -> str:
    """Format a generic action result."""
    status = result.get("status", "unknown")
    lines = [f"Action: {action}", f"Job: {job_id}", f"Status: {status}"]

    if extra.get("feedback"):
        lines.append(f"Feedback: {extra['feedback'][:200]}")
    if extra.get("agent_id"):
        lines.append(f"Agent: {extra['agent_id']}")

    # Include any extra fields from the response
    for key, value in result.items():
        if key == "status":
            continue
        if isinstance(value, str) and len(value) > 300:
            value = value[:300] + "..."
        lines.append(f"{key}: {value}")

    return "\n".join(lines)


def format_created_job(
    result: dict[str, Any], config_name: str, *, expert: str | None = None
) -> str:
    """Format the result of creating a new job.

    ``expert`` is the reference the caller actually selected. It is echoed
    because ``Config:`` alone cannot report a database expert — that choice
    lands on the ``worker_base`` base config, so the line would say
    ``worker_base`` for every DB expert and the caller could not tell which
    one it hired.
    """
    job_id = result.get("id", "unknown")
    lines = [
        "Job created successfully.",
        f"Job ID: {job_id}",
    ]
    if expert:
        lines.append(f"Expert: {expert}")
    lines += [
        f"Config: {config_name}",
        f"Status: {result.get('status', 'created')}",
    ]
    workspace = result.get("workspace_contract")
    if isinstance(workspace, dict):
        lines.append(
            "Workspace: "
            f"requested={workspace.get('requested_backend') or 'default'}, "
            f"assigned={workspace.get('assigned_backend') or 'unknown'}, "
            f"effective={workspace.get('effective_backend') or 'pending'}"
        )

    if result.get("description"):
        desc = result["description"]
        if len(desc) > 200:
            desc = desc[:200] + "..."
        lines.append(f"Description: {desc}")

    lines.append(
        "\nDispatch: Queued for automatic workspace provisioning and agent "
        "assignment. Monitor the job status; manual assignment is only an "
        "administrative override."
    )
    return "\n".join(lines)


def format_datasource_test(datasource_id: str, result: dict[str, Any]) -> str:
    """Format a connector test result."""
    status = result.get("status", "unknown")
    message = result.get("message", "")
    icon = "OK" if status == "ok" else "FAILED"
    lines = [
        f"Connector test: {icon}",
        f"Connector ID: {datasource_id}",
        f"Status: {status}",
    ]
    if message:
        lines.append(f"Message: {message}")
    return "\n".join(lines)


def format_action_error(action: str, job_id: str, error: Exception) -> str:
    """Format an action error."""
    error_msg = str(error)

    # Extract detail from httpx HTTPStatusError
    if hasattr(error, "response"):
        try:
            detail = error.response.json().get("detail", error_msg)  # type: ignore[union-attr]
            error_msg = detail
        except Exception:
            error_msg = f"HTTP {error.response.status_code}: {error_msg}"  # type: ignore[union-attr]

    return f"Action '{action}' failed for job {job_id}:\n{error_msg}"


# =============================================================================
# Git History Formatters
# =============================================================================


def format_commits(
    job_id: str,
    result: dict[str, Any],
    ref: str = "main",
    since_ref: str | None = None,
) -> str:
    """Format commit list."""
    commits = result.get("commits", [])
    total = result.get("total_commits", len(commits))

    if not commits:
        if since_ref:
            return f"No commits found between {since_ref} and {ref} for job {job_id}."
        return f"No commits found for job {job_id}."

    header = f"Commits for job {job_id}"
    if since_ref:
        header += f" ({since_ref}...{ref})"
    else:
        header += f" (ref: {ref})"
    lines = [f"{header} — {total} commit(s):\n"]

    for c in commits:
        sha_short = c.get("sha", "")[:8]
        message = c.get("message", "").split("\n")[0]  # First line only
        author = c.get("author", "unknown")
        date = c.get("date", "")
        lines.append(f"  {sha_short} {message}")
        if author or date:
            lines.append(f"           {author} — {date}")

    return "\n".join(lines)


def format_diff(
    job_id: str,
    base: str,
    head: str,
    diff_text: str,
    max_chars: int = 50000,
) -> str:
    """Format diff output with optional truncation."""
    if not diff_text:
        return f"No differences between {base} and {head} for job {job_id}."

    # Count changed files from diff headers
    file_count = diff_text.count("\ndiff --git ")
    if diff_text.startswith("diff --git "):
        file_count += 1

    lines = [
        f"Diff for job {job_id}: {base}...{head}",
        f"Files changed: {file_count}",
        "---",
    ]

    header = "\n".join(lines) + "\n"

    if max_chars > 0 and len(diff_text) > max_chars:
        truncated = diff_text[:max_chars]
        return (
            header
            + truncated
            + f"\n\n[truncated — diff exceeds {max_chars} characters]"
        )

    return header + diff_text


def filter_diff_by_file(diff_text: str, file_path: str) -> str:
    """Extract diff sections for a specific file."""
    sections = diff_text.split("\ndiff --git ")
    if diff_text.startswith("diff --git "):
        # First section doesn't have leading newline
        first = sections[0]
        rest = sections[1:]
    else:
        first = ""
        rest = sections[1:] if len(sections) > 1 else sections

    matching = []
    # Check the first section
    if first and file_path in first.split("\n")[0]:
        matching.append(first)
    # Check remaining sections
    for section in rest:
        header_line = section.split("\n")[0]
        if file_path in header_line:
            matching.append("diff --git " + section)

    if not matching:
        return f"No changes to '{file_path}' in this diff."

    return "\n".join(matching)


def format_file_listing(
    job_id: str,
    path: str,
    entries: list[dict[str, Any]],
    ref: str | None = None,
) -> str:
    """Format directory listing."""
    if not entries:
        return f"No files found at '{path or '/'}' for job {job_id}."

    ref_label = ref or "HEAD"
    lines = [f"Files in job {job_id} at '{path or '/'}' (ref: {ref_label}):\n"]

    # Sort: directories first, then files
    dirs = [e for e in entries if e.get("type") == "dir"]
    files = [e for e in entries if e.get("type") != "dir"]

    for d in sorted(dirs, key=lambda x: x.get("name", "")):
        lines.append(f"  [dir]  {d['name']}/")

    for f in sorted(files, key=lambda x: x.get("name", "")):
        size = f.get("size", 0)
        if size >= 1024:
            size_str = f"{size / 1024:.1f} KB"
        else:
            size_str = f"{size} B"
        lines.append(f"  [file] {f['name']}  ({size_str})")

    lines.append(f"\nTotal: {len(dirs)} directories, {len(files)} files")
    return "\n".join(lines)


def format_tags(job_id: str, tags: list[dict[str, Any]]) -> str:
    """Format tag list."""
    if not tags:
        return f"No tags found for job {job_id}."

    lines = [f"Tags for job {job_id} ({len(tags)}):\n"]
    for t in tags:
        name = t.get("name", "")
        sha = t.get("sha", "")[:8]
        message = t.get("message", "")
        line = f"  {name} ({sha})"
        if message:
            line += f" — {message.split(chr(10))[0]}"
        lines.append(line)

    return "\n".join(lines)


def format_git_error(operation: str, job_id: str, error: Exception) -> str:
    """Format a git operation error."""
    error_msg = str(error)

    if hasattr(error, "response"):
        try:
            detail = error.response.json().get("detail", error_msg)  # type: ignore[union-attr]
            error_msg = detail
        except Exception:
            status = error.response.status_code  # type: ignore[union-attr]
            if status == 503:
                error_msg = "Gitea is not available — git history tools require a running Gitea instance."
            else:
                error_msg = f"HTTP {status}: {error_msg}"

    return f"Failed to {operation} for job {job_id}:\n{error_msg}"


# =============================================================================
# Workspace & Job Context Formatters
# =============================================================================


def format_frozen_job(job_id: str, data: dict[str, Any]) -> str:
    """Format frozen job review data."""
    lines = [f"Frozen job review: {job_id}\n"]

    if data.get("summary"):
        lines.append(f"Summary:\n{data['summary']}\n")

    if data.get("confidence") is not None:
        lines.append(f"Confidence: {_fmt_confidence(data['confidence'])}")

    if data.get("deliverables"):
        lines.append("Deliverables:")
        for d in data["deliverables"]:
            if isinstance(d, dict):
                name = d.get("name", d.get("path", "unknown"))
                desc = d.get("description", "")
                lines.append(f"  - {name}: {desc}" if desc else f"  - {name}")
            else:
                lines.append(f"  - {d}")
        lines.append("")

    if data.get("notes"):
        lines.append(f"Agent notes:\n{data['notes']}")

    # Include any other top-level fields
    skip = {"summary", "confidence", "deliverables", "notes"}
    for key, value in data.items():
        if key not in skip:
            if isinstance(value, str) and len(value) > 300:
                value = value[:300] + "..."
            lines.append(f"{key}: {value}")

    return "\n".join(lines)


def format_workspace_overview(job_id: str, data: dict[str, Any]) -> str:
    """Format workspace overview."""
    if not data.get("has_workspace"):
        return f"No workspace found for job {job_id}."

    lines = [f"Workspace overview for job {job_id}\n"]

    # File listing (repo root; directories carry no size)
    files = data.get("files", [])
    if files:
        lines.append(f"Files ({len(files)}):")
        for f in files:
            name = f.get("name", "unknown")
            if f.get("type") == "dir":
                lines.append(f"  {name}/")
                continue
            size = f.get("size", 0)
            if size >= 1024:
                size_str = f"{size / 1024:.1f} KB"
            else:
                size_str = f"{size} B"
            lines.append(f"  {name} ({size_str})")
        lines.append("")

    # workspace.md preview
    ws_md = data.get("workspace_md")
    if ws_md:
        lines.append("=== workspace.md (preview) ===")
        lines.append(ws_md[:1000])
        if len(ws_md) > 1000:
            lines.append("... (truncated)")
        lines.append("")

    # plan.md preview
    plan_md = data.get("plan_md")
    if plan_md:
        lines.append("=== plan.md (preview) ===")
        lines.append(plan_md[:1000])
        if len(plan_md) > 1000:
            lines.append("... (truncated)")
        lines.append("")

    # Todo summary
    todos = data.get("todos")
    if todos:
        todo_list = todos.get("todos", [])
        completed = sum(1 for t in todo_list if t.get("status") == "completed")
        total = len(todo_list)
        lines.append(f"Todos: {completed}/{total} completed")

    archive_count = data.get("archive_count", 0)
    if archive_count:
        lines.append(f"Archived phases: {archive_count}")

    return "\n".join(lines)


def _liveness_lines(data: dict[str, Any]) -> list[str]:
    """Render the shared liveness contract (E3) into compact lines.

    Never fabricates progress: when the state is ``unavailable`` the lines
    say WHY the telemetry is missing instead of pretending 0% / not-stuck.
    """
    lines: list[str] = [f"Status: {data.get('status', 'unknown')}"]
    state = data.get("state")
    if state:
        line = f"Liveness: {state}"
        last_activity = data.get("last_activity_at")
        if last_activity:
            line += f" — last activity {last_activity}"
        lines.append(line)
    reasons = data.get("reasons")
    if isinstance(reasons, list) and reasons:
        lines.append("Reasons:")
        lines.extend(f"  - {reason}" for reason in reasons)
    elapsed = data.get("elapsed_seconds")
    if elapsed is not None:
        mins = int(elapsed) // 60
        secs = int(elapsed) % 60
        lines.append(f"Elapsed since creation: {mins}m {secs}s")
    for source in data.get("sources") or []:
        if not isinstance(source, dict):
            continue
        if source.get("status") in ("fresh", "empty", None):
            continue
        note = f"[{source.get('name')}: {source.get('status')}"
        if source.get("reason"):
            note += f" — {source['reason']}"
        note += "]"
        lines.append(note)
    return lines


def format_job_progress(job_id: str, data: dict[str, Any]) -> str:
    """Format job liveness/progress data (E1/E3).

    The old endpoint stub always returned ``progress_percent=0`` /
    ``eta_seconds=None``; rendering that as "Progress: 0.0%" manufactured a
    fact. This renders the honest liveness contract instead and refuses to
    invent a percentage — legacy payloads that still carry the stubbed
    fields simply have them ignored.
    """
    lines = [f"Progress for job {job_id}\n"]
    lines.extend(_liveness_lines(data))
    observed = data.get("observed_at")
    if observed:
        lines.append(f"Observed at: {observed}")
    return "\n".join(lines)


def format_workspace_error(operation: str, job_id: str, error: Exception) -> str:
    """Format a workspace operation error."""
    error_msg = str(error)

    if hasattr(error, "response"):
        try:
            detail = error.response.json().get("detail", error_msg)  # type: ignore[union-attr]
            error_msg = detail
        except Exception:
            error_msg = f"HTTP {error.response.status_code}: {error_msg}"  # type: ignore[union-attr]

    return f"Failed to {operation} for job {job_id}:\n{error_msg}"


# =============================================================================
# Evidence Manifest Formatters (E4 — officer_supervision_surface §3.3)
# =============================================================================


def _evidence_entry_lines(entry: dict[str, Any]) -> list[str]:
    lines = [
        f"  {entry.get('id', '?')} · {entry.get('kind', 'unknown')} · "
        f"{truncate_text(entry.get('label'), limit=80)}"
    ]
    meta: list[str] = []
    if entry.get("media_type"):
        meta.append(str(entry["media_type"]))
    if entry.get("byte_size") is not None:
        meta.append(f"{entry['byte_size']} B")
    if entry.get("sha256"):
        meta.append(f"sha256:{str(entry['sha256'])[:12]}…")
    if meta:
        lines.append(f"    {' · '.join(meta)}")
    provenance: list[str] = []
    source = entry.get("source")
    if isinstance(source, dict) and source.get("revision"):
        provenance.append(f"revision {str(source['revision'])[:12]}")
    if entry.get("producer"):
        provenance.append(f"producer {entry['producer']}")
    if entry.get("captured_at"):
        provenance.append(f"captured {entry['captured_at']}")
    if provenance:
        lines.append(f"    {' · '.join(provenance)}")
    availability = entry.get("availability")
    if availability and availability != "available":
        note = f"    availability: {availability}"
        if entry.get("availability_reason"):
            note += f" — {entry['availability_reason']}"
        lines.append(note)
    return lines


def format_evidence_list(job_id: str, data: dict[str, Any]) -> str:
    """Format the evidence manifest for a job."""
    entries = data.get("entries") or []
    if not entries:
        return (
            f"No evidence recorded for job {job_id}. The manifest is written "
            "at completion; if the worker published nothing, delegate a "
            "tester/recon job instead of browsing its workspace."
        )
    lines = [f"Evidence manifest for job {job_id} ({len(entries)} entr(y/ies)):"]
    if data.get("recorded_at"):
        lines.append(f"Recorded at: {data['recorded_at']}")
    lines.append("")
    for entry in entries:
        lines.extend(_evidence_entry_lines(entry))
        lines.append("")
    lines.append("Read one with read_job_evidence(job_id, evidence_id).")
    return "\n".join(lines)


def format_evidence_read(job_id: str, evidence_id: str, data: dict[str, Any]) -> str:
    """Format one evidence read: bounded text page or safe binary metadata."""
    entry = data.get("entry") or {}
    header = [
        f"Evidence {evidence_id} for job {job_id}:",
        *_evidence_entry_lines(entry),
    ]
    if data.get("content") is not None:
        offset = int(data.get("offset") or 0)
        total = data.get("total_chars")
        window = [
            "--- content"
            + (
                f" (chars {offset}..{offset + len(str(data['content']))}"
                + (f" of {total}" if total is not None else "")
                + ")"
            )
            + " ---",
            str(data["content"]),
        ]
        if data.get("truncated"):
            next_offset = offset + len(str(data["content"]))
            window.append(f"[truncated — continue with offset={next_offset}]")
        note = data.get("note")
        if note:
            window.append(f"[{note}]")
        if data.get("redacted"):
            # The API has always said so; the officer's text view did not, so
            # a withheld value read exactly like one the worker never wrote.
            window.append(
                f"[{data.get('redacted_count')} secret-shaped value(s) redacted "
                "from this page]"
            )
        return "\n".join(header + window)
    # Binary / screenshot: bytes are a typed attachment and are deliberately
    # never interpolated into ordinary formatter text.
    lines = header
    if data.get("note"):
        lines.append(f"[{data['note']}]")
    attachment = data.get("attachment")
    if isinstance(attachment, dict):
        lines.append(
            "One bounded image attachment is available for multimodal delivery "
            f"({attachment.get('media_type')}, {attachment.get('byte_size')} bytes, "
            f"{attachment.get('width')}x{attachment.get('height')})."
        )
    return "\n".join(lines)


def format_completion_report(job_id: str, data: dict[str, Any]) -> str:
    """Format the server-recorded completion report entry."""
    report = data.get("report") or {}
    lines = [f"Completion report for job {job_id}:"]
    if data.get("recorded_at"):
        lines.append(f"Recorded at: {data['recorded_at']}")
    if data.get("source_revision"):
        lines.append(f"Completion revision: {str(data['source_revision'])[:12]}")
    lines.append("")
    if report.get("summary"):
        lines.append(f"Summary:\n{report['summary']}\n")
    if report.get("confidence") is not None:
        lines.append(f"Confidence: {_fmt_confidence(report['confidence'])}")
    deliverables = report.get("deliverables") or []
    if deliverables:
        lines.append(f"Deliverables ({len(deliverables)}):")
        for item in deliverables:
            lines.append(f"  - {item}")
    if report.get("notes"):
        lines.append(f"Notes:\n{report['notes']}")
    if not (report.get("summary") or deliverables):
        lines.append("(report carries no summary or deliverables)")
    if data.get("redacted"):
        lines.append(
            f"[{data.get('redacted_count')} secret-shaped value(s) redacted from "
            "this report]"
        )
    return "\n".join(lines)


# =============================================================================
# Monitoring & Database Formatters
# =============================================================================


def format_job_stats(data: dict[str, Any]) -> str:
    """Format job queue statistics."""
    total = data.get("total", 0)
    lines = [f"Job Statistics (total: {total})\n"]

    for status in (
        "created",
        "processing",
        "pending_review",
        "completed",
        "failed",
        "cancelled",
        "blocked_undelivered",
    ):
        count = data.get(status, 0)
        if count or status in ("created", "processing", "completed"):
            lines.append(f"  {status}: {count}")

    return "\n".join(lines)


def format_daily_stats(data: list[dict[str, Any]], days: int) -> str:
    """Format daily job statistics."""
    if not data:
        return f"No job activity in the past {days} day(s)."

    lines = [f"Daily Job Statistics (past {days} days):\n"]
    for day in data:
        date = str(day.get("date", "?"))[:10]
        created = day.get("jobs_created", 0)
        completed = day.get("jobs_completed", 0)
        failed = day.get("jobs_failed", 0)
        cancelled = day.get("jobs_cancelled", 0)
        blocked = day.get("jobs_blocked_undelivered", 0)
        lines.append(
            f"  {date}  created:{created}  completed:{completed}  "
            f"failed:{failed}  cancelled:{cancelled}  "
            f"blocked/undelivered:{blocked}"
        )
    return "\n".join(lines)


def format_agent_stats(data: dict[str, Any]) -> str:
    """Format agent workforce summary."""
    total = data.get("total", 0)
    lines = [f"Agent Statistics (total: {total})\n"]

    for status in ("ready", "working", "booting", "completed", "offline", "failed"):
        count = data.get(status, 0)
        if count or status in ("ready", "working", "offline"):
            lines.append(f"  {status}: {count}")

    return "\n".join(lines)


def format_stuck_jobs(data: dict[str, Any] | list[dict[str, Any]]) -> str:
    """Format stuck jobs list (E3: liveness-backed rows).

    Rows carry the shared liveness contract: ``state`` (suspected_stuck, or
    ``unavailable`` when the activity evidence itself is unreachable — shown
    honestly, never silently promoted to "stuck"), reasons, last activity,
    and the job's description for triage.
    """
    if isinstance(data, list):
        jobs = data
        threshold = None
        source = "legacy"
    else:
        jobs = data.get("jobs") or []
        threshold = data.get("threshold_minutes")
        source = str(data.get("threshold_source") or "unknown")
    policy = (
        f"threshold: {threshold} minutes, source: {source}"
        if threshold is not None
        else f"threshold source: {source}"
    )
    if not jobs:
        return f"No stuck jobs found ({policy})."

    lines = [f"Stuck jobs ({len(jobs)}, {policy}):\n"]
    for job in jobs:
        job_id = job.get("id", job.get("job_id", "unknown"))
        status = job.get("status", "unknown")

        lines.append(f"  {job_id}")
        lines.append(f"    Status: {status}")
        if job.get("state"):
            lines.append(f"    Liveness: {job['state']}")
        if job.get("description"):
            lines.append(
                f"    Description: {truncate_text(job['description'], limit=100)}"
            )
        last_activity = job.get("last_activity_at")
        if last_activity:
            lines.append(f"    Last activity: {last_activity}")
        elif job.get("updated_at"):
            lines.append(f"    Last update: {job['updated_at']}")
        reasons = job.get("reasons")
        if isinstance(reasons, list) and reasons:
            for reason in reasons:
                lines.append(f"    Reason: {reason}")
        else:
            component = job.get("stuck_component") or job.get("component") or ""
            reason = job.get("stuck_reason") or job.get("reason") or ""
            if component and component != "unknown":
                lines.append(f"    Component: {component}")
            if reason:
                lines.append(f"    Reason: {reason}")

    return "\n".join(lines)


def format_agents(
    agents: list[dict[str, Any]], status_filter: str | None = None
) -> str:
    """Format agent list."""
    if not agents:
        filter_msg = f" with status '{status_filter}'" if status_filter else ""
        return f"No agents found{filter_msg}."

    lines = [f"Agents ({len(agents)}):\n"]
    for a in agents:
        agent_id = str(a.get("id", "unknown"))[:8]
        status = a.get("status", "unknown")
        config = a.get("config_name", a.get("config", "N/A"))
        hostname = a.get("hostname", "")
        current_job = a.get("current_job_id", "")
        heartbeat = a.get("last_heartbeat", "")

        icon = {
            "ready": "OK",
            "working": ">>",
            "booting": "..",
            "offline": "--",
            "failed": "!!",
            "completed": "++",
        }.get(status, "??")

        lines.append(f"  [{icon}] {agent_id}  ({status})")
        if config:
            lines.append(f"       Config: {config}")
        if hostname:
            lines.append(f"       Host: {hostname}")
        if current_job:
            lines.append(f"       Job: {current_job}")
        if heartbeat:
            lines.append(f"       Heartbeat: {heartbeat}")

    return "\n".join(lines)


def format_experts(experts: list[dict[str, Any]]) -> str:
    """Format expert config list."""
    if not experts:
        return "No expert configurations found."

    lines = [f"Expert Configurations ({len(experts)}):\n"]
    for e in experts:
        eid = e.get("id", "unknown")
        name = e.get("display_name", eid)
        desc = e.get("description", "")
        tags = e.get("tags", [])

        lines.append(f"  {eid}: {name}")
        if desc:
            lines.append(f"    {desc}")
        if tags:
            lines.append(f"    Tags: {', '.join(tags)}")

    return "\n".join(lines)


def format_skills(skills: list[dict[str, Any]]) -> str:
    """Format skill catalog list."""
    if not skills:
        return "No skills found."

    lines = [f"Skills ({len(skills)}):\n"]
    for s in skills:
        sid = s.get("id", "unknown")
        name = s.get("name", sid)
        source = s.get("source", "")
        desc = s.get("description", "")
        tags = s.get("tags", [])
        suffix = f" [{source}]" if source else ""
        lines.append(f"  {name}{suffix} (id: {sid})")
        if desc:
            lines.append(f"    {desc}")
        if tags:
            lines.append(f"    Tags: {', '.join(tags)}")

    return "\n".join(lines)


def format_skill_detail(skill_id: str, data: dict[str, Any]) -> str:
    """Format skill detail (metadata + SKILL.md body + file list)."""
    files = data.get("files", {})
    body = files.get("SKILL.md", "")
    extra = sorted(p for p in files if p != "SKILL.md")
    lines = [f"Skill: {data.get('name', skill_id)}"]
    if data.get("description"):
        lines.append(f"  {data['description']}")
    lines.append(f"  Bundled files: {', '.join(extra) if extra else '(none)'}")
    lines.append("\n--- SKILL.md ---")
    lines.append(body)
    return "\n".join(lines)


def format_models(data: dict[str, Any]) -> str:
    """Format model catalog for AI-friendly display."""
    lines: list[str] = []

    groups = data.get("groups", [])
    if groups:
        lines.append("Models by Provider:")
        for group in groups:
            provider = group.get("group", group.get("provider", "unknown"))
            status = "ready" if group.get("configured") else "needs API key"
            models = group.get("models", [])
            lines.append(f"\n  {provider} ({status}):")
            for model_id in models:
                lines.append(f"    - {model_id}")

    presets = data.get("presets", [])
    if presets:
        lines.append("\nPresets (strategic + tactical pairs):")
        for preset in presets:
            label = preset.get("label", "unnamed")
            status = "ready" if preset.get("configured") else "needs API key"
            lines.append(f"  {label} ({status}):")
            lines.append(f"    strategic: {preset.get('strategic', '?')}")
            lines.append(f"    tactical:  {preset.get('tactical', '?')}")

    if not lines:
        return "No models found in catalog."

    lines.append(
        '\nUsage: create_job(..., config_override={"llm": {"model": "<model_id>"}})'
    )

    return "\n".join(lines)


def format_expert_detail(expert_id: str, data: dict[str, Any]) -> str:
    """Format expert config detail."""
    lines = [f"Expert: {expert_id}\n"]

    if data.get("display_name"):
        lines.append(f"Name: {data['display_name']}")
    if data.get("description"):
        lines.append(f"Description: {data['description']}")
    if data.get("tags"):
        lines.append(f"Tags: {', '.join(data['tags'])}")

    # Config section
    config = data.get("config", {})
    if config:
        llm = config.get("llm")
        if llm:
            lines.append(f"\nLLM: {llm.get('model', 'N/A')}")
            if llm.get("base_url"):
                lines.append(f"  Base URL: {llm['base_url']}")

        tools = config.get("tools", {})
        if tools:
            lines.append("\nTools:")
            for category, tool_list in tools.items():
                if tool_list:
                    lines.append(f"  {category}: {', '.join(tool_list)}")

    # Instructions
    instructions = data.get("instructions")
    if instructions:
        preview = instructions[:1000]
        if len(instructions) > 1000:
            preview += "\n... (truncated)"
        lines.append(f"\n=== Instructions ===\n{preview}")

    return "\n".join(lines)


def format_datasources(
    datasources: list[dict[str, Any]], type_filter: str | None = None
) -> str:
    """Format a connector list."""
    if not datasources:
        filter_msg = f" of type '{type_filter}'" if type_filter else ""
        return f"No connectors found{filter_msg}."

    lines = [f"Connectors ({len(datasources)}):\n"]
    for ds in datasources:
        ds_id = str(ds.get("id", "unknown"))
        name = ds.get("name", "unknown")
        ds_type = ds.get("type", "unknown")
        read_only = ds.get("read_only", True)
        job_id = ds.get("job_id")
        scope_mode = ds.get("scope_mode", "all")
        project_ids = [str(value) for value in (ds.get("project_ids") or [])]
        if job_id:
            scope = "legacy job-scoped"
        elif scope_mode == "projects":
            scope = f"projects ({', '.join(project_ids) or 'none'})"
        else:
            scope = "all work contexts"

        lines.append(f"  {ds_id}  {name} ({ds_type})")
        lines.append(f"    Scope: {scope}  Read-only: {read_only}")
        lines.append(
            "    "
            f"Published: {bool(ds.get('is_global'))}  "
            f"Auto-attach default: {bool(ds.get('auto_attach'))}"
        )
        lines.append(f"    Policy revision: {ds.get('policy_revision', 'unknown')}")
        if job_id:
            lines.append(f"    Job: {job_id}")

    return "\n".join(lines)


def format_datasource_detail(datasource: dict[str, Any]) -> str:
    """Format one connector's redacted management state without lossy IDs."""
    ds_id = str(datasource.get("id", "unknown"))
    project_ids = [str(value) for value in (datasource.get("project_ids") or [])]
    scope_mode = datasource.get("scope_mode", "all")
    scope = (
        f"projects ({', '.join(project_ids) or 'none'})"
        if scope_mode == "projects"
        else "all work contexts"
    )
    lines = [
        f"Connector: {datasource.get('name', 'unknown')}",
        f"ID: {ds_id}",
        f"Policy revision: {datasource.get('policy_revision', 'unknown')}",
        f"Type: {datasource.get('type', 'unknown')}",
        f"Scope: {scope}",
        f"Published: {bool(datasource.get('is_global'))}",
        f"Auto-attach default: {bool(datasource.get('auto_attach'))}",
        f"Read-only: {datasource.get('read_only', True)}",
    ]
    if datasource.get("description"):
        lines.append(f"Description: {datasource['description']}")
    if datasource.get("connection_url"):
        lines.append(f"URL: {_mask_url(str(datasource['connection_url']))}")
    if datasource.get("config"):
        lines.append(
            "Config: " + json.dumps(datasource["config"], sort_keys=True, default=str)
        )
    return "\n".join(lines)


def format_tables(tables: list[dict[str, Any]]) -> str:
    """Format database table list."""
    if not tables:
        return "No tables found."

    lines = ["Database Tables:\n"]
    for t in tables:
        name = t.get("table_name", t.get("name", "unknown"))
        count = t.get("row_count", t.get("count", "?"))
        lines.append(f"  {name}: {count} rows")

    return "\n".join(lines)


def format_table_data(table_name: str, data: dict[str, Any]) -> str:
    """Format paginated table data."""
    rows = data.get("data", data.get("rows", []))
    total = data.get("total", len(rows))
    page = data.get("page", 1)
    has_more = data.get("hasMore", False)

    if not rows:
        return f"No data in table '{table_name}'."

    lines = [f"Table: {table_name} (page {page}, {len(rows)} of {total} rows)\n"]

    for row in rows[:20]:  # Cap display at 20 rows
        row_parts = []
        for key, value in row.items():
            val_str = str(value)
            if len(val_str) > 80:
                val_str = val_str[:80] + "..."
            row_parts.append(f"{key}={val_str}")
        lines.append("  " + ", ".join(row_parts))

    if len(rows) > 20:
        lines.append(f"  ... ({len(rows) - 20} more rows)")
    if has_more:
        lines.append(f"\n... more rows available (total: {total})")

    return "\n".join(lines)


def format_table_schema(table_name: str, columns: list[dict[str, Any]]) -> str:
    """Format table schema."""
    if not columns:
        return f"No schema found for table '{table_name}'."

    lines = [f"Schema for table '{table_name}':\n"]
    for col in columns:
        name = col.get("column_name", col.get("name", "unknown"))
        dtype = col.get("data_type", col.get("type", "unknown"))
        nullable = col.get("is_nullable", col.get("nullable", ""))
        default = col.get("column_default", col.get("default", ""))

        line = f"  {name}: {dtype}"
        if nullable == "NO" or nullable is False:
            line += " NOT NULL"
        if default:
            line += f" DEFAULT {default}"
        lines.append(line)

    return "\n".join(lines)


def format_monitoring_error(operation: str, error: Exception) -> str:
    """Format a monitoring operation error."""
    error_msg = str(error)

    if hasattr(error, "response"):
        try:
            detail = error.response.json().get("detail", error_msg)  # type: ignore[union-attr]
            error_msg = detail
        except Exception:
            error_msg = f"HTTP {error.response.status_code}: {error_msg}"  # type: ignore[union-attr]

    return f"Failed to {operation}:\n{error_msg}"


# =============================================================================
# Citation & Source Library Formatters
# =============================================================================


def format_sources(
    data: dict[str, Any],
    job_id: str | None = None,
    type_filter: str | None = None,
) -> str:
    """Format source list."""
    sources = data.get("sources", [])
    total = data.get("total", len(sources))

    if not sources:
        scope = f" for job {job_id}" if job_id else ""
        filter_msg = f" of type '{type_filter}'" if type_filter else ""
        return f"No sources found{scope}{filter_msg}."

    scope = f" for job {job_id}" if job_id else " (all jobs)"
    lines = [f"Sources{scope} ({len(sources)} of {total}):\n"]

    for s in sources:
        sid = s.get("id", "?")
        stype = s.get("type", "unknown")
        name = s.get("name", "unknown")
        identifier = s.get("identifier", "")
        preview = s.get("content_preview", "")
        if preview and len(preview) > 100:
            preview = preview[:100] + "..."

        lines.append(f"  [{sid}] {name} ({stype})")
        if identifier:
            lines.append(f"      Identifier: {identifier}")
        if preview:
            lines.append(f"      Preview: {preview}")
        if s.get("job_ids"):
            lines.append(f"      Jobs: {', '.join(s['job_ids'][:5])}")

    return "\n".join(lines)


def format_source_detail(data: dict[str, Any]) -> str:
    """Format single source detail."""
    sid = data.get("id", "?")
    lines = [f"Source #{sid}\n"]

    lines.append(f"Type: {data.get('type', 'unknown')}")
    lines.append(f"Name: {data.get('name', 'unknown')}")
    lines.append(f"Identifier: {data.get('identifier', 'N/A')}")

    if data.get("version"):
        lines.append(f"Version: {data['version']}")
    if data.get("content_hash"):
        lines.append(f"Content hash: {data['content_hash'][:16]}...")

    full_len = data.get("full_content_length", 0)
    lines.append(f"Content length: {full_len} chars")

    if data.get("content_truncated"):
        lines.append("(content truncated)")

    if data.get("job_ids"):
        lines.append(f"Linked jobs: {', '.join(data['job_ids'])}")

    if data.get("metadata"):
        meta = data["metadata"]
        if isinstance(meta, dict):
            meta_str = json.dumps(meta, indent=2, default=str)
            if len(meta_str) > 500:
                meta_str = meta_str[:500] + "\n..."
            lines.append(f"\nMetadata:\n{meta_str}")

    content = data.get("content", "")
    if content:
        lines.append(f"\n=== Content ===\n{content}")

    return "\n".join(lines)


def format_citations(
    job_id: str,
    data: dict[str, Any],
    status_filter: str | None = None,
) -> str:
    """Format citation list."""
    citations = data.get("citations", [])
    total = data.get("total", len(citations))

    if not citations:
        filter_msg = f" with status '{status_filter}'" if status_filter else ""
        return f"No citations found for job {job_id}{filter_msg}."

    lines = [f"Citations for job {job_id} ({len(citations)} of {total}):\n"]

    for c in citations:
        cid = c.get("id", "?")
        claim = c.get("claim", "")
        status = c.get("verification_status", "unknown")
        confidence = c.get("confidence", "")
        source_name = c.get("source_name", "unknown")
        source_id = c.get("source_id", "?")
        method = c.get("extraction_method", "")
        score = c.get("similarity_score")

        status_icon = {
            "verified": "+",
            "pending": "?",
            "failed": "x",
            "unverified": "~",
        }.get(status, " ")

        lines.append(f"  [{status_icon}] #{cid}: {claim}")
        lines.append(f"      Source: [{source_id}] {source_name}")
        parts = [f"Status: {status}", f"Confidence: {confidence}"]
        if method:
            parts.append(f"Method: {method}")
        if score is not None:
            parts.append(f"Score: {score:.3f}")
        lines.append(f"      {', '.join(parts)}")

    return "\n".join(lines)


def format_citation_detail(data: dict[str, Any]) -> str:
    """Format full citation detail."""
    cid = data.get("id", "?")
    lines = [f"Citation #{cid}\n"]

    lines.append(f"Job: {data.get('job_id', 'N/A')}")
    lines.append(f"Status: {data.get('verification_status', 'unknown')}")
    lines.append(f"Confidence: {data.get('confidence', 'N/A')}")
    lines.append(f"Extraction method: {data.get('extraction_method', 'N/A')}")

    if data.get("similarity_score") is not None:
        lines.append(f"Similarity score: {data['similarity_score']:.3f}")

    # Source info
    lines.append(
        f"\nSource: [{data.get('source_id', '?')}] {data.get('source_name', 'unknown')}"
    )
    lines.append(f"Source type: {data.get('source_type', 'N/A')}")
    if data.get("source_identifier"):
        lines.append(f"Source identifier: {data['source_identifier']}")

    # Claim and quote
    lines.append(f"\n=== Claim ===\n{data.get('claim', 'N/A')}")

    if data.get("verbatim_quote"):
        lines.append(f"\n=== Verbatim Quote ===\n{data['verbatim_quote']}")

    if data.get("quote_context"):
        ctx = data["quote_context"]
        if len(ctx) > 500:
            ctx = ctx[:500] + "..."
        lines.append(f"\n=== Quote Context ===\n{ctx}")

    if data.get("quote_language"):
        lines.append(f"Language: {data['quote_language']}")

    # Locator
    locator = data.get("locator")
    if locator and isinstance(locator, dict):
        loc_parts = []
        for key in ("page", "section", "paragraph", "line", "marginal_number"):
            if locator.get(key) is not None:
                loc_parts.append(f"{key}: {locator[key]}")
        if loc_parts:
            lines.append(f"\nLocator: {', '.join(loc_parts)}")

    # Verification details
    if data.get("verification_notes"):
        lines.append(f"\n=== Verification Notes ===\n{data['verification_notes']}")

    if data.get("matched_location") and isinstance(data["matched_location"], dict):
        lines.append(
            f"Matched location: {json.dumps(data['matched_location'], default=str)}"
        )

    if data.get("relevance_reasoning"):
        reasoning = data["relevance_reasoning"]
        if len(reasoning) > 300:
            reasoning = reasoning[:300] + "..."
        lines.append(f"\nRelevance reasoning: {reasoning}")

    if data.get("created_by"):
        lines.append(f"\nCreated by: {data['created_by']}")

    return "\n".join(lines)


def format_source_search(data: dict[str, Any]) -> str:
    """Format source search results."""
    results = data.get("results", [])
    query = data.get("query", "")
    mode = data.get("mode", "keyword")
    total = data.get("total", len(results))

    if not results:
        return f"No sources matching '{query}' found."

    lines = [f"Search results for '{query}' (mode: {mode}, {total} match(es)):\n"]

    for r in results:
        sid = r.get("source_id", "?")
        name = r.get("source_name", "unknown")
        stype = r.get("source_type", "")
        evidence = r.get("evidence_label", "")
        rank = r.get("rank", 0)
        snippet = r.get("snippet", "")

        lines.append(f"  [{evidence}] [{sid}] {name} ({stype})")
        lines.append(f"      Rank: {rank:.4f}")
        if snippet:
            # Clean up HTML tags from ts_headline
            clean = snippet.replace("<b>", "**").replace("</b>", "**")
            lines.append(f"      Snippet: {clean}")

    return "\n".join(lines)


def format_annotations(
    job_id: str,
    source_id: int,
    annotations: list[dict[str, Any]],
    type_filter: str | None = None,
) -> str:
    """Format source annotations."""
    if not annotations:
        filter_msg = f" of type '{type_filter}'" if type_filter else ""
        return (
            f"No annotations found for source {source_id} in job {job_id}{filter_msg}."
        )

    lines = [
        f"Annotations for source {source_id} in job {job_id} ({len(annotations)}):\n"
    ]

    for a in annotations:
        atype = a.get("annotation_type", "note")
        content = a.get("content", "")
        page_ref = a.get("page_reference", "")

        icon = {
            "note": "N",
            "highlight": "H",
            "summary": "S",
            "question": "Q",
            "critique": "C",
        }.get(atype, "?")

        header = f"  [{icon}] {atype}"
        if page_ref:
            header += f" (page: {page_ref})"
        lines.append(header)

        if len(content) > 200:
            content = content[:200] + "..."
        lines.append(f"      {content}")

    return "\n".join(lines)


def format_source_tags(job_id: str, source_id: int, tags: list[str]) -> str:
    """Format source tags."""
    if not tags:
        return f"No tags found for source {source_id} in job {job_id}."

    return f"Tags for source {source_id} in job {job_id}: {', '.join(tags)}"


def format_citation_stats(job_id: str, data: dict[str, Any]) -> str:
    """Format citation statistics."""
    lines = [f"Citation Statistics for job {job_id}\n"]

    # Sources
    total_sources = data.get("total_sources", 0)
    lines.append(f"Sources: {total_sources} total")
    by_type = data.get("sources_by_type", {})
    for stype, count in sorted(by_type.items()):
        lines.append(f"  {stype}: {count}")

    # Citations
    total_citations = data.get("total_citations", 0)
    lines.append(f"\nCitations: {total_citations} total")

    by_status = data.get("citations_by_verification_status", {})
    if by_status:
        lines.append("  By verification status:")
        for status in ("verified", "pending", "failed", "unverified"):
            count = by_status.get(status, 0)
            if count:
                icon = {
                    "verified": "+",
                    "pending": "?",
                    "failed": "x",
                    "unverified": "~",
                }.get(status, " ")
                lines.append(f"    [{icon}] {status}: {count}")

    by_confidence = data.get("citations_by_confidence", {})
    if by_confidence:
        lines.append("  By confidence:")
        for level in ("high", "medium", "low"):
            count = by_confidence.get(level, 0)
            if count:
                lines.append(f"    {level}: {count}")

    by_method = data.get("citations_by_extraction_method", {})
    if by_method:
        lines.append("  By extraction method:")
        for method, count in sorted(by_method.items()):
            lines.append(f"    {method}: {count}")

    return "\n".join(lines)


def format_memory_stats(job_id: str, data: dict[str, Any]) -> str:
    """Format memory statistics for display."""
    total = data.get("total", 0)
    total_tokens = data.get("total_tokens", 0)
    total_accesses = data.get("total_accesses", 0)
    avg_importance = data.get("avg_importance")

    lines = [f"Memory Statistics for job {job_id}\n"]
    lines.append(
        f"Memories: {total} total ({total_tokens:,} tokens, {total_accesses:,} accesses)"
    )
    if avg_importance is not None:
        lines.append(f"Average importance: {avg_importance:.2f}")

    # By type
    type_keys = [
        ("factual", "factual"),
        ("procedural", "procedural"),
        ("error_solution", "error_solution"),
        ("vocabulary", "vocabulary"),
        ("relational", "relational"),
    ]
    type_counts = {label: data.get(key, 0) for label, key in type_keys}
    if any(type_counts.values()):
        lines.append("\nBy type:")
        for label, count in type_counts.items():
            if count:
                lines.append(f"  {label}: {count}")

    # By source
    source_keys = [
        ("observer", "from_observer"),
        ("todo", "from_todo"),
        ("compaction", "from_compaction"),
        ("phase_archive", "from_phase_archive"),
        ("tool_error", "from_tool_error"),
    ]
    source_counts = {label: data.get(key, 0) for label, key in source_keys}
    if any(source_counts.values()):
        lines.append("\nBy source:")
        for label, count in source_counts.items():
            if count:
                lines.append(f"  {label}: {count}")

    return "\n".join(lines)


def format_citation_error(
    operation: str, error: Exception, job_id: str | None = None
) -> str:
    """Format a citation operation error."""
    error_msg = str(error)

    if hasattr(error, "response"):
        try:
            detail = error.response.json().get("detail", error_msg)  # type: ignore[union-attr]
            error_msg = detail
        except Exception:
            error_msg = f"HTTP {error.response.status_code}: {error_msg}"  # type: ignore[union-attr]

    scope = f" for job {job_id}" if job_id else ""
    return f"Failed to {operation}{scope}:\n{error_msg}"


# =============================================================================
# System Info Formatter
# =============================================================================


def format_system_info(agent_id: str, data: dict[str, Any]) -> str:
    """Format agent system info for display."""
    lines = [f"System Info for agent {agent_id}\n"]

    # Agent state
    agent = data.get("agent", {})
    lines.append(f"Agent ID: {agent.get('agent_id', 'N/A')}")
    lines.append(f"Current job: {agent.get('current_job') or 'idle'}")

    # CPU
    cpu = data.get("cpu", {})
    lines.append(f"\nCPU: {cpu.get('percent', 0)}% ({cpu.get('cores', '?')} cores)")

    # Memory
    mem = data.get("memory", {})
    lines.append(
        f"Memory: {mem.get('used_mb', 0)} / {mem.get('total_mb', 0)} MB ({mem.get('percent', 0)}%)"
    )

    # Disk
    disk = data.get("disk", {})
    lines.append(
        f"Disk: {disk.get('used_gb', 0)} / {disk.get('total_gb', 0)} GB ({disk.get('percent', 0)}%)"
    )

    # Listening ports
    ports = data.get("listening_ports", [])
    if ports:
        lines.append(f"\nListening ports ({len(ports)}):")
        for p in ports:
            lines.append(
                f"  :{p.get('port', '?')} ({p.get('address', '*')}) pid={p.get('pid', '?')}"
            )
    else:
        lines.append("\nNo listening ports detected.")

    # Top processes
    procs = data.get("processes", [])
    if procs:
        lines.append(f"\nTop processes by memory ({len(procs)}):")
        for proc in procs[:10]:
            cmd = proc.get("cmd", "") or proc.get("name", "")
            if len(cmd) > 60:
                cmd = cmd[:57] + "..."
            lines.append(
                f"  PID {proc.get('pid', '?'):>6}  "
                f"{proc.get('memory_mb', 0):>7.1f} MB  "
                f"{proc.get('cpu_percent', 0):>5.1f}% CPU  "
                f"{cmd}"
            )

    # Network connections
    conns = data.get("network_connections", [])
    if conns:
        lines.append(f"\nEstablished connections ({len(conns)}):")
        for c in conns[:10]:
            lines.append(
                f"  {c.get('local', '?')} -> {c.get('remote', '?')} pid={c.get('pid', '?')}"
            )
        if len(conns) > 10:
            lines.append(f"  ... and {len(conns) - 10} more")

    return "\n".join(lines)


# =============================================================================
# Todo Archives & Current Todos
# =============================================================================


def format_current_todos(data: dict[str, Any]) -> str:
    """Format current active todos (lightweight, no archives)."""
    todos = data.get("todos", [])
    if not todos:
        return "No active todos."

    lines = ["Current active todos:\n"]
    for todo in todos:
        status_icon = {
            "pending": "○",
            "in_progress": "◐",
            "completed": "●",
            "skipped": "⊘",
        }.get(todo.get("status", ""), "?")
        lines.append(f"  {status_icon} {_todo_label(todo)}")
        if todo.get("description"):
            desc = todo["description"][:100]
            if len(todo["description"]) > 100:
                desc += "..."
            lines.append(f"      {desc}")

    # Summary counts
    total = len(todos)
    done = sum(1 for t in todos if t.get("status") in ("completed", "skipped"))
    lines.append(f"\nProgress: {done}/{total} complete")
    return "\n".join(lines)


def format_todo_archives(job_id: str, archives: list[dict[str, Any]]) -> str:
    """Format list of archived todo files."""
    if not archives:
        return f"No archived todos for job {job_id}."

    lines = [f"Archived todo phases for job {job_id} ({len(archives)} archives):\n"]
    for archive in archives:
        filename = archive.get("filename", "unknown")
        phase_name = archive.get("phase_name", "")
        timestamp = archive.get("timestamp", "")
        line = f"  - {filename}"
        if phase_name:
            line += f"  ({phase_name})"
        if timestamp:
            line += f"  [{timestamp}]"
        lines.append(line)

    lines.append("\nUse get_todo_archive with a filename to read the full content.")
    return "\n".join(lines)


def format_todo_archive_detail(
    job_id: str,
    filename: str,
    data: dict[str, Any],
) -> str:
    """Format the content of a single archived todo file."""
    lines = [f"Archived todos: {filename} (job {job_id})\n"]

    if data.get("phase_name"):
        lines.append(f"Phase: {data['phase_name']}")
    if data.get("summary"):
        lines.append(f"Summary: {data['summary']}")

    lines.append("")

    todos = data.get("todos", [])
    if todos:
        for todo in todos:
            status_icon = {
                "pending": "○",
                "in_progress": "◐",
                "completed": "●",
                "skipped": "⊘",
            }.get(todo.get("status", ""), "?")
            lines.append(f"  {status_icon} {_todo_label(todo)}")
            if todo.get("notes"):
                lines.append(f"      Notes: {todo['notes'][:150]}")
    else:
        # Fallback: show raw content if structured todos aren't available
        content = data.get("content", "")
        if content:
            lines.append(content[:3000])
        else:
            lines.append("(empty archive)")

    return "\n".join(lines)


# =============================================================================
# Knowledge Base
# =============================================================================


def format_knowledge_summary(project_id: str, data: dict[str, Any]) -> str:
    """Format knowledge base summary statistics."""
    total = data.get("total", 0)
    lines = [f"Knowledge base for project {project_id}: {total} notes\n"]

    by_type = data.get("by_type", {})
    if by_type:
        lines.append("By type:")
        for t, count in sorted(by_type.items(), key=lambda x: -x[1]):
            lines.append(f"  {t}: {count}")
        lines.append("")

    by_status = data.get("by_status", {})
    if by_status:
        lines.append("By status:")
        for s, count in sorted(by_status.items(), key=lambda x: -x[1]):
            lines.append(f"  {s}: {count}")
        lines.append("")

    recent = data.get("recent", [])
    if recent:
        lines.append("Recent notes:")
        for note in recent:
            title = note.get("title", "Untitled")
            ntype = note.get("note_type", "")
            status = note.get("status", "")
            modified = str(note.get("modified_at", ""))[:19]
            lines.append(f"  [{ntype}] {title} ({status}) — {modified}")

    if total == 0:
        lines.append("No knowledge notes found.")

    return "\n".join(lines)


def format_knowledge_notes(data: dict[str, Any]) -> str:
    """Format paginated list of knowledge notes."""
    notes = data.get("notes", [])
    total = data.get("total", 0)
    offset = data.get("offset", 0)
    limit = data.get("limit", 50)

    if not notes:
        return "No knowledge notes found matching filters."

    lines = [f"Knowledge notes ({offset + 1}-{offset + len(notes)} of {total}):\n"]
    for note in notes:
        note_id = note.get("note_id", "?")
        title = note.get("title", "Untitled")
        ntype = note.get("note_type", "")
        status = note.get("status", "")
        confidence = note.get("confidence")
        preview = note.get("content_preview", "")

        header = f"  [{ntype}] {title}"
        if confidence is not None:
            header += f" (confidence: {_fmt_confidence(confidence)})"
        header += f" — {status}"
        lines.append(header)
        lines.append(f"    ID: {note_id}")

        tags = note.get("tags") or []
        if tags:
            lines.append(f"    Tags: {', '.join(tags)}")

        if preview:
            preview_text = preview.replace("\n", " ")[:120]
            if len(preview) > 120:
                preview_text += "..."
            lines.append(f"    {preview_text}")
        lines.append("")

    if total > offset + len(notes):
        lines.append(f"Use offset={offset + limit} to see more.")

    return "\n".join(lines)


def format_knowledge_note_detail(data: dict[str, Any]) -> str:
    """Format a single knowledge note with full content."""
    note_id = data.get("note_id", "?")
    title = data.get("title", "Untitled")
    ntype = data.get("note_type", "")
    status = data.get("status", "")

    lines = [f"Knowledge Note: {title}\n"]
    lines.append(f"ID: {note_id}")
    lines.append(f"Type: {ntype}")
    lines.append(f"Status: {status}")

    confidence = data.get("confidence")
    if confidence is not None:
        lines.append(f"Confidence: {_fmt_confidence(confidence)}")

    phase = data.get("phase")
    if phase:
        lines.append(f"Phase: {phase}")

    job_id = data.get("job_id")
    if job_id:
        lines.append(f"Source job: {job_id}")

    tags = data.get("tags") or []
    if tags:
        lines.append(f"Tags: {', '.join(tags)}")

    keywords = data.get("keywords") or []
    if keywords:
        lines.append(f"Keywords: {', '.join(keywords)}")

    created = str(data.get("created_at", ""))[:19]
    modified = str(data.get("modified_at", ""))[:19]
    if created:
        lines.append(f"Created: {created}")
    if modified and modified != created:
        lines.append(f"Modified: {modified}")

    content = data.get("content", "")
    if content:
        lines.append(f"\n--- Content ---\n{content}")

    relationships = data.get("relationships", [])
    if relationships:
        lines.append(f"\n--- Relationships ({len(relationships)}) ---")
        for rel in relationships[:20]:
            rel_type = rel.get("type", "RELATED_TO")
            target = rel.get("target", {})
            target_title = target.get("title", target.get("id", "?"))
            lines.append(f"  —[{rel_type}]→ {target_title}")
        if len(relationships) > 20:
            lines.append(f"  ... and {len(relationships) - 20} more")

    return "\n".join(lines)


def format_knowledge_search(data: dict[str, Any]) -> str:
    """Format knowledge base search results."""
    notes = data.get("notes", [])
    query = data.get("query", "")
    total = data.get("total", len(notes))

    if not notes:
        return f"No knowledge notes found matching '{query}'."

    lines = [f"Knowledge search results for '{query}' ({total} matches):\n"]
    for i, note in enumerate(notes, 1):
        note_id = note.get("note_id", "?")
        title = note.get("title", "Untitled")
        ntype = note.get("note_type", "")
        status = note.get("status", "")
        score = note.get("score") or note.get("rank")

        header = f"  {i}. [{ntype}] {title} ({status})"
        if score is not None:
            header += f" — score: {score:.3f}"
        lines.append(header)
        lines.append(f"     ID: {note_id}")

        content = note.get("content", "")
        if content:
            preview = content.replace("\n", " ")[:150]
            if len(content) > 150:
                preview += "..."
            lines.append(f"     {preview}")
        lines.append("")

    return "\n".join(lines)


# =============================================================================
# Projects
# =============================================================================


def format_projects(
    projects: list[dict[str, Any]], *, include_archived: bool = False
) -> str:
    """Format project list for display.

    Archived projects are hidden unless ``include_archived``. They are a
    read-only historical record: dispatching work, commissioning an officer
    or filing tickets against one is almost always a mistake, and the old
    rendering — one parenthesised ``(archived)`` among many rows — was easy
    to read straight past. A project that is split leaves its archive behind
    under a near-identical name, which is exactly when the confusion bites.

    Hidden is not silent. The footer names how many were withheld and how to
    see them, so a caller hunting a project they know exists is told where it
    went instead of concluding it is gone.
    """
    if not projects:
        return "No projects found."

    def _is_archived(project: dict[str, Any]) -> bool:
        return str(project.get("status") or "").lower() == "archived"

    hidden = 0
    if include_archived:
        shown = list(projects)
    else:
        shown = [p for p in projects if not _is_archived(p)]
        hidden = len(projects) - len(shown)

    if not shown:
        return (
            "No active projects found "
            f"({hidden} archived project(s) hidden — pass "
            "include_archived=true to list them)."
        )

    lines = [f"Found {len(shown)} project(s):\n"]
    for project in shown:
        pid = project.get("id", "?")
        name = project.get("name", "Untitled")
        status = project.get("status", "active")
        goal = project.get("goal", "")
        updated = str(project.get("updated_at", ""))[:19]

        # Loud on the row itself: a caller who explicitly asked for archived
        # projects still needs to see WHICH of these are the dead ones.
        marker = "  [ARCHIVED] " if _is_archived(project) else "  "
        lines.append(f"{marker}{name} ({status})")
        lines.append(f"    ID: {pid}")
        if goal:
            goal_preview = goal[:120]
            if len(goal) > 120:
                goal_preview += "..."
            lines.append(f"    Goal: {goal_preview}")
        if updated:
            lines.append(f"    Updated: {updated}")
        lines.append("")

    if hidden:
        lines.append(
            f"({hidden} archived project(s) hidden — pass include_archived=true "
            "to list them.)"
        )

    return "\n".join(lines)


def format_project_detail(project: dict[str, Any]) -> str:
    """Format single project details.

    An archived project leads with a warning line rather than burying the
    state in a ``Status:`` field, because the reader of this output is
    usually deciding whether to act on the project.
    """
    lines: list[str] = []
    if str(project.get("status") or "").lower() == "archived":
        lines.append(
            "[ARCHIVED] This project is a historical record. Do not dispatch "
            "work, commission an officer, or file tickets against it — find "
            "the active project that succeeded it."
        )
        lines.append("")
    lines += [
        f"Project: {project.get('name', 'Untitled')}",
        f"ID: {project.get('id', '?')}",
        f"Status: {project.get('status', 'active')}",
        f"Created: {str(project.get('created_at', 'N/A'))[:19]}",
        f"Updated: {str(project.get('updated_at', 'N/A'))[:19]}",
    ]

    if project.get("description"):
        desc = project["description"]
        if len(desc) > 500:
            desc = desc[:500] + "..."
        lines.append(f"\nDescription:\n{desc}")

    if project.get("goal"):
        goal = project["goal"]
        if len(goal) > 500:
            goal = goal[:500] + "..."
        lines.append(f"\nGoal:\n{goal}")

    if project.get("default_config_name"):
        lines.append(f"\nDefault config: {project['default_config_name']}")

    return "\n".join(lines)


def format_created_project(result: dict[str, Any]) -> str:
    """Format the result of creating a new project."""
    pid = result.get("id", "unknown")
    lines = [
        "Project created successfully.",
        f"Project ID: {pid}",
        f"Name: {result.get('name', 'Untitled')}",
        f"Status: {result.get('status', 'active')}",
    ]

    if result.get("description"):
        desc = result["description"]
        if len(desc) > 200:
            desc = desc[:200] + "..."
        lines.append(f"Description: {desc}")

    lines.append(
        "\nNext step: Use create_project_job(project_id, ...) to create jobs within this project."
    )
    return "\n".join(lines)


# =============================================================================
# Project Members & Experts
# =============================================================================


def format_project_members(project_id: str, members: list[dict[str, Any]]) -> str:
    """Format project member list."""
    if not members:
        return f"No members found for project {project_id}."

    lines = [f"Project {project_id} — {len(members)} member(s):\n"]
    for m in members:
        role = m.get("role", "unknown")
        role_icon = {"owner": "👑", "editor": "✏️", "viewer": "👁"}.get(role, "❓")
        name = m.get("display_name") or m.get("user_id", "?")
        email = m.get("email", "")
        email_part = f" ({email})" if email else ""
        lines.append(f"  {role_icon} {name}{email_part} — {role}")
        if m.get("added_at"):
            lines.append(f"     Added: {str(m['added_at'])[:19]}")
    return "\n".join(lines)


def format_project_experts(project_id: str, experts: list[dict[str, Any]]) -> str:
    """Format project-specific expert list."""
    if not experts:
        return f"No project-specific experts found for project {project_id}."

    lines = [f"Project {project_id} — {len(experts)} expert(s):\n"]
    for e in experts:
        icon = e.get("icon", "🧠")
        name = e.get("display_name", e.get("id", "?"))
        eid = e.get("id", "?")
        desc = e.get("description", "")
        if len(desc) > 120:
            desc = desc[:120] + "..."
        lines.append(f"  {icon} {name} ({eid})")
        if desc:
            lines.append(f"     {desc}")
        tags = e.get("tags", [])
        if tags:
            lines.append(f"     Tags: {', '.join(tags)}")
    return "\n".join(lines)


def format_project_expert_detail(project_id: str, data: dict[str, Any]) -> str:
    """Format detailed project expert configuration."""
    eid = data.get("id", "?")
    lines = [
        f"Expert: {data.get('display_name', eid)}",
        f"ID: {eid}",
        f"Project: {project_id}",
        f"Icon: {data.get('icon', '🧠')}",
        f"Color: {data.get('color', '#cba6f7')}",
    ]

    tags = data.get("tags", [])
    if tags:
        lines.append(f"Tags: {', '.join(tags)}")

    desc = data.get("description", "")
    if desc:
        if len(desc) > 500:
            desc = desc[:500] + "..."
        lines.append(f"\nDescription:\n{desc}")

    config = data.get("config")
    if config:
        lines.append(f"\nConfig:\n```json\n{json.dumps(config, indent=2)}\n```")

    instructions = data.get("instructions")
    if instructions:
        if len(instructions) > 1000:
            instructions = instructions[:1000] + "\n... (truncated)"
        lines.append(f"\nInstructions:\n{instructions}")

    return "\n".join(lines)


# =============================================================================
# Datasource CRUD
# =============================================================================


def format_created_datasource(result: dict[str, Any]) -> str:
    """Format a created connector while masking its connection URL."""
    ds_id = str(result.get("id", "unknown"))
    name = result.get("name", "unknown")
    ds_type = result.get("type", "unknown")
    read_only = result.get("read_only", True)
    url = result.get("connection_url", "")
    job_id = result.get("job_id")
    scope_mode = result.get("scope_mode", "all")
    project_ids = [str(value) for value in (result.get("project_ids") or [])]
    if job_id:
        scope = f"legacy job-scoped ({job_id})"
    elif scope_mode == "projects":
        scope = f"projects ({', '.join(project_ids) or 'none'})"
    else:
        scope = "all work contexts"

    lines = [
        "Connector created successfully.",
        f"ID: {ds_id}",
        f"Name: {name}",
        f"Type: {ds_type}",
        f"Scope: {scope}",
        f"Published: {bool(result.get('is_global'))}",
        f"Auto-attach default: {bool(result.get('auto_attach'))}",
        f"Policy revision: {result.get('policy_revision', 'unknown')}",
        f"Read-only: {read_only}",
        f"URL: {_mask_url(str(url or ''))}",
    ]

    lines.append(
        "\nUse test_datasource(datasource_id) to verify the connector's connectivity."
    )
    return "\n".join(lines)


# =============================================================================
# Message Thread Formatters
# =============================================================================


def format_message_threads(threads: list[dict[str, Any]]) -> str:
    """Format message thread list for display."""
    if not threads:
        return "No message threads found for this job."

    lines = [f"Found {len(threads)} thread(s):\n"]
    for t in threads:
        thread_id = t.get("thread_id", "?")
        subject = t.get("subject", "(no subject)")
        count = t.get("message_count", 0)
        last_at = t.get("last_message_at", "")
        mode = t.get("mode", "")
        status = t.get("status", "")

        status_str = ""
        if status == "waiting_for_reply":
            status_str = " [WAITING FOR REPLY]"
        elif mode == "blocking":
            status_str = " [blocking]"

        lines.append(f"  Thread {thread_id}: {subject}{status_str}")
        lines.append(f"    Messages: {count} | Last: {last_at}")

    return "\n".join(lines)


def format_thread_messages(messages: list[dict[str, Any]], thread_id: str) -> str:
    """Format individual messages within a thread."""
    if not messages:
        return f"No messages found in thread {thread_id}."

    lines = [f"Thread {thread_id} — {len(messages)} message(s):\n"]
    for m in messages:
        direction = m.get("direction", "?")
        icon = "→" if direction == "outbound" else "←"
        created = m.get("created_at", "")
        subject = m.get("subject", "")
        body = m.get("message", "")

        lines.append(f"  {icon} [{created}] {subject}")
        if body:
            # Truncate long messages
            preview = body[:500] + "..." if len(body) > 500 else body
            for line in preview.split("\n"):
                lines.append(f"    {line}")
        lines.append("")

    return "\n".join(lines)


# =============================================================================
# Persistent Thread Formatters
# =============================================================================


def format_persistent_threads(threads: list[dict[str, Any]]) -> str:
    """Format persistent thread list for display."""
    if not threads:
        return "No persistent threads found."

    lines = [f"Found {len(threads)} persistent thread(s):\n"]
    for t in threads:
        tid = t.get("id", "?")
        title = t.get("title", "Untitled")
        status = t.get("status", "unknown")
        config = t.get("config_name", "N/A")
        mode = t.get("permission_mode", "N/A")
        created = t.get("created_at", "")
        last_activity = t.get("last_activity", "")
        turns = t.get("total_turns", 0)
        tokens = t.get("total_tokens", 0)

        lines.append(f"  [{status}] {tid}: {title}")
        lines.append(f"    Config: {config} | Mode: {mode}")
        lines.append(f"    Created: {created} | Last activity: {last_activity}")
        lines.append(f"    Turns: {turns} | Tokens: {tokens}")

    return "\n".join(lines)


def format_persistent_thread_detail(thread: dict[str, Any]) -> str:
    """Format single persistent thread details."""
    import json as _json

    lines = [
        f"Thread: {thread.get('id', 'N/A')}",
        f"Title: {thread.get('title', 'N/A')}",
        f"Status: {thread.get('status', 'N/A')}",
        f"Config: {thread.get('config_name', 'N/A')}",
        f"Permission mode: {thread.get('permission_mode', 'N/A')}",
        f"Created: {thread.get('created_at', 'N/A')}",
        f"Last activity: {thread.get('last_activity', 'N/A')}",
        f"Turns: {thread.get('total_turns', 0)}",
        f"Tokens: {thread.get('total_tokens', 0)}",
    ]

    if thread.get("project_id"):
        lines.append(f"Project: {thread['project_id']}")
    if thread.get("agent_id"):
        lines.append(f"Agent: {thread['agent_id']}")
    if thread.get("ended_at"):
        lines.append(f"Ended: {thread['ended_at']}")

    metadata = thread.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = _json.loads(metadata)
        except (ValueError, TypeError):
            metadata = {}

    # Phase 1 of cloud_collaboration_model.md §9: project attachment is
    # exposed on the thread response as a top-level ``project_ids`` field
    # (derived from ``thread_mounts``). The legacy ``metadata.project_ids``
    # JSONB key is no longer written.
    project_ids = thread.get("project_ids")
    if project_ids:
        lines.append(f"Project IDs: {', '.join(str(p) for p in project_ids)}")

    config_override = metadata.get("config_override")
    if config_override:
        snippet = _json.dumps(config_override)
        if len(snippet) > 300:
            snippet = snippet[:300] + "..."
        lines.append(f"\nConfig override:\n{snippet}")

    ws_ctx = metadata.get("workspace_container") or {}
    if ws_ctx.get("status"):
        lines.append(f"\nWorkspace: {ws_ctx.get('status', 'N/A')}")
        if ws_ctx.get("pod_ip"):
            lines.append(f"  Pod IP: {ws_ctx['pod_ip']}")

    vm_ctx = metadata.get("vm") or {}
    if vm_ctx.get("status"):
        lines.append(f"VM: {vm_ctx.get('status', 'N/A')}")

    return "\n".join(lines)


def format_created_thread(result: dict[str, Any], config_name: str, title: str) -> str:
    """Format a newly created persistent thread."""
    thread_id = result.get("thread_id", "N/A")
    return (
        f"Thread created successfully.\n"
        f"Thread ID: {thread_id}\n"
        f"Title: {title}\n"
        f"Config: {config_name}\n"
        f"Status: {result.get('status', 'created')}\n\n"
        f"The session is provisioning. "
        f"Use get_persistent_thread({thread_id}) to check status."
    )


def format_persistent_thread_messages(
    data: dict[str, Any], full_content: bool = False, tail: bool = False
) -> str:
    """Format persistent thread message history.

    Content is truncated to a 500-char preview per message by default; pass
    ``full_content=True`` to emit the complete body of each message.

    ``tail=True`` labels the window as the END of the log and drops the
    offset-paging footer: the reader already holds the newest messages, and
    "use offset=N" would send them walking forward from the beginning.
    """
    thread_id = data.get("thread_id", "?")
    messages = data.get("messages", [])
    total = data.get("total", len(messages))

    if not messages:
        return f"No messages found for thread {thread_id}."

    if tail:
        header = (
            f"Thread {thread_id} — newest {len(messages)} of {total} message(s), "
            "oldest first within this window:\n"
        )
    else:
        header = (
            f"Thread {thread_id} — showing {len(messages)} of {total} message(s):\n"
        )
    lines = [header]
    for m in messages:
        turn = m.get("turn_number", "?")
        role = m.get("role", "?")
        created = m.get("created_at", "")
        content = m.get("content", "")

        lines.append(f"  [{turn}] {role} ({created}):")
        if content:
            if full_content:
                preview = content
            else:
                preview = content[:500] + "..." if len(content) > 500 else content
            for line in preview.split("\n"):
                lines.append(f"    {line}")

        tool_calls = m.get("tool_calls")
        if tool_calls and isinstance(tool_calls, list):
            names = [tc.get("name", "?") for tc in tool_calls if isinstance(tc, dict)]
            if names:
                lines.append(f"    Tools: {', '.join(names)}")
        lines.append("")

    if len(messages) < total:
        if tail:
            lines.append(f"... {total - len(messages)} earlier message(s) not shown.")
        else:
            lines.append(f"... more messages available (use offset={len(messages)})")

    return "\n".join(lines)


def format_persistent_thread_ide(data: dict[str, Any]) -> str:
    """Format IDE session status for a persistent thread."""
    lines = [
        f"Status: {data.get('status', 'N/A')}",
    ]
    if data.get("source"):
        lines.append(f"Source: {data['source']}")
    lines.append(f"Code Server URL: {data.get('code_server_url') or 'N/A'}")
    if data.get("gitea_url"):
        lines.append(f"Gitea URL: {data['gitea_url']}")
    return "\n".join(lines)


def format_thread_action_result(
    action: str, thread_id: str, result: dict[str, Any]
) -> str:
    """Format a generic thread action result."""
    status = result.get("status", "unknown")
    lines = [f"Action: {action}", f"Thread: {thread_id}", f"Status: {status}"]

    for key, value in result.items():
        if key in ("status", "thread_id"):
            continue
        if isinstance(value, str) and len(value) > 300:
            value = value[:300] + "..."
        lines.append(f"{key}: {value}")

    return "\n".join(lines)


# =============================================================================
# Officer Formatters (the Legate's side — officer_legate_channel.md)
# =============================================================================


def _officer_held_label(held: Any) -> str | None:
    """Render a hold as HELD — <kind>, or None when he is free."""
    if not isinstance(held, dict) or not held:
        return None
    kind = held.get("kind") or ("conference" if held.get("thread_id") else "hold")
    return f"HELD — {kind}"


def format_officer_roster(data: dict[str, Any]) -> str:
    """Every post the caller can see, one block each."""
    officers = data.get("officers") or []
    if not officers:
        return (
            "No officer posts on any project you can see. Commission one from "
            "the project's Centurion tab in the cockpit."
        )

    lines = [f"Officers — {data.get('total', len(officers))} post(s):\n"]
    for row in officers:
        name = row.get("project_name") or "(unnamed project)"
        held = _officer_held_label(row.get("held"))
        if not row.get("commissioned"):
            lines.append(f"  {name} [vacant] — {row.get('project_id')}")
            lines.append("")
            continue
        state = held or "commissioned"
        wake = row.get("next_wake_at")
        headline = f"  {name} [{state}]"
        if wake and not held:
            headline += f" — next wake {wake}"
        lines.append(headline)
        lines.append(f"    Project {row.get('project_id')}")
        detail = [
            f"thread {_short_id(row.get('thread_id'))}",
            str(row.get("model") or "model ?"),
            (
                "auto-pull on"
                if row.get("auto_pull")
                and row.get("auto_pull_enable_available") is True
                else (
                    "auto-pull configured — deployment release fenced"
                    if row.get("auto_pull")
                    else "auto-pull off"
                )
            ),
        ]
        lines.append(f"    {' | '.join(detail)}")
        counts = [
            f"{int(row.get('in_flight_jobs') or 0)} job(s) in flight",
            f"{int(row.get('pending_events') or 0)} event(s) pending on him",
        ]
        lines.append(f"    {' | '.join(counts)}")
        if row.get("last_activity_at"):
            lines.append(f"    Last acted {row['last_activity_at']}")
        lines.append("")
    return "\n".join(lines).rstrip()


def format_officer_post(
    data: dict[str, Any],
    project_name: str | None = None,
    recent: dict[str, Any] | None = None,
) -> str:
    """One project's post in full, optionally with the tail of his log."""
    officer = data.get("officer") or {}
    label = project_name or data.get("project_id") or "project"
    if not data.get("commissioned"):
        lines = [f"Officer — {label}: the post is VACANT."]
        while_vacant = data.get("while_vacant") or {}
        if while_vacant.get("entries"):
            lines.append(
                f"  {len(while_vacant['entries'])} item(s) waiting in the "
                "while-vacant ledger."
            )
        lines.append("  Commission one from the project's Centurion tab.")
        return "\n".join(lines)

    held = _officer_held_label(data.get("held") or officer.get("hold"))
    lines = [
        f"Officer — {label}",
        f"Status: commissioned ({officer.get('status') or '?'})"
        + (f" | {held}" if held else ""),
        f"Thread: {officer.get('thread_id')}"
        + (f" — {officer['title']}" if officer.get("title") else ""),
    ]
    runtime_authorization = data.get("runtime_authorization") or {}
    if runtime_authorization.get("status") == "unavailable":
        lines.append(
            "Runtime authorization: UNAVAILABLE — autonomous planning is "
            "suppressed; server retry/incident handling is active."
        )
    runtime_lifecycle = data.get("runtime_lifecycle") or {}
    if runtime_lifecycle:
        lines.append(
            "Runtime lifecycle: "
            f"{runtime_lifecycle.get('drift_state') or 'unknown'} | "
            f"{runtime_lifecycle.get('recycle_phase') or 'idle'} | "
            f"observed {runtime_lifecycle.get('observed_build_sha') or 'missing'} | "
            f"expected {runtime_lifecycle.get('expected_build_sha') or 'unpinned'}"
            + (
                f" | failure {runtime_lifecycle['last_failure']}"
                if runtime_lifecycle.get("last_failure")
                else ""
            )
        )
    brain = officer.get("model") or "?"
    if officer.get("reasoning_level"):
        brain += f" (reasoning {officer['reasoning_level']})"
    lines.append(f"Brain: {brain}")
    lines.append(
        f"Next wake: {data.get('next_wake_at') or 'not scheduled'} | "
        f"Pending events: {int(data.get('pending_events') or 0)}"
    )
    backlog = data.get("backlog") or {}
    auto_pull_control = backlog.get("auto_pull_control") or {}
    if backlog.get("auto_pull") and auto_pull_control.get("enable_available") is False:
        auto_pull_label = "configured on — deployment release fenced"
    else:
        auto_pull_label = "on" if backlog.get("auto_pull") else "off"
    lines.append(f"Backlog auto-pull: {auto_pull_label}")
    if backlog.get("worker_spend_ceiling_daily") is not None:
        lines.append(
            "Worker spend ceiling: "
            f"${backlog['worker_spend_ceiling_daily']}/day across the century"
        )
    spend = data.get("spend_today") or {}
    if spend.get("tokens") is not None:
        ceiling = spend.get("ceiling")
        budget = f"{int(ceiling):,}" if ceiling else "no ceiling"
        lines.append(f"Spend today: {int(spend['tokens']):,} / {budget} tokens")
    if data.get("conference"):
        lines.append(f"Conference open: {data['conference'].get('thread_id')}")

    kit = data.get("kit") or {}
    if kit:
        lines.append("Kit:")
        for slot, spec in kit.items():
            entry = spec if isinstance(spec, dict) else {}
            line = (
                f"  - {slot}: {entry.get('in_flight', 0)}/{entry.get('count', '?')} "
                "in flight"
            )
            if "ready_depth" in entry:
                line += f" | ready {entry['ready_depth']}"
                if entry.get("below_floor"):
                    line += " — BELOW FLOOR"
            if entry.get("spend_ceiling_daily") is not None:
                line += f" | ${entry['spend_ceiling_daily']}/day ceiling"
            lines.append(line)

    if recent and recent.get("messages"):
        lines.append("")
        lines.append(_officer_recent_lines(recent))
    return "\n".join(lines)


def _officer_recent_lines(recent: dict[str, Any]) -> str:
    """His last few turns: what woke him, what he did, how long he slept."""
    lines = ["Recent log (oldest first):"]
    for message in recent.get("messages") or []:
        role = message.get("role")
        stamp = str(message.get("created_at") or "")[:16]
        content = (message.get("content") or "").strip()
        if role == "event":
            headline = content.split("\n", 1)[0]
            lines.append(f"  {stamp} [wake] {headline[:160]}")
        elif role == "ai":
            calls = message.get("tool_calls")
            names = (
                [tc.get("name", "?") for tc in calls if isinstance(tc, dict)]
                if isinstance(calls, list)
                else []
            )
            if names:
                lines.append(f"  {stamp} [acted] {', '.join(names)}")
            elif content:
                lines.append(f"  {stamp} [said] {content.splitlines()[0][:160]}")
    return "\n".join(lines)


def format_officer_note_result(data: dict[str, Any]) -> str:
    """State how a note landed. 'Sent' is not an outcome here."""
    delivered = data.get("delivered")
    thread_id = data.get("thread_id")
    if delivered == "live":
        return (
            f"Note delivered to his input queue (thread {_short_id(thread_id)}). "
            "He reads it at his next turn — anything on the input queue wakes "
            "him before his timer."
        )
    if delivered == "held":
        held = _officer_held_label(data.get("held")) or "HELD"
        blocker = held.replace("HELD — ", "")
        return (
            f"Note QUEUED behind a {blocker} hold (thread "
            f"{_short_id(thread_id)}). He reads it when the hold lifts; "
            "nothing else delivers it in the meantime."
        )
    wake = data.get("next_wake_at")
    when = f" at {wake}" if wake else " at his next wake"
    return (
        f"Note queued durably (thread {_short_id(thread_id)}) — he was not "
        f"live. He reads it{when}."
    )
