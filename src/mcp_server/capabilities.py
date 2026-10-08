"""Authoritative capability and risk contract for the SRW MCP server.

The function signatures in :mod:`orchestrator.mcp.server` remain the source of
the protocol input/output schemas. This manifest is the authority for tool
identity, REST mapping, authorization intent, side effects, retry semantics,
and MCP risk annotations. Server registration fails if a tool has no entry.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

WorkflowDisposition = Literal["required", "intentionally_excluded", "superseded"]

_RESOURCE_AUTH = (
    "approved user; orchestrator resource visibility and MCP project scope enforced"
)
_ADMIN_AUTH = "approved administrator; orchestrator role check enforced"
_OWNER_AUTH = "approved project owner or administrator; orchestrator check enforced"
_EDITOR_AUTH = (
    "approved project editor/owner or administrator; orchestrator check enforced"
)
_SCHEMA_SOURCE = "raw MCP tools/list inputSchema and outputSchema"
_CONTRACT_COVERAGE = "tests/test_mcp_capabilities.py (registration/schema contract)"


@dataclass(frozen=True, slots=True)
class ToolCapability:
    """Security and transport contract for one registered MCP tool."""

    name: str
    operation: str
    authorization: str
    side_effects: str
    read_only: bool
    destructive: bool
    idempotent: bool
    retry_policy: str
    open_world: bool = False
    schema_source: str = _SCHEMA_SOURCE
    coverage: str = _CONTRACT_COVERAGE

    @property
    def annotations(self) -> dict[str, bool]:
        return {
            "readOnlyHint": self.read_only,
            "destructiveHint": self.destructive,
            "idempotentHint": self.idempotent,
            "openWorldHint": self.open_world,
        }

    def metadata(self) -> dict[str, str | bool]:
        return {
            "restOperation": self.operation,
            "authorization": self.authorization,
            "sideEffects": self.side_effects,
            "retryPolicy": self.retry_policy,
            "schemaSource": self.schema_source,
            "coverage": self.coverage,
        }


@dataclass(frozen=True, slots=True)
class WorkflowDecision:
    """Decision for an orchestrator workflow not represented one-to-one."""

    workflow: str
    rest_operations: str
    disposition: WorkflowDisposition
    authorization: str
    reason: str


def _read(
    name: str,
    operation: str,
    *,
    authorization: str = _RESOURCE_AUTH,
    open_world: bool = False,
    side_effects: str = "none",
) -> ToolCapability:
    retry_policy = (
        "safe GET: up to 3 attempts on connect/timeout transport failures"
        if operation.startswith("GET ")
        else "single attempt: read is transported with a non-GET method"
    )
    return ToolCapability(
        name=name,
        operation=operation,
        authorization=authorization,
        side_effects=side_effects,
        read_only=True,
        destructive=False,
        idempotent=True,
        retry_policy=retry_policy,
        open_world=open_world,
    )


def _mutation(
    name: str,
    operation: str,
    side_effects: str,
    *,
    authorization: str = _RESOURCE_AUTH,
    destructive: bool = False,
    idempotent: bool = False,
    open_world: bool = False,
) -> ToolCapability:
    return ToolCapability(
        name=name,
        operation=operation,
        authorization=authorization,
        side_effects=side_effects,
        read_only=False,
        destructive=destructive,
        idempotent=idempotent,
        retry_policy=(
            "single attempt; ambiguous post-send timeout is reported as outcome unknown"
        ),
        open_world=open_world,
    )


_READ_CAPABILITIES = (
    _read("manifest_validate", "POST /api/manifests/validate"),
    _read("manifest_preview", "POST /api/manifests/preview"),
    _read("manifest_list", "GET /api/resources"),
    _read("manifest_get", "GET /api/resources/{resource_id}"),
    _read(
        "manifest_export",
        "GET /api/resources/{resource_id} + POST /api/manifests/export",
    ),
    _read("list_jobs", "GET /api/jobs"),
    _read("get_job", "GET /api/jobs/{job_id}"),
    _read("get_audit_trail", "GET /api/jobs/{job_id}/audit"),
    _read("get_audit_bulk", "GET /api/jobs/{job_id}/audit?lean=true"),
    _read("get_chat_history", "GET /api/jobs/{job_id}/chat"),
    _read("get_chat_bulk", "GET /api/jobs/{job_id}/chat?offset&limit"),
    _read("get_todos", "GET /api/jobs/{job_id}/todos"),
    _read("get_graph_changes", "GET /api/graph/changes/{job_id}"),
    _read("get_llm_request", "GET /api/requests/{doc_id}"),
    _read(
        "get_job_summary",
        "GET /api/jobs/{job_id} + /progress + /todos + /repo/contents + /audit",
    ),
    _read("search_audit", "GET /api/jobs/{job_id}/audit (paged client search)"),
    _read(
        "test_datasource",
        "POST /api/datasources/{datasource_id}/test",
        open_world=True,
        side_effects="opens a connector probe; does not persist connector state",
    ),
    _read("list_job_commits", "GET /api/jobs/{job_id}/repo/commits"),
    _read("get_job_diff", "GET /api/jobs/{job_id}/repo/diff"),
    _read("get_job_file", "GET /api/jobs/{job_id}/repo/file"),
    _read("list_job_files", "GET /api/jobs/{job_id}/repo/contents"),
    _read("list_job_tags", "GET /api/jobs/{job_id}/repo/tags"),
    _read("get_frozen_job", "GET /api/jobs/{job_id}/frozen"),
    _read("get_workspace_file", "GET /api/jobs/{job_id}/repo/file"),
    _read(
        "get_workspace_overview",
        "GET /api/jobs/{job_id}/repo/contents + /repo/file + /todos",
    ),
    _read("get_job_progress", "GET /api/jobs/{job_id}/progress"),
    _read("list_job_evidence", "GET /api/jobs/{job_id}/evidence"),
    _read("read_job_evidence", "GET /api/jobs/{job_id}/evidence/{evidence_id}"),
    _read("get_job_completion_report", "GET /api/jobs/{job_id}/completion-report"),
    _read("get_job_stats", "GET /api/stats/jobs"),
    _read("get_agent_stats", "GET /api/stats/agents", authorization=_ADMIN_AUTH),
    _read("get_stuck_jobs", "GET /api/stats/stuck"),
    _read("list_agents", "GET /api/agents", authorization=_ADMIN_AUTH),
    _read("list_experts", "GET /api/experts"),
    _read("get_expert", "GET /api/experts/{expert_id}"),
    _read("list_skills", "GET /api/skills"),
    _read("get_skill", "GET /api/skills/{skill_id}"),
    _read("list_models", "GET /api/models"),
    _read("list_datasources", "GET /api/datasources/catalog"),
    _read("list_connector_drivers", "GET /api/datasources/drivers"),
    _read("get_datasource", "GET /api/datasources/{datasource_id}"),
    _read("list_tables", "GET /api/tables", authorization=_ADMIN_AUTH),
    _read("query_table", "GET /api/tables/{table_name}", authorization=_ADMIN_AUTH),
    _read(
        "get_table_schema",
        "GET /api/tables/{table_name}/schema",
        authorization=_ADMIN_AUTH,
    ),
    _read("list_job_sources", "GET /api/sources"),
    _read("get_source_detail", "GET /api/sources/{source_id}"),
    _read("list_job_citations", "GET /api/jobs/{job_id}/citations"),
    _read("get_citation_detail", "GET /api/citations/{citation_id}"),
    _read("search_job_sources", "GET /api/jobs/{job_id}/sources/search"),
    _read(
        "get_source_annotations",
        "GET /api/jobs/{job_id}/sources/{source_id}/annotations",
    ),
    _read("get_source_tags", "GET /api/jobs/{job_id}/sources/{source_id}/tags"),
    _read("get_citation_stats", "GET /api/jobs/{job_id}/citations/stats"),
    _read("get_memory_stats", "GET /api/jobs/{job_id}/memory/stats"),
    _read(
        "get_agent_system_info",
        "GET /api/agents/{agent_id}/system-info",
        authorization=_ADMIN_AUTH,
    ),
    _read("get_daily_stats", "GET /api/stats/daily"),
    _read("get_job_log", "GET /api/jobs/{job_id}/logs"),
    _read("get_thread_log", "GET /api/persistent/threads/{thread_id}/logs"),
    _read("list_llm_requests", "GET /api/jobs/{job_id}/llm-requests"),
    _read("get_shell_state", "GET /api/jobs/{job_id}/shell-state"),
    _read("get_current_todos", "GET /api/jobs/{job_id}/todos/current"),
    _read("list_todo_archives", "GET /api/jobs/{job_id}/todos/archives"),
    _read("get_todo_archive", "GET /api/jobs/{job_id}/todos/archives/{filename}"),
    _read("get_audit_timerange", "GET /api/jobs/{job_id}/audit/timerange"),
    _read("get_knowledge_summary", "GET /api/projects/{project_id}/knowledge/summary"),
    _read("list_knowledge_notes", "GET /api/projects/{project_id}/knowledge"),
    _read("get_knowledge_note", "GET /api/projects/{project_id}/knowledge/{note_id}"),
    _read("search_knowledge", "POST /api/projects/{project_id}/knowledge/search"),
    _read("list_projects", "GET /api/projects"),
    _read("get_project", "GET /api/projects/{project_id}"),
    _read("list_project_jobs", "GET /api/projects/{project_id}/jobs"),
    _read("list_project_members", "GET /api/projects/{project_id}/members"),
    _read("list_project_experts", "GET /api/projects/{project_id}/experts"),
    _read(
        "get_project_expert",
        "GET /api/projects/{project_id}/experts/{expert_name}",
    ),
    _read("list_project_datasources", "GET /api/projects/{project_id}/datasources"),
    _read("list_sudo_requests", "GET /api/sudo/requests"),
    _read("list_message_threads", "GET /api/jobs/{job_id}/messages"),
    _read("get_message_thread", "GET /api/jobs/{job_id}/messages (client filter)"),
    _read("list_officers", "GET /api/officers"),
    _read(
        "get_project_officer",
        "GET /api/projects/{project_id}/officer + "
        "/api/persistent/threads/{thread_id}/messages",
    ),
    _read("list_persistent_threads", "GET /api/persistent/threads"),
    _read("get_persistent_thread", "GET /api/persistent/threads/{thread_id}"),
    _read(
        "get_persistent_thread_messages",
        "GET /api/persistent/threads/{thread_id}/messages",
    ),
    _read("get_persistent_thread_ide", "GET /api/persistent/threads/{thread_id}/ide"),
)

_MUTATION_CAPABILITIES = (
    _mutation(
        "manifest_apply",
        "POST /api/manifests/apply",
        "creates/updates authorized resources; Job admission may launch an image and external work",
        authorization="approved user; account/project edit grants, dependency access and execution admission enforced",
        destructive=True,
        idempotent=True,
        open_world=True,
    ),
    _mutation(
        "manifest_delete",
        "DELETE /api/resources/{resource_id}?expected_version",
        "deletes an authorized resource at the observed version; active/managed dependencies can block deletion",
        authorization="approved user; resource owner/project editor and dependency guards enforced",
        destructive=True,
        idempotent=True,
    ),
    _mutation(
        "approve_job",
        "POST /api/jobs/{job_id}/approve",
        "marks a pending-review job completed",
        authorization=_EDITOR_AUTH,
    ),
    _mutation(
        "resume_job_with_feedback",
        "POST /api/jobs/{job_id}/resume",
        "resumes agent execution and may append feedback",
        authorization=_EDITOR_AUTH,
    ),
    _mutation(
        "cancel_job",
        "PUT /api/jobs/{job_id}/cancel",
        "stops agent execution and changes lifecycle state",
        authorization=_EDITOR_AUTH,
        destructive=True,
    ),
    _mutation(
        "pause_job",
        "PUT /api/jobs/{job_id}/pause",
        "pauses agent execution and changes lifecycle state",
        authorization=_EDITOR_AUTH,
    ),
    _mutation(
        "create_job",
        "POST /api/jobs",
        "creates a job/repository and triggers automatic dispatch",
    ),
    _mutation(
        "delete_job",
        "DELETE /api/jobs/{job_id}",
        "permanently removes the job and associated records",
        authorization=_EDITOR_AUTH,
        destructive=True,
    ),
    _mutation(
        "assign_job",
        "POST /api/jobs/{job_id}/assign/{agent_id}",
        "administrative/recovery assignment override; normal creation auto-dispatches",
        authorization=_ADMIN_AUTH,
    ),
    _mutation(
        "reload_skills",
        "POST /api/skills/reload",
        "reloads process-wide bundled skill definitions",
        authorization=_ADMIN_AUTH,
        idempotent=True,
    ),
    _mutation(
        "reload_experts",
        "POST /api/experts/reload",
        "reloads process-wide expert definitions",
        authorization=_ADMIN_AUTH,
        idempotent=True,
    ),
    _mutation(
        "deregister_agent",
        "DELETE /api/agents/{agent_id}",
        "removes an agent fleet registration",
        authorization=_ADMIN_AUTH,
        destructive=True,
    ),
    _mutation(
        "create_project",
        "POST /api/projects",
        "creates a project, ownership record, and managed repositories",
    ),
    _mutation(
        "create_project_job",
        "POST /api/projects/{project_id}/jobs",
        "creates a project job/repository branch and triggers automatic dispatch",
        authorization=_EDITOR_AUTH,
    ),
    _mutation(
        "update_project",
        "PATCH /api/projects/{project_id}",
        "updates project metadata and defaults",
        authorization=_OWNER_AUTH,
        idempotent=True,
    ),
    _mutation(
        "delete_project",
        "DELETE /api/projects/{project_id}",
        "permanently removes project data, repositories, and knowledge",
        authorization=_OWNER_AUTH,
        destructive=True,
    ),
    _mutation(
        "add_project_member",
        "POST /api/projects/{project_id}/members",
        "grants a user project membership",
        authorization=_OWNER_AUTH,
    ),
    _mutation(
        "update_project_member",
        "PATCH /api/projects/{project_id}/members/{user_id}",
        "changes a member role",
        authorization=_OWNER_AUTH,
        idempotent=True,
    ),
    _mutation(
        "remove_project_member",
        "DELETE /api/projects/{project_id}/members/{user_id}",
        "revokes project membership",
        authorization=_OWNER_AUTH,
        destructive=True,
    ),
    _mutation(
        "create_datasource",
        "POST /api/datasources",
        "creates a connector and stores encrypted credentials; may schedule indexing",
        open_world=True,
    ),
    _mutation(
        "update_datasource",
        "PUT /api/datasources/{datasource_id}",
        "updates connector metadata/credentials/publication; may schedule indexing",
        idempotent=True,
        open_world=True,
    ),
    _mutation(
        "delete_datasource",
        "DELETE /api/datasources/{datasource_id}",
        "permanently removes a connector",
        destructive=True,
    ),
    _mutation(
        "link_datasource_to_project",
        "POST /api/projects/{project_id}/datasources/{datasource_id}",
        "adds a connector/project association",
        authorization=_OWNER_AUTH,
    ),
    _mutation(
        "unlink_datasource_from_project",
        "DELETE /api/projects/{project_id}/datasources/{datasource_id}",
        "removes a connector/project association and derived knowledge",
        authorization=_OWNER_AUTH,
        destructive=True,
    ),
    _mutation(
        "update_knowledge_note",
        "PATCH /api/projects/{project_id}/knowledge/{note_id}",
        "changes knowledge-note status/tags in both stores",
        authorization=_EDITOR_AUTH,
        idempotent=True,
    ),
    _mutation(
        "delete_knowledge_note",
        "DELETE /api/projects/{project_id}/knowledge/{note_id}",
        "hard-deletes a knowledge note from both stores",
        authorization=_EDITOR_AUTH,
        destructive=True,
    ),
    _mutation(
        "export_knowledge",
        "POST /api/projects/{project_id}/knowledge/export",
        "writes an Obsidian-compatible export artifact",
        authorization=_EDITOR_AUTH,
    ),
    _mutation(
        "reindex_knowledge",
        "POST /api/projects/{project_id}/knowledge/reindex",
        "rebuilds or refreshes the project knowledge index",
        authorization=_EDITOR_AUTH,
    ),
    _mutation(
        "promote_job",
        "POST /api/jobs/{job_id}/promote",
        "creates a project/repository and moves a completed job",
    ),
    _mutation(
        "approve_sudo_request",
        "POST /api/sudo/requests/{request_id}/approve",
        "authorizes a privileged command or VM upgrade and may resume a job",
        authorization=_OWNER_AUTH,
    ),
    _mutation(
        "deny_sudo_request",
        "POST /api/sudo/requests/{request_id}/deny",
        "denies a privileged action and may resume a job without it",
        authorization=_OWNER_AUTH,
    ),
    _mutation(
        "send_message_to_job",
        "POST /api/jobs/{job_id}/messages/{thread_id}/reply",
        "delivers a message or urgent interrupt to an agent",
        authorization=_EDITOR_AUTH,
        open_world=True,
    ),
    _mutation(
        "steer_job",
        "POST /api/jobs/{job_id}/messages/officer/reply",
        "delivers non-destructive guidance through the protected officer thread",
        authorization=_EDITOR_AUTH,
        open_world=True,
    ),
    _mutation(
        "reply_to_job_message",
        "POST /api/jobs/{job_id}/messages/{thread_id}/officer-reply",
        "answers a routed worker message as the commissioned officer and may "
        "resume the waiting worker",
        authorization=(
            "commissioned officer session of the job's project; verified "
            "server-side against the durable post row"
        ),
        open_world=True,
    ),
    _mutation(
        "escalate_job_message",
        "POST /api/jobs/{job_id}/messages/{thread_id}/officer-escalate",
        "hands a routed worker message to the user with delimited officer "
        "context on the same thread",
        authorization=(
            "commissioned officer session of the job's project; verified "
            "server-side against the durable post row"
        ),
        open_world=True,
    ),
    _mutation(
        "acknowledge_job_message",
        "POST /api/jobs/{job_id}/messages/{thread_id}/officer-ack",
        "closes an async routed worker message without a reply",
        authorization=(
            "commissioned officer session of the job's project; verified "
            "server-side against the durable post row"
        ),
    ),
    _mutation(
        "send_officer_note",
        "POST /api/projects/{project_id}/officer/note",
        "delivers a Legate note to the project's officer, waking him or "
        "queuing durably for his next wake",
        authorization=_OWNER_AUTH,
        open_world=True,
    ),
    _mutation(
        "create_persistent_thread",
        "POST /api/persistent/threads",
        "creates a persistent session and provisions runtime resources",
    ),
    _mutation(
        "end_persistent_thread",
        "DELETE /api/persistent/threads/{thread_id}",
        "ends a session; permanent=true also deletes its durable record",
        destructive=True,
    ),
    _mutation(
        "resume_persistent_thread",
        "POST /api/persistent/threads/{thread_id}/resume",
        "resumes a session and provisions runtime resources",
    ),
)

_ALL_CAPABILITIES = _READ_CAPABILITIES + _MUTATION_CAPABILITIES
TOOL_CAPABILITIES: dict[str, ToolCapability] = {
    capability.name: capability for capability in _ALL_CAPABILITIES
}
if len(TOOL_CAPABILITIES) != len(_ALL_CAPABILITIES):
    raise RuntimeError("Duplicate MCP capability name in authoritative manifest")


WORKFLOW_DECISIONS: tuple[WorkflowDecision, ...] = (
    WorkflowDecision(
        "direct job message-thread retrieval",
        "GET /api/jobs/{job_id}/messages",
        "superseded",
        _RESOURCE_AUTH,
        "get_message_thread filters the authorized thread collection client-side",
    ),
    WorkflowDecision(
        "job accept/reject review actions",
        "POST /api/jobs/{id}/accept|reject",
        "required",
        _EDITOR_AUTH,
        "review semantics are not equivalent to approve/resume and need a designed tool",
    ),
    WorkflowDecision(
        "job export",
        "POST /api/jobs/{id}/export",
        "required",
        _RESOURCE_AUTH,
        "portable deliverable retrieval is a supported user workflow",
    ),
    WorkflowDecision(
        "job workspace upgrade, snapshot, and IDE controls",
        "job upgrade/snapshot/ide REST operations",
        "intentionally_excluded",
        _OWNER_AUTH,
        "interactive infrastructure controls remain Cockpit-only pending explicit safety UX",
    ),
    WorkflowDecision(
        "persistent-session input and interrupt",
        "persistent thread input/interrupt REST operations",
        "required",
        _RESOURCE_AUTH,
        "create/resume/read alone do not provide an end-to-end interactive session workflow",
    ),
    WorkflowDecision(
        "persistent approvals",
        "persistent thread approval REST operations",
        "required",
        _OWNER_AUTH,
        "supervised sessions need a capability-scoped decision path",
    ),
    WorkflowDecision(
        "persistent configuration and tool-group mutation",
        "persistent config/tool-group REST operations",
        "intentionally_excluded",
        _OWNER_AUTH,
        "broad runtime reconfiguration needs a narrower schema and threat review",
    ),
    WorkflowDecision(
        "persistent cloud-diff internals",
        "persistent cloud-diff REST operations",
        "intentionally_excluded",
        _ADMIN_AUTH,
        "implementation-level staging data is not a stable user workflow",
    ),
    WorkflowDecision(
        "datasource eligibility",
        "GET /api/datasources/eligible",
        "required",
        _RESOURCE_AUTH,
        "agents need an authorized picker before attaching connectors",
    ),
    WorkflowDecision(
        "datasource indexing status and reindex",
        "datasource indexing status/reindex REST operations",
        "required",
        _EDITOR_AUTH,
        "knowledge-base connector lifecycle is incomplete without progress and recovery",
    ),
    WorkflowDecision(
        "user expert and skill administration",
        "user expert/skill CRUD REST operations",
        "intentionally_excluded",
        _ADMIN_AUTH,
        "authoring/upload workflows remain Cockpit-only until package validation is exposed",
    ),
)


__all__ = [
    "TOOL_CAPABILITIES",
    "WORKFLOW_DECISIONS",
    "ToolCapability",
    "WorkflowDecision",
]
