"""Static tool metadata, without importing tool factories or agent state.

Runtime tool modules re-export these declarations. Descriptor-backed job tools
are still derived from the shared job surface, so its schema remains the source
of truth instead of a copied snapshot.
"""

from typing import Any, Dict

from shared.orch_surface.jobs import registry_metadata

from shared.tool_catalog.names import (
    APP_GUIDE_LOADER_TOOL,
    PRODUCT_CAPABILITIES_TOOL_NAME,
)


# canvas/__init__
CANVAS_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "get_canvas": {
        "module": "canvas",
        "function": "get_canvas",
        "description": (
            "Inspect the persistent thread's shared Canvas before replacing its "
            "stage or changing a file the user may be viewing. Returns logical "
            "presentation metadata, not source bytes or credentials."
        ),
        "category": "canvas",
        "short_description": "Inspect the current shared Canvas presentation.",
        "phases": ["strategic", "tactical"],
    },
    "set_canvas": {
        "module": "canvas",
        "function": "set_canvas",
        "description": (
            "Present or refresh a validated workspace file, an attested loopback "
            "workspace port, or the current shared browser when the matching "
            "workspace capability is advertised. browser_id='current' resolves "
            "the agent's current browser at call time; control may remain with "
            "the user. Never supply a hostname or URL. Re-read files the user "
            "may have changed before overwriting them, and call set_canvas again "
            "after updating a presented source."
        ),
        "category": "canvas",
        "short_description": "Present or refresh a workspace source on Canvas.",
        "phases": ["strategic", "tactical"],
    },
    "clear_canvas": {
        "module": "canvas",
        "function": "clear_canvas",
        "description": (
            "Clear the persistent thread's shared Canvas presentation. This does "
            "not delete its source file or stop any workspace process."
        ),
        "category": "canvas",
        "short_description": "Clear the shared Canvas without deleting its source.",
        "phases": ["strategic", "tactical"],
    },
}


# citation/sources
CITATION_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "cite_document": {
        "module": "citation.sources",
        "function": "cite_document",
        "description": "Create a verified citation for document content",
        "category": "citation",
        "defer_to_workspace": True,
        "short_description": "Create verified citation for document content.",
        "phases": ["strategic", "tactical"],
    },
    "cite_web": {
        "module": "citation.sources",
        "function": "cite_web",
        "description": "Create a verified citation for web content",
        "category": "citation",
        "defer_to_workspace": True,
        "short_description": "Create verified citation for web content.",
        "phases": ["strategic", "tactical"],
    },
    "list_sources": {
        "module": "citation.sources",
        "function": "list_sources",
        "description": "List all registered citation sources",
        "category": "citation",
        "defer_to_workspace": True,
        "short_description": "List all registered citation sources.",
        "phases": ["strategic", "tactical"],
    },
    "get_citation": {
        "module": "citation.sources",
        "function": "get_citation",
        "description": "Get details about a specific citation",
        "category": "citation",
        "defer_to_workspace": True,
        "short_description": "Get details about a specific citation by ID.",
        "phases": ["strategic", "tactical"],
    },
    "list_citations": {
        "module": "citation.sources",
        "function": "list_citations",
        "description": "List all citations created in this session",
        "category": "citation",
        "defer_to_workspace": True,
        "short_description": "List all citations with status and source info.",
        "phases": ["strategic", "tactical"],
    },
    "edit_citation": {
        "module": "citation.sources",
        "function": "edit_citation",
        "description": "Edit fields of an existing citation",
        "category": "citation",
        "defer_to_workspace": True,
        "short_description": "Edit citation fields (claim, quote, confidence, etc.).",
        "phases": ["strategic", "tactical"],
    },
    "annotate_source": {
        "module": "citation.sources",
        "function": "annotate_source",
        "description": "Add a note, highlight, summary, question, or critique to a source",
        "category": "citation",
        "defer_to_workspace": True,
        "short_description": "Add annotation to a citation source.",
        "phases": ["strategic", "tactical"],
    },
    "get_annotations": {
        "module": "citation.sources",
        "function": "get_annotations",
        "description": "Get annotations for a source",
        "category": "citation",
        "defer_to_workspace": True,
        "short_description": "Get annotations for a citation source.",
        "phases": ["strategic", "tactical"],
    },
    "tag_source": {
        "module": "citation.sources",
        "function": "tag_source",
        "description": "Add or remove tags on a citation source",
        "category": "citation",
        "defer_to_workspace": True,
        "short_description": "Add or remove tags on a citation source.",
        "phases": ["strategic", "tactical"],
    },
    "search_library": {
        "module": "citation.sources",
        "function": "search_library",
        "description": "Search the source library using keyword, semantic, or hybrid search",
        "category": "citation",
        "defer_to_workspace": True,
        "short_description": "Search source library with hybrid retrieval and evidence labels.",
        "phases": ["strategic", "tactical"],
    },
    "generate_bibliography": {
        "module": "citation.sources",
        "function": "generate_bibliography",
        "description": "Generate a formatted bibliography/references file from citations",
        "category": "citation",
        "defer_to_workspace": True,
        "short_description": "Generate formatted bibliography file from citations.",
        "phases": ["strategic", "tactical"],
    },
}


# communication/messaging
COMMUNICATION_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "send_message": {
        "module": "communication.messaging",
        "function": "send_message",
        "description": (
            "Send a message to a human via email. Use mode='async' to continue "
            "working, or mode='blocking' to pause execution until a reply arrives. "
            "Use sparingly — the recipient receives this as an email."
        ),
        "category": "communication",
        "short_description": "Email the job owner. Supports async and blocking modes.",
        "phases": ["strategic", "tactical"],
    },
}


# core/job
JOB_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "mark_complete": {
        "module": "core.job",
        "function": "mark_complete",
        "description": "Signal task/phase completion with structured report",
        "category": "core",
        "phases": ["strategic", "tactical"],  # Both modes
    },
    "job_complete": {
        "module": "core.job",
        "function": "job_complete",
        "description": "Signal FINAL job completion - call when all phases are done",
        "category": "core",
        "phases": ["strategic"],  # Strategic-only: prevents premature termination
    },
}


# core/officer
OFFICER_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "sleep": {
        "module": "core.officer",
        "function": "sleep",
        "description": (
            "End this wake and sleep for a number of minutes. Any event "
            "(job transition, user message) wakes you earlier; the timer is "
            "durable and survives restarts. Officer sessions only."
        ),
        "category": "core",
        "short_description": "End the wake; sleep until the timer or an event.",
        "phases": ["strategic", "tactical"],  # phase-free in sessions
        # No config lists this; persistent_session.py:1554-1557 appends it.
        "grant": "code",
        "gate": "officer.enabled is True",
    },
    "notify_user": {
        "module": "core.officer",
        "function": "notify_user",
        "description": (
            "Message your Legate (the user) out-of-band. urgency='log' for "
            "the record only, 'digest' for their next look at the "
            "notification center, 'page' for an immediate notification. "
            "Also how you answer a Legate note when they are not live in "
            "your session. Officer sessions only."
        ),
        "category": "core",
        "short_description": "Message the user: log, digest, or page.",
        "phases": ["strategic", "tactical"],
        # No config lists this; persistent_session.py:1554-1557 appends it.
        "grant": "code",
        "gate": "officer.enabled is True",
    },
}


# core/session_task_tools
SESSION_TASK_METADATA: Dict[str, Dict[str, Any]] = {
    "task_add": {
        "module": "core.session_task_tools",
        "function": "task_add",
        "description": "Add a task to the session task list",
        "category": "session_task",
        "phases": ["strategic", "tactical"],
    },
    "task_complete": {
        "module": "core.session_task_tools",
        "function": "task_complete",
        "description": "Mark a session task as completed",
        "category": "session_task",
        "phases": ["strategic", "tactical"],
    },
    "task_list": {
        "module": "core.session_task_tools",
        "function": "task_list",
        "description": "List all session tasks with status",
        "category": "session_task",
        "phases": ["strategic", "tactical"],
    },
}


# core/todo
TODO_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "next_phase_todos": {
        "module": "core.todo",
        "function": "next_phase_todos",
        "description": "Stage todos for the next tactical phase",
        "category": "core",
        "phases": ["strategic"],  # Strategic-only: creates work for tactical phase
    },
    "todo_complete": {
        "module": "core.todo",
        "function": "todo_complete",
        "description": "Mark a single task as complete (one at a time)",
        "category": "core",
        "phases": ["strategic", "tactical"],  # Both: used in all phases
    },
    "todo_list": {
        "module": "core.todo",
        "function": "todo_list",
        "description": "List all todos with IDs and status",
        "category": "core",
        "phases": ["strategic", "tactical"],  # Both: helps see current state
    },
    "request_replan": {
        "module": "core.todo",
        "function": "request_replan",
        "description": "End this phase early and return to planning, keeping all work and todo state",
        "category": "core",
        "phases": ["tactical"],  # Tactical-only: the in-flight adaptation path
    },
}


# core/upgrade
WORKSPACE_UPGRADE_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "request_workspace_upgrade": {
        "module": "core.upgrade",
        "function": "request_workspace_upgrade",
        "description": (
            "Request an upgrade from the lite workspace to a real sandbox "
            "container (shell, git, file tools). A human decides before "
            "anything is provisioned; you only request, and you may not be "
            "resumed afterwards."
        ),
        "category": "core",
        "short_description": "Ask to upgrade to a real sandbox workspace.",
        "phases": ["strategic", "tactical"],  # Available in both modes
        # No config lists this; persistent_session.py:1547 and agent.py:3078
        # append it, and only where there is something to upgrade to.
        "grant": "code",
        "gate": "lite tier — backend.supports_shell is False",
    },
}


# delegation/control_plane
def _metadata(
    description: str,
    short_description: str,
) -> Dict[str, Any]:
    return {
        "module": "delegation.control_plane",
        "description": description,
        "short_description": short_description,
        "category": "delegation",
        "phases": ["strategic", "tactical"],
        "grant": "explicit",
        "gate": (
            "named outright in a tools.delegation list AND "
            "delegation.enabled is true; children never receive delegation "
            "or its control plane"
        ),
    }


CONTROL_PLANE_METADATA: Dict[str, Dict[str, Any]] = {
    "wait_agent": {
        **_metadata(
            "Wait for one background subagent, or for any background "
            "subagent when handle is omitted. Use this only when its next "
            "update is immediately blocking your work. Completion reports "
            "are pushed into your next turn automatically, so do not poll "
            "with wait_agent or list_agents. Timeout is 10-3600 seconds.",
            "Wait once for a blocking subagent update; never poll.",
        ),
        "function": "wait_agent",
    },
    "message_agent": {
        **_metadata(
            "Send a concise steering message to an addressable background "
            "subagent by handle. A queued or running child accepts steering. "
            "A terminal child is durably revived on its existing transcript "
            "and worktree with a new fenced generation. Consume its prior "
            "report first. The next report is pushed automatically; do not "
            "poll after messaging.",
            "Steer a live child or durably revive a terminal one.",
        ),
        "function": "message_agent",
    },
    "stop_agent": {
        **_metadata(
            "Ask a background subagent to stop by handle. It gets a bounded "
            "grace window for a tool-less partial synthesis before a hard "
            "stop. The terminal report is pushed automatically; do not poll "
            "for it.",
            "Stop a background subagent after a bounded synthesis grace.",
        ),
        "function": "stop_agent",
    },
    "list_agents": {
        **_metadata(
            "List this parent's addressable background subagents as a "
            "bounded status view. It never returns transcripts or full "
            "reports. Completions are pushed automatically; use this for an "
            "occasional roster check, never as a polling loop.",
            "List bounded background-subagent statuses; never transcripts.",
        ),
        "function": "list_agents",
    },
}


# delegation/delegate_agent
DELEGATE_AGENT_METADATA: Dict[str, Dict[str, Any]] = {
    "delegate_agent": {
        "module": "delegation.delegate_agent",
        "function": "delegate_agent",
        "description": (
            "Delegate ONE bounded brief to a built-in subagent of the given "
            "type. Foreground calls return its report as this tool's result. "
            "Background calls return an immediate durable receipt and push "
            "the completion into a later parent turn automatically; never "
            "poll for it. Subagents run in-process on the parent's workspace "
            "with a fresh context and their own turn/token budgets; they "
            "cannot delegate further."
        ),
        "category": "delegation",
        "short_description": (
            "Delegate a bounded brief in the foreground or durable background."
        ),
        "phases": ["strategic", "tactical"],
        "grant": "explicit",
        "gate": (
            "named outright in a tools.delegation list AND delegation.enabled "
            "is true — the factory creates the tool only when both hold; "
            "children never receive it (depth 1, D7)"
        ),
    },
}


# email/tools
EMAIL_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "email_list_folders": {
        "module": "email.tools",
        "function": "email_list_folders",
        "category": "email",
        "phases": ["strategic", "tactical"],
        "description": "List accessible mailbox folders with message/unseen counts",
    },
    "email_list": {
        "module": "email.tools",
        "function": "email_list",
        "category": "email",
        "phases": ["strategic", "tactical"],
        "description": "List message envelopes in a folder (UIDs, newest first)",
    },
    "email_search": {
        "module": "email.tools",
        "function": "email_search",
        "category": "email",
        "phases": ["strategic", "tactical"],
        "description": (
            "Search messages by text/sender/subject/date across allowed folders"
        ),
    },
    "email_read": {
        "module": "email.tools",
        "function": "email_read",
        "category": "email",
        "phases": ["strategic", "tactical"],
        "description": (
            "Read one message: headers + bounded snippet; body saved to workspace"
        ),
    },
    "email_move": {
        "module": "email.tools",
        "function": "email_move",
        "category": "email",
        "phases": ["tactical"],
        "description": "Move messages to another folder (archive/trash are moves)",
    },
    "email_flag": {
        "module": "email.tools",
        "function": "email_flag",
        "category": "email",
        "phases": ["tactical"],
        "description": "Mark messages read/unread, or star/unstar them",
    },
    "email_draft": {
        "module": "email.tools",
        "function": "email_draft",
        "category": "email",
        "phases": ["tactical"],
        "description": (
            "Compose a plain-text draft into the Drafts folder "
            "(reply in-thread or to allowlisted recipients)"
        ),
    },
    "email_send": {
        "module": "email.tools",
        "function": "email_send",
        "category": "email",
        "phases": ["tactical"],
        "description": (
            "Send mail as the user via SMTP (gated unless unattended send is enabled)"
        ),
    },
}


# evaluation/evaluation_tools
EVALUATION_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "approve_job_verdict": {
        "module": "evaluation.evaluation_tools",
        "function": "approve_job_verdict",
        "description": "Approve a target job that is pending review",
        "category": "evaluation",
        "short_description": "Approve a pending_review job (transitions to completed).",
        "phases": ["strategic"],
    },
    "return_job_with_feedback": {
        "module": "evaluation.evaluation_tools",
        "function": "return_job_with_feedback",
        "description": "Resume a target job with feedback for the original agent to address",
        "category": "evaluation",
        "short_description": "Return a job to the original agent with issues to fix.",
        "phases": ["strategic"],
    },
}


# git/git_tools
GIT_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "git_log": {
        "module": "git.git_tools",
        "function": "git_log",
        "description": "View commit history with filtering",
        "category": "git",
        "short_description": "View commit history (default: last 10 commits). Pass repo='<name>' for an attached repository.",
        "phases": ["strategic", "tactical"],
    },
    "git_show": {
        "module": "git.git_tools",
        "function": "git_show",
        "description": "Inspect a specific commit's changes",
        "category": "git",
        "short_description": "Show commit details and diff (use stat_only=true for summary). Pass repo='<name>' for an attached repository.",
        "phases": ["strategic", "tactical"],
    },
    "git_diff": {
        "module": "git.git_tools",
        "function": "git_diff",
        "description": "Compare current state to previous commits",
        "category": "git",
        "short_description": "Show differences (uncommitted changes or between refs). Pass repo='<name>' for an attached repository.",
        "phases": ["strategic", "tactical"],
    },
    "git_status": {
        "module": "git.git_tools",
        "function": "git_status",
        "description": "See uncommitted changes and workspace state",
        "category": "git",
        "short_description": "Show current branch and uncommitted changes. Pass repo='<name>' for an attached repository.",
        "phases": ["strategic", "tactical"],
    },
    "git_tags": {
        "module": "git.git_tools",
        "function": "git_tags",
        "description": "List phase milestone tags",
        "category": "git",
        "short_description": "List git tags for this job (use all_jobs=True for all). Pass repo='<name>' for an attached repository.",
        "phases": ["strategic", "tactical"],
    },
}


# graph/neo4j
GRAPH_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "cypher_query": {
        "module": "graph.neo4j",
        "function": "cypher_query",
        "description": "Execute a read-only Cypher query against Neo4j",
        "category": "graph",
        "defer_to_workspace": True,
        "short_description": "Execute read-only Cypher query against Neo4j.",
        "phases": ["tactical"],
    },
    "cypher_execute": {
        "module": "graph.neo4j",
        "function": "cypher_execute",
        "description": "Execute a write Cypher statement (CREATE, MERGE, DELETE, SET) against Neo4j",
        "category": "graph",
        "defer_to_workspace": True,
        "short_description": "Execute write Cypher (CREATE/MERGE/DELETE/SET) against Neo4j.",
        "phases": ["tactical"],
    },
    "get_database_schema": {
        "module": "graph.neo4j",
        "function": "get_database_schema",
        "description": "Get Neo4j database schema (labels, relationships, properties)",
        "category": "graph",
        "defer_to_workspace": True,
        "short_description": "Get Neo4j schema (labels, relationships, properties).",
        "phases": ["tactical"],
    },
}


# knowledge/knowledge_tools
KNOWLEDGE_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    # Write tools
    "kb_write": {
        "module": "knowledge.knowledge_tools",
        "function": "kb_write",
        "description": "Create a new knowledge note (Neo4j + pgvector write-through)",
        "category": "knowledge",
        "short_description": "Create a knowledge note in the project knowledge base.",
        "phases": ["strategic", "tactical"],
    },
    "kb_update": {
        "module": "knowledge.knowledge_tools",
        "function": "kb_update",
        "description": "Update an existing knowledge note",
        "category": "knowledge",
        "short_description": "Update a knowledge note (append, status, tags, links).",
        "phases": ["strategic", "tactical"],
    },
    "kb_delete": {
        "module": "knowledge.knowledge_tools",
        "function": "kb_delete",
        "description": (
            "Retire a knowledge note (archive with a reason; reversible, "
            "hidden from search, purged later by the grace-period lane)"
        ),
        "category": "knowledge",
        "short_description": "Retire a knowledge note with a reason (archive, reversible).",
        "phases": ["strategic", "tactical"],
    },
    # Read tools
    "kb_read": {
        "module": "knowledge.knowledge_tools",
        "function": "kb_read",
        "description": "Read a full knowledge note with metadata and relationships",
        "category": "knowledge",
        "short_description": "Read a knowledge note by slug ID.",
        "phases": ["strategic", "tactical"],
    },
    "kb_list": {
        "module": "knowledge.knowledge_tools",
        "function": "kb_list",
        "description": "List knowledge notes with optional filters",
        "category": "knowledge",
        "short_description": "List knowledge notes (filter by type, tag, status).",
        "phases": ["strategic", "tactical"],
    },
    "kb_search": {
        "module": "knowledge.knowledge_tools",
        "function": "kb_search",
        "description": "Hybrid search over knowledge base (semantic + keyword + recency)",
        "category": "knowledge",
        "short_description": "Search knowledge base with hybrid ranking.",
        "phases": ["strategic", "tactical"],
    },
    "kb_grep": {
        "module": "knowledge.knowledge_tools",
        "function": "kb_grep",
        "description": "Enumerate every matching line in the knowledge base (substring or regex, with context)",
        "category": "knowledge",
        "short_description": "Grep the knowledge base for a literal or regex.",
        "phases": ["strategic", "tactical"],
    },
    # Graph query tools
    "kb_related": {
        "module": "knowledge.knowledge_tools",
        "function": "kb_related",
        "description": "Find notes related to a given note (graph traversal)",
        "category": "knowledge",
        "short_description": "Find related notes within N hops.",
        "phases": ["strategic", "tactical"],
    },
    "kb_contradictions": {
        "module": "knowledge.knowledge_tools",
        "function": "kb_contradictions",
        "description": "Find contradicting notes in the knowledge base",
        "category": "knowledge",
        "short_description": "List notes connected by CONTRADICTS edges.",
        "phases": ["strategic", "tactical"],
    },
    "kb_provenance": {
        "module": "knowledge.knowledge_tools",
        "function": "kb_provenance",
        "description": "Trace a note's derivation chain (DERIVED_FROM)",
        "category": "knowledge",
        "short_description": "Trace DERIVED_FROM chain for a note.",
        "phases": ["strategic", "tactical"],
    },
    "kb_unanswered": {
        "module": "knowledge.knowledge_tools",
        "function": "kb_unanswered",
        "description": "List open questions with no answers",
        "category": "knowledge",
        "short_description": "List question notes without ANSWERS edges.",
        "phases": ["strategic", "tactical"],
    },
    # Export
    "kb_export": {
        "module": "knowledge.knowledge_tools",
        "function": "kb_export",
        "description": "Export knowledge base as OKF/markdown files",
        "category": "knowledge",
        "short_description": "Export knowledge base to OKF .md files.",
        "phases": ["strategic", "tactical"],
    },
    # Maintenance / gardener (slice 2)
    "kb_lint": {
        "module": "knowledge.knowledge_tools",
        "function": "kb_lint",
        "description": "Lint an OKF knowledge base for structural/link/id issues",
        "category": "knowledge",
        "short_description": "Lint the knowledge base (frontmatter, links, ids).",
        "phases": ["strategic", "tactical"],
    },
    "kb_index": {
        "module": "knowledge.knowledge_tools",
        "function": "kb_index",
        "description": "Regenerate the OKF index.md for a knowledge base",
        "category": "knowledge",
        "short_description": "Regenerate index.md (grouped links by type).",
        "phases": ["strategic", "tactical"],
    },
}


# loop/plan
LOOP_PLAN_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "loop_plan": {
        "module": "loop.plan",
        "function": "loop_plan",
        "description": (
            "File the loop's next campaign plan: pick ONE initiative (an "
            "existing KB note), schedule 1..K execution stages toward it, "
            "pre-register the acceptance evidence the closing critic will "
            "check, and dispose the previous campaign (ship/extend/kill) if "
            "one awaits review. To close a reviewed campaign WITHOUT opening "
            "a new one, call with only disposition_outcome=ship|kill (no "
            "initiative, no stages). The plan is applied when this job "
            "completes; re-filing replaces the previous plan."
        ),
        "category": "loop",
        "short_description": "File the next campaign plan (checkpoint critic only).",
        "phases": ["strategic", "tactical"],
    },
}


# mongodb/mongo
MONGODB_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "mongo_query": {
        "module": "mongodb.mongo",
        "function": "mongo_query",
        "description": "Query documents from a MongoDB collection with optional filters",
        "category": "mongodb",
        "defer_to_workspace": True,
        "short_description": "Query documents from a MongoDB collection.",
        "phases": ["tactical"],
    },
    "mongo_aggregate": {
        "module": "mongodb.mongo",
        "function": "mongo_aggregate",
        "description": "Run an aggregation pipeline on a MongoDB collection",
        "category": "mongodb",
        "defer_to_workspace": True,
        "short_description": "Run aggregation pipeline on a MongoDB collection.",
        "phases": ["tactical"],
    },
    "mongo_schema": {
        "module": "mongodb.mongo",
        "function": "mongo_schema",
        "description": "Inspect MongoDB database schema (collections, sample fields, indexes)",
        "category": "mongodb",
        "defer_to_workspace": True,
        "short_description": "Inspect MongoDB schema (collections, fields, indexes).",
        "phases": ["tactical"],
    },
    "mongo_insert": {
        "module": "mongodb.mongo",
        "function": "mongo_insert",
        "description": "Insert one or more documents into a MongoDB collection",
        "category": "mongodb",
        "defer_to_workspace": True,
        "short_description": "Insert documents into a MongoDB collection.",
        "phases": ["tactical"],
    },
    "mongo_update": {
        "module": "mongodb.mongo",
        "function": "mongo_update",
        "description": "Update documents in a MongoDB collection",
        "category": "mongodb",
        "defer_to_workspace": True,
        "short_description": "Update documents in a MongoDB collection.",
        "phases": ["tactical"],
    },
}


# orchestrator/catalog
CATALOG_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "list_experts": {
        "module": "orchestrator.catalog",
        "function": "list_experts",
        "description": (
            "List bundled and visible user/global experts. Read-only expert "
            "catalog inspection."
        ),
        "category": "agent_catalog",
        "short_description": "List visible experts.",
        "phases": ["strategic", "tactical"],
    },
    "get_expert": {
        "module": "orchestrator.catalog",
        "function": "get_expert",
        "description": (
            "Get a compact summary of an expert's merged configuration, "
            "instructions preview, enabled tool categories, and effective models."
        ),
        "category": "agent_catalog",
        "short_description": "Inspect an expert.",
        "phases": ["strategic", "tactical"],
    },
    "list_skills": {
        "module": "orchestrator.catalog",
        "function": "list_skills",
        "description": "List bundled and visible user/global skills.",
        "category": "agent_catalog",
        "short_description": "List visible skills.",
        "phases": ["strategic", "tactical"],
    },
    "search_skills": {
        "module": "orchestrator.catalog",
        "function": "search_skills",
        "description": (
            "Search visible skills by id, name, display name, description, "
            "source, and tags."
        ),
        "category": "agent_catalog",
        "short_description": "Search visible skills.",
        "phases": ["strategic", "tactical"],
    },
    "get_skill": {
        "module": "orchestrator.catalog",
        "function": "get_skill",
        "description": (
            "Get a compact summary of a skill, including metadata, file index, "
            "and SKILL.md preview. Does not dump the full file tree by default."
        ),
        "category": "agent_catalog",
        "short_description": "Inspect a skill.",
        "phases": ["strategic", "tactical"],
    },
    "get_expert_bundle": {
        "module": "orchestrator.catalog",
        "function": "get_expert_bundle",
        "description": (
            "Get a portable JSON expert bundle for editing or forking. "
            "Authoring support; not injected by default."
        ),
        "category": "catalog_authoring",
        "short_description": "Get editable expert JSON.",
        "phases": ["strategic", "tactical"],
    },
    "set_expert_bundle": {
        "module": "orchestrator.catalog",
        "function": "set_expert_bundle",
        "description": (
            "Create, update, or fork an expert from a portable JSON bundle. "
            "dry_run defaults true; set dry_run=false to write. Authoring "
            "support; not injected by default."
        ),
        "category": "catalog_authoring",
        "short_description": "Create or update an expert from JSON.",
        "phases": ["strategic", "tactical"],
    },
    "get_skill_bundle": {
        "module": "orchestrator.catalog",
        "function": "get_skill_bundle",
        "description": (
            "Get a portable JSON skill bundle with the full file tree for "
            "editing or forking. Authoring support; not injected by default."
        ),
        "category": "catalog_authoring",
        "short_description": "Get editable skill JSON.",
        "phases": ["strategic", "tactical"],
    },
    "set_skill_bundle": {
        "module": "orchestrator.catalog",
        "function": "set_skill_bundle",
        "description": (
            "Create, update, or fork a skill from a portable JSON bundle. "
            "dry_run defaults true; set dry_run=false to write. Authoring "
            "support; not injected by default."
        ),
        "category": "catalog_authoring",
        "short_description": "Create or update a skill from JSON.",
        "phases": ["strategic", "tactical"],
    },
}


# orchestrator/jobs
ORCHESTRATOR_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "get_session_context": {
        "module": "orchestrator.jobs",
        "function": "get_session_context",
        "description": (
            "Summarize the current persistent session context: thread ID, user "
            "ID, project scope, workspace availability, backend capabilities, "
            "cloud mount status, knowledge/connector availability, the chat "
            "models this deployment routes, and the caller's effective grants."
        ),
        "category": "orchestrator",
        "short_description": "Show current session/project/workspace context.",
        "phases": ["strategic", "tactical"],
    },
    **registry_metadata(),
}


# orchestrator/projects
PROJECT_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "get_current_project": {
        "module": "orchestrator.projects",
        "function": "get_current_project",
        "description": (
            "Get details for the project associated with this persistent "
            "session. Uses the session's project scope from the thread context."
        ),
        "category": "orchestrator",
        "short_description": "Get the current session project.",
        "phases": ["strategic", "tactical"],
    },
    "list_project_jobs": {
        "module": "orchestrator.projects",
        "function": "list_project_jobs",
        "description": (
            "List jobs in the current project, or in a specified project when "
            "the caller has access. Returns full job IDs and useful summaries."
        ),
        "category": "orchestrator",
        "short_description": "List jobs for the current or selected project.",
        "phases": ["strategic", "tactical"],
    },
}


# orchestrator/repositories
REPOSITORY_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "list_project_repositories": {
        "module": "orchestrator.repositories",
        "function": "list_project_repositories",
        "description": (
            "List repositories attached to the current or selected project. "
            "Returned URLs are safe/redacted display URLs."
        ),
        "category": "orchestrator",
        "short_description": "List project repositories.",
        "phases": ["strategic", "tactical"],
    },
    "get_default_project_repository": {
        "module": "orchestrator.repositories",
        "function": "get_default_project_repository",
        "description": (
            "Show the project's preferred writable source repository metadata. "
            "Project cloud files and job history are not repository roles."
        ),
        "category": "orchestrator",
        "short_description": "Show the default project repository.",
        "phases": ["strategic", "tactical"],
    },
    "checkout_project_repository": {
        "module": "orchestrator.repositories",
        "function": "checkout_project_repository",
        "description": (
            "Clone a project repository into the current session workspace. "
            "Requires a shell-capable sandbox or VM workspace."
        ),
        "category": "orchestrator",
        "short_description": "Clone a project repository into the session workspace.",
        "phases": ["strategic", "tactical"],
        # No config lists this; persistent_session.py:1540 appends it. It is
        # also one of the three `orchestrator` entries absent from
        # SESSION_TOOL_OVERRIDE_NAMES, so the code grant is what keeps a
        # category-level `orchestrator: true` from widening onto it.
        "grant": "code",
        "gate": "fleet management enabled AND backend.supports_shell",
    },
}


# orchestrator/workflows
WORKFLOW_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "list_automations": {
        "module": "orchestrator.workflows",
        "function": "list_automations",
        "description": "List visible cron automations for the user or a project.",
        "category": "workflows",
        "short_description": "List automations.",
        "phases": ["strategic", "tactical"],
    },
    "get_automation": {
        "module": "orchestrator.workflows",
        "function": "get_automation",
        "description": "Inspect one visible automation.",
        "category": "workflows",
        "short_description": "Inspect an automation.",
        "phases": ["strategic", "tactical"],
    },
    "list_automation_runs": {
        "module": "orchestrator.workflows",
        "function": "list_automation_runs",
        "description": "List jobs spawned by an automation.",
        "category": "workflows",
        "short_description": "List automation runs.",
        "phases": ["strategic", "tactical"],
    },
    "propose_automation": {
        "module": "orchestrator.workflows",
        "function": "propose_automation",
        "description": (
            "Draft a disabled automation JSON bundle without writing it. Use "
            "set_automation_bundle with dry_run=false only after user approval."
        ),
        "category": "workflows",
        "short_description": "Draft a disabled automation.",
        "phases": ["strategic", "tactical"],
    },
    "get_automation_bundle": {
        "module": "orchestrator.workflows",
        "function": "get_automation_bundle",
        "description": (
            "Get a portable JSON automation bundle for editing. Authoring "
            "support; not injected by default."
        ),
        "category": "catalog_authoring",
        "short_description": "Get editable automation JSON.",
        "phases": ["strategic", "tactical"],
    },
    "set_automation_bundle": {
        "module": "orchestrator.workflows",
        "function": "set_automation_bundle",
        "description": (
            "Create or update an automation from JSON. dry_run defaults true; "
            "creation writes disabled automations unless allow_enabled=true. "
            "Authoring support; not injected by default."
        ),
        "category": "catalog_authoring",
        "short_description": "Create or update an automation from JSON.",
        "phases": ["strategic", "tactical"],
    },
    "get_project_loop": {
        "module": "orchestrator.workflows",
        "function": "get_project_loop",
        "description": "Inspect the current or most recent project loop.",
        "category": "workflows",
        "short_description": "Inspect the project loop.",
        "phases": ["strategic", "tactical"],
    },
    "list_project_loop_jobs": {
        "module": "orchestrator.workflows",
        "function": "list_project_loop_jobs",
        "description": "List jobs spawned by the active project loop.",
        "category": "workflows",
        "short_description": "List project loop jobs.",
        "phases": ["strategic", "tactical"],
    },
    "explain_project_loop": {
        "module": "orchestrator.workflows",
        "function": "explain_project_loop",
        "description": (
            "Explain the project loop state, active stage, remaining budget, "
            "last error, and likely next actions."
        ),
        "category": "workflows",
        "short_description": "Explain project loop state.",
        "phases": ["strategic", "tactical"],
    },
}


# product_capabilities
PRODUCT_CAPABILITY_TOOLS_METADATA: dict[str, dict[str, Any]] = {
    PRODUCT_CAPABILITIES_TOOL_NAME: {
        "module": "product_capabilities",
        "function": PRODUCT_CAPABILITIES_TOOL_NAME,
        "description": (
            "Check the current SRW build, deployment, permission, attachment, "
            "workspace, loaded-tool, and actionability state for exact product "
            "topics or capability IDs. The current user and thread are bound by "
            "the runtime and cannot be supplied by the model. This snapshot is "
            "advisory; an operation must still enforce current policy."
        ),
        "category": "product_help",
        "short_description": "Check current SRW capability and session state.",
        "phases": ["strategic", "tactical"],
        # Persistent-session floor appended at persistent_session.py:1442-1448
        # behind an operator-owned env canary, independent of every
        # user-selectable tool group.
        "grant": "code",
        "gate": "PRODUCT_CAPABILITIES_TOOL_ENABLED env canary",
    }
}


# product_help
PRODUCT_HELP_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    APP_GUIDE_LOADER_TOOL: {
        "module": "product_help",
        "function": APP_GUIDE_LOADER_TOOL,
        "description": (
            "Read the current SRW product guide without relying on workspace "
            "files. Pass topic_id='index' to load its procedure and current "
            "topic IDs, then read the one topic that explicitly covers the "
            "user's requested outcome. If no index row covers an exact combined "
            "workflow, stop after index and report the guide gap instead of "
            "composing adjacent features."
        ),
        "category": "product_help",
        "phases": ["strategic", "tactical"],
        # Persistent-session floor appended at persistent_session.py:1427-1432
        # and removed again by the operator break-glass. `product_help` has no
        # ToolsConfig field at all, so a config naming it is discarded today.
        "grant": "code",
        "gate": "app-guide break-glass off (app_guide_break_glass_disabled)",
    },
}


# repo/repo_tools
REPO_TOOLS_METADATA = {
    "repo_checkout": {
        "category": "repo",
        "description": (
            "Switch an attached repository to a branch, optionally creating it."
        ),
        "short_description": "Switch (or create) a branch in an attached repository.",
    },
    "repo_commit": {
        "category": "repo",
        "description": "Stage all changes and commit them in an attached repository.",
        "short_description": "Stage and commit changes in an attached repository.",
    },
    "repo_push": {
        "category": "repo",
        "description": "Push a branch of an attached repository to its remote.",
        "short_description": "Push the current branch of an attached repository.",
    },
    "repo_pull": {
        "category": "repo",
        "description": "Fast-forward pull in an attached repository.",
        "short_description": "Fast-forward pull in an attached repository.",
    },
    "repo_open_pr": {
        "category": "repo",
        "description": (
            "Open a pull request (merge request on GitLab) for an attached repository."
        ),
        "short_description": "Open a pull/merge request for an attached repository.",
    },
    "repo_pr_status": {
        "category": "repo",
        "description": (
            "Read the live open, merged, or closed state of a pull request "
            "(merge request on GitLab) in an attached repository."
        ),
        "short_description": "Read live pull/merge request status.",
    },
}


# research/browser_direct
BROWSER_DIRECT_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "browser_navigate": {
        "module": "research.browser_direct",
        "function": "browser_navigate",
        "description": (
            "Open a URL in the browser and see the page. Use when you need "
            "to inspect, interact with, or visually verify a specific page."
        ),
        "category": "browser_direct",
        "short_description": "Navigate to URL and return page DOM + screenshot.",
        "phases": ["tactical"],
    },
    "browser_snapshot": {
        "module": "research.browser_direct",
        "function": "browser_snapshot",
        "description": (
            "Get the current page state — DOM accessibility tree and optional "
            "screenshot. Use after actions to see what changed."
        ),
        "category": "browser_direct",
        "short_description": "Return current page DOM snapshot + screenshot.",
        "phases": ["tactical"],
    },
    "browser_click": {
        "module": "research.browser_direct",
        "function": "browser_click",
        "description": "Click an element on the page by its reference number from the DOM snapshot.",
        "category": "browser_direct",
        "short_description": "Click element by DOM reference number.",
        "phases": ["tactical"],
    },
    "browser_type": {
        "module": "research.browser_direct",
        "function": "browser_type",
        "description": "Type literal text or a workspace credential environment variable (env_var) into an input field identified by its reference number. Supply exactly one of text or env_var.",
        "category": "browser_direct",
        "short_description": "Type text into input field by reference number.",
        "phases": ["tactical"],
    },
    "browser_select": {
        "module": "research.browser_direct",
        "function": "browser_select",
        "description": "Select an option from a dropdown by its reference number.",
        "category": "browser_direct",
        "short_description": "Select dropdown option by reference number.",
        "phases": ["tactical"],
    },
    "browser_scroll": {
        "module": "research.browser_direct",
        "function": "browser_scroll",
        "description": (
            "Scroll the page or a specific element. Direction: up, down, left, right."
        ),
        "category": "browser_direct",
        "short_description": "Scroll page or element in a direction.",
        "phases": ["tactical"],
    },
    "browser_screenshot": {
        "module": "research.browser_direct",
        "function": "browser_screenshot",
        "description": "Take a screenshot of the current page. Always returns an image regardless of multimodal setting.",
        "category": "browser_direct",
        "short_description": "Take full screenshot of current page.",
        "phases": ["tactical"],
    },
    "browser_back": {
        "module": "research.browser_direct",
        "function": "browser_back",
        "description": "Navigate back to the previous page in browser history.",
        "category": "browser_direct",
        "short_description": "Go back one page in browser history.",
        "phases": ["tactical"],
    },
    "browser_close": {
        "module": "research.browser_direct",
        "function": "browser_close",
        "description": "Close the browser and free resources. The browser will restart on next use.",
        "category": "browser_direct",
        "short_description": "Close the browser session.",
        "phases": ["tactical"],
    },
}


# research/papers
PAPER_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "search_papers": {
        "module": "research.papers",
        "function": "search_papers",
        "description": "Search academic databases for papers",
        "category": "research",
        "short_description": "Search arXiv or Semantic Scholar for academic papers.",
        "phases": ["tactical"],
    },
    "download_paper": {
        "module": "research.papers",
        "function": "download_paper",
        "description": "Download paper PDF to workspace",
        "category": "research",
        "short_description": "Download paper PDF using arXiv/Unpaywall/browser fallback chain.",
        "phases": ["tactical"],
    },
    "get_paper_info": {
        "module": "research.papers",
        "function": "get_paper_info",
        "description": "Get metadata and citation info for a paper",
        "category": "research",
        "short_description": "Get paper metadata, abstract, and citations via Semantic Scholar.",
        "phases": ["tactical"],
    },
}


# research/web
RESEARCH_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "web_search": {
        "module": "research.web",
        "function": "web_search",
        "description": (
            "Search the web. Results are returned as bounded "
            "snippets; raw page content can be fetched and archived to the "
            "workspace for later reading/citation."
        ),
        "category": "research",
        "defer_to_workspace": True,
        "short_description": (
            "Search the web; archive full text and return compact snippets."
        ),
        "phases": ["tactical"],
    },
    "extract_webpage": {
        "module": "research.web",
        "function": "extract_webpage",
        "description": (
            "Extract full content from web pages. Content is archived when "
            "possible and inline output is bounded per call."
        ),
        "category": "research",
        "short_description": (
            "Extract and archive page content from URLs with bounded inline output."
        ),
        "phases": ["tactical"],
    },
    "crawl_website": {
        "module": "research.web",
        "function": "crawl_website",
        "description": (
            "Crawl a website from a URL. Page content is archived when possible "
            "and returned as snippets with saved-file pointers."
        ),
        "category": "research",
        "short_description": "Crawl and archive website pages with compact snippets.",
        "phases": ["tactical"],
    },
    "map_website": {
        "module": "research.web",
        "function": "map_website",
        "description": "Map website structure to discover URLs",
        "category": "research",
        "short_description": "Discover URLs in a website's structure.",
        "phases": ["tactical"],
    },
}


# research/workflow
RESEARCH_WORKFLOW_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "research_topic": {
        "module": "research.workflow",
        "function": "research_topic",
        "description": "Comprehensive literature search across multiple databases",
        "category": "research",
        "short_description": "Search arXiv + Semantic Scholar, deduplicate, download OA papers.",
        "phases": ["tactical"],
    },
}


# shell/coding_tools
CODING_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {}


# shell/shell_tools
SHELL_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "run_command": {
        "module": "shell.shell_tools",
        "function": "run_command",
        "description": "Execute a shell command and return its output",
        "category": "shell",
        "short_description": "Run a shell command and get output.",
        "phases": ["strategic", "tactical"],
    },
    "cancel_command": {
        "module": "shell.shell_tools",
        "function": "cancel_command",
        "description": "Abort a stuck/hung command by sending Ctrl+C to the shell tab",
        "category": "shell",
        "short_description": "Cancel a stuck shell command (Ctrl+C).",
        "phases": ["strategic", "tactical"],
    },
    "shell_execute": {
        "module": "shell.shell_tools",
        "function": "shell_execute",
        "description": "Execute a command or send keystrokes in an independent persistent terminal tab",
        "category": "shell",
        "short_description": "Run commands in a persistent terminal tab.",
        "phases": ["strategic", "tactical"],
    },
    "shell_read": {
        "module": "shell.shell_tools",
        "function": "shell_read",
        "description": "Read scrollback output from a persistent terminal tab",
        "category": "shell",
        "short_description": "Read output from a terminal tab.",
        "phases": ["strategic", "tactical"],
    },
    "srw_cloud_status": {
        "module": "shell.shell_tools",
        "function": "srw_cloud_status",
        "description": "Show rclone cloud mount status, cache usage, and rclone RC stats",
        "category": "shell",
        "short_description": "Show cloud mount/cache status.",
        "phases": ["strategic", "tactical"],
        # No config lists this; persistent_session.py:1526 appends it. Keeping
        # it out of a category-level `shell: true` also stops an operator who
        # wanted "shell commands on" from silently acquiring a cloud-mount
        # reporter that happens to share the category.
        "grant": "code",
        "gate": "cloud_mount_manager.active",
    },
}


# sql/postgresql
SQL_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "sql_query": {
        "module": "sql.postgresql",
        "function": "sql_query",
        "description": "Execute a read-only SQL query against the PostgreSQL connector",
        "category": "sql",
        "defer_to_workspace": True,
        "short_description": "Execute a read-only SQL query against PostgreSQL.",
        "phases": ["tactical"],
    },
    "sql_schema": {
        "module": "sql.postgresql",
        "function": "sql_schema",
        "description": "Inspect the PostgreSQL connector schema (tables, columns, types)",
        "category": "sql",
        "defer_to_workspace": True,
        "short_description": "Inspect the PostgreSQL schema (tables, columns, types).",
        "phases": ["tactical"],
    },
    "sql_execute": {
        "module": "sql.postgresql",
        "function": "sql_execute",
        "description": "Execute a write SQL statement (INSERT, UPDATE, DELETE, DDL) against the PostgreSQL connector",
        "category": "sql",
        "defer_to_workspace": True,
        "short_description": "Execute write SQL (INSERT/UPDATE/DELETE/DDL) against PostgreSQL.",
        "phases": ["tactical"],
    },
}


# webdav/tools
WEBDAV_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "webdav_list": {
        "module": "webdav.tools",
        "function": "webdav_list",
        "category": "webdav",
        "phases": ["strategic", "tactical"],
        "description": "List files and folders in WebDAV",
    },
    "webdav_read": {
        "module": "webdav.tools",
        "function": "webdav_read",
        "category": "webdav",
        "phases": ["strategic", "tactical"],
        "description": "Download a file from WebDAV into the workspace",
    },
    "webdav_info": {
        "module": "webdav.tools",
        "function": "webdav_info",
        "category": "webdav",
        "phases": ["strategic", "tactical"],
        "description": "Get metadata about a file or folder in WebDAV",
    },
    "webdav_write": {
        "module": "webdav.tools",
        "function": "webdav_write",
        "category": "webdav",
        "phases": ["tactical"],
        "description": "Upload a file from the workspace to WebDAV",
    },
    "webdav_delete": {
        "module": "webdav.tools",
        "function": "webdav_delete",
        "category": "webdav",
        "phases": ["tactical"],
        "description": "Delete a file or folder from WebDAV",
    },
}


# workspace/files
FILE_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "read_file": {
        "module": "workspace.files",
        "function": "read_file",
        "description": "Read content from a file in the workspace",
        "category": "workspace",
        "phases": ["strategic", "tactical"],
    },
    "write_file": {
        "module": "workspace.files",
        "function": "write_file",
        "description": "Write content to a file (requires read_file first for existing files)",
        "category": "workspace",
        "phases": ["strategic", "tactical"],
    },
    "edit_file": {
        "module": "workspace.files",
        "function": "edit_file",
        "description": "Edit a file: replace text, or use position='end'/'start' to append/prepend (requires read_file first)",
        "category": "workspace",
        "phases": ["strategic", "tactical"],
    },
}


# workspace/filesystem
FILESYSTEM_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "list_files": {
        "module": "workspace.filesystem",
        "function": "list_files",
        "description": "List files and directories in the workspace",
        "category": "workspace",
        "phases": ["strategic", "tactical"],
    },
    "delete_file": {
        "module": "workspace.filesystem",
        "function": "delete_file",
        "description": "Delete a file or empty directory",
        "category": "workspace",
        "phases": ["strategic", "tactical"],
    },
    "search_files": {
        "module": "workspace.filesystem",
        "function": "search_files",
        "description": "Search workspace files for literal text (not a regex)",
        "category": "workspace",
        "phases": ["strategic", "tactical"],
    },
    "file_exists": {
        "module": "workspace.filesystem",
        "function": "file_exists",
        "description": "Check if a file or directory exists",
        "category": "workspace",
        "phases": ["strategic", "tactical"],
    },
    "move_file": {
        "module": "workspace.filesystem",
        "function": "move_file",
        "description": "Move or rename a file/directory in the workspace",
        "category": "workspace",
        "phases": ["strategic", "tactical"],
    },
    "rename_file": {
        "module": "workspace.filesystem",
        "function": "rename_file",
        "description": "Rename a file or directory (keeps it in the same location)",
        "category": "workspace",
        "phases": ["strategic", "tactical"],
    },
    "copy_file": {
        "module": "workspace.filesystem",
        "function": "copy_file",
        "description": "Copy a file within the workspace",
        "category": "workspace",
        "phases": ["strategic", "tactical"],
    },
    "get_document_info": {
        "module": "workspace.filesystem",
        "function": "get_document_info",
        "description": "Get document metadata (page count, size) for planning access",
        "category": "workspace",
        "defer_to_workspace": True,
        "short_description": "Get PDF/document metadata (pages, size) for planning access.",
        "phases": ["strategic", "tactical"],
    },
    "create_directory": {
        "module": "workspace.filesystem",
        "function": "create_directory",
        "description": "Create a directory (and parents) in the workspace",
        "category": "workspace",
        "phases": ["strategic", "tactical"],
    },
    "delete_directory": {
        "module": "workspace.filesystem",
        "function": "delete_directory",
        "description": "Delete a directory and all its contents",
        "category": "workspace",
        "phases": ["strategic", "tactical"],
    },
}


# workspace/skills
SKILL_TOOLS_METADATA: Dict[str, Dict[str, Any]] = {
    "use_skill": {
        "module": "workspace.skills",
        "function": "use_skill",
        "description": "Load a skill's SKILL.md guidance into context for the current task",
        "category": "workspace",
        "phases": ["strategic", "tactical"],
    },
}
