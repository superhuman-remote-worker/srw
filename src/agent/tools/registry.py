"""Tool registry for dynamic tool loading.

Provides a centralized registry of available tools and functions
to load them based on configuration. This enables the Universal Agent
to load different tool sets based on its config file.

Usage:
    from agent.tools import load_tools, ToolContext

    context = ToolContext(workspace_manager=ws)
    tools = load_tools(["read_file", "write_file", "list_files"], context)

    # Or load all tools in a category
    tools = load_tools_by_category("workspace", context)
"""

import functools
import logging
from typing import Any, Dict, List, Optional

from agent.tools.canvas import create_canvas_tools
from agent.tools.citation import create_citation_tools
from agent.tools.webdav import create_webdav_tools
from agent.tools.repo import create_repo_tools
from agent.tools.communication import create_communication_tools
from agent.tools.context import ToolContext

# Import from core toolkit package
from agent.tools.core import create_core_tools
from agent.tools.core.session_task_tools import create_session_task_tools
from agent.tools.delegation import create_delegation_tools
from agent.tools.email import create_email_tools

# Import domain tools
from agent.tools.evaluation import create_evaluation_tools
from agent.tools.git import create_git_tools
from agent.tools.graph import create_graph_tools
from agent.tools.knowledge import create_knowledge_tools
from agent.tools.loop import create_loop_tools
from agent.tools.mongodb import create_mongodb_tools
from agent.tools.orchestrator import create_orchestrator_tools
from agent.tools.orchestrator.catalog import create_catalog_tools
from agent.tools.orchestrator.workflows import create_workflow_tools
from agent.tools.product_capabilities import create_product_capability_tools
from agent.tools.product_help import create_product_help_tools
from agent.tools.research import create_browser_direct_tools, create_research_tools
from agent.tools.shell import create_shell_tools
from agent.tools.sql import create_sql_tools

# Import workspace tools from new package
from agent.tools.workspace import create_workspace_tools

from shared.tool_catalog import (
    CODE_GRANTED_CATEGORIES as CODE_GRANTED_CATEGORIES,
    TOOL_REGISTRY as TOOL_REGISTRY,
    _EXECUTION_CATEGORIES as _EXECUTION_CATEGORIES,
    _classify_code_granted_categories as _classify_code_granted_categories,
    get_available_tools as get_available_tools,
    get_categories as get_categories,
    get_tools_by_category as get_tools_by_category,
)

logger = logging.getLogger(__name__)


def register_mcp_tools(manager: Any | None) -> None:
    """Replace dynamic MCP registry entries with a manager's live tools.

    Passing ``None`` clears entries when a pooled worker starts a job/session
    without MCP datasources or a live session detaches its final MCP server.
    """
    stale_names = [
        name
        for name, metadata in TOOL_REGISTRY.items()
        if metadata.get("category") == "mcp"
    ]
    for name in stale_names:
        del TOOL_REGISTRY[name]

    tools = manager.get_langchain_tools() if manager is not None else []
    for tool in tools:
        tool_metadata = getattr(tool, "metadata", None) or {}
        TOOL_REGISTRY[tool.name] = {
            "category": "mcp",
            "phases": ["strategic", "tactical"],
            "description": (getattr(tool, "description", None) or "")[:200],
            "mcp_server": tool_metadata.get("mcp_server"),
            "mcp_server_slug": tool_metadata.get("mcp_server_slug"),
            "mcp_tool_name": tool_metadata.get("mcp_tool_name", tool.name),
        }

    logger.info(
        "Registered %d MCP tools across %d server statuses",
        len(tools),
        len(getattr(manager, "statuses", {})) if manager is not None else 0,
    )


def expand_tool_wildcards(tool_names: List[str]) -> List[str]:
    """Expand the MCP datasource ``"*"`` sentinel after tool discovery."""
    if "*" not in tool_names:
        return tool_names

    expanded: List[str] = []
    for name in [
        *(name for name in tool_names if name != "*"),
        *get_tools_by_category("mcp"),
    ]:
        if name not in expanded:
            expanded.append(name)
    return expanded


#: Prefix a single-phase tool's description carries when every tool is bound
#: at once: the bound schema no longer filters by phase, so
#: the description states the phase generically — never a list of the other
#: tools (models read such lists as exhaustive; see the todo_list_footer
#: comment in config/guardrails/default.yaml). Both-phase tools carry none.
PHASE_DESCRIPTION_PREFIXES: Dict[str, str] = {
    "strategic": "[strategic-phase tool] ",
    "tactical": "[tactical-phase tool] ",
}


def single_phase_of(tool_name: str) -> Optional[str]:
    """The one phase a registered tool is declared for, else ``None``.

    ``None`` for tools available in both phases and for names without a
    registry entry (dynamic/test tools carry no phase restriction).
    """
    meta = TOOL_REGISTRY.get(tool_name)
    if not meta:
        return None
    phases = set(meta.get("phases", ["strategic", "tactical"]))
    if len(phases) != 1:
        return None
    (phase,) = phases
    return phase if phase in PHASE_DESCRIPTION_PREFIXES else None


def filter_tools_by_phase(tool_names: List[str], phase: str) -> List[str]:
    """Filter tool names to only those available in the given phase.

    Tools without a 'phases' field are assumed to be available in both phases.

    Args:
        tool_names: List of tool names to filter
        phase: Phase to filter for ("strategic" or "tactical")

    Returns:
        Filtered list of tool names available in the phase
    """
    filtered = []
    for name in tool_names:
        if name not in TOOL_REGISTRY:
            continue
        meta = TOOL_REGISTRY[name]
        phases = meta.get("phases", ["strategic", "tactical"])  # Default: both
        if phase in phases:
            filtered.append(name)
    return filtered


def filter_tools_by_backend(tool_names: List[str], backend: Any) -> List[str]:
    """Drop tools the workspace backend can't support (capability gate).

    Enforcement-by-construction for the lite tiers
    (``knowledge-base/knowledge/features/no_workspace_agent_mode.md`` §3.2/§7): instead of trusting
    each config's tool lists to omit them, tools are removed whenever the
    backend doesn't declare the matching capability —

    - ``not backend.supports_shell`` → drop ``shell``, ``browser_direct``,
      ``git`` (all need a workspace-backed execution environment). This is what
      ``WorkspaceBackend.supports_shell`` already promises: it "gates
      ShellManager construction *and* shell tool registration".
    - ``not backend.supports_file_tools`` → drop ``workspace`` (file) tools and
      the Slice-1 file-only ``canvas`` category — the ``none`` tier, whose
      ScratchBackend is internal-only (§6).
    - ``not backend.supports_canvas_presentation`` → drop only ``canvas``. The
      process-local virtual-memory transport has file tools, but a separate
      orchestrator process cannot materialize those bytes for the viewer.

    Everything else (web research, datasource SQL/graph/Mongo/WebDAV, knowledge,
    core, communication, delegation, citation) passes through. ``backend=None``
    is a no-op, so non-agent callers (docs, cockpit display) see the full set.
    """
    if backend is None:
        return tool_names
    drop_categories: set[str] = set()
    if not getattr(backend, "supports_shell", False):
        drop_categories.update(_EXECUTION_CATEGORIES)
    if not getattr(backend, "supports_file_tools", True):
        drop_categories.add("workspace")
        drop_categories.add("canvas")
    # Canvas is a positive, externally materializable capability. Unknown and
    # custom backends must opt in explicitly; file-tool support alone says
    # nothing about whether the orchestrator can read the same bytes.
    if not getattr(backend, "supports_canvas_presentation", False):
        drop_categories.add("canvas")
    if not drop_categories:
        return tool_names
    kept: List[str] = []
    dropped: List[str] = []
    for name in tool_names:
        category = TOOL_REGISTRY.get(name, {}).get("category")
        (dropped if category in drop_categories else kept).append(name)
    if dropped:
        logger.info(
            "Backend capability gate dropped %d tool(s) %s "
            "(supports_shell=%s, supports_file_tools=%s, supports_canvas=%s)",
            len(dropped),
            sorted(dropped),
            getattr(backend, "supports_shell", False),
            getattr(backend, "supports_file_tools", True),
            getattr(backend, "supports_canvas_presentation", False),
        )
    return kept


# ---------------------------------------------------------------------------
# Background-officer capability ceiling (knowledge-base/knowledge/features/officer_knowledge_plane.md §4)
# ---------------------------------------------------------------------------
# A commissioned background officer owns the project's knowledge plane and its
# control plane, but never the object plane. These sets deny every
# object-plane affordance — regardless of what the expert config, project
# override, or request override asked for. This is a runtime CEILING applied
# after override resolution, not a default the officer (or a config) can edit.
#
# Denied whole categories: local file tools, shell, git, direct browser,
# canvas presentation, repo remote-ops, and WebDAV (a file surface over the
# project/cloud folders — the object plane in API form).
_OFFICER_DENIED_CATEGORIES = frozenset(
    {
        "workspace",
        "shell",
        "git",
        "browser_direct",
        "canvas",
        "repo",
        "webdav",
    }
)

# Denied individual names:
# - request_workspace_upgrade: the lite-session escape hatch into a sandbox —
#   the one workspace-acquisition path a shell-less session has.
# - checkout_project_repository: acquires a repo checkout in the workspace.
# - list_project_repositories / get_default_project_repository: repository
#   discovery (clone URLs); the officer delegates object inspection instead.
# - srw_cloud_status: project cloud mounts are never attached to a background
#   officer; the status tool must not resurface them via an override.
# - kb_export: workspace-oriented KB migration/export — deliberately outside
#   the officer's explicit knowledge grant (§3), so an override cannot
#   restore it either.
_OFFICER_DENIED_TOOLS = frozenset(
    {
        "request_workspace_upgrade",
        "checkout_project_repository",
        "list_project_repositories",
        "get_default_project_repository",
        "srw_cloud_status",
        "kb_export",
    }
)

# Belt-and-suspenders for names that are not in TOOL_REGISTRY (datasource- or
# runtime-injected variants): anything shaped like a browser/git/repo/webdav
# tool is object plane.
_OFFICER_DENIED_PREFIXES = ("browser_", "git_", "repo_", "webdav_")


def officer_ceiling_active(officer_cfg: Any) -> bool:
    """Whether ``officer_cfg`` describes a commissioned BACKGROUND officer.

    Keys on the runtime fact ``officer.enabled is True`` — never on agent_id:
    a conference is the same Centurion expert with ``enabled: False`` and must
    stay an ordinary interactive session (user-selected workspace included).
    Strict ``is True`` so MagicMock configs in tests can never trip the
    ceiling; accepts the parsed ``OfficerConfig`` dataclass or a plain dict.
    """
    if isinstance(officer_cfg, dict):
        return officer_cfg.get("enabled") is True
    return getattr(officer_cfg, "enabled", False) is True


def apply_officer_tool_ceiling(tool_names: List[str], officer_cfg: Any) -> List[str]:
    """Drop object-plane tools for a commissioned background officer.

    Called AFTER the full override merge, the runtime extras appends, and
    ``filter_tools_by_backend`` — so a project/session override that granted
    (or a backend that would support) shell/file/git/browser/canvas/repo
    tools, ``request_workspace_upgrade``, repository discovery, cloud status,
    or ``kb_export`` still cannot put them in front of a background officer.
    Control-plane job tools, knowledge gardening, delegation, communication,
    and research pass through untouched. No-op for conferences and every
    non-officer session (see :func:`officer_ceiling_active`).
    """
    if not officer_ceiling_active(officer_cfg):
        return tool_names
    kept: List[str] = []
    dropped: List[str] = []
    for name in tool_names:
        metadata = TOOL_REGISTRY.get(name, {})
        category = metadata.get("category")
        denied = (
            name in _OFFICER_DENIED_TOOLS
            or category in _OFFICER_DENIED_CATEGORIES
            # officer_supervision_surface E2/§3.4: job tools on the
            # job_workspace plane (arbitrary repo file/tree reads, workspace
            # overview, shell state, unbounded diff/commit browsing) are the
            # object plane in job-tool form — the ceiling denies them even
            # when a config override names them explicitly.
            or metadata.get("plane") == "job_workspace"
            or (isinstance(name, str) and name.startswith(_OFFICER_DENIED_PREFIXES))
        )
        (dropped if denied else kept).append(name)
    if dropped:
        logger.info(
            "Background-officer capability ceiling dropped %d tool(s): %s",
            len(dropped),
            sorted(set(dropped)),
        )
    return kept


def get_tools_for_phase(phase: str) -> List[str]:
    """Get all tool names available in a given phase.

    Args:
        phase: Phase to get tools for ("strategic" or "tactical")

    Returns:
        List of tool names available in the phase
    """
    return [
        name
        for name, meta in TOOL_REGISTRY.items()
        if phase in meta.get("phases", ["strategic", "tactical"])
        and not meta.get("placeholder", False)
    ]


def get_phase_tool_summary() -> Dict[str, Dict[str, List[str]]]:
    """Get a summary of tools by phase and category.

    Returns:
        Dictionary with structure:
        {
            "strategic": {"workspace": [...], "core": [...], ...},
            "tactical": {"workspace": [...], "research": [...], ...}
        }
    """
    summary = {
        "strategic": {},
        "tactical": {},
    }

    for name, meta in TOOL_REGISTRY.items():
        if meta.get("placeholder", False):
            continue

        phases = meta.get("phases", ["strategic", "tactical"])
        category = meta.get("category", "unknown")

        for phase in phases:
            if phase not in summary:
                continue
            if category not in summary[phase]:
                summary[phase][category] = []
            summary[phase][category].append(name)

    return summary


def load_tools_for_phase(
    tool_names: List[str],
    phase: str,
    context: ToolContext,
) -> List[Any]:
    """Load tools filtered by phase availability.

    Convenience function that filters tools by phase and then loads them.

    Args:
        tool_names: List of tool names to potentially load
        phase: Phase to filter for ("strategic" or "tactical")
        context: ToolContext with dependencies

    Returns:
        List of loaded tools available in the specified phase

    Example:
        ```python
        # Load all configured tools, but only those available in strategic phase
        tools = load_tools_for_phase(
            ["read_file", "write_file", "next_phase_todos", "web_search"],
            phase="strategic",
            context=ctx
        )
        # Result: Only tools available in the strategic phase
        ```
    """
    filtered_names = filter_tools_by_phase(tool_names, phase)
    if not filtered_names:
        logger.warning(f"No tools available for phase '{phase}' from: {tool_names}")
        return []
    return load_tools(filtered_names, context)


def load_tools(tool_names: List[str], context: ToolContext) -> List[Any]:
    """Load tools by name from the registry.

    This function creates tool instances with the provided context,
    enabling dependency injection of workspace managers, database
    connections, and other resources.

    Args:
        tool_names: List of tool names to load
        context: ToolContext with dependencies

    Returns:
        List of LangChain Tool objects ready to bind to LLM

    Raises:
        ValueError: If a tool is not found or not implemented

    Example:
        ```python
        context = ToolContext(workspace_manager=ws)
        tools = load_tools(["read_file", "write_file"], context)
        ```
    """
    # Validate all tool names first
    unknown_tools = [name for name in tool_names if name not in TOOL_REGISTRY]
    if unknown_tools:
        available = ", ".join(sorted(TOOL_REGISTRY.keys()))
        raise ValueError(
            f"Unknown tools: {unknown_tools}. Available tools: {available}"
        )

    # Check for placeholder tools
    placeholder_tools = [
        name for name in tool_names if TOOL_REGISTRY[name].get("placeholder", False)
    ]
    if placeholder_tools:
        raise ValueError(
            f"Tools not yet implemented: {placeholder_tools}. "
            f"These will be available in later phases."
        )

    # Group tools by category for efficient loading
    tools_by_category: Dict[str, List[str]] = {}
    for name in tool_names:
        category = TOOL_REGISTRY[name].get("category", "unknown")
        if category not in tools_by_category:
            tools_by_category[category] = []
        tools_by_category[category].append(name)

    # Load tools by category
    all_tools = []

    # Workspace tools
    if "workspace" in tools_by_category:
        if not context.has_workspace():
            raise ValueError("Workspace tools require workspace_manager in ToolContext")
        workspace_tools = create_workspace_tools(context)
        requested = set(tools_by_category["workspace"])
        for tool in workspace_tools:
            if tool.name in requested:
                all_tools.append(tool)
                logger.debug(f"Loaded workspace tool: {tool.name}")

    # Core tools (todo + job + control). Todo/job tools need a workspace (todos
    # persist to workspace files); todo tools additionally need a TodoManager.
    # The lone exception is the control tool request_workspace_upgrade, which
    # needs neither manager — so a lite session running with todo_manager=None
    # can still expose it (workspace_tier_upgrade.md §4.2 S5). Require each
    # manager only when a tool that actually depends on it was requested.
    if "core" in tools_by_category:
        from agent.tools.core.todo import TODO_TOOLS_METADATA
        from agent.tools.core.upgrade import WORKSPACE_UPGRADE_TOOLS_METADATA
        from agent.tools.core.officer import OFFICER_TOOLS_METADATA

        requested = set(tools_by_category["core"])
        needs_workspace = (
            requested
            - set(WORKSPACE_UPGRADE_TOOLS_METADATA)
            - set(OFFICER_TOOLS_METADATA)
        )
        if needs_workspace and not context.has_workspace():
            raise ValueError("Core tools require workspace_manager in ToolContext")
        if requested & set(TODO_TOOLS_METADATA) and not context.has_todo():
            raise ValueError("Core tools require todo_manager in ToolContext")
        core_tools = create_core_tools(context)
        for tool in core_tools:
            if tool.name in requested:
                all_tools.append(tool)
                logger.debug(f"Loaded core tool: {tool.name}")

    # Session task tools (lightweight todos for persistent sessions)
    if "session_task" in tools_by_category:
        try:
            st_tools = create_session_task_tools(context)
            requested = set(tools_by_category["session_task"])
            for tool in st_tools:
                if tool.name in requested:
                    all_tools.append(tool)
                    logger.debug(f"Loaded session task tool: {tool.name}")
        except Exception as e:
            logger.debug(f"Session task tools not available: {e}")

    # Research tools
    if "research" in tools_by_category:
        try:
            research_tools = create_research_tools(context)
            requested = set(tools_by_category["research"])
            for tool in research_tools:
                if tool.name in requested:
                    all_tools.append(tool)
                    logger.debug(f"Loaded research tool: {tool.name}")
        except Exception as e:
            logger.warning(f"Could not load research tools: {e}")

    # Managed product help is persistent-session infrastructure and has no
    # workspace, datasource, or optional-service dependency.
    if "product_help" in tools_by_category:
        product_help_tools = [
            *create_product_help_tools(context),
            *create_product_capability_tools(context),
        ]
        requested = set(tools_by_category["product_help"])
        for tool in product_help_tools:
            if tool.name in requested:
                all_tools.append(tool)
                logger.debug(f"Loaded product_help tool: {tool.name}")

    # Direct browser control tools
    if "browser_direct" in tools_by_category:
        try:
            bd_tools = create_browser_direct_tools(context)
            requested = set(tools_by_category["browser_direct"])
            for tool in bd_tools:
                if tool.name in requested:
                    all_tools.append(tool)
                    logger.debug(f"Loaded browser_direct tool: {tool.name}")
        except Exception as e:
            logger.warning(f"Could not load browser_direct tools: {e}")

    # Citation tools
    if "citation" in tools_by_category:
        try:
            cite_tools = create_citation_tools(context)
            requested = set(tools_by_category["citation"])
            for tool in cite_tools:
                if tool.name in requested:
                    all_tools.append(tool)
                    logger.debug(f"Loaded citation tool: {tool.name}")
        except Exception as e:
            logger.warning(f"Could not load citation tools: {e}")

    # Graph tools
    if "graph" in tools_by_category:
        if not context.has_datasource("neo4j"):
            logger.warning("Graph tools require a neo4j datasource in ToolContext")
        else:
            try:
                graph_tools = create_graph_tools(context)
                requested = set(tools_by_category["graph"])
                for tool in graph_tools:
                    if tool.name in requested:
                        all_tools.append(tool)
                        logger.debug(f"Loaded graph tool: {tool.name}")
            except Exception as e:
                logger.warning(f"Could not load graph tools: {e}")

    # SQL tools
    if "sql" in tools_by_category:
        if not context.has_datasource("postgresql"):
            logger.warning("SQL tools require a postgresql datasource in ToolContext")
        else:
            try:
                sql_tools = create_sql_tools(context)
                requested = set(tools_by_category["sql"])
                for tool in sql_tools:
                    if tool.name in requested:
                        all_tools.append(tool)
                        logger.debug(f"Loaded sql tool: {tool.name}")
            except Exception as e:
                logger.warning(f"Could not load sql tools: {e}")

    # MongoDB tools
    if "mongodb" in tools_by_category:
        if not context.has_datasource("mongodb"):
            logger.warning("MongoDB tools require a mongodb datasource in ToolContext")
        else:
            try:
                mongo_tools = create_mongodb_tools(context)
                requested = set(tools_by_category["mongodb"])
                for tool in mongo_tools:
                    if tool.name in requested:
                        all_tools.append(tool)
                        logger.debug(f"Loaded mongodb tool: {tool.name}")
            except Exception as e:
                logger.warning(f"Could not load mongodb tools: {e}")

    # WebDAV datasource tools
    if "webdav" in tools_by_category:
        if not context.has_datasource("webdav"):
            logger.warning("WebDAV tools require a webdav datasource in ToolContext")
        else:
            try:
                webdav_tools = create_webdav_tools(context)
                requested = set(tools_by_category["webdav"])
                for tool in webdav_tools:
                    if tool.name in requested:
                        all_tools.append(tool)
                        logger.debug(f"Loaded webdav tool: {tool.name}")
            except Exception as e:
                logger.warning(f"Could not load webdav tools: {e}")

    # Repository datasource write tools. NOTE: unlike the other datasource
    # toolkits this cannot use context.has_datasource() — repository
    # datasources never enter context.datasources (process_datasources skips
    # them); the clones live on workspace_manager instead.
    if "repo" in tools_by_category:
        ws = context.workspace_manager
        if not getattr(ws, "source_repos", None):
            logger.warning("Repo tools require at least one cloned repository")
        else:
            try:
                repo_tools = create_repo_tools(context)
                requested = set(tools_by_category["repo"])
                for tool in repo_tools:
                    if tool.name in requested:
                        all_tools.append(tool)
                        logger.debug(f"Loaded repo tool: {tool.name}")
            except Exception as e:
                logger.warning(f"Could not load repo tools: {e}")

    # MCP datasource tools are already-live LangChain tools owned by the
    # per-job/session MCPManager. Missing or failed servers degrade cleanly.
    if "mcp" in tools_by_category:
        if not context.has_datasource("mcp"):
            logger.warning(
                "MCP tools require an mcp datasource connection in ToolContext"
            )
        else:
            try:
                manager = context.get_datasource("mcp")
                requested = set(tools_by_category["mcp"])
                for tool in manager.get_langchain_tools():
                    if tool.name in requested:
                        all_tools.append(tool)
                        logger.debug(f"Loaded MCP tool: {tool.name}")
            except Exception as e:
                logger.warning("Could not load MCP tools: %s", type(e).__name__)

    # Email datasource tools
    if "email" in tools_by_category:
        if not context.has_datasource("email"):
            logger.warning("Email tools require an email datasource in ToolContext")
        else:
            try:
                email_tools = create_email_tools(context)
                requested = set(tools_by_category["email"])
                for tool in email_tools:
                    if tool.name in requested:
                        all_tools.append(tool)
                        logger.debug(f"Loaded email tool: {tool.name}")
            except Exception as e:
                logger.warning(f"Could not load email tools: {e}")

    # Git tools
    if "git" in tools_by_category:
        if not context.has_workspace():
            logger.warning("Git tools require workspace_manager in ToolContext")
        elif context.workspace_manager.git_manager is None:
            logger.warning("Git tools require git_manager on workspace_manager")
        else:
            try:
                git_tools = create_git_tools(context)
                requested = set(tools_by_category["git"])
                for tool in git_tools:
                    if tool.name in requested:
                        all_tools.append(tool)
                        logger.debug(f"Loaded git tool: {tool.name}")
            except Exception as e:
                logger.warning(f"Could not load git tools: {e}")

    # Shell tools
    if "shell" in tools_by_category:
        if not context.has_workspace():
            logger.warning("Shell tools require workspace_manager in ToolContext")
        else:
            try:
                requested = set(tools_by_category["shell"])
                # The resolved names already chose the shell mode; the factory
                # must not re-derive it from a second config read.
                shell_tools = create_shell_tools(context, requested)
                for tool in shell_tools:
                    if tool.name in requested:
                        all_tools.append(tool)
                        logger.debug(f"Loaded shell tool: {tool.name}")
            except Exception as e:
                logger.warning(f"Could not load shell tools: {e}")

    # Evaluation tools
    if "evaluation" in tools_by_category:
        try:
            eval_tools = create_evaluation_tools(context)
            requested = set(tools_by_category["evaluation"])
            for tool in eval_tools:
                if tool.name in requested:
                    all_tools.append(tool)
                    logger.debug(f"Loaded evaluation tool: {tool.name}")
        except Exception as e:
            logger.warning(f"Could not load evaluation tools: {e}")

    # Knowledge tools
    if "knowledge" in tools_by_category:
        if not context.has_knowledge():
            logger.warning(
                "Knowledge tools require knowledge_store in ToolContext "
                "(knowledge_graph/Neo4j is optional — graph tools degrade)"
            )
        else:
            try:
                knowledge_tools = create_knowledge_tools(context)
                requested = set(tools_by_category["knowledge"])
                for tool in knowledge_tools:
                    if tool.name in requested:
                        all_tools.append(tool)
                        logger.debug(f"Loaded knowledge tool: {tool.name}")
            except Exception as e:
                logger.warning(f"Could not load knowledge tools: {e}")

    # Communication tools
    if "communication" in tools_by_category:
        try:
            comm_tools = create_communication_tools(context)
            requested = set(tools_by_category["communication"])
            for tool in comm_tools:
                if tool.name in requested:
                    all_tools.append(tool)
                    logger.debug(f"Loaded communication tool: {tool.name}")
        except Exception as e:
            logger.warning(f"Could not load communication tools: {e}")

    # Loop campaign tools (checkpoint-critic plan filing — injected per-job by
    # the orchestrator, never present in bundled expert configs)
    if "loop" in tools_by_category:
        try:
            loop_tools = create_loop_tools(context)
            requested = set(tools_by_category["loop"])
            for tool in loop_tools:
                if tool.name in requested:
                    all_tools.append(tool)
                    logger.debug(f"Loaded loop tool: {tool.name}")
        except Exception as e:
            logger.warning(f"Could not load loop tools: {e}")

    # Delegation tools (subagent spawning)
    if "delegation" in tools_by_category:
        try:
            requested = set(tools_by_category["delegation"])
            delegation_tools = create_delegation_tools(context, requested)
            for tool in delegation_tools:
                if tool.name in requested:
                    all_tools.append(tool)
                    logger.debug(f"Loaded delegation tool: {tool.name}")
        except Exception as e:
            logger.warning(f"Could not load delegation tools: {e}")

    # Orchestrator application tools. Job operations have descriptor-owned
    # control/inspection categories; the flat orchestrator category remains
    # for projects, repositories, catalog/workflows, and session context.
    orchestrator_categories = {
        "orchestrator",
        "job_control",
        "job_inspection",
    } & tools_by_category.keys()
    if orchestrator_categories:
        try:
            orch_tools = create_orchestrator_tools(context)
            requested = {
                name
                for category in orchestrator_categories
                for name in tools_by_category[category]
            }
            for tool in orch_tools:
                if tool.name in requested:
                    all_tools.append(tool)
                    logger.debug("Loaded orchestrator application tool: %s", tool.name)
        except Exception as e:
            logger.warning(f"Could not load orchestrator tools: {e}")

    # Dynamic Canvas tools are persistent-session-only. The category is still
    # registered globally so config/catalog surfaces can describe it, but a
    # worker context cannot accidentally acquire a nonfunctional adapter.
    if "canvas" in tools_by_category:
        if not context.thread_id or not context.user_id:
            logger.warning(
                "Canvas tools require an authenticated persistent ToolContext"
            )
        else:
            try:
                canvas_tools = create_canvas_tools(context)
                requested = set(tools_by_category["canvas"])
                for tool in canvas_tools:
                    if tool.name in requested:
                        all_tools.append(tool)
                        logger.debug(f"Loaded Canvas tool: {tool.name}")
            except Exception as e:
                logger.warning(f"Could not load Canvas tools: {e}")

    # Agent catalog tools (experts + skills)
    if "agent_catalog" in tools_by_category:
        try:
            catalog_tools = create_catalog_tools(context)
            requested = set(tools_by_category["agent_catalog"])
            for tool in catalog_tools:
                if tool.name in requested:
                    all_tools.append(tool)
                    logger.debug(f"Loaded agent_catalog tool: {tool.name}")
        except Exception as e:
            logger.warning(f"Could not load agent catalog tools: {e}")

    # Workflow tools (automations + project loops)
    if "workflows" in tools_by_category:
        try:
            workflow_tools = create_workflow_tools(context)
            requested = set(tools_by_category["workflows"])
            for tool in workflow_tools:
                if tool.name in requested:
                    all_tools.append(tool)
                    logger.debug(f"Loaded workflows tool: {tool.name}")
        except Exception as e:
            logger.warning(f"Could not load workflow tools: {e}")

    # Catalogue-authoring tools (expert / skill / automation bundle get+set).
    # The only category whose members come from TWO factories: the expert and
    # skill bundles are built by create_catalog_tools, the automation bundles by
    # create_workflow_tools.  They live in one category because they share a
    # gate — the `catalog_authoring` capability grant — not because they share a
    # module.  Constructing a factory is closure creation plus an env read, so
    # calling one again here when a session also asked for its read-only sibling
    # category is cheap; filtering by `requested` keeps each tool bound once.
    if "catalog_authoring" in tools_by_category:
        requested = set(tools_by_category["catalog_authoring"])
        for factory in (create_catalog_tools, create_workflow_tools):
            try:
                for tool in factory(context):
                    if tool.name in requested:
                        all_tools.append(tool)
                        logger.debug(f"Loaded catalog_authoring tool: {tool.name}")
            except Exception as e:
                logger.warning(
                    f"Could not load catalog_authoring tools from "
                    f"{factory.__name__}: {e}"
                )

    # Requested-vs-loaded skew must be visible: a name can pass the registry
    # check above and still not bind because its category factory never
    # produced it (a renamed/retired tool restored from an old checkpoint or
    # chat history, a degraded datasource toolkit, a mode mismatch inside a
    # factory). One aggregated WARNING per load — not one line per name — so
    # post-rename skew shows up in the log instead of silently shrinking the
    # toolset.
    loaded_names = {tool.name for tool in all_tools}
    unloaded = [name for name in dict.fromkeys(tool_names) if name not in loaded_names]
    if unloaded:
        logger.warning(
            "load_tools: %d requested tool(s) did not load (unknown to their "
            "category factories or degraded): %s",
            len(unloaded),
            ", ".join(unloaded),
        )

    logger.info(f"Loaded {len(all_tools)} tools: {[t.name for t in all_tools]}")
    return all_tools


def load_tools_by_category(category: str, context: ToolContext) -> List[Any]:
    """Load all tools in a specific category.

    Args:
        category: Category name (workspace, core, research, citation, graph)
        context: ToolContext with dependencies

    Returns:
        List of LangChain Tool objects

    Example:
        ```python
        tools = load_tools_by_category("workspace", context)
        ```
    """
    tool_names = get_tools_by_category(category)
    # Filter out placeholder tools
    tool_names = [
        name for name in tool_names if not TOOL_REGISTRY[name].get("placeholder", False)
    ]
    return load_tools(tool_names, context)


def register_tool(
    name: str,
    module: str,
    function: str,
    description: str,
    category: str = "custom",
    **kwargs,
) -> None:
    """Register a custom tool in the registry.

    Use this to add tools that aren't part of the standard tool set.

    Args:
        name: Unique tool name
        module: Module containing the tool
        function: Function name in the module
        description: Tool description
        category: Tool category
        **kwargs: Additional metadata
    """
    if name in TOOL_REGISTRY:
        logger.warning(f"Overwriting existing tool registration: {name}")

    TOOL_REGISTRY[name] = {
        "module": module,
        "function": function,
        "description": description,
        "category": category,
        **kwargs,
    }
    logger.info(f"Registered tool: {name} ({category})")


def unregister_tool(name: str) -> bool:
    """Remove a tool from the registry.

    Args:
        name: Tool name to remove

    Returns:
        True if tool was removed, False if not found
    """
    if name in TOOL_REGISTRY:
        del TOOL_REGISTRY[name]
        logger.info(f"Unregistered tool: {name}")
        return True
    return False


def apply_instruction_enforcement(
    tools: List[Any],
    context: ToolContext,
) -> List[Any]:
    """Apply instruction file enforcement wrappers to tools.

    For each tool that has a ``before_tool`` trigger with ``enforce=True``,
    wraps the tool to evaluate the binding's current read contract before
    execution. The contract may be job-scoped or phase/freshness-scoped.

    This generalizes the pattern from todo.py's hardcoded todo_guide.md
    check into a config-driven system.

    Args:
        tools: List of loaded LangChain tool objects
        context: ToolContext with instruction_files configured

    Returns:
        Same list of tools (modified in-place with enforcement wrappers)
    """
    if not context._instruction_files:
        return tools

    # Build the tool-name set once. The binding entries themselves remain in
    # ToolContext because phase filters and read freshness are evaluated at
    # invocation time, not when tools are loaded.
    enforced_tool_names: set[str] = set()
    for entry in context._instruction_files:
        if entry.trigger_type == "before_tool" and entry.enforce:
            enforced_tool_names.add(entry.trigger_target)

    if not enforced_tool_names:
        return tools

    def _enforcement_block(tool_name):
        """Return a nudge string if a required instruction file is still unread
        (gate closed), else None."""
        return context.check_tool_enforcement(tool_name)

    def _make_sync_wrapper(orig, tool_name):
        @functools.wraps(orig)
        def wrapper(*args, **kwargs):
            blocked = _enforcement_block(tool_name)
            if blocked is not None:
                return blocked
            return orig(*args, **kwargs)

        return wrapper

    def _make_async_wrapper(orig, tool_name):
        @functools.wraps(orig)
        async def wrapper(*args, **kwargs):
            blocked = _enforcement_block(tool_name)
            if blocked is not None:
                return blocked
            return await orig(*args, **kwargs)

        return wrapper

    for tool in tools:
        if tool.name not in enforced_tool_names:
            continue

        tool_name = tool.name

        # Sync tools (e.g. next_phase_todos) expose .func; async @tool functions
        # (e.g. cite_web / cite_document) expose .coroutine and are invoked via
        # .ainvoke(), which bypasses .func. Wrap whichever the tool actually has —
        # wrapping only .func makes enforcement a silent no-op for async tools.
        wrapped = False
        if getattr(tool, "func", None) is not None:
            tool.func = _make_sync_wrapper(tool.func, tool_name)
            wrapped = True
        if getattr(tool, "coroutine", None) is not None:
            tool.coroutine = _make_async_wrapper(tool.coroutine, tool_name)
            wrapped = True

        if wrapped:
            required_files = [
                entry.path
                for entry in context._instruction_files
                if entry.trigger_type == "before_tool"
                and entry.enforce
                and entry.trigger_target == tool_name
            ]
            logger.debug(
                f"Applied instruction enforcement to {tool_name}: "
                f"requires {required_files}"
            )

    wrapped_count = sum(1 for t in tools if t.name in enforced_tool_names)
    if wrapped_count:
        logger.info(f"Applied instruction enforcement to {wrapped_count} tools")

    return tools
