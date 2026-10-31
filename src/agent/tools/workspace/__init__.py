"""Workspace toolkit - file and filesystem operations.

This toolkit is always loaded as it provides essential file operations
for the agent's workspace-centric architecture.

Tools are split into two modules:
- files.py: Core file operations (read, write, edit)
- filesystem.py: Filesystem operations (list, delete, move, copy, search, etc.)

skills.py (``use_skill``) lives here because it reads workspace files, but it is
its own ``skills`` category and is loaded by the registry, not by this factory.
"""

from typing import Any, Dict, List

from agent.tools.context import ToolContext


def create_workspace_tools(context: ToolContext) -> List[Any]:
    """Create all workspace tools with injected context.

    Args:
        context: ToolContext with workspace_manager

    Returns:
        List of LangChain tool functions

    Raises:
        ValueError: If context doesn't have a workspace_manager
    """
    if not context.has_workspace():
        raise ValueError(
            "ToolContext must have a workspace_manager for workspace tools"
        )

    from agent.tools.workspace.files import create_file_tools
    from agent.tools.workspace.filesystem import create_filesystem_tools

    tools = []
    tools.extend(create_file_tools(context))
    tools.extend(create_filesystem_tools(context))

    return tools


def get_workspace_metadata() -> Dict[str, Dict[str, Any]]:
    """Get metadata for all workspace tools."""
    from agent.tools.workspace.files import FILE_TOOLS_METADATA
    from agent.tools.workspace.filesystem import FILESYSTEM_TOOLS_METADATA

    return {**FILE_TOOLS_METADATA, **FILESYSTEM_TOOLS_METADATA}


# Re-export metadata for backwards compatibility
def _get_combined_metadata() -> Dict[str, Dict[str, Any]]:
    """Internal: get combined metadata (called at module level for registry)."""
    from agent.tools.workspace.files import FILE_TOOLS_METADATA
    from agent.tools.workspace.filesystem import FILESYSTEM_TOOLS_METADATA

    return {**FILE_TOOLS_METADATA, **FILESYSTEM_TOOLS_METADATA}
