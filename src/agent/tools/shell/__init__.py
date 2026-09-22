"""Shell toolkit - shell command execution.

This toolkit provides tools in two modes (configured via shell.mode):

Stateless mode (default):
- run_command: Simple command→output execution (hidden persistent tab underneath)
- shell_read: Read more output from scrollback when needed

Persistent mode (opt-in via shell.mode: persistent):
- shell_execute: Full tab management, keystrokes, async commands
- shell_read: Read output from any named terminal tab

Available in both strategic and tactical phases. Requires tmux + ShellManager.
"""

from typing import Any, Dict, Iterable, List, Optional

from agent.tools.context import ToolContext


def create_shell_tools(
    context: ToolContext, requested_names: Optional[Iterable[str]] = None
) -> List[Any]:
    """Create all shell tools with injected context.

    Args:
        context: ToolContext with workspace_manager
        requested_names: The resolved names being bound (they pick the
            shell mode; see ``shell_tools.create_shell_tools``)

    Returns:
        List of LangChain tool functions

    Raises:
        ValueError: If workspace manager not available in context
    """
    from agent.tools.shell.coding_tools import (
        create_coding_tools as _create_coding_tools,
    )

    tools = _create_coding_tools(context)

    # Include shell tools when ShellManager is available
    if context.shell_manager is not None:
        from agent.tools.shell.shell_tools import (
            create_shell_tools as _create_shell_tools,
        )

        tools.extend(_create_shell_tools(context, requested_names))

    return tools


def get_shell_metadata() -> Dict[str, Dict[str, Any]]:
    """Get metadata for all shell tools."""
    from agent.tools.shell.coding_tools import CODING_TOOLS_METADATA
    from agent.tools.shell.shell_tools import SHELL_TOOLS_METADATA

    return {**CODING_TOOLS_METADATA, **SHELL_TOOLS_METADATA}
