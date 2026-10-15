"""Tool description manager for the Universal Agent.

Handles:
1. Extracting docstrings from tool objects (no hardcoding)
2. Rendering tool markdown (README index + per-tool docs), served live from
   ``tools/`` as a virtual directory (knowledge-base/knowledge/features/virtual_directories.md)
3. Applying runtime description overrides for deferred tools
"""

import logging
from typing import Any, Dict, Iterable, List, Optional

from agent.tools.registry import (
    PHASE_DESCRIPTION_PREFIXES,
    TOOL_REGISTRY,
    single_phase_of,
)

logger = logging.getLogger(__name__)


class DescriptionManager:
    """Unified manager for tool descriptions.

    Extracts docstrings dynamically from LangChain tool objects instead of
    maintaining hardcoded docstrings. Tool descriptions are cached after
    extraction for efficient reuse.
    """

    def __init__(self):
        self._docstring_cache: Dict[str, str] = {}

    def extract_docstrings(self, tools: List[Any]) -> None:
        """Extract and cache docstrings from tool objects.

        LangChain's @tool decorator stores docstrings in tool.description.
        We extract them here for documentation generation.

        Args:
            tools: List of LangChain tool objects
        """
        for tool in tools:
            if hasattr(tool, "name") and hasattr(tool, "description"):
                self._docstring_cache[tool.name] = tool.description

    def get_docstring(self, tool_name: str) -> Optional[str]:
        """Get docstring from cache or fall back to registry.

        Args:
            tool_name: Name of the tool

        Returns:
            The tool's docstring, or None if not found
        """
        if tool_name in self._docstring_cache:
            return self._docstring_cache[tool_name]
        # Fallback to registry description
        if tool_name in TOOL_REGISTRY:
            return TOOL_REGISTRY[tool_name].get("description")
        return None

    def generate_tool_description(self, tool_name: str) -> str:
        """Generate markdown description for a single tool.

        Args:
            tool_name: Name of the tool

        Returns:
            Markdown-formatted tool description
        """
        if tool_name not in TOOL_REGISTRY:
            return f"# {tool_name}\n\n*Tool not found in registry.*\n"

        metadata = TOOL_REGISTRY[tool_name]
        docstring = self.get_docstring(tool_name)

        lines = [
            f"# {tool_name}",
            "",
            f"**Category:** {metadata.get('category', 'unknown')}",
            "",
        ]

        if docstring:
            lines.append(docstring)
        else:
            lines.append(metadata.get("description", "No description available."))

        lines.append("")

        return "\n".join(lines)

    def generate_tool_index(self, tool_names: List[str]) -> str:
        """Generate an index markdown file listing all available tools.

        Args:
            tool_names: List of tool names to include

        Returns:
            Markdown-formatted index
        """
        # Group tools by category
        categories: Dict[str, List[str]] = {}

        for name in tool_names:
            if name in TOOL_REGISTRY:
                category = TOOL_REGISTRY[name].get("category", "other")
            else:
                category = "other"

            if category not in categories:
                categories[category] = []
            categories[category].append(name)

        # Build index
        lines = [
            "# Available Tools",
            "",
            "This directory contains detailed documentation for all tools available to this agent.",
            "Read a tool's documentation file to understand how to use it.",
            "",
            "## Quick Reference",
            "",
        ]

        # Category order (matches src/tools/ packages)
        category_order = [
            "workspace",
            "core",
            "research",
            "citation",
            "graph",
            "sql",
            "mongodb",
            "knowledge",
            "git",
            "shell",
            "evaluation",
            "communication",
            "other",
        ]

        for category in category_order:
            if category not in categories:
                continue

            tools = sorted(categories[category])
            category_title = category.replace("_", " ").title()

            lines.append(f"### {category_title} Tools")
            lines.append("")

            for tool_name in tools:
                desc = TOOL_REGISTRY.get(tool_name, {}).get("description", "")
                lines.append(f"- **[{tool_name}]({tool_name}.md)** - {desc}")

            lines.append("")

        lines.append("---")
        lines.append("")
        lines.append(
            "*To use a tool, read its documentation file for detailed arguments and examples.*"
        )

        return "\n".join(lines)

    def apply_overrides(self, tools: List[Any]) -> List[Any]:
        """Apply short descriptions to deferred tools.

        For tools with defer_to_workspace=True in registry metadata,
        replace the full docstring description with the short_description.

        Args:
            tools: List of LangChain tool objects

        Returns:
            Modified list of tools with shortened descriptions for deferred tools
        """
        modified_tools = []
        deferred_count = 0

        for tool in tools:
            tool_name = tool.name
            metadata = TOOL_REGISTRY.get(tool_name, {})

            if metadata.get("defer_to_workspace", False):
                short_desc = metadata.get("short_description")
                if short_desc:
                    modified_tool = self._copy_with_description(tool, short_desc)
                    modified_tools.append(modified_tool)
                    deferred_count += 1
                    logger.debug(
                        f"Tool '{tool_name}' using short description (deferred)"
                    )
                else:
                    modified_tools.append(tool)
                    logger.warning(
                        f"Tool '{tool_name}' marked as deferred but has no short_description"
                    )
            else:
                modified_tools.append(tool)

        if deferred_count > 0:
            logger.info(
                f"Applied description overrides: {deferred_count} tools deferred, "
                f"{len(tools) - deferred_count} tools with full descriptions"
            )

        return modified_tools

    def _copy_with_description(self, tool: Any, new_description: str) -> Any:
        """Create a copy of a tool with a modified description.

        LangChain's @tool decorator returns StructuredTool which is a Pydantic model.
        We use model_copy() to create a modified copy with the new description.

        Args:
            tool: Original LangChain tool object
            new_description: New description to use

        Returns:
            New tool object with modified description
        """
        try:
            # StructuredTool is a Pydantic v2 model - use model_copy()
            return tool.model_copy(update={"description": new_description})
        except AttributeError:
            # Fallback for older Pydantic or different tool types
            try:
                return tool.copy(update={"description": new_description})
            except AttributeError:
                # Last resort: try direct attribute modification on a copy
                logger.warning(f"Could not copy tool '{tool.name}', modifying in place")
                tool.description = new_description
                return tool


# Singleton for convenience
_manager: Optional[DescriptionManager] = None


def _get_manager() -> DescriptionManager:
    """Get or create the singleton DescriptionManager."""
    global _manager
    if _manager is None:
        _manager = DescriptionManager()
    return _manager


# === Backward-compatible module functions ===


def generate_tool_description(tool_name: str) -> str:
    """Generate a markdown description for a single tool.

    Args:
        tool_name: Name of the tool

    Returns:
        Markdown-formatted tool description
    """
    manager = _get_manager()
    return manager.generate_tool_description(tool_name)


def generate_tool_index(tool_names: List[str]) -> str:
    """Generate an index markdown file listing all available tools.

    Args:
        tool_names: List of tool names to include

    Returns:
        Markdown-formatted index
    """
    manager = _get_manager()
    return manager.generate_tool_index(tool_names)


def apply_description_overrides(tools: List[Any]) -> List[Any]:
    """Apply description overrides for deferred tools.

    For tools with defer_to_workspace=True in registry metadata,
    replace the full docstring description with the short_description.

    Args:
        tools: List of LangChain tool objects from load_tools()

    Returns:
        Modified list of tools with shortened descriptions for deferred tools
    """
    manager = _get_manager()
    manager.extract_docstrings(tools)  # Cache docstrings first
    return manager.apply_overrides(tools)


def apply_phase_description_prefixes(
    tools: List[Any], both_phases: Optional[Iterable[str]] = None
) -> List[Any]:
    """Prefix single-phase tools' descriptions with their phase (U2).

    With one tool binding for every phase the schema no longer says which
    phase a tool belongs to, so ``[strategic-phase tool] `` /
    ``[tactical-phase tool] `` (``PHASE_DESCRIPTION_PREFIXES``, from the
    registry's ``phases`` metadata) is put in front of the description at
    bind time. Both-phase and unregistered tools are returned as they are; a
    description that already carries its prefix is left alone, so the call is
    idempotent after ``apply_description_overrides`` and the guardrail
    Examples injection. Returns copies — the loaded tool objects (the
    ToolNode's, and the full-description originals behind ``tools/<name>.md``)
    are never mutated.
    """
    manager = _get_manager()
    unrestricted = set(both_phases or ())
    out: List[Any] = []
    for tool in tools:
        name = getattr(tool, "name", "") or ""
        # ``both_phases``: tools this expert may call in either phase
        # (``phase_settings.strategic_tools``) carry no phase prefix.
        phase = None if name in unrestricted else single_phase_of(name)
        description = getattr(tool, "description", None)
        if phase is None or not isinstance(description, str):
            out.append(tool)
            continue
        prefix = PHASE_DESCRIPTION_PREFIXES[phase]
        if description.startswith(prefix):
            out.append(tool)
            continue
        out.append(manager._copy_with_description(tool, prefix + description))
    return out


def get_deferred_tools() -> List[str]:
    """Get list of tool names that are deferred to workspace.

    Returns:
        List of tool names with defer_to_workspace=True
    """
    return [
        name
        for name, meta in TOOL_REGISTRY.items()
        if meta.get("defer_to_workspace", False)
    ]


def get_core_tools() -> List[str]:
    """Get list of tool names that keep full descriptions.

    Returns:
        List of tool names with defer_to_workspace=False or not set
    """
    return [
        name
        for name, meta in TOOL_REGISTRY.items()
        if not meta.get("defer_to_workspace", False)
    ]
