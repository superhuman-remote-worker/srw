"""Skill tools for the Universal Agent (Agent Skills, Slice 2).

``use_skill`` loads a skill's SKILL.md body (Level 2) from the workspace. Skill
directories are materialized at job start under skills/<name>/ by
_deploy_instruction_files. The L1 menu (name + description) is already in the
system prompt; this tool brings the body into context on demand. References
(skills/<name>/references/) are read with read_file; scripts are executed with
run_command on a shell-capable tier. On a lite (virtual) tier use_skill notes
that the scripts need a workspace upgrade first (Slice 4).

Design: knowledge-base/knowledge/features/agent_skills.md (Slice 2).
"""

import logging
from typing import Any, List

from langchain_core.tools import tool

from agent.tools.context import ToolContext

from shared.tool_catalog.definitions import (
    SKILL_TOOLS_METADATA as SKILL_TOOLS_METADATA,
)

logger = logging.getLogger(__name__)


def create_skill_tools(context: ToolContext) -> List[Any]:
    """Create skill tools with injected context."""
    if not context.has_workspace():
        raise ValueError("ToolContext must have a workspace_manager for skill tools")

    workspace = context.workspace_manager

    def _script_availability_note(skill_name: str) -> str:
        """When a skill bundles scripts but this tier has no shell, tell the
        agent the scripts can't run here and how to unlock them (Slice 4).

        A skill is script-bearing iff it has a ``scripts/`` directory — a single
        dir-aware ``exists`` probe. The note fires only on a shell-less tier
        (``virtual``); on a sandbox/vm the agent just runs the scripts via
        run_command, so no note. Detection must never break use_skill.
        """
        try:
            if getattr(workspace.backend, "supports_shell", True):
                return ""  # has a shell → agent runs scripts directly
            if not workspace.exists(f"skills/{skill_name}/scripts"):
                return ""  # prompt-only skill → nothing to gate
        except Exception:
            return ""
        return (
            "\n\n---\n"
            "[scripts need a workspace] This skill bundles runnable scripts under "
            f"`skills/{skill_name}/scripts/`, but you are on a virtual filesystem "
            "with no shell, so they cannot be executed here. The guidance above "
            "still applies without them. To ask for the scripts to be unlocked, "
            "call `request_workspace_upgrade(reason=...)`: a human decides, and "
            "if they approve, a sandbox with a shell is provisioned and your "
            "files carry over. Don't count on being resumed afterwards — ask, "
            "then do what you can without the scripts."
        )

    @tool
    def use_skill(skill_name: str) -> str:
        """Load a skill's guidance (its SKILL.md body) into your context.

        Skills are reusable "how to do X well" procedures listed in your system
        prompt under available_skills. Call this when a listed skill matches the
        task at hand; the body will appear in your context and walk you through
        the procedure. Skills bound as mandatory instruction gates (for example
        verify-before-done) are intentionally absent from that optional menu —
        they ride the frozen instructions channel to skills/<name>/SKILL.md and
        are equally loadable here or with read_file at that path. If the skill
        bundles references/ files, read them with read_file as the body directs.

        Args:
            skill_name: The skill's name exactly as shown in the available_skills
                menu, or a gate-required bound skill name (e.g.
                "verify-before-done").

        Returns:
            The SKILL.md body, or a friendly message if the skill is not present.
        """
        scoped_skills = context.config.get("_resolved_skills") or {}
        menu = scoped_skills.get("menu") if isinstance(scoped_skills, dict) else None
        entry = next(
            (
                item
                for item in menu or []
                if isinstance(item, dict) and item.get("name") == skill_name
            ),
            None,
        )
        if entry is None:
            # Bound instruction skills are deliberately absent from the optional
            # menu (filter_bound_skills) and travel via the frozen instructions
            # channel instead. Serving the currently bound names here keeps the
            # advertised loading route satisfiable: the gate nudge directs the
            # model to read_file at the same path, and both record the same
            # versioned read. Anything without a menu entry AND without a live
            # binding stays refused — workspace bytes alone grant nothing.
            bound_names = {
                getattr(item, "skill", None)
                for item in (getattr(context, "_instruction_files", None) or [])
            }
            if skill_name not in bound_names:
                return (
                    f"Skill '{skill_name}' is not available for the current session "
                    "capabilities. Use only skills listed in the current "
                    "available_skills menu, by their exact name."
                )
            if skill_name == "app-guide":
                return (
                    "Skill 'app-guide' is managed by the running SRW product and is "
                    "not loaded from mutable workspace files. Call "
                    "read_product_guide(topic_id='index'), then read the relevant "
                    "logical topic ID it returns."
                )
            skill_md = f"skills/{skill_name}/SKILL.md"
            try:
                if not workspace.exists(skill_md):
                    return (
                        f"Skill '{skill_name}' is required by an instruction gate "
                        f"but was not found in this workspace at '{skill_md}'. It "
                        "is delivered via the instructions channel, not the "
                        "available_skills menu — read it with "
                        f"read_file('{skill_md}') once deployed. If it stays "
                        "missing, report blocked rather than retrying."
                    )
                body = workspace.read_file(skill_md)
                context.record_file_read(skill_md, body)
                return f"[skill: {skill_name}]\n\n{body}{_script_availability_note(skill_name)}"
            except Exception as e:  # never raise to the model
                logger.warning("use_skill(%s) failed: %s", skill_name, e)
                return f"Error loading skill '{skill_name}': {e}"
        if (
            entry.get("system_managed") is True
            and entry.get("loader_tool") == "read_product_guide"
            and skill_name == "app-guide"
        ):
            return (
                "Skill 'app-guide' is managed by the running SRW product and is "
                "not loaded from mutable workspace files. Call "
                "read_product_guide(topic_id='index'), then read the relevant "
                "logical topic ID it returns."
            )

        skill_md = f"skills/{skill_name}/SKILL.md"
        try:
            if not workspace.exists(skill_md):
                return (
                    f"Skill '{skill_name}' not found in this workspace. "
                    f"Use only skills listed in the available_skills menu, by their "
                    f"exact name."
                )
            body = workspace.read_file(skill_md)
            context.record_file_read(skill_md, body)
            return f"[skill: {skill_name}]\n\n{body}{_script_availability_note(skill_name)}"
        except Exception as e:  # never raise to the model
            logger.warning("use_skill(%s) failed: %s", skill_name, e)
            return f"Error loading skill '{skill_name}': {e}"

    return [use_skill]
