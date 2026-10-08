"""The workspace-facts block in README.md, for job agents and sessions alike.

Connector delivery itself lives in ``agent.connectors``: materializers keyed
by delivery form, run in each entry point's order by its registry. What is
left here is the README: one marker-delimited block the agent rewrites at
every worker start, session attach and live attach/detach, listing the
attached connectors (each materializer describes its own), the input
materials and the layout.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

from agent.connectors.base import RuntimeContext
from agent.connectors.legacy import deliveries_from_payload
from agent.connectors.registry import connector_registry
from shared.datasource_policy import (
    EMAIL_TIER_ORDER as EMAIL_TIER_ORDER,
    EMAIL_TIER_TOOLS as EMAIL_TIER_TOOLS,
    DATASOURCE_TOOL_MAP as DATASOURCE_TOOL_MAP,
    email_effective_access as email_effective_access,
    datasource_tool_categories as datasource_tool_categories,
    resolve_repo_clone_names as resolve_repo_clone_names,
)

logger = logging.getLogger(__name__)


def _render_connector_lines(
    ds_configs: List[Dict[str, Any]],
    workspace_manager: Any,
    ssh_identity_status: Optional[Dict[str, str]] = None,
) -> List[str]:
    """Each attached connector's line, grouped by section."""
    rt = RuntimeContext(
        execution="session",
        workspace_manager=workspace_manager,
        ssh_identity_status=ssh_identity_status,
    )
    return connector_registry().facts(deliveries_from_payload(ds_configs), rt)


WORKSPACE_FACTS_START = "<!-- srw:workspace-facts:start -->"
WORKSPACE_FACTS_END = "<!-- srw:workspace-facts:end -->"
# Gitea auto-inits per-job repos with a stub README of the form
# "# job-xxxxxxxx\n\nWorkspace for job-xxxxxxxx"; anything else without our
# markers is a real README (a human's, on a shared project repo) that is only
# ever appended to.
# Gitea auto-init READMEs for per-job repos: the legacy form
# "# job-xxxxxxxx\n\nWorkspace for job-xxxxxxxx" and the current
# `_repository_intent_description` form "SRW managed repository; creation-intent=<uuid>"
# (orchestrator/services/gitea.py). Both are stubs to replace, never a human README.
_GITEA_STUB_README_MARKERS = (
    "Workspace for job-",
    "SRW managed repository; creation-intent=",
)
MATERIALS_LIST_CAP = 30
# Bounds on the documents/ walk so a pathological upload cannot turn the
# facts block into thousands of SFTP round trips.
_MATERIALS_MAX_DEPTH = 8
_MATERIALS_MAX_ENTRIES = 2000


def _list_materials(workspace_manager: Any) -> List[str]:
    """Workspace-relative paths of the files under ``documents/``, sorted.

    Walks through the workspace backend (the workspace is remote), bounded in
    depth and entry count. Dotfiles are skipped. Any listing failure yields
    an empty list — the facts block is advisory and never fatal.
    """
    list_files = getattr(workspace_manager, "list_files", None)
    if not callable(list_files):
        return []
    files: List[str] = []
    pending: List[Tuple[str, int]] = [("documents", 0)]
    seen = 0
    try:
        while pending and seen < _MATERIALS_MAX_ENTRIES:
            directory, depth = pending.pop()
            entries = list_files(directory)
            if not isinstance(entries, (list, tuple)):
                return []
            for entry in entries:
                seen += 1
                if seen > _MATERIALS_MAX_ENTRIES:
                    break
                rel = str(entry)
                base = rel.rstrip("/").rsplit("/", 1)[-1]
                if not base or base.startswith("."):
                    continue
                if rel.endswith("/"):
                    if depth < _MATERIALS_MAX_DEPTH:
                        pending.append((rel.rstrip("/"), depth + 1))
                    continue
                files.append(rel)
    except Exception as e:
        logger.warning("Could not list documents/ for the workspace facts: %s", e)
        return []
    return sorted(files)


def _has_dir(workspace_manager: Any, relative_path: str) -> bool:
    exists = getattr(workspace_manager, "exists", None)
    if not callable(exists):
        return False
    try:
        return bool(exists(relative_path))
    except Exception:
        return False


def render_workspace_facts(
    ds_configs: List[Dict[str, Any]],
    workspace_manager: Any,
    *,
    project_name: Optional[str] = None,
    expert: Optional[str] = None,
    ssh_identity_status: Optional[Dict[str, str]] = None,
) -> str:
    """Render the marker-delimited workspace-facts block for README.md.

    FACTS ONLY: what this workspace is, which connectors are attached and
    where they live, what input materials were provided, and the layout.
    Never the job id, description, kickoff, or todos — those stay in the
    virtual ``task_brief.md``. Subjobs share their parent's workspace and
    connector set, so the block is safe on shared workspaces.
    """
    lines: List[str] = [WORKSPACE_FACTS_START]

    facts: List[str] = []
    if isinstance(project_name, str) and project_name.strip():
        facts.append(f"- **Project**: {project_name.strip()}")
    if isinstance(expert, str) and expert.strip():
        facts.append(f"- **Expert**: {expert.strip()}")
    if facts:
        lines += ["## Workspace", "", *facts, ""]

    lines += ["## Connectors", ""]
    lines += _render_connector_lines(
        list(ds_configs or []), workspace_manager, ssh_identity_status
    )

    lines += ["## Materials", ""]
    materials = _list_materials(workspace_manager)
    if materials:
        lines += [f"- `{path}`" for path in materials[:MATERIALS_LIST_CAP]]
        if len(materials) > MATERIALS_LIST_CAP:
            lines.append(f"… and {len(materials) - MATERIALS_LIST_CAP} more")
    else:
        lines.append("_No input documents._")
    lines.append("")

    lines += ["## Layout", "", "- `output/` — deliverables"]
    if _has_dir(workspace_manager, "notes"):
        lines.append("- `notes/` — working notes")
    lines += [
        "- `tools/` — tool documentation (virtual)",
        "- `skills/` — skills available via use_skill",
        "- `archive/` — completed phases",
        WORKSPACE_FACTS_END,
    ]
    return "\n".join(lines)


def merge_workspace_facts(existing: Optional[str], block: str) -> str:
    """Merge the facts block into an existing README, or create one.

    1. README absent/empty → ``# Workspace`` + block.
    2. README with markers → replace exactly the marked span; everything
       else stays byte-identical.
    3. README without markers that is the Gitea stub → replaced as in (1).
    4. Any other README (a human's, on a shared project repo) → the block
       is appended; the existing text is never modified.
    """
    fresh = f"# Workspace\n\n{block}\n"
    if existing is None or not existing.strip():
        return fresh
    start = existing.find(WORKSPACE_FACTS_START)
    end = existing.find(WORKSPACE_FACTS_END)
    if start != -1 and end != -1 and end > start:
        return existing[:start] + block + existing[end + len(WORKSPACE_FACTS_END) :]
    if start != -1:
        # Start marker without an end marker: the block ran to EOF.
        return existing[:start] + block + "\n"
    stripped = existing.strip()
    if (
        stripped.startswith("# job-")
        and len(stripped) < 300
        and any(marker in stripped for marker in _GITEA_STUB_README_MARKERS)
    ):
        return fresh
    return existing.rstrip("\n") + "\n\n" + block + "\n"


def inject_workspace_facts(
    ds_configs: List[Dict[str, Any]],
    workspace_manager: Any,
    *,
    project_name: Optional[str] = None,
    expert: Optional[str] = None,
    ssh_identity_status: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    """Write the workspace-facts block into the workspace's README.md.

    One on-disk file orients both the agent and a human opening the job
    repo: the system prompts point the agent at README.md for connector
    names and clone paths. Regenerated at every agent init and on every
    live attach/detach, so the block always reflects the current connector
    set (including the explicit "no connectors" state after a remove-all).

    ``ssh_identity_status`` is the SSH identity materializer's
    ``{authority_id: status}``: an SSH connector whose key did not load is
    listed as not available rather than advertised.

    Non-fatal: a failure logs a warning. Returns the README content that was
    written, or None when nothing was written.
    """
    try:
        try:
            existing: Optional[str] = workspace_manager.read_file("README.md")
        except (FileNotFoundError, ValueError, OSError):
            existing = None
        if not isinstance(existing, str):
            existing = None
        block = render_workspace_facts(
            ds_configs,
            workspace_manager,
            project_name=project_name,
            expert=expert,
            ssh_identity_status=ssh_identity_status,
        )
        content = merge_workspace_facts(existing, block)
        workspace_manager.write_file("README.md", content)
        logger.info(
            "Wrote workspace facts (%d connectors) into README.md",
            len(ds_configs or []),
        )
        return content
    except Exception as e:
        logger.warning("Failed to write workspace facts into README.md: %s", e)
        return None
