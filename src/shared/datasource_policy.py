"""Datasource capability and repository-naming policies shared across apps."""

import logging
import re
from typing import Any, Dict, List, Tuple

from shared.connectors.builtin import EMAIL_SPEC, tool_map, tool_map_entry

logger = logging.getLogger(__name__)

# Cumulative email access tiers (``config.access``): each tier includes every
# tool of the tiers below it (knowledge-base/knowledge/features/email_datasource.md). Order in
# EMAIL_TIER_ORDER is the escalation order used for clamping/maxing. Both come
# from the email driver's access levels.
EMAIL_TIER_ORDER: Tuple[str, ...] = EMAIL_SPEC.ranked_access_ids()
EMAIL_TIER_TOOLS: Dict[str, List[str]] = tool_map_entry(EMAIL_SPEC)["tiers"]

# Datasource type → tool category + read/write tool sets, derived from the
# built-in driver specs (shared.connectors.builtin). Single source of truth
# for BOTH trust boundaries: the orchestrator's
# _build_datasource_tool_override (job dispatch, thread create/resume) and the
# agent's session attach path delegate to datasource_tool_categories() below.
# These were previously two hand-maintained copies that disagreed on
# read-write managed connectors (agent: write tools; orchestrator: CLI-only).
# Email is tier-keyed (config.access), not binary read/write; MCP tool names
# are discovered only after the agent connects, and its wildcard is expanded
# against the runtime registry before tool loading.
DATASOURCE_TOOL_MAP: Dict[str, Dict[str, Any]] = tool_map()


def email_effective_access(ds: Dict[str, Any]) -> str:
    """Effective email tier: ``config.access`` (default ``draft``), clamped
    to ``read`` by a read-only project link.

    Unknown values fail closed to ``read``. The clamp never empties
    credentials — email needs a live IMAP login at every tier
    (knowledge-base/knowledge/features/email_datasource.md, Touchpoints).
    """
    if ds.get("project_read_only", False):
        return "read"
    access = (ds.get("config") or {}).get("access", "draft")
    return access if access in EMAIL_TIER_TOOLS else "read"


def datasource_tool_categories(
    datasources: List[Dict[str, Any]],
) -> Dict[str, List[str]]:
    """Map attached datasources to tool-category overrides.

    Semantics:

    - type not attached → category stripped (``[]``) so stale tools from a
      previously attached datasource never survive,
    - ALL datasources of a type read-only → read tools,
    - any read-write → write tools, backed by the real connection
      process_datasources() now creates for read-write connectors too,
    - tier-keyed types (email): highest effective tier across attached
      datasources of the type (the "any read-write → write" analog);
      per-datasource tiers come from ``config.access`` clamped by
      ``project_read_only`` (email_effective_access), and the tool layer's
      per-call tier check is the backup gate.
    - dynamic types (MCP): ``["*"]`` while attached; the agent expands this
      sentinel after runtime discovery. Project read-only does not alter MCP
      tools because the server and its credentials are the access boundary.

    History: read-write managed connectors (postgresql/neo4j/mongodb) used
    to map to ``[]`` — "CLI mode" — which was dead on remote workspace
    backends and left them with no access path at all
    (knowledge-base/knowledge/issues/datasource_cli_mode_dead_on_remote.md, fixed via
    direction 1: connection-backed write tools).
    """
    by_type: Dict[str, List[Dict[str, Any]]] = {}
    for ds in datasources:
        ds_type = ds.get("type")
        if ds_type:
            by_type.setdefault(ds_type, []).append(ds)

    categories: Dict[str, List[str]] = {}
    for ds_type, tool_info in DATASOURCE_TOOL_MAP.items():
        category = tool_info["category"]
        ds_list = by_type.get(ds_type, [])
        if not ds_list:
            categories[category] = []
        elif tool_info.get("dynamic"):
            categories[category] = ["*"]
        elif "tiers" in tool_info:
            tier = max(
                (email_effective_access(ds) for ds in ds_list),
                key=EMAIL_TIER_ORDER.index,
            )
            categories[category] = list(tool_info["tiers"][tier])
        elif all(ds.get("project_read_only", False) for ds in ds_list):
            categories[category] = list(tool_info["read"])
        else:
            categories[category] = list(tool_info["write"])
    return categories


def resolve_repo_clone_names(
    repo_datasources: List[Dict[str, Any]],
) -> List[str]:
    """Return the clone-directory name for each repository datasource, in order.

    The directory under ``repos/`` uses the upstream repo name from the URL
    (falling back to the datasource-label slug), with a numeric suffix when
    two datasources resolve to the same name (e.g. forks of one upstream).
    Shared by clone_repository_datasources() and inject_workspace_facts()
    so the README.md connector list always points at the real clone paths.
    """
    from shared.git_url import repo_name_from_url

    names: List[str] = []
    used: set[str] = set()
    for ds in repo_datasources:
        ds_slug = (
            re.sub(r"[^a-z0-9]+", "-", ds.get("name", "repo").lower()).strip("-")
            or "repo"
        )
        base = repo_name_from_url(ds.get("connection_url", ""), fallback=ds_slug)
        name = base
        suffix = 2
        while name in used:
            name = f"{base}-{suffix}"
            suffix += 1
        if name != base:
            logger.info(
                "Repo name collision for %s; cloning into %s instead", base, name
            )
        used.add(name)
        names.append(name)
    return names
