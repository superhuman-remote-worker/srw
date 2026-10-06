"""Tool metadata shared by the agent runtime and its control plane.

This module loads no tool factories or agent framework. ``TOOL_REGISTRY`` is the
one mutable catalog per process: runtime registrations update this same object,
so policy/report readers observe discovered MCP tools without importing their
execution layer. Registration and tool construction remain agent-owned.
"""

from typing import Any, Dict, List

from shared.tool_catalog import definitions as _definitions


TOOL_REGISTRY: Dict[str, Dict[str, Any]] = {}

# Preserve the existing category/declaration merge order and metadata objects.
# Dynamic MCP entries are added only by the agent runtime after discovery.
for _metadata_group in (
    _definitions.FILE_TOOLS_METADATA,
    _definitions.FILESYSTEM_TOOLS_METADATA,
    _definitions.SKILL_TOOLS_METADATA,
    _definitions.TODO_TOOLS_METADATA,
    _definitions.JOB_TOOLS_METADATA,
    _definitions.WORKSPACE_UPGRADE_TOOLS_METADATA,
    _definitions.OFFICER_TOOLS_METADATA,
    _definitions.RESEARCH_TOOLS_METADATA,
    _definitions.PAPER_TOOLS_METADATA,
    _definitions.RESEARCH_WORKFLOW_TOOLS_METADATA,
    _definitions.BROWSER_DIRECT_TOOLS_METADATA,
    _definitions.CITATION_TOOLS_METADATA,
    _definitions.CANVAS_TOOLS_METADATA,
    _definitions.GRAPH_TOOLS_METADATA,
    _definitions.SQL_TOOLS_METADATA,
    _definitions.MONGODB_TOOLS_METADATA,
    _definitions.WEBDAV_TOOLS_METADATA,
    _definitions.REPO_TOOLS_METADATA,
    _definitions.EMAIL_TOOLS_METADATA,
    _definitions.GIT_TOOLS_METADATA,
    _definitions.CODING_TOOLS_METADATA,
    _definitions.SHELL_TOOLS_METADATA,
    _definitions.EVALUATION_TOOLS_METADATA,
    _definitions.KNOWLEDGE_TOOLS_METADATA,
    _definitions.MEMORY_TOOLS_METADATA,
    _definitions.COMMUNICATION_TOOLS_METADATA,
    _definitions.ORCHESTRATOR_TOOLS_METADATA,
    _definitions.PROJECT_TOOLS_METADATA,
    _definitions.REPOSITORY_TOOLS_METADATA,
    _definitions.CATALOG_TOOLS_METADATA,
    _definitions.WORKFLOW_TOOLS_METADATA,
    _definitions.PRODUCT_HELP_TOOLS_METADATA,
    _definitions.PRODUCT_CAPABILITY_TOOLS_METADATA,
    _definitions.LOOP_PLAN_TOOLS_METADATA,
    _definitions.SESSION_TASK_METADATA,
    _definitions.DELEGATE_AGENT_METADATA,
    _definitions.CONTROL_PLANE_METADATA,
):
    TOOL_REGISTRY.update(_metadata_group)
del _metadata_group


# ---------------------------------------------------------------------------
# Grant classification
# ---------------------------------------------------------------------------
# Two optional metadata keys answer one question: *who is allowed to put this
# tool's name into the list passed to* ``load_tools``? Policy expansion and
# toolset reporting read these classifications; tool execution remains gated
# at its existing runtime seams. See
# knowledge-base/knowledge/features/tool_config_policy_vs_membership.md.
#
# ``grant``
#   absent       Config may grant it, including through a category-level
#                ``true`` / ``except`` policy. This is the default.
#   ``"code"``   Runtime code binds it *instead of* config.  No shipped config
#                should name one, and a category-level ``true`` must not
#                expand to it — otherwise ``core: true`` and ``shell: true``
#                would read as granting tools whose real switch is somewhere
#                else entirely (``officer.enabled``, ``cloud_mount.active``, an
#                attached datasource).  Excluding them is what makes ``true``
#                behaviour-preserving for ``core`` and ``shell``.
#   ``"explicit"``
#                Config may grant it, but only by writing its name.  A
#                category-level ``true`` / ``except`` must not reach it.
#
#                This mark used to carry the safety judgement for the six
#                ``*_bundle`` control-plane writes, which sat inside
#                ``agent_catalog`` / ``workflows`` yet were absent from
#                ``SESSION_TOOL_OVERRIDE_NAMES``.  On 2026-08-03 they moved to
#                their own ``catalog_authoring`` category behind a capability
#                grant, so those two groups now contain only reads and their
#                ``true`` expansion equals the session vocabulary *by
#                construction* — no mark required.  Prefer that fix: a category
#                whose name matches its blast radius needs no exception list.
#                What remains marked is the residue where a category genuinely
#                mixes tiers (``delegate_agent``, ``steer_job``).
#
# ``gate``
#   A short string, present on every classified entry: what actually decides
#   whether the tool gets bound.  For ``"code"`` that is the runtime fact or
#   config key that controls the injection; for ``"explicit"`` it is the naming
#   requirement and the reason for it.  Without this field the rule is folklore
#   — see the design doc's "code floors" note.
#
# The expansion contract a consumer must implement:
#
#     expand(True, cat)            -> [n for n in get_tools_by_category(cat)
#                                      if "grant" not in TOOL_REGISTRY[n]]
#     expand({"except": xs}, cat)  -> expand(True, cat) minus xs
#     expand({"only": xs}, cat)    -> xs as written (an explicit name is an
#     expand([...], cat)              explicit name; ``"code"`` entries stay
#                                     nameable so nothing that works today
#                                     stops working)
#
# Deliberately NOT classified, and why:
#   * the 26 legacy experts-off shim names appended at
#     ``src/api/persistent_session.py:1470-1520`` — the runtime re-adds those
#     canonical ``orchestrator`` / ``agent_catalog`` / ``workflows`` lists only
#     when no disable marker is present.  On the resolved path config still
#     decides, so marking them would make those groups permanently
#     un-enableable: the current bug, re-introduced by its own fix.
#   * ``approve_job_verdict`` / ``return_job_with_feedback`` (stamped as
#     ``tools.evaluation`` by ``_critic_config_override``) and ``loop_plan``
#     (stamped as ``tools.loop`` by the planner loops).  Those are code writing
#     a *config fragment*, which is a config grant; ``evaluation: true`` and
#     ``loop: true`` must keep resolving to them.
#   * ``mcp``.  ``ToolsConfig`` has the field, the registry has no static
#     members, and ``register_mcp_tools`` populates the category per
#     job/session at runtime.  ``mcp: true`` normalises to the existing ``"*"``
#     sentinel rather than expanding against the registry, so there is nothing
#     here to mark.

#: Categories whose every tool is bound by runtime code rather than by a
#: config's tool list.  Expressed per category because that is the truth: a new
#: tool added to any of these is code-granted by construction.  Per-tool
#: ``gate`` strings win over the category default (``setdefault`` below), which
#: is how ``product_help``'s two differently-gated floors stay accurate.
CODE_GRANTED_CATEGORIES: Dict[str, str] = {
    # Datasource-derived.  ``DATASOURCE_TOOL_MAP``
    # (src/core/datasource_setup.py) maps an attached datasource type to a
    # whole category list, and the result is written straight onto
    # ``config.tools.<category>`` at attach/dispatch time.  The bases ship
    # these keys as ``[]`` with a comment saying config does not manage them.
    "graph": "a neo4j datasource is attached",
    "sql": "a postgresql datasource is attached",
    "mongodb": "a mongodb datasource is attached",
    "webdav": "a webdav datasource is attached",
    "repo": "a repository datasource is attached",
    "email": "an email datasource is attached (tier from its config.access)",
    # Persistent-session floors.  Neither category has a ``ToolsConfig`` field,
    # so ``tools.product_help: [...]`` in a YAML file is silently discarded
    # today (src/core/loader.py).  Recording it as a code grant makes that a
    # stated rule instead of an accident of a missing dataclass field.
    "product_help": "persistent-session floor; see each tool's own gate",
    "session_task": "persistent session, unconditional "
    "(src/api/persistent_session.py:1415)",
}


def _classify_code_granted_categories() -> None:
    """Stamp the category-level grant classification onto ``TOOL_REGISTRY``.

    Runs once at import, after every metadata declaration is merged above.  Uses
    ``setdefault`` so a per-tool classification declared next to the tool
    always wins over the category default.
    """
    for category, gate in CODE_GRANTED_CATEGORIES.items():
        for meta in TOOL_REGISTRY.values():
            if meta.get("category") == category:
                meta.setdefault("grant", "code")
                meta.setdefault("gate", gate)


_classify_code_granted_categories()


def get_available_tools() -> Dict[str, Dict[str, Any]]:
    """Get all registered tools with their metadata.

    Returns:
        Dictionary mapping tool names to metadata
    """
    return TOOL_REGISTRY.copy()


def get_tools_by_category(category: str) -> List[str]:
    """Get tool names in a specific category.

    Args:
        category: Category name (workspace, core, research, citation, graph)

    Returns:
        List of tool names in the category
    """
    return [
        name for name, meta in TOOL_REGISTRY.items() if meta.get("category") == category
    ]


def get_categories() -> set[str]:
    """Get all available tool categories.

    Returns:
        Set of category names
    """
    return {meta.get("category", "unknown") for meta in TOOL_REGISTRY.values()}


# Categories requiring a workspace-backed execution environment.
# Runtime capability gating and control-plane reporting share this vocabulary.
_EXECUTION_CATEGORIES = ("shell", "browser_direct", "git")
