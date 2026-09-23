"""Building a child from a RESOLVED roster entry (U3 B.2, B.8, B.9).

The child never reuses the parent's tool objects (their closures hold the
parent ``ToolContext``): it gets fresh objects from ``load_tools`` on a
``copy.copy``'d, re-rooted context — the proven ``reader_env`` pattern —
bound to a model built with ``create_llm(...).bind_tools(...)`` exactly like
a session (guardrails + the ``parallel_tool_calls`` gate; never
``apply_phase_description_prefixes`` — a child has no phases).

Tool names: ``entry allowlist ∩ parent._resolved_tool_names − CONTROL_PLANE``
(by registry category) — the parent's loaded names are the runtime ceiling
(backend capability filter, grants, datasources, MCP); the delegation tool
is denied by name as well (depth 1, D7).

Isolation: ``shared`` = the parent's tree through a copied
``WorkspaceManager`` whose git manager is read-only (the loop's turn-end
auto-commit never touches the parent tree; git read tools keep working);
``worktree`` = ``reader_env.acquire_reader_env`` (own worktree + branch,
commits stay local, push is a no-op).

Write policy (mechanical minimum): ``none`` strips the write tools by name;
``scratch_only`` / ``owned_paths`` wrap them with a path allowlist;
``full`` adds nothing. Plus the single-shared-writer guard: at most one
``shared`` child holding any write tool per parent at a time.

Nothing here imports ``agent.tools.registry`` at module level (import cycle
registry → delegation → subagents → persistent_graph): registry helpers are
imported inside the functions that need them.
"""

from __future__ import annotations

import copy
import dataclasses
import fnmatch
import functools
import hashlib
import inspect
import itertools
import logging
import posixpath
import re
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

from agent.core.backends.subdir import SubdirBackend
from shared.runtime.core.loader import ROSTER_INHERIT_MARKER, get_project_root

from agent.subagents.host import ParentHost

logger = logging.getLogger(__name__)

#: Registry categories a child never gets (B.2). ``job_inspection`` reads are
#: allowed on purpose (a critic's verifier reads its target through them).
CONTROL_PLANE_CATEGORIES = frozenset(
    {
        "delegation",
        "core",
        "communication",
        "loop",
        "evaluation",
        "session_task",
        "canvas",
        "orchestrator",
        "job_control",
        "catalog_authoring",
        "agent_catalog",
        "product_help",
    }
)
#: Denied by name too — a renamed category must never let one through.
DELEGATION_TOOL_NAMES = frozenset({"delegate_agent"})
#: The tools ``write_policy`` governs (B.8; the workspace mutators plus the
#: knowledge-base writers). ``kb_delete`` is listed for the day it exists.
WRITE_TOOLS = frozenset(
    {
        "write_file",
        "edit_file",
        "delete_file",
        "delete_directory",
        "move_file",
        "copy_file",
        "rename_file",
        "create_directory",
        "kb_write",
        "kb_update",
        "kb_delete",
    }
)
#: Argument names that carry a workspace path on the write tools.
PATH_ARGS = ("path", "file_path", "source", "dest", "destination")
WRITE_POLICIES = ("none", "scratch_only", "owned_paths", "full")
ISOLATIONS = ("shared", "worktree")

_WORKTREE_INDEX = itertools.count(1)


class SpawnRefused(ValueError):
    """A spawn the runtime must refuse with a clear message to the parent."""


# ---------------------------------------------------------------------------
# Tool selection
# ---------------------------------------------------------------------------


def entry_tool_names(entry: Mapping[str, Any]) -> List[str]:
    """The roster entry's allowlist: every name of every tool group, in order."""
    groups = entry.get("tools")
    if not isinstance(groups, Mapping):
        return []
    seen: Dict[str, None] = {}
    for names in groups.values():
        if isinstance(names, (list, tuple)):
            for name in names:
                if isinstance(name, str) and name and name not in seen:
                    seen[name] = None
    return list(seen)


def select_child_tool_names(
    entry_names: Iterable[str],
    parent_names: Iterable[str],
    *,
    write_policy: str = "full",
) -> tuple[List[str], Dict[str, str]]:
    """``entry ∩ parent − control plane`` (and minus writes under ``none``).

    Returns ``(names, dropped)`` where ``dropped`` maps each excluded name to
    the reason — logged once per spawn by the caller.
    """
    from agent.tools.registry import TOOL_REGISTRY

    parent = set(parent_names)
    names: List[str] = []
    dropped: Dict[str, str] = {}
    for name in entry_names:
        if name in DELEGATION_TOOL_NAMES:
            dropped[name] = "control plane (delegation)"
            continue
        category = (TOOL_REGISTRY.get(name) or {}).get("category")
        if category in CONTROL_PLANE_CATEGORIES:
            dropped[name] = f"control plane ({category})"
            continue
        if name not in parent:
            dropped[name] = "not loaded by the parent"
            continue
        if write_policy == "none" and name in WRITE_TOOLS:
            dropped[name] = "write_policy=none"
            continue
        names.append(name)
    return names, dropped


# ---------------------------------------------------------------------------
# Write policy
# ---------------------------------------------------------------------------


class SharedWriterGuard:
    """At most one ``shared`` child holding write tools per parent (B.8)."""

    def __init__(self) -> None:
        self._writer: Optional[str] = None

    @property
    def active_writer(self) -> Optional[str]:
        return self._writer

    def acquire(self, handle: str, *, isolation: str, writes: bool) -> None:
        if isolation != "shared" or not writes:
            return
        if self._writer is not None and self._writer != handle:
            raise SpawnRefused(
                f"subagent {handle}: another child ({self._writer}) already holds "
                "write tools in the shared tree — wait for it to return, or use "
                "isolation=worktree for parallel writers"
            )
        self._writer = handle

    def release(self, handle: str) -> None:
        if self._writer == handle:
            self._writer = None


def normalise_rel_path(value: Any) -> Optional[str]:
    """A tool's path argument as a normalised workspace-relative path."""
    if not isinstance(value, str) or not value.strip():
        return None
    path = value.strip().replace("\\", "/")
    path = posixpath.normpath(path)
    if path == ".":
        return ""
    return path


def path_allowed(path: str, globs: Sequence[str]) -> bool:
    """``fnmatch`` of a normalised path against the owned globs.

    A glob ending in ``/`` means the directory and everything below it; a
    ``dir/**`` glob also allows ``dir`` itself (``create_directory``).
    Absolute paths and ``..`` escapes never match.
    """
    if path.startswith("/") or path == ".." or path.startswith("../"):
        return False
    for raw in globs:
        glob = (raw or "").strip().replace("\\", "/")
        if not glob:
            continue
        if glob.endswith("/"):
            glob = glob + "**"
        if fnmatch.fnmatchcase(path, glob):
            return True
        if glob.endswith("/**") and path == glob[:-3]:
            return True
    return False


def _bound_arguments(
    orig: Callable[..., Any], args: tuple, kwargs: dict
) -> Dict[str, Any]:
    if not args:
        return dict(kwargs)
    try:
        return dict(inspect.signature(orig).bind_partial(*args, **kwargs).arguments)
    except (TypeError, ValueError):
        return dict(kwargs)


def write_policy_violation(
    tool_name: str,
    arguments: Mapping[str, Any],
    *,
    policy: str,
    globs: Sequence[str],
) -> Optional[str]:
    """The error text for a write outside the owned paths, else ``None``."""
    owned = ", ".join(globs)
    if tool_name.startswith("kb_"):
        return (
            f"Error: {tool_name} is not allowed under write_policy={policy} — "
            f"knowledge-base writes are outside this subagent's owned paths "
            f"[{owned}]. Report the finding instead; the parent decides what to record."
        )
    targets: List[tuple[str, Any]] = [
        (k, arguments[k]) for k in PATH_ARGS if k in arguments
    ]
    if tool_name == "rename_file" and "path" in arguments and "new_name" in arguments:
        renamed = posixpath.join(
            posixpath.dirname(str(arguments["path"])), str(arguments["new_name"])
        )
        targets.append(("new_name", renamed))
    for _key, raw in targets:
        rel = normalise_rel_path(raw)
        if rel is None:
            continue
        if not path_allowed(rel, globs):
            return (
                f"Error: {raw} is outside this subagent's owned paths [{owned}] — "
                f"write only inside them (write_policy={policy})."
            )
    return None


def apply_write_policy(
    tools: List[Any],
    *,
    policy: str,
    handle: str,
    owned_paths: Optional[Sequence[str]] = None,
) -> List[Any]:
    """Wrap the write tools with the path allowlist of ``policy`` (B.8).

    ``none`` (already stripped by name) and ``full`` add nothing;
    ``scratch_only`` allows ``.subagents/<handle>/**``; ``owned_paths`` the
    caller's globs — a spawn under ``owned_paths`` with no globs is refused.
    Same ``.func`` / ``.coroutine`` wrap as ``apply_instruction_enforcement``.
    """
    if policy not in WRITE_POLICIES:
        raise SpawnRefused(
            f"subagent {handle}: unknown write_policy {policy!r} "
            f"(expected one of {', '.join(WRITE_POLICIES)})"
        )
    if policy in ("full", "none"):
        return tools
    if policy == "scratch_only":
        globs: List[str] = [f".subagents/{handle}/**"]
    else:
        globs = [str(g) for g in (owned_paths or []) if str(g).strip()]
        if not globs:
            raise SpawnRefused(
                f"subagent {handle}: write_policy=owned_paths needs owned_paths "
                "(the globs this child may write) — pass them on the spawn"
            )

    def _guard(tool_name: str, orig: Callable[..., Any], args: tuple, kwargs: dict):
        return write_policy_violation(
            tool_name, _bound_arguments(orig, args, kwargs), policy=policy, globs=globs
        )

    def _sync(orig: Callable[..., Any], tool_name: str):
        @functools.wraps(orig)
        def wrapper(*args, **kwargs):
            blocked = _guard(tool_name, orig, args, kwargs)
            if blocked is not None:
                return blocked
            return orig(*args, **kwargs)

        return wrapper

    def _async(orig: Callable[..., Any], tool_name: str):
        @functools.wraps(orig)
        async def wrapper(*args, **kwargs):
            blocked = _guard(tool_name, orig, args, kwargs)
            if blocked is not None:
                return blocked
            return await orig(*args, **kwargs)

        return wrapper

    wrapped = 0
    for tool in tools:
        name = getattr(tool, "name", None)
        if name not in WRITE_TOOLS:
            continue
        if getattr(tool, "func", None) is not None:
            tool.func = _sync(tool.func, name)
            wrapped += 1
        if getattr(tool, "coroutine", None) is not None:
            tool.coroutine = _async(tool.coroutine, name)
            wrapped += 1
    logger.info(
        "subagent %s: write_policy=%s over %s (%d tool bindings wrapped)",
        handle,
        policy,
        globs,
        wrapped,
    )
    return tools


# ---------------------------------------------------------------------------
# Git / shell proxies for the shared tree
# ---------------------------------------------------------------------------

_GIT_MUTATORS = frozenset(
    {
        "commit",
        "push",
        "push_ref",
        "pull",
        "add_remote",
        "checkout_branch",
        "create_branch",
        "delete_branch",
        "merge",
        "merge_branch",
        "worktree_add",
        "worktree_remove",
        "init_repository",
        "commit_workspace_undo",
        "commit_workspace_undo_preparation",
        "reset_hard",
        "revert",
        "tag",
        "create_tag",
        "stash",
    }
)


class ReadOnlyGitManager:
    """A ``shared`` child's view of the parent's GitManager: reads pass
    through (``git_log`` / ``git_diff`` keep working), every mutator is a
    no-op returning ``False`` and the loop's auto-commit sees nothing to do."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    @property
    def is_active(self) -> bool:
        return bool(getattr(self._inner, "is_active", False))

    def has_uncommitted_changes(self) -> bool:
        return False

    def has_unpushed_commits(self, *args: Any, **kwargs: Any) -> bool:
        return False

    def _noop(self, *args: Any, **kwargs: Any) -> bool:
        return False

    def __getattr__(self, name: str) -> Any:
        if name in _GIT_MUTATORS:
            return self._noop
        return getattr(self._inner, name)


class NoPushGitManager:
    """A ``worktree`` child's GitManager: commits stay on the worktree branch,
    nothing is ever pushed (the parent merges or discards the branch)."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    @property
    def is_active(self) -> bool:
        return bool(getattr(self._inner, "is_active", False))

    def has_unpushed_commits(self, *args: Any, **kwargs: Any) -> bool:
        return False

    def push(self, *args: Any, **kwargs: Any) -> bool:
        return False

    def push_ref(self, *args: Any, **kwargs: Any) -> bool:
        return False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


_TAB_UNSAFE = re.compile(r"[^a-z0-9]")


def child_tab_prefix(handle: str) -> str:
    """A shell-tab prefix that survives the real transport's validator.

    ``RemoteBackend.TAB_NAME_PATTERN`` is ``^[a-z0-9-]{1,20}$`` and the tab a
    child opens is ``<prefix><name>``, so the prefix must be short and contain
    no underscore. The handle's 4-hex suffix is already unique per parent,
    which is all the namespacing a child needs, and it leaves 15 characters
    for the child's own tab name.

    The original ``f"{handle}__"`` failed that pattern twice over -- underscores
    and 25 characters for ``implementer-1cb8__default`` -- which broke
    ``run_command`` for every shell-capable child. It went unnoticed because the
    unit test drove a permissive filesystem backend; found on the U3 WP7 k3d
    acceptance run (job dde11ae6).
    """
    tail = _TAB_UNSAFE.sub("", handle.rsplit("-", 1)[-1].lower())[:4]
    if not tail:
        tail = hashlib.sha256(handle.encode()).hexdigest()[:4]
    return f"{tail}-"


class SharedTreeShellBackend(SubdirBackend):
    """The parent backend with short handle-derived prefixed shell tabs and NO
    path re-rooting — a ``shared`` child's shell runs in the parent tree on the
    parent's tmux session, on tabs of its own. See :func:`child_tab_prefix` for
    why the prefix is the handle's hex tail rather than the whole handle."""

    def __init__(self, parent: Any, *, shell_tab_prefix: str) -> None:
        super().__init__(parent, "", shell_tab_prefix=shell_tab_prefix)

    @property
    def root(self) -> str:
        return self._parent.root

    def shell_run(self, command, timeout=None, tab_name="default", working_dir=None):
        return self._parent.shell_run(
            command,
            timeout=timeout,
            tab_name=self._tab(tab_name),
            working_dir=working_dir,
        )

    def shell_send(self, name, text, enter=True, working_dir=None, allow_busy=False):
        return self._parent.shell_send(
            self._tab(name),
            text,
            enter=enter,
            working_dir=working_dir,
            allow_busy=allow_busy,
        )


# ---------------------------------------------------------------------------
# Config, LLM, prompt, context manager
# ---------------------------------------------------------------------------


def _absolute_deployment_dir(directory: Any) -> Optional[str]:
    """A frozen entry's ``_deployment_dir`` is repo-relative (the orchestrator
    and the agent image share ``config/`` but not the prefix); resolve it
    against the project root when it is."""
    if not isinstance(directory, str) or not directory:
        return None
    path = Path(directory)
    if path.is_absolute():
        return str(path)
    candidate = get_project_root() / path
    if candidate.is_dir():
        return str(candidate)
    return directory


def overlay_live_llm(child_llm: Any, live: Any) -> Any:
    """The parent's LIVE identity + transport on an ``inherit`` entry's llm.

    Same model → non-``None`` live values only (a live ``base_url: None`` never
    clears an injected endpoint); model CHANGED (a fallback-path swap) → the
    whole identity verbatim, mirroring ``loader.inherit_parent_llm``.
    """
    if live is None:
        return child_llm
    changed = getattr(live, "model", None) not in (None, child_llm.model)
    updates: Dict[str, Any] = {}
    for key in ("model", "provider", "base_url", "api_key", "model_max_context_tokens"):
        value = getattr(live, key, None)
        if value is not None or changed:
            updates[key] = value
    if not updates:
        return child_llm
    return dataclasses.replace(child_llm, **updates)


def build_child_config(entry: Mapping[str, Any], *, live_llm_config: Any = None) -> Any:
    """``AgentConfig`` of a resolved roster entry.

    ``load_agent_config_from_dict(entry, deployment_dir=entry["_deployment_dir"])``;
    an ``inherit`` entry (``llm._inherit_llm``) is then overlaid with the
    parent's LIVE ``LLMConfig`` (dispatch-time credentials, a fallback swap);
    a pinned entry without credentials borrows the parent's when it runs on
    the same provider and names no endpoint of its own (or the parent's).
    ``officer.enabled`` is forced off (D(4): a DB ``$ref``
    to a centurion must never bring the officer loop guard into a child).
    """
    data = copy.deepcopy(dict(entry))
    deployment_dir = _absolute_deployment_dir(data.get("_deployment_dir"))
    from shared.runtime.core.loader import load_agent_config_from_dict

    cfg = load_agent_config_from_dict(data, deployment_dir=deployment_dir)
    raw_llm = entry.get("llm") if isinstance(entry.get("llm"), Mapping) else {}
    if raw_llm.get(ROSTER_INHERIT_MARKER):
        cfg.llm = overlay_live_llm(cfg.llm, live_llm_config)
    elif live_llm_config is not None and cfg.llm.api_key is None:
        from shared.runtime.core.transport_resolution import same_endpoint

        same_provider = (cfg.llm.provider or None) in (
            None,
            getattr(live_llm_config, "provider", None),
        )
        # A key follows its endpoint (same rule as LLMConfig.with_override):
        # borrow the parent's key only when the entry names no endpoint of its
        # own or names the parent's.
        same_host = cfg.llm.base_url is None or same_endpoint(
            cfg.llm.base_url, getattr(live_llm_config, "base_url", None)
        )
        if same_provider and same_host:
            borrowed: Dict[str, Any] = {
                "api_key": getattr(live_llm_config, "api_key", None)
            }
            if cfg.llm.base_url is None and cfg.llm.provider is None:
                borrowed["base_url"] = getattr(live_llm_config, "base_url", None)
                borrowed["provider"] = getattr(live_llm_config, "provider", None)
            cfg.llm = dataclasses.replace(cfg.llm, **borrowed)
    officer = getattr(cfg, "officer", None)
    if officer is not None and getattr(officer, "enabled", False):
        logger.warning(
            "subagent %s: officer.enabled came through the roster — forced off",
            cfg.agent_id,
        )
    if officer is not None:
        try:
            officer.enabled = False
        except Exception:  # pragma: no cover - frozen doubles
            pass
    return cfg


def _default_llm_factory(llm_config: Any, limits: Any) -> Any:
    from shared.runtime.core.loader import create_llm

    return create_llm(llm_config, limits)


def bind_child_tools(llm: Any, tools: List[Any], cfg: Any) -> Any:
    """``persistent_session._bind_tools`` for a child: guardrail Examples on
    bound copies + the ``parallel_tool_calls`` gate. Never the phase prefixes."""
    from shared.runtime.core.loader import supports_parallel_tool_calls
    from shared.runtime.services.guardrails import apply_guardrails_to_tools

    bind_kwargs: Dict[str, Any] = {}
    if supports_parallel_tool_calls(cfg.llm.provider, cfg.llm.model):
        bind_kwargs["parallel_tool_calls"] = cfg.llm.parallel_tool_calls
    bound = apply_guardrails_to_tools(
        tools, model=cfg.llm.model, deployment_dir=cfg._deployment_dir
    )
    return llm.bind_tools(bound, **bind_kwargs)


_RETURN_EXPECTATIONS = {
    "summary": "a compact answer with the evidence needed to use it",
    "structured": "findings grouped as requested, with exact outcomes",
    "evidence": "raw, attributable evidence with commands or locations and outputs",
    "diff": "changed files and behavior, followed by verification results",
}


def render_subagent_environment(
    entry: Mapping[str, Any],
    budgets: Any,
    *,
    handle: str,
    subagent_type: str,
    isolation: str,
    write_policy: str,
    owned_paths: Sequence[str] = (),
    worktree_path: Optional[str] = None,
    worktree_branch: Optional[str] = None,
) -> str:
    """The trusted, spawn-specific environment block injected into the prompt."""
    if isolation == "worktree":
        isolation_line = (
            f"Isolation: worktree — path `{worktree_path or '(pending)'}` on branch "
            f"`{worktree_branch or f'sub/{handle}'}`; the parent integrates it."
        )
    else:
        isolation_line = (
            "Isolation: shared — the parent's tree; no commits — the parent commits."
        )

    if write_policy == "owned_paths":
        paths = ", ".join(f"`{path}`" for path in owned_paths) or "(none)"
        write_line = f"Write policy: owned_paths — write only these globs: {paths}."
    elif write_policy == "scratch_only":
        write_line = (
            f"Write policy: scratch_only — write only under `.subagents/{handle}/`."
        )
    elif write_policy == "none":
        write_line = "Write policy: none — do not write to the workspace."
    else:
        write_line = "Write policy: full — write only what the brief requires."

    return_kind = str(entry.get("return") or "summary")
    expectation = _RETURN_EXPECTATIONS.get(return_kind, _RETURN_EXPECTATIONS["summary"])
    return "\n".join(
        [
            "<subagent_environment>",
            f"Handle: {handle}",
            f"Subagent type: {subagent_type}",
            isolation_line,
            write_line,
            f"Expected report ({return_kind}): {expectation}.",
            (
                f"Budget: at most {budgets.max_turns} turns and "
                f"{budgets.max_tokens} tokens; your report is trimmed past "
                f"{budgets.return_budget_tokens} tokens — the full text is spilled "
                f"to `.subagents/{handle}/report.md`."
            ),
            "</subagent_environment>",
        ]
    )


def child_system_prompt(cfg: Any, tools: List[Any], *, environment: str) -> str:
    """Render the framework-owned child prompt with this spawn's environment."""
    from shared.runtime.core.loader import get_subagent_system_prompt

    return get_subagent_system_prompt(
        cfg,
        model=cfg.llm.model or "",
        tool_names=[t.name for t in tools],
        environment=environment,
    )


def drop_missing_before_tool_bindings(
    cfg: Any, workspace_manager: Any, *, handle: str
) -> List[str]:
    """Drop enforced tool bindings whose artifact is absent from this tree.

    A referenced expert may carry bindings for skills only its own parent
    deploys. Keeping such a gate would permanently wedge the child: it can be
    told to read a path that does not exist. Phase bindings are left alone
    (children have no phases); only ``before_tool`` enforcement is filtered.
    """
    kept = []
    dropped: List[str] = []
    for entry in list(getattr(cfg, "instruction_files", None) or []):
        if entry.trigger_type != "before_tool":
            kept.append(entry)
            continue
        try:
            exists = bool(workspace_manager.exists(entry.path))
        except Exception:
            exists = False
        if exists:
            kept.append(entry)
        else:
            dropped.append(entry.path)
    cfg.instruction_files = kept
    if dropped:
        logger.warning(
            "subagent %s: dropped before_tool binding(s) with no artifact in "
            "the child tree: %s",
            handle,
            ", ".join(dropped),
        )
    return dropped


def build_context_manager(cfg: Any) -> Any:
    """``PersistentSession._build_context_config`` / ``_setup_context_manager``."""
    from agent.core.context import ContextConfig, ContextManager

    ctx, lim = cfg.context_management, cfg.limits
    return ContextManager(
        config=ContextConfig(
            compaction_threshold_tokens=lim.context_threshold_tokens,
            summarization_threshold_tokens=lim.context_threshold_tokens,
            message_count_threshold=lim.message_count_threshold,
            message_count_min_tokens=lim.message_count_min_tokens,
            keep_recent_tool_results=ctx.keep_recent_tool_results,
            keep_recent_messages=ctx.keep_recent_messages,
            keep_window_max_tool_result_chars=ctx.keep_window_max_tool_result_chars,
            model_max_context_tokens=lim.model_max_context_tokens,
            image_tokens=lim.image_tokens,
        ),
        model=cfg.llm.model or "gpt-4",
        summarization_call_timeout=cfg.auxiliary.summarization_call_timeout,
    )


def child_tool_config(
    cfg: Any, parent_tool_config: Optional[Mapping[str, Any]]
) -> Dict[str, Any]:
    """The child's ``ToolContext.config``: the parent's runtime facts (cloud
    mount, resolved skills, ...) under the child's own settings and identity;
    never the ``delegation`` / ``subagents`` blocks."""
    identity = ("agent_id", "multimodal", "model_max_context_tokens", "pdf_render_dpi")
    out: Dict[str, Any] = {
        k: v
        for k, v in (parent_tool_config or {}).items()
        if k not in identity and k not in ("delegation", "subagents", "tags")
    }
    out.update(cfg.extra)
    out.pop("delegation", None)
    out.pop("subagents", None)
    out.update(
        {
            "agent_id": cfg.agent_id,
            "multimodal": cfg.llm.multimodal,
            "model_max_context_tokens": cfg.limits.model_max_context_tokens,
            "pdf_render_dpi": getattr(cfg.limits, "pdf_render_dpi", None),
            "tags": list(getattr(cfg, "tags", []) or []),
        }
    )
    return out


# ---------------------------------------------------------------------------
# The re-rooted ToolContext copy
# ---------------------------------------------------------------------------

#: What the copy shares BY REFERENCE with the parent (B.2): the parent job's
#: identity and its connections.
SHARED_CONTEXT_FIELDS = (
    "_job_metadata",
    "orchestrator_client",
    "postgres_db",
    "vector_db",
    "datasources",
    "knowledge_bindings",
    "runtime_actor",
    "_job_id",
    "_thread_id",
    "user_id",
)


def rebase_context(
    ctx: Any,
    *,
    cfg: Any,
    tool_config: Dict[str, Any],
    workspace_manager: Any,
    shell_manager: Any,
) -> Any:
    """Apply the B.2 resets to a ``copy.copy``'d parent context, in place."""
    ctx.workspace_manager = workspace_manager
    ctx.shell_manager = shell_manager
    ctx.todo_manager = None
    ctx.session_task_manager = None
    ctx.canvas_event_callback = None
    ctx.progress_committer = None
    ctx.citation_engine = None
    ctx._snapshot_callback = None
    ctx._freeze_request = None
    ctx._officer_sleep_request = None
    ctx._replan_request = None
    ctx._reply_drain_requested = False
    ctx._recent_reads = deque(maxlen=10)
    ctx._pinned_reads = set()
    ctx._recent_read_versions = {}
    ctx._instruction_read_stamps = {}
    ctx._pending_memories = []
    ctx._delivered_reply_keys = set()
    ctx._source_registry = {}
    ctx._current_phase = None
    ctx._current_phase_number = None
    ctx._current_turn_count = 0
    ctx._graph_progress = 0
    # Never a parent: no runtime, no host, no fork seed, no probe and no
    # admission fence of its own (the driver consults the HOST's).
    ctx.subagent_runtime = None
    ctx._parent_host = None
    ctx._subagent_parent_kind = None
    ctx._session_parent_authority_provider = None
    ctx._session_parent_authority = None
    ctx.parent_context_probe = None
    ctx.provider_admission = None
    ctx._fork_source = None
    ctx._parent_audit_metadata = None
    ctx.config = tool_config
    ctx._llm_config = cfg.llm
    ctx._limits = cfg.limits
    ctx._instruction_files = list(cfg.instruction_files)
    ctx._resolved_tool_names = []
    return ctx


# ---------------------------------------------------------------------------
# The build
# ---------------------------------------------------------------------------


@dataclass
class ChildBuild:
    """Everything the driver needs to run one child."""

    handle: str
    subagent_type: str
    isolation: str
    write_policy: str
    config: Any
    llm: Any
    llm_with_tools: Any
    tools: List[Any]
    tool_context: Any
    system_prompt: str
    subagent_environment: str
    context_manager: Any
    workspace_manager: Any
    owned_paths: List[str] = field(default_factory=list)
    dropped_tools: Dict[str, str] = field(default_factory=dict)
    writes_enabled: bool = False
    worktree_path: Optional[str] = None
    reader_env: Any = None
    shell_backend: Any = None
    writer_guard: Optional[SharedWriterGuard] = None
    released: bool = False

    @property
    def tool_names(self) -> List[str]:
        return [t.name for t in self.tools]

    async def release(self) -> None:
        """Tear the child's environment down (idempotent, best-effort)."""
        if self.released:
            return
        self.released = True
        if self.writer_guard is not None:
            self.writer_guard.release(self.handle)
        if self.reader_env is not None:
            from agent.tools.delegation.reader_env import release_reader_env

            try:
                await release_reader_env(self.reader_env)
            except Exception as e:  # pragma: no cover - best effort
                logger.warning(
                    "subagent %s: worktree release failed: %s", self.handle, e
                )
        elif self.shell_backend is not None:
            try:
                self.shell_backend.close_reader_tabs()
            except Exception as e:  # pragma: no cover - best effort
                logger.warning(
                    "subagent %s: closing shell tabs failed: %s", self.handle, e
                )


def _shared_shell_manager(
    parent_context: Any,
    workspace_manager: Any,
    *,
    handle: str,
    names: Sequence[str],
    tool_config: Mapping[str, Any],
) -> tuple[Any, Any]:
    """A fresh ``ShellManager`` over the parent backend with ``<handle>__`` tabs
    — only when the child holds a shell tool and the backend has a shell."""
    from agent.tools.registry import TOOL_REGISTRY

    wants_shell = any(
        (TOOL_REGISTRY.get(n) or {}).get("category") == "shell" for n in names
    )
    backend = getattr(workspace_manager, "backend", None)
    if (
        not wants_shell
        or backend is None
        or not getattr(backend, "supports_shell", False)
    ):
        return None, None
    prefixed = SharedTreeShellBackend(
        backend, shell_tab_prefix=child_tab_prefix(handle)
    )
    try:
        from agent.tools.shell.shell_manager import ShellManager

        shell_cfg = tool_config.get("shell") or {}
        if not isinstance(shell_cfg, Mapping):
            shell_cfg = {}
        parent_job = getattr(workspace_manager, "job_id", None) or (
            parent_context.job_id or "job"
        )
        shell = ShellManager(
            job_id=f"{parent_job}-{handle}",
            max_tabs=shell_cfg.get("max_tabs", 15),
            scrollback_limit=shell_cfg.get("scrollback_limit", 5000),
            default_timeout=shell_cfg.get("default_timeout", 120),
            blocked_commands=shell_cfg.get("blocked_commands"),
            sandbox_cwd=str(workspace_manager.path)
            if shell_cfg.get("sandbox", True)
            else None,
            backend=prefixed,
            sudo_action=shell_cfg.get("sudo_action", "freeze"),
            sudo_block_message=shell_cfg.get("sudo_block_message"),
        )
    except Exception as e:
        logger.warning(
            "subagent %s: shell manager init failed (non-fatal): %s", handle, e
        )
        return None, prefixed
    return shell, prefixed


async def build_child(
    entry: Mapping[str, Any],
    *,
    parent_context: Any,
    host: ParentHost,
    handle: str,
    subagent_type: str,
    isolation: Optional[str] = None,
    write_policy: Optional[str] = None,
    owned_paths: Optional[Sequence[str]] = None,
    writer_guard: Optional[SharedWriterGuard] = None,
    parent_tool_names: Optional[Sequence[str]] = None,
    llm_factory: Optional[Callable[[Any, Any], Any]] = None,
    worktree_index: Optional[int] = None,
    reuse_worktree: bool = False,
    budgets: Any = None,
) -> ChildBuild:
    """Build the child of ``entry`` for ``parent_context`` (see module doc).

    ``isolation`` / ``write_policy`` default to the entry's own keys, then to
    ``shared`` / ``full``. ``parent_tool_names`` defaults to the parent
    context's ``_resolved_tool_names``. ``llm_factory`` (tests) replaces
    ``create_llm``.
    """
    isolation = str(isolation or entry.get("isolation") or "shared")
    if isolation not in ISOLATIONS:
        raise SpawnRefused(
            f"subagent {handle}: unknown isolation {isolation!r} "
            f"(expected one of {', '.join(ISOLATIONS)})"
        )
    write_policy = str(write_policy or entry.get("write_policy") or "full")
    if write_policy not in WRITE_POLICIES:
        raise SpawnRefused(
            f"subagent {handle}: unknown write_policy {write_policy!r} "
            f"(expected one of {', '.join(WRITE_POLICIES)})"
        )
    owned = [str(g) for g in (owned_paths or []) if str(g).strip()]
    if write_policy == "owned_paths" and not owned:
        raise SpawnRefused(
            f"subagent {handle}: write_policy=owned_paths needs owned_paths "
            "(the globs this child may write) — pass them on the spawn"
        )

    cfg = build_child_config(
        entry, live_llm_config=getattr(host, "live_llm_config", None)
    )
    if budgets is None:
        from agent.subagents.budgets import ChildBudgets

        budgets = ChildBudgets.from_entry(entry, subagent_type)
    parent_names = list(
        parent_tool_names
        if parent_tool_names is not None
        else (getattr(parent_context, "_resolved_tool_names", None) or [])
    )
    names, dropped = select_child_tool_names(
        entry_tool_names(entry), parent_names, write_policy=write_policy
    )
    if dropped:
        logger.info(
            "subagent %s (%s): %d tool(s) not bound — %s",
            handle,
            subagent_type,
            len(dropped),
            "; ".join(f"{n}: {why}" for n, why in dropped.items()),
        )
    writes_enabled = bool(set(names) & WRITE_TOOLS)
    if writer_guard is not None:
        writer_guard.acquire(handle, isolation=isolation, writes=writes_enabled)

    parent_ws = getattr(parent_context, "workspace_manager", None)
    if parent_ws is None:
        if writer_guard is not None:
            writer_guard.release(handle)
        raise SpawnRefused(f"subagent {handle}: the parent has no workspace")

    tool_config = child_tool_config(cfg, getattr(parent_context, "config", None))
    env = None
    shell_backend = None
    worktree_path = None
    try:
        if isolation == "worktree":
            from agent.tools.delegation.reader_env import acquire_reader_env

            index = (
                worktree_index if worktree_index is not None else next(_WORKTREE_INDEX)
            )
            env = await acquire_reader_env(
                parent_context,
                [],
                index=index,
                name=handle,
                reuse_existing_branch=reuse_worktree,
                shell_tab_prefix=child_tab_prefix(handle),
            )
            ctx = env.context
            workspace_manager = ctx.workspace_manager
            if getattr(workspace_manager, "_git_manager", None) is not None:
                workspace_manager._git_manager = NoPushGitManager(
                    workspace_manager._git_manager
                )
            shell_manager = ctx.shell_manager
            shell_backend = env._subdir_backend
            worktree_path = env.worktree_path
        else:
            ctx = copy.copy(parent_context)
            workspace_manager = copy.copy(parent_ws)
            parent_git = getattr(parent_ws, "git_manager", None)
            workspace_manager._git_manager = (
                ReadOnlyGitManager(parent_git) if parent_git is not None else None
            )
            shell_manager, shell_backend = _shared_shell_manager(
                parent_context,
                workspace_manager,
                handle=handle,
                names=names,
                tool_config=tool_config,
            )
        rebase_context(
            ctx,
            cfg=cfg,
            tool_config=tool_config,
            workspace_manager=workspace_manager,
            shell_manager=shell_manager,
        )
        # The parent deploys instruction artifacts before any child exists. A
        # `$ref` may carry gates for artifacts that are not in this parent's
        # tree; remove only those impossible gates before wrapping tools.
        drop_missing_before_tool_bindings(cfg, workspace_manager, handle=handle)
        ctx._instruction_files = list(cfg.instruction_files)

        from agent.tools.description_manager import apply_description_overrides
        from agent.tools.registry import apply_instruction_enforcement, load_tools

        tools = load_tools(names, ctx)
        ctx._resolved_tool_names = [t.name for t in tools]
        tools = apply_description_overrides(tools)
        tools = apply_instruction_enforcement(tools, ctx)
        tools = apply_write_policy(
            tools, policy=write_policy, handle=handle, owned_paths=owned
        )
        if env is not None:
            env.tools.extend(tools)  # the worktree's tools/ overlay renders these

        llm = (llm_factory or _default_llm_factory)(cfg.llm, cfg.limits)
        llm_with_tools = bind_child_tools(llm, tools, cfg)
        subagent_environment = render_subagent_environment(
            entry,
            budgets,
            handle=handle,
            subagent_type=subagent_type,
            isolation=isolation,
            write_policy=write_policy,
            owned_paths=owned,
            worktree_path=worktree_path,
            worktree_branch=getattr(env, "branch", None),
        )
        system_prompt = child_system_prompt(
            cfg, tools, environment=subagent_environment
        )
        context_manager = build_context_manager(cfg)
    except BaseException:
        if writer_guard is not None:
            writer_guard.release(handle)
        if env is not None:
            from agent.tools.delegation.reader_env import release_reader_env

            try:
                await release_reader_env(env)
            except Exception:  # pragma: no cover - best effort
                logger.warning(
                    "subagent %s: worktree cleanup failed", handle, exc_info=True
                )
        raise

    return ChildBuild(
        handle=handle,
        subagent_type=subagent_type,
        isolation=isolation,
        write_policy=write_policy,
        config=cfg,
        llm=llm,
        llm_with_tools=llm_with_tools,
        tools=tools,
        tool_context=ctx,
        system_prompt=system_prompt,
        subagent_environment=subagent_environment,
        context_manager=context_manager,
        workspace_manager=workspace_manager,
        owned_paths=owned,
        dropped_tools=dropped,
        writes_enabled=writes_enabled,
        worktree_path=worktree_path,
        reader_env=env,
        shell_backend=shell_backend,
        writer_guard=writer_guard,
    )


__all__ = [
    "CONTROL_PLANE_CATEGORIES",
    "DELEGATION_TOOL_NAMES",
    "ISOLATIONS",
    "PATH_ARGS",
    "SHARED_CONTEXT_FIELDS",
    "WRITE_POLICIES",
    "WRITE_TOOLS",
    "ChildBuild",
    "NoPushGitManager",
    "ReadOnlyGitManager",
    "SharedTreeShellBackend",
    "SharedWriterGuard",
    "SpawnRefused",
    "apply_write_policy",
    "bind_child_tools",
    "build_child",
    "build_child_config",
    "build_context_manager",
    "child_system_prompt",
    "child_tab_prefix",
    "child_tool_config",
    "drop_missing_before_tool_bindings",
    "entry_tool_names",
    "normalise_rel_path",
    "overlay_live_llm",
    "path_allowed",
    "rebase_context",
    "render_subagent_environment",
    "select_child_tool_names",
    "write_policy_violation",
]
