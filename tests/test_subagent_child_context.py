"""The child build (U3 WP1, plan B.2/B.8/B.9): tools, context copy, isolation,
LLM inheritance, write policy, the single-writer guard, officer never on."""

from __future__ import annotations

import logging
import subprocess
from collections import deque

import pytest
from langchain_core.tools import tool

from shared.runtime.core.loader import LLMConfig, ROSTER_INHERIT_MARKER
from shared.runtime.core.subagent_roster import resolve_subagent_roster
from agent.core.workspace import WorkspaceManager, WorkspaceManagerConfig
from agent.subagents import (
    SimpleParentHost,
    SpawnRefused,
    build_child,
    build_child_config,
)
from agent.subagents.child import (
    CONTROL_PLANE_CATEGORIES,
    DELEGATION_TOOL_NAMES,
    WRITE_TOOLS,
    NoPushGitManager,
    ReadOnlyGitManager,
    SharedTreeShellBackend,
    SharedWriterGuard,
    apply_write_policy,
    child_tool_config,
    entry_tool_names,
    overlay_live_llm,
    path_allowed,
    rebase_context,
    select_child_tool_names,
    write_policy_violation,
)
from agent.tools.context import ToolContext
from tests._fake_chat_model import FakeChatModel
from tests._fs_backend import FilesystemTestBackend

_PARENT_LLM = {
    "model": "gpt-4o-mini",
    "provider": "openai",
    "api_key": "sk-parent-test",
    "base_url": "https://router.example/v1",
    "model_max_context_tokens": 128000,
}


def _entry(
    name="explorer", ref="subagents/explorer", parent_llm=None, **overrides
) -> dict:
    data = {
        "agent_id": "parent",
        "display_name": "Parent",
        "llm": dict(parent_llm or _PARENT_LLM),
        "subagents": {
            "roster": {name: {"$ref": ref, **overrides} if ref else overrides}
        },
    }
    return resolve_subagent_roster(data, db_refs={}, on_missing="raise")["subagents"][
        "roster"
    ][name]


def _git(args, cwd):
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True)


def _parent(tmp_path, *, git=False, names=None):
    root = tmp_path / "ws"
    root.mkdir()
    (root / "README.md").write_text("# parent\n")
    if git:
        _git(["init", "-q"], root)
        _git(["config", "user.email", "t@t.com"], root)
        _git(["config", "user.name", "T"], root)
        _git(["add", "."], root)
        _git(["commit", "-q", "-m", "init"], root)
    ws = WorkspaceManager(
        job_id="parent-job",
        config=WorkspaceManagerConfig(
            structure=[], base_path=str(root), git_versioning=False
        ),
        backend=FilesystemTestBackend(root),
    )
    ws._initialized = True
    if git:
        from agent.managers.git_manager import GitManager

        ws._git_manager = GitManager(root)
    ctx = ToolContext(
        workspace_manager=ws,
        orchestrator_client=object(),
        _job_metadata={"job_id": "parent-job", "project_id": "proj"},
        config={
            "shell": {},
            "cloud_mount": {"active": False},
            "delegation": {"enabled": True},
            "subagents": {"roster": {}},
            "agent_id": "developer",
        },
        _resolved_tool_names=list(
            names
            or [
                "read_file",
                "list_files",
                "search_files",
                "write_file",
                "edit_file",
                "create_directory",
                "delete_file",
                "delegate_agent",
                "job_complete",
            ]
        ),
    )
    return ctx, root


def _host(**kw):
    return SimpleParentHost(job_id="parent-job", agent_type="developer", **kw)


async def _build(ctx, entry, **kw):
    kw.setdefault("host", _host(live_llm_config=LLMConfig(**_PARENT_LLM)))
    kw.setdefault("handle", "explorer-0001")
    kw.setdefault("subagent_type", "explorer")
    kw.setdefault("llm_factory", lambda cfg, lim: FakeChatModel([]))
    return await build_child(entry, parent_context=ctx, **kw)


# ---------------------------------------------------------------------------
# Tool selection
# ---------------------------------------------------------------------------


class TestToolSelection:
    def test_allowlist_intersects_the_parents_loaded_names(self):
        names, dropped = select_child_tool_names(
            ["read_file", "web_search", "list_files"],
            ["read_file", "list_files", "shell_execute"],
        )
        assert names == ["read_file", "list_files"]
        assert dropped == {"web_search": "not loaded by the parent"}

    def test_control_plane_is_denied_even_when_the_parent_has_it(self):
        parent = [
            "read_file",
            "delegate_agent",
            "wait_agent",
            "message_agent",
            "stop_agent",
            "list_agents",
            "job_complete",
            "todo_complete",
            "send_message",
            "set_canvas",
            "create_job",
            "loop_plan",
            "approve_job_verdict",
            "task_add",
            "get_current_project",
            "set_expert_bundle",
            "list_experts",
            "get_product_capabilities",
            "get_job",
        ]
        names, dropped = select_child_tool_names(parent, parent)
        assert names == ["read_file", "get_job"]  # job_inspection reads survive
        assert DELEGATION_TOOL_NAMES == {"delegate_agent"}
        for name in (
            "delegate_agent",
            "wait_agent",
            "message_agent",
            "stop_agent",
            "list_agents",
        ):
            assert dropped[name] == "control plane (delegation)"
        assert dropped["job_complete"] == "control plane (core)"
        assert dropped["send_message"] == "control plane (communication)"
        assert dropped["set_canvas"] == "control plane (canvas)"
        assert dropped["create_job"] == "control plane (job_control)"
        assert "job_inspection" not in CONTROL_PLANE_CATEGORIES

    def test_write_policy_none_strips_the_write_tools(self):
        names, dropped = select_child_tool_names(
            ["read_file", "write_file", "edit_file", "kb_write"],
            ["read_file", "write_file", "edit_file", "kb_write"],
            write_policy="none",
        )
        assert names == ["read_file"]
        assert set(dropped) == {"write_file", "edit_file", "kb_write"}
        assert all(v == "write_policy=none" for v in dropped.values())

    def test_entry_tool_names_flattens_every_group_in_order(self):
        entry = _entry()
        names = entry_tool_names(entry)
        assert names[:3] == ["read_file", "use_skill", "list_files"]
        assert "web_search" in names and "git_log" in names
        assert len(names) == len(set(names))
        assert not (set(names) & DELEGATION_TOOL_NAMES)

    @pytest.mark.asyncio
    async def test_verifier_inherits_safe_inspection_but_not_parent_verdicts(
        self, tmp_path
    ):
        """A dispatched critic's real verifier roster entry crosses the ceiling.

        The critic owns the verdict; its child gets only the non-explicit
        inspection reads shared by the roster allowlist and parent toolset.
        """
        from orchestrator.services.verification_workflow import critic_config_override
        from orchestrator.services.config_resolver import resolve_config
        from shared.runtime.core.loader import (
            get_all_tool_names,
            load_config_from_resolved,
        )
        from shared.runtime.core.tool_policy import expand_category_true
        from agent.tools.registry import TOOL_REGISTRY

        blob = resolve_config(
            base_config_name="critic",
            expert_type="worker",
            request_override=critic_config_override(parent_llm=None),
        )
        parent_names = get_all_tool_names(load_config_from_resolved(blob))
        parent_name_set = set(parent_names)
        verdicts = {"approve_job_verdict", "return_job_with_feedback"}
        assert verdicts <= parent_name_set

        entry = blob["agent"]["subagents"]["roster"]["verifier"]
        ctx, _ = _parent(tmp_path, names=parent_names)
        build = await _build(
            ctx,
            entry,
            handle="verifier-abcd",
            subagent_type="verifier",
        )
        try:
            child_names = set(build.tool_names)
            effective_inspection = {
                name
                for name in child_names
                if TOOL_REGISTRY[name].get("category") == "job_inspection"
            }
            assert effective_inspection == set(expand_category_true("job_inspection"))
            assert effective_inspection
            assert verdicts.isdisjoint(child_names)
            assert not {
                name
                for name in child_names
                if TOOL_REGISTRY[name].get("category") == "evaluation"
            }

            # The category deny is structural too: even a malformed verifier
            # entry asking for the parent's verdict tools cannot cross it.
            _, dropped = select_child_tool_names(
                [*entry_tool_names(entry), *sorted(verdicts)], parent_names
            )
            assert {name: dropped[name] for name in verdicts} == {
                name: "control plane (evaluation)" for name in verdicts
            }
        finally:
            await build.release()


# ---------------------------------------------------------------------------
# The build: shared vs worktree, the copy resets, officer
# ---------------------------------------------------------------------------


class TestBuildShared:
    @pytest.mark.asyncio
    async def test_shared_child_has_fresh_tools_on_a_rerooted_copy(self, tmp_path):
        ctx, root = _parent(tmp_path)
        # Sentinels on every field the copy must reset.
        ctx.todo_manager = object()
        ctx.session_task_manager = object()
        ctx.canvas_event_callback = lambda *a: None
        ctx.progress_committer = object()
        ctx.citation_engine = object()
        ctx._snapshot_callback = lambda p: None
        ctx._freeze_request = {"freeze_type": "parent"}
        ctx._officer_sleep_request = {"x": 1}
        ctx._replan_request = "why"
        ctx._reply_drain_requested = True
        ctx._recent_reads = deque(["a.md"], maxlen=10)
        ctx._pinned_reads = {"todo_guide.md"}
        ctx._recent_read_versions = {"a.md": "sha"}
        ctx._instruction_read_stamps = {"g.md": {"turn": 1}}
        ctx._pending_memories = [{"m": 1}]
        ctx._current_phase = "tactical"
        ctx._current_phase_number = 3
        ctx._llm_config = LLMConfig(model="parent-model")
        ctx.postgres_db = object()
        ctx.vector_db = object()
        ctx.datasources = {"neo4j": object()}
        ctx.knowledge_bindings = [object()]
        ctx.runtime_actor = object()
        ctx.user_id = "user-1"
        ctx._thread_id = "thread-1"

        build = await _build(ctx, _entry())
        child = build.tool_context
        assert child is not ctx
        # explorer is read-only: it declares write_policy: none (the entry
        # grants no workspace mutator, so the stated policy matches the grant).
        assert build.isolation == "shared" and build.write_policy == "none"
        assert build.tool_names == ["read_file", "list_files", "search_files"]
        assert {t.name for t in build.tools} == set(child._resolved_tool_names)
        # Fresh objects: the parent's names stay the parent's.
        assert ctx._resolved_tool_names[0] == "read_file"
        # Reset
        for name in (
            "todo_manager",
            "session_task_manager",
            "canvas_event_callback",
            "progress_committer",
            "citation_engine",
            "_snapshot_callback",
            "_freeze_request",
            "_officer_sleep_request",
            "_replan_request",
            "_current_phase",
            "_current_phase_number",
        ):
            assert getattr(child, name) is None, name
        assert child._reply_drain_requested is False
        assert list(child._recent_reads) == [] and child._pinned_reads == set()
        assert (
            child._recent_read_versions == {} and child._instruction_read_stamps == {}
        )
        assert child._pending_memories == []
        assert child._recent_reads is not ctx._recent_reads
        # Replaced with the child's
        assert child._llm_config is build.config.llm
        assert child._llm_config.model == "gpt-4o-mini"
        assert child._limits is build.config.limits
        assert child.config["agent_id"] == "explorer"
        assert "delegation" not in child.config and "subagents" not in child.config
        assert child.config["cloud_mount"] == {
            "active": False
        }  # runtime facts ride along
        assert child.workspace_manager is not ctx.workspace_manager
        assert child.workspace_manager.backend is ctx.workspace_manager.backend
        # Shared by reference
        for name in (
            "_job_metadata",
            "orchestrator_client",
            "postgres_db",
            "vector_db",
            "datasources",
            "knowledge_bindings",
            "runtime_actor",
        ):
            assert getattr(child, name) is getattr(ctx, name), name
        assert child.user_id == "user-1" and child._thread_id == "thread-1"
        assert child.job_id == "parent-job"
        # The parent's own state is untouched.
        assert ctx._freeze_request == {"freeze_type": "parent"}
        assert ctx._pinned_reads == {"todo_guide.md"}
        await build.release()

    @pytest.mark.asyncio
    async def test_shared_child_git_is_read_only_and_writes_land_in_the_root(
        self, tmp_path
    ):
        ctx, root = _parent(tmp_path, git=True)
        entry = _entry(
            tools={"workspace": ["read_file", "write_file"]}, write_policy="full"
        )
        build = await _build(ctx, entry)
        ws = build.workspace_manager
        assert isinstance(ws.git_manager, ReadOnlyGitManager)
        assert ws.git_manager.is_active
        assert ws.git_manager.has_uncommitted_changes() is False
        before = _git(["rev-list", "--count", "HEAD"], root).stdout.strip()
        ws.write_file("out.md", "child")
        assert (root / "out.md").read_text() == "child"
        assert ws.git_manager.commit("nope") is False
        assert ws.git_manager.push() is False
        assert ws.git_manager.has_unpushed_commits() is False
        assert _git(["rev-list", "--count", "HEAD"], root).stdout.strip() == before
        # reads pass through
        assert ws.git_manager.get_current_commit()
        assert ctx.workspace_manager.git_manager.has_uncommitted_changes() is True
        await build.release()

    @pytest.mark.asyncio
    async def test_officer_is_never_enabled_on_a_child(self, tmp_path):
        ctx, _ = _parent(tmp_path)
        entry = _entry()
        # A DB $ref to a centurion would carry this; the overlay prunes it but
        # the build asserts it regardless.
        entry["officer"] = {"enabled": True, "sleep_min_minutes": 1}
        build = await _build(ctx, entry)
        assert build.config.officer.enabled is False
        await build.release()

    @pytest.mark.asyncio
    async def test_ref_child_drops_only_missing_before_tool_artifacts(
        self, tmp_path, caplog
    ):
        """A foreign expert's enforced gate cannot wedge a child when this
        parent's workspace never deployed the bound skill artifact."""
        ctx, root = _parent(tmp_path)
        present = root / "skills" / "present-guide"
        present.mkdir(parents=True)
        (present / "SKILL.md").write_text("# present\n")
        entry = _entry(
            name="reviewer",
            ref="critic",
            instruction_files=[
                {
                    "skill": "missing-critic-guide",
                    "trigger": "before_tool:read_file",
                    "enforce": True,
                },
                {
                    "skill": "present-guide",
                    "trigger": "before_tool:read_file",
                    "enforce": True,
                },
                {
                    "skill": "missing-phase-guide",
                    "trigger": "phase_start:tactical",
                    "enforce": False,
                },
            ],
        )
        with caplog.at_level(logging.WARNING):
            build = await _build(
                ctx,
                entry,
                handle="reviewer-0001",
                subagent_type="reviewer",
            )
        assert [item.path for item in build.config.instruction_files] == [
            "skills/present-guide/SKILL.md",
            "skills/missing-phase-guide/SKILL.md",
        ]
        assert [item.path for item in build.tool_context._instruction_files] == [
            "skills/present-guide/SKILL.md",
            "skills/missing-phase-guide/SKILL.md",
        ]
        records = [
            record
            for record in caplog.records
            if "dropped before_tool binding(s)" in record.getMessage()
        ]
        assert len(records) == 1
        assert "missing-critic-guide" in records[0].getMessage()
        assert "present-guide" not in records[0].getMessage()
        await build.release()

    @pytest.mark.asyncio
    async def test_shared_child_shell_gets_prefixed_tabs(self, tmp_path):
        class ShellBackend(FilesystemTestBackend):
            supports_shell = True

            def __init__(self, root):
                super().__init__(root)
                self.tabs: list[str] = []

            def shell_run(
                self, command, timeout=None, tab_name="default", working_dir=None
            ):
                self.tabs.append(tab_name)
                return f"ran {command} in {working_dir!r}"

            def shell_ensure_tab(self, name):
                self.tabs.append(name)

            def shell_list_tabs(self):
                return [{"name": t} for t in self.tabs]

            def shell_close_tab(self, name):
                self.tabs.remove(name)
                return "closed"

        root = tmp_path / "ws"
        root.mkdir()
        backend = ShellBackend(root)
        ws = WorkspaceManager(
            job_id="parent-job",
            config=WorkspaceManagerConfig(
                structure=[], base_path=str(root), git_versioning=False
            ),
            backend=backend,
        )
        ws._initialized = True
        ctx = ToolContext(
            workspace_manager=ws,
            config={"shell": {}},
            _resolved_tool_names=["read_file", "shell_execute", "run_command"],
        )
        entry = _entry(tools={"shell": ["shell_execute", "run_command"]})
        build = await _build(ctx, entry, handle="probe-1a2b")
        child_shell = build.tool_context.shell_manager
        assert child_shell is not None and child_shell is not ctx.shell_manager
        assert isinstance(build.shell_backend, SharedTreeShellBackend)
        assert build.shell_backend.root == backend.root  # no re-rooting
        out = build.shell_backend.shell_run("ls", tab_name="default", working_dir="src")
        assert out == "ran ls in 'src'"
        assert backend.tabs == ["1a2b-default"]
        assert build.shell_backend.shell_list_tabs() == [{"name": "default"}]
        await build.release()
        assert backend.tabs == []  # only the child's tabs were closed


class TestChildTabNames:
    """The child tab prefix must satisfy the REAL transport validator.

    The prefixed-tab test above drives a permissive filesystem backend, so it
    passed while every shell-capable child was broken on the cluster with
    ``Invalid tab name 'implementer-1cb8__default'`` (U3 WP7, job dde11ae6).
    This pins the produced name against RemoteBackend's own pattern.
    """

    @pytest.mark.parametrize(
        "handle",
        [
            "implementer-1cb8",
            "tester-afa0",
            "explorer-bbf4",
            "reviewer-0001",
            "verifier-dead",
            "reader-9f9f",
            "probe-1a2b",
            "weird handle!!",
            "x",
        ],
    )
    @pytest.mark.parametrize("tab", ["default", "build", "a" * 15])
    def test_prefixed_tab_matches_the_transport_pattern(self, handle, tab):
        from shared.runtime.core.backends.remote import TAB_NAME_PATTERN
        from agent.subagents.child import child_tab_prefix

        name = f"{child_tab_prefix(handle)}{tab}"
        assert TAB_NAME_PATTERN.fullmatch(name), name

    def test_prefix_is_short_enough_to_leave_the_child_a_usable_budget(self):
        from shared.runtime.core.backends.remote import TAB_NAME_PATTERN
        from agent.subagents.child import child_tab_prefix

        prefix = child_tab_prefix("implementer-1cb8")
        assert len(prefix) == 5
        # 20 - 5 leaves 15 for the child's own name; one more must fail.
        assert TAB_NAME_PATTERN.fullmatch(prefix + "a" * 15)
        assert not TAB_NAME_PATTERN.fullmatch(prefix + "a" * 16)

    def test_distinct_children_get_distinct_prefixes(self):
        from agent.subagents.child import child_tab_prefix

        assert child_tab_prefix("implementer-1cb8") != child_tab_prefix("tester-afa0")


class TestBuildWorktree:
    @pytest.mark.asyncio
    async def test_worktree_child_is_rooted_in_its_own_worktree(self, tmp_path):
        ctx, root = _parent(tmp_path, git=True)
        entry = _entry(
            tools={"workspace": ["read_file", "write_file"]},
            isolation="worktree",
            write_policy="full",
        )
        build = await _build(ctx, entry, handle="implementer-7f3a")
        assert build.isolation == "worktree"
        assert build.worktree_path == ".worktrees/implementer-7f3a"
        assert (root / ".worktrees" / "implementer-7f3a").is_dir()
        assert (
            build.reader_env is not None
            and build.reader_env.branch == "sub/implementer-7f3a"
        )
        ws = build.workspace_manager
        assert isinstance(ws.git_manager, NoPushGitManager)
        ws.write_file("notes.md", "from the worktree")
        assert (root / ".worktrees" / "implementer-7f3a" / "notes.md").read_text() == (
            "from the worktree"
        )
        assert not (root / "notes.md").exists()
        # commits stay on the worktree branch; push is a no-op
        assert ws.git_manager.has_uncommitted_changes() is True
        assert ws.git_manager.commit("child commit") is True
        assert ws.git_manager.has_unpushed_commits() is False
        assert ws.git_manager.push() is False
        assert _git(["branch", "--list", "sub/implementer-7f3a"], root).stdout.strip()
        # The child's tools are fresh objects on the worktree context.
        assert build.tool_names == ["read_file", "write_file"]
        write_tool = next(t for t in build.tools if t.name == "write_file")
        await write_tool.ainvoke({"path": "via_tool.md", "content": "x"})
        assert (root / ".worktrees" / "implementer-7f3a" / "via_tool.md").exists()
        assert not (root / "via_tool.md").exists()
        await build.release()
        assert not (root / ".worktrees" / "implementer-7f3a").exists()

    @pytest.mark.asyncio
    async def test_revival_reuses_exact_branch_and_transport_safe_tab_prefix(
        self, tmp_path
    ):
        from shared.runtime.core.backends.remote import TAB_NAME_PATTERN

        ctx, root = _parent(tmp_path, git=True)
        entry = _entry(
            tools={"workspace": ["read_file", "write_file"]},
            isolation="worktree",
            write_policy="full",
        )
        first = await _build(ctx, entry, handle="implementer-7f3a")
        first.workspace_manager.write_file("revival.txt", "durable branch")
        assert first.workspace_manager.git_manager.commit("child state") is True
        await first.release()

        revived = await _build(
            ctx,
            entry,
            handle="implementer-7f3a",
            reuse_worktree=True,
        )
        assert revived.workspace_manager.read_file("revival.txt") == "durable branch"
        assert revived.reader_env.branch == "sub/implementer-7f3a"
        assert revived.reader_env._subdir_backend._tab_prefix == "7f3a-"
        assert TAB_NAME_PATTERN.fullmatch(
            revived.reader_env._subdir_backend._tab("default")
        )
        await revived.release()
        assert not (root / ".worktrees" / "implementer-7f3a").exists()

    @pytest.mark.asyncio
    async def test_revival_never_falls_back_to_scratch_without_git(self, tmp_path):
        ctx, _ = _parent(tmp_path, git=False)
        with pytest.raises(RuntimeError, match="cannot be reconstructed"):
            await _build(
                ctx,
                _entry(isolation="worktree"),
                handle="explorer-7f3a",
                reuse_worktree=True,
            )

    @pytest.mark.asyncio
    async def test_unknown_isolation_is_refused(self, tmp_path):
        ctx, _ = _parent(tmp_path)
        with pytest.raises(SpawnRefused, match="isolation"):
            await _build(ctx, _entry(), isolation="container")


# ---------------------------------------------------------------------------
# LLM: inherit the LIVE parent config vs a pinned model
# ---------------------------------------------------------------------------


class TestChildLLM:
    def test_inherit_entry_runs_on_the_parents_live_config(self):
        entry = _entry()
        assert entry["llm"][ROSTER_INHERIT_MARKER] is True
        live = LLMConfig(
            model="gpt-4o-mini",
            provider="openai",
            api_key="sk-dispatch-injected",
            base_url="https://live.example/v1",
        )
        cfg = build_child_config(entry, live_llm_config=live)
        assert cfg.agent_id == "explorer"
        assert cfg.llm.model == "gpt-4o-mini"
        assert cfg.llm.api_key == "sk-dispatch-injected"
        assert cfg.llm.base_url == "https://live.example/v1"
        # the entry's own params (temperature etc.) are kept
        assert cfg.llm.temperature == entry["llm"].get(
            "temperature", cfg.llm.temperature
        )

    def test_inherit_follows_a_live_model_swap_and_drops_stale_transport(self):
        entry = _entry()
        live = LLMConfig(
            model="claude-sonnet-4-5", provider="anthropic", api_key="sk-ant"
        )
        cfg = build_child_config(entry, live_llm_config=live)
        assert cfg.llm.model == "claude-sonnet-4-5"
        assert cfg.llm.provider == "anthropic"
        assert cfg.llm.base_url is None  # the old router would misroute
        assert cfg.llm.api_key == "sk-ant"

    def test_pinned_entry_keeps_its_model_and_borrows_the_key_on_the_same_provider(
        self,
    ):
        entry = _entry(llm={"model": "gpt-4.1-nano", "provider": "openai"})
        assert not entry["llm"].get(ROSTER_INHERIT_MARKER)
        live = LLMConfig(model="gpt-4o-mini", provider="openai", api_key="sk-live")
        cfg = build_child_config(entry, live_llm_config=live)
        assert cfg.llm.model == "gpt-4.1-nano"
        assert cfg.llm.api_key == "sk-live"

    def test_pinned_entry_on_another_provider_borrows_nothing(self):
        entry = _entry(llm={"model": "claude-haiku-4-5", "provider": "anthropic"})
        live = LLMConfig(model="gpt-4o-mini", provider="openai", api_key="sk-live")
        cfg = build_child_config(entry, live_llm_config=live)
        assert cfg.llm.model == "claude-haiku-4-5"
        assert cfg.llm.api_key is None

    def test_pinned_entry_with_its_own_endpoint_does_not_borrow_the_key(self):
        # A key follows its endpoint: same provider is not enough when the
        # entry names a different host than the parent.
        entry = _entry(
            llm={
                "model": "gpt-4.1-nano",
                "provider": "openai",
                "base_url": "https://elsewhere.example/v1",
            }
        )
        live = LLMConfig(
            model="gpt-4o-mini",
            provider="openai",
            api_key="sk-live",
            base_url="https://live.example/v1",
        )
        cfg = build_child_config(entry, live_llm_config=live)
        assert cfg.llm.base_url == "https://elsewhere.example/v1"
        assert cfg.llm.api_key is None

    def test_pinned_entry_on_the_parents_endpoint_still_borrows(self):
        # Same origin, different spelling (case, trailing slash) still matches.
        entry = _entry(
            llm={
                "model": "gpt-4.1-nano",
                "provider": "openai",
                "base_url": "https://LIVE.example/v1/",
            }
        )
        live = LLMConfig(
            model="gpt-4o-mini",
            provider="openai",
            api_key="sk-live",
            base_url="https://live.example/v1",
        )
        cfg = build_child_config(entry, live_llm_config=live)
        assert cfg.llm.api_key == "sk-live"

    def test_overlay_live_llm_same_model_copies_non_none_only(self):
        child = LLMConfig(model="m", provider="p", base_url="https://keep", api_key="k")
        live = LLMConfig(model="m", provider="p", base_url=None, api_key="k2")
        out = overlay_live_llm(child, live)
        assert out.base_url == "https://keep" and out.api_key == "k2"
        assert overlay_live_llm(child, None) is child

    def test_child_tool_config_identity(self):
        cfg = build_child_config(_entry(), live_llm_config=LLMConfig(**_PARENT_LLM))
        tc = child_tool_config(
            cfg,
            {
                "agent_id": "developer",
                "delegation": {"x": 1},
                "subagents": {},
                "cloud_mount": 1,
            },
        )
        assert tc["agent_id"] == "explorer"
        assert tc["cloud_mount"] == 1
        assert "delegation" not in tc and "subagents" not in tc
        assert tc["model_max_context_tokens"] == cfg.limits.model_max_context_tokens
        assert tc["tags"] == ["subagent"]

    def test_rebase_resets_current_phase(self):
        ctx = ToolContext(config={"a": 1})
        ctx._current_phase = "strategic"
        cfg = build_child_config(_entry(), live_llm_config=LLMConfig(**_PARENT_LLM))
        rebase_context(
            ctx,
            cfg=cfg,
            tool_config={"b": 2},
            workspace_manager=None,
            shell_manager=None,
        )
        assert ctx._current_phase is None and ctx.config == {"b": 2}


# ---------------------------------------------------------------------------
# Write policy + the single shared writer
# ---------------------------------------------------------------------------


class TestWritePolicy:
    def test_path_allowed_globs(self):
        globs = ["src/pkg/**", "tests/test_pkg.py", "docs/"]
        assert path_allowed("src/pkg/a.py", globs)
        assert path_allowed("src/pkg/deep/b.py", globs)
        assert path_allowed("src/pkg", globs)  # create_directory on the dir itself
        assert path_allowed("tests/test_pkg.py", globs)
        assert path_allowed("docs/x.md", globs)
        assert not path_allowed("src/other.py", globs)
        assert not path_allowed("tests/test_other.py", globs)
        assert not path_allowed("../escape.py", globs)
        assert not path_allowed("/abs/src/pkg/a.py", globs)

    def test_violation_messages(self):
        globs = [".subagents/h/**"]
        assert (
            write_policy_violation(
                "write_file",
                {"path": ".subagents/h/r.md"},
                policy="scratch_only",
                globs=globs,
            )
            is None
        )
        msg = write_policy_violation(
            "write_file", {"path": "src/x.py"}, policy="scratch_only", globs=globs
        )
        assert msg.startswith(
            "Error: src/x.py is outside this subagent's owned paths [.subagents/h/**]"
        )
        msg = write_policy_violation(
            "move_file",
            {"source": ".subagents/h/a", "dest": "b"},
            policy="scratch_only",
            globs=globs,
        )
        assert "b is outside" in msg
        msg = write_policy_violation(
            "rename_file",
            {"path": ".subagents/h/a", "new_name": "../b"},
            policy="scratch_only",
            globs=globs,
        )
        assert msg is not None
        msg = write_policy_violation(
            "kb_write", {"title": "t"}, policy="owned_paths", globs=["src/**"]
        )
        assert msg.startswith(
            "Error: kb_write is not allowed under write_policy=owned_paths"
        )

    @pytest.mark.asyncio
    async def test_scratch_only_wraps_the_write_tools(self, tmp_path):
        ctx, root = _parent(tmp_path)
        entry = _entry(
            tools={
                "workspace": [
                    "read_file",
                    "write_file",
                    "create_directory",
                    "delete_file",
                ]
            },
            write_policy="scratch_only",
        )
        build = await _build(ctx, entry, handle="probe-0001")
        assert build.write_policy == "scratch_only" and build.writes_enabled
        write_tool = next(t for t in build.tools if t.name == "write_file")
        out = await write_tool.ainvoke({"path": "README.md", "content": "clobbered"})
        assert out.startswith("Error: README.md is outside this subagent's owned paths")
        assert "[.subagents/probe-0001/**]" in out
        assert (root / "README.md").read_text() == "# parent\n"
        out = await write_tool.ainvoke(
            {"path": ".subagents/probe-0001/notes.md", "content": "ok"}
        )
        assert "Error" not in out
        assert (root / ".subagents" / "probe-0001" / "notes.md").read_text() == "ok"
        read_tool = next(t for t in build.tools if t.name == "read_file")
        assert "parent" in await read_tool.ainvoke(
            {"path": "README.md"}
        )  # reads unaffected
        await build.release()

    @pytest.mark.asyncio
    async def test_owned_paths_from_the_spawn(self, tmp_path):
        ctx, root = _parent(tmp_path)
        (root / "src").mkdir()
        entry = _entry(
            tools={"workspace": ["read_file", "write_file"]}, write_policy="owned_paths"
        )
        build = await _build(
            ctx,
            entry,
            handle="implementer-0001",
            owned_paths=["src/feature/**", "tests/test_feature.py"],
        )
        assert build.owned_paths == ["src/feature/**", "tests/test_feature.py"]
        write_tool = next(t for t in build.tools if t.name == "write_file")
        assert "Error" not in await write_tool.ainvoke(
            {"path": "src/feature/a.py", "content": "x"}
        )
        assert "Error" not in await write_tool.ainvoke(
            {"path": "tests/test_feature.py", "content": "y"}
        )
        out = await write_tool.ainvoke({"path": "src/other.py", "content": "z"})
        assert out.startswith(
            "Error: src/other.py is outside this subagent's owned paths"
        )
        assert not (root / "src" / "other.py").exists()
        await build.release()

    @pytest.mark.asyncio
    async def test_owned_paths_without_globs_is_refused(self, tmp_path):
        ctx, _ = _parent(tmp_path)
        entry = _entry(
            tools={"workspace": ["read_file", "write_file"]}, write_policy="owned_paths"
        )
        with pytest.raises(SpawnRefused, match="owned_paths"):
            await _build(ctx, entry)

    @pytest.mark.asyncio
    async def test_none_strips_and_full_wraps_nothing(self, tmp_path):
        ctx, _ = _parent(tmp_path)
        entry = _entry(
            tools={"workspace": ["read_file", "write_file"]}, write_policy="none"
        )
        build = await _build(ctx, entry)
        assert build.tool_names == ["read_file"] and not build.writes_enabled
        assert build.dropped_tools["write_file"] == "write_policy=none"
        await build.release()
        entry = _entry(
            tools={"workspace": ["read_file", "write_file"]}, write_policy="full"
        )
        build = await _build(ctx, entry)
        write_tool = next(t for t in build.tools if t.name == "write_file")
        assert "Error" not in await write_tool.ainvoke(
            {"path": "anywhere.md", "content": "x"}
        )
        await build.release()

    def test_apply_write_policy_wraps_sync_and_async_bindings(self):
        calls = []

        @tool
        def write_file(path: str, content: str) -> str:
            """sync write"""
            calls.append(("sync", path))
            return "ok"

        @tool
        async def edit_file(
            path: str, old_string: str = "", new_string: str = ""
        ) -> str:
            """async edit"""
            calls.append(("async", path))
            return "ok"

        tools = apply_write_policy(
            [write_file, edit_file],
            policy="owned_paths",
            handle="h",
            owned_paths=["ok/**"],
        )
        assert write_file.invoke({"path": "ok/a", "content": "c"}) == "ok"
        assert write_file.invoke({"path": "no/a", "content": "c"}).startswith("Error:")
        assert calls == [("sync", "ok/a")]
        with pytest.raises(SpawnRefused):
            apply_write_policy(tools, policy="bogus", handle="h")

    @pytest.mark.asyncio
    async def test_single_shared_writer_guard(self, tmp_path):
        ctx, _ = _parent(tmp_path)
        guard = SharedWriterGuard()
        writer = _entry(
            tools={"workspace": ["read_file", "write_file"]}, write_policy="full"
        )
        first = await _build(ctx, writer, handle="implementer-0001", writer_guard=guard)
        assert guard.active_writer == "implementer-0001"
        # A second shared writer is refused ...
        with pytest.raises(SpawnRefused, match="isolation=worktree"):
            await _build(ctx, writer, handle="implementer-0002", writer_guard=guard)
        assert guard.active_writer == "implementer-0001"
        # ... a reader is fine ...
        reader = await _build(ctx, _entry(), handle="explorer-0001", writer_guard=guard)
        await reader.release()
        # ... and a worktree writer too.
        wt = _entry(
            tools={"workspace": ["read_file", "write_file"]},
            isolation="worktree",
            write_policy="full",
        )
        second = await _build(ctx, wt, handle="implementer-0003", writer_guard=guard)
        await second.release()
        assert guard.active_writer == "implementer-0001"
        await first.release()
        assert guard.active_writer is None
        third = await _build(ctx, writer, handle="implementer-0004", writer_guard=guard)
        assert guard.active_writer == "implementer-0004"
        await third.release()

    def test_write_tools_cover_the_registered_mutators(self):
        from agent.tools.registry import TOOL_REGISTRY

        registered = {
            n for n, m in TOOL_REGISTRY.items() if m.get("category") == "workspace"
        }
        readers = {
            "read_file",
            "list_files",
            "search_files",
            "file_exists",
            "get_document_info",
            "use_skill",
        }
        assert registered - readers <= WRITE_TOOLS


# ---------------------------------------------------------------------------
# The parent-side stashes never reach a child (U3 WP2)
# ---------------------------------------------------------------------------


def test_rebase_context_resets_the_parent_side_subagent_stashes():
    """A child is never a parent: the runtime, host, fork seed, probe,
    admission fence and audit stamp are cleared on the copy (depth 1)."""
    import copy

    from shared.runtime.core.loader import load_agent_config_from_dict

    parent = ToolContext(
        config={"agent_id": "developer"},
        _job_metadata={"job_id": "parent-job"},
    )
    parent.subagent_runtime = object()
    parent._parent_host = object()
    parent._subagent_parent_kind = "session"
    parent._session_parent_authority_provider = lambda: object()
    parent._session_parent_authority = object()
    parent.parent_context_probe = lambda: None
    parent.provider_admission = lambda: True
    parent._fork_source = [object()]
    parent._parent_audit_metadata = {"job_id": "parent-job"}
    child = copy.copy(parent)
    cfg = load_agent_config_from_dict(
        {"agent_id": "explorer", "display_name": "Explorer", "llm": dict(_PARENT_LLM)}
    )
    rebase_context(
        child,
        cfg=cfg,
        tool_config={"agent_id": "explorer"},
        workspace_manager=None,
        shell_manager=None,
    )
    for name in (
        "subagent_runtime",
        "_parent_host",
        "_subagent_parent_kind",
        "_session_parent_authority_provider",
        "_session_parent_authority",
        "parent_context_probe",
        "provider_admission",
        "_fork_source",
        "_parent_audit_metadata",
    ):
        assert getattr(child, name) is None, name
        assert getattr(parent, name) is not None, name
