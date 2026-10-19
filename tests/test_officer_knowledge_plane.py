"""Officer knowledge plane — K1/K2/K3 of knowledge-base/knowledge/features/officer_knowledge_plane.md.

Covers the three shipped slices:

- **K1** commission/attach project-binding invariant (exactly one project,
  exactly one matching native writable KnowledgeBinding, externals read-only,
  no override can replace the write target) plus degraded availability: a
  vector/KB outage never kills the officer — KB tools fail closed with a clear
  `project knowledge unavailable` error and the wake sitrep says so.
- **K2** the Centurion expert's explicit eleven-tool knowledge grant, without
  ``kb_export`` (the snapshot test pins the fixture; this file pins intent).
- **K3** the background-officer capability ceiling: object-plane tools are
  suppressed regardless of overrides or backend capability, while conferences
  (``officer.conference`` with ``enabled: False``) stay ordinary sessions.

Officer gates are strict (``is True``) so the MagicMock-config cases here are
regression tests, not conveniences — see tests/test_officer_substrate.py.
"""

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.api.persistent_session import (
    OfficerKnowledgeBindingError,
    PersistentSession,
)
from shared.runtime.core.loader import DelegationConfig, OfficerConfig, SubagentsConfig
from agent.services.knowledge.bindings import KnowledgeBinding, build_knowledge_bindings
from agent.tools.registry import (
    apply_officer_tool_ceiling,
    officer_ceiling_active,
)

from orchestrator.database.postgres import JobQueryResult

PROJECT_A = str(uuid.uuid4())
PROJECT_B = str(uuid.uuid4())
EXTERNAL_KB = str(uuid.uuid4())

#: The reviewed grant (officer_knowledge_plane.md §3) — exactly these eleven
#: (kb_delete added by kb_gardening G1; kb_grep added by kb_retrieval_hardening T7).
OFFICER_KB_TOOLS = [
    "kb_write",
    "kb_update",
    # kb_gardening G1: retire (archive with a reason) — reversible, guarded.
    "kb_delete",
    "kb_read",
    "kb_list",
    "kb_search",
    "kb_grep",
    "kb_related",
    "kb_contradictions",
    "kb_provenance",
    "kb_unanswered",
]


# ---------------------------------------------------------------------------
# Helpers (pattern from tests/test_persistent_session.py — config.extra must
# be a real dict, delegation a real dataclass)
# ---------------------------------------------------------------------------


def _make_config(**overrides):
    cfg = MagicMock()
    cfg.interactive.permission_mode = "read_only"
    cfg.workspace.backend = None
    cfg.workspace.structure = {}
    cfg.workspace.git_versioning = False
    cfg.llm.model = "gpt-4o"
    cfg.llm.multimodal = True
    cfg.llm.parallel_tool_calls = False
    cfg.memory.enabled = overrides.get("memory_enabled", False)
    cfg.memory.required = overrides.get("memory_required", False)
    cfg.memory.manager_enabled = overrides.get("manager_enabled", False)
    cfg.memory.pipeline.scorers = overrides.get("memory_scorers", [])
    cfg.memory.pipeline.retrievers = overrides.get("memory_retrievers", [])
    cfg.memory.observer_interval = 5
    cfg.extra = overrides.get("extra", {})
    cfg.agent_id = overrides.get("agent_id", "centurion")
    cfg.instruction_files = []
    cfg.tools = MagicMock()
    cfg.delegation = DelegationConfig()
    # U1 WP3: `subagents` / `tags` are parsed fields the session asdict()s
    # into tool_config next to `delegation`.
    cfg.subagents = SubagentsConfig()
    cfg.tags = []
    if "officer" in overrides:
        cfg.officer = overrides["officer"]
    return cfg


def _make_session(**overrides):
    return PersistentSession(
        thread_id=overrides.pop("thread_id", str(uuid.uuid4())),
        config=overrides.pop("config", _make_config()),
        **overrides,
    )


def _native_binding(project_id: str, *, writable: bool = True) -> KnowledgeBinding:
    return KnowledgeBinding(
        kb_id=uuid.UUID(project_id),
        alias="project",
        name="Project Knowledge",
        kind="native",
        writable=writable,
        root_path="knowledge",
    )


def _external_binding(kb_id: str, *, writable: bool = False) -> KnowledgeBinding:
    return KnowledgeBinding(
        kb_id=uuid.UUID(kb_id),
        alias="handbook",
        name="Handbook",
        kind="datasource",
        writable=writable,
    )


# ---------------------------------------------------------------------------
# K1 — commission/attach project-binding invariant
# ---------------------------------------------------------------------------


class TestOfficerBindingInvariant:
    def _officer_session(self, project_ids, bindings):
        cfg = _make_config(officer=OfficerConfig(enabled=True))
        return _make_session(
            config=cfg,
            project_ids=list(project_ids),
            knowledge_bindings=list(bindings),
        )

    def test_zero_projects_refused(self):
        session = self._officer_session([], [])
        with pytest.raises(OfficerKnowledgeBindingError, match="exactly one"):
            session._enforce_officer_knowledge_invariant()

    def test_multiple_projects_refused(self):
        session = self._officer_session(
            [PROJECT_A, PROJECT_B],
            build_knowledge_bindings(project_ids=[PROJECT_A, PROJECT_B]),
        )
        with pytest.raises(OfficerKnowledgeBindingError, match="exactly one"):
            session._enforce_officer_knowledge_invariant()

    def test_single_project_native_writable_passes(self):
        session = self._officer_session(
            [PROJECT_A], build_knowledge_bindings(project_ids=[PROJECT_A])
        )
        session._enforce_officer_knowledge_invariant()  # no raise

    def test_external_read_only_alongside_native_passes(self):
        bindings = [
            _native_binding(PROJECT_A),
            _external_binding(EXTERNAL_KB, writable=False),
        ]
        session = self._officer_session([PROJECT_A], bindings)
        session._enforce_officer_knowledge_invariant()  # no raise

    def test_no_writable_binding_refused(self):
        session = self._officer_session(
            [PROJECT_A], [_native_binding(PROJECT_A, writable=False)]
        )
        with pytest.raises(OfficerKnowledgeBindingError, match="native writable"):
            session._enforce_officer_knowledge_invariant()

    def test_two_writable_bindings_refused(self):
        session = self._officer_session(
            [PROJECT_A],
            [_native_binding(PROJECT_A), _native_binding(PROJECT_B)],
        )
        with pytest.raises(OfficerKnowledgeBindingError, match="native writable"):
            session._enforce_officer_knowledge_invariant()

    def test_forged_writable_external_refused(self):
        """An override that smuggled a writable external cannot replace the
        write target — the attach refuses the whole session."""
        session = self._officer_session(
            [PROJECT_A],
            [
                _native_binding(PROJECT_A, writable=False),
                _external_binding(EXTERNAL_KB, writable=True),
            ],
        )
        with pytest.raises(OfficerKnowledgeBindingError, match="read-only"):
            session._enforce_officer_knowledge_invariant()

    def test_writable_binding_for_wrong_project_refused(self):
        session = self._officer_session([PROJECT_A], [_native_binding(PROJECT_B)])
        with pytest.raises(OfficerKnowledgeBindingError, match="write target"):
            session._enforce_officer_knowledge_invariant()

    def test_legacy_construction_without_bindings_passes_on_single_project(self):
        # Knowledge tools synthesize the same first-native-writable binding
        # from project_ids, so a binding-less single-project attach is sound.
        session = self._officer_session([PROJECT_A], [])
        session._enforce_officer_knowledge_invariant()  # no raise

    def test_conference_is_not_gated(self):
        cfg = _make_config(officer=OfficerConfig(enabled=False, conference=True))
        session = _make_session(config=cfg, project_ids=[], knowledge_bindings=[])
        session._enforce_officer_knowledge_invariant()  # no raise

    def test_magicmock_officer_config_never_trips_the_gate(self):
        # _make_config without an explicit officer leaves cfg.officer a
        # MagicMock; the strict `is True` gate must treat that as disabled.
        session = _make_session(project_ids=[], knowledge_bindings=[])
        session._enforce_officer_knowledge_invariant()  # no raise

    @pytest.mark.asyncio
    async def test_setup_fails_before_any_resource_when_misbound(self):
        """The invariant runs as step 0 of setup — a mis-bound officer never
        creates a workspace."""
        cfg = _make_config(officer=OfficerConfig(enabled=True))
        session = _make_session(config=cfg, project_ids=[])
        with patch.object(session, "_setup_workspace", new=AsyncMock()) as ws:
            with pytest.raises(OfficerKnowledgeBindingError):
                await session.setup(llm=MagicMock())
            ws.assert_not_awaited()


class TestBuildKnowledgeBindingsGuarantees:
    """Verify what the doc says build_knowledge_bindings already holds."""

    def test_first_native_is_sole_writable(self):
        bindings = build_knowledge_bindings(project_ids=[PROJECT_A, PROJECT_B])
        assert [b.writable for b in bindings] == [True, False]
        assert all(b.is_native for b in bindings)

    def test_external_datasources_are_always_read_only(self):
        # Even a datasource payload that CLAIMS writability comes out read-only.
        bindings = build_knowledge_bindings(
            project_ids=[PROJECT_A],
            datasources=[
                {
                    "type": "kb",
                    "id": EXTERNAL_KB,
                    "name": "Handbook",
                    "config": {"writable": True},
                }
            ],
        )
        external = [b for b in bindings if not b.is_native]
        assert len(external) == 1
        assert external[0].writable is False
        writable = [b for b in bindings if b.writable]
        assert len(writable) == 1
        assert str(writable[0].kb_id) == PROJECT_A


# ---------------------------------------------------------------------------
# K3 — background-officer capability ceiling (pure function)
# ---------------------------------------------------------------------------

OBJECT_PLANE_SAMPLE = [
    # workspace / shell / git / browser / canvas / repo / webdav categories
    "read_file",
    "write_file",
    "edit_file",
    "list_files",
    "search_files",
    "use_skill",
    "run_command",
    "shell_execute",
    "cancel_command",
    "git_diff",
    "git_log",
    "browser_navigate",
    "browser_snapshot",
    "set_canvas",
    "get_canvas",
    "repo_push",
    "repo_commit",
    "webdav_read",
    "webdav_write",
    # explicit denied names
    "request_workspace_upgrade",
    "checkout_project_repository",
    "list_project_repositories",
    "get_default_project_repository",
    "srw_cloud_status",
    "kb_export",
    # officer_supervision_surface E2/§3.4: job tools on the job_workspace
    # plane are the object plane in job-tool form — the ceiling now denies
    # them by registry plane metadata even when a config names them.
    "get_job_file",
    "list_job_files",
    "get_workspace_overview",
    "get_shell_state",
    "get_job_diff",
    "list_job_commits",
]

CONTROL_AND_KNOWLEDGE_SAMPLE = [
    *OFFICER_KB_TOOLS,
    "create_job",
    "approve_job",
    "cancel_job",
    "pause_job",
    "resume_job_with_feedback",
    "steer_job",
    "list_jobs",
    "get_job",
    # E4 evidence reads are the bounded replacement for file browsing.
    "get_job_completion_report",
    "list_job_evidence",
    "read_job_evidence",
    "get_stuck_jobs",
    "get_session_context",
    "get_current_project",
    "list_project_jobs",
    "sleep",
    "notify_user",
    "send_message",
    "task_add",
    "task_complete",
    "task_list",
    "delegate_agent",
    "web_search",
]


class TestOfficerToolCeiling:
    def test_background_officer_loses_every_object_plane_tool(self):
        names = [*OBJECT_PLANE_SAMPLE, *CONTROL_AND_KNOWLEDGE_SAMPLE]
        result = apply_officer_tool_ceiling(names, OfficerConfig(enabled=True))
        for denied in OBJECT_PLANE_SAMPLE:
            assert denied not in result, denied
        # Control plane, knowledge gardening, delegation, comms all survive.
        assert result == CONTROL_AND_KNOWLEDGE_SAMPLE

    def test_dict_officer_config_is_honored(self):
        result = apply_officer_tool_ceiling(
            ["read_file", "kb_write"], {"enabled": True}
        )
        assert result == ["kb_write"]

    def test_string_true_is_not_enabled(self):
        # Strict `is True`: JSON-ish "true" strings never trip the ceiling.
        names = ["read_file", "kb_write"]
        assert apply_officer_tool_ceiling(names, {"enabled": "true"}) == names

    def test_conference_keeps_everything(self):
        names = [*OBJECT_PLANE_SAMPLE, *CONTROL_AND_KNOWLEDGE_SAMPLE]
        cfg = OfficerConfig(enabled=False, conference=True)
        assert apply_officer_tool_ceiling(names, cfg) == names

    def test_none_and_magicmock_configs_are_inert(self):
        names = ["read_file", "run_command"]
        assert apply_officer_tool_ceiling(names, None) == names
        assert apply_officer_tool_ceiling(names, MagicMock()) == names

    def test_officer_ceiling_active_predicate(self):
        assert officer_ceiling_active(OfficerConfig(enabled=True)) is True
        assert officer_ceiling_active(OfficerConfig(enabled=False)) is False
        assert officer_ceiling_active({"enabled": True}) is True
        assert officer_ceiling_active({"enabled": 1}) is False
        assert officer_ceiling_active(None) is False
        assert officer_ceiling_active(MagicMock()) is False


# ---------------------------------------------------------------------------
# K3 — runtime wiring through _setup_tools / _load_tools_for_backend
# ---------------------------------------------------------------------------


def _named_tool(name):
    tool = MagicMock()
    tool.name = name
    return tool


_SANDBOX_BACKEND = dict(
    supports_shell=True,
    supports_file_tools=True,
    supports_canvas_presentation=True,
    supports_canvas_live_apps=False,
    supports_canvas_shared_browser=False,
)
_LITE_BACKEND = dict(
    supports_shell=False,
    supports_file_tools=False,
    supports_canvas_presentation=False,
    supports_canvas_live_apps=False,
    supports_canvas_shared_browser=False,
)

_REQUESTED_BY_OVERRIDE = [
    "web_search",
    "kb_write",
    "kb_export",
    "read_file",
    "run_command",
    "git_diff",
    "browser_navigate",
    "set_canvas",
    "repo_push",
    "webdav_read",
]


def _run_setup_tools(session, requested):
    captured = []

    def load(names, _context):
        captured.append(list(names))
        return [_named_tool(name) for name in names]

    with (
        patch(
            "agent.api.persistent_session.get_all_tool_names",
            return_value=list(requested),
        ),
        patch("agent.api.persistent_session.load_tools", side_effect=load),
        patch(
            "agent.api.persistent_session.apply_description_overrides",
            side_effect=lambda tools: tools,
        ),
        patch(
            "agent.api.persistent_session.apply_instruction_enforcement",
            side_effect=lambda tools, _context: tools,
        ),
        patch.object(session, "_scope_skills_for_tool_names"),
        patch.object(session, "_deploy_catalog_skill_files"),
    ):
        session._setup_tools(None)
    return captured[0]


class TestOfficerCeilingRuntimeWiring:
    def test_background_officer_toolset_has_no_object_plane(self):
        """Even a shell-capable backend (an override-granted sandbox) plus an
        override requesting object-plane tools yields none of them — and the
        runtime appends (checkout, cloud status) are subject to the ceiling."""
        cfg = _make_config(officer=OfficerConfig(enabled=True))
        session = _make_session(config=cfg)
        session.workspace_manager = MagicMock()
        session.workspace_manager.backend = SimpleNamespace(**_SANDBOX_BACKEND)
        # Simulate a mistakenly-active cloud mount: srw_cloud_status would be
        # appended, and must still be dropped by the ceiling.
        session.cloud_mount_manager = SimpleNamespace(active=True)

        loaded = _run_setup_tools(session, _REQUESTED_BY_OVERRIDE)

        for denied in (
            "kb_export",
            "read_file",
            "run_command",
            "git_diff",
            "browser_navigate",
            "set_canvas",
            "repo_push",
            "webdav_read",
            "srw_cloud_status",
            "checkout_project_repository",
            "request_workspace_upgrade",
            "list_project_repositories",
            "get_default_project_repository",
        ):
            assert denied not in loaded, denied
        # The officer keeps knowledge, control plane, and his officer verbs.
        for kept in (
            "kb_write",
            "web_search",
            "get_session_context",
            "get_current_project",
            "list_project_jobs",
            "sleep",
            "notify_user",
            "task_add",
        ):
            assert kept in loaded, kept

    def test_background_officer_lite_backend_loses_upgrade_path(self):
        """The shell-less officer never sees request_workspace_upgrade even
        though every other shell-less Fleet session gets it."""
        cfg = _make_config(officer=OfficerConfig(enabled=True))
        session = _make_session(config=cfg)
        session.workspace_manager = MagicMock()
        session.workspace_manager.backend = SimpleNamespace(**_LITE_BACKEND)

        loaded = _run_setup_tools(session, ["web_search", "kb_write"])

        assert "request_workspace_upgrade" not in loaded
        assert "list_project_repositories" not in loaded
        assert "get_default_project_repository" not in loaded
        assert "sleep" in loaded and "notify_user" in loaded

    def test_conference_keeps_selected_workspace_tools(self):
        """A conference (enabled False) with a user-selected sandbox keeps its
        object-plane tools AND the repo-checkout affordance — it is an
        ordinary interactive session (officer_knowledge_plane.md §2)."""
        cfg = _make_config(officer=OfficerConfig(enabled=False, conference=True))
        session = _make_session(config=cfg)
        session.workspace_manager = MagicMock()
        session.workspace_manager.backend = SimpleNamespace(**_SANDBOX_BACKEND)

        loaded = _run_setup_tools(session, _REQUESTED_BY_OVERRIDE)

        for kept in (
            "read_file",
            "run_command",
            "git_diff",
            "set_canvas",
            "checkout_project_repository",
        ):
            assert kept in loaded, kept
        # A conference gets no officer verbs (enabled is False).
        assert "sleep" not in loaded

    def test_plain_session_is_unaffected(self):
        session = _make_session()  # cfg.officer stays a MagicMock
        session.workspace_manager = MagicMock()
        session.workspace_manager.backend = SimpleNamespace(**_SANDBOX_BACKEND)

        loaded = _run_setup_tools(session, _REQUESTED_BY_OVERRIDE)

        for kept in ("read_file", "run_command", "request_workspace_upgrade"):
            # request_workspace_upgrade only appears on shell-less backends;
            # assert on the two real object-plane names instead.
            if kept == "request_workspace_upgrade":
                continue
            assert kept in loaded, kept


class TestOfficerCloudMountRefusal:
    @pytest.mark.asyncio
    async def test_background_officer_never_mounts_project_cloud(self):
        cfg = _make_config(officer=OfficerConfig(enabled=True))
        session = _make_session(config=cfg)
        session.workspace_manager = MagicMock()
        await session._setup_cloud_mount({"mounts": [{"mount_id": "m1"}]})
        assert session.cloud_mount_manager is None

    @pytest.mark.asyncio
    async def test_conference_cloud_mount_still_attempted(self):
        cfg = _make_config(officer=OfficerConfig(enabled=False, conference=True))
        session = _make_session(config=cfg)
        session.workspace_manager = MagicMock()
        started = AsyncMock()
        manager = MagicMock()
        manager.start_all = started
        manager.mounts = []
        # Keep the real cloud_mount package importable: setup also resolves
        # its sidecar helpers when it builds the mounted-folder view.
        with patch(
            "shared.runtime.services.cloud_mount.RcloneMountManager",
            return_value=manager,
        ):
            await session._setup_cloud_mount({"mounts": [{"mount_id": "m1"}]})
        started.assert_awaited()
        assert session.cloud_mount_manager is manager


class TestGetCurrentProjectTrim:
    def test_format_project_drops_cloud_link_for_officers(self):
        from agent.tools.orchestrator.projects import _format_project

        project = {
            "id": PROJECT_A,
            "name": "Alpha",
            "cloud_storage_url": "https://cloud.example/f/9",
        }
        assert "Cloud storage" in _format_project(project)
        assert "Cloud storage" not in _format_project(project, include_cloud_link=False)

    @pytest.mark.asyncio
    async def test_officer_session_tool_output_has_no_cloud_link(self):
        from agent.tools.orchestrator import projects as projects_mod

        class _FakeResp:
            def raise_for_status(self):
                pass

            def json(self):
                return {
                    "id": PROJECT_A,
                    "name": "Alpha",
                    "cloud_storage_url": "https://cloud.example/f/9",
                }

        class _FakeClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def get(self, url, **kwargs):
                return _FakeResp()

        context = SimpleNamespace(
            config={"officer_session": True},
            user_id=None,
            project_id=PROJECT_A,
            project_ids=[PROJECT_A],
        )
        with patch.object(projects_mod, "_get_client", lambda **kwargs: _FakeClient()):
            tools = projects_mod.create_project_tools(context)
            get_current_project = next(
                t for t in tools if t.name == "get_current_project"
            )
            out = await get_current_project.ainvoke({})
        assert "Alpha" in out
        assert "Cloud storage" not in out

        # Non-officer sessions keep the link.
        context.config = {}
        with patch.object(projects_mod, "_get_client", lambda **kwargs: _FakeClient()):
            tools = projects_mod.create_project_tools(context)
            get_current_project = next(
                t for t in tools if t.name == "get_current_project"
            )
            out = await get_current_project.ainvoke({})
        assert "Cloud storage" in out


# ---------------------------------------------------------------------------
# K2 — explicit knowledge grant on the Centurion expert
# ---------------------------------------------------------------------------


class TestCenturionKnowledgeGrant:
    def test_grant_is_exactly_the_officer_kb_tools(self):
        from shared.runtime.core.loader import (
            get_all_tool_names,
            load_agent_config,
            resolve_config_path,
        )
        from agent.tools.registry import TOOL_REGISTRY, expand_tool_wildcards

        path, deployment_dir = resolve_config_path("centurion")
        config = load_agent_config(path, deployment_dir)
        granted = set(expand_tool_wildcards(get_all_tool_names(config)))

        kb_granted = {name for name in granted if name.startswith("kb_")}
        assert kb_granted == set(OFFICER_KB_TOOLS)
        assert "kb_export" not in granted
        # Every granted name is a real registry tool (registration test's
        # guarantee, restated here so a rename breaks THIS intent pin too).
        for name in OFFICER_KB_TOOLS:
            assert name in TOOL_REGISTRY, name


# ---------------------------------------------------------------------------
# K1 — degraded availability
# ---------------------------------------------------------------------------


class TestDegradedKnowledgeTools:
    def test_stubs_fail_closed_with_clear_error(self):
        from agent.tools.knowledge.knowledge_tools import (
            KB_UNAVAILABLE_ERROR,
            create_degraded_knowledge_tools,
        )

        stubs = create_degraded_knowledge_tools(OFFICER_KB_TOOLS)
        assert [t.name for t in stubs] == OFFICER_KB_TOOLS
        # Arbitrary args must validate (extra=allow) and answer the outage.
        out = stubs[0].invoke(
            {"title": "x", "type": "decision", "content": "y", "tags": ["a"]}
        )
        assert out == KB_UNAVAILABLE_ERROR
        assert "project knowledge unavailable" in out
        assert stubs[3].invoke({}) == KB_UNAVAILABLE_ERROR

    def test_unknown_names_are_not_stubbed(self):
        from agent.tools.knowledge.knowledge_tools import (
            create_degraded_knowledge_tools,
        )

        assert create_degraded_knowledge_tools(["run_command", "kb_read"]) and [
            t.name for t in create_degraded_knowledge_tools(["run_command", "kb_read"])
        ] == ["kb_read"]

    def test_officer_outage_binds_fail_closed_kb_tools(self):
        """Knowledge store down at attach: the officer still gets his granted
        KB names, each answering `project knowledge unavailable`."""
        cfg = _make_config(officer=OfficerConfig(enabled=True))
        session = _make_session(
            config=cfg,
            project_ids=[PROJECT_A],
            knowledge_bindings=build_knowledge_bindings(project_ids=[PROJECT_A]),
        )
        session.workspace_manager = MagicMock()
        session.workspace_manager.backend = SimpleNamespace(**_LITE_BACKEND)
        # knowledge_store stays None → real ToolContext.has_knowledge() False.

        def load(names, _context):
            # Mirror load_tools: KB tools cannot bind without a store.
            return [_named_tool(n) for n in names if not n.startswith("kb_")]

        with (
            patch(
                "agent.api.persistent_session.get_all_tool_names",
                return_value=["web_search", *OFFICER_KB_TOOLS],
            ),
            patch("agent.api.persistent_session.load_tools", side_effect=load),
            patch(
                "agent.api.persistent_session.apply_description_overrides",
                side_effect=lambda tools: tools,
            ),
            patch(
                "agent.api.persistent_session.apply_instruction_enforcement",
                side_effect=lambda tools, _context: tools,
            ),
            patch.object(session, "_scope_skills_for_tool_names"),
            patch.object(session, "_deploy_catalog_skill_files"),
        ):
            session._setup_tools(None)

        by_name = {t.name: t for t in session.tools}
        for name in OFFICER_KB_TOOLS:
            assert name in by_name, name
        assert "kb_export" not in by_name
        out = by_name["kb_write"].invoke({"title": "t", "content": "c"})
        assert "project knowledge unavailable" in out
        assert set(OFFICER_KB_TOOLS) <= set(session.tool_context._resolved_tool_names)

    def test_plain_session_outage_gets_no_stubs(self):
        session = _make_session(project_ids=[PROJECT_A])
        session.workspace_manager = MagicMock()
        session.workspace_manager.backend = SimpleNamespace(**_LITE_BACKEND)

        def load(names, _context):
            return [_named_tool(n) for n in names if not n.startswith("kb_")]

        with (
            patch(
                "agent.api.persistent_session.get_all_tool_names",
                return_value=["web_search", "kb_write"],
            ),
            patch("agent.api.persistent_session.load_tools", side_effect=load),
            patch(
                "agent.api.persistent_session.apply_description_overrides",
                side_effect=lambda tools: tools,
            ),
            patch(
                "agent.api.persistent_session.apply_instruction_enforcement",
                side_effect=lambda tools, _context: tools,
            ),
            patch.object(session, "_scope_skills_for_tool_names"),
            patch.object(session, "_deploy_catalog_skill_files"),
        ):
            session._setup_tools(None)

        assert "kb_write" not in {t.name for t in session.tools}


class TestOfficerSurvivesMemoryOutage:
    """officer_knowledge_plane.md §3.1: a vector/KB outage must not kill the
    officer — the configured⇒required memory gates degrade instead of raising."""

    _BROKEN_EMBEDDINGS = {
        "shared.runtime.services.embedding_service": MagicMock(
            get_embedding_service=MagicMock(
                side_effect=RuntimeError("embedding endpoint down")
            ),
            get_kb_embedding_service=MagicMock(
                side_effect=RuntimeError("embedding endpoint down")
            ),
        ),
        "shared.runtime.services.knowledge_store": MagicMock(
            KnowledgeStore=MagicMock(side_effect=RuntimeError("vector down"))
        ),
    }

    def test_officer_required_memory_outage_degrades_not_dies(self):
        cfg = _make_config(
            memory_enabled=True,
            memory_required=True,
            officer=OfficerConfig(enabled=True),
        )
        session = _make_session(
            config=cfg,
            project_ids=[PROJECT_A],
            knowledge_bindings=build_knowledge_bindings(project_ids=[PROJECT_A]),
        )
        session.tool_context = MagicMock()

        with patch.dict("sys.modules", self._BROKEN_EMBEDDINGS):
            session._setup_memory(postgres_conn=MagicMock(), vector_conn=MagicMock())

        assert session._memory_degraded is True
        assert session._kb_degraded is True
        assert session.recall_store is None
        assert session.knowledge_store is None

    def test_plain_session_required_memory_outage_still_raises(self):
        from agent.api.persistent_session import MemoryUnavailableError

        cfg = _make_config(memory_enabled=True, memory_required=True)
        session = _make_session(config=cfg, project_ids=[PROJECT_A])
        session.tool_context = MagicMock()

        with patch.dict("sys.modules", self._BROKEN_EMBEDDINGS):
            with pytest.raises(MemoryUnavailableError):
                session._setup_memory(
                    postgres_conn=MagicMock(), vector_conn=MagicMock()
                )


class TestSitrepKnowledgeSection:
    """The wake visibly says `project knowledge unavailable` during an outage
    (per-section degradation pattern shared with the other sitrep sections)."""

    class _Acquire:
        def __init__(self, conn):
            self._conn = conn

        async def __aenter__(self):
            return self._conn

        async def __aexit__(self, *args):
            return False

    class _BrokenAcquire:
        async def __aenter__(self):
            raise ConnectionError("vector pool down")

        async def __aexit__(self, *args):
            return False

    def _db(self):
        conn = SimpleNamespace(
            fetch=AsyncMock(return_value=[]), fetchval=AsyncMock(return_value=0)
        )
        db = SimpleNamespace()
        db.query_jobs = AsyncMock(return_value=JobQueryResult(jobs=[]))
        db.acquire = lambda: self._Acquire(conn)
        db.get_officer_capacity_lineage = AsyncMock(return_value=[])
        return db

    def _thread(self):
        return {
            "id": str(uuid.uuid4()),
            "project_id": PROJECT_A,
            "metadata": {"config_override": {"officer": {"enabled": True}}},
        }

    _ROW = {"id": 1, "source": "timer", "dedup_key": "timer", "payload": {}}

    @pytest.mark.asyncio
    async def test_outage_puts_unavailable_line_in_wake(self):
        from orchestrator.services import sitrep

        vec = SimpleNamespace(acquire=lambda: self._BrokenAcquire())
        text, patch_state = await sitrep.build_wake_message(
            self._db(),
            self._thread(),
            [self._ROW],
            audit_reader=None,
            usage_ledger=None,
            vector_db=vec,
        )
        assert text is not None
        assert "project knowledge unavailable" in text
        assert "reconstruct the backlog" in text
        assert patch_state is not None  # the wake itself is never lost

    @pytest.mark.asyncio
    async def test_healthy_probe_stays_silent(self):
        from orchestrator.services import sitrep

        conn = SimpleNamespace(fetchval=AsyncMock(return_value=1))
        vec = SimpleNamespace(acquire=lambda: self._Acquire(conn))
        text, _ = await sitrep.build_wake_message(
            self._db(),
            self._thread(),
            [self._ROW],
            audit_reader=None,
            usage_ledger=None,
            vector_db=vec,
        )
        assert text is not None
        assert "project knowledge unavailable" not in text

    @pytest.mark.asyncio
    async def test_missing_vector_handle_is_not_an_outage(self):
        from orchestrator.services import sitrep

        text, _ = await sitrep.build_wake_message(
            self._db(),
            self._thread(),
            [self._ROW],
            audit_reader=None,
            usage_ledger=None,
            vector_db=None,
        )
        assert text is not None
        assert "project knowledge unavailable" not in text


# ---------------------------------------------------------------------------
# K1 — external KBs stay read-only in tool behavior (write refusal)
# ---------------------------------------------------------------------------


class TestExternalKbWriteRefusal:
    def test_write_scope_error_names_external_read_only(self):
        from agent.tools.knowledge.knowledge_tools import (
            _get_project_id,
            _write_scope_error,
        )

        context = SimpleNamespace(
            knowledge_bindings=[_external_binding(EXTERNAL_KB)],
            project_id=None,
            project_ids=[],
        )
        # No writable native KB in scope → kb_write's guard message says so.
        assert _get_project_id(context) is None
        assert "read-only" in _write_scope_error(context)
