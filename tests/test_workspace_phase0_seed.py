"""Workspace seeding regressions: instructions clobber, README facts, Phase 0 commit.

instructions.md is now a virtual file (knowledge-base/knowledge/features/virtual_directories.md):
TestInstructionsClobber verifies the provider's inline > upload > template
precedence — the direct descendant of a real historical bug where
_deploy_instruction_files's guard called .exists() on a local Path for a path
that only ever exists on the remote host, unconditionally overwriting
user-provided instructions with the template. Plus the Phase 0 seed behavior
(README.md workspace-facts block + seed commit).
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from agent.core.workspace import WorkspaceManager
from tests._fs_backend import FilesystemTestBackend


class RemoteLikeBackend(FilesystemTestBackend):
    """Filesystem backend that resolves to a path that never exists locally.

    Models the production RemoteBackend property that broke the guard:
    all I/O works through the backend, but ``resolve_path()`` names a path
    on the remote host, so ``Path(resolve_path(...)).exists()`` is always
    False on the agent pod.
    """

    def resolve_path(self, relative_path: str) -> str:
        return f"/nonexistent-remote-host/workspace/{relative_path}"


def _bare_agent(workspace_manager, config=None):
    """UniversalAgent shell (no __init__) for unit-testing instance methods."""
    from agent.agent import UniversalAgent

    agent = UniversalAgent.__new__(UniversalAgent)
    agent._workspace_manager = workspace_manager
    agent._agent_seed_files = {}
    agent._job_metadata = None
    agent._resolved_instructions_md = None
    agent.config = config or SimpleNamespace(
        llm=SimpleNamespace(model="test-model"),
        instruction_files=[],
        extra={},
    )
    return agent


class TestInstructionsClobber:
    """User-provided instructions win over the template.

    instructions.md is virtual, so there is no exists()-probe left to fool —
    _deploy_instruction_files unconditionally registers a provider whose
    precedence (inline > upload > template) lives in
    build_instruction_providers(). This is the regression test for the bug
    that motivated the migration: a real .exists() probe on a local Path is
    always False for a remote backend, so the old guard clobbered
    user-provided instructions with the template on every remote job.
    """

    def test_custom_instructions_survive_on_remote_backend(self, tmp_path):
        ws = WorkspaceManager(job_id="t", backend=RemoteLikeBackend(tmp_path))
        agent = _bare_agent(ws)
        agent._job_metadata = {"instructions": "CUSTOM USER BRIEF"}

        agent._deploy_instruction_files([])

        assert ws.read_file("instructions.md") == "CUSTOM USER BRIEF"

    def test_template_deployed_when_instructions_missing(self, tmp_path, monkeypatch):
        import shared.runtime.core.loader as loader

        monkeypatch.setattr(loader, "load_instructions", lambda *a, **k: "TEMPLATE")
        monkeypatch.setattr(
            loader, "render_instruction_content", lambda content, *a, **k: content
        )
        ws = WorkspaceManager(job_id="t", backend=RemoteLikeBackend(tmp_path))
        agent = _bare_agent(ws)

        agent._deploy_instruction_files([])

        assert ws.read_file("instructions.md") == "TEMPLATE"


class TestBoundSkillCapabilityRendering:
    """Bound skills must see the post-backend, actually loaded tool palette."""

    @pytest.mark.parametrize(
        ("tool_names", "included", "excluded"),
        [
            (
                ["run_command", "file_exists", "read_file"],
                "`wc -w`",
                "no command runner",
            ),
            (["file_exists", "read_file"], "no command runner", "run_command"),
        ],
    )
    def test_verify_skill_is_rendered_for_loaded_tools(
        self, tmp_path, tool_names, included, excluded
    ):
        from pathlib import Path

        from shared.runtime.core.loader import InstructionFileEntry

        raw_skill = Path("config/skills/verify-before-done/SKILL.md").read_text(
            encoding="utf-8"
        )
        config = SimpleNamespace(
            llm=SimpleNamespace(model="test-model"),
            instruction_files=[
                InstructionFileEntry(
                    trigger="phase_start:tactical",
                    skill="verify-before-done",
                    enforce=False,
                )
            ],
            extra={
                "_resolved_instructions": {"verify-before-done": raw_skill},
                "_resolved_skills": {"files": {}},
            },
            _deployment_dir=None,
        )
        ws = WorkspaceManager(job_id="t", backend=RemoteLikeBackend(tmp_path))
        agent = _bare_agent(ws, config)

        agent._deploy_instruction_files(tool_names)

        rendered = ws.read_file("skills/verify-before-done/SKILL.md")
        assert included in rendered
        assert excluded not in rendered
        assert "{%" not in rendered

    def test_verify_skill_shell_execute_only_names_admitted_tool(self, tmp_path):
        """shell_execute-only palettes must not claim 'no command runner'.

        Regression for worker_verification_skill_tool_contract_mismatch: the
        shipped template tested only has_tool("run_command"), so a VM worker
        whose admitted shell tool is shell_execute materialized guidance
        saying no runner exists while shell_execute actually worked. Uses the
        real renderer + workspace delivery path, not source inspection.
        """
        from pathlib import Path

        from shared.runtime.core.loader import InstructionFileEntry

        raw_skill = Path("config/skills/verify-before-done/SKILL.md").read_text(
            encoding="utf-8"
        )
        config = SimpleNamespace(
            llm=SimpleNamespace(model="test-model"),
            instruction_files=[
                InstructionFileEntry(
                    trigger="phase_start:tactical",
                    skill="verify-before-done",
                    enforce=False,
                )
            ],
            extra={
                "_resolved_instructions": {"verify-before-done": raw_skill},
                "_resolved_skills": {"files": {}},
            },
            _deployment_dir=None,
        )
        ws = WorkspaceManager(job_id="t", backend=RemoteLikeBackend(tmp_path))
        agent = _bare_agent(ws, config)

        agent._deploy_instruction_files(["shell_execute", "read_file"])

        rendered = ws.read_file("skills/verify-before-done/SKILL.md")
        assert "no command runner" not in rendered.lower()
        assert "shell_execute" in rendered
        assert "run_command" not in rendered
        assert "{%" not in rendered

    def test_verify_skill_shell_helpers_alone_have_no_runner(self, tmp_path):
        """A shell-category member alone need not provide command execution.

        shell_read/cancel_command share the shell category but cannot run a
        check, so the no-runner fallback must stay for such palettes. Guards
        the shell_execute fix against over-broadening to has_shell.
        """
        from pathlib import Path

        from shared.runtime.core.loader import InstructionFileEntry

        raw_skill = Path("config/skills/verify-before-done/SKILL.md").read_text(
            encoding="utf-8"
        )
        config = SimpleNamespace(
            llm=SimpleNamespace(model="test-model"),
            instruction_files=[
                InstructionFileEntry(
                    trigger="phase_start:tactical",
                    skill="verify-before-done",
                    enforce=False,
                )
            ],
            extra={
                "_resolved_instructions": {"verify-before-done": raw_skill},
                "_resolved_skills": {"files": {}},
            },
            _deployment_dir=None,
        )
        ws = WorkspaceManager(job_id="t", backend=RemoteLikeBackend(tmp_path))
        agent = _bare_agent(ws, config)

        agent._deploy_instruction_files(["shell_read", "cancel_command", "read_file"])

        rendered = ws.read_file("skills/verify-before-done/SKILL.md")
        assert "no command runner" in rendered.lower()
        assert "shell_execute" not in rendered
        assert "run_command" not in rendered

    def test_bound_skill_missing_frozen_content_fails_closed(self, tmp_path):
        """Absent frozen bound content must not silently skip delivery.

        Regression for worker_verification_skill_tool_contract_mismatch: the
        worker logged and skipped, leaving no file while the gate still
        demanded a read of it — an endless missing-file loop. Exercised on an
        empty workspace so retained files cannot hide the delivery failure.
        """
        from shared.runtime.core.loader import InstructionFileEntry

        config = SimpleNamespace(
            llm=SimpleNamespace(model="test-model"),
            instruction_files=[
                InstructionFileEntry(
                    trigger="before_tool:todo_complete",
                    skill="verify-before-done",
                    enforce=True,
                )
            ],
            extra={
                "_resolved_instructions": {},
                "_resolved_skills": {"files": {}},
            },
            _deployment_dir=None,
        )
        ws = WorkspaceManager(job_id="t", backend=RemoteLikeBackend(tmp_path))
        agent = _bare_agent(ws, config)

        with pytest.raises(RuntimeError, match="verify-before-done"):
            agent._deploy_instruction_files(["read_file", "todo_complete"])
        assert not ws.exists("skills/verify-before-done/SKILL.md")

    def test_changed_tools_invalidate_prior_receipt(self, tmp_path):
        """Narrowed tools on resume must not inherit a stale shell receipt.

        Materialize with run_command, read it, then re-materialize with a
        shell_execute-only palette (simulating resume with a different tool
        set). The bytes differ, so restoring the old receipt against the new
        file fails closed and a fresh read is required.
        """
        from pathlib import Path

        from agent.tools.context import ToolContext
        from shared.runtime.core.loader import InstructionFileEntry

        raw_skill = Path("config/skills/verify-before-done/SKILL.md").read_text(
            encoding="utf-8"
        )
        entry = InstructionFileEntry(
            trigger="before_tool:todo_complete",
            skill="verify-before-done",
            enforce=True,
        )
        config = SimpleNamespace(
            llm=SimpleNamespace(model="test-model"),
            instruction_files=[entry],
            extra={
                "_resolved_instructions": {"verify-before-done": raw_skill},
                "_resolved_skills": {"files": {}},
            },
            _deployment_dir=None,
        )
        ws = WorkspaceManager(job_id="t", backend=RemoteLikeBackend(tmp_path))
        agent = _bare_agent(ws, config)

        agent._deploy_instruction_files(["run_command", "read_file"])
        first_body = ws.read_file("skills/verify-before-done/SKILL.md")
        assert "run_command" in first_body

        ctx = ToolContext(workspace_manager=ws)
        ctx._instruction_files = [entry]
        ctx._llm_config = None
        ctx.record_file_read(entry.path, first_body)
        receipts = ctx.export_instruction_read_receipts()
        assert entry.path in receipts

        agent._deploy_instruction_files(["shell_execute", "read_file"])
        second_body = ws.read_file("skills/verify-before-done/SKILL.md")
        assert "shell_execute" in second_body
        assert second_body != first_body

        successor = ToolContext(workspace_manager=ws)
        successor._instruction_files = [entry]
        assert successor.restore_instruction_read_receipts(receipts) == 0
        assert successor.check_tool_enforcement("todo_complete") is not None

        successor.record_file_read(entry.path, second_body)
        assert successor.check_tool_enforcement("todo_complete") is None

    def test_freeze_hydrate_deploy_restores_bound_skill_on_empty_workspace(
        self, tmp_path
    ):
        """The frozen blob must carry bound content to an empty resume target.

        Full resolve → freeze → hydrate → materialize cycle with the real
        worker_base bindings: an empty workspace (no retained files) still
        receives the authorized verify-before-done body. Guards against
        hydration losing content, distinct from the missing-blob failure above.
        """
        from shared.runtime.core.loader import (
            load_agent_config,
            load_config_from_resolved,
            resolve_config_path,
            serialize_resolved_config,
        )

        config_path, deployment_dir = resolve_config_path("developer")
        cfg = load_agent_config(config_path, deployment_dir)
        blob = serialize_resolved_config(cfg, model=cfg.llm.model)
        assert "verify-before-done" in blob["instructions"]

        hydrated = load_config_from_resolved(blob)
        ws = WorkspaceManager(job_id="t", backend=RemoteLikeBackend(tmp_path))
        agent = _bare_agent(ws, hydrated)
        assert not ws.exists("skills/verify-before-done/SKILL.md")

        agent._deploy_instruction_files(["run_command", "read_file", "todo_complete"])

        body = ws.read_file("skills/verify-before-done/SKILL.md")
        assert "Verify Before Done" in body
        assert "{%" not in body


class TestLegacyManifestCleanup:
    def test_removes_retired_status_file_before_agent_can_read_it(self, tmp_path):
        ws = WorkspaceManager(job_id="t", backend=RemoteLikeBackend(tmp_path))
        ws.write_file("output/manifest_status.json", '{"exists": false}')
        agent = _bare_agent(ws)

        agent._remove_legacy_manifest_status("job-1")

        assert not ws.exists("output/manifest_status.json")

    def test_missing_retired_status_file_is_a_noop(self, tmp_path):
        ws = WorkspaceManager(job_id="t", backend=RemoteLikeBackend(tmp_path))
        agent = _bare_agent(ws)

        agent._remove_legacy_manifest_status("job-1")

        assert not ws.exists("output/manifest_status.json")


class TestResolveUploadedInstructions:
    """_resolve_uploaded_instructions() — the eager async resolution that lets
    a virtual instructions.md survive the resume paths in
    _setup_job_workspace. A virtual file persists nothing between runs, so
    the upload source (unlike inline, which is read live from metadata) must
    be resolved before any of that function's early returns, not lazily
    inside the provider — this is the unit-level half of that fix; the
    call-site (top-of-function, before any branch) is covered by the full
    existing _setup_job_workspace test suite passing unchanged.
    """

    @pytest.mark.asyncio
    async def test_no_upload_id_returns_none_without_any_io(self):
        agent = _bare_agent(MagicMock())
        agent._download_upload_files = AsyncMock(
            side_effect=AssertionError("must not attempt I/O without an upload id")
        )

        result = await agent._resolve_uploaded_instructions({"instructions": "INLINE"})

        assert result is None

    @pytest.mark.asyncio
    async def test_inline_short_circuits_even_when_upload_id_is_also_set(self):
        """Inline wins over upload (mirrors the deleted if/elif), so when both
        are present the download must never be attempted — regression test
        for a real bug: an earlier version resolved the upload unconditionally,
        paying an HTTP round-trip + local glob on every job with both fields
        set even though the result was always going to be discarded."""
        agent = _bare_agent(MagicMock())
        agent._download_upload_files = AsyncMock(
            side_effect=AssertionError("must not attempt I/O when inline is present")
        )

        result = await agent._resolve_uploaded_instructions(
            {"instructions": "INLINE", "instructions_upload_id": "up-1"}
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_http_download_wins_when_available(self):
        agent = _bare_agent(MagicMock())

        async def fake_download(upload_id, dest_dir, job_logger):
            (dest_dir / "instructions.md").write_text("FROM HTTP", encoding="utf-8")
            return ["instructions.md"]

        agent._download_upload_files = fake_download

        result = await agent._resolve_uploaded_instructions(
            {"instructions_upload_id": "up-1"}
        )

        assert result == "FROM HTTP"

    @pytest.mark.asyncio
    async def test_falls_back_to_local_uploads_dir_when_http_fails(
        self, tmp_path, monkeypatch
    ):
        import agent.core.workspace as workspace_module

        monkeypatch.setattr(
            workspace_module, "get_workspace_base_path", lambda: tmp_path
        )
        local_dir = tmp_path / "uploads" / "up-2"
        local_dir.mkdir(parents=True)
        (local_dir / "instructions.md").write_text("FROM LOCAL", encoding="utf-8")

        agent = _bare_agent(MagicMock())
        agent._download_upload_files = AsyncMock(return_value=None)  # HTTP unavailable

        result = await agent._resolve_uploaded_instructions(
            {"instructions_upload_id": "up-2"}
        )

        assert result == "FROM LOCAL"

    @pytest.mark.asyncio
    async def test_returns_none_when_upload_id_resolves_to_nothing(
        self, tmp_path, monkeypatch
    ):
        import agent.core.workspace as workspace_module

        monkeypatch.setattr(
            workspace_module, "get_workspace_base_path", lambda: tmp_path
        )
        agent = _bare_agent(MagicMock())
        agent._download_upload_files = AsyncMock(return_value=None)

        result = await agent._resolve_uploaded_instructions(
            {"instructions_upload_id": "missing-upload"}
        )

        assert result is None


class TestWorkspaceFactsReadme:
    """README.md carries a marker-delimited facts block: create it, replace
    only the marked span, replace the Gitea stub, append to a human README —
    and never carry the job id, description, or kickoff."""

    START = "<!-- srw:workspace-facts:start -->"
    END = "<!-- srw:workspace-facts:end -->"
    JOB_ID = "ab12cd34-0000-0000-0000-000000000000"
    DESCRIPTION = "Analyze the quarterly reports."
    KICKOFF = "Start with the Q3 deck and report back."

    def _ws(self, tmp_path):
        return WorkspaceManager(job_id="t", backend=FilesystemTestBackend(tmp_path))

    def _inject(self, ws, ds_configs=None, **kwargs):
        from agent.core.datasource_setup import inject_workspace_facts

        return inject_workspace_facts(ds_configs or [], ws, **kwargs)

    def test_readme_created_when_absent(self, tmp_path):
        ws = self._ws(tmp_path)

        content = self._inject(ws, expert="Scholar")

        assert ws.read_file("README.md") == content
        assert content.startswith(f"# Workspace\n\n{self.START}\n")
        assert content.rstrip("\n").endswith(self.END)
        assert "- **Expert**: Scholar" in content
        assert "_No connectors attached._" in content
        assert "_No input documents._" in content
        assert "- `output/` — deliverables" in content

    def test_marked_block_is_replaced_and_the_rest_is_byte_identical(self, tmp_path):
        ws = self._ws(tmp_path)
        head = "# My notes\n\nkeep me\n\n"
        tail = "\n\n## Appendix\n\nalso keep me\n"
        ws.write_file(
            "README.md",
            f"{head}{self.START}\nstale block\n{self.END}{tail}",
        )

        self._inject(ws, [{"type": "postgresql", "name": "Sales DB"}])

        content = ws.read_file("README.md")
        assert content.startswith(head + self.START)
        assert content.endswith(self.END + tail)
        assert "stale block" not in content
        assert content.count(self.START) == 1 and content.count(self.END) == 1
        assert "**Sales DB**" in content

    def test_gitea_stub_is_replaced(self, tmp_path):
        ws = self._ws(tmp_path)
        ws.write_file("README.md", "# job-ab12cd34\n\nWorkspace for job-ab12cd34")

        self._inject(ws)

        content = ws.read_file("README.md")
        assert content.startswith(f"# Workspace\n\n{self.START}")
        assert "job-ab12cd34" not in content

    def test_current_gitea_intent_stub_is_replaced(self):
        """The orchestrator's current auto-init README ("SRW managed repository;
        creation-intent=<uuid>", orchestrator/services/gitea.py) is a stub too —
        it must be replaced, not appended to (seen live on k3d job 2e6a4518)."""
        from agent.core.datasource_setup import merge_workspace_facts

        stub = (
            "# job-2e6a4518\n\n"
            "SRW managed repository; creation-intent="
            "d1c02575-ce3d-4e2e-a8ff-77ef2d275a34\n"
        )
        block = (
            "<!-- srw:workspace-facts:start -->\nX\n<!-- srw:workspace-facts:end -->"
        )
        out = merge_workspace_facts(stub, block)
        assert out.startswith("# Workspace\n\n<!-- srw:workspace-facts:start -->")
        assert "creation-intent" not in out
        assert "# job-2e6a4518" not in out

    def test_human_readme_is_appended_to_never_modified(self, tmp_path):
        ws = self._ws(tmp_path)
        original = "# My Project\n\nHand-written project docs.\n"
        ws.write_file("README.md", original)

        self._inject(ws)

        content = ws.read_file("README.md")
        assert content.startswith(original.rstrip("\n") + "\n\n" + self.START)
        assert content.count(self.START) == 1

    def test_facts_only_never_the_job_id_description_or_kickoff(self, tmp_path):
        import inspect

        from agent.core.datasource_setup import (
            inject_workspace_facts,
            render_workspace_facts,
        )

        for fn in (inject_workspace_facts, render_workspace_facts):
            params = set(inspect.signature(fn).parameters)
            assert not params & {"job_id", "description", "kickoff_message", "todos"}

        ws = self._ws(tmp_path)
        ws.write_file("README.md", "# job-ab12cd34\n\nWorkspace for job-ab12cd34")
        content = self._inject(ws, project_name="Acme Reports", expert="Scholar")

        assert "- **Project**: Acme Reports" in content
        assert self.JOB_ID not in content
        assert self.JOB_ID[:8] not in content
        assert self.DESCRIPTION not in content
        assert self.KICKOFF not in content

    def test_project_line_omitted_when_unknown(self, tmp_path):
        content = self._inject(self._ws(tmp_path), expert="Scholar")
        assert "**Project**" not in content
        assert "- **Expert**: Scholar" in content

    def test_materials_list_documents_capped_at_30(self, tmp_path):
        ws = self._ws(tmp_path)
        for i in range(32):
            ws.write_file(f"documents/report_{i:02d}.pdf", "x")
        ws.write_file("documents/nested/deck.pptx", "x")
        ws.write_file("documents/.DS_Store", "x")

        content = self._inject(ws)

        listed = [
            line for line in content.splitlines() if line.startswith("- `documents/")
        ]
        assert len(listed) == 30
        assert listed[0] == "- `documents/nested/deck.pptx`"
        assert "… and 3 more" in content
        assert ".DS_Store" not in content
        assert "_No input documents._" not in content

    def test_notes_layout_line_only_when_the_directory_exists(self, tmp_path):
        ws = self._ws(tmp_path)
        assert "- `notes/`" not in self._inject(ws)
        ws.backend.mkdir("notes")
        assert "- `notes/` — working notes" in self._inject(ws)

    def test_write_failure_is_non_fatal(self, tmp_path):
        ws = MagicMock()
        ws.read_file.side_effect = FileNotFoundError
        ws.write_file.side_effect = OSError("sftp down")

        assert self._inject(ws) is None


class TestPhase0SeedCommit:
    def _agent_with_git(self, git_manager):
        ws = MagicMock()
        ws.git_manager = git_manager
        return _bare_agent(ws)

    def test_commits_and_pushes_seeded_workspace(self):
        git = MagicMock()
        git.is_active = True
        git.commit.return_value = True
        agent = self._agent_with_git(git)

        agent._commit_workspace_seed("job-1")

        git.commit.assert_called_once()
        message = git.commit.call_args.args[0]
        assert message.startswith("[Phase 0 Seed]")
        assert git.commit.call_args.kwargs.get("allow_empty") is False
        git.push.assert_called_once()

    def test_no_push_when_nothing_to_commit(self):
        git = MagicMock()
        git.is_active = True
        git.commit.return_value = False
        agent = self._agent_with_git(git)

        agent._commit_workspace_seed("job-1")

        git.push.assert_not_called()

    def test_noop_without_git_manager(self):
        agent = self._agent_with_git(None)
        agent._commit_workspace_seed("job-1")  # must not raise

    def test_git_errors_are_non_fatal(self):
        git = MagicMock()
        git.is_active = True
        git.commit.side_effect = RuntimeError("boom")
        agent = self._agent_with_git(git)

        agent._commit_workspace_seed("job-1")  # must not raise


class TestDocumentAutoRegistration:
    """Input-document auto-registration must discover files via the backend —
    a local Path walk never sees a remote workspace, which silently disabled
    registration on every remote-backend job."""

    def _context_for(self, ws):
        context = MagicMock()
        context.has_workspace.return_value = True
        context.vector_db = object()
        context.workspace_manager = ws
        context.get_or_register_doc_source = AsyncMock(return_value=1)
        return context

    @pytest.mark.asyncio
    async def test_registers_relative_paths_on_remote_backend(self, tmp_path):
        ws = WorkspaceManager(job_id="t", backend=RemoteLikeBackend(tmp_path))
        ws.write_file("documents/report.md", "content")
        ws.write_file("documents/sub/deep.pdf", "pdf bytes")
        ws.write_file("documents/external/page.html", "web content")  # skipped
        ws.write_file("documents/image.png", "img")  # unsupported extension
        agent = _bare_agent(ws)
        context = self._context_for(ws)

        await agent._register_initial_documents_background(context)

        registered = sorted(
            call.args[0] for call in context.get_or_register_doc_source.call_args_list
        )
        assert registered == ["documents/report.md", "documents/sub/deep.pdf"]

    @pytest.mark.asyncio
    async def test_missing_documents_dir_is_noop(self, tmp_path):
        ws = WorkspaceManager(job_id="t", backend=RemoteLikeBackend(tmp_path))
        agent = _bare_agent(ws)
        context = self._context_for(ws)

        await agent._register_initial_documents_background(context)

        context.get_or_register_doc_source.assert_not_called()

    @pytest.mark.asyncio
    async def test_registration_errors_are_non_fatal(self, tmp_path):
        ws = WorkspaceManager(job_id="t", backend=RemoteLikeBackend(tmp_path))
        ws.write_file("documents/report.md", "content")
        agent = _bare_agent(ws)
        context = self._context_for(ws)
        context.get_or_register_doc_source.side_effect = RuntimeError("boom")

        await agent._register_initial_documents_background(context)  # must not raise


class TestPersistentSessionSkillGuard:
    """Session resume must not clobber an already-deployed (possibly
    user-edited) bound skill file on a remote workspace."""

    def test_bound_skill_not_overwritten_on_remote_backend(self, tmp_path):
        pytest.importorskip("langgraph")
        from shared.runtime.core.loader import InstructionFileEntry
        from tests.test_persistent_session import _make_config, _make_session

        cfg = _make_config(
            extra={
                "_resolved_skills": {},
                "_resolved_instructions": {"cite-as-you-write": "FRESH TEMPLATE"},
            }
        )
        cfg._deployment_dir = None
        cfg.instruction_files = [
            InstructionFileEntry(
                trigger="before_tool:cite_web", skill="cite-as-you-write"
            )
        ]
        session = _make_session(config=cfg)
        session.workspace_manager = WorkspaceManager(
            job_id="t", backend=RemoteLikeBackend(tmp_path)
        )
        path = "skills/cite-as-you-write/SKILL.md"
        session.workspace_manager.write_file(path, "USER EDITED")

        session._deploy_instruction_files()

        assert session.workspace_manager.read_file(path) == "USER EDITED"


class TestReseedFromSnapshotIfFresh:
    """The snapshot re-seed must fire on a fresh workspace and ONLY there.

    ``recover_to_phase`` overwrites checkpoint.db, plan.md, todos.yaml and
    archive/ (src/core/phase_snapshot.py). Firing it on a same-pod resume
    (cooldown pause/resume, freeze-continue, outage-sweeper redispatch)
    silently rewinds the job to the last phase boundary. The seeded-content
    marker is the only thing distinguishing the two cases, and probing a
    *virtual* task_brief.md would answer "unseeded" on every single resume.
    See knowledge-base/knowledge/features/virtual_directories.md.
    """

    @staticmethod
    def _agent(tmp_path, backend):
        ws = WorkspaceManager(
            job_id="job-1",
            config=MagicMock(git_versioning=False),
            backend=backend,
            base_path=tmp_path,
        )
        agent = _bare_agent(ws)
        agent.config = SimpleNamespace(
            llm=SimpleNamespace(model="test-model"),
            instruction_files=[],
            extra={},
            workspace=SimpleNamespace(structure=["output"]),
        )
        return agent

    def test_seeded_workspace_does_not_rewind_to_the_last_snapshot(
        self, tmp_path, monkeypatch
    ):
        from agent.core.backends.seed import mark_workspace_seeded

        backend = FilesystemTestBackend(tmp_path)
        agent = self._agent(tmp_path, backend)
        # A previous boot seeded this workspace; the pod never went away.
        mark_workspace_seeded(agent._workspace_manager.backend)

        snapshot_mgr = MagicMock()
        monkeypatch.setattr(
            "agent.core.phase_snapshot.PhaseSnapshotManager",
            MagicMock(return_value=snapshot_mgr),
        )

        assert agent._reseed_from_snapshot_if_fresh("job-1", backend) is False
        snapshot_mgr.recover_to_phase.assert_not_called()
        snapshot_mgr.get_latest_snapshot.assert_not_called()

    def test_legacy_seeded_workspace_does_not_rewind_either(
        self, tmp_path, monkeypatch
    ):
        """No marker, but a real task_brief.md from a pre-marker release."""
        backend = FilesystemTestBackend(tmp_path)
        (tmp_path / "task_brief.md").write_text("# Task Brief\n")
        agent = self._agent(tmp_path, backend)

        snapshot_mgr = MagicMock()
        monkeypatch.setattr(
            "agent.core.phase_snapshot.PhaseSnapshotManager",
            MagicMock(return_value=snapshot_mgr),
        )

        assert agent._reseed_from_snapshot_if_fresh("job-1", backend) is False
        snapshot_mgr.recover_to_phase.assert_not_called()

    def test_fresh_workspace_still_recovers_from_the_last_snapshot(
        self, tmp_path, monkeypatch
    ):
        """The safety net this branch exists for must still fire."""
        backend = FilesystemTestBackend(tmp_path)
        agent = self._agent(tmp_path, backend)

        snapshot_mgr = MagicMock()
        snapshot_mgr.get_latest_snapshot.return_value = SimpleNamespace(phase_number=3)
        monkeypatch.setattr(
            "agent.core.phase_snapshot.PhaseSnapshotManager",
            MagicMock(return_value=snapshot_mgr),
        )

        assert agent._reseed_from_snapshot_if_fresh("job-1", backend) is True
        snapshot_mgr.recover_to_phase.assert_called_once()
        assert snapshot_mgr.recover_to_phase.call_args.args[0] == 3

    def test_a_virtual_task_brief_cannot_suppress_the_recovery(
        self, tmp_path, monkeypatch
    ):
        """The original hazard, inverted: virtual files must not read as seeded."""
        from agent.core.virtual_dirs import SingleFileProvider

        backend = FilesystemTestBackend(tmp_path)
        agent = self._agent(tmp_path, backend)
        agent._workspace_manager.register_virtual_provider(
            SingleFileProvider("task_brief.md", lambda: "# Task Brief\n")
        )
        assert agent._workspace_manager.backend.exists("task_brief.md")

        snapshot_mgr = MagicMock()
        snapshot_mgr.get_latest_snapshot.return_value = SimpleNamespace(phase_number=1)
        monkeypatch.setattr(
            "agent.core.phase_snapshot.PhaseSnapshotManager",
            MagicMock(return_value=snapshot_mgr),
        )

        assert (
            agent._reseed_from_snapshot_if_fresh(
                "job-1", agent._workspace_manager.backend
            )
            is True
        )
        snapshot_mgr.recover_to_phase.assert_called_once()


class TestInstructionFilesAreNeverWritten:
    """instructions.md / task_brief.md must never land in the workspace root.

    Writing them there is what dropped a critic's brief into the root its
    TARGET reads from, on every subjob that inherits its parent's workspace —
    knowledge-history/done/critic_brief_lands_in_shared_workspace_and_misleads_target.md.
    That write path was the "off" position of VIRTUAL_DIRS_ENABLED; the flag
    and the path are both gone.

    The guarantee the flag was standing in for — never boot an agent that was
    never told its task — now lives in src/graph.py, which raises instead of
    warning when both briefs resolve empty. That covers every way the overlay
    can fail, where the flag covered exactly one. See
    tests/test_graph.py::TestInitStrategicTodosNode::test_taskless_boot_raises.
    """

    def _agent(self, tmp_path):
        ws = WorkspaceManager(job_id="t", backend=RemoteLikeBackend(tmp_path))
        agent = _bare_agent(ws)
        agent._job_metadata = {
            "instructions": "CUSTOM USER BRIEF",
            "description": "Ship the thing",
        }
        return ws, agent

    def test_served_but_never_written(self, tmp_path):
        ws, agent = self._agent(tmp_path)
        assert ws.virtual_overlay is not None

        agent._deploy_instruction_files([])

        # Readable through the manager...
        assert ws.read_file("instructions.md") == "CUSTOM USER BRIEF"
        assert "Ship the thing" in ws.read_file("task_brief.md")
        # ...and absent from the real backend, which is the whole point.
        assert not (tmp_path / "instructions.md").exists()
        assert not (tmp_path / "task_brief.md").exists()
        # Not seed files either — nothing to re-assert on SSH reconnect.
        assert "instructions.md" not in agent._agent_seed_files
        assert "task_brief.md" not in agent._agent_seed_files

    def test_the_kill_switch_is_gone(self, tmp_path, monkeypatch):
        """VIRTUAL_DIRS_ENABLED must be inert — no route back to the write path.

        Pins the removal rather than the removal's side effects: if someone
        reintroduces the flag, the overlay disappears here and the write path
        comes back with it, taking the shared-workspace defect along.
        """
        monkeypatch.setenv("VIRTUAL_DIRS_ENABLED", "false")
        ws, agent = self._agent(tmp_path)

        assert ws.virtual_overlay is not None, "the flag is still being consulted"

        agent._deploy_instruction_files([])

        assert ws.read_file("instructions.md") == "CUSTOM USER BRIEF"
        assert not (tmp_path / "instructions.md").exists()
        assert not (tmp_path / "task_brief.md").exists()


class TestTaskBriefHydration:
    """Resume-lane brief starvation
    (knowledge-base/knowledge/issues/fresh_job_dispatched_as_resume_skips_seeding.md).

    JobResumeRequest ships no description/required_deliverables/kickoff, so
    the resume path must (1) serve the brief from LIVE ``_job_metadata`` — a
    bound alias goes stale when hydration replaces the dict — and (2)
    backfill those fields from the orchestrator or the agent's DB handle.
    """

    def test_brief_reads_job_metadata_live(self, tmp_path):
        ws = WorkspaceManager(job_id="t", backend=RemoteLikeBackend(tmp_path))
        agent = _bare_agent(ws)
        agent._job_metadata = {}

        agent._deploy_instruction_files([])
        # REPLACE (not mutate) the dict after provider registration: the old
        # `metadata = self._job_metadata or {}` alias bound a different empty
        # dict here and served an empty brief forever.
        agent._job_metadata = {"description": "Sum the CSV"}

        assert "Sum the CSV" in ws.read_file("task_brief.md")

    def test_resume_metadata_shape_serves_empty_brief_without_hydration(self, tmp_path):
        """The literal dual_app /job/resume metadata shape — documents the bug
        hydration exists to fix."""
        ws = WorkspaceManager(job_id="t", backend=RemoteLikeBackend(tmp_path))
        agent = _bare_agent(ws)
        agent._job_metadata = {
            "config_upload_id": "u1",
            "config_override": {},
            "datasources": [],
            "project_id": "p1",
        }

        agent._deploy_instruction_files([])

        assert ws.read_file("task_brief.md") == "# Task Brief\n\n## Description\n\n"

    @pytest.mark.asyncio
    async def test_hydration_backfills_description_deliverables_and_kickoff(
        self, tmp_path
    ):
        ws = WorkspaceManager(job_id="t", backend=RemoteLikeBackend(tmp_path))
        agent = _bare_agent(ws)
        agent._job_metadata = {}
        agent._orchestrator_client = SimpleNamespace(
            get_job_brief=AsyncMock(
                return_value={
                    "description": "Sum the CSV",
                    "required_deliverables": ["output/a.md"],
                    "kickoff_message": "Start with the header row.",
                }
            )
        )
        agent.postgres_conn = None

        await agent._hydrate_job_brief("job-1")
        agent._deploy_instruction_files([])

        brief = ws.read_file("task_brief.md")
        assert "Sum the CSV" in brief
        assert "## Kickoff Message" in brief
        assert "Start with the header row." in brief
        assert "output/a.md" in brief

    @pytest.mark.asyncio
    async def test_hydration_never_overwrites_present_fields(self):
        agent = _bare_agent(MagicMock())
        agent._job_metadata = {"description": "fresh dispatch text"}
        agent._orchestrator_client = SimpleNamespace(
            get_job_brief=AsyncMock(
                return_value={"description": "stale db text", "kickoff_message": "K"}
            )
        )
        agent.postgres_conn = None

        await agent._hydrate_job_brief("job-1")

        assert agent._job_metadata["description"] == "fresh dispatch text"
        assert agent._job_metadata["kickoff_message"] == "K"  # absent → backfilled

    @pytest.mark.asyncio
    async def test_hydration_falls_back_to_postgres_when_the_client_fails(self):
        agent = _bare_agent(MagicMock())
        agent._job_metadata = {}
        agent._orchestrator_client = SimpleNamespace(
            get_job_brief=AsyncMock(side_effect=RuntimeError("orchestrator down"))
        )
        # context arrives as a JSON string from the raw driver
        agent.postgres_conn = SimpleNamespace(
            jobs=SimpleNamespace(
                get=AsyncMock(
                    return_value={
                        "description": "From the DB",
                        "context": '{"required_deliverables": ["output/a.md"]}',
                    }
                )
            )
        )

        await agent._hydrate_job_brief("11111111-1111-1111-1111-111111111111")

        assert agent._job_metadata["description"] == "From the DB"
        assert agent._job_metadata["required_deliverables"] == ["output/a.md"]

    @pytest.mark.asyncio
    async def test_hydration_failure_is_non_fatal_and_logs_error(self, caplog):
        import logging

        agent = _bare_agent(MagicMock())
        agent._job_metadata = {}
        agent._orchestrator_client = None
        agent.postgres_conn = None

        with caplog.at_level(logging.ERROR):
            await agent._hydrate_job_brief("job-1")

        assert any("could not be hydrated" in r.getMessage() for r in caplog.records)
        assert agent._job_metadata == {}


class TestResumeWithoutCheckpointTripwire:
    """resume=True with nothing to resume from is always a routing bug —
    it must scream (ERROR) and fall toward fresh seeding, not silently start
    a blank job."""

    @pytest.mark.asyncio
    async def test_tripwire_logs_error_hydrates_and_seeds(self, caplog):
        import logging

        agent = _bare_agent(MagicMock())
        agent._hydrate_job_brief = AsyncMock()
        agent._commit_workspace_seed = MagicMock()

        with caplog.at_level(logging.ERROR):
            await agent._note_resume_without_checkpoint("job-1", "paused")

        errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert len(errors) == 1
        assert "resume=True" in errors[0].getMessage()
        assert "'paused'" in errors[0].getMessage()
        agent._hydrate_job_brief.assert_awaited_once_with("job-1")
        agent._commit_workspace_seed.assert_called_once_with("job-1")
