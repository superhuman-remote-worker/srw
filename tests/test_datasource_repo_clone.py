"""Tests for repository-datasource cloning (workspace-backend only).

Repository datasources clone exclusively on the workspace backend via
clone_repository_datasources(); the former agent-local subprocess
``git clone`` branch (setup_repository_datasource) was removed — it wrote
credentials and repos onto the agent pod (no_workspace_agent_mode.md §9.4).
"""

import logging
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.core.datasource_setup import (
    clone_repository_datasources,
    inject_workspace_facts,
    process_datasources,
    resolve_repo_clone_names,
)
from orchestrator.application import preparation as preparation_composition
from orchestrator.services import (
    agent_datasource_payload as agent_datasource_payload_module,
)


def make_workspace_manager(supports_shell=True, with_backend=True):
    """Mock WorkspaceManager with a shell-capable remote backend."""
    ws = MagicMock()
    ws.path = Path("/tmp/ws")
    ws.source_repos = {}
    ws.source_repo_meta = {}
    if not with_backend:
        ws.backend = None
        return ws
    backend = MagicMock()
    backend.supports_shell = supports_shell
    backend.exists = MagicMock(return_value=False)
    backend.resolve_home_path = MagicMock(
        side_effect=lambda rel: f"/home/agent-host/{rel}"
    )
    backend.shell_run = MagicMock(return_value="Exit code: 0")
    ws.backend = backend
    return ws


def token_ds(name="My Repo", url="https://github.com/org/repo.git", **extra):
    return {
        "type": "repository",
        "name": name,
        "connection_url": url,
        "credentials": {"auth_method": "token", "token": "tok123"},
        **extra,
    }


def agent_payload(*db_rows):
    """Run resolved DB rows through the REAL orchestrator payload builder.

    Hand-writing the agent-side dict is what let three wiring gaps ship
    green: a fixture that invents ``config=``/``read_only=`` keys tests a
    contract the orchestrator never produces. Anything asserting on what the
    agent receives must start here.
    """
    import orchestrator.main

    return agent_datasource_payload_module.build_datasources_payload(
        list(db_rows),
        dependencies=preparation_composition.datasource_payload_dependencies(
            orchestrator.main.app.state.resources
        ),
    )


class TestCapabilityGate:
    """No shell-capable backend → loud skip, never a local clone."""

    def test_no_backend_skips_without_clone(self, caplog):
        ws = make_workspace_manager(with_backend=False)
        with patch("agent.managers.git_manager.GitManager.clone") as mock_clone:
            clone_repository_datasources([token_ds()], ws)
        mock_clone.assert_not_called()
        assert ws.source_repos == {}
        assert any("no local clone" in r.message for r in caplog.records)

    def test_backend_without_shell_skips(self, caplog):
        ws = make_workspace_manager(supports_shell=False)
        with patch("agent.managers.git_manager.GitManager.clone") as mock_clone:
            clone_repository_datasources([token_ds()], ws)
        mock_clone.assert_not_called()
        assert any("shell support" in r.message for r in caplog.records)

    def test_empty_list_is_noop(self):
        ws = make_workspace_manager(with_backend=False)
        clone_repository_datasources([], ws)  # must not raise or log errors

    def test_local_clone_function_removed(self):
        from agent.core import datasource_setup

        assert not hasattr(datasource_setup, "setup_repository_datasource")


class TestBackendClone:
    """Clones run via GitManager.clone(backend=...) on the workspace."""

    def test_token_auth_injects_url_and_registers(self):
        ws = make_workspace_manager()
        git_mgr = MagicMock()
        with patch(
            "agent.managers.git_manager.GitManager.clone", return_value=git_mgr
        ) as mock_clone:
            clone_repository_datasources(
                [token_ds(default_branch="dev")],
                ws,
            )

        mock_clone.assert_called_once()
        url_arg = mock_clone.call_args[0][0]
        assert "oauth2:tok123@github.com" in url_arg
        assert mock_clone.call_args[1]["backend"] is ws.backend
        assert mock_clone.call_args[1]["remote_cwd"] == "repos/repo"
        git_mgr.checkout_branch.assert_called_once_with("dev")
        assert ws.source_repos["repo"] is git_mgr

    def test_ssh_auth_writes_key_on_backend_and_converts_url(self):
        ws = make_workspace_manager()
        ds = {
            "type": "repository",
            "name": "My Repo",
            "connection_url": "https://github.com/org/repo.git",
            "credentials": {"auth_method": "ssh", "ssh_key": "KEYMATERIAL"},
        }
        with patch(
            "agent.managers.git_manager.GitManager.clone", return_value=MagicMock()
        ) as mock_clone:
            clone_repository_datasources([ds], ws)

        # Key written to the workspace home (normalized with trailing newline)
        ws.backend.write_home_file.assert_called_once_with(
            ".ssh/repo_my-repo", "KEYMATERIAL\n"
        )
        # chmod + ssh config appended via shell on the workspace
        shell_cmds = [c[0][0] for c in ws.backend.shell_run.call_args_list]
        assert any("mkdir -p ~/.ssh" in cmd for cmd in shell_cmds)
        assert any("chmod 600" in cmd for cmd in shell_cmds)
        assert any(">> ~/.ssh/config" in cmd for cmd in shell_cmds)
        # HTTPS URL converted to SSH form so git uses the key
        assert mock_clone.call_args[0][0] == "git@github.com:org/repo.git"

    def test_name_collision_gets_suffix(self):
        ws = make_workspace_manager()
        with patch(
            "agent.managers.git_manager.GitManager.clone", return_value=MagicMock()
        ) as mock_clone:
            clone_repository_datasources(
                [token_ds(name="upstream"), token_ds(name="fork")],
                ws,
            )

        cwds = [c[1]["remote_cwd"] for c in mock_clone.call_args_list]
        assert cwds == ["repos/repo", "repos/repo-2"]
        assert set(ws.source_repos) == {"repo", "repo-2"}

    def test_failed_clone_not_registered(self, caplog):
        ws = make_workspace_manager()
        with patch("agent.managers.git_manager.GitManager.clone", return_value=None):
            clone_repository_datasources([token_ds()], ws)
        assert ws.source_repos == {}
        assert any("Failed to clone" in r.message for r in caplog.records)

    def test_required_review_branch_must_checkout_before_registration(self, caplog):
        ws = make_workspace_manager()
        git_mgr = MagicMock()
        git_mgr.checkout_branch.return_value = False
        with patch("agent.managers.git_manager.GitManager.clone", return_value=git_mgr):
            clone_repository_datasources(
                [
                    token_ds(
                        default_branch="design/hotel-rheinland-theme",
                        require_default_branch=True,
                    )
                ],
                ws,
            )

        assert ws.source_repos == {}
        assert any("required branch" in r.message for r in caplog.records)

    def test_existing_checkout_is_reused_and_re_registered_on_resume(self):
        """require_default_branch (review sessions) pins the branch even on
        reuse — the session's entire point is that this exact delivery is
        checked out (orchestrator/services/job_delivery.py)."""
        ws = make_workspace_manager()
        ws.backend.exists = MagicMock(
            side_effect=lambda path: path == "repos/repo/.git"
        )
        existing = MagicMock()
        existing.checkout_branch.return_value = True

        with patch("agent.managers.git_manager.GitManager") as git_manager:
            git_manager.return_value = existing
            clone_repository_datasources(
                [
                    token_ds(
                        default_branch="design/hotel-rheinland-theme",
                        require_default_branch=True,
                    )
                ],
                ws,
            )

        git_manager.clone.assert_not_called()
        git_manager.assert_called_once_with(
            Path("/tmp/ws/repos/repo"),
            backend=ws.backend,
            remote_cwd="repos/repo",
        )
        existing.checkout_branch.assert_called_once_with("design/hotel-rheinland-theme")
        assert ws.source_repos["repo"] is existing

    def test_reused_checkout_keeps_the_workers_branch(self, caplog):
        """Re-attach must not move HEAD in a reused clone: re-running
        checkout_branch(default_branch) on every resume silently reverted
        the branch the worker had checked out (job 12a0e92c)."""
        ws = make_workspace_manager()
        ws.backend.exists = MagicMock(
            side_effect=lambda path: path == "repos/repo/.git"
        )
        existing = MagicMock()
        existing.current_branch.return_value = "job/fix-thing"

        with patch("agent.managers.git_manager.GitManager") as git_manager:
            git_manager.return_value = existing
            with caplog.at_level(logging.DEBUG, logger="agent.core.datasource_setup"):
                clone_repository_datasources([token_ds(default_branch="dev")], ws)

        existing.checkout_branch.assert_not_called()
        assert ws.source_repos["repo"] is existing
        assert any(
            "job/fix-thing" in r.message and "dev" in r.message for r in caplog.records
        ), caplog.text

    def test_reused_checkout_still_refuses_unready_required_branch(self, caplog):
        """The require_default_branch refusal must keep meaning what it says
        on the reuse path — preserving HEAD there must not turn the gate
        into a trivially-green check."""
        ws = make_workspace_manager()
        ws.backend.exists = MagicMock(
            side_effect=lambda path: path == "repos/repo/.git"
        )
        existing = MagicMock()
        existing.checkout_branch.return_value = False

        with patch("agent.managers.git_manager.GitManager") as git_manager:
            git_manager.return_value = existing
            clone_repository_datasources(
                [token_ds(default_branch="gone", require_default_branch=True)],
                ws,
            )

        existing.checkout_branch.assert_called_once_with("gone")
        assert ws.source_repos == {}
        assert any("required branch" in r.message for r in caplog.records)

    def test_clone_root_is_ignored_by_the_durable_session_repository(self):
        """Fallback restore must re-clone content, not restore an empty gitlink."""
        ws = make_workspace_manager()

        with patch(
            "agent.managers.git_manager.GitManager.clone", return_value=MagicMock()
        ):
            clone_repository_datasources([token_ds()], ws)

        ws.backend.write_file.assert_any_call(
            ".gitignore", "# Cloned repository datasources\nrepos/\n"
        )

    def test_token_clone_records_forge_metadata_for_tools(self):
        """Tools need forge/token/owner/repo; source_repos only carries GitManager."""
        ws = make_workspace_manager()
        ds = token_ds(
            name="SRW Repository",
            url="https://github.com/superhuman-remote-worker/srw",
            config={"forge": "github"},
            default_branch="develop",
            read_only=False,
        )
        with patch(
            "agent.managers.git_manager.GitManager.clone", return_value=MagicMock()
        ):
            clone_repository_datasources([ds], ws)

        meta = ws.source_repo_meta["srw"]
        assert meta["forge"] == "github"
        assert meta["api_base"] == "https://api.github.com"
        assert meta["owner"] == "superhuman-remote-worker"
        assert meta["repo"] == "srw"
        assert meta["token"] == "tok123"
        assert meta["read_only"] is False
        assert meta["default_branch"] == "develop"

    def test_repo_metadata_marks_read_only_datasources(self):
        ws = make_workspace_manager()
        ds = token_ds(config={"forge": "github"}, read_only=True)
        with patch(
            "agent.managers.git_manager.GitManager.clone", return_value=MagicMock()
        ):
            clone_repository_datasources([ds], ws)
        assert ws.source_repo_meta["repo"]["read_only"] is True


class TestRealAgentPayloadCarriesForgeMetadata:
    """Contract tests: the clone must work on what the orchestrator ACTUALLY
    sends, not on a hand-written dict.

    ``_build_datasources_payload`` is the only producer of the agent-side
    datasource list. It used to attach ``config`` for ``kb``/``email`` only
    and it has never emitted a ``read_only`` key (it emits
    ``project_read_only``), so every repository clone recorded ``forge=""``
    (``resolve_api_base`` raised, the whole meta block was swallowed) and
    every read-only repo recorded ``read_only: False``.
    """

    def test_payload_forwards_config_with_forge(self):
        """Guard for the forge gap: no ``config`` in the payload means
        ``repo_open_pr`` always answers "has no forge recorded"."""
        payload = agent_payload(
            token_ds(config={"forge": "gitea"}, project_read_only=False)
        )
        assert payload[0]["config"] == {"forge": "gitea"}

    def test_payload_forwards_empty_config_when_unset(self):
        payload = agent_payload(token_ds(project_read_only=False))
        assert payload[0]["config"] == {}

    def test_clone_from_real_payload_records_forge_metadata(self):
        ws = make_workspace_manager()
        payload = agent_payload(
            token_ds(
                name="SRW Repository",
                url="https://github.com/superhuman-remote-worker/srw",
                config={"forge": "github"},
                default_branch="develop",
                project_read_only=False,
            )
        )
        with patch(
            "agent.managers.git_manager.GitManager.clone", return_value=MagicMock()
        ):
            clone_repository_datasources(payload, ws)

        meta = ws.source_repo_meta["srw"]
        assert meta["forge"] == "github"
        assert meta["api_base"] == "https://api.github.com"
        assert meta["owner"] == "superhuman-remote-worker"
        assert meta["repo"] == "srw"
        assert meta["token"] == "tok123"
        assert meta["read_only"] is False

    def test_real_payload_carries_server_owned_datasource_identity(self):
        datasource_id = "22222222-2222-4222-8222-222222222222"
        payload = agent_payload(
            token_ds(
                id=datasource_id,
                config={"forge": "github"},
                project_read_only=False,
            )
        )

        assert payload[0]["datasource_id"] == datasource_id
        assert "id" not in payload[0]

        ws = make_workspace_manager()
        with patch(
            "agent.managers.git_manager.GitManager.clone", return_value=MagicMock()
        ):
            clone_repository_datasources(payload, ws)

        assert ws.source_repo_meta["repo"]["datasource_id"] == datasource_id

    @pytest.mark.asyncio
    async def test_resolved_row_reaches_the_pr_authority_writer(self):
        """Resolved DB row -> payload -> clone -> tool retains one exact UUID."""

        from agent.tools.context import ToolContext
        from agent.tools.repo import create_repo_tools

        datasource_id = "22222222-2222-4222-8222-222222222222"
        payload = agent_payload(
            token_ds(
                id=datasource_id,
                name="Widget",
                url="https://github.com/acme/widget.git",
                config={"forge": "github"},
                project_read_only=False,
            )
        )
        ws = make_workspace_manager()
        git_mgr = MagicMock()
        git_mgr.current_branch.return_value = "job/exact-authority"
        git_mgr.rev_parse.return_value = "a" * 40
        with patch("agent.managers.git_manager.GitManager.clone", return_value=git_mgr):
            clone_repository_datasources(payload, ws)

        context = ToolContext(workspace_manager=ws)
        context.job_id = "11111111-1111-4111-8111-111111111111"
        context.postgres_db = MagicMock()
        context.postgres_db.jobs.record_pull_request = AsyncMock(return_value=True)
        tool = next(
            candidate
            for candidate in create_repo_tools(context)
            if candidate.name == "repo_open_pr"
        )

        with (
            patch(
                "agent.tools.repo.repo_tools.open_pull_request",
                return_value={"number": 7, "url": "https://github.test/pr/7"},
            ),
            patch(
                "agent.tools.repo.repo_tools.get_pull_request_status",
                return_value={
                    "number": 7,
                    "url": "https://github.test/pr/7",
                    "state": "open",
                    "head": "job/exact-authority",
                    "base": "main",
                    "head_sha": "a" * 40,
                    "draft": False,
                },
            ),
        ):
            result = await tool.ainvoke(
                {"repo": "widget", "title": "Exact", "base": "main"}
            )

        assert "Opened #7" in result
        call = context.postgres_db.jobs.record_pull_request.await_args
        assert str(call.args[0]) == context.job_id
        assert str(call.args[1]) == datasource_id

    def test_clone_from_real_payload_honours_project_read_only(self):
        """Guard for the key-name gap: the payload says
        ``project_read_only``, the clone read ``read_only``, so the per-repo
        write gate failed open for every read-only repository."""
        ws = make_workspace_manager()
        payload = agent_payload(
            token_ds(config={"forge": "github"}, project_read_only=True)
        )
        with patch(
            "agent.managers.git_manager.GitManager.clone", return_value=MagicMock()
        ):
            clone_repository_datasources(payload, ws)

        assert ws.source_repo_meta["repo"]["read_only"] is True

    def test_mixed_read_only_and_read_write_repos_gate_independently(self):
        """The category gate grants write tools when ANY attached repo is
        read-write, so with a mixed attach the per-repo flag is the only
        remaining guard on the read-only clone."""
        ws = make_workspace_manager()
        payload = agent_payload(
            token_ds(
                name="Writable",
                url="https://github.com/org/writable.git",
                config={"forge": "github"},
                project_read_only=False,
            ),
            token_ds(
                name="Mirror",
                url="https://github.com/org/mirror.git",
                config={"forge": "github"},
                project_read_only=True,
            ),
        )
        with patch(
            "agent.managers.git_manager.GitManager.clone", return_value=MagicMock()
        ):
            clone_repository_datasources(payload, ws)

        assert ws.source_repo_meta["writable"]["read_only"] is False
        assert ws.source_repo_meta["mirror"]["read_only"] is True

    def test_publisher_declared_read_only_still_honoured(self):
        """``read_only`` (publisher-declared, public datasources) must keep
        working alongside the project link flag."""
        ws = make_workspace_manager()
        ds = token_ds(config={"forge": "github"})
        ds["read_only"] = True
        with patch(
            "agent.managers.git_manager.GitManager.clone", return_value=MagicMock()
        ):
            clone_repository_datasources([ds], ws)

        assert ws.source_repo_meta["repo"]["read_only"] is True

    def test_full_chain_from_db_row_to_loaded_repo_tools(self):
        """The one test that would have caught all three wiring gaps.

        Walks every real seam in order — resolved DB row →
        ``_build_datasources_payload`` → ``clone_repository_datasources`` →
        ``_build_datasource_tool_override`` → ``load_config_from_resolved``
        → ``get_all_tool_names`` → ``load_tools`` — and asserts that live
        ``repo_*`` tools come out the far end. Every unit test in this branch
        passed while this chain produced nothing.
        """
        from shared.runtime.core.loader import (
            get_all_tool_names,
            load_config_from_resolved,
        )
        from agent.tools.context import ToolContext
        from agent.tools.registry import load_tools

        payload = agent_payload(
            token_ds(
                name="SRW Repository",
                url="https://github.com/superhuman-remote-worker/srw",
                config={"forge": "github"},
                default_branch="develop",
                project_read_only=False,
            )
        )
        ws = make_workspace_manager()
        with patch(
            "agent.managers.git_manager.GitManager.clone", return_value=MagicMock()
        ):
            clone_repository_datasources(payload, ws)
        assert ws.source_repo_meta["srw"]["forge"] == "github"

        import orchestrator.main

        override = agent_datasource_payload_module.build_datasource_tool_override(
            payload,
            None,
            dependencies=preparation_composition.datasource_payload_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        config = load_config_from_resolved(
            {
                "agent": {
                    "agent_id": "a",
                    "display_name": "A",
                    "tools": override["tools"],
                },
                "prompts": {},
                "instructions": {},
            }
        )
        names = [n for n in get_all_tool_names(config) if n.startswith("repo_")]
        tools = load_tools(names, ToolContext(workspace_manager=ws))

        assert [t.name for t in tools] == [
            "repo_checkout",
            "repo_commit",
            "repo_push",
            "repo_pull",
            "repo_open_pr",
            "repo_pr_status",
        ]

    def test_ssh_form_url_records_forge_metadata_for_status_reads(self):
        ws = make_workspace_manager()
        ds = token_ds(url="git@github.com:org/repo.git", config={"forge": "github"})
        with patch(
            "agent.managers.git_manager.GitManager.clone", return_value=MagicMock()
        ):
            clone_repository_datasources([ds], ws)

        assert "repo" in ws.source_repos
        assert ws.source_repo_meta["repo"] == {
            "forge": "github",
            "api_base": "https://api.github.com",
            "owner": "org",
            "repo": "repo",
            "token": "tok123",
            "read_only": False,
            "default_branch": None,
        }

    def test_attached_datasource_id_is_retained_for_server_pr_recording(self):
        ws = make_workspace_manager()
        datasource_id = "22222222-2222-4222-8222-222222222222"
        ds = token_ds(
            id=datasource_id,
            config={"forge": "github"},
        )
        with patch(
            "agent.managers.git_manager.GitManager.clone", return_value=MagicMock()
        ):
            clone_repository_datasources([ds], ws)

        assert ws.source_repo_meta["repo"]["datasource_id"] == datasource_id


class TestResolveRepoCloneNames:
    """Clone-directory names: upstream repo name, label fallback, suffixes."""

    def test_uses_upstream_repo_name_not_label(self):
        names = resolve_repo_clone_names([token_ds(name="Read-only mirror")])
        assert names == ["repo"]

    def test_falls_back_to_label_slug_without_usable_url(self):
        names = resolve_repo_clone_names([token_ds(name="My Repo!", url="")])
        assert names == ["my-repo"]

    def test_collision_gets_suffix(self):
        names = resolve_repo_clone_names(
            [token_ds(name="upstream"), token_ds(name="fork")]
        )
        assert names == ["repo", "repo-2"]


class TestDatasourceIndexRepoPaths:
    """The README.md connector list must point at the directories clones
    actually land in."""

    def test_index_uses_clone_directory_names(self):
        ws = MagicMock()
        ws.read_file.side_effect = FileNotFoundError
        written = {}
        ws.write_file.side_effect = lambda path, content: written.update(
            {path: content}
        )

        inject_workspace_facts([token_ds(name="Read-only mirror of upstream")], ws)

        content = written["README.md"]
        assert "`./repos/repo/`" in content
        # The old behavior used the datasource-label slug, which never
        # matched the clone directory (upstream repo name).
        assert "read-only-mirror-of-upstream" not in content


class TestWorkspaceFactsRepositoryLine:
    """The repository line tells the agent the clone dir, the repo= handle,
    the base branch (when known), and which repo_* tools apply."""

    def _render(self, ds, meta=None):
        ws = make_workspace_manager()
        ws.read_file.side_effect = FileNotFoundError
        written = {}
        ws.write_file.side_effect = lambda path, content: written.update(
            {path: content}
        )
        if meta is not None:
            ws.source_repo_meta["repo"] = meta
        inject_workspace_facts([ds], ws)
        return written["README.md"]

    def test_writable_repo_with_known_base_branch(self):
        content = self._render(
            token_ds(name="Mine"), meta={"read_only": False, "default_branch": "main"}
        )
        assert (
            "- **Mine** — repository cloned at `./repos/repo/` "
            '(use `repo="repo"` with the repo_* tools); base branch `main`; '
            "writable — pull requests opened with repo_open_pr are recorded "
            "for this job"
        ) in content

    def test_unknown_base_branch_omits_the_clause(self):
        content = self._render(token_ds(name="Mine"))
        assert "`./repos/repo/`" in content
        assert "base branch" not in content
        assert "writable — pull requests opened with repo_open_pr" in content

    def test_read_only_repo_lists_only_the_read_tools(self):
        content = self._render(
            token_ds(name="Mirror", project_read_only=True),
            meta={"read_only": True, "default_branch": "develop"},
        )
        assert "base branch `develop`" in content
        assert "read-only — only repo_pull/repo_pr_status" in content
        assert "repo_open_pr" not in content

    def test_read_only_falls_back_to_the_payload_flag_without_meta(self):
        content = self._render(token_ds(name="Mirror", project_read_only=True))
        assert "read-only — only repo_pull/repo_pr_status" in content


class TestProcessDatasourcesRepoGuard:
    """process_datasources never clones — repository entries are skipped."""

    def test_repository_ds_ignored_with_warning(self, caplog):
        with patch("agent.core.datasource_setup.subprocess.run") as mock_run:
            connections, clients, cli_types = process_datasources([token_ds()])
        mock_run.assert_not_called()
        assert connections == {}
        assert clients == {}
        assert cli_types == []
        assert any(
            "ignored by process_datasources" in r.message for r in caplog.records
        )


class TestDeclaredReadOnlyIndexNote:
    """Public datasources declared read-only get an advisory index note
    (knowledge-base/knowledge/features/public_datasources.md — declarative, not enforced)."""

    def test_declared_ro_repo_notes_in_index(self):
        ws = make_workspace_manager()
        ws.read_file.side_effect = FileNotFoundError
        written = {}
        ws.write_file.side_effect = lambda path, content: written.update(
            {path: content}
        )
        ds = token_ds(name="Org Wiki")
        ds["read_only"] = True

        inject_workspace_facts([ds], ws)

        assert "declared read-only" in written["README.md"]

    def test_private_repo_has_no_ro_note(self):
        ws = make_workspace_manager()
        ws.read_file.side_effect = FileNotFoundError
        written = {}
        ws.write_file.side_effect = lambda path, content: written.update(
            {path: content}
        )

        inject_workspace_facts([token_ds(name="Mine")], ws)

        assert "declared read-only" not in written["README.md"]
