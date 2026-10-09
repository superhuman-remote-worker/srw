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

from agent.connectors.checkout import clone_repository_datasources
from agent.core.datasource_setup import (
    inject_workspace_facts,
    resolve_repo_clone_names,
)
from tests._connector_runtime import open_harness
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
        from agent.connectors import checkout
        from agent.core import datasource_setup

        assert not hasattr(datasource_setup, "setup_repository_datasource")
        assert not hasattr(checkout, "setup_repository_datasource")


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

    @staticmethod
    def _ssh_entry(**row_over):
        """An SSH-key repository as the REAL payload builder emits it."""
        from shared.runtime.utils.ssh_key import generate_ed25519_keypair

        row = {
            "id": "00000000-0000-4000-8000-0000000000d1",
            "type": "repository",
            "name": "My Repo",
            "connection_url": "https://github.com/org/repo.git",
            "credentials": {
                "auth_method": "ssh",
                "ssh_key": generate_ed25519_keypair().private_key,
            },
            "config": {"forge": "github"},
            "project_read_only": False,
            **row_over,
        }
        (entry,) = agent_payload(row)
        return entry

    def test_ssh_repository_clones_its_alias_and_writes_no_key(self):
        """C1: no key file, no appended Host block, clone the opaque alias."""
        ws = make_workspace_manager()
        ds = self._ssh_entry()
        alias = ds["ssh_identity"]["alias"]
        status = {ds["ssh_identity"]["authority_id"]: "ready"}
        with patch(
            "agent.managers.git_manager.GitManager.clone", return_value=MagicMock()
        ) as mock_clone:
            clone_repository_datasources([ds], ws, ssh_identity_status=status)

        # An HTTPS URL was always cloned as the relative scp form.
        assert mock_clone.call_args[0][0] == f"{alias}:org/repo.git"
        ws.backend.write_home_file.assert_not_called()
        shell_cmds = [c[0][0] for c in ws.backend.shell_run.call_args_list]
        assert not any(">> ~/.ssh/config" in cmd for cmd in shell_cmds)
        assert not any("PRIVATE KEY" in cmd for cmd in shell_cmds)
        # Pre-agent key files are looked for, but none was listed here.
        assert not any("rm -f" in cmd for cmd in shell_cmds)
        assert ws.source_repo_meta["repo"]["token"] == ""

    def test_scp_url_reaches_its_real_host(self):
        """The scp form used to write ``Host localhost``."""
        ds = self._ssh_entry(connection_url="git@github.com:org/repo.git")
        assert ds["ssh_identity"]["host"] == "github.com"
        assert ds["ssh_identity"]["clone_url"] == (
            f"{ds['ssh_identity']['alias']}:org/repo.git"
        )

    @staticmethod
    def _warnings(log) -> str:
        return "\n".join(
            call.args[0] % call.args[1:] for call in log.warning.call_args_list
        )

    @pytest.mark.parametrize("materialized", [True, False])
    def test_unloaded_identity_is_skipped_not_cloned_without_its_key(
        self, materialized
    ):
        ws = make_workspace_manager()
        ds = self._ssh_entry()
        status = (
            {ds["ssh_identity"]["authority_id"]: "workspace_ssh_identity_load_failed"}
            if materialized
            else None
        )
        with (
            patch("agent.managers.git_manager.GitManager.clone") as mock_clone,
            patch("agent.connectors.checkout.logger") as log,
        ):
            clone_repository_datasources([ds], ws, ssh_identity_status=status)
        if not materialized:
            # No materializer ran: the alias is still the only clone target.
            assert mock_clone.call_args[0][0].startswith("srw-repo-")
            return
        mock_clone.assert_not_called()
        assert ws.source_repos == {}
        assert "load_failed" in self._warnings(log)

    def test_unavailable_or_legacy_ssh_entries_are_skipped(self):
        ws = make_workspace_manager()
        unavailable = self._ssh_entry(
            credentials={"auth_method": "ssh", "ssh_key": "not a key"}
        )
        # What a pre-C1 orchestrator sends: the key itself, no identity.
        legacy = {
            "type": "repository",
            "name": "Legacy",
            "connection_url": "git@github.com:org/legacy.git",
            "credentials": {"auth_method": "ssh", "ssh_key": "KEYMATERIAL"},
        }
        with (
            patch("agent.managers.git_manager.GitManager.clone") as mock_clone,
            patch("agent.connectors.checkout.logger") as log,
        ):
            clone_repository_datasources(
                [unavailable, legacy], ws, ssh_identity_status={}
            )
        mock_clone.assert_not_called()
        ws.backend.write_home_file.assert_not_called()
        assert "its SSH key could not be parsed" in self._warnings(log)
        assert "no workspace SSH identity" in self._warnings(log)

    def test_reused_checkout_is_pointed_at_the_alias(self):
        ws = make_workspace_manager()
        ws.backend.exists = MagicMock(return_value=True)
        ds = self._ssh_entry()
        status = {ds["ssh_identity"]["authority_id"]: "ready"}
        with (
            patch("agent.managers.git_manager.GitManager.clone") as mock_clone,
            patch(
                "agent.managers.git_manager.GitManager.add_remote", return_value=True
            ) as add_remote,
        ):
            clone_repository_datasources([ds], ws, ssh_identity_status=status)

        mock_clone.assert_not_called()
        add_remote.assert_called_once_with("origin", ds["ssh_identity"]["clone_url"])
        assert "repo" in ws.source_repos

    def test_two_deploy_keys_on_one_host_clone_through_distinct_aliases(self):
        ws = make_workspace_manager()
        first = self._ssh_entry()
        second = self._ssh_entry(
            id="00000000-0000-4000-8000-0000000000d2",
            name="Other Repo",
            connection_url="git@github.com:org/other.git",
        )
        status = {
            entry["ssh_identity"]["authority_id"]: "ready" for entry in (first, second)
        }
        with patch(
            "agent.managers.git_manager.GitManager.clone", return_value=MagicMock()
        ) as mock_clone:
            clone_repository_datasources(
                [first, second], ws, ssh_identity_status=status
            )
        urls = [call[0][0] for call in mock_clone.call_args_list]
        assert len({url.split(":")[0] for url in urls}) == 2

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
            shell_tab="git",
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
            with caplog.at_level(logging.DEBUG, logger="agent.connectors.checkout"):
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


class TestLegacyKeyFiles:
    """Pre-agent ``~/.ssh/repo_*`` key files go only once the alias works.

    The listing and the deletion run in a real bash (and python3) against a
    temporary home, so the shell that decides what is a key file, and the
    program that edits ``~/.ssh/config``, are what is tested.
    """

    _KEY = "-----BEGIN OPENSSH " + "PRIVATE KEY-----\nAAAA\n-----END\n"

    @staticmethod
    def _block(ssh, name, host="github.com"):
        """Exactly what a pre-agent clone appended to ~/.ssh/config."""
        return (
            f"\nHost {host}\n  IdentityFile {ssh}/{name}\n"
            "  StrictHostKeyChecking accept-new\n"
        )

    @pytest.fixture
    def home(self, tmp_path):
        ssh = tmp_path / ".ssh"
        ssh.mkdir()
        (ssh / "repo_my-repo").write_text(self._KEY)
        (ssh / "repo_old-name").write_text(self._KEY.replace("OPENSSH ", "RSA "))
        (ssh / "repo_pub").write_text("-----BEGIN PUBLIC KEY-----\nAAAA\n")
        (ssh / "repo_notes").write_text("my notes\n")
        (ssh / "repo_Upper").write_text(self._KEY)
        (ssh / "other_key").write_text(self._KEY)
        # Slug-named, a key, but no IdentityFile line names it: the user's.
        (ssh / "repo_unnamed").write_text(self._KEY)
        # A key the user's own block names by its absolute path.
        (ssh / "repo_userabs").write_text(self._KEY)
        # An old block the user edited (a line added after it).
        (ssh / "repo_edited").write_text(self._KEY)
        # The old block's lines, but another indentation.
        (ssh / "repo_tabbed").write_text(self._KEY)
        # An exact old block, and a line of the user's own also naming it.
        (ssh / "repo_shared").write_text(self._KEY)
        (tmp_path / "elsewhere").write_text(self._KEY)
        (ssh / "repo_link").symlink_to(tmp_path / "elsewhere")
        (ssh / "config").write_text(
            "# mine\nHost myserver\n  User me\n"
            f"\nHost userhost\n  IdentityFile {ssh}/repo_userabs\n"
            f"\nHost forge\n  IdentityFile {ssh}/repo_edited\n"
            "  StrictHostKeyChecking accept-new\n  Port 2222\n"
            f"\nHost tabbed\n\tIdentityFile {ssh}/repo_tabbed\n"
            "  StrictHostKeyChecking accept-new\n"
            + self._block(ssh, "repo_shared", host="shared")
            + f"\nHost alsomine\n  IdentityFile {ssh}/repo_shared\n"
            # Every attach appended the block again.
            + self._block(ssh, "repo_my-repo")
            + self._block(ssh, "repo_my-repo")
            + self._block(ssh, "repo_old-name", host="gitlab.example.com")
            # Named by exact blocks, but each fails another check.
            + "".join(
                self._block(ssh, name, host="other")
                for name in ("repo_pub", "repo_notes", "repo_Upper", "repo_link")
            )
        )
        return tmp_path

    @staticmethod
    def _workspace(home, *, reused=False):
        import subprocess

        ws = make_workspace_manager()
        ws.backend.exists = MagicMock(return_value=reused)
        ws.backend.resolve_home_path = MagicMock(
            side_effect=lambda rel: f"{home}/{rel}"
        )
        env = {"HOME": str(home), "PATH": "/usr/bin:/bin"}

        def shell_run(command, timeout=None, tab_name=None, working_dir=None):
            result = subprocess.run(
                ["bash", "-c", command], env=env, capture_output=True, text=True
            )
            return f"Exit code: {result.returncode}\n--- stdout ---\n{result.stdout}"

        def secret_stdin(command, secret, *, timeout=30):
            result = subprocess.run(
                ["bash", "-c", command], env=env, input=bytes(secret), check=False
            )
            return result.returncode == 0

        ws.backend.shell_run = MagicMock(side_effect=shell_run)
        ws.backend.execute_claim_resource_with_secret_stdin = None
        ws.backend.execute_with_secret_stdin = MagicMock(side_effect=secret_stdin)
        return ws

    @staticmethod
    def _left(home):
        return sorted(path.name for path in (home / ".ssh").iterdir())

    def _clone(self, ws, *entries, status=None, reachable=True, **kwargs):
        if status is None:
            status = {e["ssh_identity"]["authority_id"]: "ready" for e in entries}
        with patch("agent.managers.git_manager.GitManager") as git_manager:
            git_manager.clone.return_value = MagicMock()
            git_manager.return_value.add_remote.return_value = True
            git_manager.return_value.remote_reachable.return_value = reachable
            clone_repository_datasources(
                list(entries), ws, ssh_identity_status=status, **kwargs
            )
        return git_manager

    def test_listing_names_only_old_block_named_slug_named_private_key_files(
        self, home
    ):
        from agent.connectors.checkout import _legacy_ssh_key_files

        ws = self._workspace(home)
        assert _legacy_ssh_key_files(ws.backend, f"{home}/.ssh") == {
            "repo_my-repo",
            "repo_old-name",
        }
        (listing,) = [c[0][0] for c in ws.backend.shell_run.call_args_list]
        assert "PRIVATE KEY" not in listing

    def test_proven_clone_sweeps_its_own_and_a_renamed_connectors_file(self, home):
        ws = self._workspace(home)
        before = (home / ".ssh" / "config").read_text()
        self._clone(ws, TestBackendClone._ssh_entry())
        assert self._left(home) == [
            "config",
            "other_key",
            "repo_Upper",
            "repo_edited",
            "repo_link",
            "repo_notes",
            "repo_pub",
            "repo_shared",
            "repo_tabbed",
            "repo_unnamed",
            "repo_userabs",
        ]
        assert (home / "elsewhere").exists()
        # Each deleted key's exact pre-agent blocks went with it; nothing else.
        ssh = home / ".ssh"
        assert (ssh / "config").read_text() == (
            before.replace(self._block(ssh, "repo_my-repo"), "").replace(
                self._block(ssh, "repo_old-name", "gitlab.example.com"), ""
            )
        )
        assert "repo_my-repo" not in (ssh / "config").read_text()
        assert "Host myserver\n  User me\n" in (ssh / "config").read_text()

    def test_a_users_own_repo_key_is_never_deleted(self, home):
        """``ssh-keygen -f ~/.ssh/repo_deploy`` in a persistent home."""
        import shutil
        import subprocess

        if shutil.which("ssh-keygen") is None:
            pytest.skip("ssh-keygen not installed")
        ssh = home / ".ssh"
        subprocess.run(
            [
                "ssh-keygen",
                "-q",
                "-t",
                "ed25519",
                "-N",
                "",
                "-f",
                str(ssh / "repo_deploy"),
            ],
            check=True,
        )
        key = (ssh / "repo_deploy").read_bytes()
        # The user's own config may even point at it with ``~``.
        with (ssh / "config").open("a") as config:
            config.write("\nHost deploy\n  IdentityFile ~/.ssh/repo_deploy\n")
        ws = self._workspace(home)

        self._clone(ws, TestBackendClone._ssh_entry())

        assert (ssh / "repo_deploy").read_bytes() == key
        assert (ssh / "repo_deploy.pub").exists()
        assert "IdentityFile ~/.ssh/repo_deploy" in (ssh / "config").read_text()

    @pytest.mark.parametrize(
        "user_lines",
        [
            # A bare absolute IdentityFile in the user's own block.
            "\nHost deploy\n  IdentityFile {path}\n",
            # The old indentation and trust line, but more of the user's own.
            "\nHost deploy\n  IdentityFile {path}\n"
            "  StrictHostKeyChecking accept-new\n  User git\n",
            # Other indentation, other order.
            "\nHost deploy\n\tIdentityFile {path}\n  StrictHostKeyChecking accept-new\n",
            "\nHost deploy\n  StrictHostKeyChecking accept-new\n  IdentityFile {path}\n",
            "\nMatch host deploy\n  IdentityFile {path}\n"
            "  StrictHostKeyChecking accept-new\n",
            "\nIdentityFile {path}\n",
        ],
    )
    def test_a_user_written_identity_file_line_never_causes_a_delete(
        self, home, user_lines
    ):
        """A real ``ssh-keygen`` key the user's own config names by path."""
        import shutil
        import subprocess

        if shutil.which("ssh-keygen") is None:
            pytest.skip("ssh-keygen not installed")
        ssh = home / ".ssh"
        path = ssh / "repo_deploy"
        subprocess.run(
            ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(path)],
            check=True,
        )
        key = path.read_bytes()
        lines = user_lines.format(path=path)
        with (ssh / "config").open("a") as config:
            config.write(lines)
        ws = self._workspace(home)

        self._clone(ws, TestBackendClone._ssh_entry())

        assert path.read_bytes() == key
        assert lines in (ssh / "config").read_text()
        # The sweep itself still ran: the old blocks' keys are gone.
        assert "repo_my-repo" not in self._left(home)

    def test_a_key_its_old_block_and_a_user_line_both_name_is_kept(self, home):
        ssh = home / ".ssh"
        user_lines = f"\nHost mine\n  User me\n  IdentityFile {ssh}/repo_old-name\n"
        with (ssh / "config").open("a") as config:
            config.write(user_lines)
        ws = self._workspace(home)
        before = (ssh / "config").read_text()

        self._clone(ws, TestBackendClone._ssh_entry())

        assert "repo_old-name" in self._left(home)
        # Its old block stays with it, so nothing points at a missing file.
        after = (ssh / "config").read_text()
        assert self._block(ssh, "repo_old-name", "gitlab.example.com") in after
        assert user_lines in after
        assert after == before.replace(self._block(ssh, "repo_my-repo"), "")

    def test_the_program_rechecks_every_name_it_is_given(self, home):
        """A name no exact old block names survives even a direct request."""
        import subprocess

        from agent.connectors.checkout import _RETIRE_LEGACY_KEYS_PROGRAM

        ssh = home / ".ssh"
        (ssh / "repo_deploy").write_text(self._KEY)
        before = (ssh / "config").read_text()
        refused = [
            "repo_deploy",
            "repo_userabs",
            "repo_edited",
            "repo_tabbed",
            "repo_shared",
            "repo_pub",
            "repo_link",
        ]
        subprocess.run(
            ["python3", "-c", _RETIRE_LEGACY_KEYS_PROGRAM, str(ssh)]
            + refused
            + ["../other_key"],
            check=True,
        )
        assert {*refused, "other_key"} <= set(self._left(home))
        assert (ssh / "config").read_text() == before

    def test_a_block_the_user_edited_keeps_its_key_and_stays(self, home):
        ssh = home / ".ssh"
        edited = (
            f"\nHost forge\n  IdentityFile {ssh}/repo_edited\n"
            "  StrictHostKeyChecking accept-new\n  Port 2222\n"
        )
        ws = self._workspace(home)

        self._clone(ws, TestBackendClone._ssh_entry())

        # Not the exact old shape any more: the key and the block both stay.
        assert "repo_edited" in self._left(home)
        assert edited in (ssh / "config").read_text()

    def test_an_unloaded_identity_keeps_every_key_file(self, home):
        ws = self._workspace(home)
        ds = TestBackendClone._ssh_entry()
        self._clone(
            ws,
            ds,
            status={
                ds["ssh_identity"]["authority_id"]: "workspace_ssh_identity_load_failed"
            },
        )
        assert "repo_my-repo" in self._left(home)
        assert "repo_old-name" in self._left(home)

    def test_one_unproven_repository_blocks_the_sweep_not_the_proven_one(self, home):
        ws = self._workspace(home)
        proven = TestBackendClone._ssh_entry()
        unloaded = TestBackendClone._ssh_entry(
            id="00000000-0000-4000-8000-0000000000d2",
            name="Other Repo",
            connection_url="git@github.com:org/other.git",
        )
        status = {
            proven["ssh_identity"]["authority_id"]: "ready",
            unloaded["ssh_identity"][
                "authority_id"
            ]: "workspace_ssh_identity_load_failed",
        }
        self._clone(ws, proven, unloaded, status=status)
        left = self._left(home)
        assert "repo_my-repo" not in left
        assert "repo_old-name" in left

    @pytest.mark.parametrize("reachable", [True, False])
    def test_a_reused_checkout_is_proven_with_ls_remote_first(self, home, reachable):
        ws = self._workspace(home, reused=True)
        git_manager = self._clone(
            ws, TestBackendClone._ssh_entry(), reachable=reachable
        )
        git_manager.return_value.remote_reachable.assert_called_once_with()
        left = self._left(home)
        assert ("repo_my-repo" in left) is not reachable
        assert ("repo_old-name" in left) is not reachable

    def test_no_key_file_means_no_ls_remote(self, tmp_path):
        (tmp_path / ".ssh").mkdir()
        ws = self._workspace(tmp_path, reused=True)
        git_manager = self._clone(ws, TestBackendClone._ssh_entry())
        git_manager.return_value.remote_reachable.assert_not_called()

    def test_a_live_add_deletes_only_its_own_file(self, home):
        ws = self._workspace(home)
        self._clone(ws, TestBackendClone._ssh_entry(), legacy_key_files="own")
        left = self._left(home)
        assert "repo_my-repo" not in left
        assert "repo_old-name" in left
        assert "repo_old-name" in (home / ".ssh" / "config").read_text()

    def test_a_child_job_on_a_shared_workspace_touches_no_key_file(self, home):
        ws = self._workspace(home)
        before = (home / ".ssh" / "config").read_text()
        self._clone(ws, TestBackendClone._ssh_entry(), legacy_key_files="keep")
        assert "repo_my-repo" in self._left(home)
        assert "repo_old-name" in self._left(home)
        assert (home / ".ssh" / "config").read_text() == before
        ws.backend.shell_run.assert_not_called()

    def test_an_unknown_mode_is_refused(self, home):
        with pytest.raises(ValueError):
            clone_repository_datasources(
                [token_ds()], self._workspace(home), legacy_key_files="all"
            )

    def test_token_repositories_never_look_for_key_files(self, home):
        ws = self._workspace(home)
        with patch("agent.managers.git_manager.GitManager.clone", return_value=None):
            clone_repository_datasources([token_ds()], ws)
        ws.backend.shell_run.assert_not_called()
        assert "repo_my-repo" in self._left(home)


class TestJobWorkspaceOwnership:
    """Only the workspace owner retires pre-agent key files (job side)."""

    JOB = "00000000-0000-4000-8000-0000000000a1"
    PARENT = "00000000-0000-4000-8000-0000000000a0"

    @pytest.mark.parametrize(
        ("metadata", "owns"),
        [
            ({}, True),
            ({"context": {"description": "x"}}, True),
            ({"workspace_owner_id": JOB}, True),
            ({"workspace_owner_id": PARENT}, False),
            ({"context": {"inherits_parent_workspace": True}}, False),
            ({"context": {"inherits_parent_workspace": "true"}}, False),
            ({"context": {"provisions_parent_workspace": PARENT}}, False),
            ({"context": "not a dict"}, True),
        ],
    )
    def test_ownership(self, metadata, owns):
        from agent.agent import _job_owns_its_workspace

        assert _job_owns_its_workspace(metadata, self.JOB) is owns

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("context", "mode"),
        [({}, "sweep"), ({"inherits_parent_workspace": True}, "keep")],
    )
    async def test_job_tools_pass_the_owner_mode(self, context, mode):
        from agent.agent import UniversalAgent

        class Stop(Exception):
            pass

        agent = object.__new__(UniversalAgent)
        agent._current_job_id = self.JOB
        agent._job_metadata = {"context": context, "datasources": [token_ds()]}
        agent._workspace_manager = make_workspace_manager()
        agent._datasource_connections = {}
        agent._datasource_clients = {}
        clone = MagicMock(side_effect=Stop)
        with (
            patch("agent.connectors.checkout.clone_repository_datasources", clone),
            pytest.raises(Stop),
        ):
            await UniversalAgent._setup_job_tools(agent)

        assert clone.call_args.kwargs["legacy_key_files"] == mode


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

    def test_ssh_repository_names_its_alias(self):
        ds = TestBackendClone._ssh_entry()
        content = self._render(ds)
        assert (
            f"git uses SSH alias `{ds['ssh_identity']['alias']}`, whose key an "
            "ssh-agent holds (never on disk)"
        ) in content

    def test_an_ssh_repository_that_was_not_cloned_still_names_its_alias(self):
        # Its alias is how the agent diagnoses the failure (a wrong host-key
        # pin fails verification through it), so the skip line keeps it.
        ds = TestBackendClone._ssh_entry()
        ws = make_workspace_manager()
        ws.read_file.side_effect = FileNotFoundError
        written = {}
        ws.write_file.side_effect = lambda path, content: written.update(
            {path: content}
        )
        ws.source_repo_skipped = {"repo": "Host key verification failed."}
        inject_workspace_facts([ds], ws)
        content = written["README.md"]
        assert "repository NOT cloned" in content
        assert "Host key verification failed." in content
        assert f"git uses SSH alias `{ds['ssh_identity']['alias']}`" in content

    def test_ssh_key_lines_say_how_to_use_the_agent(self):
        from shared.runtime.utils.ssh_key import generate_ed25519_keypair

        def entry(config):
            (payload,) = agent_payload(
                {
                    "id": "00000000-0000-4000-8000-0000000000e1",
                    "type": "ssh_key",
                    "name": "Bastion",
                    "credentials": {
                        "files": [{"contents": generate_ed25519_keypair().private_key}]
                    },
                    "config": config,
                    "project_read_only": False,
                }
            )
            return payload

        with_host = entry({"host": "bastion.example.com", "user": "ops", "port": 2200})
        content = self._render(with_host)
        assert "`ssh ops@bastion.example.com` (port 2200) uses it" in content
        assert "never written to disk" in content
        assert "~/.ssh/bastion" not in content

        without_host = entry({})
        slug = without_host["ssh_identity"]["alias"].removeprefix("srw-repo-")
        assert (
            f"ssh -o IdentityAgent=~/.ssh/srw-managed/sockets/{slug}.sock"
            in self._render(without_host)
        )

    @staticmethod
    def _ssh_key_entry(datasource_id, name, config):
        from shared.runtime.utils.ssh_key import generate_ed25519_keypair

        (payload,) = agent_payload(
            {
                "id": datasource_id,
                "type": "ssh_key",
                "name": name,
                "credentials": {
                    "files": [{"contents": generate_ed25519_keypair().private_key}]
                },
                "config": config,
                "project_read_only": False,
            }
        )
        return payload

    def _render_all(self, entries, **kwargs):
        ws = make_workspace_manager()
        ws.read_file.side_effect = FileNotFoundError
        written = {}
        ws.write_file.side_effect = lambda path, content: written.update(
            {path: content}
        )
        inject_workspace_facts(entries, ws, **kwargs)
        return written["README.md"]

    def test_an_identity_whose_key_did_not_load_is_not_advertised(self):
        repository = TestBackendClone._ssh_entry()
        ssh_key = self._ssh_key_entry(
            "00000000-0000-4000-8000-0000000000e1",
            "Bastion",
            {"host": "bastion.example.com", "user": "ops"},
        )
        entries = [repository, ssh_key]
        failed = {
            e["ssh_identity"]["authority_id"]: "workspace_ssh_identity_load_failed"
            for e in entries
        }

        content = self._render_all(entries, ssh_identity_status=failed)

        assert "git uses SSH alias" not in content
        assert "uses it" not in content
        assert "workspace_ssh_identity_load_failed" not in content
        assert content.count("its key could not be loaded") == 2

        ready = {e["ssh_identity"]["authority_id"]: "ready" for e in entries}
        content = self._render_all(entries, ssh_identity_status=ready)
        assert "git uses SSH alias" in content
        assert "`ssh ops@bastion.example.com` uses it" in content

    def test_two_ssh_keys_on_one_host_are_each_named_by_their_alias(self):
        first = self._ssh_key_entry(
            "00000000-0000-4000-8000-0000000000e1",
            "Deploy",
            {"host": "bastion.example.com", "user": "deploy"},
        )
        second = self._ssh_key_entry(
            "00000000-0000-4000-8000-0000000000e2",
            "Ops",
            {"host": "BASTION.example.com", "user": "ops"},
        )
        other = self._ssh_key_entry(
            "00000000-0000-4000-8000-0000000000e3",
            "Build",
            {"host": "build.example.com"},
        )

        content = self._render_all([first, second, other])

        for entry in (first, second):
            alias = entry["ssh_identity"]["alias"]
            assert f"use the alias: `ssh {alias}` reaches" in content
        assert "`ssh deploy@bastion.example.com`" not in content
        assert "`ssh ops@bastion.example.com`" not in content
        assert "`ssh build.example.com` uses it" in content


class TestHarnessRepoGuard:
    """The harness phase never clones: a repository routes to the checkout
    materializer only, which runs once the workspace exists."""

    def test_repository_ds_opens_nothing_in_the_harness(self):
        with patch("agent.managers.git_manager.GitManager.clone") as mock_clone:
            connections, clients = open_harness([token_ds()])
        mock_clone.assert_not_called()
        assert connections == {}
        assert clients == {}


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
