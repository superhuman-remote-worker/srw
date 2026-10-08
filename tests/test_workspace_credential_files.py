"""Credential files reach the workspace home (slice D1d).

The workspace program (``INSTALL_CREDENTIAL_FILES``) runs here for real
against a temporary home, with synthetic values only: what it writes, links,
refuses, sets in the environment file and removes, and how it retires.
``RemoteBackend.install_credential_files`` is driven through a local shell
standing in for the secret stdin channel, the terminal shell retirement's
script is checked and run, and the real snapshot pipeline proves a captured
home keeps the links but never the bytes.
"""

from __future__ import annotations

import hashlib
import json
import os
import shlex
import shutil
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from orchestrator.services.snapshot_service import _snapshot_tar_pipeline
from shared.runtime.core.backends.remote import RemoteBackend
from shared.runtime.core.credential_env import (
    INSTALL_CREDENTIAL_ENV,
    INSTALL_CREDENTIAL_FILES,
    WORKSPACE_PYTHON,
)
from shared.runtime.core.workspace_backend import (
    WorkspaceBackend,
    WorkspaceUnavailableError,
)

IDENTITY = "0123abcd"
STORE = f"files-{IDENTITY}"


def _run(home: Path, request: dict | None, store: str = STORE, action: str = "sync"):
    return subprocess.run(
        ["python3", "-I", "-c", INSTALL_CREDENTIAL_FILES, str(home), store, action],
        input=json.dumps(request or {}),
        text=True,
        capture_output=True,
    )


def _sync(
    home: Path, files: list[dict], env: list[dict] = (), store: str = STORE
) -> dict:
    completed = _run(home, {"files": files, "env": list(env)}, store)
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout.splitlines()[-1])


def _file(name: str, link: str | None, content: str = "secret", mode: int = 0o600):
    return {"name": name, "content": content, "mode": mode, "link": link}


def _env(home: Path, identity: str = IDENTITY) -> dict:
    path = home / ".srw-credentials" / f"{identity}.json"
    return json.loads(path.read_text()) if path.exists() else {}


def _sourced(home: Path, name: str, identity: str = IDENTITY) -> str:
    path = home / ".srw-credentials" / f"{identity}.sh"
    return subprocess.check_output(
        ["bash", "-c", f'. {shlex.quote(str(path))}; printf %s "${{{name}-unset}}"'],
        text=True,
    )


@pytest.fixture
def home(tmp_path: Path) -> Path:
    path = tmp_path / "agent-host"
    path.mkdir()
    return path


def _store(home: Path, store: str = STORE) -> Path:
    return home / ".srw-credentials" / store


def _state(home: Path) -> dict:
    return json.loads((_store(home) / ".links.json").read_text())


class TestFiles:
    def test_files_go_to_the_private_store_without_execute_bits(self, home):
        report = _sync(
            home,
            [
                _file("sa.json", ".config/gcloud/sa.json", '{"k": 1}'),
                _file("ca.pem", ".config/gcloud/ca.pem", "pem", 0o644),
                _file("tool", ".srw-files/tool", "#!/bin/sh\n", 0o4777),
                _file("unlinked", None, "only in the store"),
            ],
        )
        store = _store(home)
        assert (home / ".srw-credentials").stat().st_mode & 0o777 == 0o700
        assert store.stat().st_mode & 0o777 == 0o700
        assert (store / "sa.json").stat().st_mode & 0o7777 == 0o600
        assert (store / "ca.pem").stat().st_mode & 0o7777 == 0o644
        assert (store / "tool").stat().st_mode & 0o7777 == 0o666
        link = home / ".config/gcloud/sa.json"
        assert link.is_symlink()
        assert os.readlink(link) == str(store / "sa.json")
        assert link.read_text() == '{"k": 1}'
        assert (store / "unlinked").read_text() == "only in the store"
        assert (home / ".config").stat().st_mode & 0o777 == 0o700
        assert report["linked"] == [
            ".config/gcloud/ca.pem",
            ".config/gcloud/sa.json",
            ".srw-files/tool",
        ]
        assert report["skipped"] == {}
        assert "secret" not in json.dumps(report) and "pem" not in report["store"]

    def test_a_pre_existing_open_credential_root_is_closed(self, home):
        root = home / ".srw-credentials"
        root.mkdir(mode=0o755)
        root.chmod(0o755)
        _sync(home, [_file("a", None)])
        assert root.stat().st_mode & 0o777 == 0o700

    def test_an_existing_file_or_foreign_link_is_never_replaced(self, home):
        (home / ".kube").mkdir()
        (home / ".kube/config").write_text("the user's own")
        (home / "elsewhere").write_text("x")
        (home / ".netrc").symlink_to(home / "elsewhere")
        report = _sync(home, [_file("kube", ".kube/config"), _file("n", ".netrc")])
        assert not (home / ".kube/config").is_symlink()
        assert (home / ".kube/config").read_text() == "the user's own"
        assert os.readlink(home / ".netrc") == str(home / "elsewhere")
        assert (_store(home) / "kube").read_text() == "secret"
        assert report["skipped"] == {
            ".kube/config": "a file of the user is there",
            ".netrc": "a file of the user is there",
        }

    def test_a_live_link_into_another_work_items_store_is_left_alone(self, home):
        _sync(home, [_file("a", ".netrc", "theirs")], store="files-other")
        report = _sync(home, [_file("a", ".netrc", "mine")])
        assert (home / ".netrc").read_text() == "theirs"
        assert report["skipped"] == {".netrc": "another work item's file is there"}

    def test_a_stale_link_from_a_restored_snapshot_is_replaced(self, home):
        """A snapshot keeps the link but never the store it points into."""
        (home / ".kube").mkdir()
        (home / ".kube/config").symlink_to(
            home / ".srw-credentials/files-gone/kubeconfig"
        )
        _sync(home, [_file("kubeconfig", ".kube/config", "fresh")])
        assert (home / ".kube/config").read_text() == "fresh"

    def test_a_resync_replaces_its_own_links_and_contents(self, home):
        _sync(home, [_file("a", ".srw-files/a", "one")])
        _sync(home, [_file("b", ".srw-files/a", "two")])
        assert (home / ".srw-files/a").read_text() == "two"
        assert not (_store(home) / "a").exists()

    def test_what_is_no_longer_delivered_is_removed(self, home):
        _sync(home, [_file("a", ".config/tool/a"), _file("b", ".kube/configs/b.yaml")])
        (home / ".config/tool/user-file").write_text("mine")
        _sync(home, [_file("a", ".config/tool/a")])
        assert not os.path.lexists(home / ".kube/configs/b.yaml")
        assert not (_store(home) / "b").exists()
        assert not (home / ".kube").exists()
        _sync(home, [])
        assert not os.path.lexists(home / ".config/tool/a")
        assert not _store(home).exists()
        assert (home / ".config/tool/user-file").read_text() == "mine"

    def test_nothing_to_sync_and_nothing_synced_writes_nothing(self, home):
        report = _sync(home, [])
        assert report["linked"] == [] and not (home / ".srw-credentials").exists()

    @pytest.mark.parametrize(
        ("link", "reason"),
        [
            ("../outside", "outside the home"),
            ("/etc/passwd-like", "outside the home"),
            ("a/../../outside", "outside the home"),
            (".srw-credentials/hijack", "inside the credential store"),
            (".srw-credentials/files-other/x", "inside the credential store"),
        ],
    )
    def test_a_link_never_lands_outside_the_home_or_in_the_store_root(
        self, home, link, reason
    ):
        report = _sync(home, [_file("x", link)])
        assert report["skipped"] == {link: reason}
        assert not (home.parent / "outside").exists()
        assert not os.path.lexists(home / ".srw-credentials/hijack")
        assert (_store(home) / "x").read_text() == "secret"

    def test_a_link_never_passes_a_symlinked_directory(self, home, tmp_path):
        outside = tmp_path / "outside"
        outside.mkdir()
        (home / ".kube").symlink_to(outside)
        os.symlink(home, home / "me")
        report = _sync(home, [_file("x", ".kube/config"), _file("y", "me/.netrc")])
        assert report["skipped"] == {
            ".kube/config": "a directory on the way is a symlink",
            "me/.netrc": "a directory on the way is a symlink",
        }
        assert not os.path.lexists(outside / "config")
        assert not os.path.lexists(home / ".netrc")

    def test_one_bad_link_never_aborts_the_rest(self, home):
        """A file in the way, an unwritable directory: skipped, the rest placed."""
        (home / ".kube").write_text("a file, not a directory")
        (home / ".aws").mkdir()
        (home / ".aws").chmod(0o500)
        try:
            report = _sync(
                home,
                [
                    _file("k", ".kube/config"),
                    _file("a", ".aws/credentials"),
                    _file("ok", ".netrc"),
                ],
                env=[{"name": "TOOL_FILE", "files": ["ok"]}],
            )
        finally:
            (home / ".aws").chmod(0o700)
        assert report["linked"] == [".netrc"]
        assert report["skipped"][".kube/config"] == "a file is in the way"
        if os.geteuid() != 0:
            assert ".aws/credentials" in report["skipped"]
        assert _state(home)["links"] == [".netrc"]
        assert _env(home)["TOOL_FILE"] == str(_store(home) / "ok")
        # A later empty sync leaves no tracked link behind.
        _sync(home, [])
        assert not os.path.lexists(home / ".netrc")

    @pytest.mark.parametrize("name", ["", "../x", "a/b", ".links.json"])
    def test_a_malformed_store_name_fails_closed(self, home, name):
        assert _run(home, {"files": [_file(name, None)]}).returncode == 3

    @pytest.mark.parametrize(
        ("store", "action"), [("other", "sync"), ("files-a/b", "sync"), (STORE, "x")]
    )
    def test_a_malformed_invocation_fails_closed(self, home, store, action):
        assert _run(home, {}, store=store, action=action).returncode == 3


class TestVariables:
    def test_variables_name_their_stored_files_in_the_env_file(self, home):
        report = _sync(
            home,
            [_file("a", ".srw-files/a"), _file("b", ".srw-files/b")],
            env=[
                {"name": "ONE", "files": ["a"]},
                {"name": "BOTH", "files": ["a", "b"]},
            ],
        )
        store = _store(home)
        assert report["env"] == ["BOTH", "ONE"]
        assert report["env_file"] is True
        assert _sourced(home, "ONE") == f"{store}/a"
        assert _sourced(home, "BOTH") == f"{store}/a:{store}/b"
        assert (
            home / ".srw-credentials" / f"{IDENTITY}.sh"
        ).stat().st_mode & 0o777 == 0o600

    def test_the_users_own_kubeconfig_comes_first(self, home):
        """A shared connector's current-context never replaces the user's."""
        (home / ".kube").mkdir()
        (home / ".kube/config").write_text("the user's own")
        _sync(
            home,
            [_file("kubeconfig", ".kube/config")],
            env=[
                {
                    "name": "KUBECONFIG",
                    "files": ["kubeconfig"],
                    "prepend": ".kube/config",
                }
            ],
        )
        assert _sourced(home, "KUBECONFIG") == (
            f"{home}/.kube/config:{_store(home)}/kubeconfig"
        )

    def test_our_own_link_is_not_listed_twice(self, home):
        _sync(
            home,
            [_file("kubeconfig", ".kube/config")],
            env=[
                {
                    "name": "KUBECONFIG",
                    "files": ["kubeconfig"],
                    "prepend": ".kube/config",
                }
            ],
        )
        assert _sourced(home, "KUBECONFIG") == f"{_store(home)}/kubeconfig"

    def test_a_variable_another_connector_set_is_never_overwritten(self, home):
        env_file = home / ".srw-credentials" / f"{IDENTITY}.sh"
        subprocess.run(
            ["python3", "-I", "-c", INSTALL_CREDENTIAL_ENV, str(env_file)],
            input=json.dumps({"TOOL_FILE": "from an env connector"}),
            text=True,
            check=True,
        )
        report = _sync(
            home,
            [_file("a", ".srw-files/a")],
            env=[{"name": "TOOL_FILE", "files": ["a"]}],
        )
        assert report["env_skipped"] == {"TOOL_FILE": "set by another connector"}
        assert _sourced(home, "TOOL_FILE") == "from an env connector"

    def test_a_variable_taken_over_after_a_sync_is_released(self, home):
        """An env connector attached later wins; the sync stops claiming it."""
        _sync(
            home,
            [_file("a", ".srw-files/a")],
            env=[{"name": "TOOL_FILE", "files": ["a"]}],
        )
        env_file = home / ".srw-credentials" / f"{IDENTITY}.sh"
        subprocess.run(
            ["python3", "-I", "-c", INSTALL_CREDENTIAL_ENV, str(env_file)],
            input=json.dumps({"TOOL_FILE": "from an env connector"}),
            text=True,
            check=True,
        )
        report = _sync(
            home,
            [_file("a", ".srw-files/a")],
            env=[{"name": "TOOL_FILE", "files": ["a"]}],
        )
        assert "TOOL_FILE" in report["env_skipped"]
        report = _sync(home, [])
        assert report["env_retired"] == []
        assert _sourced(home, "TOOL_FILE") == "from an env connector"

    def test_a_variable_no_longer_delivered_is_unset_from_the_record(self, home):
        """Between attaches too: the record, not a live diff, retires it."""
        _sync(
            home,
            [_file("kubeconfig", ".kube/config")],
            env=[{"name": "KUBECONFIG", "files": ["kubeconfig"]}],
        )
        report = _sync(home, [])
        assert report["env_retired"] == ["KUBECONFIG"]
        assert _sourced(home, "KUBECONFIG") == "unset"
        assert not _store(home).exists()
        # Attaching again claims the unset variable.
        report = _sync(
            home,
            [_file("kubeconfig", ".kube/config")],
            env=[{"name": "KUBECONFIG", "files": ["kubeconfig"]}],
        )
        assert report["env"] == ["KUBECONFIG"] and report["env_skipped"] == {}
        assert _sourced(home, "KUBECONFIG") == f"{_store(home)}/kubeconfig"

    def test_a_retired_variable_is_unset_even_in_a_shell_that_had_it(self, home):
        """Never ``NAME=``: an empty AWS_SHARED_CREDENTIALS_FILE would hide
        ~/.aws/credentials for the rest of the work item."""
        (home / ".aws").mkdir()
        (home / ".aws/credentials").write_text("[default]\nkey = user\n")
        _sync(
            home,
            [_file("creds", ".srw-files/creds")],
            env=[{"name": "AWS_SHARED_CREDENTIALS_FILE", "files": ["creds"]}],
        )
        _sync(home, [])
        env_sh = home / ".srw-credentials" / f"{IDENTITY}.sh"
        assert "unset AWS_SHARED_CREDENTIALS_FILE\n" in env_sh.read_text()
        assert "AWS_SHARED_CREDENTIALS_FILE=" not in env_sh.read_text()
        # The long-lived shell exported it earlier; sourcing unsets it.
        out = subprocess.check_output(
            [
                "bash",
                "-c",
                "export AWS_SHARED_CREDENTIALS_FILE=old; "
                f". {shlex.quote(str(env_sh))}; "
                'printf %s "${AWS_SHARED_CREDENTIALS_FILE-unset}"',
            ],
            text=True,
        )
        assert out == "unset"

    def test_the_env_installer_writes_an_unset_for_a_retired_name(self, home):
        """The env connectors' program regenerates the file: it keeps the unset."""
        _sync(
            home,
            [_file("creds", ".srw-files/creds")],
            env=[{"name": "AWS_SHARED_CREDENTIALS_FILE", "files": ["creds"]}],
        )
        _sync(home, [])
        env_sh = home / ".srw-credentials" / f"{IDENTITY}.sh"
        subprocess.run(
            ["python3", "-I", "-c", INSTALL_CREDENTIAL_ENV, str(env_sh)],
            input=json.dumps({"API_KEY": "v"}),
            text=True,
            check=True,
        )
        text = env_sh.read_text()
        assert "unset AWS_SHARED_CREDENTIALS_FILE\n" in text
        assert "export API_KEY=v\n" in text


class TestRetirement:
    def test_retire_removes_links_store_and_env_file(self, home):
        (home / ".kube").mkdir()
        (home / ".kube/mine").write_text("kept")
        _sync(
            home,
            [_file("kubeconfig", ".kube/config"), _file("n", ".netrc")],
            env=[{"name": "KUBECONFIG", "files": ["kubeconfig"]}],
        )
        _sync(home, [_file("other", ".pgpass")], store="files-other")
        completed = _run(home, None, action="retire")
        assert completed.returncode == 0, completed.stderr
        assert not os.path.lexists(home / ".kube/config")
        assert not os.path.lexists(home / ".netrc")
        assert not _store(home).exists()
        assert not (home / ".srw-credentials" / f"{IDENTITY}.sh").exists()
        assert not (home / ".srw-credentials" / f"{IDENTITY}.json").exists()
        assert (home / ".kube/mine").read_text() == "kept"
        # Another work item's files are not this retirement's.
        assert (home / ".pgpass").read_text() == "secret"

    def test_retire_with_nothing_there_succeeds(self, home):
        assert _run(home, None, action="retire").returncode == 0

    def _backend(self, **kwargs) -> RemoteBackend:
        return RemoteBackend(
            host="workspace.test",
            workspace_path="/home/agent-host/workspace",
            job_id="cccccccc-cccc-4ccc-8ccc-cccccccccccc",
            **kwargs,
        )

    def _store_name(self) -> str:
        identity = hashlib.sha256(b"cccccccc-cccc-4ccc-8ccc-cccccccccccc").hexdigest()
        return f"files-{identity}"

    def test_the_pinned_terminal_shell_retirement_removes_them(self):
        backend = self._backend()
        with patch.object(backend, "_tmux_exec_checked") as execute:
            backend.shell_cleanup()
        command = execute.call_args.args[0]
        retire = command.index(f"{self._store_name()} retire")
        assert WORKSPACE_PYTHON in command
        # After every check that can refuse the retirement (exit 7x) and
        # after the kill: an aborted retirement leaves the files alone.
        assert retire > command.rindex("|| exit 7")
        assert retire > command.index("tmux kill-session")

    def test_the_stateless_terminal_shell_retirement_removes_them(self):
        parent = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
        backend = self._backend(
            workspace_generation="11111111-1111-4111-8111-111111111111",
            runtime_incarnation="22222222-2222-4222-8222-222222222222",
            workspace_owner_kind="job",
            workspace_owner_id=parent,
        )
        backend.set_shell_owner_token(31)
        with patch.object(backend, "_tmux_exec_checked") as execute:
            backend.shell_cleanup()
        command = execute.call_args.args[0]
        # The child's own store: a parent's files are not the child's.
        retire = command.index(f"{self._store_name()} retire")
        # After the ownership, token and generation checks (exit 73-80), the
        # kill and the process-zero proof.
        assert retire > command.rindex("|| exit 7")
        assert retire > command.rindex("exit 8")
        assert retire > command.index("tmux kill-session")

    @pytest.mark.parametrize("status", [0, 1, 77])
    def test_the_retirement_never_changes_the_scripts_status(self, home, status):
        _sync(home, [_file("n", ".netrc")], store=self._store_name())
        script = (
            f"(exit {status})\n" + self._backend()._credential_files_retirement_shell()
        )
        script = script.replace(WORKSPACE_PYTHON, "python3 -I")
        completed = subprocess.run(
            ["bash", "-c", script], env={**os.environ, "HOME": str(home)}
        )
        assert completed.returncode == status
        assert not os.path.lexists(home / ".netrc")


class TestRemoteDelivery:
    def _backend(self, monkeypatch, home: Path, job_id: str, sent: list):
        backend = RemoteBackend(host="unused", job_id=job_id)
        monkeypatch.setattr(backend, "_init_shell", lambda: None)
        monkeypatch.setattr(backend, "_get_home_dir", lambda: str(home))

        def send(command, secret, **kwargs):
            sent.append((command, secret))
            command = command.replace(WORKSPACE_PYTHON, "python3 -I")
            completed = subprocess.run(
                ["bash", "-c", command], input=secret, text=True, capture_output=True
            )
            return completed.returncode, completed.stdout

        monkeypatch.setattr(
            backend, "execute_claim_resource_with_secret_stdin_output", send
        )
        return backend

    def test_contents_travel_on_secret_stdin_into_the_work_items_store(
        self, monkeypatch, home
    ):
        sent: list = []
        stores = []
        for job_id, value in (("job-one", "first-value"), ("job-two", "second")):
            backend = self._backend(monkeypatch, home, job_id, sent)
            report = backend.install_credential_files(
                [{"name": "f", "content": value, "mode": 0o600, "link": None}],
                [{"name": "TOOL_FILE", "files": ["f"]}],
            )
            command, secret = sent[-1]
            assert value not in command
            assert command.startswith(WORKSPACE_PYTHON + " -c ")
            assert json.loads(secret)["files"][0]["content"] == value
            assert Path(report["store"], "f").read_text() == value
            assert Path(report["store"]).parent == home / ".srw-credentials"
            assert backend.credential_files_report == report
            # Commands now source the environment file the sync wrote.
            assert backend._credential_env_path == str(
                home
                / ".srw-credentials"
                / f"{hashlib.sha256(job_id.encode()).hexdigest()}.sh"
            )
            stores.append(report["store"])
        assert stores[0] != stores[1]

    def test_no_environment_file_is_sourced_before_one_exists(self, monkeypatch, home):
        backend = self._backend(monkeypatch, home, "job", [])
        backend.install_credential_files([])
        assert backend._credential_env_path is None

    def test_a_failed_sync_raises(self, monkeypatch, home):
        backend = RemoteBackend(host="unused", job_id="job")
        monkeypatch.setattr(backend, "_init_shell", lambda: None)
        monkeypatch.setattr(backend, "_get_home_dir", lambda: str(home))
        monkeypatch.setattr(
            backend,
            "execute_claim_resource_with_secret_stdin_output",
            lambda *args, **kwargs: (1, ""),
        )
        with pytest.raises(WorkspaceUnavailableError):
            backend.install_credential_files([])

    def test_a_backend_without_a_shell_refuses(self):
        with pytest.raises(ValueError, match="sandbox or VM workspace"):
            WorkspaceBackend.install_credential_files(object(), [])

    def test_the_env_installer_runs_the_absolute_isolated_interpreter(
        self, monkeypatch, home
    ):
        backend = RemoteBackend(host="unused", job_id="job")
        monkeypatch.setattr(backend, "_init_shell", lambda: None)
        monkeypatch.setattr(
            backend, "_resolve_home_path", lambda path: str(home / path)
        )
        sent: list = []
        monkeypatch.setattr(
            backend,
            "execute_claim_resource_with_secret_stdin",
            lambda command, secret, **kwargs: sent.append(command) or True,
        )
        backend.install_credential_environment({"API_KEY": "v"})
        assert sent[0].startswith("/usr/bin/python3 -I -c ")


def test_a_snapshot_keeps_the_links_but_never_the_contents(tmp_path):
    """C0's excludes cover where the files land: the real capture pipeline.

    The capture excludes ``/tmp/*``, so the fixture home lives under the
    user's home directory, like the production root.
    """
    for tool in ("tar", "zstd", "bash"):
        if shutil.which(tool) is None:
            pytest.skip(f"{tool} not available")
    secret = "d1d-snapshot-secret-6f0c2b"
    with tempfile.TemporaryDirectory(
        prefix="snapshot-credential-files-", dir=os.path.expanduser("~")
    ) as root:
        home = Path(root) / "agent-host"
        home.mkdir()
        (home / "notes.md").write_text("kept\n")
        _sync(
            home,
            [
                _file("kubeconfig", ".kube/config", secret),
                _file("sa.json", ".config/gcloud/sa.json", secret),
            ],
            env=[{"name": "KUBECONFIG", "files": ["kubeconfig"]}],
        )
        for strict in (False, True):
            archive = tmp_path / f"home-{strict}.tar.zst"
            with archive.open("wb") as output:
                completed = subprocess.run(
                    [
                        "bash",
                        "-c",
                        _snapshot_tar_pipeline([f"{home}/"], strict_terminal=strict),
                    ],
                    stdout=output,
                )
            assert completed.returncode in (0, 1)
            raw = subprocess.run(
                ["zstd", "-dc", "--", str(archive)], capture_output=True, check=True
            ).stdout
            listing = subprocess.run(
                ["tar", "-tvf", "-"], input=raw, capture_output=True, check=True
            ).stdout.decode()
            assert secret.encode() not in raw
            members = [line.split(" -> ")[0] for line in listing.splitlines() if line]
            assert not [m for m in members if "/.srw-credentials" in m], members
            links = [line for line in listing.splitlines() if line.startswith("l")]
            assert any("/.kube/config ->" in line for line in links), listing
            assert any("/.config/gcloud/sa.json ->" in line for line in links)
            assert "agent-host/notes.md" in listing
