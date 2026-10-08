"""The workspace side of the git swap driver (connector drivers C3).

What a binding becomes on the workspace: the wiring ``~/.gitconfig``
includes (an insteadOf to the driver, a credential helper answering with
the connector's lease, SRW's authority for the driver's URL only), checked
with real git; and what a clone does with it: the wiring before any clone,
the clean URL as the remote, a reused checkout's token dropped, the driver
waited for, and the token never in a command.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from agent.connectors.checkout import CheckoutMaterializer, clone_repository_datasources
from agent.connectors.git_swap import (
    SwapBinding,
    install_wiring,
    render_include,
    swap_binding,
    swap_note,
    wait_for_driver,
)
from agent.connectors.legacy import checkout_auth, deliveries_from_payload
from agent.connectors.base import RuntimeContext
from shared.connectors.builtin import GIT_SWAP_SPEC, REPOSITORY_SPEC
from shared.runtime.core.credential_env import (
    GIT_SWAP_CREDENTIAL_HELPER,
    GIT_SWAP_WIRING,
)

CONNECTOR = "0d6f3a52-8b1c-4e8e-9a8f-1f2e3d4c5b6a"
OTHER = "9d6f3a52-8b1c-4e8e-9a8f-1f2e3d4c5b6a"
ORIGIN = "https://srw-ep-0d6f-abc.srw-connectors.svc.cluster.local:8443"
OTHER_ORIGIN = "https://srw-ep-9d6f-abc.srw-connectors.svc.cluster.local:8443"
CA = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"
LEASE = "scl_" + "L" * 49
OTHER_LEASE = "scl_" + "M" * 49
TOKEN = "ghp_TheForgeToken0123456789abcdefABCDEF"


def _entry(
    connector=CONNECTOR, origin=ORIGIN, url="https://github.com/o/r.git", **over
):
    path = url.split("://", 1)[1].split("/", 1)[1].removesuffix(".git")
    entry = {
        "type": "repository",
        "name": "Repo",
        "connection_url": url,
        "credentials": {
            "token": TOKEN,
            "lease": {"id": "l1", "connector_id": connector, "token": LEASE},
        },
        "project_read_only": False,
        "datasource_id": connector,
        "config": {"forge": "github"},
        "git_swap": {
            "url": f"{origin}/{connector}/{path}",
            "ca": CA,
            "wait_seconds": 60,
        },
    }
    entry.update(over)
    return entry


class TestBinding:
    def test_a_swap_entry_is_a_binding(self):
        binding, reason = swap_binding(_entry())
        assert reason == ""
        assert binding == SwapBinding(
            connector_id=CONNECTOR,
            clean_url="https://github.com/o/r.git",
            base="https://github.com/o/r",
            driver_url=f"{ORIGIN}/{CONNECTOR}/o/r",
            origin=ORIGIN,
            ca=CA,
            wait_seconds=60.0,
        )
        assert checkout_auth(_entry()) == "swap"

    @pytest.mark.parametrize(
        ("over", "reason"),
        [
            ({"git_swap": {"unavailable": "off here"}}, "off here"),
            ({"credentials": {"token": TOKEN}}, "lease was not delivered"),
            (
                {"git_swap": {"url": f"{ORIGIN}/{OTHER}/o/r", "ca": CA}},
                "not this connector's repository",
            ),
            (
                {"git_swap": {"url": f"{ORIGIN}/{CONNECTOR}/o/other", "ca": CA}},
                "not this connector's repository",
            ),
            (
                {"git_swap": {"url": f"http://x:80/{CONNECTOR}/o/r", "ca": CA}},
                "not this connector's repository",
            ),
            (
                {"git_swap": {"url": f"https://u@x:1/{CONNECTOR}/o/r", "ca": CA}},
                "not this connector's repository",
            ),
            ({"git_swap": {"url": f"{ORIGIN}/{CONNECTOR}/o/r", "ca": ""}}, "authority"),
            ({"connection_url": "http://github.com/o/r"}, "malformed"),
        ],
    )
    def test_anything_else_says_why(self, over, reason):
        binding, why = swap_binding(_entry(**over))
        assert binding is None and reason in why

    def test_a_swap_entry_routes_to_the_lease_and_the_checkout(self):
        [delivery] = deliveries_from_payload([_entry()])
        assert delivery.spec is GIT_SWAP_SPEC
        assert delivery.binding.driver == GIT_SWAP_SPEC.name
        forms = [entry.form for entry in delivery.binding.entries]
        assert forms == ["checkout", "lease_token"]
        checkout = delivery.values("checkout")[0]
        assert checkout["auth"] == "swap"
        assert checkout["url"] == "https://github.com/o/r.git"
        assert delivery.values("lease_token")[0]["token"] == LEASE
        # A refused one stays a repository: no lease, a checkout that says why.
        [refused] = deliveries_from_payload(
            [_entry(git_swap={"unavailable": "x"}, credentials={})]
        )
        assert refused.spec is REPOSITORY_SPEC
        assert refused.values("checkout")[0]["auth"] == "swap"
        assert "NOT cloned: x" in swap_note(refused.entry)

    def test_the_include_points_the_upstream_at_the_driver(self):
        binding, _ = swap_binding(_entry())
        text = render_include(binding, home="/home/agent-host")
        assert (
            f'[url "{ORIGIN}/{CONNECTOR}/o/r"]\n\tinsteadOf = https://github.com/o/r\n'
            in text
        )
        assert f'[credential "{ORIGIN}"]\n\thelper =\n' in text
        assert (
            'helper = "!/usr/bin/python3 -I '
            f"'/home/agent-host/.srw-credentials/git/credential-helper' {CONNECTOR}\""
        ) in text
        assert "useHttpPath = true" in text
        assert (
            f'[http "{ORIGIN}/"]\n\tsslCAInfo = /home/agent-host/.srw-credentials/'
            f"git/bindings/{CONNECTOR}.ca.pem\n"
        ) in text
        assert TOKEN not in text and LEASE not in text
        with pytest.raises(ValueError):
            render_include(binding, home="/home/agent host")


# =============================================================================
# The wiring, with real git
# =============================================================================


@pytest.fixture
def home(tmp_path):
    if shutil.which("git") is None or not Path("/usr/bin/python3").exists():
        pytest.skip("git and /usr/bin/python3 are needed")
    home = tmp_path / "home"
    home.mkdir()
    return home


def _env(home: Path) -> dict[str, str]:
    return {
        "HOME": str(home),
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_ASKPASS": "",
    }


def _sync(home: Path, bindings, *, remove=(), prune=False) -> dict:
    payload = {
        "helper": GIT_SWAP_CREDENTIAL_HELPER,
        "bindings": [
            {
                "id": binding.connector_id,
                "include": render_include(binding, home=str(home)),
                "ca": binding.ca,
            }
            for binding in bindings
        ],
        "remove": list(remove),
        "prune": prune,
    }
    done = subprocess.run(
        [sys.executable, "-I", "-c", GIT_SWAP_WIRING, str(home), "sync"],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        env=_env(home),
        check=False,
    )
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout.strip().splitlines()[-1])


def _git(home: Path, *args: str, cwd: Path | None = None, stdin: str = "") -> str:
    done = subprocess.run(
        ["git", *args],
        input=stdin,
        capture_output=True,
        text=True,
        env=_env(home),
        cwd=cwd or home,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    return done.stdout


def _lease(home: Path, connector: str, token: str) -> None:
    leases = home / ".srw-credentials" / "leases"
    leases.mkdir(parents=True, exist_ok=True)
    (leases / connector).write_text(token)


def _fill(home: Path, origin: str, path: str) -> dict[str, str]:
    host = origin.removeprefix("https://")
    out = _git(
        home,
        "credential",
        "fill",
        stdin=f"protocol=https\nhost={host}\npath={path}\n\n",
    )
    return dict(line.split("=", 1) for line in out.splitlines() if "=" in line)


class TestWiring:
    def test_every_git_in_the_workspace_goes_through_the_driver(self, home):
        binding, _ = swap_binding(_entry())
        report = _sync(home, [binding])
        assert report == {"bindings": [CONNECTOR], "removed": [], "include": "added"}
        _lease(home, CONNECTOR, LEASE)
        # The remote stays clean; git uses it rewritten to the driver.
        repo = home / "repos" / "r"
        _git(home, "init", "-q", str(repo))
        _git(home, "remote", "add", "origin", "https://github.com/o/r.git", cwd=repo)
        assert _git(home, "config", "--get", "remote.origin.url", cwd=repo).strip() == (
            "https://github.com/o/r.git"
        )
        assert _git(home, "remote", "get-url", "origin", cwd=repo).strip() == (
            f"{ORIGIN}/{CONNECTOR}/o/r.git"
        )
        # The helper answers with the lease, for this connector's path only.
        answer = _fill(home, ORIGIN, f"{CONNECTOR}/o/r.git")
        assert answer["password"] == LEASE
        assert answer["username"] == "srw-lease"
        assert int(answer["password_expiry_utc"]) > 0
        # The authority is trusted for the driver's URL only.
        assert (
            _git(
                home,
                "config",
                "--get-urlmatch",
                "http.sslCAInfo",
                f"{ORIGIN}/{CONNECTOR}/o/r",
            )
            .strip()
            .endswith(f"bindings/{CONNECTOR}.ca.pem")
        )
        other = subprocess.run(
            [
                "git",
                "config",
                "--get-urlmatch",
                "http.sslCAInfo",
                "https://github.com/o/r",
            ],
            capture_output=True,
            text=True,
            env=_env(home),
            check=False,
        )
        assert other.stdout == ""
        # Files: 0600 under a 0700 directory, outside every repository.
        wiring = home / ".srw-credentials" / "git"
        assert oct(wiring.stat().st_mode & 0o777) == "0o700"
        for path in wiring.rglob("*"):
            if path.is_file():
                assert path.stat().st_mode & 0o077 == 0, path

    def test_two_connectors_each_get_their_own_lease(self, home):
        first, _ = swap_binding(_entry())
        second, _ = swap_binding(
            _entry(
                connector=OTHER, origin=OTHER_ORIGIN, url="https://github.com/o/s.git"
            )
        )
        _sync(home, [first, second])
        _lease(home, CONNECTOR, LEASE)
        _lease(home, OTHER, OTHER_LEASE)
        assert _fill(home, ORIGIN, f"{CONNECTOR}/o/r.git")["password"] == LEASE
        assert _fill(home, OTHER_ORIGIN, f"{OTHER}/o/s.git")["password"] == OTHER_LEASE
        # A path that is not the helper's connector gets nothing from it.
        done = subprocess.run(
            ["git", "credential", "fill"],
            input=f"protocol=https\nhost={ORIGIN[8:]}\npath={OTHER}/o/s.git\n\n",
            capture_output=True,
            text=True,
            env=_env(home),
            check=False,
        )
        assert done.returncode != 0 and LEASE not in done.stdout

    def test_without_a_lease_git_gets_nothing(self, home):
        binding, _ = swap_binding(_entry())
        _sync(home, [binding])
        _lease(home, CONNECTOR, "not-a-lease")
        done = subprocess.run(
            ["git", "credential", "fill"],
            input=f"protocol=https\nhost={ORIGIN[8:]}\npath={CONNECTOR}/o/r.git\n\n",
            capture_output=True,
            text=True,
            env=_env(home),
            check=False,
        )
        assert done.returncode != 0

    def test_the_include_line_is_added_once_and_bindings_are_pruned(self, home):
        first, _ = swap_binding(_entry())
        second, _ = swap_binding(
            _entry(
                connector=OTHER, origin=OTHER_ORIGIN, url="https://github.com/o/s.git"
            )
        )
        (home / ".gitconfig").write_text("[user]\n\tname = Agent Worker\n")
        _sync(home, [first, second])
        report = _sync(home, [first])
        assert report["include"] == "present" and report["bindings"] == [
            CONNECTOR,
            OTHER,
        ]
        includes = _git(home, "config", "--global", "--get-all", "include.path").split()
        assert includes == ["~/.srw-credentials/git/config"]
        assert "Agent Worker" in (home / ".gitconfig").read_text()
        # The owner's attach prunes what it no longer binds.
        report = _sync(home, [first], prune=True)
        assert report["bindings"] == [CONNECTOR] and report["removed"] == [OTHER]
        assert not (
            home / ".srw-credentials/git/bindings" / f"{OTHER}.gitconfig"
        ).exists()
        # A live detach removes exactly one.
        report = _sync(home, [], remove=[CONNECTOR])
        assert report["bindings"] == [] and report["removed"] == [CONNECTOR]
        assert _git(home, "config", "--global", "--list")  # still parses

    def test_retire_removes_the_wiring_and_git_still_works(self, home):
        binding, _ = swap_binding(_entry())
        _sync(home, [binding])
        done = subprocess.run(
            [sys.executable, "-I", "-c", GIT_SWAP_WIRING, str(home), "retire"],
            capture_output=True,
            text=True,
            check=False,
        )
        assert done.returncode == 0
        assert not (home / ".srw-credentials" / "git").exists()
        # The include that is gone is ignored.
        assert "include.path" in _git(home, "config", "--global", "--list")

    def test_a_symlinked_wiring_directory_is_refused(self, home, tmp_path):
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        (home / ".srw-credentials").mkdir(mode=0o700)
        (home / ".srw-credentials" / "git").symlink_to(elsewhere)
        binding, _ = swap_binding(_entry())
        done = subprocess.run(
            [sys.executable, "-I", "-c", GIT_SWAP_WIRING, str(home), "sync"],
            input=json.dumps(
                {
                    "helper": "x",
                    "bindings": [
                        {
                            "id": CONNECTOR,
                            "include": render_include(binding, home=str(home)),
                            "ca": CA,
                        }
                    ],
                }
            ),
            capture_output=True,
            text=True,
            check=False,
        )
        assert done.returncode == 4 and not list(elsewhere.iterdir())


# =============================================================================
# The clone
# =============================================================================


def _workspace(*, exists=False, shell_outputs=("Exit code: 0",)):
    ws = MagicMock()
    ws.path = Path("/tmp/ws")
    ws.source_repos = {}
    ws.source_repo_meta = {}
    backend = MagicMock()
    backend.supports_shell = True
    backend.exists = MagicMock(
        side_effect=lambda path: exists and path.endswith(".git")
    )
    backend.resolve_home_path = MagicMock(
        side_effect=lambda rel: f"/home/agent-host/{rel}"
    )
    backend.shell_run = MagicMock(side_effect=list(shell_outputs) * 20)
    backend.install_git_swap_wiring = MagicMock(
        return_value={"bindings": [CONNECTOR], "removed": [], "include": "added"}
    )
    ws.backend = backend
    return ws


class TestClone:
    def test_the_wiring_comes_first_then_the_clean_url_is_cloned(self):
        ws = _workspace()
        calls: list[str] = []
        ws.backend.install_git_swap_wiring.side_effect = lambda *a, **k: calls.append(
            "wiring"
        ) or {"bindings": [CONNECTOR]}
        git_mgr = MagicMock()

        def clone(url, *args, **kwargs):
            calls.append(f"clone {url}")
            return git_mgr

        with patch("agent.managers.git_manager.GitManager.clone", side_effect=clone):
            clone_repository_datasources([_entry()], ws)
        assert calls == ["wiring", "clone https://github.com/o/r.git"]
        [bindings], kwargs = ws.backend.install_git_swap_wiring.call_args
        assert [item["id"] for item in bindings] == [CONNECTOR]
        assert kwargs == {"remove": [], "prune": True}
        git_mgr._run_git.assert_any_call(["config", "transfer.credentialsInUrl", "die"])
        # The token reaches the agent process's PR metadata, never a command.
        assert ws.source_repo_meta["r"]["token"] == TOKEN
        for call in ws.backend.shell_run.call_args_list:
            assert TOKEN not in str(call) and LEASE not in str(call)

    def test_a_reused_checkout_loses_its_old_token_url(self):
        ws = _workspace(exists=True)
        reused = MagicMock()
        with patch(
            "agent.managers.git_manager.GitManager", return_value=reused
        ) as manager:
            manager.clone = MagicMock()
            clone_repository_datasources([_entry()], ws)
        manager.clone.assert_not_called()
        reused.add_remote.assert_called_once_with(
            "origin", "https://github.com/o/r.git"
        )
        reused._run_git.assert_any_call(["config", "transfer.credentialsInUrl", "die"])
        ws.backend.shell_run.assert_not_called()  # no wait for a reused checkout

    def test_a_partial_set_never_prunes(self):
        ws = _workspace()
        with patch(
            "agent.managers.git_manager.GitManager.clone", return_value=MagicMock()
        ):
            clone_repository_datasources([_entry()], ws, legacy_key_files="own")
        assert ws.backend.install_git_swap_wiring.call_args.kwargs["prune"] is False

    def test_a_refused_repository_is_skipped_with_its_reason(self, caplog):
        ws = _workspace()
        with patch("agent.managers.git_manager.GitManager.clone") as clone:
            clone_repository_datasources(
                [_entry(git_swap={"unavailable": "not HTTPS"}, credentials={})], ws
            )
        clone.assert_not_called()
        ws.backend.install_git_swap_wiring.assert_not_called()
        assert "not HTTPS" in caplog.text

    def test_a_failed_wiring_skips_the_clone_rather_than_cloning_bare(self, caplog):
        ws = _workspace()
        ws.backend.install_git_swap_wiring.side_effect = RuntimeError("ssh")
        with patch("agent.managers.git_manager.GitManager.clone") as clone:
            clone_repository_datasources([_entry()], ws)
        clone.assert_not_called()
        assert "could not be installed" in caplog.text

    def test_the_first_clone_waits_for_a_starting_driver(self):
        ws = _workspace(
            shell_outputs=(
                "Exit code: 128\nfatal: unable to access: Could not resolve host",
                "Exit code: 124",
                "Exit code: 0",
            )
        )
        sleeps: list[float] = []
        with patch("agent.connectors.git_swap.time.sleep", side_effect=sleeps.append):
            with patch(
                "agent.managers.git_manager.GitManager.clone", return_value=MagicMock()
            ) as clone:
                clone_repository_datasources([_entry()], ws)
        clone.assert_called_once()
        assert ws.backend.shell_run.call_count == 3 and len(sleeps) == 2
        command = ws.backend.shell_run.call_args_list[0].args[0]
        assert command.startswith("timeout 20 git ls-remote --quiet ")
        assert "https://github.com/o/r.git" in command


class TestWait:
    def _binding(self, wait=30):
        binding, _ = swap_binding(_entry())
        return SwapBinding(**{**binding.__dict__, "wait_seconds": wait})

    def test_a_refusal_is_final_at_once(self):
        backend = MagicMock()
        backend.shell_run.return_value = "Exit code: 128\nfatal: Authentication failed for 'https://github.com/o/r.git/'"
        reason = wait_for_driver(backend, self._binding(), sleep=lambda _s: None)
        assert "did not serve" in reason and backend.shell_run.call_count == 1

    def test_it_gives_up_at_the_deadline(self):
        backend = MagicMock()
        backend.shell_run.return_value = "Exit code: 124"
        now = [0.0]

        def sleep(seconds):
            now[0] += seconds

        reason = wait_for_driver(
            backend, self._binding(wait=12), sleep=sleep, clock=lambda: now[0]
        )
        assert reason is not None and backend.shell_run.call_count == 3


class TestLiveChanges:
    def test_a_detached_swap_repository_loses_its_wiring(self):
        ws = _workspace()
        old = deliveries_from_payload([_entry()])
        rt = RuntimeContext(execution="session", workspace_manager=ws)
        CheckoutMaterializer().replace(old, [], rt)
        [bindings], kwargs = ws.backend.install_git_swap_wiring.call_args
        assert bindings == [] and kwargs["remove"] == [CONNECTOR]

    def test_a_backend_swap_writes_the_wiring_on_the_new_workspace(self):
        backend = _workspace().backend
        CheckoutMaterializer().on_backend_swap(
            deliveries_from_payload([_entry()]), backend
        )
        [bindings], kwargs = backend.install_git_swap_wiring.call_args
        assert [item["id"] for item in bindings] == [CONNECTOR]
        assert kwargs["prune"] is False

    def test_the_readme_says_how_the_repository_is_reached(self):
        deliveries = deliveries_from_payload([_entry()])
        rt = RuntimeContext(execution="session", workspace_manager=_workspace())
        [facts] = CheckoutMaterializer().facts(deliveries, rt)
        assert "git swap driver with a lease" in facts.lines[0]
        assert "no ref deletes or tags" in facts.lines[0]


def test_install_wiring_resolves_the_home_from_the_backend():
    backend = MagicMock()
    backend.resolve_home_path.side_effect = lambda rel: f"/home/agent-host/{rel}"
    binding, _ = swap_binding(_entry())
    install_wiring(backend, [binding])
    [bindings], _kwargs = backend.install_git_swap_wiring.call_args
    assert (
        "/home/agent-host/.srw-credentials/git/credential-helper"
        in bindings[0]["include"]
    )


class TestRemoteBackend:
    def _backend(self):
        from shared.runtime.core.backends.remote import RemoteBackend

        backend = object.__new__(RemoteBackend)
        backend._init_shell = MagicMock()
        backend._get_home_dir = lambda: "/home/agent-host"
        backend.execute_claim_resource_with_secret_stdin_output = MagicMock(
            return_value=(0, json.dumps({"bindings": [CONNECTOR], "removed": []}))
        )
        return backend

    def test_the_wiring_travels_on_stdin_only(self):
        from shared.runtime.core.credential_env import WORKSPACE_PYTHON

        backend = self._backend()
        binding, _ = swap_binding(_entry())
        include = render_include(binding, home="/home/agent-host")
        report = backend.install_git_swap_wiring(
            [{"id": CONNECTOR, "include": include, "ca": CA}], prune=True
        )
        assert report["bindings"] == [CONNECTOR]
        command, stdin = (
            backend.execute_claim_resource_with_secret_stdin_output.call_args[0]
        )
        assert command.startswith(WORKSPACE_PYTHON + " -c ")
        assert command.endswith("/home/agent-host sync")
        assert include not in command and CA not in command
        sent = json.loads(stdin)
        assert sent["bindings"] == [{"id": CONNECTOR, "include": include, "ca": CA}]
        assert sent["prune"] is True and sent["remove"] == []
        assert sent["helper"] == GIT_SWAP_CREDENTIAL_HELPER

    def test_a_failed_install_raises(self):
        from shared.runtime.core.workspace_backend import WorkspaceUnavailableError

        backend = self._backend()
        backend.execute_claim_resource_with_secret_stdin_output.return_value = (5, "")
        with pytest.raises(WorkspaceUnavailableError):
            backend.install_git_swap_wiring([])
        backend.execute_claim_resource_with_secret_stdin_output.return_value = (0, "")
        with pytest.raises(WorkspaceUnavailableError):
            backend.install_git_swap_wiring([])

    def test_a_backend_without_a_shell_refuses(self):
        from shared.runtime.core.workspace_backend import WorkspaceBackend

        with pytest.raises(ValueError, match="sandbox or VM"):
            WorkspaceBackend.install_git_swap_wiring(MagicMock(), [])
