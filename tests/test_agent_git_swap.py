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
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from agent.connectors.checkout import CheckoutMaterializer, clone_repository_datasources
from agent.connectors.git_swap import (
    SwapBinding,
    binding_options,
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
            f'[url "{ORIGIN}/{CONNECTOR}/o/r.git"]\n'
            "\tinsteadOf = https://github.com/o/r.git\n" in text
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


def _sync(home: Path, bindings, *, remove=(), prune=False, checkouts=None) -> dict:
    checkouts = checkouts or {}
    payload = {
        "helper": GIT_SWAP_CREDENTIAL_HELPER,
        "bindings": [
            {
                "id": binding.connector_id,
                "include": render_include(binding, home=str(home)),
                "ca": binding.ca,
                **(
                    {"gitdir": f"{checkouts[binding.connector_id]}/"}
                    if binding.connector_id in checkouts
                    else {}
                ),
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


def _git(
    home: Path,
    *args: str,
    cwd: Path | None = None,
    stdin: str = "",
    check: bool = True,
) -> str:
    done = subprocess.run(
        ["git", *args],
        input=stdin,
        capture_output=True,
        text=True,
        env=_env(home),
        cwd=cwd or home,
        check=False,
    )
    assert done.returncode == 0 or not check, done.stderr
    return done.stdout


def _checkout(home: Path, repo: Path, url: str, remote: str = "origin") -> None:
    if not (repo / ".git").exists():
        _git(home, "init", "-q", str(repo))
    _git(home, "remote", "remove", remote, cwd=repo, check=False)
    _git(home, "remote", "add", remote, url, cwd=repo)


def _url(home: Path, repo: Path, remote: str = "origin") -> str:
    return _git(home, "remote", "get-url", remote, cwd=repo).strip()


def _lease(home: Path, connector: str, token: str) -> None:
    leases = home / ".srw-credentials" / "leases"
    leases.mkdir(parents=True, exist_ok=True)
    (leases / connector).write_text(token)


def _fill(
    home: Path, origin: str, path: str, *, cwd: Path | None = None
) -> dict[str, str]:
    host = origin.removeprefix("https://")
    out = _git(
        home,
        "credential",
        "fill",
        stdin=f"protocol=https\nhost={host}\npath={path}\n\n",
        cwd=cwd,
    )
    return dict(line.split("=", 1) for line in out.splitlines() if "=" in line)


class TestWiring:
    def test_every_git_in_the_checkout_goes_through_the_driver(self, home):
        binding, _ = swap_binding(_entry())
        repo = home / "repos" / "r"
        report = _sync(home, [binding], checkouts={CONNECTOR: repo})
        assert report == {
            "bindings": [CONNECTOR],
            "removed": [],
            "unscoped": [],
            "include": "added",
        }
        _lease(home, CONNECTOR, LEASE)
        # The remote stays clean; git uses it rewritten to the driver.
        _git(home, "init", "-q", str(repo))
        _git(home, "remote", "add", "origin", "https://github.com/o/r.git", cwd=repo)
        assert _git(home, "config", "--get", "remote.origin.url", cwd=repo).strip() == (
            "https://github.com/o/r.git"
        )
        assert _git(home, "remote", "get-url", "origin", cwd=repo).strip() == (
            f"{ORIGIN}/{CONNECTOR}/o/r.git"
        )
        # The helper answers with the lease, for this connector's path only.
        answer = _fill(home, ORIGIN, f"{CONNECTOR}/o/r.git", cwd=repo)
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
                cwd=repo,
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
            cwd=repo,
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
        r, s = home / "repos" / "r", home / "repos" / "s"
        _sync(home, [first, second], checkouts={CONNECTOR: r, OTHER: s})
        _checkout(home, r, "https://github.com/o/r.git")
        _checkout(home, s, "https://github.com/o/s.git")
        _lease(home, CONNECTOR, LEASE)
        _lease(home, OTHER, OTHER_LEASE)
        assert _fill(home, ORIGIN, f"{CONNECTOR}/o/r.git", cwd=r)["password"] == LEASE
        assert (
            _fill(home, OTHER_ORIGIN, f"{OTHER}/o/s.git", cwd=s)["password"]
            == OTHER_LEASE
        )
        # A path that is not the helper's connector gets nothing from it.
        done = subprocess.run(
            ["git", "credential", "fill"],
            input=f"protocol=https\nhost={ORIGIN[8:]}\npath={OTHER}/o/s.git\n\n",
            capture_output=True,
            text=True,
            env=_env(home),
            cwd=r,
            check=False,
        )
        assert done.returncode != 0 and LEASE not in done.stdout

    def test_two_connectors_to_one_upstream_never_mix(self, home):
        # A ReadWrite and a ReadOnly binding of the same repository: git
        # would use the first-defined of two equally long insteadOf matches,
        # so each checkout must see only its own binding, whatever the ids.
        rw, _ = swap_binding(_entry())
        ro, _ = swap_binding(_entry(connector=OTHER, origin=OTHER_ORIGIN))
        rw_dir, ro_dir = home / "repos" / "r", home / "repos" / "r-2"
        for order in ([rw, ro], [ro, rw]):
            _sync(home, order, checkouts={CONNECTOR: rw_dir, OTHER: ro_dir})
            _checkout(home, rw_dir, "https://github.com/o/r.git")
            _checkout(home, ro_dir, "https://github.com/o/r.git")
            assert _url(home, rw_dir) == f"{ORIGIN}/{CONNECTOR}/o/r.git"
            assert _url(home, ro_dir) == f"{OTHER_ORIGIN}/{OTHER}/o/r.git"
        _lease(home, CONNECTOR, LEASE)
        _lease(home, OTHER, OTHER_LEASE)
        assert _fill(home, ORIGIN, f"{CONNECTOR}/o/r.git", cwd=rw_dir)["password"] == (
            LEASE
        )
        assert _fill(home, OTHER_ORIGIN, f"{OTHER}/o/r.git", cwd=ro_dir)[
            "password"
        ] == (OTHER_LEASE)

    def test_a_longer_repository_name_is_not_rewritten(self, home):
        # Git rewrites by prefix: o/r would rewrite o/r-docs. The rule names
        # the exact remote, o/r.git, which no other repository starts with.
        binding, _ = swap_binding(_entry(url="https://github.com/o/r"))
        assert binding.clean_url == "https://github.com/o/r.git"
        repo, docs = home / "repos" / "r", home / "repos" / "r-docs"
        _sync(home, [binding], checkouts={CONNECTOR: repo})
        _checkout(home, repo, "https://github.com/o/r.git")
        _git(home, "remote", "add", "docs", "https://github.com/o/r-docs.git", cwd=repo)
        assert _url(home, repo, "docs") == "https://github.com/o/r-docs.git"
        _checkout(home, docs, "https://github.com/o/r-docs.git")
        assert _url(home, docs) == "https://github.com/o/r-docs.git"
        # Outside SRW's checkouts nothing is rewritten, not even the same URL.
        outside = home / "elsewhere"
        _checkout(home, outside, "https://github.com/o/r.git")
        assert _url(home, outside) == "https://github.com/o/r.git"

    def test_the_clone_names_the_rules_before_its_checkout_exists(self, home):
        binding, _ = swap_binding(_entry())
        _sync(home, [binding], checkouts={CONNECTOR: home / "repos" / "r"})
        backend = MagicMock()
        backend.resolve_home_path.side_effect = lambda rel: f"{home}/{rel}"
        options = binding_options(backend, binding)
        command = ["ls-remote", "--get-url", "https://github.com/o/r.git"]
        assert _git(home, *command).strip() == "https://github.com/o/r.git"
        assert _git(
            home, *(arg for option in options for arg in ("-c", option)), *command
        ).strip() == (f"{ORIGIN}/{CONNECTOR}/o/r.git")

    def test_without_a_lease_git_gets_nothing(self, home):
        binding, _ = swap_binding(_entry())
        repo = home / "repos" / "r"
        _sync(home, [binding], checkouts={CONNECTOR: repo})
        _checkout(home, repo, "https://github.com/o/r.git")
        _lease(home, CONNECTOR, "not-a-lease")
        done = subprocess.run(
            ["git", "credential", "fill"],
            input=f"protocol=https\nhost={ORIGIN[8:]}\npath={CONNECTOR}/o/r.git\n\n",
            capture_output=True,
            text=True,
            env=_env(home),
            cwd=repo,
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
        checkouts = {CONNECTOR: home / "repos" / "r", OTHER: home / "repos" / "s"}
        _sync(home, [first, second], checkouts=checkouts)
        # A later sync without checkouts keeps the ones recorded.
        report = _sync(home, [first])
        assert report["include"] == "present" and report["bindings"] == [
            CONNECTOR,
            OTHER,
        ]
        assert report["unscoped"] == []
        config = (home / ".srw-credentials/git/config").read_text()
        assert f'[includeIf "gitdir:{home}/repos/r/"]' in config
        assert f'[includeIf "gitdir:{home}/repos/s/"]' in config
        includes = _git(home, "config", "--global", "--get-all", "include.path").split()
        assert includes == ["~/.srw-credentials/git/config"]
        assert "Agent Worker" in (home / ".gitconfig").read_text()
        # The owner's attach prunes what it no longer binds.
        report = _sync(home, [first], prune=True)
        assert report["bindings"] == [CONNECTOR] and report["removed"] == [OTHER]
        assert not list((home / ".srw-credentials/git/bindings").glob(f"{OTHER}.*"))
        # A live detach removes exactly one.
        report = _sync(home, [], remove=[CONNECTOR])
        assert report["bindings"] == [] and report["removed"] == [CONNECTOR]
        assert _git(home, "config", "--global", "--list")  # still parses

    def test_only_sync_runs_and_a_checkout_path_is_checked(self, home):
        binding, _ = swap_binding(_entry())
        for argv, gitdir in (
            (["retire"], None),
            (["sync"], "/tmp/repos/*/"),
            (["sync"], "/tmp/../etc/"),
            (["sync"], "relative/r/"),
            (["sync"], "/tmp/r"),
        ):
            item = {
                "id": CONNECTOR,
                "include": render_include(binding, home=str(home)),
                "ca": CA,
            }
            if gitdir is not None:
                item["gitdir"] = gitdir
            done = subprocess.run(
                [sys.executable, "-I", "-c", GIT_SWAP_WIRING, str(home), *argv],
                input=json.dumps({"helper": "x", "bindings": [item]}),
                capture_output=True,
                text=True,
                check=False,
            )
            assert done.returncode == 3, (argv, gitdir)
        assert not (home / ".srw-credentials").exists()

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
    backend.resolve_path = MagicMock(
        side_effect=lambda rel: f"/home/agent-host/workspace/{rel}"
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

        with patch(
            "agent.managers.git_manager.GitManager.clone", side_effect=clone
        ) as cloned:
            clone_repository_datasources([_entry()], ws)
        assert calls == ["wiring", "clone https://github.com/o/r.git"]
        # The clone names the binding's rules: its checkout does not exist yet.
        assert cloned.call_args.kwargs["config"] == [
            "include.path=/home/agent-host/.srw-credentials/git/bindings/"
            f"{CONNECTOR}.gitconfig"
        ]
        [bindings], kwargs = ws.backend.install_git_swap_wiring.call_args
        assert [item["id"] for item in bindings] == [CONNECTOR]
        assert [item["gitdir"] for item in bindings] == [
            "/home/agent-host/workspace/repos/r/"
        ]
        assert kwargs == {"remove": [], "prune": True}
        git_mgr._run_git.assert_any_call(["config", "transfer.credentialsInUrl", "die"])
        # The token reaches the agent process's PR metadata, never a command.
        assert ws.source_repo_meta["r"]["token"] == TOKEN
        for call in ws.backend.shell_run.call_args_list:
            assert TOKEN not in str(call) and LEASE not in str(call)

    def test_a_reused_checkout_loses_its_old_token_url(self):
        ws = _workspace(exists=True)
        reused = MagicMock()
        reused._run_git.return_value = SimpleNamespace(
            returncode=0, stdout="https://github.com/o/r.git\n"
        )
        with patch(
            "agent.managers.git_manager.GitManager", return_value=reused
        ) as manager:
            manager.clone = MagicMock()
            clone_repository_datasources([_entry(url="https://github.com/o/r")], ws)
        manager.clone.assert_not_called()
        reused.add_remote.assert_called_once_with(
            "origin", "https://github.com/o/r.git"
        )
        reused._run_git.assert_any_call(["config", "transfer.credentialsInUrl", "die"])
        ws.backend.shell_run.assert_not_called()  # no wait for a reused checkout
        assert ws.source_repos == {"r": reused}

    @pytest.mark.parametrize(
        ("reset", "found"),
        [
            (False, SimpleNamespace(returncode=0, stdout="https://github.com/o/r.git")),
            (
                True,
                SimpleNamespace(
                    returncode=0, stdout=f"https://oauth2:{TOKEN}@github.com/o/r.git"
                ),
            ),
            (True, SimpleNamespace(returncode=1, stdout="")),
        ],
    )
    def test_a_reused_checkout_that_keeps_its_token_is_not_used(
        self, reset, found, caplog
    ):
        ws = _workspace(exists=True)
        reused = MagicMock()
        reused.add_remote.return_value = reset
        reused._run_git.return_value = found
        with patch("agent.managers.git_manager.GitManager", return_value=reused):
            clone_repository_datasources([_entry()], ws)
        assert ws.source_repos == {}
        assert "origin could not be reset" in caplog.text
        assert TOKEN not in caplog.text
        rt = RuntimeContext(execution="session", workspace_manager=ws)
        [facts] = CheckoutMaterializer().facts(deliveries_from_payload([_entry()]), rt)
        assert "NOT cloned" in facts.lines[0]
        assert "origin could not be reset" in facts.lines[0]

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
        # Nothing is wired; the owner's sweep still prunes an earlier
        # attach's wiring (no binding is left).
        ws.backend.install_git_swap_wiring.assert_called_once_with(
            [], remove=[], prune=True
        )
        assert "not HTTPS" in caplog.text
        # A partial set (a live add) prunes nothing.
        ws = _workspace()
        with patch("agent.managers.git_manager.GitManager.clone"):
            clone_repository_datasources(
                [_entry(git_swap={"unavailable": "x"}, credentials={})],
                ws,
                legacy_key_files="own",
            )
        ws.backend.install_git_swap_wiring.assert_not_called()

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
        assert command.startswith(
            "timeout 20 git -c include.path=/home/agent-host/.srw-credentials/git/"
            f"bindings/{CONNECTOR}.gitconfig ls-remote --quiet "
        )
        assert "https://github.com/o/r.git" in command


def _home_backend():
    backend = MagicMock()
    backend.resolve_home_path.side_effect = lambda rel: f"/home/agent-host/{rel}"
    return backend


class TestWait:
    def _binding(self, wait=30):
        binding, _ = swap_binding(_entry())
        return SwapBinding(**{**binding.__dict__, "wait_seconds": wait})

    def test_a_refusal_is_final_at_once(self):
        backend = _home_backend()
        backend.shell_run.return_value = "Exit code: 128\nfatal: Authentication failed for 'https://github.com/o/r.git/'"
        reason = wait_for_driver(backend, self._binding(), sleep=lambda _s: None)
        assert "did not serve" in reason and backend.shell_run.call_count == 1

    @pytest.mark.parametrize("status", ["404", "403", "502"])
    def test_any_answer_of_the_driver_is_final(self, status):
        # A redirecting upstream is a 502 the driver gives at once: waiting
        # minutes for it would only delay the attach.
        backend = _home_backend()
        backend.shell_run.return_value = (
            "Exit code: 128\nfatal: unable to access 'https://github.com/o/r.git/': "
            f"The requested URL returned error: {status}"
        )
        reason = wait_for_driver(backend, self._binding(), sleep=lambda _s: None)
        assert status in reason and backend.shell_run.call_count == 1

    def test_a_503_is_retried(self):
        backend = _home_backend()
        backend.shell_run.side_effect = [
            "Exit code: 128\nfatal: The requested URL returned error: 503",
            "Exit code: 0",
        ]
        assert wait_for_driver(backend, self._binding(), sleep=lambda _s: None) is None
        assert backend.shell_run.call_count == 2

    def test_it_gives_up_at_the_deadline(self):
        backend = _home_backend()
        backend.shell_run.return_value = "Exit code: 124"
        now = [0.0]

        def sleep(seconds):
            now[0] += seconds

        reason = wait_for_driver(
            backend, self._binding(wait=12), sleep=sleep, clock=lambda: now[0]
        )
        assert reason is not None and backend.shell_run.call_count == 3


class TestFallbackAfterSwap:
    """C3 re-review S3: a re-attach that falls back after the driver served
    the checkout (its pod dead, or alive but out of this workspace's reach:
    the agent sees the same entry; the orchestrator revoked the old lease)."""

    def _fallback(self):
        return _entry(
            git_swap={"fallback": "the driver could not reach the upstream"},
            credentials={"token": TOKEN},
        )

    def test_a_reused_checkout_takes_the_token_url_and_the_wiring_goes(self):
        ws = _workspace(exists=True)
        reused = MagicMock()
        reused.add_remote.return_value = True
        reused._run_git.return_value = SimpleNamespace(
            returncode=0, stdout="https://github.com/o/r.git\n"
        )
        with patch("agent.managers.git_manager.GitManager", return_value=reused):
            clone_repository_datasources(
                [self._fallback()], ws, legacy_key_files="sweep"
            )
        # The swap era's binding is pruned: nothing rewrites the checkout.
        ws.backend.install_git_swap_wiring.assert_called_once_with(
            [], remove=[], prune=True
        )
        # Its remote takes the token URL, which it refused before.
        reused._run_git.assert_any_call(
            ["config", "--unset-all", "transfer.credentialsInUrl"]
        )
        reused.add_remote.assert_called_once_with(
            "origin", f"https://oauth2:{TOKEN}@github.com/o/r.git"
        )
        assert ws.source_repos == {"r": reused}
        rt = RuntimeContext(execution="session", workspace_manager=ws)
        [facts] = CheckoutMaterializer().facts(
            deliveries_from_payload([self._fallback()]), rt
        )
        assert "cloned with the forge token in its remote URL" in facts.lines[0]
        for call in ws.backend.shell_run.call_args_list:
            assert TOKEN not in str(call)

    def test_a_new_checkout_clones_with_the_token_url_unwired(self):
        ws = _workspace()
        with patch(
            "agent.managers.git_manager.GitManager.clone", return_value=MagicMock()
        ) as clone:
            clone_repository_datasources(
                [self._fallback()], ws, legacy_key_files="sweep"
            )
        assert clone.call_args.args[0] == f"https://oauth2:{TOKEN}@github.com/o/r.git"
        assert "config" not in clone.call_args.kwargs
        ws.backend.install_git_swap_wiring.assert_called_once_with(
            [], remove=[], prune=True
        )

    def test_a_github_app_token_clones_as_x_access_token(self):
        # C5: an installation token's entry names GitHub's documented user;
        # a static token keeps oauth2.
        ws = _workspace()
        entry = _entry(
            git_swap={"fallback": "the driver is not installed"},
            credentials={
                "token": TOKEN,
                "username": "x-access-token",
                "minted": True,
            },
        )
        with patch(
            "agent.managers.git_manager.GitManager.clone", return_value=MagicMock()
        ) as clone:
            clone_repository_datasources([entry], ws, legacy_key_files="sweep")
        assert clone.call_args.args[0] == (
            f"https://x-access-token:{TOKEN}@github.com/o/r.git"
        )

    def test_someone_elses_workspace_is_left_alone(self):
        ws = _workspace(exists=True)
        reused = MagicMock()
        reused._run_git.return_value = SimpleNamespace(
            returncode=0, stdout="https://github.com/o/r.git\n"
        )
        with patch("agent.managers.git_manager.GitManager", return_value=reused):
            clone_repository_datasources(
                [self._fallback()], ws, legacy_key_files="keep"
            )
        ws.backend.install_git_swap_wiring.assert_not_called()
        reused.add_remote.assert_not_called()

    @staticmethod
    def _swap_era_checkout(credentials_in_url: str | None):
        reused = MagicMock()
        reused.add_remote.return_value = True

        def run_git(args):
            if args == ["config", "--local", "--get", "transfer.credentialsInUrl"]:
                if credentials_in_url is None:
                    return SimpleNamespace(returncode=1, stdout="")
                return SimpleNamespace(returncode=0, stdout=f"{credentials_in_url}\n")
            return SimpleNamespace(returncode=0, stdout="https://github.com/o/r.git\n")

        reused._run_git.side_effect = run_git
        return reused

    @staticmethod
    def _driver_off():
        # gitSwap.enabled=false (or no driver CA): the pre-C3 token entry,
        # no git_swap block at all.
        entry = _entry(credentials={"token": TOKEN})
        entry.pop("git_swap")
        return entry

    def test_the_driver_turned_off_after_a_swap_era(self):
        """C3 re-review 2's probe: the entry says nothing about the driver,
        yet the reused checkout was the driver's. The owner's sweep prunes
        the old wiring, and the checkout takes the token URL, so git never
        goes to a dead driver URL and the README's "cloned" is true."""
        ws = _workspace(exists=True)
        reused = self._swap_era_checkout("die")
        entry = self._driver_off()
        assert checkout_auth(entry) == "token_in_url"
        with patch("agent.managers.git_manager.GitManager", return_value=reused):
            clone_repository_datasources([entry], ws, legacy_key_files="sweep")
        ws.backend.install_git_swap_wiring.assert_called_once_with(
            [], remove=[], prune=True
        )
        reused._run_git.assert_any_call(
            ["config", "--unset-all", "transfer.credentialsInUrl"]
        )
        reused.add_remote.assert_called_once_with(
            "origin", f"https://oauth2:{TOKEN}@github.com/o/r.git"
        )
        assert ws.source_repos == {"r": reused}
        rt = RuntimeContext(execution="session", workspace_manager=ws)
        [facts] = CheckoutMaterializer().facts(deliveries_from_payload([entry]), rt)
        assert "cloned at" in facts.lines[0] and "NOT cloned" not in facts.lines[0]
        for call in ws.backend.shell_run.call_args_list:
            assert TOKEN not in str(call)

    @pytest.mark.parametrize(
        ("value", "swap_era"),
        [
            ("die", True),
            ("die\n", True),
            ("warn", False),
            ("allow", False),
            (None, False),
        ],
    )
    def test_only_the_checkouts_own_die_is_a_swap_era_trace(self, value, swap_era):
        """The reconciler review's surviving mutant: only ``die``, set in the
        checkout's own config, is the driver's trace; ``warn`` (or a global
        setting, which ``--local`` never reads) is not."""
        from agent.connectors.checkout import _swap_era

        git_mgr = self._swap_era_checkout(value.strip() if value else None)
        assert _swap_era(git_mgr) is swap_era
        git_mgr._run_git.assert_called_once_with(
            ["config", "--local", "--get", "transfer.credentialsInUrl"]
        )
        # A "warn" checkout keeps its origin when reused.
        if value == "warn":
            ws = _workspace(exists=True)
            with patch("agent.managers.git_manager.GitManager", return_value=git_mgr):
                clone_repository_datasources(
                    [self._driver_off()], ws, legacy_key_files="sweep"
                )
            git_mgr.add_remote.assert_not_called()

    def test_a_pre_c3_token_checkout_is_left_as_it_was(self):
        # No swap-era trace: the checkout keeps its origin, as before C3.
        ws = _workspace(exists=True)
        reused = self._swap_era_checkout(None)
        with patch("agent.managers.git_manager.GitManager", return_value=reused):
            clone_repository_datasources(
                [self._driver_off()], ws, legacy_key_files="sweep"
            )
        reused.add_remote.assert_not_called()
        # The owner's sweep prunes all the same (nothing to prune here).
        ws.backend.install_git_swap_wiring.assert_called_once_with(
            [], remove=[], prune=True
        )
        # Someone else's workspace (a child on its parent's) is left alone.
        ws = _workspace(exists=True)
        reused = self._swap_era_checkout("die")
        with patch("agent.managers.git_manager.GitManager", return_value=reused):
            clone_repository_datasources(
                [self._driver_off()], ws, legacy_key_files="keep"
            )
        reused.add_remote.assert_not_called()
        ws.backend.install_git_swap_wiring.assert_not_called()

    def test_the_wiring_program_prunes_to_nothing_and_leaves_a_clean_home_alone(
        self, home
    ):
        binding, _ = swap_binding(_entry())
        repo = home / "repos" / "r"
        _sync(home, [binding], checkouts={CONNECTOR: repo})
        _checkout(home, repo, "https://github.com/o/r.git")
        assert _url(home, repo) == f"{ORIGIN}/{CONNECTOR}/o/r.git"
        report = _sync(home, [], prune=True)
        assert report["bindings"] == [] and report["removed"] == [CONNECTOR]
        assert _url(home, repo) == "https://github.com/o/r.git"
        clean = home / "clean"
        clean.mkdir()
        report = _sync(clean, [], prune=True)
        assert report["include"] == "absent"
        assert not (clean / ".srw-credentials").exists()
        assert not (clean / ".gitconfig").exists()


class TestCheckoutNames:
    """C3 re-review S4: a live add never takes another connector's
    checkout."""

    def _workspace_with(self, *checkouts: str):
        ws = _workspace()
        ws.backend.exists = MagicMock(
            side_effect=lambda path: any(path == f"repos/{c}/.git" for c in checkouts)
        )
        return ws

    def test_a_live_add_is_named_over_the_full_list(self):
        ws = self._workspace_with("r")  # connector A's checkout
        a = _entry()
        b = _entry(
            connector=OTHER, origin=OTHER_ORIGIN, url="https://github.com/x/r.git"
        )
        b["name"] = "Other"
        rt = RuntimeContext(execution="session", workspace_manager=ws)
        with (
            patch(
                "agent.managers.git_manager.GitManager.clone", return_value=MagicMock()
            ) as clone,
            patch("agent.managers.git_manager.GitManager") as manager,
        ):
            manager.clone = clone
            CheckoutMaterializer().replace(
                deliveries_from_payload([a]), deliveries_from_payload([a, b]), rt
            )
        # B clones into repos/r-2; A's checkout is never opened, let alone
        # re-pointed.
        assert clone.call_args.kwargs["remote_cwd"] == "repos/r-2"
        manager.assert_not_called()
        [items], _kwargs = ws.backend.install_git_swap_wiring.call_args
        assert [item["gitdir"] for item in items] == [
            "/home/agent-host/workspace/repos/r-2/"
        ]
        facts = CheckoutMaterializer().facts(deliveries_from_payload([a, b]), rt)
        assert "./repos/r-2/" in facts[1].lines[0]

    def test_a_checkout_of_another_repository_is_never_re_pointed(self):
        ws = self._workspace_with("r")
        b = _entry(
            connector=OTHER, origin=OTHER_ORIGIN, url="https://github.com/x/r.git"
        )
        reused = MagicMock()
        reused._run_git.return_value = SimpleNamespace(
            returncode=0, stdout=f"https://oauth2:{TOKEN}@github.com/o/r.git\n"
        )
        with patch("agent.managers.git_manager.GitManager", return_value=reused):
            clone_repository_datasources([b], ws, legacy_key_files="own")
        reused.add_remote.assert_not_called()
        assert ws.source_repos == {}
        rt = RuntimeContext(execution="session", workspace_manager=ws)
        [facts] = CheckoutMaterializer().facts(deliveries_from_payload([b]), rt)
        assert "NOT cloned" in facts.lines[0]
        assert "checkout of another repository (github.com/o/r)" in facts.lines[0]
        assert TOKEN not in facts.lines[0]

    @pytest.mark.parametrize(
        ("origin", "expected", "other"),
        [
            ("https://github.com/o/r.git", "https://github.com/o/r", False),
            (
                "https://oauth2:t@GitHub.com/O/R.git",
                "https://github.com/o/r.git",
                False,
            ),
            ("ssh://srw-repo-abc/o/r.git", "git@github.com:o/r.git", False),
            ("https://github.com/o/r.git", "https://github.com/x/r.git", True),
            ("https://gitlab.com/o/r.git", "https://github.com/o/r.git", True),
        ],
    )
    def test_what_counts_as_the_same_repository(self, origin, expected, other):
        from agent.connectors.checkout import _other_repository

        git_mgr = MagicMock()
        git_mgr._run_git.return_value = SimpleNamespace(returncode=0, stdout=origin)
        assert (_other_repository(git_mgr, expected) is not None) is other
        # An origin that cannot be read decides nothing.
        git_mgr._run_git.return_value = SimpleNamespace(returncode=1, stdout="")
        assert _other_repository(git_mgr, expected) is None


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
        assert bindings[0]["gitdir"] == "/home/agent-host/workspace/repos/r/"
        assert kwargs["prune"] is False

    def test_the_readme_says_how_the_repository_is_reached(self):
        deliveries = deliveries_from_payload([_entry()])
        rt = RuntimeContext(execution="session", workspace_manager=_workspace())
        [facts] = CheckoutMaterializer().facts(deliveries, rt)
        assert "git swap driver with a lease" in facts.lines[0]
        assert "no ref deletes or tags" in facts.lines[0]

    def test_a_repository_that_was_not_cloned_says_why(self, caplog):
        ws = _workspace(
            shell_outputs=(
                "Exit code: 128\nfatal: The requested URL returned error: 502",
            )
        )
        with patch("agent.managers.git_manager.GitManager.clone") as clone:
            clone_repository_datasources([_entry()], ws)
        clone.assert_not_called()
        rt = RuntimeContext(execution="session", workspace_manager=ws)
        [facts] = CheckoutMaterializer().facts(deliveries_from_payload([_entry()]), rt)
        assert "repository NOT cloned" in facts.lines[0]
        assert "did not serve the repository" in facts.lines[0]
        assert "cloned at" not in facts.lines[0]
        # A later clone that works clears it.
        with patch(
            "agent.managers.git_manager.GitManager.clone", return_value=MagicMock()
        ):
            ws.backend.shell_run.side_effect = ["Exit code: 0"] * 5
            clone_repository_datasources([_entry()], ws)
        [facts] = CheckoutMaterializer().facts(deliveries_from_payload([_entry()]), rt)
        assert "cloned at" in facts.lines[0]


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


def test_a_fallback_repository_that_did_not_clone_still_says_how_it_was_reached():
    entry = {
        **_entry(),
        "credentials": {"token": TOKEN},
        "git_swap": {"fallback": "its driver may not reach the upstream"},
    }
    assert checkout_auth(entry) == "token_in_url"
    [delivery] = deliveries_from_payload([entry])
    assert delivery.spec is REPOSITORY_SPEC
    ws = _workspace()
    ws.source_repo_skipped = {"r": "the clone failed"}
    rt = RuntimeContext(execution="session", workspace_manager=ws)
    [facts] = CheckoutMaterializer().facts([delivery], rt)
    assert "NOT cloned" in facts.lines[0] and "the clone failed" in facts.lines[0]
    assert "NOT through SRW's git swap driver" in facts.lines[0]
    assert "may not reach the upstream" in facts.lines[0]


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


class TestTokenUsername:
    def test_the_entrys_username_or_oauth2(self):
        from agent.connectors.checkout import token_username

        assert token_username({"token": TOKEN}) == "oauth2"
        minted = {"minted": True, "username": "x-access-token"}
        assert token_username(minted) == "x-access-token"
        # A static token's stored username is never honoured.
        assert token_username({"username": "x-access-token"}) == "oauth2"
        assert token_username({**minted, "minted": "yes"}) == "oauth2"
        # Nothing a URL's userinfo could be bent by.
        for odd in ("a:b", "a@b", "", "a/b", "x" * 65, 7, None):
            assert token_username({"minted": True, "username": odd}) == "oauth2"
        assert token_username("not a dict") == "oauth2"
