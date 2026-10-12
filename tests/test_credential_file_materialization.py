"""Credential files are delivered to the workspace (``agent.connectors.files``).

The validator (``orchestrator/security/credential_files.py``, separately
tested) resolves and stores each ``target_path`` against ``/home/srw``. The
materializer maps it to the same path under the workspace home if the
credential-file allowlist permits it, plans the store names, the kubeconfig
merge and the variables, and hands the whole set to the workspace backend
on every delivery (slice D1d). The workspace program itself is pinned in
``tests/test_workspace_credential_files.py``; the end to end tests here
drive it through a ``RemoteBackend`` whose secret stdin channel is a local
shell, with synthetic values only.
"""

from __future__ import annotations

import logging
import os
import shlex
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from agent.connectors import RuntimeContext, deliveries_from_payload
from agent.connectors.files import (
    MERGED_KUBECONFIG,
    CredentialFileMaterializer,
    _prefix_kubeconfig_yaml,
    home_target,
    merge_kubeconfigs,
    plan_credential_files,
)
from shared.runtime.core.backends.remote import RemoteBackend
from shared.runtime.core.credential_env import WORKSPACE_PYTHON
from tests._connector_goldens import resolved_row


def _kubeconfig(cluster: str, token: str) -> str:
    return (
        "apiVersion: v1\nkind: Config\ncurrent-context: default\n"
        f"clusters:\n  - name: {cluster}\n    cluster:\n"
        f"      server: https://{cluster}.example\n"
        f"users:\n  - name: admin\n    user:\n      token: {token}\n"
        "contexts:\n  - name: default\n    context:\n"
        f"      cluster: {cluster}\n      user: admin\n"
    )


def _kube_row(name: str, cluster: str, token: str, **files: Any) -> dict:
    slug = name.lower().replace(" ", "-")
    return {
        "type": "kubeconfig",
        "name": name,
        "credentials": {
            "files": [
                {
                    "contents": _kubeconfig(cluster, token),
                    "target_path": f"/home/srw/.kube/configs/{slug}.yaml",
                    "mode": "0600",
                    **files,
                }
            ]
        },
    }


def _file_row(name: str, *files: dict) -> dict:
    return {"type": "generic_file", "name": name, "credentials": {"files": list(files)}}


def _env(plan) -> dict[str, list[str]]:
    return {item["name"]: list(item["files"]) for item in plan.env}


# =============================================================================
# Targets
# =============================================================================


class TestHomeTarget:
    """The shared allowlist decides (``tests/test_credential_file_targets.py``)."""

    @pytest.mark.parametrize(
        ("path", "relative"),
        [
            ("/home/srw/.kube/configs/a.yaml", ".kube/configs/a.yaml"),
            ("~/.config/gcloud/sa.json", ".config/gcloud/sa.json"),
            ("/home/srw/./.srw-files/x/../y", ".srw-files/y"),
        ],
    )
    def test_an_allowed_home_path_maps_under_the_workspace_home(self, path, relative):
        assert home_target(path) == (relative, None)

    @pytest.mark.parametrize(
        "path", ["/tmp/x", "/workspace/x", "/run/x", "/home/srw", "/home/srw/../x"]
    )
    def test_a_path_outside_the_home_is_refused(self, path):
        assert home_target(path) == (None, "outside the home")

    @pytest.mark.parametrize(
        "path",
        [
            "/home/srw/.ssh/config",
            "/home/srw/.ssh/srw-managed/config",
            "/home/srw/.bashrc",
            "/home/srw/.local/bin/git",
            "/home/srw/.srw-credentials/x.sh",
            "/home/srw/workspace/x",
        ],
    )
    def test_anything_off_the_allowlist_is_refused(self, path):
        assert home_target(path) == (None, "not a credential-file location")


# =============================================================================
# Kubeconfigs
# =============================================================================


class TestPrefixKubeconfig:
    def test_prefixes_all_names(self):
        doc = yaml.safe_load(_prefix_kubeconfig_yaml(_kubeconfig("prod", "t"), "eu"))
        assert doc["clusters"][0]["name"] == "eu-prod"
        assert doc["users"][0]["name"] == "eu-admin"
        ctx = doc["contexts"][0]
        assert ctx["name"] == "eu-default"
        assert ctx["context"] == {"cluster": "eu-prod", "user": "eu-admin"}
        assert doc["current-context"] == "eu-default"

    def test_malformed_yaml_never_raises(self):
        bad = "this: is: not: yaml: at: all\n  - foo\n: bar"
        assert isinstance(_prefix_kubeconfig_yaml(bad, "eu"), str)

    def test_missing_sections_tolerated(self):
        out = _prefix_kubeconfig_yaml("clusters:\n  - name: only\n", "x")
        assert yaml.safe_load(out)["clusters"][0]["name"] == "x-only"


class TestMergeKubeconfigs:
    def test_every_name_is_kept_and_the_first_wins(self):
        merged = yaml.safe_load(
            merge_kubeconfigs(
                [
                    _prefix_kubeconfig_yaml(_kubeconfig("a", "ta"), "one"),
                    _prefix_kubeconfig_yaml(_kubeconfig("b", "tb"), "two"),
                    _prefix_kubeconfig_yaml(_kubeconfig("c", "tc"), "one"),
                ]
            )
        )
        assert [c["name"] for c in merged["contexts"]] == [
            "one-default",
            "two-default",
        ]
        assert [c["name"] for c in merged["clusters"]] == ["one-a", "two-b", "one-c"]
        assert merged["current-context"] == "one-default"
        assert merged["kind"] == "Config"

    @pytest.mark.parametrize("bad", ["- a list", "key: [unclosed", "plain text"])
    def test_an_input_that_is_no_kubeconfig_mapping_cannot_merge(self, bad):
        assert merge_kubeconfigs([_kubeconfig("a", "t"), bad]) is None


# =============================================================================
# The plan


class TestPlan:
    def test_files_are_linked_at_their_home_targets_with_their_modes(self):
        plan = plan_credential_files(
            deliveries_from_payload([resolved_row("generic_file")])
        )
        assert [(f["link"], f["mode"]) for f in plan.files] == [
            (".config/gcloud/sa.json", 0o600),
            (".config/gcloud/ca.pem", 0o644),
        ]
        names = [f["name"] for f in plan.files]
        assert len(set(names)) == 2
        assert all(not name.startswith(".") and "/" not in name for name in names)
        assert names[0].endswith("-sa.json")
        assert _env(plan) == {"GOOGLE_APPLICATION_CREDENTIALS": [names[0]]}

    def test_store_names_are_stable_per_target(self):
        rows = [resolved_row("generic_file")]
        first = plan_credential_files(deliveries_from_payload(rows)).files
        again = plan_credential_files(deliveries_from_payload(rows)).files
        assert [f["name"] for f in first] == [f["name"] for f in again]

    def test_kubeconfigs_are_prefixed_merged_and_named_by_kubeconfig(self):
        plan = plan_credential_files(
            deliveries_from_payload(
                [_kube_row("Prod EU", "a", "ta"), _kube_row("Staging", "b", "tb")]
            )
        )
        links = [f["link"] for f in plan.files]
        assert links == [
            ".kube/configs/prod-eu.yaml",
            ".kube/configs/staging.yaml",
            ".kube/config",
        ]
        merged = plan.files[-1]
        assert (merged["name"], merged["mode"]) == (MERGED_KUBECONFIG, 0o600)
        contexts = [c["name"] for c in yaml.safe_load(merged["content"])["contexts"]]
        assert contexts == ["prod-eu-default", "staging-default"]
        # The user's own ~/.kube/config stays visible after the merged one.
        assert plan.env == [
            {
                "name": "KUBECONFIG",
                "files": [MERGED_KUBECONFIG],
                "prepend": ".kube/config",
            }
        ]

    @pytest.mark.parametrize("kube_first", [False, True])
    def test_another_connectors_kube_config_is_never_linked_over(
        self, kube_first, caplog
    ):
        """A file connector (or a driver) delivering ~/.kube/config keeps it:
        the merged kubeconfig is listed after it, as after the user's own."""
        admin = _file_row(
            "Admin",
            {"contents": "admin", "target_path": "/home/srw/.kube/config"},
        )
        kube = _kube_row("Kube", "a", "t")
        rows = [kube, admin] if kube_first else [admin, kube]
        with caplog.at_level(logging.WARNING):
            plan = plan_credential_files(deliveries_from_payload(rows))
        by_link = {f["link"]: f for f in plan.files}
        assert by_link[".kube/config"]["content"] == "admin"
        merged = next(f for f in plan.files if f["name"] == MERGED_KUBECONFIG)
        assert merged["link"] is None
        assert [c["name"] for c in yaml.safe_load(merged["content"])["contexts"]] == [
            "kube-default"
        ]
        assert _env(plan) == {
            "KUBECONFIG": [by_link[".kube/config"]["name"], MERGED_KUBECONFIG]
        }
        assert plan.kubeconfig_after == "Admin"
        assert "'Admin' delivers ~/.kube/config" in caplog.text

    def test_a_file_claiming_a_kubeconfigs_target_keeps_it_in_the_merge(self, caplog):
        """The other file keeps the path; the kubeconfig is merged all the
        same, stored without a link of its own."""
        squatter = _file_row(
            "Squatter",
            {"contents": "x", "target_path": "/home/srw/.kube/configs/kube.yaml"},
        )
        rows = [squatter, _kube_row("Kube", "a", "t"), _kube_row("Other", "b", "u")]
        with caplog.at_level(logging.WARNING):
            plan = plan_credential_files(deliveries_from_payload(rows))
        links = [f["link"] for f in plan.files]
        assert links == [
            ".kube/configs/kube.yaml",
            None,
            ".kube/configs/other.yaml",
            ".kube/config",
        ]
        assert plan.files[0]["content"] == "x"
        assert len({f["name"] for f in plan.files}) == 4
        merged = plan.files[-1]
        assert [c["name"] for c in yaml.safe_load(merged["content"])["contexts"]] == [
            "kube-default",
            "other-default",
        ]
        assert plan.kubeconfig_after is None
        assert "merged without a link of its own" in caplog.text

    def test_kubeconfigs_that_cannot_merge_are_each_listed(self, caplog):
        broken = _kube_row("Broken", "x", "t")
        broken["credentials"]["files"][0]["contents"] = "- not a mapping"
        with caplog.at_level(logging.WARNING):
            plan = plan_credential_files(
                deliveries_from_payload([_kube_row("Good", "a", "t"), broken])
            )
        assert [f["link"] for f in plan.files] == [
            ".kube/configs/good.yaml",
            ".kube/configs/broken.yaml",
        ]
        assert _env(plan) == {"KUBECONFIG": [f["name"] for f in plan.files]}
        assert "could not be merged" in caplog.text

    def test_refused_and_duplicate_targets_are_skipped(self, caplog):
        """Rows saved before the allowlist: skipped, never delivered."""
        rows = [
            _file_row(
                "Mixed",
                {"contents": "a", "target_path": "/tmp/outside", "mode": "0600"},
                {
                    "contents": "b",
                    "target_path": "/home/srw/.ssh/config",
                    "mode": "0600",
                },
                {"contents": "c", "target_path": "/home/srw/.netrc", "mode": "0600"},
            ),
            _file_row(
                "Second",
                {"contents": "d", "target_path": "/home/srw/.netrc", "mode": "0600"},
            ),
        ]
        with caplog.at_level(logging.WARNING):
            plan = plan_credential_files(deliveries_from_payload(rows))
        assert [(f["link"], f["content"]) for f in plan.files] == [(".netrc", "c")]
        assert "outside the home" in caplog.text
        assert "not a credential-file location" in caplog.text
        assert "already delivers ~/.netrc" in caplog.text

    def test_a_reserved_repeated_or_kubeconfig_variable_is_skipped(self, caplog):
        rows = [
            _file_row(
                "Vars",
                *(
                    {
                        "contents": letter,
                        "target_path": f"/home/srw/.srw-files/{letter}",
                        "mode": "0600",
                        "env_var": name,
                    }
                    for letter, name in (
                        ("a", "PATH"),
                        ("b", "TOOL_TOKEN_FILE"),
                        ("c", "TOOL_TOKEN_FILE"),
                        ("d", "KUBECONFIG"),
                        ("e", "NODE_OPTIONS"),
                    )
                ),
            )
        ]
        with caplog.at_level(logging.WARNING):
            plan = plan_credential_files(deliveries_from_payload(rows))
        assert _env(plan) == {"TOOL_TOKEN_FILE": [plan.files[1]["name"]]}
        assert "PATH" in caplog.text and "reserved" in caplog.text
        assert (
            "KUBECONFIG for 'Vars': KUBECONFIG is reserved: it names the merged "
            "kubeconfig" in caplog.text
        )
        # A code hook saved before the one rule: skipped, and why.
        assert (
            "NODE_OPTIONS for 'Vars': NODE_OPTIONS is not a variable a connector "
            "may set" in caplog.text
        )
        # Its file still arrives.
        assert len(plan.files) == 5

    def test_a_bad_file_mode_warns_and_falls_back_to_0600(self, caplog):
        """The warning the agent always gave for an unreadable mode."""
        row = resolved_row("generic_file")
        row["credentials"]["files"][0]["mode"] = "0999"
        with caplog.at_level(logging.WARNING):
            plan = plan_credential_files(deliveries_from_payload([row]))
        assert "Bad mode '0999'" in caplog.text
        assert plan.files[0]["mode"] == 0o600

    def test_an_execute_bit_saved_before_the_rule_is_dropped(self, caplog):
        row = resolved_row("generic_file")
        row["credentials"]["files"][0]["mode"] = "0755"
        with caplog.at_level(logging.WARNING):
            plan = plan_credential_files(deliveries_from_payload([row]))
        assert plan.files[0]["mode"] == 0o644
        assert "grants more than read and write" in caplog.text

    def test_an_ssh_key_is_never_a_credential_file(self):
        plan = plan_credential_files(deliveries_from_payload([resolved_row("ssh_key")]))
        assert plan.files == [] and plan.env == []


# =============================================================================
# The materializer, against a recording backend
# =============================================================================


class _Backend:
    supports_shell = True
    store = "/home/agent-host/.srw-credentials/files-x"

    def __init__(self, *, fail: bool = False, report: dict | None = None):
        self.calls: list[tuple[list[dict], list[dict]]] = []
        self.fail = fail
        self.report = report or {}
        self.credential_files_report: dict | None = None

    def install_credential_files(self, files, env=()):
        if self.fail:
            raise RuntimeError("workspace gone")
        self.calls.append((list(files), list(env)))
        self.credential_files_report = {"store": self.store, **self.report}
        return self.credential_files_report


def _rt(backend: Any) -> RuntimeContext:
    return RuntimeContext(
        execution="session", workspace_manager=SimpleNamespace(backend=backend)
    )


class TestMaterializer:
    def test_materialize_syncs_the_files_and_their_variables(self):
        backend = _Backend()
        CredentialFileMaterializer().materialize(
            deliveries_from_payload(
                [resolved_row("generic_file"), _kube_row("Kube", "a", "t")]
            ),
            _rt(backend),
        )
        ((files, env),) = backend.calls
        assert len(files) == 4
        assert [item["name"] for item in env] == [
            "GOOGLE_APPLICATION_CREDENTIALS",
            "KUBECONFIG",
        ]

    def test_every_entry_point_syncs_a_shell_workspace_even_with_nothing(self):
        """A connector removed between attaches leaves nothing behind."""
        backend = _Backend()
        CredentialFileMaterializer().materialize([], _rt(backend))
        CredentialFileMaterializer().replace([], [], _rt(backend))
        CredentialFileMaterializer().on_backend_swap([], backend)
        assert backend.calls == [([], [])] * 3

    @pytest.mark.parametrize(
        "backend",
        [None, SimpleNamespace(supports_shell=False)],
        ids=["no workspace", "no shell"],
    )
    def test_a_workspace_without_a_shell_gets_nothing(self, backend, caplog):
        with caplog.at_level(logging.WARNING):
            CredentialFileMaterializer().materialize(
                deliveries_from_payload([resolved_row("generic_file")]), _rt(backend)
            )
            CredentialFileMaterializer().materialize([], _rt(backend))
        assert caplog.text.count("need a sandbox or VM workspace") == 1

    def test_a_failed_delivery_never_fails_the_work(self, caplog):
        with caplog.at_level(logging.WARNING):
            CredentialFileMaterializer().materialize(
                deliveries_from_payload([resolved_row("generic_file")]),
                _rt(_Backend(fail=True)),
            )
        assert "Failed to deliver credential files" in caplog.text

    def test_a_live_detach_syncs_what_remains(self):
        backend = _Backend()
        old = deliveries_from_payload(
            [resolved_row("generic_file"), _kube_row("Kube", "a", "t")]
        )
        new = deliveries_from_payload([resolved_row("generic_file")])
        CredentialFileMaterializer().replace(old, new, _rt(backend))
        ((files, env),) = backend.calls
        assert [f["link"] for f in files] == [
            ".config/gcloud/sa.json",
            ".config/gcloud/ca.pem",
        ]
        assert [item["name"] for item in env] == ["GOOGLE_APPLICATION_CREDENTIALS"]

    def test_a_backend_swap_delivers_onto_the_new_backend(self):
        backend = _Backend()
        CredentialFileMaterializer().on_backend_swap(
            deliveries_from_payload([_kube_row("Kube", "a", "t")]), backend
        )
        assert len(backend.calls) == 1 and backend.calls[0][1]

    def test_skipped_links_and_variables_are_logged(self, caplog):
        backend = _Backend(
            report={
                "skipped": {".kube/config": "a file of the user is there"},
                "env_skipped": {"KUBECONFIG": "set by another connector"},
            }
        )
        with caplog.at_level(logging.WARNING):
            CredentialFileMaterializer().materialize(
                deliveries_from_payload([_kube_row("Kube", "a", "t")]), _rt(backend)
            )
        assert "not linked at ~/.kube/config: a file of the user is there" in (
            caplog.text
        )
        assert "KUBECONFIG not set: set by another connector" in caplog.text


class TestFacts:
    def _facts(self, rows, backend=None):
        return [
            line
            for fact in CredentialFileMaterializer().facts(
                deliveries_from_payload(rows), _rt(backend)
            )
            for line in fact.lines
        ]

    def test_the_readme_names_home_paths_and_what_is_not_delivered(self):
        """The workspace home is not /home/srw: the README says ``~``."""
        row = _file_row(
            "Mixed",
            {
                "contents": "a",
                "target_path": "/home/srw/.netrc",
                "mode": "0600",
                "env_var": "NETRC",
            },
            {"contents": "b", "target_path": "/tmp/outside", "mode": "0600"},
            {"contents": "c", "target_path": "/home/srw/.ssh/config", "mode": "0600"},
            {
                "contents": "d",
                "target_path": "/home/srw/.srw-files/x",
                "mode": "0600",
                "env_var": "PATH",
            },
        )
        assert self._facts([row]) == [
            "- **Mixed** (file) — `~/.netrc` (`$NETRC`), "
            "`/tmp/outside` (not delivered: outside the home), "
            "`/home/srw/.ssh/config` (not delivered: not a credential-file "
            "location), `~/.srw-files/x` (variable not delivered by SRW: Environment name "
            "PATH is reserved by the workspace)"
        ]

    def test_the_readme_says_what_the_last_sync_could_not_place(self):
        backend = _Backend(
            report={
                "skipped": {".netrc": "a file of the user is there"},
                "env_skipped": {"TOOL": "set by another connector"},
            }
        )
        row = _file_row(
            "Logins",
            {
                "contents": "a",
                "target_path": "/home/srw/.netrc",
                "mode": "0600",
                "env_var": "NETRC",
            },
            {
                "contents": "b",
                "target_path": "/home/srw/.pgpass",
                "mode": "0600",
                "env_var": "TOOL",
            },
        )
        backend.credential_files_report = {"store": backend.store, **backend.report}
        assert self._facts([row], backend) == [
            "- **Logins** (file) — `~/.netrc` (not linked: a file of the user is "
            "there; `$NETRC`), `~/.pgpass` (`$TOOL` is another connector's)"
        ]

    @pytest.mark.parametrize(
        ("report", "where"),
        [
            ({}, "merged into `~/.kube/config` (`$KUBECONFIG`)"),
            (
                {"skipped": {".kube/config": "a file of the user is there"}},
                "merged into `$KUBECONFIG` after your own `~/.kube/config`, "
                "whose current context stays the default",
            ),
            (
                {"env_skipped": {"KUBECONFIG": "set by another connector"}},
                "merged into `~/.kube/config` (`$KUBECONFIG` is another connector's)",
            ),
        ],
    )
    def test_the_kubeconfig_line_follows_the_last_sync(self, report, where):
        backend = _Backend()
        backend.credential_files_report = report
        assert self._facts([_kube_row("Kube", "a", "t")], backend) == [
            f"- **Kube** (kubeconfig) — {where}; contexts prefixed `kube-*`. "
            "Where kubectl is installed, try `kubectl config get-contexts`."
        ]

    def test_the_readme_names_the_connector_whose_kube_config_comes_first(self):
        admin = _file_row(
            "Admin",
            {"contents": "admin", "target_path": "/home/srw/.kube/config"},
        )
        assert self._facts([admin, _kube_row("Kube", "a", "t")]) == [
            "- **Admin** (file) — `~/.kube/config`",
            "- **Kube** (kubeconfig) — merged into `$KUBECONFIG` after **Admin**'s "
            "`~/.kube/config`, whose current context stays the default; contexts "
            "prefixed `kube-*`. Where kubectl is installed, try `kubectl config "
            "get-contexts`.",
        ]

    def test_the_users_own_kube_config_stays_the_default_before_another_s(self):
        """The user's own file blocked the other connector's link: the user's
        context is the default, and the README says so first."""
        admin = _file_row(
            "Admin",
            {"contents": "admin", "target_path": "/home/srw/.kube/config"},
        )
        backend = _Backend()
        backend.credential_files_report = {
            "skipped": {".kube/config": "a file of the user is there"}
        }
        lines = self._facts([admin, _kube_row("Kube", "a", "t")], backend)
        assert lines[1] == (
            "- **Kube** (kubeconfig) — merged into `$KUBECONFIG` after your own "
            "`~/.kube/config`, whose current context stays the default, and "
            "**Admin**'s file for that path; contexts prefixed `kube-*`. Where "
            "kubectl is installed, try `kubectl config get-contexts`."
        )

    def test_a_kubeconfig_saved_off_the_allowlist_is_not_delivered(self):
        row = _kube_row("Kube", "a", "t", target_path="/tmp/kube.yaml")
        assert self._facts([row]) == [
            "- **Kube** (kubeconfig) — `/tmp/kube.yaml` not delivered: outside the home"
        ]


# =============================================================================
# End to end: the real workspace program behind a RemoteBackend
# =============================================================================


@pytest.fixture
def workspace(tmp_path: Path, monkeypatch):
    home = tmp_path / "agent-host"
    home.mkdir()
    backend = RemoteBackend(host="unused", job_id="job-d1d")
    monkeypatch.setattr(backend, "_init_shell", lambda: None)
    monkeypatch.setattr(backend, "_get_home_dir", lambda: str(home))
    sent: list[tuple[str, str]] = []

    def send(command, secret, **kwargs):
        sent.append((command, secret))
        completed = subprocess.run(
            ["bash", "-c", command.replace(WORKSPACE_PYTHON, "python3 -I")],
            input=secret,
            text=True,
            capture_output=True,
        )
        return completed.returncode, completed.stdout

    monkeypatch.setattr(
        backend, "execute_claim_resource_with_secret_stdin_output", send
    )
    return SimpleNamespace(home=home, backend=backend, sent=sent)


def _shell(workspace, script: str) -> str:
    """``script`` as the workspace shell runs it: the credential env sourced."""
    path = workspace.backend._credential_env_path
    prefix = f". {shlex.quote(path)}; " if path else ""
    return subprocess.check_output(
        ["bash", "-c", prefix + script], text=True, cwd=workspace.home
    )


def test_a_job_and_a_session_find_their_files_through_the_shell(workspace):
    deliveries = deliveries_from_payload(
        [
            resolved_row("generic_file"),
            _kube_row("Prod EU", "a", "token-a"),
            _kube_row("Staging", "b", "token-b"),
        ]
    )
    CredentialFileMaterializer().materialize(deliveries, _rt(workspace.backend))

    home = workspace.home
    kubeconfig = _shell(workspace, 'cat "$KUBECONFIG"')
    contexts = [c["name"] for c in yaml.safe_load(kubeconfig)["contexts"]]
    assert contexts == ["prod-eu-default", "staging-default"]
    assert (home / ".kube/config").is_symlink()
    staging = yaml.safe_load((home / ".kube/configs/staging.yaml").read_text())
    assert staging["current-context"] == "staging-default"
    assert _shell(workspace, 'cat "$GOOGLE_APPLICATION_CREDENTIALS"') == (
        '{"type": "service_account"}'
    )
    assert (home / ".config/gcloud/ca.pem").read_text().startswith("-----BEGIN")
    store = home / ".srw-credentials"
    assert all(
        (path.stat().st_mode & 0o777) in (0o600, 0o644)
        for path in store.rglob("*")
        if path.is_file()
    )
    # Contents travel on stdin, never in a command.
    for command, _secret in workspace.sent:
        assert "token-a" not in command and "service_account" not in command


def test_the_users_own_kubeconfig_stays_in_kubeconfig(workspace):
    (workspace.home / ".kube").mkdir()
    (workspace.home / ".kube/config").write_text("the user's own\n")
    CredentialFileMaterializer().materialize(
        deliveries_from_payload([_kube_row("Kube", "a", "t")]),
        _rt(workspace.backend),
    )
    value = _shell(workspace, 'printf %s "$KUBECONFIG"')
    own, merged = value.split(":")
    # The user's own first: its current context stays the default.
    assert own == str(workspace.home / ".kube/config")
    assert merged.endswith("/kubeconfig")
    assert (workspace.home / ".kube/config").read_text() == "the user's own\n"


def test_another_connectors_kube_config_comes_first_in_the_workspace(workspace):
    """The real workspace program: ~/.kube/config stays the other connector's
    file, and KUBECONFIG lists it before the merged one."""
    admin = _file_row(
        "Admin",
        {"contents": "admin's own\n", "target_path": "/home/srw/.kube/config"},
    )
    CredentialFileMaterializer().materialize(
        deliveries_from_payload([admin, _kube_row("Kube", "a", "t")]),
        _rt(workspace.backend),
    )
    assert (workspace.home / ".kube/config").read_text() == "admin's own\n"
    first, merged = _shell(workspace, 'printf %s "$KUBECONFIG"').split(":")
    assert Path(first).read_text() == "admin's own\n"
    assert merged.endswith("/kubeconfig")
    contexts = yaml.safe_load(Path(merged).read_text())["contexts"]
    assert [c["name"] for c in contexts] == ["kube-default"]


def test_a_detach_between_attaches_removes_the_files_and_the_variable(workspace):
    """No live diff: the next attach's sync, from the workspace's own record."""
    kube = _kube_row("Kube", "a", "t")
    files = resolved_row("generic_file")
    materializer = CredentialFileMaterializer()
    materializer.materialize(
        deliveries_from_payload([kube, files]), _rt(workspace.backend)
    )
    assert (workspace.home / ".kube/config").exists()

    # The kubeconfig connector was removed while the session was idle.
    materializer.materialize(deliveries_from_payload([files]), _rt(workspace.backend))
    assert not os.path.lexists(workspace.home / ".kube/config")
    assert not (workspace.home / ".kube").exists()
    assert _shell(workspace, 'printf %s "${KUBECONFIG-unset}"') == "unset"
    assert (workspace.home / ".config/gcloud/sa.json").exists()

    materializer.materialize([], _rt(workspace.backend))
    assert not os.path.lexists(workspace.home / ".config/gcloud/sa.json")
    assert (
        _shell(workspace, 'printf %s "${GOOGLE_APPLICATION_CREDENTIALS-unset}"')
        == "unset"
    )
