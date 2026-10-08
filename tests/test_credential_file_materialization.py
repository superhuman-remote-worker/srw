"""Credential files are delivered to the workspace (``agent.connectors.files``).

The validator (``orchestrator/security/credential_files.py``, separately
tested) resolves and stores each ``target_path`` against ``/home/srw``. The
materializer maps it to the same path under the workspace home, plans the
store names, the kubeconfig merge and the environment variables, and hands
the whole set to the workspace backend (slice D1d). The workspace program
itself is pinned in ``tests/test_workspace_credential_files.py``; the end to
end tests here drive it through a ``RemoteBackend`` whose secret stdin
channel is a local shell, with synthetic values only.
"""

from __future__ import annotations

import json
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
    REFUSED_TARGETS,
    CredentialFileMaterializer,
    _prefix_kubeconfig_yaml,
    home_target,
    merge_kubeconfigs,
    plan_credential_files,
)
from shared.runtime.core.backends.remote import RemoteBackend
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


# =============================================================================
# Targets
# =============================================================================


class TestHomeTarget:
    @pytest.mark.parametrize(
        ("path", "relative"),
        [
            ("/home/srw/.kube/configs/a.yaml", ".kube/configs/a.yaml"),
            ("~/.config/gcloud/sa.json", ".config/gcloud/sa.json"),
            ("/home/srw/./x/../y", "y"),
        ],
    )
    def test_a_home_path_maps_under_the_workspace_home(self, path, relative):
        assert home_target(path) == (relative, None)

    @pytest.mark.parametrize(
        "path",
        ["/tmp/x", "/workspace/x", "/run/x", "/home/srw", "/home/srw/../x", ""],
    )
    def test_a_path_outside_the_home_is_refused(self, path):
        assert home_target(path) == (None, "outside the home")

    @pytest.mark.parametrize(
        "relative",
        sorted(REFUSED_TARGETS)
        + [".srw-credentials/x.sh", ".srw-credentials", ".ssh/srw-managed/config"],
    )
    def test_srw_and_start_up_files_are_reserved(self, relative):
        assert home_target(f"/home/srw/{relative}") == (
            None,
            "reserved by the workspace",
        )


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
# =============================================================================


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
        assert plan.env == {"GOOGLE_APPLICATION_CREDENTIALS": (names[0],)}

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
        assert plan.env == {"KUBECONFIG": (MERGED_KUBECONFIG,)}

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
        assert plan.env == {"KUBECONFIG": tuple(f["name"] for f in plan.files)}
        assert "could not be merged" in caplog.text

    def test_refused_and_duplicate_targets_are_skipped(self, caplog):
        rows = [
            _file_row(
                "Mixed",
                {"contents": "a", "target_path": "/tmp/outside", "mode": "0600"},
                {"contents": "b", "target_path": "/home/srw/.bashrc", "mode": "0600"},
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
        assert "reserved by the workspace" in caplog.text
        assert "already delivers ~/.netrc" in caplog.text

    def test_a_reserved_or_repeated_variable_is_skipped(self, caplog):
        rows = [
            _file_row(
                "Vars",
                {
                    "contents": "a",
                    "target_path": "/home/srw/a",
                    "mode": "0600",
                    "env_var": "PATH",
                },
                {
                    "contents": "b",
                    "target_path": "/home/srw/b",
                    "mode": "0600",
                    "env_var": "TOOL_CONFIG",
                },
                {
                    "contents": "c",
                    "target_path": "/home/srw/c",
                    "mode": "0600",
                    "env_var": "TOOL_CONFIG",
                },
            )
        ]
        with caplog.at_level(logging.WARNING):
            plan = plan_credential_files(deliveries_from_payload(rows))
        assert list(plan.env) == ["TOOL_CONFIG"]
        assert plan.env["TOOL_CONFIG"] == (plan.files[1]["name"],)
        assert "PATH" in caplog.text and "reserved" in caplog.text

    def test_a_bad_file_mode_warns_and_falls_back_to_0600(self, caplog):
        """The warning the agent always gave for an unreadable mode."""
        row = resolved_row("generic_file")
        row["credentials"]["files"][0]["mode"] = "0999"
        with caplog.at_level(logging.WARNING):
            plan = plan_credential_files(deliveries_from_payload([row]))
        assert "Bad mode '0999'" in caplog.text
        assert plan.files[0]["mode"] == 0o600

    def test_an_ssh_key_is_never_a_credential_file(self):
        plan = plan_credential_files(deliveries_from_payload([resolved_row("ssh_key")]))
        assert plan.files == [] and plan.env == {}


# =============================================================================
# The materializer, against a recording backend
# =============================================================================


class _Backend:
    supports_shell = True

    def __init__(self, *, fail: bool = False):
        self.files: list[list[dict]] = []
        self.env: list[dict[str, str]] = []
        self.fail = fail

    def install_credential_files(self, files):
        if self.fail:
            raise RuntimeError("workspace gone")
        self.files.append(list(files))
        return "/home/agent-host/.srw-credentials/files-x"

    def install_credential_environment(self, values):
        self.env.append(dict(values))


def _rt(backend: Any) -> RuntimeContext:
    return RuntimeContext(
        execution="session", workspace_manager=SimpleNamespace(backend=backend)
    )


class TestMaterializer:
    def test_materialize_syncs_the_set_and_points_variables_at_the_store(self):
        backend = _Backend()
        CredentialFileMaterializer().materialize(
            deliveries_from_payload(
                [resolved_row("generic_file"), _kube_row("Kube", "a", "t")]
            ),
            _rt(backend),
        )
        (files,) = backend.files
        assert len(files) == 4
        (env,) = backend.env
        store = "/home/agent-host/.srw-credentials/files-x"
        assert env["KUBECONFIG"] == f"{store}/{MERGED_KUBECONFIG}"
        assert env["GOOGLE_APPLICATION_CREDENTIALS"] == f"{store}/{files[0]['name']}"

    def test_nothing_to_deliver_touches_no_workspace(self):
        backend = _Backend()
        CredentialFileMaterializer().materialize([], _rt(backend))
        CredentialFileMaterializer().replace([], [], _rt(backend))
        CredentialFileMaterializer().on_backend_swap([], backend)
        assert backend.files == [] and backend.env == []

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
        assert "need a sandbox or VM workspace" in caplog.text

    def test_a_failed_delivery_never_fails_the_work(self, caplog):
        with caplog.at_level(logging.WARNING):
            CredentialFileMaterializer().materialize(
                deliveries_from_payload([resolved_row("generic_file")]),
                _rt(_Backend(fail=True)),
            )
        assert "Failed to deliver credential files" in caplog.text

    def test_a_live_detach_syncs_the_rest_and_empties_retired_variables(self):
        backend = _Backend()
        old = deliveries_from_payload(
            [resolved_row("generic_file"), _kube_row("Kube", "a", "t")]
        )
        new = deliveries_from_payload([resolved_row("generic_file")])
        CredentialFileMaterializer().replace(old, new, _rt(backend))
        (files,) = backend.files
        assert [f["link"] for f in files] == [
            ".config/gcloud/sa.json",
            ".config/gcloud/ca.pem",
        ]
        (env,) = backend.env
        assert env["KUBECONFIG"] == ""
        assert env["GOOGLE_APPLICATION_CREDENTIALS"].endswith("-sa.json")

    def test_detaching_the_last_file_empties_the_store(self):
        backend = _Backend()
        old = deliveries_from_payload([_kube_row("Kube", "a", "t")])
        CredentialFileMaterializer().replace(old, [], _rt(backend))
        assert backend.files == [[]]
        assert backend.env == [{"KUBECONFIG": ""}]

    def test_a_backend_swap_delivers_onto_the_new_backend(self):
        backend = _Backend()
        CredentialFileMaterializer().on_backend_swap(
            deliveries_from_payload([_kube_row("Kube", "a", "t")]), backend
        )
        assert len(backend.files) == 1 and backend.env


# =============================================================================
# End to end: the real workspace programs behind a RemoteBackend
# =============================================================================


@pytest.fixture
def workspace(tmp_path: Path, monkeypatch):
    home = tmp_path / "agent-host"
    home.mkdir()
    backend = RemoteBackend(host="unused", job_id="job-d1d")
    monkeypatch.setattr(backend, "_init_shell", lambda: None)
    monkeypatch.setattr(backend, "_get_home_dir", lambda: str(home))
    monkeypatch.setattr(
        backend, "_resolve_home_path", lambda relative: str(home / relative)
    )
    sent: list[tuple[str, str]] = []

    def send(command, secret, **kwargs):
        sent.append((command, secret))
        return (
            subprocess.run(
                ["bash", "-c", command], input=secret, text=True, capture_output=True
            ).returncode
            == 0
        )

    monkeypatch.setattr(backend, "execute_claim_resource_with_secret_stdin", send)
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
    assert yaml.safe_load((home / ".kube/config").read_text())["contexts"]
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
        if path.is_file() and path.suffix != ".sh"
    )
    # Contents travel on stdin, never in a command.
    for command, _secret in workspace.sent:
        assert "token-a" not in command and "service_account" not in command


def test_a_live_detach_removes_the_files_and_the_variable(workspace):
    kube = _kube_row("Kube", "a", "t")
    files = resolved_row("generic_file")
    materializer = CredentialFileMaterializer()
    materializer.materialize(
        deliveries_from_payload([kube, files]), _rt(workspace.backend)
    )
    assert (workspace.home / ".kube/config").exists()

    materializer.replace(
        deliveries_from_payload([kube, files]),
        deliveries_from_payload([files]),
        _rt(workspace.backend),
    )
    assert not os.path.lexists(workspace.home / ".kube/config")
    assert not (workspace.home / ".kube").exists()
    assert _shell(workspace, 'printf %s "${KUBECONFIG-unset}"') == ""
    assert (workspace.home / ".config/gcloud/sa.json").exists()

    materializer.replace(deliveries_from_payload([files]), [], _rt(workspace.backend))
    assert not os.path.lexists(workspace.home / ".config/gcloud/sa.json")
    assert json.loads(workspace.sent[-2][1]) == []
