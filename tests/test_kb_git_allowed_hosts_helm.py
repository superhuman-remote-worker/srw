"""The chart trusts its own Gitea for external OKF KB datasources.

``KB_GIT_ALLOWED_HOSTS`` is derived from the chart's Gitea (public host and
in-cluster endpoint) plus ``orchestrator.kbGitAllowedHosts``. The orchestrator
rejects the whole list when one entry is localhost or an IP literal, so such
derived hosts must never reach it.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

from orchestrator.services.kb_git_source import _trusted_git_endpoints

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="Helm is not installed"
)


def allowed_hosts(*settings: str) -> str | None:
    command = [
        "helm",
        "template",
        "srw",
        str(ROOT / "helm"),
        "-n",
        "srw",
        "-f",
        str(ROOT / "helm/ci/test-values.yaml"),
    ]
    for setting in settings:
        command.extend(["--set", setting])
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    for doc in yaml.safe_load_all(result.stdout):
        if (
            doc
            and doc["kind"] == "Deployment"
            and doc["metadata"]["name"].endswith("-orchestrator")
        ):
            container = next(
                c
                for c in doc["spec"]["template"]["spec"]["containers"]
                if c["name"] == "orchestrator"
            )
            env = {item["name"]: item.get("value") for item in container["env"]}
            return env.get("KB_GIT_ALLOWED_HOSTS")
    raise AssertionError("no orchestrator Deployment rendered")


def test_the_bundled_gitea_is_trusted_by_default():
    assert allowed_hosts() == (
        "git.test.example.com,srw-superhuman-remote-worker-gitea:3000"
    )
    assert (
        allowed_hosts("fullnameOverride=srw", "global.hostnames.git=code.example.com")
        == "code.example.com,srw-gitea:3000"
    )


def test_extra_hosts_follow_the_derived_ones():
    assert allowed_hosts(
        r"orchestrator.kbGitAllowedHosts=gitea.example.com\, gitea.internal:3000"
    ) == (
        "git.test.example.com,srw-superhuman-remote-worker-gitea:3000,"
        "gitea.example.com,gitea.internal:3000"
    )


def test_an_external_gitea_contributes_its_configured_urls():
    assert (
        allowed_hosts(
            "gitea.internal=false",
            "gitea.externalUrl=https://code.example.org:8443/",
            "gitea.internalUrl=http://gitea.tools.svc:3000",
        )
        == "code.example.org:8443,gitea.tools.svc:3000"
    )


def test_localhost_and_ip_hosts_are_left_out(monkeypatch):
    local = allowed_hosts("global.domain=localhost", "fullnameOverride=srw")
    assert local == "srw-gitea:3000"
    monkeypatch.setenv("KB_GIT_ALLOWED_HOSTS", local)
    assert ("srw-gitea", 3000) in _trusted_git_endpoints()

    assert (
        allowed_hosts(
            "gitea.internal=false",
            "gitea.externalUrl=https://192.0.2.10/git",
            "gitea.internalUrl=http://10.0.0.5:3000",
        )
        is None
    )


def test_no_gitea_and_no_extras_render_no_variable():
    assert allowed_hosts("gitea.enabled=false") is None
    assert (
        allowed_hosts("gitea.enabled=false", "orchestrator.kbGitAllowedHosts=a.example")
        == "a.example"
    )
