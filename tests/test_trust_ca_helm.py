"""The chart delivers the operator's extra CA to every consumer.

Regression for APP-TLS-1 (university upgrade test 2026-10-14): chart 0.0.29 had
no way to trust a private CA, so an on-premises HTTPS model API could not be
used. ``trust.extraCaCertificates`` (or ``trust.existingConfigMap``) now
renders one ConfigMap that the orchestrator and the stateless agent pool mount;
the orchestrator also receives its name so the provisioners mount it into the
pods they build (``tests/test_tls_trust.py``).
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

from shared.tls_trust import (
    EXTRA_CA_FILE_ENV,
    TRUST_CA_CONFIGMAP_ENV,
    TRUST_CA_CONFIGMAP_KEY_ENV,
    TRUST_CA_FILE,
    TRUST_CA_VOLUME,
)
from tests.test_tls_trust import PEM_BEGIN, _pki

CHART = Path(__file__).resolve().parents[1] / "helm"


@pytest.fixture
def pki(tmp_path):
    return _pki(tmp_path)


def _render(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "helm",
            "template",
            "srw",
            str(CHART),
            "-f",
            str(CHART / "ci/test-values.yaml"),
            "--set",
            "agent.stateless.enabled=true",
            "--set",
            "agent.stateless.autoscaling.enabled=false",
            *args,
        ],
        capture_output=True,
        text=True,
    )


def _deployment(documents: list[dict], suffix: str) -> dict:
    return next(
        d
        for d in documents
        if d.get("kind") == "Deployment" and d["metadata"]["name"].endswith(suffix)
    )


def _container(deployment: dict, name: str) -> dict:
    return next(
        c
        for c in deployment["spec"]["template"]["spec"]["containers"]
        if c["name"] == name
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is not installed")
class TestChart:
    def test_inline_ca_reaches_the_orchestrator_and_the_stateless_pool(self, pki):
        rendered = _render("--set-file", f"trust.extraCaCertificates={pki['ca']}")
        assert rendered.returncode == 0, rendered.stderr
        documents = [d for d in yaml.safe_load_all(rendered.stdout) if d]

        configmap = next(
            d
            for d in documents
            if d.get("kind") == "ConfigMap"
            and d["metadata"]["name"].endswith("-trust-ca")
        )
        assert configmap["data"]["ca.crt"].strip() == pki["ca"].read_text().strip()
        name = configmap["metadata"]["name"]

        orchestrator = _deployment(documents, "-orchestrator")
        container = _container(orchestrator, "orchestrator")
        env = {e["name"]: e.get("value") for e in container["env"]}
        assert env[EXTRA_CA_FILE_ENV] == TRUST_CA_FILE
        assert env[TRUST_CA_CONFIGMAP_ENV] == name
        assert env[TRUST_CA_CONFIGMAP_KEY_ENV] == "ca.crt"
        # Appended after every pre-existing entry (strategic-merge safety).
        assert [e["name"] for e in container["env"]][-3:] == [
            EXTRA_CA_FILE_ENV,
            TRUST_CA_CONFIGMAP_ENV,
            TRUST_CA_CONFIGMAP_KEY_ENV,
        ]
        volumes = orchestrator["spec"]["template"]["spec"]["volumes"]
        assert {v["name"]: v for v in volumes}[TRUST_CA_VOLUME]["configMap"][
            "name"
        ] == name
        assert TRUST_CA_VOLUME in {m["name"] for m in container["volumeMounts"]}

        stateless = _deployment(documents, "-agent-stateless")
        agent = _container(stateless, "agent")
        assert {"name": EXTRA_CA_FILE_ENV, "value": TRUST_CA_FILE} in agent["env"]
        assert TRUST_CA_VOLUME in {m["name"] for m in agent["volumeMounts"]}
        assert {v["name"]: v for v in stateless["spec"]["template"]["spec"]["volumes"]}[
            TRUST_CA_VOLUME
        ]["configMap"]["name"] == name

    def test_existing_configmap_is_referenced_not_rendered(self):
        rendered = _render(
            "--set",
            "trust.existingConfigMap=uni-roots",
            "--set",
            "trust.existingConfigMapKey=bundle.pem",
        )
        assert rendered.returncode == 0, rendered.stderr
        documents = [d for d in yaml.safe_load_all(rendered.stdout) if d]
        assert not any(
            d.get("kind") == "ConfigMap" and d["metadata"]["name"].endswith("-trust-ca")
            for d in documents
        )
        orchestrator = _deployment(documents, "-orchestrator")
        env = {
            e["name"]: e.get("value")
            for e in _container(orchestrator, "orchestrator")["env"]
        }
        assert env[TRUST_CA_CONFIGMAP_ENV] == "uni-roots"
        assert env[TRUST_CA_CONFIGMAP_KEY_ENV] == "bundle.pem"

    def test_default_install_has_no_trust_wiring(self):
        rendered = _render()
        assert rendered.returncode == 0, rendered.stderr
        assert TRUST_CA_VOLUME not in rendered.stdout
        assert EXTRA_CA_FILE_ENV not in rendered.stdout

    @pytest.mark.parametrize(
        ("args", "message"),
        [
            (("--set", "trust.extraCaCertificates=not-a-pem"), "must hold PEM"),
            (
                (
                    "--set",
                    "trust.existingConfigMap=x",
                    "--set",
                    f"trust.extraCaCertificates={PEM_BEGIN}",
                ),
                "not both",
            ),
        ],
    )
    def test_misconfiguration_fails_the_render(self, args, message):
        rendered = _render(*args)
        assert rendered.returncode != 0
        assert message in rendered.stderr
