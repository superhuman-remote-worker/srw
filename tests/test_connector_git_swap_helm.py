"""The git swap driver's chart surface (connector drivers C3).

``connectors.drivers.gitSwap`` installs the driver (with service-pod
hosting) and SRW's connector driver certificate authority, which only the
orchestrator mounts; its fallback reaches the orchestrator either way.
"""

from __future__ import annotations

import ast
import base64
import subprocess

import pytest
import yaml

from orchestrator.services.connector_driver_ca import DriverCertificateAuthority
from tests.test_connector_service_hosting_helm import (
    EXCHANGE,
    ON,
    ROOT,
    orchestrator_env,
    render,
)

DIGEST = "sha256:" + "6" * 64
SWAP = "connectors.drivers.gitSwap.enabled=true"


def _deployment(docs: list[dict]) -> dict:
    return next(
        doc
        for doc in docs
        if doc["kind"] == "Deployment"
        and doc["metadata"]["name"].endswith("-orchestrator")
    )


def _ca_secrets(docs: list[dict]) -> list[dict]:
    return [
        doc
        for doc in docs
        if doc["kind"] == "Secret"
        and doc["metadata"]["name"].endswith("-connector-driver-ca")
    ]


def _ca_volume(docs: list[dict]) -> dict | None:
    volumes = _deployment(docs)["spec"]["template"]["spec"]["volumes"]
    return next((v for v in volumes if v["name"] == "connector-driver-ca"), None)


def _ca_mount(docs: list[dict]) -> dict | None:
    container = next(
        c
        for c in _deployment(docs)["spec"]["template"]["spec"]["containers"]
        if c["name"] == "orchestrator"
    )
    return next(
        (m for m in container["volumeMounts"] if m["name"] == "connector-driver-ca"),
        None,
    )


def test_off_by_default_the_fallback_still_reaches_the_orchestrator():
    docs = render()
    env = orchestrator_env(docs)
    assert env["CONNECTOR_GIT_SWAP_FALLBACK"] == "token-in-url"
    assert "CONNECTOR_GIT_SWAP_IMAGE" not in env
    assert "CONNECTOR_DRIVER_CA_DIR" not in env
    assert _ca_secrets(docs) == [] and _ca_volume(docs) is None
    assert (
        orchestrator_env(render("connectors.drivers.gitSwap.fallback=refuse"))[
            "CONNECTOR_GIT_SWAP_FALLBACK"
        ]
        == "refuse"
    )


def test_on_it_runs_its_image_and_the_orchestrator_mounts_the_authority():
    docs = render(
        EXCHANGE,
        ON,
        SWAP,
        f"connectors.drivers.gitSwap.image.digest={DIGEST}",
        "connectors.drivers.gitSwap.image.tag=1.2.3",
    )
    env = orchestrator_env(docs)
    assert env["CONNECTOR_GIT_SWAP_IMAGE"] == (
        f"ghcr.io/superhuman-remote-worker/srw-driver-git-swap:1.2.3@{DIGEST}"
    )
    assert env["CONNECTOR_DRIVER_CA_DIR"] == "/run/srw/connector-driver-ca"
    [secret] = _ca_secrets(docs)
    assert secret["type"] == "kubernetes.io/tls"
    assert secret["metadata"]["annotations"]["helm.sh/resource-policy"] == "keep"
    volume = _ca_volume(docs)
    assert volume["secret"]["secretName"] == secret["metadata"]["name"]
    assert [item["key"] for item in volume["secret"]["items"]] == ["tls.crt", "tls.key"]
    assert _ca_mount(docs) == {
        "name": "connector-driver-ca",
        "mountPath": "/run/srw/connector-driver-ca",
        "readOnly": True,
    }
    # Only the orchestrator mounts it.
    mounted = [
        doc["metadata"]["name"]
        for doc in docs
        if doc["kind"] in ("Deployment", "StatefulSet", "DaemonSet", "Job")
        and any(
            (v.get("secret") or {}).get("secretName") == secret["metadata"]["name"]
            for v in doc["spec"]["template"]["spec"].get("volumes") or []
        )
    ]
    assert mounted == [_deployment(docs)["metadata"]["name"]]


def test_helms_generated_authority_signs_a_driver_certificate():
    docs = render(EXCHANGE, ON, SWAP)
    [secret] = _ca_secrets(docs)
    authority = DriverCertificateAuthority.from_pem(
        base64.b64decode(secret["data"]["tls.crt"]),
        base64.b64decode(secret["data"]["tls.key"]),
    )
    certificate, key = authority.issue(["srw-ep-x.srw-connectors.svc.cluster.local"])
    assert "BEGIN CERTIFICATE" in certificate and "PRIVATE KEY" in key
    assert (authority.not_after.year - 2026) >= 9


def test_an_operators_own_authority_is_mounted_and_none_is_generated():
    docs = render(
        EXCHANGE, ON, SWAP, "connectors.drivers.ca.secretName=vault-driver-ca"
    )
    assert _ca_secrets(docs) == []
    assert _ca_volume(docs)["secret"]["secretName"] == "vault-driver-ca"


@pytest.mark.parametrize(
    "settings",
    [
        (SWAP,),
        (EXCHANGE, ON, SWAP, "connectors.drivers.gitSwap.image.repository="),
        ("connectors.drivers.gitSwap.fallback=leak",),
        (EXCHANGE, ON, SWAP, "connectors.drivers.ca.validityDays=1"),
    ],
)
def test_an_incomplete_setup_fails_to_render(settings):
    with pytest.raises(subprocess.CalledProcessError):
        render(*settings)


def test_the_k3d_profile_turns_it_on_with_the_token_in_url_fallback():
    example = ROOT / "deployment/values-local.yaml.example"
    swap = yaml.safe_load(example.read_text())["connectors"]["drivers"]["gitSwap"]
    assert swap["enabled"] is True and swap["fallback"] == "token-in-url"
    docs = render(values=(example,))
    env = orchestrator_env(docs)
    assert env["CONNECTOR_GIT_SWAP_IMAGE"].startswith(
        "srw-registry:5000/srw-driver-git-swap:"
    )
    assert len(_ca_secrets(docs)) == 1


def test_tilt_builds_the_driver_and_pins_it_by_digest():
    tiltfile = (ROOT / "Tiltfile").read_text()
    builds = {
        node.args[0].value: {item.arg: item.value for item in node.keywords}
        for node in ast.walk(ast.parse(tiltfile))
        if isinstance(node, ast.Call)
        and getattr(node.func, "id", None) == "docker_build"
        and node.args
        and isinstance(node.args[0], ast.Constant)
    }
    keywords = builds["srw-driver-git-swap"]
    assert (
        ast.literal_eval(keywords["dockerfile"]) == "docker/Dockerfile.driver-git-swap"
    )
    assert "drivers/git-swap/" in ast.literal_eval(keywords["only"])
    assert (
        "('srw-driver-git-swap', 'connectors.drivers.gitSwap.image.repository', "
        "'connectors.drivers.gitSwap.image.tag')" in tiltfile
    )
    assert "'srw-driver-git-swap']" in tiltfile


def test_the_image_runs_the_driver_as_an_unprivileged_user_with_ca_roots():
    dockerfile = (ROOT / "docker/Dockerfile.driver-git-swap").read_text()
    assert "COPY drivers/git-swap/ ./" in dockerfile
    assert "/etc/ssl/certs/ca-certificates.crt" in dockerfile
    assert "USER 65532:65532" in dockerfile
    assert 'ENTRYPOINT ["/srw-git-swap"]' in dockerfile
    assert 'CMD ["serve"]' in dockerfile
    shim = (ROOT / "docker/Dockerfile.driver-shim").read_text()
    base = next(line for line in dockerfile.splitlines() if line.startswith("FROM --"))
    assert base in shim, "the driver builds with the shim's pinned toolchain"
