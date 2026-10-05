"""The native first-use verifier receives only the configured gateway public keys."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from orchestrator.services.agent_provisioner import AgentProvisioner


CHART = Path(__file__).resolve().parents[1] / "helm"
HOST_DIR = "/run/secrets/ssh-gateway/host"
PUBLIC_KEYS = f"{HOST_DIR}/ssh_host_ed25519_key.pub,{HOST_DIR}/gateway_blue-2026.10.pub"
ENABLE = [
    "--set",
    "global.domain=example.com",
    "--set",
    "license.acceptTerms=true",
    "--set",
    "sshGateway.enabled=true",
    "--set",
    "sshGateway.hostname=ssh.example.com",
    "--set",
    "sshGateway.allowedOrigins={https://app.example.com}",
    "--set",
    "sshGateway.trustedProxies=10.42.0.0/16",
    "--set",
    "sshGateway.hostKeySecret=srw-ssh-gateway-hostkey",
    "--set",
    "sshGateway.userCaSecret=srw-ssh-gateway-ca",
    "--set",
    "sessionRouter.jwtSecret=chart-test-not-a-real-key",
]


def _chart(*extra: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["helm", "template", "srw", str(CHART), *ENABLE, *extra],
        capture_output=True,
        text=True,
    )


def _agent_manifest(
    provisioner: AgentProvisioner, purpose: str, thread_id: str | None
) -> dict:
    return provisioner._build_pod_manifest(
        pod_name="srw-agent-test",
        purpose=purpose,
        thread_id=thread_id,
        config_name="interactive",
        cpu_request="100m",
        memory_request="256Mi",
        cpu_limit="1",
        memory_limit="2Gi",
    )


def test_chart_passes_public_key_secret_metadata_for_dynamic_agents() -> None:
    rendered = _chart(
        "--set", "sshGateway.hostKeyNames={ssh_host_ed25519_key,gateway_blue-2026.10}"
    )
    assert rendered.returncode == 0, rendered.stderr
    deployment = next(
        doc
        for doc in yaml.safe_load_all(rendered.stdout)
        if doc
        and doc.get("kind") == "Deployment"
        and doc["metadata"]["name"].endswith("-orchestrator")
    )
    pod = deployment["spec"]["template"]["spec"]
    agent = next(c for c in pod["containers"] if c["name"] == "orchestrator")
    env = {entry["name"]: entry for entry in agent["env"]}
    assert env["SSH_GATEWAY_PUBLIC_HOST_KEYS"]["value"] == PUBLIC_KEYS
    assert (
        env["AGENT_SSH_GATEWAY_HOST_KEY_SECRET"]["value"] == "srw-ssh-gateway-hostkey"
    )
    assert [
        i["key"]
        for i in next(
            v for v in pod["volumes"] if v["name"] == "ssh-gateway-host-keys"
        )["secret"]["items"]
    ] == ["ssh_host_ed25519_key.pub", "gateway_blue-2026.10.pub"]


@pytest.mark.parametrize(
    ("purpose", "thread_id"),
    [
        ("session", "11111111-2222-4333-8444-555555555555"),
        ("session", None),
        ("job", None),
    ],
)
def test_dynamic_agent_roles_project_only_rotating_public_keys(
    monkeypatch: pytest.MonkeyPatch, purpose: str, thread_id: str | None
) -> None:
    monkeypatch.setenv("SSH_GATEWAY_PUBLIC_HOST_KEYS", PUBLIC_KEYS)
    monkeypatch.setenv("AGENT_SSH_GATEWAY_HOST_KEY_SECRET", "srw-ssh-gateway-hostkey")
    manifest = _agent_manifest(AgentProvisioner(), purpose, thread_id)
    pod = manifest["spec"]
    agent = next(c for c in pod["containers"] if c["name"] == "agent")
    env = {entry["name"]: entry for entry in agent["env"]}
    assert env["SSH_GATEWAY_PUBLIC_HOST_KEYS"]["value"] == PUBLIC_KEYS
    mount = next(
        m for m in agent["volumeMounts"] if m["name"] == "ssh-gateway-host-keys"
    )
    assert mount == {
        "name": "ssh-gateway-host-keys",
        "mountPath": HOST_DIR,
        "readOnly": True,
    }
    secret = next(v for v in pod["volumes"] if v["name"] == "ssh-gateway-host-keys")[
        "secret"
    ]
    assert secret["secretName"] == "srw-ssh-gateway-hostkey"
    assert secret["defaultMode"] == 0o444
    assert secret["items"] == [
        {"key": "ssh_host_ed25519_key.pub", "path": "ssh_host_ed25519_key.pub"},
        {"key": "gateway_blue-2026.10.pub", "path": "gateway_blue-2026.10.pub"},
    ]
    assert all(item["key"].endswith(".pub") for item in secret["items"])


def test_no_gateway_does_not_add_agent_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SSH_GATEWAY_PUBLIC_HOST_KEYS", raising=False)
    monkeypatch.delenv("AGENT_SSH_GATEWAY_HOST_KEY_SECRET", raising=False)
    manifest = _agent_manifest(AgentProvisioner(), "session", None)
    pod = manifest["spec"]
    agent = next(c for c in pod["containers"] if c["name"] == "agent")
    assert "SSH_GATEWAY_PUBLIC_HOST_KEYS" not in {e["name"] for e in agent["env"]}
    assert "ssh-gateway-host-keys" not in {m["name"] for m in agent["volumeMounts"]}
    assert "ssh-gateway-host-keys" not in {v["name"] for v in pod["volumes"]}


def test_old_chart_publication_paths_do_not_enable_agent_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A new image can construct its provisioner under a gateway-enabled old chart."""
    monkeypatch.setenv("SSH_GATEWAY_PUBLIC_HOST_KEYS", PUBLIC_KEYS)
    monkeypatch.delenv("AGENT_SSH_GATEWAY_HOST_KEY_SECRET", raising=False)
    provisioner = AgentProvisioner()
    manifest = _agent_manifest(provisioner, "session", None)
    pod = manifest["spec"]
    agent = next(c for c in pod["containers"] if c["name"] == "agent")
    assert "SSH_GATEWAY_PUBLIC_HOST_KEYS" not in {e["name"] for e in agent["env"]}
    assert "ssh-gateway-host-keys" not in {m["name"] for m in agent["volumeMounts"]}
    assert "ssh-gateway-host-keys" not in {v["name"] for v in pod["volumes"]}


def test_old_chart_new_image_import_keeps_orchestrator_bootable() -> None:
    """The module-level singleton must survive the old chart's publication env."""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(CHART.parent / "src")
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["SSH_GATEWAY_PUBLIC_HOST_KEYS"] = PUBLIC_KEYS
    env.pop("AGENT_SSH_GATEWAY_HOST_KEY_SECRET", None)
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from orchestrator.services.agent_provisioner import agent_provisioner; "
            "assert agent_provisioner._gateway_public_host_key_paths == (); "
            "print('legacy-publication-only')",
        ],
        cwd=CHART.parent,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "legacy-publication-only"


@pytest.mark.parametrize(
    "paths",
    [
        "",
        f"{HOST_DIR}/ssh_host_ed25519_key",
        f"{HOST_DIR}/../ssh_host_ed25519_key.pub",
        f"{HOST_DIR}/ssh_host_ed25519_key.pub.pub",
        f"{HOST_DIR}/ssh_host_rsa_key.pub",
        f"{HOST_DIR}/{'a' * 250}.pub",
        f"{HOST_DIR}/..rotation.pub",
        f"{HOST_DIR}/{'a' * 254}.pub",
        f"{HOST_DIR}/ssh_host_ed25519_key.pub,{HOST_DIR}/ssh_host_ed25519_key.pub",
        ",".join(f"{HOST_DIR}/ssh_host_ed25519_key_{i}.pub" for i in range(5)),
    ],
)
def test_dynamic_agent_rejects_invalid_public_key_lists(
    monkeypatch: pytest.MonkeyPatch, paths: str
) -> None:
    monkeypatch.setenv("AGENT_SSH_GATEWAY_HOST_KEY_SECRET", "srw-ssh-gateway-hostkey")
    monkeypatch.setenv("SSH_GATEWAY_PUBLIC_HOST_KEYS", paths)
    with pytest.raises(ValueError, match="SSH_GATEWAY_PUBLIC_HOST_KEYS"):
        AgentProvisioner()


def test_dynamic_agent_rejects_blank_secret_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AGENT_SSH_GATEWAY_HOST_KEY_SECRET", "  ")
    monkeypatch.setenv("SSH_GATEWAY_PUBLIC_HOST_KEYS", PUBLIC_KEYS)
    with pytest.raises(ValueError, match="SSH_GATEWAY_PUBLIC_HOST_KEYS"):
        AgentProvisioner()


@pytest.mark.parametrize(
    "names",
    [
        "{ssh_host_ed25519_key,ssh_host_ed25519_key}",
        "{ssh_host_ed25519_key,}",
        "{ssh_host_ed25519_key,ssh_host_ed25519_key_b,ssh_host_ed25519_key_c,ssh_host_ed25519_key_d,ssh_host_ed25519_key_e}",
        "{ssh_host_ed25519_key,../ssh_host_ed25519_key_b}",
        "{ssh_host_ed25519_key,other.pub}",
        "{" + "a" * 250 + "}",
        "{..rotation}",
    ],
)
def test_chart_rejects_invalid_public_projection_names(names: str) -> None:
    rendered = _chart("--set", f"sshGateway.hostKeyNames={names}")
    assert rendered.returncode != 0
    assert "sshGateway.hostKeyNames" in rendered.stderr


def test_public_key_suffix_fits_secret_key_length_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    basename = "a" * 249
    monkeypatch.setenv("AGENT_SSH_GATEWAY_HOST_KEY_SECRET", "srw-ssh-gateway-hostkey")
    monkeypatch.setenv("SSH_GATEWAY_PUBLIC_HOST_KEYS", f"{HOST_DIR}/{basename}.pub")
    manifest = _agent_manifest(AgentProvisioner(), "session", None)
    volume = next(
        v for v in manifest["spec"]["volumes"] if v["name"] == "ssh-gateway-host-keys"
    )
    assert volume["secret"]["items"][0]["key"] == basename + ".pub"
    rendered = _chart("--set", "sshGateway.hostKeyNames={" + basename + "}")
    assert rendered.returncode == 0, rendered.stderr
