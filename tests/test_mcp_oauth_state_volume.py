"""The MCP OAuth server keeps its state on a volume (G5).

FastMCP stores dynamically registered clients and in-flight authorizations
under ``FASTMCP_HOME``. The chart pointed it at the container's /tmp, so every
rollout (35 MCP image rebuilds in two weeks on dev) unregistered every
connected DCR client. Design:
knowledge-base/knowledge/features/knowledge_base_mcp_for_chat_uis.md G5.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm"

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="Helm is not installed"
)


def _render(*sets: str) -> dict[str, dict]:
    args = [
        "helm",
        "template",
        "srw",
        str(CHART),
        "-f",
        str(CHART / "ci/test-values.yaml"),
        "--show-only",
        "templates/mcp/deployment.yaml",
    ]
    for value in sets:
        args += ["--set", value]
    rendered = subprocess.run(args, capture_output=True, text=True, check=True)
    return {d["kind"]: d for d in yaml.safe_load_all(rendered.stdout) if d}


def _env(deployment) -> dict[str, str]:
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    return {e["name"]: e.get("value") for e in container["env"]}


def test_state_lives_on_a_kept_volume_by_default():
    docs = _render()
    deployment, claim = docs["Deployment"], docs["PersistentVolumeClaim"]
    pod = deployment["spec"]["template"]["spec"]
    container = pod["containers"][0]

    assert _env(deployment)["FASTMCP_HOME"] == "/var/lib/srw-mcp/fastmcp"
    assert container["volumeMounts"] == [
        {"name": "oauth-state", "mountPath": "/var/lib/srw-mcp"}
    ]
    assert (
        pod["volumes"][0]["persistentVolumeClaim"]["claimName"]
        == (claim["metadata"]["name"])
    )
    assert claim["spec"]["accessModes"] == ["ReadWriteOnce"]
    assert claim["metadata"]["annotations"]["helm.sh/resource-policy"] == "keep"
    # ReadWriteOnce: the old pod must release the volume before the new one starts.
    assert deployment["spec"]["strategy"]["type"] == "Recreate"


def test_the_volume_is_group_owned_by_the_images_user():
    pod = _render()["Deployment"]["spec"]["template"]["spec"]
    dockerfile = (ROOT / "docker" / "Dockerfile.mcp").read_text()

    assert pod["securityContext"]["fsGroup"] == 10001
    assert "groupadd -r -g 10001 srw && useradd -r -u 10001 -g srw srw" in dockerfile
    assert (
        "groupadd -r -g 10001 srw"
        in (ROOT / "docker" / "Dockerfile.mcp.dev").read_text()
    )


def test_storage_class_is_passed_through():
    claim = _render("mcp.persistence.storageClass=longhorn")["PersistentVolumeClaim"]

    assert claim["spec"]["storageClassName"] == "longhorn"


def test_disabling_persistence_restores_the_ephemeral_layout():
    docs = _render("mcp.persistence.enabled=false")
    deployment = docs["Deployment"]

    assert "PersistentVolumeClaim" not in docs
    assert "FASTMCP_HOME" not in _env(deployment)
    assert "strategy" not in deployment["spec"]
    assert "volumes" not in deployment["spec"]["template"]["spec"]


def test_more_than_one_replica_with_the_volume_is_refused_at_render():
    with pytest.raises(subprocess.CalledProcessError) as exc:
        _render("mcp.replicas=2")

    assert "mcp.persistence.enabled needs mcp.replicas=1" in exc.value.stderr


def test_default_claim_is_big_enough_for_longhorn_xfs():
    """Longhorn refuses XFS volumes under 300Mi; the 256Mi claim never bound
    and kept the MCP pod Pending on main dev (2026-11-05)."""
    claim = _render()["PersistentVolumeClaim"]
    size = claim["spec"]["resources"]["requests"]["storage"]
    assert size.endswith("Mi") and int(size[:-2]) >= 300
    # A Pending claim's size cannot be changed, so the resized claim is new.
    assert claim["metadata"]["name"].endswith("-mcp-oauth-state")
