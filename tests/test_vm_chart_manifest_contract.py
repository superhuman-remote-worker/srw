"""Rendered contracts for the VM topologies of the main chart."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
import re
import shutil
import subprocess

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
CONTROLLER_SRC = ROOT / "src/vm_controller/controller.py"
DAEMON_SRC = ROOT / "docker/agent-vm-base/files/management-daemon.py"
RELEASE_NAMESPACE = "lane-c-contract"
GUEST_ENV_PATH = "/etc/default/srw-guest"
AUTHORIZED_KEYS_PATH = "/etc/ssh/authorized_keys/agent-host"

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="helm binary not installed"
)


@dataclass(frozen=True)
class Chart:
    name: str
    path: str
    values: str

    def __str__(self) -> str:
        return self.name


MAIN = Chart("same-cluster", "helm", "helm/ci/vm-values.yaml")
MAIN_EXTERNAL = Chart("main-external", "helm", "helm/ci/vm-external-values.yaml")
MAIN_TEST = Chart("main-test", "helm", "helm/ci/test-values.yaml")
INSTALLER_ALIAS = Chart(
    "installer-alias", "helm", "helm/ci/installer-production-vms-values.yaml"
)


def attempt_render(
    chart: Chart,
    *overrides: str,
    api_versions: tuple[str, ...] = (),
    namespace: str = RELEASE_NAMESPACE,
) -> subprocess.CompletedProcess[str]:
    cmd = [
        "helm",
        "template",
        "t",
        str(ROOT / chart.path),
        "--namespace",
        namespace,
        "-f",
        str(ROOT / chart.values),
    ]
    for version in api_versions:
        cmd += ["--api-versions", version]
    for override in overrides:
        cmd += ["--set", override]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=120)


def render_chart(
    chart: Chart,
    *overrides: str,
    api_versions: tuple[str, ...] = (),
    namespace: str = RELEASE_NAMESPACE,
) -> str:
    result = attempt_render(
        chart, *overrides, api_versions=api_versions, namespace=namespace
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


def documents(rendered: str) -> list[dict]:
    return [doc for doc in yaml.safe_load_all(rendered) if isinstance(doc, dict)]


def vm_configmap(rendered: str) -> dict:
    return next(
        doc
        for doc in documents(rendered)
        if doc.get("kind") == "ConfigMap"
        and "vm-template.yaml" in (doc.get("data") or {})
    )


def vm_template(rendered: str) -> dict:
    return yaml.safe_load(vm_configmap(rendered)["data"]["vm-template.yaml"])


def cloud_init(rendered: str) -> str:
    configmap = vm_configmap(rendered)
    if "cloud-init.yaml" in configmap["data"]:
        return configmap["data"]["cloud-init.yaml"]
    vm = vm_template(rendered)
    for volume in vm["spec"]["template"]["spec"]["volumes"]:
        cloud = volume.get("cloudInitNoCloud")
        if cloud and "userData" in cloud:
            return cloud["userData"]
    raise AssertionError("no cloud-init template")


def write_files(user_data_text: str) -> dict[str, str]:
    parsed = yaml.safe_load(user_data_text)
    return {entry["path"]: entry.get("content", "") for entry in parsed["write_files"]}


def env_keys(content: str) -> set[str]:
    return set(re.findall(r"^\s*([A-Z_][A-Z0-9_]*)=", content, re.MULTILINE))


def placeholders(text: str) -> set[str]:
    return set(re.findall(r"\$\{([A-Z_][A-Z0-9_]*)\}", text))


def controller_replacements() -> set[str]:
    source = CONTROLLER_SRC.read_text()
    match = re.search(r"replacements\s*=\s*\{(.*?)\n\s*\}", source, re.S)
    assert match
    return set(re.findall(r'"\$\{([A-Z_][A-Z0-9_]*)\}"', match.group(1)))


def _env_key_of(node: ast.AST) -> str | None:
    for sub in ast.walk(node):
        if (
            isinstance(sub, ast.Call)
            and isinstance(sub.func, ast.Attribute)
            and sub.func.attr == "get"
            and sub.args
            and isinstance(sub.args[0], ast.Constant)
            and isinstance(sub.args[0].value, str)
        ):
            return sub.args[0].value
    return None


def daemon_required_env(transport: str) -> set[str]:
    """The env keys each guest transport hard-requires in ``load_config``.

    The daemon is dual-mode (HTTP for same-cluster, NATS for external), so the
    contract is stated per transport; the drift guard below fails if the daemon
    stops reading any of these keys.
    """

    contracts = {
        "http": {
            "ORCHESTRATOR_URL",
            "VM_AUTH_TOKEN",
            "ENTITY_ID",
            "ENTITY_TYPE",
            "JOB_ID",
            "VM_ID",
        },
        "nats": {"NATS_URL", "JOB_ID", "ORCHESTRATOR_ID"},
    }
    required = contracts[transport]
    source = DAEMON_SRC.read_text()
    missing = {key for key in required if f'os.environ.get("{key}")' not in source}
    assert not missing, (
        f"daemon no longer reads {sorted(missing)} — update the contract"
    )
    return set(required)


def _controller_docs(rendered: str) -> list[dict]:
    return [
        doc
        for doc in documents(rendered)
        if "vm-controller" in str((doc.get("metadata") or {}).get("name", ""))
    ]


@pytest.mark.parametrize(
    ("mode", "overrides", "expected"),
    [
        ("same-cluster", (), True),
        ("off", ("vm.mode=off",), False),
        (
            "external",
            (
                "vm.mode=external",
                "orchestratorId=external-test",
                "nats.url=nats://external:4222",
            ),
            False,
        ),
    ],
)
def test_mode_controls_same_cluster_object_set(
    mode: str, overrides: tuple[str, ...], expected: bool
) -> None:
    rendered = render_chart(MAIN, *overrides)
    controller_docs = _controller_docs(rendered)
    kinds = {doc["kind"] for doc in controller_docs}
    required = {
        "ConfigMap",
        "Deployment",
        "Service",
        "ServiceAccount",
        "Role",
        "RoleBinding",
    }
    if expected:
        assert required <= kinds
        for doc in controller_docs:
            if doc["kind"] in {"ClusterRole", "ClusterRoleBinding"}:
                assert "namespace" not in doc["metadata"]
            else:
                assert doc["metadata"].get("namespace") == RELEASE_NAMESPACE
    else:
        assert not controller_docs, f"{mode} unexpectedly rendered {kinds}"


@pytest.mark.parametrize("namespace", ("superhuman-remote-worker", "vm-runtime"))
def test_vm_controller_datavolume_source_cas_permission_is_namespaced(
    namespace: str,
) -> None:
    rendered = documents(render_chart(MAIN, namespace=namespace))
    role = next(
        doc for doc in rendered
        if doc.get("kind") == "Role"
        and doc["metadata"]["name"].endswith("-vm-controller")
    )
    assert role["metadata"]["namespace"] == namespace
    datavolume_rules = [
        rule for rule in role["rules"]
        if rule["apiGroups"] == ["cdi.kubevirt.io"]
        and rule["resources"] == ["datavolumes"]
    ]
    assert len(datavolume_rules) == 1
    assert set(datavolume_rules[0]["verbs"]) == {
        "get", "list", "create", "delete", "patch", "update",
    }
    binding = next(
        doc for doc in rendered
        if doc.get("kind") == "RoleBinding"
        and doc["metadata"]["name"] == role["metadata"]["name"]
    )
    assert binding["metadata"]["namespace"] == namespace
    assert binding["subjects"][0]["namespace"] == namespace
    assert all(
        "datavolumes" not in rule.get("resources", [])
        for doc in rendered if doc.get("kind") == "ClusterRole"
        for rule in doc["rules"]
    )


def test_same_cluster_controller_env_is_literal_and_http_only() -> None:
    deployment = next(
        doc
        for doc in _controller_docs(render_chart(MAIN))
        if doc["kind"] == "Deployment"
    )
    env = {
        item["name"]: item
        for item in deployment["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert env["TRANSPORT"]["value"] == "http"
    assert env["ORCHESTRATOR_URL"]["value"].endswith(
        f".{RELEASE_NAMESPACE}.svc.cluster.local:8085"
    )
    assert env["VM_DEFAULT_NETWORK_TIER"]["value"] == "internet-only"
    assert yaml.safe_load(env["VM_NODE_SELECTOR"]["value"]) == {
        "srw.io/vm-node": "true"
    }
    assert yaml.safe_load(env["VM_TOLERATIONS"]["value"]) == []
    assert (
        env["SSH_AUTHORIZED_KEY"]["valueFrom"]["secretKeyRef"]["key"] == "ssh-publickey"
    )
    assert "NATS_URL" not in env
    assert "HEADSCALE_URL" not in env


def test_orchestrator_mode_env_gating() -> None:
    same_docs = documents(render_chart(MAIN))
    config = next(
        doc
        for doc in same_docs
        if doc.get("kind") == "ConfigMap" and "VM_MODE" in (doc.get("data") or {})
    )["data"]
    assert config["VM_MODE"] == "same-cluster"
    assert "VM_CONTROLLER_URL" in config and "VM_NAMESPACE" in config
    assert "ORCHESTRATOR_ID" not in config
    assert "HEADSCALE_URL" not in config
    assert "NATS_URL" in config and "VM_PERSISTENT_ROOTDISK" in config

    deployment = next(
        doc
        for doc in same_docs
        if doc.get("kind") == "Deployment"
        and doc["metadata"]["name"].endswith("-orchestrator")
    )
    env = {
        item["name"]: item
        for item in deployment["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert {
        "VM_MODE",
        "VM_CONTROLLER_URL",
        "VM_NAMESPACE",
        "VM_LIFECYCLE_HMAC_SECRET",
    } <= env.keys()
    assert "VM_TEMPLATE_PATH" not in env
    assert "ORCHESTRATOR_ID" not in env


def test_same_cluster_vm_and_cloud_init_contract() -> None:
    rendered = render_chart(MAIN)
    vm = vm_template(rendered)
    spec = vm["spec"]["template"]["spec"]
    cloud_volume = next(
        item["cloudInitNoCloud"]
        for item in spec["volumes"]
        if "cloudInitNoCloud" in item
    )
    assert cloud_volume == {"secretRef": {"name": "agent-vm-${JOB_ID}-cloudinit"}}
    assert "userData" not in cloud_volume
    assert (
        vm["spec"]["template"]["metadata"]["labels"]["srw.io/network-tier"]
        == "${NETWORK_TIER}"
    )
    assert spec["readinessProbe"] == {
        "tcpSocket": {"port": 22},
        "initialDelaySeconds": 30,
        "periodSeconds": 5,
        "failureThreshold": 60,
    }
    assert "livenessProbe" not in spec

    files = write_files(cloud_init(rendered))
    assert set(env_keys(files[GUEST_ENV_PATH])) == daemon_required_env("http")
    assert set(files) == {
        GUEST_ENV_PATH,
        "/run/agent/job-config.json",
        AUTHORIZED_KEYS_PATH,
        "/usr/local/bin/srw-network-qualification",
    }
    assert "NATS_URL" not in cloud_init(rendered)
    assert "tailscale up" not in cloud_init(rendered)
    assert "rm -f /etc/ssh/ssh_host_*" not in cloud_init(rendered)
    assert "ssh-keygen -A" not in cloud_init(rendered)


def test_rootdisk_names_its_volume_mode() -> None:
    """The templated rootdisk must name volumeMode, never inherit it.

    CDI fills an unset volumeMode from the target StorageClass's StorageProfile.
    That profile is empty for ``rancher.io/local-path`` — which is why the
    omission stayed invisible — but populated for a real CSI, where it resolves
    to ``Block``. A Block rootdisk cannot be imported on a node running SELinux
    with the importer's capabilities dropped: it dies with "blockdev: cannot
    open /dev/cdi-block-volume: Permission denied". The controller hardcodes
    Filesystem for the golden DataVolume and the clone target, so an unset mode
    here left the DEFAULT path (goldenImage is off) as the only disagreeing one.
    """
    rendered = render_chart(MAIN)
    storage = vm_template(rendered)["spec"]["dataVolumeTemplates"][0]["spec"]["storage"]
    assert storage["volumeMode"] == "Filesystem"
    assert storage["accessModes"] == ["ReadWriteOnce"]

    # The template's mode must agree with the two paths the controller pins in
    # Python, or a golden clone and a templated disk would disagree at runtime.
    assert '"volumeMode": "Filesystem"' in CONTROLLER_SRC.read_text()


def test_same_cluster_network_policy_ports() -> None:
    policies = [
        doc
        for doc in documents(render_chart(MAIN))
        if doc.get("kind") == "NetworkPolicy"
        and "workspace-policy" in doc["metadata"]["name"]
    ]
    assert policies
    for policy in policies:
        ingress_ports = {
            port["port"]
            for rule in policy["spec"]["ingress"]
            for port in rule.get("ports", [])
        }
        egress_ports = {
            port["port"]
            for rule in policy["spec"]["egress"]
            for port in rule.get("ports", [])
        }
        assert {22, 30022, 9222} <= ingress_ports
        assert 8085 in egress_ports


@pytest.mark.parametrize("chart", [MAIN_EXTERNAL, MAIN_TEST], ids=str)
def test_external_workspace_policies_do_not_gain_same_cluster_ports(
    chart: Chart,
) -> None:
    policies = [
        doc
        for doc in documents(render_chart(chart))
        if doc.get("kind") == "NetworkPolicy"
        and "workspace-policy" in doc["metadata"]["name"]
    ]
    assert policies
    for policy in policies:
        ingress_ports = {
            port["port"]
            for rule in policy["spec"].get("ingress", [])
            for port in rule.get("ports", [])
        }
        egress_ports = {
            port["port"]
            for rule in policy["spec"].get("egress", [])
            for port in rule.get("ports", [])
        }
        # Port 22 remains in the pre-existing public egress rule for Git-over-SSH;
        # only the new VM ingress must be absent in external mode.
        assert 22 not in ingress_ports
        assert 8085 not in egress_ports


@pytest.mark.parametrize("chart", [MAIN_EXTERNAL, MAIN_TEST], ids=str)
def test_external_orchestrator_env_contract_is_pinned(chart: Chart) -> None:
    rendered = documents(render_chart(chart))
    config = next(
        doc
        for doc in rendered
        if doc.get("kind") == "ConfigMap" and "VM_MODE" in (doc.get("data") or {})
    )["data"]
    assert {"ORCHESTRATOR_ID", "HEADSCALE_URL", "AGENT_TAILSCALE_ENABLED"} <= set(
        config
    )
    assert {"VM_CONTROLLER_URL", "VM_NAMESPACE"}.isdisjoint(config)

    deployment = next(
        doc
        for doc in rendered
        if doc.get("kind") == "Deployment"
        and doc["metadata"]["name"].endswith("-orchestrator")
    )
    env = {
        item["name"]
        for item in deployment["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert {"ORCHESTRATOR_ID", "HEADSCALE_URL", "AGENT_TAILSCALE_ENABLED"} <= env
    assert {
        "VM_CONTROLLER_URL",
        "VM_NAMESPACE",
        "VM_LIFECYCLE_HMAC_SECRET",
    }.isdisjoint(env)


def test_preflight_job_only_renders_for_same_cluster() -> None:
    same = documents(render_chart(MAIN, "vm.preflight.enabled=true"))
    jobs = [
        doc
        for doc in same
        if doc.get("kind") == "Job" and "vm-preflight" in doc["metadata"]["name"]
    ]
    assert len(jobs) == 1
    assert jobs[0]["metadata"]["namespace"] == RELEASE_NAMESPACE
    pod_spec = jobs[0]["spec"]["template"]["spec"]
    container = pod_spec["containers"][0]
    image = container["image"]
    assert image == "registry.k8s.io/kubectl:v1.33.0"
    assert container["command"] == ["/bin/kubectl"], "distroless image: no shell"
    assert container["args"][:2] == ["get", "crd"]
    assert jobs[0]["metadata"]["name"].endswith(f"-{RELEASE_NAMESPACE}-vm-preflight")
    assert pod_spec["securityContext"] == {
        "runAsNonRoot": True,
        "runAsUser": 65532,
        "runAsGroup": 65532,
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    assert container["securityContext"] == {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
    }
    assert container["resources"]["requests"]
    assert container["resources"]["limits"]

    cluster_resources = [
        doc
        for doc in same
        if doc.get("kind") in {"ClusterRole", "ClusterRoleBinding"}
        and "vm-preflight" in doc["metadata"]["name"]
    ]
    assert {doc["metadata"]["name"] for doc in cluster_resources} == {
        jobs[0]["metadata"]["name"]
    }

    for mode in ("off", "external"):
        overrides = ["vm.preflight.enabled=true", f"vm.mode={mode}"]
        if mode == "external":
            overrides += ["orchestratorId=x", "nats.url=nats://external:4222"]
        assert "vm-preflight" not in render_chart(MAIN, *overrides)


def test_same_cluster_vmi_metering_defaults_to_release_authority() -> None:
    rendered = render_chart(
        MAIN,
        "vm.preflight.enabled=true",
        "infrastructureMetering.collectorEnabled=true",
        "infrastructureMetering.vmInventoryEnabled=true",
        "infrastructureMetering.stableClusterId=stable-main",
        "infrastructureMetering.vmIngestionSecretName=vmi-ingestion",
        "infrastructureMetering.networkPolicy.allowUnrestrictedEgress=true",
    )
    deployment = next(
        doc
        for doc in documents(rendered)
        if doc.get("kind") == "Deployment"
        and doc["metadata"]["name"].endswith("-vmi-metering")
    )
    env = {
        item["name"]: item.get("value")
        for item in deployment["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert deployment["metadata"]["namespace"] == RELEASE_NAMESPACE
    assert env["INFRASTRUCTURE_METERING_VM_STABLE_CLUSTER_ID"] == "stable-main"
    assert env["INFRASTRUCTURE_METERING_VM_NAMESPACE"] == RELEASE_NAMESPACE
    # Same-cluster collectors talk to this release's orchestrator Service; the
    # remote-cluster `orchestratorUrl` knob does not exist in this chart, and
    # an empty URL would leave the collector unable to ingest anything.
    assert (
        env["INFRASTRUCTURE_METERING_ORCHESTRATOR_URL"]
        == "http://t-superhuman-remote-worker-orchestrator:8085"
    )
    # Helm hands large YAML integers to templates as float64; without `int64`
    # the byte cap renders as 6.7108864e+07 and the collector refuses to start.
    assert env["INFRASTRUCTURE_METERING_MAX_SNAPSHOT_BYTES"] == "67108864"
    orchestrator_config = next(
        doc
        for doc in documents(rendered)
        if doc.get("kind") == "ConfigMap"
        and "INFRASTRUCTURE_METERING_VM_NAMESPACE" in (doc.get("data") or {})
    )["data"]
    assert (
        orchestrator_config["INFRASTRUCTURE_METERING_VM_STABLE_CLUSTER_ID"]
        == "stable-main"
    )
    assert (
        orchestrator_config["INFRASTRUCTURE_METERING_VM_NAMESPACE"] == RELEASE_NAMESPACE
    )


def test_same_cluster_storage_metering_targets_release_orchestrator() -> None:
    rendered = render_chart(
        MAIN,
        "vm.preflight.enabled=true",
        "infrastructureMetering.collectorEnabled=true",
        "infrastructureMetering.vmPvcInventoryEnabled=true",
        "infrastructureMetering.stableClusterId=stable-main",
        "infrastructureMetering.vmStorageIngestionSecretName=vm-storage-ingestion",
        "infrastructureMetering.networkPolicy.allowUnrestrictedEgress=true",
    )
    deployment = next(
        doc
        for doc in documents(rendered)
        if doc.get("kind") == "Deployment"
        and doc["metadata"]["name"].endswith("-storage-metering")
    )
    env = {
        item["name"]: item.get("value")
        for item in deployment["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert deployment["metadata"]["namespace"] == RELEASE_NAMESPACE
    assert (
        env["INFRASTRUCTURE_METERING_ORCHESTRATOR_URL"]
        == "http://t-superhuman-remote-worker-orchestrator:8085"
    )
    # Helm hands large YAML integers to templates as float64; without `int64`
    # the byte cap renders as 6.7108864e+07 and the collector refuses to start.
    assert env["INFRASTRUCTURE_METERING_MAX_SNAPSHOT_BYTES"] == "67108864"
    assert env["INFRASTRUCTURE_METERING_VM_NAMESPACE"] == RELEASE_NAMESPACE


def test_same_cluster_metering_rejects_double_pvc_inventory() -> None:
    result = attempt_render(
        MAIN,
        "infrastructureMetering.collectorEnabled=true",
        "infrastructureMetering.pvcInventoryEnabled=true",
        "infrastructureMetering.vmPvcInventoryEnabled=true",
        "infrastructureMetering.stableClusterId=stable-main",
        "infrastructureMetering.vmStorageIngestionSecretName=vm-storage-ingestion",
        "infrastructureMetering.networkPolicy.allowUnrestrictedEgress=true",
    )
    assert result.returncode != 0
    assert "vm-metering-pvc-double-inventory" in result.stderr


def test_every_same_cluster_lane_object_has_release_namespace() -> None:
    rendered = render_chart(
        MAIN,
        "infrastructureMetering.collectorEnabled=true",
        "infrastructureMetering.vmInventoryEnabled=true",
        "infrastructureMetering.vmPvcInventoryEnabled=true",
        "infrastructureMetering.stableClusterId=stable-main",
        "infrastructureMetering.vmIngestionSecretName=vmi-ingestion",
        "infrastructureMetering.vmStorageIngestionSecretName=storage-ingestion",
        "infrastructureMetering.networkPolicy.allowUnrestrictedEgress=true",
    )
    lane_tokens = (
        "vm-controller",
        "workspace-policy",
        "vmi-metering",
        "storage-metering",
        "infra-ingestion",
        "vm-preflight",
    )
    lane_docs = [
        doc
        for doc in documents(rendered)
        if any(
            token in str((doc.get("metadata") or {}).get("name", ""))
            for token in lane_tokens
        )
    ]
    assert lane_docs
    cluster_scoped = {"ClusterRole", "ClusterRoleBinding"}
    for doc in lane_docs:
        if doc["kind"] not in cluster_scoped:
            assert doc["metadata"].get("namespace") == RELEASE_NAMESPACE


def test_installer_alias_stays_unauthenticated_without_missing_secret_key() -> None:
    rendered = documents(render_chart(INSTALLER_ALIAS))
    deployments = [
        doc
        for doc in rendered
        if doc.get("kind") == "Deployment"
        and (
            doc["metadata"]["name"].endswith("-orchestrator")
            or doc["metadata"]["name"].endswith("-vm-controller")
        )
    ]
    assert len(deployments) == 2
    for deployment in deployments:
        env = {
            item["name"]
            for item in deployment["spec"]["template"]["spec"]["containers"][0]["env"]
        }
        assert "VM_LIFECYCLE_HMAC_SECRET" not in env

    role = next(
        doc
        for doc in rendered
        if doc.get("kind") == "Role"
        and doc["metadata"]["name"].endswith("-vm-controller")
    )
    lease_rule = next(
        rule for rule in role["rules"] if "leases" in rule.get("resources", [])
    )
    assert "create" in lease_rule["verbs"]
    # A captured teardown deletes the exact rootdisk PVC by UID precondition
    # after the DataVolume; without the verb every such teardown 403s.
    pvc_rule = next(
        rule
        for rule in role["rules"]
        if "persistentvolumeclaims" in rule.get("resources", [])
    )
    assert "delete" in pvc_rule["verbs"]
    notes = (ROOT / "helm/templates/NOTES.txt").read_text()
    assert "Guest tokens cannot be minted" in notes
    assert "Set vm.lifecycleAuthSecretName" in notes


def test_mode_off_with_external_knob_values_file_fails(tmp_path: Path) -> None:
    values = tmp_path / "legacy-external-knob.yaml"
    values.write_text(
        "license:\n"
        "  acceptTerms: true\n"
        "global:\n"
        "  domain: legacy.example.com\n"
        "orchestratorId: legacy-external\n",
        encoding="utf-8",
    )
    chart = Chart("off-external-knob", "helm", str(values))
    result = attempt_render(chart)
    assert result.returncode != 0
    assert "vm-mode-off-with-external-knobs" in result.stderr


@pytest.mark.parametrize(
    ("overrides", "reasons"),
    [
        (("vm.lifecycleAuthSecretName=",), ("vm-lifecycle-secret-required",)),
        (("vmController.namespace=agent-vms",), ("vm-release-namespace-required",)),
        (("agent.tailscale.enabled=true",), ("vm-tailscale-conflict",)),
        (("vmController.transport=nats",), ("vm-transport-alias-conflict",)),
        (("vmController.headscale.apiKeySecretName=x",), ("vm-headscale-conflict",)),
        (
            ("vm.mode=external", "vmController.enabled=true"),
            ("vm-mode-alias-conflict",),
        ),
        (
            ("vm.mode=external", "orchestratorId=", "nats.url=", "nats.internal=false"),
            ("vm-external-nats-required", "vm-external-orchestrator-id-required"),
        ),
    ],
)
def test_named_validation_reasons(
    overrides: tuple[str, ...], reasons: tuple[str, ...]
) -> None:
    result = attempt_render(MAIN, *overrides)
    assert result.returncode != 0
    for reason in reasons:
        assert reason in result.stderr


def test_capability_checks_are_opt_in() -> None:
    # Disable chart-generated random Secret values so byte comparison is stable.
    normal = render_chart(MAIN, "secrets.create=false", "searxng.enabled=false")
    with_versions = render_chart(
        MAIN,
        "secrets.create=false",
        "searxng.enabled=false",
        api_versions=("kubevirt.io/v1", "cdi.kubevirt.io/v1beta1"),
    )
    assert normal == with_versions

    missing = attempt_render(MAIN, "vm.preflight.renderTime=true")
    assert missing.returncode != 0
    assert "vm-kubevirt-crd-missing" in missing.stderr
    assert "vm-cdi-crd-missing" in missing.stderr

    assert (
        attempt_render(
            MAIN,
            "vm.preflight.renderTime=true",
            api_versions=("kubevirt.io/v1", "cdi.kubevirt.io/v1beta1"),
        ).returncode
        == 0
    )


def test_every_same_cluster_placeholder_is_substituted() -> None:
    rendered = render_chart(MAIN)
    emitted = placeholders(yaml.safe_dump(vm_template(rendered))) | placeholders(
        cloud_init(rendered)
    )
    assert not emitted - controller_replacements()


SUBSTITUTION_WIDTHS = {
    "DESCRIPTION": 200,
    "JOB_ID": 36,
    "ORCHESTRATOR_ID": 36,
    "OWNER_ID": 36,
    "OWNER_KIND": len("thread"),
    "NATS_URL": len("nats://nats.nats.svc.cluster.local:4222"),
    "HEADSCALE_URL": len("https://headscale.example.com"),
    "VM_IMAGE": 100,
    "SSH_AUTHORIZED_KEY": 100,
    "TAILSCALE_AUTH_KEY": 48,
    "AGENT_CONFIG": len("worker_base"),
    "VM_STORAGE_CLASS": len("local-path"),
    "VM_DISK_SIZE": len("20Gi"),
    "CPU_CORES": 2,
    "MEMORY": len("128Gi"),
    "VM_AUTH_TOKEN": 64,
    "ORCHESTRATOR_URL": 253,
    "NETWORK_TIER": 63,
}


@pytest.mark.parametrize(("chart", "limit"), [(MAIN, 16 * 1024)], ids=str)
def test_cloud_init_sanity_budget(chart: Chart, limit: int) -> None:
    text = cloud_init(render_chart(chart))
    for name, width in SUBSTITUTION_WIDTHS.items():
        text = text.replace(f"${{{name}}}", "x" * width)
    assert not placeholders(text)
    assert len(text.encode()) <= limit


def test_network_profile_is_default_off_and_same_policy_reaches_both_components() -> (
    None
):
    def environments(rendered: str) -> list[dict[str, str]]:
        return [
            {entry["name"]: entry.get("value") for entry in container.get("env", [])}
            for doc in documents(rendered)
            if doc.get("kind") == "Deployment"
            for container in doc["spec"]["template"]["spec"]["containers"]
            if any(
                item["name"] == "VM_NETWORK_PROFILE_ENABLED"
                for item in container.get("env", [])
            )
        ]

    default = environments(render_chart(MAIN))
    assert len(default) == 2
    assert all(
        env["VM_NETWORK_PROFILE_ENABLED"] == "false"
        and env["VM_NETWORK_PROFILE_IMAGE_ALLOWLIST"] == ""
        for env in default
    )
    image = "registry.example/srw-vm@sha256:" + "a" * 64
    selected = environments(
        render_chart(
            MAIN,
            "vmController.networkProfile.enabled=true",
            f"vmController.networkProfile.imageAllowlist[0]={image}",
        )
    )
    assert len(selected) == 2
    assert all(
        env["VM_NETWORK_PROFILE_ENABLED"] == "true"
        and env["VM_NETWORK_PROFILE_IMAGE_ALLOWLIST"] == image
        for env in selected
    )


def test_disposable_recovery_gate_receives_its_exact_pinned_vm_image() -> None:
    image = "registry.example/srw-vm@sha256:" + "a" * 64
    rendered = render_chart(
        MAIN,
        "orchestrator.vmWorkspaceRecovery.enabled=true",
        "orchestrator.vmWorkspaceRecovery.replacementEnabled=true",
        "orchestrator.vmWorkspaceRecoveryAcceptanceGate.enabled=true",
        "orchestrator.vmProvisioning.creationRetryEnabled=true",
        "vmController.networkProfile.enabled=true",
        f"vmController.networkProfile.imageAllowlist[0]={image}",
        f"vmController.defaultVmImage={image}",
    )
    orchestrator = next(
        doc
        for doc in documents(rendered)
        if doc.get("kind") == "Deployment"
        and doc["metadata"]["name"].endswith("orchestrator")
    )
    env = {
        item["name"]: item.get("value")
        for container in orchestrator["spec"]["template"]["spec"]["containers"]
        for item in container.get("env", [])
    }
    assert env["VM_CREATION_RETRY_ENABLED"] == "true"
    assert env["VM_NETWORK_PROFILE_ENABLED"] == "true"
    assert env["VM_NETWORK_PROFILE_IMAGE_ALLOWLIST"] == image
    assert env["VM_WORKSPACE_RECOVERY_GATE_VM_IMAGE"] == image
