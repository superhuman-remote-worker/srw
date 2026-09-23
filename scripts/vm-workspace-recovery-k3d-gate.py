#!/usr/bin/env python3
"""Run the retained-disk VM recovery acceptance gate on an owned k3d cluster.

The default ``--preflight`` mode is read-only and prints ``SKIPPED`` when the
host cannot run a real KubeVirt/CDI/Longhorn fixture. ``--run`` creates a new
cluster whose name must start with ``srw-vm-recovery-gate-``, installs the
pinned substrate, deploys this checkout, and invokes the source-controlled
scenario adapter. The
driver must exercise the application and return API/DB evidence; this wrapper
then validates every required invariant before it can print ``PASS``. A Pod
mock or a successful command exit is never accepted as recovery evidence.

Example (destructive only to the cluster this command creates)::

    python scripts/vm-workspace-recovery-k3d-gate.py --preflight \
      --values-file /path/to/disposable-values.yaml \
      --guest-image registry.example/srw-vm@sha256:<digest> \
      --container-engine docker \
      --scenario-driver ./scripts/vm-workspace-recovery-scenario.py

    python scripts/vm-workspace-recovery-k3d-gate.py --run \
      --cluster-name srw-vm-recovery-gate-$(date +%s) \
      --values-file /path/to/disposable-values.yaml \
      --guest-image registry.example/srw-vm@sha256:<digest> \
      --container-engine docker \
      --scenario-driver ./scripts/vm-workspace-recovery-scenario.py

The scenario driver receives ``--kube-context``, ``--namespace`` and
``--output``. Its output JSON must contain the response-loss, leader-overlap,
slow-boot, deadline, stop-evidence and retained-disk fields validated below.
It is responsible for collecting those values from the live Kubernetes API,
the application API and PostgreSQL, and for recording application/controller/
guest revisions. This wrapper independently verifies the substrate and cluster
UID, then rejects incomplete or contradictory evidence.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from typing import Any, Callable, Mapping, NamedTuple, Sequence


ROOT = Path(__file__).resolve().parents[1]
CLUSTER_PREFIX = "srw-vm-recovery-gate-"
# KubeVirt v1.6 supports Kubernetes 1.31-1.33, matching DEFAULT_K3S_IMAGE.
# https://kubevirt.io/user-guide/release_notes/#v160
DEFAULT_KUBEVIRT_VERSION = "v1.6.6"
DEFAULT_CDI_VERSION = "v1.66.0"
DEFAULT_LONGHORN_VERSION = "1.10.1"
DEFAULT_NAMESPACE = "srw"
DEFAULT_K3S_IMAGE = "rancher/k3s:v1.31.5-k3s1"
DEFAULT_SCENARIO_DRIVER = ROOT / "scripts/vm-workspace-recovery-scenario.py"


class GateFailure(RuntimeError):
    """The real acceptance evidence failed a required invariant."""


class GateSkip(RuntimeError):
    """A live disposable run lacks a declared scenario capability."""


class CapabilityReport(NamedTuple):
    ready: bool
    missing: tuple[str, ...]


def require(condition: bool, message: str) -> None:
    if not condition:
        raise GateFailure(message)


def require_disposable_cluster_name(name: str) -> None:
    require(
        bool(re.fullmatch(r"srw-vm-recovery-gate-[a-z0-9][a-z0-9-]{2,39}", name)),
        "refusing destructive gate: cluster name is not a unique "
        f"{CLUSTER_PREFIX}<suffix> name",
    )


def host_preflight(
    *,
    values_file: Path | None,
    scenario_driver: Path | None = DEFAULT_SCENARIO_DRIVER,
    guest_image: str | None = None,
    container_engine: str = "docker",
    which: Callable[[str], str | None] = shutil.which,
    device_exists: Callable[[str], bool] = lambda path: Path(path).exists(),
    command: Callable[..., Any] | None = None,
) -> CapabilityReport:
    """Inspect only host files/binaries; never contact or mutate a cluster."""

    # ``command`` is accepted for tests/callers that share runner injection.
    # It is intentionally unused: preflight is safe even with hostile kubeconfig.
    del command
    missing = [
        f"binary:{name}"
        for name in (
            "k3d",
            "kubectl",
            "helm",
            "ssh-keygen",
            "docker",
            "git",
            "iscsiadm",
            "longhornctl",
        )
        if which(name) is None
    ]
    if container_engine != "docker":
        missing.append("container-engine:k3d-docker-required")
    for device in ("/dev/kvm", "/dev/vhost-net", "/dev/net/tun"):
        if not device_exists(device):
            missing.append(f"device:{device}")
    if not device_exists("/run/iscsid/socket"):
        missing.append("socket:/run/iscsid/socket")
    if not device_exists("/sys/module/iscsi_tcp"):
        missing.append("module:iscsi_tcp")
    if values_file is None or not values_file.is_file():
        missing.append("file:disposable-values")
    if (
        scenario_driver is None
        or not scenario_driver.is_file()
        or not os.access(scenario_driver, os.X_OK)
    ):
        missing.append("executable:scenario-driver")
    if not re.fullmatch(r"[^\s@]+@sha256:[a-f0-9]{64}", str(guest_image or "")):
        missing.append("image:guest-digest")
    return CapabilityReport(not missing, tuple(missing))


def classify_stop_evidence(
    *, forced_deletion: bool, exact_termination_receipt: bool
) -> dict[str, Any]:
    """Mirror the gate's acceptance rule without treating absence as proof."""

    if forced_deletion or not exact_termination_receipt:
        return {
            "state": "paused_attention",
            "reason_code": "prior_runtime_unfenced",
            "successor_dispatched": False,
        }
    return {"state": "eligible", "reason_code": None, "successor_dispatched": False}


def _section(evidence: dict[str, Any], name: str) -> dict[str, Any]:
    value = evidence.get(name)
    require(isinstance(value, dict), f"evidence section {name!r} is absent")
    return value


def validate_acceptance_evidence(evidence: dict[str, Any]) -> None:
    """Reject any result that does not prove the supported-case matrix."""

    authorities = _section(evidence, "authority_evidence")
    application_authority = _section(authorities, "application_api")
    kubernetes_authority = _section(authorities, "kubernetes_api")
    postgres_authority = _section(authorities, "postgresql")
    require(
        application_authority.get("status_code") == 200
        and application_authority.get("job_visible") is True,
        "application API did not prove the fixture",
    )
    for key in ("vm_uid", "pvc_uid"):
        require(
            bool(
                re.fullmatch(r"[a-f0-9-]{36}", str(kubernetes_authority.get(key) or ""))
            ),
            f"Kubernetes authority did not provide {key}",
        )
    require(
        bool(str(postgres_authority.get("database") or ""))
        and isinstance(postgres_authority.get("server_version_num"), int),
        "PostgreSQL authority evidence is absent",
    )
    driver = _section(evidence, "driver")
    require(driver.get("protocol_version") == 1, "scenario driver protocol mismatch")
    require(
        driver.get("live_sources") == sorted(authorities),
        "scenario evidence was not collected from every live authority",
    )
    require(
        driver.get("scenarios")
        == [
            "response_loss",
            "leader_overlap",
            "slow_boot",
            "deadline",
            "missing_stop_evidence",
            "forced_deletion",
            "replacement",
        ],
        "scenario adapter did not execute the complete supported-case matrix",
    )

    substrate = _section(evidence, "substrate")
    for key in (
        "cluster_created_by_gate",
        "kubernetes",
        "kubevirt",
        "cdi",
        "longhorn",
        "longhorn_workloads_ready",
        "longhorn_nodes_ready",
        "longhorn_rwo_retain_test",
    ):
        require(substrate.get(key) is True, f"real substrate evidence missing: {key}")

    response_loss = _section(evidence, "response_loss")
    require(
        response_loss.get("fault_injection") == "committed_response_replay",
        "response-loss fault was not injected at the committed receipt boundary",
    )
    require(
        response_loss.get("recovery_rows") == 1, "response loss duplicated recovery"
    )
    require(
        response_loss.get("request_receipts") == 1, "response loss lost request receipt"
    )
    require(
        response_loss.get("terminal_reports") == 0,
        "response loss reported terminal failure",
    )
    before = response_loss.get("queue_token_before")
    after = response_loss.get("queue_token_after")
    require(
        isinstance(before, int) and isinstance(after, int) and after == before + 1,
        "response loss did not produce one monotonic queue fence",
    )

    overlap = _section(evidence, "leader_overlap")
    require(
        overlap.get("fault_injection") == "controlled_leader_handoff",
        "leader-overlap fault did not transfer a live reconciler claim",
    )
    require(
        overlap.get("active_operations") == 1, "leader overlap duplicated operation"
    )
    require(
        overlap.get("leader_instances") == 2,
        "leader overlap did not exercise two reconciler instances",
    )
    leader_a_identity = overlap.get("leader_a_identity")
    leader_b_identity = overlap.get("leader_b_identity")
    require(
        isinstance(leader_a_identity, str)
        and isinstance(leader_b_identity, str)
        and leader_a_identity.startswith("gate-leader-a:")
        and leader_b_identity.startswith("gate-leader-b:")
        and leader_a_identity != leader_b_identity,
        "leader overlap did not prove distinct leader identities",
    )
    leader_a_backend_pid = overlap.get("leader_a_backend_pid")
    leader_b_backend_pid = overlap.get("leader_b_backend_pid")
    require(
        isinstance(leader_a_backend_pid, int)
        and leader_a_backend_pid > 0
        and isinstance(leader_b_backend_pid, int)
        and leader_b_backend_pid > 0
        and leader_a_backend_pid != leader_b_backend_pid,
        "leader overlap did not prove distinct PostgreSQL sessions",
    )
    require(
        overlap.get("leadership_transfer_succeeded") is True,
        "leader overlap did not cross the advisory-lock handoff boundary",
    )
    require(
        overlap.get("deadline_preserved") is True, "leader overlap extended deadline"
    )
    require(
        overlap.get("configured_global_probe_limit") == 4,
        "scenario did not use the rendered global probe budget",
    )
    require(
        isinstance(overlap.get("max_global_probes"), int)
        and overlap["max_global_probes"] <= overlap["configured_global_probe_limit"],
        "global probe budget exceeded",
    )
    require(
        overlap.get("configured_node_probe_limit") == 1,
        "scenario did not use the rendered per-node probe budget",
    )
    require(
        isinstance(overlap.get("max_node_probes"), int)
        and overlap["max_node_probes"] <= overlap["configured_node_probe_limit"],
        "per-node probe budget exceeded",
    )
    require(
        overlap.get("stale_probe_finished_after_handoff") is True,
        "stale leader probe did not finish after the handoff",
    )
    stale_store_boundary = overlap.get("stale_store_boundary")
    require(
        stale_store_boundary == "claim_is_current"
        and overlap.get("stale_store_boundary_rejected") is True,
        "stale leader result did not fail the production claim-current fence",
    )
    require(
        overlap.get("stale_stage_attempted") is False,
        "stale leader staging evidence contradicts its rejected boundary",
    )
    require(
        overlap.get("stale_result_rejected") is True,
        "stale leader probe result was not rejected",
    )
    require(
        overlap.get("successor_dispatches") == 1,
        "leader handoff did not produce exactly one successor dispatch",
    )

    slow = _section(evidence, "slow_boot")
    require(slow.get("state") == "recovered", "slow boot did not recover")
    require(slow.get("attested") is True, "slow boot resumed without attestation")
    require(
        isinstance(slow.get("age_seconds"), (int, float))
        and isinstance(slow.get("injected_delay_seconds"), int)
        and slow["injected_delay_seconds"] > 0
        and slow["injected_delay_seconds"] <= slow["age_seconds"] < 900,
        "slow boot did not stay inside the immutable deadline",
    )
    require(
        slow.get("executor_occupied_while_waiting") is False,
        "slow boot retained a stateless executor slot",
    )

    deadline = _section(evidence, "deadline")
    require(
        deadline.get("fault_injection") == "live_claim_deadline_barrier",
        "deadline fault did not block a live claimed observation",
    )
    require(deadline.get("state") == "paused_attention", "deadline did not pause")
    require(
        deadline.get("reason_code") == "workspace_recovery_deadline_exceeded",
        "deadline pause has the wrong reason",
    )
    require(
        deadline.get("probe_started_before_deadline") is True,
        "deadline probe did not start before the immutable deadline",
    )
    require(
        deadline.get("probe_finished_after_deadline") is True,
        "deadline probe did not finish after the immutable deadline",
    )
    require(
        deadline.get("precondition_check_rejected") is True,
        "deadline-crossing observation passed the production precondition check",
    )
    require(
        deadline.get("stage_observation_attempted") is False,
        "deadline-crossing observation reached the later staging CAS",
    )
    require(
        deadline.get("release_attempted") is False,
        "deadline-crossing observation reached the release CAS",
    )
    require(
        deadline.get("final_release_succeeded") is False,
        "deadline-crossing probe released participants",
    )
    require(
        deadline.get("queue_still_parked") is True,
        "deadline-crossing probe removed the queue hold",
    )
    require(
        deadline.get("successor_dispatches") == 0,
        "deadline-crossing probe dispatched a successor",
    )
    require(deadline.get("disk_retained") is True, "deadline deleted retained disk")
    require(
        deadline.get("checkpoint_retained") is True,
        "deadline pruned the retained checkpoint",
    )
    require(deadline.get("late_probe_released") is False, "late probe released work")

    for name in ("missing_stop_evidence", "forced_deletion"):
        stop = _section(evidence, name)
        require(
            stop
            == classify_stop_evidence(
                forced_deletion=name == "forced_deletion",
                exact_termination_receipt=False,
            ),
            f"{name.replace('_', ' ')} did not remain pause-only",
        )

    replacement = _section(evidence, "replacement")
    require(
        replacement.get("state") == "recovered", "valid replacement did not recover"
    )
    for name in ("pvc_uid", "marker", "checkpoint"):
        old = replacement.get(f"{name}_before")
        new = replacement.get(f"{name}_after")
        require(bool(old) and old == new, f"replacement did not preserve {name}")
    for key in (
        "pin_acknowledged",
        "trusted_stop_receipt",
        "pinned_identity",
        "network_qualified",
        "ssh_host_fingerprint_pinned",
        "longhorn_volume_healthy",
    ):
        require(replacement.get(key) is True, f"replacement evidence missing: {key}")
    require(
        replacement.get("root_pv_csi_driver") == "driver.longhorn.io",
        "replacement root PVC was not backed by Longhorn CSI",
    )
    require(
        replacement.get("successor_dispatches") == 1,
        "replacement did not dispatch exactly one successor",
    )
    receipt = replacement.get("stop_receipt")
    require(isinstance(receipt, dict), "full stop receipt evidence is absent")
    require(
        bool(re.fullmatch(r"[a-f0-9-]{36}", str(receipt.get("id") or "")))
        and bool(
            re.fullmatch(
                r"sha256:[a-f0-9]{64}", str(receipt.get("evidence_digest") or "")
            )
        )
        and all(
            receipt.get(key)
            for key in ("vm_uid", "vmi_uid", "launcher_uid", "root_pvc_uid")
        ),
        "exact stop receipt identity is incomplete",
    )
    resume_receipt = replacement.get("resume_receipt")
    require(
        isinstance(resume_receipt, dict)
        and resume_receipt.get("kind") == "vm_workspace_recovery"
        and isinstance(resume_receipt.get("claim_token"), int)
        and isinstance(resume_receipt.get("successor"), dict),
        "full resume receipt evidence is absent",
    )
    retention_pin = replacement.get("retention_pin")
    require(
        isinstance(retention_pin, dict)
        and bool(retention_pin.get("controller_pin_uid"))
        and bool(retention_pin.get("controller_pin_resource_version"))
        and retention_pin.get("controller_state") in {"active", "released"},
        "controller retention-pin acknowledgement is incomplete",
    )

    revisions = _section(evidence, "revisions")
    for key in ("application", "controller", "guest_image"):
        require(bool(str(revisions.get(key) or "").strip()), f"missing {key} revision")


class Shell:
    def __init__(self, *, kubeconfig: Path | None = None) -> None:
        self.kubeconfig = kubeconfig

    def run(
        self,
        args: Sequence[str],
        *,
        data: str | None = None,
        timeout: float | None = None,
        check: bool = True,
    ) -> str:
        environ = os.environ.copy()
        if self.kubeconfig is not None:
            environ["KUBECONFIG"] = str(self.kubeconfig)
        result = subprocess.run(
            list(args),
            input=data,
            text=True,
            capture_output=True,
            timeout=timeout,
            env=environ,
        )
        if check and result.returncode:
            # Arguments can contain credentials or signed payloads. Keep the
            # error useful without copying argv/stdout into CI logs.
            raise GateFailure(f"gate command failed with exit {result.returncode}")
        return result.stdout.strip()


def _containerd_tag(image: str) -> str:
    first = image.split("/", maxsplit=1)[0]
    if "/" not in image:
        return f"docker.io/library/{image}"
    if not any(marker in first for marker in (".", ":")) and first != "localhost":
        return f"docker.io/{image}"
    return image


def image_id_matches_config_digest(image_id: object, config_digest: object) -> bool:
    """Compare a Pod runtime image ID with the imported Docker config digest."""

    expected = str(config_digest or "")
    match = re.search(r"sha256:[a-f0-9]{64}$", str(image_id or ""))
    return match is not None and match.group(0) == expected


def _archive_config_ids(archive: Path) -> dict[str, str]:
    """Read immutable config IDs from the exact platform-pruned import tar."""

    require(archive.is_file() and not archive.is_symlink(), "image archive is absent")
    try:
        with tarfile.open(archive, mode="r:*") as stream:
            member = stream.getmember("manifest.json")
            require(member.isfile(), "image archive manifest is not a file")
            source = stream.extractfile(member)
            require(source is not None, "image archive manifest is unreadable")
            document = json.loads(source.read().decode("utf-8"))
    except (
        KeyError,
        OSError,
        tarfile.TarError,
        UnicodeError,
        json.JSONDecodeError,
    ) as exc:
        raise GateFailure("image archive manifest is invalid") from exc
    require(isinstance(document, list) and document, "image archive manifest is empty")
    result: dict[str, str] = {}
    for item in document:
        require(isinstance(item, dict), "image archive manifest item is invalid")
        config = item.get("Config")
        tags = item.get("RepoTags")
        match = re.fullmatch(
            r"(?:blobs/sha256/)?([0-9a-f]{64})(?:\.json)?", str(config or "")
        )
        require(
            match is not None and isinstance(tags, list),
            "image archive identity is invalid",
        )
        image_id = "sha256:" + match.group(1)
        for tag in tags:
            require(isinstance(tag, str) and tag, "image archive tag is invalid")
            result[_containerd_tag(tag)] = image_id
    return result


def build_and_import_checkout_images(
    shell: Shell, *, cluster_name: str, archive_dir: Path
) -> dict[str, str]:
    """Build this checkout and prove each config ID on every k3d node."""

    revision = shell.run(["git", "rev-parse", "HEAD"])
    require(bool(re.fullmatch(r"[0-9a-f]{40,64}", revision)), "git revision is invalid")
    suffix = f"{revision[:12]}-{cluster_name[-8:]}"
    images = {
        "orchestrator": f"srw-vm-recovery-orchestrator:{suffix}",
        "agent": f"srw-vm-recovery-agent:{suffix}",
        "vm_controller": f"srw-vm-recovery-controller:{suffix}",
    }
    dockerfiles = {
        "orchestrator": "docker/Dockerfile.orchestrator",
        "agent": "docker/Dockerfile.agent",
        "vm_controller": "docker/Dockerfile.vm-controller",
    }
    platform = shell.run(
        ["docker", "version", "--format", "{{.Server.Os}}/{{.Server.Arch}}"]
    )
    require(
        bool(re.fullmatch(r"linux/[a-z0-9][a-z0-9_.-]*", platform)),
        "Docker server platform is unsupported",
    )
    for component, image in images.items():
        command = [
            "docker",
            "build",
            "--platform",
            platform,
            "--pull=false",
            "--label",
            f"srw.io/vm-recovery-gate={cluster_name}",
            "--label",
            f"srw.io/source-revision={revision}",
            "--build-arg",
            f"SRW_SOURCE_REVISION={revision}",
            "--build-arg",
            f"SRW_RELEASE_VERSION=vm-recovery-gate-{suffix}",
        ]
        if component == "agent":
            command.extend(["--build-arg", f"BUILD_SHA={revision}"])
        command.extend(["-f", dockerfiles[component], "-t", image, "."])
        shell.run(command, timeout=1800)
        identity = shell.run(
            [
                "docker",
                "image",
                "inspect",
                "--format",
                '{{.Id}}|{{ index .Config.Labels "srw.io/vm-recovery-gate" }}|'
                '{{ index .Config.Labels "srw.io/source-revision" }}',
                image,
            ]
        ).split("|")
        require(
            len(identity) == 3
            and bool(re.fullmatch(r"sha256:[0-9a-f]{64}", identity[0]))
            and identity[1:] == [cluster_name, revision],
            f"{component} image ownership proof failed",
        )
    archive = archive_dir / "checkout-images.tar"
    shell.run(
        [
            "docker",
            "image",
            "save",
            "--platform",
            platform,
            "--output",
            str(archive),
            *images.values(),
        ],
        timeout=1800,
    )
    expected = _archive_config_ids(archive)
    require(
        set(expected) == {_containerd_tag(image) for image in images.values()},
        "checkout image archive contains unexpected tags",
    )
    shell.run(
        [
            "k3d",
            "image",
            "import",
            str(archive),
            "--cluster",
            cluster_name,
            "--mode",
            "direct",
        ],
        timeout=1800,
    )
    for role in ("server-0", "agent-0"):
        node = f"k3d-{cluster_name}-{role}"
        inventory = json.loads(
            shell.run(["docker", "exec", node, "crictl", "images", "-o", "json"])
        )
        actual = {
            tag: item.get("id")
            for item in inventory.get("images", [])
            if isinstance(item, dict)
            for tag in item.get("repoTags", [])
            if isinstance(tag, str)
        }
        require(
            all(actual.get(tag) == image_id for tag, image_id in expected.items()),
            f"{role} did not import the exact checkout image IDs",
        )
    for component, image in tuple(images.items()):
        images[f"{component}_config_id"] = expected[_containerd_tag(image)]
    images["source_revision"] = revision
    return images


def _kubectl(context: str, *args: str) -> list[str]:
    return ["kubectl", "--context", context, *args]


def _wait(shell: Shell, context: str, *args: str, timeout: int = 900) -> None:
    shell.run(_kubectl(context, *args), timeout=timeout + 30)


def create_cluster(shell: Shell, name: str) -> str:
    require_disposable_cluster_name(name)
    listed = json.loads(shell.run(["k3d", "cluster", "list", "-o", "json"]) or "[]")
    require(
        all(item.get("name") != name for item in listed),
        "refusing to reuse an existing cluster; ownership would be ambiguous",
    )
    shell.run(
        [
            "k3d",
            "cluster",
            "create",
            name,
            "--image",
            DEFAULT_K3S_IMAGE,
            "--servers",
            "1",
            "--agents",
            "1",
            "--wait",
            "--timeout",
            "180s",
            "--registry-create",
            f"{name}-registry:0.0.0.0:0",
            "--runtime-label",
            f"srw.io/vm-recovery-gate={name}@all",
            "--volume",
            "/dev/kvm:/dev/kvm@all",
            "--volume",
            "/dev/vhost-net:/dev/vhost-net@all",
            "--volume",
            "/dev/net/tun:/dev/net/tun@all",
            "--volume",
            "/run/iscsid/socket:/run/iscsid/socket@all",
            "--volume",
            "/etc/iscsi:/etc/iscsi@all",
            "--volume",
            "/lib/modules:/lib/modules@all",
        ],
        timeout=300,
    )
    return f"k3d-{name}"


def capture_cluster_ownership(shell: Shell, name: str) -> tuple[str, ...]:
    """Capture the runtime-labeled node container IDs created by this gate."""

    require_disposable_cluster_name(name)
    output = shell.run(
        [
            "docker",
            "ps",
            "-a",
            "--filter",
            f"label=srw.io/vm-recovery-gate={name}",
            "--format",
            "{{.ID}}",
        ]
    )
    identifiers = tuple(
        sorted(line.strip() for line in output.splitlines() if line.strip())
    )
    require(len(identifiers) >= 2, "gate cluster ownership labels are incomplete")
    return identifiers


def delete_owned_cluster(
    shell: Shell, name: str, *, ownership: tuple[str, ...]
) -> None:
    """Delete only while the gate's original labeled node set is unchanged."""

    current = capture_cluster_ownership(shell, name)
    require(current == ownership, "gate cluster ownership changed; refusing deletion")
    shell.run(["k3d", "cluster", "delete", name], timeout=300)


def write_private_kubeconfig(shell: Shell, name: str, path: Path) -> None:
    """Bind all kubectl/Helm work to this gate's private cluster config."""

    document = shell.run(["k3d", "kubeconfig", "get", name])
    require("current-context:" in document, "k3d returned an invalid kubeconfig")
    path.write_text(document + "\n", encoding="utf-8")
    path.chmod(0o600)
    shell.kubeconfig = path


def run_longhorn_preflight(
    shell: Shell, *, context: str, kubeconfig: Path, version: str
) -> None:
    """Run Longhorn's own pinned node preflight before installing its chart."""

    shell.run(_kubectl(context, "create", "namespace", "longhorn-system"), check=False)
    try:
        shell.run(
            [
                "longhornctl",
                "check",
                "preflight",
                "--kubeconfig",
                str(kubeconfig),
                "--namespace",
                "longhorn-system",
                "--image",
                f"longhornio/longhorn-cli:v{version}",
            ],
            timeout=600,
        )
    except GateFailure as exc:
        raise GateSkip("longhornctl:node-preflight") from exc


def require_longhorn_node_prerequisites(cluster_name: str) -> None:
    """Refuse storage claims until every owned node proves host prerequisites."""

    probe = (
        "command -v iscsiadm >/dev/null && "
        "test -S /run/iscsid/socket && "
        "grep -qw iscsi_tcp /proc/modules && "
        "findmnt -n -o PROPAGATION /var/lib/kubelet | grep -Eq 'shared|rshared'"
    )
    for role in ("server-0", "agent-0"):
        node = f"k3d-{cluster_name}-{role}"
        result = subprocess.run(
            ["docker", "exec", node, "sh", "-ceu", probe],
            text=True,
            capture_output=True,
            timeout=30,
        )
        if result.returncode:
            raise GateSkip(f"node:{role}:longhorn-prerequisites")


def install_substrate(
    shell: Shell,
    *,
    context: str,
    kubevirt_version: str,
    cdi_version: str,
    longhorn_version: str,
) -> None:
    kv = f"https://github.com/kubevirt/kubevirt/releases/download/{kubevirt_version}"
    cdi = f"https://github.com/kubevirt/containerized-data-importer/releases/download/{cdi_version}"
    shell.run(
        _kubectl(context, "apply", "-f", f"{kv}/kubevirt-operator.yaml"), timeout=300
    )
    shell.run(_kubectl(context, "apply", "-f", f"{kv}/kubevirt-cr.yaml"), timeout=300)
    _wait(
        shell,
        context,
        "-n",
        "kubevirt",
        "wait",
        "kubevirt/kubevirt",
        "--for=condition=Available",
        "--timeout=12m",
        timeout=780,
    )
    shell.run(
        _kubectl(context, "apply", "--server-side", "-f", f"{cdi}/cdi-operator.yaml"),
        timeout=300,
    )
    shell.run(
        _kubectl(context, "apply", "--server-side", "-f", f"{cdi}/cdi-cr.yaml"),
        timeout=300,
    )
    _wait(
        shell,
        context,
        "wait",
        "cdi/cdi",
        "--for=condition=Available",
        "--timeout=8m",
        timeout=540,
    )
    shell.run(
        [
            "helm",
            "repo",
            "add",
            "longhorn",
            "https://charts.longhorn.io",
            "--force-update",
        ],
        timeout=180,
    )
    shell.run(
        [
            "helm",
            "upgrade",
            "--install",
            "longhorn",
            "longhorn/longhorn",
            "--kube-context",
            context,
            "--namespace",
            "longhorn-system",
            "--create-namespace",
            "--version",
            longhorn_version,
            "--set",
            "persistence.defaultClassReplicaCount=1",
            "--wait",
            "--timeout",
            "15m",
        ],
        timeout=960,
    )


def run_longhorn_rwo_retain_test(shell: Shell, *, context: str) -> bool:
    """Write through one Longhorn RWO claim and read it from a new Pod."""

    namespace = "srw-vm-recovery-storage-gate"
    token = "srw-longhorn-" + secrets.token_hex(12)
    shell.run(_kubectl(context, "create", "namespace", namespace), check=False)
    pvc = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "retained", "namespace": namespace},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "storageClassName": "longhorn",
            "resources": {"requests": {"storage": "128Mi"}},
        },
    }

    def pod(name: str, command: str) -> dict[str, Any]:
        return {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": name, "namespace": namespace},
            "spec": {
                "restartPolicy": "Never",
                "containers": [
                    {
                        "name": "probe",
                        "image": "busybox:1.36",
                        "command": ["sh", "-ceu", command],
                        "volumeMounts": [{"name": "data", "mountPath": "/data"}],
                    }
                ],
                "volumes": [
                    {
                        "name": "data",
                        "persistentVolumeClaim": {"claimName": "retained"},
                    }
                ],
            },
        }

    try:
        shell.run(_kubectl(context, "apply", "-f", "-"), data=json.dumps(pvc))
        writer = pod("writer", f"printf %s '{token}' > /data/probe; sync")
        shell.run(_kubectl(context, "apply", "-f", "-"), data=json.dumps(writer))
        _wait(
            shell,
            context,
            "-n",
            namespace,
            "wait",
            "pod/writer",
            "--for=jsonpath={.status.phase}=Succeeded",
            "--timeout=5m",
            timeout=330,
        )
        shell.run(
            _kubectl(context, "-n", namespace, "delete", "pod", "writer", "--wait=true")
        )
        reader = pod(
            "reader", f"test \"$(cat /data/probe)\" = '{token}'; printf %s '{token}'"
        )
        shell.run(_kubectl(context, "apply", "-f", "-"), data=json.dumps(reader))
        _wait(
            shell,
            context,
            "-n",
            namespace,
            "wait",
            "pod/reader",
            "--for=jsonpath={.status.phase}=Succeeded",
            "--timeout=5m",
            timeout=330,
        )
        observed = shell.run(_kubectl(context, "-n", namespace, "logs", "pod/reader"))
        require(observed == token, "Longhorn RWO retained read returned another value")
        return True
    finally:
        shell.run(
            _kubectl(context, "delete", "namespace", namespace, "--wait=false"),
            check=False,
            timeout=60,
        )


def deploy_application(
    shell: Shell,
    *,
    context: str,
    namespace: str,
    values_file: Path,
    images: dict[str, str],
    guest_image: str,
) -> None:
    lifecycle_secret = secrets.token_hex(48)
    secret_manifest = json.dumps(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "vm-recovery-lifecycle", "namespace": namespace},
            "stringData": {"VM_LIFECYCLE_HMAC_SECRET": lifecycle_secret},
        }
    )
    shell.run(_kubectl(context, "create", "namespace", namespace), check=False)
    shell.run(_kubectl(context, "apply", "-f", "-"), data=secret_manifest)
    with tempfile.TemporaryDirectory(prefix="srw-vm-recovery-key-") as directory:
        private_key = Path(directory) / "id_ed25519"
        shell.run(
            ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(private_key)]
        )
        secret = shell.run(
            _kubectl(
                context,
                "-n",
                namespace,
                "create",
                "secret",
                "generic",
                "vm-recovery-ssh",
                f"--from-file=ssh-privatekey={private_key}",
                f"--from-file=ssh-publickey={private_key}.pub",
                "--dry-run=client",
                "-o",
                "json",
            )
        )
        shell.run(_kubectl(context, "apply", "-f", "-"), data=secret)
    shell.run(
        [
            "helm",
            "upgrade",
            "--install",
            "srw",
            str(ROOT / "helm"),
            "--kube-context",
            context,
            "--namespace",
            namespace,
            "--values",
            str(values_file),
            "--set",
            "vm.mode=same-cluster",
            "--set",
            "vm.lifecycleAuthSecretName=vm-recovery-lifecycle",
            "--set",
            "secrets.existingVmSshKeySecret=vm-recovery-ssh",
            "--set",
            "vmController.persistentRootdisk.enabled=true",
            "--set",
            "vmController.vmStorageClass=longhorn",
            "--set",
            "agent.stateless.enabled=true",
            "--set",
            "agent.stateless.worker.enabled=true",
            "--set",
            "orchestrator.vmWorkspaceRecovery.enabled=true",
            "--set",
            "orchestrator.vmWorkspaceRecovery.replacementEnabled=true",
            "--set",
            "orchestrator.vmWorkspaceRecovery.claimTtlSeconds=90",
            "--set",
            "orchestrator.vmWorkspaceRecovery.permitTtlSeconds=90",
            "--set",
            "orchestrator.vmWorkspaceRecovery.externalCallTimeoutSeconds=60",
            "--set",
            "orchestrator.vmWorkspaceRecoveryAcceptanceGate.enabled=true",
            "--set",
            "orchestrator.vmProvisioning.creationRetryEnabled=true",
            "--set",
            "vmController.networkProfile.enabled=true",
            "--set-string",
            f"vmController.networkProfile.imageAllowlist[0]={guest_image}",
            "--set-string",
            f"image.orchestrator.repository={images['orchestrator'].rsplit(':', 1)[0]}",
            "--set-string",
            f"image.orchestrator.tag={images['orchestrator'].rsplit(':', 1)[1]}",
            "--set-string",
            "image.orchestrator.digest=",
            "--set-string",
            "image.orchestrator.pullPolicy=IfNotPresent",
            "--set-string",
            f"image.agent.repository={images['agent'].rsplit(':', 1)[0]}",
            "--set-string",
            f"image.agent.tag={images['agent'].rsplit(':', 1)[1]}",
            "--set-string",
            "image.agent.digest=",
            "--set-string",
            "image.agent.pullPolicy=IfNotPresent",
            "--set-string",
            f"vmController.image.repository={images['vm_controller'].rsplit(':', 1)[0]}",
            "--set-string",
            f"vmController.image.tag={images['vm_controller'].rsplit(':', 1)[1]}",
            "--set-string",
            "vmController.image.pullPolicy=IfNotPresent",
            "--set-string",
            f"vmController.defaultVmImage={guest_image}",
            "--wait",
            "--timeout",
            "20m",
        ],
        timeout=1260,
    )


def live_substrate_evidence(
    shell: Shell, *, context: str, longhorn_rwo_retain_test: bool
) -> dict[str, Any]:
    namespace = json.loads(
        shell.run(_kubectl(context, "get", "namespace", "kube-system", "-o", "json"))
    )
    kv = json.loads(
        shell.run(
            _kubectl(
                context, "-n", "kubevirt", "get", "kubevirt", "kubevirt", "-o", "json"
            )
        )
    )
    cdi = json.loads(shell.run(_kubectl(context, "get", "cdi", "cdi", "-o", "json")))
    storage = json.loads(
        shell.run(_kubectl(context, "get", "storageclass", "longhorn", "-o", "json"))
    )
    longhorn_pods = json.loads(
        shell.run(
            _kubectl(context, "-n", "longhorn-system", "get", "pods", "-o", "json")
        )
    ).get("items", [])
    longhorn_nodes = json.loads(
        shell.run(
            _kubectl(
                context,
                "-n",
                "longhorn-system",
                "get",
                "nodes.longhorn.io",
                "-o",
                "json",
            )
        )
    ).get("items", [])

    def available(resource: dict[str, Any]) -> bool:
        return any(
            condition.get("type") == "Available" and condition.get("status") == "True"
            for condition in resource.get("status", {}).get("conditions", [])
        )

    def pod_ready(pod: dict[str, Any]) -> bool:
        return any(
            condition.get("type") == "Ready" and condition.get("status") == "True"
            for condition in pod.get("status", {}).get("conditions", [])
        )

    manager_pods = [
        pod
        for pod in longhorn_pods
        if str(pod.get("metadata", {}).get("name") or "").startswith(
            "longhorn-manager-"
        )
    ]
    csi_pods = [
        pod
        for pod in longhorn_pods
        if str(pod.get("metadata", {}).get("name") or "").startswith(
            ("longhorn-csi-plugin-", "csi-attacher-", "csi-provisioner-")
        )
    ]
    workloads_ready = bool(manager_pods and csi_pods) and all(
        pod_ready(pod) for pod in (*manager_pods, *csi_pods)
    )

    def node_ready(node: dict[str, Any]) -> bool:
        conditions = {
            item.get("type"): item.get("status")
            for item in node.get("status", {}).get("conditions", [])
        }
        return (
            conditions.get("Ready") == "True"
            and conditions.get("Schedulable") == "True"
        )

    nodes_ready = bool(longhorn_nodes) and all(
        node_ready(node) for node in longhorn_nodes
    )

    evidence = {
        "cluster_created_by_gate": True,
        "cluster_uid": namespace["metadata"]["uid"],
        "kubernetes": bool(namespace["metadata"]["uid"]),
        "kubevirt": available(kv),
        "cdi": available(cdi),
        "longhorn": storage.get("provisioner") == "driver.longhorn.io",
        "longhorn_workloads_ready": workloads_ready,
        "longhorn_nodes_ready": nodes_ready,
        "longhorn_rwo_retain_test": longhorn_rwo_retain_test,
    }
    for key in (
        "kubernetes",
        "kubevirt",
        "cdi",
        "longhorn",
        "longhorn_workloads_ready",
        "longhorn_nodes_ready",
        "longhorn_rwo_retain_test",
    ):
        require(evidence[key] is True, f"installed substrate is not ready: {key}")
    return evidence


def deployed_checkout_revisions(
    shell: Shell,
    *,
    context: str,
    namespace: str,
    images: Mapping[str, str],
) -> dict[str, str]:
    """Bind live Pods to the exact imported checkout tags and image IDs."""

    result: dict[str, str] = {}
    for component, image_key, container in (
        ("orchestrator", "orchestrator", "orchestrator"),
        ("vm-controller", "vm_controller", "vm-controller"),
    ):
        document = json.loads(
            shell.run(
                _kubectl(
                    context,
                    "-n",
                    namespace,
                    "get",
                    "pods",
                    "-l",
                    f"app.kubernetes.io/component={component}",
                    "-o",
                    "json",
                )
            )
        )
        pods = document.get("items") or []
        require(len(pods) == 1, f"expected one live {component} Pod")
        pod = pods[0]
        declared = {
            item.get("name"): item.get("image")
            for item in pod.get("spec", {}).get("containers", [])
        }
        statuses = {
            item.get("name"): item.get("imageID")
            for item in pod.get("status", {}).get("containerStatuses", [])
        }
        require(
            declared.get(container) == images[image_key]
            and bool(str(statuses.get(container) or "").strip()),
            f"{component} Pod is not running the imported checkout image",
        )
        require(
            image_id_matches_config_digest(
                statuses.get(container), images[f"{image_key}_config_id"]
            ),
            f"{component} Pod image ID differs from the imported checkout config ID",
        )
        result["application" if component == "orchestrator" else "controller"] = str(
            statuses[container]
        )
    return result


def run_scenario_driver(
    shell: Shell,
    *,
    driver: Path,
    context: str,
    namespace: str,
    output: Path,
) -> dict[str, Any]:
    require(
        driver.is_file() and os.access(driver, os.X_OK),
        "scenario driver is not executable",
    )
    shell.run(
        [
            str(driver),
            "--kube-context",
            context,
            "--namespace",
            namespace,
            "--output",
            str(output),
        ],
        timeout=3600,
    )
    require(output.is_file(), "scenario driver did not write evidence")
    try:
        evidence = json.loads(output.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise GateFailure("scenario evidence is not valid JSON") from exc
    require(isinstance(evidence, dict), "scenario evidence is not an object")
    if evidence.get("gate_status") == "skipped":
        missing = str(evidence.get("missing_capability") or "scenario-adapter")
        raise GateSkip(missing)
    return evidence


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--preflight",
        action="store_true",
        help="read-only host capability check (default)",
    )
    mode.add_argument(
        "--run", action="store_true", help="create and exercise a disposable cluster"
    )
    parser.add_argument("--cluster-name", default=f"{CLUSTER_PREFIX}{int(time.time())}")
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    parser.add_argument("--values-file", type=Path)
    parser.add_argument(
        "--guest-image",
        help="immutable guest image reference ending in @sha256:<64 hex>",
    )
    parser.add_argument(
        "--container-engine",
        choices=("docker", "podman"),
        default=os.environ.get("CONTAINER_ENGINE", "podman"),
        help=(
            "container engine; this k3d gate currently requires docker for "
            "platform-pruned archives and exact containerd image-ID proof"
        ),
    )
    parser.add_argument("--scenario-driver", type=Path, default=DEFAULT_SCENARIO_DRIVER)
    parser.add_argument("--evidence-output", type=Path)
    parser.add_argument("--kubevirt-version", default=DEFAULT_KUBEVIRT_VERSION)
    parser.add_argument("--cdi-version", default=DEFAULT_CDI_VERSION)
    parser.add_argument("--longhorn-version", default=DEFAULT_LONGHORN_VERSION)
    parser.add_argument(
        "--keep-on-failure",
        action="store_true",
        help="retain only a failed gate-created cluster for diagnosis",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = host_preflight(
        values_file=args.values_file,
        scenario_driver=args.scenario_driver,
        guest_image=args.guest_image,
        container_engine=args.container_engine,
    )
    if not report.ready:
        print("SKIPPED: missing capability: " + ", ".join(report.missing))
        return 0
    if not args.run:
        print("READY: host capabilities available; no cluster was contacted")
        return 0
    require(args.values_file is not None, "--run requires --values-file")
    require(args.guest_image is not None, "--run requires --guest-image")
    require_disposable_cluster_name(args.cluster_name)

    shell = Shell()
    context: str | None = None
    ownership: tuple[str, ...] | None = None
    failed = True
    state_directory = tempfile.TemporaryDirectory(prefix="srw-vm-recovery-state-")
    try:
        context = create_cluster(shell, args.cluster_name)
        ownership = capture_cluster_ownership(shell, args.cluster_name)
        kubeconfig = Path(state_directory.name) / "kubeconfig.yaml"
        write_private_kubeconfig(shell, args.cluster_name, kubeconfig)
        require_longhorn_node_prerequisites(args.cluster_name)
        run_longhorn_preflight(
            shell,
            context=context,
            kubeconfig=kubeconfig,
            version=args.longhorn_version,
        )
        with tempfile.TemporaryDirectory(prefix="srw-vm-recovery-images-") as image_dir:
            images = build_and_import_checkout_images(
                shell,
                cluster_name=args.cluster_name,
                archive_dir=Path(image_dir),
            )
        install_substrate(
            shell,
            context=context,
            kubevirt_version=args.kubevirt_version,
            cdi_version=args.cdi_version,
            longhorn_version=args.longhorn_version,
        )
        longhorn_rwo_retain_test = run_longhorn_rwo_retain_test(shell, context=context)
        deploy_application(
            shell,
            context=context,
            namespace=args.namespace,
            values_file=args.values_file,
            images=images,
            guest_image=args.guest_image,
        )
        with tempfile.TemporaryDirectory(
            prefix="srw-vm-recovery-evidence-"
        ) as directory:
            evidence_path = Path(directory) / "evidence.json"
            evidence = run_scenario_driver(
                shell,
                driver=args.scenario_driver.resolve(),
                context=context,
                namespace=args.namespace,
                output=evidence_path,
            )
            evidence["substrate"] = live_substrate_evidence(
                shell,
                context=context,
                longhorn_rwo_retain_test=longhorn_rwo_retain_test,
            )
            deployed = deployed_checkout_revisions(
                shell,
                context=context,
                namespace=args.namespace,
                images=images,
            )
            revisions = _section(evidence, "revisions")
            require(
                revisions.get("application") == deployed["application"]
                and revisions.get("controller") == deployed["controller"]
                and args.guest_image in str(revisions.get("guest_image") or ""),
                "scenario revisions do not match deployed checkout and guest images",
            )
            validate_acceptance_evidence(evidence)
            if args.evidence_output is not None:
                args.evidence_output.write_text(
                    json.dumps(evidence, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
        failed = False
        print(
            "PASS: real retained-disk VM workspace recovery acceptance evidence verified"
        )
        return 0
    except GateFailure as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    except GateSkip as exc:
        print(f"SKIPPED: missing capability: {exc}")
        return 0
    finally:
        if context is not None and not (failed and args.keep_on_failure):
            if ownership is None:
                raise GateFailure(
                    "gate cluster ownership was not captured; refusing deletion"
                )
            delete_owned_cluster(shell, args.cluster_name, ownership=ownership)
        state_directory.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
