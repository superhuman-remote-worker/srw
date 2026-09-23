#!/usr/bin/env python3
"""Prepare one A1 Job through the production paused-Job and VM preflight writers.

This host command has no Kubernetes write of its own. The in-image factory is
default-off and refuses a nonempty Job database or an unowned model endpoint.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import stat
import subprocess
from typing import Any, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5


_OWNED = re.compile(r"srw-a1-[a-z0-9][a-z0-9-]{2,50}")
_DIGEST_IMAGE = re.compile(r"[a-z0-9./_-]+@sha256:[0-9a-f]{64}")
_IMAGE_ID = re.compile(
    r"(?:docker-pullable://|containerd://|docker://)?"
    r"(?:[a-z0-9./_-]+@)?sha256:[0-9a-f]{64}"
)
_CONFIRM = "disposable-vm-retained-resume-fixture-v1"


class FixtureHostRefusal(RuntimeError):
    """The exact disposable host boundary is unavailable."""


def _uuid(value: str) -> str:
    try:
        parsed = str(UUID(value))
    except (TypeError, ValueError) as exc:
        raise FixtureHostRefusal("a required UUID is malformed") from exc
    if parsed != value:
        raise FixtureHostRefusal("a required UUID is not canonical")
    return value


def guard(args: argparse.Namespace) -> None:
    if (
        args.confirm != _CONFIRM
        or not _OWNED.fullmatch(args.run_id)
        or len(f"srw-a1-provider-{args.run_id}") > 63
        or args.namespace != args.run_id
        or args.context != args.run_id
        or not re.fullmatch(r"e2e-vm-[a-z0-9-]{3,55}", args.model_id)
        or not _DIGEST_IMAGE.fullmatch(args.vm_image)
        or not re.fullmatch(r"[a-z0-9-]{3,63}-orchestrator", args.deployment)
        or not re.fullmatch(r"[a-z0-9-]{3,63}", args.pod)
        or not _IMAGE_ID.fullmatch(args.image_id)
    ):
        raise FixtureHostRefusal("A1 clean fixture scope is not exact")
    _uuid(args.cluster_uid)
    _uuid(args.pod_uid)
    output = args.output
    parent = output.parent
    if (
        not parent.is_dir() or parent.is_symlink()
        or stat.S_IMODE(parent.stat().st_mode) & 0o077
        or output.exists()
        or output.is_symlink()
    ):
        raise FixtureHostRefusal("A1 private output is not fresh")


def _kubectl(
    args: argparse.Namespace, *parts: str, timeout: int = 30,
    input_bytes: bytes | None = None,
) -> str:
    try:
        result = subprocess.run(
            ["kubectl", "--context", args.context, *parts],
            input=input_bytes, capture_output=True, check=False, timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise FixtureHostRefusal("bounded kubectl invocation failed") from exc
    if result.returncode:
        raise FixtureHostRefusal("bounded kubectl invocation was refused")
    return result.stdout.decode("utf-8", errors="strict")


def _object(args: argparse.Namespace, *parts: str) -> dict[str, Any]:
    value = json.loads(_kubectl(args, *parts, "-o", "json"))
    if not isinstance(value, dict):
        raise FixtureHostRefusal("Kubernetes inventory is malformed")
    return value


def verify_host_source(args: argparse.Namespace) -> None:
    selected = json.loads(_kubectl(args, "config", "view", "--minify", "-o", "json"))
    if (
        len(selected.get("contexts") or []) != 1
        or selected["contexts"][0].get("name") != args.context
    ):
        raise FixtureHostRefusal("kubeconfig context changed")
    system = _object(args, "get", "namespace", "kube-system")
    namespace = _object(args, "get", "namespace", args.namespace)
    if (
        system.get("metadata", {}).get("uid") != args.cluster_uid
        or namespace.get("metadata", {}).get("name") != args.namespace
        or namespace.get("metadata", {}).get("labels", {}).get("srw.io/a1-owned-run")
        != args.run_id
    ):
        raise FixtureHostRefusal("clean cluster or namespace identity changed")
    vms = _object(args, "-n", args.namespace, "get", "virtualmachines")
    expected_job = uuid5(NAMESPACE_URL, f"srw-a1-job:{args.run_id}")
    names = {item.get("metadata", {}).get("name") for item in vms.get("items") or []}
    if names not in (set(), {f"agent-vm-{expected_job}"}):
        raise FixtureHostRefusal("A1 namespace contains an unrelated VM")
    workers = _object(args, "-n", args.namespace, "get", "pods",
                      "-l", "srw/class=agent-stateless")
    if workers.get("items"):
        raise FixtureHostRefusal("a stateless worker is already active")
    pod = _object(args, "-n", args.namespace, "get", "pod", args.pod)
    deployment = _object(args, "-n", args.namespace, "get", "deployment", args.deployment)
    owners = pod.get("metadata", {}).get("ownerReferences") or []
    replica_set = (
        _object(args, "-n", args.namespace, "get", "replicaset", owners[0]["name"])
        if len(owners) == 1 and owners[0].get("kind") == "ReplicaSet"
        else {}
    )
    rs_owners = replica_set.get("metadata", {}).get("ownerReferences") or []
    statuses = pod.get("status", {}).get("containerStatuses") or []
    container = next((item for item in statuses if item.get("name") == "orchestrator"), {})
    if (
        pod.get("metadata", {}).get("uid") != args.pod_uid
        or pod.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component")
        != "orchestrator"
        or container.get("imageID") != args.image_id
        or not deployment.get("metadata", {}).get("uid")
        or replica_set.get("metadata", {}).get("uid") != owners[0].get("uid")
        or len(rs_owners) != 1
        or rs_owners[0].get("kind") != "Deployment"
        or rs_owners[0].get("name") != args.deployment
        or rs_owners[0].get("uid") != deployment["metadata"]["uid"]
    ):
        raise FixtureHostRefusal("orchestrator Pod or image identity changed")


def run(args: argparse.Namespace) -> dict[str, Any]:
    guard(args)
    verify_host_source(args)
    key_file = args.inference_key_file
    if (
        not key_file.is_file() or key_file.is_symlink()
        or stat.S_IMODE(key_file.stat().st_mode) & 0o077
    ):
        raise FixtureHostRefusal("provider inference key file is not private")
    inference_key = key_file.read_bytes().strip()
    if not 16 <= len(inference_key) <= 256 or b"\n" in inference_key:
        raise FixtureHostRefusal("provider inference key is malformed")
    remote = f"/tmp/srw-vm-retained-resume-gate/{args.run_id}/fixture.json"
    command = (
        "-n", args.namespace, "exec", "-i", "pod/" + args.pod,
        "-c", "orchestrator", "--", "python", "-m",
        "orchestrator.operator_cli.vm_retained_resume_fixture",
        "--run-id", args.run_id, "--namespace", args.namespace,
        "--vm-image", args.vm_image, "--model-id", args.model_id,
        "--confirm", _CONFIRM, "--output", remote,
    )
    try:
        _kubectl(args, *command, timeout=1020,
                 input_bytes=inference_key + b"\n")
    finally:
        # The Pod/cluster identity is rechecked before any persisted outcome is
        # trusted, including after an interrupted in-image wait.
        verify_host_source_after(args)
    value = json.loads(_kubectl(
        args, "-n", args.namespace, "exec", "pod/" + args.pod,
        "-c", "orchestrator", "--", "cat", remote,
    ))
    if (
        not isinstance(value, dict)
        or value.get("run_id") != args.run_id
        or value.get("outcome") != "ready"
        or value.get("protocol_version") != 1
    ):
        raise FixtureHostRefusal("in-image fixture did not establish Ready")
    for key in ("job_id", "owner_id", "expert_id", "request_id",
                "provision_generation", "vm_uid", "vmi_uid", "launcher_uid",
                "pvc_uid", "pv_uid", "pause_hold_id"):
        _uuid(value[key])
    if value.get("job_id") == value.get("owner_id"):
        raise FixtureHostRefusal("fixture owner and Job identities collide")
    if value["job_id"] != str(uuid5(NAMESPACE_URL, f"srw-a1-job:{args.run_id}")):
        raise FixtureHostRefusal("fixture Job changed from the run identity")
    tmp = args.output.with_name(".fixture-" + os.urandom(8).hex())
    descriptor = os.open(tmp, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        tmp.replace(args.output)
    finally:
        tmp.unlink(missing_ok=True)
    return value


def verify_host_source_after(args: argparse.Namespace) -> None:
    system = _object(args, "get", "namespace", "kube-system")
    pod = _object(args, "-n", args.namespace, "get", "pod", args.pod)
    if (
        system.get("metadata", {}).get("uid") != args.cluster_uid
        or pod.get("metadata", {}).get("uid") != args.pod_uid
    ):
        raise FixtureHostRefusal("cluster or Pod changed during fixture")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    for name in ("run-id", "context", "cluster-uid", "namespace",
                 "deployment", "pod", "pod-uid", "image-id", "vm-image",
                 "model-id", "confirm"):
        result.add_argument("--" + name, required=True)
    result.add_argument("--output", type=Path, required=True)
    result.add_argument("--inference-key-file", type=Path, required=True)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        run(args)
        return 0
    except Exception as exc:
        # kubectl and in-image errors may contain credentials or API payloads.
        print(f"A1 fixture held: {type(exc).__name__}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
