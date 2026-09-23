#!/usr/bin/env python3
"""Host-owned disposable A1 gate with an exact temporary VM-count quota.

This wrapper never creates or deletes a VM/PVC/Job. Its only Kubernetes write
is its own run-labelled ResourceQuota; the in-image command uses normal owner
HTTP Resume and production VM lifecycle authority. A generic worker pool must
remain disabled until the in-image queue isolation check passes.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import select
import socket
import stat
import subprocess
import sys
import time
from typing import Any, Mapping, Sequence
from urllib import error, request
from uuid import UUID


CONFIRMATION = "disposable-vm-retained-resume-gate-v1"
_RUN_ID = re.compile(r"srw-a1-[a-z0-9][a-z0-9-]{2,50}")
_OWNED = re.compile(r"srw-a1-[a-z0-9][a-z0-9-]{2,50}")


class GateFailure(RuntimeError):
    """The owned host boundary or actual acceptance evidence is unavailable."""


def _uuid(value: object) -> str:
    try:
        return str(UUID(str(value)))
    except (TypeError, ValueError, AttributeError) as exc:
        raise GateFailure("required UUID identity is unavailable") from exc


def require_host_guard(args: argparse.Namespace) -> None:
    if args.protocol_version != 1 or args.confirm != CONFIRMATION:
        raise GateFailure("disposable protocol or confirmation changed")
    if (
        not _RUN_ID.fullmatch(args.run_id)
        or not _OWNED.fullmatch(args.context)
        or not (_OWNED.fullmatch(args.namespace) or args.namespace == "srw")
        or args.vm_namespace != args.namespace
    ):
        raise GateFailure("context or namespace is not exclusively gate-owned")
    for value in (args.cluster_uid, args.job_id, args.expected_owner_id,
                  args.expected_pvc_uid):
        _uuid(value)
    if not re.fullmatch(r"[a-z0-9-]+-orchestrator", args.orchestrator_deploy):
        raise GateFailure("orchestrator target is malformed")
    if not re.fullmatch(r"[a-z0-9-]{3,63}", args.orchestrator_pod):
        raise GateFailure("orchestrator Pod name is malformed")
    _uuid(args.orchestrator_pod_uid)
    if not args.cleanup_only:
        if (
            not _OWNED.fullmatch(args.provider_namespace)
            or not re.fullmatch(r"[a-z0-9-]{3,63}", args.provider_pod)
        ):
            raise GateFailure("provider target is not gate-owned")
        _uuid(args.provider_pod_uid)
        token_file = args.provider_control_token_file
        if (
            not token_file.is_file() or token_file.is_symlink()
            or stat.S_IMODE(token_file.stat().st_mode) & 0o077
        ):
            raise GateFailure("provider control token file is not private")
    parent = args.output.parent
    parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    if parent.is_symlink() or stat.S_IMODE(parent.stat().st_mode) & 0o077:
        raise GateFailure("host output directory is not private")
    if args.output.exists():
        raise GateFailure("host result already exists")


def _kubectl(args: argparse.Namespace, *parts: str) -> list[str]:
    return ["kubectl", "--context", args.context, *parts]


def _run(argv: Sequence[str], *, input_bytes: bytes | None = None) -> str:
    result = subprocess.run(
        argv, input=input_bytes, capture_output=True, check=False, timeout=30,
    )
    if result.returncode != 0:
        raise GateFailure("bounded kubectl command failed")
    return result.stdout.decode("utf-8", errors="strict")


def _read_json(args: argparse.Namespace, *parts: str) -> dict[str, Any]:
    value = json.loads(_run(_kubectl(args, *parts, "-o", "json")))
    if not isinstance(value, dict):
        raise GateFailure("Kubernetes response is not an object")
    return value


def verify_context(args: argparse.Namespace) -> dict[str, str]:
    context = json.loads(_run(_kubectl(args, "config", "view", "--minify", "-o", "json")))
    selected = context.get("contexts") or []
    if len(selected) != 1 or selected[0].get("name") != args.context:
        raise GateFailure("kubeconfig context changed")
    system = _read_json(args, "get", "namespace", "kube-system")
    if system.get("metadata", {}).get("uid") != args.cluster_uid:
        raise GateFailure("cluster UID changed")
    namespace = _read_json(args, "get", "namespace", args.namespace)
    vm_namespace = _read_json(args, "get", "namespace", args.vm_namespace)
    if not namespace.get("metadata", {}).get("uid") or not vm_namespace.get("metadata", {}).get("uid"):
        raise GateFailure("owned namespaces are unavailable")
    if args.cleanup_only:
        return {"namespace_uid": namespace["metadata"]["uid"],
                "vm_namespace_uid": vm_namespace["metadata"]["uid"]}
    vms = _read_json(args, "-n", args.vm_namespace, "get", "virtualmachines")
    names = {item.get("metadata", {}).get("name") for item in vms.get("items") or []}
    if names not in ({f"agent-vm-{args.job_id}"}, set()):
        raise GateFailure("quota would affect unrelated VM objects")
    provider = _read_json(args, "-n", args.provider_namespace, "get", "pod", args.provider_pod)
    if provider.get("metadata", {}).get("uid") != args.provider_pod_uid:
        raise GateFailure("provider Pod UID changed")
    return {"namespace_uid": namespace["metadata"]["uid"],
            "vm_namespace_uid": vm_namespace["metadata"]["uid"],
            "provider_pod_uid": args.provider_pod_uid}


def quota_name(run_id: str) -> str:
    # DNS label ≤63 and deterministic across cleanup after an interrupted run.
    import hashlib

    return "srw-a1-vm-post-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]


def quota_body(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "apiVersion": "v1", "kind": "ResourceQuota",
        "metadata": {"name": quota_name(args.run_id), "namespace": args.vm_namespace,
                     "labels": {"srw.io/a1-gate-run": args.run_id,
                                "srw.io/a1-gate-job": args.job_id}},
        "spec": {"hard": {"count/virtualmachines.kubevirt.io": "0"}},
    }


def _quota_matches(quota: Mapping[str, Any], args: argparse.Namespace) -> bool:
    metadata = quota.get("metadata") or {}
    return bool(
        metadata.get("name") == quota_name(args.run_id)
        and metadata.get("namespace") == args.vm_namespace
        and (metadata.get("labels") or {}).get("srw.io/a1-gate-run") == args.run_id
        and (metadata.get("labels") or {}).get("srw.io/a1-gate-job") == args.job_id
        and (quota.get("spec") or {}).get("hard") == {"count/virtualmachines.kubevirt.io": "0"}
    )


def install_quota(core: Any, args: argparse.Namespace) -> tuple[str, str]:
    from kubernetes.client.exceptions import ApiException

    name = quota_name(args.run_id)
    try:
        core.read_namespaced_resource_quota(name, args.vm_namespace)
    except ApiException as exc:
        if exc.status != 404:
            raise GateFailure("quota inventory is unavailable") from None
    else:
        raise GateFailure("owned quota already exists; use cleanup-only inspection")
    vms = _read_json(args, "-n", args.vm_namespace, "get", "virtualmachines")
    if (vms.get("items") or []) != []:
        raise GateFailure("predecessor VM has not physically retired")
    created = core.create_namespaced_resource_quota(args.vm_namespace, quota_body(args))
    if not created.metadata.uid:
        raise GateFailure("created quota UID is unavailable")
    return str(created.metadata.uid), str(created.metadata.resource_version)


def wait_quota_active(core: Any, args: argparse.Namespace, uid: str) -> None:
    name = quota_name(args.run_id)
    until = time.monotonic() + 30
    while time.monotonic() < until:
        observed = core.read_namespaced_resource_quota(name, args.vm_namespace)
        data = observed.to_dict()
        if not _quota_matches(data, args) or str(observed.metadata.uid) != uid:
            raise GateFailure("created quota identity changed")
        status = data.get("status") or {}
        hard = (status.get("hard") or {}).get("count/virtualmachines.kubevirt.io")
        used = (status.get("used") or {}).get("count/virtualmachines.kubevirt.io")
        if str(hard) == "0" and str(used) == "0":
            return
        time.sleep(0.5)
    raise GateFailure("owned quota did not become active")


def remove_quota(core: Any, args: argparse.Namespace, uid: str) -> bool:
    from kubernetes import client
    from kubernetes.client.exceptions import ApiException

    name = quota_name(args.run_id)
    try:
        current = core.read_namespaced_resource_quota(name, args.vm_namespace)
    except ApiException as exc:
        if exc.status == 404:
            return False
        raise GateFailure("quota removal inventory is unavailable") from None
    if str(current.metadata.uid) != uid or not _quota_matches(current.to_dict(), args):
        raise GateFailure("refusing to remove a changed quota")
    core.delete_namespaced_resource_quota(
        name, args.vm_namespace,
        body=client.V1DeleteOptions(preconditions=client.V1Preconditions(
            uid=uid, resource_version=str(current.metadata.resource_version),
        )),
    )
    return True


def wait_quota_absent(core: Any, args: argparse.Namespace, uid: str) -> None:
    from kubernetes.client.exceptions import ApiException

    until = time.monotonic() + 30
    while time.monotonic() < until:
        try:
            value = core.read_namespaced_resource_quota(quota_name(args.run_id), args.vm_namespace)
        except ApiException as exc:
            if exc.status == 404:
                return
            raise GateFailure("quota removal observation failed") from None
        if str(value.metadata.uid) != uid:
            raise GateFailure("quota name was replaced during removal")
        time.sleep(0.5)
    raise GateFailure("owned quota deletion did not settle")


def existing_owned_quota_uid(core: Any, args: argparse.Namespace) -> str | None:
    """Discover only this run's exact quota for interrupted-run cleanup."""
    from kubernetes.client.exceptions import ApiException

    try:
        observed = core.read_namespaced_resource_quota(
            quota_name(args.run_id), args.vm_namespace,
        )
    except ApiException as exc:
        if exc.status == 404:
            return None
        raise GateFailure("owned quota inventory is unavailable") from None
    if not _quota_matches(observed.to_dict(), args):
        raise GateFailure("quota name belongs to a changed object")
    return _uuid(observed.metadata.uid)


def _provider_request(port: int, token: str, method: str, path: str,
                      body: Mapping[str, Any] | None = None) -> dict[str, Any]:
    payload = json.dumps(body).encode() if body is not None else None
    req = request.Request(
        f"http://127.0.0.1:{port}{path}", data=payload, method=method,
        headers={"Authorization": f"Bearer {token}",
                 "Content-Type": "application/json"},
    )
    try:
        with request.urlopen(req, timeout=10) as response:
            decoded = json.load(response)
    except (error.HTTPError, error.URLError, TimeoutError, ValueError):
        raise GateFailure("provider control request failed") from None
    if not isinstance(decoded, dict):
        raise GateFailure("provider control response is malformed")
    return decoded


def _port_forward(args: argparse.Namespace, token: str) -> tuple[subprocess.Popen[bytes], int]:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    process = subprocess.Popen(
        _kubectl(args, "-n", args.provider_namespace, "port-forward",
                 "--address", "127.0.0.1", f"pod/{args.provider_pod}", f"{port}:8001"),
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    ready = False
    try:
        until = time.monotonic() + 15
        while time.monotonic() < until:
            if process.poll() is not None:
                raise GateFailure("provider control port-forward exited")
            try:
                state = _provider_request(port, token, "GET", "/control/health")
            except GateFailure:
                time.sleep(0.2)
                continue
            if state.get("status") == "ok":
                ready = True
                return process, port
            time.sleep(0.2)
        raise GateFailure("provider control port-forward did not become ready")
    finally:
        if not ready:
            _reap_forward(process)


def _reap_forward(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    else:
        process.wait(timeout=5)


def validate_provider_barrier_state(state: Mapping[str, Any], *, run_id: str,
                                    sentinel_sha256: str) -> None:
    steps = state.get("worker_job_tool_steps")
    if (
        state.get("run_id") != run_id
        or state.get("scenario") != "retained-sentinel-worker"
        or state.get("sentinel_sha256") != sentinel_sha256
        or type(steps) is not int or not 8 <= steps <= 10
        or state.get("completion_released") is not False
        or state.get("unexpected_count") != 0
        or state.get("pending_calls") not in {0, 1}
    ):
        raise GateFailure("provider has not held the proven sentinel before completion")


def validate_provider_result(state: Mapping[str, Any], *, run_id: str,
                             sentinel_sha256: str) -> dict[str, Any]:
    if (
        state.get("run_id") != run_id
        or state.get("scenario") != "retained-sentinel-worker"
        or state.get("sentinel_sha256") != sentinel_sha256
        or type(state.get("worker_job_tool_steps")) is not int
        or state["worker_job_tool_steps"] != 11
        or state.get("completion_released") is not True
        or state.get("unexpected_count") != 0
        or state.get("pending_calls") != 0
    ):
        raise GateFailure("deterministic provider did not prove sentinel worker sequence")
    return {"scenario": state["scenario"],
            "worker_job_tool_steps": state["worker_job_tool_steps"],
            "unexpected_count": 0, "pending_calls": 0,
            "sentinel_sha256": sentinel_sha256}


def _image_command(args: argparse.Namespace) -> list[str]:
    filename = "cleanup.json" if args.cleanup_only else "result.json"
    output = f"/tmp/srw-vm-retained-resume-gate/{args.run_id}/{filename}"
    return _kubectl(
        args, "-n", args.namespace, "exec", "-i",
        f"pod/{args.orchestrator_pod}", "-c", "orchestrator", "--",
        "python", "-m", "orchestrator.operator_cli.vm_retained_resume_acceptance",
        "--cleanup-only" if args.cleanup_only else "--execute",
        "--run-id", args.run_id,
        "--job-id", args.job_id,
        "--expected-owner-id", args.expected_owner_id,
        "--expected-pvc-uid", args.expected_pvc_uid,
        "--cluster-uid", args.cluster_uid,
        "--namespace", args.namespace,
        "--context", args.context,
        "--protocol-version", str(args.protocol_version),
        "--confirm", args.confirm,
        "--output", output,
    )


def _image_output(args: argparse.Namespace) -> dict[str, Any]:
    filename = "cleanup.json" if args.cleanup_only else "result.json"
    path = f"/tmp/srw-vm-retained-resume-gate/{args.run_id}/{filename}"
    raw = _run(_kubectl(args, "-n", args.namespace, "exec",
                        f"pod/{args.orchestrator_pod}", "-c", "orchestrator",
                        "--", "cat", path))
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise GateFailure("in-image result is malformed")
    return value


def _write_result(path: Path, value: Mapping[str, Any]) -> None:
    import secrets

    temporary = path.parent / (".result-" + secrets.token_hex(8))
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, sort_keys=True, default=str)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def run_gate(args: argparse.Namespace) -> dict[str, Any]:
    from kubernetes import client, config

    require_host_guard(args)
    config.load_kube_config(context=args.context)
    core = client.CoreV1Api()
    namespace_evidence = verify_context(args)
    pod = _read_json(args, "-n", args.namespace, "get", "pod", args.orchestrator_pod)
    deployment = _read_json(args, "-n", args.namespace, "get", "deployment", args.orchestrator_deploy)
    owners = pod.get("metadata", {}).get("ownerReferences") or []
    replica_set = (
        _read_json(args, "-n", args.namespace, "get", "replicaset", owners[0]["name"])
        if len(owners) == 1 and owners[0].get("kind") == "ReplicaSet"
        else {}
    )
    rs_owners = replica_set.get("metadata", {}).get("ownerReferences") or []
    if (
        pod.get("metadata", {}).get("uid") != args.orchestrator_pod_uid
        or pod.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component")
        != "orchestrator"
        or not deployment.get("metadata", {}).get("uid")
        or not owners
        or replica_set.get("metadata", {}).get("uid") != owners[0].get("uid")
        or len(rs_owners) != 1
        or rs_owners[0].get("kind") != "Deployment"
        or rs_owners[0].get("name") != args.orchestrator_deploy
        or rs_owners[0].get("uid") != deployment["metadata"]["uid"]
        or (deployment.get("spec", {}).get("selector", {}).get("matchLabels") or {}).items()
        - (pod.get("metadata", {}).get("labels") or {}).items()
    ):
        raise GateFailure("orchestrator Pod identity changed")
    forward: subprocess.Popen[bytes] | None = None
    image: subprocess.Popen[str] | None = None
    quota_uid: str | None = None
    quota_released = False
    provider_evidence: dict[str, Any] | None = None
    sentinel_sha256: str | None = None
    token = ""
    port = 0
    try:
        if not args.cleanup_only:
            token = args.provider_control_token_file.read_text(encoding="utf-8").strip()
            if not 16 <= len(token) <= 256 or "\n" in token:
                raise GateFailure("provider control token is malformed")
            forward, port = _port_forward(args, token)
        else:
            quota_uid = existing_owned_quota_uid(core, args)
            if quota_uid is not None:
                if not remove_quota(core, args, quota_uid):
                    raise GateFailure("owned quota disappeared during cleanup")
                wait_quota_absent(core, args, quota_uid)
                quota_released = True
        image = subprocess.Popen(
            _image_command(args), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, text=True, bufsize=1,
        )
        stages = ("ARM", "QUOTA_INSTALL", "QUOTA_RELEASE",
                  "PROVIDER_BARRIER", "PROVIDER_VERIFY")
        index = 0
        deadline = time.monotonic() + 3600
        while image.poll() is None and time.monotonic() < deadline:
            ready, _, _ = select.select([image.stdout], [], [], 0.5)
            if not ready:
                continue
            line = image.stdout.readline().strip()
            if not line:
                continue
            if args.cleanup_only or index >= len(stages):
                raise GateFailure("in-image output contained an unexpected phase")
            stage = stages[index]
            prefix = f"SRW_A1_{stage}:"
            if not line.startswith(prefix):
                raise GateFailure("in-image phase order changed")
            value = line[len(prefix):]
            verify_context(args)
            if stage == "ARM":
                if not re.fullmatch(r"[0-9a-f]{64}", value):
                    raise GateFailure("sentinel digest is malformed")
                sentinel_sha256 = value
                armed = _provider_request(
                    port, token, "POST", f"/control/scenarios/{args.run_id}/arm",
                    {"scenario": "retained-sentinel-worker",
                     "sentinel_sha256": value},
                )
                if (armed.get("scenario") != "retained-sentinel-worker"
                    or armed.get("sentinel_sha256") != value):
                    raise GateFailure("provider arm receipt changed")
            elif stage == "QUOTA_INSTALL":
                _uuid(value)  # Exact completed cleanup admission ID, not a clock delay.
                quota_uid, _ = install_quota(core, args)
                wait_quota_active(core, args, quota_uid)
            elif stage == "QUOTA_RELEASE":
                _uuid(value)
                if quota_uid is None or not remove_quota(core, args, quota_uid):
                    raise GateFailure("owned quota was unavailable at release")
                wait_quota_absent(core, args, quota_uid)
                quota_released = True
            elif stage == "PROVIDER_BARRIER":
                if value != sentinel_sha256:
                    raise GateFailure("pre-terminal sentinel digest changed")
                state = _provider_request(
                    port, token, "GET", f"/control/scenarios/{args.run_id}",
                )
                validate_provider_barrier_state(
                    state, run_id=args.run_id, sentinel_sha256=value,
                )
                released = _provider_request(
                    port, token, "POST",
                    f"/control/scenarios/{args.run_id}/release-completion",
                )
                if (released.get("run_id") != args.run_id
                    or released.get("completion_released") is not True):
                    raise GateFailure("provider completion barrier did not release")
            else:
                if value != sentinel_sha256:
                    raise GateFailure("post-worker sentinel digest changed")
                state = _provider_request(
                    port, token, "GET", f"/control/scenarios/{args.run_id}",
                )
                provider_evidence = validate_provider_result(
                    state, run_id=args.run_id, sentinel_sha256=value,
                )
            assert image.stdin is not None
            image.stdin.write(f"SRW_A1_ACK:{stage}\n")
            image.stdin.flush()
            index += 1
        if image.poll() is None:
            raise GateFailure("in-image scenario exceeded the host deadline")
        if image.returncode != 0 or (not args.cleanup_only and index != len(stages)):
            raise GateFailure("in-image scenario did not complete its required phases")
        result = _image_output(args)
        if (
            result.get("run_id") != args.run_id
            or result.get("job_id") != args.job_id
            or result.get("owner_id") != args.expected_owner_id
            or result.get("cluster_uid") not in {None, args.cluster_uid}
            or result.get("outcome") != ("cleanup_requested" if args.cleanup_only else "passed")
            or (not args.cleanup_only and (
                result.get("sentinel_sha256") != sentinel_sha256
                or result.get("replacement_request_id") is None
                or provider_evidence is None or not quota_released
            ))
        ):
            raise GateFailure("in-image result failed host identity assertions")
        result["host"] = {
            "context": args.context, "cluster_uid": args.cluster_uid,
            **namespace_evidence,
            "orchestrator_pod_uid": args.orchestrator_pod_uid,
            "quota_uid": quota_uid, "quota_released": quota_released,
            "provider": provider_evidence,
        }
        _write_result(args.output, result)
        return result
    finally:
        if image is not None and image.poll() is None:
            image.terminate()
            try:
                image.wait(timeout=5)
            except subprocess.TimeoutExpired:
                image.kill()
        if quota_uid is not None and not quota_released:
            remove_quota(core, args, quota_uid)
            wait_quota_absent(core, args, quota_uid)
        if forward is not None:
            _reap_forward(forward)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--execute", action="store_true")
    mode.add_argument("--cleanup-only", action="store_true")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--expected-owner-id", required=True)
    parser.add_argument("--expected-pvc-uid", required=True)
    parser.add_argument("--context", required=True)
    parser.add_argument("--cluster-uid", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--vm-namespace", required=True)
    parser.add_argument("--orchestrator-deploy", required=True)
    parser.add_argument("--orchestrator-pod", required=True)
    parser.add_argument("--orchestrator-pod-uid", required=True)
    parser.add_argument("--provider-namespace")
    parser.add_argument("--provider-pod")
    parser.add_argument("--provider-pod-uid")
    parser.add_argument("--provider-control-token-file", type=Path)
    parser.add_argument("--protocol-version", type=int, required=True)
    parser.add_argument("--confirm", required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    guarded = False
    try:
        require_host_guard(args)
        guarded = True
        run_gate(args)
    except Exception as exc:
        # Do not echo kubectl/API responses; they may include signed material.
        if guarded and not args.output.exists():
            _write_result(args.output, {
                "protocol_version": 1, "run_id": args.run_id,
                "job_id": args.job_id, "outcome": "held",
                "error_class": type(exc).__name__,
            })
        print(f"A1 gate held ({type(exc).__name__})", file=sys.stderr)
        return 1
    print("A1 gate evidence written", file=sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
