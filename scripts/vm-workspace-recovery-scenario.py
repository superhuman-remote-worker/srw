#!/usr/bin/env python3
"""Invoke the in-release VM recovery acceptance adapter on a gate-owned cluster.

This source-controlled driver is intentionally a narrow adapter rather than a
second implementation of recovery. The disposable release must provide the
ConfigMap ``srw-vm-workspace-recovery-gate-adapter`` with this exact data:

``protocolVersion``
    ``1``.
``target``
    A workload accepted by ``kubectl exec`` (for example
    ``deploy/srw-orchestrator``).
``container``
    The target container name.
``commandJson``
    A JSON string array naming the in-image acceptance command. The command
    must implement ``--execute`` and ``--cleanup-only`` modes and write the
    requested evidence path inside the target container.
``capabilitiesJson``
    The exact JSON list in ``REQUIRED_SCENARIOS`` order.

The command is invoked with a unique run id, an explicit destructive
confirmation phrase, and an output path. It must use the live application API,
PostgreSQL, and Kubernetes API and return their aggregate evidence under the
v1 contract validated by ``vm-workspace-recovery-k3d-gate.py``. This driver
always invokes cleanup after an attempted execution. If the release does not
advertise the adapter ConfigMap, it writes an honest structured SKIP result;
the outer gate never converts that into PASS.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import secrets
import subprocess
import sys
from typing import Any, Sequence


ADAPTER_CONFIGMAP = "srw-vm-workspace-recovery-gate-adapter"
PROTOCOL_VERSION = 1
CONFIRMATION = "disposable-vm-workspace-recovery-gate-v1"
REQUIRED_SCENARIOS = [
    "response_loss",
    "leader_overlap",
    "slow_boot",
    "deadline",
    "missing_stop_evidence",
    "forced_deletion",
    "replacement",
]
_CONTEXT = re.compile(r"k3d-srw-vm-recovery-gate-[a-z0-9][a-z0-9-]{2,39}\Z")
_TARGET = re.compile(r"(?:deploy|statefulset)/[a-z0-9][a-z0-9.-]{1,62}\Z")
_CONTAINER = re.compile(r"[a-z0-9][a-z0-9-]{0,62}\Z")


class ScenarioFailure(RuntimeError):
    """The advertised adapter is malformed or failed its live scenario."""


def _run(
    args: Sequence[str], *, check: bool = True
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(list(args), text=True, capture_output=True)
    if check and result.returncode:
        raise ScenarioFailure(f"adapter command failed with exit {result.returncode}")
    return result


def _kubectl(context: str, namespace: str, *args: str) -> list[str]:
    return ["kubectl", "--context", context, "-n", namespace, *args]


def _write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def parse_adapter(resource: dict[str, Any]) -> tuple[str, str, list[str]]:
    """Validate the release-declared adapter before executing any command."""

    data = resource.get("data")
    if not isinstance(data, dict):
        raise ScenarioFailure("adapter ConfigMap has no data")
    if data.get("protocolVersion") != str(PROTOCOL_VERSION):
        raise ScenarioFailure("adapter protocolVersion must be 1")
    target = str(data.get("target") or "")
    container = str(data.get("container") or "")
    if not _TARGET.fullmatch(target):
        raise ScenarioFailure("adapter target must be a Deployment or StatefulSet")
    if not _CONTAINER.fullmatch(container):
        raise ScenarioFailure("adapter container name is invalid")
    try:
        command = json.loads(data.get("commandJson", ""))
        capabilities = json.loads(data.get("capabilitiesJson", ""))
    except json.JSONDecodeError as exc:
        raise ScenarioFailure("adapter JSON fields are invalid") from exc
    if (
        not isinstance(command, list)
        or not command
        or not all(isinstance(item, str) and item for item in command)
    ):
        raise ScenarioFailure("adapter commandJson must be a nonempty string array")
    if capabilities != REQUIRED_SCENARIOS:
        raise ScenarioFailure("adapter does not advertise the complete scenario matrix")
    return target, container, command


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kube-context", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--output", required=True, type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not _CONTEXT.fullmatch(args.kube_context):
        raise ScenarioFailure("refusing a cluster not created by the outer gate")
    lookup = _run(
        _kubectl(
            args.kube_context,
            args.namespace,
            "get",
            "configmap",
            ADAPTER_CONFIGMAP,
            "-o",
            "json",
        ),
        check=False,
    )
    if lookup.returncode:
        _write(
            args.output,
            {
                "gate_status": "skipped",
                "missing_capability": "cluster:vm-workspace-recovery-scenario-adapter-v1",
            },
        )
        return 0
    try:
        resource = json.loads(lookup.stdout)
    except json.JSONDecodeError as exc:
        raise ScenarioFailure("adapter ConfigMap response is invalid JSON") from exc
    if not isinstance(resource, dict):
        raise ScenarioFailure("adapter ConfigMap response is not an object")
    target, container, command = parse_adapter(resource)
    run_id = "vm-recovery-" + secrets.token_hex(6)
    evidence_path = f"/tmp/srw-vm-workspace-recovery-gate/{run_id}.json"
    common = [
        "--run-id",
        run_id,
        "--protocol-version",
        str(PROTOCOL_VERSION),
        "--confirm",
        CONFIRMATION,
    ]
    execute = _kubectl(
        args.kube_context,
        args.namespace,
        "exec",
        target,
        "-c",
        container,
        "--",
        *command,
        "--execute",
        *common,
        "--output",
        evidence_path,
    )
    cleanup = _kubectl(
        args.kube_context,
        args.namespace,
        "exec",
        target,
        "-c",
        container,
        "--",
        *command,
        "--cleanup-only",
        *common,
    )
    try:
        _run(execute)
        evidence_result = _run(
            _kubectl(
                args.kube_context,
                args.namespace,
                "exec",
                target,
                "-c",
                container,
                "--",
                "cat",
                evidence_path,
            )
        )
        try:
            evidence = json.loads(evidence_result.stdout)
        except json.JSONDecodeError as exc:
            raise ScenarioFailure("adapter evidence is invalid JSON") from exc
        if not isinstance(evidence, dict):
            raise ScenarioFailure("adapter evidence is not an object")
        driver = evidence.get("driver")
        if (
            not isinstance(driver, dict)
            or driver.get("protocol_version") != PROTOCOL_VERSION
            or driver.get("scenarios") != REQUIRED_SCENARIOS
            or driver.get("live_sources")
            != ["application_api", "kubernetes_api", "postgresql"]
        ):
            raise ScenarioFailure("in-image adapter did not prove the live protocol")
        _write(args.output, evidence)
    finally:
        _run(cleanup, check=False)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ScenarioFailure as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
