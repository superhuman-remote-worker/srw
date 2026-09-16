#!/usr/bin/env python3
"""Exercise real SIGTERM and rollout recovery on the local Tilt/k3d release.

Run with the repository venv. Uses the disposable k3d test account; set
SRW_K3D_TEST_PASSWORD if its password was changed. Requires idle session and
worker queues; older background pushes may recover alongside the test.
Temporarily lowers the chart's drain budget, then restores the exact local
overlay bytes. Fails on missing evidence, duplicate answers, or any park.

This gate proves generic stateless executor rotation. Retained VM disks and
exact stop evidence require the isolated sibling
``scripts/vm-workspace-recovery-k3d-gate.py``; a successful Pod replacement in
this script is not evidence for VM workspace recovery.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time

import yaml

ROOT = Path(__file__).resolve().parents[1]
K = ["kubectl", "--context=k3d-srw", "-n", "srw"]
_last_overlay = None


def command(args, *, data=None):
    result = subprocess.run(args, input=data, text=True, capture_output=True)
    if result.returncode:
        # Commands can carry an in-flight credential. Never print argv.
        raise RuntimeError(f"command failed (exit {result.returncode})")
    return result.stdout.strip()


def sql(query):
    return command(
        K
        + [
            "exec",
            "srw-postgres-0",
            "--",
            "psql",
            "-U",
            "srw",
            "-d",
            "srw",
            "-v",
            "ON_ERROR_STOP=1",
            "-tAc",
            query,
        ]
    )


def wait_for(label, probe, timeout=420):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        value = probe()
        if value:
            print(f"PASS {label}", flush=True)
            return value
        time.sleep(2)
    raise AssertionError(f"timed out: {label}")


def stateless_setting(key, value):
    global _last_overlay
    overlay = ROOT / "deployment/values-local.yaml"
    source = overlay.read_text()
    # Keep comments and all unrelated private values byte-for-byte.
    match = re.search(r"(?m)^  stateless:\s*\n", source)
    if match is None:
        raise RuntimeError("local overlay has no agent.stateless section")
    end = re.search(r"(?m)^\S|^  \S", source[match.end() :])
    finish = match.end() + end.start() if end else len(source)
    section = source[match.start() : finish]
    if re.search(rf"(?m)^    {key}:", section):
        section = re.sub(rf"(?m)^(    {key}:)\s*[^\s#]+", rf"\g<1> {value}", section)
    else:
        section = section.replace(
            "  stateless:\n", f"  stateless:\n    {key}: {value}\n", 1
        )
    changed = source[: match.start()] + section + source[finish:]
    assert (
        str(yaml.safe_load(changed)["agent"]["stateless"][key]).lower()
        == str(value).lower()
    )
    overlay.write_text(changed)
    _last_overlay = overlay.read_bytes()


def budget(value):
    stateless_setting("shutdownTimeoutSeconds", value)


def converged(value, recovery=None):
    deployment = json.loads(
        command(K + ["get", "deploy", "srw-agent-stateless", "-o", "json"])
    )
    spec, status = deployment["spec"], deployment.get("status", {})
    env = spec["template"]["spec"]["containers"][0]["env"]
    drain = next(x["value"] for x in env if x["name"] == "STATELESS_SHUTDOWN_TIMEOUT_S")
    if recovery is not None and not any(
        x["name"] == "STATELESS_CLOUD_PUSH_RECOVERY_ENABLED"
        and x.get("value") == str(recovery).lower()
        for x in env
    ):
        return False
    pods = json.loads(
        command(K + ["get", "pods", "-l", "srw/class=agent-stateless", "-o", "json"])
    )["items"]
    return (
        drain == str(value)
        and status.get("observedGeneration", 0) >= deployment["metadata"]["generation"]
        and status.get("updatedReplicas") == spec["replicas"]
        and status.get("readyReplicas") == spec["replicas"]
        and len(pods) == spec["replicas"]
        and all(not p["metadata"].get("deletionTimestamp") for p in pods)
    )


def current_runtime(*, recovery=False):
    """Check actual Pod files; a green Tilt resource can retain an old image."""
    agent_files = [
        "src/agent/api/turn_executor.py",
        "src/agent/api/persistent_app.py",
        "src/agent/services/cloud_sync/base.py",
        "src/agent/services/cloud_sync/coordinator.py",
    ]
    if recovery:
        agent_files += [
            "src/agent/api/cloud_push_task.py",
            "src/shared/cloud_push_tasks.py",
        ]
    targets = [("deploy/srw-agent-stateless", "agent", agent_files)]
    if recovery:
        targets.append(
            (
                "deploy/srw-orchestrator",
                "orchestrator",
                [
                    "src/orchestrator/services/cloud_push_recovery.py",
                    "src/shared/cloud_push_tasks.py",
                    "src/orchestrator/database/migrations/app/0233_run_queue_bg_tasks.sql",
                ],
            )
        )
    try:
        for target, container, paths in targets:
            expected = {
                f"/app/{path}": hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
                for path in paths
            }
            probe = (
                "import hashlib,os; from pathlib import Path; "
                f"expected={expected!r}; "
                "assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==h "
                "for p,h in expected.items())"
            )
            if recovery:
                probe += (
                    "; assert os.environ.get('STATELESS_CLOUD_PUSH_RECOVERY_ENABLED')"
                    " == 'true'"
                )
            command(K + ["exec", target, "-c", container, "--", "python", "-c", probe])
        return (
            not recovery
            or sql("SELECT to_regclass('public.run_queue_bg_tasks') IS NOT NULL") == "t"
        )
    except RuntimeError:
        return False


class Gate:
    def __init__(self):
        password = os.environ.get("SRW_K3D_TEST_PASSWORD", "srw-k3d-dev-test")
        result = command(
            K
            + [
                "exec",
                "deploy/srw-orchestrator",
                "-c",
                "orchestrator",
                "--",
                "curl",
                "-fsS",
                "-X",
                "POST",
                "http://srw-keycloak:8080/realms/srw/protocol/openid-connect/token",
                "-d",
                "grant_type=password",
                "-d",
                "client_id=admin-cli",
                "-d",
                "username=test",
                "--data-urlencode",
                f"password={password}",
                "-d",
                "scope=openid",
            ]
        )
        self.token = json.loads(result)["id_token"]

    def api(self, path, body):
        return json.loads(
            command(
                K
                + [
                    "exec",
                    "-i",
                    "deploy/srw-orchestrator",
                    "-c",
                    "orchestrator",
                    "--",
                    "curl",
                    "-fsS",
                    "-X",
                    "POST",
                    f"http://localhost:8085{path}",
                    "-H",
                    f"Authorization: Bearer {self.token}",
                    "-H",
                    "Content-Type: application/json",
                    "--data-binary",
                    "@-",
                ],
                data=json.dumps(body),
            )
        )

    def thread(self, label, prompt):
        result = self.api(
            "/api/persistent/threads",
            {"title": f"resilience gate {label}", "config_name": "session_base"},
        )
        thread = result.get("id") or result["thread_id"]
        print(f"thread {label}={thread}", flush=True)
        self.api(f"/api/persistent/threads/{thread}/input", {"content": prompt})
        return thread

    def state(self, thread):
        result = json.loads(
            sql(
                f"SELECT row_to_json(q) FROM (SELECT state, attempts_since_completion AS attempts, lease_token, leased_by, input_seq, consumed_seq FROM run_queue WHERE unit_id='{thread}') q"
            )
        )
        assert result["state"] != "parked", result
        assert (
            sql(
                f"SELECT count(*) FROM thread_events WHERE thread_id='{thread}' AND kind='turn.parked'"
            )
            == "0"
        )
        return result

    def answered(self, thread, count):
        state = self.state(thread)
        if state["state"] != "done" or state["input_seq"] != state["consumed_seq"]:
            return False
        answers = int(
            sql(
                f"SELECT count(*) FROM thread_messages WHERE thread_id='{thread}' AND role='ai' AND rewound_at IS NULL"
            )
        )
        assert answers == count, f"expected {count} durable answers, got {answers}"
        assert (
            sql(
                f"SELECT count(*) FROM thread_messages WHERE thread_id='{thread}' AND role='ai' AND btrim(content) IN ('', '</think>')"
            )
            == "0"
        ), "model persisted an empty/think-only reply"
        return state

    def verify_cloud_files(self, thread):
        probe = """
import asyncio, hashlib, json, os, sys, tempfile, urllib.request
from pathlib import Path
from agent.api.persistent_app import _build_sync_coordinator
thread = sys.argv[1]
url = os.environ.get("ORCHESTRATOR_URL", "http://srw-orchestrator:8085")
request = urllib.request.Request(url + "/api/agents/threads/" + thread + "/workspace", headers={"X-Internal-Key": os.environ["MCP_INTERNAL_KEY"]})
with urllib.request.urlopen(request, timeout=60) as response:
    payload = json.load(response)
async def verify():
    with tempfile.TemporaryDirectory() as directory:
        coordinator = _build_sync_coordinator(workspace_path=Path(directory), workspace_backend=None, cloud_cfg=payload["cloud_sync"], thread_id=thread, workspace_generation=payload["workspace_generation"])
        mount = next(m for m in coordinator.mounts if m.generation_id == "legacy-session")
        marker = await mount.sync.read_sync_generation_marker(thread_id=thread, sync_scope_sha256=mount.sync_scope_sha256)
        assert marker is not None
        for index in range(80):
            expected = (f"Disposable resilience fixture {index}.\\n" * 20).encode()
            path = f"output/resilience-{index:03}.txt"
            assert marker.committed_manifest[path]["sha256"] == hashlib.sha256(expected).hexdigest()
            if index in (0, 40, 79):
                target = str(Path(directory, str(index)))
                await mount.sync._download_file(path, target)
                assert Path(target).read_bytes() == expected
        await coordinator.aclose()
asyncio.run(verify())
print("cloud marker contains all 80 hashes; three remote file bodies verified")
"""
        print(
            command(
                K
                + [
                    "exec",
                    "-i",
                    "deploy/srw-agent-stateless",
                    "-c",
                    "agent",
                    "--",
                    "python",
                    "-",
                    thread,
                ],
                data=probe,
            ),
            flush=True,
        )

    def sigterm(self):
        thread = self.thread(
            "sigterm",
            "Without tools, write a 120-line numbered poem about tide pools. End with TIDE-DONE.",
        )

        def in_model():
            state = self.state(thread)
            streaming = int(
                sql(
                    f"SELECT count(*) FROM thread_events WHERE thread_id='{thread}' AND kind IN ('thinking','token')"
                )
            )
            settled = int(
                sql(
                    f"SELECT count(*) FROM thread_events WHERE thread_id='{thread}' AND kind='turn.completed'"
                )
            )
            return (
                state
                if state["state"] == "leased" and streaming > 0 and settled == 0
                else False
            )

        first = wait_for("model streaming before SIGTERM", in_model)
        pod = first["leased_by"]
        print(f"SIGTERM owner={pod} token={first['lease_token']}", flush=True)
        command(K + ["delete", "pod", pod, "--wait=false"])
        attempts = 0

        def retried():
            nonlocal attempts
            state = self.state(thread)
            attempts = max(attempts, state["attempts"])
            return (
                state
                if state["lease_token"] > first["lease_token"]
                and state["state"] == "leased"
                else False
            )

        retry = wait_for("successor claim after SIGTERM", retried)
        assert attempts >= 2, retry
        final = wait_for(
            "exactly one durable answer after SIGTERM", lambda: self.answered(thread, 1)
        )
        print(f"SIGTERM evidence attempts={attempts} state={final}", flush=True)

    def pending_push(self, label):
        thread = self.thread(
            label,
            "Without tools, write a 120-line numbered poem about lighthouses. End with ROLLOUT-READY.",
        )
        wait_for(
            "generation armed before fixture writes",
            lambda: sql(
                f"SELECT count(*) FROM thread_cloud_sync_generations WHERE thread_id='{thread}' AND required_generation=1"
            )
            == "1",
        )
        # Populate this disposable thread's virtual workspace after the turn's
        # baseline, in one bulk transfer. This makes the transmit outlast pod
        # startup without relying on model tool behavior or synthetic sleeps.
        pod = self.state(thread)["leased_by"]
        fixture = """
import json, os, sys, tempfile, urllib.request
from pathlib import Path
from shared.runtime.core.backends.rclone import object_store_from_spec
thread = sys.argv[1]
url = os.environ.get("ORCHESTRATOR_URL", "http://srw-orchestrator:8085")
request = urllib.request.Request(url + "/api/agents/threads/" + thread + "/workspace", headers={"X-Internal-Key": os.environ["MCP_INTERNAL_KEY"]})
with urllib.request.urlopen(request, timeout=60) as response:
    payload = json.load(response)
workspace = (payload.get("config_override") or {}).get("workspace") or ((payload.get("resolved_config") or {}).get("agent") or {}).get("workspace")
assert workspace["backend"] == "virtual"
mount = workspace["mounts"][0]
assert mount["prefix"] == "threads/" + thread + "/"
store = object_store_from_spec(mount)
with tempfile.TemporaryDirectory() as directory:
    for index in range(80):
        Path(directory, f"resilience-{index:03}.txt").write_text(f"Disposable resilience fixture {index}.\\n" * 20)
    result = store._run(["copy", directory, store._remote_path(mount["prefix"] + "output/"), "--transfers", "8"], timeout=90)
    assert result.returncode == 0, "fixture transfer failed"
print("80 fixture files written")
"""
        print(
            command(
                K + ["exec", "-i", pod, "-c", "agent", "--", "python", "-", thread],
                data=fixture,
            ),
            flush=True,
        )

        def pending():
            if not self.answered(thread, 1):
                return False
            raw = sql(
                f"SELECT row_to_json(g) FROM (SELECT required_generation, push_owner_token, push_owner_pod, (SELECT count(*) FROM jsonb_object_keys(COALESCE(push_progress->'files','{{}}'::jsonb))) AS files FROM thread_cloud_sync_generations WHERE thread_id='{thread}' AND acknowledged_generation<required_generation AND push_owner_pod IS NOT NULL LIMIT 1) g"
            )
            if not raw:
                return False
            result = json.loads(raw)
            planned = sql(
                f"SELECT push_progress->>'planned' FROM thread_cloud_sync_generations WHERE thread_id='{thread}'"
            )
            return (
                result
                if planned and int(planned) >= 80 and result["files"] > 0
                else False
            )

        first = wait_for("completed turn with transmitting push", pending)
        print(f"rollout push evidence={first}", flush=True)
        return thread, first

    def rollout(self):
        thread, first = self.pending_push("rollout")
        budget(6)  # Tilt applies the chart; do not bypass its image references.
        wait_for("chart rollout converged during push", lambda: converged(6))
        assert (
            sql(
                f"SELECT count(*) FROM thread_cloud_sync_generations WHERE thread_id='{thread}' AND required_generation={first['required_generation']} AND acknowledged_generation<required_generation"
            )
            == "1"
        ), "push finished before rollout; adoption was not exercised"
        self.api(
            f"/api/persistent/threads/{thread}/input",
            {"content": "Without tools, reply exactly ROLLOUT-ADOPTED."},
        )
        wait_for(
            "two total durable replies after rollout",
            lambda: self.answered(thread, 2),
            timeout=660,
        )
        wait_for(
            "all generations acknowledged after rollout",
            lambda: sql(
                f"SELECT count(*) FROM thread_cloud_sync_generations WHERE thread_id='{thread}' AND acknowledged_generation<required_generation"
            )
            == "0",
            timeout=660,
        )
        token = int(
            sql(
                f"SELECT max(push_owner_token) FROM thread_cloud_sync_generations WHERE thread_id='{thread}'"
            )
        )
        assert token > first["push_owner_token"]
        self.verify_cloud_files(thread)
        print(
            f"rollout evidence owner_token={first['push_owner_token']}->{token}, zero parks",
            flush=True,
        )

    def idle(self):
        thread, first = self.pending_push("idle push adoption")
        command(K + ["delete", "pod", first["push_owner_pod"], "--wait=false"])
        killed_at = time.monotonic()

        def scheduled():
            self.state(thread)
            return sql(
                f"SELECT unit_id FROM run_queue_bg_tasks WHERE thread_id='{thread}' LIMIT 1"
            )

        unit = wait_for(
            "idle push scheduled without another message", scheduled, timeout=240
        )
        print(
            f"idle recovery unit={unit} scheduled after {time.monotonic() - killed_at:.1f}s",
            flush=True,
        )

        def completed():
            assert self.answered(thread, 1)
            state = sql(f"SELECT state FROM run_queue WHERE unit_id='{unit}'")
            assert state != "parked", "background task exhausted retries"
            return (
                state == "done"
                and sql(
                    f"SELECT count(*) FROM thread_cloud_sync_generations WHERE thread_id='{thread}' AND acknowledged_generation<required_generation"
                )
                == "0"
            )

        wait_for(
            "idle background task completed and generation acknowledged",
            completed,
            timeout=900,
        )
        assert (
            sql(
                f"SELECT count(*) FROM thread_messages WHERE thread_id='{thread}' AND role='human'"
            )
            == "1"
        )
        assert (
            sql(
                f"SELECT count(*) FROM thread_events WHERE thread_id='{thread}' AND kind='turn.started'"
            )
            == "1"
        )
        assert (
            sql(f"SELECT count(*) FROM run_queue_bg_tasks WHERE thread_id='{thread}'")
            == "1"
        )
        self.verify_cloud_files(thread)
        print(
            f"idle adoption passed: one input, one answer, no new model turn, zero parks; thread={thread}",
            flush=True,
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("case", choices=["sigterm", "rollout", "idle", "all"])
    args = parser.parse_args()
    assert command(["kubectl", "config", "current-context"]) == "k3d-srw"
    assert (
        sql(
            "SELECT count(*) FROM run_queue WHERE state IN ('queued','leased') "
            "AND unit_kind<>'bg_task'"
        )
        == "0"
    ), "session and worker queues must be idle"
    overlay = ROOT / "deployment/values-local.yaml"
    original = overlay.read_bytes()
    value = yaml.safe_load(original)["agent"]["stateless"].get(
        "shutdownTimeoutSeconds", 300
    )
    try:
        if args.case == "idle":
            stateless_setting("cloudPushRecoveryEnabled", "true")
        budget(5)
        wait_for(
            "short drain budget deployed through Tilt",
            lambda: converged(5, recovery=True if args.case == "idle" else None),
            timeout=900,
        )
        wait_for(
            "current code and migration deployed",
            lambda: current_runtime(recovery=args.case == "idle"),
            timeout=600,
        )
        gate = Gate()
        if args.case in ("sigterm", "all"):
            gate.sigterm()
        if args.case in ("rollout", "all"):
            gate.rollout()
        if args.case == "all":
            stateless_setting("cloudPushRecoveryEnabled", "true")
            budget(5)
            wait_for("idle recovery gate deployed", lambda: converged(5, recovery=True))
            wait_for(
                "current recovery code and migration deployed",
                lambda: current_runtime(recovery=True),
                timeout=600,
            )
            gate = Gate()
        if args.case in ("idle", "all"):
            gate.idle()
    finally:
        if _last_overlay is not None and overlay.read_bytes() != _last_overlay:
            raise RuntimeError(
                "Local overlay changed concurrently; refusing to overwrite those changes during restoration"
            )
        overlay.write_bytes(original)
        wait_for("original drain budget restored", lambda: converged(value))


if __name__ == "__main__":
    main()
