#!/usr/bin/env python3
"""Local k3d gate for connector drivers D7: the cloud mount sidecar.

Design: knowledge-base/knowledge/features/connector_drivers.md (D7,
decisions 17, 18 and 22-27) and the D7 in-pod plane spike report.
Templates: scripts/k3d-main-cloud-gate.py and
scripts/k3d-credential-lease-gate.py -- the same safety envelope: dry-run by
default, the exact k3d-srw/srw context, secrets only on ``kubectl exec -i``
stdin and scrubbed from every printed line, every in-pod Python program
capping its own memory, and a cleanup in ``finally`` that touches only what
this run created, followed by a residue check by gate id.

It needs the k3d profile of deployment/values-local.yaml.example
(``connectors.inPodPlane.enabled: true`` with both images built by Tilt), the
bundled Nextcloud with its files on the PVC (``nextcloud.objectStore.enabled:
false``: the gate reads uploaded files on disk), ``docker`` on PATH (the k3d
nodes are containers; the sidecar is killed from its node) and, for the
protected checks, ``agent.protectedCloudModeEnabled: "true"``.

Fixtures (all disposable, all named after the gate id): projects ``rw``,
``missing`` (its Nextcloud group folder renamed before its session starts,
and renamed back before cleanup) and ``prot``; one stateless sandbox session in each (``prot`` protected); one
stateless job in ``rw``. Each session gets one cheap turn ("Reply with the
single word ready.") so its agent attaches.

Checks (each printed PASS/FAIL; the exit status is 0 only if all pass):

  preflight   Tilt reports the srw resource ``ok``; every orchestrator pod
              serves this checkout's D7 modules and every stateless agent pod
              its sidecar watcher, byte for byte; the orchestrator runs with
              both sidecar images; the protected mode is read (its checks run
              only when it is on; ``--require-protected`` fails the gate when
              it is off)
  plane       the rw session's workspace Pod has the opener (privileged,
              native sidecar) and the supervisor (uid 65534, every capability
              dropped, read-only root filesystem, no probe); its plan
              annotation names the project folder; the credential Secret is
              immutable and owned by the Pod; no container has an RCLONE_*
              variable; the workspace container is not privileged, holds no
              SYS_ADMIN and sees no /dev/fuse; its status file says mounted and
              its mount table shows fuse.rclone at /cloud/<folder>; no
              rclone.conf and no copy of the password under the workspace
              home or /srw, nor in the stateless agent logs; nothing listens
              on rclone's remote-control port; after the turn
              threads.metadata.cloud_mount_status says mounted (reported by
              the agent); a write as agent-host reaches Nextcloud
  killed      SIGKILL to rclone inside the supervisor: the folder comes back
              and the workspace never restarts; the supervisor container
              stopped from its node: the kubelet restarts it, the folder comes
              back, the workspace never restarts
  missing     the missing session's workspace is Ready with no restart while
              its folder (whose path was moved away) reads unavailable with
              not_found, in the status file and, after the turn, in the
              thread's state
  protected   (protected mode on) the prot session's Pod keeps the
              workspace's FUSE profile for the capture overlay; the opener
              serves /srw/cloud/lower read-only and creates /srw/cloud/merged
              for agent-host; /cloud/lower is a read-only fuse.rclone mount and
              a write to it fails with EROFS; after the turn /cloud/merged is
              the overlay and a write through it lands in the upper layer; the
              reader credential in the Secret is refused a WebDAV PUT; with
              the reader account disabled and the supervisor restarted, the
              lower reads unavailable with credential_rejected, the workspace
              never restarts, and the next claim starts without cloud with
              the thread's state saying credential_rejected (decision 42;
              the account is enabled again at once)
  teardown    50 MB written into the rw folder, then End at once: End returns
              within the grace period, the Pod is gone with nothing of it in
              the node's mount table, its plan ConfigMap and credential Secret
              are garbage-collected, and the file is complete in Nextcloud
  regression  a stateless job's workspace Pod has no sidecars and no plan
  cleanup     nothing this run created is left: sessions, the job, projects,
              pods, plan ConfigMaps and credential Secrets; a moved group folder
              is moved back and a disabled reader account enabled again

Run with the repository venv on the k3d-srw cluster, alone: this is a
mutating gate.

  .venv/bin/python scripts/k3d-cloud-mount-sidecar-gate.py           # plan
  .venv/bin/python scripts/k3d-cloud-mount-sidecar-gate.py \\
      --run --confirm LOCAL-K3D-DISPOSABLE [--require-protected]
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import secrets
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
LOCAL_CONTEXT = "k3d-srw"
LOCAL_NAMESPACE = "srw"
LOCAL_CONFIRMATION = "LOCAL-K3D-DISPOSABLE"
K = ["kubectl", f"--context={LOCAL_CONTEXT}", "-n", LOCAL_NAMESPACE]
ORCHESTRATOR = "deploy/srw-orchestrator"
ORCHESTRATOR_CONTAINER = "orchestrator"
AGENT_CONTAINER = "agent"
POSTGRES_POD = "srw-postgres-0"
NEXTCLOUD = "deploy/srw-nextcloud"
NEXTCLOUD_CONTAINER = "nextcloud"
WORKSPACE_CONTAINER = "workspace"
OPENER_CONTAINER = "srw-fuse-opener"
SUPERVISOR_CONTAINER = "srw-cloud-mount"
PLAN_ANNOTATION = "srw.io/cloud-mount-plan"
CREDENTIAL_VOLUME = "srw-cloud-credential"
CLOUD_VOLUME = "srw-cloud"
CREDENTIAL_FILE = "/etc/srw-cloud/rclone.conf"
HOME = "/home/agent-host"
KEYCLOAK_TOKEN_URL = "http://srw-keycloak:8080/realms/srw/protocol/openid-connect/token"
POD_ROOT = "/app"
DEFAULT_MODEL = "muse-spark-1.3-contributor"
TURN_PROMPT = "Reply with the single word ready."
#: rclone's default remote-control port: nothing may listen there.
RCLONE_RC_PORT = 5572
#: The workspace Pod's terminationGracePeriodSeconds.
GRACE_SECONDS = 120
CAP_SYS_ADMIN = 21
LARGE_FILE_BYTES = 50 * 1024 * 1024
_SELECTOR = (
    "app.kubernetes.io/instance=srw,app.kubernetes.io/name=superhuman-remote-worker"
)
_GATE_ID_RE = re.compile(r"d7-[0-9a-f]{10}\Z")
_MODEL_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@-]{0,199}\Z")
_USER_RE = re.compile(r"[a-z][a-z0-9._-]{0,63}\Z")
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")
_NODE_RE = re.compile(r"k3d-srw-(server|agent)-[0-9]+\Z")
_CONTAINER_ID_RE = re.compile(r"[0-9a-f]{12,64}\Z")
_FOLDER_ID_RE = re.compile(r"[0-9]{1,12}\Z")
_READER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._@-]{0,127}\Z")
_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._ -]{0,127}\Z")

#: What every orchestrator pod must serve byte for byte.
SERVED = (
    "src/orchestrator/services/in_pod_mount.py",
    "src/orchestrator/services/cloud_mount_plan.py",
    "src/orchestrator/services/cloud_mount_sidecar.py",
    "src/orchestrator/services/cloud_mount_status.py",
    "src/orchestrator/services/agent_cloud_mounts.py",
    "src/orchestrator/services/container_provisioner.py",
    "src/orchestrator/services/thread_workspace_delivery.py",
    "src/orchestrator/services/thread_admission.py",
    "src/orchestrator/services/stateless_session_retirement.py",
    "src/orchestrator/routers/agent_thread_workspace.py",
    "src/orchestrator/application/workspace.py",
    "src/orchestrator/application/lifecycle.py",
)
#: What every stateless agent pod must serve.
AGENT_SERVED = (
    "src/shared/runtime/services/cloud_mount/sidecar.py",
    "src/agent/api/persistent_session.py",
    "src/agent/api/session_attach.py",
    "src/agent/api/session_workspace.py",
    "src/agent/api/orchestrator_client.py",
)
_SECRETS: list[str] = []


class GateError(RuntimeError):
    """Infrastructure trouble: the gate could not observe the product."""


class SafetyError(RuntimeError):
    """The requested run is outside the local disposable boundary."""


def _scrub(text: str) -> str:
    for value in _SECRETS:
        if value:
            text = text.replace(value, "<redacted>")
    return text


def secret(value: str) -> str:
    _SECRETS.append(value)
    return value


def run(
    args: list[str], *, data: str | None = None, timeout: int = 180
) -> tuple[int, str, str]:
    """Run argv; never echo argv or stdin. Output comes back scrubbed."""
    label = " ".join(args[4:7]) if args[:1] == ["kubectl"] else " ".join(args[:3])
    try:
        result = subprocess.run(
            args, input=data, text=True, capture_output=True, timeout=timeout
        )
    except subprocess.TimeoutExpired:
        raise GateError(f"{label[:80]} timed out after {timeout}s") from None
    return result.returncode, _scrub(result.stdout.strip()), _scrub(result.stderr)


def command(args: list[str], *, data: str | None = None, timeout: int = 180) -> str:
    rc, out, err = run(args, data=data, timeout=timeout)
    if rc:
        label = " ".join(args[4:7]) if args[:1] == ["kubectl"] else " ".join(args[:3])
        raise GateError(f"{label[:80]} failed (exit {rc}): {err.strip()[-400:]}")
    return out


def sql(query: str) -> str:
    """One statement on the app database (no secret may be in ``query``)."""
    return command(
        K
        + ["exec", POSTGRES_POD, "--", "psql", "-U", "srw", "-d", "srw"]
        + ["-v", "ON_ERROR_STOP=1", "-tAc", query]
    )


def sql_json(query: str) -> Any:
    out = sql(query)
    return json.loads(out) if out else None


def lit(text: str) -> str:
    return "'" + str(text).replace("'", "''") + "'"


def wait_for(
    label: str, probe: Callable[[], Any], *, timeout: int, interval: float = 3.0
):
    deadline = time.monotonic() + timeout
    while True:
        value = probe()
        if value:
            return value
        if time.monotonic() >= deadline:
            raise GateError(f"timed out: {label}")
        time.sleep(interval)


# ---------------------------------------------------------------------------
# Programs run inside pods (stdin carries every secret)
# ---------------------------------------------------------------------------

# Every Python program the gate runs inside a pod calls cap_memory() once its
# imports are done: a gate program that grows must fail the gate with a
# MemoryError, never take the pod to the OOM killer (the helper of
# scripts/k3d-connector-drivers-gate.py).
POD_MEMORY_BUDGET = 256 << 20
_POD_MEMORY_CAP = (
    r"""
import resource as _srw_resource
def cap_memory(budget=%d):
    with open("/proc/self/status") as status:
        data = next(
            int(line.split()[1]) * 1024
            for line in status
            if line.startswith("VmData:")
        )
    limit = data + budget
    _soft, hard = _srw_resource.getrlimit(_srw_resource.RLIMIT_DATA)
    if hard != _srw_resource.RLIM_INFINITY:
        limit = min(limit, hard)  # an inherited cap is only ever tightened
    _srw_resource.setrlimit(_srw_resource.RLIMIT_DATA, (limit, limit))
"""
    % POD_MEMORY_BUDGET
)

_API_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import json, sys, urllib.error, urllib.parse, urllib.request
cap_memory()
envelope = json.loads(sys.stdin.readline())
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
form = urllib.parse.urlencode({
    "grant_type": "password", "client_id": "admin-cli", "scope": "openid",
    "username": envelope["username"], "password": envelope["password"],
}).encode()
with opener.open(envelope["token_url"], data=form, timeout=30) as response:
    token = json.load(response)["id_token"]
body = envelope.get("body")
request = urllib.request.Request(
    "http://localhost:8085" + envelope["path"],
    data=None if body is None else json.dumps(body).encode(),
    method=envelope["method"],
    headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
)
try:
    with opener.open(request, timeout=envelope.get("timeout") or 180) as response:
        status, text = response.status, response.read().decode("utf-8", "replace")
except urllib.error.HTTPError as error:
    status, text = error.code, error.read().decode("utf-8", "replace")
print(json.dumps({"status": status, "body": text}))
"""
)

_HASH_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import hashlib, json, sys
from pathlib import Path
cap_memory()
root = Path(sys.argv[1])
expected = json.loads(sys.stdin.read())
stale = sorted(
    path for path, digest in expected.items()
    if not (root / path).is_file()
    or hashlib.sha256((root / path).read_bytes()).hexdigest() != digest
)
print(json.dumps({"stale": stale}))
"""
)

# One look at a workspace container, in sections, with nothing secret in it.
_INSPECT_SCRIPT = r"""
echo "==fuse"
if [ -e /dev/fuse ]; then echo present; else echo absent; fi
echo "==capeff"
grep '^CapEff:' /proc/1/status
echo "==mountinfo"
cat /proc/self/mountinfo
echo "==status"
for f in /srw/cloud-status/*.json; do
  [ -f "$f" ] || continue
  echo "--$(basename "$f" .json)"
  cat "$f"
  echo
done
echo "==tcp"
cat /proc/net/tcp /proc/net/tcp6 2>/dev/null
echo "==rclone-conf"
find "$1" /srw -xdev -name rclone.conf 2>/dev/null
echo "==end"
"""

# A shell for a workspace container, run as agent-host: ``$1`` is the command.
_AS_AGENT = 'exec su -s /bin/sh agent-host -c "$1"'

# Files the main cloud stored for a group folder, by name: ``$1`` is the
# folder id, ``$2`` the file name. Both layouts of the groupfolders app
# (``__groupfolders/<id>/`` and ``__groupfolders/<id>/files/``) are searched.
_NEXTCLOUD_FIND = r"""
data=$(php /var/www/html/occ config:system:get datadirectory)
find "$data/__groupfolders/$1" -type f -name "$2" -exec sha256sum {} + 2>/dev/null \
  | while read -r digest path; do echo "$digest $(stat -c %s "$path")"; done
"""


def in_orchestrator(program: str, payload: Any, *, timeout: int = 300) -> dict:
    data = payload if isinstance(payload, str) else json.dumps(payload) + "\n"
    out = command(
        K
        + ["exec", "-i", ORCHESTRATOR, "-c", ORCHESTRATOR_CONTAINER, "--"]
        + ["python", "-c", program],
        data=data,
        timeout=timeout,
    )
    return json.loads(out.splitlines()[-1])


class Api:
    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self.password = secret(password)

    def call(
        self, method: str, path: str, body: Any = None, *, timeout: int = 180
    ) -> tuple[int, Any]:
        result = in_orchestrator(
            _API_PROGRAM,
            {
                "username": self.username,
                "password": self.password,
                "token_url": KEYCLOAK_TOKEN_URL,
                "method": method,
                "path": path,
                "body": body,
                "timeout": timeout,
            },
            timeout=timeout + 60,
        )
        text = _scrub(result.get("body") or "")
        try:
            parsed = json.loads(text) if text.strip() else {}
        except json.JSONDecodeError:
            parsed = {"raw": text[:400]}
        return int(result["status"]), parsed

    def ok(self, method: str, path: str, body: Any = None) -> Any:
        status, parsed = self.call(method, path, body)
        if status not in (200, 201, 202, 204):
            raise GateError(f"{method} {path} -> HTTP {status}: {str(parsed)[:300]}")
        return parsed


def expected_bytes(paths: tuple[str, ...]) -> dict[str, str]:
    return {
        path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() for path in paths
    }


# ---------------------------------------------------------------------------
# What the slice says (pure, unit-tested)
# ---------------------------------------------------------------------------

# rclone's fixed obscuring key (lib/obscure), as
# orchestrator.services.cloud_mount_plan holds it.
_OBSCURE_KEY = bytes(
    [
        0x9C, 0x93, 0x5B, 0x48, 0x73, 0x0A, 0x55, 0x4D,
        0x6B, 0xFD, 0x7C, 0x63, 0xC8, 0x86, 0xA9, 0x2B,
        0xD3, 0x90, 0x19, 0x8E, 0xB8, 0x12, 0x8A, 0xFB,
        0xF4, 0xDE, 0x16, 0x2B, 0x8B, 0x95, 0xF6, 0x38,
    ]
)  # fmt: skip


def reveal(obscured: str) -> str:
    """What ``rclone reveal`` prints."""
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    raw = base64.urlsafe_b64decode(obscured + "=" * (-len(obscured) % 4))
    decryptor = Cipher(algorithms.AES(_OBSCURE_KEY), modes.CTR(raw[:16])).decryptor()
    return (decryptor.update(raw[16:]) + decryptor.finalize()).decode("utf-8")


def parse_rclone_conf(text: str) -> dict[str, dict[str, str]]:
    sections: dict[str, dict[str, str]] = {}
    current: dict[str, str] | None = None
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("[") and line.endswith("]"):
            current = sections.setdefault(line[1:-1], {})
        elif current is not None and "=" in line:
            key, value = line.split("=", 1)
            current[key.strip()] = value.strip()
    return sections


def sections(text: str) -> dict[str, str]:
    """The inspect script's output by section."""
    out: dict[str, list[str]] = {}
    name = None
    for line in text.splitlines():
        if line.startswith("=="):
            name = line[2:]
            out[name] = []
        elif name is not None:
            out[name].append(line)
    return {key: "\n".join(value) for key, value in out.items()}


def has_cap(capeff_line: str, bit: int) -> bool:
    match = re.search(r"CapEff:\s*([0-9a-fA-F]+)", capeff_line)
    if match is None:
        raise GateError("no CapEff line")
    return bool(int(match[1], 16) >> bit & 1)


def top_mounts(mountinfo: str) -> dict[str, tuple[str, str]]:
    """``{mountpoint: (fstype, per-mount options)}`` for the top of each stack."""
    tops: dict[str, tuple[str, str]] = {}
    for line in mountinfo.splitlines():
        fields = line.split()
        if len(fields) < 7 or "-" not in fields[6:]:
            continue
        separator = fields.index("-", 6)
        if separator + 1 < len(fields):
            point = re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), fields[4])
            tops[point] = (fields[separator + 1], fields[5])
    return tops


def listening_ports(proc_net_tcp: str) -> set[int]:
    ports: set[int] = set()
    for line in proc_net_tcp.splitlines():
        fields = line.split()
        if len(fields) > 3 and fields[3] == "0A" and ":" in fields[1]:
            ports.add(int(fields[1].rsplit(":", 1)[1], 16))
    return ports


def status_files(text: str) -> dict[int, dict[str, Any]]:
    """The supervisor's status files from the inspect script's section."""
    files: dict[int, dict[str, Any]] = {}
    index: int | None = None
    body: list[str] = []

    def flush() -> None:
        if index is None:
            return
        try:
            parsed = json.loads("\n".join(body) or "{}")
        except json.JSONDecodeError:
            parsed = {"state": "unparsable"}
        files[index] = parsed if isinstance(parsed, dict) else {}

    for line in text.splitlines():
        if line.startswith("--") and line[2:].isdigit():
            flush()
            index, body = int(line[2:]), []
        else:
            body.append(line)
    flush()
    return files


def container(pod: dict, name: str) -> dict | None:
    spec = pod.get("spec") or {}
    for item in [*(spec.get("initContainers") or []), *(spec.get("containers") or [])]:
        if item.get("name") == name:
            return item
    return None


def restarts(pod: dict, name: str) -> int:
    status = pod.get("status") or {}
    for item in [
        *(status.get("initContainerStatuses") or []),
        *(status.get("containerStatuses") or []),
    ]:
        if item.get("name") == name:
            return int(item.get("restartCount") or 0)
    raise GateError(f"no status for container {name}")


def recorded_plan(pod: dict) -> dict[str, Any] | None:
    raw = ((pod.get("metadata") or {}).get("annotations") or {}).get(PLAN_ANNOTATION)
    try:
        plan = json.loads(raw) if raw else None
    except json.JSONDecodeError:
        return None
    return plan if isinstance(plan, dict) else None


def credential_secret_name(pod: dict) -> str | None:
    for volume in (pod.get("spec") or {}).get("volumes") or []:
        if volume.get("name") == CREDENTIAL_VOLUME:
            return (volume.get("secret") or {}).get("secretName")
    return None


def sidecar_pod_problems(pod: dict, *, protected: bool) -> list[str]:
    """What a workspace Pod with the in-pod plane must look like."""
    problems: list[str] = []
    opener = container(pod, OPENER_CONTAINER)
    supervisor = container(pod, SUPERVISOR_CONTAINER)
    workspace = container(pod, WORKSPACE_CONTAINER)
    if opener is None or supervisor is None or workspace is None:
        return ["the opener, the supervisor or the workspace is missing"]
    init_names = [c.get("name") for c in pod["spec"].get("initContainers") or []]
    if init_names.index(OPENER_CONTAINER) > init_names.index(SUPERVISOR_CONTAINER):
        problems.append("the opener does not start first")
    for sidecar in (opener, supervisor):
        if sidecar.get("restartPolicy") != "Always":
            problems.append(f"{sidecar['name']} is not a native sidecar")
    if not (opener.get("securityContext") or {}).get("privileged"):
        problems.append("the opener is not privileged")
    context = supervisor.get("securityContext") or {}
    if (
        context.get("runAsUser") != 65534
        or context.get("privileged")
        or context.get("allowPrivilegeEscalation") is not False
        or context.get("readOnlyRootFilesystem") is not True
        or (context.get("capabilities") or {}).get("drop") != ["ALL"]
    ):
        problems.append("the supervisor is not unprivileged")
    if any(supervisor.get(probe) for probe in ("startupProbe", "readinessProbe")):
        problems.append("the supervisor has a probe")
    for item in (opener, supervisor, workspace):
        names = [env.get("name", "") for env in item.get("env") or []]
        if any(name.startswith("RCLONE_") for name in names):
            problems.append(f"{item['name']} has an RCLONE_ variable")
    workspace_context = workspace.get("securityContext") or {}
    added = [
        str(cap).upper().removeprefix("CAP_")
        for cap in (workspace_context.get("capabilities") or {}).get("add") or []
    ]
    can_mount = bool(workspace_context.get("privileged")) or "SYS_ADMIN" in added
    if can_mount != protected:
        problems.append(
            "the workspace " + ("lost" if protected else "kept") + " its FUSE profile"
        )
    views = {m["name"]: m for m in workspace.get("volumeMounts") or []}
    view = views.get(CLOUD_VOLUME) or {}
    if view.get("mountPath") != "/cloud" or view.get("mountPropagation") != (
        "HostToContainer"
    ):
        problems.append("the workspace does not see the cloud volume at /cloud")
    if bool(view.get("readOnly")) == protected:
        problems.append(
            "the workspace's /cloud view is "
            + ("read-only" if protected else "writable")
        )
    if CREDENTIAL_VOLUME in views:
        problems.append("the workspace mounts the credential")
    if recorded_plan(pod) is None:
        problems.append("no plan annotation")
    return problems


def evaluate_status(
    status: Any, name: str, *, state: str, reason: str | None = None
) -> tuple[bool, str]:
    """Whether ``threads.metadata.cloud_mount_status`` reports ``name`` so."""
    entry = ((status or {}).get("mounts") or {}).get(name) or {}
    ok = (
        entry.get("state") == state
        and entry.get("reason") == reason
        and entry.get("reported_by") == "agent"
    )
    return ok, json.dumps(
        {key: entry.get(key) for key in ("state", "reason", "reported_by")}
    )


@dataclass
class Report:
    gate_id: str
    results: list[tuple[str, bool, str]] = field(default_factory=list)

    def check(self, name: str, ok: bool, detail: str = "") -> bool:
        detail = _scrub(detail)
        self.results.append((name, bool(ok), detail))
        print(
            f"{'PASS' if ok else 'FAIL'} {name}{': ' + detail if detail else ''}",
            flush=True,
        )
        return bool(ok)

    def note(self, text: str) -> None:
        print(f"NOTE {_scrub(text)}", flush=True)

    @property
    def passed(self) -> bool:
        return bool(self.results) and all(ok for _, ok, _ in self.results)


PLAN = [
    "preflight: Tilt srw ok; orchestrator pods serve this checkout's D7 modules "
    "and stateless agent pods its sidecar watcher, byte for byte; both sidecar "
    "images configured; protected mode read (its checks need it on)",
    "plane: the rw session's Pod has the opener and the unprivileged "
    "supervisor, a plan annotation, an immutable credential Secret owned by the "
    "Pod; the workspace has no privilege, SYS_ADMIN or /dev/fuse; mounted in "
    "the status file, the mount table and, after one turn, the thread state; "
    "no rclone.conf or password copy in the workspace or the agent logs; "
    "nothing on rclone's rc port; a write as agent-host reaches Nextcloud",
    "killed: SIGKILL to rclone, then the supervisor stopped from its node: "
    "the folder comes back each time and the workspace never restarts",
    "missing: a session whose group folder was moved away first: workspace Ready "
    "with no restart, the folder unavailable with not_found (status file and "
    "thread state)",
    "protected: the prot session keeps the workspace FUSE profile; the lower "
    "is a read-only sidecar mount (EROFS for agent-host); the overlay at "
    "/cloud/merged captures a write in its upper layer; the reader credential "
    "is refused a WebDAV PUT; reader disabled + supervisor restarted -> "
    "credential_rejected, no workspace restart, the next claim starts without "
    "cloud and says so (decision 42)",
    "teardown: 50 MB written, End at once: End within the grace period, the "
    "Pod gone with nothing in the node's mount table, its ConfigMap and "
    "Secret collected, the file complete in Nextcloud",
    "regression: a stateless job's workspace Pod has no sidecars and no plan",
    "cleanup: sessions, the job, projects, pods, plan ConfigMaps and "
    "credential Secrets; a moved group folder moved back and a disabled reader "
    "enabled again; residue check by gate id",
]


@dataclass
class Session:
    label: str
    project: str
    folder_id: str
    thread: str = ""
    pod: str = ""
    pod_uid: str = ""
    objects: str = ""
    mount: str = ""


class CloudMountSidecarGate:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.gate_id = args.gate_id or f"d7-{secrets.token_hex(5)}"
        self.report = Report(self.gate_id)
        self.api = Api(args.user, args.password)
        self.started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.user_id = ""
        self.protected_mode = False
        self.projects: dict[str, str] = {}  # label -> project id
        self.sessions: dict[str, Session] = {}
        self.jobs: dict[str, str] = {}
        self.objects: set[str] = set()  # plan ConfigMap / credential Secret names
        self.disabled_reader: str | None = None
        self.moved_folders: dict[str, str] = {}  # group folder id -> mountpoint

    # -- naming ------------------------------------------------------------
    def name(self, label: str) -> str:
        return f"{self.gate_id} {label}"

    def title(self, label: str) -> str:
        return f"D7 cloud mount sidecar gate {self.gate_id} {label}"

    # -- cluster reads -------------------------------------------------------
    def pods(self, component: str) -> list[dict]:
        listing = json.loads(
            command(
                K
                + ["get", "pods", "-l"]
                + [f"{_SELECTOR},app.kubernetes.io/component={component}"]
                + ["-o", "json"]
            )
        )["items"]
        return [pod for pod in listing if not pod["metadata"].get("deletionTimestamp")]

    def pod_json(self, name: str) -> dict | None:
        rc, out, _err = run(K + ["get", "pod", name, "-o", "json"], timeout=60)
        return json.loads(out) if rc == 0 and out else None

    def workspace_pod(self, selector: str, prefix: str, *, timeout: int) -> dict:
        def probe() -> dict | None:
            items = json.loads(
                command(K + ["get", "pods", "-l", selector, "-o", "json"])
            )["items"]
            ready = [
                pod
                for pod in items
                if pod["metadata"]["name"].startswith(prefix)
                and not pod["metadata"].get("deletionTimestamp")
                and pod.get("status", {}).get("phase") == "Running"
                and any(
                    status.get("name") == WORKSPACE_CONTAINER and status.get("ready")
                    for status in pod.get("status", {}).get("containerStatuses") or []
                )
            ]
            return ready[0] if len(ready) == 1 else None

        return wait_for(f"workspace pod {selector}", probe, timeout=timeout)

    def served_problems(self, component: str, container_name: str, paths) -> list[str]:
        problems: list[str] = []
        pods = self.pods(component)
        if not pods:
            problems.append(f"no {component} pod")
        for pod in pods:
            name = pod["metadata"]["name"]
            found = json.loads(
                command(
                    K
                    + ["exec", "-i", name, "-c", container_name, "--"]
                    + ["python", "-c", _HASH_PROGRAM, POD_ROOT],
                    data=json.dumps(expected_bytes(paths)),
                ).splitlines()[-1]
            )
            if found.get("stale"):
                problems.append(f"{name} stale: {found['stale'][:6]}")
        return problems

    def orchestrator_env(self, name: str) -> str:
        rc, out, _err = run(
            K
            + ["exec", ORCHESTRATOR, "-c", ORCHESTRATOR_CONTAINER, "--"]
            + ["printenv", name],
            timeout=60,
        )
        return out.strip() if rc == 0 else ""

    def ws(self, pod: str, script: str, *args: str, data: str | None = None) -> str:
        rc, out, err = run(
            K
            + ["exec", "-i", pod, "-c", WORKSPACE_CONTAINER, "--"]
            + ["sh", "-c", script, "gate", *args],
            data=data,
            timeout=300,
        )
        if rc:
            raise GateError(f"workspace command failed on {pod}: {err.strip()[-300:]}")
        return out

    def as_agent(self, pod: str, shell: str) -> tuple[int, str, str]:
        """``shell`` as agent-host in the workspace (no secret in it)."""
        return run(
            K
            + ["exec", pod, "-c", WORKSPACE_CONTAINER, "--"]
            + ["sh", "-c", _AS_AGENT, "gate", shell],
            timeout=300,
        )

    def inspect(self, pod: str) -> dict[str, str]:
        return sections(self.ws(pod, _INSPECT_SCRIPT, HOME))

    def status_entry(self, pod: str, index: int) -> dict[str, Any]:
        return status_files(self.inspect(pod).get("status", "")).get(index) or {}

    def wait_status_file(
        self, pod: str, index: int, state: str, *, timeout: int = 180
    ) -> dict:
        return wait_for(
            f"status file {index} {state} on {pod}",
            lambda: (
                entry
                if (entry := self.status_entry(pod, index)).get("state") == state
                else None
            ),
            timeout=timeout,
            interval=4,
        )

    def folder_back(self, session: Session, readable: str) -> bool:
        return (
            self.as_agent(session.pod, readable)[0] == 0
            and self.status_entry(session.pod, 0).get("state") == "mounted"
        )

    def rw_session(self) -> Session:
        session = self.sessions.get("rw")
        if session is None or not session.pod or not session.mount:
            raise GateError("the rw session did not start")
        return session

    def thread_status(self, thread: str) -> Any:
        return sql_json(
            "SELECT metadata->'cloud_mount_status' FROM threads WHERE id = "
            f"{lit(thread)}"
        )

    def nextcloud(self, script: str, *args: str) -> str:
        return command(
            K
            + ["exec", NEXTCLOUD, "-c", NEXTCLOUD_CONTAINER, "--"]
            + ["su", "-s", "/bin/sh", "www-data", "-c", script, "gate", *args],
            timeout=180,
        )

    def occ(self, arguments: list[str]) -> int:
        rc, _out, _err = run(
            K
            + ["exec", NEXTCLOUD, "-c", NEXTCLOUD_CONTAINER, "--"]
            + ["su", "-s", "/bin/sh", "www-data", "-c"]
            + ['exec php /var/www/html/occ "$@"', "occ", *arguments],
            timeout=180,
        )
        return rc

    def nextcloud_file(self, folder_id: str, name: str) -> tuple[str, int] | None:
        if not _FOLDER_ID_RE.fullmatch(folder_id) or not _NAME_RE.fullmatch(name):
            raise GateError("refusing an unexpected folder id or file name")
        lines = self.nextcloud(_NEXTCLOUD_FIND, folder_id, name).splitlines()
        if len(lines) != 1:
            return None
        digest, size = lines[0].split()
        return digest, int(size)

    def node_of(self, pod: dict) -> str:
        node = (pod.get("spec") or {}).get("nodeName") or ""
        if not _NODE_RE.fullmatch(node):
            raise GateError(f"refusing node {node!r}: not a k3d-srw node")
        if not shutil.which("docker"):
            raise GateError("docker is not on PATH: the node cannot be reached")
        return node

    def stop_supervisor_from_node(self, pod: dict) -> None:
        node = self.node_of(pod)
        uid = pod["metadata"]["uid"]
        ids = command(
            ["docker", "exec", node, "crictl", "ps", "-q"]
            + [
                "--name",
                SUPERVISOR_CONTAINER,
                "--label",
                f"io.kubernetes.pod.uid={uid}",
            ]
        ).split()
        if len(ids) != 1 or not _CONTAINER_ID_RE.fullmatch(ids[0]):
            raise GateError(f"expected one running supervisor container, got {ids}")
        command(["docker", "exec", node, "crictl", "stop", "--timeout", "0", ids[0]])

    def node_mounts_of(self, node: str, uid: str) -> int:
        rc, out, _err = run(
            ["docker", "exec", node, "grep", "-c", "-F", uid, "/proc/mounts"],
            timeout=60,
        )
        if rc not in (0, 1):
            raise GateError(f"could not read {node}'s mount table")
        return int(out or "0")

    # -- fixtures -----------------------------------------------------------
    def create_project(self, label: str) -> tuple[str, str, str]:
        created = self.api.ok(
            "POST",
            "/api/projects",
            {
                "name": self.name(label),
                "description": "D7 cloud mount sidecar gate (disposable)",
                "user_id": self.user_id,
            },
        )
        project = self.projects[label] = str(created["id"])
        handle = wait_for(
            f"project {label}'s main-cloud folder",
            lambda: sql(
                "SELECT main_cloud_folder_handle FROM projects WHERE id = "
                f"{lit(project)} AND main_cloud_folder_handle IS NOT NULL"
            ),
            timeout=180,
            interval=5,
        )
        parsed = json.loads(handle)
        folder_id = str(parsed.get("native_id") or "")
        mountpoint = str((parsed.get("vendor_meta") or {}).get("mountpoint") or "")
        if not _FOLDER_ID_RE.fullmatch(folder_id) or not _NAME_RE.fullmatch(mountpoint):
            raise GateError(f"project {label} has no group folder id or mountpoint")
        return project, folder_id, mountpoint

    def start_session(
        self, label: str, *, protected: bool = False, folder_moved: bool = False
    ) -> Session:
        project, folder_id, mountpoint = self.create_project(label)
        session = self.sessions[label] = Session(label, project, folder_id)
        if folder_moved:
            # The folder's path is gone; cleanup moves it back so the project
            # deletes as any other.
            self.moved_folders[folder_id] = mountpoint
            if self.occ(["groupfolders:rename", folder_id, f"{self.gate_id}-moved"]):
                raise GateError(f"could not move group folder {folder_id}")
        body: dict[str, Any] = {
            "title": self.title(label),
            "permission_mode": "autonomous",
            "project_id": project,
            "config_override": {"workspace": {"backend": "sandbox"}},
            "model": self.args.model,
        }
        if protected:
            body["protected_cloud"] = True
        created = self.api.ok("POST", "/api/persistent/threads", body)
        session.thread = str(created.get("thread_id") or created["id"])
        print(f"session {label} {session.thread}", flush=True)
        lane = sql(
            f"SELECT execution_lane FROM threads WHERE id = {lit(session.thread)}"
        )
        if lane != "stateless":
            raise GateError(f"session lane is {lane!r}, not stateless")
        pod = self.workspace_pod(
            f"srw/thread-id={session.thread}",
            "ws-thread-",
            timeout=self.args.pod_timeout,
        )
        session.pod = pod["metadata"]["name"]
        session.pod_uid = pod["metadata"]["uid"]
        session.objects = credential_secret_name(pod) or ""
        if session.objects:
            self.objects.add(session.objects)
        plan = recorded_plan(pod) or {}
        mounts = plan.get("mounts") or []
        if len(mounts) == 1:
            session.mount = str(mounts[0].get("name") or "")
        return session

    def turn(self, session: Session) -> None:
        self.api.ok(
            "POST",
            f"/api/persistent/threads/{session.thread}/input",
            {"content": TURN_PROMPT},
        )

    def wait_thread_state(
        self, session: Session, state: str, reason: str | None = None
    ) -> tuple[bool, str]:
        detail = ""

        def probe() -> bool:
            nonlocal detail
            ok, detail = evaluate_status(
                self.thread_status(session.thread),
                session.mount,
                state=state,
                reason=reason,
            )
            return ok

        try:
            wait_for(
                "thread cloud mount state",
                probe,
                timeout=self.args.turn_timeout,
                interval=5,
            )
        except GateError:
            return False, detail
        return True, detail

    # -- phases ------------------------------------------------------------
    def preflight(self) -> None:
        if shutil.which("tilt"):
            rc, status, _err = run(
                ["tilt", "get", "uiresource", "srw", "-o"]
                + ["jsonpath={.status.updateStatus}"],
                timeout=30,
            )
            if rc or status != "ok":
                raise GateError(f"Tilt srw update status is {status or 'unknown'!r}")
        else:
            self.report.note("tilt not on PATH; rollout state not read")
        problems = self.served_problems("orchestrator", ORCHESTRATOR_CONTAINER, SERVED)
        problems += self.served_problems(
            "agent-stateless", AGENT_CONTAINER, AGENT_SERVED
        )
        self.report.check(
            "preflight: every orchestrator and stateless agent pod serves this "
            "checkout's D7 code",
            not problems,
            "; ".join(problems)[:600],
        )
        if problems:
            raise GateError("the deployment does not serve this checkout")
        opener = self.orchestrator_env("CONNECTOR_IN_POD_OPENER_IMAGE")
        rclone = self.orchestrator_env("CONNECTOR_IN_POD_RCLONE_IMAGE")
        self.report.check(
            "preflight: the orchestrator runs with both sidecar images",
            bool(opener and rclone),
            f"opener {opener or '-'} rclone {rclone or '-'}",
        )
        if not (opener and rclone):
            raise GateError("connectors.inPodPlane is off")
        self.protected_mode = self.orchestrator_env(
            "PROTECTED_CLOUD_MODE_ENABLED"
        ).lower() in ("true", "1", "yes")
        if self.args.require_protected:
            self.report.check(
                "preflight: protected cloud mode is on",
                self.protected_mode,
                "agent.protectedCloudModeEnabled",
            )
        elif not self.protected_mode:
            self.report.note(
                "protected cloud mode is off: the protected checks are skipped "
                '(set agent.protectedCloudModeEnabled: "true" and pass '
                "--require-protected to run them)"
            )
        self.user_id = sql(
            f"SELECT id FROM users WHERE preferred_username = {lit(self.args.user)}"
        )
        if not _UUID_RE.fullmatch(self.user_id):
            raise GateError(f"no app user {self.args.user!r}")

    def plane(self) -> None:
        session = self.start_session("rw")
        pod = self.pod_json(session.pod) or {}
        problems = sidecar_pod_problems(pod, protected=False)
        plan = recorded_plan(pod) or {}
        kinds = [m.get("mount_kind") for m in plan.get("mounts") or []]
        if kinds != ["project"] or not session.mount:
            problems.append(f"the plan's mounts are {kinds}, not the project folder")
        self.report.check(
            "plane: the Pod has the opener, the unprivileged supervisor and a "
            "plan; the workspace has no FUSE profile",
            not problems,
            "; ".join(problems),
        )
        secret_body = (
            json.loads(command(K + ["get", "secret", session.objects, "-o", "json"]))
            if session.objects
            else {}
        )
        owners = (secret_body.get("metadata") or {}).get("ownerReferences") or []
        self.report.check(
            "plane: the credential Secret is immutable and owned by the Pod",
            secret_body.get("immutable") is True
            and [o.get("uid") for o in owners] == [session.pod_uid],
            session.objects,
        )
        conf = base64.b64decode(
            (secret_body.get("data") or {}).get("rclone.conf") or ""
        ).decode("utf-8", "replace")
        password = ""
        for values in parse_rclone_conf(conf).values():
            if values.get("pass"):
                password = secret(reveal(values["pass"]))
        if not password:
            raise GateError("the credential Secret holds no password")
        entry = wait_for(
            "the folder settles in its status file",
            lambda: (
                found
                if (found := self.status_entry(session.pod, 0)).get("state")
                in ("mounted", "unavailable")
                else None
            ),
            timeout=180,
            interval=4,
        )
        seen = self.inspect(session.pod)
        tops = top_mounts(seen.get("mountinfo", ""))
        target = f"/cloud/{session.mount}"
        self.report.check(
            "plane: the folder is mounted (status file and the workspace's mount "
            "table)",
            entry.get("state") == "mounted"
            and tops.get(target, ("", ""))[0] == "fuse.rclone",
            f"{entry.get('state')} {entry.get('reason')} {tops.get(target)}",
        )
        self.report.check(
            "plane: the workspace has no /dev/fuse and no SYS_ADMIN",
            seen.get("fuse", "").strip() == "absent"
            and not has_cap(seen.get("capeff", ""), CAP_SYS_ADMIN),
            f"fuse {seen.get('fuse', '').strip()}",
        )
        ports = listening_ports(seen.get("tcp", ""))
        self.report.check(
            "plane: nothing listens on rclone's remote-control port",
            RCLONE_RC_PORT not in ports,
            f"listening {sorted(ports)}",
        )
        copies = [
            line for line in seen.get("rclone-conf", "").splitlines() if line.strip()
        ]
        rc, out, _err = run(
            K
            + ["exec", "-i", session.pod, "-c", WORKSPACE_CONTAINER, "--"]
            + ["grep", "-rlsF", "-f", "-", HOME, "/srw"],
            data=password + "\n",
            timeout=180,
        )
        holders = [line for line in out.splitlines() if line] if rc == 0 else []
        self.report.check(
            "plane: no rclone.conf and no copy of the password in the workspace",
            not copies and not holders,
            f"rclone.conf {copies} holders {holders}",
        )
        self.turn(session)
        ok, detail = self.wait_thread_state(session, "mounted")
        self.report.check(
            "plane: after a turn the thread's state says mounted, from the agent",
            ok,
            detail,
        )
        leaked = []
        for agent in self.pods("agent-stateless"):
            rc, out, _err = run(
                K
                + ["logs", agent["metadata"]["name"], "-c", AGENT_CONTAINER]
                + [f"--since-time={self.started}"],
                timeout=120,
            )
            # Output is scrubbed: a copy of the password reads <redacted>.
            if rc == 0 and "<redacted>" in out:
                leaked.append(agent["metadata"]["name"])
        self.report.check(
            "plane: no stateless agent log holds the password", not leaked, str(leaked)
        )
        marker = secrets.token_hex(16)
        filename = f"{self.gate_id}-rw.txt"
        rc, _out, err = self.as_agent(
            session.pod, f"printf %s {marker} > /cloud/{session.mount}/{filename}"
        )
        if rc:
            raise GateError(f"the write as agent-host failed: {err.strip()[-200:]}")
        expected = (hashlib.sha256(marker.encode()).hexdigest(), len(marker))
        try:
            found = wait_for(
                "the write reaches Nextcloud",
                lambda: self.nextcloud_file(session.folder_id, filename) == expected,
                timeout=180,
                interval=5,
            )
        except GateError:
            found = False
        self.report.check(
            "plane: a write as agent-host reaches Nextcloud", bool(found), filename
        )

    def killed(self) -> None:
        session = self.rw_session()
        before = self.pod_json(session.pod) or {}
        workspace_restarts = restarts(before, WORKSPACE_CONTAINER)
        supervisor_restarts = restarts(before, SUPERVISOR_CONTAINER)
        filename = f"{self.gate_id}-rw.txt"
        readable = f"cat /cloud/{session.mount}/{filename} >/dev/null"
        command(
            K
            + ["exec", session.pod, "-c", SUPERVISOR_CONTAINER, "--"]
            + ["pkill", "-9", "-x", "rclone"]
        )
        time.sleep(2)  # the mount is dead once rclone is
        try:
            wait_for(
                "the folder comes back after rclone was killed",
                lambda: self.folder_back(session, readable),
                timeout=180,
                interval=5,
            )
            back = True
        except GateError:
            back = False
        after = self.pod_json(session.pod) or {}
        self.report.check(
            "killed: rclone SIGKILLed, the folder comes back and the workspace "
            "never restarts",
            back and restarts(after, WORKSPACE_CONTAINER) == workspace_restarts,
            f"workspace restarts {restarts(after, WORKSPACE_CONTAINER)}",
        )
        self.stop_supervisor_from_node(after)
        try:
            wait_for(
                "the kubelet restarts the supervisor",
                lambda: restarts(
                    self.pod_json(session.pod) or after, SUPERVISOR_CONTAINER
                )
                > supervisor_restarts,
                timeout=180,
                interval=5,
            )
            wait_for(
                "the folder comes back after the supervisor restarted",
                lambda: self.folder_back(session, readable),
                timeout=240,
                interval=5,
            )
            back = True
        except GateError:
            back = False
        final = self.pod_json(session.pod) or {}
        self.report.check(
            "killed: the supervisor stopped from its node is restarted, the "
            "folder comes back and the workspace never restarts",
            back and restarts(final, WORKSPACE_CONTAINER) == workspace_restarts,
            f"supervisor restarts {restarts(final, SUPERVISOR_CONTAINER)}",
        )

    def missing(self) -> None:
        session = self.start_session("missing", folder_moved=True)
        entry = self.wait_status_file(session.pod, 0, "unavailable")
        pod = self.pod_json(session.pod) or {}
        self.report.check(
            "missing: the workspace is Ready with no restart while its moved "
            "folder reads unavailable with not_found",
            entry.get("reason") == "not_found"
            and restarts(pod, WORKSPACE_CONTAINER) == 0,
            f"{entry.get('state')} {entry.get('reason')}",
        )
        self.turn(session)
        ok, detail = self.wait_thread_state(session, "unavailable", "not_found")
        self.report.check(
            "missing: after a turn the thread's state says not_found", ok, detail
        )

    def protected(self) -> None:
        if not self.protected_mode:
            return
        session = self.start_session("prot", protected=True)
        pod = self.pod_json(session.pod) or {}
        problems = sidecar_pod_problems(pod, protected=True)
        plan = recorded_plan(pod) or {}
        opener = container(pod, OPENER_CONTAINER) or {}
        args = opener.get("args") or []
        if not plan.get("protected"):
            problems.append("the plan is not protected")
        if "/srw/cloud/lower:ro" not in args or args[-4:] != [
            "--dir",
            "/srw/cloud/merged",
            "--dir-uid",
            "1000",
        ]:
            problems.append(f"opener args {args[4:]}")
        self.report.check(
            "protected: the Pod keeps the workspace's FUSE profile; the opener "
            "serves the lower read-only and makes /srw/cloud/merged for "
            "agent-host",
            not problems,
            "; ".join(problems),
        )
        entry = self.wait_status_file(session.pod, 0, "mounted")
        tops = top_mounts(self.inspect(session.pod).get("mountinfo", ""))
        fstype, options = tops.get("/cloud/lower", ("", ""))
        rc, _out, err = self.as_agent(session.pod, "touch /cloud/lower/gate-probe")
        self.report.check(
            "protected: /cloud/lower is a read-only sidecar mount and a write "
            "to it fails with EROFS",
            entry.get("state") == "mounted"
            and fstype == "fuse.rclone"
            and "ro" in options.split(",")
            and rc != 0
            and "Read-only file system" in err,
            f"{fstype} {options} rc={rc}",
        )
        self.turn(session)
        ok, detail = self.wait_thread_state(session, "mounted")
        self.report.check(
            "protected: after a turn the thread's state says the lower mounted",
            ok,
            detail,
        )
        try:
            wait_for(
                "the overlay at /cloud/merged",
                lambda: top_mounts(self.inspect(session.pod).get("mountinfo", "")).get(
                    "/cloud/merged", ("",)
                )[0]
                == "fuse.fuse-overlayfs",
                timeout=self.args.turn_timeout,
                interval=5,
            )
            overlay = True
        except GateError:
            overlay = False
        filename = f"{self.gate_id}-p.txt"
        rc, _out, _err = self.as_agent(
            session.pod,
            f"printf captured > /cloud/merged/{filename} && "
            f"test -f {HOME}/.overlay/upper/{filename}",
        )
        self.report.check(
            "protected: /cloud/merged is the capture overlay and a write through "
            "it lands in the upper layer",
            overlay and rc == 0,
            f"overlay {overlay} rc={rc}",
        )
        rc, _out, err = run(
            K
            + ["exec", "-i", session.pod, "-c", SUPERVISOR_CONTAINER, "--"]
            + ["rclone", "--config", CREDENTIAL_FILE, "rcat", "m0:gate-ro-probe"],
            data="probe\n",
            timeout=120,
        )
        self.report.check(
            "protected: the reader credential is refused a WebDAV PUT",
            rc != 0 and ("403" in err or "Forbidden" in err),
            f"rc={rc} {err.strip()[-160:]}",
        )
        reader = sql(
            "SELECT reader_id FROM cloud_ro_mounts WHERE thread_id = "
            f"{lit(session.thread)} AND status = 'active'"
        )
        if not _READER_RE.fullmatch(reader or ""):
            raise GateError("the protected session has no active reader")
        before = self.pod_json(session.pod) or {}
        workspace_restarts = restarts(before, WORKSPACE_CONTAINER)
        self.disabled_reader = reader
        if self.occ(["user:disable", reader]):
            raise GateError("could not disable the reader account")
        try:
            self.stop_supervisor_from_node(before)
            entry = self.wait_status_file(session.pod, 0, "unavailable", timeout=240)
            # Decision 42: the next claim starts without cloud and reports
            # why, instead of failing.
            self.turn(session)
            started, detail = self.wait_thread_state(
                session, "unavailable", "credential_rejected"
            )
        finally:
            if self.occ(["user:enable", reader]) == 0:
                self.disabled_reader = None
        after = self.pod_json(session.pod) or {}
        self.report.check(
            "protected: with the reader disabled the restarted supervisor reads "
            "the lower unavailable with credential_rejected; no workspace restart",
            entry.get("reason") == "credential_rejected"
            and restarts(after, WORKSPACE_CONTAINER) == workspace_restarts,
            f"{entry.get('state')} {entry.get('reason')}",
        )
        self.report.check(
            "protected: a claim then starts without cloud and the thread's state "
            "says credential_rejected (decision 42)",
            started,
            detail,
        )

    def teardown(self) -> None:
        session = self.rw_session()
        pod = self.pod_json(session.pod) or {}
        node = self.node_of(pod)
        filename = f"{self.gate_id}-50mb.bin"
        rc, out, err = self.as_agent(
            session.pod,
            f'f=$(mktemp) && head -c {LARGE_FILE_BYTES} /dev/urandom > "$f" '
            f'&& cp "$f" /cloud/{session.mount}/{filename} '
            '&& sha256sum "$f" && rm -f "$f"',
        )
        if rc or not re.fullmatch(r"[0-9a-f]{64}", (out.split() or [""])[0]):
            raise GateError(f"the large write failed: {err.strip()[-200:]}")
        digest = out.split()[0]
        started = time.monotonic()
        status, body = self.api.call(
            "DELETE",
            f"/api/persistent/threads/{session.thread}?force=true",
            timeout=GRACE_SECONDS + 240,
        )
        elapsed = time.monotonic() - started
        self.report.check(
            "teardown: End returns within the grace period",
            status in (200, 202, 204) and elapsed <= GRACE_SECONDS + 60,
            f"HTTP {status} after {elapsed:.0f}s {str(body)[:120]}",
        )
        try:
            wait_for(
                "the Pod is gone",
                lambda: self.pod_json(session.pod) is None,
                timeout=GRACE_SECONDS + 60,
                interval=5,
            )
            gone = True
        except GateError:
            gone = False
        left = self.node_mounts_of(node, session.pod_uid) if gone else -1
        self.report.check(
            "teardown: the Pod is gone and nothing of it is in the node's mount table",
            gone and left == 0,
            f"gone {gone}, {left} node mounts",
        )

        def collected() -> bool:
            for kind in ("configmap", "secret"):
                rc, _out, _err = run(K + ["get", kind, session.objects], timeout=60)
                if rc == 0:
                    return False
            return True

        try:
            wait_for("objects collected", collected, timeout=180, interval=5)
            done = True
        except GateError:
            done = False
        self.report.check(
            "teardown: the plan ConfigMap and the credential Secret are "
            "garbage-collected",
            done,
            session.objects,
        )
        try:
            complete = wait_for(
                "the large file in Nextcloud",
                lambda: self.nextcloud_file(session.folder_id, filename)
                == (digest, LARGE_FILE_BYTES),
                timeout=180,
                interval=5,
            )
        except GateError:
            complete = False
        self.report.check(
            "teardown: the 50 MB file written just before End is complete in Nextcloud",
            bool(complete),
            filename,
        )

    def regression(self) -> None:
        project = self.projects.get("rw") or self.create_project("job")[0]
        created = self.api.ok(
            "POST",
            "/api/jobs",
            {
                "description": f"[{self.gate_id} job] Write the word done to "
                "output/d7.txt and complete the job.",
                "project_id": project,
                "execution_lane": "stateless",
                "config_override": {
                    "workspace": {"backend": "sandbox"},
                    "llm": {"model": self.args.model},
                },
            },
        )
        job = self.jobs["job"] = str(created.get("job_id") or created["id"])
        pod = self.workspace_pod(
            f"srw/job-id={job}", "workspace-", timeout=self.args.pod_timeout
        )
        self.report.check(
            "regression: a job's workspace Pod has no sidecars and no plan",
            container(pod, OPENER_CONTAINER) is None
            and container(pod, SUPERVISOR_CONTAINER) is None
            and recorded_plan(pod) is None,
            pod["metadata"]["name"],
        )

    # -- cleanup -------------------------------------------------------------
    def titled_threads(self) -> list[str]:
        rows = sql(
            "SELECT id FROM threads WHERE "
            f"position({lit(self.gate_id)} in coalesce(title, '')) > 0"
        )
        return [row for row in rows.splitlines() if _UUID_RE.fullmatch(row)]

    def described_jobs(self) -> list[str]:
        rows = sql(
            "SELECT id FROM jobs WHERE "
            f"position({lit('[' + self.gate_id)} in coalesce(description, '')) > 0"
        )
        return [row for row in rows.splitlines() if _UUID_RE.fullmatch(row)]

    def delete_thread(self, thread: str) -> bool:
        def gone() -> bool:
            status, _body = self.api.call(
                "DELETE", f"/api/persistent/threads/{thread}?force=true&permanent=true"
            )
            if status == 404:
                return True
            return sql(f"SELECT count(*) FROM threads WHERE id = {lit(thread)}") == "0"

        try:
            wait_for("session deleted", gone, timeout=300, interval=5)
        except GateError:
            return False
        return True

    def delete_job(self, job: str) -> bool:
        self.api.call("PUT", f"/api/jobs/{job}/cancel")

        def gone() -> bool:
            status, _body = self.api.call("DELETE", f"/api/jobs/{job}")
            return status in (200, 204, 404)

        try:
            wait_for("job deleted", gone, timeout=240, interval=10)
        except GateError:
            return False
        return True

    def cleanup(self) -> list[str]:
        problems: list[str] = []

        def step(label: str, action: Callable[[], Any]) -> None:
            try:
                if action() is False:
                    problems.append(label)
            except GateError as exc:
                problems.append(f"{label} ({exc})")

        if self.disabled_reader:
            reader = self.disabled_reader
            step(
                f"enable the reader account {reader}",
                lambda: self.occ(["user:enable", reader]) == 0,
            )
        threads = [s.thread for s in self.sessions.values() if s.thread]
        for thread in dict.fromkeys([*threads, *self.titled_threads()]):
            step(f"delete session {thread}", lambda t=thread: self.delete_thread(t))
        for folder_id, mountpoint in list(self.moved_folders.items()):

            def moved_back(folder_id=folder_id, mountpoint=mountpoint) -> bool:
                if self.occ(["groupfolders:rename", folder_id, mountpoint]):
                    return False
                del self.moved_folders[folder_id]
                return True

            step(f"move group folder {folder_id} back", moved_back)
        for job in dict.fromkeys([*self.jobs.values(), *self.described_jobs()]):
            step(f"delete job {job}", lambda j=job: self.delete_job(j))
        for label, project in list(self.projects.items()):

            def project_deleted(project=project) -> bool:
                status, _body = self.api.call("DELETE", f"/api/projects/{project}")
                return status in (200, 204, 404)

            step(
                f"delete project {label}",
                lambda p=project_deleted: bool(
                    wait_for("project deleted", p, timeout=180, interval=10)
                ),
            )
        for problem in problems:
            print(f"cleanup: {problem} failed", flush=True)
        return problems

    def residue(self) -> list[str]:
        """What this run created and cleanup did not remove."""
        left: list[str] = []
        titled = self.titled_threads()
        if titled:
            left.append(f"sessions titled with the gate id: {titled}")
        described = self.described_jobs()
        if described:
            left.append(f"jobs described with the gate id: {described}")
        prefix = self.gate_id + " %"
        count = sql(f"SELECT count(*) FROM projects WHERE name LIKE {lit(prefix)}")
        if count != "0":
            left.append(f"{count} projects")
        threads = [s.thread for s in self.sessions.values() if s.thread]
        selectors = [
            *(f"srw/thread-id={thread}" for thread in threads),
            *(f"srw/job-id={job}" for job in self.jobs.values()),
        ]
        for selector in selectors:
            try:
                wait_for(
                    f"pods {selector} gone",
                    lambda selector=selector: not json.loads(
                        command(K + ["get", "pods", "-l", selector, "-o", "json"])
                    )["items"],
                    timeout=180,
                    interval=10,
                )
            except GateError:
                left.append(f"pods {selector}")
        for name in sorted(self.objects):
            for kind in ("configmap", "secret"):
                rc, _out, _err = run(K + ["get", kind, name], timeout=60)
                if rc == 0:
                    left.append(f"{kind} {name}")
        if self.disabled_reader:
            left.append(f"the reader account {self.disabled_reader} is disabled")
        if self.moved_folders:
            left.append(f"group folders not moved back: {sorted(self.moved_folders)}")
        return left

    # -- run -------------------------------------------------------------------
    def run(self) -> int:
        try:
            self.preflight()
            # Independent groups: a failure is recorded and the next group
            # still runs, so one broken hop does not hide the others.
            for group in (
                (self.plane, self.killed),
                (self.teardown,),
                (self.missing,),
                (self.protected,),
                (self.regression,),
            ):
                for phase in group:
                    try:
                        phase()
                    except GateError as exc:
                        self.report.check(
                            f"{phase.__name__}: infrastructure", False, str(exc)
                        )
                        break
        except GateError as exc:
            self.report.check("gate infrastructure", False, str(exc))
        finally:
            if self.args.keep:
                print(
                    "kept: "
                    + json.dumps(
                        {
                            "projects": self.projects,
                            "sessions": {k: v.thread for k, v in self.sessions.items()},
                            "jobs": self.jobs,
                        }
                    )
                )
                if self.disabled_reader:
                    self.occ(["user:enable", self.disabled_reader])
            else:
                self.cleanup()
                try:
                    left = self.residue()
                except GateError as exc:
                    left = [f"residue check failed: {exc}"]
                self.report.check(
                    "cleanup: nothing this run created is left",
                    not left,
                    "; ".join(left),
                )
        verdict = "PASS" if self.report.passed else "FAIL"
        failed = [name for name, ok, _ in self.report.results if not ok]
        print(
            f"{verdict} {self.gate_id}: {len(self.report.results)} checks, "
            f"{len(failed)} failed {failed if failed else ''}".rstrip()
        )
        return 0 if self.report.passed else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--context", default=LOCAL_CONTEXT)
    parser.add_argument("--namespace", default=LOCAL_NAMESPACE)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--confirm")
    parser.add_argument("--gate-id")
    parser.add_argument("--user", default="test")
    parser.add_argument("--password", default="srw-k3d-dev-test")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--pod-timeout", type=int, default=420)
    parser.add_argument("--turn-timeout", type=int, default=420)
    parser.add_argument(
        "--require-protected",
        action="store_true",
        help="fail when protected cloud mode is off instead of skipping",
    )
    parser.add_argument("--keep", action="store_true", help="skip cleanup")
    return parser


def validate(args: argparse.Namespace) -> None:
    if args.context != LOCAL_CONTEXT or args.namespace != LOCAL_NAMESPACE:
        raise SafetyError("this gate is restricted to k3d-srw/srw")
    if args.run and args.confirm != LOCAL_CONFIRMATION:
        raise SafetyError(f"--run requires --confirm {LOCAL_CONFIRMATION}")
    if not args.run and args.confirm is not None:
        raise SafetyError("--confirm is accepted only with --run")
    if args.gate_id is not None and not _GATE_ID_RE.fullmatch(args.gate_id):
        raise SafetyError("--gate-id must be d7- followed by 10 hex digits")
    if not _MODEL_RE.fullmatch(args.model):
        raise SafetyError("model id is malformed")
    if not _USER_RE.fullmatch(args.user):
        raise SafetyError("user name is malformed")
    if not 60 <= args.pod_timeout <= 1800:
        raise SafetyError("--pod-timeout must be between 60 and 1800 seconds")
    if not 60 <= args.turn_timeout <= 1800:
        raise SafetyError("--turn-timeout must be between 60 and 1800 seconds")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        validate(args)
    except SafetyError as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2
    if not args.run:
        print("plan (dry run; pass --run --confirm LOCAL-K3D-DISPOSABLE to execute):")
        for step in PLAN:
            print(f"  - {step}")
        return 0
    return CloudMountSidecarGate(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
