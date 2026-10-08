#!/usr/bin/env python3
"""Local k3d spike for connector drivers D7: the in-pod plane.

Design: knowledge-base/knowledge/features/connector_drivers.md, "Three
planes" (In-pod), slice D7 and "To verify while building" (emptyDir mount
propagation, rclone on a /dev/fd/N mountpoint). It reproduces the evidence
the D7 decision rests on, comparing two ways to give a workspace Pod a FUSE
cloud mount without FUSE, privilege or the credential in the workspace
container:

  a   a privileged native mount sidecar: rclone itself is privileged and
      mounts into a memory emptyDir with Bidirectional propagation
  b   fd passing: a privileged native sidecar (srw-fuse-opener,
      drivers/fuse-opener) opens /dev/fuse, mounts it and hands the
      descriptor to an unprivileged rclone sidecar through rclone's
      fusermount3 (the _FUSE_COMMFD protocol); the Pod is the one
      src/orchestrator/services/in_pod_mount.py builds
  b1  b, but rclone serves the magic /dev/fd/3 mountpoint
      (srw-fuse-opener exec), with and without --allow-non-empty

The workspace container is busybox as uid 1000 with every capability
dropped and no /dev/fuse; it mounts the shared emptyDir with HostToContainer.
The cloud is a WebDAV server in the same namespace (rclone serve webdav, a
random throwaway password), not the cluster's Nextcloud: the spike touches
nothing outside its namespace.

Checks (each PASS/FAIL; exit 0 only if all pass):

  preflight  the k3d-srw context, k3d-srw-server-0 one of its nodes (before
             any docker exec into it), k3s, /var/lib/kubelet a shared mount on
             the node, the namespace and the spike's registry repositories
             absent; any failure aborts the run before it creates anything
  start      (a, b) the workspace's first command sees the fuse.rclone mount
             and lists the fixture: the sidecar's startup probe held it back
  readonly   (a, b) reads work; create, overwrite, mkdir and rm fail with
             EROFS before reaching rclone (its debug log shows no write op);
             remount and umount fail (no CAP_SYS_ADMIN)
  isolation  (a, b) the workspace sees only its own processes, no
             credential variable or file, only the mountpoint in the shared
             emptyDir, and no listening socket in the Pod's network namespace
  privilege  (b) rclone runs as 65534 with no capability, no_new_privs,
             seccomp, no /dev/fuse and no setuid fusermount3; the opener image
             has no shell; the opener refuses another path and a suid mount
  restart    (a, b) SIGKILL rclone: the workspace reads ENOTCONN, the sidecar
             restarts, detaches the dead mount and mounts again, and the
             workspace reads the new mount without restarting
  opener     (b) SIGKILL the opener: the live mount is untouched; after a
             second rclone kill the restarted opener detaches and remounts
  dead       (b) SIGKILL rclone twice (the second restart waits out a 10 s
             back-off), then SIGKILL the opener over the dead mount: it
             detaches it on start and serves again, rclone remounts, and the
             Pod still goes without sticking Terminating
  delete     (a, b) Pod deletion unmounts within the grace period and leaves
             no mount or pod directory on the node
  hang       (a, b) SIGSTOP rclone, then delete the Pod: rclone never answers
             SIGTERM and is SIGKILLed at grace expiry. a: nothing is left to
             unmount, the emptyDir teardown fails "Resource busy" and the Pod
             never goes; the spike detaches the mount on the node and the Pod
             goes. b: the opener, stopped last, detaches it and the Pod goes
  crash      (a, b) SIGKILL rclone while the Pod terminates (the workspace
             takes 8 s to stop). a is a race: a kubelet that has not begun
             terminating restarts the sidecar, whose start detaches the dead
             mount; one that has does not, and the Pod sticks as in hang
             (both seen on k3d). b: the opener detaches it and the Pod goes
  devfd      (b1) rclone mount2 serves /dev/fd/3 with --allow-non-empty and
             fails without
  privws     (b) a privileged workspace as root (today's FUSE profile)
             remounts the mount read-write; the write reaches rclone and only
             rclone's --read-only refuses it
  cleanup    best-effort, and only what this run created: the namespace is
             deleted (no privileged Pod left), no node mount names a spike
             Pod, the run's registry repositories, node images and local tags
             are gone

Run with the repository venv, on k3d-srw only. It creates and deletes the
namespace srw-spike-d7 (Pod Security privileged) and never touches another;
it refuses to start if that namespace or its registry repositories already
exist. At most two small Pods run at a time.

  .venv/bin/python scripts/k3d-in-pod-plane-spike.py            # plan
  .venv/bin/python scripts/k3d-in-pod-plane-spike.py \\
      --run --confirm LOCAL-K3D-DISPOSABLE [--only b,b-hang]
"""

from __future__ import annotations

import argparse
import base64
import copy
import json
import re
import secrets
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
LOCAL_CONTEXT = "k3d-srw"
NAMESPACE = "srw-spike-d7"
LOCAL_CONFIRMATION = "LOCAL-K3D-DISPOSABLE"
#: Namespaces the spike must never touch, whatever it is asked.
FORBIDDEN_NAMESPACES = frozenset({"srw", "srw-connectors", "default", "kube-system"})
NODE_CONTAINER = "k3d-srw-server-0"
PUSH_REGISTRY = "localhost:5005"
NODE_REGISTRY = "srw-registry:5000"
REGISTRY_CONTAINER = "srw-registry"
REGISTRY_REPOSITORIES = "/var/lib/registry/docker/registry/v2/repositories"
UPSTREAM_RCLONE = (
    "rclone/rclone:1.74.3"
    "@sha256:623378ad0ff3ebd5cebf77720843c0e02edfe46e2d5b5ac6bed54c6371780dfb"
)
WORKSPACE_IMAGE = "docker.io/library/busybox:1.36"
REPOSITORIES = {
    "opener": "srw-spike-d7-opener",
    "rclone": "srw-spike-d7-rclone-shim",
    "upstream": "srw-spike-d7-rclone",
}
FIXTURE_USER = "spike"
WORKSPACE_UID = 1000
TERM_DELAY_SECONDS = 8
_RUN_ID_RE = re.compile(r"d7-[0-9a-f]{10}\Z")

PLAN = [
    "preflight: k3d-srw context, its node k3d-srw-server-0, k3s, shared "
    "/var/lib/kubelet, namespace and registry repositories absent (any failure aborts)",
    "images: docker/Dockerfile.in-pod-mount (opener, rclone) and upstream "
    "rclone, pushed as localhost:5005/srw-spike-d7-*:<run id>",
    f"namespace: {NAMESPACE}, Pod Security privileged, srw.io/spike=d7",
    "fixture: WebDAV (rclone serve webdav, unprivileged) with a random password",
    "a: privileged rclone sidecar: start, readonly, isolation, restart, delete",
    "a-crash: rclone killed during termination (a race: clean or stuck)",
    "a-hang: rclone hung at termination (expected: stuck teardown, repaired on the node)",
    "b: opener + unprivileged rclone (in_pod_mount.py): start, readonly, "
    "isolation, privilege, restart, opener restart, delete",
    "b-crash / b-hang: the same failures (expected: the opener detaches, the Pod goes)",
    "b-dead: the opener restarts over a dead mount during rclone's back-off",
    "b1: rclone on /dev/fd/3 with and without --allow-non-empty",
    "privws: today's privileged workspace profile against b's mount",
    "cleanup: delete what this run created (namespace, node mounts, registry "
    "repositories, node images, local tags), best-effort",
]
PHASES = frozenset(
    {"a", "a-crash", "a-hang", "b", "b-crash", "b-hang", "b-dead", "b1", "privws"}
)


class SafetyError(RuntimeError):
    """A request outside the local, disposable boundary."""


class SpikeError(RuntimeError):
    """The spike could not go on."""


def run(
    argv: list[str],
    *,
    input: str | None = None,
    timeout: float = 120,
    check: bool = False,
) -> subprocess.CompletedProcess:
    try:
        result = subprocess.run(
            argv, input=input, capture_output=True, text=True, timeout=timeout
        )
    except subprocess.TimeoutExpired:
        raise SpikeError(f"timed out: {' '.join(argv[:4])}") from None
    if check and result.returncode != 0:
        raise SpikeError(
            f"{' '.join(argv[:4])} failed: {(result.stderr or result.stdout).strip()[:400]}"
        )
    return result


def kubectl(
    *args: str, namespaced: bool = True, **kwargs: Any
) -> subprocess.CompletedProcess:
    argv = ["kubectl", f"--context={LOCAL_CONTEXT}"]
    if namespaced:
        argv += ["-n", NAMESPACE]
    return run(argv + list(args), **kwargs)


def node(*args: str, **kwargs: Any) -> subprocess.CompletedProcess:
    return run(["docker", "exec", NODE_CONTAINER, *args], **kwargs)


# ---------------------------------------------------------------------------
# Manifests (pure)
# ---------------------------------------------------------------------------

#: The workspace: report the mount at its first instruction, then idle; on
#: SIGTERM wait TERM_DELAY (the crash check stretches termination with it).
WORKSPACE_SCRIPT = r"""
ts() { echo "[$(cut -d' ' -f1 /proc/uptime)] $*"; }
ts "workspace start"
grep " $CLOUD_ROOT" /proc/self/mountinfo || ts "no mount yet"
ls -la "$CLOUD_ROOT/$CLOUD_NAME" 2>&1
trap 'ts "workspace TERM"; sleep "${TERM_DELAY:-0}"; ts "workspace exits"; exit 0' TERM
while true; do sleep 1; done
"""

#: Approach a's sidecar: a restarted container must detach its predecessor's
#: dead mount before anything stats the mountpoint (ENOTCONN).
A_SIDECAR_SCRIPT = r"""
set -eu
T=/srw/cloud/root
ts() { echo "[$(cut -d' ' -f1 /proc/uptime)] $*"; }
fstype() { awk -v t="$T" '$5==t {for(i=7;i<=NF;i++) if($i=="-") print $(i+1)}' /proc/self/mountinfo; }
ts "sidecar start; mounts at target: [$(fstype | tr '\n' ' ')]"
if [ -n "$(fstype)" ]; then
  ls "$T" >/dev/null 2>/tmp/ls.err || ts "stale mount: $(cat /tmp/ls.err)"
  umount -l "$T" && ts "lazy-unmounted the stale mount"
fi
mkdir -p "$T"
rclone mount2 cloud: "$T" --config /etc/srw-cloud/rclone.conf --read-only --allow-other \
  --uid 1000 --gid 1000 --umask 022 -vv --log-file /tmp/rclone.log &
pid=$!
trap 'ts "TERM"; kill -TERM $pid; wait $pid; ts "rclone exited $?"; exit 0' TERM INT
wait $pid
"""

A_STARTUP_PROBE = (
    '[ "$(awk \'$5=="/srw/cloud/root" {for(i=7;i<=NF;i++) if($i=="-") '
    "print $(i+1)}' /proc/self/mountinfo | tail -1)\" = fuse.rclone ] "
    "&& ls /srw/cloud/root >/dev/null"
)

SMALL = {
    "requests": {"cpu": "10m", "memory": "16Mi"},
    "limits": {"cpu": "200m", "memory": "64Mi"},
}


def fixture_manifests(password: str, rclone_conf: str) -> list[dict]:
    secret = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "cloud-credential", "namespace": NAMESPACE},
        "data": {
            "password": base64.b64encode(password.encode()).decode(),
            "rclone.conf": base64.b64encode(rclone_conf.encode()).decode(),
        },
    }
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "webdav",
            "namespace": NAMESPACE,
            "labels": {"app": "webdav", "srw.io/spike": "d7"},
        },
        "spec": {
            "automountServiceAccountToken": False,
            "enableServiceLinks": False,
            "terminationGracePeriodSeconds": 2,
            "securityContext": {
                "runAsUser": 65534,
                "runAsGroup": 65534,
                "runAsNonRoot": True,
                "seccompProfile": {"type": "RuntimeDefault"},
            },
            "containers": [
                {
                    "name": "webdav",
                    "image": "UPSTREAM",
                    "command": ["/bin/sh", "-c"],
                    "args": [
                        "set -eu; mkdir -p /data/docs; "
                        "echo 'hello from the fixture' > /data/docs/readme.txt; "
                        "echo 'second file' > /data/notes.txt; "
                        f"exec rclone serve webdav /data --addr :8080 --user {FIXTURE_USER} "
                        '--pass "$(cat /etc/fixture/password)" --config /dev/null'
                    ],
                    "ports": [{"containerPort": 8080, "name": "webdav"}],
                    "resources": {
                        "requests": {"cpu": "10m", "memory": "32Mi"},
                        "limits": {"cpu": "200m", "memory": "128Mi"},
                    },
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "capabilities": {"drop": ["ALL"]},
                    },
                    "volumeMounts": [
                        {"name": "data", "mountPath": "/data"},
                        {
                            "name": "credential",
                            "mountPath": "/etc/fixture",
                            "readOnly": True,
                        },
                    ],
                    "readinessProbe": {"tcpSocket": {"port": 8080}, "periodSeconds": 2},
                }
            ],
            "volumes": [
                {"name": "data", "emptyDir": {"sizeLimit": "16Mi"}},
                {
                    "name": "credential",
                    "secret": {
                        "secretName": "cloud-credential",
                        "items": [{"key": "password", "path": "password"}],
                    },
                },
            ],
        },
    }
    service = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "webdav",
            "namespace": NAMESPACE,
            "labels": {"srw.io/spike": "d7"},
        },
        "spec": {
            "selector": {"app": "webdav"},
            "ports": [{"port": 8080, "targetPort": "webdav", "name": "webdav"}],
        },
    }
    return [secret, pod, service]


def rclone_config(obscured: str) -> str:
    return (
        "[cloud]\ntype = webdav\nurl = http://webdav:8080\nvendor = other\n"
        f"user = {FIXTURE_USER}\npass = {obscured}\n"
    )


def workspace_container(
    cloud_root: str, cloud_name: str, *, term_delay: int = 0
) -> dict:
    return {
        "name": "workspace",
        "image": WORKSPACE_IMAGE,
        "command": ["/bin/sh", "-c", WORKSPACE_SCRIPT],
        "env": [
            {"name": "CLOUD_ROOT", "value": cloud_root},
            {"name": "CLOUD_NAME", "value": cloud_name},
            {"name": "TERM_DELAY", "value": str(term_delay)},
        ],
        "resources": copy.deepcopy(SMALL),
        "securityContext": {
            "runAsUser": WORKSPACE_UID,
            "runAsGroup": WORKSPACE_UID,
            "runAsNonRoot": True,
            "allowPrivilegeEscalation": False,
            "capabilities": {"drop": ["ALL"]},
            "seccompProfile": {"type": "RuntimeDefault"},
        },
        "volumeMounts": [],
    }


def base_pod(name: str, approach: str, workspace: dict) -> dict:
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": NAMESPACE,
            "labels": {"srw.io/spike": "d7", "srw.io/approach": approach},
        },
        "spec": {
            "automountServiceAccountToken": False,
            "enableServiceLinks": False,
            "restartPolicy": "Never",
            "terminationGracePeriodSeconds": 30,
            "containers": [workspace],
            "volumes": [],
        },
    }


def pod_a(name: str, upstream_image: str, *, term_delay: int = 0) -> dict:
    """Approach a: rclone itself privileged, Bidirectional on the emptyDir."""
    workspace = workspace_container("/home/agent/cloud", "root", term_delay=term_delay)
    workspace["volumeMounts"].append(
        {
            "name": "cloud",
            "mountPath": "/home/agent/cloud",
            "mountPropagation": "HostToContainer",
        }
    )
    pod = base_pod(name, "a", workspace)
    pod["spec"]["initContainers"] = [
        {
            "name": "cloud-mount",
            "image": upstream_image,
            "restartPolicy": "Always",
            "command": ["/bin/sh", "-c", A_SIDECAR_SCRIPT],
            "resources": {
                "requests": {"cpu": "10m", "memory": "32Mi"},
                "limits": {"cpu": "500m", "memory": "256Mi"},
            },
            "securityContext": {"privileged": True},
            "volumeMounts": [
                {
                    "name": "cloud",
                    "mountPath": "/srw/cloud",
                    "mountPropagation": "Bidirectional",
                },
                {"name": "credential", "mountPath": "/etc/srw-cloud", "readOnly": True},
            ],
            "startupProbe": {
                "exec": {"command": ["/bin/sh", "-c", A_STARTUP_PROBE]},
                "periodSeconds": 1,
                "failureThreshold": 60,
            },
        }
    ]
    pod["spec"]["volumes"] = [
        {"name": "cloud", "emptyDir": {"medium": "Memory", "sizeLimit": "1Mi"}},
        {
            "name": "credential",
            "secret": {"secretName": "cloud-credential", "defaultMode": 0o400},
        },
    ]
    return pod


def pod_b(
    name: str,
    opener_image: str,
    rclone_image: str,
    *,
    term_delay: int = 0,
    devfd: bool = False,
    allow_non_empty: bool = True,
    privileged_workspace: bool = False,
) -> dict:
    """Approach b: the Pod SRW's prototype builder makes, plus debug logging."""
    if str(ROOT / "src") not in sys.path:
        sys.path.insert(0, str(ROOT / "src"))
    from orchestrator.services import in_pod_mount as plane

    workspace = workspace_container(
        plane.WORKSPACE_CLOUD_ROOT, "root", term_delay=term_delay
    )
    pod = base_pod(name, "b1" if devfd else "b", workspace)
    mount = plane.CloudMountSidecar(
        name="root", secret_name="cloud-credential", remote="cloud:"
    )
    plane.add_cloud_mount_sidecars(
        pod, mount, plane.InPodPlaneImages(opener=opener_image, rclone=rclone_image)
    )
    if privileged_workspace:
        # Today's FUSE profile, with a root process inside it: what the
        # builder refuses, set afterwards to measure why.
        workspace["securityContext"] = {"privileged": True}
    rclone = next(
        c for c in pod["spec"]["initContainers"] if c["name"] == plane.RCLONE_CONTAINER
    )
    if not allow_non_empty:
        rclone["args"].remove("--allow-non-empty")
    else:
        # Which FUSE operations reach rclone (none of the refused writes may).
        rclone["args"] += ["-vv", "--log-file", "/tmp/rclone.log"]
    if devfd:
        rclone["command"] = [
            "srw-fuse-opener",
            "exec",
            "--socket",
            plane.SOCKET_PATH,
            "--",
            "rclone",
        ]
        rclone["args"][rclone["args"].index(mount.target)] = "/dev/fd/3"
    return pod


# ---------------------------------------------------------------------------
# Evaluators (pure)
# ---------------------------------------------------------------------------

WORKSPACE_PROBE = r"""
C="$1"
echo "== read"; cat "$C/docs/readme.txt"
echo "== create"; touch "$C/new.txt" 2>&1
echo "== overwrite"; sh -c "echo x > $C/notes.txt" 2>&1
echo "== mkdir"; mkdir "$C/d" 2>&1
echo "== rm"; rm "$C/notes.txt" 2>&1
echo "== remount"; mount -o remount,rw "$C" 2>&1; echo "rc=$?"
echo "== umount"; umount "$C" 2>&1; echo "rc=$?"
echo "== emptydir"; ls -A "$(dirname "$C")"
echo "== procs"; for p in /proc/[0-9]*; do tr '\0' ' ' < $p/cmdline 2>/dev/null | cut -c1-60; echo; done | grep -v '^$' | sort -u
echo "== env"; env | grep -i -E "pass|rclone|webdav|cred" || echo "none"
echo "== credfile"; ls /etc/srw-cloud 2>&1
echo "== listen"; netstat -ltnu 2>&1 | tail -n +3
echo "== caps"; grep -E "^CapEff" /proc/self/status
echo "== end"
"""


def sections(output: str) -> dict[str, str]:
    """Split a probe's ``== name`` sections."""
    found: dict[str, str] = {}
    current = None
    for line in output.splitlines():
        if line.startswith("== "):
            current = line[3:].strip()
            found[current] = ""
        elif current is not None:
            found[current] += line + "\n"
    return found


def evaluate_readonly(output: str) -> list[str]:
    s = sections(output)
    problems = []
    if "hello from the fixture" not in s.get("read", ""):
        problems.append("the read failed")
    for name in ("create", "overwrite", "mkdir", "rm"):
        if "Read-only file system" not in s.get(name, ""):
            problems.append(
                f"{name} was not refused with EROFS: {s.get(name, '').strip()!r}"
            )
    for name in ("remount", "umount"):
        if "rc=0" in s.get(name, ""):
            problems.append(f"{name} succeeded in the workspace")
    return problems


WRITE_OPS = re.compile(r": (Create|Mkdir|Remove|Rmdir|Unlink|Rename|Setattr|Write)\b")


def evaluate_rclone_log(log: str) -> list[str]:
    hits = [line for line in log.splitlines() if WRITE_OPS.search(line)]
    return [f"a write reached rclone: {hits[0][:160]}"] if hits else []


def evaluate_isolation(output: str, mount_name: str) -> list[str]:
    s = sections(output)
    problems = []
    if "rclone" in s.get("procs", ""):
        problems.append("the workspace sees an rclone process")
    if s.get("env", "").strip() != "none":
        problems.append("a credential-like variable is visible")
    if "No such file" not in s.get("credfile", ""):
        problems.append("the credential directory is visible")
    if s.get("emptydir", "").split() != [mount_name]:
        problems.append(
            f"the shared emptyDir holds more than the mountpoint: {s.get('emptydir')!r}"
        )
    if s.get("listen", "").strip():
        problems.append(f"a socket listens in the Pod: {s['listen'].strip()[:200]}")
    if "0000000000000000" not in s.get("caps", ""):
        problems.append("the workspace has a capability")
    return problems


def evaluate_watch(log: str) -> tuple[list[str], float | None]:
    """Did the workspace see ENOTCONN and then a new, readable mount?"""
    lines = [line for line in log.splitlines() if line.startswith("[")]
    first_id = None
    broke_at = None
    for line in lines:
        stamp = float(line[1 : line.index("]")])
        ids = re.search(r"mounts=\[([0-9 ]*)\]", line)
        mount_ids = ids.group(1).split() if ids else []
        if first_id is None and mount_ids:
            first_id = mount_ids[-1]
        if "Transport endpoint is not connected" in line and broke_at is None:
            broke_at = stamp
        if (
            broke_at is not None
            and "hello from the fixture" in line
            and mount_ids
            and mount_ids[-1] != first_id
        ):
            return [], stamp - broke_at
    problems = []
    if broke_at is None:
        problems.append("the workspace never saw ENOTCONN")
    else:
        problems.append("the workspace never read a new mount")
    return problems, None


WATCH_SCRIPT = r"""
rm -f /tmp/watch.log
nohup sh -c '
i=0; last=""
while [ $i -lt 240 ]; do
  out=$(cat "$1/docs/readme.txt" 2>&1 | head -1)
  mnt=$(awk -v t="$1" "\$5==t {print \$1}" /proc/self/mountinfo | tr "\n" " ")
  cur="mounts=[$mnt] read=$out"
  if [ "$cur" != "$last" ]; then echo "[$(cut -d" " -f1 /proc/uptime)] $cur"; last="$cur"; fi
  i=$((i+1)); usleep 250000
done' watch "$1" > /tmp/watch.log 2>&1 &
echo started
"""


# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------


def registry_repositories() -> set[str]:
    """Which of the spike's repositories the k3d registry already holds."""
    present = set()
    for repository in REPOSITORIES.values():
        try:
            with urllib.request.urlopen(
                f"http://{PUSH_REGISTRY}/v2/{repository}/tags/list", timeout=30
            ):
                present.add(repository)
        except urllib.error.HTTPError as exc:
            if exc.code != 404:
                raise SpikeError(f"the k3d registry answered {exc.code}") from None
        except OSError as exc:
            raise SpikeError(f"the k3d registry is unreachable: {exc}") from None
    return present


def pod_mounts(mountinfo: str, uid: str) -> list[tuple[str, str]]:
    """(mountpoint, type) of every mount under a Pod's kubelet directory,
    matched on mountinfo's mountpoint field, never on the whole line."""
    prefix = f"/var/lib/kubelet/pods/{uid}/"
    found = []
    for line in mountinfo.splitlines():
        fields = line.split()
        if len(fields) < 7 or " - " not in line or not fields[4].startswith(prefix):
            continue
        found.append((fields[4], line.split(" - ", 1)[1].split()[0]))
    return found


@dataclass
class Spike:
    run_id: str
    keep: bool = False
    results: list[tuple[str, bool, str]] = field(default_factory=list)
    pod_uids: set[str] = field(default_factory=set)
    images: dict[str, str] = field(default_factory=dict)
    # What this run created, and so may delete: nothing else, ever.
    created_namespace: bool = False
    built_refs: list[str] = field(default_factory=list)
    pushed_keys: list[str] = field(default_factory=list)
    owned_repositories: set[str] = field(default_factory=set)

    def check(self, name: str, ok: bool, detail: str = "") -> bool:
        self.results.append((name, ok, detail))
        print(
            f"{'PASS' if ok else 'FAIL'} {name}{': ' + detail if detail else ''}",
            flush=True,
        )
        return ok

    # -- plumbing -------------------------------------------------------------
    def apply(self, manifest: dict) -> None:
        kubectl("apply", "-f", "-", input=json.dumps(manifest), check=True)

    def start(self, pod: dict, timeout: int = 120) -> str:
        self.apply(pod)
        name = pod["metadata"]["name"]
        uid = kubectl(
            "get", "pod", name, "-o", "jsonpath={.metadata.uid}", check=True
        ).stdout
        self.pod_uids.add(uid.strip())
        waited = kubectl(
            "wait",
            "--for=condition=Ready",
            f"pod/{name}",
            f"--timeout={timeout}s",
            timeout=timeout + 15,
        )
        if waited.returncode != 0:
            raise SpikeError(f"{name} never became ready: {self.describe(name)}")
        return uid.strip()

    def describe(self, name: str) -> str:
        return kubectl(
            "get",
            "pod",
            name,
            "-o",
            "jsonpath={.status.phase} {range .status.initContainerStatuses[*]}"
            "{.name}={.restartCount}/{.ready} {end}",
        ).stdout

    def sh(self, pod: str, container: str, script: str, *args: str) -> str:
        result = kubectl(
            "exec",
            "-i",
            pod,
            "-c",
            container,
            "--",
            "/bin/sh",
            "-s",
            "--",
            *args,
            input=script,
        )
        return result.stdout + result.stderr

    def logs(self, pod: str, container: str, *extra: str) -> str:
        return kubectl("logs", pod, "-c", container, *extra).stdout

    def restarts(self, pod: str, container: str) -> tuple[int, bool]:
        out = kubectl(
            "get",
            "pod",
            pod,
            "-o",
            "jsonpath={range .status.initContainerStatuses[*]}{.name}={.restartCount}={.ready}\n{end}"
            "{range .status.containerStatuses[*]}{.name}={.restartCount}={.ready}\n{end}",
        ).stdout
        for line in out.splitlines():
            name, count, ready = line.split("=")
            if name == container:
                return int(count), ready == "true"
        return -1, False

    def kill_container(self, pod: str, container: str) -> None:
        """SIGKILL a container's first process from the node, as the OOM
        killer would: from inside, PID 1 ignores SIGKILL. rclone dies with it
        (it is PID 1, or a child of a PID 1 whose namespace dies)."""
        node("kill", "-9", str(self.host_pid(pod, container)), check=True)

    def stop_rclone(self, pod: str, container: str) -> None:
        """SIGSTOP rclone from the node: a daemon hung on its remote, which
        never answers SIGTERM, so the kubelet SIGKILLs it at grace expiry."""
        init = self.host_pid(pod, container)
        children = node("cat", f"/proc/{init}/task/{init}/children").stdout.split()
        for pid in [str(init), *children]:
            if node("cat", f"/proc/{pid}/comm").stdout.strip() == "rclone":
                node("kill", "-STOP", pid, check=True)
                return
        raise SpikeError(f"no rclone process in {pod}/{container}")

    def host_pid(self, pod: str, container: str) -> int:
        ids = kubectl(
            "get",
            "pod",
            pod,
            "-o",
            "jsonpath={range .status.initContainerStatuses[*]}{.name}={.containerID}\n{end}",
        ).stdout
        container_id = next(
            (
                line.split("=", 1)[1]
                for line in ids.splitlines()
                if line.startswith(container + "=")
            ),
            "",
        ).removeprefix("containerd://")
        if not re.fullmatch(r"[0-9a-f]{64}", container_id):
            raise SpikeError(f"no running {container} in {pod}")
        pid = node(
            "crictl",
            "inspect",
            "--output",
            "go-template",
            "--template",
            "{{.info.pid}}",
            container_id,
            check=True,
        ).stdout.strip()
        if not pid.isdigit() or int(pid) <= 1:
            raise SpikeError(f"{container} has no host PID")
        return int(pid)

    def follow(self, pod: str, container: str) -> list[str]:
        """Collect a container's log lines from now on (they vanish with the Pod)."""
        lines: list[str] = []

        def reader() -> None:
            proc = subprocess.Popen(
                [
                    "kubectl",
                    f"--context={LOCAL_CONTEXT}",
                    "-n",
                    NAMESPACE,
                    "logs",
                    "-f",
                    pod,
                    "-c",
                    container,
                    "--since=1s",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            assert proc.stdout is not None
            for line in proc.stdout:
                lines.append(line.rstrip())

        threading.Thread(target=reader, daemon=True).start()
        time.sleep(1)
        return lines

    def wait_for(
        self, what: str, predicate: Callable[[], bool], timeout: float
    ) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(1)
        print(f"     waited {timeout:.0f}s for {what}", flush=True)
        return False

    def node_mounts(self, uid: str) -> list[str]:
        mountinfo = node("cat", "/proc/self/mountinfo", check=True).stdout
        return [
            f"{mountpoint} ({fstype})"
            for mountpoint, fstype in pod_mounts(mountinfo, uid)
        ]

    def pod_gone(self, name: str) -> bool:
        result = kubectl("get", "pod", name, "-o", "name")
        return result.returncode != 0 and "NotFound" in result.stderr

    def pod_dir_gone(self, uid: str) -> bool:
        return node("test", "-e", f"/var/lib/kubelet/pods/{uid}").returncode != 0

    # -- phases -----------------------------------------------------------------
    def preflight(self) -> None:
        """Every failure here aborts the run before it creates anything."""
        contexts = run(
            ["kubectl", "config", "get-contexts", "-o", "name"], check=True
        ).stdout
        if LOCAL_CONTEXT not in contexts.split():
            raise SpikeError(f"no {LOCAL_CONTEXT} context")
        # docker exec reaches the node container by name: it must be this
        # cluster's node, never some other container of that name.
        if kubectl("get", "node", NODE_CONTAINER, namespaced=False).returncode != 0:
            raise SpikeError(f"{LOCAL_CONTEXT} has no node {NODE_CONTAINER}")
        version = kubectl("version", "-o", "json", namespaced=False, check=True).stdout
        server = json.loads(version)["serverVersion"]["gitVersion"]
        if "k3s" not in server:
            raise SpikeError(f"{LOCAL_CONTEXT} is not k3s: {server}")
        self.check("preflight cluster", True, server)
        shared = node("grep", " /var/lib/kubelet ", "/proc/self/mountinfo").stdout
        tags = [f for f in shared.split() if f.startswith(("shared:", "master:"))]
        if not any(tag.startswith("shared:") for tag in tags):
            raise SpikeError(f"the node's /var/lib/kubelet is not shared: {tags}")
        self.check(
            "preflight shared kubelet dir",
            True,
            "node /var/lib/kubelet " + " ".join(tags),
        )
        if kubectl("get", "namespace", NAMESPACE, namespaced=False).returncode == 0:
            raise SpikeError(
                f"{NAMESPACE} exists: this run would not own it; delete it first"
            )
        existing = registry_repositories() & set(REPOSITORIES.values())
        if existing:
            raise SpikeError(
                f"registry repositories {sorted(existing)} exist: this run would not own them"
            )

    def build_images(self) -> None:
        tag = self.run_id
        for target in ("opener", "rclone"):
            ref = f"{PUSH_REGISTRY}/{REPOSITORIES[target]}:{tag}"
            run(
                [
                    "docker",
                    "build",
                    "-q",
                    "-f",
                    str(ROOT / "docker/Dockerfile.in-pod-mount"),
                    "--target",
                    target,
                    "-t",
                    ref,
                    str(ROOT),
                ],
                timeout=900,
                check=True,
            )
            self.built_refs.append(ref)
            self.images[target] = ref
        upstream = f"{PUSH_REGISTRY}/{REPOSITORIES['upstream']}:{tag}"
        run(["docker", "pull", "-q", UPSTREAM_RCLONE], timeout=600, check=True)
        run(["docker", "tag", UPSTREAM_RCLONE, upstream], check=True)
        self.built_refs.append(upstream)
        self.images["upstream"] = upstream
        for key, ref in self.images.items():
            # Preflight saw the repository absent, so the first push creates it.
            self.owned_repositories.add(REPOSITORIES[key])
            run(["docker", "push", "-q", ref], timeout=600, check=True)
            self.pushed_keys.append(key)
        self.check("images", True, ", ".join(sorted(self.images.values())))

    def node_image(self, key: str) -> str:
        return self.images[key].replace(PUSH_REGISTRY, NODE_REGISTRY, 1)

    def namespace(self) -> None:
        # create, not apply: it fails if the namespace appeared meanwhile.
        kubectl(
            "create",
            "-f",
            "-",
            namespaced=False,
            check=True,
            input=json.dumps(
                {
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {
                        "name": NAMESPACE,
                        "labels": {
                            "srw.io/spike": "d7",
                            "pod-security.kubernetes.io/enforce": "privileged",
                            "pod-security.kubernetes.io/enforce-version": "v1.31",
                        },
                    },
                }
            ),
        )
        self.created_namespace = True

    def fixture(self) -> None:
        password = secrets.token_urlsafe(24)
        obscured = run(
            ["docker", "run", "--rm", "-i", UPSTREAM_RCLONE, "obscure", "-"],
            input=password,
            check=True,
        ).stdout.strip()
        manifests = fixture_manifests(password, rclone_config(obscured))
        manifests[1]["spec"]["containers"][0]["image"] = self.node_image("upstream")
        for manifest in manifests:
            self.apply(manifest)
        if kubectl(
            "wait", "--for=condition=Ready", "pod/webdav", "--timeout=120s", timeout=135
        ).returncode:
            raise SpikeError("the WebDAV fixture never became ready")

    def exercise(
        self, label: str, pod: dict, *, sidecar: str, cloud: str, rclone_log: str
    ) -> None:
        name = pod["metadata"]["name"]
        self.start(pod)
        first = self.logs(name, "workspace")
        self.check(
            f"{label} start",
            "fuse.rclone" in first and "docs" in first and "no mount yet" not in first,
            "the workspace's first command saw the mount: "
            + " ".join(x for x in first.splitlines() if "fuse.rclone" in x)[:200],
        )
        probe = self.sh(name, "workspace", WORKSPACE_PROBE, cloud)
        problems = evaluate_readonly(probe)
        problems += evaluate_rclone_log(self.sh(name, sidecar, f"cat {rclone_log}"))
        self.check(
            f"{label} readonly",
            not problems,
            "; ".join(problems) or "EROFS in the kernel, no write op in rclone's log",
        )
        problems = evaluate_isolation(probe, Path(cloud).name)
        self.check(
            f"{label} isolation",
            not problems,
            "; ".join(problems) or "no rclone, credential or listener visible",
        )
        if label == "b":
            self.privilege(name, sidecar)
        # restart
        self.sh(name, "workspace", WATCH_SCRIPT, cloud)
        before, _ = self.restarts(name, sidecar)
        self.kill_container(name, sidecar)
        recovered = self.wait_for(
            f"{sidecar} to restart",
            lambda: self.restarts(name, sidecar) == (before + 1, True),
            90,
        )
        time.sleep(3)
        problems, seconds = evaluate_watch(
            self.sh(name, "workspace", "cat /tmp/watch.log")
        )
        workspace_restarts, _ = self.restarts(name, "workspace")
        if not recovered:
            problems.append(f"{sidecar} did not come back ready")
        if workspace_restarts != 0:
            problems.append("the workspace restarted")
        self.check(
            f"{label} restart",
            not problems,
            "; ".join(problems)
            or f"ENOTCONN, then the new mount readable {seconds:.1f}s later, workspace not restarted",
        )
        if label == "b":
            self.opener_restart(name)
        self.delete_and_check(f"{label} delete", name)

    def privilege(self, name: str, sidecar: str) -> None:
        out = self.sh(
            name,
            sidecar,
            'id -u; grep -E "^(CapEff|NoNewPrivs|Seccomp):" /proc/self/status; '
            "ls /dev/fuse 2>&1; ls -l /usr/bin/fusermount3 2>&1; "
            "SRW_FUSE_OPENER_SOCKET=/run/srw-fuse/opener.sock _FUSE_COMMFD=1 "
            "fusermount3 /tmp 2>&1; "
            "SRW_FUSE_OPENER_SOCKET=/run/srw-fuse/opener.sock _FUSE_COMMFD=1 "
            "fusermount3 -o suid /srw/cloud/root 2>&1; true",
        )
        problems = []
        expectations = {
            "uid 65534": out.splitlines()[0:1] == ["65534"],
            "no capability": "CapEff:\t0000000000000000" in out,
            "no_new_privs": "NoNewPrivs:\t1" in out,
            "seccomp": "Seccomp:\t2" in out,
            "no /dev/fuse": "/dev/fuse: No such file" in out,
            "no setuid fusermount3": "/usr/bin/fusermount3: No such file" in out,
            "another path refused": "only /srw/cloud/root may be mounted" in out,
            "suid refused": 'option "suid" is not allowed' in out,
        }
        problems = [k for k, ok in expectations.items() if not ok]
        shell = kubectl(
            "exec", name, "-c", "srw-fuse-opener", "--", "/bin/sh", "-c", "true"
        )
        if shell.returncode == 0:
            problems.append("the opener image has a shell")
        self.check(
            "b privilege",
            not problems,
            "missing: " + ", ".join(problems)
            if problems
            else "rclone unprivileged; opener refuses; no shell in the opener",
        )

    def delete_and_check(self, label: str, name: str) -> bool:
        uid = kubectl(
            "get", "pod", name, "-o", "jsonpath={.metadata.uid}"
        ).stdout.strip()
        started = time.monotonic()
        kubectl("delete", "pod", name, "--wait=false", check=True)
        gone = self.wait_for(f"{name} to go", lambda: self.pod_gone(name), 45)
        took = time.monotonic() - started
        self.wait_for("the pod directory to go", lambda: self.pod_dir_gone(uid), 20)
        left = self.node_mounts(uid)
        ok = gone and not left and self.pod_dir_gone(uid)
        self.check(
            label,
            ok,
            f"gone after {took:.1f}s, no node mount or pod directory"
            if ok
            else f"gone={gone} after {took:.1f}s; node mounts {left}",
        )
        return ok

    def termination(self, approach: str, mode: str) -> None:
        """rclone fails while its Pod terminates; does the teardown finish?

        crash: SIGKILL the rclone container while the workspace still takes
        TERM_DELAY to stop. hang: SIGSTOP rclone, so it never answers its
        SIGTERM and the kubelet SIGKILLs it at grace expiry.
        """
        name = f"{approach}-{mode}"
        delay = TERM_DELAY_SECONDS if mode == "crash" else 0
        if approach == "a":
            pod, sidecar = (
                pod_a(name, self.node_image("upstream"), term_delay=delay),
                "cloud-mount",
            )
        else:
            pod = pod_b(
                name,
                self.node_image("opener"),
                self.node_image("rclone"),
                term_delay=delay,
            )
            sidecar = "srw-cloud-mount"
        grace = pod["spec"]["terminationGracePeriodSeconds"]
        uid = self.start(pod)
        opener_log = self.follow(name, "srw-fuse-opener") if approach == "b" else []
        if mode == "hang":
            self.stop_rclone(name, sidecar)
        started = time.monotonic()
        kubectl("delete", "pod", name, "--wait=false", check=True)
        if mode == "crash":
            self.kill_container(name, sidecar)
        gone = self.wait_for(f"{name} to go", lambda: self.pod_gone(name), grace + 20)
        took = time.monotonic() - started
        restarts, _ = self.restarts(name, sidecar)
        left = self.node_mounts(uid)
        busy = (
            "Resource busy"
            in run(
                ["docker", "logs", "--since", "3m", NODE_CONTAINER], timeout=60
            ).stderr
        )
        repaired = 0
        if not gone:
            repaired = self.detach_on_node(uid)
            gone = self.wait_for(
                f"{name} to go after the repair", lambda: self.pod_gone(name), 180
            )
        clean = gone and not self.node_mounts(uid)
        detail = (
            f"gone after {took:.1f}s, no node residue"
            if not repaired
            else f"STUCK: dead mount {left} kept the Pod (kubelet 'Resource busy'={busy}, "
            f"sidecar restarts={restarts}); detached {repaired} on the node, then it went"
        )
        if opener_log:
            detail += (
                "; opener: " + " | ".join(x for x in opener_log if "unmount" in x)[:200]
            )
        if approach == "a" and mode == "hang":
            # The finding: nothing is left to unmount a privileged sidecar's mount.
            self.check(
                "a hang (expected stuck teardown)", bool(repaired) and clean, detail
            )
        elif approach == "a":
            # A race: a kubelet that has not begun terminating restarts the
            # sidecar, whose start detaches the dead mount; otherwise it sticks.
            self.check("a crash (race, either outcome)", clean, detail)
        else:
            self.check(f"b {mode} (opener detaches)", clean and not repaired, detail)

    def opener_restart(self, name: str) -> None:
        """The opener dies: the live mount is untouched, and the new opener
        still serves a remount and still sees what it must detach."""
        mount_id = self.sh(
            name,
            "workspace",
            "awk '$5==\"/cloud/root\" {print $1}' /proc/self/mountinfo",
        )
        self.kill_container(name, "srw-fuse-opener")
        back = self.wait_for(
            "the opener to restart",
            lambda: self.restarts(name, "srw-fuse-opener") == (1, True),
            60,
        )
        during = self.sh(
            name,
            "workspace",
            "cat /cloud/root/docs/readme.txt; awk '$5==\"/cloud/root\" {print $1}' /proc/self/mountinfo",
        )
        before, _ = self.restarts(name, "srw-cloud-mount")
        self.kill_container(name, "srw-cloud-mount")
        remounted = self.wait_for(
            "rclone to remount through the new opener",
            lambda: self.restarts(name, "srw-cloud-mount") == (before + 1, True),
            60,
        )
        after = self.sh(
            name,
            "workspace",
            "cat /cloud/root/docs/readme.txt; awk '$5==\"/cloud/root\" {print $1}' /proc/self/mountinfo",
        )
        # One mount at the target: the new opener detached the dead one
        # instead of stacking the new mount on top of it.
        stacked = len(re.findall(r"^\d+$", after, re.MULTILINE))
        self.check(
            "b opener restart",
            back
            and "hello from the fixture" in during
            and mount_id.strip() in during
            and remounted
            and "hello from the fixture" in after
            and stacked == 1,
            f"the mount outlived the opener; the restarted opener detached the dead "
            f"mount and mounted anew ({stacked} mount at the target)",
        )

    def opener_over_dead_mount(self) -> None:
        """The opener restarts while rclone is down: its predecessor's mount
        is dead, and every stat of it fails. The opener must detach it, serve
        again, and still be there to clean up when the Pod goes."""
        name = "b-dead"
        self.start(pod_b(name, self.node_image("opener"), self.node_image("rclone")))
        self.kill_container(name, "srw-cloud-mount")
        self.wait_for(
            "rclone's first restart",
            lambda: self.restarts(name, "srw-cloud-mount") == (1, True),
            60,
        )
        # A second crash waits out the kubelet's back-off (10 s): the mount
        # stays dead meanwhile.
        self.kill_container(name, "srw-cloud-mount")
        dead = self.wait_for(
            "the mount to be dead",
            lambda: "Transport endpoint is not connected"
            in self.sh(name, "workspace", "ls /cloud/root 2>&1"),
            10,
        )
        self.kill_container(name, "srw-fuse-opener")
        back = self.wait_for(
            "the opener to serve again",
            lambda: self.restarts(name, "srw-fuse-opener") == (1, True),
            30,
        )
        opener_log = self.logs(name, "srw-fuse-opener")
        recovered = self.wait_for(
            "rclone to remount after its back-off",
            lambda: self.restarts(name, "srw-cloud-mount") == (2, True),
            90,
        )
        read = self.sh(name, "workspace", "cat /cloud/root/docs/readme.txt 2>&1")
        detached = "detached 1 dead mount(s)" in opener_log
        self.check(
            "b opener over a dead mount",
            dead
            and back
            and detached
            and recovered
            and "hello from the fixture" in read,
            f"dead={dead}, opener ready again={back}, detached on start={detached}, "
            f"rclone remounted={recovered}; opener: "
            + " | ".join(x for x in opener_log.splitlines() if "detached" in x)[:200],
        )
        self.delete_and_check("b-dead delete (no stuck Terminating)", name)

    def detach_on_node(self, uid: str) -> int:
        """Lazily unmount a spike Pod's FUSE mounts on the node (its own)."""
        if uid not in self.pod_uids:
            raise SpikeError(f"Pod {uid} is not this run's")
        mountinfo = node("cat", "/proc/self/mountinfo", check=True).stdout
        count = 0
        for mountpoint, fstype in pod_mounts(mountinfo, uid):
            if fstype.startswith("fuse."):
                node("umount", "-l", mountpoint, check=True)
                count += 1
        return count

    def devfd(self) -> None:
        name = "b1-devfd"
        self.start(
            pod_b(
                name, self.node_image("opener"), self.node_image("rclone"), devfd=True
            )
        )
        out = self.sh(
            name,
            "workspace",
            "cat /cloud/root/docs/readme.txt; touch /cloud/root/x 2>&1; true",
        )
        fd = self.sh(
            name,
            "srw-cloud-mount",
            'ls -l /proc/$(pidof rclone)/fd/3; tr "\\0" " " < /proc/$(pidof rclone)/cmdline',
        )
        self.check(
            "b1 devfd",
            "hello from the fixture" in out
            and "Read-only file system" in out
            and "/dev/fuse" in fd
            and "/dev/fd/3" in fd,
            "rclone mount2 serves /dev/fd/3 (fd 3 -> /dev/fuse), read-only",
        )
        self.delete_and_check("b1 delete", name)
        name = "b1-noallow"
        pod = pod_b(
            name,
            self.node_image("opener"),
            self.node_image("rclone"),
            devfd=True,
            allow_non_empty=False,
        )
        rclone = next(
            c for c in pod["spec"]["initContainers"] if c["name"] == "srw-cloud-mount"
        )
        # The kubelet keeps the log tail in the status; a crash-looping
        # container's previous log is often gone already.
        rclone["terminationMessagePolicy"] = "FallbackToLogsOnError"
        self.apply(pod)
        uid = kubectl(
            "get", "pod", name, "-o", "jsonpath={.metadata.uid}"
        ).stdout.strip()
        self.pod_uids.add(uid)

        seen: list[str] = []

        def failure_recorded() -> bool:
            # Kept as seen: the status flickers while the container restarts.
            message = kubectl(
                "get",
                "pod",
                name,
                "-o",
                'jsonpath={.status.initContainerStatuses[?(@.name=="srw-cloud-mount")]'
                ".lastState.terminated.message}",
            ).stdout
            if "/dev/fd/3" in message:
                seen.append(message)
            return bool(seen)

        failed = self.wait_for("rclone's failure in the status", failure_recorded, 90)
        error = seen[0] if seen else ""
        self.check(
            "b1 devfd needs --allow-non-empty",
            failed and "/dev/fd/3" in error,
            " ".join(error.split())[-240:],
        )
        self.delete_and_check("b1-noallow delete", name)

    def privileged_workspace(self) -> None:
        name = "b-privws"
        self.start(
            pod_b(
                name,
                self.node_image("opener"),
                self.node_image("rclone"),
                privileged_workspace=True,
            )
        )
        out = self.sh(
            name,
            "workspace",
            "C=/cloud/root; touch $C/a 2>&1; mount -o remount,rw $C; echo remount=$?; touch $C/b 2>&1; true",
        )
        log = self.sh(name, "srw-cloud-mount", "cat /tmp/rclone.log")
        reached = [line for line in log.splitlines() if 'Create: name="b"' in line]
        self.check(
            "privws (today's profile)",
            "remount=0" in out and bool(reached) and "read-only file system" in log,
            "root in a privileged workspace remounted rw; the write reached rclone, "
            "which refused it only through --read-only",
        )
        self.delete_and_check("privws delete", name)

    def cleanup(self) -> None:
        """Remove what this run created, and only that. Every step is
        best-effort: one failing never skips the next."""
        if self.keep:
            print("--keep: leaving the namespace", flush=True)
            return
        problems: list[str] = []

        def attempt(what: str, step: Callable[[], Any]) -> None:
            try:
                step()
            except (SpikeError, OSError, subprocess.SubprocessError) as exc:
                problems.append(f"{what}: {exc}")

        if self.created_namespace:
            for uid in sorted(self.pod_uids):
                attempt(f"detach {uid}", lambda uid=uid: self.detach_on_node(uid))
            attempt(
                "delete the namespace",
                lambda: kubectl(
                    "delete",
                    "namespace",
                    NAMESPACE,
                    "--wait=true",
                    "--timeout=180s",
                    namespaced=False,
                    timeout=200,
                    check=True,
                ),
            )
        for repository in sorted(self.owned_repositories):
            attempt(
                f"registry {repository}",
                lambda repository=repository: run(
                    [
                        "docker",
                        "exec",
                        REGISTRY_CONTAINER,
                        "rm",
                        "-rf",
                        f"{REGISTRY_REPOSITORIES}/{repository}",
                    ],
                    check=True,
                ),
            )
        for key in self.pushed_keys:
            attempt(
                f"node image {key}",
                lambda key=key: node("crictl", "rmi", self.node_image(key)),
            )
        for ref in self.built_refs:
            attempt(f"local tag {ref}", lambda ref=ref: run(["docker", "rmi", ref]))
        absent: list[bool] = [not self.created_namespace]
        if self.created_namespace:
            attempt(
                "read the namespace",
                lambda: absent.append(
                    kubectl("get", "namespace", NAMESPACE, namespaced=False).returncode
                    != 0
                ),
            )
        gone = absent[-1]
        left: list[str] = []
        attempt(
            "read node mounts",
            lambda: left.extend(
                m for uid in self.pod_uids for m in self.node_mounts(uid)
            ),
        )
        self.check(
            "cleanup",
            gone and not left and not problems,
            f"namespace deleted={gone} (created by this run: {self.created_namespace}), "
            f"node mounts left {left}, problems {problems}",
        )

    def run(self, phases: frozenset[str] = PHASES) -> int:
        try:
            self.preflight()
            self.build_images()
            self.namespace()
            self.fixture()
            if "a" in phases:
                self.exercise(
                    "a",
                    pod_a("a-mount", self.node_image("upstream")),
                    sidecar="cloud-mount",
                    cloud="/home/agent/cloud/root",
                    rclone_log="/tmp/rclone.log",
                )
            for approach in ("a", "b"):
                if approach == "b" and "b" in phases:
                    self.exercise(
                        "b",
                        pod_b(
                            "b-mount",
                            self.node_image("opener"),
                            self.node_image("rclone"),
                        ),
                        sidecar="srw-cloud-mount",
                        cloud="/cloud/root",
                        rclone_log="/tmp/rclone.log",
                    )
                for mode in ("crash", "hang"):
                    if f"{approach}-{mode}" in phases:
                        self.termination(approach, mode)
            if "b-dead" in phases:
                self.opener_over_dead_mount()
            if "b1" in phases:
                self.devfd()
            if "privws" in phases:
                self.privileged_workspace()
        except SpikeError as exc:
            self.check("spike", False, str(exc))
        finally:
            self.cleanup()
        failed = [name for name, ok, _ in self.results if not ok]
        print(
            f"{len(self.results) - len(failed)} passed, {len(failed)} failed",
            flush=True,
        )
        return 1 if failed else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--context", default=LOCAL_CONTEXT)
    parser.add_argument("--namespace", default=NAMESPACE)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--confirm")
    parser.add_argument("--run-id")
    parser.add_argument(
        "--only",
        help=f"comma-separated phases to run (default all): {','.join(sorted(PHASES))}",
    )
    parser.add_argument(
        "--keep", action="store_true", help="skip cleanup (privileged Pods stay!)"
    )
    return parser


def phases_of(args: argparse.Namespace) -> frozenset[str]:
    if args.only is None:
        return PHASES
    return frozenset(p.strip() for p in args.only.split(",") if p.strip())


def validate(args: argparse.Namespace) -> None:
    if args.context != LOCAL_CONTEXT:
        raise SafetyError(f"this spike is restricted to {LOCAL_CONTEXT}")
    if args.namespace != NAMESPACE or args.namespace in FORBIDDEN_NAMESPACES:
        raise SafetyError(f"this spike only creates and deletes {NAMESPACE}")
    if args.run and args.confirm != LOCAL_CONFIRMATION:
        raise SafetyError(f"--run requires --confirm {LOCAL_CONFIRMATION}")
    if not args.run and args.confirm is not None:
        raise SafetyError("--confirm is accepted only with --run")
    if args.run_id is not None and not _RUN_ID_RE.fullmatch(args.run_id):
        raise SafetyError("--run-id must be d7- followed by 10 hex digits")
    unknown = phases_of(args) - PHASES
    if unknown or not phases_of(args):
        raise SafetyError(f"unknown phases {sorted(unknown)}; known: {sorted(PHASES)}")


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
    run_id = args.run_id or f"d7-{secrets.token_hex(5)}"
    return Spike(run_id=run_id, keep=args.keep).run(phases_of(args))


if __name__ == "__main__":
    raise SystemExit(main())
