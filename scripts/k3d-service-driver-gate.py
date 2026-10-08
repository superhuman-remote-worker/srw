#!/usr/bin/env python3
"""Local k3d gate for connector drivers D5: service-plane hosting.

Design: knowledge-base/knowledge/features/connector_drivers.md, "Three
planes" (service), "Reachability", "The driver namespace baseline", "Driver
versions" and slice D5.
Template: scripts/k3d-credential-lease-gate.py (C2) -- the same safety
envelope: dry-run by default, the exact k3d-srw/srw context, secrets only on
``kubectl exec -i`` stdin and scrubbed from every printed line, every
in-pod program capping its own memory, and a cleanup in ``finally`` that
touches only what this run created and then checks for residue by gate id.

It needs the k3d profile of deployment/values-local.yaml.example (keys in
--help) and Tilt (which builds srw-driver-shim and srw-driver-echo and pins
both by digest), plus a local docker that can push to localhost:5005 for the
moved-tag phase.

Fixtures (all disposable, named after the gate id): a project (tier
internet-only); echo connectors A and B (srw.echo-service/v1) with random
fake secrets, declaring egress to --egress-host:--egress-port; echo connector
C for the moved tag; stateless session 1 with A and B, session 2 with A only;
a busybox probe pod in the release namespace; an echo image pushed as
localhost:5005/srw-driver-echo:<gate id> (a compatible build, then an
incompatible one under the same tag).

Checks (each printed PASS/FAIL; the exit status is 0 only if all pass):

  preflight   every orchestrator pod serves this checkout's D5 modules, byte
              for byte; hosting on with an exchange port, a digest-pinned shim
              image, the echo driver, a short idle time and the k3d registry
              reachable over HTTP; migrations 0360-0363 applied; the driver
              namespace has Pod Security baseline and its static default deny,
              and the cluster enforces it: a busybox pod under that deny never
              reaches the orchestrator's API port (without enforcement every
              driver pod's canary wait refuses to start the driver)
  pod         session 1 binds A and B; A gets exactly one service pod in the
              driver namespace, ready, its canary wait passed (exit 0, "default
              deny enforced"), no ServiceAccount token (spec and in the pod),
              every capability dropped (spec and CapEff/CapBnd 0, NoNewPrivs),
              the image by digest, the shim as its command, DNS off, the
              pinned host in hostAliases; the identity row matches the pod
  reach       the agent pod and session 1's workspace reach A's Service on
              srw-driver; neither reaches A's pod on its other port; a plain
              release-namespace pod reaches neither
  egress      from inside A's pod: the pinned host answers; a canary address
              and the orchestrator's API port are refused; the exchange port
              answers; no name resolves through DNS while the pinned name
              resolves (hostAliases) to the recorded addresses; the
              connector's egress view and the matrix show what is enforced
  exchange    A's pod calls the exchange with its sdi_ identity: A's lease
              returns A's secret (by digest only, no-store); B's lease is
              refused (driver_identity_of_another_connector)
  sharing     session 2 binds A too: still one A pod (shared); session 2's
              workspace reaches A but not B (one ingress policy per binding)
  moved-tag   a bind of C resolves the gate tag (compatible label) to its
              digest through the registry; after an incompatible image is
              pushed under the same tag, the next bind is refused ("changed its
              contract") and audited
  idle        both sessions end: A's and B's pods go idle, are stopped after
              the idle time with reason idle, their objects are gone and
              recorded removed, and A's former identity is refused by the
              exchange (driver_identity_revoked)
  cleanup     sessions, connectors (identities and leases cascade), project,
              probe pod, the gate's image rows and local image tags are gone,
              and no object in the driver namespace names this run's
              connectors

Run with the repository venv on the k3d-srw cluster, alone: this is a
mutating gate.

  .venv/bin/python scripts/k3d-service-driver-gate.py           # plan
  .venv/bin/python scripts/k3d-service-driver-gate.py \\
      --run --confirm LOCAL-K3D-DISPOSABLE
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import secrets
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
LOCAL_CONTEXT = "k3d-srw"
LOCAL_NAMESPACE = "srw"
LOCAL_CONFIRMATION = "LOCAL-K3D-DISPOSABLE"
K = ["kubectl", f"--context={LOCAL_CONTEXT}", "-n", LOCAL_NAMESPACE]
ORCHESTRATOR = "deploy/srw-orchestrator"
ORCHESTRATOR_CONTAINER = "orchestrator"
ORCHESTRATOR_SERVICE = "srw-orchestrator"
POSTGRES_POD = "srw-postgres-0"
WORKSPACE_CONTAINER = "workspace"
AGENT_CONTAINER = "agent"
KEYCLOAK_TOKEN_URL = "http://srw-keycloak:8080/realms/srw/protocol/openid-connect/token"
POD_ROOT = "/app"
DEFAULT_MODEL = "muse-spark-1.3-contributor"
ECHO_TYPE = "echo_service"
ECHO_DRIVER = "srw.echo-service/v1"
EXCHANGE_PATH = "/v1/leases/exchange"
GATE_LABEL = "srw.io/gate"
LOCAL_REGISTRY = "localhost:5005"
CLUSTER_REGISTRY = "srw-registry:5000"
ECHO_REPOSITORY = "srw-driver-echo"
#: The k3d registry's container (scripts/local-dev-up.sh) and where it keeps
#: a repository's tags. It runs without REGISTRY_STORAGE_DELETE_ENABLED, and a
#: manifest delete by digest would untag every tag on that digest (Tilt's own
#: tag can share the gate's): the gate removes its tag's link only.
REGISTRY_CONTAINER = "srw-registry"
REGISTRY_TAGS = (
    "/var/lib/registry/docker/registry/v2/repositories/{repository}/_manifests/tags"
)
_SELECTOR = (
    "app.kubernetes.io/instance=srw,app.kubernetes.io/name=superhuman-remote-worker"
)
_GATE_ID_RE = re.compile(r"d5-[0-9a-f]{10}\Z")
_MODEL_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@-]{0,199}\Z")
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")
_HOST_RE = re.compile(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?\Z")
_IPV4_RE = re.compile(r"(?:\d{1,3}\.){3}\d{1,3}\Z")
ZERO_CAPS = "0000000000000000"
#: The service-pod migrations this gate needs.
MIGRATIONS = (
    "0360_connector_driver_images.sql",
    "0361_connector_service_pods.sql",
    "0362_validate_connector_service_pods.sql",
    "0363_connector_service_pod_key_idx.notx.sql",
)
#: An incompatible spec label: another protocol major, the secret slot gone.
INCOMPATIBLE_SPEC = json.dumps(
    {
        "name": ECHO_DRIVER,
        "protocol_version": "2.0",
        "config_schema": {"type": "object"},
        "credential_slots": [],
    },
    separators=(",", ":"),
)

SHARED_CONNECTORS = "src/shared/connectors"
DRIVERS = "src/orchestrator/services/connector_drivers"


@dataclass(frozen=True)
class ServedSet:
    label: str
    component: str
    container: str
    dirs: tuple[str, ...]
    files: tuple[str, ...]


SERVED_SETS = (
    ServedSet(
        "orchestrator",
        "orchestrator",
        ORCHESTRATOR_CONTAINER,
        (SHARED_CONNECTORS, DRIVERS),
        (
            "src/shared/oci_registry.py",
            "src/orchestrator/application/__init__.py",
            "src/orchestrator/application/background_tasks.py",
            "src/orchestrator/application/connectors.py",
            "src/orchestrator/application/projects.py",
            "src/orchestrator/application/settings.py",
            "src/orchestrator/routers/datasources.py",
            "src/orchestrator/services/connector_credential_leases.py",
            "src/orchestrator/services/connector_driver_identities.py",
            "src/orchestrator/services/connector_egress.py",
            "src/orchestrator/services/connector_lease_exchange.py",
            "src/orchestrator/services/connector_service_hosting.py",
            "src/orchestrator/services/connector_service_images.py",
            "src/orchestrator/services/connector_service_launch.py",
            *(
                f"src/orchestrator/database/migrations/app/{name}"
                for name in MIGRATIONS
            ),
        ),
    ),
    # The agent reads the echo driver's spec to materialize its lease entry.
    ServedSet(
        "stateless agent",
        "agent-stateless",
        AGENT_CONTAINER,
        (SHARED_CONNECTORS,),
        (),
    ),
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
    label = " ".join(args[4:7]) if args[:1] == ["kubectl"] else " ".join(args[:2])
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
        label = " ".join(args[4:7]) if args[:1] == ["kubectl"] else " ".join(args[:2])
        raise GateError(f"{label[:80]} failed (exit {rc}): {err.strip()[-400:]}")
    return out


def sql(query: str) -> str:
    """One statement on the app database (no secret may be in ``query``)."""
    return command(
        K
        + ["exec", POSTGRES_POD, "--", "psql", "-U", "srw", "-d", "srw"]
        + ["-v", "ON_ERROR_STOP=1", "-tAc", query]
    )


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

# Every program the gate runs inside a pod starts with this and calls
# cap_memory() once its imports are done: the pod serves the product
# meanwhile, and a gate program that grows must fail with a MemoryError,
# never take the pod to the OOM killer (as in the C0 to C2 gates).
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
    with opener.open(request, timeout=180) as response:
        status, text = response.status, response.read().decode("utf-8", "replace")
except urllib.error.HTTPError as error:
    status, text = error.code, error.read().decode("utf-8", "replace")
print(json.dumps({"status": status, "body": text}))
"""
)

# Network probes from inside a pod (the orchestrator, an agent pod or a
# workspace): an HTTP call or a bare TCP connect. A call may carry a lease
# token, decrypted here from its row (in the orchestrator only) and never
# printed; any credential in an answer comes back as its SHA-256 only.
_NET_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import hashlib, json, socket, sys, urllib.error, urllib.request
cap_memory()
request = json.loads(sys.stdin.readline())
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

def lease_token(lease_id):
    import asyncio
    from uuid import UUID
    from orchestrator.database.postgres import PostgresDB
    from orchestrator.security.crypto import decrypt

    async def read():
        db = PostgresDB(min_connections=1, max_connections=1)
        await db.connect()
        try:
            async with db.acquire() as conn:
                return await conn.fetchval(
                    "SELECT token_ciphertext FROM connector_credential_leases "
                    "WHERE id = $1", UUID(lease_id),
                )
        finally:
            await db.close()
    ciphertext = asyncio.run(read())
    return decrypt(ciphertext) if ciphertext else None

def http(call):
    body = call.get("body")
    if body is not None and call.get("lease_id"):
        body = {**body, "lease_token": lease_token(call["lease_id"])}
    headers = {"Content-Type": "application/json"}
    if call.get("identity"):
        headers["Authorization"] = "Bearer " + call["identity"]
    req = urllib.request.Request(
        call["url"], data=None if body is None else json.dumps(body).encode(),
        method=call.get("method", "GET"), headers=headers,
    )
    try:
        with opener.open(req, timeout=call.get("timeout", 8)) as response:
            status, raw, cache = (
                response.status, response.read(65536),
                response.headers.get("Cache-Control"),
            )
    except urllib.error.HTTPError as error:
        status, raw, cache = error.code, error.read(65536), error.headers.get("Cache-Control")
    except Exception as error:
        return {"status": 0, "error": type(error).__name__ + ": " + str(error)[:200]}
    try:
        parsed = json.loads(raw.decode("utf-8", "replace")) if raw else {}
    except ValueError:
        parsed = {"raw": raw[:200].decode("utf-8", "replace")}
    if isinstance(parsed, dict) and isinstance(parsed.get("credential"), str):
        credential = parsed.pop("credential")
        parsed["credential_sha256"] = hashlib.sha256(credential.encode()).hexdigest()
    return {"status": status, "cache_control": cache, "body": parsed}

def connect(call):
    try:
        with socket.create_connection((call["host"], call["port"]), timeout=call.get("timeout", 4)):
            return {"reachable": True}
    except OSError as error:
        return {"reachable": False, "error": type(error).__name__}

results = []
for call in request["calls"]:
    results.append(http(call) if call["kind"] == "http" else connect(call))
print(json.dumps(results))
"""
)

# The moved-tag phase, in the orchestrator: bind connector C to the gate's
# image reference through the deployed resolver, image record and check. The
# reference is configured for this process only; the server's own driver
# keeps its image.
_BIND_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import asyncio, json, sys
from orchestrator.database.postgres import PostgresDB
from orchestrator.services import connector_credential_leases as leases
from orchestrator.services import connector_service_images as images
from shared.connectors.builtin import ECHO_SERVICE_SPEC
from shared.oci_registry import DEFAULT_TOKEN_HOSTS, RegistryResolver
cap_memory()
request = json.loads(sys.stdin.readline())

async def main():
    db = PostgresDB(min_connections=1, max_connections=3)
    await db.connect()
    images.configure_service_images(images.ServiceImageSettings(
        references={ECHO_SERVICE_SPEC.name: request["reference"]},
        resolver=RegistryResolver(
            hosts=None, insecure_hosts=set(request["insecure_hosts"]),
            private_hosts=set(request["insecure_hosts"]),
            token_hosts=DEFAULT_TOKEN_HOSTS, same_host_tokens=True, timeout=20,
        ),
        cache_seconds=30, timeout_seconds=30, store=db,
    ))
    try:
        owner = leases.LeaseOwner.thread(request["owner"])
        entry = {"type": "echo_service", "datasource_id": request["connector"]}
        # As a dispatch does: the image is decided before the transaction,
        # and the bind inside it applies the decision.
        await leases.prepare_lease_delivery(db, [entry], owner=owner)
        async with db.acquire() as conn:
            try:
                async with conn.transaction():
                    digest = await images.bind_service_image(
                        conn, spec=ECHO_SERVICE_SPEC,
                        connector_id=request["connector"], owner=owner,
                    )
            except images.ServiceImageRefused as exc:
                return {"refused": str(exc)}
            if request.get("record"):
                # The binding the next bind compares against; revoked at once
                # so no pod starts for it.
                await leases.issue_or_redeliver(
                    conn, owner=owner, connector_id=request["connector"],
                    driver=ECHO_SERVICE_SPEC.name, access="ReadWrite",
                    image_digest=digest,
                )
                await leases.revoke_connector_leases(
                    conn, owner=owner, connector_ids=[request["connector"]],
                )
            return {"digest": digest}
    finally:
        await db.close()
print(json.dumps(asyncio.run(main())))
"""
)

_HASH_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import hashlib, json, sys
from pathlib import Path
cap_memory()
root = Path(sys.argv[1])
request = json.loads(sys.stdin.read())
expected = request["files"]
stale = sorted(
    path for path, digest in expected.items()
    if not (root / path).is_file()
    or hashlib.sha256((root / path).read_bytes()).hexdigest() != digest
)
extra = sorted(
    str(found.relative_to(root))
    for directory in request["dirs"] if (root / directory).is_dir()
    for found in (root / directory).rglob("*")
    if found.is_file() and "__pycache__" not in found.parts
    and str(found.relative_to(root)) not in expected
)
print(json.dumps({"stale": stale, "extra": extra}))
"""
)

# A busybox pod in the release namespace with no SRW label: the driver's
# ingress admits it nowhere. "blocked" is a timeout or a refusal.
_NETPROBE_SCRIPT = r"""
for i in 1 2 3; do
  if wget -q -T 4 -O /dev/null "http://$1:$2/healthz"; then echo driver=http; else echo driver=blocked; fi
  sleep 2
done
"""


# A busybox pod in the driver namespace, under its static default deny and
# nothing else: it must not reach the orchestrator's API port. "open" is any
# TCP answer (an HTTP status included), "closed" a refusal or a timeout. The
# first rounds may race the policy's arrival (kube-router applies it after the
# pod starts); the verdict is the last rounds.
_DENYPROBE_SCRIPT = r"""
for i in 1 2 3 4 5 6 7 8 9 10 11 12; do
  out=$(wget -q -T 3 -O /dev/null "http://$1:$2/" 2>&1)
  if [ $? -eq 0 ] || echo "$out" | grep -q "server returned error"; then
    echo canary=open
  else
    echo canary=closed
  fi
  sleep 2
done
"""
DENYPROBE_SETTLED = 5


def parse_denyprobe(log: str) -> bool:
    """Whether the default deny held in the probe's last rounds."""
    verdicts = [
        line.split("=", 1)[1] for line in log.splitlines() if line.startswith("canary=")
    ]
    if len(verdicts) < DENYPROBE_SETTLED:
        raise GateError("the default-deny probe printed too few verdicts")
    return all(verdict == "closed" for verdict in verdicts[-DENYPROBE_SETTLED:])


def pod_diagnosis(pod: dict | None, canary_log: str = "") -> str:
    """Why a driver pod is not ready, from its status and canary log."""
    if not pod:
        return "no pod"
    status = pod.get("status") or {}
    parts = [f"phase={status.get('phase')}"]
    for item in status.get("initContainerStatuses") or []:
        state = item.get("state") or {}
        last = (item.get("lastState") or {}).get("terminated") or {}
        detail = next(iter(state), "?")
        reason = (state.get(detail) or {}).get("reason") or ""
        exit_code = (state.get("terminated") or {}).get("exitCode")
        parts.append(
            f"{item.get('name')}={detail}"
            + (f"/{reason}" if reason else "")
            + (f" exit={exit_code}" if exit_code is not None else "")
            + (
                f" restarts={item.get('restartCount')}"
                if item.get("restartCount")
                else ""
            )
            + (f" last-exit={last.get('exitCode')}" if last else "")
        )
    lines = [line for line in canary_log.splitlines() if line.strip()]
    if lines:
        parts.append(f"canary-wait log: {lines[-1].strip()[:300]}")
    return "; ".join(parts)


def parse_netprobe(log: str) -> bool:
    """Whether the probe reached the driver in its last round."""
    verdicts = [
        line.split("=", 1)[1] for line in log.splitlines() if line.startswith("driver=")
    ]
    if not verdicts:
        raise GateError("the network probe printed no verdict")
    return verdicts[-1] == "http"


def capabilities_dropped(pod: dict) -> list[str]:
    """Containers of a pod spec that keep a capability or may escalate."""
    spec = pod.get("spec") or {}
    problems = []
    for container in [
        *(spec.get("initContainers") or []),
        *(spec.get("containers") or []),
    ]:
        security = container.get("securityContext") or {}
        if (security.get("capabilities") or {}).get("drop") != ["ALL"]:
            problems.append(f"{container.get('name')}: capabilities not dropped")
        if (security.get("capabilities") or {}).get("add"):
            problems.append(f"{container.get('name')}: capabilities added")
        if security.get("allowPrivilegeEscalation") is not False:
            problems.append(f"{container.get('name')}: privilege escalation allowed")
        if security.get("privileged"):
            problems.append(f"{container.get('name')}: privileged")
    return problems


def token_mounted(pod: dict) -> bool:
    """Whether a pod spec mounts a ServiceAccount token anywhere."""
    spec = pod.get("spec") or {}
    if spec.get("automountServiceAccountToken") is not False:
        return True
    for volume in spec.get("volumes") or []:
        for source in (volume.get("projected") or {}).get("sources") or []:
            if "serviceAccountToken" in source:
                return True
    return any(
        "serviceaccount" in (mount.get("mountPath") or "")
        for container in [
            *(spec.get("initContainers") or []),
            *(spec.get("containers") or []),
        ]
        for mount in container.get("volumeMounts") or []
    )


def canary_passed(pod: dict) -> bool:
    """Whether the canary-wait init container ran first and exited 0."""
    spec = pod.get("spec") or {}
    first = (spec.get("initContainers") or [{}])[0].get("name")
    statuses = {
        status.get("name"): status
        for status in (pod.get("status") or {}).get("initContainerStatuses") or []
    }
    state = (statuses.get("canary-wait") or {}).get("state") or {}
    terminated = state.get("terminated") or {}
    return first == "canary-wait" and terminated.get("exitCode") == 0


def pod_ready(pod: dict) -> bool:
    statuses = (pod.get("status") or {}).get("containerStatuses") or []
    return (pod.get("status") or {}).get("phase") == "Running" and any(
        status.get("name") == "driver" and status.get("ready") for status in statuses
    )


def moved_tag_verdict(first: dict, second: dict) -> tuple[bool, str]:
    """A compatible bind returns a digest; the incompatible re-push is refused."""
    digest = first.get("digest") or ""
    refusal = second.get("refused") or ""
    ok = (
        bool(re.fullmatch(r"sha256:[0-9a-f]{64}", digest))
        and "changed its contract" in refusal
        and "protocol 2.0" in refusal
    )
    return ok, f"first={digest[:19] or first} second={refusal[:160] or second}"


class Api:
    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self.password = secret(password)

    def call(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        result = in_pod(
            ORCHESTRATOR,
            ORCHESTRATOR_CONTAINER,
            _API_PROGRAM,
            {
                "username": self.username,
                "password": self.password,
                "token_url": KEYCLOAK_TOKEN_URL,
                "method": method,
                "path": path,
                "body": body,
            },
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


def in_pod(
    target: str,
    container: str,
    program: str,
    payload: dict[str, Any],
    *,
    python: str = "python",
    timeout: int = 180,
) -> Any:
    out = command(
        K + ["exec", "-i", target, "-c", container, "--"] + [python, "-c", program],
        data=json.dumps(payload) + "\n",
        timeout=timeout,
    )
    return json.loads(out.splitlines()[-1])


def expected_bytes(served: ServedSet) -> dict[str, str]:
    """``{path: sha256}`` for every file of ``served`` in this checkout."""
    paths = set(served.files)
    for directory in served.dirs:
        for found in (ROOT / directory).rglob("*"):
            if found.is_file() and "__pycache__" not in found.parts:
                paths.add(str(found.relative_to(ROOT)))
    return {
        path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
        for path in sorted(paths)
    }


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

    @property
    def passed(self) -> bool:
        return bool(self.results) and all(ok for _, ok, _ in self.results)


PLAN = [
    "preflight: orchestrator pods serve this checkout's D5 modules; hosting "
    "on with an exchange port, a digest-pinned shim, the echo driver, a short "
    "idle time and the k3d registry over HTTP; migrations 0360-0363; the "
    "driver namespace has Pod Security baseline and its default deny",
    "pod: session 1 binds A and B; A has one ready service pod in the driver "
    "namespace: canary wait passed, no ServiceAccount token, capabilities "
    "dropped (spec and in the pod), image by digest, the shim as command, DNS "
    "off, the pinned host in hostAliases; its identity row matches",
    "reach: the agent pod and session 1's workspace reach A's Service on "
    "srw-driver, not A's pod on another port; a plain release-namespace pod "
    "reaches neither",
    "egress: from A's pod the pinned host answers, a canary address and the "
    "orchestrator API port are refused, the exchange port answers, DNS "
    "resolves nothing while the pinned name resolves to the recorded "
    "addresses; the connector egress view and the matrix show it",
    "exchange: A's pod exchanges A's lease with its identity (secret by digest, "
    "no-store); B's lease is refused as another connector's",
    "sharing: session 2 binds A: still one A pod; session 2's workspace "
    "reaches A but not B",
    "moved-tag: a bind of C follows the gate tag to its digest; an "
    "incompatible image pushed under it is refused at the next bind, audited",
    "idle: both sessions end; A's and B's pods stop with reason idle, objects "
    "gone, removal recorded; A's former identity is refused by the exchange",
    "cleanup: sessions, connectors, project, probe pod, image rows and local "
    "tags are gone; no driver-namespace object names this run's connectors",
]


class ServiceDriverGate:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.gate_id = args.gate_id or f"d5-{secrets.token_hex(5)}"
        self.report = Report(self.gate_id)
        self.api = Api(args.user, args.password)
        # Everything this run creates, recorded before it is created.
        self.connectors: dict[str, str] = {}  # label -> datasource id
        self.threads: dict[str, str] = {}  # label -> thread id
        self.project: str | None = None
        self.probe_pod = f"{self.gate_id}-netprobe"
        self.probe_started = False
        self.deny_probe = f"{self.gate_id}-denyprobe"
        self.deny_probe_started = False
        self.images_pushed = False
        self.user_id = ""
        self.namespace = ""
        self.exchange_port = 0
        self.idle_seconds = 0
        self.reconcile_seconds = 0
        self.orchestrator_ip = ""
        self.pods: dict[str, dict] = {}  # connector label -> identity row
        self.identity_tokens: dict[str, str] = {}
        self.secrets = {
            label: secret(f"d5-upstream-{label}-{secrets.token_hex(16)}")
            for label in ("a", "b", "c")
        }

    # -- naming and helpers ------------------------------------------------
    def name(self, label: str) -> str:
        return f"{self.gate_id} {label}"

    def digest(self, label: str) -> str:
        return hashlib.sha256(self.secrets[label].encode()).hexdigest()

    @property
    def kc(self) -> list[str]:
        """kubectl in the driver namespace."""
        return ["kubectl", f"--context={LOCAL_CONTEXT}", "-n", self.namespace]

    def release_pods(self, selector: str) -> list[dict]:
        listing = json.loads(
            command(K + ["get", "pods", "-l", selector, "-o", "json"])
        )["items"]
        return [pod for pod in listing if not pod["metadata"].get("deletionTimestamp")]

    def served_problems(self, pod: str, served: ServedSet) -> list[str]:
        found = json.loads(
            command(
                K
                + ["exec", "-i", pod, "-c", served.container, "--"]
                + ["python", "-c", _HASH_PROGRAM, POD_ROOT],
                data=json.dumps(
                    {"files": expected_bytes(served), "dirs": list(served.dirs)}
                ),
            ).splitlines()[-1]
        )
        return [f"{pod}: stale {path}" for path in found["stale"]] + [
            f"{pod}: extra {path}" for path in found["extra"]
        ]

    def orchestrator_env(self, name: str) -> str:
        rc, out, _err = run(
            K
            + ["exec", ORCHESTRATOR, "-c", ORCHESTRATOR_CONTAINER, "--"]
            + ["printenv", name],
            timeout=60,
        )
        return out.strip() if rc == 0 else ""

    def net(
        self, target: str, container: str, calls: list[dict], *, python: str = "python"
    ) -> list[dict]:
        return in_pod(target, container, _NET_PROGRAM, {"calls": calls}, python=python)

    def from_orchestrator(self, calls: list[dict]) -> list[dict]:
        return self.net(ORCHESTRATOR, ORCHESTRATOR_CONTAINER, calls)

    def agent_pod(self) -> str:
        pods = [
            pod["metadata"]["name"]
            for pod in self.release_pods(
                f"{_SELECTOR},app.kubernetes.io/component=agent-stateless"
            )
            if pod.get("status", {}).get("phase") == "Running"
        ]
        if not pods:
            raise GateError("no running stateless agent pod")
        return pods[0]

    def workspace_pod(self, thread: str) -> str:
        def probe() -> str | None:
            running = [
                pod["metadata"]["name"]
                for pod in self.release_pods(f"srw/thread-id={thread}")
                if pod.get("status", {}).get("phase") == "Running"
            ]
            return running[0] if len(running) == 1 else None

        return wait_for(f"workspace of {thread}", probe, timeout=300)

    def identity_rows(self, label: str) -> list[dict]:
        out = sql(
            "SELECT coalesce(json_agg(row_to_json(i) ORDER BY i.created_at), '[]') "
            "FROM (SELECT id, pod_name, pod_namespace, pod_uid, image_digest, "
            "image_reference, credential_generation, egress, ready_at IS NOT NULL "
            "AS ready, idle_since IS NOT NULL AS idle, revoked_at IS NOT NULL AS "
            "revoked, revoke_reason, removed_at IS NOT NULL AS removed, "
            "launch_error, created_at FROM connector_driver_identities WHERE "
            f"connector_id = {lit(self.connectors[label])} AND "
            "credential_generation IS NOT NULL) i"
        )
        return json.loads(out or "[]")

    def live_pods(self, label: str) -> list[dict]:
        return [row for row in self.identity_rows(label) if not row["revoked"]]

    def driver_pod(self, row: dict) -> dict | None:
        rc, out, _err = run(
            self.kc + ["get", "pod", row["pod_name"], "-o", "json"], timeout=60
        )
        return json.loads(out) if rc == 0 and out else None

    def service_url(self, label: str, path: str = "/") -> str:
        row = self.pods[label]
        return f"http://{row['pod_name']}.{self.namespace}.svc:8080{path}"

    def live_lease(self, label: str, thread: str) -> dict | None:
        out = sql(
            "SELECT coalesce(row_to_json(l)::text, '') FROM (SELECT id, image_digest "
            "FROM connector_credential_leases WHERE "
            f"connector_id = {lit(self.connectors[label])} AND thread_id = {lit(thread)} "
            "AND revoked_at IS NULL AND expires_at > now() LIMIT 1) l"
        )
        return json.loads(out) if out else None

    def create_session(self, label: str, connectors: list[str]) -> str:
        created = self.api.ok(
            "POST",
            "/api/persistent/threads",
            {
                "title": f"D5 service driver gate {self.gate_id} {label}",
                "permission_mode": "autonomous",
                "project_id": self.project,
                "datasource_ids": [self.connectors[c] for c in connectors],
                "config_override": {"workspace": {"backend": "sandbox"}},
                "model": self.args.model,
            },
        )
        thread = str(created.get("thread_id") or created["id"])
        self.threads[label] = thread
        print(f"session {label}: {thread}", flush=True)
        self.api.ok(
            "POST",
            f"/api/persistent/threads/{thread}/input",
            {"content": "Reply with the single word ready."},
        )
        for connector in connectors:
            wait_for(
                f"a lease for {connector} on {label}",
                lambda connector=connector: self.live_lease(connector, thread),
                timeout=self.args.turn_timeout,
                interval=5,
            )
        return thread

    def wait_ready_pod(self, label: str) -> dict:
        def probe() -> dict | None:
            live = self.live_pods(label)
            if len(live) != 1 or not live[0]["ready"]:
                return None
            pod = self.driver_pod(live[0])
            return live[0] if pod and pod_ready(pod) else None

        try:
            row = wait_for(
                f"connector {label}'s pod ready",
                probe,
                timeout=self.args.start_timeout,
                interval=5,
            )
        except GateError as exc:
            raise GateError(f"{exc} ({self.why_not_ready(label)})") from None
        self.pods[label] = row
        return row

    def why_not_ready(self, label: str) -> str:
        """The latest pod's state, its canary log and the recorded error."""
        rows = self.identity_rows(label)
        if not rows:
            return "no driver identity was minted (no binding, or the image refused)"
        row = rows[-1]
        rc, log, _err = run(
            self.kc + ["logs", row["pod_name"], "-c", "canary-wait", "--tail=5"],
            timeout=60,
        )
        recorded = (
            f"; stopped: {row['revoke_reason']} ({row['launch_error'] or 'no error'})"
            if row["revoked"]
            else ""
        )
        return pod_diagnosis(self.driver_pod(row), log if rc == 0 else "") + recorded

    def default_deny_enforced(self) -> bool:
        """A busybox pod under the driver namespace's default deny only."""
        self.deny_probe_started = True
        command(
            self.kc
            + ["run", self.deny_probe, "--image=busybox:1.36", "--restart=Never"]
            + [f"--labels={GATE_LABEL}={self.gate_id}"]
            + [
                "--overrides",
                json.dumps(
                    {
                        "spec": {
                            "activeDeadlineSeconds": 180,
                            "automountServiceAccountToken": False,
                        }
                    }
                ),
            ]
            + ["--command", "--", "sh", "-c", _DENYPROBE_SCRIPT, "denyprobe"]
            + [self.orchestrator_ip, "8085"]
        )

        def finished() -> bool:
            phase = command(
                self.kc
                + ["get", "pod", self.deny_probe, "-o", "jsonpath={.status.phase}"]
            )
            return phase in ("Succeeded", "Failed")

        wait_for("default-deny probe finished", finished, timeout=240, interval=5)
        return parse_denyprobe(command(self.kc + ["logs", self.deny_probe]))

    # -- phases --------------------------------------------------------------
    def preflight(self) -> None:
        problems: list[str] = []
        for served in SERVED_SETS:
            pods = self.release_pods(
                f"{_SELECTOR},app.kubernetes.io/component={served.component}"
            )
            if not pods:
                problems.append(f"no {served.label} pod")
            for pod in pods:
                name = pod["metadata"]["name"]
                statuses = pod.get("status", {}).get("containerStatuses") or []
                if pod.get("status", {}).get("phase") != "Running" or not all(
                    status.get("ready") for status in statuses
                ):
                    problems.append(f"{name} is not running and ready")
                    continue
                problems += self.served_problems(name, served)
        self.report.check(
            "preflight: orchestrator pods serve this checkout's D5 modules",
            not problems,
            "; ".join(problems)[:600],
        )
        if problems:
            raise GateError("the deployment does not serve this checkout")
        env = {
            name: self.orchestrator_env(name)
            for name in (
                "CONNECTOR_SERVICE_PODS_ENABLED",
                "CONNECTOR_SERVICE_NAMESPACE",
                "CONNECTOR_LEASE_EXCHANGE_PORT",
                "CONNECTOR_DRIVER_SHIM_IMAGE",
                "CONNECTOR_ECHO_DRIVER_IMAGE",
                "CONNECTOR_SERVICE_IDLE_SECONDS",
                "CONNECTOR_SERVICE_RECONCILE_SECONDS",
                "CONNECTOR_DRIVER_REGISTRY_INSECURE_HOSTS",
                "CONNECTOR_DRIVER_RESOLVE_CACHE_SECONDS",
            )
        }
        idle = float(env["CONNECTOR_SERVICE_IDLE_SECONDS"] or 0)
        configured = (
            env["CONNECTOR_SERVICE_PODS_ENABLED"].lower() == "true"
            and env["CONNECTOR_SERVICE_NAMESPACE"] != ""
            and env["CONNECTOR_LEASE_EXCHANGE_PORT"].isdigit()
            and int(env["CONNECTOR_LEASE_EXCHANGE_PORT"]) not in (0, 8085)
            and "@sha256:" in env["CONNECTOR_DRIVER_SHIM_IMAGE"]
            and env["CONNECTOR_ECHO_DRIVER_IMAGE"] != ""
            and 0 < idle <= self.args.max_idle
            and CLUSTER_REGISTRY
            in env["CONNECTOR_DRIVER_REGISTRY_INSECURE_HOSTS"].split(",")
        )
        self.report.check(
            "preflight: hosting on, an exchange port, a digest-pinned shim, the "
            "echo driver, a short idle time, the k3d registry over HTTP",
            configured,
            json.dumps(env),
        )
        if not configured:
            raise GateError(
                "set connectors.servicePods (enabled, idleSeconds <= "
                f"{self.args.max_idle}), connectors.drivers.registry.insecureHosts "
                "[srw-registry:5000], connectors.drivers.echo.enabled and "
                "orchestrator.connectorLeases.exchangePort as the k3d profile does, "
                "under Tilt"
            )
        self.namespace = env["CONNECTOR_SERVICE_NAMESPACE"]
        self.exchange_port = int(env["CONNECTOR_LEASE_EXCHANGE_PORT"])
        self.idle_seconds = int(idle)
        self.reconcile_seconds = int(
            float(env["CONNECTOR_SERVICE_RECONCILE_SECONDS"] or 15)
        )
        names = ", ".join(lit(name) for name in MIGRATIONS)
        applied = sql(
            "SELECT count(*) FROM schema_migrations WHERE success AND filename "
            f"IN ({names})"
        )
        self.report.check(
            "preflight: migrations 0360-0363 applied", applied == str(len(MIGRATIONS))
        )
        if applied != str(len(MIGRATIONS)):
            raise GateError("the service-pod migrations are not applied")
        namespace = json.loads(
            command(
                ["kubectl", f"--context={LOCAL_CONTEXT}", "get", "namespace"]
                + [self.namespace, "-o", "json"]
            )
        )
        labels = namespace["metadata"].get("labels") or {}
        deny = json.loads(
            command(
                self.kc
                + ["get", "networkpolicy", "srw-connectors-default-deny", "-o", "json"]
            )
        )
        self.report.check(
            "preflight: the driver namespace enforces Pod Security baseline and "
            "has its static default deny",
            labels.get("pod-security.kubernetes.io/enforce") == "baseline"
            and deny["spec"].get("podSelector") == {}
            and sorted(deny["spec"].get("policyTypes") or []) == ["Egress", "Ingress"],
            str({k: v for k, v in labels.items() if "pod-security" in k}),
        )
        self.orchestrator_ip = command(
            K + ["get", "svc", ORCHESTRATOR_SERVICE, "-o", "jsonpath={.spec.clusterIP}"]
        )
        if not _IPV4_RE.fullmatch(self.orchestrator_ip):
            raise GateError("the orchestrator Service has no IPv4 ClusterIP")
        enforced = self.default_deny_enforced()
        self.report.check(
            "preflight: the cluster enforces NetworkPolicy (a pod under the driver "
            "namespace's default deny is refused the orchestrator's API port)",
            enforced,
            "enforced"
            if enforced
            else "reached: k3s's network policy controller enforces nothing. "
            "After a k3d restart it may hold the node's old IP (k3s log "
            "'Successfully retrieved node IP(s)' differs from the node's "
            "InternalIP); restart the node: docker restart k3d-srw-server-0",
        )
        if not enforced:
            raise GateError("NetworkPolicy is not enforced on this cluster")
        self.user_id = sql(
            f"SELECT id FROM users WHERE preferred_username = {lit(self.args.user)}"
        )
        if not _UUID_RE.fullmatch(self.user_id):
            raise GateError(f"no user {self.args.user!r}")

    def fixture(self) -> None:
        created = self.api.ok(
            "POST",
            "/api/projects",
            {
                "name": self.name("project"),
                "description": "D5 service driver gate (disposable)",
                "user_id": self.user_id,
            },
        )
        self.project = str(created["id"])
        for label in ("a", "b", "c"):
            status, parsed = self.api.call(
                "POST",
                "/api/datasources",
                {
                    "name": self.name(f"echo-{label}"),
                    "type": ECHO_TYPE,
                    "scope_mode": "all",
                    "credentials": {"secret": self.secrets[label]},
                    "config": {
                        "host": self.args.egress_host,
                        "port": self.args.egress_port,
                        "message": f"{self.gate_id}-{label}",
                    },
                },
            )
            if isinstance(parsed, dict) and parsed.get("id"):
                self.connectors[label] = str(parsed["id"])
            if status not in (200, 201) or label not in self.connectors:
                raise GateError(f"echo connector {label}: HTTP {status} {parsed}")
        print(f"fixture: project {self.project}, connectors {self.connectors}")

    def pod_checks(self) -> None:
        thread = self.create_session("one", ["a", "b"])
        row = self.wait_ready_pod("a")
        self.wait_ready_pod("b")
        pod = self.driver_pod(row) or {}
        self.report.check(
            "pod: connector A has exactly one live service pod, in the driver "
            "namespace, ready",
            len(self.live_pods("a")) == 1
            and row["pod_namespace"] == self.namespace
            and pod.get("metadata", {}).get("uid") == row["pod_uid"]
            and pod_ready(pod),
            f"{row['pod_name']} uid={row['pod_uid']}",
        )
        lease = self.live_lease("a", thread) or {}
        self.report.check(
            "pod: the pod runs the binding's image digest, with the shim as its "
            "command",
            pod["spec"]["containers"][0]["image"].endswith("@" + row["image_digest"])
            and lease.get("image_digest") == row["image_digest"]
            and pod["spec"]["containers"][0]["command"][:2]
            == ["/srw/bin/srw-driver-shim", "serve"],
            pod["spec"]["containers"][0]["image"],
        )
        _rc, canary_log, _err = run(
            self.kc + ["logs", row["pod_name"], "-c", "canary-wait"], timeout=60
        )
        self.report.check(
            "pod: the canary-wait init container ran first and exited 0 after the "
            "default deny was enforced",
            canary_passed(pod) and "default deny enforced" in canary_log,
            canary_log.splitlines()[-1][:200] if canary_log else "no log",
        )
        caps = capabilities_dropped(pod)
        (facts,) = self.from_orchestrator(
            [{"kind": "http", "url": self.service_url("a", "/self")}]
        )
        process = (facts.get("body") or {}).get("process") or {}
        self.report.check(
            "pod: every capability dropped and no privilege escalation (spec and "
            "kernel)",
            not caps
            and process.get("CapEff") == ZERO_CAPS
            and process.get("CapBnd") == ZERO_CAPS
            and process.get("NoNewPrivs") == "1",
            "; ".join(caps) or json.dumps(process),
        )
        self.report.check(
            "pod: no ServiceAccount token (spec and filesystem)",
            not token_mounted(pod)
            and pod["spec"].get("serviceAccountName") == "srw-connector-driver"
            and (facts.get("body") or {}).get("service_account_token") is False,
            str(pod["spec"].get("serviceAccountName")),
        )
        aliases = {
            name
            for alias in pod["spec"].get("hostAliases") or []
            for name in alias.get("hostnames") or []
        }
        self.report.check(
            "pod: DNS off, the pinned host and the exchange in hostAliases",
            pod["spec"].get("dnsPolicy") == "None"
            and self.args.egress_host in aliases
            and f"{ORCHESTRATOR_SERVICE}.{LOCAL_NAMESPACE}.svc" in aliases,
            str(sorted(aliases)),
        )
        (root,) = self.from_orchestrator(
            [{"kind": "http", "url": self.service_url("a")}]
        )
        body = root.get("body") or {}
        self.report.check(
            "pod: the shim delivered the request file and the identity (the echo "
            "answers its connector, no secret)",
            body.get("driver") == ECHO_DRIVER
            and (body.get("connector") or {}).get("id") == self.connectors["a"]
            and body.get("has_identity") is True
            and self.secrets["a"] not in json.dumps(root),
            str({k: body.get(k) for k in ("driver", "plane", "has_identity")}),
        )

    def reach_checks(self) -> None:
        a = self.pods["a"]
        pod = self.driver_pod(a) or {}
        pod_ip = (pod.get("status") or {}).get("podIP") or ""
        calls = [
            {"kind": "http", "url": self.service_url("a", "/healthz")},
            {"kind": "connect", "host": pod_ip, "port": 9090},
        ]
        agent = self.net(self.agent_pod(), AGENT_CONTAINER, calls)
        self.report.check(
            "reach: an agent pod reaches A's Service on srw-driver but not A's "
            "other port",
            agent[0].get("status") == 200 and agent[1].get("reachable") is False,
            json.dumps(agent)[:300],
        )
        workspace = self.net(
            self.workspace_pod(self.threads["one"]),
            WORKSPACE_CONTAINER,
            calls,
            python="python3",
        )
        self.report.check(
            "reach: session 1's workspace reaches A's Service on srw-driver but "
            "not A's other port",
            workspace[0].get("status") == 200
            and workspace[1].get("reachable") is False,
            json.dumps(workspace)[:300],
        )
        self.probe_started = True
        command(
            K
            + ["run", self.probe_pod, "--image=busybox:1.36", "--restart=Never"]
            + [f"--labels={GATE_LABEL}={self.gate_id}", "--command", "--"]
            + ["sh", "-c", _NETPROBE_SCRIPT, "netprobe"]
            + [f"{a['pod_name']}.{self.namespace}.svc", "8080"]
        )

        def finished() -> bool:
            phase = command(
                K + ["get", "pod", self.probe_pod, "-o", "jsonpath={.status.phase}"]
            )
            return phase in ("Succeeded", "Failed")

        wait_for("network probe finished", finished, timeout=180, interval=5)
        reached = parse_netprobe(command(K + ["logs", self.probe_pod]))
        self.report.check(
            "reach: a release-namespace pod that is no agent and no bound "
            "workspace does not reach the driver",
            not reached,
            "reached" if reached else "blocked",
        )

    def egress_checks(self) -> None:
        a = self.pods["a"]
        egress = (
            a["egress"] if isinstance(a["egress"], dict) else json.loads(a["egress"])
        )
        pinned = next(
            (
                h
                for h in egress.get("hosts") or []
                if h["host"] == self.args.egress_host
            ),
            {},
        )
        port = self.args.egress_port
        probes = self.from_orchestrator(
            [
                {
                    "kind": "http",
                    "url": self.service_url(
                        "a", f"/probe?addr={self.args.egress_host}:{port}"
                    ),
                },
                {
                    "kind": "http",
                    "url": self.service_url("a", f"/probe?addr={self.args.canary}"),
                },
                {
                    "kind": "http",
                    "url": self.service_url(
                        "a", f"/probe?addr={self.orchestrator_ip}:8085"
                    ),
                },
                {
                    "kind": "http",
                    "url": self.service_url(
                        "a",
                        f"/probe?addr={self.orchestrator_ip}:{self.exchange_port}",
                    ),
                },
                {
                    "kind": "http",
                    "url": self.service_url(
                        "a", "/resolve?name=kubernetes.default.svc.cluster.local"
                    ),
                },
                {
                    "kind": "http",
                    "url": self.service_url(
                        "a", f"/resolve?name={self.args.egress_host}"
                    ),
                },
            ]
        )
        reach = [(p.get("body") or {}).get("reachable") for p in probes[:4]]
        self.report.check(
            "egress: from A's pod the pinned host answers, a canary address and "
            "the orchestrator API port are refused, the exchange port answers",
            reach == [True, False, False, True],
            str(reach),
        )
        no_dns = probes[4].get("body") or {}
        pinned_name = probes[5].get("body") or {}
        self.report.check(
            "egress: no DNS from A's pod; the pinned name resolves only to its "
            "recorded addresses",
            no_dns.get("resolved") is False
            and pinned_name.get("resolved") is True
            and sorted(pinned_name.get("addresses") or [])
            == sorted(pinned.get("addresses") or ["?"])
            and egress.get("dns") == "none",
            f"dns={no_dns.get('resolved')} pinned={pinned.get('addresses')} "
            f"answered={pinned_name.get('addresses')}",
        )
        view = self.api.ok("GET", f"/api/datasources/{self.connectors['a']}/egress")
        live = [pod for pod in view.get("pods") or [] if pod.get("live")]
        matrix = self.api.ok("GET", "/api/datasources/drivers")
        echo = next(
            (d for d in matrix.get("drivers") or [] if d["name"] == ECHO_DRIVER), {}
        )
        self.report.check(
            "egress: the connector's egress view shows the pinned addresses and "
            "their resolution time; the matrix shows pinned per pod and the "
            "start-up wait",
            len(live) == 1
            and live[0]["enforced"]["hosts"][0]["addresses"] == pinned.get("addresses")
            and live[0]["resolved_at"]
            and echo.get("egress", {}).get("enforced", {}).get("reason")
            == "pinned_per_pod"
            and echo.get("egress", {}).get("installation", {}).get("start_up_wait")
            is True,
            str(echo.get("egress", {}).get("installation")),
        )

    def exchange_checks(self) -> None:
        thread = self.threads["one"]
        a_lease = self.live_lease("a", thread) or {}
        b_lease = self.live_lease("b", thread) or {}
        calls = [
            {
                "kind": "http",
                "method": "POST",
                "url": self.service_url("a", "/exchange"),
                "lease_id": lease["id"],
                "body": {"operation": "read"},
            }
            for lease in (a_lease, b_lease)
        ]
        own, other = self.from_orchestrator(calls)
        own_body, other_body = own.get("body") or {}, other.get("body") or {}
        self.report.check(
            "exchange: A's pod exchanges A's lease with its sdi_ identity (secret "
            "by digest, no-store)",
            own_body.get("exchange_status") == 200
            and own_body.get("credential_sha256") == self.digest("a")
            and own_body.get("cache_control") == "no-store",
            str({k: own_body.get(k) for k in ("exchange_status", "cache_control")}),
        )
        self.report.check(
            "exchange: the exchange refuses A's identity for connector B's lease",
            other_body.get("exchange_status") == 403
            and other_body.get("error") == "driver_identity_of_another_connector",
            str(other_body),
        )
        # Kept for the idle phase: the identity must stop working with its pod.
        raw = command(
            self.kc
            + ["get", "secret", self.pods["a"]["pod_name"]]
            + ["-o", "jsonpath={.data.identity}"]
        )
        self.identity_tokens["a"] = secret(base64.b64decode(raw).decode())

    def sharing_checks(self) -> None:
        thread = self.create_session("two", ["a"])
        time.sleep(3 * self.reconcile_seconds)
        live = self.live_pods("a")
        self.report.check(
            "sharing: a second session's binding of A shares A's one pod",
            len(live) == 1 and live[0]["id"] == self.pods["a"]["id"],
            str([row["pod_name"] for row in live]),
        )
        calls = [
            {"kind": "http", "url": self.service_url("a", "/healthz")},
            {"kind": "http", "url": self.service_url("b", "/healthz"), "timeout": 5},
        ]
        own, other = self.net(
            self.workspace_pod(thread), WORKSPACE_CONTAINER, calls, python="python3"
        )
        self.report.check(
            "sharing: session 2's workspace reaches A (bound) but not B (one "
            "ingress policy per binding)",
            own.get("status") == 200 and other.get("status") != 200,
            f"a={own.get('status')} b={other.get('status') or other.get('error')}",
        )

    def build_echo(self, *, spec: str | None) -> str:
        """Build and push the echo image under the gate tag; its digest."""
        tag = f"{LOCAL_REGISTRY}/{ECHO_REPOSITORY}:{self.gate_id}"
        build = ["docker", "build", "-q", "-f", "docker/Dockerfile.driver-echo"]
        if spec is not None:
            build += ["--build-arg", f"SRW_DRIVER_SPEC={spec}"]
        self.images_pushed = True
        command(build + ["-t", tag, str(ROOT)], timeout=900)
        out = command(["docker", "push", tag], timeout=600)
        match = re.search(r"digest: (sha256:[0-9a-f]{64})", out)
        if match is None:
            raise GateError("docker push printed no digest")
        return match.group(1)

    def bind_c(self, *, record: bool) -> dict:
        return in_pod(
            ORCHESTRATOR,
            ORCHESTRATOR_CONTAINER,
            _BIND_PROGRAM,
            {
                "reference": f"{CLUSTER_REGISTRY}/{ECHO_REPOSITORY}:{self.gate_id}",
                "insecure_hosts": [CLUSTER_REGISTRY],
                "connector": self.connectors["c"],
                # A live thread to own the recorded binding; the refused bind
                # needs none.
                "owner": self.threads["one"],
                "record": record,
            },
            timeout=300,
        )

    def moved_tag_checks(self) -> None:
        pushed = self.build_echo(spec=None)
        first = self.bind_c(record=True)
        digest = str(first.get("digest") or "")
        reference = f"{CLUSTER_REGISTRY}/{ECHO_REPOSITORY}:{self.gate_id}"
        recorded = (
            sql(
                "SELECT spec->>'protocol_version' FROM connector_driver_images "
                f"WHERE reference = {lit(reference)} AND digest = {lit(digest)}"
            )
            if re.fullmatch(r"sha256:[0-9a-f]{64}", digest)
            else ""
        )
        # The pushed digest may be an index whose linux/amd64 child is bound.
        self.report.check(
            "moved-tag: a bind of C follows the gate tag through the registry to a "
            "digest, recorded with the image's compatible spec label",
            recorded == "1.0",
            f"pushed {pushed[:19]} bound {digest[:19]} label={recorded!r}",
        )
        self.build_echo(spec=INCOMPATIBLE_SPEC)
        second = self.bind_c(record=False)
        ok, detail = moved_tag_verdict(first, second)
        events = sql(
            "SELECT count(*) FROM security_events WHERE event_type = "
            f"'connector_driver_image_refused' AND resource_id = "
            f"{lit(self.connectors['c'])}"
        )
        self.report.check(
            "moved-tag: the incompatible image pushed under the same tag is "
            "refused at the next bind, and audited",
            ok and events != "0",
            detail,
        )

    def idle_checks(self) -> None:
        for label in ("two", "one"):
            thread = self.threads.get(label)
            if thread:
                self.api.ok("DELETE", f"/api/persistent/threads/{thread}?force=true")
        timeout = self.idle_seconds + 6 * self.reconcile_seconds + 120

        def stopped(label: str) -> dict | None:
            rows = [
                row
                for row in self.identity_rows(label)
                if row["id"] == self.pods[label]["id"]
            ]
            return rows[0] if rows and rows[0]["removed"] else None

        rows = {
            label: wait_for(
                f"connector {label}'s pod stopped",
                lambda label=label: stopped(label),
                timeout=timeout,
                interval=5,
            )
            for label in ("a", "b")
        }
        gone = {
            label: run(
                self.kc
                + ["get", "pod,service,secret,networkpolicy", "-l"]
                + [f"srw.io/driver-identity={rows[label]['id']}", "-o", "name"],
                timeout=60,
            )[1]
            for label in ("a", "b")
        }
        self.report.check(
            "idle: both pods stopped after the idle time with reason idle, their "
            "objects gone and the removal recorded",
            all(row["revoke_reason"] == "idle" for row in rows.values())
            and not any(gone.values()),
            str({k: (v["revoke_reason"], gone[k] or "gone") for k, v in rows.items()}),
        )
        lease = sql(
            "SELECT id FROM connector_credential_leases WHERE "
            f"connector_id = {lit(self.connectors['a'])} AND thread_id = "
            f"{lit(self.threads['one'])} ORDER BY issued_at DESC LIMIT 1"
        )
        (answer,) = self.from_orchestrator(
            [
                {
                    "kind": "http",
                    "method": "POST",
                    "url": f"http://127.0.0.1:{self.exchange_port}{EXCHANGE_PATH}",
                    "identity": self.identity_tokens["a"],
                    "lease_id": lease,
                    "body": {"operation": "read"},
                }
            ]
        )
        self.report.check(
            "idle: the stopped pod's identity is refused by the exchange",
            answer.get("status") == 401
            and (answer.get("body") or {}).get("error") == "driver_identity_revoked",
            str(answer.get("body")),
        )

    # -- cleanup -------------------------------------------------------------
    def titled_threads(self) -> list[str]:
        rows = sql(
            "SELECT id FROM threads WHERE "
            f"position({lit(self.gate_id)} in coalesce(title, '')) > 0"
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

    def cleanup(self) -> list[str]:
        problems: list[str] = []

        def step(label: str, action: Callable[[], Any]) -> None:
            try:
                if action() is False:
                    problems.append(label)
            except GateError as exc:
                problems.append(f"{label} ({exc})")

        for thread in dict.fromkeys([*self.threads.values(), *self.titled_threads()]):
            step(
                f"delete session {thread}",
                lambda thread=thread: self.delete_thread(thread),
            )
        for label, datasource_id in list(self.connectors.items()):

            def delete(datasource_id=datasource_id) -> bool:
                status, _body = self.api.call(
                    "DELETE", f"/api/datasources/{datasource_id}"
                )
                return status in (200, 204, 404)

            step(f"delete connector {label}", delete)
        if self.project:

            def project_deleted() -> bool:
                status, _body = self.api.call("DELETE", f"/api/projects/{self.project}")
                return status in (200, 204, 404)

            step(
                "delete project",
                lambda: bool(
                    wait_for(
                        "project deleted", project_deleted, timeout=180, interval=10
                    )
                ),
            )
        if self.deny_probe_started:
            step(
                "delete the default-deny probe pod",
                lambda: command(
                    self.kc
                    + ["delete", "pod", self.deny_probe]
                    + ["--ignore-not-found", "--wait=true", "--timeout=120s"]
                )
                is not None,
            )
        if self.probe_started:
            step(
                "delete network probe pod",
                lambda: command(
                    K
                    + ["delete", "pod", "-l", f"{GATE_LABEL}={self.gate_id}"]
                    + ["--ignore-not-found", "--wait=true", "--timeout=120s"]
                )
                is not None,
            )
        step(
            "delete the gate's image rows",
            lambda: sql(
                "DELETE FROM connector_driver_images WHERE reference = "
                + lit(f"{CLUSTER_REGISTRY}/{ECHO_REPOSITORY}:{self.gate_id}")
            )
            is not None,
        )
        if self.images_pushed:
            step("delete the gate's tag from the k3d registry", self.delete_pushed_tag)
            step(
                "remove the local image tag",
                lambda: run(
                    [
                        "docker",
                        "rmi",
                        f"{LOCAL_REGISTRY}/{ECHO_REPOSITORY}:{self.gate_id}",
                    ]
                )[0]
                in (0, 1),
            )
        for problem in problems:
            print(f"cleanup: {problem} failed", flush=True)
        return problems

    def registry_tags(self) -> list[str]:
        url = f"http://{LOCAL_REGISTRY}/v2/{ECHO_REPOSITORY}/tags/list"
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                return list(json.load(response).get("tags") or [])
        except (OSError, ValueError) as exc:
            raise GateError(f"registry tag list failed: {exc}") from None

    def delete_pushed_tag(self) -> bool:
        """Remove the gate's tag from the k3d registry, and nothing else."""
        tags = REGISTRY_TAGS.format(repository=ECHO_REPOSITORY)
        command(
            ["docker", "exec", REGISTRY_CONTAINER, "rm", "-rf"]
            + [f"{tags}/{self.gate_id}"]
        )
        return self.gate_id not in self.registry_tags()

    def residue(self) -> list[str]:
        """What this run created and cleanup did not remove."""
        left: list[str] = []
        if self.images_pushed and self.gate_id in self.registry_tags():
            left.append(f"registry tag {ECHO_REPOSITORY}:{self.gate_id}")
        titled = self.titled_threads()
        if titled:
            left.append(f"sessions titled with the gate id: {titled}")
        prefix = self.gate_id + " %"
        count = sql(f"SELECT count(*) FROM datasources WHERE name LIKE {lit(prefix)}")
        if count != "0":
            left.append(f"{count} connectors")
        ids = [value for value in self.connectors.values() if _UUID_RE.fullmatch(value)]
        if ids:
            listed = ", ".join(lit(value) for value in ids)
            for table in ("connector_credential_leases", "connector_driver_identities"):
                rows = sql(
                    f"SELECT count(*) FROM {table} WHERE connector_id IN ({listed})"
                )
                if rows != "0":
                    left.append(f"{rows} rows in {table}")
        images = sql(
            "SELECT count(*) FROM connector_driver_images WHERE reference LIKE "
            + lit(f"%:{self.gate_id}")
        )
        if images != "0":
            left.append(f"{images} image rows")
        if (
            self.project
            and sql(f"SELECT count(*) FROM projects WHERE id = {lit(self.project)}")
            != "0"
        ):
            left.append(f"project {self.project}")
        if self.namespace:
            for connector in ids:
                try:
                    wait_for(
                        f"driver objects of {connector} gone",
                        lambda connector=connector: not run(
                            self.kc
                            + ["get", "pod,service,secret,networkpolicy", "-l"]
                            + [f"srw.io/connector-id={connector}", "-o", "name"],
                            timeout=60,
                        )[1],
                        timeout=max(120, 6 * self.reconcile_seconds),
                        interval=5,
                    )
                except GateError:
                    left.append(f"driver objects of connector {connector}")
        selectors = [
            f"{GATE_LABEL}={self.gate_id}",
            *(f"srw/thread-id={thread}" for thread in self.threads.values()),
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
        if (
            self.deny_probe_started
            and json.loads(
                command(
                    self.kc
                    + [
                        "get",
                        "pods",
                        "-l",
                        f"{GATE_LABEL}={self.gate_id}",
                        "-o",
                        "json",
                    ]
                )
            )["items"]
        ):
            left.append(f"pods {GATE_LABEL}={self.gate_id} in {self.namespace}")
        return left

    # -- run -------------------------------------------------------------------
    def run(self) -> int:
        try:
            self.preflight()
            self.fixture()
            # pod_checks creates session 1 and the pods every later phase
            # needs; a failure there ends the run (cleanup still runs).
            self.pod_checks()
            for phase in (
                self.reach_checks,
                self.egress_checks,
                self.exchange_checks,
                self.sharing_checks,
                self.moved_tag_checks,
                self.idle_checks,
            ):
                try:
                    phase()
                except GateError as exc:
                    self.report.check(
                        f"{phase.__name__}: infrastructure", False, str(exc)
                    )
        except GateError as exc:
            self.report.check("gate infrastructure", False, str(exc))
        finally:
            if self.args.keep:
                print(
                    "kept: "
                    + json.dumps(
                        {
                            "connectors": self.connectors,
                            "threads": self.threads,
                            "project": self.project,
                        }
                    )
                )
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


VALUES_LOCAL_KEYS = """values-local.yaml keys (the k3d profile of values-local.yaml.example):
  orchestrator.connectorLeases.exchangePort: 8088
  connectors.servicePods.enabled: true
  connectors.servicePods.maxInstallation: 4
  connectors.servicePods.idleSeconds: 60
  connectors.servicePods.reconcileIntervalSeconds: 5
  connectors.drivers.registry.insecureHosts: ["srw-registry:5000"]
  connectors.drivers.registry.resolveCacheSeconds: 5
  connectors.drivers.echo.enabled: true
  connectors.drivers.echo.image: {repository: srw-registry:5000/srw-driver-echo, tag: dev}
Tilt overrides the shim and echo images (repository, tag, digest).
"""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog=VALUES_LOCAL_KEYS,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--context", default=LOCAL_CONTEXT)
    parser.add_argument("--namespace", default=LOCAL_NAMESPACE)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--confirm")
    parser.add_argument("--gate-id")
    parser.add_argument("--user", default="test")
    parser.add_argument("--password", default="srw-k3d-dev-test")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--turn-timeout", type=int, default=420)
    parser.add_argument("--start-timeout", type=int, default=300)
    parser.add_argument(
        "--max-idle",
        type=int,
        default=120,
        help="refuse a deployment whose service-pod idle time is longer (seconds)",
    )
    parser.add_argument(
        "--egress-host",
        default="one.one.one.one",
        help="the public host the echo connectors declare as their egress",
    )
    parser.add_argument("--egress-port", type=int, default=443)
    parser.add_argument(
        "--canary",
        default="9.9.9.9:443",
        help="an address:port the driver pod must not reach",
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
        raise SafetyError("--gate-id must be d5- followed by 10 hex digits")
    if not _MODEL_RE.fullmatch(args.model):
        raise SafetyError("model id is malformed")
    if not re.fullmatch(r"[a-z][a-z0-9._-]{0,63}", args.user):
        raise SafetyError("user name is malformed")
    if not _HOST_RE.fullmatch(args.egress_host):
        raise SafetyError("--egress-host must be a host name or IPv4 address")
    if not 1 <= args.egress_port <= 65535:
        raise SafetyError("--egress-port must be a port")
    host, _, port = args.canary.rpartition(":")
    if not _IPV4_RE.fullmatch(host) or not port.isdigit():
        raise SafetyError("--canary must be IPv4:PORT")
    if not 60 <= args.turn_timeout <= 1800:
        raise SafetyError("--turn-timeout must be between 60 and 1800 seconds")
    if not 60 <= args.start_timeout <= 1800:
        raise SafetyError("--start-timeout must be between 60 and 1800 seconds")
    if not 10 <= args.max_idle <= 900:
        raise SafetyError("--max-idle must be between 10 and 900 seconds")


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
        print(VALUES_LOCAL_KEYS)
        return 0
    return ServiceDriverGate(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
