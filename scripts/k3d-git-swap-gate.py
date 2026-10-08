#!/usr/bin/env python3
"""Local k3d gate for connector drivers C3: the git swap driver.

Design: knowledge-base/knowledge/features/connector_drivers.md, "The git swap
driver", "The lease service" and slice C3 (its gate). Templates:
scripts/k3d-managed-mcp-gate.py (D5a: the hosting preflights, the enforcement
and DNS probes, the safety envelope) and scripts/k3d-connector-selection-gate.py
(D3c: a disposable OAuth client). The same envelope: dry-run by default, the
exact k3d-srw/srw context, secrets only on ``kubectl exec -i`` stdin and
scrubbed from every printed line, every in-pod program capping its own memory,
and a cleanup in ``finally`` that touches only what this run created and then
checks for residue by gate id.

Why a real forge. A git swap driver pod may reach neither the cluster's pod
and service ranges nor (on k3d) the docker network the node and load
balancer live on: those are refused by design, so the in-cluster Gitea is out
of reach. It verifies its upstream's certificate against public roots (v1
has no private upstream CA), so an HTTPS git server this gate started on the
workstation would be refused too. The least-bad option is the operator's own
DISPOSABLE repository on a public forge (GitHub, GitLab.com, Gitea.com) and a
token that can read and push it, passed as a file. The gate pushes branches
named srw-gate-<gate id>[-...] there and deletes them again at cleanup,
directly with the token from this workstation (never through a workspace).

It needs the k3d profile of deployment/values-local.yaml.example (keys in
--help) and Tilt, which builds srw-driver-shim and srw-driver-git-swap and pins
them by digest.

Fixtures (all disposable, named after the gate id):

  client      ``<gate id>-oauth``, a public Keycloak client with direct
              access grants, which the owner logs in with (the D3c fixture)
  project     one project of the owner
  connectors  of the owner, repository connectors with token auth, all on
              the operator's token: ``rw`` (--upstream-url, linked ReadWrite),
              ``ro`` (the same URL, linked read-only, so it binds ReadOnly),
              ``redirect`` (--redirect-url, a repository whose forge answers
              git with a redirect) and ``private`` (--private-url, a host
              inside the cluster's service range, with a fake token)
  sessions    ``one`` (rw and ro) and ``two`` (rw, redirect and private),
              stateless

Checks (each printed PASS/FAIL; the exit status is 0 only if all pass):

  preflight   every orchestrator and stateless agent pod serves this
              checkout's C3 modules, byte for byte; hosting on with an
              exchange and canary port, a digest-pinned shim and swap image,
              SRW's driver CA mounted and the driver listed in the capability
              matrix; migrations applied; room for three pods; the driver
              namespace's baseline and default deny, enforced (the 12-probe
              harness); refusedCidrs covers the node; the forge host resolves
              and answers from the orchestrator
  startup     each swap pod's canary wait ran first and exited 0; the driver
              runs the pinned swap image with every capability dropped and no
              ServiceAccount token; its Secret holds its TLS certificate and
              key, the clean upstream URL, and no token; each binding's
              workspace has its own ingress policy; each connector's first
              binding has a serving pod within COLD_START_BUDGET seconds
              (measured and printed: the C3 review's cold start)
  push        session one cloned rw through the driver: its remote is the
              clean URL (``git remote -v`` shows the driver's, neither with a
              token), .git/config holds no token or lease and refuses
              credentials in URLs, ~/.gitconfig includes SRW's wiring once;
              a branch push lands upstream through the driver (its log
              records the ref under rw's lease); the workspace holds the forge
              token nowhere (files, environments, command lines); its README
              names the driver
  readonly    pushing ro is refused with "read-only"; raw requests with ro's
              lease get 403 for GET info/refs?service=git-receive-pack and for
              POST git-receive-pack, also with service=git-upload-pack in the
              query; nothing reached the upstream and the exchange saw no
              write for the lease
  refs        on rw, a tag push and a branch delete are refused with their
              reasons (git shows "! [remote rejected] ... (reason)"); the
              upstream still has the branch and no tag
  leases      session one's two connectors each use their own lease: inside
              each checkout (rw and ro name one upstream; each binding's rules
              apply in its own checkout only) the credential helper answers
              its driver URL with its own lease (by digest) and each driver
              pod logs its own lease id
  lfs         a Git LFS batch request to the driver gets 501 and the message
  ide         git outside the agent's tmux (an IDE terminal's bare
              environment, an ssh-gateway session's login shell) run in the
              checkout goes through the driver too
  redirect    session two's redirect connector: the forge's redirect is not
              followed with the credential (git shows the driver's 502 and
              its message; the driver logs the redirect); the README says
              the repository was not cloned and why
  fallback    session two's private connector falls back visibly: no lease,
              its README line says the token goes in the clone URL and why
              (the driver may not reach the upstream), and so does its Test
  reused      a pre-C3 checkout (an oauth2:<token>@ origin, no
              credentialsInUrl) loses its token URL on session two's next
              attach and refuses credentials in URLs again
  revoked     rw is detached from session one: its lease is revoked, and
              the token gets 401 from the driver (presented from session
              two's workspace, which stays admitted) within the revocation
              lag, while session two's own lease still gets 200; session
              one's wiring for rw is removed
  cleanup     sessions, connectors (identities and leases cascade), project,
              OAuth client and probe pods are gone; no driver-namespace object
              names this run's connectors; the upstream has no srw-gate-<gate
              id> ref left

Run with the repository venv on the k3d-srw cluster, alone: this is a
mutating gate.

  .venv/bin/python scripts/k3d-git-swap-gate.py           # plan
  .venv/bin/python scripts/k3d-git-swap-gate.py \\
      --run --confirm LOCAL-K3D-DISPOSABLE \\
      --upstream-url https://github.com/<you>/<disposable>.git \\
      --upstream-token-file ~/.config/srw-gate/<disposable>.token
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import ipaddress
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import tempfile
import time
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
KEYCLOAK_TOKEN_URL = "http://srw-keycloak:8080/realms/srw/protocol/openid-connect/token"
POD_ROOT = "/app"
DEFAULT_MODEL = "muse-spark-1.3-contributor"
SWAP_DRIVER = "srw.git-swap/v1"
REPOSITORY_TYPE = "repository"
DRIVER_PORT = 8443
GATE_LABEL = "srw.io/gate"
WORKSPACE_HOME = "/home/agent-host"
_SELECTOR = (
    "app.kubernetes.io/instance=srw,app.kubernetes.io/name=superhuman-remote-worker"
)
_GATE_ID_RE = re.compile(r"c3-[0-9a-f]{10}\Z")
_MODEL_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@-]{0,199}\Z")
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")
#: A repository URL the driver serves, in the clean form a checkout keeps:
#: HTTPS, a lowercase host on port 443, a plain path, no trailing slash.
_UPSTREAM_RE = re.compile(
    r"https://[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?(?:/[A-Za-z0-9._~-]{1,128}){1,16}\Z"
)
_IPV4_RE = re.compile(r"(?:\d{1,3}\.){3}\d{1,3}\Z")
#: A token repository on a host inside the cluster's service range: the
#: driver's egress refuses it, so it must fall back (C3 review B1).
DEFAULT_PRIVATE_URL = (
    "https://traefik.kube-system.svc.cluster.local/srw-gate/private.git"
)
#: Seconds from a connector's first binding to its serving pod the gate
#: accepts (the agent's first clone waits for at most ~195 s).
COLD_START_BUDGET = 120
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
#: Forges the repository connector accepts (shared.connectors.builtin.FORGES).
FORGES = ("gitea", "github", "gitlab")
#: The migrations this gate needs: C2's leases, D5's service pods, D5a's re-pin.
MIGRATIONS = (
    "0346_connector_driver_identities.sql",
    "0347_connector_credential_leases.sql",
    "0360_connector_driver_images.sql",
    "0361_connector_service_pods.sql",
    "0362_validate_connector_service_pods.sql",
    "0363_connector_service_pod_key_idx.notx.sql",
    "0390_connector_service_pod_replacement.sql",
    "0391_connector_service_pod_key_v2_idx.notx.sql",
    "0392_drop_connector_service_pod_key_idx.notx.sql",
)

SHARED_CONNECTORS = "src/shared/connectors"
DRIVERS = "src/orchestrator/services/connector_drivers"
AGENT_CONNECTORS = "src/agent/connectors"


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
            "src/orchestrator/application/__init__.py",
            "src/orchestrator/application/connectors.py",
            "src/orchestrator/application/preparation.py",
            "src/orchestrator/application/settings.py",
            "src/orchestrator/services/agent_datasource_payload.py",
            "src/orchestrator/services/connector_credential_leases.py",
            "src/orchestrator/services/connector_driver_ca.py",
            "src/orchestrator/services/connector_git_swap_delivery.py",
            "src/orchestrator/services/connector_lease_exchange.py",
            "src/orchestrator/services/datasource_config.py",
            "src/orchestrator/services/connector_service_hosting.py",
            "src/orchestrator/services/connector_service_images.py",
            "src/orchestrator/services/connector_service_launch.py",
            *(
                f"src/orchestrator/database/migrations/app/{name}"
                for name in MIGRATIONS
            ),
        ),
    ),
    # The agent writes the workspace's wiring and runs the clone.
    ServedSet(
        "stateless agent",
        "agent-stateless",
        "agent",
        (SHARED_CONNECTORS, AGENT_CONNECTORS),
        (
            "src/shared/runtime/core/credential_env.py",
            "src/shared/runtime/core/workspace_backend.py",
            "src/shared/runtime/core/backends/remote.py",
            "src/agent/managers/git_manager.py",
            "src/agent/core/workspace.py",
        ),
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
    args: list[str],
    *,
    data: str | None = None,
    timeout: int = 180,
    env: dict[str, str] | None = None,
) -> tuple[int, str, str]:
    """Run argv; never echo argv or stdin. Output comes back scrubbed."""
    label = " ".join(args[4:7]) if args[:1] == ["kubectl"] else " ".join(args[:2])
    try:
        result = subprocess.run(
            args, input=data, text=True, capture_output=True, timeout=timeout, env=env
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
# never take the pod to the OOM killer (as in the C0 to D5a gates).
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
    "grant_type": "password", "client_id": envelope["client_id"], "scope": "openid",
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

# A lease's token, decrypted from its row in the orchestrator (its key never
# leaves the pod). The gate scrubs it from everything it prints and hands it
# to a workspace on stdin only.
_LEASE_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import asyncio, json, sys
from uuid import UUID
from orchestrator.database.postgres import PostgresDB
from orchestrator.security.crypto import decrypt
cap_memory()
request = json.loads(sys.stdin.readline())

async def main():
    db = PostgresDB(min_connections=1, max_connections=1)
    await db.connect()
    try:
        async with db.acquire() as conn:
            ciphertext = await conn.fetchval(
                "SELECT token_ciphertext FROM connector_credential_leases "
                "WHERE id = $1", UUID(request["lease_id"]),
            )
    finally:
        await db.close()
    return {"token": decrypt(ciphertext) if ciphertext else None}
print(json.dumps(asyncio.run(main())))
"""
)

# SRW's connector driver CA, loaded in the orchestrator as the reconciler
# loads it (whether a TLS driver pod can be signed at all).
_CA_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import json, os, sys
cap_memory()
from orchestrator.services.connector_driver_ca import DriverCertificateAuthority
try:
    authority = DriverCertificateAuthority.load(os.environ["CONNECTOR_DRIVER_CA_DIR"])
    authority.issue(["srw-gate-probe"])
    print(json.dumps({"ok": True, "not_after": authority.not_after.isoformat()}))
except Exception as exc:
    print(json.dumps({"ok": False, "error": type(exc).__name__ + ": " + str(exc)[:200]}))
"""
)

# A TCP connect from a pod (the orchestrator's view of the forge host: driver
# pods are pinned from its lookup).
_CONNECT_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import json, socket, sys
cap_memory()
request = json.loads(sys.stdin.readline())
try:
    socket.create_connection((request["host"], request["port"]), timeout=15).close()
    print(json.dumps({"reachable": True}))
except OSError as exc:
    print(json.dumps({"reachable": False, "error": type(exc).__name__}))
"""
)

# Raw HTTPS requests from a workspace to a git swap driver, trusting SRW's
# CA for it as the binding's wiring does (the CA file the agent installed).
# The lease travels in Basic auth, as git's credential helper sends it.
# Only statuses, a few headers and the first bytes of each answer return.
_WS_HTTP_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import base64, json, ssl, sys, urllib.error, urllib.request
cap_memory()
request = json.loads(sys.stdin.readline())
answers = []
for call in request["calls"]:
    context = ssl.create_default_context(cafile=call["ca"])
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=context)
    )
    headers = dict(call.get("headers") or {})
    if call.get("lease"):
        headers["Authorization"] = "Basic " + base64.b64encode(
            ("srw-lease:" + call["lease"]).encode()
        ).decode()
    body = call.get("body")
    message = urllib.request.Request(
        call["url"],
        data=None if body is None else body.encode("latin-1"),
        method=call["method"],
        headers=headers,
    )
    try:
        with opener.open(message, timeout=60) as response:
            status, text, got = response.status, response.read(4096), response.headers
    except urllib.error.HTTPError as error:
        status, text, got = error.code, error.read(4096), error.headers
    except Exception as exc:
        answers.append({"status": 0, "error": type(exc).__name__ + ": " + str(exc)[:200]})
        continue
    answers.append({
        "status": status,
        "content_type": got.get("Content-Type", ""),
        "www_authenticate": got.get("WWW-Authenticate", ""),
        "body": text.decode("utf-8", "replace")[:600],
    })
print(json.dumps(answers))
"""
)

# What one OAuth client the gate logs in with: created, deleted, counted.
_KEYCLOAK_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import json, os, sys, urllib.error, urllib.parse, urllib.request
cap_memory()
request = json.loads(sys.stdin.readline())
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
base = os.environ.get("KEYCLOAK_URL", "http://srw-keycloak:8080").rstrip("/")
realm = os.environ.get("KEYCLOAK_REALM", "srw")
admin = os.environ.get("KEYCLOAK_ADMIN_USERNAME") or os.environ.get("KEYCLOAK_ADMIN", "admin")
admin_password = os.environ.get("KEYCLOAK_ADMIN_PASSWORD", "")
form = urllib.parse.urlencode({
    "grant_type": "password", "client_id": "admin-cli",
    "username": admin, "password": admin_password,
}).encode()
with opener.open(
    base + "/realms/master/protocol/openid-connect/token", data=form, timeout=30
) as response:
    token = json.load(response)["access_token"]
admin_api = base + "/admin/realms/" + urllib.parse.quote(realm, safe="")
def call(method, url, body=None):
    message = urllib.request.Request(
        url,
        data=None if body is None else json.dumps(body).encode(),
        method=method,
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
    )
    try:
        with opener.open(message, timeout=30) as response:
            text = response.read().decode("utf-8", "replace")
            return response.status, text, response.headers.get("Location") or ""
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode("utf-8", "replace"), ""
def clients():
    status, text, _location = call(
        "GET", admin_api + "/clients?" + urllib.parse.urlencode({"clientId": request["client"]})
    )
    if status != 200:
        raise SystemExit("client lookup answered HTTP %d" % status)
    return [c for c in json.loads(text) if c.get("clientId") == request["client"]]
def owned(client):
    return (client.get("attributes") or {}).get("srw-gate") == request["marker"] and (
        not request.get("client_uuid") or client.get("id") == request["client_uuid"]
    )
action = request["action"]
if action == "create-client":
    if clients():
        print(json.dumps({"exists": True}))
        raise SystemExit(0)
    status, text, location = call("POST", admin_api + "/clients", {
        "clientId": request["client"],
        "name": request["client"],
        "enabled": True,
        "protocol": "openid-connect",
        "publicClient": True,
        "standardFlowEnabled": False,
        "directAccessGrantsEnabled": True,
        "serviceAccountsEnabled": False,
        "fullScopeAllowed": True,
        "defaultClientScopes": ["profile", "email", "roles"],
        "optionalClientScopes": [],
        "attributes": {"srw-gate": request["marker"]},
    })
    if status != 201:
        print(json.dumps({"error": "client create answered HTTP %d: %s" % (status, text[:200])}))
        raise SystemExit(0)
    print(json.dumps({
        "id": location.rstrip("/").rsplit("/", 1)[-1] if location else "",
        "found": [c["id"] for c in clients() if owned(c)],
    }))
elif action == "delete":
    deleted, refused = [], []
    for client in clients():
        if not owned(client):
            refused.append(client.get("id"))
            continue
        status, _text, _location = call("DELETE", admin_api + "/clients/" + client["id"])
        if status not in (204, 404):
            raise SystemExit("client delete answered HTTP %d" % status)
        deleted.append(client["id"])
    print(json.dumps({"deleted": deleted, "refused": refused, "clients": len(clients())}))
elif action == "count":
    print(json.dumps({"clients": len(clients())}))
else:
    print(json.dumps({"error": "unknown action"}))
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

# Every process's environment and command line, and every file under the
# roots, searched for the secrets on stdin: only paths come back.
_SCAN_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import json, os, sys
cap_memory()
request = json.loads(sys.stdin.readline())
needles = [value.encode() for value in request["secrets"]]
me = str(os.getpid())
found, scanned = [], {"processes": 0, "files": 0, "unreadable": 0}

def holds(data):
    return any(needle in data for needle in needles)

for pid in os.listdir("/proc"):
    if not pid.isdigit() or pid == me or pid == str(os.getppid()):
        continue
    for part in ("environ", "cmdline"):
        try:
            with open(f"/proc/{pid}/{part}", "rb") as handle:
                data = handle.read()
        except OSError:
            scanned["unreadable"] += 1
            continue
        scanned["processes"] += 1
        if holds(data):
            found.append(f"/proc/{pid}/{part}")
for root in request["roots"]:
    for directory, subdirectories, files in os.walk(root):
        subdirectories[:] = [
            d for d in subdirectories
            if not os.path.join(directory, d).startswith(("/proc", "/sys", "/dev"))
        ]
        for name in files:
            path = os.path.join(directory, name)
            try:
                if os.path.islink(path) or os.path.getsize(path) > 4 << 20:
                    continue
                with open(path, "rb") as handle:
                    data = handle.read()
            except OSError:
                scanned["unreadable"] += 1
                continue
            scanned["files"] += 1
            if holds(data):
                found.append(path)
print(json.dumps({"found": found[:20], "scanned": scanned}))
"""
)

# A busybox pod in the driver namespace, under its static default deny and
# nothing else: it must not reach the orchestrator's API port (the D5 gate's
# 12-probe harness). "open" is any TCP answer, "closed" a refusal or a
# timeout; the verdict is the last rounds.
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


def node_refused(node: str, refused: str) -> bool:
    """Whether the node address lies in one of the refused ranges."""
    try:
        address = ipaddress.ip_address(node)
    except ValueError:
        return False
    for cidr in refused.split(","):
        try:
            network = ipaddress.ip_network(cidr.strip(), strict=False)
        except ValueError:
            continue
        if network.version == address.version and address in network:
            return True
    return False


# ---------------------------------------------------------------------------
# Evaluators (pure; tests/test_k3d_git_swap_gate.py runs them)
# ---------------------------------------------------------------------------


def repository_path(url: str) -> str:
    """The path the driver serves a clean upstream URL under (no ``.git``)."""
    return url.split("://", 1)[1].split("/", 1)[1].removesuffix(".git")


def endpoint_name(connector_id: str, digest: str) -> str:
    """The connector's endpoint Service at one digest (connector_service_launch)."""
    hex_id = connector_id.replace("-", "")
    return f"srw-ep-{hex_id}-{digest.removeprefix('sha256:')[:12]}"


def driver_origin(namespace: str, connector_id: str, digest: str) -> str:
    """What a binding's workspace dials: the endpoint Service, over TLS."""
    return (
        f"https://{endpoint_name(connector_id, digest)}.{namespace}"
        f".svc.cluster.local:{DRIVER_PORT}"
    )


def driver_url(namespace: str, connector_id: str, digest: str, upstream: str) -> str:
    """The driver URL ``insteadOf`` rewrites the clean upstream URL to."""
    return (
        f"{driver_origin(namespace, connector_id, digest)}/{connector_id}/"
        f"{repository_path(upstream)}"
    )


def canary_passed(pod: dict) -> bool:
    """Whether the canary-wait init container ran first and exited 0 (the
    shim install follows it for a shim-run driver)."""
    spec = pod.get("spec") or {}
    names = [c.get("name") for c in spec.get("initContainers") or []]
    statuses = {
        status.get("name"): status
        for status in (pod.get("status") or {}).get("initContainerStatuses") or []
    }
    terminated = ((statuses.get("canary-wait") or {}).get("state") or {}).get(
        "terminated"
    ) or {}
    return names == ["canary-wait", "install-shim"] and terminated.get("exitCode") == 0


def pod_ready(pod: dict) -> bool:
    status = pod.get("status") or {}
    statuses = status.get("containerStatuses") or []
    return (
        status.get("phase") == "Running"
        and [s.get("name") for s in statuses] == ["driver"]
        and all(s.get("ready") for s in statuses)
    )


def swap_pod_problems(
    pod: dict, secret_doc: dict, swap_image: str, tokens: list[str]
) -> list[str]:
    """How a swap pod and its Secret differ from what C3 requires."""
    problems: list[str] = []
    spec = pod.get("spec") or {}
    containers = spec.get("containers") or []
    if [c.get("name") for c in containers] != ["driver"]:
        return [f"containers {[c.get('name') for c in containers]}"]
    driver = containers[0]
    digest = swap_image.rsplit("@", 1)[-1] if "@" in swap_image else ""
    if not digest or not str(driver.get("image", "")).endswith("@" + digest):
        problems.append(
            f"the driver runs {driver.get('image')}, not the pinned swap image"
        )
    security = driver.get("securityContext") or {}
    if (security.get("capabilities") or {}).get("drop") != ["ALL"]:
        problems.append("capabilities not dropped")
    if security.get("allowPrivilegeEscalation") is not False:
        problems.append("privilege escalation allowed")
    if spec.get("automountServiceAccountToken") is not False:
        problems.append("a ServiceAccount token may be mounted")
    mounts = {m.get("mountPath"): m for m in driver.get("volumeMounts") or []}
    for path in ("/run/srw/tls.crt", "/run/srw/tls.key"):
        if path not in mounts:
            problems.append(f"{path} is not mounted")
    data = secret_doc.get("data") or {}
    if not {"request.json", "identity", "tls.crt", "tls.key"} <= set(data):
        problems.append(f"the Secret holds {sorted(data)}")
        return problems
    decoded = {
        key: base64.b64decode(value).decode("utf-8", "replace")
        for key, value in data.items()
    }
    request = json.loads(decoded["request.json"])
    if request.get("credentials") != {}:
        problems.append("the request file carries credentials")
    config = (request.get("connector") or {}).get("config") or {}
    if set(config) != {"upstream", "host"} or not _UPSTREAM_RE.fullmatch(
        str(config.get("upstream"))
    ):
        problems.append(f"the pod's config is {sorted(config)}")
    if request.get("tls") != {
        "cert_file": "/run/srw/tls.crt",
        "key_file": "/run/srw/tls.key",
    }:
        problems.append("the request file names no TLS certificate")
    if (
        "BEGIN CERTIFICATE" not in decoded["tls.crt"]
        or "PRIVATE KEY" not in decoded["tls.key"]
    ):
        problems.append("the Secret's TLS material is not a certificate and key")
    blob = json.dumps(decoded)
    if any(token and token in blob for token in tokens):
        problems.append("the Secret holds an upstream token")
    return problems


def clean_remote(url: str) -> str:
    """The one form a swap checkout's origin keeps: ``<repository>.git``
    (shared.connectors.git_swap.SwapUpstream.remote)."""
    return url.rstrip("/").removesuffix(".git") + ".git"


def config_problems(config: str, *, clean_url: str, tokens: list[str]) -> list[str]:
    """How a swap checkout's .git/config differs from what C3 requires."""
    problems: list[str] = []
    lines = [line.strip() for line in config.splitlines()]
    urls = [line.split("=", 1)[1].strip() for line in lines if line.startswith("url =")]
    if urls != [clean_url]:
        problems.append(f"remote URLs {urls}")
    if any(token and token in config for token in tokens):
        problems.append("a token or lease is in .git/config")
    if "oauth2:" in config or re.search(r"https://[^/\s]*@", config):
        problems.append("credentials are in a URL")
    if (
        "credentialsInUrl = die" not in config
        and "credentialsinurl = die" not in config.lower()
    ):
        problems.append("transfer.credentialsInUrl is not die")
    return problems


def push_refused(output: str, reason: str) -> bool:
    """Whether git reports the driver's ``ng`` for the ref with ``reason``."""
    return "[remote rejected]" in output and reason in output


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


class Api:
    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self.password = secret(password)
        self.client_id: str | None = None

    def call(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        if not self.client_id:
            raise GateError("no OAuth client to log in with")
        result = in_pod(
            ORCHESTRATOR,
            ORCHESTRATOR_CONTAINER,
            _API_PROGRAM,
            {
                "username": self.username,
                "password": self.password,
                "client_id": self.client_id,
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


PLAN = [
    "preflight: orchestrator and stateless agent pods serve this checkout's C3 "
    "modules; hosting on with an exchange and canary port, a digest-pinned shim "
    "and swap image, SRW's driver CA loadable and the swap driver installed; "
    "migrations; room for three pods; the driver namespace's baseline and "
    "default deny, enforced (12-probe harness); refusedCidrs covers the node; "
    "the forge host resolves and answers from the orchestrator",
    "accounts: a disposable OAuth client the owner logs in with",
    "startup: each swap pod's canary wait ran first; the pinned swap image, "
    "capabilities dropped, no ServiceAccount token; its Secret holds its TLS "
    "certificate, the clean upstream and no token; each binding's workspace has "
    "its own ingress policy; first binding to a serving pod within the budget "
    "(measured, S1)",
    "push: rw cloned through the driver; remote is the clean URL, git remote -v "
    "and .git/config hold no token, credentialsInUrl=die, ~/.gitconfig includes "
    "SRW's wiring once; a branch push lands upstream through the driver; no "
    "forge token anywhere in the workspace; the README names the driver",
    "readonly: pushing ro is refused (read-only); raw GET info/refs?service="
    "git-receive-pack and POST git-receive-pack (also ?service=git-upload-pack) "
    "get 403; nothing reached the upstream; no write exchange for the lease",
    "refs: a tag push and a branch delete on rw are refused with their reasons; "
    "the upstream keeps the branch and has no tag",
    "leases: rw and ro in session one (one upstream) each use their own lease "
    "inside their own checkout (helper answers, driver logs)",
    "lfs: a Git LFS batch request gets 501 and the message",
    "ide: git in an IDE terminal's bare environment and an ssh-gateway login "
    "shell, run in the checkout, goes through the driver too",
    "redirect: the forge's redirect is not followed with the credential (502 "
    "and the driver's message); the README says it was not cloned and why",
    "fallback: a private-host token repository (--private-url) gets no lease; "
    "the README and Test say its token goes in the clone URL and why",
    "reused: a pre-C3 checkout loses its oauth2:<token>@ origin on the next "
    "attach and refuses credentials in URLs again",
    "revoked: detaching rw from session one revokes its lease; that token gets "
    "401 within the revocation lag while session two's still gets 200; session "
    "one's wiring for rw is removed",
    "cleanup: sessions, connectors, project, OAuth client and probe pods are "
    "gone; no driver-namespace object names this run's connectors; no "
    "srw-gate-<gate id> ref is left upstream",
]


class GitSwapGate:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.gate_id = args.gate_id or f"c3-{secrets.token_hex(5)}"
        self.report = Report(self.gate_id)
        self.owner = Api(args.user, args.password)
        self.oauth_client = f"{self.gate_id}-oauth"
        self.client_started = False
        self.client_uuid: str | None = None
        self.connectors: dict[str, str] = {}  # label -> datasource id
        self.threads: dict[str, str] = {}  # label -> thread id
        self.project: str | None = None
        self.deny_probe = f"{self.gate_id}-denyprobe"
        self.deny_probe_started = False
        self.owner_id = ""
        self.namespace = ""
        self.swap_image = ""
        self.orchestrator_ip = ""
        self.reconcile_seconds = 15
        self.lease_tokens: dict[tuple[str, str], str] = {}
        self.pushed = False
        self.branch = f"srw-gate-{self.gate_id}"
        self.upstream = args.upstream_url
        self.redirect = args.redirect_url
        self.token_file = args.upstream_token_file
        self.token = (
            secret(Path(self.token_file).expanduser().read_text().strip())
            if args.run
            else ""
        )
        self.fake = secret(f"srw-gate-fake-{secrets.token_hex(16)}")
        self.private = args.private_url
        self.urls = {
            "rw": self.upstream,
            "ro": self.upstream,
            "redirect": self.redirect,
            "private": self.private,
        }
        #: Seconds from a connector's first binding to its pod serving (S1).
        self.cold_start: dict[str, float] = {}

    # -- naming and helpers ------------------------------------------------
    def name(self, label: str) -> str:
        return f"{self.gate_id} {label}"

    def title(self, label: str) -> str:
        return f"C3 git swap gate {self.gate_id} {label}"

    @property
    def kc(self) -> list[str]:
        """kubectl in the driver namespace."""
        return ["kubectl", f"--context={LOCAL_CONTEXT}", "-n", self.namespace]

    def clone_dir(self, session: str, label: str) -> str:
        """The checkout of ``label`` in a session's workspace, found by the
        driver URL its remote resolves to (rw and ro share an upstream, so
        one of their clones is suffixed)."""
        marker = f"/{self.connectors[label]}/"
        script = (
            "for d in ~/workspace/repos/*/; do\n"
            '  u=$(git -C "$d" remote get-url origin 2>/dev/null) || continue\n'
            '  printf "%s %s\\n" "$d" "$u"\n'
            "done\n"
        )
        _rc, out = self.ws(session, script)
        for line in out.splitlines():
            path, _, url = line.partition(" ")
            if marker in url:
                return path.rstrip("/")
        return ""

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

    def workspace_pod(self, thread: str) -> str:
        def probe() -> str | None:
            running = [
                pod["metadata"]["name"]
                for pod in self.release_pods(f"srw/thread-id={thread}")
                if pod.get("status", {}).get("phase") == "Running"
            ]
            return running[0] if len(running) == 1 else None

        return wait_for(f"workspace of {thread}", probe, timeout=300)

    def ws(self, session: str, script: str, *, timeout: int = 180) -> tuple[int, str]:
        """Run ``script`` as agent-host in a session's workspace (stdin, never
        argv). Output is scrubbed."""
        pod = self.workspace_pod(self.threads[session])
        rc, out, err = run(
            K
            + ["exec", "-i", pod, "-c", WORKSPACE_CONTAINER, "--"]
            + ["su", "-s", "/bin/bash", "agent-host", "-c", "bash -s"],
            data="set -u\ncd ~\nexport GIT_TERMINAL_PROMPT=0\n" + script,
            timeout=timeout,
        )
        return rc, (out + "\n" + err).strip()

    def ws_http(self, session: str, calls: list[dict]) -> list[dict]:
        pod = self.workspace_pod(self.threads[session])
        out = command(
            K
            + ["exec", "-i", pod, "-c", WORKSPACE_CONTAINER, "--"]
            + ["su", "-s", "/bin/bash", "agent-host", "-c"]
            + ["/usr/bin/python3 -I -c " + shlex_quote(_WS_HTTP_PROGRAM)],
            data=json.dumps({"calls": calls}) + "\n",
        )
        return json.loads(out.splitlines()[-1])

    def identity_rows(self, label: str) -> list[dict]:
        out = sql(
            "SELECT coalesce(json_agg(row_to_json(i) ORDER BY i.created_at), '[]') "
            "FROM (SELECT id, pod_name, image_digest, ready_at IS NOT NULL AS ready, "
            "revoked_at IS NOT NULL AS revoked, revoke_reason, created_at FROM "
            "connector_driver_identities WHERE "
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

    def lease_row(self, label: str, session: str, *, live: bool = True) -> dict | None:
        condition = "AND revoked_at IS NULL AND expires_at > now()" if live else ""
        out = sql(
            "SELECT coalesce(row_to_json(l)::text, '') FROM (SELECT id, driver, "
            "image_digest, access, exchange_count, revoke_reason FROM "
            "connector_credential_leases WHERE "
            f"connector_id = {lit(self.connectors[label])} AND thread_id = "
            f"{lit(self.threads[session])} {condition} ORDER BY issued_at DESC "
            "LIMIT 1) l"
        )
        return json.loads(out) if out else None

    def lease_token(self, label: str, session: str) -> str:
        """The lease token session ``session`` holds for ``label``, as its
        binding delivered it; scrubbed from every printed line."""
        key = (label, session)
        if key not in self.lease_tokens:
            lease = self.lease_row(label, session)
            if not lease:
                raise GateError(f"no live lease of {label} for session {session}")
            found = in_pod(
                ORCHESTRATOR,
                ORCHESTRATOR_CONTAINER,
                _LEASE_PROGRAM,
                {"lease_id": lease["id"]},
            )
            if not isinstance(found.get("token"), str):
                raise GateError(f"the lease of {label} has no readable token")
            self.lease_tokens[key] = secret(found["token"])
        return self.lease_tokens[key]

    def driver(self, label: str, session: str) -> tuple[str, str]:
        """The driver origin and repository URL a session's binding of
        ``label`` carries, and the CA file its wiring installed."""
        lease = self.lease_row(label, session)
        if not lease or not lease.get("image_digest"):
            raise GateError(f"no live binding of {label} in session {session}")
        connector = self.connectors[label]
        return (
            driver_origin(self.namespace, connector, lease["image_digest"]),
            driver_url(
                self.namespace, connector, lease["image_digest"], self.urls[label]
            ),
        )

    def ca_file(self, label: str) -> str:
        return (
            f"{WORKSPACE_HOME}/.srw-credentials/git/bindings/"
            f"{self.connectors[label]}.ca.pem"
        )

    def driver_log(self, label: str) -> str:
        text = ""
        for row in self.live_pods(label):
            _rc, out, _err = run(
                self.kc + ["logs", row["pod_name"], "-c", "driver"], timeout=60
            )
            text += out + "\n"
        return text

    def titled_threads(self) -> list[str]:
        out = sql(
            "SELECT id FROM threads WHERE "
            f"position({lit(self.gate_id)} in coalesce(title, '')) > 0"
        )
        return [row for row in out.splitlines() if _UUID_RE.fullmatch(row)]

    def upstream_git(self, *args: str, url: str | None = None) -> tuple[int, str]:
        """git on this workstation against the forge with the operator's
        token, which reaches git only through an askpass program that reads
        the token file (never argv, never an environment value)."""
        git = shutil.which("git")
        if git is None:
            raise GateError("git is not installed on this workstation")
        with tempfile.TemporaryDirectory(prefix="srw-gate-") as scratch:
            askpass = Path(scratch) / "askpass"
            askpass.write_text(
                "#!/bin/sh\nexec cat "
                + shlex_quote(str(Path(self.token_file).expanduser()))
                + "\n"
            )
            askpass.chmod(0o700)
            target = (url or self.upstream).replace("https://", "https://oauth2@", 1)
            env = {
                "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
                "HOME": scratch,
                "GIT_ASKPASS": str(askpass),
                "GIT_TERMINAL_PROMPT": "0",
                "GIT_CONFIG_NOSYSTEM": "1",
            }
            rc, out, err = run(
                [git, "-c", "credential.helper=", *args[:1], target, *args[1:]],
                env=env,
                timeout=120,
            )
        return rc, (out + "\n" + err).strip()

    def upstream_refs(self) -> dict[str, str]:
        rc, out = self.upstream_git("ls-remote")
        if rc:
            raise GateError(f"the forge did not list its refs: {out[-300:]}")
        refs = {}
        for line in out.splitlines():
            parts = line.split()
            if len(parts) == 2 and re.fullmatch(r"[0-9a-f]{40,64}", parts[0]):
                refs[parts[1]] = parts[0]
        return refs

    # -- phases --------------------------------------------------------------
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
            "preflight: orchestrator and stateless agent pods serve this "
            "checkout's C3 modules",
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
                "CONNECTOR_LEASE_CANARY_PORT",
                "CONNECTOR_DRIVER_SHIM_IMAGE",
                "CONNECTOR_GIT_SWAP_IMAGE",
                "CONNECTOR_GIT_SWAP_FALLBACK",
                "CONNECTOR_DRIVER_CA_DIR",
                "CONNECTOR_SERVICE_MAX_INSTALLATION",
                "CONNECTOR_SERVICE_RECONCILE_SECONDS",
                "CONNECTOR_SERVICE_REFUSED_CIDRS",
                "CONNECTOR_SERVICE_NODE_IP",
            )
        }
        configured = (
            env["CONNECTOR_SERVICE_PODS_ENABLED"].lower() == "true"
            and env["CONNECTOR_SERVICE_NAMESPACE"] != ""
            and env["CONNECTOR_LEASE_EXCHANGE_PORT"].isdigit()
            and env["CONNECTOR_LEASE_CANARY_PORT"].isdigit()
            and "@sha256:" in env["CONNECTOR_DRIVER_SHIM_IMAGE"]
            and "@sha256:" in env["CONNECTOR_GIT_SWAP_IMAGE"]
            and env["CONNECTOR_DRIVER_CA_DIR"] != ""
        )
        self.report.check(
            "preflight: hosting on with an exchange and canary port, a "
            "digest-pinned shim and swap image, SRW's driver CA mounted",
            configured,
            json.dumps(env),
        )
        if not configured:
            raise GateError(
                "set connectors.servicePods.enabled and connectors.drivers.gitSwap "
                "as the k3d profile does, under Tilt"
            )
        self.namespace = env["CONNECTOR_SERVICE_NAMESPACE"]
        self.swap_image = env["CONNECTOR_GIT_SWAP_IMAGE"]
        self.reconcile_seconds = int(
            float(env["CONNECTOR_SERVICE_RECONCILE_SECONDS"] or 15)
        )
        ca = in_pod(ORCHESTRATOR, ORCHESTRATOR_CONTAINER, _CA_PROGRAM, {})
        self.report.check(
            "preflight: SRW's connector driver CA loads in the orchestrator and "
            "signs a certificate",
            ca.get("ok") is True,
            json.dumps(ca),
        )
        if not ca.get("ok"):
            raise GateError("the driver CA is unusable")
        node = env["CONNECTOR_SERVICE_NODE_IP"]
        covered = node_refused(node, env["CONNECTOR_SERVICE_REFUSED_CIDRS"])
        self.report.check(
            "preflight: driver pods are refused the orchestrator's node",
            covered,
            f"node={node} refused={env['CONNECTOR_SERVICE_REFUSED_CIDRS']}",
        )
        if not covered:
            raise GateError("set connectors.servicePods.refusedCidrs to cover the node")
        names = ", ".join(lit(name) for name in MIGRATIONS)
        applied = sql(
            "SELECT count(*) FROM schema_migrations WHERE success AND filename "
            f"IN ({names})"
        )
        self.report.check(
            "preflight: the lease and service-pod migrations are applied",
            applied == str(len(MIGRATIONS)),
            f"{applied} of {len(MIGRATIONS)}",
        )
        if applied != str(len(MIGRATIONS)):
            raise GateError("the lease and service-pod migrations are not applied")
        live = sql(
            "SELECT count(*) FROM connector_driver_identities WHERE "
            "credential_generation IS NOT NULL AND removed_at IS NULL"
        )
        cap = int(env["CONNECTOR_SERVICE_MAX_INSTALLATION"] or 0)
        room = live.isdigit() and int(live) + 3 <= cap
        self.report.check(
            "preflight: the installation has room for this run's three pods",
            room,
            f"{live} live of {cap}",
        )
        if not room:
            raise GateError("too many live service pods: stop other gates first")
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
        for url in dict.fromkeys(self.urls.values()):
            host = url.split("://", 1)[1].split("/", 1)[0]
            upstream = in_pod(
                ORCHESTRATOR,
                ORCHESTRATOR_CONTAINER,
                _CONNECT_PROGRAM,
                {"host": host, "port": 443},
            )
            self.report.check(
                f"preflight: {host} resolves and answers from the orchestrator "
                "(driver pods are pinned from its lookup)",
                upstream.get("reachable") is True,
                "reachable"
                if upstream.get("reachable")
                else f"{upstream.get('error')}: on k3d a dead DNS upstream after a "
                "host network change; restart the node: docker restart "
                "k3d-srw-server-0",
            )
            if not upstream.get("reachable"):
                raise GateError(f"{host} is not reachable")
        rc, out = self.upstream_git("ls-remote")
        self.report.check(
            "preflight: the operator's token lists the disposable repository",
            rc == 0,
            out[-200:] if rc else "listed",
        )
        if rc:
            raise GateError("the upstream token cannot read the repository")
        enforced = self.default_deny_enforced()
        self.report.check(
            "preflight: the cluster enforces NetworkPolicy (12 probes under the "
            "driver namespace's default deny never reach the orchestrator's API "
            "port)",
            enforced,
            "enforced"
            if enforced
            else "reached: restart the node (docker restart k3d-srw-server-0)",
        )
        if not enforced:
            raise GateError("NetworkPolicy is not enforced on this cluster")

    def keycloak(self, action: str) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "action": action,
            "client": self.oauth_client,
            "marker": self.gate_id,
        }
        if self.client_uuid:
            payload["client_uuid"] = self.client_uuid
        result = in_pod(
            ORCHESTRATOR, ORCHESTRATOR_CONTAINER, _KEYCLOAK_PROGRAM, payload
        )
        if result.get("error"):
            raise GateError(f"Keycloak {action}: {result['error']}")
        return result

    def accounts(self) -> None:
        self.client_started = True
        created = self.keycloak("create-client")
        if created.get("exists"):
            self.client_started = False  # not this run's: never adopted
            raise GateError(f"a Keycloak client {self.oauth_client} already exists")
        found = created.get("found") or []
        made = created.get("id") or (found[0] if len(found) == 1 else "")
        if not _UUID_RE.fullmatch(made or "") or found != [made]:
            raise GateError(f"no Keycloak receipt for the OAuth client: {created}")
        self.client_uuid = made
        self.owner.client_id = self.oauth_client
        owner = self.owner.ok("GET", "/api/auth/me")["user"]
        self.owner_id = str(owner["id"])
        drivers = self.owner.ok("GET", "/api/datasources/drivers")
        swap = next(
            (d for d in drivers.get("drivers") or [] if d.get("name") == SWAP_DRIVER),
            None,
        )
        self.report.check(
            "accounts: the owner logs in with the disposable OAuth client; the "
            "capability matrix lists the swap driver as SRW's TLS service driver",
            swap is not None
            and swap["service"]["tls"] is True
            and swap["trust"]["trusted"] is True
            and swap["holds_upstream_credentials"] is True,
            json.dumps((swap or {}).get("trust")),
        )

    def create_connector(self, label: str) -> str:
        """POST a token repository connector; its id is recorded before
        anything checks it."""
        status, parsed = self.owner.call(
            "POST",
            "/api/datasources",
            {
                "name": self.name(label),
                "scope_mode": "all",
                "type": REPOSITORY_TYPE,
                "connection_url": self.urls[label],
                # The private-host one never reaches a forge: a fake token.
                "credentials": {
                    "auth_method": "token",
                    "token": self.fake if label == "private" else self.token,
                },
                "config": {"forge": self.args.forge},
            },
        )
        if isinstance(parsed, dict) and parsed.get("id"):
            self.connectors[label] = str(parsed["id"])
        if status not in (200, 201) or label not in self.connectors:
            raise GateError(f"{label} create answered HTTP {status}: {parsed}")
        return self.connectors[label]

    def fixture(self) -> None:
        created = self.owner.ok(
            "POST",
            "/api/projects",
            {
                "name": self.name("project"),
                "description": "C3 git swap gate (disposable)",
                "user_id": self.owner_id,
            },
        )
        self.project = str(created["id"])
        for label in ("rw", "ro", "redirect", "private"):
            self.create_connector(label)
            self.owner.ok(
                "POST",
                f"/api/projects/{self.project}/datasources/{self.connectors[label]}",
                {"read_only": label == "ro"},
            )
        print(f"fixture: project {self.project}, connectors {self.connectors}")

    def create_session(
        self, label: str, connectors: list[str], *, fallback: tuple[str, ...] = ()
    ) -> str:
        created = self.owner.ok(
            "POST",
            "/api/persistent/threads",
            {
                "title": self.title(label),
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
        lane = sql(f"SELECT execution_lane FROM threads WHERE id = {lit(thread)}")
        if lane != "stateless":
            raise GateError(f"session lane is {lane!r}, not stateless")
        self.say(label, "Reply with the single word ready.")
        for connector in connectors:
            if connector in fallback:
                continue  # never served: no lease to wait for
            lease = wait_for(
                f"a swap lease for {connector} on {label}",
                lambda connector=connector: (
                    row
                    if (row := self.lease_row(connector, label))
                    and row.get("driver") == SWAP_DRIVER
                    and row.get("image_digest")
                    else None
                ),
                timeout=self.args.turn_timeout,
                interval=5,
            )
            expected = "ReadOnly" if connector == "ro" else "ReadWrite"
            if lease["access"] != expected:
                raise GateError(f"{connector} binds {lease['access']}, not {expected}")
        return thread

    def say(self, session: str, text: str) -> None:
        self.owner.ok(
            "POST",
            f"/api/persistent/threads/{self.threads[session]}/input",
            {"content": text},
        )

    def wait_ready_pod(self, label: str) -> dict:
        def probe() -> dict | None:
            live = self.live_pods(label)
            if len(live) != 1 or not live[0]["ready"]:
                return None
            pod = self.driver_pod(live[0])
            return live[0] if pod and pod_ready(pod) else None

        return wait_for(
            f"connector {label}'s swap pod ready",
            probe,
            timeout=self.args.start_timeout,
            interval=5,
        )

    def wait_clone(self, session: str, label: str) -> str:
        """The checkout of ``label`` in a session's workspace, once cloned."""
        return wait_for(
            f"session {session}'s clone of {label}",
            lambda: self.clone_dir(session, label),
            timeout=self.args.turn_timeout,
            interval=10,
        )

    def startup_checks(self) -> None:
        self.create_session("one", ["rw", "ro"])
        self.create_session("two", ["rw", "redirect", "private"], fallback=("private",))
        served = [label for label in self.connectors if label != "private"]
        rows = {label: self.wait_ready_pod(label) for label in served}
        for label in served:
            self.cold_start[label] = float(
                sql(
                    "SELECT round(EXTRACT(EPOCH FROM (min(i.ready_at) - "
                    "min(l.issued_at)))::numeric, 1) FROM connector_driver_identities i "
                    "JOIN connector_credential_leases l ON l.connector_id = "
                    f"i.connector_id WHERE i.connector_id = {lit(self.connectors[label])}"
                )
                or "nan"
            )
        print(f"cold start (first binding to a serving pod): {self.cold_start}")
        self.report.check(
            "startup: each connector's first binding has a serving pod within "
            f"{COLD_START_BUDGET}s (a new binding wakes the reconciler; S1)",
            all(value <= COLD_START_BUDGET for value in self.cold_start.values()),
            json.dumps(self.cold_start),
        )
        for label, row in rows.items():
            pod = self.driver_pod(row) or {}
            _rc, canary_log, _err = run(
                self.kc + ["logs", row["pod_name"], "-c", "canary-wait"], timeout=60
            )
            self.report.check(
                f"startup: {label}'s canary wait ran first and exited 0 after the "
                "default deny was enforced",
                canary_passed(pod) and "default deny enforced" in canary_log,
                canary_log.splitlines()[-1][:200] if canary_log else "no log",
            )
            secret_doc = json.loads(
                command(self.kc + ["get", "secret", row["pod_name"], "-o", "json"])
            )
            problems = swap_pod_problems(
                pod,
                secret_doc,
                self.swap_image,
                [self.token, *self.lease_tokens.values()],
            )
            self.report.check(
                f"startup: {label}'s pod runs the pinned swap image unprivileged; "
                "its Secret holds its TLS certificate and the clean upstream, no "
                "token",
                not problems,
                "; ".join(problems),
            )
        policies = json.loads(
            command(
                self.kc
                + ["get", "networkpolicy", "-l"]
                + [f"srw.io/connector-id={self.connectors['rw']}", "-o", "json"]
            )
        )["items"]
        owners = {
            (p["metadata"].get("labels") or {}).get("srw.io/binding-owner")
            for p in policies
        }
        self.report.check(
            "startup: rw's pod admits each bound workspace by its own ingress "
            "policy (sessions one and two)",
            {self.threads["one"], self.threads["two"]} <= owners,
            f"{len(policies)} policies",
        )

    def push_checks(self) -> None:
        repo = shlex_quote(self.wait_clone("one", "rw"))
        _rc, config = self.ws("one", f"cat {repo}/.git/config\n")
        tokens = [self.token, self.lease_token("rw", "one")]
        problems = config_problems(
            config, clean_url=clean_remote(self.upstream), tokens=tokens
        )
        _rc, shown = self.ws("one", f"git -C {repo} remote -v\n")
        origin, url = self.driver("rw", "one")
        self.report.check(
            "push: rw's remote is the clean upstream URL; git remote -v shows the "
            "driver's URL; neither .git/config nor git remote -v holds a token; "
            "credentials in URLs are refused",
            not problems
            and url in shown
            and "@" not in shown
            and not any(token in shown for token in tokens),
            "; ".join(problems) or shown[:200],
        )
        _rc, gitconfig = self.ws("one", "git config --global --get-all include.path\n")
        self.report.check(
            "push: ~/.gitconfig includes SRW's wiring exactly once",
            gitconfig.splitlines().count("~/.srw-credentials/git/config") == 1,
            gitconfig[:200],
        )
        script = (
            f"cd {repo}\n"
            f"git checkout -q -b {self.branch}\n"
            f"printf '%s\\n' {shlex_quote(self.gate_id)} > srw-gate-{self.gate_id}.txt\n"
            f"git add srw-gate-{self.gate_id}.txt\n"
            f"git -c user.name=srw-gate -c user.email=gate@example.invalid "
            f"commit -q -m 'srw gate {self.gate_id}'\n"
            f"git push origin {self.branch} 2>&1\n"
            "echo pushed=$?\n"
            "git rev-parse HEAD\n"
        )
        self.pushed = True  # a branch may exist upstream from here on
        _rc, out = self.ws("one", script, timeout=300)
        head = out.strip().splitlines()[-1] if out.strip() else ""
        refs = self.upstream_refs()
        log = self.driver_log("rw")
        lease = self.lease_row("rw", "one") or {}
        self.report.check(
            "push: a branch push from session one lands upstream through the "
            "driver (its log records the ref under rw's lease)",
            "pushed=0" in out
            and refs.get(f"refs/heads/{self.branch}") == head
            and f'push lease={lease.get("id")} ref="refs/heads/{self.branch}"' in log
            and "allowed" in log,
            f"upstream={refs.get(f'refs/heads/{self.branch}')} head={head[:12]}",
        )
        scan = in_pod(
            self.workspace_pod(self.threads["one"]),
            WORKSPACE_CONTAINER,
            _SCAN_PROGRAM,
            {
                "secrets": [self.token],
                "roots": ["/home", "/tmp", "/root", "/var/tmp", "/run"],
            },
            python="python3",
            timeout=300,
        )
        self.report.check(
            "push: session one's workspace holds the forge token nowhere (files, "
            "environments, command lines)",
            not scan["found"] and scan["scanned"]["processes"] > 0,
            json.dumps(scan)[:400],
        )
        _rc, readme = self.ws("one", "cat ~/workspace/README.md 2>/dev/null\n")
        self.report.check(
            "push: session one's README says rw goes through the git swap driver",
            "git swap driver with a lease" in readme,
        )

    def readonly_checks(self) -> None:
        repo = shlex_quote(self.wait_clone("one", "ro"))
        branch = f"{self.branch}-ro"
        script = (
            f"cd {repo}\n"
            f"git checkout -q -b {branch}\n"
            f"git -c user.name=srw-gate -c user.email=gate@example.invalid "
            f"commit -q --allow-empty -m 'srw gate {self.gate_id} ro'\n"
            f"git push origin {branch} 2>&1\n"
            "echo pushed=$?\n"
        )
        _rc, out = self.ws("one", script, timeout=300)
        self.report.check(
            "readonly: a push of the ReadOnly connector is refused, and git shows "
            "the driver's reason",
            "pushed=0" not in out and "read-only" in out,
            out[-300:],
        )
        _origin, url = self.driver("ro", "one")
        lease = self.lease_token("ro", "one")
        ca = self.ca_file("ro")
        receive = {"Content-Type": "application/x-git-receive-pack-request"}
        calls = [
            {"method": "GET", "url": f"{url}.git/info/refs?service=git-receive-pack"},
            {
                "method": "POST",
                "url": f"{url}.git/git-receive-pack",
                "body": "0000",
                "headers": receive,
            },
            {
                "method": "POST",
                "url": f"{url}.git/git-receive-pack?service=git-upload-pack",
                "body": "0000",
                "headers": receive,
            },
            {
                "method": "POST",
                "url": f"{url}/git-receive-pack?service=git-upload-pack",
                "body": "0000",
                "headers": receive,
            },
            # A read still works.
            {"method": "GET", "url": f"{url}.git/info/refs?service=git-upload-pack"},
        ]
        answers = self.ws_http(
            "one", [{**call, "lease": lease, "ca": ca} for call in calls]
        )
        statuses = [answer.get("status") for answer in answers]
        self.report.check(
            "readonly: with the ReadOnly lease, GET info/refs?service="
            "git-receive-pack and POST git-receive-pack (also with service="
            "git-upload-pack in the query) get 403 by path; a fetch gets 200",
            statuses == [403, 403, 403, 403, 200]
            and all("read-only" in answers[i].get("body", "") for i in range(4)),
            json.dumps(statuses),
        )
        refs = self.upstream_refs()
        lease_row = self.lease_row("ro", "one") or {}
        writes = sql(
            "SELECT count(*) FROM security_events WHERE resource_id = "
            f"{lit(lease_row.get('id', ''))} AND detail LIKE '%operation=write%'"
        )
        self.report.check(
            "readonly: nothing reached the upstream and the exchange saw no write "
            "for the ReadOnly lease",
            f"refs/heads/{branch}" not in refs and writes == "0",
            f"{writes} write events",
        )

    def refs_checks(self) -> None:
        repo = shlex_quote(self.wait_clone("one", "rw"))
        tag = f"{self.branch}-tag"
        _rc, out = self.ws(
            "one",
            f"cd {repo}\ngit tag {tag}\ngit push origin {tag} 2>&1\necho pushed=$?\n",
            timeout=300,
        )
        self.report.check(
            "refs: a tag push is refused, and git shows why",
            "pushed=0" not in out
            and push_refused(out, "only branches (refs/heads/*) may be pushed"),
            out[-300:],
        )
        _rc, out = self.ws(
            "one",
            f"cd {repo}\ngit push origin --delete {self.branch} 2>&1\necho pushed=$?\n",
            timeout=300,
        )
        self.report.check(
            "refs: a branch delete is refused, and git shows why",
            "pushed=0" not in out
            and push_refused(out, "deleting a ref is not allowed"),
            out[-300:],
        )
        refs = self.upstream_refs()
        self.report.check(
            "refs: the upstream keeps the branch and has no tag",
            f"refs/heads/{self.branch}" in refs and f"refs/tags/{tag}" not in refs,
        )

    def lease_checks(self) -> None:
        digests = {}
        for label in ("rw", "ro"):
            origin, url = self.driver(label, "one")
            host = origin.removeprefix("https://")
            path = url.split(host, 1)[1].lstrip("/") + ".git"
            # Each binding's rules apply in its own checkout (rw and ro name
            # one upstream): the helper answers there.
            repo = shlex_quote(self.wait_clone("one", label))
            _rc, out = self.ws(
                "one",
                f"cd {repo}\n"
                "printf 'protocol=https\\nhost=%s\\npath=%s\\n\\n' "
                f"{shlex_quote(host)} {shlex_quote(path)} | git credential fill "
                "| sed -n 's/^password=//p' | tr -d '\\n' | sha256sum\n",
            )
            digests[label] = out.split()[0] if out.split() else ""
        expected = {
            label: hashlib.sha256(self.lease_token(label, "one").encode()).hexdigest()
            for label in ("rw", "ro")
        }
        rows = {label: self.lease_row(label, "one") or {} for label in ("rw", "ro")}
        logs = {label: self.driver_log(label) for label in ("rw", "ro")}
        self.report.check(
            "leases: session one's two connectors each use their own lease (the "
            "credential helper answers each driver URL with its own lease; each "
            "driver pod logs its own lease id)",
            digests == expected
            and rows["rw"].get("id") != rows["ro"].get("id")
            and f"lease={rows['rw'].get('id')}" in logs["rw"]
            and f"lease={rows['ro'].get('id')}" in logs["ro"]
            and f"lease={rows['ro'].get('id')}" not in logs["rw"],
            json.dumps({k: v[:12] for k, v in digests.items()}),
        )

    def lfs_checks(self) -> None:
        _origin, url = self.driver("rw", "one")
        (answer,) = self.ws_http(
            "one",
            [
                {
                    "method": "POST",
                    "url": f"{url}.git/info/lfs/objects/batch",
                    "body": json.dumps({"operation": "upload", "objects": []}),
                    "headers": {"Content-Type": "application/vnd.git-lfs+json"},
                    "lease": self.lease_token("rw", "one"),
                    "ca": self.ca_file("rw"),
                }
            ],
        )
        self.report.check(
            "lfs: a Git LFS batch request gets 501 and the message",
            answer.get("status") == 501
            and "Git LFS is not supported" in answer.get("body", ""),
            json.dumps(answer)[:300],
        )

    def ide_checks(self) -> None:
        repo = self.wait_clone("one", "rw")
        before = self.driver_log("rw").count("info/refs?service=git-upload-pack")
        bare = (
            "env -i HOME=" + WORKSPACE_HOME + " PATH=/usr/local/bin:/usr/bin:/bin "
            f"git -C {shlex_quote(repo)} ls-remote origin refs/heads/{self.branch} 2>&1\n"
            "echo listed=$?\n"
        )
        _rc, ide = self.ws("one", bare)
        pod = self.workspace_pod(self.threads["one"])
        rc, login, err = run(
            K
            + ["exec", "-i", pod, "-c", WORKSPACE_CONTAINER, "--"]
            + ["su", "-", "agent-host", "-c", "bash -s"],
            data=(
                f"git -C {shlex_quote(repo)} ls-remote origin "
                f"refs/heads/{self.branch} 2>&1\necho listed=$?\n"
            ),
            timeout=120,
        )
        after = self.driver_log("rw").count("info/refs?service=git-upload-pack")
        self.report.check(
            "ide: git in an IDE terminal's bare environment and in an "
            "ssh-gateway login shell lists the branch through the driver",
            "listed=0" in ide
            and "listed=0" in login
            and self.branch in ide
            and self.branch in login
            and after >= before + 2,
            f"ide={ide[-120:]!r} login={(login or err)[-120:]!r}",
        )

    def redirect_checks(self) -> None:
        # No checkout exists (its clone got the driver's 502): name the
        # binding's rules, as the agent's own wait for the driver does.
        rules = (
            f"~/.srw-credentials/git/bindings/{self.connectors['redirect']}.gitconfig"
        )
        _rc, out = self.ws(
            "two",
            f"git -c include.path={rules} ls-remote "
            f"{shlex_quote(clean_remote(self.redirect))} 2>&1\necho listed=$?\n",
            timeout=120,
        )
        log = self.driver_log("redirect")
        _rc, readme = self.ws("two", "cat ~/workspace/README.md 2>/dev/null\n")
        line = next(
            (row for row in readme.splitlines() if self.name("redirect") in row), ""
        )
        self.report.check(
            "redirect: the forge's redirect is not followed with the credential "
            "(git shows the driver's 502 and message; the driver logs it; the "
            "README says the repository was not cloned and why)",
            "listed=0" not in out
            and "never follows a redirect" in out
            and "the upstream redirected to" in log
            and "NOT cloned" in line
            and "redirect" in line,
            f"{out[-200:]!r} readme={line[-200:]!r}; if the forge answered without "
            "a redirect, pass --redirect-url",
        )

    def fallback_checks(self) -> None:
        """A token repository on a host the driver may not reach (inside the
        cluster's service range) falls back to the token in its clone URL,
        visibly: no lease, the README says so and why, Test says so too."""
        lease = self.lease_row("private", "two", live=False)
        _rc, readme = self.ws("two", "cat ~/workspace/README.md 2>/dev/null\n")
        line = next(
            (row for row in readme.splitlines() if self.name("private") in row), ""
        )
        self.report.check(
            "fallback: a private-host token repository gets no lease, and session "
            "two's README says it is NOT reached through the driver, and why",
            lease is None
            and "NOT through SRW's git swap driver" in line
            and "may not reach the upstream" in line,
            line[-300:] or "no README line",
        )
        status, result = self.owner.call(
            "POST", f"/api/datasources/{self.connectors['private']}/test", {}
        )
        delivery = ((result or {}).get("details") or {}).get("delivery") or {}
        self.report.check(
            "fallback: Test of the private-host repository says its token goes in "
            "the clone URL, not through the driver, and why",
            status == 200
            and delivery.get("mode") == "token-in-url"
            and "may not reach the upstream" in (delivery.get("reason") or "")
            and "NOT through SRW's git swap driver" in str(result.get("message")),
            json.dumps(delivery)[:300],
        )

    def reused_checks(self) -> None:
        repo = shlex_quote(self.wait_clone("two", "rw"))
        planted = self.upstream.replace("https://", f"https://oauth2:{self.fake}@", 1)
        self.ws(
            "two",
            f"git -C {repo} config --unset transfer.credentialsInUrl || true\n"
            f"git -C {repo} config remote.origin.url {shlex_quote(planted)}\n",
        )
        _rc, config = self.ws("two", f"cat {repo}/.git/config\n")
        if "oauth2:" not in config:
            raise GateError("the pre-C3 origin was not planted")
        # The next turn of a stateless session attaches its workspace again.
        self.say("two", "Reply with the single word again.")

        def reset() -> str | None:
            _rc, text = self.ws("two", f"cat {repo}/.git/config\n")
            return text if "oauth2:" not in text else None

        try:
            config = wait_for(
                "the reused checkout's origin reset",
                reset,
                timeout=self.args.turn_timeout,
                interval=10,
            )
        except GateError:
            config = ""
        problems = (
            config_problems(
                config,
                clean_url=clean_remote(self.upstream),
                tokens=[self.fake, self.token],
            )
            if config
            else ["the oauth2: origin stayed"]
        )
        self.report.check(
            "reused: a pre-C3 checkout loses its oauth2:<token>@ origin on the "
            "next attach and refuses credentials in URLs again",
            not problems,
            "; ".join(problems),
        )

    def revoked_checks(self) -> None:
        revoked = self.lease_token("rw", "one")
        before = self.lease_row("rw", "one") or {}
        own = self.lease_token("rw", "two")
        self.owner.ok(
            "PATCH",
            f"/api/persistent/threads/{self.threads['one']}/config",
            {"datasource_ids": [self.connectors["ro"]]},
        )
        row = wait_for(
            "rw's lease in session one revoked",
            lambda: (
                found
                if (found := self.lease_row("rw", "one", live=False))
                and found.get("id") == before.get("id")
                and found.get("revoke_reason")
                else None
            ),
            timeout=120,
            interval=3,
        )
        _origin, url = self.driver("rw", "two")
        ca = self.ca_file("rw")
        target = f"{url}.git/info/refs?service=git-upload-pack"
        started = time.monotonic()

        def refused() -> list[dict] | None:
            answers = self.ws_http(
                "two",
                [
                    {"method": "GET", "url": target, "lease": revoked, "ca": ca},
                    {"method": "GET", "url": target, "lease": own, "ca": ca},
                ],
            )
            return answers if answers[0].get("status") == 401 else None

        try:
            answers = wait_for(
                "the revoked lease refused", refused, timeout=90, interval=5
            )
        except GateError:
            answers = []
        lag = round(time.monotonic() - started)
        self.report.check(
            "revoked: a detached connector's lease gets 401 with WWW-Authenticate: "
            "Basic within the revocation lag, while session two's own lease of the "
            "same connector still gets 200",
            row.get("revoke_reason") == "connector_detached"
            and len(answers) == 2
            and answers[0].get("www_authenticate", "").startswith("Basic")
            and answers[1].get("status") == 200
            and lag <= 60,
            f"reason={row.get('revoke_reason')} lag={lag}s "
            f"statuses={[a.get('status') for a in answers]}",
        )

        # A stateless session applies the detach when it next attaches.
        self.say("one", "Reply with the single word detached.")

        def unwired() -> bool:
            rc, _out = self.ws(
                "one",
                "test ! -e ~/.srw-credentials/git/bindings/"
                f"{self.connectors['rw']}.gitconfig\n",
            )
            return rc == 0

        try:
            wait_for(
                "session one's rw wiring removed",
                unwired,
                timeout=self.args.turn_timeout,
                interval=10,
            )
            removed = True
        except GateError:
            removed = False
        self.report.check(
            "revoked: session one's wiring for the detached connector is removed",
            removed,
        )

    def cleanup(self) -> list[str]:
        problems: list[str] = []

        def step(label: str, action: Callable[[], Any]) -> None:
            try:
                if action() is False:
                    problems.append(label)
            except GateError as exc:
                problems.append(f"{label} ({exc})")

        for thread in dict.fromkeys([*self.threads.values(), *self.titled_threads()]):

            def delete_thread(thread=thread) -> bool:
                def gone() -> bool:
                    status, _body = self.owner.call(
                        "DELETE",
                        f"/api/persistent/threads/{thread}?force=true&permanent=true",
                    )
                    if status == 404:
                        return True
                    return (
                        sql(f"SELECT count(*) FROM threads WHERE id = {lit(thread)}")
                        == "0"
                    )

                return bool(wait_for("session deleted", gone, timeout=300, interval=5))

            step(f"delete session {thread}", delete_thread)
        for label, datasource_id in list(self.connectors.items()):

            def delete(datasource_id=datasource_id) -> bool:
                status, _body = self.owner.call(
                    "DELETE", f"/api/datasources/{datasource_id}"
                )
                return status in (200, 204, 404)

            step(f"delete connector {label}", delete)
        if self.project:

            def project_deleted() -> bool:
                status, _body = self.owner.call(
                    "DELETE", f"/api/projects/{self.project}"
                )
                return status in (200, 204, 404)

            step(
                "delete project",
                lambda: bool(
                    wait_for(
                        "project deleted", project_deleted, timeout=180, interval=10
                    )
                ),
            )
        if self.client_started:
            step(
                "delete the OAuth client",
                lambda: self.keycloak("delete").get("refused") == [],
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
        if self.pushed:

            def delete_branches() -> bool:
                left = [
                    ref
                    for ref in self.upstream_refs()
                    if ref.startswith(("refs/heads/srw-gate-", "refs/tags/srw-gate-"))
                    and self.gate_id in ref
                ]
                for ref in left:
                    rc, out = self.upstream_git("push", "--delete", ref)
                    if rc:
                        raise GateError(f"the forge kept {ref}: {out[-200:]}")
                return True

            step("delete this run's refs upstream", delete_branches)
        for problem in problems:
            print(f"cleanup: {problem} failed", flush=True)
        return problems

    def residue(self) -> list[str]:
        """What this run created and cleanup did not remove."""
        left: list[str] = []
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
        if (
            self.project
            and sql(f"SELECT count(*) FROM projects WHERE id = {lit(self.project)}")
            != "0"
        ):
            left.append(f"project {self.project}")
        if self.client_started:
            try:
                counts = self.keycloak("count")
                if counts.get("clients"):
                    left.append(f"Keycloak residue {counts}")
            except GateError as exc:
                left.append(f"Keycloak residue unknown ({exc})")
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
        for thread in self.threads.values():
            try:
                wait_for(
                    f"pods of {thread} gone",
                    lambda thread=thread: not json.loads(
                        command(
                            K
                            + ["get", "pods", "-l", f"srw/thread-id={thread}"]
                            + ["-o", "json"]
                        )
                    )["items"],
                    timeout=180,
                    interval=10,
                )
            except GateError:
                left.append(f"pods of session {thread}")
        if self.deny_probe_started and self.namespace:
            listing = json.loads(
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
            if listing:
                left.append(f"pods {GATE_LABEL}={self.gate_id} in {self.namespace}")
        if self.pushed:
            try:
                upstream = [ref for ref in self.upstream_refs() if self.gate_id in ref]
                if upstream:
                    left.append(f"refs upstream: {upstream}")
            except GateError as exc:
                left.append(f"upstream refs unknown ({exc})")
        return left

    # -- run -------------------------------------------------------------------
    def run(self) -> int:
        try:
            self.preflight()
            self.accounts()
            self.fixture()
            # startup creates both sessions and the pods every later phase
            # needs; a failure there ends the run (cleanup still runs).
            self.startup_checks()
            for phase in (
                self.push_checks,
                self.readonly_checks,
                self.refs_checks,
                self.lease_checks,
                self.lfs_checks,
                self.ide_checks,
                self.redirect_checks,
                self.fallback_checks,
                self.reused_checks,
                self.revoked_checks,
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
                            "branch": self.branch,
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


def shlex_quote(text: str) -> str:
    import shlex

    return shlex.quote(text)


VALUES_LOCAL_KEYS = """values-local.yaml keys (the k3d profile of values-local.yaml.example):
  orchestrator.connectorLeases.exchangePort: 8088
  connectors.servicePods.enabled: true
  connectors.servicePods.maxInstallation: 4      (this gate runs three pods)
  connectors.servicePods.reconcileIntervalSeconds: 5
  connectors.servicePods.refusedCidrs: [172.16.0.0/12, 10.42.0.0/16, 10.43.0.0/16, 169.254.0.0/16]
  connectors.drivers.gitSwap.enabled: true
  connectors.drivers.gitSwap.image: {repository: srw-registry:5000/srw-driver-git-swap, tag: dev}
  connectors.drivers.gitSwap.fallback: token-in-url
  connectors.drivers.ca: {}                      (the chart generates SRW's driver CA)
Tilt overrides the shim and swap images (repository, tag, digest). The upstream
is the operator's disposable repository on a public forge (--upstream-url and
--upstream-token-file): driver pods may not reach the in-cluster Gitea.
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
        "--upstream-url",
        help=(
            "a DISPOSABLE repository on a public forge, in clean form "
            "(https://github.com/<owner>/<repo>.git): the gate pushes "
            "srw-gate-<gate id> refs there and deletes them at cleanup"
        ),
    )
    parser.add_argument(
        "--upstream-token-file",
        help=(
            "a file holding a token that can read and push --upstream-url "
            "(a fine-grained token for that one repository); read here, never "
            "on a command line"
        ),
    )
    parser.add_argument(
        "--redirect-url",
        default="https://github.com/docker/docker.git",
        help=(
            "a repository URL the forge answers git with a redirect for (a "
            "renamed repository's old URL; docker/docker is moby/moby on GitHub)"
        ),
    )
    parser.add_argument(
        "--private-url",
        default=DEFAULT_PRIVATE_URL,
        help=(
            "a token repository URL on a host the driver may not reach (inside "
            "the cluster's service range, or a private address the project tier "
            "refuses): it must fall back visibly; it gets a fake token"
        ),
    )
    parser.add_argument("--forge", choices=FORGES, help="defaults from the host")
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
        raise SafetyError("--gate-id must be c3- followed by 10 hex digits")
    if not _MODEL_RE.fullmatch(args.model):
        raise SafetyError("model id is malformed")
    if not re.fullmatch(r"[a-z][a-z0-9._-]{0,63}", args.user):
        raise SafetyError("user name is malformed")
    for name in ("upstream_url", "redirect_url", "private_url"):
        value = getattr(args, name)
        if value is not None and (
            not _UPSTREAM_RE.fullmatch(value)
            or {".", ".."} & set(value.split("://", 1)[1].split("/"))
        ):
            raise SafetyError(
                f"--{name.replace('_', '-')} must be a clean https://host/path URL "
                "(lowercase host, port 443, no credentials, no trailing slash)"
            )
    if args.run:
        if not args.upstream_url or not args.upstream_token_file:
            raise SafetyError(
                "--run needs --upstream-url and --upstream-token-file: the "
                "operator's disposable repository and a token for it"
            )
        token_file = Path(args.upstream_token_file).expanduser()
        if not token_file.is_file() or not token_file.read_text().strip():
            raise SafetyError("--upstream-token-file names no file with a token")
        if token_file.stat().st_mode & 0o077:
            raise SafetyError("--upstream-token-file must not be readable by others")
    if args.forge is None and args.upstream_url:
        host = args.upstream_url.split("://", 1)[1].split("/", 1)[0]
        args.forge = {"github.com": "github", "gitlab.com": "gitlab"}.get(host, "gitea")
    if not 60 <= args.turn_timeout <= 1800:
        raise SafetyError("--turn-timeout must be between 60 and 1800 seconds")
    if not 60 <= args.start_timeout <= 1800:
        raise SafetyError("--start-timeout must be between 60 and 1800 seconds")


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
    return GitSwapGate(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
