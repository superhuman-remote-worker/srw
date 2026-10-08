#!/usr/bin/env python3
"""Local k3d gate for connector drivers D5a: managed MCP, HTTP-native images.

Design: knowledge-base/knowledge/features/connector_drivers.md, "MCP", "Three
planes" (service), "Reachability", "The lease service" (introspection) and
slice D5a. Templates: scripts/k3d-service-driver-gate.py (D5: the hosting
preflights, the enforcement and DNS probes) and
scripts/k3d-connector-selection-gate.py (D3c: a disposable OAuth client and
account). The same safety envelope: dry-run by default, the exact
k3d-srw/srw context, secrets only on ``kubectl exec -i`` stdin and scrubbed
from every printed line, every in-pod program capping its own memory, and a
cleanup in ``finally`` that touches only what this run created and then
checks for residue by gate id.

It needs the k3d profile of deployment/values-local.yaml.example (keys in
--help) and Tilt, which builds srw-driver-shim, srw-driver-mcp-front and
srw-driver-mcp-test and pins them by digest. The stock image is the official
Gitea MCP server (docker.gitea.com/gitea-mcp-server, by digest), pulled by
the k3d node.

Fixtures (all disposable, named after the gate id):

  client      ``<gate id>-oauth``, a public Keycloak client with direct
              access grants and the profile, email and roles scopes, which
              both accounts log in with (the D3a/D3c fixture)
  account     the second account, a Keycloak user named the gate id, and its
              app row admitted before its first login (the D3c fixture)
  project     one project of the owner (tier internet-only)
  connectors  of the owner: ``gitea`` (srw.gitea-mcp/v1, the stock image,
              ReadWrite, pointed at --gitea-url with a random fake token) and
              ``notes`` (srw.mcp-test/v1, SRW's test server, a random token);
              of the second account: ``gitea-ro`` (srw.gitea-mcp/v1,
              ReadOnly)
  sessions    ``one`` (the owner, gitea and notes) and ``two`` (the second
              account, gitea-ro only)

Checks (each printed PASS/FAIL; the exit status is 0 only if all pass):

  preflight   every orchestrator pod and stateless agent pod serves this
              checkout's D5a modules, byte for byte; hosting on with an
              exchange and canary port, a digest-pinned shim and front, both
              managed servers installed, room for three pods; migrations
              0360-0363 and 0390-0392 applied; the driver namespace has Pod
              Security baseline and its default deny, the cluster enforces it
              (the 12-probe harness: a busybox pod under the default deny never
              reaches the orchestrator's API port) and refusedCidrs covers the
              node; the Gitea host resolves and answers from the orchestrator
  startup     each managed pod's canary wait ran first and exited 0 after the
              default deny was enforced; the pod is the server image as
              itself (its own program, no identity, request file or SRW
              environment) beside the pinned front; its Secret holds no
              upstream credential; the front's log holds no token
  serve       the stock Gitea image serves session one: its README lists
              both servers as managed and connected; through the front, the
              agent's own MCP client lists Gitea's tools and calls
              get_gitea_mcp_server_version; the test server's whoami proves
              the front injected the notes connector's token, and its
              leak_credential answer comes back scrubbed
  credential  the agent pod holds no credential of either server's upstream:
              not in any process's environment or command line, nor in any
              file under /app, /tmp, /home, /root, /var/tmp or /run (neither
              does session one's workspace)
  denied      an execution without the connector gets 401: no token, a
              malformed one and session two's lease (another connector's)
              against the gitea and notes endpoints; session two's client
              finds the gitea server unavailable
  workspace   session one's workspace pod cannot connect to either endpoint,
              not even with session one's own lease
  readonly    session two's ReadOnly binding of the stock image lists only
              Gitea's read tools; a call of create_repo is answered "Unknown
              tool" without reaching the server, and the exchange never saw a
              write for that lease; session one's ReadWrite binding lists it
  replace     the notes pod is deleted mid-session: the agent's client keeps
              calling whoami and gets an answer from the new pod (a new pod
              name, the same token digest) without a tool error, reconnecting
              within its budget and waiting for the new pod's start; the
              endpoint Service names the new pod and the old identity is
              stopped
  cleanup     sessions, connectors (identities and leases cascade), project,
              the second account (app row, then Keycloak user), the OAuth
              client and probe pods are gone, and no object in the driver
              namespace names this run's connectors (endpoint Services
              included)

Run with the repository venv on the k3d-srw cluster, alone: this is a
mutating gate. The owner (--user) must be an administrator (it admits and
deletes the disposable account).

  .venv/bin/python scripts/k3d-managed-mcp-gate.py           # plan
  .venv/bin/python scripts/k3d-managed-mcp-gate.py \\
      --run --confirm LOCAL-K3D-DISPOSABLE
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import ipaddress
import json
import re
import secrets
import subprocess
import sys
import threading
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
AGENT_CONTAINER = "agent"
KEYCLOAK_TOKEN_URL = "http://srw-keycloak:8080/realms/srw/protocol/openid-connect/token"
POD_ROOT = "/app"
DEFAULT_MODEL = "muse-spark-1.3-contributor"
GITEA_TYPE = "gitea_mcp"
GITEA_DRIVER = "srw.gitea-mcp/v1"
NOTES_TYPE = "mcp_test"
NOTES_DRIVER = "srw.mcp-test/v1"
GATE_LABEL = "srw.io/gate"
ACCOUNT_DOMAIN = "example.invalid"
FRONT_PORT = 8080
#: Gitea tools the stock image lists to a ReadWrite binding and must hide
#: from a ReadOnly one (gitea-mcp 1.8).
GITEA_WRITE_TOOLS = (
    "create_repo",
    "create_or_update_file",
    "delete_file",
    "create_branch",
    "delete_branch",
    "issue_write",
)
_SELECTOR = (
    "app.kubernetes.io/instance=srw,app.kubernetes.io/name=superhuman-remote-worker"
)
_GATE_ID_RE = re.compile(r"d5a-[0-9a-f]{10}\Z")
_MODEL_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@-]{0,199}\Z")
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")
_URL_RE = re.compile(
    r"https://[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?(?::[0-9]{1,5})?\Z"
)
_IPV4_RE = re.compile(r"(?:\d{1,3}\.){3}\d{1,3}\Z")
#: The migrations this gate needs: D5's service pods and D5a's re-pin.
MIGRATIONS = (
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
            "src/shared/datasource_policy.py",
            "src/orchestrator/application/__init__.py",
            "src/orchestrator/application/connectors.py",
            "src/orchestrator/application/settings.py",
            "src/orchestrator/services/connector_credential_leases.py",
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
    # The agent is the managed servers' client.
    ServedSet(
        "stateless agent",
        "agent-stateless",
        AGENT_CONTAINER,
        (SHARED_CONNECTORS,),
        (
            "src/shared/datasource_policy.py",
            "src/agent/tools/mcp/manager.py",
            "src/agent/connectors/mcp.py",
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
# never take the pod to the OOM killer (as in the C0 to D5 gates).
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
# to the agent pod on stdin only, as a binding delivers it.
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

# The agent's own MCP client (agent.tools.mcp.MCPManager) in an agent pod,
# configured from a binding exactly as the orchestrator delivers it: the
# endpoint URL and the lease token. It connects (waiting for the pod's front
# as a session does), lists the tools and makes the calls asked for; ``raw``
# calls go straight to the session, past the tool list.
_MCP_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import asyncio, json, sys, time
from agent.tools.mcp.manager import MCPManager
cap_memory()
request = json.loads(sys.stdin.readline())

def text(result):
    if isinstance(result, tuple):
        result = result[0]
    if isinstance(result, list):
        result = "\n".join(
            block.get("text", "") if isinstance(block, dict) else str(block)
            for block in result
        )
    return str(result)

async def main():
    entry = request["entry"]
    manager = MCPManager([entry])
    started = time.monotonic()
    await manager.connect_all()
    tools = {t.metadata["mcp_tool_name"]: t for t in manager.get_langchain_tools()}
    out = {
        "status": manager.statuses[entry["name"]],
        "connect_seconds": round(time.monotonic() - started, 1),
        "tools": sorted(tools),
        "calls": [],
    }
    handle = manager._handles[0]
    for call in request.get("calls", []):
        name, arguments = call["tool"], call.get("arguments") or {}
        record = {"tool": name}
        try:
            if call.get("raw"):
                if handle.session is None:
                    record["error"] = "no session"
                else:
                    result = await handle.session.call_tool(name, arguments)
                    record["text"] = "\n".join(
                        getattr(block, "text", "") or "" for block in result.content
                    )
                    record["is_error"] = bool(result.isError)
            elif name in tools:
                record["text"] = text(await tools[name].coroutine(**arguments))
            else:
                record["missing"] = True
        except Exception as error:
            record["error"] = type(error).__name__ + ": " + str(error)[:200]
        out["calls"].append(record)
    out["reconnects"] = len(handle.reconnects)
    await manager.aclose()
    return out
print(json.dumps(asyncio.run(main())))
"""
)

# A session's client while its pod is replaced: whoami until it answers
# from another pod. The first line says which pod answered first, so the
# gate deletes that pod; the last line is the verdict.
_REPLACE_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import asyncio, json, sys, time
from agent.tools.mcp.manager import MCPManager
cap_memory()
request = json.loads(sys.stdin.readline())

def text(result):
    if isinstance(result, tuple):
        result = result[0]
    if isinstance(result, list):
        result = "\n".join(
            block.get("text", "") if isinstance(block, dict) else str(block)
            for block in result
        )
    return str(result)

async def main():
    entry = request["entry"]
    manager = MCPManager([entry])
    await manager.connect_all()
    tools = {t.metadata["mcp_tool_name"]: t for t in manager.get_langchain_tools()}
    if "whoami" not in tools:
        print(json.dumps({"phase": "failed", "status": manager.statuses}), flush=True)
        return
    whoami = tools["whoami"]
    first = json.loads(text(await whoami.coroutine()))
    print(json.dumps({"phase": "ready", "pod": first["pod"]}), flush=True)
    deadline = time.monotonic() + request["timeout"]
    answers = []
    while time.monotonic() < deadline:
        began = time.monotonic()
        answer = text(await whoami.coroutine())
        try:
            parsed = json.loads(answer)
        except ValueError:
            parsed = None
        answers.append({
            "seconds": round(time.monotonic() - began, 1),
            "pod": parsed.get("pod") if parsed else None,
            "sha": parsed.get("credential_sha256") if parsed else None,
            "error": None if parsed else answer[:200],
        })
        if parsed and parsed.get("pod") != first["pod"]:
            break
        await asyncio.sleep(2)
    handle = manager._handles[0]
    print(json.dumps({
        "phase": "done", "first": first["pod"], "answers": answers,
        "reconnects": len(handle.reconnects), "status": handle.status,
    }), flush=True)
    await manager.aclose()
asyncio.run(main())
"""
)

# Plain HTTP and TCP probes from a pod: an MCP initialize with a bearer (or
# none), a TCP connect. Only statuses come back.
_NET_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import json, socket, sys, urllib.error, urllib.request
cap_memory()
request = json.loads(sys.stdin.readline())
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
INITIALIZE = json.dumps({
    "jsonrpc": "2.0", "id": 1, "method": "initialize",
    "params": {"protocolVersion": "2025-06-18", "capabilities": {},
               "clientInfo": {"name": "srw-gate", "version": "1"}},
}).encode()

def initialize(call):
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    if call.get("bearer") is not None:
        headers["Authorization"] = "Bearer " + call["bearer"]
    req = urllib.request.Request(call["url"], data=INITIALIZE, method="POST", headers=headers)
    try:
        with opener.open(req, timeout=call.get("timeout", 8)) as response:
            return {"status": response.status}
    except urllib.error.HTTPError as error:
        return {"status": error.code}
    except Exception as error:
        return {"status": 0, "error": type(error).__name__}

def connect(call):
    try:
        with socket.create_connection((call["host"], call["port"]), timeout=call.get("timeout", 4)):
            return {"reachable": True}
    except OSError as error:
        return {"reachable": False, "error": type(error).__name__}

print(json.dumps([
    initialize(call) if call["kind"] == "initialize" else connect(call)
    for call in request["calls"]
]))
"""
)

# Whether any of the given secrets is held by a pod: in a process's
# environment or command line, or in a file under the listed roots (files
# up to 4 MiB; /proc, /sys and /dev are never walked). Counts only: never a
# secret, a path holding one is named.
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

# The run's Keycloak fixtures (the D3c gate's program), through the
# orchestrator's own admin credentials: they stay in the pod and are never
# printed. The client carries this run's marker; the user is named the gate
# id. Every action finds both by exact name; delete removes only what carries
# this run's marker or email (and its recorded id, once known).
_KEYCLOAK_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import json, os, sys, urllib.error, urllib.parse, urllib.request
cap_memory()
request = json.loads(sys.stdin.readline())
base = os.environ.get("KEYCLOAK_URL", "").rstrip("/")
realm = os.environ.get("KEYCLOAK_REALM", "") or "srw"
admin = os.environ.get("KEYCLOAK_ADMIN_USER", "")
admin_password = os.environ.get("KEYCLOAK_ADMIN_PASSWORD", "")
if not (base and admin and admin_password):
    print(json.dumps({"error": "the orchestrator has no Keycloak admin credentials"}))
    raise SystemExit(0)
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
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
def listing(path, query):
    status, text, _location = call(
        "GET", admin_api + path + "?" + urllib.parse.urlencode(query)
    )
    if status != 200:
        raise SystemExit("%s lookup answered HTTP %d" % (path, status))
    return json.loads(text)
def users():
    name = request["username"]
    found = listing("/users", {"username": name, "exact": "true"})
    return [u for u in found if u.get("username") == name]
def clients():
    name = request["client"]
    return [c for c in listing("/clients", {"clientId": name}) if c.get("clientId") == name]
def user_owned(user):
    return user.get("email") == request["email"] and (
        not request.get("user_id") or user.get("id") == request["user_id"]
    )
def client_owned(client):
    return (client.get("attributes") or {}).get("srw-gate") == request["marker"] and (
        not request.get("client_uuid") or client.get("id") == request["client_uuid"]
    )
def create(path, body, existing, owned):
    if existing():
        return {"exists": True}
    status, text, location = call("POST", admin_api + path, body)
    if status != 201:
        return {"error": "%s create answered HTTP %d: %s" % (path, status, text[:200])}
    return {
        "id": location.rstrip("/").rsplit("/", 1)[-1] if location else "",
        "found": [item["id"] for item in existing() if owned(item)],
    }
def counts():
    return {
        "users": len(users()) if request.get("user_started") else 0,
        "clients": len(clients()) if request.get("client_started") else 0,
    }
action = request["action"]
if action == "create-client":
    print(json.dumps(create("/clients", {
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
    }, clients, client_owned)))
elif action == "create-user":
    print(json.dumps(create("/users", {
        "username": request["username"],
        "email": request["email"],
        "emailVerified": True,
        "enabled": True,
        "firstName": "D5a",
        "lastName": "Gate",
        "requiredActions": [],
        "credentials": [
            {"type": "password", "value": request["password"], "temporary": False}
        ],
    }, users, user_owned)))
elif action == "delete":
    # The user first: the client is how the gate still logs in until the end.
    deleted, refused = [], []
    for path, found, owned in (
        ("/users/", users() if request.get("user_started") else [], user_owned),
        ("/clients/", clients() if request.get("client_started") else [], client_owned),
    ):
        for item in found:
            if not owned(item):
                refused.append(item.get("id"))
                continue
            status, _text, _location = call("DELETE", admin_api + path + item["id"])
            if status not in (204, 404):
                raise SystemExit("%s delete answered HTTP %d" % (path, status))
            deleted.append(item["id"])
    print(json.dumps({"deleted": deleted, "refused": refused, **counts()}))
elif action == "count":
    print(json.dumps(counts()))
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


def endpoint_name(connector_id: str, digest: str) -> str:
    """The connector's endpoint Service at one digest (connector_service_launch)."""
    hex_id = connector_id.replace("-", "")
    return f"srw-ep-{hex_id}-{digest.removeprefix('sha256:')[:12]}"


def canary_passed(pod: dict) -> bool:
    """Whether the canary-wait init container ran first, alone, and exited 0."""
    spec = pod.get("spec") or {}
    names = [c.get("name") for c in spec.get("initContainers") or []]
    statuses = {
        status.get("name"): status
        for status in (pod.get("status") or {}).get("initContainerStatuses") or []
    }
    state = (statuses.get("canary-wait") or {}).get("state") or {}
    terminated = state.get("terminated") or {}
    return names == ["canary-wait"] and terminated.get("exitCode") == 0


def pod_ready(pod: dict) -> bool:
    """Running with every container ready: the front's readiness is a real
    MCP probe of the server."""
    status = pod.get("status") or {}
    statuses = status.get("containerStatuses") or []
    return (
        status.get("phase") == "Running"
        and {s.get("name") for s in statuses} == {"driver", "front"}
        and all(s.get("ready") for s in statuses)
    )


def layout_problems(pod: dict, front_image: str) -> list[str]:
    """How a managed MCP pod differs from the server as itself beside the
    pinned front."""
    spec = pod.get("spec") or {}
    containers = {c.get("name"): c for c in spec.get("containers") or []}
    problems = []
    if set(containers) != {"driver", "front"}:
        return [f"containers {sorted(containers)}"]
    server, front = containers["driver"], containers["front"]
    if "@sha256:" not in server.get("image", ""):
        problems.append("the server image is not pinned by digest")
    if front.get("image") != front_image:
        problems.append(f"the front runs {front.get('image')}, not {front_image}")
    if any(str(e.get("name", "")).startswith("SRW_") for e in server.get("env") or []):
        problems.append("the server has SRW's environment")
    if any(m.get("name") == "delivery" for m in server.get("volumeMounts") or []):
        problems.append("the server mounts the delivery Secret")
    if server.get("ports"):
        problems.append("the server declares a port")
    if [p.get("name") for p in front.get("ports") or []] != ["srw-driver"]:
        problems.append("the front does not serve srw-driver alone")
    for name, container in containers.items():
        security = container.get("securityContext") or {}
        if (security.get("capabilities") or {}).get("drop") != ["ALL"]:
            problems.append(f"{name}: capabilities not dropped")
        if security.get("allowPrivilegeEscalation") is not False:
            problems.append(f"{name}: privilege escalation allowed")
    if spec.get("automountServiceAccountToken") is not False:
        problems.append("a ServiceAccount token may be mounted")
    return problems


def replace_verdict(result: dict, expected_sha: str) -> tuple[bool, str]:
    """A replaced pod answered without a tool error, reconnecting within the
    budget, with the same token digest."""
    answers = result.get("answers") or []
    last = answers[-1] if answers else {}
    errors = [a for a in answers if a.get("error")]
    waited = max((a.get("seconds") or 0 for a in answers), default=0)
    ok = (
        result.get("phase") == "done"
        and bool(last.get("pod"))
        and last.get("pod") != result.get("first")
        and last.get("sha") == expected_sha
        and not errors
        and 1 <= int(result.get("reconnects") or 0) <= 3
        and result.get("status") == "connected"
    )
    return ok, (
        f"first={result.get('first')} last={last.get('pod')} "
        f"reconnects={result.get('reconnects')} calls={len(answers)} "
        f"longest={waited}s errors={len(errors)}"
    )


def readonly_verdict(read_only: dict, read_write_tools: list[str]) -> tuple[bool, str]:
    """A ReadOnly binding lists no write tool and a write call is refused
    without reaching the server."""
    tools = set(read_only.get("tools") or [])
    hidden = sorted(tools & set(GITEA_WRITE_TOOLS))
    call = next(
        (c for c in read_only.get("calls") or [] if c.get("tool") == "create_repo"),
        {},
    )
    refused = "Unknown tool" in (call.get("error") or "")
    ok = (
        read_only.get("status") == "connected"
        and bool(tools)
        and not hidden
        and refused
        and set(GITEA_WRITE_TOOLS) <= set(read_write_tools)
        and tools < set(read_write_tools)
    )
    return ok, (
        f"read-only lists {len(tools)} tools, read-write {len(read_write_tools)}; "
        f"write tools visible: {hidden}; create_repo: {call.get('error') or call}"
    )


class Api:
    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self.password = secret(password)
        # The run's own OAuth client, once it exists.
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
    "preflight: orchestrator and stateless agent pods serve this checkout's D5a "
    "modules; hosting on with an exchange and canary port, a digest-pinned shim "
    "and front, the Gitea and test servers installed, room for three pods; "
    "migrations 0360-0363 and 0390-0392; the driver namespace's baseline and "
    "default deny, enforced (12-probe harness); refusedCidrs covers the node; "
    "the Gitea host resolves and answers from the orchestrator",
    "accounts: a disposable OAuth client and second account; the owner is an "
    "administrator, the second account is not",
    "startup: each managed pod's canary wait ran first and exited 0; the "
    "server image runs as itself beside the pinned front; its Secret and the "
    "front's log hold no credential or token",
    "serve: the stock Gitea image serves session one (README lists both "
    "servers connected; the agent's client lists Gitea's tools and calls "
    "get_gitea_mcp_server_version); the test server proves the injected token "
    "and the scrubbed leak",
    "credential: the agent pod (and session one's workspace) holds neither "
    "upstream token in any environment, command line or file",
    "denied: no token, a malformed one and session two's lease of another "
    "connector get 401 from both endpoints",
    "workspace: session one's workspace cannot connect to either endpoint",
    "readonly: session two's ReadOnly Gitea binding lists only read tools and "
    "create_repo is refused without a write exchange; session one lists it",
    "replace: the notes pod is deleted mid-session; the client reconnects within "
    "its budget, waits for the new pod and answers without a tool error; the "
    "endpoint names the new pod",
    "cleanup: sessions, connectors, project, account, OAuth client and probe "
    "pods are gone; no driver-namespace object names this run's connectors",
]


class ManagedMcpGate:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.gate_id = args.gate_id or f"d5a-{secrets.token_hex(5)}"
        self.report = Report(self.gate_id)
        self.owner = Api(args.user, args.password)
        self.other_email = f"{self.gate_id}@{ACCOUNT_DOMAIN}"
        self.other = Api(self.gate_id, secrets.token_urlsafe(24))
        # Everything this run creates, recorded before it is created.
        self.oauth_client = f"{self.gate_id}-oauth"
        self.client_started = False
        self.client_uuid: str | None = None
        self.account_started = False
        self.account_keycloak_id: str | None = None
        self.account_row = False
        self.connectors: dict[str, str] = {}  # label -> datasource id
        self.connector_api: dict[str, str] = {}  # label -> "owner" | "other"
        self.threads: dict[str, str] = {}  # label -> thread id
        self.thread_api: dict[str, str] = {}  # thread id -> "owner" | "other"
        self.project: str | None = None
        self.deny_probe = f"{self.gate_id}-denyprobe"
        self.deny_probe_started = False
        self.owner_id = ""
        self.other_id = ""
        self.namespace = ""
        self.front_image = ""
        self.orchestrator_ip = ""
        self.reconcile_seconds = 15
        self.lease_tokens: dict[tuple[str, str], str] = {}
        self.tokens = {
            label: secret(f"d5a-upstream-{label}-{secrets.token_hex(16)}")
            for label in ("gitea", "notes", "gitea-ro")
        }

    # -- naming and helpers ------------------------------------------------
    def name(self, label: str) -> str:
        return f"{self.gate_id} {label}"

    def title(self, label: str) -> str:
        return f"D5a managed MCP gate {self.gate_id} {label}"

    def digest(self, label: str) -> str:
        return hashlib.sha256(self.tokens[label].encode()).hexdigest()

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
            "FROM (SELECT id, pod_name, image_digest, ready_at IS NOT NULL AS ready, "
            "revoked_at IS NOT NULL AS revoked, revoke_reason, removed_at IS NOT "
            "NULL AS removed, created_at FROM connector_driver_identities WHERE "
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

    def live_lease(self, label: str, thread: str) -> dict | None:
        out = sql(
            "SELECT coalesce(row_to_json(l)::text, '') FROM (SELECT id, image_digest, "
            "access FROM connector_credential_leases WHERE "
            f"connector_id = {lit(self.connectors[label])} AND thread_id = "
            f"{lit(thread)} AND revoked_at IS NULL AND expires_at > now() LIMIT 1) l"
        )
        return json.loads(out) if out else None

    def lease_token(self, label: str, session: str) -> str:
        """The lease token session ``session`` holds for connector ``label``,
        as its binding delivered it; scrubbed from every printed line."""
        key = (label, session)
        if key not in self.lease_tokens:
            lease = self.live_lease(label, self.threads[session])
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

    def endpoint(self, label: str) -> str:
        """The URL a binding of ``label`` carries: its connector's endpoint
        Service at the binding's digest."""
        lease = None
        for thread in self.threads.values():
            lease = self.live_lease(label, thread)
            if lease:
                break
        if not lease:
            raise GateError(f"no live binding of {label}")
        name = endpoint_name(self.connectors[label], lease["image_digest"])
        return f"http://{name}.{self.namespace}.svc.cluster.local:{FRONT_PORT}/mcp"

    def entry(self, label: str, session: str, *, token: str | None = None) -> dict:
        """A binding of ``label`` as session ``session`` received it."""
        return {
            "type": GITEA_TYPE if label.startswith("gitea") else NOTES_TYPE,
            "name": self.name(label),
            "connection_url": self.endpoint(label),
            "credentials": {
                "lease": {
                    "id": "gate",
                    "connector_id": self.connectors[label],
                    "token": token
                    if token is not None
                    else self.lease_token(label, session),
                }
            },
            "datasource_id": self.connectors[label],
            "config": {},
        }

    def client(self, entry: dict, calls: list[dict] | None = None) -> dict:
        """The agent's own MCP client in an agent pod, on ``entry``."""
        return in_pod(
            self.agent_pod(),
            AGENT_CONTAINER,
            _MCP_PROGRAM,
            {"entry": entry, "calls": calls or []},
            timeout=self.args.start_timeout + 120,
        )

    def net(
        self, target: str, container: str, calls: list[dict], *, python: str = "python"
    ) -> list[dict]:
        return in_pod(target, container, _NET_PROGRAM, {"calls": calls}, python=python)

    def scan(
        self, target: str, container: str, roots: list[str], *, python: str = "python"
    ) -> dict:
        return in_pod(
            target,
            container,
            _SCAN_PROGRAM,
            {"secrets": list(self.tokens.values()), "roots": roots},
            python=python,
            timeout=300,
        )

    def ws(self, pod: str, script: str) -> tuple[int, str]:
        """Run ``script`` as agent-host in the workspace (stdin, never argv)."""
        rc, out, _err = run(
            K
            + ["exec", "-i", pod, "-c", WORKSPACE_CONTAINER, "--"]
            + ["su", "-s", "/bin/bash", "agent-host", "-c", "bash -s"],
            data="set -u\ncd ~\n" + script,
            timeout=120,
        )
        return rc, out

    def titled_threads(self) -> list[str]:
        out = sql(
            "SELECT id FROM threads WHERE "
            f"position({lit(self.gate_id)} in coalesce(title, '')) > 0"
        )
        return [row for row in out.splitlines() if _UUID_RE.fullmatch(row)]

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
            "checkout's D5a modules",
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
                "CONNECTOR_MCP_FRONT_IMAGE",
                "CONNECTOR_MANAGED_MCP_IMAGES",
                "CONNECTOR_SERVICE_MAX_INSTALLATION",
                "CONNECTOR_SERVICE_RECONCILE_SECONDS",
                "CONNECTOR_SERVICE_REFUSED_CIDRS",
                "CONNECTOR_SERVICE_NODE_IP",
            )
        }
        try:
            managed = json.loads(env["CONNECTOR_MANAGED_MCP_IMAGES"] or "{}")
        except ValueError:
            managed = {}
        configured = (
            env["CONNECTOR_SERVICE_PODS_ENABLED"].lower() == "true"
            and env["CONNECTOR_SERVICE_NAMESPACE"] != ""
            and env["CONNECTOR_LEASE_EXCHANGE_PORT"].isdigit()
            and env["CONNECTOR_LEASE_CANARY_PORT"].isdigit()
            and "@sha256:" in env["CONNECTOR_DRIVER_SHIM_IMAGE"]
            and "@sha256:" in env["CONNECTOR_MCP_FRONT_IMAGE"]
            and {GITEA_DRIVER, NOTES_DRIVER} <= set(managed)
        )
        self.report.check(
            "preflight: hosting on with an exchange and canary port, a "
            "digest-pinned shim and front, the Gitea and test servers installed",
            configured,
            json.dumps(env),
        )
        if not configured:
            raise GateError(
                "set connectors.servicePods.enabled, connectors.drivers.mcpFront, "
                "managedMcp.gitea.enabled and mcpTest.enabled as the k3d profile "
                "does, under Tilt"
            )
        self.namespace = env["CONNECTOR_SERVICE_NAMESPACE"]
        self.front_image = env["CONNECTOR_MCP_FRONT_IMAGE"]
        self.reconcile_seconds = int(
            float(env["CONNECTOR_SERVICE_RECONCILE_SECONDS"] or 15)
        )
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
            "preflight: migrations 0360-0363 and 0390-0392 applied",
            applied == str(len(MIGRATIONS)),
            f"{applied} of {len(MIGRATIONS)}",
        )
        if applied != str(len(MIGRATIONS)):
            raise GateError("the service-pod migrations are not applied")
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
        # The Gitea connector's pod is pinned from the orchestrator's lookup
        # of its host: a dead cluster DNS upstream refuses the launch.
        host = self.args.gitea_url.removeprefix("https://").split(":")[0]
        (upstream,) = self.net(
            ORCHESTRATOR,
            ORCHESTRATOR_CONTAINER,
            [{"kind": "connect", "host": host, "port": 443, "timeout": 15}],
        )
        self.report.check(
            "preflight: the Gitea host resolves and answers from the orchestrator "
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
            "client_started": self.client_started,
            "username": self.other.username,
            "email": self.other_email,
            "user_started": self.account_started,
        }
        if action == "create-user":
            payload["password"] = self.other.password
        if self.client_uuid:
            payload["client_uuid"] = self.client_uuid
        if self.account_keycloak_id:
            payload["user_id"] = self.account_keycloak_id
        result = in_pod(
            ORCHESTRATOR, ORCHESTRATOR_CONTAINER, _KEYCLOAK_PROGRAM, payload
        )
        if result.get("error"):
            raise GateError(f"Keycloak {action}: {result['error']}")
        return result

    @staticmethod
    def receipt(created: dict[str, Any], what: str) -> str:
        found = created.get("found") or []
        made = created.get("id") or (found[0] if len(found) == 1 else "")
        if not _UUID_RE.fullmatch(made or "") or found != [made]:
            raise GateError(f"no Keycloak receipt for the {what}: {created}")
        return made

    def accounts(self) -> None:
        self.client_started = True
        created = self.keycloak("create-client")
        if created.get("exists"):
            self.client_started = False  # not this run's: never adopted
            raise GateError(f"a Keycloak client {self.oauth_client} already exists")
        self.client_uuid = self.receipt(created, "OAuth client")
        self.owner.client_id = self.other.client_id = self.oauth_client
        owner = self.owner.ok("GET", "/api/auth/me")["user"]
        self.owner_id = str(owner["id"])
        if not owner.get("is_admin"):
            raise GateError("the owner must be an administrator")
        self.account_started = True
        created = self.keycloak("create-user")
        if created.get("exists"):
            self.account_started = False
            raise GateError(f"a Keycloak user {self.other.username} already exists")
        self.account_keycloak_id = self.receipt(created, "account")
        app_id = sql("SELECT gen_random_uuid()")
        if not _UUID_RE.fullmatch(app_id):
            raise GateError(f"no fresh id: {app_id!r}")
        self.other_id = app_id
        self.account_row = True
        sql(
            "INSERT INTO users (id, display_name, email, keycloak_sub, "
            "preferred_username, is_approved, approved_at, approved_by) VALUES ("
            f"{lit(app_id)}, {lit(self.other.username)}, {lit(self.other_email)}, "
            f"{lit(self.account_keycloak_id)}, {lit(self.other.username)}, true, "
            f"now(), {lit(self.owner_id)})"
        )
        other = self.other.ok("GET", "/api/auth/me")["user"]
        self.report.check(
            "accounts: the owner is an administrator; the disposable second "
            "account logs in as its admitted row and is none",
            str(other["id"]) == self.other_id and not other.get("is_admin"),
            f"second account {other.get('id')} admin={other.get('is_admin')}",
        )

    def create_connector(self, label: str, body: dict[str, Any], who: str) -> str:
        """POST a connector; its id is recorded before anything checks it."""
        api = self.owner if who == "owner" else self.other
        status, parsed = api.call(
            "POST",
            "/api/datasources",
            {"name": self.name(label), "scope_mode": "all", **body},
        )
        if isinstance(parsed, dict) and parsed.get("id"):
            self.connectors[label] = str(parsed["id"])
            self.connector_api[label] = who
        if status not in (200, 201) or label not in self.connectors:
            raise GateError(f"{label} create answered HTTP {status}: {parsed}")
        return self.connectors[label]

    def fixture(self) -> None:
        created = self.owner.ok(
            "POST",
            "/api/projects",
            {
                "name": self.name("project"),
                "description": "D5a managed MCP gate (disposable)",
                "user_id": self.owner_id,
            },
        )
        self.project = str(created["id"])
        url = self.args.gitea_url
        self.create_connector(
            "gitea",
            {
                "type": GITEA_TYPE,
                "credentials": {"token": self.tokens["gitea"]},
                "config": {"url": url, "access": "ReadWrite"},
            },
            "owner",
        )
        self.create_connector(
            "notes",
            {
                "type": NOTES_TYPE,
                "credentials": {"token": self.tokens["notes"]},
                "config": {"message": self.gate_id},
            },
            "owner",
        )
        self.create_connector(
            "gitea-ro",
            {
                "type": GITEA_TYPE,
                "credentials": {"token": self.tokens["gitea-ro"]},
                "config": {"url": url, "access": "ReadOnly"},
            },
            "other",
        )
        print(f"fixture: project {self.project}, connectors {self.connectors}")

    def create_session(self, label: str, connectors: list[str], who: str) -> str:
        api = self.owner if who == "owner" else self.other
        created = api.ok(
            "POST",
            "/api/persistent/threads",
            {
                "title": self.title(label),
                "permission_mode": "autonomous" if who == "owner" else "auto_accept",
                **({"project_id": self.project} if who == "owner" else {}),
                "datasource_ids": [self.connectors[c] for c in connectors],
                "config_override": {"workspace": {"backend": "sandbox"}},
                "model": self.args.model,
            },
        )
        thread = str(created.get("thread_id") or created["id"])
        self.threads[label] = thread
        self.thread_api[thread] = who
        print(f"session {label}: {thread}", flush=True)
        api.ok(
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

        return wait_for(
            f"connector {label}'s pod ready",
            probe,
            timeout=self.args.start_timeout,
            interval=5,
        )

    def startup_checks(self) -> None:
        self.create_session("one", ["gitea", "notes"], "owner")
        self.create_session("two", ["gitea-ro"], "other")
        rows = {label: self.wait_ready_pod(label) for label in self.connectors}
        for label, row in rows.items():
            pod = self.driver_pod(row) or {}
            _rc, canary_log, _err = run(
                self.kc + ["logs", row["pod_name"], "-c", "canary-wait"], timeout=60
            )
            self.report.check(
                f"startup: {label}'s canary wait ran first, alone, and exited 0 "
                "after the default deny was enforced",
                canary_passed(pod) and "default deny enforced" in canary_log,
                canary_log.splitlines()[-1][:200] if canary_log else "no log",
            )
            problems = layout_problems(pod, self.front_image)
            self.report.check(
                f"startup: {label}'s pod is the server image as itself beside the "
                "pinned front",
                not problems,
                "; ".join(problems),
            )
            secret_doc = json.loads(
                command(self.kc + ["get", "secret", row["pod_name"], "-o", "json"])
            )
            request = json.loads(
                base64.b64decode(secret_doc["data"]["request.json"]).decode()
            )
            held = [
                name
                for name, token in self.tokens.items()
                if token in json.dumps(request)
            ]
            self.report.check(
                f"startup: {label}'s pod Secret holds no upstream credential",
                request.get("credentials") == {} and not held,
                f"credentials={sorted(request.get('credentials') or {})} held={held}",
            )

    def serve_checks(self) -> None:
        one = self.workspace_pod(self.threads["one"])

        def listed() -> str | None:
            _rc, text = self.ws(one, "cat ~/workspace/README.md 2>/dev/null\n")
            pattern = r"\*\*{}\*\* \(mcp, (\d+) tools\) — managed by SRW"
            counts = [
                re.search(pattern.format(re.escape(self.name(label))), text)
                for label in ("gitea", "notes")
            ]
            return text if all(c and int(c.group(1)) > 0 for c in counts) else None

        try:
            wait_for(
                "session one's README lists both managed servers connected",
                listed,
                timeout=self.args.turn_timeout,
                interval=10,
            )
            listed_ok = True
        except GateError:
            listed_ok = False
        self.report.check(
            "serve: session one's own client connected both managed servers "
            "(README: mcp, N tools, managed by SRW)",
            listed_ok,
        )
        gitea = self.client(
            self.entry("gitea", "one"),
            [{"tool": "get_gitea_mcp_server_version"}],
        )
        (version,) = gitea["calls"]
        self.report.check(
            "serve: the stock Gitea image serves the session's binding through "
            "the front (tools listed, get_gitea_mcp_server_version answers)",
            gitea["status"] == "connected"
            and {"get_gitea_mcp_server_version", "create_repo"} <= set(gitea["tools"])
            and bool(version.get("text"))
            and not version.get("error"),
            f"{gitea['status']}, {len(gitea['tools'])} tools, "
            f"version={str(version.get('text'))[:80]!r} {version.get('error') or ''}",
        )
        self.gitea_tools = gitea["tools"]
        notes = self.client(
            self.entry("notes", "one"),
            [{"tool": "whoami"}, {"tool": "leak_credential"}],
        )
        whoami, leak = notes["calls"]
        try:
            answer = json.loads(whoami.get("text") or "{}")
        except ValueError:
            answer = {}
        self.report.check(
            "serve: the front injected the notes connector's token (the server "
            "saw its digest) and the lease token never reached the server",
            notes["status"] == "connected"
            and answer.get("credential_sha256") == self.digest("notes")
            and answer.get("message") == self.gate_id,
            json.dumps({k: answer.get(k) for k in ("message", "pod")}),
        )
        leaked = leak.get("text") or ""
        self.report.check(
            "serve: a server echoing the token is scrubbed by the front",
            "[redacted]" in leaked and "<redacted>" not in leaked,
            leaked[:120],
        )

    def credential_checks(self) -> None:
        agent = self.scan(
            self.agent_pod(),
            AGENT_CONTAINER,
            ["/app", "/tmp", "/home", "/root", "/var/tmp", "/run"],
        )
        self.report.check(
            "credential: the agent pod holds neither managed server's upstream "
            "token (environments, command lines, files)",
            not agent["found"] and agent["scanned"]["processes"] > 0,
            json.dumps(agent)[:400],
        )
        workspace = self.scan(
            self.workspace_pod(self.threads["one"]),
            WORKSPACE_CONTAINER,
            ["/home", "/tmp", "/root", "/var/tmp", "/run"],
            python="python3",
        )
        self.report.check(
            "credential: session one's workspace holds neither token either",
            not workspace["found"],
            json.dumps(workspace)[:400],
        )
        logs = ""
        for label in self.connectors:
            for row in self.live_pods(label):
                _rc, text, _err = run(
                    self.kc + ["logs", row["pod_name"], "-c", "front"], timeout=60
                )
                logs += text
        leaked = [name for name, token in self.tokens.items() if token in logs] + [
            "lease token" for token in self.lease_tokens.values() if token in logs
        ]
        self.report.check(
            "credential: the fronts logged calls (tool, class, status) but no "
            "token or credential",
            "call lease=" in logs and not leaked and "<redacted>" not in logs,
            f"leaked={leaked}",
        )

    def denied_checks(self) -> None:
        stranger = self.lease_token("gitea-ro", "two")
        calls = []
        for label in ("gitea", "notes"):
            url = self.endpoint(label)
            calls += [
                {"kind": "initialize", "url": url, "bearer": None},
                {"kind": "initialize", "url": url, "bearer": "scl_not-a-lease"},
                {"kind": "initialize", "url": url, "bearer": stranger},
            ]
        answers = self.net(self.agent_pod(), AGENT_CONTAINER, calls)
        self.report.check(
            "denied: an execution without the connector gets 401 (no token, a "
            "malformed one, another connector's lease) from both endpoints",
            [a.get("status") for a in answers] == [401] * 6,
            json.dumps(answers),
        )
        refused = self.client(self.entry("gitea", "two", token=stranger))
        self.report.check(
            "denied: session two's client finds the gitea server unavailable",
            refused["status"].startswith("unavailable") and not refused["tools"],
            refused["status"][:160],
        )

    def workspace_checks(self) -> None:
        pod = self.workspace_pod(self.threads["one"])
        calls = [
            {"kind": "initialize", "url": self.endpoint(label), "bearer": token}
            for label, token in (
                ("gitea", self.lease_token("gitea", "one")),
                ("notes", self.lease_token("notes", "one")),
            )
        ]
        answers = self.net(pod, WORKSPACE_CONTAINER, calls, python="python3")
        self.report.check(
            "workspace: session one's workspace cannot connect to either "
            "endpoint, even with session one's own leases",
            all(a.get("status") == 0 for a in answers),
            json.dumps(answers),
        )

    def readonly_checks(self) -> None:
        lease = self.live_lease("gitea-ro", self.threads["two"]) or {}
        read_only = self.client(
            self.entry("gitea-ro", "two"),
            [
                {
                    "tool": "create_repo",
                    "arguments": {"name": self.gate_id},
                    "raw": True,
                }
            ],
        )
        ok, detail = readonly_verdict(read_only, getattr(self, "gitea_tools", []))
        self.report.check(
            "readonly: session two's ReadOnly binding of the stock image lists "
            "only read tools; create_repo is answered 'Unknown tool' by the front; "
            "session one's ReadWrite binding lists every write tool",
            ok and lease.get("access") == "ReadOnly",
            detail,
        )
        writes = sql(
            "SELECT count(*) FROM security_events WHERE resource_id = "
            f"{lit(lease.get('id', ''))} AND detail LIKE '%operation=write%'"
        )
        self.report.check(
            "readonly: the exchange never saw a write for the ReadOnly lease",
            writes == "0",
            f"{writes} write events",
        )

    def replace_checks(self) -> None:
        (before,) = self.live_pods("notes")
        entry = self.entry("notes", "one")
        process = subprocess.Popen(
            K
            + ["exec", "-i", self.agent_pod(), "-c", AGENT_CONTAINER, "--"]
            + ["python", "-c", _REPLACE_PROGRAM],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        # Never wait on the client forever, not even for its first line.
        watchdog = threading.Timer(2 * self.args.start_timeout + 120, process.kill)
        watchdog.start()
        try:
            process.stdin.write(
                json.dumps({"entry": entry, "timeout": self.args.start_timeout}) + "\n"
            )
            process.stdin.close()
            first = json.loads(process.stdout.readline() or "{}")
            if first.get("phase") != "ready":
                raise GateError(f"the client did not connect: {_scrub(str(first))}")
            # The pod that answered goes, mid-session.
            command(
                self.kc
                + ["delete", "pod", first["pod"], "--wait=false", "--grace-period=1"]
            )
            out, err = process.communicate(timeout=self.args.start_timeout + 120)
        except subprocess.TimeoutExpired:
            process.kill()
            raise GateError("the replacement client did not finish") from None
        finally:
            watchdog.cancel()
            if process.poll() is None:
                process.kill()
        lines = [line for line in _scrub(out).splitlines() if line.strip()]
        result = json.loads(lines[-1]) if lines else {}
        ok, detail = replace_verdict(result, self.digest("notes"))
        self.report.check(
            "replace: a pod replaced mid-session reconnects: the next calls answer "
            "from the new pod with the same token, without a tool error, within "
            "the reconnect budget, waiting for the new pod's start",
            ok,
            detail + (f" stderr={_scrub(err)[-200:]}" if not ok else ""),
        )
        after = wait_for(
            "the notes connector's new pod",
            lambda: next(
                (
                    row
                    for row in self.live_pods("notes")
                    if row["id"] != before["id"] and row["ready"]
                ),
                None,
            ),
            timeout=self.args.start_timeout,
            interval=5,
        )
        old = next(
            (row for row in self.identity_rows("notes") if row["id"] == before["id"]),
            {},
        )
        service = endpoint_name(self.connectors["notes"], before["image_digest"])

        def selected() -> str | None:
            rc, out, _err = run(
                self.kc
                + ["get", "service", service, "-o"]
                + ["jsonpath={.spec.selector.srw\\.io/driver-identity}"],
                timeout=60,
            )
            return out if rc == 0 and out == after["id"] else None

        try:
            wait_for(
                "the endpoint names the new pod",
                selected,
                timeout=6 * self.reconcile_seconds + 30,
                interval=3,
            )
            moved = True
        except GateError:
            moved = False
        self.report.check(
            "replace: the endpoint Service names the new pod and the old identity "
            "is stopped (pod_lost)",
            moved and old.get("revoked") and old.get("revoke_reason") == "pod_lost",
            f"old={old.get('revoke_reason')} new={after['pod_name']}",
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
                # Each session by the account that owns it.
                api = (
                    self.other if self.thread_api.get(thread) == "other" else self.owner
                )

                def gone() -> bool:
                    status, _body = api.call(
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
            api = self.owner if self.connector_api.get(label) == "owner" else self.other

            def delete(datasource_id=datasource_id, api=api) -> bool:
                status, _body = api.call("DELETE", f"/api/datasources/{datasource_id}")
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
        if self.account_row and self.other_id:

            def account_deleted() -> bool:
                status, _body = self.owner.call("DELETE", f"/api/users/{self.other_id}")
                return status in (200, 204, 404)

            step("delete the second account's app row", account_deleted)
        if self.account_started or self.client_started:
            step(
                "delete the Keycloak account and OAuth client",
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
        if self.other_id and (
            sql(f"SELECT count(*) FROM users WHERE id = {lit(self.other_id)}") != "0"
        ):
            left.append(f"the second account's app row {self.other_id}")
        if self.account_started or self.client_started:
            try:
                counts = self.keycloak("count")
                if counts.get("users") or counts.get("clients"):
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
                self.serve_checks,
                self.credential_checks,
                self.denied_checks,
                self.workspace_checks,
                self.readonly_checks,
                self.replace_checks,
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
                            "account": self.other.username,
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
  connectors.servicePods.reconcileIntervalSeconds: 5
  connectors.servicePods.refusedCidrs: [172.16.0.0/12, 10.42.0.0/16, 10.43.0.0/16, 169.254.0.0/16]
  connectors.drivers.mcpFront.image: {repository: srw-registry:5000/srw-driver-mcp-front, tag: dev, digest: <any sha256>}
  connectors.drivers.managedMcp.gitea.enabled: true
  connectors.drivers.mcpTest.enabled: true
  connectors.drivers.mcpTest.image: {repository: srw-registry:5000/srw-driver-mcp-test, tag: dev}
Tilt overrides the shim, front and test server images (repository, tag, digest).
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
        "--gitea-url",
        default="https://gitea.com",
        help=(
            "the Gitea instance the stock image's connectors name (public, "
            "https). The gate calls no tool that reaches it; the fake token "
            "goes there only if one would."
        ),
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
        raise SafetyError("--gate-id must be d5a- followed by 10 hex digits")
    if not _MODEL_RE.fullmatch(args.model):
        raise SafetyError("model id is malformed")
    if not re.fullmatch(r"[a-z][a-z0-9._-]{0,63}", args.user):
        raise SafetyError("user name is malformed")
    if not _URL_RE.fullmatch(args.gitea_url):
        raise SafetyError("--gitea-url must be https://host[:port]")
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
    return ManagedMcpGate(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
