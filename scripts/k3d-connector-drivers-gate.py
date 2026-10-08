#!/usr/bin/env python3
"""Local k3d gate for connector drivers D1a and D1b: the contract, the control
plane and the agent's materializers.

Design: knowledge-base/knowledge/features/connector_drivers.md, Track D, D1a
and D1b.
Templates: scripts/k3d-connector-credential-residue-gate.py (C0) and
scripts/k3d-ssh-agent-connectors-gate.py (C1) -- the same safety envelope:
dry-run by default, the exact k3d-srw/srw context, secrets only on
``kubectl exec -i`` stdin and scrubbed from every printed line, and a cleanup
in ``finally`` that touches only what this run created.

Fixtures (all disposable, all named after the gate id):

  postgres   a database and a login role on srw-postgres; the role may only
             SELECT one marker table
  nextcloud  a Nextcloud user with one marker file in its WebDAV root
  gitea      a private repository (token and SSH repository connectors), a
             public repository with two Markdown notes (KB), and an access
             token of the Gitea service user (repository + user scopes)
  project    one project owned by the test account

Checks (each printed PASS/FAIL; the exit status is 0 only if all pass):

  preflight  Tilt reports the srw resource ``ok``; every orchestrator,
             stateless agent and MCP pod serves this checkout's bytes for the
             whole connector module set (every file under
             src/shared/connectors/, the drivers package and, on agents, the
             materializers in src/agent/connectors/, none extra) and the
             agent's Neo4j wrapper opens READ_ACCESS sessions
  lifecycle  per type -- postgresql, webdav, repository (token), repository
             (SSH), kb, generic, credentials, kubeconfig, generic_file and a
             host-less ssh_key -- create (nothing secret echoed), Test,
             update, delete. Test answers are the pre-D1a ones except the
             deliberate changes: kubeconfig, generic_file and a host-less
             ssh_key answer ``unsupported``. WebDAV's Test answer is printed
             as a NOTE: the orchestrator image has no webdav client
  kb         index status and reindex answer; the index reaches ``ready``
  refusals   a repository without a URL is refused (400); with
             MCP_DATASOURCES_ENABLED off an MCP create is refused (403)
  job        a stateless job in the project with Postgres linked read-only,
             a generic and a credentials connector, and an MCP row written
             straight to the table (the API refuses to create one): the
             deployed payload builder drops the MCP row and binds read-only
             SQL tools; both env variables land in ~/.srw-credentials/; the
             agent logs the read-only Postgres connection, never mentions
             the MCP row, and its audited tool list has sql_query but not
             sql_execute
  session    a stateless session in the project with WebDAV linked
             read-only: the agent logs the read-only WebDAV connection, the
             deployed builder binds read tools only (a session's audit rows
             carry no tool list), a webdav_list call returns the marker, and
             asking for a webdav_write creates no file in Nextcloud
  live       (D1b) every pooled pinned agent pod (``srw-agent-j-*``, which
             keeps its image after a Tilt rebuild while idle) serves this
             checkout's connector modules, or the phase refuses and names the
             idle ones to delete. Then a PINNED session (an Officer conference
             in the project, sandbox workspace) whose agent pod, found through
             the thread's assignment (threads.agent_id -> agents.hostname,
             never a label: a pooled pod carries none), serves this
             checkout's connector modules. A live ``config.update`` attaches the generic
             env connector, Postgres (linked read-only) and an SSH repository
             (a read deploy key on the private repository): the ack lists
             all three; the pod logs the read-only Postgres connection and
             "3 attached (3 added, 0 removed)"; the variable lands in
             ~/.srw-credentials/; the key is held by a workspace ssh-agent; the
             repository is cloned and fetches through its alias; README.md
             lists the three. A second update detaches all three: the ack
             lists them removed; the pod logs "0 attached (0 added, 3
             removed)" and closes the replaced Postgres connection after the
             turn; the key's ssh-agent is retired and the clone no longer
             fetches; README.md says no connectors are attached. A detached
             env value staying in the workspace is a NOTE (v1 keeps values).
             ``--skip-live`` runs the D1a gate alone
  cockpit    Playwright logs in as the test account, presses Test on a
             kubeconfig row and finds the neutral ``unsupported`` result
  cleanup    nothing this run created is left (rows, pods, Gitea
             repositories and token, Nextcloud user, database and role),
             including any session titled with the gate id whose create
             timed out before its id came back

Every program the gate runs in a pod caps its own memory (cap_memory, as in
the C0 and C1 gates), and the live WebSocket reader bounds its frames.

Neo4j is not deployed on k3d (the gate checks and says so); its read-only
enforcement is covered by tests/test_neo4j_read_access.py against a real
neo4j:5 container. MongoDB and email have no k3d server either.

Run with the repository venv on the k3d-srw cluster, alone: this is a
mutating gate.

  .venv/bin/python scripts/k3d-connector-drivers-gate.py           # plan
  .venv/bin/python scripts/k3d-connector-drivers-gate.py \\
      --run --confirm LOCAL-K3D-DISPOSABLE
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import secrets
import shutil
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
LOCAL_CONTEXT = "k3d-srw"
LOCAL_NAMESPACE = "srw"
LOCAL_CONFIRMATION = "LOCAL-K3D-DISPOSABLE"
K = ["kubectl", f"--context={LOCAL_CONTEXT}", "-n", LOCAL_NAMESPACE]
ORCHESTRATOR = "deploy/srw-orchestrator"
ORCHESTRATOR_CONTAINER = "orchestrator"
POSTGRES_POD = "srw-postgres-0"
AUDIT_POD = "srw-auditdb-0"
NEXTCLOUD = "deploy/srw-nextcloud"
NEXTCLOUD_CONTAINER = "nextcloud"
WORKSPACE_CONTAINER = "workspace"
AGENT_CONTAINER = "agent"
PINNED_THREAD_LABEL = "srw.io/thread-id"
#: A pinned pool's pods: an idle one is reused for a new pinned thread and
#: keeps the image it was created with after a Tilt rebuild.
POOLED_PINNED_PREFIX = "srw-agent-j-"
_POD_NAME_RE = re.compile(r"[a-z0-9]([-a-z0-9]{0,251}[a-z0-9])?\Z")
AGENT_PORT = 8001
HOME = "/home/agent-host"
KEYCLOAK_TOKEN_URL = "http://srw-keycloak:8080/realms/srw/protocol/openid-connect/token"
POD_ROOT = "/app"
PG_HOST = "srw-postgres"
DAV_BASE = "http://srw-nextcloud/remote.php/dav/files"
COCKPIT_URL = "https://localhost/datasources"
DEFAULT_MODEL = "muse-spark-1.3-contributor"
_SELECTOR = (
    "app.kubernetes.io/instance=srw,app.kubernetes.io/name=superhuman-remote-worker"
)
_GATE_ID_RE = re.compile(r"d1a-[0-9a-f]{10}\Z")
_MODEL_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@-]{0,199}\Z")

# The connector module set each image must serve byte for byte. A directory
# is compared whole: every checkout file must match and the pod may hold no
# file the checkout lacks (a stale ``legacy.py`` from an older image).
SHARED_CONNECTORS = "src/shared/connectors"
DRIVERS = "src/orchestrator/services/connector_drivers"
AGENT_CONNECTORS = "src/agent/connectors"
NEO4J_DB = "src/shared/runtime/database/neo4j_db.py"
_SHARED_FILES = (
    "src/shared/credential_connectors.py",
    "src/shared/datasource_policy.py",
)


@dataclass(frozen=True)
class ServedSet:
    label: str
    component: str
    container: str
    dirs: tuple[str, ...]
    files: tuple[str, ...]
    contains: tuple[tuple[str, str], ...] = ()


# The agent side: the materializers and every entry point that runs them.
_AGENT_FILES = (
    *_SHARED_FILES,
    NEO4J_DB,
    "src/shared/runtime/core/datasource_catalog.py",
    "src/agent/core/datasource_setup.py",
    "src/agent/tools/graph/neo4j.py",
    "src/agent/agent.py",
    "src/agent/api/session_attach.py",
    "src/agent/api/persistent_session.py",
    "src/agent/api/persistent_app.py",
)

SERVED_SETS = (
    ServedSet(
        "orchestrator",
        "orchestrator",
        "orchestrator",
        (SHARED_CONNECTORS, DRIVERS),
        (
            *_SHARED_FILES,
            NEO4J_DB,
            "src/shared/orch_surface/formatters.py",
            "src/shared/runtime/core/datasource_catalog.py",
            "src/orchestrator/application/__init__.py",
            "src/orchestrator/application/catalogue.py",
            "src/orchestrator/application/controls.py",
            "src/orchestrator/application/preparation.py",
            "src/orchestrator/application/projects.py",
            "src/orchestrator/application/resources.py",
            "src/orchestrator/security/credential_files.py",
            "src/orchestrator/services/agent_datasource_payload.py",
            "src/orchestrator/services/datasources.py",
            "src/orchestrator/services/datasource_config.py",
            "src/orchestrator/services/deployment_gates.py",
            "src/orchestrator/services/job_control_delivery.py",
            "src/orchestrator/services/job_start_bundle.py",
            "src/orchestrator/services/manifest_execution.py",
            "src/orchestrator/services/thread_mount_rows.py",
            "src/orchestrator/services/workspace_ssh_connector.py",
        ),
        ((NEO4J_DB, "READ_ACCESS"),),
    ),
    ServedSet(
        "stateless agent",
        "agent-stateless",
        AGENT_CONTAINER,
        (SHARED_CONNECTORS, AGENT_CONNECTORS),
        _AGENT_FILES,
        ((NEO4J_DB, "READ_ACCESS"),),
    ),
    ServedSet(
        "mcp",
        "mcp",
        "mcp",
        (SHARED_CONNECTORS,),
        (
            *_SHARED_FILES,
            "src/shared/orch_surface/formatters.py",
            "src/mcp_server/server.py",
        ),
    ),
)

# A pinned session's own agent pod (no component label; checked in ``live``).
PINNED_AGENT = ServedSet(
    "pinned agent",
    "",
    AGENT_CONTAINER,
    (SHARED_CONNECTORS, AGENT_CONNECTORS),
    _AGENT_FILES,
    ((NEO4J_DB, "READ_ACCESS"),),
)

_SECRETS: list[str] = []


class GateError(RuntimeError):
    """Infrastructure trouble: the gate could not observe the product."""


class SafetyError(RuntimeError):
    """The requested run is outside the local disposable boundary."""


def _scrub(text: str) -> str:
    for secret in _SECRETS:
        if secret:
            text = text.replace(secret, "<redacted>")
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


def sql(query: str, *, database: str = "srw") -> str:
    """One statement on the app database (no secret may be in ``query``)."""
    return command(
        K
        + ["exec", POSTGRES_POD, "--", "psql", "-U", "srw", "-d", database]
        + ["-v", "ON_ERROR_STOP=1", "-tAc", query]
    )


def sql_script(script: str, *, database: str = "srw") -> str:
    """A script on stdin, so a password in it never becomes an argument."""
    return command(
        K
        + ["exec", "-i", POSTGRES_POD, "--", "psql", "-U", "srw", "-d", database]
        + ["-v", "ON_ERROR_STOP=1", "-tAq", "-f", "-"],
        data=script,
    )


def audit_sql(query: str) -> str:
    return command(
        K
        + ["exec", AUDIT_POD, "--", "psql", "-U", "srw", "-d", "srw_audit"]
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
# Programs run inside the orchestrator container (stdin carries every secret)
# ---------------------------------------------------------------------------

# Every program the gate runs inside a pod starts with this and calls
# cap_memory() once its imports are done. The orchestrator pod has a 1 GiB
# limit and serves the product meanwhile: a gate program that grows must
# fail the gate with a MemoryError, never take the pod to the OOM killer.
# RLIMIT_DATA counts heap, anonymous maps and thread stacks, not the shared
# libraries mapped in, so the budget is what the program adds after import.
# (The same helper as scripts/k3d-ssh-agent-connectors-gate.py.)
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
#: The largest WebSocket frame the live-update reader accepts.
WS_MAX_FRAME = 8 << 20

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

_GITEA_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import asyncio, json, sys
from orchestrator.services.gitea import GiteaClient
cap_memory()
request = json.loads(sys.stdin.readline())
async def main():
    client = GiteaClient()
    if not await client.ensure_initialized():
        raise SystemExit("gitea is not initialized")
    http, base, owner = client._get_client(), client._url, client.repository_owner
    out = {}
    try:
        if request["action"] == "setup":
            host, port = client._ssh_internal_endpoint()
            out.update(owner=owner, url=base, ssh_host=host, ssh_port=port)
            for repo, marker in request["repos"].items():
                if not await client.create_repo(repo, intent_marker=marker):
                    raise SystemExit("repository creation failed")
            kb = request["kb"]
            response = await http.patch(
                f"{base}/api/v1/repos/{owner}/{kb['repo']}", json={"private": False}
            )
            if response.status_code != 200:
                raise SystemExit(f"kb repository stayed private ({response.status_code})")
            for path, content in kb["notes"].items():
                if not await client.create_or_update_file(
                    kb["repo"], path, content, "D1a gate note"
                ):
                    raise SystemExit("kb note write failed")
            response = await http.post(
                f"{base}/api/v1/users/{owner}/tokens",
                json={"name": request["token_name"], "scopes": request["scopes"]},
            )
            if response.status_code != 201:
                raise SystemExit(f"token creation failed ({response.status_code})")
            out["token"] = response.json()["sha1"]
        elif request["action"] == "deploy_key":
            key_id = await client.ensure_repo_deploy_key(
                request["repo"], title=request["title"],
                public_key=request["public_key"], access_mode=request["access_mode"],
            )
            if key_id is None:
                raise SystemExit("deploy key registration failed")
            out["key_id"] = key_id
        elif request["action"] == "cleanup":
            out["deleted"] = [
                repo for repo, marker in request["repos"].items()
                if await client.delete_repo(repo, intent_marker=marker)
            ]
            response = await http.delete(
                f"{base}/api/v1/users/{owner}/tokens/{request['token_name']}"
            )
            out["token_status"] = response.status_code
        elif request["action"] == "residue":
            remaining = []
            for repo in request["repos"]:
                response = await http.get(f"{base}/api/v1/repos/{owner}/{repo}")
                if response.status_code != 404:
                    remaining.append(repo)
            response = await http.get(f"{base}/api/v1/users/{owner}/tokens")
            names = [t.get("name") for t in response.json()] if response.status_code == 200 else None
            out["remaining"] = remaining
            out["token_left"] = names is None or request["token_name"] in names
    finally:
        await client.close()
    print(json.dumps(out))
asyncio.run(main())
"""
)

# A cockpit-like WebSocket on a pinned session's agent pod sends one live
# ``config.update`` (the settings pane's frame) and prints the answer with the
# same request id: ``config.changed`` or ``error``. Names only come back.
_LIVE_UPDATE_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import asyncio, json, sys, urllib.error, urllib.parse, urllib.request
import websockets
cap_memory()
r = json.loads(sys.stdin.readline())
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
# A session still attaching answers /connection with 409 or 425, or closes
# the socket with 4500 or 4503: the gate retries those, nothing else.
RETRY_STATUS = (409, 425)
RETRY_CLOSE = (4500, 4503)
def session_token():
    form = urllib.parse.urlencode({
        "grant_type": "password", "client_id": "admin-cli", "scope": "openid",
        "username": r["username"], "password": r["password"],
    }).encode()
    with opener.open(r["token_url"], data=form, timeout=30) as response:
        bearer = json.load(response)["id_token"]
    request = urllib.request.Request(
        "http://localhost:8085/api/sessions/%s/connection" % r["thread"],
        headers={"Authorization": "Bearer " + bearer},
    )
    with opener.open(request, timeout=60) as response:
        return json.load(response)["token"]
def retry(reason):
    print(json.dumps({"outcome": "retry", "reason": reason}))
async def update():
    try:
        token = await asyncio.to_thread(session_token)
    except urllib.error.HTTPError as error:
        if error.code in RETRY_STATUS:
            return retry("connection %d" % error.code)
        raise
    url = "ws://%s:%d/p/%s/ws?t=%s" % (r["ip"], r["port"], r["thread"], token)
    loop = asyncio.get_running_loop()
    async with websockets.connect(
        url, max_size=r["max_size"], open_timeout=30
    ) as ws:
        await ws.send(json.dumps({
            "method": "config.update", "config": {},
            "datasource_ids": r["datasource_ids"], "request_id": r["request_id"],
        }))
        deadline = loop.time() + r["timeout"]
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                print(json.dumps({"outcome": "timeout"}))
                return
            try:
                raw = await asyncio.wait_for(ws.recv(), remaining)
            except asyncio.TimeoutError:
                continue
            try:
                frame = json.loads(raw)
            except ValueError:
                continue
            params = frame.get("params") if isinstance(frame, dict) else None
            if not isinstance(params, dict):
                continue
            if params.get("request_id") != r["request_id"]:
                continue
            if frame.get("method") in ("config.changed", "error"):
                print(json.dumps({
                    "outcome": frame["method"],
                    "datasources": params.get("datasources"),
                    "message": params.get("message"),
                    "detail": params.get("detail"),
                }))
                return
async def main():
    try:
        await update()
    except websockets.exceptions.ConnectionClosed as error:
        received = getattr(error, "rcvd", None)
        code = getattr(received, "code", None) or getattr(error, "code", None)
        if code in RETRY_CLOSE:
            return retry("closed %s" % code)
        raise
    except websockets.exceptions.InvalidHandshake as error:
        response = getattr(error, "response", None)
        status = getattr(error, "status_code", None) or getattr(
            response, "status_code", None
        )
        if status in RETRY_STATUS or status == 503:
            return retry("handshake %s" % status)
        raise
asyncio.run(main())
"""
)

_DAV_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import base64, json, sys, urllib.error, urllib.request
cap_memory()
r = json.loads(sys.stdin.readline())
auth = base64.b64encode(f"{r['user']}:{r['password']}".encode()).decode()
body = r.get("body")
request = urllib.request.Request(
    r["url"], data=None if body is None else body.encode(), method=r["method"],
    headers={"Authorization": "Basic " + auth, "Content-Type": "text/plain"},
)
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
try:
    with opener.open(request, timeout=60) as response:
        status = response.status
except urllib.error.HTTPError as error:
    status = error.code
print(json.dumps({"status": status}))
"""
)

# The deployed payload builder over a job's or a session's exactly resolved
# connectors, with the deployment's own gates. It prints types, names and
# tool lists only: the resolved rows carry decrypted credentials.
_PAYLOAD_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import asyncio, json, sys
from types import SimpleNamespace
from orchestrator.application.preparation import datasource_payload_dependencies
from orchestrator.application.settings import DeploymentSettings
from orchestrator.database.postgres import PostgresDB
from orchestrator.services import deployment_gates
from orchestrator.services.agent_datasource_payload import (
    build_datasource_tool_override, build_datasources_payload,
)
from orchestrator.services.connector_drivers.registry import builtin_connector_drivers
cap_memory()
request = json.loads(sys.stdin.readline())
async def main():
    db = PostgresDB(
        min_connections=1, max_connections=1,
        server_settings={"default_transaction_read_only": "on"},
    )
    await db.connect()
    try:
        if request.get("job"):
            rows = await db.resolve_datasources_for_job(
                request["job"], request["project"]
            )
        else:
            rows = await db.resolve_datasources_for_thread(
                request["datasource_ids"], [request["project"]]
            )
    finally:
        await db.close()
    # The application's own wiring, over the two resources it reads.
    deps = datasource_payload_dependencies(
        SimpleNamespace(
            connector_drivers=builtin_connector_drivers(),
            settings=DeploymentSettings.from_environment(),
        )
    )
    payload = build_datasources_payload(rows, dependencies=deps) or []
    tools = build_datasource_tool_override(rows, {}, dependencies=deps)["tools"]
    print(json.dumps({
        "mcp_gate": bool(deployment_gates.mcp_datasources_enabled()),
        "resolved": sorted(f"{r['type']}:{r['name']}" for r in rows),
        "read_only": sorted(r["name"] for r in rows if r.get("project_read_only")),
        "payload": sorted(f"{e.get('type')}:{e.get('name')}" for e in payload),
        "tools": {k: v for k, v in tools.items() if v},
    }))
asyncio.run(main())
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
missing = sorted(
    f"{path}:{text}" for path, text in request["contains"]
    if not (root / path).is_file() or text.encode() not in (root / path).read_bytes()
)
print(json.dumps({"stale": stale, "extra": extra, "missing_text": missing}))
"""
)


def in_orchestrator(
    program: str, payload: dict[str, Any], *, timeout: int = 180
) -> dict:
    out = command(
        K
        + ["exec", "-i", ORCHESTRATOR, "-c", ORCHESTRATOR_CONTAINER, "--"]
        + ["python", "-c", program],
        data=json.dumps(payload) + "\n",
        timeout=timeout,
    )
    return json.loads(out.splitlines()[-1])


class Api:
    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self.password = secret(password)

    def call(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        result = in_orchestrator(
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


# ---------------------------------------------------------------------------
# Keys and expected bytes
# ---------------------------------------------------------------------------


def make_key() -> str:
    """An unencrypted ed25519 OpenSSH private key, registered for scrubbing."""
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives.serialization import (
        Encoding,
        NoEncryption,
        PrivateFormat,
    )

    text = (
        Ed25519PrivateKey.generate()
        .private_bytes(Encoding.PEM, PrivateFormat.OpenSSH, NoEncryption())
        .decode()
    )
    secret(text)
    _SECRETS.extend(line for line in text.splitlines()[1:-1] if len(line) > 20)
    return text


def make_key_pair() -> tuple[str, str, str]:
    """``(private key, public key line, SHA256 fingerprint)``; private scrubbed."""
    import base64

    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives.serialization import (
        Encoding,
        NoEncryption,
        PrivateFormat,
        PublicFormat,
    )

    key = Ed25519PrivateKey.generate()
    private = key.private_bytes(
        Encoding.PEM, PrivateFormat.OpenSSH, NoEncryption()
    ).decode()
    secret(private)
    _SECRETS.extend(line for line in private.splitlines()[1:-1] if len(line) > 20)
    public = key.public_key().public_bytes(Encoding.OpenSSH, PublicFormat.OpenSSH)
    digest = hashlib.sha256(base64.b64decode(public.split()[1])).digest()
    fingerprint = "SHA256:" + base64.b64encode(digest).decode().rstrip("=")
    return private, public.decode() + " srw-d1b-gate", fingerprint


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


# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------


@dataclass
class Report:
    gate_id: str
    results: list[tuple[str, bool, str]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def check(self, name: str, ok: bool, detail: str = "") -> bool:
        detail = _scrub(detail)
        self.results.append((name, bool(ok), detail))
        print(
            f"{'PASS' if ok else 'FAIL'} {name}{': ' + detail if detail else ''}",
            flush=True,
        )
        return bool(ok)

    def note(self, text: str) -> None:
        text = _scrub(text)
        self.notes.append(text)
        print(f"NOTE {text}", flush=True)

    @property
    def passed(self) -> bool:
        return bool(self.results) and all(ok for _, ok, _ in self.results)


PLAN = [
    "preflight: Tilt srw ok; every orchestrator, stateless agent and MCP pod "
    "serves this checkout's connector modules (whole set, none extra) and "
    "READ_ACCESS Neo4j sessions",
    "fixture: Postgres database + SELECT-only role, Nextcloud user + marker "
    "file, Gitea repos (private + public KB with notes) + scoped token, project",
    "lifecycle: create / Test / update / delete for postgresql, webdav, "
    "repository (token), repository (SSH, pinned on update), kb, generic, "
    "credentials, kubeconfig, generic_file, host-less ssh_key",
    "lifecycle: kubeconfig, generic_file and a host-less ssh_key Test as "
    "unsupported; WebDAV Test reported as a NOTE",
    "kb: index status and reindex answer; the index reaches ready",
    "refusals: repository without URL (400); MCP create with the gate off (403)",
    "job: stateless, Postgres read-only via the project + generic + "
    "credentials + an MCP row; payload drops MCP; sql_query without "
    "sql_execute; env vars in ~/.srw-credentials/",
    "session: stateless, WebDAV read-only via the project; read-only "
    "connection logged; read tools only; webdav_list returns the marker; "
    "a requested write creates nothing",
    "live (D1b): every pooled pinned agent pod (srw-agent-j-*) serves this "
    "checkout's materializers, or the phase refuses and names the idle ones to "
    "delete; a pinned Officer-conference session; the pod its assignment names "
    "(threads.agent_id -> agents.hostname, never a label) serves this "
    "checkout's materializers; a live config.update attaches the generic env "
    "connector, Postgres (read-only link) and an SSH repository (read deploy "
    "key), then a second one detaches all three: acks, pod logs, "
    "~/.srw-credentials/, the ssh-agent, the clone and README.md each way "
    "(--skip-live skips it)",
    "cockpit: Playwright presses Test on a kubeconfig row; result is unsupported",
    "notes: Neo4j not deployed on k3d (covered by the real-container test)",
    "cleanup: end + delete sessions (also any titled with the gate id), "
    "cancel + delete job, delete connectors, project, Gitea repos + token, "
    "Nextcloud user, database + role; residue check",
]

# Attached live to the pinned session, then detached: env, managed, SSH.
LIVE_LABELS = ("attach-generic", "attach-pg", "live-ssh")

JOB_TERMINAL = frozenset({"completed", "failed", "cancelled"})
JOB_RESTING = JOB_TERMINAL | {"pending_review", "paused", "waiting"}

# Deliberate D1a changes are the ``unsupported`` answers; every other answer
# is the one these connectors gave before the drivers existed.
UNSUPPORTED = {
    "kubeconfig": "Kubeconfig connectors have no connection test",
    "generic_file": "Generic file connectors have no connection test",
    "ssh_key": "SSH Key connectors without a host have no endpoint to test",
}


@dataclass(frozen=True)
class TypeCase:
    label: str
    body: dict[str, Any]
    expect_status: str | None  # None: report the answer as a NOTE only
    expect_text: tuple[str, ...] = ()


class ConnectorDriversGate:
    #: What a pinned agent pod must serve (a gate for a later slice extends it).
    pinned_served: ServedSet = PINNED_AGENT

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.gate_id = args.gate_id or f"d1a-{secrets.token_hex(5)}"
        self.suffix = self.gate_id.split("-", 1)[1]
        self.report = Report(self.gate_id)
        self.api = Api(args.user, args.password)
        self.started = (datetime.now(timezone.utc) - timedelta(seconds=30)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        # Everything this run creates, recorded before it is created.
        self.connectors: dict[str, str] = {}  # label -> datasource id
        self.sql_rows: dict[str, str] = {}  # label -> id written by SQL
        self.repos: dict[str, str] = {}  # Gitea repo -> creation intent marker
        self.token_name = f"srw-{self.gate_id}"
        self.pg_name = f"d1a_{self.suffix}"
        self.pg_started = False
        self.nc_user = self.gate_id
        self.nc_started = False
        self.project: str | None = None
        self.job: str | None = None
        self.thread: str | None = None
        self.live_thread: str | None = None
        self.live_fingerprint = ""
        self.user_id = ""
        self.gitea: dict[str, Any] = {}
        self.pg_password = secret(secrets.token_hex(16))
        self.nc_password = secret(f"D1a-{secrets.token_urlsafe(18)}-x9")
        self.marker = f"d1a-marker-{self.suffix}"
        self.dav_file = f"{self.gate_id}-marker.txt"
        self.dav_probe = f"{self.gate_id}-probe.txt"
        self.env = {
            "generic": (
                f"D1A_GENERIC_{self.suffix.upper()}",
                secret(secrets.token_hex(12)),
            ),
            "credentials": (
                f"D1A_CRED_{self.suffix.upper()}",
                secret(secrets.token_hex(12)),
            ),
        }

    # -- naming ------------------------------------------------------------
    def name(self, label: str) -> str:
        return f"{self.gate_id} {label}"

    @property
    def repo(self) -> str:
        return f"srw-{self.gate_id}-repo"

    @property
    def kb_repo(self) -> str:
        return f"srw-{self.gate_id}-kb"

    @property
    def pg_url(self) -> str:
        return (
            f"postgresql://{self.pg_name}:{self.pg_password}@{PG_HOST}:5432/"
            f"{self.pg_name}"
        )

    @property
    def dav_url(self) -> str:
        return f"{DAV_BASE}/{self.nc_user}/"

    # -- cluster helpers ---------------------------------------------------
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

    def workspace_pod(self, selector: str, *, timeout: int = 300) -> str:
        def probe() -> str | None:
            pods = json.loads(
                command(K + ["get", "pods", "-l", selector, "-o", "json"])
            )["items"]
            running = [
                pod["metadata"]["name"]
                for pod in pods
                if pod.get("status", {}).get("phase") == "Running"
                and not pod["metadata"].get("deletionTimestamp")
            ]
            return running[0] if len(running) == 1 else None

        return wait_for(f"workspace pod {selector}", probe, timeout=timeout)

    def workspace_grep(self, pod: str, needle: str, path: str) -> list[str]:
        """Files under ``path`` holding ``needle`` (sent on stdin)."""
        rc, out, _err = run(
            K
            + ["exec", "-i", pod, "-c", WORKSPACE_CONTAINER, "--"]
            + ["grep", "-rlF", "-f", "-", path],
            data=needle + "\n",
            timeout=60,
        )
        return [line for line in out.splitlines() if line] if rc == 0 else []

    def nextcloud_occ(self, arguments: str, *, data: str | None = None) -> int:
        """``occ`` as www-data; ``arguments`` holds no secret."""
        rc, _out, _err = run(
            K
            + ["exec", "-i", NEXTCLOUD, "-c", NEXTCLOUD_CONTAINER, "--"]
            + ["su", "-s", "/bin/sh", "www-data", "-c"]
            + [
                ("IFS= read -r OC_PASS; export OC_PASS; " if data is not None else "")
                + f"exec php /var/www/html/occ {arguments}"
            ],
            data=data,
            timeout=120,
        )
        return rc

    def agent_log_lines(self, needles: list[str]) -> list[str]:
        """Stateless agent log lines since the gate started holding a needle."""
        found: list[str] = []
        for pod in self.pods("agent-stateless"):
            rc, out, _err = run(
                K
                + ["logs", pod["metadata"]["name"], "-c", "agent"]
                + [f"--since-time={self.started}"],
                timeout=120,
            )
            if rc == 0:
                found += [
                    line for line in out.splitlines() if any(n in line for n in needles)
                ]
        return found

    def audited_tools(self, unit: str) -> set[str]:
        names = audit_sql(
            "SELECT coalesce(string_agg(DISTINCT t->'function'->>'name', ',' "
            "ORDER BY t->'function'->>'name'), '') FROM llm_requests r, "
            "jsonb_array_elements(CASE WHEN jsonb_typeof(r.request->'tools') = "
            "'array' THEN r.request->'tools' ELSE '[]'::jsonb END) t "
            f"WHERE r.job_id::text = {lit(unit)} AND r.call_type = 'main'"
        )
        return {name for name in names.split(",") if name}

    def wait_audited_tools(self, unit: str) -> set[str]:
        """The unit's audited tool names; empty when none were audited."""
        try:
            return wait_for(
                f"audited tools of {unit}",
                lambda: self.audited_tools(unit),
                timeout=120,
                interval=10,
            )
        except GateError:
            return set()

    def queue(self, thread: str) -> tuple[str, int, int] | None:
        row = sql(
            "SELECT state || ' ' || coalesce(input_seq, 0) || ' ' || "
            f"coalesce(consumed_seq, 0) FROM run_queue WHERE unit_id = {lit(thread)}"
        )
        if not row:
            return None
        state, input_seq, consumed_seq = row.split()
        if state == "parked":
            raise GateError("the session unit parked")
        return state, int(input_seq), int(consumed_seq)

    def turn(self, text: str, step: int) -> None:
        before = self.queue(self.thread)
        previous = before[1] if before else 0
        self.api.ok(
            "POST", f"/api/persistent/threads/{self.thread}/input", {"content": text}
        )

        def answered() -> bool:
            current = self.queue(self.thread)
            return bool(
                current
                and current[0] == "done"
                and current[1] > previous
                and current[1] == current[2]
            )

        wait_for(f"turn {step} answered", answered, timeout=self.args.turn_timeout)

    def served_problems(self, pod: str, served: ServedSet) -> list[str]:
        """How ``pod`` differs from this checkout for ``served`` (empty: same)."""
        found = json.loads(
            command(
                K
                + ["exec", "-i", pod, "-c", served.container, "--"]
                + ["python", "-c", _HASH_PROGRAM, POD_ROOT],
                data=json.dumps(
                    {
                        "files": expected_bytes(served),
                        "dirs": list(served.dirs),
                        "contains": [list(pair) for pair in served.contains],
                    }
                ),
            ).splitlines()[-1]
        )
        return [
            f"{pod} {kind}: {found[kind][:6]}"
            for kind in ("stale", "extra", "missing_text")
            if found.get(kind)
        ]

    def ws(self, pod: str, script: str, *, check: bool = True) -> tuple[int, str]:
        """Run ``script`` as agent-host in the workspace (stdin, never argv)."""
        rc, out, err = run(
            K
            + ["exec", "-i", pod, "-c", WORKSPACE_CONTAINER, "--"]
            + ["su", "-s", "/bin/bash", "agent-host", "-c", "bash -s"],
            data="set -u\ncd ~\n" + script,
            timeout=120,
        )
        if check and rc:
            raise GateError(f"workspace command failed (exit {rc}): {err[-300:]}")
        return rc, out

    def create_connector(self, label: str, body: dict[str, Any]) -> tuple[int, Any]:
        """POST a connector; its id is recorded before the status is read."""
        status, parsed = self.api.call(
            "POST",
            "/api/datasources",
            {"name": self.name(label), "scope_mode": "all", **body},
        )
        if isinstance(parsed, dict) and parsed.get("id"):
            self.connectors[label] = str(parsed["id"])
        return status, parsed

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
        problems: list[str] = []
        for served in SERVED_SETS:
            pods = self.pods(served.component)
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
            "preflight: every orchestrator, stateless agent and MCP pod serves "
            "this checkout's connector modules",
            not problems,
            "; ".join(problems)[:600],
        )
        if problems:
            raise GateError("the deployment does not serve this checkout")
        self.user_id = sql(
            f"SELECT id FROM users WHERE preferred_username = {lit(self.args.user)}"
        )
        if not re.fullmatch(r"[0-9a-f-]{36}", self.user_id):
            raise GateError(f"no user {self.args.user!r}")

    def fixture(self) -> None:
        # Postgres: a disposable database and a role that may only SELECT.
        self.pg_started = True
        sql_script(
            f"CREATE ROLE {self.pg_name} LOGIN PASSWORD '{self.pg_password}' "
            "NOSUPERUSER NOCREATEDB NOCREATEROLE CONNECTION LIMIT 20;\n"
            f"CREATE DATABASE {self.pg_name} OWNER srw;\n"
            f"REVOKE ALL ON DATABASE {self.pg_name} FROM PUBLIC;\n"
            f"GRANT CONNECT ON DATABASE {self.pg_name} TO {self.pg_name};\n"
        )
        sql_script(
            "CREATE TABLE d1a_marker (note text NOT NULL);\n"
            f"INSERT INTO d1a_marker VALUES ('{self.marker}');\n"
            "REVOKE CREATE ON SCHEMA public FROM PUBLIC;\n"
            f"GRANT USAGE ON SCHEMA public TO {self.pg_name};\n"
            f"GRANT SELECT ON d1a_marker TO {self.pg_name};\n",
            database=self.pg_name,
        )
        # Nextcloud: a user whose WebDAV root holds one marker file.
        self.nc_started = True
        if self.nextcloud_occ(
            f"user:add --password-from-env --display-name={self.gate_id} {self.nc_user}",
            data=self.nc_password + "\n",
        ):
            raise GateError("Nextcloud user creation failed")
        put = in_orchestrator(
            _DAV_PROGRAM,
            {
                "method": "PUT",
                "url": self.dav_url + self.dav_file,
                "user": self.nc_user,
                "password": self.nc_password,
                "body": f"{self.marker}\n",
            },
        )
        if put.get("status") not in (201, 204):
            raise GateError(f"WebDAV marker upload answered {put.get('status')}")
        # Gitea: a private repository, a public KB repository, a token.
        self.repos = {self.repo: str(uuid.uuid4()), self.kb_repo: str(uuid.uuid4())}
        self.gitea = in_orchestrator(
            _GITEA_PROGRAM,
            {
                "action": "setup",
                "repos": self.repos,
                "kb": {
                    "repo": self.kb_repo,
                    "notes": {
                        "notes/alpha.md": f"# Alpha\n\nD1a gate note {self.marker}.\n",
                        "notes/beta.md": "# Beta\n\nSee [[alpha]].\n",
                    },
                },
                "token_name": self.token_name,
                "scopes": ["read:repository", "write:repository", "read:user"],
            },
        )
        self.gitea["token"] = secret(self.gitea["token"])
        # The project the job and the session run in.
        created = self.api.ok(
            "POST",
            "/api/projects",
            {
                "name": self.name("project"),
                "description": "D1a connector drivers gate (disposable)",
                "user_id": self.user_id,
            },
        )
        self.project = str(created["id"])
        print(
            f"fixture: database {self.pg_name}, Nextcloud user {self.nc_user}, "
            f"Gitea {self.gitea['url']} repos {sorted(self.repos)}, "
            f"project {self.project}",
            flush=True,
        )

    def type_cases(self) -> list[TypeCase]:
        owner, base = self.gitea["owner"], self.gitea["url"]
        host, port = self.gitea["ssh_host"], self.gitea["ssh_port"]
        generic_name, generic_value = (
            f"D1A_LC_{self.suffix.upper()}",
            secret(secrets.token_hex(12)),
        )
        cred_name, cred_value = (
            f"D1A_LCC_{self.suffix.upper()}",
            secret(secrets.token_hex(12)),
        )
        return [
            TypeCase(
                "postgresql",
                {"type": "postgresql", "connection_url": self.pg_url},
                "ok",
                ("Connected: PostgreSQL",),
            ),
            TypeCase(
                "webdav",
                {
                    "type": "webdav",
                    "connection_url": self.dav_url,
                    "credentials": {
                        "username": self.nc_user,
                        "password": self.nc_password,
                    },
                },
                None,
            ),
            TypeCase(
                "repository-token",
                {
                    "type": "repository",
                    "connection_url": f"{base}/{owner}/{self.repo}.git",
                    "config": {"forge": "gitea"},
                    "credentials": {
                        "auth_method": "token",
                        "token": self.gitea["token"],
                    },
                },
                "ok",
                ("Authenticated as", f"access to {owner}/{self.repo}"),
            ),
            TypeCase(
                "repository-ssh",
                {
                    "type": "repository",
                    "connection_url": f"ssh://git@{host}:{port}/{owner}/{self.repo}.git",
                    "config": {"forge": "gitea"},
                    "credentials": {"auth_method": "ssh", "ssh_key": make_key()},
                },
                "ok",
                (f"Reached {host}:{port}", "is not pinned"),
            ),
            TypeCase(
                "kb",
                {
                    "type": "kb",
                    "connection_url": f"{base}/{owner}/{self.kb_repo}.git",
                    "default_branch": "main",
                    "config": {"root_path": "notes", "forge": "gitea"},
                },
                "ok",
                ("Found 2 Markdown note(s)",),
            ),
            TypeCase(
                "generic",
                {
                    "type": "generic",
                    "connection_url": "https://d1a-gate.invalid/api",
                    "cli_hint": "D1a gate env connector",
                    "credentials": {"env_vars": {generic_name: generic_value}},
                },
                "ok",
                ("No connectivity test for generic connectors",),
            ),
            TypeCase(
                "credentials",
                {
                    "type": "credentials",
                    "credentials": {"env_vars": {cred_name: cred_value}},
                },
                "ok",
                ("Credential variables are valid",),
            ),
            TypeCase(
                "kubeconfig",
                {
                    "type": "kubeconfig",
                    "credentials": {
                        "files": [{"contents": "apiVersion: v1\nkind: Config\n"}]
                    },
                },
                "unsupported",
                (UNSUPPORTED["kubeconfig"],),
            ),
            TypeCase(
                "generic_file",
                {
                    "type": "generic_file",
                    "credentials": {
                        "files": [
                            {
                                "contents": '{"gate": "d1a"}',
                                "target_path": f"~/.config/d1a/{self.suffix}.json",
                                "env_var": f"D1A_FILE_{self.suffix.upper()}",
                            }
                        ]
                    },
                },
                "unsupported",
                (UNSUPPORTED["generic_file"],),
            ),
            TypeCase(
                "ssh_key",
                {
                    "type": "ssh_key",
                    "credentials": {"files": [{"contents": make_key()}]},
                },
                "unsupported",
                (UNSUPPORTED["ssh_key"],),
            ),
        ]

    def lifecycle(self) -> None:
        for case in self.type_cases():
            label = f"lc-{case.label}"
            status, created = self.create_connector(label, case.body)
            echoed = "<redacted>" in json.dumps(created)
            if not self.report.check(
                f"lifecycle {case.label}: create",
                status in (200, 201) and label in self.connectors and not echoed,
                f"HTTP {status}"
                + ("; a secret was echoed" if echoed else "")
                + ("" if status in (200, 201) else f"; {str(created)[:200]}"),
            ):
                continue
            ds = self.connectors[label]
            status, tested = self.api.call("POST", f"/api/datasources/{ds}/test")
            answer = tested.get("status") if isinstance(tested, dict) else None
            message = str(tested.get("message", "")) if isinstance(tested, dict) else ""
            if case.expect_status is None:
                self.report.note(
                    f"lifecycle {case.label}: Test answered HTTP {status} "
                    f"{answer!r}: {message[:160]} (the orchestrator image has no "
                    "webdav client; the agent side is checked in the session)"
                )
            else:
                self.report.check(
                    f"lifecycle {case.label}: Test answers {case.expect_status}",
                    status == 200
                    and answer == case.expect_status
                    and all(text in message for text in case.expect_text),
                    f"HTTP {status} {answer!r}: {message[:200]}",
                )
            self.update(case, ds, tested if isinstance(tested, dict) else {})
            if case.label == "kb":
                self.kb_index(ds)
            status, _body = self.api.call("DELETE", f"/api/datasources/{ds}")
            gone, _after = self.api.call("GET", f"/api/datasources/{ds}")
            if status == 200 and gone == 404:
                self.connectors.pop(label, None)
            self.report.check(
                f"lifecycle {case.label}: delete",
                status == 200 and gone == 404,
                f"DELETE HTTP {status}; GET after HTTP {gone}",
            )

    def update(self, case: TypeCase, ds: str, tested: dict[str, Any]) -> None:
        if case.label == "repository-ssh":
            host_key = (tested.get("details") or {}).get("host_key")
            status, _body = self.api.call(
                "PUT",
                f"/api/datasources/{ds}",
                {"config": {"forge": "gitea", "known_hosts": host_key or ""}},
            )
            _s, retested = self.api.call("POST", f"/api/datasources/{ds}/test")
            self.report.check(
                "lifecycle repository-ssh: update pins the host key Test reported",
                bool(host_key)
                and status == 200
                and retested.get("status") == "ok"
                and "matches the pin" in str(retested.get("message", "")),
                f"PUT HTTP {status}; {str(retested.get('message', ''))[:160]}",
            )
            return
        description = f"D1a gate {case.label}: updated"
        status, body = self.api.call(
            "PUT", f"/api/datasources/{ds}", {"description": description}
        )
        _s, after = self.api.call("GET", f"/api/datasources/{ds}")
        self.report.check(
            f"lifecycle {case.label}: update",
            status == 200
            and isinstance(after, dict)
            and after.get("description") == description
            and "<redacted>" not in json.dumps(body),
            f"PUT HTTP {status}",
        )

    def kb_index(self, ds: str) -> None:
        status, body = self.api.call("GET", f"/api/datasources/{ds}/index-status")
        self.report.check(
            "kb: index status answers",
            status == 200
            and isinstance(body, dict)
            and body.get("datasource_id") == ds
            and isinstance(body.get("status"), str),
            f"HTTP {status} status {body.get('status') if isinstance(body, dict) else None!r}",
        )
        status, body = self.api.call("POST", f"/api/datasources/{ds}/reindex")
        self.report.check(
            "kb: reindex answers",
            status == 200 and isinstance(body, dict) and "status" in body,
            f"HTTP {status} {str(body)[:160]}",
        )

        def settled() -> dict | None:
            _s, current = self.api.call("GET", f"/api/datasources/{ds}/index-status")
            if isinstance(current, dict) and current.get("status") in (
                "ready",
                "error",
                "failed",
            ):
                return current
            return None

        try:
            final = wait_for(
                "kb index settles", settled, timeout=self.args.kb_timeout, interval=5
            )
        except GateError:
            _s, final = self.api.call("GET", f"/api/datasources/{ds}/index-status")
        final = final if isinstance(final, dict) else {}
        self.report.check(
            "kb: the index reaches ready",
            final.get("status") == "ready" and final.get("notes_total") == 2,
            f"status {final.get('status')!r}, notes {final.get('notes_done')}/"
            f"{final.get('notes_total')}, error {str(final.get('last_error'))[:120]}",
        )

    def refusals(self) -> None:
        status, body = self.create_connector(
            "refused-repository",
            {
                "type": "repository",
                "config": {"forge": "gitea"},
                "credentials": {"auth_method": "token", "token": self.gitea["token"]},
            },
        )
        self.report.check(
            "refusal: a repository without a URL is refused",
            status == 400 and "require a repository URL" in json.dumps(body),
            f"HTTP {status}: {str(body)[:160]}",
        )
        status, body = self.create_connector(
            "refused-mcp",
            {
                "type": "mcp",
                "connection_url": "http://d1a-gate.invalid/mcp",
                "credentials": {"transport": "http"},
            },
        )
        self.report.check(
            "refusal: MCP create is refused while MCP_DATASOURCES_ENABLED is off",
            status == 403 and "MCP connectors are disabled" in json.dumps(body),
            f"HTTP {status}: {str(body)[:160]}",
        )

    def attach_setup(self) -> None:
        bodies = {
            "attach-pg": {"type": "postgresql", "connection_url": self.pg_url},
            "attach-generic": {
                "type": "generic",
                "cli_hint": "D1a gate env connector",
                "credentials": {"env_vars": dict([self.env["generic"]])},
            },
            "attach-credentials": {
                "type": "credentials",
                "credentials": {"env_vars": dict([self.env["credentials"]])},
            },
            "attach-webdav": {
                "type": "webdav",
                "connection_url": self.dav_url,
                "credentials": {"username": self.nc_user, "password": self.nc_password},
            },
        }
        for label, body in bodies.items():
            status, created = self.create_connector(label, body)
            if status not in (200, 201) or label not in self.connectors:
                raise GateError(f"{label} create answered HTTP {status}: {created}")
        # The API refuses MCP connectors here, so an existing row (one made
        # before the gate was turned off) is written straight to the table.
        mcp_name = self.name("mcp-row")
        self.sql_rows["mcp-row"] = mcp_name
        mcp_id = sql(
            "INSERT INTO datasources (name, type, connection_url, credentials, "
            "created_by, scope_mode) VALUES ("
            f"{lit(mcp_name)}, 'mcp', 'http://d1a-gate.invalid/mcp', '{{}}'::jsonb, "
            f"{lit(self.user_id)}, 'all') RETURNING id"
        ).splitlines()[0]
        self.connectors["mcp-row"] = mcp_id
        for label in ("attach-pg", "attach-webdav"):
            self.api.ok(
                "POST",
                f"/api/projects/{self.project}/datasources/{self.connectors[label]}",
                {"read_only": True},
            )

    def job_run(self) -> None:
        ids = [
            self.connectors[label]
            for label in (
                "attach-pg",
                "attach-generic",
                "attach-credentials",
                "mcp-row",
            )
        ]
        created = self.api.ok(
            "POST",
            "/api/jobs",
            {
                "description": (
                    "D1a connector gate. Use the sql_query tool to run "
                    "SELECT note FROM d1a_marker against the attached PostgreSQL "
                    "connector, write the value to output/d1a.txt, then "
                    "complete the job."
                ),
                "project_id": self.project,
                "datasource_ids": ids,
                "execution_lane": "stateless",
                "config_override": {
                    "workspace": {"backend": "sandbox"},
                    "llm": {"model": self.args.model},
                },
            },
        )
        self.job = str(created.get("job_id") or created["id"])
        print(f"job {self.job}", flush=True)
        self.payload_checks()
        self.job_credentials()

    def payload_checks(self) -> None:
        result = in_orchestrator(
            _PAYLOAD_PROGRAM, {"job": self.job, "project": self.project}
        )
        mcp = f"mcp:{self.name('mcp-row')}"
        self.report.check(
            "job payload: MCP_DATASOURCES_ENABLED is off on this deployment",
            result.get("mcp_gate") is False,
            f"gate {result.get('mcp_gate')}",
        )
        self.report.check(
            "job payload: the MCP row is resolved but dropped from the payload",
            mcp in result.get("resolved", []) and mcp not in result.get("payload", []),
            f"resolved {len(result.get('resolved', []))}, payload "
            f"{result.get('payload')}",
        )
        tools = result.get("tools") or {}
        self.report.check(
            "job payload: read-only Postgres binds sql_query without sql_execute",
            self.name("attach-pg") in result.get("read_only", [])
            and "sql_query" in tools.get("sql", [])
            and "sql_execute" not in tools.get("sql", [])
            and not tools.get("mcp"),
            f"tools {tools}",
        )

    def job_credentials(self) -> None:
        """Both env connectors land in ~/.srw-credentials/ while the job runs."""
        selector = f"app=srw-workspace,srw/job-id={self.job}"
        missing = dict(self.env)
        found: list[str] = []

        def materialized() -> bool:
            status = self.job_status()
            if status in JOB_TERMINAL:
                raise GateError(f"job {status} before its credentials were seen")
            pods = json.loads(
                command(K + ["get", "pods", "-l", selector, "-o", "json"])
            )["items"]
            running = [
                pod["metadata"]["name"]
                for pod in pods
                if pod.get("status", {}).get("phase") == "Running"
            ]
            for pod in running:
                for label, (_name, value) in list(missing.items()):
                    paths = self.workspace_grep(pod, value, f"{HOME}/.srw-credentials")
                    if paths:
                        found.extend(paths)
                        missing.pop(label)
            return not missing

        try:
            wait_for(
                "job credentials materialized",
                materialized,
                timeout=self.args.turn_timeout,
                interval=5,
            )
        except GateError as exc:
            self.report.check(
                "job workspace: generic and credentials variables land in "
                "~/.srw-credentials/",
                False,
                f"missing {sorted(missing)}: {exc}",
            )
            return
        self.report.check(
            "job workspace: generic and credentials variables land in "
            "~/.srw-credentials/",
            all(path.startswith(f"{HOME}/.srw-credentials/") for path in found),
            ", ".join(sorted({p.replace(HOME, "~") for p in found})),
        )

    def job_status(self) -> str:
        return sql(f"SELECT coalesce(status, '') FROM jobs WHERE id = {lit(self.job)}")

    def job_settle(self) -> None:
        def status_in(statuses: frozenset[str]) -> Callable[[], str | None]:
            def probe() -> str | None:
                status = self.job_status()
                return status if status in statuses else None

            return probe

        timeout = self.args.job_timeout
        try:
            status = wait_for(
                "job resting", status_in(JOB_RESTING), timeout=timeout, interval=5
            )
            if status == "pending_review":
                self.api.ok("POST", f"/api/jobs/{self.job}/approve", {})
                print("job approved after its review pause", flush=True)
                status = wait_for(
                    "approved job ends",
                    status_in(JOB_TERMINAL),
                    timeout=timeout,
                    interval=5,
                )
        except GateError as exc:
            status = f"{self.job_status() or 'missing'} ({exc})"
        self.report.check(
            "job: settles completed", status == "completed", f"status {status}"
        )

    def job_agent_checks(self) -> None:
        pg = self.name("attach-pg")
        mcp = self.name("mcp-row")
        lines = self.agent_log_lines([pg, mcp])
        connected = f"Connected to postgresql datasource: {pg} (read-only)"
        self.report.check(
            "job agent: logs the read-only Postgres connection",
            any(connected in line for line in lines),
            f"{sum(pg in line for line in lines)} matching lines",
        )
        self.report.check(
            "job agent: never mentions the dropped MCP row",
            not any(mcp in line for line in lines),
            f"{sum(mcp in line for line in lines)} lines",
        )
        tools = self.wait_audited_tools(self.job)
        self.report.check(
            "job agent: audited tools have sql_query but not sql_execute",
            "sql_query" in tools and "sql_execute" not in tools,
            f"{len(tools)} tools; sql: {sorted(t for t in tools if t.startswith('sql'))}",
        )
        used = audit_sql(
            f"SELECT count(*) FROM llm_requests WHERE job_id::text = {lit(self.job)} "
            f"AND position({lit(self.marker)} in request::text) > 0"
        )
        self.report.note(
            f"job agent: {used} audited LLM requests carry the marker row "
            "sql_query read (model behaviour, not gated)"
        )

    def session(self) -> None:
        body: dict[str, Any] = {
            "title": f"D1a connector drivers gate {self.gate_id}",
            "permission_mode": "autonomous",
            "project_id": self.project,
            "datasource_ids": [self.connectors["attach-webdav"]],
            "config_override": {"workspace": {"backend": "sandbox"}},
            "model": self.args.model,
        }
        created = self.api.ok("POST", "/api/persistent/threads", body)
        self.thread = str(created.get("thread_id") or created["id"])
        print(f"session {self.thread}", flush=True)
        lane = sql(f"SELECT execution_lane FROM threads WHERE id = {lit(self.thread)}")
        if lane != "stateless":
            raise GateError(f"session lane is {lane!r}, not stateless")
        self.turn(
            "Call the webdav_list tool on the root folder of the attached WebDAV "
            "storage, then reply with the exact file names it returned.",
            1,
        )

    def session_checks(self) -> None:
        dav = self.name("attach-webdav")
        lines = self.agent_log_lines([dav])
        connected = f"Connected to webdav datasource: {dav} (read-only)"
        self.report.check(
            "session agent: logs the read-only WebDAV connection",
            any(connected in line for line in lines),
            f"{len(lines)} matching lines",
        )
        # A session's audited LLM requests carry no tool list, so the tool
        # surface is read from the deployed builder over the session's exact
        # selection; the agent derives its categories from the same
        # read-only flag the log line above shows it received.
        result = in_orchestrator(
            _PAYLOAD_PROGRAM,
            {
                "datasource_ids": [self.connectors["attach-webdav"]],
                "project": self.project,
            },
        )
        webdav = (result.get("tools") or {}).get("webdav", [])
        self.report.check(
            "session payload: WebDAV linked read-only binds read tools only",
            dav in result.get("read_only", [])
            and "webdav_list" in webdav
            and not {"webdav_write", "webdav_delete"} & set(webdav),
            f"webdav tools {webdav}",
        )
        called, returned = self.tool_use("webdav_list", self.dav_file)
        self.report.check(
            "session agent: webdav_list returns the marker file",
            called > 0 and returned > 0,
            f"{called} webdav_list calls, {returned} tool results with the marker",
        )
        self.turn(
            "Now use the webdav_write tool to create a file named "
            f"{self.dav_probe} in the root folder of the WebDAV storage, "
            "containing the word probe. If you have no such tool, say so and stop.",
            2,
        )
        probe = in_orchestrator(
            _DAV_PROGRAM,
            {
                "method": "GET",
                "url": self.dav_url + self.dav_probe,
                "user": self.nc_user,
                "password": self.nc_password,
            },
        )
        self.report.check(
            "session agent: the read-only link wrote nothing",
            probe.get("status") == 404,
            f"probe file answered HTTP {probe.get('status')}",
        )
        attempted, _results = self.tool_use("webdav_write", self.dav_probe)
        self.report.note(
            f"session agent: the model made {attempted} webdav_write calls when "
            "asked to write (model behaviour, not gated)"
        )

    def tool_use(self, tool: str, text: str) -> tuple[int, int]:
        """Session tool calls of ``tool`` and tool results holding ``text``."""
        called, returned = sql(
            "SELECT (SELECT count(*) FROM thread_messages WHERE thread_id = "
            f"{lit(self.thread)} AND role IN ('ai', 'assistant') AND "
            f"position({lit(chr(34) + tool + chr(34))} in "
            "coalesce(tool_calls::text, '')) > 0) || ' ' || (SELECT count(*) "
            f"FROM thread_messages WHERE thread_id = {lit(self.thread)} AND "
            f"role = 'tool' AND position({lit(text)} in coalesce(content, '')) > 0)"
        ).split()
        return int(called), int(returned)

    def end_session(self) -> bool:
        """End and permanently delete the session; True once its row is gone."""
        return self.delete_thread(self.thread)

    def delete_thread(self, thread: str | None) -> bool:
        def gone() -> bool:
            status, _body = self.api.call(
                "DELETE",
                f"/api/persistent/threads/{thread}?force=true&permanent=true",
            )
            if status == 404:
                return True
            return sql(f"SELECT count(*) FROM threads WHERE id = {lit(thread)}") == "0"

        try:
            wait_for("session deleted", gone, timeout=300, interval=5)
        except GateError:
            return False
        return True

    # -- live attach and detach on a pinned session (D1b) -----------------
    def live(self) -> None:
        self.live_setup()
        self.check_pinned_pool()
        self.live_session()
        self.live_attached(
            self.live_update(
                [self.connectors[label] for label in LIVE_LABELS], "attach"
            )
        )
        self.live_detached(self.live_update([], "detach"))
        self.report.check(
            "live: session ended and deleted",
            self.delete_thread(self.live_thread),
            "thread row",
        )

    def live_setup(self) -> None:
        private, public, self.live_fingerprint = make_key_pair()
        in_orchestrator(
            _GITEA_PROGRAM,
            {
                "action": "deploy_key",
                "repo": self.repo,
                "title": self.name("live deploy key"),
                "public_key": public,
                "access_mode": "read",
            },
        )
        owner, host = self.gitea["owner"], self.gitea["ssh_host"]
        status, created = self.create_connector(
            "live-ssh",
            {
                "type": "repository",
                "connection_url": (
                    f"ssh://git@{host}:{self.gitea['ssh_port']}/{owner}/{self.repo}.git"
                ),
                "config": {"forge": "gitea"},
                "credentials": {"auth_method": "ssh", "ssh_key": private},
            },
        )
        if status not in (200, 201) or "live-ssh" not in self.connectors:
            raise GateError(f"live-ssh create answered HTTP {status}: {created}")

    def live_session(self) -> None:
        body: dict[str, Any] = {
            "title": f"D1b live connectors gate {self.gate_id}",
            "permission_mode": "autonomous",
            "project_id": self.project,
            # Explicitly none: the project's linked connectors would otherwise
            # be selected by default.
            "datasource_ids": [],
            # An Officer conference is pinned by rule: its own agent pod.
            "config_override": {
                "workspace": {"backend": "sandbox"},
                "officer": {"conference": True},
            },
            "model": self.args.model,
        }
        created = self.api.ok("POST", "/api/persistent/threads", body)
        self.live_thread = str(created.get("thread_id") or created["id"])
        print(f"live session {self.live_thread}", flush=True)
        lane = sql(
            f"SELECT execution_lane FROM threads WHERE id = {lit(self.live_thread)}"
        )
        if lane != "pinned":
            raise GateError(f"live session lane is {lane!r}, not pinned")
        pod = self.pinned_pod()["metadata"]["name"]
        problems = self.served_problems(pod, self.pinned_served)
        self.report.check(
            "live: the pinned agent pod serves this checkout's connector modules",
            not problems,
            "; ".join(problems)[:600],
        )
        if problems:
            raise GateError("the pinned agent pod does not serve this checkout")
        wait_for(
            "pinned session attached (workspace facts written)",
            lambda: self.pinned_log_lines(["Wrote workspace facts"]),
            timeout=self.args.turn_timeout,
            interval=5,
        )

    def pinned_assignment(self) -> dict[str, str] | None:
        """The agent the live thread is assigned to: ``hostname``, ``pod_ip``.

        A pooled pinned pod (``srw-agent-j-*``) serves the thread without a
        thread label, so the assignment, as C0 reads a pinned job's, is the
        only reliable pointer.
        """
        row = sql(
            "SELECT coalesce(json_build_object('hostname', a.hostname, "
            "'pod_ip', a.pod_ip)::text, '') FROM threads t JOIN agents a ON "
            f"a.id = t.agent_id WHERE t.id = {lit(self.live_thread)}"
        )
        if not row:
            return None
        assignment = json.loads(row)
        hostname = str(assignment.get("hostname") or "")
        if not _POD_NAME_RE.fullmatch(hostname):
            return None
        return {"hostname": hostname, "pod_ip": str(assignment.get("pod_ip") or "")}

    def pinned_pod(self) -> dict:
        """The running, ready pod the live thread's assignment names."""

        def probe() -> dict | None:
            assignment = self.pinned_assignment()
            if assignment is None:
                return None
            rc, out, _err = run(
                K + ["get", "pod", assignment["hostname"], "-o", "json"], timeout=60
            )
            if rc:
                return None
            pod = json.loads(out)
            status = pod.get("status", {})
            ready = (
                not pod["metadata"].get("deletionTimestamp")
                and status.get("phase") == "Running"
                and status.get("podIP")
                and all(
                    item.get("ready")
                    for item in status.get("containerStatuses") or [{}]
                )
            )
            if not ready:
                return None
            if assignment["pod_ip"] and assignment["pod_ip"] != status["podIP"]:
                return None
            return pod

        return wait_for(
            f"pinned agent pod assigned to {self.live_thread}",
            probe,
            timeout=600,
            interval=5,
        )

    def pooled_pinned_pods(self) -> list[str]:
        """Running pinned-pool pods, by name (no label: the name is the pool's)."""
        pods = json.loads(command(K + ["get", "pods", "-o", "json"]))["items"]
        return sorted(
            pod["metadata"]["name"]
            for pod in pods
            if pod["metadata"]["name"].startswith(POOLED_PINNED_PREFIX)
            and not pod["metadata"].get("deletionTimestamp")
            and pod.get("status", {}).get("phase") == "Running"
        )

    def idle_pinned_pods(self, names: list[str]) -> list[str]:
        """Of ``names``, the pods with no current job and no live thread."""
        if not names:
            return []
        listed = ", ".join(lit(name) for name in names if _POD_NAME_RE.fullmatch(name))
        if not listed:
            return []
        rows = sql(
            "SELECT a.hostname FROM agents a WHERE a.hostname IN ("
            + listed
            + ") AND a.current_job_id IS NULL AND NOT EXISTS (SELECT 1 FROM "
            "threads t WHERE t.agent_id = a.id AND t.ended_at IS NULL) "
            "ORDER BY a.hostname"
        )
        return [row for row in rows.splitlines() if row]

    def check_pinned_pool(self) -> None:
        """Refuse a pinned pool whose idle pods serve other connector code.

        A new pinned thread may land on any idle pooled pod, and an idle one
        keeps the image it was created with after a Tilt rebuild. The byte
        check of the assigned pod (``live_session``) would then only fail
        after the session exists; this names the pods to delete up front. A
        busy stale pod (a current job or a live thread) cannot receive the
        thread, so it is only noted.
        """
        stale = [
            name
            for name in self.pooled_pinned_pods()
            if self.served_problems(name, self.pinned_served)
        ]
        idle = self.idle_pinned_pods(stale)
        busy = [name for name in stale if name not in idle]
        if busy:
            self.report.note(
                f"live: busy pooled pinned pods serve stale connector code {busy}; "
                "they cannot take the live session's thread"
            )
        detail = ""
        if idle:
            detail = (
                f"idle and stale: {idle}; idle pods keep their image after a "
                "Tilt rebuild. Delete them and rerun: kubectl "
                f"--context={LOCAL_CONTEXT} -n {LOCAL_NAMESPACE} delete pod "
                + " ".join(idle)
            )
        self.report.check(
            "live: every idle pooled pinned agent pod serves this checkout's "
            "connector modules",
            not idle,
            detail,
        )
        if idle:
            raise GateError(
                "an idle pooled pinned agent pod serves stale connector code: " + detail
            )

    def pinned_log_lines(self, needles: list[str]) -> list[str]:
        """The pinned agent pod's log lines since the gate started."""
        pod = self.pinned_pod()["metadata"]["name"]
        rc, out, _err = run(
            K + ["logs", pod, "-c", AGENT_CONTAINER] + [f"--since-time={self.started}"],
            timeout=120,
        )
        if rc:
            return []
        return [line for line in out.splitlines() if any(n in line for n in needles)]

    def session_ready(self) -> bool:
        """Whether /connection admits the live session (409/425: attaching)."""
        status, body = self.api.call(
            "GET", f"/api/sessions/{self.live_thread}/connection"
        )
        if status == 200:
            return True
        if status in (409, 425):
            return False
        raise GateError(f"/connection answered HTTP {status}: {str(body)[:200]}")

    def live_update(self, datasource_ids: list[str], label: str) -> dict:
        """One live ``config.update``; retried while the session still attaches.

        The agent writes the workspace facts before the thread is marked
        active, so /connection is polled until it admits the session, and an
        attempt that still meets 409/425, a 4500/4503 close or "No active
        session" is retried.
        """
        request_id = f"{self.gate_id}-{label}"
        wait_for(
            "live session admitted by /connection",
            self.session_ready,
            timeout=self.args.turn_timeout,
            interval=5,
        )

        def attempt() -> dict | None:
            result = in_orchestrator(
                _LIVE_UPDATE_PROGRAM,
                {
                    "username": self.api.username,
                    "password": self.api.password,
                    "token_url": KEYCLOAK_TOKEN_URL,
                    "thread": self.live_thread,
                    "ip": self.pinned_pod()["status"]["podIP"],
                    "port": AGENT_PORT,
                    "datasource_ids": datasource_ids,
                    "request_id": request_id,
                    "timeout": self.args.turn_timeout,
                    "max_size": WS_MAX_FRAME,
                },
                timeout=self.args.turn_timeout + 90,
            )
            if result.get("outcome") == "retry":
                print(f"live {label}: retrying ({result.get('reason')})", flush=True)
                return None
            if result.get("outcome") == "error" and "No active session" in str(
                result.get("message")
            ):
                return None
            return result

        result = wait_for(
            f"live {label} answered",
            attempt,
            timeout=self.args.turn_timeout,
            interval=10,
        )
        print(f"live {label}: {json.dumps(result)[:300]}", flush=True)
        return result

    def live_workspace(self) -> str:
        return self.workspace_pod(f"app=srw-workspace,srw/thread-id={self.live_thread}")

    def held_fingerprints(self, pod: str) -> set[str]:
        """Fingerprints every connector ssh-agent socket in the workspace holds.

        The script always exits 0, so a failed exec raises instead of reading
        as "no key held".
        """
        _rc, listing = self.ws(
            pod,
            'for socket in ~/.ssh/srw-managed/sockets/*.sock; do test -S "$socket" '
            '|| continue; SSH_AUTH_SOCK="$socket" ssh-add -l 2>/dev/null | '
            "awk 'NF { print $2 }'; done\nexit 0\n",
        )
        return set(listing.split())

    def live_checkout(self, pod: str) -> tuple[bool, bool]:
        """``(cloned, fetches)`` for the live repository's checkout.

        One script that always exits 0 and prints both answers, so a missing
        clone or a failed exec never reads as "no longer fetches".
        """
        _rc, out = self.ws(
            pod,
            f"repo=~/workspace/repos/{self.repo}; cloned=0; fetched=0\n"
            'test -d "$repo/.git" && cloned=1\n'
            'if [ "$cloned" = 1 ]; then (cd "$repo" && GIT_TERMINAL_PROMPT=0 '
            "git fetch -q origin 2>/dev/null) && fetched=1; fi\n"
            'echo "checkout cloned=$cloned fetched=$fetched"\nexit 0\n',
        )
        match = re.search(r"checkout cloned=([01]) fetched=([01])", out)
        if not match:
            raise GateError(f"the checkout state is unreadable: {out[-200:]}")
        return match.group(1) == "1", match.group(2) == "1"

    def live_readme(self, pod: str) -> str:
        _rc, text = self.ws(pod, "cat ~/workspace/README.md 2>/dev/null\n", check=False)
        return text

    def live_attached(self, result: dict) -> None:
        names = {self.name(label) for label in LIVE_LABELS}
        added = set((result.get("datasources") or {}).get("added") or [])
        self.report.check(
            "live attach: config.changed lists the three connectors added",
            result.get("outcome") == "config.changed" and added == names,
            f"{result.get('outcome')}: added {sorted(added)} "
            f"{result.get('message') or ''} {result.get('detail') or ''}".strip(),
        )
        pg = self.name("attach-pg")
        lines = self.pinned_log_lines([pg, "Datasources re-set up live"])
        connected = f"Connected to postgresql datasource: {pg} (read-only)"
        self.report.check(
            "live attach: the pinned agent opens the read-only Postgres connection",
            any(connected in line for line in lines),
            f"{sum(pg in line for line in lines)} matching lines",
        )
        self.report.check(
            "live attach: the registry re-set up the three connectors",
            any("3 attached (3 added, 0 removed)" in line for line in lines),
            next((line[-120:] for line in lines if "re-set up" in line), "no line"),
        )
        pod = self.live_workspace()
        _name, value = self.env["generic"]
        paths = self.workspace_grep(pod, value, f"{HOME}/.srw-credentials")
        self.report.check(
            "live attach: the generic connector's variable lands in "
            "~/.srw-credentials/",
            bool(paths),
            ", ".join(sorted({p.replace(HOME, "~") for p in paths})),
        )
        self.report.check(
            "live attach: a workspace ssh-agent holds the SSH repository's key",
            self.live_fingerprint in self.held_fingerprints(pod),
        )
        self.report.check(
            "live attach: the SSH repository is cloned and fetches through its alias",
            self.live_checkout(pod) == (True, True),
        )
        readme = self.live_readme(pod)
        self.report.check(
            "live attach: README.md lists the three connectors",
            all(name in readme for name in names),
            f"{sum(name in readme for name in names)} of 3 named",
        )

    def live_detached(self, result: dict) -> None:
        names = {self.name(label) for label in LIVE_LABELS}
        removed = set((result.get("datasources") or {}).get("removed") or [])
        self.report.check(
            "live detach: config.changed lists the three connectors removed",
            result.get("outcome") == "config.changed" and removed == names,
            f"{result.get('outcome')}: removed {sorted(removed)} "
            f"{result.get('message') or ''} {result.get('detail') or ''}".strip(),
        )
        lines = self.pinned_log_lines(["Datasources re-set up live"])
        self.report.check(
            "live detach: the registry re-set up no connectors",
            any("0 attached (0 added, 3 removed)" in line for line in lines),
            (lines[-1][-120:] if lines else "no line"),
        )
        try:
            closed = wait_for(
                "replaced connection closed after the turn",
                lambda: self.pinned_log_lines(
                    ["Closed 1 replaced datasource connection(s) after turn end"]
                ),
                timeout=120,
                interval=5,
            )
        except GateError:
            closed = []
        self.report.check(
            "live detach: the replaced Postgres connection closes after the turn",
            bool(closed),
        )
        pod = self.live_workspace()
        self.report.check(
            "live detach: the SSH repository's ssh-agent is retired",
            self.live_fingerprint not in self.held_fingerprints(pod),
        )
        self.report.check(
            "live detach: the clone stays but no longer fetches",
            self.live_checkout(pod) == (True, False),
        )
        self.report.check(
            "live detach: README.md says no connectors are attached",
            "_No connectors attached._" in self.live_readme(pod),
        )
        _name, value = self.env["generic"]
        kept = bool(self.workspace_grep(pod, value, f"{HOME}/.srw-credentials"))
        self.report.note(
            "live detach: the detached generic variable "
            + ("stays in" if kept else "left")
            + " ~/.srw-credentials/ (v1 keeps installed values for the session)"
        )

    def cockpit(self) -> None:
        """Press Test on a kubeconfig row in the real cockpit (Playwright)."""
        if self.args.skip_cockpit:
            self.report.note("cockpit check skipped (--skip-cockpit)")
            return
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            self.report.check(
                "cockpit: kubeconfig Test renders unsupported",
                False,
                "playwright is not installed in this interpreter",
            )
            return
        status, _body = self.create_connector(
            "cockpit-kubeconfig",
            {
                "type": "kubeconfig",
                "credentials": {
                    "files": [{"contents": "apiVersion: v1\nkind: Config\n"}]
                },
            },
        )
        if status not in (200, 201):
            raise GateError(f"cockpit kubeconfig create answered HTTP {status}")
        name = self.name("cockpit-kubeconfig")
        detail = ""
        ok = False
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                context = browser.new_context(
                    ignore_https_errors=True,
                    service_workers="block",
                    viewport={"width": 1600, "height": 1000},
                )
                page = context.new_page()
                page.goto(COCKPIT_URL, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_selector("#username", timeout=60000)
                page.fill("#username", self.args.user)
                page.fill("#password", self.args.password)
                page.click("#kc-login")
                row = page.locator("tr", has_text=name)
                row.first.wait_for(timeout=60000)
                row.first.locator("app-icon-button").filter(
                    has=page.locator("app-icon", has_text="cable")
                ).first.click()
                result = row.first.locator("span.inline-test")
                result.wait_for(timeout=30000)
                classes = result.get_attribute("class") or ""
                # textContent: the icon ligature can be invisible until the
                # icon font loads, which leaves innerText empty.
                icon = (result.text_content() or "").strip()
                title = result.get_attribute("title") or ""
                ok = (
                    "test-unsupported" in classes
                    and "test-error" not in classes
                    and icon == "info"
                    and UNSUPPORTED["kubeconfig"] in title
                )
                detail = f"classes {classes!r}, icon {icon!r}, title {title[:100]!r}"
            except Exception as exc:  # noqa: BLE001 -- the check reports it
                detail = f"{type(exc).__name__}: {str(exc).splitlines()[0][:200]}"
            finally:
                browser.close()
        self.report.check(
            "cockpit (Playwright): kubeconfig Test renders unsupported", ok, detail
        )

    def notes(self) -> None:
        services = command(K + ["get", "svc", "-o", "name"])
        if "neo4j" in services:
            self.report.note(
                "a Neo4j service exists on this cluster, but this gate does not "
                "exercise it"
            )
        else:
            self.report.note(
                "Neo4j is not deployed on k3d; read-only Neo4j enforcement "
                "(READ_ACCESS sessions) is covered by tests/test_neo4j_read_access.py "
                "against a real neo4j:5 container"
            )
        self.report.note(
            "MongoDB and email have no k3d server; their drivers are covered by "
            "the connector goldens"
        )

    # -- cleanup -----------------------------------------------------------
    def cleanup(self) -> list[str]:
        problems: list[str] = []

        def step(label: str, action: Callable[[], Any]) -> None:
            try:
                if action() is False:
                    problems.append(label)
            except GateError as exc:
                problems.append(f"{label} ({exc})")

        if self.thread:
            step("delete session", self.end_session)
        if self.live_thread:
            step("delete live session", lambda: self.delete_thread(self.live_thread))
        # A create that timed out may still have made its session (and a
        # pinned pod): every session titled with this gate id goes too.
        for leftover in self.titled_threads():
            if leftover not in (self.thread, self.live_thread):
                step(
                    f"delete leftover session {leftover}",
                    lambda leftover=leftover: self.delete_thread(leftover),
                )
        if self.job:
            step(
                "cancel job",
                lambda: self.api.call("PUT", f"/api/jobs/{self.job}/cancel") and None,
            )

            def deleted() -> bool:
                status, _body = self.api.call("DELETE", f"/api/jobs/{self.job}")
                return status in (200, 204, 404)

            step(
                "delete job",
                lambda: bool(
                    wait_for("job deleted", deleted, timeout=240, interval=10)
                ),
            )
        for label, datasource_id in list(self.connectors.items()):

            def delete(datasource_id=datasource_id, label=label) -> bool:
                status, _body = self.api.call(
                    "DELETE", f"/api/datasources/{datasource_id}"
                )
                if status in (200, 204, 404):
                    return True
                if label in self.sql_rows:
                    sql(
                        f"DELETE FROM datasources WHERE id = {lit(datasource_id)} "
                        f"AND name = {lit(self.sql_rows[label])}"
                    )
                    return True
                return False

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
        if self.repos:
            step(
                "delete Gitea repositories and token",
                lambda: in_orchestrator(
                    _GITEA_PROGRAM,
                    {
                        "action": "cleanup",
                        "repos": self.repos,
                        "token_name": self.token_name,
                    },
                ),
            )
        if self.nc_started:
            step(
                "delete Nextcloud user",
                lambda: self.nextcloud_occ(f"user:delete {self.nc_user}") in (0, 1),
            )
        if self.pg_started:
            step(
                "drop database and role",
                lambda: sql_script(
                    f"DROP DATABASE IF EXISTS {self.pg_name} WITH (FORCE);\n"
                    f"DROP ROLE IF EXISTS {self.pg_name};\n"
                ),
            )
        for problem in problems:
            print(f"cleanup: {problem} failed", flush=True)
        return problems

    def titled_threads(self) -> list[str]:
        """Sessions whose title carries this gate id (both phases title them)."""
        rows = sql(
            "SELECT id FROM threads WHERE "
            f"position({lit(self.gate_id)} in coalesce(title, '')) > 0"
        )
        return [row for row in rows.splitlines() if re.fullmatch(r"[0-9a-f-]{36}", row)]

    def residue(self) -> list[str]:
        """What this run created and cleanup did not remove."""
        left: list[str] = []
        titled = self.titled_threads()
        if titled:
            left.append(f"sessions titled with the gate id: {titled}")
        prefix = self.gate_id.replace("_", "\\_") + " %"
        count = sql(f"SELECT count(*) FROM datasources WHERE name LIKE {lit(prefix)}")
        if count != "0":
            left.append(f"{count} connectors")
        for table, value in (
            ("jobs", self.job),
            ("threads", self.thread),
            ("threads", self.live_thread),
            ("projects", self.project),
        ):
            if (
                value
                and sql(f"SELECT count(*) FROM {table} WHERE id = {lit(value)}") != "0"
            ):
                left.append(f"{table} row {value}")
        selectors = [
            f"srw/job-id={self.job}" if self.job else "",
            f"srw/thread-id={self.thread}" if self.thread else "",
            f"srw/thread-id={self.live_thread}" if self.live_thread else "",
            f"{PINNED_THREAD_LABEL}={self.live_thread}" if self.live_thread else "",
            *(
                selector
                for thread in titled
                for selector in (
                    f"srw/thread-id={thread}",
                    f"{PINNED_THREAD_LABEL}={thread}",
                )
            ),
        ]
        for selector in filter(None, selectors):
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
            self.pg_started
            and sql(
                "SELECT (SELECT count(*) FROM pg_database WHERE datname = "
                f"{lit(self.pg_name)}) + (SELECT count(*) FROM pg_roles WHERE "
                f"rolname = {lit(self.pg_name)})"
            )
            != "0"
        ):
            left.append(f"database or role {self.pg_name}")
        if self.nc_started and self.nextcloud_occ(f"user:info {self.nc_user}") == 0:
            left.append(f"Nextcloud user {self.nc_user}")
        if self.repos:
            gitea = in_orchestrator(
                _GITEA_PROGRAM,
                {
                    "action": "residue",
                    "repos": list(self.repos),
                    "token_name": self.token_name,
                },
            )
            left += [f"Gitea repo {repo}" for repo in gitea.get("remaining", [])]
            if gitea.get("token_left"):
                left.append(f"Gitea token {self.token_name}")
        return left

    # -- run ---------------------------------------------------------------
    def run(self) -> int:
        try:
            self.preflight()
            self.fixture()
            self.lifecycle()
            self.refusals()
            self.attach_setup()
            self.job_run()
            self.session()
            self.session_checks()
            self.report.check(
                "session: ended and deleted", self.end_session(), "thread row"
            )
            if not self.args.skip_live:
                try:
                    self.live()
                except GateError as exc:
                    # The job, its agent checks and the cockpit still run.
                    self.report.check("live: infrastructure", False, str(exc))
            self.job_settle()
            self.job_agent_checks()
            self.cockpit()
            self.notes()
        except GateError as exc:
            self.report.check("gate infrastructure", False, str(exc))
        finally:
            if self.args.keep:
                print(
                    "kept: "
                    + json.dumps(
                        {
                            "connectors": self.connectors,
                            "project": self.project,
                            "job": self.job,
                            "thread": self.thread,
                            "live_thread": self.live_thread,
                            "repos": sorted(self.repos),
                            "database": self.pg_name if self.pg_started else None,
                            "nextcloud_user": self.nc_user if self.nc_started else None,
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
    parser.add_argument("--turn-timeout", type=int, default=420)
    parser.add_argument("--job-timeout", type=int, default=900)
    parser.add_argument("--kb-timeout", type=int, default=240)
    parser.add_argument("--skip-cockpit", action="store_true")
    parser.add_argument(
        "--skip-live",
        action="store_true",
        help="skip the D1b live attach/detach on a pinned session",
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
        raise SafetyError("--gate-id must be d1a- followed by 10 hex digits")
    if not _MODEL_RE.fullmatch(args.model):
        raise SafetyError("model id is malformed")
    if not re.fullmatch(r"[a-z][a-z0-9._-]{0,63}", args.user):
        raise SafetyError("user name is malformed")
    if not 60 <= args.turn_timeout <= 1800:
        raise SafetyError("--turn-timeout must be between 60 and 1800 seconds")
    if not 60 <= args.job_timeout <= 3600:
        raise SafetyError("--job-timeout must be between 60 and 3600 seconds")
    if not 30 <= args.kb_timeout <= 1800:
        raise SafetyError("--kb-timeout must be between 30 and 1800 seconds")


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
    return ConnectorDriversGate(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
