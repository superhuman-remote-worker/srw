#!/usr/bin/env python3
"""Local k3d gate for connector drivers D3a: datasources become Connectors.

Design: knowledge-base/knowledge/features/connector_drivers.md, "Datasources
become Connectors" and Track D, D3 (the D3a part of its gate).
Templates: scripts/k3d-connector-drivers-gate.py (D1a) and the C0 and C1
gates -- the same safety envelope: dry-run by default, the exact
k3d-srw/srw context, passwords only on ``kubectl exec -i`` stdin and
scrubbed from every printed line, ``cap_memory()`` in every program the gate
runs in a pod, and a cleanup in ``finally`` that touches only what this run
created, followed by a residue check by gate id.

Fixtures (all disposable, all named after the gate id):

  postgres   a database and a login role on srw-postgres (SELECT on one
             marker table) that the gate's Postgres connectors point at
  project    one project owned by the owner account (``--user``), with its
             native knowledge base, and the second account (``--other-user``)
             added as an editor
  legacy     one Postgres row written straight to the table and linked to the
             project, as an orchestrator without the write-through would

Checks (each printed PASS/FAIL; the exit status is 0 only if all pass):

  preflight  Tilt reports the srw resource ``ok``; every orchestrator pod
             serves this checkout's D3a modules byte for byte; the migration
             ledger holds 0350-0353 as applied; both accounts are approved
  backfill   the deployed ``migrate_stored_connectors``, rerun in the
             orchestrator pod, creates the legacy row's Connector and defers
             nothing; afterwards every datasource in the database has the
             Connector the mapping names -- uid and linked id = the row id,
             ``<slug>-<12 hex>`` name, scope (creator's Account, a native KB's
             project, an ownerless row's one project), driver, access, the
             platform marker -- or none when it stays on the legacy path, and
             no resource carries credentials. The rerun changed no
             ``policy_revision``, no project link and none of the legacy row's
             reconcile entries
  write      through the API: create, update, link, unlink and delete keep the
             row and its Connector in step (a policy-only write leaves the
             resource version alone; delete retires it and keeps the
             tombstone); the stored documents never hold the Postgres
             password; the resource API refuses to delete a linked Connector
  native     the project's own knowledge base has ``managed_key``
             ``project-kb:<project>`` and a platform-managed Connector in the
             project's scope; a policy update and a delete through the
             datasource API answer 409 as before, an unlink 409, and a
             resource API delete 409; row and resource are unchanged after
  shared     a public (published) Postgres connector of the owner and one
             linked to the project still work for the other account: its
             stateless session selecting both is admitted and keeps both ids,
             the deployed payload builder delivers both for that session, and
             after one turn the agent logs a connection to each
  cleanup    nothing this run created is left: connectors (live rows and live
             Connector resources by gate id), sessions titled with the gate
             id and their pods, the project, the database and the role

Run with the repository venv on the k3d-srw cluster, alone: this is a
mutating gate. The owner must be able to publish (an administrator or the
``public_datasources`` grant); the other account must be an approved
non-administrator.

  .venv/bin/python scripts/k3d-connector-resources-gate.py           # plan
  .venv/bin/python scripts/k3d-connector-resources-gate.py \\
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
KEYCLOAK_TOKEN_URL = "http://srw-keycloak:8080/realms/srw/protocol/openid-connect/token"
POD_ROOT = "/app"
PG_HOST = "srw-postgres"
DEFAULT_MODEL = "muse-spark-1.3-contributor"
_SELECTOR = (
    "app.kubernetes.io/instance=srw,app.kubernetes.io/name=superhuman-remote-worker"
)
_GATE_ID_RE = re.compile(r"d3a-[0-9a-f]{10}\Z")
_MODEL_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@-]{0,199}\Z")
_USER_RE = re.compile(r"[a-z][a-z0-9._-]{0,63}\Z")
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")
_NAME_RE = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")

MIGRATIONS = (
    "0350_datasource_manifest_identity.sql",
    "0351_resource_platform_managed.sql",
    "0352_validate_datasource_manifest_identity.sql",
    "0353_datasources_managed_key_idx.notx.sql",
)
#: What the orchestrator must serve byte for byte for D3a.
SERVED = (
    "src/orchestrator/services/manifest_connectors.py",
    "src/orchestrator/services/manifest_store.py",
    "src/orchestrator/database/postgres.py",
    "src/orchestrator/services/datasources.py",
    "src/orchestrator/services/projects.py",
    "src/orchestrator/services/datasource_policy.py",
    "src/orchestrator/services/connector_drivers/base.py",
    "src/orchestrator/services/connector_drivers/kb.py",
    "src/orchestrator/services/connector_drivers/mcp_client.py",
    "src/orchestrator/application/lifecycle.py",
    "src/shared/connectors/platform.py",
    "src/shared/connectors/builtin.py",
    *(f"src/orchestrator/database/migrations/app/{name}" for name in MIGRATIONS),
)
PROJECT_KB = "project-kb"
MCP_REMOTE_DRIVER = "srw.mcp-remote/v1"
NATIVE_POLICY_DETAIL = (
    "The native project knowledge connector policy is managed by its project"
)
NATIVE_DELETE_DETAIL = "The project knowledge connector is managed by its project"
NATIVE_UNLINK_DETAIL = "The native knowledge connector link is managed by its project"
PLATFORM_MANAGED_MESSAGE = (
    "SRW manages this connector; it cannot be changed or deleted here."
)
LINKED_CONNECTOR_MESSAGE = (
    "Change or delete this connector on the Connectors page "
    "(/api/datasources); its resource is written from there."
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


def sql_json(query: str) -> Any:
    """``query`` returns one JSON value; parse it."""
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
# Programs run inside the orchestrator container (stdin carries every secret)
# ---------------------------------------------------------------------------

# Every program the gate runs inside a pod starts with this and calls
# cap_memory() once its imports are done. The orchestrator pod has a 1 GiB
# limit and serves the product meanwhile: a gate program that grows must
# fail the gate with a MemoryError, never take the pod to the OOM killer.
# (The same helper as scripts/k3d-connector-drivers-gate.py.)
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

# The deployed startup backfill, rerun: idempotent, and the one that heals
# rows an orchestrator without the write-through wrote.
_BACKFILL_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import asyncio, json, logging, sys
from orchestrator.database.postgres import PostgresDB
from orchestrator.services.manifest_connectors import migrate_stored_connectors
cap_memory()
logging.disable(logging.CRITICAL)
async def main():
    db = PostgresDB(min_connections=1, max_connections=2)
    await db.connect()
    try:
        counts = await migrate_stored_connectors(db)
    finally:
        await db.close()
    print(json.dumps(counts))
asyncio.run(main())
"""
)

# The deployed payload builder over a session's selection. It prints names
# and types only: the resolved rows carry decrypted credentials.
_PAYLOAD_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import asyncio, json, sys
from types import SimpleNamespace
from orchestrator.application.preparation import datasource_payload_dependencies
from orchestrator.application.settings import DeploymentSettings
from orchestrator.database.postgres import PostgresDB
from orchestrator.services.agent_datasource_payload import build_datasources_payload
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
        rows = await db.resolve_datasources_for_thread(
            request["datasource_ids"], [request["project"]]
        )
    finally:
        await db.close()
    deps = datasource_payload_dependencies(
        SimpleNamespace(
            connector_drivers=builtin_connector_drivers(),
            settings=DeploymentSettings.from_environment(),
        )
    )
    payload = build_datasources_payload(rows, dependencies=deps) or []
    print(json.dumps({
        "resolved": sorted(r["name"] for r in rows),
        "payload": sorted(e.get("name") for e in payload),
        "with_endpoint": sorted(
            e.get("name") for e in payload if e.get("connection_url")
        ),
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
expected = json.loads(sys.stdin.read())
stale = sorted(
    path for path, digest in expected.items()
    if not (root / path).is_file()
    or hashlib.sha256((root / path).read_bytes()).hexdigest() != digest
)
print(json.dumps({"stale": stale}))
"""
)


def in_orchestrator(
    program: str, payload: dict[str, Any], *, timeout: int = 300
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


def can_publish_connectors(capabilities: Any) -> bool:
    """An administrator (``grants`` is then null) or the publish grant."""
    if not isinstance(capabilities, dict):
        return False
    if capabilities.get("is_admin"):
        return True
    grants = capabilities.get("grants")
    return isinstance(grants, dict) and grants.get("public_datasources") is True


def expected_bytes() -> dict[str, str]:
    return {
        path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() for path in SERVED
    }


# ---------------------------------------------------------------------------
# What the mapping says (pure, unit-tested)
# ---------------------------------------------------------------------------


def name_stem(name: str) -> str:
    """The ``<slug>`` of ``<slug>-<12 hex>`` (manifest_connectors' rule)."""
    stem = re.sub(r"[^a-z0-9]+", "-", str(name or "").lower()).strip("-")
    return (stem or "connector")[:50].rstrip("-")


def driver_for(ds_type: str, transport: str | None) -> str | None:
    """The driver a row of ``ds_type`` stores, from this checkout's specs."""
    if str(ROOT / "src") not in sys.path:
        sys.path.insert(0, str(ROOT / "src"))
    from shared.connectors.builtin import spec_for_type

    spec = spec_for_type(ds_type)
    if spec is None:
        return None
    if ds_type == "mcp" and str(transport or "http") != "stdio":
        return MCP_REMOTE_DRIVER
    return spec.name


def _uuid(value: Any) -> str | None:
    text = str(value or "").lower()
    return text if _UUID_RE.fullmatch(text) else None


def expected_scope(row: dict[str, Any]) -> tuple[str, str] | None:
    """The Connector scope a row lives in, or None for the legacy path."""
    native = _uuid(row.get("native")) if row.get("type") == "kb" else None
    key = str(row.get("managed_key") or "")
    if key.startswith(PROJECT_KB + ":"):
        native = _uuid(key.split(":", 1)[1])
    if native:
        return ("Project", native) if row.get("native_exists") else None
    if row.get("created_by"):
        return ("Account", str(row["created_by"]))
    links = sorted(set(row.get("links") or []))
    if len(links) == 1:
        return ("Project", links[0])
    return None


def row_problems(row: dict[str, Any], *, exact_name: bool = False) -> list[str]:
    """Everything wrong with one row's Connector against the D3a mapping."""
    label = f"{row.get('name')!r} ({row.get('id')})"
    resource = row.get("resource")
    live = resource if resource and not resource.get("deleted") else None
    if row.get("job_id"):
        return [f"{label}: a legacy job clone has a Connector"] if live else []
    scope = expected_scope(row)
    driver = driver_for(str(row.get("type")), (live or {}).get("transport"))
    if scope is None or driver is None:
        return [f"{label}: on the legacy path but has a Connector"] if live else []
    if live is None:
        return [f"{label}: no live Connector"]
    problems: list[str] = []
    hex_id = str(row["id"]).replace("-", "")
    name = str(live.get("name") or "")
    if not _NAME_RE.fullmatch(name) or not (
        name.endswith("-" + hex_id[:12]) or name.endswith("-" + hex_id)
    ):
        problems.append(f"{label}: name {name!r} is not <slug>-<hex of id>")
    elif exact_name and name != f"{name_stem(row['name'])}-{hex_id[:12]}":
        problems.append(f"{label}: name {name!r} is not the row name's slug")
    if live.get("kind") != "Connector" or str(live.get("linked_id")) != str(row["id"]):
        problems.append(f"{label}: not a Connector linked to its row")
    if (live.get("scope_kind"), live.get("scope_name")) != scope:
        problems.append(
            f"{label}: scope {live.get('scope_kind')}/{live.get('scope_name')}, "
            f"expected {scope[0]}/{scope[1]}"
        )
    if live.get("driver") != driver:
        problems.append(f"{label}: driver {live.get('driver')}, expected {driver}")
    access = "ReadOnly" if row.get("read_only") is True else None
    if live.get("access") != access:
        problems.append(f"{label}: access {live.get('access')}, expected {access}")
    if live.get("has_credentials"):
        problems.append(f"{label}: the Connector carries credentials")
    if str(row.get("manifest_resource_id") or "") != str(row["id"]):
        problems.append(f"{label}: manifest_resource_id is not the resource")
    key = row.get("managed_key") or (
        f"{PROJECT_KB}:{scope[1]}"
        if scope[0] == "Project" and row.get("type") == "kb" and row.get("native")
        else None
    )
    if live.get("platform_managed") != key:
        problems.append(
            f"{label}: platform marker {live.get('platform_managed')}, expected {key}"
        )
    return problems


def key_problems(rows: list[dict[str, Any]]) -> list[str]:
    """A native knowledge base row holds its key unless another row does."""
    held = {row.get("managed_key") for row in rows if row.get("managed_key")}
    problems = []
    for row in rows:
        native = _uuid(row.get("native")) if row.get("type") == "kb" else None
        if native and row.get("native_exists") and not row.get("managed_key"):
            if f"{PROJECT_KB}:{native}" not in held:
                problems.append(f"{row.get('id')}: native KB without managed_key")
    return problems


#: Every row with what the mapping reads, and its own resource (uid = id).
ROWS_QUERY = """
SELECT coalesce(json_agg(json_build_object(
  'id', d.id, 'name', d.name, 'type', d.type, 'created_by', d.created_by,
  'read_only', d.read_only, 'job_id', d.job_id, 'managed_key', d.managed_key,
  'manifest_resource_id', d.manifest_resource_id,
  'policy_revision', d.policy_revision,
  'native', CASE WHEN d.type = 'kb' THEN d.config->>'native_project_id' END,
  'native_exists', EXISTS (SELECT 1 FROM projects p
                           WHERE p.id::text = d.config->>'native_project_id'),
  'links', (SELECT coalesce(json_agg(pd.project_id::text), '[]'::json)
            FROM project_datasources pd WHERE pd.datasource_id = d.id),
  'resource', (SELECT json_build_object(
      'kind', r.kind, 'scope_kind', r.scope_kind, 'scope_name', r.scope_name,
      'name', r.name, 'linked_id', r.linked_id,
      'driver', r.document->'spec'->>'driver',
      'access', r.document->'spec'->>'access',
      'transport', r.document->'spec'->'config'->>'transport',
      'has_credentials', (r.document->'spec') ? 'credentials',
      'platform_managed', r.platform_managed,
      'version', r.resource_version,
      'display_name', r.document->'metadata'->'annotations'->>'srw.io/display-name',
      'description', r.document->'metadata'->'annotations'->>'srw.io/description',
      'deleted', r.deleted_at IS NOT NULL)
    FROM srw_resources r WHERE r.id = d.id)
) ORDER BY d.id), '[]'::json)
FROM datasources d {where}
"""


def rows(where: str = "") -> list[dict[str, Any]]:
    return sql_json(ROWS_QUERY.format(where=where)) or []


# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------


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
    "preflight: Tilt srw ok; every orchestrator pod serves this checkout's D3a "
    "modules and migrations; the ledger holds 0350-0353; both accounts approved",
    "fixture: Postgres database + SELECT-only role, a project (with its native "
    "KB) owned by --user with --other-user as editor, and one legacy row "
    "written straight to the table and linked to the project",
    "backfill: rerun the deployed migrate_stored_connectors in the "
    "orchestrator pod: the legacy row gets its Connector, nothing deferred, "
    "every datasource has the Connector the mapping names (uid, name, scope, "
    "driver, access, platform marker) or none on the legacy path; no "
    "policy_revision, link or reconcile entry changed",
    "write: create, update, link, unlink and delete through the API keep row "
    "and Connector in step; no password in any stored document; the resource "
    "API refuses to delete a linked Connector",
    "native: the project's KB has managed_key project-kb:<project> and a "
    "platform-managed Connector; policy update and delete 409 as before, "
    "unlink 409, resource API delete 409; nothing changed",
    "shared: a public and a project-linked Postgres connector of the owner "
    "work for --other-user: its stateless session keeps both, the deployed "
    "payload builder delivers both, the agent logs a connection to each",
    "cleanup: sessions titled with the gate id, connectors, project, database "
    "and role; residue check by gate id (rows, live Connectors, sessions, pods)",
]


class ConnectorResourcesGate:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.gate_id = args.gate_id or f"d3a-{secrets.token_hex(5)}"
        self.suffix = self.gate_id.split("-", 1)[1]
        self.report = Report(self.gate_id)
        self.owner = Api(args.user, args.password)
        self.other = Api(args.other_user, args.other_password)
        self.started = (datetime.now(timezone.utc) - timedelta(seconds=30)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        # Everything this run creates, recorded before it is created.
        self.connectors: dict[str, str] = {}  # label -> datasource id
        self.project: str | None = None
        self.thread: str | None = None
        self.pg_name = f"d3a_{self.suffix}"
        self.pg_started = False
        self.owner_id = ""
        self.other_id = ""
        self.pg_password = secret(secrets.token_hex(16))
        self.marker = f"d3a-marker-{self.suffix}"

    # -- naming ------------------------------------------------------------
    def name(self, label: str) -> str:
        return f"{self.gate_id} {label}"

    @property
    def pg_url(self) -> str:
        return (
            f"postgresql://{self.pg_name}:{self.pg_password}@{PG_HOST}:5432/"
            f"{self.pg_name}"
        )

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

    def row(self, label: str) -> dict[str, Any] | None:
        found = rows(f"WHERE d.id = {lit(self.connectors[label])}")
        return found[0] if found else None

    def resource(self, datasource_id: str) -> dict[str, Any] | None:
        return sql_json(
            "SELECT row_to_json(r) FROM (SELECT id, kind, scope_kind, scope_name, "
            "name, resource_version, platform_managed, deleted_at "
            f"FROM srw_resources WHERE id = {lit(datasource_id)}) r"
        )

    def create_connector(self, label: str, body: dict[str, Any]) -> dict[str, Any]:
        """POST a connector as the owner; its id is recorded before checks."""
        status, parsed = self.owner.call(
            "POST", "/api/datasources", {"name": self.name(label), **body}
        )
        if isinstance(parsed, dict) and parsed.get("id"):
            self.connectors[label] = str(parsed["id"])
        if status not in (200, 201) or label not in self.connectors:
            raise GateError(f"{label} create answered HTTP {status}: {parsed}")
        return parsed

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
        pods = self.pods("orchestrator")
        if not pods:
            problems.append("no orchestrator pod")
        for pod in pods:
            name = pod["metadata"]["name"]
            statuses = pod.get("status", {}).get("containerStatuses") or []
            if pod.get("status", {}).get("phase") != "Running" or not all(
                status.get("ready") for status in statuses
            ):
                problems.append(f"{name} is not running and ready")
                continue
            found = json.loads(
                command(
                    K
                    + ["exec", "-i", name, "-c", ORCHESTRATOR_CONTAINER, "--"]
                    + ["python", "-c", _HASH_PROGRAM, POD_ROOT],
                    data=json.dumps(expected_bytes()),
                ).splitlines()[-1]
            )
            if found.get("stale"):
                problems.append(f"{name} stale: {found['stale'][:6]}")
        applied = sql(
            "SELECT count(*) FROM schema_migrations WHERE success AND filename IN ("
            + ", ".join(lit(name) for name in MIGRATIONS)
            + ")"
        )
        if applied != str(len(MIGRATIONS)):
            problems.append(f"{applied} of {len(MIGRATIONS)} D3a migrations applied")
        self.report.check(
            "preflight: the orchestrator serves this checkout's D3a code and the "
            "migrations are applied",
            not problems,
            "; ".join(problems)[:600],
        )
        if problems:
            raise GateError("the deployment does not serve this checkout")
        owner = self.owner.ok("GET", "/api/auth/me")["user"]
        other = self.other.ok("GET", "/api/auth/me")["user"]
        self.owner_id, self.other_id = str(owner["id"]), str(other["id"])
        capabilities = self.owner.ok("GET", "/api/users/me/capabilities")
        can_publish = can_publish_connectors(capabilities)
        accounts_ok = (
            owner.get("is_approved")
            and other.get("is_approved")
            and not other.get("is_admin")
            and self.owner_id != self.other_id
            and can_publish
        )
        self.report.check(
            "preflight: both accounts are approved, the other is not an "
            "administrator, the owner may publish connectors",
            bool(accounts_ok),
            f"owner admin={owner.get('is_admin')} publish={can_publish}, "
            f"other admin={other.get('is_admin')}",
        )
        if not accounts_ok:
            raise GateError("the two accounts cannot run this gate")

    def fixture(self) -> None:
        self.pg_started = True
        sql_script(
            f"CREATE ROLE {self.pg_name} LOGIN PASSWORD '{self.pg_password}' "
            "NOSUPERUSER NOCREATEDB NOCREATEROLE CONNECTION LIMIT 20;\n"
            f"CREATE DATABASE {self.pg_name} OWNER srw;\n"
            f"REVOKE ALL ON DATABASE {self.pg_name} FROM PUBLIC;\n"
            f"GRANT CONNECT ON DATABASE {self.pg_name} TO {self.pg_name};\n"
        )
        sql_script(
            "CREATE TABLE d3a_marker (note text NOT NULL);\n"
            f"INSERT INTO d3a_marker VALUES ('{self.marker}');\n"
            "REVOKE CREATE ON SCHEMA public FROM PUBLIC;\n"
            f"GRANT USAGE ON SCHEMA public TO {self.pg_name};\n"
            f"GRANT SELECT ON d3a_marker TO {self.pg_name};\n",
            database=self.pg_name,
        )
        created = self.owner.ok(
            "POST",
            "/api/projects",
            {
                "name": self.name("project"),
                "description": "D3a connector resources gate (disposable)",
                "user_id": self.owner_id,
            },
        )
        self.project = str(created["id"])
        self.owner.ok(
            "POST",
            f"/api/projects/{self.project}/members",
            {"user_id": self.other_id, "role": "editor"},
        )
        # As an orchestrator without the write-through would write it. Its id
        # is chosen here, so it is recorded before the row exists.
        legacy_id = sql("SELECT gen_random_uuid()")
        if not _UUID_RE.fullmatch(legacy_id):
            raise GateError(f"no fresh id: {legacy_id!r}")
        self.connectors["legacy"] = legacy_id
        sql_script(
            "INSERT INTO datasources (id, name, type, connection_url, created_by, "
            f"scope_mode) VALUES ({lit(legacy_id)}, {lit(self.name('legacy'))}, "
            f"'postgresql', 'postgresql://{PG_HOST}:5432/{self.pg_name}', "
            f"{lit(self.owner_id)}, 'projects');\n"
            "INSERT INTO project_datasources (project_id, datasource_id) VALUES "
            f"({lit(self.project)}, {lit(legacy_id)});\n"
        )
        print(
            f"fixture: database {self.pg_name}, project {self.project}, "
            f"legacy row {legacy_id}",
            flush=True,
        )

    def backfill(self) -> None:
        legacy = self.connectors["legacy"]
        before = rows()
        queue_before = sql(
            "SELECT coalesce(json_agg(row_to_json(q) ORDER BY q.project_id), '[]') "
            "FROM (SELECT project_id, policy_revision, claim_token, attempts "
            "FROM datasource_project_reconcile_queue "
            f"WHERE datasource_id = {lit(legacy)}) q"
        )
        counts = in_orchestrator(_BACKFILL_PROGRAM, {})
        self.report.check(
            "backfill: the rerun wrote the legacy row's Connector and deferred nothing",
            counts.get("created", 0) >= 1 and counts.get("deferred") == 0,
            json.dumps(counts),
        )
        after = rows()
        by_id = {row["id"]: row for row in after}
        problems = [
            problem
            for row in after
            for problem in row_problems(row, exact_name=row["id"] == legacy)
        ] + key_problems(after)
        self.report.check(
            f"backfill: all {len(after)} datasources map to the Connector the "
            "mapping names (uid, name, scope, driver, access, marker) or stay on "
            "the legacy path",
            not problems,
            "; ".join(problems[:5]),
        )
        changed = [
            row["id"]
            for row in before
            if row["id"] in by_id
            and (row["policy_revision"], sorted(row["links"]))
            != (by_id[row["id"]]["policy_revision"], sorted(by_id[row["id"]]["links"]))
        ]
        queue_after = sql(
            "SELECT coalesce(json_agg(row_to_json(q) ORDER BY q.project_id), '[]') "
            "FROM (SELECT project_id, policy_revision, claim_token, attempts "
            "FROM datasource_project_reconcile_queue "
            f"WHERE datasource_id = {lit(legacy)}) q"
        )
        self.report.check(
            "backfill: no policy_revision, project link or reconcile entry changed",
            not changed and queue_before == queue_after,
            f"changed rows {changed[:5]}"
            if changed
            else ("" if queue_before == queue_after else "reconcile queue changed"),
        )

    def write_through(self) -> None:
        created = self.create_connector(
            "orders",
            {
                "type": "postgresql",
                "connection_url": self.pg_url,
                "scope_mode": "all",
            },
        )
        ds = str(created["id"])
        row = self.row("orders")
        resource = (row or {}).get("resource") or {}
        problems = row_problems(row, exact_name=True) if row else ["no row"]
        self.report.check(
            "write: create writes the row's Connector (Account scope, "
            "srw.postgresql/v1, no credentials)",
            not problems and resource.get("version") == 1,
            "; ".join(problems[:5]) or f"version {resource.get('version')}",
        )
        stored = sql(
            "SELECT coalesce(string_agg(document::text, ''), '') FROM "
            f"srw_resource_revisions WHERE resource_id = {lit(ds)}"
        )
        self.report.check(
            "write: no stored Connector revision holds the Postgres password",
            bool(stored) and self.pg_password not in stored,
        )
        self.owner.ok(
            "PUT",
            f"/api/datasources/{ds}",
            {
                "name": self.name("orders eu"),
                "description": "D3a gate orders",
                "read_only": True,
            },
        )
        row = self.row("orders")
        resource = row["resource"]
        self.report.check(
            "write: update rewrites the Connector (display name, description, "
            "access ReadOnly), its name stays",
            resource.get("version") == 2
            and resource.get("display_name") == self.name("orders eu")
            and resource.get("description") == "D3a gate orders"
            and resource.get("access") == "ReadOnly"
            and resource.get("name", "").startswith(name_stem(self.name("orders"))),
            json.dumps({k: resource.get(k) for k in ("version", "access", "name")}),
        )
        self.owner.ok("POST", f"/api/projects/{self.project}/datasources/{ds}", {})
        linked = self.row("orders")
        self.owner.ok("DELETE", f"/api/projects/{self.project}/datasources/{ds}")
        unlinked = self.row("orders")
        self.report.check(
            "write: link and unlink change the row's links, not its Connector",
            linked["links"] == [self.project]
            and unlinked["links"] == []
            and linked["resource"]["version"] == unlinked["resource"]["version"] == 2
            and not row_problems(unlinked),
            f"versions {linked['resource']['version']}, "
            f"{unlinked['resource']['version']}",
        )
        status, body = self.owner.call(
            "DELETE",
            f"/api/resources/{ds}?expected_version={unlinked['resource']['version']}",
        )
        self.report.check(
            "write: the resource API refuses to delete a linked Connector",
            status == 409 and body.get("detail") == LINKED_CONNECTOR_MESSAGE,
            f"HTTP {status}: {str(body.get('detail'))[:120]}",
        )
        self.owner.ok("DELETE", f"/api/datasources/{ds}")
        resource = self.resource(ds) or {}
        tombstone = sql(
            f"SELECT count(*) FROM datasource_tombstones WHERE id = {lit(ds)}"
        )
        self.report.check(
            "write: delete removes the row, retires its Connector and keeps the "
            "tombstone",
            sql(f"SELECT count(*) FROM datasources WHERE id = {lit(ds)}") == "0"
            and resource.get("deleted_at") is not None
            and tombstone == "1",
            f"resource deleted_at {resource.get('deleted_at')}, tombstones {tombstone}",
        )
        self.connectors.pop("orders")

    def native(self) -> None:
        native = sql(
            "SELECT id FROM datasources WHERE type = 'kb' AND "
            f"config->>'native_project_id' = {lit(self.project)} "
            "ORDER BY created_at LIMIT 1"
        )
        if not _UUID_RE.fullmatch(native or ""):
            self.report.check(
                "native: the project has its own knowledge base",
                False,
                "no native KB row (is Gitea up?)",
            )
            return
        found = rows(f"WHERE d.id = {lit(native)}")[0]
        key = f"{PROJECT_KB}:{self.project}"
        problems = row_problems(found)
        self.report.check(
            "native: managed_key project-kb:<project> and a platform-managed "
            "Connector in the project's scope",
            found.get("managed_key") == key
            and (found.get("resource") or {}).get("platform_managed") == key
            and not problems,
            "; ".join(problems[:5]) or f"managed_key {found.get('managed_key')}",
        )
        refusals = [
            (
                "policy update",
                self.owner.call(
                    "PUT",
                    f"/api/datasources/{native}",
                    {
                        "auto_attach": False,
                        "policy_revision": found["policy_revision"],
                    },
                ),
                NATIVE_POLICY_DETAIL,
            ),
            (
                "delete",
                self.owner.call("DELETE", f"/api/datasources/{native}"),
                NATIVE_DELETE_DETAIL,
            ),
            (
                "unlink",
                self.owner.call(
                    "DELETE", f"/api/projects/{self.project}/datasources/{native}"
                ),
                NATIVE_UNLINK_DETAIL,
            ),
            (
                "resource delete",
                self.owner.call(
                    "DELETE",
                    f"/api/resources/{native}?expected_version="
                    f"{found['resource']['version']}",
                ),
                PLATFORM_MANAGED_MESSAGE,
            ),
        ]
        wrong = [
            f"{label}: HTTP {status} {str(body.get('detail'))[:80]}"
            for label, (status, body), detail in refusals
            if status != 409 or body.get("detail") != detail
        ]
        self.report.check(
            "native: policy update, delete, unlink and resource delete answer 409",
            not wrong,
            "; ".join(wrong),
        )
        again = rows(f"WHERE d.id = {lit(native)}")
        unchanged = (
            again
            and again[0]["policy_revision"] == found["policy_revision"]
            and again[0]["links"] == found["links"]
            and again[0]["resource"] == found["resource"]
        )
        self.report.check(
            "native: row and Connector are unchanged after the refusals",
            bool(unchanged),
        )

    def shared(self) -> None:
        self.create_connector(
            "public",
            {
                "type": "postgresql",
                "connection_url": self.pg_url,
                "scope_mode": "all",
                "is_global": True,
                "read_only": True,
            },
        )
        self.create_connector(
            "linked",
            {
                "type": "postgresql",
                "connection_url": self.pg_url,
                "scope_mode": "projects",
                "project_ids": [self.project],
            },
        )
        ids = [self.connectors["public"], self.connectors["linked"]]
        status, created = self.other.call(
            "POST",
            "/api/persistent/threads",
            {
                "title": f"D3a connector resources gate {self.gate_id}",
                "permission_mode": "autonomous",
                "project_id": self.project,
                "datasource_ids": ids,
                "config_override": {"workspace": {"backend": "sandbox"}},
                "model": self.args.model,
            },
        )
        if isinstance(created, dict) and (
            created.get("thread_id") or created.get("id")
        ):
            self.thread = str(created.get("thread_id") or created["id"])
        selection = (
            sql_json(
                "SELECT metadata->'datasource_ids' FROM threads "
                f"WHERE id = {lit(self.thread)}"
            )
            if self.thread
            else None
        )
        self.report.check(
            "shared: the other account's session selecting both connectors is "
            "admitted and keeps both",
            status in (200, 201) and sorted(selection or []) == sorted(ids),
            f"HTTP {status}: {str(created)[:160]}" if status not in (200, 201) else "",
        )
        if not self.thread:
            return
        result = in_orchestrator(
            _PAYLOAD_PROGRAM, {"datasource_ids": ids, "project": self.project}
        )
        names = sorted(self.name(label) for label in ("public", "linked"))
        self.report.check(
            "shared: the deployed payload builder delivers both, with their "
            "endpoint, for that session",
            result.get("payload") == names and result.get("with_endpoint") == names,
            json.dumps(result),
        )
        previous = (self.queue(self.thread) or ("", 0, 0))[1]
        self.other.ok(
            "POST",
            f"/api/persistent/threads/{self.thread}/input",
            {"content": "Reply with the single word ready."},
        )

        def answered() -> bool:
            current = self.queue(self.thread)
            return bool(
                current
                and current[0] == "done"
                and current[1] > previous
                and current[1] == current[2]
            )

        wait_for("turn answered", answered, timeout=self.args.turn_timeout)
        lines = self.agent_log_lines(names)
        connected = [
            name
            for name in names
            if any(
                f"Connected to postgresql datasource: {name}" in line for line in lines
            )
        ]
        self.report.check(
            "shared: the agent logs a connection to the public and the "
            "project-linked connector",
            connected == names,
            f"connected {connected}",
        )

    def delete_thread(self, thread: str | None) -> bool:
        def gone() -> bool:
            status, _body = self.other.call(
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

    def titled_threads(self) -> list[str]:
        rows_ = sql(
            "SELECT id FROM threads WHERE "
            f"position({lit(self.gate_id)} in coalesce(title, '')) > 0"
        )
        return [row for row in rows_.splitlines() if _UUID_RE.fullmatch(row)]

    def cleanup(self) -> list[str]:
        problems: list[str] = []

        def step(label: str, action: Callable[[], Any]) -> None:
            try:
                if action() is False:
                    problems.append(label)
            except GateError as exc:
                problems.append(f"{label} ({exc})")

        for thread in dict.fromkeys(
            [t for t in (self.thread,) if t] + self.titled_threads()
        ):
            step(f"delete session {thread}", lambda t=thread: self.delete_thread(t))
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
        live = sql(
            "SELECT count(*) FROM srw_resources WHERE kind = 'Connector' AND "
            f"deleted_at IS NULL AND name LIKE {lit(self.gate_id + '-%')}"
        )
        if live != "0":
            left.append(f"{live} live Connector resources")
        if self.project and (
            sql(f"SELECT count(*) FROM projects WHERE id = {lit(self.project)}") != "0"
        ):
            left.append(f"project {self.project}")
        for thread in [t for t in (self.thread,) if t] + titled:
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
                left.append(f"pods of {thread}")
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
        return left

    # -- run ---------------------------------------------------------------
    def run(self) -> int:
        try:
            self.preflight()
            self.fixture()
            self.backfill()
            self.write_through()
            self.native()
            self.shared()
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
                            "thread": self.thread,
                            "database": self.pg_name if self.pg_started else None,
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
    parser.add_argument("--other-user", default="dev-user-1")
    parser.add_argument("--other-password", default="srw-k3d-dev-usr1")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--turn-timeout", type=int, default=420)
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
        raise SafetyError("--gate-id must be d3a- followed by 10 hex digits")
    if not _MODEL_RE.fullmatch(args.model):
        raise SafetyError("model id is malformed")
    for user in (args.user, args.other_user):
        if not _USER_RE.fullmatch(user):
            raise SafetyError("user name is malformed")
    if args.user == args.other_user:
        raise SafetyError("--other-user must be a second account")
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
    return ConnectorResourcesGate(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
