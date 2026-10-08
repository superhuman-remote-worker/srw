#!/usr/bin/env python3
"""Local k3d gate for connector drivers D3c: selecting Connectors.

Design: knowledge-base/knowledge/features/connector_drivers.md, "Datasources
become Connectors" ("Project defaults", "Selection") and Track D, D3 (the D3c
part of its gate).
Template: scripts/k3d-connector-resources-gate.py (D3a) -- the same safety
envelope: dry-run by default, the exact k3d-srw/srw context, passwords only on
``kubectl exec -i`` stdin and scrubbed from every printed line, ``cap_memory()``
in every program the gate runs in a pod, and a cleanup in ``finally`` that
touches only what this run created, followed by a residue check by gate id.

Fixtures (all disposable, all named after the gate id):

  client      ``<gate id>-oauth``, a public Keycloak client of the srw realm
              with direct access grants and the profile, email and roles
              scopes, which both accounts log in with: an admin-cli token
              carries no roles, and the JIT path then records the owner as no
              administrator (the D3a gate's fixture)
  account     the second account: a Keycloak user of the srw realm named the
              gate id (``<gate id>@example.invalid``), created inside the
              orchestrator pod with the pod's own Keycloak admin credentials
              (they never leave the pod; the user's password goes over stdin),
              and its app row admitted before its first login -- so the login
              takes the existing-account path and no cloud or Gitea account is
              provisioned for it. ``--other-user``/``--other-password`` name an
              existing approved non-administrator instead
  postgres    a database and a login role on srw-postgres (SELECT on one
              marker table) that the gate's Postgres connectors point at
  project     one project owned by the owner account (``--user``), with its
              native knowledge base when Gitea provisions one, and the second
              account added as an editor
  connectors  of the owner: ``public`` (published), ``linked`` (linked to the
              project), ``private`` (neither) and ``extra``/``doomed`` for the
              link checks; of the other account: ``own``

Checks (each printed PASS/FAIL; the exit status is 0 only if all pass):

  preflight   Tilt reports the srw resource ``ok``; every orchestrator pod
              serves this checkout's D3c modules byte for byte; the ledger
              holds 0380 as applied
  account     both accounts are approved, the second is not an
              administrator, the owner may publish
  manifest    the project's manifest lists exactly its links, each a ref to
              the connector's Connector resource (name and scope), after the
              fixture, after a link, an unlink, a connector created linked and
              its delete; no inline ``srw.datasource/v1`` child is left live
  sessions    the other account creates one session selecting its own, the
              public, the project-linked connector and the project knowledge
              base through ``execution.connectors`` (by name with ``me``, by
              uid, by name in the owner's Account, by name with the scope
              left to the project) and one through ``datasource_ids`` with the
              same ids: both are admitted with the same selection record, and
              the deployed attach path (re-authorization for the owner of the
              session, exact resolution) and payload builder deliver the same
              connectors, the public and the project-linked one included; the
              preview answers the same ids both ways
  jobs        the same for two jobs (job_datasources, the selection record,
              the deployed dispatch-time authorization and resolution, the
              payload builder); both jobs are deleted right after
  conflicts   execution.connectors with datasource_ids (even []) or with
              use_datasource_defaults is a 400 on the preview, session and
              job create, and creates nothing
  refusals    a ref to the owner's private connector (by name, by uid), a ref
              that names nothing in the owner's Account, a ref into a project
              that does not exist, and the private id in datasource_ids all
              answer the same 403 with the same body, on the preview and on
              job create
  defaults    the project's connector defaults: a member reads the knowledge
              base as the first, platform-owned entry; an editor cannot write
              them; a connector that is not linked is a 422; the owner's
              linked default attaches to a member's session preview that takes
              its defaults, and stops once cleared
  cleanup     nothing this run created is left: sessions titled and jobs
              described with the gate id (and their pods), connectors (live
              rows and live Connector resources by gate id), the project, the
              second account (its app row through the user API, then its
              Keycloak user, found by the gate id), the OAuth client, the
              database and the role

Run with the repository venv on the k3d-srw cluster, alone: this is a
mutating gate. The owner must be able to publish (an administrator or the
``public_datasources`` grant) and, for the disposable second account, be an
administrator (it deletes that account's app row); an account named with
``--other-user`` must be an approved non-administrator. The second account's
sessions ask for ``auto_accept``, the most an ungranted user may pick.

  .venv/bin/python scripts/k3d-connector-selection-gate.py           # plan
  .venv/bin/python scripts/k3d-connector-selection-gate.py \\
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
PINNED_THREAD_LABEL = "srw.io/thread-id"
_SELECTOR = (
    "app.kubernetes.io/instance=srw,app.kubernetes.io/name=superhuman-remote-worker"
)
_GATE_ID_RE = re.compile(r"d3c-[0-9a-f]{10}\Z")
_MODEL_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@-]{0,199}\Z")
_USER_RE = re.compile(r"[a-z][a-z0-9._-]{0,63}\Z")
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")

MIGRATIONS = ("0380_project_connector_defaults.sql",)
#: What the orchestrator must serve byte for byte for D3c.
SERVED = (
    "src/orchestrator/schemas/execution_selection.py",
    "src/orchestrator/schemas/job_create.py",
    "src/orchestrator/schemas/thread_admission.py",
    "src/orchestrator/schemas/projects.py",
    "src/orchestrator/services/connector_refs.py",
    "src/orchestrator/services/job_admission.py",
    "src/orchestrator/services/job_admission_datasources.py",
    "src/orchestrator/services/thread_admission.py",
    "src/orchestrator/services/project_connectors.py",
    "src/orchestrator/services/project_connector_defaults.py",
    "src/orchestrator/services/manifest_projects.py",
    "src/orchestrator/services/manifest_resources.py",
    "src/orchestrator/services/manifest_resolution.py",
    "src/orchestrator/services/manifest_authority.py",
    "src/orchestrator/services/manifest_store.py",
    "src/orchestrator/services/manifest_connectors.py",
    "src/orchestrator/services/datasource_policy.py",
    "src/orchestrator/database/postgres.py",
    "src/orchestrator/routers/projects.py",
    "src/orchestrator/application/jobs.py",
    "src/orchestrator/application/lifecycle.py",
    *(f"src/orchestrator/database/migrations/app/{name}" for name in MIGRATIONS),
)
DATASOURCE_DRIVER = "srw.datasource/v1"
#: The disposable second account's email domain (RFC 2606, never delivered).
ACCOUNT_DOMAIN = "example.invalid"
#: The second account's sessions: the most an approved user without grants
#: may pick (``shared.runtime.core.capability_grants.CATALOG``).
SESSION_PERMISSION_MODE = "auto_accept"
GENERIC_UNAVAILABLE_DETAIL = "One or more selected connectors are unavailable"
#: The start of every selector-conflict 400 (``connector_refs``).
SELECTOR_CONFLICT_PREFIX = "execution.connectors and "
#: Selection-record fields that differ between two creations by design.
_VOLATILE = frozenset({"materialized_at"})
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

# The deployed delivery path over this run's sessions and jobs: a session's
# attach-time re-authorization for its owner and exact resolution, a job's
# dispatch-time revalidation and exact resolution, then the payload builder.
# It prints ids, names and the selection record only: the resolved rows carry
# decrypted credentials.
_BINDINGS_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import asyncio, json, sys
from functools import partial
from types import SimpleNamespace
from uuid import UUID
from orchestrator.application.preparation import datasource_payload_dependencies
from orchestrator.application.settings import DeploymentSettings
from orchestrator.database.postgres import PostgresDB
from orchestrator.services import job_datasource_selection as jds
from orchestrator.services.agent_datasource_payload import build_datasources_payload
from orchestrator.services.connector_drivers.registry import builtin_connector_drivers
from orchestrator.services.thread_datasource_authorization import (
    ThreadDatasourceAuthorizationDependencies,
    authorize_thread_datasource_selection,
    resolve_authorized_thread_datasources,
)
from orchestrator.services.workspace_tier_policy import backend_from_override
cap_memory()
request = json.loads(sys.stdin.readline())
VOLATILE = {"materialized_at"}
def loads(value):
    return json.loads(value) if isinstance(value, str) else (value or {})
def record(selection):
    return {k: v for k, v in (selection or {}).items() if k not in VOLATILE}
async def main():
    db = PostgresDB(
        min_connections=1, max_connections=2,
        server_settings={"default_transaction_read_only": "on"},
    )
    await db.connect()
    project = request["project"]
    async def project_ids(_thread_id):
        return [project]
    thread_deps = ThreadDatasourceAuthorizationDependencies(
        store=db, thread_project_ids=project_ids
    )
    authorize = partial(authorize_thread_datasource_selection, dependencies=thread_deps)
    holder = {}
    async def revalidate(job):
        return await jds.revalidate_job_datasource_selection(
            job, dependencies=holder["deps"]
        )
    holder["deps"] = jds.JobDatasourceSelectionDependencies(
        store=db,
        authorize_thread_datasource_selection=authorize,
        backend_from_override=backend_from_override,
        revalidate_selection=revalidate,
    )
    payload_deps = datasource_payload_dependencies(
        SimpleNamespace(
            connector_drivers=builtin_connector_drivers(),
            settings=DeploymentSettings.from_environment(),
        )
    )
    out = {}
    try:
        for label, thread_id in request.get("threads", {}).items():
            row = await db.fetchrow(
                "SELECT id, user_id, metadata FROM threads WHERE id=$1",
                UUID(thread_id),
            )
            thread = dict(row)
            thread["metadata"] = loads(thread["metadata"])
            ids = thread["metadata"].get("datasource_ids") or []
            rows = await resolve_authorized_thread_datasources(
                thread, ids, target_project_ids=[project], dependencies=thread_deps
            )
            payload = build_datasources_payload(rows, dependencies=payload_deps) or []
            out[label] = {
                "ids": [str(v) for v in ids],
                "selection": record(thread["metadata"].get("datasource_selection")),
                "resolved": sorted(str(r["id"]) for r in rows),
                "payload": sorted(e.get("name") for e in payload),
            }
        for label, job_id in request.get("jobs", {}).items():
            job = await db.get_job(job_id)
            ids = await db.list_job_datasource_ids(job_id)
            rows = await jds.resolve_authorized_job_datasources(
                job, dependencies=holder["deps"]
            )
            payload = build_datasources_payload(rows, dependencies=payload_deps) or []
            out[label] = {
                "ids": [str(v) for v in ids],
                "selection": record(loads(job.get("context")).get("datasource_selection")),
                "resolved": sorted(str(r["id"]) for r in rows),
                "payload": sorted(e.get("name") for e in payload),
            }
    finally:
        await db.close()
    print(json.dumps(out))
asyncio.run(main())
"""
)

# The run's Keycloak fixtures (the D3a gate's program, unchanged but for the
# account's first name), through the orchestrator's own admin
# credentials (KEYCLOAK_ADMIN_*, the KeycloakGroupSync ones): they stay in the
# pod and are never printed.
#
#   client  ``<gate id>-oauth``: a public client with direct access grants and
#           the profile, email and roles scopes, so a gate login carries the
#           claims a cockpit login does. (admin-cli carries none: its tokens
#           have no realm_access, and every such login makes the JIT path
#           record the account as no administrator.) Marked with the gate id.
#   user    the disposable second account, named the gate id. The local
#           realm's user profile may drop custom attributes, so its ownership
#           is the gate id in username and email, not a marker.
#
# Every action finds both by exact name; delete removes only what carries this
# run's marker or email (and its recorded id, once known).
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
        "firstName": "D3c",
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
        # The run's own OAuth client, once it exists (see _KEYCLOAK_PROGRAM).
        self.client_id: str | None = None

    def call(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        if not self.client_id:
            raise GateError("no OAuth client to log in with")
        result = in_orchestrator(
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
# What the slice says (pure, unit-tested)
# ---------------------------------------------------------------------------


def listed_ids(
    entries: dict[str, Any],
    lookup: Callable[[str, str, str], str | None],
    *,
    project: str,
) -> tuple[set[str], list[str]]:
    """The connector ids a Project's ``resources.connectors`` lists, and how
    its entries break the D3c form.

    ``lookup(kind, scope, name)`` is the linked id of the live Connector
    resource at that identity, or None. Every entry should be a ref with an
    explicit scope to a datasource's Connector; an inline
    ``srw.datasource/v1`` entry is the row fallback and is reported, since
    every connector of this run has a resource.
    """
    ids: set[str] = set()
    problems: list[str] = []
    for alias, entry in sorted((entries or {}).items()):
        if "ref" in entry:
            ref = entry["ref"]
            scope = ref.get("scope") or {"kind": "Project", "name": project}
            if "scope" not in ref:
                problems.append(f"{alias}: ref without an explicit scope")
            found = lookup(scope["kind"], scope["name"], ref.get("name", ""))
            if found:
                ids.add(found)
                if alias != ref.get("name") and not alias.startswith("connector-"):
                    problems.append(f"{alias}: alias is not the resource name")
            else:
                problems.append(f"{alias}: ref names no datasource Connector")
        elif (entry.get("inline") or {}).get("driver") == DATASOURCE_DRIVER:
            value = str((entry["inline"].get("config") or {}).get("datasourceId"))
            ids.add(value)
            problems.append(f"{alias}: inline {DATASOURCE_DRIVER} entry")
        else:
            problems.append(f"{alias}: not a datasource entry")
    return ids, problems


def same_bindings(a: dict[str, Any], b: dict[str, Any]) -> list[str]:
    """How two creations' bindings differ (ids, selection record, resolved
    ids, payload names); empty when they are the same."""
    differences = []
    for key in ("ids", "selection", "resolved", "payload"):
        left, right = a.get(key), b.get(key)
        if isinstance(left, dict):
            left = {k: v for k, v in left.items() if k not in _VOLATILE}
            right = {k: v for k, v in (right or {}).items() if k not in _VOLATILE}
        if left != right:
            differences.append(f"{key}: {left!r} != {right!r}"[:300])
    return differences


def connector_refs(refs: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """An ``execution`` block from alias -> ref."""
    return {"connectors": {alias: {"ref": ref} for alias, ref in refs.items()}}


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
    "preflight: Tilt srw ok; every orchestrator pod serves this checkout's D3c "
    "modules and migration; the ledger holds 0380",
    "account: in the orchestrator pod, with its Keycloak admin credentials, "
    "an OAuth client <gate id>-oauth (roles in the token) for both logins and "
    "the second account -- a Keycloak user named the gate id, its app row "
    "admitted before its first login (no cloud or Gitea account), or the "
    "existing --other-user; both accounts approved, the second no admin, the "
    "owner may publish",
    "fixture: Postgres database + SELECT-only role, a project (with its native "
    "KB when Gitea provisions it) owned by --user with the second account as "
    "editor, the owner's public, project-linked and private connectors, the "
    "second account's own connector",
    "manifest: the project's manifest lists exactly its links as Connector "
    "refs after the fixture, a link, an unlink, a connector created linked and "
    "its delete; no inline srw.datasource/v1 child is live",
    "sessions: the second account creates a session through execution.connectors "
    "(name + me, uid, name in the owner's Account, name with the project's "
    "scope) and one through datasource_ids: the same selection record, and the "
    "deployed attach path and payload builder deliver the same connectors, "
    "the public and the project-linked one included; the preview agrees",
    "jobs: the same for two jobs (job_datasources, selection record, "
    "dispatch-time authorization and resolution, payload builder); both jobs "
    "are deleted right after",
    "conflicts: execution.connectors with datasource_ids or "
    "use_datasource_defaults is a 400 on preview, session and job create, and "
    "creates nothing",
    "refusals: refs to a private connector (name, uid), to nothing, into a "
    "missing project, and the private id in datasource_ids answer the same "
    "403 body, on the preview and on job create",
    "defaults: the project's connector defaults list the knowledge base "
    "first; an editor cannot write them; an unlinked connector is a 422; a "
    "linked default reaches a member's defaults preview until cleared",
    "cleanup: sessions titled and jobs described with the gate id (and their "
    "pods), connectors, project, the disposable account (app row, then "
    "Keycloak user), the OAuth client, database and role; residue check by "
    "gate id (rows, live Connectors, sessions, jobs, pods, account, client)",
]


class ConnectorSelectionGate:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.gate_id = args.gate_id or f"d3c-{secrets.token_hex(5)}"
        self.suffix = self.gate_id.split("-", 1)[1]
        self.report = Report(self.gate_id)
        self.owner = Api(args.user, args.password)
        # The second account: disposable (named the gate id) unless named.
        self.disposable = args.other_user is None
        self.other_email = f"{self.gate_id}@{ACCOUNT_DOMAIN}"
        self.other = Api(
            args.other_user or self.gate_id,
            args.other_password or secrets.token_urlsafe(24),
        )
        # Everything this run creates, recorded before it is checked.
        self.oauth_client = f"{self.gate_id}-oauth"
        self.client_started = False  # the run's Keycloak OAuth client
        self.client_uuid: str | None = None
        self.account_started = False  # the disposable Keycloak user
        self.account_keycloak_id: str | None = None
        self.account_row = False  # its app row (id = self.other_id)
        self.connectors: dict[str, str] = {}  # label -> datasource id
        self.connector_api: dict[str, str] = {}  # label -> "owner" | "other"
        self.project: str | None = None
        self.kb: str | None = None
        self.threads: dict[str, str] = {}
        self.jobs: dict[str, str] = {}
        self.pg_name = f"d3c_{self.suffix}"
        self.pg_started = False
        self.owner_id = ""
        self.other_id = ""
        self.pg_password = secret(secrets.token_hex(16))

    # -- naming ------------------------------------------------------------
    def name(self, label: str) -> str:
        return f"{self.gate_id} {label}"

    def title(self, label: str) -> str:
        return f"D3c connector selection gate {self.gate_id} {label}"

    @property
    def pg_url(self) -> str:
        return (
            f"postgresql://{self.pg_name}:{self.pg_password}@{PG_HOST}:5432/"
            f"{self.pg_name}"
        )

    def api(self, who: str) -> Api:
        return self.owner if who == "owner" else self.other

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

    def create_connector(
        self, label: str, body: dict[str, Any], *, who: str = "owner"
    ) -> str:
        """POST a connector; its id is recorded before anything checks it."""
        status, parsed = self.api(who).call(
            "POST", "/api/datasources", {"name": self.name(label), **body}
        )
        if isinstance(parsed, dict) and parsed.get("id"):
            self.connectors[label] = str(parsed["id"])
            self.connector_api[label] = who
        if status not in (200, 201) or label not in self.connectors:
            raise GateError(f"{label} create answered HTTP {status}: {parsed}")
        return self.connectors[label]

    def postgres(self, **extra: Any) -> dict[str, Any]:
        return {"type": "postgresql", "connection_url": self.pg_url, **extra}

    def resource_ref(self, datasource_id: str) -> dict[str, Any]:
        """The Connector resource's name and scope (uid = datasource id)."""
        found = sql_json(
            "SELECT json_build_object('name', name, 'scope', json_build_object("
            "'kind', scope_kind, 'name', scope_name)) FROM srw_resources "
            f"WHERE id = {lit(datasource_id)} AND kind = 'Connector' "
            "AND deleted_at IS NULL"
        )
        if not found:
            raise GateError(f"connector {datasource_id} has no live Connector")
        return found

    def lookup(self, kind: str, scope: str, name: str) -> str | None:
        found = sql(
            "SELECT linked_id FROM srw_resources WHERE kind = 'Connector' AND "
            f"scope_kind = {lit(kind)} AND scope_name = {lit(scope)} AND "
            f"name = {lit(name)} AND deleted_at IS NULL"
        )
        return found if _UUID_RE.fullmatch(found or "") else None

    def manifest_entries(self) -> dict[str, Any]:
        document = sql_json(
            "SELECT document FROM srw_resources WHERE kind = 'Project' AND "
            f"linked_id = {lit(self.project)} AND deleted_at IS NULL"
        )
        if not document:
            raise GateError("the project has no live Project manifest")
        return (document.get("spec", {}).get("resources") or {}).get("connectors") or {}

    def links(self) -> set[str]:
        out = sql(
            "SELECT datasource_id FROM project_datasources WHERE "
            f"project_id = {lit(self.project)}"
        )
        return {line for line in out.splitlines() if _UUID_RE.fullmatch(line)}

    def titled_threads(self) -> list[str]:
        out = sql(
            "SELECT id FROM threads WHERE "
            f"position({lit(self.gate_id)} in coalesce(title, '')) > 0"
        )
        return [row for row in out.splitlines() if _UUID_RE.fullmatch(row)]

    def described_jobs(self) -> list[str]:
        out = sql(
            "SELECT id FROM jobs WHERE "
            f"position({lit(self.gate_id)} in coalesce(description, '')) > 0"
        )
        return [row for row in out.splitlines() if _UUID_RE.fullmatch(row)]

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
            problems.append(f"{applied} of {len(MIGRATIONS)} D3c migrations applied")
        self.report.check(
            "preflight: the orchestrator serves this checkout's D3c code and the "
            "migration is applied",
            not problems,
            "; ".join(problems)[:600],
        )
        if problems:
            raise GateError("the deployment does not serve this checkout")

    def keycloak(self, action: str) -> dict[str, Any]:
        """Run one action of the in-pod Keycloak program on this run's
        OAuth client and disposable user."""
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
        result = in_orchestrator(_KEYCLOAK_PROGRAM, payload, timeout=120)
        if result.get("error"):
            raise GateError(f"Keycloak {action}: {result['error']}")
        return result

    @staticmethod
    def receipt(created: dict[str, Any], what: str) -> str:
        """The created object's id: its Location, and the one owned match."""
        found = created.get("found") or []
        made = created.get("id") or (found[0] if len(found) == 1 else "")
        if not _UUID_RE.fullmatch(made or "") or found != [made]:
            raise GateError(f"no Keycloak receipt for the {what}: {created}")
        return made

    def mint_client(self) -> None:
        """The run's OAuth client: its logins carry the claims a cockpit login
        does, so the JIT path keeps the owner an administrator."""
        self.client_started = True
        created = self.keycloak("create-client")
        if created.get("exists"):
            # Not this run's: never adopted, never cleaned up.
            self.client_started = False
            raise GateError(f"a Keycloak client {self.oauth_client} already exists")
        self.client_uuid = self.receipt(created, "OAuth client")
        self.owner.client_id = self.other.client_id = self.oauth_client

    def mint_account(self) -> None:
        """The disposable second account: its Keycloak user, then its app row,
        admitted by the owner before the first login, so the login takes the
        existing-account path and provisions no cloud or Gitea account."""
        self.account_started = True
        created = self.keycloak("create-user")
        if created.get("exists"):
            # Not this run's: never adopted, never cleaned up.
            self.account_started = False
            raise GateError(f"a Keycloak user {self.other.username} already exists")
        keycloak_id = self.receipt(created, "account")
        self.account_keycloak_id = keycloak_id
        app_id = sql("SELECT gen_random_uuid()")
        if not _UUID_RE.fullmatch(app_id):
            raise GateError(f"no fresh id: {app_id!r}")
        self.other_id = app_id
        self.account_row = True
        sql_script(
            "INSERT INTO users (id, display_name, email, keycloak_sub, "
            "preferred_username, is_approved, approved_at, approved_by) VALUES ("
            f"{lit(app_id)}, {lit(self.other.username)}, {lit(self.other_email)}, "
            f"{lit(keycloak_id)}, {lit(self.other.username)}, true, now(), "
            f"{lit(self.owner_id)});\n"
        )

    def accounts(self) -> None:
        self.mint_client()
        owner = self.owner.ok("GET", "/api/auth/me")["user"]
        self.owner_id = str(owner["id"])
        capabilities = self.owner.ok("GET", "/api/users/me/capabilities")
        can_publish = can_publish_connectors(capabilities)
        if self.disposable:
            if not owner.get("is_admin"):
                raise GateError(
                    "the disposable second account needs an administrator owner"
                )
            self.mint_account()
        other = self.other.ok("GET", "/api/auth/me")["user"]
        if self.disposable and str(other["id"]) != self.other_id:
            raise GateError(
                f"the second account logged in as {other['id']}, not the admitted "
                f"row {self.other_id}"
            )
        self.other_id = str(other["id"])
        accounts_ok = (
            owner.get("is_approved")
            and other.get("is_approved")
            and not other.get("is_admin")
            and self.owner_id != self.other_id
            and can_publish
        )
        self.report.check(
            "account: both accounts are approved, the second is not an "
            "administrator, the owner may publish connectors",
            bool(accounts_ok),
            f"owner admin={owner.get('is_admin')} publish={can_publish}, "
            f"second {'disposable' if self.disposable else 'named'} "
            f"admin={other.get('is_admin')}",
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
            "CREATE TABLE d3c_marker (note text NOT NULL);\n"
            f"INSERT INTO d3c_marker VALUES ('d3c-marker-{self.suffix}');\n"
            "REVOKE CREATE ON SCHEMA public FROM PUBLIC;\n"
            f"GRANT USAGE ON SCHEMA public TO {self.pg_name};\n"
            f"GRANT SELECT ON d3c_marker TO {self.pg_name};\n",
            database=self.pg_name,
        )
        created = self.owner.ok(
            "POST",
            "/api/projects",
            {
                "name": self.name("project"),
                "description": "D3c connector selection gate (disposable)",
                "user_id": self.owner_id,
            },
        )
        self.project = str(created["id"])
        self.owner.ok(
            "POST",
            f"/api/projects/{self.project}/members",
            {"user_id": self.other_id, "role": "editor"},
        )
        kb = sql(
            "SELECT id FROM datasources WHERE type = 'kb' AND "
            f"config->>'native_project_id' = {lit(self.project)} "
            "ORDER BY created_at LIMIT 1"
        )
        self.kb = kb if _UUID_RE.fullmatch(kb or "") else None
        if self.kb is None:
            self.report.note(
                "the project has no knowledge base (is Gitea up?); the "
                "project-scope ref and the platform-owned default are skipped"
            )
        self.create_connector(
            "public",
            self.postgres(scope_mode="all", is_global=True, read_only=True),
        )
        self.create_connector(
            "linked", self.postgres(scope_mode="projects", project_ids=[self.project])
        )
        self.create_connector("private", self.postgres(scope_mode="all"))
        self.create_connector("own", self.postgres(scope_mode="all"), who="other")
        print(
            f"fixture: database {self.pg_name}, project {self.project}, "
            f"knowledge base {self.kb}, connectors {self.connectors}",
            flush=True,
        )

    def check_manifest(self, label: str) -> None:
        listed, problems = listed_ids(
            self.manifest_entries(), self.lookup, project=str(self.project)
        )
        links = self.links()
        self.report.check(
            f"manifest: after {label}, the project's manifest lists exactly its "
            "links, each a Connector ref",
            listed == links and not problems,
            "; ".join(problems[:4])
            or (
                f"listed {sorted(listed)} links {sorted(links)}"
                if listed != links
                else ""
            ),
        )

    def manifest(self) -> None:
        self.check_manifest("the fixture")
        extra = self.create_connector("extra", self.postgres(scope_mode="all"))
        self.owner.ok("POST", f"/api/projects/{self.project}/datasources/{extra}", {})
        self.check_manifest("a link")
        ref = self.resource_ref(extra)
        entries = self.manifest_entries()
        self.report.check(
            "manifest: the linked connector's entry is a ref to its Connector, "
            "under the resource name",
            entries.get(ref["name"]) == {"ref": ref},
            json.dumps(entries.get(ref["name"]))[:200],
        )
        self.owner.ok("DELETE", f"/api/projects/{self.project}/datasources/{extra}")
        self.check_manifest("an unlink")
        doomed = self.create_connector(
            "doomed", self.postgres(scope_mode="projects", project_ids=[self.project])
        )
        self.check_manifest("a connector created linked")
        self.owner.ok("DELETE", f"/api/datasources/{doomed}")
        self.connectors.pop("doomed")
        self.check_manifest("its delete")
        children = sql(
            "SELECT count(*) FROM srw_resources child JOIN srw_resources parent "
            "ON child.managed_by = parent.id WHERE parent.kind = 'Project' AND "
            f"parent.linked_id = {lit(self.project)} AND child.kind = 'Connector' "
            "AND child.deleted_at IS NULL"
        )
        self.report.check(
            "manifest: no inline srw.datasource/v1 child of the project is live",
            children == "0",
            f"{children} live children",
        )

    def selection_refs(self) -> tuple[list[str], dict[str, Any]]:
        """The ids, and the same connectors as execution.connectors refs."""
        ids = [
            self.connectors["own"],
            self.connectors["public"],
            self.connectors["linked"],
        ]
        own = self.resource_ref(self.connectors["own"])
        refs: dict[str, dict[str, Any]] = {
            "own": {"name": own["name"], "scope": {"kind": "Account", "name": "me"}},
            "public": {"uid": self.connectors["public"]},
            "linked": self.resource_ref(self.connectors["linked"]),
        }
        if self.kb:
            ids.append(self.kb)
            # The scope left out is the execution's: its project.
            refs["knowledge"] = {"name": self.resource_ref(self.kb)["name"]}
        return ids, connector_refs(refs)

    def session_body(self, label: str, **selection: Any) -> dict[str, Any]:
        return {
            "title": self.title(label),
            # The ceiling of an approved user without grants (capability
            # grants' default): autonomous needs a permission_mode grant.
            "permission_mode": SESSION_PERMISSION_MODE,
            "project_id": self.project,
            "config_override": {"workspace": {"backend": "sandbox"}},
            "model": self.args.model,
            **selection,
        }

    def job_body(self, label: str, **selection: Any) -> dict[str, Any]:
        return {
            "description": (
                f"D3c connector selection gate {self.gate_id} {label}: reply "
                "with the single word ready and complete the job."
            ),
            "project_id": self.project,
            "execution_lane": "stateless",
            "config_override": {
                "workspace": {"backend": "sandbox"},
                "llm": {"model": self.args.model},
            },
            **selection,
        }

    def bindings(self, *, threads=None, jobs=None) -> dict[str, Any]:
        return in_orchestrator(
            _BINDINGS_PROGRAM,
            {"project": self.project, "threads": threads or {}, "jobs": jobs or {}},
        )

    def delivered(self, found: dict[str, Any], label: str) -> bool:
        names = {self.name("public"), self.name("linked")}
        return names <= set(found.get(label, {}).get("payload") or [])

    def sessions(self) -> None:
        ids, execution = self.selection_refs()
        status, preview = self.other.call(
            "POST",
            "/api/persistent/threads/preview",
            self.session_body("preview", execution=execution),
        )
        self.report.check(
            "sessions: the preview resolves the refs to the same ids, in order",
            status == 200 and preview.get("datasource_ids") == ids,
            f"HTTP {status}: {str(preview)[:200]}",
        )
        for label, selection in (
            ("refs", {"execution": execution}),
            ("ids", {"datasource_ids": ids}),
        ):
            status, created = self.other.call(
                "POST",
                "/api/persistent/threads",
                self.session_body(label, **selection),
            )
            if isinstance(created, dict) and (
                created.get("thread_id") or created.get("id")
            ):
                self.threads[label] = str(created.get("thread_id") or created["id"])
            self.report.check(
                f"sessions: the other account's session by {label} is admitted",
                status in (200, 201) and label in self.threads,
                "" if status in (200, 201) else f"HTTP {status}: {str(created)[:200]}",
            )
        if len(self.threads) < 2:
            return
        found = self.bindings(threads=self.threads)
        differences = same_bindings(found.get("refs", {}), found.get("ids", {}))
        self.report.check(
            "sessions: refs and ids give the same selection record, attach-time "
            "resolution and payload",
            not differences and found["refs"].get("ids") == ids,
            "; ".join(differences[:3]),
        )
        self.report.check(
            "sessions: the public and the project-linked connector reach the "
            "non-owner's session payload",
            self.delivered(found, "refs") and self.delivered(found, "ids"),
            json.dumps({k: v.get("payload") for k, v in found.items()})[:300],
        )

    def delete_job(self, job: str) -> bool:
        self.other.call("PUT", f"/api/jobs/{job}/cancel")

        def deleted() -> bool:
            status, _body = self.other.call("DELETE", f"/api/jobs/{job}")
            if status in (200, 204, 404):
                return True
            return sql(f"SELECT count(*) FROM jobs WHERE id = {lit(job)}") == "0"

        try:
            wait_for("job deleted", deleted, timeout=240, interval=10)
        except GateError:
            return False
        return True

    def job_phase(self) -> None:
        ids, execution = self.selection_refs()
        for label, selection in (
            ("refs", {"execution": execution}),
            ("ids", {"datasource_ids": ids}),
        ):
            status, created = self.other.call(
                "POST", "/api/jobs", self.job_body(label, **selection)
            )
            if isinstance(created, dict) and (
                created.get("job_id") or created.get("id")
            ):
                self.jobs[label] = str(created.get("job_id") or created["id"])
            self.report.check(
                f"jobs: the other account's job by {label} is admitted",
                status in (200, 201) and label in self.jobs,
                "" if status in (200, 201) else f"HTTP {status}: {str(created)[:200]}",
            )
        try:
            if len(self.jobs) == 2:
                found = self.bindings(jobs=self.jobs)
                differences = same_bindings(found.get("refs", {}), found.get("ids", {}))
                self.report.check(
                    "jobs: refs and ids give the same job_datasources, selection "
                    "record, dispatch-time resolution and payload",
                    not differences and found["refs"].get("ids") == ids,
                    "; ".join(differences[:3]),
                )
                self.report.check(
                    "jobs: the public and the project-linked connector reach the "
                    "non-owner's job payload",
                    self.delivered(found, "refs") and self.delivered(found, "ids"),
                    json.dumps({k: v.get("payload") for k, v in found.items()})[:300],
                )
        finally:
            for label, job in list(self.jobs.items()):
                if self.delete_job(job):
                    self.jobs.pop(label)

    def conflicts(self) -> None:
        ids, execution = self.selection_refs()
        before = (len(self.titled_threads()), len(self.described_jobs()))
        attempts = [
            (
                "preview with datasource_ids",
                "/api/persistent/threads/preview",
                self.session_body("conflict", execution=execution, datasource_ids=ids),
            ),
            (
                "session with use_datasource_defaults",
                "/api/persistent/threads",
                self.session_body(
                    "conflict", execution=execution, use_datasource_defaults=True
                ),
            ),
            (
                "session with datasource_ids []",
                "/api/persistent/threads",
                self.session_body("conflict", execution=execution, datasource_ids=[]),
            ),
            (
                "job with datasource_ids",
                "/api/jobs",
                self.job_body("conflict", execution=execution, datasource_ids=ids),
            ),
        ]
        wrong = []
        for label, path, body in attempts:
            status, parsed = self.other.call("POST", path, body)
            if status != 400 or not str(parsed.get("detail", "")).startswith(
                SELECTOR_CONFLICT_PREFIX
            ):
                wrong.append(f"{label}: HTTP {status} {str(parsed)[:120]}")
        after = (len(self.titled_threads()), len(self.described_jobs()))
        self.report.check(
            "conflicts: execution.connectors with another selector is a 400 "
            "and creates nothing",
            not wrong and before == after,
            "; ".join(wrong)
            or (f"created {before} -> {after}" if before != after else ""),
        )

    def refusals(self) -> None:
        private = self.connectors["private"]
        ref = self.resource_ref(private)
        variants = {
            "ref by name": {"execution": connector_refs({"db": ref})},
            "ref by uid": {"execution": connector_refs({"db": {"uid": private}})},
            "ref to nothing": {
                "execution": connector_refs(
                    {"db": {**ref, "name": f"absent-{self.suffix}ab"}}
                )
            },
            "ref into a missing project": {
                "execution": connector_refs(
                    {
                        "db": {
                            "name": ref["name"],
                            "scope": {"kind": "Project", "name": str(uuid.uuid4())},
                        }
                    }
                )
            },
            "id": {"datasource_ids": [private]},
        }
        answers: dict[str, tuple[int, str]] = {}
        before = len(self.described_jobs())
        for label, selection in variants.items():
            status, parsed = self.other.call(
                "POST",
                "/api/persistent/threads/preview",
                self.session_body("refused", **selection),
            )
            answers[f"preview {label}"] = (status, json.dumps(parsed, sort_keys=True))
        for label in ("ref by name", "ref to nothing", "id"):
            status, parsed = self.other.call(
                "POST", "/api/jobs", self.job_body("refused", **variants[label])
            )
            answers[f"job {label}"] = (status, json.dumps(parsed, sort_keys=True))
        expected = (403, json.dumps({"detail": GENERIC_UNAVAILABLE_DETAIL}))
        wrong = {
            label: answer for label, answer in answers.items() if answer != expected
        }
        self.report.check(
            "refusals: every unusable ref and the unusable id answer the same 403 "
            "with the same body",
            not wrong and len(self.described_jobs()) == before,
            json.dumps(wrong)[:400],
        )

    def defaults(self) -> None:
        path = f"/api/projects/{self.project}/connector-defaults"
        linked = self.connectors["linked"]
        view = self.other.ok("GET", path)
        effective = [item["id"] for item in view.get("effective") or []]
        # Nothing is stored yet: only platform-owned entries, the knowledge
        # base first.
        self.report.check(
            "defaults: a member reads the knowledge base as the first, "
            "platform-owned entry and cannot edit",
            (self.kb is None or effective[:1] == [self.kb])
            and all(item.get("platform_owned") for item in view.get("effective") or [])
            and view.get("stored") == []
            and view.get("can_edit") is False,
            json.dumps(view)[:300],
        )

        def preview_ids() -> list[str]:
            status, preview = self.other.call(
                "POST",
                "/api/persistent/threads/preview",
                self.session_body("defaults", use_datasource_defaults=True),
            )
            if status != 200:
                raise GateError(f"defaults preview answered HTTP {status}: {preview}")
            return list(preview.get("datasource_ids") or [])

        baseline = preview_ids()
        self.report.check(
            "defaults: before any project default, the linked connector is not "
            "among a member's defaults (the knowledge base is)",
            linked not in baseline and (self.kb is None or self.kb in baseline),
            json.dumps(baseline),
        )
        status, _body = self.other.call("PUT", path, {"connector_ids": [linked]})
        self.report.check(
            "defaults: an editor cannot write them", status == 403, f"HTTP {status}"
        )
        status, body = self.owner.call(
            "PUT", path, {"connector_ids": [self.connectors["private"]]}
        )
        self.report.check(
            "defaults: a connector that is not linked is a 422",
            status == 422,
            f"HTTP {status}: {str(body)[:160]}",
        )
        view = self.owner.ok("PUT", path, {"connector_ids": [linked]})
        effective = [item["id"] for item in view.get("effective") or []]
        with_default = preview_ids()
        self.report.check(
            "defaults: the owner's linked default is stored after the platform "
            "entries and reaches a member's defaults preview",
            view.get("stored") == [linked]
            and effective[-1:] == [linked]
            and (self.kb is None or effective[:1] == [self.kb])
            and set(baseline) | {linked} == set(with_default),
            f"effective {effective}, preview {with_default}",
        )
        view = self.owner.ok("PUT", path, {"connector_ids": []})
        cleared = preview_ids()
        self.report.check(
            "defaults: cleared, the default no longer attaches",
            view.get("stored") == [] and sorted(cleared) == sorted(baseline),
            json.dumps(cleared),
        )

    def delete_thread(self, thread: str) -> bool:
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

    def cleanup(self) -> list[str]:
        problems: list[str] = []

        def step(label: str, action: Callable[[], Any]) -> None:
            try:
                if action() is False:
                    problems.append(label)
            except GateError as exc:
                problems.append(f"{label} ({exc})")

        # A create that timed out may still have made its session or job:
        # every one titled or described with this gate id goes too.
        for thread in dict.fromkeys(
            list(self.threads.values()) + self.titled_threads()
        ):
            step(f"delete session {thread}", lambda t=thread: self.delete_thread(t))
        for job in dict.fromkeys(list(self.jobs.values()) + self.described_jobs()):
            step(f"delete job {job}", lambda j=job: self.delete_job(j))
        for label, datasource_id in list(self.connectors.items()):

            def delete(datasource_id=datasource_id, label=label) -> bool:
                status, _body = self.api(self.connector_api.get(label, "owner")).call(
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
        if self.account_row:

            def account_row_deleted() -> bool:
                status, body = self.owner.call("DELETE", f"/api/users/{self.other_id}")
                if status in (200, 204, 404):
                    return True
                if status == 409:  # its sessions' workspaces are still releasing
                    return False
                raise GateError(f"HTTP {status}: {str(body)[:200]}")

            step(
                "delete the second account's app row",
                lambda: bool(
                    wait_for(
                        "account row deleted",
                        account_row_deleted,
                        timeout=180,
                        interval=10,
                    )
                ),
            )
        if self.account_started or self.client_started:
            # Last of the API users: the client is how the gate logs in.
            def keycloak_deleted() -> bool:
                result = self.keycloak("delete")
                return not result.get("refused") and not (
                    result.get("users") or result.get("clients")
                )

            step("delete the Keycloak user and OAuth client", keycloak_deleted)
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
        described = self.described_jobs()
        if described:
            left.append(f"jobs described with the gate id: {described}")
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
        threads = list(dict.fromkeys(list(self.threads.values()) + titled))
        jobs = list(dict.fromkeys(list(self.jobs.values()) + described))
        selectors = [
            *(f"srw/thread-id={thread}" for thread in threads),
            *(f"{PINNED_THREAD_LABEL}={thread}" for thread in threads),
            *(f"srw/job-id={job}" for job in jobs),
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
        if self.account_row or self.account_started:
            conditions = [f"lower(email) = lower({lit(self.other_email)})"]
            if self.account_row:
                conditions.append(f"id = {lit(self.other_id)}")
            if self.account_keycloak_id:
                conditions.append(f"keycloak_sub = {lit(self.account_keycloak_id)}")
            if (
                sql(f"SELECT count(*) FROM users WHERE {' OR '.join(conditions)}")
                != "0"
            ):
                left.append(f"the second account's app row {self.other_id}")
        if self.account_started or self.client_started:
            counts = self.keycloak("count")
            if counts.get("users"):
                left.append(f"the Keycloak user {self.other.username}")
            if counts.get("clients"):
                left.append(f"the Keycloak client {self.oauth_client}")
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
            self.accounts()
            self.fixture()
            self.manifest()
            self.sessions()
            self.job_phase()
            self.conflicts()
            self.refusals()
            self.defaults()
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
                            "threads": self.threads,
                            "jobs": self.jobs,
                            "account": (
                                {
                                    "username": self.other.username,
                                    "keycloak_id": self.account_keycloak_id,
                                    "app_id": self.other_id or None,
                                }
                                if self.account_started or self.account_row
                                else None
                            ),
                            "oauth_client": (
                                self.oauth_client if self.client_started else None
                            ),
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
    parser.add_argument(
        "--other-user",
        help="an existing approved non-administrator as the second account "
        "(default: a disposable account named the gate id)",
    )
    parser.add_argument("--other-password")
    parser.add_argument("--model", default=DEFAULT_MODEL)
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
        raise SafetyError("--gate-id must be d3c- followed by 10 hex digits")
    if not _MODEL_RE.fullmatch(args.model):
        raise SafetyError("model id is malformed")
    if (args.other_user is None) != (args.other_password is None):
        raise SafetyError("--other-user and --other-password go together")
    for user in (args.user, args.other_user):
        if user is not None and not _USER_RE.fullmatch(user):
            raise SafetyError("user name is malformed")
    if args.user == args.other_user:
        raise SafetyError("--other-user must be a second account")


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
    return ConnectorSelectionGate(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
