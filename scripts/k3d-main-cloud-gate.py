#!/usr/bin/env python3
"""Local k3d gate for the main cloud as connectors, slices 1 and 2 (D4).

Design: knowledge-base/knowledge/features/main_cloud_as_connectors.md
("Slices" 1 and 2) and connector_drivers.md, D4.
Template: scripts/k3d-connector-selection-gate.py (D3c) -- the same safety
envelope: dry-run by default, the exact k3d-srw/srw context, passwords only on
``kubectl exec -i`` stdin and scrubbed from every printed line,
``cap_memory()`` in every program the gate runs in a pod, and a cleanup in
``finally`` that touches only what this run created, followed by a residue
check by gate id.

Slice 1 gate: "a session's connector eligibility is unchanged with
thread_mounts emptied". Slice 2 gate: "the page shows the table on a
bundled-Nextcloud (and a bundled-OpenCloud) k3d, and no code outside the
adapters branches on backend_id". This gate proves the second half for the
Python sources only (the ratchet's scope): the cockpit still holds two
provider branches for the protected-cloud toggle, in
``session-create.component.ts`` and ``protected-folder-link.ts``, which slice 5
removes with the toggle.

Fixtures (all disposable, all named after the gate id):

  client      ``<gate id>-oauth``, a public Keycloak client of the srw realm
              with direct access grants and the profile, email and roles
              scopes, which both accounts log in with (the D3a gate's fixture)
  account     the second account: a Keycloak user named the gate id
              (``<gate id>@example.invalid``), created inside the orchestrator
              pod with the pod's own Keycloak admin credentials, its app row
              admitted before its first login (no cloud or Gitea account)
  projects    ``home``, owned by the owner account (``--user``) with the
              second account as editor, provisioned on the main cloud (its
              folder handle is waited for); ``elsewhere``, owned by the owner
              alone
  connectors  ``credentials`` connectors (no infrastructure): the owner's
              ``linked`` (linked to home) and ``elsewhere`` (linked to the
              other project), the second account's ``own`` (scope all)
  session     the second account's session in ``home`` selecting ``linked``
              (and home's knowledge base when Gitea provisions one), on the
              sandbox tier; no message is ever sent

Checks (each printed PASS/FAIL; the exit status is 0 only if all pass):

  preflight   Tilt reports the srw resource ``ok``; every orchestrator pod
              serves this checkout's D4 modules byte for byte, and every
              stateless agent pod its reader-transport check; the active
              provider is the one ``--expect-provider`` names (default: the
              orchestrator's MAIN_CLOUD_BACKEND); this checkout's
              ``scripts/check_cloud_provider_branches.py --check`` passes
              (Python only; the cockpit's two protected-toggle branches stay
              until slice 5)
  account     both accounts are approved, the owner is an administrator, the
              second is not
  page        GET /api/admin/main-cloud as the owner: 200; the provider, its
              public URL and installation id are the active instance's; the
              health probe is ok; the configuration source is Helm and Helm's
              values match the active installation; the matrix is the design
              table (both providers, the active one marked); no secret value
              of the orchestrator's environment appears in the response
  retired     GET/PUT/DELETE /api/admin/system-settings/main_cloud and POST
              .../test and .../reload answer 410 with the Helm detail as the
              owner and leave the active-instance pointer unchanged; the
              second account gets 403 from the page and every retired route
  cockpit     Playwright: Settings -> Main cloud (/admin/cloud) renders one
              row per matrix row and one cell per provider with the API's
              status, shows the installation id and has no form field; at
              phone width the page does not scroll sideways
  scope       the deployed thread_project_ids, runtime-actor scope, policy
              verdicts and attach-time resolution for the gate's session,
              read in the orchestrator pod over a read-only connection: the
              scope is [home]; linked, own and the knowledge base are
              eligible, elsewhere is out of scope; then the session's
              thread_mounts rows (at least one, else the check is vacuous and
              fails) are deleted and every answer is the same; the rows are
              restored. The live protected-lane query agrees with the page:
              the session's project mount is the protected one exactly when
              its provider offers ``protected``
  cleanup     nothing this run created is left: the session titled with the
              gate id (and its pods), connectors, projects, the disposable
              account (app row, then Keycloak user), the OAuth client

Run with the repository venv on the k3d-srw cluster, alone: this is a
mutating gate. The owner must be an administrator (the page is admin-only and
the disposable account's app row is deleted through the user API).

  .venv/bin/python scripts/k3d-main-cloud-gate.py           # plan
  .venv/bin/python scripts/k3d-main-cloud-gate.py \\
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
KEYCLOAK_TOKEN_URL = "http://srw-keycloak:8080/realms/srw/protocol/openid-connect/token"
POD_ROOT = "/app"
DEFAULT_MODEL = "muse-spark-1.3-contributor"
PINNED_THREAD_LABEL = "srw.io/thread-id"
_SELECTOR = (
    "app.kubernetes.io/instance=srw,app.kubernetes.io/name=superhuman-remote-worker"
)
_GATE_ID_RE = re.compile(r"d4-[0-9a-f]{10}\Z")
_MODEL_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@-]{0,199}\Z")
_USER_RE = re.compile(r"[a-z][a-z0-9._-]{0,63}\Z")
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")
PROVIDERS = ("nextcloud", "opencloud")

#: What the orchestrator must serve byte for byte for slices 1 and 2.
SERVED = (
    "src/orchestrator/services/cloud/__init__.py",
    "src/orchestrator/services/cloud/base.py",
    "src/orchestrator/services/cloud/capabilities.py",
    "src/orchestrator/services/cloud/config.py",
    "src/orchestrator/services/cloud/instance_registry.py",
    "src/orchestrator/services/cloud/nextcloud.py",
    "src/orchestrator/services/cloud/opencloud.py",
    "src/orchestrator/services/cloud_staging/__init__.py",
    "src/orchestrator/services/cloud_staging/apply.py",
    "src/orchestrator/services/agent_cloud_mounts.py",
    "src/orchestrator/services/main_cloud_settings.py",
    "src/orchestrator/services/protected_cloud_engage.py",
    "src/orchestrator/services/ro_reader_reconciler.py",
    "src/orchestrator/services/runtime_actor.py",
    "src/orchestrator/services/thread_mount_rows.py",
    "src/orchestrator/routers/main_cloud_settings.py",
    "src/orchestrator/application/lifecycle.py",
    "src/orchestrator/application/settings.py",
    "src/orchestrator/application/workspace.py",
)
#: What every stateless agent pod must serve: the reader-transport check.
AGENT_SERVED = (
    "src/agent/services/cloud_sync/protected_lower.py",
    "src/agent/api/session_workspace.py",
    "src/agent/api/persistent_session.py",
)
#: The design's provider support table (main_cloud_as_connectors.md,
#: "Provider support matrix"): (type, folder kind, access) -> status per
#: provider. ``planned`` is "with slice N".
EXPECTED_MATRIX: dict[tuple[str, str | None, str], dict[str, str]] = {
    ("cloud_folder", "project", "read_only"): {
        "nextcloud": "planned",
        "opencloud": "unsupported",
    },
    ("cloud_folder", "project", "read_write"): {
        "nextcloud": "offered",
        "opencloud": "offered",
    },
    ("cloud_folder", "project", "protected"): {
        "nextcloud": "offered",
        "opencloud": "unsupported",
    },
    ("cloud_folder", "user_root", "read_write"): {
        "nextcloud": "planned",
        "opencloud": "unsupported",
    },
    ("cloud_folder", "user_root", "read_only"): {
        "nextcloud": "unsupported",
        "opencloud": "unsupported",
    },
    ("cloud_folder", "user_root", "protected"): {
        "nextcloud": "unsupported",
        "opencloud": "unsupported",
    },
    ("cloud_folder_checkout", "project", "reviewed_write_back"): {
        "nextcloud": "offered",
        "opencloud": "offered",
    },
    ("cloud_outbox", None, "read_write"): {
        "nextcloud": "offered",
        "opencloud": "offered",
    },
}
PROTECTED = ("cloud_folder", "project", "protected")
RETIRED = (
    ("GET", "/api/admin/system-settings/main_cloud", None),
    ("PUT", "/api/admin/system-settings/main_cloud", {"value": {"backend_id": "x"}}),
    ("DELETE", "/api/admin/system-settings/main_cloud", None),
    ("POST", "/api/admin/system-settings/main_cloud/test", {"value": {}}),
    ("POST", "/api/admin/system-settings/main_cloud/reload", None),
)
PAGE = "/api/admin/main-cloud"
#: The start of the 410 body (``routers.main_cloud_settings.RETIRED_DETAIL``).
RETIRED_PREFIX = "The main cloud is configured by Helm only"
ACCOUNT_DOMAIN = "example.invalid"
#: The second account's session: the most an ungranted user may pick.
SESSION_PERMISSION_MODE = "auto_accept"
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


def sql_script(script: str) -> str:
    """A script on stdin, so nothing in it becomes an argument."""
    return command(
        K
        + ["exec", "-i", POSTGRES_POD, "--", "psql", "-U", "srw", "-d", "srw"]
        + ["-v", "ON_ERROR_STOP=1", "-tAq", "-f", "-"],
        data=script,
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

# Every program the gate runs inside a pod starts with this and calls
# cap_memory() once its imports are done: a gate program that grows must fail
# the gate with a MemoryError, never take the pod to the OOM killer. (The
# same helper as scripts/k3d-connector-drivers-gate.py.)
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

# Whether any secret value of the orchestrator's own environment appears in a
# text the gate received. It answers with the variable names only.
_LEAK_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import json, os, sys
cap_memory()
text = sys.stdin.read()
names = (
    "NEXTCLOUD_ADMIN_PASSWORD", "NEXTCLOUD_AGENT_PASSWORD",
    "MAIN_CLOUD_ADMIN_PASSWORD", "MAIN_CLOUD_AGENT_PASSWORD",
    "NEXTCLOUD_OIDC_CLIENT_SECRET", "NEXTCLOUD_PROTECTED_EFFECT_HMAC_KEY",
    "OPENCLOUD_KEYCLOAK_CLIENT_SECRET",
)
leaks = [n for n in names if len(os.environ.get(n, "")) >= 6 and os.environ[n] in text]
print(json.dumps({"leaks": leaks, "backend": os.environ.get("MAIN_CLOUD_BACKEND", "")}))
"""
)

# The deployed project scope and connector eligibility of one session, over a
# read-only connection: thread_project_ids (its delivery-only mount backfill
# is stubbed to build nothing), the runtime actor's scope, the policy's
# verdicts for the candidate connectors and the attach-time resolution of the
# session's selection. Prints ids and verdict reasons only: resolved rows
# carry decrypted credentials.
_SCOPE_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import asyncio, json, sys
from functools import partial
from uuid import UUID
from orchestrator.database.postgres import PostgresDB
from orchestrator.services import thread_mount_rows
from orchestrator.services.cloud import PROTECTED_PROJECT_FOLDER, provider_offers
from orchestrator.services.cloud_staging import select_protected_mount
from orchestrator.services.datasource_policy import classify_datasource_selection
from orchestrator.services.runtime_actor import _thread_project_ids as actor_scope
from orchestrator.services.thread_datasource_authorization import (
    ThreadDatasourceAuthorizationDependencies,
    resolve_authorized_thread_datasources,
)
cap_memory()
request = json.loads(sys.stdin.readline())
async def _no_rows(*_args, **_kwargs):
    return []
thread_mount_rows.build_thread_mount_rows = _no_rows
async def main():
    db = PostgresDB(
        min_connections=1, max_connections=2,
        server_settings={"default_transaction_read_only": "on"},
    )
    await db.connect()
    try:
        thread = await db.get_thread(request["thread"])
        mount_deps = thread_mount_rows.ThreadMountDependencies(
            store=db, cloud_router=None, resolve_user_identity_cached=None,
            externalize_gitea_url=str, resolve_authorized_thread_datasources=None,
            build_datasources_payload=None, build_workspace_ssh_identities=None,
            cloud_workspace_driver=lambda: "sync",
        )
        scope = await thread_mount_rows.thread_project_ids(
            request["thread"], dependencies=mount_deps
        )
        owner = await db.get_user(str(thread["user_id"]))
        verdicts, _revisions = await classify_datasource_selection(
            db, owner, str(thread["user_id"]), request["candidates"], scope, None
        )
        auth = ThreadDatasourceAuthorizationDependencies(
            store=db,
            thread_project_ids=partial(
                thread_mount_rows.thread_project_ids, dependencies=mount_deps
            ),
        )
        rows = await resolve_authorized_thread_datasources(
            thread, request["selected"], dependencies=auth
        )
        mounts = await db.list_thread_mounts(request["thread"])
        protected = select_protected_mount(mounts)
        print(json.dumps({
            "scope": scope,
            "actor_scope": await actor_scope(db, thread),
            "verdicts": {v.datasource_id: v.reason or "eligible" for v in verdicts},
            "resolved": sorted(str(row["id"]) for row in rows),
            "mounts": len(mounts),
            "mount_providers": sorted({str(m.get("backend_id")) for m in mounts}),
            "protected_mount": bool(protected),
            "offers_protected": {
                str(m.get("backend_id")): provider_offers(
                    m.get("backend_id"), PROTECTED_PROJECT_FOLDER
                )
                for m in mounts
            },
        }))
    finally:
        await db.close()
asyncio.run(main())
"""
)

# The run's Keycloak fixtures, through the orchestrator's own admin
# credentials (they stay in the pod and are never printed): the OAuth client
# ``<gate id>-oauth`` and the disposable user named the gate id. Every action
# finds both by exact name; delete removes only what carries this run's
# marker or email (and its recorded id, once known). The D3c gate's program.
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
        "firstName": "D4",
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


def in_pod(
    target: str, container: str, program: str, payload: Any, *, timeout: int = 300
) -> dict:
    data = payload if isinstance(payload, str) else json.dumps(payload) + "\n"
    out = command(
        K + ["exec", "-i", target, "-c", container, "--", "python", "-c", program],
        data=data,
        timeout=timeout,
    )
    return json.loads(out.splitlines()[-1])


def in_orchestrator(program: str, payload: Any, *, timeout: int = 300) -> dict:
    return in_pod(
        ORCHESTRATOR, ORCHESTRATOR_CONTAINER, program, payload, timeout=timeout
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


def expected_bytes(paths: tuple[str, ...]) -> dict[str, str]:
    return {
        path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() for path in paths
    }


# ---------------------------------------------------------------------------
# What the slices say (pure, unit-tested)
# ---------------------------------------------------------------------------


def matrix_problems(matrix: Any, *, active: str) -> list[str]:
    """How a page's matrix differs from the design table."""
    if not isinstance(matrix, dict):
        return ["no matrix"]
    problems: list[str] = []
    providers = matrix.get("providers") or []
    ids = [p.get("backend_id") for p in providers]
    if ids != list(PROVIDERS):
        problems.append(f"providers {ids}, expected {list(PROVIDERS)}")
    marked = [p.get("backend_id") for p in providers if p.get("active")]
    if marked != [active]:
        problems.append(f"active marked on {marked}, expected [{active}]")
    rows = matrix.get("rows") or []
    keys = [
        (r.get("connector_type"), r.get("folder_kind"), r.get("access")) for r in rows
    ]
    if keys != list(EXPECTED_MATRIX):
        problems.append(f"rows {keys}")
    for row in rows:
        key = (row.get("connector_type"), row.get("folder_kind"), row.get("access"))
        for provider, status in (EXPECTED_MATRIX.get(key) or {}).items():
            cell = (row.get("cells") or {}).get(provider) or {}
            if cell.get("status") != status:
                problems.append(f"{key} {provider}: {cell.get('status')} != {status}")
            if cell.get("status") in ("offered", "planned") and not cell.get("note"):
                problems.append(f"{key} {provider}: no enforced-by line")
            if cell.get("status") == "unsupported" and not cell.get("note"):
                problems.append(f"{key} {provider}: no reason")
    protected = next(
        (
            row
            for row in rows
            if (row.get("connector_type"), row.get("folder_kind"), row.get("access"))
            == PROTECTED
        ),
        None,
    )
    if protected is not None:
        tiers = ((protected.get("cells") or {}).get("nextcloud") or {}).get(
            "workspace_backends"
        )
        if tiers != ["sandbox"]:
            problems.append(
                f"Nextcloud protected tiers {tiers}, expected the container tier"
            )
    return problems


def offered(matrix: dict[str, Any], provider: str, key: tuple) -> bool:
    for row in matrix.get("rows") or []:
        if (
            row.get("connector_type"),
            row.get("folder_kind"),
            row.get("access"),
        ) == key:
            return ((row.get("cells") or {}).get(provider) or {}).get(
                "status"
            ) == "offered"
    return False


def same_scope(a: dict[str, Any], b: dict[str, Any]) -> list[str]:
    """How two scope readings differ (scope, actor scope, verdicts, resolved)."""
    return [
        f"{key}: {a.get(key)!r} != {b.get(key)!r}"[:300]
        for key in ("scope", "actor_scope", "verdicts", "resolved")
        if a.get(key) != b.get(key)
    ]


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
    "preflight: Tilt srw ok; every orchestrator pod serves this checkout's D4 "
    "modules and every stateless agent pod its reader-transport check, byte "
    "for byte; the active provider is the expected one; the provider-branch "
    "ratchet (scripts/check_cloud_provider_branches.py --check) passes on the "
    "Python sources (the cockpit's two protected-toggle branches stay until "
    "slice 5)",
    "account: in the orchestrator pod, an OAuth client <gate id>-oauth and the "
    "disposable second account (a Keycloak user named the gate id, its app row "
    "admitted before its first login); the owner is an administrator, the "
    "second account is not",
    "page: GET /api/admin/main-cloud shows the active instance's provider, "
    "public URL and installation id, a healthy probe, Helm as the source with "
    "matching values, and the design table; no secret of the orchestrator's "
    "environment is in it",
    "retired: the connection form's GET/PUT/DELETE/test/reload answer 410 and "
    "leave the active-instance pointer unchanged; the second account gets 403 "
    "from the page and every retired route",
    "cockpit (Playwright): /admin/cloud shows the table cell for cell as the "
    "API answers, the installation id, no form field, and no sideways scroll "
    "at phone width",
    "fixture: projects home (owner, second account as editor, provisioned on "
    "the main cloud) and elsewhere; credentials connectors linked (home), "
    "elsewhere (other project), own (second account); the second account's "
    "sandbox session in home selecting linked (and home's knowledge base)",
    "scope: in the orchestrator pod over a read-only connection, the deployed "
    "thread_project_ids, runtime-actor scope, policy verdicts and attach-time "
    "resolution of the session; its thread_mounts rows are deleted and every "
    "answer is the same, then the rows are restored; the protected-lane "
    "query agrees with the page's protected cell",
    "cleanup: the session (and its pods), connectors, projects, the disposable "
    "account (app row, then Keycloak user), the OAuth client; residue check "
    "by gate id",
]


class MainCloudGate:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.gate_id = args.gate_id or f"d4-{secrets.token_hex(5)}"
        self.report = Report(self.gate_id)
        self.owner = Api(args.user, args.password)
        self.other_email = f"{self.gate_id}@{ACCOUNT_DOMAIN}"
        self.other = Api(self.gate_id, secrets.token_urlsafe(24))
        # Everything this run creates, recorded before it is checked.
        self.oauth_client = f"{self.gate_id}-oauth"
        self.client_started = False
        self.client_uuid: str | None = None
        self.account_started = False
        self.account_keycloak_id: str | None = None
        self.account_row = False
        self.owner_id = ""
        self.other_id = ""
        self.projects: dict[str, str] = {}  # label -> project id
        self.connectors: dict[str, str] = {}  # label -> datasource id
        self.connector_api: dict[str, str] = {}  # label -> "owner" | "other"
        self.kb: str | None = None
        self.thread: str | None = None
        self.mount_snapshot: list[dict[str, Any]] | None = None
        self.mounts_deleted = False
        self.page: dict[str, Any] = {}
        self.provider = ""

    # -- naming ------------------------------------------------------------
    def name(self, label: str) -> str:
        return f"{self.gate_id} {label}"

    def title(self, label: str) -> str:
        return f"D4 main cloud gate {self.gate_id} {label}"

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

    def titled_threads(self) -> list[str]:
        out = sql(
            "SELECT id FROM threads WHERE "
            f"position({lit(self.gate_id)} in coalesce(title, '')) > 0"
        )
        return [row for row in out.splitlines() if _UUID_RE.fullmatch(row)]

    def active_pointer(self) -> dict[str, Any]:
        return (
            sql_json(
                "SELECT json_build_object('instance', backend_instance_id, 'backend', "
                "backend_id, 'revision', activation_revision) FROM "
                "main_cloud_active_backend WHERE singleton"
            )
            or {}
        )

    # -- phases ------------------------------------------------------------
    def served_problems(self, component: str, container: str, paths) -> list[str]:
        problems: list[str] = []
        pods = self.pods(component)
        if not pods:
            problems.append(f"no {component} pod")
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
                    + ["exec", "-i", name, "-c", container, "--"]
                    + ["python", "-c", _HASH_PROGRAM, POD_ROOT],
                    data=json.dumps(expected_bytes(paths)),
                ).splitlines()[-1]
            )
            if found.get("stale"):
                problems.append(f"{name} stale: {found['stale'][:6]}")
        return problems

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
            "checkout's D4 code",
            not problems,
            "; ".join(problems)[:600],
        )
        if problems:
            raise GateError("the deployment does not serve this checkout")
        environment = in_orchestrator(_LEAK_PROGRAM, "")
        pointer = self.active_pointer()
        expected = self.args.expect_provider or environment.get("backend") or ""
        self.provider = str(pointer.get("backend") or "")
        self.report.check(
            "preflight: the active main cloud is the expected provider",
            self.provider == expected and self.provider in PROVIDERS,
            f"active {self.provider!r}, expected {expected!r}",
        )
        rc, out, err = run(
            [sys.executable, str(ROOT / "scripts/check_cloud_provider_branches.py")]
            + ["--check"],
            timeout=120,
        )
        self.report.check(
            "preflight: no Python code outside the adapter modules branches on "
            "a provider (the ratchet passes on the served checkout; the cockpit's "
            "two protected-toggle branches are outside it)",
            rc == 0,
            (out or err).strip().splitlines()[-1][:300] if (out or err) else "",
        )

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
        result = in_orchestrator(_KEYCLOAK_PROGRAM, payload, timeout=120)
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
            raise GateError(
                "the owner must be an administrator (the page is admin-only)"
            )
        self.account_started = True
        created = self.keycloak("create-user")
        if created.get("exists"):
            self.account_started = False  # not this run's: never adopted
            raise GateError(f"a Keycloak user {self.other.username} already exists")
        self.account_keycloak_id = self.receipt(created, "account")
        app_id = sql("SELECT gen_random_uuid()")
        if not _UUID_RE.fullmatch(app_id):
            raise GateError(f"no fresh id: {app_id!r}")
        self.other_id = app_id
        self.account_row = True
        sql_script(
            "INSERT INTO users (id, display_name, email, keycloak_sub, "
            "preferred_username, is_approved, approved_at, approved_by) VALUES ("
            f"{lit(app_id)}, {lit(self.other.username)}, {lit(self.other_email)}, "
            f"{lit(self.account_keycloak_id)}, {lit(self.other.username)}, true, "
            f"now(), {lit(self.owner_id)});\n"
        )
        other = self.other.ok("GET", "/api/auth/me")["user"]
        if str(other["id"]) != self.other_id:
            raise GateError(
                f"the second account logged in as {other['id']}, not the admitted "
                f"row {self.other_id}"
            )
        self.report.check(
            "account: both accounts are approved, the owner is an administrator, "
            "the second is not",
            bool(owner.get("is_approved") and other.get("is_approved"))
            and not other.get("is_admin"),
            f"second admin={other.get('is_admin')}",
        )

    def page_phase(self) -> None:
        status, page = self.owner.call("GET", PAGE)
        self.page = page if isinstance(page, dict) else {}
        pointer = self.active_pointer()
        routing = (
            sql_json(
                "SELECT routing FROM main_cloud_backend_instances WHERE id = "
                f"{lit(pointer.get('instance') or '00000000-0000-0000-0000-000000000000')}"
            )
            or {}
        )
        provider = self.page.get("provider") or {}
        configuration = self.page.get("configuration") or {}
        self.report.check(
            "page: the provider, public URL and installation are the active instance's",
            status == 200
            and provider.get("backend_id") == self.provider
            and provider.get("backend_instance_id") == pointer.get("instance")
            and provider.get("activation_revision") == pointer.get("revision")
            and provider.get("public_url") == routing.get("public_url"),
            f"HTTP {status}: {json.dumps(provider)[:300]}",
        )
        self.report.check(
            "page: the provider is initialized and its health probe is ok",
            bool(provider.get("initialized"))
            and (self.page.get("health") or {}).get("ok") is True,
            json.dumps(self.page.get("health"))[:200],
        )
        self.report.check(
            "page: the configuration comes from Helm and matches the active "
            "installation",
            configuration.get("source") == "helm"
            and (configuration.get("helm") or {}).get("state") == "matches",
            json.dumps(configuration)[:300],
        )
        problems = matrix_problems(self.page.get("matrix"), active=self.provider)
        self.report.check(
            "page: the matrix is the design's provider support table",
            not problems,
            "; ".join(problems[:6]),
        )
        leaks = in_orchestrator(_LEAK_PROGRAM, json.dumps(page)).get("leaks")
        self.report.check(
            "page: no secret of the orchestrator's environment is in the response",
            leaks == [],
            f"leaked: {leaks}",
        )

    def retired_phase(self) -> None:
        before = self.active_pointer()
        answers = []
        for method, path, body in RETIRED:
            status, parsed = self.owner.call(method, path, body)
            detail = (
                str((parsed or {}).get("detail", ""))
                if isinstance(parsed, dict)
                else ""
            )
            answers.append((method, path, status, detail.startswith(RETIRED_PREFIX)))
        after = self.active_pointer()
        self.report.check(
            "retired: the connection form's API answers 410 with the Helm detail",
            all(status == 410 and helm for _m, _p, status, helm in answers),
            "; ".join(f"{m} {p.rsplit('/', 1)[-1]} {s}" for m, p, s, _h in answers),
        )
        self.report.check(
            "retired: the active-instance pointer is unchanged",
            before == after and bool(before),
            f"{before} -> {after}",
        )
        refused = []
        for method, path, body in (("GET", PAGE, None),) + RETIRED:
            status, _parsed = self.other.call(method, path, body)
            refused.append((method, path, status))
        self.report.check(
            "retired: the second account gets 403 from the page and every "
            "retired route",
            all(status == 403 for _m, _p, status in refused),
            "; ".join(f"{m} {p.rsplit('/', 1)[-1]} {s}" for m, p, s in refused),
        )

    def cockpit_phase(self) -> None:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            self.report.check(
                "cockpit", False, "playwright is not installed in this interpreter"
            )
            return
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                context = browser.new_context(
                    ignore_https_errors=True,
                    service_workers="block",
                    viewport={"width": 1440, "height": 1000},
                )
                page = context.new_page()
                page.goto(
                    f"{self.args.base_url}/admin/cloud",
                    wait_until="domcontentloaded",
                    timeout=60000,
                )
                page.wait_for_selector("#username", timeout=60000)
                page.fill("#username", self.args.user)
                page.fill("#password", self.owner.password)
                page.click("#kc-login")
                self.check_cockpit(page)
            except Exception as exc:  # noqa: BLE001 -- the check reports it
                self.report.check(
                    "cockpit",
                    False,
                    f"{type(exc).__name__}: {str(exc).splitlines()[0][:200]}",
                )
            finally:
                browser.close()

    def check_cockpit(self, page: Any) -> None:
        host = "app-main-cloud-settings"
        page.locator(f"{host} tbody tr[data-row]").first.wait_for(timeout=60000)
        shown = page.locator(f"{host} td[data-cell]").evaluate_all(
            "els => els.map(e => [e.closest('tr').dataset.row, e.dataset.cell, "
            "e.dataset.status])"
        )
        expected = []
        for row in (self.page.get("matrix") or {}).get("rows") or []:
            key = "/".join(
                [row["connector_type"], row.get("folder_kind") or "-", row["access"]]
            )
            for provider in PROVIDERS:
                expected.append([key, provider, row["cells"][provider]["status"]])
        installation = (self.page.get("provider") or {}).get(
            "backend_instance_id"
        ) or ""
        facts = page.locator(f"{host} [data-fact='installation']").inner_text()
        fields = page.locator(f"{host} input, {host} select, {host} textarea").count()
        self.report.check(
            "cockpit (Playwright): Main cloud shows the API's table cell for cell, "
            "the installation id and no form field",
            shown == expected and installation in facts and fields == 0,
            f"{len(shown)} cells, {len(expected)} expected, fields={fields}",
        )
        page.set_viewport_size({"width": 375, "height": 800})
        page.reload(wait_until="domcontentloaded")
        page.locator(f"{host} tbody tr[data-row]").first.wait_for(timeout=60000)
        overflow = page.evaluate(
            "() => document.scrollingElement.scrollWidth - window.innerWidth"
        )
        self.report.check(
            "cockpit (Playwright): at phone width the page does not scroll sideways",
            overflow <= 1,
            f"overflow {overflow}px",
        )

    def create_connector(self, label: str, body: dict[str, Any], *, who: str) -> str:
        """POST a connector; its id is recorded before anything checks it."""
        status, parsed = self.api(who).call(
            "POST",
            "/api/datasources",
            {
                "name": self.name(label),
                "type": "credentials",
                "credentials": {
                    "env_vars": {"D4_GATE_TOKEN": secret(secrets.token_hex(12))}
                },
                **body,
            },
        )
        if isinstance(parsed, dict) and parsed.get("id"):
            self.connectors[label] = str(parsed["id"])
            self.connector_api[label] = who
        if status not in (200, 201) or label not in self.connectors:
            raise GateError(f"{label} create answered HTTP {status}: {parsed}")
        return self.connectors[label]

    def create_project(self, label: str) -> str:
        status, created = self.owner.call(
            "POST",
            "/api/projects",
            {
                "name": self.name(label),
                "description": "D4 main cloud gate (disposable)",
                "user_id": self.owner_id,
            },
        )
        if isinstance(created, dict) and created.get("id"):
            self.projects[label] = str(created["id"])
        if status not in (200, 201) or label not in self.projects:
            raise GateError(f"project {label} answered HTTP {status}: {created}")
        return self.projects[label]

    def fixture(self) -> None:
        home = self.create_project("home")
        elsewhere = self.create_project("elsewhere")
        self.owner.ok(
            "POST",
            f"/api/projects/{home}/members",
            {"user_id": self.other_id, "role": "editor"},
        )
        wait_for(
            "the home project's main-cloud folder",
            lambda: sql(
                "SELECT main_cloud_folder_handle IS NOT NULL FROM projects WHERE "
                f"id = {lit(home)}"
            )
            == "t",
            timeout=180,
            interval=5,
        )
        kb = sql(
            "SELECT id FROM datasources WHERE type = 'kb' AND "
            f"config->>'native_project_id' = {lit(home)} ORDER BY created_at LIMIT 1"
        )
        self.kb = kb if _UUID_RE.fullmatch(kb or "") else None
        if self.kb is None:
            self.report.note("home has no knowledge base (is Gitea up?); skipped")
        self.create_connector(
            "linked", {"scope_mode": "projects", "project_ids": [home]}, who="owner"
        )
        self.create_connector(
            "elsewhere",
            {"scope_mode": "projects", "project_ids": [elsewhere]},
            who="owner",
        )
        self.create_connector("own", {"scope_mode": "all"}, who="other")
        selected = [self.connectors["linked"], *([self.kb] if self.kb else [])]
        status, created = self.other.call(
            "POST",
            "/api/persistent/threads",
            {
                "title": self.title("session"),
                "permission_mode": SESSION_PERMISSION_MODE,
                "project_id": home,
                "config_override": {"workspace": {"backend": "sandbox"}},
                "model": self.args.model,
                "datasource_ids": selected,
            },
        )
        if isinstance(created, dict) and (
            created.get("thread_id") or created.get("id")
        ):
            self.thread = str(created.get("thread_id") or created["id"])
        if status not in (200, 201) or not self.thread:
            raise GateError(f"session create answered HTTP {status}: {created}")
        print(
            f"fixture: projects {self.projects}, connectors {self.connectors}, "
            f"knowledge base {self.kb}, session {self.thread}",
            flush=True,
        )

    def scope(self) -> dict[str, Any]:
        candidates = [
            self.connectors[label] for label in ("linked", "elsewhere", "own")
        ] + ([self.kb] if self.kb else [])
        selected = [self.connectors["linked"], *([self.kb] if self.kb else [])]
        return in_orchestrator(
            _SCOPE_PROGRAM,
            {"thread": self.thread, "candidates": candidates, "selected": selected},
        )

    def scope_phase(self) -> None:
        home = self.projects["home"]
        before = self.scope()
        expected = {
            self.connectors["linked"]: "eligible",
            self.connectors["elsewhere"]: "out_of_scope",
            self.connectors["own"]: "eligible",
            **({self.kb: "eligible"} if self.kb else {}),
        }
        self.report.check(
            "scope: the session's scope is its project, and eligibility is the "
            "design's (linked, own and the KB eligible; elsewhere out of scope)",
            before.get("scope") == [home]
            and before.get("actor_scope") == [home]
            and before.get("verdicts") == expected
            and before.get("resolved")
            == sorted([self.connectors["linked"], *([self.kb] if self.kb else [])]),
            json.dumps(before)[:400],
        )
        self.report.check(
            "scope: the session has thread_mounts rows to empty",
            int(before.get("mounts") or 0) > 0,
            f"{before.get('mounts')} rows (none means the project has no "
            "main-cloud folder, and the next check would prove nothing)",
        )
        self.mount_snapshot = (
            sql_json(
                "SELECT coalesce(json_agg(m), '[]'::json) FROM thread_mounts m WHERE "
                f"thread_id = {lit(self.thread)}"
            )
            or []
        )
        sql(f"DELETE FROM thread_mounts WHERE thread_id = {lit(self.thread)}")
        self.mounts_deleted = True
        after = self.scope()
        differences = same_scope(before, after)
        self.report.check(
            "scope: with the session's thread_mounts emptied, its scope, the "
            "runtime actor's, every verdict and the attach-time resolution are "
            "unchanged",
            not differences and int(after.get("mounts") or 0) == 0,
            "; ".join(differences[:4]),
        )
        self.restore_mounts()
        offers = offered(self.page.get("matrix") or {}, self.provider, PROTECTED)
        self.report.check(
            "scope: the protected-lane query agrees with the page (the project "
            "mount is the protected one exactly when the provider offers it)",
            before.get("protected_mount") == offers
            and before.get("offers_protected", {}).get(self.provider) == offers,
            f"page offers={offers}, live={before.get('protected_mount')}, "
            f"{before.get('offers_protected')}",
        )

    def restore_mounts(self) -> bool:
        if not self.mounts_deleted:
            return True
        rows = json.dumps(self.mount_snapshot or [])
        sql_script(
            "INSERT INTO thread_mounts SELECT * FROM "
            f"json_populate_recordset(NULL::thread_mounts, {lit(rows)}::json) "
            "ON CONFLICT DO NOTHING;\n"
        )
        self.mounts_deleted = False
        return True

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

        if self.mounts_deleted:
            step("restore the session's thread_mounts rows", self.restore_mounts)
        threads = ([self.thread] if self.thread else []) + self.titled_threads()
        for thread in dict.fromkeys(threads):
            step(f"delete session {thread}", lambda t=thread: self.delete_thread(t))
        for label, datasource_id in list(self.connectors.items()):

            def delete(datasource_id=datasource_id, label=label) -> bool:
                status, _body = self.api(self.connector_api.get(label, "owner")).call(
                    "DELETE", f"/api/datasources/{datasource_id}"
                )
                return status in (200, 204, 404)

            step(f"delete connector {label}", delete)
        for label, project in list(self.projects.items()):

            def project_deleted(project=project) -> bool:
                status, _body = self.owner.call("DELETE", f"/api/projects/{project}")
                return status in (200, 204, 404)

            step(
                f"delete project {label}",
                lambda p=project_deleted: bool(
                    wait_for("project deleted", p, timeout=180, interval=10)
                ),
            )
        if self.account_row:

            def account_row_deleted() -> bool:
                status, body = self.owner.call("DELETE", f"/api/users/{self.other_id}")
                if status in (200, 204, 404):
                    return True
                if status == 409:  # its session's workspace is still releasing
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
        count = sql(f"SELECT count(*) FROM projects WHERE name LIKE {lit(prefix)}")
        if count != "0":
            left.append(f"{count} projects")
        threads = list(dict.fromkeys(([self.thread] if self.thread else []) + titled))
        for selector in [
            *(f"srw/thread-id={thread}" for thread in threads),
            *(f"{PINNED_THREAD_LABEL}={thread}" for thread in threads),
        ]:
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
        return left

    # -- run ---------------------------------------------------------------
    def run(self) -> int:
        try:
            self.preflight()
            self.accounts()
            self.page_phase()
            self.retired_phase()
            self.cockpit_phase()
            self.fixture()
            self.scope_phase()
            legacy = sql(
                "SELECT count(*) FROM threads t WHERE t.project_id IS NULL AND "
                "(SELECT count(DISTINCT m.source_ref) FROM thread_mounts m WHERE "
                "m.thread_id = t.id AND m.mount_kind IN ('project', "
                "'project_default')) > 1"
            )
            self.report.note(
                f"{legacy} legacy multi-project sessions still read their scope "
                "from thread_mounts (slice 8 decides them)"
            )
        except GateError as exc:
            self.report.check("gate infrastructure", False, str(exc))
        finally:
            if self.args.keep:
                print(
                    "kept: "
                    + json.dumps(
                        {
                            "projects": self.projects,
                            "connectors": self.connectors,
                            "session": self.thread,
                            "account": self.other.username
                            if self.account_started
                            else None,
                            "oauth_client": (
                                self.oauth_client if self.client_started else None
                            ),
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
    parser.add_argument(
        "--expect-provider",
        choices=PROVIDERS,
        help="the bundled provider this k3d runs (default: the orchestrator's "
        "MAIN_CLOUD_BACKEND)",
    )
    parser.add_argument("--base-url", default="https://localhost")
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
        raise SafetyError("--gate-id must be d4- followed by 10 hex digits")
    if not _MODEL_RE.fullmatch(args.model):
        raise SafetyError("model id is malformed")
    if not _USER_RE.fullmatch(args.user):
        raise SafetyError("user name is malformed")
    if not re.fullmatch(
        r"https://(localhost|[a-z0-9.-]+\.localhost)(:\d+)?", args.base_url
    ):
        raise SafetyError("--base-url must be the local k3d edge")


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
    return MainCloudGate(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
