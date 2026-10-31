#!/usr/bin/env python3
"""Local k3d gate for the main cloud's per-folder grants (slices 5 and 7).

Design: knowledge-base/knowledge/features/main_cloud_as_connectors.md
("Access levels", "Overnight run: slices 3 to 8", decision "Per-folder
grants"). Templates: scripts/k3d-cloud-mount-sidecar-gate.py (sessions, their
workspace Pods, the credential Secret) and scripts/k3d-main-cloud-gate.py
(the disposable second account) -- the same safety envelope: dry-run by
default, the exact k3d-srw/srw context, secrets only on ``kubectl exec -i``
stdin and scrubbed from every printed line, ``cap_memory()`` in every
program the gate runs in a pod, and a cleanup in ``finally`` that touches
only what this run created, followed by a residue check by gate id.

It needs the k3d profile of deployment/values-local.yaml.example: the
bundled Nextcloud with its files on the PVC, and the in-pod plane
(``connectors.inPodPlane.enabled: true``), whose recorded plan and
credential Secret say which principal each folder is mounted as.

Fixtures (all disposable, all named after the gate id):

  client      ``<gate id>-oauth``, a public Keycloak client of the srw realm
              with direct access grants, which both accounts log in with
  account     the second account: a Keycloak user named the gate id, its app
              row admitted before its first login (no cloud or Gitea account)
  projects    ``home`` (the owner, the second account as editor), ``other``
              (the owner alone: the isolation target) and ``ro`` (the owner;
              its folder's project link at ``read_only``), each with its folder
              in the main cloud and its own cloud folder connector
  sessions    sandbox sessions: two of the owner's and one of the second
              account's in ``home``, one of the owner's in ``ro``; each gets
              one cheap turn ("Reply with the single word ready."), which
              ensures its stateless workspace Pod

Checks (each printed PASS/FAIL; the exit status is 0 only if all pass):

  preflight   Tilt reports srw ``ok``; every orchestrator pod serves this
              checkout's grant, mount and hook modules byte for byte; the
              in-pod plane is on
  writer      the owner's home session plans home's folder as a writer of
              its own (``srw-writer-f-<32 hex>``) over
              ``/dav/groupfolders/<writer>/<mountpoint>/``, read-write; no
              project mount of the plan is agent-service (the outbox's
              principal is reported); one active writer row with that
              principal; a write as agent-host lands in the group folder; with
              the writer's password (from the Pod's credential Secret) a
              PROPFIND of ``/dav/groupfolders/<writer>/`` lists exactly home's
              folder, other's folder is 404 and a PUT into it is refused, a
              PUT into home's succeeds; a second session of the owner reuses
              the same writer and the slot still has one active row
  member      the second account's home session mounts its own writer; the
              owner removes the second account from home: its row is revoked
              (``member_removed``) and its Nextcloud user is gone, while the
              owner's writer stays active
  reader      the ro session, its link read-only, plans ro's folder as a
              reader of its own (``srw-reader-f-<32 hex>``), read-only, whose
              row is active and verified; a write as agent-host fails with
              EROFS; with the reader's password a PUT gets 403 and a PROPFIND
              207. If the deployment cannot bind the link read-only (the
              per-session level of slice 5 not yet in), the check is
              reported pending, never passed
  delete      home's sessions end and the owner's writer outlives them;
              deleting home revokes every grant of it, and their Nextcloud
              users are gone
  cleanup     nothing this run created is left: sessions (and their pods),
              projects, the second account (app row, then Keycloak user), the
              OAuth client; no live grant of the gate's projects and none of
              their principals in Nextcloud

Run with the repository venv on the k3d-srw cluster, alone: this is a
mutating gate. The owner must be an administrator (the second account's app
row is admitted and deleted through the admin user API).

  .venv/bin/python scripts/k3d-folder-grants-gate.py           # plan
  .venv/bin/python scripts/k3d-folder-grants-gate.py \\
      --run --confirm LOCAL-K3D-DISPOSABLE

A run that was interrupted, or whose cleanup did not finish, is swept by its
id (sessions titled with it, projects named after it, its second account and
OAuth client, its grants' principals):

  .venv/bin/python scripts/k3d-folder-grants-gate.py \\
      --run --confirm LOCAL-K3D-DISPOSABLE --sweep --gate-id fg-0123456789
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import importlib.util
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
from urllib.parse import quote, unquote

ROOT = Path(__file__).resolve().parents[1]

# The run's OAuth client gets the gate clients' protocol mappers: `sub` and
# the audience the API's Bearer check accepts (scripts/k3d_gate_oauth.py),
# so its logins send an access token, never an ID token.
_OAUTH_SPEC = importlib.util.spec_from_file_location(
    "_k3d_gate_oauth", ROOT / "scripts" / "k3d_gate_oauth.py"
)
assert _OAUTH_SPEC is not None and _OAUTH_SPEC.loader is not None
gate_oauth = importlib.util.module_from_spec(_OAUTH_SPEC)
_OAUTH_SPEC.loader.exec_module(gate_oauth)
LOCAL_CONTEXT = "k3d-srw"
LOCAL_NAMESPACE = "srw"
LOCAL_CONFIRMATION = "LOCAL-K3D-DISPOSABLE"
K = ["kubectl", f"--context={LOCAL_CONTEXT}", "-n", LOCAL_NAMESPACE]
ORCHESTRATOR = "deploy/srw-orchestrator"
ORCHESTRATOR_CONTAINER = "orchestrator"
POSTGRES_POD = "srw-postgres-0"
NEXTCLOUD = "deploy/srw-nextcloud"
NEXTCLOUD_CONTAINER = "nextcloud"
WORKSPACE_CONTAINER = "workspace"
PLAN_ANNOTATION = "srw.io/cloud-mount-plan"
CREDENTIAL_VOLUME = "srw-cloud-credential"
KEYCLOAK_TOKEN_URL = "http://srw-keycloak:8080/realms/srw/protocol/openid-connect/token"
POD_ROOT = "/app"
DEFAULT_MODEL = "muse-spark-1.3-contributor"
TURN_PROMPT = "Reply with the single word ready."
ACCOUNT_DOMAIN = "example.invalid"
SESSION_PERMISSION_MODE = "auto_accept"
_SELECTOR = (
    "app.kubernetes.io/instance=srw,app.kubernetes.io/name=superhuman-remote-worker"
)
_GATE_ID_RE = re.compile(r"fg-[0-9a-f]{10}\Z")
_MODEL_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@-]{0,199}\Z")
_USER_RE = re.compile(r"[a-z][a-z0-9._-]{0,63}\Z")
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")
_FOLDER_ID_RE = re.compile(r"[0-9]{1,12}\Z")
_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._ -]{0,127}\Z")
#: A per-folder grant's principal (``folder_grants.principal_ids``).
_PRINCIPAL_RE = re.compile(r"srw-(reader|writer)-f-[0-9a-f]{32}\Z")

#: What every orchestrator pod must serve byte for byte.
SERVED = (
    "src/orchestrator/services/cloud/folder_grants.py",
    "src/orchestrator/services/cloud/nextcloud.py",
    "src/orchestrator/services/cloud/base.py",
    "src/orchestrator/services/agent_cloud_mounts.py",
    "src/orchestrator/services/cloud_mount_plan.py",
    "src/orchestrator/services/cloud_connector_mounts.py",
    "src/orchestrator/services/session_cloud_preparation.py",
    "src/orchestrator/services/projects.py",
    "src/orchestrator/application/workspace.py",
    "src/orchestrator/application/background_tasks.py",
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
    token = json.load(response)["access_token"]
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

# WebDAV requests as one principal against the internal Nextcloud URL: a
# list of {method, path, depth?, body?}; each answers its status and, for a
# PROPFIND, the hrefs. The password arrives on stdin and is never printed.
_DAV_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import base64, json, os, re, sys, urllib.error, urllib.request
cap_memory()
envelope = json.loads(sys.stdin.readline())
base = os.environ["NEXTCLOUD_URL"].rstrip("/")
token = base64.b64encode(
    (envelope["user"] + ":" + envelope["password"]).encode()
).decode()
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
out = []
for step in envelope["requests"]:
    headers = {"Authorization": "Basic " + token}
    if step.get("depth") is not None:
        headers["Depth"] = str(step["depth"])
    data = step.get("body")
    request = urllib.request.Request(
        base + step["path"],
        data=None if data is None else data.encode(),
        method=step["method"],
        headers=headers,
    )
    try:
        with opener.open(request, timeout=60) as response:
            status, text = response.status, response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as error:
        status, text = error.code, error.read().decode("utf-8", "replace")
    hrefs = re.findall(r"<d:href>([^<]+)</d:href>", text, flags=re.I)
    out.append({"status": status, "hrefs": hrefs if step["method"] == "PROPFIND" else []})
print(json.dumps({"results": out}))
"""
)

# Whether Nextcloud still has each named user, as the admin account (its
# credentials are the orchestrator's own and stay in the pod).
_NC_USERS_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import base64, json, os, sys, urllib.error, urllib.parse, urllib.request
cap_memory()
envelope = json.loads(sys.stdin.readline())
base = os.environ["NEXTCLOUD_URL"].rstrip("/")
token = base64.b64encode(
    (os.environ["NEXTCLOUD_ADMIN_USER"] + ":" + os.environ["NEXTCLOUD_ADMIN_PASSWORD"]).encode()
).decode()
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
found = {}
for uid in envelope["users"]:
    request = urllib.request.Request(
        base + "/ocs/v2.php/cloud/users/" + urllib.parse.quote(uid, safe="") + "?format=json",
        headers={"Authorization": "Basic " + token, "OCS-APIRequest": "true"},
    )
    try:
        with opener.open(request, timeout=30) as response:
            found[uid] = response.status
    except urllib.error.HTTPError as error:
        found[uid] = error.code
print(json.dumps({"users": found}))
"""
)

# The D3c/D4 gates' Keycloak program, for the OAuth client and the second
# account: every action finds both by exact name; delete removes only what
# carries this run's marker or email (and its recorded id, once known).
_KEYCLOAK_PROGRAM = (
    _POD_MEMORY_CAP
    + gate_oauth.MAPPERS_SOURCE
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
        "protocolMappers": _SRW_GATE_MAPPERS,
        "optionalClientScopes": [],
        "attributes": {"srw-gate": request["marker"]},
    }, clients, client_owned)))
elif action == "create-user":
    print(json.dumps(create("/users", {
        "username": request["username"],
        "email": request["email"],
        "emailVerified": True,
        "enabled": True,
        "firstName": "Folder",
        "lastName": "Grants",
        "requiredActions": [],
        "credentials": [
            {"type": "password", "value": request["password"], "temporary": False}
        ],
    }, users, user_owned)))
elif action == "delete":
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

# The supervisor's status files, one section each.
_STATUS_SCRIPT = r"""
for f in /srw/cloud-status/*.json; do
  [ -f "$f" ] || continue
  echo "--$(basename "$f" .json)"
  cat "$f"
  echo
done
"""

# A shell for a workspace container, run as agent-host: ``$1`` is the command.
_AS_AGENT = 'exec su -s /bin/sh agent-host -c "$1"'

# Files the main cloud stored for a group folder, by name: ``$1`` is the
# folder id, ``$2`` the file name (both groupfolders layouts).
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


def status_files(text: str) -> dict[int, dict[str, Any]]:
    """The supervisor's status files from :data:`_STATUS_SCRIPT`'s output."""
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


def plan_mount(plan: dict[str, Any], connector_id: str) -> dict[str, Any] | None:
    """The plan's mount of one cloud folder connector (its mount id)."""
    for mount in plan.get("mounts") or []:
        if str(mount.get("mount_id") or "") == connector_id:
            return mount
    return None


def plan_exclusion(plan: dict[str, Any], connector_id: str) -> dict[str, Any] | None:
    for entry in plan.get("excluded") or []:
        if str(entry.get("source_ref") or "") == connector_id:
            return entry
    return None


def grant_url(base_url: str, principal: str, mountpoint: str) -> str:
    """A per-folder grant's mount URL (``folder_grant_webdav_url``)."""
    return (
        f"{base_url.rstrip('/')}/remote.php/dav/groupfolders/"
        f"{quote(principal, safe='')}/{quote(mountpoint, safe='')}/"
    )


def mount_problems(
    mount: dict[str, Any] | None,
    *,
    kind: str,
    access: str,
    base_url: str,
    mountpoint: str,
) -> list[str]:
    """How a planned project mount differs from its per-folder grant's."""
    if mount is None:
        return ["the folder is not mounted"]
    remote = mount.get("remote") or {}
    user = str(remote.get("user") or "")
    problems: list[str] = []
    matched = _PRINCIPAL_RE.fullmatch(user)
    if matched is None or matched.group(1) != kind:
        problems.append(f"mounted as {user!r}, not a {kind} of its own")
    if remote.get("url") != grant_url(base_url, user, mountpoint):
        problems.append(f"url {remote.get('url')!r}")
    if remote.get("nextcloud_chunk_size") != "0":
        problems.append("chunked uploads are on")
    if mount.get("access") != access:
        problems.append(f"access {mount.get('access')!r}, not {access}")
    return problems


def service_account_mounts(plan: dict[str, Any], agent_user: str) -> list[str]:
    """Every project mount of a plan made as the service account."""
    return [
        str(mount.get("name") or "")
        for mount in plan.get("mounts") or []
        if mount.get("mount_kind") == "project"
        and (mount.get("remote") or {}).get("user") == agent_user
    ]


def listing_problems(hrefs: list[str], principal: str, mountpoint: str) -> list[str]:
    """What a Depth 1 PROPFIND of ``/dav/groupfolders/<principal>/`` lists
    beyond its own collection and the one folder."""
    root = f"/remote.php/dav/groupfolders/{principal}/"
    seen = sorted({unquote(href) for href in hrefs})
    expected = sorted({root, f"{root}{mountpoint}/"})
    return [] if seen == expected else [f"lists {seen}"]


@dataclass
class Report:
    gate_id: str
    results: list[tuple[str, bool, str]] = field(default_factory=list)
    pending: list[str] = field(default_factory=list)

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

    def pend(self, name: str, why: str) -> None:
        self.pending.append(name)
        print(f"PENDING {name}: {_scrub(why)}", flush=True)

    @property
    def passed(self) -> bool:
        return bool(self.results) and all(ok for _, ok, _ in self.results)


PLAN = [
    "preflight: Tilt srw ok; every orchestrator pod serves this checkout's "
    "grant, mount and hook modules byte for byte; the in-pod plane is on",
    "accounts: a disposable OAuth client and second account (its app row "
    "admitted before its first login)",
    "fixture: projects home (owner, second account as editor), other (owner) "
    "and ro (owner, its folder's link at read_only through the API), each "
    "with its folder and its cloud folder connector",
    "writer: the owner's home session mounts home as a writer of its own over "
    "/dav/groupfolders/<writer>/<mountpoint>/, read-write; no project mount is "
    "agent-service; one active writer row; a write lands in the group folder; "
    "with the writer's password /dav/groupfolders/<writer>/ lists exactly "
    "home's folder, other's is 404 and refuses a PUT; a second session reuses "
    "the writer",
    "member: the second account's home session mounts its own writer; removing "
    "it from home revokes that row (member_removed) and deletes its Nextcloud "
    "user; the owner's writer stays",
    "reader: the ro session mounts ro as a verified reader of its own, "
    "read-only; a workspace write gets EROFS; a WebDAV PUT with the reader's "
    "password gets 403, a PROPFIND 207 (pending if the deployment cannot bind "
    "the link read-only)",
    "delete: home's sessions end and the writer outlives them; deleting home "
    "revokes every grant of it and deletes their Nextcloud users",
    "cleanup: sessions (and pods), projects, the second account, the OAuth "
    "client; no live grant of the gate's projects and none of their "
    "principals in Nextcloud",
]


@dataclass
class Session:
    label: str
    project: str
    api: Api
    thread: str = ""
    pod: str = ""
    plan: dict[str, Any] = field(default_factory=dict)
    objects: str = ""


@dataclass
class Folder:
    project: str
    connector: str
    folder_id: str
    mountpoint: str


class FolderGrantsGate:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.gate_id = args.gate_id or f"fg-{secrets.token_hex(5)}"
        self.report = Report(self.gate_id)
        self.owner = Api(args.user, args.password)
        self.other_email = f"{self.gate_id}@{ACCOUNT_DOMAIN}"
        self.other = Api(self.gate_id, secrets.token_urlsafe(24))
        self.oauth_client = f"{self.gate_id}-oauth"
        self.client_started = False
        self.client_uuid: str | None = None
        self.account_started = False
        self.account_keycloak_id: str | None = None
        self.account_row = False
        self.owner_id = ""
        self.other_id = ""
        self.base_url = ""
        self.agent_user = ""
        self.projects: dict[str, str] = {}  # label -> project id
        self.folders: dict[str, Folder] = {}
        self.sessions: dict[str, Session] = {}
        self.principals: set[str] = set()
        self.ro_link = False
        self.connectors_seen: list[str] = []
        self.ro_link_refusal = ""

    # -- naming ------------------------------------------------------------
    def name(self, label: str) -> str:
        return f"{self.gate_id} {label}"

    def title(self, label: str) -> str:
        return f"Folder grants gate {self.gate_id} {label}"

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

    def served_problems(self) -> list[str]:
        problems: list[str] = []
        pods = self.pods("orchestrator")
        if not pods:
            problems.append("no orchestrator pod")
        for pod in pods:
            name = pod["metadata"]["name"]
            found = json.loads(
                command(
                    K
                    + ["exec", "-i", name, "-c", ORCHESTRATOR_CONTAINER, "--"]
                    + ["python", "-c", _HASH_PROGRAM, POD_ROOT],
                    data=json.dumps(expected_bytes(SERVED)),
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

    def workspace_pod(self, thread: str) -> dict:
        def probe() -> dict | None:
            items = json.loads(
                command(
                    K + ["get", "pods", "-l", f"srw/thread-id={thread}", "-o", "json"]
                )
            )["items"]
            ready = [
                pod
                for pod in items
                if pod["metadata"]["name"].startswith("ws-thread-")
                and not pod["metadata"].get("deletionTimestamp")
                and pod.get("status", {}).get("phase") == "Running"
                and any(
                    status.get("name") == WORKSPACE_CONTAINER and status.get("ready")
                    for status in pod.get("status", {}).get("containerStatuses") or []
                )
            ]
            return ready[0] if len(ready) == 1 else None

        return wait_for(
            f"the workspace pod of {thread}", probe, timeout=self.args.pod_timeout
        )

    def as_agent(self, pod: str, shell: str) -> tuple[int, str, str]:
        """``shell`` as agent-host in the workspace (no secret in it)."""
        return run(
            K
            + ["exec", pod, "-c", WORKSPACE_CONTAINER, "--"]
            + ["sh", "-c", _AS_AGENT, "gate", shell],
            timeout=300,
        )

    def status_entry(self, pod: str, index: int) -> dict[str, Any]:
        rc, out, err = run(
            K
            + ["exec", "-i", pod, "-c", WORKSPACE_CONTAINER, "--"]
            + ["sh", "-c", _STATUS_SCRIPT],
            timeout=120,
        )
        if rc:
            raise GateError(f"status files unreadable on {pod}: {err.strip()[-200:]}")
        return status_files(out).get(index) or {}

    def wait_mounted(self, session: Session, mount: dict[str, Any]) -> dict:
        index = int(mount.get("index") or 0)
        return wait_for(
            f"{session.label}'s folder settles in its status file",
            lambda: (
                found
                if (found := self.status_entry(session.pod, index)).get("state")
                in ("mounted", "unavailable")
                else None
            ),
            timeout=240,
            interval=4,
        )

    def mount_password(self, session: Session, mount: dict[str, Any]) -> str:
        """The folder's password from the Pod's credential Secret."""
        if not session.objects:
            raise GateError(f"{session.label}'s Pod has no credential Secret")
        body = json.loads(command(K + ["get", "secret", session.objects, "-o", "json"]))
        conf = base64.b64decode(
            (body.get("data") or {}).get("rclone.conf") or ""
        ).decode("utf-8", "replace")
        section = parse_rclone_conf(conf).get(f"m{int(mount.get('index') or 0)}") or {}
        if not section.get("pass"):
            raise GateError(f"{session.label}'s credential Secret has no password")
        if section.get("user") != (mount.get("remote") or {}).get("user"):
            raise GateError(f"{session.label}'s Secret names another user")
        return secret(reveal(section["pass"]))

    def dav(
        self, user: str, password: str, requests: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        result = in_orchestrator(
            _DAV_PROGRAM,
            {"user": user, "password": password, "requests": requests},
            timeout=240,
        )
        return result["results"]

    def nextcloud_users(self, users: list[str]) -> dict[str, int]:
        if any(not _PRINCIPAL_RE.fullmatch(user) for user in users):
            raise GateError("refusing to look up a user that is not a grant's")
        if not users:
            return {}
        return in_orchestrator(_NC_USERS_PROGRAM, {"users": users})["users"]

    def nextcloud_file(self, folder_id: str, name: str) -> tuple[str, int] | None:
        if not _FOLDER_ID_RE.fullmatch(folder_id) or not _NAME_RE.fullmatch(name):
            raise GateError("refusing an unexpected folder id or file name")
        lines = command(
            K
            + ["exec", NEXTCLOUD, "-c", NEXTCLOUD_CONTAINER, "--"]
            + ["su", "-s", "/bin/sh", "www-data", "-c", _NEXTCLOUD_FIND]
            + ["gate", folder_id, name],
            timeout=180,
        ).splitlines()
        if len(lines) != 1:
            return None
        digest, size = lines[0].split()
        return digest, int(size)

    def grants(self, connector: str, user_id: str | None = None) -> list[dict]:
        where = f"datasource_id = {lit(connector)}"
        if user_id:
            where += f" AND user_id = {lit(user_id)}"
        return (
            sql_json(
                "SELECT coalesce(json_agg(json_build_object("
                "'id', id, 'kind', kind, 'user_id', user_id, 'status', status, "
                "'principal_id', principal_id, 'verified', verified_at IS NOT NULL, "
                "'has_password', credentials IS NOT NULL, "
                "'revoke_reason', revoke_reason) ORDER BY created_at), '[]') "
                f"FROM cloud_folder_grants WHERE {where}"
            )
            or []
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

    # -- phases ------------------------------------------------------------
    def preflight(self) -> None:
        if shutil.which("tilt"):
            # Other work syncs into the shared cluster too: wait out an
            # update in progress, never one that failed.
            def settled() -> str | None:
                rc, status, _err = run(
                    ["tilt", "get", "uiresource", "srw", "-o"]
                    + ["jsonpath={.status.updateStatus}"],
                    timeout=30,
                )
                if rc or status not in ("ok", "in_progress", "pending"):
                    raise GateError(
                        f"Tilt srw update status is {status or 'unknown'!r}"
                    )
                return status if status == "ok" else None

            wait_for("Tilt srw ok", settled, timeout=600, interval=10)
        else:
            self.report.note("tilt not on PATH; rollout state not read")
        problems = self.served_problems()
        self.report.check(
            "preflight: every orchestrator pod serves this checkout's grant, mount "
            "and hook modules",
            not problems,
            "; ".join(problems)[:600],
        )
        if problems:
            raise GateError("the deployment does not serve this checkout")
        if not (
            self.orchestrator_env("CONNECTOR_IN_POD_OPENER_IMAGE")
            and self.orchestrator_env("CONNECTOR_IN_POD_RCLONE_IMAGE")
        ):
            raise GateError("connectors.inPodPlane is off: no plan to read")
        self.base_url = self.orchestrator_env("NEXTCLOUD_URL").rstrip("/")
        self.agent_user = self.orchestrator_env("NEXTCLOUD_AGENT_USER")
        if (
            self.orchestrator_env("MAIN_CLOUD_BACKEND") != "nextcloud"
            or not self.base_url
            or not self.agent_user
        ):
            raise GateError("the active main cloud is not the bundled Nextcloud")

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
            raise GateError("the second account logged in as another row")

    def create_folder(self, label: str) -> Folder:
        created = self.owner.ok(
            "POST",
            "/api/projects",
            {
                "name": self.name(label),
                "description": "Folder grants gate (disposable)",
                "user_id": self.owner_id,
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
        connector = wait_for(
            f"project {label}'s cloud folder connector",
            lambda: sql(
                "SELECT id FROM datasources WHERE managed_key = "
                f"{lit('project-cloud:' + project)}"
            ),
            timeout=180,
            interval=5,
        )
        if not _UUID_RE.fullmatch(connector):
            raise GateError(f"project {label} has no cloud folder connector")
        folder = self.folders[label] = Folder(project, connector, folder_id, mountpoint)
        return folder

    def fixture(self) -> None:
        self.create_folder("home")
        self.create_folder("other")
        self.create_folder("ro")
        self.owner.ok(
            "POST",
            f"/api/projects/{self.projects['home']}/members",
            {"user_id": self.other_id, "role": "editor"},
        )
        ro = self.folders["ro"]
        # The project link's default level, as a project owner sets it (slice
        # 5's per-link access). A deployment that does not offer the level
        # leaves the reader phase pending.
        status, body = self.owner.call(
            "PATCH",
            f"/api/projects/{ro.project}/datasources/{ro.connector}",
            {"access": "read_only"},
        )
        self.ro_link = status in (200, 204)
        if not self.ro_link:
            self.ro_link_refusal = f"HTTP {status}: {str(body)[:200]}"

    def start_session(self, label: str, project_label: str, *, api: Api) -> Session:
        session = self.sessions[label] = Session(
            label, self.projects[project_label], api
        )
        created = api.ok(
            "POST",
            "/api/persistent/threads",
            {
                "title": self.title(label),
                "permission_mode": SESSION_PERMISSION_MODE,
                "project_id": session.project,
                "config_override": {"workspace": {"backend": "sandbox"}},
                "model": self.args.model,
            },
        )
        session.thread = str(created.get("thread_id") or created["id"])
        print(f"session {label} {session.thread}", flush=True)
        # A stateless session's workspace Pod is ensured by its first input.
        self.turn(session)
        pod = self.workspace_pod(session.thread)
        session.pod = pod["metadata"]["name"]
        session.plan = recorded_plan(pod) or {}
        session.objects = credential_secret_name(pod) or ""
        return session

    def turn(self, session: Session) -> None:
        """One cheap turn; an input that meets the session still attaching
        (a retryable 409/425/503, or the relayed retryable 502) is retried."""
        deadline = time.monotonic() + self.args.pod_timeout
        while True:
            status, body = session.api.call(
                "POST",
                f"/api/persistent/threads/{session.thread}/input",
                {"content": TURN_PROMPT},
            )
            if status in (200, 201, 202, 204):
                return
            text = json.dumps(body).replace(" ", "")
            retryable = (
                (status == 409 and "session_binding_invalid" in text)
                or status in (425, 503)
                or (status == 502 and '"retryable\\":true' in text)
            )
            if not retryable or time.monotonic() > deadline:
                raise GateError(
                    f"input to {session.label} -> HTTP {status}: {str(body)[:300]}"
                )
            time.sleep(5)

    def writer_phase(self) -> None:
        home, other = self.folders["home"], self.folders["other"]
        first = self.start_session("writer", "home", api=self.owner)
        mount = plan_mount(first.plan, home.connector)
        problems = mount_problems(
            mount,
            kind="writer",
            access="read_write",
            base_url=self.base_url,
            mountpoint=home.mountpoint,
        )
        self.report.check(
            "writer: the owner's session mounts home as a writer of its own over "
            "/dav/groupfolders/<writer>/<mountpoint>/, read-write",
            not problems,
            "; ".join(problems) or str((mount or {}).get("remote", {}).get("user")),
        )
        agent_mounts = service_account_mounts(first.plan, self.agent_user)
        self.report.check(
            "writer: no project mount of the plan is agent-service",
            not agent_mounts,
            str(agent_mounts),
        )
        others = [
            f"{m.get('mount_kind')}: {(m.get('remote') or {}).get('user')}"
            for m in first.plan.get("mounts") or []
            if m.get("mount_kind") != "project"
        ]
        if others:
            self.report.note(f"other mounts of the plan (slice 7b): {others}")
        if mount is None:
            raise GateError("home's folder is not mounted")
        writer = str(mount["remote"]["user"])
        self.principals.add(writer)
        rows = [
            row
            for row in self.grants(home.connector, self.owner_id)
            if row["status"] == "active"
        ]
        self.report.check(
            "writer: one active writer row for home and the owner, with that "
            "principal and its password",
            [(r["kind"], r["principal_id"], r["has_password"]) for r in rows]
            == [("writer", writer, True)],
            str(rows),
        )
        password = self.mount_password(first, mount)
        entry = self.wait_mounted(first, mount)
        marker = secrets.token_hex(16)
        filename = f"{self.gate_id}-writer.txt"
        rc, _out, err = self.as_agent(
            first.pod, f"printf %s {marker} > /cloud/{mount['name']}/{filename}"
        )
        if rc:
            raise GateError(f"the write as agent-host failed: {err.strip()[-200:]}")
        expected = (hashlib.sha256(marker.encode()).hexdigest(), len(marker))
        try:
            landed = wait_for(
                "the write reaches the group folder",
                lambda: self.nextcloud_file(home.folder_id, filename) == expected,
                timeout=180,
                interval=5,
            )
        except GateError:
            landed = False
        self.report.check(
            "writer: a write through the mount lands in home's group folder",
            bool(landed) and entry.get("state") == "mounted",
            f"{entry.get('state')} {filename}",
        )
        root = f"/remote.php/dav/groupfolders/{quote(writer, safe='')}/"
        own = root + quote(home.mountpoint, safe="")
        foreign = root + quote(other.mountpoint, safe="")
        probe = f"{self.gate_id}-probe.txt"
        results = self.dav(
            writer,
            password,
            [
                {"method": "PROPFIND", "path": root, "depth": 1},
                {"method": "PROPFIND", "path": foreign + "/", "depth": 0},
                {"method": "PUT", "path": f"{foreign}/{probe}", "body": "x"},
                {"method": "PUT", "path": f"{own}/{probe}", "body": "x"},
                {"method": "DELETE", "path": f"{own}/{probe}"},
            ],
        )
        listing = listing_problems(results[0]["hrefs"], writer, home.mountpoint)
        self.report.check(
            "writer: as the writer, /dav/groupfolders/<writer>/ lists exactly home's "
            "folder",
            results[0]["status"] == 207 and not listing,
            f"{results[0]['status']} {'; '.join(listing)}",
        )
        self.report.check(
            "writer: another project's folder is 404 to the writer and refuses a PUT",
            results[1]["status"] == 404 and not 200 <= results[2]["status"] < 300,
            f"PROPFIND {results[1]['status']} PUT {results[2]['status']}",
        )
        self.report.check(
            "writer: the writer may write its own folder",
            results[3]["status"] in (201, 204) and results[4]["status"] in (204, 404),
            f"PUT {results[3]['status']} DELETE {results[4]['status']}",
        )
        second = self.start_session("writer-again", "home", api=self.owner)
        again = plan_mount(second.plan, home.connector) or {}
        rows = [
            row
            for row in self.grants(home.connector, self.owner_id)
            if row["status"] in ("minting", "active")
        ]
        self.report.check(
            "writer: a second session of the owner reuses the same writer; the slot "
            "has one live row",
            (again.get("remote") or {}).get("user") == writer
            and [r["principal_id"] for r in rows] == [writer],
            f"{(again.get('remote') or {}).get('user')} rows {len(rows)}",
        )

    def member_phase(self) -> None:
        home = self.folders["home"]
        session = self.start_session("member", "home", api=self.other)
        mount = plan_mount(session.plan, home.connector)
        problems = mount_problems(
            mount,
            kind="writer",
            access="read_write",
            base_url=self.base_url,
            mountpoint=home.mountpoint,
        )
        theirs = str(((mount or {}).get("remote") or {}).get("user") or "")
        owner_rows = [
            r
            for r in self.grants(home.connector, self.owner_id)
            if r["status"] == "active"
        ]
        mine = owner_rows[0]["principal_id"] if owner_rows else ""
        self.report.check(
            "member: the second account's session mounts home as a writer of its "
            "own, not the owner's",
            not problems and theirs != mine,
            "; ".join(problems) or theirs,
        )
        if not _PRINCIPAL_RE.fullmatch(theirs):
            raise GateError("the second account's folder is not mounted as a grant")
        self.principals.add(theirs)
        self.owner.ok("DELETE", f"/api/projects/{home.project}/members/{self.other_id}")
        try:
            row = wait_for(
                "the second account's grant is revoked",
                lambda: next(
                    (
                        r
                        for r in self.grants(home.connector, self.other_id)
                        if r["principal_id"] == theirs and r["status"] == "revoked"
                    ),
                    None,
                ),
                timeout=120,
                interval=5,
            )
        except GateError:
            row = {}
        users = self.nextcloud_users([theirs, mine] if mine else [theirs])
        self.report.check(
            "member: removing the second account from home revokes its grant "
            "(member_removed) and deletes its Nextcloud user",
            row.get("revoke_reason") == "member_removed"
            and not row.get("has_password")
            and users.get(theirs) == 404,
            f"{row.get('status')} {row.get('revoke_reason')} user {users.get(theirs)}",
        )
        still = [
            r
            for r in self.grants(home.connector, self.owner_id)
            if r["status"] == "active"
        ]
        self.report.check(
            "member: the owner's writer stays active and in Nextcloud",
            [r["principal_id"] for r in still] == [mine] and users.get(mine) == 200,
            f"rows {len(still)} user {users.get(mine)}",
        )

    def reader_phase(self) -> None:
        ro = self.folders["ro"]
        if not self.ro_link:
            self.report.pend(
                "reader: the ro session mounts its folder as a verified reader",
                f"the ro link takes no read_only default ({self.ro_link_refusal})",
            )
            return
        session = self.start_session("reader", "ro", api=self.owner)
        mount = plan_mount(session.plan, ro.connector)
        if mount is None:
            excluded = plan_exclusion(session.plan, ro.connector) or {}
            self.report.pend(
                "reader: the ro session mounts its folder as a verified reader",
                f"the link's read-only default does not bind here "
                f"({excluded.get('reason')}: {excluded.get('detail')})",
            )
            return
        problems = mount_problems(
            mount,
            kind="reader",
            access="read_only",
            base_url=self.base_url,
            mountpoint=ro.mountpoint,
        )
        reader = str((mount.get("remote") or {}).get("user") or "")
        if _PRINCIPAL_RE.fullmatch(reader):
            self.principals.add(reader)
        rows = [
            r
            for r in self.grants(ro.connector, self.owner_id)
            if r["status"] == "active"
        ]
        self.report.check(
            "reader: the ro session mounts ro as a reader of its own, read-only; its "
            "row is active and verified",
            not problems
            and [(r["kind"], r["principal_id"], r["verified"]) for r in rows]
            == [("reader", reader, True)],
            "; ".join(problems) or str(rows),
        )
        if problems:
            return
        password = self.mount_password(session, mount)
        entry = self.wait_mounted(session, mount)
        rc, _out, err = self.as_agent(
            session.pod, f"touch /cloud/{mount['name']}/{self.gate_id}-ro.txt"
        )
        self.report.check(
            "reader: a write in the workspace fails with EROFS",
            entry.get("state") == "mounted"
            and rc != 0
            and "read-only file system" in err.lower(),
            f"{entry.get('state')} rc {rc} {err.strip()[-120:]}",
        )
        folder = (
            f"/remote.php/dav/groupfolders/{quote(reader, safe='')}/"
            f"{quote(ro.mountpoint, safe='')}"
        )
        results = self.dav(
            reader,
            password,
            [
                {"method": "PUT", "path": f"{folder}/{self.gate_id}.txt", "body": "x"},
                {"method": "PROPFIND", "path": folder + "/", "depth": 0},
            ],
        )
        self.report.check(
            "reader: with the reader's password a WebDAV PUT gets 403 and a "
            "PROPFIND 207",
            results[0]["status"] == 403 and results[1]["status"] == 207,
            f"PUT {results[0]['status']} PROPFIND {results[1]['status']}",
        )

    def delete_phase(self) -> None:
        home = self.folders["home"]
        for label in ("writer", "writer-again", "member"):
            session = self.sessions.get(label)
            if session and session.thread:
                if not self.delete_thread(session.thread, api=session.api):
                    raise GateError(f"session {label} did not end")
        live = [
            r
            for r in self.grants(home.connector)
            if r["status"] in ("minting", "active")
        ]
        self.report.check(
            "delete: the owner's writer outlives home's sessions",
            [r["user_id"] for r in live] == [self.owner_id],
            str(live),
        )
        principals = [r["principal_id"] for r in self.grants(home.connector)]
        self.owner.ok("DELETE", f"/api/projects/{home.project}")
        self.projects.pop("home", None)
        try:
            wait_for(
                "every grant of home is done",
                lambda: all(
                    r["status"] in ("revoked", "failed")
                    for r in self.grants(home.connector)
                ),
                timeout=120,
                interval=5,
            )
            done = True
        except GateError:
            done = False
        users = self.nextcloud_users(principals)
        rows = self.grants(home.connector)
        self.report.check(
            "delete: deleting home revokes every grant of it and deletes their "
            "Nextcloud users",
            done
            and all(code == 404 for code in users.values())
            and any(r["revoke_reason"] == "project_deleted" for r in rows),
            f"{[(r['status'], r['revoke_reason']) for r in rows]} users {users}",
        )

    # -- cleanup -------------------------------------------------------------
    def titled_threads(self) -> list[str]:
        rows = sql(
            "SELECT id FROM threads WHERE "
            f"position({lit(self.gate_id)} in coalesce(title, '')) > 0"
        )
        return [row for row in rows.splitlines() if _UUID_RE.fullmatch(row)]

    def named_projects(self) -> list[str]:
        rows = sql(
            f"SELECT id FROM projects WHERE name LIKE {lit(self.gate_id + ' %')}"
        )
        return [row for row in rows.splitlines() if _UUID_RE.fullmatch(row)]

    def delete_thread(self, thread: str, *, api: Api | None = None) -> bool:
        def gone() -> bool:
            status, _body = (api or self.owner).call(
                "DELETE", f"/api/persistent/threads/{thread}?force=true&permanent=true"
            )
            if status == 404:
                return True
            return sql(f"SELECT count(*) FROM threads WHERE id = {lit(thread)}") == "0"

        try:
            wait_for("session deleted", gone, timeout=900, interval=10)
        except GateError:
            return False
        return True

    def gate_connectors(self) -> list[str]:
        """The cloud folder connectors of the gate's projects, those of a
        swept run included (a deleted project's row keeps its key)."""
        known = [folder.connector for folder in self.folders.values()]
        rows = sql(
            "SELECT DISTINCT g.datasource_id FROM cloud_folder_grants g "
            "LEFT JOIN projects p ON p.id = g.project_id WHERE "
            f"p.name LIKE {lit(self.gate_id + ' %')} "
            "OR g.datasource_id::text = ANY(ARRAY["
            + ", ".join(lit(c) for c in known)
            + "]::text[])"
        )
        return sorted(
            {*known, *(row for row in rows.splitlines() if _UUID_RE.fullmatch(row))}
        )

    def cleanup(self) -> list[str]:
        problems: list[str] = []
        try:
            self.connectors_seen = sorted(
                {*self.connectors_seen, *self.gate_connectors()}
            )
        except GateError:
            pass

        def step(label: str, action: Callable[[], Any]) -> None:
            try:
                if action() is False:
                    problems.append(label)
            except GateError as exc:
                problems.append(f"{label} ({exc})")

        apis = {s.thread: s.api for s in self.sessions.values() if s.thread}
        if self.owner.client_id:
            for thread in dict.fromkeys([*apis, *self.titled_threads()]):
                step(
                    f"delete session {thread}",
                    lambda t=thread: self.delete_thread(t, api=apis.get(t)),
                )
            for project in dict.fromkeys(
                [*self.projects.values(), *self.named_projects()]
            ):

                def project_deleted(project=project) -> bool:
                    status, _body = self.owner.call(
                        "DELETE", f"/api/projects/{project}"
                    )
                    return status in (200, 204, 404)

                step(
                    f"delete project {project}",
                    lambda p=project_deleted: bool(
                        wait_for("project deleted", p, timeout=600, interval=10)
                    ),
                )
        if self.account_row and self.owner.client_id:

            def account_row_deleted() -> bool:
                status, body = self.owner.call("DELETE", f"/api/users/{self.other_id}")
                if status in (200, 204, 404):
                    return True
                if status == 409:
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
        count = sql(
            f"SELECT count(*) FROM projects WHERE name LIKE {lit(self.gate_id + ' %')}"
        )
        if count != "0":
            left.append(f"{count} projects")
        for thread in [s.thread for s in self.sessions.values() if s.thread]:
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
        connectors = sorted({*self.gate_connectors(), *self.connectors_seen})
        if connectors:
            ids = ", ".join(lit(c) for c in connectors)
            try:
                wait_for(
                    "the gate's grants are done",
                    lambda: sql(
                        "SELECT count(*) FROM cloud_folder_grants WHERE "
                        f"datasource_id IN ({ids}) AND status IN "
                        "('minting', 'active', 'revoking')"
                    )
                    == "0",
                    timeout=180,
                    interval=10,
                )
            except GateError:
                left.append("live grants of the gate's projects")
            principals = sql(
                "SELECT principal_id FROM cloud_folder_grants WHERE "
                f"datasource_id IN ({ids})"
            ).splitlines()
            present = [
                user
                for user, code in self.nextcloud_users(
                    sorted({*principals, *self.principals})
                ).items()
                if code != 404
            ]
            if present:
                left.append(f"Nextcloud principals {present}")
        if self.account_row or self.account_started:
            conditions = [f"lower(email) = lower({lit(self.other_email)})"]
            if self.account_row:
                conditions.append(f"id = {lit(self.other_id)}")
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
    def sweep(self) -> int:
        """Remove what an earlier run of this gate id left behind."""
        self.owner.client_id = self.oauth_client
        self.client_started = self.account_started = True
        found = sql(
            f"SELECT id FROM users WHERE lower(email) = lower({lit(self.other_email)})"
        )
        if _UUID_RE.fullmatch(found):
            self.other_id, self.account_row = found, True
        self.connectors_seen = self.gate_connectors()
        problems = self.cleanup()
        try:
            left = self.residue()
        except GateError as exc:
            left = [f"residue check failed: {exc}"]
        ok = self.report.check(
            f"sweep {self.gate_id}: nothing is left",
            not problems and not left,
            "; ".join([*problems, *left]),
        )
        return 0 if ok else 1

    def run(self) -> int:
        try:
            self.preflight()
            self.accounts()
            self.fixture()
            for phase in (
                self.writer_phase,
                self.member_phase,
                self.reader_phase,
                self.delete_phase,
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
                            "projects": self.projects,
                            "sessions": {k: v.thread for k, v in self.sessions.items()},
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
        pending = f" pending {self.report.pending}" if self.report.pending else ""
        print(
            f"{verdict} {self.gate_id}: {len(self.report.results)} checks, "
            f"{len(failed)} failed {failed if failed else ''}".rstrip()
            + pending
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
    parser.add_argument("--keep", action="store_true", help="skip cleanup")
    parser.add_argument(
        "--sweep",
        action="store_true",
        help="only remove what an earlier run of --gate-id left behind",
    )
    return parser


def validate(args: argparse.Namespace) -> None:
    if args.context != LOCAL_CONTEXT or args.namespace != LOCAL_NAMESPACE:
        raise SafetyError("this gate is restricted to k3d-srw/srw")
    if args.run and args.confirm != LOCAL_CONFIRMATION:
        raise SafetyError(f"--run requires --confirm {LOCAL_CONFIRMATION}")
    if not args.run and args.confirm is not None:
        raise SafetyError("--confirm is accepted only with --run")
    if args.gate_id is not None and not _GATE_ID_RE.fullmatch(args.gate_id):
        raise SafetyError("--gate-id must be fg- followed by 10 hex digits")
    if args.sweep and (not args.run or args.gate_id is None or args.keep):
        raise SafetyError("--sweep needs --run, --confirm and --gate-id, not --keep")
    if not _MODEL_RE.fullmatch(args.model):
        raise SafetyError("model id is malformed")
    if not _USER_RE.fullmatch(args.user):
        raise SafetyError("user name is malformed")
    if not 60 <= args.pod_timeout <= 1800:
        raise SafetyError("--pod-timeout must be between 60 and 1800 seconds")


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
    gate = FolderGrantsGate(args)
    return gate.sweep() if args.sweep else gate.run()


if __name__ == "__main__":
    raise SystemExit(main())
