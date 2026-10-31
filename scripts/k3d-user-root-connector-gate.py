#!/usr/bin/env python3
"""Local k3d gate for main cloud slice 4: a user's own cloud root connector.

Design: knowledge-base/knowledge/features/main_cloud_as_connectors.md (slice 4,
"Overnight run: slices 3 to 8") and nextcloud_user_home_app_token_grant.md
(Lifecycle, Security). Templates: scripts/k3d-main-cloud-gate.py (its
Keycloak fixtures) and scripts/k3d-cloud-mount-sidecar-gate.py (sessions, plan
annotations, the credential Secret) -- the same safety envelope: dry-run by
default, the exact k3d-srw/srw context, passwords only on ``kubectl exec -i``
stdin and scrubbed from every printed line, ``cap_memory()`` in every program
the gate runs in a pod, and a cleanup in ``finally`` that touches only what
this run created, followed by a residue check by gate id.

It needs the bundled Nextcloud as the active main cloud with its files on the
PVC (the gate reads the user's file on disk), the in-pod plane of
deployment/values-local.yaml.example, and the Nextcloud public URL reachable
from this host (``--nextcloud-url``, default https://cloud.localhost): the
gate completes Login Flow v2 the way a browser does, from here, because
Nextcloud's session cookies are Secure.

Fixtures (all disposable, all named after the gate id):

  client      ``<gate id>-oauth``, a public Keycloak client of the srw realm
              with direct access grants and the profile, email and roles
              scopes (the D4 gate's fixture)
  accounts    A (``<gate id>``) and B (``<gate id>-b``): Keycloak users whose
              first login JIT-creates their app rows (pending), approved by
              the owner (``--user``, an administrator) through the admin API
  cloud       ``<64 hex>`` (A's Nextcloud account, A's e-mail, as user_oidc
              would name it) and ``<gate id>-eve`` (another e-mail), local
              Nextcloud users created with ``occ``; their passwords never
              leave this process and the pod's stdin
  sessions    A's sandbox sessions in A's personal project, the connector
              list omitted (the work's own connectors attach); no turn is sent

Checks (each printed PASS/FAIL; the exit status is 0 only if all pass):

  preflight   every orchestrator pod serves this checkout's slice 4 modules
              byte for byte; the active provider is Nextcloud and its grant
              mode is not off; the public Nextcloud URL answers here
  row         A's approval created ``user-cloud-root:<A>``: cloud_folder,
              folder_kind user_root, the active installation, linked to A's
              personal project only, auto-attach, no credential, created by A
  waiting     GET /api/me/cloud-root says not_connected; A's eligible list for
              the personal project has "My cloud" selected by default; a
              session with no list attaches it, and its Pod's plan excludes it
              as {user_root, cloud_not_connected}, with no home or session
              folder mounted in its place
  connect     POST .../connect answers a login page on the public host; the
              flow completed as A's Nextcloud account makes the state
              connected (account id = the uid) within 60 s; the account has
              exactly one app password named "Superhuman Remote Worker"; a
              cloud_root_granted security event
  mount       a new session's plan mounts my_cloud (user_root, read-write)
              from http://srw-nextcloud/remote.php/dav/files/<uid>/ as the
              uid, with chunked uploads (no nextcloud_chunk_size); the
              credential Secret's password is that app password; a file the
              agent account writes at cloud/my_cloud/ appears in the user's
              Nextcloud files on disk
  hygiene     the app password is in no orchestrator log line since the gate
              started, no security event, no thread metadata, no Pod
              annotation or ConfigMap, no connector config, resource document
              or stored credential column (ciphertext only)
  wrong       a flow completed as eve (another e-mail): the failure is
              wrong_account, A stays connected with the same app password, and
              eve's account keeps no "Superhuman Remote Worker" password
  revoke      the app password deleted in Nextcloud (occ): a new session's
              plan excludes the root as {user_root, cloud_grant_revoked} and
              the state is revoked (rejected_by_cloud); a reconnect gives a
              fresh app password, the old one stays gone
  owner       B, an editor of A's personal project, does not see A's root in
              the eligible list, and a session B creates there naming it is
              refused
  delete      A's account deleted through the admin API: the delete succeeds,
              the row is gone and so is the app password in Nextcloud
  cleanup     nothing this run created is left: sessions titled with the gate
              id, both app rows, both Keycloak users and the client, both
              Nextcloud users

Not covered here (a manual browser check, step 12 of the plan): a real
Keycloak SSO "Connect your cloud" provisions the user_oidc account, whose uid
is sha256("1_0_<preferred_username>").

Run with the repository venv on the k3d-srw cluster, alone: this is a
mutating gate.

  .venv/bin/python scripts/k3d-user-root-connector-gate.py           # plan
  .venv/bin/python scripts/k3d-user-root-connector-gate.py \\
      --run --confirm LOCAL-K3D-DISPOSABLE

A run that was interrupted, or whose cleanup did not finish, is swept by its
id:

  .venv/bin/python scripts/k3d-user-root-connector-gate.py \\
      --run --confirm LOCAL-K3D-DISPOSABLE --sweep --gate-id ur-0123456789
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import html
import importlib.util
import json
import re
import secrets
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit

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
NEXTCLOUD_INTERNAL = "http://srw-nextcloud"
WORKSPACE_CONTAINER = "workspace"
PLAN_ANNOTATION = "srw.io/cloud-mount-plan"
CREDENTIAL_VOLUME = "srw-cloud-credential"
KEYCLOAK_TOKEN_URL = "http://srw-keycloak:8080/realms/srw/protocol/openid-connect/token"
POD_ROOT = "/app"
DEFAULT_MODEL = "muse-spark-1.3-contributor"
DEVICE_NAME = "Superhuman Remote Worker"
MOUNT_NAME = "my_cloud"
_SELECTOR = (
    "app.kubernetes.io/instance=srw,app.kubernetes.io/name=superhuman-remote-worker"
)
_GATE_ID_RE = re.compile(r"ur-[0-9a-f]{10}\Z")
_MODEL_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@-]{0,199}\Z")
_USER_RE = re.compile(r"[a-z][a-z0-9._-]{0,63}\Z")
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")
_NC_UID_RE = re.compile(r"(ur-[0-9a-f]{10}-eve|[0-9a-f]{64})\Z")
_FILE_RE = re.compile(r"ur-gate-[0-9a-f]{10}\.txt\Z")

#: What every orchestrator pod must serve byte for byte.
SERVED = (
    "src/orchestrator/services/cloud_user_root.py",
    "src/orchestrator/services/cloud/user_root_grant.py",
    "src/orchestrator/services/cloud/nextcloud_user_root.py",
    "src/orchestrator/services/cloud/nextcloud.py",
    "src/orchestrator/services/cloud_connector_mounts.py",
    "src/orchestrator/services/agent_cloud_mounts.py",
    "src/orchestrator/services/connector_drivers/cloud_folder.py",
    "src/orchestrator/routers/cloud_user_root.py",
    "src/orchestrator/application/workspace.py",
    "src/orchestrator/application/lifecycle.py",
    "src/orchestrator/services/user_administration.py",
    "src/orchestrator/security/auth.py",
    "src/shared/connectors/builtin.py",
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


def raw_output(args: list[str], *, timeout: int = 180) -> str:
    """A command's stdout, unscrubbed (for the hygiene scan only)."""
    try:
        result = subprocess.run(args, text=True, capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        raise GateError("a hygiene read timed out") from None
    if result.returncode:
        raise GateError("a hygiene read failed")
    return result.stdout


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


def sql_count_containing(table: str, column: str, needle: str) -> int:
    """Rows of ``table`` whose ``column`` (as text) contains ``needle``; the
    needle travels in the script on stdin, never as an argument."""
    if not re.fullmatch(r"[a-z_]+", table) or not re.fullmatch(r"[a-z_]+", column):
        raise GateError("refusing an unexpected table or column")
    if not re.fullmatch(r"[A-Za-z0-9._~+/=-]{8,512}", needle):
        raise GateError("refusing an unexpected needle")
    out = command(
        K
        + ["exec", "-i", POSTGRES_POD, "--", "psql", "-U", "srw", "-d", "srw"]
        + ["-v", "ON_ERROR_STOP=1", "-tAq", "-f", "-"],
        data=(
            f"SELECT count(*) FROM {table} "
            f"WHERE strpos({column}::text, $n${needle}$n$) > 0;\n"
        ),
    )
    return int(out or "0")


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

_ENV_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import json, os
cap_memory()
print(json.dumps({
    "backend": os.environ.get("MAIN_CLOUD_BACKEND", ""),
    "grant_mode": os.environ.get("NEXTCLOUD_USER_ROOT_GRANT_MODE", ""),
}))
"""
)

# The run's Keycloak fixtures through the orchestrator's own admin
# credentials (they stay in the pod and are never printed): the OAuth client
# ``<gate id>-oauth`` and the disposable users named after the gate id. Every
# action finds them by exact name; delete removes only what carries this
# run's marker or e-mail. The D4 gate's program, for several users.
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
def users(name):
    found = listing("/users", {"username": name, "exact": "true"})
    return [u for u in found if u.get("username") == name]
def clients():
    name = request["client"]
    return [c for c in listing("/clients", {"clientId": name}) if c.get("clientId") == name]
action = request["action"]
if action == "create-client":
    if clients():
        print(json.dumps({"exists": True}))
        raise SystemExit(0)
    status, text, location = call("POST", admin_api + "/clients", {
        "clientId": request["client"], "name": request["client"], "enabled": True,
        "protocol": "openid-connect", "publicClient": True,
        "standardFlowEnabled": False, "directAccessGrantsEnabled": True,
        "serviceAccountsEnabled": False, "fullScopeAllowed": True,
        "defaultClientScopes": ["profile", "email", "roles"],
        "protocolMappers": _SRW_GATE_MAPPERS,
        "optionalClientScopes": [], "attributes": {"srw-gate": request["marker"]},
    })
    print(json.dumps({"status": status, "found": [c["id"] for c in clients()]}))
elif action == "create-user":
    name = request["username"]
    if users(name):
        print(json.dumps({"exists": True}))
        raise SystemExit(0)
    status, text, location = call("POST", admin_api + "/users", {
        "username": name, "email": request["email"], "emailVerified": True,
        "enabled": True, "firstName": "UR", "lastName": "Gate", "requiredActions": [],
        "credentials": [
            {"type": "password", "value": request["password"], "temporary": False}
        ],
    })
    print(json.dumps({"status": status, "found": [u["id"] for u in users(name)]}))
elif action == "reset-password":
    # A sweep logs in as the run's account to end its sessions: only an
    # account with this run's name and e-mail gets a new password.
    found = [
        u for u in users(request["username"]) if u.get("email") == request["email"]
    ]
    if len(found) != 1:
        print(json.dumps({"reset": False}))
        raise SystemExit(0)
    status, _t, _l = call(
        "PUT",
        admin_api + "/users/" + found[0]["id"] + "/reset-password",
        {"type": "password", "value": request["password"], "temporary": False},
    )
    print(json.dumps({"reset": status == 204}))
elif action == "delete":
    deleted, refused = [], []
    for spec in request["users"]:
        for user in users(spec["username"]):
            if user.get("email") != spec["email"]:
                refused.append(user.get("id"))
                continue
            status, _t, _l = call("DELETE", admin_api + "/users/" + user["id"])
            if status not in (204, 404):
                raise SystemExit("user delete answered HTTP %d" % status)
            deleted.append(user["id"])
    if request.get("client_started"):
        for client in clients():
            if (client.get("attributes") or {}).get("srw-gate") != request["marker"]:
                refused.append(client.get("id"))
                continue
            status, _t, _l = call("DELETE", admin_api + "/clients/" + client["id"])
            if status not in (204, 404):
                raise SystemExit("client delete answered HTTP %d" % status)
            deleted.append(client["id"])
    left = sum(len(users(spec["username"])) for spec in request["users"])
    print(json.dumps({
        "deleted": deleted, "refused": refused, "users": left,
        "clients": len(clients()) if request.get("client_started") else 0,
    }))
else:
    print(json.dumps({"error": "unknown action"}))
"""
)

# Nextcloud as www-data: options live in the script, values are positional,
# so none of them can be read as an option of su. A password is on stdin.
_OCC_ADD_USER = (
    'OC_PASS="$(cat)" exec php /var/www/html/occ user:add --password-from-env '
    '--display-name "$2" "$1"'
)
_OCC_SET_EMAIL = 'exec php /var/www/html/occ user:setting "$1" settings email "$2"'
_OCC_TOKENS = 'exec php /var/www/html/occ user:auth-tokens:list --output=json "$1"'
_OCC_DELETE_TOKEN = 'exec php /var/www/html/occ user:auth-tokens:delete "$1" "$2"'
_OCC_DELETE_USER = 'exec php /var/www/html/occ user:delete "$1"'
_OCC_USER_INFO = 'exec php /var/www/html/occ user:info --output=json "$1"'
_NC_FILE = r"""
data=$(php /var/www/html/occ config:system:get datadirectory)
f="$data/$1/files/$2"
if [ -f "$f" ]; then sha256sum "$f" | cut -d' ' -f1; fi
"""
_AS_AGENT = 'exec su -s /bin/sh agent-host -c "$1"'


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
    """``program`` in the orchestrator pod; retried while the pod is being
    replaced (a Tilt rollout), never on an answer the program printed."""
    for attempt in range(6):
        try:
            return in_pod(
                ORCHESTRATOR, ORCHESTRATOR_CONTAINER, program, payload, timeout=timeout
            )
        except GateError:
            if attempt == 5:
                raise
            time.sleep(10)
    raise GateError("unreachable")


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
# Pure helpers (pinned by tests/test_k3d_user_root_connector_gate.py)
# ---------------------------------------------------------------------------

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


def nextcloud_uid(gate_id: str) -> str:
    """A's Nextcloud account: 64 hex digits, the shape user_oidc gives."""
    return hashlib.sha256(f"srw-user-root-gate:{gate_id}".encode()).hexdigest()


def device_tokens(listing: Any, name: str = DEVICE_NAME) -> list[dict[str, Any]]:
    """The app passwords named ``name`` in ``occ user:auth-tokens:list``."""
    if not isinstance(listing, list):
        return []
    return [t for t in listing if isinstance(t, dict) and t.get("name") == name]


def form_value(page: str, name: str) -> str | None:
    """A hidden input's value in a Nextcloud page."""
    match = re.search(
        r'name="' + re.escape(name) + r'"\s+value="([^"]*)"', page
    ) or re.search(r'value="([^"]*)"\s+name="' + re.escape(name) + '"', page)
    return html.unescape(match.group(1)) if match else None


def page_request_token(page: str) -> str | None:
    match = re.search(r'data-requesttoken="([^"]+)"', page)
    return html.unescape(match.group(1)) if match else None


def picker_action(page: str) -> str | None:
    """The "Log in" form's target on the login flow's picker page."""
    match = re.search(r'id="login-form"\s+action="([^"]+)"', page)
    return html.unescape(match.group(1)) if match else None


def top_mounts(mountinfo: str) -> dict[str, tuple[str, str]]:
    """``{mountpoint: (fstype, per-mount options)}`` for the top of each stack
    (the D7 gate's reader)."""
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


def root_exclusion(plan: dict[str, Any], connector_id: str) -> str | None:
    """The reason ``plan`` gives for leaving the root out, or ``None``."""
    for entry in plan.get("excluded") or []:
        if (
            entry.get("source_ref") == connector_id
            and entry.get("mount_kind") == "user_root"
        ):
            return str(entry.get("reason") or "")
    return None


def substitute_problems(plan: dict[str, Any]) -> list[str]:
    """Anything mounted in a missing root's place: a home, a session folder."""
    problems = []
    for mount in plan.get("mounts") or []:
        if mount.get("name") == "home" or mount.get("mount_kind") in (
            "session_folder",
            "project_default",
        ):
            problems.append(f"{mount.get('name')} ({mount.get('mount_kind')})")
        if mount.get("mount_id") == "legacy-session":
            problems.append("legacy-session")
    return problems


def root_mount_problems(plan: dict[str, Any], connector_id: str, uid: str) -> list[str]:
    """What is wrong with the root's mount in ``plan`` (empty when right)."""
    found = [m for m in plan.get("mounts") or [] if m.get("source_ref") == connector_id]
    if len(found) != 1:
        return [f"{len(found)} mounts of the root"]
    mount = found[0]
    remote = mount.get("remote") or {}
    problems = []
    if mount.get("name") != MOUNT_NAME:
        problems.append(f"name {mount.get('name')!r}")
    if mount.get("mount_kind") != "user_root" or mount.get("access") != "read_write":
        problems.append(f"{mount.get('mount_kind')}/{mount.get('access')}")
    if remote.get("url") != f"{NEXTCLOUD_INTERNAL}/remote.php/dav/files/{uid}/":
        problems.append(f"url {remote.get('url')!r}")
    if remote.get("user") != uid:
        problems.append(f"user {remote.get('user')!r}")
    if "nextcloud_chunk_size" in remote:
        problems.append("chunked uploads are off")
    return problems


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
    "preflight: orchestrator pods serve this checkout's slice 4 modules; the "
    "active provider is Nextcloud with grants on; the public Nextcloud URL "
    "answers here",
    "row: A's approval created user-cloud-root:<A> (cloud_folder, user_root, "
    "the active installation, A's personal project only, auto-attach, no "
    "credential)",
    "waiting: not_connected; default-selected for A at home; a session's plan "
    "excludes it as cloud_not_connected with nothing in its place",
    "connect: a public login page; the flow as A's Nextcloud account connects "
    "within 60 s; one 'Superhuman Remote Worker' app password; a granted event",
    "mount: my_cloud from /dav/files/<uid>/ as the uid with chunked uploads; "
    "the Secret's password is that app password; an agent write reaches the "
    "user's files",
    "hygiene: the app password is in no log, event, thread, annotation, "
    "ConfigMap, connector config, resource document or stored credential column",
    "wrong: a flow as eve fails wrong_account; A stays connected; eve keeps no "
    "app password",
    "revoke: deleted in Nextcloud -> cloud_grant_revoked and revoked; a "
    "reconnect gives a fresh app password",
    "owner: B, an editor of A's personal project, neither sees A's root nor "
    "may name it",
    "delete: A's deletion succeeds, removes the row and the app password",
    "cleanup: sessions, app rows, Keycloak users and client, Nextcloud users; "
    "residue check by gate id",
]


@dataclass
class Account:
    label: str
    api: Api
    email: str
    user_id: str = ""
    default_project: str = ""
    keycloak_started: bool = False


class UserRootGate:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.gate_id = args.gate_id or f"ur-{secrets.token_hex(5)}"
        self.report = Report(self.gate_id)
        self.owner = Api(args.user, args.password)
        self.oauth_client = f"{self.gate_id}-oauth"
        self.client_started = False
        self.a = Account(
            "A",
            Api(self.gate_id, secrets.token_urlsafe(18)),
            f"{self.gate_id}@example.invalid",
        )
        self.b = Account(
            "B",
            Api(f"{self.gate_id}-b", secrets.token_urlsafe(18)),
            f"{self.gate_id}-b@example.invalid",
        )
        self.uid = nextcloud_uid(self.gate_id)
        self.uid_password = secret(secrets.token_urlsafe(24))
        self.eve = f"{self.gate_id}-eve"
        self.eve_password = secret(secrets.token_urlsafe(24))
        self.nc_users: list[str] = []
        self.started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.root = ""
        self.token = ""
        self.token_id: Any = None
        self.threads: list[str] = []
        #: Whose session each one is (only its user may end it).
        self.thread_account: dict[str, Account] = {}
        self.b_member = False
        self.a_deleted = False

    # -- naming --------------------------------------------------------------
    def title(self, label: str) -> str:
        return f"User root gate {self.gate_id} {label}"

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

    def workspace_fstype(self, pod: str, point: str) -> str:
        """The file system on top of ``point`` in the workspace container."""
        rc, out, _err = run(
            K
            + ["exec", pod, "-c", WORKSPACE_CONTAINER, "--"]
            + ["cat", "/proc/self/mountinfo"],
            data="",
            timeout=60,
        )
        return top_mounts(out).get(point, ("", ""))[0] if rc == 0 else ""

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
            f"workspace pod of {thread}", probe, timeout=self.args.pod_timeout
        )

    # -- Nextcloud -------------------------------------------------------------
    def nc(self, script: str, *args: str, data: str | None = None) -> tuple[int, str]:
        """``script`` as www-data in the Nextcloud container; stdin is always
        given (empty without ``data``), never this process's own."""
        for value in args:
            if value.startswith("-"):
                raise GateError("refusing an occ value that reads as an option")
        for _attempt in range(5):
            rc, out, err = run(
                K
                + ["exec", "-i", NEXTCLOUD, "-c", NEXTCLOUD_CONTAINER, "--"]
                + ["su", "-s", "/bin/sh", "www-data", "-c", script, "occ", *args],
                data=data if data is not None else "",
                timeout=180,
            )
            # A silent failure is the exec's or the bundled Nextcloud's SQLite
            # ("database is locked", logged, not printed): asked again.
            if rc == 0 or out.strip() or err.strip():
                break
            time.sleep(5)
        return rc, out if rc == 0 else (out + "\n" + err).strip()

    def nc_tokens(self, uid: str) -> list[dict[str, Any]]:
        if not _NC_UID_RE.fullmatch(uid):
            raise GateError("refusing an unexpected Nextcloud user")
        rc, out = self.nc(_OCC_TOKENS, uid)
        if rc:
            return []
        try:
            return json.loads(out)
        except json.JSONDecodeError:
            return []

    def nc_add_user(self, uid: str, password: str, email: str) -> None:
        if not _NC_UID_RE.fullmatch(uid):
            raise GateError("refusing an unexpected Nextcloud user")
        rc, out = self.nc(_OCC_USER_INFO, uid)
        if rc == 0 and out.strip():
            raise GateError(f"a Nextcloud user {uid} already exists")
        self.nc_users.append(uid)
        rc, out = self.nc(_OCC_ADD_USER, uid, f"UR gate {uid[:12]}", data=password)
        if rc and not self.nc(_OCC_USER_INFO, uid)[1].strip().startswith("{"):
            raise GateError(f"occ user:add {uid} failed: {out[-200:]}")
        for _attempt in range(3):
            rc, out = self.nc(_OCC_SET_EMAIL, uid, email)
            if rc == 0:
                break
            time.sleep(3)
        if rc:
            raise GateError(f"occ user:setting {uid} failed: {out[-200:]}")

    def nc_file(self, uid: str, name: str) -> str | None:
        if not _NC_UID_RE.fullmatch(uid) or not _FILE_RE.fullmatch(name):
            raise GateError("refusing an unexpected user or file name")
        rc, out = self.nc(_NC_FILE, uid, name)
        return out.strip() or None if rc == 0 else None

    def complete_login_flow(self, login_url: str, login: str, password: str) -> None:
        """Log into Nextcloud with a password and grant the flow, as a browser
        does: login form, the flow's picker, its grant page, the grant."""
        import httpx

        base = self.args.nextcloud_url.rstrip("/")
        if urlsplit(login_url).netloc != urlsplit(base).netloc:
            raise GateError(f"the login page is not on {base}: {login_url[:80]}")
        with httpx.Client(
            verify=False,
            follow_redirects=True,
            timeout=30,
            headers={"User-Agent": "Mozilla/5.0 (user root gate)"},
        ) as client:
            page = client.get(base + "/login")
            token = page_request_token(page.text)
            if not token:
                raise GateError("no request token on the Nextcloud login page")
            logged = client.post(
                base + "/login",
                data={
                    "user": login,
                    "password": password,
                    "requesttoken": token,
                    "timezone": "UTC",
                    "timezone_offset": "0",
                },
                headers={"Origin": base, "Referer": base + "/login"},
            )
            if urlsplit(str(logged.url)).path.startswith("/login"):
                raise GateError("the Nextcloud password login was refused")
            picker = client.get(login_url)
            action = picker_action(picker.text)
            if not action:
                raise GateError("no Log in form on the login flow's picker page")
            grant_page = client.get(action)
            state = form_value(grant_page.text, "stateToken")
            request_token = form_value(grant_page.text, "requesttoken")
            if grant_page.status_code != 200 or not state or not request_token:
                raise GateError(
                    f"the grant page answered HTTP {grant_page.status_code}"
                )
            granted = client.post(
                base + "/login/v2/grant",
                data={"stateToken": state, "requesttoken": request_token},
                headers={"Origin": base, "Referer": action},
            )
            if granted.status_code != 200:
                raise GateError(f"the grant answered HTTP {granted.status_code}")

    # -- accounts --------------------------------------------------------------
    def keycloak(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = in_orchestrator(
            _KEYCLOAK_PROGRAM,
            {"client": self.oauth_client, "marker": self.gate_id, **payload},
            timeout=120,
        )
        if result.get("error"):
            raise GateError(f"Keycloak {payload.get('action')}: {result['error']}")
        return result

    def admit(self, account: Account) -> None:
        account.keycloak_started = True
        created = self.keycloak(
            {
                "action": "create-user",
                "username": account.api.username,
                "email": account.email,
                "password": account.api.password,
            }
        )
        if created.get("exists"):
            account.keycloak_started = False  # not this run's: never adopted
            raise GateError(f"a Keycloak user {account.api.username} already exists")
        account.api.client_id = self.oauth_client
        me = account.api.ok("GET", "/api/auth/me")["user"]
        account.user_id = str(me["id"])
        if not _UUID_RE.fullmatch(account.user_id):
            raise GateError(f"account {account.label} got no app row")
        self.owner.ok(
            "PATCH", f"/api/admin/users/{account.user_id}", {"is_approved": True}
        )
        account.default_project = sql(
            f"SELECT default_project_id FROM users WHERE id = {lit(account.user_id)}"
        )
        if not _UUID_RE.fullmatch(account.default_project):
            raise GateError(f"account {account.label} has no personal project")

    def accounts(self) -> None:
        self.client_started = True
        created = self.keycloak({"action": "create-client"})
        if created.get("exists"):
            self.client_started = False
            raise GateError(f"a Keycloak client {self.oauth_client} already exists")
        # Everyone logs in through the run's client: its access token carries
        # the e-mail and the realm roles (admin-cli's does not, and a login
        # without them would demote the owner and give an account a new row).
        self.owner.client_id = self.oauth_client
        owner = self.owner.ok("GET", "/api/auth/me")["user"]
        if not owner.get("is_admin"):
            raise GateError("the owner must be an administrator (it approves)")
        self.admit(self.a)
        self.admit(self.b)

    # -- phases ------------------------------------------------------------
    def preflight(self) -> None:
        problems = self.served_problems()
        self.report.check(
            "preflight: every orchestrator pod serves this checkout's slice 4 "
            "modules byte for byte",
            not problems,
            "; ".join(problems),
        )
        env = in_orchestrator(_ENV_PROGRAM, {})
        self.report.check(
            "preflight: the active main cloud is Nextcloud and user roots are on",
            env.get("backend") == "nextcloud"
            and env.get("grant_mode", "") in ("", "login_flow"),
            json.dumps(env),
        )
        rc, out, _err = run(
            ["curl", "-sk", "-o", "/dev/null", "-w", "%{http_code}"]
            + [self.args.nextcloud_url.rstrip("/") + "/status.php"],
            timeout=30,
        )
        self.report.check(
            "preflight: the public Nextcloud URL answers here", out == "200", out
        )

    def row_phase(self) -> None:
        self.root = sql(
            "SELECT id FROM datasources WHERE managed_key = "
            f"{lit('user-cloud-root:' + self.a.user_id)}"
        )
        row = (
            sql_json(
                "SELECT json_build_object('type', type, 'config', config, "
                "'auto_attach', auto_attach, 'scope', scope_mode, 'created_by', "
                "created_by, 'empty', credentials::text = '{}', 'links', "
                "(SELECT json_agg(project_id) FROM project_datasources WHERE "
                f"datasource_id = d.id)) FROM datasources d WHERE id = {lit(self.root)}"
            )
            if _UUID_RE.fullmatch(self.root)
            else None
        ) or {}
        active = sql(
            "SELECT backend_instance_id FROM main_cloud_active_backend WHERE singleton"
        )
        config = row.get("config") or {}
        self.report.check(
            "row: A's approval created its cloud root (cloud_folder, user_root, "
            "the active installation, A's personal project only, auto-attach, no "
            "credential, created by A)",
            row.get("type") == "cloud_folder"
            and config.get("folder_kind") == "user_root"
            and config.get("backend_instance_id") == active
            and row.get("links") == [self.a.default_project]
            and row.get("auto_attach") is True
            and row.get("scope") == "projects"
            and row.get("empty") is True
            and row.get("created_by") == self.a.user_id,
            json.dumps({k: v for k, v in row.items() if k != "config"})[:300],
        )

    def start_session(self, label: str, account: Account, **body: Any) -> str:
        created = account.api.ok(
            "POST",
            "/api/persistent/threads",
            {
                "title": self.title(label),
                "project_id": account.default_project,
                "config_override": {"workspace": {"backend": "sandbox"}},
                "model": self.args.model,
                **body,
            },
        )
        thread = str(created.get("thread_id") or created["id"])
        self.threads.append(thread)
        self.thread_account[thread] = account
        print(f"session {label} {thread}", flush=True)
        return thread

    def session_plan(self, thread: str) -> tuple[dict, dict]:
        pod = self.workspace_pod(thread)
        plan = recorded_plan(pod)
        if plan is None:
            raise GateError(f"the workspace pod of {thread} has no plan annotation")
        return pod, plan

    def end_session(self, thread: str) -> bool:
        def gone() -> bool:
            # Only a session's own user may end it.
            account = self.thread_account.get(thread, self.a)
            status, _body = account.api.call(
                "DELETE", f"/api/persistent/threads/{thread}?force=true&permanent=true"
            )
            if status == 404:
                return True
            return sql(f"SELECT count(*) FROM threads WHERE id = {lit(thread)}") == "0"

        try:
            wait_for("session deleted", gone, timeout=900, interval=10)
        except GateError:
            return False
        if thread in self.threads:
            self.threads.remove(thread)
        return True

    def status(self) -> dict[str, Any]:
        return self.a.api.ok("GET", "/api/me/cloud-root")

    def waiting_phase(self) -> None:
        status = self.status()
        self.report.check(
            "waiting: GET /api/me/cloud-root says not_connected",
            status.get("supported") is True
            and status.get("state") == "not_connected"
            and status.get("connector_id") == self.root,
            json.dumps(status)[:300],
        )
        eligible = self.a.api.ok(
            "GET",
            f"/api/datasources/eligible?project_id={self.a.default_project}"
            "&execution=session&workspace_backend=sandbox",
        )
        mine = [e for e in eligible if str(e.get("id")) == self.root]
        self.report.check(
            "waiting: A's eligible list for the personal project has My cloud, "
            "selected by default",
            len(mine) == 1 and mine[0].get("default_selected") is True,
            json.dumps(mine)[:200],
        )
        thread = self.start_session("waiting", self.a)
        selected = sql_json(
            f"SELECT metadata->'datasource_ids' FROM threads WHERE id = {lit(thread)}"
        )
        _pod, plan = self.session_plan(thread)
        reason = root_exclusion(plan, self.root)
        substitutes = substitute_problems(plan)
        self.report.check(
            "waiting: a session with no list attaches it; its plan excludes it as "
            "cloud_not_connected and mounts nothing in its place",
            self.root in (selected or [])
            and reason == "cloud_not_connected"
            and not substitutes,
            f"reason={reason} substitutes={substitutes}",
        )
        self.end_session(thread)

    def connect(self, login: str, password: str) -> dict[str, Any]:
        started = self.a.api.ok("POST", "/api/me/cloud-root/connect")
        login_url = str(started.get("login_url") or "")
        self.complete_login_flow(login_url, login, password)
        return started

    def wait_state(self, predicate: Callable[[dict], bool]) -> dict[str, Any]:
        found: dict[str, Any] = {}

        def probe() -> bool:
            nonlocal found
            found = self.status()
            return predicate(found)

        try:
            wait_for("cloud root state", probe, timeout=60, interval=3)
        except GateError:
            pass
        return found

    def connect_phase(self) -> None:
        self.nc_add_user(self.uid, self.uid_password, self.a.email)
        started = self.connect(self.uid, self.uid_password)
        self.report.check(
            "connect: POST /api/me/cloud-root/connect answers a login page on the "
            "public Nextcloud host",
            str(started.get("login_url") or "").startswith(
                self.args.nextcloud_url.rstrip("/") + "/login/v2/flow/"
            )
            and started.get("state") == "connecting",
            str(started.get("login_url") or "")[:60],
        )
        status = self.wait_state(lambda s: s.get("state") == "connected")
        self.report.check(
            "connect: the flow completed as A's Nextcloud account connects the "
            "root within 60 s",
            status.get("state") == "connected" and status.get("account_id") == self.uid,
            json.dumps(status)[:300],
        )
        tokens = device_tokens(self.nc_tokens(self.uid))
        self.token_id = tokens[0].get("id") if len(tokens) == 1 else None
        self.report.check(
            f"connect: the account has exactly one app password named {DEVICE_NAME!r}",
            len(tokens) == 1,
            f"{len(tokens)} found",
        )
        events = sql(
            "SELECT string_agg(event_type, ',' ORDER BY created_at) FROM "
            f"security_events WHERE resource_id = {lit(self.root)}"
        )
        self.report.check(
            "connect: a cloud_root_granted security event",
            "cloud_root_granted" in (events or ""),
            events,
        )

    def mount_phase(self) -> None:
        thread = self.start_session("mount", self.a)
        pod, plan = self.session_plan(thread)
        problems = root_mount_problems(plan, self.root, self.uid)
        self.report.check(
            "mount: the plan mounts my_cloud from the account's /dav/files/ as the "
            "uid, read-write, with chunked uploads",
            not problems,
            "; ".join(problems),
        )
        secret_name = credential_secret_name(pod) or ""
        conf = ""
        if secret_name:
            body = json.loads(
                raw_output(K + ["get", "secret", secret_name, "-o", "json"])
            )
            conf = base64.b64decode(
                (body.get("data") or {}).get("rclone.conf") or ""
            ).decode("utf-8", "replace")
        index = next(
            (
                str(m.get("index"))
                for m in plan.get("mounts") or []
                if m.get("source_ref") == self.root
            ),
            "",
        )
        password = ""
        for name, values in parse_rclone_conf(conf).items():
            if name == f"m{index}" and values.get("pass"):
                password = reveal(values["pass"])
        self.token = secret(password) if password else ""
        self.report.check(
            "mount: the Pod's credential Secret holds the root's own app password",
            bool(self.token) and len(self.token) >= 20,
            secret_name,
        )
        name = f"ur-gate-{self.gate_id[3:]}.txt"
        content = f"user root gate {self.gate_id}"
        pod_name = pod["metadata"]["name"]
        try:
            wait_for(
                "my_cloud mounted in the workspace",
                lambda: self.workspace_fstype(pod_name, f"/cloud/{MOUNT_NAME}")
                == "fuse.rclone",
                timeout=180,
                interval=5,
            )
        except GateError:
            pass
        # The gate id and the name are [a-z0-9-.] only: nothing to quote.
        rc, _out, err = run(
            K
            + ["exec", pod_name, "-c", WORKSPACE_CONTAINER, "--"]
            + ["sh", "-c", _AS_AGENT, "gate"]
            + [f"printf %s '{content}' > /cloud/{MOUNT_NAME}/{name}"],
            data="",
            timeout=180,
        )
        digest = hashlib.sha256(content.encode()).hexdigest()
        try:
            stored = wait_for(
                "the file in the user's Nextcloud files",
                lambda: self.nc_file(self.uid, name),
                timeout=180,
                interval=5,
            )
        except GateError:
            stored = None
        self.report.check(
            "mount: a file the agent account writes at cloud/my_cloud/ appears in "
            "the user's own Nextcloud files",
            rc == 0 and stored == digest,
            err.strip()[-200:] if rc else "",
        )
        self.hygiene_phase(thread, pod)
        self.end_session(thread)

    def hygiene_phase(self, thread: str, pod: dict) -> None:
        if not self.token:
            self.report.check("hygiene: the app password is known", False)
            return
        token = self.token
        places: list[str] = []
        logs = raw_output(
            K
            + ["logs", ORCHESTRATOR, "-c", ORCHESTRATOR_CONTAINER]
            + [f"--since-time={self.started}"],
            timeout=180,
        )
        if token in logs:
            places.append("orchestrator log")
        annotations = json.dumps((pod.get("metadata") or {}).get("annotations") or {})
        if token in annotations:
            places.append("pod annotations")
        configmaps = raw_output(K + ["get", "configmaps", "-o", "json"], timeout=180)
        if token in configmaps:
            places.append("a ConfigMap")
        for table, column in (
            ("security_events", "detail"),
            ("threads", "metadata"),
            ("datasources", "config"),
            ("datasources", "credentials"),
            ("srw_resources", "document"),
        ):
            if sql_count_containing(table, column, token):
                places.append(f"{table}.{column}")
        self.report.check(
            "hygiene: the app password is in no orchestrator log, pod annotation, "
            "ConfigMap, security event, thread, connector config, resource "
            "document or stored credential column",
            not places,
            ", ".join(places),
        )

    def wrong_phase(self) -> None:
        self.nc_add_user(self.eve, self.eve_password, f"{self.eve}@example.invalid")
        self.connect(self.eve, self.eve_password)
        status = self.wait_state(lambda s: (s.get("failure") or {}).get("reason"))
        tokens = device_tokens(self.nc_tokens(self.uid))
        self.report.check(
            "wrong: a flow completed as another person's account fails "
            "wrong_account; A stays connected with the same app password",
            (status.get("failure") or {}).get("reason") == "wrong_account"
            and status.get("state") == "connected"
            and [t.get("id") for t in tokens] == [self.token_id],
            json.dumps(status.get("failure"))[:200],
        )
        self.report.check(
            "wrong: the other account keeps no 'Superhuman Remote Worker' app "
            "password (SRW revoked it)",
            not device_tokens(self.nc_tokens(self.eve)),
        )

    def revoke_phase(self) -> None:
        if self.token_id is None:
            self.report.check("revoke: the app password's id is known", False)
            return
        rc, out = self.nc(_OCC_DELETE_TOKEN, self.uid, str(self.token_id))
        if rc:
            raise GateError(f"occ user:auth-tokens:delete failed: {out[-200:]}")
        thread = self.start_session("revoked", self.a)
        _pod, plan = self.session_plan(thread)
        reason = root_exclusion(plan, self.root)
        status = self.status()
        self.report.check(
            "revoke: after its app password was deleted in Nextcloud, a new "
            "session's plan excludes the root as cloud_grant_revoked and the state "
            "is revoked (rejected_by_cloud)",
            reason == "cloud_grant_revoked"
            and status.get("state") == "revoked"
            and status.get("revoked_reason") == "rejected_by_cloud",
            f"reason={reason} state={status.get('state')}",
        )
        self.end_session(thread)
        old = self.token_id
        self.connect(self.uid, self.uid_password)
        status = self.wait_state(lambda s: s.get("state") == "connected")
        tokens = device_tokens(self.nc_tokens(self.uid))
        self.token_id = tokens[0].get("id") if len(tokens) == 1 else None
        self.report.check(
            "revoke: a reconnect connects again with a fresh app password; the old "
            "one stays gone",
            status.get("state") == "connected"
            and len(tokens) == 1
            and tokens[0].get("id") != old,
            f"{len(tokens)} found",
        )

    def owner_phase(self) -> None:
        self.owner.ok(
            "POST",
            f"/api/projects/{self.a.default_project}/members",
            {"user_id": self.b.user_id, "role": "editor"},
        )
        self.b_member = True
        eligible = self.b.api.ok(
            "GET",
            f"/api/datasources/eligible?project_id={self.a.default_project}"
            "&execution=session",
        )
        self.report.check(
            "owner: B, an editor of A's personal project, does not see A's root",
            self.root not in {str(e.get("id")) for e in eligible},
        )
        status, body = self.b.api.call(
            "POST",
            "/api/persistent/threads",
            {
                "title": self.title("forced"),
                "project_id": self.a.default_project,
                "config_override": {"workspace": {"backend": "sandbox"}},
                "model": self.args.model,
                "datasource_ids": [self.root],
            },
        )
        if status in (200, 201):
            forced = str(body.get("thread_id") or body.get("id"))
            self.threads.append(forced)
            self.thread_account[forced] = self.b
        self.report.check(
            "owner: a session B creates there naming A's root is refused",
            400 <= status < 500,
            f"HTTP {status}",
        )

    def delete_phase(self) -> None:
        for thread in list(self.threads):
            self.end_session(thread)
        if self.b_member:
            self.owner.call(
                "DELETE",
                f"/api/projects/{self.a.default_project}/members/{self.b.user_id}",
            )
            self.b_member = False

        def deleted() -> bool:
            status, body = self.owner.call("DELETE", f"/api/users/{self.a.user_id}")
            if status in (200, 204):
                return True
            if status == 409:  # a session's workspace is still releasing
                return False
            raise GateError(f"user delete -> HTTP {status}: {str(body)[:200]}")

        try:
            ok = bool(wait_for("A deleted", deleted, timeout=300, interval=10))
        except GateError:
            ok = False
        self.a_deleted = ok
        left = sql(
            "SELECT count(*) FROM datasources WHERE managed_key = "
            f"{lit('user-cloud-root:' + self.a.user_id)}"
        )
        tokens = device_tokens(self.nc_tokens(self.uid))
        self.report.check(
            "delete: A's deletion succeeds and removes the row and the app password",
            ok and left == "0" and not tokens,
            f"deleted={ok} rows={left} tokens={len(tokens)}",
        )

    # -- cleanup -------------------------------------------------------------
    def cleanup(self) -> list[str]:
        problems: list[str] = []

        def step(label: str, action: Callable[[], Any]) -> None:
            try:
                if action() is False:
                    problems.append(label)
            except Exception as exc:  # noqa: BLE001 - every step is attempted
                problems.append(f"{label} ({_scrub(str(exc))[:160]})")

        for thread in list(self.threads):
            step(f"delete session {thread}", lambda t=thread: self.end_session(t))
        if self.b_member:
            step(
                "remove B from A's project",
                lambda: self.owner.call(
                    "DELETE",
                    f"/api/projects/{self.a.default_project}/members/{self.b.user_id}",
                ),
            )
        for account in (self.a, self.b):
            if account.user_id and not (account is self.a and self.a_deleted):

                def delete(account=account) -> bool:
                    status, _body = self.owner.call(
                        "DELETE", f"/api/users/{account.user_id}"
                    )
                    return status in (200, 204, 404)

                step(
                    f"delete account {account.label}",
                    lambda d=delete: bool(
                        wait_for("account deleted", d, timeout=300, interval=10)
                    ),
                )
        for uid in list(self.nc_users):
            step(
                f"delete Nextcloud user {uid[:16]}",
                lambda u=uid: self.nc(_OCC_DELETE_USER, u)[0] == 0,
            )
        started = [a for a in (self.a, self.b) if a.keycloak_started]
        if started or self.client_started:

            def keycloak_deleted() -> bool:
                result = self.keycloak(
                    {
                        "action": "delete",
                        "users": [
                            {"username": a.api.username, "email": a.email}
                            for a in started
                        ],
                        "client_started": self.client_started,
                    }
                )
                return not result.get("refused") and not (
                    result.get("users") or result.get("clients")
                )

            step("delete the Keycloak users and OAuth client", keycloak_deleted)
        for problem in problems:
            print(f"cleanup: {problem} failed", flush=True)
        return problems

    def residue(self) -> list[str]:
        left: list[str] = []
        prefix = f"User root gate {self.gate_id} %"
        count = sql(f"SELECT count(*) FROM threads WHERE title LIKE {lit(prefix)}")
        if count not in ("", "0"):
            left.append(f"{count} sessions")
        emails = [self.a.email, self.b.email]
        count = sql(
            "SELECT count(*) FROM users WHERE lower(email) IN ("
            + ", ".join(lit(e) for e in emails)
            + ")"
        )
        if count != "0":
            left.append(f"{count} app rows")
        for uid in (self.uid, self.eve):
            rc, out = self.nc(_OCC_USER_INFO, uid)
            if rc == 0 and out.strip().startswith("{"):
                left.append(f"Nextcloud user {uid[:16]}")
        return left

    def relogin(self, account: Account) -> None:
        """Log in as one of the run's accounts again, with a fresh password.
        If its Keycloak user is already gone, a new one is created under the
        same name and e-mail and its app row relinked by e-mail (the first
        login of a fresh Keycloak user links a row without a sub)."""
        account.api.password = secret(secrets.token_urlsafe(18))
        payload = {
            "username": account.api.username,
            "email": account.email,
            "password": account.api.password,
        }
        if not self.keycloak({"action": "reset-password", **payload}).get("reset"):
            if not account.user_id:
                return
            sql(
                f"UPDATE users SET keycloak_sub = NULL WHERE id = {lit(account.user_id)}"
            )
            self.keycloak({"action": "create-user", **payload})
        account.api.client_id = self.oauth_client
        me = account.api.ok("GET", "/api/auth/me")["user"]
        if str(me.get("id")) != account.user_id:
            raise GateError(
                f"account {account.label} logged in as {me.get('id')}, not its row"
            )

    def threads_left(self) -> bool:
        prefix = f"User root gate {self.gate_id} %"
        count = sql(f"SELECT count(*) FROM threads WHERE title LIKE {lit(prefix)}")
        return count not in ("", "0")

    def sweep(self) -> int:
        """Remove what an earlier run of this gate id left behind, found by
        name: its sessions, app rows, Nextcloud users, Keycloak users and
        client."""
        self.client_started = True
        for account in (self.a, self.b):
            account.keycloak_started = True
            found = sql(
                f"SELECT id FROM users WHERE lower(email) = lower({lit(account.email)})"
            )
            account.user_id = found if _UUID_RE.fullmatch(found or "") else ""
            if account.user_id:
                account.default_project = sql(
                    "SELECT default_project_id FROM users WHERE id = "
                    f"{lit(account.user_id)}"
                )
        # The run's client, made again if it is gone: every login goes
        # through it.
        self.keycloak({"action": "create-client"})
        self.owner.client_id = self.oauth_client
        if self.threads_left():
            # Its sessions end only as their user: fresh passwords, this run's.
            for account in (self.a, self.b):
                self.relogin(account)
        prefix = f"User root gate {self.gate_id} %"
        for line in sql(
            f"SELECT id || '|' || user_id FROM threads WHERE title LIKE {lit(prefix)}"
        ).splitlines():
            thread, _, user_id = line.partition("|")
            if _UUID_RE.fullmatch(thread):
                self.threads.append(thread)
                self.thread_account[thread] = (
                    self.b if user_id == self.b.user_id else self.a
                )
        for uid in (self.uid, self.eve):
            rc, out = self.nc(_OCC_USER_INFO, uid)
            if rc == 0 and out.strip().startswith("{"):
                self.nc_users.append(uid)
        self.b_member = bool(self.a.default_project and self.b.user_id)
        problems = self.cleanup()
        left = self.residue()
        self.report.check(
            "sweep: nothing of this gate id is left",
            not problems and not left,
            "; ".join(problems + left),
        )
        return 0 if self.report.passed else 1

    # -- run ---------------------------------------------------------------
    def run(self) -> int:
        try:
            self.preflight()
            self.accounts()
            self.row_phase()
            self.waiting_phase()
            self.connect_phase()
            self.mount_phase()
            self.wrong_phase()
            self.revoke_phase()
            self.owner_phase()
            self.delete_phase()
        except GateError as exc:
            self.report.check("gate infrastructure", False, str(exc))
        finally:
            if not self.args.keep:
                problems = self.cleanup()
                left = self.residue()
                self.report.check(
                    "cleanup: nothing this run created is left",
                    not problems and not left,
                    "; ".join(problems + left),
                )
        print(
            f"{'PASS' if self.report.passed else 'FAIL'} user root gate "
            f"{self.gate_id}: {sum(ok for _, ok, _ in self.report.results)}/"
            f"{len(self.report.results)}",
            flush=True,
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
    parser.add_argument("--nextcloud-url", default="https://cloud.localhost")
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
        raise SafetyError("--gate-id must be ur- followed by 10 hex digits")
    if args.sweep and (not args.run or args.gate_id is None or args.keep):
        raise SafetyError("--sweep needs --run, --confirm and --gate-id, not --keep")
    if not _MODEL_RE.fullmatch(args.model):
        raise SafetyError("model id is malformed")
    if not _USER_RE.fullmatch(args.user):
        raise SafetyError("user name is malformed")
    host = urlsplit(args.nextcloud_url).hostname or ""
    if urlsplit(args.nextcloud_url).scheme != "https" or not (
        host == "localhost" or host.endswith(".localhost")
    ):
        raise SafetyError("--nextcloud-url must be an https://*.localhost URL")
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
        print("Plan (dry run; add --run --confirm LOCAL-K3D-DISPOSABLE):")
        for line in PLAN:
            print(f"  - {line}")
        return 0
    gate = UserRootGate(args)
    return gate.sweep() if args.sweep else gate.run()


if __name__ == "__main__":
    raise SystemExit(main())
