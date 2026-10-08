#!/usr/bin/env python3
"""Local k3d gate for connector drivers D6: registered bind-time image drivers.

Design: knowledge-base/knowledge/features/connector_drivers.md, "Trust and
registration", "The driver namespace baseline", "Driver versions", "Three
planes" (bind-time) and slice D6, whose gate this proves: a custom driver
image registered at Account scope delivers an env binding to a workspace and
runs unprivileged; a moved tag with an incompatible spec is refused at bind;
what a driver returns cannot run code in the workspace; nothing it minted
outlives its binding. Templates: scripts/k3d-service-driver-gate.py (D5: the
hosting preflights, the moved tag pushed to the k3d registry),
scripts/k3d-managed-mcp-gate.py (D5a: disposable OAuth client and accounts)
and scripts/parallel-subagents-k3d-gate.py (a pinned session: an Officer
conference). The same safety envelope: dry-run by default, the exact
k3d-srw/srw context, secrets only on ``kubectl exec -i`` stdin and scrubbed
from every printed line, every in-pod program capping its own memory, and a
cleanup in ``finally`` that touches only what this run created and then
checks for residue by gate id.

It needs the k3d profile of deployment/values-local.yaml.example (keys in
--help), Tilt (which builds the shim and pins it by digest) and a local
docker that can push to localhost:5005. The driver is SRW's example driver
(docker/Dockerfile.driver-example, example.env/v1), built by this gate with
its io.srw.driver.spec label from drivers/example/spec.json: a custom image,
outside srw.* and outside any trusted repository. Its ``misbehave`` config
makes a bind return what SRW refuses, or fail.

Fixtures (all disposable, named after the gate id):

  client      ``<gate id>-oauth``, a public Keycloak client the accounts log
              in with (the D3c/D5a fixture)
  accounts    ``<gate id>-ed`` (a Project editor) and ``<gate id>-vw`` (a
              Project viewer, and the "other user" of the Account checks):
              Keycloak users and app rows admitted before their first login
  images      localhost:5005/srw-driver-example:<gate id> (the compatible
              build, the incompatible one under the same tag, then the
              compatible one again) and :<gate id>-srw (a label naming
              srw.example/v1)
  project     one project of the owner, the editor and the viewer members
  connectors  of the owner's Account registration: ``example`` (a file too),
              ``denied``, ``undeclared`` and ``file`` (each misbehaving one
              way), ``fail`` (fails with a config error) and ``doomed``
              (deleted while bound)
  sessions    ``one`` (bound, then detached live), ``two`` (after the tag
              moved), ``refusals`` (the misbehaving connectors and
              ``doomed``), ``pinned`` (an Officer conference: the pinned
              lane, with ``example`` and ``fail``) and ``disabled`` (after
              the registration was disabled)
  job         ``job`` (``--job-lane``, pinned by default), with ``example``

Checks (each printed PASS/FAIL; the exit status is 0 only if all pass):

  preflight   every orchestrator pod and stateless agent pod serves this
              checkout's D6 modules, byte for byte; driver pods on, with an
              exchange and canary port, a digest-pinned shim, the k3d
              registry over HTTP, room for bind-time pods, custom privilege
              off and the k3d registry not trusted; migration 0420 applied;
              the driver namespace has Pod Security baseline, its default deny
              and the Terminating (bind-time) pod quota, and the cluster
              enforces NetworkPolicy (12 probes); docker pushes to the k3d
              registry
  register    the owner registers the image at Account scope: the label's
              spec and declared variables, the pushed digest, tier custom, no
              privilege; an image naming srw.example/v1 is refused ("SRW's
              own"); the viewer (another user) neither lists nor reads it, and
              cannot create a connector of it by id or by name; the project
              editor registers at Project scope, the viewer cannot (403) but
              lists it; the owner's capability matrix shows the registration,
              the viewer's does not
  bind        session one binds ``example``: one bind-time pod ran,
              unprivileged (restartPolicy Never, a deadline, no
              ServiceAccount token, every capability dropped, no privilege
              escalation, seccomp RuntimeDefault, no host namespaces, the
              image by its digest under the shim), its Secret held only
              request.json and the identity, and its NetworkPolicy existed
              while it ran: it selects the pod, admits nothing and reaches
              only the lease exchange's port; inside the driver, the kernel
              reported UID 10001, no effective or bounding capability,
              no_new_privs and seccomp filtering (EXAMPLE_DRIVER_PROCESS); the
              workspace has the variable the driver minted for this binding
              (never the connector's token) and its credential file (0600);
              the binding records reference, digest, resolution time, spec
              hash and protocol version; afterwards the pod, its Secret and
              its policy are gone and the operation is recorded removed
  identity    the bind pod's sdi_ identity is refused by the lease exchange
              and its introspection (unknown_driver_identity), and a replay
              of its result is refused (409 operation_closed)
  moved-tag   an incompatible image is pushed under the same tag; session
              two's bind is refused without a pod ("changed its contract",
              the removed slot and the new required config named), recorded
              on the binding with the new digest, shown on the connector
              (driver_status.last_bind) and in session two's README, and
              audited; the compatible image is pushed back
  detach      session one's connector is detached live: its binding is
              revoking at once (connector_detached) and revoked by a revoke
              pod within a reconciler pass, which received the binding's own
              inputs and driver_state, and is gone afterwards
  refusals    a variable a driver may not set, one the spec does not declare
              and a file outside ~/.srw-files/, ~/.netrc and ~/.pgpass are
              each refused at bind with the reason on the connector and in
              the README, and what the bind minted is revoked
              (binding_refused); ``doomed``, bound, is deleted: its binding is
              still revoked by a revoke pod (connector_deleted) with the
              inputs it was bound with
  pinned      a pinned session (an Officer conference) binds and delivers
              ``example``, and ``fail`` fails for good without holding it:
              the README and the connector say why, the session answers a
              turn, and no attach of it logged a connector delivery failure
  job         a job (``--job-lane``) is held until ``example`` is bound,
              delivers it to its workspace, runs to an end, and its binding
              is revoked (execution_ended) in a revoke pod
  disable     the owner disables the registration: the viewer may not, the
              connector says "registration disabled", the pinned session's
              live binding is revoked (registration_disabled), and a new
              session's bind is refused with the reason, without a pod
  cleanup     jobs, sessions, connectors, registrations (once their bindings
              are revoked), project, accounts, OAuth client, probe pod, the
              gate's registry tags and image rows are gone; no binding of the
              run's connectors is unrevoked and no driver-namespace object
              names one of them

Run with the repository venv on the k3d-srw cluster, alone: this is a
mutating gate. The owner (--user) must be an administrator (it admits and
deletes the disposable accounts) and must own no open Officer conference
(the gate's own project holds the pinned session).

  .venv/bin/python scripts/k3d-custom-driver-gate.py           # plan
  .venv/bin/python scripts/k3d-custom-driver-gate.py \\
      --run --confirm LOCAL-K3D-DISPOSABLE
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import subprocess
import sys
import threading
import time
import urllib.request
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
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
HOME = "/home/agent-host"
DRIVER = "example.env/v1"
IMAGE_TYPE = "image_driver"
GATE_LABEL = "srw.io/gate"
ACCOUNT_DOMAIN = "example.invalid"
LOCAL_REGISTRY = "localhost:5005"
#: What the k3d registry may answer for a pushed digest: an image index
#: (docker's containerd image store) or a single image manifest.
INDEX_TYPES = (
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
)
MANIFEST_TYPES = (
    *INDEX_TYPES,
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
)
CLUSTER_REGISTRY = "srw-registry:5000"
REPOSITORY = "srw-driver-example"
SPEC_FILE = ROOT / "drivers/example/spec.json"
DOCKERFILE = "docker/Dockerfile.driver-example"
SPEC_LABEL = "io.srw.driver.spec"
BIND_TIME_MANAGER = "connector-bind-time"
#: The UID docker/Dockerfile.driver-example runs the driver as.
DRIVER_UID = "10001"
#: What the example driver sets (drivers/example/spec.json, env_names) and
#: where its file lands.
TOKEN_VARIABLE = "EXAMPLE_TOKEN"
FILE_VARIABLE = "EXAMPLE_TOKEN_FILE"
TOKEN_FILE = "~/.srw-files/example/token"
#: The lease exchange's routes and the result route, on the exchange port.
EXCHANGE_PATH = "/v1/leases/exchange"
INTROSPECT_PATH = "/v1/leases/introspect"
RESULT_PATH = "/v1/drivers/result"
#: A pinned session's agent serves its WebSocket here (the settings pane's).
AGENT_PORT = 8001
#: The largest WebSocket frame the live-update reader accepts.
WS_MAX_FRAME = 8 << 20
#: What the workspace script prints for a variable that is not set.
EMPTY_SHA = hashlib.sha256(b"").hexdigest()
#: The orchestrator's log line when the dispatcher hands a job on, per lane:
#: after the bind gate let it through.
DISPATCH_LINES = {
    "pinned": "Dispatch: assigned job {job}",
    "stateless": "Dispatcher: admitted stateless worker job {job}",
}
_POD_NAME_RE = re.compile(r"[a-z0-9]([-a-z0-9]{0,251}[a-z0-9])?\Z")
#: The example driver's misbehaviours (its ``misbehave`` config) and the
#: reason SRW gives for refusing each binding.
REFUSALS = {
    "denied": (
        "denied_variable",
        "GIT_SSH_COMMAND is not a variable a driver may set",
    ),
    "undeclared": (
        "undeclared_variable",
        "sets EXAMPLE_UNDECLARED, which the driver's spec does not declare",
    ),
    "file": ("refused_file", "a driver's file goes to ~/.srw-files/"),
}
#: The ``fail`` connector's bind error, a final ``config`` failure.
FAILING = "Told to fail (misbehave)"
#: What a session's README says of a connector it was not delivered.
NOTICE = "Not delivered"
PINNED_LANE = "pinned"
JOB_TERMINAL = frozenset({"completed", "failed", "cancelled", "pending_review"})
#: The k3d registry's container (scripts/local-dev-up.sh) and where it keeps
#: a repository's tags; the gate removes its own tags' links only (the
#: service-driver gate's way).
REGISTRY_CONTAINER = "srw-registry"
REGISTRY_TAGS = (
    "/var/lib/registry/docker/registry/v2/repositories/{repository}/_manifests/tags"
)
_SELECTOR = (
    "app.kubernetes.io/instance=srw,app.kubernetes.io/name=superhuman-remote-worker"
)
_GATE_ID_RE = re.compile(r"d6-[0-9a-f]{10}\Z")
_MODEL_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@-]{0,199}\Z")
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")
_DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}\Z")
_IPV4_RE = re.compile(r"(?:\d{1,3}\.){3}\d{1,3}\Z")
#: The migration this gate needs.
MIGRATIONS = ("0420_connector_driver_registrations.sql",)

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
            "src/orchestrator/application/connectors.py",
            "src/orchestrator/application/background_tasks.py",
            "src/orchestrator/application/settings.py",
            "src/orchestrator/routers/connector_drivers.py",
            "src/orchestrator/routers/connector_lease_exchange.py",
            "src/orchestrator/services/connector_bind_time.py",
            "src/orchestrator/services/connector_bind_time_launch.py",
            "src/orchestrator/services/connector_credential_leases.py",
            "src/orchestrator/services/connector_driver_imports.py",
            "src/orchestrator/services/connector_driver_registrations.py",
            "src/orchestrator/services/connector_service_images.py",
            "src/orchestrator/services/datasources.py",
            "src/orchestrator/services/job_dispatcher.py",
            "src/orchestrator/services/job_start_bundle.py",
            "src/orchestrator/services/session_attach_binding.py",
            "src/orchestrator/services/thread_config_update.py",
            "src/orchestrator/services/unit_claim_bundle.py",
            *(
                f"src/orchestrator/database/migrations/app/{name}"
                for name in MIGRATIONS
            ),
        ),
    ),
    # The agent materializes the binding: the stored type's spec and the
    # environment and credential-file materializers.
    ServedSet(
        "stateless agent",
        "agent-stateless",
        AGENT_CONTAINER,
        (SHARED_CONNECTORS,),
        (
            "src/agent/connectors/env.py",
            "src/agent/connectors/files.py",
            "src/agent/connectors/legacy.py",
            "src/shared/runtime/core/backends/remote.py",
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

# The run's Keycloak fixtures (the D3c/D5a gates' program), through the
# orchestrator's own admin credentials: they stay in the pod and are never
# printed. The client carries this run's marker; each user is named after the
# gate id. Every action finds them by exact name; delete removes only what
# carries this run's marker or email (and its recorded id, once known).
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
    if not request.get("username"):
        return []
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
        "firstName": "D6",
        "lastName": "Gate",
        "requiredActions": [],
        "credentials": [
            {"type": "password", "value": request["password"], "temporary": False}
        ],
    }, users, user_owned)))
elif action == "delete":
    # Users first: the client is how the gate still logs in until the end.
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
# 12-probe harness). Without enforcement every driver pod's canary wait
# refuses to start the driver.
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

# What the workspace holds after a bind, as the agent-host user (the
# variable's value and the file's contents only as hashes), and the README
# lines that say a connector was not delivered.
_WORKSPACE_SCRIPT = r"""
for f in ~/.srw-credentials/*.sh; do [ -r "$f" ] && . "$f"; done
printf 'value_sha=%s\n' "$(printf '%s' "${EXAMPLE_TOKEN:-}" | sha256sum | cut -d' ' -f1)"
printf 'process=%s\n' "${EXAMPLE_DRIVER_PROCESS:-}"
printf 'file_var=%s\n' "${EXAMPLE_TOKEN_FILE:-}"
if [ -e "$GATE_FILE" ]; then
  printf 'file_sha=%s\n' "$(sha256sum < "$GATE_FILE" | cut -d' ' -f1)"
  printf 'file_mode=%s\n' "$(stat -L -c %a "$GATE_FILE")"
  printf 'file_real=%s\n' "$(readlink -f "$GATE_FILE")"
fi
if [ -n "${EXAMPLE_TOKEN_FILE:-}" ]; then
  printf 'var_real=%s\n' "$(readlink -f "$EXAMPLE_TOKEN_FILE")"
fi
printf 'notices=%s\n' "$(grep -rhF --include=README.md 'Not delivered' ~ 2>/dev/null | tr '\n' '|' | head -c 4000)"
"""

# Calls to the lease exchange's port from inside the orchestrator pod (the
# driver identity rides stdin, never argv).
_EXCHANGE_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import json, sys, urllib.error, urllib.request
cap_memory()
envelope = json.loads(sys.stdin.readline())
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
answers = []
for call in envelope["calls"]:
    request = urllib.request.Request(
        "http://127.0.0.1:%d%s" % (int(envelope["port"]), call["path"]),
        data=json.dumps(call["body"]).encode(),
        method="POST",
        headers={
            "Authorization": "Bearer " + call["token"],
            "Content-Type": "application/json",
        },
    )
    try:
        with opener.open(request, timeout=30) as response:
            status, text = response.status, response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as error:
        status, text = error.code, error.read().decode("utf-8", "replace")
    answers.append({"status": status, "body": text[:400]})
print(json.dumps({"answers": answers}))
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
        "grant_type": "password", "client_id": r["client_id"], "scope": "openid",
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
                    "message": params.get("message"),
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


def parse_instant(text: str) -> datetime | None:
    """An RFC 3339 instant (a row's timestamp, a log line's prefix) as an
    aware datetime; nanoseconds are cut to what datetime holds."""
    text = text.strip().replace("Z", "+00:00")
    match = re.fullmatch(r"(.*T\d\d:\d\d:\d\d)(\.\d+)?([+-]\d\d:\d\d)?", text)
    if match is None:
        return None
    head, fraction, offset = match.groups()
    fraction = (fraction or "")[:7]
    try:
        instant = datetime.fromisoformat(head + fraction + (offset or "+00:00"))
    except ValueError:
        return None
    return instant.astimezone(timezone.utc)


def dispatched_at(logs: str, job: str, lane: str) -> datetime | None:
    """When the orchestrator's log (``kubectl logs --timestamps``) says the
    dispatcher handed ``job`` on in ``lane``: the earliest such line."""
    needle = DISPATCH_LINES[lane].format(job=job)
    found = [
        instant
        for line in logs.splitlines()
        if needle in line and (instant := parse_instant(line.split(" ", 1)[0]))
    ]
    return min(found) if found else None


def parse_denyprobe(log: str) -> bool:
    """Whether the default deny held in the probe's last rounds."""
    verdicts = [
        line.split("=", 1)[1] for line in log.splitlines() if line.startswith("canary=")
    ]
    if len(verdicts) < DENYPROBE_SETTLED:
        raise GateError("the default-deny probe printed too few verdicts")
    return all(verdict == "closed" for verdict in verdicts[-DENYPROBE_SETTLED:])


# ---------------------------------------------------------------------------
# Evaluators (pure; pinned by tests/test_k3d_custom_driver_gate.py)
# ---------------------------------------------------------------------------


def compact(spec: dict) -> str:
    """A spec as the label carries it."""
    return json.dumps(spec, separators=(",", ":"))


def spec_hash(spec: dict) -> str:
    """``shared.connectors.images.spec_hash`` of a label spec."""
    canonical = json.dumps(spec, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def incompatible(spec: dict) -> dict:
    """The moved tag's label: the token slot is gone and the stored config
    no longer validates (a new required property)."""
    moved = json.loads(json.dumps(spec))
    moved["title"] = spec["title"] + " (incompatible)"
    moved["credential_slots"] = []
    schema = moved["config_schema"]
    schema["properties"]["region"] = {"type": "string"}
    schema["required"] = [*schema.get("required", []), "region"]
    return moved


def minted(token: str, binding_id: str) -> str:
    """What the example driver mints for one binding (drivers/example)."""
    digest = hmac.new(token.encode(), binding_id.encode(), hashlib.sha256)
    return "example-" + digest.hexdigest()[:32]


def process_facts(line: str) -> dict[str, str]:
    """``EXAMPLE_DRIVER_PROCESS`` as fields."""
    facts = {}
    for part in line.split():
        name, _, value = part.partition("=")
        facts[name] = value
    return facts


def unprivileged_process(facts: dict[str, str]) -> list[str]:
    """Why the driver's process was not unprivileged (empty when it was)."""
    problems = []
    if facts.get("uid") != DRIVER_UID:
        problems.append(f"uid {facts.get('uid')!r}, not {DRIVER_UID}")
    for name in ("capeff", "capbnd", "capprm"):
        value = facts.get(name, "")
        if not value or int(value, 16) != 0:
            problems.append(f"{name} {value!r} is not empty")
    if facts.get("nonewprivs") != "1":
        problems.append(f"no_new_privs {facts.get('nonewprivs')!r}")
    if facts.get("seccomp") != "2":
        problems.append(f"seccomp mode {facts.get('seccomp')!r}, not filtering")
    return problems


def pod_problems(pod: dict, *, digest: str, namespace: str) -> list[str]:
    """Why a bind-time pod is not the hardened, unprivileged one SRW builds."""
    problems: list[str] = []
    metadata, spec = pod.get("metadata", {}), pod.get("spec", {})
    name = metadata.get("name", "")
    if (metadata.get("labels") or {}).get("srw/managed-by") != BIND_TIME_MANAGER:
        problems.append("not managed by connector-bind-time")
    if metadata.get("namespace") != namespace:
        problems.append(f"in {metadata.get('namespace')}, not {namespace}")
    if spec.get("restartPolicy") != "Never":
        problems.append("restartPolicy is not Never")
    if not spec.get("activeDeadlineSeconds"):
        problems.append("no activeDeadlineSeconds")
    if spec.get("automountServiceAccountToken") is not False:
        problems.append("a ServiceAccount token may be mounted")
    if spec.get("serviceAccountName") != "srw-connector-driver":
        problems.append(f"service account {spec.get('serviceAccountName')!r}")
    if spec.get("enableServiceLinks") is not False:
        problems.append("service links on")
    for host in ("hostNetwork", "hostPID", "hostIPC"):
        if spec.get(host):
            problems.append(f"{host} on")
    if (spec.get("securityContext") or {}).get("seccompProfile") != {
        "type": "RuntimeDefault"
    }:
        problems.append("seccomp is not RuntimeDefault")
    containers = spec.get("containers") or []
    if [c.get("name") for c in containers] != ["driver"]:
        problems.append("the pod runs more than its driver")
    for container in [*containers, *(spec.get("initContainers") or [])]:
        context = container.get("securityContext") or {}
        if (
            context.get("privileged")
            or context.get("allowPrivilegeEscalation") is not False
        ):
            problems.append(f"{container.get('name')} may escalate privilege")
        if (context.get("capabilities") or {}).get("drop") != ["ALL"]:
            problems.append(f"{container.get('name')} keeps capabilities")
        if (context.get("capabilities") or {}).get("add"):
            problems.append(f"{container.get('name')} adds capabilities")
        if container.get("envFrom"):
            problems.append(f"{container.get('name')} takes envFrom")
        for item in container.get("env") or []:
            if item.get("valueFrom"):
                problems.append(
                    f"{container.get('name')} env {item.get('name')} is a reference"
                )
    if containers:
        driver = containers[0]
        if not str(driver.get("image", "")).endswith("@" + digest):
            problems.append("the image is not pinned to the bound digest")
        if driver.get("command") != ["/srw/bin/srw-driver-shim", "run", "--"]:
            problems.append("the shim does not run the driver")
        if sorted(item.get("name") for item in driver.get("env") or []) != [
            "SRW_DRIVER_IDENTITY_FILE",
            "SRW_REQUEST_FILE",
            "SRW_RESULT_URL",
        ]:
            problems.append("the driver's environment is not SRW's three files")
    secrets_mounted = [
        volume["secret"].get("secretName")
        for volume in spec.get("volumes") or []
        if volume.get("secret")
    ]
    if secrets_mounted != [name]:
        problems.append(f"secrets mounted {secrets_mounted}, not its own only")
    if any(
        volume.get(kind)
        for volume in spec.get("volumes") or []
        for kind in ("hostPath", "projected", "persistentVolumeClaim")
    ):
        problems.append("a host, projected or claimed volume is mounted")
    return problems


def refusal_problems(row: dict, *, digest: str) -> list[str]:
    """Why a failed binding is not the moved-tag refusal."""
    problems = []
    message = row.get("error_message") or ""
    if row.get("status") != "failed":
        problems.append(f"status {row.get('status')!r}")
    for needle in (
        "changed its contract",
        "credential slots disappeared: token",
        "region",
        "pin a digest or register a new driver major",
    ):
        if needle not in message:
            problems.append(f"the message lacks {needle!r}")
    if row.get("image_digest") != digest:
        problems.append("the refused digest is not recorded")
    return problems


def policy_problems(
    policy: dict | None, pod: dict, *, exchange_port: int, namespace: str
) -> list[str]:
    """Why a bind-time pod's NetworkPolicy, read while the pod ran, is not
    the one SRW builds for a driver without egress: named after the pod, it
    selects exactly that pod, admits nothing, and reaches only the
    orchestrator pods' exchange port in the release namespace."""
    if not policy:
        return ["no NetworkPolicy named after the pod while it ran"]
    problems: list[str] = []
    metadata = pod.get("metadata") or {}
    spec = policy.get("spec") or {}
    operation = (metadata.get("labels") or {}).get("srw.io/driver-operation")
    if (policy.get("metadata") or {}).get("name") != metadata.get("name"):
        problems.append("not named after its pod")
    if not operation or spec.get("podSelector") != {
        "matchLabels": {"srw.io/driver-operation": operation}
    }:
        problems.append(f"selects {spec.get('podSelector')}, not the pod")
    if sorted(spec.get("policyTypes") or []) != ["Egress", "Ingress"]:
        problems.append(f"policyTypes {spec.get('policyTypes')}")
    if spec.get("ingress"):
        problems.append("admits ingress")
    egress = spec.get("egress") or []
    if len(egress) != 1:
        problems.append(f"{len(egress)} egress rules, not the exchange's alone")
        return problems
    if egress[0].get("ports") != [{"protocol": "TCP", "port": exchange_port}]:
        problems.append(f"egress ports {egress[0].get('ports')}")
    peers = egress[0].get("to") or []
    if (
        len(peers) != 1
        or "ipBlock" in peers[0]
        or not (peers[0].get("podSelector") or {}).get("matchLabels")
        or (peers[0].get("namespaceSelector") or {}).get("matchLabels")
        != {"kubernetes.io/metadata.name": namespace}
    ):
        problems.append(f"egress peers {peers}, not the orchestrator's pods")
    return problems


_BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"


def _base62(value: int, width: int) -> str:
    digits = []
    while value:
        value, digit = divmod(value, 62)
        digits.append(_BASE62[digit])
    return "".join(reversed(digits)).rjust(width, "0")


def well_formed_token(prefix: str) -> str:
    """A token of SRW's shape (``shared.connectors.leases.mint_token``) that
    no row holds: what a caller that was never issued one presents."""
    body = _base62(int.from_bytes(secrets.token_bytes(32), "big"), 43)
    checksum = _base62(zlib.crc32(f"{prefix}_{body}".encode("ascii")), 6)
    return f"{prefix}_{body}{checksum}"


def notice_lines(facts: dict[str, str]) -> list[str]:
    """The README's "Not delivered" lines a workspace script reported."""
    return [line for line in facts.get("notices", "").split("|") if NOTICE in line]


# ---------------------------------------------------------------------------
# The API, as each account
# ---------------------------------------------------------------------------


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


class PodWatch(threading.Thread):
    """Records every bind-time pod of the run's connectors while it exists:
    the pod, its Secret's keys, owners and identity, and its NetworkPolicy (a
    bind-time pod lives seconds)."""

    def __init__(self, namespace: str) -> None:
        super().__init__(daemon=True)
        self.namespace = namespace
        self.connectors: set[str] = set()
        self.pods: dict[str, dict] = {}
        self.secret_keys: dict[str, list[str]] = {}
        self.secret_owners: dict[str, list[str]] = {}
        self.identities: dict[str, str] = {}
        self.policies: dict[str, dict] = {}
        self.stopped = threading.Event()

    def _get(self, *args: str) -> dict | None:
        rc, out, _err = run(
            ["kubectl", f"--context={LOCAL_CONTEXT}", "-n", self.namespace, "get"]
            + list(args)
            + ["-o", "json"],
            timeout=30,
        )
        try:
            return json.loads(out) if rc == 0 and out else None
        except ValueError:
            return None

    def _record(self, pod: dict) -> None:
        name = pod["metadata"]["name"]
        self.pods.setdefault(name, pod)
        if name not in self.secret_keys:
            found = self._get("secret", name)
            if found:
                data = found.get("data") or {}
                self.secret_keys[name] = sorted(data)
                self.secret_owners[name] = [
                    owner.get("kind", "")
                    for owner in found["metadata"].get("ownerReferences") or []
                ]
                if data.get("identity"):
                    self.identities[name] = secret(
                        base64.b64decode(data["identity"]).decode("ascii")
                    )
        if name not in self.policies:
            policy = self._get("networkpolicy", name)
            if policy:
                self.policies[name] = policy

    def run(self) -> None:
        selector = f"srw/managed-by={BIND_TIME_MANAGER}"
        while not self.stopped.is_set():
            listing = self._get("pods", "-l", selector) or {}
            for pod in listing.get("items") or []:
                labels = pod["metadata"].get("labels") or {}
                if labels.get("srw.io/connector-id") in self.connectors:
                    self._record(pod)
            self.stopped.wait(0.25)


PLAN = [
    "preflight: orchestrator and stateless agent pods serve this checkout's D6 "
    "modules; driver pods on (exchange and canary port, digest-pinned shim), "
    "the k3d registry over HTTP, room for bind-time pods, custom privilege off, "
    "the k3d registry untrusted; migration 0420; the driver namespace's "
    "baseline, default deny and Terminating pod quota, enforced (12 probes); "
    "docker pushes to localhost:5005",
    "accounts: a disposable OAuth client, a project editor and a project viewer",
    "register: the example image registers at the owner's Account (label spec, "
    "declared variables, pushed digest, tier custom, unprivileged); "
    "srw.example/v1 is refused; another user neither sees nor uses it; a "
    "Project editor registers at Project scope, a viewer cannot; the matrix "
    "shows registrations to who may see them",
    "bind: session one's bind runs one unprivileged pod (spec, Secret, its "
    "NetworkPolicy while it ran, the driver's own UID and capabilities) that "
    "delivers the minted variable and file to the workspace; the binding "
    "records reference, digest, resolved_at, spec_hash and protocol_version; "
    "the pod, its Secret and policy are gone afterwards",
    "identity: the bind pod's sdi_ is refused by the lease exchange and its "
    "introspection; a replay of its result is refused (409)",
    "moved-tag: an incompatible image under the same tag is refused at "
    "session two's bind without a pod; the refusal is on the binding, the "
    "connector, session two's README and in the audit; the compatible image "
    "is pushed back",
    "detach: detaching session one's connector live revokes its binding "
    "(connector_detached) in a revoke pod within a reconciler pass",
    "refusals: a denied variable, an undeclared one and a refused file are "
    "each refused at bind with a visible reason, and revoked "
    "(binding_refused); a bound connector deleted is still revoked "
    "(connector_deleted) with the inputs of its bind",
    "pinned: a pinned session binds and delivers; a driver that fails for good "
    "leaves it usable, with the notice in its README and on the connector; a "
    "live detach over its WebSocket revokes the binding and unsets "
    "EXAMPLE_TOKEN in its workspace; attaching it live again binds anew",
    "job: a job is held until its bind ends (the dispatcher's log hands it on "
    "at or after bound_at), receives the binding, and its binding is revoked "
    "when it ends (execution_ended)",
    "disable: disabling the registration revokes its live bindings "
    "(registration_disabled) and refuses new binds; the pinned session's next "
    "delivery unsets EXAMPLE_TOKEN with a notice; the viewer may not",
    "cleanup: jobs, sessions, connectors, registrations, project, accounts, "
    "OAuth client, probe pod, registry tags and image rows are gone; no "
    "binding of the run's connectors is unrevoked; no driver-namespace object "
    "names one of them",
]


class CustomDriverGate:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.gate_id = args.gate_id or f"d6-{secrets.token_hex(5)}"
        self.report = Report(self.gate_id)
        self.owner = Api(args.user, args.password)
        self.editor = Api(f"{self.gate_id}-ed", secrets.token_urlsafe(24))
        self.viewer = Api(f"{self.gate_id}-vw", secrets.token_urlsafe(24))
        self.accounts_by_role = {"editor": self.editor, "viewer": self.viewer}
        # Everything this run creates, recorded before it is created.
        self.oauth_client = f"{self.gate_id}-oauth"
        self.client_started = False
        self.client_uuid: str | None = None
        self.users_started: dict[str, bool] = {"editor": False, "viewer": False}
        self.keycloak_ids: dict[str, str] = {}
        self.app_rows: dict[str, str] = {}  # role -> app user id
        self.owner_id = ""
        self.project: str | None = None
        self.registrations: dict[str, tuple[str, str]] = {}  # label -> (id, who)
        self.connectors: dict[str, str] = {}  # label -> id
        self.deleted_connectors: set[str] = set()
        self.threads: dict[str, str] = {}
        self.jobs: dict[str, str] = {}
        self.images_pushed = False
        self.digests: dict[str, str] = {}
        self.deny_probe = f"{self.gate_id}-denyprobe"
        self.deny_probe_started = False
        self.namespace = ""
        self.orchestrator_ip = ""
        self.exchange_port = 8088
        self.reconcile_seconds = 15
        self.deadline_seconds = 120
        self.watch: PodWatch | None = None
        self.bind_identity = ""
        self.token = secret(f"d6-token-{secrets.token_hex(16)}")
        self.spec = json.loads(SPEC_FILE.read_text(encoding="utf-8"))

    @property
    def connector(self) -> str | None:
        """The ``example`` connector, the one every delivery check uses."""
        return self.connectors.get("example")

    # -- naming and helpers ------------------------------------------------
    def name(self, label: str) -> str:
        return f"{self.gate_id} {label}"

    def title(self, label: str) -> str:
        return f"D6 custom driver gate {self.gate_id} {label}"

    def reference(self, tag: str | None = None) -> str:
        return f"{CLUSTER_REGISTRY}/{REPOSITORY}:{tag or self.gate_id}"

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

    def workspace_pod(self, selector: str) -> str:
        """The one running workspace pod of ``srw/thread-id=…`` or
        ``srw/job-id=…``."""

        def probe() -> str | None:
            running = [
                pod["metadata"]["name"]
                for pod in self.release_pods(selector)
                if pod.get("status", {}).get("phase") == "Running"
            ]
            return running[0] if len(running) == 1 else None

        return wait_for(f"workspace of {selector}", probe, timeout=300)

    def workspace_facts(self, pod: str) -> dict[str, str]:
        rc, out = self.ws(
            pod, f"GATE_FILE={TOKEN_FILE.replace('~', HOME, 1)}\n" + _WORKSPACE_SCRIPT
        )
        if rc:
            return {}
        return dict(line.split("=", 1) for line in out.splitlines() if "=" in line)

    def delivered(
        self, selector: str, binding_id: str, *, with_file: bool = False
    ) -> dict[str, str]:
        """The workspace's facts once it holds the variable the driver minted
        for ``binding_id`` (and, ``with_file``, its credential file, which a
        separate sync step places); ``{}`` when it never does."""
        pod = self.workspace_pod(selector)
        expected = hashlib.sha256(
            secret(minted(self.token, binding_id)).encode()
        ).hexdigest()

        def probe() -> dict | None:
            facts = self.workspace_facts(pod)
            if facts.get("value_sha") != expected:
                return None
            if with_file and not (facts.get("file_sha") and facts.get("var_real")):
                return None
            return facts

        try:
            return wait_for(
                f"the minted variable in {selector}'s workspace",
                probe,
                timeout=self.args.turn_timeout,
                interval=5,
            )
        except GateError:
            return {}

    def noticed(self, selector: str, needles: list[str]) -> list[str]:
        """The workspace README's "Not delivered" lines once every needle is
        in one; what it has when they never all are."""
        pod = self.workspace_pod(selector)
        lines: list[str] = []

        def probe() -> bool:
            nonlocal lines
            lines = notice_lines(self.workspace_facts(pod))
            return all(any(needle in line for line in lines) for needle in needles)

        try:
            wait_for(
                f"the notices in {selector}'s README",
                probe,
                timeout=self.args.turn_timeout,
                interval=5,
            )
        except GateError:
            pass
        return lines

    def binding(
        self, owner: str, connector: str | None = None, kind: str = "thread"
    ) -> dict | None:
        """The newest binding of an execution (a thread, or a job with
        ``kind="job"``) and a connector (``example`` by default)."""
        out = sql(
            "SELECT coalesce(row_to_json(b)::text, '') FROM (SELECT id, status, "
            "image_reference, image_digest, resolved_at, spec_hash, "
            "protocol_version, access, error_class, error_message, retry_at, "
            "revoke_reason, revoke_error, revoked_at, revoke_requested_at, "
            "bound_at FROM connector_bind_time_bindings WHERE connector_id = "
            f"{lit(connector or self.connector)} AND owner_kind = {lit(kind)} "
            f"AND owner_id = {lit(owner)} ORDER BY created_at DESC LIMIT 1) b"
        )
        return json.loads(out) if out else None

    def binding_in(
        self,
        owner: str,
        statuses: tuple[str, ...],
        connector: str | None = None,
        kind: str = "thread",
    ) -> dict | None:
        found = self.binding(owner, connector, kind)
        return found if found and found["status"] in statuses else None

    def settled(
        self, owner: str, connector: str | None = None, kind: str = "thread"
    ) -> dict:
        """The binding once its bind ended: bound, failed, or revoking or
        revoked after a refusal."""

        def probe() -> dict | None:
            row = self.binding(owner, connector, kind)
            if row is None or row["status"] == "pending":
                return None
            if row["status"] in ("revoking", "revoked") and not row["error_message"]:
                return None  # a revoke asked for a bound one; wait for a refusal
            return row

        return wait_for(
            f"the bind of {kind} {owner}",
            probe,
            timeout=self.args.turn_timeout,
            interval=3,
        )

    def revoked(self, binding_id: str, *, passes: int = 3) -> dict | None:
        """The binding once a revoke retired it, within ``passes`` reconciler
        passes and a pod's deadline; ``None`` when it never is."""
        try:
            return wait_for(
                f"binding {binding_id} revoked",
                lambda: (
                    row
                    if (
                        row := json.loads(
                            sql(
                                "SELECT coalesce(row_to_json(b)::text, '{}') FROM "
                                "(SELECT id, status, revoke_reason, revoke_error, "
                                "revoked_at FROM connector_bind_time_bindings "
                                f"WHERE id = {lit(binding_id)}) b"
                            )
                            or "{}"
                        )
                    ).get("status")
                    == "revoked"
                    else None
                ),
                timeout=passes * self.reconcile_seconds + self.deadline_seconds + 60,
                interval=3,
            )
        except GateError:
            return None

    def revoke_operation(self, binding_id: str) -> dict | None:
        revokes = [
            op for op in self.operations(binding_id) if op["operation"] == "revoke"
        ]
        return revokes[-1] if revokes else None

    def objects_gone(self, name: str) -> bool:
        try:
            wait_for(
                f"{name}'s pod, Secret and policy gone",
                lambda: self.gone("pod", name)
                and self.gone("secret", name)
                and self.gone("networkpolicy", name),
                timeout=120,
                interval=3,
            )
            return True
        except GateError:
            return False

    def exchange_calls(self, calls: list[dict]) -> list[dict]:
        result = in_pod(
            ORCHESTRATOR,
            ORCHESTRATOR_CONTAINER,
            _EXCHANGE_PROGRAM,
            {"port": self.exchange_port, "calls": calls},
        )
        answers = []
        for answer in result["answers"]:
            try:
                body = json.loads(_scrub(answer["body"]) or "{}")
            except ValueError:
                body = {"raw": _scrub(answer["body"])[:200]}
            answers.append({"status": answer["status"], "body": body})
        return answers

    def driver_status(self, label: str) -> dict:
        detail = self.owner.ok("GET", f"/api/datasources/{self.connectors[label]}")
        return (detail or {}).get("driver_status") or {}

    def operations(self, binding_id: str) -> list[dict]:
        out = sql(
            "SELECT coalesce(json_agg(row_to_json(o) ORDER BY o.created_at), '[]') "
            "FROM (SELECT id, operation, status, exit_code, pod_name, error, "
            "removed_at IS NOT NULL AS removed, created_at FROM "
            f"connector_driver_operations WHERE binding_id = {lit(binding_id)}) o"
        )
        return json.loads(out or "[]")

    def gone(self, kind: str, name: str) -> bool:
        rc, out, _err = run(
            self.kc + ["get", kind, name, "--ignore-not-found", "-o", "name"],
            timeout=60,
        )
        return rc == 0 and not out

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
            "checkout's D6 modules",
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
                "CONNECTOR_DRIVER_REGISTRY_INSECURE_HOSTS",
                "CONNECTOR_DRIVER_REGISTRY_PRIVATE_HOSTS",
                "CONNECTOR_DRIVER_TRUSTED_REPOSITORIES",
                "CONNECTOR_CUSTOM_DRIVERS_PRIVILEGED",
                "CONNECTOR_BIND_TIME_MAX_PODS",
                "CONNECTOR_BIND_TIME_DEADLINE_SECONDS",
                "CONNECTOR_SERVICE_RECONCILE_SECONDS",
            )
        }
        try:
            trusted = json.loads(env["CONNECTOR_DRIVER_TRUSTED_REPOSITORIES"] or "[]")
        except ValueError:
            trusted = ["<unreadable>"]
        configured = (
            env["CONNECTOR_SERVICE_PODS_ENABLED"].lower() == "true"
            and env["CONNECTOR_SERVICE_NAMESPACE"] != ""
            and env["CONNECTOR_LEASE_EXCHANGE_PORT"].isdigit()
            and env["CONNECTOR_LEASE_CANARY_PORT"].isdigit()
            and "@sha256:" in env["CONNECTOR_DRIVER_SHIM_IMAGE"]
            and CLUSTER_REGISTRY
            in env["CONNECTOR_DRIVER_REGISTRY_INSECURE_HOSTS"].split(",")
            and CLUSTER_REGISTRY
            in env["CONNECTOR_DRIVER_REGISTRY_PRIVATE_HOSTS"].split(",")
            and env["CONNECTOR_BIND_TIME_MAX_PODS"].isdigit()
            and int(env["CONNECTOR_BIND_TIME_MAX_PODS"]) >= 1
        )
        self.report.check(
            "preflight: driver pods on with an exchange and canary port, a "
            "digest-pinned shim, the k3d registry over HTTP and room for "
            "bind-time pods",
            configured,
            json.dumps(env),
        )
        if not configured:
            raise GateError(
                "set connectors.servicePods.enabled and connectors.drivers.registry "
                "as the k3d profile does, under Tilt"
            )
        custom = env[
            "CONNECTOR_CUSTOM_DRIVERS_PRIVILEGED"
        ].lower() != "true" and not any(
            str(item) == CLUSTER_REGISTRY
            or str(item).startswith(CLUSTER_REGISTRY + "/")
            for item in trusted
        )
        self.report.check(
            "preflight: custom drivers get no privilege and the k3d registry is "
            "not trusted (the example image is custom)",
            custom,
            f"privileged={env['CONNECTOR_CUSTOM_DRIVERS_PRIVILEGED']} "
            f"trusted={trusted}",
        )
        if not custom:
            raise GateError(
                "unset connectors.customDrivers.privileged and keep the k3d "
                "registry out of connectors.drivers.trustedRepositories"
            )
        self.namespace = env["CONNECTOR_SERVICE_NAMESPACE"]
        self.exchange_port = int(env["CONNECTOR_LEASE_EXCHANGE_PORT"])
        self.reconcile_seconds = int(
            float(env["CONNECTOR_SERVICE_RECONCILE_SECONDS"] or 15)
        )
        self.deadline_seconds = int(
            float(env["CONNECTOR_BIND_TIME_DEADLINE_SECONDS"] or 120)
        )
        names = ", ".join(lit(name) for name in MIGRATIONS)
        applied = sql(
            "SELECT count(*) FROM schema_migrations WHERE success AND filename "
            f"IN ({names})"
        )
        self.report.check(
            "preflight: migration 0420 applied",
            applied == str(len(MIGRATIONS)),
            f"{applied} of {len(MIGRATIONS)}",
        )
        if applied != str(len(MIGRATIONS)):
            raise GateError("the D6 migration is not applied")
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
        quota = json.loads(
            command(
                self.kc
                + ["get", "resourcequota", "srw-connector-bind-time-pods", "-o", "json"]
            )
        )
        self.report.check(
            "preflight: the driver namespace enforces Pod Security baseline, has "
            "its static default deny and caps bind-time (Terminating) pods",
            labels.get("pod-security.kubernetes.io/enforce") == "baseline"
            and deny["spec"].get("podSelector") == {}
            and sorted(deny["spec"].get("policyTypes") or []) == ["Egress", "Ingress"]
            and quota["spec"].get("scopes") == ["Terminating"],
            str({k: v for k, v in labels.items() if "pod-security" in k}),
        )
        self.orchestrator_ip = command(
            K + ["get", "svc", ORCHESTRATOR_SERVICE, "-o", "jsonpath={.spec.clusterIP}"]
        )
        if not _IPV4_RE.fullmatch(self.orchestrator_ip):
            raise GateError("the orchestrator Service has no IPv4 ClusterIP")
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
        try:
            with urllib.request.urlopen(
                f"http://{LOCAL_REGISTRY}/v2/", timeout=15
            ) as response:
                reachable = response.status == 200
        except OSError:
            reachable = False
        self.report.check(
            "preflight: the k3d registry answers on localhost:5005 (docker pushes there)",
            reachable,
        )
        if not reachable:
            raise GateError("start the k3d registry (scripts/local-dev-up.sh)")

    def keycloak(self, action: str, role: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "action": action,
            "client": self.oauth_client,
            "marker": self.gate_id,
            "client_started": self.client_started,
            "user_started": False,
            "username": "",
            "email": "",
        }
        if role is not None:
            account = self.accounts_by_role[role]
            payload.update(
                username=account.username,
                email=f"{account.username}@{ACCOUNT_DOMAIN}",
                user_started=self.users_started[role],
            )
            if action == "create-user":
                payload["password"] = account.password
            if role in self.keycloak_ids:
                payload["user_id"] = self.keycloak_ids[role]
        if self.client_uuid:
            payload["client_uuid"] = self.client_uuid
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
        for api in (self.owner, self.editor, self.viewer):
            api.client_id = self.oauth_client
        owner = self.owner.ok("GET", "/api/auth/me")["user"]
        self.owner_id = str(owner["id"])
        if not owner.get("is_admin"):
            raise GateError("the owner must be an administrator")
        seen = []
        for role, api in self.accounts_by_role.items():
            self.users_started[role] = True
            created = self.keycloak("create-user", role)
            if created.get("exists"):
                self.users_started[role] = False
                raise GateError(f"a Keycloak user {api.username} already exists")
            self.keycloak_ids[role] = self.receipt(created, f"{role} account")
            app_id = sql("SELECT gen_random_uuid()")
            if not _UUID_RE.fullmatch(app_id):
                raise GateError(f"no fresh id: {app_id!r}")
            self.app_rows[role] = app_id
            sql(
                "INSERT INTO users (id, display_name, email, keycloak_sub, "
                "preferred_username, is_approved, approved_at, approved_by) VALUES ("
                f"{lit(app_id)}, {lit(api.username)}, "
                f"{lit(api.username + '@' + ACCOUNT_DOMAIN)}, "
                f"{lit(self.keycloak_ids[role])}, {lit(api.username)}, true, "
                f"now(), {lit(self.owner_id)})"
            )
            me = api.ok("GET", "/api/auth/me")["user"]
            seen.append(str(me["id"]) == app_id and not me.get("is_admin"))
        self.report.check(
            "accounts: the owner is an administrator; the editor and viewer log "
            "in as their admitted rows and are none",
            all(seen),
        )

    # -- images ------------------------------------------------------------
    def build(self, tag: str, spec: dict) -> str:
        """Build the example image with ``spec`` as its label and push it as
        ``tag``; its digest."""
        local = f"{LOCAL_REGISTRY}/{REPOSITORY}:{tag}"
        self.images_pushed = True
        command(
            ["docker", "build", "-q", "-f", DOCKERFILE]
            + ["--label", f"{SPEC_LABEL}={compact(spec)}"]
            + ["--label", f"{GATE_LABEL}={self.gate_id}"]
            + ["-t", local, str(ROOT)],
            timeout=900,
        )
        out = command(["docker", "push", local], timeout=600)
        match = re.search(r"digest: (sha256:[0-9a-f]{64})", out)
        if match is None:
            raise GateError("docker push printed no digest")
        return self.platform_digest(match.group(1))

    def platform_digest(self, digest: str) -> str:
        """The digest SRW binds for a pushed ``digest``. Docker's containerd
        image store pushes an image index (the image plus attestations); SRW's
        resolver reads the index and records the linux manifest it lists, so
        that manifest's digest is the one to expect. A plain manifest is its
        own answer."""
        request = urllib.request.Request(
            f"http://{LOCAL_REGISTRY}/v2/{REPOSITORY}/manifests/{digest}",
            headers={"Accept": ", ".join(MANIFEST_TYPES)},
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            document = json.load(response)
        if document.get("mediaType") not in INDEX_TYPES:
            return digest
        architecture = {"x86_64": "amd64", "aarch64": "arm64"}.get(
            os.uname().machine, os.uname().machine
        )
        for manifest in document.get("manifests") or []:
            platform = manifest.get("platform") or {}
            if (
                platform.get("os") == "linux"
                and platform.get("architecture") == architecture
                and not platform.get("variant")
            ):
                return str(manifest["digest"])
        raise GateError(
            f"the pushed index {digest} lists no linux/{architecture} image"
        )

    def registry_tags(self) -> list[str]:
        url = f"http://{LOCAL_REGISTRY}/v2/{REPOSITORY}/tags/list"
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                return list(json.load(response).get("tags") or [])
        except (OSError, ValueError) as exc:
            raise GateError(f"registry tag list failed: {exc}") from None

    def own_tags(self) -> list[str]:
        return [self.gate_id, f"{self.gate_id}-srw"]

    # -- register ------------------------------------------------------------
    def register(self, api: Api, who: str, label: str, body: dict) -> tuple[int, Any]:
        status, parsed = api.call("POST", "/api/connector-drivers", body)
        if isinstance(parsed, dict) and _UUID_RE.fullmatch(str(parsed.get("id", ""))):
            self.registrations[label] = (str(parsed["id"]), who)
        return status, parsed

    def register_checks(self) -> None:
        self.digests["compatible"] = self.build(self.gate_id, self.spec)
        self.digests["srw"] = self.build(
            f"{self.gate_id}-srw", {**self.spec, "name": "srw.example/v1"}
        )
        status, account = self.register(
            self.owner,
            "owner",
            "account",
            {"image": self.reference(), "name": DRIVER},
        )
        trust = (account or {}).get("trust") or {}
        self.report.check(
            "register: the example image registers at the owner's Account from "
            "its label: the variables it declares, the pushed digest, tier "
            "custom, no privilege",
            status == 201
            and account.get("scope") == {"kind": "Account", "name": self.owner_id}
            and account.get("name") == DRIVER
            and account.get("image_digest") == self.digests["compatible"]
            and account.get("spec_source") == "label"
            and account.get("spec_hash") == spec_hash(self.spec)
            and account.get("env_names") == self.spec["env_names"]
            and trust.get("tier") == "custom"
            and trust.get("privileged") is False,
            f"HTTP {status}: {json.dumps(account)[:400]}",
        )
        if status != 201:
            raise GateError("the Account registration failed")
        status, refused = self.register(
            self.owner, "owner", "srw", {"image": self.reference(f"{self.gate_id}-srw")}
        )
        self.report.check(
            "register: an image naming srw.example/v1 is refused (srw.* is SRW's own)",
            status == 422 and "SRW's own" in str(refused.get("detail")),
            f"HTTP {status}: {str(refused)[:300]}",
        )
        registration = self.registrations["account"][0]
        listed = self.viewer.ok("GET", "/api/connector-drivers")
        status_get, _body = self.viewer.call(
            "GET", f"/api/connector-drivers/{registration}"
        )
        by_id, by_id_body = self.viewer.call(
            "POST",
            "/api/datasources",
            {
                "name": self.name("stolen-by-id"),
                "type": IMAGE_TYPE,
                "driver_registration_id": registration,
                "config": {"file": True},
                "credentials": {"token": "x"},
            },
        )
        by_name, by_name_body = self.viewer.call(
            "POST",
            "/api/datasources",
            {
                "name": self.name("stolen-by-name"),
                "type": IMAGE_TYPE,
                "driver": DRIVER,
                "config": {"file": True},
                "credentials": {"token": "x"},
            },
        )
        self.report.check(
            "register: another user neither lists nor reads the Account "
            "registration, and cannot create a connector of it by id or name",
            registration
            not in [item.get("id") for item in listed.get("registrations") or []]
            and status_get == 404
            and by_id == 404
            and by_name == 404,
            f"get={status_get} by_id={by_id} {str(by_id_body)[:120]} "
            f"by_name={by_name} {str(by_name_body)[:120]}",
        )
        created = self.owner.ok(
            "POST",
            "/api/projects",
            {
                "name": self.name("project"),
                "description": "D6 custom driver gate (disposable)",
                "user_id": self.owner_id,
            },
        )
        self.project = str(created["id"])
        for role in ("editor", "viewer"):
            self.owner.ok(
                "POST",
                f"/api/projects/{self.project}/members",
                {"user_id": self.app_rows[role], "role": role},
            )
        scope = {"kind": "Project", "name": self.project}
        viewer_status, viewer_body = self.register(
            self.viewer,
            "viewer",
            "project-viewer",
            {"image": self.reference(), "scope": scope},
        )
        editor_status, editor_body = self.register(
            self.editor,
            "editor",
            "project",
            {"image": self.reference(), "scope": scope},
        )
        project_id = (editor_body or {}).get("id")
        viewer_list = self.viewer.ok("GET", "/api/connector-drivers")
        self.report.check(
            "register: a Project editor registers at Project scope, a viewer "
            "cannot (403) but sees it",
            editor_status == 201
            and (editor_body or {}).get("scope") == scope
            and viewer_status == 403
            and project_id
            in [item.get("id") for item in viewer_list.get("registrations") or []],
            f"editor={editor_status} viewer={viewer_status} {str(viewer_body)[:200]}",
        )
        owner_matrix = self.owner.ok("GET", "/api/datasources/drivers")
        viewer_matrix = self.viewer.ok("GET", "/api/datasources/drivers")

        def registered(matrix: dict) -> list[str]:
            return [
                (driver.get("registration") or {}).get("id")
                for driver in matrix.get("drivers") or []
                if driver.get("registration")
            ]

        self.report.check(
            "register: the capability matrix lists the Account registration to "
            "its owner, marked custom, and not to another user",
            registration in registered(owner_matrix)
            and registration not in registered(viewer_matrix)
            and any(
                driver.get("trust", {}).get("claims_declared_by_author") is True
                for driver in owner_matrix["drivers"]
                if (driver.get("registration") or {}).get("id") == registration
            ),
        )

    # -- connectors and executions ------------------------------------------
    def create_connector(self, label: str, config: dict | None = None) -> str:
        status, parsed = self.owner.call(
            "POST",
            "/api/datasources",
            {
                "name": self.name(label),
                "type": IMAGE_TYPE,
                "scope_mode": "all",
                "driver_registration_id": self.registrations["account"][0],
                "config": config if config is not None else {"file": True},
                "credentials": {"token": self.token},
            },
        )
        if isinstance(parsed, dict) and parsed.get("id"):
            self.connectors[label] = str(parsed["id"])
            if self.watch is not None:
                self.watch.connectors.add(self.connectors[label])
        if status not in (200, 201) or label not in self.connectors:
            raise GateError(f"the {label} connector create answered HTTP {status}")
        return self.connectors[label]

    def create_session(
        self,
        label: str,
        connectors: list[str],
        *,
        pinned: bool = False,
    ) -> str:
        config: dict[str, Any] = {"workspace": {"backend": "sandbox"}}
        if pinned:
            # An Officer conference runs on the pinned lane (one per project).
            config["officer"] = {"conference": True}
        created = self.owner.ok(
            "POST",
            "/api/persistent/threads",
            {
                "title": self.title(label),
                "permission_mode": "autonomous",
                "project_id": self.project,
                "datasource_ids": [self.connectors[name] for name in connectors],
                "config_override": config,
                "model": self.args.model,
            },
        )
        thread = str(created.get("thread_id") or created["id"])
        self.threads[label] = thread
        print(f"session {label}: {thread}", flush=True)
        if pinned:
            # A pinned session refuses input (409 session_binding_invalid)
            # until its agent has attached; /connection admits it then.
            wait_for(
                f"pinned session {label} admitted by /connection",
                lambda: self.owner.call("GET", f"/api/sessions/{thread}/connection")[0]
                == 200,
                timeout=self.args.turn_timeout,
                interval=5,
            )
        self.owner.ok(
            "POST",
            f"/api/persistent/threads/{thread}/input",
            {"content": "Reply with the single word ready."},
        )
        return thread

    def create_job(self, label: str, connectors: list[str]) -> str:
        created = self.owner.ok(
            "POST",
            "/api/jobs",
            {
                "description": (
                    f"[{self.gate_id} {label}] Run the shell command "
                    f"`printenv {TOKEN_VARIABLE} | sha256sum` once, write the "
                    "word done to output/d6.txt and complete the job."
                ),
                "project_id": self.project,
                "datasource_ids": [self.connectors[name] for name in connectors],
                "execution_lane": self.args.job_lane,
                "config_override": {
                    "workspace": {"backend": "sandbox"},
                    "llm": {"model": self.args.model},
                },
            },
        )
        job = str(created.get("job_id") or created["id"])
        self.jobs[label] = job
        print(f"job {label}: {job}", flush=True)
        return job

    def job_status(self, job: str) -> str:
        return sql(f"SELECT status FROM jobs WHERE id = {lit(job)}")

    # -- a pinned session's live selection ---------------------------------------
    def pinned_assignment(self, thread: str) -> dict[str, str] | None:
        """The agent a pinned thread is assigned to: ``hostname``, ``pod_ip``
        (a pooled pinned pod carries no thread label)."""
        row = sql(
            "SELECT coalesce(json_build_object('hostname', a.hostname, "
            "'pod_ip', a.pod_ip)::text, '') FROM threads t JOIN agents a ON "
            f"a.id = t.agent_id WHERE t.id = {lit(thread)}"
        )
        if not row:
            return None
        assignment = json.loads(row)
        hostname = str(assignment.get("hostname") or "")
        if not _POD_NAME_RE.fullmatch(hostname):
            return None
        return {"hostname": hostname, "pod_ip": str(assignment.get("pod_ip") or "")}

    def pinned_ip(self, thread: str) -> str:
        """The pod IP of the running, ready agent pod ``thread`` is assigned to."""

        def probe() -> str | None:
            assignment = self.pinned_assignment(thread)
            if assignment is None:
                return None
            rc, out, _err = run(
                K + ["get", "pod", assignment["hostname"], "-o", "json"], timeout=60
            )
            if rc:
                return None
            status = json.loads(out).get("status", {})
            ready = status.get("phase") == "Running" and all(
                item.get("ready") for item in status.get("containerStatuses") or [{}]
            )
            ip = str(status.get("podIP") or "")
            if not ready or not ip:
                return None
            if assignment["pod_ip"] and assignment["pod_ip"] != ip:
                return None
            return ip

        return wait_for(f"pinned agent pod of {thread}", probe, timeout=600)

    def live_update(self, thread: str, labels: list[str], label: str) -> dict:
        """One live ``config.update`` selecting ``labels``' connectors, over
        the pinned session's own WebSocket (as the settings pane sends it);
        retried while the session still attaches."""

        def ready() -> bool:
            status, body = self.owner.call("GET", f"/api/sessions/{thread}/connection")
            if status == 200:
                return True
            if status in (409, 425):
                return False
            raise GateError(f"/connection answered HTTP {status}: {str(body)[:200]}")

        wait_for(
            f"session {thread} admitted by /connection",
            ready,
            timeout=self.args.turn_timeout,
            interval=5,
        )

        def attempt() -> dict | None:
            result = in_pod(
                ORCHESTRATOR,
                ORCHESTRATOR_CONTAINER,
                _LIVE_UPDATE_PROGRAM,
                {
                    "username": self.owner.username,
                    "password": self.owner.password,
                    "client_id": self.owner.client_id,
                    "token_url": KEYCLOAK_TOKEN_URL,
                    "thread": thread,
                    "ip": self.pinned_ip(thread),
                    "port": AGENT_PORT,
                    "datasource_ids": [self.connectors[name] for name in labels],
                    "request_id": f"{self.gate_id}-{label}",
                    "timeout": self.args.turn_timeout,
                    "max_size": WS_MAX_FRAME,
                },
                timeout=self.args.turn_timeout + 90,
            )
            if result.get("outcome") == "retry":
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

    def unset(self, selector: str) -> dict[str, str]:
        """The workspace's facts once the minted variable is no longer set
        (its environment files, sourced, leave it empty); ``{}`` when it
        stays set."""
        pod = self.workspace_pod(selector)

        def probe() -> dict | None:
            facts = self.workspace_facts(pod)
            return facts if facts.get("value_sha") == EMPTY_SHA else None

        try:
            return wait_for(
                f"the minted variable gone from {selector}'s workspace",
                probe,
                timeout=self.args.turn_timeout,
                interval=5,
            )
        except GateError:
            return {}

    def dispatch_instant(self, job: str, since: float) -> datetime | None:
        """When the orchestrator handed ``job`` on (its log, with timestamps)."""
        rc, logs, _err = run(
            K
            + [
                "logs",
                ORCHESTRATOR,
                "-c",
                ORCHESTRATOR_CONTAINER,
                "--timestamps",
                f"--since={max(60, int(time.time() - since) + 60)}s",
            ],
            timeout=120,
        )
        return dispatched_at(logs, job, self.args.job_lane) if rc == 0 else None

    def answered(self, thread: str) -> bool:
        """Whether the session answered a turn (a message SRW stores with
        role ``ai``)."""
        try:
            wait_for(
                f"session {thread} answers",
                lambda: sql(
                    "SELECT count(*) FROM thread_messages WHERE thread_id = "
                    f"{lit(thread)} AND role = 'ai'"
                )
                not in ("", "0"),
                timeout=self.args.turn_timeout,
                interval=5,
            )
            return True
        except GateError:
            return False

    # -- bind ------------------------------------------------------------------
    def bind_checks(self) -> None:
        self.watch = PodWatch(self.namespace)
        self.watch.start()
        self.create_connector("example")
        thread = self.create_session("one", ["example"])
        row = self.settled(thread)
        self.report.check(
            "bind: session one's binding is bound and records reference, digest, "
            "resolution time, spec hash and protocol version",
            row["status"] == "bound"
            and row["image_reference"] == self.reference()
            and row["image_digest"] == self.digests["compatible"]
            and row["resolved_at"]
            and row["spec_hash"] == spec_hash(self.spec)
            and row["protocol_version"] == self.spec["protocol_version"],
            json.dumps(
                {k: row.get(k) for k in ("status", "image_digest", "error_message")}
            ),
        )
        if row["status"] != "bound":
            raise GateError(f"the bind failed: {row.get('error_message')}")
        value = secret(minted(self.token, row["id"]))
        (operation,) = [
            op for op in self.operations(row["id"]) if op["operation"] == "bind"
        ]
        name = operation["pod_name"]
        pod = wait_for(
            "the bind pod seen while it ran",
            lambda: self.watch.pods.get(name),
            timeout=30,
            interval=1,
        )
        problems = pod_problems(
            pod, digest=self.digests["compatible"], namespace=self.namespace
        )
        self.report.check(
            "bind: one bind-time pod ran, unprivileged: restartPolicy Never with "
            "a deadline, no ServiceAccount token, every capability dropped, no "
            "privilege escalation, seccomp RuntimeDefault, no host namespaces, "
            "only its own Secret, the image by digest under the shim",
            not problems,
            "; ".join(problems),
        )
        keys = self.watch.secret_keys.get(name)
        owners = self.watch.secret_owners.get(name) or []
        self.report.check(
            "bind: the pod's Secret held only request.json and its identity, "
            "owned by the pod (never the app Secret)",
            keys == ["identity", "request.json"] and owners in ([], ["Pod"]),
            f"keys={keys} owners={owners}",
        )
        problems = policy_problems(
            self.watch.policies.get(name),
            pod,
            exchange_port=self.exchange_port,
            namespace=LOCAL_NAMESPACE,
        )
        self.report.check(
            "bind: the pod's NetworkPolicy existed while it ran: it selects the "
            "pod, admits nothing and reaches only the lease exchange's port",
            not problems,
            "; ".join(problems),
        )
        self.bind_identity = self.watch.identities.get(name, "")
        facts = self.delivered(f"srw/thread-id={thread}", row["id"], with_file=True)
        self.report.check(
            "bind: the workspace has the variable the driver minted for this "
            "binding and its credential file (0600), named by "
            f"{FILE_VARIABLE}",
            bool(facts)
            and facts.get("file_sha")
            == hashlib.sha256((value + "\n").encode()).hexdigest()
            and facts.get("file_mode") == "600"
            # The store keeps the file under ~/.srw-credentials and links the
            # declared path to it; the variable names the stored file.
            and bool(facts.get("file_real"))
            and facts.get("file_real") == facts.get("var_real"),
            json.dumps(
                {k: facts.get(k) for k in ("file_mode", "file_var", "file_real")}
            ),
        )
        process = process_facts(facts.get("process", ""))
        problems = unprivileged_process(process)
        self.report.check(
            "bind: inside the driver the kernel reported its own UID, no "
            "effective, permitted or bounding capability, no_new_privs and "
            "seccomp filtering",
            bool(process) and not problems,
            facts.get("process", "") + ("; " + "; ".join(problems) if problems else ""),
        )
        workspace = self.workspace_pod(f"srw/thread-id={thread}")
        leaked = run(
            K
            + ["exec", "-i", workspace, "-c", WORKSPACE_CONTAINER, "--"]
            + ["grep", "-rlF", "-f", "-", HOME],
            data=self.token + "\n",
            timeout=120,
        )
        self.report.check(
            "bind: the connector's token itself never reached the workspace (the "
            "driver minted what it delivers)",
            leaked[0] == 1 and not leaked[1],
            leaked[1][:200],
        )
        gone = self.objects_gone(name)
        (closed,) = [
            op for op in self.operations(row["id"]) if op["operation"] == "bind"
        ]
        self.report.check(
            "bind: afterwards the pod, its Secret and its policy are gone and the "
            "operation is recorded finished and removed",
            gone
            and closed["status"] == "finished"
            and closed["exit_code"] == 0
            and closed["removed"],
            json.dumps(closed),
        )

    # -- identity --------------------------------------------------------------
    def identity_checks(self) -> None:
        if not self.bind_identity:
            raise GateError("the bind pod's identity was not seen")
        lease = secret(well_formed_token("scl"))
        exchange, introspect, replay = self.exchange_calls(
            [
                {
                    "path": EXCHANGE_PATH,
                    "token": self.bind_identity,
                    "body": {"lease_token": lease, "operation": "read"},
                },
                {
                    "path": INTROSPECT_PATH,
                    "token": self.bind_identity,
                    "body": {"lease_token": lease},
                },
                {
                    "path": RESULT_PATH,
                    "token": self.bind_identity,
                    "body": {
                        "protocol_version": "1.0",
                        "operation": "bind",
                        "exit_code": 0,
                        "lines": [{"type": "result", "result": {}}],
                    },
                },
            ]
        )
        self.report.check(
            "identity: the bind pod's sdi_ is refused by the lease exchange and "
            "its introspection (unknown_driver_identity)",
            exchange["status"] == 401
            and exchange["body"].get("error") == "unknown_driver_identity"
            and introspect["status"] == 401
            and introspect["body"].get("error") == "unknown_driver_identity",
            json.dumps([exchange, introspect]),
        )
        self.report.check(
            "identity: a replay of the bind pod's result is refused "
            "(409 operation_closed)",
            replay["status"] == 409
            and replay["body"].get("error") == "operation_closed",
            json.dumps(replay),
        )

    # -- moved tag -------------------------------------------------------------
    def moved_tag_checks(self) -> None:
        self.digests["incompatible"] = self.build(self.gate_id, incompatible(self.spec))
        seen_before = set(self.watch.pods) if self.watch else set()
        # The k3d profile caches a tag's resolution for 5 seconds.
        time.sleep(8)
        thread = self.create_session("two", ["example"])
        row = self.settled(thread)
        problems = refusal_problems(row, digest=self.digests["incompatible"])
        self.report.check(
            "moved-tag: session two's bind is refused: the image behind the tag "
            "changed its contract (the token slot gone, region required)",
            not problems,
            "; ".join(problems) + f" | {row.get('error_message')}",
        )
        self.report.check(
            "moved-tag: no driver pod ran for the refused bind",
            self.operations(row["id"]) == []
            and set(self.watch.pods if self.watch else ()) == seen_before,
        )
        last = self.driver_status("example").get("last_bind") or {}
        self.report.check(
            "moved-tag: the connector shows the refusal (driver_status.last_bind)",
            last.get("status") == "failed"
            and "changed its contract" in (last.get("message") or "")
            and last.get("digest") == self.digests["incompatible"],
            json.dumps(last)[:400],
        )
        lines = self.noticed(f"srw/thread-id={thread}", ["changed its contract"])
        self.report.check(
            "moved-tag: session two goes on without the connector, and its "
            "README says why",
            any("changed its contract" in line for line in lines),
            " | ".join(lines)[:400],
        )
        audited = sql(
            "SELECT count(*) FROM security_events WHERE event_type = "
            "'connector_driver_image_refused' AND resource_id = "
            f"{lit(self.connector)}"
        )
        self.report.check(
            "moved-tag: the refusal is audited",
            audited.isdigit() and int(audited) >= 1,
            f"{audited} events",
        )
        # The compatible image again, for every later bind.
        self.digests["restored"] = self.build(self.gate_id, self.spec)
        time.sleep(8)

    # -- detach ------------------------------------------------------------------
    def detach_checks(self) -> None:
        thread = self.threads["one"]
        bound = self.binding(thread)
        if not bound or bound["status"] != "bound":
            raise GateError("session one has no bound binding to detach")
        self.owner.ok(
            "PATCH",
            f"/api/persistent/threads/{thread}/config",
            {"datasource_ids": []},
        )
        at_once = self.binding(thread) or {}
        row = self.revoked(bound["id"], passes=2)
        revoke = self.revoke_operation(bound["id"])
        self.report.check(
            "detach: the live detach moved session one's binding to revoking at "
            "once (connector_detached), and a revoke pod retired it within a "
            "reconciler pass with the binding's inputs and driver_state",
            at_once.get("status") in ("revoking", "revoked")
            and at_once.get("revoke_reason") == "connector_detached"
            and row is not None
            and not row.get("revoke_error")
            and revoke is not None
            and revoke["status"] == "finished"
            and revoke["exit_code"] == 0,
            json.dumps({"at_once": at_once.get("status"), "revoke": revoke})[:400],
        )
        self.report.check(
            "detach: the revoke pod, its Secret and its policy are gone",
            revoke is not None and self.objects_gone(revoke["pod_name"]),
        )

    # -- refusals ------------------------------------------------------------------
    def refusal_checks(self) -> None:
        for label, (misbehave, _reason) in REFUSALS.items():
            self.create_connector(label, {"misbehave": misbehave})
        self.create_connector("doomed")
        thread = self.create_session("refusals", [*REFUSALS, "doomed"])
        rows = {
            label: self.settled(thread, self.connectors[label])
            for label in [*REFUSALS, "doomed"]
        }
        for label, (_misbehave, reason) in REFUSALS.items():
            row = rows[label]
            last = self.driver_status(label).get("last_bind") or {}
            self.report.check(
                f"refusals: {label}: the bind is refused with a visible reason on "
                "the binding and the connector",
                row["status"] in ("revoking", "revoked")
                and "will not deliver" in (row.get("error_message") or "")
                and reason in (row.get("error_message") or "")
                and reason in (last.get("message") or ""),
                (row.get("error_message") or "")[:300],
            )
        lines = self.noticed(
            f"srw/thread-id={thread}", [self.name(label) for label in REFUSALS]
        )
        self.report.check(
            "refusals: the session's README says each refused connector was not "
            "delivered, and why",
            all(
                any(self.name(label) in line and reason in line for line in lines)
                for label, (_misbehave, reason) in REFUSALS.items()
            ),
            " | ".join(lines)[:600],
        )
        retired = {label: self.revoked(rows[label]["id"]) for label in REFUSALS}
        self.report.check(
            "refusals: what each refused bind minted is revoked (binding_refused) "
            "in a revoke pod",
            all(
                row is not None
                and row.get("revoke_reason") == "binding_refused"
                and not row.get("revoke_error")
                for row in retired.values()
            ),
            json.dumps(retired)[:400],
        )
        doomed = rows["doomed"]
        if doomed["status"] != "bound":
            raise GateError(f"doomed did not bind: {doomed.get('error_message')}")
        connector = self.connectors["doomed"]
        status, _body = self.owner.call("DELETE", f"/api/datasources/{connector}")
        if status in (200, 204):
            self.deleted_connectors.add("doomed")
        row = self.revoked(doomed["id"])
        revoke = self.revoke_operation(doomed["id"])
        self.report.check(
            "refusals: a bound connector deleted is still revoked "
            "(connector_deleted) by a revoke pod that received the inputs and "
            "driver_state of its bind",
            status in (200, 204)
            and row is not None
            and row.get("revoke_reason") == "connector_deleted"
            and not row.get("revoke_error")
            and revoke is not None
            and revoke["exit_code"] == 0,
            json.dumps({"delete": status, "row": row, "revoke": revoke})[:400],
        )

    # -- pinned --------------------------------------------------------------------
    def pinned_checks(self) -> None:
        self.create_connector("fail", {"misbehave": "fail"})
        started = time.time()
        thread = self.create_session("pinned", ["example", "fail"], pinned=True)
        lane = sql(f"SELECT execution_lane FROM threads WHERE id = {lit(thread)}")
        self.report.check(
            "pinned: the Officer conference runs on the pinned lane",
            lane == PINNED_LANE,
            lane,
        )
        if lane != PINNED_LANE:
            raise GateError(f"session lane is {lane!r}, not pinned")
        row = self.settled(thread)
        failing = self.settled(thread, self.connectors["fail"])
        facts = (
            self.delivered(f"srw/thread-id={thread}", row["id"])
            if row["status"] == "bound"
            else {}
        )
        self.report.check(
            "pinned: the pinned session binds example and its workspace has the "
            "minted variable",
            row["status"] == "bound" and bool(facts),
            json.dumps({"status": row["status"], "error": row.get("error_message")}),
        )
        self.report.check(
            "pinned: fail fails for good (a config error, never retried)",
            failing["status"] == "failed"
            and failing.get("error_class") == "config"
            and failing.get("retry_at") is None
            and FAILING in (failing.get("error_message") or ""),
            json.dumps(failing)[:300],
        )
        lines = self.noticed(f"srw/thread-id={thread}", [FAILING])
        last = self.driver_status("fail").get("last_bind") or {}
        self.report.check(
            "pinned: the README and the connector say why fail was not delivered",
            any(FAILING in line for line in lines)
            and FAILING in (last.get("message") or ""),
            " | ".join(lines)[:300],
        )
        self.report.check(
            "pinned: the session stays usable: it answers its turn",
            self.answered(thread),
        )
        if row["status"] == "bound":
            detached = self.live_update(thread, ["fail"], "detach")
            retired = self.revoked(row["id"])
            facts = self.unset(f"srw/thread-id={thread}")
            self.report.check(
                "pinned: a live detach over the session's WebSocket revokes the "
                "binding (connector_detached) and unsets EXAMPLE_TOKEN in its "
                "workspace",
                detached.get("outcome") == "config.changed"
                and retired is not None
                and retired.get("revoke_reason") == "connector_detached"
                and not retired.get("revoke_error")
                and bool(facts),
                json.dumps({"update": detached, "revoked": retired})[:400],
            )
            attached = self.live_update(thread, ["example", "fail"], "attach")
            again = self.settled(thread)
            facts = (
                self.delivered(f"srw/thread-id={thread}", again["id"])
                if again["status"] == "bound"
                else {}
            )
            self.report.check(
                "pinned: attaching it live again binds anew and delivers the "
                "new binding's variable",
                attached.get("outcome") == "config.changed"
                and again["id"] != row["id"]
                and again["status"] == "bound"
                and bool(facts),
                json.dumps({"update": attached, "status": again["status"]})[:300],
            )
        since = max(60, int(time.time() - started) + 30)
        rc, logs, _err = run(
            K
            + ["logs", ORCHESTRATOR, "-c", ORCHESTRATOR_CONTAINER, f"--since={since}s"],
            timeout=120,
        )
        failures = [
            line
            for line in logs.splitlines()
            if thread in line
            and ("connector leases unavailable" in line or "Traceback" in line)
        ]
        self.report.check(
            "pinned: no attach of the session logged a connector delivery failure",
            rc == 0 and not failures,
            " | ".join(failures)[:300],
        )

    # -- job -------------------------------------------------------------------------
    def job_checks(self) -> None:
        created = time.time()
        job = self.create_job("job", ["example"])
        row = self.settled(job, kind="job")
        claimed = sql(
            "SELECT coalesce(min(created_at)::text, '') FROM connector_driver_operations "
            f"WHERE binding_id = {lit(row['id'])} AND operation = 'bind'"
        )
        self.report.check(
            f"job: the {self.args.job_lane} job's binding is bound",
            row["status"] == "bound",
            json.dumps({"status": row["status"], "error": row.get("error_message")}),
        )
        if row["status"] != "bound":
            return
        bound_at = parse_instant(str(row.get("bound_at") or ""))
        handed_on = wait_for(
            f"the dispatcher hands job {job} on",
            lambda: self.dispatch_instant(job, created),
            timeout=self.args.turn_timeout,
            interval=5,
        )
        self.report.check(
            f"job: the dispatcher held the {self.args.job_lane} job until its "
            "bind ended (its log hands it on at or after bound_at)",
            bound_at is not None and handed_on >= bound_at,
            json.dumps(
                {
                    "bound_at": str(bound_at),
                    "handed_on": str(handed_on),
                    "line": DISPATCH_LINES[self.args.job_lane],
                }
            ),
        )
        facts = self.delivered(f"srw/job-id={job}", row["id"])
        self.report.check(
            "job: its workspace received the variable the driver minted for it",
            bool(facts),
        )

        def ended(statuses: frozenset[str]) -> str:
            try:
                return wait_for(
                    f"job {job} ends",
                    lambda: (s if (s := self.job_status(job)) in statuses else None),
                    timeout=self.args.turn_timeout * 2,
                    interval=5,
                )
            except GateError:
                return self.job_status(job)

        status = ended(JOB_TERMINAL)
        if status == "pending_review":
            # A review pause keeps the execution (and its binding) alive.
            self.owner.call("POST", f"/api/jobs/{job}/approve", {})
            status = ended(JOB_TERMINAL - {"pending_review"})
        retired = self.revoked(row["id"])
        revoke = self.revoke_operation(row["id"])
        self.report.check(
            "job: when the job ends its binding is revoked (execution_ended) in "
            "a revoke pod",
            status in JOB_TERMINAL
            and status != "pending_review"
            and retired is not None
            and retired.get("revoke_reason") == "execution_ended"
            and revoke is not None
            and revoke["exit_code"] == 0,
            json.dumps({"job": status, "bind": claimed, "revoke": revoke})[:400],
        )

    # -- disable ---------------------------------------------------------------------
    def disable_checks(self) -> None:
        registration = self.registrations["account"][0]
        refused, _body = self.viewer.call(
            "POST", f"/api/connector-drivers/{registration}/disable"
        )
        project_registration = self.registrations.get("project", ("", ""))[0]
        refused_project, _body = (
            self.viewer.call(
                "POST", f"/api/connector-drivers/{project_registration}/disable"
            )
            if project_registration
            else (403, None)
        )
        self.report.check(
            "disable: the viewer may disable neither the owner's registration "
            "(404: it cannot see it) nor the project's (403)",
            refused == 404 and refused_project == 403,
            f"account={refused} project={refused_project}",
        )
        live = self.binding_in(self.threads.get("pinned", ""), ("bound",))
        status, body = self.owner.call(
            "POST", f"/api/connector-drivers/{registration}/disable"
        )
        self.report.check(
            "disable: the owner disables the registration",
            status == 200 and (body or {}).get("disabled") is True,
            f"HTTP {status}",
        )
        if live is not None:
            row = self.revoked(live["id"])
            self.report.check(
                "disable: the pinned session's live binding is revoked "
                "(registration_disabled)",
                row is not None and row.get("revoke_reason") == "registration_disabled",
                json.dumps(row)[:300],
            )
        self.report.check(
            "disable: the connector says its registration is disabled",
            self.driver_status("example").get("notice") == "registration disabled",
        )
        pinned = self.threads.get("pinned")
        if live is not None and pinned:
            redelivered = self.live_update(pinned, ["example", "fail"], "redeliver")
            facts = self.unset(f"srw/thread-id={pinned}")
            lines = self.noticed(f"srw/thread-id={pinned}", ["disabled"])
            self.report.check(
                "disable: the pinned session's next delivery unsets EXAMPLE_TOKEN "
                "and its README says the connector's registration is disabled",
                redelivered.get("outcome") == "config.changed"
                and bool(facts)
                and any("disabled" in line for line in lines),
                json.dumps({"update": redelivered, "notices": lines})[:400],
            )
        seen_before = set(self.watch.pods if self.watch else ())
        thread = self.create_session("disabled", ["example"])
        row = self.settled(thread)
        self.report.check(
            "disable: a new session's bind is refused (disabled), without a pod",
            row["status"] == "failed"
            and "disabled" in (row.get("error_message") or "")
            and self.operations(row["id"]) == []
            and set(self.watch.pods if self.watch else ()) == seen_before,
            (row.get("error_message") or "")[:200],
        )

    # -- cleanup ---------------------------------------------------------------
    def cleanup(self) -> list[str]:
        problems: list[str] = []

        def step(label: str, action: Callable[[], Any]) -> None:
            try:
                if action() is False:
                    problems.append(label)
            except GateError as exc:
                problems.append(f"{label} ({exc})")

        for label, job in self.jobs.items():

            def delete_job(job=job) -> bool:
                self.owner.call("PUT", f"/api/jobs/{job}/cancel")

                def gone() -> bool:
                    status, _body = self.owner.call("DELETE", f"/api/jobs/{job}")
                    return status in (200, 204, 404)

                return bool(wait_for("job deleted", gone, timeout=240, interval=10))

            step(f"delete job {label}", delete_job)
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
        for label, connector in self.connectors.items():
            if label in self.deleted_connectors:
                continue
            step(
                f"delete the {label} connector",
                lambda connector=connector: self.owner.call(
                    "DELETE", f"/api/datasources/{connector}"
                )[0]
                in (200, 204, 404),
            )
        apis = {"owner": self.owner, "editor": self.editor, "viewer": self.viewer}
        for label, (registration, who) in list(self.registrations.items()):

            def delete_registration(registration=registration, who=who) -> bool:
                # Refused (409) while a binding of it is unrevoked: the
                # reconciler revokes the deleted connectors' first.
                def deleted() -> bool:
                    status, _body = apis[who].call(
                        "DELETE", f"/api/connector-drivers/{registration}"
                    )
                    if status == 409:
                        apis[who].call(
                            "POST", f"/api/connector-drivers/{registration}/disable"
                        )
                    return status in (200, 204, 404)

                return bool(
                    wait_for(
                        "registration deleted",
                        deleted,
                        timeout=max(300, 10 * self.reconcile_seconds),
                        interval=10,
                    )
                )

            step(f"delete registration {label}", delete_registration)
        if self.watch is not None:
            self.watch.stopped.set()
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
        for role, app_id in self.app_rows.items():
            step(
                f"delete the {role}'s app row",
                lambda app_id=app_id: self.owner.call("DELETE", f"/api/users/{app_id}")[
                    0
                ]
                in (200, 204, 404),
            )
        for role in ("editor", "viewer"):
            if self.users_started[role]:
                step(
                    f"delete the {role}'s Keycloak account",
                    lambda role=role: self.keycloak("delete", role).get("refused")
                    == [],
                )
        if self.client_started:
            step(
                "delete the OAuth client",
                lambda: self.keycloak("delete").get("refused") == [],
            )
        step(
            "delete the gate's image rows",
            lambda: sql(
                "DELETE FROM connector_driver_images WHERE reference IN ("
                + ", ".join(lit(self.reference(tag)) for tag in self.own_tags())
                + ")"
            )
            is not None,
        )
        if self.images_pushed:
            step(
                "delete the gate's tags from the k3d registry", self.delete_pushed_tags
            )
            for tag in self.own_tags():
                step(
                    f"remove the local image {tag}",
                    lambda tag=tag: run(
                        ["docker", "rmi", f"{LOCAL_REGISTRY}/{REPOSITORY}:{tag}"]
                    )[0]
                    in (0, 1),
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

    def delete_pushed_tags(self) -> bool:
        """Remove the gate's tags from the k3d registry, and nothing else."""
        tags = REGISTRY_TAGS.format(repository=REPOSITORY)
        for tag in self.own_tags():
            command(
                ["docker", "exec", REGISTRY_CONTAINER, "rm", "-rf", f"{tags}/{tag}"]
            )
        left = set(self.registry_tags())
        return not any(tag in left for tag in self.own_tags())

    def residue(self) -> list[str]:
        """What this run created and cleanup did not remove."""
        left: list[str] = []
        if self.images_pushed:
            tags = [tag for tag in self.own_tags() if tag in self.registry_tags()]
            if tags:
                left.append(f"registry tags {tags}")
        titled = self.titled_threads()
        if titled:
            left.append(f"sessions titled with the gate id: {titled}")
        for label, job in self.jobs.items():
            if sql(f"SELECT count(*) FROM jobs WHERE id = {lit(job)}") != "0":
                left.append(f"job {label} {job}")
        prefix = self.gate_id + " %"
        count = sql(f"SELECT count(*) FROM datasources WHERE name LIKE {lit(prefix)}")
        if count != "0":
            left.append(f"{count} connectors")
        ids = [r for r, _who in self.registrations.values() if _UUID_RE.fullmatch(r)]
        if ids:
            listed = ", ".join(lit(value) for value in ids)
            rows = sql(
                f"SELECT count(*) FROM connector_driver_registrations WHERE id IN ({listed})"
            )
            if rows != "0":
                left.append(f"{rows} registrations")
        connectors = [c for c in self.connectors.values() if _UUID_RE.fullmatch(c)]
        if connectors:
            listed = ", ".join(lit(value) for value in connectors)
            for table, open_rows in (
                (
                    "connector_bind_time_bindings",
                    "status IN ('pending', 'bound', 'revoking')",
                ),
                ("connector_driver_operations", "removed_at IS NULL"),
            ):
                rows = sql(
                    f"SELECT count(*) FROM {table} WHERE connector_id IN ({listed}) "
                    f"AND {open_rows}"
                )
                if rows != "0":
                    left.append(f"{rows} open rows in {table}")
        if (
            self.project
            and sql(f"SELECT count(*) FROM projects WHERE id = {lit(self.project)}")
            != "0"
        ):
            left.append(f"project {self.project}")
        for role, app_id in self.app_rows.items():
            if sql(f"SELECT count(*) FROM users WHERE id = {lit(app_id)}") != "0":
                left.append(f"the {role}'s app row {app_id}")
        for role in ("editor", "viewer"):
            if self.users_started[role]:
                try:
                    counts = self.keycloak("count", role)
                    if counts.get("users"):
                        left.append(f"Keycloak residue for the {role}")
                except GateError as exc:
                    left.append(f"Keycloak residue unknown ({exc})")
        if self.client_started:
            try:
                if self.keycloak("count").get("clients"):
                    left.append("the OAuth client")
            except GateError as exc:
                left.append(f"Keycloak residue unknown ({exc})")
        if self.namespace:
            for connector in connectors:
                try:
                    wait_for(
                        "driver objects of the connector gone",
                        lambda connector=connector: not run(
                            self.kc
                            + ["get", "pod,secret,networkpolicy", "-l"]
                            + [f"srw.io/connector-id={connector}", "-o", "name"],
                            timeout=60,
                        )[1],
                        timeout=max(120, 6 * self.reconcile_seconds),
                        interval=5,
                    )
                except GateError:
                    left.append(f"driver objects of connector {connector}")
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
            self.register_checks()
            # bind creates the example connector and session one every later
            # phase needs; a failure there ends the run (cleanup still runs).
            self.bind_checks()
            for phase in (
                self.identity_checks,
                self.moved_tag_checks,
                self.detach_checks,
                self.refusal_checks,
                self.pinned_checks,
                self.job_checks,
                self.disable_checks,
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
                if self.watch is not None:
                    self.watch.stopped.set()
                print(
                    "kept: "
                    + json.dumps(
                        {
                            "connectors": self.connectors,
                            "threads": self.threads,
                            "jobs": self.jobs,
                            "registrations": self.registrations,
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
  connectors.servicePods.refusedCidrs: [172.16.0.0/12, 10.42.0.0/16, 10.43.0.0/16, 169.254.0.0/16]
  connectors.drivers.registry.insecureHosts: ["srw-registry:5000"]
  connectors.drivers.registry.privateHosts: ["srw-registry:5000"]
  connectors.drivers.registry.resolveCacheSeconds: 5
Left at their defaults: connectors.customDrivers.privileged (false) and
connectors.drivers.trustedRepositories (empty; never the k3d registry), so the
example image is a custom, unprivileged driver; connectors.customDrivers.
bindWaitSeconds (20). The pinned session is an Officer conference in the
gate's own project; the job's lane is --job-lane (pinned by default).
Tilt overrides the shim image (repository, tag, digest).
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
    parser.add_argument(
        "--job-lane",
        choices=("pinned", "stateless"),
        default="pinned",
        help="the job phase's execution lane (both go through the dispatcher)",
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
        raise SafetyError("--gate-id must be d6- followed by 10 hex digits")
    if not _MODEL_RE.fullmatch(args.model):
        raise SafetyError("model id is malformed")
    if not re.fullmatch(r"[a-z][a-z0-9._-]{0,63}", args.user):
        raise SafetyError("user name is malformed")
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
        print(VALUES_LOCAL_KEYS)
        return 0
    return CustomDriverGate(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
