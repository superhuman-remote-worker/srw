#!/usr/bin/env python3
"""Local k3d gate for connector drivers D6: registered bind-time image drivers.

Design: knowledge-base/knowledge/features/connector_drivers.md, "Trust and
registration", "The driver namespace baseline", "Driver versions", "Three
planes" (bind-time) and slice D6, whose gate this proves: a custom driver
image registered at Account scope delivers an env binding to a workspace and
runs unprivileged; a moved tag with an incompatible spec is refused at bind.
Templates: scripts/k3d-service-driver-gate.py (D5: the hosting preflights,
the moved tag pushed to the k3d registry) and scripts/k3d-managed-mcp-gate.py
(D5a: disposable OAuth client and accounts). The same safety envelope:
dry-run by default, the exact k3d-srw/srw context, secrets only on
``kubectl exec -i`` stdin and scrubbed from every printed line, every in-pod
program capping its own memory, and a cleanup in ``finally`` that touches
only what this run created and then checks for residue by gate id.

It needs the k3d profile of deployment/values-local.yaml.example (keys in
--help), Tilt (which builds the shim and pins it by digest) and a local
docker that can push to localhost:5005. The driver is SRW's example driver
(docker/Dockerfile.driver-example, example.env/v1), built by this gate with
its io.srw.driver.spec label from drivers/example/spec.json: a custom image,
outside srw.* and outside any trusted repository.

Fixtures (all disposable, named after the gate id):

  client      ``<gate id>-oauth``, a public Keycloak client the accounts log
              in with (the D3c/D5a fixture)
  accounts    ``<gate id>-ed`` (a Project editor) and ``<gate id>-vw`` (a
              Project viewer, and the "other user" of the Account checks):
              Keycloak users and app rows admitted before their first login
  images      localhost:5005/srw-driver-example:<gate id> (the compatible
              build, then the incompatible one under the same tag) and
              :<gate id>-srw (a label naming srw.example/v1)
  project     one project of the owner, the editor and the viewer members
  connector   ``example`` of the owner, of the owner's Account registration
  sessions    ``one`` (bound with the compatible image) and ``two`` (after
              the tag moved)

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
              spec, the pushed digest, tier custom, no privilege; an image
              naming srw.example/v1 is refused ("SRW's own"); the viewer
              (another user) neither lists nor reads it, and cannot create a
              connector of it by id or by name; the project editor registers
              at Project scope, the viewer cannot (403) but lists it; the
              owner's capability matrix shows the registration, the
              viewer's does not
  bind        session one binds the connector: one bind-time pod ran,
              unprivileged (restartPolicy Never, a deadline, no
              ServiceAccount token, every capability dropped, no privilege
              escalation, seccomp RuntimeDefault, no host namespaces, the
              image by its digest under the shim), its Secret held only
              request.json and the identity; inside the driver, the kernel
              reported UID 10001, no effective or bounding capability,
              no_new_privs and seccomp filtering (EXAMPLE_DRIVER_PROCESS);
              the workspace has the variable the driver minted for this
              binding (never the connector's token) and its credential file
              (0600); the binding records reference, digest, resolution time,
              spec hash and protocol version; afterwards the pod, its Secret
              and its policy are gone and the operation is recorded removed
  moved-tag   an incompatible image is pushed under the same tag; session
              two's bind is refused without a pod ("changed its contract",
              the removed slot and the new required config named), recorded
              on the binding with the new digest, shown on the connector
              (driver_status.last_bind) and audited
  revoke      session one ends: the reconciler revokes its binding in a
              revoke pod (reason execution_ended), and that pod is gone too
  cleanup     sessions, connector, registrations, project, accounts, OAuth
              client, probe pod, the gate's registry tags and image rows are
              gone; no driver-namespace object names this run's connector

Run with the repository venv on the k3d-srw cluster, alone: this is a
mutating gate. The owner (--user) must be an administrator (it admits and
deletes the disposable accounts).

  .venv/bin/python scripts/k3d-custom-driver-gate.py           # plan
  .venv/bin/python scripts/k3d-custom-driver-gate.py \\
      --run --confirm LOCAL-K3D-DISPOSABLE
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import re
import secrets
import subprocess
import sys
import threading
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
HOME = "/home/agent-host"
DRIVER = "example.env/v1"
IMAGE_TYPE = "image_driver"
GATE_LABEL = "srw.io/gate"
ACCOUNT_DOMAIN = "example.invalid"
LOCAL_REGISTRY = "localhost:5005"
CLUSTER_REGISTRY = "srw-registry:5000"
REPOSITORY = "srw-driver-example"
SPEC_FILE = ROOT / "drivers/example/spec.json"
DOCKERFILE = "docker/Dockerfile.driver-example"
SPEC_LABEL = "io.srw.driver.spec"
BIND_TIME_MANAGER = "connector-bind-time"
#: The UID docker/Dockerfile.driver-example runs the driver as.
DRIVER_UID = "10001"
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

# What the workspace holds after the bind, as the agent-host user (the
# variable's value and the file's contents only as hashes).
_WORKSPACE_SCRIPT = r"""
for f in ~/.srw-credentials/*.sh; do [ -r "$f" ] && . "$f"; done
printf 'value_sha=%s\n' "$(printf '%s' "${!GATE_VAR:-}" | sha256sum | cut -d' ' -f1)"
printf 'process=%s\n' "${EXAMPLE_DRIVER_PROCESS:-}"
if [ -e "$GATE_FILE" ]; then
  printf 'file_sha=%s\n' "$(sha256sum < "$GATE_FILE" | cut -d' ' -f1)"
  printf 'file_mode=%s\n' "$(stat -L -c %a "$GATE_FILE")"
fi
"""


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
    """Records every bind-time pod of one connector while it exists, with
    its Secret's keys (a bind-time pod lives seconds)."""

    def __init__(self, namespace: str, connector_id: str) -> None:
        super().__init__(daemon=True)
        self.namespace = namespace
        self.connector_id = connector_id
        self.pods: dict[str, dict] = {}
        self.secret_keys: dict[str, list[str]] = {}
        self.secret_owners: dict[str, list[str]] = {}
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

    def run(self) -> None:
        selector = (
            f"srw/managed-by={BIND_TIME_MANAGER},"
            f"srw.io/connector-id={self.connector_id}"
        )
        while not self.stopped.is_set():
            listing = self._get("pods", "-l", selector) or {}
            for pod in listing.get("items") or []:
                name = pod["metadata"]["name"]
                self.pods.setdefault(name, pod)
                if name not in self.secret_keys:
                    found = self._get("secret", name)
                    if found:
                        self.secret_keys[name] = sorted(found.get("data") or {})
                        self.secret_owners[name] = [
                            owner.get("kind", "")
                            for owner in found["metadata"].get("ownerReferences") or []
                        ]
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
    "pushed digest, tier custom, unprivileged); srw.example/v1 is refused; "
    "another user neither sees nor uses it; a Project editor registers at "
    "Project scope, a viewer cannot; the matrix shows registrations to who "
    "may see them",
    "bind: session one's bind runs one unprivileged pod (spec, Secret, the "
    "driver's own UID and capabilities) that delivers the minted variable and "
    "file to the workspace; the binding records reference, digest, "
    "resolved_at, spec_hash and protocol_version; the pod, its Secret and "
    "policy are gone afterwards",
    "moved-tag: an incompatible image under the same tag is refused at "
    "session two's bind without a pod; the refusal is on the binding, the "
    "connector (driver_status.last_bind) and in the audit",
    "revoke: ending session one revokes its binding in a revoke pod, which is "
    "gone afterwards",
    "cleanup: sessions, connector, registrations, project, accounts, OAuth "
    "client, probe pod, registry tags and image rows are gone; no "
    "driver-namespace object names this run's connector",
]


class CustomDriverGate:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.gate_id = args.gate_id or f"d6-{secrets.token_hex(5)}"
        self.report = Report(self.gate_id)
        self.owner = Api(args.user, args.password)
        hexid = self.gate_id.removeprefix("d6-")
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
        self.connector: str | None = None
        self.threads: dict[str, str] = {}
        self.images_pushed = False
        self.digests: dict[str, str] = {}
        self.deny_probe = f"{self.gate_id}-denyprobe"
        self.deny_probe_started = False
        self.namespace = ""
        self.orchestrator_ip = ""
        self.reconcile_seconds = 15
        self.deadline_seconds = 120
        self.watch: PodWatch | None = None
        self.token = secret(f"d6-token-{secrets.token_hex(16)}")
        self.variable = f"D6_GATE_{hexid.upper()}"
        self.file = f"~/.srw-files/{self.gate_id}/token"
        self.spec = json.loads(SPEC_FILE.read_text(encoding="utf-8"))

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

    def workspace_pod(self, thread: str) -> str:
        def probe() -> str | None:
            running = [
                pod["metadata"]["name"]
                for pod in self.release_pods(f"srw/thread-id={thread}")
                if pod.get("status", {}).get("phase") == "Running"
            ]
            return running[0] if len(running) == 1 else None

        return wait_for(f"workspace of {thread}", probe, timeout=300)

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

    def binding(self, thread: str) -> dict | None:
        out = sql(
            "SELECT coalesce(row_to_json(b)::text, '') FROM (SELECT id, status, "
            "image_reference, image_digest, resolved_at, spec_hash, "
            "protocol_version, error_class, error_message, revoke_reason, "
            "revoked_at FROM connector_bind_time_bindings WHERE connector_id = "
            f"{lit(self.connector)} AND owner_kind = 'thread' AND owner_id = "
            f"{lit(thread)} ORDER BY created_at DESC LIMIT 1) b"
        )
        return json.loads(out) if out else None

    def binding_in(self, thread: str, statuses: tuple[str, ...]) -> dict | None:
        found = self.binding(thread)
        return found if found and found["status"] in statuses else None

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
        return match.group(1)

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
            "its label: the pushed digest, tier custom, no privilege",
            status == 201
            and account.get("scope") == {"kind": "Account", "name": self.owner_id}
            and account.get("name") == DRIVER
            and account.get("image_digest") == self.digests["compatible"]
            and account.get("spec_source") == "label"
            and account.get("spec_hash") == spec_hash(self.spec)
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
                "config": {"variable": self.variable},
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
                "config": {"variable": self.variable},
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

    # -- bind ------------------------------------------------------------------
    def create_connector(self) -> str:
        status, parsed = self.owner.call(
            "POST",
            "/api/datasources",
            {
                "name": self.name("example"),
                "type": IMAGE_TYPE,
                "scope_mode": "all",
                "driver_registration_id": self.registrations["account"][0],
                "config": {"variable": self.variable, "file": self.file},
                "credentials": {"token": self.token},
            },
        )
        if isinstance(parsed, dict) and parsed.get("id"):
            self.connector = str(parsed["id"])
        if status not in (200, 201) or not self.connector:
            raise GateError(f"the connector create answered HTTP {status}: {parsed}")
        return self.connector

    def create_session(self, label: str) -> str:
        created = self.owner.ok(
            "POST",
            "/api/persistent/threads",
            {
                "title": self.title(label),
                "permission_mode": "autonomous",
                "project_id": self.project,
                "datasource_ids": [self.connector],
                "config_override": {"workspace": {"backend": "sandbox"}},
                "model": self.args.model,
            },
        )
        thread = str(created.get("thread_id") or created["id"])
        self.threads[label] = thread
        print(f"session {label}: {thread}", flush=True)
        self.owner.ok(
            "POST",
            f"/api/persistent/threads/{thread}/input",
            {"content": "Reply with the single word ready."},
        )
        return thread

    def bind_checks(self) -> None:
        connector = self.create_connector()
        self.watch = PodWatch(self.namespace, connector)
        self.watch.start()
        thread = self.create_session("one")
        row = wait_for(
            "session one's binding",
            lambda: self.binding_in(thread, ("bound", "failed")),
            timeout=self.args.turn_timeout,
            interval=3,
        )
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
        pod = wait_for(
            "the bind pod seen while it ran",
            lambda: self.watch.pods.get(operation["pod_name"]),
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
        keys = self.watch.secret_keys.get(operation["pod_name"])
        owners = self.watch.secret_owners.get(operation["pod_name"]) or []
        self.report.check(
            "bind: the pod's Secret held only request.json and its identity, "
            "owned by the pod (never the app Secret)",
            keys == ["identity", "request.json"] and owners in ([], ["Pod"]),
            f"keys={keys} owners={owners}",
        )
        workspace = self.workspace_pod(thread)
        script = (
            f"GATE_VAR={self.variable}\n"
            f"GATE_FILE={self.file.replace('~', HOME, 1)}\n" + _WORKSPACE_SCRIPT
        )

        def delivered() -> dict | None:
            rc, out = self.ws(workspace, script)
            if rc:
                return None
            facts = dict(line.split("=", 1) for line in out.splitlines() if "=" in line)
            expected = hashlib.sha256(value.encode()).hexdigest()
            return facts if facts.get("value_sha") == expected else None

        try:
            facts = wait_for(
                "the minted variable in session one's workspace",
                delivered,
                timeout=self.args.turn_timeout,
                interval=5,
            )
        except GateError:
            facts = {}
        self.report.check(
            "bind: the workspace has the variable the driver minted for this "
            "binding and its credential file (0600)",
            bool(facts)
            and facts.get("file_sha")
            == hashlib.sha256((value + "\n").encode()).hexdigest()
            and facts.get("file_mode") == "600",
            json.dumps({k: facts.get(k) for k in ("file_mode",)}),
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
        name = operation["pod_name"]
        try:
            wait_for(
                "the bind pod, its Secret and policy gone",
                lambda: self.gone("pod", name)
                and self.gone("secret", name)
                and self.gone("networkpolicy", name),
                timeout=120,
                interval=3,
            )
            gone = True
        except GateError:
            gone = False
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

    # -- moved tag -------------------------------------------------------------
    def moved_tag_checks(self) -> None:
        self.digests["incompatible"] = self.build(self.gate_id, incompatible(self.spec))
        seen_before = set(self.watch.pods) if self.watch else set()
        # The k3d profile caches a tag's resolution for 5 seconds.
        time.sleep(8)
        thread = self.create_session("two")
        row = wait_for(
            "session two's binding refused",
            lambda: self.binding_in(thread, ("failed", "bound")),
            timeout=self.args.turn_timeout,
            interval=3,
        )
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
        detail = self.owner.ok("GET", f"/api/datasources/{self.connector}")
        last = ((detail or {}).get("driver_status") or {}).get("last_bind") or {}
        self.report.check(
            "moved-tag: the connector shows the refusal (driver_status.last_bind)",
            last.get("status") == "failed"
            and "changed its contract" in (last.get("message") or "")
            and last.get("digest") == self.digests["incompatible"],
            json.dumps(last)[:400],
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
        self.owner.call("DELETE", f"/api/persistent/threads/{thread}?force=true")

    # -- revoke ----------------------------------------------------------------
    def revoke_checks(self) -> None:
        thread = self.threads["one"]
        bound = self.binding(thread)
        self.owner.ok("DELETE", f"/api/persistent/threads/{thread}?force=true")
        row = wait_for(
            "session one's binding revoked",
            lambda: self.binding_in(thread, ("revoked",)),
            timeout=max(240, 8 * self.reconcile_seconds + self.deadline_seconds),
            interval=5,
        )
        revokes = [
            op for op in self.operations(bound["id"]) if op["operation"] == "revoke"
        ]
        name = revokes[0]["pod_name"] if revokes else ""
        try:
            wait_for(
                "the revoke pod gone",
                lambda: name and self.gone("pod", name) and self.gone("secret", name),
                timeout=120,
                interval=3,
            )
            gone = True
        except GateError:
            gone = False
        self.report.check(
            "revoke: ending session one revoked its binding in a revoke pod "
            "(execution_ended), which is gone afterwards",
            row["revoke_reason"] == "execution_ended"
            and len(revokes) == 1
            and revokes[0]["status"] == "finished"
            and revokes[0]["exit_code"] == 0
            and gone,
            json.dumps(revokes)[:400],
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

        if self.watch is not None:
            self.watch.stopped.set()
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
        if self.connector:
            step(
                "delete the connector",
                lambda: self.owner.call("DELETE", f"/api/datasources/{self.connector}")[
                    0
                ]
                in (200, 204, 404),
            )
        apis = {"owner": self.owner, "editor": self.editor, "viewer": self.viewer}
        for label, (registration, who) in list(self.registrations.items()):
            step(
                f"delete registration {label}",
                lambda registration=registration, who=who: apis[who].call(
                    "DELETE", f"/api/connector-drivers/{registration}"
                )[0]
                in (200, 204, 404),
            )
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
        if self.connector:
            for table in (
                "connector_bind_time_bindings",
                "connector_driver_operations",
            ):
                rows = sql(
                    f"SELECT count(*) FROM {table} WHERE connector_id = "
                    f"{lit(self.connector)} AND "
                    + (
                        "status NOT IN ('revoked', 'failed')"
                        if table == "connector_bind_time_bindings"
                        else "removed_at IS NULL"
                    )
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
        if self.namespace and self.connector:
            try:
                wait_for(
                    "driver objects of the connector gone",
                    lambda: not run(
                        self.kc
                        + ["get", "pod,secret,networkpolicy", "-l"]
                        + [f"srw.io/connector-id={self.connector}", "-o", "name"],
                        timeout=60,
                    )[1],
                    timeout=max(120, 6 * self.reconcile_seconds),
                    interval=5,
                )
            except GateError:
                left.append(f"driver objects of connector {self.connector}")
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
            # bind creates the connector and the session every later phase
            # needs; a failure there ends the run (cleanup still runs).
            self.bind_checks()
            for phase in (self.moved_tag_checks, self.revoke_checks):
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
                            "connector": self.connector,
                            "threads": self.threads,
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
example image is a custom, unprivileged driver.
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
