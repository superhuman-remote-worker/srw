#!/usr/bin/env python3
"""Local k3d gate for connector drivers C2: the credential lease service.

Design: knowledge-base/knowledge/features/connector_drivers.md, "The lease
service" and slice C2.
Templates: scripts/k3d-connector-drivers-gate.py (D1a) and
scripts/k3d-connector-credential-residue-gate.py (C0) -- the same safety
envelope: dry-run by default, the exact k3d-srw/srw context, secrets only on
``kubectl exec -i`` stdin and scrubbed from every printed line, every
in-pod program capping its own memory, and a cleanup in ``finally`` that
touches only what this run created and then checks for residue by gate id.

It needs the k3d profile of deployment/values-local.yaml.example:
``orchestrator.connectorLeases.probeDriver: true`` (the development driver
``srw.lease-probe/v1``, which keeps a fake upstream secret behind a lease)
and a short ``ttlSeconds`` (120), so lapse and renewal show in minutes.

Fixtures (all disposable, all named after the gate id): a project and two
lease probe connectors, A and B, each holding a random fake secret; driver
identities (``sdi_``) minted for A and B, bound to a fictitious driver pod.

Checks (each printed PASS/FAIL; the exit status is 0 only if all pass):

  preflight  every orchestrator pod serves this checkout's lease modules and
             every stateless agent pod its lease materializer, byte for byte;
             the orchestrator runs with the probe driver, an exchange port and
             a TTL of at most --max-ttl; migrations 0346 and 0347 are applied
  port       the main port serves no exchange (8085 answers 404 on the
             exchange path and on /api/internal/connector-leases/exchange);
             the exchange port answers an unknown identity 401 with no-store
             and serves no docs, OpenAPI, health or renew route; no Ingress
             names the port, and the exchange path through the public
             ingress never answers like the exchange; a probe pod in the
             release namespace reaches 8085 but not the exchange port
  job        a stateless job with A: a lease for the job and A; its token in
             ~/.srw-credentials/leases/<A> (0600, directory 0700) with the
             lease's digest; the secret nowhere in the workspace or the agent
             log; the exchange returns the secret (by digest), no-store,
             max_cache_seconds 30, and counts; the first exchange is audited
  refusals   the exchange refuses an identity of connector B for A's lease,
             a write on a ReadOnly lease (reads pass), and a revoked identity
  pause      the job paused: its lease is not renewed and lapses; exchanges
             and introspections every few seconds never move its expiry (a
             lease cannot renew itself); there is no renew route
  resume     resumed: a NEW lease (new id, new digest in the workspace file)
             that the exchange honours
  cancel     cancelled: every lease of the job revoked (job_cancelled) and
             refused at the exchange (lease_revoked)
  delete     a second job with A, paused while its lease is live, deleted:
             the lease row went with the job and its revocation was audited
             first (reason job_deleted)
  completion a third job with A that finishes (approved if it rests in
             review): its lease revoked (job_completed or job_failed)
  session    a stateless session with A and B, one turn: both leases live and
             both token files written; idle past its first window, both are
             still live and renewed; a live detach of B revokes B's lease
             (connector_detached) and leaves A's; End revokes A's
             (session_end)
  cleanup    nothing this run created is left: sessions, jobs, connectors
             (and with them every lease and identity), the project, the probe
             pod and the workspace pods. Audit rows stay (security_events is
             the audit log; its retention prunes them)

Run with the repository venv on the k3d-srw cluster, alone: this is a
mutating gate.

  .venv/bin/python scripts/k3d-credential-lease-gate.py           # plan
  .venv/bin/python scripts/k3d-credential-lease-gate.py \\
      --run --confirm LOCAL-K3D-DISPOSABLE
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import secrets
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request
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
ORCHESTRATOR_SERVICE = "srw-orchestrator"
POSTGRES_POD = "srw-postgres-0"
WORKSPACE_CONTAINER = "workspace"
AGENT_CONTAINER = "agent"
HOME = "/home/agent-host"
KEYCLOAK_TOKEN_URL = "http://srw-keycloak:8080/realms/srw/protocol/openid-connect/token"
PUBLIC_URL = "https://localhost"
POD_ROOT = "/app"
DEFAULT_MODEL = "muse-spark-1.3-contributor"
PROBE_TYPE = "lease_probe"
PROBE_DRIVER = "srw.lease-probe/v1"
EXCHANGE_PATH = "/v1/leases/exchange"
INTROSPECT_PATH = "/v1/leases/introspect"
GATE_LABEL = "srw.io/gate"
_SELECTOR = (
    "app.kubernetes.io/instance=srw,app.kubernetes.io/name=superhuman-remote-worker"
)
_GATE_ID_RE = re.compile(r"c2-[0-9a-f]{10}\Z")
_MODEL_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@-]{0,199}\Z")
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")
JOB_TERMINAL = frozenset({"completed", "failed", "cancelled"})
JOB_RESTING = JOB_TERMINAL | {"pending_review", "paused"}

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
            "src/shared/content_redaction.py",
            "src/shared/logging_format.py",
            "src/orchestrator/application/__init__.py",
            "src/orchestrator/application/background_tasks.py",
            "src/orchestrator/application/connectors.py",
            "src/orchestrator/application/settings.py",
            "src/orchestrator/database/postgres.py",
            "src/orchestrator/database/migrations/app/0346_connector_driver_identities.sql",
            "src/orchestrator/database/migrations/app/0347_connector_credential_leases.sql",
            "src/orchestrator/routers/connector_lease_exchange.py",
            "src/orchestrator/services/connector_credential_leases.py",
            "src/orchestrator/services/connector_driver_identities.py",
            "src/orchestrator/services/connector_lease_exchange.py",
            "src/orchestrator/services/container_provisioner.py",
            "src/orchestrator/services/job_control_delivery.py",
            "src/orchestrator/services/job_controls.py",
            "src/orchestrator/services/job_start_bundle.py",
            "src/orchestrator/services/legacy_job_completion.py",
            "src/orchestrator/services/manifest_execution.py",
            "src/orchestrator/services/session_attach_binding.py",
            "src/orchestrator/services/thread_config_update.py",
            "src/orchestrator/services/thread_workspace_delivery.py",
            "src/orchestrator/services/unit_claim_bundle.py",
        ),
    ),
    ServedSet(
        "stateless agent",
        "agent-stateless",
        AGENT_CONTAINER,
        (SHARED_CONNECTORS, AGENT_CONNECTORS),
        (
            "src/shared/runtime/core/backends/remote.py",
            "src/shared/runtime/core/credential_env.py",
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


def raw_contains(args: list[str], needle: str, *, timeout: int = 120) -> bool:
    """Whether a command's unscrubbed output holds ``needle``; nothing printed."""
    try:
        result = subprocess.run(args, text=True, capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        raise GateError(f"{' '.join(args[4:7])[:80]} timed out") from None
    return needle in result.stdout or needle in result.stderr


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
# Programs run inside the orchestrator container (stdin carries every secret)
# ---------------------------------------------------------------------------

# Every program the gate runs inside a pod starts with this and calls
# cap_memory() once its imports are done: the orchestrator pod serves the
# product meanwhile, and a gate program that grows must fail with a
# MemoryError, never take the pod to the OOM killer (as in the C0, C1 and
# D1a gates).
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

# Lease operations the product has no API for (minting a driver identity is
# D5's job), and HTTP calls to a port on the orchestrator's own loopback, so
# no NetworkPolicy is involved. A lease token is decrypted here from its
# row, used, and never printed: an exchanged credential comes back as its
# SHA-256 only.
_LEASE_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import asyncio, hashlib, json, sys, urllib.error, urllib.request
from uuid import UUID
from orchestrator.database.postgres import PostgresDB
from orchestrator.security.crypto import decrypt
from orchestrator.services import connector_credential_leases as leases
from orchestrator.services.connector_driver_identities import (
    mint_driver_identity, revoke_driver_identity,
)
cap_memory()
request = json.loads(sys.stdin.readline())
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

def http(method, port, path, *, identity=None, body=None):
    headers = {"Content-Type": "application/json"}
    if identity:
        headers["Authorization"] = "Bearer " + identity
    call = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=None if body is None else json.dumps(body).encode(),
        method=method, headers=headers,
    )
    try:
        with opener.open(call, timeout=30) as response:
            status, raw, cache = (
                response.status, response.read(), response.headers.get("Cache-Control")
            )
    except urllib.error.HTTPError as error:
        status, raw, cache = error.code, error.read(), error.headers.get("Cache-Control")
    try:
        parsed = json.loads(raw.decode("utf-8", "replace")) if raw else {}
    except ValueError:
        parsed = {"raw": raw[:200].decode("utf-8", "replace")}
    if isinstance(parsed, dict) and isinstance(parsed.get("credential"), str):
        credential = parsed.pop("credential")
        parsed["credential_sha256"] = hashlib.sha256(credential.encode()).hexdigest()
    return {"status": status, "cache_control": cache, "body": parsed}

async def main():
    db = PostgresDB(min_connections=1, max_connections=2)
    await db.connect()
    try:
        action = request["action"]
        if action == "mint":
            async with db.acquire() as conn:
                minted = await mint_driver_identity(
                    conn, connector_id=request["connector"], driver=request["driver"],
                    pod_namespace=request["pod_namespace"], pod_name=request["pod_name"],
                    pod_uid=request["pod_uid"],
                )
            return {"id": minted.id, "token": minted.token}
        if action == "revoke_identity":
            async with db.acquire() as conn:
                revoked = await revoke_driver_identity(
                    conn, identity_id=request["identity_id"], reason="gate"
                )
            return {"revoked": revoked}
        if action == "issue":
            async with db.acquire() as conn:
                lease = await leases.issue_or_redeliver(
                    conn, owner=leases.LeaseOwner.job(request["job"]),
                    connector_id=request["connector"], driver=request["driver"],
                    access=request["access"],
                )
            return {"id": lease.id}
        if action == "call":
            token = request.get("lease_token")
            if request.get("lease_id"):
                async with db.acquire() as conn:
                    ciphertext = await conn.fetchval(
                        "SELECT token_ciphertext FROM connector_credential_leases "
                        "WHERE id = $1", UUID(request["lease_id"]),
                    )
                if ciphertext is None:
                    return {"error": "no such lease"}
                token = decrypt(ciphertext)
            body = request.get("body")
            if body is not None and token is not None:
                body = {**body, "lease_token": token}
            return http(
                request["method"], request["port"], request["path"],
                identity=request.get("identity"), body=body,
            )
        raise SystemExit(f"unknown action {action!r}")
    finally:
        await db.close()
print(json.dumps(asyncio.run(main())))
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


# A shell for a workspace container: one connector's lease file, by mode and
# digest only. The token is never printed. ``$2`` is the workspace user's
# home: ``kubectl exec`` may run as another user.
_LEASE_FILE_SCRIPT = r"""
f="$2/.srw-credentials/leases/$1"
if [ -f "$f" ]; then
  echo "file=$(stat -c %a "$f") dir=$(stat -c %a "$(dirname "$f")") sha256=$(sha256sum "$f" | cut -d' ' -f1)"
else
  echo missing
fi
"""


def parse_lease_file(line: str) -> dict[str, str] | None:
    """``{"file", "dir", "sha256"}`` from the script's one line, else ``None``."""
    match = re.fullmatch(r"file=(\d+) dir=(\d+) sha256=([0-9a-f]{64})", line.strip())
    if match is None:
        return None
    return {"file": match[1], "dir": match[2], "sha256": match[3]}


# A busybox probe in the release namespace: the API port must answer, the
# exchange port must not (the orchestrator's NetworkPolicy admits only the
# driver namespace to it). An HTTP answer of any status means "reachable".
_NETPROBE_SCRIPT = r"""
for i in 1 2 3; do
  if wget -q -T 4 -O /dev/null "http://$1:8085/api/health"; then echo api=http; else echo api=fail; fi
  out=$(wget -q -T 4 -O /dev/null "http://$1:$2/" 2>&1)
  case "$out" in
    ""|*"server returned"*|*HTTP/*) echo exchange=http ;;
    *) echo exchange=blocked ;;
  esac
  sleep 2
done
"""


def parse_netprobe(log: str) -> tuple[bool, bool]:
    """``(api reachable, exchange reachable)`` from the probe's last round."""
    api = [
        line.split("=", 1)[1] for line in log.splitlines() if line.startswith("api=")
    ]
    exchange = [
        line.split("=", 1)[1]
        for line in log.splitlines()
        if line.startswith("exchange=")
    ]
    if not api or not exchange:
        raise GateError("the network probe printed no verdict")
    return api[-1] == "http", exchange[-1] == "http"


def answers_like_the_exchange(body: str) -> bool:
    """Whether an HTTP body is one the exchange or introspection would send."""
    try:
        parsed = json.loads(body)
    except (TypeError, ValueError):
        return False
    return isinstance(parsed, dict) and (
        parsed.get("error") == "unknown_driver_identity" or "active" in parsed
    )


def self_renewal_verdict(observations: list[dict[str, Any]]) -> tuple[bool, str]:
    """Whether a lease used through its window kept its expiry and lapsed.

    Each observation is ``{"expires": epoch seconds, "status": exchange
    status, "error": denial or None}``, in time order.
    """
    if len(observations) < 2:
        return False, "too few observations"
    expiries = {obs["expires"] for obs in observations}
    used = [obs for obs in observations if obs["status"] == 200]
    last = observations[-1]
    if len(expiries) != 1:
        return False, f"the expiry moved: {sorted(expiries)}"
    if not used:
        return False, "the lease was never exchanged while live"
    if last["error"] != "lease_expired":
        return False, f"last answer {last['status']} {last['error']}"
    return True, f"{len(used)} exchanges, one expiry, then lease_expired"


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
    "preflight: orchestrator pods serve this checkout's lease modules and "
    "stateless agent pods its lease materializer; probe driver on, an "
    "exchange port, a TTL of at most --max-ttl; migrations 0346/0347 applied",
    "port: 8085 serves no exchange; the exchange port answers 401 no-store "
    "to an unknown identity and serves no docs, OpenAPI, health or renew "
    "route; no Ingress names it and the public ingress never answers like "
    "it; a release-namespace probe pod reaches 8085 but not the exchange port",
    "job: a stateless job with probe A gets a lease, its token file "
    "(0600 in a 0700 directory) matches the lease digest, the secret is in "
    "neither the workspace nor the agent log, and the exchange returns the "
    "secret (by digest) with no-store, max_cache_seconds 30, counted, the "
    "first exchange audited",
    "refusals: an identity of connector B, a write on a ReadOnly lease and a "
    "revoked identity are refused",
    "pause: the paused job's lease is not renewed; exchanges through its "
    "window never move its expiry and it lapses; no renew route",
    "resume: a new lease (new id and file digest) the exchange honours",
    "cancel: every lease of the job revoked (job_cancelled), refused as lease_revoked",
    "delete: a second job's live lease is revoked (audited, job_deleted) "
    "before the row cascades away",
    "completion: a third job that finishes has its lease revoked "
    "(job_completed or job_failed)",
    "session: a stateless session with A and B keeps both leases (renewed) "
    "idle past their first window; a live detach of B revokes only B's; End "
    "revokes A's",
    "cleanup: sessions, jobs, connectors (leases and identities cascade), "
    "project, probe pod and workspace pods are gone",
]


class CredentialLeaseGate:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.gate_id = args.gate_id or f"c2-{secrets.token_hex(5)}"
        self.report = Report(self.gate_id)
        self.api = Api(args.user, args.password)
        self.started = (datetime.now(timezone.utc) - timedelta(seconds=30)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        # Everything this run creates, recorded before it is created.
        self.connectors: dict[str, str] = {}  # label -> datasource id
        self.identities: dict[str, dict[str, str]] = {}  # label -> {id, token}
        self.jobs: dict[str, str] = {}  # label -> job id
        self.thread: str | None = None
        self.project: str | None = None
        self.probe_pod = f"{self.gate_id}-netprobe"
        self.probe_started = False
        self.user_id = ""
        self.main_lease: dict[str, Any] = {}
        self.port = 0
        self.ttl = 0
        self.sweep = 0
        self.secrets = {
            "a": secret(f"c2-upstream-a-{secrets.token_hex(16)}"),
            "b": secret(f"c2-upstream-b-{secrets.token_hex(16)}"),
        }

    # -- naming and helpers ------------------------------------------------
    def name(self, label: str) -> str:
        return f"{self.gate_id} {label}"

    def digest(self, label: str) -> str:
        return hashlib.sha256(self.secrets[label].encode()).hexdigest()

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

    def lease(self, payload: dict[str, Any]) -> dict:
        return in_orchestrator(_LEASE_PROGRAM, payload)

    def exchange(
        self, identity: str, lease_id: str, operation: str = "read"
    ) -> dict[str, Any]:
        return self.lease(
            {
                "action": "call",
                "method": "POST",
                "port": self.port,
                "path": EXCHANGE_PATH,
                "identity": self.identities[identity]["token"],
                "lease_id": lease_id,
                "body": {"operation": operation},
            }
        )

    def introspect(self, identity: str, lease_id: str) -> dict[str, Any]:
        return self.lease(
            {
                "action": "call",
                "method": "POST",
                "port": self.port,
                "path": INTROSPECT_PATH,
                "identity": self.identities[identity]["token"],
                "lease_id": lease_id,
                "body": {},
            }
        )

    def leases(
        self, *, job: str | None = None, thread: str | None = None
    ) -> list[dict]:
        """Every lease row of one execution, oldest first (no token)."""
        column, value = ("job_id", job) if job else ("thread_id", thread)
        if not value or not _UUID_RE.fullmatch(value):
            raise GateError("lease lookup needs an execution id")
        out = sql(
            "SELECT coalesce(json_agg(row_to_json(l) ORDER BY l.issued_at), '[]') "
            "FROM (SELECT id, connector_id, access, revoke_reason, issued_at, "
            "revoked_at IS NOT NULL AS revoked, expires_at > now() AS unexpired, "
            "extract(epoch FROM expires_at)::bigint AS expires, "
            "extract(epoch FROM now() - issued_at)::int AS age, "
            "last_renewed_at IS NOT NULL AS renewed, exchange_count, "
            "encode(token_hash, 'hex') AS digest "
            f"FROM connector_credential_leases WHERE {column} = {lit(value)}) l"
        )
        return json.loads(out or "[]")

    def live_lease(
        self, connector: str, *, job: str | None = None, thread: str | None = None
    ) -> dict | None:
        for row in self.leases(job=job, thread=thread):
            if (
                row["connector_id"] == self.connectors[connector]
                and not row["revoked"]
                and row["unexpired"]
            ):
                return row
        return None

    def lease_row(self, lease_id: str) -> dict | None:
        out = sql(
            "SELECT coalesce(row_to_json(l)::text, '') FROM (SELECT id, "
            "revoke_reason, revoked_at IS NOT NULL AS revoked, "
            "expires_at > now() AS unexpired, "
            "extract(epoch FROM expires_at)::bigint AS expires, exchange_count "
            f"FROM connector_credential_leases WHERE id = {lit(lease_id)}) l"
        )
        return json.loads(out) if out else None

    def events(self, event_type: str, resource_id: str) -> list[str]:
        out = sql(
            "SELECT coalesce(json_agg(detail), '[]') FROM security_events WHERE "
            f"event_type = {lit(event_type)} AND resource_id = {lit(resource_id)}"
        )
        return json.loads(out or "[]")

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

    def lease_file(self, selector: str, connector: str) -> dict[str, str] | None:
        pod = self.workspace_pod(selector)
        rc, out, _err = run(
            K
            + ["exec", pod, "-c", WORKSPACE_CONTAINER, "--"]
            + ["sh", "-c", _LEASE_FILE_SCRIPT, "lease", self.connectors[connector]]
            + [HOME],
            timeout=60,
        )
        if rc:
            raise GateError(f"workspace read failed on {pod}")
        return parse_lease_file(out.splitlines()[-1] if out else "")

    def workspace_holds(self, selector: str, needle: str) -> list[str]:
        """Files under the workspace home holding ``needle`` (sent on stdin)."""
        pod = self.workspace_pod(selector)
        rc, out, _err = run(
            K
            + ["exec", "-i", pod, "-c", WORKSPACE_CONTAINER, "--"]
            + ["grep", "-rlF", "-f", "-", HOME],
            data=needle + "\n",
            timeout=120,
        )
        return [line for line in out.splitlines() if line] if rc == 0 else []

    def agent_logs_hold(self, needle: str) -> bool:
        for pod in self.pods("agent-stateless"):
            if raw_contains(
                K
                + ["logs", pod["metadata"]["name"], "-c", AGENT_CONTAINER]
                + [f"--since-time={self.started}"],
                needle,
            ):
                return True
        return False

    def job_status(self, job: str) -> str:
        return sql(f"SELECT status FROM jobs WHERE id = {lit(job)}")

    def wait_status(self, job: str, statuses: frozenset[str], timeout: int) -> str:
        return wait_for(
            f"job {job} reaches {sorted(statuses)}",
            lambda: (status if (status := self.job_status(job)) in statuses else None),
            timeout=timeout,
            interval=5,
        )

    def wait_live_lease(self, connector: str, **owner: str) -> dict:
        return wait_for(
            f"a live lease for {connector}",
            lambda: self.live_lease(connector, **owner),
            timeout=self.args.job_timeout,
            interval=5,
        )

    # -- phases --------------------------------------------------------------
    def preflight(self) -> None:
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
            "preflight: orchestrator and stateless agent pods serve this "
            "checkout's lease modules",
            not problems,
            "; ".join(problems)[:600],
        )
        if problems:
            raise GateError("the deployment does not serve this checkout")
        probe = self.orchestrator_env("CONNECTOR_LEASE_PROBE_ENABLED")
        port = self.orchestrator_env("CONNECTOR_LEASE_EXCHANGE_PORT")
        ttl = self.orchestrator_env("CONNECTOR_LEASE_TTL_SECONDS")
        sweep = self.orchestrator_env("CONNECTOR_LEASE_SWEEP_INTERVAL_SECONDS")
        configured = (
            probe.lower() == "true"
            and port.isdigit()
            and int(port) not in (0, 8085)
            and ttl.isdigit()
            and 60 <= int(ttl) <= self.args.max_ttl
        )
        self.report.check(
            "preflight: probe driver on, an exchange port, a short lease TTL",
            configured,
            f"probe={probe!r} port={port!r} ttl={ttl!r} sweep={sweep!r}",
        )
        if not configured:
            raise GateError(
                "set orchestrator.connectorLeases (probeDriver: true, a ttlSeconds "
                f"of at most {self.args.max_ttl}) as the k3d profile does"
            )
        self.port, self.ttl = int(port), int(ttl)
        self.sweep = min(int(sweep) if sweep.isdigit() else 60, self.ttl // 4)
        applied = sql(
            "SELECT count(*) FROM schema_migrations WHERE success AND filename IN "
            "('0346_connector_driver_identities.sql', "
            "'0347_connector_credential_leases.sql')"
        )
        self.report.check("preflight: migrations 0346 and 0347 applied", applied == "2")
        if applied != "2":
            raise GateError("the lease migrations are not applied")
        self.user_id = sql(
            f"SELECT id FROM users WHERE preferred_username = {lit(self.args.user)}"
        )
        if not _UUID_RE.fullmatch(self.user_id):
            raise GateError(f"no user {self.args.user!r}")

    def fixture(self) -> None:
        created = self.api.ok(
            "POST",
            "/api/projects",
            {
                "name": self.name("project"),
                "description": "C2 credential lease gate (disposable)",
                "user_id": self.user_id,
            },
        )
        self.project = str(created["id"])
        for label in ("a", "b"):
            status, parsed = self.api.call(
                "POST",
                "/api/datasources",
                {
                    "name": self.name(f"probe-{label}"),
                    "type": PROBE_TYPE,
                    "scope_mode": "all",
                    "credentials": {"secret": self.secrets[label]},
                    "config": {"upstream": f"https://{self.gate_id}-{label}.invalid"},
                },
            )
            if isinstance(parsed, dict) and parsed.get("id"):
                self.connectors[label] = str(parsed["id"])
            if status not in (200, 201) or label not in self.connectors:
                raise GateError(f"probe connector {label}: HTTP {status} {parsed}")
            echoed = json.dumps(parsed)
            self.report.check(
                f"fixture: probe connector {label} created; its secret is not echoed",
                self.secrets[label] not in echoed,
            )
        for label in ("a", "b"):
            self.identities[label] = {}
            minted = self.lease(
                {
                    "action": "mint",
                    "connector": self.connectors[label],
                    "driver": PROBE_DRIVER,
                    "pod_namespace": "gate",
                    "pod_name": f"{self.gate_id}-driver-{label}",
                    "pod_uid": f"{self.gate_id}-{label}",
                }
            )
            self.identities[label] = {
                "id": minted["id"],
                "token": secret(minted["token"]),
            }
        print(
            f"fixture: project {self.project}, connectors {self.connectors}",
            flush=True,
        )

    def port_checks(self) -> None:
        bogus = "sdi_" + "0" * 49
        main = [
            self.lease(
                {
                    "action": "call",
                    "method": "POST",
                    "port": 8085,
                    "path": path,
                    "identity": bogus,
                    "body": {"lease_token": "scl_" + "0" * 49, "operation": "read"},
                }
            )
            for path in (EXCHANGE_PATH, "/api/internal/connector-leases/exchange")
        ]
        self.report.check(
            "port: the main port serves no exchange",
            all(
                call["status"] in (404, 405)
                and "unknown_driver_identity" not in json.dumps(call["body"])
                for call in main
            ),
            str([call["status"] for call in main]),
        )
        denied = self.lease(
            {
                "action": "call",
                "method": "POST",
                "port": self.port,
                "path": EXCHANGE_PATH,
                "identity": bogus,
                "body": {"lease_token": "scl_" + "0" * 49, "operation": "read"},
            }
        )
        self.report.check(
            "port: the exchange port answers an unknown identity 401, no-store",
            denied["status"] == 401
            and denied["body"] == {"error": "unknown_driver_identity"}
            and denied["cache_control"] == "no-store",
            f"{denied['status']} {denied['body']} {denied['cache_control']}",
        )
        others = {
            path: self.lease(
                {
                    "action": "call",
                    "method": method,
                    "port": self.port,
                    "path": path,
                    "body": None if method == "GET" else {},
                }
            )["status"]
            for method, path in (
                ("GET", "/docs"),
                ("GET", "/openapi.json"),
                ("GET", "/api/health"),
                ("POST", "/v1/leases/renew"),
            )
        }
        self.report.check(
            "port: the exchange port serves nothing else (no docs, OpenAPI, "
            "health or renew route)",
            all(status in (404, 405) for status in others.values()),
            str(others),
        )
        ingresses = json.loads(command(K + ["get", "ingress", "-o", "json"]))["items"]
        named = [
            ingress["metadata"]["name"]
            for ingress in ingresses
            for rule in ingress.get("spec", {}).get("rules", [])
            for path in rule.get("http", {}).get("paths", [])
            if path.get("backend", {}).get("service", {}).get("port", {}).get("number")
            == self.port
            or path.get("backend", {}).get("service", {}).get("port", {}).get("name")
            == "lease-exchange"
        ]
        self.report.check(
            "port: no Ingress routes the exchange port", not named, str(named)
        )
        public = [self.public_post(path) for path in (EXCHANGE_PATH, INTROSPECT_PATH)]
        self.report.check(
            "port: the public ingress never answers like the exchange",
            not any(answers_like_the_exchange(body) for _status, body in public),
            str([status for status, _body in public]),
        )
        self.netprobe()

    def public_post(self, path: str) -> tuple[int, str]:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        call = urllib.request.Request(
            PUBLIC_URL + path,
            data=json.dumps(
                {"lease_token": "scl_" + "0" * 49, "operation": "read"}
            ).encode(),
            method="POST",
            headers={
                "Authorization": "Bearer sdi_" + "0" * 49,
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(call, timeout=30, context=context) as response:
                return response.status, response.read(4096).decode("utf-8", "replace")
        except urllib.error.HTTPError as error:
            return error.code, error.read(4096).decode("utf-8", "replace")
        except urllib.error.URLError as error:
            return 0, str(error.reason)

    def netprobe(self) -> None:
        self.probe_started = True
        command(
            K
            + ["run", self.probe_pod, "--image=busybox:1.36", "--restart=Never"]
            + [f"--labels={GATE_LABEL}={self.gate_id}", "--command", "--"]
            + ["sh", "-c", _NETPROBE_SCRIPT, "netprobe"]
            + [ORCHESTRATOR_SERVICE, str(self.port)]
        )

        def finished() -> bool:
            phase = command(
                K + ["get", "pod", self.probe_pod, "-o", "jsonpath={.status.phase}"]
            )
            return phase in ("Succeeded", "Failed")

        wait_for("network probe finished", finished, timeout=180, interval=5)
        api, exchange = parse_netprobe(command(K + ["logs", self.probe_pod]))
        self.report.check(
            "port: a release-namespace pod reaches 8085 but not the exchange port",
            api and not exchange,
            f"api={'http' if api else 'fail'} "
            f"exchange={'http' if exchange else 'blocked'}",
        )
        command(K + ["delete", "pod", self.probe_pod, "--wait=false"])

    def create_job(self, label: str, description: str, *, approve: bool = False) -> str:
        created = self.api.ok(
            "POST",
            "/api/jobs",
            {
                "description": f"[{self.gate_id} {label}] {description}",
                "project_id": self.project,
                "datasource_ids": [self.connectors["a"]],
                "execution_lane": "stateless",
                "config_override": {
                    "workspace": {"backend": "sandbox"},
                    "llm": {"model": self.args.model},
                },
            },
        )
        job = str(created.get("job_id") or created["id"])
        self.jobs[label] = job
        print(f"job {label} {job}", flush=True)
        return job

    def job_checks(self) -> None:
        job = self.create_job(
            "main",
            "Run the shell command `sleep 25` eight times, one command per tool "
            "call, then write the word done to output/c2.txt and complete the "
            "job.",
        )
        lease = self.wait_live_lease("a", job=job)
        self.report.check(
            "job: a lease for the job and connector A, at ReadWrite",
            lease["access"] == "ReadWrite",
            f"lease {lease['id']}",
        )
        selector = f"srw/job-id={job}"
        found = wait_for(
            "the lease file in the job workspace",
            lambda: self.lease_file(selector, "a"),
            timeout=self.args.job_timeout,
            interval=5,
        )
        self.report.check(
            "job: the token file is 0600 in a 0700 directory and holds the "
            "lease's token",
            found["file"] == "600"
            and found["dir"] == "700"
            and found["sha256"] == lease["digest"],
            f"file={found['file']} dir={found['dir']} "
            f"digest_match={found['sha256'] == lease['digest']}",
        )
        held = self.workspace_holds(selector, self.secrets["a"])
        self.report.check(
            "job: the upstream secret is nowhere in the workspace", not held, str(held)
        )
        self.report.check(
            "job: the upstream secret is not in the agent log",
            not self.agent_logs_hold(self.secrets["a"]),
        )
        answer = self.exchange("a", lease["id"], "write")
        body = answer["body"]
        self.report.check(
            "job: the exchange returns the secret, no-store, cache 30 s",
            answer["status"] == 200
            and body.get("credential_sha256") == self.digest("a")
            and answer["cache_control"] == "no-store"
            and body.get("max_cache_seconds") == 30
            and body.get("access") == "ReadWrite"
            and body.get("allowed_upstream") == [f"https://{self.gate_id}-a.invalid"],
            f"{answer['status']} {sorted(body)} {answer['cache_control']}",
        )
        self.exchange("a", lease["id"], "read")
        row = self.lease_row(lease["id"]) or {}
        self.report.check(
            "job: exchanges are counted on the lease",
            row.get("exchange_count", 0) >= 2,
            f"count {row.get('exchange_count')}",
        )
        firsts = self.events("connector_lease_first_exchange", lease["id"])
        issued = self.events("connector_lease_issued", lease["id"])
        self.report.check(
            "job: issue and the first exchange are audited once",
            len(firsts) == 1 and len(issued) == 1,
            f"first={len(firsts)} issued={len(issued)}",
        )
        self.main_lease = lease

    def refusals(self) -> None:
        job, lease = self.jobs["main"], self.main_lease
        other = self.exchange("b", lease["id"], "read")
        self.report.check(
            "refusals: an identity of another connector is refused",
            other["status"] == 403
            and other["body"] == {"error": "driver_identity_of_another_connector"},
            f"{other['status']} {other['body']}",
        )
        read_only = self.lease(
            {
                "action": "issue",
                "job": job,
                "connector": self.connectors["b"],
                "driver": PROBE_DRIVER,
                "access": "ReadOnly",
            }
        )["id"]
        write = self.exchange("b", read_only, "write")
        read = self.exchange("b", read_only, "read")
        self.report.check(
            "refusals: a write on a ReadOnly lease is refused, a read passes",
            write["status"] == 403
            and write["body"] == {"error": "operation_not_allowed"}
            and read["status"] == 200
            and read["body"].get("credential_sha256") == self.digest("b"),
            f"write {write['status']} {write['body']}, read {read['status']}",
        )
        denials = sql(
            "SELECT count(*) FROM security_events WHERE event_type = "
            "'connector_lease_exchange_denied' AND "
            f"resource_id IN ({lit(lease['id'])}, {lit(read_only)})"
        )
        self.report.check(
            "refusals: every denial is audited", denials == "2", f"{denials} rows"
        )
        self.lease(
            {"action": "revoke_identity", "identity_id": self.identities["a"]["id"]}
        )
        revoked = self.exchange("a", lease["id"], "read")
        self.report.check(
            "refusals: a revoked identity is refused",
            revoked["status"] == 401
            and revoked["body"] == {"error": "driver_identity_revoked"},
            f"{revoked['status']} {revoked['body']}",
        )
        minted = self.lease(
            {
                "action": "mint",
                "connector": self.connectors["a"],
                "driver": PROBE_DRIVER,
                "pod_namespace": "gate",
                "pod_name": f"{self.gate_id}-driver-a2",
                "pod_uid": f"{self.gate_id}-a2",
            }
        )
        self.identities["a"] = {"id": minted["id"], "token": secret(minted["token"])}

    def pause_and_resume(self) -> None:
        job, lease = self.jobs["main"], self.main_lease
        self.api.ok("PUT", f"/api/jobs/{job}/pause")
        self.wait_status(job, frozenset({"paused"}), self.args.job_timeout)
        observations: list[dict[str, Any]] = []
        deadline = time.monotonic() + self.ttl + 4 * self.sweep + 60
        while time.monotonic() < deadline:
            answer = self.exchange("a", lease["id"], "read")
            self.introspect("a", lease["id"])
            row = self.lease_row(lease["id"]) or {}
            observations.append(
                {
                    "expires": row.get("expires"),
                    "status": answer["status"],
                    "error": answer["body"].get("error"),
                }
            )
            if answer["body"].get("error") == "lease_expired":
                break
            time.sleep(10)
        ok, detail = self_renewal_verdict(observations)
        self.report.check(
            "pause: a paused job's lease is not renewed, use never moves its "
            "expiry, and it lapses",
            ok,
            detail,
        )
        renew = self.lease(
            {
                "action": "call",
                "method": "POST",
                "port": self.port,
                "path": f"/v1/leases/{lease['id']}/renew",
                "identity": self.identities["a"]["token"],
                "lease_id": lease["id"],
                "body": {},
            }
        )
        self.report.check(
            "pause: there is no renew route",
            renew["status"] in (404, 405),
            str(renew["status"]),
        )
        self.api.ok("POST", f"/api/jobs/{job}/resume", {})
        fresh = wait_for(
            "a new lease after resume",
            lambda: (
                row
                if (row := self.live_lease("a", job=job)) and row["id"] != lease["id"]
                else None
            ),
            timeout=self.args.job_timeout,
            interval=5,
        )
        found = wait_for(
            "the new token in the workspace file",
            lambda: (
                f
                if (f := self.lease_file(f"srw/job-id={job}", "a"))
                and f["sha256"] == fresh["digest"]
                else None
            ),
            timeout=self.args.job_timeout,
            interval=5,
        )
        answer = self.exchange("a", fresh["id"], "read")
        old = self.lease_row(lease["id"]) or {}
        self.report.check(
            "resume: a new lease, delivered to the workspace and honoured; the "
            "old one retired as expired",
            bool(found)
            and fresh["digest"] != lease["digest"]
            and answer["status"] == 200
            and old.get("revoke_reason") == "expired",
            f"new {fresh['id']} answer {answer['status']} old "
            f"{old.get('revoke_reason')}",
        )
        self.main_lease = fresh

    def cancel(self) -> None:
        job, lease = self.jobs["main"], self.main_lease
        self.api.ok("PUT", f"/api/jobs/{job}/cancel")
        rows = wait_for(
            "the cancelled job's leases revoked",
            lambda: (
                rows
                if (rows := self.leases(job=job))
                and all(row["revoked"] for row in rows)
                else None
            ),
            timeout=120,
            interval=3,
        )
        live_at_cancel = [row for row in rows if row["revoke_reason"] != "expired"]
        answer = self.exchange("a", lease["id"], "read")
        self.report.check(
            "cancel: every live lease of the job revoked (job_cancelled) and refused",
            live_at_cancel
            and all(row["revoke_reason"] == "job_cancelled" for row in live_at_cancel)
            and answer["body"] == {"error": "lease_revoked"},
            f"{[row['revoke_reason'] for row in rows]} answer {answer['body']}",
        )

    def delete(self) -> None:
        job = self.create_job(
            "delete",
            "Run the shell command `sleep 25` eight times, one command per tool "
            "call, then complete the job.",
        )
        lease = self.wait_live_lease("a", job=job)
        self.api.ok("PUT", f"/api/jobs/{job}/pause")
        self.wait_status(job, frozenset({"paused"}), self.args.job_timeout)
        still_live = (self.lease_row(lease["id"]) or {}).get("unexpired")

        def deleted() -> bool:
            status, _body = self.api.call("DELETE", f"/api/jobs/{job}")
            return status in (200, 204, 404)

        wait_for("the paused job deleted", deleted, timeout=240, interval=10)
        events = self.events("connector_lease_revoked", lease["id"])
        self.report.check(
            "delete: the live lease's revocation is audited (job_deleted) and the "
            "row went with the job",
            bool(still_live)
            and self.lease_row(lease["id"]) is None
            and any("reason=job_deleted" in (detail or "") for detail in events),
            f"live_at_delete={still_live} events={len(events)}",
        )

    def completion(self) -> None:
        job = self.create_job(
            "complete",
            "Write the word done to output/c2.txt, then complete the job.",
        )
        # The job may finish before a poll sees its lease live: any row will do.
        lease = wait_for(
            "a lease for the job",
            lambda: next(
                (
                    row
                    for row in self.leases(job=job)
                    if row["connector_id"] == self.connectors["a"]
                ),
                None,
            ),
            timeout=self.args.job_timeout,
            interval=3,
        )
        status = self.wait_status(job, JOB_RESTING, self.args.job_timeout)
        if status == "pending_review":
            self.api.ok("POST", f"/api/jobs/{job}/approve", {})
            status = self.wait_status(job, JOB_TERMINAL, self.args.job_timeout)
        row = self.lease_row(lease["id"]) or {}
        expected = {"completed": "job_completed", "failed": "job_failed"}.get(status)
        self.report.check(
            "completion: the finished job's lease is revoked",
            expected is not None and row.get("revoke_reason") == expected,
            f"status {status} reason {row.get('revoke_reason')}",
        )

    def session(self) -> None:
        created = self.api.ok(
            "POST",
            "/api/persistent/threads",
            {
                "title": f"C2 credential lease gate {self.gate_id}",
                "permission_mode": "autonomous",
                "project_id": self.project,
                "datasource_ids": [self.connectors["a"], self.connectors["b"]],
                "config_override": {"workspace": {"backend": "sandbox"}},
                "model": self.args.model,
            },
        )
        self.thread = thread = str(created.get("thread_id") or created["id"])
        print(f"session {thread}", flush=True)
        lane = sql(f"SELECT execution_lane FROM threads WHERE id = {lit(thread)}")
        if lane != "stateless":
            raise GateError(f"session lane is {lane!r}, not stateless")
        self.api.ok(
            "POST",
            f"/api/persistent/threads/{thread}/input",
            {"content": "Reply with the single word ready."},
        )
        a = self.wait_live_lease("a", thread=thread)
        b = self.wait_live_lease("b", thread=thread)
        selector = f"srw/thread-id={thread}"
        files = {
            label: wait_for(
                f"session lease file {label}",
                lambda label=label: self.lease_file(selector, label),
                timeout=self.args.turn_timeout,
                interval=5,
            )
            for label in ("a", "b")
        }
        self.report.check(
            "session: both leases issued and both token files written",
            files["a"]["sha256"] == a["digest"]
            and files["b"]["sha256"] == b["digest"]
            and files["a"]["file"] == files["b"]["file"] == "600",
            f"a={a['id']} b={b['id']}",
        )
        idle = self.ttl + 2 * self.sweep + 15
        print(
            f"session idle for {idle} s (one lease window and two sweeps)", flush=True
        )
        time.sleep(idle)
        rows = {label: self.live_lease(label, thread=thread) for label in ("a", "b")}
        self.report.check(
            "session: idle past its first window, both leases are still live and "
            "renewed",
            all(
                row is not None
                and row["id"] == lease["id"]
                and row["renewed"]
                and row["age"] > self.ttl
                for row, lease in ((rows["a"], a), (rows["b"], b))
            ),
            str({k: (v or {}).get("age") for k, v in rows.items()}),
        )
        self.api.ok(
            "PATCH",
            f"/api/persistent/threads/{thread}/config",
            {"datasource_ids": [self.connectors["a"]]},
        )
        detached = self.lease_row(b["id"]) or {}
        kept = self.lease_row(a["id"]) or {}
        self.report.check(
            "session: a live detach revokes only the detached connector's lease",
            detached.get("revoke_reason") == "connector_detached"
            and not kept.get("revoked")
            and kept.get("unexpired"),
            f"b {detached.get('revoke_reason')} a revoked={kept.get('revoked')}",
        )
        refused = self.exchange("b", b["id"], "read")
        self.report.check(
            "session: the detached lease is refused at the exchange",
            refused["body"] == {"error": "lease_revoked"},
            str(refused["body"]),
        )
        self.api.ok("DELETE", f"/api/persistent/threads/{thread}?force=true")
        ended = wait_for(
            "End revokes the session's lease",
            lambda: (
                row if (row := self.lease_row(a["id"])) and row["revoked"] else None
            ),
            timeout=120,
            interval=3,
        )
        self.report.check(
            "session: End revokes the session's lease",
            ended.get("revoke_reason") == "session_end",
            str(ended.get("revoke_reason")),
        )

    # -- cleanup -------------------------------------------------------------
    def titled_threads(self) -> list[str]:
        rows = sql(
            "SELECT id FROM threads WHERE "
            f"position({lit(self.gate_id)} in coalesce(title, '')) > 0"
        )
        return [row for row in rows.splitlines() if _UUID_RE.fullmatch(row)]

    def described_jobs(self) -> list[str]:
        rows = sql(
            "SELECT id FROM jobs WHERE "
            f"position({lit('[' + self.gate_id)} in coalesce(description, '')) > 0"
        )
        return [row for row in rows.splitlines() if _UUID_RE.fullmatch(row)]

    def delete_thread(self, thread: str) -> bool:
        def gone() -> bool:
            status, _body = self.api.call(
                "DELETE", f"/api/persistent/threads/{thread}?force=true&permanent=true"
            )
            if status == 404:
                return True
            return sql(f"SELECT count(*) FROM threads WHERE id = {lit(thread)}") == "0"

        try:
            wait_for("session deleted", gone, timeout=300, interval=5)
        except GateError:
            return False
        return True

    def delete_job(self, job: str) -> bool:
        self.api.call("PUT", f"/api/jobs/{job}/cancel")

        def gone() -> bool:
            status, _body = self.api.call("DELETE", f"/api/jobs/{job}")
            return status in (200, 204, 404)

        try:
            wait_for("job deleted", gone, timeout=240, interval=10)
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

        for thread in dict.fromkeys(
            [*filter(None, [self.thread]), *self.titled_threads()]
        ):
            step(
                f"delete session {thread}",
                lambda thread=thread: self.delete_thread(thread),
            )
        for job in dict.fromkeys([*self.jobs.values(), *self.described_jobs()]):
            step(f"delete job {job}", lambda job=job: self.delete_job(job))
        for label, datasource_id in list(self.connectors.items()):

            def delete(datasource_id=datasource_id) -> bool:
                status, _body = self.api.call(
                    "DELETE", f"/api/datasources/{datasource_id}"
                )
                return status in (200, 204, 404)

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
        if self.probe_started:
            step(
                "delete network probe pod",
                lambda: command(
                    K
                    + ["delete", "pod", "-l", f"{GATE_LABEL}={self.gate_id}"]
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
        described = self.described_jobs()
        if described:
            left.append(f"jobs described with the gate id: {described}")
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
        identities = sql(
            "SELECT count(*) FROM connector_driver_identities WHERE "
            f"position({lit(self.gate_id)} in coalesce(pod_name, '')) > 0"
        )
        if identities != "0":
            left.append(f"{identities} driver identities")
        if (
            self.project
            and sql(f"SELECT count(*) FROM projects WHERE id = {lit(self.project)}")
            != "0"
        ):
            left.append(f"project {self.project}")
        selectors = [
            f"{GATE_LABEL}={self.gate_id}",
            *(f"srw/job-id={job}" for job in self.jobs.values()),
            *(f"srw/thread-id={thread}" for thread in filter(None, [self.thread])),
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
        return left

    # -- run -------------------------------------------------------------------
    def run(self) -> int:
        try:
            # Everything after needs the deployment and the fixtures.
            self.preflight()
            self.fixture()
            # Independent groups: a failure is recorded and the next group
            # still runs, so one broken hop does not hide the others.
            for group in (
                (self.port_checks,),
                (self.job_checks, self.refusals, self.pause_and_resume, self.cancel),
                (self.delete,),
                (self.completion,),
                (self.session,),
            ):
                for phase in group:
                    try:
                        phase()
                    except GateError as exc:
                        self.report.check(
                            f"{phase.__name__}: infrastructure", False, str(exc)
                        )
                        break
        except GateError as exc:
            self.report.check("gate infrastructure", False, str(exc))
        finally:
            if self.args.keep:
                print(
                    "kept: "
                    + json.dumps(
                        {
                            "connectors": self.connectors,
                            "jobs": self.jobs,
                            "thread": self.thread,
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
    parser.add_argument(
        "--max-ttl",
        type=int,
        default=300,
        help="refuse a deployment whose lease TTL is longer (seconds)",
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
        raise SafetyError("--gate-id must be c2- followed by 10 hex digits")
    if not _MODEL_RE.fullmatch(args.model):
        raise SafetyError("model id is malformed")
    if not re.fullmatch(r"[a-z][a-z0-9._-]{0,63}", args.user):
        raise SafetyError("user name is malformed")
    if not 60 <= args.turn_timeout <= 1800:
        raise SafetyError("--turn-timeout must be between 60 and 1800 seconds")
    if not 60 <= args.job_timeout <= 3600:
        raise SafetyError("--job-timeout must be between 60 and 3600 seconds")
    if not 60 <= args.max_ttl <= 900:
        raise SafetyError("--max-ttl must be between 60 and 900 seconds")


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
    return CredentialLeaseGate(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
