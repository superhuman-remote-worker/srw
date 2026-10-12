#!/usr/bin/env python3
"""WP5 live gate: session subagent fan-out on the local Tilt/k3d release.

Design: knowledge-base/knowledge/features/parallel_subagents.md §11 (live gate
K1-K4), §8 (failure matrix), §12 (what to watch) and §13 (family caps).
Template: scripts/stateless-resilience-k3d-gate.py.

Scenarios (each selectable by flag, run one at a time, in this order):

  --k1  Supervised session; the parent issues N delegate_agent calls; the
        executor holding the lease is killed while the approval cards are
        pending. The successor's replayed cards are declined by the script.
        Pass: one final answer, zero child rows and zero child provider calls,
        no parked unit.
  --k2  N=4, cap 2, slow briefs (each child runs `sleep 45`); the lease holder
        is killed while two child rows are running (default: the second wave,
        two rows already ended). Pass: one continuation, one final answer,
        every child of the batch ended, one result per call, the parent's
        provider called once after recovery (audit) with each report exactly
        once in that call's provider input, no child call after the kill.
  --k3  N=4, all concurrent; killed during the parent's final streaming call,
        after the four results are durable. Pass: no continuation, one final
        answer, no further child calls, the four results untouched.
  --k4  K2, then the successor is killed during the recovery turn. Pass: as
        K2, and never a second continuation for the original input.
  --measure  4, 8 and 20 concurrent children (session cap = N): executor pod
        memory (cgroup samples plus memory.peak), batch duration, provider 429
        count (from the executor logs; the audit store keeps no failed main or
        child call), child provider calls and tokens.
  --preflight  Only the read-only precondition checks. Creates nothing.

"Kill" is `kubectl delete pod <leased_by> --wait=false`. A plain delete makes
the stateless executor DRAIN: it finishes the turn within
STATELESS_SHUTDOWN_TIMEOUT_S (240 s on k3d), which would complete a K2 batch on
the old pod and never reach recovery. The default `--kill hard` therefore adds
`--grace-period=1`: SIGTERM, then SIGKILL one second later, the crash §8 is
about. `--kill drain` keeps the plain delete (K1 and K2 only). K2's sleep is
then stretched past the drain budget, so the batch is still running when the
budget ends; since WP3d the draining executor releases the claim with the
children left `running` and the input unconsumed, and the successor settles
exactly as after a crash (the classes follow the children's state at the
release, and no result may be STOPPED). No pkill anywhere: a `pkill -f`
pattern can match this script's own shell (WP0b).

Preconditions, checked before every scenario and re-checked after it:
k3d-srw context; no other gate, VM gate or full pytest run on this host (a
/proc scan that skips this process's own ancestry); no running VMIs; idle run
queue and no live child rows; the feature files have no uncommitted change and
every executor pod and the orchestrator serve exactly the HEAD bytes (a green
Tilt resource can still run an old image); the operator fan-out switch is on
(see OPERATOR SWITCH below); the chosen expert's roster child has a shell tool
a MiniMax-M3 parent can bind. A warm-up turn then asks the model to quote its
delegate_agent description, which proves the switch and the capability reached
that session and that the cap override applied.

Operating rules (parallel_subagents.md §10.3, §11): never beside the full test
suite or the VM gate (the k3d node ran out of memory before); one scenario at
a time (a host-wide lock); never while source changes (the dirty check and the
post-scenario hash re-check mark such a run INVALID).

Evidence per scenario in the output directory: transcript, deliveries, child
rows and child transcripts, events, permission rows, run-queue row, audit
requests (with the provider input of every main call), executor logs, the
timeline, facts (thread id, killed pods, lease tokens) and the verdict. The
test sessions are ended afterwards (`DELETE /api/persistent/threads/{id}
?force=true`, not permanent) unless --keep-sessions.

Pinned lane (P6, parallel_subagents.md §14.2), `--lane pinned`:

  The session is an Officer conference (`officer.conference: true`, one per
  project, owned by the user) with a sandbox workspace: pinned by rule, its
  own agent pod (label srw.io/thread-id), its inputs pinned deliveries. The
  parent is HEAD's `--expert` sent as an inline Expert on `--model` that
  authors `llm.parallel_tool_calls: true` (muse-spark-1.3 is false in the
  matrix, so the parent would never fan out; see pinned_expert_runtime).

  --pk0  Live preflight: lane, image (P3 code), attach advertisement
         fanout=True, the offer and cap, a 2-call batch whose children overlap.
  --pk1  4 calls, cap 2: waves, one answer, one tool row per call; measures
         the batch and the pod's memory.
  --pk2  Kill when one child ended, two sleep and one is queued. `kill -9` of
         the agent process from the k3d node (default; no SIGTERM, no preStop,
         like an OOM kill) or `--pinned-kill force-delete`. The stale-agent
         detector retires the runtime (P0: no refused receipt; P0b: children
         interrupted:parent_restart, cause runtime_lost); no parent result
         before Resume (P2); Resume; the successor settles one result per
         call, one continuation, the source settled, one answer.
  --pk3  As PK2 with a plain `kubectl delete pod` (180 s grace): the batch is
         handed over (P4 log, no parent result), the session ends (or
         suspends: recorded), and Resume settles as in PK2.
  --pk4  PK2, then the successor is killed while the continuation's answer
         streams; the next runtime serves the admitted continuation exactly
         once (P3: log, admission_count 2), one answer.
  --pk5  The switch turned off under a live pinned session: the gate edits
         only `sessionSubagentFanoutLanes` in deployment/values-local.yaml,
         waits for the orchestrator restart and a heartbeat (>=60 s), then a
         2-call request is refused (both calls, no child); the pod logs the
         re-applied advertisement. Restores the value and checks fanout=True.
  --pk6  A person's End mid-batch cancels the children (D4); Resume settles
         nothing.

  A viewer is held by default: one cockpit-like WebSocket subscriber on the
  session's current pod (class Viewer). Headless (`--no-viewer`), a pinned
  session reads awaiting_user mid-turn, the detector then settles a lost
  runtime `suspended` and the gate revives it with `prepare` instead of
  Resume (seen 2026-10-07: that path wedged, see the P6 record). An End that
  retries "subagent quiesce cannot prove exact settlement authority" is
  recorded and the session's pod deleted so the retirement can settle.

Uses the disposable k3d `test` account; set SRW_K3D_TEST_PASSWORD if its
password was changed. The password travels over stdin into the orchestrator
pod, which mints the token and makes the request; no credential is placed on
a command line or printed.
"""

from __future__ import annotations

import argparse
from collections import Counter
import copy
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
import threading
import time
import uuid

import yaml

ROOT = Path(__file__).resolve().parents[1]
K = ["kubectl", "--context=k3d-srw", "-n", "srw"]
EXECUTOR_DEPLOYMENT = "srw-agent-stateless"
EXECUTOR_SELECTOR = "srw/class=agent-stateless"
EXECUTOR_CONTAINER = "agent"
ORCHESTRATOR_DEPLOYMENT = "srw-orchestrator"
ORCHESTRATOR_CONTAINER = "orchestrator"
KEYCLOAK_TOKEN_URL = "http://srw-keycloak:8080/realms/srw/protocol/openid-connect/token"
POD_ROOT = "/app"
CGROUP = "/sys/fs/cgroup"

# ---------------------------------------------------------------------------
# OPERATOR SWITCH (WP3c, commit 7d25b7cbf). Precondition; adjust HERE only.
#   On by default in the chart since WP6 (helm/values.yaml); on k3d make sure
#   deployment/values-local.yaml does not turn it off:
#       orchestrator:
#         sessionSubagentFanoutLanes: "stateless"
#   Tilt re-applies the chart on that edit (never `helm upgrade` under Tilt);
#   the orchestrator then carries the env below and advertises
#   FANOUT_CLAIM_KEY: true at every stateless claim.
# ---------------------------------------------------------------------------
FANOUT_SWITCH_ENV = "SESSION_SUBAGENT_FANOUT_LANES"
FANOUT_SWITCH_LANE = "stateless"
FANOUT_SWITCH_VALUES_KEY = "orchestrator.sessionSubagentFanoutLanes"
FANOUT_CLAIM_KEY = "session_subagent_fanout"
SETTLE_CONTRACT_KEY = "session_subagent_batch_settle_contract"
# The committed module that must define both advertised keys.
ADVERTISEMENT_MODULE = "src/shared/session_subagent_batch.py"

# Model-facing texts the gate recognises (sources in comments).
# agent.tools.delegation.delegate_agent: the refusal the live path drops (WP3b).
FANOUT_REFUSAL = "sessions may delegate only one child per parent turn"
# delegate_agent.build_description: a session that may NOT fan out.
SINGLE_CHILD_SENTENCE = "One subagent at a time"
# shared.session_subagent_batch.batch_continuation_text.
CONTINUATION_MARKER = "[subagent recovery]"
# agent.subagents.runtime.STOPPED_HEADER: a person's Stop (WP3b). Neither kill
# may produce it; WP3d made a graceful shutdown leave the batch to a successor.
STOPPED_MARKER = "[delegate_agent: STOPPED"
# turn_executor (WP3d): the draining executor released the claim.
RELEASE_LOG = "for a successor to settle"
RECOVERY_METRICS_KEY = "subagent_recovery"
# The live loop's text for a declined call (persistent_graph).
DECLINED_TEXT = "User declined this tool call."
SHELL_TOOLS = ("shell_execute", "run_command")

# Files whose served bytes must equal HEAD. Executors run the agent side; the
# orchestrator owns the settle, the claim bundle and config resolution.
AGENT_FILES = [
    "src/agent/tools/delegation/delegate_agent.py",
    "src/agent/tools/delegation/fanout.py",
    "src/agent/tools/context.py",
    "src/agent/subagents/batch_recovery.py",
    "src/agent/subagents/runtime.py",
    "src/agent/subagents/limiter.py",
    "src/agent/subagents/envelope.py",
    "src/agent/subagents/child.py",
    "src/agent/subagents/driver.py",
    "src/agent/subagents/session_persistence.py",
    "src/agent/persistent_graph.py",
    "src/agent/api/turn_executor.py",
    "src/agent/api/persistent_app.py",
    "src/agent/api/persistent_session.py",
    "src/agent/api/orchestrator_client.py",
    "src/agent/api/session_input.py",
    "src/agent/core/context.py",
    "src/agent/core/archiver.py",
    "src/agent/database/postgres_db.py",
    "src/shared/session_subagent_batch.py",
    "src/shared/persistent_input_delivery.py",
    "src/shared/session_retirement.py",
    "src/shared/runtime/core/delegation_settings.py",
    "src/shared/runtime/core/loader.py",
]
ORCHESTRATOR_FILES = [
    "src/orchestrator/database/session_subagent_recovery.py",
    "src/orchestrator/database/postgres.py",
    "src/orchestrator/services/agent_child_threads.py",
    "src/orchestrator/routers/agent_child_threads.py",
    "src/orchestrator/schemas/agent_child_threads.py",
    "src/orchestrator/services/unit_claim_bundle.py",
    "src/orchestrator/services/session_attach_binding.py",
    "src/orchestrator/services/session_attach_payload.py",
    "src/orchestrator/services/session_create_overrides.py",
    "src/orchestrator/application/settings.py",
    "src/orchestrator/application/sessions.py",
    "src/orchestrator/application/preparation.py",
    "src/orchestrator/services/thread_workspace_delivery.py",
    "src/orchestrator/services/session_state_snapshot.py",
    "src/orchestrator/services/run_queue_reaper.py",
    "src/shared/persistent_input_delivery.py",
    "src/shared/session_retirement.py",
    "src/shared/session_subagent_batch.py",
    "src/shared/thread_rewind.py",
    "src/shared/runtime/core/delegation_settings.py",
    "src/shared/runtime/core/loader.py",
]
CONFIG_FILES = [
    "config/model_config_matrix.yaml",
    "config/expert_base.yaml",
    "config/overlays/session.yaml",
    "config/overlays/subagent.yaml",
]

# Processes that must not run beside a live gate (/proc scan, own ancestry
# excluded).
OTHER_GATES = (
    "parallel-subagents-k3d-gate",
    "stateless-resilience-k3d-gate",
    "stateless-scale-k3d-gate",
    "stateless-stale-writer-k3d-gate",
    "stateless-durable-wake-k3d-gate",
    "vm-workspace-recovery-k3d-gate",
    "vm-retained-resume-gate",
    "local-kubevirt-up",
)
LOCK_PATH = Path(tempfile.gettempdir()) / "srw-parallel-subagents-k3d-gate.lock"
SCENARIOS = ("k1", "k2", "k3", "k4", "measure")
_SECRETS: list[str] = []

# ---------------------------------------------------------------------------
# PINNED LANE (P6, parallel_subagents.md §14.2). `--lane pinned` selects it.
#   The switch must name `pinned` too:
#       orchestrator:
#         sessionSubagentFanoutLanes: "stateless,pinned"
#   PK5 edits that one key of the gitignored values file itself (and restores
#   it); Tilt re-applies the chart and the orchestrator restarts.
# ---------------------------------------------------------------------------
PINNED_SCENARIOS = (
    "pk0",
    "pk1",
    "pk2",
    "pk2e",
    "pk3",
    "pk4",
    "pk5",
    "pk6",
    "pk6b",
    "pk6c",
)
PINNED_LANE = "pinned"
PINNED_CONTAINER = "agent"
PINNED_THREAD_LABEL = "srw.io/thread-id"
ORCHESTRATOR_SELECTOR = (
    "app.kubernetes.io/component=orchestrator,app.kubernetes.io/instance=srw"
)
# The k3d node container; `kill -9` of the agent process from here is the
# crash (an OOM kill), with no SIGTERM and no preStop (see PinnedRun.kill).
K3D_NODE = "k3d-srw-server-0"
VALUES_LOCAL = ROOT / "deployment" / "values-local.yaml"
_VALUES_LANES = re.compile(r'^(\s*sessionSubagentFanoutLanes:\s*)"([^"]*)"\s*$', re.M)
# Code a served pinned image must contain: P3's re-serve and the person-End
# fix (54ac1ea99..0f4b59aa9).
PINNED_IMAGE_MARKERS = (
    ("src/shared/persistent_input_delivery.py", "reserve_stale_pinned_admissions"),
    ("src/agent/api/session_termination.py", "retirement_authorized_now"),
)
PINNED_AGENT_FILES = AGENT_FILES + [
    "src/agent/subagents/host.py",
    "src/agent/api/session_attach.py",
    "src/agent/api/session_termination.py",
]
PINNED_ORCHESTRATOR_FILES = ORCHESTRATOR_FILES + [
    "src/orchestrator/services/stale_agent_detector.py",
    "src/orchestrator/services/thread_retirement.py",
    "src/orchestrator/services/pinned_retirement.py",
    "src/orchestrator/services/agent_registration.py",
    "src/orchestrator/services/thread_resume.py",
]
# Agent and orchestrator log lines the pinned scenarios read (sources).
# session_attach: the attach advertisement and the heartbeat re-apply (P5).
ADVERTISEMENT_RE = re.compile(
    r"Session delegation advertisement: thread=(?P<thread>\S+) "
    r"batch_settle=(?P<settle>\w+) fanout=(?P<fanout>\w+) source=(?P<source>\w+)"
)
REAPPLIED_RE = re.compile(
    r"Session delegation advertisement re-applied: thread=(?P<thread>\S+) "
    r"batch_settle=(?P<settle>\w+) fanout=(?P<fanout>\w+)"
)
# batch_recovery: the successor's batch settle at attach.
BATCH_SETTLED_LOG = "settled as one batch"
# subagents.runtime (P4): a child ended for the successor at the fence.
HANDOVER_LOG = "for the successor (parent shutting down)"
# session attach (P3): stale admissions settled / served again / parked.
STALE_ADMISSION_RE = re.compile(
    r"Stale pinned admissions for thread (?P<thread>\S+): (?P<counts>[^\n]*?)"
    r"(?P<served>\d+) served again, (?P<parked>\d+) parked"
)
# pinned_retirement (P0): the exit receipt refused; must never appear.
RECEIPT_REFUSED_LOG = "Pinned retirement receipt refused"
# subagents.runtime.quiesce: End cannot settle the runtime's children (seen
# after a recovery: the recovered handles are never marked settled).
QUIESCE_WEDGE_LOG = "subagent quiesce cannot prove exact settlement authority"
# Raw errors a stopping runtime once wrote as a batch's results (PK6, run 2).
END_ERROR_TEXTS = ("pinned_parent_not_current", "quiescing; child was not started")
# stale_agent_detector step 3: the detector retired an offline runtime.
DETECTOR_RETIRED_LOG = "offline pinned runtime(s) through exact End"
PARENT_RESTART_OUTCOME = "interrupted:parent_restart"
PARENT_RESTART_NOT_STARTED_OUTCOME = "interrupted:not_started"
PERSON_END_OUTCOME = "cancelled:parent_retired"


class GateError(RuntimeError):
    """An infrastructure command failed."""


class GateFailure(AssertionError):
    """A scenario cannot pass (fail fast)."""


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def _scrub(text: str) -> str:
    for secret in _SECRETS:
        if secret:
            text = text.replace(secret, "<redacted>")
    return text


def command(args, *, data=None, timeout=180):
    """Run argv; never echo argv (the template's rule), only a short label."""
    label = " ".join(args[4:8]) if args[:1] == ["kubectl"] else " ".join(args[:3])
    try:
        result = subprocess.run(
            args, input=data, text=True, capture_output=True, timeout=timeout
        )
    except subprocess.TimeoutExpired:
        raise GateError(f"{label[:80]} timed out after {timeout}s") from None
    if result.returncode:
        tail = _scrub(result.stderr.strip()[-500:])
        raise GateError(f"{label[:80]} failed (exit {result.returncode}): {tail}")
    return result.stdout.strip()


def _psql(pod, database, query):
    return command(
        K
        + ["exec", pod, "--", "psql", "-U", "srw", "-d", database]
        + ["-v", "ON_ERROR_STOP=1", "-tAc", query]
    )


def sql(query):
    return _psql("srw-postgres-0", "srw", query)


def audit_sql(query):
    return _psql("srw-auditdb-0", "srw_audit", query)


def rows(query, *, audit=False):
    wrapped = f"SELECT coalesce(json_agg(q), '[]'::json) FROM ({query}) q"
    raw = (audit_sql if audit else sql)(wrapped)
    return json.loads(raw) if raw else []


def one(query):
    raw = sql(f"SELECT row_to_json(q) FROM ({query}) q")
    return json.loads(raw) if raw else None


def uid(value) -> str:
    return str(uuid.UUID(str(value)))


def lit(text: str) -> str:
    return "'" + str(text).replace("'", "''") + "'"


def in_list(values) -> str:
    return "(" + ", ".join(lit(v) for v in values) + ")" if values else "(NULL)"


def db_now() -> str:
    return sql("SELECT clock_timestamp()")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def rfc3339_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def wait_for(label, probe, *, timeout, interval=2.0):
    """Poll ``probe`` until truthy. A probe raises GateFailure to fail fast;
    five consecutive infrastructure errors abort the wait."""
    deadline = time.monotonic() + timeout
    errors = 0
    while True:
        try:
            value = probe()
            errors = 0
        except GateError as exc:
            errors += 1
            if errors >= 5:
                raise
            print(f"     retrying after: {exc}", flush=True)
            value = None
        if value:
            print(f"PASS {label}", flush=True)
            return value
        if time.monotonic() >= deadline:
            raise GateFailure(f"timed out after {timeout:.0f}s: {label}")
        time.sleep(interval)


# ---------------------------------------------------------------------------
# REST (inside the orchestrator pod: the password arrives on stdin, the token
# never leaves the pod)
# ---------------------------------------------------------------------------

_API_PROGRAM = r"""
import json, sys, urllib.error, urllib.parse, urllib.request
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


class Api:
    def __init__(self):
        self.username = os.environ.get("SRW_K3D_TEST_USER", "test")
        self.password = os.environ.get("SRW_K3D_TEST_PASSWORD", "srw-k3d-dev-test")
        _SECRETS.append(self.password)

    def call(self, method, path, body=None, *, ok=(200, 201, 202, 204)):
        envelope = {
            "username": self.username,
            "password": self.password,
            "token_url": KEYCLOAK_TOKEN_URL,
            "method": method,
            "path": path,
            "body": body,
        }
        out = command(
            K
            + ["exec", "-i", f"deploy/{ORCHESTRATOR_DEPLOYMENT}"]
            + ["-c", ORCHESTRATOR_CONTAINER, "--", "python", "-c", _API_PROGRAM],
            data=json.dumps(envelope) + "\n",
        )
        result = json.loads(out.splitlines()[-1])
        text = _scrub(result.get("body") or "")
        if result["status"] not in ok:
            raise GateError(f"{method} {path} -> HTTP {result['status']}: {text[:400]}")
        try:
            return json.loads(text) if text.strip() else {}
        except json.JSONDecodeError:
            return {"raw": text[:400]}


# ---------------------------------------------------------------------------
# Cluster facts
# ---------------------------------------------------------------------------


def executor_pods(*, running_only=True, selector=EXECUTOR_SELECTOR):
    """Pods matching ``selector`` (default: the stateless executor pool; the
    pinned mode passes a session's ``srw.io/thread-id`` label)."""
    data = json.loads(command(K + ["get", "pods", "-l", selector, "-o", "json"]))
    pods = []
    for item in data["items"]:
        status = item.get("status") or {}
        terminating = bool(item["metadata"].get("deletionTimestamp"))
        if running_only and (status.get("phase") != "Running" or terminating):
            continue
        container = (status.get("containerStatuses") or [{}])[0]
        pods.append(
            {
                "name": item["metadata"]["name"],
                "phase": status.get("phase"),
                "terminating": terminating,
                "image": item["spec"]["containers"][0]["image"],
                "template_hash": item["metadata"]
                .get("labels", {})
                .get("pod-template-hash"),
                "restarts": container.get("restartCount", 0),
                "last_termination": (container.get("lastState") or {})
                .get("terminated", {})
                .get("reason"),
            }
        )
    return pods


def deployment(name):
    return json.loads(command(K + ["get", "deploy", name, "-o", "json"]))


def rollout_converged(dep):
    spec, status = dep["spec"], dep.get("status") or {}
    return (
        status.get("observedGeneration", 0) >= dep["metadata"]["generation"]
        and status.get("updatedReplicas") == spec["replicas"]
        and status.get("readyReplicas") == spec["replicas"]
        and status.get("replicas") == spec["replicas"]
    )


def container_env(dep, name):
    for entry in dep["spec"]["template"]["spec"]["containers"][0].get("env") or []:
        if entry["name"] == name:
            return entry.get("value")
    return None


def drain_budget_seconds() -> float:
    raw = container_env(deployment(EXECUTOR_DEPLOYMENT), "STATELESS_SHUTDOWN_TIMEOUT_S")
    return float(raw) if raw else 120.0


_HASH_PROGRAM = r"""
import hashlib, json, sys
expected = json.loads(sys.stdin.read())
bad = {}
for path, digest in expected.items():
    try:
        with open(sys.argv[1] + "/" + path, "rb") as handle:
            actual = hashlib.sha256(handle.read()).hexdigest()
    except OSError as error:
        actual = "missing:" + type(error).__name__
    if actual != digest:
        bad[path] = actual[:16]
print(json.dumps(bad))
"""


_DNS_PROGRAM = r"""
import socket
try:
    socket.getaddrinfo("openrouter.ai", 443)
    print("ok")
except OSError as error:
    print("fail: " + str(error))
"""

_HEAD_CACHE: dict[tuple[str, str], bytes] = {}


def head_sha() -> str:
    return subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"], capture_output=True, text=True
    ).stdout.strip()


def head_bytes(path: str) -> bytes:
    """The committed bytes of ``path`` (read-only, one path at a time)."""
    key = (head_sha(), path)
    if key not in _HEAD_CACHE:
        result = subprocess.run(
            ["git", "-C", str(ROOT), "show", f"HEAD:{path}"], capture_output=True
        )
        if result.returncode:
            raise GateError(f"{path} is not in HEAD")
        _HEAD_CACHE[key] = result.stdout
    return _HEAD_CACHE[key]


def dirty(paths) -> list[str]:
    # Path-limited and read-only: never a whole-tree git command.
    out = subprocess.run(
        ["git", "-C", str(ROOT), "diff", "--name-only", "HEAD", "--", *paths],
        capture_output=True,
        text=True,
    )
    if out.returncode:
        raise GateError("git diff failed")
    return [line for line in out.stdout.splitlines() if line]


def served_mismatches(target: str, container: str, paths) -> dict:
    expected = {p: hashlib.sha256(head_bytes(p)).hexdigest() for p in paths}
    out = command(
        K
        + ["exec", "-i", target, "-c", container, "--"]
        + ["python", "-c", _HASH_PROGRAM, POD_ROOT],
        data=json.dumps(expected),
    )
    return json.loads(out.splitlines()[-1])


def roster_entry_paths(expert: str, child: str):
    """(expert file, child file, problem) from the committed bundled configs."""
    expert_path = f"config/experts/{expert}/config.yaml"
    try:
        manifest = yaml.safe_load(head_bytes(expert_path))
        config = manifest["spec"]["runtime"]["config"]["config"]
    except GateError:
        return None, None, f"{expert_path} not in HEAD (a DB expert is not checked)"
    except (KeyError, TypeError, yaml.YAMLError) as exc:
        return expert_path, None, f"{expert_path} unreadable: {exc!r}"
    entry = ((config.get("subagents") or {}).get("roster") or {}).get(child)
    if not isinstance(entry, dict):
        return expert_path, None, f"roster of {expert} has no {child!r}"
    if not (config.get("delegation") or {}).get("enabled"):
        return expert_path, None, f"{expert} does not set delegation.enabled"
    if "delegate_agent" not in ((config.get("tools") or {}).get("delegation") or []):
        return expert_path, None, f"{expert} does not name delegate_agent"
    ref = str(entry.get("$ref") or "")
    name = ref.split("/", 1)[1] if ref.startswith("subagents/") else ref
    child_path = f"config/subagents/{name}/config.yaml"
    try:
        child_manifest = yaml.safe_load(head_bytes(child_path))
        child_config = child_manifest["spec"]["runtime"]["config"]["config"]
    except GateError:
        return expert_path, None, f"{child!r} is not a library $ref ({ref!r})"
    except (KeyError, TypeError, yaml.YAMLError) as exc:
        return expert_path, child_path, f"{child_path} unreadable: {exc!r}"
    # A sibling ``tools.shell`` on the roster entry replaces the library list
    # (deep merge replaces lists), e.g. the assistant's ``shell: []``.
    entry_shell = (entry.get("tools") or {}).get("shell")
    shells = set(
        entry_shell
        if entry_shell is not None
        else ((child_config.get("tools") or {}).get("shell") or [])
    )
    if "shell_execute" not in shells:
        # MiniMax-M3 runs shell_mode persistent: the parent binds shell_execute
        # only, and a child keeps entry ∩ parent names (subagents/child.py), so
        # a child that lists only run_command gets no command tool at all.
        return expert_path, child_path, f"{child!r} lists no shell_execute"
    return expert_path, child_path, None


def _tokens(argv, depth=3) -> list[str]:
    """argv with quoted command strings split too (``bash -c '...'`` and the
    ``eval '...'`` wrappers of agent shells nest the real command)."""
    out = []
    for token in argv:
        out.append(token)
        if depth and any(ch.isspace() for ch in token):
            try:
                inner = shlex.split(token)
            except ValueError:
                inner = token.split()
            out.extend(_tokens(inner, depth - 1))
    return out


def other_runs() -> tuple[list[str], list[str]]:
    """(blocking, advisory) processes on this host, from a /proc scan.

    Blocking: another live gate, the VM gate, or a whole-suite pytest run
    (no path, a bare ``tests`` directory, or pytest-fast.sh). Advisory: a
    parallel (xdist) run of named files. Skipped: this process, its
    ancestors, and forked copies of an ancestor (a pipeline sibling of our own
    shell carries the shell's command line until it execs; WP0b's pkill -f
    matched exactly that)."""
    mine, own_cmdlines, pid = set(), set(), os.getpid()
    while pid > 1 and pid not in mine:
        mine.add(pid)
        try:
            own_cmdlines.add(Path(f"/proc/{pid}/cmdline").read_bytes())
            stat = Path(f"/proc/{pid}/stat").read_text()
        except OSError:
            break
        pid = int(stat.rsplit(")", 1)[1].split()[1])
    blocking, advisory = [], []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit() or int(entry.name) in mine:
            continue
        try:
            raw = (entry / "cmdline").read_bytes()
        except OSError:
            continue
        if raw in own_cmdlines:
            continue
        argv = [part.decode("utf-8", "replace") for part in raw.split(b"\0") if part]
        if not argv:
            continue
        # Only interpreters and shells run a gate or a suite; an editor or a
        # pager that merely has the file open does not count.
        program = argv[0].rsplit("/", 1)[-1]
        if not (
            program.startswith(("python", "bash", "sh", "zsh", "dash", "uv", "pytest"))
            or program.endswith((".py", ".sh"))
        ):
            continue
        tokens = _tokens(argv)
        gate = next(
            (
                name
                for name in OTHER_GATES
                for token in tokens
                if token.rsplit("/", 1)[-1] in (f"{name}.py", f"{name}.sh")
            ),
            None,
        )
        if gate:
            blocking.append(f"pid {entry.name}: {gate}")
            continue
        if any(t.endswith("pytest-fast.sh") for t in tokens):
            blocking.append(f"pid {entry.name}: pytest-fast.sh (whole suite)")
            continue
        starts = [i for i, t in enumerate(tokens) if t.rsplit("/", 1)[-1] == "pytest"]
        for start in starts:
            rest = []
            for token in tokens[start + 1 :]:
                if token in ("&&", "||", ";", "|") or any(c.isspace() for c in token):
                    break
                rest.append(token)
            # Test targets only; option values (``-p no:cacheprovider``,
            # ``-n 4``) are not paths.
            paths = [
                t
                for t in rest
                if not t.startswith("-")
                and ("/" in t or t.endswith(".py") or "::" in t or t in ("tests", "."))
            ]
            whole = ("tests", "./tests", ".", "")
            if not paths or any(t.rstrip("/") in whole for t in paths):
                blocking.append(f"pid {entry.name}: a whole-suite pytest run")
                break
            if any(t == "-n" or t.startswith(("-n", "--numprocesses")) for t in rest):
                advisory.append(f"pid {entry.name}: parallel pytest on {paths[:3]}")
                break
    return blocking, advisory


def acquire_lock():
    handle = open(LOCK_PATH, "a+")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return None
    handle.seek(0)
    handle.truncate()
    handle.write(f"{os.getpid()} {utc_now()}\n")
    handle.flush()
    return handle


# ---------------------------------------------------------------------------
# Verdict, logs, memory
# ---------------------------------------------------------------------------


def _short(value, limit=300):
    text = value if isinstance(value, str) else json.dumps(value, default=str)
    return text if len(text) <= limit else text[: limit - 1] + "…"


class Verdict:
    def __init__(self, name):
        self.name = name
        self.items = []

    def check(self, label, ok, detail=None):
        self.items.append({"check": label, "ok": bool(ok), "detail": detail})
        tail = f" — {_short(detail)}" if detail is not None else ""
        print(f"{'PASS' if ok else 'FAIL'} {self.name}: {label}{tail}", flush=True)
        return bool(ok)

    def warn(self, label, detail=None):
        self.items.append({"check": label, "ok": None, "detail": detail})
        tail = f" — {_short(detail)}" if detail is not None else ""
        print(f"WARN {self.name}: {label}{tail}", flush=True)

    @property
    def ok(self):
        return bool(self.items) and all(i["ok"] is not False for i in self.items)


class LogCollector(threading.Thread):
    """Follows every executor pod's log from ``since`` (new pods included), so
    a killed pod's log survives it. On stop, every pod that still exists is
    read once more in full (``<pod>.snapshot.log``, plus ``.previous.log`` for
    a restarted container); counts prefer the snapshot, so nothing is counted
    twice."""

    def __init__(
        self, directory: Path, selector=EXECUTOR_SELECTOR, container=EXECUTOR_CONTAINER
    ):
        super().__init__(daemon=True)
        self.directory = directory
        self.since = rfc3339_now()
        self.stop_event = threading.Event()
        self.procs = {}
        # A label selector, or a callable returning one (None: nothing yet;
        # the pinned mode learns its thread id only after the create).
        self.selector = selector
        self.container = container

    def _selector(self):
        return self.selector() if callable(self.selector) else self.selector

    def _pods(self):
        selector = self._selector()
        return executor_pods(running_only=False, selector=selector) if selector else []

    def _follow(self, name):
        path = self.directory / f"{name}.followed.log"
        handle = open(path, "a")
        proc = subprocess.Popen(
            K
            + ["logs", "-f", name, "-c", self.container]
            + [f"--since-time={self.since}"],
            stdout=handle,
            stderr=subprocess.DEVNULL,
        )
        self.procs[name] = (proc, handle, path)

    def run(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        while True:
            try:
                for pod in self._pods():
                    name, current = pod["name"], self.procs.get(pod["name"])
                    if pod["phase"] != "Running":
                        continue
                    if current is None:
                        self._follow(name)
                    elif (
                        current[0].poll() is not None and not current[2].stat().st_size
                    ):
                        # It never attached (container still starting): retry.
                        current[1].close()
                        self._follow(name)
            except (GateError, OSError):
                pass
            if self.stop_event.wait(5):
                break
        for proc, handle, _path in self.procs.values():
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(5)
                except subprocess.TimeoutExpired:
                    proc.kill()
            handle.close()
        try:
            existing = {p["name"]: p for p in self._pods()}
        except GateError:
            existing = {}
        for name in set(self.procs) | set(existing):
            if name not in existing:
                continue
            base = K + ["logs", name, "-c", self.container]
            try:
                text = command(base + [f"--since-time={self.since}"], timeout=60)
                (self.directory / f"{name}.snapshot.log").write_text(text + "\n")
                if existing[name]["restarts"]:
                    text = command(base + ["--previous"], timeout=60)
                    (self.directory / f"{name}.previous.log").write_text(text + "\n")
            except GateError:
                continue

    def stop(self):
        self.stop_event.set()
        if self.ident is not None:
            self.join(120)


class MemorySampler(threading.Thread):
    """cgroup memory of every running executor pod, every ``interval`` s."""

    def __init__(
        self,
        path: Path,
        interval: float,
        selector=EXECUTOR_SELECTOR,
        container=EXECUTOR_CONTAINER,
    ):
        super().__init__(daemon=True)
        self.path = path
        self.interval = interval
        self.stop_event = threading.Event()
        self.samples = []
        self.selector = selector
        self.container = container

    def sample_once(self):
        for pod in executor_pods(selector=self.selector):
            try:
                out = command(
                    K
                    + ["exec", pod["name"], "-c", self.container, "--", "sh", "-c"]
                    + [
                        f"cat {CGROUP}/memory.current; "
                        f"cat {CGROUP}/memory.peak 2>/dev/null || echo -1; "
                        f"cat {CGROUP}/memory.events"
                    ],
                    timeout=20,
                )
            except GateError:
                continue
            lines = out.splitlines()
            events = dict(
                line.split(None, 1) for line in lines[2:] if len(line.split()) == 2
            )
            try:
                sample = {
                    "t": time.time(),
                    "at": utc_now(),
                    "pod": pod["name"],
                    "current": int(lines[0]),
                    "peak": int(lines[1]),
                    "oom_kill": int(events.get("oom_kill", 0)),
                    "restarts": pod["restarts"],
                }
            except (IndexError, ValueError):
                continue
            self.samples.append(sample)
            with open(self.path, "a") as handle:
                handle.write(json.dumps(sample) + "\n")

    def run(self):
        while True:
            try:
                self.sample_once()
            except GateError:
                pass
            if self.stop_event.wait(self.interval):
                break

    def stop(self):
        self.stop_event.set()
        if self.ident is not None:
            self.join(60)


# Provider 429s are not in the audit store (only auxiliary failures are
# archived, archiver.archive_error), so they are counted in the executor logs:
# httpx logs every response at INFO, the OpenAI SDK logs its own retries, and
# the persistent loop logs a stream retry.
_429 = re.compile(
    r"HTTP/\S+\s+429\b|Error code: 429\b|RateLimitError|Too Many Requests", re.I
)
# The OpenAI SDK logs "Retrying request in 0.41 seconds" (k3d, 2026-09-29).
_RETRY = re.compile(r"Retrying request|Transient LLM stream error")
_LOG_TS = re.compile(r"^(\d{4}-\d\d-\d\d[ T]\d\d:\d\d:\d\d)")


def count_log_lines(directory: Path, window=None) -> dict:
    """429 and retry lines, per pod from its snapshot (else its followed log).
    ``window`` is (start, finish) in epoch seconds; lines stamped outside it
    are skipped (log stamps are read as UTC)."""
    counts = {"lines_with_429": 0, "retry_lines": 0, "pods": {}}
    pods = {p.name.split(".", 1)[0] for p in directory.glob("*.log")}
    for pod in sorted(pods):
        snapshot = directory / f"{pod}.snapshot.log"
        sources = (
            [snapshot, directory / f"{pod}.previous.log"]
            if snapshot.exists()
            else [directory / f"{pod}.followed.log"]
        )
        found_429 = found_retry = 0
        for path in sources:
            if not path.exists():
                continue
            for line in path.read_text(errors="replace").splitlines():
                stamp = _LOG_TS.match(line)
                if window and stamp:
                    at = datetime.fromisoformat(stamp.group(1).replace(" ", "T"))
                    epoch = at.replace(tzinfo=timezone.utc).timestamp()
                    if not window[0] - 5 <= epoch <= window[1] + 5:
                        continue
                found_429 += bool(_429.search(line))
                found_retry += bool(_RETRY.search(line))
        counts["pods"][pod] = {"429": found_429, "retry": found_retry}
        counts["lines_with_429"] += found_429
        counts["retry_lines"] += found_retry
    return counts


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------


def child_brief(marker: str, sleep_s: int) -> str:
    return (
        "Automated infrastructure test. Run exactly one shell command and "
        f"nothing else: sleep {sleep_s} && echo {marker} . Use your shell "
        f"command tool (shell_execute or run_command) with timeout={sleep_s + 90} "
        "so that the call waits until the command finishes. Do not run any "
        "other command and do not read or write any file. When the command "
        "returns, reply with exactly the line it printed and nothing else."
    )


def short_step_brief(marker: str, steps: int, step_s: int = 4) -> str:
    """A child that takes many quick steps (one tool call and one provider
    call each) instead of one long sleep: it reaches a write boundary every
    few seconds (PK6b)."""
    commands = ", ".join(
        f"`sleep {step_s} && echo {marker}-S{i}`" for i in range(1, int(steps) + 1)
    )
    return (
        "Automated infrastructure test. Run these shell commands one at a "
        "time, each as its own separate tool call, and wait for each to "
        f"return before the next: {commands}. Use your shell command tool "
        "(shell_execute or run_command) with timeout=60. Do not combine "
        "commands, do not run any other command and do not read or write any "
        f"file. When the last one has returned, reply with exactly the line "
        f"{marker}-DONE and nothing else."
    )


def batch_prompt(
    tag, n, sleep_s, child_type, final_instruction, extra_rule="", brief_fn=None
) -> str:
    """``sleep_s`` is one value for every child, or a list with one per call
    (the pinned kills want a short first child and long siblings).
    ``brief_fn(marker, value)`` replaces the sleep brief (PK6b: step counts)."""
    brief_fn = brief_fn or child_brief
    sleeps = list(sleep_s) if isinstance(sleep_s, (list, tuple)) else [sleep_s] * n
    lines = [
        f"[{tag}] Automated infrastructure test. Follow these steps exactly.",
        "",
        f"Step 1: In ONE single response, call the delegate_agent tool exactly "
        f"{n} times in parallel: one call per brief below, all {n} calls in "
        "that same response. Do not call any other tool, do not run any "
        "command yourself, and write no text before the calls. Use exactly "
        "these arguments:",
    ]
    for index in range(1, n + 1):
        lines.append(
            f'Call {index}: subagent_type="{child_type}", '
            f'description="gate {tag} child {index}", '
            f'prompt="{brief_fn(f"{tag}-C{index}", sleeps[index - 1])}"'
        )
    lines += [
        "",
        f"Step 2: When all {n} results are back, never call delegate_agent "
        "again and call no other tool, even if a result is marked INTERRUPTED "
        f"or NOT STARTED or was declined.{extra_rule} {final_instruction}",
    ]
    return "\n".join(lines)


def short_final(done: str) -> str:
    return (
        "Then reply with one line per call: the call number and the first line "
        f"of its result. End with a last line {done}."
    )


def long_final(done: str) -> str:
    # A long answer keeps the final provider call streaming long enough to be
    # killed in flight (K3, K4).
    return (
        "Then write your final answer: one line per call with the call number "
        "and the first line of its result, then a numbered list of 150 short "
        "lines, each naming a different animal or plant of the sea shore with "
        f"five words about it. End with a last line {done}."
    )


def warm_up_prompt(tag) -> str:
    return (
        f"[{tag}] Automated infrastructure test, warm-up. Do not call any tool. "
        "Copy verbatim the sentences of your delegate_agent tool description "
        "that say how many delegate_agent calls you may send per response and "
        "how many subagents run at once. If you have no delegate_agent tool, "
        "reply exactly NO-DELEGATE-TOOL."
    )


# ---------------------------------------------------------------------------
# One scenario run: one session, its evidence and verdict
# ---------------------------------------------------------------------------


class Run:
    def __init__(self, gate, name):
        self.gate = gate
        self.name = name
        self.nonce = uuid.uuid4().hex[:6]
        self.dir = gate.out / name
        self.dir.mkdir(parents=True, exist_ok=True)
        self.verdict = Verdict(name)
        self.timeline = []
        self.facts = {"scenario": name, "nonce": self.nonce, "kills": []}
        self.thread = None
        self.logs = LogCollector(self.dir / "logs")

    # -- bookkeeping --------------------------------------------------------
    def tag(self, step):
        return f"{self.name.upper()}-{self.nonce}-{step}"

    def mark(self, label, /, **data):
        entry = {"at": utc_now(), "label": label, **data}
        self.timeline.append(entry)
        extra = f" {_short(data, 200)}" if data else ""
        print(f"---- {self.name}: {label}{extra}", flush=True)

    def dump(self, name, value):
        (self.dir / name).write_text(json.dumps(value, indent=1, default=str))

    # -- session -----------------------------------------------------------
    def create_session(self, *, permission_mode, cap=None):
        delegation = {"enabled": True}
        if cap is not None:
            delegation["session_max_concurrent"] = int(cap)
        body = {
            "title": f"WP5 gate {self.name} {self.nonce}",
            "config_name": self.gate.args.expert,
            "permission_mode": permission_mode,
            "config_override": {
                "workspace": {"backend": "sandbox"},
                "delegation": delegation,
            },
        }
        result = self.gate.api.call("POST", "/api/persistent/threads", body)
        self.thread = uid(result.get("thread_id") or result["id"])
        self.gate.record_thread(self.name, self.thread)
        self.facts.update(thread=self.thread, permission_mode=permission_mode, cap=cap)
        self.mark("session created", thread=self.thread)
        if result.get("ignored_config_keys"):
            raise GateFailure(
                f"create ignored config keys {result['ignored_config_keys']}"
            )
        lane = sql(f"SELECT execution_lane FROM threads WHERE id='{self.thread}'")
        if lane != "stateless":
            raise GateFailure(f"session lane is {lane!r}, not stateless")

    def send(self, text, step) -> int:
        self.gate.api.call(
            "POST", f"/api/persistent/threads/{self.thread}/input", {"content": text}
        )
        marker = self.tag(step)
        seq = wait_for(
            f"{self.name}: input {step} persisted",
            lambda: sql(
                f"SELECT seq FROM thread_messages WHERE thread_id='{self.thread}' "
                f"AND role='human' AND position({lit(marker)} in content) > 0 "
                "ORDER BY seq LIMIT 1"
            ),
            timeout=60,
            interval=1,
        )
        self.mark(f"input {step} sent", seq=int(seq))
        return int(seq)

    def end(self):
        """End the test session (not permanent: the rows stay as evidence)."""
        if self.thread is None:
            return
        try:
            self.gate.api.call(
                "DELETE", f"/api/persistent/threads/{self.thread}?force=true"
            )
            self.mark("session ended")
        except GateError as exc:
            self.verdict.warn("ending the session failed", str(exc))
            return
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            try:
                state = self.queue()
            except GateError:
                state = None
            if not state or state["state"] not in ("queued", "leased"):
                return
            time.sleep(3)
        self.verdict.warn("the ended session's unit is still queued or leased")

    # -- reads ---------------------------------------------------------------
    def queue(self):
        return one(
            "SELECT state, attempts_since_completion AS attempts, max_attempts, "
            "lease_token, leased_by, last_leased_by, input_seq, consumed_seq, "
            "park_reason, last_error "
            f"FROM run_queue WHERE unit_id='{self.thread}'"
        )

    def assert_not_parked(self):
        state = self.queue()
        if state and state["state"] == "parked":
            raise GateFailure(f"unit parked: {state.get('park_reason')}")
        parks = sql(
            f"SELECT count(*) FROM thread_events WHERE thread_id='{self.thread}' "
            "AND kind='turn.parked'"
        )
        if parks != "0":
            raise GateFailure(f"{parks} turn.parked event(s)")
        return state

    def final_answers(self, after_seq):
        return rows(
            "SELECT seq, id, turn_number, created_at, left(content, 4000) AS content "
            f"FROM thread_messages WHERE thread_id='{self.thread}' AND role='ai' "
            f"AND rewound_at IS NULL AND seq > {int(after_seq)} "
            "AND jsonb_array_length(CASE WHEN jsonb_typeof(tool_calls)='array' "
            "THEN tool_calls ELSE '[]'::jsonb END) = 0 "
            "AND btrim(coalesce(content, '')) NOT IN ('', '</think>') ORDER BY seq"
        )

    def answered(self, after_seq):
        state = self.assert_not_parked()
        if not state or state["state"] != "done":
            return False
        if state["input_seq"] != state["consumed_seq"]:
            return False
        answers = self.final_answers(after_seq)
        if not answers:
            # A settled unit without an answer never answers: fail fast on the
            # turn's error row (e.g. a provider "Connection error.") or on an
            # input consumed without a turn.
            error = sql(
                "SELECT left(content, 300) FROM thread_messages "
                f"WHERE thread_id='{self.thread}' AND role='error' "
                f"AND seq > {int(after_seq)} ORDER BY seq DESC LIMIT 1"
            )
            raise GateFailure(
                f"the unit is done without an answer: {error or 'no error row'}"
            )
        return answers

    def first_call_message(self, after_seq):
        return one(
            "SELECT seq, id, created_at, tool_calls FROM thread_messages "
            f"WHERE thread_id='{self.thread}' AND role='ai' AND rewound_at IS NULL "
            f"AND seq > {int(after_seq)} AND jsonb_typeof(tool_calls)='array' "
            "AND jsonb_array_length(tool_calls) > 0 ORDER BY seq LIMIT 1"
        )

    def results(self, call_ids):
        return rows(
            "SELECT seq, id, tool_call_id, turn_number, created_at, content, metrics "
            f"FROM thread_messages WHERE thread_id='{self.thread}' AND role='tool' "
            f"AND rewound_at IS NULL AND tool_call_id IN {in_list(call_ids)} "
            "ORDER BY seq"
        )

    def children(self):
        shells = in_list(SHELL_TOOLS)
        return rows(
            "SELECT c.id, c.subagent_handle, c.subagent_type, c.subagent_status, "
            "c.subagent_outcome, c.subagent_error, c.status, c.parent_tool_call_id, "
            "c.report_path, c.created_at, c.ended_at, c.total_turns, c.total_tokens, "
            "c.metadata->'subagent' AS spawn, EXISTS (SELECT 1 FROM thread_messages m "
            "CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN "
            "jsonb_typeof(m.tool_calls)='array' THEN m.tool_calls ELSE '[]'::jsonb "
            "END) AS call WHERE m.thread_id=c.id AND m.role='ai' "
            f"AND call->>'name' IN {shells} AND NOT EXISTS (SELECT 1 FROM "
            "thread_messages r WHERE r.thread_id=c.id AND r.role='tool' "
            "AND r.tool_call_id=call->>'id')) AS in_shell "
            f"FROM threads c WHERE c.parent_thread_id='{self.thread}' "
            "AND c.kind='subagent' ORDER BY c.created_at"
        )

    def continuations(self):
        # Rewound rows included: a second continuation must never exist at all.
        return rows(
            "SELECT seq, id, turn_number, created_at, rewound_at, content, metrics "
            f"FROM thread_messages WHERE thread_id='{self.thread}' AND role='event' "
            f"AND metrics->'{RECOVERY_METRICS_KEY}'->>'kind'='continuation' ORDER BY seq"
        )

    def subagent_deliveries(self):
        return rows(
            "SELECT delivery_id, message_id, state, supersedes_input_seq, "
            "admitted_turn_number, persisted_at, admitted_at, settled_at "
            f"FROM thread_input_deliveries WHERE thread_id='{self.thread}' "
            "AND source='subagent' ORDER BY persisted_at"
        )

    def stream_events_after(self, timestamp_sql):
        return int(
            sql(
                f"SELECT count(*) FROM thread_events WHERE thread_id='{self.thread}' "
                f"AND kind IN ('token', 'thinking') AND created_at > ({timestamp_sql})"
            )
        )

    def permissions(self):
        return rows(
            "SELECT id, tool_call_id, tool_name, status, requested_at, decided_at, "
            "decided_by, accepted_lease_token FROM thread_permission_requests "
            f"WHERE thread_id='{self.thread}' ORDER BY requested_at"
        )

    def audit_count(self, call_type, after_db_ts=None):
        after = (
            f"AND timestamp > {lit(after_db_ts)}::timestamptz" if after_db_ts else ""
        )
        return int(
            audit_sql(
                f"SELECT count(*) FROM llm_requests WHERE job_id='{self.thread}' "
                "AND timestamp > now() - interval '2 days' "
                f"AND call_type={lit(call_type)} {after}"
            )
            or 0
        )

    def main_calls_with(self, text):
        return rows(
            "SELECT id, timestamp, request->'messages' AS messages FROM llm_requests "
            f"WHERE job_id='{self.thread}' AND timestamp > now() - interval '2 days' "
            f"AND call_type='main' AND position({lit(text)} in request::text) > 0 "
            "ORDER BY id",
            audit=True,
        )

    # -- steps ---------------------------------------------------------------
    def warm_up(self, cap):
        seq = self.send(warm_up_prompt(self.tag("warmup")), "warmup")
        answers = wait_for(
            f"{self.name}: warm-up answered (sandbox booted, session attached)",
            lambda: self.answered(seq),
            timeout=self.gate.args.timeout,
        )
        text = answers[-1]["content"] or ""
        self.facts["warm_up_answer"] = text
        if "NO-DELEGATE-TOOL" in text:
            raise GateFailure("the session has no delegate_agent tool")
        if SINGLE_CHILD_SENTENCE in text:
            raise GateFailure(
                "the session is told one child per response: fan-out is not "
                f"allowed (operator switch {FANOUT_SWITCH_ENV}, the claim keys "
                f"{FANOUT_CLAIM_KEY}/{SETTLE_CONTRACT_KEY}, an old image, or a "
                "model family with parallel_tool_calls: false)"
            )
        # Both sentences exist only in the description of a session that may
        # fan out (delegate_agent._session_fanout_lines).
        if "in a single response" in text or "calls per turn" in text:
            self.verdict.check("the session is offered fan-out", True)
        else:
            self.verdict.warn("the model paraphrased the fan-out sentence", text)
        if cap is not None:
            capped = re.search(rf"Up to {int(cap)} subagents? runs? at once", text)
            if capped:
                self.verdict.check(f"the session cap {cap} is applied", True)
            else:
                self.verdict.warn(f"cap {cap} not visible in the quote", text)
        return seq

    def wait_batch(self, after_seq, n):
        def probe():
            self.assert_not_parked()
            message = self.first_call_message(after_seq)
            if not message:
                if self.final_answers(after_seq):
                    raise GateFailure("the model answered without delegating")
                return False
            calls = message["tool_calls"] or []
            names = [c.get("name") for c in calls]
            if names.count("delegate_agent") != n or len(calls) != n:
                raise GateFailure(
                    f"model behaviour, not the feature: the first response "
                    f"carries {names} instead of {n} delegate_agent calls; rerun"
                )
            return message

        message = wait_for(
            f"{self.name}: parent issued {n} delegate_agent calls in one response",
            probe,
            timeout=self.gate.args.timeout,
        )
        calls = [c["id"] for c in message["tool_calls"]]
        self.facts["batch"] = {
            "seq": message["seq"],
            "id": message["id"],
            "calls": calls,
        }
        self.mark("batch durable", seq=message["seq"], calls=len(calls))
        return calls

    def check_refusal(self, calls):
        for result in self.results(calls):
            if FANOUT_REFUSAL in (result["content"] or ""):
                raise GateFailure(
                    "the executor refused the batch (" + FANOUT_REFUSAL + "): "
                    "the live batch path (WP3b) is not serving"
                )

    def kill_lease_holder(self, label):
        state = self.assert_not_parked()
        if not state or state["state"] != "leased" or not state["leased_by"]:
            raise GateFailure(f"no lease to kill at {label}: {state}")
        pod = state["leased_by"]
        at = db_now()
        # hard: SIGTERM, SIGKILL one second later (a crash); drain: the pod's
        # own grace period, in which the executor finishes the turn.
        grace = ["--grace-period=1"] if self.gate.args.kill == "hard" else []
        command(K + ["delete", "pod", pod, "--wait=false"] + grace)
        kill = {
            "label": label,
            "pod": pod,
            "lease_token": int(state["lease_token"]),
            "db_ts": at,
            "mode": self.gate.args.kill,
        }
        self.facts["kills"].append(kill)
        self.mark(f"killed {pod}", **kill)
        return kill

    def wait_successor(self, kill):
        def probe():
            state = self.assert_not_parked()
            if not state or int(state["lease_token"]) <= kill["lease_token"]:
                return False
            if state["state"] == "leased" and state["leased_by"] != kill["pod"]:
                return state
            return state if state["state"] == "done" else False

        state = wait_for(
            f"{self.name}: successor claimed after killing {kill['pod']}",
            probe,
            timeout=self.gate.args.timeout,
        )
        kill["successor_db_ts"] = db_now()
        kill["successor_pod"] = state["leased_by"]
        self.mark("successor claim", pod=state["leased_by"], token=state["lease_token"])
        return state

    def wait_audit_settled(self):
        """Audit rows are archived off the loop; wait until the count holds."""
        last, stable_since = None, time.monotonic()
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            count = int(
                audit_sql(
                    f"SELECT count(*) FROM llm_requests WHERE job_id='{self.thread}' "
                    "AND timestamp > now() - interval '2 days'"
                )
                or 0
            )
            if count != last:
                last, stable_since = count, time.monotonic()
            elif time.monotonic() - stable_since >= 10:
                return count
            time.sleep(2)
        return last

    # -- evidence --------------------------------------------------------------
    def collect(self):
        if self.thread is None:
            return
        t = self.thread
        pieces = {
            "session.json": lambda: one(
                "SELECT id, title, config_name, execution_lane, status, "
                "permission_mode, total_turns, total_tokens, created_at, ended_at "
                f"FROM threads WHERE id='{t}'"
            ),
            "run_queue.json": self.queue,
            "transcript.json": lambda: rows(
                "SELECT seq, id, role, turn_number, tool_call_id, tool_calls, "
                "content, metrics, created_at, rewound_at FROM thread_messages "
                f"WHERE thread_id='{t}' ORDER BY seq"
            ),
            "deliveries.json": lambda: rows(
                "SELECT delivery_id, message_id, source, state, claim_generation, "
                "admitted_turn_number, supersedes_input_seq, owner_executor, "
                "owner_run_queue_lease_token, persisted_at, admitted_at, settled_at, "
                "cancelled_at, cancelled_reason, deferred_reason "
                f"FROM thread_input_deliveries WHERE thread_id='{t}' "
                "ORDER BY persisted_at"
            ),
            "children.json": self.children,
            "child_transcripts.json": lambda: rows(
                "SELECT m.thread_id, m.seq, m.role, m.turn_number, m.tool_call_id, "
                "m.tool_calls, left(m.content, 4000) AS content, m.created_at "
                "FROM thread_messages m JOIN threads c ON c.id=m.thread_id "
                f"WHERE c.parent_thread_id='{t}' ORDER BY m.thread_id, m.seq"
            ),
            "events.json": lambda: {
                "stream_counts": rows(
                    "SELECT kind, count(*) FROM thread_events "
                    f"WHERE thread_id='{t}' AND kind IN ('token', 'thinking') "
                    "GROUP BY kind"
                ),
                "events": rows(
                    "SELECT id, epoch, seq, kind, left(payload::text, 2000) AS "
                    f"payload, created_at FROM thread_events WHERE thread_id='{t}' "
                    "AND kind NOT IN ('token', 'thinking') ORDER BY id"
                ),
            },
            "permissions.json": self.permissions,
            "audit_requests.json": lambda: rows(
                "SELECT id, call_type, model, timestamp, latency_ms, metadata, "
                "auxiliary_metadata, metrics FROM llm_requests "
                f"WHERE job_id='{t}' AND timestamp > now() - interval '2 days'",
                audit=True,
            ),
            "audit_main_requests.json": lambda: rows(
                "SELECT id, timestamp, request->'messages' AS messages FROM "
                f"llm_requests WHERE job_id='{t}' AND call_type='main' "
                "AND timestamp > now() - interval '2 days'",
                audit=True,
            ),
        }
        for name, read in pieces.items():
            try:
                self.dump(name, read())
            except Exception as exc:  # partial evidence beats none
                (self.dir / f"{name}.error").write_text(str(exc))
        self.dump("timeline.json", self.timeline)
        self.dump("facts.json", self.facts)
        self.dump("verdict.json", {"ok": self.verdict.ok, "checks": self.verdict.items})


# ---------------------------------------------------------------------------
# Shared assertions
# ---------------------------------------------------------------------------


def _norm(text) -> str:
    return " ".join(str(text or "").split())


def _message_text(message) -> str:
    content = message.get("content")
    if isinstance(content, list):
        content = " ".join(
            part.get("text", "") if isinstance(part, dict) else str(part)
            for part in content
        )
    return _norm(content)


def each_report_once(messages, results) -> list[str]:
    """Problems with 'each report exactly once in the provider input'."""
    problems = []
    tools = [m for m in messages if m.get("role") == "tool"]
    for result in results:
        count = sum(1 for m in tools if m.get("tool_call_id") == result["tool_call_id"])
        if count != 1:
            problems.append(f"{result['tool_call_id']}: {count} tool messages")
    # Identical texts (two NOT STARTED results) must appear as often as rows.
    expected = Counter(_norm(r["content"])[:300] for r in results)
    for text, want in expected.items():
        got = sum(1 for m in messages if text and text in _message_text(m))
        if got != want:
            problems.append(f"report {text[:60]!r}: {got} copies, expected {want}")
    continuation = sum(1 for m in messages if CONTINUATION_MARKER in _message_text(m))
    if continuation != 1:
        problems.append(f"continuation text: {continuation} copies")
    return problems


def recovery_class(result):
    metrics = result.get("metrics") or {}
    recovery = metrics.get(RECOVERY_METRICS_KEY) if isinstance(metrics, dict) else None
    return (recovery or {}).get("class") if isinstance(recovery, dict) else None


# ---------------------------------------------------------------------------
# Pinned lane (P6): helpers
# ---------------------------------------------------------------------------


def pod_json(name):
    """The pod object, or None when it no longer exists."""
    try:
        return json.loads(command(K + ["get", "pod", name, "-o", "json"]))
    except GateError as exc:
        if "NotFound" in str(exc) or "not found" in str(exc):
            return None
        raise


def pod_ready(pod) -> bool:
    status = (pod or {}).get("status") or {}
    if status.get("phase") != "Running" or pod["metadata"].get("deletionTimestamp"):
        return False
    return all(c.get("ready") for c in status.get("containerStatuses") or [{}])


def pod_log(name, since=None, container=PINNED_CONTAINER) -> str:
    """A pod's log ('' once the pod is gone)."""
    args = K + ["logs", name, "-c", container]
    if since:
        args.append(f"--since-time={since}")
    try:
        return command(args, timeout=60)
    except GateError:
        return ""


def node_agent_process(pod) -> tuple[int, str]:
    """(host pid, command line) of a pod's agent process, seen from the k3d
    node. The agent is PID 1 of its container, which ignores a SIGKILL sent
    from inside; from the node it is an ordinary process."""
    ids = command(
        ["docker", "exec", K3D_NODE, "crictl", "ps", "-q", "--state", "running"]
        + ["--name", PINNED_CONTAINER, "--label", f"io.kubernetes.pod.name={pod}"]
    ).split()
    if len(ids) != 1:
        raise GateFailure(f"expected one running agent container in {pod}: {ids}")
    pid = int(
        command(
            ["docker", "exec", K3D_NODE, "crictl", "inspect", "--output"]
            + ["go-template", "--template", "{{.info.pid}}", ids[0]]
        )
    )
    cmdline = command(["docker", "exec", K3D_NODE, "cat", f"/proc/{pid}/cmdline"])
    return pid, cmdline.replace("\x00", " ")


def values_lanes() -> str:
    """The switch value in the gitignored values-local.yaml (exactly one line)."""
    found = _VALUES_LANES.findall(VALUES_LOCAL.read_text())
    if len(found) != 1:
        raise GateError(
            f"{VALUES_LOCAL.name}: expected one sessionSubagentFanoutLanes line, "
            f"found {len(found)}"
        )
    return found[0][1]


def set_values_lanes(value: str) -> str:
    """Rewrite only the switch line; returns the previous value."""
    previous = values_lanes()
    text = VALUES_LOCAL.read_text()
    VALUES_LOCAL.write_text(
        _VALUES_LANES.sub(lambda m: f'{m.group(1)}"{value}"', text, count=1)
    )
    return previous


def lane_names(value) -> set[str]:
    return {x.strip().lower() for x in str(value or "").split(",") if x.strip()}


def orchestrator_serves_lanes(value: str) -> bool:
    """Tilt re-applied the chart and every live orchestrator pod carries
    ``value``. The env comes from the ConfigMap (``valueFrom``), so the
    deployment template holds no literal value: read it in each pod."""
    dep = deployment(ORCHESTRATOR_DEPLOYMENT)
    if not rollout_converged(dep):
        return False
    pods = [
        p
        for p in executor_pods(running_only=False, selector=ORCHESTRATOR_SELECTOR)
        if not p["terminating"]
    ]
    if len(pods) != dep["spec"]["replicas"] or any(
        p["phase"] != "Running" for p in pods
    ):
        return False
    for pod in pods:
        served = command(
            K
            + ["exec", pod["name"], "-c", ORCHESTRATOR_CONTAINER, "--", "sh", "-c"]
            + [f'printenv {FANOUT_SWITCH_ENV} || echo "<unset>"']
        )
        if served != value:
            return False
    return True


# A cockpit-like viewer (run inside the orchestrator pod, password on stdin):
# one WebSocket subscriber on the session's current pod. Without a subscriber
# a pinned session flips to awaiting_user at its input wait ("eager" headless
# mode) and keeps that status through later turns, so the detector settles a
# runtime lost mid-turn as `suspended` instead of `ended`. Commands on stdin:
# `connect <pod ip>`; output lines: connected/closed/error, never the token.
_VIEWER_PROGRAM = r"""
import asyncio, json, sys, urllib.error, urllib.parse, urllib.request
import websockets
envelope = json.loads(sys.stdin.readline())
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
def session_token():
    form = urllib.parse.urlencode({
        "grant_type": "password", "client_id": "admin-cli", "scope": "openid",
        "username": envelope["username"], "password": envelope["password"],
    }).encode()
    with opener.open(envelope["token_url"], data=form, timeout=30) as response:
        bearer = json.load(response)["id_token"]
    request = urllib.request.Request(
        "http://localhost:8085/api/sessions/%s/connection" % envelope["thread"],
        headers={"Authorization": "Bearer " + bearer},
    )
    with opener.open(request, timeout=60) as response:
        return json.load(response)["token"]
async def hold(ip):
    for attempt in range(20):
        try:
            token = await asyncio.to_thread(session_token)
            url = "ws://%s:8001/p/%s/ws?t=%s" % (ip, envelope["thread"], token)
            async with websockets.connect(
                url, max_size=None, open_timeout=30, ping_interval=20
            ) as ws:
                print("connected " + ip, flush=True)
                async for _ in ws:
                    pass
            print("closed " + ip, flush=True)
            return
        except asyncio.CancelledError:
            raise
        except Exception as error:
            code = getattr(error, "code", "")
            print("error %s %s %s" % (ip, type(error).__name__, code), flush=True)
            await asyncio.sleep(3)
async def main():
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    await loop.connect_read_pipe(
        lambda: asyncio.StreamReaderProtocol(reader), sys.stdin
    )
    task = None
    while True:
        line = await reader.readline()
        if not line:
            break
        parts = line.decode().split()
        if len(parts) == 2 and parts[0] == "connect":
            if task is not None:
                task.cancel()
            task = asyncio.create_task(hold(parts[1]))
    if task is not None:
        task.cancel()
asyncio.run(main())
"""


class Viewer:
    """Holds one cockpit-like WebSocket subscriber on the session's pod."""

    def __init__(self, api, thread, path: Path):
        self.path = path
        self.events = []
        self.proc = subprocess.Popen(
            K
            + ["exec", "-i", f"deploy/{ORCHESTRATOR_DEPLOYMENT}"]
            + ["-c", ORCHESTRATOR_CONTAINER, "--", "python", "-u", "-c"]
            + [_VIEWER_PROGRAM],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        envelope = {
            "username": api.username,
            "password": api.password,
            "token_url": KEYCLOAK_TOKEN_URL,
            "thread": thread,
        }
        self.proc.stdin.write(json.dumps(envelope) + "\n")
        self.proc.stdin.flush()
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()

    def _read(self):
        for line in self.proc.stdout:
            entry = {"at": utc_now(), "line": _scrub(line.strip())}
            self.events.append(entry)
            with open(self.path, "a") as handle:
                handle.write(json.dumps(entry) + "\n")

    def connect(self, ip, timeout=120) -> bool:
        start = len(self.events)
        try:
            self.proc.stdin.write(f"connect {ip}\n")
            self.proc.stdin.flush()
        except (BrokenPipeError, OSError, ValueError):
            return False
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if any(e["line"] == f"connected {ip}" for e in self.events[start:]):
                return True
            if self.proc.poll() is not None:
                return False
            time.sleep(1)
        return False

    def stop(self):
        try:
            self.proc.stdin.close()
        except (OSError, ValueError):
            pass
        try:
            self.proc.wait(15)
        except subprocess.TimeoutExpired:
            self.proc.kill()


def expected_class_from_row(child) -> str:
    """The class the settle gives a call, from its child's end state."""
    if child is None:
        return "not_started"  # queued behind the cap: no row
    if child["subagent_outcome"] == PARENT_RESTART_NOT_STARTED_OUTCOME:
        return "not_started"
    if child["subagent_status"] == "completed":
        return "completed"
    return "interrupted"


class PinnedRun(Run):
    """One pinned conference session (P6).

    The runtime is the session's own agent pod (label srw.io/thread-id); its
    inputs are pinned deliveries (no run_queue row); a lost runtime is retired
    by the stale-agent detector and Resume provisions the successor, whose
    attach settles the batch."""

    def __init__(self, gate, name):
        super().__init__(gate, name)
        self.logs = LogCollector(
            self.dir / "logs",
            selector=lambda: (
                f"{PINNED_THREAD_LABEL}={self.thread}" if self.thread else None
            ),
            container=PINNED_CONTAINER,
        )
        self.orchestrator_logs = LogCollector(
            self.dir / "logs" / "orchestrator",
            selector=ORCHESTRATOR_SELECTOR,
            container=ORCHESTRATOR_CONTAINER,
        )
        self.extra_collectors = [self.orchestrator_logs]
        self.runtimes = []
        self.facts["runtimes"] = self.runtimes
        self.facts["lane"] = PINNED_LANE
        self._answerless_since = None
        self.viewer = None

    # -- session -----------------------------------------------------------
    def create_session(self, *, permission_mode="autonomous", cap=None):
        gate, args = self.gate, self.gate.args
        delegation = {"enabled": True}
        if cap is not None:
            delegation["session_max_concurrent"] = int(cap)
        body = {
            "title": f"P6 pinned gate {self.name} {self.nonce}",
            "expert": {"inline": {"runtime": gate.pinned_expert_runtime()}},
            "permission_mode": permission_mode,
            "project_id": gate.project_id,
            "config_override": {
                "workspace": {"backend": "sandbox"},
                "delegation": delegation,
                "officer": {"conference": True},
                "llm": {"model": args.model},
            },
        }
        result = gate.api.call("POST", "/api/persistent/threads", body)
        self.thread = uid(result.get("thread_id") or result["id"])
        gate.record_thread(self.name, self.thread)
        self.facts.update(
            thread=self.thread,
            permission_mode=permission_mode,
            cap=cap,
            model=args.model,
            project=gate.project_id,
            parent_parallel_tool_calls=args.parent_parallel_tool_calls,
        )
        self.mark("session created", thread=self.thread)
        if args.viewer:
            self.viewer = Viewer(gate.api, self.thread, self.dir / "viewer.jsonl")
            self.extra_collectors.append(self.viewer)
        if result.get("ignored_config_keys"):
            raise GateFailure(
                f"create ignored config keys {result['ignored_config_keys']}"
            )
        row = one(
            "SELECT execution_lane, metadata->'config_override'->'officer' AS officer "
            f"FROM threads WHERE id='{self.thread}'"
        )
        if not row or row["execution_lane"] != PINNED_LANE:
            raise GateFailure(f"session lane is {row and row['execution_lane']!r}")
        self.verdict.check(
            "threads.execution_lane = 'pinned' (officer.conference)", True, row
        )
        return self.wait_runtime("first runtime")

    def runtime(self):
        return one(
            "SELECT t.status, t.agent_id, t.runtime_generation, "
            "t.runtime_attach_token, t.runtime_retirement_token, "
            "t.runtime_retirement_context, t.runtime_retirement_started_at, "
            "a.hostname, a.pod_uid, a.status AS agent_status, a.last_heartbeat "
            "FROM threads t LEFT JOIN agents a ON a.id=t.agent_id "
            f"WHERE t.id='{self.thread}'"
        )

    def wait_runtime(self, label, previous_pod=None):
        """Wait for a runtime pod of this session to attach; check its code."""

        def probe():
            row = self.runtime()
            if not row or not row["agent_id"] or not row["hostname"]:
                return False
            if previous_pod and row["hostname"] == previous_pod:
                return False
            pod = pod_json(row["hostname"])
            if pod is None or not pod_ready(pod):
                return False
            labels = pod["metadata"].get("labels") or {}
            if labels.get(PINNED_THREAD_LABEL) != self.thread:
                raise GateFailure(
                    f"{row['hostname']} is labelled for "
                    f"{labels.get(PINNED_THREAD_LABEL)!r}, not this session"
                )
            ads = [
                m.groupdict()
                for m in ADVERTISEMENT_RE.finditer(pod_log(row["hostname"]))
                if m.group("thread") == self.thread
            ]
            return (row, pod, ads[-1]) if ads else False

        row, pod, ad = wait_for(
            f"{self.name}: {label} attached (pod ready, advertisement logged)",
            probe,
            timeout=self.gate.args.timeout,
            interval=3,
        )
        name = row["hostname"]
        for path, marker in PINNED_IMAGE_MARKERS:
            try:
                count = command(
                    K
                    + ["exec", name, "-c", PINNED_CONTAINER, "--", "grep", "-c"]
                    + [marker, f"{POD_ROOT}/{path}"]
                )
            except GateError:
                count = "0"
            self.verdict.check(
                f"{name} runs the current image ({marker} present)",
                int(count or 0) > 0,
                count,
            )
        bad = served_mismatches(
            name, PINNED_CONTAINER, PINNED_AGENT_FILES + self.gate.config_paths()
        )
        self.verdict.check(f"{name} serves the HEAD bytes", not bad, bad)
        container = (pod["status"].get("containerStatuses") or [{}])[0]
        info = {
            "label": label,
            "pod": name,
            "pod_uid": pod["metadata"]["uid"],
            "agent_id": row["agent_id"],
            "generation": row["runtime_generation"],
            "attach_token": row["runtime_attach_token"],
            "advertisement": ad,
            "restarts": container.get("restartCount", 0),
            "image": pod["spec"]["containers"][0]["image"],
            "db_ts": db_now(),
        }
        if self.viewer is not None:
            ip = (pod.get("status") or {}).get("podIP")
            info["viewer"] = bool(ip) and self.viewer.connect(ip)
            self.verdict.check(
                f"a viewer is subscribed to {name} (the cockpit's WebSocket)",
                info["viewer"],
                ip,
            )
        self.runtimes.append(info)
        self.mark(f"{label} attached", pod=name, fanout=ad["fanout"])
        return info

    def runtime_log(self, pod) -> str:
        """The longer of the live log and the followed copy (a killed pod's
        log survives only in the followed copy)."""
        live = pod_log(pod, since=self.logs.since)
        followed = self.dir / "logs" / f"{pod}.followed.log"
        kept = followed.read_text(errors="replace") if followed.exists() else ""
        return live if len(live) >= len(kept) else kept

    def orchestrator_log(self) -> str:
        directory = self.dir / "logs" / "orchestrator"
        return "\n".join(
            p.read_text(errors="replace") for p in sorted(directory.glob("*.log"))
        )

    def end(self):
        if self.thread is None:
            return

        def settled(row):
            return (
                row
                and row["status"] == "ended"
                and not row["agent_id"]
                and not row["runtime_retirement_token"]
            )

        row = self.runtime()
        if settled(row):
            self.mark("session already ended")
            return
        path = f"/api/persistent/threads/{self.thread}?force=true"
        try:
            self.gate.api.call("DELETE", path)
            self.mark("session End requested")
        except GateError as exc:
            if not (row and row["status"] == "suspended" and not row["agent_id"]):
                self.verdict.warn("ending the session failed", str(exc))
                return
            # A runtime-less suspended sandbox session refuses End (its
            # workspace pod is gone): bring it up, then End it.
            self.verdict.warn("End of the suspended session refused", str(exc))
            try:
                self.gate.api.call("POST", f"/api/sessions/{self.thread}/prepare", {})
                self.wait_runtime("runtime for the End")
                self.gate.api.call("DELETE", path)
                self.mark("session End requested after prepare")
            except (GateError, GateFailure) as again:
                self.verdict.warn("ending the session failed", str(again))
                return
        started = time.monotonic()
        deadline, row, checked = started + 300, None, started
        while time.monotonic() < deadline:
            try:
                row = self.runtime()
            except GateError:
                row = None
            if settled(row):
                self.mark("session ended")
                return
            pod = (row or {}).get("hostname")
            if pod and time.monotonic() - checked >= 15:
                checked = time.monotonic()
                log = pod_log(pod)
                if log.count(QUIESCE_WEDGE_LOG) >= 3:
                    self._end_wedged(pod, log, row)
                    return
            time.sleep(3)
        self.verdict.warn("the session's End did not settle within 300 s", row)

    def _end_wedged(self, pod, log, row):
        """The runtime retries its End quiescence forever (a recovered child's
        handle is never settled); keep the evidence, then delete this
        session's pod so the retirement can settle without it."""
        (self.dir / "logs").mkdir(parents=True, exist_ok=True)
        (self.dir / "logs" / f"{pod}.after-end.log").write_text(log + "\n")
        self.facts["end_wedged"] = {
            "pod": pod,
            "quiesce_failures": log.count(QUIESCE_WEDGE_LOG),
            "thread": row,
        }
        self.dump("facts.json", self.facts)
        self.verdict.warn(
            "End wedged: the runtime cannot quiesce its subagents "
            f"({QUIESCE_WEDGE_LOG!r}); deleting its pod",
            {"pod": pod, "failures": log.count(QUIESCE_WEDGE_LOG)},
        )
        labels = ((pod_json(pod) or {}).get("metadata") or {}).get("labels") or {}
        if labels.get(PINNED_THREAD_LABEL) == self.thread:
            command(K + ["delete", "pod", pod, "--wait=false"])
            self.mark("deleted the wedged runtime pod", pod=pod)

    # -- reads ---------------------------------------------------------------
    def queue(self):
        return None  # a pinned session has no run_queue row

    def deliveries(self):
        return rows(
            "SELECT d.delivery_id, d.source, d.state, d.deferred_reason, "
            "d.claim_generation, d.admission_count, d.supersedes_input_seq, "
            "d.admitted_turn_number, d.owner_agent_id, d.owner_runtime_generation, "
            "m.seq, d.persisted_at, d.admitted_at, d.settled_at, d.cancelled_at, "
            "d.cancelled_reason FROM thread_input_deliveries d LEFT JOIN "
            "thread_messages m ON m.id=d.message_id "
            f"WHERE d.thread_id='{self.thread}' ORDER BY d.persisted_at"
        )

    def assert_not_parked(self):
        deliveries = self.deliveries()
        parked = [
            d
            for d in deliveries
            if d["state"] == "deferred" and d["deferred_reason"] == "max_attempts"
        ]
        if parked:
            raise GateFailure(f"input parked at the recovery bound: {parked}")
        return {"state": PINNED_LANE, "deliveries": deliveries}

    def answered(self, after_seq):
        deliveries = self.assert_not_parked()["deliveries"]
        if any(d["state"] not in ("settled", "cancelled") for d in deliveries):
            self._answerless_since = None
            return False
        answers = self.final_answers(after_seq)
        if answers:
            self._answerless_since = None
            return answers
        # The pinned turn broadcasts its end before its reconcile (P1): give
        # the answer row a moment, then fail fast on an answerless settle.
        now = time.monotonic()
        if self._answerless_since is None:
            self._answerless_since = now
        elif now - self._answerless_since > 45:
            error = sql(
                "SELECT left(content, 300) FROM thread_messages "
                f"WHERE thread_id='{self.thread}' AND role='error' "
                f"AND seq > {int(after_seq)} ORDER BY seq DESC LIMIT 1"
            )
            raise GateFailure(
                f"every input settled without an answer: {error or 'no error row'}"
            )
        return False

    def tool_row_counts(self, call_ids):
        """Tool rows per call, rewound rows included (P2: never a duplicate)."""
        found = rows(
            "SELECT tool_call_id, count(*) AS n FROM thread_messages "
            f"WHERE thread_id='{self.thread}' AND role='tool' "
            f"AND tool_call_id IN {in_list(call_ids)} GROUP BY tool_call_id"
        )
        return {r["tool_call_id"]: r["n"] for r in found}

    # -- kills ---------------------------------------------------------------
    def kill(self, label, mode):
        """End the current runtime. sigkill: ``kill -9`` of the agent process
        from the k3d node (no SIGTERM, no preStop: an OOM kill); force-delete:
        ``kubectl delete --grace-period=0 --force`` (kubelet still sends
        SIGTERM and runs preStop for its 2 s minimum grace); graceful: a plain
        delete (terminationGracePeriodSeconds 180, P4's hand-over)."""
        row = self.runtime()
        if not row or not row["hostname"]:
            raise GateFailure(f"no runtime to kill at {label}: {row}")
        name = row["hostname"]
        pod = pod_json(name)
        labels = ((pod or {}).get("metadata") or {}).get("labels") or {}
        if labels.get(PINNED_THREAD_LABEL) != self.thread:
            raise GateFailure(f"{name} is not this session's runtime pod")
        kill = {
            "label": label,
            "mode": mode,
            "pod": name,
            "pod_uid": pod["metadata"]["uid"],
            "agent_id": row["agent_id"],
            "generation": row["runtime_generation"],
            "attach_token": row["runtime_attach_token"],
            # The detector settles `suspended` for awaiting_user, else `ended`.
            "thread_status": row["status"],
        }
        if mode == "sigkill":
            pid, cmdline = node_agent_process(name)
            if f"--thread-id {self.thread}" not in cmdline:
                raise GateFailure(f"pid {pid} is not this session's agent")
            kill["host_pid"] = pid
            kill["db_ts"] = db_now()
            command(["docker", "exec", K3D_NODE, "kill", "-9", str(pid)])
        elif mode == "force-delete":
            kill["db_ts"] = db_now()
            command(
                K
                + ["delete", "pod", name, "--grace-period=0", "--force"]
                + ["--wait=false"]
            )
        elif mode == "graceful":
            kill["db_ts"] = db_now()
            command(K + ["delete", "pod", name, "--wait=false"])
        else:
            raise GateError(f"unknown kill mode {mode!r}")
        self.facts["kills"].append(kill)
        self.mark(f"{mode}: {name}", label=label, db_ts=kill["db_ts"])
        return kill

    def wait_runtime_dead(self, kill, timeout=300):
        """Record how the killed runtime's container ended."""

        def probe():
            pod = pod_json(kill["pod"])
            if pod is None:
                return {"pod": "deleted"}
            if pod["metadata"]["uid"] != kill["pod_uid"]:
                return {"pod": "replaced"}
            container = (pod["status"].get("containerStatuses") or [{}])[0]
            terminated = (container.get("state") or {}).get("terminated")
            if terminated:
                return {
                    "phase": pod["status"].get("phase"),
                    "reason": terminated.get("reason"),
                    "exit_code": terminated.get("exitCode"),
                    "signal": terminated.get("signal"),
                }
            return False

        kill["runtime_end"] = wait_for(
            f"{self.name}: {kill['pod']} stopped", probe, timeout=timeout, interval=1
        )
        kill["runtime_end_db_ts"] = db_now()
        return kill["runtime_end"]

    def wait_retired(self, kill, *, live_runtime=False):
        """Wait until the lost or deleted runtime's retirement settled; sample
        the pending retirement context (``cause``) on the way, because the
        settle clears it. ``live_runtime``: the runtime is still running (a
        person's End), so watch its log for the End's quiescence wedge; on a
        wedge, record the failure and delete this session's pod."""
        contexts = kill.setdefault("retirement_contexts_seen", [])
        statuses = kill.setdefault("statuses_seen", [])
        started = time.monotonic()
        checked = [started]

        def probe():
            row = self.runtime()
            if not statuses or statuses[-1]["status"] != row["status"]:
                statuses.append({"at": utc_now(), "status": row["status"]})
            context = row.get("runtime_retirement_context")
            if context and (not contexts or contexts[-1]["context"] != context):
                contexts.append({"at": utc_now(), "context": context})
            if (
                row["status"] in ("ended", "suspended")
                and not row["agent_id"]
                and not row["runtime_retirement_token"]
            ):
                return row
            # A wedge is a retry loop that outlives a normal End (a soft End
            # with a snapshot upload took about 75 s): never delete on a
            # transient retry.
            if (
                live_runtime
                and "end_wedged" not in kill
                and time.monotonic() - started >= 240
                and time.monotonic() - checked[0] >= 15
            ):
                checked[0] = time.monotonic()
                log = pod_log(kill["pod"])
                if log.count(QUIESCE_WEDGE_LOG) >= 3:
                    kill["end_wedged"] = {
                        "at": utc_now(),
                        "failures": log.count(QUIESCE_WEDGE_LOG),
                    }
                    self.verdict.check(
                        "the End completes on its own (no quiescence wedge)",
                        False,
                        f"{log.count(QUIESCE_WEDGE_LOG)} x {QUIESCE_WEDGE_LOG!r}; "
                        "the gate deletes the pod",
                    )
                    self._end_wedged(kill["pod"], log, row)
            return False

        row = wait_for(
            f"{self.name}: the runtime retired after the {kill['mode']} of "
            f"{kill['pod']}",
            probe,
            timeout=self.gate.args.retire_timeout,
            interval=2,
        )
        kill["retired_status"] = row["status"]
        kill["retired_db_ts"] = db_now()
        kill["settled_after_s"] = round(
            float(
                sql(
                    f"SELECT extract(epoch FROM {lit(kill['retired_db_ts'])}::"
                    f"timestamptz - {lit(kill['db_ts'])}::timestamptz)"
                )
            ),
            1,
        )
        kill["retirement_outcomes"] = rows(
            "SELECT disposition, permanent, outcome, settled_at, "
            "retired_agent_pod->>'pod_name' AS pod "
            "FROM thread_runtime_retirement_outcomes "
            f"WHERE thread_id='{self.thread}' "
            f"AND runtime_generation='{kill['generation']}' ORDER BY settled_at"
        )
        self.mark(
            "runtime retired",
            status=row["status"],
            causes=[(c["context"] or {}).get("cause") for c in contexts],
        )
        return row

    def resume(self, kill):
        """Bring the session back the way the cockpit does: Resume for an
        ended session; ``prepare`` for a suspended one (the detector settles a
        runtime lost while the thread read ``awaiting_user`` as suspended, and
        Resume refuses that with 409 session_not_ended)."""
        status = (self.runtime() or {}).get("status")
        if status == "suspended":
            result = self.gate.api.call(
                "POST", f"/api/sessions/{self.thread}/prepare", {}
            )
            self.mark("prepare requested (suspended session)", result=result)
        else:
            result = self.gate.api.call(
                "POST", f"/api/persistent/threads/{self.thread}/resume", {}
            )
            self.mark("resume requested", result=result)
        kill["revived_by"] = "prepare" if status == "suspended" else "resume"
        return self.wait_runtime("successor", previous_pod=kill["pod"])

    # -- evidence --------------------------------------------------------------
    def collect(self):
        super().collect()
        if self.thread is None:
            return
        t = self.thread
        agent_ids = sorted(
            {r["agent_id"] for r in self.runtimes if r.get("agent_id")}
            | {k["agent_id"] for k in self.facts["kills"] if k.get("agent_id")}
        )
        pieces = {
            "thread_runtime.json": lambda: one(
                "SELECT id, status, execution_lane, agent_id, runtime_generation, "
                "runtime_attach_token, runtime_retirement_token, "
                "runtime_retirement_context, ended_at, metadata->'agent_pod' AS "
                "agent_pod, metadata->'config_override' AS config_override "
                f"FROM threads WHERE id='{t}'"
            ),
            "retirement_outcomes.json": lambda: rows(
                "SELECT * FROM thread_runtime_retirement_outcomes "
                f"WHERE thread_id='{t}' ORDER BY settled_at"
            ),
            "agents.json": lambda: rows(
                "SELECT id, hostname, pod_uid, status, registered_at, "
                "last_heartbeat, thread_id FROM agents "
                f"WHERE id::text IN {in_list(agent_ids)}"
            ),
            "pinned_deliveries.json": self.deliveries,
            "child_outcomes.json": lambda: rows(
                "SELECT id, subagent_handle, parent_tool_call_id, subagent_status, "
                "subagent_outcome, subagent_error, status, created_at, ended_at "
                f"FROM threads WHERE parent_thread_id='{t}' ORDER BY created_at"
            ),
        }
        for name, read in pieces.items():
            try:
                self.dump(name, read())
            except Exception as exc:  # partial evidence beats none
                (self.dir / f"{name}.error").write_text(str(exc))


class PinnedScenarios:
    """The pinned twins of K1-K4, the switch and a person's End (P6).

    Mixed into Gate; selected with ``--lane pinned``."""

    # -- configuration ---------------------------------------------------------
    def pinned_expert_runtime(self):
        """HEAD's bundled expert, sent as an inline Expert that authors
        ``llm.model`` and ``llm.parallel_tool_calls``.

        Session create carries only llm.model/temperature/reasoning_level, and
        the family matrix fills ``parallel_tool_calls`` unless the Expert
        authored it. ``muse-spark-1.3`` (the k3d model while MiniMax has no
        credit) is ``parallel_tool_calls: false`` (start safe), so a parent on
        it is told "One subagent at a time" and never fans out. P6 tests the
        lane, not that family flag, so the gate authors it."""
        path = f"config/experts/{self.args.expert}/config.yaml"
        manifest = yaml.safe_load(head_bytes(path))
        runtime = copy.deepcopy(manifest["spec"]["runtime"])
        private = runtime.setdefault("config", {})
        private.pop("config_name", None)  # the session role's own base
        llm = private.setdefault("config", {}).setdefault("llm", {})
        llm["model"] = self.args.model
        if self.args.parent_parallel_tool_calls:
            llm["parallel_tool_calls"] = True
        return runtime

    def config_paths(self):
        expert_file, child_file, _ = roster_entry_paths(
            self.args.expert, self.args.child_type
        )
        return CONFIG_FILES + [p for p in (expert_file, child_file) if p]

    def pinned_preflight(self, need, warn, facts):
        args = self.args
        try:
            model = one(
                "SELECT model_id, family, enabled FROM models "
                f"WHERE model_id={lit(args.model)}"
            )
            need(
                bool(model and model["enabled"]),
                f"model {args.model!r} is in the registry and enabled",
                model,
            )
            if args.project:
                project = one(
                    "SELECT p.id, p.name, pm.role FROM projects p JOIN "
                    "project_members pm ON pm.project_id=p.id JOIN users u ON "
                    f"u.id=pm.user_id WHERE u.preferred_username={lit(self.api_user)} "
                    f"AND p.id={lit(uid(args.project))}"
                )
            else:
                project = one(
                    "SELECT p.id, p.name, pm.role FROM users u JOIN projects p ON "
                    "p.id=u.default_project_id JOIN project_members pm ON "
                    "pm.project_id=p.id AND pm.user_id=u.id "
                    f"WHERE u.preferred_username={lit(self.api_user)}"
                )
            facts["project"] = project
            self.project_id = project["id"] if project else None
            need(
                bool(project) and project["role"] == "owner",
                f"{self.api_user!r} owns the conference project (officer.conference "
                "needs exactly one project and its owner)",
                project,
            )
            if project:
                open_conference = sql(
                    "SELECT coalesce(string_agg(id::text, ','), '') FROM threads "
                    f"WHERE project_id={lit(project['id'])} AND status <> 'ended' "
                    "AND runtime_retirement_authorized_at IS NULL AND "
                    "COALESCE(metadata->'config_override'->'officer'->>'conference',"
                    "'false') = 'true'"
                )
                need(
                    not open_conference,
                    "no open conference on the project (one per project)",
                    open_conference,
                )
        except GateError as exc:
            need(False, "pinned database checks", str(exc))
        if args.pinned_kill == "sigkill":
            try:
                version = command(["docker", "exec", K3D_NODE, "crictl", "version"])
                need("RuntimeName" in version, f"crictl reachable on {K3D_NODE}")
            except GateError as exc:
                need(False, f"crictl reachable on {K3D_NODE} (sigkill)", str(exc))

    # -- shared steps ----------------------------------------------------------
    def _pinned_window(
        self,
        run,
        calls,
        *,
        cap,
        ended_at_kill,
        running_at_kill,
        only_calls=False,
        require_shell=True,
        at_least=False,
    ):
        """Wait for: ``ended_at_kill`` children ended, ``running_at_kill`` in
        their shell sleep (or merely running, without ``require_shell``), the
        rest queued (no row). ``only_calls``: count the batch's children only
        (a background child runs beside it); ``at_least``: more running is
        fine (the expectations follow the observed rows)."""

        def probe():
            run.assert_not_parked()
            run.check_refusal(calls)
            children = run.children()
            if only_calls:
                children = [c for c in children if c["parent_tool_call_id"] in calls]
            running = [c for c in children if c["status"] != "ended"]
            ended = [c for c in children if c["status"] == "ended"]
            if len(running) > cap:
                raise GateFailure(f"{len(running)} children run at once, cap {cap}")
            if len(ended) > ended_at_kill:
                raise GateFailure(
                    f"missed the kill window: {len(ended)} children already ended"
                )
            busy = [c for c in running if c["in_shell"] or not require_shell]
            if len(ended) == ended_at_kill and (
                len(busy) >= running_at_kill
                if at_least
                else len(busy) == running_at_kill
            ):
                return children
            return False

        return wait_for(
            f"{run.name}: {ended_at_kill} child ended, {running_at_kill} in their "
            "sleep, the rest queued",
            probe,
            timeout=self.args.timeout,
            interval=1,
        )

    def _pinned_batch(
        self,
        run,
        *,
        final,
        cap=2,
        n=4,
        sleeps=None,
        brief_fn=None,
        create=True,
        window=None,
    ):
        if create:
            run.create_session(cap=cap)
            run.warm_up(cap=cap)
        sleeps = self.args.pk_sleeps if sleeps is None else sleeps
        if len(sleeps) != n:
            raise GateError(f"the per-child values need {n} entries: {sleeps}")
        seq = run.send(
            batch_prompt(
                run.tag("batch"),
                n,
                sleeps,
                self.args.child_type,
                final,
                brief_fn=brief_fn,
            ),
            "batch",
        )
        calls = run.wait_batch(seq, n)
        at_kill = self._pinned_window(
            run, calls, cap=cap, ended_at_kill=1, running_at_kill=2, **(window or {})
        )
        run.facts["children_at_kill"] = at_kill
        return seq, calls, at_kill

    def _check_retired_children(self, run, calls, kill, *, graceful=False):
        """What the retirement left before Resume: no parent result (the dead
        or draining runtime writes none, P2/P4), every child that ran ended
        interrupted:parent_restart (P0b/P4), never cancelled:parent_retired."""
        v = run.verdict
        written = run.results(calls)
        v.check(
            "no parent tool result written before Resume",
            not written,
            [r["tool_call_id"] for r in written],
        )
        children = run.children()
        kill["children_at_retirement"] = [
            (c["subagent_handle"], c["status"], c["subagent_outcome"]) for c in children
        ]
        open_rows = [c["subagent_handle"] for c in children if c["status"] != "ended"]
        v.check("every child row ended by the retirement", not open_rows, open_rows)
        cancelled = [
            c["subagent_handle"]
            for c in children
            if c["subagent_outcome"] == PERSON_END_OUTCOME
        ]
        v.check(
            f"no child ended {PERSON_END_OUTCOME} (recoverable, not cancelled)",
            not cancelled,
            cancelled,
        )
        ran = [
            c
            for c in children
            if c["subagent_status"] != "completed"
            and c["subagent_outcome"] != PARENT_RESTART_NOT_STARTED_OUTCOME
        ]
        v.check(
            f"the interrupted children ended {PARENT_RESTART_OUTCOME}",
            bool(ran)
            and all(c["subagent_outcome"] == PARENT_RESTART_OUTCOME for c in ran),
            [(c["subagent_handle"], c["subagent_outcome"]) for c in ran],
        )
        if graceful:
            log = run.runtime_log(kill["pod"])
            handed = [line for line in log.splitlines() if HANDOVER_LOG in line]
            kill["handover_log_lines"] = handed
            v.check(
                "the draining runtime ended its children for the successor (P4 log)",
                bool(handed),
                handed[:4],
            )
        else:
            causes = [
                (c["context"] or {}).get("cause")
                for c in kill.get("retirement_contexts_seen", [])
            ]
            if causes:
                v.check(
                    "the retirement names cause runtime_lost (P0b)",
                    "runtime_lost" in causes,
                    causes,
                )
            else:
                v.warn("the pending retirement context was never sampled")
        orchestrator = run.orchestrator_log()
        refused = [
            line
            for line in orchestrator.splitlines()
            if RECEIPT_REFUSED_LOG in line and run.thread in line
        ]
        v.check(
            "no 'Pinned retirement receipt refused' for the session (P0)",
            not refused,
            refused[:3],
        )
        if not graceful:
            v.check(
                "the stale-agent detector retired the offline runtime (P0)",
                DETECTOR_RETIRED_LOG in orchestrator,
            )

    def _assert_pinned_recovered_batch(
        self, run, *, seq, calls, expected, kill, settler, second=None
    ):
        v = run.verdict
        run.facts["expected_classes"] = expected
        children = run.children()
        results = run.results(calls)
        per_call = Counter(r["tool_call_id"] for r in results)
        v.check(
            "exactly one result per delegate call",
            len(results) == len(calls) and all(per_call[c] == 1 for c in calls),
            dict(per_call),
        )
        counts = run.tool_row_counts(calls)
        v.check(
            "one tool row per tool_call_id, rewound rows included (P2)",
            all(counts.get(c) == 1 for c in calls),
            counts,
        )
        actual = {
            r["tool_call_id"]: recovery_class(r) or "live-written" for r in results
        }
        mismatched = {
            c: (expected[c], actual.get(c))
            for c in calls
            if actual.get(c) != expected[c]
        }
        v.check(
            "each result has the class its child had when the runtime died",
            not mismatched,
            {"mismatched": mismatched, "classes": actual},
        )
        stopped = [
            r["tool_call_id"] for r in results if STOPPED_MARKER in (r["content"] or "")
        ]
        v.check("no STOPPED result (a kill is not a Stop)", not stopped, stopped)
        conts = run.continuations()
        v.check("exactly one continuation", len(conts) == 1, len(conts))
        superseding = [
            d for d in run.subagent_deliveries() if d["supersedes_input_seq"] == seq
        ]
        v.check(
            "exactly one subagent delivery supersedes the original input",
            len(superseding) == 1,
            superseding,
        )
        v.check(
            "the continuation's delivery is settled",
            len(superseding) == 1 and superseding[0]["state"] == "settled",
            [d["state"] for d in superseding],
        )
        if conts:
            metrics = (conts[0].get("metrics") or {}).get(RECOVERY_METRICS_KEY) or {}
            want = {
                "calls": len(calls),
                "interrupted": sum(1 for c in calls if expected[c] == "interrupted"),
                "not_started": sum(1 for c in calls if expected[c] == "not_started"),
            }
            got = {key: metrics.get(key) for key in want}
            v.check(
                "continuation counts match the batch",
                got == want,
                {"got": got, "want": want},
            )
        finals = run.final_answers(seq)
        v.check(
            "exactly one final answer (the model answers once)",
            len(finals) == 1,
            len(finals),
        )
        if finals and conts:
            v.check(
                "the final answer follows the continuation",
                finals[-1]["seq"] > conts[0]["seq"],
            )
        batch_children = [c for c in children if c["parent_tool_call_id"] in calls]
        v.check(
            "every child row of the batch ended",
            all(c["status"] == "ended" for c in batch_children),
            [
                (c["subagent_handle"], c["status"], c["subagent_outcome"])
                for c in batch_children
            ],
        )
        v.check(
            "no child spawned for another call (the model did not re-delegate)",
            len(children) == len(batch_children),
            len(children) - len(batch_children),
        )
        grace = sql(f"SELECT {lit(kill['db_ts'])}::timestamptz + interval '10 seconds'")
        late = run.audit_count("subagent", after_db_ts=grace)
        v.check("no child provider call after the kill", late == 0, late)
        deliveries = run.assert_not_parked()["deliveries"]
        source = [d for d in deliveries if d["seq"] == seq]
        v.check(
            "the source input is settled",
            len(source) == 1 and source[0]["state"] == "settled",
            source,
        )
        open_ = [d for d in deliveries if d["state"] not in ("settled", "cancelled")]
        v.check("no delivery left open or parked", not open_, open_)
        settled_lines = [
            line
            for line in run.runtime_log(settler["pod"]).splitlines()
            if BATCH_SETTLED_LOG in line
        ]
        run.facts["batch_settle_log"] = settled_lines
        v.check(
            f"the successor {settler['pod']} settled the turn as one batch",
            bool(settled_lines),
            settled_lines[:2],
        )
        recovery_calls = run.main_calls_with(CONTINUATION_MARKER)
        run.facts["recovery_main_call_ids"] = [c["id"] for c in recovery_calls]
        if second is not None:
            v.check(
                "the parent's provider called once or twice after recovery (audit)",
                1 <= len(recovery_calls) <= 2,
                len(recovery_calls),
            )
            served_again = [
                c
                for c in recovery_calls
                if sql(
                    f"SELECT {lit(c['timestamp'])}::timestamptz > "
                    f"{lit(second['db_ts'])}::timestamptz"
                )
                == "t"
            ]
            v.check(
                "the admitted continuation was served once after the second kill",
                len(served_again) == 1,
                [c["id"] for c in served_again],
            )
        else:
            v.check(
                "the parent's provider called once after recovery (audit)",
                len(recovery_calls) == 1,
                len(recovery_calls),
            )
        if recovery_calls:
            problems = each_report_once(recovery_calls[-1]["messages"] or [], results)
            v.check(
                "each report exactly once in that call's provider input",
                not problems,
                problems,
            )

    # -- PK0 -------------------------------------------------------------------
    def scenario_pk0(self, run):
        """Preflight on a live pinned session: lane, image, the attach
        advertisement (fanout=True), the offer, and a 2-call batch whose
        children run at the same time."""
        cap = 2
        info = run.create_session(cap=cap)
        v = run.verdict
        ad = info["advertisement"]
        v.check(
            "attach advertisement batch_settle=True fanout=True",
            ad["settle"] == "True" and ad["fanout"] == "True",
            ad,
        )
        run.warm_up(cap=cap)
        seq = run.send(
            batch_prompt(
                run.tag("batch"),
                2,
                self.args.pk0_sleep,
                self.args.child_type,
                short_final("PK0-DONE"),
            ),
            "batch",
        )
        calls = run.wait_batch(seq, 2)
        wait_for("pk0: answered", lambda: run.answered(seq), timeout=self.args.timeout)
        run.wait_audit_settled()
        children = run.children()
        v.check(
            "two child rows, both completed",
            len(children) == 2
            and all(c["subagent_status"] == "completed" for c in children),
            [(c["subagent_handle"], c["subagent_outcome"]) for c in children],
        )
        if len(children) == 2:
            overlap = sql(
                "SELECT max(created_at) < min(ended_at) FROM threads "
                f"WHERE parent_thread_id='{run.thread}' AND kind='subagent'"
            )
            v.check("the two children ran at the same time", overlap == "t")
        results = run.results(calls)
        v.check(
            "one live result per call, no refusal",
            len(results) == 2
            and not any(FANOUT_REFUSAL in (r["content"] or "") for r in results)
            and not any(recovery_class(r) for r in results),
            [r["tool_call_id"] for r in results],
        )
        v.check("no continuation", not run.continuations())
        v.check("exactly one final answer", len(run.final_answers(seq)) == 1)

    # -- PK1 -------------------------------------------------------------------
    def scenario_pk1(self, run):
        """A live batch: 4 children under cap 2 run in waves; one answer."""
        n, cap, sleep_s = 4, 2, self.args.pk1_sleep
        info = run.create_session(cap=cap)
        run.warm_up(cap=cap)
        sampler = MemorySampler(
            run.dir / "memory.jsonl",
            self.args.sample_interval,
            selector=f"{PINNED_THREAD_LABEL}={run.thread}",
            container=PINNED_CONTAINER,
        )
        sampler.sample_once()
        baseline = {s["pod"]: s for s in sampler.samples}
        sampler.start()
        max_running = 0
        try:
            seq = run.send(
                batch_prompt(
                    run.tag("batch"),
                    n,
                    sleep_s,
                    self.args.child_type,
                    short_final("PK1-DONE"),
                ),
                "batch",
            )
            calls = run.wait_batch(seq, n)

            def finished():
                nonlocal max_running
                run.check_refusal(calls)
                running = sum(1 for c in run.children() if c["status"] != "ended")
                if running > cap:
                    raise GateFailure(f"{running} children run at once, cap {cap}")
                max_running = max(max_running, running)
                return run.answered(seq)

            wait_for(
                "pk1: batch finished and answered",
                finished,
                timeout=self.args.timeout,
                interval=1,
            )
        finally:
            sampler.stop()
        run.wait_audit_settled()
        v = run.verdict
        children = run.children()
        v.check(
            f"{n} child rows, all completed",
            len(children) == n
            and all(c["subagent_status"] == "completed" for c in children),
            [(c["subagent_handle"], c["subagent_outcome"]) for c in children],
        )
        v.check(f"at most {cap} ran at once (sampled)", max_running <= cap, max_running)
        waves = one(
            "SELECT (array_agg(created_at ORDER BY created_at))[3] >= "
            "(SELECT min(ended_at) FROM threads WHERE parent_thread_id="
            f"'{run.thread}' AND kind='subagent') AS third_after_first_end "
            f"FROM threads WHERE parent_thread_id='{run.thread}' AND kind='subagent'"
        )
        v.check(
            "the third child started only after one of the first wave ended",
            bool(waves and waves["third_after_first_end"]),
            waves,
        )
        results = run.results(calls)
        counts = run.tool_row_counts(calls)
        v.check(
            "one live result per call (no recovery class)",
            len(results) == n
            and all(counts.get(c) == 1 for c in calls)
            and not any(recovery_class(r) for r in results),
            counts,
        )
        v.check("no continuation", not run.continuations())
        v.check("exactly one final answer", len(run.final_answers(seq)) == 1)
        window = one(
            "SELECT extract(epoch FROM min(created_at)) AS start, "
            "extract(epoch FROM max(ended_at)) AS finish, "
            "sum(total_tokens) AS child_tokens FROM threads "
            f"WHERE parent_thread_id='{run.thread}' AND kind='subagent'"
        )
        start, finish = float(window["start"] or 0), float(window["finish"] or 0)
        in_batch = [s for s in sampler.samples if start - 2 <= s["t"] <= finish + 2]
        measurement = {
            "children": n,
            "cap": cap,
            "sleep_s": sleep_s,
            "max_running_observed": max_running,
            "batch_duration_s": round(finish - start, 1) if finish else None,
            "pod": info["pod"],
            "memory_baseline_mib": {
                p: round(s["current"] / 2**20, 1) for p, s in baseline.items()
            },
            "memory_peak_sampled_mib": round(
                max((s["current"] for s in in_batch), default=0) / 2**20, 1
            ),
            "cgroup_memory_peak_mib": round(
                max((s["peak"] for s in sampler.samples), default=0) / 2**20, 1
            ),
            "oom_kills": max((s["oom_kill"] for s in sampler.samples), default=0),
            "child_provider_calls": run.audit_count("subagent"),
            "child_tokens": int(window["child_tokens"] or 0),
        }
        run.facts["measurement"] = measurement
        run.dump("measurement.json", measurement)
        print("measurement: " + json.dumps(measurement), flush=True)

    # -- PK2 / PK4 -------------------------------------------------------------
    def scenario_pk2(self, run, *, pk4=False, end_after=False):
        """SIGKILL mid-batch (1 ended, 2 running, 1 queued) -> the detector
        retires the runtime (P0, P0b) -> Resume -> the successor settles.
        ``end_after`` (PK2e): then a person's End of the recovered life."""
        done = "PK4-DONE" if pk4 else "PK2-DONE"
        final = long_final(done) if pk4 else short_final(done)
        seq, calls, at_kill = self._pinned_batch(run, final=final)
        status_at_kill = {c["parent_tool_call_id"]: c["status"] for c in at_kill}
        expected = {
            c: {"ended": "completed", None: "not_started"}.get(
                status_at_kill.get(c), "interrupted"
            )
            for c in calls
        }
        kill = run.kill("one ended, two running, one queued", self.args.pinned_kill)
        run.wait_runtime_dead(kill)
        run.wait_retired(kill)
        self._check_retired_children(run, calls, kill)
        successor = run.resume(kill)
        second = None
        if pk4:

            def recovery_streaming():
                conts = run.continuations()
                if not conts:
                    return False
                if run.final_answers(seq):
                    raise GateFailure(
                        "the recovery turn answered before it could be killed"
                    )
                streamed = run.stream_events_after(
                    f"SELECT created_at FROM thread_messages WHERE id='{conts[0]['id']}'"
                )
                return conts if streamed else False

            wait_for(
                "pk4: the continuation's answer streaming on the successor",
                recovery_streaming,
                timeout=self.args.timeout,
                interval=1,
            )
            second = run.kill("continuation answer streaming", self.args.pinned_kill)
            run.wait_runtime_dead(second)
            run.wait_retired(second)
            third = run.resume(second)
        wait_for(
            f"{run.name}: final answer after recovery",
            lambda: run.answered(seq),
            timeout=self.args.timeout,
        )
        run.wait_audit_settled()
        self._assert_pinned_recovered_batch(
            run,
            seq=seq,
            calls=calls,
            expected=expected,
            kill=kill,
            settler=successor,
            second=second,
        )
        if pk4:
            v = run.verdict
            cont = [
                d for d in run.subagent_deliveries() if d["supersedes_input_seq"] == seq
            ]
            admissions = [
                d["admission_count"]
                for d in run.deliveries()
                if d["source"] == "subagent" and d["supersedes_input_seq"] == seq
            ]
            v.check(
                "the continuation was admitted twice (P3 counts both)",
                admissions == [2],
                {"admission_count": admissions, "deliveries": cont},
            )
            lines = [
                m.groupdict()
                for m in STALE_ADMISSION_RE.finditer(run.runtime_log(third["pod"]))
                if m.group("thread") == run.thread
            ]
            run.facts["stale_admission_log"] = lines
            v.check(
                f"the third runtime {third['pod']} served the admitted event again "
                "once (P3 log)",
                any(x["served"] == "1" and x["parked"] == "0" for x in lines),
                lines,
            )
        if end_after:
            # The successor recovered the batch at attach: its handles must
            # settle so that a person's End can close the life (54ac1ea99).
            self._person_end(run, "person's End after the recovery")
            self._no_runtime_error_rows(run, "after the End")

    def scenario_pk4(self, run):
        self.scenario_pk2(run, pk4=True)

    # -- PK3 -------------------------------------------------------------------
    def scenario_pk3(self, run):
        """A graceful pod deletion mid-batch hands the batch over (P4, D7)."""
        seq, calls, at_kill = self._pinned_batch(run, final=short_final("PK3-DONE"))
        kill = run.kill("one ended, two running, one queued", "graceful")
        run.wait_runtime_dead(kill)
        run.wait_retired(kill)
        run.facts["graceful_retired_status"] = kill["retired_status"]
        run.verdict.check(
            f"the session ended or was suspended ({kill['retired_status']})",
            kill["retired_status"] in ("ended", "suspended"),
        )
        self._check_retired_children(run, calls, kill, graceful=True)
        by_call = {c["parent_tool_call_id"]: c for c in run.children()}
        expected = {c: expected_class_from_row(by_call.get(c)) for c in calls}
        successor = run.resume(kill)
        wait_for(
            "pk3: final answer after recovery",
            lambda: run.answered(seq),
            timeout=self.args.timeout,
        )
        run.wait_audit_settled()
        self._assert_pinned_recovered_batch(
            run, seq=seq, calls=calls, expected=expected, kill=kill, settler=successor
        )

    # -- PK5 -------------------------------------------------------------------
    def _switch_lanes(self, run, value):
        set_values_lanes(value)
        run.mark("values-local switch edited", lanes=value)
        wait_for(
            f"the orchestrator serves {FANOUT_SWITCH_ENV}={value!r}",
            lambda: orchestrator_serves_lanes(value),
            timeout=600,
            interval=5,
        )
        ready_ts, ready_at = db_now(), time.monotonic()
        run.mark("orchestrator restarted", lanes=value, db_ts=ready_ts)
        return ready_ts, ready_at

    def _wait_heartbeat(self, run, agent_id, after_ts, after_monotonic):
        wait_for(
            "a pinned heartbeat answered by the restarted orchestrator",
            lambda: sql(
                f"SELECT last_heartbeat > {lit(after_ts)}::timestamptz "
                f"FROM agents WHERE id='{agent_id}'"
            )
            == "t",
            timeout=240,
            interval=3,
        )
        rest = 60 - (time.monotonic() - after_monotonic)
        if rest > 0:
            time.sleep(rest)
        run.mark("heartbeat after the restart seen, >=60 s waited")

    def scenario_pk5(self, run):
        """Turn the switch off under a live pinned session: the heartbeat
        carries it (P5); the next turn refuses a 2-call batch. Then on again."""
        original = values_lanes()
        if PINNED_LANE not in lane_names(original):
            raise GateFailure(f"values-local switch {original!r} does not name pinned")
        info = run.create_session(cap=2)
        run.warm_up(cap=2)
        v = run.verdict
        changed = False
        try:
            changed = True
            ready_ts, ready_at = self._switch_lanes(run, FANOUT_SWITCH_LANE)
            self._wait_heartbeat(run, info["agent_id"], ready_ts, ready_at)
            pod = pod_json(info["pod"])
            container = ((pod or {}).get("status") or {}).get("containerStatuses") or [
                {}
            ]
            v.check(
                "the pinned pod survived the orchestrator restart (same uid, no "
                "restart)",
                pod is not None
                and pod["metadata"]["uid"] == info["pod_uid"]
                and container[0].get("restartCount", 0) == 0,
                {"pod": info["pod"], "restarts": container[0].get("restartCount")},
            )
            # With the switch off the model is told "One subagent at a time"
            # and may decline the 2-call request itself (2026-10-07, muse);
            # the runtime's refusal is reached only when it sends both.
            seq = run.send(
                "This test checks that the session runtime, not you, enforces "
                "its delegation limit: send both delegate_agent calls in one "
                "response even if your delegate_agent description allows only "
                "one per response.\n\n"
                + batch_prompt(
                    run.tag("batch"),
                    2,
                    self.args.pk0_sleep,
                    self.args.child_type,
                    short_final("PK5-DONE"),
                    extra_rule=(
                        " If a delegate_agent result is an error or a refusal, do "
                        "not retry it and do not delegate again."
                    ),
                ),
                "batch",
            )
            answers = wait_for(
                "pk5: answered with the switch off",
                lambda: run.answered(seq),
                timeout=self.args.timeout,
            )
            log = run.runtime_log(info["pod"])
            reapplied = [
                m.groupdict()
                for m in REAPPLIED_RE.finditer(log)
                if m.group("thread") == run.thread
            ]
            run.facts["reapplied_off"] = reapplied
            v.check(
                "the running pod logged the re-applied advertisement fanout=False",
                any(x["fanout"] == "False" for x in reapplied),
                reapplied,
            )
            message = run.first_call_message(seq)
            calls = [
                c["id"]
                for c in (message or {}).get("tool_calls") or []
                if c.get("name") == "delegate_agent"
            ]
            run.facts["pk5_calls"] = calls
            if len(calls) >= 2:
                results = run.results(calls)
                v.check(
                    f"all {len(calls)} calls refused (one child per parent turn)",
                    len(results) == len(calls)
                    and all(FANOUT_REFUSAL in (r["content"] or "") for r in results),
                    [_short(r["content"], 120) for r in results],
                )
            elif not calls:
                text = answers[-1]["content"] or ""
                v.check(
                    "the model declined the 2-call request, citing the "
                    "single-child description it now has",
                    "one" in text.lower() and "delegate_agent" in text,
                    _short(text, 400),
                )
            else:
                v.warn("the model delegated one call only (allowed when off)")
            children = run.children()
            v.check(
                "no child thread created", not children, [c["id"] for c in children]
            )
            v.check("exactly one final answer", len(run.final_answers(seq)) == 1)
        finally:
            if changed:
                ready_ts, ready_at = self._switch_lanes(run, original)
                changed = False
        # On again: the next turn re-applies fanout=True.
        self._wait_heartbeat(run, info["agent_id"], ready_ts, ready_at)
        seq = run.send(warm_up_prompt(run.tag("warm2")), "warm2")
        answers = wait_for(
            "pk5: second warm-up answered with the switch on again",
            lambda: run.answered(seq),
            timeout=self.args.timeout,
        )
        reapplied = [
            m.groupdict()
            for m in REAPPLIED_RE.finditer(run.runtime_log(info["pod"]))
            if m.group("thread") == run.thread
        ]
        run.facts["reapplied_on"] = reapplied
        v.check(
            "switched back: the pod re-applied fanout=True",
            bool(reapplied) and reapplied[-1]["fanout"] == "True",
            reapplied,
        )
        text = answers[-1]["content"] or ""
        if "in a single response" in text:
            v.check("the offer is back in the tool description", True)
        else:
            v.warn("the model paraphrased the fan-out sentence", text)

    # -- PK6a / PK6b / PK6c: a person's End mid-batch ----------------------
    def _person_end(self, run, end_label):
        """A person's End (the cockpit's End anyway: DELETE ?force=true) of
        the current runtime; returns the end record once the retirement
        settled. The End must complete on its own (54ac1ea99, c22c4abe1)."""
        row = run.runtime()
        end = {
            "label": end_label,
            "mode": "end",
            "pod": row["hostname"],
            "pod_uid": None,
            "agent_id": row["agent_id"],
            "generation": row["runtime_generation"],
            "thread_status": row["status"],
            "db_ts": db_now(),
        }
        run.facts["kills"].append(end)
        self.api.call("DELETE", f"/api/persistent/threads/{run.thread}?force=true")
        run.mark("person's End (force)", label=end_label)
        run.wait_retired(end, live_runtime=True)
        run.verdict.check(
            f"the End completes on its own in {end.get('settled_after_s')} s "
            "(no quiescence wedge, no pod deleted)",
            "end_wedged" not in end,
            {
                "settled_after_s": end.get("settled_after_s"),
                "status": end.get("retired_status"),
            },
        )
        causes = [
            (c["context"] or {}).get("cause")
            for c in end.get("retirement_contexts_seen", [])
        ]
        run.verdict.check(
            "the End names no runtime_lost cause", "runtime_lost" not in causes, causes
        )
        return end

    def _no_runtime_error_rows(self, run, label):
        found = rows(
            "SELECT seq, role, tool_call_id, left(content, 200) AS content "
            f"FROM thread_messages WHERE thread_id='{run.thread}' AND ("
            + " OR ".join(f"position({lit(t)} in content) > 0" for t in END_ERROR_TEXTS)
            + ") ORDER BY seq"
        )
        run.verdict.check(
            f"no {' / '.join(END_ERROR_TEXTS)} row in the parent transcript ({label})",
            not found,
            found,
        )

    def _person_end_scenario(self, run, *, variant):
        """PK6a long sleeps, PK6b short steps, PK6c with a background child:
        a person's End mid-batch completes on its own, cancels the live
        children (D4), the old runtime writes nothing into the parent, and
        Resume settles every call once (completed / CANCELLED / NOT STARTED)
        with one continuation and one answer to it (design §8)."""
        v = run.verdict
        cap = 3 if variant == "pk6c" else 2
        background = None
        window = {}
        brief_fn = None
        sleeps = None
        if variant == "pk6b":
            brief_fn, sleeps = short_step_brief, self.args.pk6b_steps
            window = {"require_shell": False}
        run.create_session(cap=cap)
        run.warm_up(cap=cap)
        if variant == "pk6c":
            tag = run.tag("bg")
            bg_seq = run.send(
                f"[{tag}] Automated infrastructure test. Call the delegate_agent "
                "tool exactly once, with run_in_background=true and these "
                f'arguments: subagent_type="{self.args.child_type}", '
                f'description="gate {tag} background child", '
                f'prompt="{child_brief(f"{tag}-BG", self.args.pk6c_bg_sleep)}". '
                "Call no other tool. When the tool returns its receipt, reply "
                "with exactly one line: BG-STARTED.",
                "bg",
            )
            wait_for(
                "pk6c: background delegation answered",
                lambda: run.answered(bg_seq),
                timeout=self.args.timeout,
            )

            def background_running():
                kids = run.children()
                if len(kids) > 1:
                    raise GateFailure(f"expected one background child, saw {len(kids)}")
                return kids[0] if kids and kids[0]["in_shell"] else False

            background = wait_for(
                "pk6c: the background child is in its sleep",
                background_running,
                timeout=self.args.timeout,
            )
            run.facts["background_child"] = background
            window = {"only_calls": True, "at_least": True}
        seq, calls, at_end = self._pinned_batch(
            run,
            final=short_final(f"{variant.upper()}-DONE"),
            cap=cap,
            sleeps=sleeps,
            brief_fn=brief_fn,
            create=False,
            window=window,
        )
        end = self._person_end(run, "person's End mid-batch")
        children = run.children()
        batch = [c for c in children if c["parent_tool_call_id"] in calls]
        by_call = {c["parent_tool_call_id"]: c for c in batch}
        live_at_end = [c for c in batch if c["subagent_status"] != "completed"]
        v.check(
            f"the batch children live at the End ended {PERSON_END_OUTCOME} (D4)",
            bool(live_at_end)
            and all(c["subagent_outcome"] == PERSON_END_OUTCOME for c in live_at_end),
            [(c["subagent_handle"], c["subagent_outcome"]) for c in batch],
        )
        counts = run.tool_row_counts(calls)
        v.check(
            "the old runtime wrote nothing into the parent (no tool row for a call)",
            not counts,
            counts,
        )
        self._no_runtime_error_rows(run, "before Resume")
        if background is not None:
            bg = next(c for c in children if c["id"] == background["id"])
            v.check(
                f"the background child ended {PERSON_END_OUTCOME}",
                bg["status"] == "ended"
                and bg["subagent_outcome"] == PERSON_END_OUTCOME,
                (bg["subagent_handle"], bg["status"], bg["subagent_outcome"]),
            )
        expected = {}
        for call in calls:
            child = by_call.get(call)
            if child is None or child["subagent_outcome"] in (
                PARENT_RESTART_NOT_STARTED_OUTCOME,
            ):
                expected[call] = "not_started"
            elif child["subagent_status"] == "completed":
                expected[call] = "completed"
            else:
                expected[call] = "retired"
        run.facts["expected_classes"] = expected
        successor = run.resume(end)
        wait_for(
            f"{run.name}: answered after Resume",
            lambda: run.answered(seq),
            timeout=self.args.timeout,
        )
        run.wait_audit_settled()
        results = run.results(calls)
        counts = run.tool_row_counts(calls)
        v.check(
            "exactly one result per call, rewound rows included",
            len(results) == len(calls) and all(counts.get(c) == 1 for c in calls),
            counts,
        )
        actual = {r["tool_call_id"]: recovery_class(r) for r in results}
        v.check(
            "each result is completed / CANCELLED (retired) / NOT STARTED as its "
            "child was at the End",
            all(actual.get(c) == expected[c] for c in calls),
            {"expected": expected, "actual": actual},
        )
        settled_lines = [
            line
            for line in run.runtime_log(successor["pod"]).splitlines()
            if BATCH_SETTLED_LOG in line
        ]
        run.facts["batch_settle_log"] = settled_lines
        v.check("the successor settled the turn as one batch", bool(settled_lines))
        conts = run.continuations()
        v.check("exactly one continuation", len(conts) == 1, len(conts))
        superseding = [
            d for d in run.subagent_deliveries() if d["supersedes_input_seq"] == seq
        ]
        v.check(
            "one settled delivery supersedes the original input",
            len(superseding) == 1 and superseding[0]["state"] == "settled",
            superseding,
        )
        if conts:
            metrics = (conts[0].get("metrics") or {}).get(RECOVERY_METRICS_KEY) or {}
            want = {
                "calls": len(calls),
                "retired": sum(1 for c in calls if expected[c] == "retired"),
                "not_started": sum(1 for c in calls if expected[c] == "not_started"),
                "interrupted": 0,
            }
            got = {key: metrics.get(key) for key in want}
            v.check(
                "continuation counts match", got == want, {"got": got, "want": want}
            )
            answers = [
                a
                for a in run.final_answers(seq)
                if a["turn_number"] == conts[0]["turn_number"]
            ]
            v.check(
                "exactly one answer to the continuation, after it",
                len(answers) == 1 and answers[0]["seq"] > conts[0]["seq"],
                [(a["seq"], a["turn_number"]) for a in answers],
            )
            run.facts["answers_after_input"] = [
                (a["seq"], a["turn_number"]) for a in run.final_answers(seq)
            ]
        self._no_runtime_error_rows(run, "after Resume")
        v.check(
            "no further child (nothing runs again)",
            len(run.children()) == len(children),
            len(run.children()),
        )
        if background is not None:
            events = rows(
                "SELECT seq, role, turn_number, left(content, 300) AS content, "
                f"created_at FROM thread_messages WHERE thread_id='{run.thread}' "
                f"AND role='event' AND position({lit(background['subagent_handle'])} "
                "in content) > 0 ORDER BY seq"
            )
            run.facts["background_events"] = events
            v.check(
                "the background child's cancellation reached the parent as one event",
                len(events) == 1,
                events,
            )

    def scenario_pk6(self, run):
        """PK6a: long-sleep children."""
        self._person_end_scenario(run, variant="pk6")

    def scenario_pk6b(self, run):
        """PK6b: short-step children (a write boundary every few seconds)."""
        self._person_end_scenario(run, variant="pk6b")

    def scenario_pk6c(self, run):
        """PK6c: a background child running beside the foreground batch."""
        self._person_end_scenario(run, variant="pk6c")

    def scenario_pk2e(self, run):
        """PK2, then a person's End after the recovery (it hung 4/4 before
        54ac1ea99: the recovered handles were never settled)."""
        self.scenario_pk2(run, end_after=True)


# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------


class Gate(PinnedScenarios):
    def __init__(self, args):
        self.args = args
        self.api_user = os.environ.get("SRW_K3D_TEST_USER", "test")
        self.project_id = None  # pinned: resolved by the preflight
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        default = Path(
            os.environ.get("SRW_GATE_EVIDENCE_DIR")
            or Path(tempfile.gettempdir()) / "srw-gates"
        )
        self.out = (
            Path(args.out) if args.out else default / "parallel-subagents" / stamp
        )
        self.api = None
        self.threads = {}
        self.results = {}

    def record_thread(self, name, thread):
        self.threads[name] = thread
        self.out.mkdir(parents=True, exist_ok=True)
        (self.out / "threads.json").write_text(json.dumps(self.threads, indent=1))
        print(f"thread {name}={thread}", flush=True)

    # -- preconditions ---------------------------------------------------------
    def preflight(self, when="before", stage="precondition") -> tuple[bool, dict]:
        """Read-only checks; ``stage`` only labels the printed lines."""
        args = self.args
        problems, warnings, facts = [], [], {}

        def need(ok, label, detail=None):
            tail = f" — {_short(detail)}" if detail else ""
            print(f"{'PASS' if ok else 'FAIL'} {stage}: {label}{tail}", flush=True)
            if not ok:
                problems.append(label)

        def warn(label, detail=None):
            tail = f" — {_short(detail)}" if detail else ""
            print(f"WARN {stage}: {label}{tail}", flush=True)
            warnings.append(label)

        try:
            context = command(["kubectl", "config", "current-context"])
        except GateError as exc:
            context = str(exc)
        need(context == "k3d-srw", "kubectl context is k3d-srw", context)

        blocking, advisory = other_runs()
        need(
            not blocking, "no other live gate, VM gate or whole-suite pytest", blocking
        )
        if advisory:
            warn("parallel pytest runs by file on this host (memory)", advisory)
        try:
            vmis = command(
                K[:2] + ["get", "vmi", "-A", "--no-headers", "--ignore-not-found"]
            )
        except GateError:
            vmis = ""
        need(not vmis.strip(), "no running KubeVirt VMIs (VM gate)", vmis[:200])
        try:
            meminfo = Path("/proc/meminfo").read_text()
            available = int(re.search(r"MemAvailable:\s+(\d+)", meminfo).group(1))
            facts["host_mem_available_gib"] = round(available / 2**20, 1)
            if available < 6 * 2**20:
                warn("host MemAvailable below 6 GiB (the k3d node shares it)")
        except (OSError, AttributeError):
            pass

        pinned = getattr(args, "lane", "stateless") == PINNED_LANE
        orchestrator_files = PINNED_ORCHESTRATOR_FILES if pinned else ORCHESTRATOR_FILES
        switch_lane = PINNED_LANE if pinned else FANOUT_SWITCH_LANE
        feature = sorted(
            set(AGENT_FILES + ORCHESTRATOR_FILES + CONFIG_FILES)
            | (set(PINNED_AGENT_FILES + PINNED_ORCHESTRATOR_FILES) if pinned else set())
        )
        expert_file, child_file, roster_problem = roster_entry_paths(
            args.expert, args.child_type
        )
        need(
            roster_problem is None,
            f"expert {args.expert!r} delegates to {args.child_type!r} with a "
            "shell_execute tool",
            roster_problem,
        )
        configs = CONFIG_FILES + [p for p in (expert_file, child_file) if p]
        try:
            changed = dirty(sorted(set(feature + configs)))
        except GateError as exc:
            changed = [str(exc)]
        need(
            not changed,
            "no uncommitted change in the feature files (never run while source "
            "changes)",
            changed,
        )
        try:
            advertised = head_bytes(ADVERTISEMENT_MODULE).decode()
        except GateError:
            advertised = ""
        need(
            f'"{FANOUT_CLAIM_KEY}"' in advertised
            and f'"{SETTLE_CONTRACT_KEY}"' in advertised,
            f"HEAD defines the claim keys {FANOUT_CLAIM_KEY} and "
            f"{SETTLE_CONTRACT_KEY} (WP3c committed)",
        )

        try:
            executor = deployment(EXECUTOR_DEPLOYMENT)
            template_image = executor["spec"]["template"]["spec"]["containers"][0][
                "image"
            ]
            pods = executor_pods()
            facts["executor_image"] = template_image
            facts["executor_pods"] = pods
            facts["executor_template_hashes"] = sorted(
                {p["template_hash"] for p in pods}
            )
            facts["drain_budget_s"] = drain_budget_seconds()
            need(
                rollout_converged(executor)
                and len(pods) == executor["spec"]["replicas"]
                and all(p["image"] == template_image for p in pods),
                "executor rollout converged, every pod on the current template",
                [(p["name"], p["image"]) for p in pods],
            )
            for pod in pods:
                bad = served_mismatches(
                    pod["name"], EXECUTOR_CONTAINER, AGENT_FILES + configs
                )
                need(not bad, f"{pod['name']} serves the HEAD bytes", bad)
            if pods:
                # The k3d node pins its DNS upstream at start; after a network
                # move every external name fails (2026-09-29, K1 run 1).
                resolved = command(
                    K
                    + ["exec", pods[0]["name"], "-c", EXECUTOR_CONTAINER, "--"]
                    + ["python", "-c", _DNS_PROGRAM],
                    timeout=60,
                )
                need(
                    resolved.startswith("ok"),
                    "external DNS resolves from an executor",
                    resolved,
                )
            orchestrator = deployment(ORCHESTRATOR_DEPLOYMENT)
            need(rollout_converged(orchestrator), "orchestrator rollout converged")
            bad = served_mismatches(
                f"deploy/{ORCHESTRATOR_DEPLOYMENT}",
                ORCHESTRATOR_CONTAINER,
                orchestrator_files + configs,
            )
            need(not bad, "orchestrator serves the HEAD bytes", bad)
            lanes = command(
                K
                + ["exec", f"deploy/{ORCHESTRATOR_DEPLOYMENT}"]
                + ["-c", ORCHESTRATOR_CONTAINER, "--", "sh", "-c"]
                + [f'printenv {FANOUT_SWITCH_ENV} || echo "<unset>"']
            )
            facts["fanout_switch"] = lanes
            names = {x.strip().lower() for x in lanes.split(",")}
            need(
                switch_lane in names,
                f"operator switch {FANOUT_SWITCH_ENV} names {switch_lane!r} "
                f"(values-local: {FANOUT_SWITCH_VALUES_KEY})",
                repr(lanes),
            )
        except GateError as exc:
            need(False, "cluster inspection", str(exc))

        try:
            busy = sql(
                "SELECT count(*) FROM run_queue WHERE state IN ('queued','leased') "
                "AND unit_kind <> 'bg_task'"
            )
            if pinned:
                # A pinned session never uses the queue; others' turns only
                # add load.
                if busy != "0":
                    warn("stateless turns in flight (load only)", busy)
            else:
                need(
                    busy == "0",
                    "run queue idle (no other session turn in flight)",
                    busy,
                )
            live = sql(
                "SELECT coalesce(string_agg(id::text, ','), '') FROM threads "
                "WHERE kind='subagent' AND status <> 'ended' "
                "AND subagent_status IN ('queued','running') "
                "AND coalesce(last_activity, created_at) > now() - interval '30 minutes'"
            )
            need(not live, "no live child rows anywhere", live)
        except GateError as exc:
            need(False, "database reachable", str(exc))
        if pinned:
            self.pinned_preflight(need, warn, facts)

        facts["problems"], facts["warnings"] = problems, warnings
        facts["when"], facts["at"] = when, utc_now()
        return not problems, facts

    # -- scenario harness ------------------------------------------------------
    def settle_pool(self):
        """A killed executor's replacement may still be starting."""

        def settled():
            executor = deployment(EXECUTOR_DEPLOYMENT)
            pods = executor_pods(running_only=False)
            return rollout_converged(executor) and all(
                p["phase"] == "Running" and not p["terminating"] for p in pods
            )

        try:
            wait_for("executor pool settled", settled, timeout=300, interval=5)
        except GateFailure as exc:
            print(f"WARN {exc}", flush=True)

    def run(self, name):
        print(f"\n==== {name.upper()} ====", flush=True)
        self.settle_pool()
        ok, facts = self.preflight(f"before {name}")
        self.out.mkdir(parents=True, exist_ok=True)
        (self.out / f"preflight-{name}.json").write_text(
            json.dumps(facts, indent=1, default=str)
        )
        if not ok:
            print(f"FAIL {name}: preconditions not met; scenario not started")
            self.results[name] = "NOT RUN"
            return
        run = (PinnedRun if name in PINNED_SCENARIOS else Run)(self, name)
        run.facts["preflight"] = facts
        extra = getattr(run, "extra_collectors", [])
        if name != "measure":  # each measurement size follows its own logs
            run.logs.start()
        for collector in extra:
            collector.start()
        try:
            getattr(self, f"scenario_{name}")(run)
        except (GateFailure, GateError) as exc:
            run.verdict.check("scenario completed", False, str(exc))
        except Exception as exc:  # a script bug must still leave evidence
            import traceback

            run.verdict.check(
                "scenario completed", False, f"{exc!r}\n{traceback.format_exc()}"
            )
        except KeyboardInterrupt:
            run.verdict.check(
                "scenario completed", False, "interrupted by the operator"
            )
            raise
        finally:
            try:
                run.collect()
            finally:
                run.logs.stop()
                for collector in extra:
                    collector.stop()
                if not self.args.keep_sessions:
                    run.end()
        _after_ok, after = self.preflight(f"after {name}", stage="postflight")
        (self.out / f"postflight-{name}.json").write_text(
            json.dumps(after, indent=1, default=str)
        )
        # A rollout during a run is expected (the kill replaces a pod); a
        # changed file or a pod serving other bytes is not.
        changed = [
            p for p in after["problems"] if "HEAD bytes" in p or "uncommitted" in p
        ]
        if changed:
            self.results[name] = "INVALID (the served code changed during the run)"
        else:
            self.results[name] = "PASS" if run.verdict.ok else "FAIL"
        run.dump("verdict.json", {"ok": run.verdict.ok, "checks": run.verdict.items})
        print(f"==== {name.upper()}: {self.results[name]} — evidence {run.dir}")

    # -- K1 --------------------------------------------------------------------
    def scenario_k1(self, run):
        n = self.args.k1_calls
        run.create_session(permission_mode="supervised")
        run.warm_up(cap=None)
        seq = run.send(
            batch_prompt(
                run.tag("batch"), n, 30, self.args.child_type, short_final("K1-DONE")
            ),
            "batch",
        )
        calls = run.wait_batch(seq, n)

        def cards_pending():
            run.assert_not_parked()
            pending = [
                p
                for p in run.permissions()
                if p["status"] == "pending"
                and p["tool_name"] == "delegate_agent"
                and p["tool_call_id"] in calls
            ]
            if len(run.children()):
                raise GateFailure("a child started before its card was answered")
            return pending if len(pending) == n else False

        before = wait_for(
            f"k1: {n} approval cards pending", cards_pending, timeout=self.args.timeout
        )
        pre_kill = {p["id"] for p in before}
        run.facts["cards_before_kill"] = before
        kill = run.kill_lease_holder("approval cards pending")
        successor = run.wait_successor(kill)
        successor_at = time.monotonic()
        declined = run.facts.setdefault("declined", [])

        def decline_and_wait():
            for card in run.permissions():
                if card["status"] != "pending":
                    continue
                fresh = card["id"] not in pre_kill
                if not fresh and time.monotonic() - successor_at < 30:
                    continue  # give the replay 30 s to raise its own cards
                try:
                    self.api.call(
                        "POST",
                        f"/api/persistent/threads/{run.thread}/approve/{card['id']}",
                        {"decision": "deny"},
                        ok=(200,),
                    )
                    declined.append({**card, "orphan_from_killed_pod": not fresh})
                    run.mark("declined card", tool_call_id=card["tool_call_id"])
                except GateError as exc:
                    if "409" not in str(exc) and "404" not in str(exc):
                        raise
            return run.answered(seq)

        wait_for(
            "k1: final answer after the declined replay",
            decline_and_wait,
            timeout=self.args.timeout,
        )
        run.wait_audit_settled()
        v = run.verdict
        finals = run.final_answers(seq)
        v.check("exactly one final answer", len(finals) == 1, len(finals))
        children = run.children()
        v.check("zero child rows", not children, [c["id"] for c in children])
        v.check(
            "zero child provider calls (audit)",
            run.audit_count("subagent") == 0,
            run.audit_count("subagent"),
        )
        v.check("no parked unit", run.assert_not_parked()["state"] == "done")
        v.check("no continuation", not run.continuations())
        orphans = [
            c
            for c in run.permissions()
            if c["id"] in pre_kill and c["status"] == "pending"
        ]
        if orphans or any(d["orphan_from_killed_pod"] for d in declined):
            v.warn(
                "approval cards of the killed executor stayed pending",
                [c["tool_call_id"] for c in orphans] or "declined by the gate",
            )
        run.facts["successor"] = successor

    # -- K2 / K4 ---------------------------------------------------------------
    def scenario_k2(self, run, *, k4=False):
        n, cap = 4, 2
        sleep_s = self.args.sleep
        if self.args.kill == "drain":
            budget = drain_budget_seconds()
            if sleep_s < budget + 60:
                sleep_s = int(budget + 60)
                run.verdict.warn(
                    f"--kill drain: child sleep stretched to {sleep_s}s so the "
                    f"batch outlasts the {budget:.0f}s drain budget"
                )
        run.create_session(permission_mode="autonomous", cap=cap)
        run.warm_up(cap=cap)
        done = "K4-DONE" if k4 else "K2-DONE"
        final = long_final(done) if k4 else short_final(done)
        seq = run.send(
            batch_prompt(run.tag("batch"), n, sleep_s, self.args.child_type, final),
            "batch",
        )
        calls = run.wait_batch(seq, n)
        wave = self.args.k2_kill_at
        running_at_kill = 2
        ended_at_kill = 2 if wave == "wave2" else 0

        def two_running():
            run.assert_not_parked()
            run.check_refusal(calls)
            children = run.children()
            running = [c for c in children if c["status"] != "ended"]
            ended = [c for c in children if c["status"] == "ended"]
            if len(running) > cap:
                raise GateFailure(f"{len(running)} children run at once, cap {cap}")
            if len(ended) > ended_at_kill:
                raise GateFailure(
                    f"missed the kill window: {len(ended)} children already ended"
                )
            in_shell = [c for c in running if c["in_shell"]]
            if len(ended) == ended_at_kill and len(in_shell) == running_at_kill:
                return children
            return False

        at_kill = wait_for(
            f"k2: two child rows running in their sleep ({wave})",
            two_running,
            timeout=self.args.timeout,
            interval=1,
        )
        status_at_kill = {c["parent_tool_call_id"]: c["status"] for c in at_kill}
        run.facts["children_at_kill"] = at_kill
        kill = run.kill_lease_holder(f"two children running ({wave})")
        run.wait_successor(kill)

        if k4:

            def recovery_turn_streaming():
                conts = run.continuations()
                if not conts:
                    return False
                if run.final_answers(seq):
                    raise GateFailure(
                        "the recovery turn answered before it could be killed"
                    )
                state = run.assert_not_parked()
                if (
                    state["state"] != "leased"
                    or state["leased_by"] == kill["pod"]
                    or int(state["lease_token"]) <= kill["lease_token"]
                ):
                    return False
                streamed = run.stream_events_after(
                    f"SELECT created_at FROM thread_messages WHERE id='{conts[0]['id']}'"
                )
                return state if streamed else False

            wait_for(
                "k4: recovery turn streaming on the successor",
                recovery_turn_streaming,
                timeout=self.args.timeout,
                interval=1,
            )
            second = run.kill_lease_holder("recovery turn in flight")
            run.wait_successor(second)
        else:
            second = None

        wait_for(
            f"{run.name}: final answer after recovery",
            lambda: run.answered(seq),
            timeout=self.args.timeout,
        )
        run.wait_audit_settled()
        self._assert_recovered_batch(
            run,
            seq=seq,
            calls=calls,
            status_at_kill=status_at_kill,
            kill=kill,
            k4=k4,
            second=second,
        )

    def scenario_k4(self, run):
        self.scenario_k2(run, k4=True)

    def _assert_recovered_batch(
        self, run, *, seq, calls, status_at_kill, kill, k4, second=None
    ):
        v = run.verdict
        drain = self.args.kill == "drain"
        children = run.children()
        by_call = {c["parent_tool_call_id"]: c for c in children}

        def final_class(call):
            # The class the settle must give a call, from the child's end state.
            child = by_call.get(call)
            if child is None:
                return "not_started"
            if child["subagent_status"] == "interrupted":
                return "interrupted"
            return "completed"

        if drain:
            # The old executor kept working through the drain budget; WP3d then
            # left the children as they were at the release. What they were at
            # the delete no longer decides the class.
            expected = {c: final_class(c) for c in calls}
        else:
            expected = {
                c: {"ended": "completed", None: "not_started"}.get(
                    status_at_kill.get(c), "interrupted"
                )
                for c in calls
            }
        run.facts["expected_classes"] = expected

        results = run.results(calls)
        per_call = Counter(r["tool_call_id"] for r in results)
        v.check(
            "exactly one result per delegate call",
            len(results) == len(calls) and all(per_call[c] == 1 for c in calls),
            dict(per_call),
        )
        actual = {
            r["tool_call_id"]: recovery_class(r) or "live-written" for r in results
        }
        mismatched = {
            c: (expected[c], actual.get(c))
            for c in calls
            if actual.get(c) != expected[c]
            and not (expected[c] == "completed" and actual.get(c) == "live-written")
        }
        v.check(
            "each result has the class its child had when the executor died",
            not mismatched,
            {"mismatched": mismatched, "classes": actual},
        )
        stopped = [
            r["tool_call_id"] for r in results if STOPPED_MARKER in (r["content"] or "")
        ]
        v.check("no STOPPED result (a kill is not a Stop)", not stopped, stopped)

        conts = run.continuations()
        v.check(
            "exactly one continuation (never a second for the input)",
            len(conts) == 1,
            len(conts),
        )
        superseding = [
            d for d in run.subagent_deliveries() if d["supersedes_input_seq"] == seq
        ]
        v.check(
            "exactly one subagent delivery supersedes the original input",
            len(superseding) == 1,
            superseding,
        )
        v.check(
            "the continuation's delivery is settled",
            len(superseding) == 1 and superseding[0]["state"] == "settled",
            [d["state"] for d in superseding],
        )
        if conts:
            metrics = (conts[0].get("metrics") or {}).get(RECOVERY_METRICS_KEY) or {}
            want = {
                "calls": len(calls),
                "interrupted": sum(1 for c in calls if expected[c] == "interrupted"),
                "not_started": sum(1 for c in calls if expected[c] == "not_started"),
            }
            got = {key: metrics.get(key) for key in want}
            v.check(
                "continuation counts match the batch",
                got == want,
                {"got": got, "want": want},
            )

        finals = run.final_answers(seq)
        v.check("exactly one final answer", len(finals) == 1, len(finals))
        if finals and conts:
            v.check(
                "the final answer follows the continuation",
                finals[-1]["seq"] > conts[0]["seq"],
            )

        batch_children = [c for c in children if c["parent_tool_call_id"] in calls]
        if drain:
            label = "every child row of the batch ended"
            ok = all(c["status"] == "ended" for c in batch_children)
        else:
            with_rows = sum(1 for c in calls if status_at_kill.get(c) is not None)
            label = f"{with_rows} child rows for the batch, every one ended"
            ok = len(batch_children) == with_rows and all(
                c["status"] == "ended" for c in batch_children
            )
        v.check(
            label,
            ok,
            [
                (c["subagent_handle"], c["status"], c["subagent_outcome"])
                for c in batch_children
            ],
        )
        v.check(
            "no child spawned for another call (the model did not re-delegate)",
            len(children) == len(batch_children),
            len(children) - len(batch_children),
        )
        # After the process died no child may call its provider again. Under a
        # hard kill that is the delete; under drain it is the release (the
        # successor's claim). The archive write of a call finished just before
        # can land a moment later, hence ten seconds of grace.
        died = kill["successor_db_ts"] if drain else kill["db_ts"]
        grace = sql(f"SELECT {lit(died)}::timestamptz + interval '10 seconds'")
        late = run.audit_count("subagent", after_db_ts=grace)
        v.check("no child provider call after the executor died", late == 0, late)
        if drain:
            released = any(
                RELEASE_LOG in path.read_text(errors="replace")
                for path in (run.dir / "logs").glob(f"{kill['pod']}.*.log")
            )
            v.check(
                "the draining executor released the claim (WP3d log line)",
                released,
                kill["pod"],
            )

        recovery_calls = run.main_calls_with(CONTINUATION_MARKER)
        run.facts["recovery_main_call_ids"] = [c["id"] for c in recovery_calls]
        if k4:
            # The successor's in-flight call was killed; an unfinished call is
            # never archived, and §1 allows the continuation call twice. The
            # next successor must serve the admitted continuation again
            # (2b7938539): a completed call after the second kill.
            v.check(
                "the parent's provider called once or twice after recovery (audit)",
                1 <= len(recovery_calls) <= 2,
                len(recovery_calls),
            )
            served_again = [
                c
                for c in recovery_calls
                if sql(
                    f"SELECT {lit(c['timestamp'])}::timestamptz > "
                    f"{lit(second['db_ts'])}::timestamptz"
                )
                == "t"
            ]
            v.check(
                "the admitted continuation was served again after the second kill",
                len(served_again) == 1,
                [c["id"] for c in served_again],
            )
        else:
            v.check(
                "the parent's provider called once after recovery (audit)",
                len(recovery_calls) == 1,
                len(recovery_calls),
            )
        if recovery_calls:
            problems = each_report_once(recovery_calls[-1]["messages"] or [], results)
            v.check(
                "each report exactly once in that call's provider input",
                not problems,
                problems,
            )
        state = run.assert_not_parked()
        v.check("no parked unit; queue done", state["state"] == "done", state)

    # -- K3 --------------------------------------------------------------------
    def scenario_k3(self, run):
        n = 4
        run.create_session(permission_mode="autonomous", cap=n)
        run.warm_up(cap=n)
        seq = run.send(
            batch_prompt(
                run.tag("batch"),
                n,
                self.args.k3_sleep,
                self.args.child_type,
                long_final("K3-DONE"),
            ),
            "batch",
        )
        calls = run.wait_batch(seq, n)

        def final_call_streaming():
            run.assert_not_parked()
            run.check_refusal(calls)
            results = run.results(calls)
            if len(results) < n:
                return False
            if run.final_answers(seq):
                raise GateFailure("the final answer landed before the kill")
            streamed = run.stream_events_after(
                f"SELECT max(created_at) FROM thread_messages WHERE "
                f"thread_id='{run.thread}' AND role='tool' "
                f"AND tool_call_id IN {in_list(calls)}"
            )
            return results if streamed else False

        before = wait_for(
            "k3: four results durable and the final call streaming",
            final_call_streaming,
            timeout=self.args.timeout,
            interval=1,
        )
        children_before = run.children()
        run.facts["children_at_kill"] = children_before
        kill = run.kill_lease_holder("final streaming call")
        run.wait_successor(kill)
        wait_for(
            "k3: final answer after the replay",
            lambda: run.answered(seq),
            timeout=self.args.timeout,
        )
        run.wait_audit_settled()
        v = run.verdict
        v.check("no continuation", not run.continuations(), len(run.continuations()))
        v.check("no subagent delivery", not run.subagent_deliveries())
        finals = run.final_answers(seq)
        v.check("exactly one final answer", len(finals) == 1, len(finals))
        after = run.results(calls)
        v.check(
            "the four results are unchanged and live-written",
            [r["id"] for r in after] == [r["id"] for r in before]
            and not any(recovery_class(r) for r in after),
        )
        children = run.children()
        v.check(
            "no further child (same rows, all ended)",
            [c["id"] for c in children] == [c["id"] for c in children_before]
            and all(c["status"] == "ended" for c in children),
            len(children),
        )
        grace = sql(f"SELECT {lit(kill['db_ts'])}::timestamptz + interval '10 seconds'")
        v.check(
            "no child provider call after the kill",
            run.audit_count("subagent", after_db_ts=grace) == 0,
        )
        state = run.assert_not_parked()
        v.check("no parked unit; queue done", state["state"] == "done", state)

    # -- measurement -----------------------------------------------------------
    def scenario_measure(self, run):
        summary = []
        for size in self.args.measure_sizes:
            sub = Run(self, f"measure-{size}")
            sub.logs.start()
            try:
                summary.append(self._measure_one(sub, size))
            except (GateFailure, GateError) as exc:
                sub.verdict.check("measurement completed", False, str(exc))
                summary.append({"children": size, "error": str(exc)})
            finally:
                try:
                    sub.collect()
                finally:
                    sub.logs.stop()
                    if not self.args.keep_sessions:
                        sub.end()
            for item in sub.verdict.items:
                run.verdict.items.append({**item, "check": f"{size}: {item['check']}"})
            self.settle_pool()
            ok, facts = self.preflight(f"between measurements (after {size})")
            if not ok:
                run.verdict.check(
                    "preconditions hold between sizes", False, facts["problems"]
                )
                break
        run.dump("measurements.json", summary)
        print("\nmeasurements:", flush=True)
        for row in summary:
            print("  " + json.dumps(row, default=str), flush=True)

    def _measure_one(self, run, size):
        sleep_s = self.args.measure_sleep
        run.create_session(permission_mode="autonomous", cap=size)
        run.warm_up(cap=size)
        sampler = MemorySampler(run.dir / "memory.jsonl", self.args.sample_interval)
        sampler.sample_once()
        baseline = {s["pod"]: s for s in sampler.samples}
        sampler.start()
        holders, max_running = set(), 0
        try:
            seq = run.send(
                batch_prompt(
                    run.tag("batch"),
                    size,
                    sleep_s,
                    self.args.child_type,
                    short_final("MEASURE-DONE"),
                ),
                "batch",
            )
            calls = run.wait_batch(seq, size)

            def finished():
                nonlocal max_running
                state = run.assert_not_parked()
                if state and state["leased_by"]:
                    holders.add(state["leased_by"])
                run.check_refusal(calls)
                running = sum(1 for c in run.children() if c["status"] != "ended")
                max_running = max(max_running, running)
                return run.answered(seq)

            wait_for(
                f"measure {size}: batch finished and answered",
                finished,
                timeout=self.args.timeout + size * (sleep_s + 60),
            )
        finally:
            sampler.stop()
        run.wait_audit_settled()
        window = one(
            "SELECT extract(epoch FROM min(created_at)) AS start, "
            "extract(epoch FROM max(ended_at)) AS finish, count(*) AS granted, "
            "count(*) FILTER (WHERE subagent_status='completed') AS completed, "
            "sum(total_tokens) AS child_tokens FROM threads "
            f"WHERE parent_thread_id='{run.thread}' AND kind='subagent'"
        )
        start, finish = float(window["start"] or 0), float(window["finish"] or 0)
        in_batch = [
            s
            for s in sampler.samples
            if start - 2 <= s["t"] <= finish + 2
            and (not holders or s["pod"] in holders)
        ]
        pods_now = {p["name"]: p for p in executor_pods(running_only=False)}
        peaks, oom = {}, {}
        for sample in sampler.samples:
            pod = sample["pod"]
            peaks[pod] = max(peaks.get(pod, 0), sample["peak"])
            before = baseline[pod]["oom_kill"] if pod in baseline else 0
            oom[pod] = max(oom.get(pod, 0), sample["oom_kill"] - before)
        audit = one_audit_tokens(run.thread)
        run.logs.stop()  # writes the per-pod snapshots the counts read
        logs = count_log_lines(run.dir / "logs", window=(start, finish or time.time()))
        result = {
            "children": size,
            "cap": size,
            "requested_calls": len(calls),
            "granted_children": int(window["granted"] or 0),
            "completed_children": int(window["completed"] or 0),
            "max_running_observed": max_running,
            "batch_duration_s": round(finish - start, 1) if finish else None,
            "sleep_s": sleep_s,
            "lease_holders": sorted(holders),
            "memory_baseline_mib": {
                pod: round(s["current"] / 2**20, 1) for pod, s in baseline.items()
            },
            "memory_peak_sampled_mib": round(
                max((s["current"] for s in in_batch), default=0) / 2**20, 1
            ),
            "cgroup_memory_peak_mib": {
                p: round(v / 2**20, 1) for p, v in peaks.items()
            },
            "oom_kills": sum(oom.values()),
            "pod_restarts": {
                p: (pods_now[p]["restarts"], pods_now[p]["last_termination"])
                for p in holders
                if p in pods_now
            },
            "child_provider_calls": run.audit_count("subagent"),
            "child_tokens": int(window["child_tokens"] or 0),
            "audit_tokens": audit,
            "lines_with_429_in_batch": logs["lines_with_429"],
            "retry_lines_in_batch": logs["retry_lines"],
            "log_counts_per_pod": logs["pods"],
            "thread": run.thread,
        }
        run.facts["measurement"] = result
        run.verdict.check(
            f"{size} delegate calls requested, {result['granted_children']} granted",
            result["granted_children"] == size,
        )
        run.verdict.check(
            f"at most {size} children ran at once", max_running <= size, max_running
        )
        return result


def one_audit_tokens(thread):
    raw = audit_sql(
        "SELECT json_build_object("
        "'input', sum((metadata->>'input_tokens')::bigint), "
        "'output', sum((metadata->>'output_tokens')::bigint), "
        "'cached', sum((metadata->>'cached_tokens')::bigint)) "
        f"FROM llm_requests WHERE job_id='{uid(thread)}' AND call_type='subagent' "
        "AND timestamp > now() - interval '2 days'"
    )
    return json.loads(raw) if raw else {}


# ---------------------------------------------------------------------------


BANNER = """\
parallel-subagents WP5 live gate (parallel_subagents.md §11)
  * never beside the full test suite or the VM gate (k3d node OOM)
  * one scenario at a time; never while source changes
  * every scenario re-checks that the pods serve the HEAD bytes
  * kill mode: {kill} ({kill_note})
"""


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    group = parser.add_argument_group("what to run (one scenario at a time)")
    group.add_argument("--preflight", action="store_true", help="checks only")
    group.add_argument("--k1", action="store_true", help="kill at pending approvals")
    group.add_argument("--k2", action="store_true", help="kill with two rows running")
    group.add_argument("--k3", action="store_true", help="kill in the final stream")
    group.add_argument("--k4", action="store_true", help="K2, then kill the recovery")
    group.add_argument("--measure", action="store_true", help="4/8/20 children")
    pinned = parser.add_argument_group(
        "pinned lane (P6; needs --lane pinned, one scenario at a time)"
    )
    pinned.add_argument(
        "--lane",
        choices=("stateless", PINNED_LANE),
        default="stateless",
        help="stateless: --k1..--measure (unchanged); pinned: --pk0..--pk6",
    )
    pinned.add_argument("--pk0", action="store_true", help="live preflight, 2 calls")
    pinned.add_argument("--pk1", action="store_true", help="4 calls, cap 2, waves")
    pinned.add_argument("--pk2", action="store_true", help="SIGKILL mid-batch")
    pinned.add_argument("--pk3", action="store_true", help="graceful delete (P4)")
    pinned.add_argument("--pk4", action="store_true", help="PK2, then kill again (P3)")
    pinned.add_argument("--pk5", action="store_true", help="switch off via heartbeat")
    pinned.add_argument(
        "--pk2e", action="store_true", help="PK2, then a person's End after it"
    )
    pinned.add_argument(
        "--pk6", action="store_true", help="PK6a: a person's End mid-batch (D4)"
    )
    pinned.add_argument(
        "--pk6b", action="store_true", help="PK6a with short-step children"
    )
    pinned.add_argument(
        "--pk6c", action="store_true", help="PK6a beside a background child"
    )
    pinned.add_argument(
        "--pk6b-steps",
        type=lambda raw: [int(x) for x in raw.split(",") if x.strip()],
        default=[2, 15, 15, 2],
        help="PK6b: shell steps per child (4 s each)",
    )
    pinned.add_argument("--pk6c-bg-sleep", type=int, default=600)
    pinned.add_argument(
        "--model",
        default="muse-spark-1.3-contributor",
        help="pinned parent model (children inherit)",
    )
    pinned.add_argument(
        "--project", help="conference project (default: the user's default project)"
    )
    pinned.add_argument(
        "--pinned-kill",
        choices=("sigkill", "force-delete"),
        default="sigkill",
        help="PK2/PK4: kill -9 from the k3d node, or delete --grace-period=0 --force",
    )
    pinned.add_argument(
        "--pk-sleeps",
        type=lambda raw: [int(x) for x in raw.split(",") if x.strip()],
        default=[20, 90, 90, 20],
        help="PK2/PK3/PK4/PK6 per-child sleeps (a short first child, long siblings)",
    )
    pinned.add_argument("--pk0-sleep", type=int, default=20)
    pinned.add_argument("--pk1-sleep", type=int, default=30)
    pinned.add_argument(
        "--retire-timeout", type=float, default=900, help="wait for a retirement, s"
    )
    pinned.add_argument(
        "--no-viewer",
        dest="viewer",
        action="store_false",
        help="drive the session headless (no WebSocket subscriber): it then "
        "reads awaiting_user mid-turn and a lost runtime settles suspended",
    )
    pinned.add_argument(
        "--no-parent-parallel-tool-calls",
        dest="parent_parallel_tool_calls",
        action="store_false",
        help="do not author llm.parallel_tool_calls on the inline expert",
    )
    parser.add_argument("--out", help="evidence directory (default: timestamped)")
    parser.add_argument(
        "--kill",
        choices=("hard", "drain"),
        default="hard",
        help="hard: delete with --grace-period=1 (crash); drain: plain delete",
    )
    parser.add_argument("--expert", default="bughunter", help="session expert")
    parser.add_argument(
        "--child-type",
        default="probe",
        help="roster child; must list shell_execute (MiniMax-M3 parents)",
    )
    parser.add_argument("--sleep", type=int, default=45, help="K2/K4 child sleep, s")
    parser.add_argument("--k3-sleep", type=int, default=15, help="K3 child sleep, s")
    parser.add_argument("--k1-calls", type=int, default=3, help="K1 delegate calls")
    parser.add_argument(
        "--k2-kill-at",
        choices=("wave2", "wave1"),
        default="wave2",
        help="wave2: two ended + two running; wave1: two running, two queued",
    )
    parser.add_argument(
        "--measure-sizes",
        type=lambda raw: [int(x) for x in raw.split(",") if x.strip()],
        default=[4, 8, 20],
    )
    parser.add_argument("--measure-sleep", type=int, default=30)
    parser.add_argument("--sample-interval", type=float, default=2.0)
    parser.add_argument("--timeout", type=float, default=900, help="per wait, s")
    parser.add_argument("--keep-sessions", action="store_true")
    parser.add_argument(
        "--print-prompts",
        action="store_true",
        help="print the scenario prompts and exit (no cluster access)",
    )
    args = parser.parse_args(argv)
    args.scenarios = [name for name in SCENARIOS if getattr(args, name)]
    pinned_selected = [name for name in PINNED_SCENARIOS if getattr(args, name)]
    if args.lane == PINNED_LANE:
        if args.scenarios:
            parser.error("--k1..--measure are stateless scenarios (omit --lane pinned)")
        args.scenarios = pinned_selected
    elif pinned_selected:
        parser.error("--pk0..--pk6 need --lane pinned")
    if not (args.scenarios or args.preflight or args.print_prompts):
        parser.error(
            "select --preflight, --print-prompts, --k1..--k4, --measure, or "
            "--lane pinned with --pk0..--pk6"
        )
    if any(not 1 <= size <= 20 for size in args.measure_sizes):
        parser.error("--measure-sizes must lie in 1..20 (the session cap range)")
    if args.kill == "drain" and ({"k3", "k4"} & set(args.scenarios)):
        parser.error(
            "K3 and K4 need --kill hard: a draining executor finishes the "
            "stream it should lose, so the scenario would pass trivially"
        )
    return args


def main(argv=None):
    args = parse_args(argv)
    if args.print_prompts:
        print(warm_up_prompt("K2-000000-warmup") + "\n")
        print(
            batch_prompt(
                "K2-000000-batch",
                4,
                args.sleep,
                args.child_type,
                short_final("K2-DONE"),
            )
        )
        return 0
    note = (
        "SIGKILL one second after SIGTERM"
        if args.kill == "hard"
        else "the executor drains for STATELESS_SHUTDOWN_TIMEOUT_S"
    )
    if args.lane == PINNED_LANE:
        note = (
            "pinned lane; PK2/PK4 kill -9 the agent process from the k3d node"
            if args.pinned_kill == "sigkill"
            else "pinned lane; PK2/PK4 delete --grace-period=0 --force"
        )
    print(BANNER.format(kill=args.kill, kill_note=note), flush=True)
    lock = acquire_lock()
    if lock is None:
        print(f"FAIL precondition: another gate run holds {LOCK_PATH}")
        return 1
    gate = Gate(args)
    if args.preflight:
        ok, facts = gate.preflight("preflight only")
        print(json.dumps({"ok": ok, **facts}, indent=1, default=str))
        return 0 if ok else 1
    gate.api = Api()
    gate.out.mkdir(parents=True, exist_ok=True)
    (gate.out / "run.json").write_text(
        json.dumps({"head": head_sha(), "args": vars(args), "at": utc_now()}, indent=1)
    )
    print(f"evidence: {gate.out}", flush=True)
    try:
        for name in args.scenarios:
            gate.run(name)
    finally:
        (gate.out / "results.json").write_text(
            json.dumps({"results": gate.results, "threads": gate.threads}, indent=1)
        )
        print("\nresults:", flush=True)
        for name in args.scenarios:
            thread = gate.threads.get(name) or ""
            print(f"  {name}: {gate.results.get(name, 'NOT RUN')} {thread}", flush=True)
        print(f"evidence: {gate.out}", flush=True)
        lock.close()
    return 0 if all(gate.results.get(n) == "PASS" for n in args.scenarios) else 1


if __name__ == "__main__":
    sys.exit(main())
