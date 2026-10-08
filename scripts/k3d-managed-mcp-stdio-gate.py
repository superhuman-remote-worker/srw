#!/usr/bin/env python3
"""Local k3d gate for connector drivers D5b: managed MCP, stdio images.

Design: knowledge-base/knowledge/features/connector_drivers.md, "MCP" (a
process per binding), "The driver contract" (the stdio bridge, injected by
an init container), "Three planes" and slice D5b, whose gate this is: "a
stock stdio image from Docker's mcp/* catalog serves two sessions, each with
its own process". It builds on the D5a gate (scripts/k3d-managed-mcp-gate.py,
loaded as ``base``): the same safety envelope (dry-run by default, the exact
k3d-srw/srw context, secrets only on ``kubectl exec -i`` stdin and scrubbed
from every printed line, every in-pod program capping its own memory, a
cleanup in ``finally`` that touches only what this run created, then a
residue check by gate id), its accounts (a disposable OAuth client and
second account, as the D3a to D3c gates make them), its D5 preflights
(hosting, the 12-probe enforcement harness, the node refused to driver pods,
a DNS check from the orchestrator) and its in-pod programs.

The stock image is Docker's mcp/memory (the official MCP memory server,
Node, stdio only), pinned by digest in the chart. Chosen because it needs no
external account and no egress, it has real read and write tools (read_graph,
search_nodes and open_nodes read the knowledge graph; create_entities and
five more change it), so ReadOnly has something to hide, and the graph it
writes shows which pod a call reached. It takes no credential of its own;
srw.mcp-stdio-test/v1 delivers the connector's token to each binding's
process as MCP_STDIO_TEST_TOKEN anyway, which is what this gate traces. Its
image has a shell, which the gate uses to read a process's environment.

Fixtures (all disposable, named after the gate id):

  client      ``<gate id>-oauth`` and the second account (the D3c fixture)
  projects    two of the owner: ``rw``, and ``ro``, whose link of the memory
              connector is read-only
  connectors  of the owner: ``memory`` (srw.mcp-stdio-test/v1, ReadWrite,
              a random fake token); of the second account: ``stranger``
              (the same driver, its own token)
  sessions    ``one`` (the owner, project rw, memory: a ReadWrite binding),
              ``two`` (the owner, project ro, memory: a ReadOnly binding of
              the same connector, so the same pod) and ``three`` (the second
              account, stranger only)

Checks (each printed PASS/FAIL; the exit status is 0 only if all pass):

  preflight   orchestrator and stateless agent pods serve this checkout's
              D5a and D5b modules, byte for byte; hosting on with an exchange
              and canary port, a digest-pinned shim and front (whose image
              carries the bridge), the stdio test server installed at a
              pinned digest, room for two pods; migrations 0360-0363 and
              0390-0392; the driver namespace's baseline and default deny,
              enforced (12-probe harness); refusedCidrs covers the node;
              Docker Hub's registry and token hosts resolve and answer from
              the orchestrator (a bind reads the stock image's config there)
  startup     each pod's canary wait ran first and exited 0, then the bridge
              install from the pinned front image; the server container runs
              the stock image at its digest with the bridge as its command
              and the image's own program (node dist/index.js) after it,
              nothing of SRW's in its environment and no delivery mount; its
              Secret holds no credential; the bridge answers its status
  processes   both sessions' own clients connected the server (README: mcp,
              N tools, managed by SRW); the bridge in the one memory pod runs
              two processes, one per binding (session one's and session two's
              leases), with two process ids, both children of the bridge
              running node; each process's environment holds the connector's
              token as MCP_STDIO_TEST_TOKEN and no SRW_ variable; the agent's
              own client, connecting with session one's lease, keeps that
              binding's process (the same id): a process serves its binding's
              sessions; an entity session one creates is read by session two
              (one pod, one graph)
  credential  the agent pod holds no token of either connector (environments,
              command lines, files under /app, /tmp, /home, /root, /var/tmp,
              /run), nor does session one's workspace; the front's and the
              bridge's logs hold no token and no lease token
  denied      an execution without the connector gets 401 from the memory
              endpoint: no token, a malformed one, session three's lease of
              the stranger connector; session three's client finds the server
              unavailable; no process was started for any of them
  workspace   session one's workspace cannot connect to the memory endpoint,
              not even with session one's own lease
  readonly    session two's ReadOnly binding lists only read_graph,
              search_nodes and open_nodes; create_entities is answered
              "Unknown tool" by the front; session one's binding lists all
              nine; the exchange never saw a write for the ReadOnly lease;
              the D5a review's parsing-differential corpus
              (drivers/mcp-front/testdata/bypass_corpus.json, its write and
              read tools named create_entities and read_graph) sent with
              session two's lease: no answer from the server is an error or
              names create_entities, the well-formed write is "Unknown tool",
              and the graph is unchanged
  ending      session two ends: its lease is revoked, and its binding's
              process stops (it leaves the bridge's status and the pod's
              process table) while session one's runs on
  cleanup     sessions, connectors, projects, the second account, the OAuth
              client and probe pods are gone, and no object in the driver
              namespace names this run's connectors

Run with the repository venv on the k3d-srw cluster, alone: this is a
mutating gate. The owner (--user) must be an administrator.

  .venv/bin/python scripts/k3d-managed-mcp-stdio-gate.py           # plan
  .venv/bin/python scripts/k3d-managed-mcp-stdio-gate.py \\
      --run --confirm LOCAL-K3D-DISPOSABLE
"""

from __future__ import annotations

import argparse
import base64
import importlib.util
import json
import re
import secrets
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_D5A_GATE = ROOT / "scripts" / "k3d-managed-mcp-gate.py"


def _load_d5a_gate():
    name = "k3d_managed_mcp_gate"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, _D5A_GATE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


base = _load_d5a_gate()
GateError = base.GateError
SafetyError = base.SafetyError
LOCAL_CONTEXT = base.LOCAL_CONTEXT
LOCAL_NAMESPACE = base.LOCAL_NAMESPACE
LOCAL_CONFIRMATION = base.LOCAL_CONFIRMATION
DEFAULT_MODEL = base.DEFAULT_MODEL
K = base.K
AGENT_CONTAINER = base.AGENT_CONTAINER
ORCHESTRATOR = base.ORCHESTRATOR
ORCHESTRATOR_CONTAINER = base.ORCHESTRATOR_CONTAINER
WORKSPACE_CONTAINER = base.WORKSPACE_CONTAINER
FRONT_PORT = base.FRONT_PORT
MIGRATIONS = base.MIGRATIONS
run, command, sql, lit, wait_for, secret, in_pod = (
    base.run,
    base.command,
    base.sql,
    base.lit,
    base.wait_for,
    base.secret,
    base.in_pod,
)

STDIO_TYPE = "mcp_stdio_test"
STDIO_DRIVER = "srw.mcp-stdio-test/v1"
#: The stock image the chart pins (helm/values.yaml connectors.drivers
#: .mcpStdioTest).
STOCK_IMAGE = "docker.io/mcp/memory"
STOCK_DIGEST = "sha256:db0c2db07a44b6797eba7a832b1bda142ffc899588aae82c92780cbb2252407f"
STOCK_PROGRAM = ["node", "dist/index.js"]
BRIDGE = "/srw/bin/srw-mcp-bridge"
#: Where the bridge listens in the pod (the spec's mcp port).
BRIDGE_LISTEN = "127.0.0.1:8091"
TOKEN_ENV = "MCP_STDIO_TEST_TOKEN"
READ_TOOLS = ("read_graph", "search_nodes", "open_nodes")
WRITE_TOOLS = (
    "create_entities",
    "create_relations",
    "add_observations",
    "delete_entities",
    "delete_observations",
    "delete_relations",
)
CORPUS = ROOT / "drivers" / "mcp-front" / "testdata" / "bypass_corpus.json"
REGISTRY_HOSTS = ("registry-1.docker.io", "auth.docker.io")
_GATE_ID_RE = re.compile(r"d5b-[0-9a-f]{10}\Z")

# A process's environment, read in the server container by its shell. The
# token comes on stdin and is compared by shell builtins only (never in an
# argument of a program another process could read); only counts come back.
_ENVIRON_SCRIPT = r"""
pid=$1
IFS= read -r token
[ -r "/proc/$pid/environ" ] || { echo missing; exit 0; }
tr '\0' '\n' < "/proc/$pid/environ" | {
  tokens=0; srw=0
  while IFS= read -r line; do
    case "$line" in
      "MCP_STDIO_TEST_TOKEN=$token") tokens=$((tokens + 1)) ;;
      SRW_*) srw=$((srw + 1)) ;;
    esac
  done
  printf 'token=%s\nsrw=%s\n' "$tokens" "$srw"
}
stat=$(cat "/proc/$pid/stat" 2>/dev/null)
set -- ${stat##*) }
printf 'parent=%s\n' "$2"
printf 'program=%s\n' "$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null)"
"""

# Raw JSON-RPC bodies against one endpoint with one bearer, after an
# initialize and its notification: each body is sent exactly as given (bytes,
# base64 on stdin, never re-encoded), and each answer's status, content type
# and first bytes come back (the gate scrubs what it prints).
_RAW_PROGRAM = (
    base._POD_MEMORY_CAP
    + r"""
import base64, json, sys, urllib.error, urllib.request
cap_memory()
request = json.loads(sys.stdin.readline())
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

def post(body, session=None):
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "Authorization": "Bearer " + request["bearer"],
    }
    if session:
        headers["Mcp-Session-Id"] = session
    req = urllib.request.Request(request["url"], data=body, method="POST", headers=headers)
    try:
        with opener.open(req, timeout=30) as response:
            text = response.read(65536).decode("utf-8", "replace")
            return (response.status, response.headers.get("Mcp-Session-Id"),
                    response.headers.get("Content-Type") or "", text)
    except urllib.error.HTTPError as error:
        return (error.code, None, error.headers.get("Content-Type") or "",
                error.read(65536).decode("utf-8", "replace"))
    except Exception as error:
        return 0, None, "", type(error).__name__

status, session, _, _ = post(json.dumps({
    "jsonrpc": "2.0", "id": 1, "method": "initialize",
    "params": {"protocolVersion": "2025-06-18", "capabilities": {},
               "clientInfo": {"name": "srw-gate", "version": "1"}},
}).encode())
out = {"initialize": status, "answers": []}
if status == 200:
    post(b'{"jsonrpc":"2.0","method":"notifications/initialized"}', session)
    for encoded in request["bodies"]:
        code, _, kind, text = post(base64.b64decode(encoded), session)
        out["answers"].append({"status": code, "type": kind, "body": text[:4000]})
print(json.dumps(out))
"""
)


def corpus_bodies(write_tool: str, read_tool: str, path: Path = CORPUS) -> list[bytes]:
    """The D5a review's corpus with its write and read tools named for this
    server (they are plain JSON strings in every body)."""
    shared = json.loads(path.read_text())
    bodies = [body.encode() for body in shared["bodies"]] + [
        base64.b64decode(body) for body in shared["bodies_base64"]
    ]
    return [
        body.replace(shared["write_tool"].encode(), write_tool.encode()).replace(
            shared["read_tool"].encode(), read_tool.encode()
        )
        for body in bodies
    ]


def control_body(write_tool: str, marker: str) -> bytes:
    """A well-formed call of the write tool: the front's "Unknown tool" for
    a ReadOnly lease, and the graph would hold ``marker`` if it ran."""
    arguments = {
        "entities": [
            {"name": marker, "entityType": "srw-gate", "observations": ["bypass"]}
        ]
    }
    return json.dumps(
        {
            "jsonrpc": "2.0",
            "id": 99,
            "method": "tools/call",
            "params": {"name": write_tool, "arguments": arguments},
        },
        separators=(",", ":"),
    ).encode()


def corpus_verdict(result: dict, *, sent: int, write_tool: str) -> tuple[bool, str]:
    """No body of the corpus reached the server as a write call: an answer
    from the server (a stream the bridge relayed) never names the write tool
    and is never a JSON-RPC error (a write call with the corpus's missing or
    empty arguments fails in the server); the D5a review's six bodies are
    refused as malformed (400); the well-formed write control, sent last,
    is the front's own "Unknown tool" (JSON, never a stream)."""
    answers = result.get("answers") or []
    streamed = [
        (index, answer)
        for index, answer in enumerate(answers)
        if (answer.get("type") or "").startswith("text/event-stream")
    ]
    leaked = [
        index
        for index, answer in streamed
        if write_tool in (answer.get("body") or "")
        or '"error"' in (answer.get("body") or "")
    ]
    control = answers[-1] if answers else {}
    reviewers = [answer.get("status") for answer in answers[:6]]
    ok = (
        result.get("initialize") == 200
        and len(answers) == sent
        and not leaked
        and reviewers == [400] * 6
        and control.get("status") == 200
        and (control.get("type") or "").startswith("application/json")
        and "Unknown tool" in (control.get("body") or "")
    )
    statuses: dict[str, int] = {}
    for answer in answers:
        key = f"{answer.get('status')}{'/stream' if (answer.get('type') or '').startswith('text/event-stream') else ''}"
        statuses[key] = statuses.get(key, 0) + 1
    return ok, (
        f"initialize={result.get('initialize')} answers={len(answers)}/{sent} "
        f"statuses={statuses} leaked={leaked} reviewers={reviewers} "
        f"control={(control.get('body') or '')[:100]}"
    )


def processes_verdict(
    status: dict, leases: dict[str, str]
) -> tuple[bool, dict[str, int], str]:
    """The bridge runs one process per binding: one for each of the two
    leases, two process ids, each with the binding's credential."""
    by_binding = {
        item.get("binding"): item
        for item in status.get("processes") or []
        if not item.get("probe")
    }
    pids = {
        label: int((by_binding.get(lease) or {}).get("pid") or 0)
        for label, lease in leases.items()
    }
    ok = (
        set(by_binding) == set(leases.values())
        and all(pids.values())
        and len(set(pids.values())) == len(pids)
        and all(
            (by_binding.get(lease) or {}).get("credential") for lease in leases.values()
        )
    )
    return (
        ok,
        pids,
        (
            f"bindings={sorted(by_binding)} pids={pids} "
            f"max={status.get('max_processes')} env={status.get('credential_env')}"
        ),
    )


def environ_verdict(lines: str) -> tuple[bool, str]:
    """A process's environment holds the token once and nothing of SRW's;
    it is the bridge's child running the server's program."""
    found = dict(line.split("=", 1) for line in lines.splitlines() if "=" in line)
    ok = (
        found.get("token") == "1"
        and found.get("srw") == "0"
        and found.get("parent") == "1"
        and found.get("program", "").startswith(" ".join(STOCK_PROGRAM))
    )
    return ok, json.dumps(found)


def stdio_init_passed(pod: dict) -> bool:
    """The canary wait ran first and exited 0, then the bridge install."""
    spec = pod.get("spec") or {}
    names = [c.get("name") for c in spec.get("initContainers") or []]
    statuses = {
        status.get("name"): ((status.get("state") or {}).get("terminated") or {})
        for status in (pod.get("status") or {}).get("initContainerStatuses") or []
    }
    return names == ["canary-wait", "install-bridge"] and all(
        statuses.get(name, {}).get("exitCode") == 0 for name in names
    )


def stdio_layout_problems(pod: dict, front_image: str) -> list[str]:
    """How a stdio pod differs from the stock image at its digest behind
    the bridge (installed from the pinned front image) beside the front."""
    problems = base.layout_problems(pod, front_image)
    spec = pod.get("spec") or {}
    containers = {c.get("name"): c for c in spec.get("containers") or []}
    server = containers.get("driver") or {}
    if server.get("image") != f"{STOCK_IMAGE}@{STOCK_DIGEST}":
        problems.append(f"the server runs {server.get('image')}")
    command_ = server.get("command") or []
    if command_[:2] != [BRIDGE, "serve"] or command_[-3:] != ["--", *STOCK_PROGRAM]:
        problems.append(f"the server's command is {command_}")
    if "--credential-env" not in command_ or TOKEN_ENV not in command_:
        problems.append("the bridge names no credential variable")
    mounts = {m.get("name"): m for m in server.get("volumeMounts") or []}
    if not (mounts.get("srw-bin") or {}).get("readOnly"):
        problems.append("the bridge is not mounted read-only")
    if (server.get("securityContext") or {}).get("readOnlyRootFilesystem") is not True:
        problems.append("the server's root filesystem is writable")
    install = next(
        (
            c
            for c in spec.get("initContainers") or []
            if c.get("name") == "install-bridge"
        ),
        {},
    )
    if install.get("image") != front_image:
        problems.append(
            f"the bridge comes from {install.get('image')}, not the front's image"
        )
    if any(e.get("name") == TOKEN_ENV for e in server.get("env") or []):
        problems.append("the pod sets the credential variable itself")
    return problems


PLAN = [
    "preflight: orchestrator and stateless agent pods serve this checkout's "
    "D5a and D5b modules; hosting on with an exchange and canary port, a "
    "digest-pinned shim and front (carrying the bridge), the stdio test server "
    "installed at its pinned stock digest, room for two pods; migrations "
    "0360-0363 and 0390-0392; the driver namespace's baseline and default deny, "
    "enforced (12-probe harness); refusedCidrs covers the node; Docker Hub "
    "resolves and answers from the orchestrator",
    "accounts: a disposable OAuth client and second account; the owner is an "
    "administrator, the second account is not",
    "startup: each pod's canary wait, then the bridge install from the front's "
    "image; the stock mcp/memory image at its digest behind the bridge, with "
    "node dist/index.js after it and nothing of SRW's; no credential in its "
    "Secret; the bridge answers its status",
    "processes: both sessions' clients connected; the one memory pod runs two "
    "processes, one per binding, children of the bridge, each with the token "
    "in MCP_STDIO_TEST_TOKEN and no SRW_ variable; a new session of a binding "
    "keeps its process; session two reads what session one wrote",
    "credential: the agent pod and session one's workspace hold no token; the "
    "front's and the bridge's logs hold no token or lease token",
    "denied: no token, a malformed one and session three's lease of another "
    "connector get 401 from the memory endpoint, and start no process",
    "workspace: session one's workspace cannot connect to the memory endpoint",
    "readonly: session two lists only the three read tools and create_entities "
    "is 'Unknown tool'; the D5a review's corpus sent with its lease never "
    "reaches the server as a write call; the graph is unchanged",
    "ending: session two ends; its binding's process stops, session one's runs on",
    "cleanup: sessions, connectors, projects, account, OAuth client and probe "
    "pods are gone; no driver-namespace object names this run's connectors",
]


class StdioGate(base.ManagedMcpGate):
    def __init__(self, args: argparse.Namespace) -> None:
        super().__init__(args)
        self.projects: dict[str, str] = {}
        self.tokens = {
            label: secret(f"d5b-upstream-{label}-{secrets.token_hex(16)}")
            for label in ("memory", "stranger")
        }
        self.memory_pod = ""
        self.read_write_tools: list[str] = []
        # The process id of each memory binding (session label -> pid).
        self.pids: dict[str, int] = {}

    # -- naming and helpers ------------------------------------------------
    def title(self, label: str) -> str:
        return f"D5b managed MCP stdio gate {self.gate_id} {label}"

    def entry(self, label: str, session: str, *, token: str | None = None) -> dict:
        found = super().entry(label, session, token=token)
        found["type"] = STDIO_TYPE
        return found

    def bridge_status(self, pod: str) -> dict:
        out = command(
            self.kc
            + ["exec", pod, "-c", "driver", "--", BRIDGE, "status"]
            + ["--listen", BRIDGE_LISTEN],
            timeout=60,
        )
        return json.loads(out.splitlines()[-1])

    def environ(self, pod: str, pid: int, token: str) -> str:
        return command(
            self.kc
            + ["exec", "-i", pod, "-c", "driver", "--"]
            + ["sh", "-c", _ENVIRON_SCRIPT, "environ", str(pid)],
            data=token + "\n",
            timeout=60,
        )

    def lease_id(self, label: str, session: str) -> str:
        lease = self.live_lease(label, self.threads[session])
        if not lease:
            raise GateError(f"no live lease of {label} for session {session}")
        return str(lease["id"])

    def create_session(
        self, label: str, connectors: list[str], who: str, project: str | None = None
    ) -> str:
        api = self.owner if who == "owner" else self.other
        created = api.ok(
            "POST",
            "/api/persistent/threads",
            {
                "title": self.title(label),
                "permission_mode": "autonomous" if who == "owner" else "auto_accept",
                **({"project_id": self.projects[project]} if project else {}),
                "datasource_ids": [self.connectors[c] for c in connectors],
                "config_override": {"workspace": {"backend": "sandbox"}},
                "model": self.args.model,
            },
        )
        thread = str(created.get("thread_id") or created["id"])
        self.threads[label] = thread
        self.thread_api[thread] = who
        print(f"session {label}: {thread}", flush=True)
        api.ok(
            "POST",
            f"/api/persistent/threads/{thread}/input",
            {"content": "Reply with the single word ready."},
        )
        for connector in connectors:
            wait_for(
                f"a lease for {connector} on {label}",
                lambda connector=connector: self.live_lease(connector, thread),
                timeout=self.args.turn_timeout,
                interval=5,
            )
        return thread

    def wait_ready_pod(self, label: str) -> dict:
        try:
            return super().wait_ready_pod(label)
        except GateError:
            live = self.live_pods(label)
            pod = self.driver_pod(live[0]) if live else None
            waiting = [
                (status.get("name"), (status.get("state") or {}).get("waiting"))
                for status in ((pod or {}).get("status") or {}).get("containerStatuses")
                or []
            ]
            raise GateError(
                f"connector {label}'s pod is not ready: {waiting} (an image pull "
                "from Docker Hub needs the node's DNS: docker restart "
                "k3d-srw-server-0 after a host network change)"
            ) from None

    # -- phases --------------------------------------------------------------
    def preflight(self) -> None:
        problems: list[str] = []
        for served in base.SERVED_SETS:
            pods = self.release_pods(
                f"{base._SELECTOR},app.kubernetes.io/component={served.component}"
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
            "checkout's D5a and D5b modules",
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
                "CONNECTOR_MCP_FRONT_IMAGE",
                "CONNECTOR_MANAGED_MCP_IMAGES",
                "CONNECTOR_SERVICE_MAX_INSTALLATION",
                "CONNECTOR_SERVICE_RECONCILE_SECONDS",
                "CONNECTOR_SERVICE_REFUSED_CIDRS",
                "CONNECTOR_SERVICE_NODE_IP",
            )
        }
        try:
            managed = json.loads(env["CONNECTOR_MANAGED_MCP_IMAGES"] or "{}")
        except ValueError:
            managed = {}
        configured = (
            env["CONNECTOR_SERVICE_PODS_ENABLED"].lower() == "true"
            and env["CONNECTOR_SERVICE_NAMESPACE"] != ""
            and env["CONNECTOR_LEASE_EXCHANGE_PORT"].isdigit()
            and env["CONNECTOR_LEASE_CANARY_PORT"].isdigit()
            and "@sha256:" in env["CONNECTOR_DRIVER_SHIM_IMAGE"]
            and "@sha256:" in env["CONNECTOR_MCP_FRONT_IMAGE"]
            and str(managed.get(STDIO_DRIVER, "")).endswith(f"@{STOCK_DIGEST}")
        )
        self.report.check(
            "preflight: hosting on with an exchange and canary port, a "
            "digest-pinned shim and front, the stdio test server installed at "
            "its pinned stock digest",
            configured,
            json.dumps(env),
        )
        if not configured:
            raise GateError(
                "set connectors.servicePods.enabled, connectors.drivers.mcpFront "
                "and connectors.drivers.mcpStdioTest.enabled as the k3d profile "
                "does, under Tilt"
            )
        self.namespace = env["CONNECTOR_SERVICE_NAMESPACE"]
        self.front_image = env["CONNECTOR_MCP_FRONT_IMAGE"]
        self.reconcile_seconds = int(
            float(env["CONNECTOR_SERVICE_RECONCILE_SECONDS"] or 15)
        )
        node = env["CONNECTOR_SERVICE_NODE_IP"]
        covered = base.node_refused(node, env["CONNECTOR_SERVICE_REFUSED_CIDRS"])
        self.report.check(
            "preflight: driver pods are refused the orchestrator's node",
            covered,
            f"node={node} refused={env['CONNECTOR_SERVICE_REFUSED_CIDRS']}",
        )
        if not covered:
            raise GateError("set connectors.servicePods.refusedCidrs to cover the node")
        names = ", ".join(lit(name) for name in MIGRATIONS)
        applied = sql(
            "SELECT count(*) FROM schema_migrations WHERE success AND filename "
            f"IN ({names})"
        )
        self.report.check(
            "preflight: migrations 0360-0363 and 0390-0392 applied",
            applied == str(len(MIGRATIONS)),
            f"{applied} of {len(MIGRATIONS)}",
        )
        if applied != str(len(MIGRATIONS)):
            raise GateError("the service-pod migrations are not applied")
        live = sql(
            "SELECT count(*) FROM connector_driver_identities WHERE "
            "credential_generation IS NOT NULL AND removed_at IS NULL"
        )
        cap = int(env["CONNECTOR_SERVICE_MAX_INSTALLATION"] or 0)
        room = live.isdigit() and int(live) + 2 <= cap
        self.report.check(
            "preflight: the installation has room for this run's two pods",
            room,
            f"{live} live of {cap}",
        )
        if not room:
            raise GateError("too many live service pods: stop other gates first")
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
        self.report.check(
            "preflight: the driver namespace enforces Pod Security baseline and "
            "has its static default deny",
            labels.get("pod-security.kubernetes.io/enforce") == "baseline"
            and deny["spec"].get("podSelector") == {}
            and sorted(deny["spec"].get("policyTypes") or []) == ["Egress", "Ingress"],
            str({k: v for k, v in labels.items() if "pod-security" in k}),
        )
        self.orchestrator_ip = command(
            K
            + ["get", "svc", base.ORCHESTRATOR_SERVICE]
            + ["-o", "jsonpath={.spec.clusterIP}"]
        )
        if not base._IPV4_RE.fullmatch(self.orchestrator_ip):
            raise GateError("the orchestrator Service has no IPv4 ClusterIP")
        # A bind reads the stock image's config (its entrypoint) from Docker
        # Hub, from the orchestrator: a dead cluster DNS upstream refuses it.
        answers = self.net(
            ORCHESTRATOR,
            ORCHESTRATOR_CONTAINER,
            [
                {"kind": "connect", "host": host, "port": 443, "timeout": 15}
                for host in REGISTRY_HOSTS
            ],
        )
        reachable = all(answer.get("reachable") is True for answer in answers)
        self.report.check(
            "preflight: Docker Hub's registry and token hosts resolve and answer "
            "from the orchestrator (a bind reads the stock image's config there)",
            reachable,
            "reachable"
            if reachable
            else f"{answers}: on k3d a dead DNS upstream after a host network "
            "change; restart the node: docker restart k3d-srw-server-0",
        )
        if not reachable:
            raise GateError("Docker Hub is not reachable from the orchestrator")
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

    def fixture(self) -> None:
        for label in ("rw", "ro"):
            created = self.owner.ok(
                "POST",
                "/api/projects",
                {
                    "name": self.name(f"project {label}"),
                    "description": "D5b managed MCP stdio gate (disposable)",
                    "user_id": self.owner_id,
                },
            )
            self.projects[label] = str(created["id"])
        # The D5a cleanup and residue know one project; the other is ours.
        self.project = self.projects["rw"]
        self.create_connector(
            "memory",
            {
                "type": STDIO_TYPE,
                "credentials": {"token": self.tokens["memory"]},
                "config": {"access": "ReadWrite"},
            },
            "owner",
        )
        self.create_connector(
            "stranger",
            {
                "type": STDIO_TYPE,
                "credentials": {"token": self.tokens["stranger"]},
                "config": {"access": "ReadWrite"},
            },
            "other",
        )
        # Session two binds the same connector through a read-only link.
        self.owner.ok(
            "POST",
            f"/api/projects/{self.projects['ro']}/datasources/"
            f"{self.connectors['memory']}",
            {"read_only": True},
        )
        print(f"fixture: projects {self.projects}, connectors {self.connectors}")

    def startup_checks(self) -> None:
        self.create_session("one", ["memory"], "owner", project="rw")
        self.create_session("two", ["memory"], "owner", project="ro")
        self.create_session("three", ["stranger"], "other")
        rows = {label: self.wait_ready_pod(label) for label in self.connectors}
        for label, row in rows.items():
            pod = self.driver_pod(row) or {}
            _rc, canary_log, _err = run(
                self.kc + ["logs", row["pod_name"], "-c", "canary-wait"], timeout=60
            )
            self.report.check(
                f"startup: {label}'s canary wait ran first and exited 0 after the "
                "default deny was enforced, then the bridge install",
                stdio_init_passed(pod) and "default deny enforced" in canary_log,
                canary_log.splitlines()[-1][:200] if canary_log else "no log",
            )
            problems = stdio_layout_problems(pod, self.front_image)
            self.report.check(
                f"startup: {label}'s pod runs the stock image at its digest behind "
                "the bridge from the pinned front image, with nothing of SRW's",
                not problems,
                "; ".join(problems),
            )
            secret_doc = json.loads(
                command(self.kc + ["get", "secret", row["pod_name"], "-o", "json"])
            )
            request = json.loads(
                base64.b64decode(secret_doc["data"]["request.json"]).decode()
            )
            held = [
                name
                for name, token in self.tokens.items()
                if token in json.dumps(request)
            ]
            self.report.check(
                f"startup: {label}'s pod Secret holds no credential; the front's "
                "block names the stdio bridge and the credential's variable",
                request.get("credentials") == {}
                and not held
                and (request.get("mcp") or {}).get("transport") == "stdio"
                and (request.get("mcp") or {}).get("credential") == {"env": TOKEN_ENV},
                f"credentials={sorted(request.get('credentials') or {})} held={held}",
            )
        self.memory_pod = rows["memory"]["pod_name"]
        try:
            status = self.bridge_status(self.memory_pod)
        except (GateError, ValueError) as exc:
            status = {"error": str(exc)}
        # "session" is this checkout's bridge's (a process per binding that
        # serves its later sessions).
        self.report.check(
            "startup: the memory pod's bridge answers its status (this "
            "checkout's: it reports each process's session)",
            status.get("credential_env") == TOKEN_ENV
            and all("session" in item for item in status.get("processes") or []),
            json.dumps(status)[:300],
        )

    def processes_checks(self) -> None:
        for label in ("one", "two"):
            pod = self.workspace_pod(self.threads[label])

            def listed(pod=pod) -> str | None:
                _rc, text = self.ws(pod, "cat ~/workspace/README.md 2>/dev/null\n")
                pattern = r"\*\*{}\*\* \(mcp, (\d+) tools\) — managed by SRW"
                found = re.search(pattern.format(re.escape(self.name("memory"))), text)
                return text if found and int(found.group(1)) > 0 else None

            try:
                wait_for(
                    f"session {label}'s README lists the memory server connected",
                    listed,
                    timeout=self.args.turn_timeout,
                    interval=10,
                )
                connected = True
            except GateError:
                connected = False
            self.report.check(
                f"processes: session {label}'s own client connected the stock "
                "stdio server (README: mcp, N tools, managed by SRW)",
                connected,
            )
        leases = {label: self.lease_id("memory", label) for label in ("one", "two")}
        status = wait_for(
            "a process for each binding",
            lambda: (
                found
                if processes_verdict(
                    found := self.bridge_status(self.memory_pod), leases
                )[0]
                else None
            ),
            timeout=120,
            interval=5,
        )
        ok, pids, detail = processes_verdict(status, leases)
        self.pids = dict(pids)
        self.report.check(
            "processes: two sessions get two processes in one pod, one per "
            "binding, each holding its credential",
            ok and len(self.live_pods("memory")) == 1,
            detail,
        )
        for label, pid in pids.items():
            ok, detail = environ_verdict(
                self.environ(self.memory_pod, pid, self.tokens["memory"])
            )
            self.report.check(
                f"processes: session {label}'s process is the bridge's child "
                "running node, with the connector's token in "
                "MCP_STDIO_TEST_TOKEN and no SRW_ variable",
                ok,
                detail,
            )
        marker = f"{self.gate_id}-one"
        wrote = self.client(
            self.entry("memory", "one"),
            [
                {
                    "tool": "create_entities",
                    "arguments": {
                        "entities": [
                            {
                                "name": marker,
                                "entityType": "srw-gate",
                                "observations": ["one"],
                            }
                        ]
                    },
                },
                {"tool": "read_graph"},
            ],
        )
        self.read_write_tools = wrote["tools"]
        after = self.bridge_status(self.memory_pod)
        kept = processes_verdict(after, leases)[1]
        self.report.check(
            "processes: the agent's client, a new session of session one's "
            "binding, is served by that binding's process (the same process id)",
            wrote["status"] == "connected" and kept.get("one") == pids["one"],
            f"status={wrote['status']} before={pids['one']} after={kept.get('one')}",
        )
        read = self.client(
            self.entry("memory", "two"),
            [{"tool": "open_nodes", "arguments": {"names": [marker]}}],
        )
        (opened,) = read["calls"] or [{}]
        self.report.check(
            "processes: session two's binding reads the entity session one's "
            "wrote (one pod, one graph)",
            marker in (opened.get("text") or ""),
            (opened.get("text") or opened.get("error") or "")[:160],
        )

    def credential_checks(self) -> None:
        agent = self.scan(
            self.agent_pod(),
            AGENT_CONTAINER,
            ["/app", "/tmp", "/home", "/root", "/var/tmp", "/run"],
        )
        self.report.check(
            "credential: the agent pod holds neither connector's token "
            "(environments, command lines, files)",
            not agent["found"] and agent["scanned"]["processes"] > 0,
            json.dumps(agent)[:400],
        )
        workspace = self.scan(
            self.workspace_pod(self.threads["one"]),
            WORKSPACE_CONTAINER,
            ["/home", "/tmp", "/root", "/var/tmp", "/run"],
            python="python3",
        )
        self.report.check(
            "credential: session one's workspace holds neither token either",
            not workspace["found"],
            json.dumps(workspace)[:400],
        )
        logs = ""
        for label in self.connectors:
            for row in self.live_pods(label):
                for container in ("front", "driver"):
                    _rc, text, _err = run(
                        self.kc + ["logs", row["pod_name"], "-c", container],
                        timeout=60,
                    )
                    logs += text
        leaked = [name for name, token in self.tokens.items() if token in logs] + [
            "lease token" for token in self.lease_tokens.values() if token in logs
        ]
        self.report.check(
            "credential: the front's and the bridge's logs hold no token and no "
            "lease token (the bridge logged its processes)",
            "started process" in logs and not leaked and "<redacted>" not in logs,
            f"leaked={leaked}",
        )

    def denied_checks(self) -> None:
        before = self.bridge_status(self.memory_pod)
        stranger = self.lease_token("stranger", "three")
        url = self.endpoint("memory")
        answers = self.net(
            self.agent_pod(),
            AGENT_CONTAINER,
            [
                {"kind": "initialize", "url": url, "bearer": None},
                {"kind": "initialize", "url": url, "bearer": "scl_not-a-lease"},
                {"kind": "initialize", "url": url, "bearer": stranger},
            ],
        )
        self.report.check(
            "denied: an execution without the connector gets 401 (no token, a "
            "malformed one, session three's lease of another connector)",
            [a.get("status") for a in answers] == [401] * 3,
            json.dumps(answers),
        )
        refused = self.client(self.entry("memory", "three", token=stranger))
        after = self.bridge_status(self.memory_pod)

        def bindings(status: dict) -> list[str]:
            return sorted(
                item.get("binding")
                for item in status.get("processes") or []
                if not item.get("probe")
            )

        self.report.check(
            "denied: session three's client finds the server unavailable and no "
            "process was started for any refused request",
            refused["status"].startswith("unavailable")
            and not refused["tools"]
            and bindings(after) == bindings(before),
            f"{refused['status'][:120]} before={bindings(before)} after={bindings(after)}",
        )

    def workspace_checks(self) -> None:
        pod = self.workspace_pod(self.threads["one"])
        answers = self.net(
            pod,
            WORKSPACE_CONTAINER,
            [
                {
                    "kind": "initialize",
                    "url": self.endpoint("memory"),
                    "bearer": self.lease_token("memory", "one"),
                }
            ],
            python="python3",
        )
        self.report.check(
            "workspace: session one's workspace cannot connect to the memory "
            "endpoint, even with session one's own lease",
            all(a.get("status") == 0 for a in answers),
            json.dumps(answers),
        )

    def graph_names(self) -> set[str]:
        graph = self.client(self.entry("memory", "one"), [{"tool": "read_graph"}])
        (read,) = graph["calls"] or [{}]
        try:
            parsed = json.loads(read.get("text") or "{}")
        except ValueError:
            return set()
        return {entity.get("name") for entity in parsed.get("entities") or []}

    def readonly_checks(self) -> None:
        lease = self.live_lease("memory", self.threads["two"]) or {}
        read_only = self.client(
            self.entry("memory", "two"),
            [{"tool": "create_entities", "arguments": {"entities": []}, "raw": True}],
        )
        tools = set(read_only.get("tools") or [])
        (call,) = read_only["calls"] or [{}]
        self.report.check(
            "readonly: session two's ReadOnly binding lists only read_graph, "
            "search_nodes and open_nodes; create_entities is 'Unknown tool'; "
            "session one's binding lists all nine",
            lease.get("access") == "ReadOnly"
            and tools == set(READ_TOOLS)
            and "Unknown tool" in (call.get("error") or "")
            and set(READ_TOOLS + WRITE_TOOLS) <= set(self.read_write_tools),
            f"read-only lists {sorted(tools)}; read-write {len(self.read_write_tools)}; "
            f"create_entities: {call.get('error') or call}",
        )
        before = self.graph_names()
        marker = f"{self.gate_id}-bypass"
        bodies = corpus_bodies("create_entities", "read_graph") + [
            control_body("create_entities", marker)
        ]
        raw = in_pod(
            self.agent_pod(),
            AGENT_CONTAINER,
            _RAW_PROGRAM,
            {
                "url": self.endpoint("memory"),
                "bearer": self.lease_token("memory", "two"),
                "bodies": [base64.b64encode(body).decode() for body in bodies],
            },
            timeout=600,
        )
        ok, detail = corpus_verdict(raw, sent=len(bodies), write_tool="create_entities")
        after = self.graph_names()
        self.report.check(
            "readonly: the D5a review's parsing-differential corpus, sent with the "
            "ReadOnly lease through the front and the bridge, never reaches the "
            "stock server as a write call; the well-formed write is 'Unknown "
            "tool'; the graph is unchanged",
            ok and before == after and marker not in after and bool(before),
            f"{detail} graph {len(before)}->{len(after)}",
        )
        writes = sql(
            "SELECT count(*) FROM security_events WHERE resource_id = "
            f"{lit(lease.get('id', ''))} AND detail LIKE '%operation=write%'"
        )
        self.report.check(
            "readonly: the exchange never saw a write for the ReadOnly lease",
            writes == "0",
            f"{writes} write events",
        )

    def ending_checks(self) -> None:
        lease_two = self.lease_id("memory", "two")
        lease_one = self.lease_id("memory", "one")
        pids = self.pids
        self.owner.ok(
            "DELETE", f"/api/persistent/threads/{self.threads['two']}?force=true"
        )
        revoked = wait_for(
            "End revokes session two's lease",
            lambda: sql(
                "SELECT coalesce(revoke_reason, '') FROM connector_credential_leases "
                f"WHERE id = {lit(lease_two)} AND revoked_at IS NOT NULL"
            )
            or None,
            timeout=120,
            interval=3,
        )

        def stopped() -> dict | None:
            status = self.bridge_status(self.memory_pod)
            bindings = {item.get("binding") for item in status.get("processes") or []}
            return status if lease_two not in bindings else None

        try:
            # The front's revocation lag (30 s) and its sweep (30 s).
            status = wait_for(
                "session two's process stops", stopped, timeout=150, interval=5
            )
            ended = True
        except GateError:
            status, ended = self.bridge_status(self.memory_pod), False
        gone = "missing" in self.environ(
            self.memory_pod, pids.get("two", 0), self.tokens["memory"]
        )
        bindings = {
            item.get("binding"): item.get("pid")
            for item in status.get("processes") or []
        }
        self.report.check(
            "ending: session two's End revokes its lease and its binding's process "
            "stops (gone from the bridge and the pod), while session one's runs on",
            ended
            and gone
            and bindings.get(lease_one) == pids.get("one")
            and revoked != "",
            f"revoke={revoked} processes={bindings} two={pids.get('two')} gone={gone}",
        )

    def cleanup(self) -> list[str]:
        # The read-only project goes while the OAuth client the owner logs in
        # with still exists (the D5a cleanup deletes that last), once its
        # session's row is gone (a project with a thread row is kept); the
        # D5a cleanup then removes everything else.
        problems: list[str] = []
        project = self.projects.get("ro")
        thread = self.threads.get("two")
        if project:
            try:
                if thread:
                    self.owner.call(
                        "DELETE",
                        f"/api/persistent/threads/{thread}?force=true&permanent=true",
                    )

                def project_deleted() -> bool:
                    status, _body = self.owner.call(
                        "DELETE", f"/api/projects/{project}"
                    )
                    return status in (200, 204, 404)

                wait_for(
                    "project ro deleted", project_deleted, timeout=300, interval=10
                )
            except GateError as exc:
                problems.append(f"delete project ro ({exc})")
                print(f"cleanup: delete project ro failed ({exc})", flush=True)
        return problems + super().cleanup()

    def residue(self) -> list[str]:
        left = super().residue()
        project = self.projects.get("ro")
        if (
            project
            and sql(f"SELECT count(*) FROM projects WHERE id = {lit(project)}") != "0"
        ):
            left.append(f"project {project}")
        return left

    # -- run -------------------------------------------------------------------
    def run(self) -> int:
        try:
            self.preflight()
            self.accounts()
            self.fixture()
            # startup creates the sessions and the pods every later phase
            # needs; a failure there ends the run (cleanup still runs).
            self.startup_checks()
            for phase in (
                self.processes_checks,
                self.credential_checks,
                self.denied_checks,
                self.workspace_checks,
                self.readonly_checks,
                self.ending_checks,
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
                            "connectors": self.connectors,
                            "threads": self.threads,
                            "projects": self.projects,
                            "account": self.other.username,
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
  connectors.servicePods.maxInstallation: 4
  connectors.servicePods.reconcileIntervalSeconds: 5
  connectors.servicePods.refusedCidrs: [172.16.0.0/12, 10.42.0.0/16, 10.43.0.0/16, 169.254.0.0/16]
  connectors.drivers.mcpFront.image: {repository: srw-registry:5000/srw-driver-mcp-front, tag: dev, digest: <any sha256>}
  connectors.drivers.mcpStdioTest.enabled: true
The stock image is the chart's default (docker.io/mcp/memory:latest at a
pinned digest), pulled from Docker Hub by the node. Tilt builds the shim and
the front (whose image now carries the stdio bridge, from drivers/mcp-bridge)
and pins both by digest.
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
    parser.add_argument("--start-timeout", type=int, default=300)
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
        raise SafetyError("--gate-id must be d5b- followed by 10 hex digits")
    if not base._MODEL_RE.fullmatch(args.model):
        raise SafetyError("model id is malformed")
    if not re.fullmatch(r"[a-z][a-z0-9._-]{0,63}", args.user):
        raise SafetyError("user name is malformed")
    if not 60 <= args.turn_timeout <= 1800:
        raise SafetyError("--turn-timeout must be between 60 and 1800 seconds")
    if not 60 <= args.start_timeout <= 1800:
        raise SafetyError("--start-timeout must be between 60 and 1800 seconds")


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
    if args.gate_id is None:
        args.gate_id = f"d5b-{secrets.token_hex(5)}"
    return StdioGate(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
