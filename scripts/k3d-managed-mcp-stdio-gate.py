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
writes shows which process a call reached (each binding's graph is a file in
its process's private directory). It takes no credential of its own;
srw.mcp-stdio-test/v1 delivers the connector's token to each binding's
process as MCP_STDIO_TEST_TOKEN anyway. Its image has a shell, which the gate
uses to read a process's parent, user and command line (never its
environment: the server container's root has no CAP_SYS_PTRACE, so it cannot
read another user's, which is the point).

mcp/memory has no tool that looks around the pod, so the isolation checks
run against a second development server, srw.mcp-stdio-probe/v1: SRW's own
MCP test server (the mcpTest image) in stdio mode, whose probe tools report,
from inside a binding's process, its user, capabilities, no_new_privs,
directories and variables' names, and whether it may read a path, connect
to a socket or signal a process (never what it read). Its whoami reports the
SHA-256 of the credential in its environment, which is how the gate traces
a stdio credential to its binding's process.

Fixtures (all disposable, named after the gate id):

  client      ``<gate id>-oauth`` and the second account (the D3c fixture)
  projects    two of the owner: ``rw``, and ``ro``, whose link of the memory
              connector is read-only
  connectors  of the owner: ``memory`` (srw.mcp-stdio-test/v1, ReadWrite,
              a random fake token) and ``probe`` (srw.mcp-stdio-probe/v1,
              ReadWrite, its own token); of the second account: ``stranger``
              (srw.mcp-stdio-test/v1, its own token)
  sessions    ``one`` (the owner, project rw, memory and probe: ReadWrite
              bindings), ``two`` (the owner, project ro, memory: a ReadOnly
              binding of the same connector, so the same pod, and probe) and
              ``three`` (the second account, stranger only)

Checks (each printed PASS/FAIL; the exit status is 0 only if all pass):

  preflight   orchestrator and stateless agent pods serve this checkout's
              D5a and D5b modules, byte for byte; hosting on with an exchange
              and canary port, a digest-pinned shim and front (whose image
              carries the bridge), the stdio test server installed at a
              pinned digest and the probe server, room for three pods;
              migrations 0360-0363 and 0390-0392; the driver namespace's
              baseline and default deny, enforced (12-probe harness);
              refusedCidrs covers the node; Docker Hub's registry and token
              hosts resolve and answer from the orchestrator (a bind reads the
              stock image's config there)
  startup     each pod's canary wait ran first and exited 0, then the bridge
              install from the pinned front image; the server container runs
              the image at its digest with the bridge as its command (on its
              socket, each process as a user of its own) and the image's own
              program after it, as root with the bridge's capabilities alone,
              the socket's and the homes' emptyDirs mounted, the front with
              the socket's read-only; nothing of SRW's in its environment and
              no delivery mount; its Secret holds no credential; the bridge
              answers its status on its socket
  processes   both sessions' own clients connected the server (README: mcp,
              N tools, managed by SRW); the bridge in the one memory pod runs
              two processes, one per binding (session one's and session two's
              leases), with two process ids and two users, both children of
              the bridge running node; the agent's own client, connecting with
              session one's lease, keeps that binding's process (the same id):
              a process serves its binding's sessions; an entity session one
              creates is not in session two's graph (each binding's process
              has its own private directory)
  isolation   in the probe pod, from inside session one's process: it runs as
              a pool user (not root, not session two's), with no capability
              (permitted, effective, ambient) and no_new_privs, its user's
              process limit (256) and no core dump, its private
              directory as HOME and TMPDIR, the probe connector's token in its
              environment (whoami's digest) and no SRW_ variable; it may not
              read session two's process environment or list its directory,
              signal it, or connect to the bridge's socket (so it cannot name
              a binding or take session two's process over)
  credential  the agent pod holds no token of any connector (environments,
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
              processes stop (they leave the bridge's status and the pod's
              process table, in the memory and the probe pod) while session
              one's run on
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
import hashlib
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
#: SRW's MCP test server in stdio mode (the mcpTest image): the probes.
PROBE_TYPE = "mcp_stdio_probe"
PROBE_DRIVER = "srw.mcp-stdio-probe/v1"
PROBE_PROGRAM = ["/srw-mcp-test", "-stdio", "-credential-env", "MCP_TEST_TOKEN"]
PROBE_TOKEN_ENV = "MCP_TEST_TOKEN"
BRIDGE = "/srw/bin/srw-mcp-bridge"
#: The bridge's socket (shared/connectors/mcp.py BRIDGE_SOCKET): the front's
#: group's alone.
BRIDGE_SOCKET = "/srw/bridge/bridge.sock"
#: The processes' users and directories (BINDING_UID_BASE, BINDING_HOME_ROOT).
UID_BASE = 20000
HOME_ROOT = "/srw/home"
#: The processes and threads each binding's user may have (the specs'
#: default process_limit).
PROCESS_LIMIT = 256
BRIDGE_CAPABILITIES = ["CHOWN", "DAC_OVERRIDE", "FOWNER", "KILL", "SETGID", "SETUID"]
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

# A process's parent, user and command line, read in the server container by
# its shell (all world-readable; its environment is not, even to the
# container's root, which has no CAP_SYS_PTRACE).
_PROCESS_SCRIPT = r"""
pid=$1
[ -e "/proc/$pid/stat" ] || { echo missing; exit 0; }
stat=$(cat "/proc/$pid/stat" 2>/dev/null)
set -- ${stat##*) }
printf 'state=%s\nparent=%s\n' "$1" "$2"
while read -r key real rest; do
  [ "$key" = "Uid:" ] && printf 'uid=%s\n' "$real"
done < "/proc/$pid/status" 2>/dev/null
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


def process_verdict(lines: str, uid: int) -> tuple[bool, str]:
    """A binding's process is the bridge's child running the server's
    program, as the user the bridge reports (a pool user, not root)."""
    found = dict(line.split("=", 1) for line in lines.splitlines() if "=" in line)
    ok = (
        found.get("parent") == "1"
        and found.get("state") not in (None, "Z")
        and found.get("program", "").startswith(" ".join(STOCK_PROGRAM))
        and found.get("uid") == str(uid)
        and UID_BASE <= uid < UID_BASE + 64 * 2 + 2
    )
    return ok, json.dumps(found)


def process_gone(lines: str) -> bool:
    """The process left the pod's process table (or is a zombie)."""
    found = dict(line.split("=", 1) for line in lines.splitlines() if "=" in line)
    return "missing" in lines or found.get("state") == "Z"


def _refused(outcome: str) -> bool:
    return outcome.startswith("refused:") and (
        "permission denied" in outcome or "operation not permitted" in outcome
    )


def isolation_verdict(
    one: dict, two: dict, probes: dict[str, str], *, token_sha256: str, whoami: dict
) -> tuple[bool, str]:
    """From inside session one's probe process: it runs as a pool user of its
    own with no capability and no_new_privs, its private directory as HOME
    and TMPDIR, the probe connector's token and nothing of SRW's in its
    environment; and every probe of session two's process and of the
    bridge's socket was refused."""
    uid = one.get("uid")
    pool = range(UID_BASE, UID_BASE + 64 * 2 + 2)
    home = f"{HOME_ROOT}/{uid}"
    names = one.get("env_names") or []
    problems = []
    if not isinstance(uid, int) or uid not in pool or uid == two.get("uid"):
        problems.append(f"user {uid} (session two's {two.get('uid')})")
    if one.get("gid") != uid:
        problems.append(f"group {one.get('gid')}")
    for capability in ("CapPrm", "CapEff", "CapAmb"):
        if (one.get(capability) or "x").strip("0") != "":
            problems.append(f"{capability}={one.get(capability)}")
    if one.get("NoNewPrivs") != "1":
        problems.append(f"NoNewPrivs={one.get('NoNewPrivs')}")
    if one.get("max_processes") != str(PROCESS_LIMIT) or one.get("max_core") != "0":
        problems.append(
            f"limits processes={one.get('max_processes')} core={one.get('max_core')}"
        )
    if one.get("home") != home or one.get("tmpdir") != home:
        problems.append(f"home={one.get('home')} tmpdir={one.get('tmpdir')}")
    if any(name.upper().startswith("SRW_") for name in names):
        problems.append("an SRW_ variable")
    if PROBE_TOKEN_ENV not in names or whoami.get("credential_sha256") != token_sha256:
        problems.append("not its binding's credential")
    allowed = sorted(name for name, outcome in probes.items() if not _refused(outcome))
    if allowed:
        problems.append(f"allowed {allowed}")
    return not problems, (
        f"uid={uid} two={two.get('uid')} probes={probes} "
        + ("; ".join(problems) if problems else "isolated")
    )


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


def _flag(command_: list[str], name: str) -> str | None:
    return command_[command_.index(name) + 1] if name in command_[:-1] else None


def stdio_layout_problems(
    pod: dict,
    front_image: str,
    *,
    image: str = f"{STOCK_IMAGE}@{STOCK_DIGEST}",
    program: list[str] = STOCK_PROGRAM,
    token_env: str = TOKEN_ENV,
) -> list[str]:
    """How a stdio pod differs from the image at its digest behind the
    bridge (installed from the pinned front image, serving the front on its
    socket, each process as a user of its own) beside the front."""
    problems = base.layout_problems(pod, front_image)
    spec = pod.get("spec") or {}
    containers = {c.get("name"): c for c in spec.get("containers") or []}
    server = containers.get("driver") or {}
    front = containers.get("front") or {}
    if server.get("image") != image:
        problems.append(f"the server runs {server.get('image')}, not {image}")
    command_ = server.get("command") or []
    tail = ["--", *program]
    if command_[:2] != [BRIDGE, "serve"] or command_[-len(tail) :] != tail:
        problems.append(f"the server's command is {command_}")
    if _flag(command_, "--credential-env") != token_env:
        problems.append("the bridge names no credential variable")
    if (
        _flag(command_, "--socket") != BRIDGE_SOCKET
        or _flag(command_, "--socket-group") != "65532"
        or _flag(command_, "--uid-base") != str(UID_BASE)
        or _flag(command_, "--home-root") != HOME_ROOT
        or "--listen" in command_
    ):
        problems.append("the bridge does not serve its socket with users of its own")
    if _flag(command_, "--process-limit") != str(PROCESS_LIMIT):
        problems.append("the bridge caps no binding's processes")
    mounts = {m.get("name"): m for m in server.get("volumeMounts") or []}
    if not (mounts.get("srw-bin") or {}).get("readOnly"):
        problems.append("the bridge is not mounted read-only")
    if (mounts.get("srw-bridge") or {}).get("mountPath") != "/srw/bridge" or (
        mounts.get("srw-home") or {}
    ).get("mountPath") != HOME_ROOT:
        problems.append("the socket's or the homes' emptyDir is not mounted")
    front_mounts = {m.get("name"): m for m in front.get("volumeMounts") or []}
    if not (front_mounts.get("srw-bridge") or {}).get("readOnly"):
        problems.append("the front does not mount the socket's directory read-only")
    security = server.get("securityContext") or {}
    if security.get("readOnlyRootFilesystem") is not True:
        problems.append("the server's root filesystem is writable")
    if (
        security.get("runAsUser") != 0
        or security.get("allowPrivilegeEscalation") is not False
        or security.get("capabilities") != {"drop": ["ALL"], "add": BRIDGE_CAPABILITIES}
    ):
        problems.append(f"the server's securityContext is {security}")
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
    if any(e.get("name") == token_env for e in server.get("env") or []):
        problems.append("the pod sets the credential variable itself")
    return problems


PLAN = [
    "preflight: orchestrator and stateless agent pods serve this checkout's "
    "D5a and D5b modules; hosting on with an exchange and canary port, a "
    "digest-pinned shim and front (carrying the bridge), the stdio test server "
    "installed at its pinned stock digest and the stdio probe server, room for "
    "three pods; migrations 0360-0363 and 0390-0392; the driver namespace's "
    "baseline and default deny, enforced (12-probe harness); refusedCidrs "
    "covers the node; Docker Hub resolves and answers from the orchestrator",
    "accounts: a disposable OAuth client and second account; the owner is an "
    "administrator, the second account is not",
    "startup: each pod's canary wait, then the bridge install from the front's "
    "image; the image at its digest behind the bridge (its socket, users of "
    "its own, root with the bridge's capabilities alone), its program after "
    "it and nothing of SRW's; no credential in its Secret; the bridge answers "
    "its status on its socket",
    "processes: both sessions' clients connected; the one memory pod runs two "
    "processes, one per binding, children of the bridge, as two users; a new "
    "session of a binding keeps its process; session two's graph does not "
    "hold what session one wrote",
    "isolation: from inside session one's probe process: a pool user, no "
    "capability, no_new_privs, its user's process limit and no core dump, a "
    "private HOME and TMPDIR, its binding's token "
    "and no SRW_ variable; session two's environment, directory and process "
    "and the bridge's socket are refused to it",
    "credential: the agent pod and session one's workspace hold no token; the "
    "front's and the bridge's logs hold no token or lease token",
    "denied: no token, a malformed one and session three's lease of another "
    "connector get 401 from the memory endpoint, and start no process",
    "workspace: session one's workspace cannot connect to the memory endpoint",
    "readonly: session two lists only the three read tools and create_entities "
    "is 'Unknown tool'; the D5a review's corpus sent with its lease never "
    "reaches the server as a write call; its graph is unchanged",
    "ending: session two ends; its binding's processes stop, session one's runs on",
    "cleanup: sessions, connectors, projects, account, OAuth client and probe "
    "pods are gone; no driver-namespace object names this run's connectors",
]


class StdioGate(base.ManagedMcpGate):
    def __init__(self, args: argparse.Namespace) -> None:
        super().__init__(args)
        self.projects: dict[str, str] = {}
        self.tokens = {
            label: secret(f"d5b-upstream-{label}-{secrets.token_hex(16)}")
            for label in ("memory", "stranger", "probe")
        }
        self.memory_pod = ""
        self.probe_pod = ""
        self.probe_image = ""
        self.read_write_tools: list[str] = []
        # The process id and user of each memory binding (session label ->).
        self.pids: dict[str, int] = {}
        self.uids: dict[str, int] = {}
        self.probe_pids: dict[str, int] = {}

    # -- naming and helpers ------------------------------------------------
    def title(self, label: str) -> str:
        return f"D5b managed MCP stdio gate {self.gate_id} {label}"

    def entry(self, label: str, session: str, *, token: str | None = None) -> dict:
        found = super().entry(label, session, token=token)
        found["type"] = PROBE_TYPE if label == "probe" else STDIO_TYPE
        return found

    def bridge_status(self, pod: str) -> dict:
        """GET /srw/status on the bridge's socket, from the server container
        (its root may enter the socket's directory; no binding's process
        may)."""
        out = command(
            self.kc
            + ["exec", pod, "-c", "driver", "--", BRIDGE, "status"]
            + ["--socket", BRIDGE_SOCKET],
            timeout=60,
        )
        return json.loads(out.splitlines()[-1])

    def process(self, pod: str, pid: int) -> str:
        return command(
            self.kc
            + ["exec", pod, "-c", "driver", "--"]
            + ["sh", "-c", _PROCESS_SCRIPT, "process", str(pid)],
            timeout=60,
        )

    def probe_calls(self, session: str, calls: list[tuple[str, dict]]) -> list[str]:
        """Probe tools, from inside session ``session``'s probe process (one
        client session, so one process)."""
        result = self.client(
            self.entry("probe", session),
            [
                {"tool": tool, **({"arguments": arguments} if arguments else {})}
                for tool, arguments in calls
            ],
        )
        found = result.get("calls") or []
        if len(found) != len(calls) or any(call.get("error") for call in found):
            raise GateError(
                f"probes for session {session}: {result.get('status')} "
                f"{[call.get('error') for call in found]}"
            )
        return [call.get("text") or "" for call in found]

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
            and bool(managed.get(PROBE_DRIVER))
        )
        self.report.check(
            "preflight: hosting on with an exchange and canary port, a "
            "digest-pinned shim and front, the stdio test server installed at "
            "its pinned stock digest, and the stdio probe server",
            configured,
            json.dumps(env),
        )
        if not configured:
            raise GateError(
                "set connectors.servicePods.enabled, connectors.drivers.mcpFront, "
                "connectors.drivers.mcpStdioTest.enabled and "
                "connectors.drivers.mcpStdioProbe.enabled (with mcpTest's image) "
                "as the k3d profile does, under Tilt"
            )
        self.namespace = env["CONNECTOR_SERVICE_NAMESPACE"]
        self.front_image = env["CONNECTOR_MCP_FRONT_IMAGE"]
        # The pod runs the image by the digest a bind resolved: compared by
        # repository then.
        self.probe_image = str(managed[PROBE_DRIVER]).split("@")[0].rsplit(":", 1)[0]
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
        room = live.isdigit() and int(live) + 3 <= cap
        self.report.check(
            "preflight: the installation has room for this run's three pods",
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
        self.create_connector(
            "probe",
            {
                "type": PROBE_TYPE,
                "credentials": {"token": self.tokens["probe"]},
                "config": {"access": "ReadWrite", "message": self.gate_id},
            },
            "owner",
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
        self.create_session("one", ["memory", "probe"], "owner", project="rw")
        self.create_session("two", ["memory", "probe"], "owner", project="ro")
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
            if label == "probe":
                # The mcpTest image, at the digest the bind resolved.
                running = next(
                    (
                        c.get("image", "")
                        for c in (pod.get("spec") or {}).get("containers") or []
                        if c.get("name") == "driver"
                    ),
                    "",
                )
                expected = (
                    running
                    if running.startswith(f"{self.probe_image}@sha256:")
                    else f"{self.probe_image}@sha256:<a pinned digest>"
                )
                problems = stdio_layout_problems(
                    pod,
                    self.front_image,
                    image=expected,
                    program=PROBE_PROGRAM,
                    token_env=PROBE_TOKEN_ENV,
                )
            else:
                problems = stdio_layout_problems(pod, self.front_image)
            self.report.check(
                f"startup: {label}'s pod runs its image at its digest behind the "
                "bridge from the pinned front image (on its socket, each process "
                "as a user of its own, the container root with the bridge's "
                "capabilities alone), with nothing of SRW's",
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
            variable = PROBE_TOKEN_ENV if label == "probe" else TOKEN_ENV
            self.report.check(
                f"startup: {label}'s pod Secret holds no credential; the front's "
                "block names the stdio bridge's socket and the credential's "
                "variable",
                request.get("credentials") == {}
                and not held
                and (request.get("mcp") or {}).get("transport") == "stdio"
                and (request.get("mcp") or {}).get("socket") == BRIDGE_SOCKET
                and (request.get("mcp") or {}).get("credential") == {"env": variable},
                f"credentials={sorted(request.get('credentials') or {})} held={held}",
            )
        self.memory_pod = rows["memory"]["pod_name"]
        self.probe_pod = rows["probe"]["pod_name"]
        try:
            status = self.bridge_status(self.memory_pod)
        except (GateError, ValueError) as exc:
            status = {"error": str(exc)}
        # "session" and "uid" are this checkout's bridge's (a process per
        # binding that serves its later sessions, each as a user of its own).
        self.report.check(
            "startup: the memory pod's bridge answers its status on its socket "
            "(this checkout's: it reports each process's session and user)",
            status.get("credential_env") == TOKEN_ENV
            and all(
                "session" in item and int(item.get("uid") or 0) >= UID_BASE
                for item in status.get("processes") or []
            ),
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
        self.uids = {
            label: int(
                next(
                    (
                        item.get("uid") or 0
                        for item in status.get("processes") or []
                        if item.get("binding") == lease
                    ),
                    0,
                )
            )
            for label, lease in leases.items()
        }
        self.report.check(
            "processes: two sessions get two processes in one pod, one per "
            "binding, each holding its credential, as two users",
            ok
            and len(self.live_pods("memory")) == 1
            and len(set(self.uids.values())) == 2,
            f"{detail} uids={self.uids}",
        )
        for label, pid in pids.items():
            ok, detail = process_verdict(
                self.process(self.memory_pod, pid), self.uids.get(label, 0)
            )
            self.report.check(
                f"processes: session {label}'s process is the bridge's child "
                "running node as its binding's user (not root)",
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
            "processes: session two's binding does not see the entity session "
            "one wrote (each binding's graph is in its own process's private "
            "directory)",
            read["status"] == "connected"
            and not opened.get("error")
            and marker not in (opened.get("text") or ""),
            (opened.get("text") or opened.get("error") or "")[:160],
        )

    def isolation_checks(self) -> None:
        """From inside session one's probe process (the probe server's tools
        run as that process): its own user and privileges, and what it may
        reach of session two's process and of the bridge."""
        leases = {label: self.lease_id("probe", label) for label in ("one", "two")}
        (raw_two,) = self.probe_calls("two", [("self_status", {})])
        two = json.loads(raw_two)
        attempts = {
            "session two's environment": (
                "probe_path",
                {"path": f"/proc/{two.get('pid')}/environ"},
            ),
            "session two's directory": ("probe_path", {"path": str(two.get("home"))}),
            "the home root": ("probe_path", {"path": HOME_ROOT}),
            "session two's process": ("probe_signal", {"pid": str(two.get("pid"))}),
            "the bridge's socket": ("probe_socket", {"path": BRIDGE_SOCKET}),
            "the bridge's environment": ("probe_path", {"path": "/proc/1/environ"}),
        }
        raw_one, raw_whoami, *outcomes = self.probe_calls(
            "one", [("self_status", {}), ("whoami", {}), *attempts.values()]
        )
        one, whoami = json.loads(raw_one), json.loads(raw_whoami)
        probes = dict(zip(attempts, outcomes))
        status = self.bridge_status(self.probe_pod)
        token_sha256 = hashlib.sha256(self.tokens["probe"].encode()).hexdigest()
        ok, detail = isolation_verdict(
            one, two, probes, token_sha256=token_sha256, whoami=whoami
        )
        listed = {
            item.get("binding"): item.get("uid")
            for item in status.get("processes") or []
            if not item.get("probe")
        }
        self.probe_pids = {
            "one": int(one.get("pid") or 0),
            "two": int(two.get("pid") or 0),
        }
        self.report.check(
            "isolation: session one's process runs as a user of its own with no "
            "capability and no_new_privs, its private directory as HOME and "
            "TMPDIR, its binding's credential and nothing of SRW's; it may not "
            "read session two's environment or directory, signal its process, "
            "or connect to the bridge (so it cannot take session two's process "
            "over or end it)",
            ok
            and listed.get(leases["one"]) == one.get("uid")
            and listed.get(leases["two"]) == two.get("uid"),
            f"{detail} bridge={listed}",
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

    def graph_names(self, session: str) -> set[str] | None:
        """The entities in the graph of ``session``'s memory process (each
        binding's own), or None when it cannot be read."""
        graph = self.client(self.entry("memory", session), [{"tool": "read_graph"}])
        (read,) = graph["calls"] or [{}]
        try:
            parsed = json.loads(read.get("text") or "")
        except ValueError:
            return None
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
        # The corpus goes with session two's lease, to session two's process:
        # its graph is the one a write would change.
        before = self.graph_names("two")
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
        after = self.graph_names("two")
        self.report.check(
            "readonly: the D5a review's parsing-differential corpus, sent with the "
            "ReadOnly lease through the front and the bridge, never reaches the "
            "stock server as a write call; the well-formed write is 'Unknown "
            "tool'; the ReadOnly binding's graph is unchanged",
            ok
            and before is not None
            and after is not None
            and before == after
            and marker not in after,
            f"{detail} graph {before}->{after}",
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
        probe_two = self.lease_id("probe", "two") if self.probe_pod else ""
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
            if lease_two in bindings:
                return None
            if probe_two:
                probe = self.bridge_status(self.probe_pod)
                if probe_two in {
                    item.get("binding") for item in probe.get("processes") or []
                }:
                    return None
            return status

        try:
            # The front's revocation lag (30 s) and its sweep (30 s).
            status = wait_for(
                "session two's processes stop", stopped, timeout=150, interval=5
            )
            ended = True
        except GateError:
            status, ended = self.bridge_status(self.memory_pod), False
        gone = process_gone(self.process(self.memory_pod, pids.get("two", 0)))
        bindings = {
            item.get("binding"): item.get("pid")
            for item in status.get("processes") or []
        }
        self.report.check(
            "ending: session two's End revokes its lease and its binding's "
            "processes stop (gone from the bridges and the pod), while session "
            "one's runs on",
            ended
            and gone
            and bindings.get(lease_one) == pids.get("one")
            and revoked != ""
            and not status.get("held_users"),
            f"revoke={revoked} processes={bindings} two={pids.get('two')} "
            f"gone={gone} held={status.get('held_users')}",
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
                self.isolation_checks,
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
  connectors.drivers.mcpTest: {enabled: true, image: {repository: srw-registry:5000/srw-driver-mcp-test, tag: dev}}
  connectors.drivers.mcpStdioTest.enabled: true
  connectors.drivers.mcpStdioProbe.enabled: true
The stock image is the chart's default (docker.io/mcp/memory:latest at a
pinned digest), pulled from Docker Hub by the node. Tilt builds the shim, the
front (whose image now carries the stdio bridge, from drivers/mcp-bridge) and
the MCP test server (whose -stdio mode is the probe server), and pins them by
digest. The driver namespace stays at Pod Security baseline: a stdio server's
container runs as root with SETUID, SETGID, KILL, CHOWN, DAC_OVERRIDE and
FOWNER (all allowed by baseline) for the bridge alone.
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
