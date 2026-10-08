#!/usr/bin/env python3
"""Local k3d gate for connector drivers C5: provider-minted credentials.

Design: knowledge-base/knowledge/features/connector_drivers.md, "Three ways
to give an agent ephemeral authority" (item 1), "The lease service", "The
git swap driver" and slice C5. Templates: scripts/k3d-git-swap-gate.py (C3:
the envelope, the disposable OAuth client, the in-pod programs, imported,
not copied) and scripts/k3d-kubeconfig-connector-gate.py (D1d: the
workspace's egress to the API server). Dry-run by default, the exact
k3d-srw/srw context, secrets only on ``kubectl exec -i`` stdin and scrubbed
from every printed line, every in-pod program capping its own memory, and a
cleanup in ``finally`` that touches only what this run created and then
checks for residue by gate id.

Fixtures (all disposable, named after the gate id ``c5-<10 hex>``):

  client      ``<gate id>-oauth``, a public Keycloak client with direct
              access grants, which the owner logs in with (the D3c fixture)
  identities  namespace ``srw-gate-<gate id>-ids``: the target
              ServiceAccount ``agent`` (nothing granted there), the minting
              ServiceAccount ``minter`` and its Role, the documented minimum:
              ``create`` on ``serviceaccounts/token`` for ``agent`` only, and
              ``create``/``delete`` on Secrets (SRW's bound Secrets live
              there). Its one-hour token, minted here, is the connector's
              minting credential
  work        namespace ``srw-gate-<gate id>-work``: a Role that may get and
              list ConfigMaps and Pods, bound to ``agent``, and a marker
              ConfigMap
  egress      workspace pods may not reach the API server (the tier policies
              deny the cluster CIDRs), so, as the D1d gate does, one
              NetworkPolicy per unit selects only that unit's workspace pod
              and allows only the API server's Service IP and endpoints
  project     one project owned by the test account
  connectors  ``kube``: a kubeconfig connector with ``token_request``
              (``agent``, expiration_seconds 600). With --github-*:
              ``gh-rw`` and ``gh-ro``, repository connectors authenticating
              as the App on the given disposable repository, linked
              ReadWrite and read-only

Checks (each printed PASS/FAIL; the exit status is 0 only if all pass):

  preflight   every orchestrator and stateless agent pod serves this
              checkout's C5 modules, byte for byte; migrations 0347 and 0430
              applied; the lease sweep's interval read from the orchestrator
  accounts    the owner logs in with the disposable OAuth client
  fixture     the minting account may create a token for ``agent`` only and
              may not read Secrets; a token_request connector over an exec
              kubeconfig is refused; Test of ``kube`` mints and revokes, and
              leaves no Secret
  mint        a stateless session (sandbox) with ``kube``; after its first
              turn: one live minted credential; the workspace's kubeconfig
              has one user holding a token and nothing else (no exec, no
              client certificate), the token is the one SRW minted (by
              digest), its ``sub`` is ``agent``, it is bound to a srw-mint-
              Secret and its ``exp`` is short (600 s); kubectl reads the
              marker and may do nothing else (create ConfigMaps or Pods, read
              Secrets: no); the workspace and the stateless agent pods hold
              the minting token nowhere (files, environments, command lines)
  renewal     past half the token's life, the next turn's claim delivers a
              new token (by digest) before the old one expires; the old one
              is superseded and still valid, the new one is live
  end         End revokes: every credential of the session is revoked, its
              bound Secrets are gone and both tokens get 401 from the API
              server
  github      only with --github-app-id, --github-installation-id,
              --github-key-file and --github-repo (else SKIPPED, saying why):
              Test mints, reads and revokes; a session with gh-rw and gh-ro
              clones both (through the git swap driver where it serves them,
              else on the stated token-in-URL fallback); each connector's
              minted token covers the one repository, the read-only one cannot
              write (a blob create gets 403) and the ReadWrite one can; the
              workspace never holds the App's key, and through the driver no
              installation token either; End revokes both tokens (401)
  cleanup     sessions, connectors, project, OAuth client, both namespaces
              and the NetworkPolicies are gone; no credential of this run's
              connectors is left unrevoked

Run with the repository venv on the k3d-srw cluster, alone: this is a
mutating gate. It needs the workspace image built from this checkout (full
target: kubectl is installed there).

  .venv/bin/python scripts/k3d-provider-minted-gate.py           # plan
  .venv/bin/python scripts/k3d-provider-minted-gate.py \\
      --run --confirm LOCAL-K3D-DISPOSABLE
  .venv/bin/python scripts/k3d-provider-minted-gate.py \\
      --run --confirm LOCAL-K3D-DISPOSABLE --github-app-id 123 \\
      --github-installation-id 456 --github-key-file ~/app.pem \\
      --github-repo https://github.com/<you>/<disposable>.git
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
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]


def _load(name: str, path: Path):
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


swap = _load("k3d_git_swap_gate", ROOT / "scripts" / "k3d-git-swap-gate.py")
d1d = _load(
    "k3d_kubeconfig_connector_gate",
    ROOT / "scripts" / "k3d-kubeconfig-connector-gate.py",
)

GateError = swap.GateError
SafetyError = swap.SafetyError
ServedSet = swap.ServedSet
Api = swap.Api
LOCAL_CONTEXT = swap.LOCAL_CONTEXT
LOCAL_NAMESPACE = swap.LOCAL_NAMESPACE
LOCAL_CONFIRMATION = swap.LOCAL_CONFIRMATION
K = swap.K
ORCHESTRATOR = swap.ORCHESTRATOR
ORCHESTRATOR_CONTAINER = swap.ORCHESTRATOR_CONTAINER
WORKSPACE_CONTAINER = swap.WORKSPACE_CONTAINER
POD_ROOT = swap.POD_ROOT
DEFAULT_MODEL = swap.DEFAULT_MODEL
run, command, sql, lit, wait_for, secret, in_pod, expected_bytes = (
    swap.run,
    swap.command,
    swap.sql,
    swap.lit,
    swap.wait_for,
    swap.secret,
    swap.in_pod,
    swap.expected_bytes,
)


class Report(swap.Report):
    """The C3 gate's report, and a note line (scrubbed like every line)."""

    def note(self, text: str) -> None:
        print(f"NOTE {swap._scrub(text)}", flush=True)


_GATE_ID_RE = re.compile(r"c5-[0-9a-f]{10}\Z")
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")
_GITHUB_REPO_RE = re.compile(
    r"https://github\.com/([A-Za-z0-9._-]{1,100})/([A-Za-z0-9._-]{1,100}?)(\.git)?\Z"
)
KUBE = ["kubectl", f"--context={LOCAL_CONTEXT}"]
API_SERVER = "https://kubernetes.default.svc"
GATE_LABEL = swap.GATE_LABEL
TARGET_SA = "agent"
MINTER_SA = "minter"
MARKER_CONFIGMAP = "srw-gate-marker"
#: The shortest lifetime the API server mints: the renewal comes after half.
EXPIRATION_SECONDS = 600
SWAP_DRIVER = swap.SWAP_DRIVER
GITHUB_API = "https://api.github.com"
MIGRATIONS = (
    "0347_connector_credential_leases.sql",
    "0430_connector_minted_credentials.sql",
)
SHARED_CONNECTORS = swap.SHARED_CONNECTORS
DRIVERS = swap.DRIVERS
AGENT_CONNECTORS = swap.AGENT_CONNECTORS

SERVED_SETS = (
    ServedSet(
        "orchestrator",
        "orchestrator",
        ORCHESTRATOR_CONTAINER,
        (SHARED_CONNECTORS, DRIVERS),
        (
            "src/orchestrator/application/__init__.py",
            "src/orchestrator/application/background_tasks.py",
            "src/orchestrator/services/agent_datasource_payload.py",
            "src/orchestrator/services/connector_credential_leases.py",
            "src/orchestrator/services/connector_git_swap_delivery.py",
            "src/orchestrator/services/connector_lease_exchange.py",
            "src/orchestrator/services/connector_minted_credentials.py",
            "src/orchestrator/services/connector_service_hosting.py",
            "src/orchestrator/services/datasources.py",
            *(
                f"src/orchestrator/database/migrations/app/{name}"
                for name in MIGRATIONS
            ),
        ),
    ),
    # The agent writes the kubeconfig into the workspace and clones.
    ServedSet(
        "stateless agent",
        "agent-stateless",
        "agent",
        (SHARED_CONNECTORS, AGENT_CONNECTORS),
        (
            "src/shared/runtime/core/credential_env.py",
            "src/shared/runtime/core/backends/remote.py",
        ),
    ),
)

_POD_MEMORY_CAP = swap._POD_MEMORY_CAP

# The minted credentials of one session (and connector), from the app
# database; ``tokens`` decrypts their tokens in the orchestrator (the key
# never leaves the pod). The gate registers every token as a secret before
# it prints anything.
_MINTED_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import asyncio, json, sys
from uuid import UUID
from orchestrator.database.postgres import PostgresDB
from orchestrator.security.crypto import decrypt
cap_memory()
request = json.loads(sys.stdin.readline())

async def main():
    db = PostgresDB(min_connections=1, max_connections=1)
    await db.connect()
    try:
        async with db.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, provider, access, status, revoke_reason, token_ciphertext, "
                "extract(epoch FROM expires_at) AS expires, "
                "extract(epoch FROM minted_at) AS minted, "
                "extract(epoch FROM now()) AS now "
                "FROM connector_minted_credentials "
                "WHERE owner_kind = 'thread' AND owner_id = $1 AND connector_id = $2 "
                "ORDER BY created_at",
                UUID(request["thread"]), UUID(request["connector"]),
            )
    finally:
        await db.close()
    found = []
    for row in rows:
        item = {
            "id": str(row["id"]), "provider": row["provider"], "access": row["access"],
            "status": row["status"], "revoke_reason": row["revoke_reason"],
            "expires": float(row["expires"]) if row["expires"] is not None else None,
            "minted": float(row["minted"]) if row["minted"] is not None else None,
            "now": float(row["now"]),
        }
        if request.get("tokens") and row["token_ciphertext"]:
            item["token"] = decrypt(row["token_ciphertext"])
        found.append(item)
    return found
print(json.dumps(asyncio.run(main())))
"""
)

# Each token on stdin presented, alone, to the API server as a bearer: only
# the statuses come back (200 valid and allowed, 401 not a token any more).
_BEARER_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import json, ssl, sys, urllib.error, urllib.request
cap_memory()
request = json.loads(sys.stdin.readline())
context = ssl.create_default_context(cadata=request["ca"])
opener = urllib.request.build_opener(
    urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=context)
)
statuses = []
for token in request["tokens"]:
    message = urllib.request.Request(
        request["url"], headers={"Authorization": "Bearer " + token}
    )
    try:
        with opener.open(message, timeout=30) as response:
            statuses.append(response.status)
    except urllib.error.HTTPError as error:
        statuses.append(error.code)
    except Exception as exc:
        statuses.append(type(exc).__name__)
print(json.dumps(statuses))
"""
)

# What the workspace's shell sees, as agent-host with the credential
# environment sourced: the kubeconfig's users (their keys, never values), the
# one token's digest and claims, the marker and what kubectl may do. The
# request is inlined (namespaces only, nothing secret).
_WS_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import base64, hashlib, json, os, subprocess
cap_memory()
request = json.loads(REQUEST)

def kubectl(*args):
    done = subprocess.run(
        ["kubectl", *args], capture_output=True, text=True, timeout=60
    )
    return done.returncode, done.stdout.strip()

rc, raw = kubectl("config", "view", "--raw", "-o", "json")
config = json.loads(raw) if rc == 0 and raw else {}
users = [u.get("user") or {} for u in config.get("users") or []]
tokens = [u["token"] for u in users if isinstance(u.get("token"), str)]
out = {
    "kubeconfig_var": bool(os.environ.get("KUBECONFIG")),
    "users": len(users),
    "user_keys": sorted({key for user in users for key in user}),
    "tokens": len(tokens),
}
if len(tokens) == 1:
    token = tokens[0]
    out["digest"] = hashlib.sha256(token.encode()).hexdigest()
    try:
        segment = token.split(".")[1]
        segment += "=" * (-len(segment) % 4)
        claims = json.loads(base64.urlsafe_b64decode(segment))
        bound = claims.get("kubernetes.io") or {}
        out["claims"] = {
            "sub": claims.get("sub"),
            "iat": claims.get("iat"),
            "exp": claims.get("exp"),
            "secret": (bound.get("secret") or {}).get("name"),
            "namespace": bound.get("namespace"),
        }
    except Exception as exc:
        out["claims_error"] = type(exc).__name__
rc, marker = kubectl(
    "-n", request["work"], "get", "configmap", request["marker"],
    "-o", "jsonpath={.data.marker}",
)
out["marker"] = marker if rc == 0 else ""
out["can"] = {}
for verb, resource, namespace in request["can_i"]:
    rc, answer = kubectl("auth", "can-i", verb, resource, "-n", namespace)
    out["can"][f"{verb} {resource} -n {namespace}"] = answer
rc, _listing = kubectl("-n", request["ids"], "get", "secrets")
out["secrets_listed"] = rc == 0
print(json.dumps(out))
"""
)


#: What the target account may and may not do (``kubectl auth can-i`` from
#: the workspace): only its work Role's reads.
def can_i_requests(ids: str, work: str) -> list[list[str]]:
    return [
        ["get", "configmaps", work],
        ["list", "pods", work],
        ["create", "configmaps", work],
        ["create", "pods", work],
        ["get", "secrets", ids],
        ["delete", "secrets", ids],
        ["create", "serviceaccounts", ids],
    ]


ALLOWED = {"get configmaps", "list pods"}

PLAN = [
    "preflight: the orchestrator and the stateless agents serve this checkout's "
    "C5 modules; migrations 0347 and 0430 applied; the lease sweep interval",
    "accounts: a disposable OAuth client the owner logs in with",
    "fixture: namespaces srw-gate-<gate id>-ids (target ServiceAccount agent, "
    "minting ServiceAccount minter with the minimal Role: create "
    "serviceaccounts/token for agent, create/delete secrets) and "
    "srw-gate-<gate id>-work (a read Role bound to agent, a marker); a "
    "project; a kubeconfig connector with token_request (600 s); the minter "
    "may do nothing more; an exec kubeconfig with token_request is refused; "
    "Test mints and revokes",
    "egress: one NetworkPolicy per session letting only its workspace pod "
    "reach the API server",
    "mint: a stateless session; the workspace's kubeconfig holds one minted "
    "token (digest = SRW's), sub agent, bound to a srw-mint- Secret, exp 600 s, "
    "no exec; kubectl reads the marker and nothing more; no minting token in "
    "the workspace or the agent pods",
    "renewal: past half the token's life the next turn delivers a new token "
    "before the old one expires; the old one superseded and still valid",
    "end: End revokes every credential of the session, deletes its bound "
    "Secrets, and both tokens get 401",
    "github (only with --github-*; else skipped): Test; a session with gh-rw "
    "and gh-ro clones both (swap driver or stated fallback); each token covers "
    "the one repository, read-only cannot write, ReadWrite can; no App key (and "
    "through the driver no token) in the workspace; End revokes both (401)",
    "cleanup: sessions, connectors, project, OAuth client, namespaces and "
    "NetworkPolicies gone; no credential of this run left unrevoked",
]

VALUES_LOCAL_KEYS = """values-local.yaml keys (the k3d profile of values-local.yaml.example):
  orchestrator.connectorLeases.sweepIntervalSeconds   the minted-credential
      sweep's cadence too (a revoke request also wakes it at once); the gate
      waits for End's revoke at most max(120, 3 x the interval) seconds
  nothing else for the TokenRequest phase: the orchestrator mints from its
      own pod against https://kubernetes.default.svc
  for the GitHub phase through the git swap driver: the C3 gate's keys
      (orchestrator.connectorLeases.exchangePort, connectors.servicePods.enabled,
      connectors.drivers.gitSwap.enabled and its image); without them the
      phase runs on connectors.drivers.gitSwap.fallback (token-in-url, the
      default) and says so
The GitHub App must be installed on the --github-repo repository with
Contents: Read and write (and nothing else needed); the repository must be
DISPOSABLE: the gate only clones it and creates unreferenced blobs.
"""


def identity_manifests(ids: str, work: str, gate_id: str, marker: str) -> list[dict]:
    """The two namespaces, the accounts, the minimal minting Role and the
    target account's read Role."""
    labels = {GATE_LABEL: gate_id}

    def meta(name: str, namespace: str) -> dict:
        return {"name": name, "namespace": namespace, "labels": labels}

    return [
        {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": ids, "labels": labels},
        },
        {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": work, "labels": labels},
        },
        {
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": meta(TARGET_SA, ids),
        },
        {
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": meta(MINTER_SA, ids),
        },
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "Role",
            "metadata": meta(MINTER_SA, ids),
            "rules": [
                {
                    "apiGroups": [""],
                    "resources": ["serviceaccounts/token"],
                    "resourceNames": [TARGET_SA],
                    "verbs": ["create"],
                },
                {
                    "apiGroups": [""],
                    "resources": ["secrets"],
                    "verbs": ["create", "delete"],
                },
            ],
        },
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "RoleBinding",
            "metadata": meta(MINTER_SA, ids),
            "roleRef": {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "Role",
                "name": MINTER_SA,
            },
            "subjects": [
                {"kind": "ServiceAccount", "name": MINTER_SA, "namespace": ids}
            ],
        },
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "Role",
            "metadata": meta("reader", work),
            "rules": [
                {
                    "apiGroups": [""],
                    "resources": ["configmaps", "pods"],
                    "verbs": ["get", "list"],
                }
            ],
        },
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "RoleBinding",
            "metadata": meta("reader", work),
            "roleRef": {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "Role",
                "name": "reader",
            },
            "subjects": [
                {"kind": "ServiceAccount", "name": TARGET_SA, "namespace": ids}
            ],
        },
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": meta(MARKER_CONFIGMAP, work),
            "data": {"marker": marker},
        },
    ]


def workspace_problems(
    found: dict[str, Any], *, digest: str, ids: str, marker: str
) -> list[str]:
    """What is wrong with the workspace's kubeconfig and kubectl's reach."""
    problems: list[str] = []
    if not found.get("kubeconfig_var"):
        problems.append("KUBECONFIG is not set")
    if found.get("tokens") != 1 or found.get("users") != 1:
        problems.append(f"{found.get('users')} users / {found.get('tokens')} tokens")
    if found.get("user_keys") != ["token"]:
        problems.append(f"the user holds {found.get('user_keys')}")
    if found.get("digest") != digest:
        problems.append("the token is not the one SRW minted")
    claims = found.get("claims") or {}
    if claims.get("sub") != f"system:serviceaccount:{ids}:{TARGET_SA}":
        problems.append(f"sub {claims.get('sub')}")
    if not str(claims.get("secret") or "").startswith("srw-mint-"):
        problems.append(f"bound to {claims.get('secret')!r}, not a srw-mint- Secret")
    try:
        lifetime = int(claims["exp"]) - int(claims["iat"])
    except (KeyError, TypeError, ValueError):
        lifetime = -1
    if not 0 < lifetime <= EXPIRATION_SECONDS + 5:
        problems.append(f"lifetime {lifetime} s")
    if found.get("marker") != marker:
        problems.append("kubectl did not read the marker")
    for question, answer in (found.get("can") or {}).items():
        allowed = question.rsplit(" -n ", 1)[0] in ALLOWED
        if answer != ("yes" if allowed else "no"):
            problems.append(f"can-i {question}: {answer}")
    if found.get("secrets_listed"):
        problems.append("kubectl listed the identity namespace's Secrets")
    return problems


def jwt_claims(token: str) -> dict[str, Any]:
    segment = token.split(".")[1]
    segment += "=" * (-len(segment) % 4)
    return json.loads(base64.urlsafe_b64decode(segment))


def github_call(
    method: str, path: str, token: str, body: Any = None
) -> tuple[int, Any]:
    """A GitHub API call from this workstation with an installation token
    (in a header, never argv or a log line)."""
    request = urllib.request.Request(
        GITHUB_API + path,
        data=None if body is None else json.dumps(body).encode(),
        method=method,
        headers={
            "Authorization": "Bearer " + token,
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "srw-c5-gate",
        },
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=30) as response:
            text = response.read().decode("utf-8", "replace")
            status = response.status
    except urllib.error.HTTPError as error:
        text = error.read().decode("utf-8", "replace")
        status = error.code
    try:
        return status, json.loads(text) if text.strip() else {}
    except ValueError:
        return status, {}


class ProviderMintedGate:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.gate_id = args.gate_id or f"c5-{secrets.token_hex(5)}"
        self.report = Report(self.gate_id)
        self.owner = Api(args.user, args.password)
        self.oauth_client = f"{self.gate_id}-oauth"
        self.client_started = False
        self.client_uuid: str | None = None
        self.owner_id = ""
        self.project: str | None = None
        self.connectors: dict[str, str] = {}
        self.threads: dict[str, str] = {}
        self.policies: list[str] = []
        self.ids = f"srw-gate-{self.gate_id}-ids"
        self.work = f"srw-gate-{self.gate_id}-work"
        self.namespaces_started = False
        self.marker = f"c5-marker-{secrets.token_hex(4)}"
        self.minting_token = ""
        self.ca = ""
        self.sweep_seconds = 60.0
        #: The session's first and renewed credentials (with their tokens).
        self.first: dict[str, Any] = {}
        self.first_exp: Any = None
        self.second: dict[str, Any] = {}
        self.github = bool(args.github_app_id)
        self.github_key = (
            secret(Path(args.github_key_file).expanduser().read_text().strip())
            if args.run and self.github
            else ""
        )

    # -- helpers -------------------------------------------------------------
    def name(self, label: str) -> str:
        return f"{self.gate_id} {label}"

    def title(self, label: str) -> str:
        return f"C5 provider-minted gate {self.gate_id} {label}"

    def keycloak(self, action: str) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "action": action,
            "client": self.oauth_client,
            "marker": self.gate_id,
        }
        if self.client_uuid:
            payload["client_uuid"] = self.client_uuid
        result = in_pod(
            ORCHESTRATOR, ORCHESTRATOR_CONTAINER, swap._KEYCLOAK_PROGRAM, payload
        )
        if result.get("error"):
            raise GateError(f"Keycloak {action}: {result['error']}")
        return result

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
                + ["python", "-c", swap._HASH_PROGRAM, POD_ROOT],
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
                for pod in self.release_pods(
                    f"app=srw-workspace,srw/thread-id={thread}"
                )
                if pod.get("status", {}).get("phase") == "Running"
            ]
            return running[0] if len(running) == 1 else None

        return wait_for(f"workspace of {thread}", probe, timeout=300)

    def ws(self, session: str, script: str, *, timeout: int = 180) -> tuple[int, str]:
        """``script`` as agent-host in a session's workspace (stdin, never
        argv), with the credential environment sourced; output scrubbed."""
        pod = self.workspace_pod(self.threads[session])
        rc, out, err = run(
            K
            + ["exec", "-i", pod, "-c", WORKSPACE_CONTAINER, "--"]
            + ["su", "-s", "/bin/bash", "agent-host", "-c", "bash -s"],
            data=(
                "set -u\ncd ~\nexport GIT_TERMINAL_PROMPT=0\n"
                'for env in ~/.srw-credentials/*.sh; do [ -r "$env" ] && . "$env"; done\n'
                + script
            ),
            timeout=timeout,
        )
        return rc, (out + "\n" + err).strip()

    def ws_kube(self, session: str) -> dict[str, Any]:
        request = json.dumps(
            {
                "work": self.work,
                "ids": self.ids,
                "marker": MARKER_CONFIGMAP,
                "can_i": can_i_requests(self.ids, self.work),
            }
        )
        program = _WS_PROGRAM.replace("REQUEST", repr(request))
        rc, out = self.ws(
            session, "/usr/bin/python3 -I -c " + swap.shlex_quote(program) + "\n"
        )
        try:
            return json.loads(out.splitlines()[-1])
        except (IndexError, ValueError):
            raise GateError(f"the workspace program failed (exit {rc}): {out[-300:]}")

    def minted(self, session: str, label: str, *, tokens: bool = False) -> list[dict]:
        found = in_pod(
            ORCHESTRATOR,
            ORCHESTRATOR_CONTAINER,
            _MINTED_PROGRAM,
            {
                "thread": self.threads[session],
                "connector": self.connectors[label],
                "tokens": tokens,
            },
        )
        for row in found:
            if row.get("token"):
                secret(row["token"])
        return found

    def bearer(self, tokens: list[str]) -> list[Any]:
        """Each token's status at the API server (from the orchestrator pod,
        the token on stdin)."""
        return in_pod(
            ORCHESTRATOR,
            ORCHESTRATOR_CONTAINER,
            _BEARER_PROGRAM,
            {
                "ca": self.ca,
                "tokens": tokens,
                "url": f"{API_SERVER}/api/v1/namespaces/{self.work}/configmaps/"
                f"{MARKER_CONFIGMAP}",
            },
        )

    def queue(self, thread: str) -> tuple[str, int, int] | None:
        row = sql(
            "SELECT state || ' ' || coalesce(input_seq, 0) || ' ' || "
            f"coalesce(consumed_seq, 0) FROM run_queue WHERE unit_id = {lit(thread)}"
        )
        if not row:
            return None
        state, input_seq, consumed_seq = row.split()
        if state == "parked":
            raise GateError("the session unit parked")
        return state, int(input_seq), int(consumed_seq)

    def turn(self, session: str, text: str) -> None:
        thread = self.threads[session]
        before = self.queue(thread)
        previous = before[1] if before else 0
        self.owner.ok(
            "POST", f"/api/persistent/threads/{thread}/input", {"content": text}
        )

        def answered() -> bool:
            current = self.queue(thread)
            return bool(
                current
                and current[0] == "done"
                and current[1] > previous
                and current[1] == current[2]
            )

        wait_for(
            f"session {session} answered", answered, timeout=self.args.turn_timeout
        )

    def create_connector(self, label: str, body: dict[str, Any]) -> tuple[int, Any]:
        status, parsed = self.owner.call(
            "POST",
            "/api/datasources",
            {"name": self.name(label), "scope_mode": "all", **body},
        )
        if isinstance(parsed, dict) and parsed.get("id"):
            self.connectors[label] = str(parsed["id"])
        return status, parsed

    def link(self, label: str, *, read_only: bool) -> None:
        self.owner.ok(
            "POST",
            f"/api/projects/{self.project}/datasources/{self.connectors[label]}",
            {"read_only": read_only},
        )

    def create_session(self, label: str, connectors: list[str]) -> str:
        created = self.owner.ok(
            "POST",
            "/api/persistent/threads",
            {
                "title": self.title(label),
                "permission_mode": "autonomous",
                "project_id": self.project,
                "datasource_ids": [self.connectors[c] for c in connectors],
                "config_override": {"workspace": {"backend": "sandbox"}},
                "model": self.args.model,
            },
        )
        thread = str(created.get("thread_id") or created["id"])
        self.threads[label] = thread
        print(f"session {label}: {thread}", flush=True)
        lane = sql(f"SELECT execution_lane FROM threads WHERE id = {lit(thread)}")
        if lane != "stateless":
            raise GateError(f"session lane is {lane!r}, not stateless")
        return thread

    def open_egress(self, session: str) -> None:
        """Let this session's workspace pod reach the API server (only it)."""
        service_ip = command(
            KUBE
            + ["-n", "default", "get", "service", "kubernetes"]
            + ["-o", "jsonpath={.spec.clusterIP}"]
        ).strip()
        endpoints = json.loads(
            command(
                KUBE + ["-n", "default", "get", "endpoints", "kubernetes", "-o", "json"]
            )
        )
        pairs = [
            (address["ip"], port["port"])
            for subset in endpoints.get("subsets") or []
            for address in subset.get("addresses") or []
            for port in subset.get("ports") or []
        ]
        if not re.fullmatch(r"[0-9.]+", service_ip) or not pairs:
            raise GateError("the API server's Service IP or endpoints are unreadable")
        name = f"srw-gate-{self.gate_id}-{session}"
        self.policies.append(name)
        d1d.apply(
            [
                d1d.egress_policy(
                    name,
                    self.gate_id,
                    {"srw/thread-id": self.threads[session]},
                    service_ip,
                    pairs,
                )
            ]
        )

    # -- phases --------------------------------------------------------------
    def preflight(self) -> None:
        problems: list[str] = []
        for served in SERVED_SETS:
            pods = self.release_pods(
                f"{swap._SELECTOR},app.kubernetes.io/component={served.component}"
            )
            if not pods:
                problems.append(f"no {served.label} pod")
            for pod in pods:
                problems += self.served_problems(pod["metadata"]["name"], served)
        self.report.check(
            "preflight: the orchestrator and the stateless agents serve this "
            "checkout's C5 modules",
            not problems,
            "; ".join(problems)[:600],
        )
        if problems:
            raise GateError("the deployment does not serve this checkout")
        applied = sql(
            "SELECT count(*) FROM schema_migrations WHERE filename IN ("
            + ", ".join(lit(name) for name in MIGRATIONS)
            + ")"
        )
        self.report.check(
            "preflight: migrations 0347 and 0430 are applied",
            applied == str(len(MIGRATIONS)),
            applied,
        )
        if applied != str(len(MIGRATIONS)):
            raise GateError("migrations missing")
        interval = self.orchestrator_env("CONNECTOR_LEASE_SWEEP_INTERVAL_SECONDS")
        try:
            self.sweep_seconds = max(5.0, float(interval or 60))
        except ValueError:
            self.sweep_seconds = 60.0
        print(f"preflight: lease sweep every {self.sweep_seconds:.0f} s", flush=True)

    def accounts(self) -> None:
        self.client_started = True
        created = self.keycloak("create-client")
        if created.get("exists"):
            self.client_started = False  # not this run's: never adopted
            raise GateError(f"a Keycloak client {self.oauth_client} already exists")
        found = created.get("found") or []
        made = created.get("id") or (found[0] if len(found) == 1 else "")
        if not _UUID_RE.fullmatch(made or "") or found != [made]:
            raise GateError(f"no Keycloak receipt for the OAuth client: {created}")
        self.client_uuid = made
        self.owner.client_id = self.oauth_client
        owner = self.owner.ok("GET", "/api/auth/me")["user"]
        self.owner_id = str(owner["id"])
        self.report.check(
            "accounts: the owner logs in with the disposable OAuth client",
            bool(self.owner_id),
        )

    def can(self, verb: str, resource: str, namespace: str, extra: list[str]) -> str:
        rc, out, _err = run(
            KUBE
            + ["auth", "can-i", verb, resource, "-n", namespace]
            + [f"--as=system:serviceaccount:{self.ids}:{MINTER_SA}"]
            + extra,
            timeout=60,
        )
        return out.strip() or ("no" if rc else "")

    def fixture(self) -> None:
        self.namespaces_started = True
        d1d.apply(identity_manifests(self.ids, self.work, self.gate_id, self.marker))

        def root_ca() -> str:
            rc, out, _err = run(
                KUBE
                + ["-n", self.ids, "get", "configmap", "kube-root-ca.crt", "-o"]
                + ["jsonpath={.data.ca\\.crt}"],
                timeout=30,
            )
            return out if rc == 0 and "BEGIN CERTIFICATE" in out else ""

        self.ca = wait_for("the namespace's root CA", root_ca, timeout=60)
        self.minting_token = secret(
            command(
                KUBE
                + ["-n", self.ids, "create", "token", MINTER_SA, "--duration=3600s"]
            ).strip()
        )
        answers = {
            "create token for agent": self.can(
                "create",
                f"serviceaccounts/{TARGET_SA}",
                self.ids,
                ["--subresource=token"],
            ),
            "create token for default": self.can(
                "create", "serviceaccounts/default", self.ids, ["--subresource=token"]
            ),
            "get secrets": self.can("get", "secrets", self.ids, []),
            "list secrets": self.can("list", "secrets", self.ids, []),
            "create pods": self.can("create", "pods", self.ids, []),
            "get configmaps (work)": self.can("get", "configmaps", self.work, []),
        }
        self.report.check(
            "fixture: the minting account may mint a token for agent only, may "
            "not read Secrets, and nothing else",
            answers["create token for agent"] == "yes"
            and all(
                answer == "no"
                for question, answer in answers.items()
                if question != "create token for agent"
            ),
            json.dumps(answers),
        )
        created = self.owner.ok(
            "POST",
            "/api/projects",
            {
                "name": self.name("project"),
                "description": "C5 provider-minted gate (disposable)",
                "user_id": self.owner_id,
            },
        )
        self.project = str(created["id"])
        minting = secret(
            d1d.kubeconfig_yaml(
                API_SERVER,
                base64.b64encode(self.ca.encode()).decode(),
                self.minting_token,
                self.work,
            )
        )
        token_request = {
            "namespace": self.ids,
            "service_account": TARGET_SA,
            "expiration_seconds": EXPIRATION_SECONDS,
        }
        # An exec user cannot be a minting credential (a static one still can
        # be delivered as before, D1d).
        import yaml

        doc = yaml.safe_load(minting)
        doc["users"][0]["user"] = {"exec": {"command": "aws", "apiVersion": "v1"}}
        status, parsed = self.create_connector(
            "refused",
            {
                "type": "kubeconfig",
                "credentials": {"files": [{"contents": yaml.safe_dump(doc)}]},
                "config": {"token_request": token_request},
            },
        )
        self.report.check(
            "fixture: a token_request connector over an exec kubeconfig is refused",
            status == 400 and "exec plugin" in json.dumps(parsed),
            f"HTTP {status}: {str(parsed)[:200]}",
        )
        status, parsed = self.create_connector(
            "kube",
            {
                "type": "kubeconfig",
                "credentials": {"files": [{"contents": minting}]},
                "config": {"token_request": token_request},
            },
        )
        if status not in (200, 201) or "kube" not in self.connectors:
            raise GateError(f"kube create answered HTTP {status}: {parsed}")
        self.link("kube", read_only=False)
        tested = self.owner.ok(
            "POST", f"/api/datasources/{self.connectors['kube']}/test"
        )
        left = self.bound_secrets()
        self.report.check(
            "fixture: Test mints a token for agent and revokes it (no Secret left)",
            tested.get("status") == "ok"
            and "revoked it" in str(tested.get("message"))
            and not left,
            f"{tested.get('status')}: {str(tested.get('message'))[:200]}; secrets {left}",
        )
        print(f"fixture: {self.ids}, {self.work}, project {self.project}", flush=True)

    def bound_secrets(self) -> list[str]:
        out = command(
            KUBE
            + ["-n", self.ids, "get", "secrets", "-l", "srw.io/minted-credential"]
            + ["-o", "jsonpath={.items[*].metadata.name}"]
        )
        return out.split()

    def mint_checks(self) -> None:
        self.create_session("kube", ["kube"])
        self.open_egress("kube")
        self.turn("kube", "Reply with the single word ready.")
        rows = self.minted("kube", "kube", tokens=True)
        live = [row for row in rows if row["status"] == "live"]
        lifetime = live[0]["expires"] - live[0]["minted"] if len(live) == 1 else -1.0
        self.report.check(
            "mint: the session's first turn left one live minted credential "
            f"living about {EXPIRATION_SECONDS} s",
            len(live) == 1
            and live[0]["provider"] == "kubernetes"
            and EXPIRATION_SECONDS - 5 <= lifetime <= EXPIRATION_SECONDS + 5,
            f"{[(r['status'], r['revoke_reason']) for r in rows]} lifetime {lifetime:.0f}",
        )
        if len(live) != 1:
            raise GateError("no live credential to check")
        self.first = live[0]
        found = self.ws_kube("kube")
        digest = hashlib.sha256(self.first["token"].encode()).hexdigest()
        problems = workspace_problems(
            found, digest=digest, ids=self.ids, marker=self.marker
        )
        self.report.check(
            "mint: the workspace's kubeconfig holds only the minted token (sub "
            "agent, bound to a srw-mint- Secret, exp 600 s, no exec); kubectl "
            "reads the marker and may do nothing else",
            not problems,
            "; ".join(problems) or json.dumps(found.get("claims")),
        )
        self.first_exp = (found.get("claims") or {}).get("exp")
        secret_names = self.bound_secrets()
        self.report.check(
            "mint: the token's bound Secret exists in the identity namespace",
            (found.get("claims") or {}).get("secret") in secret_names,
            str(secret_names),
        )
        scans = {
            "workspace": in_pod(
                self.workspace_pod(self.threads["kube"]),
                WORKSPACE_CONTAINER,
                swap._SCAN_PROGRAM,
                {
                    "secrets": [self.minting_token],
                    "roots": ["/home", "/tmp", "/root", "/var/tmp", "/run"],
                },
                python="python3",
                timeout=300,
            )
        }
        for pod in self.release_pods(
            f"{swap._SELECTOR},app.kubernetes.io/component=agent-stateless"
        ):
            name = pod["metadata"]["name"]
            scans[name] = in_pod(
                name,
                "agent",
                swap._SCAN_PROGRAM,
                {"secrets": [self.minting_token], "roots": ["/tmp", "/home"]},
                timeout=300,
            )
        found_in = {
            name: scan["found"] for name, scan in scans.items() if scan["found"]
        }
        self.report.check(
            "mint: the workspace and the stateless agent pods hold the minting "
            "token nowhere (files, environments, command lines)",
            not found_in and scans["workspace"]["scanned"]["processes"] > 0,
            json.dumps(found_in)[:400],
        )

    def renewal_checks(self) -> None:
        first = self.first
        half = first["minted"] + (first["expires"] - first["minted"]) / 2

        def past_half() -> bool:
            [row] = [r for r in self.minted("kube", "kube") if r["id"] == first["id"]]
            return row["now"] > half + 5

        wait_for(
            "the first token past half its life",
            past_half,
            timeout=EXPIRATION_SECONDS,
            interval=15,
        )
        self.turn("kube", "Reply with the single word again.")
        rows = self.minted("kube", "kube", tokens=True)
        by_id = {row["id"]: row for row in rows}
        live = [row for row in rows if row["status"] == "live"]
        old = by_id.get(first["id"]) or {}
        new = live[0] if len(live) == 1 else {}
        found = self.ws_kube("kube")
        new_digest = (
            hashlib.sha256(new["token"].encode()).hexdigest()
            if new.get("token")
            else ""
        )
        self.report.check(
            "renewal: past half its life, the next turn delivers a new token "
            "before the old one expires",
            bool(new)
            and new["id"] != first["id"]
            and old.get("status") == "superseded"
            and found.get("digest") == new_digest
            and new_digest != hashlib.sha256(first["token"].encode()).hexdigest()
            and new["minted"] < first["expires"]
            and int((found.get("claims") or {}).get("exp") or 0)
            > int(self.first_exp or 0),
            f"old {old.get('status')} new {new.get('id', '')[:8]} "
            f"minted {new.get('minted', 0) - first['expires']:.0f} s before expiry",
        )
        statuses = self.bearer([first["token"], new.get("token", "")])
        self.report.check(
            "renewal: the old token stays valid until it expires, the new one works",
            statuses == [200, 200],
            str(statuses),
        )
        self.second = new

    def end_checks(self) -> None:
        tokens = [self.first["token"], self.second.get("token", "")]
        thread = self.threads["kube"]
        self.owner.ok("DELETE", f"/api/persistent/threads/{thread}?force=true")
        budget = max(120, int(3 * self.sweep_seconds))

        def all_revoked() -> list[dict] | None:
            rows = self.minted("kube", "kube")
            return (
                rows if rows and all(r["status"] == "revoked" for r in rows) else None
            )

        try:
            rows = wait_for(
                "End revokes the minted credentials", all_revoked, timeout=budget
            )
        except GateError:
            rows = self.minted("kube", "kube")
        reasons = {row["revoke_reason"] for row in rows}
        self.report.check(
            "end: End revokes every minted credential of the session",
            bool(rows) and all(row["status"] == "revoked" for row in rows),
            str([(row["status"], row["revoke_reason"]) for row in rows]),
        )
        self.report.check(
            "end: the revoke reason is the session's end (the superseded token too)",
            "session_end" in reasons,
            str(sorted(str(reason) for reason in reasons)),
        )
        left = self.bound_secrets()
        self.report.check("end: the bound Secrets are deleted", not left, str(left))
        try:
            statuses = wait_for(
                "both tokens refused",
                lambda: (s if (s := self.bearer(tokens)) == [401, 401] else None),
                timeout=90,
                interval=5,
            )
        except GateError:
            statuses = self.bearer(tokens)
        self.report.check(
            "end: both tokens get 401 from the API server",
            statuses == [401, 401],
            str(statuses),
        )

    # -- GitHub ----------------------------------------------------------------
    def github_checks(self) -> None:
        if not self.github:
            self.report.note(
                "github: SKIPPED: no --github-app-id, --github-installation-id, "
                "--github-key-file and --github-repo were given (a GitHub App "
                "installed on a disposable repository is the operator's own)"
            )
            return
        match = _GITHUB_REPO_RE.fullmatch(self.args.github_repo)
        assert match is not None
        owner, repository = match.group(1), match.group(2)
        app = {
            "app_id": str(self.args.github_app_id),
            "installation_id": str(self.args.github_installation_id),
        }
        for label in ("gh-rw", "gh-ro"):
            status, parsed = self.create_connector(
                label,
                {
                    "type": "repository",
                    "connection_url": self.args.github_repo,
                    "credentials": {
                        "auth_method": "github_app",
                        "private_key": self.github_key,
                    },
                    "config": {"forge": "github", "github_app": app},
                },
            )
            if status not in (200, 201) or label not in self.connectors:
                raise GateError(f"{label} create answered HTTP {status}: {parsed}")
            self.link(label, read_only=label == "gh-ro")
        tested = self.owner.ok(
            "POST", f"/api/datasources/{self.connectors['gh-rw']}/test"
        )
        self.report.check(
            "github: Test mints a read token, reads the repository and revokes it",
            tested.get("status") == "ok"
            and "revoked the token" in str(tested.get("message")),
            str(tested.get("message"))[:300],
        )
        self.create_session("gh", ["gh-rw", "gh-ro"])
        self.turn(
            "gh",
            "Use the run_command tool to run `ls ~/workspace/repos` in the "
            "workspace shell, then reply with its output.",
        )
        leases = sql(
            "SELECT count(*) FROM connector_credential_leases WHERE thread_id = "
            f"{lit(self.threads['gh'])} AND driver = {lit(SWAP_DRIVER)} AND "
            "revoked_at IS NULL"
        )
        through_swap = leases == "2"
        rc, readme = self.ws("gh", "cat ~/workspace/README.md 2>/dev/null || true\n")
        self.report.note(
            "github: the repositories are served "
            + (
                "through SRW's git swap driver"
                if through_swap
                else "on the token-in-URL fallback (the README says why)"
            )
        )
        if not through_swap:
            self.report.check(
                "github: the README states the fallback and why",
                "clone URL" in readme or "fallback" in readme.lower(),
                readme[-300:],
            )
        rc, listing = self.ws(
            "gh",
            'for d in ~/workspace/repos/*/; do git -C "$d" rev-parse --is-inside-work-tree '
            ">/dev/null 2>&1 && echo cloned; done\n",
        )
        self.report.check(
            "github: both connectors' repositories are cloned",
            listing.count("cloned") >= 2,
            listing[-200:],
        )
        rw = [
            r for r in self.minted("gh", "gh-rw", tokens=True) if r["status"] == "live"
        ]
        ro = [
            r for r in self.minted("gh", "gh-ro", tokens=True) if r["status"] == "live"
        ]
        if len(rw) != 1 or len(ro) != 1:
            self.report.check(
                "github: each connector holds one live minted token",
                False,
                f"rw {len(rw)} ro {len(ro)}",
            )
            return
        rw_token, ro_token = rw[0]["token"], ro[0]["token"]
        covered: dict[str, Any] = {}
        for label, token in (("gh-rw", rw_token), ("gh-ro", ro_token)):
            status, body = github_call("GET", "/installation/repositories", token)
            covered[label] = (
                status,
                [repo.get("full_name") for repo in body.get("repositories") or []],
            )
        self.report.check(
            "github: each minted token covers the one repository",
            all(
                status == 200 and names == [f"{owner}/{repository}"]
                for status, names in covered.values()
            )
            and rw[0]["access"] == "ReadWrite"
            and ro[0]["access"] == "ReadOnly",
            json.dumps(covered),
        )
        blob = {
            "content": base64.b64encode(self.gate_id.encode()).decode(),
            "encoding": "base64",
        }
        write_ro, _ = github_call(
            "POST", f"/repos/{owner}/{repository}/git/blobs", ro_token, blob
        )
        write_rw, _ = github_call(
            "POST", f"/repos/{owner}/{repository}/git/blobs", rw_token, blob
        )
        self.report.check(
            "github: the read-only token cannot write (contents: read), the "
            "ReadWrite one can (an unreferenced blob)",
            write_ro == 403 and write_rw == 201,
            f"read-only {write_ro}, ReadWrite {write_rw}",
        )
        key_line = next(
            (
                line
                for line in self.github_key.splitlines()
                if len(line) >= 40 and "-" not in line
            ),
            "",
        )
        needles = [key_line] + ([rw_token, ro_token] if through_swap else [])
        scan = in_pod(
            self.workspace_pod(self.threads["gh"]),
            WORKSPACE_CONTAINER,
            swap._SCAN_PROGRAM,
            {
                "secrets": needles,
                "roots": ["/home", "/tmp", "/root", "/var/tmp", "/run"],
            },
            python="python3",
            timeout=300,
        )
        self.report.check(
            "github: the workspace holds the App's key nowhere"
            + (
                ", and no installation token (through the driver)"
                if through_swap
                else ""
            ),
            not scan["found"] and scan["scanned"]["processes"] > 0,
            json.dumps(scan)[:300],
        )
        self.owner.ok(
            "DELETE", f"/api/persistent/threads/{self.threads['gh']}?force=true"
        )
        budget = max(120, int(3 * self.sweep_seconds))

        def revoked() -> bool:
            rows = self.minted("gh", "gh-rw") + self.minted("gh", "gh-ro")
            return bool(rows) and all(row["status"] == "revoked" for row in rows)

        try:
            wait_for("End revokes the GitHub tokens", revoked, timeout=budget)
        except GateError:
            pass
        statuses = [
            github_call("GET", "/installation/repositories", token)[0]
            for token in (rw_token, ro_token)
        ]
        self.report.check(
            "github: End revokes both installation tokens (401)",
            statuses == [401, 401],
            str(statuses),
        )

    # -- cleanup ---------------------------------------------------------------
    def titled_threads(self) -> list[str]:
        out = sql(
            "SELECT id FROM threads WHERE "
            f"position({lit(self.gate_id)} in coalesce(title, '')) > 0"
        )
        return [row for row in out.splitlines() if _UUID_RE.fullmatch(row)]

    def cleanup(self) -> list[str]:
        problems: list[str] = []

        def step(label: str, action: Callable[[], Any]) -> None:
            try:
                if action() is False:
                    problems.append(label)
            except GateError as exc:
                problems.append(f"{label} ({exc})")

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
        for label, datasource_id in list(self.connectors.items()):

            def delete(datasource_id=datasource_id) -> bool:
                status, _body = self.owner.call(
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
        if self.connectors:
            # SRW revokes what it minted (the sweep) before the namespace,
            # whose deletion would revoke the tokens anyway, goes.
            ids = ", ".join(lit(value) for value in self.connectors.values())

            def all_revoked() -> bool:
                return (
                    sql(
                        "SELECT count(*) FROM connector_minted_credentials WHERE "
                        f"connector_id IN ({ids}) AND status <> 'revoked'"
                    )
                    == "0"
                )

            step(
                "wait for every minted credential to be revoked",
                lambda: bool(
                    wait_for(
                        "minted credentials revoked",
                        all_revoked,
                        timeout=max(120, int(3 * self.sweep_seconds)),
                        interval=5,
                    )
                ),
            )
        for name in self.policies:
            step(
                f"delete NetworkPolicy {name}",
                lambda name=name: command(
                    K + ["delete", "networkpolicy", name, "--ignore-not-found"]
                )
                is not None,
            )
        if self.namespaces_started:
            for namespace in (self.ids, self.work):
                step(
                    f"delete namespace {namespace}",
                    lambda namespace=namespace: command(
                        KUBE
                        + ["delete", "namespace", namespace, "--ignore-not-found"]
                        + ["--wait=true", "--timeout=180s"],
                        timeout=240,
                    )
                    is not None,
                )
        if self.client_started:
            step(
                "delete the OAuth client",
                lambda: self.keycloak("delete").get("refused") == [],
            )
        for problem in problems:
            print(f"cleanup: {problem} failed", flush=True)
        return problems

    def residue(self) -> list[str]:
        left: list[str] = []
        titled = self.titled_threads()
        if titled:
            left.append(f"sessions titled with the gate id: {titled}")
        prefix = self.gate_id + " %"
        count = sql(f"SELECT count(*) FROM datasources WHERE name LIKE {lit(prefix)}")
        if count != "0":
            left.append(f"{count} connectors")
        ids = [value for value in self.connectors.values() if _UUID_RE.fullmatch(value)]
        if ids:
            listed = ", ".join(lit(value) for value in ids)
            unrevoked = sql(
                "SELECT count(*) FROM connector_minted_credentials WHERE "
                f"connector_id IN ({listed}) AND status <> 'revoked'"
            )
            if unrevoked != "0":
                left.append(f"{unrevoked} minted credentials not revoked")
            leases = sql(
                "SELECT count(*) FROM connector_credential_leases WHERE "
                f"connector_id IN ({listed})"
            )
            if leases != "0":
                left.append(f"{leases} leases")
        if (
            self.project
            and sql(f"SELECT count(*) FROM projects WHERE id = {lit(self.project)}")
            != "0"
        ):
            left.append(f"project {self.project}")
        if self.namespaces_started:
            for namespace in (self.ids, self.work):
                rc, out, _err = run(
                    KUBE + ["get", "namespace", namespace, "-o", "name"], timeout=60
                )
                if rc == 0 and out:
                    left.append(f"namespace {namespace}")
        listing = command(
            K
            + [
                "get",
                "networkpolicy",
                "-l",
                f"{GATE_LABEL}={self.gate_id}",
                "-o",
                "name",
            ]
        )
        if listing:
            left.append(f"NetworkPolicies {listing.split()}")
        if self.client_started:
            try:
                counts = self.keycloak("count")
                if counts.get("clients"):
                    left.append(f"Keycloak residue {counts}")
            except GateError as exc:
                left.append(f"Keycloak residue unknown ({exc})")
        for thread in self.threads.values():
            try:
                wait_for(
                    f"pods of {thread} gone",
                    lambda thread=thread: not json.loads(
                        command(
                            K
                            + [
                                "get",
                                "pods",
                                "-l",
                                f"srw/thread-id={thread}",
                                "-o",
                                "json",
                            ]
                        )
                    )["items"],
                    timeout=180,
                    interval=10,
                )
            except GateError:
                left.append(f"pods of session {thread}")
        return left

    # -- run -------------------------------------------------------------------
    def run(self) -> int:
        try:
            self.preflight()
            self.accounts()
            self.fixture()
            self.mint_checks()
            for phase in (self.renewal_checks, self.end_checks, self.github_checks):
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
                            "project": self.project,
                            "namespaces": [self.ids, self.work],
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
    parser.add_argument("--github-app-id", help="the GitHub App's id")
    parser.add_argument("--github-installation-id", help="its installation's id")
    parser.add_argument(
        "--github-key-file",
        help="the App's private key (PEM), a file others may not read; read here, "
        "sent to SRW once as the connectors' secret",
    )
    parser.add_argument(
        "--github-repo",
        help="a DISPOSABLE repository the installation covers: "
        "https://github.com/<owner>/<repo>.git",
    )
    parser.add_argument("--keep", action="store_true", help="skip cleanup")
    return parser


GITHUB_ARGS = (
    "github_app_id",
    "github_installation_id",
    "github_key_file",
    "github_repo",
)


def validate(args: argparse.Namespace) -> None:
    if args.context != LOCAL_CONTEXT or args.namespace != LOCAL_NAMESPACE:
        raise SafetyError("this gate is restricted to k3d-srw/srw")
    if args.run and args.confirm != LOCAL_CONFIRMATION:
        raise SafetyError(f"--run requires --confirm {LOCAL_CONFIRMATION}")
    if not args.run and args.confirm is not None:
        raise SafetyError("--confirm is accepted only with --run")
    if args.gate_id is not None and not _GATE_ID_RE.fullmatch(args.gate_id):
        raise SafetyError("--gate-id must be c5- followed by 10 hex digits")
    if not swap._MODEL_RE.fullmatch(args.model):
        raise SafetyError("model id is malformed")
    if not re.fullmatch(r"[a-z][a-z0-9._-]{0,63}", args.user):
        raise SafetyError("user name is malformed")
    if not 60 <= args.turn_timeout <= 1800:
        raise SafetyError("--turn-timeout must be between 60 and 1800 seconds")
    given = [name for name in GITHUB_ARGS if getattr(args, name)]
    if given and len(given) != len(GITHUB_ARGS):
        raise SafetyError(
            "the GitHub phase needs all of --github-app-id, "
            "--github-installation-id, --github-key-file and --github-repo"
        )
    if given:
        for name in ("github_app_id", "github_installation_id"):
            if not re.fullmatch(r"[1-9][0-9]{0,19}", str(getattr(args, name))):
                raise SafetyError(f"--{name.replace('_', '-')} must be a number")
        if not _GITHUB_REPO_RE.fullmatch(args.github_repo):
            raise SafetyError(
                "--github-repo must be https://github.com/<owner>/<repo>[.git]"
            )
        if args.run:
            key_file = Path(args.github_key_file).expanduser()
            if not key_file.is_file() or "PRIVATE KEY" not in key_file.read_text():
                raise SafetyError("--github-key-file names no PEM private key")
            if key_file.stat().st_mode & 0o077:
                raise SafetyError("--github-key-file must not be readable by others")


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
        if not any(getattr(args, name) for name in GITHUB_ARGS):
            print("  (github: skipped without --github-*)")
        print(VALUES_LOCAL_KEYS)
        return 0
    started = time.monotonic()
    code = ProviderMintedGate(args).run()
    print(f"took {time.monotonic() - started:.0f} s")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
