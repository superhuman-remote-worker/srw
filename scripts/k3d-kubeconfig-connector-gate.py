#!/usr/bin/env python3
"""Local k3d gate for connector drivers D1d: credential files reach the
workspace, so a session and a job can run ``kubectl`` against a kubeconfig
connector.

Design: knowledge-base/knowledge/features/connector_drivers.md, Track D, D1d;
the issue it closes: knowledge-base/knowledge/issues/
credential_files_never_reach_the_workspace_shell.md.

The safety envelope and the helpers are scripts/k3d-connector-drivers-gate.py's
(imported, not copied): dry-run by default, the exact k3d-srw/srw context,
secrets only on stdin and scrubbed from every printed line, and a cleanup in
``finally`` that touches only what this run created.

Fixtures (all disposable, all named after the gate id ``d1d-<10 hex>``):

  namespace  ``srw-gate-<gate id>``: a ServiceAccount ``reader`` bound to a
             Role that may only get/list ConfigMaps and Pods there, a marker
             ConfigMap, and a one-hour token for the account. The kubeconfig
             names ``https://kubernetes.default.svc`` with the cluster's root
             CA and that namespace as the context's default
  egress     workspace pods may not reach the API server (the tier policies
             deny the cluster CIDRs and allow 80/443/22 only), so the gate adds
             one NetworkPolicy per unit in ``srw``, selecting only that unit's
             workspace pod (``srw/thread-id`` / ``srw/job-id``) and allowing
             only the API server's Service IP and endpoints
  project    one project owned by the test account
  connectors a kubeconfig connector (default target
             ``~/.kube/configs/<slug>.yaml``) and a generic-file connector
             (``~/.srw-files/d1d/<suffix>.json`` with an ``env_var``)

Checks (each printed PASS/FAIL; the exit status is 0 only if all pass):

  preflight  Tilt reports the srw resource ``ok``; every stateless agent pod
             and the orchestrator serve this checkout's D1d modules
  session    a stateless session (sandbox) with both connectors. In its
             workspace: ``~/.kube/config`` and the connector's own file are
             symlinks into ``~/.srw-credentials/files-*`` (mode 0700 store,
             0600 files); the generic file's link and variable resolve to its
             contents; as agent-host with the credential environment sourced,
             ``kubectl`` reads the marker, the prefixed context is current,
             ``kubectl auth can-i create configmaps`` answers no and the
             ``srw`` namespace is forbidden; README.md lists both connectors;
             no stateless agent pod holds the token. Then the agent itself is
             asked to run kubectl: a run_command result carries the marker
  job        a stateless job (sandbox) with both connectors, asked to run
             kubectl and write the marker to output/d1d.txt: the same
             workspace checks while it runs, the job settles completed, and
             an audited LLM request carries the marker (the tool result)
  live       a PINNED session (an Officer conference, sandbox) with no
             connectors, after every pooled pinned pod is checked for this
             checkout's code (the drivers gate's pool check: an idle pooled
             pod keeps its old image). A live ``config.update`` attaches
             both connectors: the ack lists them and the same workspace
             checks pass. A second one detaches both: the links, the store
             and the files are gone, KUBECONFIG and the file's variable are
             empty, and kubectl no longer reads the marker. ``--skip-live``
             skips it
  cleanup    nothing this run created is left: sessions, the job, both
             connectors, the project, the NetworkPolicies and the namespace

Run with the repository venv on the k3d-srw cluster, alone: this is a
mutating gate. It needs the workspace image built from this checkout (full
target: kubectl is installed there).

  .venv/bin/python scripts/k3d-kubeconfig-connector-gate.py           # plan
  .venv/bin/python scripts/k3d-kubeconfig-connector-gate.py \\
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
import shutil
import sys
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
_DRIVERS_GATE = ROOT / "scripts" / "k3d-connector-drivers-gate.py"


def _load_drivers_gate():
    name = "k3d_connector_drivers_gate"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, _DRIVERS_GATE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


base = _load_drivers_gate()
GateError = base.GateError
SafetyError = base.SafetyError
LOCAL_CONTEXT = base.LOCAL_CONTEXT
LOCAL_NAMESPACE = base.LOCAL_NAMESPACE
LOCAL_CONFIRMATION = base.LOCAL_CONFIRMATION
DEFAULT_MODEL = base.DEFAULT_MODEL
K = base.K
HOME = base.HOME
AGENT_CONTAINER = base.AGENT_CONTAINER
ORCHESTRATOR = base.ORCHESTRATOR
ORCHESTRATOR_CONTAINER = base.ORCHESTRATOR_CONTAINER
POD_ROOT = base.POD_ROOT
JOB_TERMINAL = base.JOB_TERMINAL
run, command, sql, audit_sql, lit, wait_for, secret = (
    base.run,
    base.command,
    base.sql,
    base.audit_sql,
    base.lit,
    base.wait_for,
    base.secret,
)

_GATE_ID_RE = re.compile(r"d1d-[0-9a-f]{10}\Z")
KUBE = ["kubectl", f"--context={LOCAL_CONTEXT}"]
API_SERVER = "https://kubernetes.default.svc"
MARKER_CONFIGMAP = "srw-gate-marker"
#: The tools that run a shell command (the shell tool category).
SHELL_TOOLS = frozenset({"run_command", "shell_execute"})
GATE_LABEL = "srw.io/gate"

#: The D1d modules a deployment must serve, by pod.
AGENT_FILES = (
    "src/agent/connectors/files.py",
    "src/agent/connectors/registry.py",
    "src/agent/connectors/legacy.py",
    "src/shared/runtime/core/credential_env.py",
    "src/shared/runtime/core/backends/remote.py",
    "src/shared/connectors/builtin.py",
    "src/shared/connectors/file_targets.py",
    "src/shared/credential_connectors.py",
)
ORCHESTRATOR_FILES = (
    "src/shared/connectors/builtin.py",
    "src/shared/connectors/file_targets.py",
    "src/shared/credential_connectors.py",
    "src/orchestrator/security/credential_files.py",
    "src/orchestrator/services/connector_drivers/credential_files.py",
)

# Reads JSON ``{"root": ..., "paths": [...]}`` on stdin; prints
# ``{path: sha256 | null}``. Nothing secret crosses it.
_HASH_PROGRAM = (
    "import hashlib, json, os, sys\n"
    "request = json.load(sys.stdin)\n"
    "found = {}\n"
    "for path in request['paths']:\n"
    "    full = os.path.join(request['root'], path)\n"
    "    try:\n"
    "        with open(full, 'rb') as handle:\n"
    "            found[path] = hashlib.sha256(handle.read()).hexdigest()\n"
    "    except OSError:\n"
    "        found[path] = None\n"
    "print(json.dumps(found))\n"
)

PLAN = [
    "preflight: Tilt srw ok; the stateless agents and the orchestrator serve "
    "this checkout's D1d modules",
    "fixture: scratch namespace srw-gate-<gate id> with a read-only "
    "ServiceAccount (get/list ConfigMaps and Pods), a marker ConfigMap and a "
    "one-hour token; a project; a kubeconfig and a generic-file connector",
    "egress: one NetworkPolicy per unit (session, job, live session) letting only that "
    "unit's workspace pod reach the API server",
    "session: stateless sandbox session; workspace links into "
    "~/.srw-credentials, modes, kubectl as agent-host reads the marker, "
    "can-i create answers no, the srw namespace is forbidden, README lists "
    "both, no agent pod holds the token; the agent's own run_command reads "
    "the marker",
    "job: stateless sandbox job asked to run kubectl; the same workspace "
    "checks while it runs; settles completed; an audited request carries "
    "the marker",
    "live: pooled pinned pods checked for this checkout's code first; a pinned "
    "Officer-conference session; a live config.update attaches both "
    "connectors (same workspace checks), a second detaches both (links, "
    "store and variables gone) (--skip-live skips it)",
    "cleanup: sessions, job, connectors, project, NetworkPolicies, namespace",
]


def kubeconfig_yaml(server: str, ca_data: str, token: str, namespace: str) -> str:
    """A kubeconfig for one ServiceAccount token, defaulting to ``namespace``."""
    import yaml

    return yaml.safe_dump(
        {
            "apiVersion": "v1",
            "kind": "Config",
            "clusters": [
                {
                    "name": "scratch",
                    "cluster": {
                        "server": server,
                        "certificate-authority-data": ca_data,
                    },
                }
            ],
            "users": [{"name": "reader", "user": {"token": token}}],
            "contexts": [
                {
                    "name": "scratch",
                    "context": {
                        "cluster": "scratch",
                        "user": "reader",
                        "namespace": namespace,
                    },
                }
            ],
            "current-context": "scratch",
        },
        sort_keys=False,
    )


def scratch_manifests(namespace: str, gate_id: str, marker: str) -> list[dict]:
    """The namespace and its read-only account, Role, binding and marker."""
    labels = {GATE_LABEL: gate_id}
    meta = {"namespace": namespace, "labels": labels}
    return [
        {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": namespace, "labels": labels},
        },
        {
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {"name": "reader", **meta},
        },
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "Role",
            "metadata": {"name": "reader", **meta},
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
            "metadata": {"name": "reader", **meta},
            "roleRef": {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "Role",
                "name": "reader",
            },
            "subjects": [
                {"kind": "ServiceAccount", "name": "reader", "namespace": namespace}
            ],
        },
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": MARKER_CONFIGMAP, **meta},
            "data": {"marker": marker},
        },
    ]


def egress_policy(
    name: str,
    gate_id: str,
    selector: dict[str, str],
    service_ip: str,
    endpoints: list[tuple[str, int]],
) -> dict:
    """Egress from one unit's workspace pod to the API server, and nothing else.

    NetworkPolicies add up: the tier policies still apply, this only adds
    the API server's Service IP (443) and its endpoints (their port).
    """
    egress = [
        {
            "to": [{"ipBlock": {"cidr": f"{service_ip}/32"}}],
            "ports": [{"protocol": "TCP", "port": 443}],
        }
    ]
    for address, port in endpoints:
        egress.append(
            {
                "to": [{"ipBlock": {"cidr": f"{address}/32"}}],
                "ports": [{"protocol": "TCP", "port": port}],
            }
        )
    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {
            "name": name,
            "namespace": LOCAL_NAMESPACE,
            "labels": {GATE_LABEL: gate_id},
        },
        "spec": {
            "podSelector": {
                "matchLabels": {"srw.io/component": "agent-workspace", **selector}
            },
            "policyTypes": ["Egress"],
            "egress": egress,
        },
    }


def apply(manifests: list[dict]) -> None:
    command(
        KUBE + ["apply", "-f", "-"],
        data=json.dumps({"apiVersion": "v1", "kind": "List", "items": manifests}),
    )


#: A pinned agent pod serves the drivers gate's set plus the D1d workspace
#: programs and their transport.
PINNED_SERVED = base.ServedSet(
    base.PINNED_AGENT.label,
    base.PINNED_AGENT.component,
    base.PINNED_AGENT.container,
    base.PINNED_AGENT.dirs,
    (
        *base.PINNED_AGENT.files,
        "src/shared/runtime/core/credential_env.py",
        "src/shared/runtime/core/backends/remote.py",
    ),
    base.PINNED_AGENT.contains,
)


class KubeconfigConnectorGate(base.ConnectorDriversGate):
    pinned_served = PINNED_SERVED

    def __init__(self, args: argparse.Namespace) -> None:
        args.gate_id = args.gate_id or f"d1d-{secrets.token_hex(5)}"
        super().__init__(args)
        self.namespace = f"srw-gate-{self.gate_id}"
        self.namespace_started = False
        self.policies: list[str] = []
        self.kube_marker = f"d1d-marker-{self.suffix}"
        self.file_marker = secret(f"d1d-file-{secrets.token_hex(8)}")
        #: Printed lines are scrubbed of the marker: the workspace reports
        #: its digest.
        self.file_digest = hashlib.sha256(self.file_marker.encode()).hexdigest()
        self.file_var = f"D1D_FILE_{self.suffix.upper()}"
        self.token = ""

    # -- naming ------------------------------------------------------------
    @property
    def kube_slug(self) -> str:
        return re.sub(r"[^a-z0-9]+", "-", self.name("kubeconfig").lower()).strip("-")

    @property
    def kubectl_command(self) -> str:
        return (
            f"kubectl get configmap {MARKER_CONFIGMAP} -o jsonpath='{{.data.marker}}'"
        )

    # -- phases ------------------------------------------------------------
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
        problems: list[str] = []
        targets = [
            (pod["metadata"]["name"], AGENT_CONTAINER, AGENT_FILES)
            for pod in self.pods("agent-stateless")
        ] + [
            (pod["metadata"]["name"], ORCHESTRATOR_CONTAINER, ORCHESTRATOR_FILES)
            for pod in self.pods("orchestrator")
        ]
        if not targets:
            problems.append("no stateless agent or orchestrator pod")
        for pod, container, files in targets:
            found = json.loads(
                command(
                    K
                    + ["exec", "-i", pod, "-c", container, "--"]
                    + ["python", "-c", _HASH_PROGRAM],
                    data=json.dumps({"root": POD_ROOT, "paths": list(files)}),
                ).splitlines()[-1]
            )
            stale = [
                path
                for path in files
                if found.get(path)
                != hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
            ]
            if stale:
                problems.append(f"{pod} serves other bytes for {stale}")
        self.report.check(
            "preflight: the stateless agents and the orchestrator serve this "
            "checkout's D1d modules",
            not problems,
            "; ".join(problems)[:600],
        )
        if problems:
            raise GateError("the deployment does not serve this checkout")
        self.user_id = sql(
            f"SELECT id FROM users WHERE preferred_username = {lit(self.args.user)}"
        )
        if not re.fullmatch(r"[0-9a-f-]{36}", self.user_id):
            raise GateError(f"no user {self.args.user!r}")

    def fixture(self) -> None:
        self.namespace_started = True
        apply(scratch_manifests(self.namespace, self.gate_id, self.kube_marker))
        in_ns = KUBE + ["-n", self.namespace]

        def root_ca() -> str:
            # Published into every namespace shortly after it is created.
            rc, out, _err = run(
                in_ns
                + ["get", "configmap", "kube-root-ca.crt", "-o"]
                + ["jsonpath={.data.ca\\.crt}"],
                timeout=30,
            )
            return out if rc == 0 and "BEGIN CERTIFICATE" in out else ""

        ca = wait_for("the namespace's root CA", root_ca, timeout=60)
        self.token = secret(
            command(in_ns + ["create", "token", "reader", "--duration=3600s"]).strip()
        )
        kubeconfig = secret(
            kubeconfig_yaml(
                API_SERVER,
                base64.b64encode(ca.encode()).decode(),
                self.token,
                self.namespace,
            )
        )
        created = self.api.ok(
            "POST",
            "/api/projects",
            {
                "name": self.name("project"),
                "description": "D1d kubeconfig connector gate (disposable)",
                "user_id": self.user_id,
            },
        )
        self.project = str(created["id"])
        bodies = {
            "kubeconfig": {
                "type": "kubeconfig",
                "credentials": {"files": [{"contents": kubeconfig}]},
            },
            "file": {
                "type": "generic_file",
                "credentials": {
                    "files": [
                        {
                            "contents": self.file_marker,
                            "target_path": f"~/.srw-files/d1d/{self.suffix}.json",
                            "env_var": self.file_var,
                        }
                    ]
                },
            },
        }
        for label, body in bodies.items():
            status, answer = self.create_connector(label, body)
            if status not in (200, 201) or label not in self.connectors:
                raise GateError(f"{label} create answered HTTP {status}: {answer}")
        print(
            f"fixture: namespace {self.namespace}, project {self.project}, "
            f"connectors {sorted(self.connectors)}",
            flush=True,
        )

    def open_egress(self, unit: str, selector: dict[str, str]) -> None:
        """Let this unit's workspace pod reach the API server (only it)."""
        service_ip = command(
            KUBE
            + ["-n", "default", "get", "service", "kubernetes"]
            + ["-o", "jsonpath={.spec.clusterIP}"]
        ).strip()
        endpoints = json.loads(
            command(
                KUBE
                + ["-n", "default", "get", "endpoints", "kubernetes"]
                + ["-o", "json"]
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
        name = f"srw-gate-{self.gate_id}-{unit}"
        self.policies.append(name)
        apply([egress_policy(name, self.gate_id, selector, service_ip, pairs)])

    def workspace_checks(self, label: str, pod: str) -> None:
        """What the shell sees, as agent-host with the credential env sourced."""
        # ``ws`` runs under ``set -u``: every variable read has a default.
        _rc, out = self.ws(
            pod,
            'for env in ~/.srw-credentials/*.sh; do . "$env"; done\n'
            "store=$(readlink ~/.kube/config)\n"
            'echo "link=$store"\n'
            f'echo "own=$(readlink ~/.kube/configs/{self.kube_slug}.yaml)"\n'
            'echo "storemode=$(stat -c %a "$(dirname "$store")")"\n'
            'echo "filemode=$(stat -c %a "$store")"\n'
            'echo "kubeconfig=${KUBECONFIG-}"\n'
            f'echo "file=$(sha256sum < ~/.srw-files/d1d/{self.suffix}.json '
            '| cut -c1-64)"\n'
            f'echo "varpath=${{{self.file_var}-}}"\n'
            f'echo "var=$(sha256sum < "${{{self.file_var}:-/dev/null}}" '
            '| cut -c1-64)"\n'
            'echo "context=$(kubectl config current-context 2>&1)"\n'
            f'echo "marker=$({self.kubectl_command} 2>&1)"\n'
            'echo "cancreate=$(kubectl auth can-i create configmaps 2>&1)"\n'
            'echo "srw=$(kubectl get pods -n srw 2>&1 | head -c 300)"\n'
            "echo \"readme=$(grep -c 'kubeconfig\\|(file)' ~/workspace/README.md)\"\n"
            "exit 0\n",
            check=False,
        )
        facts = dict(line.split("=", 1) for line in out.splitlines() if "=" in line)
        store = facts.get("link", "")
        self.report.check(
            f"{label} workspace: ~/.kube/config links into the private store",
            store.startswith(f"{HOME}/.srw-credentials/files-")
            and facts.get("own", "").startswith(f"{HOME}/.srw-credentials/files-"),
            f"link {store.replace(HOME, '~')}",
        )
        self.report.check(
            f"{label} workspace: the store is 0700 and the kubeconfig 0600",
            facts.get("storemode") == "700" and facts.get("filemode") == "600",
            f"store {facts.get('storemode')}, file {facts.get('filemode')}",
        )
        self.report.check(
            f"{label} workspace: KUBECONFIG names the merged config",
            facts.get("kubeconfig") == store,
            f"KUBECONFIG {facts.get('kubeconfig', '').replace(HOME, '~')}",
        )
        self.report.check(
            f"{label} workspace: the generic file and its variable hold its contents",
            facts.get("file") == self.file_digest
            and facts.get("var") == self.file_digest,
            f"file {'matches' if facts.get('file') == self.file_digest else 'differs'}, "
            f"{self.file_var} -> {facts.get('varpath', '').replace(HOME, '~')!r} "
            f"{'matches' if facts.get('var') == self.file_digest else 'differs'}",
        )
        self.report.check(
            f"{label} workspace: the connector's context is current",
            facts.get("context") == f"{self.kube_slug}-scratch",
            f"context {facts.get('context')}",
        )
        self.report.check(
            f"{label} workspace: kubectl as agent-host reads the marker",
            facts.get("marker") == self.kube_marker,
            f"kubectl said {facts.get('marker', '')[:200]!r}",
        )
        self.report.check(
            f"{label} workspace: the account is read-only and namespaced",
            facts.get("cancreate", "").startswith("no")
            and "forbidden" in facts.get("srw", "").lower(),
            f"can-i create: {facts.get('cancreate')}; srw: "
            f"{facts.get('srw', '')[:120]}",
        )
        self.report.check(
            f"{label} workspace: README.md lists both connectors",
            facts.get("readme", "0").isdigit() and int(facts.get("readme", "0")) >= 2,
            f"{facts.get('readme')} matching lines",
        )

    def agent_pods_hold_no_token(self, label: str) -> None:
        holders: list[str] = []
        for pod in self.pods("agent-stateless"):
            name = pod["metadata"]["name"]
            rc, out, _err = run(
                K
                + ["exec", "-i", name, "-c", AGENT_CONTAINER, "--"]
                + ["grep", "-rlsF", "-f", "-", "/home/srw", "/tmp"],
                data=self.token + "\n",
                timeout=120,
            )
            if rc == 0 and out.strip():
                holders.append(f"{name}: {out.splitlines()[:3]}")
        self.report.check(
            f"{label}: no stateless agent pod holds the kubeconfig's token",
            not holders,
            "; ".join(holders),
        )

    def session(self) -> None:
        body: dict[str, Any] = {
            "title": f"D1d kubeconfig connector gate {self.gate_id}",
            "permission_mode": "autonomous",
            "project_id": self.project,
            "datasource_ids": [self.connectors["kubeconfig"], self.connectors["file"]],
            "config_override": {"workspace": {"backend": "sandbox"}},
            "model": self.args.model,
        }
        created = self.api.ok("POST", "/api/persistent/threads", body)
        self.thread = str(created.get("thread_id") or created["id"])
        print(f"session {self.thread}", flush=True)
        self.open_egress("session", {"srw/thread-id": self.thread})
        self.turn(
            "Use the run_command tool to run exactly this command in the "
            f"workspace shell, then reply with its output: {self.kubectl_command}",
            1,
        )
        pod = self.workspace_pod(f"app=srw-workspace,srw/thread-id={self.thread}")
        self.workspace_checks("session", pod)
        self.agent_pods_hold_no_token("session")
        shell = self.session_shell_tools()
        called, returned = self.tool_use("run_command", self.kube_marker)
        tools = self.session_tool_calls()
        self.report.note(
            "session agent: shell tools bound: "
            f"{shell or 'none (the default session expert binds no shell)'}; "
            f"{called} run_command calls, {returned} tool results with the "
            f"marker; tools called: {sorted(tools) or 'none'} (not gated: the "
            "workspace check above runs kubectl as agent-host with the same "
            "environment)"
        )
        self.report.check(
            "session: ended and deleted", self.end_session(), "thread row"
        )

    def session_shell_tools(self) -> list[str]:
        """The shell tools the live session holds (its tool-groups report)."""
        status, body = self.api.call(
            "GET", f"/api/persistent/threads/{self.thread}/tool-groups"
        )
        if status != 200 or not isinstance(body, dict):
            return []
        categories = body.get("categories") or {}
        shell = categories.get("shell") if isinstance(categories, dict) else None
        return sorted(str(name) for name in shell or [])

    def session_tool_calls(self) -> set[str]:
        """The tool names the session's agent called (from its messages)."""
        rows = sql(
            "SELECT coalesce(string_agg(tool_calls::text, ' '), '') FROM "
            f"thread_messages WHERE thread_id = {lit(self.thread)} AND role IN "
            "('ai', 'assistant')"
        )
        return set(re.findall(r'"name": ?"([A-Za-z0-9_.:-]+)"', rows))

    def run_job(self) -> None:
        created = self.api.ok(
            "POST",
            "/api/jobs",
            {
                "description": (
                    "D1d kubeconfig gate. Use the run_command tool to run exactly "
                    f"{self.kubectl_command} in the workspace shell, write its "
                    "output to output/d1d.txt, then complete the job."
                ),
                "project_id": self.project,
                "datasource_ids": [
                    self.connectors["kubeconfig"],
                    self.connectors["file"],
                ],
                "execution_lane": "stateless",
                "config_override": {
                    "workspace": {"backend": "sandbox"},
                    "llm": {"model": self.args.model},
                },
            },
        )
        self.job = str(created.get("job_id") or created["id"])
        print(f"job {self.job}", flush=True)
        self.open_egress("job", {"srw/job-id": self.job})

        def running_pod() -> str | None:
            status = self.job_status()
            if status in JOB_TERMINAL:
                raise GateError(f"job {status} before its workspace was seen")
            pods = json.loads(
                command(
                    K
                    + ["get", "pods", "-l", f"app=srw-workspace,srw/job-id={self.job}"]
                    + ["-o", "json"]
                )
            )["items"]
            for pod in pods:
                if pod.get("status", {}).get("phase") != "Running":
                    continue
                name = pod["metadata"]["name"]
                rc, _out = self.ws(name, "test -L ~/.kube/config\n", check=False)
                if rc == 0:
                    return name
            return None

        try:
            pod = wait_for(
                "job credential files materialized",
                running_pod,
                timeout=self.args.turn_timeout,
                interval=5,
            )
        except GateError as exc:
            self.report.check(
                "job workspace: the credential files arrive", False, str(exc)
            )
        else:
            self.workspace_checks("job", pod)
            self.agent_pods_hold_no_token("job")
        self.job_settle()
        self.job_agent_checks_after_settle()

    def job_agent_checks_after_settle(self) -> None:
        """run_command offered is the gate; the model using it is a NOTE."""
        offered = self.wait_audited_tools(self.job)
        shell = sorted(name for name in offered if name in SHELL_TOOLS)
        used = audit_sql(
            f"SELECT count(*) FROM llm_requests WHERE job_id::text = {lit(self.job)} "
            f"AND position({lit(self.kube_marker)} in request::text) > 0"
        )
        self.report.note(
            "job agent: shell tools offered: "
            f"{shell or 'none (the default worker expert binds no shell)'}; "
            f"{used} audited requests carry the marker kubectl read (not gated: "
            "the workspace check runs kubectl as agent-host)"
        )

    # -- live attach and detach on a pinned session -----------------------
    def live_phase(self) -> None:
        """Attach and detach both connectors live (``live_attach`` is true).

        The pool check, the pinned session, its assigned pod's byte check and
        the live ``config.update`` are the drivers gate's own.
        """
        self.check_pinned_pool()
        self.live_session()
        self.open_egress("live", {"srw/thread-id": self.live_thread})
        labels = ("kubeconfig", "file")
        names = sorted(self.name(label) for label in labels)
        attached = self.live_update(
            [self.connectors[label] for label in labels], "attach"
        )
        added = sorted((attached.get("datasources") or {}).get("added") or [])
        self.report.check(
            "live attach: config.changed lists both connectors added",
            attached.get("outcome") == "config.changed" and added == names,
            f"outcome {attached.get('outcome')}, added {added}",
        )
        pod = self.live_workspace()
        self.workspace_checks("live attach", pod)
        detached = self.live_update([], "detach")
        removed = sorted((detached.get("datasources") or {}).get("removed") or [])
        self.report.check(
            "live detach: config.changed lists both connectors removed",
            detached.get("outcome") == "config.changed" and removed == names,
            f"outcome {detached.get('outcome')}, removed {removed}",
        )
        self.live_detached_checks(pod)
        self.report.check(
            "live: session ended and deleted",
            self.delete_thread(self.live_thread),
            "thread row",
        )

    def live_detached_checks(self, pod: str) -> None:
        """After a live detach: no link, no store file, no variable."""
        _rc, out = self.ws(
            pod,
            'for env in ~/.srw-credentials/*.sh; do . "$env"; done\n'
            'test -L ~/.kube/config && echo "kubelink=yes" || echo "kubelink=no"\n'
            f"test -e ~/.srw-files/d1d/{self.suffix}.json "
            '&& echo "filelink=yes" || echo "filelink=no"\n'
            "ls ~/.srw-credentials/files-*/ >/dev/null 2>&1 "
            '&& echo "store=yes" || echo "store=no"\n'
            # Unset, never exported empty (an empty value masks a default).
            'echo "kubeconfig=${KUBECONFIG-<unset>}"\n'
            f'echo "var=${{{self.file_var}-<unset>}}"\n'
            f'echo "marker=$({self.kubectl_command} 2>&1 | head -c 200)"\n'
            "exit 0\n",
            check=False,
        )
        facts = dict(line.split("=", 1) for line in out.splitlines() if "=" in line)
        self.report.check(
            "live detach: the links and the store are gone",
            facts.get("kubelink") == "no"
            and facts.get("filelink") == "no"
            and facts.get("store") == "no",
            f"~/.kube/config link {facts.get('kubelink')}, file link "
            f"{facts.get('filelink')}, store {facts.get('store')}",
        )
        self.report.check(
            "live detach: KUBECONFIG and the file's variable are unset",
            facts.get("kubeconfig") == "<unset>" and facts.get("var") == "<unset>",
            f"KUBECONFIG {facts.get('kubeconfig', '').replace(HOME, '~')!r}, "
            f"{self.file_var} {facts.get('var', '').replace(HOME, '~')!r}",
        )
        self.report.check(
            "live detach: kubectl no longer reads the marker",
            facts.get("marker") != self.kube_marker,
            f"kubectl said {facts.get('marker', '')[:120]!r}",
        )

    # -- cleanup -----------------------------------------------------------
    def cleanup(self) -> list[str]:
        problems: list[str] = []

        def step(label: str, action: Callable[[], Any]) -> None:
            try:
                if action() is False:
                    problems.append(label)
            except GateError as exc:
                problems.append(f"{label} ({exc})")

        if self.thread:
            step("delete session", self.end_session)
        if self.live_thread:
            step("delete live session", lambda: self.delete_thread(self.live_thread))
        for leftover in self.titled_threads():
            if leftover not in (self.thread, self.live_thread):
                step(
                    f"delete leftover session {leftover}",
                    lambda leftover=leftover: self.delete_thread(leftover),
                )
        if self.job:
            step(
                "cancel job",
                lambda: self.api.call("PUT", f"/api/jobs/{self.job}/cancel") and None,
            )

            def deleted() -> bool:
                status, _body = self.api.call("DELETE", f"/api/jobs/{self.job}")
                return status in (200, 204, 404)

            step(
                "delete job",
                lambda: bool(
                    wait_for("job deleted", deleted, timeout=240, interval=10)
                ),
            )
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
        for name in self.policies:
            step(
                f"delete NetworkPolicy {name}",
                lambda name=name: command(
                    K + ["delete", "networkpolicy", name, "--ignore-not-found"]
                ),
            )
        if self.namespace_started:
            step(
                f"delete namespace {self.namespace}",
                lambda: command(
                    KUBE
                    + ["delete", "namespace", self.namespace, "--ignore-not-found"]
                    + ["--wait=true", "--timeout=180s"],
                    timeout=240,
                ),
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
        for table, value in (
            ("jobs", self.job),
            ("threads", self.thread),
            ("threads", self.live_thread),
            ("projects", self.project),
        ):
            if (
                value
                and sql(f"SELECT count(*) FROM {table} WHERE id = {lit(value)}") != "0"
            ):
                left.append(f"{table} row {value}")
        policies = command(
            K
            + ["get", "networkpolicy", "-l", f"{GATE_LABEL}={self.gate_id}"]
            + ["-o", "name"]
        )
        if policies.strip():
            left.append(f"NetworkPolicies {policies.split()}")
        rc, _out, _err = run(KUBE + ["get", "namespace", self.namespace], timeout=30)
        if rc == 0:
            left.append(f"namespace {self.namespace}")
        for selector in filter(
            None,
            [
                f"srw/job-id={self.job}" if self.job else "",
                f"srw/thread-id={self.thread}" if self.thread else "",
                f"srw/thread-id={self.live_thread}" if self.live_thread else "",
            ],
        ):
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

    # -- run ---------------------------------------------------------------
    def run(self) -> int:
        try:
            self.preflight()
            self.fixture()
            try:
                self.session()
            except GateError as exc:
                # The job still runs.
                self.report.check("session: infrastructure", False, str(exc))
            self.run_job()
            if not self.args.skip_live:
                try:
                    self.live_phase()
                except GateError as exc:
                    self.report.check("live: infrastructure", False, str(exc))
        except GateError as exc:
            self.report.check("gate infrastructure", False, str(exc))
        finally:
            if self.args.keep:
                print(
                    "kept: "
                    + json.dumps(
                        {
                            "namespace": self.namespace,
                            "policies": self.policies,
                            "connectors": self.connectors,
                            "project": self.project,
                            "job": self.job,
                            "thread": self.thread,
                            "live_thread": self.live_thread,
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
        "--skip-live",
        action="store_true",
        help="skip the live attach/detach on a pinned session",
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
        raise SafetyError("--gate-id must be d1d- followed by 10 hex digits")
    if not base._MODEL_RE.fullmatch(args.model):
        raise SafetyError("model id is malformed")
    if not re.fullmatch(r"[a-z][a-z0-9._-]{0,63}", args.user):
        raise SafetyError("user name is malformed")
    if not 60 <= args.turn_timeout <= 1800:
        raise SafetyError("--turn-timeout must be between 60 and 1800 seconds")
    if not 60 <= args.job_timeout <= 3600:
        raise SafetyError("--job-timeout must be between 60 and 3600 seconds")


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
    return KubeconfigConnectorGate(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
