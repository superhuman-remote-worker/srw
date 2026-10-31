#!/usr/bin/env python3
"""Run production manifest services against disposable PostgreSQL and Cilium.

Accepts only the temporary kubeconfig/context created by the Cilium wrapper.
The PostgreSQL container, Account and Kubernetes namespace belong to this test.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import subprocess
import time
from uuid import UUID, uuid4

from fastapi import HTTPException
from kubernetes import client
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.services.generic_harness_runtime import (
    GenericAttemptIdentity,
    GenericHarnessRuntime,
    build_generic_launch,
)
from orchestrator.services.manifest_execution import ManifestExecutionService
from orchestrator.services.manifest_execution_snapshot import read_execution
from orchestrator.services.manifest_resources import ManifestResourceService
from orchestrator.services.manifest_workspace_runtime import ManifestWorkspaceRuntime
from orchestrator.services.manifest_workspaces import ManifestWorkspaceService

ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "srw_native_gate_support", ROOT / "scripts/manifests-native-k3d-gate.py"
)
native = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(native)
GateFailure = native.GateFailure
require, announce = native.require, native.announce


def verify_disposable_profile(context, kubeconfig):
    require(
        re.fullmatch(r"k3d-srw-native-gate-[0-9a-f]{8}", context) and kubeconfig,
        "Service integration requires an explicit disposable native-gate context and kubeconfig.",
    )
    api, nodes = native.verify_cluster(context=context, kubeconfig=kubeconfig)
    try:
        cilium = client.CoreV1Api(api).read_namespaced_config_map(
            "cilium-config", "kube-system", _request_timeout=15
        )
        require(
            cilium.data.get("enable-policy") == "always",
            "The disposable cluster does not always enforce Cilium policy.",
        )
    except BaseException:
        api.close()
        raise
    return api, nodes


def job_document(
    name, image, *, command=None, workspace=None, attempts=1, private=None
):
    runtime = {
        "image": image,
        "pullPolicy": "IfNotPresent",
        "resources": {
            "requests": {"cpu": 0.05, "memory": "64Mi"},
            "limits": {"cpu": 0.5, "memory": "256Mi"},
        },
    }
    if command is not None:
        runtime["command"] = command
    if private is not None:
        runtime["config"] = private
    return {
        "apiVersion": "srw/v1alpha1",
        "kind": "Job",
        "metadata": {"name": name},
        "spec": {
            "task": {
                "text": "Run the service integration gate",
                "data": {"ticket": 17},
            },
            "execution": {
                "expert": {"inline": {"runtime": runtime}},
                "workspace": workspace,
            },
            "completion": {"mode": "ProcessExit"},
            "retry": {"maxAttempts": attempts},
            "timeoutSeconds": 300,
        },
    }


def ssh_command(remote):
    # This optional SSH helper is a workspace binding, not a lifecycle hook.
    return [
        "/bin/sh",
        "-ec",
        """test ! -f /var/run/secrets/kubernetes.io/serviceaccount/token
test -z "${SRW_INTERNAL_API_KEY+x}"
test -z "${DATABASE_URL+x}"
test ! -d /home/agent-host
ready=0
for n in 1 2 3 4 5 6 7 8 9 10; do
  if timeout 4 /run/srw-workspace/ssh true >/dev/null 2>&1; then ready=1; break; fi
  sleep 1
done
test "$ready" = 1
exec timeout 30 /run/srw-workspace/ssh """
        + shlex.quote(remote),
    ]


class ServiceGate:
    def __init__(self, api, db, images, actor):
        self.api, self.db, self.images, self.actor = api, db, images, actor
        self.core, self.network = client.CoreV1Api(api), client.NetworkingV1Api(api)
        self.namespace = "srw-native-service-" + uuid4().hex[:12]
        self.namespace_uid = None
        self.attempt_evidence = {}
        self.provider_fixtures = {}
        self.provider_evidence = {}
        self.checks, self.executions, self.observed, self.workspace_history = (
            [],
            {},
            {},
            {},
        )
        self.configure_services()

    def configure_services(self):
        self.workspace = ManifestWorkspaceService(
            self.db,
            ManifestWorkspaceRuntime(self.core, self.network, namespace=self.namespace),
            namespace=self.namespace,
            default_image=self.images["workspace"],
            storage_class_name="local-path",
        )
        self.execution = ManifestExecutionService(
            self.db,
            runtime=GenericHarnessRuntime(
                self.core, self.network, namespace=self.namespace
            ),
            namespace=self.namespace,
            workspace=self.workspace,
            native_hosting_enabled=True,
            harness_egress=os.environ.get("MANIFEST_HARNESS_EGRESS", "[]"),
            approved_images=[
                {"name": k, "image": v}
                for k, v in self.images.items()
                if "@sha256:" in v
            ],
        )
        self.resources = ManifestResourceService(
            self.db, admit_job=self.execution.admit
        )

    def install_harness_egress(self, rules):
        # Match the Helm ConfigMap/main factory boundary in this child process.
        # The existing SRW installation's configuration is never read or changed.
        os.environ["MANIFEST_HARNESS_EGRESS"] = json.dumps(rules)
        self.configure_services()

    async def provider_fixture(self):
        identity = GenericAttemptIdentity(str(uuid4()), 1)
        code = """import json
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
class Provider(BaseHTTPRequestHandler):
 def log_message(self,*args): pass
 def do_POST(self):
  assert self.path == '/v1/chat/completions'
  assert self.headers.get('Authorization') == 'Bearer fixture-key'
  request=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
  assert request['model'] == 'fixture-model'
  body=json.dumps({'choices':[{'message':{'role':'assistant','content':'operator-egress-ok'}}]}).encode()
  self.send_response(200); self.send_header('Content-Type','application/json'); self.send_header('Content-Length',str(len(body))); self.end_headers(); self.wfile.write(body)
ThreadingHTTPServer(('0.0.0.0',8443),Provider).serve_forever()
"""
        spec = job_document(
            "provider-fixture", self.images["python"], command=["python", "-c", code]
        )["spec"]
        spec.pop("timeoutSeconds")
        spec["execution"]["expert"]["inline"]["runtime"]["probes"] = {
            "readiness": {
                "exec": {
                    "command": [
                        "python",
                        "-c",
                        "import socket; socket.create_connection(('127.0.0.1',8443),1).close()",
                    ]
                },
                "periodSeconds": 1,
                "timeoutSeconds": 2,
            }
        }
        runtime = GenericHarnessRuntime(
            self.core, self.network, namespace=self.namespace
        )
        plan = build_generic_launch(identity, spec, namespace=self.namespace)
        plan.network_policy["spec"]["ingress"] = [
            {
                "from": [
                    {
                        "namespaceSelector": {
                            "matchLabels": {
                                "kubernetes.io/metadata.name": self.namespace
                            }
                        },
                        "podSelector": {},
                    }
                ],
                "ports": [{"protocol": "TCP", "port": 8443}],
            }
        ]
        observed = await runtime.launch(plan)
        require(observed.pod_uid, "The owned provider fixture has no Pod identity.")
        self.provider_fixtures[identity] = observed.pod_uid
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            observed = await runtime.observe(
                identity, expected_pod_uid=observed.pod_uid
            )
            if observed.readiness and observed.pod_ip:
                return identity, observed
            await asyncio.sleep(1)
        raise GateFailure("The owned provider fixture did not become ready.")

    def provider_rule(self, identity):
        return {
            "to": [
                {
                    "namespaceSelector": {
                        "matchLabels": {"kubernetes.io/metadata.name": self.namespace}
                    },
                    "podSelector": {"matchLabels": identity.labels},
                }
            ],
            "ports": [{"protocol": "TCP", "port": 8443}],
        }

    async def provider_job(self, name, endpoints, *, forbidden=()):
        code = f"""import errno,json,os,socket,urllib.request
for host,port in {list(forbidden)!r}:
 try:
  connection=socket.create_connection((host,port),timeout=2)
 except TimeoutError: pass
 except OSError as error:
  assert error.errno in (errno.ECONNREFUSED,errno.ENETUNREACH,errno.EHOSTUNREACH,errno.EACCES,errno.EPERM)
 else:
  connection.close(); raise SystemExit(31)
client=urllib.request.build_opener(urllib.request.ProxyHandler({{}}))
for endpoint in {list(endpoints)!r}:
 request=urllib.request.Request(endpoint+'/v1/chat/completions',data=json.dumps({{'model':'fixture-model','messages':[{{'role':'user','content':'reply'}}]}}).encode(),headers={{'Content-Type':'application/json','Authorization':'Bearer '+os.environ['PROVIDER_API_KEY']}})
 with client.open(request,timeout=5) as response:
  assert json.load(response)['choices'][0]['message']['content'] == 'operator-egress-ok'
"""
        document = job_document(
            name, self.images["python"], command=["python", "-c", code]
        )
        document["spec"]["execution"]["connectors"] = {
            "provider": {
                "inline": {
                    "driver": "srw.env/v1",
                    "config": {"env": {"PROVIDER_API_KEY": "fixture-key"}},
                }
            }
        }
        work_id, _, _ = await self.apply(document)
        await self.finish(work_id)
        return work_id

    async def provider_connectivity(self):
        allowed, target = await self.provider_fixture()
        forbidden, other = await self.provider_fixture()
        api = await self.call(
            self.core.read_namespaced_service, name="kubernetes", namespace="default"
        )
        target_url, other_url = (
            f"http://{target.pod_ip}:8443",
            f"http://{other.pod_ip}:8443",
        )
        both_rules = [self.provider_rule(allowed), self.provider_rule(forbidden)]
        self.install_harness_egress(both_rules)
        before = await self.provider_job(
            "provider-positive-before", [target_url, other_url]
        )
        self.install_harness_egress([self.provider_rule(allowed)])
        restricted = await self.provider_job(
            "provider-egress",
            [target_url],
            forbidden=[(other.pod_ip, 8443), (api.spec.cluster_ip, 443)],
        )
        # A second positive control proves the denied peer stayed reachable and
        # listening; its denial cannot pass merely because the fixture died.
        self.install_harness_egress(both_rules)
        after = await self.provider_job(
            "provider-positive-after", [target_url, other_url]
        )
        self.install_harness_egress([self.provider_rule(allowed)])
        self.provider_evidence = {
            "providerPodUID": target.pod_uid,
            "otherPodUID": other.pod_uid,
            "positiveBeforeWorkId": before,
            "restrictedWorkId": restricted,
            "positiveAfterWorkId": after,
            "installedRules": [self.provider_rule(allowed)],
            "deniedTargets": [
                "unselected live provider fixture",
                "Kubernetes API service",
            ],
            "negativeChecksBeforeFirstProviderCall": True,
            "transport": "HTTP POST /v1/chat/completions from a generic Python harness",
        }
        self.passed(
            "operator JSON policy permits provider HTTP from a real harness while an unselected live peer and Kubernetes API stay denied from startup"
        )

    async def call(self, method, **kwargs):
        return await asyncio.to_thread(method, _request_timeout=20, **kwargs)

    def passed(self, check):
        self.checks.append(check)
        announce("PASS: " + check)

    async def create_namespace(self):
        namespace = await self.call(
            self.core.create_namespace,
            body={
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": self.namespace,
                    "labels": {"srw.io/test-gate": "manifest-service"},
                },
            },
        )
        self.namespace_uid = namespace.metadata.uid
        await self.call(
            self.network.create_namespaced_network_policy,
            namespace=self.namespace,
            body={
                "apiVersion": "networking.k8s.io/v1",
                "kind": "NetworkPolicy",
                "metadata": {"name": "default-deny", "namespace": self.namespace},
                "spec": {
                    "podSelector": {},
                    "policyTypes": ["Ingress", "Egress"],
                    "ingress": [],
                    "egress": [],
                },
            },
        )
        announce("Created service integration namespace " + self.namespace)

    async def apply(self, document):
        try:
            result = await self.resources.apply(
                json.dumps(document), self.actor, format="json"
            )
        except HTTPException as exc:
            # The manifest name belongs to this script. Do not log arbitrary
            # response bodies which could include materialized bindings.
            raise GateFailure(
                f"Test manifest {document['metadata']['name']} admission returned HTTP {exc.status_code}."
            ) from None
        work_id = str(next(iter(result["executions"].values())))
        snapshot = await read_execution(self.db, "Job", work_id)
        require(
            snapshot is not None and snapshot["harness_adapter"] == "generic",
            "Native admission did not freeze a generic execution.",
        )
        require(
            str(snapshot["owner_id"]) == str(self.actor["id"]),
            "Admission changed the Account owner.",
        )
        self.executions[work_id] = str(snapshot["id"])
        return work_id, snapshot, result

    async def observe_objects(self):
        pods = await self.call(self.core.list_namespaced_pod, namespace=self.namespace)
        for pod in pods.items:
            container = pod.spec.containers[0]
            record = self.observed.setdefault(
                pod.metadata.name,
                {
                    "uid": pod.metadata.uid,
                    "image": container.image,
                    "command": container.command,
                    "args": container.args,
                    "initializers": [
                        item.name for item in pod.spec.init_containers or []
                    ],
                    "workspace": (pod.metadata.labels or {}).get(
                        "srw.io/workspace-instance"
                    ),
                    "phases": [],
                },
            )
            require(record["uid"] == pod.metadata.uid, "Observed Pod identity changed.")
            require(
                pod.spec.automount_service_account_token is False,
                "A native Pod received an ambient Kubernetes token.",
            )
            require(
                not container.env_from,
                "A native Pod received shared environment imports.",
            )
            if pod.status.phase not in record["phases"]:
                record["phases"].append(pod.status.phase)
            for state in pod.status.container_statuses or []:
                if state.image_id:
                    record["imageID"] = state.image_id
        rows = await self.db.fetch(
            "SELECT id,generation,pvc_uid,initialized,ssh_ciphertext FROM srw_workspace_instances"
        )
        for row in rows:
            if row["pvc_uid"]:
                generations = self.workspace_history.setdefault(str(row["id"]), {})
                entry = generations.setdefault(
                    row["generation"], {"pvcUID": row["pvc_uid"]}
                )
                entry["initialized"] = bool(row["initialized"])
                if row["ssh_ciphertext"]:
                    entry["credentialDigest"] = hashlib.sha256(
                        row["ssh_ciphertext"].encode()
                    ).hexdigest()

    async def finish(self, work_id):
        deadline = time.monotonic() + 330
        statuses, phases, restarted = [], [], False
        while time.monotonic() < deadline:
            await self.execution.reconcile_one(self.executions[work_id])
            await self.observe_objects()
            job = await self.db.get_job(work_id)
            if job["status"] not in statuses:
                statuses.append(job["status"])
                announce(f"Service execution {work_id}: {job['status']}")
            attempts = await self.db.fetch(
                "SELECT attempt,phase,pod_uid,image_id,exit_code,cleaned_at FROM srw_execution_attempts WHERE execution_id=$1 ORDER BY attempt",
                UUID(self.executions[work_id]),
            )
            state = [
                (row["attempt"], row["phase"], bool(row["cleaned_at"]))
                for row in attempts
            ]
            if state != phases:
                phases = state
                announce("Reconciler attempts: " + json.dumps(state))
            if not restarted and any(row["pod_uid"] for row in attempts):
                self.configure_services()
                restarted = True
            if job["status"] in {"completed", "failed", "cancelled"} and all(
                row["cleaned_at"] for row in attempts
            ):
                require(
                    job["status"] == "completed",
                    "The service execution did not complete successfully.",
                )
                require(
                    attempts
                    and all(row["pod_uid"] and row["image_id"] for row in attempts),
                    "PostgreSQL lacks exact Pod/image evidence for an attempt.",
                )
                require(
                    restarted,
                    "The gate did not reconstruct the reconciler during execution.",
                )
                self.attempt_evidence[work_id] = [
                    {
                        "attempt": row["attempt"],
                        "phase": row["phase"],
                        "podUID": str(row["pod_uid"]),
                        "imageID": row["image_id"],
                        "exitCode": row["exit_code"],
                        "cleaned": bool(row["cleaned_at"]),
                    }
                    for row in attempts
                ]
                return [dict(row) for row in attempts]
            await asyncio.sleep(1)
        raise GateFailure(
            "Service reconciliation exceeded its bounded deadline; cleanup will use recorded identities."
        )

    async def exercise(self):
        await self.create_namespace()
        forbidden = job_document("foreign-owner", self.images["busybox"])
        forbidden["metadata"]["scope"] = {"kind": "Account", "name": str(uuid4())}
        try:
            await self.resources.apply(json.dumps(forbidden), self.actor, format="json")
        except HTTPException as exc:
            require(
                exc.status_code == 403,
                "Foreign-account admission returned an unexpected result.",
            )
        else:
            raise GateFailure("A non-admin Account could admit another Account's Job.")
        require(
            await self.db.fetchval("SELECT count(*) FROM jobs") == 0,
            "Denied admission created a Job.",
        )
        require(
            await self.db.fetchval("SELECT count(*) FROM srw_resources") == 0,
            "Denied admission committed a resource.",
        )
        self.passed(
            "real Account authorization refuses foreign admission before resource/job/Pod effects"
        )

        default = job_document("image-defaults", self.images["busybox"])
        work_id, _, result = await self.apply(default)
        require(
            (await self.db.get_job(work_id))["status"] == "created",
            "Admission provisioned work before reconciliation.",
        )
        attempts = await self.finish(work_id)
        require(
            len(attempts) == 1 and attempts[0]["exit_code"] == 0,
            "Default image command did not exit successfully.",
        )
        pods = [
            row
            for row in self.observed.values()
            if row["uid"] == str(attempts[0]["pod_uid"])
        ]
        require(
            len(pods) == 1 and pods[0]["command"] is None and pods[0]["args"] is None,
            "The service replaced image ENTRYPOINT/CMD.",
        )
        replay = await self.resources.apply(
            json.dumps(default), self.actor, format="json"
        )
        require(
            replay["executions"] == result["executions"],
            "Reapply created a replacement execution.",
        )
        await self.execution.reconcile()
        require(
            await self.db.fetchval("SELECT count(*) FROM srw_execution_attempts") == 1,
            "Reapply replayed completed work.",
        )
        self.passed(
            "production admission and restarted reconciler complete an unmodified image; reapply never replays it"
        )

        private = {"tools": {"custom_read_file": None}, "unused": [None, False, {}]}
        code = (
            "import json,os,pathlib; "
            "assert json.load(open(os.environ['SRW_CONFIG_FILE'])) == "
            + repr(private)
            + "; "
            "assert json.load(open(os.environ['SRW_TASK_FILE']))['data']['ticket'] == 17; "
            "assert os.environ['SERVICE_GATE_VALUE'] == 'explicit-binding'; "
            "assert pathlib.Path('/run/srw/bindings/fixture').read_text() == 'service-file'; "
            "assert not pathlib.Path('/var/run/secrets/kubernetes.io/serviceaccount/token').exists(); "
            "assert not any(k in os.environ for k in ('DATABASE_URL','SRW_INTERNAL_API_KEY','MCP_INTERNAL_KEY','ORCHESTRATOR_API_KEY')); "
            "assert not pathlib.Path('/home/agent-host').exists()"
        )
        payload = job_document(
            "opaque-delivery",
            self.images["python"],
            command=["python", "-c", code],
            private=private,
        )
        payload["spec"]["execution"]["connectors"] = {
            "environment": {
                "inline": {
                    "driver": "srw.env/v1",
                    "config": {"env": {"SERVICE_GATE_VALUE": "explicit-binding"}},
                }
            },
            "file": {
                "inline": {
                    "driver": "srw.files/v1",
                    "config": {"files": {"/run/srw/bindings/fixture": "service-file"}},
                }
            },
        }
        work_id, snapshot, _ = await self.apply(payload)
        require(
            snapshot["resolved"]["spec"]["execution"]["expert"]["inline"]["runtime"][
                "config"
            ]
            == private,
            "Admission changed opaque private configuration.",
        )
        await self.finish(work_id)
        self.passed(
            "real generic image reads exact opaque config/task/env/file bindings without ambient SRW credentials"
        )

        await self.provider_connectivity()

        recipe = {
            "template": {
                "inline": {
                    "backend": "sandbox",
                    "retention": "Retain",
                    "environment": {"image": self.images["workspace"]},
                    "resources": {"cpu": 0.25, "memory": "256Mi", "storage": "256Mi"},
                    "initialize": [
                        {
                            "command": [
                                "/bin/sh",
                                "-ec",
                                "n=$(cat init-count 2>/dev/null || printf 0); printf '%s\\n' $((n + 1)) > init-count; printf 'service-seed\\n' > seed.txt",
                            ]
                        }
                    ],
                }
            }
        }
        remote = """set -eu
cd /home/agent-host/workspace
test "$(id -u)" = 1000
test "$(cat init-count)" = 1
test "$(cat seed.txt)" = service-seed
n=$(cat service-attempt-count 2>/dev/null || printf 0)
printf '%s\\n' $((n + 1)) > service-attempt-count
if test "$n" = 0; then printf 'attempt-one-data\\n' > proof; exit 23; fi
test "$(cat proof)" = attempt-one-data
printf 'successful\\n' > proof-done
"""
        retry = job_document(
            "workspace-retry",
            self.images["ssh"],
            command=ssh_command(remote),
            workspace=recipe,
            attempts=2,
        )
        work_id, snapshot, _ = await self.apply(retry)
        attempts = await self.finish(work_id)
        require(
            [row["exit_code"] for row in attempts] == [23, 0],
            "The real reconciler did not preserve failure then retry success.",
        )
        require(
            len({row["pod_uid"] for row in attempts}) == 2,
            "Retry reused an old process identity.",
        )
        instance = await self.db.fetchrow("SELECT * FROM srw_workspace_instances")
        instance_id, pvc_uid = str(instance["id"]), str(instance["pvc_uid"])
        require(
            instance["status"] == "Detached"
            and instance["execution_id"] is None
            and instance["initialized"],
            "The retained instance was not safely detached.",
        )
        history = self.workspace_history[instance_id]
        require(
            set(history) == {1, 2}
            and {str(row["pvcUID"]) for row in history.values()} == {pvc_uid},
            "Retry did not reuse the exact retained PVC.",
        )
        require(
            history[1]["credentialDigest"] != history[2]["credentialDigest"],
            "The retry did not rotate stored attachment credentials.",
        )
        workspace_pods = [
            row for row in self.observed.values() if row["workspace"] == instance_id
        ]
        require(
            len(workspace_pods) == 2
            and sorted(len(row["initializers"]) for row in workspace_pods) == [0, 1],
            "Initialization was not limited to the first workspace generation.",
        )
        self.passed(
            "Postgres reconciler retries exit23 as a fresh Pod on the same PVC, initializes once, and rotates attachment credentials"
        )

        reused = job_document(
            "retained-cross-job",
            self.images["ssh"],
            command=ssh_command(
                'set -eu; cd /home/agent-host/workspace; test "$(cat init-count)" = 1; test "$(cat service-attempt-count)" = 2; test "$(cat proof-done)" = successful'
            ),
            workspace={"instanceRef": {"uid": instance_id}},
        )
        work_id, second_snapshot, _ = await self.apply(reused)
        require(
            second_snapshot["id"] != snapshot["id"],
            "Workspace reuse did not admit an independent execution.",
        )
        await self.finish(work_id)
        instance = await self.db.fetchrow(
            "SELECT * FROM srw_workspace_instances WHERE id=$1", UUID(instance_id)
        )
        require(
            instance["generation"] == 3
            and str(instance["pvc_uid"]) == pvc_uid
            and instance["execution_id"] is None,
            "Cross-job workspace ownership/storage did not converge.",
        )
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            result = await self.workspace.delete(
                instance_id, self.actor, expected_generation=3
            )
            if result["deleted"]:
                break
            await asyncio.sleep(1)
        else:
            raise GateFailure(
                "Service-owned retained workspace release did not finish."
            )
        require(
            not (
                await self.call(
                    self.core.list_namespaced_persistent_volume_claim,
                    namespace=self.namespace,
                )
            ).items,
            "The released PVC remains present.",
        )
        self.passed(
            "a second manifest Job reuses initialized files across executions, then the workspace service releases the exact PVC"
        )

    async def retire_provider_fixtures(self):
        pending = dict(self.provider_fixtures)
        terminal = set()
        fixtures = GenericHarnessRuntime(
            self.core, self.network, namespace=self.namespace
        )
        for identity, pod_uid in pending.items():
            observed = await fixtures.observe(identity, expected_pod_uid=pod_uid)
            if observed.containers_terminal:
                terminal.add(identity)
            elif not observed.pod_absent:
                await fixtures.cancel(identity, expected_pod_uid=pod_uid)
        deadline = time.monotonic() + 120
        while pending and time.monotonic() < deadline:
            for identity, pod_uid in list(pending.items()):
                observed = await fixtures.observe(identity, expected_pod_uid=pod_uid)
                if observed.containers_terminal:
                    terminal.add(identity)
                # Pod removal may precede the Secret/policy retirement. Keep
                # positive terminal evidence and retry until every owned object
                # is gone; absence alone never establishes that evidence.
                if identity in terminal and await fixtures.cleanup(
                    identity, expected_pod_uid=pod_uid
                ):
                    del pending[identity]
            if pending:
                await asyncio.sleep(1)
        require(not pending, "Provider fixture retirement did not finish.")

    async def cleanup(self):
        if not self.namespace_uid:
            return
        for work_id in self.executions:
            job = await self.db.get_job(work_id)
            if job["status"] not in {"completed", "failed", "cancelled"}:
                await self.execution.cancel(work_id)
        await self.retire_provider_fixtures()
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            for execution_id in self.executions.values():
                await self.execution.reconcile_one(execution_id)
            if not (
                await self.call(self.core.list_namespaced_pod, namespace=self.namespace)
            ).items:
                break
            await asyncio.sleep(1)
        else:
            raise GateFailure("Service cleanup could not fence every test Pod.")
        for method in (
            self.core.list_namespaced_secret,
            self.network.list_namespaced_network_policy,
        ):
            objects = await self.call(method, namespace=self.namespace)
            require(
                all(item.metadata.name == "default-deny" for item in objects.items),
                "Service cleanup left runtime delivery objects.",
            )
        current = await self.call(self.core.read_namespace, name=self.namespace)
        require(
            current.metadata.uid == self.namespace_uid
            and current.metadata.labels.get("srw.io/test-gate") == "manifest-service",
            "Disposable namespace ownership changed; cleanup refused.",
        )
        await self.call(
            self.core.delete_namespace,
            name=self.namespace,
            body={"preconditions": {"uid": self.namespace_uid}},
        )
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            try:
                await self.call(self.core.read_namespace, name=self.namespace)
            except client.ApiException as exc:
                if exc.status == 404:
                    return
                raise
            await asyncio.sleep(1)
        raise GateFailure("The test-owned namespace did not finish deleting.")


async def run(args):
    # Generic admission runs only approved images, compared by exact string:
    # the gate's job images must be digest references (publish_images pins
    # them) to be on the approved list ServiceGate derives from them.
    api, nodes = await asyncio.to_thread(
        verify_disposable_profile, args.kube_context, args.kubeconfig
    )
    # Own encryption material exists only in this child process and ephemeral DB.
    os.environ["APP_ENCRYPTION_KEY"] = secrets.token_hex(16)
    os.environ["MANIFEST_HARNESS_EGRESS"] = "[]"
    from orchestrator.security.crypto import reset_cipher_cache

    reset_cipher_cache()
    pg, db, gate, container_id = None, None, None, None
    try:
        cilium = await asyncio.to_thread(
            client.CoreV1Api(api).list_namespaced_pod,
            "kube-system",
            label_selector="k8s-app=cilium",
            _request_timeout=15,
        )
        cilium_images = [
            state.image_id
            for pod in cilium.items
            for state in pod.status.container_statuses or []
            if state.ready and state.image_id
        ]
        require(
            cilium.items and len(cilium_images) == len(cilium.items),
            "The Cilium DaemonSet is not ready with observed image identities.",
        )
        images, image_evidence = await asyncio.to_thread(native.publish_images, args)
        pg = PostgresContainer("pgvector/pgvector:pg15")
        await asyncio.to_thread(pg.start)
        container_id = pg.get_wrapped_container().id
        db = PostgresDB(
            re.sub(r"^postgresql\+\w+://", "postgresql://", pg.get_connection_url())
        )
        await db.connect()
        await db.execute(
            (ROOT / "src/orchestrator/database/schema_current.sql").read_text()
        )
        actor = dict(
            await db.fetchrow(
                "INSERT INTO users(display_name,is_approved,is_admin) VALUES('Disposable service gate',TRUE,FALSE) RETURNING *"
            )
        )
        gate = ServiceGate(api, db, images, actor)
        try:
            await gate.exercise()
        finally:
            await gate.cleanup()
        evidence = {
            "result": "passed",
            "context": args.kube_context,
            "nodes": nodes,
            "namespaceRemoved": gate.namespace,
            "namespaceUID": gate.namespace_uid,
            "checks": gate.checks,
            "images": image_evidence,
            "ciliumImageIDs": cilium_images,
            "executionCount": len(gate.executions),
            "executions": gate.executions,
            "attempts": gate.attempt_evidence,
            "providerConnectivity": gate.provider_evidence,
            "workspaceGenerations": {
                instance_id: {
                    str(generation): {
                        "pvcUID": str(row["pvcUID"]),
                        "initialized": row["initialized"],
                    }
                    for generation, row in generations.items()
                }
                for instance_id, generations in gate.workspace_history.items()
            },
            "schemaSHA256": hashlib.sha256(
                (ROOT / "src/orchestrator/database/schema_current.sql").read_bytes()
            ).hexdigest(),
            "sourceSHA256": {
                str(path.relative_to(ROOT)): hashlib.sha256(
                    path.read_bytes()
                ).hexdigest()
                for path in (
                    ROOT / "src/orchestrator/services/manifest_execution.py",
                    ROOT / "src/orchestrator/services/manifest_harness_egress.py",
                    ROOT / "src/orchestrator/services/manifest_workspaces.py",
                    ROOT / "src/orchestrator/services/generic_harness_runtime.py",
                    ROOT / "src/orchestrator/services/manifest_workspace_runtime.py",
                )
            },
            "podIdentities": {
                name: {
                    key: value
                    for key, value in row.items()
                    if key not in {"command", "args"}
                }
                for name, row in gate.observed.items()
            },
            "scope": "Production manifest services and full-schema PostgreSQL admission/reconciliation with real Cilium Pods, SSH workspaces and PVCs. No installed SRW identity or database was used.",
        }
    finally:
        if db is not None:
            await db.disconnect()
        if pg is not None:
            await asyncio.to_thread(pg.stop)
        api.close()
        if container_id:
            inspected = await asyncio.to_thread(
                subprocess.run,
                ["docker", "inspect", container_id],
                capture_output=True,
                timeout=15,
            )
            require(
                inspected.returncode != 0,
                "The disposable PostgreSQL container was not removed.",
            )
    evidence["postgresContainerRemoved"] = container_id
    announce(json.dumps(evidence, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kube-context", required=True)
    parser.add_argument("--kubeconfig", required=True)
    parser.add_argument("--workspace-image", default="srw-manifest-workspace:gate")
    parser.add_argument("--busybox-image", default="busybox:1.36")
    parser.add_argument("--ssh-image", default="alpine/git:latest")
    parser.add_argument("--python-image", default="python:3.12-slim")
    args = parser.parse_args()
    try:
        asyncio.run(run(args))
    except Exception as exc:
        reason = (
            str(exc)
            if isinstance(exc, GateFailure)
            else f"Unexpected {type(exc).__name__}; no arbitrary API body logged."
        )
        announce(json.dumps({"result": "failed", "reason": reason}))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
