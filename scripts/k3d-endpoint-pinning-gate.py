#!/usr/bin/env python3
"""Local k3d gate for per-endpoint TLS certificate pinning.

Design: knowledge-base/knowledge/superpowers/specs/
2026-11-06-endpoint-certificate-pinning-design.md, "Live check on k3d (final
tree)". The eight checks there need a model server whose certificate SRW can't
verify, and that can change its certificate on demand. This gate brings one
up inside the cluster and runs the API side of the checks against it; the
Cockpit side (badges, the certificate block) is looked at in a browser.

The stand-in. A Deployment from the orchestrator image (already in the
cluster, so nothing is pulled) and a ClusterIP Service with two ports, 8443
and 9443 (check 8 moves the endpoint to the second one). It terminates TLS
itself with one of four certificates, chosen with ``kubectl set env
CERT=...``:

  leaf     X.509 v1, CN=<ClusterIP>, no subjectAltName, signed by a test CA
           (like the university router: valid for no address)
  expired  the same key and names, validity 2020-01-01 .. 2021-01-01
  swap     a new key, the same names and CA
  good     subjectAltName IP:<ClusterIP>, the same CA (verifies normally once
           the CA is in trust.extraCaCertificates)

Behind TLS it runs the repository's deterministic E2E provider
(tests/e2e/app/deterministic_provider/provider.py), keyless: it answers chat
itself (a scripted reply for Sessions; for jobs three read_file turns and
job_complete, which takes a direct-task worker job on a virtual workspace to
completion), /v1/embeddings (4096 dimensions), /v1/rerank and GET /v1/models.
No chat upstream is needed. Memory extraction returns two fixed memories, so a
later retrieval in the user's default Project has candidates and the reranker
is called. Every handshake (ok or failed) and every request (path, client IP,
whether Authorization was sent) is one JSON line in the stand-in's log.

Check 3 sets the test user's default chat, auxiliary, embedding and rerank
models to the stand-in's rows. A Session takes its memory embedding and
reranker from the system defaults instead, so on a Session only chat reaches
the stand-in; jobs cover the embedding and rerank clients.

Safety envelope: only the context k3d-srw and the namespace srw; nothing
changes without --confirm LOCAL-K3D-DISPOSABLE; the admin API is reached from
inside the orchestrator pod with the key that pod already holds, so no secret
is ever on a command line; the certificates are throwaway test material.

  .venv/bin/python scripts/k3d-endpoint-pinning-gate.py plan
  ... --confirm LOCAL-K3D-DISPOSABLE up              # stand-in + certificates
  ... --confirm LOCAL-K3D-DISPOSABLE check 1         # one check, PASS/FAIL
  ... --confirm LOCAL-K3D-DISPOSABLE cert swap       # switch the certificate
  ... logs --since 2026-11-23T00:30:00Z              # the stand-in's log
  ... --confirm LOCAL-K3D-DISPOSABLE cleanup         # remove everything

A command that stops early (an error, not a FAIL line) runs cleanup before it
exits, so nothing is left behind; --keep skips that to inspect the state.
Cleanup attempts every step and removes the stand-in last whatever failed
before it. A FAIL line doesn't clean up: later checks build on the state.

Checks 1b (address mismatch) and 7 (a certificate that verifies normally)
need the stand-in's CA in ``trust.extraCaCertificates`` (``ca-pem`` prints
it); that is a Helm values change the operator makes and reverts. Evidence
(API responses, log excerpts) goes to <workdir>/evidence/.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import functools
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
CONTEXT = "k3d-srw"
NAMESPACE = "srw"
LOCAL_CONFIRMATION = "LOCAL-K3D-DISPOSABLE"
NAME = "pingate-standin"
LABEL = "pingate"
PORT, SECOND_PORT = 8443, 9443
CERTS = ("leaf", "expired", "swap", "good")
CHAT, EMBEDDING, RERANK = "pingate-chat", "pingate-embedding", "pingate-rerank"
PROVIDER = ROOT / "tests/e2e/app/deterministic_provider/provider.py"
PIN_SENTENCE = re.compile(
    r"TLS certificate changed for (?P<host>[0-9.]+):(?P<port>\d+): pinned SHA-256 "
    r"(?P<pinned>[0-9A-F:]{95}), server presents (?P<presented>[0-9A-F:]{95})\. "
    r"An admin must re-trust it in Admin → Models\."
)
EVENTS = (
    "endpoint_certificate_trusted",
    "endpoint_certificate_retrusted",
    "endpoint_certificate_pin_removed",
)

# The stand-in's own code: TLS front, logging, and the provider's settings.
STANDIN = r"""
import asyncio, hashlib, json, os, secrets, signal, socket, ssl, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("E2E_CHAT_MODEL_ID", "pingate-chat")
import provider as fixture
import uvicorn

CERT = os.environ.get("CERT", "leaf")
PORTS = (8443, 9443)
fixture.EMBEDDING_MODEL_ID = "pingate-embedding"
fixture.RERANK_MODEL_ID = "pingate-rerank"

# Two fixed memories for every extraction, so a later retrieval in the same
# scope has two candidates and calls the reranker. No E2E token in them.
_MEMORIES = json.dumps({"memories": [
    {"content": "The pin gate stand-in serves chat, embeddings and rerank over TLS on port 8443.",
     "summary": "pin gate stand-in serves three routes",
     "keywords": ["pin", "gate", "standin", "tls", "rerank"], "importance": 0.9,
     "type": "factual", "retrieval_messages": ["which routes does the pin gate stand-in serve"]},
    {"content": "The pin gate checks that every model client verifies the pinned certificate.",
     "summary": "pin gate checks every model client",
     "keywords": ["pin", "gate", "certificate", "client", "verify"], "importance": 0.85,
     "type": "factual", "retrieval_messages": ["what does the pin gate check"]},
]})
_structured = fixture._structured_content
fixture._structured_content = lambda name, run: (
    _MEMORIES if name == "ExtractedMemories" else _structured(name, run))

# Today's worker job with only read_file and job_complete bound runs in
# direct-task mode (no todo_complete / next_phase_todos), so the fixture's
# phased worker-job script is replaced: three read_file turns (memory is
# extracted every turn and retrieved, so the reranker runs), then job_complete.
def _direct_job_tool_call(step, run_id):
    if step < 3:
        path = "skills/direct-task/SKILL.md" if step == 0 else "README.md"
        return fixture.ToolCallSpec(name="read_file", arguments=json.dumps({"path": path}))
    return fixture.ToolCallSpec(name="job_complete", arguments=json.dumps({
        "summary": f"Completed the pin gate job for E2E-{run_id}.",
        "deliverables": [], "confidence": 1.0}))
fixture._worker_job_tool_call = _direct_job_tool_call

# An uncorrelated call (a dimension probe, a Test probe) goes to the most
# recently armed run instead of failing when several runs are armed.
_resolve = fixture.ScenarioStore.resolve_run
async def _resolve_run(self, payload, *, endpoint):
    try:
        return await _resolve(self, payload, endpoint=endpoint)
    except fixture.ScenarioError as exc:
        if exc.error_type == "run_correlation_required" and self._runs:
            return list(self._runs)[-1]
        raise
fixture.ScenarioStore.resolve_run = _resolve_run

PEERS = {}
TASKS = set()  # asyncio keeps only weak references to tasks
def log(event, **fields):
    print(json.dumps({"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                      "event": event, "cert": CERT, **fields}), flush=True)

class AccessLog:
    def __init__(self, app):
        self.app = app
    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        client = scope.get("client") or ("?", 0)
        peer = PEERS.get(client[1], {"ip": client[0], "listener": None})
        names = {name.lower() for name, _ in scope.get("headers", [])}
        async def logged(message):
            if message["type"] == "http.response.start" and scope["path"] != "/health":
                log("request", method=scope["method"], path=scope["path"],
                    client_ip=peer["ip"], listener=peer["listener"],
                    authorization=b"authorization" in names, status=message["status"])
            await send(message)
        return await self.app(scope, receive, logged)

async def pump(src, dst):
    try:
        while data := await src.read(65536):
            dst.write(data)
            await dst.drain()
    except Exception:
        pass
    finally:
        try:
            dst.close()
        except Exception:
            pass

async def front(listener, ctx):
    # Accept raw sockets and wrap them in TLS from the first byte
    # (connect_accepted_socket): start_tls after start_server races with a
    # ClientHello the plain reader already buffered, and the handshake stalls.
    loop = asyncio.get_running_loop()
    srv = socket.create_server(("0.0.0.0", listener), backlog=128)
    srv.setblocking(False)
    while True:
        conn, peer = await loop.sock_accept(srv)
        task = asyncio.create_task(handle(conn, peer, listener, ctx))
        TASKS.add(task)
        task.add_done_callback(TASKS.discard)

async def handle(conn, peer, listener, ctx):
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    try:
        transport, _ = await loop.connect_accepted_socket(
            lambda: protocol, conn, ssl=ctx, ssl_handshake_timeout=10)
    except Exception as exc:
        log("handshake_failed", client_ip=peer[0], listener=listener,
            error=f"{type(exc).__name__}: {exc}"[:300])
        conn.close()
        return
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)
    log("handshake_ok", client_ip=peer[0], listener=listener,
        version=writer.get_extra_info("ssl_object").version())
    b_reader, b_writer = await asyncio.open_connection("127.0.0.1", 8000)
    local = b_writer.get_extra_info("sockname")[1]
    PEERS[local] = {"ip": peer[0], "listener": listener}
    try:
        await asyncio.gather(pump(reader, b_writer), pump(b_reader, writer))
    finally:
        PEERS.pop(local, None)

async def main():
    store = fixture.ScenarioStore()
    await store.arm("pingate-ambient",
                    fixture.ArmScenarioRequest(scenario="reply", required_responses=100))
    token = secrets.token_urlsafe(32)
    with open("/tmp/control-token", "w") as fh:
        fh.write(token)
    apps = ((AccessLog(fixture.create_inference_app(store, inference_api_key=None)), 8000),
            (fixture.create_control_app(store, control_token=token), 8001))
    servers = [uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port,
               access_log=False, server_header=False, log_level="warning"))
               for app, port in apps]
    for server in servers:
        server.install_signal_handlers = lambda: None
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(f"/certs/{CERT}.pem", f"/certs/{CERT}.key")
    der = ssl.PEM_cert_to_DER_cert(open(f"/certs/{CERT}.pem").read())
    digest = hashlib.sha256(der).hexdigest().upper()
    fronts = [asyncio.create_task(front(p, ctx)) for p in PORTS]
    log("started", ports=list(PORTS), sha256=":".join(digest[i:i + 2] for i in range(0, 64, 2)))
    stop = asyncio.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(signum, stop.set)
    tasks = [asyncio.create_task(server.serve()) for server in servers]
    await stop.wait()
    for server in servers:
        server.should_exit = True
    for f in fronts:
        f.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

asyncio.run(main())
"""

# Runs inside the orchestrator pod: one admin API request, key from the pod.
IN_POD_API = r"""
import json, os, sys, urllib.error, urllib.request
spec = json.loads(sys.stdin.read())
data = None if spec.get("body") is None else json.dumps(spec["body"]).encode()
req = urllib.request.Request("http://localhost:8085" + spec["path"], data=data,
    method=spec["method"], headers={"Content-Type": "application/json",
    "X-Internal-Key": os.environ["MCP_INTERNAL_KEY"], "X-MCP-User-Id": spec["user"]})
try:
    with urllib.request.urlopen(req, timeout=spec["timeout"]) as r:
        status, text = r.status, r.read().decode()
except urllib.error.HTTPError as e:
    status, text = e.code, e.read().decode()
print(json.dumps({"status": status, "text": text}))
"""

# Runs inside the stand-in pod: one control-plane request to the provider.
IN_POD_CONTROL = r"""
import json, sys, urllib.error, urllib.request
spec = json.loads(sys.stdin.read())
token = open("/tmp/control-token").read().strip()
data = None if spec.get("body") is None else json.dumps(spec["body"]).encode()
req = urllib.request.Request("http://127.0.0.1:8001" + spec["path"], data=data,
    method=spec["method"], headers={"Content-Type": "application/json",
    "Authorization": "Bearer " + token})
try:
    with urllib.request.urlopen(req, timeout=30) as r:
        status, text = r.status, r.read().decode()
except urllib.error.HTTPError as e:
    status, text = e.code, e.read().decode()
print(json.dumps({"status": status, "text": text}))
"""


class GateError(RuntimeError):
    pass


def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Gate:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.work = Path(args.workdir)
        self.work.mkdir(parents=True, exist_ok=True)
        (self.work / "certs").mkdir(exist_ok=True)
        (self.work / "evidence").mkdir(exist_ok=True)
        self.state_file = self.work / "state.json"
        self.state: dict[str, Any] = (
            json.loads(self.state_file.read_text()) if self.state_file.exists() else {}
        )
        self.failed: list[str] = []

    # -- plumbing -----------------------------------------------------------

    def save(self) -> None:
        self.state_file.write_text(json.dumps(self.state, indent=2, sort_keys=True))

    def kubectl(self, *argv: str, stdin: str | None = None, timeout: int = 180) -> str:
        done = subprocess.run(
            ["kubectl", f"--context={CONTEXT}", "-n", NAMESPACE, *argv],
            input=stdin,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if done.returncode != 0:
            raise GateError(
                f"kubectl {' '.join(argv[:3])}: {done.stderr.strip()[:500]}"
            )
        return done.stdout

    def psql(self, sql: str) -> str:
        return self.kubectl(
            "exec",
            "-i",
            "srw-postgres-0",
            "-c",
            "postgres",
            "--",
            "psql",
            "-U",
            "srw",
            "-d",
            "srw",
            "-At",
            "-v",
            "ON_ERROR_STOP=1",
            stdin=sql,
        )

    def orchestrator_pod(self) -> str:
        return self.kubectl(
            "get",
            "pod",
            "-l",
            "app.kubernetes.io/component=orchestrator",
            "--field-selector=status.phase=Running",
            "-o",
            "jsonpath={.items[0].metadata.name}",
        )

    def api(
        self,
        method: str,
        path: str,
        body: Any = None,
        *,
        user: str | None = None,
        timeout: int = 120,
    ) -> tuple[int, Any]:
        spec = {
            "method": method,
            "path": path,
            "body": body,
            "user": user or self.admin_id(),
            "timeout": timeout,
        }
        out = self.kubectl(
            "exec",
            "-i",
            self.orchestrator_pod(),
            "-c",
            "orchestrator",
            "--",
            "python3",
            "-c",
            IN_POD_API,
            stdin=json.dumps(spec),
            timeout=timeout + 30,
        )
        result = json.loads(out.strip().splitlines()[-1])
        try:
            return result["status"], json.loads(result["text"])
        except ValueError:
            return result["status"], result["text"]

    def control(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        out = self.kubectl(
            "exec",
            "-i",
            f"deploy/{NAME}",
            "--",
            "python3",
            "-c",
            IN_POD_CONTROL,
            stdin=json.dumps({"method": method, "path": path, "body": body}),
        )
        result = json.loads(out.strip().splitlines()[-1])
        return result["status"], json.loads(result["text"])

    def admin_id(self) -> str:
        if "admin_id" not in self.state:
            uid = self.psql(
                f"SELECT id FROM users WHERE email = '{self.args.admin}' AND is_admin;"
            ).strip()
            if not re.fullmatch(r"[0-9a-f-]{36}", uid):
                raise GateError(f"no admin user {self.args.admin}")
            self.state["admin_id"] = uid
            self.save()
        return self.state["admin_id"]

    def evidence(self, name: str, data: Any) -> Path:
        path = self.work / "evidence" / f"{name}.json"
        path.write_text(json.dumps(data, indent=2, sort_keys=True, default=str))
        return path

    def check(self, label: str, ok: bool, detail: str = "") -> bool:
        print(
            f"  {'PASS' if ok else 'FAIL'}  {label}"
            + (f"  [{detail}]" if detail else "")
        )
        if not ok:
            self.failed.append(label)
        return ok

    def fingerprint(self, cert: str) -> str:
        out = subprocess.run(
            [
                "openssl",
                "x509",
                "-noout",
                "-fingerprint",
                "-sha256",
                "-in",
                str(self.work / "certs" / f"{cert}.pem"),
            ],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        return out.strip().split("=", 1)[1]

    def logs(self, since: str | None = None) -> list[dict[str, Any]]:
        argv = ["logs", f"deploy/{NAME}"]
        if since:
            argv.append(f"--since-time={since}")
        lines = []
        for raw in self.kubectl(*argv).splitlines():
            try:
                lines.append(json.loads(raw))
            except ValueError:
                lines.append({"event": "raw", "text": raw})
        return lines

    def pod_ips(self) -> dict[str, str]:
        out = self.kubectl("get", "pods", "-o", "json")
        return {
            item["status"]["podIP"]: item["metadata"]["name"]
            for item in json.loads(out)["items"]
            if item["status"].get("podIP")
        }

    # -- the stand-in -------------------------------------------------------

    def make_certs(self, ip: str) -> None:
        d = self.work / "certs"
        (d / "min.cnf").write_text(
            "[req]\ndistinguished_name = dn\nprompt = no\n[dn]\nCN = " + ip + "\n"
        )

        def ossl(*argv: str) -> None:
            subprocess.run(["openssl", *argv], cwd=d, check=True, capture_output=True)

        ossl(
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
            "-days",
            "3650",
            "-subj",
            "/CN=SRW pin gate test CA",
        )
        for key in ("leaf", "swap", "good"):
            ossl("genrsa", "-out", f"{key}.key", "2048")
        sign = ("-config", "min.cnf", "-CA", "ca.pem", "-CAkey", "ca.key")
        ossl(
            "req",
            *sign,
            "-x509v1",
            "-key",
            "leaf.key",
            "-days",
            "3650",
            "-out",
            "leaf.pem",
        )
        ossl(
            "req",
            *sign,
            "-x509v1",
            "-key",
            "leaf.key",
            "-not_before",
            "20200101000000Z",
            "-not_after",
            "20210101000000Z",
            "-out",
            "expired.pem",
        )
        ossl(
            "req",
            *sign,
            "-x509v1",
            "-key",
            "swap.key",
            "-days",
            "3650",
            "-out",
            "swap.pem",
        )
        ossl(
            "req",
            *sign,
            "-x509",
            "-key",
            "good.key",
            "-days",
            "365",
            "-addext",
            f"subjectAltName=IP:{ip}",
            "-addext",
            "basicConstraints=critical,CA:FALSE",
            "-addext",
            "keyUsage=critical,digitalSignature,keyEncipherment",
            "-addext",
            "extendedKeyUsage=serverAuth",
            "-out",
            "good.pem",
        )
        (d / "expired.key").write_bytes((d / "leaf.key").read_bytes())

    def up(self) -> None:
        image = self.kubectl(
            "get",
            "deploy/srw-orchestrator",
            "-o",
            "jsonpath={.spec.template.spec.containers[0].image}",
        ).strip()
        service = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": NAME, "labels": {"app": LABEL}},
            "spec": {
                "selector": {"app": LABEL},
                "ports": [
                    {"name": "tls", "port": PORT, "targetPort": PORT},
                    {
                        "name": "tls-second",
                        "port": SECOND_PORT,
                        "targetPort": SECOND_PORT,
                    },
                ],
            },
        }
        self.kubectl("apply", "-f", "-", stdin=json.dumps(service))
        ip = self.kubectl(
            "get", "svc", NAME, "-o", "jsonpath={.spec.clusterIP}"
        ).strip()
        leaf = self.work / "certs" / "leaf.pem"
        if (
            not leaf.exists()
            or f"CN = {ip}" not in (self.work / "certs" / "min.cnf").read_text()
        ):
            self.make_certs(ip)
        d = self.work / "certs"
        files = {f"{c}.pem": d / f"{c}.pem" for c in CERTS}
        files.update({f"{c}.key": d / f"{c}.key" for c in CERTS})
        files["ca.pem"] = d / "ca.pem"
        secret = {
            "apiVersion": "v1",
            "kind": "Secret",
            "type": "Opaque",
            "metadata": {"name": NAME + "-certs", "labels": {"app": LABEL}},
            "data": {
                k: base64.b64encode(p.read_bytes()).decode() for k, p in files.items()
            },
        }
        code = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": NAME + "-code", "labels": {"app": LABEL}},
            "data": {"provider.py": PROVIDER.read_text(), "standin.py": STANDIN},
        }
        probe = "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2)"
        deployment = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": NAME, "labels": {"app": LABEL}},
            "spec": {
                "replicas": 1,
                "strategy": {"type": "Recreate"},
                "selector": {"matchLabels": {"app": LABEL}},
                "template": {
                    "metadata": {"labels": {"app": LABEL}},
                    "spec": {
                        "automountServiceAccountToken": False,
                        "containers": [
                            {
                                "name": "standin",
                                "image": image,
                                "imagePullPolicy": "IfNotPresent",
                                "command": ["python3", "-u", "/standin/standin.py"],
                                "env": [{"name": "CERT", "value": "leaf"}],
                                "ports": [
                                    {"containerPort": PORT},
                                    {"containerPort": SECOND_PORT},
                                ],
                                "readinessProbe": {
                                    "exec": {"command": ["python3", "-c", probe]},
                                    "periodSeconds": 5,
                                    "timeoutSeconds": 4,
                                },
                                "volumeMounts": [
                                    {
                                        "name": "code",
                                        "mountPath": "/standin",
                                        "readOnly": True,
                                    },
                                    {
                                        "name": "certs",
                                        "mountPath": "/certs",
                                        "readOnly": True,
                                    },
                                ],
                                "resources": {
                                    "requests": {"cpu": "20m", "memory": "96Mi"},
                                    "limits": {"memory": "384Mi"},
                                },
                            }
                        ],
                        "volumes": [
                            {"name": "code", "configMap": {"name": NAME + "-code"}},
                            {
                                "name": "certs",
                                "secret": {"secretName": NAME + "-certs"},
                            },
                        ],
                    },
                },
            },
        }
        running = self.kubectl(
            "get",
            "pod",
            "-l",
            f"app={LABEL}",
            "-o",
            "jsonpath={.items[*].metadata.name}",
        ).split()
        for pod in running:
            log = self.work / "evidence" / f"standin-{self.state.get('cert')}-{pod}.log"
            log.write_text(self.kubectl("logs", pod))
        for doc in (secret, code, deployment):
            self.kubectl("apply", "-f", "-", stdin=json.dumps(doc))
        if running:
            self.kubectl("rollout", "restart", f"deploy/{NAME}")
        self.kubectl(
            "rollout", "status", f"deploy/{NAME}", "--timeout=180s", timeout=200
        )
        self.wait_reachable()
        self.state.update(cluster_ip=ip, cert="leaf", image=image)
        self.save()
        print(
            f"stand-in up at https://{ip}:{PORT}/v1 (and :{SECOND_PORT}), image {image}"
        )
        for cert in CERTS:
            print(f"  {cert:8s} {self.fingerprint(cert)}")

    def switch(self, cert: str) -> None:
        old = self.kubectl(
            "get",
            "pod",
            "-l",
            f"app={LABEL}",
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ).strip()
        if old:
            (
                self.work
                / "evidence"
                / f"standin-{self.state.get('cert', 'x')}-{old}.log"
            ).write_text(self.kubectl("logs", old))
        self.kubectl("set", "env", f"deploy/{NAME}", f"CERT={cert}")
        self.kubectl(
            "rollout", "status", f"deploy/{NAME}", "--timeout=180s", timeout=200
        )
        self.wait_reachable()
        self.state["cert"] = cert
        self.save()
        started = [x for x in self.logs() if x.get("event") == "started"]
        print(f"stand-in now presents {cert}: {started[-1] if started else '?'}")

    def wait_reachable(self) -> None:
        """The Service answers a few seconds after the rollout (kube-router)."""
        ip = self.kubectl(
            "get", "svc", NAME, "-o", "jsonpath={.spec.clusterIP}"
        ).strip()
        probe = (
            "import socket, sys, time\n"
            "for _ in range(60):\n"
            "    try:\n"
            f"        socket.create_connection(('{ip}', {PORT}), 2).close(); sys.exit(0)\n"
            "    except OSError:\n"
            "        time.sleep(1)\n"
            "sys.exit(1)\n"
        )
        self.kubectl(
            "exec",
            self.orchestrator_pod(),
            "-c",
            "orchestrator",
            "--",
            "python3",
            "-c",
            probe,
            timeout=90,
        )

    def down(self) -> list[str]:
        """Delete the stand-in's objects, each one attempted. Returns the errors."""
        errors: list[str] = []
        for kind in ("deploy", "svc", "secret", "configmap"):
            name = {"secret": NAME + "-certs", "configmap": NAME + "-code"}.get(
                kind, NAME
            )
            try:
                self.kubectl("delete", kind, name, "--ignore-not-found")
            except Exception as error:
                errors.append(f"delete {kind}/{name}: {error}")
        print(
            "stand-in removed"
            if not errors
            else "stand-in NOT fully removed: " + "; ".join(errors)
        )
        return errors

    # -- catalog ------------------------------------------------------------

    def base_url(self, port: int = PORT) -> str:
        return f"https://{self.state['cluster_ip']}:{port}/v1"

    def endpoint_id(self) -> str:
        return self.state["endpoint_id"]

    def endpoint_row(self) -> dict[str, Any]:
        status, rows = self.api("GET", "/api/admin/providers/endpoints")
        return next(r for r in rows if r["id"] == self.endpoint_id())

    def test(self, tag: str) -> dict[str, Any]:
        since = now_iso()
        status, result = self.api(
            "POST", f"/api/admin/providers/endpoints/{self.endpoint_id()}/test"
        )
        time.sleep(1)
        log = self.logs(since)
        self.evidence(
            f"{tag}-test", {"status": status, "result": result, "standin_log": log}
        )
        tls = (result or {}).get("tls") or {}
        print(
            f"  Test: http {status} ok={result.get('ok')} status={result.get('status')} "
            f"reason={tls.get('reason')} verified={tls.get('verified')} "
            f"pin_matches={tls.get('pin_matches')} publicly_valid={tls.get('publicly_valid')} "
            f"can_trust={tls.get('can_trust')} error={result.get('error')!r}"
        )
        result["_log"] = log
        return result

    def events(self, event_type: str) -> list[dict[str, Any]]:
        status, body = self.api(
            "GET", f"/api/admin/security-events?event_type={event_type}&limit=50"
        )
        rows = body.get("events", body) if isinstance(body, dict) else body
        return [r for r in rows if str(r.get("resource_id")) == self.endpoint_id()]

    def all_pin_events(self) -> list[dict[str, Any]]:
        return [e for t in EVENTS for e in self.events(t)]

    def requests_in(self, log: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [x for x in log if x.get("event") == "request"]

    # -- checks -------------------------------------------------------------

    def check1(self) -> None:
        leaf = self.fingerprint("leaf")
        if "endpoint_id" not in self.state:
            status, row = self.api(
                "POST",
                "/api/admin/providers/endpoints",
                {"label": "pingate", "base_url": self.base_url()},
            )
            if status != 200:
                raise GateError(f"create endpoint: {status} {row}")
            self.state["endpoint_id"] = row["id"]
            self.save()
        result = self.test("check1a")
        tls = result.get("tls") or {}
        cert = tls.get("certificate") or {}
        self.check(
            "Test fails without sending a request",
            result.get("ok") is False and not self.requests_in(result["_log"]),
        )
        self.check(
            "reason is unknown_issuer",
            tls.get("reason") == "unknown_issuer",
            tls.get("reason"),
        )
        self.check(
            "fingerprint equals openssl x509 -fingerprint -sha256 of leaf",
            cert.get("sha256") == leaf,
            f"{cert.get('sha256')} vs {leaf}",
        )
        self.check("Trust is offered", tls.get("can_trust") is True)

    def check1b(self) -> None:
        result = self.test("check1b")
        tls = result.get("tls") or {}
        self.check(
            "with the CA trusted: reason is address_mismatch",
            tls.get("reason") == "address_mismatch",
            f"{tls.get('reason')}: {tls.get('message')}",
        )
        self.check("no request was sent", not self.requests_in(result["_log"]))

    def trust(self, cert: str, tag: str) -> tuple[int, Any]:
        fp = self.fingerprint(cert)
        status, row = self.api(
            "PUT",
            f"/api/admin/providers/endpoints/{self.endpoint_id()}/tls-pin",
            {"sha256": fp},
        )
        self.evidence(f"{tag}-trust-{cert}", {"status": status, "body": row})
        return status, row

    def ensure_models(self) -> None:
        if self.state.get("model_ids"):
            return
        ids = {}
        for model_id, caps in (
            (CHAT, ["chat", "auxiliary"]),
            (EMBEDDING, ["embedding"]),
            (RERANK, ["rerank"]),
        ):
            status, row = self.api(
                "POST",
                "/api/admin/providers/models",
                {
                    "provider_kind": "endpoint",
                    "provider_ref": self.endpoint_id(),
                    "model_id": model_id,
                    "display_label": model_id,
                    "capabilities": caps,
                    "family": "e2e",
                    "context_window": 128000,
                    "enabled": True,
                    "notes": "pinning k3d gate",
                },
            )
            if status != 200:
                raise GateError(f"create model {model_id}: {status} {row}")
            ids[model_id] = row["id"]
        self.state["model_ids"] = ids
        self.save()

    def check2(self) -> None:
        leaf = self.fingerprint("leaf")
        row = self.endpoint_row()
        if (row.get("tls_pin") or {}).get("sha256") == leaf:
            print("  (already trusted, from the Cockpit)")
            self.evidence("check2-endpoint-after-cockpit-trust", row)
        else:
            status, row = self.trust("leaf", "check2")
            self.check(
                "Trust returns the endpoint with the pin",
                status == 200 and (row.get("tls_pin") or {}).get("sha256") == leaf,
                f"http {status}",
            )
        events = self.events("endpoint_certificate_trusted")
        self.evidence("check2-events", events)
        self.check(
            "security event endpoint_certificate_trusted",
            any(leaf in str(e.get("detail")) for e in events),
        )
        result = self.test("check2")
        self.check(
            "Test is OK through the pin",
            result.get("ok") is True
            and (result.get("tls") or {}).get("pin_matches") is True,
        )
        self.check(
            "the probe reached the stand-in",
            any(x["path"] == "/v1/models" for x in self.requests_in(result["_log"])),
        )
        status, disc = self.api(
            "POST", f"/api/admin/providers/endpoints/{self.endpoint_id()}/discover"
        )
        self.evidence("check2-discover", {"status": status, "body": disc})
        names = json.dumps(disc)
        self.check(
            "Discover lists the models",
            status == 200 and CHAT in names and EMBEDDING in names,
        )
        self.ensure_models()
        for model_id, catalog_id in self.state["model_ids"].items():
            since = now_iso()
            status, body = self.api(
                "POST", f"/api/admin/providers/models/{catalog_id}/test"
            )
            log = self.logs(since)
            self.evidence(
                f"check2-model-test-{model_id}",
                {"status": status, "body": body, "standin_log": log},
            )
            self.check(
                f"model-row Test {model_id} OK",
                status == 200 and body.get("ok") is True,
                f"{body.get('error') if isinstance(body, dict) else body}",
            )

    # user defaults --------------------------------------------------------

    def set_user_defaults(self) -> None:
        if "settings_backup" not in self.state:
            self.state["settings_backup"] = self.psql(
                f"SELECT COALESCE(settings::text, 'null') FROM users WHERE id = '{self.admin_id()}';"
            ).strip()
            self.save()
        status, body = self.api(
            "PATCH",
            "/api/settings/preferences",
            {
                "default_model": CHAT,
                "default_auxiliary_model": CHAT,
                "default_embedding_model": EMBEDDING,
            },
        )
        if status != 200:
            raise GateError(f"preferences: {status} {body}")
        # No preference field for the reranker; dispatch reads the key.
        self.psql(
            "UPDATE users SET settings = COALESCE(settings, '{}'::jsonb) || "
            f"'{{\"default_rerank_model\": \"{RERANK}\"}}'::jsonb WHERE id = '{self.admin_id()}';"
        )
        print(
            "  test user defaults:",
            self.psql(
                f"SELECT settings::text FROM users WHERE id = '{self.admin_id()}';"
            ).strip(),
        )

    def restore_user_defaults(self) -> None:
        backup = self.state.get("settings_backup")
        if backup is None:
            return
        value = (
            "NULL" if backup == "null" else "'" + backup.replace("'", "''") + "'::jsonb"
        )
        self.psql(
            f"UPDATE users SET settings = {value} WHERE id = '{self.admin_id()}';"
        )
        now = self.psql(
            f"SELECT COALESCE(settings::text, 'null') FROM users WHERE id = '{self.admin_id()}';"
        ).strip()
        if self.check("test user settings restored", now == backup, now):
            self.state.pop("settings_backup")
            self.save()

    # jobs and sessions ----------------------------------------------------

    def tool_groups(self) -> dict[str, list[str]]:
        import yaml

        groups: set[str] = set()
        for path in (
            "config/expert_base.yaml",
            "config/overlays/worker.yaml",
            "config/overlays/session.yaml",
        ):
            groups.update(
                (yaml.safe_load((ROOT / path).read_text()) or {}).get("tools", {})
            )
        tools = {name: [] for name in groups}
        tools.update(
            workspace=["read_file"],
            core=["next_phase_todos", "todo_complete", "job_complete"],
        )
        return tools

    def arm(self, run: str, scenario: str, responses: int) -> None:
        status, body = self.control(
            "POST",
            f"/control/scenarios/{run}/arm",
            {"scenario": scenario, "required_responses": responses},
        )
        if (
            status not in (200, 201)
            and body.get("error", {}).get("type") != "scenario_already_armed"
        ):
            raise GateError(f"arm {run}: {status} {body}")

    def run_job(
        self, tag: str, *, expect_fail: bool = False, timeout: int = 600
    ) -> dict[str, Any]:
        run = f"pg-{tag}-{int(time.time()) % 100000}"
        self.arm(run, "worker-job", 100)
        expert = {
            "runtime": {
                "adapter": "srw/v1",
                "config": {
                    "config_name": "worker_base",
                    "config": {
                        "llm": {"model": CHAT, "temperature": 0.0},
                        "auxiliary": {"model": CHAT, "enabled": True},
                        "memory": {"enabled": True, "observer_interval": 1},
                        "tools": self.tool_groups(),
                        "limits": {"max_tool_calls_per_job": 50},
                        "autonomy": "full",
                        "instruction_files": [],
                        "verification": {"enabled": False},
                        "curator": {"enabled": False},
                        "scholar": {"enabled": False},
                        "delegation": {"enabled": False},
                    },
                    "prompts": {
                        "persona": "Use only the configured deterministic test provider."
                    },
                },
            }
        }
        since = now_iso()
        status, job = self.api(
            "POST",
            "/api/jobs",
            {
                "description": f"E2E-{run} pin gate {tag}: finish the hermetic worker job.",
                "expert": {"inline": expert},
                # The worker-job script reads files: a virtual workspace has
                # file tools without a workspace pod.
                "workspace": {"template": {"inline": {"backend": "virtual"}}},
                "user_id": self.admin_id(),
            },
        )
        if status not in (200, 201):
            raise GateError(f"create job: {status} {job}")
        job_id = job["id"]
        self.state.setdefault("jobs", {})[tag] = job_id
        self.save()
        print(f"  job {job_id} ({run})")
        ips: dict[str, str] = {}
        deadline = time.time() + timeout
        current: dict[str, Any] = {}
        while time.time() < deadline:
            ips.update({ip: n for ip, n in self.pod_ips().items() if "agent" in n})
            _, current = self.api("GET", f"/api/jobs/{job_id}")
            if current.get("status") in (
                "completed",
                "failed",
                "cancelled",
                "pending_review",
                "paused",
            ):
                break
            time.sleep(10)
        time.sleep(3)
        log = self.logs(since)
        _, state = self.control("GET", f"/control/scenarios/{run}")
        record = {
            "job_id": job_id,
            "run": run,
            "status": current.get("status"),
            "error": current.get("error_message") or current.get("error_details"),
            "job": current,
            "agent_pod_ips": ips,
            "standin_log": log,
            "provider_state": state,
        }
        self.evidence(f"{tag}-job", record)
        print(
            f"  job status {current.get('status')} error={str(record['error'])[:300]!r}"
        )
        return record

    def summarize_routes(
        self, log: list[dict[str, Any]], ips: dict[str, str]
    ) -> dict[str, Any]:
        summary: dict[str, dict[str, int]] = {}
        for x in self.requests_in(log):
            who = ips.get(x.get("client_ip"), x.get("client_ip"))
            summary.setdefault(who, {}).setdefault(x["path"], 0)
            summary[who][x["path"]] += 1
        return summary

    def open_session(self, tag: str) -> str:
        run = self.state.setdefault(
            "session_run", f"pg-sess-{int(time.time()) % 100000}"
        )
        self.save()
        self.arm(run, "reply", 20)
        status, body = self.api(
            "POST",
            "/api/persistent/threads",
            {
                "title": f"E2E-{run} pin gate session",
                "config_name": "session_base",
                "model": CHAT,
                "project_ids": [],
                "datasource_ids": [],
                "use_datasource_defaults": False,
                "workspace": None,
            },
        )
        if status not in (200, 201):
            raise GateError(f"create session: {status} {body}")
        thread_id = body.get("thread_id") or body.get("id")
        self.state["session_id"] = thread_id
        self.save()
        print(f"  session {thread_id} ({run})")
        return thread_id

    def session_turn(
        self, tag: str, *, expect_reply: bool = True, timeout: int = 240
    ) -> dict[str, Any]:
        thread_id, run = self.state["session_id"], self.state["session_run"]
        self.arm(run, "reply", 20)
        n = self.state.get("turns", 0) + 1
        self.state["turns"] = n
        self.save()
        since = now_iso()
        status, body = self.api(
            "POST",
            f"/api/persistent/threads/{thread_id}/input",
            {"content": f"E2E-{run} pin gate turn {n} ({tag})"},
        )
        if status not in (200, 201, 202):
            raise GateError(f"session input: {status} {body}")
        ips: dict[str, str] = {}
        deadline = time.time() + timeout
        replies, errors, rows = 0, [], []
        while time.time() < deadline:
            ips.update({ip: n for ip, n in self.pod_ips().items() if "agent" in n})
            _, msgs = self.api("GET", f"/api/persistent/threads/{thread_id}/messages")
            rows = msgs.get("messages", []) if isinstance(msgs, dict) else []
            replies = sum(
                1
                for r in rows
                if r.get("role") == "ai" and r.get("content") == f"E2E_REPLY:{run}"
            )
            errors = [r for r in rows if "TLS certificate changed" in json.dumps(r)]
            if (expect_reply and replies >= n - self.state.get("failed_turns", 0)) or (
                not expect_reply and errors
            ):
                break
            time.sleep(8)
        time.sleep(3)
        log = self.logs(since)
        if not expect_reply:
            self.state["failed_turns"] = self.state.get("failed_turns", 0) + 1
            self.save()
        record = {
            "thread_id": thread_id,
            "turn": n,
            "replies": replies,
            "errors": errors,
            "agent_pod_ips": ips,
            "standin_log": log,
            "messages_tail": rows[-6:],
        }
        self.evidence(f"{tag}-session-turn{n}", record)
        return record

    def check3(self) -> None:
        self.set_user_defaults()
        record = self.run_job("check3")
        routes = self.summarize_routes(record["standin_log"], record["agent_pod_ips"])
        print("  routes by client:", json.dumps(routes))
        agent = {k: v for k, v in routes.items() if "agent" in str(k)}
        paths = {p for v in agent.values() for p in v}
        self.check(
            "job completed", record["status"] == "completed", str(record["status"])
        )
        for path in ("/v1/chat/completions", "/v1/embeddings", "/v1/rerank"):
            self.check(f"job agent pod called {path} through the pin", path in paths)
        self.open_session("check3")
        turn = self.session_turn("check3")
        routes = self.summarize_routes(turn["standin_log"], turn["agent_pod_ips"])
        print("  session routes by client:", json.dumps(routes))
        self.check("Session turn answered", turn["replies"] >= 1)
        stateless = {p for k, v in routes.items() if "stateless" in str(k) for p in v}
        self.check(
            "stateless executor called /v1/chat/completions through the pin",
            "/v1/chat/completions" in stateless,
        )
        # A Session's memory embedding and reranker come from the system
        # defaults, not the user's preferences (the Session dispatch passes no
        # user settings), so they reach the stand-in only when the system
        # defaults name its models. Reported, not asserted.
        print(
            "  (stateless executor: embeddings "
            f"{'/v1/embeddings' in stateless}, rerank {'/v1/rerank' in stateless})"
        )

    def check4(self) -> None:
        leaf, swap = self.fingerprint("leaf"), self.fingerprint("swap")
        if self.state.get("cert") != "swap":
            self.switch("swap")
        record = self.run_job("check4", timeout=420)
        error = str(record.get("error") or "")
        m = PIN_SENTENCE.search(error) or PIN_SENTENCE.search(json.dumps(record["job"]))
        self.check("job failed", record["status"] == "failed", str(record["status"]))
        self.check(
            "job error is the pin sentence with both fingerprints",
            bool(
                m
                and m["pinned"] == leaf
                and m["presented"] == swap
                and m["port"] == str(PORT)
            ),
            error[:200],
        )
        log = record["standin_log"]
        failed = [x for x in log if x.get("event") == "handshake_failed"]
        print(
            f"  stand-in: {len(failed)} failed handshakes, {len(self.requests_in(log))} requests"
        )
        self.check("stand-in logged failed handshakes", bool(failed))
        self.check("stand-in logged no request", not self.requests_in(log))
        result = self.test("check4")
        tls = result.get("tls") or {}
        self.check(
            "Test shows certificate changed",
            result.get("ok") is False
            and tls.get("pin_matches") is False
            and (tls.get("certificate") or {}).get("sha256") == swap
            and tls.get("can_trust") is True,
        )

    def check5(self) -> None:
        leaf, swap = self.fingerprint("leaf"), self.fingerprint("swap")
        status, row = self.trust("swap", "check5")
        self.check(
            "re-trust stores the new pin",
            status == 200 and (row.get("tls_pin") or {}).get("sha256") == swap,
            f"http {status}",
        )
        events = self.events("endpoint_certificate_retrusted")
        self.evidence("check5-events", events)
        self.check(
            "endpoint_certificate_retrusted names the previous fingerprint",
            any(
                f"previous={leaf}" in str(e.get("detail"))
                and swap in str(e.get("detail"))
                for e in events
            ),
        )
        record = self.run_job("check5")
        self.check(
            "job completes after re-trust",
            record["status"] == "completed",
            str(record["status"]),
        )

    def check6(self) -> None:
        if self.state.get("cert") != "expired":
            self.switch("expired")
        result = self.test("check6-before")
        self.check(
            "Test shows certificate changed (expired leaf)",
            (result.get("tls") or {}).get("pin_matches") is False,
        )
        status, row = self.trust("expired", "check6")
        pin = row.get("tls_pin") or {}
        self.check(
            "trusting the expired certificate works",
            status == 200 and pin.get("sha256") == self.fingerprint("expired"),
            f"http {status} {row}",
        )
        self.check("the pin is marked expired", pin.get("expired") is True, str(pin))
        result = self.test("check6-after")
        self.check("Test is OK through the expired pin", result.get("ok") is True)
        record = self.run_job("check6")
        self.check(
            "job completes on the expired pin",
            record["status"] == "completed",
            str(record["status"]),
        )

    def check7(self) -> None:
        if self.state.get("cert") != "good":
            self.switch("good")
        result = self.test("check7-before")
        tls = result.get("tls") or {}
        self.check(
            "Test: certificate changed, verifies normally, no Trust",
            result.get("ok") is False
            and tls.get("pin_matches") is False
            and tls.get("publicly_valid") is True
            and tls.get("can_trust") is False,
            str(
                {k: tls.get(k) for k in ("pin_matches", "publicly_valid", "can_trust")}
            ),
        )
        self.check("no request was sent", not self.requests_in(result["_log"]))
        status, body = self.trust("good", "check7")
        self.check(
            "Trust is refused for a certificate that verifies normally",
            status == 400 and "verifies normally" in json.dumps(body),
            f"http {status} {body}",
        )
        status, row = self.api(
            "DELETE", f"/api/admin/providers/endpoints/{self.endpoint_id()}/tls-pin"
        )
        self.evidence("check7-remove", {"status": status, "body": row})
        self.check(
            "Remove pin", status == 200 and row.get("tls_pin") is None, f"http {status}"
        )
        events = self.events("endpoint_certificate_pin_removed")
        self.evidence("check7-events", events)
        self.check("endpoint_certificate_pin_removed recorded", bool(events))
        result = self.test("check7-after")
        self.check(
            "Test passes with normal verification",
            result.get("ok") is True and result.get("tls") is None,
            str(result.get("tls")),
        )

    def check8(self) -> None:
        if self.state.get("cert") != "leaf":
            self.switch("leaf")
        status, row = self.trust("leaf", "check8")
        if status != 200:
            raise GateError(f"trust leaf for check 8: {status} {row}")
        before = self.endpoint_row()
        events_before = self.all_pin_events()
        status, patched = self.api(
            "PATCH",
            f"/api/admin/providers/endpoints/{self.endpoint_id()}",
            {"base_url": self.base_url(SECOND_PORT)},
        )
        after = self.endpoint_row()
        self.evidence(
            "check8-edit",
            {
                "before": before,
                "patch_status": status,
                "patch_response": patched,
                "after": after,
            },
        )
        self.check(
            "edit to the second port saved",
            status == 200 and after["base_url"] == self.base_url(SECOND_PORT),
        )
        self.check(
            "PATCH response carries the pin",
            (patched.get("tls_pin") or {}) == before["tls_pin"],
        )
        self.check(
            "tls_pin unchanged in the list",
            after.get("tls_pin") == before.get("tls_pin"),
            json.dumps(after.get("tls_pin")),
        )
        result = self.test("check8")
        self.check(
            "Test is OK through the pin on the second port",
            result.get("ok") is True
            and any(
                x.get("listener") == SECOND_PORT
                for x in self.requests_in(result["_log"])
            ),
        )
        time.sleep(2)
        events_after = self.all_pin_events()
        self.evidence("check8-events", {"before": events_before, "after": events_after})
        self.check(
            "no audit event for the edit",
            len(events_after) == len(events_before),
            f"{len(events_before)} -> {len(events_after)}",
        )

    # -- cleanup ------------------------------------------------------------

    def cleanup(self) -> list[str]:
        """Undo everything the gate made. Every step is attempted, and the
        stand-in goes last whatever failed before it. Returns the errors."""
        errors: list[str] = []

        def attempt(label: str, step: Any) -> bool:
            try:
                step()
            except Exception as error:
                errors.append(f"{label}: {type(error).__name__}: {error}")
                print(f"  cleanup step failed: {label}: {error}")
                return False
            return True

        for model_id, catalog_id in dict(self.state.get("model_ids") or {}).items():
            path = f"/api/admin/providers/models/{catalog_id}"
            step = functools.partial(self.delete_row, f"model {model_id}", path)
            if attempt(f"delete model {model_id}", step):
                self.state["model_ids"].pop(model_id)
                self.save()
        if self.state.get("endpoint_id"):
            path = f"/api/admin/providers/endpoints/{self.endpoint_id()}"
            if attempt(
                "delete endpoint", functools.partial(self.delete_row, "endpoint", path)
            ):
                self.state.pop("endpoint_id")
                self.save()
        attempt("restore test user settings", self.restore_user_defaults)
        attempt("delete gate memories", self.delete_memories)
        attempt("save stand-in logs", self.save_standin_logs)
        errors.extend(self.down())
        if errors:
            print(f"cleanup INCOMPLETE ({len(errors)}): " + "; ".join(errors))
        return errors

    def delete_row(self, what: str, path: str) -> None:
        status, body = self.api("DELETE", path)
        print(f"  delete {what}: http {status}")
        if status not in (200, 204, 404):
            raise GateError(f"delete {what}: {status} {body}")

    def delete_memories(self) -> None:
        # The two fixed memories every extraction stored (see STANDIN).
        removed = self.kubectl(
            "exec",
            "-i",
            "srw-pgvector-0",
            "-c",
            "pgvector",
            "--",
            "psql",
            "-U",
            "srw",
            "-d",
            "srw_vector",
            "-At",
            "-v",
            "ON_ERROR_STOP=1",
            stdin="DELETE FROM memories WHERE source = 'observer' AND content IN ("
            "'The pin gate stand-in serves chat, embeddings and rerank over TLS on port 8443.', "
            "'The pin gate checks that every model client verifies the pinned certificate.');\n",
        )
        print(f"  memories: {removed.strip()}")

    def save_standin_logs(self) -> None:
        pods = self.kubectl(
            "get",
            "pod",
            "-l",
            f"app={LABEL}",
            "-o",
            "jsonpath={.items[*].metadata.name}",
        ).split()
        for pod in pods:
            (
                self.work
                / "evidence"
                / f"standin-{self.state.get('cert', 'x')}-{pod}.log"
            ).write_text(self.kubectl("logs", pod))


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument(
        "--confirm", default="", help=f"must be {LOCAL_CONFIRMATION} to change anything"
    )
    p.add_argument("--admin", default="admin@localhost", help="admin and test user")
    p.add_argument(
        "--keep",
        action="store_true",
        help="when a command stops early, leave everything in place (no cleanup)",
    )
    p.add_argument(
        "--workdir", default=str(Path(tempfile.gettempdir()) / "srw-pingate")
    )
    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("plan", help="what the gate does; touches nothing")
    sub.add_parser("up", help="create the stand-in and its certificates")
    c = sub.add_parser("cert", help="switch the stand-in's certificate")
    c.add_argument("name", choices=CERTS)
    sub.add_parser("ca-pem", help="print the test CA for trust.extraCaCertificates")
    sub.add_parser("fingerprints", help="openssl fingerprints of the four certificates")
    lg = sub.add_parser("logs", help="the stand-in's log")
    lg.add_argument("--since", default="", help="RFC3339 time")
    k = sub.add_parser("check", help="run one check")
    k.add_argument("number", choices=["1", "1b", "2", "3", "4", "5", "6", "7", "8"])
    sub.add_parser("session-turn", help="one more Session turn (expects a reply)")
    sub.add_parser(
        "session-turn-fail", help="one Session turn that should fail on the pin"
    )
    sub.add_parser(
        "cleanup", help="delete the catalog rows, restore settings, remove the stand-in"
    )
    sub.add_parser("down", help="remove only the stand-in objects")
    return p


# The commands that, when they stop early, clean up before exiting.
CLEAN_UP_ON_STOP = {"up", "cert", "check", "session-turn", "session-turn-fail"}

MUTATING = {
    "up",
    "cert",
    "check",
    "session-turn",
    "session-turn-fail",
    "cleanup",
    "down",
}


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    current = subprocess.run(
        ["kubectl", "config", "current-context"], capture_output=True, text=True
    ).stdout.strip()
    if args.command in MUTATING and args.confirm != LOCAL_CONFIRMATION:
        print(
            f"refused: {args.command} changes the cluster; add --confirm {LOCAL_CONFIRMATION}",
            file=sys.stderr,
        )
        return 2
    if args.command == "plan":
        print(__doc__)
        print(f"kube context (always forced to {CONTEXT}): current is {current}")
        return 0
    sys.stdout.reconfigure(line_buffering=True)
    gate = Gate(args)
    stopped = True
    try:
        if args.command == "up":
            gate.up()
        elif args.command == "cert":
            gate.switch(args.name)
        elif args.command == "ca-pem":
            print((gate.work / "certs" / "ca.pem").read_text(), end="")
        elif args.command == "fingerprints":
            for cert in CERTS:
                print(f"{cert:8s} {gate.fingerprint(cert)}")
        elif args.command == "logs":
            for line in gate.logs(args.since or None):
                print(json.dumps(line))
        elif args.command == "check":
            print(f"check {args.number}:")
            getattr(gate, f"check{args.number}")()
        elif args.command in ("session-turn", "session-turn-fail"):
            ok = args.command == "session-turn"
            turn = gate.session_turn("manual", expect_reply=ok)
            print(json.dumps({k: turn[k] for k in ("thread_id", "turn", "replies")}))
            print(
                json.dumps(
                    gate.summarize_routes(turn["standin_log"], turn["agent_pod_ips"])
                )
            )
            for e in turn["errors"][-1:]:
                print("  error row:", json.dumps(e)[:600])
        elif args.command == "cleanup":
            if gate.cleanup():
                return 1
        elif args.command == "down":
            if gate.down():
                return 1
        stopped = False
    except GateError as error:
        print(f"  FAIL  stopped: {error}")
        return 1
    finally:
        # Any early stop (GateError, another exception, Ctrl-C) lands here.
        if stopped and args.command in CLEAN_UP_ON_STOP:
            if args.keep:
                print("stopped early; --keep leaves everything in place")
            else:
                print("stopped early; cleaning up (--keep skips this)")
                gate.cleanup()
    if gate.failed:
        print(f"FAILED: {len(gate.failed)}: " + "; ".join(gate.failed))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
