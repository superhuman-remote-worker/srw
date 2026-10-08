"""Safety contract and evaluators of the local C3 git swap gate (never run
here)."""

from __future__ import annotations

import base64
import datetime as dt
import importlib.util
import json
import ssl
import subprocess
import sys
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = ROOT / "scripts" / "k3d-git-swap-gate.py"
_SPEC = importlib.util.spec_from_file_location("k3d_git_swap_gate", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gate
_SPEC.loader.exec_module(gate)

PROGRAMS = {
    "api": gate._API_PROGRAM,
    "lease": gate._LEASE_PROGRAM,
    "ca": gate._CA_PROGRAM,
    "connect": gate._CONNECT_PROGRAM,
    "ws_http": gate._WS_HTTP_PROGRAM,
    "keycloak": gate._KEYCLOAK_PROGRAM,
    "hash": gate._HASH_PROGRAM,
    "scan": gate._SCAN_PROGRAM,
    "gitea": gate._GITEA_PROGRAM,
}
CONNECTOR = "66666666-7777-4888-8999-aaaaaaaaaaaa"
DIGEST = "sha256:" + "ab" * 32
UPSTREAM = "https://github.com/srw-gates/disposable.git"
SWAP_IMAGE = "srw-registry:5000/srw-driver-git-swap:tilt-1@" + DIGEST
TOKEN = "ghp_GateToken0123456789abcdefABCDEFghij"


@pytest.fixture
def no_cluster(monkeypatch):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: pytest.fail("must not touch the cluster")
    )


@pytest.fixture
def token_file(tmp_path):
    path = tmp_path / "upstream.token"
    path.write_text(TOKEN + "\n")
    path.chmod(0o600)
    return path


def _args(token_file, *extra: str):
    args = gate.build_parser().parse_args(
        [
            "--run",
            "--confirm",
            gate.LOCAL_CONFIRMATION,
            "--upstream-url",
            UPSTREAM,
            "--upstream-token-file",
            str(token_file),
            *extra,
        ]
    )
    gate.validate(args)
    return args


@pytest.mark.parametrize(
    "argv",
    [
        ["--context", "k3d-other"],
        ["--namespace", "default"],
        ["--run"],
        ["--run", "--confirm", "yes"],
        ["--confirm", gate.LOCAL_CONFIRMATION],
        ["--gate-id", "d5a-0123456789"],
        ["--gate-id", "c3-xyz"],
        ["--model", "bad model; rm -rf /"],
        ["--user", "Robert'); DROP"],
        ["--upstream-url", "http://github.com/o/r.git"],
        ["--upstream-url", "https://user:pw@github.com/o/r.git"],
        ["--upstream-url", "https://github.com:8443/o/r.git"],
        ["--upstream-url", "https://GitHub.com/o/r.git"],
        ["--upstream-url", "https://github.com/o/r.git/"],
        ["--upstream-url", "https://github.com/o/r.git?x=1"],
        ["--redirect-url", "https://github.com/o/../r"],
        ["--start-timeout", "5"],
        ["--turn-timeout", "99999"],
        # A run needs the operator's disposable repository and its token.
        ["--run", "--confirm", gate.LOCAL_CONFIRMATION],
        ["--run", "--confirm", gate.LOCAL_CONFIRMATION, "--upstream-url", UPSTREAM],
    ],
)
def test_refuses_anything_outside_the_local_disposable_boundary(argv, no_cluster):
    assert gate.main(argv) == 2


def test_a_token_file_others_may_read_is_refused(token_file, no_cluster):
    token_file.chmod(0o644)
    argv = [
        "--run",
        "--confirm",
        gate.LOCAL_CONFIRMATION,
        "--upstream-url",
        UPSTREAM,
        "--upstream-token-file",
        str(token_file),
    ]
    assert gate.main(argv) == 2
    token_file.chmod(0o600)
    token_file.write_text("  \n")
    assert gate.main(argv) == 2


def test_the_forge_defaults_from_the_host(token_file):
    assert _args(token_file).forge == "github"
    args = gate.build_parser().parse_args(
        ["--upstream-url", "https://gitea.example.org/o/r.git"]
    )
    gate.validate(args)
    assert args.forge == "gitea"


def test_dry_run_prints_the_plan_and_the_values_keys(no_cluster, capsys):
    assert gate.main([]) == 0
    out = capsys.readouterr().out
    for phase in (
        "preflight",
        "accounts",
        "startup",
        "push",
        "readonly",
        "refs",
        "leases",
        "lfs",
        "ide",
        "redirect",
        "reused",
        "revoked",
        "cleanup",
    ):
        assert f"- {phase}:" in out
    for key in (
        "connectors.servicePods.enabled: true",
        "connectors.drivers.gitSwap.enabled: true",
        "connectors.drivers.gitSwap.image",
        "connectors.drivers.gitSwap.fallback: token-in-url",
        "orchestrator.connectorLeases.exchangePort: 8088",
    ):
        assert key in out


def test_the_values_keys_are_the_k3d_profile():
    import yaml

    example = yaml.safe_load(
        (ROOT / "deployment/values-local.yaml.example").read_text()
    )
    drivers = example["connectors"]["drivers"]
    assert example["connectors"]["servicePods"]["enabled"] is True
    assert example["connectors"]["servicePods"]["maxInstallation"] >= 3
    assert drivers["gitSwap"]["enabled"] is True
    assert drivers["gitSwap"]["fallback"] == "token-in-url"
    assert drivers["gitSwap"]["image"]["repository"].endswith("srw-driver-git-swap")
    assert example["orchestrator"]["connectorLeases"]["exchangePort"] == 8088


@pytest.mark.parametrize("name", sorted(PROGRAMS))
def test_embedded_programs_compile_and_cap_their_memory(name):
    program = PROGRAMS[name]
    compile(program, name, "exec")
    assert "\ncap_memory()\n" in program


def test_the_memory_cap_is_real():
    probe = gate._POD_MEMORY_CAP + (
        "\ncap_memory(64 << 20)\nimport resource\n"
        "print(resource.getrlimit(resource.RLIMIT_DATA)[0])\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=True
    ).stdout
    assert int(out) != -1 and int(out) > 0


def test_the_served_sets_name_every_c3_module_and_exist():
    orchestrator, agent = gate.SERVED_SETS
    assert agent.component == "agent-stateless"
    assert "src/agent/connectors" in agent.dirs
    assert "src/shared/runtime/core/credential_env.py" in agent.files
    assert "src/orchestrator/services/connector_driver_ca.py" in orchestrator.files
    assert "src/orchestrator/services/connector_drivers" in orchestrator.dirs
    for served in gate.SERVED_SETS:
        for path in served.files:
            assert (ROOT / path).is_file(), path
        for directory in served.dirs:
            assert (ROOT / directory).is_dir(), directory
    for name in gate.MIGRATIONS:
        assert (ROOT / "src/orchestrator/database/migrations/app" / name).is_file()


def test_the_driver_url_is_the_one_bindings_carry():
    from orchestrator.services.connector_service_launch import endpoint_url
    from shared.connectors.git_swap import driver_repository_url, swap_upstream

    endpoint = endpoint_url(
        namespace="srw-connectors",
        connector_id=CONNECTOR,
        digest=DIGEST,
        port=8443,
        scheme="https",
    )
    assert gate.driver_origin("srw-connectors", CONNECTOR, DIGEST) == endpoint
    assert gate.driver_url(
        "srw-connectors", CONNECTOR, DIGEST, UPSTREAM
    ) == driver_repository_url(endpoint, CONNECTOR, swap_upstream(UPSTREAM))
    assert gate.repository_path(UPSTREAM) == "srw-gates/disposable"


def _authority():
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    from orchestrator.services.connector_driver_ca import DriverCertificateAuthority

    key = ec.generate_private_key(ec.SECP256R1())
    now = datetime.now(timezone.utc)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "gate test CA")])
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(1)
        .not_valid_before(now - dt.timedelta(days=1))
        .not_valid_after(now + dt.timedelta(days=30))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), True)
        # What Helm's genCA sets; strict verifiers (OpenSSL 3.5's
        # X509_STRICT) refuse a CA without it.
        .add_extension(
            x509.KeyUsage(True, False, True, False, False, True, False, False, False),
            True,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()), False
        )
        .sign(key, hashes.SHA256())
    )
    return DriverCertificateAuthority.from_pem(
        certificate.public_bytes(serialization.Encoding.PEM),
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ),
    )


def _swap_pod():
    """A swap pod and its Secret as the launch builder makes them, as the
    API shows them."""
    from orchestrator.services.connector_egress import EgressPins, PinnedHost
    from orchestrator.services.connector_service_launch import (
        ServiceLaunchPolicy,
        ServicePodIdentity,
        build_service_launch,
    )
    from shared.connectors.builtin import GIT_SWAP_SPEC

    plan = build_service_launch(
        ServicePodIdentity(
            identity_id="11111111-2222-4333-8444-555555555555",
            connector_id=CONNECTOR,
            driver=GIT_SWAP_SPEC.name,
            digest=DIGEST,
            generation="g",
        ),
        spec=GIT_SWAP_SPEC,
        image=f"srw-registry:5000/srw-driver-git-swap@{DIGEST}",
        entrypoint=["/srw-git-swap"],
        cmd=["serve"],
        config={"upstream": UPSTREAM, "host": "github.com"},
        credentials={"token": TOKEN},
        identity_token="sdi_" + "A" * 49,
        pins=EgressPins(
            hosts=(PinnedHost("github.com", ("140.82.121.3",), (443,)),),
            resolved_at=datetime.now(timezone.utc),
        ),
        policy=ServiceLaunchPolicy(
            namespace="srw-connectors",
            release_namespace="srw",
            shim_image="r/shim@sha256:" + "ef" * 32,
            exchange_host="srw-orchestrator.srw.svc",
            exchange_address="10.43.0.20",
            exchange_port=8088,
            orchestrator_labels={"app.kubernetes.io/component": "orchestrator"},
            driver_ca=_authority(),
        ),
    )
    pod = json.loads(json.dumps(plan.pod))
    pod["status"] = {
        "phase": "Running",
        "initContainerStatuses": [
            {"name": "canary-wait", "state": {"terminated": {"exitCode": 0}}},
            {"name": "install-shim", "state": {"terminated": {"exitCode": 0}}},
        ],
        "containerStatuses": [{"name": "driver", "ready": True}],
    }
    return pod, json.loads(json.dumps(plan.secret))


def test_the_pod_evaluators_accept_what_the_launch_builder_makes():
    pod, secret_doc = _swap_pod()
    assert gate.canary_passed(pod)
    assert gate.pod_ready(pod)
    assert gate.swap_pod_problems(pod, secret_doc, SWAP_IMAGE, [TOKEN]) == []


def test_the_pod_evaluators_refuse_what_the_gate_must_not_accept():
    pod, secret_doc = _swap_pod()
    assert gate.swap_pod_problems(
        pod, secret_doc, "srw-registry:5000/other@sha256:" + "0" * 64, [TOKEN]
    )
    pod, secret_doc = _swap_pod()
    request = json.loads(base64.b64decode(secret_doc["data"]["request.json"]))
    request["credentials"] = {"token": TOKEN}
    secret_doc["data"]["request.json"] = base64.b64encode(
        json.dumps(request).encode()
    ).decode()
    problems = gate.swap_pod_problems(pod, secret_doc, SWAP_IMAGE, [TOKEN])
    assert any("carries credentials" in p for p in problems)
    assert any("upstream token" in p for p in problems)
    pod, secret_doc = _swap_pod()
    del secret_doc["data"]["tls.key"]
    pod["spec"]["containers"][0]["securityContext"] = {}
    problems = gate.swap_pod_problems(pod, secret_doc, SWAP_IMAGE, [TOKEN])
    assert any("capabilities" in p for p in problems)
    assert any("Secret holds" in p for p in problems)
    pod, _ = _swap_pod()
    pod["status"]["containerStatuses"][0]["ready"] = False
    assert not gate.pod_ready(pod)
    pod, _ = _swap_pod()
    pod["spec"]["initContainers"].reverse()
    assert not gate.canary_passed(pod)


def test_the_config_evaluator():
    clean = (
        "[core]\n\trepositoryformatversion = 0\n"
        '[remote "origin"]\n'
        f"\turl = {UPSTREAM}\n"
        "\tfetch = +refs/heads/*:refs/remotes/origin/*\n"
        "[transfer]\n\tcredentialsInUrl = die\n"
    )
    assert gate.config_problems(clean, clean_url=UPSTREAM, tokens=[TOKEN]) == []
    planted = clean.replace(UPSTREAM, f"https://oauth2:{TOKEN}@github.com/x/y.git")
    problems = gate.config_problems(planted, clean_url=UPSTREAM, tokens=[TOKEN])
    assert any("credentials are in a URL" in p for p in problems)
    assert any("token or lease" in p for p in problems)
    no_die = clean.replace("[transfer]\n\tcredentialsInUrl = die\n", "")
    assert gate.config_problems(no_die, clean_url=UPSTREAM, tokens=[TOKEN])


def test_the_push_refusal_evaluator():
    out = (
        "To https://github.com/srw-gates/disposable.git\n"
        " ! [remote rejected] srw-gate-c3-0123456789-tag -> "
        "srw-gate-c3-0123456789-tag (only branches (refs/heads/*) may be pushed "
        "through SRW's git swap driver)\n"
    )
    assert gate.push_refused(out, "only branches (refs/heads/*) may be pushed")
    assert not gate.push_refused(out, "deleting a ref is not allowed")
    assert not gate.push_refused("error: failed to push", "only branches")


def test_secrets_reach_the_cluster_only_on_stdin(monkeypatch, token_file):
    seen: list[tuple[list[str], str | None, dict | None]] = []

    def fake_run(argv, **kwargs):
        seen.append((argv, kwargs.get("input"), kwargs.get("env")))
        if "-c" in argv and gate._LEASE_PROGRAM in argv:
            body = json.dumps({"token": "scl_" + "L" * 49})
        elif "-tAc" in argv:
            body = json.dumps(
                {"id": "00000000-0000-4000-8000-000000000001", "image_digest": DIGEST}
            )
        elif "get" in argv and "pods" in argv:
            body = json.dumps(
                {
                    "items": [
                        {"metadata": {"name": "ws-0"}, "status": {"phase": "Running"}}
                    ]
                }
            )
        else:
            body = json.dumps([{"status": 401}])
        return subprocess.CompletedProcess(argv, 0, stdout=body + "\n", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    run = gate.GitSwapGate(_args(token_file, "--password", "s3cret-pw"))
    run.threads["one"] = "00000000-0000-4000-8000-0000000000aa"
    run.connectors["rw"] = CONNECTOR
    run.namespace = "srw-connectors"
    lease = run.lease_token("rw", "one")
    run.ws_http(
        "one", [{"method": "GET", "url": "https://x", "lease": lease, "ca": "/c"}]
    )
    run.upstream_git("ls-remote")
    assert seen
    for argv, _stdin, env in seen:
        joined = " ".join(argv)
        assert "s3cret-pw" not in joined and lease not in joined and TOKEN not in joined
        assert TOKEN not in json.dumps(env or {})
    # The lease travels on stdin; the forge token only through the askpass
    # program, which reads the file.
    assert any(lease in (stdin or "") for _argv, stdin, _env in seen)
    git_call = next(argv for argv, _stdin, env in seen if env and "GIT_ASKPASS" in env)
    assert "https://oauth2@github.com/srw-gates/disposable.git" in git_call
    assert gate._scrub(f"x {lease} y") == "x <redacted> y"
    assert gate._scrub(f"x {TOKEN} y") == "x <redacted> y"
    assert gate._scrub(f"x {run.fake} y") == "x <redacted> y"


def test_the_workspace_program_trusts_the_ca_and_sends_the_lease_as_basic(tmp_path):
    """The in-workspace HTTPS program, run here against a TLS server whose
    certificate SRW's own CA code signed: it verifies it with the CA file,
    presents the lease in Basic auth and returns statuses and headers."""
    authority = _authority()
    certificate, key = authority.issue(["localhost"])
    (tmp_path / "ca.pem").write_text(authority.certificate_pem)
    (tmp_path / "leaf.pem").write_text(certificate)
    (tmp_path / "leaf.key").write_text(key)
    seen: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            seen.append(self.headers.get("Authorization", ""))
            self.send_response(401)
            self.send_header("WWW-Authenticate", 'Basic realm="SRW git swap driver"')
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"a live lease of this connector is required\n")

    server = HTTPServer(("127.0.0.1", 0), Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(tmp_path / "leaf.pem", tmp_path / "leaf.key")
    server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        port = server.server_address[1]
        lease = "scl_" + "Q" * 49
        out = subprocess.run(
            [sys.executable, "-c", gate._WS_HTTP_PROGRAM],
            input=json.dumps(
                {
                    "calls": [
                        {
                            "method": "GET",
                            "url": f"https://localhost:{port}/x/info/refs",
                            "lease": lease,
                            "ca": str(tmp_path / "ca.pem"),
                        },
                        {
                            "method": "GET",
                            "url": f"https://127.0.0.1:{port}/x",
                            "ca": str(tmp_path / "ca.pem"),
                        },
                    ]
                }
            )
            + "\n",
            capture_output=True,
            text=True,
            check=True,
            timeout=60,
        ).stdout
    finally:
        server.shutdown()
    first, second = json.loads(out.splitlines()[-1])
    assert first["status"] == 401, first
    assert first["www_authenticate"].startswith("Basic")
    assert (
        seen[0] == "Basic " + base64.b64encode(f"srw-lease:{lease}".encode()).decode()
    )
    # A name the certificate does not carry is refused, never trusted.
    assert second["status"] == 0 and "CERTIFICATE" in second["error"].upper()


def test_the_default_deny_probe_verdict_is_its_last_rounds():
    raced = "\n".join(["canary=open"] * 3 + ["canary=closed"] * 9)
    assert gate.parse_denyprobe(raced) is True
    unenforced = "\n".join(["canary=closed"] * 2 + ["canary=open"] * 10)
    assert gate.parse_denyprobe(unenforced) is False
    with pytest.raises(gate.GateError):
        gate.parse_denyprobe("canary=closed\n" * 3)


def test_the_node_must_lie_in_the_refused_ranges():
    assert gate.node_refused("172.18.0.2", "172.16.0.0/12,10.42.0.0/16")
    assert not gate.node_refused("172.18.0.2", "10.0.50.0/24")
    assert not gate.node_refused("", "172.16.0.0/12")


def test_the_scan_finds_a_secret_and_names_only_its_path(tmp_path):
    (tmp_path / "clean.txt").write_text("nothing here")
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested" / "config").write_text(f"url = https://oauth2:{TOKEN}@h/x")
    out = subprocess.run(
        [sys.executable, "-c", gate._SCAN_PROGRAM],
        input=json.dumps({"secrets": [TOKEN], "roots": [str(tmp_path)]}) + "\n",
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    result = json.loads(out.splitlines()[-1])
    assert result["found"] == [str(tmp_path / "nested" / "config")]
    assert TOKEN not in out


def test_the_clean_remote_is_the_agents():
    from shared.connectors.git_swap import swap_upstream

    for url in (UPSTREAM, UPSTREAM.removesuffix(".git"), "https://github.com/o/r"):
        assert gate.clean_remote(url) == swap_upstream(url).remote


def test_the_private_url_must_be_served_by_shape_and_falls_back(token_file):
    from shared.connectors.git_swap import swap_upstream

    args = _args(token_file)
    assert args.private_url == gate.DEFAULT_PRIVATE_URL
    # The driver would serve its shape: only the per-delivery egress check
    # (a cluster service address) can put it on the fallback.
    assert swap_upstream(args.private_url).host.endswith(".svc.cluster.local")
    with pytest.raises(gate.SafetyError):
        _args(token_file, "--private-url", "https://git.corp/o/../r")


def test_the_gate_proves_the_fallback_and_measures_the_cold_start():
    assert any(line.startswith("fallback:") for line in gate.PLAN)
    assert "S1" in next(line for line in gate.PLAN if line.startswith("startup:"))
    # Below the agent's own wait for a first clone (reconcile + start timeout).
    assert gate.COLD_START_BUDGET < 15 + 180
    orchestrator, agent = gate.SERVED_SETS
    assert "src/orchestrator/services/connector_git_swap_delivery.py" in (
        orchestrator.files
    )
    assert "src/agent/managers/git_manager.py" in agent.files


# ---------------------------------------------------------------------------
# --self-hosted-upstream (the C3 re-review: a self-contained gate)
# ---------------------------------------------------------------------------

GATE_ID = "c3-0123456789"
LAN_IP = "192.168.178.23"
IP_ADDR = "\n".join(
    [
        "1: lo    inet 127.0.0.1/8 scope host lo\\       valid_lft forever",
        "2: docker0    inet 172.17.0.1/16 brd 172.17.255.255 scope global docker0",
        "3: br-1a2b3c4d    inet 172.18.0.1/16 scope global br-1a2b3c4d",
        "4: tailscale0    inet 100.101.102.103/32 scope global tailscale0",
        "5: wg0    inet 10.8.0.2/24 scope global wg0",
        "6: enp0s31f6    inet 10.42.7.9/24 scope global enp0s31f6",
        "7: enp1s0    inet 172.20.1.5/24 scope global enp1s0",
        "8: eth9    inet 81.2.69.160/24 scope global eth9",
        "9: wlp3s0    inet 192.168.178.23/24 brd 192.168.178.255 scope global "
        "dynamic noprefixroute wlp3s0\\       valid_lft 86000sec",
        "10: wlp3s0    inet 192.168.178.99/24 scope global secondary wlp3s0",
    ]
)
REFUSED = ["10.42.0.0/16", "10.43.0.0/16", "172.16.0.0/12", "169.254.0.0/16"]


def _self_hosted(*extra: str):
    args = gate.build_parser().parse_args(
        [
            "--run",
            "--confirm",
            gate.LOCAL_CONFIRMATION,
            "--self-hosted-upstream",
            "--gate-id",
            GATE_ID,
            *extra,
        ]
    )
    gate.validate(args)
    return args


def test_the_lan_address_is_private_and_outside_every_refused_range():
    candidates = gate.lan_ipv4_candidates(IP_ADDR)
    # Bridges, VPN tunnels, loopback and public addresses are never candidates.
    assert candidates == [
        ("enp0s31f6", "10.42.7.9"),
        ("enp1s0", "172.20.1.5"),
        ("wlp3s0", LAN_IP),
        ("wlp3s0", "192.168.178.99"),
    ]
    # The cluster's ranges and refusedCidrs (the k3d docker networks) are
    # skipped: a driver pod may never reach them.
    assert gate.choose_lan_ip(candidates, REFUSED) == LAN_IP
    assert gate.choose_lan_ip(candidates[:2], REFUSED) is None
    assert gate.choose_lan_ip([], REFUSED) is None
    assert gate.choose_lan_ip(candidates, ["not a cidr"]) == "10.42.7.9"


def test_the_self_hosted_url_has_the_shape_the_driver_serves():
    from shared.connectors.git_swap import swap_upstream

    host = gate.sslip_host(GATE_ID, LAN_IP)
    assert host == f"{GATE_ID}.192-168-178-23.sslip.io"
    url = f"https://{host}/srw-{GATE_ID}/{GATE_ID}-repo.git"
    assert gate._UPSTREAM_RE.fullmatch(url)
    assert swap_upstream(url).host == host


def test_the_gate_ca_signs_a_certificate_strict_verifiers_and_srw_accept(tmp_path):
    import socket

    from orchestrator.services.connector_git_swap_delivery import (
        validate_upstream_ca,
    )

    host = gate.sslip_host(GATE_ID, LAN_IP)
    ca_pem, cert_pem, key_pem = gate.make_gate_ca(host)
    # The connector's upstream_ca takes it as it is.
    assert validate_upstream_ca(ca_pem).strip() == ca_pem.strip()
    (tmp_path / "ca.pem").write_text(ca_pem)
    (tmp_path / "leaf.pem").write_text(cert_pem)
    (tmp_path / "leaf.key").write_text(key_pem)
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain(tmp_path / "leaf.pem", tmp_path / "leaf.key")
    listener = socket.create_server(("127.0.0.1", 0))
    port = listener.getsockname()[1]

    def serve() -> None:
        for _ in range(2):
            conn, _addr = listener.accept()
            try:
                with server_context.wrap_socket(conn, server_side=True) as tls:
                    tls.recv(1)
            except (ssl.SSLError, OSError):
                pass

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    client = ssl.create_default_context(cafile=str(tmp_path / "ca.pem"))
    client.verify_flags |= ssl.VERIFY_X509_STRICT
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=10) as raw:
            with client.wrap_socket(raw, server_hostname=host) as tls:
                assert tls.getpeercert()["subjectAltName"] == (("DNS", host),)
        with socket.create_connection(("127.0.0.1", port), timeout=10) as raw:
            with pytest.raises(ssl.SSLCertVerificationError):
                client.wrap_socket(raw, server_hostname="other.sslip.io")
    finally:
        listener.close()
        thread.join(timeout=10)


def test_the_ingress_publishes_gitea_at_the_name_with_its_secret():
    ca_pem, cert_pem, key_pem = gate.make_gate_ca("h.example")
    secret_object, ingress = gate.forge_objects(
        gate_id=GATE_ID,
        namespace="srw",
        host="h.example",
        service="srw-gitea",
        port=3000,
        cert_pem=cert_pem,
        key_pem=key_pem,
    )
    assert secret_object["type"] == "kubernetes.io/tls"
    assert base64.b64decode(secret_object["data"]["tls.crt"]).decode() == cert_pem
    assert ca_pem not in json.dumps(secret_object)
    spec = ingress["spec"]
    assert spec["ingressClassName"] == "traefik"
    assert spec["tls"] == [
        {"hosts": ["h.example"], "secretName": secret_object["metadata"]["name"]}
    ]
    (rule,) = spec["rules"]
    assert rule["host"] == "h.example"
    assert rule["http"]["paths"][0]["backend"] == {
        "service": {"name": "srw-gitea", "port": {"number": 3000}}
    }
    for made in (secret_object, ingress):
        assert made["metadata"]["labels"] == {gate.GATE_LABEL: GATE_ID}
        assert made["metadata"]["namespace"] == "srw"


def test_the_reach_probe_opens_one_address_and_port_and_is_a_bind_time_pod(
    tmp_path,
):
    policy, pod = gate.reach_probe_objects(
        gate_id=GATE_ID, namespace="srw-connectors", address=LAN_IP
    )
    assert policy["spec"]["podSelector"]["matchLabels"] == pod["metadata"]["labels"]
    assert policy["spec"]["egress"] == [
        {
            "to": [{"ipBlock": {"cidr": f"{LAN_IP}/32"}}],
            "ports": [{"protocol": "TCP", "port": 443}],
        }
    ]
    assert pod["metadata"]["labels"][gate.GATE_LABEL] == GATE_ID
    # A deadline: the bind-time quota, never a service pod's room.
    assert pod["spec"]["activeDeadlineSeconds"] > 0
    assert pod["spec"]["automountServiceAccountToken"] is False
    (container,) = pod["spec"]["containers"]
    assert container["command"][-1] == LAN_IP

    # The script under a fake busybox wget: any HTTP answer is a reach.
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "sleep").write_text("#!/bin/sh\nexit 0\n")
    for answer, reached in (
        ("echo 'wget: server returned error: HTTP/1.0 400 Bad Request' >&2", True),
        ("echo 'wget: can not connect to remote host: Connection refused' >&2", False),
    ):
        (bin_dir / "wget").write_text(f"#!/bin/sh\n{answer}\nexit 1\n")
        for tool in ("sleep", "wget"):
            (bin_dir / tool).chmod(0o755)
        out = subprocess.run(
            ["sh", "-c", gate._REACH_SCRIPT, "reach", LAN_IP],
            capture_output=True,
            text=True,
            timeout=30,
            env={"PATH": f"{bin_dir}:/usr/bin:/bin"},
        ).stdout
        assert ("reach=ok" in out) is reached, out
        if not reached:
            assert "reach=failed" in out and "Connection refused" in out


@pytest.mark.parametrize(
    "extra",
    [
        ["--upstream-url", UPSTREAM],
        ["--forge", "github"],
    ],
)
def test_the_self_hosted_mode_refuses_an_operator_upstream(extra, no_cluster):
    assert gate.main(["--self-hosted-upstream", *extra]) == 2


def test_the_self_hosted_mode_needs_no_repository_or_token(no_cluster, capsys):
    args = _self_hosted()
    assert args.forge == "gitea"
    run = gate.GitSwapGate(args)
    assert run.forge is not None and run.token == ""
    assert run.forge.user == f"srw-{GATE_ID}"
    assert gate.main(["--self-hosted-upstream"]) == 0
    out = capsys.readouterr().out
    assert "- self-hosted (--self-hosted-upstream" in out
    for need in ("privateTiers: [home-allowed]", "firewalld", "sslip.io", "443"):
        assert need in out


def _fake_gitea():
    """A Gitea API with the calls the gate's program makes, checking each
    one's credential: the admin's, the new user's password, its token."""
    state: dict = {"users": set(), "repos": set(), "calls": []}
    admin = "Basic " + base64.b64encode(b"srw:admin-pw").decode()
    token = "f" * 40

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def answer(self, status, body=None):
            raw = json.dumps(body).encode() if body is not None else b""
            self.send_response(status)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def body(self):
            length = int(self.headers.get("Content-Length") or 0)
            return json.loads(self.rfile.read(length) or b"null")

        def route(self, method):
            auth = self.headers.get("Authorization", "")
            path = self.path.removeprefix("/api/v1")
            state["calls"].append((method, path))
            body = self.body() if method in ("POST", "PATCH") else None
            if method == "POST" and path == "/admin/users" and auth == admin:
                state["users"].add(body["username"])
                state["password"] = body["password"]
                return self.answer(201, {"login": body["username"]})
            if method == "POST" and path.endswith("/tokens"):
                user = path.split("/")[2]
                mine = base64.b64encode(f"{user}:{state['password']}".encode())
                if auth == "Basic " + mine.decode():
                    state["scopes"] = body["scopes"]
                    return self.answer(201, {"sha1": token})
                return self.answer(401)
            if method == "POST" and path == "/user/repos" and auth == f"token {token}":
                state["created"] = state.get("created", []) + [body]
                state["repos"].add(body["name"])
                return self.answer(201, {})
            if method == "PATCH" and auth == f"token {token}":
                old = path.split("/")[3]
                state["repos"].discard(old)
                state["repos"].add(body["name"])
                state["redirect"] = (old, body["name"])
                return self.answer(200, {})
            if (
                method == "DELETE"
                and path.startswith("/admin/users/")
                and auth == admin
            ):
                state["purged"] = path.endswith("?purge=true")
                state["users"].discard(path.split("/")[3].split("?")[0])
                state["repos"].clear()
                return self.answer(204)
            if method == "GET" and path.startswith("/users/") and auth == admin:
                user = path.split("/")[2]
                return self.answer(200 if user in state["users"] else 404, {})
            return self.answer(403)

        def do_GET(self):
            self.route("GET")

        def do_POST(self):
            self.route("POST")

        def do_PATCH(self):
            self.route("PATCH")

        def do_DELETE(self):
            self.route("DELETE")

    return Handler, state, token


def test_the_gitea_program_makes_and_removes_the_fixture():
    handler, state, token = _fake_gitea()
    server = HTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    env = {
        "PATH": "/usr/bin:/bin",
        "GITEA_INTERNAL_URL": f"http://127.0.0.1:{server.server_address[1]}/",
        "GITEA_ADMIN_USER": "srw",
        "GITEA_ADMIN_PASSWORD": "admin-pw",
    }

    def program(request):
        out = subprocess.run(
            [sys.executable, "-c", gate._GITEA_PROGRAM],
            input=json.dumps(request) + "\n",
            capture_output=True,
            text=True,
            check=True,
            timeout=60,
            env=env,
        ).stdout
        return json.loads(out.splitlines()[-1])

    user = f"srw-{GATE_ID}"
    try:
        made = program(
            {
                "action": "create",
                "user": user,
                "password": "user-pw-0123456789",
                "token_name": GATE_ID,
                "repo": f"{GATE_ID}-repo",
                "old_repo": f"{GATE_ID}-old",
                "moved_repo": f"{GATE_ID}-moved",
            }
        )
        assert made == {"statuses": gate.GITEA_CREATED, "token": token}
        assert set(state["scopes"]) == {"write:repository", "write:user"}
        assert all(
            body["private"] is True and body["auto_init"] is True
            for body in state["created"]
        )
        assert state["repos"] == {f"{GATE_ID}-repo", f"{GATE_ID}-moved"}
        assert state["redirect"] == (f"{GATE_ID}-old", f"{GATE_ID}-moved")
        assert program({"action": "exists", "user": user}) == {"status": 200}
        assert program({"action": "delete", "user": user}) == {"status": 204}
        assert state["purged"] is True
        assert program({"action": "exists", "user": user}) == {"status": 404}
        # Wrong admin credentials: the first status says so, nothing else runs.
        env["GITEA_ADMIN_PASSWORD"] = "wrong"
        refused = program({"action": "create", "user": user, "password": "x"})
        assert refused == {"statuses": [403]}
    finally:
        server.shutdown()


class _FakeCluster:
    """subprocess.run for the self-hosted preflight and cleanup."""

    def __init__(self, ip_output: str = IP_ADDR) -> None:
        self.ip_output = ip_output
        self.calls: list[tuple[list[str], str | None]] = []
        self.gitea: list[dict] = []
        self.applied: list[dict] = []

    def __call__(self, argv, **kwargs):
        data = kwargs.get("input")
        self.calls.append((list(argv), data))
        out = ""
        if argv[:1] == ["ip"]:
            out = self.ip_output
        elif "printenv" in argv:
            out = "http://srw-gitea:3000"
        elif "apply" in argv:
            self.applied += json.loads(data)["items"]
        elif gate._GITEA_PROGRAM in argv:
            request = json.loads(data)
            self.gitea.append(request)
            out = json.dumps(
                {
                    "create": {"statuses": gate.GITEA_CREATED, "token": "e" * 40},
                    "delete": {"status": 204},
                    "exists": {"status": 404},
                }[request["action"]]
            )
        elif "jsonpath={.status.phase}" in argv:
            out = "Succeeded"
        elif "logs" in argv:
            out = "reach=ok"
        elif "delete" in argv or ("get" in argv and "name" in argv):
            out = ""
        elif not argv[0].endswith("git"):
            pytest.fail(f"unexpected call {argv[:8]}")
        return subprocess.CompletedProcess(argv, 0, stdout=out + "\n", stderr="")


SELF_HOSTED_ENV = {
    "CONNECTOR_SERVICE_PRIVATE_TIERS": "home-allowed",
    "CONNECTOR_SERVICE_CLUSTER_CIDRS": "10.42.0.0/16,10.43.0.0/16",
    "CONNECTOR_SERVICE_REFUSED_CIDRS": "172.16.0.0/12,10.42.0.0/16,169.254.0.0/16",
}


def test_the_self_hosted_preflight_sets_the_upstream_and_cleans_up(monkeypatch):
    cluster = _FakeCluster()
    monkeypatch.setattr(subprocess, "run", cluster)
    run = gate.GitSwapGate(_self_hosted())
    run.namespace = "srw-connectors"
    run.forge.prepare(SELF_HOSTED_ENV)
    host = f"{GATE_ID}.192-168-178-23.sslip.io"
    assert run.upstream == f"https://{host}/srw-{GATE_ID}/{GATE_ID}-repo.git"
    assert run.redirect == f"https://{host}/srw-{GATE_ID}/{GATE_ID}-old.git"
    assert run.urls["rw"] == run.urls["ro"] == run.upstream
    assert run.urls["redirect"] == run.redirect
    assert run.urls["private"] == gate.DEFAULT_PRIVATE_URL
    assert all(ok for _name, ok, _detail in run.report.results), run.report.results
    # The token: a 0600 file in the run's own directory, scrubbed everywhere.
    token_file = Path(run.token_file)
    assert token_file.read_text() == "e" * 40 == run.token
    assert token_file.stat().st_mode & 0o077 == 0
    assert gate._scrub("x " + "e" * 40) == "x <redacted>"
    # The connectors carry the gate's CA, except the never-served private one.
    ca = run.forge.ca_pem
    assert run.connector_config("rw") == {"forge": "gitea", "upstream_ca": ca}
    assert run.connector_config("redirect")["upstream_ca"] == ca
    assert run.connector_config("private") == {"forge": "gitea"}
    # Gitea published at the name, the reach probe in the driver namespace.
    kinds = {(made["kind"], made["metadata"]["namespace"]) for made in cluster.applied}
    assert kinds == {
        ("Secret", "srw"),
        ("Ingress", "srw"),
        ("NetworkPolicy", "srw-connectors"),
        ("Pod", "srw-connectors"),
    }
    ingress = next(made for made in cluster.applied if made["kind"] == "Ingress")
    assert ingress["spec"]["rules"][0]["host"] == host
    (create,) = cluster.gitea
    assert create["user"] == f"srw-{GATE_ID}"
    assert create["password"] == run.forge.password
    # Secrets never on a command line.
    for argv, _data in cluster.calls:
        joined = " ".join(argv)
        assert run.forge.password not in joined and "e" * 40 not in joined
    # git on the workstation trusts the gate's CA.
    run.upstream_git("ls-remote")
    git_call = next(argv for argv, _ in cluster.calls if argv[0].endswith("git"))
    assert f"http.sslCAInfo={run.git_ca_file}" in git_call
    assert f"https://oauth2@{host}/" in " ".join(git_call)

    problems: list[str] = []

    def step(label, action):
        if action() is False:
            problems.append(label)

    scratch = run.forge.scratch
    run.forge.cleanup(step)
    assert problems == []
    assert not Path(scratch).exists()
    assert [request["action"] for request in cluster.gitea] == ["create", "delete"]
    deleted = [argv for argv, _ in cluster.calls if "delete" in argv]
    assert any("pod,networkpolicy" in argv for argv in deleted)
    assert any("ingress,secret" in argv for argv in deleted)
    assert run.forge.residue() == []


def test_the_self_hosted_preflight_says_what_the_host_lacks(monkeypatch):
    cluster = _FakeCluster(ip_output="1: lo    inet 127.0.0.1/8 scope host lo")
    monkeypatch.setattr(subprocess, "run", cluster)
    run = gate.GitSwapGate(_self_hosted())
    run.namespace = "srw-connectors"
    with pytest.raises(gate.GateError, match="LAN"):
        run.forge.prepare(SELF_HOSTED_ENV)
    assert not run.forge.objects_started and not run.forge.user_started
    run = gate.GitSwapGate(_self_hosted())
    with pytest.raises(gate.GateError, match="privateTiers"):
        run.forge.prepare({**SELF_HOSTED_ENV, "CONNECTOR_SERVICE_PRIVATE_TIERS": ""})
    failed = [name for name, ok, _detail in run.report.results if not ok]
    assert failed and "home-allowed" in failed[0]
