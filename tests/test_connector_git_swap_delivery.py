"""The git swap driver's per-delivery decision (C3 review B1, S1, S3).

Turning the driver on must never break a token repository it cannot
serve: the lease step decides per entry (the workspace's reach, the
connector's last pod, the installation's room, the upstream's egress and
TLS, the image) and falls back per entry, visibly, instead of failing the
claim. Test reports the same verdict and probes the upstream's TLS. A
connector may name an upstream CA. The reconciler stops a pod whose driver
reports its upstream unusable at once, wakes for a new binding, and keeps a
git swap pod an hour.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import re
import ssl
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import pytest_asyncio
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from fastapi import HTTPException

from orchestrator.services import connector_credential_leases as leases
from orchestrator.services import connector_driver_ca as driver_ca_module
from orchestrator.services import connector_git_swap_delivery as swaps
from orchestrator.services import connector_service_hosting as hosting
from orchestrator.services import connector_service_images as images
from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.connector_drivers.git_swap import GitSwapDriver
from orchestrator.services.connector_drivers.repository import RepositoryDriver
from orchestrator.services.connector_service_hosting import (
    PodState,
    ServiceHostingReconciler,
    ServiceHostingSettings,
    credential_generation,
)
from orchestrator.services.datasource_config import normalize_repository_config
from shared.connectors.builtin import GIT_SWAP_SPEC, REPOSITORY_SPEC

ROOT = Path(__file__).resolve().parents[1]
CONNECTOR = "0d6f3a52-8b1c-4e8e-9a8f-1f2e3d4c5b6a"
OTHER = "9d6f3a52-8b1c-4e8e-9a8f-1f2e3d4c5b6a"
TOKEN = "ghp_TheForgeToken0123456789abcdefABCDEF"
DIGEST = "sha256:" + "ab" * 32
IMAGE = "ghcr.io/superhuman-remote-worker/srw-driver-git-swap:1.0.0@" + DIGEST
JOB = "00000000-0000-4000-8000-000000000001"


@pytest.fixture(autouse=True)
def _fresh_settings():
    swaps.configure_git_swap_delivery(swaps.GitSwapDeliverySettings())
    yield
    swaps.configure_git_swap_delivery(swaps.GitSwapDeliverySettings())
    images.configure_service_images(images.ServiceImageSettings())
    driver_ca_module.configure_driver_ca(None)


def _self_signed(host: str = "git.corp.example") -> tuple[str, str]:
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, host)])
    now = dt.datetime.now(dt.timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=5))
        .not_valid_after(now + dt.timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(host)]), False)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), True)
        .sign(key, hashes.SHA256())
    )
    return (
        certificate.public_bytes(serialization.Encoding.PEM).decode(),
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ).decode(),
    )


def _candidate(connector: str = CONNECTOR, url: str = "https://github.com/o/r.git"):
    return {
        "type": "repository",
        "name": f"Repo {connector[:4]}",
        "connection_url": url,
        "credentials": {"token": TOKEN},
        "project_read_only": False,
        "datasource_id": connector,
        "config": {"forge": "github"},
        "git_swap": {},
    }


# =============================================================================
# The workspace
# =============================================================================


class FakeConn:
    """Answers fetchrow/fetchval by the first word of a known query."""

    def __init__(self, rows: dict[str, Any] | None = None, values=()):
        self.rows = rows or {}
        self.values = list(values)
        self.queries: list[str] = []

    async def fetchrow(self, query: str, *args: Any):
        self.queries.append(query)
        for key, row in self.rows.items():
            if key in query:
                return row
        return None

    async def fetchval(self, query: str, *args: Any):
        self.queries.append(query)
        return self.values.pop(0) if self.values else None


class TestWorkspaceReach:
    def test_only_a_container_or_a_same_cluster_vm_reaches_the_driver(self):
        assert swaps.workspace_reach_problem("sandbox", "k8s") is None
        assert swaps.workspace_reach_problem("sandbox", None) is None
        assert "static-pool" in swaps.workspace_reach_problem("sandbox", "docker")
        assert "another cluster" in swaps.workspace_reach_problem("vm", None)
        swaps.configure_git_swap_delivery(
            swaps.GitSwapDeliverySettings(vm_on_pod_network=lambda: True)
        )
        assert swaps.workspace_reach_problem("vm", None) is None
        for backend in ("virtual", "none", None):
            assert swaps.workspace_reach_problem(backend, "k8s") is not None

    @pytest.mark.asyncio
    async def test_the_owners_row_says_where_its_workspace_runs(self):
        job = FakeConn(
            {
                "FROM jobs": {
                    "context": json.dumps(
                        {"workspace_container": {"provisioner": "docker"}}
                    ),
                    "config_override": json.dumps(
                        {"workspace": {"backend": "sandbox"}}
                    ),
                }
            }
        )
        problem = await swaps.owner_workspace_problem(job, leases.LeaseOwner.job(JOB))
        assert "static-pool" in problem
        thread = FakeConn(
            {
                "FROM threads": {
                    "metadata": {"config_override": {"workspace": {"backend": "vm"}}}
                }
            }
        )
        problem = await swaps.owner_workspace_problem(
            thread, leases.LeaseOwner.thread(JOB)
        )
        assert "another cluster" in problem
        container = FakeConn(
            {
                "FROM threads": {
                    "metadata": {
                        "config_override": {"workspace": {"backend": "sandbox"}},
                        "workspace_container": {"provisioner": "k8s"},
                    }
                }
            }
        )
        assert (
            await swaps.owner_workspace_problem(
                container, leases.LeaseOwner.thread(JOB)
            )
            is None
        )
        assert "gone" in await swaps.owner_workspace_problem(
            FakeConn(), leases.LeaseOwner.job(JOB)
        )


# =============================================================================
# The connector's pods
# =============================================================================


class TestLaunch:
    @pytest.mark.asyncio
    async def test_a_serving_pod_is_no_problem(self):
        assert await swaps.launch_problem(FakeConn(values=[1]), CONNECTOR) is None

    @pytest.mark.asyncio
    async def test_a_pod_that_could_not_start_is(self):
        conn = FakeConn(
            {
                "revoke_reason = ANY": {
                    "revoke_reason": "upstream_unreachable",
                    "launch_error": "the certificate of git.corp does not\nverify",
                }
            },
            values=[None],
        )
        problem = await swaps.launch_problem(conn, CONNECTOR)
        assert problem == (
            "its driver pod did not start (upstream_unreachable: the certificate "
            "of git.corp does not verify)"
        )
        # Only within the back-off and since the connector last changed.
        assert "launch_backoff" not in conn.queries[1]
        assert "ds.updated_at" in conn.queries[1]

    @pytest.mark.asyncio
    async def test_a_full_installation_is(self):
        swaps.configure_git_swap_delivery(
            swaps.GitSwapDeliverySettings(max_installation=3)
        )
        problem = await swaps.launch_problem(FakeConn(values=[None, 3]), CONNECTOR)
        assert "cap of 3 driver pods" in problem
        assert await swaps.launch_problem(FakeConn(values=[None, 2]), CONNECTOR) is None

    def test_every_stop_that_backs_off_makes_a_connector_unservable(self):
        assert set(swaps.UNSERVABLE_STOPS) == set(hosting._BACKOFF_REASONS)


# =============================================================================
# The upstream
# =============================================================================


@pytest_asyncio.fixture
async def tls_upstream():
    """A TLS server on 127.0.0.1 with a self-signed certificate."""
    certificate, key = _self_signed()
    import tempfile

    with tempfile.TemporaryDirectory() as directory:
        cert_file = Path(directory) / "cert.pem"
        key_file = Path(directory) / "key.pem"
        cert_file.write_text(certificate)
        key_file.write_text(key)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert_file, key_file)

        async def answer(reader, writer):
            writer.close()

        server = await asyncio.start_server(answer, "127.0.0.1", 0, ssl=context)
        port = server.sockets[0].getsockname()[1]
        try:
            yield SimpleNamespace(port=port, certificate=certificate)
        finally:
            server.close()
            await server.wait_closed()


class TestUpstream:
    @pytest.mark.asyncio
    async def test_a_private_ca_does_not_verify_until_the_connector_names_it(
        self, tls_upstream
    ):
        problem = await swaps.probe_upstream_tls(
            "git.corp.example", "127.0.0.1", port=tls_upstream.port, timeout=5
        )
        assert "does not verify against public roots" in problem
        assert "set the connector's upstream CA" in problem
        assert swaps._definite(problem)
        assert (
            await swaps.probe_upstream_tls(
                "git.corp.example",
                "127.0.0.1",
                port=tls_upstream.port,
                ca_pem=tls_upstream.certificate,
            )
            is None
        )
        # The name must match too.
        problem = await swaps.probe_upstream_tls(
            "other.example",
            "127.0.0.1",
            port=tls_upstream.port,
            ca_pem=tls_upstream.certificate,
        )
        assert "does not verify against its upstream CA" in problem

    @pytest.mark.asyncio
    async def test_an_upstream_that_does_not_answer_decides_nothing(self):
        problem = await swaps.probe_upstream_tls(
            "git.corp.example", "127.0.0.1", port=1, timeout=2
        )
        assert "did not complete a TLS handshake" in problem
        assert not swaps._definite(problem)

    @pytest.mark.asyncio
    async def test_the_reconcilers_egress_rule_decides_first(self):
        async def private(host, ipv6):
            return ["10.20.30.40"]

        swaps.configure_git_swap_delivery(
            swaps.GitSwapDeliverySettings(resolver=private)
        )
        problem = await swaps.check_upstream(
            "git.corp.example", ca_pem=None, private_allowed=False
        )
        assert problem.startswith("its driver may not reach the upstream")
        assert "private" in problem

        async def node(host, ipv6):
            return ["172.18.0.2"]

        swaps.configure_git_swap_delivery(
            swaps.GitSwapDeliverySettings(
                resolver=node, refused_cidrs=("172.16.0.0/12",)
            )
        )
        problem = await swaps.check_upstream(
            "git.corp.example", ca_pem=None, private_allowed=True
        )
        assert "refuses" in problem

    @pytest.mark.asyncio
    async def test_a_verdict_is_remembered_per_host_ca_and_tier(self, monkeypatch):
        calls: list[tuple] = []

        async def check(host, *, ca_pem, private_allowed):
            calls.append((host, ca_pem, private_allowed))
            return None

        monkeypatch.setattr(swaps, "check_upstream", check)
        now = [100.0]
        swaps.configure_git_swap_delivery(
            swaps.GitSwapDeliverySettings(verdict_seconds=60, clock=lambda: now[0])
        )
        for _ in range(2):
            await swaps.upstream_verdict("h", ca_pem=None, private_allowed=False)
        await swaps.upstream_verdict("h", ca_pem="CA", private_allowed=False)
        await swaps.upstream_verdict("h", ca_pem=None, private_allowed=True)
        assert len(calls) == 3
        now[0] += 61
        await swaps.upstream_verdict("h", ca_pem=None, private_allowed=False)
        await swaps.upstream_verdict(
            "h", ca_pem=None, private_allowed=False, fresh=True
        )
        assert len(calls) == 5


# =============================================================================
# The decision and the delivery
# =============================================================================


class TestDecision:
    @pytest.mark.asyncio
    async def test_the_installation_comes_first(self, monkeypatch):
        conn = FakeConn()
        owner = leases.LeaseOwner.job(JOB)
        assert "not installed" in await swaps.git_swap_problem(
            conn, _candidate(), connector_id=CONNECTOR, owner=owner
        )
        swaps.configure_git_swap_delivery(swaps.GitSwapDeliverySettings(installed=True))
        images.configure_service_images(
            images.ServiceImageSettings(service_namespace="srw-connectors")
        )
        assert "certificate authority" in await swaps.git_swap_problem(
            conn, _candidate(), connector_id=CONNECTOR, owner=owner
        )

    @pytest.mark.asyncio
    async def test_each_check_in_order_and_the_first_problem_wins(self, monkeypatch):
        swaps.configure_git_swap_delivery(swaps.GitSwapDeliverySettings(installed=True))
        images.configure_service_images(
            images.ServiceImageSettings(service_namespace="srw-connectors")
        )
        driver_ca_module.configure_driver_ca(object())
        asked: list[str] = []

        def check(name, answer):
            async def run(*_args, **_kwargs):
                asked.append(name)
                return answer

            return run

        monkeypatch.setattr(swaps, "owner_workspace_problem", check("workspace", None))
        monkeypatch.setattr(swaps, "launch_problem", check("launch", None))
        monkeypatch.setattr(swaps, "_upstream_problem", check("upstream", None))
        owner = leases.LeaseOwner.job(JOB)
        assert (
            await swaps.git_swap_problem(
                FakeConn(), _candidate(), connector_id=CONNECTOR, owner=owner
            )
            is None
        )
        assert asked == ["workspace", "launch", "upstream"]
        asked.clear()
        monkeypatch.setattr(swaps, "launch_problem", check("launch", "no room"))
        assert (
            await swaps.git_swap_problem(
                FakeConn(), _candidate(), connector_id=CONNECTOR, owner=owner
            )
            == "no room"
        )
        assert asked == ["workspace", "launch"]
        # A URL the driver cannot serve, though a candidate.
        assert "cannot serve it" in await swaps.git_swap_problem(
            FakeConn(),
            _candidate(url="http://gitea:3000/o/r.git"),
            connector_id=CONNECTOR,
            owner=owner,
        )
        # A token the driver would refuse (it masks it in every answer).
        short = {**_candidate(), "credentials": {"token": "short-token"}}
        assert "shorter than 16 characters" in await swaps.git_swap_problem(
            FakeConn(), short, connector_id=CONNECTOR, owner=owner
        )
        proxy = (ROOT / "drivers/git-swap/proxy.go").read_text()
        found = re.search(r"minCredentialLength = (\d+)", proxy)
        assert found and int(found.group(1)) == swaps.MIN_TOKEN_LENGTH


@pytest.fixture
def delivery(monkeypatch, request):
    """deliver_connector_leases with the image, the issue and the decision
    stubbed: ``problems`` maps a connector to its problem."""
    state = SimpleNamespace(problems={}, image_error=None, issued=[], woken=0)

    async def problem(conn, entry, *, connector_id, owner):
        return state.problems.get(connector_id)

    async def bind(conn, *, spec, connector_id, owner):
        if state.image_error is not None:
            raise state.image_error
        return DIGEST

    async def issue(
        _conn, *, owner, connector_id, driver, access, image_digest, ttl_seconds
    ):
        state.issued.append(connector_id)
        return SimpleNamespace(
            id=f"lease-{connector_id[:4]}",
            connector_id=connector_id,
            token="scl_x",
            issued=True,
        )

    def wake():
        state.woken += 1

    monkeypatch.setattr(swaps, "git_swap_problem", problem)
    monkeypatch.setattr(images, "bind_service_image", bind)
    monkeypatch.setattr(leases, "issue_or_redeliver", issue)
    monkeypatch.setattr(hosting, "request_reconcile", wake)
    images.configure_service_images(
        images.ServiceImageSettings(service_namespace="srw-connectors")
    )
    from tests.test_connector_git_swap_driver import _make_ca

    certificate, key = _make_ca()
    driver_ca_module.configure_driver_ca(
        driver_ca_module.DriverCertificateAuthority.from_pem(certificate, key)
    )
    return state


class TestDeliver:
    @pytest.mark.asyncio
    async def test_a_candidate_the_driver_cannot_serve_falls_back_alone(
        self, delivery, caplog
    ):
        delivery.problems[OTHER] = "its workspace is a VM in another cluster"
        served, fallen = _candidate(CONNECTOR), _candidate(OTHER)
        with caplog.at_level(logging.WARNING, logger=swaps.__name__):
            count = await leases.deliver_connector_leases(
                object(), [served, fallen], owner=leases.LeaseOwner.job(JOB)
            )
        assert count == 1 and delivery.issued == [CONNECTOR]
        assert served["git_swap"]["url"].endswith(f"/{CONNECTOR}/o/r")
        assert served["credentials"]["lease"]["token"] == "scl_x"
        # The fallback: no lease, the token kept for the clone URL, and why.
        assert fallen["git_swap"] == {
            "fallback": "its workspace is a VM in another cluster"
        }
        assert fallen["credentials"] == {"token": TOKEN}
        assert "delivers its token in the clone URL" in caplog.text
        # A new binding asks the reconciler for a pass (S1).
        assert delivery.woken == 1

    @pytest.mark.asyncio
    async def test_refuse_delivers_nothing_and_says_why(self, delivery):
        swaps.configure_git_swap_delivery(
            swaps.GitSwapDeliverySettings(installed=True, fallback="refuse")
        )
        delivery.problems[CONNECTOR] = "its driver pod did not start (capacity)"
        entry = _candidate()
        assert (
            await leases.deliver_connector_leases(
                object(), [entry], owner=leases.LeaseOwner.job(JOB)
            )
            == 0
        )
        assert entry["credentials"] == {}
        assert "did not start (capacity)" in entry["git_swap"]["unavailable"]
        assert "refuses token-in-URL" in entry["git_swap"]["unavailable"]

    @pytest.mark.asyncio
    async def test_an_image_that_does_not_resolve_falls_back_instead_of_refusing(
        self, delivery
    ):
        delivery.image_error = images.ServiceImageUnavailable(
            "The image of the service driver srw.git-swap/v1 cannot be resolved"
        )
        entry = _candidate()
        assert (
            await leases.deliver_connector_leases(
                object(), [entry], owner=leases.LeaseOwner.job(JOB)
            )
            == 0
        )
        assert "image" in entry["git_swap"]["fallback"]
        assert entry["credentials"] == {"token": TOKEN}

    @pytest.mark.asyncio
    async def test_another_service_drivers_image_failure_still_refuses(self, delivery):
        delivery.image_error = images.ServiceImageUnavailable("no image")
        entry = {
            "type": "echo_service",
            "name": "Echo",
            "datasource_id": CONNECTOR,
            "credentials": {"secret": "s"},
        }
        with pytest.raises(leases.LeaseDeliveryError):
            await leases.deliver_connector_leases(
                object(), [entry], owner=leases.LeaseOwner.job(JOB)
            )
        assert "secret" not in entry["credentials"]

    @pytest.mark.asyncio
    async def test_the_readme_states_the_fallback(self, delivery):
        from agent.connectors.checkout import CheckoutMaterializer
        from agent.connectors.base import RuntimeContext
        from agent.connectors.legacy import checkout_auth, deliveries_from_payload

        delivery.problems[CONNECTOR] = "its workspace is a static-pool host"
        entry = _candidate()
        await leases.deliver_connector_leases(
            object(), [entry], owner=leases.LeaseOwner.job(JOB)
        )
        assert checkout_auth(entry) == "token_in_url"
        [item] = deliveries_from_payload([entry])
        assert item.spec is REPOSITORY_SPEC
        workspace = SimpleNamespace(source_repo_meta={}, source_repo_skipped={})
        rt = RuntimeContext(execution="session", workspace_manager=workspace)
        [facts] = CheckoutMaterializer().facts([item], rt)
        assert (
            "forge token in its remote URL, NOT through SRW's git swap driver"
            in (facts.lines[0])
        )
        assert "static-pool host" in facts.lines[0]


@pytest.mark.asyncio
async def test_preparing_a_delivery_checks_each_candidates_upstream(monkeypatch):
    seen: list[tuple] = []

    async def verdict(host, *, ca_pem, private_allowed, fresh=False):
        seen.append((host, ca_pem, private_allowed))
        return None

    async def private(conn, connector_id, *, private_tiers):
        return connector_id == OTHER

    class Store:
        def acquire(self):
            class Context:
                async def __aenter__(self_inner):
                    return object()

                async def __aexit__(self_inner, *exc):
                    return False

            return Context()

    monkeypatch.setattr(swaps, "upstream_verdict", verdict)
    monkeypatch.setattr(swaps, "private_addresses_allowed", private)
    swaps.configure_git_swap_delivery(swaps.GitSwapDeliverySettings(installed=True))
    other = _candidate(OTHER, "https://git.corp.example/o/r.git")
    other["config"]["upstream_ca"] = "CA"
    await swaps.prepare_git_swap_delivery(
        Store(), [_candidate(), other, {"type": "repository"}, "x"]
    )
    assert seen == [("github.com", None, False), ("git.corp.example", "CA", True)]


def test_every_lease_preparation_checks_git_swap_upstreams():
    source = (
        ROOT / "src/orchestrator/services/connector_credential_leases.py"
    ).read_text()
    prepare = source[source.index("async def prepare_lease_delivery") :]
    prepare = prepare[: prepare.index("\nasync def ")]
    assert "prepare_git_swap_delivery(db, entries)" in prepare


# =============================================================================
# Test (the connector's check)
# =============================================================================


class TestCheck:
    @pytest.mark.asyncio
    async def test_test_says_how_the_token_is_delivered(self, monkeypatch):
        async def probe(ds, url, creds):
            return {"status": "ok", "message": "Authenticated as octo"}

        async def report(row, *, token=None):
            assert token == TOKEN
            return {
                "driver": GIT_SWAP_SPEC.name,
                "mode": "token-in-url",
                "reason": "the certificate of git.corp does not verify",
                "upstream_tls": "the certificate of git.corp does not verify",
            }

        from orchestrator.services.connector_drivers import repository

        monkeypatch.setattr(repository, "probe_repository", probe)
        monkeypatch.setattr(swaps, "delivery_report", report)
        row = {
            "id": CONNECTOR,
            "type": "repository",
            "connection_url": "https://git.corp/o/r.git",
            "config": {"forge": "gitlab"},
        }
        result = await RepositoryDriver().check(
            row, {"auth_method": "token", "token": TOKEN}, ctx=None
        )
        assert result["status"] == "ok"
        assert "NOT through SRW's git swap driver" in result["message"]
        assert result["details"]["delivery"]["mode"] == "token-in-url"
        # An SSH-key repository has nothing to say about it.
        ssh = await RepositoryDriver().check(row, {"auth_method": "ssh"}, ctx=None)
        assert "delivery" not in (ssh.get("details") or {})

    @pytest.mark.asyncio
    async def test_the_report_probes_the_upstream_afresh(self, monkeypatch):
        asked: list[bool] = []

        async def verdict(host, *, ca_pem, private_allowed, fresh=False):
            asked.append(fresh)
            return None

        async def private(conn, connector_id, *, private_tiers):
            return False

        async def launch(conn, connector_id):
            return None

        class Store:
            def acquire(self):
                class Context:
                    async def __aenter__(self_inner):
                        return object()

                    async def __aexit__(self_inner, *exc):
                        return False

                return Context()

        monkeypatch.setattr(swaps, "upstream_verdict", verdict)
        monkeypatch.setattr(swaps, "private_addresses_allowed", private)
        monkeypatch.setattr(swaps, "launch_problem", launch)
        assert await swaps.delivery_report({"id": CONNECTOR}) is None
        swaps.configure_git_swap_delivery(
            swaps.GitSwapDeliverySettings(installed=True, store=Store())
        )
        report = await swaps.delivery_report(
            {"id": CONNECTOR, "connection_url": "https://github.com/o/r.git"}
        )
        assert report["mode"] == "git-swap"
        assert report["upstream_tls"] == "verified against public roots"
        assert asked == [True]
        assert "through SRW's git swap driver" in swaps.describe(report)
        report = await swaps.delivery_report(
            {"id": CONNECTOR, "connection_url": "https://github.com:8443/o/r.git"}
        )
        assert report["mode"] == "token-in-url" and "port 443" in report["reason"]


# =============================================================================
# The upstream CA
# =============================================================================


class TestUpstreamCa:
    def test_only_pem_certificates(self):
        certificate, key = _self_signed()
        assert (
            swaps.validate_upstream_ca(f"  {certificate}  ")
            == certificate.strip() + "\n"
        )
        assert swaps.validate_upstream_ca("") is None
        assert swaps.validate_upstream_ca(None) is None
        for bad, why in (
            ("not pem", "not PEM"),
            (certificate + key, "private key"),
            ("-----BEGIN CERTIFICATE-----\n" + "A" * 70000, "64 KiB"),
            (42, "PEM text"),
        ):
            with pytest.raises(ValueError, match=why):
                swaps.validate_upstream_ca(bad)

    def test_a_repository_connector_keeps_it(self):
        certificate, _ = _self_signed()
        config = normalize_repository_config(
            {"forge": "gitlab", "upstream_ca": certificate}, "https://git.corp/o/r.git"
        )
        assert config["upstream_ca"] == certificate
        assert "upstream_ca" not in normalize_repository_config(
            {"forge": "gitlab", "upstream_ca": " "}, "https://git.corp/o/r.git"
        )
        with pytest.raises(HTTPException) as refused:
            normalize_repository_config(
                {"forge": "gitlab", "upstream_ca": "x"}, "https://git.corp/o/r.git"
            )
        assert refused.value.status_code == 400
        assert "upstream_ca" in REPOSITORY_SPEC.config_schema["properties"]

    def test_the_driver_trusts_it_alone_and_a_change_starts_a_new_pod(self):
        certificate, _ = _self_signed()
        row = {
            "id": CONNECTOR,
            "connection_url": "https://git.corp/o/r.git",
            "config": {"forge": "gitlab", "upstream_ca": certificate},
        }
        connector = GitSwapDriver(IMAGE).service_connector(row)
        assert connector["config"] == {
            "upstream": "https://git.corp/o/r.git",
            "host": "git.corp",
            "upstream_ca": certificate,
        }
        plain = GitSwapDriver(IMAGE).service_connector({**row, "config": {}})
        assert "upstream_ca" not in plain["config"]
        assert credential_generation(
            GIT_SWAP_SPEC, connector, private_allowed=False
        ) != credential_generation(GIT_SWAP_SPEC, plain, private_allowed=False)


# =============================================================================
# The reconciler
# =============================================================================


def _pod_row(**over):
    row = {
        "id": "11111111-1111-4111-8111-111111111111",
        "connector_id": CONNECTOR,
        "driver": GIT_SWAP_SPEC.name,
        "image_digest": DIGEST,
        "credential_generation": "g1",
        "pod_name": "srw-drv-1",
        "ready_at": None,
        "created_at": dt.datetime.now(dt.timezone.utc),
        "idle_since": None,
    }
    row.update(over)
    return row


def _reconciler(state: PodState, **settings: Any) -> ServiceHostingReconciler:
    runtime = SimpleNamespace()

    async def observe(identity):
        return state

    runtime.observe = observe
    reconciler = ServiceHostingReconciler(
        store=None,
        runtime=runtime,
        drivers=builtin_connector_drivers(git_swap_image=IMAGE),
        settings=ServiceHostingSettings(
            namespace="srw-connectors",
            release_namespace="srw",
            shim_image="shim",
            exchange_host="srw-exchange.srw.svc",
            exchange_port=8088,
            orchestrator_labels={"app": "srw-orchestrator"},
            **settings,
        ),
    )
    stops: list[tuple[str, str | None]] = []

    async def stop(row, reason, report, *, error=None):
        stops.append((reason, error))

    reconciler._stop = stop
    reconciler.stops = stops
    return reconciler


class TestReconciler:
    @pytest.mark.asyncio
    async def test_the_drivers_upstream_report_stops_its_pod_at_once(self):
        reconciler = _reconciler(
            PodState("Running", upstream="the certificate of git.corp does not verify")
        )
        alive = await reconciler._observe(_pod_row(), hosting.ReconcileReport())
        assert alive is False
        assert reconciler.stops == [
            (
                hosting.UPSTREAM_UNREACHABLE,
                "the certificate of git.corp does not verify",
            )
        ]
        assert hosting.UPSTREAM_UNREACHABLE in hosting._BACKOFF_REASONS

    @pytest.mark.asyncio
    async def test_observe_reads_the_drivers_exit_code_78_and_its_message(self):
        from tests.test_connector_service_hosting import IDENTITY, POD, FakeApi

        api = FakeApi()
        runtime = hosting.ServicePodRuntime(api, api, namespace="srw-connectors")

        def pod(terminated):
            return {
                "metadata": {"name": POD, "uid": "u", "labels": dict(IDENTITY.labels)},
                "status": {
                    "phase": "Running",
                    "containerStatuses": [
                        {
                            "name": "driver",
                            "ready": False,
                            "state": {"waiting": {"reason": "CrashLoopBackOff"}},
                            "lastState": {"terminated": terminated},
                        }
                    ],
                },
            }

        api.objects[("pod", POD)] = pod(
            {"exitCode": 78, "message": "the upstream git.corp is unreachable\n"}
        )
        assert (await runtime.observe(IDENTITY)).upstream == (
            "the upstream git.corp is unreachable"
        )
        api.objects[("pod", POD)] = pod({"exitCode": 1, "message": "panic"})
        assert (await runtime.observe(IDENTITY)).upstream is None

    def test_the_exit_code_is_the_drivers_own(self):
        probe = (ROOT / "drivers/git-swap/probe.go").read_text()
        found = re.search(r"upstreamExitCode = (\d+)", probe)
        assert found and int(found.group(1)) == hosting.UPSTREAM_EXIT_CODE

    def test_a_git_swap_pod_idles_an_hour_at_least(self):
        reconciler = _reconciler(PodState("Running"), idle_seconds=600)
        assert reconciler._idle_seconds(_pod_row()) == 3600
        assert GIT_SWAP_SPEC.service.idle_seconds == 3600
        longer = _reconciler(PodState("Running"), idle_seconds=7200)
        assert longer._idle_seconds(_pod_row()) == 7200
        assert reconciler._idle_seconds(_pod_row(driver="srw.unknown/v1")) == 600

    @pytest.mark.asyncio
    async def test_a_new_binding_wakes_the_loop(self, monkeypatch):
        monkeypatch.setattr(hosting, "WAKE_SETTLE_SECONDS", 0.01)
        shutdown = asyncio.Event()
        passes: list[float] = []

        class Reconciler:
            async def reconcile_once(self):
                passes.append(asyncio.get_running_loop().time())
                return hosting.ReconcileReport()

        loop = asyncio.create_task(
            hosting.connector_service_reconciler(
                shutdown, build=Reconciler, interval_seconds=60
            )
        )
        for _ in range(100):
            if passes:
                break
            await asyncio.sleep(0.01)
        hosting.request_reconcile()
        for _ in range(200):
            if len(passes) >= 2:
                break
            await asyncio.sleep(0.01)
        shutdown.set()
        await asyncio.wait_for(loop, timeout=5)
        assert len(passes) == 2 and passes[1] - passes[0] < 2
        hosting.request_reconcile()  # no loop: a no-op


def test_the_driver_ca_volume_is_optional():
    deployment = (ROOT / "helm/templates/orchestrator/deployment.yaml").read_text()
    volume = deployment[
        deployment.index("- name: connector-driver-ca\n          secret:") :
    ]
    volume = volume[: volume.index("items:")]
    assert "optional: true" in volume
