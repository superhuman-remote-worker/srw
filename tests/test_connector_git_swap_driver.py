"""The git swap driver on the control plane (connector drivers C3).

Which token repositories go through it and what their binding carries (the
clean URL, the forge token for the agent process only, a lease, the
driver's URL and SRW's authority), the fallback where it cannot serve one,
what the lease exchange and the reconciler read from it, the certificate
authority that signs its pods, the TLS pod itself, and its installation.
"""

from __future__ import annotations

import base64
import datetime as dt
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from orchestrator.application import connectors as connectors_composition
from orchestrator.application.settings import (
    DeploymentSettings,
    parse_git_swap_fallback,
)
from orchestrator.services import agent_datasource_payload as payloads
from orchestrator.services import connector_credential_leases as leases
from orchestrator.services import connector_driver_ca as driver_ca_module
from orchestrator.services import connector_git_swap_delivery as swaps
from orchestrator.services import connector_service_hosting as hosting
from orchestrator.services import connector_service_images as images
from orchestrator.services.connector_driver_ca import (
    DriverCaError,
    DriverCertificateAuthority,
    load_driver_ca,
)
from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.connector_drivers.base import (
    BindContext,
    DeploymentGates,
    SupportsCredentialLease,
    SupportsServiceConnector,
)
from orchestrator.services.connector_drivers.git_swap import (
    GitSwapDriver,
    route_token_repository,
)
from orchestrator.services.connector_drivers.matrix import (
    HostingStatus,
    capability_matrix,
)
from orchestrator.services.connector_egress import EgressPins, PinnedHost
from orchestrator.services.connector_service_hosting import credential_generation
from orchestrator.services.connector_service_launch import (
    TLS_CERT_PATH,
    TLS_KEY_PATH,
    ServiceLaunchError,
    ServiceLaunchPolicy,
    ServicePodIdentity,
    build_service_launch,
    endpoint_service_name,
    endpoint_url,
    service_dns_names,
)
from shared.connectors.builtin import GIT_SWAP_SPEC, REPOSITORY_SPEC

CONNECTOR = "0d6f3a52-8b1c-4e8e-9a8f-1f2e3d4c5b6a"
OTHER = "9d6f3a52-8b1c-4e8e-9a8f-1f2e3d4c5b6a"
DIGEST = "sha256:" + "ab" * 32
IMAGE = "ghcr.io/superhuman-remote-worker/srw-driver-git-swap:1.0.0@" + DIGEST
TOKEN = "ghp_TheForgeToken0123456789abcdefABCDEF"
IDENTITY = "sdi_" + "A" * 49
NAMESPACE = "srw-connectors"


# =============================================================================
# A certificate authority, as the chart's genCA makes one
# =============================================================================


def _make_ca(*, rsa_key: bool = False, ca: bool = True, days: int = 3650):
    key = (
        rsa.generate_private_key(public_exponent=65537, key_size=2048)
        if rsa_key
        else ec.generate_private_key(ec.SECP256R1())
    )
    now = datetime.now(timezone.utc)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "srw-connector-drivers")])
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(days=1))
        .not_valid_after(now + dt.timedelta(days=days))
        .add_extension(x509.BasicConstraints(ca=ca, path_length=None), True)
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()), False
        )
        .sign(key, hashes.SHA256())
    )
    key_format = (
        serialization.PrivateFormat.TraditionalOpenSSL
        if rsa_key
        else serialization.PrivateFormat.PKCS8
    )
    return (
        certificate.public_bytes(serialization.Encoding.PEM),
        key.private_bytes(
            serialization.Encoding.PEM, key_format, serialization.NoEncryption()
        ),
    )


@pytest.fixture(scope="module")
def ca() -> DriverCertificateAuthority:
    return DriverCertificateAuthority.from_pem(*_make_ca())


class TestCertificateAuthority:
    def test_a_leaf_names_the_service_and_chains_to_the_authority(self, ca):
        names = ["srw-ep-x.srw-connectors.svc.cluster.local", "srw-ep-x"]
        certificate_pem, key_pem = ca.issue(names)
        leaf = x509.load_pem_x509_certificate(certificate_pem.encode())
        authority = x509.load_pem_x509_certificate(ca.certificate_pem.encode())
        authority.public_key().verify(
            leaf.signature,
            leaf.tbs_certificate_bytes,
            ec.ECDSA(leaf.signature_hash_algorithm),
        )
        assert leaf.issuer == authority.subject
        san = leaf.extensions.get_extension_for_class(x509.SubjectAlternativeName)
        assert san.value.get_values_for_type(x509.DNSName) == names
        assert not leaf.extensions.get_extension_for_class(
            x509.BasicConstraints
        ).value.ca
        assert (
            ExtendedKeyUsageOID.SERVER_AUTH
            in leaf.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value
        )
        aki = leaf.extensions.get_extension_for_class(x509.AuthorityKeyIdentifier)
        ski = authority.extensions.get_extension_for_class(x509.SubjectKeyIdentifier)
        assert aki.value.key_identifier == ski.value.digest
        key = serialization.load_pem_private_key(key_pem.encode(), password=None)
        assert key.public_key().public_numbers() == leaf.public_key().public_numbers()

    def test_a_leaf_never_outlives_the_authority(self):
        short = DriverCertificateAuthority.from_pem(*_make_ca(days=10))
        leaf = x509.load_pem_x509_certificate(short.issue(["a"])[0].encode())
        assert leaf.not_valid_after_utc <= short.not_after

    def test_helms_rsa_authority_loads_and_signs(self, tmp_path):
        certificate, key = _make_ca(rsa_key=True)
        assert b"BEGIN RSA PRIVATE KEY" in key
        (tmp_path / "tls.crt").write_bytes(certificate)
        (tmp_path / "tls.key").write_bytes(key)
        authority = load_driver_ca(str(tmp_path))
        assert authority is not None
        leaf = x509.load_pem_x509_certificate(authority.issue(["h"])[0].encode())
        assert leaf.signature_hash_algorithm.name == "sha256"

    def test_an_unusable_authority_is_refused(self, tmp_path, caplog):
        not_ca = _make_ca(ca=False)
        with pytest.raises(DriverCaError, match="no CA"):
            DriverCertificateAuthority.from_pem(*not_ca)
        with pytest.raises(DriverCaError, match="does not match"):
            DriverCertificateAuthority.from_pem(_make_ca()[0], _make_ca()[1])
        with pytest.raises(DriverCaError, match="does not load"):
            DriverCertificateAuthority.from_pem(b"nope", b"nope")
        assert load_driver_ca("") is None
        with caplog.at_level(logging.ERROR):
            assert load_driver_ca(str(tmp_path / "missing")) is None
        assert "not installed" in caplog.text


# =============================================================================
# Routing a token repository at bind
# =============================================================================


def _entry(url: str = "https://GitHub.com/o/r.git/", **credentials: Any) -> dict:
    return {
        "type": "repository",
        "name": "Repo",
        "description": None,
        "connection_url": url,
        "credentials": credentials or {"auth_method": "token", "token": TOKEN},
        "project_read_only": False,
        "datasource_id": CONNECTOR,
        "config": {"forge": "github"},
    }


@pytest.fixture(autouse=True)
def _fresh_notes():
    swaps.configure_git_swap_delivery(swaps.GitSwapDeliverySettings())
    yield
    swaps.configure_git_swap_delivery(swaps.GitSwapDeliverySettings())


class TestRouting:
    def test_a_token_repository_is_a_candidate_until_delivery(self):
        # The lease step decides per delivery; until then the entry is the
        # one SRW sent before C3, plus the empty block.
        entry = _entry()
        before = json.loads(json.dumps(entry))
        route_token_repository(entry, git_swap=GitSwapDriver(IMAGE), fallback="refuse")
        assert entry["git_swap"] == {}
        assert {k: v for k, v in entry.items() if k != "git_swap"} == before
        assert leases.lease_spec(entry) is GIT_SWAP_SPEC

    def test_without_the_driver_the_entry_is_what_it_was(self, caplog):
        entry = _entry()
        before = json.loads(json.dumps(entry))
        with caplog.at_level(logging.WARNING):
            route_token_repository(entry, git_swap=None, fallback="token-in-url")
        # The entry SRW sent before C3, byte for byte: nothing to say on an
        # installation without the driver.
        assert entry == before
        assert not caplog.records

    @pytest.mark.parametrize(
        ("url", "why"),
        [
            ("http://gitea.srw.svc:3000/o/r.git", "not HTTPS"),
            ("https://git.example.com:8443/o/r.git", "port 443"),
            ("https://oauth2:x@github.com/o/r.git", "credentials"),
        ],
    )
    def test_a_url_the_driver_cannot_serve_takes_the_fallback(self, url, why, caplog):
        entry = _entry(url)
        with caplog.at_level(logging.WARNING, logger=swaps.__name__):
            route_token_repository(
                entry, git_swap=GitSwapDriver(IMAGE), fallback="token-in-url"
            )
            route_token_repository(
                entry, git_swap=GitSwapDriver(IMAGE), fallback="token-in-url"
            )
        # Visible: the entry says why (the README states it: one of the fixed
        # reasons), logged once with the detail.
        assert entry["git_swap"]["fallback"] == swaps.REASONS["url_not_served"]
        assert entry["credentials"]["token"] == TOKEN
        assert leases.lease_spec(entry) is None
        assert caplog.text.count("delivers its token in the clone URL") == 1
        assert why in caplog.text

    def test_refuse_delivers_no_token_and_says_why(self):
        entry = _entry("http://gitea.srw.svc:3000/o/r.git")
        route_token_repository(entry, git_swap=GitSwapDriver(IMAGE), fallback="refuse")
        assert entry["credentials"] == {}
        assert swaps.REASONS["url_not_served"] in entry["git_swap"]["unavailable"]
        assert "refuses token-in-URL" in entry["git_swap"]["unavailable"]
        # Still a repository entry: no lease is issued for it.
        assert leases.lease_spec(entry) is None
        uninstalled = _entry()
        route_token_repository(uninstalled, git_swap=None, fallback="refuse")
        assert "not installed" in uninstalled["git_swap"]["unavailable"]

    @pytest.mark.parametrize(
        "credentials",
        [
            {"auth_method": "ssh"},
            {"auth_method": "none"},
            {},
            {"auth_method": "token"},
        ],
    )
    def test_anything_but_a_token_is_left_alone(self, credentials):
        entry = _entry()
        entry["credentials"] = dict(credentials)
        before = json.loads(json.dumps(entry))
        route_token_repository(entry, git_swap=GitSwapDriver(IMAGE), fallback="refuse")
        assert entry == before

    def test_an_ssh_identity_is_never_swapped(self):
        entry = _entry(**{"token": TOKEN})
        entry["ssh_identity"] = {"alias": "srw-repo-x"}
        before = json.loads(json.dumps(entry))
        route_token_repository(entry, git_swap=GitSwapDriver(IMAGE), fallback="refuse")
        assert entry == before


def _dependencies(registry, fallback: str = "token-in-url"):
    return payloads.DatasourcePayloadDependencies(
        logger=logging.getLogger("test"),
        mcp_datasources_enabled=lambda: False,
        mcp_stdio_enabled=lambda: False,
        connector_drivers=registry,
        workspace_ssh_known_hosts=lambda: "",
        git_swap_fallback=lambda: fallback,
    )


def _row(url="https://github.com/o/r.git"):
    return {
        "id": CONNECTOR,
        "type": "repository",
        "name": "Repo",
        "description": None,
        "connection_url": url,
        "credentials": {"auth_method": "token", "token": TOKEN},
        "config": {"forge": "github"},
        "project_read_only": False,
    }


class TestPayload:
    def test_a_token_repository_is_a_swap_candidate(self):
        registry = builtin_connector_drivers(git_swap_image=IMAGE)
        [entry] = payloads.build_datasources_payload(
            [_row()], dependencies=_dependencies(registry)
        )
        assert entry["git_swap"] == {}
        # A lease driver's entry carries only what its driver keeps for the
        # agent process: the forge token (a fallback clones with it).
        assert entry["credentials"] == {"token": TOKEN}
        assert leases.lease_spec(entry) is GIT_SWAP_SPEC

    def test_without_the_driver_the_payload_is_what_it_was(self):
        registry = builtin_connector_drivers()
        [entry] = payloads.build_datasources_payload(
            [_row()], dependencies=_dependencies(registry)
        )
        assert "git_swap" not in entry
        assert entry["credentials"] == {"auth_method": "token", "token": TOKEN}
        assert leases.lease_spec(entry) is None

    def test_refuse_reaches_the_payload(self):
        registry = builtin_connector_drivers()
        [entry] = payloads.build_datasources_payload(
            [_row()], dependencies=_dependencies(registry, "refuse")
        )
        assert entry["credentials"] == {}
        assert "unavailable" in entry["git_swap"]

    def test_the_dependencies_default_to_the_behaviour_before_c3(self):
        deps = payloads.DatasourcePayloadDependencies(
            logger=logging.getLogger("test"),
            mcp_datasources_enabled=lambda: False,
            mcp_stdio_enabled=lambda: False,
            connector_drivers=builtin_connector_drivers(),
            workspace_ssh_known_hosts=lambda: "",
        )
        assert deps.git_swap_fallback() == "token-in-url"
        assert (
            BindContext(
                gates=DeploymentGates(lambda: False, lambda: False),
                logger=logging.getLogger("t"),
                default_known_hosts="",
            ).git_swap
            is None
        )


# =============================================================================
# Delivery: the lease, the driver's URL and the authority
# =============================================================================


@pytest.mark.asyncio
async def test_a_binding_gets_a_lease_the_drivers_url_and_the_authority(
    monkeypatch, ca
):
    seen: dict[str, Any] = {}

    async def bind(conn, *, spec, connector_id, owner):
        return DIGEST

    async def issue(
        _conn, *, owner, connector_id, driver, access, image_digest, ttl_seconds
    ):
        seen["issue"] = (driver, access, image_digest)
        return MagicMock(id="l1", connector_id=connector_id, token="scl_x")

    async def servable(conn, entry, *, connector_id, owner):
        return None

    monkeypatch.setattr(images, "bind_service_image", bind)
    monkeypatch.setattr(leases, "issue_or_redeliver", issue)
    monkeypatch.setattr(swaps, "git_swap_problem", servable)
    images.configure_service_images(
        images.ServiceImageSettings(
            service_namespace=NAMESPACE, service_start_seconds=195
        )
    )
    driver_ca_module.configure_driver_ca(ca)
    try:
        registry = builtin_connector_drivers(git_swap_image=IMAGE)
        [entry] = payloads.build_datasources_payload(
            [{**_row("https://GitHub.com/o/r.git/"), "project_read_only": True}],
            dependencies=_dependencies(registry),
        )
        owner = leases.LeaseOwner.job("00000000-0000-4000-8000-000000000001")
        assert await leases.deliver_connector_leases(object(), [entry], owner=owner)
        assert seen["issue"] == ("srw.git-swap/v1", "ReadOnly", DIGEST)
        # No auth_method, no URL token: the workspace never gets the token.
        assert entry["credentials"] == {
            "token": TOKEN,
            "lease": {"id": "l1", "connector_id": CONNECTOR, "token": "scl_x"},
        }
        endpoint = endpoint_service_name(CONNECTOR, DIGEST)
        assert entry["git_swap"] == {
            "url": (
                f"https://{endpoint}.{NAMESPACE}.svc.cluster.local:8443/{CONNECTOR}/o/r"
            ),
            "ca": ca.certificate_pem,
            "wait_seconds": 195,
        }
        # The remote is the clean upstream URL.
        assert entry["connection_url"] == "https://github.com/o/r.git"
    finally:
        images.configure_service_images(images.ServiceImageSettings())
        driver_ca_module.configure_driver_ca(None)


def test_harness_credentials_are_only_what_the_spec_names():
    entry = {"credentials": {"token": TOKEN, "auth_method": "token", "x": "y"}}
    assert leases.harness_credentials(entry, GIT_SWAP_SPEC) == {"token": TOKEN}
    assert leases.harness_credentials(entry, REPOSITORY_SPEC) == {}
    assert leases.harness_credentials({"credentials": "x"}, GIT_SWAP_SPEC) == {}


# =============================================================================
# The exchange and the reconciler
# =============================================================================


class TestDriver:
    def test_the_exchange_gets_the_forge_token_and_the_one_upstream(self):
        driver = GitSwapDriver(IMAGE)
        assert isinstance(driver, SupportsCredentialLease)
        row = {
            "type": "repository",
            "connection_url": "https://github.com/o/r.git",
            "credentials": json.dumps({"auth_method": "token", "token": TOKEN}),
        }
        assert driver.lease_upstream(row) == {
            "credential": TOKEN,
            "allowed_upstream": ["https://github.com/o/r.git"],
        }
        for broken in (
            {**row, "credentials": {"auth_method": "ssh", "ssh_key": "k"}},
            {**row, "credentials": {}},
            {**row, "connection_url": "http://github.com/o/r.git"},
        ):
            with pytest.raises(ValueError):
                driver.lease_upstream(broken)

    def test_its_pods_are_built_from_the_clean_url_and_its_host(self):
        driver = GitSwapDriver(IMAGE)
        assert isinstance(driver, SupportsServiceConnector)
        row = {"id": CONNECTOR, "connection_url": "https://GitHub.com/o/r.git/"}
        view = driver.service_connector(row)
        assert view["config"] == {
            "upstream": "https://github.com/o/r.git",
            "host": "github.com",
        }
        # An access change starts no pod; a URL change does.
        same = credential_generation(GIT_SWAP_SPEC, view, private_allowed=False)
        assert same == credential_generation(
            GIT_SWAP_SPEC,
            {**view, "config": {**view["config"], "access": "ReadOnly"}},
            private_allowed=False,
        )
        moved = driver.service_connector(
            {**row, "connection_url": "https://github.com/o/s"}
        )
        assert (
            credential_generation(GIT_SWAP_SPEC, moved, private_allowed=False) != same
        )
        # Nothing to pin: the pod is refused at launch.
        assert (
            driver.service_connector({**row, "connection_url": "http://x/o/r"})[
                "config"
            ]
            == {}
        )

    def test_it_is_installed_right_after_the_repository_driver(self):
        assert builtin_connector_drivers().get(GIT_SWAP_SPEC.name) is None
        registry = builtin_connector_drivers(git_swap_image=IMAGE)
        names = [driver.spec.name for driver in registry.drivers()]
        at = names.index(REPOSITORY_SPEC.name)
        assert names[at + 1] == GIT_SWAP_SPEC.name
        # The repository type stays the repository driver's.
        assert registry.for_type("repository").spec is REPOSITORY_SPEC
        assert registry.get(GIT_SWAP_SPEC.name).image_reference == IMAGE


@pytest.mark.asyncio
async def test_the_reconciler_starts_its_pod_from_the_derived_connector(monkeypatch):
    registry = builtin_connector_drivers(git_swap_image=IMAGE)
    started: list[Any] = []

    class Conn:
        async def fetch(self, query, *args):
            if "FROM connector_credential_leases" in query:
                return [
                    {
                        "connector_id": CONNECTOR,
                        "driver": GIT_SWAP_SPEC.name,
                        "image_digest": DIGEST,
                        "job_id": "00000000-0000-4000-8000-000000000001",
                        "thread_id": None,
                    }
                ]
            return []

    class Store:
        @asynccontextmanager
        async def acquire(self):
            yield Conn()

        async def get_datasource(self, connector_id):
            return {**_row("https://GitHub.com/o/r.git"), "id": connector_id}

    class Runtime:
        async def service_cluster_ip(self, name, namespace):
            return "10.43.0.20"

        async def endpoint_services(self):
            return []

        async def managed_objects(self):
            return []

    async def private(conn, connector_id, *, private_tiers):
        return False

    async def start(self, spec, binding, connector, **kwargs):
        started.append((spec.name, connector))

    async def not_backed_off(self, *args):
        return False

    monkeypatch.setattr(hosting, "private_addresses_allowed", private)
    monkeypatch.setattr(hosting.ServiceHostingReconciler, "_start", start)
    monkeypatch.setattr(hosting.ServiceHostingReconciler, "_backed_off", not_backed_off)
    settings = hosting.ServiceHostingSettings(
        namespace=NAMESPACE,
        release_namespace="srw",
        shim_image="shim@sha256:" + "ef" * 32,
        exchange_host="srw-orchestrator.srw.svc",
        exchange_port=8088,
        orchestrator_labels={"app": "o"},
        pod_ip="10.42.0.5",
        node_ip="172.18.0.2",
        refused_cidrs=("172.16.0.0/12",),
    )
    reconciler = hosting.ServiceHostingReconciler(
        store=Store(), runtime=Runtime(), drivers=registry, settings=settings
    )
    await reconciler.reconcile_once()
    assert started == [
        (
            GIT_SWAP_SPEC.name,
            {
                **_row("https://GitHub.com/o/r.git"),
                "config": {
                    "upstream": "https://github.com/o/r.git",
                    "host": "github.com",
                },
            },
        )
    ]


# =============================================================================
# The pod: TLS on its srw-driver port
# =============================================================================


def _identity() -> ServicePodIdentity:
    return ServicePodIdentity(
        identity_id="11111111-2222-4333-8444-555555555555",
        connector_id=CONNECTOR,
        driver=GIT_SWAP_SPEC.name,
        digest=DIGEST,
        generation="hmac-sha256:" + "cd" * 32,
    )


def _policy(ca=None) -> ServiceLaunchPolicy:
    return ServiceLaunchPolicy(
        namespace=NAMESPACE,
        release_namespace="srw",
        shim_image="srw-registry:5000/srw-driver-shim@sha256:" + "ef" * 32,
        exchange_host="srw-orchestrator.srw.svc",
        exchange_address="10.43.0.20",
        exchange_port=8088,
        orchestrator_labels={"app.kubernetes.io/component": "orchestrator"},
        driver_ca=ca,
    )


def _plan(ca):
    return build_service_launch(
        _identity(),
        spec=GIT_SWAP_SPEC,
        image=f"ghcr.io/superhuman-remote-worker/srw-driver-git-swap@{DIGEST}",
        entrypoint=["/srw-git-swap"],
        cmd=["serve"],
        config={"upstream": "https://github.com/o/r.git", "host": "github.com"},
        credentials={"token": TOKEN},
        identity_token=IDENTITY,
        pins=EgressPins(
            hosts=(PinnedHost("github.com", ("140.82.121.3",), (443,)),),
            resolved_at=datetime(2026, 10, 8, tzinfo=timezone.utc),
        ),
        policy=_policy(ca),
    )


class TestPod:
    def test_the_secret_holds_its_certificate_and_never_the_token(self, ca):
        plan = _plan(ca)
        data = {
            key: base64.b64decode(value).decode()
            for key, value in plan.secret["data"].items()
        }
        assert set(data) == {"request.json", "identity", "tls.crt", "tls.key"}
        assert TOKEN not in json.dumps(data)
        request = json.loads(data["request.json"])
        assert request["tls"] == {"cert_file": TLS_CERT_PATH, "key_file": TLS_KEY_PATH}
        assert request["connector"]["config"] == {
            "upstream": "https://github.com/o/r.git",
            "host": "github.com",
        }
        assert request["credentials"] == {}
        leaf = x509.load_pem_x509_certificate(data["tls.crt"].encode())
        names = leaf.extensions.get_extension_for_class(
            x509.SubjectAlternativeName
        ).value.get_values_for_type(x509.DNSName)
        endpoint = endpoint_service_name(CONNECTOR, DIGEST)
        assert f"{endpoint}.{NAMESPACE}.svc.cluster.local" in names
        assert names == service_dns_names(_identity(), NAMESPACE)
        assert "PRIVATE KEY" in data["tls.key"]

    def test_the_driver_mounts_its_certificate_and_serves_its_port(self, ca):
        plan = _plan(ca)
        [driver] = plan.pod["spec"]["containers"]
        mounts = {mount["mountPath"]: mount for mount in driver["volumeMounts"]}
        assert mounts[TLS_CERT_PATH]["subPath"] == "tls.crt"
        assert mounts[TLS_KEY_PATH]["subPath"] == "tls.key"
        assert all(mounts[path]["readOnly"] for path in (TLS_CERT_PATH, TLS_KEY_PATH))
        assert driver["ports"][0]["containerPort"] == 8443
        assert driver["args"] == ["/srw-git-swap", "serve"]
        # Only each binding's own workspace is admitted (per-binding policies);
        # the pod's own policy admits nobody, and egress is the pinned host.
        assert plan.network_policy["spec"]["ingress"] == []
        egress = plan.network_policy["spec"]["egress"]
        assert {"ipBlock": {"cidr": "140.82.121.3/32"}} in egress[1]["to"]
        assert egress[1]["ports"] == [{"protocol": "TCP", "port": 443}]

    def test_without_the_authority_the_pod_is_refused(self):
        with pytest.raises(ServiceLaunchError, match="certificate authority"):
            _plan(None)

    def test_the_endpoint_url_is_https_for_a_tls_driver(self):
        url = endpoint_url(
            namespace=NAMESPACE,
            connector_id=CONNECTOR,
            digest=DIGEST,
            port=8443,
            scheme="https",
        )
        assert url.startswith("https://srw-ep-") and url.endswith(":8443")
        with pytest.raises(ValueError):
            endpoint_url(
                namespace=NAMESPACE,
                connector_id=CONNECTOR,
                digest=DIGEST,
                port=1,
                scheme="ftp",
            )


# =============================================================================
# Installation and the matrix
# =============================================================================


class TestInstallation:
    def test_the_fallback_comes_from_the_deployment(self, monkeypatch, caplog):
        assert parse_git_swap_fallback(None) == "token-in-url"
        assert parse_git_swap_fallback(" Refuse ") == "refuse"
        with caplog.at_level(logging.WARNING):
            assert parse_git_swap_fallback("token_in_url") == "refuse"
        assert "refusing" in caplog.text
        monkeypatch.setenv("CONNECTOR_GIT_SWAP_FALLBACK", "refuse")
        monkeypatch.setenv("CONNECTOR_GIT_SWAP_IMAGE", f" {IMAGE} ")
        monkeypatch.setenv("CONNECTOR_DRIVER_CA_DIR", "/run/srw/connector-driver-ca")
        settings = DeploymentSettings.from_environment()
        assert settings.connector_git_swap_fallback == "refuse"
        assert settings.connector_git_swap_image == IMAGE
        assert settings.connector_driver_ca_dir == "/run/srw/connector-driver-ca"

    def test_it_is_installed_only_with_hosting_and_the_authority(self, ca, caplog):
        settings = SimpleNamespace(
            connector_git_swap_image=IMAGE,
            connector_service_pods_enabled=True,
            connector_git_swap_fallback="token-in-url",
        )
        assert connectors_composition.git_swap_image(settings, ca) == IMAGE
        with caplog.at_level(logging.ERROR):
            assert connectors_composition.git_swap_image(settings, None) is None
            settings.connector_service_pods_enabled = False
            assert connectors_composition.git_swap_image(settings, ca) is None
        assert "certificate authority" in caplog.text
        assert "service-pod hosting" in caplog.text
        settings.connector_git_swap_image = ""
        assert connectors_composition.git_swap_image(settings, ca) is None

    def test_the_first_clone_waits_for_the_reconciler_and_the_start(self):
        settings = DeploymentSettings.from_environment()
        resources = SimpleNamespace(
            settings=settings,
            connector_drivers=builtin_connector_drivers(git_swap_image=IMAGE),
            postgres_db=object(),
        )
        image_settings = connectors_composition.service_image_settings(resources)
        assert image_settings.references[GIT_SWAP_SPEC.name] == IMAGE
        assert image_settings.service_start_seconds == (
            settings.connector_service_reconcile_seconds
            + settings.connector_service_start_timeout_seconds
        )

    def test_the_matrix_shows_srws_own_tls_driver_and_its_egress(self):
        registry = builtin_connector_drivers(git_swap_image=IMAGE)
        entry = next(
            item
            for item in capability_matrix(
                registry, hosting=HostingStatus(enabled=True)
            )["drivers"]
            if item["name"] == GIT_SWAP_SPEC.name
        )
        assert entry["trust"] == {
            "tier": "trusted",
            "trusted": True,
            "image": IMAGE,
            "claims_declared_by_author": False,
        }
        assert entry["serves_stored_type"] is False
        assert entry["holds_upstream_credentials"] is True
        assert entry["service"]["tls"] is True
        assert entry["service"]["callers"] == ["workspace"]
        assert entry["egress"]["declared"]["rules"] == [
            {"host": "${config.host}", "ports": [443], "protocol": "tcp"}
        ]
        levels = {level["id"]: level for level in entry["access_levels"]}
        assert levels["ReadOnly"]["advisory"] is False
        assert "request path" in levels["ReadOnly"]["enforced_by"]
        assert "refs/heads/" in levels["ReadWrite"]["enforced_by"]
