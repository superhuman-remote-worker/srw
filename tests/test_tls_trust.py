"""An HTTPS model API behind a private CA is reachable once the operator trusts it.

Regression for APP-TLS-1 (university upgrade test 2026-10-14, vault
``university-generator-upgrade-issues-2026-10-14.md``): the university's own
OpenAI-compatible model API serves HTTPS with a certificate from its own CA.
SRW offered no way to trust that CA, the API has no plain-HTTP port, and the
operator fell back to OpenRouter, sending prompts and documents off-site.

The contract: the chart value ``trust.extraCaCertificates`` reaches the
orchestrator, every agent pod type and every container workspace; each Python
process then trusts the public roots *plus* that CA with verification on, so
httpx, the OpenAI SDK, requests and the ``ssl`` default context reach the
private endpoint, and nothing else about public endpoints changes.
"""

from __future__ import annotations

import datetime
import json
import logging
import ssl
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from ipaddress import IPv4Address
from pathlib import Path

import httpx
import pytest

from shared.tls_trust import (
    BUNDLE_ENV_NAMES,
    EXTRA_CA_FILE_ENV,
    TRUST_CA_CONFIGMAP_ENV,
    TRUST_CA_CONFIGMAP_KEY_ENV,
    TRUST_CA_FILE,
    TRUST_CA_MOUNT_DIR,
    TRUST_CA_VOLUME,
    TrustCaSource,
    install_extra_ca_trust,
)

ROOT = Path(__file__).resolve().parents[1]
PEM_BEGIN = "-----BEGIN CERTIFICATE-----"


# ---------------------------------------------------------------------------
# A private CA and an HTTPS "model API" whose certificate it signed
# ---------------------------------------------------------------------------


def _pki(directory: Path) -> dict[str, Path]:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    now = datetime.datetime.now(datetime.timezone.utc)
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "University CA")])
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            True,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), False
        )
        .sign(ca_key, hashes.SHA256())
    )
    leaf_key = ec.generate_private_key(ec.SECP256R1())
    leaf_cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "model-api")]))
        .issuer_name(ca_name)
        .public_key(leaf_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(IPv4Address("127.0.0.1"))]),
            False,
        )
        .add_extension(
            x509.ExtendedKeyUsage([x509.oid.ExtendedKeyUsageOID.SERVER_AUTH]), False
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
            False,
        )
        .sign(ca_key, hashes.SHA256())
    )
    paths = {
        "ca": directory / "ca.crt",
        "cert": directory / "server.crt",
        "key": directory / "server.key",
    }
    paths["ca"].write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    paths["cert"].write_bytes(leaf_cert.public_bytes(serialization.Encoding.PEM))
    paths["key"].write_bytes(
        leaf_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return paths


class _ModelApi(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 - http.server API
        body = json.dumps(
            {
                "object": "list",
                "data": [
                    {
                        "id": "gemma-4-31b-it",
                        "object": "model",
                        "created": 0,
                        "owned_by": "university",
                    }
                ],
            }
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        return None


@pytest.fixture
def pki(tmp_path):
    return _pki(tmp_path)


@pytest.fixture
def model_api(pki):
    """``https://127.0.0.1:<port>``, served with the private CA's certificate."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ModelApi)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(pki["cert"], pki["key"])
    server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"https://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def trust_env(monkeypatch):
    """Clear the trust variables; every value the hook writes is undone."""
    for name in (*BUNDLE_ENV_NAMES, "NODE_EXTRA_CA_CERTS", "SSL_CERT_DIR"):
        monkeypatch.setenv(name, "")
        monkeypatch.delenv(name)
    monkeypatch.setenv(EXTRA_CA_FILE_ENV, "")
    monkeypatch.delenv(EXTRA_CA_FILE_ENV)
    return monkeypatch


# ---------------------------------------------------------------------------
# The process hook
# ---------------------------------------------------------------------------


class TestPrivateCaEndpoint:
    def test_without_the_ca_the_endpoint_fails_verification(self, model_api, trust_env):
        with pytest.raises(httpx.ConnectError, match="CERTIFICATE_VERIFY_FAILED"):
            httpx.get(f"{model_api}/v1/models")

    def test_with_the_ca_every_client_reaches_it_verified(
        self, model_api, pki, trust_env, tmp_path
    ):
        trust_env.setenv(EXTRA_CA_FILE_ENV, str(pki["ca"]))
        bundle = install_extra_ca_trust(bundle_dir=str(tmp_path))
        assert bundle is not None

        # httpx: what the catalog Test, discovery and the agent's clients use.
        response = httpx.get(f"{model_api}/v1/models")
        assert response.status_code == 200

        # The OpenAI SDK, as the chat/embedding/vision clients build it.
        from openai import OpenAI

        client = OpenAI(base_url=f"{model_api}/v1", api_key="not-needed")
        assert [model.id for model in client.models.list()] == ["gemma-4-31b-it"]

        # requests (citation engine) and the ssl default context (aiohttp,
        # urllib): both honour the published bundle.
        import requests

        assert requests.get(f"{model_api}/v1/models", timeout=10).status_code == 200
        import urllib.request

        with urllib.request.urlopen(
            f"{model_api}/v1/models", context=ssl.create_default_context(), timeout=10
        ) as raw:
            assert raw.status == 200

    def test_verification_stays_on_for_other_certificates(
        self, model_api, trust_env, tmp_path
    ):
        # Trusting one CA does not trust a certificate it did not sign.
        (tmp_path / "other").mkdir()
        other = _pki(tmp_path / "other")
        trust_env.setenv(EXTRA_CA_FILE_ENV, str(other["ca"]))
        assert install_extra_ca_trust(bundle_dir=str(tmp_path)) is not None
        with pytest.raises(httpx.ConnectError, match="CERTIFICATE_VERIFY_FAILED"):
            httpx.get(f"{model_api}/v1/models")


class TestBundle:
    def test_bundle_is_the_public_roots_plus_the_operator_ca(
        self, pki, trust_env, tmp_path
    ):
        import certifi

        trust_env.setenv(EXTRA_CA_FILE_ENV, str(pki["ca"]))
        bundle = Path(install_extra_ca_trust(bundle_dir=str(tmp_path)))
        roots = Path(certifi.where()).read_text()
        text = bundle.read_text()
        assert text.startswith(roots.rstrip("\n"))
        assert pki["ca"].read_text().strip() in text
        assert text.count(PEM_BEGIN) == roots.count(PEM_BEGIN) + 1
        import os

        for name in BUNDLE_ENV_NAMES:
            assert os.environ[name] == str(bundle)
        assert os.environ["NODE_EXTRA_CA_CERTS"] == str(pki["ca"])

    def test_a_second_install_does_not_duplicate_the_ca(self, pki, trust_env, tmp_path):
        trust_env.setenv(EXTRA_CA_FILE_ENV, str(pki["ca"]))
        first = install_extra_ca_trust(bundle_dir=str(tmp_path))
        count = Path(first).read_text().count(PEM_BEGIN)
        assert install_extra_ca_trust(bundle_dir=str(tmp_path)) == first
        assert Path(first).read_text().count(PEM_BEGIN) == count

    def test_an_operator_ssl_cert_file_is_the_base(self, pki, trust_env, tmp_path):
        (tmp_path / "own").mkdir()
        own = tmp_path / "own-roots.pem"
        own.write_text(_pki(tmp_path / "own")["ca"].read_text())
        trust_env.setenv("SSL_CERT_FILE", str(own))
        trust_env.setenv(EXTRA_CA_FILE_ENV, str(pki["ca"]))
        bundle = Path(install_extra_ca_trust(bundle_dir=str(tmp_path)))
        assert bundle.read_text().count(PEM_BEGIN) == 2
        assert bundle.read_text().startswith(own.read_text().strip())

    def test_unset_is_a_noop(self, trust_env, tmp_path):
        import os

        assert install_extra_ca_trust(bundle_dir=str(tmp_path)) is None
        assert "SSL_CERT_FILE" not in os.environ
        assert list(tmp_path.iterdir()) == []

    @pytest.mark.parametrize(
        "content",
        ["not a certificate\n", f"{PEM_BEGIN}\nAAAA\n-----END CERTIFICATE-----\n"],
    )
    def test_a_broken_file_is_logged_and_changes_nothing(
        self, content, trust_env, tmp_path, caplog
    ):
        import os

        broken = tmp_path / "ca.crt"
        broken.write_text(content)
        trust_env.setenv(EXTRA_CA_FILE_ENV, str(broken))
        with caplog.at_level(logging.ERROR, logger="shared.tls_trust"):
            assert install_extra_ca_trust(bundle_dir=str(tmp_path)) is None
        assert "Extra CA trust NOT installed" in caplog.text
        assert "SSL_CERT_FILE" not in os.environ


# ---------------------------------------------------------------------------
# The pods the provisioners build
# ---------------------------------------------------------------------------


@pytest.fixture
def trust_configmap(monkeypatch):
    monkeypatch.setenv(TRUST_CA_CONFIGMAP_ENV, "srw-trust-ca")
    monkeypatch.setenv(TRUST_CA_CONFIGMAP_KEY_ENV, "university.pem")


def _assert_trusts(container: dict, volumes: list[dict]) -> None:
    assert {"name": EXTRA_CA_FILE_ENV, "value": TRUST_CA_FILE} in container["env"]
    mounts = {m["name"]: m for m in container["volumeMounts"]}
    assert mounts[TRUST_CA_VOLUME] == {
        "name": TRUST_CA_VOLUME,
        "mountPath": TRUST_CA_MOUNT_DIR,
        "readOnly": True,
    }
    volume = {v["name"]: v for v in volumes}[TRUST_CA_VOLUME]
    assert volume["configMap"]["name"] == "srw-trust-ca"
    assert volume["configMap"]["items"] == [{"key": "university.pem", "path": "ca.crt"}]


def _assert_no_trust(container: dict, volumes: list[dict]) -> None:
    assert all(e["name"] != EXTRA_CA_FILE_ENV for e in container.get("env", []))
    assert all(v["name"] != TRUST_CA_VOLUME for v in volumes)


def _job_agent_manifest():
    from orchestrator.services.agent_provisioner import AgentProvisioner

    return AgentProvisioner()._build_pod_manifest(
        pod_name="srw-agent-j-test",
        purpose="job",
        thread_id=None,
        config_name="worker_base",
        cpu_request="100m",
        memory_request="256Mi",
        cpu_limit="1",
        memory_limit="2Gi",
    )


def _persistent_agent_manifest():
    from orchestrator.services.persistent_provisioner import PersistentProvisioner

    return PersistentProvisioner()._build_agent_pod_manifest(
        pod_name="persistent-abc123def456",
        thread_id="abc123def456-full-uuid",
        config_name="interactive",
        cpu_request="250m",
        memory_request="512Mi",
        cpu_limit="1000m",
        memory_limit="2Gi",
    )


def _workspace_manifest():
    from orchestrator.services.container_provisioner import ContainerProvisioner
    from orchestrator.services.workspace_lifecycle import WorkspaceOwner

    return ContainerProvisioner()._build_pod_manifest(
        pod_name="workspace-abc123",
        owner=WorkspaceOwner.job("abc123"),
        image="test:latest",
        cpu="500m",
        memory="1Gi",
        cpu_limit="2000m",
        memory_limit="4Gi",
    )


BUILDERS = {
    "job-agent": _job_agent_manifest,
    "persistent-agent": _persistent_agent_manifest,
    "workspace": _workspace_manifest,
}


class TestProvisionedPods:
    @pytest.mark.parametrize("kind", sorted(BUILDERS))
    def test_every_pod_mounts_the_operator_ca(self, kind, trust_configmap):
        manifest = BUILDERS[kind]()
        spec = manifest["spec"]
        _assert_trusts(spec["containers"][0], spec["volumes"])

    @pytest.mark.parametrize("kind", sorted(BUILDERS))
    def test_without_trust_no_pod_changes(self, kind, monkeypatch):
        monkeypatch.delenv(TRUST_CA_CONFIGMAP_ENV, raising=False)
        manifest = BUILDERS[kind]()
        spec = manifest["spec"]
        _assert_no_trust(spec["containers"][0], spec["volumes"])

    def test_source_defaults_the_key(self, monkeypatch):
        monkeypatch.setenv(TRUST_CA_CONFIGMAP_ENV, "cm")
        monkeypatch.delenv(TRUST_CA_CONFIGMAP_KEY_ENV, raising=False)
        assert TrustCaSource.from_env() == TrustCaSource(configmap="cm", key="ca.crt")

    def test_workspace_entrypoint_installs_it_into_the_system_store(self):
        body = (ROOT / "docker" / "workspace-entrypoint.sh").read_text()
        assert '"$SRW_EXTRA_CA_FILE"' in body
        assert "/usr/local/share/ca-certificates/srw-extra-ca.crt" in body
        assert "update-ca-certificates" in body
