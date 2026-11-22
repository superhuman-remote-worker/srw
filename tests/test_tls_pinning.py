"""A pinned context trusts exactly one endpoint certificate, with real handshakes."""
# ruff: noqa: F811  (the imported fixture is requested by name as a test argument)

from __future__ import annotations

import socket
import ssl
import subprocess

import pytest

from shared.tls_pinning import (
    NO_CHECK_TIME,
    TlsPin,
    fingerprint_sha256,
    pinned_ssl_context,
)
from tests.tls_pin_fixtures import tls_certs, tls_server  # noqa: F401


def _pin(path) -> TlsPin:
    return TlsPin.from_pem(path.read_text())


def _handshake(pin: TlsPin, port: int) -> bool:
    ctx = pinned_ssl_context(pin)
    with socket.create_connection(("127.0.0.1", port), timeout=5) as raw:
        try:
            with ctx.wrap_socket(raw, server_hostname="127.0.0.1"):
                return True
        except ssl.SSLCertVerificationError:
            return False


def test_fingerprint_matches_openssl(tls_certs):
    want = (
        subprocess.run(
            [
                "openssl",
                "x509",
                "-in",
                tls_certs.leaf,
                "-noout",
                "-fingerprint",
                "-sha256",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        .stdout.strip()
        .split("=", 1)[1]
    )
    got = _pin(tls_certs.leaf).sha256
    assert got == want and len(got) == 95


def test_fingerprint_format():
    assert fingerprint_sha256(b"") == (
        "E3:B0:C4:42:98:FC:1C:14:9A:FB:F4:C8:99:6F:B9:24:"
        "27:AE:41:E4:64:9B:93:4C:A4:95:99:1B:78:52:B8:55"
    )


def test_wire_round_trip_is_one_line(tls_certs):
    pin = _pin(tls_certs.leaf)
    assert "\n" not in pin.wire and TlsPin.from_wire(pin.wire) == pin
    assert TlsPin.from_pem(pin.pem) == pin


def test_from_pem_accepts_exactly_one_certificate(tls_certs):
    one = tls_certs.leaf.read_text()
    with pytest.raises(ValueError):
        TlsPin.from_pem("")
    with pytest.raises(ValueError):
        TlsPin.from_pem(one + one)


def test_from_env_reads_the_prefix_and_rejects_garbage(tls_certs):
    pin = _pin(tls_certs.leaf)
    assert TlsPin.from_env("EMBEDDING", {"EMBEDDING_TLS_PINNED_CERT": pin.wire}) == pin
    assert TlsPin.from_env("EMBEDDING", {"EMBEDDING_TLS_PINNED_CERT": ""}) is None
    assert TlsPin.from_env("EMBEDDING", {}) is None
    with pytest.raises(ValueError, match="EMBEDDING_TLS_PINNED_CERT"):
        TlsPin.from_env("EMBEDDING", {"EMBEDDING_TLS_PINNED_CERT": "not-base64!"})
    with pytest.raises(ValueError, match="EMBEDDING_TLS_PINNED_CERT"):
        TlsPin.from_env("EMBEDDING", {"EMBEDDING_TLS_PINNED_CERT": "QUJD"})


@pytest.mark.parametrize(
    "served,accepted",
    [("leaf", True), ("swap", False), ("good", False), ("self_signed", False)],
)
def test_pinned_context_accepts_only_the_pinned_leaf(tls_certs, served, accepted):
    pin = _pin(tls_certs.leaf)
    cert = getattr(tls_certs, served)
    key = getattr(tls_certs, {"self_signed": "self_key"}.get(served, f"{served}_key"))
    with tls_server(cert, key) as server:
        assert _handshake(pin, server.port) is accepted


def test_expired_pin_still_connects(tls_certs):
    pin = _pin(tls_certs.expired)
    with tls_server(tls_certs.expired, tls_certs.leaf_key) as server:
        assert _handshake(pin, server.port) is True


def test_pinned_context_ignores_ssl_cert_file(tls_certs, monkeypatch):
    monkeypatch.setenv("SSL_CERT_FILE", str(tls_certs.ca))
    pin = _pin(tls_certs.leaf)
    with tls_server(tls_certs.good, tls_certs.good_key) as server:
        assert _handshake(pin, server.port) is False


def test_pinned_context_has_no_hostname_check_and_partial_chain(tls_certs):
    ctx = pinned_ssl_context(_pin(tls_certs.leaf))
    assert ctx.check_hostname is False and ctx.verify_mode == ssl.CERT_REQUIRED
    assert int(ctx.verify_flags) & ssl.VERIFY_X509_PARTIAL_CHAIN
    assert int(ctx.verify_flags) & NO_CHECK_TIME == NO_CHECK_TIME
    assert not int(ctx.verify_flags) & ssl.VERIFY_X509_STRICT


def test_each_call_builds_a_fresh_context(tls_certs):
    pin = _pin(tls_certs.leaf)
    assert pinned_ssl_context(pin) is not pinned_ssl_context(pin)


def test_refused_handshake_leaves_no_request_and_server_survives(tls_certs):
    with tls_server(tls_certs.swap, tls_certs.swap_key) as server:
        assert _handshake(_pin(tls_certs.leaf), server.port) is False
        assert server.requests == []
        assert _handshake(_pin(tls_certs.swap), server.port) is True
