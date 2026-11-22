"""A pinned context trusts exactly one endpoint certificate, with real handshakes."""
# ruff: noqa: F811  (the imported fixture is requested by name as a test argument)

from __future__ import annotations

import copy
import pickle
import shutil
import socket
import threading
import ssl
import subprocess

import httpx
import openai
import pytest
from langchain_openai import ChatOpenAI

from shared.tls_pinning import (
    NO_CHECK_TIME,
    CertificateChangedError,
    PinnedCertificateRejectedError,
    TlsPin,
    apresented_certificate_sha256,
    fingerprint_sha256,
    httpx_client_kwargs,
    pinned_ssl_context,
    presented_certificate_sha256,
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


@pytest.mark.asyncio
async def test_async_transport_maps_a_swap_to_certificate_changed(tls_certs):
    pin = _pin(tls_certs.leaf)
    with tls_server(tls_certs.swap, tls_certs.swap_key) as srv:
        async with httpx.AsyncClient(
            **httpx_client_kwargs(pin, asynchronous=True)
        ) as c:
            with pytest.raises(CertificateChangedError) as e:
                await c.get(
                    srv.url + "/models", headers={"Authorization": "Bearer SECRET"}
                )
        assert srv.requests == []
    assert e.value.pinned_sha256 == pin.sha256
    assert e.value.presented_sha256 == _pin(tls_certs.swap).sha256
    assert e.value.host == "127.0.0.1"
    assert e.value.port == srv.port
    assert str(e.value) == (
        f"TLS certificate changed for 127.0.0.1:{srv.port}: pinned SHA-256 "
        f"{pin.sha256}, server presents {_pin(tls_certs.swap).sha256}. "
        "An admin must re-trust it in Admin \u2192 Models."
    )
    assert isinstance(e.value.__cause__, httpx.ConnectError)


def test_sync_transport_works_and_maps_too(tls_certs):
    pin = _pin(tls_certs.leaf)
    with tls_server(tls_certs.leaf, tls_certs.leaf_key) as srv:
        with httpx.Client(**httpx_client_kwargs(pin, asynchronous=False)) as c:
            assert c.get(srv.url + "/models").status_code == 200
    with tls_server(tls_certs.swap, tls_certs.swap_key) as srv:
        with httpx.Client(**httpx_client_kwargs(pin, asynchronous=False)) as c:
            with pytest.raises(CertificateChangedError) as e:
                c.get(srv.url + "/models", headers={"Authorization": "Bearer S"})
        assert srv.requests == []
    assert e.value.presented_sha256 == _pin(tls_certs.swap).sha256


def test_no_pin_means_no_transport():
    assert httpx_client_kwargs(None, asynchronous=True) == {}
    assert httpx_client_kwargs(None, asynchronous=False) == {}


def test_unreadable_presented_certificate_uses_the_fallback_wording(tls_certs):
    # Nothing listens on this port by the time the lookup runs.
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    assert presented_certificate_sha256("127.0.0.1", port, timeout=1) is None
    err = CertificateChangedError("127.0.0.1", port, _pin(tls_certs.leaf).sha256, None)
    assert str(err) == (
        f"TLS certificate changed for 127.0.0.1:{port}: pinned SHA-256 "
        f"{_pin(tls_certs.leaf).sha256}, server presents a different "
        "certificate. An admin must re-trust it in Admin \u2192 Models."
    )


@pytest.mark.asyncio
async def test_presented_certificate_lookup_sends_nothing(tls_certs):
    with tls_server(tls_certs.swap, tls_certs.swap_key) as srv:
        want = _pin(tls_certs.swap).sha256
        assert presented_certificate_sha256("127.0.0.1", srv.port) == want
        assert await apresented_certificate_sha256("127.0.0.1", srv.port) == want
        assert srv.requests == []


@pytest.mark.asyncio
async def test_openai_sdk_through_the_pin_and_the_error_cause(tls_certs):
    pin = _pin(tls_certs.leaf)

    def client(url):
        return openai.AsyncOpenAI(
            base_url=url,
            api_key="k",
            max_retries=0,
            http_client=openai.DefaultAsyncHttpxClient(
                **httpx_client_kwargs(pin, asynchronous=True)
            ),
        )

    with tls_server(tls_certs.leaf, tls_certs.leaf_key) as srv:
        c = client(srv.url)
        assert [m.id async for m in c.models.list()] == ["m1"]
        emb = await c.embeddings.create(model="m1", input="x")
        assert emb.data[0].embedding == [0.1, 0.2]
        await c.close()
    with tls_server(tls_certs.swap, tls_certs.swap_key) as srv:
        with pytest.raises(openai.APIConnectionError) as e:
            bad = client(srv.url)
            try:
                await bad.models.list()
            finally:
                await bad.close()
        assert isinstance(e.value.__cause__, CertificateChangedError)
        assert srv.requests == []


@pytest.mark.asyncio
async def test_chat_openai_invoke_and_ainvoke_through_the_pin(tls_certs):
    pin = _pin(tls_certs.leaf)

    def llm(url):
        sync = httpx.Client(**httpx_client_kwargs(pin, asynchronous=False))
        asyn = httpx.AsyncClient(**httpx_client_kwargs(pin, asynchronous=True))
        model = ChatOpenAI(
            base_url=url,
            api_key="k",
            model="m1",
            max_retries=0,
            http_client=sync,
            http_async_client=asyn,
        )
        return model, sync, asyn

    with tls_server(tls_certs.leaf, tls_certs.leaf_key) as srv:
        m, sync, asyn = llm(srv.url)
        try:
            assert m.invoke("hi").content == "ok"
            assert (await m.ainvoke("hi")).content == "ok"
        finally:
            sync.close()
            await asyn.aclose()
    with tls_server(tls_certs.swap, tls_certs.swap_key) as srv:
        m, sync, asyn = llm(srv.url)
        try:
            with pytest.raises(openai.APIConnectionError) as e:
                m.invoke("hi")
            assert isinstance(e.value.__cause__, CertificateChangedError)
            with pytest.raises(openai.APIConnectionError) as e2:
                await m.ainvoke("hi")
            assert isinstance(e2.value.__cause__, CertificateChangedError)
        finally:
            sync.close()
            await asyn.aclose()
        assert srv.requests == []


def test_a_refused_connection_is_not_certificate_changed(tls_certs):
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    pin = _pin(tls_certs.leaf)
    with httpx.Client(**httpx_client_kwargs(pin, asynchronous=False)) as c:
        with pytest.raises(httpx.ConnectError) as e:
            c.get(f"https://127.0.0.1:{port}/v1/models")
    assert not isinstance(e.value, CertificateChangedError)


def test_a_pin_the_server_presents_but_openssl_rejects_is_not_reported_as_changed(
    tmp_path,
):
    if shutil.which("openssl") is None:
        pytest.skip("openssl CLI not available")
    subprocess.run(
        [
            "openssl", "req", "-x509", "-newkey", "rsa:1024", "-nodes",
            "-keyout", "weak.key", "-out", "weak.pem", "-days", "30",
            "-subj", "/CN=127.0.0.1",
        ],
        cwd=tmp_path, check=True, capture_output=True,
    )  # fmt: skip
    pin = _pin(tmp_path / "weak.pem")
    # The shared fixture server cannot load a weak key, so lower its level here.
    sctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    sctx.set_ciphers("DEFAULT:@SECLEVEL=0")
    sctx.load_cert_chain(str(tmp_path / "weak.pem"), str(tmp_path / "weak.key"))
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(5)
    port = listener.getsockname()[1]
    completed: list[int] = []

    def serve() -> None:
        while True:
            try:
                conn, _ = listener.accept()
            except OSError:
                return
            try:
                conn.settimeout(5)
                # The certificate lookup completes a handshake but sends nothing.
                with sctx.wrap_socket(conn, server_side=True) as tls:
                    data = tls.recv(1)
                    if data:
                        completed.append(data)
            except (OSError, ssl.SSLError):
                conn.close()

    th = threading.Thread(target=serve, daemon=True)
    th.start()
    try:
        with httpx.Client(**httpx_client_kwargs(pin, asynchronous=False)) as c:
            with pytest.raises(PinnedCertificateRejectedError) as e:
                c.get(
                    f"https://127.0.0.1:{port}/v1/models",
                    headers={"Authorization": "Bearer S"},
                )
    finally:
        listener.close()
        th.join(timeout=5)
    assert completed == []
    assert not isinstance(e.value, CertificateChangedError)
    assert e.value.sha256 == pin.sha256
    assert e.value.port == port
    assert "too weak" in e.value.reason
    assert str(e.value) == (
        f"TLS handshake with 127.0.0.1:{port} failed although the server "
        f"presents the pinned certificate (SHA-256 {pin.sha256}): "
        f"{e.value.reason}. SRW cannot use this certificate; the server needs "
        "a different one."
    )


@pytest.mark.parametrize(
    "err",
    [
        CertificateChangedError("h", 443, "AA", "BB"),
        CertificateChangedError("h", 443, "AA", None),
        PinnedCertificateRejectedError("h", 443, "AA", "key too weak"),
    ],
)
def test_errors_are_copyable_and_picklable(err):
    for clone in (copy.copy(err), pickle.loads(pickle.dumps(err))):
        assert type(clone) is type(err)
        assert str(clone) == str(err)
        assert vars(clone).keys() >= {"host", "port"}
        for k in (
            "host",
            "port",
            "pinned_sha256",
            "presented_sha256",
            "sha256",
            "reason",
        ):
            assert getattr(clone, k, None) == getattr(err, k, None)


@pytest.mark.asyncio
async def test_an_async_refused_connection_is_not_certificate_changed(tls_certs):
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    pin = _pin(tls_certs.leaf)
    async with httpx.AsyncClient(**httpx_client_kwargs(pin, asynchronous=True)) as c:
        with pytest.raises(httpx.ConnectError) as e:
            await c.get(f"https://127.0.0.1:{port}/v1/models")
    assert type(e.value) is httpx.ConnectError


def test_a_non_verification_tls_failure_stays_a_plain_connect_error(tls_certs):
    import http.server

    class H(http.server.BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            self.send_response(200)
            self.end_headers()

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
    srv.daemon_threads = True
    th = threading.Thread(target=srv.serve_forever, daemon=True)
    th.start()
    try:
        pin = _pin(tls_certs.leaf)
        with httpx.Client(**httpx_client_kwargs(pin, asynchronous=False)) as c:
            with pytest.raises(httpx.ConnectError) as e:
                c.get(f"https://127.0.0.1:{srv.server_port}/v1/models")
        assert type(e.value) is httpx.ConnectError
    finally:
        srv.shutdown()
        srv.server_close()
        th.join(timeout=5)
