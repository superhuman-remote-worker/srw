"""Certificates and a TLS server for the certificate-pinning tests.

The certificates are made with the ``openssl`` CLI, using the commands of the
2026-11-06 spike: ``leaf`` is a true X.509 v1 certificate without extensions
(like the university router's), so no CA-based hostname check can pass it.
Import ``tls_certs`` and ``tls_server`` into the test module:
``from tests.tls_pin_fixtures import tls_certs, tls_server``. The helpers
:func:`pin_of` and :func:`caused_by` are shared by the pin suites.
"""

from __future__ import annotations

import json
import shutil
import ssl
import subprocess
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterator, Mapping, Optional

import pytest

from shared.tls_pinning import TlsPin


def pin_of(path: Path) -> TlsPin:
    """The pin of the PEM certificate file at ``path``."""
    return TlsPin.from_pem(path.read_text())


def caused_by(exc: BaseException | None, cls: type) -> bool:
    """Whether ``cls`` is anywhere in ``exc``'s cause/context chain.

    SDK and LangChain releases add wrapper layers (langchain-openai 1.6+ wraps
    openai's ``APIConnectionError`` once more), so never pin a chain's depth.
    """
    seen: set[int] = set()
    cur = exc
    while cur is not None and id(cur) not in seen:
        if isinstance(cur, cls):
            return True
        seen.add(id(cur))
        cur = cur.__cause__ or cur.__context__
    return False


_MIN_CNF = """[req]
distinguished_name = dn
prompt = no
[dn]
CN = 127.0.0.1
emailAddress = jfink@example.org
"""

_GOOD_EXT = "subjectAltName=IP:127.0.0.1\n"


@dataclass(frozen=True)
class CertSet:
    ca: Path
    leaf: Path
    leaf_key: Path
    expired: Path
    swap: Path
    swap_key: Path
    self_signed: Path
    self_key: Path
    good: Path
    good_key: Path


def _openssl(*args: str, cwd: Path) -> None:
    subprocess.run(["openssl", *args], cwd=cwd, check=True, capture_output=True)


@pytest.fixture(scope="session")
def tls_certs(tmp_path_factory: pytest.TempPathFactory) -> CertSet:
    if shutil.which("openssl") is None:
        pytest.skip("openssl CLI not available")
    d = tmp_path_factory.mktemp("tls-pin-certs")
    (d / "min.cnf").write_text(_MIN_CNF)
    (d / "good.ext").write_text(_GOOD_EXT)
    _openssl(
        "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", "ca.key",
        "-out", "ca.pem", "-days", "3650", "-subj", "/CN=pin test CA", cwd=d,
    )  # fmt: skip
    _openssl("genrsa", "-out", "leaf.key", "2048", cwd=d)
    v1 = ["req", "-config", "min.cnf", "-x509v1", "-CA", "ca.pem", "-CAkey", "ca.key"]
    _openssl(*v1, "-key", "leaf.key", "-days", "3650", "-out", "leaf.pem", cwd=d)
    _openssl(
        *v1, "-key", "leaf.key", "-not_before", "20200101000000Z",
        "-not_after", "20210101000000Z", "-out", "expired.pem", cwd=d,
    )  # fmt: skip
    _openssl("genrsa", "-out", "swap.key", "2048", cwd=d)
    _openssl(*v1, "-key", "swap.key", "-days", "3650", "-out", "swap.pem", cwd=d)
    _openssl(
        "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", "self.key",
        "-out", "self.pem", "-days", "3650", "-subj", "/CN=model-box.local", cwd=d,
    )  # fmt: skip
    _openssl("genrsa", "-out", "good.key", "2048", cwd=d)
    _openssl(
        "req", "-new", "-key", "good.key", "-subj", "/CN=127.0.0.1",
        "-out", "good.csr", cwd=d,
    )  # fmt: skip
    _openssl(
        "x509", "-req", "-in", "good.csr", "-CA", "ca.pem", "-CAkey", "ca.key",
        "-CAcreateserial", "-days", "3650", "-extfile", "good.ext",
        "-out", "good.pem", cwd=d,
    )  # fmt: skip
    return CertSet(
        ca=d / "ca.pem",
        leaf=d / "leaf.pem",
        leaf_key=d / "leaf.key",
        expired=d / "expired.pem",
        swap=d / "swap.pem",
        swap_key=d / "swap.key",
        self_signed=d / "self.pem",
        self_key=d / "self.key",
        good=d / "good.pem",
        good_key=d / "good.key",
    )


#: A route value is a JSON body, or a callable ``(body: bytes) -> (status, json)``.
Route = Any

_DEFAULT_ROUTES: dict[tuple[str, str], Route] = {
    ("GET", "/v1/models"): {
        "object": "list",
        "data": [{"id": "m1", "object": "model"}],
    },
    ("POST", "/v1/embeddings"): {
        "object": "list",
        "data": [{"object": "embedding", "index": 0, "embedding": [0.1, 0.2]}],
        "model": "m1",
        "usage": {"prompt_tokens": 1, "total_tokens": 1},
    },
    ("POST", "/v1/rerank"): {"results": [{"index": 0, "relevance_score": 0.9}]},
    ("POST", "/v1/chat/completions"): {
        "id": "c1",
        "object": "chat.completion",
        "created": 0,
        "model": "m1",
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": "ok"},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
    },
}


@dataclass
class TlsServer:
    port: int
    requests: list[dict] = field(default_factory=list)

    @property
    def url(self) -> str:
        return f"https://127.0.0.1:{self.port}/v1"


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False

    def __init__(self, ctx: ssl.SSLContext, routes: dict, record: list[dict]):
        self.ctx = ctx
        self.routes = routes
        self.record = record
        super().__init__(("127.0.0.1", 0), _Handler)

    def finish_request(self, request, client_address):  # type: ignore[override]
        # Handshake in the worker thread, so one refused or stalled client
        # never blocks the accept loop. A refused handshake records nothing.
        try:
            request.settimeout(10)
            tls = self.ctx.wrap_socket(
                request, server_side=True, do_handshake_on_connect=False
            )
        except OSError:
            return
        try:
            tls.do_handshake()
        except (OSError, ssl.SSLError):
            tls.close()
            return
        try:
            _Handler(tls, client_address, self)
        finally:
            tls.close()

    def handle_error(self, request, client_address):  # type: ignore[override]
        pass  # a client dropping the connection is not a test failure


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server: _Server

    def log_message(self, format: str, *args: Any) -> None:
        pass

    def _serve(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        path = self.path.split("?", 1)[0]
        self.server.record.append(
            {
                "method": self.command,
                "path": path,
                "authorization": self.headers.get("Authorization"),
            }
        )
        route = self.server.routes.get((self.command, path))
        if route is None:
            status, payload = 404, {"error": "not found"}
        elif callable(route):
            status, payload = route(body)
        else:
            status, payload = 200, route
        data = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(data)
        self.close_connection = True

    do_GET = do_POST = _serve


@contextmanager
def tls_server(
    cert: Path,
    key: Path,
    routes: Optional[Mapping[tuple[str, str] | str, Route]] = None,
) -> Iterator[TlsServer]:
    """Serve ``cert`` over HTTPS on 127.0.0.1 with the OpenAI-shaped routes.

    ``routes`` maps ``(method, path)`` (or a bare path, meaning GET and POST) to
    a JSON body or a ``callable(body: bytes) -> (status, json)``, merged over
    the defaults.
    """
    merged = dict(_DEFAULT_ROUTES)
    for k, v in (routes or {}).items():
        if isinstance(k, str):
            merged[("GET", k)] = v
            merged[("POST", k)] = v
        else:
            merged[k] = v
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(str(cert), str(key))
    record: list[dict] = []
    server = _Server(ctx, merged, record)
    state = TlsServer(port=server.server_address[1], requests=record)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
