"""Project group sync keeps a Keycloak context path during real client requests."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import pytest

from orchestrator.services.keycloak_admin import KeycloakGroupSync


@pytest.mark.parametrize("context_path", ["", "/identity", "/identity/"])
def test_group_sync_preserves_keycloak_context_path(monkeypatch, context_path):
    paths = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            paths.append(self.path.split("?", 1)[0])
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(
                b'{"access_token":"test","expires_in":300,"token_type":"Bearer"}'
            )

        def do_GET(self):
            paths.append(self.path.split("?", 1)[0])
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b"[]")

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        monkeypatch.setenv(
            "KEYCLOAK_URL", f"http://127.0.0.1:{server.server_port}{context_path}"
        )
        monkeypatch.setenv("KEYCLOAK_ADMIN_USER", "test")
        monkeypatch.setenv("KEYCLOAK_ADMIN_PASSWORD", "test")
        monkeypatch.setenv("KEYCLOAK_REALM", "srw")
        assert KeycloakGroupSync()._init_admin_sync()
        prefix = context_path.rstrip("/")
        assert paths == [
            f"{prefix}/realms/master/protocol/openid-connect/token",
            f"{prefix}/admin/realms/srw/roles",
        ]
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
