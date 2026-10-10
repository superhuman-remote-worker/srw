"""The Keycloak client the local k3d gates log in through (never run here).

An ID token of the srw realm's admin-cli client carries no e-mail and no
realm roles, and a login recomputes ``is_admin`` from the token: every gate
logs its accounts in through ``srw-k3d-gate`` instead, created once when
the realm does not know it.
"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
import urllib.error
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # a gate's dataclasses look their module up
    spec.loader.exec_module(module)
    return module


oauth = _load("k3d_gate_oauth_under_test", ROOT / "scripts" / "k3d_gate_oauth.py")
TOKEN_URL = "http://srw-keycloak:8080/realms/srw/protocol/openid-connect/token"
GATES = (
    "k3d-cloud-mount-sidecar-gate.py",
    "k3d-connector-drivers-gate.py",
    "k3d-connector-matrix-gate.py",
    "k3d-credential-lease-gate.py",
    "k3d-service-driver-gate.py",
    "k3d-ssh-agent-connectors-gate.py",
)


class _Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def _refusal(url: str, code: int, error: str) -> urllib.error.HTTPError:
    body = io.BytesIO(json.dumps({"error": error}).encode())
    return urllib.error.HTTPError(url, code, error, {}, body)


class _Opener:
    """Answers like Keycloak: the token endpoint, the admin token and the
    client listing. ``known`` says whether the realm has the gate client."""

    def __init__(self, *, known: bool, refusal: str = "invalid_client") -> None:
        self.known = known
        self.refusal = refusal
        self.requests: list[tuple[str, dict]] = []

    def open(self, request, data=None, timeout=None):
        url = getattr(request, "full_url", request)
        body = data if data is not None else getattr(request, "data", None)
        self.requests.append((url, {"body": body}))
        if url == TOKEN_URL:
            form = dict(
                pair.split("=", 1) for pair in (body or b"").decode().split("&")
            )
            assert form["client_id"] == oauth.GATE_CLIENT
            if not self.known:
                raise _refusal(url, 401, self.refusal)
            return _Response(json.dumps({"id_token": "id.token.sig"}).encode())
        if url.endswith("/realms/master/protocol/openid-connect/token"):
            return _Response(json.dumps({"access_token": "admin"}).encode())
        if url.endswith("/admin/realms/srw/clients"):
            created = json.loads(request.data)
            assert created["clientId"] == oauth.GATE_CLIENT
            assert created["publicClient"] and created["directAccessGrantsEnabled"]
            assert created["defaultClientScopes"] == ["profile", "email", "roles"]
            assert created["attributes"] == {"srw-gate": oauth.GATE_MARKER}
            self.known = True
            return _Response(b"")
        raise AssertionError(f"unexpected request {url}")


def _program(monkeypatch) -> dict:
    monkeypatch.setenv("KEYCLOAK_URL", "http://srw-keycloak:8080")
    monkeypatch.setenv("KEYCLOAK_ADMIN_USER", "admin")
    monkeypatch.setenv("KEYCLOAK_ADMIN_PASSWORD", "admin-secret")
    monkeypatch.delenv("KEYCLOAK_REALM", raising=False)
    namespace: dict = {}
    exec(compile(oauth.LOGIN_SOURCE, "login", "exec"), namespace)
    return namespace


def test_a_known_client_logs_in_without_touching_the_admin_api(monkeypatch):
    opener = _Opener(known=True)
    token = _program(monkeypatch)["gate_id_token"](opener, TOKEN_URL, "test", "pw")
    assert token == "id.token.sig"
    assert [url for url, _ in opener.requests] == [TOKEN_URL]


def test_a_realm_without_the_client_gets_it_once_then_logs_in(monkeypatch):
    opener = _Opener(known=False)
    token = _program(monkeypatch)["gate_id_token"](opener, TOKEN_URL, "test", "pw")
    assert token == "id.token.sig"
    urls = [url for url, _ in opener.requests]
    assert urls[0] == urls[-1] == TOKEN_URL and len(urls) == 4
    assert urls[1].endswith("/realms/master/protocol/openid-connect/token")


def test_a_refused_password_is_no_reason_to_create_a_client(monkeypatch):
    opener = _Opener(known=False, refusal="invalid_grant")
    with pytest.raises(urllib.error.HTTPError):
        _program(monkeypatch)["gate_id_token"](opener, TOKEN_URL, "test", "bad")
    assert [url for url, _ in opener.requests] == [TOKEN_URL]


@pytest.mark.parametrize("name", GATES)
def test_every_gate_that_logged_in_through_admin_cli_uses_the_gate_client(name):
    source = (ROOT / "scripts" / name).read_text()
    module = "gate_oauth_" + name.replace("-", "_").removesuffix(".py")
    gate = _load(module, ROOT / "scripts" / name)
    assert oauth.LOGIN_SOURCE in gate._API_PROGRAM
    assert "gate_id_token(" in gate._API_PROGRAM.replace(oauth.LOGIN_SOURCE, "")
    # The only admin-cli login left is the master realm's admin token.
    assert '"client_id": "admin-cli", "scope": "openid"' not in source
