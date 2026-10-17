"""The Keycloak client the local k3d gates log in through (never run here).

The orchestrator takes a Keycloak token as a Bearer credential only when it
is an access token bound to the API (``security/oidc.py``): ID tokens are
refused, and admin-cli's lightweight access token carries no ``sub`` and no
realm roles. Every gate client therefore carries a ``sub`` mapper and the
``orchestrator-api`` audience, the chart's default allowed audience: the
gates log in with access tokens the default binding accepts, with no
values-local allowance.
"""

from __future__ import annotations

import base64
import importlib.util
import io
import json
import re
import subprocess
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
ADMIN = "http://srw-keycloak:8080/admin/realms/srw"
CLIENT_UUID = "0b8f3a52-5d0c-4a54-9a43-6d6b2b0a9c11"
#: Gates whose accounts log in through the standing gate client.
GATES = (
    "k3d-cloud-mount-sidecar-gate.py",
    "k3d-connector-drivers-gate.py",
    "k3d-connector-matrix-gate.py",
    "k3d-credential-lease-gate.py",
    "k3d-service-driver-gate.py",
    "k3d-ssh-agent-connectors-gate.py",
)
#: Gates that make an OAuth client of their own per run.
PER_RUN_GATES = (
    "k3d-connector-resources-gate.py",
    "k3d-connector-secrets-gate.py",
    "k3d-connector-selection-gate.py",
    "k3d-custom-driver-gate.py",
    "k3d-folder-grants-gate.py",
    "k3d-git-swap-gate.py",
    "k3d-main-cloud-gate.py",
    "k3d-managed-mcp-gate.py",
    "k3d-user-root-connector-gate.py",
)
#: Scripts that logged ``test`` in through admin-cli and now use the helper.
HELPER_SCRIPTS = (
    "k3d-nextcloud-perf-gate.py",
    "manifests-cutover-k3d-gate.py",
    "manifests-k3d-gate.py",
    "parallel-subagents-k3d-gate.py",
    "stateless-resilience-k3d-gate.py",
)


def _jwt(claims: dict) -> str:
    def part(value: dict) -> str:
        raw = json.dumps(value).encode()
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

    return f"{part({'alg': 'RS256'})}.{part(claims)}.sig"


BOUND = {
    "typ": "Bearer",
    "azp": oauth.GATE_CLIENT,
    "aud": oauth.GATE_AUDIENCE,
    "sub": "u-1",
}
UNBOUND = {"typ": "Bearer", "azp": oauth.GATE_CLIENT, "email": "admin@localhost"}


class _Response(io.BytesIO):
    status = 200

    def __init__(self, body: bytes, status: int = 200) -> None:
        super().__init__(body)
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def _refusal(url: str, code: int, error: str = "") -> urllib.error.HTTPError:
    body = io.BytesIO(json.dumps({"error": error}).encode())
    return urllib.error.HTTPError(url, code, error, {}, body)


class _Keycloak:
    """Answers like Keycloak: the token endpoint, the admin token, the client
    listing and its protocol mappers. ``client`` is the realm's gate client
    (``None``: the realm does not know it)."""

    def __init__(
        self,
        *,
        client: dict | None,
        refusal: str = "invalid_client",
    ) -> None:
        self.client = client
        self.refusal = refusal
        self.requests: list[tuple[str, str, object]] = []

    def _bound(self) -> bool:
        names = {m["name"] for m in (self.client or {}).get("protocolMappers", [])}
        return {m["name"] for m in oauth.GATE_CLIENT_MAPPERS} <= names

    def open(self, request, data=None, timeout=None):
        url = getattr(request, "full_url", request)
        method = getattr(request, "get_method", lambda: "POST")()
        body = data if data is not None else getattr(request, "data", None)
        self.requests.append((method, url, body))
        if url == TOKEN_URL:
            form = dict(pair.split("=", 1) for pair in body.decode().split("&"))
            assert form["client_id"] == oauth.GATE_CLIENT
            if self.client is None or self.refusal != "invalid_client":
                raise _refusal(url, 401 if self.client is None else 400, self.refusal)
            claims = BOUND if self._bound() else UNBOUND
            return _Response(
                json.dumps({"access_token": _jwt(claims), "id_token": "id"}).encode()
            )
        if url.endswith("/realms/master/protocol/openid-connect/token"):
            return _Response(json.dumps({"access_token": "admin"}).encode())
        if url == f"{ADMIN}/clients?clientId={oauth.GATE_CLIENT}" and method == "GET":
            found = [] if self.client is None else [self.client]
            return _Response(json.dumps(found).encode())
        if url == f"{ADMIN}/clients" and method == "POST":
            created = json.loads(body)
            self.client = {"id": CLIENT_UUID, **created}
            return _Response(b"", 201)
        mappers = f"{ADMIN}/clients/{CLIENT_UUID}/protocol-mappers/models"
        if url == mappers and method == "GET":
            return _Response(
                json.dumps(self.client.get("protocolMappers", [])).encode()
            )
        if url == mappers and method == "POST":
            self.client.setdefault("protocolMappers", []).append(json.loads(body))
            return _Response(b"", 201)
        raise AssertionError(f"unexpected request {method} {url}")


def _program(monkeypatch) -> dict:
    monkeypatch.setenv("KEYCLOAK_URL", "http://srw-keycloak:8080")
    monkeypatch.setenv("KEYCLOAK_ADMIN_USER", "admin")
    monkeypatch.setenv("KEYCLOAK_ADMIN_PASSWORD", "admin-secret")
    monkeypatch.delenv("KEYCLOAK_REALM", raising=False)
    namespace: dict = {}
    exec(compile(oauth.LOGIN_SOURCE, "login", "exec"), namespace)
    return namespace


def _login(monkeypatch, keycloak: _Keycloak) -> str:
    return _program(monkeypatch)["gate_access_token"](keycloak, TOKEN_URL, "test", "pw")


def _claims(token: str) -> dict:
    part = token.split(".")[1]
    return json.loads(base64.urlsafe_b64decode(part + "=" * (-len(part) % 4)))


def _gate_client(*, mappers: list[dict], marker: str = oauth.GATE_MARKER) -> dict:
    return {
        "id": CLIENT_UUID,
        "clientId": oauth.GATE_CLIENT,
        "attributes": {"srw-gate": marker},
        "protocolMappers": mappers,
    }


def test_the_mappers_put_sub_and_the_api_audience_in_the_access_token():
    # orchestrator-api is the chart's default auth.oidc.allowedAudiences;
    # tests/test_oidc_token_binding.py checks the binding accepts the shape.
    assert oauth.GATE_AUDIENCE == "orchestrator-api"
    kinds = {m["protocolMapper"]: m["config"] for m in oauth.GATE_CLIENT_MAPPERS}
    assert kinds["oidc-sub-mapper"]["access.token.claim"] == "true"
    audience = kinds["oidc-audience-mapper"]
    assert audience["included.custom.audience"] == oauth.GATE_AUDIENCE
    assert audience["access.token.claim"] == "true"
    namespace: dict = {}
    exec(oauth.MAPPERS_SOURCE, namespace)
    assert namespace["_SRW_GATE_MAPPERS"] == list(oauth.GATE_CLIENT_MAPPERS)
    assert namespace["_SRW_GATE_AUDIENCE"] == oauth.GATE_AUDIENCE


def test_a_bound_client_logs_in_without_touching_the_admin_api(monkeypatch):
    keycloak = _Keycloak(client=_gate_client(mappers=list(oauth.GATE_CLIENT_MAPPERS)))
    token = _login(monkeypatch, keycloak)
    assert _claims(token)["aud"] == oauth.GATE_AUDIENCE
    assert [url for _method, url, _body in keycloak.requests] == [TOKEN_URL]


def test_a_realm_without_the_client_gets_it_with_the_mappers(monkeypatch):
    keycloak = _Keycloak(client=None)
    token = _login(monkeypatch, keycloak)
    assert _claims(token)["sub"] == "u-1"
    created = keycloak.client
    assert created["clientId"] == oauth.GATE_CLIENT
    assert created["publicClient"] and created["directAccessGrantsEnabled"]
    assert created["defaultClientScopes"] == ["profile", "email", "roles"]
    assert created["attributes"] == {"srw-gate": oauth.GATE_MARKER}
    assert created["protocolMappers"] == list(oauth.GATE_CLIENT_MAPPERS)
    urls = [url for _method, url, _body in keycloak.requests]
    assert urls[0] == urls[-1] == TOKEN_URL and len(urls) == 5


def test_a_client_made_before_the_mappers_gets_the_missing_ones(monkeypatch):
    subject = dict(oauth.GATE_CLIENT_MAPPERS[0])
    keycloak = _Keycloak(client=_gate_client(mappers=[subject]))
    token = _login(monkeypatch, keycloak)
    assert _claims(token)["aud"] == oauth.GATE_AUDIENCE
    added = [
        json.loads(body)
        for method, url, body in keycloak.requests
        if method == "POST" and url.endswith("/protocol-mappers/models")
    ]
    assert added == [oauth.GATE_CLIENT_MAPPERS[1]]


def test_a_client_that_is_not_the_gates_own_is_left_alone(monkeypatch):
    keycloak = _Keycloak(client=_gate_client(mappers=[], marker="someone-else"))
    with pytest.raises(SystemExit, match="not the gates' own"):
        _login(monkeypatch, keycloak)
    writes = [
        url
        for method, url, _body in keycloak.requests
        if method == "POST" and url.startswith(ADMIN)
    ]
    assert writes == []


def test_a_token_still_unbound_after_the_upgrade_fails_loudly(monkeypatch):
    keycloak = _Keycloak(client=_gate_client(mappers=[]))
    keycloak._bound = lambda: False  # Keycloak ignores the mappers
    with pytest.raises(SystemExit, match="still lacks sub"):
        _login(monkeypatch, keycloak)


def test_a_refused_password_is_no_reason_to_create_a_client(monkeypatch):
    keycloak = _Keycloak(
        client=_gate_client(mappers=list(oauth.GATE_CLIENT_MAPPERS)),
        refusal="invalid_grant",
    )
    with pytest.raises(urllib.error.HTTPError):
        _login(monkeypatch, keycloak)
    assert [url for _method, url, _body in keycloak.requests] == [TOKEN_URL]


def test_the_host_helper_runs_the_login_in_the_orchestrator_pod(monkeypatch):
    calls: list[dict] = []

    def run(argv, **kwargs):
        calls.append({"argv": argv, **kwargs})
        return subprocess.CompletedProcess(
            argv, 0, stdout=_jwt(BOUND) + "\n", stderr=""
        )

    monkeypatch.setattr(oauth.subprocess, "run", run)
    token = oauth.access_token(
        "test", "pw-not-in-argv", kubectl=["kubectl", "--context", "x"]
    )
    assert token == _jwt(BOUND)
    [call] = calls
    assert call["argv"][:3] == ["kubectl", "--context", "x"]
    assert call["argv"][3:9] == [
        "exec",
        "-i",
        oauth.ORCHESTRATOR,
        "-c",
        oauth.ORCHESTRATOR_CONTAINER,
        "--",
    ]
    assert "pw-not-in-argv" not in " ".join(call["argv"])
    envelope = json.loads(call["input"])
    assert envelope["password"] == "pw-not-in-argv"
    assert envelope["client"] == oauth.GATE_CLIENT
    program = call["argv"][-1]
    compile(program, "host-login", "exec")
    assert oauth.LOGIN_SOURCE in program and "gate_access_token(" in program


def test_the_host_helper_reports_a_failed_login_without_a_token(monkeypatch):
    def run(argv, **kwargs):
        return subprocess.CompletedProcess(argv, 1, stdout="", stderr="x\nHTTP 401\n")

    monkeypatch.setattr(oauth.subprocess, "run", run)
    with pytest.raises(RuntimeError, match="HTTP 401"):
        oauth.access_token("test", "pw")


@pytest.mark.parametrize("name", GATES)
def test_every_gate_on_the_standing_client_sends_its_access_token(name):
    source = (ROOT / "scripts" / name).read_text()
    module = "gate_oauth_" + name.replace("-", "_").removesuffix(".py")
    gate = _load(module, ROOT / "scripts" / name)
    assert oauth.LOGIN_SOURCE in gate._API_PROGRAM
    assert "gate_access_token(" in gate._API_PROGRAM.replace(oauth.LOGIN_SOURCE, "")
    assert "gate_id_token" not in source and '["id_token"]' not in source
    # The only admin-cli login left is the master realm's admin token.
    assert '"client_id": "admin-cli", "scope": "openid"' not in source


@pytest.mark.parametrize("name", PER_RUN_GATES)
def test_every_per_run_client_gets_the_mappers_and_logs_in_by_access_token(name):
    source = (ROOT / "scripts" / name).read_text()
    module = "gate_oauth_" + name.replace("-", "_").removesuffix(".py")
    gate = _load(module, ROOT / "scripts" / name)
    program = gate._KEYCLOAK_PROGRAM
    assert oauth.MAPPERS_SOURCE in program
    compile(program, name, "exec")
    create = program.split('action == "create-client"', 1)[1].split("elif", 1)[0]
    assert '"protocolMappers": _SRW_GATE_MAPPERS' in create
    assert '["id_token"]' not in source
    assert re.search(r'json\.load\(response\)\["access_token"\]', gate._API_PROGRAM)


@pytest.mark.parametrize("name", HELPER_SCRIPTS)
def test_scripts_that_used_admin_cli_log_in_through_the_gate_client(name):
    source = (ROOT / "scripts" / name).read_text()
    assert "id_token" not in source
    assert "client_id=admin-cli" not in source
    assert '"client_id": "admin-cli", "scope": "openid"' not in source
    assert '"client_id": "admin-cli",\n                    "scope"' not in source
    assert "k3d_gate_oauth.py" in source
    assert "gate_oauth.access_token(" in source or "gate_access_token(" in source


def test_the_lane_probe_mints_through_the_helper():
    source = (ROOT / "scripts" / "stateless-lane-probe.sh").read_text()
    assert "id_token" not in source and "client_id=admin-cli" not in source
    assert 'python3 "$ROOT/scripts/k3d_gate_oauth.py"' in source


def test_the_manifest_smoke_client_gets_the_mappers():
    source = (ROOT / "scripts" / "manifests-srw-k3d-smoke.py").read_text()
    assert '"protocolMappers": list(cutover.gate_oauth.GATE_CLIENT_MAPPERS)' in source
