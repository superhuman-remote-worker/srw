"""Keycloak token binding: a realm-signed token is not enough.

Every client of the realm (Gitea, Nextcloud, OpenCloud's service account and
its user-impersonation tokens in workspaces, admin-cli) gets tokens with the
same signature and issuer. ``validate_token`` accepts only an access token
(``typ: Bearer``) issued to an allowed client (``azp``) or for an allowed
audience (``aud``); ID and logout tokens must name the BFF client in ``aud``
(security audit 2026-08-27, ``security/oidc.py`` token binding).

The claim shapes below are the ones a live k3d realm issues: a public or
confidential client's access token carries ``azp`` and no ``aud`` (or only
``account``); its ID token carries ``aud`` = ``azp`` = the client and
``typ: ID``.
"""

from __future__ import annotations

import importlib.util
import logging
import time
from pathlib import Path
from types import SimpleNamespace

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import HTTPException

from orchestrator.security import auth, oidc
from orchestrator.security.oidc import (
    BACKCHANNEL_LOGOUT_EVENT,
    OIDCValidator,
    TokenBinding,
    parse_name_list,
)

ISSUER_URL = "https://auth.example.test"
REALM = "srw"
BFF = "cockpit-bff"


@pytest.fixture(scope="module")
def keypair():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture
def make_validator(monkeypatch, keypair):
    monkeypatch.setenv("KEYCLOAK_URL", "http://keycloak.internal:8080")
    monkeypatch.setenv("KEYCLOAK_ISSUER_URL", ISSUER_URL)
    monkeypatch.setenv("KEYCLOAK_REALM", REALM)
    monkeypatch.setenv("KEYCLOAK_CLIENT_ID", BFF)
    monkeypatch.delenv(oidc.OIDC_ALLOWED_CLIENTS_ENV, raising=False)
    monkeypatch.delenv(oidc.OIDC_ALLOWED_AUDIENCES_ENV, raising=False)
    signing_key = SimpleNamespace(key=keypair.public_key())

    def build(**kwargs) -> OIDCValidator:
        validator = OIDCValidator(**kwargs)
        validator._jwks_client = SimpleNamespace(
            get_signing_key_from_jwt=lambda _token: signing_key
        )
        return validator

    return build


@pytest.fixture
def validator(make_validator):
    return make_validator()


@pytest.fixture
def sign(keypair):
    def _sign(claims: dict, *, drop: tuple[str, ...] = ()) -> str:
        now = int(time.time())
        payload = {
            "iss": f"{ISSUER_URL}/realms/{REALM}",
            "iat": now,
            "exp": now + 300,
            "sub": "4b4f8f39-0000-4000-8000-000000000001",
            "sid": "session-1",
        }
        payload.update(claims)
        for key in drop:
            payload.pop(key, None)
        return jwt.encode(payload, keypair, algorithm="RS256")

    return _sign


def access(azp: str | None, aud=None, **extra) -> dict:
    claims = {"typ": "Bearer", "scope": "openid email profile", **extra}
    if azp is not None:
        claims["azp"] = azp
    if aud is not None:
        claims["aud"] = aud
    return claims


class TestAccessTokenBinding:
    @pytest.mark.parametrize(
        "claims",
        [
            pytest.param(access(BFF), id="bff-client-no-aud"),
            pytest.param(access("cockpit"), id="spa-client-no-aud"),
            pytest.param(access("cockpit", aud="account"), id="spa-client-aud-account"),
            pytest.param(
                access("cockpit", aud=["account", "broker"]), id="spa-client-aud-list"
            ),
            pytest.param(
                access("srw-cli", aud="orchestrator-api"), id="other-client-api-aud"
            ),
            pytest.param(
                access("srw-cli", aud=["account", "orchestrator-api"]),
                id="other-client-api-aud-in-list",
            ),
            pytest.param(access(None, aud="orchestrator-api"), id="no-azp-api-aud"),
            pytest.param(access(BFF, typ="bearer"), id="typ-case-insensitive"),
        ],
    )
    def test_accepted(self, validator, sign, claims):
        decoded = validator.validate_token(sign(claims))
        assert decoded is not None
        assert decoded["sub"]

    @pytest.mark.parametrize(
        "claims",
        [
            pytest.param(access("gitea"), id="gitea-client"),
            pytest.param(
                access("nextcloud", aud="account"), id="nextcloud-aud-account"
            ),
            pytest.param(
                access("opencloud-orchestrator", aud=["account"]),
                id="opencloud-impersonation-token",
            ),
            pytest.param(access("admin-cli"), id="admin-cli-direct-grant"),
            pytest.param(access("OpenCloudDesktop"), id="opencloud-desktop-public"),
            pytest.param(access("mcp-server"), id="mcp-server-client"),
            pytest.param(access("Cockpit"), id="client-id-is-case-sensitive"),
            pytest.param(
                access("gitea", aud=["account", "broker"]), id="aud-list-none-allowed"
            ),
            pytest.param(access(None), id="no-azp-no-aud"),
            pytest.param(access(["cockpit"]), id="azp-not-a-string"),
            pytest.param(access("gitea", aud=[["orchestrator-api"]]), id="aud-nested"),
            pytest.param(access("gitea", aud=42), id="aud-not-a-string"),
            # An ID token for an allowed client is still not an access token.
            pytest.param(
                {"typ": "ID", "azp": BFF, "aud": BFF, "at_hash": "x"},
                id="bff-id-token",
            ),
            pytest.param(
                {"typ": "ID", "azp": "cockpit", "aud": "cockpit"}, id="spa-id"
            ),
            pytest.param(access(BFF, typ="Refresh"), id="refresh-token"),
            pytest.param(access(BFF, typ="Logout"), id="logout-token"),
            pytest.param({"azp": BFF, "scope": "openid"}, id="no-typ"),
            pytest.param(access(BFF, typ=None), id="typ-null"),
        ],
    )
    def test_rejected(self, validator, sign, claims):
        assert validator.validate_token(sign(claims)) is None

    def test_missing_exp_is_rejected(self, validator, sign):
        assert validator.validate_token(sign(access(BFF), drop=("exp",))) is None

    def test_expired_is_rejected(self, validator, sign):
        assert (
            validator.validate_token(sign(access(BFF, exp=int(time.time()) - 60)))
            is None
        )

    def test_wrong_issuer_is_rejected(self, validator, sign):
        token = sign(access(BFF, iss="https://auth.example.test/realms/other"))
        assert validator.validate_token(token) is None

    def test_a_rejection_logs_the_client_and_never_the_subject(
        self, validator, sign, caplog
    ):
        with caplog.at_level(logging.WARNING, logger="orchestrator.security.oidc"):
            assert (
                validator.validate_token(sign(access("gitea", aud="account"))) is None
            )
        record = next(r for r in caplog.records if "rejected" in r.getMessage())
        message = record.getMessage()
        assert "'gitea'" in message and "account" in message
        assert "4b4f8f39" not in message and "session-1" not in message

    def test_an_id_token_rejection_names_its_type(self, validator, sign, caplog):
        with caplog.at_level(logging.WARNING, logger="orchestrator.security.oidc"):
            validator.validate_token(sign({"typ": "ID", "azp": BFF, "aud": BFF}))
        assert any("typ='ID'" in r.getMessage() for r in caplog.records)


class TestConfiguration:
    def test_defaults_are_the_bundled_realm(self, validator):
        assert validator.binding.allowed_clients == {BFF, "cockpit"}
        assert validator.binding.allowed_audiences == {"orchestrator-api"}

    def test_the_bff_client_is_always_allowed(self, make_validator, monkeypatch, sign):
        monkeypatch.setenv(oidc.OIDC_ALLOWED_CLIENTS_ENV, "")
        monkeypatch.setenv(oidc.OIDC_ALLOWED_AUDIENCES_ENV, "")
        validator = make_validator()
        assert validator.binding.allowed_clients == {BFF}
        assert validator.binding.allowed_audiences == frozenset()
        assert validator.validate_token(sign(access(BFF)))
        # Explicitly cleared lists fall back to nothing, not to the defaults.
        assert validator.validate_token(sign(access("cockpit"))) is None
        assert (
            validator.validate_token(sign(access("x", aud="orchestrator-api"))) is None
        )

    def test_env_lists_are_comma_separated(self, make_validator, monkeypatch, sign):
        monkeypatch.setenv(oidc.OIDC_ALLOWED_CLIENTS_ENV, " cockpit-bff, srw-cli ,, ")
        monkeypatch.setenv(
            oidc.OIDC_ALLOWED_AUDIENCES_ENV, "api://srw, orchestrator-api"
        )
        validator = make_validator()
        assert validator.binding.allowed_clients == {BFF, "srw-cli"}
        assert validator.binding.allowed_audiences == {"api://srw", "orchestrator-api"}
        assert validator.validate_token(sign(access("srw-cli")))
        assert validator.validate_token(sign(access("other", aud=["api://srw"])))
        assert validator.validate_token(sign(access("cockpit"))) is None

    @pytest.mark.parametrize("audience", ["account", "*"])
    def test_an_audience_every_token_carries_is_ignored(
        self, make_validator, monkeypatch, sign, caplog, audience
    ):
        monkeypatch.setenv(
            oidc.OIDC_ALLOWED_AUDIENCES_ENV, f"orchestrator-api,{audience}"
        )
        with caplog.at_level(logging.WARNING, logger="orchestrator.security.oidc"):
            validator = make_validator()
        assert validator.binding.allowed_audiences == {"orchestrator-api"}
        assert any("ignored" in r.getMessage() for r in caplog.records)
        assert validator.validate_token(sign(access("gitea", aud=audience))) is None

    def test_constructor_arguments_override_the_environment(self, make_validator, sign):
        validator = make_validator(
            client_id="bff-2", allowed_clients=["cli"], allowed_audiences=[]
        )
        assert validator.binding.allowed_clients == {"bff-2", "cli"}
        assert validator.validate_token(sign(access("cli")))
        assert validator.validate_token(sign(access(BFF))) is None

    def test_parse_name_list(self):
        assert parse_name_list(None, ("a",)) == {"a"}
        assert parse_name_list("", ("a",)) == frozenset()
        assert parse_name_list(" b , c,", ("a",)) == {"b", "c"}

    def test_binding_drops_blank_names(self):
        binding = TokenBinding.build(["", "cockpit"], ["", "orchestrator-api"])
        assert binding.allowed_clients == {"cockpit"}
        assert binding.allowed_audiences == {"orchestrator-api"}


class TestIdToken:
    def test_accepted_for_the_bff_client(self, validator, sign):
        claims = validator.decode_id_token(sign({"typ": "ID", "azp": BFF, "aud": BFF}))
        assert claims and claims["sid"] == "session-1"

    def test_accepted_with_the_bff_client_among_audiences(self, validator, sign):
        assert validator.decode_id_token(
            sign({"typ": "ID", "azp": BFF, "aud": [BFF, "other"]})
        )

    @pytest.mark.parametrize(
        "claims",
        [
            pytest.param(
                {"typ": "ID", "azp": "gitea", "aud": "gitea"}, id="other-client"
            ),
            pytest.param(
                {"typ": "ID", "azp": "cockpit", "aud": "cockpit"}, id="spa-client"
            ),
            pytest.param({"typ": "ID", "azp": BFF}, id="no-aud"),
            pytest.param({"typ": "Bearer", "azp": BFF, "aud": BFF}, id="access-token"),
        ],
    )
    def test_rejected(self, validator, sign, claims):
        assert validator.decode_id_token(sign(claims)) is None


class TestLogoutToken:
    def _logout(self, **claims) -> dict:
        return {
            "typ": "Logout",
            "events": {BACKCHANNEL_LOGOUT_EVENT: {}},
            **claims,
        }

    def test_accepted_for_the_bff_client(self, validator, sign):
        claims = validator.verify_logout_token(sign(self._logout(aud=BFF)))
        assert claims and claims["sid"] == "session-1"

    @pytest.mark.parametrize("aud", ["gitea", None])
    def test_rejected_for_another_client(self, validator, sign, aud):
        claims = self._logout() if aud is None else self._logout(aud=aud)
        assert validator.verify_logout_token(sign(claims)) is None


def _gate_oauth():
    path = Path(__file__).resolve().parents[1] / "scripts" / "k3d_gate_oauth.py"
    spec = importlib.util.spec_from_file_location("_k3d_gate_oauth_binding", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestK3dGateTokens:
    """The local k3d gates keep authenticating under the default binding:
    their clients' mappers give the access token ``sub`` and the
    ``orchestrator-api`` audience (``scripts/k3d_gate_oauth.py``), with no
    values-local allowance for the gate clients."""

    def test_a_gate_clients_access_token_is_accepted(self, validator, sign):
        gate = _gate_oauth()
        assert gate.GATE_AUDIENCE in oidc.DEFAULT_ALLOWED_AUDIENCES
        assert gate.GATE_CLIENT not in validator.binding.allowed_clients
        for client in (gate.GATE_CLIENT, "gate-0123abcd-oauth"):
            claims = validator.validate_token(
                sign(access(client, aud=gate.GATE_AUDIENCE))
            )
            assert claims and claims["azp"] == client

    @pytest.mark.parametrize(
        "claims",
        [
            pytest.param({"typ": "ID", "azp": "g", "aud": "g"}, id="id-token"),
            pytest.param(access("g"), id="access-token-without-audience"),
            pytest.param(access("admin-cli"), id="admin-cli-lightweight"),
        ],
    )
    def test_what_the_gates_sent_before_is_refused(self, validator, sign, claims):
        assert validator.validate_token(sign(claims)) is None


class _Request:
    def __init__(self, authorization: str) -> None:
        self.cookies: dict[str, str] = {}
        self.headers = {"Authorization": authorization}


class TestBearerPath:
    """``get_current_user`` refuses a realm token of another client."""

    @pytest.mark.asyncio
    async def test_another_clients_token_is_401(self, monkeypatch, validator, sign):
        monkeypatch.setattr(auth, "oidc_validator", validator)

        async def _never(*_args, **_kwargs):
            raise AssertionError("an unbound token must not resolve a user")

        monkeypatch.setattr(auth, "_resolve_user_from_claims", _never)
        token = sign(access("opencloud-orchestrator", aud=["account"]))
        with pytest.raises(HTTPException) as caught:
            await auth.get_current_user(_Request(f"Bearer {token}"), db=None)
        assert caught.value.status_code == 401

    @pytest.mark.asyncio
    async def test_an_allowed_clients_token_resolves(
        self, monkeypatch, validator, sign
    ):
        monkeypatch.setattr(auth, "oidc_validator", validator)
        seen: list[dict] = []

        async def _resolve(claims, _db):
            seen.append(claims)
            return {"id": "user-1"}

        monkeypatch.setattr(auth, "_resolve_user_from_claims", _resolve)
        token = sign(access("cockpit", aud="account"))
        assert await auth.get_current_user(_Request(f"Bearer {token}"), db=None) == {
            "id": "user-1"
        }
        assert seen[0]["azp"] == "cockpit"
