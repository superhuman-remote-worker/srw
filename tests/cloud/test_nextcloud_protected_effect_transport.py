"""Nextcloud adapter transport belt for the isolated protected-effect URL."""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from orchestrator.services.cloud import CloudBackendError
from orchestrator.services.cloud.config import NextcloudSettings
from orchestrator.services.cloud.nextcloud import NextcloudBackend
from orchestrator.services.cloud.nextcloud_agent_credential import AgentServiceAuth
from orchestrator.services.cloud.protected_effect_contract import (
    NextcloudEffectCapability,
    NextcloudEffectFenceIntent,
    NextcloudEffectRequestAuthority,
    NextcloudInstallationAttestation,
    adopt_protected_effect_capability,
    sign_protected_effect_capability,
    sign_protected_effect_installation_attestation,
    sign_protected_effect_request,
)


INSTANCE = "99999999-9999-4999-8999-999999999999"
ATTEMPT = "33333333-3333-4333-8333-333333333333"
CONFIG_SHA = "a" * 64
INSTALLATION_PROOF_SHA = "d" * 64
KEY = b"k" * 32
NOW = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)


def _settings() -> NextcloudSettings:
    return NextcloudSettings(
        base_url="https://cloud.internal.example/nextcloud",
        public_url="https://cloud.example/nextcloud",
        admin_user="admin",
        admin_password="admin-password",
        agent_user="agent-service",
        agent_password="agent-password",
        protected_effect_url="https://effect.internal.example",
        protected_effect_config_sha256=CONFIG_SHA,
        protected_effect_hmac_key=KEY.decode("ascii"),
    )


def _capability() -> NextcloudEffectCapability:
    return NextcloudEffectCapability(
        backend_instance_id=INSTANCE,
        config_sha256=CONFIG_SHA,
        queue_bound_seconds=30,
        handler_bound_seconds=10,
        clock_skew_bound_seconds=2,
        safety_margin_seconds=5,
        capability_max_age_seconds=5,
        server_time=NOW,
    )


def _capability_payload(capability: NextcloudEffectCapability) -> dict:
    attestation = NextcloudInstallationAttestation(
        backend_instance_id=INSTANCE,
        config_sha256=CONFIG_SHA,
        installation_proof_sha256=INSTALLATION_PROOF_SHA,
        capability_sha256=capability.capability_sha256,
        server_time=capability.server_time,
    )
    return {
        "capability": capability.binding,
        "signature": sign_protected_effect_capability(capability, key=KEY),
        "installation_attestation": attestation.binding,
        "installation_signature": (
            sign_protected_effect_installation_attestation(
                attestation,
                key=KEY,
            )
        ),
    }


def _intent(*, body: bytes, path: str) -> NextcloudEffectFenceIntent:
    capability = _capability()
    validated = adopt_protected_effect_capability(
        capability.binding,
        signature=sign_protected_effect_capability(capability, key=KEY),
        key=KEY,
        db_before=NOW - timedelta(seconds=1),
        db_after=NOW + timedelta(seconds=1),
        expected_backend_instance_id=INSTANCE,
        expected_config_sha256=CONFIG_SHA,
    )
    assert validated is not None
    request = NextcloudEffectRequestAuthority(
        backend_instance_id=INSTANCE,
        config_sha256=CONFIG_SHA,
        engage_attempt=ATTEMPT,
        method="POST",
        path=path,
        body_sha256=hashlib.sha256(body).hexdigest(),
        effect_not_after=NOW + timedelta(seconds=30),
    )
    return NextcloudEffectFenceIntent.capture(
        capability=validated,
        request=request,
        request_signature=sign_protected_effect_request(request, key=KEY),
        key=KEY,
        db_dispatched_at=NOW + timedelta(seconds=2),
    )


@pytest.mark.asyncio
async def test_initialization_builds_a_distinct_json_effect_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clients: list[object] = []

    class _Client:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs
            self.closed = False
            clients.append(self)

        async def get(self, path: str, **_kwargs) -> httpx.Response:
            if path == "/status.php":
                # Nextcloud 31 deliberately no longer publishes instanceid.
                payload = {"installed": True, "version": "31.0.0"}
            elif path.endswith("/api/v1/capability"):
                capability = NextcloudEffectCapability(
                    backend_instance_id=INSTANCE,
                    config_sha256=CONFIG_SHA,
                    queue_bound_seconds=30,
                    handler_bound_seconds=10,
                    clock_skew_bound_seconds=2,
                    safety_margin_seconds=5,
                    capability_max_age_seconds=5,
                    server_time=datetime.now(timezone.utc),
                )
                payload = _capability_payload(capability)
            else:
                payload = {"ocs": {"meta": {"statuscode": 100}, "data": {}}}
            return httpx.Response(
                200,
                request=httpx.Request("GET", f"https://cloud.invalid{path}"),
                json=payload,
            )

        async def aclose(self) -> None:
            self.closed = True

    monkeypatch.setattr(
        "orchestrator.services.cloud.nextcloud.httpx.AsyncClient",
        _Client,
    )
    backend = NextcloudBackend(_settings())
    backend.prepare_backend_instance_attestation(INSTANCE)

    assert await backend.ensure_initialized() is True
    assert backend.backend_instance_id is None
    assert backend.installation_proof_sha256 == INSTALLATION_PROOF_SHA
    backend.bind_backend_instance(INSTANCE)
    assert backend.backend_instance_id == INSTANCE
    # The admin's, agent-service's and the effect lane's: three clients,
    # and only the effect lane's speaks to the isolated origin.
    assert len(clients) == 3
    ordinary, agent, effect = clients
    assert ordinary.kwargs["base_url"] == "https://cloud.internal.example/nextcloud"
    assert ordinary.kwargs["auth"] == ("admin", "admin-password")
    assert agent.kwargs["base_url"] == "https://cloud.internal.example/nextcloud"
    assert isinstance(agent.kwargs["auth"], AgentServiceAuth)
    assert effect.kwargs["base_url"] == "https://effect.internal.example"
    assert effect.kwargs["headers"]["Accept"] == "application/json"
    assert effect.kwargs["headers"]["OCS-APIRequest"] == "true"
    assert effect.kwargs["auth"] == ("admin", "admin-password")

    await backend.close()
    assert ordinary.closed is True
    assert agent.closed is True
    assert effect.closed is True


@pytest.mark.asyncio
async def test_capability_and_effect_use_only_the_isolated_effect_origin() -> None:
    seen: list[httpx.Request] = []
    capability = _capability()

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path.endswith("/api/v1/capability"):
            return httpx.Response(
                200,
                json=_capability_payload(capability),
            )
        return httpx.Response(
            200,
            json={"ocs": {"meta": {"status": "ok", "statuscode": 100}}},
        )

    backend = NextcloudBackend(_settings())
    backend.bind_backend_instance(INSTANCE)
    backend._client = httpx.AsyncClient(  # noqa: SLF001 - exact transport seam
        base_url="https://cloud.internal.example/nextcloud",
        transport=httpx.MockTransport(lambda _request: httpx.Response(500)),
    )
    backend._protected_effect_client = httpx.AsyncClient(  # noqa: SLF001
        base_url="https://effect.internal.example",
        auth=("admin", "admin-password"),
        transport=httpx.MockTransport(handler),
    )
    backend._initialized = True  # noqa: SLF001
    try:
        binding, signature = await backend.fetch_protected_effect_capability()
        assert binding == capability.binding
        assert signature == sign_protected_effect_capability(capability, key=KEY)

        body = b"userid=srw-reader-a-33333333333343338333333333333333&password=pw"
        path = "/nextcloud/ocs/v2.php/cloud/users"
        intent = _intent(body=body, path=path)
        response = await backend.dispatch_protected_effect(intent, body=body)
        assert response.status_code == 200
    finally:
        await backend.close()

    assert [request.url.host for request in seen] == [
        "effect.internal.example",
        "effect.internal.example",
    ]
    assert [request.url.path for request in seen] == [
        "/nextcloud/index.php/apps/srw_protected_effect/api/v1/capability",
        "/nextcloud/ocs/v2.php/cloud/users",
    ]
    assert seen[0].headers["X-SRW-Backend-Instance"] == INSTANCE
    assert seen[1].headers["X-SRW-Backend-Instance"] == INSTANCE
    assert seen[1].headers["X-SRW-Protected-Effect-Authority"] == (
        intent.request.canonical_json
    )
    assert seen[1].headers["X-SRW-Protected-Effect-Signature"] == (
        intent.request_signature
    )
    assert seen[1].content == body
    assert not any(request.url.host == "cloud.internal.example" for request in seen)


@pytest.mark.asyncio
async def test_dispatch_refuses_body_or_instance_substitution_before_io() -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200)

    backend = NextcloudBackend(_settings())
    backend.bind_backend_instance(INSTANCE)
    backend._client = httpx.AsyncClient(  # noqa: SLF001
        transport=httpx.MockTransport(handler)
    )
    backend._protected_effect_client = httpx.AsyncClient(  # noqa: SLF001
        base_url="https://effect.internal.example",
        transport=httpx.MockTransport(handler),
    )
    backend._initialized = True  # noqa: SLF001
    body = b"userid=srw-reader-a-33333333333343338333333333333333&password=pw"
    intent = _intent(body=body, path="/nextcloud/ocs/v2.php/cloud/users")
    try:
        with pytest.raises(CloudBackendError, match="dispatch authority is malformed"):
            await backend.dispatch_protected_effect(intent, body=b"changed")
    finally:
        await backend.close()

    assert calls == 0
