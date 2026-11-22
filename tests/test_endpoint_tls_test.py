"""Admin endpoint Test, model-row Test and Discover inspect the certificate first.

For an ``https://`` endpoint the certificate is read before any request: when it
fails normal verification (no pin) or its pin, the HTTP probe is skipped, so no
request and no API key reach that server, and the ``tls`` block explains why.
With a matching pin the probe runs through it. One case per row of the spec's
"What Test shows" table, against a recording probe and an ``inspect_tls`` stub;
the last cases run end to end against a real TLS server.
"""
# ruff: noqa: F811  (the imported fixtures are requested by name as test arguments)

from __future__ import annotations

import base64
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import httpx
import pytest

from orchestrator.services import endpoint_tls, llm_endpoint_probe
from orchestrator.services.llm_endpoint_probe import ProbeResult
from shared import tls_pinning
from shared.subscription_routing import SUBSCRIPTION_PROXY_TRANSPORT
from shared.tls_pinning import (
    CertificateDetails,
    TlsInspection,
    TlsPin,
    fingerprint_sha256,
)
from tests._mounted_router import mount_router
from tests.tls_pin_fixtures import pin_of
from tests.tls_pin_fixtures import tls_certs, tls_server  # noqa: F401

REAL_ASYNC_CLIENT = httpx.AsyncClient

ENDPOINT_ID = "55555555-5555-4555-8555-555555555555"
ROW_ID = "66666666-6666-4666-8666-666666666666"
ADMIN_ID = "77777777-7777-4777-8777-777777777777"
URL = "https://10.18.2.105:8090/v1"
MODELS_URL = f"{URL}/models"
API_KEY = "endpoint-secret-key"
PINNED_AT = datetime(2026, 11, 22, 9, 30, tzinfo=timezone.utc)
MISMATCH = "IP address mismatch, certificate is not valid for '10.18.2.105'."


# --- Builders -----------------------------------------------------------------


def details(path) -> CertificateDetails:
    return tls_pinning._certificate_details(TlsPin.from_pem(path.read_text()).der)


def pin_columns(path, **updates) -> dict:
    d = details(path)
    return {
        "tls_pin_sha256": d.sha256,
        "tls_pin_cert": path.read_text(),
        "tls_pin_subject": d.subject,
        "tls_pin_issuer": d.issuer,
        "tls_pin_not_after": d.not_after,
        "tls_pinned_at": PINNED_AT,
        "tls_pinned_by": UUID(ADMIN_ID),
        **updates,
    }


NO_PIN = {
    "tls_pin_sha256": None,
    "tls_pin_cert": None,
    "tls_pin_subject": None,
    "tls_pin_issuer": None,
    "tls_pin_not_after": None,
    "tls_pinned_at": None,
    "tls_pinned_by": None,
}


def endpoint_row(pin=None, **updates) -> dict:
    return {
        "id": UUID(ENDPOINT_ID),
        "label": "University GPU box",
        "base_url": URL,
        "api_key": API_KEY,
        "key_prefix": "endpoint",
        "transport_kind": None,
        "created_at": datetime(2026, 11, 1, tzinfo=timezone.utc),
        "updated_at": None,
        **(pin_columns(pin) if pin is not None else NO_PIN),
        **updates,
    }


def model_row(**updates) -> dict:
    return {
        "id": UUID(ROW_ID),
        "provider_kind": "endpoint",
        "provider_ref": ENDPOINT_ID,
        "model_id": "gemma-4-31b",
        "served_model_id": None,
        "display_label": "Gemma 4 31B",
        "capabilities": ["chat", "auxiliary"],
        "family": "gemma",
        "context_window": None,
        "params_json": None,
        "enabled": True,
        **updates,
    }


def verified(path) -> TlsInspection:
    return TlsInspection(True, None, None, details(path), None, None)


def unverified(path, reason="address_mismatch", message=MISMATCH) -> TlsInspection:
    return TlsInspection(False, reason, message, details(path), None, None)


REFUSED = TlsInspection(
    False, None, None, None, None, "[Errno 111] Connect call failed"
)


class FakeProbe:
    """Stands in for ``probe_endpoint_models``; records every call."""

    def __init__(self, result: ProbeResult | None = None):
        self.result = result or ProbeResult(True, 200, None, MODELS_URL)
        self.calls: list[dict] = []

    async def __call__(self, *, base_url, api_key, tls_pin=None):
        self.calls.append(
            {"base_url": base_url, "api_key": api_key, "tls_pin": tls_pin}
        )
        return self.result


class InspectStub:
    def __init__(self, result: TlsInspection):
        self.result = result
        self.calls: list[tuple[str, TlsPin | None]] = []

    async def __call__(self, base_url, *, pin=None, timeout=10.0):
        self.calls.append((base_url, pin))
        return self.result


@pytest.fixture
def inspect(monkeypatch):
    def install(result: TlsInspection) -> InspectStub:
        stub = InspectStub(result)
        monkeypatch.setattr(endpoint_tls, "inspect_tls", stub)
        return stub

    return install


@pytest.fixture
def store():
    store = MagicMock()
    store.get_system_llm_endpoint = AsyncMock(return_value=endpoint_row())
    store.list_models = AsyncMock(return_value=[model_row()])
    store.get_model = AsyncMock(return_value=model_row())
    return store


def provider_service(store, probe):
    from orchestrator.services.provider_catalog import ProviderCatalogService

    return ProviderCatalogService(store, MagicMock(), probe, MagicMock())


def model_service(store, probe):
    from orchestrator.services.model_catalog import ModelCatalogService

    return ModelCatalogService(
        store=store,
        probe=probe,
        get_config_dir=MagicMock(),
        load_settings_matrix=MagicMock(),
        settings_for_family=MagicMock(),
        family_detector=MagicMock(),
        reasoning_capability=MagicMock(),
    )


async def endpoint_test(store, probe):
    return await provider_service(store, probe).test_provider_endpoint(ENDPOINT_ID)


async def row_test(store, probe):
    return await model_service(store, probe).test_catalog_model(ROW_ID)


def pin_line(path) -> dict:
    d = details(path)
    return {
        "sha256": d.sha256,
        "subject": d.subject,
        "issuer": d.issuer,
        "not_after": d.not_after.isoformat(),
        "expired": d.expired,
        "trusted_at": PINNED_AT.isoformat(),
    }


# --- "What Test shows", one case per row --------------------------------------


@pytest.mark.asyncio
async def test_unpinned_certificate_that_verifies_answers_as_today(
    tls_certs, inspect, store
):
    stub = inspect(verified(tls_certs.good))
    probe = FakeProbe()

    body = await endpoint_test(store, probe)

    assert body == {
        "ok": True,
        "status": 200,
        "error": None,
        "probe_url": MODELS_URL,
        "tls": None,
    }
    assert stub.calls == [(URL, None)]
    assert probe.calls == [{"base_url": URL, "api_key": API_KEY, "tls_pin": None}]


@pytest.mark.asyncio
async def test_unpinned_certificate_that_fails_is_explained_and_never_probed(
    tls_certs, inspect, store
):
    inspect(unverified(tls_certs.leaf))
    probe = FakeProbe()

    body = await endpoint_test(store, probe)

    assert probe.calls == []
    assert body["ok"] is False
    assert body["status"] is None
    assert body["probe_url"] == MODELS_URL
    assert body["error"] == f"The endpoint's certificate is not trusted: {MISMATCH}"
    assert body["tls"] == {
        "verified": False,
        "reason": "address_mismatch",
        "message": MISMATCH,
        "certificate": details(tls_certs.leaf).to_public(),
        "pin": None,
        "pin_matches": None,
        "publicly_valid": False,
        "can_trust": True,
        "refusal": None,
    }


@pytest.mark.asyncio
async def test_matching_pin_is_probed_through_the_pin(tls_certs, inspect, store):
    store.get_system_llm_endpoint.return_value = endpoint_row(pin=tls_certs.leaf)
    stub = inspect(unverified(tls_certs.leaf, "unknown_issuer", "unable to get"))
    probe = FakeProbe()

    body = await endpoint_test(store, probe)

    assert probe.calls == [
        {"base_url": URL, "api_key": API_KEY, "tls_pin": pin_of(tls_certs.leaf)}
    ]
    assert stub.calls == [(URL, pin_of(tls_certs.leaf))]
    assert body["ok"] is True and body["status"] == 200 and body["error"] is None
    assert body["tls"] == {
        "verified": True,
        "reason": None,
        "message": None,
        "certificate": details(tls_certs.leaf).to_public(),
        "pin": pin_line(tls_certs.leaf),
        "pin_matches": True,
        "publicly_valid": False,
        "can_trust": False,
        "refusal": None,
    }


@pytest.mark.asyncio
async def test_expired_pin_keeps_probing_and_says_it_expired(tls_certs, inspect, store):
    store.get_system_llm_endpoint.return_value = endpoint_row(pin=tls_certs.expired)
    inspect(unverified(tls_certs.expired, "unknown_issuer", "unable to get"))
    probe = FakeProbe(ProbeResult(True, 200, None, MODELS_URL))

    body = await endpoint_test(store, probe)

    assert body["ok"] is True
    assert probe.calls[0]["tls_pin"] == pin_of(tls_certs.expired)
    assert body["tls"]["pin"]["expired"] is True
    assert body["tls"]["pin"]["not_after"].startswith("2021-01-01")
    assert body["tls"]["certificate"]["expired"] is True
    assert body["tls"]["pin_matches"] is True
    assert body["tls"]["reason"] is None


@pytest.mark.asyncio
async def test_changed_certificate_that_fails_verification_offers_a_retrust(
    tls_certs, inspect, store
):
    store.get_system_llm_endpoint.return_value = endpoint_row(pin=tls_certs.leaf)
    inspect(unverified(tls_certs.swap, "unknown_issuer", "unable to get"))
    probe = FakeProbe()

    body = await endpoint_test(store, probe)

    changed = (
        "TLS certificate changed for 10.18.2.105:8090: pinned SHA-256 "
        f"{details(tls_certs.leaf).sha256}, server presents "
        f"{details(tls_certs.swap).sha256}. "
        "An admin must re-trust it in Admin → Models."
    )
    assert probe.calls == []
    assert body["ok"] is False and body["status"] is None
    assert body["error"] == changed
    assert body["tls"] == {
        "verified": False,
        "reason": "certificate_changed",
        "message": changed,
        "certificate": details(tls_certs.swap).to_public(),
        "pin": pin_line(tls_certs.leaf),
        "pin_matches": False,
        "publicly_valid": False,
        "can_trust": True,
        "refusal": None,
    }


@pytest.mark.asyncio
async def test_changed_certificate_that_verifies_normally_fails_closed(
    tls_certs, inspect, store
):
    # Owner decision 1: a pin is never bypassed by normal verification.
    store.get_system_llm_endpoint.return_value = endpoint_row(pin=tls_certs.leaf)
    inspect(verified(tls_certs.good))
    probe = FakeProbe()

    body = await endpoint_test(store, probe)

    assert probe.calls == []
    assert body["ok"] is False
    tls = body["tls"]
    assert tls["reason"] == "certificate_changed"
    assert tls["verified"] is False
    assert tls["publicly_valid"] is True
    assert tls["pin_matches"] is False
    assert tls["can_trust"] is False
    assert tls["certificate"]["sha256"] == details(tls_certs.good).sha256


@pytest.mark.asyncio
@pytest.mark.parametrize("pinned", [False, True])
async def test_unreachable_endpoint_keeps_todays_error(
    tls_certs, inspect, store, pinned
):
    if pinned:
        store.get_system_llm_endpoint.return_value = endpoint_row(pin=tls_certs.leaf)
    inspect(REFUSED)
    refused = ProbeResult(False, None, "ConnectError: connection refused", MODELS_URL)
    probe = FakeProbe(refused)

    body = await endpoint_test(store, probe)

    # The probe still runs: it has today's error text, and a pin still guards it.
    assert probe.calls == [
        {
            "base_url": URL,
            "api_key": API_KEY,
            "tls_pin": pin_of(tls_certs.leaf) if pinned else None,
        }
    ]
    assert body["ok"] is False
    assert body["status"] is None
    assert body["error"] == "ConnectError: connection refused"
    tls = body["tls"]
    assert tls["certificate"] is None
    assert tls["verified"] is False
    assert tls["reason"] is None
    assert tls["pin_matches"] is None
    assert tls["can_trust"] is False
    assert (tls["pin"] is not None) is pinned


@pytest.mark.asyncio
async def test_matching_pin_with_an_http_error_keeps_the_error_and_the_pin(
    tls_certs, inspect, store
):
    store.get_system_llm_endpoint.return_value = endpoint_row(pin=tls_certs.leaf)
    inspect(unverified(tls_certs.leaf, "unknown_issuer", "unable to get"))
    probe = FakeProbe(ProbeResult(False, 401, '{"error":"bad key"}', MODELS_URL))

    body = await endpoint_test(store, probe)

    assert body["ok"] is False
    assert body["status"] == 401
    assert body["error"] == '{"error":"bad key"}'
    assert body["tls"]["pin"] == pin_line(tls_certs.leaf)
    assert body["tls"]["pin_matches"] is True


@pytest.mark.asyncio
async def test_http_endpoint_is_not_inspected_and_carries_no_pin(
    tls_certs, inspect, store
):
    # Decision 4 keeps a pin across a base-URL edit, even to http://; a pin
    # never travels with a plain-HTTP probe.
    store.get_system_llm_endpoint.return_value = endpoint_row(
        pin=tls_certs.leaf, base_url="http://10.18.2.105:8090/v1"
    )
    stub = inspect(verified(tls_certs.good))
    probe = FakeProbe(ProbeResult(True, 200, None, "http://10.18.2.105:8090/v1/models"))

    body = await endpoint_test(store, probe)

    assert stub.calls == []
    assert body["tls"] is None
    assert body["ok"] is True
    assert probe.calls[0]["tls_pin"] is None


@pytest.mark.asyncio
async def test_tei_fallback_probes_the_capability_through_the_same_pin(
    tls_certs, inspect, store, monkeypatch
):
    store.get_system_llm_endpoint.return_value = endpoint_row(pin=tls_certs.leaf)
    store.list_models.return_value = [
        model_row(model_id="qwen3-embedding-8b", capabilities=["embedding"])
    ]
    inspect(unverified(tls_certs.leaf, "unknown_issuer", "unable to get"))
    probe = FakeProbe(ProbeResult(False, 404, "not found", MODELS_URL))
    capability_calls = []

    async def capability_probe(**kwargs):
        capability_calls.append(kwargs)
        return ProbeResult(True, 200, None, f"{URL}/embeddings")

    monkeypatch.setattr(
        llm_endpoint_probe, "probe_endpoint_capability", capability_probe
    )

    body = await endpoint_test(store, probe)

    assert capability_calls == [
        {
            "base_url": URL,
            "api_key": API_KEY,
            "capability": "embedding",
            "model": "qwen3-embedding-8b",
            "tls_pin": pin_of(tls_certs.leaf),
        }
    ]
    assert body["ok"] is True
    assert body["probe_url"] == f"{URL}/embeddings"
    assert body["tls"]["pin_matches"] is True


@pytest.mark.asyncio
async def test_endpoint_rows_are_read_once(tls_certs, inspect, store):
    inspect(verified(tls_certs.good))
    store.list_models.return_value = [
        model_row(model_id="qwen3-embedding-8b", capabilities=["embedding"])
    ]

    await endpoint_test(store, FakeProbe(ProbeResult(False, 404, None, MODELS_URL)))

    store.list_models.assert_awaited_once_with(
        provider_kind="endpoint", provider_ref=ENDPOINT_ID
    )


# --- Endpoints that can't be pinned never offer Trust ------------------------


SEARXNG = model_row(
    model_id="searxng",
    capabilities=["search"],
    params_json={"provider": "searxng", "ops": ["search"]},
)
CRAWL4AI = model_row(
    model_id="crawl4ai",
    capabilities=["fetch"],
    params_json={"provider": "crawl4ai"},
)
ELEVENLABS = model_row(
    model_id="eleven_v3", capabilities=["tts"], params_json={"provider": "elevenlabs"}
)
LIVE_VOICE = model_row(model_id="gpt-live-1", capabilities=["live_voice"])
IMAGE = model_row(
    model_id="gpt-image-1.5",
    capabilities=["image_generation"],
    params_json={"provider": "openai-images"},
)
GEMINI_IMAGE = model_row(
    model_id="gemini-3-pro-image",
    capabilities=["image_generation"],
    params_json={"provider": "gemini"},
)


@pytest.mark.parametrize(
    "updates,rows,sentence",
    [
        (
            {"base_url": "http://10.18.2.105:8090/v1"},
            [model_row()],
            "Pinning applies to https:// endpoints.",
        ),
        (
            {"transport_kind": SUBSCRIPTION_PROXY_TRANSPORT},
            [model_row()],
            "The subscription proxy can't be pinned.",
        ),
        (
            {
                "transport_kind": SUBSCRIPTION_PROXY_TRANSPORT,
                "base_url": "http://p:8317/v1",
            },
            [],
            "The subscription proxy can't be pinned.",
        ),
        (
            {"label": "SearXNG"},
            [SEARXNG],
            "SearXNG is served by the searxng adapter, which doesn't use a "
            "pinned certificate.",
        ),
        (
            {"label": "Tools"},
            [SEARXNG, CRAWL4AI, ELEVENLABS, SEARXNG],
            "Tools is served by the crawl4ai, elevenlabs, searxng adapter, which "
            "doesn't use a pinned certificate.",
        ),
        (
            {},
            [model_row(), LIVE_VOICE],
            "Live voice does not support pinned certificates yet; give the "
            "endpoint a certificate that verifies.",
        ),
    ],
)
def test_trust_refusal_sentences(updates, rows, sentence):
    assert endpoint_tls.endpoint_trust_refusal(endpoint_row(**updates), rows) == (
        sentence
    )


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [model_row()],
        [SEARXNG, model_row()],
        # Image generation honours the pin (ruling R3): its rows are pinnable
        # even though they name an adapter.
        [IMAGE],
        [GEMINI_IMAGE, SEARXNG],
    ],
)
def test_pinnable_endpoints_have_no_refusal(rows):
    assert endpoint_tls.endpoint_trust_refusal(endpoint_row(), rows) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "updates,rows,refusal",
    [
        (
            {"transport_kind": SUBSCRIPTION_PROXY_TRANSPORT},
            [model_row()],
            endpoint_tls.SUBSCRIPTION_PROXY_REFUSAL,
        ),
        (
            {"label": "SearXNG"},
            [SEARXNG],
            "SearXNG is served by the searxng adapter, which doesn't use a "
            "pinned certificate.",
        ),
        ({}, [model_row(), LIVE_VOICE], endpoint_tls.LIVE_VOICE_REFUSAL),
    ],
)
async def test_untrustable_endpoint_explains_but_offers_no_trust(
    tls_certs, inspect, store, updates, rows, refusal
):
    store.get_system_llm_endpoint.return_value = endpoint_row(**updates)
    store.list_models.return_value = rows
    inspect(unverified(tls_certs.leaf))
    probe = FakeProbe()

    body = await endpoint_test(store, probe)

    assert probe.calls == []
    assert body["tls"]["reason"] == "address_mismatch"
    assert body["tls"]["can_trust"] is False
    # Why Trust is not offered: the Trust API's 400 sentence, verbatim.
    assert body["tls"]["refusal"] == refusal


@pytest.mark.asyncio
async def test_a_changed_pin_on_an_untrustable_endpoint_says_why_no_retrust(
    tls_certs, inspect, store
):
    store.get_system_llm_endpoint.return_value = endpoint_row(pin=tls_certs.leaf)
    store.list_models.return_value = [model_row(), LIVE_VOICE]
    inspect(unverified(tls_certs.swap, "unknown_issuer", "unable to get"))

    body = await endpoint_test(store, FakeProbe())

    assert body["tls"]["reason"] == "certificate_changed"
    assert body["tls"]["can_trust"] is False
    assert body["tls"]["refusal"] == endpoint_tls.LIVE_VOICE_REFUSAL


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["pin_matches", "verifies_normally", "unreachable"])
async def test_no_refusal_where_trust_would_not_be_offered_anyway(
    tls_certs, inspect, store, case
):
    store.list_models.return_value = [model_row(), LIVE_VOICE]
    if case == "pin_matches":
        store.get_system_llm_endpoint.return_value = endpoint_row(pin=tls_certs.leaf)
        inspect(unverified(tls_certs.leaf, "unknown_issuer", "unable to get"))
    elif case == "verifies_normally":
        store.get_system_llm_endpoint.return_value = endpoint_row(pin=tls_certs.leaf)
        inspect(verified(tls_certs.good))
    else:
        inspect(REFUSED)

    body = await endpoint_test(store, FakeProbe())

    assert (body["tls"] or {}).get("refusal") is None


@pytest.mark.asyncio
async def test_image_generation_endpoint_offers_trust(tls_certs, inspect, store):
    store.list_models.return_value = [IMAGE]
    inspect(unverified(tls_certs.leaf))

    body = await endpoint_test(store, FakeProbe())

    assert body["tls"]["can_trust"] is True


# --- A stored pin that can't be read fails closed ----------------------------


#: Valid PEM armour around a truncated DER: it parses, but OpenSSL can't load
#: it as a trust anchor.
TRUNCATED = "-----BEGIN CERTIFICATE-----\nMIIBAAAA\n-----END CERTIFICATE-----\n"


def broken_pin(kind, certs) -> dict:
    if kind == "not_pem":
        return {"tls_pin_cert": "not a certificate"}
    if kind == "other_fingerprint":  # the stored PEM is not the stored fingerprint's
        return {"tls_pin_cert": certs.swap.read_text()}
    return {
        "tls_pin_cert": TRUNCATED,
        "tls_pin_sha256": fingerprint_sha256(base64.b64decode("MIIBAAAA")),
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["not_pem", "other_fingerprint", "truncated_der"])
@pytest.mark.parametrize("inspection", ["unverified", "verified", "refused"])
async def test_unreadable_stored_pin_fails_closed(
    tls_certs, inspect, store, kind, inspection
):
    broken = broken_pin(kind, tls_certs)
    store.get_system_llm_endpoint.return_value = endpoint_row(
        pin=tls_certs.leaf, **broken
    )
    inspect(
        {
            "unverified": unverified(tls_certs.leaf, "unknown_issuer", "x"),
            "verified": verified(tls_certs.good),
            "refused": REFUSED,
        }[inspection]
    )
    probe = FakeProbe()

    body = await endpoint_test(store, probe)

    assert probe.calls == []
    assert body["ok"] is False
    tls = body["tls"]
    assert tls["reason"] == "other"
    assert (
        "pinned certificate stored for this endpoint can't be read" in (tls["message"])
    )
    # Each kind fails on its own check, not on an earlier one.
    assert ("PEM certificate" in tls["message"]) is (kind == "not_pem")
    assert ("stored fingerprint" in tls["message"]) is (kind == "other_fingerprint")
    assert body["error"] == tls["message"]
    assert tls["pin"]["sha256"] == broken.get(
        "tls_pin_sha256", details(tls_certs.leaf).sha256
    )
    assert tls["verified"] is False
    assert tls["pin_matches"] is (None if inspection == "refused" else False)


@pytest.mark.parametrize("kind", ["not_pem", "other_fingerprint", "truncated_der"])
def test_unreadable_stored_pin_is_a_pin_that_matches_nothing(tls_certs, kind):
    row = endpoint_row(pin=tls_certs.leaf, **broken_pin(kind, tls_certs))

    pin = endpoint_tls.stored_pin(row)

    assert pin is not None
    assert pin.sha256 not in {
        details(p).sha256
        for p in (tls_certs.leaf, tls_certs.swap, tls_certs.good, tls_certs.expired)
    }
    with pytest.raises(Exception):
        tls_pinning.pinned_ssl_context(pin)


def test_stored_pin_and_its_public_line(tls_certs):
    assert endpoint_tls.stored_pin(endpoint_row()) is None
    assert endpoint_tls.pin_public(endpoint_row()) is None
    row = endpoint_row(pin=tls_certs.leaf)
    assert endpoint_tls.stored_pin(row) == pin_of(tls_certs.leaf)
    assert endpoint_tls.pin_public(row) == pin_line(tls_certs.leaf)
    expired = endpoint_tls.pin_public(endpoint_row(pin=tls_certs.expired))
    assert expired["expired"] is True


# --- The model row's Test shows the same block --------------------------------


@pytest.mark.asyncio
async def test_model_row_test_returns_the_endpoints_tls_block(
    tls_certs, inspect, store
):
    inspect(unverified(tls_certs.leaf))
    probe = FakeProbe()

    row_body = await row_test(store, probe)
    endpoint_body = await endpoint_test(store, probe)

    assert probe.calls == []
    assert row_body == endpoint_body
    assert row_body["tls"]["can_trust"] is True


@pytest.mark.asyncio
async def test_model_row_test_offers_no_trust_on_an_untrustable_endpoint(
    tls_certs, inspect, store
):
    store.list_models.return_value = [model_row(), LIVE_VOICE]
    inspect(unverified(tls_certs.leaf))

    body = await row_test(store, FakeProbe())

    assert body["tls"]["can_trust"] is False
    assert body["tls"]["refusal"] == endpoint_tls.LIVE_VOICE_REFUSAL


@pytest.mark.asyncio
async def test_model_row_test_probes_through_the_pin_and_its_fallback(
    tls_certs, inspect, store, monkeypatch
):
    store.get_system_llm_endpoint.return_value = endpoint_row(pin=tls_certs.leaf)
    store.get_model.return_value = model_row(
        model_id="bge-reranker-v2-m3", capabilities=["rerank"]
    )
    inspect(unverified(tls_certs.leaf, "unknown_issuer", "unable to get"))
    probe = FakeProbe(ProbeResult(False, 404, None, MODELS_URL))
    capability_calls = []

    async def capability_probe(**kwargs):
        capability_calls.append(kwargs)
        return ProbeResult(True, 200, None, f"{URL}/rerank")

    monkeypatch.setattr(
        llm_endpoint_probe, "probe_endpoint_capability", capability_probe
    )

    body = await row_test(store, probe)

    assert probe.calls[0]["tls_pin"] == pin_of(tls_certs.leaf)
    assert capability_calls[0]["tls_pin"] == pin_of(tls_certs.leaf)
    assert capability_calls[0]["capability"] == "rerank"
    assert body["ok"] is True
    assert body["tls"]["pin"] == pin_line(tls_certs.leaf)


@pytest.mark.asyncio
async def test_system_model_row_test_gains_an_empty_tls(store):
    store.get_model.return_value = model_row(
        provider_kind="system", provider_ref="openai"
    )
    store.get_system_api_key = AsyncMock(return_value="sk-test")

    body = await row_test(store, FakeProbe())

    assert body == {
        "ok": True,
        "status": 200,
        "error": None,
        "probe_url": None,
        "tls": None,
    }


# --- Discover -----------------------------------------------------------------


async def discover(store, probe):
    return await provider_service(store, probe).discover_provider_endpoint_models(
        ENDPOINT_ID
    )


@pytest.mark.asyncio
async def test_discover_refuses_an_untrusted_certificate_without_probing(
    tls_certs, inspect, store
):
    inspect(unverified(tls_certs.leaf))
    probe = FakeProbe()

    body = await discover(store, probe)

    assert probe.calls == []
    assert body == {
        "subscription": False,
        "ok": False,
        "status": None,
        "error": (
            "The endpoint's certificate is not trusted. Test the endpoint to review it."
        ),
        "probe_url": MODELS_URL,
        "models": [],
    }


@pytest.mark.asyncio
async def test_discover_probes_through_the_pin(tls_certs, inspect, store):
    store.get_system_llm_endpoint.return_value = endpoint_row(pin=tls_certs.leaf)
    inspect(unverified(tls_certs.leaf, "unknown_issuer", "unable to get"))
    models = [{"id": "gemma-4-31b"}]
    probe = FakeProbe(ProbeResult(True, 200, None, MODELS_URL, models=models))

    body = await discover(store, probe)

    assert probe.calls == [
        {"base_url": URL, "api_key": API_KEY, "tls_pin": pin_of(tls_certs.leaf)}
    ]
    assert body["ok"] is True and body["models"] == models


@pytest.mark.asyncio
async def test_discover_with_a_changed_certificate_says_so(tls_certs, inspect, store):
    store.get_system_llm_endpoint.return_value = endpoint_row(pin=tls_certs.leaf)
    inspect(verified(tls_certs.good))
    probe = FakeProbe()

    body = await discover(store, probe)

    assert probe.calls == []
    assert body["ok"] is False
    assert body["error"].startswith("TLS certificate changed for 10.18.2.105:8090:")


@pytest.mark.asyncio
async def test_discover_of_a_verified_unpinned_endpoint_is_unchanged(
    tls_certs, inspect, store
):
    inspect(verified(tls_certs.good))
    probe = FakeProbe()

    body = await discover(store, probe)

    assert probe.calls == [{"base_url": URL, "api_key": API_KEY, "tls_pin": None}]
    assert body == {
        "subscription": False,
        "ok": True,
        "status": 200,
        "error": None,
        "probe_url": MODELS_URL,
        "models": [],
    }


# --- End to end: a real TLS server, the real inspection and probe ------------


async def post_endpoint_test(store) -> httpx.Response:
    from orchestrator.routers import provider_catalog
    from orchestrator.services import discovery, subscription_discovery
    from orchestrator.services.llm_endpoint_probe import probe_endpoint_models
    from orchestrator.services.provider_catalog import ProviderCatalogService

    deps = provider_catalog.ProviderCatalogDependencies(
        service=ProviderCatalogService(
            store, discovery, probe_endpoint_models, subscription_discovery
        ),
        require_admin=AsyncMock(return_value={"id": ADMIN_ID}),
    )
    app = mount_router(
        provider_catalog.router,
        factories={"provider_catalog_dependencies_factory": lambda: deps},
    )
    async with REAL_ASYNC_CLIENT(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        return await client.post(f"/api/admin/providers/endpoints/{ENDPOINT_ID}/test")


@pytest.mark.asyncio
async def test_pinned_endpoint_test_reaches_the_server_through_the_pin(
    tls_certs, store
):
    with tls_server(tls_certs.leaf, tls_certs.leaf_key) as srv:
        store.get_system_llm_endpoint.return_value = endpoint_row(
            pin=tls_certs.leaf, base_url=srv.url
        )
        response = await post_endpoint_test(store)

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["status"] == 200
    assert body["tls"]["pin_matches"] is True
    assert body["tls"]["verified"] is True
    assert body["tls"]["publicly_valid"] is False
    assert body["tls"]["pin"] == pin_line(tls_certs.leaf)
    assert srv.requests == [
        {"method": "GET", "path": "/v1/models", "authorization": f"Bearer {API_KEY}"}
    ]


@pytest.mark.asyncio
async def test_unpinned_untrusted_endpoint_test_sends_nothing(tls_certs, store):
    with tls_server(tls_certs.leaf, tls_certs.leaf_key) as srv:
        store.get_system_llm_endpoint.return_value = endpoint_row(base_url=srv.url)
        body = (await post_endpoint_test(store)).json()

    assert srv.requests == []
    assert body["ok"] is False
    assert body["tls"]["reason"] == "unknown_issuer"
    assert body["tls"]["certificate"]["sha256"] == details(tls_certs.leaf).sha256
    assert body["tls"]["certificate"]["subject"] == (
        "CN=127.0.0.1, emailAddress=jfink@example.org"
    )
    assert body["tls"]["can_trust"] is True


@pytest.mark.asyncio
async def test_wrong_pin_sends_nothing(tls_certs, store):
    with tls_server(tls_certs.leaf, tls_certs.leaf_key) as srv:
        store.get_system_llm_endpoint.return_value = endpoint_row(
            pin=tls_certs.swap, base_url=srv.url
        )
        body = (await post_endpoint_test(store)).json()

    assert srv.requests == []
    assert body["tls"]["reason"] == "certificate_changed"
    assert body["tls"]["can_trust"] is True


@pytest.mark.asyncio
async def test_pinned_endpoint_with_a_publicly_valid_certificate_sends_nothing(
    tls_certs, store, monkeypatch
):
    monkeypatch.setenv("SSL_CERT_FILE", str(tls_certs.ca))
    with tls_server(tls_certs.good, tls_certs.good_key) as srv:
        store.get_system_llm_endpoint.return_value = endpoint_row(
            pin=tls_certs.leaf, base_url=srv.url
        )
        body = (await post_endpoint_test(store)).json()

    assert srv.requests == []
    assert body["tls"]["reason"] == "certificate_changed"
    assert body["tls"]["publicly_valid"] is True
    assert body["tls"]["can_trust"] is False


@pytest.mark.asyncio
async def test_unpinned_untrusted_endpoint_discover_sends_nothing(tls_certs, store):
    from orchestrator.services.llm_endpoint_probe import probe_endpoint_models

    with tls_server(tls_certs.leaf, tls_certs.leaf_key) as srv:
        store.get_system_llm_endpoint.return_value = endpoint_row(base_url=srv.url)
        body = await discover(store, probe_endpoint_models)

    assert srv.requests == []
    assert body["ok"] is False
    assert body["error"] == (
        "The endpoint's certificate is not trusted. Test the endpoint to review it."
    )
