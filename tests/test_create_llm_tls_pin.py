"""Every chat-model factory builds its HTTP client with the endpoint's pin.

Chat, auxiliary, summarization, subagent and citation models all come from
``create_llm``; a model on a pinned endpoint must verify exactly the pinned
certificate, against a real TLS server. A pin that can't be read or can't
apply fails the build: never an unpinned client (ruling R11).
"""
# ruff: noqa: F811  (the imported fixture is requested by name as a test argument)

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import anthropic
import httpx
import pytest
from langchain_anthropic import ChatAnthropic

from shared.runtime.core.loader import LimitsConfig, LLMConfig, create_llm
from shared.runtime.llm.pinned_anthropic import PinnedChatAnthropic
from shared.runtime.llm.reasoning_chat import ReasoningChatOpenAI
from shared.tls_pinning import (
    UNREADABLE_PIN,
    CertificateChangedError,
    TlsPin,
    pinned_transport_for,
)
from tests.tls_pin_fixtures import tls_certs, tls_server  # noqa: F401

#: Providers whose factory builds a ReasoningChatOpenAI (OpenAI wire format).
OPENAI_SHAPED = ("openai", "codex", "openrouter", "mistral")
#: Every provider an endpoint row can route to.
ENDPOINT_PROVIDERS = OPENAI_SHAPED + ("anthropic",)

ANTHROPIC_REPLY = {
    "id": "msg_1",
    "type": "message",
    "role": "assistant",
    "model": "m1",
    "content": [{"type": "text", "text": "ok"}],
    "stop_reason": "end_turn",
    "stop_sequence": None,
    "usage": {"input_tokens": 1, "output_tokens": 1},
}
MESSAGES_ROUTE = {("POST", "/v1/messages"): ANTHROPIC_REPLY}


def _pin(path) -> TlsPin:
    return TlsPin.from_pem(path.read_text())


def _config(provider: str, base_url, pin=None, **overrides) -> LLMConfig:
    fields = dict(
        model="m1",
        provider=provider,
        base_url=base_url,
        api_key="sk-test",
        tls_pinned_cert=pin.wire if isinstance(pin, TlsPin) else pin,
        max_retries=0,
        timeout=20,
    )
    fields.update(overrides)
    return LLMConfig(**fields)


def _caused_by(exc: BaseException, cls: type) -> bool:
    seen: set[int] = set()
    cur = exc
    while cur is not None and id(cur) not in seen:
        if isinstance(cur, cls):
            return True
        seen.add(id(cur))
        cur = cur.__cause__ or cur.__context__
    return False


def _transports(llm):
    """The (sync, async) transports a built model sends through."""
    if isinstance(llm, ChatAnthropic):
        return llm._client._client._transport, llm._async_client._client._transport
    return llm._reasoning_client._transport, llm._async_reasoning_client._transport


# ---------------------------------------------------------------------------
# OpenAI-shaped factories (openai, codex, openrouter, mistral)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", OPENAI_SHAPED)
async def test_openai_shaped_model_answers_through_the_pin(tls_certs, provider):
    pin = _pin(tls_certs.leaf)
    with tls_server(tls_certs.leaf, tls_certs.leaf_key) as server:
        llm = create_llm(_config(provider, server.url, pin))
        assert isinstance(llm, ReasoningChatOpenAI)
        sync_transport, async_transport = _transports(llm)
        assert sync_transport._pin == pin and async_transport._pin == pin

        reply = await llm.ainvoke("hi")

    assert reply.content == "ok"
    assert [r["path"] for r in server.requests] == ["/v1/chat/completions"]


def test_the_sync_client_answers_through_the_pin(tls_certs):
    pin = _pin(tls_certs.leaf)
    with tls_server(tls_certs.leaf, tls_certs.leaf_key) as server:
        reply = create_llm(_config("openai", server.url, pin)).invoke("hi")
    assert reply.content == "ok"


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ENDPOINT_PROVIDERS)
async def test_a_changed_certificate_fails_and_sends_nothing(tls_certs, provider):
    pin = _pin(tls_certs.leaf)
    with tls_server(tls_certs.swap, tls_certs.swap_key, MESSAGES_ROUTE) as server:
        llm = create_llm(_config(provider, server.url, pin))
        with pytest.raises(Exception) as raised:
            await llm.ainvoke("hi")

    assert _caused_by(raised.value, CertificateChangedError), repr(raised.value)
    assert server.requests == []  # no request, so no key, reached the impostor


# ---------------------------------------------------------------------------
# Anthropic: PinnedChatAnthropic, never the shared lru_cache client
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_anthropic_builds_a_pinned_client_and_answers(tls_certs):
    pin = _pin(tls_certs.leaf)
    with tls_server(tls_certs.leaf, tls_certs.leaf_key, MESSAGES_ROUTE) as server:
        llm = create_llm(_config("anthropic", server.url, pin))
        assert type(llm) is PinnedChatAnthropic
        sync_transport, async_transport = _transports(llm)
        expected_sync = pinned_transport_for(anthropic.DefaultHttpxClient, pin)
        expected_async = pinned_transport_for(anthropic.DefaultAsyncHttpxClient, pin)
        assert type(sync_transport) is type(expected_sync)
        assert type(async_transport) is type(expected_async)
        assert sync_transport._pin == pin and async_transport._pin == pin

        reply = await llm.ainvoke("hi")
        sync_reply = llm.invoke("hi")

    assert reply.content == "ok" and sync_reply.content == "ok"
    assert [r["path"] for r in server.requests] == ["/v1/messages"] * 2


def test_anthropic_client_is_pinned_in_the_installed_sdk(tls_certs):
    # On anthropic>=1 the SDK runs on httpx2 and refuses httpx transports; the
    # pinned transport must be of the SDK's own package.
    pin = _pin(tls_certs.leaf)
    llm = create_llm(_config("anthropic", "https://127.0.0.1:9/v1", pin))
    _, async_transport = _transports(llm)
    if issubclass(anthropic.DefaultAsyncHttpxClient, httpx.AsyncClient):
        from shared.tls_pinning import AsyncPinnedTransport

        assert type(async_transport) is AsyncPinnedTransport
    else:
        import httpx2

        assert isinstance(async_transport, httpx2.AsyncHTTPTransport)


def test_pinned_and_unpinned_anthropic_on_one_url_share_no_client(tls_certs):
    pin = _pin(tls_certs.leaf)
    url = "https://127.0.0.1:9/v1"
    pinned = create_llm(_config("anthropic", url, pin))
    pinned_async = pinned._async_client._client
    pinned_sync = pinned._client._client
    unpinned = create_llm(_config("anthropic", url))
    again = create_llm(_config("anthropic", url, pin))

    assert type(unpinned) is ChatAnthropic
    assert unpinned._async_client._client is not pinned_async
    assert unpinned._client._client is not pinned_sync
    assert not hasattr(unpinned._async_client._client._transport, "_pin")
    assert not hasattr(unpinned._client._client._transport, "_pin")
    # Each pinned instance has its own client, too.
    assert again._async_client._client is not pinned_async


def test_pinned_anthropic_keeps_an_anthropic_proxy(tls_certs, monkeypatch):
    # Upstream routes through ANTHROPIC_PROXY; the pinned client does too, and
    # the TLS to the endpoint through the tunnel is still the pinned context.
    monkeypatch.setenv("ANTHROPIC_PROXY", "http://proxy.local:3128")
    pin = _pin(tls_certs.leaf)
    llm = create_llm(_config("anthropic", "https://127.0.0.1:9/v1", pin))
    for transport in _transports(llm):
        assert transport._pin == pin
        assert "Proxy" in type(transport._pool).__name__


@pytest.mark.parametrize("which", ["_client", "_async_client"])
def test_pinned_anthropic_differs_from_the_unpinned_only_in_trust(tls_certs, which):
    url = "https://127.0.0.1:9/v1"
    headers = {"X-Route": "r1"}
    pin = _pin(tls_certs.leaf)
    pinned = getattr(
        create_llm(_config("anthropic", url, pin, extra_headers=headers)), which
    )
    plain = getattr(create_llm(_config("anthropic", url, extra_headers=headers)), which)

    assert type(pinned) is type(plain)
    assert str(pinned.base_url) == str(plain.base_url)
    assert pinned.api_key == plain.api_key == "sk-test"
    assert pinned.max_retries == plain.max_retries == 0
    assert pinned.timeout == plain.timeout
    assert pinned.default_headers == plain.default_headers
    assert pinned.default_headers["X-Route"] == "r1"
    assert type(pinned._client) is type(plain._client)
    assert pinned._client.timeout == plain._client.timeout
    assert str(pinned._client.base_url) == str(plain._client.base_url)
    assert pinned._client.follow_redirects == plain._client.follow_redirects


# ---------------------------------------------------------------------------
# A pin that can't be read or can't apply fails the build
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("provider", ENDPOINT_PROVIDERS)
@pytest.mark.parametrize("bad", ["not-base64!", "QUJD", UNREADABLE_PIN])
def test_an_unusable_pin_fails_the_build(provider, bad):
    url = "https://model-box.local:8443/v1"
    with pytest.raises(ValueError) as raised:
        create_llm(_config(provider, url, bad))
    message = str(raised.value)
    assert "'m1'" in message and url in message
    assert "pinned certificate" in message


def test_the_unreadable_marker_names_the_admin_fix():
    with pytest.raises(ValueError, match="trust it again or remove the pin"):
        create_llm(_config("openai", "https://model-box.local/v1", UNREADABLE_PIN))


@pytest.mark.parametrize("provider", ENDPOINT_PROVIDERS)
def test_a_pin_on_an_http_url_fails_the_build(tls_certs, provider):
    url = "http://model-box.local:8080/v1"
    with pytest.raises(ValueError) as raised:
        create_llm(_config(provider, url, _pin(tls_certs.leaf)))
    assert url in str(raised.value) and "https://" in str(raised.value)


@pytest.mark.parametrize("provider", ENDPOINT_PROVIDERS)
def test_a_pin_without_a_base_url_fails_the_build(tls_certs, provider):
    # A pin travels with its endpoint's base URL; alone it would be put on the
    # provider's own default host (or codex's http proxy default).
    with pytest.raises(ValueError, match="pinned certificate"):
        create_llm(_config(provider, None, _pin(tls_certs.leaf)))


@pytest.mark.parametrize("provider", ["google", "groq"])
def test_provider_key_factories_refuse_a_pin(tls_certs, provider):
    with pytest.raises(ValueError) as raised:
        create_llm(_config(provider, "https://h/v1", _pin(tls_certs.leaf)))
    assert str(raised.value) == (
        "A pinned certificate applies to endpoint-backed models; the "
        f"{provider} factory reaches only its provider's own API."
    )


# ---------------------------------------------------------------------------
# No pin: the existing types, unchanged
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("provider", OPENAI_SHAPED)
@pytest.mark.parametrize("blank", [None, ""])
def test_no_pin_keeps_the_default_transports(provider, blank):
    llm = create_llm(_config(provider, "https://model-box.local/v1", blank))
    assert type(llm) is ReasoningChatOpenAI
    sync_transport, async_transport = _transports(llm)
    assert type(sync_transport) is httpx.HTTPTransport
    assert type(async_transport) is httpx.AsyncHTTPTransport


def test_no_pin_keeps_plain_chat_anthropic():
    llm = create_llm(_config("anthropic", "https://model-box.local/v1"))
    assert type(llm) is ChatAnthropic


# ---------------------------------------------------------------------------
# Citation verifier (A7): the transport's pin reaches the LLMConfig
# ---------------------------------------------------------------------------

CITATION_ENV = (
    "CITATION_LLM_MODEL",
    "CITATION_LLM_SERVED_MODEL",
    "CITATION_LLM_PROVIDER",
    "CITATION_LLM_BASE_URL",
    "CITATION_LLM_URL",
    "CITATION_LLM_API_KEY",
    "CITATION_LLM_TLS_PINNED_CERT",
    "OPENAI_API_KEY",
)
CITE_URL = "https://cite.internal:8443/v1"


@pytest.fixture
def citation_env(monkeypatch):
    for name in CITATION_ENV:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("CITATION_LLM_MODEL", "cite-model")
    monkeypatch.setenv("CITATION_LLM_BASE_URL", CITE_URL)
    monkeypatch.setenv("CITATION_LLM_API_KEY", "sk-cit")
    return monkeypatch


def _citation_agent():
    from agent.agent import UniversalAgent
    from shared.runtime.core.loader import load_agent_config

    agent = UniversalAgent.__new__(UniversalAgent)
    cfg = load_agent_config("config/worker_base.yaml")
    cfg.auxiliary.enabled = True
    cfg.auxiliary.tasks["verify_citations"].enabled = True
    agent.config = cfg
    agent._summarization_llm = MagicMock(name="summarization_llm")
    agent._auxiliary_llm = MagicMock(name="auxiliary_llm")
    agent._citation_verify_aux = None
    agent._citation_verification_prompt = ""
    return agent


def _captured_citation_configs(agent) -> list[LLMConfig]:
    created: list[LLMConfig] = []

    def fake_create_llm(cfg, limits=None):
        created.append(cfg)
        return SimpleNamespace(model=cfg.model)

    with patch("agent.agent.create_llm", side_effect=fake_create_llm):
        agent._initialize_citation_verifier(LimitsConfig())
    return created


def test_the_citation_config_carries_the_transport_pin(tls_certs, citation_env):
    pin = _pin(tls_certs.leaf)
    citation_env.setenv("CITATION_LLM_TLS_PINNED_CERT", pin.wire)

    created = _captured_citation_configs(_citation_agent())

    assert len(created) == 1
    assert created[0].base_url == CITE_URL
    assert created[0].tls_pinned_cert == pin.wire


def test_an_unpinned_citation_endpoint_has_no_pin(citation_env):
    created = _captured_citation_configs(_citation_agent())
    assert len(created) == 1 and created[0].tls_pinned_cert is None


def test_the_citation_client_is_built_pinned(tls_certs, citation_env):
    pin = _pin(tls_certs.leaf)
    citation_env.setenv("CITATION_LLM_TLS_PINNED_CERT", pin.wire)
    agent = _citation_agent()

    agent._initialize_citation_verifier(LimitsConfig())

    client = agent._citation_verify_aux.llm
    assert isinstance(client, ReasoningChatOpenAI)
    assert all(t._pin == pin for t in _transports(client))
