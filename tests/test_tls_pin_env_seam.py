"""The pinned-certificate env names travel, scrub and resolve like the URL."""
# ruff: noqa: F811  (the imported fixture is requested by name as a test argument)

import os

import pytest

from agent.api import session_attach
from shared.runtime.core.transport_resolution import (
    DISPATCH_ENV_KEY_NAMES,
    CitationTransportError,
    exportable_env_keys,
    resolve_citation_transport,
)
from shared.runtime.services import embedding_service as emb
from shared.tls_pinning import UNREADABLE_PIN, TlsPin
from tests.tls_pin_fixtures import tls_certs  # noqa: F401

PREFIXES = (
    "EMBEDDING",
    "KB_EMBEDDING",
    "RERANK",
    "VISION",
    "WHISPER",
    "TTS",
    "CITATION_LLM",
)


@pytest.fixture
def pin(tls_certs):
    return TlsPin.from_pem(tls_certs.leaf.read_text())


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for prefix in PREFIXES:
        monkeypatch.delenv(f"{prefix}_TLS_PINNED_CERT", raising=False)
    yield
    for prefix in PREFIXES:
        os.environ.pop(f"{prefix}_TLS_PINNED_CERT", None)


def test_whitelist_names_every_prefix_and_still_drops_unknown(pin):
    for prefix in PREFIXES:
        assert f"{prefix}_TLS_PINNED_CERT" in DISPATCH_ENV_KEY_NAMES
    kept, dropped = exportable_env_keys(
        {"EMBEDDING_TLS_PINNED_CERT": pin.wire, "RANDOM_SDK_VAR": "x"}
    )
    assert kept == {"EMBEDDING_TLS_PINNED_CERT": pin.wire}
    assert dropped == ["RANDOM_SDK_VAR"]


@pytest.mark.parametrize("spelling", ["CITATION_LLM_BASE_URL", "CITATION_LLM_URL"])
def test_citation_transport_carries_its_pin(pin, spelling):
    transport = resolve_citation_transport(
        {
            spelling: "https://cite.internal/v1",
            "CITATION_LLM_API_KEY": "k",
            "CITATION_LLM_TLS_PINNED_CERT": pin.wire,
        }
    )
    assert transport.tls_pin == pin


def test_citation_transport_without_pin_has_none():
    transport = resolve_citation_transport({"CITATION_LLM_API_KEY": "k"})
    assert transport.tls_pin is None


@pytest.mark.parametrize("value", [UNREADABLE_PIN, "not-base64!"])
def test_citation_transport_refuses_an_unreadable_pin(value):
    with pytest.raises(CitationTransportError, match="CITATION_LLM_TLS_PINNED_CERT"):
        resolve_citation_transport(
            {
                "CITATION_LLM_BASE_URL": "https://cite.internal/v1",
                "CITATION_LLM_API_KEY": "k",
                "CITATION_LLM_TLS_PINNED_CERT": value,
            }
        )


def test_memory_embedding_pin_is_scrubbed_on_the_next_attach(pin):
    session_attach.apply_session_embedding_env(
        {
            "EMBEDDING_BASE_URL": "https://a.example",
            "EMBEDDING_TLS_PINNED_CERT": pin.wire,
            "RERANK_BASE_URL": "https://a-rerank.example",
            "RERANK_TLS_PINNED_CERT": pin.wire,
            "KB_EMBEDDING_TLS_PINNED_CERT": pin.wire,
        }
    )
    assert os.environ["EMBEDDING_TLS_PINNED_CERT"] == pin.wire
    assert os.environ["RERANK_TLS_PINNED_CERT"] == pin.wire
    assert os.environ["KB_EMBEDDING_TLS_PINNED_CERT"] == pin.wire
    session_attach.apply_session_embedding_env(
        {"EMBEDDING_BASE_URL": "https://b.example"}
    )
    for name in (
        "EMBEDDING_TLS_PINNED_CERT",
        "RERANK_TLS_PINNED_CERT",
        "KB_EMBEDDING_TLS_PINNED_CERT",
    ):
        assert name not in os.environ


def test_kb_embedding_pin_is_scrubbed_by_the_kb_apply(pin):
    emb.apply_kb_embedding_env({"KB_EMBEDDING_TLS_PINNED_CERT": pin.wire})
    assert os.environ["KB_EMBEDDING_TLS_PINNED_CERT"] == pin.wire
    emb.apply_kb_embedding_env({"KB_EMBEDDING_BASE_URL": "https://b.example"})
    assert "KB_EMBEDDING_TLS_PINNED_CERT" not in os.environ
    emb.apply_kb_embedding_env(None)
    assert "KB_EMBEDDING_BASE_URL" not in os.environ


def test_whisper_pin_is_scrubbed_by_the_whisper_override(pin):
    session_attach.apply_session_whisper_env(
        {"WHISPER_BASE_URL": "https://w.example", "WHISPER_TLS_PINNED_CERT": pin.wire}
    )
    assert os.environ["WHISPER_TLS_PINNED_CERT"] == pin.wire
    session_attach.apply_session_whisper_env({"WHISPER_BASE_URL": "https://x.example"})
    assert "WHISPER_TLS_PINNED_CERT" not in os.environ


def test_whisper_signature_notices_a_pin_change(pin, monkeypatch):
    from agent.services import audio_helper

    before = audio_helper._whisper_env_signature()
    monkeypatch.setenv("WHISPER_TLS_PINNED_CERT", pin.wire)
    assert audio_helper._whisper_env_signature() != before
