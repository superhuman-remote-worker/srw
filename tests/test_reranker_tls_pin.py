"""The reranker verifies the pin of the URL it actually calls (Review Focus 3).

``resolve_reranker_transport`` returns the pin with the model, URL and key:
the embedding fallback takes ``EMBEDDING_TLS_PINNED_CERT``, ``RERANK_BASE_URL``
takes ``RERANK_TLS_PINNED_CERT``, and an explicit ``memory.reranker.base_url``
takes the pin of the paired prefix's endpoint it lies within, else none. A pin
that can't be read, or that can't apply, means no scorer and no request.
"""
# ruff: noqa: F811  (the imported fixture is requested by name as a test argument)

from __future__ import annotations

from types import SimpleNamespace

import pytest

from agent.services.memory.plugins.reranker import (
    RerankerScorer,
    resolve_reranker_transport,
)
from shared.tls_pinning import UNREADABLE_PIN, CertificateChangedError, TlsPin
from tests.tls_pin_fixtures import tls_certs, tls_server


def _pin(path) -> TlsPin:
    return TlsPin.from_pem(path.read_text())


def _cfg(**kw):
    return SimpleNamespace(**{"model": None, "base_url": None, "api_key": None, **kw})


def _caused_by(exc: BaseException, cls: type) -> bool:
    seen: set[int] = set()
    cur = exc
    while cur is not None and id(cur) not in seen:
        if isinstance(cur, cls):
            return True
        seen.add(id(cur))
        cur = cur.__cause__ or cur.__context__
    return False


@pytest.fixture
def two_pins(tls_certs):
    return _pin(tls_certs.leaf), _pin(tls_certs.swap)


class TestPinFollowsTheUrlTheRerankerCalls:
    def test_embedding_fallback_uses_the_embedding_pin(self, two_pins):
        emb, rr = two_pins
        env = {
            "EMBEDDING_BASE_URL": "https://emb.example/v1",
            "EMBEDDING_TLS_PINNED_CERT": emb.wire,
            "RERANK_TLS_PINNED_CERT": rr.wire,  # no RERANK_BASE_URL: not in play
        }
        _, url, _, pin = resolve_reranker_transport(_cfg(), env=env)
        assert url == "https://emb.example/v1"
        assert pin == emb

    def test_rerank_base_url_uses_the_rerank_pin(self, two_pins):
        emb, rr = two_pins
        env = {
            "RERANK_BASE_URL": "https://rr.example/v1",
            "RERANK_TLS_PINNED_CERT": rr.wire,
            "EMBEDDING_BASE_URL": "https://emb.example/v1",
            "EMBEDDING_TLS_PINNED_CERT": emb.wire,
        }
        _, url, _, pin = resolve_reranker_transport(_cfg(), env=env)
        assert url == "https://rr.example/v1"
        assert pin == rr

    def test_explicit_url_on_another_origin_gets_no_pin(self, two_pins):
        emb, rr = two_pins
        env = {
            "RERANK_BASE_URL": "https://rr.example/v1",
            "RERANK_TLS_PINNED_CERT": rr.wire,
            "EMBEDDING_BASE_URL": "https://emb.example/v1",
            "EMBEDDING_TLS_PINNED_CERT": emb.wire,
        }
        cfg = _cfg(base_url="https://other.example/v1")
        assert resolve_reranker_transport(cfg, env=env)[3] is None

    def test_explicit_url_within_a_prefix_gets_that_prefixes_pin(self, two_pins):
        emb, rr = two_pins
        env = {
            "RERANK_BASE_URL": "https://rr.example/v1",
            "RERANK_TLS_PINNED_CERT": rr.wire,
            "EMBEDDING_BASE_URL": "https://emb.example/v1",
            "EMBEDDING_TLS_PINNED_CERT": emb.wire,
        }
        got = resolve_reranker_transport(
            _cfg(base_url="https://emb.example/v1/rerank-x"), env=env
        )
        assert got[3] == emb
        got = resolve_reranker_transport(
            _cfg(base_url="https://rr.example/v1"), env=env
        )
        assert got[3] == rr

    def test_no_pin_set_is_none(self):
        env = {"EMBEDDING_BASE_URL": "https://emb.example/v1"}
        assert resolve_reranker_transport(_cfg(), env=env)[3] is None

    @pytest.mark.parametrize("value", [UNREADABLE_PIN, "not-base64!"])
    def test_a_broken_pin_raises_instead_of_going_unpinned(self, value):
        env = {
            "RERANK_BASE_URL": "https://rr.example/v1",
            "RERANK_TLS_PINNED_CERT": value,
        }
        with pytest.raises(ValueError, match="RERANK_TLS_PINNED_CERT"):
            resolve_reranker_transport(_cfg(), env=env)

    def test_a_broken_pin_of_an_unrelated_prefix_is_not_read(self, two_pins):
        _, rr = two_pins
        env = {
            "RERANK_BASE_URL": "https://rr.example/v1",
            "RERANK_TLS_PINNED_CERT": rr.wire,
            "EMBEDDING_BASE_URL": "https://emb.example/v1",
            "EMBEDDING_TLS_PINNED_CERT": UNREADABLE_PIN,
        }
        assert resolve_reranker_transport(_cfg(), env=env)[3] == rr


class TestScorerVerifiesThePin:
    @pytest.mark.asyncio
    async def test_reaches_the_pinned_server(self, tls_certs):
        with tls_server(tls_certs.leaf, tls_certs.leaf_key) as server:
            scorer = RerankerScorer(
                model="m",
                base_url=server.url,
                api_key="k",
                tls_pin=_pin(tls_certs.leaf),
            )
            ranked = await scorer._rerank("q", ["a"])
            await scorer._client.aclose()
        assert ranked == [(0, 0.9)]
        assert server.requests[0]["authorization"] == "Bearer k"

    @pytest.mark.asyncio
    async def test_swapped_certificate_fails_without_a_request(self, tls_certs):
        with tls_server(tls_certs.swap, tls_certs.swap_key) as server:
            scorer = RerankerScorer(
                model="m",
                base_url=server.url,
                api_key="secret",
                tls_pin=_pin(tls_certs.leaf),
                retries=2,
                retry_backoff=0,
            )
            with pytest.raises(Exception) as exc:
                await scorer._rerank("q", ["a"])
            await scorer._client.aclose()
        assert isinstance(exc.value, CertificateChangedError) or _caused_by(
            exc.value, CertificateChangedError
        )
        assert server.requests == []

    def test_a_pin_on_an_http_url_is_refused(self, tls_certs):
        with pytest.raises(ValueError, match="https"):
            RerankerScorer(
                model="m",
                base_url="http://rr.example/v1",
                api_key="k",
                tls_pin=_pin(tls_certs.leaf),
            )

    def test_build_with_the_unreadable_marker_builds_no_scorer(self, monkeypatch):
        from agent.services.memory.plugins.reranker import _build_reranker

        monkeypatch.setenv("RERANK_BASE_URL", "https://rr.example/v1")
        monkeypatch.setenv("RERANK_TLS_PINNED_CERT", UNREADABLE_PIN)
        runtime = SimpleNamespace(
            memory_config=SimpleNamespace(
                reranker=_cfg(top_k=5, timeout=5, retries=0, retry_backoff=0)
            )
        )
        with pytest.raises(ValueError, match="RERANK_TLS_PINNED_CERT"):
            _build_reranker(runtime)
