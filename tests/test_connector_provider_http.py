"""The provider calls of the provider-minted drivers (connector drivers C5).

Every call SRW makes to a Kubernetes API server or GitHub with a connector's
address runs under one deadline, reads a capped answer, dials an address it
checked (with the name kept for the Host header and TLS) and says one of a
fixed set of reasons when it fails.
"""

from __future__ import annotations

import asyncio
import logging
import time

import httpx
import pytest

from orchestrator.services.connector_drivers import provider_http
from orchestrator.services.connector_drivers import token_request as kube_calls
from orchestrator.services.connector_drivers.provider_http import (
    MAX_BODY_BYTES,
    ProviderError,
    ProviderNetwork,
    provider_request,
)
from orchestrator.services.connector_drivers.token_request import (
    mint_token,
    parse_minting_kubeconfig,
)
from shared.connectors.token_request import parse_token_request
from tests._provider_fakes import (
    FakeKubeApi,
    ProviderRouter,
    fake_resolver,
    minting_kubeconfig_yaml,
)

WHO = "the provider"
URL = "https://provider.test:8443/thing"
PUBLIC = "203.0.113.5"


def _route(
    monkeypatch,
    handler,
    *,
    addresses: dict[str, tuple[str, ...]] | None = None,
    private_hosts: tuple[str, ...] = (),
) -> list[httpx.Request]:
    """Send provider calls to ``handler`` (sync or async), resolving names
    from ``addresses``; returns the requests it saw."""
    seen: list[httpx.Request] = []

    async def record(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        answer = handler(request)
        if asyncio.iscoroutine(answer):
            answer = await answer
        return answer

    transport = httpx.MockTransport(record)

    def make(*, verify=True, timeout=10.0) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=transport, timeout=timeout, follow_redirects=False
        )

    monkeypatch.setitem(provider_http._state, "factory", make)
    monkeypatch.setitem(
        provider_http._state,
        "network",
        ProviderNetwork(
            resolver=fake_resolver(
                addresses if addresses is not None else {"provider.test": (PUBLIC,)}
            ),
            private_hosts=frozenset(private_hosts),
        ),
    )
    return seen


def _ok(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json={"ok": True})


async def _call(url: str = URL, **kwargs) -> provider_http.ProviderAnswer:
    return await provider_request("GET", url, who=WHO, headers={}, **kwargs)


class TestDeadline:
    @pytest.mark.asyncio
    async def test_a_trickling_answer_ends_at_the_deadline(self, monkeypatch):
        async def trickle():
            while True:
                yield b" "
                await asyncio.sleep(0.05)

        _route(monkeypatch, lambda request: httpx.Response(200, content=trickle()))
        started = time.monotonic()
        with pytest.raises(ProviderError) as caught:
            await _call(deadline=0.3)
        assert time.monotonic() - started < 2
        assert caught.value.reason == "timeout" and caught.value.transient is True
        assert str(caught.value) == "the provider did not answer in time"

    @pytest.mark.asyncio
    async def test_a_slow_resolver_counts_against_the_same_deadline(self, monkeypatch):
        _route(monkeypatch, _ok)

        async def slow(host: str, ipv6: bool):
            await asyncio.sleep(3600)
            return (PUBLIC,)

        monkeypatch.setitem(
            provider_http._state, "network", ProviderNetwork(resolver=slow)
        )
        with pytest.raises(ProviderError) as caught:
            await _call(deadline=0.2)
        assert caught.value.reason == "timeout"

    @pytest.mark.asyncio
    async def test_the_default_deadline_is_read_per_call(self, monkeypatch):
        async def hang(request):
            await asyncio.sleep(3600)

        _route(monkeypatch, hang)
        monkeypatch.setattr(provider_http, "DEFAULT_DEADLINE_SECONDS", 0.2)
        with pytest.raises(ProviderError) as caught:
            await _call()
        assert caught.value.reason == "timeout"

    @pytest.mark.asyncio
    async def test_an_answer_past_the_cap_is_refused_without_reading_it_all(
        self, monkeypatch
    ):
        produced = []

        async def huge():
            for _ in range(10_000):
                produced.append(1)
                yield b"x" * 8192

        _route(monkeypatch, lambda request: httpx.Response(200, content=huge()))
        with pytest.raises(ProviderError) as caught:
            await _call()
        assert caught.value.reason == "too_large" and caught.value.transient is False
        # The stream stopped just past the cap, not at its end (80 MB).
        assert len(produced) * 8192 <= MAX_BODY_BYTES + 2 * 8192

    @pytest.mark.asyncio
    async def test_an_answer_at_the_cap_is_read(self, monkeypatch):
        _route(
            monkeypatch,
            lambda request: httpx.Response(200, content=b"x" * MAX_BODY_BYTES),
        )
        answer = await _call()
        assert answer.status == 200 and len(answer.body) == MAX_BODY_BYTES

    @pytest.mark.asyncio
    async def test_a_hanging_cleanup_after_a_refused_mint_is_bounded(self, monkeypatch):
        kube = FakeKubeApi()
        kube.forbidden.add("token")
        router = ProviderRouter(kube=kube)

        async def handler(request):
            if request.method == "DELETE":
                await asyncio.sleep(3600)
            return router(request)

        _route(monkeypatch, handler, addresses={"kube.test": (PUBLIC,)})
        monkeypatch.setattr(kube_calls, "CLEANUP_DEADLINE_SECONDS", 0.2)
        started = time.monotonic()
        with pytest.raises(ProviderError) as caught:
            await mint_token(
                parse_minting_kubeconfig(minting_kubeconfig_yaml(kube.minting_token)),
                parse_token_request(
                    {"namespace": "srw-identities", "service_account": "agent"}
                ),
                secret="srw-mint-h",
                credential_id="c-h",
                annotations={},
            )
        assert time.monotonic() - started < 2
        # The TokenRequest's refusal, not the clean-up's timeout; the Secret
        # is left for the sweep (the caller's record names it).
        assert "HTTP 403" in str(caught.value)
        assert "srw-mint-h" in kube.secrets


class TestAddresses:
    @pytest.mark.asyncio
    async def test_the_call_dials_the_checked_address_with_the_name_for_tls(
        self, monkeypatch
    ):
        seen = _route(monkeypatch, _ok)
        answer = await _call()
        assert answer.status == 200 and answer.json() == {"ok": True}
        (request,) = seen
        assert request.url.host == PUBLIC and request.url.port == 8443
        assert request.url.path == "/thing"
        assert request.headers["host"] == "provider.test:8443"
        assert request.headers["connection"] == "close"
        assert request.extensions["sni_hostname"] == "provider.test"

    @pytest.mark.asyncio
    async def test_a_tls_server_name_is_kept_for_the_pinned_dial(self, monkeypatch):
        seen = _route(monkeypatch, _ok)
        await _call(sni_hostname="kubernetes")
        assert seen[0].extensions["sni_hostname"] == "kubernetes"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("address", "allow_private"),
        [
            ("10.1.2.3", False),
            ("192.168.1.10", False),
            ("100.64.0.9", False),
            # The cluster's pod and service ranges, metadata and loopback:
            # never, whatever the project's tier.
            ("10.43.0.1", True),
            ("10.42.7.7", True),
            ("169.254.169.254", True),
            ("168.63.129.16", True),
            ("127.0.0.1", True),
            ("0.0.0.0", True),
        ],
    )
    async def test_an_address_the_projects_may_not_reach_is_refused(
        self, monkeypatch, address, allow_private
    ):
        seen = _route(monkeypatch, _ok, addresses={"provider.test": (address,)})
        with pytest.raises(ProviderError) as caught:
            await _call(allow_private=allow_private)
        assert caught.value.reason == "address_refused"
        assert caught.value.transient is False
        assert address not in str(caught.value)
        assert seen == []

    @pytest.mark.asyncio
    async def test_a_private_address_on_a_tier_that_allows_it(self, monkeypatch):
        seen = _route(monkeypatch, _ok, addresses={"provider.test": ("192.168.1.10",)})
        await _call(allow_private=True)
        assert seen[0].url.host == "192.168.1.10"

    @pytest.mark.asyncio
    async def test_every_answer_of_the_lookup_must_pass(self, monkeypatch):
        seen = _route(
            monkeypatch, _ok, addresses={"provider.test": (PUBLIC, "10.0.0.5")}
        )
        with pytest.raises(ProviderError, match="not one this connector"):
            await _call()
        assert seen == []

    @pytest.mark.asyncio
    async def test_a_literal_address_is_checked_and_dialled_as_it_is(self, monkeypatch):
        seen = _route(monkeypatch, _ok, addresses={})
        with pytest.raises(ProviderError) as caught:
            await _call("https://10.0.0.5/thing")
        assert caught.value.reason == "address_refused"
        await _call("https://203.0.113.9/thing")
        assert seen[0].url.host == "203.0.113.9"
        assert "sni_hostname" not in seen[0].extensions

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("listed", "url"),
        [
            ("kubernetes.default.svc", "https://kubernetes.default.svc/x"),
            ("kubernetes.default.svc:443", "https://kubernetes.default.svc/x"),
            ("kubernetes.default.svc:6443", "https://kubernetes.default.svc:6443/x"),
        ],
    )
    async def test_an_operator_listed_host_may_be_in_the_cluster(
        self, monkeypatch, listed, url
    ):
        seen = _route(
            monkeypatch,
            _ok,
            addresses={"kubernetes.default.svc": ("10.43.0.1",)},
            private_hosts=(listed,),
        )
        await _call(url)
        assert seen[0].url.host == "10.43.0.1"

    @pytest.mark.asyncio
    async def test_a_listing_names_its_port(self, monkeypatch):
        _route(
            monkeypatch,
            _ok,
            addresses={"kubernetes.default.svc": ("10.43.0.1",)},
            private_hosts=("kubernetes.default.svc:443",),
        )
        with pytest.raises(ProviderError, match="not one this connector"):
            await _call("https://kubernetes.default.svc:6443/x")

    @pytest.mark.asyncio
    async def test_a_listed_host_never_reaches_metadata_or_loopback(self, monkeypatch):
        for address in ("169.254.169.254", "127.0.0.1"):
            _route(
                monkeypatch,
                _ok,
                addresses={"kubernetes.default.svc": (address,)},
                private_hosts=("kubernetes.default.svc",),
            )
            with pytest.raises(ProviderError) as caught:
                await _call("https://kubernetes.default.svc/x")
            assert caught.value.reason == "address_refused"

    @pytest.mark.asyncio
    async def test_a_name_that_does_not_resolve(self, monkeypatch):
        _route(monkeypatch, _ok, addresses={})
        with pytest.raises(ProviderError) as caught:
            await _call()
        assert caught.value.reason == "does_not_resolve"
        assert caught.value.transient is True
        assert str(caught.value) == "the provider's host does not resolve"

    @pytest.mark.asyncio
    async def test_the_next_address_is_tried_when_one_does_not_connect(
        self, monkeypatch
    ):
        def handler(request):
            if request.url.host == "203.0.113.5":
                raise httpx.ConnectError("refused", request=request)
            return httpx.Response(200, json={})

        seen = _route(
            monkeypatch,
            handler,
            addresses={"provider.test": ("203.0.113.5", "203.0.113.6")},
        )
        assert (await _call()).status == 200
        assert [request.url.host for request in seen] == [
            "203.0.113.5",
            "203.0.113.6",
        ]

    @pytest.mark.asyncio
    async def test_a_server_no_address_answers_is_transient(self, monkeypatch):
        def handler(request):
            raise httpx.ConnectError(
                "connection refused to 203.0.113.5", request=request
            )

        _route(monkeypatch, handler)
        with pytest.raises(ProviderError) as caught:
            await _call()
        assert caught.value.reason == "unreachable" and caught.value.transient
        assert "203.0.113.5" not in str(caught.value)


class TestReasons:
    @pytest.mark.asyncio
    async def test_an_untrusted_certificate_is_final(self, monkeypatch, caplog):
        def handler(request):
            raise httpx.ConnectError(
                "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: "
                "self-signed certificate in chain for internal.corp",
                request=request,
            )

        _route(monkeypatch, handler)
        with caplog.at_level(logging.INFO):
            with pytest.raises(ProviderError) as caught:
                await _call()
        assert caught.value.reason == "certificate"
        assert caught.value.transient is False
        assert "internal.corp" not in str(caught.value)
        # The raw detail is in the server log, for the operator.
        assert "internal.corp" in caplog.text

    @pytest.mark.asyncio
    async def test_a_ca_that_does_not_load_is_final(self, monkeypatch):
        seen = _route(monkeypatch, _ok)
        with pytest.raises(ProviderError) as caught:
            await _call(ca_pem="-----BEGIN CERTIFICATE-----\nnot one\n")
        assert caught.value.reason == "ca_unusable"
        assert seen == []

    def test_every_reason_is_a_fixed_text(self):
        for reason in provider_http._TEXTS:
            error = provider_http.failure(reason, WHO, transient=False)
            assert error.reason == reason and "{" not in str(error)

    @pytest.mark.asyncio
    async def test_a_redirect_is_not_followed(self, monkeypatch):
        seen = _route(
            monkeypatch,
            lambda request: httpx.Response(
                302, headers={"location": "http://169.254.169.254/"}
            ),
        )
        assert (await _call()).status == 302
        assert len(seen) == 1
