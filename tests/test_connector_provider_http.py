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


def _gzip_bomb(megabytes: int) -> bytes:
    import gzip

    return gzip.compress(b"\0" * (megabytes << 20), compresslevel=9)


def _zstd_bomb(megabytes: int) -> bytes:
    zstandard = pytest.importorskip("zstandard")
    return zstandard.ZstdCompressor(level=19).compress(b"\0" * (megabytes << 20))


class TestContentCoding:
    @pytest.mark.asyncio
    async def test_srw_asks_for_no_content_coding(self, monkeypatch):
        seen = _route(monkeypatch, _ok)
        await _call()
        assert seen[0].headers["accept-encoding"] == "identity"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("coding", ["gzip", "zstd", "deflate", "br", "x-custom"])
    async def test_a_coded_answer_is_refused_and_never_decoded(
        self, monkeypatch, coding
    ):
        body = {
            "gzip": lambda: _gzip_bomb(8),
            "zstd": lambda: _zstd_bomb(8),
        }.get(coding, lambda: b"\x00" * 64)()
        assert len(body) < MAX_BODY_BYTES  # small on the wire

        decoded = []

        def handler(request):
            # A stream, as a network transport hands it over (content= would
            # be read, and decoded, when the test builds the answer).
            return httpx.Response(
                201,
                headers={"content-encoding": coding},
                stream=httpx.ByteStream(body),
            )

        _route(monkeypatch, handler)
        real_decoders = dict(httpx._decoders.SUPPORTED_DECODERS)
        for name, decoder in real_decoders.items():

            class Spy(decoder):  # type: ignore[valid-type, misc]
                def decode(self, data: bytes) -> bytes:
                    decoded.append(len(data))
                    return super().decode(data)

            monkeypatch.setitem(httpx._decoders.SUPPORTED_DECODERS, name, Spy)
        with pytest.raises(ProviderError) as caught:
            await _call()
        assert caught.value.reason == "malformed"
        assert caught.value.transient is False
        assert decoded == []

    @pytest.mark.asyncio
    async def test_identity_named_explicitly_is_read(self, monkeypatch):
        _route(
            monkeypatch,
            lambda request: httpx.Response(
                200, headers={"content-encoding": "identity"}, content=b"{}"
            ),
        )
        assert (await _call()).json() == {}


async def _serve(handler) -> tuple[asyncio.AbstractServer, int]:
    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1]


class TestOnTheWire:
    """The real client over a real socket (loopback let through: these test
    the reading, not the address policy)."""

    @pytest.fixture
    def loopback(self, monkeypatch):
        async def local(host: str, ipv6: bool):
            return ("127.0.0.1",)

        monkeypatch.setattr(provider_http, "refusal", lambda address, policy: None)
        monkeypatch.setitem(
            provider_http._state, "factory", provider_http._default_client
        )
        monkeypatch.setitem(
            provider_http._state, "network", ProviderNetwork(resolver=local)
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("coding", ["gzip", "zstd"])
    async def test_a_compression_bomb_is_refused_before_it_is_inflated(
        self, loopback, coding
    ):
        import resource

        body = _gzip_bomb(64) if coding == "gzip" else _zstd_bomb(256)
        requests: list[bytes] = []

        async def handle(reader, writer):
            requests.append(await reader.readuntil(b"\r\n\r\n"))
            writer.write(
                b"HTTP/1.1 201 Created\r\nContent-Encoding: %s\r\n"
                b"Content-Length: %d\r\n\r\n" % (coding.encode(), len(body)) + body
            )
            try:
                await writer.drain()
            except ConnectionError:
                pass
            writer.close()

        server, port = await _serve(handle)
        before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        try:
            with pytest.raises(ProviderError) as caught:
                await _call(f"http://bomb.test:{port}/x")
        finally:
            server.close()
        grown_mib = (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss - before) / 1024
        assert caught.value.reason == "malformed"
        assert grown_mib < 32, grown_mib
        assert b"accept-encoding: identity" in requests[0].lower()

    @pytest.mark.asyncio
    async def test_a_large_plain_answer_is_cut_at_the_cap_on_the_wire(self, loopback):
        async def handle(reader, writer):
            await reader.readuntil(b"\r\n\r\n")
            writer.write(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
            try:
                for _ in range(256):
                    writer.write(b"4000\r\n" + b"x" * 0x4000 + b"\r\n")
                    await writer.drain()
                writer.write(b"0\r\n\r\n")
                await writer.drain()
            except ConnectionError:
                pass
            writer.close()

        server, port = await _serve(handle)
        try:
            with pytest.raises(ProviderError) as caught:
                await _call(f"http://big.test:{port}/x")
        finally:
            server.close()
        assert caught.value.reason == "too_large"

    @pytest.mark.asyncio
    async def test_a_plain_answer_is_read_from_the_wire(self, loopback):
        async def handle(reader, writer):
            await reader.readuntil(b"\r\n\r\n")
            writer.write(
                b"HTTP/1.1 201 Created\r\nContent-Length: 12\r\n\r\n" + b'{"ok": true}'
            )
            await writer.drain()
            writer.close()

        server, port = await _serve(handle)
        try:
            answer = await _call(f"http://plain.test:{port}/x")
        finally:
            server.close()
        assert answer.status == 201 and answer.json() == {"ok": True}


class TestResolver:
    @pytest.mark.asyncio
    async def test_lookups_run_on_their_own_bounded_threads(self, monkeypatch):
        import socket
        import threading

        release = threading.Event()
        names: list[str] = []

        def hung(*args, **kwargs):
            names.append(threading.current_thread().name)
            release.wait(10)
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("203.0.113.5", 0))]

        monkeypatch.setattr(socket, "getaddrinfo", hung)
        try:
            lookups = [
                asyncio.ensure_future(provider_http.provider_resolver("x.test", False))
                for _ in range(provider_http.RESOLVER_THREADS)
            ]
            await asyncio.sleep(0.2)
            # Every resolver thread is held: the next lookup fails at once,
            # and the loop's shared executor was never used.
            started = time.monotonic()
            with pytest.raises(OSError, match="busy"):
                await provider_http.provider_resolver("y.test", False)
            assert time.monotonic() - started < 0.5
            assert all(name.startswith("srw-provider-resolve") for name in names)
        finally:
            release.set()
        assert [await lookup for lookup in lookups] == [
            ["203.0.113.5"]
        ] * provider_http.RESOLVER_THREADS
        # The threads are free again.
        assert await provider_http.provider_resolver("z.test", False) == ["203.0.113.5"]

    @pytest.mark.asyncio
    async def test_a_connectors_tests_never_hold_a_mints_threads(self, monkeypatch):
        """Any user may start Tests: with every Test thread held by a name
        that never answers, a mint's lookup still runs at once."""
        import socket
        import threading

        release = threading.Event()
        names: list[str] = []

        def lookup(host, *args, **kwargs):
            names.append(threading.current_thread().name)
            if host.startswith("hang"):
                release.wait(10)
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("203.0.113.5", 0))]

        monkeypatch.setattr(socket, "getaddrinfo", lookup)
        network = ProviderNetwork()
        test_lane = network.resolver_for(provider_http.LANE_TEST)
        assert test_lane is provider_http.connector_test_resolver
        assert (
            network.resolver_for(provider_http.LANE_PROVIDER)
            is provider_http.provider_resolver
        )
        try:
            held = [
                asyncio.ensure_future(test_lane(f"hang{i}.test", False))
                for i in range(provider_http.RESOLVER_THREADS)
            ]
            await asyncio.sleep(0.2)
            with pytest.raises(OSError, match="busy"):
                await test_lane("next.test", False)
            started = time.monotonic()
            assert await provider_http.provider_resolver("mint.test", False) == [
                "203.0.113.5"
            ]
            assert time.monotonic() - started < 0.5
        finally:
            release.set()
        for lookup_future in held:
            await lookup_future
        assert (
            sum(name.startswith("srw-test-probe-resolve") for name in names)
            == provider_http.RESOLVER_THREADS
        )
        assert sum(name.startswith("srw-provider-resolve") for name in names) == 1

    def test_an_installed_resolver_serves_every_lane(self):
        installed = fake_resolver({})
        network = ProviderNetwork(resolver=installed)
        assert network.resolver_for(provider_http.LANE_TEST) is installed
        assert network.resolver_for(provider_http.LANE_PROVIDER) is installed
