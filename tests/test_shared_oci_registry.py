"""The shared OCI resolver: digests plus the image config (connector drivers D5).

VM preparation's behaviour is pinned by tests/test_vm_preparation_registry.py;
these cover what driver hosting adds: any registry, the config blob with the
entrypoint, command and labels, its digest check, the blob redirect, and the
address checks every request of a resolver without an allow-list passes.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json

import httpx
import pytest

from shared.oci_registry import (
    RegistryResolutionError,
    RegistryResolver,
    address_refusal,
    image_config,
)

#: The fake DNS every test resolver uses: never the system resolver.
DNS = {
    "anywhere.example": ["93.184.216.34"],
    "storage.example": ["93.184.216.35"],
    "ghcr.example": ["140.82.112.33"],
    "other": ["93.184.216.36"],
    "srw-registry": ["172.18.0.5"],
    "internal.example": ["10.0.0.5"],
    "tokens.internal": ["10.0.0.6"],
    "storage.internal": ["192.168.1.10"],
    "mixed.example": ["93.184.216.37", "10.0.0.7"],
    "pods.example": ["11.1.2.3"],
    "two.example": ["93.184.216.40", "93.184.216.41"],
}


async def fake_dns(host: str):
    if host not in DNS:
        raise OSError("Name or service not known")
    return DNS[host]


def _resolver(handler, **kwargs) -> RegistryResolver:
    kwargs.setdefault("hosts", None)
    return RegistryResolver(
        transport=httpx.MockTransport(handler), host_resolver=fake_dns, **kwargs
    )


def _name(request: httpx.Request) -> str:
    """The host a request is for (it dials the checked address)."""
    return request.headers.get("Host", request.url.host).split(":")[0]


def _digest(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


def _config(**config) -> bytes:
    return json.dumps(
        {"architecture": "amd64", "os": "linux", "config": config}
    ).encode()


def _manifest(config: bytes) -> bytes:
    return json.dumps(
        {
            "schemaVersion": 2,
            "config": {"digest": _digest(config), "size": len(config)},
            "layers": [],
        }
    ).encode()


def _registry(config: bytes, *, seen: list | None = None, redirect: str | None = None):
    manifest = _manifest(config)

    def handler(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append(request)
        path = request.url.path
        if "/manifests/" in path:
            return httpx.Response(
                200,
                content=manifest,
                headers={"Docker-Content-Digest": _digest(manifest)},
            )
        if path.endswith("/blobs/" + _digest(config)):
            if redirect:
                return httpx.Response(307, headers={"Location": redirect})
            return httpx.Response(200, content=config)
        if _name(request) == "storage.example":
            return httpx.Response(200, content=config)
        return httpx.Response(404)

    return manifest, handler


@pytest.mark.asyncio
async def test_any_registry_resolves_the_digest_and_the_runtime_config():
    config = _config(
        Entrypoint=["/srw-driver-echo"],
        Cmd=["--port", "8080"],
        Labels={"io.srw.driver.spec": '{"name": "srw.echo-service/v1"}'},
        User="65532",
    )
    manifest, handler = _registry(config)
    resolver = _resolver(handler)
    image = await resolver.resolve_image("anywhere.example/team/echo:1.0")
    assert image.digest == _digest(manifest)
    assert image.reference == "anywhere.example/team/echo@" + _digest(manifest)
    assert image.entrypoint == ("/srw-driver-echo",)
    assert image.cmd == ("--port", "8080")
    assert image.labels == {"io.srw.driver.spec": '{"name": "srw.echo-service/v1"}'}


@pytest.mark.asyncio
async def test_a_substituted_config_blob_is_refused():
    config = _config(Entrypoint=["/a"])
    manifest = _manifest(config)

    def handler(request):
        if "/manifests/" in request.url.path:
            return httpx.Response(200, content=manifest)
        return httpx.Response(200, content=_config(Entrypoint=["/evil"]))

    resolver = _resolver(handler)
    with pytest.raises(RegistryResolutionError, match="config digest"):
        await resolver.resolve_image("anywhere.example/team/echo:1.0")


@pytest.mark.asyncio
async def test_a_blob_redirect_is_followed_once_over_https_without_the_token():
    config = _config(Entrypoint=["/a"])
    seen: list = []
    _manifest_bytes, handler = _registry(
        config, seen=seen, redirect="https://storage.example/blob?sig=1"
    )
    resolver = _resolver(handler)
    image = await resolver.resolve_image("anywhere.example/team/echo:1.0")
    assert image.entrypoint == ("/a",)
    storage = [r for r in seen if _name(r) == "storage.example"]
    assert len(storage) == 1
    assert "Authorization" not in storage[0].headers


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "location", ["http://storage.example/blob", "https://u:p@storage.example/blob"]
)
async def test_an_unsafe_blob_redirect_is_refused(location):
    config = _config(Entrypoint=["/a"])
    _manifest_bytes, handler = _registry(config, redirect=location)
    resolver = _resolver(handler)
    with pytest.raises(RegistryResolutionError, match="redirect"):
        await resolver.resolve_image("anywhere.example/team/echo:1.0")


@pytest.mark.asyncio
async def test_a_registry_may_send_tokens_to_its_own_host_only_when_allowed():
    config = _config()
    manifest = _manifest(config)

    def handler(request):
        if request.url.path == "/token":
            return httpx.Response(200, json={"token": "anon"})
        if request.headers.get("Authorization") != "Bearer anon":
            return httpx.Response(
                401,
                headers={
                    "WWW-Authenticate": 'Bearer realm="https://ghcr.example/token",'
                    'service="ghcr.example"'
                },
            )
        if "/manifests/" in request.url.path:
            return httpx.Response(200, content=manifest)
        return httpx.Response(200, content=config)

    same_host = _resolver(handler, same_host_tokens=True)
    image = await same_host.resolve_image("ghcr.example/org/echo:1")
    assert image.digest == _digest(manifest)
    strict = _resolver(handler)
    with pytest.raises(RegistryResolutionError, match="token endpoint"):
        await strict.resolve_image("ghcr.example/org/echo:1")


@pytest.mark.asyncio
async def test_insecure_hosts_use_plain_http_only_when_named():
    config = _config()
    seen: list = []
    _manifest_bytes, handler = _registry(config, seen=seen)
    resolver = _resolver(
        handler,
        insecure_hosts=["srw-registry:5000"],
        private_hosts=["srw-registry:5000"],
    )
    await resolver.resolve_image("srw-registry:5000/srw-driver-echo:tilt-1")
    assert {r.url.scheme for r in seen} == {"http"}
    seen.clear()
    await resolver.resolve_image("other:5000/srw-driver-echo:tilt-1")
    assert {r.url.scheme for r in seen} == {"https"}


@pytest.mark.parametrize(
    "config",
    [
        {"config": {"Entrypoint": "/bin/sh"}},
        {"config": {"Cmd": [1]}},
        {"config": {"Labels": {"a": 1}}},
        {"config": []},
        [],
    ],
)
def test_malformed_runtime_config_is_refused(config):
    with pytest.raises(RegistryResolutionError):
        image_config(json.dumps(config).encode())


def test_an_image_without_runtime_config_has_empty_fields():
    assert image_config(b'{"architecture": "amd64"}') == {
        "entrypoint": (),
        "cmd": (),
        "labels": {},
    }


@pytest.mark.asyncio
async def test_vm_preparation_keeps_its_registry_allow_list():
    from vm_controller.preparation_registry import RegistryResolver as Preparation

    resolver = Preparation(hosts=["registry.example"])
    with pytest.raises(RegistryResolutionError, match="not enabled for preparation"):
        await resolver.resolve("elsewhere.example/base:1")


@pytest.mark.asyncio
async def test_vm_preparation_trusts_its_allow_list_by_name():
    """The operator's allow-list is its word: no address checks, so an
    approved in-cluster registry still works and nothing is looked up."""
    from vm_controller.preparation_registry import RegistryResolver as Preparation

    seen: list = []
    _manifest_bytes, handler = _registry(_config(), seen=seen)
    resolver = Preparation(
        hosts=["registry.internal"], transport=httpx.MockTransport(handler)
    )
    assert resolver.checks_addresses is False
    await resolver.resolve("registry.internal/base:1")
    assert {r.url.host for r in seen} == {"registry.internal"}


# =============================================================================
# Address checks (any registry, so every request is checked by address)
# =============================================================================


@pytest.mark.parametrize(
    ("address", "listed", "refused"),
    [
        ("93.184.216.34", False, False),
        ("2606:4700::1111", False, False),
        ("10.0.0.5", False, True),
        ("10.0.0.5", True, False),
        ("127.0.0.1", False, True),
        ("127.0.0.1", True, False),
        ("100.64.0.1", False, True),
        ("fd12::1", False, True),
        ("::ffff:10.0.0.1", False, True),
        ("64:ff9b::a00:1", False, True),
        ("2002:a00:1::1", False, True),
        # Never, listed or not.
        ("169.254.169.254", True, True),
        ("fd00:ec2::254", True, True),
        ("168.63.129.16", True, True),
        ("fe80::1", True, True),
        ("0.0.0.0", True, True),
        ("224.0.0.1", True, True),
        # IPv4-mapped addresses are their IPv4 address: listed private ones
        # pass, mapped metadata never does.
        ("::ffff:10.0.0.1", True, False),
        ("::ffff:169.254.169.254", True, True),
        ("::ffff:168.63.129.16", True, True),
        # A NAT64 or 6to4 address carrying a metadata address, listed or not.
        ("64:ff9b::a9fe:a9fe", True, True),
        ("64:ff9b:1::a9fe:a9fe", True, True),
        ("2002:a9fe:a9fe::1", True, True),
        ("64:ff9b::a00:1", True, False),
        # IPv4-compatible IPv6 is deprecated: never.
        ("::a00:1", True, True),
        ("::a9fe:a9fe", False, True),
        # ...but loopback stays loopback.
        ("::1", True, False),
    ],
)
def test_address_refusal(address, listed, refused):
    found = address_refusal(ipaddress.ip_address(address), private_allowed=listed)
    assert (found is not None) is refused


def test_the_cluster_ranges_are_refused_unless_listed():
    pods = [ipaddress.ip_network("11.0.0.0/8")]
    address = ipaddress.ip_address("11.1.2.3")
    assert address_refusal(address, refused=pods) is not None
    assert address_refusal(address, refused=pods, private_allowed=True) is None


@pytest.mark.asyncio
async def test_a_request_dials_the_checked_address_with_its_name_kept():
    """No second lookup can send it elsewhere; TLS still checks the name."""
    seen: list = []
    _manifest_bytes, handler = _registry(_config(), seen=seen)
    await _resolver(handler).resolve_image("anywhere.example/team/echo:1.0")
    assert {r.url.host for r in seen} == {"93.184.216.34"}
    assert {r.headers["Host"] for r in seen} == {"anywhere.example"}
    assert {r.extensions.get("sni_hostname") for r in seen} == {"anywhere.example"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reference",
    [
        "internal.example/team/echo:1",
        "mixed.example/team/echo:1",
        "10.0.0.9:5000/team/echo:1",
        "169.254.169.254/team/echo:1",
        "pods.example/team/echo:1",
    ],
)
async def test_a_registry_at_a_private_or_cluster_address_is_refused(reference):
    def handler(request):
        pytest.fail("A refused address was contacted")

    resolver = _resolver(handler, refused_networks=["11.0.0.0/8"])
    with pytest.raises(RegistryResolutionError) as raised:
        await resolver.resolve_image(reference)
    message = str(raised.value)
    assert message == "Registry address is not allowed."
    # The caller never learns the address.
    assert "10.0.0" not in message and "169.254" not in message


@pytest.mark.asyncio
async def test_a_listed_registry_may_be_private_but_never_metadata():
    seen: list = []
    _manifest_bytes, handler = _registry(_config(), seen=seen)
    resolver = _resolver(
        handler,
        private_hosts=["internal.example", "169.254.169.254"],
        refused_networks=["10.0.0.0/8"],
    )
    await resolver.resolve_image("internal.example/team/echo:1")
    assert {r.url.host for r in seen} == {"10.0.0.5"}
    with pytest.raises(RegistryResolutionError, match="address is not allowed"):
        await resolver.resolve_image("169.254.169.254/team/echo:1")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "location",
    [
        "https://10.0.0.7/blob",
        "https://storage.internal/blob",
        "https://[fd00:ec2::254]/blob",
    ],
)
async def test_a_blob_redirect_to_a_private_address_is_refused(location):
    seen: list = []
    _manifest_bytes, handler = _registry(_config(), seen=seen, redirect=location)
    resolver = _resolver(handler)
    with pytest.raises(RegistryResolutionError, match="address is not allowed"):
        await resolver.resolve_image("anywhere.example/team/echo:1.0")
    assert all(_name(r) == "anywhere.example" for r in seen)


@pytest.mark.asyncio
async def test_a_token_realm_at_a_private_address_is_refused():
    seen: list = []

    def handler(request):
        seen.append(request)
        return httpx.Response(
            401,
            headers={
                "WWW-Authenticate": 'Bearer realm="https://tokens.internal/token"'
            },
        )

    resolver = _resolver(handler, token_hosts=["tokens.internal"])
    with pytest.raises(RegistryResolutionError, match="address is not allowed"):
        await resolver.resolve_image("anywhere.example/team/echo:1.0")
    assert [_name(r) for r in seen] == ["anywhere.example"]


@pytest.mark.asyncio
async def test_the_next_answer_is_dialled_when_one_does_not_connect():
    seen: list = []
    _manifest_bytes, registry = _registry(_config(), seen=seen)

    def handler(request):
        if request.url.host == "93.184.216.40":
            seen.append(request)
            raise httpx.ConnectError("unreachable", request=request)
        return registry(request)

    image = await _resolver(handler).resolve_image("two.example/team/echo:1")
    assert image.entrypoint == ()
    hosts = [r.url.host for r in seen]
    assert hosts[:2] == ["93.184.216.40", "93.184.216.41"]
    assert {r.headers["Host"] for r in seen} == {"two.example"}


@pytest.mark.asyncio
async def test_every_answer_failing_is_an_error():
    def handler(request):
        raise httpx.ConnectError("unreachable", request=request)

    with pytest.raises(httpx.ConnectError):
        await _resolver(handler).resolve_image("two.example/team/echo:1")


@pytest.mark.asyncio
async def test_no_connection_is_kept_alive_across_names():
    """A pool keyed by address would reuse one name's TLS session for
    another name at the same address."""
    seen: list = []
    _manifest_bytes, handler = _registry(_config(), seen=seen)
    await _resolver(handler).resolve_image("anywhere.example/team/echo:1.0")
    assert {r.headers.get("Connection") for r in seen} == {"close"}
    client = RegistryResolver(hosts=None)._client()
    try:
        assert client._transport._pool._max_keepalive_connections == 0
    finally:
        await client.aclose()
    # An operator's allow-list keeps httpx's default pool.
    allowed = RegistryResolver(hosts=["registry.internal"])._client()
    try:
        assert allowed._transport._pool._max_keepalive_connections > 0
    finally:
        await allowed.aclose()


@pytest.mark.asyncio
async def test_errors_are_generic():
    def handler(request):
        return httpx.Response(404)

    resolver = _resolver(handler)
    with pytest.raises(RegistryResolutionError) as raised:
        await resolver.resolve_image("anywhere.example/team/echo:1.0")
    assert str(raised.value) == "Image manifest resolution failed."
    with pytest.raises(RegistryResolutionError) as raised:
        await resolver.resolve_image("nowhere.example/team/echo:1.0")
    assert str(raised.value) == "Registry host does not resolve."
