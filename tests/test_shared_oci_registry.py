"""The shared OCI resolver: digests plus the image config (connector drivers D5).

VM preparation's behaviour is pinned by tests/test_vm_preparation_registry.py;
these cover what driver hosting adds: any registry, the config blob with the
entrypoint, command and labels, its digest check, and the blob redirect.
"""

from __future__ import annotations

import hashlib
import json

import httpx
import pytest

from shared.oci_registry import (
    RegistryResolutionError,
    RegistryResolver,
    image_config,
)


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
        if request.url.host == "storage.example":
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
    resolver = RegistryResolver(hosts=None, transport=httpx.MockTransport(handler))
    image = await resolver.resolve_image("anywhere.example/team/echo:1.0")
    assert image.digest == _digest(manifest)
    assert image.reference == "anywhere.example/team/echo@" + _digest(manifest)
    assert image.entrypoint == ("/srw-driver-echo",)
    assert image.cmd == ("--port", "8080")
    assert image.labels == {"io.srw.driver.spec": '{"name": "srw.echo-service/v1"}'}
    assert image.user == "65532"


@pytest.mark.asyncio
async def test_a_substituted_config_blob_is_refused():
    config = _config(Entrypoint=["/a"])
    manifest = _manifest(config)

    def handler(request):
        if "/manifests/" in request.url.path:
            return httpx.Response(200, content=manifest)
        return httpx.Response(200, content=_config(Entrypoint=["/evil"]))

    resolver = RegistryResolver(hosts=None, transport=httpx.MockTransport(handler))
    with pytest.raises(RegistryResolutionError, match="config digest"):
        await resolver.resolve_image("anywhere.example/team/echo:1.0")


@pytest.mark.asyncio
async def test_a_blob_redirect_is_followed_once_over_https_without_the_token():
    config = _config(Entrypoint=["/a"])
    seen: list = []
    _manifest_bytes, handler = _registry(
        config, seen=seen, redirect="https://storage.example/blob?sig=1"
    )
    resolver = RegistryResolver(hosts=None, transport=httpx.MockTransport(handler))
    image = await resolver.resolve_image("anywhere.example/team/echo:1.0")
    assert image.entrypoint == ("/a",)
    storage = [r for r in seen if r.url.host == "storage.example"]
    assert len(storage) == 1
    assert "Authorization" not in storage[0].headers


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "location", ["http://storage.example/blob", "https://u:p@storage.example/blob"]
)
async def test_an_unsafe_blob_redirect_is_refused(location):
    config = _config(Entrypoint=["/a"])
    _manifest_bytes, handler = _registry(config, redirect=location)
    resolver = RegistryResolver(hosts=None, transport=httpx.MockTransport(handler))
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

    same_host = RegistryResolver(
        hosts=None, same_host_tokens=True, transport=httpx.MockTransport(handler)
    )
    image = await same_host.resolve_image("ghcr.example/org/echo:1")
    assert image.digest == _digest(manifest)
    strict = RegistryResolver(hosts=None, transport=httpx.MockTransport(handler))
    with pytest.raises(RegistryResolutionError, match="token endpoint"):
        await strict.resolve_image("ghcr.example/org/echo:1")


@pytest.mark.asyncio
async def test_insecure_hosts_use_plain_http_only_when_named():
    config = _config()
    seen: list = []
    _manifest_bytes, handler = _registry(config, seen=seen)
    resolver = RegistryResolver(
        hosts=None,
        insecure_hosts=["srw-registry:5000"],
        transport=httpx.MockTransport(handler),
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
        "user": None,
    }


@pytest.mark.asyncio
async def test_vm_preparation_keeps_its_registry_allow_list():
    from vm_controller.preparation_registry import RegistryResolver as Preparation

    resolver = Preparation(hosts=["registry.example"])
    with pytest.raises(RegistryResolutionError, match="not enabled for preparation"):
        await resolver.resolve("elsewhere.example/base:1")
