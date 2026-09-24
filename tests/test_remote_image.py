"""Security and endpoint tests for explicit-consent remote image loading."""

from __future__ import annotations

import io
import socket
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from PIL import Image

from orchestrator.services import remote_image as subject
from orchestrator.application import projects as projects_composition


def _png(width: int = 2, height: int = 3) -> bytes:
    output = io.BytesIO()
    Image.new("RGB", (width, height), "red").save(output, format="PNG")
    return output.getvalue()


@pytest.mark.parametrize(
    "url, code",
    [
        ("http://example.com/a.png", "remote_image_https_required"),
        ("https://user:secret@example.com/a.png", "remote_image_credentials_forbidden"),
        ("https://example.com:8443/a.png", "remote_image_port_forbidden"),
        ("https://example.com/a.png#reviewed", "remote_image_fragment_forbidden"),
        ("https://127.0.0.1/a.png", "remote_image_destination_blocked"),
        ("https://[::1]/a.png", "remote_image_destination_blocked"),
        ("https://localhost/a.png", "remote_image_destination_blocked"),
        (" https://example.com/a.png", "remote_image_url_invalid"),
        ("https://example.com/a.png\nX-Test: yes", "remote_image_url_invalid"),
    ],
)
def test_url_policy_rejects_ambiguous_or_non_public_targets(url: str, code: str):
    with pytest.raises(subject.RemoteImageError) as exc:
        subject.validate_remote_image_url(url)
    assert exc.value.code == code
    assert url not in exc.value.message


def test_url_policy_accepts_public_https_with_query():
    target = subject.validate_remote_image_url(
        "https://images.example.com/path/chart.png?token=review-me"
    )
    assert target.host == "images.example.com"
    assert target.port == 443


@pytest.mark.parametrize(
    "address",
    [
        "0.0.0.0",
        "10.0.0.1",
        "100.64.0.1",
        "127.0.0.1",
        "169.254.169.254",
        "192.168.1.1",
        "::1",
        "fd00::1",
        "fe80::1",
        "::ffff:127.0.0.1",
        "2002:7f00:1::",
    ],
)
def test_public_address_policy_rejects_protected_ranges(address: str):
    assert subject.is_public_address(address) is False


def test_public_address_policy_accepts_globally_routable_addresses():
    assert subject.is_public_address("8.8.8.8") is True
    assert subject.is_public_address("2606:4700:4700::1111") is True


@pytest.mark.asyncio
async def test_resolver_pins_public_results_and_does_not_resolve_twice():
    calls: list[tuple[str, int]] = []

    async def lookup(host: str, port: int):
        calls.append((host, port))
        return [
            subject.ResolvedAddress("93.184.216.34", socket.AF_INET),
            subject.ResolvedAddress("93.184.216.34", socket.AF_INET),
        ]

    resolver = subject.PinnedPublicResolver(lookup)
    first = await resolver.pin("example.com", 443)
    second = await resolver.resolve("example.com", 443)

    assert calls == [("example.com", 443)]
    assert first == second
    assert first[0]["host"] == "93.184.216.34"
    assert first[0]["flags"] == socket.AI_NUMERICHOST


@pytest.mark.asyncio
async def test_resolver_rejects_a_hostname_with_any_private_answer():
    async def lookup(_host: str, _port: int):
        return [
            subject.ResolvedAddress("93.184.216.34", socket.AF_INET),
            subject.ResolvedAddress("10.0.0.9", socket.AF_INET),
        ]

    resolver = subject.PinnedPublicResolver(lookup)
    with pytest.raises(subject.RemoteImageError) as exc:
        await resolver.pin("split.example", 443)
    assert exc.value.code == "remote_image_destination_blocked"


def test_byte_validator_accepts_png_and_uses_detected_type():
    data = _png()
    image = subject.validate_remote_image_bytes(data)
    assert image.content == data
    assert image.media_type == "image/png"
    assert (image.width, image.height) == (2, 3)


def test_byte_validator_rejects_svg_and_html():
    for data in (
        b'<svg xmlns="http://www.w3.org/2000/svg"></svg>',
        b"<html><body>not an image</body></html>",
    ):
        with pytest.raises(subject.RemoteImageError) as exc:
            subject.validate_remote_image_bytes(data)
        assert exc.value.code == "remote_image_invalid"


def test_byte_validator_rejects_unsupported_decodable_format():
    output = io.BytesIO()
    Image.new("RGB", (2, 2), "blue").save(output, format="BMP")
    with pytest.raises(subject.RemoteImageError) as exc:
        subject.validate_remote_image_bytes(output.getvalue())
    assert exc.value.status_code == 415


def test_byte_validator_enforces_total_pixel_budget(monkeypatch):
    monkeypatch.setattr(subject, "MAX_REMOTE_IMAGE_PIXELS", 10)
    with pytest.raises(subject.RemoteImageError) as exc:
        subject.validate_remote_image_bytes(_png(4, 4))
    assert exc.value.code == "remote_image_too_large"


class _FakeContent:
    def __init__(self, chunks: list[bytes]):
        self._chunks = chunks

    async def iter_chunked(self, _size: int):
        for chunk in self._chunks:
            yield chunk


class _FakeResponse:
    def __init__(
        self,
        status: int,
        *,
        headers: dict[str, str] | None = None,
        chunks: list[bytes] | None = None,
    ):
        self.status = status
        self.headers = headers or {}
        self.content = _FakeContent(chunks or [])

    async def __aenter__(self):
        return self

    async def __aexit__(self, _exc_type, _exc, _tb):
        return False


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse]):
        self.responses = list(responses)
        self.calls: list[tuple[str, dict]] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, _exc_type, _exc, _tb):
        return False

    def get(self, url: str, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses.pop(0)


def _public_resolver() -> tuple[subject.PinnedPublicResolver, list[str]]:
    calls: list[str] = []

    async def lookup(host: str, _port: int):
        calls.append(host)
        return [subject.ResolvedAddress("93.184.216.34", socket.AF_INET)]

    return subject.PinnedPublicResolver(lookup), calls


@pytest.mark.asyncio
async def test_fetch_returns_verified_image_after_one_explicit_request():
    data = _png()
    session = _FakeSession(
        [
            _FakeResponse(
                200,
                headers={"Content-Type": "application/octet-stream"},
                chunks=[data[:10], data[10:]],
            )
        ]
    )
    resolver, dns_calls = _public_resolver()

    result = await subject.fetch_remote_image(
        "https://images.example/a.png?q=reviewed",
        resolver=resolver,
        session_factory=lambda _resolver: session,
    )

    assert result.media_type == "image/png"
    assert session.calls == [
        (
            "https://images.example/a.png?q=reviewed",
            {"allow_redirects": False},
        )
    ]
    assert dns_calls == ["images.example"]


@pytest.mark.asyncio
async def test_fetch_revalidates_redirect_before_second_request():
    session = _FakeSession(
        [
            _FakeResponse(
                302,
                headers={"Location": "https://127.0.0.1/private.png"},
            )
        ]
    )
    resolver, _dns_calls = _public_resolver()

    with pytest.raises(subject.RemoteImageError) as exc:
        await subject.fetch_remote_image(
            "https://images.example/a.png",
            resolver=resolver,
            session_factory=lambda _resolver: session,
        )

    assert exc.value.code == "remote_image_destination_blocked"
    assert len(session.calls) == 1


@pytest.mark.asyncio
async def test_fetch_rejects_oversized_declared_body_without_reading_it():
    session = _FakeSession(
        [
            _FakeResponse(
                200,
                headers={
                    "Content-Type": "image/png",
                    "Content-Length": str(subject.MAX_REMOTE_IMAGE_BYTES + 1),
                },
                chunks=[_png()],
            )
        ]
    )
    resolver, _dns_calls = _public_resolver()

    with pytest.raises(subject.RemoteImageError) as exc:
        await subject.fetch_remote_image(
            "https://images.example/a.png",
            resolver=resolver,
            session_factory=lambda _resolver: session,
        )
    assert exc.value.status_code == 413


@pytest.mark.asyncio
async def test_endpoint_authenticates_before_fetch(fake_request):
    import orchestrator.main
    from orchestrator.routers.media import load_remote_image
    from orchestrator.schemas.media import RemoteImageRequest

    denied = HTTPException(status_code=401, detail="signed out")
    fetch = AsyncMock()
    with (
        patch(
            "orchestrator.security.auth.require_approved_user",
            AsyncMock(side_effect=denied),
        ),
        patch("orchestrator.routers.media.fetch_remote_image", fetch),
    ):
        with pytest.raises(HTTPException) as exc:
            await load_remote_image(
                fake_request,
                RemoteImageRequest(url="https://images.example/a.png"),
                dependencies=projects_composition.media_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )
    assert exc.value.status_code == 401
    fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_endpoint_returns_no_store_nosniff_image(fake_request, user_a):
    import orchestrator.main
    from orchestrator.routers.media import load_remote_image
    from orchestrator.schemas.media import RemoteImageRequest

    image = subject.RemoteImage(
        content=_png(),
        media_type="image/png",
        width=2,
        height=3,
    )
    with (
        patch(
            "orchestrator.security.auth.require_approved_user",
            AsyncMock(return_value=user_a),
        ),
        patch(
            "orchestrator.routers.media.fetch_remote_image",
            AsyncMock(return_value=image),
        ),
    ):
        response = await load_remote_image(
            fake_request,
            RemoteImageRequest(url="https://images.example/a.png"),
            dependencies=projects_composition.media_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

    assert response.media_type == "image/png"
    assert response.headers["cache-control"] == "private, no-store"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["cross-origin-resource-policy"] == "same-origin"
    assert bytes(response.body) == image.content
