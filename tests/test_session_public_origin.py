"""The browser socket authority is independent of Ingress host matching."""

import pytest

from orchestrator.services.session_urls import session_websocket_origin


@pytest.mark.parametrize("origin, expected", [
    ("https://192.0.2.10:30443", "wss://192.0.2.10:30443"),
    ("https://localhost:8443/", "wss://localhost:8443"),
    ("https://localhost:443", "wss://localhost:443"),
    ("", "wss://api.example.com"),
])
def test_session_websocket_origin_preserves_authority(origin, expected):
    assert session_websocket_origin(origin, "api.example.com") == expected


@pytest.mark.parametrize("origin", [
    "http://localhost:8443", "wss://localhost:8443", "https://",
    "https://localhost:99999", "https://localhost:invalid",
    "https://user:password@localhost:8443", "https://localhost:8443/api",
    "https://localhost:8443?redirect=x", "https://localhost:8443#fragment",
])
def test_session_public_origin_rejects_non_origin_values(origin):
    with pytest.raises(ValueError, match="SESSION_PUBLIC_ORIGIN"):
        session_websocket_origin(origin, "api.example.com")
