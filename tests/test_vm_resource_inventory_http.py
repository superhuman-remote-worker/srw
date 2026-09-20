from types import SimpleNamespace
from unittest.mock import AsyncMock

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest
from starlette.requests import Request

from orchestrator.routers import vm_resource_inventory as route
from shared.vm_lifecycle_auth import AUTH_FIELD, sign_payload, verify_payload
from shared.vm_resource_inventory import snapshot_digest, InventoryError
from tests.test_vm_resource_inventory_contract import snapshot


SECRET = b"inventory-test-secret-with-at-least-32-bytes"
PATH = "/api/internal/vm-resource-inventory/publish"


def signed(value=None, **kwargs):
    value = value or snapshot()
    return sign_payload(
        {"snapshot": value, "digest": snapshot_digest(value)},
        direction="request",
        operation=kwargs.get("operation", route.OPERATION),
        secret=kwargs.get("secret", SECRET),
    )


def client(monkeypatch, *, enabled=True, max_bytes=100000):
    monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", SECRET.decode())
    store = SimpleNamespace(publish=AsyncMock())

    async def publish(*, snapshot, digest):
        return {
            "accepted": True,
            "snapshot_id": snapshot["snapshot_id"],
            "digest": digest,
            "current": True,
            "received_at": snapshot["finished_at"],
        }

    store.publish.side_effect = publish
    monkeypatch.setattr(route, "_configuration", None)
    if enabled:
        route.configure(store_factory=lambda: store, max_items=100, max_bytes=max_bytes)
    app = FastAPI()
    app.include_router(route.router)
    return TestClient(app), store


def test_valid_publication_returns_exact_correlated_signed_receipt(monkeypatch):
    http, store = client(monkeypatch)
    request = signed()
    response = http.post(PATH, json=request)
    assert response.status_code == 200
    assert verify_payload(
        response.json(),
        direction="response",
        operation=route.OPERATION,
        secret=SECRET,
        expected_correlation_id=request[AUTH_FIELD]["request_id"],
    )
    assert response.json()["complete"] is True
    assert response.json()["snapshot_id"] == request["snapshot"]["snapshot_id"]
    store.publish.assert_awaited_once()


@pytest.mark.parametrize(
    "change",
    [
        "mac",
        "operation",
        "unsigned",
        "auth-extra",
        "payload-extra",
        "digest",
        "correlation",
    ],
)
def test_untrusted_or_ambiguous_envelope_never_reaches_store(monkeypatch, change):
    http, store = client(monkeypatch)
    value = signed()
    if change == "mac":
        value[AUTH_FIELD]["signature"] = "f" * 64
    elif change == "operation":
        value = signed(operation="other")
    elif change == "unsigned":
        value.pop(AUTH_FIELD)
    elif change == "auth-extra":
        value[AUTH_FIELD]["private"] = "not-allowed"
    elif change == "payload-extra":
        value["private"] = "not-allowed"
    elif change == "correlation":
        value[AUTH_FIELD]["correlation_id"] = value[AUTH_FIELD]["request_id"]
    else:
        value["digest"] = "sha256:" + "b" * 64
        value = sign_payload(
            value, direction="request", operation=route.OPERATION, secret=SECRET
        )
    assert http.post(PATH, json=value).status_code in {400, 401}
    store.publish.assert_not_awaited()


@pytest.mark.parametrize(
    "body",
    [
        b'{"x":1,"x":2}',
        b'{"x":NaN}',
        b'{"x":Infinity}',
        b'{"x":1.1}',
        b'{"x":9223372036854775808}',
        b"[" * 40 + b"0" + b"]" * 40,
        b'{"x":"\xff"}',
    ],
)
def test_bounded_json_refuses_ambiguous_or_unbounded_values_before_mac(
    monkeypatch, body
):
    http, store = client(monkeypatch)
    monkeypatch.setattr(
        route, "verify_payload", lambda *a, **kw: pytest.fail("MAC called")
    )
    assert http.post(PATH, content=body).status_code == 400
    store.publish.assert_not_awaited()


def test_depth_counter_ignores_escaped_quotes_and_brackets_in_strings(monkeypatch):
    http, _ = client(monkeypatch)
    value = snapshot()
    value["cluster_id"] = 'test"' + "[" * 60
    assert http.post(PATH, json=signed(value)).status_code == 200


def test_off_and_compressed_requests_are_rejected_before_body_or_store(monkeypatch):
    http, store = client(monkeypatch, enabled=False)
    assert http.post(PATH, json=signed()).status_code == 503
    store.publish.assert_not_awaited()
    http, store = client(monkeypatch)
    assert (
        http.post(
            PATH, content=b"invalid", headers={"Content-Encoding": "gzip"}
        ).status_code
        == 415
    )
    store.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_chunked_body_limit_stops_before_parse_even_with_false_content_length(
    monkeypatch,
):
    _, store = client(monkeypatch, max_bytes=100)
    chunks = iter([b" " * 1500, b" " * 1500])
    calls = 0

    async def receive():
        nonlocal calls
        calls += 1
        if calls > 2:
            pytest.fail("read beyond byte cap")
        return {"type": "http.request", "body": next(chunks), "more_body": True}

    request = Request({"type": "http", "headers": [(b"content-length", b"1")]}, receive)
    monkeypatch.setattr(
        route, "verify_payload", lambda *a, **kw: pytest.fail("MAC called")
    )
    result = await route.publish(request)
    assert result.status_code == 413 and calls == 2
    store.publish.assert_not_awaited()


@pytest.mark.parametrize(
    "error,status",
    [
        (InventoryError("inventory_observation_old"), 409),
        (RuntimeError("private-credential"), 503),
    ],
)
def test_store_refusal_is_signed_bounded_and_does_not_expose_error(
    monkeypatch, error, status
):
    http, store = client(monkeypatch)
    store.publish.side_effect = error
    request = signed()
    response = http.post(PATH, json=request)
    assert response.status_code == status
    assert "private-credential" not in response.text
    assert verify_payload(
        response.json(),
        direction="response",
        operation=route.OPERATION,
        secret=SECRET,
        expected_correlation_id=request[AUTH_FIELD]["request_id"],
    )


def test_missing_hmac_never_uses_legacy_unsigned_mode(monkeypatch):
    http, store = client(monkeypatch)
    monkeypatch.delenv("VM_LIFECYCLE_HMAC_SECRET")
    assert http.post(PATH, json=signed()).status_code == 503
    store.publish.assert_not_awaited()


def test_disabled_bootstrap_does_not_construct_or_touch_store(monkeypatch):
    from orchestrator.services import vm_resource_inventory_store

    monkeypatch.delenv("VM_RESOURCE_ADMISSION_CONFIG", raising=False)
    monkeypatch.setattr(route, "_configuration", None)
    monkeypatch.setattr(
        vm_resource_inventory_store,
        "VMResourceInventoryStore",
        lambda *a, **kw: pytest.fail("constructed inventory store while off"),
    )
    route.configure_from_environment(object())
    assert route._configuration is None
