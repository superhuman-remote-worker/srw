"""The programmatic resume client surfaces drift instead of a bare HTTP error.

``AsyncCockpitClient.resume_persistent_thread`` must turn a 428 config-drift
response into a ``SessionConfigDriftError`` that names the drifted items, must
transmit an ``acknowledge`` list when the caller supplies one, and must leave
ordinary 200 and non-428-error handling unchanged. See
knowledge-history/done/session_config_drift_resume.md §4.6.

``str(error)`` must carry the raw ids (not just labels) and a literal,
copy-pasteable ``acknowledge=[...]`` list, because the MCP tool path renders
only ``str(error)`` (via ``_format_action_error`` -> plain ``str()``) and has
no other way to hand the ids back to the caller. A malformed 428 body (a
non-list ``drift``, a missing ``detail`` key, or a non-JSON body) must
degrade to an empty ``.drift``/``.ids`` instead of raising AttributeError or
TypeError while building the message.

This repo has no ``httpx_mock``/pytest-httpx fixture; existing orch_surface
client tests (tests/test_mcp_client_contracts.py, tests/test_mcp_client_safety.py)
stub the transport directly with ``httpx.MockTransport``, so these tests follow
that pattern instead of adding a new test dependency.
"""

from __future__ import annotations

import json

import httpx
import pytest

from shared.orch_surface.client import AsyncCockpitClient, SessionConfigDriftError

RESUME_URL = "http://orchestrator.test/api/persistent/threads/t1/resume"


def _client(handler) -> AsyncCockpitClient:
    return AsyncCockpitClient(
        base_url="http://orchestrator.test",
        transport=httpx.MockTransport(handler),
    )


@pytest.mark.asyncio
async def test_428_raises_a_drift_error_naming_the_items() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == RESUME_URL
        return httpx.Response(
            428,
            json={
                "detail": {
                    "code": "config_drift",
                    "detail": (
                        "Parts of this session's configuration are no longer available"
                    ),
                    "drift": [
                        {
                            "id": "connector:abc",
                            "kind": "connector",
                            "reason": "deleted",
                            "label": "WidgetShop",
                        }
                    ],
                    "summary": ["WidgetShop"],
                }
            },
        )

    client = _client(handler)
    try:
        with pytest.raises(SessionConfigDriftError) as excinfo:
            await client.resume_persistent_thread("t1")
    finally:
        await client.close()

    assert "WidgetShop" in str(excinfo.value)
    assert excinfo.value.drift == [
        {
            "id": "connector:abc",
            "kind": "connector",
            "reason": "deleted",
            "label": "WidgetShop",
        }
    ]


@pytest.mark.asyncio
async def test_error_message_carries_ids_and_a_literal_acknowledge_list() -> None:
    """The MCP tool path renders only str(error) -- it must be self-sufficient.

    A caller reading only the message text (no .drift access) still needs to
    learn the raw id, not just the human label, to build its next call.
    """

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            428,
            json={
                "detail": {
                    "code": "config_drift",
                    "drift": [
                        {
                            "id": "connector:abc",
                            "kind": "connector",
                            "reason": "deleted",
                            "label": "WidgetShop",
                        }
                    ],
                }
            },
        )

    client = _client(handler)
    try:
        with pytest.raises(SessionConfigDriftError) as excinfo:
            await client.resume_persistent_thread("t1")
    finally:
        await client.close()

    message = str(excinfo.value)
    assert "connector:abc" in message
    assert "acknowledge=['connector:abc']" in message
    assert excinfo.value.ids == ["connector:abc"]


@pytest.mark.asyncio
async def test_428_with_non_list_drift_degrades_without_crashing() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            428,
            json={"detail": {"code": "config_drift", "drift": "not-a-list"}},
        )

    client = _client(handler)
    try:
        with pytest.raises(SessionConfigDriftError) as excinfo:
            await client.resume_persistent_thread("t1")
    finally:
        await client.close()

    assert excinfo.value.drift == []
    assert excinfo.value.ids == []


@pytest.mark.asyncio
async def test_428_with_no_detail_key_degrades_without_crashing() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(428, json={"code": "config_drift"})

    client = _client(handler)
    try:
        with pytest.raises(SessionConfigDriftError) as excinfo:
            await client.resume_persistent_thread("t1")
    finally:
        await client.close()

    assert excinfo.value.drift == []
    assert excinfo.value.ids == []


@pytest.mark.asyncio
async def test_428_with_non_json_body_degrades_without_crashing() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(428, content=b"not json at all")

    client = _client(handler)
    try:
        with pytest.raises(SessionConfigDriftError) as excinfo:
            await client.resume_persistent_thread("t1")
    finally:
        await client.close()

    assert excinfo.value.drift == []
    assert excinfo.value.ids == []


@pytest.mark.asyncio
async def test_acknowledge_is_sent_in_the_body() -> None:
    captured: list[dict] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        captured.append(json.loads(request.content))
        return httpx.Response(200, json={"status": "created", "thread_id": "t1"})

    client = _client(handler)
    try:
        await client.resume_persistent_thread("t1", acknowledge=["connector:abc"])
    finally:
        await client.close()

    assert captured == [{"acknowledge": ["connector:abc"]}]


@pytest.mark.asyncio
async def test_200_without_acknowledge_returns_parsed_json_as_before() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "created", "thread_id": "t1"})

    client = _client(handler)
    try:
        result = await client.resume_persistent_thread("t1")
    finally:
        await client.close()

    assert result == {"status": "created", "thread_id": "t1"}


@pytest.mark.asyncio
async def test_non_428_error_still_raises_via_raise_for_status() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"detail": "boom"})

    client = _client(handler)
    try:
        with pytest.raises(httpx.HTTPStatusError):
            await client.resume_persistent_thread("t1")
    finally:
        await client.close()
