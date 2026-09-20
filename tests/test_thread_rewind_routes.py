from __future__ import annotations

from unittest.mock import AsyncMock
from uuid import UUID

from fastapi import FastAPI
from fastapi.testclient import TestClient

from orchestrator.routers import thread_rewind as routes
from orchestrator.schemas.thread_rewind import (
    RewindExpected,
    StatelessRewindPreview,
    StatelessRewindResult,
)
from orchestrator.services.thread_rewind import RewindFailure


THREAD_ID = "00000000-0000-4000-8000-000000000001"
MESSAGE_ID = UUID("00000000-0000-4000-8000-000000000002")
REQUEST_ID = UUID("00000000-0000-4000-8000-000000000003")


def _expected() -> RewindExpected:
    return RewindExpected(
        session_runtime_generation=UUID("00000000-0000-4000-8000-000000000004"),
        conversation_revision=0,
        events_epoch=7,
        transcript_tail_seq="9",
        input_seq="2",
        consumed_seq="2",
    )


def _result() -> StatelessRewindResult:
    return StatelessRewindResult(
        rewind_id=UUID("00000000-0000-4000-8000-000000000005"),
        client_request_id=REQUEST_ID,
        message_id=MESSAGE_ID,
        prompt="original prompt",
        swept_count=3,
        surviving_turn=1,
        conversation_revision=1,
        events_epoch=8,
        event_seq="1",
    )


def _client(service: AsyncMock) -> tuple[TestClient, AsyncMock]:
    require_owner = AsyncMock(
        return_value=({"id": "owner", "is_admin": False}, {"id": THREAD_ID})
    )
    app = FastAPI()
    app.state.thread_rewind_dependencies_factory = (
        lambda: routes.ThreadRewindDependencies(
            store=object(), service=service, require_thread_owner=require_owner
        )
    )
    app.include_router(routes.router)
    return TestClient(app), require_owner


def test_preview_post_and_receipt_are_owner_gated_and_no_store() -> None:
    service = AsyncMock()
    service.preview.return_value = StatelessRewindPreview(
        message_id=MESSAGE_ID,
        prompt="original prompt",
        eligible=True,
        refusal_code=None,
        swept_count=3,
        expected=_expected(),
    )
    service.apply.return_value = (_result(), False)
    service.receipt.return_value = _result()
    client, require_owner = _client(service)

    preview = client.get(
        f"/api/persistent/threads/{THREAD_ID}/rewinds/preview",
        params={"message_id": str(MESSAGE_ID)},
    )
    posted = client.post(
        f"/api/persistent/threads/{THREAD_ID}/rewinds",
        json={
            "client_request_id": str(REQUEST_ID),
            "message_id": str(MESSAGE_ID),
            "mode": "conversation",
            "expected": _expected().model_dump(mode="json"),
        },
    )
    receipt = client.get(
        f"/api/persistent/threads/{THREAD_ID}/rewinds/by-client-request/{REQUEST_ID}"
    )

    assert [preview.status_code, posted.status_code, receipt.status_code] == [
        200,
        200,
        200,
    ]
    assert posted.json()["duplicate"] is False
    for response in (preview, posted, receipt):
        assert response.headers["cache-control"] == "private, no-store"
    assert require_owner.await_count == 3


def test_structured_service_refusal_is_preserved() -> None:
    service = AsyncMock()
    service.apply.side_effect = RewindFailure(409, "rewind_busy", "pending_child")
    client, _require_owner = _client(service)

    response = client.post(
        f"/api/persistent/threads/{THREAD_ID}/rewinds",
        json={
            "client_request_id": str(REQUEST_ID),
            "message_id": str(MESSAGE_ID),
            "mode": "conversation",
            "expected": _expected().model_dump(mode="json"),
        },
    )

    assert response.status_code == 409
    assert response.json() == {
        "detail": {"code": "rewind_busy", "reason": "pending_child"}
    }
    assert response.headers["cache-control"] == "private, no-store"
