"""A bound thread remains reserved even before construction starts."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from agent.api.session_attach import SessionAttachCoordinator
from tests.test_session_attach_runtime import THREAD, _identity, _ports


@pytest.mark.parametrize("bound", [True, False])
def test_only_threadless_idle_runtime_can_advertise_ready(bound):
    identity, _ = _identity()
    if bound:
        identity.bind_thread(THREAD)
    owner = SessionAttachCoordinator(_ports(identity))
    assert owner.pool_heartbeat_status() == ("session" if bound else "ready")


@pytest.mark.asyncio
@pytest.mark.parametrize("bound", [True, False])
async def test_pool_admission_preserves_a_thread_reservation_without_a_session(bound):
    identity, _ = _identity()
    if bound:
        identity.bind_thread(THREAD)
    owner = SessionAttachCoordinator(_ports(identity))
    owner.attach = AsyncMock()
    try:
        response = await owner.admit_pool_attach(THREAD, {})
        assert response.status_code == (409 if bound else 200)
        if bound:
            assert owner.pool_claim == (None, None, None)
            assert owner.pool_task is None
            owner.attach.assert_not_awaited()
        else:
            await owner.pool_task
            owner.attach.assert_awaited_once()
    finally:
        if owner.pool_task is not None:
            await asyncio.gather(owner.pool_task, return_exceptions=True)
