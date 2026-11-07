"""inject_officer_notice — the one-liner the Post lifecycle sends a live officer.

Hold, release, post-update and conference-start notices all go through it. It
called a raw-IP primitive the runtime-authority tranche removed on 2026-08-27;
the AttributeError was swallowed and every notice silently vanished. These
tests pin the replacement: the notice enters the durable input ledger for the
live runtime to claim, and nothing is written for an officer that is down.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from orchestrator.services import officer_notices, session_wake
from orchestrator.services.officer_notices import OfficerNoticeDependencies

THREAD_ID = "aa11bb22-0000-4000-8000-000000000002"


FULL_ROW = {"id": THREAD_ID, "metadata": {"agent_pod": {"runtime": "full"}}}


def _store(state: str | None = "persisted", thread_id: str = THREAD_ID):
    store = AsyncMock()
    store.get_thread = AsyncMock(return_value=FULL_ROW)
    store.persist_thread_input_delivery = AsyncMock(
        return_value={"thread_id": thread_id, "state": state}
    )
    return store


@pytest.mark.asyncio
async def test_live_officer_gets_a_durable_ledger_notice(monkeypatch) -> None:
    store = _store()
    monkeypatch.setattr(
        session_wake, "_resolve_live_agent", AsyncMock(return_value=object())
    )

    delivered = await officer_notices.inject_officer_notice(
        {"id": THREAD_ID},
        "[hold released — resume your duties.]",
        dependencies=OfficerNoticeDependencies(store=store),
    )

    assert delivered is True
    store.persist_thread_input_delivery.assert_awaited_once()
    kwargs = store.persist_thread_input_delivery.await_args.kwargs
    assert kwargs["thread_id"] == THREAD_ID
    assert kwargs["role"] == "event"
    assert kwargs["source"] == "officer_wake"
    assert kwargs["content"] == "[hold released — resume your duties.]"
    assert kwargs["delivery_id"]


@pytest.mark.asyncio
async def test_the_live_probe_reads_the_full_thread_row(monkeypatch) -> None:
    """The Post's thread projection has no runtime authority (k3d 2026-11-05):
    probing it always answered "not live", so no notice was ever sent."""
    store = _store()
    probe = AsyncMock(return_value=object())
    monkeypatch.setattr(session_wake, "_resolve_live_agent", probe)

    await officer_notices.inject_officer_notice(
        {"id": THREAD_ID, "status": "active"},
        "[maintenance hold]",
        dependencies=OfficerNoticeDependencies(store=store),
    )

    store.get_thread.assert_awaited_once_with(THREAD_ID)
    assert probe.await_args.args[1] is FULL_ROW


@pytest.mark.asyncio
async def test_down_officer_gets_nothing_written(monkeypatch) -> None:
    store = _store()
    monkeypatch.setattr(
        session_wake, "_resolve_live_agent", AsyncMock(return_value=None)
    )

    delivered = await officer_notices.inject_officer_notice(
        {"id": THREAD_ID},
        "[post updated]",
        dependencies=OfficerNoticeDependencies(store=store),
    )

    assert delivered is False
    store.persist_thread_input_delivery.assert_not_awaited()


@pytest.mark.asyncio
async def test_ledger_row_for_another_thread_is_not_a_delivery(monkeypatch) -> None:
    store = _store(thread_id="ffffffff-0000-4000-8000-000000000009")
    monkeypatch.setattr(
        session_wake, "_resolve_live_agent", AsyncMock(return_value=object())
    )

    delivered = await officer_notices.inject_officer_notice(
        {"id": THREAD_ID},
        "[post updated]",
        dependencies=OfficerNoticeDependencies(store=store),
    )

    assert delivered is False


@pytest.mark.asyncio
async def test_ledger_failure_never_raises(monkeypatch) -> None:
    store = _store()
    store.persist_thread_input_delivery.side_effect = RuntimeError("db down")
    monkeypatch.setattr(
        session_wake, "_resolve_live_agent", AsyncMock(return_value=object())
    )

    delivered = await officer_notices.inject_officer_notice(
        {"id": THREAD_ID},
        "[post updated]",
        dependencies=OfficerNoticeDependencies(store=store),
    )

    assert delivered is False
