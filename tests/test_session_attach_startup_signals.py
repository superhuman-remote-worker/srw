"""Startup termination belongs to attach cancellation and retains cleanup proof."""

import asyncio

import pytest

from agent.api import persistent_app as pa


@pytest.mark.asyncio
async def test_shutdown_confirms_retained_startup_abort_before_client_close(
    monkeypatch,
):
    from unittest.mock import AsyncMock

    receipt = {
        "thread_id": "captured",
        "session_runtime_generation": "g",
        "session_runtime_attach_token": "a",
    }
    monkeypatch.setattr(pa._session_attach, "_release_receipt", receipt)
    monkeypatch.setattr(pa._session_attach, "_startup_task", None)
    release = AsyncMock(return_value=True)
    monkeypatch.setattr(pa._session_attach, "release_receipt_until_confirmed", release)
    assert await pa._session_attach.release_shutdown_receipt(timeout=0.1) is True
    release.assert_awaited_once_with(
        "captured", runtime_generation="g", runtime_attach_token="a"
    )


@pytest.mark.asyncio
async def test_shutdown_release_timeout_keeps_captured_identity_and_obligation(
    monkeypatch,
):
    receipt = {
        "thread_id": "captured",
        "session_runtime_generation": "g",
        "session_runtime_attach_token": "a",
    }
    monkeypatch.setattr(pa._session_attach, "_release_receipt", receipt)
    monkeypatch.setattr(pa._session_attach, "_startup_task", None)

    async def release(*args, **kwargs):
        await asyncio.Future()

    monkeypatch.setattr(pa._session_attach, "release_receipt_until_confirmed", release)
    assert await pa._session_attach.release_shutdown_receipt(timeout=0.01) is False
    assert pa._session_attach.release_receipt is receipt
