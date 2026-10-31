"""The stateless upgrade control's receipt (C1)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

import agent.api.persistent_app as pa
from agent.api import workspace_upgrade_seed as wus
from agent.api.lease_context import LeaseHandle, current_lease
from shared.thread_controls import ControlRequest, control_receipt_result

THREAD_ID = UUID("11111111-1111-4111-8111-111111111111")
REQUEST_ID = UUID("22222222-2222-4222-8222-222222222222")
CLIENT_REQUEST_ID = UUID(int=1)
GENERATION = "55555555-5555-4555-8555-555555555555"
LEASE = 9
PAYLOAD = {"runtime_generation": GENERATION, "target_tier": "sandbox"}


def _request(payload=None) -> ControlRequest:
    return ControlRequest(
        id=REQUEST_ID,
        thread_id=THREAD_ID,
        request_seq=1,
        client_request_id=CLIENT_REQUEST_ID,
        verb="upgrade-to-workspace",
        payload=PAYLOAD if payload is None else payload,
        accepted_agent_id=None,
        runtime_generation=UUID(GENERATION),
    )


def _envelope(**extra):
    return {
        "request_id": str(REQUEST_ID),
        "client_request_id": str(CLIENT_REQUEST_ID),
        "request_seq": 1,
        "method": "upgrade-to-workspace",
        **extra,
    }


class _Acquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *exc):
        return False


@pytest.fixture
def owner(monkeypatch):
    pool = SimpleNamespace(
        acquire=lambda: _Acquire(MagicMock()),
        workspace_upgrade_seed_pending=AsyncMock(return_value=False),
    )
    monkeypatch.setattr(pa, "_session", SimpleNamespace(postgres_conn=pool))
    monkeypatch.setattr(pa._session_identity, "_thread_id", str(THREAD_ID))
    handle = LeaseHandle()
    handle.update(str(THREAD_ID), LEASE)
    token = current_lease.set(handle)
    try:
        yield pool
    finally:
        current_lease.reset(token)


async def _drain(request):
    journal = AsyncMock(return_value=(6, 44))
    finalize = AsyncMock(side_effect=lambda _r, **kw: kw["outcome"])
    with (
        patch(
            "shared.thread_controls.owner_fence_current", AsyncMock(return_value=True)
        ),
        patch(
            "shared.thread_controls.adopt_next_pinned_control_request",
            AsyncMock(return_value=False),
        ),
        patch(
            "shared.thread_controls.fetch_next_control_request",
            AsyncMock(side_effect=[request, None]),
        ),
        patch(
            "shared.thread_controls.fetch_control_receipt",
            AsyncMock(return_value=None),
        ),
        patch.object(pa, "_broadcast_durable", journal),
        patch.object(pa, "_finalize_durable_control", finalize),
    ):
        await pa._drain_thread_controls(lease_token=LEASE)
    return journal, finalize


@pytest.mark.asyncio
async def test_a_seeded_upgrade_is_receipted_complete_with_its_file_count(owner):
    wus._SEED_RESULTS[(str(THREAD_ID), LEASE)] = 3

    journal, finalize = await _drain(_request())

    kind, params = journal.await_args.args
    assert kind == "workspace_upgrade.complete"
    assert params == _envelope(target_tier="sandbox", seeded_files=3)
    assert finalize.await_args.kwargs["outcome"] == "applied"


@pytest.mark.asyncio
async def test_an_upgrade_still_owing_its_copy_stays_pending(owner):
    owner.workspace_upgrade_seed_pending.return_value = True
    with pytest.raises(Exception, match="seed still pending"):
        await _drain(_request())


@pytest.mark.asyncio
async def test_a_vm_target_is_not_c1s(owner):
    journal, finalize = await _drain(
        _request({"runtime_generation": GENERATION, "target_tier": "vm"})
    )
    kind, params = journal.await_args.args
    assert kind == "control.rejected"
    assert params["error_code"] == "unsupported_control"


@pytest.mark.parametrize(
    "kind", ["workspace_upgrade.complete", "workspace_upgrade.started"]
)
def test_upgrade_receipts_are_applied(kind):
    assert control_receipt_result(
        request_id=REQUEST_ID,
        client_request_id=CLIENT_REQUEST_ID,
        request_seq=1,
        verb="upgrade-to-workspace",
        request_payload=PAYLOAD,
        event_kind=kind,
        event_payload=_envelope(target_tier="sandbox"),
    ) == ("applied", None, None)


def test_a_failed_upgrade_receipt_is_rejected_with_its_code():
    assert control_receipt_result(
        request_id=REQUEST_ID,
        client_request_id=CLIENT_REQUEST_ID,
        request_seq=1,
        verb="upgrade-to-workspace",
        request_payload={"runtime_generation": GENERATION, "target_tier": "vm"},
        event_kind="workspace_upgrade.failed",
        event_payload=_envelope(target_tier="vm", error_code="upgrade_cancelled"),
    ) == ("rejected", "upgrade_cancelled", None)


@pytest.mark.parametrize(
    "override",
    [{"request_id": str(UUID(int=9))}, {"method": "compact"}, {"target_tier": "gpu"}],
)
def test_upgrade_receipts_reject_mismatches(override):
    assert (
        control_receipt_result(
            request_id=REQUEST_ID,
            client_request_id=CLIENT_REQUEST_ID,
            request_seq=1,
            verb="upgrade-to-workspace",
            request_payload=PAYLOAD,
            event_kind="workspace_upgrade.complete",
            event_payload={**_envelope(target_tier="sandbox"), **override},
        )
        is None
    )
