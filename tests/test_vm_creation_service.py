"""Only durable claims feed the creation transport, including feature-off drains."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from orchestrator.services.vm_creation_transport import CreationConfigurationUnavailable
from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict


def service(monkeypatch):
    from orchestrator.services import vm_creation_retry as module

    instance = module.VMCreationRetryService(
        object(),
        SimpleNamespace(_http_client=object(), _lifecycle_hmac_secret=b"secret"),
    )
    instance.preflight = SimpleNamespace(
        settle_cancelled=AsyncMock(return_value=0),
        claim_due=AsyncMock(return_value=[]),
        complete_resolution=AsyncMock(),
        record_failure=AsyncMock(),
    )
    instance.store = SimpleNamespace(
        claim_due=AsyncMock(return_value=[]),
        settle_never_issued=AsyncMock(return_value={"settled": False}),
        apply_observation=AsyncMock(),
    )
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "false")
    monkeypatch.setattr(module, "resolve_vm_creation_configuration", AsyncMock())
    monkeypatch.setattr(
        module,
        "replay_vm_creation",
        AsyncMock(return_value={"outcome": "capacity_wait", "reason": "capacity_wait"}),
    )
    return instance, module


def claim(state="reconciling"):
    return dict(
        request_id=uuid4(), job_id=uuid4(), claim_token=uuid4(), revision=3, state=state
    )


@pytest.mark.asyncio
async def test_service_resolves_frozen_preflight_then_admits_before_replay_even_flag_off(
    monkeypatch,
):
    instance, module = service(monkeypatch)
    preflight = {"request": {"frozen": "original"}}
    instance.preflight.claim_due.return_value = [preflight]
    row = claim()
    sequence = []
    instance.preflight.complete_resolution.side_effect = lambda *args: sequence.append(
        "committed"
    )

    async def due(**kwargs):
        assert sequence == ["committed"]
        return [row]

    instance.store.claim_due.side_effect = due
    await instance.reconcile_once()
    module.resolve_vm_creation_configuration.assert_awaited_once_with(
        instance.provisioner._http_client, preflight["request"], secret=b"secret"
    )
    module.replay_vm_creation.assert_awaited_once_with(
        instance.provisioner._http_client, row, secret=b"secret"
    )
    instance.store.apply_observation.assert_awaited_once_with(
        request_id=str(row["request_id"]),
        claim_token=str(row["claim_token"]),
        expected_revision=3,
        observation={"outcome": "capacity_wait", "reason": "capacity_wait"},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["outage", "cancelled", "expired", "database"])
async def test_failed_resolution_handoff_never_sends_uncommitted_create(
    monkeypatch, failure
):
    instance, module = service(monkeypatch)
    pending = {"request": {}}
    instance.preflight.claim_due.return_value = [pending]
    if failure == "outage":
        module.resolve_vm_creation_configuration.side_effect = (
            CreationConfigurationUnavailable("controller_unavailable")
        )
    else:
        instance.preflight.complete_resolution.side_effect = (
            VMCreationRetryConflict(
                "job_cancelled" if failure == "cancelled" else "job_admission_expired"
            )
            if failure != "database"
            else RuntimeError("private database detail")
        )
    await instance.reconcile_once()
    module.replay_vm_creation.assert_not_awaited()
    if failure == "outage":
        instance.preflight.record_failure.assert_awaited_once_with(
            pending, reason="controller_unavailable"
        )
    else:
        instance.preflight.record_failure.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("settled", [False, True])
async def test_cancellation_only_settles_database_proven_nonissuance_or_waits_for_observer(
    monkeypatch, settled
):
    instance, module = service(monkeypatch)
    row = claim("cancel_requested")
    instance.store.claim_due.return_value = [row]
    instance.store.settle_never_issued.return_value = {"settled": settled}
    await instance.reconcile_once()
    module.replay_vm_creation.assert_not_awaited()
    instance.store.settle_never_issued.assert_awaited_once_with(
        request_id=str(row["request_id"])
    )
    if settled:
        instance.store.apply_observation.assert_not_awaited()
    else:
        assert (
            instance.store.apply_observation.call_args.kwargs["observation"]["outcome"]
            == "observation_wait"
        )


@pytest.mark.asyncio
async def test_adopted_response_cannot_merge_context_or_release_worker_hold(
    monkeypatch,
):
    instance, module = service(monkeypatch)
    row = claim()
    instance.store.claim_due.return_value = [row]
    module.replay_vm_creation.return_value = {"outcome": "adopted"}
    await instance.reconcile_once()
    assert (
        instance.store.apply_observation.call_args.kwargs["observation"]["outcome"]
        == "observation_wait"
    )
