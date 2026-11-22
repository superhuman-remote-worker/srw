"""A VM Session's wake is not held back by the access waiting for it.

An Open browser, IDE or SSH request on a sleeping VM Session records an access
lease bound to the wake and to the stopped VM. Creating the wake's successor
must not refuse for that lease (it waits for the successor and cannot use the
stopped VM); a pinned Job's wake has the same exemption.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest

from orchestrator.services import vm_creation_retry_store as retry_store
from orchestrator.services.vm_creation_retry_store import (
    VMCreationRetryStore,
    _creation_intent,
)
from orchestrator.services.vm_workspace_recovery_store import cleanup_intent_digest

NOW = datetime(2026, 11, 22, 12, 0, tzinfo=timezone.utc)


def _digest(value) -> str:
    return "sha256:" + repr(sorted(value.items()))


@pytest.fixture(autouse=True)
def _plain_digests(monkeypatch):
    # The digests' own rules have their own tests; here they only bind the
    # frozen request and configuration to the retry row.
    monkeypatch.setattr(retry_store, "canonical_request_digest", _digest)
    monkeypatch.setattr(retry_store, "canonical_configuration_digest", _digest)


THREAD = UUID("11111111-1111-4111-8111-111111111111")
OPERATION = UUID("22222222-2222-4222-8222-222222222222")
WAKE = UUID("33333333-3333-4333-8333-333333333333")
OLD_GENERATION = UUID("44444444-4444-4444-8444-444444444444")
NEW_GENERATION = UUID("55555555-5555-4555-8555-555555555555")
PVC = UUID("66666666-6666-4666-8666-666666666666")
OLD_VM = UUID("77777777-7777-4777-8777-777777777777")


def _retry(**overrides):
    request = {"job_id": str(THREAD), "memory": "4Gi", "cpu_cores": 2}
    configuration = {"version": 1, "namespace": "srw"}
    row = {
        "request_id": uuid4(),
        "owner_kind": "thread",
        "thread_id": THREAD,
        "job_id": None,
        "state": "reconciling",
        "claim_token": uuid4(),
        "claim_expires_at": NOW + timedelta(seconds=30),
        "expected_pvc_uid": PVC,
        "provision_generation": NEW_GENERATION,
        "canonical_request": request,
        "request_digest": _digest(request),
        "controller_configuration": configuration,
        "controller_configuration_digest": _digest(configuration),
        "thread_runtime_generation": uuid4(),
        "thread_agent_id": None,
        "thread_attach_token": None,
        "thread_wake_operation_id": OPERATION,
    }
    row.update(overrides)
    return row


def _operation(**overrides):
    row = {
        "id": OPERATION,
        "owner_kind": "thread",
        "owner_id": THREAD,
        "release_kind": "pinned_thread",
        "closed_at": None,
        "phase": "waking",
        "wake_id": WAKE,
        "wake_generation": NEW_GENERATION,
        "wake_ready_at": None,
        "pvc_uid": PVC,
        "retained_kind": "rootdisk",
        "stop_verified_at": NOW - timedelta(minutes=1),
        "provision_generation": OLD_GENERATION,
        "vm_uid": OLD_VM,
        "vmi_uid": uuid4(),
        "launcher_uid": uuid4(),
    }
    row.update(overrides)
    row.setdefault(
        "stop_evidence",
        {
            "version": 1,
            "kind": "vm_idle_physical_stop",
            "operation_id": str(row["id"]),
            "generation": str(row["provision_generation"]),
            "vm_uid": str(row["vm_uid"]),
            "vmi_uid": str(row["vmi_uid"]),
            "launcher_uid": str(row["launcher_uid"]),
            "pvc_uid": str(row["pvc_uid"]),
            "vm_absent": True,
            "vmi_absent": True,
            "launcher_absent": True,
            "retained_pvc": True,
            "controller_authenticated": True,
            "same_generation_replacement": False,
        },
    )
    return row


class _Conn:
    def __init__(self, retry, operation, *, vm_generation=NEW_GENERATION):
        self.retry = retry
        self.operation = operation
        self.vm_generation = vm_generation

    async def fetch(self, query, *args):
        assert "vm_creation_retries" in query and args == (THREAD,)
        return [self.retry] if self.retry is not None else []

    async def fetchval(self, query, *args):
        return NOW

    async def fetchrow(self, query, *args):
        if "vm_idle_operations" in query:
            return self.operation
        return {"metadata": {"vm": {"provision_generation": str(self.vm_generation)}}}


async def _scope(conn, retry, *, request_id=None, intent=None):
    request_id = request_id or retry["request_id"]
    return await VMCreationRetryStore(object()).thread_idle_wake_access_scope_on_conn(
        conn,
        owner_id=THREAD,
        pvc_uid=PVC,
        reservation_request_id=uuid5(NAMESPACE_URL, "vm-create:" + str(request_id)),
        intent_digest=intent or cleanup_intent_digest(_creation_intent(retry)),
    )


@pytest.mark.asyncio
async def test_the_wake_successor_may_ignore_leases_bound_to_its_wake():
    retry = _retry()

    scope = await _scope(_Conn(retry, _operation()), retry)

    assert scope == {
        "wake_id": WAKE,
        "provision_generation": OLD_GENERATION,
        "vm_uid": OLD_VM,
    }


@pytest.mark.asyncio
async def test_another_creation_gets_no_exemption():
    retry = _retry()

    assert await _scope(_Conn(retry, _operation()), retry, request_id=uuid4()) is None
    assert await _scope(_Conn(retry, _operation()), retry, intent="sha256:x") is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("retry_changes", "operation_changes"),
    [
        ({"claim_expires_at": NOW - timedelta(seconds=1)}, {}),
        ({"claim_token": None}, {}),
        ({"expected_pvc_uid": uuid4()}, {}),
        ({"request_digest": "sha256:changed"}, {}),
        ({}, {"wake_ready_at": NOW}),
        ({}, {"closed_at": NOW}),
        ({}, {"phase": "releasing"}),
        ({}, {"release_kind": "pinned_job"}),
        ({}, {"wake_generation": uuid4()}),
        ({}, {"stop_verified_at": None}),
        ({}, {"stop_evidence": {}}),
        ({}, {"retained_kind": "snapshot"}),
    ],
)
async def test_anything_short_of_the_exact_stopped_wake_gets_no_exemption(
    retry_changes, operation_changes
):
    retry = _retry(**retry_changes)

    assert await _scope(_Conn(retry, _operation(**operation_changes)), retry) is None


@pytest.mark.asyncio
async def test_a_vm_record_for_another_generation_gets_no_exemption():
    retry = _retry()

    conn = _Conn(retry, _operation(), vm_generation=uuid4())

    assert await _scope(conn, retry) is None
