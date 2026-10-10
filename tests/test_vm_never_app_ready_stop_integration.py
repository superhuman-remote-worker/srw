"""Core dispatch of the committed never-app-Ready retained stop."""

from __future__ import annotations

from contextlib import asynccontextmanager
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import httpx
import pytest
import pytest_asyncio

from orchestrator.services import vm_provisioner as module
from orchestrator.services.vm_provisioner import (
    VMProvisioner,
    VMTeardownIdentity,
    VMTeardownResult,
    _VMTeardownProbe,
)
from shared.vm_cancel_retention import (
    never_app_ready_retention_authority_matches,
    retained_rootdisk_from_preflight,
)
from orchestrator.services.vm_lifecycle_auth import AUTH_FIELD, sign_payload
from orchestrator.services.vm_pre_ssh_stop_store import VMPreSSHStopStore
from orchestrator.services.vm_workspace_recovery_store import complete_vm_cleanup_permit
from tests.test_vm_never_app_ready_stop_real_postgres import (  # noqa: F401
    _base_db,
    _db_fixture,
    _initial_ready_db,
    _pre_ssh_db,
    _retention_db,
    _resume_db,
    _schema_applied,
    held_state,
    held_stop_schema,
    initial_ready_schema,
    pg_dsn,
    postgres_db_fixture,
    pre_ssh_schema,
    retention_schema,
    resume_schema,
    whole_schema,
)
from tests.test_vm_pre_ssh_stop_real_postgres import terminal_proof


@pytest_asyncio.fixture
async def db(held_stop_schema, _initial_ready_db):  # noqa: F811
    yield _initial_ready_db


@pytest.fixture(autouse=True)
def enabled(monkeypatch):
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_JOB_CANCEL_RETENTION_ENABLED", "true")


class FakeHeldController:
    def __init__(self, state):
        self.state = state
        self.actions = []
        self.stopped = False
        self.released = False
        self.lost_once = set()
        self.bad_once = set()
        self.wrong_once = set()

    async def request(self, payload):
        self.actions.append(payload)
        assert payload["action"] in {
            "inspect_never_app_ready_retained",
            "stop",
            "release",
        }
        assert payload["parent_cleanup"] == self.state["parent"]
        authority = payload["held_stop_authority"]
        assert authority["kind"] == "vm_job_cancel_retention_held_stop_authority_v1"
        if payload["action"] == "inspect_never_app_ready_retained":
            assert not self.stopped
            assert authority["job_id"] == self.state["job_id"]
            if "inspect" in self.lost_once:
                self.lost_once.remove("inspect")
                return None
            if "inspect" in self.wrong_once:
                self.wrong_once.remove("inspect")
                return {
                    "status": "candidate",
                    "frozen": {**self.state["frozen"], "vm_uid": str(uuid4())},
                    "retention_preflight": self.state["preflight"],
                    "_identity_authenticated": True,
                }
            return {
                "status": "candidate",
                "frozen": self.state["frozen"],
                "retention_preflight": self.state["preflight"],
                "_identity_authenticated": True,
            }
        assert never_app_ready_retention_authority_matches(
            authority, self.state["parent"], payload["frozen"]
        )
        if payload["action"] == "stop":
            self.stopped = True
            if "stop" in self.lost_once:
                self.lost_once.remove("stop")
                return None
            evidence = terminal_proof(payload["frozen"], payload["frozen_digest"])
            evidence["kind"] = "vm_job_never_app_ready_retained_positive_stop_v1"
            if "stop" in self.wrong_once:
                self.wrong_once.remove("stop")
                evidence["vm_uid"] = str(uuid4())
            if "stop" in self.bad_once:
                self.bad_once.remove("stop")
                return {
                    "status": "positive_terminal_proof",
                    "terminal_evidence": evidence,
                }
            return {
                "status": "positive_terminal_proof",
                "terminal_evidence": evidence,
                "_identity_authenticated": True,
            }
        assert self.stopped
        self.released = True
        if "release" in self.lost_once:
            self.lost_once.remove("release")
            return None
        return {"status": "finalizer_released", "_identity_authenticated": True}


def configured_provisioner(db, state, controller, monkeypatch):
    provisioner = VMProvisioner()
    provisioner._db = db
    provisioner._snapshot_service = None
    provisioner._request_pre_ssh_stop = controller.request

    async def probe(*_args):
        return _VMTeardownProbe(
            "absent" if controller.stopped else "present",
            VMTeardownIdentity(
                state["generation"],
                state["frozen"]["vm_uid"],
                state["frozen"]["pvc_uid"],
                credential_runtime_started=True,
            ),
            rootdisk_identity_known=True,
            runtime_absence_known=controller.stopped,
            vmi_absent=controller.stopped,
            launcher_absent=controller.stopped,
            retained_rootdisk=retained_rootdisk_from_preflight(state["preflight"]),
        )

    provisioner._probe_vm_teardown_identity = probe
    delete = AsyncMock(side_effect=AssertionError("generic deletion is forbidden"))
    retire = AsyncMock(side_effect=AssertionError("generic SSH is forbidden"))
    provisioner._delete_vm_with_identity = delete
    monkeypatch.setattr(module, "retire_managed_repository_processes", retire)
    return provisioner, retire, delete


@pytest.mark.asyncio
async def test_old_zero_and_kube_ready_stop_through_typed_intent_and_proof(
    db, monkeypatch
):
    state = await held_state(db, zero=True)
    old_zero = dict(state["zero"])
    controller = FakeHeldController(state)
    provisioner, retire, delete = configured_provisioner(
        db, state, controller, monkeypatch
    )
    identity = state["identity"]

    result = await provisioner.release_vm_captured(
        state["job_id"],
        identity,
        purge_disk=False,
        capture_snapshot=False,
        parent_cleanup=state["parent"],
    )
    assert result == VMTeardownResult("completed", True)
    assert [call["action"] for call in controller.actions] == [
        "inspect_never_app_ready_retained",
        "stop",
        "release",
    ]
    assert controller.released
    assert (
        await db.fetchval(
            "SELECT prior_zero_receipt_id FROM vm_pre_ssh_stop_intents WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == old_zero["id"]
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_pre_ssh_stop_proofs WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == 1
    )
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM managed_repository_process_zero_receipts WHERE id=$1",
                old_zero["id"],
            )
        )
        == old_zero
    )
    retire.assert_not_awaited()
    delete.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("lost", "first_actions", "replay_actions"),
    [
        (
            "inspect",
            ["inspect_never_app_ready_retained"],
            ["inspect_never_app_ready_retained", "stop", "release"],
        ),
        ("stop", ["inspect_never_app_ready_retained", "stop"], ["stop", "release"]),
        (
            "release",
            ["inspect_never_app_ready_retained", "stop", "release"],
            ["release"],
        ),
    ],
)
async def test_lost_reply_replays_durable_stop_and_old_zero(
    db, monkeypatch, lost, first_actions, replay_actions
):
    state = await held_state(db, zero=True)
    controller = FakeHeldController(state)
    controller.lost_once.add(lost)
    provisioner, retire, delete = configured_provisioner(
        db, state, controller, monkeypatch
    )
    kwargs = dict(
        purge_disk=False, capture_snapshot=False, parent_cleanup=state["parent"]
    )
    first = await provisioner.release_vm_captured(
        state["job_id"], state["identity"], **kwargs
    )
    assert first == VMTeardownResult("process_zero_unproven", False)
    assert [item["action"] for item in controller.actions] == first_actions
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(state["reservation_id"]),
        )
        == "teardown"
    )
    before = len(controller.actions)
    second = await provisioner.release_vm_captured(
        state["job_id"], state["identity"], **kwargs
    )
    assert second == VMTeardownResult("completed", True)
    assert [item["action"] for item in controller.actions[before:]] == replay_actions
    assert (
        await db.fetchval(
            "SELECT prior_zero_receipt_id FROM vm_pre_ssh_stop_intents WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == state["zero"]["id"]
    )
    retire.assert_not_awaited()
    delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_unauthenticated_stop_reply_holds_proof_and_capacity(db, monkeypatch):
    state = await held_state(db, zero=True)
    controller = FakeHeldController(state)
    controller.bad_once.add("stop")
    provisioner, retire, delete = configured_provisioner(
        db, state, controller, monkeypatch
    )
    result = await provisioner.release_vm_captured(
        state["job_id"],
        state["identity"],
        purge_disk=False,
        capture_snapshot=False,
        parent_cleanup=state["parent"],
    )
    assert result == VMTeardownResult("process_zero_unproven", False)
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_pre_ssh_stop_proofs WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == 0
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(state["reservation_id"]),
        )
        == "teardown"
    )
    assert not controller.released
    retire.assert_not_awaited()
    delete.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("wrong", ["inspect", "stop"])
async def test_foreign_candidate_or_proof_holds_without_capacity_release(
    db, monkeypatch, wrong
):
    state = await held_state(db, zero=True)
    controller = FakeHeldController(state)
    controller.wrong_once.add(wrong)
    provisioner, retire, delete = configured_provisioner(
        db, state, controller, monkeypatch
    )
    assert await provisioner.release_vm_captured(
        state["job_id"],
        state["identity"],
        purge_disk=False,
        capture_snapshot=False,
        parent_cleanup=state["parent"],
    ) == VMTeardownResult("process_zero_unproven", False)
    assert [item["action"] for item in controller.actions] == (
        ["inspect_never_app_ready_retained"]
        if wrong == "inspect"
        else ["inspect_never_app_ready_retained", "stop"]
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_pre_ssh_stop_proofs WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == 0
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(state["reservation_id"]),
        )
        == "teardown"
    )
    assert not controller.released
    retire.assert_not_awaited()
    delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_fresh_stop_writes_new_zero_only_after_typed_proof(db, monkeypatch):
    state = await held_state(db, zero=False)
    controller = FakeHeldController(state)
    provisioner, retire, delete = configured_provisioner(
        db, state, controller, monkeypatch
    )
    assert await provisioner.release_vm_captured(
        state["job_id"],
        state["identity"],
        purge_disk=False,
        capture_snapshot=False,
        parent_cleanup=state["parent"],
    ) == VMTeardownResult("completed", True)
    assert (
        await db.fetchval(
            "SELECT prior_zero_receipt_id FROM vm_pre_ssh_stop_intents WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        is None
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM managed_repository_process_zero_receipts "
            "WHERE owner_kind='job' AND owner_id=$1 AND scope='vm'",
            UUID(state["job_id"]),
        )
        == 1
    )
    retire.assert_not_awaited()
    delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_settled_replay_uses_released_charge_and_prior_proof(db, monkeypatch):
    from tests.test_vm_job_cancel_retention_real_postgres import acquire

    state = await held_state(db, zero=True)
    controller = FakeHeldController(state)
    provisioner, retire, delete = configured_provisioner(
        db, state, controller, monkeypatch
    )
    kwargs = dict(
        purge_disk=False, capture_snapshot=False, parent_cleanup=state["parent"]
    )
    assert await provisioner.release_vm_captured(
        state["job_id"], state["identity"], **kwargs
    ) == VMTeardownResult("completed", True)
    permit = await acquire(state)
    await complete_vm_cleanup_permit(
        state["recovery"], permit, outcome="completed", provisioner=provisioner
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(state["reservation_id"]),
        )
        == "released"
    )
    before = len(controller.actions)
    assert await provisioner.release_vm_captured(
        state["job_id"], state["identity"], **kwargs
    ) == VMTeardownResult("completed", True)
    assert len(controller.actions) == before
    retire.assert_not_awaited()
    delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_authority_revoked_after_intent_holds_before_stop(db, monkeypatch):
    state = await held_state(db, zero=True)
    controller = FakeHeldController(state)
    provisioner, retire, delete = configured_provisioner(
        db, state, controller, monkeypatch
    )
    original = VMPreSSHStopStore.current_intent
    reads = 0

    async def revoke_after_committed_intent(self, *args):
        nonlocal reads
        result = await original(self, *args)
        reads += 1
        if reads == 2:
            await db.execute(
                "UPDATE run_queue SET state='queued' WHERE unit_id=$1",
                UUID(state["job_id"]),
            )
        return result

    monkeypatch.setattr(
        VMPreSSHStopStore, "current_intent", revoke_after_committed_intent
    )
    assert await provisioner.release_vm_captured(
        state["job_id"],
        state["identity"],
        purge_disk=False,
        capture_snapshot=False,
        parent_cleanup=state["parent"],
    ) == VMTeardownResult("process_zero_unproven", False)
    assert [item["action"] for item in controller.actions] == [
        "inspect_never_app_ready_retained"
    ]
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_pre_ssh_stop_proofs WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == 0
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(state["reservation_id"]),
        )
        == "teardown"
    )
    retire.assert_not_awaited()
    delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_committed_proof_survives_lost_store_reply_without_new_inspect(
    db, monkeypatch
):
    state = await held_state(db, zero=True)
    controller = FakeHeldController(state)
    provisioner, retire, delete = configured_provisioner(
        db, state, controller, monkeypatch
    )
    original = VMPreSSHStopStore.commit_positive_proof
    once = True

    async def lost_commit_reply(self, *args):
        nonlocal once
        result = await original(self, *args)
        if once:
            once = False
            raise TimeoutError("lost database acknowledgement")
        return result

    monkeypatch.setattr(VMPreSSHStopStore, "commit_positive_proof", lost_commit_reply)
    kwargs = dict(
        purge_disk=False, capture_snapshot=False, parent_cleanup=state["parent"]
    )
    assert await provisioner.release_vm_captured(
        state["job_id"], state["identity"], **kwargs
    ) == VMTeardownResult("process_zero_unproven", False)
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_pre_ssh_stop_proofs WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == 1
    )
    before = len(controller.actions)
    assert await provisioner.release_vm_captured(
        state["job_id"], state["identity"], **kwargs
    ) == VMTeardownResult("completed", True)
    assert [item["action"] for item in controller.actions[before:]] == ["release"]
    retire.assert_not_awaited()
    delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_open_held_parent_without_controller_reply_never_spends_old_zero(
    db, monkeypatch
):
    state = await held_state(db, zero=True)
    controller = FakeHeldController(state)
    provisioner, retire, delete = configured_provisioner(
        db, state, controller, monkeypatch
    )
    provisioner._request_pre_ssh_stop = AsyncMock(return_value=None)
    result = await provisioner.release_vm_captured(
        state["job_id"],
        state["identity"],
        purge_disk=False,
        capture_snapshot=False,
        parent_cleanup=state["parent"],
    )
    assert result == VMTeardownResult("process_zero_unproven", False)
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_pre_ssh_stop_intents WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == 0
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(state["reservation_id"]),
        )
        == "teardown"
    )
    retire.assert_not_awaited()
    delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_outer_transaction_cannot_dispatch_even_an_inspect(db, monkeypatch):
    state = await held_state(db, zero=True)
    controller = FakeHeldController(state)
    provisioner, _, _ = configured_provisioner(db, state, controller, monkeypatch)
    async with db.acquire() as conn, conn.transaction():

        @asynccontextmanager
        async def acquire():
            yield conn

        provisioner._db = SimpleNamespace(acquire=acquire)
        assert not await provisioner._attempt_never_app_ready_retained_stop(
            state["job_id"], state["identity"], state["parent"]
        )
        assert controller.actions == []


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [None, "signature", "correlation"])
async def test_new_inspect_accepts_only_valid_signed_http_reply(monkeypatch, fault):
    monkeypatch.setenv("VM_MODE", "same-cluster")
    provisioner = VMProvisioner()
    provisioner._controller_url = "http://controller.test"
    provisioner._lifecycle_hmac_secret = b"held-stop-test-secret-at-least-32-bytes"
    generation = "aaaaaaaa-1111-4222-8333-bbbbbbbbbbbb"
    payload = {
        "action": "inspect_never_app_ready_retained",
        "job_id": generation,
        "provision_generation": generation,
    }

    def handler(request):
        signed = json.loads(request.content)
        reply = sign_payload(
            {
                "status": "candidate",
                "job_id": generation,
                "provision_generation": generation,
            },
            direction="response",
            operation="pre-ssh-stop",
            secret=(
                b"wrong-secret-at-least-32-bytes-long"
                if fault == "signature"
                else provisioner._lifecycle_hmac_secret
            ),
            correlation_id=(
                generation
                if fault == "correlation"
                else signed[AUTH_FIELD]["request_id"]
            ),
        )
        return httpx.Response(200, json=reply)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url=provisioner._controller_url
    ) as client:
        provisioner._http_client = client
        observed = await provisioner._request_pre_ssh_stop(payload)
        if fault is None:
            assert observed == {
                "status": "candidate",
                "job_id": generation,
                "provision_generation": generation,
                "_identity_authenticated": True,
            }
        else:
            assert observed is None
