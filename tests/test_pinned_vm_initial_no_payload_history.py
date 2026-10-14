"""An unissued pre-delivery attach must not consume a Session's first VM create."""

from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime, timezone
import json
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from orchestrator.database.postgres import PostgresDB
from shared.vm_creation_issuance import canonical_configuration_digest
from shared.vm_creation_retry import canonical_request_digest
from tests.test_vm_resource_configuration import V1_JSON


THREAD = UUID("11111111-1111-4111-8111-111111111111")
OLD_GENERATION = UUID("22222222-2222-4222-8222-222222222222")
CURRENT_GENERATION = UUID("33333333-3333-4333-8333-333333333333")
MIDDLE_GENERATION = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
OLD_AGENT = UUID("44444444-4444-4444-8444-444444444444")
CURRENT_AGENT = UUID("55555555-5555-4555-8555-555555555555")
MIDDLE_AGENT = UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")
OLD_ATTACH = UUID("66666666-6666-4666-8666-666666666666")
CURRENT_ATTACH = UUID("77777777-7777-4777-8777-777777777777")
MIDDLE_ATTACH = UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd")
EXECUTION = UUID("88888888-8888-4888-8888-888888888888")
PROVISION = UUID("99999999-9999-4999-8999-999999999999")
REQUEST = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")


def no_payload_outcome(**changes):
    row = {
        "thread_id": THREAD,
        "runtime_generation": OLD_GENERATION,
        "runtime_attach_token": OLD_ATTACH,
        "agent_id": OLD_AGENT,
        "agent_pod_uid": "old-agent-pod",
        "successor_generation": CURRENT_GENERATION,
        "release_kind": "server_pre_delivery",
        "quiescence_protocol": "pre_delivery_no_payload_v1",
        "workspace_generation": None,
        "workspace_runtime_incarnation": None,
        "released_at": datetime(2026, 10, 13, tzinfo=timezone.utc),
    }
    row.update(changes)
    return row


class LockedInitialCreate:
    """Only the SQL seams used by the real initial-create admission method."""

    def __init__(
        self,
        outcomes,
        *,
        other_history=(),
        receipt=True,
        terminal_outcome=None,
        receipt_changes=None,
    ):
        self.outcomes = outcomes
        self.other_history = set(other_history)
        self.inserted_source = None
        self.metadata_after = None
        terminal = terminal_outcome or (outcomes[-1] if outcomes else None)
        abort_receipt = (
            {
                "version": 1,
                **{
                    key: str(terminal[key])
                    for key in (
                        "runtime_generation",
                        "successor_generation",
                        "agent_id",
                        "runtime_attach_token",
                    )
                },
                "agent_pod_uid": terminal["agent_pod_uid"],
                "release_kind": terminal["release_kind"],
                "quiescence_protocol": terminal["quiescence_protocol"],
                "workspace_generation": (
                    str(terminal["workspace_generation"])
                    if terminal["workspace_generation"] is not None
                    else None
                ),
                "workspace_runtime_incarnation": (
                    str(terminal["workspace_runtime_incarnation"])
                    if terminal["workspace_runtime_incarnation"] is not None
                    else None
                ),
            }
            if receipt and terminal
            else None
        )
        if abort_receipt is not None:
            abort_receipt.update(receipt_changes or {})
        self.thread = {
            "id": THREAD,
            "status": "created",
            "execution_lane": "pinned",
            "created_at": datetime(2026, 10, 12, tzinfo=timezone.utc),
            "runtime_generation": CURRENT_GENERATION,
            "runtime_retirement_token": None,
            "runtime_authority_exposed": True,
            "agent_id": CURRENT_AGENT,
            "runtime_attach_token": CURRENT_ATTACH,
            "pinned_idle_terminal_intent_at": None,
            "runtime_attach_abort_receipt": abort_receipt,
            "metadata": {"config_override": {"workspace": {"backend": "vm"}}},
        }
        self.execution = {
            "id": EXECUTION,
            "revision": "frozen-revision",
            "generation": 1,
            "harness_adapter": "srw/v1",
        }

    @asynccontextmanager
    async def transaction(self):
        yield

    async def fetchrow(self, query, *args):
        if "FROM threads WHERE id=" in query:
            return deepcopy(self.thread)
        if "FROM srw_execution_specs" in query:
            return deepcopy(self.execution)
        if "FROM vm_idle_operations" in query:
            return None
        if "INSERT INTO vm_creation_retries" in query:
            self.inserted_source = args
            return {"request_id": REQUEST}
        raise AssertionError(f"Unexpected fetchrow: {query}")

    async def fetchval(self, query, *args):
        history_tables = (
            "vm_creation_retries",
            "vm_idle_operations",
            "thread_runtime_retirement_outcomes",
            "thread_runtime_attach_abort_outcomes",
            "thread_workspace_provision_intents",
            "srw_execution_workspace_bindings",
        )
        if any(table in query for table in history_tables):
            return any(table in query for table in self.other_history) or (
                bool(self.outcomes) and "thread_runtime_attach_abort_outcomes" in query
            )
        raise AssertionError(f"Unexpected fetchval: {query}")

    async def fetch(self, query, *args):
        if "FROM agents" in query:
            return [{"id": CURRENT_AGENT}]
        if "FROM thread_runtime_attach_abort_outcomes" in query:
            return deepcopy(self.outcomes)
        raise AssertionError(f"Unexpected fetch: {query}")

    async def execute(self, query, *args):
        if "UPDATE threads SET metadata=" in query:
            self.metadata_after = json.loads(args[1])
            return "UPDATE 1"
        raise AssertionError(f"Unexpected execute: {query}")


async def begin_initial(
    monkeypatch,
    outcomes,
    *,
    other_history=(),
    receipt=True,
    terminal_outcome=None,
    receipt_changes=None,
):
    conn = LockedInitialCreate(
        outcomes,
        other_history=other_history,
        receipt=receipt,
        terminal_outcome=terminal_outcome,
        receipt_changes=receipt_changes,
    )
    db = PostgresDB.__new__(PostgresDB)

    @asynccontextmanager
    async def acquire():
        yield conn

    db.acquire = acquire
    monkeypatch.setattr(
        "orchestrator.services.vm_thread_retained_resume.lock_owner_on_conn",
        AsyncMock(),
    )
    monkeypatch.setattr(
        "orchestrator.services.manifest_execution_snapshot.srw_snapshot_config",
        lambda _snapshot: ({}, {"workspace": {"backend": "vm"}}),
    )
    monkeypatch.setattr(
        "orchestrator.services.vm_resource_job_runtime.installed_job_resource_store",
        AsyncMock(return_value=None),
    )
    monkeypatch.setenv("VM_NETWORK_PROFILE_ENABLED", "false")
    configuration = json.loads(V1_JSON)
    request = {
        "job_id": str(THREAD),
        "entity_type": "thread",
        "provision_generation": str(PROVISION),
        "vm_image": "registry.example/session:fixed",
    }
    admitted = await db.begin_pinned_thread_vm_provisioning(
        str(THREAD),
        expected_runtime_generation=str(CURRENT_GENERATION),
        expected_agent_id=str(CURRENT_AGENT),
        expected_attach_token=str(CURRENT_ATTACH),
        expected_vm_context=None,
        provision_context={
            "provision_generation": str(PROVISION),
            "status": "provisioning",
            "identity_authenticated": False,
        },
        initial_creation={
            "execution_id": str(EXECUTION),
            "execution_revision": "frozen-revision",
            "execution_generation": 1,
        },
        creation_source={
            "request_id": str(REQUEST),
            "request": request,
            "request_digest": canonical_request_digest(request),
            "controller_configuration": configuration,
            "controller_configuration_digest": canonical_configuration_digest(
                configuration
            ),
        },
    )
    return admitted, conn


@pytest.mark.asyncio
async def test_zero_history_control_admits_same_first_vm_source(monkeypatch):
    admitted, conn = await begin_initial(monkeypatch, [])
    assert admitted is True
    assert conn.inserted_source is not None


@pytest.mark.asyncio
async def test_one_exact_no_payload_detach_keeps_first_vm_creation_eligible(
    monkeypatch,
):
    admitted, conn = await begin_initial(monkeypatch, [no_payload_outcome()])
    assert admitted is True
    assert conn.metadata_after["vm"]["initial_runtime"] == {
        "runtime_generation": str(CURRENT_GENERATION),
        "agent_id": str(CURRENT_AGENT),
        "runtime_attach_token": str(CURRENT_ATTACH),
    }
    assert conn.inserted_source is not None


@pytest.mark.asyncio
async def test_shuffled_two_detaches_form_one_never_issued_chain(monkeypatch):
    first = no_payload_outcome(successor_generation=MIDDLE_GENERATION)
    terminal = no_payload_outcome(
        runtime_generation=MIDDLE_GENERATION,
        runtime_attach_token=MIDDLE_ATTACH,
        agent_id=MIDDLE_AGENT,
        agent_pod_uid="middle-agent-pod",
    )
    admitted, conn = await begin_initial(
        monkeypatch, [terminal, first], terminal_outcome=terminal
    )
    assert admitted is True
    assert conn.metadata_after["vm"]["initial_runtime"]["runtime_generation"] == str(
        CURRENT_GENERATION
    )
    assert conn.inserted_source is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "receipt_changes",
    [
        {"version": 2},
        {"runtime_generation": str(CURRENT_GENERATION)},
        {"successor_generation": str(EXECUTION)},
        {"runtime_attach_token": str(CURRENT_ATTACH)},
        {"unexpected": "extra"},
    ],
)
async def test_bad_terminal_receipt_refuses_initial_vm(monkeypatch, receipt_changes):
    admitted, conn = await begin_initial(
        monkeypatch, [no_payload_outcome()], receipt_changes=receipt_changes
    )
    assert admitted is False
    assert conn.metadata_after is None
    assert conn.inserted_source is None


@pytest.mark.asyncio
async def test_more_than_4096_detaches_refuses_even_complete_chain(monkeypatch):
    outcomes = [
        no_payload_outcome(
            runtime_generation=UUID(int=1000 + index),
            successor_generation=(
                UUID(int=1001 + index) if index < 4096 else CURRENT_GENERATION
            ),
            runtime_attach_token=UUID(int=10000 + index),
            agent_id=UUID(int=20000 + index),
            agent_pod_uid=f"agent-pod-{index}",
        )
        for index in range(4097)
    ]
    admitted, conn = await begin_initial(monkeypatch, outcomes)
    assert admitted is False
    assert conn.metadata_after is None
    assert conn.inserted_source is None


@pytest.mark.asyncio
async def test_receipt_without_any_abort_outcome_refuses_initial_vm(monkeypatch):
    admitted, conn = await begin_initial(
        monkeypatch, [], terminal_outcome=no_payload_outcome()
    )
    assert admitted is False
    assert conn.metadata_after is None
    assert conn.inserted_source is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcomes,other_history,receipt",
    [
        ([no_payload_outcome(release_kind="process_zero")], (), True),
        ([no_payload_outcome(quiescence_protocol="agent_runtime_zero_v1")], (), True),
        ([no_payload_outcome(workspace_generation=PROVISION)], (), True),
        ([no_payload_outcome(workspace_runtime_incarnation=PROVISION)], (), True),
        ([no_payload_outcome(agent_pod_uid="")], (), True),
        ([no_payload_outcome(thread_id=EXECUTION)], (), True),
        ([no_payload_outcome(successor_generation=EXECUTION)], (), True),
        (
            [
                no_payload_outcome(successor_generation=MIDDLE_GENERATION),
                no_payload_outcome(
                    runtime_generation=EXECUTION,
                    runtime_attach_token=MIDDLE_ATTACH,
                    agent_id=MIDDLE_AGENT,
                ),
            ],
            (),
            True,
        ),
        (
            [
                no_payload_outcome(),
                no_payload_outcome(runtime_attach_token=CURRENT_ATTACH),
            ],
            (),
            True,
        ),
        (
            [
                no_payload_outcome(successor_generation=MIDDLE_GENERATION),
                no_payload_outcome(
                    successor_generation=CURRENT_GENERATION,
                    runtime_attach_token=MIDDLE_ATTACH,
                ),
            ],
            (),
            True,
        ),
        (
            [
                no_payload_outcome(),
                no_payload_outcome(
                    runtime_generation=MIDDLE_GENERATION,
                    runtime_attach_token=MIDDLE_ATTACH,
                    agent_id=MIDDLE_AGENT,
                    successor_generation=EXECUTION,
                ),
                no_payload_outcome(
                    runtime_generation=EXECUTION,
                    runtime_attach_token=CURRENT_ATTACH,
                    agent_id=CURRENT_AGENT,
                    successor_generation=MIDDLE_GENERATION,
                ),
            ],
            (),
            True,
        ),
        ([no_payload_outcome(successor_generation=OLD_GENERATION)], (), True),
        ([no_payload_outcome()], (), False),
        *[
            ([no_payload_outcome()], (table,), True)
            for table in (
                "vm_creation_retries",
                "vm_idle_operations",
                "thread_runtime_retirement_outcomes",
                "thread_workspace_provision_intents",
                "srw_execution_workspace_bindings",
            )
        ],
    ],
)
async def test_ambiguous_or_physical_history_still_blocks_initial_vm(
    monkeypatch, outcomes, other_history, receipt
):
    admitted, conn = await begin_initial(
        monkeypatch, outcomes, other_history=other_history, receipt=receipt
    )
    assert admitted is False
    assert conn.metadata_after is None
    assert conn.inserted_source is None
