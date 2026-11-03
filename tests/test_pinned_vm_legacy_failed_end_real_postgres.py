"""Real DB: a legacy failed VM marker becomes retirable once proven absent.

The legacy create path leaves ``metadata.vm`` failed with a provision
generation but no creation request and no VM identity. Begin used to report
that as ``physical_runtime_identity_malformed`` forever, so the agent Pod kept
its protection finalizer (vault issue
``failed_vm_session_strands_agent_pod_on_finalizer``).
"""

import json
from uuid import UUID, uuid4

import pytest

from orchestrator.services.vm_provisioner import VMProvisioner
from shared.pinned_vm_creation_retirement import bound_vm_without_source
from tests.test_pinned_vm_bound_no_source_end_real_postgres import (
    _begin_authorized,
    _source_free_receipt,
)
from tests.test_pinned_vm_initial_binding_real_postgres import (
    _bind_cold_agent,
    _bind_protected_agent,
    _initial_vm,
    db as _db,
)
from tests.test_vm_resource_thread_source_real_postgres import (
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    pg_dsn,  # noqa: F401
    thread_schema,  # noqa: F401
)


db = _db

PROOF = {
    "evidence": "controller_delete_then_exact_absence_v1",
    "proven_at": "2026-11-03T00:00:00+00:00",
}


def _legacy_marker(generation, status="failed"):
    marker = VMProvisioner._fresh_provision_ctx()
    marker.update(
        provision_generation=generation,
        status=status,
        error="VM create dispatch was not accepted",
        creation_observation={
            "version": 1,
            "provision_generation": generation,
            "outcome": "transport_unknown",
            "authenticated": False,
        },
    )
    return marker


def _metadata(row):
    metadata = row["metadata"]
    return json.loads(metadata) if isinstance(metadata, str) else metadata


async def _legacy_failed(db, monkeypatch, *, binding="cold", status="failed"):
    thread_id, _policy, _override, _dependencies = await _initial_vm(
        db, monkeypatch, native=True
    )
    if binding == "warm":
        current = await _bind_protected_agent(db, thread_id)
    else:
        current = await _bind_cold_agent(db, thread_id)
    generation = str(uuid4())
    marker = _legacy_marker(generation, status)
    await db.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata,'{vm}',$2::jsonb) WHERE id=$1",
        current["id"],
        json.dumps(marker),
    )
    current = await db.get_thread(str(current["id"]))
    agent = await db.fetchrow(
        "SELECT hostname,pod_uid FROM agents WHERE id=$1 AND thread_id=$2",
        current["agent_id"],
        current["id"],
    )
    assert agent is not None
    return current, agent, generation, marker


RECEIPT = {"owner_kind": "thread", "scope": "vm", "provisioner": "vm"}


async def _receipt(db, current, generation):
    # Production order: claim the generation, prove it absent (controller
    # effects are not under test here), then record its process-zero receipt.
    thread_id = str(current["id"])
    assert await db.claim_managed_repository_workspace_retirement(
        thread_id, runtime_incarnation=generation, **RECEIPT
    )
    assert await db.record_managed_repository_workspace_process_zero(
        thread_id, runtime_incarnation=generation, **RECEIPT
    )


async def _drop(db, current, generation, *, receipt=True):
    if receipt:
        await _receipt(db, current, generation)
    return await db.drop_legacy_failed_thread_vm_marker(
        str(current["id"]),
        runtime_generation=str(current["runtime_generation"]),
        provision_generation=generation,
        proof=PROOF,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("binding", ["warm", "cold"])
@pytest.mark.parametrize("status", ["failed", "deleted", "retiring_process_zero"])
async def test_begin_names_the_unproven_legacy_marker(db, monkeypatch, binding, status):
    current, _agent, _generation, _marker = await _legacy_failed(
        db, monkeypatch, binding=binding, status=status
    )
    retirement = await db.begin_pinned_thread_retirement(
        str(current["id"]),
        permanent=False,
        settle_status="ended",
        expected_runtime_generation=str(current["runtime_generation"]),
        expected_agent_id=str(current["agent_id"]),
        expected_attach_token=str(current["runtime_attach_token"]),
    )
    assert retirement == {
        "state": "malformed",
        "reason": "legacy_vm_absence_unproven",
    }
    assert (await db.get_thread(str(current["id"])))["runtime_retirement_token"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("binding", ["warm", "cold"])
async def test_proven_absence_retires_through_the_no_vm_path(db, monkeypatch, binding):
    current, agent, generation, marker = await _legacy_failed(
        db, monkeypatch, binding=binding
    )
    assert await _drop(db, current, generation)
    current = await db.get_thread(str(current["id"]))
    metadata = _metadata(current)
    assert "vm" not in metadata
    assert metadata["vm_legacy_absence"] == {
        **PROOF,
        "version": 1,
        "provision_generation": generation,
        "marker": {**marker, "status": "retiring_process_zero"},
    }

    retirement = await _begin_authorized(db, current, permanent=False)
    context = retirement["context"]
    assert context["workspace_backend"] == "vm"
    assert context["vm"] is None
    assert context["vm_creation_source"] is None
    assert bound_vm_without_source(context)
    assert await db.fetchval(
        "SELECT public.pinned_vm_bound_no_vm_source($1,$2,$3)",
        current["id"],
        current["runtime_generation"],
        UUID(retirement["token"]),
    )
    receipt = await _source_free_receipt(db, current, agent, retirement)
    assert receipt is not None
    assert receipt["quiescence_protocol"] == "agent_runtime_zero_v1"
    assert receipt["agent_pod_uid"] == agent["pod_uid"]
    assert await db.settle_pinned_thread_retirement(
        str(current["id"]),
        token=retirement["token"],
        generation=retirement["generation"],
        final_status="ended",
    )
    assert (await db.get_thread(str(current["id"])))["status"] == "ended"


@pytest.mark.asyncio
async def test_proven_absence_permits_permanent_delete(db, monkeypatch):
    current, agent, generation, _marker = await _legacy_failed(
        db, monkeypatch, binding="warm"
    )
    assert await _drop(db, current, generation)
    current = await db.get_thread(str(current["id"]))
    retirement = await _begin_authorized(db, current, permanent=True)
    assert await _source_free_receipt(db, current, agent, retirement) is not None
    assert await db.clear_pinned_retirement_physical_runtime_endpoint(
        str(current["id"]),
        runtime_generation=retirement["generation"],
        retirement_token=retirement["token"],
        completed_quiescence_protocol="agent_runtime_zero_v1",
        expected_stopped_agent_pod_name=agent["hostname"],
        expected_stopped_agent_pod_uid=agent["pod_uid"],
    )
    await db.delete_thread(
        str(current["id"]),
        expected_runtime_retirement_token=retirement["token"],
        expected_runtime_generation=retirement["generation"],
    )
    assert await db.get_thread(str(current["id"])) is None


@pytest.mark.asyncio
async def test_drop_refuses_without_the_process_zero_receipt(db, monkeypatch):
    current, _agent, generation, marker = await _legacy_failed(db, monkeypatch)
    assert not await _drop(db, current, generation, receipt=False)
    assert _metadata(await db.get_thread(str(current["id"])))["vm"] == marker


@pytest.mark.asyncio
async def test_drop_refuses_another_life_or_generation(db, monkeypatch):
    current, _agent, generation, _marker = await _legacy_failed(db, monkeypatch)
    await _receipt(db, current, generation)
    assert not await db.drop_legacy_failed_thread_vm_marker(
        str(current["id"]),
        runtime_generation=str(uuid4()),
        provision_generation=generation,
        proof=PROOF,
    )
    assert not await _drop(db, current, str(uuid4()), receipt=False)
    vm = _metadata(await db.get_thread(str(current["id"])))["vm"]
    assert vm["provision_generation"] == generation


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("vm_uid", "77777777-7777-4777-8777-777777777777"),
        ("creation_request_id", "88888888-8888-4888-8888-888888888888"),
        ("status", "aborted"),
    ],
)
async def test_drop_refuses_a_marker_that_names_vm_authority(
    db, monkeypatch, field, value
):
    current, _agent, generation, _marker = await _legacy_failed(db, monkeypatch)
    await _receipt(db, current, generation)
    await db.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata,ARRAY['vm',$2],to_jsonb($3::text)) "
        "WHERE id=$1",
        current["id"],
        field,
        value,
    )
    assert not await _drop(db, current, generation, receipt=False)
    assert "vm" in _metadata(await db.get_thread(str(current["id"])))


@pytest.mark.asyncio
async def test_drop_refuses_while_a_vm_ledger_names_the_thread(db, monkeypatch):
    current, _agent, generation, _marker = await _legacy_failed(db, monkeypatch)
    await _receipt(db, current, generation)
    async with db.acquire() as conn:
        async with conn.transaction():
            # Seed one VM ledger directly; its own writers are not under test.
            await conn.execute("SET LOCAL session_replication_role='replica'")
            await conn.execute(
                "INSERT INTO vm_thread_creation_owners(thread_id,live_thread_id) "
                "VALUES ($1,$1)",
                current["id"],
            )
    assert not await _drop(db, current, generation)
    assert "vm" in _metadata(await db.get_thread(str(current["id"])))


def _stuck_terminating_pod(api, *, namespace, name, uid):
    """The dev shape: deleted, every container exited, held by the finalizer."""

    from types import SimpleNamespace

    from orchestrator.services.pinned_k8s_effect import PINNED_AUTHORITY_FINALIZER

    metadata = api._metadata(
        uid=uid,
        labels={"srw/component": "persistent-agent"},
        finalizers=[PINNED_AUTHORITY_FINALIZER],
    )
    metadata.name = name
    metadata.namespace = namespace
    metadata.deletion_timestamp = "2026-09-23T09:25:40Z"
    exited = SimpleNamespace(
        name="agent",
        ready=False,
        state=SimpleNamespace(terminated=SimpleNamespace(exit_code=137)),
    )
    api.pods[(namespace, name)] = SimpleNamespace(
        metadata=metadata,
        spec=SimpleNamespace(
            restart_policy="Never",
            containers=[SimpleNamespace(name="agent")],
            init_containers=[],
            ephemeral_containers=[],
        ),
        status=SimpleNamespace(
            phase="Failed",
            pod_ip=None,
            container_statuses=[exited],
            init_container_statuses=[],
            ephemeral_container_statuses=[],
            conditions=[],
        ),
    )


@pytest.mark.asyncio
async def test_offline_retirement_proves_the_vm_absent_then_releases_the_pod(
    db, monkeypatch
):
    """The dev incident end to end, through the stale-agent detector's own
    entry points: offline retirement, then its durable retirement retry."""

    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from orchestrator import main
    from orchestrator.application import controls
    from orchestrator.services import agent_provisioner as agent_module
    from orchestrator.services import stale_agent_detector as detector
    from orchestrator.services import vm_provisioner as vm_module
    from orchestrator.services import vm_thread_legacy_absence as legacy
    from tests import test_persistent_recycler_real_postgres as k8s

    current, agent, generation, _marker = await _legacy_failed(db, monkeypatch)
    thread_id = str(current["id"])
    await db.execute(
        "UPDATE agents SET status='offline' WHERE id=$1", current["agent_id"]
    )
    api = k8s.StatefulPinnedK8sApi()
    _stuck_terminating_pod(
        api, namespace="agents-a", name=agent["hostname"], uid=agent["pod_uid"]
    )
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(
        agent_module,
        "agent_provisioner",
        k8s._production_warm_provisioner(db, api, namespace="agents-a"),
    )
    controller = SimpleNamespace(
        lifecycle_available=True,
        prove_legacy_thread_vm_absent=AsyncMock(return_value=True),
    )
    monkeypatch.setattr(vm_module, "vm_provisioner", controller)
    monkeypatch.setattr(legacy, "_first_seen", {})
    monkeypatch.setattr(detector, "_offline_retirement_backoff", {})
    dependencies = controls.stale_agent_detector_dependencies(main.app.state.resources)
    candidate = {
        "id": thread_id,
        "status": "created",
        "runtime_generation": str(current["runtime_generation"]),
        "agent_id": str(current["agent_id"]),
        "runtime_attach_token": str(current["runtime_attach_token"]),
    }

    # First sweep: the marker is new to this process, so End waits.
    assert not await detector.retire_orphaned_pinned_runtime(
        candidate, dependencies=dependencies
    )
    controller.prove_legacy_thread_vm_absent.assert_not_awaited()
    assert (await db.get_thread(thread_id))["runtime_retirement_token"] is None
    assert not detector._offline_retirement_backoff

    # A later sweep, past the controller's replay window: proof, then Begin.
    legacy._first_seen[(thread_id, generation)] -= (
        legacy.LEGACY_VM_ABSENCE_SETTLE_DELAY_SECONDS
    )
    assert await detector.retire_orphaned_pinned_runtime(
        candidate, dependencies=dependencies
    )
    controller.prove_legacy_thread_vm_absent.assert_awaited_once_with(
        thread_id, provision_generation=generation
    )
    pending = await db.get_thread(thread_id)
    assert pending["runtime_retirement_token"] is not None
    assert "vm" not in _metadata(pending)

    # The durable retry stops the exact Pod, releases its finalizer, settles.
    assert await detector.retry_pending_pinned_retirement(
        dict(pending), dependencies=dependencies
    )
    ended = await db.get_thread(thread_id)
    assert ended["status"] == "ended"
    assert ended["runtime_retirement_token"] is None
    assert _metadata(ended)["vm_legacy_absence"]["provision_generation"] == (generation)
    # The finalizer came off, so the Pod stuck since September is gone.
    assert ("agents-a", agent["hostname"]) not in api.pods
