"""Normal End must dispose a failed initial source without rewriting its actor."""

import json
import logging
import asyncio
from time import monotonic
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio
import asyncpg

from orchestrator.services.pinned_retirement import PinnedRetirementOperations
from orchestrator.services.session_attach_binding import release_session_attach_binding
from orchestrator.services.vm_creation_retry_store import (
    VMCreationRetryConflict,
    VMCreationRetryStore,
)
from tests.test_pinned_vm_initial_binding_real_postgres import (
    _bind_protected_agent,
    _initial_vm,
    _poll,
)
from tests.test_vm_resource_thread_source_real_postgres import (
    _adopted_charged_thread,
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    pg_dsn,  # noqa: F401
    thread_schema,  # noqa: F401
)
from tests.test_vm_creation_actuation import setup as _setup_fixture, SECRET

from shared.vm_creation_issuance import seal_creation_carrier
from vm_controller.creation_actuation import CreationActuator

setup = _setup_fixture


@pytest_asyncio.fixture(scope="module")
async def cleanup_lineage_schema(pg_dsn, thread_schema):  # noqa: F811
    conn = await asyncpg.connect(pg_dsn)
    try:
        if not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname='thread_vm_creation_cleanup_lineage')"
        ):
            await conn.execute(
                (
                    Path(__file__).resolve().parents[1]
                    / (
                        "src/orchestrator/database/migrations/app/"
                        "0288_vm_initial_creation_cleanup_lineage.sql"
                    )
                ).read_text()
            )
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(cleanup_lineage_schema, _base_db):  # noqa: F811
    yield _base_db


async def _release_binding(db, current):
    agent = await db.fetchrow("SELECT * FROM agents WHERE id=$1", current["agent_id"])

    async def finalizer_released(store, *, protection_id, **_kwargs):
        assert await store.complete_pinned_warm_binding_release(
            protection_id,
            release_outcome="exact_live_unprotected_v1",
            agent_present=True,
        )

    return await release_session_attach_binding(
        str(current["agent_id"]),
        str(current["id"]),
        expected_runtime_generation=str(current["runtime_generation"]),
        expected_attach_token=str(current["runtime_attach_token"]),
        expected_agent_pod_uid=agent["pod_uid"],
        local_runtime_quiesced=True,
        local_quiescence_protocol="agent_attach_not_started_v1",
        dependencies=SimpleNamespace(
            store=db,
            release_pinned_warm_binding_protection=finalizer_released,
            agent_provisioner=None,
            persistent_provisioner=None,
        ),
    )


async def _abort_and_rebind_same_pod(db, current, *, rebind=True):
    """Real abort/rotation/rebinding; substitute only the finalizer receipt."""
    agent = await db.fetchrow("SELECT * FROM agents WHERE id=$1", current["agent_id"])
    assert await _release_binding(db, current) == "released"
    detached = await db.get_thread(str(current["id"]))
    assert detached["runtime_generation"] != current["runtime_generation"]
    assert detached["agent_id"] is None and detached["runtime_attach_token"] is None
    outcome = await db.fetchrow(
        "SELECT * FROM thread_runtime_attach_abort_outcomes WHERE thread_id=$1 AND runtime_generation=$2",
        current["id"],
        current["runtime_generation"],
    )
    assert outcome["successor_generation"] == detached["runtime_generation"]
    assert outcome["agent_pod_uid"] == agent["pod_uid"]
    assert outcome["release_kind"] == "process_zero"
    assert outcome["quiescence_protocol"] == "agent_attach_not_started_v1"
    assert outcome["workspace_runtime_incarnation"] is None
    if not rebind:
        return detached

    protection_id, attach_token, effect_token = (uuid4() for _ in range(3))
    assert await db.plan_pinned_warm_binding_protection(
        str(current["id"]),
        expected_runtime_generation=str(detached["runtime_generation"]),
        runtime_attach_token=str(attach_token),
        agent_id=str(agent["id"]),
        protection_id=str(protection_id),
        source="attach",
        provisioner="agent",
        namespace="agents-a",
        pod_name=agent["hostname"],
        pod_uid=agent["pod_uid"],
        discovered_resource_version="20",
    )
    assert await db.claim_pinned_warm_binding_effect(
        str(protection_id), effect_token=str(effect_token)
    )
    assert await db.publish_pinned_warm_binding_protection(
        str(protection_id),
        effect_token=str(effect_token),
        expected_pod_uid=agent["pod_uid"],
        protection_resource_version="21",
        evidence_protocol="exact_live_finalizer_v1",
    )
    assert await db.bind_pinned_warm_agent(str(protection_id))
    rebound = await db.get_thread(str(current["id"]))
    assert rebound["agent_id"] == current["agent_id"]
    assert rebound["runtime_attach_token"] != current["runtime_attach_token"]
    return rebound


async def _failed_source(db, monkeypatch, source_order="bound", abort_count=2):
    thread_id, policy, override, dependencies = await _initial_vm(
        db, monkeypatch, native=True
    )
    if source_order == "prebind":
        owner = await db.get_thread(str(thread_id))
        assert await dependencies.vm_provisioner.create_thread_vm(
            str(thread_id),
            vm_image=override["workspace"]["vm"]["image"],
            expected_runtime_generation=str(owner["runtime_generation"]),
        )
        source = await db.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE thread_id=$1", thread_id
        )
        assert (await policy.admit(request_id=str(source["request_id"])))[
            "action"
        ] == "admitted"
    current = await _bind_protected_agent(db, thread_id)
    if source_order == "bound":
        await _poll(db, dependencies.vm_provisioner, current)
        source = await db.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE thread_id=$1", thread_id
        )
        assert (await policy.admit(request_id=str(source["request_id"])))[
            "action"
        ] == "admitted"
    for _ in range(abort_count):
        current = await _abort_and_rebind_same_pod(db, current)
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
            source["request_id"],
        )
        == 0
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE request_id=$1",
            source["request_id"],
        )
        == "reserved"
    )

    return current, source


async def _begin(db, current, *, permanent=True):
    operations = PinnedRetirementOperations(
        SimpleNamespace(
            store=db,
            agent_provisioner=None,
            persistent_provisioner=None,
            logger=logging.getLogger(__name__),
        )
    )
    return await operations.begin_pinned_thread_retirement(
        str(current["id"]),
        permanent=permanent,
        settle_status="ended",
        expected_runtime_generation=str(current["runtime_generation"]),
        expected_agent_id=str(current["agent_id"]) if current["agent_id"] else None,
        expected_attach_token=str(current["runtime_attach_token"])
        if current["runtime_attach_token"]
        else None,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source_order,abort_count",
    [
        ("bound", 0),
        ("prebind", 0),
        ("prebind", 2),
        ("bound", 2),
    ],
)
async def test_normal_end_settles_never_issued_initial_source_across_proven_binding_history(
    db,
    monkeypatch,
    source_order,
    abort_count,
):
    current, source = await _failed_source(db, monkeypatch, source_order, abort_count)
    thread_id = current["id"]
    retirement = await _begin(db, current)
    assert retirement["state"] == "pending", retirement
    captured = retirement["context"]["vm_creation_source"]
    assert captured["thread_runtime_generation"] == str(
        source["thread_runtime_generation"]
    )
    assert captured["request_id"] == str(source["request_id"])
    assert await db.authorize_pinned_thread_retirement(
        str(thread_id),
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    assert await VMCreationRetryStore(db).settle_never_issued(
        request_id=str(source["request_id"])
    ) == {
        "settled": True,
        "disposition": "never_issued",
    }
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE request_id=$1",
            source["request_id"],
        )
        == "released"
    )
    after = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1", source["request_id"]
    )
    for field in (
        "thread_runtime_generation",
        "thread_agent_id",
        "thread_attach_token",
        "provision_generation",
        "canonical_request",
        "request_digest",
    ):
        assert after[field] == source[field]
    assert "vm" not in json.loads((await db.get_thread(str(thread_id)))["metadata"])
    assert await db.fetchval(
        "SELECT public.thread_vm_creation_never_issued_source($1,$2)",
        thread_id,
        str(source["provision_generation"]),
    )


async def _partial_after_aborts(db, monkeypatch, last_effect, *, permanent=True):
    from orchestrator.services.vm_creation_disposition_store import (
        VMCreationDispositionStore,
    )

    (
        _,
        _,
        _,
        _,
        thread_id,
        original_runtime,
        generation,
        request_id,
        admitted,
        observations,
        carrier,
    ) = await _adopted_charged_thread(
        db, monkeypatch, stop_after=last_effect, adopt=False
    )
    current = await _bind_protected_agent(db, thread_id)
    for _ in range(2):
        current = await _abort_and_rebind_same_pod(db, current)
    retirement = await _begin(db, current, permanent=permanent)
    assert retirement["state"] == "pending", retirement
    await _authorize(db, current, retirement)
    return (
        VMCreationDispositionStore(VMCreationRetryStore(db)),
        thread_id,
        original_runtime,
        generation,
        request_id,
        admitted,
        observations,
        carrier,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "last_effect,lost_reply", [("rootdisk", False), ("cloud_init", True)]
)
async def test_partial_storage_across_aborts_keeps_original_uid_and_disposes_before_release(
    db, setup, monkeypatch, last_effect, lost_reply
):
    from tests import test_vm_resource_thread_disposition_real_postgres as helpers
    from shared.vm_creation_disposition import disposition_identity
    from vm_controller.creation_disposition import CreationDisposer

    # Reuse only the existing modeled Kubernetes transport; source, protected
    # abort/rebind, End, child grants and disposition settlement use the real DB.
    monkeypatch.setattr(helpers, "partial_thread", _partial_after_aborts)
    (
        ctrl,
        api,
        store,
        row,
        thread_id,
        admitted,
        observations,
        carrier,
    ) = await helpers.controller_runtime(db, setup, monkeypatch, last_effect)
    frozen = await store.freeze_disposition(
        request_id=row["request_id"], carrier=carrier
    )
    assert frozen["frozen"]
    assert (
        frozen["disposition"]["thread_runtime_generation"]
        == row["thread_runtime_generation"]
    )
    current = await db.get_thread(str(thread_id))
    assert str(current["runtime_generation"]) != row["thread_runtime_generation"]
    retirement = await _begin(db, current)
    arguments = await _current_zero_arguments(db, current, retirement)
    assert (
        await db.acknowledge_pinned_thread_local_quiescence(str(thread_id), **arguments)
        is None
    )
    assert (
        frozen["disposition"]["objects"]["rootdisk"]["pvc_uid"]
        == observations["rootdisk"]["pvc"]["metadata"]["uid"]
    )
    with pytest.raises(
        VMCreationRetryConflict, match="creation_disposition_incomplete"
    ):
        await store.settle_disposition(request_id=row["request_id"], carrier=carrier)
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            admitted["reservation_id"],
        )
        == "reserved"
    )
    if lost_reply:
        api.lost_deletes.add("DataVolume")
    for _ in range(8):
        result = await CreationDisposer(ctrl).run(disposition_identity(row))
        if result["status"] == "creation_disposed":
            break
    assert result["status"] == "creation_disposed", result
    assert await VMCreationRetryStore(db).settle_disposition(
        request_id=row["request_id"], carrier=carrier
    ) == {"settled": True, "disposition": "creation_disposed"}
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            admitted["reservation_id"],
        )
        == "released"
    )
    assert await db.fetchval(
        "SELECT public.thread_vm_creation_never_issued_source($1,$2)",
        thread_id,
        row["provision_generation"],
    )
    operations = PinnedRetirementOperations(SimpleNamespace(store=db))
    assert await operations._settle_vm_creation_source(
        retirement, require_initial_agent_zero=True
    )
    assert set(kind for kind, _, _ in api.deletes) >= {
        "DataVolume",
        "PersistentVolumeClaim",
    }


@pytest.mark.asyncio
async def test_source_pin_debt_after_aborts_cannot_use_no_effect_shortcut(
    db, setup, monkeypatch
):
    from tests import test_vm_resource_thread_disposition_real_postgres as helpers
    from shared.vm_creation_disposition import disposition_identity
    from vm_controller.creation_disposition import CreationDisposer
    from vm_controller.creation_sources import pins

    original = helpers._adopted_charged_thread

    async def prepare_then_abort(*args, **kwargs):
        result = await original(*args, **kwargs)
        current = await _bind_protected_agent(db, result[4])
        current = await _abort_and_rebind_same_pod(db, current)
        current = await _abort_and_rebind_same_pod(db, current, rebind=False)
        return (*result[:5], current["runtime_generation"], *result[6:])

    monkeypatch.setattr(helpers, "_adopted_charged_thread", prepare_then_abort)
    (
        ctrl,
        api,
        store,
        row,
        admitted,
        source_name,
    ) = await helpers.missing_carrier_runtime(db, setup, monkeypatch)
    assert row["effects"] == []
    assert (
        pins(api.read("DataVolume", source_name))[row["request_id"]]["state"]
        == "active"
    )
    assert await store.settle_never_issued(request_id=row["request_id"]) == {
        "settled": False,
        "reason": "creation_source_unresolved",
    }
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            admitted["reservation_id"],
        )
        == "reserved"
    )
    api.lost.add("DataVolume")
    for _ in range(8):
        result = await CreationDisposer(ctrl).run(disposition_identity(row))
        if result["status"] == "creation_disposed":
            break
    assert result["status"] == "creation_disposed", result
    assert (
        pins(api.read("DataVolume", source_name))[row["request_id"]]["state"]
        == "disposed"
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            admitted["reservation_id"],
        )
        == "released"
    )


async def _damage_first_edge(db, source, corruption):
    """Adversarial history only: production insert/update guards remain enabled elsewhere."""
    async with db.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL session_replication_role='replica'")
        if corruption == "missing":
            await conn.execute(
                "DELETE FROM thread_runtime_attach_abort_outcomes WHERE thread_id=$1 AND runtime_generation=$2",
                source["thread_id"],
                source["thread_runtime_generation"],
            )
        elif corruption == "branch":
            await conn.execute(
                "INSERT INTO thread_runtime_attach_abort_outcomes "
                "SELECT thread_id,runtime_generation,$3,agent_id,agent_pod_uid,successor_generation,"
                "release_kind,quiescence_protocol,workspace_generation,workspace_runtime_incarnation,released_at "
                "FROM thread_runtime_attach_abort_outcomes WHERE thread_id=$1 AND runtime_generation=$2",
                source["thread_id"],
                source["thread_runtime_generation"],
                uuid4(),
            )
        else:
            columns = {
                "actor": ("agent_id", uuid4()),
                "attach": ("runtime_attach_token", uuid4()),
                "pod": ("agent_pod_uid", " "),
                "cycle": ("successor_generation", source["thread_runtime_generation"]),
                "foreign": ("thread_id", uuid4()),
                "protocol": ("quiescence_protocol", "agent_runtime_zero_v1"),
                "workspace": ("workspace_runtime_incarnation", uuid4()),
            }
            column, value = columns[corruption]
            await conn.execute(
                f"UPDATE thread_runtime_attach_abort_outcomes SET {column}=$3 WHERE thread_id=$1 AND runtime_generation=$2",
                source["thread_id"],
                source["thread_runtime_generation"],
                value,
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "corruption",
    [
        "missing",
        "branch",
        "actor",
        "attach",
        "pod",
        "cycle",
        "foreign",
        "protocol",
        "workspace",
    ],
)
async def test_incomplete_or_incompatible_abort_authority_never_cancels_source(
    db, monkeypatch, corruption
):
    current, source = await _failed_source(db, monkeypatch)
    await _damage_first_edge(db, source, corruption)
    assert await _begin(db, current) == {
        "state": "malformed",
        "reason": "physical_runtime_identity_malformed",
    }
    assert (
        await db.fetchval(
            "SELECT state FROM vm_creation_retries WHERE request_id=$1",
            source["request_id"],
        )
        == source["state"]
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE request_id=$1",
            source["request_id"],
        )
        == "reserved"
    )
    assert (await db.get_thread(str(current["id"])))["runtime_retirement_token"] is None


@pytest.mark.asyncio
async def test_settlement_rechecks_database_lineage_after_end_capture(db, monkeypatch):
    current, source = await _failed_source(db, monkeypatch)
    retirement = await _begin(db, current)
    assert retirement["state"] == "pending"
    assert await db.authorize_pinned_thread_retirement(
        str(current["id"]),
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    await _damage_first_edge(db, source, "missing")
    with pytest.raises(
        VMCreationRetryConflict, match="thread_retirement_source_changed"
    ):
        await VMCreationRetryStore(db).settle_never_issued(
            request_id=str(source["request_id"])
        )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE request_id=$1",
            source["request_id"],
        )
        == "reserved"
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_creation_retries WHERE request_id=$1",
            source["request_id"],
        )
        == "cancel_requested"
    )


@pytest.mark.asyncio
async def test_more_than_64_real_attach_aborts_remain_retirable(db, monkeypatch):
    current, source = await _failed_source(db, monkeypatch, abort_count=65)
    started = monotonic()
    retirement = await _begin(db, current)
    print(f"65 real aborts: Begin {monotonic() - started:.3f}s")
    assert retirement["state"] == "pending"
    assert retirement["context"]["vm_creation_source"][
        "thread_runtime_generation"
    ] == str(source["thread_runtime_generation"])


@pytest.mark.asyncio
@pytest.mark.parametrize("hops", [4096, 4097])
async def test_lineage_traversal_bound_uses_index_and_holds_on_overflow(
    db, monkeypatch, hops
):
    current, source = await _failed_source(db, monkeypatch)
    # Boundary/algorithm fixture only. The65-edge case above uses actual
    # production transitions; these synthetic rows do not claim that proof.
    first = await db.fetchrow(
        "SELECT * FROM thread_runtime_attach_abort_outcomes WHERE thread_id=$1 AND runtime_generation=$2",
        current["id"],
        source["thread_runtime_generation"],
    )
    generations = [
        source["thread_runtime_generation"],
        *(uuid4() for _ in range(hops - 1)),
        current["runtime_generation"],
    ]
    async with db.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL session_replication_role='replica'")
        await conn.execute(
            "DELETE FROM thread_runtime_attach_abort_outcomes WHERE thread_id=$1",
            current["id"],
        )
        await conn.executemany(
            "INSERT INTO thread_runtime_attach_abort_outcomes "
            "(thread_id,runtime_generation,runtime_attach_token,agent_id,agent_pod_uid,successor_generation,release_kind,quiescence_protocol) "
            "VALUES($1,$2,$3,$4,$5,$6,'process_zero','agent_attach_not_started_v1')",
            [
                (
                    current["id"],
                    predecessor,
                    first["runtime_attach_token"] if index == 0 else uuid4(),
                    first["agent_id"],
                    first["agent_pod_uid"],
                    successor,
                )
                for index, (predecessor, successor) in enumerate(
                    zip(generations, generations[1:])
                )
            ],
        )
    plan = await db.fetchval(
        "EXPLAIN (FORMAT JSON) SELECT * FROM thread_runtime_attach_abort_outcomes WHERE thread_id=$1 AND runtime_generation=$2",
        current["id"],
        source["thread_runtime_generation"],
    )
    assert "thread_runtime_attach_abort_outcomes_pkey" in str(plan)
    started = monotonic()
    retirement = await _begin(db, current)
    print(f"{hops} synthetic edges: Begin {monotonic() - started:.3f}s; plan={plan}")
    assert retirement["state"] == ("pending" if hops == 4096 else "malformed")
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE request_id=$1",
            source["request_id"],
        )
        == "reserved"
    )


async def _authorize(db, current, retirement):
    assert await db.authorize_pinned_thread_retirement(
        str(current["id"]),
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("late_proof", ["unused_grant", "api_rejected"])
async def test_issued_bound_source_across_aborts_waits_for_exact_issuer_proof(
    db, monkeypatch, late_proof
):
    current, source = await _failed_source(db, monkeypatch, abort_count=0)
    monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", SECRET.decode())
    retries = VMCreationRetryStore(db)
    claim = (await retries.claim_due(limit=1))[0]
    observed = {
        "job_id": str(current["id"]),
        "provision_generation": str(source["provision_generation"]),
        "request_digest": source["request_digest"],
        "controller_configuration_digest": source["controller_configuration_digest"],
        "expected_pvc_uid": None,
    }
    permit = await retries.authorize_controller(
        request_id=str(source["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed=observed,
    )
    assert permit["allowed"]
    inspected = await retries.inspect(request_id=str(source["request_id"]))
    values = CreationActuator.values(
        None,
        inspected,
        permit,
        "rootdisk",
        None,
        None,
        None,
        {"kind": "registry", "image": inspected["request"]["vm_image"]},
    )
    carrier = seal_creation_carrier(
        values,
        namespace="workers",
        uid=str(uuid4()),
        resource_version="1",
        secret=SECRET,
    )
    granted = await retries.begin_effect(
        request_id=str(source["request_id"]),
        claim_token=str(claim["claim_token"]),
        carrier=carrier,
    )
    assert granted["actuation_allowed"]
    for _ in range(2):
        current = await _abort_and_rebind_same_pod(db, current)
    assert not (
        await retries.authorize_controller(
            request_id=str(source["request_id"]),
            claim_token=str(claim["claim_token"]),
            observed=observed,
        )
    )["allowed"]
    with pytest.raises(VMCreationRetryConflict):
        await retries.begin_effect(
            request_id=str(source["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=carrier,
        )
    retirement = await _begin(db, current)
    assert retirement["state"] == "pending"
    await _authorize(db, current, retirement)
    assert await retries.settle_never_issued(request_id=str(source["request_id"])) == {
        "settled": False,
        "reason": "creation_effect_unresolved",
    }
    arguments = await _current_zero_arguments(db, current, retirement)
    assert (
        await db.acknowledge_pinned_thread_local_quiescence(
            str(current["id"]), **arguments
        )
        is None
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE request_id=$1",
            source["request_id"],
        )
        == "reserved"
    )
    if late_proof == "unused_grant":
        payload = dict(
            request_id=str(source["request_id"]),
            effect_nonce=granted["effect_nonce"],
            carrier=carrier,
            issuer_receipt=granted["issuer_receipt"],
            reason="resource_node_changed",
        )
        with pytest.raises(VMCreationRetryConflict):
            await retries.record_not_attempted(
                **{**payload, "issuer_receipt": "a" * 64}
            )
        assert await retries.record_not_attempted(**payload) == {
            "recorded": True,
            "effect_state": "rejected",
        }
    else:
        assert await retries.observe_effect(
            request_id=str(source["request_id"]),
            carrier=carrier,
            observation={
                "outcome": "rejected",
                "api_status": {
                    "apiVersion": "v1",
                    "kind": "Status",
                    "status": "Failure",
                    "code": 403,
                    "reason": "Forbidden",
                },
            },
        ) == {"recorded": True, "effect_state": "rejected"}
    assert await VMCreationRetryStore(db).settle_never_issued(
        request_id=str(source["request_id"])
    ) == {"settled": True, "disposition": "never_issued"}
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE request_id=$1",
            source["request_id"],
        )
        == "released"
    )


@pytest.mark.asyncio
async def test_replaced_partial_uid_after_aborts_keeps_charge_without_deletion(
    db, setup, monkeypatch
):
    from tests import test_vm_resource_thread_disposition_real_postgres as helpers
    from shared.vm_creation_disposition import disposition_identity
    from vm_controller.creation_disposition import CreationDisposer

    monkeypatch.setattr(helpers, "partial_thread", _partial_after_aborts)
    ctrl, api, store, row, _, admitted, _, _ = await helpers.controller_runtime(
        db, setup, monkeypatch, "cloud_init"
    )
    name = next(
        name for resource, name in api.objects if resource == "PersistentVolumeClaim"
    )
    api.objects["PersistentVolumeClaim", name]["metadata"]["uid"] = str(uuid4())
    result = await CreationDisposer(ctrl).run(disposition_identity(row))
    assert result["status"] == "creation_attention"
    assert api.deletes == []
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            admitted["reservation_id"],
        )
        == "reserved"
    )


async def _wait_for_owner_waiters(db, count):
    for _ in range(300):
        waiting = await db.fetchval(
            "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
            # Owner/PVC admission now queues some contenders on the shared
            # advisory prefix before the same owner row. This database has only
            # the two test producers; count both parts of that lock chain.
            "AND pid<>pg_backend_pid() AND wait_event_type='Lock'"
        )
        if waiting >= count:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("production owner-lock contenders did not queue")


@pytest.mark.asyncio
@pytest.mark.parametrize("first", ["end", "abort"])
async def test_end_and_attach_abort_serialize_on_current_owner_before_successor(
    db, monkeypatch, first
):
    current, source = await _failed_source(db, monkeypatch)
    async with db.acquire() as owner_lock:
        async with owner_lock.transaction():
            await owner_lock.fetchval(
                "SELECT id FROM threads WHERE id=$1 FOR UPDATE", current["id"]
            )
            first_task = asyncio.create_task(
                _begin(db, current) if first == "end" else _release_binding(db, current)
            )
            await _wait_for_owner_waiters(db, 1)
            second_task = asyncio.create_task(
                _release_binding(db, current) if first == "end" else _begin(db, current)
            )
            await _wait_for_owner_waiters(db, 2)
    first_result, second_result = await asyncio.gather(first_task, second_task)
    begin, released = (
        (first_result, second_result)
        if first == "end"
        else (second_result, first_result)
    )
    after = await db.get_thread(str(current["id"]))
    if first == "end":
        assert begin["state"] == "pending"
        assert released == "unsafe"
        assert after["runtime_generation"] == current["runtime_generation"]
        assert str(after["runtime_retirement_token"]) == begin["token"]
    else:
        assert released == "released"
        assert begin["state"] == "conflict"
        assert after["runtime_generation"] != current["runtime_generation"]
        assert after["runtime_retirement_token"] is None
        successor = await _bind_protected_agent(db, current["id"])
        assert successor["agent_id"] != current["agent_id"]
        begin = await _begin(db, successor)
        assert begin["state"] == "pending"
        assert begin["generation"] == str(successor["runtime_generation"])
    assert begin["context"]["vm_creation_source"]["thread_runtime_generation"] == str(
        source["thread_runtime_generation"]
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE request_id=$1",
            source["request_id"],
        )
        == "reserved"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("permanent", [False, True])
@pytest.mark.parametrize("abort_count", [0, 2])
async def test_lost_replies_and_aborted_preflight_preserve_source_and_require_current_zero(
    db, monkeypatch, permanent, abort_count
):
    current, source = await _failed_source(db, monkeypatch, abort_count=abort_count)
    hidden = await _begin(db, current, permanent=permanent)
    assert hidden["state"] == "pending"
    replay = await _begin(db, current, permanent=permanent)
    assert replay["reused"] is True and replay["token"] == hidden["token"]
    assert await db.abort_pinned_thread_retirement(
        str(current["id"]), token=hidden["token"], generation=hidden["generation"]
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_creation_retries WHERE request_id=$1",
            source["request_id"],
        )
        == source["state"]
    )
    retirement = await _begin(db, current, permanent=permanent)
    assert retirement["token"] != hidden["token"]
    await _authorize(db, current, retirement)
    await _authorize(db, current, retirement)
    for _ in range(2):
        assert await VMCreationRetryStore(db).settle_never_issued(
            request_id=str(source["request_id"])
        ) == {"settled": True, "disposition": "never_issued"}
    if not permanent:
        assert not await db.settle_pinned_thread_retirement(
            str(current["id"]),
            token=retirement["token"],
            generation=retirement["generation"],
            final_status="ended",
        )
    assert not await db.clear_pinned_retirement_physical_runtime_endpoint(
        str(current["id"]),
        runtime_generation=retirement["generation"],
        retirement_token=retirement["token"],
    )
    with pytest.raises(RuntimeError):
        await db.delete_thread(
            str(current["id"]),
            expected_runtime_retirement_token=retirement["token"],
            expected_runtime_generation=retirement["generation"],
        )
    agent = await db.fetchrow("SELECT * FROM agents WHERE id=$1", current["agent_id"])
    quiescence = dict(
        expected_runtime_generation=retirement["generation"],
        expected_retirement_token=retirement["token"],
        expected_agent_id=str(current["agent_id"]),
        expected_attach_token=str(current["runtime_attach_token"]),
        expected_settle_status="ended",
        expected_quiescence_protocol="agent_runtime_zero_v1",
        expected_workspace_generation=None,
        expected_workspace_runtime_incarnation=None,
        expected_agent_pod_uid=agent["pod_uid"],
        require_zero_admission=True,
    )
    assert (
        await db.acknowledge_pinned_thread_local_quiescence(
            str(current["id"]),
            **{**quiescence, "expected_runtime_generation": str(uuid4())},
        )
        is None
    )
    if abort_count:
        assert (
            await db.acknowledge_pinned_thread_local_quiescence(
                str(current["id"]),
                **{
                    **quiescence,
                    "expected_runtime_generation": str(
                        source["thread_runtime_generation"]
                    ),
                    "expected_attach_token": str(source["thread_attach_token"]),
                },
            )
            is None
        )
    assert await db.acknowledge_pinned_thread_local_quiescence(
        str(current["id"]), **quiescence
    )
    if not permanent:
        assert await db.settle_pinned_thread_retirement(
            str(current["id"]),
            token=retirement["token"],
            generation=retirement["generation"],
            final_status="ended",
        )
        ended = await db.get_thread(str(current["id"]))
        assert ended["status"] == "ended" and ended["agent_id"] is None
        assert "vm" not in json.loads(ended["metadata"])
        return
    # Supply the existing exact actuator receipt boundary, not cluster absence.
    assert await db.clear_pinned_retirement_physical_runtime_endpoint(
        str(current["id"]),
        runtime_generation=retirement["generation"],
        retirement_token=retirement["token"],
        completed_quiescence_protocol="agent_runtime_zero_v1",
        expected_stopped_agent_pod_name=agent["hostname"],
        expected_stopped_agent_pod_uid=agent["pod_uid"],
    )
    assert await db.pinned_retirement_external_cleanup_complete(
        str(current["id"]),
        runtime_generation=retirement["generation"],
        retirement_token=retirement["token"],
    )
    # Stable audit ownership retains the settled source byte-for-byte after
    # exact permanent End removes its live thread.
    settled_source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1", source["request_id"]
    )
    await db.delete_thread(
        str(current["id"]),
        expected_runtime_retirement_token=retirement["token"],
        expected_runtime_generation=retirement["generation"],
    )
    assert await db.get_thread(str(current["id"])) is None
    assert (
        await db.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1",
            source["request_id"],
        )
        == settled_source
    )


async def _current_zero_arguments(db, current, retirement):
    agent = await db.fetchrow("SELECT * FROM agents WHERE id=$1", current["agent_id"])
    return dict(
        expected_runtime_generation=retirement["generation"],
        expected_retirement_token=retirement["token"],
        expected_agent_id=str(current["agent_id"]),
        expected_attach_token=str(current["runtime_attach_token"]),
        expected_settle_status="ended",
        expected_quiescence_protocol="agent_runtime_zero_v1",
        expected_workspace_generation=None,
        expected_workspace_runtime_incarnation=None,
        expected_agent_pod_uid=agent["pod_uid"],
        require_zero_admission=True,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("history", ["kept", "idle_predecessor", "prior_retirement"])
async def test_exact_source_final_zero_still_requires_initial_history(
    db, monkeypatch, history
):
    current, source = await _failed_source(db, monkeypatch, "bound", 0)
    retirement = await _begin(db, current, permanent=False)
    await _authorize(db, current, retirement)
    assert (
        await VMCreationRetryStore(db).settle_never_issued(
            request_id=str(source["request_id"])
        )
    )["settled"]
    assert await db.fetchval(
        "SELECT public.pinned_vm_creation_agent_zero_source($1,$2,$3)",
        current["id"],
        current["runtime_generation"],
        retirement["token"],
    )
    # Deliberately corrupt otherwise immutable captured history. The SQL
    # publication predicate must independently fence it, even when original
    # source and current actor/G are still exactly equal.
    async with db.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL session_replication_role='replica'")
        if history == "prior_retirement":
            await conn.execute(
                "INSERT INTO thread_runtime_retirement_outcomes "
                "(thread_id,runtime_generation,retirement_token,disposition,permanent,outcome) "
                "VALUES ($1,$2,$3,'ended',false,'settled')",
                current["id"],
                uuid4(),
                uuid4(),
            )
        else:
            context = dict(retirement["context"])
            captured = context["vm_creation_source"]["captured_vm"]
            captured[
                "rootdisk" if history == "kept" else "idle_predecessor_pvc_uid"
            ] = "kept" if history == "kept" else str(uuid4())
            await conn.execute(
                "UPDATE threads SET runtime_retirement_context=$2::jsonb WHERE id=$1",
                current["id"],
                json.dumps(context),
            )
    # Original exact source settlement stays compatible. The new final actor
    # protocol is the strictly initial-only extension under test.
    assert await db.fetchval(
        "SELECT public.thread_vm_creation_never_issued_source($1,$2)",
        current["id"],
        str(source["provision_generation"]),
    )
    assert not await db.fetchval(
        "SELECT public.pinned_vm_creation_agent_zero_source($1,$2,$3)",
        current["id"],
        current["runtime_generation"],
        retirement["token"],
    )


@pytest.mark.asyncio
async def test_source_zero_publication_and_final_end_have_independent_database_fences(
    db, monkeypatch
):
    current, source = await _failed_source(db, monkeypatch)
    retirement = await _begin(db, current, permanent=False)
    await _authorize(db, current, retirement)
    arguments = await _current_zero_arguments(db, current, retirement)
    assert (
        await db.acknowledge_pinned_thread_local_quiescence(
            str(current["id"]), **arguments
        )
        is None
    )
    forged = dict(
        version=1,
        runtime_generation=retirement["generation"],
        retirement_token=retirement["token"],
        agent_id=str(current["agent_id"]),
        runtime_attach_token=str(current["runtime_attach_token"]),
        settle_status="ended",
        quiescence_protocol="agent_runtime_zero_v1",
        quiescence_actor="orchestrator",
        workspace_generation=None,
        workspace_runtime_incarnation=None,
        vm_creation_request_id=str(source["request_id"]),
        vm_creation_provision_generation=str(source["provision_generation"]),
    )
    with pytest.raises(
        asyncpg.CheckViolationError, match="local-quiescence receipt is malformed"
    ):
        await db.execute(
            "UPDATE threads SET runtime_retirement_local_quiescence=$2::jsonb WHERE id=$1",
            current["id"],
            json.dumps(forged),
        )
    assert (
        await VMCreationRetryStore(db).settle_never_issued(
            request_id=str(source["request_id"])
        )
    )["settled"]
    for field in (
        "expected_retirement_token",
        "expected_runtime_generation",
        "expected_agent_id",
        "expected_attach_token",
        "expected_agent_pod_uid",
    ):
        assert (
            await db.acknowledge_pinned_thread_local_quiescence(
                str(current["id"]), **{**arguments, field: str(uuid4())}
            )
            is None
        )
    for field in ("vm_creation_request_id", "vm_creation_provision_generation"):
        with pytest.raises(
            asyncpg.CheckViolationError, match="local-quiescence receipt is malformed"
        ):
            await db.execute(
                "UPDATE threads SET runtime_retirement_local_quiescence=$2::jsonb WHERE id=$1",
                current["id"],
                json.dumps({**forged, field: str(uuid4())}),
            )
    original_context = await db.fetchval(
        "SELECT runtime_retirement_context FROM threads WHERE id=$1", current["id"]
    )
    for field in ("request_id", "provision_generation", "request_digest"):
        damaged = json.loads(original_context)
        damaged["vm_creation_source"][field] = str(uuid4())
        # Adversarial corrupted context: normal writers cannot mutate this
        # immutable capture. The source predicate must independently refuse it.
        async with db.acquire() as conn, conn.transaction():
            await conn.execute("SET LOCAL session_replication_role='replica'")
            await conn.execute(
                "UPDATE threads SET runtime_retirement_context=$2::jsonb WHERE id=$1",
                current["id"],
                json.dumps(damaged),
            )
        assert (
            await db.acknowledge_pinned_thread_local_quiescence(
                str(current["id"]), **arguments
            )
            is None
        )
        async with db.acquire() as conn, conn.transaction():
            await conn.execute("SET LOCAL session_replication_role='replica'")
            await conn.execute(
                "UPDATE threads SET runtime_retirement_context=$2::jsonb WHERE id=$1",
                current["id"],
                original_context,
            )
    assert await db.acknowledge_pinned_thread_local_quiescence(
        str(current["id"]), **arguments
    )
    await _damage_first_edge(db, source, "missing")
    with pytest.raises(asyncpg.CheckViolationError):
        await db.settle_pinned_thread_retirement(
            str(current["id"]),
            token=retirement["token"],
            generation=retirement["generation"],
            final_status="ended",
        )
    still_owned = await db.get_thread(str(current["id"]))
    assert (
        still_owned["status"] == "created"
        and still_owned["agent_id"] == current["agent_id"]
    )


@pytest.mark.asyncio
async def test_owner_recovery_settles_source_then_stops_exact_current_actor_before_soft_end(
    db, monkeypatch
):
    current, source = await _failed_source(db, monkeypatch)
    retirement = await _begin(db, current, permanent=False)
    await _authorize(db, current, retirement)
    agent = await db.fetchrow("SELECT * FROM agents WHERE id=$1", current["agent_id"])
    stops = []

    async def stop(name, *, expected_pod_uid, namespace):
        assert (name, expected_pod_uid, namespace) == (
            agent["hostname"],
            agent["pod_uid"],
            "agents-a",
        )
        assert (
            await db.fetchval(
                "SELECT state FROM vm_creation_retries WHERE request_id=$1",
                source["request_id"],
            )
            == "settled"
        )
        stops.append(expected_pod_uid)
        return len(stops) > 1

    async def authority(name, *, expected_pod_uid, namespace):
        assert (name, expected_pod_uid, namespace) == (
            agent["hostname"],
            agent["pod_uid"],
            "agents-a",
        )
        return "exact_absent"

    operations = PinnedRetirementOperations(
        SimpleNamespace(
            store=db,
            logger=logging.getLogger(__name__),
            agent_provisioner=SimpleNamespace(
                is_available=True,
                delete_agent_pod_exact=stop,
                agent_pod_authority=authority,
            ),
        )
    )
    assert not await operations.recover_captured_process_zero(retirement)
    assert (await db.get_thread(str(current["id"])))[
        "runtime_retirement_local_quiescence"
    ] is None
    assert not await db.settle_pinned_thread_retirement(
        str(current["id"]),
        token=retirement["token"],
        generation=retirement["generation"],
    )
    assert await operations.recover_captured_process_zero(retirement)
    proof = json.loads(
        (await db.get_thread(str(current["id"])))["runtime_retirement_local_quiescence"]
    )
    assert proof["runtime_generation"] == retirement["generation"]
    assert proof["vm_creation_request_id"] == str(source["request_id"])
    assert proof["runtime_attach_token"] == str(current["runtime_attach_token"])
    assert proof["quiescence_actor"] == "orchestrator"
    assert await db.settle_pinned_thread_retirement(
        str(current["id"]),
        token=retirement["token"],
        generation=retirement["generation"],
    )
    assert (await db.get_thread(str(current["id"])))["status"] == "ended"
