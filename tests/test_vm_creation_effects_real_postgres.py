"""Single-flight create effects using actual cleanup, job and claim locks."""

import asyncio
import json
from uuid import UUID, uuid4
import pytest

from orchestrator.services.vm_creation_retry_store import (
    VMCreationRetryStore,
    VMCreationRetryConflict,
)
from shared.vm_creation_issuance import seal_creation_carrier, verify_creation_carrier
from tests.test_vm_creation_retry_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    admitted_job,
    admit,
    observed,
)

db = _db_fixture

SECRET = b"creation-issuance-test-secret-at-least-32-bytes"


async def reserved(
    db,
    monkeypatch,
    *,
    timeout=3600,
    configuration_proven=True,
    persistent_rootdisk=True,
):
    monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", SECRET.decode())
    from types import SimpleNamespace
    from vm_controller.creation_configuration import resolve_creation_configuration
    from vm_controller import controller as controller_settings

    monkeypatch.setattr(
        controller_settings, "VM_PERSISTENT_ROOTDISK", persistent_rootdisk
    )

    configuration = resolve_creation_configuration(
        SimpleNamespace(
            template_text="kind: VirtualMachine",
            cloud_init_text="#cloud-config",
            headscale=SimpleNamespace(is_available=True),
        ),
        {
            "job_id": str(uuid4()),
            "entity_type": "job",
            "provision_generation": str(uuid4()),
        },
    )["controller_configuration"]
    job, generation, proposal = await admitted_job(
        db,
        timeout=timeout,
        controller_configuration=configuration if configuration_proven else None,
    )
    row = await admit(db, job, generation, proposal)
    store = VMCreationRetryStore(db)
    claim = (await store.claim_due(limit=1))[0]
    reservation = await store.authorize_controller(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed=observed(job, generation, proposal),
    )
    values = dict(
        version=1,
        source="controller_vm_create",
        admission_id=str(reservation["admission_id"]),
        reservation_request_id=reservation["request_id"],
        intent_digest=reservation["intent_digest"],
        retry_request_id=str(row["request_id"]),
        job_id=str(job),
        provision_generation=str(generation),
        request_digest=proposal["request_digest"],
        controller_configuration_digest=proposal["controller_configuration_digest"],
        expected_pvc_uid=None,
        retained_dv_uid=None,
        current_dv_uid=None,
        current_pvc_uid=None,
        current_secret_uid=None,
        effect_kind="rootdisk",
        effect_nonce=str(uuid4()),
        object_name=f"agent-vm-{job}-rootdisk",
    )
    carrier = seal_creation_carrier(
        values,
        namespace="agent-vms",
        uid=str(uuid4()),
        resource_version="2",
        secret=SECRET,
    )
    return store, row, claim, carrier


@pytest.mark.asyncio
async def test_two_replicas_receive_only_one_effect_grant(db, monkeypatch):
    store, row, claim, carrier = await reserved(db, monkeypatch)

    async def begin():
        return await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=carrier,
        )

    results = await asyncio.gather(begin(), begin())
    assert sum(result["actuation_allowed"] for result in results) == 1
    assert {result["disposition"] for result in results} == {"issued", "observe_only"}
    winner = next(result for result in results if result["actuation_allowed"])
    loser = next(result for result in results if not result["actuation_allowed"])
    assert len(bytes.fromhex(winner["issuer_receipt"])) == 32
    assert winner["issuer_receipt"] == winner["issuer_receipt"].lower()
    assert "issuer_receipt" not in loser
    inspected = await store.inspect(request_id=str(row["request_id"]))
    assert "issuer_receipt" not in str(inspected)
    assert "issuer_receipt_sha256" not in str(inspected)
    assert winner["issuer_receipt"] not in str(inspected)
    async with db.acquire() as conn:
        stored_hash = await conn.fetchval(
            "SELECT issuer_receipt_sha256 FROM vm_creation_effects WHERE request_id=$1",
            row["request_id"],
        )
    assert len(stored_hash) == 32
    assert stored_hash.hex() not in str(inspected)


@pytest.mark.asyncio
async def test_receipt_surrenders_only_its_issued_effect_and_schedules_failure(db, monkeypatch):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    grant = await store.begin_effect(
        request_id=str(row["request_id"]), claim_token=str(claim["claim_token"]),
        carrier=carrier,
    )
    receipt = grant["issuer_receipt"]
    arguments = dict(
        request_id=str(row["request_id"]), effect_nonce=grant["effect_nonce"],
        carrier=carrier, issuer_receipt=receipt,
        reason="resource_inventory_unavailable",
    )
    for invalid in (None, "a" * 64, receipt.upper(), receipt[:-1]):
        with pytest.raises((ValueError, VMCreationRetryConflict)):
            await store.record_not_attempted(**{**arguments, "issuer_receipt": invalid})
    with pytest.raises((ValueError, VMCreationRetryConflict)):
        await store.record_not_attempted(**{**arguments, "effect_nonce": str(uuid4())})
    with pytest.raises((ValueError, VMCreationRetryConflict)):
        await store.record_not_attempted(**{**arguments, "reason": "unknown"})
    with pytest.raises((ValueError, VMCreationRetryConflict)):
        await store.observe_effect(
            request_id=str(row["request_id"]), carrier=carrier,
            observation={"outcome": "not_attempted", "reason": "resource_inventory_unavailable"},
        )

    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_creation_retries SET backoff_attempt=6,"
            "transport_outage_started_at=clock_timestamp()-interval '90 seconds' "
            "WHERE request_id=$1", row["request_id"],
        )
        before = await conn.fetchrow(
            "SELECT revision,transport_outage_started_at FROM vm_creation_retries "
            "WHERE request_id=$1", row["request_id"],
        )
    assert await store.record_not_attempted(**arguments) == {
        "recorded": True, "effect_state": "rejected",
    }
    async with db.acquire() as conn:
        scheduled = await conn.fetchrow(
            "SELECT state,reason,revision,backoff_attempt,claim_token,claim_expires_at,"
            "transport_outage_started_at,extract(epoch FROM next_probe_at-updated_at) AS delay "
            "FROM vm_creation_retries WHERE request_id=$1", row["request_id"],
        )
        evidence = await conn.fetchval(
            "SELECT evidence FROM vm_creation_effects WHERE effect_nonce=$1",
            UUID(grant["effect_nonce"]),
        )
    assert scheduled["state"] == "queued"
    assert scheduled["revision"] == before["revision"] + 1
    assert scheduled["backoff_attempt"] == 7
    assert scheduled["claim_token"] is None
    assert scheduled["claim_expires_at"] is None
    assert scheduled["transport_outage_started_at"] == before["transport_outage_started_at"]
    assert 300 <= scheduled["delay"] <= 360
    assert json.loads(evidence) == {"outcome": "not_attempted", "reason": "resource_inventory_unavailable"}
    assert await store.claim_due(limit=1) == []
    with pytest.raises(VMCreationRetryConflict):
        await store.begin_effect(
            request_id=str(row["request_id"]), claim_token=str(claim["claim_token"]),
            carrier=carrier,
        )
    assert await store.apply_observation(
        request_id=str(row["request_id"]), claim_token=str(claim["claim_token"]),
        expected_revision=claim["revision"], observation={"outcome": "observation_wait"},
    ) is False


@pytest.mark.asyncio
async def test_duplicate_surrender_after_successor_is_a_pure_read(db, monkeypatch):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    grant = await store.begin_effect(
        request_id=str(row["request_id"]), claim_token=str(claim["claim_token"]),
        carrier=carrier,
    )
    old = dict(
        request_id=str(row["request_id"]), effect_nonce=grant["effect_nonce"],
        carrier=carrier, issuer_receipt=grant["issuer_receipt"],
        reason="resource_inventory_unavailable",
    )
    await store.record_not_attempted(**old)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_creation_retries SET next_probe_at=clock_timestamp()-interval '1 second' "
            "WHERE request_id=$1", row["request_id"],
        )
    successor_claim = (await store.claim_due(limit=1))[0]
    successor = seal_creation_carrier(
        {**verify_creation_carrier(carrier, secret=SECRET), "effect_nonce": str(uuid4())},
        namespace="agent-vms", uid=carrier["metadata"]["uid"],
        resource_version="3", secret=SECRET,
    )
    successor_grant = await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(successor_claim["claim_token"]), carrier=successor,
    )
    assert successor_grant["actuation_allowed"] is True
    async with db.acquire() as conn:
        before = await conn.fetchrow(
            "SELECT state,reason,revision,claim_token,claim_expires_at,backoff_attempt,"
            "next_probe_at,transport_outage_started_at,updated_at FROM vm_creation_retries "
            "WHERE request_id=$1", row["request_id"],
        )
        effects_before = await conn.fetch(
            "SELECT effect_nonce,state,evidence,resolved_at FROM vm_creation_effects "
            "WHERE request_id=$1 ORDER BY effect_number", row["request_id"],
        )
    assert await store.record_not_attempted(**old) == {
        "recorded": True, "effect_state": "rejected",
    }
    async with db.acquire() as conn:
        after = await conn.fetchrow(
            "SELECT state,reason,revision,claim_token,claim_expires_at,backoff_attempt,"
            "next_probe_at,transport_outage_started_at,updated_at FROM vm_creation_retries "
            "WHERE request_id=$1", row["request_id"],
        )
        effects_after = await conn.fetch(
            "SELECT effect_nonce,state,evidence,resolved_at FROM vm_creation_effects "
            "WHERE request_id=$1 ORDER BY effect_number", row["request_id"],
        )
    assert after == before
    assert effects_after == effects_before
    with pytest.raises(VMCreationRetryConflict, match="creation_effect_changed"):
        await store.record_not_attempted(**{**old, "reason": "resource_node_changed"})
    with pytest.raises(VMCreationRetryConflict, match="creation_effect_changed"):
        await store.record_not_attempted(**{
            **old, "effect_nonce": successor_grant["effect_nonce"],
        })


@pytest.mark.asyncio
async def test_nonretryable_refusal_overrides_queued_but_preserves_attention(db, monkeypatch):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    grant = await store.begin_effect(
        request_id=str(row["request_id"]), claim_token=str(claim["claim_token"]),
        carrier=carrier,
    )
    assert await store.apply_observation(
        request_id=str(row["request_id"]), claim_token=str(claim["claim_token"]),
        expected_revision=claim["revision"], observation={"outcome": "observation_wait"},
    )
    arguments = dict(
        request_id=str(row["request_id"]), effect_nonce=grant["effect_nonce"],
        carrier=carrier, issuer_receipt=grant["issuer_receipt"],
        reason="resource_node_changed",
    )
    assert await store.record_not_attempted(**arguments) == {
        "recorded": True, "effect_state": "rejected",
    }
    async with db.acquire() as conn:
        attention = await conn.fetchrow(
            "SELECT state,reason,backoff_attempt,claim_token FROM vm_creation_retries "
            "WHERE request_id=$1", row["request_id"],
        )
    assert attention["state"] == "attention"
    assert attention["reason"] == "vm_creation_retry_blocked"
    assert attention["backoff_attempt"] == 1
    assert attention["claim_token"] is None
    assert await store.claim_due(limit=1) == []


@pytest.mark.asyncio
async def test_late_surrender_does_not_downgrade_existing_attention(db, monkeypatch):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    grant = await store.begin_effect(
        request_id=str(row["request_id"]), claim_token=str(claim["claim_token"]),
        carrier=carrier,
    )
    assert await store.apply_observation(
        request_id=str(row["request_id"]), claim_token=str(claim["claim_token"]),
        expected_revision=claim["revision"], observation={"outcome": "blocked"},
    )
    async with db.acquire() as conn:
        before = await conn.fetchrow(
            "SELECT state,reason,revision,backoff_attempt,next_probe_at "
            "FROM vm_creation_retries WHERE request_id=$1", row["request_id"],
        )
    assert before["state"] == "attention"
    await store.record_not_attempted(
        request_id=str(row["request_id"]), effect_nonce=grant["effect_nonce"],
        carrier=carrier, issuer_receipt=grant["issuer_receipt"],
        reason="resource_inventory_unavailable",
    )
    async with db.acquire() as conn:
        after = await conn.fetchrow(
            "SELECT state,reason,revision,backoff_attempt,next_probe_at,claim_token "
            "FROM vm_creation_retries WHERE request_id=$1", row["request_id"],
        )
    assert after["state"] == before["state"]
    assert after["reason"] == before["reason"]
    assert after["backoff_attempt"] == before["backoff_attempt"]
    assert after["next_probe_at"] == before["next_probe_at"]
    assert after["revision"] == before["revision"] + 1
    assert after["claim_token"] is None


@pytest.mark.asyncio
async def test_two_surrenders_race_to_one_revision(db, monkeypatch):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    grant = await store.begin_effect(
        request_id=str(row["request_id"]), claim_token=str(claim["claim_token"]),
        carrier=carrier,
    )
    arguments = dict(
        request_id=str(row["request_id"]), effect_nonce=grant["effect_nonce"],
        carrier=carrier, issuer_receipt=grant["issuer_receipt"],
        reason="resource_inventory_unavailable",
    )
    async with db.acquire() as conn:
        original_revision = await conn.fetchval(
            "SELECT revision FROM vm_creation_retries WHERE request_id=$1",
            row["request_id"],
        )
    start = asyncio.Event()

    async def surrender():
        await start.wait()
        return await store.record_not_attempted(**arguments)

    first = asyncio.create_task(surrender())
    second = asyncio.create_task(surrender())
    start.set()
    assert await asyncio.wait_for(asyncio.gather(first, second), 5) == [
        {"recorded": True, "effect_state": "rejected"},
        {"recorded": True, "effect_state": "rejected"},
    ]
    async with db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT revision FROM vm_creation_retries WHERE request_id=$1",
            row["request_id"],
        ) == original_revision + 1


@pytest.mark.asyncio
async def test_late_surrender_after_cancel_preserves_cancel_and_replay(db, monkeypatch):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    grant = await store.begin_effect(
        request_id=str(row["request_id"]), claim_token=str(claim["claim_token"]),
        carrier=carrier,
    )
    assert await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
    arguments = dict(
        request_id=str(row["request_id"]), effect_nonce=grant["effect_nonce"],
        carrier=carrier, issuer_receipt=grant["issuer_receipt"],
        reason="resource_node_changed",
    )
    assert await store.record_not_attempted(**arguments) == {
        "recorded": True, "effect_state": "rejected",
    }
    async with db.acquire() as conn:
        before = await conn.fetchrow(
            "SELECT state,reason,revision,claim_token,backoff_attempt,next_probe_at,updated_at "
            "FROM vm_creation_retries WHERE request_id=$1", row["request_id"],
        )
    assert before["state"] == "cancel_requested"
    assert before["claim_token"] is None
    assert await store.record_not_attempted(**arguments) == {
        "recorded": True, "effect_state": "rejected",
    }
    async with db.acquire() as conn:
        after = await conn.fetchrow(
            "SELECT state,reason,revision,claim_token,backoff_attempt,next_probe_at,updated_at "
            "FROM vm_creation_retries WHERE request_id=$1", row["request_id"],
        )
    assert after == before


@pytest.mark.asyncio
async def test_historical_null_receipt_and_sibling_receipt_cannot_surrender(db, monkeypatch):
    store, row, _, carrier = await reserved(db, monkeypatch)
    values = verify_creation_carrier(carrier, secret=SECRET)
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO vm_creation_effects(effect_nonce,request_id,effect_number,"
            "effect_kind,carrier_uid,carrier_namespace,carrier_intent) "
            "VALUES($1,$2,1,$3,$4,$5,$6::jsonb)",
            UUID(values["effect_nonce"]), row["request_id"], values["effect_kind"],
            UUID(carrier["metadata"]["uid"]), carrier["metadata"]["namespace"],
            json.dumps(values),
        )
    with pytest.raises(VMCreationRetryConflict, match="creation_effect_changed"):
        await store.record_not_attempted(
            request_id=str(row["request_id"]), effect_nonce=values["effect_nonce"],
            carrier=carrier, issuer_receipt="b" * 64,
            reason="resource_node_changed",
        )
    async with db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT state FROM vm_creation_effects WHERE effect_nonce=$1",
            UUID(values["effect_nonce"]),
        ) == "issued"

    # A separate issuer's valid receipt has no authority over this effect.
    other_store, other_row, other_claim, other_carrier = await reserved(db, monkeypatch)
    other_grant = await other_store.begin_effect(
        request_id=str(other_row["request_id"]),
        claim_token=str(other_claim["claim_token"]), carrier=other_carrier,
    )
    with pytest.raises(VMCreationRetryConflict, match="creation_effect_changed"):
        await store.record_not_attempted(
            request_id=str(row["request_id"]), effect_nonce=values["effect_nonce"],
            carrier=carrier, issuer_receipt=other_grant["issuer_receipt"],
            reason="resource_node_changed",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["observed", "api_rejected"])
async def test_terminal_public_effect_cannot_be_replaced_by_surrender(
    db, monkeypatch, terminal,
):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    grant = await store.begin_effect(
        request_id=str(row["request_id"]), claim_token=str(claim["claim_token"]),
        carrier=carrier,
    )
    observation = (
        disk_observation(carrier)
        if terminal == "observed"
        else {
            "outcome": "rejected",
            "api_status": {
                "kind": "Status", "apiVersion": "v1", "status": "Failure",
                "reason": "Invalid", "code": 422,
            },
        }
    )
    await store.observe_effect(
        request_id=str(row["request_id"]), carrier=carrier,
        observation=observation,
    )
    with pytest.raises(VMCreationRetryConflict, match="creation_effect_changed"):
        await store.record_not_attempted(
            request_id=str(row["request_id"]), effect_nonce=grant["effect_nonce"],
            carrier=carrier, issuer_receipt=grant["issuer_receipt"],
            reason="resource_node_changed",
        )
    async with db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT state FROM vm_creation_effects WHERE effect_nonce=$1",
            UUID(grant["effect_nonce"]),
        ) == ("observed" if terminal == "observed" else "rejected")


@pytest.mark.asyncio
async def test_surrender_waits_for_owner_lock_before_effect_resolution(db, monkeypatch):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    grant = await store.begin_effect(
        request_id=str(row["request_id"]), claim_token=str(claim["claim_token"]),
        carrier=carrier,
    )
    entered = asyncio.Event()
    finished_locks = asyncio.Event()
    original = store._unused_grant_owner_locks

    async def traced_locks(conn, retry):
        entered.set()
        await original(conn, retry)
        finished_locks.set()

    monkeypatch.setattr(store, "_unused_grant_owner_locks", traced_locks)
    async with db.acquire() as lock_conn:
        async with lock_conn.transaction():
            await lock_conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                f"workspace-recovery:job:{row['job_id']}",
            )
            task = asyncio.create_task(store.record_not_attempted(
                request_id=str(row["request_id"]), effect_nonce=grant["effect_nonce"],
                carrier=carrier, issuer_receipt=grant["issuer_receipt"],
                reason="resource_node_changed",
            ))
            await asyncio.wait_for(entered.wait(), 5)
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(finished_locks.wait(), 0.1)
            assert not task.done()
    assert await asyncio.wait_for(task, 5) == {
        "recorded": True, "effect_state": "rejected",
    }


@pytest.mark.asyncio
async def test_new_effect_issuance_resets_capped_wait_to_first_probe(db, monkeypatch):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_creation_retries SET backoff_attempt=6,"
            "next_probe_at=clock_timestamp()+interval '1 hour' WHERE request_id=$1",
            row["request_id"],
        )

    assert (await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]), carrier=carrier,
    ))["actuation_allowed"] is True
    async with db.acquire() as conn:
        progress = await conn.fetchrow(
            "SELECT backoff_attempt,extract(epoch FROM next_probe_at-updated_at) "
            "AS delay FROM vm_creation_retries WHERE request_id=$1", row["request_id"],
        )
    assert progress["backoff_attempt"] == 0
    assert 0 < progress["delay"] <= 5

    assert await store.apply_observation(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]), expected_revision=claim["revision"],
        observation={"outcome": "observation_wait"},
    )
    async with db.acquire() as conn:
        scheduled = await conn.fetchrow(
            "SELECT state,backoff_attempt,"
            "extract(epoch FROM next_probe_at-updated_at) AS delay "
            "FROM vm_creation_retries WHERE request_id=$1", row["request_id"],
        )
    assert scheduled["state"] == "queued"
    assert scheduled["backoff_attempt"] == 1
    assert 5 <= scheduled["delay"] <= 6


@pytest.mark.asyncio
async def test_observed_effect_resets_capped_wait_to_first_probe(db, monkeypatch):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]), carrier=carrier,
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_creation_retries SET backoff_attempt=6,"
            "next_probe_at=clock_timestamp()+interval '1 hour' WHERE request_id=$1",
            row["request_id"],
        )
    assert await store.observe_effect(
        request_id=str(row["request_id"]), carrier=carrier,
        observation=disk_observation(carrier),
    ) == {"recorded": True, "effect_state": "observed"}
    async with db.acquire() as conn:
        progress = await conn.fetchrow(
            "SELECT backoff_attempt,extract(epoch FROM next_probe_at-updated_at) "
            "AS delay FROM vm_creation_retries WHERE request_id=$1", row["request_id"],
        )
    assert progress["backoff_attempt"] == 0
    assert 0 < progress["delay"] <= 5


@pytest.mark.asyncio
async def test_duplicate_effect_calls_do_not_reset_backoff(db, monkeypatch):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]), carrier=carrier,
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_creation_retries SET backoff_attempt=6,"
            "next_probe_at=clock_timestamp()+interval '1 hour' WHERE request_id=$1",
            row["request_id"],
        )
    assert (await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]), carrier=carrier,
    ))["actuation_allowed"] is False
    async with db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT backoff_attempt FROM vm_creation_retries WHERE request_id=$1",
            row["request_id"],
        ) == 6

    observation = disk_observation(carrier)
    await store.observe_effect(
        request_id=str(row["request_id"]), carrier=carrier,
        observation=observation,
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_creation_retries SET backoff_attempt=6,"
            "next_probe_at=clock_timestamp()+interval '1 hour' WHERE request_id=$1",
            row["request_id"],
        )
    assert await store.observe_effect(
        request_id=str(row["request_id"]), carrier=carrier,
        observation=observation,
    ) == {"recorded": True, "effect_state": "observed"}
    async with db.acquire() as conn:
        unchanged = await conn.fetchrow(
            "SELECT backoff_attempt,extract(epoch FROM next_probe_at-clock_timestamp()) "
            "AS due_in FROM vm_creation_retries WHERE request_id=$1", row["request_id"],
        )
    assert unchanged["backoff_attempt"] == 6
    assert unchanged["due_in"] > 3000


@pytest.mark.asyncio
async def test_no_progress_observation_keeps_capped_wait(db, monkeypatch):
    store, row, claim, _ = await reserved(db, monkeypatch)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_creation_retries SET backoff_attempt=6 WHERE request_id=$1",
            row["request_id"],
        )
    assert await store.apply_observation(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]), expected_revision=claim["revision"],
        observation={"outcome": "observation_wait"},
    )
    async with db.acquire() as conn:
        scheduled = await conn.fetchrow(
            "SELECT state,backoff_attempt,"
            "extract(epoch FROM next_probe_at-updated_at) AS delay "
            "FROM vm_creation_retries WHERE request_id=$1", row["request_id"],
        )
    assert scheduled["state"] == "queued"
    assert scheduled["backoff_attempt"] == 7
    assert 300 <= scheduled["delay"] <= 360


@pytest.mark.asyncio
async def test_rejected_effect_does_not_reset_capped_retry(db, monkeypatch):
    from shared.vm_creation_issuance import verify_creation_carrier

    store, row, claim, carrier = await reserved(db, monkeypatch)
    await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]), carrier=carrier,
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_creation_retries SET backoff_attempt=6,"
            "next_probe_at=clock_timestamp()+interval '1 hour' WHERE request_id=$1",
            row["request_id"],
        )
    assert await store.observe_effect(
        request_id=str(row["request_id"]), carrier=carrier,
        observation={
            "outcome": "rejected",
            "api_status": {
                "kind": "Status", "apiVersion": "v1", "status": "Failure",
                "reason": "Invalid", "code": 422,
            },
        },
    ) == {"recorded": True, "effect_state": "rejected"}
    async with db.acquire() as conn:
        unchanged = await conn.fetchrow(
            "SELECT backoff_attempt,extract(epoch FROM next_probe_at-clock_timestamp()) "
            "AS due_in FROM vm_creation_retries WHERE request_id=$1", row["request_id"],
        )
    assert unchanged["backoff_attempt"] == 6
    assert unchanged["due_in"] > 3000
    next_values = {
        **verify_creation_carrier(carrier, secret=SECRET),
        "effect_nonce": str(uuid4()),
    }
    next_carrier = seal_creation_carrier(
        next_values,
        namespace="agent-vms",
        uid=carrier["metadata"]["uid"],
        resource_version="3",
        secret=SECRET,
    )
    assert (await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]), carrier=next_carrier,
    ))["actuation_allowed"] is True
    async with db.acquire() as conn:
        retried = await conn.fetchrow(
            "SELECT backoff_attempt,extract(epoch FROM next_probe_at-clock_timestamp()) "
            "AS due_in FROM vm_creation_retries WHERE request_id=$1", row["request_id"],
        )
    assert retried["backoff_attempt"] == 6
    assert retried["due_in"] > 3000
    assert await store.apply_observation(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]), expected_revision=claim["revision"],
        observation={"outcome": "observation_wait"},
    )
    async with db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT backoff_attempt FROM vm_creation_retries WHERE request_id=$1",
            row["request_id"],
        ) == 7


@pytest.mark.asyncio
async def test_late_progress_cannot_replace_claim_or_clear_transport_outage(
    db, monkeypatch
):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_creation_retries SET backoff_attempt=6 WHERE request_id=$1",
            row["request_id"],
        )
    assert await store.apply_observation(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]), expected_revision=claim["revision"],
        observation={"outcome": "transport_unknown"},
    )
    assert (await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]), carrier=carrier,
    ))["actuation_allowed"] is True
    async with db.acquire() as conn:
        held = await conn.fetchrow(
            "SELECT revision,backoff_attempt,claim_token,transport_outage_started_at "
            "FROM vm_creation_retries WHERE request_id=$1", row["request_id"],
        )
        await conn.execute(
            "UPDATE vm_creation_retries SET next_probe_at=clock_timestamp()-interval '1 second' "
            "WHERE request_id=$1", row["request_id"],
        )
    assert held["backoff_attempt"] == 0
    assert held["claim_token"] == claim["claim_token"]
    assert held["transport_outage_started_at"] is not None
    assert await store.claim_due(limit=1) == []

    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_creation_retries SET transport_outage_started_at="
            "clock_timestamp()-interval '901 seconds' WHERE request_id=$1",
            row["request_id"],
        )
    assert await store.apply_observation(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]), expected_revision=held["revision"],
        observation={"outcome": "transport_unknown"},
    )
    async with db.acquire() as conn:
        attention = await conn.fetchrow(
            "SELECT state,claim_token FROM vm_creation_retries WHERE request_id=$1",
            row["request_id"],
        )
    assert attention["state"] == "attention"
    assert attention["claim_token"] is None
    assert await store.claim_due(limit=1) == []


@pytest.mark.asyncio
async def test_observed_progress_after_cancel_cannot_reopen_creation(db, monkeypatch):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]), carrier=carrier,
    )
    await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_creation_retries SET backoff_attempt=6,"
            "next_probe_at=clock_timestamp()+interval '1 hour' WHERE request_id=$1",
            row["request_id"],
        )
    assert await store.observe_effect(
        request_id=str(row["request_id"]), carrier=carrier,
        observation=disk_observation(carrier),
    ) == {"recorded": True, "effect_state": "observed"}
    async with db.acquire() as conn:
        cancelled = await conn.fetchrow(
            "SELECT state,claim_token,backoff_attempt FROM vm_creation_retries "
            "WHERE request_id=$1", row["request_id"],
        )
        await conn.execute(
            "UPDATE vm_creation_retries SET next_probe_at=clock_timestamp()-interval '1 second' "
            "WHERE request_id=$1", row["request_id"],
        )
    assert cancelled["state"] == "cancel_requested"
    assert cancelled["claim_token"] is None
    assert cancelled["backoff_attempt"] == 0
    cancellation_claim = (await store.claim_due(limit=1))[0]
    assert cancellation_claim["state"] == "cancel_requested"
    with pytest.raises(VMCreationRetryConflict):
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(cancellation_claim["claim_token"]), carrier=carrier,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_transport_timeout_keeps_only_original_effect_authority(db, monkeypatch, cancel):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_creation_retries SET claim_expires_at=clock_timestamp()+interval '30 seconds' WHERE request_id=$1",
            row["request_id"],
        )
    assert await store.apply_observation(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]),
        expected_revision=claim["revision"],
        observation={"outcome": "transport_unknown"},
    )
    assert await store.claim_due(limit=1) == []
    if cancel:
        await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
        with pytest.raises(VMCreationRetryConflict):
            await store.begin_effect(
                request_id=str(row["request_id"]),
                claim_token=str(claim["claim_token"]), carrier=carrier,
            )
        assert await db.fetchval(
            "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1", row["request_id"],
        ) == 0
    else:
        grant = await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]), carrier=carrier,
        )
        assert grant["actuation_allowed"] is True
        replay = await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]), carrier=carrier,
        )
        assert replay["actuation_allowed"] is False


@pytest.mark.asyncio
async def test_unknown_issuance_survives_claim_expiry_and_cancellation(db, monkeypatch):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]),
        carrier=carrier,
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_creation_retries SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE request_id=$1",
            row["request_id"],
        )
    new_claim = (await store.claim_due(limit=1))[0]
    replay = await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(new_claim["claim_token"]),
        carrier=carrier,
    )
    assert replay["actuation_allowed"] is False
    await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
    result = await store.settle_never_issued(request_id=str(row["request_id"]))
    assert result == {"settled": False, "reason": "creation_effect_unresolved"}
    async with db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT completed_at IS NULL FROM vm_workspace_cleanup_admissions WHERE id=$1",
            row["creation_admission_id"] or UUID(carrier["spec"]["holderIdentity"]),
        )


@pytest.mark.asyncio
async def test_cancel_before_effect_grant_can_settle_never_issued(db, monkeypatch):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
    with pytest.raises(VMCreationRetryConflict, match="job_cancelled"):
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=carrier,
        )
    assert await store.settle_never_issued(request_id=str(row["request_id"])) == {
        "settled": True,
        "disposition": "never_issued",
    }
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT state FROM vm_creation_retries WHERE request_id=$1",
                row["request_id"],
            )
            == "settled"
        )


@pytest.mark.asyncio
async def test_carrier_boolean_or_foreign_source_does_not_grant(db, monkeypatch):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    with pytest.raises(ValueError):
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier={"authenticated": True},
        )
    carrier["metadata"]["namespace"] = "foreign"
    with pytest.raises(ValueError):
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=carrier,
        )


@pytest.mark.asyncio
async def test_generic_cleanup_completion_cannot_release_create_authority(
    db, monkeypatch
):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    from uuid import UUID

    assert (
        await store.cleanup.complete_cleanup_permit(
            UUID(carrier["spec"]["holderIdentity"]), outcome="adopted"
        )
        is False
    )


@pytest.mark.asyncio
async def test_definitive_api_rejection_allows_new_nonce_but_unknown_does_not(
    db, monkeypatch
):
    from shared.vm_creation_issuance import verify_creation_carrier

    store, row, claim, carrier = await reserved(db, monkeypatch)
    await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]),
        carrier=carrier,
    )
    new_values = {
        **verify_creation_carrier(carrier, secret=SECRET),
        "effect_nonce": str(uuid4()),
    }
    next_carrier = seal_creation_carrier(
        new_values,
        namespace="agent-vms",
        uid=carrier["metadata"]["uid"],
        resource_version="3",
        secret=SECRET,
    )
    with pytest.raises(VMCreationRetryConflict, match="creation_effect_unresolved"):
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=next_carrier,
        )
    for observation in [
        {"outcome": "not_issued", "authenticated": True},
        {
            "outcome": "rejected",
            "api_status": {
                "kind": "Status",
                "apiVersion": "v1",
                "status": "Failure",
                "reason": "InternalError",
                "code": 500,
            },
        },
    ]:
        with pytest.raises(ValueError):
            await store.observe_effect(
                request_id=str(row["request_id"]),
                carrier=carrier,
                observation=observation,
            )
    assert await store.observe_effect(
        request_id=str(row["request_id"]),
        carrier=carrier,
        observation={
            "outcome": "rejected",
            "api_status": {
                "kind": "Status",
                "apiVersion": "v1",
                "status": "Failure",
                "reason": "Invalid",
                "code": 422,
            },
        },
    ) == {"recorded": True, "effect_state": "rejected"}
    assert (
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=next_carrier,
        )
    )["actuation_allowed"] is True


def disk_observation(carrier):
    from shared.vm_creation_issuance import (
        verify_creation_carrier,
        EFFECT_NONCE_ANNOTATION,
        REQUEST_ANNOTATION,
    )

    values = verify_creation_carrier(carrier, secret=SECRET)
    dv, pvc = str(uuid4()), str(uuid4())
    metadata = {
        "name": values["object_name"],
        "namespace": "agent-vms",
        "uid": dv,
        "labels": {"srw.io/owner-kind": "job", "srw.io/owner-id": values["job_id"]},
        "annotations": {
            EFFECT_NONCE_ANNOTATION: values["effect_nonce"],
            REQUEST_ANNOTATION: values["retry_request_id"],
            "srw.io/provision-generation": values["provision_generation"],
        },
    }
    return {
        "outcome": "observed",
        "object": {
            "apiVersion": "cdi.kubevirt.io/v1beta1",
            "kind": "DataVolume",
            "metadata": metadata,
        },
        "pvc": {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {
                "name": values["object_name"],
                "namespace": "agent-vms",
                "uid": pvc,
                "ownerReferences": [{"kind": "DataVolume", "uid": dv}],
            },
        },
    }


@pytest.mark.asyncio
async def test_disk_observation_after_cancel_binds_identity_without_new_authority(
    db, monkeypatch
):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]),
        carrier=carrier,
    )
    await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
    observation = disk_observation(carrier)
    assert await store.observe_effect(
        request_id=str(row["request_id"]), carrier=carrier, observation=observation
    ) == {"recorded": True, "effect_state": "observed"}
    assert await store.observe_effect(
        request_id=str(row["request_id"]), carrier=carrier, observation=observation
    ) == {"recorded": True, "effect_state": "observed"}
    changed = disk_observation(carrier)
    with pytest.raises(VMCreationRetryConflict):
        await store.observe_effect(
            request_id=str(row["request_id"]), carrier=carrier, observation=changed
        )
    assert (await store.settle_never_issued(request_id=str(row["request_id"])))[
        "settled"
    ] is False
    async with db.acquire() as conn:
        value = await conn.fetchrow(
            "SELECT state,observed_pvc_uid,boot_counted FROM vm_creation_retries WHERE request_id=$1",
            row["request_id"],
        )
        assert value["state"] == "cancel_requested"
        assert str(value["observed_pvc_uid"]) == observation["pvc"]["metadata"]["uid"]
        assert value["boot_counted"] is False


@pytest.mark.asyncio
async def test_next_stage_revalidates_exact_new_disk_and_completed_vm_never_recreates(
    db, monkeypatch
):
    from shared.vm_creation_issuance import (
        verify_creation_carrier,
        EFFECT_NONCE_ANNOTATION,
        REQUEST_ANNOTATION,
    )

    store, row, claim, carrier = await reserved(db, monkeypatch)
    await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]),
        carrier=carrier,
    )
    disk = disk_observation(carrier)
    await store.observe_effect(
        request_id=str(row["request_id"]), carrier=carrier, observation=disk
    )
    original = verify_creation_carrier(carrier, secret=SECRET)
    reservation = await store.authorize_controller(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed={
            "job_id": str(row["job_id"]),
            "provision_generation": str(row["provision_generation"]),
            "request_digest": row["request_digest"],
            "controller_configuration_digest": row["controller_configuration_digest"],
            "expected_pvc_uid": None,
        },
    )
    assert reservation["allowed"] is True
    assert str(reservation["admission_id"]) == original["admission_id"]
    secret_uid = None
    for kind, suffix in [("cloud_init", "-cloudinit"), ("vm", "")]:
        values = {
            **original,
            "effect_kind": kind,
            "effect_nonce": str(uuid4()),
            "object_name": f"agent-vm-{row['job_id']}{suffix}",
            "current_dv_uid": disk["object"]["metadata"]["uid"],
            "current_pvc_uid": disk["pvc"]["metadata"]["uid"],
            "current_secret_uid": secret_uid,
        }
        next_carrier = seal_creation_carrier(
            values,
            namespace="agent-vms",
            uid=carrier["metadata"]["uid"],
            resource_version="3",
            secret=SECRET,
        )
        wrong = {**values, "current_pvc_uid": str(uuid4())}
        wrong_carrier = seal_creation_carrier(
            wrong,
            namespace="agent-vms",
            uid=carrier["metadata"]["uid"],
            resource_version="4",
            secret=SECRET,
        )
        with pytest.raises(VMCreationRetryConflict, match="retained_disk_changed"):
            await store.begin_effect(
                request_id=str(row["request_id"]),
                claim_token=str(claim["claim_token"]),
                carrier=wrong_carrier,
            )
        assert (
            await store.begin_effect(
                request_id=str(row["request_id"]),
                claim_token=str(claim["claim_token"]),
                carrier=next_carrier,
            )
        )["actuation_allowed"]
        obj = {
            "apiVersion": "v1" if kind == "cloud_init" else "kubevirt.io/v1",
            "kind": "Secret" if kind == "cloud_init" else "VirtualMachine",
            "metadata": {
                "name": values["object_name"],
                "namespace": "agent-vms",
                "uid": str(uuid4()),
                "labels": {
                    "srw.io/owner-kind": "job",
                    "srw.io/owner-id": str(row["job_id"]),
                },
                "annotations": {
                    EFFECT_NONCE_ANNOTATION: values["effect_nonce"],
                    REQUEST_ANNOTATION: str(row["request_id"]),
                    "srw.io/provision-generation": str(row["provision_generation"]),
                    "srw.io/ssh-host-key-fingerprint": "SHA256:" + "A" * 43,
                },
            },
            "spec": {
                "template": {
                    "spec": {
                        "volumes": [
                            {
                                "name": "rootdisk",
                                "dataVolume": {
                                    "name": disk["object"]["metadata"]["name"]
                                },
                            }
                        ]
                    }
                }
            },
            "data": {"userdata": "must-not-persist-secret"},
        }
        if kind == "vm":
            from copy import deepcopy

            wrong_vm = deepcopy(obj)
            wrong_vm["spec"]["template"]["spec"]["volumes"].append(
                {
                    "name": "cloud-init",
                    "cloudInitNoCloud": {"secretRef": {"name": "foreign-secret"}},
                }
            )
            with pytest.raises(ValueError, match="cloud-init"):
                await store.observe_effect(
                    request_id=str(row["request_id"]),
                    carrier=next_carrier,
                    observation={"outcome": "observed", "object": wrong_vm},
                )
            obj["spec"]["template"]["spec"]["volumes"].append(
                {
                    "name": "cloud-init",
                    "cloudInitNoCloud": {
                        "secretRef": {"name": f"agent-vm-{row['job_id']}-cloudinit"}
                    },
                }
            )
        assert (
            await store.observe_effect(
                request_id=str(row["request_id"]),
                carrier=next_carrier,
                observation={"outcome": "observed", "object": obj},
            )
        )["recorded"]
        if kind == "cloud_init":
            secret_uid = obj["metadata"]["uid"]
        carrier = next_carrier
    replay = {**values, "effect_nonce": str(uuid4())}
    with pytest.raises(VMCreationRetryConflict, match="creation_already_admitted"):
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=seal_creation_carrier(
                replay,
                namespace="agent-vms",
                uid=carrier["metadata"]["uid"],
                resource_version="9",
                secret=SECRET,
            ),
        )
    async with db.acquire() as conn:
        effects = await conn.fetch(
            "SELECT evidence::text FROM vm_creation_effects WHERE request_id=$1",
            row["request_id"],
        )
        assert all(
            "must-not-persist-secret" not in effect["evidence"] for effect in effects
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("expires", ["claim", "deadline"])
async def test_begin_effect_expiry_after_lock_wait_creates_no_grant(
    db, monkeypatch, expires
):
    store, row, claim, carrier = await reserved(
        db, monkeypatch, timeout=2 if expires == "deadline" else 3600
    )
    async with db.acquire() as conn:
        if expires == "claim":
            await conn.execute(
                "UPDATE vm_creation_retries SET claim_expires_at=clock_timestamp()+interval '1 second' WHERE request_id=$1",
                row["request_id"],
            )
        async with conn.transaction():
            await conn.execute(
                "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", row["job_id"]
            )
            begin = asyncio.create_task(
                store.begin_effect(
                    request_id=str(row["request_id"]),
                    claim_token=str(claim["claim_token"]),
                    carrier=carrier,
                )
            )
            await asyncio.sleep(2.2 if expires == "deadline" else 1.3)
            assert not begin.done()
        with pytest.raises(
            VMCreationRetryConflict,
            match="job_admission_expired"
            if expires == "deadline"
            else "retry_claim_changed",
        ):
            await asyncio.wait_for(begin, timeout=5)
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
                row["request_id"],
            )
            == 0
        )


@pytest.mark.asyncio
async def test_carrier_and_admission_agreement_does_not_replace_full_intent_binding(
    db, monkeypatch
):
    from shared.vm_creation_issuance import verify_creation_carrier

    store, row, claim, carrier = await reserved(db, monkeypatch)
    digest = "sha256:" + "d" * 64
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_workspace_cleanup_admissions SET intent_digest=$2 WHERE id=$1",
            UUID(carrier["spec"]["holderIdentity"]),
            digest,
        )
    values = {
        **verify_creation_carrier(carrier, secret=SECRET),
        "intent_digest": digest,
    }
    changed = seal_creation_carrier(
        values,
        namespace="agent-vms",
        uid=carrier["metadata"]["uid"],
        resource_version="5",
        secret=SECRET,
    )
    with pytest.raises(VMCreationRetryConflict, match="creation_reservation_changed"):
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=changed,
        )


@pytest.mark.asyncio
async def test_cancel_racing_single_effect_grant_never_loses_issuance(db, monkeypatch):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    effect, cancelled = await asyncio.wait_for(
        asyncio.gather(
            store.begin_effect(
                request_id=str(row["request_id"]),
                claim_token=str(claim["claim_token"]),
                carrier=carrier,
            ),
            db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused"),
            return_exceptions=True,
        ),
        timeout=5,
    )
    assert cancelled is True
    result = await store.settle_never_issued(request_id=str(row["request_id"]))
    if isinstance(effect, VMCreationRetryConflict):
        assert result["settled"] is True
    else:
        assert effect["actuation_allowed"] is True
        assert result["settled"] is False


@pytest.mark.asyncio
async def test_completed_reservation_without_vm_never_grants_create(db, monkeypatch):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),outcome='adopted' WHERE id=$1",
            UUID(carrier["spec"]["holderIdentity"]),
        )
    with pytest.raises(VMCreationRetryConflict, match="creation_reservation_changed"):
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=carrier,
        )


@pytest.mark.asyncio
async def test_sealed_foreign_namespace_is_not_authority_for_frozen_configuration(
    db, monkeypatch
):
    from shared.vm_creation_issuance import verify_creation_carrier

    store, row, claim, carrier = await reserved(db, monkeypatch)
    foreign = seal_creation_carrier(
        verify_creation_carrier(carrier, secret=SECRET),
        namespace="foreign",
        uid=carrier["metadata"]["uid"],
        resource_version="8",
        secret=SECRET,
    )
    with pytest.raises(VMCreationRetryConflict, match="creation_configuration_changed"):
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=foreign,
        )


@pytest.mark.asyncio
async def test_digest_only_record_cannot_receive_effect_grant(db, monkeypatch):
    store, row, claim, carrier = await reserved(
        db, monkeypatch, configuration_proven=False
    )
    with pytest.raises(
        VMCreationRetryConflict, match="creation_configuration_unproven"
    ):
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=carrier,
        )
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
                row["request_id"],
            )
            == 0
        )


@pytest.mark.asyncio
async def test_database_rejects_carrier_effect_and_configuration_identity_mutation(
    db, monkeypatch
):
    import asyncpg
    from shared.vm_creation_issuance import verify_creation_carrier

    store, row, claim, carrier = await reserved(db, monkeypatch)
    await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]),
        carrier=carrier,
    )
    nonce = UUID(verify_creation_carrier(carrier, secret=SECRET)["effect_nonce"])
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE vm_creation_effects SET effect_nonce=$2 WHERE effect_nonce=$1",
                nonce,
                uuid4(),
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE vm_creation_retries SET controller_configuration=NULL WHERE request_id=$1",
                row["request_id"],
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE vm_creation_retries SET creation_carrier_uid=$2 WHERE request_id=$1",
                row["request_id"],
                uuid4(),
            )


@pytest.mark.asyncio
async def test_nonpersistent_controller_configuration_cannot_grant_staged_creation(
    db, monkeypatch
):
    store, row, claim, carrier = await reserved(
        db, monkeypatch, persistent_rootdisk=False
    )
    with pytest.raises(VMCreationRetryConflict, match="retry_protocol_unavailable"):
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=carrier,
        )


async def observed_creation(db, monkeypatch, *, timeout=3600, stop_after="vm"):
    from shared.vm_creation_issuance import (
        verify_creation_carrier,
        EFFECT_NONCE_ANNOTATION,
        REQUEST_ANNOTATION,
    )

    store, row, claim, carrier = await reserved(db, monkeypatch, timeout=timeout)
    observations = {}
    original = verify_creation_carrier(carrier, secret=SECRET)
    for kind in ("rootdisk", "cloud_init", "vm"):
        if kind != "rootdisk":
            values = {
                **original,
                "effect_kind": kind,
                "effect_nonce": str(uuid4()),
                "object_name": f"agent-vm-{row['job_id']}"
                + ("-cloudinit" if kind == "cloud_init" else ""),
                "current_dv_uid": observations["rootdisk"]["object"]["metadata"]["uid"],
                "current_pvc_uid": observations["rootdisk"]["pvc"]["metadata"]["uid"],
                "current_secret_uid": observations["cloud_init"]["object"]["metadata"][
                    "uid"
                ]
                if kind == "vm"
                else None,
            }
            carrier = seal_creation_carrier(
                values,
                namespace="agent-vms",
                uid=carrier["metadata"]["uid"],
                resource_version="3",
                secret=SECRET,
            )
        else:
            values = original
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=carrier,
        )
        if kind == "rootdisk":
            observation = disk_observation(carrier)
        else:
            observation = {
                "outcome": "observed",
                "object": {
                    "apiVersion": "v1" if kind == "cloud_init" else "kubevirt.io/v1",
                    "kind": "Secret" if kind == "cloud_init" else "VirtualMachine",
                    "metadata": {
                        "uid": str(uuid4()),
                        "name": values["object_name"],
                        "namespace": "agent-vms",
                        "labels": {
                            "srw.io/owner-kind": "job",
                            "srw.io/owner-id": str(row["job_id"]),
                        },
                        "annotations": {
                            EFFECT_NONCE_ANNOTATION: values["effect_nonce"],
                            REQUEST_ANNOTATION: str(row["request_id"]),
                            "srw.io/provision-generation": str(
                                row["provision_generation"]
                            ),
                            "srw.io/ssh-host-key-fingerprint": "SHA256:" + "A" * 43,
                        },
                    },
                    "spec": {
                        "template": {
                            "spec": {
                                "volumes": [
                                    {
                                        "name": "rootdisk",
                                        "dataVolume": {"name": original["object_name"]},
                                    },
                                    {
                                        "name": "cloud-init",
                                        "cloudInitNoCloud": {
                                            "secretRef": {
                                                "name": f"agent-vm-{row['job_id']}-cloudinit"
                                            }
                                        },
                                    },
                                ]
                            }
                        }
                    },
                },
            }
        await store.observe_effect(
            request_id=str(row["request_id"]), carrier=carrier, observation=observation
        )
        observations[kind] = observation
        if kind == stop_after:
            break
    return store, row, carrier, observations


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [False, True])
async def test_exact_adoption_settles_same_permit_once_and_preserves_worker_hold(
    db, monkeypatch, cancelled
):
    import json

    store, row, carrier, observations = await observed_creation(db, monkeypatch)
    if cancelled:
        async with db.acquire() as conn:
            await conn.execute(
                "UPDATE jobs SET status='cancelled' WHERE id=$1", row["job_id"]
            )
    result = await store.settle_adopted(
        request_id=str(row["request_id"]), carrier=carrier, observations=observations
    )
    assert result["settled"] is True
    assert (
        await store.settle_adopted(
            request_id=str(row["request_id"]),
            carrier=carrier,
            observations=observations,
        )
        == result
    )
    async with db.acquire() as conn:
        retry = await conn.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1", row["request_id"]
        )
        job = await conn.fetchrow("SELECT * FROM jobs WHERE id=$1", row["job_id"])
        permit = await conn.fetchrow(
            "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1",
            retry["creation_admission_id"],
        )
    context = json.loads(job["context"])
    assert retry["boot_counted"] is True
    assert retry["ready_at"] is None
    assert retry["state"] == ("settled" if cancelled else "succeeded")
    assert permit["outcome"] == "adopted" and permit["completed_at"] is not None
    assert context["_vm_creation_pending"] == str(row["request_id"])
    assert context["vm"]["vm_uid"] == observations["vm"]["object"]["metadata"]["uid"]
    assert context["vm"]["provision_attempts"] == 1
    if cancelled:
        assert job["status"] == "cancelled"


@pytest.mark.asyncio
async def test_adoption_refuses_replaced_secret_or_disk_without_releasing_authority(
    db, monkeypatch
):
    from copy import deepcopy

    store, row, carrier, observations = await observed_creation(db, monkeypatch)
    for kind in ("rootdisk", "cloud_init", "vm"):
        changed = deepcopy(observations)
        changed[kind]["object"]["metadata"]["uid"] = str(uuid4())
        with pytest.raises((ValueError, VMCreationRetryConflict)):
            await store.settle_adopted(
                request_id=str(row["request_id"]), carrier=carrier, observations=changed
            )
    async with db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT completed_at IS NULL FROM vm_workspace_cleanup_admissions WHERE id=(SELECT creation_admission_id FROM vm_creation_retries WHERE request_id=$1)",
            row["request_id"],
        )


@pytest.mark.asyncio
async def test_source_inspection_is_public_observation_only(db, monkeypatch):
    store, row, carrier, observations = await observed_creation(db, monkeypatch)
    result = await store.inspect(request_id=str(row["request_id"]))
    assert result["request"] == row["canonical_request"]
    assert len(result["effects"]) == 3
    assert result["creation_carrier_uid"] == carrier["metadata"]["uid"]
    assert "claim_token" not in result
    assert "actuation_allowed" not in result


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["queued", "attention"])
async def test_late_adoption_from_wait_needs_no_live_claim(db, monkeypatch, state):
    store, row, carrier, observations = await observed_creation(db, monkeypatch)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_creation_retries SET state=$2,claim_token=NULL,claim_expires_at=NULL WHERE request_id=$1",
            row["request_id"],
            state,
        )
    assert (
        await store.settle_adopted(
            request_id=str(row["request_id"]),
            carrier=carrier,
            observations=observations,
        )
    )["settled"]


@pytest.mark.asyncio
async def test_accepted_vm_can_be_adopted_after_immutable_deadline(db, monkeypatch):
    store, row, carrier, observations = await observed_creation(
        db, monkeypatch, timeout=2
    )
    async with db.acquire() as conn:
        await conn.execute(
            "SELECT pg_sleep(GREATEST(0,extract(epoch FROM (admission_deadline-clock_timestamp())))+0.05) FROM vm_creation_retries WHERE request_id=$1",
            row["request_id"],
        )
    assert (
        await store.settle_adopted(
            request_id=str(row["request_id"]),
            carrier=carrier,
            observations=observations,
        )
    )["settled"]
    async with db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT ready_at IS NULL AND admission_deadline<clock_timestamp() FROM vm_creation_retries WHERE request_id=$1",
            row["request_id"],
        )
