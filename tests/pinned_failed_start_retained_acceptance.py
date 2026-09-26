"""Explicit acceptance run: preserves the real SQL ten-minute create horizon.

Run this file explicitly with pytest. It is intentionally outside default test
discovery because it waits for the production horizon, without backdating rows,
disabling triggers, substituting a clock, or fabricating lifecycle receipts.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest

from orchestrator.services import container_provisioner as provider_module
from orchestrator.services.session_provisioner import ensure_session_workspace
from orchestrator.services.workspace_lifecycle import EnsureOutcome
from tests import test_pinned_failed_start_end_real_postgres as cases

db = cases.db
pg_dsn = cases.pg_dsn
_schema_applied = cases._schema_applied


async def _race_end_and_admission(db, thread_id, successor, *, end_first):
    """Queue both production transitions behind the same real PostgreSQL row lock."""

    async def wait_blocked(conn, count, contenders):
        for _ in range(250):
            for contender in contenders:
                if contender.done():
                    contender.result()  # Surface an early refusal/exception.
                    raise AssertionError(
                        "lifecycle contender did not wait for its lock"
                    )
            await conn.execute("SELECT pg_stat_clear_snapshot()")
            blocked = await conn.fetchval(
                "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                "AND pid<>pg_backend_pid() AND wait_event_type='Lock'"
            )
            if blocked >= count:
                return
            await asyncio.sleep(0.02)
        raise AssertionError("lifecycle contenders did not reach the row lock")

    async def admit():
        return await db.admit_retained_pinned_workspace_creation_effects(
            thread_id,
            runtime_generation=str(successor["runtime_generation"]),
            attempt_id=str(successor["attempt_id"]),
        )

    async def end():
        return await db.begin_pinned_thread_retirement(thread_id, permanent=True)

    contenders = []
    try:
        async with db.acquire() as conn:
            async with conn.transaction():
                await conn.fetchrow(
                    "SELECT id FROM threads WHERE id=$1::uuid FOR UPDATE", thread_id
                )
                first = asyncio.create_task(end() if end_first else admit())
                contenders.append(first)
                await wait_blocked(conn, 1, contenders)
                second = asyncio.create_task(admit() if end_first else end())
                contenders.append(second)
                await wait_blocked(conn, 2, contenders)
    except BaseException:
        for contender in contenders:
            contender.cancel()
        await asyncio.gather(*contenders, return_exceptions=True)
        raise
    first_result, second_result = await asyncio.gather(first, second)
    return (first_result, second_result) if end_first else (second_result, first_result)


@pytest.mark.asyncio
async def test_retained_resume_replays_one_claim_and_end_owns_precreate_storage(
    db, monkeypatch, tmp_path
):
    scenarios = {}
    for mode in (
        "ready",
        "failed_again",
        "end_before_create",
        "soft_end_before_create",
        "effects_admitted",
        "prior_virtual",
        "prior_remote",
    ):
        if mode.startswith("prior_"):
            (
                ids,
                cluster,
                provider,
                intent,
            ) = await cases._failed_start_with_prior_binding(
                db, monkeypatch, tmp_path, mode.removeprefix("prior_")
            )
        else:
            ids, cluster, provider, intent = await cases._failed_start(db, monkeypatch)
        retirement = await cases._retire_unbound(
            db, provider, ids["thread"], permanent=False
        )
        scenarios[mode] = (ids, cluster, provider, intent, retirement)
        assert not await db.resume_thread(ids["thread"])
        assert await db.get_pinned_retained_creation_wait(ids["thread"]) is not None

    # The real database horizon is the test clock. Existing causal fences must
    # remain present until it expires; the test never advances their timestamps.
    while True:
        remaining = await db.fetchval(
            "SELECT greatest(0,extract(epoch FROM max(gc_after)-clock_timestamp())) "
            "FROM thread_workspace_provision_intents WHERE status='fenced'"
        )
        if float(remaining) <= 0:
            break
        await asyncio.sleep(min(float(remaining) + 0.05, 20))

    for ids, cluster, provider, intent, _ in scenarios.values():
        rows = await db.list_pinned_thread_workspace_provision_fences_for_gc()
        source = next(row for row in rows if row["attempt_id"] == intent["attempt_id"])
        assert await provider.delete_pinned_workspace_provision_fences_exact(source)
        assert await db.retire_pinned_thread_workspace_provision_fence(
            str(source["attempt_id"]),
            **{
                f"expected_{key}": source[key]
                for key in (
                    "fence_pod_uid",
                    "fence_pvc_uid",
                    "fence_configmap_uid",
                    "fence_service_uid",
                )
            },
        )
        assert set(cluster.objects) == {"pvc", "service"}
        # A metadata pointer cannot grant another owner's retained storage.
        with pytest.raises(asyncpg.CheckViolationError):
            async with db.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        "UPDATE threads SET metadata=jsonb_set(metadata,'{_pinned_retained_creation_attempt}',to_jsonb($2::text)) WHERE id=$1::uuid",
                        ids["thread"],
                        str(uuid4()),
                    )
                    await conn.execute(
                        "UPDATE threads SET status='created' WHERE id=$1::uuid",
                        ids["thread"],
                    )
        assert await db.resume_thread(ids["thread"])
        thread = await db.get_thread(ids["thread"])
        assert not await db.resume_thread(ids["thread"])
        assert (
            await db.fetchval(
                "SELECT count(*) FROM thread_workspace_provision_intents WHERE thread_id=$1::uuid AND status='planned'",
                ids["thread"],
            )
            == 1
        )
        successor = await db.fetchrow(
            "SELECT * FROM thread_workspace_provision_intents WHERE thread_id=$1::uuid AND status='planned'",
            ids["thread"],
        )
        assert successor["runtime_generation"] == thread["runtime_generation"]
        assert successor["retained_source_attempt_id"] == intent["attempt_id"]
        assert cases.fixtures._json(
            successor["previous_binding"]
        ) == cases.fixtures._json(intent["previous_binding"])
        assert (
            successor["retained_binding_generation"]
            == intent["retained_binding_generation"]
        )
        assert successor["retained_pvc_uid"] == intent["pvc_uid"]
        assert successor["pod_uid"] is None
        assert successor["creation_effects_admitted_at"] is None
        source = await db.fetchrow(
            "SELECT * FROM thread_workspace_provision_intents WHERE attempt_id=$1",
            intent["attempt_id"],
        )
        assert source["retained_successor_attempt_id"] == successor["attempt_id"]
        assert source["retained_resume_generation"] == thread["runtime_generation"]
        assert thread["runtime_generation"] != intent["runtime_generation"]
        with pytest.raises(asyncpg.CheckViolationError):
            await db.execute(
                "UPDATE thread_workspace_provision_intents SET retained_successor_attempt_id=$2 WHERE attempt_id=$1",
                intent["attempt_id"],
                uuid4(),
            )

    ids, cluster, provider, intent, retirement = scenarios["ready"]
    # A reconstructed caller reuses the already committed intent and exact
    # retained storage. Only external SSH transport and Pod exec are faked.
    provider = cases.pull._provisioner(monkeypatch, db, cluster)
    generation = (await db.get_thread(ids["thread"]))["runtime_generation"]
    await cases._attach_after_start(db, ids["thread"])
    assert (await db.get_thread(ids["thread"]))["runtime_generation"] == generation
    fingerprint = "SHA256:" + "A" * 43
    monkeypatch.setattr(
        provider_module, "wait_for_agent_ssh", AsyncMock(return_value=(True, 1, None))
    )
    monkeypatch.setattr(
        provider_module, "workspace_private_key_fingerprint", lambda _: fingerprint
    )
    monkeypatch.setattr(
        provider_module,
        "_isolated_pod_exec",
        lambda *args, **kwargs: f"256 {fingerprint} workspace (ED25519)",
    )
    create = cluster.create_namespaced_pod

    def ready_pod(**kwargs):
        pod = create(**kwargs)
        pod.status.phase = "Running"
        pod.status.container_statuses[0].ready = True
        pod.status.container_statuses[0].started = True
        pod.status.container_statuses[0].state = SimpleNamespace(
            waiting=None, running=SimpleNamespace(), terminated=None
        )
        return pod

    cluster.create_namespaced_pod = ready_pod
    result = await ensure_session_workspace(
        ids["thread"],
        db=db,
        provisioner=provider,
        suspension=None,
        expected_runtime_generation=str(generation),
    )
    assert result is not None and result.outcome != EnsureOutcome.FAILED
    ready = await db.get_thread(ids["thread"])
    metadata = (
        json.loads(ready["metadata"])
        if isinstance(ready["metadata"], str)
        else ready["metadata"]
    )
    assert metadata["workspace_container"]["status"] == "ready"
    assert (
        metadata["_workspace_binding"]["backing_id"]
        == f"k8s-pvc:agent-workspaces:{intent['pvc_uid']}"
    )
    assert "_pinned_retained_creation_attempt" not in metadata
    assert cluster.objects["pvc"].metadata.uid == intent["pvc_uid"]
    assert not await db.revoke_pinned_thread_workspace_provision_intent(
        ids["thread"],
        expected_runtime_generation=retirement["generation"],
        expected_retirement_token=retirement["token"],
        expected_attempt_id=str(intent["attempt_id"]),
    )

    for mode in ("prior_virtual", "prior_remote"):
        ids, cluster, provider, intent, _ = scenarios[mode]
        assert not await provider.create_pinned_thread_workspace(ids["thread"])
        binding = await cases._make_existing_pod_ready(
            db, monkeypatch, ids, cluster, provider
        )
        assert binding["kind"] == "remote"
        assert binding["backing_id"] == f"k8s-pvc:agent-workspaces:{intent['pvc_uid']}"
        if mode == "prior_remote":
            assert binding == cases.fixtures._json(intent["previous_binding"])

    ids, cluster, provider, intent, _ = scenarios["failed_again"]
    assert not await provider.create_pinned_thread_workspace(ids["thread"])
    await cases._retire_unbound(db, provider, ids["thread"], permanent=False)
    assert cluster.objects["pvc"].metadata.uid == intent["pvc_uid"]
    metadata = (await db.get_thread(ids["thread"]))["metadata"]
    metadata = json.loads(metadata) if isinstance(metadata, str) else metadata
    assert metadata["_pinned_retained_creation_attempt"] != str(intent["attempt_id"])

    ids, cluster, provider, intent, _ = scenarios["soft_end_before_create"]
    successor = await db.fetchrow(
        "SELECT * FROM thread_workspace_provision_intents WHERE thread_id=$1::uuid AND status='planned'",
        ids["thread"],
    )
    await cases._retire_unbound(db, provider, ids["thread"], permanent=False)
    ended = await db.get_thread(ids["thread"])
    assert ended["status"] == "ended"
    assert cluster.objects["pvc"].metadata.uid == intent["pvc_uid"]
    assert cases.fixtures._json(ended["metadata"])[
        "_pinned_retained_creation_attempt"
    ] == str(successor["attempt_id"])
    assert not await db.admit_retained_pinned_workspace_creation_effects(
        ids["thread"],
        runtime_generation=str(successor["runtime_generation"]),
        attempt_id=str(successor["attempt_id"]),
    )

    ids, cluster, provider, intent, _ = scenarios["end_before_create"]
    successor = await db.fetchrow(
        "SELECT * FROM thread_workspace_provision_intents WHERE thread_id=$1::uuid AND status='planned'",
        ids["thread"],
    )
    retirement, admitted = await _race_end_and_admission(
        db, ids["thread"], successor, end_first=True
    )
    assert retirement["state"] == "pending"
    assert admitted is False
    await cases._retire_unbound(db, provider, ids["thread"], permanent=True)
    assert await db.get_thread(ids["thread"]) is None
    assert cluster.objects["pvc"].metadata.uid != intent["pvc_uid"]

    ids, cluster, provider, intent, _ = scenarios["effects_admitted"]
    successor = await db.fetchrow(
        "SELECT * FROM thread_workspace_provision_intents WHERE thread_id=$1::uuid AND status='planned'",
        ids["thread"],
    )
    assert await db.admit_retained_pinned_workspace_creation_effects(
        ids["thread"],
        runtime_generation=str(successor["runtime_generation"]),
        attempt_id=str(successor["attempt_id"]),
    )
    before = cases.fixtures._json((await db.get_thread(ids["thread"]))["metadata"])
    try:
        await db.execute(
            "UPDATE threads SET metadata=jsonb_set(metadata,"
            "'{workspace_container,_workspace_provision_attempt}',to_jsonb($2::text)) "
            "WHERE id=$1::uuid",
            ids["thread"],
            str(uuid4()),
        )
        assert not await db.admit_retained_pinned_workspace_creation_effects(
            ids["thread"],
            runtime_generation=str(successor["runtime_generation"]),
            attempt_id=str(successor["attempt_id"]),
        )
    finally:
        await db.execute(
            "UPDATE threads SET metadata=$2::jsonb WHERE id=$1::uuid",
            ids["thread"],
            json.dumps(before),
        )
    retirement, admitted = await _race_end_and_admission(
        db, ids["thread"], successor, end_first=False
    )
    assert retirement["state"] == "pending"
    assert admitted is True
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE thread_workspace_provision_intents SET pod_uid=$2 WHERE attempt_id=$1",
            successor["attempt_id"],
            str(uuid4()),
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE thread_workspace_provision_intents SET creation_effects_admitted_at=NULL WHERE attempt_id=$1",
            successor["attempt_id"],
        )
    assert await db.authorize_pinned_thread_retirement(
        ids["thread"],
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    assert not await db.admit_retained_pinned_workspace_creation_effects(
        ids["thread"],
        runtime_generation=retirement["generation"],
        attempt_id=str(successor["attempt_id"]),
    )
    revoked = await db.revoke_pinned_thread_workspace_provision_intent(
        ids["thread"],
        expected_runtime_generation=retirement["generation"],
        expected_retirement_token=retirement["token"],
        expected_attempt_id=str(successor["attempt_id"]),
    )
    assert (
        await provider.fence_pinned_workspace_provision_intent(
            revoked, permanent=True, expected_retirement_token=retirement["token"]
        )
        is None
    )
    assert cluster.objects["pvc"].metadata.uid == intent["pvc_uid"]
