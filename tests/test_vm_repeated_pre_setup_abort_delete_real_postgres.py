"""Repeated confirmed pre-setup releases retain creation and purge authority."""

from uuid import uuid4

import asyncpg
import pytest

from orchestrator.services.vm_thread_network import document
from tests.test_pinned_vm_failed_initial_end_real_postgres import (
    _damage_first_edge,
    _release_binding,
)
from tests.test_pinned_vm_initial_binding_real_postgres import _bind_cold_agent
from tests.test_vm_thread_adopted_without_quotas_delete_real_postgres import (
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    adopted_source,
    cleaned_retirement,
    db,  # noqa: F401
    pg_dsn,  # noqa: F401
    setup,  # noqa: F401
    thread_schema,  # noqa: F401
)


async def released_chain(store, controller_setup, monkeypatch, count):
    current, source = await adopted_source(store, controller_setup, monkeypatch)
    for index in range(count):
        previous = current
        assert await _release_binding(store, previous) == "released"
        assert await store.delete_agent(str(previous["agent_id"]))
        current = await store.get_thread(str(previous["id"]))
        assert current["runtime_generation"] != previous["runtime_generation"]
        assert current["agent_id"] is None
        assert current["runtime_attach_token"] is None
        if index + 1 < count:
            current = await _bind_cold_agent(store, current["id"])
    outcomes = [
        dict(row)
        for row in await store.fetch(
            "SELECT * FROM thread_runtime_attach_abort_outcomes WHERE thread_id=$1 "
            "ORDER BY released_at",
            current["id"],
        )
    ]
    assert len(outcomes) == count
    assert all(
        row["quiescence_protocol"] == "agent_attach_not_started_v1" for row in outcomes
    )
    assert outcomes[0]["runtime_generation"] == source["thread_runtime_generation"]
    assert outcomes[-1]["successor_generation"] == current["runtime_generation"]
    return current, source, outcomes


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [2, 3])
async def test_repeated_confirmed_pre_setup_release_then_normal_permanent_delete(
    db,  # noqa: F811 - imported full-schema PostgreSQL fixture
    setup,  # noqa: F811 - imported actuator fixture
    monkeypatch,
    count,
):
    current, source, outcomes = await released_chain(db, setup, monkeypatch, count)
    retirement = await cleaned_retirement(db, current, permanent=True)
    cleared = await db.get_thread(str(current["id"]))
    assert not document(cleared["metadata"]).get("vm")
    retry = await cleaned_retirement(db, cleared, permanent=True)
    assert (retry["generation"], retry["token"], retry["context"]["vm"]) == (
        retirement["generation"],
        retirement["token"],
        retirement["context"]["vm"],
    )
    assert await db.fetchval(
        "SELECT EXISTS(SELECT 1 FROM vm_workspace_cleanup_admissions WHERE owner_id=$1 "
        "AND source='pinned_thread_retirement' AND completed_at IS NOT NULL "
        "AND outcome='completed')",
        current["id"],
    )
    await db.delete_thread(
        str(current["id"]),
        expected_runtime_generation=retirement["generation"],
        expected_runtime_retirement_token=retirement["token"],
    )
    assert await db.get_thread(str(current["id"])) is None
    audit = await db.fetchrow(
        "SELECT * FROM vm_thread_creation_owners WHERE thread_id=$1", current["id"]
    )
    assert audit["live_thread_id"] is None and audit["deleted_at"] is not None
    assert str(audit["deleted_retirement_token"]) == retirement["token"]
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE request_id=$1",
                source["request_id"],
            )
        )
        == source
    )
    assert [
        dict(row)
        for row in await db.fetch(
            "SELECT * FROM thread_runtime_attach_abort_outcomes WHERE thread_id=$1 ORDER BY released_at",
            current["id"],
        )
    ] == outcomes


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "defect",
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
        "last_missing",
        "last_branch",
        "last_cycle",
        "last_protocol",
        "last_release_kind",
        "last_pod",
        "last_workspace_generation",
        "last_workspace_incarnation",
        "last_partial_workspace",
        "last_future_release",
        "last_reverse_time",
        "first_before_adoption",
        "last_unpublished",
        "last_unprotected",
        "last_late_publication",
        "purge_digest",
        "purge_incomplete",
        "purge_outcome",
        "vm_uid",
        "pvc_uid",
        "capture_missing",
    ],
)
async def test_repeated_abort_delete_refuses_inexact_path_or_purge(
    db,  # noqa: F811
    setup,  # noqa: F811
    monkeypatch,
    defect,
):
    current, source, outcomes = await released_chain(db, setup, monkeypatch, 2)
    retirement = await cleaned_retirement(db, current, permanent=True)
    first, last = outcomes
    if defect in {
        "missing",
        "branch",
        "actor",
        "attach",
        "pod",
        "cycle",
        "foreign",
        "protocol",
        "workspace",
    }:
        await _damage_first_edge(db, source, defect)
    else:
        async with db.acquire() as conn, conn.transaction():
            await conn.execute("SET LOCAL session_replication_role='replica'")
            if defect == "last_missing":
                await conn.execute(
                    "DELETE FROM thread_runtime_attach_abort_outcomes WHERE thread_id=$1 AND runtime_generation=$2",
                    current["id"],
                    last["runtime_generation"],
                )
            elif defect == "last_branch":
                await conn.execute(
                    "INSERT INTO thread_runtime_attach_abort_outcomes SELECT "
                    "thread_id,runtime_generation,$3,agent_id,agent_pod_uid,successor_generation,release_kind,"
                    "quiescence_protocol,workspace_generation,workspace_runtime_incarnation,released_at "
                    "FROM thread_runtime_attach_abort_outcomes WHERE thread_id=$1 AND runtime_generation=$2",
                    current["id"],
                    last["runtime_generation"],
                    uuid4(),
                )
            elif defect in {
                "last_unpublished",
                "last_unprotected",
                "last_late_publication",
            }:
                change = {
                    "last_unpublished": "status='retired',fenced_at=now(),gc_after=now()",
                    "last_unprotected": "protection_protocol=NULL,namespace=NULL",
                    "last_late_publication": "resolved_at=now()+interval '1 day'",
                }[defect]
                await conn.execute(
                    f"UPDATE thread_agent_pod_provision_intents SET {change} WHERE thread_id=$1 AND runtime_generation=$2",
                    current["id"],
                    last["runtime_generation"],
                )
            elif defect.startswith("last_"):
                change = {
                    "last_cycle": "successor_generation=$3",
                    "last_protocol": "quiescence_protocol='agent_runtime_zero_v1'",
                    "last_release_kind": "release_kind='server_pre_delivery'",
                    "last_pod": "agent_pod_uid='replacement-pod-uid'",
                    "last_workspace_generation": "workspace_generation='"
                    + str(uuid4())
                    + "',workspace_runtime_incarnation='"
                    + str(source["observed_vm_uid"])
                    + "'",
                    "last_workspace_incarnation": "workspace_generation='"
                    + str(source["provision_generation"])
                    + "',workspace_runtime_incarnation='"
                    + str(uuid4())
                    + "'",
                    "last_partial_workspace": "workspace_generation='"
                    + str(source["provision_generation"])
                    + "'",
                    "last_future_release": "released_at=now()+interval '1 day'",
                    "last_reverse_time": "released_at=now()-interval '1 day'",
                }[defect]
                args = [current["id"], last["runtime_generation"]]
                if defect == "last_cycle":
                    args.append(first["runtime_generation"])
                await conn.execute(
                    f"UPDATE thread_runtime_attach_abort_outcomes SET {change} WHERE thread_id=$1 AND runtime_generation=$2",
                    *args,
                )
                if defect == "last_reverse_time":
                    # Keep publication earlier so this independently exercises
                    # monotonic abort order rather than the Pod-time check.
                    await conn.execute(
                        "UPDATE thread_agent_pod_provision_intents SET resolved_at=now()-interval '2 days' "
                        "WHERE thread_id=$1 AND runtime_generation=$2",
                        current["id"],
                        last["runtime_generation"],
                    )
            elif defect == "first_before_adoption":
                await conn.execute(
                    "UPDATE thread_runtime_attach_abort_outcomes SET released_at=now()-interval '1 day' "
                    "WHERE thread_id=$1 AND runtime_generation=$2",
                    current["id"],
                    first["runtime_generation"],
                )
                await conn.execute(
                    "UPDATE thread_agent_pod_provision_intents SET resolved_at=now()-interval '2 days' "
                    "WHERE thread_id=$1 AND runtime_generation=$2",
                    current["id"],
                    first["runtime_generation"],
                )
            elif defect.startswith("purge_"):
                change = {
                    "purge_digest": "intent_digest='sha256:" + "0" * 64 + "'",
                    "purge_incomplete": "completed_at=NULL,outcome=NULL",
                    "purge_outcome": "outcome='superseded'",
                }[defect]
                await conn.execute(
                    f"UPDATE vm_workspace_cleanup_admissions SET {change} WHERE owner_id=$1 AND source='pinned_thread_retirement'",
                    current["id"],
                )
            elif defect in {"vm_uid", "pvc_uid"}:
                key = "vm_uid" if defect == "vm_uid" else "rootdisk_pvc_uid"
                await conn.execute(
                    "UPDATE threads SET runtime_retirement_context=jsonb_set(runtime_retirement_context,$2::text[],to_jsonb($3::text)) WHERE id=$1",
                    current["id"],
                    ["vm", key],
                    str(uuid4()),
                )
            elif defect == "capture_missing":
                await conn.execute(
                    "UPDATE threads SET runtime_retirement_context=runtime_retirement_context-'vm' WHERE id=$1",
                    current["id"],
                )
    if defect in {"vm_uid", "pvc_uid", "capture_missing"}:
        with pytest.raises(
            RuntimeError, match="permanent pinned delete lacks physical quiescence"
        ):
            await db.delete_thread(
                str(current["id"]),
                expected_runtime_generation=retirement["generation"],
                expected_runtime_retirement_token=retirement["token"],
            )
        assert await db.get_thread(str(current["id"])) is not None
        return
    try:
        await db.delete_thread(
            str(current["id"]),
            expected_runtime_generation=retirement["generation"],
            expected_runtime_retirement_token=retirement["token"],
        )
    except asyncpg.CheckViolationError:
        pass
    assert await db.get_thread(str(current["id"])) is not None
