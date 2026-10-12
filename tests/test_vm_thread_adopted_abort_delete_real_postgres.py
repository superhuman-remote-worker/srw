"""A confirmed pre-setup release preserves the original VM creation audit."""

from uuid import uuid4

import asyncpg
import pytest

from tests.test_pinned_vm_failed_initial_end_real_postgres import _release_binding
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


@pytest.mark.asyncio
@pytest.mark.parametrize("soft_first", [False, True])
async def test_adopted_vm_pre_setup_release_then_permanent_delete(
    db,  # noqa: F811
    setup,  # noqa: F811
    monkeypatch,
    soft_first,
):
    original, source = await adopted_source(db, setup, monkeypatch)
    assert await _release_binding(db, original) == "released"
    current = await db.get_thread(str(original["id"]))
    assert current["runtime_generation"] != original["runtime_generation"]
    assert current["agent_id"] is None and current["runtime_attach_token"] is None
    abort = await db.fetchrow(
        "SELECT * FROM thread_runtime_attach_abort_outcomes "
        "WHERE thread_id=$1 AND runtime_generation=$2",
        original["id"],
        original["runtime_generation"],
    )
    assert abort["successor_generation"] == current["runtime_generation"]
    assert abort["agent_id"] == source["thread_agent_id"]
    assert abort["runtime_attach_token"] == source["thread_attach_token"]
    assert abort["release_kind"] == "process_zero"
    assert abort["quiescence_protocol"] == "agent_attach_not_started_v1"
    assert abort["workspace_generation"] is None
    assert abort["workspace_runtime_incarnation"] is None
    # The exact aborted actor deregisters; its absence is not VM process zero.
    assert await db.delete_agent(str(original["agent_id"]))
    retirement = await cleaned_retirement(db, current, permanent=not soft_first)
    if soft_first:
        assert await db.settle_pinned_thread_retirement(
            str(current["id"]),
            token=retirement["token"],
            generation=retirement["generation"],
            final_status="ended",
        )
        current = await db.get_thread(str(current["id"]))
        retirement = await cleaned_retirement(db, current, permanent=True)
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
    assert dict(
        await db.fetchrow(
            "SELECT * FROM thread_runtime_attach_abort_outcomes "
            "WHERE thread_id=$1 AND runtime_generation=$2",
            original["id"],
            original["runtime_generation"],
        )
    ) == dict(abort)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "defect",
    [
        "replacement_uid",
        "missing_intent",
        "other_generation",
        "other_owner",
        "unpublished",
        "unprotected",
        "late_pod_publication",
        "missing_abort",
        "source_generation",
        "abort_owner",
        "successor_generation",
        "source_actor",
        "source_token",
        "setup_work",
        "release_kind",
        "late_creation_adoption",
        "workspace_generation",
        "workspace_incarnation",
        "partial_workspace_identity",
    ],
)
async def test_pre_setup_abort_delete_requires_exact_published_agent_pod(
    db,  # noqa: F811
    setup,  # noqa: F811
    monkeypatch,
    defect,
):
    original, _ = await adopted_source(db, setup, monkeypatch)
    assert await _release_binding(db, original) == "released"
    current = await db.get_thread(str(original["id"]))
    assert await db.delete_agent(str(original["agent_id"]))
    retirement = await cleaned_retirement(db, current, permanent=True)
    # Corrupt only a disposable test database to test final audit authority.
    async with db.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL session_replication_role='replica'")
        if defect == "replacement_uid":
            await conn.execute(
                "UPDATE thread_runtime_attach_abort_outcomes SET agent_pod_uid=$3 "
                "WHERE thread_id=$1 AND runtime_generation=$2",
                original["id"],
                original["runtime_generation"],
                str(uuid4()),
            )
        elif defect == "missing_intent":
            await conn.execute(
                "DELETE FROM thread_agent_pod_provision_intents "
                "WHERE thread_id=$1 AND runtime_generation=$2",
                original["id"],
                original["runtime_generation"],
            )
        elif defect in {
            "other_generation",
            "other_owner",
            "unpublished",
            "unprotected",
            "late_pod_publication",
        }:
            change = {
                "other_generation": "runtime_generation='" + str(uuid4()) + "'",
                "other_owner": "thread_id='" + str(uuid4()) + "'",
                "unpublished": "status='retired',fenced_at=now(),gc_after=now()",
                "unprotected": "namespace=NULL,protection_protocol=NULL",
                "late_pod_publication": "resolved_at=now()+interval '1 day'",
            }[defect]
            await conn.execute(
                f"UPDATE thread_agent_pod_provision_intents SET {change} "
                "WHERE thread_id=$1 AND runtime_generation=$2",
                original["id"],
                original["runtime_generation"],
            )
        elif defect == "late_creation_adoption":
            await conn.execute(
                "UPDATE vm_workspace_cleanup_admissions SET completed_at=now()+interval '1 day' "
                "WHERE owner_kind='thread' AND owner_id=$1 AND source='controller_vm_create'",
                original["id"],
            )
        elif defect == "missing_abort":
            await conn.execute(
                "DELETE FROM thread_runtime_attach_abort_outcomes "
                "WHERE thread_id=$1 AND runtime_generation=$2",
                original["id"],
                original["runtime_generation"],
            )
        else:
            change = {
                "source_generation": "runtime_generation='" + str(uuid4()) + "'",
                "abort_owner": "thread_id='" + str(uuid4()) + "'",
                "successor_generation": "successor_generation='" + str(uuid4()) + "'",
                "source_actor": "agent_id='" + str(uuid4()) + "'",
                "source_token": "runtime_attach_token='" + str(uuid4()) + "'",
                "setup_work": "quiescence_protocol='agent_quiescent_v1'",
                "release_kind": "release_kind='server_pre_delivery'",
                "workspace_generation": "workspace_generation='"
                + str(uuid4())
                + "',workspace_runtime_incarnation=(SELECT observed_vm_uid::uuid FROM vm_creation_retries WHERE thread_id=$1 LIMIT 1)",
                "workspace_incarnation": "workspace_generation=(SELECT provision_generation FROM vm_creation_retries WHERE thread_id=$1 LIMIT 1),workspace_runtime_incarnation='"
                + str(uuid4())
                + "'",
                "partial_workspace_identity": "workspace_generation='"
                + str(uuid4())
                + "'",
            }[defect]
            await conn.execute(
                f"UPDATE thread_runtime_attach_abort_outcomes SET {change} "
                "WHERE thread_id=$1 AND runtime_generation=$2",
                original["id"],
                original["runtime_generation"],
            )
        if defect in {"source_generation", "abort_owner"}:
            column = (
                "runtime_generation" if defect == "source_generation" else "thread_id"
            )
            # Keep the Pod edge internally coherent; the independent source
            # owner/generation comparison must still refuse the wrong life.
            await conn.execute(
                f"UPDATE thread_agent_pod_provision_intents SET {column}="
                f"(SELECT {column} FROM thread_runtime_attach_abort_outcomes "
                "WHERE agent_id=$3) WHERE thread_id=$1 AND runtime_generation=$2",
                original["id"],
                original["runtime_generation"],
                original["agent_id"],
            )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.delete_thread(
            str(current["id"]),
            expected_runtime_generation=retirement["generation"],
            expected_runtime_retirement_token=retirement["token"],
        )
    assert await db.get_thread(str(current["id"])) is not None
