"""Exact pre-setup abort lineage retains cleanup of its same published workspace."""

from datetime import timedelta
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from orchestrator import main
from orchestrator.application import controls as controls_composition
from orchestrator.services.session_attach_binding import release_session_attach_binding
from tests import test_pinned_workspace_cleanup_real_postgres as workspace


db = workspace.db
pg_dsn = workspace.pg_dsn
_schema_applied = workspace._schema_applied


async def _rotated_soft_end(db, monkeypatch, *, ephemeral, aborts=1):
    ids, owner, provisioner, resources, effects = await workspace._scenario(
        db, monkeypatch, ephemeral=ephemeral
    )
    # The inherited fixture seeds an Officer post, while this scenario
    # deliberately uses its disabled-Office config and a general successor.
    await db.execute("DELETE FROM project_officers WHERE thread_id=$1::uuid", owner.id)
    before = await db.get_thread(owner.id)
    metadata = workspace.authority._json(before["metadata"])
    initial = str(before["runtime_generation"])
    pod_uid = resources["pod"].metadata.uid
    for _ in range(aborts):
        current = await db.get_thread(owner.id)
        metadata = workspace.authority._json(current["metadata"])
        generation = str(current["runtime_generation"])
        await db.execute(
            "UPDATE threads SET status='created' WHERE id=$1::uuid", owner.id
        )
        released = await release_session_attach_binding(
            ids["agent"],
            owner.id,
            expected_runtime_generation=generation,
            expected_attach_token=ids["attach_token"],
            expected_agent_pod_uid=metadata["agent_pod"]["pod_uid"],
            local_runtime_quiesced=True,
            local_quiescence_protocol="agent_attach_not_started_v1",
            workspace_generation=metadata["_workspace_binding"]["generation"],
            workspace_runtime_incarnation=pod_uid,
            dependencies=SimpleNamespace(store=db),
        )
        assert released == "released"
        # Confirmed release is followed by normal actor deregistration. The
        # exited-Pod model must not retain a ready registration as live proof.
        assert await db.delete_agent(ids["agent"])
        rotated = await db.get_thread(owner.id)
        assert str(rotated["runtime_generation"]) != generation
        successor, _actor = await workspace.authority._bind_replacement_agent(
            db,
            thread_id=owner.id,
            pod_uid=str(uuid4()),
            pod_name="successor-" + str(uuid4())[:12],
        )
        current = await db.get_thread(owner.id)
        ids["agent"] = successor
        ids["attach_token"] = str(current["runtime_attach_token"])
    assert (
        await db.fetchval(
            "SELECT runtime_generation::text FROM thread_workspace_provision_intents WHERE thread_id=$1::uuid",
            owner.id,
        )
        == initial
    )
    soft = await workspace._begin(db, ids, False)
    await controls_composition.pinned_retirement_operations(
        main.app.state.resources
    ).cleanup_pinned_thread_retirement(soft, cleanup_agent_pod=False)
    assert await db.settle_pinned_thread_retirement(
        owner.id,
        token=soft["token"],
        generation=soft["generation"],
        final_status="ended",
    )
    assert "pod" not in resources
    assert (
        await db.fetchval(
            "SELECT count(*) FROM managed_repository_process_zero_receipts WHERE owner_id=$1::uuid AND scope='workspace_container' AND runtime_incarnation=$2",
            owner.id,
            pod_uid,
        )
        == 1
    )
    return (
        ids,
        owner,
        provisioner,
        resources,
        effects,
        initial,
        soft["generation"],
        pod_uid,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("ephemeral", [True, False])
async def test_abort_rotation_soft_end_keeps_exact_workspace_permanent_delete(
    db, monkeypatch, ephemeral
):
    (
        ids,
        owner,
        _provisioner,
        resources,
        _effects,
        initial,
        current,
        _pod,
    ) = await _rotated_soft_end(db, monkeypatch, ephemeral=ephemeral)
    permanent = await db.begin_pinned_thread_retirement(owner.id, permanent=True)
    assert permanent["state"] == "pending", permanent
    assert permanent["generation"] == current
    assert initial != current
    assert await db.authorize_pinned_thread_retirement(
        owner.id,
        token=permanent["token"],
        generation=permanent["generation"],
        settle_status="ended",
    )
    await controls_composition.pinned_retirement_operations(
        main.app.state.resources
    ).cleanup_pinned_thread_retirement(permanent, cleanup_agent_pod=False)
    await db.delete_thread(
        owner.id,
        expected_runtime_retirement_token=permanent["token"],
        expected_runtime_generation=permanent["generation"],
    )
    assert await db.get_thread(owner.id) is None
    assert not resources


@pytest.mark.asyncio
@pytest.mark.parametrize("aborts", [2, 16, 17])
async def test_abort_workspace_lineage_is_bounded_and_records_distinct_lives(
    db, monkeypatch, aborts
):
    _, owner, _, _, _, initial, current, _ = await _rotated_soft_end(
        db, monkeypatch, ephemeral=True, aborts=aborts
    )
    from orchestrator.database.postgres import (
        _settled_pinned_workspace_current_generation,
    )

    metadata = workspace.authority._json((await db.get_thread(owner.id))["metadata"])
    async with db.acquire() as conn:
        capture = await _settled_pinned_workspace_current_generation(
            conn,
            thread_id=UUID(owner.id),
            current_generation=UUID(current),
            workspace=metadata["workspace_container"],
            binding=metadata["_workspace_binding"],
        )
    if aborts == 17:
        assert capture is None
    else:
        assert capture is not None
        assert capture["source_runtime_generation"] == initial
        assert capture["runtime_generation"] == current
        assert capture["attach_abort_path"][0] == initial
        assert capture["attach_abort_path"][-1] == current
        assert len(capture["attach_abort_path"]) == aborts + 1


@pytest.mark.asyncio
@pytest.mark.parametrize("ephemeral", [True, False])
@pytest.mark.parametrize(
    "fault",
    [
        "missing-link",
        "branch",
        "cycle",
        "foreign-owner",
        "setup-exposed",
        "no-zero-release",
        "workspace-generation",
        "workspace-runtime",
        "missing-zero",
        "agent-only-zero",
        "wrong-zero-runtime",
        "foreign-zero-owner",
        "zero-before-abort",
        "zero-after-end",
        "missing-end",
        "suspended-end",
        "successor-end",
        "permanent-end",
        "unsettled-end",
        "unpublished-source",
        "replacement-pod",
        "replacement-resource",
        "source-generation",
    ],
)
async def test_abort_workspace_lineage_refuses_incomplete_or_changed_authority(
    db, monkeypatch, ephemeral, fault
):
    _, owner, _, resources, effects, initial, current, _ = await _rotated_soft_end(
        db, monkeypatch, ephemeral=ephemeral
    )
    saved_effects = list(effects)
    # Only disposable test history is corrupted; native positive paths above
    # retain every authority trigger. Never use this seam in live acceptance.
    async with db.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL session_replication_role='replica'")
        if fault == "missing-link":
            await conn.execute(
                "DELETE FROM thread_runtime_attach_abort_outcomes WHERE thread_id=$1::uuid",
                owner.id,
            )
        elif fault == "branch":
            await conn.execute(
                "INSERT INTO thread_runtime_attach_abort_outcomes SELECT thread_id,runtime_generation,$2,agent_id,agent_pod_uid,successor_generation,release_kind,quiescence_protocol,workspace_generation,workspace_runtime_incarnation,released_at FROM thread_runtime_attach_abort_outcomes WHERE thread_id=$1::uuid",
                owner.id,
                uuid4(),
            )
        elif fault in {
            "cycle",
            "foreign-owner",
            "setup-exposed",
            "no-zero-release",
            "workspace-generation",
            "workspace-runtime",
        }:
            column, value = {
                "cycle": ("successor_generation", UUID(initial)),
                "foreign-owner": ("thread_id", uuid4()),
                "setup-exposed": ("quiescence_protocol", "agent_runtime_zero_v1"),
                "no-zero-release": ("release_kind", "server_pre_delivery"),
                "workspace-generation": ("workspace_generation", uuid4()),
                "workspace-runtime": ("workspace_runtime_incarnation", uuid4()),
            }[fault]
            await conn.execute(
                f"UPDATE thread_runtime_attach_abort_outcomes SET {column}=$2 WHERE thread_id=$1::uuid",
                owner.id,
                value,
            )
        elif fault == "missing-zero":
            await conn.execute(
                "DELETE FROM managed_repository_process_zero_receipts WHERE owner_id=$1::uuid",
                owner.id,
            )
        elif fault in {"agent-only-zero", "wrong-zero-runtime", "foreign-zero-owner"}:
            column, value = {
                "agent-only-zero": ("scope", "stateless_workspace"),
                "wrong-zero-runtime": ("runtime_incarnation", str(uuid4())),
                "foreign-zero-owner": ("owner_id", uuid4()),
            }[fault]
            await conn.execute(
                f"UPDATE managed_repository_process_zero_receipts SET {column}=$2 WHERE owner_id=$1::uuid",
                owner.id,
                value,
            )
        elif fault in {"zero-before-abort", "zero-after-end"}:
            timestamp = await conn.fetchval(
                "SELECT released_at FROM thread_runtime_attach_abort_outcomes WHERE thread_id=$1::uuid"
                if fault == "zero-before-abort"
                else "SELECT settled_at FROM thread_runtime_retirement_outcomes WHERE thread_id=$1::uuid",
                owner.id,
            )
            timestamp += timedelta(seconds=-1 if fault == "zero-before-abort" else 1)
            await conn.execute(
                "UPDATE managed_repository_process_zero_receipts SET observed_at=$2 WHERE owner_id=$1::uuid",
                owner.id,
                timestamp,
            )
        elif fault == "missing-end":
            await conn.execute(
                "DELETE FROM thread_runtime_retirement_outcomes WHERE thread_id=$1::uuid",
                owner.id,
            )
        elif fault in {
            "suspended-end",
            "successor-end",
            "permanent-end",
            "unsettled-end",
        }:
            column, value = {
                "suspended-end": ("disposition", "suspended"),
                "successor-end": ("runtime_generation", uuid4()),
                "permanent-end": ("permanent", True),
                "unsettled-end": ("outcome", "deleted"),
            }[fault]
            await conn.execute(
                f"UPDATE thread_runtime_retirement_outcomes SET {column}=$2 WHERE thread_id=$1::uuid",
                owner.id,
                value,
            )
        elif fault == "unpublished-source":
            await conn.execute(
                "UPDATE thread_workspace_provision_intents SET status='revoking',resolved_at=NULL WHERE thread_id=$1::uuid",
                owner.id,
            )
        else:
            column, value = {
                "replacement-pod": ("pod_uid", str(uuid4())),
                "replacement-resource": (
                    "pod_uid" if ephemeral else "pvc_uid",
                    str(uuid4()),
                ),
                "source-generation": ("runtime_generation", uuid4()),
            }[fault]
            await conn.execute(
                f"UPDATE thread_workspace_provision_intents SET {column}=$2 WHERE thread_id=$1::uuid",
                owner.id,
                value,
            )
    from orchestrator.database.postgres import (
        _settled_pinned_workspace_current_generation,
    )

    metadata = workspace.authority._json((await db.get_thread(owner.id))["metadata"])
    async with db.acquire() as conn:
        assert (
            await _settled_pinned_workspace_current_generation(
                conn,
                thread_id=UUID(owner.id),
                current_generation=UUID(current),
                workspace=metadata["workspace_container"],
                binding=metadata["_workspace_binding"],
            )
            is None
        )
    permanent = await db.begin_pinned_thread_retirement(owner.id, permanent=True)
    assert permanent["state"] == "malformed", permanent
    thread = await db.get_thread(owner.id)
    assert str(thread["runtime_generation"]) == current
    assert thread["runtime_retirement_token"] is None
    assert (
        workspace.authority._json(thread["metadata"])["workspace_container"]["status"]
        == "deleted"
    )
    assert effects == saved_effects
    assert not resources if ephemeral else "pvc" in resources
