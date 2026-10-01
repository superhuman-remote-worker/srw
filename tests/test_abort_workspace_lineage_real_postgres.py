"""Exact pre-setup abort lineage retains cleanup of its same published workspace."""

from types import SimpleNamespace
from uuid import uuid4

import pytest

from orchestrator import main
from orchestrator.application import controls as controls_composition
from orchestrator.services.session_attach_binding import release_session_attach_binding
from tests import test_pinned_workspace_cleanup_real_postgres as workspace


db = workspace.db
pg_dsn = workspace.pg_dsn
_schema_applied = workspace._schema_applied


async def _rotated_soft_end(db, monkeypatch, *, ephemeral):
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
    await db.execute("UPDATE threads SET status='created' WHERE id=$1::uuid", owner.id)
    released = await release_session_attach_binding(
        ids["agent"],
        owner.id,
        expected_runtime_generation=initial,
        expected_attach_token=ids["attach_token"],
        expected_agent_pod_uid=metadata["agent_pod"]["pod_uid"],
        local_runtime_quiesced=True,
        local_quiescence_protocol="agent_attach_not_started_v1",
        workspace_generation=metadata["_workspace_binding"]["generation"],
        workspace_runtime_incarnation=pod_uid,
        dependencies=SimpleNamespace(store=db),
    )
    assert released == "released"
    rotated = await db.get_thread(owner.id)
    assert str(rotated["runtime_generation"]) != initial
    assert (
        await db.fetchval(
            "SELECT runtime_generation::text FROM thread_workspace_provision_intents WHERE thread_id=$1::uuid",
            owner.id,
        )
        == initial
    )
    successor, _actor = await workspace.authority._bind_replacement_agent(
        db,
        thread_id=owner.id,
        pod_uid=str(uuid4()),
        pod_name="successor-" + ids["thread"][:12],
    )
    current = await db.get_thread(owner.id)
    ids["agent"] = successor
    ids["attach_token"] = str(current["runtime_attach_token"])
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
