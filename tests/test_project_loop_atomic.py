"""Unit contracts for the project-loop atomic Class-C transaction."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from orchestrator.services.project_loop_atomic import (
    LoopAdvanceExpectation,
    LoopAdvanceMutation,
    materialize_loop_advance_atomic,
    plan_loop_advance,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def _loop(*, stage_ids: list[str], **over):
    row = {
        "id": str(uuid4()),
        "status": "running",
        "scheduling": "standard",
        "role_sequence": ["scholar", "critic", "developer"],
        "seq_index": 0,
        "total_jobs_run": 1,
        "remaining_iterations": 5,
        "max_iterations": 6,
        "run_until": None,
        "max_consecutive_failures": 3,
        "consecutive_failures": 0,
        "current_job_id": stage_ids[0] if len(stage_ids) == 1 else None,
        "current_stage_jobs": stage_ids,
        "campaign": None,
        "campaign_history": [],
        "project_id": str(uuid4()),
        "owner_id": None,
    }
    row.update(over)
    return row


class _DB:
    def __init__(self, loop, states):
        self.loop = loop
        self.states = states
        self.lock_project_loop_for_advance = AsyncMock(return_value=loop)
        self.lock_loop_stage_member_statuses = AsyncMock(return_value=states)
        self.update_project_loop = AsyncMock(
            side_effect=lambda _loop_id, **fields: {**loop, **fields}
        )
        self.merge_job_context = AsyncMock(return_value=True)
        self.transaction_entries = 0

    @asynccontextmanager
    async def transaction_scope(self):
        self.transaction_entries += 1
        yield object()


@pytest.mark.asyncio
async def test_world_cas_miss_returns_supersede_shape_without_materializing():
    member_id = str(uuid4())
    observed = _loop(stage_ids=[member_id])
    expected = LoopAdvanceExpectation.from_rows(observed, {member_id: "completed"})
    moved = {**observed, "current_stage_jobs": [str(uuid4())]}
    db = _DB(moved, {member_id: "completed"})
    create = AsyncMock()

    result = await materialize_loop_advance_atomic(
        db,
        loop_id=observed["id"],
        member_job_id=member_id,
        expected=expected,
        mutation=LoopAdvanceMutation(
            stage="critic",
            seq_index=1,
            remaining_iterations=4,
            consecutive_failures=0,
            last_error=None,
        ),
        create_job_fn=create,
    )

    assert result == {
        "won": False,
        "reason": "loop_world_changed",
        "loop_id": observed["id"],
        "completed_member_id": member_id,
        "spawned_job_ids": [],
        "replay": {},
    }
    create.assert_not_awaited()
    db.update_project_loop.assert_not_awaited()


@pytest.mark.asyncio
async def test_fanout_jobs_and_campaign_handoff_share_pointer_transaction():
    member_id = str(uuid4())
    observed = _loop(
        stage_ids=[member_id],
        scheduling="campaign",
        campaign={"id": "campaign-a", "cursor": 1},
    )
    states = {member_id: "completed"}
    expected = LoopAdvanceExpectation.from_rows(observed, states)
    db = _DB(observed, states)
    ids = [str(uuid4()), str(uuid4())]
    create = AsyncMock(
        side_effect=[
            {"id": ids[0], "status": "created"},
            {"id": ids[1], "status": "created"},
        ]
    )
    campaign = {"id": "campaign-a", "cursor": 2, "status": "active"}

    result = await materialize_loop_advance_atomic(
        db,
        loop_id=observed["id"],
        member_job_id=member_id,
        expected=expected,
        mutation=LoopAdvanceMutation(
            stage=["scholar", "product-qa"],
            seq_index=1,
            remaining_iterations=4,
            consecutive_failures=0,
            last_error=None,
            campaign_changed=True,
            campaign=campaign,
            extra_context={"loop_campaign_id": "campaign-a"},
            park_until=datetime(2030, 1, 1, tzinfo=timezone.utc),
            replay={"notify": "campaign-next"},
        ),
        backlog_block="prepared backlog",
        history_block="prepared history",
        create_job_fn=create,
    )

    assert result["won"] is True
    assert result["spawned_job_ids"] == ids
    assert result["replay"] == {"notify": "campaign-next"}
    assert db.transaction_entries == 1
    update = db.update_project_loop.await_args.kwargs
    assert update["current_stage_jobs"] == ids
    assert update["current_job_id"] is None
    assert update["total_jobs_run"] == 3
    assert update["campaign"] == campaign
    assert create.await_count == 2
    for call in create.await_args_list:
        assert call.kwargs["backlog_block"] == "prepared backlog"
        assert call.kwargs["history_block"] == "prepared history"
        assert call.kwargs["extra_context"] == {"loop_campaign_id": "campaign-a"}
    marker = db.merge_job_context.await_args.args[1]["_project_loop_advance_handoff"]
    assert marker["state"] == "pending"
    assert marker["output"] == {"applicable": True, **result}


@pytest.mark.asyncio
async def test_stop_settles_pointers_without_creating_a_job():
    member_id = str(uuid4())
    observed = _loop(stage_ids=[member_id])
    states = {member_id: "failed"}
    expected = LoopAdvanceExpectation.from_rows(observed, states)
    db = _DB(observed, states)
    create = AsyncMock()

    result = await materialize_loop_advance_atomic(
        db,
        loop_id=observed["id"],
        member_job_id=member_id,
        expected=expected,
        mutation=LoopAdvanceMutation(
            stage=None,
            seq_index=0,
            remaining_iterations=0,
            consecutive_failures=1,
            last_error="job failed",
            status="completed",
            stop_reason="budget",
        ),
        create_job_fn=create,
    )

    assert result["won"] is True
    assert result["spawned_job_ids"] == []
    create.assert_not_awaited()
    update = db.update_project_loop.await_args.kwargs
    assert update["status"] == "completed"
    assert update["current_stage_jobs"] == []
    assert update["current_job_id"] is None
    assert update["stop_reason"] == "budget"


def test_officer_plan_clears_turn_and_persists_wake_identity():
    member_id = str(uuid4())
    loop = _loop(
        stage_ids=[member_id],
        scheduling="officer",
        seq_index=7,
        remaining_iterations=None,
        max_iterations=None,
    )
    mutation = plan_loop_advance(
        loop,
        completed_job={"id": member_id},
        completed_context={"loop_id": loop["id"]},
        member_states={member_id: "completed"},
        failed=False,
        member_error=None,
        deadline_passed=False,
    )

    assert mutation.stage is None
    assert mutation.status == "running"
    assert mutation.replay["action"] == {"kind": "officer"}
    assert mutation.replay["officer"]["dedup_key"].endswith(":7")


def test_campaign_member_plan_advances_cursor_with_spawn_stamp():
    member_id = str(uuid4())
    campaign = {
        "id": "campaign-1",
        "title": "Ship it",
        "status": "active",
        "cursor": 1,
        "stages_done": 0,
        "member_failures": 0,
        "stages": [{"role": "developer"}, {"role": "critic"}],
    }
    loop = _loop(
        stage_ids=[member_id],
        scheduling="campaign",
        role_sequence=["critic", "developer"],
        seq_index=1,
        campaign=campaign,
    )
    mutation = plan_loop_advance(
        loop,
        completed_job={"id": member_id},
        completed_context={
            "loop_id": loop["id"],
            "loop_campaign_id": "campaign-1",
            "loop_campaign_index": 0,
        },
        member_states={member_id: "completed"},
        failed=False,
        member_error=None,
        deadline_passed=False,
    )

    assert mutation.stage == "critic"
    assert mutation.seq_index == 1
    assert mutation.campaign_changed is True
    assert mutation.campaign["cursor"] == 2
    assert mutation.campaign["stages_done"] == 1
    assert mutation.extra_context == {
        "loop_campaign_id": "campaign-1",
        "loop_campaign_index": 1,
    }


def test_cooldown_plan_born_parks_next_fanout_and_records_notification():
    member_id = str(uuid4())
    park_until = datetime.now(timezone.utc) + timedelta(hours=1)
    loop = _loop(
        stage_ids=[member_id],
        role_sequence=["critic", ["scholar", "product-qa"]],
        seq_index=0,
    )
    mutation = plan_loop_advance(
        loop,
        completed_job={"id": member_id},
        completed_context={"loop_id": loop["id"]},
        member_states={member_id: "failed"},
        failed=True,
        member_error="quota cooldown",
        deadline_passed=False,
        park_until=park_until,
    )

    assert mutation.stage == ["scholar", "product-qa"]
    assert mutation.park_until == park_until
    assert mutation.consecutive_failures == 1
    notice = mutation.replay["notifications"][0]
    assert notice["event_type"] == "loop_cooldown_park"
    assert park_until.isoformat() in notice["message"]


def test_oversized_member_error_is_bounded_with_audit_count():
    member_id = str(uuid4())
    loop = _loop(stage_ids=[member_id])
    huge_error = "é" * 20_000
    mutation = plan_loop_advance(
        loop,
        completed_job={"id": member_id},
        completed_context={"loop_id": loop["id"]},
        member_states={member_id: "failed"},
        failed=True,
        member_error=huge_error,
        deadline_passed=False,
    )

    record = mutation.replay["record_member"]
    assert len(record["last_error"].encode("utf-8")) <= 1024
    assert record["last_error_truncation"] == {
        "original_bytes": len(huge_error.encode("utf-8")),
        "retained_bytes": len(record["last_error"].encode("utf-8")),
    }
    # S32 persists this replay projection plus fixed IDs/counters under the
    # finalizer's 8 KiB correctness cap.
    assert len(json.dumps(dict(mutation.replay)).encode("utf-8")) < 8192


def test_vector_idempotency_ledgers_are_current_and_in_generated_snapshot():
    migrations = sorted(
        (
            REPO_ROOT / "src" / "orchestrator" / "database" / "migrations" / "vector"
        ).glob("*.sql")
    )
    assert migrations[-1].name == "0027_memory_search_includes_pinned_rows.sql"
    snapshot = (
        REPO_ROOT / "src/orchestrator/database/vector_schema_current.sql"
    ).read_text()
    assert "CREATE TABLE public.project_loop_ttl_effects" in snapshot
    assert "project_loop_ttl_effects_pkey" in snapshot
    assert "CREATE TABLE public.session_memory_effect_executions" in snapshot
    assert "session_memory_effect_executions_pkey" in snapshot
    # 0020 (B2): ready authorization for backlog tickets. 0021 (BP-06):
    # stable keyset ordering for exhaustive Officer scans.
    assert "ready_at timestamp with time zone" in snapshot
    # 0022 (WP3/H3): wedge detector streak columns on the watermark row.
    assert "error_streak integer DEFAULT 0 NOT NULL" in snapshot
    # 0024 (S1): the trigram index kb_grep's ILIKE path plans against — a
    # regenerated snapshot missing it means `.notx.sql` migrations were skipped.
    assert "idx_knowledge_content_trgm" in snapshot
    # 0025: the multi-angle ranking function search_chunks dispatches to.
    assert "knowledge_chunk_multi_angle_search" in snapshot
    # 0027: the memory search functions no longer skip TTL-pinned rows.
    assert "remaining_turns <= 0" not in snapshot
