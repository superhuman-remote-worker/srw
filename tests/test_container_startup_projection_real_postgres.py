"""The public startup view reads only exact current container receipts."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from orchestrator.database.container_startup_stage import (
    ScheduledAt,
    StageBudgets,
    StartupAttention,
    Unscheduled,
)
from tests import test_container_startup_stage_real_postgres as fixtures

db = fixtures.db
pg_dsn = fixtures.pg_dsn
_schema_applied = fixtures._schema_applied


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_kind", ["job", "thread"])
async def test_exact_current_receipt_projects_wait_and_frozen_hard_deadline(
    db, owner_kind
):
    if owner_kind == "job":
        owner_id, receipt, pod_uid, _ = await fixtures._bound_job(db)
    else:
        owner_id, _, receipt, pod_uid = await fixtures._bound_thread(db)
    other_id = fixtures.uuid4()
    kwargs = dict(
        owner_kind=owner_kind,
        owner_id=str(owner_id),
        reservation_id=str(receipt["id"]),
        claim_token=receipt["claim_token"],
        pod_uid=pod_uid,
    )
    assert await db.observe_container_startup(
        **kwargs,
        observation=Unscheduled("scheduler_unschedulable"),
        adopt_if_unmarked=True,
    )
    view = await db.container_workspace_creation_views(
        owner_kind, [str(owner_id), str(other_id)]
    )
    assert view == {
        owner_id: {
            "stage": "scheduling",
            "state": "waiting_capacity",
            "reason_code": "scheduler_unschedulable",
            "readiness_deadline_at": None,
        }
    }
    scheduled = datetime.now(timezone.utc)
    assert await db.observe_container_startup(
        **kwargs,
        observation=ScheduledAt(scheduled),
        budgets=StageBudgets(ready_seconds=180, pull_seconds=300, ssh_seconds=40),
    )
    view = (await db.container_workspace_creation_views(owner_kind, [owner_id]))[
        owner_id
    ]
    assert view == {
        "stage": "readiness",
        "state": "starting",
        "reason_code": "scheduled",
        "readiness_deadline_at": scheduled + timedelta(seconds=300),
    }


@pytest.mark.asyncio
async def test_attention_is_allowlisted_without_private_authority(db):
    owner_id, receipt, pod_uid, _ = await fixtures._bound_job(db)
    kwargs = fixtures._observe_kwargs(owner_id, receipt, pod_uid)
    assert await db.observe_container_startup(
        **kwargs, observation=Unscheduled("scheduling_other"), adopt_if_unmarked=True
    )
    assert await db.observe_container_startup(
        **kwargs,
        observation=ScheduledAt(datetime.now(timezone.utc)),
        budgets=StageBudgets(ready_seconds=180, pull_seconds=None, ssh_seconds=30),
    )
    assert await db.observe_container_startup(
        **kwargs, observation=StartupAttention("invalid_image")
    )
    view = (await db.container_workspace_creation_views("job", [owner_id]))[owner_id]
    assert set(view) == {
        "stage",
        "state",
        "reason_code",
        "readiness_deadline_at",
    }
    assert view["reason_code"] == "invalid_image"


@pytest.mark.asyncio
async def test_expired_lease_does_not_hide_valid_wait_and_rotation_keeps_view(db):
    owner_id, receipt, pod_uid, _ = await fixtures._bound_job(db)
    assert await db.observe_container_startup(
        **fixtures._observe_kwargs(owner_id, receipt, pod_uid),
        observation=Unscheduled("scheduler_unschedulable"),
        adopt_if_unmarked=True,
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE managed_repository_workspace_creation_reservations "
            "SET created_at=clock_timestamp()-interval '1 hour', "
            "expires_at=clock_timestamp()-interval '1 second' WHERE id=$1",
            receipt["id"],
        )
    before = await db.container_workspace_creation_views("job", [owner_id])
    assert before[owner_id]["state"] == "waiting_capacity"
    rotated = await db.reserve_managed_repository_workspace_creation(
        str(owner_id),
        owner_kind="job",
        scope="workspace_container",
        claimant="projection-successor",
        desired_manifest_digest="a" * 64,
        expected_existing_reservation_id=str(receipt["id"]),
        expected_existing_claim_token=int(receipt["claim_token"]),
    )
    assert rotated is not None and rotated["claim_token"] != receipt["claim_token"]
    assert await db.container_workspace_creation_views("job", [owner_id]) == before


@pytest.mark.asyncio
async def test_old_settled_creating_job_projects_only_legacy_hold(db):
    owner_id, receipt, pod_uid, gate = await fixtures._bound_job(db)
    assert await db.settle_managed_repository_workspace_creation_reservation(
        str(owner_id), **gate, runtime_incarnation=pod_uid
    )
    assert await db.container_workspace_creation_views("job", [owner_id]) == {
        owner_id: {
            "stage": "scheduling",
            "state": "observing",
            "reason_code": "legacy_receipt_held",
            "readiness_deadline_at": None,
        }
    }


@pytest.mark.asyncio
async def test_old_settled_session_does_not_inherit_job_only_legacy_hold(db):
    owner_id, _, receipt, pod_uid = await fixtures._bound_thread(db)
    assert await db.settle_managed_repository_workspace_creation_reservation(
        str(owner_id),
        owner_kind="thread",
        scope="workspace_container",
        reservation_generation=receipt["reservation_generation"],
        claimant="session-startup-test",
        claim_token=receipt["claim_token"],
        runtime_incarnation=pod_uid,
    )
    assert await db.container_workspace_creation_views("thread", [owner_id]) == {}


@pytest.mark.asyncio
async def test_unmarked_open_receipt_is_observing_without_adoption(db):
    owner_id, receipt, _, _ = await fixtures._bound_job(db)
    assert await db.container_workspace_creation_views("job", [owner_id]) == {
        owner_id: {
            "stage": "scheduling",
            "state": "observing",
            "reason_code": "observation_pending",
            "readiness_deadline_at": None,
        }
    }
    assert (await fixtures._reservation(db, receipt))[
        "startup_protocol_version"
    ] is None


@pytest.mark.asyncio
async def test_terminal_job_does_not_expose_open_creation(db):
    owner_id, _, _, _ = await fixtures._bound_job(db)
    assert owner_id in await db.container_workspace_creation_views("job", [owner_id])
    async with db.acquire() as conn:
        await conn.execute("UPDATE jobs SET status='cancelled' WHERE id=$1", owner_id)
    assert await db.container_workspace_creation_views("job", [owner_id]) == {}
