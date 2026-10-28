"""A Job whose workspace Pod the cluster refuses at admission (roadmap item 18).

Real PostgreSQL with the full migration chain; only Kubernetes is faked. The
API server answers the Pod create with 403 and a v1 Status, so no Pod exists.
The Job fails at once with the cluster's message. The refusal ends the wait for
a late create at once, so both a delete right after the failure and the
background sweep settle the creation with no Pod, remove its PVC,
and let the row go.
"""

from __future__ import annotations

import json

import pytest

from orchestrator.services import container_provisioner as provisioner_module
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from tests import _b09_control_seams as control_seams
from tests import test_workspace_pull_failure_real_postgres as fixtures

db = fixtures.db
pg_dsn = fixtures.pg_dsn
_schema_applied = fixtures._schema_applied

REFUSAL = (
    'pods "workspace-x" is forbidden: maximum cpu usage per Container is 1, '
    "but limit is 2"
)


def _refusal(status: int = 403, *, body: bool = True):
    exc = fixtures.ApiException(status=status, reason="Forbidden")
    if body:
        exc.body = json.dumps(
            {
                "kind": "Status",
                "apiVersion": "v1",
                "status": "Failure",
                "message": REFUSAL,
                "reason": "Forbidden",
                "code": status,
            }
        ).encode()
    return exc


class RefusingCluster(fixtures.NeverPullingCluster):
    """Admission refuses the workspace Pod; the API server persists nothing."""

    def __init__(self, error=None):
        super().__init__()
        self.error = error or _refusal()

    def create_namespaced_pod(self, *, body, **_):
        raise self.error


async def _pod_effect(db, job):
    async with db.acquire() as conn:
        raw = await conn.fetchval(
            "SELECT external_effects->'pod' "
            "FROM managed_repository_workspace_creation_reservations "
            "WHERE owner_kind='job' AND owner_id=$1",
            job,
        )
    return json.loads(raw) if isinstance(raw, str) else raw


async def _effects_quiescent(db, job):
    async with db.acquire() as conn:
        generation = await conn.fetchval(
            "SELECT reservation_generation "
            "FROM managed_repository_workspace_creation_reservations "
            "WHERE owner_kind='job' AND owner_id=$1",
            job,
        )
    return await db.managed_repository_workspace_creation_effects_are_quiescent(
        str(job),
        owner_kind="job",
        scope="workspace_container",
        reservation_generation=int(generation),
    )


async def _refused_job(db, monkeypatch, cluster):
    job = await fixtures._job(db)
    p = fixtures._provisioner(monkeypatch, db, cluster)
    monkeypatch.setattr(provisioner_module, "container_provisioner", p)
    import orchestrator.main as main

    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    assert await p.create_workspace(WorkspaceOwner.job(str(job))) is False
    return job, p


@pytest.mark.asyncio
async def test_a_refused_pod_create_ends_the_wait_for_a_late_create(db, monkeypatch):
    cluster = RefusingCluster()
    job, _ = await _refused_job(db, monkeypatch, cluster)

    assert (await fixtures._workspace(db, job)) == {
        "error": f"Workspace pod was rejected by the cluster: {REFUSAL}"
    }
    effect = await _pod_effect(db, job)
    assert effect["observed_uid"] is None
    assert effect["refused_status"] == 403
    assert await _effects_quiescent(db, job)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        _refusal(500),
        _refusal(403, body=False),
        fixtures.ApiException(status=409, reason="AlreadyExists"),
        TimeoutError("no answer"),
    ],
    ids=["server-error", "no-status-body", "conflict", "timeout"],
)
async def test_an_answer_that_may_have_persisted_keeps_the_wait(db, monkeypatch, error):
    cluster = RefusingCluster(error)
    job, _ = await _refused_job(db, monkeypatch, cluster)

    effect = await _pod_effect(db, job)
    assert "refused_status" not in effect
    assert not await _effects_quiescent(db, job)


@pytest.mark.asyncio
async def test_a_refused_job_can_be_deleted_right_after_it_failed(db, monkeypatch):
    """DELETE /api/jobs/{id} settles the creation itself; no sweep needed."""

    cluster = RefusingCluster()
    job, _ = await _refused_job(db, monkeypatch, cluster)
    await fixtures._fail_like_the_dispatcher(db, job)
    assert set(cluster.objects) == {"pvc"}

    assert await control_seams.archive_and_cleanup_workspace(str(job)) == [
        "refused k8s workspace creation settled"
    ]
    assert cluster.objects == {}
    assert await fixtures._open_authority(db, job) == {
        "reservations": 0,
        "intents": 0,
    }
    async with db.acquire() as conn:
        assert await conn.execute("DELETE FROM jobs WHERE id=$1", job) == "DELETE 1"


@pytest.mark.asyncio
async def test_the_sweep_settles_a_refused_job(db, monkeypatch):
    cluster = RefusingCluster()
    job, p = await _refused_job(db, monkeypatch, cluster)
    await fixtures._fail_like_the_dispatcher(db, job)

    counts = await p.reconcile_pending_workspace_creation_reservations(limit=25)

    assert counts["aborted"] >= 1
    assert cluster.objects == {}
    assert (await fixtures._workspace(db, job))["error"].endswith(REFUSAL)
    async with db.acquire() as conn:
        assert await conn.execute("DELETE FROM jobs WHERE id=$1", job) == "DELETE 1"
