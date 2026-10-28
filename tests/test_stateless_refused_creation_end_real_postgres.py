"""A Session whose workspace Pod the cluster refused can still be Ended.

Roadmap item 18, Finding 2. Real PostgreSQL with the production schema
snapshot; only Kubernetes is faked. The API server answers the Session's one
Pod create with 403 and a v1 Status, so no Pod and no runtime UID ever exist.
End settles that creation with no Pod: soft End keeps the PVC (Resume stays
refused), permanent End removes it and the row.
"""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
from fastapi import HTTPException
import pytest

from orchestrator.services import container_provisioner as provisioner_module
from orchestrator.services import thread_retirement
from orchestrator.services.container_provisioner import ContainerProvisioner
from orchestrator.services.manifest_workspace_selection import (
    select_execution_workspace,
)
from orchestrator.services.session_provisioner import ensure_session_workspace
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from tests import test_stateless_failed_start_end_real_postgres as failed_start
from tests import test_stateless_workspace_continuation_real_postgres as continuation
from tests import test_stateless_workspace_upgrade_real_postgres as upgrade
from tests import test_workspace_pull_failure_real_postgres as pull_fixtures
from tests.test_sandbox_workspace_provisioner import stub_plan_inputs

actor = continuation.actor
database = continuation.database
postgres_url = continuation.postgres_url
metadata = continuation.metadata
end_case = failed_start.end_case
resume_case = failed_start.resume_case

REFUSAL = (
    'pods "workspace-x" is forbidden: maximum cpu usage per Container is 1, '
    "but limit is 2"
)


def _refusal():
    exc = pull_fixtures.ApiException(status=403, reason="Forbidden")
    exc.body = json.dumps(
        {
            "kind": "Status",
            "apiVersion": "v1",
            "status": "Failure",
            "message": REFUSAL,
            "reason": "Forbidden",
            "code": 403,
        }
    ).encode()
    return exc


class RefusingCluster(continuation.DelayedWorkspaceCluster):
    """Admission refuses the workspace Pod; the API server persists nothing."""

    def __init__(self, error=None):
        super().__init__()
        self.error = error or _refusal()

    def create_namespaced_pod(self, *, body, **_):
        self.pod_create_calls += 1
        raise self.error


class LingeringPvcCluster(RefusingCluster):
    """A deleted PVC stays Terminating for one more read, as on a real cluster."""

    def __init__(self):
        super().__init__()
        self.lingering = False

    def delete_namespaced_persistent_volume_claim(self, *, name, body=None, **_):
        pvc = self._read("pvc", name)
        if pull_fixtures._precondition(body) not in {None, pvc.metadata.uid}:
            raise pull_fixtures.ApiException(status=409, reason="Conflict")
        pvc.metadata.deletion_timestamp = "now"
        self.lingering = True

    def read_namespaced_persistent_volume_claim(self, *, name, **_):
        pvc = self._read("pvc", name)
        if self.lingering:
            self.lingering = False
        elif pvc.metadata.deletion_timestamp is not None:
            del self.objects["pvc"]
            raise pull_fixtures._missing()
        return pvc


def _refusing_provisioner(database, monkeypatch, cluster):
    provisioner = ContainerProvisioner()
    provisioner._db = database
    provisioner._k8s_available = True
    provisioner._namespace = "agent-workspaces"
    provisioner._storage_class = "test-storage"
    provisioner._pvc_enabled = True
    provisioner._core_api = cluster
    stub_plan_inputs(monkeypatch, provisioner)
    monkeypatch.setattr(
        provisioner,
        "_resolve_ide_seed_files",
        AsyncMock(return_value={"settings.json": {"content": "{}"}}),
    )
    for name in ("open_interval", "close_interval"):
        monkeypatch.setattr(
            provisioner_module.workspace_metering, name, AsyncMock(return_value=None)
        )
    return provisioner


async def _refused_case(database, thread_id, provisioner, cluster):
    suspension = SimpleNamespace()
    await ensure_session_workspace(
        thread_id, db=database, provisioner=provisioner, suspension=suspension
    )
    assert cluster.pod_create_calls == 1
    assert "pod" not in cluster.objects
    creation = await continuation.reservation(database, thread_id)
    assert creation["phase"] == "mutating"
    assert creation["runtime_incarnation"] is None
    workspace = metadata(await database.get_thread(thread_id))["workspace_container"]
    assert workspace["status"] == "pending"
    assert workspace["_runtime_creation"]["attempted"] is True
    assert "_runtime_incarnation" not in workspace
    return SimpleNamespace(
        thread_id=thread_id,
        owner=WorkspaceOwner.session(thread_id),
        provisioner=provisioner,
        cluster=cluster,
        suspension=suspension,
        creation=creation,
        pvc_uid=cluster.objects["pvc"].metadata.uid,
    )


async def refused_attempt(database, actor, monkeypatch, *, cluster=None):
    workspace, selection = await select_execution_workspace(
        database,
        actor,
        role="session",
        project_id=None,
        supplied=True,
        workspace={
            "template": {
                "inline": {
                    "backend": "sandbox",
                    "resources": {"cpu": 2, "memory": "3Gi", "storage": "15Gi"},
                    "environment": {
                        "image": continuation.IMAGE,
                        "pullPolicy": "IfNotPresent",
                    },
                }
            }
        },
    )
    thread_id = await database.create_thread(
        user_id=str(actor["id"]),
        datasource_ids=[],
        execution_lane="stateless",
        initial_metadata={
            "config_override": {"workspace": workspace},
            "workspace_container": {
                "status": "pending",
                "provisioner": "k8s",
                "_runtime_creation": {
                    "generation": str(uuid4()),
                    "mode": "create",
                    "attempted": False,
                    "replaces_uid": None,
                },
            },
        },
        workspace_selection=selection,
    )
    cluster = cluster or RefusingCluster()
    provisioner = _refusing_provisioner(database, monkeypatch, cluster)
    return await _refused_case(database, thread_id, provisioner, cluster)


async def refused_upgrade(database, actor, monkeypatch, *, earlier_turn=None):
    """A lite Session upgraded in place (C1) whose container create is refused."""
    thread_id = await upgrade.lite_session(database, actor)
    if earlier_turn is not None:
        await earlier_turn(thread_id)
    await upgrade.admit_upgrade(database, actor, thread_id)
    cluster = RefusingCluster()
    provisioner = _refusing_provisioner(database, monkeypatch, cluster)
    return await _refused_case(database, thread_id, provisioner, cluster)


async def _pod_ambiguity(database, thread_id, seconds):
    await database.execute(
        "UPDATE managed_repository_workspace_creation_reservations "
        "SET external_effects = jsonb_set(external_effects, "
        "'{pod,ambiguity_until}', to_jsonb((now() + make_interval(secs => $2))"
        "::text)) WHERE owner_kind = 'thread' AND owner_id = $1::uuid",
        thread_id,
        float(seconds),
    )


async def _receipts(database, thread_id):
    return await database.fetchval(
        "SELECT count(*) FROM managed_repository_process_zero_receipts "
        "WHERE owner_kind='thread' AND owner_id=$1::uuid",
        thread_id,
    )


@pytest.mark.asyncio
async def test_soft_end_settles_the_refused_creation_and_keeps_its_volume(
    database, actor, monkeypatch
):
    case = await refused_attempt(database, actor, monkeypatch)
    assert sorted(case.cluster.objects) == ["pvc", "seed"]

    assert await end_case(database, case) == {"status": "ended"}

    # The seed went with the creation; the PVC stays, as for any soft End.
    assert sorted(case.cluster.objects) == ["pvc"]
    assert case.cluster.objects["pvc"].metadata.uid == case.pvc_uid
    assert case.cluster.pod_create_calls == 1
    ended = await database.get_thread(case.thread_id)
    assert ended["status"] == "ended"
    meta = metadata(ended)
    workspace = meta["workspace_container"]
    assert "_runtime_creation" not in workspace
    assert workspace["status"] == "deleted"
    assert workspace["_runtime_incarnation"] is None
    assert workspace["volume_reclaimed"] is False
    settled = meta["_stateless_workspace_retirement_settled"]
    assert settled["unpublished_creation"] == {
        "generation": str(ended["runtime_generation"]),
        "reservation_id": str(case.creation["id"]),
        "reservation_generation": case.creation["reservation_generation"],
        "namespace": "agent-workspaces",
    }
    assert settled["runtime_incarnation"] is None
    assert settled["backing_id"] is None
    creation = await continuation.reservation(database, case.thread_id)
    assert (creation["phase"], creation["result_kind"]) == ("aborted", "aborted")
    assert creation["cancel_resource_policy"] == "preserve"
    assert await _receipts(database, case.thread_id) == 0

    # A duplicate End changes nothing.
    assert await end_case(database, case) == {"status": "ended"}
    assert await database.get_thread(case.thread_id) == ended


@pytest.mark.asyncio
async def test_permanent_end_removes_the_refused_creation_and_the_row(
    database, actor, monkeypatch
):
    case = await refused_attempt(database, actor, monkeypatch)

    assert await end_case(database, case, permanent=True) == {"status": "deleted"}

    assert case.cluster.objects == {}
    assert await database.get_thread(case.thread_id) is None


@pytest.mark.asyncio
async def test_permanent_end_after_soft_end_reclaims_the_kept_volume(
    database, actor, monkeypatch
):
    case = await refused_attempt(database, actor, monkeypatch)
    assert await end_case(database, case) == {"status": "ended"}
    assert "pvc" in case.cluster.objects

    assert await end_case(database, case, permanent=True) == {"status": "deleted"}

    assert case.cluster.objects == {}
    assert await database.get_thread(case.thread_id) is None


@pytest.mark.asyncio
async def test_the_kept_volume_holds_the_row_against_a_raw_delete(
    database, actor, monkeypatch
):
    """The delete guard reads the refused creation as metadata only once its
    storage is reclaimed (migration 0561)."""
    case = await refused_attempt(database, actor, monkeypatch)
    assert await end_case(database, case) == {"status": "ended"}

    async with database.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError) as refused:
            await conn.execute(
                "DELETE FROM threads WHERE id = $1::uuid", case.thread_id
            )

    assert refused.value.constraint_name == (
        "managed_repository_legacy_workspace_cleanup_required_before_owner_delete"
    )


@pytest.mark.asyncio
async def test_resume_after_soft_end_is_a_clear_refusal(database, actor, monkeypatch):
    """Like a Session Ended after a failed upgrade (roadmap item 10)."""
    case = await refused_attempt(database, actor, monkeypatch)
    assert await end_case(database, case) == {"status": "ended"}
    ended = await database.get_thread(case.thread_id)

    with pytest.raises(HTTPException) as refused:
        await resume_case(database, case, actor)

    assert refused.value.status_code == 409
    assert "refused this Session's workspace" in refused.value.detail
    assert await database.get_thread(case.thread_id) == ended


@pytest.mark.asyncio
async def test_permanent_end_waits_out_a_terminating_volume(
    database, actor, monkeypatch
):
    sleep = AsyncMock()
    monkeypatch.setattr(thread_retirement.asyncio, "sleep", sleep)
    case = await refused_attempt(
        database, actor, monkeypatch, cluster=LingeringPvcCluster()
    )

    assert await end_case(database, case, permanent=True) == {"status": "deleted"}

    sleep.assert_awaited_once_with(2)
    assert case.cluster.objects == {}
    assert await database.get_thread(case.thread_id) is None


@pytest.mark.asyncio
async def test_permanent_end_after_soft_end_waits_out_a_terminating_volume(
    database, actor, monkeypatch
):
    """Seen live on k3d: the kept PVC stays Terminating for a moment."""
    sleep = AsyncMock()
    monkeypatch.setattr(thread_retirement.asyncio, "sleep", sleep)
    case = await refused_attempt(
        database, actor, monkeypatch, cluster=LingeringPvcCluster()
    )
    assert await end_case(database, case) == {"status": "ended"}
    assert "pvc" in case.cluster.objects

    assert await end_case(database, case, permanent=True) == {"status": "deleted"}

    sleep.assert_awaited_once_with(2)
    assert case.cluster.objects == {}
    assert await database.get_thread(case.thread_id) is None


@pytest.mark.asyncio
async def test_a_refusal_from_before_its_record_ends_once_its_window_expired(
    database, actor, monkeypatch
):
    """A row refused before the refusal was recorded (k3d, 2026-10-14) only
    has its Pod effect's 90 s ambiguity window, which End waits out."""
    monkeypatch.setattr(ContainerProvisioner, "_record_refused_pod_create", AsyncMock())
    case = await refused_attempt(database, actor, monkeypatch)
    before = await database.get_thread(case.thread_id)

    with pytest.raises(HTTPException) as held:
        await end_case(database, case)
    assert held.value.status_code == 503
    assert await database.get_thread(case.thread_id) == before
    assert (await continuation.reservation(database, case.thread_id))[
        "cancel_requested_at"
    ] is None

    await _pod_ambiguity(database, case.thread_id, -1)
    assert await end_case(database, case) == {"status": "ended"}
    assert sorted(case.cluster.objects) == ["pvc"]


@pytest.mark.asyncio
async def test_a_create_that_may_still_land_keeps_end_held(
    database, actor, monkeypatch
):
    """A timeout may have stored the Pod: End changes nothing until its
    window ends."""
    case = await refused_attempt(
        database,
        actor,
        monkeypatch,
        cluster=RefusingCluster(TimeoutError("no answer")),
    )
    before = await database.get_thread(case.thread_id)

    assert (
        await case.provisioner.capture_unpublished_stateless_creation_retirement(
            case.owner
        )
        is None
    )
    with pytest.raises(HTTPException) as held:
        await end_case(database, case, permanent=True)

    assert held.value.status_code == 503
    assert await database.get_thread(case.thread_id) == before
    assert sorted(case.cluster.objects) == ["pvc", "seed"]


@pytest.mark.asyncio
async def test_a_pod_under_the_workspace_name_refuses_the_end(
    database, actor, monkeypatch
):
    case = await refused_attempt(database, actor, monkeypatch)
    case.cluster.objects["pod"] = SimpleNamespace(
        metadata=SimpleNamespace(name=case.owner.pod_name, uid=str(uuid4()))
    )
    before = await database.get_thread(case.thread_id)

    assert (
        await case.provisioner.capture_unpublished_stateless_creation_retirement(
            case.owner
        )
        is None
    )
    # The ensure pass after a refused capture is today's path; only the
    # refusal before any lifecycle effect matters here.
    monkeypatch.setattr(thread_retirement, "ensure_session_workspace", AsyncMock())
    with pytest.raises(HTTPException) as held:
        await end_case(database, case)

    assert held.value.status_code == 503
    assert await database.get_thread(case.thread_id) == before
    assert sorted(case.cluster.objects) == ["pod", "pvc", "seed"]


@pytest.mark.asyncio
async def test_begin_refuses_a_creation_whose_pod_effect_was_observed(
    database, actor, monkeypatch
):
    """The capture's ledger facts are rechecked under Begin's locks."""
    case = await refused_attempt(database, actor, monkeypatch)
    unpublished = (
        await case.provisioner.capture_unpublished_stateless_creation_retirement(
            case.owner
        )
    )
    assert unpublished is not None
    await database.execute(
        "UPDATE managed_repository_workspace_creation_reservations "
        "SET external_effects = jsonb_set(external_effects, "
        "'{pod,observed_uid}', to_jsonb($2::text)) "
        "WHERE owner_kind = 'thread' AND owner_id = $1::uuid",
        case.thread_id,
        str(uuid4()),
    )
    before = await database.get_thread(case.thread_id)

    with pytest.raises(RuntimeError):
        await database.begin_stateless_thread_workspace_retirement(
            case.thread_id, force=True, unpublished_creation=unpublished
        )

    assert await database.get_thread(case.thread_id) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("permanent", [False, True])
@pytest.mark.parametrize("served_lite_turn", [False, True])
async def test_forced_end_retires_a_refused_upgrade_create(
    database, actor, monkeypatch, permanent, served_lite_turn
):
    """C1: a Session upgraded in place from Virtual can hit the same refusal.
    Its unconsumed upgrade control makes a normal End busy, as for any
    upgrade create. A served lite turn leaves its claim record and an
    awaiting_user status, which the End admits as the initial End does."""
    earlier_turn = None
    if served_lite_turn:
        claimant = upgrade.upgrade_claimant(
            database, monkeypatch, attach=upgrade.LITE_ATTACH
        )

        async def earlier_turn(thread_id):
            await upgrade.run_a_lite_turn(database, thread_id, claimant=claimant)

    case = await refused_upgrade(
        database, actor, monkeypatch, earlier_turn=earlier_turn
    )
    if served_lite_turn:
        served = await database.get_thread(case.thread_id)
        assert served["status"] == "awaiting_user"
        assert "_stateless_active_claim" in metadata(served)

    with pytest.raises(HTTPException) as busy:
        await upgrade.end(database, case, force=False)
    assert busy.value.status_code == 409

    assert await upgrade.end(database, case, force=True, permanent=permanent) == {
        "status": "deleted" if permanent else "ended"
    }
    if permanent:
        assert case.cluster.objects == {}
        assert await database.get_thread(case.thread_id) is None
    else:
        assert sorted(case.cluster.objects) == ["pvc"]
        workspace = metadata(await database.get_thread(case.thread_id))[
            "workspace_container"
        ]
        assert workspace["seed_from"]["backend"] == "virtual"
        assert "_runtime_creation" not in workspace
