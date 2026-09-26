"""Initial stateless creation failure must remain normally endable.

The Session is admitted and provisioned by production helpers against the
production PostgreSQL schema snapshot. Kubernetes follows finalizer retention.
"""

from dataclasses import fields
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from orchestrator.services.session_class_policy import require_stateless_end_workspace
from orchestrator.services.thread_retirement import (
    ThreadRetirementDependencies,
    end_thread_flow,
)
from tests import test_stateless_workspace_continuation_real_postgres as continuation

actor = continuation.actor
database = continuation.database
postgres_url = continuation.postgres_url
metadata = continuation.metadata
workspace_attempt = continuation.workspace_attempt


def retirement_dependencies(database, case):
    values = {field.name: None for field in fields(ThreadRetirementDependencies)}
    values.update(
        store=database,
        container_provisioner=case.provisioner,
        workspace_suspension_service=case.suspension,
        pinned_retirement=Mock(),
        require_stateless_end_workspace=require_stateless_end_workspace,
        conclude_conference_if_any=AsyncMock(),
        snapshot_service=SimpleNamespace(is_available=False),
        logger=logging.getLogger(__name__),
    )
    return ThreadRetirementDependencies(**values)


@pytest.mark.asyncio
@pytest.mark.parametrize("permanent", [False, True])
@pytest.mark.parametrize("pvc_enabled", [False, True])
async def test_normal_end_retires_exact_initial_pull_failed_workspace(
    database, actor, monkeypatch, permanent, pvc_enabled
):
    case = await workspace_attempt(
        database, actor, monkeypatch, pvc_enabled=pvc_enabled
    )
    result = await end_thread_flow(
        case.thread_id,
        case.before,
        permanent=permanent,
        force=False,
        dependencies=retirement_dependencies(database, case),
    )
    assert result == {"status": "deleted" if permanent else "ended"}
    assert "pod" not in case.cluster.objects
    assert "service" not in case.cluster.objects
    if permanent:
        assert "pvc" not in case.cluster.objects
        assert await database.get_thread(case.thread_id) is None
    else:
        if pvc_enabled:
            assert case.cluster.objects["pvc"].metadata.uid == case.pvc_uid
        else:
            assert case.cluster.objects == {}
        ended = await database.get_thread(case.thread_id)
        assert ended["status"] == "ended"
        workspace = metadata(ended)["workspace_container"]
        assert "_runtime_creation" not in workspace
        assert (
            metadata(ended)["_stateless_workspace_retirement_settled"][
                "runtime_incarnation"
            ]
            == case.pod_uid
        )
    receipts = await database.fetch(
        "SELECT * FROM managed_repository_process_zero_receipts "
        "WHERE owner_kind='thread' AND owner_id=$1::uuid "
        "AND scope='stateless_workspace'",
        case.thread_id,
    )
    assert len(receipts) == 1
    assert str(receipts[0]["runtime_incarnation"]) == case.pod_uid


async def end_case(database, case, *, permanent=False):
    return await end_thread_flow(
        case.thread_id,
        await database.get_thread(case.thread_id),
        permanent=permanent,
        force=False,
        dependencies=retirement_dependencies(database, case),
    )


@pytest.mark.asyncio
async def test_soft_end_duplicate_and_permanent_upgrade_retain_exact_storage_authority(
    database, actor, monkeypatch
):
    case = await workspace_attempt(database, actor, monkeypatch)
    assert await end_case(database, case) == {"status": "ended"}
    settled = await database.get_thread(case.thread_id)
    assert await end_case(database, case) == {"status": "ended"}
    assert await database.get_thread(case.thread_id) == settled
    assert case.cluster.objects["pvc"].metadata.uid == case.pvc_uid
    assert await end_case(database, case, permanent=True) == {"status": "deleted"}
    assert case.cluster.objects == {}
    assert await database.get_thread(case.thread_id) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "permanent,upgrade", [(False, False), (True, False), (True, True)]
)
async def test_disconnected_end_finishes_through_background_full_end(
    database, actor, monkeypatch, permanent, upgrade
):
    import asyncio
    from orchestrator.services.stale_agent_detector import (
        retry_initial_creation_retirement,
    )
    from orchestrator.services.thread_retirement import ThreadRetirementOperations

    case = await workspace_attempt(database, actor, monkeypatch)
    if upgrade:
        await end_case(database, case)
    begin = type(database).begin_stateless_thread_workspace_retirement

    async def disconnected(store, *args, **kwargs):
        await begin(store, *args, **kwargs)
        raise asyncio.CancelledError()

    with monkeypatch.context() as patch:
        patch.setattr(
            type(database), "begin_stateless_thread_workspace_retirement", disconnected
        )
        with pytest.raises(asyncio.CancelledError):
            await end_case(database, case, permanent=permanent)
    pending = await database.get_thread(case.thread_id)
    assert pending["status"] == "ended"
    if not upgrade:
        authority = metadata(pending)["_stateless_claim_retirement"]
        assert authority["remote_retired"] is False
        assert authority["residents_retired"] is False
        assert "pod" in case.cluster.objects
    candidates = await database.list_retryable_initial_creation_retirements(limit=25)
    assert len(candidates) == 1
    # A restart has no in-memory creator or captured observation. The Pod may
    # even start after Begin; the admitted UID must still be terminated.
    from orchestrator.services.container_provisioner import ContainerProvisioner

    previous = case.provisioner
    case.provisioner = ContainerProvisioner()
    for attribute in (
        "_db",
        "_core_api",
        "_k8s_available",
        "_namespace",
        "_storage_class",
        "_pvc_enabled",
    ):
        setattr(case.provisioner, attribute, getattr(previous, attribute))
    if not permanent:
        case.cluster.become_ready()
    mutation = case.provisioner._bounded_kubernetes_mutation
    finalizer_releases = []

    async def observe_finalizer_release(call, *args, **kwargs):
        if getattr(call, "__name__", "") == "patch_namespaced_pod":
            receipt = await database.fetchval(
                "SELECT EXISTS(SELECT 1 FROM managed_repository_process_zero_receipts "
                "WHERE owner_kind='thread' AND owner_id=$1::uuid "
                "AND scope='stateless_workspace' AND runtime_incarnation=$2)",
                case.thread_id,
                case.pod_uid,
            )
            assert receipt
            assert all(
                status.state.terminated is not None
                for status in case.cluster.objects["pod"].status.container_statuses
            )
            finalizer_releases.append(case.pod_uid)
        return await mutation(call, *args, **kwargs)

    monkeypatch.setattr(
        case.provisioner, "_bounded_kubernetes_mutation", observe_finalizer_release
    )
    dependencies = SimpleNamespace(
        store=database,
        thread_retirement_operations=lambda: ThreadRetirementOperations(
            retirement_dependencies(database, case)
        ),
    )
    assert await retry_initial_creation_retirement(
        candidates[0], dependencies=dependencies
    )
    assert finalizer_releases == ([] if upgrade else [case.pod_uid])
    assert await database.list_retryable_initial_creation_retirements(limit=25) == []
    assert "pod" not in case.cluster.objects
    if permanent:
        assert case.cluster.objects == {}
        assert await database.get_thread(case.thread_id) is None
    else:
        ended = await database.get_thread(case.thread_id)
        assert "_stateless_workspace_retirement_settled" in metadata(ended)
        assert case.cluster.objects["pvc"].metadata.uid == case.pvc_uid


async def resume_case(database, case, actor):
    from orchestrator.services.session_class_policy import require_stateless_workspace
    from orchestrator.services.thread_resume import (
        ThreadResumeDependencies,
        resume_thread,
    )
    from orchestrator.services.thread_retirement import ThreadRetirementOperations
    from orchestrator.services.workspace_tier_policy import thread_workspace_backend

    values = {field.name: None for field in fields(ThreadResumeDependencies)}
    values.update(
        store=database,
        container_provisioner=case.provisioner,
        workspace_suspension_service=case.suspension,
        retirement=ThreadRetirementOperations(retirement_dependencies(database, case)),
        require_stateless_workspace=require_stateless_workspace,
        require_supported_protected_session_class=AsyncMock(),
        thread_workspace_backend=thread_workspace_backend,
        thread_project_ids=AsyncMock(return_value=[]),
        classify_thread_project_ids=AsyncMock(return_value=[]),
        resolve_session_config=AsyncMock(return_value={}),
        officer_conference_service=SimpleNamespace(
            thread_is_conference=lambda _: False
        ),
        should_skip_session_folder=lambda _: True,
        schedule_stateless_workspace_ensure=lambda _: None,
        logger=logging.getLogger(__name__),
    )
    return await resume_thread(
        case.thread_id,
        actor,
        await database.get_thread(case.thread_id),
        dependencies=ThreadResumeDependencies(**values),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("replace_pvc", [False, True])
async def test_normal_resume_keeps_exact_initial_retained_volume(
    database, actor, monkeypatch, replace_pvc
):
    from uuid import uuid4
    from fastapi import HTTPException
    from orchestrator.services.session_provisioner import ensure_session_workspace

    case = await workspace_attempt(database, actor, monkeypatch)
    assert await end_case(database, case) == {"status": "ended"}
    ended = await database.get_thread(case.thread_id)
    if replace_pvc:
        case.cluster.objects["pvc"].metadata.uid = str(uuid4())
        with pytest.raises(HTTPException) as refusal:
            await resume_case(database, case, actor)
        assert refusal.value.status_code in {409, 503}
        assert await database.get_thread(case.thread_id) == ended
        assert case.cluster.pod_create_calls == 1
        return
    assert await resume_case(database, case, actor) == {
        "status": "created",
        "thread_id": case.thread_id,
    }
    resumed = await database.get_thread(case.thread_id)
    assert resumed["runtime_generation"] != ended["runtime_generation"]
    assert (
        metadata(resumed)["workspace_container"]["_runtime_creation"]["attempted"]
        is False
    )
    await ensure_session_workspace(
        case.thread_id,
        db=database,
        provisioner=case.provisioner,
        suspension=case.suspension,
    )
    assert case.cluster.pod_create_calls == 2
    assert case.cluster.objects["pvc"].metadata.uid == case.pvc_uid


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [False, True])
@pytest.mark.parametrize("after_read", [False, True])
async def test_resume_refuses_volume_replacement_during_create(
    database, actor, monkeypatch, missing, after_read
):
    from uuid import uuid4
    from orchestrator.services.session_provisioner import ensure_session_workspace

    case = await workspace_attempt(database, actor, monkeypatch)
    await end_case(database, case)
    await resume_case(database, case, actor)
    create_pvc = case.provisioner._create_pvc
    successor = str(uuid4())

    async def replace_before_pvc_read(*args, **kwargs):
        prior_result = await create_pvc(*args, **kwargs) if after_read else None
        if missing:
            case.cluster.objects.pop("pvc")
        else:
            case.cluster.objects["pvc"].metadata.uid = successor
        return prior_result if after_read else await create_pvc(*args, **kwargs)

    monkeypatch.setattr(case.provisioner, "_create_pvc", replace_before_pvc_read)
    await ensure_session_workspace(
        case.thread_id,
        db=database,
        provisioner=case.provisioner,
        suspension=case.suspension,
    )
    assert case.cluster.pod_create_calls == 1
    if missing:
        assert "pvc" not in case.cluster.objects
    else:
        assert case.cluster.objects["pvc"].metadata.uid == successor
    latest = await database.fetchrow(
        "SELECT * FROM managed_repository_workspace_creation_reservations "
        "WHERE owner_id=$1::uuid ORDER BY created_at DESC LIMIT 1",
        case.thread_id,
    )
    assert latest["pvc_uid"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("resume", [False, True])
async def test_initial_emptydir_soft_end_can_resume_or_permanently_end(
    database, actor, monkeypatch, resume
):
    from orchestrator.services.session_provisioner import ensure_session_workspace

    case = await workspace_attempt(database, actor, monkeypatch, pvc_enabled=False)
    await end_case(database, case)
    ended = metadata(await database.get_thread(case.thread_id))
    assert not ended.get("_workspace_binding")
    assert ended["_stateless_workspace_retirement_settled"]["backing_id"] is None
    assert case.cluster.objects == {}
    if resume:
        await resume_case(database, case, actor)
        await ensure_session_workspace(
            case.thread_id,
            db=database,
            provisioner=case.provisioner,
            suspension=case.suspension,
        )
        assert case.cluster.pod_create_calls == 2
        assert "pvc" not in case.cluster.objects
    else:
        assert await end_case(database, case, permanent=True) == {"status": "deleted"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "unsafe", ["restarted", "init", "ephemeral", "missing_finalizer", "replacement_pvc"]
)
async def test_initial_end_refuses_unproven_fresh_never_started_runtime(
    database, actor, monkeypatch, unsafe
):
    from copy import deepcopy
    from uuid import uuid4
    from fastapi import HTTPException

    case = await workspace_attempt(database, actor, monkeypatch)
    pod = case.cluster.objects["pod"]
    if unsafe == "restarted":
        pod.status.container_statuses[0].restart_count = 1
    elif unsafe in {"init", "ephemeral"}:
        status = deepcopy(pod.status.container_statuses[0])
        status.started = True
        setattr(pod.status, f"{unsafe}_container_statuses", [status])
    elif unsafe == "missing_finalizer":
        pod.metadata.finalizers = []
    else:
        case.cluster.objects["pvc"].metadata.uid = str(uuid4())
    # The existing Ready continuation remains available, but supplies no Ready
    # proof here. Only the new initial End authority is under test.
    monkeypatch.setattr(
        case.provisioner, "_wait_for_ready", AsyncMock(return_value=None)
    )
    with pytest.raises(HTTPException) as refused:
        await end_case(database, case)
    assert refused.value.status_code in {409, 503}
    assert (await database.get_thread(case.thread_id))["status"] == "created"
    assert case.cluster.pod_deletes == 0
    assert case.cluster.pod_create_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field", ["generation", "claim_token", "runtime_incarnation", "pvc_uid"]
)
async def test_initial_end_rejects_stale_captured_authority(
    database, actor, monkeypatch, field
):
    from uuid import uuid4
    from fastapi import HTTPException

    case = await workspace_attempt(database, actor, monkeypatch)
    capture = type(case.provisioner).capture_initial_stateless_creation_retirement

    async def stale_capture(provisioner, owner):
        captured = await capture(provisioner, owner)
        captured[field] = (
            captured[field] + 1 if field == "claim_token" else str(uuid4())
        )
        return captured

    monkeypatch.setattr(
        type(case.provisioner),
        "capture_initial_stateless_creation_retirement",
        stale_capture,
    )
    with pytest.raises(HTTPException) as refused:
        await end_case(database, case)
    assert refused.value.status_code in {409, 503}
    assert (await database.get_thread(case.thread_id))["status"] == "created"
    assert case.cluster.pod_deletes == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["terminal", "pod", "pvc"])
async def test_admitted_end_preserves_resources_without_exact_stop_authority(
    database, actor, monkeypatch, missing
):
    from uuid import uuid4
    from fastapi import HTTPException

    case = await workspace_attempt(database, actor, monkeypatch)
    begin = type(database).begin_stateless_thread_workspace_retirement

    async def drift_after_begin(store, *args, **kwargs):
        closure = await begin(store, *args, **kwargs)
        if missing in {"pod", "pvc"}:
            case.cluster.objects[missing].metadata.uid = str(uuid4())
        return closure

    monkeypatch.setattr(
        type(database), "begin_stateless_thread_workspace_retirement", drift_after_begin
    )
    if missing == "terminal":
        delete = case.cluster.delete_namespaced_pod

        def no_terminal_evidence(**kwargs):
            delete(**kwargs)
            case.cluster.become_ready()

        monkeypatch.setattr(case.cluster, "delete_namespaced_pod", no_terminal_evidence)
        wait = case.provisioner._wait_for_exact_workspace_pod_terminal

        async def bounded_wait(*args, **kwargs):
            kwargs["timeout"] = 0.01
            return await wait(*args, **kwargs)

        monkeypatch.setattr(
            case.provisioner, "_wait_for_exact_workspace_pod_terminal", bounded_wait
        )
    with pytest.raises(HTTPException) as refused:
        await end_case(database, case, permanent=True)
    assert refused.value.status_code in {409, 503}
    pending = metadata(await database.get_thread(case.thread_id))
    assert pending["_stateless_claim_retirement"]["remote_retired"] is False
    assert pending["_stateless_claim_retirement"]["residents_retired"] is False
    assert set(case.cluster.objects) == {"pod", "pvc", "service"}
    assert case.cluster.objects["pod"].metadata.finalizers
    assert not await database.fetchval(
        "SELECT EXISTS(SELECT 1 FROM managed_repository_process_zero_receipts "
        "WHERE owner_id=$1::uuid)",
        case.thread_id,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("stale", ["generation", "terminal_token", "permanent"])
async def test_background_candidate_cannot_change_current_end_authority(
    database, actor, monkeypatch, stale
):
    import asyncio
    from copy import deepcopy
    from uuid import uuid4
    from orchestrator.services.stale_agent_detector import (
        retry_initial_creation_retirement,
    )
    from orchestrator.services.thread_retirement import ThreadRetirementOperations

    case = await workspace_attempt(database, actor, monkeypatch)
    begin = type(database).begin_stateless_thread_workspace_retirement

    async def disconnected(store, *args, **kwargs):
        await begin(store, *args, **kwargs)
        raise asyncio.CancelledError()

    with monkeypatch.context() as patch:
        patch.setattr(
            type(database), "begin_stateless_thread_workspace_retirement", disconnected
        )
        with pytest.raises(asyncio.CancelledError):
            await end_case(database, case)
    pending = await database.get_thread(case.thread_id)
    candidate = deepcopy(pending)
    candidate["metadata"] = metadata(candidate)
    marker = candidate["metadata"]["_stateless_claim_retirement"]
    if stale == "generation":
        candidate["runtime_generation"] = marker["initial_creation"]["generation"] = (
            str(uuid4())
        )
    elif stale == "terminal_token":
        marker["terminal_token"] += 1
    else:
        marker["permanent"] = True
    dependencies = SimpleNamespace(
        store=database,
        thread_retirement_operations=lambda: ThreadRetirementOperations(
            retirement_dependencies(database, case)
        ),
    )
    assert not await retry_initial_creation_retirement(
        candidate, dependencies=dependencies
    )
    assert await database.get_thread(case.thread_id) == pending
    assert case.cluster.pod_deletes == 0


@pytest.mark.asyncio
async def test_resumed_ready_publication_refuses_replaced_retained_volume(
    database, actor, monkeypatch
):
    from uuid import uuid4
    from orchestrator.services.session_provisioner import ensure_session_workspace

    case = await workspace_attempt(database, actor, monkeypatch)
    await end_case(database, case)
    await resume_case(database, case, actor)
    wait = case.provisioner._wait_for_ready

    async def ready(*args, **kwargs):
        case.cluster.become_ready()
        return await wait(*args, **kwargs)

    monkeypatch.setattr(case.provisioner, "_wait_for_ready", ready)
    identity = case.provisioner._trusted_pod_ssh_identity
    successor = str(uuid4())

    async def replace_before_ready(*args, **kwargs):
        case.cluster.objects["pvc"].metadata.uid = successor
        return await identity(*args, **kwargs)

    monkeypatch.setattr(
        case.provisioner, "_trusted_pod_ssh_identity", replace_before_ready
    )
    await ensure_session_workspace(
        case.thread_id,
        db=database,
        provisioner=case.provisioner,
        suspension=case.suspension,
    )
    workspace = metadata(await database.get_thread(case.thread_id))[
        "workspace_container"
    ]
    assert workspace["status"] != "ready"
    assert "_runtime_creation" in workspace
    assert case.cluster.objects["pvc"].metadata.uid == successor
    latest = await database.fetchrow(
        "SELECT * FROM managed_repository_workspace_creation_reservations "
        "WHERE owner_id=$1::uuid ORDER BY created_at DESC LIMIT 1",
        case.thread_id,
    )
    assert str(latest["pvc_uid"]) == case.pvc_uid
    assert latest["settled_at"] is None
