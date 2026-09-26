"""Unattended exact creation must be rediscovered without inventing a source."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4
from datetime import datetime, timezone

import pytest

from orchestrator.database.postgres import PostgresDB
from orchestrator.services import thread_retirement
from orchestrator.services.container_provisioner import ContainerProvisioner
from orchestrator.services.session_provisioner import ensure_session_workspace
from orchestrator.services.workspace_lifecycle import EnsureOutcome
from tests import test_workspace_pull_failure_real_postgres as pull
from tests.test_active_session_creator_end_real_postgres import (
    exact_source,
    metadata,
    normal_end,
    observe_readiness,
    ready_external_runtime,
    setup_case,
)
from tests.test_sandbox_workspace_provisioner import stub_plan_inputs

pg_dsn = pull.pg_dsn
db = pull.db
_schema_applied = pull._schema_applied


async def leave_exact_source_after_end_disconnect(
    db, monkeypatch, lane, *, seeded=False, protected_agent=False
):
    case = await setup_case(db, monkeypatch, lane=lane, protected_agent=protected_agent)
    if seeded:
        monkeypatch.setattr(
            case.provider,
            "_resolve_ide_seed_files",
            AsyncMock(return_value={"settings.json": {"content": "{}"}}),
        )
        monkeypatch.setattr(
            case.cluster,
            "read_namespaced_config_map",
            lambda *, name, **_: case.cluster._read("seed", name),
        )

        def patch_seed(*, name, body, **_):
            seed = case.cluster._read("seed", name)
            assert body["metadata"]["resourceVersion"] == seed.metadata.resource_version
            seed.metadata.owner_references = body["metadata"]["ownerReferences"]
            seed.metadata.labels.update(body["metadata"].get("labels", {}))
            seed.metadata.resource_version = str(
                int(seed.metadata.resource_version) + 1
            )
            return seed

        monkeypatch.setattr(
            case.cluster, "patch_namespaced_config_map", patch_seed, raising=False
        )
    observing = observe_readiness(case, monkeypatch)
    before_begin = asyncio.Event()
    actual_end = thread_retirement._end_thread_flow_owned
    end_request = None

    async def pause_before_begin(*args, **kwargs):
        if asyncio.current_task() is end_request:
            before_begin.set()
            await asyncio.Event().wait()
        return await actual_end(*args, **kwargs)

    monkeypatch.setattr(thread_retirement, "_end_thread_flow_owned", pause_before_begin)
    creator = asyncio.create_task(
        ensure_session_workspace(
            case.thread_id,
            db=db,
            provisioner=case.provider,
            suspension=SimpleNamespace(),
        )
    )
    try:
        done, _ = await asyncio.wait(
            (creator, asyncio.create_task(observing.wait())),
            timeout=10,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if creator in done:
            raise AssertionError(
                f"Creator returned before observation: {creator.result()}"
            )
        assert observing.is_set()
        original = await db.get_thread(case.thread_id)
        source = await exact_source(db, case, lane)
        assert source["pod_uid"] is not None
        end_request = asyncio.create_task(normal_end(case))
        await asyncio.wait_for(before_begin.wait(), 5)
        assert (
            await asyncio.wait_for(asyncio.shield(creator), 5)
        ).outcome is EnsureOutcome.PENDING
        end_request.cancel()  # Caller disconnect, not creator cancellation.
        with pytest.raises(asyncio.CancelledError):
            await end_request
        current = await db.get_thread(case.thread_id)
        assert current["status"] == "created"
        assert current["runtime_generation"] == original["runtime_generation"]
        assert current["runtime_retirement_token"] is None
        assert "_stateless_claim_retirement" not in metadata(current)
        assert not metadata(current).get("_workspace_binding")
        assert not await db.session_workspace_observation_yield_requested(
            case.thread_id, runtime_generation=str(current["runtime_generation"])
        )
        return case, source, original
    finally:
        if end_request is not None and not end_request.done():
            end_request.cancel()
            await asyncio.gather(end_request, return_exceptions=True)
        if not creator.done():
            case.cluster.objects["pod"].status.container_statuses[
                0
            ].state.waiting.reason = "InvalidImageName"
        await asyncio.wait_for(asyncio.gather(creator, return_exceptions=True), 10)


def reconstructed_provider(db, case, monkeypatch):
    provider = ContainerProvisioner()
    provider._db = db
    provider._k8s_available = True
    provider._namespace = "agent-workspaces"
    provider._storage_class = "test-storage"
    provider._pvc_enabled = True
    provider._core_api = case.cluster  # External cluster survives process restart.
    stub_plan_inputs(monkeypatch, provider)
    case.provider = provider
    return provider


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_restarted_prepare_finishes_exact_source_after_end_disconnect(
    db, pg_dsn, monkeypatch, lane
):
    (
        case,
        original_source,
        original_thread,
    ) = await leave_exact_source_after_end_disconnect(db, monkeypatch, lane)
    pod_uid = case.cluster.objects["pod"].metadata.uid
    pvc_uid = case.cluster.objects["pvc"].metadata.uid
    restarted = PostgresDB(pg_dsn, min_connections=1, max_connections=3)
    await restarted.connect()
    restarted.manifest_runtime_image = "test.invalid/srw:installed"
    try:
        provider = reconstructed_provider(restarted, case, monkeypatch)
        ready_external_runtime(case, monkeypatch)
        result = await asyncio.wait_for(
            ensure_session_workspace(
                case.thread_id,
                db=restarted,
                provisioner=provider,
                suspension=SimpleNamespace(),
            ),
            15,
        )
        current = await restarted.get_thread(case.thread_id)
        source = await exact_source(restarted, case, lane)
        assert source.get("id", source.get("attempt_id")) == original_source.get(
            "id", original_source.get("attempt_id")
        )
        assert current["runtime_generation"] == original_thread["runtime_generation"]
        assert str(source["pod_uid"]) == pod_uid
        assert case.cluster.objects["pvc"].metadata.uid == pvc_uid
        assert case.cluster.pod_create_calls == 1
        workspace = metadata(current)["workspace_container"]
        assert workspace["status"] == "ready", {
            "lane": lane,
            "sweep_result": str(result),
            "thread_status": current["status"],
            "workspace_status": workspace["status"],
            "source_phase": source.get("phase", source.get("status")),
            "same_pod_uid": str(source["pod_uid"]) == pod_uid,
            "binding": metadata(current).get("_workspace_binding"),
        }
        assert workspace["_runtime_incarnation"] == pod_uid
        assert metadata(current)["_workspace_binding"]["backing_id"].endswith(pvc_uid)
        if lane == "pinned":
            assert source["status"] == "published"
        else:
            assert source["settled_at"] is not None
            assert "_runtime_creation" not in workspace
    finally:
        await restarted.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_reconstruction_never_reissues_an_already_recorded_pod_after_absence(
    db, monkeypatch, lane
):
    case, original_source, _ = await leave_exact_source_after_end_disconnect(
        db, monkeypatch, lane
    )
    original_pod = case.cluster.objects.pop("pod")
    # Model API absence only; there is no terminal observation or stop receipt.
    provider = reconstructed_provider(db, case, monkeypatch)
    result = await asyncio.wait_for(
        ensure_session_workspace(
            case.thread_id, db=db, provisioner=provider, suspension=SimpleNamespace()
        ),
        15,
    )
    source = await exact_source(db, case, lane)
    assert (
        str(source["pod_uid"])
        == str(original_source["pod_uid"])
        == original_pod.metadata.uid
    )
    assert not metadata(await db.get_thread(case.thread_id)).get("_workspace_binding")
    assert case.cluster.pod_create_calls == 1, {
        "lane": lane,
        "result": str(result),
        "original_uid": original_pod.metadata.uid,
        "new_uid": getattr(
            getattr(case.cluster.objects.get("pod"), "metadata", None), "uid", None
        ),
        "durable_source_uid": source["pod_uid"],
    }
    assert "pod" not in case.cluster.objects


@pytest.mark.asyncio
@pytest.mark.parametrize("resource", ["pod", "pvc", "service", "seed"])
@pytest.mark.parametrize("drift", ["absent", "replacement", "terminating", "unchanged"])
async def test_pinned_recorded_resources_are_read_only(
    db, monkeypatch, resource, drift
):
    case, original_source, _ = await leave_exact_source_after_end_disconnect(
        db, monkeypatch, "pinned", seeded=True
    )
    original = case.cluster.objects[resource]
    original_uid = original.metadata.uid
    if drift == "absent":
        case.cluster.objects.pop(resource)
    elif drift == "replacement":
        original.metadata.uid = str(uuid4())
    elif drift == "terminating":
        original.metadata.deletion_timestamp = datetime.now(timezone.utc)
    provider = reconstructed_provider(db, case, monkeypatch)
    monkeypatch.setattr(
        provider,
        "_resolve_ide_seed_files",
        AsyncMock(return_value={"settings.json": {"content": "{}"}}),
    )
    creates = []
    for kind in ("pod", "persistent_volume_claim", "service", "config_map"):
        method = "create_namespaced_" + kind
        original_create = getattr(case.cluster, method)

        def record(*args, _method=method, _original=original_create, **kwargs):
            creates.append(_method)
            return _original(*args, **kwargs)

        monkeypatch.setattr(case.cluster, method, record)
    ready_external_runtime(
        case, monkeypatch
    ) if resource != "pod" or drift != "absent" else None
    result = await asyncio.wait_for(
        ensure_session_workspace(
            case.thread_id, db=db, provisioner=provider, suspension=SimpleNamespace()
        ),
        15,
    )
    assert creates == [], {"resource": resource, "drift": drift, "creates": creates}
    source = await exact_source(db, case, "pinned")
    uid_key = "seed_configmap_uid" if resource == "seed" else resource + "_uid"
    assert str(source[uid_key]) == str(original_source[uid_key]) == original_uid
    current = metadata(await db.get_thread(case.thread_id))
    if drift == "unchanged":
        # Existing ensure reports a completed create as pending/creating;
        # the durable publication is the success authority.
        assert result.outcome is EnsureOutcome.PENDING
        assert current["workspace_container"]["status"] == "ready"
        assert source["status"] == "published"
    else:
        # Preserve the foreground wrapper's legacy False -> FAILED result;
        # no durable failure or replacement authority is written.
        assert result.outcome is EnsureOutcome.FAILED
        assert source["status"] == "planned"
        assert not current.get("_workspace_binding")
        if drift == "absent":
            assert resource not in case.cluster.objects
