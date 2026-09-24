"""Real migrated PostgreSQL proof for End -> Resume -> permanent deletion."""

from tests import _b09_control_seams as control_seams

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

import asyncpg
from kubernetes.client.exceptions import ApiException
import pytest

from orchestrator.services.container_provisioner import (
    ContainerProvisioner,
    WorkspaceTeardownIdentity,
)
from orchestrator.services.manifest_store import ManifestStore
from orchestrator.services.stateless_workspace_history_cleanup import (
    reclaim_stateless_workspace_history,
)
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from tests import test_manifest_native_full_schema as full_schema
from orchestrator.security import access as access_module
from orchestrator.services import container_provisioner as container_provisioner_module
from orchestrator.services import officer_conference as officer_conference_module
from orchestrator.services import snapshot_service as snapshot_service_module

actor = full_schema.actor
database = full_schema.database
postgres_url = full_schema.postgres_url


async def create_runtime(db, thread_id, pvc_uid, *, namespace="history-workspaces"):
    claimant = "history-test-creator"
    reservation = await db.reserve_managed_repository_workspace_creation(
        thread_id,
        owner_kind="thread",
        scope="workspace_container",
        claimant=claimant,
        desired_manifest_digest="0" * 64,
    )
    assert reservation is not None
    authority = {
        "owner_kind": "thread",
        "scope": "workspace_container",
        "reservation_generation": int(reservation["reservation_generation"]),
        "claimant": claimant,
        "claim_token": int(reservation["claim_token"]),
    }
    assert await db.mark_managed_repository_workspace_creation_started(
        thread_id, **authority
    )
    runtime = str(uuid4())
    assert await db.authorize_managed_repository_workspace_creation_runtime(
        thread_id, **authority, runtime_incarnation=runtime
    )
    generation = str(uuid4())
    fingerprint = "SHA256:" + "A" * 43
    metadata = {
        "config_override": {"workspace": {"backend": "sandbox"}},
        "workspace_container": {
            "provisioner": "k8s",
            "status": "ready",
            "_runtime_incarnation": runtime,
            "_creation_reservation_id": str(reservation["id"]),
            "_creation_claim_token": str(reservation["claim_token"]),
            "pod_name": WorkspaceOwner.session(thread_id).pod_name,
            "namespace": namespace,
            "pod_ip": "10.42.0.31",
            "host": "10.42.0.31",
            "port": 30022,
            "_canvas_workspace_generation": generation,
            "host_key_fingerprint": fingerprint,
        },
        "_workspace_binding": {
            "generation": generation,
            "kind": "remote",
            "backing_id": "k8s-pvc:" + namespace + ":" + str(pvc_uid),
            "ssh_host_key_fingerprint": fingerprint,
        },
    }
    await db.execute(
        "UPDATE threads SET metadata=$2::jsonb WHERE id=$1",
        UUID(thread_id),
        json.dumps(metadata),
    )
    assert await db.settle_managed_repository_workspace_creation_reservation(
        thread_id, **authority, runtime_incarnation=runtime
    )
    return runtime


async def retire_runtime(
    db,
    thread_id,
    runtime,
    pvc_uid,
    *,
    permanent,
    namespace="history-workspaces",
    capture_location=True,
):
    closure = await db.begin_stateless_thread_workspace_retirement(
        thread_id, force=True, permanent=permanent
    )
    assert closure["state"] == "closed"
    # Kubernetes is the external boundary: this records its exact terminal
    # observation using the production authority and receipt methods.
    assert await db.acknowledge_stateless_thread_shell_absent(
        thread_id,
        terminal_token=closure["terminal_token"],
        runtime_incarnation=runtime,
    )
    assert await db.record_managed_repository_workspace_process_zero(
        thread_id,
        owner_kind="thread",
        scope="workspace_container",
        provisioner="k8s",
        runtime_incarnation=runtime,
    )
    assert await db.record_stateless_thread_workspace_process_zero(
        thread_id, runtime_incarnation=runtime
    )
    intent = await db.prepare_managed_repository_workspace_cleanup_intent(
        thread_id,
        owner_kind="thread",
        scope="workspace_container",
        runtime_incarnation=runtime,
        target_disposition="deleted",
        reclaim_shared_resources=permanent,
        pod_uid=runtime,
        pvc_uid=str(pvc_uid),
        seed_configmap_uid=str(uuid4()),
        service_uid=str(uuid4()),
        resources_captured=True,
    )
    assert intent is not None
    claimed = await db.claim_managed_repository_workspace_cleanup_intent(
        str(intent["id"]), claimant="history-test-retirement"
    )
    assert claimed is not None
    capture_provisioner = absent_kubernetes(db)
    capture_provisioner._namespace = namespace
    claimed = await db.record_managed_repository_workspace_cleanup_resources(
        str(intent["id"]),
        claimant=claimed["claimed_by"],
        claim_token=claimed["claim_token"],
        pod_uid=runtime,
        pvc_uid=str(pvc_uid),
        seed_configmap_uid=str(uuid4()),
        service_uid=str(uuid4()),
        resource_location=(
            capture_provisioner.workspace_cleanup_location(
                WorkspaceOwner.session(thread_id)
            )
            if capture_location
            else None
        ),
    )
    assert claimed is not None
    assert await db.settle_managed_repository_workspace_cleanup_intent(
        thread_id,
        owner_kind="thread",
        scope="workspace_container",
        runtime_incarnation=runtime,
        intent_generation=claimed["intent_generation"],
        claimant=claimed["claimed_by"],
        claim_token=claimed["claim_token"],
    )
    if not permanent:
        assert await db.finish_stateless_thread_workspace_retirement(thread_id)
    return claimed


async def ended_resumed_thread(
    db, owner, *, first_namespace="history-workspaces", first_location=True
):
    thread_id = str(
        await db.fetchval(
            "INSERT INTO threads(user_id,kind,title,status,execution_lane) "
            "VALUES($1,'session','Retained generations','active','stateless') RETURNING id",
            owner["id"],
        )
    )
    store = ManifestStore(db)
    document = {
        "apiVersion": "srw.dev/v1alpha1",
        "kind": "Session",
        "spec": {
            "execution": {
                "expert": {
                    "inline": {
                        "runtime": {
                            "adapter": "srw/v1",
                            "image": "test.invalid/srw:installed",
                            "config": {"schema": "srw/v1", "temperature": 0.17},
                        }
                    }
                }
            }
        },
    }
    snapshot_args = {
        "work_id": thread_id,
        "document": document,
        "resolved": document,
        "revision": "1" * 64,
        "dependencies": [],
        "owner_id": owner["id"],
        "project_ids": [],
        "harness_adapter": "srw/v1",
    }
    async with db.transaction_scope() as conn:
        snapshot = await store.freeze_execution(
            work_kind="Session", **snapshot_args, conn=conn
        )
    pvc_uid = uuid4()
    first = await create_runtime(db, thread_id, pvc_uid, namespace=first_namespace)
    preserve = await retire_runtime(
        db,
        thread_id,
        first,
        pvc_uid,
        permanent=False,
        namespace=first_namespace,
        capture_location=first_location,
    )
    old_generation = await db.fetchval(
        "SELECT runtime_generation FROM threads WHERE id=$1", UUID(thread_id)
    )
    assert await db.resume_thread(thread_id)
    assert (
        await db.fetchval(
            "SELECT runtime_generation FROM threads WHERE id=$1", UUID(thread_id)
        )
        != old_generation
    )
    async with db.transaction_scope() as conn:
        snapshot = await store.update_session_execution(
            **{**snapshot_args, "revision": "2" * 64},
            expected_generation=1,
            conn=conn,
        )
    second = await create_runtime(db, thread_id, pvc_uid)
    terminal = await retire_runtime(db, thread_id, second, pvc_uid, permanent=True)
    return thread_id, first, second, preserve, terminal, snapshot


def absent_kubernetes(db):
    provisioner = ContainerProvisioner()
    provisioner._db = db
    provisioner._k8s_available = True
    provisioner._namespace = "history-workspaces"
    provisioner._core_api = SimpleNamespace(
        **{
            name: Mock(side_effect=ApiException(status=404))
            for name in (
                "read_namespaced_pod",
                "read_namespaced_config_map",
                "read_namespaced_persistent_volume_claim",
                "read_namespaced_service",
            )
        }
    )
    return provisioner


@pytest.mark.asyncio
async def test_permanent_delete_after_resume_keeps_history_and_exact_receipts(
    database, actor
):
    thread_id, first, second, preserve, terminal, snapshot = await ended_resumed_thread(
        database, actor
    )
    with pytest.raises(asyncpg.CheckViolationError) as caught:
        await database.delete_thread(thread_id)
    assert caught.value.constraint_name == (
        "managed_repository_workspace_cleanup_required_before_owner_delete"
    )
    before = await database.get_thread(thread_id)
    provisioner = absent_kubernetes(database)
    assert await reclaim_stateless_workspace_history(database, provisioner, thread_id)
    assert await database.get_thread(thread_id) == before
    historical = await database.get_managed_repository_workspace_cleanup_intent(
        thread_id,
        owner_kind="thread",
        scope="workspace_container",
        runtime_incarnation=first,
    )
    assert historical["id"] != preserve["id"]
    assert historical["intent_source"] == "historical"
    assert historical["resource_policy"] == "terminal_reclaim"
    assert historical["result_kind"] == "settled"
    for key in (
        "runtime_incarnation",
        "pod_uid",
        "pvc_uid",
        "seed_configmap_uid",
        "service_uid",
    ):
        assert historical[key] == preserve[key]
    assert historical["thread_runtime_generation"] == before["runtime_generation"]
    assert historical["resource_location"] == preserve["resource_location"]
    fingerprint = json.loads(historical["lifecycle_fingerprint"])
    assert fingerprint["preserve_intent_id"] == str(preserve["id"])
    assert fingerprint["terminal_reclaim_intent_id"] == str(terminal["id"])
    assert await reclaim_stateless_workspace_history(database, provisioner, thread_id)
    assert provisioner._core_api.read_namespaced_pod.call_count == 2
    await database.delete_thread(thread_id)
    assert await database.get_thread(thread_id) is None
    retained = await ManifestStore(database).execution("Session", thread_id)
    assert retained == snapshot
    assert retained["work_id"] == UUID(thread_id)
    assert (
        await database.fetchval(
            "SELECT count(*) FROM srw_execution_spec_revisions WHERE execution_id=$1",
            snapshot["id"],
        )
        == 2
    )
    assert (
        await database.fetchval(
            "SELECT count(*) FROM managed_repository_process_zero_receipts "
            "WHERE owner_kind='thread' AND owner_id=$1",
            UUID(thread_id),
        )
        == 4
    )
    assert str(terminal["runtime_incarnation"]) == second


@pytest.mark.asyncio
async def test_permanent_end_replays_current_projection_after_historical_commit(
    database, actor, monkeypatch
):
    """Retry the actual End funnel after history commits but owner deletion fails."""
    import orchestrator.main as main

    (
        thread_id,
        first,
        second,
        _preserve,
        _terminal,
        snapshot,
    ) = await ended_resumed_thread(database, actor)
    provisioner = absent_kubernetes(database)
    assert await reclaim_stateless_workspace_history(database, provisioner, thread_id)
    before = await database.get_thread(thread_id)
    receipts = await database.fetch(
        "SELECT * FROM managed_repository_workspace_cleanup_intents "
        "WHERE owner_kind='thread' AND owner_id=$1 ORDER BY intent_generation",
        UUID(thread_id),
    )
    assert str(receipts[-1]["runtime_incarnation"]) == first
    assert str(receipts[-2]["runtime_incarnation"]) == second
    monkeypatch.setattr(main.app.state.resources, "postgres_db", database)
    monkeypatch.setattr(
        container_provisioner_module, "container_provisioner", provisioner
    )
    monkeypatch.setattr(
        access_module,
        "require_thread_owner",
        AsyncMock(return_value=({"sub": str(actor["id"])}, before)),
    )
    monkeypatch.setattr(
        snapshot_service_module, "snapshot_service", SimpleNamespace(is_available=False)
    )
    monkeypatch.setattr(
        main.app.state.resources, "gitea_client", SimpleNamespace(is_initialized=False)
    )
    monkeypatch.setattr(
        officer_conference_module, "conclude_conference_if_any", AsyncMock()
    )

    assert await control_seams.end_thread(
        thread_id, SimpleNamespace(), permanent=True, force=True
    ) == {"status": "deleted"}
    assert await database.get_thread(thread_id) is None
    assert await ManifestStore(database).execution("Session", thread_id) == snapshot
    assert (
        await database.fetch(
            "SELECT * FROM managed_repository_workspace_cleanup_intents "
            "WHERE owner_kind='thread' AND owner_id=$1 ORDER BY intent_generation",
            UUID(thread_id),
        )
        == receipts
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    (
        "pending",
        "preserved",
        "missing_process_zero",
        "wrong_terminal_token",
        "missing_terminal_token",
        "new_current_intent",
        "settled_without_location",
    ),
)
async def test_current_projection_replay_requires_complete_newer_history(
    database, actor, case
):
    (
        thread_id,
        first,
        second,
        preserve,
        terminal,
        _snapshot,
    ) = await ended_resumed_thread(database, actor)
    before = await database.get_thread(thread_id)
    runtime = uuid4() if case == "missing_process_zero" else UUID(first)
    token = terminal["terminal_queue_token"]
    if case == "wrong_terminal_token":
        token += 1
    elif case == "missing_terminal_token":
        token = None
    policy = "preserve" if case == "preserved" else "terminal_reclaim"
    intent_id = await database.fetchval(
        "INSERT INTO managed_repository_workspace_cleanup_intents ("
        "owner_kind,owner_id,thread_runtime_generation,scope,runtime_incarnation,"
        "intent_source,target_disposition,resource_policy,reclaim_shared_resources,"
        "terminal_queue_token,pod_uid,pvc_uid,capture_complete,resources_captured_at,"
        "phase,resource_location) VALUES ('thread',$1,$2,'workspace_container',$3,"
        "$4,$5,$6,$7,$8,$3,$9,TRUE,now(),'captured',$10::jsonb) RETURNING id",
        UUID(thread_id),
        before["runtime_generation"],
        runtime,
        "current" if case == "new_current_intent" else "historical",
        "suspended" if case == "preserved" else "deleted",
        policy,
        policy == "terminal_reclaim",
        token,
        preserve["pvc_uid"],
        None if case == "settled_without_location" else preserve["resource_location"],
    )
    if case != "pending":
        await database.execute(
            "UPDATE managed_repository_workspace_cleanup_intents "
            "SET cleanup_completed_at=now(),settled_at=now(),"
            "result_kind='settled',phase='settled' WHERE id=$1",
            intent_id,
        )
    # A previously proven operator receipt can predate capture provenance.
    # Replay consumes this immutable settlement; the history producer still
    # refuses to create such a receipt from unknown locations (covered below).
    accepted = case == "settled_without_location"
    assert (
        await database.restore_settled_thread_workspace_cleanup_projection(
            thread_id,
            runtime_incarnation=second,
            intent_generation=terminal["intent_generation"],
        )
        is accepted
    )
    outcome = await absent_kubernetes(database).reconcile_workspace_cleanup_intent(
        WorkspaceOwner.session(thread_id),
        expected_runtime_incarnation=second,
        intent_generation=terminal["intent_generation"],
    )
    assert outcome.settled is accepted
    assert await database.get_thread(thread_id) == before


@pytest.mark.asyncio
async def test_history_never_authorizes_another_runtime_or_intent_generation(
    database, actor
):
    (
        thread_id,
        first,
        second,
        _preserve,
        terminal,
        _snapshot,
    ) = await ended_resumed_thread(database, actor)
    assert await reclaim_stateless_workspace_history(
        database, absent_kubernetes(database), thread_id
    )
    historical = await database.get_managed_repository_workspace_cleanup_intent(
        thread_id,
        owner_kind="thread",
        scope="workspace_container",
        runtime_incarnation=first,
    )
    before = await database.get_thread(thread_id)
    assert not await database.restore_settled_thread_workspace_cleanup_projection(
        thread_id,
        runtime_incarnation=second,
        intent_generation=historical["intent_generation"],
    )
    assert not await database.restore_settled_thread_workspace_cleanup_projection(
        thread_id,
        runtime_incarnation=first,
        intent_generation=historical["intent_generation"],
    )
    assert await database.restore_settled_thread_workspace_cleanup_projection(
        thread_id,
        runtime_incarnation=second,
        intent_generation=terminal["intent_generation"],
    )
    assert await database.get_thread(thread_id) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "resource", ("pod_uid", "seed_configmap_uid", "pvc_uid", "service_uid")
)
async def test_any_surviving_workspace_resource_refuses_history_settlement(
    database, actor, resource
):
    thread_id, *_ = await ended_resumed_thread(database, actor)
    provisioner = absent_kubernetes(database)

    async def capture(_owner):
        return WorkspaceTeardownIdentity(
            **{
                "pod_uid": None,
                "pvc_uid": None,
                "service_uid": None,
                resource: str(uuid4()),
            }
        )

    provisioner.capture_workspace_teardown_identity = capture
    assert not await reclaim_stateless_workspace_history(
        database, provisioner, thread_id
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await database.delete_thread(thread_id)
    assert (
        await database.fetchval(
            "SELECT count(*) FROM managed_repository_workspace_cleanup_intents "
            "WHERE owner_id=$1 AND intent_source='historical'",
            UUID(thread_id),
        )
        == 0
    )


@pytest.mark.asyncio
async def test_ambiguous_kubernetes_read_keeps_cleanup_retryable(database, actor):
    thread_id, *_ = await ended_resumed_thread(database, actor)
    provisioner = absent_kubernetes(database)
    provisioner._core_api.read_namespaced_service.side_effect = ApiException(status=403)
    assert not await reclaim_stateless_workspace_history(
        database, provisioner, thread_id
    )
    assert await database.get_thread(thread_id) is not None


@pytest.mark.asyncio
async def test_queue_less_virtual_session_does_not_require_historical_reclaim(
    database, actor
):
    thread_id = str(
        await database.fetchval(
            "INSERT INTO threads(user_id,status,execution_lane) VALUES($1,'ended','stateless') RETURNING id",
            actor["id"],
        )
    )
    provisioner = absent_kubernetes(database)
    assert await reclaim_stateless_workspace_history(database, provisioner, thread_id)
    provisioner._core_api.read_namespaced_pod.assert_not_called()


@pytest.mark.asyncio
async def test_changed_namespace_does_not_relabel_old_resources(database, actor):
    thread_id, *_ = await ended_resumed_thread(
        database, actor, first_namespace="old-workspaces"
    )
    provisioner = absent_kubernetes(database)
    assert not await reclaim_stateless_workspace_history(
        database, provisioner, thread_id
    )
    provisioner._core_api.read_namespaced_pod.assert_not_called()
    with pytest.raises(asyncpg.CheckViolationError):
        await database.delete_thread(thread_id)


@pytest.mark.asyncio
async def test_unknown_historical_location_cannot_be_backfilled(database, actor):
    thread_id, _, _, preserve, _, _ = await ended_resumed_thread(
        database, actor, first_location=False
    )
    provisioner = absent_kubernetes(database)
    assert not await reclaim_stateless_workspace_history(
        database, provisioner, thread_id
    )
    provisioner._core_api.read_namespaced_pod.assert_not_called()
    with pytest.raises(asyncpg.CheckViolationError) as caught:
        await database.execute(
            "UPDATE managed_repository_workspace_cleanup_intents "
            "SET resource_location=$2::jsonb WHERE id=$1",
            preserve["id"],
            json.dumps(
                provisioner.workspace_cleanup_location(
                    WorkspaceOwner.session(thread_id)
                )
            ),
        )
    assert (
        caught.value.constraint_name == "workspace_cleanup_capture_location_no_backfill"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    ("resource_location", "pod_uid", "seed_configmap_uid", "pvc_uid", "service_uid"),
)
async def test_capture_provenance_and_resource_uids_are_immutable(
    database, actor, field
):
    thread_id, _, _, _, terminal, _ = await ended_resumed_thread(database, actor)
    if field == "resource_location":
        replacement = json.loads(terminal[field])
        replacement["pod"] = "different-pod-name"
        value = json.dumps(replacement)
        cast = "::jsonb"
    else:
        value = uuid4()
        cast = "::uuid"
    with pytest.raises(asyncpg.CheckViolationError) as caught:
        await database.execute(
            "UPDATE managed_repository_workspace_cleanup_intents "
            f"SET {field}=$2{cast} WHERE id=$1",
            terminal["id"],
            value,
        )
    assert (
        caught.value.constraint_name == "workspace_cleanup_capture_location_immutable"
    )
    assert not await database.resume_thread(thread_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ("namespace", "pod"))
async def test_capture_must_match_current_owner_location(database, actor, field):
    thread_id = str(
        await database.fetchval(
            "INSERT INTO threads(user_id,status,execution_lane) VALUES($1,'active','stateless') RETURNING id",
            actor["id"],
        )
    )
    pvc_uid = uuid4()
    runtime = await create_runtime(database, thread_id, pvc_uid)
    await database.begin_stateless_thread_workspace_retirement(
        thread_id, force=True, permanent=True
    )
    intent = await database.get_managed_repository_workspace_cleanup_intent(
        thread_id,
        owner_kind="thread",
        scope="workspace_container",
        runtime_incarnation=runtime,
    )
    claimed = await database.claim_managed_repository_workspace_cleanup_intent(
        str(intent["id"]), claimant="wrong-location-test"
    )
    location = absent_kubernetes(database).workspace_cleanup_location(
        WorkspaceOwner.session(thread_id)
    )
    location[field] = "wrong-location"
    assert (
        await database.record_managed_repository_workspace_cleanup_resources(
            str(intent["id"]),
            claimant=claimed["claimed_by"],
            claim_token=claimed["claim_token"],
            pod_uid=runtime,
            pvc_uid=str(pvc_uid),
            seed_configmap_uid=None,
            service_uid=None,
            resource_location=location,
        )
        is None
    )
    assert not await database.fetchval(
        "SELECT capture_complete FROM managed_repository_workspace_cleanup_intents WHERE id=$1",
        intent["id"],
    )
