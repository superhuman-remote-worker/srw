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


async def retained_attention_case(database, actor, monkeypatch):
    """Build a real G1 End followed by G2 sticky startup attention."""
    from dataclasses import replace
    from datetime import datetime, timedelta, timezone

    from orchestrator.database.container_startup_stage import (
        ReadyObservedAt,
        ScheduledAt,
        StageBudgets,
    )
    from orchestrator.services import stateless_session_retirement as protocol
    from orchestrator.services.session_provisioner import ensure_session_workspace

    original_create = continuation.DelayedWorkspaceCluster.create_namespaced_pod
    original_ready = continuation.DelayedWorkspaceCluster.become_ready

    def scheduled_first(self, *, body, **kwargs):
        pod = original_create(self, body=body, **kwargs)
        pod.spec.node_name = "node8"
        pod.status.conditions = [
            SimpleNamespace(
                type="PodScheduled",
                status="True",
                last_transition_time=datetime.now(timezone.utc),
            )
        ]
        return pod

    def ready_first(self):
        original_ready(self)
        self.objects["pod"].status.conditions.append(
            SimpleNamespace(
                type="Ready",
                status="True",
                last_transition_time=datetime.now(timezone.utc),
            )
        )

    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    monkeypatch.setattr(
        continuation.DelayedWorkspaceCluster, "create_namespaced_pod", scheduled_first
    )
    monkeypatch.setattr(
        continuation.DelayedWorkspaceCluster, "become_ready", ready_first
    )
    case = await workspace_attempt(
        database, actor, monkeypatch, first_wait="ready", seeded=True
    )
    monkeypatch.setattr(
        continuation.DelayedWorkspaceCluster, "create_namespaced_pod", original_create
    )
    monkeypatch.setattr(
        continuation.DelayedWorkspaceCluster, "become_ready", original_ready
    )
    await database.execute(
        "INSERT INTO run_queue(unit_id,unit_kind,state,lease_token) "
        "VALUES($1::uuid,'session_turn','done',2)",
        case.thread_id,
    )

    async def residents(thread, *, terminal_token, **_):
        return protocol.ResidentRetirementProof(
            authority=protocol.resolve_shell_retirement_authority(
                thread, terminal_token=terminal_token
            )
        )

    async def shell(thread, *, terminal_token, **_):
        return protocol.resolve_shell_retirement_authority(
            thread, terminal_token=terminal_token
        )

    monkeypatch.setattr(protocol, "retire_stateless_workspace_residents", residents)
    monkeypatch.setattr(protocol, "retire_stateless_session_shell", shell)
    monkeypatch.setattr(protocol, "verify_stateless_workspace_residents_retired", shell)
    terminal_capture = case.provisioner.capture_terminal_workspace_identity

    async def capture_after_seed_disappears(owner):
        captured = await terminal_capture(owner)
        case.cluster.objects.pop("seed")
        absent = await case.provisioner.capture_workspace_teardown_identity(owner)
        return replace(captured, seed_configmap_uid=absent.seed_configmap_uid)

    monkeypatch.setattr(
        case.provisioner,
        "capture_terminal_workspace_identity",
        capture_after_seed_disappears,
    )
    dependencies = replace(
        retirement_dependencies(database, case),
        build_agent_cloud_mount=AsyncMock(return_value=None),
    )
    assert await end_thread_flow(
        case.thread_id,
        case.before,
        permanent=False,
        force=False,
        dependencies=dependencies,
    ) == {"status": "ended"}
    predecessor = await database.fetchrow(
        "SELECT * FROM managed_repository_workspace_creation_reservations "
        "WHERE id=$1::uuid",
        case.creation["id"],
    )
    await resume_case(database, case, actor)
    create_pod = case.cluster.create_namespaced_pod

    def waiting_pod(*, body, **kwargs):
        pod = create_pod(body=body, **kwargs)
        pod.spec.node_name = None
        pod.status.phase = "Pending"
        pod.status.conditions = [
            SimpleNamespace(type="PodScheduled", status="False", reason="Unschedulable")
        ]
        return pod

    monkeypatch.setattr(case.cluster, "create_namespaced_pod", waiting_pod)
    await ensure_session_workspace(
        case.thread_id,
        db=database,
        provisioner=case.provisioner,
        suspension=case.suspension,
    )
    source = await database.fetchrow(
        "SELECT * FROM managed_repository_workspace_creation_reservations "
        "WHERE owner_kind='thread' AND owner_id=$1::uuid AND settled_at IS NULL",
        case.thread_id,
    )
    observed = datetime.now(timezone.utc) - timedelta(milliseconds=10)
    scheduled_at = observed.replace(microsecond=(observed.microsecond // 10) * 10)
    pod = case.cluster.objects["pod"]
    pod.spec.node_name = "node8"
    case.cluster.become_ready()
    pod.status.conditions = [
        SimpleNamespace(
            type="PodScheduled", status="True", last_transition_time=scheduled_at
        ),
        SimpleNamespace(type="Ready", status="True", last_transition_time=scheduled_at),
    ]
    identity = dict(
        owner_kind="thread",
        owner_id=case.thread_id,
        reservation_id=str(source["id"]),
        claim_token=source["claim_token"],
        pod_uid=str(source["pod_uid"]),
    )
    assert await database.observe_container_startup(
        **identity,
        observation=ScheduledAt(scheduled_at),
        budgets=StageBudgets(180, None, 0.001),
    )
    assert await database.observe_container_startup(
        **identity, observation=ReadyObservedAt(scheduled_at)
    )
    attention = await database.fetchrow(
        "SELECT * FROM managed_repository_workspace_creation_reservations "
        "WHERE id=$1::uuid",
        source["id"],
    )
    assert attention["startup_state"] == "attention"
    return case, predecessor, attention, dependencies


@pytest.mark.asyncio
@pytest.mark.parametrize("pending_input", [False, True])
async def test_normal_end_settles_retained_startup_attention(
    database, actor, monkeypatch, pending_input
):
    case, predecessor, attention, dependencies = await retained_attention_case(
        database, actor, monkeypatch
    )
    from orchestrator.services.container_provisioner import WorkspaceOwner

    assert await database.get_current_retained_startup_attention_source(case.thread_id)
    captured = (
        await case.provisioner.capture_retained_stateless_startup_attention_retirement(
            WorkspaceOwner.session(case.thread_id)
        )
    )
    assert captured
    original_pvc_uid = case.cluster.objects["pvc"].metadata.uid
    original_binding = metadata(await database.get_thread(case.thread_id))[
        "_workspace_binding"
    ]
    if pending_input:
        from uuid import uuid4

        message_id, delivery_id = uuid4(), uuid4()
        await database.execute(
            "INSERT INTO thread_messages "
            "(id,thread_id,role,content,turn_number) "
            "VALUES ($1,$2::uuid,'event','pending wake',1000)",
            message_id,
            case.thread_id,
        )
        await database.execute(
            "INSERT INTO thread_input_deliveries "
            "(delivery_id,thread_id,message_id,source,execution_lane,"
            "conversation_revision) "
            "VALUES ($1,$2::uuid,$3,'officer_wake','stateless',0)",
            delivery_id,
            case.thread_id,
            message_id,
        )
        await database.execute(
            "UPDATE run_queue SET state='parked',input_seq=$2,consumed_seq=NULL,"
            "leased_by=NULL,leased_until=NULL WHERE unit_id=$1::uuid",
            case.thread_id,
            await database.fetchval(
                "SELECT seq FROM thread_messages WHERE id=$1", message_id
            ),
        )
    before_messages = await database.fetch(
        "SELECT * FROM thread_messages WHERE thread_id=$1::uuid ORDER BY id",
        case.thread_id,
    )
    before_deliveries = await database.fetch(
        "SELECT * FROM thread_input_deliveries "
        "WHERE thread_id=$1::uuid ORDER BY delivery_id",
        case.thread_id,
    )
    before_queue = await database.fetchrow(
        "SELECT * FROM run_queue WHERE unit_id=$1::uuid", case.thread_id
    )
    assert (
        await database.get_current_retained_startup_attention_source(case.thread_id)
        == captured
    )
    if pending_input:
        assert before_queue["input_seq"] is not None
        assert before_queue["consumed_seq"] is None
        ordinary = await database.begin_stateless_thread_workspace_retirement(
            case.thread_id, force=False, permanent=False
        )
        assert ordinary["state"] == "busy"
        assert ordinary["pending_input"] is True
        assert (
            await database.fetchrow(
                "SELECT * FROM run_queue WHERE unit_id=$1::uuid", case.thread_id
            )
            == before_queue
        )
    result = await end_thread_flow(
        case.thread_id,
        await database.get_thread(case.thread_id),
        permanent=False,
        force=False,
        dependencies=dependencies,
    )
    assert result == {"status": "ended"}
    assert case.cluster.objects["pvc"].metadata.uid == original_pvc_uid
    assert "service" not in case.cluster.objects
    assert "pod" not in case.cluster.objects
    after_receipt = await database.fetchrow(
        "SELECT * FROM managed_repository_workspace_creation_reservations "
        "WHERE id=$1::uuid",
        attention["id"],
    )
    after_queue = await database.fetchrow(
        "SELECT * FROM run_queue WHERE unit_id=$1::uuid", case.thread_id
    )
    assert after_receipt["result_kind"] == "aborted"
    assert after_receipt["scheduled_at"] == attention["scheduled_at"]
    assert after_receipt["startup_attention_at"] == attention["startup_attention_at"]
    assert after_queue["input_seq"] == before_queue["input_seq"]
    assert after_queue["consumed_seq"] == before_queue["consumed_seq"]
    assert (
        await database.fetch(
            "SELECT * FROM thread_messages WHERE thread_id=$1::uuid ORDER BY id",
            case.thread_id,
        )
        == before_messages
    )
    assert (
        await database.fetch(
            "SELECT * FROM thread_input_deliveries "
            "WHERE thread_id=$1::uuid ORDER BY delivery_id",
            case.thread_id,
        )
        == before_deliveries
    )
    assert await database.fetchval(
        "SELECT EXISTS(SELECT 1 FROM managed_repository_process_zero_receipts "
        "WHERE owner_kind='thread' AND owner_id=$1::uuid "
        "AND scope='stateless_workspace' AND runtime_incarnation=$2)",
        case.thread_id,
        str(attention["runtime_incarnation"]),
    )
    assert (
        await database.fetchrow(
            "SELECT * FROM managed_repository_workspace_creation_reservations "
            "WHERE id=$1::uuid",
            predecessor["id"],
        )
        == predecessor
    )
    ended = await database.get_thread(case.thread_id)
    assert ended["status"] == "ended"
    assert metadata(ended)["_workspace_binding"] == original_binding
    assert (
        metadata(ended)["workspace_container"].get("_snapshot_restore_required")
        is not True
    )
    cleanup = await database.fetchrow(
        "SELECT * FROM managed_repository_workspace_cleanup_intents "
        "WHERE owner_kind='thread' AND owner_id=$1::uuid "
        "AND thread_runtime_generation=$2::uuid",
        case.thread_id,
        attention["thread_runtime_generation"],
    )
    assert cleanup["result_kind"] == "settled"
    assert cleanup["pod_uid"] == attention["pod_uid"]
    assert cleanup["pvc_uid"] == attention["pvc_uid"]
    assert cleanup["service_uid"] == attention["service_uid"]
    assert cleanup["seed_configmap_uid"] == attention["seed_configmap_uid"]
    assert cleanup["resource_policy"] == "preserve"
    assert case.cluster.pod_create_calls == 2
    assert metadata(ended)["_stateless_workspace_retirement_settled"][
        "retained_startup_attention"
    ]
    assert (
        await database.get_stateless_retained_startup_attention_retirement(
            case.thread_id, require_settled=True
        )
        == captured
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("interruption", ["after_begin", "after_stop"])
async def test_retained_attention_end_replays_full_funnel_after_response_loss(
    database, actor, monkeypatch, interruption
):
    import asyncio

    from orchestrator.services.stale_agent_detector import (
        retry_initial_creation_retirement,
    )
    from orchestrator.services.thread_retirement import ThreadRetirementOperations
    from tests.test_session_created_source_rediscovery_real_postgres import (
        reconstructed_provider,
    )

    case, _, attention, dependencies = await retained_attention_case(
        database, actor, monkeypatch
    )
    before = await database.get_thread(case.thread_id)
    original = (
        type(database).begin_stateless_thread_workspace_retirement
        if interruption == "after_begin"
        else type(database).acknowledge_stateless_thread_runtime_process_zero
    )

    async def response_lost(store, *args, **kwargs):
        await original(store, *args, **kwargs)
        raise asyncio.CancelledError()

    target = (
        "begin_stateless_thread_workspace_retirement"
        if interruption == "after_begin"
        else "acknowledge_stateless_thread_runtime_process_zero"
    )
    with monkeypatch.context() as patch:
        patch.setattr(type(database), target, response_lost)
        with pytest.raises(asyncio.CancelledError):
            await end_thread_flow(
                case.thread_id,
                before,
                permanent=False,
                force=False,
                dependencies=dependencies,
            )
    pending = await database.get_thread(case.thread_id)
    marker = metadata(pending)["_stateless_claim_retirement"]
    token = marker["terminal_token"]
    assert marker["retained_startup_attention"]["reservation_id"] == str(
        attention["id"]
    )
    candidates = await database.list_retryable_initial_creation_retirements(limit=25)
    assert [str(candidate["id"]) for candidate in candidates] == [case.thread_id]
    restarted = type(database)(
        database._connection_string, min_connections=1, max_connections=3
    )
    await restarted.connect()
    try:
        case.provisioner = reconstructed_provider(restarted, case, monkeypatch)
        replay_dependencies = SimpleNamespace(
            store=restarted,
            thread_retirement_operations=lambda: ThreadRetirementOperations(
                retirement_dependencies(restarted, case)
            ),
        )
        assert await retry_initial_creation_retirement(
            candidates[0], dependencies=replay_dependencies
        )
        ended = await restarted.get_thread(case.thread_id)
        assert ended["status"] == "ended"
        settled = metadata(ended)["_stateless_workspace_retirement_settled"]
        assert settled["terminal_token"] == token
        assert (
            settled["retained_startup_attention"]
            == marker["retained_startup_attention"]
        )
        assert (
            await restarted.list_retryable_initial_creation_retirements(limit=25) == []
        )
        assert case.cluster.pod_create_calls == 2
        assert "pod" not in case.cluster.objects
        assert case.cluster.objects["pvc"].metadata.uid == str(attention["pvc_uid"])
    finally:
        await restarted.close()


@pytest.mark.asyncio
async def test_retained_attention_ack_rechecks_predecessor_zero_on_locked_source(
    database, actor, monkeypatch
):
    from fastapi import HTTPException

    case, predecessor, _, dependencies = await retained_attention_case(
        database, actor, monkeypatch
    )
    acknowledge = type(database).acknowledge_stateless_thread_runtime_process_zero

    async def lose_predecessor_proof(store, *args, **kwargs):
        await store.execute(
            "DELETE FROM managed_repository_process_zero_receipts "
            "WHERE owner_kind='thread' AND owner_id=$1::uuid "
            "AND runtime_incarnation=$2",
            case.thread_id,
            str(predecessor["runtime_incarnation"]),
        )
        return await acknowledge(store, *args, **kwargs)

    monkeypatch.setattr(
        type(database),
        "acknowledge_stateless_thread_runtime_process_zero",
        lose_predecessor_proof,
    )
    with pytest.raises(HTTPException) as refused:
        await end_thread_flow(
            case.thread_id,
            await database.get_thread(case.thread_id),
            permanent=False,
            force=False,
            dependencies=dependencies,
        )
    assert refused.value.status_code == 503
    current = await database.get_thread(case.thread_id)
    marker = metadata(current)["_stateless_claim_retirement"]
    assert marker["remote_retired"] is False
    assert marker["residents_retired"] is False
    assert "_stateless_workspace_retirement_settled" not in metadata(current)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "replacement", ["pod", "pvc", "service", "seed", "seed_controller"]
)
async def test_retained_attention_end_refuses_changed_physical_identity_before_begin(
    database, actor, monkeypatch, replacement
):
    from uuid import uuid4

    from fastapi import HTTPException

    case, _, attention, dependencies = await retained_attention_case(
        database, actor, monkeypatch
    )
    before_thread = await database.get_thread(case.thread_id)
    before_queue = await database.fetchrow(
        "SELECT * FROM run_queue WHERE unit_id=$1::uuid", case.thread_id
    )
    if replacement == "seed_controller":
        case.cluster.objects["seed"].metadata.owner_references = []
    else:
        case.cluster.objects[replacement].metadata.uid = str(uuid4())
    with pytest.raises(HTTPException) as refused:
        await end_thread_flow(
            case.thread_id,
            before_thread,
            permanent=False,
            force=False,
            dependencies=dependencies,
        )
    assert refused.value.status_code == 503
    assert await database.get_thread(case.thread_id) == before_thread
    assert (
        await database.fetchrow(
            "SELECT * FROM run_queue WHERE unit_id=$1::uuid", case.thread_id
        )
        == before_queue
    )
    assert (
        await database.fetchval(
            "SELECT count(*) FROM managed_repository_workspace_cleanup_intents "
            "WHERE owner_kind='thread' AND owner_id=$1::uuid "
            "AND thread_runtime_generation=$2::uuid",
            case.thread_id,
            attention["thread_runtime_generation"],
        )
        == 0
    )
    assert case.cluster.pod_create_calls == 2
    assert {"pod", "pvc", "service", "seed"} <= set(case.cluster.objects)


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement", ["pod", "pvc", "service", "seed"])
async def test_retained_attention_end_refuses_replacement_after_capture(
    database, actor, monkeypatch, replacement
):
    from uuid import uuid4

    from fastapi import HTTPException

    case, _, attention, dependencies = await retained_attention_case(
        database, actor, monkeypatch
    )
    begin = type(database).begin_stateless_thread_workspace_retirement

    async def replace_before_begin(store, *args, **kwargs):
        case.cluster.objects[replacement].metadata.uid = str(uuid4())
        return await begin(store, *args, **kwargs)

    monkeypatch.setattr(
        type(database),
        "begin_stateless_thread_workspace_retirement",
        replace_before_begin,
    )
    with pytest.raises(HTTPException) as refused:
        await end_thread_flow(
            case.thread_id,
            await database.get_thread(case.thread_id),
            permanent=False,
            force=False,
            dependencies=dependencies,
        )
    assert refused.value.status_code == 503
    current = await database.get_thread(case.thread_id)
    assert current["status"] == "ended"
    marker = metadata(current)["_stateless_claim_retirement"]
    assert marker["retained_startup_attention"]["runtime_incarnation"] == str(
        attention["pod_uid"]
    )
    assert case.cluster.pod_create_calls == 2
    assert {"pod", "pvc", "service", "seed"} <= set(case.cluster.objects)
    assert not await database.fetchval(
        "SELECT EXISTS(SELECT 1 FROM managed_repository_process_zero_receipts "
        "WHERE owner_kind='thread' AND owner_id=$1::uuid "
        "AND scope='stateless_workspace' AND runtime_incarnation=$2)",
        case.thread_id,
        str(attention["pod_uid"]),
    )


@pytest.mark.asyncio
async def test_retained_attention_end_refuses_deleted_storage_after_physical_stop(
    database, actor, monkeypatch
):
    from fastapi import HTTPException

    case, _, attention, dependencies = await retained_attention_case(
        database, actor, monkeypatch
    )
    reconcile = case.provisioner.reconcile_workspace_cleanup_intent

    async def lose_volume_after_stop(*args, **kwargs):
        result = await reconcile(*args, **kwargs)
        if result.settled:
            case.cluster.objects.pop("pvc")
        return result

    monkeypatch.setattr(
        case.provisioner,
        "reconcile_workspace_cleanup_intent",
        lose_volume_after_stop,
    )
    with pytest.raises(HTTPException) as refused:
        await end_thread_flow(
            case.thread_id,
            await database.get_thread(case.thread_id),
            permanent=False,
            force=False,
            dependencies=dependencies,
        )
    assert refused.value.status_code == 503
    current = await database.get_thread(case.thread_id)
    assert current["status"] == "ended"
    marker = metadata(current)["_stateless_claim_retirement"]
    assert marker["remote_retired"] is False
    assert marker["residents_retired"] is False
    assert await database.fetchval(
        "SELECT EXISTS(SELECT 1 FROM managed_repository_process_zero_receipts "
        "WHERE owner_kind='thread' AND owner_id=$1::uuid "
        "AND scope='stateless_workspace' AND runtime_incarnation=$2)",
        case.thread_id,
        str(attention["pod_uid"]),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("version", True),
        ("claim_token", True),
        ("reservation_id", "00000000-0000-0000-0000-000000000001"),
        ("predecessor_reservation_id", "00000000-0000-0000-0000-000000000001"),
        ("scheduled_at", "2000-01-01T00:00:00+00:00"),
    ],
)
async def test_retained_attention_begin_refuses_changed_provenance_without_effect(
    database, actor, monkeypatch, field, value
):
    from orchestrator.services.container_provisioner import WorkspaceOwner

    case, _, attention, _ = await retained_attention_case(database, actor, monkeypatch)
    captured = (
        await case.provisioner.capture_retained_stateless_startup_attention_retirement(
            WorkspaceOwner.session(case.thread_id)
        )
    )
    assert captured
    before_thread = await database.get_thread(case.thread_id)
    before_queue = await database.fetchrow(
        "SELECT * FROM run_queue WHERE unit_id=$1::uuid", case.thread_id
    )
    changed = {**captured, field: value}
    with pytest.raises(RuntimeError):
        await database.begin_stateless_thread_workspace_retirement(
            case.thread_id,
            force=False,
            permanent=False,
            retained_startup_attention=changed,
        )
    assert await database.get_thread(case.thread_id) == before_thread
    assert (
        await database.fetchrow(
            "SELECT * FROM run_queue WHERE unit_id=$1::uuid", case.thread_id
        )
        == before_queue
    )
    assert (
        await database.fetchval(
            "SELECT count(*) FROM managed_repository_workspace_cleanup_intents "
            "WHERE owner_kind='thread' AND owner_id=$1::uuid "
            "AND thread_runtime_generation=$2::uuid",
            case.thread_id,
            attention["thread_runtime_generation"],
        )
        == 0
    )


@pytest.mark.asyncio
async def test_retained_attention_begin_keeps_pending_control_busy(
    database, actor, monkeypatch
):
    from orchestrator.services.container_provisioner import WorkspaceOwner

    case, _, attention, _ = await retained_attention_case(database, actor, monkeypatch)
    captured = (
        await case.provisioner.capture_retained_stateless_startup_attention_retirement(
            WorkspaceOwner.session(case.thread_id)
        )
    )
    assert captured
    await database.execute(
        "UPDATE run_queue SET control_input_seq=1,control_consumed_seq=0 "
        "WHERE unit_id=$1::uuid",
        case.thread_id,
    )
    before_thread = await database.get_thread(case.thread_id)
    before_queue = await database.fetchrow(
        "SELECT * FROM run_queue WHERE unit_id=$1::uuid", case.thread_id
    )
    result = await database.begin_stateless_thread_workspace_retirement(
        case.thread_id,
        force=False,
        permanent=False,
        retained_startup_attention=captured,
    )
    assert result["state"] == "busy"
    assert result["pending_control"] is True
    assert await database.get_thread(case.thread_id) == before_thread
    assert (
        await database.fetchrow(
            "SELECT * FROM run_queue WHERE unit_id=$1::uuid", case.thread_id
        )
        == before_queue
    )
    assert (
        await database.fetchval(
            "SELECT count(*) FROM managed_repository_workspace_cleanup_intents "
            "WHERE owner_kind='thread' AND owner_id=$1::uuid "
            "AND thread_runtime_generation=$2::uuid",
            case.thread_id,
            attention["thread_runtime_generation"],
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed_authority",
    [
        "active_claimant",
        "live_lease",
        "unresolved_loss",
        "attempted_marker",
        "restore_marker",
        "retained_binding",
        "predecessor_zero",
    ],
)
async def test_retained_attention_begin_refuses_changed_current_authority_without_effect(
    database, actor, monkeypatch, changed_authority
):
    from uuid import uuid4

    from orchestrator.services.container_provisioner import WorkspaceOwner

    case, predecessor, attention, _ = await retained_attention_case(
        database, actor, monkeypatch
    )
    captured = (
        await case.provisioner.capture_retained_stateless_startup_attention_retirement(
            WorkspaceOwner.session(case.thread_id)
        )
    )
    assert captured
    if changed_authority == "active_claimant":
        await database.execute(
            "UPDATE threads SET metadata=metadata || jsonb_build_object("
            "'_stateless_active_claim',jsonb_build_object("
            "'lease_token',2,'pod','active-worker-pod','pod_uid',$2::text)) "
            "WHERE id=$1::uuid",
            case.thread_id,
            str(attention["pod_uid"]),
        )
    elif changed_authority == "live_lease":
        await database.execute(
            "UPDATE run_queue SET state='leased',leased_by='active-worker',"
            "leased_until=now()+interval '30 seconds' WHERE unit_id=$1::uuid",
            case.thread_id,
        )
    elif changed_authority == "unresolved_loss":
        await database.execute(
            "UPDATE run_queue SET state='parked' WHERE unit_id=$1::uuid",
            case.thread_id,
        )
        await database.execute(
            "UPDATE threads SET metadata=metadata || jsonb_build_object("
            "'_stateless_claim_losses',jsonb_build_object("
            "'2',jsonb_build_object('quiesced',false,'pod','lost-worker-pod',"
            "'pod_uid',$2::text)),"
            "'_stateless_claim_loss_hold',jsonb_build_object("
            "'lease_token',2,'attempts_since_completion',0,"
            "'intended_state','parked')) WHERE id=$1::uuid",
            case.thread_id,
            str(attention["pod_uid"]),
        )
    elif changed_authority == "attempted_marker":
        await database.execute(
            "UPDATE threads SET metadata=jsonb_set(metadata,"
            "'{workspace_container,_runtime_creation,attempted}',"
            "'false'::jsonb) WHERE id=$1::uuid",
            case.thread_id,
        )
    elif changed_authority == "restore_marker":
        await database.execute(
            "UPDATE threads SET metadata=jsonb_set(metadata,"
            "'{workspace_container,_runtime_creation,mode}',"
            "'\"restore\"'::jsonb) WHERE id=$1::uuid",
            case.thread_id,
        )
    elif changed_authority == "retained_binding":
        await database.execute(
            "UPDATE threads SET metadata=jsonb_set(metadata,"
            "'{_workspace_binding,generation}',to_jsonb($2::text)) "
            "WHERE id=$1::uuid",
            case.thread_id,
            str(uuid4()),
        )
    else:
        await database.execute(
            "DELETE FROM managed_repository_process_zero_receipts "
            "WHERE owner_kind='thread' AND owner_id=$1::uuid "
            "AND runtime_incarnation=$2",
            case.thread_id,
            str(predecessor["runtime_incarnation"]),
        )

    before_thread = await database.get_thread(case.thread_id)
    before_queue = await database.fetchrow(
        "SELECT * FROM run_queue WHERE unit_id=$1::uuid", case.thread_id
    )
    physical = {name: obj.metadata.uid for name, obj in case.cluster.objects.items()}
    with pytest.raises(RuntimeError):
        await database.begin_stateless_thread_workspace_retirement(
            case.thread_id,
            force=False,
            permanent=False,
            retained_startup_attention=captured,
        )
    assert await database.get_thread(case.thread_id) == before_thread
    assert (
        await database.fetchrow(
            "SELECT * FROM run_queue WHERE unit_id=$1::uuid", case.thread_id
        )
        == before_queue
    )
    assert (
        await database.fetchval(
            "SELECT count(*) FROM managed_repository_workspace_cleanup_intents "
            "WHERE owner_kind='thread' AND owner_id=$1::uuid "
            "AND thread_runtime_generation=$2::uuid",
            case.thread_id,
            str(attention["thread_runtime_generation"]),
        )
        == 0
    )
    assert case.cluster.pod_create_calls == 2
    assert {
        name: obj.metadata.uid for name, obj in case.cluster.objects.items()
    } == physical


@pytest.mark.asyncio
async def test_retained_attention_predecessor_result_cannot_change_after_ready_end(
    database, actor, monkeypatch
):
    from asyncpg import PostgresError

    case, predecessor, attention, _ = await retained_attention_case(
        database, actor, monkeypatch
    )
    before_thread = await database.get_thread(case.thread_id)
    before_queue = await database.fetchrow(
        "SELECT * FROM run_queue WHERE unit_id=$1::uuid", case.thread_id
    )
    with pytest.raises(PostgresError):
        await database.execute(
            "UPDATE managed_repository_workspace_creation_reservations "
            "SET result_kind='aborted' WHERE id=$1::uuid",
            str(predecessor["id"]),
        )
    assert (
        await database.fetchrow(
            "SELECT * FROM managed_repository_workspace_creation_reservations "
            "WHERE id=$1::uuid",
            str(predecessor["id"]),
        )
        == predecessor
    )
    assert await database.get_thread(case.thread_id) == before_thread
    assert (
        await database.fetchrow(
            "SELECT * FROM run_queue WHERE unit_id=$1::uuid", case.thread_id
        )
        == before_queue
    )
    assert not await database.fetchval(
        "SELECT EXISTS(SELECT 1 FROM managed_repository_workspace_cleanup_intents "
        "WHERE owner_kind='thread' AND owner_id=$1::uuid "
        "AND thread_runtime_generation=$2::uuid)",
        case.thread_id,
        str(attention["thread_runtime_generation"]),
    )
    assert case.cluster.pod_create_calls == 2
    assert {"pod", "pvc", "service", "seed"} <= set(case.cluster.objects)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed_authority", ["predecessor_cleanup", "current_generation", "current_claim"]
)
async def test_retained_attention_invalid_drift_is_rejected_by_native_schema(
    database, actor, monkeypatch, changed_authority
):
    from uuid import uuid4

    from asyncpg import PostgresError

    case, predecessor, attention, _ = await retained_attention_case(
        database, actor, monkeypatch
    )
    before_thread = await database.get_thread(case.thread_id)
    before_queue = await database.fetchrow(
        "SELECT * FROM run_queue WHERE unit_id=$1::uuid", case.thread_id
    )
    before_cleanup = await database.fetchrow(
        "SELECT * FROM managed_repository_workspace_cleanup_intents "
        "WHERE owner_kind='thread' AND owner_id=$1::uuid "
        "AND thread_runtime_generation=$2::uuid",
        case.thread_id,
        str(predecessor["thread_runtime_generation"]),
    )
    physical = {name: obj.metadata.uid for name, obj in case.cluster.objects.items()}
    with pytest.raises(PostgresError):
        if changed_authority == "predecessor_cleanup":
            await database.execute(
                "UPDATE managed_repository_workspace_cleanup_intents "
                "SET resource_policy='terminal_reclaim',"
                "reclaim_shared_resources=true "
                "WHERE owner_kind='thread' AND owner_id=$1::uuid "
                "AND thread_runtime_generation=$2::uuid",
                case.thread_id,
                str(predecessor["thread_runtime_generation"]),
            )
        elif changed_authority == "current_generation":
            await database.execute(
                "UPDATE threads SET runtime_generation=$2::uuid WHERE id=$1::uuid",
                case.thread_id,
                str(uuid4()),
            )
        else:
            await database.execute(
                "UPDATE threads SET metadata=jsonb_set(metadata,"
                "'{workspace_container,_creation_claim_token}',to_jsonb($2::text)) "
                "WHERE id=$1::uuid",
                case.thread_id,
                str(attention["claim_token"] + 1),
            )
    assert await database.get_thread(case.thread_id) == before_thread
    assert (
        await database.fetchrow(
            "SELECT * FROM run_queue WHERE unit_id=$1::uuid", case.thread_id
        )
        == before_queue
    )
    assert (
        await database.fetchrow(
            "SELECT * FROM managed_repository_workspace_cleanup_intents "
            "WHERE owner_kind='thread' AND owner_id=$1::uuid "
            "AND thread_runtime_generation=$2::uuid",
            case.thread_id,
            str(predecessor["thread_runtime_generation"]),
        )
        == before_cleanup
    )
    assert not await database.fetchval(
        "SELECT EXISTS(SELECT 1 FROM managed_repository_workspace_cleanup_intents "
        "WHERE owner_kind='thread' AND owner_id=$1::uuid "
        "AND thread_runtime_generation=$2::uuid)",
        case.thread_id,
        str(attention["thread_runtime_generation"]),
    )
    assert case.cluster.pod_create_calls == 2
    assert {
        name: obj.metadata.uid for name, obj in case.cluster.objects.items()
    } == physical


@pytest.mark.asyncio
@pytest.mark.parametrize("pending_gate", ["permission", "interrupt"])
async def test_retained_attention_begin_preserves_pending_permission_or_interrupt(
    database, actor, monkeypatch, pending_gate
):
    from uuid import uuid4

    from orchestrator.services.container_provisioner import WorkspaceOwner

    case, _, attention, _ = await retained_attention_case(database, actor, monkeypatch)
    captured = (
        await case.provisioner.capture_retained_stateless_startup_attention_retirement(
            WorkspaceOwner.session(case.thread_id)
        )
    )
    assert captured
    request_id = uuid4()
    if pending_gate == "permission":
        await database.execute(
            "INSERT INTO thread_permission_requests "
            "(id,thread_id,tool_call_id,tool_name) "
            "VALUES($1,$2::uuid,$3,'bash')",
            request_id,
            case.thread_id,
            f"tool-{request_id}",
        )
        table = "thread_permission_requests"
    else:
        await database.execute(
            "INSERT INTO thread_interrupt_requests "
            "(id,thread_id,client_request_id,target_turn_id,"
            "accepted_lease_token,accepted_leased_by,requested_by) "
            "VALUES($1,$2::uuid,$3,1,2,'old-worker','user')",
            request_id,
            case.thread_id,
            uuid4(),
        )
        table = "thread_interrupt_requests"
    before_request = await database.fetchrow(
        f"SELECT * FROM {table} WHERE id=$1", request_id
    )
    before_thread = await database.get_thread(case.thread_id)
    before_queue = await database.fetchrow(
        "SELECT * FROM run_queue WHERE unit_id=$1::uuid", case.thread_id
    )
    physical = {name: obj.metadata.uid for name, obj in case.cluster.objects.items()}
    result = await database.begin_stateless_thread_workspace_retirement(
        case.thread_id,
        force=False,
        permanent=False,
        retained_startup_attention=captured,
    )
    assert result["state"] == "busy"
    assert result[f"pending_{pending_gate}"] is True
    assert (
        await database.fetchrow(f"SELECT * FROM {table} WHERE id=$1", request_id)
        == before_request
    )
    assert await database.get_thread(case.thread_id) == before_thread
    assert (
        await database.fetchrow(
            "SELECT * FROM run_queue WHERE unit_id=$1::uuid", case.thread_id
        )
        == before_queue
    )
    assert (
        await database.fetchval(
            "SELECT count(*) FROM managed_repository_workspace_cleanup_intents "
            "WHERE owner_kind='thread' AND owner_id=$1::uuid "
            "AND thread_runtime_generation=$2::uuid",
            case.thread_id,
            str(attention["thread_runtime_generation"]),
        )
        == 0
    )
    assert case.cluster.pod_create_calls == 2
    assert {
        name: obj.metadata.uid for name, obj in case.cluster.objects.items()
    } == physical


@pytest.mark.asyncio
async def test_retained_attention_end_refuses_replaced_pvc_after_stop_before_ack(
    database, actor, monkeypatch
):
    from uuid import uuid4

    from fastapi import HTTPException

    case, _, attention, dependencies = await retained_attention_case(
        database, actor, monkeypatch
    )
    reconcile = case.provisioner.reconcile_workspace_cleanup_intent
    acknowledgement = type(database).acknowledge_stateless_thread_runtime_process_zero
    acknowledgements = 0
    replacement_uid = str(uuid4())

    async def replace_after_stop(*args, **kwargs):
        outcome = await reconcile(*args, **kwargs)
        if outcome.settled:
            case.cluster.objects["pvc"].metadata.uid = replacement_uid
        return outcome

    async def record_ack(store, *args, **kwargs):
        nonlocal acknowledgements
        acknowledgements += 1
        return await acknowledgement(store, *args, **kwargs)

    monkeypatch.setattr(
        case.provisioner, "reconcile_workspace_cleanup_intent", replace_after_stop
    )
    monkeypatch.setattr(
        type(database),
        "acknowledge_stateless_thread_runtime_process_zero",
        record_ack,
    )
    with pytest.raises(HTTPException) as refused:
        await end_thread_flow(
            case.thread_id,
            await database.get_thread(case.thread_id),
            permanent=False,
            force=False,
            dependencies=dependencies,
        )
    assert refused.value.status_code == 503
    assert acknowledgements == 0
    current = await database.get_thread(case.thread_id)
    assert current["status"] == "ended"
    marker = metadata(current)["_stateless_claim_retirement"]
    assert marker["remote_retired"] is False
    assert marker["residents_retired"] is False
    assert "_stateless_workspace_retirement_settled" not in metadata(current)
    queue = await database.fetchrow(
        "SELECT * FROM run_queue WHERE unit_id=$1::uuid", case.thread_id
    )
    assert queue["state"] == "done"
    assert queue["lease_token"] == marker["terminal_token"]
    receipt = await database.fetchrow(
        "SELECT * FROM managed_repository_workspace_creation_reservations "
        "WHERE id=$1::uuid",
        str(attention["id"]),
    )
    assert receipt["result_kind"] == "aborted"
    cleanup = await database.fetchrow(
        "SELECT * FROM managed_repository_workspace_cleanup_intents "
        "WHERE owner_kind='thread' AND owner_id=$1::uuid "
        "AND thread_runtime_generation=$2::uuid",
        case.thread_id,
        str(attention["thread_runtime_generation"]),
    )
    assert cleanup["result_kind"] == "settled"
    assert case.cluster.objects["pvc"].metadata.uid == replacement_uid
    assert "pod" not in case.cluster.objects
    assert case.cluster.pod_create_calls == 2


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
@pytest.mark.parametrize(
    "late_change",
    (
        None,
        "seed_uid",
        "seed_missing",
        "seed_controller_wrong",
        "seed_controller_ambiguous",
        "service_uid",
        "seed_uid_after_ssh",
        "seed_controller_ambiguous_after_ssh",
        "service_uid_after_ssh",
    ),
    ids=lambda change: change or "healthy",
)
async def test_interrupted_retained_resume_is_rediscovered_before_scheduling(
    database, actor, monkeypatch, late_change
):
    import asyncio
    from datetime import datetime, timezone
    from orchestrator.services.session_creation_continuation import (
        SessionCreationContinuationRunner,
    )
    from orchestrator.services.session_provisioner import ensure_session_workspace
    from orchestrator.services import stateless_session_retirement as protocol
    from tests.test_session_created_source_rediscovery_real_postgres import (
        reconstructed_provider,
    )

    # G1 itself is a settled v1 Ready create, matching the installed receipt
    # shape: settlement leaves its startup state at readiness/starting.
    original_create = continuation.DelayedWorkspaceCluster.create_namespaced_pod
    original_ready = continuation.DelayedWorkspaceCluster.become_ready

    def scheduled_g1(self, *, body, **kwargs):
        pod = original_create(self, body=body, **kwargs)
        pod.spec.node_name = "node8"
        pod.status.conditions = [
            SimpleNamespace(
                type="PodScheduled",
                status="True",
                last_transition_time=datetime.now(timezone.utc),
            )
        ]
        return pod

    def ready_g1(self):
        original_ready(self)
        self.objects["pod"].status.conditions.append(
            SimpleNamespace(
                type="Ready",
                status="True",
                last_transition_time=datetime.now(timezone.utc),
            )
        )

    monkeypatch.setattr(
        continuation.DelayedWorkspaceCluster,
        "create_namespaced_pod",
        scheduled_g1,
    )
    monkeypatch.setattr(continuation.DelayedWorkspaceCluster, "become_ready", ready_g1)
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    case = await workspace_attempt(
        database, actor, monkeypatch, first_wait="ready", seeded=True
    )
    assert case.creation["startup_protocol_version"] == 1
    assert case.creation["startup_stage"] == "readiness"
    assert case.creation["startup_state"] == "starting"
    assert case.creation["startup_first_ready_at"] is not None
    assert (
        str(case.creation["seed_configmap_uid"])
        == case.cluster.objects["seed"].metadata.uid
    )
    monkeypatch.setattr(
        continuation.DelayedWorkspaceCluster,
        "create_namespaced_pod",
        original_create,
    )
    monkeypatch.setattr(
        continuation.DelayedWorkspaceCluster, "become_ready", original_ready
    )
    await database.execute(
        "INSERT INTO run_queue(unit_id,unit_kind,state,lease_token) "
        "VALUES($1::uuid,'session_turn','done',2)",
        case.thread_id,
    )

    async def residents(thread, *, terminal_token, **_):
        return protocol.ResidentRetirementProof(
            authority=protocol.resolve_shell_retirement_authority(
                thread, terminal_token=terminal_token
            )
        )

    async def shell(thread, *, terminal_token, **_):
        return protocol.resolve_shell_retirement_authority(
            thread, terminal_token=terminal_token
        )

    monkeypatch.setattr(protocol, "retire_stateless_workspace_residents", residents)
    monkeypatch.setattr(protocol, "retire_stateless_session_shell", shell)
    monkeypatch.setattr(protocol, "verify_stateless_workspace_residents_retired", shell)
    from dataclasses import replace

    # Simulate the Pod-owned seed disappearing between terminal attestation
    # and the final physical cleanup capture. The latter uses the production
    # read-only capture helper and observes its legitimate ConfigMap 404.
    original_terminal_capture = case.provisioner.capture_terminal_workspace_identity

    async def capture_after_seed_absence(owner):
        terminal = await original_terminal_capture(owner)
        case.cluster.objects.pop("seed")
        absent = await case.provisioner.capture_workspace_teardown_identity(owner)
        assert absent.seed_configmap_uid is None
        assert absent.pod_uid == terminal.pod_uid
        return replace(terminal, seed_configmap_uid=absent.seed_configmap_uid)

    monkeypatch.setattr(
        case.provisioner,
        "capture_terminal_workspace_identity",
        capture_after_seed_absence,
    )

    end_dependencies = replace(
        retirement_dependencies(database, case),
        build_agent_cloud_mount=AsyncMock(return_value=None),
    )
    assert await end_thread_flow(
        case.thread_id,
        case.before,
        permanent=False,
        force=False,
        dependencies=end_dependencies,
    ) == {"status": "ended"}
    cleanup = await database.fetchrow(
        "SELECT * FROM managed_repository_workspace_cleanup_intents "
        "WHERE owner_kind='thread' AND owner_id=$1::uuid "
        "AND thread_runtime_generation=$2::uuid",
        case.thread_id,
        case.creation["thread_runtime_generation"],
    )
    assert cleanup["capture_complete"] is True
    assert cleanup["seed_configmap_uid"] is None
    assert cleanup["pvc_uid"] == case.creation["pvc_uid"]
    await resume_case(database, case, actor)
    create_pod = case.cluster.create_namespaced_pod

    def unscheduled_pod(*, body, **kwargs):
        pod = create_pod(body=body, **kwargs)
        pod.spec.node_name = None
        pod.status.phase = "Pending"
        pod.status.conditions = [
            SimpleNamespace(type="PodScheduled", status="False", reason="Unschedulable")
        ]
        return pod

    monkeypatch.setattr(case.cluster, "create_namespaced_pod", unscheduled_pod)
    await ensure_session_workspace(
        case.thread_id,
        db=database,
        provisioner=case.provisioner,
        suspension=case.suspension,
    )
    source = await database.fetchrow(
        "SELECT * FROM managed_repository_workspace_creation_reservations "
        "WHERE owner_kind='thread' AND owner_id=$1::uuid "
        "AND settled_at IS NULL",
        case.thread_id,
    )
    assert source["startup_protocol_version"] == 1
    assert source["startup_state"] == "waiting_capacity"
    assert source["scheduled_at"] is None
    assert source["pvc_uid"] == case.creation["pvc_uid"]
    assert (
        str(source["seed_configmap_uid"]) == case.cluster.objects["seed"].metadata.uid
    )
    assert source["seed_configmap_uid"] != case.creation["seed_configmap_uid"]
    mutations = []
    for verb in ("create", "patch", "delete", "replace"):
        for kind in ("pod", "persistent_volume_claim", "service", "config_map"):
            method = f"{verb}_namespaced_{kind}"

            def refuse_mutation(*args, _method=method, **kwargs):
                mutations.append(_method)
                raise AssertionError(
                    f"background continuation mutated Kubernetes: {_method}"
                )

            monkeypatch.setattr(case.cluster, method, refuse_mutation, raising=False)
    # Capable gate-off instances must finish a receipt that was already v1.
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "false")
    restarted = type(database)(
        database._connection_string, min_connections=1, max_connections=3
    )
    await restarted.connect()
    try:
        provider = reconstructed_provider(restarted, case, monkeypatch)
        page = await restarted.list_current_session_creation_candidates()
        assert len(page.candidates) == 1
        (candidate,) = page.candidates
        assert candidate.route == "retained_create"
        assert await restarted.current_session_creation_candidate_is_exact(candidate)
        from dataclasses import replace as replace_candidate
        from uuid import uuid4

        for change in (
            {"runtime_generation": str(uuid4())},
            {"claim_token": candidate.claim_token + 1},
            {"pod_uid": str(uuid4())},
            {"pvc_uid": str(uuid4())},
            {"seed_configmap_uid": str(uuid4())},
            {"source_fingerprint": "0" * 64},
        ):
            assert not await restarted.current_session_creation_candidate_is_exact(
                replace_candidate(candidate, **change)
            ), change

        import copy
        import json
        from uuid import UUID
        from orchestrator.database.session_creation_candidates import (
            SESSION_CREATION_SCAN_SQL,
            candidate_from_record,
        )

        raw = dict(
            await restarted.fetchrow(
                SESSION_CREATION_SCAN_SQL, UUID(case.thread_id), None, None, None, 2
            )
        )
        for key in (
            "owner",
            "source",
            "retained_predecessors",
            "retained_cleanups",
            "retained_process_zeroes",
        ):
            if isinstance(raw[key], str):
                raw[key] = json.loads(raw[key])
        assert candidate_from_record(raw) == candidate
        refusals = (
            ("open_cleanup", lambda row: row.update(conflicting_authority=True)),
            (
                "unsettled_predecessor",
                lambda row: row["retained_predecessors"][0].update(
                    phase="runtime_bound"
                ),
            ),
            (
                "predecessor_attention",
                lambda row: row["retained_predecessors"][0].update(
                    startup_state="attention"
                ),
            ),
            (
                "incomplete_cleanup",
                lambda row: row["retained_cleanups"][0].update(
                    cleanup_completed_at=None
                ),
            ),
            (
                "not_ready_predecessor",
                lambda row: row["retained_cleanups"][0]["lifecycle_fingerprint"].update(
                    runtime_status="failed"
                ),
            ),
            (
                "missing_process_zero",
                lambda row: row.update(retained_process_zeroes=None),
            ),
            (
                "contradictory_captured_seed",
                lambda row: row["retained_cleanups"][0].update(
                    seed_configmap_uid=str(uuid4())
                ),
            ),
            ("current_end", lambda row: row["owner"].update(status="ended")),
            (
                "claim_loss_hold",
                lambda row: row["owner"]["metadata"].update(
                    _stateless_claim_loss_hold={}
                ),
            ),
            (
                "restore_marker",
                lambda row: row["owner"]["metadata"]["workspace_container"].update(
                    _snapshot_restore_required=True
                ),
            ),
            (
                "restore_operation",
                lambda row: row["source"].update(operation_kind="restore"),
            ),
            (
                "current_cancellation",
                lambda row: row["source"].update(
                    cancel_requested_at=row["source"]["created_at"]
                ),
            ),
            (
                "replaced_binding",
                lambda row: row["owner"]["metadata"]["_workspace_binding"].update(
                    backing_id="k8s-pvc:agent-workspaces:" + str(uuid4())
                ),
            ),
            (
                "bindingless_initial",
                lambda row: row["owner"]["metadata"].pop("_workspace_binding"),
            ),
        )
        for name, mutate in refusals:
            altered = copy.deepcopy(raw)
            mutate(altered)
            assert candidate_from_record(altered) is None, name
        runner = SessionCreationContinuationRunner(
            db=restarted, provisioner=provider, shutdown_event=asyncio.Event()
        )
        for kind in ("pod", "pvc", "seed"):
            resource = case.cluster.objects[kind]
            original_uid = resource.metadata.uid
            resource.metadata.uid = str(uuid4())
            try:
                (changed_candidate,) = (
                    await restarted.list_current_session_creation_candidates()
                ).candidates
                assert not await runner._continue(changed_candidate)
                refused = await restarted.fetchrow(
                    "SELECT settled_at FROM managed_repository_workspace_creation_reservations WHERE id=$1",
                    source["id"],
                )
                assert refused["settled_at"] is None
            finally:
                resource.metadata.uid = original_uid
        seed = case.cluster.objects["seed"]
        original_references = seed.metadata.owner_references
        for changed_references in (
            [],
            original_references + [original_references[0]],
        ):
            seed.metadata.owner_references = changed_references
            try:
                (changed_candidate,) = (
                    await restarted.list_current_session_creation_candidates()
                ).candidates
                assert not await runner._continue(changed_candidate)
            finally:
                seed.metadata.owner_references = original_references
        assert mutations == []
        (candidate,) = (
            await restarted.list_current_session_creation_candidates()
        ).candidates
        pod = case.cluster.objects["pod"]
        scheduled = datetime.now(timezone.utc)
        pod.spec.node_name = "node8"
        pod.status.conditions = [
            SimpleNamespace(
                type="PodScheduled", status="True", last_transition_time=scheduled
            )
        ]
        # First continuation freezes the physical scheduling timestamp.
        assert not await runner._continue(candidate)
        still_open = await restarted.fetchrow(
            "SELECT * FROM managed_repository_workspace_creation_reservations WHERE id=$1",
            source["id"],
        )
        assert still_open["scheduled_at"] == scheduled
        assert not await restarted.current_session_creation_candidate_is_exact(
            candidate
        )
        case.cluster.become_ready()
        pod.status.conditions.append(
            SimpleNamespace(type="Ready", status="True", last_transition_time=scheduled)
        )
        (candidate,) = (
            await restarted.list_current_session_creation_candidates()
        ).candidates
        if late_change is not None:
            # _wait_for_ready follows the early UID checks and resource receipt
            # recording. Inject external replacement at that awaited boundary.
            original_wait = provider._wait_for_ready
            restorations = []
            injections = []
            change_kind = late_change.removesuffix("_after_ssh")

            def change_physical_identity():
                injections.append(late_change)
                if change_kind == "seed_missing":
                    original = case.cluster.objects.pop("seed")
                    restorations.append(
                        lambda: case.cluster.objects.__setitem__("seed", original)
                    )
                elif change_kind == "service_uid":
                    service = case.cluster.objects["service"]
                    original = service.metadata.uid
                    service.metadata.uid = str(uuid4())
                    restorations.append(
                        lambda: setattr(service.metadata, "uid", original)
                    )
                else:
                    seed = case.cluster.objects["seed"]
                    if change_kind == "seed_uid":
                        original = seed.metadata.uid
                        seed.metadata.uid = str(uuid4())
                        restorations.append(
                            lambda: setattr(seed.metadata, "uid", original)
                        )
                    else:
                        original = seed.metadata.owner_references
                        if change_kind == "seed_controller_wrong":
                            changed = copy.deepcopy(original)
                            changed[0]["uid"] = str(uuid4())
                        else:
                            changed = original + [dict(original[0])]
                        seed.metadata.owner_references = changed
                        restorations.append(
                            lambda: setattr(seed.metadata, "owner_references", original)
                        )

            async def inject_after_record(*args, **kwargs):
                recorded = await restarted.fetchrow(
                    "SELECT seed_configmap_uid,service_uid FROM "
                    "managed_repository_workspace_creation_reservations WHERE id=$1",
                    source["id"],
                )
                assert recorded["seed_configmap_uid"] == source["seed_configmap_uid"]
                assert recorded["service_uid"] == source["service_uid"]
                if late_change.endswith("_after_ssh"):
                    ready_ip = await original_wait(*args, **kwargs)
                    assert ready_ip is not None
                    change_physical_identity()
                    return ready_ip
                change_physical_identity()
                return await original_wait(*args, **kwargs)

            monkeypatch.setattr(provider, "_wait_for_ready", inject_after_record)
            assert not await runner._continue(candidate)
            assert injections == [late_change]
            refused = await restarted.fetchrow(
                "SELECT settled_at FROM managed_repository_workspace_creation_reservations WHERE id=$1",
                source["id"],
            )
            assert refused["settled_at"] is None
            assert mutations == []
            for restore in restorations:
                restore()
            monkeypatch.setattr(provider, "_wait_for_ready", original_wait)
            (candidate,) = (
                await restarted.list_current_session_creation_candidates()
            ).candidates
        assert await runner._continue(candidate)
        settled = await restarted.fetchrow(
            "SELECT * FROM managed_repository_workspace_creation_reservations WHERE id=$1",
            source["id"],
        )
        assert settled["settled_at"] is not None
        assert settled["startup_first_ready_at"] == scheduled
        completed_metadata = metadata(await restarted.get_thread(case.thread_id))
        assert completed_metadata["workspace_container"]["status"] == "ready"
        assert (
            completed_metadata["_workspace_binding"]["ssh_host_key_fingerprint"]
            == continuation.FINGERPRINT
        )
        assert completed_metadata["_workspace_binding"]["backing_id"].endswith(
            case.pvc_uid
        )
        assert case.cluster.objects["pod"].metadata.uid == str(source["pod_uid"])
        assert case.cluster.objects["pvc"].metadata.uid == case.pvc_uid
        assert not (
            await restarted.list_current_session_creation_candidates()
        ).candidates
        assert not await runner._continue(candidate)
        assert case.cluster.pod_create_calls == 2
        assert mutations == []
    finally:
        await restarted.close()


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
@pytest.mark.parametrize("unsafe", ["missing_finalizer", "replacement_pvc"])
async def test_initial_end_refuses_unproven_fresh_owned_storage(
    database, actor, monkeypatch, unsafe
):
    from uuid import uuid4
    from fastapi import HTTPException

    case = await workspace_attempt(database, actor, monkeypatch)
    pod = case.cluster.objects["pod"]
    if unsafe == "missing_finalizer":
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
@pytest.mark.parametrize("running", ["restarted", "init", "ephemeral"])
async def test_initial_running_capture_does_not_confer_stop_proof(
    database, actor, monkeypatch, running
):
    from copy import deepcopy
    from orchestrator.services.workspace_lifecycle import WorkspaceOwner

    case = await workspace_attempt(database, actor, monkeypatch)
    pod = case.cluster.objects["pod"]
    if running == "restarted":
        pod.status.container_statuses[0].restart_count = 1
    else:
        status = deepcopy(pod.status.container_statuses[0])
        status.started = True
        setattr(pod.status, f"{running}_container_statuses", [status])
    captured = await case.provisioner.capture_initial_stateless_creation_retirement(
        WorkspaceOwner.session(case.thread_id)
    )
    assert captured is not None
    assert captured["runtime_incarnation"] == pod.metadata.uid
    assert (await database.get_thread(case.thread_id))["status"] == "created"
    assert case.cluster.pod_deletes == 0
    assert not await database.fetchval(
        "SELECT EXISTS(SELECT 1 FROM managed_repository_process_zero_receipts "
        "WHERE owner_id=$1::uuid)",
        case.thread_id,
    )


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


@pytest.mark.asyncio
async def test_background_scan_advances_past_held_batch_and_wraps(
    database, actor, monkeypatch
):
    """An accepted 26th End must not depend on 25 unsafe holds clearing."""
    import asyncio
    from uuid import UUID, uuid4

    from orchestrator.services import stale_agent_detector as detector
    from tests.test_stale_agent_detector import _detector_dependencies, _mock_db

    begin = type(database).begin_stateless_thread_workspace_retirement

    async def disconnected(store, *args, **kwargs):
        await begin(store, *args, **kwargs)
        raise asyncio.CancelledError()

    cases = {}
    with monkeypatch.context() as patch:
        patch.setattr(
            type(database), "begin_stateless_thread_workspace_retirement", disconnected
        )
        for _ in range(26):
            case = await workspace_attempt(
                database, actor, monkeypatch, first_wait="not_ready"
            )
            cases[case.thread_id] = case
            with pytest.raises(asyncio.CancelledError):
                await end_case(database, case)

    # Scheduling hints may tie. Keep the accepted authority untouched and
    # exercise the UUID part of the cursor against an actual PostgreSQL sort.
    first = await database.get_thread(next(iter(cases)))
    await database.execute(
        "UPDATE threads SET ended_at=$1 WHERE id=ANY($2::uuid[])",
        first["ended_at"],
        [UUID(thread_id) for thread_id in cases],
    )
    ordered = sorted(cases)
    for thread_id in ordered[:25]:
        cases[thread_id].cluster.objects["pvc"].metadata.uid = str(uuid4())

    class ImmediateCadence(asyncio.Event):
        async def wait(self):
            if not self.is_set():
                raise asyncio.TimeoutError()
            return True

    shutdown = ImmediateCadence()
    store = _mock_db(shutdown)
    sweep = 0
    pages = []
    visits = []

    def next_sweep(**_):
        nonlocal sweep
        sweep += 1
        if sweep == 3:
            shutdown.set()
        return []

    async def discover(**kwargs):
        rows = await database.list_retryable_initial_creation_retirements(**kwargs)
        pages.append([str(row["id"]) for row in rows])
        return rows

    async def real_end(thread_id, thread, **kwargs):
        visits.append((sweep, thread_id))
        return await end_thread_flow(
            thread_id,
            thread,
            **kwargs,
            dependencies=retirement_dependencies(database, cases[thread_id]),
        )

    store.mark_stale_agents_offline = AsyncMock(side_effect=next_sweep)
    store.list_retryable_initial_creation_retirements = AsyncMock(side_effect=discover)
    store.get_thread = database.get_thread
    await detector.stale_agent_detector(
        shutdown,
        dependencies=_detector_dependencies(
            store,
            thread_retirement_operations=lambda: SimpleNamespace(
                end_thread_flow=real_end
            ),
        ),
    )

    assert pages == [ordered[:25], ordered[25:], ordered[:25]]
    assert (2, ordered[25]) in visits
    assert (await database.get_thread(ordered[25]))["status"] == "ended"
    settled = metadata(await database.get_thread(ordered[25]))
    assert "_stateless_workspace_retirement_settled" in settled
    for thread_id in ordered[:25]:
        assert (1, thread_id) in visits and (3, thread_id) in visits
        assert cases[thread_id].cluster.pod_deletes == 0
        assert "_stateless_workspace_retirement_pending" in metadata(
            await database.get_thread(thread_id)
        )
