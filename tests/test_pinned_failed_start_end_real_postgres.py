"""Pinned failed-start End uses the captured create and current actor authority."""

from __future__ import annotations

import json
from uuid import UUID, uuid4

import pytest
import asyncpg
from types import SimpleNamespace

from orchestrator import main
from orchestrator.application import controls
from orchestrator.services import container_provisioner as provider_module

from tests import test_persistent_recycler_real_postgres as fixtures
from tests import test_workspace_pull_failure_real_postgres as pull
from tests.test_container_provisioner import _configmap_from_manifest

db = fixtures.db
pg_dsn = fixtures.pg_dsn
_schema_applied = pull._schema_applied


class PinnedPullCluster(pull.NeverPullingCluster):
    def create_namespaced_pod(self, *, body, **kwargs):
        pod = super().create_namespaced_pod(body=body, **kwargs)
        # The real client returns container models, including declared names.
        # Preserve that shape for complete terminal-container verification.
        for field in ("containers", "init_containers", "ephemeral_containers"):
            setattr(
                pod.spec,
                field,
                [SimpleNamespace(**item) for item in getattr(pod.spec, field)],
            )
        return pod

    def create_namespaced_service(self, *, body, **kwargs):
        body = {**body, "spec": {"ports": [], **body["spec"]}}
        return super().create_namespaced_service(body=body, **kwargs)

    def create_namespaced_config_map(self, *, body, **_):
        self.objects["seed"] = _configmap_from_manifest(body, uid=str(uuid4()))
        return self.objects["seed"]

    def delete_namespaced_config_map(self, *, name, body=None, **_):
        self._delete("seed", name, body)


async def _failed_start(db, monkeypatch, *, virtual_backing_root=None):
    ids = await fixtures._seed(db, bind_agent=False, publish_agent_pod=False)
    async with db.acquire() as conn:
        await conn.execute(
            "DELETE FROM project_officers WHERE thread_id=$1", UUID(ids["thread"])
        )
        await conn.execute(
            "UPDATE threads SET config_name='assistant',metadata=$2::jsonb WHERE id=$1",
            UUID(ids["thread"]),
            json.dumps(
                {
                    "config_override": {
                        "officer": {"enabled": False},
                        "workspace": {"backend": "sandbox"},
                    }
                }
            ),
        )
    if virtual_backing_root is not None:
        from orchestrator.services.workspace_binding import (
            ensure_virtual_thread_workspace_binding,
        )

        monkeypatch.setenv("VIRTUAL_WORKSPACE_RCLONE_TYPE", "local")
        monkeypatch.setenv("VIRTUAL_WORKSPACE_RCLONE_ROOT", str(virtual_backing_root))
        assert await ensure_virtual_thread_workspace_binding(db, ids["thread"])
    cluster = PinnedPullCluster()
    provider = pull._provisioner(monkeypatch, db, cluster)
    assert not await provider.create_pinned_thread_workspace(ids["thread"])
    async with db.acquire() as conn:
        intent = dict(
            await conn.fetchrow(
                "SELECT * FROM thread_workspace_provision_intents WHERE thread_id=$1",
                UUID(ids["thread"]),
            )
        )
    assert intent["created_agent_id"] is None
    assert intent["pod_uid"] == cluster.objects["pod"].metadata.uid
    return ids, cluster, provider, intent


async def _attach_after_start(db, thread_id):
    thread = await db.get_thread(thread_id)
    generation = str(thread["runtime_generation"])
    attempt = str(uuid4())
    agent = uuid4()
    token = uuid4()
    pod_name = f"agent-{thread_id[:12]}"
    assert await db.reserve_pinned_agent_pod_provision_intent(
        thread_id,
        expected_runtime_generation=generation,
        attempt_id=attempt,
        pod_name=pod_name,
        provisioner="agent",
        namespace="agents-a",
    )
    assert await db.publish_pinned_agent_pod_provision_intent(
        thread_id,
        expected_runtime_generation=generation,
        attempt_id=attempt,
        pod_name=pod_name,
        pod_uid="agent-original",
        namespace="agents-a",
    )
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO agents(id,config_name,hostname,pod_ip,pod_uid,status,agent_mode,last_heartbeat) VALUES($1,'assistant',$2,'127.0.0.2','agent-original','session','persistent',now())",
                agent,
                pod_name,
            )
            await conn.execute(
                "UPDATE threads SET agent_id=$2,control_admission_agent_id=$2,runtime_attach_token=$3 WHERE id=$1",
                UUID(thread_id),
                agent,
                token,
            )
            await conn.execute(
                "UPDATE agents SET thread_id=$2 WHERE id=$1", agent, UUID(thread_id)
            )
    return str(agent), str(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("permanent", [False, True])
async def test_failed_start_end_captures_the_later_attached_actor(
    db, monkeypatch, permanent
):
    ids, _, _, intent = await _failed_start(db, monkeypatch)
    agent, token = await _attach_after_start(db, ids["thread"])

    retirement = await db.begin_pinned_thread_retirement(
        ids["thread"], permanent=permanent
    )

    assert retirement["state"] == "pending", retirement
    assert retirement["context"]["agent_id"] == agent
    assert retirement["context"]["runtime_attach_token"] == token
    assert (
        retirement["context"]["workspace_provision_intent"]["created_agent_id"] is None
    )
    assert retirement["context"]["workspace_provision_intent"]["attempt_id"] == str(
        intent["attempt_id"]
    )


@pytest.mark.asyncio
async def test_failed_start_end_fences_exact_terminal_pod(db, monkeypatch):
    ids, cluster, provider, intent = await _failed_start(db, monkeypatch)
    retirement = await db.begin_pinned_thread_retirement(ids["thread"], permanent=True)
    assert retirement["state"] == "pending"
    assert await db.authorize_pinned_thread_retirement(
        ids["thread"],
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    revoked = await db.revoke_pinned_thread_workspace_provision_intent(
        ids["thread"],
        expected_runtime_generation=retirement["generation"],
        expected_retirement_token=retirement["token"],
        expected_attempt_id=str(intent["attempt_id"]),
    )

    fences = await provider.fence_pinned_workspace_provision_intent(
        revoked, permanent=True, expected_retirement_token=retirement["token"]
    )

    assert fences is not None
    assert fences["fence_pod_uid"] != intent["pod_uid"]
    assert cluster.objects["pod"].metadata.uid == fences["fence_pod_uid"]


async def _retire_unbound(db, provider, thread_id, *, permanent):
    retirement = await db.begin_pinned_thread_retirement(thread_id, permanent=permanent)
    assert retirement["state"] == "pending", retirement
    assert await db.authorize_pinned_thread_retirement(
        thread_id,
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    attempt = retirement["context"]["workspace_provision_intent"]["attempt_id"]
    current = await db.revoke_pinned_thread_workspace_provision_intent(
        thread_id,
        expected_runtime_generation=retirement["generation"],
        expected_retirement_token=retirement["token"],
        expected_attempt_id=attempt,
    )
    fences = await provider.fence_pinned_workspace_provision_intent(
        current,
        permanent=permanent,
        expected_retirement_token=retirement["token"],
        expected_retirement_generation=retirement["generation"],
    )
    assert fences is not None
    assert await db.fence_pinned_thread_workspace_provision_intent(
        thread_id,
        expected_runtime_generation=retirement["generation"],
        expected_retirement_token=retirement["token"],
        expected_attempt_id=attempt,
        **fences,
        permanent=permanent,
    )
    if permanent:
        assert await db.clear_pinned_retirement_physical_runtime_endpoint(
            thread_id,
            runtime_generation=retirement["generation"],
            retirement_token=retirement["token"],
            completed_external_cleanup_protocol="workspace_provision_fence_v1",
        )
        await db.delete_thread(
            thread_id,
            expected_runtime_retirement_token=retirement["token"],
            expected_runtime_generation=retirement["generation"],
        )
    else:
        assert await db.settle_pinned_thread_retirement(
            thread_id,
            token=retirement["token"],
            generation=retirement["generation"],
            final_status="ended",
        )
    return retirement


@pytest.mark.asyncio
async def test_soft_failed_start_retains_exact_volume_and_permanent_upgrade_purges(
    db, monkeypatch
):
    ids, cluster, provider, intent = await _failed_start(db, monkeypatch)
    soft = await _retire_unbound(db, provider, ids["thread"], permanent=False)
    assert cluster.objects["pvc"].metadata.uid == intent["pvc_uid"]
    assert cluster.objects["service"].metadata.uid == intent["service_uid"]
    assert (await db.get_thread(ids["thread"]))["status"] == "ended"

    upgrade = await db.begin_pinned_thread_retirement(ids["thread"], permanent=True)
    assert await db.authorize_pinned_thread_retirement(
        ids["thread"],
        token=upgrade["token"],
        generation=upgrade["generation"],
        settle_status="ended",
    )
    assert not await db.revoke_pinned_thread_workspace_provision_intent(
        ids["thread"],
        expected_runtime_generation=soft["generation"],
        expected_retirement_token=soft["token"],
        expected_attempt_id=str(intent["attempt_id"]),
    )
    purge = await db.revoke_pinned_thread_workspace_provision_intent(
        ids["thread"],
        expected_runtime_generation=upgrade["generation"],
        expected_retirement_token=upgrade["token"],
        expected_attempt_id=str(intent["attempt_id"]),
    )
    assert purge["cleanup_disposition"] == "purge"
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE thread_workspace_provision_intents SET cleanup_retirement_token=$2 WHERE attempt_id=$1",
            intent["attempt_id"],
            UUID(soft["token"]),
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE thread_workspace_provision_intents SET cleanup_disposition='retain' WHERE attempt_id=$1",
            intent["attempt_id"],
        )
    assert not await provider.fence_pinned_workspace_provision_intent(
        purge, permanent=True, expected_retirement_token=soft["token"]
    )
    assert cluster.objects["pvc"].metadata.uid == intent["pvc_uid"]
    permanent = await _retire_unbound(db, provider, ids["thread"], permanent=True)

    assert permanent["token"] != soft["token"]
    assert (
        str(
            await db.fetchval(
                "SELECT retirement_token FROM thread_workspace_provision_stop_receipts WHERE attempt_id=$1",
                intent["attempt_id"],
            )
        )
        == soft["token"]
    )
    assert await db.get_thread(ids["thread"]) is None
    assert cluster.objects["pvc"].metadata.uid != intent["pvc_uid"]
    assert cluster.objects["service"].metadata.uid != intent["service_uid"]


@pytest.mark.asyncio
@pytest.mark.parametrize("permanent", [False, True])
async def test_normal_failed_start_end_survives_reconstructed_request(
    db, monkeypatch, permanent
):
    ids, cluster, provider, intent = await _failed_start(db, monkeypatch)
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(provider_module, "container_provisioner", provider)
    # A disconnected request has already committed the owner's authorization.
    retirement = await db.begin_pinned_thread_retirement(
        ids["thread"], permanent=permanent
    )
    assert await db.authorize_pinned_thread_retirement(
        ids["thread"],
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    result = await controls.thread_retirement_operations(
        main.app.state.resources
    ).end_thread_flow(
        ids["thread"],
        dict(await db.get_thread(ids["thread"])),
        permanent=permanent,
        force=False,
    )
    assert result["status"] == ("deleted" if permanent else "ended")
    if not permanent:
        assert cluster.objects["pvc"].metadata.uid == intent["pvc_uid"]
        duplicate = await controls.thread_retirement_operations(
            main.app.state.resources
        ).end_thread_flow(
            ids["thread"],
            dict(await db.get_thread(ids["thread"])),
            permanent=False,
            force=False,
        )
        assert duplicate["status"] == "ended"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "absent",
        "replacement",
        "still_waiting",
        "running",
        "no_finalizer",
        "deleting_wrong_labels",
    ],
)
async def test_failed_start_end_holds_without_exact_stop_evidence(
    db, monkeypatch, fault
):
    ids, cluster, provider, intent = await _failed_start(db, monkeypatch)
    retirement = await db.begin_pinned_thread_retirement(ids["thread"], permanent=True)
    assert await db.authorize_pinned_thread_retirement(
        ids["thread"],
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    revoked = await db.revoke_pinned_thread_workspace_provision_intent(
        ids["thread"],
        expected_runtime_generation=retirement["generation"],
        expected_retirement_token=retirement["token"],
        expected_attempt_id=str(intent["attempt_id"]),
    )
    if fault == "absent":
        del cluster.objects["pod"]
    elif fault == "replacement":
        cluster.objects["pod"].metadata.uid = str(uuid4())
    elif fault == "no_finalizer":
        cluster.objects["pod"].metadata.finalizers = []
    elif fault == "deleting_wrong_labels":
        cluster.objects["pod"].metadata.deletion_timestamp = "now"
        cluster.objects["pod"].metadata.labels["srw/job-id"] = str(uuid4())
    else:
        pod = cluster.objects["pod"]
        pod.spec.node_name = "test-node"
        if fault == "running":
            pod.status.container_statuses[0].state = SimpleNamespace(
                running=SimpleNamespace(), waiting=None, terminated=None
            )

        def delete_without_kubelet(**_):
            pod.metadata.deletion_timestamp = "now"

        cluster.delete_namespaced_pod = delete_without_kubelet
    assert (
        await provider.fence_pinned_workspace_provision_intent(
            revoked, permanent=True, expected_retirement_token=retirement["token"]
        )
        is None
    )
    assert cluster.objects["pvc"].metadata.uid == intent["pvc_uid"]
    assert (
        await db.fetchval(
            "SELECT count(*) FROM thread_workspace_provision_stop_receipts WHERE attempt_id=$1",
            intent["attempt_id"],
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    [
        "retirement_token",
        "runtime_generation",
        "thread_id",
        "pod_uid",
        "pod_name",
        "namespace",
        "attempt_id",
    ],
)
async def test_sql_rejects_stop_receipt_for_a_different_authority(
    db, monkeypatch, field
):
    ids, _, _, intent = await _failed_start(db, monkeypatch)
    retirement = await db.begin_pinned_thread_retirement(ids["thread"], permanent=False)
    assert await db.authorize_pinned_thread_retirement(
        ids["thread"],
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    await db.revoke_pinned_thread_workspace_provision_intent(
        ids["thread"],
        expected_runtime_generation=retirement["generation"],
        expected_retirement_token=retirement["token"],
        expected_attempt_id=str(intent["attempt_id"]),
    )
    receipt = {
        key: intent[key]
        for key in (
            "attempt_id",
            "thread_id",
            "runtime_generation",
            "namespace",
            "pod_name",
            "pod_uid",
        )
    }
    receipt["retirement_token"] = UUID(retirement["token"])
    receipt[field] = (
        str(uuid4()) if field in {"namespace", "pod_name", "pod_uid"} else uuid4()
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "INSERT INTO thread_workspace_provision_stop_receipts(attempt_id,thread_id,runtime_generation,namespace,pod_name,pod_uid,retirement_token) VALUES($1,$2,$3,$4,$5,$6,$7)",
            *(
                receipt[key]
                for key in (
                    "attempt_id",
                    "thread_id",
                    "runtime_generation",
                    "namespace",
                    "pod_name",
                    "pod_uid",
                    "retirement_token",
                )
            ),
        )


@pytest.mark.asyncio
async def test_purge_adapter_refuses_a_soft_retirement_token(db, monkeypatch):
    ids, cluster, provider, intent = await _failed_start(db, monkeypatch)
    retirement = await db.begin_pinned_thread_retirement(ids["thread"], permanent=False)
    assert await db.authorize_pinned_thread_retirement(
        ids["thread"],
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    revoked = await db.revoke_pinned_thread_workspace_provision_intent(
        ids["thread"],
        expected_runtime_generation=retirement["generation"],
        expected_retirement_token=retirement["token"],
        expected_attempt_id=str(intent["attempt_id"]),
    )
    assert (
        await provider.fence_pinned_workspace_provision_intent(
            revoked, permanent=True, expected_retirement_token=retirement["token"]
        )
        is None
    )
    assert cluster.objects["pvc"].metadata.uid == intent["pvc_uid"]
    assert cluster.pod_deletes == 0


@pytest.mark.asyncio
async def test_finalizer_patch_response_loss_replays_the_exact_stop_receipt(
    db, monkeypatch
):
    ids, cluster, provider, intent = await _failed_start(db, monkeypatch)
    retirement = await db.begin_pinned_thread_retirement(ids["thread"], permanent=False)
    assert await db.authorize_pinned_thread_retirement(
        ids["thread"],
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    revoked = await db.revoke_pinned_thread_workspace_provision_intent(
        ids["thread"],
        expected_runtime_generation=retirement["generation"],
        expected_retirement_token=retirement["token"],
        expected_attempt_id=str(intent["attempt_id"]),
    )
    original_patch = cluster.patch_namespaced_pod

    def lost_response(**kwargs):
        original_patch(**kwargs)
        raise OSError("response lost after the finalizer patch committed")

    cluster.patch_namespaced_pod = lost_response
    assert (
        await provider.fence_pinned_workspace_provision_intent(
            revoked, permanent=False, expected_retirement_token=retirement["token"]
        )
        is None
    )
    assert "pod" not in cluster.objects
    assert (
        await db.fetchval(
            "SELECT count(*) FROM thread_workspace_provision_stop_receipts WHERE attempt_id=$1",
            intent["attempt_id"],
        )
        == 1
    )
    provider = pull._provisioner(monkeypatch, db, cluster)
    assert await provider.fence_pinned_workspace_provision_intent(
        revoked, permanent=False, expected_retirement_token=retirement["token"]
    )
    assert cluster.objects["pvc"].metadata.uid == intent["pvc_uid"]
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE thread_workspace_provision_stop_receipts SET pod_uid=$2 WHERE attempt_id=$1",
            intent["attempt_id"],
            str(uuid4()),
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "DELETE FROM thread_workspace_provision_stop_receipts WHERE attempt_id=$1",
            intent["attempt_id"],
        )


@pytest.mark.asyncio
async def test_retry_query_pages_past_a_full_batch_without_changing_authority(db):
    ids = []
    for _ in range(27):
        owner = await fixtures._seed(db, bind_agent=False, publish_agent_pod=False)
        retirement = await db.begin_pinned_thread_retirement(
            owner["thread"], permanent=False
        )
        assert await db.authorize_pinned_thread_retirement(
            owner["thread"],
            token=retirement["token"],
            generation=retirement["generation"],
            settle_status="ended",
        )
        ids.append(owner["thread"])
    first = await db.list_retryable_pinned_retirements(grace_seconds=0, limit=25)
    assert len(first) == 25
    after = (first[-1]["runtime_retirement_started_at"], str(first[-1]["id"]))
    second = await db.list_retryable_pinned_retirements(
        grace_seconds=0, limit=25, after=after
    )
    assert len(second) == 2
    assert {str(row["id"]) for row in first + second} == set(ids)
    for row in first + second:
        current = await db.get_thread(str(row["id"]))
        assert current["runtime_retirement_token"] == row["runtime_retirement_token"]
        assert (
            current["runtime_retirement_authorized_at"]
            == row["runtime_retirement_authorized_at"]
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "namespace='other-namespace'",
        "pod_uid='replacement-pod'",
        "runtime_generation=uuid_generate_v4()",
        "created_agent_id=uuid_generate_v4(),created_attach_token=uuid_generate_v4()",
        "retained_source_attempt_id=uuid_generate_v4()",
        "retained_resume_generation=uuid_generate_v4()",
        "creation_effects_admitted_at=now()",
        "cleanup_disposition='purge',cleanup_retirement_token=uuid_generate_v4()",
    ],
)
async def test_sql_rejects_rewritten_original_or_unadmitted_retention_authority(
    db, monkeypatch, mutation
):
    _, cluster, _, intent = await _failed_start(db, monkeypatch)
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            f"UPDATE thread_workspace_provision_intents SET {mutation} WHERE attempt_id=$1",
            intent["attempt_id"],
        )
    assert cluster.pod_deletes == 0


@pytest.mark.asyncio
async def test_sql_does_not_turn_existing_missing_uid_into_a_never_issued_receipt(
    db, monkeypatch
):
    ids = await fixtures._seed(db, bind_agent=False, publish_agent_pod=False)
    thread = await db.get_thread(ids["thread"])
    attempt = str(uuid4())
    assert await db.reserve_pinned_thread_workspace_provision_intent(
        ids["thread"],
        expected_runtime_generation=str(thread["runtime_generation"]),
        expected_agent_id=None,
        expected_attach_token=None,
        expected_workspace_context=None,
        expected_binding_context=None,
        attempt_id=attempt,
        namespace="agent-workspaces",
        pod_name=f"ws-thread-{ids['thread'][:12]}",
        pvc_name=None,
        seed_configmap_name=None,
        service_name=None,
        retained_service_uid=None,
        network_tier="internet-only",
        manifest_fingerprint="a" * 64,
    )
    retirement = await db.begin_pinned_thread_retirement(ids["thread"], permanent=False)
    assert await db.authorize_pinned_thread_retirement(
        ids["thread"],
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    await db.revoke_pinned_thread_workspace_provision_intent(
        ids["thread"],
        expected_runtime_generation=retirement["generation"],
        expected_retirement_token=retirement["token"],
        expected_attempt_id=attempt,
    )
    with pytest.raises(
        asyncpg.CheckViolationError, match="issued Pod identity is unresolved"
    ):
        await db.fence_pinned_thread_workspace_provision_intent(
            ids["thread"],
            expected_runtime_generation=retirement["generation"],
            expected_retirement_token=retirement["token"],
            expected_attempt_id=attempt,
            fence_pod_uid=str(uuid4()),
            fence_pvc_uid=None,
            fence_configmap_uid=None,
            fence_service_uid=None,
            permanent=False,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("permanent", [False, True])
async def test_disconnected_end_with_later_actor_finishes_through_durable_retry(
    db, monkeypatch, permanent
):
    from unittest.mock import MagicMock
    from orchestrator.services import agent_provisioner as agent_module
    from orchestrator.services import stale_agent_detector
    from orchestrator.services.session_router import SessionRouterService

    ids, cluster, provider, intent = await _failed_start(db, monkeypatch)
    agent, _ = await _attach_after_start(db, ids["thread"])
    actor_api = fixtures.StatefulPinnedK8sApi()
    name = f"agent-{ids['thread'][:12]}"
    actor_api.install_old_pod(
        namespace="agents-a",
        name=name,
        uid="agent-original",
        labels={"srw/managed-by": "agent-provisioner", "srw/purpose": "job"},
    )
    actor_api.mark_terminal("agents-a", name)
    actor_provider = fixtures._production_warm_provisioner(db, actor_api)
    routes = MagicMock()
    routes.read_namespaced_service.side_effect = fixtures._K8sError(404)
    networking = MagicMock()
    networking.read_namespaced_ingress.side_effect = fixtures._K8sError(404)
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(provider_module, "container_provisioner", provider)
    monkeypatch.setattr(agent_module, "agent_provisioner", actor_provider)
    monkeypatch.setattr(
        main.app.state.resources,
        "session_router",
        SessionRouterService(
            namespace="agents-a",
            ingress_host="unused.example",
            core_api=routes,
            networking_api=networking,
        ),
    )
    retirement = await db.begin_pinned_thread_retirement(
        ids["thread"], permanent=permanent
    )
    assert await db.authorize_pinned_thread_retirement(
        ids["thread"],
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    await db.execute("UPDATE agents SET status='offline' WHERE id=$1::uuid", agent)
    candidate = dict(await db.get_thread(ids["thread"]))
    assert await stale_agent_detector.retry_pending_pinned_retirement(
        candidate,
        dependencies=controls.stale_agent_detector_dependencies(
            main.app.state.resources
        ),
    )
    assert ("agents-a", name) not in actor_api.pods
    thread = await db.get_thread(ids["thread"])
    if permanent:
        assert thread is None
        assert cluster.objects["pvc"].metadata.uid != intent["pvc_uid"]
    else:
        assert thread["status"] == "ended"
        assert cluster.objects["pvc"].metadata.uid == intent["pvc_uid"]


@pytest.mark.asyncio
async def test_rolling_upgrade_adopts_exact_already_revoking_intent(db, monkeypatch):
    ids, cluster, provider, intent = await _failed_start(db, monkeypatch)
    retirement = await db.begin_pinned_thread_retirement(ids["thread"], permanent=False)
    assert await db.authorize_pinned_thread_retirement(
        ids["thread"],
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    # The previous release's exact SQL edge can already have committed before
    # this replica restarts on 0286. No timestamps, identity or triggers change.
    await db.execute(
        "UPDATE thread_workspace_provision_intents SET status='revoking' WHERE attempt_id=$1",
        intent["attempt_id"],
    )
    revoked = await db.revoke_pinned_thread_workspace_provision_intent(
        ids["thread"],
        expected_runtime_generation=retirement["generation"],
        expected_retirement_token=retirement["token"],
        expected_attempt_id=str(intent["attempt_id"]),
    )
    assert revoked["cleanup_disposition"] == "retain"
    assert str(revoked["cleanup_retirement_token"]) == retirement["token"]
    assert revoked["created_agent_id"] is None
    assert revoked["pod_uid"] == intent["pod_uid"]
    assert await provider.fence_pinned_workspace_provision_intent(
        revoked, permanent=False, expected_retirement_token=retirement["token"]
    )
    assert cluster.objects["pvc"].metadata.uid == intent["pvc_uid"]


async def _make_existing_pod_ready(db, monkeypatch, ids, cluster, provider):
    from unittest.mock import AsyncMock

    fingerprint = "SHA256:" + "A" * 43
    pod = cluster.objects["pod"]
    pod.status.phase = "Running"
    pod.status.container_statuses[0].ready = True
    pod.status.container_statuses[0].started = True
    pod.status.container_statuses[0].state = SimpleNamespace(
        waiting=None, running=SimpleNamespace(), terminated=None
    )
    with monkeypatch.context() as ready_transport:
        ready_transport.setattr(
            provider_module,
            "wait_for_agent_ssh",
            AsyncMock(return_value=(True, 1, None)),
        )
        ready_transport.setattr(
            provider_module, "workspace_private_key_fingerprint", lambda _: fingerprint
        )
        ready_transport.setattr(
            provider_module,
            "_isolated_pod_exec",
            lambda *args, **kwargs: f"256 {fingerprint} workspace (ED25519)",
        )
        assert await provider.create_pinned_thread_workspace(ids["thread"])
    metadata = fixtures._json((await db.get_thread(ids["thread"]))["metadata"])
    assert metadata["workspace_container"]["status"] == "ready"
    return metadata["_workspace_binding"]


async def _failed_start_with_prior_binding(db, monkeypatch, tmp_path, kind):
    if kind == "virtual":
        ids, cluster, provider, intent = await _failed_start(
            db, monkeypatch, virtual_backing_root=tmp_path
        )
        return ids, cluster, provider, intent

    ids, cluster, provider, _ = await _failed_start(db, monkeypatch)
    binding = await _make_existing_pod_ready(db, monkeypatch, ids, cluster, provider)
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(provider_module, "container_provisioner", provider)
    result = await controls.thread_retirement_operations(
        main.app.state.resources
    ).end_thread_flow(
        ids["thread"],
        dict(await db.get_thread(ids["thread"])),
        permanent=False,
        force=False,
    )
    assert result["status"] == "ended"
    assert (
        cluster.objects["pvc"].metadata.uid == binding["backing_id"].rsplit(":", 1)[-1]
    )
    assert await db.resume_thread(ids["thread"])
    assert not await provider.create_pinned_thread_workspace(ids["thread"])
    intent = dict(
        await db.fetchrow(
            "SELECT * FROM thread_workspace_provision_intents WHERE thread_id=$1::uuid AND status='planned'",
            ids["thread"],
        )
    )
    assert fixtures._json(intent["previous_binding"]) == binding
    return ids, cluster, provider, intent


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["virtual", "remote"])
async def test_failed_recreation_soft_end_keeps_prior_binding_and_exact_storage(
    db, monkeypatch, tmp_path, kind
):
    ids, cluster, provider, intent = await _failed_start_with_prior_binding(
        db, monkeypatch, tmp_path, kind
    )
    binding = fixtures._json(intent["previous_binding"])
    assert binding["kind"] == kind
    await _retire_unbound(db, provider, ids["thread"], permanent=False)
    current = await db.get_thread(ids["thread"])
    metadata = fixtures._json(current["metadata"])
    assert current["status"] == "ended"
    assert metadata["_workspace_binding"] == binding
    assert cluster.objects["pvc"].metadata.uid == (
        intent["pvc_uid"] or intent["retained_pvc_uid"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("end_first", [False, True])
async def test_competing_lifecycle_probe_observes_real_postgres_locks(
    db, monkeypatch, end_first
):
    from tests.pinned_failed_start_retained_acceptance import _race_end_and_admission

    ids, _, _, intent = await _failed_start(db, monkeypatch)
    retirement, admitted = await _race_end_and_admission(
        db, ids["thread"], intent, end_first=end_first
    )
    assert retirement["state"] == "pending"
    # Initial intents cannot borrow the retained-successor no-effects protocol.
    assert admitted is False
