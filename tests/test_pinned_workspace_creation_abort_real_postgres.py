"""A failed attach must not abandon a durable workspace create obligation."""

import asyncio
import json
import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from orchestrator.services.session_attach_binding import release_session_attach_binding
from tests import test_persistent_recycler_real_postgres as authority
from tests import test_pinned_failed_start_end_real_postgres as start

db = authority.db
pg_dsn = authority.pg_dsn
_schema_applied = start._schema_applied


async def _creating(db, monkeypatch, *, published=False):
    ids = await authority._seed(db, protected_agent_pod=True, workspace_claim=False)
    await db.execute(
        "DELETE FROM project_officers WHERE thread_id=$1::uuid", ids["thread"]
    )
    await db.execute(
        "UPDATE threads SET status='created',metadata=jsonb_set(metadata,"
        "'{config_override,workspace,backend}',to_jsonb('sandbox'::text)) WHERE id=$1::uuid",
        ids["thread"],
    )
    thread = await db.get_thread(ids["thread"])
    generation = str(thread["runtime_generation"])
    cluster = PartialCluster()
    provider = start.pull._provisioner(monkeypatch, db, cluster)
    if published:
        from kubernetes.client.exceptions import ApiException
        original_pod = cluster.create_namespaced_pod

        def fail_original_pod(**kwargs):
            if kwargs["body"]["metadata"]["labels"].get("srw.io/workspace-provision-fence") != "true":
                raise ApiException(status=503)
            return original_pod(**kwargs)

        monkeypatch.setattr(cluster, "create_namespaced_pod", fail_original_pod)
    returned, resume = asyncio.Event(), asyncio.Event()
    method = "_create_seed_configmap" if published else "_create_pvc"
    original = getattr(provider, method)

    async def create(*args, **kwargs):
        if published:
            returned.set()
            await resume.wait()
            return await original(*args, **kwargs)
        result = await original(*args, **kwargs)
        returned.set()
        await resume.wait()
        return result

    monkeypatch.setattr(provider, method, create)
    task = asyncio.create_task(provider.create_pinned_thread_workspace(ids["thread"]))
    await asyncio.wait_for(returned.wait(), 10)
    return ids, generation, cluster, provider, task, resume


async def _release(db, ids, generation):
    return await release_session_attach_binding(
        ids["agent"],
        ids["thread"],
        expected_runtime_generation=generation,
        expected_attach_token=ids["attach_token"],
        expected_agent_pod_uid="old-pod",
        local_runtime_quiesced=True,
        local_quiescence_protocol="agent_attach_not_started_v1",
        dependencies=SimpleNamespace(store=db),
    )


@pytest.mark.asyncio
async def test_creation_return_before_abort_retains_publication_authority(
    db, monkeypatch
):
    ids, generation, cluster, _, task, resume = await _creating(db, monkeypatch)
    try:
        assert "pvc" in cluster.objects and "pod" not in cluster.objects
        intent = await db.fetchrow(
            "SELECT * FROM thread_workspace_provision_intents WHERE thread_id=$1::uuid",
            ids["thread"],
        )
        assert intent["status"] == "planned" and intent["pvc_uid"] is None
        # Ordinary creates do not use the retained-successor effect latch.
        assert intent["creation_effects_admitted_at"] is None
        assert await _release(db, ids, generation) == "unsafe"
        current = await db.get_thread(ids["thread"])
        assert str(current["runtime_generation"]) == generation
        assert str(current["agent_id"]) == ids["agent"]
    finally:
        resume.set()
        await task
    intent = await db.fetchrow(
        "SELECT * FROM thread_workspace_provision_intents WHERE thread_id=$1::uuid",
        ids["thread"],
    )
    assert intent["pvc_uid"] == cluster.objects["pvc"].metadata.uid
    retirement = await db.begin_pinned_thread_retirement(ids["thread"], permanent=True)
    assert retirement["state"] == "pending", json.dumps(retirement, default=str)


def _prevention_base_release():
    """Execute the exact old owner, rather than manufacturing stranded rows."""
    from orchestrator.services import session_attach_binding

    path = Path(__file__).parent / "fixtures/r33c_prevention_base_release.txt"
    assert (
        hashlib.sha256(path.read_bytes()).hexdigest()
        == "ea05633a773c815da9fb47d2c4453f76030362c2270072694bf30f4c2b6c1bec"
    )
    namespace = dict(vars(session_attach_binding))
    exec(compile(path.read_text(), str(path), "exec"), namespace)
    return namespace["release_session_attach_binding"]


@pytest.mark.asyncio
async def test_recorded_abort_recovers_old_partial_creation_without_rebinding(
    db, monkeypatch
):
    ids, generation, cluster, provider, task, resume = await _creating(db, monkeypatch, published=True)
    try:
        old_release = _prevention_base_release()
        released = await old_release(
            ids["agent"],
            ids["thread"],
            expected_runtime_generation=generation,
            expected_attach_token=ids["attach_token"],
            expected_agent_pod_uid="old-pod",
            local_runtime_quiesced=True,
            local_quiescence_protocol="agent_attach_not_started_v1",
            dependencies=SimpleNamespace(store=db),
        )
        assert released == "released"
    finally:
        resume.set()
        assert await task is False
    before = await db.fetchrow(
        "SELECT * FROM thread_workspace_provision_intents WHERE thread_id=$1::uuid",
        ids["thread"],
    )
    abort = await db.fetchrow(
        "SELECT * FROM thread_runtime_attach_abort_outcomes WHERE thread_id=$1::uuid",
        ids["thread"],
    )
    assert before["pvc_uid"] == cluster.objects["pvc"].metadata.uid
    assert str(before["runtime_generation"]) == generation
    current = await db.get_thread(ids["thread"])
    assert str(current["runtime_generation"]) == str(abort["successor_generation"])
    retirement = await start._retire_unbound(
        db, provider, ids["thread"], permanent=True
    )
    assert retirement["generation"] == str(abort["successor_generation"])
    assert (
        retirement["context"]["workspace_provision_intent"]["runtime_generation"]
        == generation
    )
    assert await db.get_thread(ids["thread"]) is None
    # Immutable cleanup expectations survive the deletion they authorize.
    assert await db.fetchval(
        "SELECT public.pinned_retirement_external_cleanup_expected($1::jsonb,$2::uuid,$3::uuid)",
        json.dumps(retirement["context"], default=str), retirement["generation"], retirement["token"]
    ) is not None
    after = await db.fetchrow(
        "SELECT * FROM thread_workspace_provision_intents WHERE attempt_id=$1",
        before["attempt_id"],
    )
    assert after["runtime_generation"] == before["runtime_generation"]
    assert after["pvc_uid"] == before["pvc_uid"] and after["status"] == "fenced"
    assert (
        await db.fetchrow(
            "SELECT * FROM thread_runtime_attach_abort_outcomes WHERE thread_id=$1::uuid",
            ids["thread"],
        )
        == abort
    )


class PartialCluster(start.PinnedPullCluster):
    def create_namespaced_pod(self, *, body, **kwargs):
        pod = super().create_namespaced_pod(body=body, **kwargs)
        spec = body["spec"]
        pod.spec.scheduler_name = spec.get("schedulerName")
        pod.spec.node_name = spec.get("nodeName")
        pod.spec.restart_policy = spec.get("restartPolicy")
        pod.spec.automount_service_account_token = spec.get(
            "automountServiceAccountToken"
        )
        for container in pod.spec.containers:
            container.volume_mounts = None
        return pod


async def _stranded(db, monkeypatch, *, published=True):
    ids, generation, cluster, provider, task, resume = await _creating(db, monkeypatch, published=published)
    try:
        assert (
            await _prevention_base_release()(
                ids["agent"],
                ids["thread"],
                expected_runtime_generation=generation,
                expected_attach_token=ids["attach_token"],
                expected_agent_pod_uid="old-pod",
                local_runtime_quiesced=True,
                local_quiescence_protocol="agent_attach_not_started_v1",
                dependencies=SimpleNamespace(store=db),
            )
            == "released"
        )
    finally:
        resume.set()
        assert await task is False
    return ids, generation, cluster, provider


@pytest.mark.asyncio
async def test_stranded_intent_does_not_retire_a_bound_successor(db, monkeypatch):
    from uuid import uuid4

    ids, _, cluster, _ = await _stranded(db, monkeypatch)
    replacement, _ = await authority._bind_replacement_agent(
        db,
        thread_id=ids["thread"],
        pod_uid=str(uuid4()),
        pod_name="successor-" + str(uuid4())[:12],
    )
    before = dict(cluster.objects)
    retirement = await db.begin_pinned_thread_retirement(ids["thread"], permanent=True)
    assert retirement["state"] == "malformed"
    assert str((await db.get_thread(ids["thread"]))["agent_id"]) == replacement
    assert cluster.objects == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "replacement_uid",
        "replacement_generation",
        "replacement_attempt",
        "unreadable",
        "inert_scheduled",
        "inert_mount",
        "possible_writer",
    ],
)
async def test_partial_retirement_refuses_ambiguous_or_replacement_resources(
    db, monkeypatch, fault
):
    from uuid import uuid4
    from kubernetes.client.exceptions import ApiException
    from orchestrator.services.container_provisioner import (
        WORKSPACE_PROVISION_GENERATION_LABEL,
        WORKSPACE_PROVISION_ATTEMPT_LABEL,
    )

    ids, _, cluster, provider = await _stranded(db, monkeypatch)
    retirement = await db.begin_pinned_thread_retirement(ids["thread"], permanent=True)
    assert retirement["state"] == "pending"
    assert await db.authorize_pinned_thread_retirement(
        ids["thread"],
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    intent = await db.revoke_pinned_thread_workspace_provision_intent(
        ids["thread"],
        expected_runtime_generation=retirement["generation"],
        expected_retirement_token=retirement["token"],
        expected_attempt_id=retirement["context"]["workspace_provision_intent"][
            "attempt_id"
        ],
    )
    if fault in {"replacement_uid", "replacement_generation", "replacement_attempt"}:
        # A foreign UID cannot be adopted solely from matching name/labels.
        if fault == "replacement_uid":
            cluster.objects["pvc"].metadata.uid = str(uuid4())
        else:
            key = (
                WORKSPACE_PROVISION_GENERATION_LABEL
                if fault == "replacement_generation"
                else WORKSPACE_PROVISION_ATTEMPT_LABEL
            )
            cluster.objects["pvc"].metadata.labels[key] = str(uuid4())
    if fault == "unreadable":

        def unreadable(**_):
            raise ApiException(status=503)

        monkeypatch.setattr(cluster, "read_namespaced_pod", unreadable)
    if fault in {"inert_scheduled", "inert_mount", "possible_writer"}:
        original = cluster.create_namespaced_pod

        def create(**kwargs):
            pod = original(**kwargs)
            if fault == "inert_scheduled":
                pod.spec.node_name = "worker"
            elif fault == "inert_mount":
                pod.spec.volumes = [{"name": "writer"}]
            else:
                pod.metadata.labels.pop("srw.io/workspace-provision-fence", None)
                pod.metadata.labels["app"] = "srw-workspace"
            return pod

        monkeypatch.setattr(cluster, "create_namespaced_pod", create)
    fences = await provider.fence_pinned_workspace_provision_intent(
        intent,
        permanent=True,
        expected_retirement_token=retirement["token"],
        expected_retirement_generation=retirement["generation"],
    )
    assert fences is None
    assert (
        await db.clear_pinned_retirement_physical_runtime_endpoint(
            ids["thread"],
            runtime_generation=retirement["generation"],
            retirement_token=retirement["token"],
            completed_external_cleanup_protocol="workspace_provision_fence_v1",
        )
        is False
    )
    assert await db.get_thread(ids["thread"]) is not None


@pytest.mark.asyncio
async def test_missing_causal_fence_receipt_cannot_settle_partial_creation(
    db, monkeypatch
):
    import asyncpg
    from uuid import uuid4

    ids, _, _, _ = await _stranded(db, monkeypatch)
    retirement = await db.begin_pinned_thread_retirement(ids["thread"], permanent=True)
    assert await db.authorize_pinned_thread_retirement(
        ids["thread"],
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    attempt = retirement["context"]["workspace_provision_intent"]["attempt_id"]
    assert await db.revoke_pinned_thread_workspace_provision_intent(
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
            fence_pvc_uid=str(uuid4()),
            fence_configmap_uid=None,
            fence_service_uid=str(uuid4()),
            permanent=True,
        )


@pytest.mark.asyncio
async def test_unpublished_existing_pvc_is_not_adopted_from_name_and_labels(db, monkeypatch):
    ids, _, cluster, provider = await _stranded(db, monkeypatch, published=False)
    original_uid = cluster.objects["pvc"].metadata.uid
    retirement = await db.begin_pinned_thread_retirement(ids["thread"], permanent=True)
    assert retirement["state"] == "pending"
    assert await db.authorize_pinned_thread_retirement(
        ids["thread"], token=retirement["token"], generation=retirement["generation"], settle_status="ended"
    )
    intent = await db.revoke_pinned_thread_workspace_provision_intent(
        ids["thread"], expected_runtime_generation=retirement["generation"],
        expected_retirement_token=retirement["token"],
        expected_attempt_id=retirement["context"]["workspace_provision_intent"]["attempt_id"]
    )
    assert intent["pvc_uid"] is None
    assert await provider.fence_pinned_workspace_provision_intent(
        intent, permanent=True, expected_retirement_token=retirement["token"],
        expected_retirement_generation=retirement["generation"]
    ) is None
    assert cluster.objects["pvc"].metadata.uid == original_uid
