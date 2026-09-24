"""Exercise pinned End through the real workspace provisioner and PostgreSQL."""

from __future__ import annotations

import json
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from kubernetes.client.exceptions import ApiException

from orchestrator import main
from orchestrator.services.container_provisioner import (
    STATELESS_WORKSPACE_PROCESS_ZERO_FINALIZER,
    WorkspaceRuntimeAttestation,
)
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from tests import test_persistent_recycler_real_postgres as authority
from tests.test_workspace_cleanup_retry_real_postgres import _absent_pod_provisioner
from orchestrator.application import controls as controls_composition
from orchestrator.application import preparation as preparation_composition
from orchestrator.services import container_provisioner as container_provisioner_module
from orchestrator.services import dispatch_credentials as dispatch_credentials_module
from orchestrator.services import (
    thread_workspace_delivery as thread_workspace_delivery_module,
)


db = authority.db
pg_dsn = authority.pg_dsn
_schema_applied = authority._schema_applied


async def _scenario(db, monkeypatch):
    ids = await authority._seed(db, protected_agent_pod=True, workspace_claim=False)
    thread = await db.get_thread(ids["thread"])
    generation = str(thread["runtime_generation"])
    metadata = authority._json(thread["metadata"])
    metadata["config_override"]["workspace"]["backend"] = "sandbox"
    metadata["config_override"]["officer"]["enabled"] = False
    await db.execute(
        "UPDATE threads SET metadata=$2::jsonb WHERE id=$1::uuid",
        ids["thread"],
        json.dumps(metadata),
    )
    owner = WorkspaceOwner.session(ids["thread"])
    attempt, pod_uid, pvc_uid, service_uid = (str(uuid4()) for _ in range(4))
    assert await db.reserve_pinned_thread_workspace_provision_intent(
        owner.id,
        expected_runtime_generation=generation,
        expected_agent_id=ids["agent"],
        expected_attach_token=ids["attach_token"],
        expected_workspace_context=None,
        expected_binding_context=None,
        attempt_id=attempt,
        namespace="agent-workspaces",
        pod_name=owner.pod_name,
        pvc_name="pvc-" + owner.pod_name,
        seed_configmap_name=None,
        service_name=owner.pod_name,
        retained_service_uid=None,
        network_tier="internet-only",
        manifest_fingerprint="a" * 64,
    )
    for resource, uid in (("pod", pod_uid), ("pvc", pvc_uid), ("service", service_uid)):
        assert await db.publish_pinned_thread_workspace_provision_resource(
            owner.id,
            expected_runtime_generation=generation,
            attempt_id=attempt,
            resource=resource,
            resource_uid=uid,
        )
    assert await db.complete_pinned_thread_workspace_provision_intent(
        owner.id,
        expected_runtime_generation=generation,
        attempt_id=attempt,
        expected_pod_uid=pod_uid,
        expected_pvc_uid=pvc_uid,
        expected_seed_configmap_uid=None,
        expected_service_uid=service_uid,
        pod_ip="10.0.0.8",
        ssh_host_key_fingerprint="SHA256:" + "A" * 43,
    )
    p, resources = _absent_pod_provisioner(
        db, owner, {"pvc_uid": pvc_uid, "service_uid": service_uid}
    )
    pod = NS(
        metadata=NS(
            name=owner.pod_name,
            namespace=p._namespace,
            uid=pod_uid,
            resource_version="7",
            deletion_timestamp=None,
            finalizers=[STATELESS_WORKSPACE_PROCESS_ZERO_FINALIZER],
            labels={
                "app": "srw-workspace",
                "srw/component": owner.component_label,
                "srw.io/component": "agent-workspace",
                owner.label_key: owner.id,
            },
        ),
        spec=NS(
            containers=[
                NS(
                    name="workspace",
                    volume_mounts=[
                        NS(name="workspace-data", mount_path="/home/agent-host")
                    ],
                )
            ],
            init_containers=[],
            ephemeral_containers=[],
            volumes=[
                NS(
                    name="workspace-data",
                    persistent_volume_claim=NS(claim_name="pvc-" + owner.pod_name),
                )
            ],
        ),
        status=NS(
            phase="Running",
            pod_ip="10.0.0.8",
            init_container_statuses=[],
            ephemeral_container_statuses=[],
            container_statuses=[
                NS(name="workspace", ready=True, state=NS(terminated=None))
            ],
        ),
    )
    resources["pod"] = pod
    effects = []

    def read_pod(**kwargs):
        if "pod" not in resources:
            raise ApiException(status=404)
        return resources["pod"]

    def delete_pod(**kwargs):
        current = read_pod()
        assert kwargs["body"]["preconditions"]["uid"] == current.metadata.uid
        current.metadata.deletion_timestamp = "now"
        current.status.phase = "Succeeded"
        current.status.container_statuses[0].state.terminated = NS(exit_code=0)
        effects.append(("delete", current.metadata.uid))

    def patch_pod(**kwargs):
        current = read_pod()
        body = kwargs["body"]
        assert {"op": "test", "path": "/metadata/uid", "value": pod_uid} in body
        assert any(x["op"] == "remove" and "finalizers" in x["path"] for x in body)
        assert current.status.container_statuses[0].state.terminated is not None
        effects.append(("finalizer", current.metadata.uid))
        del resources["pod"]
        return current

    p._core_api.read_namespaced_pod.side_effect = read_pod
    p._core_api.delete_namespaced_pod.side_effect = delete_pod
    p._core_api.patch_namespaced_pod.side_effect = patch_pod
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(container_provisioner_module, "container_provisioner", p)
    monkeypatch.setattr(
        main.app.state.resources.session_router,
        "teardown_route",
        AsyncMock(return_value=True),
    )
    return ids, owner, p, resources, effects


async def _begin(db, ids, permanent):
    retirement = await db.begin_pinned_thread_retirement(
        ids["thread"], permanent=permanent
    )
    await authority._authorize_and_ack(db, ids, retirement)
    return retirement


@pytest.mark.asyncio
async def test_delivered_pinned_workspace_generation_can_ack_retirement(
    db, monkeypatch
):
    """Kubernetes backing UID and the durable session binding are distinct UUIDs."""
    ids, owner, p, resources, _ = await _scenario(db, monkeypatch)
    thread = await db.get_thread(owner.id)
    metadata = authority._json(thread["metadata"])
    binding = metadata["_workspace_binding"]
    backing_uid = resources["pvc"].metadata.uid
    assert backing_uid != binding["generation"]
    monkeypatch.setattr(
        p,
        "attest_workspace_runtime",
        AsyncMock(
            return_value=WorkspaceRuntimeAttestation(
                backing_id=binding["backing_id"],
                workspace_generation=backing_uid,
                runtime_incarnation=resources["pod"].metadata.uid,
                ssh_host_key_fingerprint=binding["ssh_host_key_fingerprint"],
                host=p._workspace_dns(owner),
                pod_ip="10.0.0.8",
            )
        ),
    )
    # Credential resolution is independent of the workspace-generation wire
    # contract. Preserve the config without constructing live provider clients.
    inject_credentials = AsyncMock(side_effect=lambda config, **_kwargs: config)
    monkeypatch.setattr(
        dispatch_credentials_module,
        "inject_thread_dispatch_credentials",
        inject_credentials,
    )
    payload = await thread_workspace_delivery_module.agent_get_thread_workspace_locked(
        owner.id,
        presented_agent_id=ids["agent"],
        presented_runtime_generation=str(thread["runtime_generation"]),
        presented_attach_token=ids["attach_token"],
        dependencies=preparation_composition.thread_workspace_delivery_dependencies(
            main.app.state.resources
        ),
    )
    # Delivery materializes both the compatibility override and the canonical
    # resolved blob. Both credential lookups must retain the same recipient.
    assert inject_credentials.await_count == 2
    for call in inject_credentials.await_args_list:
        assert call.kwargs["user_id"] == str(thread["user_id"])
        assert call.kwargs["project_id"] == str(thread["project_id"])
    assert payload["resolved_config"]["execution_snapshot"]["generation"] == 1
    retirement = await db.begin_pinned_thread_retirement(owner.id, permanent=False)
    assert await db.authorize_pinned_thread_retirement(
        owner.id,
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    receipt = await db.acknowledge_pinned_thread_local_quiescence(
        owner.id,
        expected_runtime_generation=retirement["generation"],
        expected_retirement_token=retirement["token"],
        expected_agent_id=ids["agent"],
        expected_attach_token=ids["attach_token"],
        expected_settle_status="ended",
        expected_quiescence_protocol="workspace_process_zero_v1",
        expected_workspace_generation=payload["workspace_generation"],
        expected_workspace_runtime_incarnation=payload["workspace_runtime_incarnation"],
    )
    assert receipt is not None
    assert payload["workspace_generation"] == binding["generation"]
    assert payload["workspace_runtime_incarnation"] == resources["pod"].metadata.uid


@pytest.mark.asyncio
@pytest.mark.parametrize("permanent_first", [False, True])
async def test_pinned_workspace_end_and_permanent_delete(
    db, monkeypatch, permanent_first
):
    ids, owner, p, resources, effects = await _scenario(db, monkeypatch)
    pvc_uid = resources["pvc"].metadata.uid
    pod_uid = resources["pod"].metadata.uid
    retirement = await _begin(db, ids, permanent_first)
    await controls_composition.pinned_retirement_operations(
        main.app.state.resources
    ).cleanup_pinned_thread_retirement(retirement, cleanup_agent_pod=False)
    assert effects == [("delete", pod_uid), ("finalizer", pod_uid)]
    assert (
        await db.fetchval(
            "SELECT count(*) FROM managed_repository_process_zero_receipts "
            "WHERE owner_id=$1::uuid AND scope='workspace_container' "
            "AND runtime_incarnation=$2",
            owner.id,
            pod_uid,
        )
        == 1
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM managed_repository_workspace_cleanup_intents "
            "WHERE owner_id=$1::uuid",
            owner.id,
        )
        == 0
    )
    if not permanent_first:
        assert set(resources) == {"pvc"}
        assert resources["pvc"].metadata.uid == pvc_uid
        assert await db.settle_pinned_thread_retirement(
            owner.id,
            token=retirement["token"],
            generation=retirement["generation"],
            final_status="ended",
        )
        retirement = await db.begin_pinned_thread_retirement(owner.id, permanent=True)
        assert retirement["context"]["retained_soft_workspace"]["pvc_uid"] == pvc_uid
        assert await db.authorize_pinned_thread_retirement(
            owner.id,
            token=retirement["token"],
            generation=retirement["generation"],
            settle_status="ended",
        )
        await controls_composition.pinned_retirement_operations(
            main.app.state.resources
        ).cleanup_pinned_thread_retirement(retirement, cleanup_agent_pod=False)
    assert not resources
    await db.delete_thread(
        owner.id,
        expected_runtime_retirement_token=retirement["token"],
        expected_runtime_generation=retirement["generation"],
    )
    assert await db.get_thread(owner.id) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "unauthorized",
        "missing_ack",
        "stale_token",
        "altered_context",
        "pod_replacement",
        "pvc_replacement",
        "absent_without_receipt",
    ],
)
async def test_pinned_workspace_refuses_incomplete_cleanup_authority(
    db, monkeypatch, fault
):
    ids, owner, p, resources, effects = await _scenario(db, monkeypatch)
    retirement = await db.begin_pinned_thread_retirement(owner.id, permanent=True)
    if fault != "unauthorized":
        assert await db.authorize_pinned_thread_retirement(
            owner.id,
            token=retirement["token"],
            generation=retirement["generation"],
            settle_status="ended",
        )
    if fault not in {"unauthorized", "missing_ack"}:
        await authority._authorize_and_ack(db, ids, retirement)
    if fault == "stale_token":
        retirement = {**retirement, "token": str(uuid4())}
    elif fault == "altered_context":
        retirement = {
            **retirement,
            "context": {**retirement["context"], "entry_status": "ended"},
        }
    elif fault in {"pod_replacement", "pvc_replacement"}:
        resources[fault.split("_")[0]].metadata.uid = str(uuid4())
    elif fault == "absent_without_receipt":
        del resources["pod"]
    with pytest.raises(RuntimeError):
        await controls_composition.pinned_retirement_operations(
            main.app.state.resources
        ).cleanup_pinned_thread_retirement(retirement, cleanup_agent_pod=False)
    assert effects == []
    assert "pvc" in resources and "service" in resources
    assert not p._core_api.delete_namespaced_persistent_volume_claim.called
    assert not p._core_api.delete_namespaced_service.called


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ["pod_ack", "service_ack", "projection_ack", "retained_pvc_ack"]
)
async def test_pinned_workspace_replays_lost_responses(db, monkeypatch, fault):
    ids, owner, p, resources, effects = await _scenario(db, monkeypatch)
    retirement = await _begin(db, ids, False)
    if fault == "retained_pvc_ack":
        await controls_composition.pinned_retirement_operations(
            main.app.state.resources
        ).cleanup_pinned_thread_retirement(retirement, cleanup_agent_pod=False)
        assert await db.settle_pinned_thread_retirement(
            owner.id,
            token=retirement["token"],
            generation=retirement["generation"],
            final_status="ended",
        )
        retirement = await db.begin_pinned_thread_retirement(owner.id, permanent=True)
        assert await db.authorize_pinned_thread_retirement(
            owner.id,
            token=retirement["token"],
            generation=retirement["generation"],
            settle_status="ended",
        )
    target = {
        "pod_ack": p._core_api.patch_namespaced_pod,
        "service_ack": p._core_api.delete_namespaced_service,
        "retained_pvc_ack": p._core_api.delete_namespaced_persistent_volume_claim,
    }.get(fault)
    if target is not None:
        effect = target.side_effect
        lost = False

        def lose_once(**kwargs):
            nonlocal lost
            result = effect(**kwargs)
            if not lost:
                lost = True
                raise TimeoutError("accepted response lost")
            return result

        target.side_effect = lose_once
    else:
        original = p._set_context
        lost = False

        async def lose_projection(*args, **kwargs):
            nonlocal lost
            result = await original(*args, **kwargs)
            if not lost:
                lost = True
                return False
            return result

        monkeypatch.setattr(p, "_set_context", lose_projection)
    with pytest.raises(RuntimeError):
        await controls_composition.pinned_retirement_operations(
            main.app.state.resources
        ).cleanup_pinned_thread_retirement(retirement, cleanup_agent_pod=False)
    assert lost
    await controls_composition.pinned_retirement_operations(
        main.app.state.resources
    ).cleanup_pinned_thread_retirement(retirement, cleanup_agent_pod=False)
    assert len(effects) == 2
    assert set(resources) == (set() if fault == "retained_pvc_ack" else {"pvc"})
    current = await db.get_thread(owner.id)
    assert (
        authority._json(current["metadata"])["workspace_container"]["status"]
        == "deleted"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("replaced_during_snapshot", [False, True])
async def test_pinned_snapshot_pins_host_key_and_rechecks_pod_before_delete(
    db, monkeypatch, replaced_during_snapshot
):
    ids, owner, p, resources, effects = await _scenario(db, monkeypatch)
    retirement = await _begin(db, ids, False)
    fingerprint = retirement["context"]["workspace_binding"]["ssh_host_key_fingerprint"]

    async def capture(**kwargs):
        assert kwargs["ssh_host"] == "10.0.0.8"
        assert kwargs["expected_host_key_fingerprint"] == fingerprint
        assert not effects
        if replaced_during_snapshot:
            resources["pod"].metadata.uid = str(uuid4())
        return True

    p._snapshot_service = NS(
        is_available=True, capture_vm_snapshot=AsyncMock(side_effect=capture)
    )
    if replaced_during_snapshot:
        with pytest.raises(RuntimeError, match="workspace cleanup is retryable"):
            await controls_composition.pinned_retirement_operations(
                main.app.state.resources
            ).cleanup_pinned_thread_retirement(retirement, cleanup_agent_pod=False)
        assert not effects
        assert set(resources) == {"pod", "pvc", "service"}
    else:
        await controls_composition.pinned_retirement_operations(
            main.app.state.resources
        ).cleanup_pinned_thread_retirement(retirement, cleanup_agent_pod=False)
        assert set(resources) == {"pvc"}
    p._snapshot_service.capture_vm_snapshot.assert_awaited_once()
