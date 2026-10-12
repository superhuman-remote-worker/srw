"""A denied first read must retain the exact published cleanup identity."""

import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import httpx
import pytest
from fastapi import HTTPException

from agent.api.orchestrator_client import OrchestratorClient, SessionGrantDenied
from agent.api.session_attach import SessionAttachCoordinator
from agent.api.session_identity import SessionIdentityPorts, SessionIdentityRuntime
from orchestrator.services import thread_workspace_delivery as delivery
from orchestrator.services.container_provisioner import WorkspaceRuntimeAttestation
from orchestrator.services.session_attach_binding import release_session_attach_binding
from tests import test_persistent_recycler_real_postgres as fixtures
from tests.test_pinned_vm_workspace_delivery import vm_delivery
from tests.test_session_attach_runtime import _ports


db = fixtures.db
pg_dsn = fixtures.pg_dsn
_schema_applied = fixtures._schema_applied


class Denied(Exception):
    def __init__(self):
        self.violations = [{"capability": "shell_tools", "reason": "denied"}]


async def _published_workspace(db):
    ids = await fixtures._seed(db, protected_agent_pod=True, workspace_claim=False)
    row = await db.get_thread(ids["thread"])
    metadata = fixtures._json(row["metadata"])
    generation, incarnation, backing = (str(uuid4()) for _ in range(3))
    metadata["config_override"] = {"workspace": {"backend": "sandbox"}}
    metadata["workspace_container"] = {
        "status": "ready",
        "provisioner": "k8s",
        "host": "workspace.example.svc",
        "pod_ip": "10.42.0.25",
        "port": 30022,
        "_canvas_workspace_generation": generation,
        "_runtime_incarnation": incarnation,
    }
    metadata["_workspace_binding"] = {
        "generation": generation,
        "kind": "remote",
        "backing_id": "k8s-pvc:agents-a:" + backing,
        "ssh_host_key_fingerprint": "SHA256:" + "A" * 43,
    }
    await db.execute(
        "UPDATE threads SET status='created',config_name='assistant',metadata=$2::jsonb WHERE id=$1::uuid",
        ids["thread"],
        json.dumps(metadata),
    )
    ids.update(
        session_generation=str(row["runtime_generation"]),
        workspace_generation=generation,
        incarnation=incarnation,
    )
    attestation = WorkspaceRuntimeAttestation(
        backing_id=metadata["_workspace_binding"]["backing_id"],
        workspace_generation=backing,
        runtime_incarnation=incarnation,
        ssh_host_key_fingerprint=metadata["_workspace_binding"][
            "ssh_host_key_fingerprint"
        ],
        host=metadata["workspace_container"]["host"],
        pod_ip="10.42.0.25",
        port=30022,
    )
    return ids, attestation


def _expected_headers(ids):
    return {
        "X-SRW-Pre-Setup-Workspace-Identity": "1",
        "X-SRW-Thread-ID": ids["thread"],
        "X-SRW-Session-Runtime-Generation": ids["session_generation"],
        "X-SRW-Workspace-Generation": ids["workspace_generation"],
        "X-SRW-Workspace-Runtime-Incarnation": ids["incarnation"],
    }


@pytest.mark.asyncio
async def test_denied_first_workspace_read_keeps_attested_cleanup_identity(
    db, monkeypatch
):
    ids, attestation = await _published_workspace(db)
    scaffold = vm_delivery.__wrapped__(monkeypatch)
    dependencies = replace(
        scaffold.dependencies,
        store=db,
        vm_provisioner=None,
        container_provisioner=SimpleNamespace(
            is_available=True,
            attest_workspace_runtime=AsyncMock(return_value=attestation),
        ),
        GrantDenied=Denied,
        resolve_session_config=AsyncMock(side_effect=Denied()),
    )
    with pytest.raises(HTTPException) as refused:
        await delivery.agent_get_thread_workspace_locked(
            ids["thread"],
            presented_agent_id=ids["agent"],
            presented_runtime_generation=ids["session_generation"],
            presented_attach_token=ids["attach_token"],
            dependencies=dependencies,
        )
    assert refused.value.status_code == 403
    assert refused.value.detail == Denied().violations
    assert refused.value.headers == _expected_headers(ids)


def _client_and_owner(ids, headers, monkeypatch):
    client = OrchestratorClient(
        "http://orchestrator", "127.0.0.1", 8001, "agent", "session_base"
    )
    client.agent_id = ids["agent"]
    client.adopt_session_runtime_identity(
        ids["session_generation"], ids["attach_token"], contract_advertised=True
    )
    client._client = MagicMock(
        get=AsyncMock(
            return_value=httpx.Response(403, json={"detail": "denied"}, headers=headers)
        )
    )
    identity = SessionIdentityRuntime(
        SessionIdentityPorts(
            agent_id=lambda: ids["agent"],
            pod_uid=lambda: "old-pod",
            lease=lambda: None,
            stateless_mode=lambda: False,
            orchestrator_client=lambda: client,
            identity_replaced=lambda: None,
        )
    )
    identity.bind_thread(ids["thread"])
    identity.adopt(
        ids["session_generation"], ids["attach_token"], contract_advertised=True
    )
    owner = SessionAttachCoordinator(
        _ports(
            identity,
            orchestrator_client=lambda: client,
            stop_interrupt_watcher=AsyncMock(),
            stop_control_watcher=AsyncMock(),
            stop_and_join_watchdogs=AsyncMock(),
            quiesce_side_tasks=AsyncMock(),
            event_writer=lambda: None,
        )
    )
    owner._cleanup_context = {
        "thread_id": ids["thread"],
        "setup_started": False,
        "workspace_tier": "sandbox",
        "workspace_generation": None,
        "workspace_runtime_incarnation": None,
        "remote": None,
        "datasources": {},
        "datasource_clients": {},
    }
    monkeypatch.setenv("POD_UID", "old-pod")

    async def denied_attach(**_kwargs):
        await client.get_thread_workspace(ids["thread"], raise_on_denied=True)

    monkeypatch.setattr(owner, "_attach_inner", denied_attach)
    return client, identity, owner


@pytest.mark.asyncio
async def test_denied_pre_setup_identity_reaches_real_release_cas(db, monkeypatch):
    ids, _attestation = await _published_workspace(db)
    _client, identity, owner = _client_and_owner(
        ids, _expected_headers(ids), monkeypatch
    )
    with pytest.raises(SessionGrantDenied):
        await owner.attach(ids["thread"])
    receipt = owner.release_receipt
    assert receipt["local_quiescence_protocol"] == "agent_attach_not_started_v1"
    assert identity.session_generation == ids["session_generation"]
    result = await release_session_attach_binding(
        ids["agent"],
        ids["thread"],
        expected_runtime_generation=ids["session_generation"],
        expected_attach_token=ids["attach_token"],
        expected_agent_pod_uid=receipt["agent_pod_uid"],
        local_runtime_quiesced=True,
        local_quiescence_protocol=receipt["local_quiescence_protocol"],
        workspace_generation=receipt["workspace_generation"],
        workspace_runtime_incarnation=receipt["workspace_runtime_incarnation"],
        dependencies=SimpleNamespace(store=db),
    )
    assert result == "released"
    current = await db.get_thread(ids["thread"])
    assert str(current["runtime_generation"]) != ids["session_generation"]
    assert current["agent_id"] is None
    outcome = await db.fetchrow(
        "SELECT * FROM thread_runtime_attach_abort_outcomes WHERE thread_id=$1::uuid",
        ids["thread"],
    )
    assert str(outcome["workspace_generation"]) == ids["workspace_generation"]
    assert str(outcome["workspace_runtime_incarnation"]) == ids["incarnation"]
    assert outcome["quiescence_protocol"] == "agent_attach_not_started_v1"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "defect",
    [
        "missing_contract",
        "future_contract",
        "other_thread",
        "other_session",
        "missing_generation",
        "missing_incarnation",
        "invalid_incarnation",
        "noncanonical_generation",
    ],
)
async def test_unscoped_denial_identity_cannot_rotate_published_life(
    db, monkeypatch, defect
):
    ids, _attestation = await _published_workspace(db)
    headers = _expected_headers(ids)
    if defect == "missing_contract":
        del headers["X-SRW-Pre-Setup-Workspace-Identity"]
    elif defect == "future_contract":
        headers["X-SRW-Pre-Setup-Workspace-Identity"] = "2"
    elif defect == "other_thread":
        headers["X-SRW-Thread-ID"] = str(uuid4())
    elif defect == "other_session":
        headers["X-SRW-Session-Runtime-Generation"] = str(uuid4())
    elif defect == "missing_generation":
        del headers["X-SRW-Workspace-Generation"]
    elif defect == "missing_incarnation":
        del headers["X-SRW-Workspace-Runtime-Incarnation"]
    elif defect == "invalid_incarnation":
        headers["X-SRW-Workspace-Runtime-Incarnation"] = "not-a-uuid"
    else:
        headers["X-SRW-Workspace-Generation"] = "{" + ids["workspace_generation"] + "}"
    _client, identity, owner = _client_and_owner(ids, headers, monkeypatch)
    with pytest.raises(SessionGrantDenied) as denied:
        await owner.attach(ids["thread"])
    assert denied.value.cleanup_identity is None
    receipt = owner.release_receipt
    assert receipt["workspace_generation"] is None
    assert receipt["workspace_runtime_incarnation"] is None
    result = await release_session_attach_binding(
        ids["agent"],
        ids["thread"],
        expected_runtime_generation=ids["session_generation"],
        expected_attach_token=ids["attach_token"],
        expected_agent_pod_uid=receipt["agent_pod_uid"],
        local_runtime_quiesced=True,
        local_quiescence_protocol=receipt["local_quiescence_protocol"],
        workspace_generation=receipt["workspace_generation"],
        workspace_runtime_incarnation=receipt["workspace_runtime_incarnation"],
        dependencies=SimpleNamespace(store=db),
    )
    assert result == "unsafe"
    current = await db.get_thread(ids["thread"])
    assert str(current["runtime_generation"]) == ids["session_generation"]
    assert str(current["agent_id"]) == ids["agent"]
    assert identity.session_generation == ids["session_generation"]
    assert (
        await db.fetchval(
            "SELECT count(*) FROM thread_runtime_attach_abort_outcomes WHERE thread_id=$1::uuid",
            ids["thread"],
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "boundary",
    [
        "setup_started",
        "session_exists",
        "remote_exists",
        "thread_replaced",
        "generation_replaced",
        "workspace_generation_replaced",
        "incarnation_replaced",
    ],
)
async def test_denial_hint_cannot_cross_construction_or_replacement_boundary(
    db, monkeypatch, boundary
):
    ids, _attestation = await _published_workspace(db)
    _client, identity, owner = _client_and_owner(
        ids, _expected_headers(ids), monkeypatch
    )
    original = owner._attach_inner
    context = owner._cleanup_context
    owner.cleanup_failed_attach_until_proven = AsyncMock(return_value={})

    async def delayed_denial(**kwargs):
        try:
            await original(**kwargs)
        except SessionGrantDenied:
            if boundary == "setup_started":
                context["setup_started"] = True
            elif boundary == "session_exists":
                owner._ports = replace(owner._ports, session=lambda: object())
            elif boundary == "remote_exists":
                context["remote"] = {"host": "constructed.example"}
            elif boundary == "thread_replaced":
                identity.bind_thread(str(uuid4()))
            elif boundary == "generation_replaced":
                identity.adopt(str(uuid4()), str(uuid4()), contract_advertised=True)
            elif boundary == "workspace_generation_replaced":
                context["workspace_generation"] = str(uuid4())
            else:
                context["workspace_runtime_incarnation"] = str(uuid4())
            raise

    monkeypatch.setattr(owner, "_attach_inner", delayed_denial)
    with pytest.raises(SessionGrantDenied) as denied:
        await owner.attach(ids["thread"])
    assert denied.value.cleanup_identity is not None
    assert context["workspace_generation"] != ids["workspace_generation"]
    assert context["workspace_runtime_incarnation"] != ids["incarnation"]
    if boundary == "setup_started":
        assert context["setup_started"] is True
    assert owner.release_receipt is None
    assert (
        await db.fetchval(
            "SELECT count(*) FROM thread_runtime_attach_abort_outcomes WHERE thread_id=$1::uuid",
            ids["thread"],
        )
        == 0
    )


@pytest.mark.asyncio
async def test_denied_read_without_physical_attestation_carries_no_hint(
    db, monkeypatch
):
    ids, _attestation = await _published_workspace(db)
    scaffold = vm_delivery.__wrapped__(monkeypatch)
    dependencies = replace(
        scaffold.dependencies,
        store=db,
        vm_provisioner=None,
        container_provisioner=None,
        GrantDenied=Denied,
        resolve_session_config=AsyncMock(side_effect=Denied()),
    )
    monkeypatch.setattr(
        delivery, "attest_pinned_thread_k8s_workspace", AsyncMock(return_value=None)
    )
    with pytest.raises(HTTPException) as denied:
        await delivery.agent_get_thread_workspace_locked(
            ids["thread"],
            presented_agent_id=ids["agent"],
            presented_runtime_generation=ids["session_generation"],
            presented_attach_token=ids["attach_token"],
            dependencies=dependencies,
        )
    assert denied.value.status_code == 403
    assert denied.value.headers is None


@pytest.mark.asyncio
async def test_denied_read_still_refuses_wrong_pinned_authority_before_hint(
    db, monkeypatch
):
    ids, attestation = await _published_workspace(db)
    scaffold = vm_delivery.__wrapped__(monkeypatch)
    resolver = AsyncMock(side_effect=Denied())
    dependencies = replace(
        scaffold.dependencies,
        store=db,
        vm_provisioner=None,
        container_provisioner=SimpleNamespace(
            is_available=True,
            attest_workspace_runtime=AsyncMock(return_value=attestation),
        ),
        GrantDenied=Denied,
        resolve_session_config=resolver,
    )
    with pytest.raises(HTTPException) as refused:
        await delivery.agent_get_thread_workspace_locked(
            ids["thread"],
            presented_agent_id=ids["agent"],
            presented_runtime_generation=str(uuid4()),
            presented_attach_token=ids["attach_token"],
            dependencies=dependencies,
        )
    assert refused.value.status_code == 409
    assert refused.value.headers is None
    resolver.assert_not_awaited()
