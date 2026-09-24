"""Gateway signature, owner lookup and exact VM binding at the actual route."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncssh
import pytest
from fastapi import HTTPException

from orchestrator.routers.ssh_access import (
    SshAccessDependencies,
    internal_vm_ssh_access,
)
from orchestrator.schemas.ssh_access import VMGatewayAccessRequest
from orchestrator.services.ssh_gateway_vm_access_proof import mint_vm_access_proof
from orchestrator.services.vm_ssh_access_binding import vm_binding_digest


@pytest.fixture
def route(monkeypatch):
    gateway = asyncssh.generate_private_key("ssh-ed25519")
    owner = str(uuid4())
    thread = str(uuid4())
    generation = uuid4()
    vm = uuid4()
    proof = SimpleNamespace(
        workspace_generation=str(generation),
        vm_uid=str(vm),
        vmi_uid=str(uuid4()),
        launcher_pod_uid=str(uuid4()),
        rootdisk_pvc_uid=str(uuid4()),
        ssh_host_key_fingerprint="SHA256:guest",
        host="10.0.0.2",
        port=22,
    )
    lease = {"id": uuid4(), "vm_uid": vm, "provision_generation": generation}
    store = SimpleNamespace(
        get_thread_id_by_ssh_handle=AsyncMock(return_value=thread),
        resolve_user_by_ssh_fingerprint=AsyncMock(return_value={"id": owner}),
        get_thread=AsyncMock(
            return_value={
                "id": thread,
                "execution_lane": "pinned",
                "status": "active",
                "metadata": {"vm": {"status": "ready"}},
            }
        ),
    )
    access = SimpleNamespace(
        request=AsyncMock(return_value=lease),
        inspect=AsyncMock(return_value=lease),
        renew=AsyncMock(return_value=True),
        close=AsyncMock(return_value=True),
    )
    deps = SshAccessDependencies(
        store=store,
        operations=SimpleNamespace(
            host_keys=SimpleNamespace(
                load=lambda _paths: [
                    {
                        "public_key": gateway.export_public_key().decode(),
                    }
                ]
            ),
            thread_is_vm_tier=lambda *_: True,
        ),
        require_internal=AsyncMock(),
        user_can_access_ide_entity=AsyncMock(return_value=True),
        vm_access_store=access,
        vm_provisioner=SimpleNamespace(
            attest_workspace_runtime=AsyncMock(return_value=proof),
        ),
    )
    monkeypatch.setenv("SSH_GATEWAY_PUBLIC_HOST_KEYS", "/public/gateway.pub")
    return gateway, owner, thread, proof, access, deps


def _signed(gateway, action="admit", **other):
    return VMGatewayAccessRequest(
        proof=mint_vm_access_proof(
            gateway,
            connection_id="a" * 32,
            handle="s-7f3a91c2",
            fingerprint="SHA256:user",
            action=action,
            **other,
        )
    )


@pytest.mark.asyncio
async def test_post_verified_owner_admits_one_exact_vm_target_then_renews_and_closes(
    route,
):
    gateway, owner, thread, proof, access, deps = route
    result = await internal_vm_ssh_access(
        "admit",
        object(),
        _signed(gateway),
        dependencies=deps,
    )
    assert result == {
        "state": "live",
        "thread_id": thread,
        "user_id": owner,
        "pod_ip": proof.host,
        "pod_port": 22,
        "host_key_fingerprint": proof.ssh_host_key_fingerprint,
        "lease_id": str(access.request.return_value["id"]),
        "binding": vm_binding_digest(proof),
    }
    access.request.assert_awaited_once_with(
        owner_kind="thread",
        owner_id=thread,
        kind="ssh",
        user_id=owner,
        connection_id="a" * 32,
    )
    common = dict(lease_id=result["lease_id"], binding=result["binding"])
    assert (
        await internal_vm_ssh_access(
            "renew",
            object(),
            _signed(gateway, "renew", **common),
            dependencies=deps,
        )
    ) == {"renewed": True}
    assert (
        await internal_vm_ssh_access(
            "close",
            object(),
            _signed(gateway, "close", **common),
            dependencies=deps,
        )
    ) == {"closed": True}


@pytest.mark.asyncio
async def test_bad_signature_or_not_owner_never_admits_or_wakes(route):
    gateway, _, _, _, access, deps = route
    bad = _signed(gateway)
    bad.proof["fingerprint"] = "SHA256:other"
    with pytest.raises(HTTPException) as error:
        await internal_vm_ssh_access("admit", object(), bad, dependencies=deps)
    assert error.value.status_code == 404
    access.request.assert_not_awaited()

    deps.user_can_access_ide_entity.return_value = False
    with pytest.raises(HTTPException) as error:
        await internal_vm_ssh_access(
            "admit",
            object(),
            _signed(gateway),
            dependencies=deps,
        )
    assert error.value.status_code == 404
    access.request.assert_not_awaited()


@pytest.mark.asyncio
async def test_runtime_successor_refuses_renew_and_missing_key_refuses_admit(route):
    gateway, _, _, proof, access, deps = route
    access.request.return_value["id"] = uuid4()
    signed = _signed(
        gateway,
        "renew",
        lease_id=str(access.request.return_value["id"]),
        binding=vm_binding_digest(proof),
    )
    proof.vmi_uid = str(uuid4())
    assert await internal_vm_ssh_access(
        "renew",
        object(),
        signed,
        dependencies=deps,
    ) == {"renewed": False}
    access.renew.assert_not_awaited()

    deps.operations.host_keys.load = lambda _paths: []
    with pytest.raises(HTTPException) as error:
        await internal_vm_ssh_access(
            "admit",
            object(),
            _signed(gateway),
            dependencies=deps,
        )
    assert error.value.status_code == 404
    access.request.assert_not_awaited()
