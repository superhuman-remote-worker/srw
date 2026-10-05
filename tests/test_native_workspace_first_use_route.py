"""The orchestrator relays only a current signed exact pinned notice."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncssh
import pytest

from orchestrator.routers.ssh_access import (
    SshAccessDependencies,
    _native_workspace_current,
    internal_ssh_native_first_use,
)
from orchestrator.services.native_workspace_first_use import container_workspace_digest
from shared.pinned_session_identity import pinned_session_ready_identity_fingerprint
from shared.native_workspace_first_use import mint_native_first_use_proof


@pytest.mark.asyncio
async def test_tampered_notice_never_reaches_agent(monkeypatch):
    gateway = asyncssh.generate_private_key("ssh-ed25519")
    proof = mint_native_first_use_proof(
        gateway, event_id="a" * 32, connection_id="b" * 32,
        channel_kind="ssh_session", handle="s-7f3a91c2", fingerprint="SHA256:user",
        thread_id=str(uuid4()), runtime_generation=str(uuid4()), agent_id=str(uuid4()),
        pod_uid=str(uuid4()), process_generation=str(uuid4()),
        session_identity_fingerprint="sha256:" + "c" * 64,
        backend="container", workspace_digest="sha256:" + "d" * 64,
        lease_id="", binding="",
    )
    proof["agent_id"] = str(uuid4())
    get_thread = AsyncMock()
    dependencies = SshAccessDependencies(
        store=SimpleNamespace(get_thread=get_thread),
        operations=SimpleNamespace(host_keys=SimpleNamespace(load=lambda _: [
            {"public_key": gateway.export_public_key().decode()}
        ])),
        require_internal=AsyncMock(),
    )
    monkeypatch.setenv("SSH_GATEWAY_PUBLIC_HOST_KEYS", "/pub/gateway.pub")
    request = SimpleNamespace(json=AsyncMock(return_value={"proof": proof}))
    result = await internal_ssh_native_first_use(request, dependencies=dependencies)
    assert result.status_code == 404
    get_thread.assert_not_awaited()


@pytest.mark.asyncio
async def test_workspace_rotation_with_same_pinned_process_refuses_relay():
    thread_id, generation, agent_id, attach, pod = [str(uuid4()) for _ in range(5)]
    workspace_generation = str(uuid4())
    binding = {
        "kind": "remote", "generation": workspace_generation,
        "backing_id": str(uuid4()), "ssh_host_key_fingerprint": "SHA256:host",
    }
    thread = {
        "id": thread_id, "execution_lane": "pinned", "status": "active",
        "agent_id": agent_id, "runtime_generation": generation,
        "runtime_attach_token": attach, "runtime_retirement_token": None,
        "metadata": {
            "_workspace_binding": binding,
            "workspace_container": {
                "status": "ready", "_canvas_workspace_generation": workspace_generation,
                "ssh_host": "workspace", "ssh_port": 22,
            },
        },
    }
    original = container_workspace_digest(thread)
    assert original is not None
    proof = {
        "thread_id": thread_id, "runtime_generation": generation,
        "agent_id": agent_id, "workspace_digest": original,
        "pod_uid": pod,
        "session_identity_fingerprint": pinned_session_ready_identity_fingerprint(
            thread_id=thread_id, runtime_generation=generation,
            agent_id=agent_id, runtime_attach_token=attach, pod_uid=pod,
        ),
        "backend": "container",
    }
    store = SimpleNamespace(get_thread=AsyncMock(return_value=thread))
    dependencies = SshAccessDependencies(store=store, operations=SimpleNamespace())
    assert await _native_workspace_current(proof, {}, dependencies)
    binding["backing_id"] = str(uuid4())
    assert not await _native_workspace_current(proof, {}, dependencies)
    thread["metadata"]["vm"] = {"status": "ready"}
    assert not await _native_workspace_current(proof, {}, dependencies)


@pytest.mark.asyncio
@pytest.mark.parametrize("change_during_forward", [None, "workspace", "user_key"])
async def test_relay_forwards_unchanged_proof_to_exact_recipient_and_rechecks_after_reply(
    monkeypatch, change_during_forward,
):
    signer = asyncssh.generate_private_key("ssh-ed25519")
    thread_id, generation, agent_id, attach, pod, process = [str(uuid4()) for _ in range(6)]
    workspace_generation = str(uuid4())
    thread = {
        "id": thread_id, "execution_lane": "pinned", "status": "active",
        "agent_id": agent_id, "runtime_generation": generation,
        "runtime_attach_token": attach, "runtime_retirement_token": None,
        "metadata": {
            "_workspace_binding": {
                "kind": "remote", "generation": workspace_generation,
                "backing_id": str(uuid4()), "ssh_host_key_fingerprint": "SHA256:host",
            },
            "workspace_container": {
                "status": "ready", "_canvas_workspace_generation": workspace_generation,
                "ssh_host": "workspace", "ssh_port": 22,
            },
        },
    }
    recipient = {
        "thread_id": thread_id, "runtime_generation": generation,
        "agent_id": agent_id, "pod_uid": pod,
        "process_generation": process,
        "session_identity_fingerprint": pinned_session_ready_identity_fingerprint(
            thread_id=thread_id, runtime_generation=generation,
            agent_id=agent_id, runtime_attach_token=attach, pod_uid=pod,
        ),
        "workspace_digest": container_workspace_digest(thread),
    }
    proof = mint_native_first_use_proof(
        signer, event_id="a" * 32, connection_id="b" * 32,
        channel_kind="ssh_session", handle="s-7f3a91c2", fingerprint="SHA256:user",
        backend="container", lease_id="", binding="", **recipient,
    )
    expected_agent_recipient = {
        "expected_thread_id": thread_id, "expected_agent_id": agent_id,
        "expected_pod_uid": pod, "expected_process_generation": process,
    }
    target = SimpleNamespace(
        binding=SimpleNamespace(pod_ip="10.0.0.2", pod_port=8001),
        recipient=expected_agent_recipient,
    )
    async def description(*_args, **_kwargs):
        return {"native_first_use_contract": 1, "native_recipient": recipient}, target

    monkeypatch.setattr("orchestrator.routers.ssh_access._native_description", description)
    monkeypatch.setattr(
        "orchestrator.routers.ssh_access.pinned_session_mutation_target_is_current",
        AsyncMock(return_value=True),
    )
    forwarded = []
    key_owner = {"id": str(uuid4())}
    key_active = True

    class Client:
        def __init__(self, *, timeout):
            assert timeout == 5.0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def post(self, url, *, json):
            nonlocal key_active
            forwarded.append((url, json))
            if change_during_forward == "workspace":
                thread["metadata"]["_workspace_binding"]["backing_id"] = str(uuid4())
            elif change_during_forward == "user_key":
                key_active = False
            return SimpleNamespace(status_code=200, json=lambda: {
                "event_id": proof["event_id"],
                "session_identity_fingerprint": proof["session_identity_fingerprint"],
                "process_generation": proof["process_generation"],
                "status": "accepted",
            })

    monkeypatch.setattr("orchestrator.routers.ssh_access.httpx.AsyncClient", Client)
    store = SimpleNamespace(
        get_thread_id_by_ssh_handle=AsyncMock(return_value=thread_id),
        resolve_user_by_ssh_fingerprint=AsyncMock(side_effect=lambda _: key_owner if key_active else None),
        get_thread=AsyncMock(return_value=thread),
    )
    dependencies = SshAccessDependencies(
        store=store,
        operations=SimpleNamespace(host_keys=SimpleNamespace(load=lambda _: [
            {"public_key": signer.export_public_key().decode()}
        ])),
        require_internal=AsyncMock(),
        user_can_access_ide_entity=AsyncMock(return_value=True),
        native_mutation_dependencies=object(),
    )
    monkeypatch.setenv("SSH_GATEWAY_PUBLIC_HOST_KEYS", "/pub/gateway.pub")
    request = SimpleNamespace(json=AsyncMock(return_value={"proof": proof}))
    response = await internal_ssh_native_first_use(request, dependencies=dependencies)
    assert response.status_code == (409 if change_during_forward else 200)
    assert forwarded == [(
        "http://10.0.0.2:8001/session/native-first-use",
        {"proof": proof, "_recipient": expected_agent_recipient},
    )]
