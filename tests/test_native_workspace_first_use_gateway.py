"""The gateway retries the same exact event and requires its acknowledgement."""

import asyncio
from types import SimpleNamespace
from uuid import uuid4

import asyncssh
import pytest

from orchestrator.services.ssh_gateway_client import SshTarget, post_native_first_use


def _target():
    thread, generation, agent, pod, process = [str(uuid4()) for _ in range(5)]
    return SshTarget(
        thread_id=thread,
        user_id=str(uuid4()),
        pod_ip="10.0.0.2",
        pod_port=22,
        host_key_fingerprint="SHA256:host",
        state="live",
        backend="container",
        execution_lane="pinned",
        native_recipient={
            "thread_id": thread,
            "runtime_generation": generation,
            "agent_id": agent,
            "pod_uid": pod,
            "process_generation": process,
            "session_identity_fingerprint": "sha256:" + "a" * 64,
            "workspace_digest": "sha256:" + "b" * 64,
        },
    )


def _config():
    return SimpleNamespace(
        orchestrator_url="http://orchestrator",
        internal_key="internal",
        orchestrator_request_timeout=5.0,
    )


@pytest.mark.asyncio
async def test_lost_first_reply_retries_one_signed_event(monkeypatch):
    signer = asyncssh.generate_private_key("ssh-ed25519")
    target = _target()
    attempts = []

    async def post(_url, *, headers, json, timeout):
        proof = json["proof"]
        attempts.append(proof)
        if len(attempts) == 1:
            raise ConnectionError("reply lost")
        return SimpleNamespace(
            status_code=200,
            json=lambda: {
                "event_id": proof["event_id"],
                "session_identity_fingerprint": proof["session_identity_fingerprint"],
                "process_generation": proof["process_generation"],
                "status": "already_observed",
            },
        )

    monkeypatch.setattr("orchestrator.services.ssh_gateway_client._http_post", post)
    assert await post_native_first_use(
        _config(),
        signer,
        target,
        event_id="c" * 32,
        connection_id="d" * 32,
        channel_kind="ssh_session",
        handle="s-7f3a91c2",
        fingerprint="SHA256:user",
        still_live=lambda: True,
    )
    assert len(attempts) == 2
    assert attempts[0] == attempts[1]


@pytest.mark.asyncio
async def test_peer_loss_prevents_retry_after_lost_reply(monkeypatch):
    live = True
    attempts = []

    async def post(_url, *, headers, json, timeout):
        nonlocal live
        attempts.append(json["proof"])
        live = False
        raise ConnectionError("reply lost")

    monkeypatch.setattr("orchestrator.services.ssh_gateway_client._http_post", post)
    assert not await post_native_first_use(
        _config(),
        asyncssh.generate_private_key("ssh-ed25519"),
        _target(),
        event_id="c" * 32,
        connection_id="d" * 32,
        channel_kind="ssh_session",
        handle="s-7f3a91c2",
        fingerprint="SHA256:user",
        still_live=lambda: live,
    )
    assert len(attempts) == 1


@pytest.mark.asyncio
async def test_notice_has_hard_five_second_deadline_even_if_transport_ignores_timeout(
    monkeypatch,
):
    attempts = []

    async def post(_url, *, headers, json, timeout):
        attempts.append(timeout)
        await asyncio.sleep(6)
        raise AssertionError("transport exceeded native notice deadline")

    monkeypatch.setattr("orchestrator.services.ssh_gateway_client._http_post", post)
    started = asyncio.get_running_loop().time()
    assert not await post_native_first_use(
        _config(),
        asyncssh.generate_private_key("ssh-ed25519"),
        _target(),
        event_id="c" * 32,
        connection_id="d" * 32,
        channel_kind="ssh_session",
        handle="s-7f3a91c2",
        fingerprint="SHA256:user",
        still_live=lambda: True,
    )
    assert asyncio.get_running_loop().time() - started < 5.5
    assert len(attempts) == 1
