"""A signed native notice reaches the existing watchdog under runtime authority."""

import asyncio
import dataclasses
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import asyncssh
import pytest

from agent.api.native_workspace_first_use import (
    NativeFirstUseRefused,
    apply_native_first_use,
    configured_public_keys,
)
from shared.native_workspace_first_use import mint_native_first_use_proof
from shared.pinned_session_identity import pinned_session_ready_identity_fingerprint
from tests.test_session_termination_boundary import owner


def test_agent_verifier_loads_selected_public_file_only(tmp_path, monkeypatch):
    signer = asyncssh.generate_private_key("ssh-ed25519")
    public = signer.export_public_key().decode()
    selected = tmp_path / "gateway_blue-2026.10.pub"
    selected.write_text(public, encoding="ascii")
    monkeypatch.setenv("SSH_GATEWAY_PUBLIC_HOST_KEYS", str(selected))
    assert configured_public_keys() == [public]

    selected.write_text(public + signer.export_private_key().decode(), encoding="ascii")
    assert configured_public_keys() == []
    monkeypatch.setenv("SSH_GATEWAY_PUBLIC_HOST_KEYS", str(selected) + ",/missing/key.pub")
    assert configured_public_keys() == []


@pytest.mark.asyncio
async def test_exact_signed_notice_latches_once_inside_runtime_transaction(monkeypatch):
    gateway = asyncssh.generate_private_key("ssh-ed25519")
    thread, generation, agent, attach, pod, process = [str(uuid4()) for _ in range(6)]
    fingerprint = pinned_session_ready_identity_fingerprint(
        thread_id=thread, runtime_generation=generation, agent_id=agent,
        runtime_attach_token=attach, pod_uid=pod,
    )
    proof = mint_native_first_use_proof(
        gateway, event_id="a" * 32, connection_id="b" * 32,
        channel_kind="ssh_session", handle="s-7f3a91c2", fingerprint="SHA256:user",
        thread_id=thread, runtime_generation=generation, agent_id=agent,
        pod_uid=pod, process_generation=process,
        session_identity_fingerprint=fingerprint, backend="container",
        workspace_digest="sha256:" + "c" * 64, lease_id="", binding="",
    )
    active = False

    @asynccontextmanager
    async def acquire():
        yield conn

    @asynccontextmanager
    async def transaction():
        nonlocal active
        active = True
        try:
            yield
        finally:
            active = False

    async def fetchrow(_query, *_args):
        assert active
        return {"metadata": {"dispatch_process_generation": process}}

    conn = SimpleNamespace(transaction=transaction, fetchrow=fetchrow)
    session = SimpleNamespace(postgres_conn=SimpleNamespace(acquire=acquire))
    identity = SimpleNamespace(
        thread_id=thread, session_generation=generation, attach_token=attach,
        agent_id=agent, pod_uid=pod, runtime_contract=True,
        fingerprint=lambda: fingerprint,
    )
    identity.snapshot = lambda: identity
    observed = []

    def note(life):
        assert active
        observed.append(life)
        return "accepted" if len(observed) == 1 else "already_observed"

    termination = SimpleNamespace(
        runtime_admission_closed=lambda: False, terminating=False,
        note_native_first_use=note,
    )

    async def lock(_conn, **_kwargs):
        assert active

    monkeypatch.setattr("agent.api.native_workspace_first_use.lock_runtime_authority", lock)
    args = dict(
        session=session, identity=identity, termination=termination,
        agent_id=agent, pod_uid=pod, process_generation=process,
        public_keys=[gateway.export_public_key().decode()],
    )
    recipient = dict(expected_thread_id=thread, expected_agent_id=agent,
                     expected_pod_uid=pod, expected_process_generation=process)
    first = await apply_native_first_use(proof, recipient, **args)
    second = await apply_native_first_use(proof, recipient, **args)
    assert first == {"event_id": "a" * 32, "session_identity_fingerprint": fingerprint,
                     "process_generation": process, "status": "accepted"}
    assert second["status"] == "already_observed"
    assert observed == [(thread, generation, agent, attach, pod, process)] * 2


@pytest.mark.asyncio
async def test_read_only_transaction_exit_failure_retains_verified_first_use_fact(
    monkeypatch, tmp_path,
):
    """A lost acknowledgement cannot roll back the lock-held local observation."""

    gateway = asyncssh.generate_private_key("ssh-ed25519")
    thread, generation, agent, attach, pod, process = [str(uuid4()) for _ in range(6)]
    life = (thread, generation, agent, attach, pod, process)
    fingerprint = pinned_session_ready_identity_fingerprint(
        thread_id=thread, runtime_generation=generation, agent_id=agent,
        runtime_attach_token=attach, pod_uid=pod,
    )
    proof = mint_native_first_use_proof(
        gateway, event_id="a" * 32, connection_id="b" * 32,
        channel_kind="ssh_session", handle="s-7f3a91c2", fingerprint="SHA256:user",
        thread_id=thread, runtime_generation=generation, agent_id=agent,
        pod_uid=pod, process_generation=process,
        session_identity_fingerprint=fingerprint, backend="container",
        workspace_digest="sha256:" + "c" * 64, lease_id="", binding="",
    )
    recipient = dict(
        expected_thread_id=thread, expected_agent_id=agent,
        expected_pod_uid=pod, expected_process_generation=process,
    )
    identity = SimpleNamespace(
        thread_id=thread, session_generation=generation, attach_token=attach,
        agent_id=agent, pod_uid=pod, runtime_contract=True,
        fingerprint=lambda: fingerprint,
        retirement_identity=lambda: (thread, generation, attach),
    )
    identity.snapshot = lambda: identity
    failing_exit = [True]
    active = False

    @asynccontextmanager
    async def acquire():
        yield conn

    @asynccontextmanager
    async def transaction():
        nonlocal active
        active = True
        try:
            yield
        finally:
            active = False
            if failing_exit[0]:
                raise RuntimeError("simulated read-only transaction exit failure")

    async def fetchrow(_query, *_args):
        assert active
        return {"metadata": {"dispatch_process_generation": process}}

    conn = SimpleNamespace(transaction=transaction, fetchrow=fetchrow)
    session = SimpleNamespace(postgres_conn=SimpleNamespace(acquire=acquire))
    client = SimpleNamespace(dispatch_process_generation=process)
    termination = owner(session=session)
    termination._ports = dataclasses.replace(
        termination._ports,
        identity=lambda: identity,
        orchestrator_client=lambda: client,
    )
    termination.termination_sentinel_path = tmp_path / "absent-termination-sentinel"
    termination.ws_connected_event = asyncio.Event()

    async def lock(_conn, **_kwargs):
        assert active

    monkeypatch.setattr("agent.api.native_workspace_first_use.lock_runtime_authority", lock)
    args = dict(
        session=session, identity=identity, termination=termination,
        agent_id=agent, pod_uid=pod, process_generation=process,
        public_keys=[gateway.export_public_key().decode()],
    )
    with pytest.raises(NativeFirstUseRefused, match="native_authority_unavailable"):
        await apply_native_first_use(proof, recipient, **args)
    assert termination.native_first_use_identity == life
    assert termination.ws_connected_event.is_set()

    # The same signed event can still get an idempotent receipt on retry.
    failing_exit[0] = False
    assert (await apply_native_first_use(proof, recipient, **args))["status"] == "already_observed"
    assert termination.native_first_use_identity == life

    # A successor process cannot use the predecessor's signed recipient.
    client.dispatch_process_generation = str(uuid4())
    args["process_generation"] = client.dispatch_process_generation
    with pytest.raises(NativeFirstUseRefused, match="recipient_authority_mismatch"):
        await apply_native_first_use(proof, recipient, **args)
    assert termination.native_first_use_identity == life
    assert termination.ws_connected_event.is_set()
