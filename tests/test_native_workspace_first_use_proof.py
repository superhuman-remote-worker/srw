"""A gateway notice is a separate, exact signed authority."""

from uuid import uuid4

import asyncssh

from shared.native_workspace_first_use import (
    mint_native_first_use_proof,
    verify_native_first_use_proof,
)
from orchestrator.services.ssh_gateway_vm_access_proof import mint_vm_access_proof


def test_signed_native_notice_binds_every_target_field_and_rejects_other_domains():
    gateway = asyncssh.generate_private_key("ssh-ed25519")
    other = asyncssh.generate_private_key("ssh-ed25519")
    fields = dict(
        event_id="a" * 32,
        connection_id="b" * 32,
        channel_kind="ssh_session",
        handle="s-7f3a91c2",
        fingerprint="SHA256:user",
        thread_id=str(uuid4()),
        runtime_generation=str(uuid4()),
        agent_id=str(uuid4()),
        pod_uid=str(uuid4()),
        process_generation=str(uuid4()),
        session_identity_fingerprint="sha256:" + "c" * 64,
        backend="container",
        workspace_digest="sha256:" + "d" * 64,
        lease_id="",
        binding="",
    )
    payload = mint_native_first_use_proof(gateway, **fields, now=1000)
    public = [gateway.export_public_key().decode()]
    assert verify_native_first_use_proof(payload, public, now=1001)
    for name, value in (
        ("thread_id", str(uuid4())),
        ("process_generation", str(uuid4())),
        ("pod_uid", str(uuid4())),
        ("workspace_digest", "sha256:" + "e" * 64),
        ("channel_kind", "sftp"),
        ("fingerprint", "SHA256:other"),
        ("event_id", "f" * 32),
        ("domain", "srw-vm-ssh-access1"),
    ):
        assert not verify_native_first_use_proof(
            {**payload, name: value}, public, now=1001
        )
    assert not verify_native_first_use_proof(
        payload, [other.export_public_key().decode()], now=1001
    )
    assert not verify_native_first_use_proof(payload, [], now=1001)
    assert not verify_native_first_use_proof(payload, public, now=1040)
    assert not verify_native_first_use_proof(
        {**payload, "extra": True}, public, now=1001
    )
    assert not verify_native_first_use_proof(
        {**payload, "expires_at": True}, public, now=1001
    )
    assert not verify_native_first_use_proof(
        {**payload, "signature": ""}, public, now=1001
    )
    assert verify_native_first_use_proof(
        payload,
        [other.export_public_key().decode(), *public],
        now=1001,
    )
    vm_access = mint_vm_access_proof(
        gateway,
        connection_id="a" * 32,
        handle="s-7f3a91c2",
        fingerprint="SHA256:user",
        action="admit",
        now=1000,
    )
    assert not verify_native_first_use_proof(vm_access, public, now=1001)


def test_native_verifier_rejects_malformed_types_and_unexpected_field_values():
    gateway = asyncssh.generate_private_key("ssh-ed25519")
    ids = [str(uuid4()) for _ in range(5)]
    proof = mint_native_first_use_proof(
        gateway,
        event_id="a" * 32,
        connection_id="b" * 32,
        channel_kind="sftp",
        handle="s-7f3a91c2",
        fingerprint="SHA256:user",
        thread_id=ids[0],
        runtime_generation=ids[1],
        agent_id=ids[2],
        pod_uid=ids[3],
        process_generation=ids[4],
        session_identity_fingerprint="sha256:" + "a" * 64,
        backend="container",
        workspace_digest="sha256:" + "b" * 64,
        lease_id="",
        binding="",
        now=1000,
    )
    public = [gateway.export_public_key().decode()]
    for field, value in (
        ("action", "renew"),
        ("connection_id", "z" * 32),
        ("handle", "../thread"),
        ("runtime_generation", str(uuid4())),
        ("agent_id", str(uuid4())),
        ("pod_uid", str(uuid4())),
        ("process_generation", str(uuid4())),
        ("session_identity_fingerprint", "sha256:" + "c" * 64),
        ("backend", "vm"),
        ("lease_id", str(uuid4())),
        ("binding", "c" * 64),
        ("key_fingerprint", "SHA256:other"),
        ("expires_at", 1031),
        ("channel_kind", ["sftp"]),
        ("fingerprint", "bad\nvalue"),
        ("signature", "a" * 10000),
    ):
        assert not verify_native_first_use_proof(
            {**proof, field: value},
            public,
            now=1001,
        ), field
    assert not verify_native_first_use_proof(proof, public, now=1030)
