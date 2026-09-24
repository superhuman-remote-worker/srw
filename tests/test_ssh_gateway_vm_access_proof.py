"""The signed VM admission is narrower than the internal gateway key."""

import asyncssh

from orchestrator.services.ssh_gateway_vm_access_proof import (
    mint_vm_access_proof,
    verify_vm_access_proof,
)


def test_signed_connection_proof_binds_action_handle_key_and_short_clock():
    gateway = asyncssh.generate_private_key("ssh-ed25519")
    other = asyncssh.generate_private_key("ssh-ed25519")
    public = [gateway.export_public_key().decode()]
    payload = mint_vm_access_proof(
        gateway, connection_id="a" * 32, handle="s-7f3a91c2",
        fingerprint="SHA256:key", action="admit", now=1000,
    )
    assert verify_vm_access_proof(payload, public, action="admit", now=1001)
    for change in (
        {"action": "renew"}, {"handle": "s-7f3a91c3"},
        {"fingerprint": "SHA256:other"}, {"connection_id": "b" * 32},
        {"expires_at": 2000},
    ):
        assert not verify_vm_access_proof(
            {**payload, **change}, public, action="admit", now=1001,
        )
    assert not verify_vm_access_proof(payload, public, action="renew", now=1001)
    assert not verify_vm_access_proof(payload, public, action="admit", now=1040)
    assert not verify_vm_access_proof(payload, [], action="admit", now=1001)
    assert not verify_vm_access_proof(
        payload, [other.export_public_key().decode()], action="admit", now=1001,
    )


def test_future_window_and_malformed_payload_refuse():
    gateway = asyncssh.generate_private_key("ssh-ed25519")
    public = [gateway.export_public_key().decode()]
    payload = mint_vm_access_proof(
        gateway, connection_id="c" * 32, handle="s-7f3a91c2",
        fingerprint="SHA256:key", action="admit", now=2000,
    )
    assert not verify_vm_access_proof(payload, public, action="admit", now=1000)
    for value in ({}, {**payload, "signature": "a" * 10000},
                  {**payload, "fingerprint": "bad\nkey"}):
        assert not verify_vm_access_proof(value, public, action="admit", now=2000)
