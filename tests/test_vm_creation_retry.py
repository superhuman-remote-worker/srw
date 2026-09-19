"""Fail-closed policy and immutable identity for same-generation VM retries."""

from copy import deepcopy
from dataclasses import FrozenInstanceError

import pytest

from shared.vm_creation_retry import (
    VMCreationRetryIdentity,
    canonical_request_digest,
    retry_block_reason,
    retry_delay_seconds,
)


REFUSALS = [
    ("deadline_valid", "job_admission_expired"),
    ("canonical_request_proven", "creation_request_unproven"),
    ("generation_matches", "generation_changed"),
    ("disk_matches", "retained_disk_changed"),
    ("predecessor_settled", "predecessor_cleanup_pending"),
    ("control_available", "job_control_busy"),
    ("recovery_clear", "workspace_recovery_held"),
    ("controller_capable", "retry_protocol_unavailable"),
]
JOB_ID = "00000000-0000-4000-8000-000000000001"
GENERATION = "00000000-0000-4000-8000-000000000002"


@pytest.mark.parametrize("field,reason", REFUSALS)
@pytest.mark.parametrize("unproven", [False, None, 1, "true", [], {}])
def test_unproven_retry_is_refused(field, reason, unproven):
    facts = dict.fromkeys((key for key, _ in REFUSALS), True)
    facts[field] = unproven
    assert retry_block_reason(facts) == reason


@pytest.mark.parametrize("field,reason", REFUSALS)
def test_missing_evidence_is_refused(field, reason):
    facts = dict.fromkeys((key for key, _ in REFUSALS), True)
    del facts[field]
    assert retry_block_reason(facts) == reason


def test_all_proven_is_admitted_and_reason_precedence_is_stable():
    assert retry_block_reason(dict.fromkeys((key for key, _ in REFUSALS), True)) is None
    assert retry_block_reason({}) == "job_admission_expired"
    assert (
        retry_block_reason(dict.fromkeys((key for key, _ in reversed(REFUSALS)), False))
        == "job_admission_expired"
    )


def test_transport_backoff_is_bounded_even_for_very_large_attempts():
    assert [retry_delay_seconds(i) for i in range(1, 9)] == [
        5,
        10,
        20,
        40,
        80,
        160,
        300,
        300,
    ]
    assert retry_delay_seconds(8, 0.2) == 360
    assert retry_delay_seconds(10**100) == 300
    assert retry_delay_seconds(1, 0.1) == 5.5


@pytest.mark.parametrize("attempt", [0, -1, True, 1.5, "1", None])
def test_invalid_attempt_is_rejected(attempt):
    with pytest.raises(ValueError):
        retry_delay_seconds(attempt)


@pytest.mark.parametrize(
    "jitter", [-0.01, 0.21, float("nan"), float("inf"), True, "0", None]
)
def test_invalid_jitter_is_rejected(jitter):
    with pytest.raises(ValueError):
        retry_delay_seconds(1, jitter)


def identity_fields():
    return {
        "request_id": "00000000-0000-4000-8000-000000000003",
        "job_id": JOB_ID,
        "provision_generation": GENERATION,
        "request_digest": "sha256:" + "a" * 64,
        "expected_pvc_uid": "00000000-0000-4000-8000-000000000004",
        "claim_token": "00000000-0000-4000-8000-000000000005",
    }


def test_identity_round_trip_preserves_exact_wire_fields_and_is_immutable():
    fields = identity_fields()
    identity = VMCreationRetryIdentity.from_mapping(fields)
    assert identity.to_dict() == fields
    with pytest.raises(FrozenInstanceError):
        identity.claim_token = 2
    fields["claim_token"] = 2
    assert identity.claim_token == "00000000-0000-4000-8000-000000000005"


def test_new_disk_identity_explicitly_allows_no_captured_pvc():
    fields = {**identity_fields(), "expected_pvc_uid": None}
    assert VMCreationRetryIdentity(**fields).to_dict() == fields


@pytest.mark.parametrize(
    "field",
    ["request_id", "job_id", "provision_generation", "expected_pvc_uid", "claim_token"],
)
@pytest.mark.parametrize(
    "invalid",
    [
        "bad",
        "00000000000040008000000000000001",
        "ABCDEF00-0000-4000-8000-000000000001",
        1,
    ],
)
def test_identity_rejects_malformed_or_noncanonical_uuid(field, invalid):
    with pytest.raises(ValueError):
        VMCreationRetryIdentity(**{**identity_fields(), field: invalid})


@pytest.mark.parametrize(
    "invalid",
    ["a" * 64, "sha256:" + "a" * 63, "sha256:" + "A" * 64, "sha256:" + "z" * 64, None],
)
def test_identity_rejects_malformed_digest(invalid):
    with pytest.raises(ValueError):
        VMCreationRetryIdentity(**{**identity_fields(), "request_digest": invalid})


@pytest.mark.parametrize("invalid", [0, -1, True, 1.0, "1", None])
def test_identity_rejects_invalid_claim_token(invalid):
    with pytest.raises(ValueError):
        VMCreationRetryIdentity(**{**identity_fields(), "claim_token": invalid})


def test_wire_identity_rejects_missing_and_unknown_fields():
    fields = identity_fields()
    with pytest.raises(ValueError):
        VMCreationRetryIdentity.from_mapping({**fields, "protocol": 2})
    del fields["expected_pvc_uid"]
    with pytest.raises(ValueError):
        VMCreationRetryIdentity.from_mapping(fields)


def create_options():
    return {
        "job_id": JOB_ID,
        "provision_generation": GENERATION,
        "entity_type": "job",
        "vm_image": "registry.example/image@sha256:abc",
        "agent_config": "worker_base",
        "cpu_cores": 8,
        "memory": "16Gi",
        "disk_size": "100Gi",
        "network_tier": "restricted",
        "workspace_storage": {"pvc_uid": "old-disk", "generation": 2},
        "preparation": {"revision": "prep-v1"},
        "initialization": {"steps": [{"command": ["echo", "hello"]}]},
    }


def test_digest_ignores_mapping_order_but_preserves_list_order_and_input():
    options = create_options()
    original = deepcopy(options)
    digest = canonical_request_digest(options)
    assert digest == canonical_request_digest(dict(reversed(list(options.items()))))
    assert options == original
    options["initialization"]["steps"][0]["command"].reverse()
    assert canonical_request_digest(options) != digest


@pytest.mark.parametrize(
    "field",
    [
        "vm_image",
        "agent_config",
        "cpu_cores",
        "memory",
        "disk_size",
        "network_tier",
        "workspace_storage",
        "preparation",
        "initialization",
        "provision_generation",
    ],
)
def test_every_workspace_option_change_changes_digest(field):
    original = create_options()
    changed = deepcopy(original)
    changed[field] = None
    assert canonical_request_digest(changed) != canonical_request_digest(original)


@pytest.mark.parametrize(
    "field",
    [
        "token",
        "password",
        "api_key",
        "credentials",
        "ssh_private_key",
        "tailscale_auth_key",
        "_lifecycle_auth",
    ],
)
def test_digest_rejects_sensitive_or_transport_fields_even_when_nested(field):
    options = create_options()
    options["preparation"] = {"nested": {field: "must-not-persist"}}
    with pytest.raises(ValueError):
        canonical_request_digest(options)


def test_digest_preserves_semantic_timestamps_and_key_identities():
    options = {"preparation": {"deadline": 123, "created_at": 100, "key": "public-id"}}
    changed = deepcopy(options)
    changed["preparation"]["deadline"] = 124
    assert canonical_request_digest(options) != canonical_request_digest(changed)


@pytest.mark.parametrize(
    "field", ["signature", "issued_at", "created_at", "_lifecycle_auth"]
)
def test_digest_refuses_top_level_transport_fields(field):
    with pytest.raises(ValueError):
        canonical_request_digest({field: "not-create-options"})


@pytest.mark.parametrize(
    "bad",
    [
        {1: "key"},
        {"a": (1, 2)},
        {"a": {1, 2}},
        {"a": float("nan")},
        {"a": float("inf")},
        {"a": object()},
        {"a": b"bytes"},
    ],
)
def test_digest_rejects_values_json_would_coerce_or_cannot_represent(bad):
    with pytest.raises(ValueError):
        canonical_request_digest({"preparation": bad})


@pytest.mark.parametrize(
    "url", ["https://user:secret@example.com", "https://example.com/?token=secret"]
)
def test_digest_rejects_credentials_embedded_in_endpoints(url):
    with pytest.raises(ValueError):
        canonical_request_digest({"orchestrator_url": url})


def test_digest_rejects_unknown_options_instead_of_silently_omitting_them():
    with pytest.raises(ValueError):
        canonical_request_digest({"new_create_option": True})


@pytest.mark.parametrize(
    "field", ["AWS_SECRET_ACCESS_KEY", "auth", "clientSecret", "X-Api-Key"]
)
def test_digest_refuses_common_credential_spellings(field):
    with pytest.raises(ValueError):
        canonical_request_digest({"preparation": {field: "credential"}})


@pytest.mark.parametrize(
    "argument",
    [
        "PASSWORD=secret",
        "--token=secret",
        "Authorization: Bearer secret",
        "curl https://user:secret@example.com",
    ],
)
def test_digest_refuses_recognizable_credentials_in_command_arguments(argument):
    with pytest.raises(ValueError):
        canonical_request_digest({"initialization": {"command": [argument]}})


def test_digest_refuses_cycles_and_invalid_unicode_without_coercing_them():
    cyclic = []
    cyclic.append(cyclic)
    with pytest.raises(ValueError):
        canonical_request_digest({"initialization": cyclic})
    with pytest.raises(ValueError):
        canonical_request_digest({"description": "\ud800"})


def test_digest_keeps_absent_null_and_numeric_type_distinct():
    assert canonical_request_digest({}) != canonical_request_digest({"vm_image": None})
    assert canonical_request_digest({"cpu_cores": 8}) != canonical_request_digest(
        {"cpu_cores": 8.0}
    )


def test_digest_matches_known_canonical_json_vector():
    # SHA-256 of the UTF-8 bytes {"cpu_cores":8}, without whitespace.
    assert (
        canonical_request_digest({"cpu_cores": 8})
        == "sha256:459c1ce4ab8647215df622381962cd9d045a2348d4f47250507ccddb51ee5533"
    )
