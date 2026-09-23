"""Pure policy and unsigned identity for same-generation Job VM creation retry.

These values are not authority. Store/controller callers must assemble fresh
facts and authenticate wire payloads with ``shared.vm_lifecycle_auth``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, fields
import hashlib
import json
import math
import re
from urllib.parse import parse_qsl, urlsplit
from uuid import UUID


VM_CREATION_RETRY_PROTOCOL = 1

_REFUSALS = (
    ("deadline_valid", "job_admission_expired"),
    ("canonical_request_proven", "creation_request_unproven"),
    ("generation_matches", "generation_changed"),
    ("disk_matches", "retained_disk_changed"),
    ("predecessor_settled", "predecessor_cleanup_pending"),
    ("control_available", "job_control_busy"),
    ("recovery_clear", "workspace_recovery_held"),
    ("controller_capable", "retry_protocol_unavailable"),
)
_CREATE_OPTION_FIELDS = frozenset(
    {
        "job_id",
        "entity_type",
        "provision_generation",
        "agent_config",
        "vm_image",
        "cpu_cores",
        "memory",
        "disk_size",
        "description",
        "nats_url",
        "network_tier",
        "orchestrator_url",
        "workspace_storage",
        "preparation",
        "initialization",
        "network_profile",
    }
)
_SENSITIVE_KEY_SUFFIXES = (
    "password",
    "passwd",
    "secret",
    "token",
    "apikey",
    "privatekey",
    "credentials",
    "credential",
    "authkey",
    "authorization",
    "secretaccesskey",
)


def retry_block_reason(facts: Mapping[str, object]) -> str | None:
    """Return the first unproven condition, in stable safety-policy order."""
    for field, reason in _REFUSALS:
        if facts.get(field) is not True:
            return reason
    return None


def retry_delay_seconds(attempt: int, jitter_fraction: float = 0.0) -> float:
    """Capped transport/polling delay; callers supply jitter, never identity."""
    if type(attempt) is not int or attempt < 1:
        raise ValueError("Retry attempt must be a positive integer.")
    if type(jitter_fraction) not in (int, float) or not 0 <= jitter_fraction <= 0.2:
        raise ValueError("Retry jitter must be between zero and 0.2.")
    base = 300 if attempt >= 7 else 5 * 2 ** (attempt - 1)
    return base * (1.0 + jitter_fraction)


def _canonical_uuid(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return str(UUID(value)) == value
    except ValueError:
        return False


@dataclass(frozen=True, slots=True)
class VMCreationRetryIdentity:
    """Validated wire identity; claim token is the durable UUID claim nonce.

    ``expected_pvc_uid=None`` explicitly means no retained disk. It never means
    that an unknown retained disk is acceptable. Transport protocol capability,
    envelope authentication and correlation are validated by the caller.
    """

    request_id: str
    job_id: str
    provision_generation: str
    request_digest: str
    expected_pvc_uid: str | None
    claim_token: str

    def __post_init__(self) -> None:
        for value in (
            self.request_id,
            self.job_id,
            self.provision_generation,
            self.claim_token,
        ):
            if not _canonical_uuid(value):
                raise ValueError("Retry identity requires canonical UUIDs.")
        if self.expected_pvc_uid is not None and not _canonical_uuid(
            self.expected_pvc_uid
        ):
            raise ValueError("Retry disk identity requires a canonical UUID.")
        if not isinstance(self.request_digest, str) or not re.fullmatch(
            r"sha256:[0-9a-f]{64}", self.request_digest
        ):
            raise ValueError("Retry identity requires a canonical SHA-256 digest.")

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> VMCreationRetryIdentity:
        """Reject unknown/missing wire fields; never normalize signed identity."""
        if not isinstance(value, Mapping) or set(value) != {
            field.name for field in fields(cls)
        }:
            raise ValueError("Retry identity fields are incomplete or unsupported.")
        return cls(**value)

    def to_dict(self) -> dict[str, object]:
        """Return a fresh unsigned payload for existing lifecycle transport."""
        return asdict(self)


def _sensitive_key(key: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]", "", key.lower())
    return normalized in {"auth", "lifecycleauth"} or normalized.endswith(
        _SENSITIVE_KEY_SUFFIXES
    )


def _validate_json(value: object, depth: int = 0) -> None:
    if depth > 64:
        raise ValueError("Create options are cyclic or nested too deeply.")
    if value is None or type(value) in (bool, int):
        return
    if type(value) is float and math.isfinite(value):
        return
    if type(value) is str:
        assignments = re.findall(
            r"(?:^|[\s;])(?:--)?([A-Za-z_][A-Za-z0-9_-]*)\s*[:=]", value
        )
        if (
            "PRIVATE KEY-----" in value
            or value.lower().startswith("bearer ")
            or any(_sensitive_key(key) for key in assignments)
            or (value.startswith("--") and _sensitive_key(value[2:]))
        ):
            raise ValueError("Create options must not contain credentials.")
        for url in re.findall(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s\"'<>]+", value):
            parsed = urlsplit(url)
            if (
                parsed.username is not None
                or parsed.password is not None
                or any(_sensitive_key(key) for key, _ in parse_qsl(parsed.query))
            ):
                raise ValueError("Create options must not contain URL credentials.")
        return
    if type(value) is list:
        for item in value:
            _validate_json(item, depth + 1)
        return
    if type(value) is dict:
        for key, item in value.items():
            if type(key) is not str or _sensitive_key(key):
                raise ValueError(
                    "Create options contain an unsupported or credential field."
                )
            _validate_json(item, depth + 1)
        return
    raise ValueError("Create options require canonical JSON values.")


def canonical_request_digest(options: Mapping[str, object]) -> str:
    """Hash every supplied unsigned create option without dropping any field.

    Accept only the current create-option vocabulary and exact JSON containers
    and scalars (finite numbers). Unknown top-level fields, transport envelopes,
    signatures and transport timestamps are refused. Nested semantic timestamps,
    revisions and public key identities remain part of the hash. Credential-key
    fields and recognizable embedded URL/private-key/bearer credentials are
    refused recursively; callers must still validate recipes and must never put
    arbitrary secret literals in command arguments or descriptions.

    This is identity validation, not create-schema validation: callers freeze a
    complete validated request with resolved defaults before hashing it. Absent
    and explicit-null options intentionally have different identities.
    """
    if not isinstance(options, Mapping) or set(options) - _CREATE_OPTION_FIELDS:
        raise ValueError("Unsupported unsigned create options.")
    snapshot = dict(options)
    if "network_profile" in snapshot:
        from shared.vm_network_profile import validate_network_profile

        validate_network_profile(snapshot["network_profile"])
    _validate_json(snapshot)
    try:
        encoded = json.dumps(
            snapshot,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise ValueError("Create options require canonical UTF-8 JSON.") from exc
    return "sha256:" + hashlib.sha256(encoded).hexdigest()
