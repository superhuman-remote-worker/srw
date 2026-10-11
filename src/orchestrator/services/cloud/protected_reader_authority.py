"""Pure authority for one attempt-scoped protected Nextcloud reader grant.

The plan deliberately contains no credentials, URL, user identity, or thread
identity.  It derives both remote principal names from the complete engage
attempt UUID and binds their grant handle to the canonical protected source.
The independent ``backend_instance_id`` names an immutable, durably resolvable
Nextcloud configuration revision; it is supplied by configuration authority,
never reconstructed from a mutable URL.
Backend code may consume the plan later, but constructing or parsing one has
no external effects.

The dependency direction is safe for backend integration:
``cloud.nextcloud -> cloud.protected_reader_authority -> cloud_staging.source_identity
-> cloud.handles``.  None of the latter modules imports a backend.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from orchestrator.services.cloud_staging.source_identity import (
    ProtectedMountSourceIdentity,
)
from orchestrator.services.cloud.handles import ProjectFolderHandle


PROTECTED_NEXTCLOUD_GRANT_VERSION = 1

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_GRANT_HANDLE_KEYS = frozenset(
    {
        "version",
        "backend",
        "backend_instance_id",
        "engage_attempt",
        "reader_id",
        "group_id",
        "folder_id",
        "mountpoint",
        "source_sha256",
    }
)


def _canonical_uuid(value: Any, *, coordinate: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"protected reader {coordinate} is not a canonical UUID")
    try:
        parsed = UUID(value)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError(
            f"protected reader {coordinate} is not a canonical UUID"
        ) from exc
    canonical = str(parsed)
    if not parsed.int or value != canonical:
        raise ValueError(f"protected reader {coordinate} is not a canonical UUID")
    return canonical


def _canonical_json(value: dict[str, Any]) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


@dataclass(frozen=True, slots=True)
class ProtectedNextcloudReaderGrantPlan:
    """Deterministic, non-secret authority for one remote grant attempt."""

    engage_attempt: str
    backend_instance_id: str
    source: ProtectedMountSourceIdentity
    version: int = PROTECTED_NEXTCLOUD_GRANT_VERSION
    backend: str = "nextcloud"

    def __post_init__(self) -> None:
        if (
            type(self.version) is not int
            or self.version != PROTECTED_NEXTCLOUD_GRANT_VERSION
        ):
            raise ValueError("protected reader grant version is unsupported")
        if not isinstance(self.backend, str) or self.backend != "nextcloud":
            raise ValueError("protected reader grant backend is unsupported")
        if not isinstance(self.source, ProtectedMountSourceIdentity):
            raise ValueError("protected reader grant source is malformed")
        if self.source.backend != self.backend:
            raise ValueError("protected reader grant source backend does not match")
        object.__setattr__(
            self,
            "engage_attempt",
            _canonical_uuid(self.engage_attempt, coordinate="attempt"),
        )
        object.__setattr__(
            self,
            "backend_instance_id",
            _canonical_uuid(
                self.backend_instance_id,
                coordinate="backend instance",
            ),
        )
        if self.backend_instance_id != self.source.backend_instance_id:
            raise ValueError(
                "protected reader backend instance does not match its source"
            )

    @property
    def attempt_hex(self) -> str:
        """The complete UUID payload used in both remote principal names."""

        return UUID(self.engage_attempt).hex

    @property
    def reader_id(self) -> str:
        return f"srw-reader-a-{self.attempt_hex}"

    @property
    def group_id(self) -> str:
        return f"srw-rog-a-{self.attempt_hex}"

    @property
    def folder_id(self) -> str:
        return self.source.native_id

    @property
    def mountpoint(self) -> str:
        return self.source.mountpoint

    @property
    def source_sha256(self) -> str:
        return self.source.sha256

    @property
    def grant_handle_binding(self) -> dict[str, Any]:
        """The exact non-secret object persisted as the opaque grant handle."""

        return {
            "version": self.version,
            "backend": self.backend,
            "backend_instance_id": self.backend_instance_id,
            "engage_attempt": self.engage_attempt,
            "reader_id": self.reader_id,
            "group_id": self.group_id,
            "folder_id": self.folder_id,
            "mountpoint": self.mountpoint,
            "source_sha256": self.source_sha256,
        }

    @property
    def grant_handle(self) -> str:
        """Canonical compact JSON suitable for the existing TEXT handle."""

        return _canonical_json(self.grant_handle_binding)

    @property
    def grant_handle_sha256(self) -> str:
        return hashlib.sha256(self.grant_handle.encode("utf-8")).hexdigest()

    def to_project_folder_handle(self) -> ProjectFolderHandle:
        """Recover only the folder coordinates covered by this authority."""

        return self.source.to_project_folder_handle()

    @classmethod
    def from_grant_handle(
        cls,
        grant_handle: str,
        *,
        expected_sha256: str,
        expected_engage_attempt: str,
        expected_backend_instance_id: str,
        expected_source: ProtectedMountSourceIdentity,
    ) -> ProtectedNextcloudReaderGrantPlan | None:
        """Adopt only the exact canonical handle for the expected authority.

        A caller must already hold the durable attempt, immutable backend
        configuration identity, and full logical source.  Merely presenting a
        self-consistent handle and recomputed digest is not enough to redirect
        authority to a different installation, attempt, or folder.
        """

        if (
            not isinstance(grant_handle, str)
            or not grant_handle
            or not isinstance(expected_sha256, str)
            or not _SHA256_RE.fullmatch(expected_sha256)
            or not isinstance(expected_source, ProtectedMountSourceIdentity)
        ):
            return None
        try:
            expected = cls(
                engage_attempt=expected_engage_attempt,
                backend_instance_id=expected_backend_instance_id,
                source=expected_source,
            )
        except (TypeError, ValueError, AttributeError):
            return None
        try:
            decoded = json.loads(grant_handle)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        if not isinstance(decoded, dict) or set(decoded) != _GRANT_HANDLE_KEYS:
            return None

        # Comparing canonical bytes rejects types that compare equal in Python
        # (True/1 and 1.0/1), alternate UUID spellings, extra whitespace,
        # reordered keys, escaped Unicode, and every derived-coordinate drift.
        if grant_handle != expected.grant_handle:
            return None
        if decoded != expected.grant_handle_binding:
            return None
        if expected.grant_handle_sha256 != expected_sha256:
            return None
        return expected

    @classmethod
    def from_ro_mount_row(
        cls,
        row: Mapping[str, Any] | None,
    ) -> ProtectedNextcloudReaderGrantPlan | None:
        """Adopt an exact non-secret authority projection from PostgreSQL."""

        if not isinstance(row, Mapping) or row.get("backend") != "nextcloud":
            return None
        source = ProtectedMountSourceIdentity.from_binding(
            row.get("source_binding"),
            expected_sha256=str(row.get("source_binding_sha256") or ""),
        )
        if source is None:
            return None
        plan = cls.from_grant_handle(
            str(row.get("grant_handle") or ""),
            expected_sha256=str(row.get("grant_handle_sha256") or ""),
            expected_engage_attempt=str(row.get("engage_attempt") or ""),
            expected_backend_instance_id=str(row.get("backend_instance_id") or ""),
            expected_source=source,
        )
        if plan is None:
            return None
        if (
            row.get("reader_id") != plan.reader_id
            or row.get("grant_group_id") != plan.group_id
        ):
            return None
        return plan


def readerless_protected_life(
    captured: Mapping[str, Any] | None,
    current: Mapping[str, Any] | None,
    *,
    thread_id: str,
    runtime_generation: str,
) -> bool:
    """Whether one protected life minted no reader, so it never had the folder.

    Main cloud slice 5 refuses a demoted owner's re-engage on Resume, which
    leaves the thread's only reader row an earlier life's, revoked; a session
    whose first engage never installed a row has none. Rows are never deleted,
    and only a new engage moves the row to the current generation, which a
    pending retirement refuses. Begin's ``captured`` copy and the locked
    ``current`` row of either shape therefore prove that no credential for the
    folder reached this life's runtime, whatever its agent did.
    """

    if captured is None:
        return current is None
    if not isinstance(captured, Mapping) or not isinstance(current, Mapping):
        return False
    plan = ProtectedNextcloudReaderGrantPlan.from_ro_mount_row(captured)
    generation = str(captured.get("runtime_generation") or "")
    return bool(
        plan is not None
        and generation
        and generation != str(runtime_generation)
        and str(captured.get("thread_id") or "") == str(thread_id)
        and captured.get("status") == "revoked"
        and str(current.get("id") or "") == str(captured.get("id") or "")
        and str(current.get("thread_id") or "") == str(thread_id)
        and str(current.get("runtime_generation") or "") == generation
        and str(current.get("engage_attempt") or "")
        == str(captured.get("engage_attempt") or "")
        and current.get("status") == "revoked"
        and ProtectedNextcloudReaderGrantPlan.from_ro_mount_row(current) == plan
    )
