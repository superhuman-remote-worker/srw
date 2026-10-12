"""Credential-free identity for an attested workspace read denied before setup.

This identifies a cleanup obligation. It never asserts workspace process zero.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from uuid import UUID


CONTRACT_HEADER = "X-SRW-Pre-Setup-Workspace-Identity"
THREAD_HEADER = "X-SRW-Thread-ID"
SESSION_HEADER = "X-SRW-Session-Runtime-Generation"
WORKSPACE_HEADER = "X-SRW-Workspace-Generation"
INCARNATION_HEADER = "X-SRW-Workspace-Runtime-Incarnation"


@dataclass(frozen=True, slots=True)
class PreSetupWorkspaceIdentity:
    thread_id: str
    session_runtime_generation: str
    workspace_generation: str
    workspace_runtime_incarnation: str

    def __post_init__(self) -> None:
        for value in (
            self.thread_id,
            self.session_runtime_generation,
            self.workspace_generation,
            self.workspace_runtime_incarnation,
        ):
            if type(value) is not str or str(UUID(value)) != value:
                raise ValueError("Cleanup identity requires canonical UUIDs")

    def headers(self) -> dict[str, str]:
        return {
            CONTRACT_HEADER: "1",
            THREAD_HEADER: self.thread_id,
            SESSION_HEADER: self.session_runtime_generation,
            WORKSPACE_HEADER: self.workspace_generation,
            INCARNATION_HEADER: self.workspace_runtime_incarnation,
        }

    @classmethod
    def from_headers(
        cls,
        headers: Mapping[str, str],
        *,
        expected_thread_id: str,
        expected_session_generation: str | None,
    ) -> "PreSetupWorkspaceIdentity | None":
        if headers.get(CONTRACT_HEADER) != "1":
            return None
        try:
            identity = cls(
                headers.get(THREAD_HEADER),
                headers.get(SESSION_HEADER),
                headers.get(WORKSPACE_HEADER),
                headers.get(INCARNATION_HEADER),
            )
        except (TypeError, ValueError, AttributeError):
            return None
        if (
            identity.thread_id != expected_thread_id
            or identity.session_runtime_generation != expected_session_generation
        ):
            return None
        return identity
