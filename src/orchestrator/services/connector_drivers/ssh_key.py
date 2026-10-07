"""``srw.ssh-key/v1``: an SSH key loaded into the workspace's ssh-agent.

The connector stores its key as ``credentials.files[0]`` (an optional public
key may follow) and, optionally, the host it is for in ``config``: ``host``,
``user``, ``port`` and pinned ``known_hosts``. The key never reaches the
agent pod or the workspace disk: the payload entry keeps the file names and
paths without their contents plus the non-secret ``ssh_identity``, and the
key travels in ``workspace_ssh_identities`` (slice C1).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fastapi import HTTPException

from orchestrator.security.credential_files import (
    CredentialFileValidationError,
    normalize_credential_files,
)
from orchestrator.services.connector_drivers.base import (
    BindContext,
    CheckContext,
    ConnectorDraft,
    NormalizedConnector,
    ValidationContext,
    payload_entry,
)
from orchestrator.services.connector_drivers.workspace_ssh import WorkspaceSshDriver
from orchestrator.services.workspace_ssh_connector import (
    probe_workspace_ssh_connector,
)
from shared.connectors.builtin import SSH_KEY_SPEC
from shared.connectors.envelope import unsupported_check


class SshKeyDriver(WorkspaceSshDriver):
    def __init__(self) -> None:
        super().__init__(SSH_KEY_SPEC)

    def holds_ssh_key(self, row: Mapping[str, Any]) -> bool:
        return True

    def _normalize_files(
        self, name: str, credentials: dict[str, Any] | None
    ) -> dict[str, Any] | None:
        try:
            return normalize_credential_files(self.type_id, name, credentials)
        except CredentialFileValidationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    async def validate(
        self,
        draft: ConnectorDraft,
        *,
        existing: Mapping[str, Any] | None,
        ctx: ValidationContext,
    ) -> NormalizedConnector:
        if existing is None:
            # Host, user, port and pinned host keys; validated with the key.
            config = dict(draft.config or {})
            credentials = self._normalize_files(
                draft.name or "", self.stored_credentials(draft, existing)
            )
            config = self.validate_endpoint(
                connection_url=draft.connection_url,
                config=config,
                credentials=credentials,
            )
            return NormalizedConnector(draft.connection_url, config, credentials)

        credentials = self.stored_credentials(draft, existing)
        if credentials is not None:
            # A rename moves the default target paths with the new name.
            credentials = self._normalize_files(
                draft.name or existing.get("name", ""), credentials
            )
        config = self.validate_effective_endpoint(
            draft, existing, config=draft.config, credentials=credentials
        )
        return NormalizedConnector(draft.connection_url, config, credentials)

    async def check(
        self, row: Mapping[str, Any], credentials: dict[str, Any], *, ctx: CheckContext
    ) -> dict[str, Any]:
        # Only a declared host has an endpoint to test.
        probed = await probe_workspace_ssh_connector(row)
        if probed is not None:
            return probed
        return unsupported_check(
            f"{self.spec.title} connectors without a host have no endpoint to "
            "test; add the host to test it and pin its host key"
        )

    def bind(
        self, row: Mapping[str, Any], credentials: Any, *, ctx: BindContext
    ) -> dict[str, Any] | None:
        # SSH keys never ride ``datasources``: that list becomes job metadata
        # and graph state. The entry keeps the file names and paths, and the
        # non-secret alias the key is reached through.
        entry = payload_entry(
            row,
            credentials={
                **credentials,
                "files": [
                    {key: value for key, value in item.items() if key != "contents"}
                    for item in credentials.get("files") or []
                    if isinstance(item, dict)
                ],
            },
            read_only=row.get("project_read_only", False),
        )
        ssh_identity = self.ssh_identity_descriptor(row)
        if ssh_identity is not None:
            entry["ssh_identity"] = ssh_identity
        return entry
