"""The legacy path: ``repository`` and ``ssh_key``.

Slice C1 reworks how these two deliver SSH keys (an ``ssh-agent`` in the
workspace instead of key files) and with it their validation, Test and
payload; they get drivers of their own after it lands.  Until then
``LegacyDatasourceDriver`` answers the driver contract for them with the code
they had before drivers existed: the same helpers
(``normalize_repository_config``, ``normalize_credential_files``,
``services.datasources.test_repository_datasource``) in the same order, and
the payload lines from the old payload builder.
"""

from __future__ import annotations

import json
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
    DatasourceDriver,
    NormalizedConnector,
    ValidationContext,
    payload_entry,
)
from orchestrator.services.datasource_config import normalize_repository_config
from orchestrator.services.workspace_ssh_connector import (
    WorkspaceSshConnectorError,
    probe_workspace_ssh_connector,
    repository_uses_ssh_key,
    validate_workspace_ssh_connector,
    workspace_ssh_descriptor,
)


class LegacyDatasourceDriver(DatasourceDriver):
    """``repository`` or ``ssh_key``, behind the driver contract."""

    def _normalize_files(
        self, name: str, credentials: dict[str, Any] | None
    ) -> dict[str, Any] | None:
        # A no-op for repository; ssh_key applies its file defaults.
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
        is_repository = self.type_id == "repository"
        if existing is None:
            if is_repository:
                config = normalize_repository_config(draft.config, draft.connection_url)
            else:
                # Host, user, port and pinned host keys; validated with the key.
                config = dict(draft.config or {})
            credentials = self._normalize_files(
                draft.name or "", self.stored_credentials(draft, existing)
            )
            try:
                config = validate_workspace_ssh_connector(
                    self.type_id,
                    connection_url=draft.connection_url,
                    config=config,
                    credentials=credentials,
                )
            except WorkspaceSshConnectorError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            return NormalizedConnector(draft.connection_url, config, credentials)

        credentials = self.stored_credentials(draft, existing)
        if credentials is not None and not is_repository:
            credentials = self._normalize_files(
                draft.name or existing.get("name", ""), credentials
            )
        config = draft.config
        if is_repository and config is not None:
            config = normalize_repository_config(
                config, draft.connection_url or existing.get("connection_url")
            )
        if (
            config is not None
            or credentials is not None
            or "connection_url" in draft.supplied
        ):
            config = self._validate_effective_endpoint(
                draft, existing, config=config, credentials=credentials
            )
        return NormalizedConnector(draft.connection_url, config, credentials)

    def _validate_effective_endpoint(
        self,
        draft: ConnectorDraft,
        existing: Mapping[str, Any],
        *,
        config: dict[str, Any] | None,
        credentials: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        """Validate the EFFECTIVE endpoint of an update.

        A URL-only edit of an SSH-key repository moves the host its stored
        key and pins are used for. A preserved (None) key was checked when it
        was stored. Returns the config to store (``None`` keeps it).
        """
        effective_config = config
        if effective_config is None:
            effective_config = existing.get("config") or {}
            if isinstance(effective_config, str):
                try:
                    effective_config = json.loads(effective_config)
                except ValueError:
                    effective_config = {}
        effective_credentials = (
            credentials
            if credentials is not None
            else (existing.get("credentials") or {})
        )
        if (
            config is None
            and self.type_id == "repository"
            and not repository_uses_ssh_key(effective_credentials)
        ):
            # A switch to token auth leaves a stored pin unread, not invalid.
            effective_config = {
                key: value
                for key, value in effective_config.items()
                if key != "known_hosts"
            }
        try:
            checked_config = validate_workspace_ssh_connector(
                self.type_id,
                connection_url=draft.connection_url or existing.get("connection_url"),
                config=effective_config,
                credentials=effective_credentials,
                check_key=credentials is not None,
            )
        except WorkspaceSshConnectorError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return checked_config if config is not None else None

    async def check(
        self, row: Mapping[str, Any], credentials: dict[str, Any], *, ctx: CheckContext
    ) -> dict[str, Any]:
        if self.type_id == "repository":
            from orchestrator.services.datasources import test_repository_datasource

            return await test_repository_datasource(
                dict(row), row["connection_url"], credentials
            )
        # Only a declared host has an endpoint to test.
        probed = await probe_workspace_ssh_connector(row)
        if probed is not None:
            return probed
        return {"status": "error", "message": f"Unknown connector type: {self.type_id}"}

    def bind(
        self, row: Mapping[str, Any], credentials: Any, *, ctx: BindContext
    ) -> dict[str, Any] | None:
        read_only = row.get("project_read_only", False)
        # SSH keys never ride ``datasources``: that list becomes job metadata
        # and graph state. The key travels once, in the hidden
        # ``workspace_ssh_identities`` field, into a workspace ssh-agent; the
        # entry keeps only the non-secret alias it is reached through.
        ssh_identity = workspace_ssh_descriptor(row)
        if self.type_id != "repository":
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
                read_only=read_only,
            )
            if ssh_identity is not None:
                entry["ssh_identity"] = ssh_identity
            return entry
        if ssh_identity is not None:
            credentials = {
                key: value for key, value in credentials.items() if key != "ssh_key"
            }
        fields: dict[str, Any] = {}
        # Repository identity is server-owned runtime authority.  Keep the
        # raw database ``id`` out of the payload, but carry its exact value
        # under the dedicated internal key consumed by the clone/tool
        # binding.  ``resolved_ds`` comes from the authorization query;
        # callers and models never select this field.
        datasource_id = row.get("id")
        if datasource_id is not None:
            fields["datasource_id"] = str(datasource_id)
        # The clone reads config["forge"] to resolve the forge API base;
        # without it every repository records forge="" and repo_open_pr
        # can never be used. _datasource_row_to_dict already parsed the
        # JSONB, so this is a real dict. No secrets live in config —
        # credentials travel in `creds`.
        fields["config"] = row.get("config") or {}
        entry = payload_entry(
            row, credentials=credentials, read_only=read_only, fields=fields
        )
        if row.get("require_default_branch") is True:
            entry["require_default_branch"] = True
        if ssh_identity is not None:
            entry["ssh_identity"] = ssh_identity
        return entry
