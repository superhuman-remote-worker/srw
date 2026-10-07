"""``srw.generic/v1`` and ``srw.credentials/v1``: environment-file delivery.

Both put ``credentials.env_vars`` into an environment file in the workspace
(the agent installs it; ``shared.credential_connectors`` collects the set).
``generic`` stores whatever it is given and checks names only when the agent
installs them; ``credentials`` requires and checks its variables on every
write, merges an edit into the stored set, and is never published.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fastapi import HTTPException

from orchestrator.services.connector_drivers.base import (
    CheckContext,
    ConnectorDraft,
    DatasourceDriver,
    NormalizedConnector,
    ValidationContext,
)
from shared.connectors.builtin import CREDENTIALS_SPEC, GENERIC_SPEC
from shared.credential_connectors import normalize_credential_env


class GenericDriver(DatasourceDriver):
    def __init__(self) -> None:
        super().__init__(GENERIC_SPEC)

    async def check(
        self, row: Mapping[str, Any], credentials: dict[str, Any], *, ctx: CheckContext
    ) -> dict[str, Any]:
        return {
            "status": "ok",
            "message": "No connectivity test for generic connectors",
        }


class CredentialsDriver(DatasourceDriver):
    def __init__(self) -> None:
        super().__init__(CREDENTIALS_SPEC)

    @staticmethod
    def _refuse_publish(published: bool) -> None:
        if published:
            raise HTTPException(
                status_code=400, detail="Credential connectors cannot be published"
            )

    async def validate(
        self,
        draft: ConnectorDraft,
        *,
        existing: Mapping[str, Any] | None,
        ctx: ValidationContext,
    ) -> NormalizedConnector:
        if existing is None:
            config = self.no_config(draft, existing)
            credentials = self.stored_credentials(draft, existing)
            self._refuse_publish(bool(draft.is_global))
            try:
                credentials = {
                    "env_vars": normalize_credential_env(
                        (credentials or {}).get("env_vars", {}), required=True
                    )
                }
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            return NormalizedConnector(draft.connection_url, config, credentials)

        credentials = self.stored_credentials(draft, existing)
        self._refuse_publish(draft.is_global is True)
        if credentials is not None:
            # An edit names only the variables it changes; the rest stay.
            try:
                values = normalize_credential_env(
                    credentials.get("env_vars", {}), required=True
                )
                previous = (existing.get("credentials") or {}).get("env_vars", {})
                credentials = {
                    "env_vars": normalize_credential_env(
                        {**previous, **values}, required=True
                    )
                }
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
        return NormalizedConnector(
            draft.connection_url, self.no_config(draft, existing), credentials
        )

    async def check(
        self, row: Mapping[str, Any], credentials: dict[str, Any], *, ctx: CheckContext
    ) -> dict[str, Any]:
        # A stored set that no longer validates is a server fault (500), as
        # it always was: it cannot come from the API.
        normalize_credential_env(credentials.get("env_vars", {}), required=True)
        return {
            "status": "ok",
            "message": "Credential variables are valid; provider access is tested in the workspace",
        }
