"""``srw.generic-file/v1`` and ``srw.kubeconfig/v1``: credential files.

The connector stores ``credentials.files[]``; validation applies the type's
defaults and path rules (``orchestrator.security.credential_files``) on every
write.  The agent writes the files to its own pod for worker jobs only, so
there is no workspace to probe from here.  ``ssh_key`` shares the file rules
but stays on the legacy path until the ssh-agent rework (slice C1) lands.
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
    CheckContext,
    ConnectorDraft,
    DatasourceDriver,
    NormalizedConnector,
    ValidationContext,
)
from shared.connectors.builtin import GENERIC_FILE_SPEC, KUBECONFIG_SPEC
from shared.connectors.envelope import unsupported_check


class CredentialFileDriver(DatasourceDriver):
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
            config = self.no_config(draft, existing)
            credentials = self._normalize_files(
                draft.name or "", self.stored_credentials(draft, existing)
            )
            return NormalizedConnector(draft.connection_url, config, credentials)
        credentials = self.stored_credentials(draft, existing)
        if credentials is not None:
            # A rename moves the default target paths with the new name.
            credentials = self._normalize_files(
                draft.name or existing.get("name", ""), credentials
            )
        return NormalizedConnector(
            draft.connection_url, self.no_config(draft, existing), credentials
        )

    async def check(
        self, row: Mapping[str, Any], credentials: dict[str, Any], *, ctx: CheckContext
    ) -> dict[str, Any]:
        # Nothing to connect to: the file is the credential, and its path and
        # size were checked when it was saved.
        return unsupported_check(
            f"{self.spec.title} connectors have no connection test; the "
            "file's path and size are checked when it is saved"
        )


def drivers() -> tuple[CredentialFileDriver, ...]:
    return (
        CredentialFileDriver(KUBECONFIG_SPEC),
        CredentialFileDriver(GENERIC_FILE_SPEC),
    )
