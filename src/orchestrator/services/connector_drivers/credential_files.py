"""``srw.generic-file/v1`` and ``srw.kubeconfig/v1``: credential files.

The connector stores ``credentials.files[]``; validation applies the type's
defaults and path rules (``orchestrator.security.credential_files``) on every
write.  The agent writes the files into the workspace home of each job or
session it is attached to, so there is no connection to probe from here.
Test reports what a connector saved before the credential-file allowlist
would no longer deliver (``shared.connectors.file_targets``).
"""

from __future__ import annotations

import secrets
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
from shared.connectors.envelope import (
    DriverError,
    DriverOutcome,
    api_check_result,
    unsupported_check,
)
from shared.connectors.file_targets import (
    allowed_targets_text,
    mode_problem,
    target_problem,
)


def undeliverable_files(credentials: Mapping[str, Any] | None) -> list[str]:
    """Why each stored file would not be delivered as saved (empty: none).

    A row saved before the allowlist keeps its target; the agent skips it
    and an execute bit is dropped. Paths and modes only, never contents.
    """
    files = (
        (credentials or {}).get("files") if isinstance(credentials, Mapping) else None
    )
    problems: list[str] = []
    for item in files if isinstance(files, list) else []:
        if not isinstance(item, Mapping):
            continue
        path = str(item.get("target_path") or "")
        _relative, refused = target_problem(path)
        if refused is not None:
            problems.append(f"{path or '(no target)'} is {refused}")
        try:
            mode = int(str(item.get("mode") or "0600"), 8)
        except ValueError:
            continue
        if mode_problem(mode) is not None:
            problems.append(f"{path} has mode {mode:04o}: {mode_problem(mode)}")
    return problems


def new_directory_token() -> str:
    """A new connector's default-directory token (no id exists yet)."""
    return secrets.token_hex(4)


def directory_token(existing: Mapping[str, Any] | None) -> str:
    """The token a connector's default directory carries: its id's first
    eight hex digits once it has one, a fresh token while it is created."""
    if existing is not None and existing.get("id"):
        return str(existing["id"]).replace("-", "")[:8]
    return new_directory_token()


class CredentialFileDriver(DatasourceDriver):
    def _normalize_files(
        self,
        name: str,
        credentials: dict[str, Any] | None,
        existing: Mapping[str, Any] | None,
    ) -> dict[str, Any] | None:
        try:
            return normalize_credential_files(
                self.type_id,
                name,
                credentials,
                directory_token=directory_token(existing),
            )
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
                draft.name or "", self.stored_credentials(draft, existing), None
            )
            return NormalizedConnector(draft.connection_url, config, credentials)
        credentials = self.stored_credentials(draft, existing)
        if credentials is not None:
            # A rename moves the default target paths with the new name.
            credentials = self._normalize_files(
                draft.name or existing.get("name", ""), credentials, existing
            )
        return NormalizedConnector(
            draft.connection_url, self.no_config(draft, existing), credentials
        )

    async def check(
        self, row: Mapping[str, Any], credentials: dict[str, Any], *, ctx: CheckContext
    ) -> dict[str, Any]:
        # Nothing to connect to: the file is the credential. A row saved
        # before the allowlist says what the workspace will not receive.
        problems = undeliverable_files(credentials)
        if problems:
            return api_check_result(
                DriverOutcome(
                    error=DriverError(
                        "config",
                        "Not delivered as saved: "
                        + "; ".join(problems)
                        + f". Credential files go under {allowed_targets_text()}; "
                        "save the connector with a new target.",
                    )
                )
            )
        return unsupported_check(
            f"{self.spec.title} connectors have no connection test; the "
            "file's path and size are checked when it is saved"
        )


def drivers() -> tuple[CredentialFileDriver, ...]:
    return (
        CredentialFileDriver(KUBECONFIG_SPEC),
        CredentialFileDriver(GENERIC_FILE_SPEC),
    )
