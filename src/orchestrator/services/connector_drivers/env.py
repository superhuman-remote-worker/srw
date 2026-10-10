"""``srw.generic/v1`` and ``srw.credentials/v1``: environment-file delivery.

Both put ``credentials.env_vars`` into an environment file in the workspace
(the agent installs it; ``shared.credential_connectors`` collects the set).
Every name follows the one rule all connectors do
(``shared.connectors.env_names``: no SRW reserved name, no known code hook),
checked whenever the variables are written; ``generic`` keeps any other
credential field it is given. ``credentials`` requires its variables, merges
an edit into the stored set, and is never published. A row saved before the
rule is delivered without a refused name, and its Test says which.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fastapi import HTTPException

from orchestrator.services.connector_drivers import knowledge_note
from orchestrator.services.connector_drivers.base import (
    CheckContext,
    ConnectorDraft,
    DatasourceDriver,
    NormalizedConnector,
    SecretLeaf,
    ValidationContext,
    string_leaves,
    top_level_leaves,
)
from shared.connectors.builtin import CREDENTIALS_SPEC, GENERIC_SPEC
from shared.connectors.env_names import connector_env_problem
from shared.connectors.envelope import DriverError, DriverOutcome, api_check_result
from shared.credential_connectors import normalize_credential_env, split_credential_env

#: How a ``credentials`` connector drops a stored variable: an edit keeps
#: every one it does not name.
RECREATE = "Create the connector again without it: an edit keeps every stored variable"


def _refused_result(variables: Any, fix: str) -> dict[str, Any] | None:
    """A Test's result for stored names no connector may set (a row saved
    before the rule, delivered without them); ``None`` when there are none."""
    if not isinstance(variables, Mapping):
        return None
    refused = [why for name in variables if (why := connector_env_problem(name))]
    if not refused:
        return None
    return api_check_result(
        DriverOutcome(
            error=DriverError(
                "config", "Not delivered as saved: " + "; ".join(refused) + f". {fix}."
            )
        )
    )


class EnvironmentDriver(DatasourceDriver):
    """What both environment drivers say about themselves."""

    def knowledge_note(self, row: Mapping[str, Any]) -> str:
        return knowledge_note.environment_note(row)

    def retrieval_messages(self, row: Mapping[str, Any]) -> list[str]:
        return knowledge_note.environment_phrases(row)

    def secret_leaves(self, credentials: Mapping[str, Any]) -> list[SecretLeaf]:
        """Each variable as ``env.<NAME>``; any other top-level string a
        ``generic`` row stores under its own name.  A ``generic`` row saved
        before its names were checked may hold a name that is not an
        environment name: it stays in the ``shape``."""
        return string_leaves(
            credentials.get("env_vars"),
            ("env_vars",),
            lambda name: f"env.{name}",
            named=True,
        ) + top_level_leaves(credentials)


class GenericDriver(EnvironmentDriver):
    def __init__(self) -> None:
        super().__init__(GENERIC_SPEC)

    async def validate(
        self,
        draft: ConnectorDraft,
        *,
        existing: Mapping[str, Any] | None,
        ctx: ValidationContext,
    ) -> NormalizedConnector:
        normalized = await super().validate(draft, existing=existing, ctx=ctx)
        credentials = normalized.credentials or {}
        if "env_vars" in credentials:
            try:
                normalize_credential_env(credentials["env_vars"])
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
        return normalized

    async def check(
        self, row: Mapping[str, Any], credentials: dict[str, Any], *, ctx: CheckContext
    ) -> dict[str, Any]:
        fix = "Save its variables again without it"
        return _refused_result(credentials.get("env_vars"), fix) or {
            "status": "ok",
            "message": "No connectivity test for generic connectors",
        }


class CredentialsDriver(EnvironmentDriver):
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
                _kept, refused = split_credential_env(previous)
                if refused:
                    # Saved before the rule: the edit would store it again.
                    raise ValueError(
                        "; ".join(refused.values())
                        + f". It was saved before this rule. {RECREATE}."
                    )
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
        # A stored set of the wrong shape is a server fault (500), as it
        # always was: it cannot come from the API. A name refused since it
        # was saved is the connector's to fix.
        split_credential_env(credentials.get("env_vars", {}), required=True)
        return _refused_result(credentials.get("env_vars"), RECREATE) or {
            "status": "ok",
            "message": "Credential variables are valid; provider access is tested in the workspace",
        }
