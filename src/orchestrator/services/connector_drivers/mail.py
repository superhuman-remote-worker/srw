"""``srw.email/v1``: an IMAP/SMTP mailbox the agent process connects to.

The access level is a tier in ``config.access`` (read, read_write, draft,
send), clamped to ``read`` by a read-only project link; the credentials stay
in the payload at every tier because every tier needs an IMAP login.  A
mailbox is never published, and one execution gets one mailbox
(``max_per_execution``): the agent keys connections by type.

Validation and the probe are ``orchestrator.services.email_datasource``;
spec: knowledge-base/knowledge/features/email_datasource.md.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any

from fastapi import HTTPException

from orchestrator.services.connector_drivers.base import (
    BindContext,
    CheckContext,
    ConnectorDraft,
    DatasourceDriver,
    NormalizedConnector,
    ValidationContext,
    payload_entry,
    probe_failure,
)
from orchestrator.services.email_datasource import (
    email_dispatch_config,
    probe_email_connection,
    validate_email_config,
    validate_email_credentials,
)
from shared.connectors.builtin import EMAIL_SPEC
from shared.datasource_policy import email_effective_access


class EmailDriver(DatasourceDriver):
    def __init__(self) -> None:
        super().__init__(EMAIL_SPEC)

    @staticmethod
    def _refuse_publish(published: bool) -> None:
        # A public mailbox would hand the owner's IMAP/SMTP credentials to
        # every user's agents, so no capability can allow it.
        if published:
            raise HTTPException(
                status_code=400,
                detail="Email connectors cannot be published (is_global)",
            )

    @staticmethod
    async def _config(
        config: dict[str, Any] | None, ctx: ValidationContext
    ) -> dict[str, Any]:
        # The grant is only consulted when unattended_send is requested, so
        # the common draft-tier write skips the grant-resolution round-trip.
        wants_unattended = bool((config or {}).get("unattended_send"))
        owner_has_send_grant = (
            await ctx.can_autonomous_send() if wants_unattended else False
        )
        try:
            return validate_email_config(config, owner_has_send_grant)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @staticmethod
    def _credentials(credentials: Any, *, access: str) -> dict[str, Any]:
        # Shape check runs BEFORE encryption at rest (the store encrypts
        # transparently); the smtp block is required only for access='send'.
        try:
            return validate_email_credentials(credentials, access=access)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    async def validate(
        self,
        draft: ConnectorDraft,
        *,
        existing: Mapping[str, Any] | None,
        ctx: ValidationContext,
    ) -> NormalizedConnector:
        if existing is None:
            self._refuse_publish(bool(draft.is_global))
            config = await self._config(draft.config, ctx)
            credentials = self._credentials(
                self.stored_credentials(draft, existing), access=config["access"]
            )
            return NormalizedConnector(draft.connection_url, config, credentials)

        self._refuse_publish(draft.is_global is True)
        credentials = self.stored_credentials(draft, existing)
        config = draft.config
        if config is not None:
            config = await self._config(config, ctx)
        # Check the EFFECTIVE credential shape against the EFFECTIVE tier
        # (flipping access to 'send' without a stored smtp block must 400
        # here, not fail at first use). Kept credentials are checked but not
        # rewritten.
        effective_config = (
            config if config is not None else (existing.get("config") or {})
        )
        checked = self._credentials(
            credentials
            if credentials is not None
            else (existing.get("credentials") or {}),
            access=effective_config.get("access", "draft"),
        )
        return NormalizedConnector(
            draft.connection_url,
            config,
            checked if credentials is not None else None,
        )

    async def check(
        self, row: Mapping[str, Any], credentials: dict[str, Any], *, ctx: CheckContext
    ) -> dict[str, Any]:
        # The blocking imaplib/smtplib probe runs off the event loop.
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(
                    probe_email_connection, credentials, row.get("config") or {}
                ),
                timeout=10,
            )
        except asyncio.TimeoutError:
            return {
                "status": "error",
                "message": "IMAP/SMTP connectivity test timed out after 10s",
            }
        except Exception:
            return probe_failure("IMAP/SMTP connection failed", self.type_id)

    def effective_access(self, row: Mapping[str, Any]) -> str | None:
        return email_effective_access(dict(row))

    def bind(
        self, row: Mapping[str, Any], credentials: Any, *, ctx: BindContext
    ) -> dict[str, Any] | None:
        read_only = row.get("project_read_only", False)
        return payload_entry(
            row,
            credentials=credentials,
            read_only=read_only,
            fields={
                "config": email_dispatch_config(
                    row.get("config"),
                    project_read_only=bool(read_only),
                    owner_can_autonomous_send=bool(
                        row.get("_owner_can_autonomous_send", False)
                    ),
                )
            },
        )
