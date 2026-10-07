"""What the ``ssh_key`` and SSH-key ``repository`` drivers share.

Both can hand a private key to a workspace, which loads it into an
``ssh-agent`` and reaches it through an opaque ``srw-repo-<32hex>`` alias
(slice C1). The key never rides the ``datasources`` payload: it travels once,
in the hidden ``workspace_ssh_identities`` field. The rules themselves are
``orchestrator.services.workspace_ssh_connector``, unchanged; this base calls
them at the points each driver operation needs:

* validation of the effective endpoint on create and update (host, user,
  port and pinned host keys are held to a strict grammar, an alias-shaped
  host is refused, a passphrase-protected key is refused);
* Test reaching the endpoint being edited (``apply_test_overrides``);
* the non-secret ``ssh_identity`` descriptor in the payload entry, and the
  identity itself for ``workspace_ssh_identities``.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from fastapi import HTTPException

from orchestrator.services.connector_drivers.base import (
    ConnectorDraft,
    DatasourceDriver,
)
from orchestrator.services.workspace_ssh_connector import (
    WorkspaceSshConnectorError,
    WorkspaceSshIdentity,
    apply_ssh_test_overrides,
    validate_workspace_ssh_connector,
    workspace_ssh_descriptor,
    workspace_ssh_identity,
)


def json_object(value: Any) -> dict[str, Any]:
    """A stored JSONB value as a dict (a string is parsed, junk is empty)."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return {}
    return dict(value) if isinstance(value, Mapping) else {}


class WorkspaceSshDriver(DatasourceDriver):
    """A driver whose connector may hold a key for a workspace ssh-agent."""

    def holds_ssh_key(self, row: Mapping[str, Any]) -> bool:
        """Whether this stored connector authenticates with an SSH key."""
        raise NotImplementedError

    def validate_endpoint(
        self,
        *,
        connection_url: str | None,
        config: Mapping[str, Any] | None,
        credentials: Mapping[str, Any] | None,
        check_key: bool = True,
    ) -> dict[str, Any]:
        """The normalized config to store (HTTP 400 when unsafe or unusable)."""
        try:
            return validate_workspace_ssh_connector(
                self.type_id,
                connection_url=connection_url,
                config=config,
                credentials=credentials,
                check_key=check_key,
            )
        except WorkspaceSshConnectorError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    def unread_pins_dropped(
        self, config: dict[str, Any], credentials: Mapping[str, Any]
    ) -> dict[str, Any]:
        """The stored config an edit is validated against."""
        return config

    def validate_effective_endpoint(
        self,
        draft: ConnectorDraft,
        existing: Mapping[str, Any],
        *,
        config: dict[str, Any] | None,
        credentials: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        """Validate the endpoint an update leaves the connector with.

        A URL-only edit of an SSH-key repository moves the host its stored key
        and pins are used for, so the EFFECTIVE values are checked. A
        preserved (None) key was checked when it was stored. Returns the config
        to store (``None`` keeps the stored one).
        """
        if not (
            config is not None
            or credentials is not None
            or "connection_url" in draft.supplied
        ):
            return config
        effective_credentials = (
            credentials
            if credentials is not None
            else (existing.get("credentials") or {})
        )
        effective_config = config
        if effective_config is None:
            effective_config = self.unread_pins_dropped(
                json_object(existing.get("config")), effective_credentials
            )
        checked = self.validate_endpoint(
            connection_url=draft.connection_url or existing.get("connection_url"),
            config=effective_config,
            credentials=effective_credentials,
            check_key=credentials is not None,
        )
        return checked if config is not None else None

    def apply_test_overrides(
        self, row: Mapping[str, Any], overrides: Mapping[str, Any]
    ) -> dict[str, Any]:
        """The row Test probes: the endpoint as the connector form edits it."""
        try:
            return apply_ssh_test_overrides(row, overrides)
        except WorkspaceSshConnectorError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    def ssh_identity_descriptor(self, row: Mapping[str, Any]) -> dict[str, Any] | None:
        """The non-secret ``ssh_identity`` a payload entry carries, if any."""
        return workspace_ssh_descriptor(row)

    def workspace_ssh_identity(
        self, row: Mapping[str, Any], *, default_known_hosts: str
    ) -> WorkspaceSshIdentity | None:
        """The identity to load into the workspace ssh-agent, if any.

        Raises ``WorkspaceSshConnectorError`` (with its fixed reason code)
        for a stored row that cannot be delivered.
        """
        if not self.holds_ssh_key(row):
            return None
        return workspace_ssh_identity(row, default_known_hosts=default_known_hosts)
