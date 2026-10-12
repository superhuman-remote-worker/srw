"""``srw.env/v1`` and ``srw.files/v1``: delivery into a generic-hosting pod.

A manifest connector of either driver names values inline, or selects one of
its declared credentials with ``{"credential": "<name>"}``.  Binding resolves
those selections and returns ``pod_env`` or ``pod_file`` entries for the
harness pod; the manifest execution service turns them into the pod's
immutable Secret.  Neither driver can enforce an access level, so a connector
that declares one is refused.

The refusals are ``HTTPException(422)`` with the details manifest admission
has always returned.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from fastapi import HTTPException

from orchestrator.services.connector_drivers.base import ManifestDeliveryDriver
from shared.connectors.binding import BindingDescriptor, BindingEntry
from shared.connectors.builtin import ENV_SPEC, FILES_SPEC

BINDINGS_ROOT = "/run/srw/bindings/"


class _InlineValuesDriver(ManifestDeliveryDriver):
    #: The one config key holding the connector's values.
    key: str
    form: str

    def validate(self, connector: Mapping[str, Any]) -> None:
        if "access" in connector:
            raise HTTPException(
                422,
                "Env/file delivery cannot enforce ReadOnly/ReadWrite; use credentials scoped by the external resource.",
            )
        config = connector.get("config", {})
        values = config.get(self.key, {})
        if not isinstance(values, dict) or set(config) - {self.key}:
            raise HTTPException(422, "Invalid env/file connector configuration.")

    def check_target(self, name: str) -> None:
        """Refuse a value's name; the env driver accepts every name."""

    async def bind(
        self,
        connector: Mapping[str, Any],
        *,
        alias: str,
        resolve_credential: Callable[[Mapping[str, Any]], Awaitable[str]],
    ) -> BindingDescriptor:
        """One entry per value, in config order; credentials resolved first."""
        values = connector.get("config", {}).get(self.key, {})
        credentials = connector.get("credentials", {})
        entries: list[BindingEntry] = []
        for name, value in values.items():
            if (
                isinstance(value, dict)
                and set(value) == {"credential"}
                and value["credential"] in credentials
            ):
                value = await resolve_credential(
                    credentials[value["credential"]]["secretRef"]
                )
            if not isinstance(value, str):
                raise HTTPException(
                    422,
                    "Connector values must be strings or declared credential selections.",
                )
            self.check_target(name)
            entries.append(
                BindingEntry(
                    recipient="harness_pod",
                    form=self.form,  # type: ignore[arg-type]
                    value=self.entry_value(name, value),
                    collision="error",
                )
            )
        return BindingDescriptor(
            driver=self.spec.name, name=alias, entries=tuple(entries)
        )

    def entry_value(self, name: str, value: str) -> dict[str, str]:
        raise NotImplementedError


class EnvDriver(_InlineValuesDriver):
    key = "env"
    form = "pod_env"

    def __init__(self) -> None:
        super().__init__(ENV_SPEC)

    def entry_value(self, name: str, value: str) -> dict[str, str]:
        return {"name": name, "value": value}


class FilesDriver(_InlineValuesDriver):
    key = "files"
    form = "pod_file"

    def __init__(self) -> None:
        super().__init__(FILES_SPEC)

    def check_target(self, name: str) -> None:
        if not name.startswith(BINDINGS_ROOT):
            raise HTTPException(
                422, "Connector files must be under /run/srw/bindings/."
            )

    def entry_value(self, name: str, value: str) -> dict[str, str]:
        return {"path": name, "content": value}
