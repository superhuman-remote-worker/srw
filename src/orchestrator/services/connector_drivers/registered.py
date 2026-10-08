"""Connectors of registered image drivers (connector drivers D6).

Every connector of a driver someone registered is stored with one type,
``image_driver`` (``shared.connectors.builtin.IMAGE_DRIVER_SPEC``). The
registry answers that type with :class:`RegisteredDriverHost`, which knows no
registration: it binds the wire entry every such connector shares (no
credential: the bind-time pod's result is filled in at delivery,
``connector_bind_time``) and hands out a :class:`RegisteredImageDriver` for
one registration (:class:`SupportsDriverRegistration`). That driver is the
registration's spec at work: it validates config against the spec's
``config_schema``, credentials against its slots, names the spec's driver
on the connector's manifest resource, and answers Test with a ``check`` pod
when the installation runs driver pods.

Callers find the registration through
``orchestrator.services.connector_driver_registrations`` (the assignment
row), never by comparing a type: a driver that supports registrations says
so by capability.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import replace
from typing import Any, Protocol, runtime_checkable

from fastapi import HTTPException

from orchestrator.services.connector_drivers.base import (
    BindContext,
    CheckContext,
    ConnectorDraft,
    DatasourceDriver,
    NormalizedConnector,
    ValidationContext,
    payload_entry,
)
from shared.connectors.builtin import IMAGE_DRIVER_SPEC
from shared.connectors.contract import DriverSpec, effective_access
from shared.connectors.envelope import unsupported_check
from shared.connectors.registration import schema_problems

#: The longest value a registered driver's credential may hold.
_MAX_CREDENTIAL = 64 * 1024
#: Runs one ``check`` of a registered driver in its own pod:
#: ``(registration, row, credentials) -> Test connection answer``.
CheckRunner = Callable[[Any, Mapping[str, Any], dict[str, Any]], Awaitable[dict]]


def _slot_keys(slot: Any) -> frozenset[str]:
    """The credential keys a slot owns: its schema's properties."""
    schema = slot.schema if isinstance(slot.schema, Mapping) else {}
    properties = schema.get("properties")
    return frozenset(properties) if isinstance(properties, Mapping) else frozenset()


@runtime_checkable
class SupportsDriverRegistration(Protocol):
    """A stored type whose connectors each run a registered image driver."""

    def for_registration(
        self, registration: Any, *, check_runner: CheckRunner | None = None
    ) -> DatasourceDriver: ...


class RegisteredDriverHost(DatasourceDriver):
    """The stored type of every registered driver's connector."""

    def __init__(self) -> None:
        super().__init__(IMAGE_DRIVER_SPEC)

    def for_registration(
        self, registration: Any, *, check_runner: CheckRunner | None = None
    ) -> RegisteredImageDriver:
        return RegisteredImageDriver(registration, check_runner=check_runner)

    async def validate(
        self,
        draft: ConnectorDraft,
        *,
        existing: Mapping[str, Any] | None,
        ctx: ValidationContext,
    ) -> NormalizedConnector:
        if existing is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    "A registered driver's connector names its driver: "
                    "driver_registration_id or driver"
                ),
            )
        # Its registration is gone (a user or project delete): nothing can
        # check new config or credentials, but naming and sharing still work.
        if draft.config or draft.credentials or draft.connection_url:
            raise HTTPException(
                status_code=409,
                detail=(
                    "This connector's driver registration is gone; only its "
                    "name, description and sharing can change"
                ),
            )
        return NormalizedConnector(connection_url=None, config=None, credentials=None)

    async def check(
        self, row: Mapping[str, Any], credentials: dict[str, Any], *, ctx: CheckContext
    ) -> dict[str, Any]:
        return unsupported_check(
            "This connector's driver registration is gone; it cannot be tested"
        )

    def effective_access(self, row: Mapping[str, Any]) -> str | None:
        return effective_access(row, self.spec)

    def bind(
        self, row: Mapping[str, Any], credentials: Any, *, ctx: BindContext
    ) -> dict[str, Any] | None:
        # Never the stored credentials: the driver's bind-time pod decides
        # what reaches the workspace, and its result replaces ``credentials``
        # at delivery (``connector_bind_time.deliver_bind_time_entries``).
        return payload_entry(
            row,
            credentials={},
            read_only=row.get("project_read_only", False),
            connection_url=None,
            fields={"datasource_id": str(row["id"])},
        )

    def credential_config(self, credentials: Mapping[str, Any]) -> dict[str, Any]:
        return {}


class RegisteredImageDriver(RegisteredDriverHost):
    """One registration's spec, serving its connectors' writes and Test."""

    def __init__(
        self, registration: Any, *, check_runner: CheckRunner | None = None
    ) -> None:
        DatasourceDriver.__init__(
            self,
            replace(registration.spec, legacy_type=IMAGE_DRIVER_SPEC.legacy_type),
        )
        self.registration = registration
        #: The image the registration names (a tag follows, a digest pins).
        self.image_reference = registration.image_reference
        self._check_runner = check_runner

    @property
    def registered_spec(self) -> DriverSpec:
        return self.registration.spec

    def resource_driver(self, credentials: Mapping[str, Any]) -> str:
        return self.registration.spec.name

    # -- config and credentials ------------------------------------------

    def _refuse_invalid(self, schema: Mapping[str, Any], value: Any, what: str) -> None:
        from jsonschema import Draft202012Validator
        from jsonschema.exceptions import SchemaError

        # Registration refused these; a stored schema is never run unread.
        if schema_problems(schema, what):
            raise HTTPException(
                status_code=400,
                detail=f"The driver {self.spec.name} declares a {what} schema "
                "SRW does not run",
            )
        try:
            Draft202012Validator.check_schema(schema)
        except SchemaError:
            raise HTTPException(
                status_code=400,
                detail=f"The driver {self.spec.name} declares an invalid {what} schema",
            ) from None
        errors = sorted(
            Draft202012Validator(schema).iter_errors(value),
            key=lambda error: list(error.path),
        )
        if errors:
            error = errors[0]
            where = "/".join(str(part) for part in error.path)
            # The message names the rule, never the value (it may be secret).
            raise HTTPException(
                status_code=400,
                detail=f"{self.spec.title} {what}{f' {where}' if where else ''}: "
                f"{error.validator} {error.validator_value!r} not satisfied",
            )

    def _checked_credentials(
        self, credentials: Mapping[str, Any], *, creating: bool
    ) -> None:
        owned = {key for slot in self.spec.credential_slots for key in _slot_keys(slot)}
        unknown = sorted(set(credentials) - owned)
        if unknown:
            raise HTTPException(
                status_code=400,
                detail=f"{self.spec.title} has no credential slot for {unknown}",
            )
        for key, value in credentials.items():
            if isinstance(value, str) and len(value) > _MAX_CREDENTIAL:
                raise HTTPException(
                    status_code=400, detail=f"The credential {key} is too long"
                )
        for slot in self.spec.credential_slots:
            keys = _slot_keys(slot)
            part = {key: value for key, value in credentials.items() if key in keys}
            if not part:
                if slot.required and creating:
                    raise HTTPException(
                        status_code=400,
                        detail=f"{self.spec.title} needs its {slot.name} credential",
                    )
                continue
            self._refuse_invalid(slot.schema, part, f"credential {slot.name}")

    async def validate(
        self,
        draft: ConnectorDraft,
        *,
        existing: Mapping[str, Any] | None,
        ctx: ValidationContext,
    ) -> NormalizedConnector:
        if draft.connection_url:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"A {self.spec.title} connector has no connection URL; its "
                    "config names what it connects to"
                ),
            )
        credentials = self.stored_credentials(draft, existing)
        if credentials is not None:
            self._checked_credentials(credentials, creating=existing is None)
        elif existing is None:
            self._checked_credentials({}, creating=True)
        config = draft.config
        if config is None and existing is None:
            config = {}
        if config is not None:
            if not isinstance(config, Mapping):
                raise HTTPException(status_code=400, detail="config must be an object")
            config = dict(config)
            self._refuse_invalid(self.spec.config_schema, config, "config")
        return NormalizedConnector(
            connection_url=None, config=config, credentials=credentials
        )

    async def check(
        self, row: Mapping[str, Any], credentials: dict[str, Any], *, ctx: CheckContext
    ) -> dict[str, Any]:
        if self._check_runner is None:
            return unsupported_check(
                f"{self.spec.title} is tested in a driver pod, and this "
                "installation runs none (connectors.servicePods.enabled)"
            )
        return await self._check_runner(self.registration, row, credentials)


__all__ = [
    "CheckRunner",
    "RegisteredDriverHost",
    "RegisteredImageDriver",
    "SupportsDriverRegistration",
]
