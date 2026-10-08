"""``env_file``: environment connectors, written under ``~/.srw-credentials/``.

The variables go to the workspace over its own transport and are sourced
for every command there; they never enter the agent process. The file
belongs to the physical workspace, so a backend swap installs it again on
the new host before the old one retires.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from agent.connectors.base import (
    Delivery,
    FactsLines,
    RuntimeContext,
    declared_read_only_note,
)
from shared.credential_connectors import normalize_credential_env


def _required(delivery: Delivery) -> bool:
    """Whether the driver requires at least one variable (``credentials``)."""
    return delivery.spec is not None and any(
        slot.required and slot.delivery == "env"
        for slot in delivery.spec.credential_slots
    )


def credential_environment(deliveries: Sequence[Delivery]) -> dict[str, str]:
    """One unambiguous environment for the attached connectors.

    Raises ``ValueError`` (with no secret in the message) for an invalid or
    reserved name, a required connector without variables, or a name two
    connectors define.
    """
    result: dict[str, str] = {}
    for delivery in deliveries:
        values = normalize_credential_env(
            {value["name"]: value["value"] for value in delivery.values("env_file")},
            required=_required(delivery),
        )
        for name, secret in values.items():
            if name in result:
                raise ValueError(f"Multiple attached connectors define {name}")
            result[name] = secret
    return result


class EnvFileMaterializer:
    form = "env_file"

    def materialize(self, deliveries: Sequence[Delivery], rt: RuntimeContext) -> None:
        values = credential_environment(deliveries)
        if not values:
            return
        workspace = rt.workspace_manager
        if workspace is None or not workspace.backend.supports_shell:
            raise ValueError("Credential connectors require a sandbox or VM workspace")
        workspace.backend.install_credential_environment(values)

    def replace(
        self,
        old: Sequence[Delivery],
        new: Sequence[Delivery],
        rt: RuntimeContext,
    ) -> None:
        # The file is rewritten whole: a detached connector's variables go
        # with it.
        self.materialize(new, rt)

    def on_backend_swap(self, deliveries: Sequence[Delivery], backend: Any) -> None:
        values = credential_environment(deliveries)
        if values:
            backend.install_credential_environment(values)

    def facts(
        self, deliveries: Sequence[Delivery], rt: RuntimeContext
    ) -> list[FactsLines]:
        out: list[FactsLines] = []
        for delivery in deliveries:
            ds = delivery.entry
            cli = ds.get("cli_hint") or "CLI via env vars"
            lines = [
                f"- **{ds.get('name', 'Unnamed')}** ({ds.get('type', 'unknown')}) "
                f"— {cli}{declared_read_only_note(ds)}"
            ]
            variables = (ds.get("credentials") or {}).get("env_vars", {})
            if variables:
                lines.append(
                    "  Environment: " + ", ".join(f"`{key}`" for key in variables)
                )
                lines.append(
                    "  Read values with os.environ in workspace scripts. For login forms, "
                    'use browser_type(ref=..., env_var="VARIABLE_NAME"). '
                    "Avoid printing credentials or writing literal values into scripts."
                )
            out.append(FactsLines("Other", delivery.index, lines))
        return out
