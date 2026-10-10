"""``env_file``: environment connectors, written under ``~/.srw-credentials/``.

The variables go to the workspace over its own transport and are sourced
for every command there; they never enter the agent process. The file
belongs to the physical workspace, so a backend swap installs it again on
the new host before the old one retires.

A name no connector may set (``shared.connectors.env_names``: an SRW
reserved name or a known code hook) is refused when a connector is saved. A
row saved before that rule is delivered without it: the variable is
skipped, logged and named in the README with the reason, as a credential
file outside the allowlist is (``agent.connectors.files``). A value an
earlier delivery installed stays in the work item's environment until the
work ends: installs merge, and SRW never unsets a name it did not set
(``shared.runtime.core.credential_env``).
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from typing import Any

from agent.connectors.base import (
    Delivery,
    FactsLines,
    RuntimeContext,
    read_only_note,
)
from agent.connectors.legacy import env_vars_unreadable
from shared.connectors.builtin import IMAGE_DRIVER_SPEC
from shared.connectors.env_names import connector_env_problem
from shared.credential_connectors import split_credential_env

logger = logging.getLogger(__name__)


def _required(delivery: Delivery) -> bool:
    """Whether the driver requires at least one variable (``credentials``)."""
    return delivery.spec is not None and any(
        slot.required and slot.delivery == "env"
        for slot in delivery.spec.credential_slots
    )


def credential_environment(deliveries: Sequence[Delivery]) -> dict[str, str]:
    """One unambiguous environment for the attached connectors.

    A name no connector may set is skipped with a warning (the README says
    why: :meth:`EnvFileMaterializer.facts`). Raises ``ValueError`` (with no
    secret in the message) for a set that is not a name/value object, a
    value the workspace refuses, a required connector without variables, or
    a name two connectors define.
    """
    result: dict[str, str] = {}
    for delivery in deliveries:
        if env_vars_unreadable(delivery.entry):
            raise ValueError("Environment variables must be a name/value object")
        values, refused = split_credential_env(
            {value["name"]: value["value"] for value in delivery.values("env_file")},
            required=_required(delivery),
        )
        for name, why in refused.items():
            logger.warning("Skipping %s for '%s': %s", name, delivery.name, why)
        for name, secret in values.items():
            if name in result:
                raise ValueError(f"Multiple attached connectors define {name}")
            result[name] = secret
    return result


def _driver_names(deliveries: Sequence[Delivery]) -> set[str]:
    """The variables registered image drivers' bindings set."""
    return {
        str(value["name"])
        for delivery in deliveries
        if delivery.spec is IMAGE_DRIVER_SPEC
        for value in delivery.values("env_file")
    }


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
        # The current set is installed again. The workspace program merges
        # it into what earlier installs left: a detached connector's values
        # stay in the session workspace, as agreed for v1
        # (shared.runtime.core.credential_env), and a ``credentials``
        # connector cannot be detached live at all. A registered image
        # driver's variables are unset when it is detached (D6): its driver
        # revokes the credential behind them, and the names are its own.
        stale = sorted(_driver_names(old) - _driver_names(new))
        workspace = rt.workspace_manager
        if stale and workspace is not None and workspace.backend.supports_shell:
            unset = getattr(workspace.backend, "unset_credential_environment", None)
            if unset is not None:
                unset(stale)
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
                f"— {cli}{read_only_note(ds)}"
            ]
            variables = (ds.get("credentials") or {}).get("env_vars", {})
            if not isinstance(variables, Mapping):
                variables = {}
            refused = {key: connector_env_problem(key) for key in variables}
            delivered = [key for key, why in refused.items() if why is None]
            if delivered:
                lines.append(
                    "  Environment: " + ", ".join(f"`{key}`" for key in delivered)
                )
                lines.append(
                    "  Read values with os.environ in workspace scripts. For login forms, "
                    'use browser_type(ref=..., env_var="VARIABLE_NAME"). '
                    "Avoid printing credentials or writing literal values into scripts."
                )
            lines += [
                f"  {why}; not delivered by SRW (a value set by an earlier "
                "delivery stays until the work ends)"
                for why in refused.values()
                if why
            ]
            out.append(FactsLines("Other", delivery.index, lines))
        return out
