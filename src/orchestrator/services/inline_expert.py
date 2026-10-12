"""Admit a caller's inline SRW Expert as the definition its work freezes.

An inline Expert is a complete authored definition with no catalogue identity.
Job creation, session creation, the tool-group preview and the manifest Job
path all turn it into the same resolver row, so ``prepare_srw_snapshot``
renders it exactly as it renders a selected Expert's projected row. The row
carries no ``id`` and no ``manifest_uid``: an inline Expert is owned by its
work and records no resource dependency.

The gates are the manifest path's: the installed-image and launch-envelope
check, the authored-fragment write gates (credentials, runtime authority, tool
vocabulary, roster shape) and the persona placeholder rule, plus the DB-expert
prompt-key allowlist. They run at admission so a bad definition is refused
before any write; the snapshot re-runs the fragment gates inside the insert.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Literal, Mapping

from fastapi import HTTPException

from orchestrator.schemas.inline_expert import InlineExpertSpec


def srw_expert_row(private: Mapping[str, Any], *, expert_type: str) -> dict[str, Any]:
    """The resolver row for an ``srw/v1`` runtime's private settings."""
    return {
        "expert_type": expert_type,
        "config": deepcopy(private.get("config", {})),
        "prompts": deepcopy(private.get("prompts", {})),
        "harness_config_layers": deepcopy(private.get("layers", [])),
        "harness_asset_name": private.get("asset_name"),
    }


def admit_inline_expert(
    spec: InlineExpertSpec,
    *,
    role: Literal["worker", "session"],
    trusted_image: str | None,
) -> dict[str, Any]:
    """Validate an inline Expert and return its resolver row.

    ``harness_config_name`` carries the authored base exactly as a selected
    Expert's projection does, so the snapshot resolves the same base whichever
    way the definition arrived. Callers persist the role base as the work's
    ``config_name``, as they do for a DB Expert selected by id.
    """
    from orchestrator.schemas.expert_catalog import (
        validate_expert_prompt_source_or_422,
    )
    from orchestrator.services.config_overrides import validated_config_name
    from orchestrator.services.manifest_execution_snapshot import (
        validate_srw_authored_fragment,
    )
    from orchestrator.services.manifest_runtime_ownership import (
        require_srw_launch_configuration,
    )
    from shared.runtime.core.loader import resolve_bundled_config_path
    from shared.runtime.core.srw_manifest_config import validate_srw_asset_name

    # Unset keys stay absent: an omitted image follows the installation, an
    # explicit one (even null) must equal it.
    runtime = spec.runtime.model_dump(exclude_unset=True)
    private = require_srw_launch_configuration(runtime, trusted_image=trusted_image)

    config_name = private.get("config_name")
    if config_name is not None:
        config_name = validated_config_name(config_name)
    asset_name = private.get("asset_name")
    if asset_name is not None:
        try:
            validate_srw_asset_name(asset_name)
            _, asset_dir = resolve_bundled_config_path(asset_name)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        if not asset_dir:
            raise HTTPException(
                422, "SRW harness asset_name does not select installed assets."
            )
    prompts = validate_expert_prompt_source_or_422(private.get("prompts") or {})
    row = srw_expert_row(
        {
            "config": validate_srw_authored_fragment(private.get("config", {})),
            "prompts": prompts or {},
            "layers": [
                validate_srw_authored_fragment(layer)
                for layer in private.get("layers", [])
            ],
            "asset_name": asset_name,
        },
        expert_type=role,
    )
    row["harness_config_name"] = config_name
    return row


__all__ = ["admit_inline_expert", "srw_expert_row"]
