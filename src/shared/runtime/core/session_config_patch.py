"""Reference-harness changes to an already frozen session configuration."""

from copy import deepcopy

from shared.runtime.core.delegation_settings import FAMILY_SESSION_MAX_CONCURRENT_KEY
from shared.runtime.core.loader import (
    _apply_settings_matrix,
    deep_merge,
    load_agent_config_from_dict,
    normalize_delegation_block,
    normalize_llm_tiers,
    strip_loader_owned_keys,
)
from shared.runtime.core.tool_policy import normalize_tool_policy


RESOLVED_PATCH_MARKER = "_srw_resolved_patch"


def validate_session_settings_patch(override):
    """Source replacement requires a new session's complete admission path."""
    source_keys = {
        "expert",
        "expert_id",
        "config_name",
        "asset_name",
        "$extends",
        "$ref",
        "prompts",
        "persona",
        "instructions",
        "instruction_files",
        "skills",
        "subagents",
    }
    offending = set(source_keys.intersection(override))
    extra = override.get("extra")
    if isinstance(extra, dict):
        offending.update("extra." + key for key in source_keys.intersection(extra))
    if offending:
        raise ValueError(
            "Session settings PATCH cannot replace Expert, prompt, skill or roster sources "
            f"({', '.join(sorted(offending))}). Create a new session with the selected Expert."
        )


def patch_frozen_session(blob, policy, override, *, deployment_dir=None):
    """Apply only the requested delta; retain the captured source and assets.

    Changing the model intentionally derives its model-family settings once.
    Cosmetic LLM changes preserve the frozen family settings. The returned
    delta is the exact input for a pinned runtime's merge, before credentials
    are stripped from the persistable snapshot.
    """
    validate_session_settings_patch(override)
    delta = strip_loader_owned_keys(
        normalize_delegation_block(
            normalize_llm_tiers(
                normalize_tool_policy(deepcopy(override), source="thread-override"),
                source="thread-override",
            ),
            source="thread-override",
        )
    )
    agent = deep_merge(deepcopy(blob["agent"]), delta)
    if (delta.get("llm") or {}).get("model") and (
        delta["llm"]["model"] != (blob["agent"].get("llm") or {}).get("model")
    ):
        _apply_settings_matrix(agent, set(delta["llm"]), deployment_dir)
        for section in ("llm", "limits", "shell"):
            if section == "shell" and agent.get(section) == blob["agent"].get(section):
                continue
            delta[section] = deepcopy(agent.get(section, {}))
            # A model swap can remove the old transport. Keep its explicit
            # deletion in the pinned-runtime delta as well as the snapshot.
            for key in (blob["agent"].get(section) or {}).keys() - delta[
                section
            ].keys():
                delta[section][key] = None
        # The session delegation cap has a per-family slot the matrix just
        # re-derived (or removed). Carry only that slot: the rest of the
        # delegation block is authored and grant-checked.
        before = (blob["agent"].get("delegation") or {}).get(
            FAMILY_SESSION_MAX_CONCURRENT_KEY
        )
        after = (agent.get("delegation") or {}).get(FAMILY_SESSION_MAX_CONCURRENT_KEY)
        if after != before:
            delta_delegation = delta.get("delegation")
            if not isinstance(delta_delegation, dict):
                delta_delegation = delta["delegation"] = {}
            delta_delegation[FAMILY_SESSION_MAX_CONCURRENT_KEY] = after
    for group, marker in {
        "orchestrator": "_fleet_management_disabled",
        "job_control": "_job_control_disabled",
        "job_inspection": "_job_inspection_disabled",
        "agent_catalog": "_agent_catalog_disabled",
        "workflows": "_workflows_disabled",
        "canvas": "_canvas_disabled",
    }.items():
        if group in (delta.get("tools") or {}):
            if delta["tools"][group] == []:
                agent[marker] = True
            else:
                agent.pop(marker, None)
    # Check the same typed configuration the runtime will hydrate, without
    # reading an Expert, account default, prompt file or config leaf.
    load_agent_config_from_dict(agent, deployment_dir=deployment_dir)
    updated = deepcopy(blob)
    updated["agent"] = agent
    return updated, deep_merge(deepcopy(policy), delta), delta
