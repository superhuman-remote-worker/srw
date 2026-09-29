"""U3/U4 delegation compatibility and explicit control-plane grants.

The settings block is ``{enabled, max_concurrent,
run_in_background_default}`` plus the session fan-out keys
(``shared.runtime.core.delegation_settings``). ``delegate_agent`` plus four U4 controls are the
current delegation tools, while every layer authored before them keeps
resolving to the foreground spawn tool only.

Contract (u3_plan.md B.12, universal_experts_and_subagents.md §0 D1/D2/D7):

* ``normalize_delegation_block`` drops the pre-U3 keys (``max_depth`` /
  ``default_timeout`` / ``max_timeout`` / ``allowed_configs`` / ``mode`` /
  ``light``) of ONE layer, keeps the rest of the block, never mutates its
  input, and logs one de-duplicated deprecation warning per (source, layer);
* ``normalize_tool_policy`` maps ``tools.delegation: [spawn_subagent |
  delegate_work | resume_delegation_child]`` onto ``[delegate_agent]`` (a hard
  rename — no alias tool exists), deduplicated, one warning per layer; the
  request boundary (``validate_tool_override_fragment``) does the same;
* ``DelegationConfig`` is the three keys plus the session fan-out keys;
  ``delegation.enabled`` gates the ``delegate_agent`` binding (``load_tools``
  yields nothing when it is false);
* a stored critic fragment carrying the old shape resolves through
  ``resolve_config`` and passes the expert save validation, canonical.
"""

from __future__ import annotations

from tests._expert_catalog import catalogue_service


import logging

import pytest

from orchestrator.services.config_resolver import resolve_config
from shared.runtime.core import loader, tool_policy
from shared.runtime.core.expert_resolution import build_expert_config
from shared.runtime.core.loader import (
    DelegationConfig,
    load_agent_config_from_dict,
    load_role_base,
    normalize_delegation_block,
)
from shared.runtime.core.tool_policy import (
    LEGACY_TOOL_NAME_ALIASES,
    normalize_tool_policy,
    validate_tool_override_fragment,
)
from agent.tools.context import ToolContext
from agent.tools.registry import TOOL_REGISTRY, load_tools

_LEGACY_BLOCK = {
    "enabled": True,
    "max_depth": 2,
    "default_timeout": 7200,
    "max_timeout": 14400,
    "allowed_configs": ["scholar"],
    "mode": "light",
    "light": {"enabled": True, "max_parallel": 3, "allow_writes": True},
    "max_concurrent": 2,
}


@pytest.fixture(autouse=True)
def _fresh_dedup_sets():
    """Both warning de-dup sets are module-global; every test starts clean."""
    loader._DELEGATION_LOG_SEEN.clear()
    tool_policy._ALIAS_LOG_SEEN.clear()
    yield
    loader._DELEGATION_LOG_SEEN.clear()
    tool_policy._ALIAS_LOG_SEEN.clear()


# --- the settings block ------------------------------------------------------


def test_legacy_keys_are_dropped_and_the_rest_of_the_block_survives():
    out = normalize_delegation_block({"delegation": dict(_LEGACY_BLOCK)}, source="t")
    assert out["delegation"] == {"enabled": True, "max_concurrent": 2}


def test_normalize_delegation_block_never_mutates_and_is_identity_when_clean():
    legacy = {"delegation": dict(_LEGACY_BLOCK), "llm": {"model": "m"}}
    before = {"delegation": dict(_LEGACY_BLOCK), "llm": {"model": "m"}}
    normalize_delegation_block(legacy, source="t")
    assert legacy == before

    clean = {"delegation": {"enabled": False, "max_concurrent": 4}}
    assert normalize_delegation_block(clean, source="t") is clean
    no_block = {"llm": {"model": "m"}}
    assert normalize_delegation_block(no_block, source="t") is no_block
    assert normalize_delegation_block("not a dict", source="t") == "not a dict"
    odd = {"delegation": "light"}
    assert normalize_delegation_block(odd, source="t") is odd


def test_one_deprecation_warning_per_source_and_layer(caplog):
    with caplog.at_level(logging.WARNING, logger="shared.runtime.core.loader"):
        for _ in range(3):
            normalize_delegation_block({"delegation": dict(_LEGACY_BLOCK)}, source="a")
        normalize_delegation_block({"delegation": {"mode": "heavy"}}, source="a")
        normalize_delegation_block({"delegation": dict(_LEGACY_BLOCK)}, source="b")
    warnings = [r for r in caplog.records if "legacy delegation key(s)" in r.message]
    assert len(warnings) == 3  # (a, full), (a, mode-only), (b, full)
    assert "'mode'" in warnings[0].message and " in a " in warnings[0].message
    assert "max_concurrent" in warnings[0].message  # names the surviving shape


def test_delegation_config_is_the_three_keys_plus_session_fanout():
    assert {f.name for f in DelegationConfig.__dataclass_fields__.values()} == {
        "enabled",
        "max_concurrent",
        "run_in_background_default",
        # parallel_subagents.md §6.4: the session fan-out gate, cap and
        # per-turn maximum (shared.runtime.core.delegation_settings).
        "session_fanout",
        "session_fanout_pinned",
        "session_max_concurrent",
        "family_session_max_concurrent",
        "session_max_calls_per_turn",
    }
    cfg = load_agent_config_from_dict(
        {
            "agent_id": "a",
            "display_name": "A",
            "delegation": dict(_LEGACY_BLOCK),
        }
    )
    assert cfg.delegation == DelegationConfig(enabled=True, max_concurrent=2)
    assert "delegation" not in cfg.extra


def test_merged_dict_path_logs_the_frozen_blob_once(caplog):
    """A pre-U3 frozen ``resolved_config`` blob enters through the merged-dict
    seam (``load_agent_config_from_dict``) — the same drop, source ``merged:<id>``."""
    with caplog.at_level(logging.WARNING, logger="shared.runtime.core.loader"):
        for _ in range(2):
            load_agent_config_from_dict(
                {"agent_id": "frozen", "display_name": "F", "delegation": _LEGACY_BLOCK}
            )
    hits = [r for r in caplog.records if "legacy delegation key(s)" in r.message]
    assert len(hits) == 1 and "merged:frozen" in hits[0].message


# --- the tool name -------------------------------------------------------------


def test_the_old_delegation_tools_are_gone_and_u4_controls_are_explicit():
    for old in ("spawn_subagent", "delegate_work", "resume_delegation_child"):
        assert old not in TOOL_REGISTRY, old
    assert TOOL_REGISTRY["delegate_agent"]["category"] == "delegation"
    assert {
        name
        for name, metadata in TOOL_REGISTRY.items()
        if metadata.get("category") == "delegation"
    } == {
        "delegate_agent",
        "wait_agent",
        "message_agent",
        "stop_agent",
        "list_agents",
    }
    assert all(
        TOOL_REGISTRY[name].get("grant") == "explicit"
        for name in (
            "delegate_agent",
            "wait_agent",
            "message_agent",
            "stop_agent",
            "list_agents",
        )
    )
    assert set(LEGACY_TOOL_NAME_ALIASES) == {"delegation"}
    assert set(LEGACY_TOOL_NAME_ALIASES["delegation"].values()) == {"delegate_agent"}


@pytest.mark.parametrize(
    "value",
    [
        ["spawn_subagent"],
        ["delegate_work", "resume_delegation_child"],
        ["spawn_subagent", "delegate_agent"],
        {"only": ["spawn_subagent"]},
    ],
)
def test_legacy_tool_names_map_to_delegate_agent_deduplicated(value):
    out = normalize_tool_policy({"tools": {"delegation": value}}, source="t")
    assert out["tools"]["delegation"] == ["delegate_agent"]


def test_tool_rename_is_layer_local_logged_once_and_never_mutates(caplog):
    fragment = {"tools": {"delegation": ["spawn_subagent"], "git": ["git_status"]}}
    with caplog.at_level(logging.WARNING, logger="shared.runtime.core.tool_policy"):
        for _ in range(3):
            normalize_tool_policy(fragment, source="db-expert:critic")
        normalize_tool_policy(fragment, source="job-override")
    hits = [r for r in caplog.records if "legacy tool name(s)" in r.message]
    assert len(hits) == 2
    assert "db-expert:critic" in hits[0].message and "delegate_agent" in hits[0].message
    assert fragment["tools"]["delegation"] == ["spawn_subagent"]  # input untouched


def test_a_clean_delegation_list_is_returned_as_written():
    out = normalize_tool_policy({"tools": {"delegation": ["delegate_agent"]}})
    assert out["tools"]["delegation"] == ["delegate_agent"]
    # Only the delegation category has aliases — a stray old name elsewhere is
    # left for the foreign-name gate, not silently rewritten.
    out = normalize_tool_policy({"tools": {"research": ["spawn_subagent"]}})
    assert out["tools"]["research"] == ["spawn_subagent"]


def test_request_boundary_accepts_and_maps_the_old_name():
    """The cockpit re-submits a stored row's tools verbatim; the write boundary
    must not 400 an old name, and what it accepts is canonical."""
    accepted = validate_tool_override_fragment(
        {"tools": {"delegation": ["delegate_work", "spawn_subagent"]}}
    )
    assert accepted == {"delegation": ["delegate_agent"]}


def test_delegation_enumerates_and_a_stored_true_maps_to_delegate_agent(caplog):
    """``delegate_agent`` is ``grant: explicit``, so ``delegation: true`` would
    expand to ``[]`` — "off" while a toggle believed it turned delegation on.
    The category enumerates like ``shell`` for the VOCABULARY: ``except`` is
    refused everywhere, the request boundary refuses ``true`` (a new request
    has the served enumeration to send), and the cockpit's generic
    enumerate-only path gets ``[delegate_agent]``. A STORED ``true`` (jobs /
    threads created through the toggle while ``true`` was legal) is compat:
    the config-layer seam maps it to exactly ``[delegate_agent]`` — never all
    members — with one warning per (source, category), never an error."""
    from shared.runtime.core.tool_policy import (
        ENUMERATE_ONLY_CATEGORIES,
        LEGACY_TRUE_EXPANSIONS,
        ToolPolicyError,
        enumerate_only_members,
        expand_category_true,
    )

    assert "delegation" in ENUMERATE_ONLY_CATEGORIES
    assert enumerate_only_members()["delegation"] == [
        "delegate_agent",
        "list_agents",
        "message_agent",
        "stop_agent",
        "wait_agent",
    ]
    assert LEGACY_TRUE_EXPANSIONS == {"delegation": ("delegate_agent",)}
    with pytest.raises(ToolPolicyError, match="must enumerate"):
        expand_category_true("delegation")
    with pytest.raises(ToolPolicyError, match="must enumerate"):
        validate_tool_override_fragment({"tools": {"delegation": True}})
    for boundary in (normalize_tool_policy, validate_tool_override_fragment):
        with pytest.raises(ToolPolicyError, match="must enumerate"):
            boundary({"tools": {"delegation": {"except": ["delegate_agent"]}}})

    fragment = {"tools": {"delegation": True, "git": ["git_status"]}}
    with caplog.at_level(logging.WARNING, logger="shared.runtime.core.tool_policy"):
        for _ in range(3):
            out = normalize_tool_policy(fragment, source="request-override")
        normalize_tool_policy(fragment, source="thread-override")
    assert out["tools"] == {"delegation": ["delegate_agent"], "git": ["git_status"]}
    assert fragment["tools"]["delegation"] is True  # input untouched
    hits = [r for r in caplog.records if "legacy `tools.delegation: true`" in r.message]
    assert len(hits) == 2  # once per (source, category)
    assert "request-override" in hits[0].message and "delegate_agent" in hits[0].message
    assert "thread-override" in hits[1].message

    assert normalize_tool_policy({"tools": {"delegation": False}})["tools"] == {
        "delegation": []
    }
    assert normalize_tool_policy({"tools": {"delegation": []}})["tools"] == {
        "delegation": []
    }
    assert validate_tool_override_fragment(
        {"tools": {"delegation": {"only": ["delegate_agent"]}}}
    ) == {"delegation": ["delegate_agent"]}


def test_a_stored_job_override_with_delegation_true_resolves(caplog):
    """A job created through the cockpit toggle while ``true`` was legal
    carries ``config_override.tools.delegation: true`` — its resume must
    resolve to ``[delegate_agent]`` with one warning and no error."""
    cap: dict = {}
    with caplog.at_level(logging.WARNING, logger="shared.runtime.core.tool_policy"):
        blob = resolve_config(
            base_config_name="worker_base",
            request_override={
                "tools": {"delegation": True},
                "delegation": {"enabled": True, "max_depth": 2},
            },
            expert_type="worker",
            capture=cap,
        )
    assert cap["merged_fragment"]["tools"]["delegation"] == ["delegate_agent"]
    assert blob["agent"]["tools"]["delegation"] == ["delegate_agent"]
    assert blob["agent"]["delegation"]["enabled"] is True
    assert "max_depth" not in blob["agent"]["delegation"]
    hits = [r for r in caplog.records if "legacy `tools.delegation: true`" in r.message]
    assert len(hits) == 1 and "request-override" in hits[0].message
    # and the frozen blob re-parses without a legacy shape
    cfg = load_agent_config_from_dict(blob["agent"])
    assert cfg.tools.delegation == ["delegate_agent"] and cfg.delegation.enabled


def test_a_stored_thread_override_with_delegation_true_resolves():
    """The session resolve path hands the thread override to ``resolve_config``
    as the request layer; the agent-side attach / live-update seams label the
    same layer ``thread-override`` (pinned above). Either way: mapped, not refused."""
    cap: dict = {}
    blob = resolve_config(
        base_config_name="session_base",
        request_override={
            "tools": {"delegation": True},
            "delegation": {"enabled": True},
        },
        expert_type="session",
        capture=cap,
    )
    assert cap["merged_fragment"]["tools"]["delegation"] == ["delegate_agent"]
    assert blob["agent"]["tools"]["delegation"] == ["delegate_agent"]


def test_an_authored_yaml_true_is_mapped_and_logged(tmp_path, caplog):
    """A bundled/uploaded YAML that still says ``delegation: true`` behaves
    like a stored layer: mapped to ``[delegate_agent]`` at the chain seam,
    logged once with the file as the source."""
    from shared.runtime.core.loader import load_agent_config

    leaf = tmp_path / "config.yaml"
    leaf.write_text(
        "$extends: worker_base\nagent_id: legacy-true\ndisplay_name: Legacy\n"
        "tools:\n  delegation: true\ndelegation:\n  enabled: true\n",
        encoding="utf-8",
    )
    with caplog.at_level(logging.WARNING, logger="shared.runtime.core.tool_policy"):
        cfg = load_agent_config(str(leaf))
        load_agent_config(str(leaf))
    assert cfg.tools.delegation == ["delegate_agent"]
    assert cfg.delegation.enabled is True
    hits = [r for r in caplog.records if "legacy `tools.delegation: true`" in r.message]
    assert len(hits) == 1 and str(leaf) in hits[0].message


# --- the binding gate ----------------------------------------------------------


def _ctx(enabled: bool) -> ToolContext:
    return ToolContext(
        config={
            "agent_id": "t",
            "delegation": {"enabled": enabled, "max_concurrent": 4},
            "subagents": {"default": "explorer", "roster": {"explorer": {}}},
        },
        _job_metadata={"job_id": "j"},
    )


def test_delegation_enabled_gates_the_binding():
    assert [t.name for t in load_tools(["delegate_agent"], _ctx(True))] == [
        "delegate_agent"
    ]
    assert load_tools(["delegate_agent"], _ctx(False)) == []


# --- the shipped configs --------------------------------------------------------


@pytest.mark.parametrize(
    "name", ["bughunter", "critic", "developer", "product-qa", "scholar"]
)
def test_the_five_delegating_experts_grant_spawn_and_controls(name):
    from shared.runtime.core.loader import load_agent_config, resolve_config_path

    cfg = load_agent_config(*resolve_config_path(name))
    assert cfg.tools.delegation == [
        "delegate_agent",
        "wait_agent",
        "message_agent",
        "stop_agent",
        "list_agents",
    ], name
    assert cfg.delegation.enabled is True, name


def test_general_worker_and_the_bases_keep_delegation_off():
    from shared.runtime.core.loader import load_agent_config, resolve_config_path

    cfg = load_agent_config(*resolve_config_path("general-worker"))
    assert cfg.delegation.enabled is False and cfg.tools.delegation == []
    for role in ("worker", "session", "subagent"):
        base = load_role_base(role)
        assert (base.get("delegation") or {}).get("enabled") is not True, role
        assert not base.get("tools", {}).get("delegation"), role


# --- a stored DB expert row authored before U3 ---------------------------------

_STORED_CRITIC_FRAGMENT = {
    "llm": {"model": "critic-model"},
    "tools": {"delegation": ["spawn_subagent"], "git": ["git_status"]},
    "delegation": {"enabled": True, "mode": "light", "max_timeout": 14400},
}


def _critic_row() -> dict:
    return {
        "id": "00000000-0000-4000-8000-000000000001",
        "name": "my-critic",
        "expert_type": "worker",
        "config": dict(_STORED_CRITIC_FRAGMENT),
        "prompts": {},
    }


def test_build_expert_config_normalises_a_stored_critic_fragment(caplog):
    base = load_role_base("worker")
    with caplog.at_level(logging.WARNING):
        merged, _prompts = build_expert_config(base, _critic_row())
        build_expert_config(base, _critic_row())  # the second row logs nothing new
    assert merged["tools"]["delegation"] == ["delegate_agent"]
    assert merged["delegation"]["enabled"] is True
    assert (
        "mode" not in merged["delegation"] and "max_timeout" not in merged["delegation"]
    )
    tool_hits = [r for r in caplog.records if "legacy tool name(s)" in r.message]
    block_hits = [r for r in caplog.records if "legacy delegation key(s)" in r.message]
    assert len(tool_hits) == 1 and "db-expert:my-critic" in tool_hits[0].message
    assert len(block_hits) == 1 and "db-expert:my-critic" in block_hits[0].message


def test_resolve_config_resolves_a_stored_critic_fragment_canonically():
    cap: dict = {}
    blob = resolve_config(
        base_config_name="critic",
        expert_row=_critic_row(),
        expert_type="worker",
        capture=cap,
    )
    merged = cap["merged_fragment"]
    assert merged["tools"]["delegation"] == ["delegate_agent"]
    assert merged["delegation"] == {
        "enabled": True,
        "max_concurrent": 4,
        "run_in_background_default": False,
    }
    agent = blob["agent"]
    assert agent["tools"]["delegation"] == ["delegate_agent"]
    # The frozen blob is the typed config: the authored keys, plus the session
    # fan-out keys at their defaults (no model family sets a cap yet).
    assert agent["delegation"] == {
        **merged["delegation"],
        "session_fanout": False,
        "session_fanout_pinned": False,
        "session_max_concurrent": None,
        "family_session_max_concurrent": None,
        "session_max_calls_per_turn": 20,
    }
    # The blob round-trips through the merged-dict seam without a legacy key.
    cfg = load_agent_config_from_dict(agent)
    assert cfg.delegation == DelegationConfig(enabled=True)
    assert cfg.tools.delegation == ["delegate_agent"]


def test_expert_save_validation_persists_the_stored_fragment_canonical():
    """The save path (``_validate_expert_fragment``) is where a managed seed
    row's next edit would 422/400 on the old shape — it must map instead."""

    saved = catalogue_service().validate_expert_fragment(dict(_STORED_CRITIC_FRAGMENT))
    assert saved["tools"]["delegation"] == ["delegate_agent"]
    assert saved["tools"]["git"] == ["git_status"]
    assert saved["delegation"] == {"enabled": True}
    assert saved["llm"] == {"model": "critic-model"}
