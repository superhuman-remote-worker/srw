"""A partial session edit preserves its admitted sources and runtime parity."""

from copy import deepcopy
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from fastapi import HTTPException
import pytest

from orchestrator.services.manifest_execution_snapshot import (
    prepare_srw_session_patch,
    rendered_srw_snapshot,
    srw_snapshot_config,
)
from shared.runtime.core.loader import (
    deep_merge,
    load_agent_config_from_dict,
)
from shared.runtime.core.session_config_patch import patch_frozen_session


WORK = "22222222-2222-4222-8222-222222222222"


def frozen(workspace=None):
    config = {
        "agent_id": "session",
        "display_name": "Captured Session",
        "llm": {"model": "gpt-4o", "temperature": 0.3, "top_p": 0.73},
        "limits": {"context_threshold_tokens": 54321},
        "workspace": workspace or {"backend": "none"},
        "interactive": {"permission_mode": "read_only"},
        "autonomy": "review",
        "tools": {"shell": []},
    }
    blob = {
        "agent": config,
        "prompts": {"persona": "The captured persona"},
        "instructions": "The captured instructions",
        "skills": [{"name": "captured-skill"}],
    }
    snapshot = rendered_srw_snapshot(
        blob,
        config,
        work_kind="Session",
        work_id=WORK,
        owner_id=None,
        project_ids=[],
        config_name="session_base",
        description="Session",
        datasource_ids=[],
        policy_revisions={},
        image="srw:original",
        dependencies=[{"uid": "captured-expert", "revision": "original"}],
    )
    snapshot["generation"] = 1
    return snapshot, blob


@pytest.mark.asyncio
async def test_partial_patch_never_reads_changed_expert_account_or_skill_sources(
    monkeypatch,
):
    from orchestrator.services import config_resolver

    def changed_source(*args, **kwargs):
        pytest.fail("A partial edit consulted current configuration sources")

    monkeypatch.setattr(config_resolver, "resolve_config", changed_source)
    db = SimpleNamespace(
        get_expert_by_id=AsyncMock(side_effect=changed_source),
        get_user_settings=AsyncMock(side_effect=changed_source),
        manifest_skills_provider=AsyncMock(side_effect=changed_source),
        get_system_setting=AsyncMock(return_value={"value": {"enabled": False}}),
    )
    current, original = frozen()
    result, delta = await prepare_srw_session_patch(
        db,
        current,
        {"id": WORK},
        {"expert_id": "changed-expert"},
        [],
        {"llm": {"temperature": 0.7}},
    )
    blob, policy = srw_snapshot_config(result)
    assert blob["prompts"] == original["prompts"]
    assert blob["instructions"] == original["instructions"]
    assert blob["skills"] == original["skills"]
    assert blob["agent"]["llm"] == {
        "model": "gpt-4o",
        "temperature": 0.7,
        "top_p": 0.73,
    }
    assert blob["agent"]["limits"] == original["agent"]["limits"]
    assert policy["llm"] == blob["agent"]["llm"]
    assert result["dependencies"] == current["dependencies"]
    assert result["expected_generation"] == 1
    assert (
        result["resolved"]["spec"]["execution"]["expert"]["inline"]["runtime"]["image"]
        == "srw:original"
    )
    # This is the pinned runtime's typed merge, including its nested extra
    # representation. It must hydrate to exactly the persisted generation.
    pinned = load_agent_config_from_dict(original["agent"])
    patched = load_agent_config_from_dict(deep_merge(asdict(pinned), delta))
    saved = load_agent_config_from_dict(blob["agent"])
    assert asdict(patched) == asdict(saved)


def test_explicit_model_change_derives_once_and_clears_old_transport(monkeypatch):
    from shared.runtime.core import session_config_patch

    calls = []

    def model_matrix(agent, explicit_keys, deployment_dir):
        calls.append(agent["llm"]["model"])
        agent["llm"]["top_p"] = 0.91
        agent["limits"]["context_threshold_tokens"] = 32100

    monkeypatch.setattr(session_config_patch, "_apply_settings_matrix", model_matrix)
    _, original = frozen()
    original["agent"]["llm"].update(
        provider="custom", base_url="https://old.invalid", api_key="test-old"
    )
    updated, policy, delta = patch_frozen_session(
        original,
        original["agent"],
        {
            "llm": {
                "model": "new-model",
                "provider": None,
                "base_url": None,
                "api_key": None,
            }
        },
    )
    assert calls == ["new-model"]
    assert delta["llm"]["base_url"] is None
    assert delta["llm"]["api_key"] is None
    assert "base_url" not in updated["agent"]["llm"]
    assert updated["agent"]["limits"]["context_threshold_tokens"] == 32100
    assert policy["llm"]["model"] == "new-model"
    assert deep_merge(original["agent"], delta) == updated["agent"]


@pytest.mark.asyncio
async def test_partial_patch_rechecks_full_frozen_policy_under_current_grants():
    current, _ = frozen()
    runtime = current["resolved"]["spec"]["execution"]["expert"]["inline"]["runtime"]
    runtime["config"]["resolved"]["agent"]["tools"]["shell"] = ["run_command"]
    runtime["config"]["policy"]["tools"]["shell"] = ["run_command"]
    db = SimpleNamespace(
        get_system_setting=AsyncMock(return_value=None),
        list_grants_for_scopes=AsyncMock(
            return_value={"user": [], "project": [], "global": []}
        ),
    )
    with pytest.raises(HTTPException, match="shell_tools"):
        await prepare_srw_session_patch(
            db,
            current,
            {"id": WORK},
            {},
            [],
            {"llm": {"temperature": 0.7}},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fragment", [{"llm": {"temperature": 0.7}}, {"auxiliary": {"model": "new-model"}}]
)
async def test_managed_patch_failure_precedes_any_local_mutation(monkeypatch, fragment):
    import agent.api.persistent_app as mod

    config = object()
    session = SimpleNamespace(config=config, execution_snapshot={"generation": 4})
    client = SimpleNamespace(update_thread_config=AsyncMock(return_value=None))
    monkeypatch.setattr(mod, "_session", session)
    monkeypatch.setattr(mod, "_orchestrator_client", client)
    monkeypatch.setattr(mod._session_identity, "_thread_id", WORK)
    send = AsyncMock()
    monkeypatch.setattr(mod, "_ws_send", send)
    await mod._handle_config_update(MagicMock(), deepcopy(fragment))
    client.update_thread_config.assert_awaited_once_with(
        WORK,
        fragment,
        datasource_ids=None,
        snapshot_generation=4,
    )
    assert session.config is config
    assert session.execution_snapshot == {"generation": 4}
    assert send.await_args.args[1] == "error"


UNCHECKED_SANDBOX = {"image": "evil.example/x y", "cpu": "lots"}
UNCHECKED_VM = {"image": "evil.example/x y", "cpu_cores": 999}


def settings_db():
    return SimpleNamespace(
        get_system_setting=AsyncMock(return_value={"value": {"enabled": False}})
    )


async def patch_workspace(current, workspace):
    prepared, _ = await prepare_srw_session_patch(
        settings_db(), current, {"id": WORK}, {}, [], {"workspace": workspace}
    )
    prepared["generation"] = prepared["expected_generation"] + 1
    return prepared


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "captured,workspace",
    [
        ("virtual", {"backend": "sandbox", "sandbox": UNCHECKED_SANDBOX}),
        ("sandbox", {"backend": "virtual", "sandbox": UNCHECKED_SANDBOX}),
        ("virtual", {"backend": "vm", "vm": UNCHECKED_VM}),
    ],
)
async def test_a_tier_change_never_freezes_caller_written_workspace_settings(
    captured, workspace
):
    current, _ = frozen({"backend": captured})
    with pytest.raises(HTTPException) as denied:
        await patch_workspace(current, workspace)
    assert denied.value.status_code == 422
    assert "cannot change the captured" in denied.value.detail


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "steps",
    [
        [{"backend": "virtual", "sandbox": UNCHECKED_SANDBOX}, {"backend": "sandbox"}],
        [{"backend": "virtual"}, {"backend": "sandbox", "sandbox": UNCHECKED_SANDBOX}],
    ],
)
async def test_a_round_trip_through_virtual_cannot_smuggle_container_settings(steps):
    current, _ = frozen({"backend": "sandbox"})
    with pytest.raises(HTTPException) as denied:
        for workspace in steps:
            current = await patch_workspace(current, workspace)
    assert denied.value.status_code == 422
    _, policy = srw_snapshot_config(current)
    assert "sandbox" not in policy["workspace"]


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ["sandbox", "vm"])
async def test_a_bare_tier_upgrade_binds_only_the_new_backend(target):
    # The live upgrade persists exactly this fragment (persistent_app
    # _handle_workspace_upgrade). It keeps the installation defaults.
    current, _ = frozen({"backend": "virtual"})
    prepared = await patch_workspace(current, {"backend": target})
    _, policy = srw_snapshot_config(prepared)
    assert policy["workspace"] == {"backend": target}
    assert prepared["resolved"]["spec"]["execution"]["workspace"] == {
        "template": {"inline": {"backend": target}}
    }


# The llm block a session on MiniMax-M3 freezes: the `minimax-m3` family's
# matrix values (top_p, extra_body.reasoning_split) next to the base's
# reasoning_level, as config/model_config_matrix.yaml resolves them.
MINIMAX_M3_LLM = {
    "model": "MiniMax-M3",
    "provider": "openai",
    "base_url": "https://api.minimax.io/v1",
    "temperature": 1.0,
    "top_p": 0.95,
    "reasoning_level": "high",
    "max_output_tokens": 65536,
    "model_max_context_tokens": 1000000,
    "multimodal": True,
    "parallel_tool_calls": True,
    "extra_body": {"reasoning_split": True},
}
SWAP_TO_CLAUDE = {
    "llm": {
        "model": "claude-opus-5-5",
        "provider": "anthropic",
        "base_url": None,
        "api_key": None,
    }
}


def frozen_on(llm):
    current, _ = frozen()
    runtime = current["resolved"]["spec"]["execution"]["expert"]["inline"]["runtime"]
    for section in ("resolved", "policy"):
        target = runtime["config"][section]
        target = target["agent"] if section == "resolved" else target
        target["llm"] = deepcopy(llm)
    return current


async def swap(current, override, metadata=None):
    prepared, delta = await prepare_srw_session_patch(
        settings_db(), current, {"id": WORK}, metadata or {}, [], deepcopy(override)
    )
    blob, _ = srw_snapshot_config(prepared)
    return blob["agent"]["llm"], delta


@pytest.mark.asyncio
async def test_a_swap_to_another_family_drops_the_old_familys_matrix_settings():
    llm, delta = await swap(frozen_on(MINIMAX_M3_LLM), SWAP_TO_CLAUDE)

    assert llm["model"] == "claude-opus-5-5"
    # Only MiniMax's matrix defines these; Claude's API refuses both.
    assert "extra_body" not in llm
    assert "top_p" not in llm
    # The pinned runtime's merge clears them too.
    assert delta["llm"]["extra_body"] is None
    assert delta["llm"]["top_p"] is None
    # Keys the new family defines are re-derived as before.
    assert llm["max_output_tokens"] == 65536
    assert llm["model_max_context_tokens"] == 1000000
    assert llm["reasoning_level"] == "high"


@pytest.mark.asyncio
async def test_a_swap_off_muse_returns_reasoning_level_to_the_loader_default():
    muse = {
        "model": "muse-spark-1.3-contributor",
        "temperature": 1.0,
        "top_p": 1.0,
        "reasoning_level": "medium",
        "max_output_tokens": 131072,
        "model_max_context_tokens": 1048576,
    }
    llm, _ = await swap(frozen_on(muse), SWAP_TO_CLAUDE)

    assert "reasoning_level" not in llm
    typed = load_agent_config_from_dict(
        {"agent_id": "session", "display_name": "Session", "llm": llm}
    )
    assert typed.llm.reasoning_level == "high"
    assert "top_p" not in llm


@pytest.mark.asyncio
async def test_a_value_that_is_not_the_matrix_value_survives_the_swap():
    authored = {**MINIMAX_M3_LLM, "top_p": 0.8, "extra_body": {"foo": 1}}
    llm, _ = await swap(frozen_on(authored), SWAP_TO_CLAUDE)

    assert llm["top_p"] == 0.8
    assert llm["extra_body"] == {"foo": 1}


@pytest.mark.asyncio
async def test_a_value_the_user_pinned_or_sends_survives_the_swap():
    pinned = {"config_override": {"llm": {"top_p": 0.95}}}
    override = deepcopy(SWAP_TO_CLAUDE)
    override["llm"]["extra_body"] = {"reasoning_split": True}
    llm, _ = await swap(frozen_on(MINIMAX_M3_LLM), override, metadata=pinned)

    assert llm["top_p"] == 0.95
    assert llm["extra_body"] == {"reasoning_split": True}


@pytest.mark.asyncio
async def test_a_swap_within_the_family_keeps_the_familys_settings():
    llm, _ = await swap(
        frozen_on({**MINIMAX_M3_LLM, "model": "MiniMax-M2.7"}),
        {"llm": {"model": "MiniMax-M3"}},
    )

    assert llm["extra_body"] == {"reasoning_split": True}
    assert llm["top_p"] == 0.95


@pytest.mark.asyncio
async def test_a_cosmetic_llm_change_clears_nothing():
    llm, _ = await swap(frozen_on(MINIMAX_M3_LLM), {"llm": {"temperature": 0.7}})

    assert llm["extra_body"] == {"reasoning_split": True}
    assert llm["top_p"] == 0.95
    assert llm["temperature"] == 0.7
