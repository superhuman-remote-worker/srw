"""U1 tier collapse — the legacy ``llm.strategic`` / ``llm.tactical`` /
``llm.subagent`` tiers are ACCEPTED in every authored layer and mapped onto the
single ``llm.model`` (+ ``subagents.llm``) by ``normalize_llm_tiers``.

Contract (u1_plan.md B.6, universal_experts_and_subagents.md §1.1):

* layer-local rule — a layer with no ``llm.model`` lifts its strategic block
  (model AND transport AND params) into ``llm``; strategic > tactical; an
  explicit ``llm.model`` in the same layer wins over both; ``llm.subagent`` moves
  to ``subagents.llm``;
* merged-dict rule — pre-U1 frozen blobs / hand-built dicts:
  ``strategic.model`` > ``tactical.model`` > ``model``;
* one de-duplicated deprecation warning per (source, layer).
"""

import logging

import pytest
import yaml

from orchestrator.services.config_resolver import resolve_config, unrouted_model_slots
from shared.runtime.core import loader
from shared.runtime.core.loader import (
    LLMConfig,
    load_agent_config,
    load_agent_config_from_dict,
    load_config_from_resolved,
    normalize_llm_tiers,
)


@pytest.fixture(autouse=True)
def _fresh_dedup_set():
    """The warning de-dup set is module-global; every test starts clean."""
    loader._TIER_LOG_SEEN.clear()
    yield
    loader._TIER_LOG_SEEN.clear()


# --- pure mapping: layer-local rule -----------------------------------------


def test_layer_strategic_lifts_to_model_when_unset():
    out = normalize_llm_tiers(
        {"llm": {"temperature": 0.1, "strategic": {"model": "big"}}}, source="t"
    )
    assert out["llm"] == {"temperature": 0.1, "model": "big"}
    assert "strategic" not in out["llm"]


def test_layer_explicit_model_beats_legacy_strategic():
    """July incident: a phase pin must never shadow the model the layer picked."""
    out = normalize_llm_tiers(
        {"llm": {"model": "picked", "strategic": {"model": "pin", "base_url": "u"}}},
        source="t",
    )
    assert out["llm"] == {"model": "picked"}  # block dropped wholesale


def test_tactical_only_lifts_when_no_strategic():
    only_tactical = normalize_llm_tiers(
        {"llm": {"tactical": {"model": "tac"}}}, source="t"
    )
    assert only_tactical["llm"]["model"] == "tac"

    both = normalize_llm_tiers(
        {"llm": {"strategic": {"model": "strat"}, "tactical": {"model": "tac"}}},
        source="t",
    )
    assert both["llm"] == {"model": "strat"}  # strategic wins, tactical dropped


def test_transport_travels_with_the_lifted_block():
    """A dispatcher credentials the phase block it sees; the lift must carry
    base_url/api_key/provider with the model or the pin lands transport-less."""
    out = normalize_llm_tiers(
        {
            "llm": {
                "base_url": "http://base/v1",
                "tactical": {
                    "model": "codex",
                    "base_url": "http://codex/v1",
                    "api_key": "sk-codex",
                    "provider": None,  # serialized None leaf must not clear anything
                },
            }
        },
        source="t",
    )
    assert out["llm"] == {
        "model": "codex",
        "base_url": "http://codex/v1",
        "api_key": "sk-codex",
    }


def test_subagent_tier_moves_to_subagents_llm():
    out = normalize_llm_tiers(
        {"llm": {"model": "opus", "subagent": {"model": "sonnet"}}}, source="t"
    )
    assert out["llm"] == {"model": "opus"}
    assert out["subagents"] == {"llm": {"model": "sonnet"}}

    explicit = normalize_llm_tiers(
        {
            "llm": {"model": "opus", "subagent": {"model": "legacy"}},
            "subagents": {"llm": {"model": "new"}},
        },
        source="t",
    )
    assert explicit["subagents"]["llm"] == {"model": "new"}  # explicit wins
    assert "subagent" not in explicit["llm"]


def test_identity_and_no_mutation_without_legacy_keys():
    clean = {"llm": {"model": "m"}, "tools": {"core": []}}
    assert normalize_llm_tiers(clean, source="t") is clean

    legacy = {"llm": {"strategic": {"model": "s"}}}
    normalize_llm_tiers(legacy, source="t")
    assert legacy == {"llm": {"strategic": {"model": "s"}}}  # caller-owned layer


# --- merged-dict rule (pre-U1 frozen blobs) ----------------------------------


def test_merged_dict_rule_strategic_wins():
    """A pre-U1 ``resolved_config`` blob hydrates to the model the strategic
    phase used to run — ``strategic.model`` beats the top-level model."""
    pre_u1_blob = {
        "agent": {
            "agent_id": "legacy",
            "display_name": "Legacy",
            "llm": {
                "model": "base",
                "provider": None,
                "base_url": "http://router/v1",
                "strategic": {
                    "model": "strat",
                    "provider": None,
                    "base_url": None,
                    "temperature": 0.7,
                },
                "tactical": {"model": "tac", "provider": None, "base_url": None},
                "summarization": None,
                "subagent": {"model": "reader", "provider": None},
            },
        },
        "prompts": {},
        "instructions": {},
    }
    cfg = load_config_from_resolved(pre_u1_blob)
    assert cfg.llm.model == "strat"
    assert cfg.llm.temperature == 0.7
    assert cfg.llm.base_url == "http://router/v1"  # None leaf did not clear it
    assert cfg.llm.summarization is None
    assert not hasattr(cfg.llm, "strategic")
    assert not hasattr(cfg.llm, "tactical")
    assert cfg.subagents.llm == {"model": "reader"}  # parsed field since WP3
    assert "subagents" not in cfg.extra


def test_merged_dict_rule_tactical_when_no_strategic_model():
    cfg = load_agent_config_from_dict(
        {
            "agent_id": "a",
            "display_name": "A",
            "llm": {"model": "base", "tactical": {"model": "tac"}},
        }
    )
    assert cfg.llm.model == "tac"


def test_single_model_get_phase_config_is_identity_for_legacy_names():
    llm = LLMConfig(model="one")
    for phase in ("strategic", "tactical", "subagent"):
        assert llm.get_phase_config(phase) is llm


# --- authored layers through the real seams ---------------------------------


def test_bundled_layer_strategic_pin_lifts_through_extends(tmp_path):
    """A bundled YAML leaf (``$extends`` chain) carrying a legacy strategic pin
    resolves to that model, and the lifted params count as explicit for the
    settings matrix (they are not clobbered by the family default)."""
    leaf = tmp_path / "legacy_expert.yaml"
    leaf.write_text(
        yaml.safe_dump(
            {
                "$extends": "worker_base",
                "agent_id": "legacy_expert",
                "display_name": "Legacy",
                "llm": {"strategic": {"model": "gpt-5.5", "temperature": 0.42}},
            }
        ),
        encoding="utf-8",
    )
    cfg = load_agent_config(str(leaf))
    assert cfg.llm.model == "gpt-5.5"
    assert cfg.llm.temperature == 0.42
    assert cfg.llm.summarization is None


def test_resolve_config_db_expert_strategic_pin_yields_same_model():
    """Acceptance (U1): an existing DB expert pinned via ``llm.strategic``
    resolves to the same effective model it ran on before the collapse."""
    row = {
        "expert_type": "worker",
        "name": "pinned-worker",
        "config": {"llm": {"strategic": {"model": "expert-strategic-model"}}},
        "prompts": {},
    }
    blob = resolve_config(
        base_config_name="worker_base", expert_row=row, expert_type="worker"
    )
    assert blob["agent"]["llm"]["model"] == "expert-strategic-model"
    assert "strategic" not in blob["agent"]["llm"]
    assert "tactical" not in blob["agent"]["llm"]


def test_resolve_config_job_override_tactical_pin():
    """A stored job ``config_override`` with a (credentialed) tactical pin: the
    model AND its transport land at the top level of the frozen blob, so the
    fail-fast routing check sees a routed slot."""
    blob = resolve_config(
        base_config_name="worker_base",
        request_override={
            "llm": {
                "tactical": {
                    "model": "job-tactical-model",
                    "base_url": "http://tac/v1",
                    "api_key": "sk-tac",
                }
            }
        },
        expert_type="worker",
    )
    llm = blob["agent"]["llm"]
    assert llm["model"] == "job-tactical-model"
    assert llm["base_url"] == "http://tac/v1"
    assert "api_key" not in llm  # serialize_resolved_config strips secrets
    assert "tactical" not in llm
    # The lifted slot is routed (base_url survived the lift). The base's
    # transport-less `auxiliary` model is a pre-existing, unrelated report —
    # production credentials it in inject_blob_credentials.
    assert not [p for p in unrouted_model_slots(blob) if p.startswith("llm")]


def test_request_layer_explicit_model_wins_over_its_own_legacy_pin():
    """Per-layer: the request layer's explicit model beats the pin in the SAME
    layer, and a lower layer's model is irrelevant to that decision."""
    blob = resolve_config(
        base_config_name="worker_base",
        expert_row={
            "expert_type": "worker",
            "name": "low",
            "config": {"llm": {"model": "expert-model"}},
            "prompts": {},
        },
        request_override={"llm": {"model": "picked", "strategic": {"model": "shadow"}}},
        expert_type="worker",
    )
    assert blob["agent"]["llm"]["model"] == "picked"


def test_thread_override_legacy_subagent():
    """A session thread override carrying the old ``llm.subagent`` tier (the
    session resolve path hands it to ``resolve_config`` as the request layer)
    lands as ``subagents.llm`` in the blob, where the light runner reads it."""
    blob = resolve_config(
        base_config_name="session_base",
        request_override={"llm": {"subagent": {"model": "reader-model"}}},
        expert_type="session",
    )
    assert "subagent" not in blob["agent"]["llm"]
    assert blob["agent"]["subagents"]["llm"] == {"model": "reader-model"}
    # …and it survives hydration on the agent side as the parsed field.
    cfg = load_config_from_resolved(blob)
    assert cfg.subagents.llm == {"model": "reader-model"}
    assert "subagents" not in cfg.extra


# --- logging ----------------------------------------------------------------


def test_deprecation_logged_once_per_layer(caplog):
    layer = {"llm": {"strategic": {"model": "s"}, "subagent": {"model": "r"}}}
    with caplog.at_level(logging.WARNING, logger="shared.runtime.core.loader"):
        normalize_llm_tiers(layer, source="db-expert:x")
        normalize_llm_tiers(layer, source="db-expert:x")  # same layer: silent
        normalize_llm_tiers(layer, source="request-override")  # new source: logs
    records = [r for r in caplog.records if "legacy llm tier" in r.getMessage()]
    assert len(records) == 2
    first = records[0].getMessage()
    assert "['strategic', 'subagent']" in first
    assert "db-expert:x" in first
    assert "llm.strategic -> llm (model='s')" in first
    assert "llm.subagent -> subagents.llm" in first
    assert "deprecated" in first
    assert ".md" not in first  # no pointer into the private design vault


def test_no_warning_for_empty_legacy_blocks(caplog):
    with caplog.at_level(logging.WARNING, logger="shared.runtime.core.loader"):
        out = normalize_llm_tiers(
            {"llm": {"model": "m", "strategic": {}, "tactical": None}}, source="t"
        )
    assert out["llm"] == {"model": "m"}
    assert not [r for r in caplog.records if "legacy llm tier" in r.getMessage()]
