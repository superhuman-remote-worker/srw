"""The model config carries the endpoint's pinned certificate (wire string).

The value stays opaque here: the chat factories parse it into a ``TlsPin``.
"""

from __future__ import annotations

import dataclasses

from shared.runtime.core.loader import (
    LLM_INHERITED_KEYS,
    AuxiliaryConfig,
    LLMConfig,
    PhaseLLMOverride,
    _parse_auxiliary_config,
    _parse_llm_config,
    _parse_phase_override,
    build_auxiliary_llm_config,
    deep_merge,
    inherit_parent_llm,
)

PIN = "MIIBpinned"
OTHER = "MIIBother"


def test_inherited_keys_include_pin():
    assert "tls_pinned_cert" in LLM_INHERITED_KEYS


def test_llm_parse_reads_pin():
    assert (
        _parse_llm_config({"model": "m", "tls_pinned_cert": PIN}).tls_pinned_cert == PIN
    )
    assert _parse_llm_config({"model": "m"}).tls_pinned_cert is None


def test_phase_override_parse_reads_pin():
    got = _parse_phase_override({"model": "m", "tls_pinned_cert": PIN})
    assert got.tls_pinned_cert == PIN


def test_auxiliary_parse_and_build_carry_pin():
    aux = _parse_auxiliary_config({"model": "m", "tls_pinned_cert": PIN})
    assert aux.tls_pinned_cert == PIN
    assert build_auxiliary_llm_config(aux, {}).tls_pinned_cert == PIN
    assert AuxiliaryConfig().tls_pinned_cert is None


def test_override_same_origin_keeps_parents_pin():
    parent = LLMConfig(base_url="https://h:8443/v1", tls_pinned_cert=PIN)
    assert parent.with_override(PhaseLLMOverride(model="x")).tls_pinned_cert == PIN
    same = PhaseLLMOverride(base_url="https://h:8443/other")
    assert parent.with_override(same).tls_pinned_cert == PIN


def test_override_other_origin_drops_pin_unless_it_brings_one():
    parent = LLMConfig(base_url="https://h:8443/v1", tls_pinned_cert=PIN)
    moved = PhaseLLMOverride(base_url="https://elsewhere/v1")
    assert parent.with_override(moved).tls_pinned_cert is None
    brings = PhaseLLMOverride(base_url="https://elsewhere/v1", tls_pinned_cert=OTHER)
    assert parent.with_override(brings).tls_pinned_cert == OTHER


def test_override_pin_wins_without_base_url():
    parent = LLMConfig(base_url="https://h/v1", tls_pinned_cert=PIN)
    got = parent.with_override(PhaseLLMOverride(tls_pinned_cert=OTHER))
    assert got.tls_pinned_cert == OTHER


def test_inherit_parent_llm_copies_and_drops_pin():
    entry = {"model": "inherit"}
    inherit_parent_llm(entry, {"model": "a", "tls_pinned_cert": PIN})
    assert entry["tls_pinned_cert"] == PIN
    # Parent model changed to one with no pin: the old pin must go.
    inherit_parent_llm(entry, {"model": "b", "tls_pinned_cert": None})
    assert "tls_pinned_cert" not in entry


def test_overlay_live_llm_copies_and_drops_pin():
    from agent.subagents.child import overlay_live_llm

    child = LLMConfig(model="a", tls_pinned_cert=PIN)
    same = overlay_live_llm(child, LLMConfig(model="a", tls_pinned_cert=OTHER))
    assert same.tls_pinned_cert == OTHER
    swapped = overlay_live_llm(child, LLMConfig(model="b", tls_pinned_cert=None))
    assert swapped.tls_pinned_cert is None


def test_hot_swap_delta_clears_pin_through_merge():
    """Review Focus 1: orchestrator's swap delta over a pinned config."""
    pinned = LLMConfig(model="m", base_url="https://h:8443/v1", tls_pinned_cert=PIN)
    base = {"llm": dataclasses.asdict(pinned)}
    delta = {
        "llm": {
            "model": "p",
            "provider": "openrouter",
            "tls_pinned_cert": None,
            "base_url": None,
        }
    }
    merged = deep_merge(base, delta)
    parsed = _parse_llm_config(merged["llm"])
    assert parsed.tls_pinned_cert is None
    assert parsed.base_url is None
    assert parsed.model == "p"
