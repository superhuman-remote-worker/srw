"""Arm (config-variant) loading for the memory eval harness.

An *arm* is one system configuration under test: a base agent config
(usually persistent_defaults), optional deep-merged overrides into the
raw YAML (so the override surface is exactly the production config
surface — memory.pipeline lists, observer_interval, …), auxiliary-LLM
routing for the extraction writers, and ingestion-mode knobs.

Config resolution mirrors the production expert reload
(src/api/persistent_app._load_expert_config): resolve_config_path →
load_and_merge_config ($extends) → deep_merge(arm overrides) →
settings matrix → load_agent_config_from_dict. What the arm runs is
therefore exactly what a session/job with that YAML would run.
"""

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)

INGESTION_MODES = ("seam", "verbatim")


@dataclass
class IngestionOptions:
    """How haystack sessions become memories.

    seam: production-faithful — replay each session through
        MemoryManager.capture() (turn_end per round + session_end), with
        the per-turn read path (assemble()) on by default so access
        stats accrue exactly as in a live session.
    verbatim: no extraction LLM — each user-assistant round is stored
        directly via RecallStore.store(), making question-time retrieval
        pure hybrid search
        over a flat round-granularity index. This is the cheap smoke arm
        AND the published-baseline reproduction arm (LongMemEval round
        granularity).
    """

    mode: str = "seam"
    #: seam mode: run assemble() before each assistant turn like the live
    #: persistent loop does (drives TTL decrement + access-reset dynamics).
    read_path_per_turn: bool = True
    #: Prefix stored content with the session date (verbatim mode). Off by
    #: default: production memories carry no dates either.
    date_prefix: bool = False
    #: verbatim mode: importance for stored rounds — must clear the read
    #: floor (retrieval_importance_floor, default 0.4).
    verbatim_importance: float = 0.5

    def __post_init__(self) -> None:
        if self.mode not in INGESTION_MODES:
            raise ValueError(f"ingestion.mode '{self.mode}' not in {INGESTION_MODES}")


@dataclass
class ArmSpec:
    """One fully-described eval arm, loaded from eval/memory/arms/*.yaml."""

    name: str
    description: str = ""
    base_config: str = "session_base"
    config_overrides: Dict[str, Any] = field(default_factory=dict)
    auxiliary: Dict[str, Any] = field(default_factory=dict)
    ingestion: IngestionOptions = field(default_factory=IngestionOptions)
    ks: List[int] = field(default_factory=lambda: [1, 3, 5, 10])
    #: Override the matrix-resolved extraction prompt with a file (path
    #: relative to the repo root) — the prompt-variant A/B knob.
    extraction_prompt_file: Optional[str] = None

    @classmethod
    def from_file(cls, path: str) -> "ArmSpec":
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        query = data.get("query") or {}
        return cls(
            name=data.get("name") or Path(path).stem,
            description=data.get("description", ""),
            base_config=data.get("base_config", "session_base"),
            config_overrides=data.get("config_overrides") or {},
            auxiliary=data.get("auxiliary") or {},
            ingestion=IngestionOptions(**(data.get("ingestion") or {})),
            ks=[int(k) for k in query.get("ks", [1, 3, 5, 10])],
            extraction_prompt_file=data.get("extraction_prompt_file"),
        )


def build_agent_config(arm: ArmSpec) -> Any:
    """Resolve the arm into a parsed AgentConfig (production loader path).

    The harness requires the seam: memory.enabled + memory.manager.enabled
    are forced on after the merge (an arm explicitly disabling memory has
    nothing to measure), and seam-mode arms must bind at least one writer
    or ingestion would be a silent no-op.
    """
    from shared.runtime.core.loader import (
        _apply_settings_matrix,
        deep_merge,
        load_agent_config_from_dict,
        load_and_merge_config,
        resolve_config_path,
    )

    config_path, deployment_dir = resolve_config_path(arm.base_config)
    merged = load_and_merge_config(config_path)
    if arm.config_overrides:
        merged = deep_merge(merged, arm.config_overrides)

    memory_section = merged.setdefault("memory", {})
    memory_section["enabled"] = True
    memory_section.setdefault("manager", {})["enabled"] = True

    raw_llm_keys: set = set()
    try:
        raw_cfg = yaml.safe_load(Path(config_path).read_text(encoding="utf-8")) or {}
        raw_llm_keys = set((raw_cfg.get("llm") or {}).keys())
    except Exception:
        pass
    _apply_settings_matrix(merged, raw_llm_keys, deployment_dir)
    config = load_agent_config_from_dict(merged, deployment_dir=deployment_dir)

    _apply_auxiliary_overrides(config, arm.auxiliary)

    if arm.ingestion.mode == "seam" and not config.memory.pipeline.writers:
        raise ValueError(
            f"arm '{arm.name}': seam ingestion with an empty "
            "memory.pipeline.writers list would store nothing"
        )
    return config


def _apply_auxiliary_overrides(config: Any, overrides: Dict[str, Any]) -> None:
    """Apply the arm's auxiliary-LLM routing onto the parsed config.

    ``api_key_env`` names an environment variable (the key itself never
    lives in a committed arm file); the remaining keys map 1:1 onto
    AuxiliaryConfig fields.
    """
    if not overrides:
        return
    api_key_env: Optional[str] = overrides.get("api_key_env")
    if api_key_env:
        api_key = os.environ.get(api_key_env)
        if not api_key:
            raise ValueError(
                f"auxiliary.api_key_env={api_key_env} is set but the "
                "environment variable is empty — extraction calls would 401"
            )
        config.auxiliary.api_key = api_key
    for key in ("model", "base_url", "temperature", "max_iterations", "timeout"):
        if key in overrides:
            setattr(config.auxiliary, key, overrides[key])
    logger.info(
        "Arm auxiliary override: model=%s base_url=%s",
        config.auxiliary.model,
        config.auxiliary.base_url or "default",
    )
