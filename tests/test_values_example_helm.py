"""helm/values.example.yaml, the production starter overlay, stays current.

helm/README.md tells operators to copy it as their starting point, so it must
render once the license is accepted and must seed models the readiness gate
accepts: the one-key OpenRouter set (4096-dim embedding plus a reranker).
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "helm/values.example.yaml"
REMOVED_LLM_KEYS = {"baseUrl", "visionModel", "embeddingModel", "embeddingBaseUrl"}


def test_the_example_uses_the_seed_and_no_removed_llm_keys():
    llm = yaml.safe_load(EXAMPLE.read_text())["llm"]
    assert not REMOVED_LLM_KEYS & set(llm)
    seed = llm["seed"]
    assert seed["systemApiKeys"]["openrouter"]["secretName"]
    assert seed["defaults"] == {
        "chat": "openrouter/anthropic/claude-sonnet-5.5",
        "auxiliary": "openrouter/anthropic/claude-sonnet-5.5",
        "embedding": "openrouter/qwen/qwen3-embedding-8b",
        "rerank": "openrouter/qwen/qwen3-reranker-8b",
    }
    seeded = {model["id"] for model in seed["systemModels"]}
    assert set(seed["defaults"].values()) <= seeded


@pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is not installed")
def test_the_example_renders_once_the_license_is_accepted():
    result = subprocess.run(
        [
            "helm",
            "template",
            "srw",
            str(ROOT / "helm"),
            "-n",
            "srw",
            "-f",
            str(EXAMPLE),
            "--set",
            "license.acceptTerms=true",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
