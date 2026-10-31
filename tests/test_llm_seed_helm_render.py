"""Render-level contracts for Helm's LLM seed payload.

The seeder accepts both the preferred ``capabilities`` array and the legacy
singular ``capability`` shorthand.  These tests render the ConfigMap consumed
by the real seed Job so a template that silently drops either spelling cannot
pass by testing only Python-side parsing.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

from orchestrator.schemas.provider_catalog import VALID_DEFAULT_MODEL_KINDS

ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm"
SEED_CONFIGMAP = "templates/orchestrator/llm-seed-configmap.yaml"

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="Helm is not installed"
)


SEED_JOB = "templates/orchestrator/llm-seed-job.yaml"
SEED_INLINE_SECRET = "templates/orchestrator/llm-seed-inline-secret.yaml"


def _render(
    tmp_path: Path,
    seed: dict,
    *,
    show_only: str = SEED_CONFIGMAP,
    extra_values: dict | None = None,
) -> subprocess.CompletedProcess:
    values = tmp_path / "llm-seed-values.yaml"
    merged = {"llm": {"seed": {"enabled": True, **seed}}}
    merged.update(extra_values or {})
    values.write_text(yaml.safe_dump(merged), encoding="utf-8")
    return subprocess.run(
        [
            "helm",
            "template",
            "llm-seed-test",
            str(CHART),
            "-f",
            str(CHART / "ci/test-values.yaml"),
            "-f",
            str(values),
            "--show-only",
            show_only,
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )


def _render_seed(tmp_path: Path, seed: dict) -> subprocess.CompletedProcess:
    return _render(tmp_path, seed)


def _render_docs(tmp_path: Path, seed: dict, **kw) -> list[dict]:
    result = _render(tmp_path, seed, **kw)
    assert result.returncode == 0, f"helm template failed:\n{result.stderr}"
    return [document for document in yaml.safe_load_all(result.stdout) if document]


def _render_seed_payload(tmp_path: Path, seed: dict) -> dict:
    result = _render_seed(tmp_path, seed)
    assert result.returncode == 0, f"helm template failed:\n{result.stderr}"

    documents = [document for document in yaml.safe_load_all(result.stdout) if document]
    assert len(documents) == 1
    return yaml.safe_load(documents[0]["data"]["llm.yaml"])


def test_endpoint_models_render_e2e_capabilities_and_legacy_fallback(
    tmp_path: Path,
) -> None:
    payload = _render_seed_payload(
        tmp_path,
        {
            "systemEndpoints": [
                {
                    "label": "E2E deterministic provider",
                    "baseUrl": "http://e2e-provider:8000/v1",
                    "models": [
                        {
                            "id": "e2e-chat",
                            "displayName": "E2E Chat",
                            "capabilities": ["chat", "auxiliary"],
                            "multimodal": True,
                        },
                        {
                            "id": "e2e-embedding",
                            "displayName": "E2E Embedding",
                            "capability": "embedding",
                        },
                    ],
                }
            ]
        },
    )

    models = payload["systemEndpoints"][0]["models"]
    assert models[0]["id"] == "e2e-chat"
    assert models[0]["capabilities"] == ["chat", "auxiliary"]
    assert models[0]["multimodal"] is True
    assert "capability" not in models[0]
    assert models[1]["id"] == "e2e-embedding"
    assert models[1]["capability"] == "embedding"
    assert "capabilities" not in models[1]


def test_system_models_render_capability_arrays(tmp_path: Path) -> None:
    payload = _render_seed_payload(
        tmp_path,
        {
            "systemModels": [
                {
                    "provider": "fixture",
                    "id": "fixture-chat",
                    "capabilities": ["chat", "auxiliary"],
                    "multimodal": False,
                }
            ]
        },
    )

    model = payload["systemModels"][0]
    assert model["capabilities"] == ["chat", "auxiliary"]
    assert model["multimodal"] is False
    assert "capability" not in model


def test_defaults_render_as_kind_to_model_map(tmp_path: Path) -> None:
    payload = _render_seed_payload(
        tmp_path,
        {
            "defaults": {
                "chat": "e2e-chat",
                "embedding": "e2e-embedding",
                "search_fallback": "searxng",
                "vision": "",  # empty = not declared; must not render
            }
        },
    )

    assert payload["defaults"] == {
        "chat": "e2e-chat",
        "embedding": "e2e-embedding",
        "search_fallback": "searxng",
    }


def test_defaults_default_to_an_empty_section(tmp_path: Path) -> None:
    payload = _render_seed_payload(tmp_path, {"systemModels": []})
    # `defaults: {}` renders a bare key; the seeder treats None as "nothing".
    assert payload.get("defaults") in (None, {})


def test_defaults_unknown_kind_fails_the_render(tmp_path: Path) -> None:
    result = _render_seed(tmp_path, {"defaults": {"chatt": "e2e-chat"}})
    assert result.returncode != 0
    assert "llm.seed.defaults: unknown kind" in result.stderr
    assert "chatt" in result.stderr


def test_chart_kind_list_matches_admin_schema() -> None:
    # The template carries its own copy of the kind list so a typo fails at
    # render time; keep it identical to what Admin → Models → Defaults accepts.
    template = (CHART / SEED_CONFIGMAP).read_text(encoding="utf-8")
    match = re.search(r'\$validKinds := list ((?:"[a-z_]+"\s*)+)', template)
    assert match, "kind list not found in the seed ConfigMap template"
    chart_kinds = set(re.findall(r'"([a-z_]+)"', match.group(1)))
    assert chart_kinds == VALID_DEFAULT_MODEL_KINDS


# ---------------------------------------------------------------------------
# params per catalog row
# ---------------------------------------------------------------------------


def test_params_render_for_endpoint_and_system_models(tmp_path: Path) -> None:
    payload = _render_seed_payload(
        tmp_path,
        {
            "systemEndpoints": [
                {
                    "label": "MiniMax",
                    "baseUrl": "https://api.minimax.io/v1",
                    "models": [
                        {
                            "id": "MiniMax-M3",
                            "capabilities": ["chat"],
                            "params": {"pricing_id": "minimax/m3", "temperature": 0.7},
                        }
                    ],
                }
            ],
            "systemModels": [
                {
                    "provider": "openai",
                    "id": "gpt-5-mini",
                    "capability": "chat",
                    "params": {"reasoning_effort": "low", "nested": {"a": 1}},
                }
            ],
        },
    )
    assert payload["systemEndpoints"][0]["models"][0]["params"] == {
        "pricing_id": "minimax/m3",
        "temperature": 0.7,
    }
    assert payload["systemModels"][0]["params"] == {
        "reasoning_effort": "low",
        "nested": {"a": 1},
    }


def test_params_must_be_a_mapping(tmp_path: Path) -> None:
    result = _render_seed(
        tmp_path,
        {"systemModels": [{"provider": "openai", "id": "x", "params": "low"}]},
    )
    assert result.returncode != 0
    assert "params must be a mapping" in result.stderr


# ---------------------------------------------------------------------------
# inline dev-mode key values
# ---------------------------------------------------------------------------

_INLINE_SEED = {
    "systemApiKeys": {
        "openai": {"value": "sk-inline-openai"},
        "anthropic": {"secretName": "srw-llm-seed", "key": "anthropic_api_key"},
    },
    "systemEndpoints": [
        {
            "label": "Keyed by ref",
            "baseUrl": "http://a:8000/v1",
            "apiKeyRef": {"secretName": "srw-llm-seed", "key": "a_key"},
            "models": [],
        },
        {
            "label": "Inline",
            "baseUrl": "http://b:8000/v1",
            "apiKey": "sk-inline-endpoint",
            "models": [{"id": "b-chat", "capability": "chat"}],
        },
        {"label": "Keyless", "baseUrl": "http://c:8000/v1", "models": []},
    ],
}
_DEV_SECRETS = {"secrets": {"create": True, "values": {}}}


def test_inline_values_render_one_dedicated_secret(tmp_path: Path) -> None:
    docs = _render_docs(
        tmp_path, _INLINE_SEED, show_only=SEED_INLINE_SECRET, extra_values=_DEV_SECRETS
    )
    assert len(docs) == 1
    secret = docs[0]
    assert secret["kind"] == "Secret"
    assert secret["metadata"]["name"].endswith("-llm-seed-inline")
    assert secret["stringData"] == {
        "openai": "sk-inline-openai",
        "endpoint-1": "sk-inline-endpoint",
    }
    # A regular resource, applied with the release ahead of the hook Job.
    assert "helm.sh/hook" not in (secret["metadata"].get("annotations") or {})


def test_inline_values_wire_the_job_env_to_that_secret(tmp_path: Path) -> None:
    docs = _render_docs(
        tmp_path, _INLINE_SEED, show_only=SEED_JOB, extra_values=_DEV_SECRETS
    )
    job = next(d for d in docs if d["kind"] == "Job")
    seed = next(c for c in job["spec"]["template"]["spec"]["containers"])
    refs = {
        e["name"]: e["valueFrom"]["secretKeyRef"]
        for e in seed["env"]
        if e["name"].startswith("SEED_")
    }
    inline = refs["SEED_OPENAI_API_KEY"]["name"]
    assert inline.endswith("-llm-seed-inline")
    assert refs["SEED_OPENAI_API_KEY"]["key"] == "openai"
    assert refs["SEED_ANTHROPIC_API_KEY"]["name"] == "srw-llm-seed"
    assert refs["SEED_ANTHROPIC_API_KEY"]["key"] == "anthropic_api_key"
    assert refs["SEED_ENDPOINT_0_API_KEY"]["name"] == "srw-llm-seed"
    assert refs["SEED_ENDPOINT_1_API_KEY"]["name"] == inline
    assert refs["SEED_ENDPOINT_1_API_KEY"]["key"] == "endpoint-1"
    assert "SEED_ENDPOINT_2_API_KEY" not in refs
    assert all(ref["optional"] is True for ref in refs.values())


def test_inline_endpoint_key_sets_api_key_env_in_payload(tmp_path: Path) -> None:
    docs = _render_docs(tmp_path, _INLINE_SEED, extra_values=_DEV_SECRETS)
    payload = yaml.safe_load(docs[0]["data"]["llm.yaml"])
    by_label = {ep["label"]: ep for ep in payload["systemEndpoints"]}
    assert by_label["Keyed by ref"]["apiKeyEnv"] == "SEED_ENDPOINT_0_API_KEY"
    assert by_label["Inline"]["apiKeyEnv"] == "SEED_ENDPOINT_1_API_KEY"
    assert "apiKeyEnv" not in by_label["Keyless"]


def test_no_inline_values_renders_no_secret(tmp_path: Path) -> None:
    result = _render(
        tmp_path,
        {"systemApiKeys": {"openai": {"secretName": "s", "key": "k"}}},
        show_only=SEED_INLINE_SECRET,
        extra_values=_DEV_SECRETS,
    )
    # helm exits non-zero when --show-only matches an empty template.
    assert "kind: Secret" not in result.stdout


def test_inline_values_are_refused_outside_dev_secret_mode(tmp_path: Path) -> None:
    result = _render(
        tmp_path,
        {"systemApiKeys": {"openai": {"value": "sk-x"}}},
        show_only=SEED_INLINE_SECRET,
        extra_values={"secrets": {"create": False}},
    )
    assert result.returncode != 0
    assert "secrets.create=true" in result.stderr


def test_inline_and_ref_on_the_same_entry_is_refused(tmp_path: Path) -> None:
    result = _render(
        tmp_path,
        {"systemApiKeys": {"openai": {"value": "sk-x", "secretName": "s", "key": "k"}}},
        show_only=SEED_INLINE_SECRET,
        extra_values=_DEV_SECRETS,
    )
    assert result.returncode != 0
    assert "not both" in result.stderr

    result = _render(
        tmp_path,
        {
            "systemEndpoints": [
                {
                    "label": "Both",
                    "baseUrl": "http://x/v1",
                    "apiKey": "sk-x",
                    "apiKeyRef": {"secretName": "s", "key": "k"},
                    "models": [],
                }
            ]
        },
        show_only=SEED_INLINE_SECRET,
        extra_values=_DEV_SECRETS,
    )
    assert result.returncode != 0
    assert "not both" in result.stderr


# ---------------------------------------------------------------------------
# reconcile flags
# ---------------------------------------------------------------------------


def test_reconcile_flags_render_through(tmp_path: Path) -> None:
    payload = _render_seed_payload(
        tmp_path,
        {
            "systemApiKeys": {
                "openai": {
                    "secretName": "s",
                    "key": "k",
                    "label": "Main",
                    "reconcile": True,
                },
                "anthropic": {"secretName": "s", "key": "a"},
            },
            "systemEndpoints": [
                {
                    "label": "Gemma",
                    "baseUrl": "http://g/v1",
                    "reconcile": True,
                    "models": [
                        {"id": "gemma", "capability": "chat", "reconcile": True},
                        {"id": "gemma-emb", "capability": "embedding"},
                    ],
                }
            ],
            "systemModels": [
                {
                    "provider": "openai",
                    "id": "gpt-5-mini",
                    "capability": "chat",
                    "reconcile": False,
                }
            ],
            "defaults": {
                "chat": {"model": "gemma", "reconcile": True},
                "embedding": {"model": "gemma-emb"},
                "vision": "gemma",
            },
        },
    )
    keys = {k["provider"]: k for k in payload["systemApiKeys"]}
    assert keys["openai"]["reconcile"] is True
    assert keys["openai"]["label"] == "Main"
    assert "reconcile" not in keys["anthropic"]
    ep = payload["systemEndpoints"][0]
    assert ep["reconcile"] is True
    assert ep["models"][0]["reconcile"] is True
    assert "reconcile" not in ep["models"][1]
    assert payload["systemModels"][0]["reconcile"] is False
    assert payload["defaults"] == {
        "chat": {"model": "gemma", "reconcile": True},
        "embedding": {"model": "gemma-emb"},
        "vision": "gemma",
    }


def test_defaults_mapping_form_without_model_is_dropped(tmp_path: Path) -> None:
    payload = _render_seed_payload(
        tmp_path, {"defaults": {"chat": {"reconcile": True}, "vision": {"model": ""}}}
    )
    assert payload.get("defaults") in (None, {})


def test_served_model_id_renders_for_endpoint_and_system_models(tmp_path: Path) -> None:
    # A model id names one catalog row; `servedModelId` is how a second
    # provider's row sends the same name upstream under its own id.
    payload = _render_seed_payload(
        tmp_path,
        {
            "systemEndpoints": [
                {
                    "label": "Proxy",
                    "baseUrl": "https://proxy.example/v1",
                    "models": [
                        {
                            "id": "claude-opus-5-5-proxy",
                            "servedModelId": "claude-opus-5-5",
                            "capabilities": ["chat"],
                        },
                        {"id": "plain", "capabilities": ["chat"]},
                    ],
                }
            ],
            "systemModels": [
                {
                    "provider": "anthropic",
                    "id": "claude-opus-5-5-api",
                    "servedModelId": "claude-opus-5-5",
                    "capabilities": ["chat"],
                }
            ],
        },
    )
    endpoint_models = payload["systemEndpoints"][0]["models"]
    assert endpoint_models[0]["servedModelId"] == "claude-opus-5-5"
    assert "servedModelId" not in endpoint_models[1]
    assert payload["systemModels"][0]["servedModelId"] == "claude-opus-5-5"
