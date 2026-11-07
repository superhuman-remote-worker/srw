"""The citation model is called with its catalog row's API shape.

The citation model travels as flat ``CITATION_LLM_*`` env vars, which named no
client factory, so the verifier always built an OpenAI chat client: a row on
the ``anthropic-messages`` shape was sent chat-completions requests (vault
issue citation_verifier_ignores_model_api_shape). Dispatch now names the
Anthropic client for such a row, and only for the citation slot:
``EMBEDDING_PROVIDER`` already means local or openrouter.
"""

from __future__ import annotations

import os

import pytest

os.environ.setdefault("VECTOR_DB_URL", "postgresql://test@localhost/test")

from orchestrator.services import dispatch_credentials as dc  # noqa: E402
from shared.runtime.core.loader import LimitsConfig  # noqa: E402
from shared.runtime.core.transport_resolution import (  # noqa: E402
    clear_served_model_env,
)
from tests.test_citation_llm_credentials import (  # noqa: E402
    _agent,
    _init_with_captured_create_llm,
)
from tests.test_served_model_dispatch import _deps, _meta  # noqa: E402

ANTHROPIC_ONLY = "https://anthropic-only.example/v1"


async def _inject(env, prefix, meta):
    await dc.inject_env_key_credentials(
        env_keys=env,
        prefix=prefix,
        model_id=meta.model_id,
        user_id=None,
        resolved_keys={},
        dependencies=_deps(meta),
    )


@pytest.mark.asyncio
async def test_an_anthropic_shaped_citation_row_names_the_anthropic_client():
    env: dict = {}
    await _inject(env, "CITATION_LLM", _meta("muse-spark", provider="anthropic"))
    assert env["CITATION_LLM_PROVIDER"] == "anthropic"
    assert env["CITATION_LLM_BASE_URL"] == "https://proxy.example/v1"


@pytest.mark.asyncio
async def test_any_other_shape_names_none_and_drops_a_previous_one():
    env = {"CITATION_LLM_PROVIDER": "anthropic"}
    await _inject(env, "CITATION_LLM", _meta("gpt-cite", provider="openai"))
    assert "CITATION_LLM_PROVIDER" not in env


@pytest.mark.asyncio
async def test_only_the_citation_slot_is_given_a_provider():
    env = {"EMBEDDING_PROVIDER": "local"}
    await _inject(env, "EMBEDDING", _meta("embed", provider="anthropic"))
    assert env["EMBEDDING_PROVIDER"] == "local"


def test_the_verifier_is_built_with_the_dispatched_provider(monkeypatch):
    for name in ("CITATION_LLM_URL", "CITATION_LLM_SERVED_MODEL", "OPENAI_API_KEY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("CITATION_LLM_MODEL", "muse-spark")
    monkeypatch.setenv("CITATION_LLM_BASE_URL", ANTHROPIC_ONLY)
    monkeypatch.setenv("CITATION_LLM_API_KEY", "sk-cit")
    monkeypatch.setenv("CITATION_LLM_PROVIDER", "anthropic")

    created = _init_with_captured_create_llm(_agent())
    assert created[0].provider == "anthropic"

    agent = _agent()
    agent._initialize_citation_verifier(LimitsConfig())
    assert type(agent._citation_verify_aux.llm).__name__ == "ChatAnthropic"


def test_a_reused_pod_forgets_the_previous_rows_provider(monkeypatch):
    monkeypatch.setenv("CITATION_LLM_PROVIDER", "anthropic")
    clear_served_model_env()
    assert "CITATION_LLM_PROVIDER" not in os.environ
