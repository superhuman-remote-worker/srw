"""The dedicated citation client never borrows the chat model's key.

Regression tests for
knowledge-base/knowledge/issues/citation_llm_api_key_isolation.md, at the
agent call site (``UniversalAgent._initialize_citation_verifier``):

- ``CITATION_LLM_API_KEY`` is the key the citation client is built with.
- A custom ``CITATION_LLM_URL`` without a dedicated key must NOT fall back to
  ``OPENAI_API_KEY`` — no client is built for that endpoint at all; the
  verifier degrades to the auxiliary model and logs a configuration error.
- With no citation endpoint configured the client keeps today's behaviour
  (api.openai.com with ``OPENAI_API_KEY``).
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from agent.agent import UniversalAgent
from shared.runtime.core.loader import LimitsConfig, load_agent_config

CITATION_ENV = (
    "CITATION_LLM_MODEL",
    "CITATION_LLM_SERVED_MODEL",
    "CITATION_LLM_BASE_URL",
    "CITATION_LLM_URL",
    "CITATION_LLM_API_KEY",
    "OPENAI_API_KEY",
)
CUSTOM_URL = "http://cite.internal:8080/v1"


@pytest.fixture(autouse=True)
def _clean_citation_env(monkeypatch):
    # A developer .env can leak into pytest; start from nothing.
    for var in CITATION_ENV:
        monkeypatch.delenv(var, raising=False)


def _agent() -> UniversalAgent:
    agent = UniversalAgent.__new__(UniversalAgent)
    cfg = load_agent_config("config/worker_base.yaml")
    cfg.auxiliary.enabled = True
    cfg.auxiliary.tasks["verify_citations"].enabled = True
    agent.config = cfg
    agent._summarization_llm = MagicMock(name="summarization_llm")
    agent._auxiliary_llm = MagicMock(name="auxiliary_llm")
    agent._citation_verify_aux = None
    agent._citation_verification_prompt = ""
    return agent


def _init_with_captured_create_llm(agent: UniversalAgent):
    """Run the verifier init with ``create_llm`` replaced by a recorder."""
    created = []

    def fake_create_llm(cfg, limits=None):
        created.append(cfg)
        return SimpleNamespace(model=cfg.model, cfg=cfg)

    with patch("agent.agent.create_llm", side_effect=fake_create_llm):
        agent._initialize_citation_verifier(LimitsConfig())
    return created


class TestDedicatedKeyPrecedence:
    def test_dedicated_key_is_the_citation_client_key(self, monkeypatch):
        monkeypatch.setenv("CITATION_LLM_MODEL", "cite-model")
        monkeypatch.setenv("CITATION_LLM_URL", CUSTOM_URL)
        monkeypatch.setenv("CITATION_LLM_API_KEY", "sk-cit")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-openai")
        agent = _agent()

        created = _init_with_captured_create_llm(agent)

        assert len(created) == 1
        assert created[0].model == "cite-model"
        assert created[0].base_url == CUSTOM_URL
        assert created[0].api_key == "sk-cit"
        # A dedicated verifier was built (not the auxiliary fallback).
        assert agent._citation_verify_aux is not agent._auxiliary_llm
        assert agent._citation_verify_aux.llm.cfg is created[0]

    def test_real_client_carries_dedicated_key_and_endpoint(self, monkeypatch):
        # End-to-end through the real create_llm: the constructed ChatOpenAI
        # client is keyed with the citation key and pointed at the citation URL.
        monkeypatch.setenv("CITATION_LLM_MODEL", "cite-model")
        monkeypatch.setenv("CITATION_LLM_BASE_URL", CUSTOM_URL)
        monkeypatch.setenv("CITATION_LLM_API_KEY", "sk-cit")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-openai")
        agent = _agent()

        agent._initialize_citation_verifier(LimitsConfig())

        client = agent._citation_verify_aux.llm
        assert client.openai_api_base == CUSTOM_URL
        assert client.openai_api_key.get_secret_value() == "sk-cit"


class TestServedModel:
    """The catalog row's upstream string rides with CITATION_LLM_MODEL."""

    def test_served_model_is_threaded_into_the_citation_config(self, monkeypatch):
        monkeypatch.setenv("CITATION_LLM_MODEL", "cite-model-eu")
        monkeypatch.setenv("CITATION_LLM_SERVED_MODEL", "cite-model")
        monkeypatch.setenv("CITATION_LLM_URL", CUSTOM_URL)
        monkeypatch.setenv("CITATION_LLM_API_KEY", "sk-cit")

        created = _init_with_captured_create_llm(_agent())

        assert created[0].model == "cite-model-eu"
        assert created[0].served_model == "cite-model"

    def test_real_client_sends_the_served_string(self, monkeypatch):
        monkeypatch.setenv("CITATION_LLM_MODEL", "cite-model-eu")
        monkeypatch.setenv("CITATION_LLM_SERVED_MODEL", "cite-model")
        monkeypatch.setenv("CITATION_LLM_BASE_URL", CUSTOM_URL)
        monkeypatch.setenv("CITATION_LLM_API_KEY", "sk-cit")
        agent = _agent()

        agent._initialize_citation_verifier(LimitsConfig())

        assert agent._citation_verify_aux.llm.model_name == "cite-model"

    def test_empty_served_model_sends_the_catalog_id(self, monkeypatch):
        monkeypatch.setenv("CITATION_LLM_MODEL", "cite-model")
        monkeypatch.setenv("CITATION_LLM_SERVED_MODEL", "")
        monkeypatch.setenv("CITATION_LLM_URL", CUSTOM_URL)
        monkeypatch.setenv("CITATION_LLM_API_KEY", "sk-cit")

        created = _init_with_captured_create_llm(_agent())

        assert created[0].served_model is None


class TestNoLeakToCustomEndpoint:
    def test_custom_url_without_dedicated_key_builds_no_client(
        self, monkeypatch, caplog
    ):
        monkeypatch.setenv("CITATION_LLM_MODEL", "cite-model")
        monkeypatch.setenv("CITATION_LLM_URL", CUSTOM_URL)
        monkeypatch.setenv("OPENAI_API_KEY", "sk-openai")
        agent = _agent()

        with caplog.at_level(logging.ERROR, logger="agent.agent"):
            created = _init_with_captured_create_llm(agent)

        # No client was constructed for the custom endpoint, so OPENAI_API_KEY
        # cannot have been handed to it.
        assert created == []
        assert agent._citation_verify_aux is agent._auxiliary_llm
        errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert errors, "misconfiguration must be logged as an error"
        assert "CITATION_LLM_API_KEY" in errors[0].getMessage()
        assert "sk-openai" not in errors[0].getMessage()

    def test_empty_dedicated_key_counts_as_unset(self, monkeypatch):
        monkeypatch.setenv("CITATION_LLM_MODEL", "cite-model")
        monkeypatch.setenv("CITATION_LLM_URL", CUSTOM_URL)
        monkeypatch.setenv("CITATION_LLM_API_KEY", "")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-openai")
        agent = _agent()

        created = _init_with_captured_create_llm(agent)

        assert created == []
        assert agent._citation_verify_aux is agent._auxiliary_llm

    def test_real_path_never_constructs_a_client_for_the_custom_endpoint(
        self, monkeypatch
    ):
        # Same guarantee without any patching: the loader's own
        # `config.api_key or OPENAI_API_KEY` fallback is never reached.
        monkeypatch.setenv("CITATION_LLM_MODEL", "cite-model")
        monkeypatch.setenv("CITATION_LLM_URL", CUSTOM_URL)
        monkeypatch.setenv("OPENAI_API_KEY", "sk-openai")
        agent = _agent()

        agent._initialize_citation_verifier(LimitsConfig())

        assert agent._citation_verify_aux is agent._auxiliary_llm


class TestDefaultsUnchanged:
    def test_no_citation_endpoint_keeps_openai_default(self, monkeypatch):
        monkeypatch.setenv("CITATION_LLM_MODEL", "gpt-4o-mini")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-openai")
        agent = _agent()

        created = _init_with_captured_create_llm(agent)

        assert len(created) == 1
        assert created[0].base_url is None
        assert created[0].api_key == "sk-openai"
        assert agent._citation_verify_aux is not agent._auxiliary_llm

    def test_openai_default_url_keeps_openai_key(self, monkeypatch):
        monkeypatch.setenv("CITATION_LLM_MODEL", "gpt-4o-mini")
        monkeypatch.setenv("CITATION_LLM_URL", "https://api.openai.com/v1")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-openai")
        agent = _agent()

        created = _init_with_captured_create_llm(agent)

        assert len(created) == 1
        assert created[0].base_url == "https://api.openai.com/v1"
        assert created[0].api_key == "sk-openai"

    def test_no_citation_model_reuses_auxiliary(self):
        agent = _agent()

        created = _init_with_captured_create_llm(agent)

        assert created == []
        assert agent._citation_verify_aux is agent._auxiliary_llm
