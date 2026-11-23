"""The pinned-certificate env names travel, scrub and resolve like the URL."""
# ruff: noqa: F811  (the imported fixture is requested by name as a test argument)

import os

import pytest

from agent.api import session_attach
from shared.runtime.core.transport_resolution import (
    DISPATCH_ENV_KEY_NAMES,
    CitationTransportError,
    clear_replaced_endpoint_pins,
    exportable_env_keys,
    resolve_citation_transport,
)
from shared.runtime.services import embedding_service as emb
from shared.tls_pinning import UNREADABLE_PIN, TlsPin
from tests.tls_pin_fixtures import tls_certs  # noqa: F401

PREFIXES = (
    "EMBEDDING",
    "KB_EMBEDDING",
    "RERANK",
    "VISION",
    "WHISPER",
    "TTS",
    "CITATION_LLM",
)


@pytest.fixture
def pin(tls_certs):
    return TlsPin.from_pem(tls_certs.leaf.read_text())


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for prefix in PREFIXES:
        monkeypatch.delenv(f"{prefix}_TLS_PINNED_CERT", raising=False)
    yield
    for prefix in PREFIXES:
        os.environ.pop(f"{prefix}_TLS_PINNED_CERT", None)


def test_whitelist_names_every_prefix_and_still_drops_unknown(pin):
    for prefix in PREFIXES:
        assert f"{prefix}_TLS_PINNED_CERT" in DISPATCH_ENV_KEY_NAMES
    kept, dropped = exportable_env_keys(
        {"EMBEDDING_TLS_PINNED_CERT": pin.wire, "RANDOM_SDK_VAR": "x"}
    )
    assert kept == {"EMBEDDING_TLS_PINNED_CERT": pin.wire}
    assert dropped == ["RANDOM_SDK_VAR"]


@pytest.mark.parametrize("spelling", ["CITATION_LLM_BASE_URL", "CITATION_LLM_URL"])
def test_citation_transport_carries_its_pin(pin, spelling):
    transport = resolve_citation_transport(
        {
            spelling: "https://cite.internal/v1",
            "CITATION_LLM_API_KEY": "k",
            "CITATION_LLM_TLS_PINNED_CERT": pin.wire,
        }
    )
    assert transport.tls_pin == pin


def test_citation_transport_without_pin_has_none():
    transport = resolve_citation_transport({"CITATION_LLM_API_KEY": "k"})
    assert transport.tls_pin is None


@pytest.mark.parametrize("value", [UNREADABLE_PIN, "not-base64!"])
def test_citation_transport_refuses_an_unreadable_pin(value):
    with pytest.raises(CitationTransportError, match="CITATION_LLM_TLS_PINNED_CERT"):
        resolve_citation_transport(
            {
                "CITATION_LLM_BASE_URL": "https://cite.internal/v1",
                "CITATION_LLM_API_KEY": "k",
                "CITATION_LLM_TLS_PINNED_CERT": value,
            }
        )


def test_memory_embedding_pin_is_scrubbed_on_the_next_attach(pin):
    session_attach.apply_session_embedding_env(
        {
            "EMBEDDING_BASE_URL": "https://a.example",
            "EMBEDDING_TLS_PINNED_CERT": pin.wire,
            "RERANK_BASE_URL": "https://a-rerank.example",
            "RERANK_TLS_PINNED_CERT": pin.wire,
            "KB_EMBEDDING_TLS_PINNED_CERT": pin.wire,
        }
    )
    assert os.environ["EMBEDDING_TLS_PINNED_CERT"] == pin.wire
    assert os.environ["RERANK_TLS_PINNED_CERT"] == pin.wire
    assert os.environ["KB_EMBEDDING_TLS_PINNED_CERT"] == pin.wire
    session_attach.apply_session_embedding_env(
        {"EMBEDDING_BASE_URL": "https://b.example"}
    )
    for name in (
        "EMBEDDING_TLS_PINNED_CERT",
        "RERANK_TLS_PINNED_CERT",
        "KB_EMBEDDING_TLS_PINNED_CERT",
    ):
        assert name not in os.environ


def test_kb_embedding_pin_is_scrubbed_by_the_kb_apply(pin):
    emb.apply_kb_embedding_env({"KB_EMBEDDING_TLS_PINNED_CERT": pin.wire})
    assert os.environ["KB_EMBEDDING_TLS_PINNED_CERT"] == pin.wire
    emb.apply_kb_embedding_env({"KB_EMBEDDING_BASE_URL": "https://b.example"})
    assert "KB_EMBEDDING_TLS_PINNED_CERT" not in os.environ
    emb.apply_kb_embedding_env(None)
    assert "KB_EMBEDDING_BASE_URL" not in os.environ


def test_whisper_pin_is_scrubbed_by_the_whisper_override(pin):
    session_attach.apply_session_whisper_env(
        {"WHISPER_BASE_URL": "https://w.example", "WHISPER_TLS_PINNED_CERT": pin.wire}
    )
    assert os.environ["WHISPER_TLS_PINNED_CERT"] == pin.wire
    session_attach.apply_session_whisper_env({"WHISPER_BASE_URL": "https://x.example"})
    assert "WHISPER_TLS_PINNED_CERT" not in os.environ


def test_whisper_signature_notices_a_pin_change(pin, monkeypatch):
    from agent.services import audio_helper

    before = audio_helper._whisper_env_signature()
    monkeypatch.setenv("WHISPER_TLS_PINNED_CERT", pin.wire)
    assert audio_helper._whisper_env_signature() != before


# ---------------------------------------------------------------------------
# A reused pod: a block that replaces an endpoint replaces its pin too
# ---------------------------------------------------------------------------


@pytest.fixture
def scratch_env(monkeypatch):
    """Every dispatch env name unset, and restored after the test whatever the
    code under test sets (monkeypatch.delenv of an unset name restores nothing)."""
    saved = {name: os.environ.get(name) for name in DISPATCH_ENV_KEY_NAMES}
    for name in DISPATCH_ENV_KEY_NAMES:
        os.environ.pop(name, None)
    yield monkeypatch
    for name, value in saved.items():
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value


# The prefixes a job block exports name by name (KB_EMBEDDING is replaced as
# a whole by apply_kb_embedding_env).
EXPORTED = ("EMBEDDING", "RERANK", "VISION", "WHISPER", "TTS", "CITATION_LLM")


def _url_name(prefix):
    return "CITATION_LLM_BASE_URL" if prefix == "CITATION_LLM" else f"{prefix}_BASE_URL"


def _pinned_block(prefix, pin, host="a.example"):
    return {
        _url_name(prefix): f"https://{host}/v1",
        f"{prefix}_API_KEY": "key-a",
        f"{prefix}_TLS_PINNED_CERT": pin.wire,
    }


def _unpinned_block(prefix, host="b.example"):
    return {_url_name(prefix): f"https://{host}/v1", f"{prefix}_API_KEY": "key-b"}


def test_a_replaced_endpoint_loses_its_pin(scratch_env, pin):
    scratch_env.setenv("EMBEDDING_BASE_URL", "https://a.example/v1")
    scratch_env.setenv("EMBEDDING_TLS_PINNED_CERT", pin.wire)
    clear_replaced_endpoint_pins({"EMBEDDING_BASE_URL": "https://b.example/v1"})
    assert "EMBEDDING_TLS_PINNED_CERT" not in os.environ


@pytest.mark.parametrize(
    "block",
    [
        {},
        {"VISION_BASE_URL": "https://b.example/v1"},
        # A key alone does not move the URL: the old URL stays, with its pin.
        {"EMBEDDING_API_KEY": "key-b", "EMBEDDING_MODEL": "m"},
        # None is never applied.
        {"EMBEDDING_BASE_URL": None},
    ],
)
def test_a_block_that_keeps_the_url_keeps_its_pin(scratch_env, pin, block):
    scratch_env.setenv("EMBEDDING_BASE_URL", "https://a.example/v1")
    scratch_env.setenv("EMBEDDING_TLS_PINNED_CERT", pin.wire)
    clear_replaced_endpoint_pins(block)
    assert os.environ["EMBEDDING_TLS_PINNED_CERT"] == pin.wire


def test_the_citation_alias_replaces_only_the_url_the_reader_picks(scratch_env, pin):
    scratch_env.setenv("CITATION_LLM_BASE_URL", "https://a.example/v1")
    scratch_env.setenv("CITATION_LLM_TLS_PINNED_CERT", pin.wire)
    # CITATION_LLM_BASE_URL still wins over the alias: the pinned URL stays.
    clear_replaced_endpoint_pins({"CITATION_LLM_URL": "https://b.example/v1"})
    assert os.environ["CITATION_LLM_TLS_PINNED_CERT"] == pin.wire
    clear_replaced_endpoint_pins({"CITATION_LLM_BASE_URL": "https://b.example/v1"})
    assert "CITATION_LLM_TLS_PINNED_CERT" not in os.environ


@pytest.mark.parametrize("prefix", EXPORTED)
def test_job_after_a_pinned_job_carries_no_stale_pin(scratch_env, pin, prefix):
    from agent.agent import export_job_env_keys

    export_job_env_keys(_pinned_block(prefix, pin))
    assert os.environ[f"{prefix}_TLS_PINNED_CERT"] == pin.wire
    export_job_env_keys(_unpinned_block(prefix))
    assert os.environ[_url_name(prefix)] == "https://b.example/v1"
    assert f"{prefix}_TLS_PINNED_CERT" not in os.environ


@pytest.mark.parametrize("prefix", EXPORTED)
def test_job_without_the_slot_keeps_url_and_pin_together(scratch_env, pin, prefix):
    from agent.agent import export_job_env_keys

    export_job_env_keys(_pinned_block(prefix, pin))
    export_job_env_keys({"OPENROUTER_REFERER": "x"})
    assert os.environ[_url_name(prefix)] == "https://a.example/v1"
    assert os.environ[f"{prefix}_TLS_PINNED_CERT"] == pin.wire


def test_kb_embedding_is_replaced_as_a_whole(scratch_env, pin):
    from agent.agent import export_job_env_keys

    export_job_env_keys(_pinned_block("KB_EMBEDDING", pin))
    assert os.environ["KB_EMBEDDING_TLS_PINNED_CERT"] == pin.wire
    export_job_env_keys(_unpinned_block("KB_EMBEDDING"))
    assert os.environ["KB_EMBEDDING_BASE_URL"] == "https://b.example/v1"
    assert "KB_EMBEDDING_TLS_PINNED_CERT" not in os.environ


def test_stateless_restore_gives_back_the_baseline_pin(scratch_env, pin):
    from agent.agent import UniversalAgent, export_job_env_keys

    scratch_env.setenv("VISION_BASE_URL", "https://base.example/v1")
    scratch_env.setenv("VISION_TLS_PINNED_CERT", pin.wire)
    agent = UniversalAgent.__new__(UniversalAgent)
    agent._worker_env_restore = {}
    block = _unpinned_block("VISION")
    agent._capture_worker_environment(
        {"resolved_config": {"agent": {"env_keys": block}}}
    )
    export_job_env_keys(block)
    assert "VISION_TLS_PINNED_CERT" not in os.environ
    agent._restore_worker_environment()
    assert os.environ["VISION_BASE_URL"] == "https://base.example/v1"
    assert os.environ["VISION_TLS_PINNED_CERT"] == pin.wire


@pytest.mark.parametrize("prefix", ["EMBEDDING", "RERANK"])
def test_live_update_after_a_pinned_endpoint_carries_no_stale_pin(
    scratch_env, pin, prefix
):
    from agent.api.persistent_app import _apply_live_embedding_env

    assert _apply_live_embedding_env(_pinned_block(prefix, pin))
    assert os.environ[f"{prefix}_TLS_PINNED_CERT"] == pin.wire
    assert _apply_live_embedding_env(_unpinned_block(prefix))
    assert os.environ[f"{prefix}_BASE_URL"] == "https://b.example/v1"
    assert f"{prefix}_TLS_PINNED_CERT" not in os.environ


@pytest.mark.parametrize(
    "update",
    [
        {"RERANK_MODEL": "rr", "RERANK_BASE_URL": "https://b.example/v1"},
        {"EMBEDDING_MODEL": "e2"},
        {"EMBEDDING_BASE_URL": None, "EMBEDDING_MODEL": "e2"},
    ],
)
def test_live_update_without_the_url_keeps_url_and_pin_together(
    scratch_env, pin, update
):
    from agent.api.persistent_app import _apply_live_embedding_env

    _apply_live_embedding_env(_pinned_block("EMBEDDING", pin))
    assert _apply_live_embedding_env(update)
    assert os.environ["EMBEDDING_BASE_URL"] == "https://a.example/v1"
    assert os.environ["EMBEDDING_TLS_PINNED_CERT"] == pin.wire


def test_live_update_without_embedding_keys_changes_nothing(scratch_env, pin):
    from agent.api.persistent_app import _apply_live_embedding_env

    _apply_live_embedding_env(_pinned_block("EMBEDDING", pin))
    assert _apply_live_embedding_env({"VISION_BASE_URL": "https://b"}) is False
    assert os.environ["EMBEDDING_TLS_PINNED_CERT"] == pin.wire
