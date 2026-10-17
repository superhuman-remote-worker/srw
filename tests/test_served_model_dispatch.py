"""A catalog row's served model id reaches the provider; the id stays the reference.

A model id names one catalog row across every provider. A row that offers a
model some other row already serves gets its own id (``claude-opus-5-5-api``)
and sends the shared name (``claude-opus-5-5``) through ``served_model_id``.
Dispatch carries that name next to the id: ``served_model`` in a model section,
``{PREFIX}_SERVED_MODEL`` in a flat env block. See
knowledge-base/knowledge/issues/same_model_id_on_two_providers_routes_to_the_api_key_row.md.
"""

from __future__ import annotations

import logging
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

os.environ.setdefault("VECTOR_DB_URL", "postgresql://test@localhost/test")

from orchestrator.services import dispatch_credentials as dc  # noqa: E402
from orchestrator.services.capability_credentials import (  # noqa: E402
    CapabilityCredentials,
    resolve_capability_credentials,
)
from shared.runtime.core.model_registry import (  # noqa: E402
    ModelMeta,
    UnknownModelError,
    _catalog_row_to_meta,
    served_model_problem,
)

ENDPOINT_ID = "11111111-1111-1111-1111-111111111111"


def _meta(model_id: str, served: str | None = None, **overrides) -> ModelMeta:
    fields = dict(
        model_id=model_id,
        provider="openai",
        family="claude-opus-5-5",
        display_name=model_id,
        origin="catalog",
        endpoint_id=ENDPOINT_ID,
        served_model_id=served,
    )
    fields.update(overrides)
    return ModelMeta(**fields)


def _deps(meta: ModelMeta | None) -> dc.DispatchCredentialDependencies:
    store = MagicMock()
    store.get_user_llm_endpoint = AsyncMock(
        return_value={"base_url": "https://proxy.example/v1", "api_key": "sk-proxy"}
    )
    store.catalog_model_states = AsyncMock(return_value=[])

    async def _resolve(model_id, **kwargs):
        if meta is None:
            raise UnknownModelError(model_id)
        return meta

    return dc.DispatchCredentialDependencies(
        store=store,
        logger=logging.getLogger("test"),
        resolve_model=AsyncMock(side_effect=_resolve),
    )


class TestModelSection:
    @pytest.mark.asyncio
    async def test_the_rows_served_name_rides_next_to_the_id(self):
        section = {"model": "claude-opus-5-5-api"}
        reason = await dc.inject_model_credentials(
            section=section,
            model_id="claude-opus-5-5-api",
            user_id=None,
            resolved_keys={},
            dependencies=_deps(_meta("claude-opus-5-5-api", "claude-opus-5-5")),
        )
        assert reason is None
        assert section["model"] == "claude-opus-5-5-api"
        assert section["served_model"] == "claude-opus-5-5"
        assert section["base_url"] == "https://proxy.example/v1"

    @pytest.mark.asyncio
    async def test_a_callers_served_name_never_survives(self):
        # Usage is priced under the id, so the served name is the catalog's.
        section = {"model": "claude-opus-5-5-api", "served_model": "claude-fable-5-1"}
        await dc.inject_model_credentials(
            section=section,
            model_id="claude-opus-5-5-api",
            user_id=None,
            resolved_keys={},
            dependencies=_deps(_meta("claude-opus-5-5-api", "claude-opus-5-5")),
        )
        assert section["served_model"] == "claude-opus-5-5"

    @pytest.mark.asyncio
    async def test_a_row_without_one_clears_the_previous_models(self):
        # A session hot swap deep-merges this section over the old model's:
        # None is the agent-side "clear this field" sentinel.
        section = {"model": "claude-opus-5-5", "served_model": "claude-opus-5-5"}
        await dc.inject_model_credentials(
            section=section,
            model_id="claude-opus-5-5",
            user_id=None,
            resolved_keys={},
            dependencies=_deps(_meta("claude-opus-5-5")),
        )
        assert "served_model" in section
        assert section["served_model"] is None


class TestEnvBlock:
    @pytest.mark.asyncio
    async def test_the_served_name_travels_beside_the_catalog_id(self):
        env: dict = {}
        await dc.inject_env_key_credentials(
            env_keys=env,
            prefix="EMBEDDING",
            model_id="qwen/qwen3-embedding-8b-local",
            user_id=None,
            resolved_keys={},
            capability="embedding",
            dependencies=_deps(
                _meta("qwen/qwen3-embedding-8b-local", "qwen/qwen3-embedding-8b")
            ),
        )
        # The id is what a resumed session resolves again.
        assert env["EMBEDDING_MODEL"] == "qwen/qwen3-embedding-8b-local"
        assert env["EMBEDDING_SERVED_MODEL"] == "qwen/qwen3-embedding-8b"
        assert env["EMBEDDING_BASE_URL"] == "https://proxy.example/v1"

    @pytest.mark.asyncio
    async def test_a_resumed_block_drops_a_previous_rows_served_name(self):
        env = {
            "EMBEDDING_MODEL": "qwen3-embedding-8b",
            "EMBEDDING_SERVED_MODEL": "stale",
        }
        await dc.inject_env_key_credentials(
            env_keys=env,
            prefix="EMBEDDING",
            model_id="qwen3-embedding-8b",
            user_id=None,
            resolved_keys={},
            capability="embedding",
            dependencies=_deps(_meta("qwen3-embedding-8b")),
        )
        assert "EMBEDDING_SERVED_MODEL" not in env

    @pytest.mark.asyncio
    async def test_a_callers_own_endpoint_gets_no_served_name(self):
        env = {
            "EMBEDDING_BASE_URL": "https://byo.example/v1",
            "EMBEDDING_API_KEY": "sk-owner",
            "EMBEDDING_SERVED_MODEL": "stale",
        }
        await dc.inject_env_key_credentials(
            env_keys=env,
            prefix="EMBEDDING",
            model_id="owner-embed",
            user_id=None,
            resolved_keys={},
            capability="embedding",
            dependencies=_deps(_meta("owner-embed", "owner")),
        )
        assert "EMBEDDING_SERVED_MODEL" not in env


class TestRegistry:
    def test_both_row_shapes_carry_the_served_model_id(self):
        endpoint = _catalog_row_to_meta(
            {
                "provider_kind": "endpoint",
                "provider_ref": ENDPOINT_ID,
                "model_id": "claude-opus-5-5-proxy",
                "served_model_id": "claude-opus-5-5",
                "endpoint_id": ENDPOINT_ID,
                "endpoint_base_url": "https://proxy.example/v1",
            }
        )
        system = _catalog_row_to_meta(
            {
                "provider_kind": "system",
                "provider_ref": "anthropic",
                "model_id": "claude-opus-5-5-api",
                "served_model_id": "claude-opus-5-5",
            }
        )
        plain = _catalog_row_to_meta(
            {
                "provider_kind": "system",
                "provider_ref": "anthropic",
                "model_id": "claude-opus-5-5",
                "served_model_id": None,
            }
        )
        assert endpoint.served_model_id == "claude-opus-5-5"
        assert system.served_model_id == "claude-opus-5-5"
        assert plain.served_model_id is None

    @pytest.mark.parametrize(
        "model_id,served",
        [
            ("claude-opus-5-5-api", "claude-opus-5-5"),
            ("minimax-m3-256k", "minimax-m3"),
            ("gpt-6.1-sol-api", "gpt-6.1-sol"),
            ("openrouter/z-ai/glm-5.3-flash-cheap", "z-ai/glm-5.3-flash"),
            ("claude-opus-5-5", None),
            ("claude-opus-5-5", "claude-opus-5-5"),
        ],
    )
    def test_the_id_may_be_the_served_name_plus_a_same_family_suffix(
        self, model_id, served
    ):
        assert served_model_problem(model_id, served) is None

    @pytest.mark.parametrize(
        "model_id,served,why",
        [
            ("opus-api", "claude-opus-5-5", "plus a suffix"),
            ("sol-direct", "gpt-6.1-sol", "plus a suffix"),
            # gpt-6.1-solar would read as another model to every prefix check.
            ("gpt-6.1-solar", "gpt-6.1-sol", "plus a suffix"),
            ("gemini-3.8-flash-lite", "gemini-3.8-flash", "keeps the family"),
        ],
    )
    def test_anything_else_is_refused(self, model_id, served, why):
        assert why in (served_model_problem(model_id, served) or "")


class TestCapabilityCredentials:
    def test_wire_model_is_the_served_name_when_there_is_one(self):
        assert (
            CapabilityCredentials(
                model="tts-1-hd-proxy", served_model="tts-1-hd"
            ).wire_model
            == "tts-1-hd"
        )
        assert CapabilityCredentials(model="tts-1-hd").wire_model == "tts-1-hd"

    @pytest.mark.asyncio
    async def test_the_catalog_rows_served_name_is_resolved(self):
        db = MagicMock()
        db.resolve_default_for_capability = AsyncMock(return_value="whisper-1-proxy")
        db.resolve_catalog_model = AsyncMock(
            return_value={
                "catalog_id": "c1",
                "provider_kind": "endpoint",
                "provider_ref": ENDPOINT_ID,
                "model_id": "whisper-1-proxy",
                "served_model_id": "whisper-1",
                "endpoint_base_url": "https://proxy.example/v1",
                "api_key": "sk-proxy",
            }
        )
        creds = await resolve_capability_credentials(
            capability="whisper",
            user_settings={},
            user_id=None,
            resolved_keys={},
            postgres_db=db,
        )
        assert creds.model == "whisper-1-proxy"
        assert creds.wire_model == "whisper-1"

    @pytest.mark.asyncio
    async def test_transcription_sends_the_served_name(self):
        from orchestrator.services.transcribe import transcribe_thread_audio

        client = MagicMock()
        client.audio.transcriptions.create = AsyncMock(return_value="hello")
        client.close = AsyncMock()
        db = MagicMock()
        db.get_user_settings = AsyncMock(return_value={})
        db.resolve_api_keys_for_job = AsyncMock(return_value={})
        with (
            patch(
                "orchestrator.services.transcribe.AsyncOpenAI",
                MagicMock(return_value=client),
            ),
            patch(
                "orchestrator.services.transcribe.resolve_capability_credentials",
                AsyncMock(
                    return_value=CapabilityCredentials(
                        model="whisper-1-proxy",
                        served_model="whisper-1",
                        api_key="sk-proxy",
                    )
                ),
            ),
        ):
            await transcribe_thread_audio(
                audio_bytes=b"\x00\x01",
                filename="voice.webm",
                user_id="u1",
                postgres_db=db,
            )
        kwargs = client.audio.transcriptions.create.await_args.kwargs
        assert kwargs["model"] == "whisper-1"


class _NullStore:
    """Every store read answers nothing unless a test names it."""

    def __init__(self, **overrides):
        self._overrides = overrides

    def __getattr__(self, name):
        if name in self._overrides:
            return self._overrides[name]
        return AsyncMock(return_value=None)


class TestJobTopLevelModel:
    """The job's own ``llm`` is routed by its own branch, not by
    ``inject_model_credentials`` — the k3d smoke caught it sending the catalog
    id upstream (OpenRouter: "z-ai/glm-5.3-flash-endpoint is not a valid
    model ID")."""

    @staticmethod
    def _deps(meta):
        from orchestrator.services import job_dispatch_credentials as jdc

        store = _NullStore(
            resolve_api_keys_for_job=AsyncMock(return_value={}),
            get_user_settings=AsyncMock(return_value={}),
            get_user_llm_endpoint=AsyncMock(
                return_value={
                    "base_url": "https://openrouter.ai/api/v1",
                    "api_key": "sk-endpoint",
                    "label": "Local verified embedding",
                }
            ),
        )
        return jdc.DispatchCredentialDependencies(
            store=store,
            logger=logging.getLogger("test"),
            resolve_model=AsyncMock(return_value=meta),
            inject_model_credentials=AsyncMock(return_value=None),
            inject_env_key_credentials=AsyncMock(return_value=None),
            inject_search_credentials=AsyncMock(side_effect=lambda co, **kw: co),
            inject_system_kb_embedding_profile=AsyncMock(return_value=None),
            dispatch_llm_provider_fallback=lambda job, co: None,
            nested_model_slots=lambda co: [],
        )

    @pytest.mark.asyncio
    async def test_the_jobs_model_carries_its_rows_served_name(self):
        from orchestrator.services import job_dispatch_credentials as jdc

        out = await jdc.inject_dispatch_credentials(
            {"id": "j1", "user_id": None, "project_id": None},
            {
                "llm": {
                    "model": "z-ai/glm-5.3-flash-endpoint",
                    "served_model": "caller-choice",
                }
            },
            dependencies=self._deps(
                _meta("z-ai/glm-5.3-flash-endpoint", "z-ai/glm-5.3-flash")
            ),
        )
        assert out["llm"]["model"] == "z-ai/glm-5.3-flash-endpoint"
        assert out["llm"]["served_model"] == "z-ai/glm-5.3-flash"
        assert out["llm"]["base_url"] == "https://openrouter.ai/api/v1"

    @pytest.mark.asyncio
    async def test_a_row_without_one_clears_it(self):
        from orchestrator.services import job_dispatch_credentials as jdc

        out = await jdc.inject_dispatch_credentials(
            {"id": "j1", "user_id": None, "project_id": None},
            {"llm": {"model": "muse-spark-1.3-contributor", "served_model": "stale"}},
            dependencies=self._deps(_meta("muse-spark-1.3-contributor")),
        )
        assert out["llm"]["served_model"] is None
