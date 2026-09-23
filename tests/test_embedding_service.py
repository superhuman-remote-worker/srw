"""Unit tests for EmbeddingService."""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture
def mock_env(monkeypatch):
    """Set up environment variables for testing."""
    monkeypatch.setenv("OPENAI_API_KEY", "test-key-123")
    monkeypatch.delenv("EMBEDDING_API_KEY", raising=False)
    monkeypatch.delenv("EMBEDDING_BASE_URL", raising=False)
    monkeypatch.delenv("EMBEDDING_MODEL", raising=False)
    monkeypatch.delenv("EMBEDDING_PROVIDER", raising=False)
    monkeypatch.delenv("EMBEDDING_DIMENSIONS", raising=False)
    for key in (
        "KB_EMBEDDING_PROVIDER",
        "KB_EMBEDDING_MODEL",
        "KB_EMBEDDING_BASE_URL",
        "KB_EMBEDDING_API_KEY",
        "KB_EMBEDDING_DIMENSIONS",
        "KB_EMBEDDING_PROFILE_ID",
    ):
        monkeypatch.delenv(key, raising=False)


@pytest.fixture
def mock_openai_client():
    """Mock AsyncOpenAI client."""
    with patch("shared.runtime.services.embedding_service.AsyncOpenAI") as mock_cls:
        mock_client = MagicMock()
        mock_cls.return_value = mock_client
        yield mock_client, mock_cls


class TestEmbeddingServiceInit:
    """Test EmbeddingService initialization."""

    def test_init_with_openai_key(self, mock_env, mock_openai_client):
        """Uses OPENAI_API_KEY when EMBEDDING_API_KEY not set."""
        from shared.runtime.services.embedding_service import EmbeddingService

        service = EmbeddingService()
        assert service.api_key == "test-key-123"
        assert service.model == "qwen3-embedding-8b"
        assert service.base_url == "https://api.openai.com/v1"

    def test_init_with_embedding_key(self, monkeypatch, mock_openai_client):
        """Prefers EMBEDDING_API_KEY over OPENAI_API_KEY."""
        monkeypatch.setenv("OPENAI_API_KEY", "openai-key")
        monkeypatch.setenv("EMBEDDING_API_KEY", "embedding-key")

        from shared.runtime.services.embedding_service import EmbeddingService

        service = EmbeddingService()
        assert service.api_key == "embedding-key"

    def test_init_custom_model(self, mock_env, monkeypatch, mock_openai_client):
        """Respects EMBEDDING_MODEL env var."""
        monkeypatch.setenv("EMBEDDING_MODEL", "text-embedding-3-large")

        from shared.runtime.services.embedding_service import EmbeddingService

        service = EmbeddingService()
        assert service.model == "text-embedding-3-large"

    def test_init_custom_base_url(self, mock_env, monkeypatch, mock_openai_client):
        """Respects EMBEDDING_BASE_URL env var."""
        monkeypatch.setenv("EMBEDDING_BASE_URL", "http://localhost:11434/v1")

        from shared.runtime.services.embedding_service import EmbeddingService

        service = EmbeddingService()
        assert service.base_url == "http://localhost:11434/v1"

    def test_profile_fingerprint_is_normalized_and_never_depends_on_key(
        self, mock_env, mock_openai_client
    ):
        from shared.runtime.services.embedding_service import EmbeddingService

        first = EmbeddingService(
            provider="openai",
            base_url="HTTPS://AI.Example:443/v1/?access_token=secret-one",
            api_key="secret-one",
            profile_identity="system:endpoint-1",
        )
        same_transport = EmbeddingService(
            provider="OPENAI",
            base_url="https://ai.example/v1#secret-two",
            api_key="secret-two",
            profile_identity="system:endpoint-1",
        )
        moved_endpoint = EmbeddingService(
            provider="openai",
            base_url="https://other.example/v1",
            api_key="secret-one",
            profile_identity="system:endpoint-2",
        )

        assert first.profile_fingerprint == same_transport.profile_fingerprint
        assert first.profile_fingerprint != moved_endpoint.profile_fingerprint
        assert "secret" not in first.profile_fingerprint


class TestEmbeddingServiceSingleton:
    """Test get_embedding_service singleton pattern."""

    def test_singleton_returns_same_instance(self, mock_env, mock_openai_client):
        """get_embedding_service returns the same instance on repeated calls."""
        import shared.runtime.services.embedding_service as mod

        # Reset singleton
        mod._embedding_service = None

        s1 = mod.get_embedding_service()
        s2 = mod.get_embedding_service()
        assert s1 is s2

        # Cleanup
        mod._embedding_service = None


class TestKnowledgeEmbeddingService:
    """The centrally indexed OKF corpus uses its own stable profile."""

    def _reset(self, mod):
        mod._embedding_service = None
        mod._kb_embedding_service = None
        mod._kb_embedding_profile = None

    def test_dedicated_profile_is_independent_of_user_memory(
        self, mock_env, monkeypatch, mock_openai_client
    ):
        import shared.runtime.services.embedding_service as mod

        self._reset(mod)
        monkeypatch.setenv("EMBEDDING_MODEL", "user-memory-model")
        monkeypatch.setenv("EMBEDDING_BASE_URL", "https://user.example/v1")
        monkeypatch.setenv("EMBEDDING_API_KEY", "user-key")
        monkeypatch.setenv("KB_EMBEDDING_PROVIDER", "openai")
        monkeypatch.setenv("KB_EMBEDDING_MODEL", "system-kb-model")
        monkeypatch.setenv("KB_EMBEDDING_BASE_URL", "https://system.example/v1")
        monkeypatch.setenv("KB_EMBEDDING_API_KEY", "system-key")
        monkeypatch.setenv("KB_EMBEDDING_DIMENSIONS", "4096")
        monkeypatch.setenv("KB_EMBEDDING_PROFILE_ID", "system:endpoint-1")

        memory = mod.get_embedding_service()
        knowledge = mod.get_kb_embedding_service()

        assert memory.model == "user-memory-model"
        assert memory.api_key == "user-key"
        assert knowledge is not memory
        assert knowledge.model == "system-kb-model"
        assert knowledge.base_url == "https://system.example/v1"
        assert knowledge.api_key == "system-key"
        assert knowledge.expected_dimensions == 4096
        assert knowledge.profile_identity == "system:endpoint-1"
        self._reset(mod)

    def test_legacy_deployment_falls_back_to_memory_profile(
        self, mock_env, mock_openai_client
    ):
        import shared.runtime.services.embedding_service as mod

        self._reset(mod)
        assert mod.get_kb_embedding_service() is mod.get_embedding_service()
        self._reset(mod)

    def test_declared_profile_never_borrows_user_key(
        self, mock_env, monkeypatch, mock_openai_client
    ):
        import shared.runtime.services.embedding_service as mod

        self._reset(mod)
        monkeypatch.setenv("EMBEDDING_API_KEY", "user-key")
        monkeypatch.setenv("KB_EMBEDDING_MODEL", "system-kb-model")
        monkeypatch.delenv("KB_EMBEDDING_API_KEY", raising=False)

        knowledge = mod.get_kb_embedding_service()

        assert knowledge.model == "system-kb-model"
        assert knowledge.api_key == ""
        self._reset(mod)

    def test_profile_change_rebuilds_only_kb_singleton(
        self, mock_env, monkeypatch, mock_openai_client
    ):
        import shared.runtime.services.embedding_service as mod

        self._reset(mod)
        monkeypatch.setenv("KB_EMBEDDING_MODEL", "kb-v1")
        monkeypatch.setenv("KB_EMBEDDING_API_KEY", "system-key")
        first = mod.get_kb_embedding_service()
        memory = mod.get_embedding_service()

        monkeypatch.setenv("KB_EMBEDDING_MODEL", "kb-v2")
        second = mod.get_kb_embedding_service()

        assert second is not first
        assert second.model == "kb-v2"
        assert mod.get_embedding_service() is memory
        self._reset(mod)

    def test_authoritative_apply_clears_stale_endpoint_and_profile_removal(
        self, mock_env, mock_openai_client
    ):
        import shared.runtime.services.embedding_service as mod

        self._reset(mod)
        mod.apply_kb_embedding_env(
            {
                "KB_EMBEDDING_PROVIDER": "openai",
                "KB_EMBEDDING_MODEL": "endpoint-model",
                "KB_EMBEDDING_BASE_URL": "https://old.example/v1",
                "KB_EMBEDDING_API_KEY": "old-system-key",
                "KB_EMBEDDING_DIMENSIONS": "4096",
            }
        )
        first = mod.get_kb_embedding_service()
        assert first.base_url == "https://old.example/v1"

        # Provider-backed replacement deliberately has no BASE_URL. The old
        # endpoint must disappear rather than combine with the new model/key.
        mod.apply_kb_embedding_env(
            {
                "KB_EMBEDDING_PROVIDER": "openrouter",
                "KB_EMBEDDING_MODEL": "openai/text-embedding-3-large",
                "KB_EMBEDDING_API_KEY": "new-system-key",
            }
        )
        second = mod.get_kb_embedding_service()
        assert second is not first
        assert second.base_url == mod.EmbeddingService.OPENROUTER_API_URL
        assert second.api_key == "new-system-key"
        assert "KB_EMBEDDING_BASE_URL" not in os.environ

        # Attaching a thread/job without knowledge scope removes the whole
        # system profile and restores the legacy general-profile fallback.
        mod.apply_kb_embedding_env(None)
        assert all(key not in os.environ for key in mod.KB_EMBEDDING_ENV_KEYS)
        assert mod.get_kb_embedding_service() is mod.get_embedding_service()
        self._reset(mod)


class TestEmbeddingServiceEmbed:
    """Test embed() and embed_batch() methods."""

    @pytest.mark.asyncio
    async def test_embed_single(self, mock_env, monkeypatch, mock_openai_client):
        """embed() returns a vector from the API response."""
        monkeypatch.setenv("EMBEDDING_DIMENSIONS", "3")
        mock_client, _ = mock_openai_client
        from shared.runtime.services.embedding_service import EmbeddingService

        # Mock the API response
        mock_embedding = MagicMock()
        mock_embedding.embedding = [0.1, 0.2, 0.3]
        mock_response = MagicMock()
        mock_response.data = [mock_embedding]
        mock_client.embeddings.create = AsyncMock(return_value=mock_response)

        service = EmbeddingService()
        result = await service.embed("test text")

        assert result == [0.1, 0.2, 0.3]
        mock_client.embeddings.create.assert_awaited_once_with(
            input="test text",
            model="qwen3-embedding-8b",
        )

    @pytest.mark.asyncio
    async def test_embed_batch(self, mock_env, monkeypatch, mock_openai_client):
        """embed_batch() returns vectors in input order."""
        monkeypatch.setenv("EMBEDDING_DIMENSIONS", "1")
        mock_client, _ = mock_openai_client
        from shared.runtime.services.embedding_service import EmbeddingService

        # Mock response with out-of-order indices
        mock_e1 = MagicMock()
        mock_e1.embedding = [0.1]
        mock_e1.index = 1
        mock_e2 = MagicMock()
        mock_e2.embedding = [0.2]
        mock_e2.index = 0

        mock_response = MagicMock()
        mock_response.data = [mock_e1, mock_e2]  # Intentionally out of order
        mock_client.embeddings.create = AsyncMock(return_value=mock_response)

        service = EmbeddingService()
        result = await service.embed_batch(["hello", "world"])

        # Should be sorted by index
        assert result == [[0.2], [0.1]]

    @pytest.mark.asyncio
    async def test_embed_batch_empty(self, mock_env, mock_openai_client):
        """embed_batch() handles empty input."""
        from shared.runtime.services.embedding_service import EmbeddingService

        service = EmbeddingService()
        result = await service.embed_batch([])
        assert result == []


class TestDimensionGuard:
    """B4: dimension mismatch must latch loudly instead of failing quietly."""

    def _mock_response(self, mock_client, vector):
        mock_embedding = MagicMock()
        mock_embedding.embedding = vector
        mock_embedding.index = 0
        mock_response = MagicMock()
        mock_response.data = [mock_embedding]
        mock_client.embeddings.create = AsyncMock(return_value=mock_response)

    def test_default_expected_dimensions(self, mock_env, mock_openai_client):
        """Defaults to the schema's vector(4096)."""
        from shared.runtime.services.embedding_service import EmbeddingService

        assert EmbeddingService().expected_dimensions == 4096

    @pytest.mark.asyncio
    async def test_mismatch_raises_and_latches(self, mock_env, mock_openai_client):
        """Wrong dimensionality raises EmbeddingDimensionError and latches."""
        mock_client, _ = mock_openai_client
        from shared.runtime.services.embedding_service import (
            EmbeddingDimensionError,
            EmbeddingService,
        )

        self._mock_response(mock_client, [0.1, 0.2, 0.3])  # 3 != 4096
        service = EmbeddingService()

        with pytest.raises(EmbeddingDimensionError, match="3-dim"):
            await service.embed("test")
        assert service.degraded_reason is not None

        # Second call fails fast without touching the API again.
        mock_client.embeddings.create.reset_mock()
        with pytest.raises(EmbeddingDimensionError):
            await service.embed("test")
        mock_client.embeddings.create.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_batch_mismatch_raises(self, mock_env, mock_openai_client):
        """embed_batch() checks dimensions too."""
        mock_client, _ = mock_openai_client
        from shared.runtime.services.embedding_service import (
            EmbeddingDimensionError,
            EmbeddingService,
        )

        self._mock_response(mock_client, [0.1])
        service = EmbeddingService()

        with pytest.raises(EmbeddingDimensionError):
            await service.embed_batch(["a"])
        assert service.degraded_reason is not None

    @pytest.mark.asyncio
    async def test_verify_dimensions_ok(
        self, mock_env, monkeypatch, mock_openai_client
    ):
        """Probe returns True when dimensions match."""
        monkeypatch.setenv("EMBEDDING_DIMENSIONS", "2")
        mock_client, _ = mock_openai_client
        from shared.runtime.services.embedding_service import EmbeddingService

        self._mock_response(mock_client, [0.1, 0.2])
        service = EmbeddingService()

        assert await service.verify_dimensions() is True
        assert service.degraded_reason is None

    @pytest.mark.asyncio
    async def test_verify_dimensions_mismatch(self, mock_env, mock_openai_client):
        """Probe returns False on mismatch and latches degraded."""
        mock_client, _ = mock_openai_client
        from shared.runtime.services.embedding_service import EmbeddingService

        self._mock_response(mock_client, [0.1, 0.2])
        service = EmbeddingService()

        assert await service.verify_dimensions() is False
        assert service.degraded_reason is not None

    @pytest.mark.asyncio
    async def test_verify_dimensions_inconclusive(self, mock_env, mock_openai_client):
        """Connectivity failures must NOT latch degraded (transient)."""
        mock_client, _ = mock_openai_client
        from shared.runtime.services.embedding_service import EmbeddingService

        mock_client.embeddings.create = AsyncMock(side_effect=ConnectionError("down"))
        service = EmbeddingService()

        assert await service.verify_dimensions() is None
        assert service.degraded_reason is None

    def test_health_snapshot(self, mock_env, mock_openai_client):
        """Snapshot carries the degraded flag for /status."""
        from shared.runtime.services.embedding_service import EmbeddingService

        service = EmbeddingService()
        snap = service.health_snapshot()
        assert snap["degraded"] is False
        assert snap["expected_dimensions"] == 4096
        assert snap["model"] == "qwen3-embedding-8b"

        service.degraded_reason = "boom"
        snap = service.health_snapshot()
        assert snap["degraded"] is True
        assert snap["degraded_reason"] == "boom"

    def test_peek_does_not_construct(self, mock_env, mock_openai_client):
        """peek_embedding_service() never builds the singleton."""
        import shared.runtime.services.embedding_service as mod

        mod._embedding_service = None
        assert mod.peek_embedding_service() is None
        assert mod._embedding_service is None

        s = mod.get_embedding_service()
        assert mod.peek_embedding_service() is s
        mod._embedding_service = None


class TestEmbeddingProviders:
    """Test provider-based initialization."""

    def test_local_provider_default(self, mock_env, mock_openai_client):
        """Default provider is 'local'."""
        from shared.runtime.services.embedding_service import EmbeddingService

        service = EmbeddingService()
        assert service.provider == "local"
        assert service.model == "qwen3-embedding-8b"
        assert service.base_url == "https://api.openai.com/v1"

    def test_openrouter_provider(self, monkeypatch, mock_openai_client):
        """OpenRouter provider uses correct URL and prefixed model."""
        monkeypatch.setenv("EMBEDDING_PROVIDER", "openrouter")
        monkeypatch.setenv("OPENROUTER_API_KEY", "or-key-123")

        from shared.runtime.services.embedding_service import EmbeddingService

        service = EmbeddingService()
        assert service.provider == "openrouter"
        assert service.api_key == "or-key-123"
        assert service.base_url == "https://openrouter.ai/api/v1"
        assert service.model == "qwen/qwen3-embedding-8b"

    def test_openrouter_provider_custom_model(self, monkeypatch, mock_openai_client):
        """OpenRouter auto-prefixes model name with qwen/."""
        monkeypatch.setenv("EMBEDDING_PROVIDER", "openrouter")
        monkeypatch.setenv("OPENROUTER_API_KEY", "or-key-123")
        monkeypatch.setenv("EMBEDDING_MODEL", "qwen3-embedding-0.6b")

        from shared.runtime.services.embedding_service import EmbeddingService

        service = EmbeddingService()
        assert service.model == "qwen/qwen3-embedding-0.6b"

    def test_openrouter_provider_full_model_name(self, monkeypatch, mock_openai_client):
        """OpenRouter skips prefix when model already contains /."""
        monkeypatch.setenv("EMBEDDING_PROVIDER", "openrouter")
        monkeypatch.setenv("OPENROUTER_API_KEY", "or-key-123")
        monkeypatch.setenv("EMBEDDING_MODEL", "openai/text-embedding-3-small")

        from shared.runtime.services.embedding_service import EmbeddingService

        service = EmbeddingService()
        assert service.model == "openai/text-embedding-3-small"

    def test_explicit_local_provider(self, mock_env, monkeypatch, mock_openai_client):
        """Explicit EMBEDDING_PROVIDER=local works."""
        monkeypatch.setenv("EMBEDDING_PROVIDER", "local")

        from shared.runtime.services.embedding_service import EmbeddingService

        service = EmbeddingService()
        assert service.provider == "local"
        assert service.api_key == "test-key-123"

    def test_unknown_provider_falls_back_to_local(
        self, mock_env, monkeypatch, mock_openai_client
    ):
        """Unknown provider name falls back to local behavior."""
        monkeypatch.setenv("EMBEDDING_PROVIDER", "nonexistent")

        from shared.runtime.services.embedding_service import EmbeddingService

        service = EmbeddingService()
        # Unknown provider goes through the else (local) branch
        assert service.provider == "nonexistent"
        assert service.base_url == "https://api.openai.com/v1"


class TestExplicitConfigOverrides:
    """Explicit constructor kwargs (slice 3 PR3 — the orchestrator builds an
    EmbeddingService from catalog-resolved credentials, not env)."""

    def test_explicit_kwargs_override_env(self, monkeypatch, mock_openai_client):
        monkeypatch.setenv("EMBEDDING_API_KEY", "env-key")
        monkeypatch.setenv("EMBEDDING_MODEL", "env-model")
        monkeypatch.setenv("EMBEDDING_BASE_URL", "https://env.example/v1")

        from shared.runtime.services.embedding_service import EmbeddingService

        service = EmbeddingService(
            model="catalog-model",
            base_url="https://catalog.example/v1",
            api_key="catalog-key",
        )
        assert service.model == "catalog-model"
        assert service.base_url == "https://catalog.example/v1"
        assert service.api_key == "catalog-key"

    def test_explicit_client_uses_explicit_credentials(
        self, mock_env, mock_openai_client
    ):
        _, mock_cls = mock_openai_client

        from shared.runtime.services.embedding_service import EmbeddingService

        EmbeddingService(model="m", base_url="https://x.example/v1", api_key="k")
        kwargs = mock_cls.call_args[1]
        assert kwargs["api_key"] == "k"
        assert kwargs["base_url"] == "https://x.example/v1"

    def test_openai_key_not_inherited_by_foreign_embedding_host(
        self, mock_env, monkeypatch, mock_openai_client
    ):
        # A key follows its endpoint: OPENAI_API_KEY belongs to api.openai.com,
        # never to a custom EMBEDDING_BASE_URL that carries no EMBEDDING_API_KEY.
        monkeypatch.setenv("EMBEDDING_BASE_URL", "https://env.example/v1")

        from shared.runtime.services.embedding_service import EmbeddingService

        service = EmbeddingService(model="only-model-given")
        assert service.model == "only-model-given"
        assert service.base_url == "https://env.example/v1"
        assert service.api_key == ""

    def test_embedding_key_paired_with_custom_host_is_used(
        self, mock_env, monkeypatch, mock_openai_client
    ):
        # The endpoint's own EMBEDDING_API_KEY is what authenticates it.
        monkeypatch.setenv("EMBEDDING_BASE_URL", "https://env.example/v1")
        monkeypatch.setenv("EMBEDDING_API_KEY", "emb-key")

        from shared.runtime.services.embedding_service import EmbeddingService

        service = EmbeddingService(model="only-model-given")
        assert service.base_url == "https://env.example/v1"
        assert service.api_key == "emb-key"

    def test_no_kwargs_is_pure_env_backward_compat(self, mock_env, mock_openai_client):
        from shared.runtime.services.embedding_service import EmbeddingService

        service = EmbeddingService()
        assert service.model == "qwen3-embedding-8b"
        assert service.api_key == "test-key-123"
