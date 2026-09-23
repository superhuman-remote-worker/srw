"""Embedding Service for generating text embeddings.

Used by RecallStore (Memory Light) for dense vector search.
Follows the same singleton pattern as VisionHelper.

Provider-based configuration via environment variables:
- EMBEDDING_PROVIDER: Provider to use ("local" or "openrouter", default: "local")
- EMBEDDING_MODEL: Model name (default: qwen3-embedding-8b)

Provider-specific env vars:
- local:       EMBEDDING_BASE_URL, EMBEDDING_API_KEY (falls back to OPENAI_API_KEY)
- openrouter:  OPENROUTER_API_KEY

Per-account provider override is injected by the orchestrator dispatcher
via EMBEDDING_PROVIDER + the account's API key.
"""

import asyncio
import hashlib
import json
import logging
import os
from typing import List, Mapping, Optional
from urllib.parse import urlsplit

from openai import AsyncOpenAI

logger = logging.getLogger(__name__)

# Module-level singleton
_embedding_service: Optional["EmbeddingService"] = None
_kb_embedding_service: Optional["EmbeddingService"] = None
_kb_embedding_profile: Optional[
    tuple[str, str, Optional[str], str, int, Optional[str]]
] = None
KB_EMBEDDING_ENV_KEYS = (
    "KB_EMBEDDING_PROVIDER",
    "KB_EMBEDDING_MODEL",
    "KB_EMBEDDING_BASE_URL",
    "KB_EMBEDDING_API_KEY",
    "KB_EMBEDDING_DIMENSIONS",
    "KB_EMBEDDING_PROFILE_ID",
)


def _normalize_embedding_base_url(base_url: Optional[str]) -> str:
    """Canonical, credential-free endpoint identity used for vector stamps.

    Base URLs are configuration rather than user-facing links, but still avoid
    carrying URL userinfo, query strings, or fragments into the identity: those
    fields can contain credentials. Equivalent spellings (host casing, default
    ports, and a trailing slash) intentionally converge on one value.
    """
    if not base_url:
        return ""
    try:
        parsed = urlsplit(str(base_url).strip())
        if not parsed.scheme or not parsed.hostname:
            return "invalid-endpoint"
        scheme = parsed.scheme.lower()
        host = parsed.hostname.rstrip(".").lower()
        if ":" in host:
            host = f"[{host}]"
        port = parsed.port
        if port is not None and not (
            (scheme == "https" and port == 443) or (scheme == "http" and port == 80)
        ):
            host = f"{host}:{port}"
        path = (parsed.path or "").rstrip("/")
        return f"{scheme}://{host}{path}"
    except (TypeError, ValueError):
        return "invalid-endpoint"


def embedding_profile_fingerprint(
    provider: Optional[str],
    base_url: Optional[str],
    catalog_identity: Optional[str] = None,
) -> str:
    """Return a stable, non-secret identity for an embedding transport.

    A model name and dimensions do not uniquely identify vectors: the same
    label can be served by another provider or endpoint after an admin routing
    change. The short digest is safe to persist in ``embedding_version``. API
    keys are deliberately not accepted, so credential rotation does not force
    a corpus rebuild and secret material can never enter the stamp.
    """
    canonical = json.dumps(
        {
            "catalog": str(catalog_identity or "").strip(),
            "endpoint": _normalize_embedding_base_url(base_url),
            "provider": str(provider or "local").strip().lower(),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return f"pf-{hashlib.sha256(canonical.encode('utf-8')).hexdigest()[:16]}"


class EmbeddingDimensionError(RuntimeError):
    """Provider returned vectors that don't match the schema dimension.

    Every memory/KB write and dense query would fail at INSERT/cast with a
    raw DB error swallowed per call site, so the service latches degraded
    and fails fast instead (knowledge-base/knowledge/issues/memory_bugs.md B4).
    """


class EmbeddingInvalidVectorError(RuntimeError):
    """Provider returned a vector containing non-finite values (NaN/Inf).

    pgvector rejects these at INSERT, and the generic per-caller swallow used
    to collapse them into the same "skipping" state as an oversized batch —
    preventing targeted recovery. Typed so callers can count/report them
    separately (knowledge-base/knowledge/issues/
    embedding_batch_overflow_skips_citation_source_embeddings.md, fix 4).
    """


class EmbeddingService:
    """Service for generating text embeddings via configurable provider.

    Supports multiple providers (local endpoint, OpenRouter) selected
    via the EMBEDDING_PROVIDER environment variable. Each provider
    resolves its own base URL and API key from the environment.

    Example:
        ```python
        service = get_embedding_service()
        vector = await service.embed("Hello world")
        vectors = await service.embed_batch(["Hello", "World"])
        ```
    """

    OPENAI_API_URL = "https://api.openai.com/v1"
    OPENROUTER_API_URL = "https://openrouter.ai/api/v1"

    def __init__(
        self,
        model: Optional[str] = None,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        provider: Optional[str] = None,
        expected_dimensions: Optional[int] = None,
        profile_identity: Optional[str] = None,
    ):
        """Initialize the Embedding Service.

        Configuration comes from the environment by default; any explicit
        kwarg overrides its env counterpart (slice 3 PR3 — the orchestrator's
        KB reindexer constructs an instance from catalog-resolved credentials,
        since the orchestrator pod carries no EMBEDDING_* env post
        models_yaml_removal).
        """
        self.provider = (provider or os.getenv("EMBEDDING_PROVIDER", "local")).lower()
        base_model = model or os.getenv("EMBEDDING_MODEL", "qwen3-embedding-8b")

        # Provider batch ceiling. TEI-compatible backends 422 above 64 inputs;
        # embed_batch splits transparently so no caller ever needs to know.
        # 0/negative disables splitting (a provider without a limit).
        try:
            self.max_batch_size = int(os.getenv("EMBEDDING_MAX_BATCH_SIZE", "64"))
        except ValueError:
            self.max_batch_size = 64

        if self.provider == "openrouter":
            self.api_key = (
                api_key if api_key is not None else os.getenv("OPENROUTER_API_KEY", "")
            )
            self.base_url = base_url or self.OPENROUTER_API_URL
            # OpenRouter uses provider-prefixed model names
            self.model = f"qwen/{base_model}" if "/" not in base_model else base_model
        else:
            # "local" provider (default) — custom endpoint or OpenAI
            self.base_url = base_url or os.getenv(
                "EMBEDDING_BASE_URL", self.OPENAI_API_URL
            )
            if api_key is not None:
                self.api_key = api_key
            else:
                # A key follows its endpoint: OPENAI_API_KEY belongs to OpenAI
                # itself and is never the fallback for another embedding host.
                from shared.runtime.core.transport_resolution import (
                    is_openai_default_endpoint,
                )

                self.api_key = os.getenv("EMBEDDING_API_KEY") or (
                    os.getenv("OPENAI_API_KEY", "")
                    if is_openai_default_endpoint(self.base_url)
                    else ""
                )
            self.model = base_model

        # Schema columns are vector(4096); a provider returning any other
        # dimensionality breaks every write/query. Override only together
        # with a schema re-dimension (memory overhaul Phase 6 territory).
        self.expected_dimensions = (
            expected_dimensions
            if expected_dimensions is not None
            else int(os.getenv("EMBEDDING_DIMENSIONS", "4096"))
        )
        self.profile_identity = profile_identity
        self.profile_fingerprint = embedding_profile_fingerprint(
            self.provider,
            self.base_url,
            self.profile_identity,
        )
        #: Set once a dimension mismatch is detected; latches for the
        #: process lifetime (a mismatch is config, not transient).
        self.degraded_reason: Optional[str] = None

        if not self.api_key:
            logger.warning(
                "No API key found for embedding provider '%s'. "
                "Embedding calls will fail.",
                self.provider,
            )

        self._client = AsyncOpenAI(
            api_key=self.api_key,
            base_url=self.base_url,
        )

        logger.info(
            f"EmbeddingService initialized (provider={self.provider}, "
            f"model={self.model}, base_url={self.base_url})"
        )

    def _check_dimensions(self, vector: List[float]) -> None:
        """Latch degraded + raise if the provider's dimensionality is wrong."""
        if len(vector) == self.expected_dimensions:
            return
        self.degraded_reason = (
            f"provider '{self.provider}' model '{self.model}' returned "
            f"{len(vector)}-dim vectors; schema expects "
            f"vector({self.expected_dimensions})"
        )
        logger.error(
            "EMBEDDING DIMENSION MISMATCH: %s — every memory/KB write and "
            "dense query would fail at INSERT, so the dense path is disabled "
            "loudly instead. Fix EMBEDDING_MODEL/EMBEDDING_BASE_URL (or set "
            "EMBEDDING_DIMENSIONS alongside a schema re-dimension) and "
            "restart. See knowledge-base/knowledge/issues/memory_bugs.md B4.",
            self.degraded_reason,
        )
        raise EmbeddingDimensionError(self.degraded_reason)

    def _fail_fast_if_degraded(self) -> None:
        if self.degraded_reason:
            raise EmbeddingDimensionError(self.degraded_reason)

    async def embed(self, text: str) -> List[float]:
        """Generate embedding for a single text.

        Args:
            text: Text to embed

        Returns:
            Embedding vector as list of floats

        Raises:
            EmbeddingDimensionError: provider dimensionality doesn't match
                the schema (latched — subsequent calls fail without I/O).
        """
        self._fail_fast_if_degraded()
        response = await self._client.embeddings.create(
            input=text,
            model=self.model,
        )
        vector = response.data[0].embedding
        self._check_dimensions(vector)
        return vector

    def _check_finite(self, vector: List[float], position: int) -> None:
        """Reject NaN/Inf vectors before they reach pgvector (typed)."""
        import math

        if all(map(math.isfinite, vector)):
            return
        raise EmbeddingInvalidVectorError(
            f"provider '{self.provider}' model '{self.model}' returned a "
            f"non-finite embedding vector at input position {position}"
        )

    async def _embed_slice(self, texts: List[str]) -> List[List[float]]:
        """One bounded provider request with transient-only retry.

        Retry policy per the house rule (one layer per call path — no caller
        of embed_batch retries): 429/5xx/timeouts get two extra attempts via
        the shared classifier; deterministic 4xx (an oversize 422 can no
        longer happen, but validation errors still can) raises immediately.
        """
        from openai import BadRequestError, UnprocessableEntityError

        from shared.runtime.core.llm_retry import RetryPolicy, invoke_with_retry

        async def _attempt() -> List[List[float]]:
            response = await self._client.embeddings.create(
                input=texts,
                model=self.model,
            )
            sorted_data = sorted(response.data, key=lambda x: x.index)
            return [item.embedding for item in sorted_data]

        return await invoke_with_retry(
            _attempt,
            policy=RetryPolicy(
                max_attempts=3,
                base_delay=1.0,
                max_delay=15.0,
                # Deterministic validation failures (the 422 oversize class,
                # malformed input) re-fail identically — retrying them is pure
                # backend load. The shared classifier defaults unknown 4xx to
                # "transient", so pin them here per the never_retry doctrine.
                never_retry=(UnprocessableEntityError, BadRequestError),
            ),
            description=f"embedding batch ({len(texts)} inputs, {self.model})",
        )

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for multiple texts, splitting at the provider cap.

        THE batching seam (knowledge-base/knowledge/issues/
        embedding_batch_overflow_skips_citation_source_embeddings.md): inputs
        are split into consecutive slices of at most ``max_batch_size`` so a
        whole-source chunk list (observed up to 1,458 chunks) can never hit a
        TEI backend as one oversized request. Slices run sequentially and
        results concatenate in input order — index ``i`` of the return value
        is always the embedding of ``texts[i]``.

        Args:
            texts: List of texts to embed

        Returns:
            List of embedding vectors (same order as input)

        Raises:
            EmbeddingDimensionError: provider dimensionality doesn't match
                the schema (latched — subsequent calls fail without I/O).
            EmbeddingInvalidVectorError: provider returned NaN/Inf values.
        """
        self._fail_fast_if_degraded()
        if not texts:
            return []

        cap = self.max_batch_size if self.max_batch_size > 0 else len(texts)
        vectors: List[List[float]] = []
        for start in range(0, len(texts), cap):
            vectors.extend(await self._embed_slice(texts[start : start + cap]))

        for position, vector in enumerate(vectors):
            self._check_dimensions(vector)
            self._check_finite(vector, position)
        return vectors

    async def verify_dimensions(self, timeout: float = 20.0) -> Optional[bool]:
        """Probe the endpoint's dimensionality (B4 startup guard).

        Returns:
            True if the dimensionality matches, False on mismatch (degraded
            state is latched and the ERROR logged by the check itself), or
            None if the probe was inconclusive (endpoint unreachable /
            timeout) — connectivity issues are transient and must NOT latch.
        """
        try:
            await asyncio.wait_for(self.embed("dimension probe"), timeout=timeout)
            return True
        except EmbeddingDimensionError:
            return False
        except Exception as e:
            logger.warning(
                "Embedding dimension probe inconclusive (%s: %s) — endpoint "
                "unreachable? Dimensions will be checked on first real use.",
                type(e).__name__,
                e,
            )
            return None

    def health_snapshot(self) -> dict:
        """Status-endpoint view of the embedding path (see B4)."""
        return {
            "provider": self.provider,
            "model": self.model,
            "base_url": self.base_url,
            "expected_dimensions": self.expected_dimensions,
            "profile_fingerprint": self.profile_fingerprint,
            "degraded": self.degraded_reason is not None,
            "degraded_reason": self.degraded_reason,
        }


def get_embedding_service() -> EmbeddingService:
    """Get or create the singleton EmbeddingService instance.

    Returns:
        EmbeddingService singleton
    """
    global _embedding_service
    if _embedding_service is None:
        _embedding_service = EmbeddingService()
    return _embedding_service


def get_kb_embedding_service() -> EmbeddingService:
    """Return the embedding service used for all knowledge-base vectors.

    External OKF repositories are indexed centrally by the orchestrator.  A
    worker's regular ``EMBEDDING_*`` profile may be a per-user preference used
    by memory, so querying the central index with that profile can produce an
    incompatible vector (or an ``embedding_version`` that filters every row
    out).  Dispatch therefore supplies the orchestrator-owned profile under
    ``KB_EMBEDDING_*`` and this function keeps it on a separate singleton.

    Older/local deployments that do not yet dispatch a dedicated profile keep
    the historical behaviour by falling back to ``get_embedding_service()``.
    A *partial* dedicated profile does not fall back: once a KB model is
    declared, using a different user model would silently return wrong results,
    so the declared profile is constructed and any missing credential fails at
    the embedding call in the normal, visible way.
    """
    global _kb_embedding_profile, _kb_embedding_service

    model = os.getenv("KB_EMBEDDING_MODEL")
    if not model:
        return get_embedding_service()

    provider = os.getenv("KB_EMBEDDING_PROVIDER", "local").lower()
    base_url = os.getenv("KB_EMBEDDING_BASE_URL")
    api_key = os.getenv("KB_EMBEDDING_API_KEY", "")
    dimensions = int(os.getenv("KB_EMBEDDING_DIMENSIONS", "4096"))
    profile_identity = os.getenv("KB_EMBEDDING_PROFILE_ID")
    profile = (provider, model, base_url, api_key, dimensions, profile_identity)

    # Compare the effective profile on every access. Worker pods can process a
    # later dispatch after an administrator changes the system embedding pin;
    # persistent pods also receive refreshed in-flight credentials on attach.
    if _kb_embedding_service is None or _kb_embedding_profile != profile:
        _kb_embedding_service = EmbeddingService(
            model=model,
            base_url=base_url,
            api_key=api_key,
            provider=provider,
            expected_dimensions=dimensions,
            profile_identity=profile_identity,
        )
        _kb_embedding_profile = profile
    return _kb_embedding_service


def apply_kb_embedding_env(env_keys: Optional[Mapping]) -> None:
    """Replace the process KB profile with one authoritative dispatch snapshot.

    Agent processes can be reused. Clear every known field first so switching
    from an endpoint-backed profile to a provider-backed profile (which omits a
    base URL), or to no knowledge scope at all, cannot retain a previous
    endpoint or credential.
    """
    global _kb_embedding_profile, _kb_embedding_service

    for key in KB_EMBEDDING_ENV_KEYS:
        os.environ.pop(key, None)
    if env_keys:
        for key in KB_EMBEDDING_ENV_KEYS:
            value = env_keys.get(key)
            if value is not None:
                os.environ[key] = str(value)
    _kb_embedding_service = None
    _kb_embedding_profile = None


def peek_embedding_service() -> Optional[EmbeddingService]:
    """Return the singleton if it already exists, without constructing it.

    For status surfacing: a status poll must not be the thing that
    instantiates (and logs) the service on agents that never use memory.
    """
    return _embedding_service
