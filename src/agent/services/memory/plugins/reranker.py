"""Reranker scorer (memory overhaul Phase 3, slice 2).

Reorders memory candidates by query relevance via an external rerank
endpoint (Cohere-shaped ``POST {base_url}/rerank``). The model and transport
come from the ``rerank`` catalog slot (Admin → Models): the orchestrator
injects ``RERANK_MODEL``/``RERANK_BASE_URL``/``RERANK_API_KEY`` from the
pinned row at dispatch. With no ``RERANK_*`` at all the plugin rides the
**embedding** transport (``EMBEDDING_BASE_URL``/``EMBEDDING_API_KEY`` — the
single-router layout where ``qwen3-reranker-8b`` sits next to
``qwen3-embedding-8b``). It deliberately does NOT ride the auxiliary model: the
auxiliary is a freely-swapped chat model that may be OpenRouter-direct (no
explicit ``base_url``) or a provider that serves no ``/rerank`` route —
coupling to it crashed session startup and silently no-op'd reranking. See
``knowledge-base/knowledge/issues/openrouter_auxiliary_crashes_session_via_memory_reranker.md``.

Motivated by the Phase-2 baseline finding: hybrid retrieval surfaces
the evidence memory at ~rank 13 of ~122 injected items on the seam arm
(eval/memory runs `seam_s20` vs `flat_s100`) — injection order is what
the model reads, so ordering IS retrieval quality.

Scope discipline:
- Only ``kind == "memory"`` items are reranked; knowledge items render
  to their own block and pass through in original order.
- TTL-pinned items (``record.remaining_turns > 0``) stay ahead of the
  reranked tail by default (``memory.reranker.keep_pinned_first``) —
  the pinned tier is the recency working set; replacing it is the
  bounded-core policy's job, not the scorer's.
- At most ``memory.reranker.top_k`` candidates go to the endpoint; the
  remainder keeps its original (hybrid) order behind the reranked head.
- Failure semantics split by cause (mirrors the main-LLM retry
  classifier; knowledge-base/knowledge/issues/reranker_transient_fault_hard_fails_job.md):
  *transient* transport faults — timeouts, connection errors/resets,
  5xx/429 — get a bounded in-call retry with backoff, and if they
  persist the scorer raises ``TransientScorerError`` so the manager can
  degrade to the pre-scorer (hybrid) order for that one turn.
  *Structural* failures — 4xx, bad URL, malformed response shape —
  raise immediately and the manager escalates them to
  ``MemoryPipelineError`` (a configured reranker is required; never
  silently serve legacy order behind a broken config). The scorer never
  partially reorders either way.
"""

import asyncio
import logging
import os
from typing import Any, List, Mapping, Optional, Tuple

from agent.services.memory.registry import register_memory_plugin
from agent.services.memory.types import AssembleRequest, Scored, TransientScorerError

logger = logging.getLogger(__name__)


def _is_transient(exc: Exception) -> bool:
    """True for faults where retrying is not futile (network blips, 5xx).

    4xx (except 429) means the request itself is wrong — auth, route,
    payload — and will fail identically on retry; those stay structural.
    Everything raised outside httpx (shape/parse errors) is structural too.
    """
    import httpx

    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return code >= 500 or code == 429
    # TimeoutException covers Connect/Read/Write/PoolTimeout; NetworkError
    # covers ConnectError/ReadError/WriteError/CloseError; RemoteProtocolError
    # is the server dropping the connection mid-request (observed live as the
    # uni-box reranker restarting under it).
    return isinstance(
        exc, (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError)
    )


def _is_pinned(item: Scored) -> bool:
    record = item.candidate.record
    return bool(getattr(record, "remaining_turns", 0) or 0)


class RerankerScorer:
    """Scorer protocol implementation over a Cohere-shaped /rerank route."""

    def __init__(
        self,
        model: str,
        base_url: str,
        api_key: Optional[str],
        top_k: int = 64,
        timeout: float = 10.0,
        keep_pinned_first: bool = True,
        retries: int = 2,
        retry_backoff: float = 1.0,
        client: Optional[Any] = None,
    ) -> None:
        if not base_url:
            raise ValueError(
                "reranker needs a base_url: pin a rerank model in Admin → Models "
                "(RERANK_BASE_URL), set memory.reranker.base_url, or provide "
                "EMBEDDING_BASE_URL (the reranker rides the embedding endpoint)"
            )
        self.model = model
        self.endpoint = base_url.rstrip("/") + "/rerank"
        self.api_key = api_key
        self.top_k = top_k
        self.timeout = timeout
        self.keep_pinned_first = keep_pinned_first
        self.retries = max(0, retries)  # extra attempts after the first
        self.retry_backoff = retry_backoff  # base delay, doubles per retry
        self._client = client  # injectable for tests; lazily built otherwise

    def _http_client(self) -> Any:
        if self._client is None:
            import httpx

            headers = {}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            self._client = httpx.AsyncClient(headers=headers, timeout=self.timeout)
        return self._client

    async def _rerank(
        self, query: str, documents: List[str]
    ) -> List[Tuple[int, float]]:
        """Call the endpoint; return (index, relevance_score) pairs.

        Transient transport faults are retried in-call (``retries`` extra
        attempts, exponential backoff from ``retry_backoff``); if they outlast
        the budget the call raises ``TransientScorerError`` for the manager to
        degrade this one turn. Structural failures raise immediately.
        """
        attempts = self.retries + 1
        last_exc: Optional[Exception] = None
        for attempt in range(attempts):
            if attempt:
                await asyncio.sleep(self.retry_backoff * (2 ** (attempt - 1)))
            try:
                response = await self._http_client().post(
                    self.endpoint,
                    json={
                        "model": self.model,
                        "query": query,
                        "documents": documents,
                    },
                )
                response.raise_for_status()
                payload = response.json()
                return [
                    (int(r["index"]), float(r["relevance_score"]))
                    for r in payload["results"]
                ]
            except Exception as e:
                if not _is_transient(e):
                    raise
                last_exc = e
                logger.warning(
                    "reranker transient fault (attempt %d/%d) on %s: %s: %s",
                    attempt + 1,
                    attempts,
                    self.endpoint,
                    type(e).__name__,
                    e,
                )
        raise TransientScorerError(
            f"rerank endpoint {self.endpoint} kept failing transiently "
            f"({attempts} attempts): {type(last_exc).__name__}: {last_exc}"
        ) from last_exc

    async def score(self, req: AssembleRequest, items: List[Scored]) -> List[Scored]:
        if not req.query_text:
            return items

        head: List[Scored] = []  # pinned memory items, original order
        eligible: List[Scored] = []  # memory items to rerank
        others: List[Scored] = []  # knowledge/other kinds, original order
        for item in items:
            if item.candidate.kind != "memory":
                others.append(item)
            elif self.keep_pinned_first and _is_pinned(item):
                head.append(item)
            else:
                eligible.append(item)

        to_rank = eligible[: self.top_k]
        tail = eligible[self.top_k :]
        if len(to_rank) < 2:
            return items

        ranked = await self._rerank(
            req.query_text, [item.candidate.text or "" for item in to_rank]
        )
        if len(ranked) != len(to_rank):
            raise ValueError(
                f"rerank returned {len(ranked)} results for {len(to_rank)} documents"
            )

        # Apply only after a fully valid response — no partial reorder.
        reordered = []
        for index, relevance in sorted(ranked, key=lambda r: r[1], reverse=True):
            item = to_rank[index]
            item.score = relevance
            item.candidate.channel_scores["rerank"] = relevance
            reordered.append(item)

        return [*head, *reordered, *tail, *others]


DEFAULT_RERANK_MODEL = "qwen3-reranker-8b"


def resolve_reranker_transport(
    cfg: Any, env: Optional[Mapping[str, str]] = None
) -> Tuple[str, Optional[str], Optional[str]]:
    """``(model, base_url, api_key)`` for the scorer, explicit config first.

    Per field: ``memory.reranker.*`` (YAML / job override) beats the
    ``RERANK_*`` env the orchestrator injects from the ``rerank`` catalog pin,
    which beats the embedding transport. ``base_url`` and ``api_key`` travel
    as a pair: a host chosen by ``RERANK_BASE_URL`` is authenticated only by
    ``RERANK_API_KEY`` (the embedding key never leaves the embedding host),
    and when no rerank endpoint resolves at all both fall back to
    ``EMBEDDING_*`` — the single-router deployment the plugin was built for.
    An explicit ``memory.reranker.base_url`` is authenticated by its own
    ``memory.reranker.api_key``, or by an env key only when that key's paired
    base URL is the same host: a key follows its endpoint.
    """
    if env is None:
        env = os.environ
    model = cfg.model or env.get("RERANK_MODEL") or DEFAULT_RERANK_MODEL
    if cfg.base_url:
        base_url = cfg.base_url
        api_key = cfg.api_key
        if not api_key:
            from shared.runtime.core.transport_resolution import same_endpoint

            for prefix in ("RERANK", "EMBEDDING"):
                paired = env.get(f"{prefix}_BASE_URL")
                if (
                    paired
                    and same_endpoint(paired, base_url)
                    and env.get(f"{prefix}_API_KEY")
                ):
                    api_key = env[f"{prefix}_API_KEY"]
                    break
    elif env.get("RERANK_BASE_URL"):
        base_url = env["RERANK_BASE_URL"]
        api_key = cfg.api_key or env.get("RERANK_API_KEY")
    else:
        base_url = env.get("EMBEDDING_BASE_URL")
        api_key = cfg.api_key or env.get("EMBEDDING_API_KEY")
    return model, base_url, api_key


@register_memory_plugin(
    "scorer",
    "reranker",
    description="Cross-encoder rerank of memory candidates against the "
    "query (Cohere-shaped /rerank route; defaults to the embedding "
    "endpoint's transport)",
)
def _build_reranker(runtime: Any) -> RerankerScorer:
    cfg = getattr(runtime.memory_config, "reranker", None)
    if cfg is None:
        raise ValueError("memory.reranker config section missing")
    # Transport precedence: explicit memory.reranker.* → the RERANK_* env the
    # orchestrator injects from the `rerank` catalog pin → the embedding
    # endpoint (single-router deployments). Never the auxiliary: it may be
    # OpenRouter-direct or serve no /rerank route at all.
    model, base_url, api_key = resolve_reranker_transport(cfg)
    return RerankerScorer(
        model=model,
        base_url=base_url,
        api_key=api_key,
        top_k=cfg.top_k,
        timeout=cfg.timeout,
        keep_pinned_first=cfg.keep_pinned_first,
        retries=getattr(cfg, "retries", 2),
        retry_backoff=getattr(cfg, "retry_backoff", 1.0),
    )
