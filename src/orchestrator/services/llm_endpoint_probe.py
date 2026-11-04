"""Shared probe helper for LLM endpoint connectivity + model discovery.

Both the ``POST /endpoints/{id}/test`` (connectivity check) and the
``POST /endpoints/{id}/discover`` (import flow) endpoints call into the
same probe: ``GET {base_url}/models`` with the endpoint's Bearer token.
The test handler cares only about success/status; the discover handler
additionally parses the ``data`` array and attaches a capability hint
per model so the UI can prefill the per-row capability <select>.

A server with no model list (TEI) answers that probe 404; both Tests then
judge an embedding or rerank row by :func:`probe_endpoint_capability`, one
minimal call of that capability.

The capability hint is a name-based heuristic, not authoritative — users
are expected to override it at import time when the provider's naming
convention doesn't line up (e.g., ``text-embedding-3-*`` → embedding, but
a homegrown ``my-multimodal-8b`` would be tagged ``chat`` and the user
flips it to ``vision`` before committing).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import httpx

from shared.runtime.core.model_registry import family_of
from shared.runtime.core.transport_resolution import wire_model_id


@dataclass
class ProbeResult:
    """Outcome of probing ``GET {base_url}/models`` on an LLM endpoint.

    ``models`` is empty on any non-2xx response or on parse failure; callers
    that only care about connectivity (the ``/test`` endpoint) should
    ignore it.
    """

    ok: bool
    status: int | None
    error: str | None
    probe_url: str
    models: list[dict[str, Any]] = field(default_factory=list)


def _is_live_voice_id(name: str) -> bool:
    """A live (full-duplex voice) model by its name: never a chat row, so it
    is classified before the chat fallback."""
    return "gpt-live" in name or "realtime" in name


def declares_live_voice(item: Any) -> bool:
    """A model listing entry that says it is a live voice model.

    SRW's voice gateway lists each profile with ``"capabilities":
    ["live_voice"]``: a profile's id is the operator's name for it and need
    not contain ``gpt-live`` or ``realtime``. Only this declaration is
    honoured; other servers' capability vocabularies differ from the
    catalog's."""
    capabilities = item.get("capabilities") if isinstance(item, dict) else None
    return isinstance(capabilities, list) and "live_voice" in capabilities


def _capability_hint(model_id: str) -> str:
    """Heuristic: map a model_id to the most likely PRIMARY capability slot.

    Treated as a suggestion — the UI lets the user override per-row before
    import. Pattern checks run in decreasing specificity so ``whisper`` and
    ``embed`` hit before the ``vision``/``vl`` fallbacks.

    Returns the singular primary capability — used by the legacy
    ``capability_hint`` wire field and by callers that want one label.
    The full ``capability_hints`` array (with auxiliary auto-expansion
    and multimodal-vision detection) lives in :func:`_capability_hints`.
    """
    name = model_id.lower()
    if _is_live_voice_id(name):
        return "live_voice"
    if "whisper" in name:
        return "whisper"
    if "tts" in name or name.endswith("-speech") or "text-to-speech" in name:
        return "tts"
    if "rerank" in name:
        return "rerank"
    if "embed" in name or name.endswith("-embedding"):
        return "embedding"
    if "vision" in name or "-vl" in name or "multimodal" in name:
        return "vision"
    # Deliberately no 'auxiliary' heuristic — auxiliary is a role, not a
    # naming convention. Users pick which chat models should fill it.
    return "chat"


def _capability_hints(model_id: str) -> list[str]:
    """Heuristic: map a model_id to the most likely ``capabilities[]``.

    Chat-capable IDs auto-expand to ``[chat, auxiliary]`` (chat-capable
    LLMs always serve auxiliary). The result mirrors the discovery
    service's :func:`_capabilities_from_model_id` semantics, intentionally
    minus the multimodal-vision heuristic — endpoint probes return raw
    server inventories with arbitrary IDs, and we don't want to claim
    vision capability without explicit naming evidence.
    """
    primary = _capability_hint(model_id)
    if primary != "chat":
        return [primary]
    return ["chat", "auxiliary"]


def _is_web_page(resp: httpx.Response) -> bool:
    """True when the body is an HTML page rather than an API response.

    An HTML page means the base URL reaches a web UI or a non-LLM service
    (SearXNG answers ``/models`` with its 404 page). Echoing the page as the
    error would bury the one useful fact under markup.
    """
    content_type = resp.headers.get("content-type", "").lower()
    return "html" in content_type or resp.text.lstrip().startswith("<")


def _web_page_error(resp: httpx.Response, probe_url: str) -> str:
    return (
        f"HTTP {resp.status_code}: {probe_url} answered with a web page, "
        "not an OpenAI-compatible model list."
    )


async def probe_endpoint_models(
    base_url: str,
    api_key: str | None,
    *,
    timeout: float = 10.0,
) -> ProbeResult:
    """Probe ``{base_url}/models`` and return the parsed model list.

    Does not raise. Transport errors land in ``ProbeResult.error`` so both
    the test and discover handlers can surface them without wrapping.
    """
    probe_url = base_url.rstrip("/") + "/models"
    headers: dict[str, str] = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(probe_url, headers=headers)
    except httpx.HTTPError as e:
        return ProbeResult(
            ok=False,
            status=None,
            error=f"{type(e).__name__}: {e}",
            probe_url=probe_url,
        )

    if not (200 <= resp.status_code < 300):
        return ProbeResult(
            ok=False,
            status=resp.status_code,
            # JSON error bodies (a bad key, an unknown route) are what LLM
            # servers send and stay verbatim.
            error=(
                _web_page_error(resp, probe_url)
                if _is_web_page(resp)
                else (resp.text[:500] if resp.text else None)
            ),
            probe_url=probe_url,
        )

    try:
        payload = resp.json()
    except ValueError as e:
        return ProbeResult(
            ok=False,
            status=resp.status_code,
            error=(
                _web_page_error(resp, probe_url)
                if _is_web_page(resp)
                else f"Response body is not JSON: {e}"
            ),
            probe_url=probe_url,
        )

    raw = payload.get("data") if isinstance(payload, dict) else payload
    if not isinstance(raw, list):
        return ProbeResult(
            ok=True,
            status=resp.status_code,
            error=None,
            probe_url=probe_url,
            models=[],
        )

    models: list[dict[str, Any]] = []
    for item in raw:
        if isinstance(item, str):
            model_id = item
            owned_by = None
            context_window = None
        elif isinstance(item, dict):
            model_id = item.get("id") or item.get("name")
            owned_by = item.get("owned_by") or item.get("owner")
            # vLLM reports max_model_len; some Ollama/llama.cpp forks expose
            # context_length. Both name the same thing — the model's max
            # context in tokens. Fall back to None for servers that omit it.
            raw_ctx = item.get("max_model_len") or item.get("context_length")
            try:
                context_window = int(raw_ctx) if raw_ctx is not None else None
            except (TypeError, ValueError):
                context_window = None
            # The pydantic ge=1000 constraint on import would reject tiny
            # values; drop them here so a partial pre-fill still validates.
            if context_window is not None and context_window < 1000:
                context_window = None
        else:
            continue
        if not model_id:
            continue
        # Infer family from the model_id (gemma-4-31b → "gemma", gpt-oss-20b →
        # "gpt-oss"). family_of returns "default" on miss; surface that as
        # None so the runtime settings_matrix fallback kicks in instead of
        # locking the row to the literal "default" family.
        inferred_family = family_of(model_id)
        family = inferred_family if inferred_family != "default" else None
        models.append(
            {
                "id": model_id,
                "owned_by": owned_by,
                "capability_hints": (
                    ["live_voice"]
                    if declares_live_voice(item)
                    else _capability_hints(model_id)
                ),
                "family": family,
                "context_window": context_window,
            }
        )

    return ProbeResult(
        ok=True,
        status=resp.status_code,
        error=None,
        probe_url=probe_url,
        models=models,
    )


# Capabilities served by model servers that may have no model list: Hugging
# Face TEI answers ``POST /embeddings`` or ``POST /rerank`` but 404s
# ``GET /models``. In probe order, for a row that declares both.
_CAPABILITY_PROBE_ORDER = ("embedding", "rerank")


def capability_to_probe(capabilities: Any) -> str | None:
    """The capability a model row is tested by when its server has no model
    list: embedding, else rerank. ``None`` for any other row, so a chat row
    never spends a completion on a Test."""
    declared = capabilities if isinstance(capabilities, (list, tuple)) else ()
    for capability in _CAPABILITY_PROBE_ORDER:
        if capability in declared:
            return capability
    return None


def _capability_probe_request(
    capability: str, model: str
) -> tuple[str, dict[str, Any], str]:
    """Path, body and required list field of a one-item capability call.

    Bodies match SRW's own clients: the embedding service's
    ``embeddings.create(input=, model=)`` and the memory reranker's
    Cohere-shaped ``{model, query, documents}`` read back from ``results``.
    """
    if capability == "embedding":
        return "/embeddings", {"model": model, "input": "ping"}, "data"
    return (
        "/rerank",
        {"model": model, "query": "ping", "documents": ["pong"]},
        "results",
    )


async def probe_endpoint_capability(
    *,
    base_url: str,
    api_key: str | None,
    capability: str,
    model: str,
    timeout: float = 10.0,
) -> ProbeResult:
    """Make one minimal ``capability`` call against ``{base_url}``.

    ``model`` is the row's upstream name (its served id, else its catalog id);
    a routing prefix the provider rejects is dropped as SRW's embedding and
    rerank clients drop it. Same client settings as the model-list probe
    (timeout, default TLS verification, Bearer key only when set) and the same
    host. Does not raise.
    """
    path, body, result_field = _capability_probe_request(
        capability, wire_model_id(model, base_url)
    )
    probe_url = base_url.rstrip("/") + path
    headers: dict[str, str] = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(probe_url, headers=headers, json=body)
    except httpx.HTTPError as e:
        return ProbeResult(False, None, f"{type(e).__name__}: {e}", probe_url)

    status = resp.status_code
    if _is_web_page(resp):
        return ProbeResult(
            False,
            status,
            f"HTTP {status}: {probe_url} answered with a web page, not a "
            f"{capability} response.",
            probe_url,
        )
    if not (200 <= status < 300):
        return ProbeResult(False, status, resp.text[:500] or None, probe_url)
    try:
        payload = resp.json()
    except ValueError as e:
        return ProbeResult(False, status, f"Response body is not JSON: {e}", probe_url)
    if not (isinstance(payload, dict) and payload.get(result_field)):
        return ProbeResult(
            False,
            status,
            f"HTTP {status}: {probe_url} answered without a {capability} result.",
            probe_url,
        )
    return ProbeResult(True, status, None, probe_url)
